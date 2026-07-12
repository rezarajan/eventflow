package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/admission"
	"github.com/rezarajan/eventflow/httpflow"
	"github.com/rezarajan/eventflow/quarantine"
	"github.com/rezarajan/eventflow/redpanda"
	"github.com/rezarajan/eventflow/resource"
	"github.com/rezarajan/eventflow/spool"
	"gopkg.in/yaml.v3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return nil
	}
	switch args[0] {
	case "validate", "inspect", "run", "status":
		return resourceCommand(ctx, args[0], args[1:], stdout, stderr)
	case "policy":
		if len(args) > 1 && args[1] == "test" {
			return policyTest(ctx, args[2:], stdout, stderr)
		}
	case "quarantine":
		return quarantineCommand(ctx, args[1:], stdout, stderr)
	}
	return fmt.Errorf("unknown command %q", strings.Join(args, " "))
}

func catalog() (*resource.Catalog, error) {
	catalog := resource.NewCatalog()
	for _, register := range []func(*resource.Catalog) error{httpflow.Register, redpanda.Register, quarantine.Register, spool.Register} {
		if err := register(catalog); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func resourceCommand(ctx context.Context, command string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("eventflow "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var configs multiFlag
	flags.Var(&configs, "config", "resource YAML file; may be repeated")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(configs) == 0 {
		return fmt.Errorf("--config is required")
	}
	catalog, err := catalog()
	if err != nil {
		return err
	}
	docs, err := resource.LoadFiles(configs...)
	if err != nil {
		return err
	}
	switch command {
	case "validate":
		_, err := resource.Validate(ctx, catalog, docs)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "resources valid: %d documents\n", len(docs))
		return nil
	case "inspect":
		graph, err := resource.Validate(ctx, catalog, docs)
		if err != nil {
			return err
		}
		for _, node := range graph.Nodes() {
			fmt.Fprintln(stdout, node.String())
		}
		return nil
	case "run":
		compiled, err := resource.Compile(ctx, catalog, docs)
		if err != nil {
			return err
		}
		if len(compiled.Flows) != 1 {
			return fmt.Errorf("exactly one EventFlow is required")
		}
		return runFlow(ctx, compiled.Flows[0], stdout)
	case "status":
		compiled, err := resource.Compile(ctx, catalog, docs)
		if err != nil {
			return err
		}
		return printStatus(ctx, compiled.Flows, stdout)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func runFlow(ctx context.Context, flow resource.Flow, stdout io.Writer) error {
	receiver, ok := flow.Receiver.(eventflow.Receiver)
	if !ok {
		return fmt.Errorf("flow receiver is invalid")
	}
	store, ok := flow.Quarantine.(*quarantine.Store)
	if !ok {
		return fmt.Errorf("quarantine store is invalid")
	}
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close(ctx)
	if err := receiver.Open(ctx); err != nil {
		return err
	}
	defer receiver.Close(ctx)
	var emitter eventflow.Emitter
	if flow.Emitter != nil {
		emitter, ok = flow.Emitter.(eventflow.Emitter)
		if !ok {
			return fmt.Errorf("flow emitter is invalid")
		}
		if err := emitter.Open(ctx); err != nil {
			return err
		}
		defer emitter.Close(ctx)
	}
	var localSpool *spool.SQLite
	if flow.Spool != nil {
		localSpool, ok = flow.Spool.(*spool.SQLite)
		if !ok {
			return fmt.Errorf("spool is invalid")
		}
		if err := localSpool.Open(ctx); err != nil {
			return err
		}
		defer localSpool.Close(ctx)
	}
	limiter := admission.NewRateLimiter()
	for {
		received, err := receiver.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if decision, limited := limiter.Check(received.Principal, flow.RateLimitPerMinute, time.Now(), flow.Policy); limited {
			_, _ = store.Put(ctx, quarantineRecord(flow, received, decision, admission.OpenLineageEvent{}))
			if received.Nack != nil {
				_ = received.Nack(ctx)
			}
			continue
		}
		decision, ol := admission.Evaluate(received.Event, received.Raw, received.Principal, flow.Contract, flow.Policy)
		if decision.Outcome != eventflow.OutcomeAccept {
			_, _ = store.Put(ctx, quarantineRecord(flow, received, decision, ol))
			if received.Nack != nil {
				_ = received.Nack(ctx)
			}
			continue
		}
		if emitter == nil {
			if localSpool == nil {
				return fmt.Errorf("accepted event has no destination")
			}
			if err := localSpool.Put(ctx, received.Event); err != nil {
				return err
			}
		} else if err := emitter.Emit(ctx, received.Event); err != nil {
			if flow.BrokerUnavailablePolicy == "spool" && localSpool != nil {
				if spoolErr := localSpool.Put(ctx, received.Event); spoolErr != nil {
					return spoolErr
				}
			} else {
				brokerDecision := eventflow.Rejected(eventflow.ReasonBrokerUnavailable, err.Error(), "broker", received.Principal, flow.Policy.Name, flow.Policy.Version)
				_, _ = store.Put(ctx, quarantineRecord(flow, received, brokerDecision, ol))
				if received.Nack != nil {
					_ = received.Nack(ctx)
				}
				continue
			}
		}
		if received.Ack != nil {
			if err := received.Ack(ctx); err != nil {
				return err
			}
		}
		fmt.Fprintln(stdout, "accepted", received.Event.ID())
	}
}

func quarantineRecord(flow resource.Flow, received eventflow.ReceivedEvent, decision eventflow.Decision, ol admission.OpenLineageEvent) quarantine.Record {
	return quarantine.Record{
		Raw: received.Raw, CloudEventsID: received.Event.ID(), OpenLineageRunID: ol.Run.RunID,
		Principal: received.Principal, PolicyName: flow.Policy.Name, PolicyVersion: flow.Policy.Version,
		ContractName: flow.Contract.Name, ContractVersion: flow.Contract.Version, Decision: decision,
		Field: decision.Field, ReceiveTime: time.Now().UTC(), Target: flow.Name,
	}
}

func policyTest(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("eventflow policy test", flag.ContinueOnError)
	flags.SetOutput(stderr)
	config := flags.String("config", "", "resource YAML file")
	eventPath := flags.String("event", "", "event JSON file")
	suitePath := flags.String("suite", "", "declarative policy test suite YAML")
	principal := flags.String("principal", "", "authenticated principal")
	expectOutcome := flags.String("expect-outcome", "", "expected outcome")
	expectReason := flags.String("expect-reason", "", "expected reason code")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *config == "" || (*eventPath == "" && *suitePath == "") {
		return fmt.Errorf("--config and either --event or --suite are required")
	}
	flow, err := compileOne(ctx, *config)
	if err != nil {
		return err
	}
	if *suitePath != "" {
		return runPolicySuite(flow, *suitePath, stdout)
	}
	raw, err := os.ReadFile(*eventPath)
	if err != nil {
		return err
	}
	event, err := httpflowEvent(raw)
	if err != nil {
		return err
	}
	decision, _ := admission.Evaluate(event, raw, *principal, flow.Contract, flow.Policy)
	body, _ := json.MarshalIndent(decision, "", "  ")
	fmt.Fprintln(stdout, string(body))
	if *expectOutcome != "" && string(decision.Outcome) != *expectOutcome {
		return fmt.Errorf("expected outcome %s, got %s", *expectOutcome, decision.Outcome)
	}
	if *expectReason != "" && decision.ReasonCode != *expectReason {
		return fmt.Errorf("expected reason %s, got %s", *expectReason, decision.ReasonCode)
	}
	return nil
}

type policySuite struct {
	Tests []policySuiteCase `yaml:"tests" json:"tests"`
}

type policySuiteCase struct {
	Name          string `yaml:"name" json:"name"`
	Event         string `yaml:"event" json:"event"`
	Principal     string `yaml:"principal" json:"principal"`
	ExpectOutcome string `yaml:"expectOutcome" json:"expectOutcome"`
	ExpectReason  string `yaml:"expectReason" json:"expectReason"`
}

func runPolicySuite(flow resource.Flow, path string, stdout io.Writer) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var suite policySuite
	if err := yaml.Unmarshal(body, &suite); err != nil {
		return err
	}
	if len(suite.Tests) == 0 {
		return fmt.Errorf("policy test suite has no tests")
	}
	for _, test := range suite.Tests {
		raw, err := os.ReadFile(test.Event)
		if err != nil {
			return fmt.Errorf("%s: %w", test.Name, err)
		}
		event, err := httpflowEvent(raw)
		if err != nil {
			return fmt.Errorf("%s: %w", test.Name, err)
		}
		decision, _ := admission.Evaluate(event, raw, test.Principal, flow.Contract, flow.Policy)
		if test.ExpectOutcome != "" && string(decision.Outcome) != test.ExpectOutcome {
			return fmt.Errorf("%s: expected outcome %s, got %s", test.Name, test.ExpectOutcome, decision.Outcome)
		}
		if test.ExpectReason != "" && decision.ReasonCode != test.ExpectReason {
			return fmt.Errorf("%s: expected reason %s, got %s", test.Name, test.ExpectReason, decision.ReasonCode)
		}
		fmt.Fprintf(stdout, "ok %s %s %s\n", test.Name, decision.Outcome, decision.ReasonCode)
	}
	return nil
}

func httpflowEvent(raw []byte) (eventflow.Event, error) {
	return httpflow.TestEventFromBytes(raw)
}

func compileOne(ctx context.Context, config string) (resource.Flow, error) {
	catalog, err := catalog()
	if err != nil {
		return resource.Flow{}, err
	}
	docs, err := resource.LoadFiles(config)
	if err != nil {
		return resource.Flow{}, err
	}
	compiled, err := resource.Compile(ctx, catalog, docs)
	if err != nil {
		return resource.Flow{}, err
	}
	if len(compiled.Flows) != 1 {
		return resource.Flow{}, fmt.Errorf("exactly one EventFlow is required")
	}
	return compiled.Flows[0], nil
}

func quarantineCommand(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("quarantine subcommand is required")
	}
	flags := flag.NewFlagSet("eventflow quarantine "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	config := flags.String("config", "", "resource YAML file")
	showPayload := flags.Bool("payload", false, "include payload")
	corrected := flags.String("event", "", "corrected event file")
	principal := flags.String("principal", "operator", "operator principal")
	replaceIdentity := flags.Bool("replace-identity", false, "generate a replacement CloudEvents id during replay")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *config == "" {
		return fmt.Errorf("--config is required")
	}
	flow, err := compileOne(ctx, *config)
	if err != nil {
		return err
	}
	store := flow.Quarantine.(*quarantine.Store)
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close(ctx)
	switch args[0] {
	case "list":
		records, err := store.List(ctx, "", 100)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(records)
	case "show", "validate", "dismiss", "replay":
		if flags.NArg() < 1 {
			return fmt.Errorf("record id is required")
		}
		id, err := strconv.ParseInt(flags.Arg(0), 10, 64)
		if err != nil {
			return err
		}
		record, err := store.Get(ctx, id)
		if err != nil {
			return err
		}
		if args[0] == "show" {
			if !*showPayload {
				record.Raw = nil
			}
			return json.NewEncoder(stdout).Encode(record)
		}
		if args[0] == "dismiss" {
			return store.Dismiss(ctx, id)
		}
		raw := record.Raw
		if *corrected != "" {
			raw, err = os.ReadFile(*corrected)
			if err != nil {
				return err
			}
		}
		event, err := httpflowEvent(raw)
		if err != nil {
			return err
		}
		if args[0] == "replay" && *replaceIdentity {
			event.SetID(fmt.Sprintf("eventflow-replay-%d-%d", id, time.Now().UnixNano()))
		}
		decision, _ := admission.Evaluate(event, raw, *principal, flow.Contract, flow.Policy)
		if args[0] == "validate" {
			return json.NewEncoder(stdout).Encode(decision)
		}
		if decision.Outcome != eventflow.OutcomeAccept {
			return fmt.Errorf("record is not replayable: %s", decision.ReasonCode)
		}
		emitter, ok := flow.Emitter.(eventflow.Emitter)
		if !ok {
			return fmt.Errorf("flow emitter is invalid")
		}
		if err := emitter.Open(ctx); err != nil {
			return err
		}
		defer emitter.Close(ctx)
		if err := emitter.Emit(ctx, event); err != nil {
			return err
		}
		return store.MarkReplayed(ctx, id, *principal)
	default:
		return fmt.Errorf("unknown quarantine subcommand %q", args[0])
	}
}

func printStatus(ctx context.Context, flows []resource.Flow, stdout io.Writer) error {
	if len(flows) != 1 {
		return fmt.Errorf("exactly one EventFlow is required")
	}
	flow := flows[0]
	status := map[string]any{
		"policyVersion":             flow.Policy.Version,
		"contractVersion":           flow.Contract.Version,
		"acceptedCount":             0,
		"rejectedCount":             0,
		"quarantineCount":           0,
		"brokerPublicationFailures": 0,
	}
	if store, ok := flow.Quarantine.(*quarantine.Store); ok {
		if err := store.Open(ctx); err == nil {
			records, _ := store.List(ctx, quarantine.StatusOpen, 1000)
			status["rejectedCount"] = len(records)
			status["quarantineCount"] = len(records)
			if len(records) > 0 {
				status["oldestUnresolvedQuarantineRecord"] = records[0].ID
			}
			brokerFailures := 0
			for _, record := range records {
				if record.Decision.ReasonCode == eventflow.ReasonBrokerUnavailable {
					brokerFailures++
				}
			}
			status["brokerPublicationFailures"] = brokerFailures
			_ = store.Close(ctx)
		}
	}
	if sp, ok := flow.Spool.(*spool.SQLite); ok {
		if err := sp.Open(ctx); err == nil {
			depth, _ := sp.Depth(ctx)
			status["spoolDepth"] = depth
			_ = sp.Close(ctx)
		}
	}
	return json.NewEncoder(stdout).Encode(status)
}

type multiFlag []string

func (m *multiFlag) String() string         { return fmt.Sprint([]string(*m)) }
func (m *multiFlag) Set(value string) error { *m = append(*m, value); return nil }

func usage(w io.Writer) {
	fmt.Fprint(w, `eventflow is an OpenLineage admission and quarantine gateway.

Usage:
  eventflow validate --config resources.yaml
  eventflow inspect --config resources.yaml
  eventflow run --config resources.yaml
  eventflow policy test --config resources.yaml --event event.json --principal spiffe://example
  eventflow quarantine list --config resources.yaml
  eventflow status --config resources.yaml
`)
}
