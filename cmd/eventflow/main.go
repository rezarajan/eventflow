package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/adapters/bundled"
	"github.com/rezarajan/eventflow/gateway/dispatch"
	"github.com/rezarajan/eventflow/journal"
	"github.com/rezarajan/eventflow/resource"
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
	if len(args) == 0 || args[0] == "help" || args[0] == "-help" || args[0] == "--help" {
		usage(stderr)
		return nil
	}
	switch args[0] {
	case "validate", "inspect", "run", "replay":
		return runCommand(ctx, args[0], args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runCommand(ctx context.Context, command string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("eventflow "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var configs multiFlag
	flags.Var(&configs, "config", "resource YAML file; may be repeated")
	if command == "replay" {
		flags.String("flow", "", "flow name; defaults to the only compiled flow")
		flags.String("destination", "", "destination resource key such as RedpandaEmitter/lineage-events")
		flags.String("state", string(journal.StateFailed), "delivery state filter: FAILED, PENDING, DELIVERED, or empty")
		flags.String("event-id", "", "CloudEvents id filter")
		flags.Int("limit", 100, "maximum events to replay or inspect")
		flags.Bool("dry-run", false, "print selected records without emitting")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(configs) == 0 {
		return fmt.Errorf("--config is required")
	}
	catalog := resource.NewCatalog()
	if err := bundled.Register(catalog); err != nil {
		return err
	}
	docs, err := resource.LoadFiles(configs...)
	if err != nil {
		return err
	}
	switch command {
	case "validate":
		if _, err := resource.Validate(ctx, catalog, docs); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "resources valid: %d documents\n", len(docs))
		return nil
	case "inspect":
		graph, err := resource.Validate(ctx, catalog, docs)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "documents: %d\nresources: %d\n", len(docs), len(graphNodes(graph)))
		for _, key := range graphNodes(graph) {
			fmt.Fprintf(stdout, "- %s\n", key.String())
		}
		return nil
	case "run":
		compiled, err := resource.Compile(ctx, catalog, docs)
		if err != nil {
			return err
		}
		if len(compiled.Flows) == 0 {
			if len(compiled.ObservationFlows) == 0 {
				return fmt.Errorf("no EventFlow or ObservationFlow resources compiled")
			}
			return runObservationFlow(ctx, compiled.ObservationFlows[0])
		}
		return runFlow(ctx, compiled.Flows[0])
	case "replay":
		return replayCommand(ctx, flags, docs, catalog, stdout)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func runFlow(ctx context.Context, flow resource.Flow) error {
	for _, emitter := range flow.Emitters {
		if err := emitter.Open(ctx); err != nil {
			return err
		}
		defer emitter.Close(ctx)
	}
	if flow.InvalidEmitter != nil {
		if err := flow.InvalidEmitter.Open(ctx); err != nil {
			return err
		}
		defer flow.InvalidEmitter.Close(ctx)
	}
	if flow.Journal != nil {
		if err := flow.Journal.Open(ctx); err != nil {
			return err
		}
		defer flow.Journal.Close(ctx)
		dispatcher := dispatch.New(flow.Dispatch, flow.Journal, destinationMap(flow))
		dispatchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		errs := make(chan error, 1)
		go func() { errs <- dispatcher.Run(dispatchCtx) }()
		runErr := flow.Runtime.Run(ctx)
		cancel()
		dispatchErr := <-errs
		if runErr != nil {
			return runErr
		}
		if dispatchErr != nil && !errors.Is(dispatchErr, context.Canceled) {
			return dispatchErr
		}
		return dispatcher.DispatchReady(context.Background())
	}
	if flow.Runtime.Receiver != nil {
		return flow.Runtime.Run(ctx)
	}
	return fmt.Errorf("flow has no receiver")
}

func replayCommand(ctx context.Context, flags *flag.FlagSet, docs []resource.Document, catalog *resource.Catalog, stdout io.Writer) error {
	destination := flags.Lookup("destination")
	if destination == nil || strings.TrimSpace(destination.Value.String()) == "" {
		return fmt.Errorf("--destination is required")
	}
	flowName := flagValue(flags, "flow")
	state := flagValue(flags, "state")
	if strings.EqualFold(state, "all") {
		state = ""
	}
	eventID := flagValue(flags, "event-id")
	limitValue := flagValue(flags, "limit")
	dryRun := flagValue(flags, "dry-run") == "true"
	limit := 0
	if limitValue != "" {
		parsed, err := strconv.Atoi(limitValue)
		if err != nil {
			return fmt.Errorf("limit: %w", err)
		}
		limit = parsed
	}
	compiled, err := resource.Compile(ctx, catalog, docs)
	if err != nil {
		return err
	}
	flow, err := selectFlow(compiled.Flows, flowName)
	if err != nil {
		return err
	}
	if flow.Journal == nil {
		return fmt.Errorf("flow %s has no journal", flow.Name)
	}
	if err := flow.Journal.Open(ctx); err != nil {
		return err
	}
	defer flow.Journal.Close(ctx)
	emitter := destinationMap(flow)[journal.DestinationID(destination.Value.String())]
	if emitter == nil {
		return fmt.Errorf("destination %s is not configured", destination.Value.String())
	}
	if !dryRun {
		if err := emitter.Open(ctx); err != nil {
			return err
		}
		defer emitter.Close(ctx)
	}
	iter, err := flow.Journal.Query(ctx, journal.ReplayFilter{
		Flow:        flow.Name,
		Destination: journal.DestinationID(destination.Value.String()),
		State:       journal.State(state),
		EventID:     eventID,
		Limit:       limit,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	count := 0
	for {
		record, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if dryRun {
			fmt.Fprintf(stdout, "%d %s %s %s\n", record.ID, record.EventID, record.EventType, record.Source)
		} else if err := emitter.Emit(ctx, record.Event); err != nil {
			return err
		}
		count++
	}
	fmt.Fprintf(stdout, "replayed events: %d\n", count)
	return nil
}

func destinationMap(flow resource.Flow) map[journal.DestinationID]eventflow.Emitter {
	out := map[journal.DestinationID]eventflow.Emitter{}
	for i, destination := range flow.Destinations {
		if i < len(flow.Emitters) {
			out[destination] = flow.Emitters[i]
		}
	}
	return out
}

func selectFlow(flows []resource.Flow, name string) (resource.Flow, error) {
	if name == "" {
		if len(flows) != 1 {
			return resource.Flow{}, fmt.Errorf("--flow is required when %d flows are compiled", len(flows))
		}
		return flows[0], nil
	}
	for _, flow := range flows {
		if flow.Name == name {
			return flow, nil
		}
	}
	return resource.Flow{}, fmt.Errorf("flow %q not found", name)
}

func flagValue(flags *flag.FlagSet, name string) string {
	flag := flags.Lookup(name)
	if flag == nil {
		return ""
	}
	return flag.Value.String()
}

func runObservationFlow(ctx context.Context, flow resource.ObservationFlow) error {
	for _, emitter := range flow.Emitters {
		if err := emitter.Open(ctx); err != nil {
			return err
		}
		defer emitter.Close(ctx)
	}
	if flow.InvalidEmitter != nil {
		if err := flow.InvalidEmitter.Open(ctx); err != nil {
			return err
		}
		defer flow.InvalidEmitter.Close(ctx)
	}
	return flow.Runtime.Run(ctx)
}

type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprint([]string(*m)) }
func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}

func graphNodes(graph *resource.Graph) []resource.ResourceKey {
	// Graph intentionally exposes validation results without its internal edges.
	// Re-validating order is stable through inspect output by using compiled docs.
	return graph.Nodes()
}

func usage(w io.Writer) {
	fmt.Fprint(w, `eventflow validates and runs declarative Eventflow resources.

Usage:
  eventflow validate --config resources.yaml
  eventflow inspect --config resources.yaml
  eventflow run --config resources.yaml

`)
}
