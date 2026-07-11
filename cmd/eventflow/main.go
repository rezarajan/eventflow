package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rezarajan/eventflow/adapters/bundled"
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
	case "validate", "inspect", "run":
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
			return fmt.Errorf("no EventFlow resources compiled")
		}
		return runFlow(ctx, compiled.Flows[0])
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
	if flow.Runtime.Receiver != nil {
		return flow.Runtime.Run(ctx)
	}
	if flow.Observer == nil {
		return fmt.Errorf("flow has neither receiver nor observer")
	}
	if err := flow.Observer.Open(ctx); err != nil {
		return err
	}
	defer flow.Observer.Close(ctx)
	for {
		observation, err := flow.Observer.Observe(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if observation.Event == nil {
			continue
		}
		if flow.Runtime.Validator != nil {
			if err := flow.Runtime.Validator.Validate(ctx, *observation.Event, flow.Runtime.Mode); err != nil {
				if flow.InvalidEmitter != nil {
					if emitErr := flow.InvalidEmitter.Emit(ctx, *observation.Event); emitErr != nil {
						return emitErr
					}
					continue
				}
				return err
			}
		}
		if err := flow.Runtime.Handler.Handle(ctx, *observation.Event); err != nil {
			return err
		}
	}
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
