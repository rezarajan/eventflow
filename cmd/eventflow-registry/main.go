// Package main provides the eventflow-registry utility command.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/rezarajan/project-datascape/internal/contracts/registry"
	"gopkg.in/yaml.v3"
)

// main runs the registry utility and exits with a process status code.
func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run validates a registry or renders standards artifacts from it.
func run(ctx context.Context, args []string, stdout *os.File, stderr *os.File) error {
	_ = ctx
	if len(args) == 0 || args[0] == "help" || args[0] == "-help" || args[0] == "--help" {
		printUsage(stderr)
		return nil
	}
	command := args[0]
	switch command {
	case "validate", "asyncapi":
	default:
		return fmt.Errorf("unknown registry command %q; expected validate or asyncapi", command)
	}
	flags := flag.NewFlagSet("eventflow-registry "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	registryPath := flags.String("registry", envString("EVENTFLOW_REGISTRY", envString("DATASCAPE_REGISTRY", "")), "event registry YAML path")
	outputPath := flags.String("output", "-", "AsyncAPI output path; use - for stdout")
	flags.Usage = func() {
		printCommandUsage(stderr, command)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	loaded, err := registry.Load(*registryPath)
	if err != nil {
		return err
	}
	switch command {
	case "validate":
		if err := loaded.ValidateSchemas(); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "registry valid: %d events\n", len(loaded.Events))
		return nil
	case "asyncapi":
		writer, closeWriter, err := asyncAPIWriter(stdout, *outputPath)
		if err != nil {
			return err
		}
		defer closeWriter()
		encoder := yaml.NewEncoder(writer)
		defer encoder.Close()
		return encoder.Encode(asyncAPIDocument(loaded))
	default:
		return fmt.Errorf("unknown registry command %q", command)
	}
}

// asyncAPIDocument returns a minimal AsyncAPI document for registered channels.
func asyncAPIDocument(loaded registry.Registry) map[string]any {
	channels := map[string]any{}
	operations := map[string]any{}
	messages := map[string]any{}
	for _, eventType := range loaded.Types() {
		registered, _ := loaded.Lookup(eventType)
		messages[eventType] = map[string]any{
			"name": eventType,
			"payload": map[string]any{
				"$ref": registered.Schema,
			},
		}
		channel, _ := channels[registered.Channel].(map[string]any)
		if channel == nil {
			channel = map[string]any{
				"address":  registered.Channel,
				"messages": map[string]any{},
			}
			channels[registered.Channel] = channel
		}
		channelMessages := channel["messages"].(map[string]any)
		channelMessages[eventType] = map[string]any{"$ref": "#/components/messages/" + eventType}
		operations["publish."+eventType] = map[string]any{
			"action": "send",
			"channel": map[string]any{
				"$ref": "#/channels/" + registered.Channel,
			},
		}
	}
	return map[string]any{
		"asyncapi":           "3.1.0",
		"info":               map[string]any{"title": "Eventflow Domain Events", "version": "0.1.0"},
		"defaultContentType": "application/cloudevents+json",
		"channels":           channels,
		"operations":         operations,
		"components":         map[string]any{"messages": messages},
	}
}

// envString returns an environment variable value or a fallback.
func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// asyncAPIWriter returns the configured AsyncAPI destination.
func asyncAPIWriter(stdout *os.File, outputPath string) (io.Writer, func(), error) {
	if outputPath == "" || outputPath == "-" {
		return stdout, func() {}, nil
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, func() {}, fmt.Errorf("create AsyncAPI output %s: %w", outputPath, err)
	}
	return file, func() { _ = file.Close() }, nil
}

// printUsage writes top-level usage for the registry utility.
func printUsage(stderr *os.File) {
	fmt.Fprint(stderr, `eventflow-registry manages event registry contracts.

Usage:
  eventflow-registry validate --registry eventflow.yaml
  eventflow-registry asyncapi --registry eventflow.yaml [--output asyncapi.yaml|-]

Commands:
  validate   Validate registry structure and local schema file references.
  asyncapi   Render an AsyncAPI 3.1 document from registered event channels.

Environment:
  EVENTFLOW_REGISTRY   Default registry YAML path.

Run "eventflow-registry <command> -help" for command flags.
`)
}

// printCommandUsage writes usage for one registry subcommand.
func printCommandUsage(stderr *os.File, command string) {
	switch command {
	case "validate":
		fmt.Fprint(stderr, `Usage:
  eventflow-registry validate --registry eventflow.yaml

Validates the registry file, checks for duplicate event types, resolves relative
schema paths, and verifies that each local schema file exists. Remote schema
URIs are accepted without a network check.

Flags:
`)
	case "asyncapi":
		fmt.Fprint(stderr, `Usage:
  eventflow-registry asyncapi --registry eventflow.yaml [--output asyncapi.yaml|-]

Writes an AsyncAPI 3.1 document that describes the registered CloudEvents
messages and broker channels. The default output is stdout.

Flags:
`)
	default:
		fmt.Fprintf(stderr, "Usage:\n  eventflow-registry %s --registry eventflow.yaml\n\nFlags:\n", command)
	}
}
