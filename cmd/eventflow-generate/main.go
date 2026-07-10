// Package main provides the eventflow-generate command.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/project-datascape/internal/adapters/generator/registry"
	lineageadapters "github.com/rezarajan/project-datascape/internal/adapters/lineage"
	"github.com/rezarajan/project-datascape/internal/app/generate"
	"github.com/rezarajan/project-datascape/internal/lineage"
	"github.com/rezarajan/project-datascape/internal/ports/generator"
)

// main runs the generator command and exits with a process status code.
func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run parses command arguments and streams generated events to stdout.
func run(ctx context.Context, args []string, stdout *os.File, stderr *os.File) error {
	flags := flag.NewFlagSet("eventflow-generate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	generatorName := flags.String("generator", envString("EVENTFLOW_GENERATOR", envString("DATASCAPE_GENERATOR", "")), "registered generator name")
	runID := flags.String("run-id", envString("EVENTFLOW_RUN_ID", envString("DATASCAPE_RUN_ID", defaultRunID())), "generation run id")
	seed := flags.Int64("seed", envInt64("EVENTFLOW_SEED", envInt64("DATASCAPE_SEED", 42)), "deterministic generator seed")
	source := flags.String("source", envString("EVENTFLOW_EVENT_SOURCE", envString("DATASCAPE_EVENT_SOURCE", "urn:eventflow:generate")), "CloudEvents source")
	params := multiValueFlag{}
	flags.Var(&params, "param", "generator parameter as key=value; may be repeated")
	flags.Usage = func() {
		fmt.Fprint(stderr, `eventflow-generate streams CloudEvents JSONL from a registered generator.

Usage:
  eventflow-generate --generator name [--param key=value]...

Generators are extension points. The core runtime does not compile in a domain
generator by default; example or downstream modules should register their own
generator implementations.

Data is written to stdout as one CloudEvent JSON document per line. Logs and
lineage diagnostics go to stderr.

Common environment:
  EVENTFLOW_GENERATOR        Default generator name.
  EVENTFLOW_RUN_ID           Stable run id for lineage.
  EVENTFLOW_SEED             Deterministic generator seed.
  EVENTFLOW_EVENT_SOURCE     CloudEvents source value.
  EVENTFLOW_LINEAGE_OUTPUT   noop, file, or marquez.
  EVENTFLOW_MARQUEZ_URL      Marquez API URL when lineage output is marquez.

Example:
  eventflow-generate --generator domain-generator --param count=10

Flags:
`)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{}))
	factory, err := registry.Defaults()
	if err != nil {
		return err
	}
	if *generatorName == "" {
		return fmt.Errorf("generator is required; available generators: %v", factory.Names())
	}
	service := generate.Service{Factory: factory, Logger: logger, Buffer: 128}
	request := generate.Request{
		Generator: *generatorName,
		Config:    generator.Config{RunID: *runID, Seed: *seed, Parameters: params.Map()},
		Source:    *source,
	}
	emitter, lineageConfig, err := lineageFromEnv()
	if err != nil {
		return err
	}
	outputs := []lineage.Dataset{{Namespace: "eventflow-generate", Name: "stdout/events"}}
	if err := emitter.Emit(ctx, lineage.NewEvent("START", lineageConfig.Namespace, "eventflow-generate", *runID, nil, outputs, nil, time.Now)); err != nil {
		return err
	}
	_, runErr := service.Run(ctx, request, stdout)
	eventType := "COMPLETE"
	if runErr != nil {
		eventType = "FAIL"
	}
	if err := emitter.Emit(ctx, lineage.NewEvent(eventType, lineageConfig.Namespace, "eventflow-generate", *runID, nil, outputs, runErr, time.Now)); err != nil {
		return err
	}
	return runErr
}

// lineageFromEnv constructs the configured lineage emitter and config.
func lineageFromEnv() (lineage.Emitter, lineage.Config, error) {
	config := lineage.FromEnv()
	emitter, err := lineageadapters.NewEmitter(config)
	return emitter, config, err
}

// multiValueFlag stores repeated key=value flag entries.
type multiValueFlag []string

// String returns the raw flag values as a comma-separated string.
func (m *multiValueFlag) String() string {
	return strings.Join(*m, ",")
}

// Set appends one key=value parameter entry.
func (m *multiValueFlag) Set(value string) error {
	if !strings.Contains(value, "=") {
		return fmt.Errorf("parameter must be key=value")
	}
	*m = append(*m, value)
	return nil
}

// Map returns parsed parameter values with simple scalar type inference.
func (m *multiValueFlag) Map() map[string]any {
	out := map[string]any{}
	for _, item := range *m {
		key, value, _ := strings.Cut(item, "=")
		out[key] = inferScalar(value)
	}
	return out
}

// inferScalar converts a string flag value into a basic JSON-compatible scalar.
func inferScalar(value string) any {
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsed
	}
	if parsed, err := strconv.ParseFloat(value, 64); err == nil {
		return parsed
	}
	if parsed, err := strconv.ParseBool(value); err == nil {
		return parsed
	}
	return value
}

// envString returns an environment variable value or a fallback.
func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// envInt64 returns an int64 environment variable value or a fallback.
func envInt64(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

// defaultRunID returns a compact default run identifier.
func defaultRunID() string {
	return "run-" + time.Now().UTC().Format("20060102T150405Z")
}
