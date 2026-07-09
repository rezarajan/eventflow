// Package main provides the datascape-generate command.
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

	"github.com/datascape/lakehouse-poc/internal/adapters/generator/registry"
	lineageadapters "github.com/datascape/lakehouse-poc/internal/adapters/lineage"
	"github.com/datascape/lakehouse-poc/internal/app/generate"
	"github.com/datascape/lakehouse-poc/internal/lineage"
	"github.com/datascape/lakehouse-poc/internal/ports/generator"
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
	flags := flag.NewFlagSet("datascape-generate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	generatorName := flags.String("generator", envString("DATASCAPE_GENERATOR", ""), "registered generator name")
	runID := flags.String("run-id", envString("DATASCAPE_RUN_ID", defaultRunID()), "generation run id")
	seed := flags.Int64("seed", envInt64("DATASCAPE_SEED", 42), "deterministic generator seed")
	source := flags.String("source", envString("DATASCAPE_EVENT_SOURCE", "urn:datascape:generate"), "CloudEvents source")
	params := multiValueFlag{}
	flags.Var(&params, "param", "generator parameter as key=value; may be repeated")
	if err := flags.Parse(args); err != nil {
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
	outputs := []lineage.Dataset{{Namespace: "datascape-generate", Name: "stdout/events"}}
	if err := emitter.Emit(ctx, lineage.NewEvent("START", lineageConfig.Namespace, "datascape-generate", *runID, nil, outputs, nil, time.Now)); err != nil {
		return err
	}
	_, runErr := service.Run(ctx, request, stdout)
	eventType := "COMPLETE"
	if runErr != nil {
		eventType = "FAIL"
	}
	if err := emitter.Emit(ctx, lineage.NewEvent(eventType, lineageConfig.Namespace, "datascape-generate", *runID, nil, outputs, runErr, time.Now)); err != nil {
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
