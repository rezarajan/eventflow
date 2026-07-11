// Package main provides the eventflow-consume command.
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

	consumeadapters "github.com/rezarajan/eventflow/internal/adapters/consume"
	redpandasource "github.com/rezarajan/eventflow/internal/adapters/consume/redpanda"
	lineageadapters "github.com/rezarajan/eventflow/internal/adapters/lineage"
	"github.com/rezarajan/eventflow/internal/app/consume"
	"github.com/rezarajan/eventflow/internal/lineage"
	port "github.com/rezarajan/eventflow/internal/ports/consume"
)

// main runs the consumer command and exits with a process status code.
func main() {
	if err := run(context.Background(), os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run parses command arguments and consumes events from the configured source.
func run(ctx context.Context, args []string, stderr *os.File) error {
	flags := flag.NewFlagSet("eventflow-consume", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sourceName := flags.String("source", envString("EVENTFLOW_CONSUME_SOURCE", envString("DATASCAPE_CONSUME_SOURCE", redpandasource.Name)), "event source name")
	handlersList := flags.String("handlers", envString("EVENTFLOW_CONSUME_HANDLERS", envString("DATASCAPE_CONSUME_HANDLERS", "jsonl")), "comma-separated handler names")
	runID := flags.String("run-id", envString("EVENTFLOW_RUN_ID", envString("DATASCAPE_RUN_ID", defaultRunID())), "consumer run id")
	batchSize := flags.Int("batch-size", envInt("EVENTFLOW_CONSUME_BATCH_SIZE", envInt("DATASCAPE_CONSUME_BATCH_SIZE", 100)), "maximum events to read and handle per batch")
	maxEvents := flags.Int("max-events", envInt("EVENTFLOW_CONSUME_MAX_EVENTS", envInt("DATASCAPE_CONSUME_MAX_EVENTS", 0)), "maximum events to consume before exiting; 0 means unbounded")
	flags.Usage = func() {
		fmt.Fprint(stderr, `eventflow-consume reads CloudEvents and applies configured handlers.

Usage:
  eventflow-consume [--source redpanda] [--handlers jsonl,duckdb] [--max-events 1]

Handlers:
  jsonl    Append events as JSON Lines.
  objects  Write event payload objects.
  duckdb   Project raw events, and typed tables when registry projection hints exist.

Common environment:
  EVENTFLOW_CONSUME_SOURCE       Source adapter name, default redpanda.
  EVENTFLOW_CONSUME_HANDLERS     Comma-separated handler names.
  EVENTFLOW_REDPANDA_BROKERS     Kafka broker list, default localhost:19092.
  EVENTFLOW_REDPANDA_TOPIC       Topic to consume when using redpanda.
  EVENTFLOW_REGISTRY             Registry used by registry-aware handlers.
  EVENTFLOW_DUCKDB_PATH          DuckDB database path for the duckdb handler.
  EVENTFLOW_LINEAGE_OUTPUT       noop, file, or marquez.
  EVENTFLOW_MARQUEZ_URL          Marquez API URL when lineage output is marquez.

Example:
  EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
  EVENTFLOW_REDPANDA_TOPIC=attendance.events.v1 \
  EVENTFLOW_CONSUME_HANDLERS=duckdb \
  eventflow-consume --max-events 1

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
	source, err := createSource(*sourceName)
	if err != nil {
		return err
	}
	registry, err := consumeadapters.Defaults()
	if err != nil {
		return err
	}
	handlers, err := createHandlers(registry, *handlersList)
	if err != nil {
		return err
	}
	emitter, lineageConfig, err := lineageFromEnv()
	if err != nil {
		return err
	}
	service := consume.Service{Source: source, Handlers: handlers, Logger: logger, Lineage: emitter, LineageNS: lineageConfig.Namespace, BatchSize: *batchSize, MaxEvents: *maxEvents}
	_, err = service.Run(ctx, *runID)
	return err
}

// createSource creates the configured event source.
func createSource(name string) (port.EventSource, error) {
	switch strings.TrimSpace(name) {
	case "", redpandasource.Name:
		return redpandasource.New(redpandasource.FromEnv()), nil
	default:
		return nil, fmt.Errorf("unknown event source %q; available sources: [%s]", name, redpandasource.Name)
	}
}

// createHandlers creates configured handlers from a comma-separated list.
func createHandlers(factory port.Factory, handlerList string) ([]port.EventHandler, error) {
	parts := strings.Split(handlerList, ",")
	handlers := make([]port.EventHandler, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		handler, err := factory.Create(name)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, handler)
	}
	if len(handlers) == 0 {
		return nil, fmt.Errorf("at least one handler must be configured")
	}
	return handlers, nil
}

// lineageFromEnv constructs the configured lineage emitter and config.
func lineageFromEnv() (lineage.Emitter, lineage.Config, error) {
	config := lineage.FromEnv()
	emitter, err := lineageadapters.NewEmitter(config)
	return emitter, config, err
}

// envString returns an environment variable value or a fallback.
func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// envInt returns an integer environment variable value or a fallback.
func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

// defaultRunID returns a compact default run identifier.
func defaultRunID() string {
	return "consume-" + time.Now().UTC().Format("20060102T150405Z")
}
