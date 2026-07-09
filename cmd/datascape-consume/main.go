// Package main provides the datascape-consume command.
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

	consumeadapters "github.com/datascape/lakehouse-poc/internal/adapters/consume"
	redpandasource "github.com/datascape/lakehouse-poc/internal/adapters/consume/redpanda"
	lineageadapters "github.com/datascape/lakehouse-poc/internal/adapters/lineage"
	"github.com/datascape/lakehouse-poc/internal/app/consume"
	"github.com/datascape/lakehouse-poc/internal/lineage"
	port "github.com/datascape/lakehouse-poc/internal/ports/consume"
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
	flags := flag.NewFlagSet("datascape-consume", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sourceName := flags.String("source", envString("DATASCAPE_CONSUME_SOURCE", redpandasource.Name), "event source name")
	handlersList := flags.String("handlers", envString("DATASCAPE_CONSUME_HANDLERS", "jsonl,objects"), "comma-separated handler names")
	runID := flags.String("run-id", envString("DATASCAPE_RUN_ID", defaultRunID()), "consumer run id")
	batchSize := flags.Int("batch-size", envInt("DATASCAPE_CONSUME_BATCH_SIZE", 100), "maximum events to read and handle per batch")
	maxEvents := flags.Int("max-events", envInt("DATASCAPE_CONSUME_MAX_EVENTS", 0), "maximum events to consume before exiting; 0 means unbounded")
	if err := flags.Parse(args); err != nil {
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
	inputs := []lineage.Dataset{source.Dataset()}
	outputs := handlerDatasets(handlers)
	if err := emitter.Emit(ctx, lineage.NewEvent("START", lineageConfig.Namespace, "datascape-consume", *runID, inputs, outputs, nil, time.Now)); err != nil {
		return err
	}
	service := consume.Service{Source: source, Handlers: handlers, Logger: logger, Lineage: emitter, LineageNS: lineageConfig.Namespace, BatchSize: *batchSize, MaxEvents: *maxEvents}
	_, runErr := service.Run(ctx, *runID)
	eventType := "COMPLETE"
	if runErr != nil {
		eventType = "FAIL"
	}
	if err := emitter.Emit(ctx, lineage.NewEvent(eventType, lineageConfig.Namespace, "datascape-consume", *runID, inputs, outputs, runErr, time.Now)); err != nil {
		return err
	}
	return runErr
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

// handlerDatasets returns stable datasets for configured handlers.
func handlerDatasets(handlers []port.EventHandler) []lineage.Dataset {
	datasets := make([]lineage.Dataset, 0, len(handlers))
	for _, handler := range handlers {
		datasets = append(datasets, handler.Dataset())
	}
	return datasets
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
