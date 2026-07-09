// Package main provides the datascape-fanout command.
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

	fanoutadapters "github.com/datascape/lakehouse-poc/internal/adapters/fanout"
	"github.com/datascape/lakehouse-poc/internal/adapters/fanout/redpanda"
	lineageadapters "github.com/datascape/lakehouse-poc/internal/adapters/lineage"
	"github.com/datascape/lakehouse-poc/internal/app/fanout"
	"github.com/datascape/lakehouse-poc/internal/lineage"
	port "github.com/datascape/lakehouse-poc/internal/ports/fanout"
)

// main runs the fan-out command and exits with a process status code.
func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run parses command arguments and publishes input events to configured outputs.
func run(ctx context.Context, args []string, stdin *os.File, stdout *os.File, stderr *os.File) error {
	flags := flag.NewFlagSet("datascape-fanout", flag.ContinueOnError)
	flags.SetOutput(stderr)
	outputs := flags.String("outputs", envString("DATASCAPE_OUTPUTS", "log"), "comma-separated output adapter names")
	runID := flags.String("run-id", envString("DATASCAPE_RUN_ID", defaultRunID()), "fan-out run id")
	batchSize := flags.Int("batch-size", envInt("DATASCAPE_FANOUT_BATCH_SIZE", 100), "maximum event batch size for batch-capable outputs")
	if err := flags.Parse(args); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{}))
	registry, err := fanoutadapters.Defaults(stdout, logger)
	if err != nil {
		return err
	}
	publishers, err := createPublishers(registry, *outputs)
	if err != nil {
		return err
	}
	service := fanout.Service{Publishers: publishers, Logger: logger, Buffer: 256, BatchSize: *batchSize}
	emitter, lineageConfig, err := lineageFromEnv()
	if err != nil {
		return err
	}
	inputs := []lineage.Dataset{{Namespace: "datascape-generate", Name: "stdout/events"}}
	outputDatasets := outputDatasets(*outputs)
	if err := emitter.Emit(ctx, lineage.NewEvent("START", lineageConfig.Namespace, "datascape-fanout", *runID, inputs, outputDatasets, nil, time.Now)); err != nil {
		return err
	}
	_, runErr := service.Run(ctx, *runID, stdin)
	eventType := "COMPLETE"
	if runErr != nil {
		eventType = "FAIL"
	}
	if err := emitter.Emit(ctx, lineage.NewEvent(eventType, lineageConfig.Namespace, "datascape-fanout", *runID, inputs, outputDatasets, runErr, time.Now)); err != nil {
		return err
	}
	return runErr
}

// createPublishers creates configured publisher adapters from a comma-separated output list.
func createPublishers(factory port.Factory, outputList string) ([]port.Publisher, error) {
	parts := strings.Split(outputList, ",")
	publishers := make([]port.Publisher, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		publisher, err := factory.Create(name)
		if err != nil {
			return nil, err
		}
		publishers = append(publishers, publisher)
	}
	if len(publishers) == 0 {
		return nil, fmt.Errorf("at least one output must be configured")
	}
	return publishers, nil
}

// outputDatasets returns stable lineage datasets for configured fan-out outputs.
func outputDatasets(outputList string) []lineage.Dataset {
	parts := strings.Split(outputList, ",")
	datasets := make([]lineage.Dataset, 0, len(parts))
	for _, part := range parts {
		switch strings.TrimSpace(part) {
		case redpanda.Name:
			config := redpanda.FromEnv()
			datasets = append(datasets, lineage.RedpandaDataset(config.Brokers, config.Topic))
		case "log":
			datasets = append(datasets, lineage.Dataset{Namespace: "log", Name: "stdout/events"})
		case "stdout":
			datasets = append(datasets, lineage.Dataset{Namespace: "datascape-fanout", Name: "stdout/events"})
		}
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
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

// defaultRunID returns a compact default run identifier.
func defaultRunID() string {
	return "fanout-" + time.Now().UTC().Format("20060102T150405Z")
}
