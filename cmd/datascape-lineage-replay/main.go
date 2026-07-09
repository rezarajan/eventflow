// Package main provides the datascape-lineage-replay command.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	lineageadapters "github.com/datascape/lakehouse-poc/internal/adapters/lineage"
	"github.com/datascape/lakehouse-poc/internal/adapters/lineage/ndjson"
	"github.com/datascape/lakehouse-poc/internal/lineage"
)

// main runs the lineage replay command and exits with a process status code.
func main() {
	if err := run(context.Background(), os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run parses command arguments and replays lineage events to the configured emitter.
func run(ctx context.Context, args []string, stderr *os.File) error {
	flags := flag.NewFlagSet("datascape-lineage-replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	file := flags.String("file", envString("DATASCAPE_LINEAGE_FILE", "var/datascape/lineage/openlineage.ndjson"), "OpenLineage NDJSON file to replay")
	limit := flags.Int("limit", envInt("DATASCAPE_LINEAGE_REPLAY_LIMIT", 0), "maximum events to replay; 0 means all")
	if err := flags.Parse(args); err != nil {
		return err
	}
	reader, err := ndjson.NewFileReader(*file)
	if err != nil {
		return err
	}
	defer reader.Close()
	config := lineage.FromEnv()
	emitter, err := lineageadapters.NewEmitter(config)
	if err != nil {
		return err
	}
	return replay(ctx, reader, emitter, *limit)
}

// replay reads lineage events and emits them to a target emitter.
func replay(ctx context.Context, reader lineage.EventReader, emitter lineage.Emitter, limit int) error {
	count := 0
	for limit <= 0 || count < limit {
		event, err := reader.Read(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := emitter.Emit(ctx, event); err != nil {
			return err
		}
		count++
	}
	return nil
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
