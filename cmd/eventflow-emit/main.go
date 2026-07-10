// Package main provides a thin Eventflow emitter command.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	eventflow "github.com/rezarajan/project-datascape"
	"github.com/rezarajan/project-datascape/cloudevent"
	"github.com/rezarajan/project-datascape/filesystem"
	"github.com/rezarajan/project-datascape/httpflow"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("eventflow-emit", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	adapter := flags.String("adapter", envString("EVENTFLOW_EMIT_ADAPTER", "filesystem"), "emitter adapter: filesystem or http")
	path := flags.String("path", envString("EVENTFLOW_FILESYSTEM_PATH", "-"), "filesystem output path")
	mode := flags.String("mode", envString("EVENTFLOW_FILESYSTEM_MODE", "ndjson"), "filesystem mode: ndjson or files")
	url := flags.String("url", envString("EVENTFLOW_HTTP_URL", ""), "HTTP emitter URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	event, err := cloudevent.StructuredJSONCodec{}.Decode(ctx, os.Stdin)
	if err != nil {
		return err
	}
	emitter, err := newEmitter(*adapter, *path, *mode, *url)
	if err != nil {
		return err
	}
	if err := emitter.Open(ctx); err != nil {
		return err
	}
	defer emitter.Close(ctx)
	return emitter.Emit(ctx, event)
}

func newEmitter(adapter string, path string, mode string, url string) (eventflow.Emitter, error) {
	switch strings.ToLower(strings.TrimSpace(adapter)) {
	case "filesystem", "file", "stdout":
		return filesystem.NewEmitter(filesystem.Config{Path: path, Mode: filesystem.Mode(mode)}), nil
	case "http":
		return httpflow.NewEmitter(httpflow.EmitterConfig{URL: url, Mode: httpflow.ModeStructuredCloudEvents}), nil
	default:
		return nil, fmt.Errorf("unsupported emitter adapter %q", adapter)
	}
}

func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
