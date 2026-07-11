// Package main provides a thin Eventflow receiver command.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/cloudevent"
	"github.com/rezarajan/eventflow/filesystem"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("eventflow-receive", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	adapter := flags.String("adapter", envString("EVENTFLOW_RECEIVE_ADAPTER", "filesystem"), "receiver adapter: filesystem")
	path := flags.String("path", envString("EVENTFLOW_FILESYSTEM_PATH", "-"), "filesystem input path")
	mode := flags.String("mode", envString("EVENTFLOW_FILESYSTEM_MODE", "ndjson"), "filesystem mode: ndjson or files")
	maxEvents := flags.Int("max-events", envInt("EVENTFLOW_RECEIVE_MAX_EVENTS", 0), "maximum events to stream; 0 means all")
	if err := flags.Parse(args); err != nil {
		return err
	}
	receiver, err := newReceiver(*adapter, *path, *mode)
	if err != nil {
		return err
	}
	if err := receiver.Open(ctx); err != nil {
		return err
	}
	defer receiver.Close(ctx)
	codec := cloudevent.StructuredJSONCodec{}
	count := 0
	for *maxEvents <= 0 || count < *maxEvents {
		event, err := receiver.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := codec.Encode(ctx, os.Stdout, event); err != nil {
			return err
		}
		count++
	}
	return nil
}

func newReceiver(adapter string, path string, mode string) (eventflow.Receiver, error) {
	switch strings.ToLower(strings.TrimSpace(adapter)) {
	case "filesystem", "file", "stdin":
		return filesystem.NewReceiver(filesystem.Config{Path: path, Mode: filesystem.Mode(mode)}), nil
	default:
		return nil, fmt.Errorf("unsupported receiver adapter %q", adapter)
	}
}

func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	var value int
	if _, err := fmt.Sscanf(os.Getenv(key), "%d", &value); err == nil {
		return value
	}
	return fallback
}
