// Package main provides a thin Eventflow relay command.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/rezarajan/project-datascape/filesystem"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("eventflow-relay", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	in := flags.String("in", envString("EVENTFLOW_RELAY_IN", "-"), "input filesystem path")
	out := flags.String("out", envString("EVENTFLOW_RELAY_OUT", "-"), "output filesystem path")
	mode := flags.String("mode", envString("EVENTFLOW_FILESYSTEM_MODE", "ndjson"), "filesystem mode: ndjson or files")
	if err := flags.Parse(args); err != nil {
		return err
	}
	receiver := filesystem.NewReceiver(filesystem.Config{Path: *in, Mode: filesystem.Mode(*mode)})
	emitter := filesystem.NewEmitter(filesystem.Config{Path: *out, Mode: filesystem.Mode(*mode), Atomic: true})
	if err := receiver.Open(ctx); err != nil {
		return err
	}
	defer receiver.Close(ctx)
	if err := emitter.Open(ctx); err != nil {
		return err
	}
	defer emitter.Close(ctx)
	for {
		event, err := receiver.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := emitter.Emit(ctx, event); err != nil {
			return err
		}
	}
}

func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
