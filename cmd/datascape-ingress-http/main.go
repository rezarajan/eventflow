// Package main provides the datascape-ingress-http command.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/datascape/lakehouse-poc/internal/adapters/fanout/redpanda"
	"github.com/datascape/lakehouse-poc/internal/adapters/ingest/httpapi"
	schemavalidator "github.com/datascape/lakehouse-poc/internal/adapters/ingest/jsonschema"
	lineageadapters "github.com/datascape/lakehouse-poc/internal/adapters/lineage"
	"github.com/datascape/lakehouse-poc/internal/app/ingest"
	"github.com/datascape/lakehouse-poc/internal/contracts/event"
	"github.com/datascape/lakehouse-poc/internal/lineage"
)

// main runs the HTTP ingress command and exits with a process status code.
func main() {
	if err := run(context.Background(), os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run parses command arguments and serves the producer-facing HTTP ingress API.
func run(ctx context.Context, args []string, stderr *os.File) error {
	flags := flag.NewFlagSet("datascape-ingress-http", flag.ContinueOnError)
	flags.SetOutput(stderr)
	addr := flags.String("addr", envString("DATASCAPE_INGRESS_HTTP_ADDR", ":8080"), "HTTP listen address")
	maxBody := flags.Int64("max-body", envInt64("DATASCAPE_INGRESS_MAX_BODY", 1<<20), "maximum request body size in bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{}))
	redpandaConfig := redpanda.FromEnv()
	redpandaConfig.TopicMode = envString("DATASCAPE_REDPANDA_TOPIC_MODE", "catalog")
	publisher := redpanda.New(redpandaConfig)
	if err := publisher.Open(ctx); err != nil {
		return err
	}
	defer publisher.Close(context.Background())
	emitter, lineageConfig, err := lineageFromEnv()
	if err != nil {
		return err
	}
	service := ingest.Service{
		Catalog:   event.DefaultCatalog(),
		Factory:   event.NewFactory("", "urn:datascape:ingress:http", time.Now),
		Validator: schemavalidator.New(),
		Publisher: publisher,
		Lineage:   emitter,
		LineageNS: lineageConfig.Namespace,
		Logger:    logger,
		Now:       time.Now,
	}
	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.Handler{Service: service, MaxBody: *maxBody},
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("ingress_http_started", "addr", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errCh
	case err := <-errCh:
		return err
	}
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

// envInt64 returns an integer environment variable value or a fallback.
func envInt64(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
