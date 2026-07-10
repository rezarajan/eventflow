// Package main provides the eventflow-ingress-http command.
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

	"github.com/rezarajan/project-datascape/internal/adapters/fanout/redpanda"
	"github.com/rezarajan/project-datascape/internal/adapters/ingest/httpapi"
	schemavalidator "github.com/rezarajan/project-datascape/internal/adapters/ingest/jsonschema"
	lineageadapters "github.com/rezarajan/project-datascape/internal/adapters/lineage"
	"github.com/rezarajan/project-datascape/internal/app/ingest"
	"github.com/rezarajan/project-datascape/internal/contracts/event"
	"github.com/rezarajan/project-datascape/internal/contracts/registry"
	"github.com/rezarajan/project-datascape/internal/lineage"
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
	flags := flag.NewFlagSet("eventflow-ingress-http", flag.ContinueOnError)
	flags.SetOutput(stderr)
	addr := flags.String("addr", envString("EVENTFLOW_INGRESS_HTTP_ADDR", envString("DATASCAPE_INGRESS_HTTP_ADDR", ":8080")), "HTTP listen address")
	registryPath := flags.String("registry", envString("EVENTFLOW_REGISTRY", envString("DATASCAPE_REGISTRY", "")), "event registry YAML path")
	maxBody := flags.Int64("max-body", envInt64("EVENTFLOW_INGRESS_MAX_BODY", envInt64("DATASCAPE_INGRESS_MAX_BODY", 1<<20)), "maximum request body size in bytes")
	flags.Usage = func() {
		fmt.Fprint(stderr, `eventflow-ingress-http accepts plain domain JSON and publishes CloudEvents.

Usage:
  eventflow-ingress-http --registry eventflow.yaml [--addr :8080]

Producer API:
  POST /v1/events/{event_type}
  Content-Type: application/json
  X-Eventflow-Subject: optional CloudEvents subject
  X-Correlation-ID: optional correlation id extension

The event_type path segment must exist in the registry. The request body is
validated against that event's JSON Schema, wrapped as a CloudEvent, and
published to the registered channel when EVENTFLOW_REDPANDA_TOPIC_MODE=registry.

Common environment:
  EVENTFLOW_REGISTRY              Default registry YAML path.
  EVENTFLOW_INGRESS_HTTP_ADDR     Default listen address.
  EVENTFLOW_REDPANDA_BROKERS      Kafka broker list, default localhost:19092.
  EVENTFLOW_REDPANDA_TOPIC_MODE   Use registry for per-event channels.
  EVENTFLOW_LINEAGE_OUTPUT        noop, file, or marquez.
  EVENTFLOW_MARQUEZ_URL           Marquez API URL when lineage output is marquez.

Example:
  EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
  EVENTFLOW_REDPANDA_TOPIC_MODE=registry \
  eventflow-ingress-http --addr :8080

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
	loadedRegistry, err := registry.Load(*registryPath)
	if err != nil {
		return err
	}
	redpandaConfig := redpanda.FromEnv()
	redpandaConfig.RegistryPath = *registryPath
	redpandaConfig.TopicMode = envString("EVENTFLOW_REDPANDA_TOPIC_MODE", envString("DATASCAPE_REDPANDA_TOPIC_MODE", "registry"))
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
		Registry:  loadedRegistry,
		Factory:   event.NewFactory("", "urn:eventflow:ingress:http", time.Now),
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
