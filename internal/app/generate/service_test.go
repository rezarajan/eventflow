package generate

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/lakehouse-poc/internal/contracts/event"
	"github.com/datascape/lakehouse-poc/internal/ports/generator"
	"github.com/datascape/lakehouse-poc/internal/testkit/fakes"
)

// TestServiceUsesGenericGenerator verifies that the service depends on the generator port, not a concrete domain generator.
func TestServiceUsesGenericGenerator(t *testing.T) {
	fixed := time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC)
	gen := fakes.Generator{GeneratorName: "any.domain.v1", Facts: []event.Fact{{Kind: "thing.created.v1", Subject: "thing-1", Data: map[string]any{"thing_id": "thing-1"}}}}
	factory := fakes.GeneratorFactory{Generators: map[string]generator.Port{"any.domain.v1": gen}}
	var stdout bytes.Buffer
	var logs bytes.Buffer
	service := Service{Factory: factory, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Now: func() time.Time { return fixed }}
	summary, err := service.Run(context.Background(), Request{Generator: "any.domain.v1", Config: generator.Config{RunID: "run-1", Seed: 1}, Source: "urn:test"}, &stdout)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if summary.Events != 1 || summary.Facts != 1 || summary.ByType["thing.created.v1"] != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	events := make(chan cloudevents.Event, 1)
	if err := event.DecodeJSONL(context.Background(), &stdout, events); err != nil {
		t.Fatalf("decode generated event: %v", err)
	}
	evt := <-events
	if evt.Type() != "thing.created.v1" || evt.Source() != "urn:test" {
		t.Fatalf("unexpected event: type=%s source=%s", evt.Type(), evt.Source())
	}
	if !bytes.Contains(logs.Bytes(), []byte("generation_completed")) {
		t.Fatal("expected structured completion log")
	}
}

// TestServiceRequiresFactory verifies a generator factory is mandatory.
func TestServiceRequiresFactory(t *testing.T) {
	_, err := Service{}.Run(context.Background(), Request{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "factory is required") {
		t.Fatalf("expected factory error, got %v", err)
	}
}

// TestServiceReturnsUnknownGeneratorError verifies factory errors are surfaced.
func TestServiceReturnsUnknownGeneratorError(t *testing.T) {
	factory := fakes.GeneratorFactory{Generators: map[string]generator.Port{}}
	_, err := Service{Factory: factory}.Run(context.Background(), Request{Generator: "missing", Config: generator.Config{RunID: "run-1"}}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "unknown fake generator") {
		t.Fatalf("expected unknown generator error, got %v", err)
	}
}

// TestServiceReturnsGeneratorErrors verifies generator execution errors are surfaced.
func TestServiceReturnsGeneratorErrors(t *testing.T) {
	gen := fakes.Generator{GeneratorName: "broken", Err: fmt.Errorf("generator failed")}
	factory := fakes.GeneratorFactory{Generators: map[string]generator.Port{"broken": gen}}
	_, err := Service{Factory: factory}.Run(context.Background(), Request{Generator: "broken", Config: generator.Config{RunID: "run-1"}}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "generate facts") {
		t.Fatalf("expected generator error, got %v", err)
	}
}

// TestServiceReturnsInvalidFactErrors verifies invalid facts are rejected before serialization.
func TestServiceReturnsInvalidFactErrors(t *testing.T) {
	gen := fakes.Generator{GeneratorName: "bad-fact", Facts: []event.Fact{{Subject: "missing-kind"}}}
	factory := fakes.GeneratorFactory{Generators: map[string]generator.Port{"bad-fact": gen}}
	_, err := Service{Factory: factory}.Run(context.Background(), Request{Generator: "bad-fact", Config: generator.Config{RunID: "run-1"}}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "map fact") {
		t.Fatalf("expected fact mapping error, got %v", err)
	}
}
