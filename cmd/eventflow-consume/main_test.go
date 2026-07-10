package main

import (
	"context"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/eventflow/internal/lineage"
	port "github.com/datascape/eventflow/internal/ports/consume"
)

// TestCreateHandlersParsesCommaSeparatedHandlers verifies CLI handler selection uses the registry only.
func TestCreateHandlersParsesCommaSeparatedHandlers(t *testing.T) {
	factory := fakeConsumeFactory{handlers: map[string]port.EventHandler{"a": &fakeHandler{name: "a"}, "b": &fakeHandler{name: "b"}}}
	handlers, err := createHandlers(factory, " a, b ")
	if err != nil {
		t.Fatalf("createHandlers returned error: %v", err)
	}
	if len(handlers) != 2 || handlers[0].Name() != "a" || handlers[1].Name() != "b" {
		t.Fatalf("unexpected handlers: %+v", handlers)
	}
}

// TestCreateHandlersRejectsEmptyList verifies an empty handler list fails fast.
func TestCreateHandlersRejectsEmptyList(t *testing.T) {
	_, err := createHandlers(fakeConsumeFactory{}, ", ,")
	if err == nil || !strings.Contains(err.Error(), "at least one handler") {
		t.Fatalf("expected empty handler error, got %v", err)
	}
}

// TestCreateSourceRejectsUnknownSource verifies unknown source names fail fast.
func TestCreateSourceRejectsUnknownSource(t *testing.T) {
	_, err := createSource("missing")
	if err == nil || !strings.Contains(err.Error(), "unknown event source") {
		t.Fatalf("expected unknown source error, got %v", err)
	}
}

// TestHandlerDatasetsReturnsConfiguredHandlerDatasets verifies consume output lineage uses handler datasets.
func TestHandlerDatasetsReturnsConfiguredHandlerDatasets(t *testing.T) {
	datasets := handlerDatasets([]port.EventHandler{&fakeHandler{name: "jsonl"}, &fakeHandler{name: "objects"}})
	if len(datasets) != 2 || datasets[0].Name != "jsonl" || datasets[1].Name != "objects" {
		t.Fatalf("unexpected handler datasets: %+v", datasets)
	}
}

// fakeConsumeFactory is an in-memory handler factory for command tests.
type fakeConsumeFactory struct {
	handlers map[string]port.EventHandler
}

// Names returns configured fake handler names.
func (f fakeConsumeFactory) Names() []string {
	return nil
}

// Create returns a configured fake handler by name.
func (f fakeConsumeFactory) Create(name string) (port.EventHandler, error) {
	if handler, ok := f.handlers[name]; ok {
		return handler, nil
	}
	return nil, fakeError("unknown fake handler " + name)
}

// fakeHandler is a minimal event handler for command tests.
type fakeHandler struct {
	name string
}

// Name returns the fake handler name.
func (h *fakeHandler) Name() string {
	return h.name
}

// Dataset returns the fake handler dataset.
func (h *fakeHandler) Dataset() lineage.Dataset {
	return lineage.Dataset{Namespace: "fake", Name: h.name}
}

// Open opens the fake handler.
func (h *fakeHandler) Open(ctx context.Context) error {
	return ctx.Err()
}

// Handle accepts one fake event.
func (h *fakeHandler) Handle(ctx context.Context, event cloudevents.Event) error {
	return ctx.Err()
}

// Close closes the fake handler.
func (h *fakeHandler) Close(ctx context.Context) error {
	return ctx.Err()
}

// fakeError is a minimal error type that avoids additional dependencies in command tests.
type fakeError string

// Error returns the fake error text.
func (e fakeError) Error() string {
	return string(e)
}
