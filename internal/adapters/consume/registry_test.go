package consume

import (
	"context"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/eventflow/internal/lineage"
	port "github.com/datascape/eventflow/internal/ports/consume"
)

// TestRegistryCreatesHandlers verifies registered constructors are used by name.
func TestRegistryCreatesHandlers(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("fake", func() port.EventHandler { return fakeRegistryHandler{name: "fake"} }); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	handler, err := registry.Create("fake")
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if handler.Name() != "fake" {
		t.Fatalf("handler name = %q, want fake", handler.Name())
	}
}

// TestRegistryRejectsUnknownHandlers verifies unknown handler names include available options.
func TestRegistryRejectsUnknownHandlers(t *testing.T) {
	registry := NewRegistry()
	err := registry.Register("fake", func() port.EventHandler { return fakeRegistryHandler{name: "fake"} })
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	_, err = registry.Create("missing")
	if err == nil || !strings.Contains(err.Error(), "available handlers") {
		t.Fatalf("expected unknown handler error, got %v", err)
	}
}

// fakeRegistryHandler is a minimal registry test handler.
type fakeRegistryHandler struct {
	name string
}

// Name returns the fake handler name.
func (h fakeRegistryHandler) Name() string {
	return h.name
}

// Dataset returns the fake handler dataset.
func (h fakeRegistryHandler) Dataset() lineage.Dataset {
	return lineage.Dataset{Namespace: "fake", Name: h.name}
}

// Open opens the fake handler.
func (h fakeRegistryHandler) Open(ctx context.Context) error {
	return ctx.Err()
}

// Handle accepts one fake event.
func (h fakeRegistryHandler) Handle(ctx context.Context, event cloudevents.Event) error {
	return ctx.Err()
}

// Close closes the fake handler.
func (h fakeRegistryHandler) Close(ctx context.Context) error {
	return ctx.Err()
}
