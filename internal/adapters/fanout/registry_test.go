package fanout

import (
	"context"
	"reflect"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	port "github.com/datascape/eventflow/internal/ports/fanout"
)

// TestRegistryRegistersCreatesAndSortsPublishers verifies normal registry behavior.
func TestRegistryRegistersCreatesAndSortsPublishers(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("z", func() port.Publisher { return fanoutTestPublisher{name: "z"} }); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := registry.Register("a", func() port.Publisher { return fanoutTestPublisher{name: "a"} }); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if got, want := registry.Names(), []string{"a", "z"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Names = %v, want %v", got, want)
	}
	publisher, err := registry.Create("a")
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if publisher.Name() != "a" {
		t.Fatalf("publisher name = %q, want a", publisher.Name())
	}
}

// TestRegistryRejectsInvalidRegistration verifies invalid registry entries fail fast.
func TestRegistryRejectsInvalidRegistration(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("", func() port.Publisher { return fanoutTestPublisher{name: "x"} }); err == nil {
		t.Fatal("expected missing name error")
	}
	if err := registry.Register("x", nil); err == nil {
		t.Fatal("expected missing constructor error")
	}
	if err := registry.Register("x", func() port.Publisher { return fanoutTestPublisher{name: "x"} }); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := registry.Register("x", func() port.Publisher { return fanoutTestPublisher{name: "x2"} }); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

// TestRegistryRejectsUnknownPublisher verifies unknown output names include available outputs.
func TestRegistryRejectsUnknownPublisher(t *testing.T) {
	registry := NewRegistry()
	_ = registry.Register("known", func() port.Publisher { return fanoutTestPublisher{name: "known"} })
	_, err := registry.Create("missing")
	if err == nil || !strings.Contains(err.Error(), "unknown output") || !strings.Contains(err.Error(), "known") {
		t.Fatalf("expected unknown output error, got %v", err)
	}
}

// fanoutTestPublisher is a minimal publisher used by registry tests.
type fanoutTestPublisher struct{ name string }

// Name returns the test publisher name.
func (p fanoutTestPublisher) Name() string { return p.name }

// Open satisfies the publisher lifecycle for registry tests.
func (p fanoutTestPublisher) Open(ctx context.Context) error { return nil }

// Publish satisfies the publisher interface for registry tests.
func (p fanoutTestPublisher) Publish(ctx context.Context, event cloudevents.Event) error { return nil }

// Close satisfies the publisher lifecycle for registry tests.
func (p fanoutTestPublisher) Close(ctx context.Context) error { return nil }
