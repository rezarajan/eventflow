package registry

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/rezarajan/eventflow/internal/contracts/event"
	"github.com/rezarajan/eventflow/internal/ports/generator"
)

// TestRegistryRegistersCreatesAndSortsGenerators verifies normal generator registry behavior.
func TestRegistryRegistersCreatesAndSortsGenerators(t *testing.T) {
	registry := New()
	if err := registry.Register("z", func() generator.Port { return generatorTestPort{name: "z"} }); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := registry.Register("a", func() generator.Port { return generatorTestPort{name: "a"} }); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if got, want := registry.Names(), []string{"a", "z"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Names = %v, want %v", got, want)
	}
	gen, err := registry.Create("a")
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if gen.Name() != "a" {
		t.Fatalf("generator name = %q, want a", gen.Name())
	}
}

// TestRegistryRejectsInvalidRegistration verifies invalid generator registry entries fail fast.
func TestRegistryRejectsInvalidRegistration(t *testing.T) {
	registry := New()
	if err := registry.Register("", func() generator.Port { return generatorTestPort{name: "x"} }); err == nil {
		t.Fatal("expected missing name error")
	}
	if err := registry.Register("x", nil); err == nil {
		t.Fatal("expected missing constructor error")
	}
	if err := registry.Register("x", func() generator.Port { return generatorTestPort{name: "x"} }); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := registry.Register("x", func() generator.Port { return generatorTestPort{name: "x2"} }); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

// TestRegistryRejectsUnknownGenerator verifies unknown generator names include available values.
func TestRegistryRejectsUnknownGenerator(t *testing.T) {
	registry := New()
	_ = registry.Register("known", func() generator.Port { return generatorTestPort{name: "known"} })
	_, err := registry.Create("missing")
	if err == nil || !strings.Contains(err.Error(), "unknown generator") || !strings.Contains(err.Error(), "known") {
		t.Fatalf("expected unknown generator error, got %v", err)
	}
}

// TestDefaultsStartsEmpty verifies core defaults do not register domain-specific generators.
func TestDefaultsStartsEmpty(t *testing.T) {
	registry, err := Defaults()
	if err != nil {
		t.Fatalf("Defaults returned error: %v", err)
	}
	if len(registry.Names()) != 0 {
		t.Fatalf("unexpected defaults: %v", registry.Names())
	}
}

// generatorTestPort is a minimal generator for registry tests.
type generatorTestPort struct{ name string }

// Name returns the generator test name.
func (p generatorTestPort) Name() string { return p.name }

// Generate closes the output channel without emitting facts.
func (p generatorTestPort) Generate(ctx context.Context, config generator.Config, out chan<- event.Fact) error {
	close(out)
	return nil
}
