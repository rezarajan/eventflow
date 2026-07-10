package registry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadResolvesRelativeSchemas(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "eventflow.yaml")
	body := []byte(`version: eventflow.registry.v1
events:
  - type: example.created.v1
    schema: ./schemas/example-created.v1.schema.json
    channel: example.events.v1
    projection:
      table: examples
`)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write registry: %v", err)
	}
	registry, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	event, ok := registry.Lookup("example.created.v1")
	if !ok {
		t.Fatal("event registration not found")
	}
	want := filepath.Join(dir, "schemas/example-created.v1.schema.json")
	if event.Schema != want {
		t.Fatalf("schema = %s, want %s", event.Schema, want)
	}
	if event.Projection.Table != "examples" {
		t.Fatalf("projection table = %s", event.Projection.Table)
	}
}

func TestNewRejectsDuplicateEventTypes(t *testing.T) {
	_, err := New([]Event{
		{Type: "example.created.v1", Schema: "a.json", Channel: "example.events.v1"},
		{Type: "example.created.v1", Schema: "b.json", Channel: "example.events.v1"},
	})
	if err == nil {
		t.Fatal("expected duplicate event type error")
	}
}

func TestValidateSchemasAcceptsExistingLocalFiles(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "example.schema.json")
	if err := os.WriteFile(schemaPath, []byte(`{"type":"object"}`), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	registry, err := New([]Event{
		{Type: "example.created.v1", Schema: schemaPath, Channel: "example.events.v1"},
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := registry.ValidateSchemas(); err != nil {
		t.Fatalf("ValidateSchemas returned error: %v", err)
	}
}

func TestValidateSchemasRejectsMissingLocalFiles(t *testing.T) {
	registry, err := New([]Event{
		{Type: "example.created.v1", Schema: filepath.Join(t.TempDir(), "missing.schema.json"), Channel: "example.events.v1"},
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := registry.ValidateSchemas(); err == nil {
		t.Fatal("expected missing schema file error")
	}
}

func TestValidateSchemasAllowsRemoteSchemaReferences(t *testing.T) {
	registry, err := New([]Event{
		{Type: "example.created.v1", Schema: "https://schemas.example.com/example-created.v1.schema.json", Channel: "example.events.v1"},
		{Type: "example.updated.v1", Schema: "urn:example:schema:example-updated:v1", Channel: "example.events.v1"},
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := registry.ValidateSchemas(); err != nil {
		t.Fatalf("ValidateSchemas returned error: %v", err)
	}
}

func TestLoadRequiresRegistryPath(t *testing.T) {
	if _, err := Load(""); err == nil {
		t.Fatal("expected missing path error")
	}
}
