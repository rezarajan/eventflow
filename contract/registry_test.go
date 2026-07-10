package contract

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseV2Registry(t *testing.T) {
	dir := t.TempDir()
	schema := filepath.Join(dir, "event.schema.json")
	if err := os.WriteFile(schema, []byte(`{"type":"object"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	body := []byte(`version: eventflow.registry.v2
events:
  - kind: example.created.v1
    payload_schema: ./event.schema.json
    channel:
      name: example.events.v1
      protocol: redpanda
    validation:
      mode: strict
    source:
      regex: '^urn:example:'
`)
	registry, err := Parse(body, dir)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	event, ok := registry.Lookup("example.created.v1")
	if !ok {
		t.Fatal("event not indexed")
	}
	if event.SchemaRef() != schema {
		t.Fatalf("schema = %s, want %s", event.SchemaRef(), schema)
	}
}

func TestParseMigratesV1Registry(t *testing.T) {
	dir := t.TempDir()
	body := []byte(`version: eventflow.registry.v1
events:
  - type: example.created.v1
    schema: ./event.schema.json
    channel: example.events.v1
`)
	registry, err := Parse(body, dir)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if registry.Version != Version {
		t.Fatalf("version = %s, want %s", registry.Version, Version)
	}
	event, ok := registry.Lookup("example.created.v1")
	if !ok {
		t.Fatal("event not indexed")
	}
	if event.Channel.Name != "example.events.v1" || event.Channel.Topic != "example.events.v1" {
		t.Fatalf("channel = %#v", event.Channel)
	}
}

func TestParseRejectsInvalidRegex(t *testing.T) {
	_, err := Parse([]byte(`version: eventflow.registry.v2
events:
  - kind: example.created.v1
    payload_schema: event.schema.json
    channel:
      name: example.events.v1
    source:
      regex: '['
`), "")
	if err == nil {
		t.Fatal("expected invalid regex error")
	}
}
