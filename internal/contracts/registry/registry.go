// Package registry loads domain event registration from external YAML files.
package registry

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const Version = "eventflow.registry.v1"

// Registry describes the domain event contracts available to a runtime process.
type Registry struct {
	Version string  `yaml:"version" json:"version"`
	Events  []Event `yaml:"events" json:"events"`

	byType map[string]Event
}

// Event describes one domain event contract.
type Event struct {
	Type       string     `yaml:"type" json:"type"`
	Schema     string     `yaml:"schema" json:"schema"`
	Channel    string     `yaml:"channel" json:"channel"`
	Projection Projection `yaml:"projection,omitempty" json:"projection,omitempty"`
}

// Projection describes optional materialization hints for generic projectors.
type Projection struct {
	Table string `yaml:"table,omitempty" json:"table,omitempty"`
}

// Load reads and validates a registry YAML file.
func Load(path string) (Registry, error) {
	if strings.TrimSpace(path) == "" {
		return Registry{}, fmt.Errorf("EVENTFLOW_REGISTRY is required")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return Registry{}, fmt.Errorf("read registry %s: %w", path, err)
	}
	var registry Registry
	if err := yaml.Unmarshal(body, &registry); err != nil {
		return Registry{}, fmt.Errorf("parse registry %s: %w", path, err)
	}
	if err := registry.normalize(filepath.Dir(path)); err != nil {
		return Registry{}, err
	}
	return registry, nil
}

// New constructs a registry from already parsed events.
func New(events []Event) (Registry, error) {
	registry := Registry{Version: Version, Events: append([]Event(nil), events...)}
	if err := registry.normalize(""); err != nil {
		return Registry{}, err
	}
	return registry, nil
}

// Lookup returns the event registration for a CloudEvents type.
func (r Registry) Lookup(eventType string) (Event, bool) {
	event, ok := r.byType[strings.TrimSpace(eventType)]
	return event, ok
}

// MustLookup returns the event registration for a CloudEvents type or an error.
func (r Registry) MustLookup(eventType string) (Event, error) {
	event, ok := r.Lookup(eventType)
	if !ok {
		return Event{}, fmt.Errorf("unknown event type %q; available event types: %v", eventType, r.Types())
	}
	return event, nil
}

// Types returns registered CloudEvents types in stable order.
func (r Registry) Types() []string {
	types := make([]string, 0, len(r.byType))
	for eventType := range r.byType {
		types = append(types, eventType)
	}
	sort.Strings(types)
	return types
}

// HasEvents reports whether the registry contains any event registrations.
func (r Registry) HasEvents() bool {
	return len(r.byType) > 0
}

// ValidateSchemas verifies that every registered local schema file exists.
//
// Remote schema identifiers are allowed because they are resolved by external
// tooling or a schema registry. Local file and file:// references must point to
// a regular file so misspelled contracts fail during registry validation.
func (r Registry) ValidateSchemas() error {
	for _, event := range r.Events {
		if err := validateSchemaFile(event); err != nil {
			return err
		}
	}
	return nil
}

// normalize validates fields, resolves relative schema paths, and builds indexes.
func (r *Registry) normalize(baseDir string) error {
	if strings.TrimSpace(r.Version) == "" {
		r.Version = Version
	}
	if r.Version != Version {
		return fmt.Errorf("unsupported registry version %q", r.Version)
	}
	if len(r.Events) == 0 {
		return fmt.Errorf("registry must contain at least one event")
	}
	r.byType = make(map[string]Event, len(r.Events))
	for i, event := range r.Events {
		event.Type = strings.TrimSpace(event.Type)
		event.Schema = strings.TrimSpace(event.Schema)
		event.Channel = strings.TrimSpace(event.Channel)
		event.Projection.Table = strings.TrimSpace(event.Projection.Table)
		if event.Type == "" {
			return fmt.Errorf("event %d type is required", i)
		}
		if event.Schema == "" {
			return fmt.Errorf("event %s schema is required", event.Type)
		}
		if event.Channel == "" {
			return fmt.Errorf("event %s channel is required", event.Type)
		}
		if _, exists := r.byType[event.Type]; exists {
			return fmt.Errorf("duplicate event type %q", event.Type)
		}
		event.Schema = resolveSchema(baseDir, event.Schema)
		r.Events[i] = event
		r.byType[event.Type] = event
	}
	return nil
}

// resolveSchema resolves local relative schema paths against the registry file directory.
func resolveSchema(baseDir string, schema string) string {
	if baseDir == "" || filepath.IsAbs(schema) || strings.Contains(schema, "://") {
		return schema
	}
	return filepath.Clean(filepath.Join(baseDir, schema))
}

// validateSchemaFile checks one event schema reference when it is local.
func validateSchemaFile(event Event) error {
	schema := schemaDocumentPath(event.Schema)
	if strings.TrimSpace(schema) == "" {
		return fmt.Errorf("event %q schema %q does not include a schema document path", event.Type, event.Schema)
	}
	parsed, err := url.Parse(schema)
	if err == nil && parsed.Scheme != "" {
		if parsed.Scheme != "file" {
			return nil
		}
		schema = parsed.Path
	}
	info, err := os.Stat(schema)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("event %q schema %q does not exist", event.Type, event.Schema)
		}
		return fmt.Errorf("event %q schema %q: %w", event.Type, event.Schema, err)
	}
	if info.IsDir() {
		return fmt.Errorf("event %q schema %q is a directory, not a file", event.Type, event.Schema)
	}
	return nil
}

// schemaDocumentPath removes a JSON Pointer fragment from a schema reference.
func schemaDocumentPath(schema string) string {
	document, _, _ := strings.Cut(schema, "#")
	return document
}
