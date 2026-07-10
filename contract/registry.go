// Package contract loads and validates Eventflow registry contracts.
package contract

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	// Version is the canonical registry v2 version.
	Version = "eventflow.registry.v2"
	// VersionV1 is accepted only for migration.
	VersionV1 = "eventflow.registry.v1"
)

// Registry describes all event contracts available to a runtime.
type Registry struct {
	Version string  `yaml:"version" json:"version"`
	Events  []Event `yaml:"events" json:"events"`

	byType map[string]Event
}

// Event describes one registered event type.
type Event struct {
	Kind               string            `yaml:"kind,omitempty" json:"kind,omitempty"`
	Type               string            `yaml:"type,omitempty" json:"type,omitempty"`
	Envelope           Envelope          `yaml:"envelope,omitempty" json:"envelope,omitempty"`
	PayloadSchema      string            `yaml:"payload_schema,omitempty" json:"payload_schema,omitempty"`
	Schema             string            `yaml:"schema,omitempty" json:"schema,omitempty"`
	Channel            Channel           `yaml:"channel,omitempty" json:"channel,omitempty"`
	Adapter            AdapterConfig     `yaml:"adapter,omitempty" json:"adapter,omitempty"`
	Validation         ValidationPolicy  `yaml:"validation,omitempty" json:"validation,omitempty"`
	Source             SourceConstraints `yaml:"source,omitempty" json:"source,omitempty"`
	RequiredExtensions []string          `yaml:"required_extensions,omitempty" json:"required_extensions,omitempty"`
	AsyncAPIRef        string            `yaml:"asyncapi_ref,omitempty" json:"asyncapi_ref,omitempty"`
	OpenAPIRef         string            `yaml:"openapi_ref,omitempty" json:"openapi_ref,omitempty"`
	Projection         Projection        `yaml:"projection,omitempty" json:"projection,omitempty"`
	Storage            Storage           `yaml:"storage,omitempty" json:"storage,omitempty"`
	Metadata           map[string]string `yaml:"metadata,omitempty" json:"metadata,omitempty"`
}

// Envelope constrains CloudEvents envelope attributes.
type Envelope struct {
	Type            string   `yaml:"type,omitempty" json:"type,omitempty"`
	Source          string   `yaml:"source,omitempty" json:"source,omitempty"`
	SourceRegex     string   `yaml:"source_regex,omitempty" json:"source_regex,omitempty"`
	Subject         string   `yaml:"subject,omitempty" json:"subject,omitempty"`
	DataContentType string   `yaml:"datacontenttype,omitempty" json:"datacontenttype,omitempty"`
	Extensions      []string `yaml:"extensions,omitempty" json:"extensions,omitempty"`
}

// Channel describes transport routing.
type Channel struct {
	Name     string `yaml:"name,omitempty" json:"name,omitempty"`
	Protocol string `yaml:"protocol,omitempty" json:"protocol,omitempty"`
	Topic    string `yaml:"topic,omitempty" json:"topic,omitempty"`
	Path     string `yaml:"path,omitempty" json:"path,omitempty"`
	Ref      string `yaml:"ref,omitempty" json:"ref,omitempty"`
}

// UnmarshalYAML accepts both v2 mapping syntax and v1 scalar channel names.
func (c *Channel) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		c.Name = strings.TrimSpace(value.Value)
		c.Topic = c.Name
		return nil
	}
	type channelAlias Channel
	var decoded channelAlias
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*c = Channel(decoded)
	return nil
}

// AdapterConfig declares the expected adapter and codec for an event.
type AdapterConfig struct {
	Name   string         `yaml:"name,omitempty" json:"name,omitempty"`
	Codec  string         `yaml:"codec,omitempty" json:"codec,omitempty"`
	Config map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

// ValidationPolicy configures validation for an event.
type ValidationPolicy struct {
	Mode             string   `yaml:"mode,omitempty" json:"mode,omitempty"`
	CustomValidators []string `yaml:"custom_validators,omitempty" json:"custom_validators,omitempty"`
}

// SourceConstraints validates producer source values.
type SourceConstraints struct {
	Allow      []string `yaml:"allow,omitempty" json:"allow,omitempty"`
	Regex      string   `yaml:"regex,omitempty" json:"regex,omitempty"`
	RequireURI bool     `yaml:"require_uri,omitempty" json:"require_uri,omitempty"`
}

// Projection describes typed materialization metadata.
type Projection struct {
	Table string `yaml:"table,omitempty" json:"table,omitempty"`
	Mode  string `yaml:"mode,omitempty" json:"mode,omitempty"`
}

// Storage describes event storage metadata.
type Storage struct {
	Kind   string `yaml:"kind,omitempty" json:"kind,omitempty"`
	Bucket string `yaml:"bucket,omitempty" json:"bucket,omitempty"`
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	Table  string `yaml:"table,omitempty" json:"table,omitempty"`
}

// Load reads and validates a registry file.
func Load(path string) (Registry, error) {
	if strings.TrimSpace(path) == "" {
		return Registry{}, fmt.Errorf("EVENTFLOW_REGISTRY is required")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return Registry{}, fmt.Errorf("read registry %s: %w", path, err)
	}
	return Parse(body, filepath.Dir(path))
}

// Parse parses registry YAML from memory.
func Parse(body []byte, baseDir string) (Registry, error) {
	var registry Registry
	if err := yaml.Unmarshal(body, &registry); err != nil {
		return Registry{}, fmt.Errorf("parse registry: %w", err)
	}
	if err := registry.Normalize(baseDir); err != nil {
		return Registry{}, err
	}
	return registry, nil
}

// Normalize validates fields, resolves references, and builds indexes.
func (r *Registry) Normalize(baseDir string) error {
	if strings.TrimSpace(r.Version) == "" {
		r.Version = Version
	}
	if r.Version == VersionV1 {
		migrateV1(r)
	} else if r.Version != Version {
		return fmt.Errorf("unsupported registry version %q", r.Version)
	}
	if len(r.Events) == 0 {
		return fmt.Errorf("registry must contain at least one event")
	}
	r.byType = map[string]Event{}
	for i, event := range r.Events {
		normalized, err := normalizeEvent(baseDir, i, event)
		if err != nil {
			return err
		}
		key := normalized.EventType()
		if _, exists := r.byType[key]; exists {
			return fmt.Errorf("duplicate event type %q", key)
		}
		r.Events[i] = normalized
		r.byType[key] = normalized
	}
	return nil
}

// Lookup returns the registration for a CloudEvents type.
func (r Registry) Lookup(eventType string) (Event, bool) {
	event, ok := r.byType[strings.TrimSpace(eventType)]
	return event, ok
}

// MustLookup returns a registration or a diagnostic error.
func (r Registry) MustLookup(eventType string) (Event, error) {
	event, ok := r.Lookup(eventType)
	if !ok {
		return Event{}, fmt.Errorf("unknown event type %q; available event types: %v", eventType, r.Types())
	}
	return event, nil
}

// Types returns registered event types in stable order.
func (r Registry) Types() []string {
	types := make([]string, 0, len(r.byType))
	for eventType := range r.byType {
		types = append(types, eventType)
	}
	sort.Strings(types)
	return types
}

// EventType returns the canonical CloudEvents type for this event.
func (e Event) EventType() string {
	if strings.TrimSpace(e.Envelope.Type) != "" {
		return strings.TrimSpace(e.Envelope.Type)
	}
	if strings.TrimSpace(e.Type) != "" {
		return strings.TrimSpace(e.Type)
	}
	return strings.TrimSpace(e.Kind)
}

// SchemaRef returns the payload schema reference.
func (e Event) SchemaRef() string {
	if strings.TrimSpace(e.PayloadSchema) != "" {
		return strings.TrimSpace(e.PayloadSchema)
	}
	return strings.TrimSpace(e.Schema)
}

func normalizeEvent(baseDir string, index int, event Event) (Event, error) {
	event.Kind = strings.TrimSpace(event.Kind)
	event.Type = strings.TrimSpace(event.Type)
	event.Envelope.Type = strings.TrimSpace(event.Envelope.Type)
	event.PayloadSchema = resolveRef(baseDir, strings.TrimSpace(event.PayloadSchema))
	event.Schema = resolveRef(baseDir, strings.TrimSpace(event.Schema))
	event.Channel.Name = strings.TrimSpace(event.Channel.Name)
	event.Channel.Topic = strings.TrimSpace(event.Channel.Topic)
	event.Channel.Protocol = strings.TrimSpace(event.Channel.Protocol)
	event.Adapter.Name = strings.TrimSpace(event.Adapter.Name)
	event.Adapter.Codec = strings.TrimSpace(event.Adapter.Codec)
	event.Validation.Mode = strings.TrimSpace(event.Validation.Mode)
	event.Source.Regex = strings.TrimSpace(event.Source.Regex)
	if event.EventType() == "" {
		return Event{}, fmt.Errorf("event %d kind/type is required", index)
	}
	if event.Envelope.Type == "" {
		event.Envelope.Type = event.EventType()
	}
	if event.Kind == "" {
		event.Kind = event.Envelope.Type
	}
	if event.Type == "" {
		event.Type = event.Envelope.Type
	}
	if event.SchemaRef() == "" {
		return Event{}, fmt.Errorf("event %s payload schema is required", event.EventType())
	}
	if event.Channel.Name == "" && event.Channel.Topic == "" && event.Channel.Path == "" && event.Channel.Ref == "" {
		return Event{}, fmt.Errorf("event %s channel config is required", event.EventType())
	}
	if err := validateMode(event.Validation.Mode); err != nil {
		return Event{}, fmt.Errorf("event %s: %w", event.EventType(), err)
	}
	if err := validateCodec(event.Adapter.Codec); err != nil {
		return Event{}, fmt.Errorf("event %s: %w", event.EventType(), err)
	}
	if event.Envelope.SourceRegex != "" {
		if _, err := regexp.Compile(event.Envelope.SourceRegex); err != nil {
			return Event{}, fmt.Errorf("event %s invalid envelope source regex: %w", event.EventType(), err)
		}
	}
	if event.Source.Regex != "" {
		if _, err := regexp.Compile(event.Source.Regex); err != nil {
			return Event{}, fmt.Errorf("event %s invalid source regex: %w", event.EventType(), err)
		}
	}
	if event.Source.RequireURI && event.Envelope.Source != "" {
		if parsed, err := url.Parse(event.Envelope.Source); err != nil || parsed.Scheme == "" {
			return Event{}, fmt.Errorf("event %s envelope source must be a URI", event.EventType())
		}
	}
	return event, nil
}

func migrateV1(r *Registry) {
	r.Version = Version
	for i := range r.Events {
		event := &r.Events[i]
		if event.Kind == "" {
			event.Kind = event.Type
		}
		if event.Envelope.Type == "" {
			event.Envelope.Type = event.Type
		}
		if event.PayloadSchema == "" {
			event.PayloadSchema = event.Schema
		}
		if event.Channel.Name == "" {
			event.Channel.Name = event.Channel.Topic
		}
	}
}

func validateMode(mode string) error {
	switch mode {
	case "", "strict", "compatible", "permissive", "disabled":
		return nil
	default:
		return fmt.Errorf("unsupported validation mode %q", mode)
	}
}

func validateCodec(codec string) error {
	switch codec {
	case "", "cloudevents-json", "cloudevents-binary-http", "openlineage-json", "ndjson", "json":
		return nil
	default:
		return fmt.Errorf("unsupported codec %q", codec)
	}
}

func resolveRef(baseDir string, ref string) string {
	if ref == "" || baseDir == "" || filepath.IsAbs(ref) || strings.Contains(ref, "://") {
		return ref
	}
	document, fragment, found := strings.Cut(ref, "#")
	if document == "" {
		return ref
	}
	resolved := filepath.Clean(filepath.Join(baseDir, document))
	if found {
		return resolved + "#" + fragment
	}
	return resolved
}
