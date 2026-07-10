package contract

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	eventflow "github.com/rezarajan/project-datascape"
)

// Validator validates CloudEvents against a registry and JSON Schemas.
type Validator struct {
	Registry Registry
	mu       sync.Mutex
	schemas  map[string]*jsonschema.Schema
}

// NewValidator constructs a registry-backed validator.
func NewValidator(registry Registry) *Validator {
	return &Validator{Registry: registry, schemas: map[string]*jsonschema.Schema{}}
}

// Validate applies the standard Eventflow validation pipeline.
func (v *Validator) Validate(ctx context.Context, event eventflow.Event, mode eventflow.ValidationMode) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if mode == "" {
		mode = eventflow.ValidationStrict
	}
	if mode == eventflow.ValidationDisabled {
		return nil
	}
	if err := event.Validate(); err != nil {
		return eventflow.ValidationError("validate cloudevent", err)
	}
	registered, ok := v.Registry.Lookup(event.Type())
	if !ok {
		if mode == eventflow.ValidationPermissive {
			return nil
		}
		return eventflow.ValidationError("resolve registry entry", fmt.Errorf("%w: %s", eventflow.ErrNotFound, event.Type()))
	}
	if err := validateEnvelope(registered, event); err != nil {
		return eventflow.ValidationError("validate envelope", err)
	}
	if mode == eventflow.ValidationPermissive {
		return nil
	}
	if err := v.validatePayload(registered, event); err != nil {
		return eventflow.ValidationError("validate payload", err)
	}
	return nil
}

func validateEnvelope(registered Event, event eventflow.Event) error {
	if registered.Envelope.Type != "" && registered.Envelope.Type != event.Type() {
		return fmt.Errorf("type %q does not match registry type %q", event.Type(), registered.Envelope.Type)
	}
	if registered.Envelope.Source != "" && registered.Envelope.Source != event.Source() {
		return fmt.Errorf("source %q does not match registry source %q", event.Source(), registered.Envelope.Source)
	}
	if registered.Envelope.SourceRegex != "" {
		matched, err := regexp.MatchString(registered.Envelope.SourceRegex, event.Source())
		if err != nil {
			return err
		}
		if !matched {
			return fmt.Errorf("source %q does not match regex %q", event.Source(), registered.Envelope.SourceRegex)
		}
	}
	if registered.Envelope.Subject != "" && registered.Envelope.Subject != event.Subject() {
		return fmt.Errorf("subject %q does not match registry subject %q", event.Subject(), registered.Envelope.Subject)
	}
	for _, extension := range append(registered.RequiredExtensions, registered.Envelope.Extensions...) {
		if _, ok := event.Extensions()[strings.ToLower(strings.TrimSpace(extension))]; !ok {
			return fmt.Errorf("required extension %q is missing", extension)
		}
	}
	return nil
}

func (v *Validator) validatePayload(registered Event, event eventflow.Event) error {
	schema, err := v.schemaFor(registered.SchemaRef())
	if err != nil {
		return err
	}
	var payload any
	if err := json.Unmarshal(event.Data(), &payload); err != nil {
		return fmt.Errorf("decode event data: %w", err)
	}
	return schema.Validate(payload)
}

func (v *Validator) schemaFor(ref string) (*jsonschema.Schema, error) {
	document, _, _ := strings.Cut(ref, "#")
	path, err := resolveSchemaPath(document)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	if schema, ok := v.schemas[path]; ok {
		v.mu.Unlock()
		return schema, nil
	}
	v.mu.Unlock()
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open schema %s: %w", path, err)
	}
	defer file.Close()
	documentJSON, err := jsonschema.UnmarshalJSON(file)
	if err != nil {
		return nil, fmt.Errorf("parse schema %s: %w", path, err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	if err := compiler.AddResource(path, documentJSON); err != nil {
		return nil, fmt.Errorf("add schema resource %s: %w", path, err)
	}
	schema, err := compiler.Compile(path)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", path, err)
	}
	v.mu.Lock()
	v.schemas[path] = schema
	v.mu.Unlock()
	return schema, nil
}

func resolveSchemaPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	clean := filepath.Clean(path)
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(dir, clean)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("schema %s does not exist", path)
}
