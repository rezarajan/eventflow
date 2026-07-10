// Package jsonschema validates domain payloads against the repository JSON Schemas.
package jsonschema

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/datascape/eventflow/internal/contracts/registry"
)

// Validator validates event payloads with Draft 2020-12 JSON Schemas.
type Validator struct {
	mu      sync.Mutex
	schemas map[string]*jsonschema.Schema
}

// New constructs a schema validator with an empty compile cache.
func New() *Validator {
	return &Validator{schemas: map[string]*jsonschema.Schema{}}
}

// Validate checks one payload against the schema referenced by an event spec.
func (v *Validator) Validate(ctx context.Context, registered registry.Event, payload map[string]any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	schema, err := v.schemaFor(registered)
	if err != nil {
		return err
	}
	if err := schema.Validate(payload); err != nil {
		return err
	}
	return nil
}

// schemaFor returns a compiled schema for the event spec.
func (v *Validator) schemaFor(registered registry.Event) (*jsonschema.Schema, error) {
	if registered.Schema == "" {
		return nil, fmt.Errorf("schema path is required for %s", registered.Type)
	}
	path, err := resolvePath(registered.Schema)
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
	document, err := jsonschema.UnmarshalJSON(file)
	if err != nil {
		return nil, fmt.Errorf("parse schema %s: %w", path, err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	if err := compiler.AddResource(path, document); err != nil {
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

// resolvePath resolves repository-relative schema paths from any package working directory.
func resolvePath(path string) (string, error) {
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) {
		return clean, nil
	}
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
	return "", fmt.Errorf("resolve schema path %s: file not found from current directory", path)
}
