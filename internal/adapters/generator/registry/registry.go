// Package registry provides a simple in-process registry for generator adapters.
package registry

import (
	"fmt"
	"sort"

	"github.com/datascape/eventflow/internal/ports/generator"
)

// Constructor creates a generator adapter.
type Constructor func() generator.Port

// Registry stores generator constructors keyed by generator name.
type Registry struct {
	constructors map[string]Constructor
}

// New constructs an empty generator registry.
func New() *Registry {
	return &Registry{constructors: map[string]Constructor{}}
}

// Register adds a generator constructor to the registry.
func (r *Registry) Register(name string, constructor Constructor) error {
	if name == "" {
		return fmt.Errorf("generator name is required")
	}
	if constructor == nil {
		return fmt.Errorf("generator constructor is required")
	}
	if _, exists := r.constructors[name]; exists {
		return fmt.Errorf("generator %q is already registered", name)
	}
	r.constructors[name] = constructor
	return nil
}

// Names returns the registered generator names in stable order.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.constructors))
	for name := range r.constructors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Create constructs a generator by name.
func (r *Registry) Create(name string) (generator.Port, error) {
	constructor, ok := r.constructors[name]
	if !ok {
		return nil, fmt.Errorf("unknown generator %q; available generators: %v", name, r.Names())
	}
	return constructor(), nil
}
