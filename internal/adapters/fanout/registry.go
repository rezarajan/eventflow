// Package fanout contains publisher adapter registry utilities.
package fanout

import (
	"fmt"
	"sort"

	"github.com/rezarajan/project-datascape/internal/ports/fanout"
)

// Constructor creates a publisher adapter.
type Constructor func() fanout.Publisher

// Registry stores publisher constructors by output name.
type Registry struct {
	constructors map[string]Constructor
}

// NewRegistry constructs an empty publisher registry.
func NewRegistry() *Registry {
	return &Registry{constructors: map[string]Constructor{}}
}

// Register adds a publisher constructor to the registry.
func (r *Registry) Register(name string, constructor Constructor) error {
	if name == "" {
		return fmt.Errorf("publisher name is required")
	}
	if constructor == nil {
		return fmt.Errorf("publisher constructor is required")
	}
	if _, exists := r.constructors[name]; exists {
		return fmt.Errorf("publisher %q is already registered", name)
	}
	r.constructors[name] = constructor
	return nil
}

// Names returns registered publisher names in stable order.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.constructors))
	for name := range r.constructors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Create constructs a publisher by name.
func (r *Registry) Create(name string) (fanout.Publisher, error) {
	constructor, ok := r.constructors[name]
	if !ok {
		return nil, fmt.Errorf("unknown output %q; available outputs: %v", name, r.Names())
	}
	return constructor(), nil
}
