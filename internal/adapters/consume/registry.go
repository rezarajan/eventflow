// Package consume contains consumer handler adapter registry utilities.
package consume

import (
	"fmt"
	"sort"

	port "github.com/datascape/eventflow/internal/ports/consume"
)

// Constructor creates an event handler adapter.
type Constructor func() port.EventHandler

// Registry stores handler constructors by name.
type Registry struct {
	constructors map[string]Constructor
}

// NewRegistry constructs an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{constructors: map[string]Constructor{}}
}

// Register adds a handler constructor to the registry.
func (r *Registry) Register(name string, constructor Constructor) error {
	if name == "" {
		return fmt.Errorf("handler name is required")
	}
	if constructor == nil {
		return fmt.Errorf("handler constructor is required")
	}
	if _, exists := r.constructors[name]; exists {
		return fmt.Errorf("handler %q is already registered", name)
	}
	r.constructors[name] = constructor
	return nil
}

// Names returns registered handler names in stable order.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.constructors))
	for name := range r.constructors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Create constructs a handler by name.
func (r *Registry) Create(name string) (port.EventHandler, error) {
	constructor, ok := r.constructors[name]
	if !ok {
		return nil, fmt.Errorf("unknown handler %q; available handlers: %v", name, r.Names())
	}
	return constructor(), nil
}
