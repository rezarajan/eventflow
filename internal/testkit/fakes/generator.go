// Package fakes provides in-memory test doubles for ports.
package fakes

import (
	"context"
	"fmt"

	"github.com/datascape/eventflow/internal/contracts/event"
	"github.com/datascape/eventflow/internal/ports/generator"
)

// Generator is an in-memory generator test double.
type Generator struct {
	GeneratorName string
	Facts         []event.Fact
	Err           error
}

// Name returns the configured generator name.
func (g Generator) Name() string {
	if g.GeneratorName == "" {
		return "fake.generator.v1"
	}
	return g.GeneratorName
}

// Generate streams configured facts to the output channel.
func (g Generator) Generate(ctx context.Context, config generator.Config, out chan<- event.Fact) error {
	defer close(out)
	if g.Err != nil {
		return g.Err
	}
	for _, fact := range g.Facts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- fact:
		}
	}
	return nil
}

// GeneratorFactory is an in-memory generator factory test double.
type GeneratorFactory struct {
	Generators map[string]generator.Port
}

// Names returns the configured generator names.
func (f GeneratorFactory) Names() []string {
	names := make([]string, 0, len(f.Generators))
	for name := range f.Generators {
		names = append(names, name)
	}
	return names
}

// Create returns a configured generator by name.
func (f GeneratorFactory) Create(name string) (generator.Port, error) {
	gen, ok := f.Generators[name]
	if !ok {
		return nil, fmt.Errorf("unknown fake generator %q", name)
	}
	return gen, nil
}
