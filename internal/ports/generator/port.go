// Package generator defines domain-neutral generator ports.
package generator

import (
	"context"

	"github.com/datascape/lakehouse-poc/internal/contracts/event"
)

// Config contains generic generator settings and adapter-specific parameters.
type Config struct {
	RunID      string         `json:"run_id"`
	Seed       int64          `json:"seed"`
	Parameters map[string]any `json:"parameters"`
}

// Port streams generated facts without knowing where those facts will be stored or published.
type Port interface {
	Name() string
	Generate(ctx context.Context, config Config, out chan<- event.Fact) error
}

// Factory creates named generators without coupling callers to concrete implementations.
type Factory interface {
	Names() []string
	Create(name string) (Port, error)
}
