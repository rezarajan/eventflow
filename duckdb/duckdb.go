// Package duckdb exposes DuckDB Eventflow adapters.
package duckdb

import (
	"context"
	"fmt"

	eventflow "github.com/rezarajan/eventflow"
	internalduckdb "github.com/rezarajan/eventflow/internal/adapters/consume/duckdb"
	"github.com/rezarajan/eventflow/resource"
)

// Config defines DuckDB adapter settings.
type Config struct {
	Path         string
	RegistryPath string
	RawTable     string
}

type ResourceSpec struct {
	Path         string `yaml:"path" json:"path"`
	RegistryPath string `yaml:"registryPath,omitempty" json:"registryPath,omitempty"`
	RawTable     string `yaml:"rawTable,omitempty" json:"rawTable,omitempty"`
}

func Register(catalog *resource.Catalog) error {
	if err := resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK: resource.GVK("DuckDBEmitter"),
		Default: func(spec *ResourceSpec) error {
			if spec.Path == "" {
				spec.Path = "var/eventflow/eventflow.duckdb"
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec ResourceSpec) (any, error) {
			return NewEmitter(Config{Path: spec.Path, RegistryPath: spec.RegistryPath, RawTable: spec.RawTable}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityEmitter, resource.CapabilityBatchEmission},
	}); err != nil {
		return err
	}
	return resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK: resource.GVK("DuckDBReceiver"),
		Build: func(context.Context, resource.BuildContext, ResourceSpec) (any, error) {
			return nil, fmt.Errorf("DuckDBReceiver is not implemented by the current DuckDB adapter")
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent},
	})
}

// Emitter writes validated events to Eventflow-owned DuckDB tables.
type Emitter struct {
	config Config
	inner  *internalduckdb.Projector
}

// NewEmitter constructs a DuckDB emitter.
func NewEmitter(config Config) *Emitter { return &Emitter{config: config} }

// Name returns the adapter name.
func (*Emitter) Name() string { return "duckdb" }

// Open opens DuckDB and loads registry-driven projection metadata.
func (e *Emitter) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.inner = internalduckdb.New(internalduckdb.Config{Path: e.config.Path, RegistryPath: e.config.RegistryPath})
	return e.inner.Open(ctx)
}

// Emit inserts one event into Eventflow-owned tables.
func (e *Emitter) Emit(ctx context.Context, event eventflow.Event) error {
	if e.inner == nil {
		return fmt.Errorf("duckdb emitter is not open")
	}
	return e.inner.Handle(ctx, event)
}

// EmitBatch inserts a batch transactionally through the existing projector.
func (e *Emitter) EmitBatch(ctx context.Context, events []eventflow.Event) error {
	if e.inner == nil {
		return fmt.Errorf("duckdb emitter is not open")
	}
	return e.inner.HandleBatch(ctx, events)
}

// Close closes DuckDB resources.
func (e *Emitter) Close(ctx context.Context) error {
	if e.inner == nil {
		return ctx.Err()
	}
	return e.inner.Close(ctx)
}
