package consume

import (
	"github.com/rezarajan/eventflow/internal/adapters/consume/documents"
	"github.com/rezarajan/eventflow/internal/adapters/consume/duckdb"
	"github.com/rezarajan/eventflow/internal/adapters/consume/jsonl"
	port "github.com/rezarajan/eventflow/internal/ports/consume"
)

// Defaults constructs a handler registry for CLI use without imposing a storage backend in core code.
func Defaults() (*Registry, error) {
	reg := NewRegistry()
	if err := reg.Register(jsonl.Name, func() port.EventHandler { return jsonl.New(jsonl.FromEnv()) }); err != nil {
		return nil, err
	}
	if err := reg.Register(documents.Name, func() port.EventHandler { return documents.New(documents.FromEnv()) }); err != nil {
		return nil, err
	}
	if err := reg.Register(duckdb.Name, func() port.EventHandler { return duckdb.New(duckdb.FromEnv()) }); err != nil {
		return nil, err
	}
	return reg, nil
}
