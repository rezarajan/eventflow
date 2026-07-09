package registry

import (
	"github.com/datascape/lakehouse-poc/internal/adapters/generator/schoolfaker"
	"github.com/datascape/lakehouse-poc/internal/ports/generator"
)

// Defaults constructs the built-in generator registry used by the CLI.
func Defaults() (*Registry, error) {
	reg := New()
	if err := reg.Register(schoolfaker.Name, func() generator.Port { return schoolfaker.New() }); err != nil {
		return nil, err
	}
	return reg, nil
}
