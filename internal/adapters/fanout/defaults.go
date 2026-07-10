package fanout

import (
	"io"
	"log/slog"

	"github.com/datascape/eventflow/internal/adapters/fanout/discard"
	logadapter "github.com/datascape/eventflow/internal/adapters/fanout/log"
	"github.com/datascape/eventflow/internal/adapters/fanout/redpanda"
	stdoutadapter "github.com/datascape/eventflow/internal/adapters/fanout/stdout"
	port "github.com/datascape/eventflow/internal/ports/fanout"
)

// Defaults constructs a publisher registry for CLI use without imposing a storage backend.
func Defaults(stdout io.Writer, logger *slog.Logger) (*Registry, error) {
	reg := NewRegistry()
	if err := reg.Register(stdoutadapter.Name, func() port.Publisher { return stdoutadapter.New(stdout) }); err != nil {
		return nil, err
	}
	if err := reg.Register(discard.Name, func() port.Publisher { return discard.New() }); err != nil {
		return nil, err
	}
	if err := reg.Register(logadapter.Name, func() port.Publisher { return logadapter.New(logger) }); err != nil {
		return nil, err
	}
	if err := reg.Register(redpanda.Name, func() port.Publisher { return redpanda.New(redpanda.FromEnv()) }); err != nil {
		return nil, err
	}
	return reg, nil
}
