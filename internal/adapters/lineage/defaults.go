// Package lineage constructs concrete lineage adapters from configuration.
package lineage

import (
	"context"
	"fmt"
	"strings"

	"github.com/datascape/lakehouse-poc/internal/adapters/lineage/marquez"
	core "github.com/datascape/lakehouse-poc/internal/lineage"
)

// NewEmitter constructs a lineage emitter from configuration.
func NewEmitter(config core.Config) (core.Emitter, error) {
	var emitter core.Emitter
	switch strings.ToLower(strings.TrimSpace(config.Output)) {
	case "", "noop":
		emitter = core.NoopEmitter{}
	case "file":
		file := config.File
		if file == "" {
			file = "var/datascape/lineage/openlineage.ndjson"
		}
		emitter = core.FileEmitter{Path: file}
	case "marquez":
		emitter = marquez.New(marquez.Config{URL: config.MarquezURL, Timeout: config.Timeout})
	default:
		return nil, fmt.Errorf("unsupported lineage output %q", config.Output)
	}
	return metadataEmitter{emitter: emitter, producer: config.Producer, schemaURL: config.SchemaURL}, nil
}

// metadataEmitter stamps configured OpenLineage producer metadata before delegation.
type metadataEmitter struct {
	emitter   core.Emitter
	producer  string
	schemaURL string
}

// Emit stamps producer metadata and emits one lineage event.
func (e metadataEmitter) Emit(ctx context.Context, event core.Event) error {
	return e.emitter.Emit(ctx, core.WithProducer(event, e.producer, e.schemaURL))
}
