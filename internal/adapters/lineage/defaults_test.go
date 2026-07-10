package lineage

import (
	"context"
	"testing"

	core "github.com/datascape/eventflow/internal/lineage"
)

// TestNewEmitterRejectsUnknownOutput verifies unsupported lineage outputs fail fast.
func TestNewEmitterRejectsUnknownOutput(t *testing.T) {
	if _, err := NewEmitter(core.Config{Output: "unknown"}); err == nil {
		t.Fatal("expected unsupported output error")
	}
}

// TestMetadataEmitterStampsProducer verifies configured OpenLineage metadata is applied.
func TestMetadataEmitterStampsProducer(t *testing.T) {
	recorder := &recordingEmitter{}
	emitter := metadataEmitter{emitter: recorder, producer: "producer", schemaURL: "schema"}
	if err := emitter.Emit(context.Background(), core.NewEvent("START", "ns", "job", "run", nil, nil, nil, nil)); err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}
	if recorder.event.Producer != "producer" || recorder.event.SchemaURL != "schema" {
		t.Fatalf("unexpected metadata: %+v", recorder.event)
	}
}

// recordingEmitter records one lineage event.
type recordingEmitter struct {
	event core.Event
}

// Emit records one lineage event.
func (e *recordingEmitter) Emit(ctx context.Context, event core.Event) error {
	e.event = event
	return ctx.Err()
}
