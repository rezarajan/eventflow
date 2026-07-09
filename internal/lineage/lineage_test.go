package lineage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRedpandaDatasetUsesStableBrokerAndTopic verifies Redpanda dataset naming is stitchable.
func TestRedpandaDatasetUsesStableBrokerAndTopic(t *testing.T) {
	got := RedpandaDataset([]string{"localhost:19092"}, "datascape.events.v1")
	if got.Namespace != "redpanda://localhost:19092" || got.Name != "datascape.events.v1" {
		t.Fatalf("unexpected dataset: %+v", got)
	}
}

// TestFileEmitterWritesNDJSON verifies file lineage output writes one JSON event per line.
func TestFileEmitterWritesNDJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "openlineage.ndjson")
	emitter := FileEmitter{Path: path}
	event := NewEvent("START", "datascape", "job", "run-1", nil, []Dataset{{Namespace: "file://out", Name: "data"}}, nil, fixedLineageTime)
	if err := emitter.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read lineage file: %v", err)
	}
	var decoded Event
	if err := json.Unmarshal(body[:len(body)-1], &decoded); err != nil {
		t.Fatalf("unmarshal lineage event: %v", err)
	}
	if decoded.EventType != "START" || decoded.Job.Name != "job" || decoded.Outputs[0].Name != "data" {
		t.Fatalf("unexpected decoded event: %+v", decoded)
	}
}

// TestEmitLifecycleEmitsFail verifies lifecycle helper records failures.
func TestEmitLifecycleEmitsFail(t *testing.T) {
	emitter := &fakeEmitter{}
	runErr := fakeLineageError("failed")
	if err := EmitLifecycle(context.Background(), emitter, "datascape", "job", "run-1", nil, nil, runErr, fixedLineageTime); err != nil {
		t.Fatalf("EmitLifecycle returned error: %v", err)
	}
	if len(emitter.events) != 2 || emitter.events[0].EventType != "START" || emitter.events[1].EventType != "FAIL" || emitter.events[1].Error != "failed" {
		t.Fatalf("unexpected lifecycle events: %+v", emitter.events)
	}
}

// fakeEmitter records lineage events in memory.
type fakeEmitter struct {
	events []Event
}

// Emit records one lineage event.
func (e *fakeEmitter) Emit(ctx context.Context, event Event) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	e.events = append(e.events, event)
	return nil
}

// fakeLineageError is a minimal test error.
type fakeLineageError string

// Error returns the fake error text.
func (e fakeLineageError) Error() string {
	return string(e)
}

// fixedLineageTime returns a stable timestamp for lineage tests.
func fixedLineageTime() time.Time {
	return time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
}
