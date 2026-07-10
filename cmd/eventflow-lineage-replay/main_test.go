package main

import (
	"context"
	"io"
	"testing"

	"github.com/datascape/eventflow/internal/lineage"
)

// TestReplayEmitsEventsUntilEOF verifies replay reads and emits all events.
func TestReplayEmitsEventsUntilEOF(t *testing.T) {
	reader := &fakeReader{events: []lineage.Event{
		lineage.NewEvent("START", "datascape", "job", "run-1", nil, nil, nil, nil),
		lineage.NewEvent("COMPLETE", "datascape", "job", "run-1", nil, nil, nil, nil),
	}}
	emitter := &fakeEmitter{}
	if err := replay(context.Background(), reader, emitter, 0); err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if len(emitter.events) != 2 {
		t.Fatalf("emitted events = %d, want 2", len(emitter.events))
	}
}

// TestReplayHonorsLimit verifies replay can be bounded.
func TestReplayHonorsLimit(t *testing.T) {
	reader := &fakeReader{events: []lineage.Event{
		lineage.NewEvent("START", "datascape", "job", "run-1", nil, nil, nil, nil),
		lineage.NewEvent("COMPLETE", "datascape", "job", "run-1", nil, nil, nil, nil),
	}}
	emitter := &fakeEmitter{}
	if err := replay(context.Background(), reader, emitter, 1); err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("emitted events = %d, want 1", len(emitter.events))
	}
}

// fakeReader returns lineage events from memory.
type fakeReader struct {
	events []lineage.Event
	offset int
}

// Read returns the next fake lineage event.
func (r *fakeReader) Read(ctx context.Context) (lineage.Event, error) {
	if ctx.Err() != nil {
		return lineage.Event{}, ctx.Err()
	}
	if r.offset >= len(r.events) {
		return lineage.Event{}, io.EOF
	}
	event := r.events[r.offset]
	r.offset++
	return event, nil
}

// Close closes the fake reader.
func (r *fakeReader) Close() error {
	return nil
}

// fakeEmitter records emitted lineage events.
type fakeEmitter struct {
	events []lineage.Event
}

// Emit records one lineage event.
func (e *fakeEmitter) Emit(ctx context.Context, event lineage.Event) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	e.events = append(e.events, event)
	return nil
}
