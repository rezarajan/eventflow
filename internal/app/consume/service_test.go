package consume

import (
	"context"
	"io"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/eventflow/internal/lineage"
	port "github.com/datascape/eventflow/internal/ports/consume"
)

// TestServiceConsumesBatchesWithBatchHandlers verifies batch-capable handlers receive consumed batches.
func TestServiceConsumesBatchesWithBatchHandlers(t *testing.T) {
	source := &fakeSource{events: []cloudevents.Event{
		consumeTestEvent(t, "1", "example.created.v1"),
		consumeTestEvent(t, "2", "class.created.v1"),
		consumeTestEvent(t, "3", "student.enrolled.v1"),
	}}
	handler := &fakeBatchHandler{fakeHandler: fakeHandler{name: "batch"}}
	service := Service{Source: source, Handlers: []port.EventHandler{handler}, BatchSize: 2, MaxEvents: 3, Now: fixedConsumeTime}
	summary, err := service.Run(context.Background(), "consume-test")
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if summary.Events != 3 || handler.batchCalls != 2 || len(handler.events) != 3 {
		t.Fatalf("unexpected consumption summary/events: summary=%+v calls=%d events=%d", summary, handler.batchCalls, len(handler.events))
	}
	if !source.closed || !handler.closed {
		t.Fatalf("expected source and handler to close")
	}
}

// TestServiceStopsAtMaxEvents verifies the configured event limit bounds consumption.
func TestServiceStopsAtMaxEvents(t *testing.T) {
	source := &fakeSource{events: []cloudevents.Event{
		consumeTestEvent(t, "1", "example.created.v1"),
		consumeTestEvent(t, "2", "class.created.v1"),
		consumeTestEvent(t, "3", "student.enrolled.v1"),
	}}
	handler := &fakeHandler{name: "one"}
	service := Service{Source: source, Handlers: []port.EventHandler{handler}, BatchSize: 10, MaxEvents: 2, Now: fixedConsumeTime}
	summary, err := service.Run(context.Background(), "consume-test")
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if summary.Events != 2 || len(handler.events) != 2 {
		t.Fatalf("expected two consumed events, summary=%+v handled=%d", summary, len(handler.events))
	}
}

// TestServiceRequiresHandlers verifies handler configuration is mandatory.
func TestServiceRequiresHandlers(t *testing.T) {
	service := Service{Source: &fakeSource{}, Handlers: nil}
	if _, err := service.Run(context.Background(), "consume-test"); err == nil {
		t.Fatal("expected missing handler error")
	}
}

// TestServiceEmitsHandlerLineageWithoutMutatingCloudEvents verifies projector lineage is separate metadata.
func TestServiceEmitsConsumeAndHandlerLineageWithoutMutatingCloudEvents(t *testing.T) {
	evt := consumeTestEvent(t, "1", "document.uploaded.v1")
	before := string(evt.Data())
	source := &fakeSource{events: []cloudevents.Event{evt}}
	handler := &fakeBatchHandler{fakeHandler: fakeHandler{name: "objects", outputs: []lineage.Dataset{{Namespace: "file://objects", Name: "documents/"}}}}
	emitter := &fakeLineageEmitter{}
	service := Service{Source: source, Handlers: []port.EventHandler{handler}, Lineage: emitter, BatchSize: 1, MaxEvents: 1, Now: fixedConsumeTime}
	if _, err := service.Run(context.Background(), "consume-test"); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(emitter.events) != 4 {
		t.Fatalf("unexpected lineage events: %+v", emitter.events)
	}
	consumeStart := emitter.events[0]
	if consumeStart.Job.Name != "eventflow-consume" || consumeStart.Inputs[0].Name != "events" || consumeStart.Outputs[0].Name != "documents/" {
		t.Fatalf("unexpected consume lineage event: %+v", consumeStart)
	}
	handlerStart := emitter.events[1]
	parent, ok := handlerStart.Run.Facets["parent"].(lineage.ParentRunFacet)
	if !ok || parent.Run.RunID != "consume-test" || parent.Job.Name != "eventflow-consume" {
		t.Fatalf("unexpected parent facet: %+v", handlerStart.Run.Facets)
	}
	if handlerStart.Job.Name != "eventflow-objects-projector" || handlerStart.Outputs[0].Name != "documents/" {
		t.Fatalf("unexpected handler lineage event: %+v", handlerStart)
	}
	if emitter.events[2].EventType != "COMPLETE" || emitter.events[3].EventType != "COMPLETE" {
		t.Fatalf("unexpected lineage lifecycle events: %+v", emitter.events)
	}
	if string(evt.Data()) != before {
		t.Fatalf("CloudEvent data mutated: before=%s after=%s", before, evt.Data())
	}
}

// fakeSource is an in-memory bounded event source.
type fakeSource struct {
	events []cloudevents.Event
	offset int
	closed bool
}

// Name returns the fake source name.
func (s *fakeSource) Name() string {
	return "fake"
}

// Dataset returns the fake source dataset.
func (s *fakeSource) Dataset() lineage.Dataset {
	return lineage.Dataset{Namespace: "fake", Name: "events"}
}

// Open records source opening.
func (s *fakeSource) Open(ctx context.Context) error {
	return ctx.Err()
}

// ReadBatch returns the next bounded group of fake events.
func (s *fakeSource) ReadBatch(ctx context.Context, maxEvents int) ([]cloudevents.Event, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if s.offset >= len(s.events) {
		return nil, io.EOF
	}
	end := s.offset + maxEvents
	if end > len(s.events) {
		end = len(s.events)
	}
	out := append([]cloudevents.Event(nil), s.events[s.offset:end]...)
	s.offset = end
	return out, nil
}

// Close records source closure.
func (s *fakeSource) Close(ctx context.Context) error {
	s.closed = true
	return ctx.Err()
}

// fakeHandler records one-by-one handled events.
type fakeHandler struct {
	name    string
	events  []cloudevents.Event
	outputs []lineage.Dataset
	closed  bool
}

// Name returns the fake handler name.
func (h *fakeHandler) Name() string {
	return h.name
}

// Dataset returns the fake handler dataset.
func (h *fakeHandler) Dataset() lineage.Dataset {
	return lineage.Dataset{Namespace: "fake", Name: h.name}
}

// OutputDatasets returns precise fake output datasets when configured.
func (h *fakeHandler) OutputDatasets() []lineage.Dataset {
	return append([]lineage.Dataset(nil), h.outputs...)
}

// Open records handler opening.
func (h *fakeHandler) Open(ctx context.Context) error {
	return ctx.Err()
}

// Handle records one handled event.
func (h *fakeHandler) Handle(ctx context.Context, event cloudevents.Event) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	h.events = append(h.events, event)
	return nil
}

// Close records handler closure.
func (h *fakeHandler) Close(ctx context.Context) error {
	h.closed = true
	return ctx.Err()
}

// fakeBatchHandler records batch handled events.
type fakeBatchHandler struct {
	fakeHandler
	batchCalls int
}

// fakeLineageEmitter records lineage events from the consume service.
type fakeLineageEmitter struct {
	events []lineage.Event
}

// Emit records one lineage event.
func (e *fakeLineageEmitter) Emit(ctx context.Context, event lineage.Event) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	e.events = append(e.events, event)
	return nil
}

// HandleBatch records one handled event batch.
func (h *fakeBatchHandler) HandleBatch(ctx context.Context, events []cloudevents.Event) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	h.batchCalls++
	h.events = append(h.events, events...)
	return nil
}

// consumeTestEvent constructs a valid CloudEvent for consumer service tests.
func consumeTestEvent(t *testing.T, id string, eventType string) cloudevents.Event {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource("urn:test")
	evt.SetType(eventType)
	evt.SetTime(fixedConsumeTime())
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]any{"id": id}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("validate event: %v", err)
	}
	return evt
}

// fixedConsumeTime returns a stable timestamp for tests.
func fixedConsumeTime() time.Time {
	return time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
}
