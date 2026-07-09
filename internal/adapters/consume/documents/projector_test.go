package documents

import (
	"context"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestProjectorWritesTextDocumentArtifacts verifies document upload events become text artifacts.
func TestProjectorWritesTextDocumentArtifacts(t *testing.T) {
	store := &fakeStore{files: map[string]string{}}
	projector := NewWithStore(Config{Dir: "ignored"}, store)
	evt := documentsTestEvent(t, "1", "document.uploaded.v1", map[string]any{
		"filename":        "../homework.txt",
		"content_preview": "Synthetic homework",
	})

	if err := projector.Handle(context.Background(), evt); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	if store.files["homework.txt"] != "Synthetic homework" {
		t.Fatalf("unexpected document files: %+v", store.files)
	}
}

// TestProjectorIgnoresNonDocumentEvents verifies unrelated events are skipped.
func TestProjectorIgnoresNonDocumentEvents(t *testing.T) {
	store := &fakeStore{files: map[string]string{}}
	projector := NewWithStore(Config{Dir: "ignored"}, store)
	if err := projector.Handle(context.Background(), documentsTestEvent(t, "1", "grade.recorded.v1", map[string]any{})); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if len(store.files) != 0 {
		t.Fatalf("expected no files, got %+v", store.files)
	}
}

// fakeStore records written text artifacts.
type fakeStore struct {
	files map[string]string
}

// WriteText records one text artifact.
func (s *fakeStore) WriteText(ctx context.Context, name string, content string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.files == nil {
		s.files = map[string]string{}
	}
	s.files[name] = content
	return nil
}

// documentsTestEvent constructs a valid CloudEvent for document projector tests.
func documentsTestEvent(t *testing.T, id string, eventType string, data map[string]any) cloudevents.Event {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource("urn:test")
	evt.SetType(eventType)
	evt.SetSubject(id)
	evt.SetTime(time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("validate event: %v", err)
	}
	return evt
}
