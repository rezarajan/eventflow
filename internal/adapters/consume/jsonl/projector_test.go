package jsonl

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestProjectorWritesRowsByTable verifies domain event types route to expected JSONL tables.
func TestProjectorWritesRowsByTable(t *testing.T) {
	store := &fakeStore{lines: map[string][][]byte{}}
	projector := NewWithStore(Config{Dir: "ignored"}, store)
	events := []cloudevents.Event{
		jsonlTestEvent(t, "1", "school.registered.v1", map[string]any{"school_id": "SCH-001"}),
		jsonlTestEvent(t, "2", "student.enrolled.v1", map[string]any{"student_id": "S-001"}),
		jsonlTestEvent(t, "3", "ignored.v1", map[string]any{"id": "ignored"}),
	}

	if err := projector.HandleBatch(context.Background(), events); err != nil {
		t.Fatalf("HandleBatch returned error: %v", err)
	}

	if len(store.lines["schools.jsonl"]) != 1 || len(store.lines["students.jsonl"]) != 1 {
		t.Fatalf("unexpected table writes: %+v", store.lines)
	}
	row := map[string]any{}
	if err := json.Unmarshal(store.lines["schools.jsonl"][0], &row); err != nil {
		t.Fatalf("unmarshal row: %v", err)
	}
	if row["event_id"] != "1" || row["school_id"] != "SCH-001" {
		t.Fatalf("unexpected row: %+v", row)
	}
}

// fakeStore records appended JSONL lines by table.
type fakeStore struct {
	lines map[string][][]byte
}

// AppendLines records lines for one table.
func (s *fakeStore) AppendLines(ctx context.Context, table string, lines [][]byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.lines == nil {
		s.lines = map[string][][]byte{}
	}
	s.lines[table] = append(s.lines[table], lines...)
	return nil
}

// jsonlTestEvent constructs a valid CloudEvent for projector tests.
func jsonlTestEvent(t *testing.T, id string, eventType string, data map[string]any) cloudevents.Event {
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
