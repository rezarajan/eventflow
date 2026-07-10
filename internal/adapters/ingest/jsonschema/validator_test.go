package jsonschema

import (
	"context"
	"strings"
	"testing"

	"github.com/datascape/eventflow/internal/contracts/registry"
)

func TestValidatorAcceptsValidPayload(t *testing.T) {
	spec := attendanceSubmittedEvent()
	payload := map[string]any{
		"attendance_id":   "11111111-1111-1111-1111-111111111111",
		"student_id":      "22222222-2222-2222-2222-222222222222",
		"class_id":        "33333333-3333-3333-3333-333333333333",
		"school_id":       "44444444-4444-4444-4444-444444444444",
		"attendance_date": "2026-07-09",
		"status_code":     "PRESENT",
		"submitted_at":    "2026-07-09T01:00:00Z",
	}
	if err := New().Validate(context.Background(), spec, payload); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestValidatorRejectsInvalidPayload(t *testing.T) {
	spec := attendanceSubmittedEvent()
	err := New().Validate(context.Background(), spec, map[string]any{"status_code": "UNKNOWN"})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected schema validation error, got %v", err)
	}
}

func TestValidatorAssertsFormats(t *testing.T) {
	spec := attendanceSubmittedEvent()
	payload := map[string]any{
		"attendance_id":   "not-a-uuid",
		"student_id":      "22222222-2222-2222-2222-222222222222",
		"class_id":        "33333333-3333-3333-3333-333333333333",
		"school_id":       "44444444-4444-4444-4444-444444444444",
		"attendance_date": "2026-07-09",
		"status_code":     "PRESENT",
		"submitted_at":    "2026-07-09T01:00:00Z",
	}
	err := New().Validate(context.Background(), spec, payload)
	if err == nil || !strings.Contains(err.Error(), "uuid") {
		t.Fatalf("expected UUID format error, got %v", err)
	}
}

func attendanceSubmittedEvent() registry.Event {
	return registry.Event{
		Type:    "example.created.v1",
		Schema:  "../../../examples/school/contracts/events/payloads/attendance-submitted.v1.schema.json",
		Channel: "example.events.v1",
	}
}
