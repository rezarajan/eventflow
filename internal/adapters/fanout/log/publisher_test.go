package log

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestPublisherLogsMetadata verifies the log publisher records event metadata only.
func TestPublisherLogsMetadata(t *testing.T) {
	var buffer bytes.Buffer
	publisher := New(slog.New(slog.NewJSONHandler(&buffer, nil)))
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := publisher.Publish(context.Background(), logTestEvent(t)); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	out := buffer.String()
	if !strings.Contains(out, "event_published") || !strings.Contains(out, "thing.created.v1") {
		t.Fatalf("unexpected log output: %s", out)
	}
}

// logTestEvent constructs a valid event for log publisher tests.
func logTestEvent(t *testing.T) cloudevents.Event {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID("1")
	evt.SetSource("urn:test")
	evt.SetType("thing.created.v1")
	evt.SetTime(time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]any{"id": "1"}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	return evt
}
