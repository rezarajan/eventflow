package stdout

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestPublisherWritesJSONL verifies stdout publisher encodes one CloudEvent per line.
func TestPublisherWritesJSONL(t *testing.T) {
	var buffer bytes.Buffer
	publisher := New(&buffer)
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := publisher.Publish(context.Background(), stdoutTestEvent(t)); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if !strings.Contains(buffer.String(), "thing.created.v1") || !strings.HasSuffix(buffer.String(), "\n") {
		t.Fatalf("unexpected output: %q", buffer.String())
	}
}

// TestPublisherRequiresWriter verifies nil writers are rejected.
func TestPublisherRequiresWriter(t *testing.T) {
	err := New(nil).Publish(context.Background(), stdoutTestEvent(t))
	if err == nil || !strings.Contains(err.Error(), "writer is required") {
		t.Fatalf("expected writer error, got %v", err)
	}
}

// stdoutTestEvent constructs a valid event for stdout tests.
func stdoutTestEvent(t *testing.T) cloudevents.Event {
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
