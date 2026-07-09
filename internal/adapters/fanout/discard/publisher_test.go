package discard

import (
	"context"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestPublisherLifecycle verifies discard accepts Open, Publish, and Close without side effects.
func TestPublisherLifecycle(t *testing.T) {
	publisher := New()
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := publisher.Publish(context.Background(), discardTestEvent(t)); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if err := publisher.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

// TestPublisherHonorsCanceledContext verifies cancellation is respected.
func TestPublisherHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := New().Open(ctx); err == nil {
		t.Fatal("expected Open cancellation error")
	}
	if err := New().Publish(ctx, discardTestEvent(t)); err == nil {
		t.Fatal("expected Publish cancellation error")
	}
	if err := New().Close(ctx); err == nil {
		t.Fatal("expected Close cancellation error")
	}
}

// discardTestEvent constructs a valid event for discard tests.
func discardTestEvent(t *testing.T) cloudevents.Event {
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
