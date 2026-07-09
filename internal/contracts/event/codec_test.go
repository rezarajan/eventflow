package event

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestEncodeDecodeJSONLRoundTrip verifies JSONL serialization with SDK CloudEvents.
func TestEncodeDecodeJSONLRoundTrip(t *testing.T) {
	events := make(chan cloudevents.Event, 2)
	events <- codecTestEvent(t, "1", "thing.created.v1")
	events <- codecTestEvent(t, "2", "thing.updated.v1")
	close(events)
	var buffer bytes.Buffer
	if err := EncodeJSONL(context.Background(), &buffer, events); err != nil {
		t.Fatalf("EncodeJSONL returned error: %v", err)
	}
	out := make(chan cloudevents.Event, 2)
	if err := DecodeJSONL(context.Background(), &buffer, out); err != nil {
		t.Fatalf("DecodeJSONL returned error: %v", err)
	}
	var decoded []cloudevents.Event
	for evt := range out {
		decoded = append(decoded, evt)
	}
	if len(decoded) != 2 || decoded[0].ID() != "1" || decoded[1].ID() != "2" {
		t.Fatalf("unexpected decoded events: %+v", decoded)
	}
}

// TestDecodeJSONLRejectsMalformedJSON verifies invalid JSON is surfaced.
func TestDecodeJSONLRejectsMalformedJSON(t *testing.T) {
	out := make(chan cloudevents.Event, 1)
	err := DecodeJSONL(context.Background(), strings.NewReader("{bad-json}\n"), out)
	if err == nil || !strings.Contains(err.Error(), "decode CloudEvent") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

// TestDecodeJSONLRejectsInvalidCloudEvent verifies structurally invalid events are surfaced.
func TestDecodeJSONLRejectsInvalidCloudEvent(t *testing.T) {
	out := make(chan cloudevents.Event, 1)
	err := DecodeJSONL(context.Background(), strings.NewReader(`{"specversion":"1.0","id":"1"}`+"\n"), out)
	if err == nil || !strings.Contains(err.Error(), "validate CloudEvent") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

// TestEncodeJSONLReturnsCanceledContext verifies context cancellation is respected before writes.
func TestEncodeJSONLReturnsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := make(chan cloudevents.Event, 1)
	events <- codecTestEvent(t, "1", "thing.created.v1")
	err := EncodeJSONL(ctx, &bytes.Buffer{}, events)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

// codecTestEvent constructs a valid event for codec tests.
func codecTestEvent(t *testing.T, id string, eventType string) cloudevents.Event {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource("urn:test")
	evt.SetType(eventType)
	evt.SetTime(time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]any{"id": id}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("validate event: %v", err)
	}
	return evt
}
