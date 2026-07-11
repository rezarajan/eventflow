// Package adaptertest provides reusable conformance checks for Eventflow adapters.
package adaptertest

import (
	"bytes"
	"context"
	"io"
	"testing"

	sdk "github.com/cloudevents/sdk-go/v2"

	eventflow "github.com/rezarajan/eventflow"
)

// RunEmitterContract verifies the minimal emitter lifecycle.
func RunEmitterContract(t *testing.T, emitter eventflow.Emitter, event eventflow.Event) {
	t.Helper()
	ctx := context.Background()
	if err := emitter.Open(ctx); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := emitter.Emit(ctx, event); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := emitter.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// RunReceiverContract verifies a receiver can open, receive one valid event, and close.
func RunReceiverContract(t *testing.T, receiver eventflow.Receiver) {
	t.Helper()
	ctx := context.Background()
	if err := receiver.Open(ctx); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	event, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("received invalid event: %v", err)
	}
	if err := receiver.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// RunObserverContract verifies a basic observation lifecycle.
func RunObserverContract(t *testing.T, observer eventflow.Observer) {
	t.Helper()
	ctx := context.Background()
	if err := observer.Open(ctx); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := observer.Observe(ctx); err != nil && err != io.EOF {
		t.Fatalf("Observe() error = %v", err)
	}
	if err := observer.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// RunCodecContract verifies codec round-trip validity.
func RunCodecContract(t *testing.T, codec eventflow.Codec, event eventflow.Event) {
	t.Helper()
	var buf bytes.Buffer
	if err := codec.Encode(context.Background(), &buf, event); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := codec.Decode(context.Background(), &buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.ID() != event.ID() || decoded.Type() != event.Type() {
		t.Fatalf("decoded event mismatch: got %s/%s want %s/%s", decoded.ID(), decoded.Type(), event.ID(), event.Type())
	}
}

// NewTestEvent returns a valid JSON CloudEvent for contract tests.
func NewTestEvent() eventflow.Event {
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID("evt-test")
	event.SetType("io.eventflow.test.v1")
	event.SetSource("urn:eventflow:test")
	_ = event.SetData(sdk.ApplicationJSON, map[string]any{"ok": true})
	return event
}
