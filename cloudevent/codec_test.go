package cloudevent

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"
)

func TestStructuredJSONCodecContract(t *testing.T) {
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID("evt-1")
	event.SetType("example.created.v1")
	event.SetSource("urn:test")
	event.SetTime(time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC))
	if err := event.SetData(sdk.ApplicationJSON, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := (StructuredJSONCodec{}).Encode(context.Background(), &buf, event); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	got, err := (StructuredJSONCodec{}).Decode(context.Background(), &buf)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.ID() != event.ID() {
		t.Fatalf("decoded id = %s", got.ID())
	}
}

func TestBinaryHTTPRequestDecode(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"ok":true}`))
	req.Header.Set("ce-id", "evt-1")
	req.Header.Set("ce-type", "example.created.v1")
	req.Header.Set("ce-source", "urn:test")
	req.Header.Set("content-type", "application/json")
	event, err := FromBinaryHTTPRequest(req)
	if err != nil {
		t.Fatalf("FromBinaryHTTPRequest() error = %v", err)
	}
	if event.ID() != "evt-1" || event.Type() != "example.created.v1" {
		t.Fatalf("decoded event = %s/%s", event.ID(), event.Type())
	}
	if err := (StructuredJSONCodec{}).Encode(context.Background(), httptest.NewRecorder().Body, event); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
}
