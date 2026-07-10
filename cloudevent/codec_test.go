package cloudevent

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rezarajan/project-datascape/adaptertest"
)

func TestStructuredJSONCodecContract(t *testing.T) {
	adaptertest.RunCodecContract(t, StructuredJSONCodec{}, adaptertest.NewTestEvent())
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
