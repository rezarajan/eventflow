// Package cloudevent provides CloudEvents helpers for an OpenLineage admission and quarantine gateway for shared data-platform infrastructure.
package cloudevent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	sdk "github.com/cloudevents/sdk-go/v2"

	eventflow "github.com/rezarajan/eventflow"
)

// StructuredJSONCodec encodes and decodes application/cloudevents+json events.
type StructuredJSONCodec struct{}

// Encode writes one structured CloudEvent JSON document.
func (StructuredJSONCodec) Encode(ctx context.Context, writer io.Writer, event eventflow.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := event.Validate(); err != nil {
		return eventflow.ValidationError("validate cloudevent", err)
	}
	return json.NewEncoder(writer).Encode(event)
}

// Decode reads one structured CloudEvent JSON document.
func (StructuredJSONCodec) Decode(ctx context.Context, reader io.Reader) (eventflow.Event, error) {
	select {
	case <-ctx.Done():
		return eventflow.Event{}, ctx.Err()
	default:
	}
	var event sdk.Event
	if err := json.NewDecoder(reader).Decode(&event); err != nil {
		return eventflow.Event{}, fmt.Errorf("decode structured CloudEvent: %w", err)
	}
	if err := event.Validate(); err != nil {
		return eventflow.Event{}, eventflow.ValidationError("validate cloudevent", err)
	}
	return event, nil
}

// Wrap constructs a CloudEvents v1 event with JSON data.
func Wrap(eventType string, source string, subject string, data any) (eventflow.Event, error) {
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetType(eventType)
	event.SetSource(source)
	event.SetSubject(subject)
	if err := event.SetData(sdk.ApplicationJSON, data); err != nil {
		return eventflow.Event{}, err
	}
	if err := event.Validate(); err != nil {
		return eventflow.Event{}, err
	}
	return event, nil
}

// FromBinaryHTTPRequest decodes a CloudEvent from HTTP binary binding headers.
func FromBinaryHTTPRequest(r *http.Request) (eventflow.Event, error) {
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID(strings.TrimSpace(r.Header.Get("ce-id")))
	event.SetType(strings.TrimSpace(r.Header.Get("ce-type")))
	event.SetSource(strings.TrimSpace(r.Header.Get("ce-source")))
	event.SetSubject(strings.TrimSpace(r.Header.Get("ce-subject")))
	contentType := r.Header.Get("content-type")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return eventflow.Event{}, err
	}
	if err := event.SetData(contentType, body); err != nil {
		return eventflow.Event{}, err
	}
	if err := event.Validate(); err != nil {
		return eventflow.Event{}, eventflow.ValidationError("validate binary cloudevent", err)
	}
	return event, nil
}

// AddBinaryHTTPHeaders writes CloudEvents binary binding headers.
func AddBinaryHTTPHeaders(header http.Header, event eventflow.Event) {
	header.Set("ce-specversion", event.SpecVersion())
	header.Set("ce-id", event.ID())
	header.Set("ce-type", event.Type())
	header.Set("ce-source", event.Source())
	if event.Subject() != "" {
		header.Set("ce-subject", event.Subject())
	}
	for key, value := range event.Extensions() {
		header.Set("ce-"+key, fmt.Sprint(value))
	}
	if event.DataContentType() != "" {
		header.Set("content-type", event.DataContentType())
	}
}

// ValidateExtensions rejects empty extension names and reserved ce- prefixes.
func ValidateExtensions(event eventflow.Event) error {
	for key := range event.Extensions() {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" || strings.HasPrefix(strings.ToLower(trimmed), "ce-") {
			return fmt.Errorf("invalid CloudEvents extension %q", key)
		}
	}
	return nil
}
