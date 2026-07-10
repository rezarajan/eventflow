package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/rezarajan/project-datascape/internal/contracts/event"
	"github.com/rezarajan/project-datascape/internal/contracts/registry"
	"github.com/rezarajan/project-datascape/internal/lineage"
)

func TestServicePublishesValidatedCloudEvent(t *testing.T) {
	publisher := &fakePublisher{}
	validator := &fakeValidator{}
	emitter := &fakeLineageEmitter{}
	service := Service{
		Registry:  testRegistry(t),
		Factory:   event.NewFactory("", "urn:test:ingress", fixedIngestTime),
		Validator: validator,
		Publisher: publisher,
		Lineage:   emitter,
		Now:       fixedIngestTime,
	}
	result, err := service.Publish(context.Background(), PublishRequest{
		EventType:     "example.created.v1",
		Subject:       "student-1",
		CorrelationID: "corr-1",
		Payload:       map[string]any{"attendance_id": "att-1"},
	})
	if err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if result.EventType != "example.created.v1" || result.Channel != "example.events.v1" {
		t.Fatalf("unexpected publish result: %+v", result)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(publisher.events))
	}
	if publisher.events[0].Extensions()["correlationid"] != "corr-1" {
		t.Fatalf("correlation extension missing: %+v", publisher.events[0].Extensions())
	}
	if validator.calls != 1 {
		t.Fatalf("validator calls = %d, want 1", validator.calls)
	}
	if len(emitter.events) != 2 || emitter.events[0].EventType != "START" || emitter.events[1].EventType != "COMPLETE" {
		t.Fatalf("unexpected lineage events: %+v", emitter.events)
	}
}

func TestServiceReturnsValidationErrorForUnknownType(t *testing.T) {
	service := Service{Registry: testRegistry(t), Publisher: &fakePublisher{}}
	_, err := service.Publish(context.Background(), PublishRequest{EventType: "missing.v1", Payload: map[string]any{}})
	if !IsValidationError(err) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestServiceReturnsValidationErrorForSchemaFailure(t *testing.T) {
	service := Service{
		Registry:  testRegistry(t),
		Validator: &fakeValidator{err: errors.New("required field missing")},
		Publisher: &fakePublisher{},
	}
	_, err := service.Publish(context.Background(), PublishRequest{EventType: "example.created.v1", Payload: map[string]any{}})
	if !IsValidationError(err) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

type fakeValidator struct {
	calls int
	err   error
}

func (v *fakeValidator) Validate(ctx context.Context, spec registry.Event, payload map[string]any) error {
	v.calls++
	return v.err
}

type fakePublisher struct {
	events []cloudevents.Event
	err    error
}

func (p *fakePublisher) Name() string {
	return "fake"
}

func (p *fakePublisher) Open(ctx context.Context) error {
	return nil
}

func (p *fakePublisher) Publish(ctx context.Context, event cloudevents.Event) error {
	p.events = append(p.events, event)
	return p.err
}

func (p *fakePublisher) Close(ctx context.Context) error {
	return nil
}

type fakeLineageEmitter struct {
	events []lineage.Event
}

func (e *fakeLineageEmitter) Emit(ctx context.Context, event lineage.Event) error {
	e.events = append(e.events, event)
	return nil
}

func fixedIngestTime() time.Time {
	return time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC)
}

func testRegistry(t *testing.T) registry.Registry {
	t.Helper()
	registered, err := registry.New([]registry.Event{{
		Type:    "example.created.v1",
		Schema:  "attendance-submitted.v1.schema.json",
		Channel: "example.events.v1",
	}})
	if err != nil {
		t.Fatalf("registry.New returned error: %v", err)
	}
	return registered
}
