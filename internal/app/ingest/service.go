// Package ingest coordinates producer-facing domain event publication.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/rezarajan/eventflow/internal/contracts/event"
	"github.com/rezarajan/eventflow/internal/contracts/registry"
	"github.com/rezarajan/eventflow/internal/lineage"
	port "github.com/rezarajan/eventflow/internal/ports/fanout"
)

// Validator validates a producer-submitted domain payload.
type Validator interface {
	Validate(ctx context.Context, registered registry.Event, payload map[string]any) error
}

// Service validates producer payloads, wraps them as CloudEvents, and publishes them.
type Service struct {
	Registry  registry.Registry
	Factory   event.Factory
	Validator Validator
	Publisher port.Publisher
	Lineage   lineage.Emitter
	LineageNS string
	Logger    *slog.Logger
	Now       func() time.Time
}

// PublishRequest contains one producer-submitted domain event.
type PublishRequest struct {
	EventType     string
	Source        string
	Subject       string
	RunID         string
	CorrelationID string
	CausationID   string
	Tenant        string
	Payload       map[string]any
}

// PublishResult describes the accepted CloudEvent and its canonical channel.
type PublishResult struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	Source    string `json:"source"`
	Subject   string `json:"subject,omitempty"`
	Channel   string `json:"channel"`
}

// ValidationError marks producer-correctable publish request failures.
type ValidationError struct {
	Message string
}

// Error returns the validation error message.
func (e ValidationError) Error() string {
	return e.Message
}

// IsValidationError reports whether an error should be returned as a producer validation failure.
func IsValidationError(err error) bool {
	var validationErr ValidationError
	return errors.As(err, &validationErr)
}

// Publish validates and publishes a domain event while keeping lineage separate from the CloudEvent.
func (s Service) Publish(ctx context.Context, request PublishRequest) (PublishResult, error) {
	if s.Publisher == nil {
		return PublishResult{}, fmt.Errorf("publisher is required")
	}
	if !s.Registry.HasEvents() {
		return PublishResult{}, fmt.Errorf("event registry is required")
	}
	registered, err := s.Registry.MustLookup(request.EventType)
	if err != nil {
		return PublishResult{}, ValidationError{Message: err.Error()}
	}
	if request.Payload == nil {
		return PublishResult{}, ValidationError{Message: "payload must be a JSON object"}
	}
	if s.Validator != nil {
		if err := s.Validator.Validate(ctx, registered, request.Payload); err != nil {
			return PublishResult{}, ValidationError{Message: fmt.Sprintf("validate %s payload: %v", request.EventType, err)}
		}
	}
	factory := s.Factory
	if factory.Source == "" {
		factory.Source = "urn:eventflow:ingress:http"
	}
	evt, err := factory.FromPayload(event.Metadata{
		Source:        request.Source,
		Type:          request.EventType,
		Subject:       request.Subject,
		RunID:         request.RunID,
		CorrelationID: request.CorrelationID,
		CausationID:   request.CausationID,
		Tenant:        request.Tenant,
		Time:          s.now(),
	}, request.Payload)
	if err != nil {
		return PublishResult{}, ValidationError{Message: err.Error()}
	}
	runID := request.RunID
	if runID == "" {
		runID = "ingress-" + evt.ID()
	}
	inputs := []lineage.Dataset{{Namespace: "http", Name: "/v1/events/" + request.EventType}}
	outputs := []lineage.Dataset{{Namespace: "redpanda", Name: registered.Channel}}
	if err := s.emitLineage(ctx, "START", runID, inputs, outputs, nil); err != nil {
		return PublishResult{}, err
	}
	publishErr := s.Publisher.Publish(ctx, evt)
	eventType := "COMPLETE"
	if publishErr != nil {
		eventType = "FAIL"
	}
	if err := s.emitLineage(ctx, eventType, runID, inputs, outputs, publishErr); err != nil {
		return PublishResult{}, err
	}
	if publishErr != nil {
		return PublishResult{}, fmt.Errorf("publish CloudEvent %s: %w", evt.ID(), publishErr)
	}
	s.logger().Info("ingress_published", "event_id", evt.ID(), "event_type", evt.Type(), "channel", registered.Channel)
	return PublishResult{EventID: evt.ID(), EventType: evt.Type(), Source: evt.Source(), Subject: evt.Subject(), Channel: registered.Channel}, nil
}

// emitLineage emits one ingress lineage event when lineage is configured.
func (s Service) emitLineage(ctx context.Context, eventType string, runID string, inputs []lineage.Dataset, outputs []lineage.Dataset, runErr error) error {
	if s.Lineage == nil {
		return nil
	}
	namespace := s.LineageNS
	if namespace == "" {
		namespace = "eventflow"
	}
	return s.Lineage.Emit(ctx, lineage.NewEvent(eventType, namespace, "eventflow-ingress-http", runID, inputs, outputs, runErr, s.Now))
}

// logger returns a usable structured logger.
func (s Service) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// now returns the configured clock time.
func (s Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
