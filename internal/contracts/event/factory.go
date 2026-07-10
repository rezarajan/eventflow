package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Factory converts generated facts into CloudEvents SDK events.
type Factory struct {
	RunID  string
	Source string
	Now    func() time.Time
}

// NewFactory constructs an event factory with deterministic metadata defaults.
func NewFactory(runID string, source string, now func() time.Time) Factory {
	if source == "" {
		source = "urn:eventflow:generator"
	}
	if now == nil {
		now = time.Now
	}
	return Factory{RunID: runID, Source: source, Now: now}
}

// FromFact converts a generated fact into a CloudEvents v1.0 event.
func (f Factory) FromFact(sequence int, fact Fact) (cloudevents.Event, error) {
	if fact.Kind == "" {
		return cloudevents.Event{}, fmt.Errorf("fact kind is required")
	}
	if fact.Data == nil {
		fact.Data = map[string]any{}
	}
	id, err := stableID(f.RunID, sequence, fact)
	if err != nil {
		return cloudevents.Event{}, err
	}
	return f.FromPayload(Metadata{ID: id, Type: fact.Kind, Subject: fact.Subject, RunID: f.RunID}, fact.Data)
}

// FromPayload converts a domain payload and platform metadata into a CloudEvents v1.0 event.
func (f Factory) FromPayload(metadata Metadata, payload map[string]any) (cloudevents.Event, error) {
	if metadata.Type == "" {
		return cloudevents.Event{}, fmt.Errorf("event type is required")
	}
	if payload == nil {
		payload = map[string]any{}
	}
	source := metadata.Source
	if source == "" {
		source = f.Source
	}
	if source == "" {
		source = "urn:eventflow:generator"
	}
	eventTime := metadata.Time
	if eventTime.IsZero() {
		now := f.Now
		if now == nil {
			now = time.Now
		}
		eventTime = now().UTC()
	}
	id := metadata.ID
	if id == "" {
		var err error
		id, err = stablePayloadID(source, metadata.Type, metadata.Subject, payload)
		if err != nil {
			return cloudevents.Event{}, err
		}
	}
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource(source)
	evt.SetType(metadata.Type)
	evt.SetSubject(metadata.Subject)
	evt.SetTime(eventTime.UTC())
	setExtension(&evt, "runid", firstNonEmpty(metadata.RunID, f.RunID))
	setExtension(&evt, "correlationid", metadata.CorrelationID)
	setExtension(&evt, "causationid", metadata.CausationID)
	setExtension(&evt, "tenant", metadata.Tenant)
	if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
		return cloudevents.Event{}, fmt.Errorf("set CloudEvents data: %w", err)
	}
	if err := evt.Validate(); err != nil {
		return cloudevents.Event{}, fmt.Errorf("validate CloudEvent: %w", err)
	}
	return evt, nil
}

// stableID creates a deterministic event identifier from the run, sequence, and fact payload.
func stableID(runID string, sequence int, fact Fact) (string, error) {
	body, err := json.Marshal(fact)
	if err != nil {
		return "", fmt.Errorf("marshal fact for event id: %w", err)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", runID, sequence, body)))
	return hex.EncodeToString(sum[:16]), nil
}

// stablePayloadID creates a deterministic event identifier for producer-submitted payloads.
func stablePayloadID(source string, eventType string, subject string, payload map[string]any) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload for event id: %w", err)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s:%s", source, eventType, subject, body)))
	return hex.EncodeToString(sum[:16]), nil
}

// setExtension sets a CloudEvents extension only when the value is present.
func setExtension(evt *cloudevents.Event, key string, value string) {
	if value != "" {
		evt.SetExtension(key, value)
	}
}

// firstNonEmpty returns the first non-empty string in order.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
