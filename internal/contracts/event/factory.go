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
		source = "urn:datascape:generator"
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
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource(f.Source)
	evt.SetType(fact.Kind)
	evt.SetSubject(fact.Subject)
	evt.SetTime(f.Now().UTC())
	if err := evt.SetData(cloudevents.ApplicationJSON, fact.Data); err != nil {
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
