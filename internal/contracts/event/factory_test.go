package event

import (
	"testing"
	"time"
)

// TestFactoryUsesCloudEventsSDK verifies that facts become valid SDK CloudEvents.
func TestFactoryUsesCloudEventsSDK(t *testing.T) {
	fixed := time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC)
	factory := NewFactory("run-1", "urn:test", func() time.Time { return fixed })
	evt, err := factory.FromFact(1, Fact{Kind: "thing.created.v1", Subject: "thing-1", Data: map[string]any{"thing_id": "thing-1"}})
	if err != nil {
		t.Fatalf("FromFact returned error: %v", err)
	}
	if evt.SpecVersion() != "1.0" || evt.Type() != "thing.created.v1" || evt.Source() != "urn:test" || evt.Subject() != "thing-1" {
		t.Fatalf("unexpected event metadata: spec=%s type=%s source=%s subject=%s", evt.SpecVersion(), evt.Type(), evt.Source(), evt.Subject())
	}
	if !evt.Time().Equal(fixed) {
		t.Fatalf("event time = %s, want %s", evt.Time(), fixed)
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("expected valid CloudEvent: %v", err)
	}
}

// TestFactoryStableIDsAreDeterministic verifies stable identifiers for identical input.
func TestFactoryStableIDsAreDeterministic(t *testing.T) {
	factory := NewFactory("run-1", "urn:test", func() time.Time { return time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC) })
	fact := Fact{Kind: "thing.created.v1", Subject: "thing-1", Data: map[string]any{"thing_id": "thing-1"}}
	first, err := factory.FromFact(1, fact)
	if err != nil {
		t.Fatalf("FromFact returned error: %v", err)
	}
	second, err := factory.FromFact(1, fact)
	if err != nil {
		t.Fatalf("FromFact returned error: %v", err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("event IDs differ: %s != %s", first.ID(), second.ID())
	}
}

// TestFactoryRejectsMissingKind verifies event type is required.
func TestFactoryRejectsMissingKind(t *testing.T) {
	_, err := NewFactory("run-1", "urn:test", nil).FromFact(1, Fact{})
	if err == nil {
		t.Fatal("expected missing kind error")
	}
}

// TestFactoryDefaultsSource verifies an empty source receives a valid default.
func TestFactoryDefaultsSource(t *testing.T) {
	evt, err := NewFactory("run-1", "", func() time.Time { return time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC) }).FromFact(1, Fact{Kind: "thing.created.v1"})
	if err != nil {
		t.Fatalf("FromFact returned error: %v", err)
	}
	if evt.Source() != "urn:datascape:generator" {
		t.Fatalf("source = %q, want default", evt.Source())
	}
}
