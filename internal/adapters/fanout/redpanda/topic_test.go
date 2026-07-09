package redpanda

import (
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestTopicForSingleMode verifies that single topic mode always resolves the configured topic.
func TestTopicForSingleMode(t *testing.T) {
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetType("school.registered.v1")
	topic, err := TopicFor(Config{Topic: "datascape.events.v1", TopicMode: "single"}, evt)
	if err != nil {
		t.Fatalf("TopicFor returned error: %v", err)
	}
	if topic != "datascape.events.v1" {
		t.Fatalf("topic = %q, want datascape.events.v1", topic)
	}
}

// TestTopicForTypePrefixMode verifies that type-prefix mode derives a topic from the event type.
func TestTopicForTypePrefixMode(t *testing.T) {
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetType("attendance.submitted.v1")
	topic, err := TopicFor(Config{TopicMode: "type-prefix"}, evt)
	if err != nil {
		t.Fatalf("TopicFor returned error: %v", err)
	}
	if topic != "attendance.events.v1" {
		t.Fatalf("topic = %q, want attendance.events.v1", topic)
	}
}

// TestTopicForRejectsMissingSingleTopic verifies static topic mode requires a topic name.
func TestTopicForRejectsMissingSingleTopic(t *testing.T) {
	_, err := TopicFor(Config{TopicMode: "single"}, cloudevents.NewEvent(cloudevents.VersionV1))
	if err == nil || !strings.Contains(err.Error(), "topic is required") {
		t.Fatalf("expected missing topic error, got %v", err)
	}
}

// TestTopicForRejectsUnsupportedMode verifies unknown topic routing modes fail fast.
func TestTopicForRejectsUnsupportedMode(t *testing.T) {
	_, err := TopicFor(Config{TopicMode: "unknown"}, cloudevents.NewEvent(cloudevents.VersionV1))
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported mode error, got %v", err)
	}
}

// TestIsSingleTopicMode verifies topic mode normalization.
func TestIsSingleTopicMode(t *testing.T) {
	if !IsSingleTopicMode("") || !IsSingleTopicMode(" single ") || IsSingleTopicMode("type-prefix") {
		t.Fatal("unexpected single-topic mode classification")
	}
}
