package redpanda

import (
	"reflect"
	"testing"
)

// TestFromEnvReadsConsumerSettings verifies Redpanda consumer settings come from environment variables.
func TestFromEnvReadsConsumerSettings(t *testing.T) {
	t.Setenv("EVENTFLOW_REDPANDA_BROKERS", "a:9092,b:9092")
	t.Setenv("EVENTFLOW_REDPANDA_TOPIC", "custom.events.v1")
	t.Setenv("EVENTFLOW_REDPANDA_CONSUMER_GROUP", "group-a")
	t.Setenv("EVENTFLOW_REDPANDA_CONSUMER_START_OFFSET", "latest")
	got := FromEnv()
	if !reflect.DeepEqual(got.Brokers, []string{"a:9092", "b:9092"}) || got.Topic != "custom.events.v1" || got.GroupID != "group-a" || got.StartOffset != "latest" {
		t.Fatalf("unexpected config: %+v", got)
	}
}

// TestKafkaStartOffsetNormalizesValues verifies string offsets map to kafka-go constants.
func TestKafkaStartOffsetNormalizesValues(t *testing.T) {
	if kafkaStartOffset("latest") == kafkaStartOffset("first") {
		t.Fatal("expected latest and first offsets to differ")
	}
}
