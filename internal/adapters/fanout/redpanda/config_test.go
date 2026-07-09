package redpanda

import (
	"reflect"
	"testing"
)

// TestSplitCSVTrimsEmptyValues verifies broker parsing is stable and whitespace-tolerant.
func TestSplitCSVTrimsEmptyValues(t *testing.T) {
	got := splitCSV(" broker-1:9092, ,broker-2:9092 ")
	want := []string{"broker-1:9092", "broker-2:9092"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitCSV = %v, want %v", got, want)
	}
}

// TestFromEnvReadsRedpandaSettings verifies 12FA-style configuration via environment variables.
func TestFromEnvReadsRedpandaSettings(t *testing.T) {
	t.Setenv("DATASCAPE_REDPANDA_BROKERS", "a:9092,b:9092")
	t.Setenv("DATASCAPE_REDPANDA_TOPIC", "custom.events.v1")
	t.Setenv("DATASCAPE_REDPANDA_TOPIC_MODE", "single")
	t.Setenv("DATASCAPE_REDPANDA_BATCH_SIZE", "250")
	got := FromEnv()
	if !reflect.DeepEqual(got.Brokers, []string{"a:9092", "b:9092"}) || got.Topic != "custom.events.v1" || got.TopicMode != "single" || got.BatchSize != 250 {
		t.Fatalf("unexpected config: %+v", got)
	}
}

// TestEnvIntFallsBackForInvalidValues verifies invalid integer configuration uses the safe default.
func TestEnvIntFallsBackForInvalidValues(t *testing.T) {
	t.Setenv("DATASCAPE_REDPANDA_BATCH_SIZE", "not-an-int")
	if got := envInt("DATASCAPE_REDPANDA_BATCH_SIZE", 100); got != 100 {
		t.Fatalf("envInt = %d, want 100", got)
	}
}
