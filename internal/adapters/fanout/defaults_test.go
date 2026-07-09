package fanout

import (
	"bytes"
	"log/slog"
	"reflect"
	"testing"
)

// TestDefaultsRegistersExpectedPublishers verifies CLI defaults include only adapter names and do not open external connections.
func TestDefaultsRegistersExpectedPublishers(t *testing.T) {
	registry, err := Defaults(&bytes.Buffer{}, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatalf("Defaults returned error: %v", err)
	}
	want := []string{"discard", "log", "redpanda", "stdout"}
	if got := registry.Names(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Names = %v, want %v", got, want)
	}
}
