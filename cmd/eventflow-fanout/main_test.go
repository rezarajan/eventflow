package main

import (
	"context"
	"strings"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	port "github.com/datascape/eventflow/internal/ports/fanout"
	"github.com/datascape/eventflow/internal/testkit/fakes"
)

// TestCreatePublishersParsesCommaSeparatedOutputs verifies CLI output selection uses the registry only.
func TestCreatePublishersParsesCommaSeparatedOutputs(t *testing.T) {
	factory := fakeFanoutFactory{publishers: map[string]port.Publisher{"a": &fakes.Publisher{PublisherName: "a"}, "b": &fakes.Publisher{PublisherName: "b"}}}
	publishers, err := createPublishers(factory, " a, b ")
	if err != nil {
		t.Fatalf("createPublishers returned error: %v", err)
	}
	if len(publishers) != 2 || publishers[0].Name() != "a" || publishers[1].Name() != "b" {
		t.Fatalf("unexpected publishers: %+v", publishers)
	}
}

// TestCreatePublishersRejectsEmptyOutputs verifies an empty output list fails fast.
func TestCreatePublishersRejectsEmptyOutputs(t *testing.T) {
	_, err := createPublishers(fakeFanoutFactory{}, ", ,")
	if err == nil || !strings.Contains(err.Error(), "at least one output") {
		t.Fatalf("expected empty output error, got %v", err)
	}
}

// TestCreatePublishersReturnsFactoryErrors verifies unknown output names are surfaced.
func TestCreatePublishersReturnsFactoryErrors(t *testing.T) {
	_, err := createPublishers(fakeFanoutFactory{publishers: map[string]port.Publisher{}}, "missing")
	if err == nil || !strings.Contains(err.Error(), "unknown fake output") {
		t.Fatalf("expected fake output error, got %v", err)
	}
}

// TestOutputDatasetsStitchesRedpandaTopic verifies fan-out Redpanda lineage matches consumer input naming.
func TestOutputDatasetsStitchesRedpandaTopic(t *testing.T) {
	t.Setenv("DATASCAPE_REDPANDA_BROKERS", "localhost:19092")
	t.Setenv("DATASCAPE_REDPANDA_TOPIC", "example.events.v1")
	datasets := outputDatasets("redpanda,log")
	if len(datasets) != 2 || datasets[0].Namespace != "redpanda://localhost:19092" || datasets[0].Name != "example.events.v1" {
		t.Fatalf("unexpected datasets: %+v", datasets)
	}
}

// fakeFanoutFactory is an in-memory fan-out factory for command tests.
type fakeFanoutFactory struct{ publishers map[string]port.Publisher }

// Names returns configured fake output names.
func (f fakeFanoutFactory) Names() []string { return nil }

// Create returns a configured fake publisher by name.
func (f fakeFanoutFactory) Create(name string) (port.Publisher, error) {
	if publisher, ok := f.publishers[name]; ok {
		return publisher, nil
	}
	return nil, fakeError("unknown fake output " + name)
}

// fakeError is a minimal error type that avoids additional dependencies in command tests.
type fakeError string

// Error returns the fake error text.
func (e fakeError) Error() string { return string(e) }

// compilePublisherInterface ensures the fake publisher package remains compatible with the port.
func compilePublisherInterface(ctx context.Context, publisher port.Publisher, event cloudevents.Event) error {
	return publisher.Publish(ctx, event)
}
