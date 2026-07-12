package redpanda

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/segmentio/kafka-go"
)

// TestSourceReadsCloudEventsAndCommits verifies Redpanda messages decode as CloudEvents and are committed.
func TestSourceReadsCloudEventsAndCommits(t *testing.T) {
	body := redpandaConsumerEventBody(t, "1", "example.created.v1")
	reader := &fakeReader{messages: []kafka.Message{{Value: body}}}
	source := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "events", GroupID: "group"}, fakeReaderFactory{reader: reader})
	if err := source.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	events, err := source.ReadBatch(context.Background(), 1)
	if err != nil {
		t.Fatalf("ReadBatch returned error: %v", err)
	}
	if len(events) != 1 || events[0].ID() != "1" || reader.commits != 1 {
		t.Fatalf("unexpected read result: events=%+v commits=%d", events, reader.commits)
	}
	if err := source.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !reader.closed {
		t.Fatal("expected reader to close")
	}
}

func TestSourceReadBatchAckCommitsOnlyWhenCallbackRuns(t *testing.T) {
	body := redpandaConsumerEventBody(t, "1", "example.created.v1")
	reader := &fakeReader{messages: []kafka.Message{{Value: body}}}
	source := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "events", GroupID: "group"}, fakeReaderFactory{reader: reader})
	if err := source.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	events, err := source.ReadBatchAck(context.Background(), 1)
	if err != nil {
		t.Fatalf("ReadBatchAck returned error: %v", err)
	}
	if len(events) != 1 || reader.commits != 0 {
		t.Fatalf("unexpected read result: events=%+v commits=%d", events, reader.commits)
	}
	if err := events[0].Commit(context.Background()); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if reader.commits != 1 {
		t.Fatalf("commits = %d, want 1", reader.commits)
	}
}

// TestSourceRequiresOpenBeforeRead verifies the explicit lifecycle is enforced.
func TestSourceRequiresOpenBeforeRead(t *testing.T) {
	source := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "events", GroupID: "group"}, fakeReaderFactory{})
	if _, err := source.ReadBatch(context.Background(), 1); err == nil {
		t.Fatal("expected read before open error")
	}
}

// fakeReaderFactory returns a configured fake reader.
type fakeReaderFactory struct {
	reader Reader
}

// NewReader returns the configured fake reader.
func (f fakeReaderFactory) NewReader(config Config) (Reader, error) {
	return f.reader, nil
}

// fakeReader records consumed and committed messages.
type fakeReader struct {
	messages []kafka.Message
	offset   int
	commits  int
	closed   bool
}

// FetchMessage returns the next fake message or EOF.
func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if ctx.Err() != nil {
		return kafka.Message{}, ctx.Err()
	}
	if r.offset >= len(r.messages) {
		return kafka.Message{}, io.EOF
	}
	message := r.messages[r.offset]
	r.offset++
	return message, nil
}

// CommitMessages records committed messages.
func (r *fakeReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	r.commits += len(messages)
	return nil
}

// Close records reader closure.
func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

// redpandaConsumerEventBody constructs a serialized CloudEvent for source tests.
func redpandaConsumerEventBody(t *testing.T, id string, eventType string) []byte {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource("urn:test")
	evt.SetType(eventType)
	evt.SetTime(time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]any{"id": id}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return body
}
