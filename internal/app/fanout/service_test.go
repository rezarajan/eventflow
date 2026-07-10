package fanout

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/rezarajan/project-datascape/internal/contracts/event"
	port "github.com/rezarajan/project-datascape/internal/ports/fanout"
	"github.com/rezarajan/project-datascape/internal/testkit/fakes"
)

// TestServiceBroadcastsToAllPublishers verifies that fan-out broadcasts instead of load-balancing.
func TestServiceBroadcastsToAllPublishers(t *testing.T) {
	input := jsonl(t, []cloudevents.Event{testEvent(t, "1", "thing.created.v1"), testEvent(t, "2", "thing.updated.v1")})
	first := &fakes.Publisher{PublisherName: "first"}
	second := &fakes.Publisher{PublisherName: "second"}
	service := testService([]port.Publisher{first, second})
	summary, err := service.Run(context.Background(), "fanout-1", bytes.NewReader(input))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if first.Count() != 2 || second.Count() != 2 {
		t.Fatalf("expected both publishers to receive both events; got %d and %d", first.Count(), second.Count())
	}
	if !first.Opened || !first.Closed || !second.Opened || !second.Closed {
		t.Fatalf("expected all publishers to be opened and closed")
	}
	if summary.Events != 4 {
		t.Fatalf("expected 4 publish operations, got %d", summary.Events)
	}
}

// TestServiceRejectsEmptyPublishers verifies that fan-out requires at least one output.
func TestServiceRejectsEmptyPublishers(t *testing.T) {
	_, err := testService(nil).Run(context.Background(), "fanout-1", bytes.NewReader(nil))
	if err == nil || !strings.Contains(err.Error(), "at least one publisher") {
		t.Fatalf("expected missing publisher error, got %v", err)
	}
}

// TestServiceOpensBeforePublishing verifies publisher lifecycle ordering.
func TestServiceOpensBeforePublishing(t *testing.T) {
	input := jsonl(t, []cloudevents.Event{testEvent(t, "1", "thing.created.v1")})
	publisher := &fakes.Publisher{PublisherName: "tracked"}
	_, err := testService([]port.Publisher{publisher}).Run(context.Background(), "fanout-1", bytes.NewReader(input))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	want := []string{"open", "publish", "close"}
	if got := publisher.SnapshotCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

// TestServiceClosesOpenedPublishersWhenOpenFails verifies that partially opened publishers are closed.
func TestServiceClosesOpenedPublishersWhenOpenFails(t *testing.T) {
	first := &fakes.Publisher{PublisherName: "first"}
	second := &fakes.Publisher{PublisherName: "second", OpenErr: fmt.Errorf("open failed")}
	_, err := testService([]port.Publisher{first, second}).Run(context.Background(), "fanout-1", bytes.NewReader(nil))
	if err == nil || !strings.Contains(err.Error(), "open publisher second") {
		t.Fatalf("expected open error, got %v", err)
	}
	if !first.Closed {
		t.Fatal("expected first publisher to close after later open failure")
	}
	if second.Closed {
		t.Fatal("did not expect failed publisher to close because it was not opened")
	}
}

// TestServiceClosesPublishersWhenPublishFails verifies that output resources close on publish failure.
func TestServiceClosesPublishersWhenPublishFails(t *testing.T) {
	input := jsonl(t, []cloudevents.Event{testEvent(t, "1", "thing.created.v1")})
	publisher := fakes.FailingPublisher("bad")
	_, err := testService([]port.Publisher{publisher}).Run(context.Background(), "fanout-1", bytes.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "publish to bad") {
		t.Fatalf("expected publish error, got %v", err)
	}
	if !publisher.Closed {
		t.Fatal("expected publisher to close after publish error")
	}
}

// TestServiceReturnsDecodeErrors verifies malformed input is surfaced and publishers close.
func TestServiceReturnsDecodeErrors(t *testing.T) {
	publisher := &fakes.Publisher{PublisherName: "first"}
	_, err := testService([]port.Publisher{publisher}).Run(context.Background(), "fanout-1", strings.NewReader("not-json\n"))
	if err == nil || !strings.Contains(err.Error(), "decode CloudEvents") {
		t.Fatalf("expected decode error, got %v", err)
	}
	if !publisher.Closed {
		t.Fatal("expected publisher to close after decode error")
	}
}

// TestServiceReturnsCloseErrors verifies successful processing still surfaces close failures.
func TestServiceReturnsCloseErrors(t *testing.T) {
	input := jsonl(t, []cloudevents.Event{testEvent(t, "1", "thing.created.v1")})
	publisher := &fakes.Publisher{PublisherName: "first", CloseErr: fmt.Errorf("close failed")}
	_, err := testService([]port.Publisher{publisher}).Run(context.Background(), "fanout-1", bytes.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "close publisher first") {
		t.Fatalf("expected close error, got %v", err)
	}
}

// TestServicePublishesOutputsConcurrently verifies one slow output does not block another output worker.
func TestServicePublishesOutputsConcurrently(t *testing.T) {
	input := jsonl(t, []cloudevents.Event{testEvent(t, "1", "thing.created.v1")})
	gate := make(chan struct{})
	slow := &fakes.Publisher{PublisherName: "slow", PublishGate: gate}
	fast := &fakes.Publisher{PublisherName: "fast"}
	done := make(chan error, 1)
	go func() {
		_, err := testService([]port.Publisher{slow, fast}).Run(context.Background(), "fanout-1", bytes.NewReader(input))
		done <- err
	}()
	waitForCount(t, fast, 1)
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if slow.Count() != 1 || fast.Count() != 1 {
		t.Fatalf("expected both publishers to receive one event; got slow=%d fast=%d", slow.Count(), fast.Count())
	}
}

// TestServiceUsesBatchPublisher verifies batch-capable publishers receive bounded batches.
func TestServiceUsesBatchPublisher(t *testing.T) {
	input := jsonl(t, []cloudevents.Event{
		testEvent(t, "1", "thing.created.v1"),
		testEvent(t, "2", "thing.created.v1"),
		testEvent(t, "3", "thing.created.v1"),
	})
	publisher := &fakes.BatchPublisher{Publisher: fakes.Publisher{PublisherName: "batch"}}
	summary, err := testService([]port.Publisher{publisher}).Run(context.Background(), "fanout-1", bytes.NewReader(input))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(publisher.Batches) != 2 || len(publisher.Batches[0]) != 2 || len(publisher.Batches[1]) != 1 {
		t.Fatalf("unexpected batch sizes: %+v", publisher.Batches)
	}
	if summary.Events != 3 {
		t.Fatalf("expected 3 published events, got %d", summary.Events)
	}
}

// waitForCount waits for an in-memory publisher to record the requested event count.
func waitForCount(t *testing.T, publisher *fakes.Publisher, count int) {
	t.Helper()
	deadline := time.After(500 * time.Millisecond)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("publisher %s did not reach count %d; got %d", publisher.Name(), count, publisher.Count())
		case <-tick.C:
			if publisher.Count() >= count {
				return
			}
		}
	}
}

// testService constructs a fan-out service with a quiet logger and stable clock.
func testService(publishers []port.Publisher) Service {
	fixed := time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC)
	return Service{Publishers: publishers, Logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), Buffer: 16, BatchSize: 2, Now: func() time.Time { return fixed }}
}

// jsonl encodes events as test JSONL without using disk.
func jsonl(t *testing.T, events []cloudevents.Event) []byte {
	t.Helper()
	var buffer bytes.Buffer
	ch := make(chan cloudevents.Event, len(events))
	for _, evt := range events {
		ch <- evt
	}
	close(ch)
	if err := event.EncodeJSONL(context.Background(), &buffer, ch); err != nil {
		t.Fatalf("encode events: %v", err)
	}
	return buffer.Bytes()
}

// testEvent constructs a valid CloudEvents SDK event for fan-out tests.
func testEvent(t *testing.T, id string, eventType string) cloudevents.Event {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource("urn:test")
	evt.SetType(eventType)
	evt.SetTime(time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]any{"id": id}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("validate event: %v", err)
	}
	return evt
}
