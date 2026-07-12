package dispatch_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"
	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/gateway/dispatch"
	"github.com/rezarajan/eventflow/journal"
	sqlitejournal "github.com/rezarajan/eventflow/journal/sqlite"
)

func TestDispatcherRetryAndEventualDelivery(t *testing.T) {
	ctx := context.Background()
	j := sqlitejournal.New(sqlitejournal.Config{Path: filepath.Join(t.TempDir(), "journal.sqlite")})
	if err := j.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer j.Close(ctx)
	record, err := j.Append(ctx, testEvent(t, "event-1"), journal.AppendOptions{Flow: "flow", Destinations: []journal.DestinationID{"out"}})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &flakyEmitter{failures: 1}
	d := dispatch.New(dispatch.Config{Flow: "flow", MaxAttempts: 3, InitialRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, WorkerConcurrency: 1}, j, map[journal.DestinationID]eventflow.Emitter{"out": emitter})
	if err := d.DispatchReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkFailed(ctx, record.ID, "out", errors.New("force ready"), false, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := d.DispatchReady(ctx); err != nil {
		t.Fatal(err)
	}
	iter, err := j.Query(ctx, journal.ReplayFilter{Flow: "flow", Destination: "out", State: journal.StateDelivered})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if _, err := iter.Next(ctx); err != nil {
		t.Fatalf("expected delivered record: %v", err)
	}
	if emitter.calls != 2 {
		t.Fatalf("calls = %d, want 2", emitter.calls)
	}
}

func TestDispatcherRetryExhaustion(t *testing.T) {
	ctx := context.Background()
	j := sqlitejournal.New(sqlitejournal.Config{Path: filepath.Join(t.TempDir(), "journal.sqlite")})
	if err := j.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer j.Close(ctx)
	if _, err := j.Append(ctx, testEvent(t, "event-2"), journal.AppendOptions{Flow: "flow", Destinations: []journal.DestinationID{"out"}}); err != nil {
		t.Fatal(err)
	}
	emitter := &flakyEmitter{failures: 10}
	d := dispatch.New(dispatch.Config{Flow: "flow", MaxAttempts: 1, InitialRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, WorkerConcurrency: 1}, j, map[journal.DestinationID]eventflow.Emitter{"out": emitter})
	if err := d.DispatchReady(ctx); err != nil {
		t.Fatal(err)
	}
	iter, err := j.Query(ctx, journal.ReplayFilter{Flow: "flow", Destination: "out", State: journal.StateFailed})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if _, err := iter.Next(ctx); err != nil {
		t.Fatalf("expected failed record: %v", err)
	}
}

type flakyEmitter struct {
	failures int
	calls    int
}

func (*flakyEmitter) Open(context.Context) error  { return nil }
func (*flakyEmitter) Close(context.Context) error { return nil }
func (e *flakyEmitter) Emit(context.Context, eventflow.Event) error {
	e.calls++
	if e.calls <= e.failures {
		return errors.New("temporary")
	}
	return nil
}

func testEvent(t *testing.T, id string) eventflow.Event {
	t.Helper()
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID(id)
	event.SetType("example.created.v1")
	event.SetSource("urn:test")
	event.SetTime(time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC))
	if err := event.SetData(sdk.ApplicationJSON, map[string]any{"id": id}); err != nil {
		t.Fatal(err)
	}
	return event
}
