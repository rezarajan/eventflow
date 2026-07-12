package sqlite

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"
	"github.com/rezarajan/eventflow/journal"
)

func TestAppendAndQueryRecord(t *testing.T) {
	ctx := context.Background()
	j := New(Config{Path: filepath.Join(t.TempDir(), "journal.sqlite")})
	if err := j.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer j.Close(ctx)
	record, err := j.Append(ctx, testEvent(t, "event-1"), journal.AppendOptions{
		Flow:         "lineage",
		Destinations: []journal.DestinationID{"HTTPEmitter/marquez"},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if record.ID == 0 || record.EventID != "event-1" {
		t.Fatalf("record = %+v", record)
	}
	pending, err := j.Pending(ctx, journal.PendingFilter{Flow: "lineage", Now: time.Now(), Limit: 10})
	if err != nil {
		t.Fatalf("Pending() error = %v", err)
	}
	if len(pending) != 1 || pending[0].Destination != "HTTPEmitter/marquez" || pending[0].State != journal.StatePending {
		t.Fatalf("pending = %+v", pending)
	}
	iter, err := j.Query(ctx, journal.ReplayFilter{Flow: "lineage", Destination: "HTTPEmitter/marquez", State: journal.StatePending})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer iter.Close()
	got, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if got.Event.ID() != "event-1" {
		t.Fatalf("event id = %s", got.Event.ID())
	}
	_, err = iter.Next(ctx)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Next() error = %v, want EOF", err)
	}
}

func TestDeliveryStateTransitions(t *testing.T) {
	ctx := context.Background()
	j := New(Config{Path: filepath.Join(t.TempDir(), "journal.sqlite")})
	if err := j.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer j.Close(ctx)
	record, err := j.Append(ctx, testEvent(t, "event-2"), journal.AppendOptions{Flow: "flow", Destinations: []journal.DestinationID{"FilesystemEmitter/out"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkAttempt(ctx, record.ID, "FilesystemEmitter/out", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkFailed(ctx, record.ID, "FilesystemEmitter/out", errors.New("temporary"), false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	pending, err := j.Pending(ctx, journal.PendingFilter{Flow: "flow", Now: time.Now(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].AttemptCount != 1 || pending[0].LastError == "" {
		t.Fatalf("pending after failure = %+v", pending)
	}
	if err := j.MarkDelivered(ctx, record.ID, "FilesystemEmitter/out"); err != nil {
		t.Fatal(err)
	}
	iter, err := j.Query(ctx, journal.ReplayFilter{Flow: "flow", Destination: "FilesystemEmitter/out", State: journal.StateDelivered})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if _, err := iter.Next(ctx); err != nil {
		t.Fatalf("delivered query Next() error = %v", err)
	}
}

func testEvent(t *testing.T, id string) sdk.Event {
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
