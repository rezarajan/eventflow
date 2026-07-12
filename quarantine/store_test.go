package quarantine

import (
	"context"
	"testing"
	"time"

	eventflow "github.com/rezarajan/eventflow"
)

func TestStoreRecordLifecycle(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir() + "/quarantine.sqlite")
	if err := store.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)
	record, err := store.Put(ctx, Record{
		Raw:              []byte(`{"eventType":"COMPLETE"}`),
		CloudEventsID:    "event-1",
		OpenLineageRunID: "run-1",
		Principal:        "principal",
		PolicyName:       "policy",
		PolicyVersion:    "v1",
		ContractName:     "contract",
		ContractVersion:  "v1",
		Decision:         eventflow.Rejected(eventflow.ReasonJobNamespaceNotAllowed, "no", "job.namespace", "principal", "policy", "v1"),
		Field:            "job.namespace",
		ReceiveTime:      time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC),
		Target:           "flow",
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.ID == 0 || record.Digest == "" || record.Status != StatusOpen {
		t.Fatalf("record = %+v", record)
	}
	got, err := store.Get(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Decision.ReasonCode != eventflow.ReasonJobNamespaceNotAllowed {
		t.Fatalf("reason = %s", got.Decision.ReasonCode)
	}
	records, err := store.List(ctx, StatusOpen, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	if err := store.MarkReplayed(ctx, record.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusReplayed || got.ReplayCount != 1 {
		t.Fatalf("record after replay = %+v", got)
	}
	if err := store.Dismiss(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusDismissed {
		t.Fatalf("status = %s", got.Status)
	}
}
