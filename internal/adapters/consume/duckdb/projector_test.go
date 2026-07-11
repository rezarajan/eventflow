package duckdb

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/rezarajan/eventflow/internal/contracts/registry"
)

func TestProjectorWritesRawAndTypedRowsIdempotently(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	projector := NewWithDB(Config{Path: ":memory:"}, db)
	projector.registry = duckdbTestRegistry(t)
	if err := projector.Open(ctx); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	evt := duckdbTestEvent(t, "evt-1", "example.created.v1")
	if err := projector.HandleBatch(ctx, []cloudevents.Event{evt, evt}); err != nil {
		t.Fatalf("HandleBatch returned error: %v", err)
	}
	assertCount(t, db, "_raw_events", 1)
	assertCount(t, db, "attendance", 1)
	if err := projector.Close(ctx); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestProjectorStoresUnknownEventsOnlyInRawTable(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	projector := NewWithDB(Config{Path: ":memory:"}, db)
	if err := projector.Open(ctx); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := projector.Handle(ctx, duckdbTestEvent(t, "evt-2", "unknown.created.v1")); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	assertCount(t, db, "_raw_events", 1)
	if err := projector.Close(ctx); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestOutputDatasetsIncludesRawAndProjectedDuckDBTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "eventflow.duckdb")
	projector := New(Config{Path: path})
	projector.registry = duckdbTestRegistry(t)

	datasets := projector.OutputDatasets()
	if len(datasets) != 2 {
		t.Fatalf("unexpected datasets: %+v", datasets)
	}
	if datasets[0].Namespace != "duckdb://"+filepath.ToSlash(path) || datasets[0].Name != "_raw_events" {
		t.Fatalf("unexpected raw dataset: %+v", datasets[0])
	}
	if datasets[1].Name != "attendance" {
		t.Fatalf("unexpected projected dataset: %+v", datasets[1])
	}
}

func duckdbTestRegistry(t *testing.T) registry.Registry {
	t.Helper()
	registered, err := registry.New([]registry.Event{{
		Type:    "example.created.v1",
		Schema:  "attendance-submitted.v1.schema.json",
		Channel: "example.events.v1",
		Projection: registry.Projection{
			Table: "attendance",
		},
	}})
	if err != nil {
		t.Fatalf("registry.New returned error: %v", err)
	}
	return registered
}

func duckdbTestEvent(t *testing.T, id string, eventType string) cloudevents.Event {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetType(eventType)
	evt.SetSource("urn:test")
	evt.SetTime(time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]any{"id": id}); err != nil {
		t.Fatalf("SetData returned error: %v", err)
	}
	return evt
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT count(*) FROM ` + quoteIdent(table)).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}
