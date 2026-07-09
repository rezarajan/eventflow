package ndjson

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestNewFileReaderReadsEvents verifies NDJSON files can be replayed as lineage events.
func TestNewFileReaderReadsEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "openlineage.ndjson")
	body := `{"eventType":"START","eventTime":"2026-07-09T12:00:00Z","run":{"runId":"run-1"},"job":{"namespace":"datascape","name":"job"},"producer":"producer","schemaURL":"schema"}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write lineage file: %v", err)
	}
	reader, err := NewFileReader(path)
	if err != nil {
		t.Fatalf("NewFileReader returned error: %v", err)
	}
	defer reader.Close()
	event, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if event.EventType != "START" || event.Run.RunID != "run-1" {
		t.Fatalf("unexpected event: %+v", event)
	}
	if _, err := reader.Read(context.Background()); err != io.EOF {
		t.Fatalf("second Read error = %v, want EOF", err)
	}
}
