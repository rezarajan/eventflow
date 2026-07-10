package schoolfaker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rezarajan/project-datascape/internal/contracts/event"
	"github.com/rezarajan/project-datascape/internal/ports/generator"
)

// TestGeneratorIsDeterministic verifies that the same seed and parameters produce identical facts.
func TestGeneratorIsDeterministic(t *testing.T) {
	first := collect(t, smallConfig(true))
	second := collect(t, smallConfig(true))
	firstJSON := mustJSON(t, first)
	secondJSON := mustJSON(t, second)
	if firstJSON != secondJSON {
		t.Fatalf("facts are not deterministic:\n%s\n%s", firstJSON, secondJSON)
	}
}

// TestGeneratorCounts verifies expected fact counts for a bounded school demo configuration.
func TestGeneratorCounts(t *testing.T) {
	facts := collect(t, generator.Config{RunID: "run-1", Seed: 1, Parameters: map[string]any{"schools": int64(1), "classes_per_school": int64(1), "students_per_class": int64(2), "attendance_days": int64(3), "documents": true}})
	want := 1 + 1 + 2 + 6 + 2 + 2
	if len(facts) != want {
		t.Fatalf("got %d facts, want %d", len(facts), want)
	}
}

// TestGeneratorCanDisableDocuments verifies document events are optional.
func TestGeneratorCanDisableDocuments(t *testing.T) {
	facts := collect(t, smallConfig(false))
	for _, fact := range facts {
		if fact.Kind == "document.uploaded.v1" {
			t.Fatal("did not expect document facts when documents=false")
		}
	}
}

// TestGeneratorDocumentFactsAreTextPlain verifies documents are represented as text metadata only.
func TestGeneratorDocumentFactsAreTextPlain(t *testing.T) {
	facts := collect(t, smallConfig(true))
	for _, fact := range facts {
		if fact.Kind != "document.uploaded.v1" {
			continue
		}
		if fact.Data["media_type"] != "text/plain" {
			t.Fatalf("media_type = %v, want text/plain", fact.Data["media_type"])
		}
		if !strings.HasSuffix(fact.Data["filename"].(string), ".txt") {
			t.Fatalf("filename = %v, want .txt", fact.Data["filename"])
		}
		return
	}
	t.Fatal("expected at least one document fact")
}

// TestGeneratorRejectsInvalidSettings verifies non-positive counts fail fast.
func TestGeneratorRejectsInvalidSettings(t *testing.T) {
	out := make(chan event.Fact, 1)
	err := New().Generate(context.Background(), generator.Config{RunID: "run-1", Seed: 1, Parameters: map[string]any{"schools": int64(0)}}, out)
	if err == nil || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

// collect gathers facts from the generator into memory for testing.
func collect(t *testing.T, config generator.Config) []event.Fact {
	t.Helper()
	out := make(chan event.Fact, 64)
	gen := New()
	if err := gen.Generate(context.Background(), config, out); err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}
	facts := []event.Fact{}
	for fact := range out {
		facts = append(facts, fact)
	}
	return facts
}

// smallConfig returns a small deterministic generator configuration.
func smallConfig(documents bool) generator.Config {
	return generator.Config{RunID: "run-1", Seed: 7, Parameters: map[string]any{"schools": int64(1), "classes_per_school": int64(1), "students_per_class": int64(1), "attendance_days": int64(1), "documents": documents}}
}

// mustJSON marshals a value into deterministic JSON for test comparison.
func mustJSON(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(body)
}
