// Package event defines domain-neutral facts and summaries used by generation and fan-out.
package event

import "time"

// Fact is a domain-neutral generated fact before CloudEvents metadata is applied.
type Fact struct {
	Kind    string         `json:"kind"`
	Subject string         `json:"subject,omitempty"`
	Data    map[string]any `json:"data,omitempty"`
}

// Summary describes the result of a generation, fan-out, or consumption run.
type Summary struct {
	RunID       string         `json:"run_id"`
	Generator   string         `json:"generator,omitempty"`
	Events      int            `json:"events"`
	Facts       int            `json:"facts,omitempty"`
	ByType      map[string]int `json:"by_type"`
	OutputNames []string       `json:"output_names,omitempty"`
	StartedAt   time.Time      `json:"started_at"`
	CompletedAt time.Time      `json:"completed_at"`
	DurationMS  int64          `json:"duration_ms"`
}

// Metadata carries platform-owned CloudEvents attributes for one domain event.
type Metadata struct {
	ID            string
	Source        string
	Type          string
	Subject       string
	RunID         string
	CorrelationID string
	CausationID   string
	Tenant        string
	Time          time.Time
}
