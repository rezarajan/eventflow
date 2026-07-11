// Package lineage defines local OpenLineage-compatible run event emission.
package lineage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Dataset identifies a stable input or output dataset boundary.
type Dataset struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// Run identifies one executable run.
type Run struct {
	RunID  string         `json:"runId"`
	Facets map[string]any `json:"facets,omitempty"`
}

// Job identifies one executable job.
type Job struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// ParentRunFacet links a child run to the run that orchestrated it.
type ParentRunFacet struct {
	Producer  string `json:"_producer"`
	SchemaURL string `json:"_schemaURL"`
	Run       Run    `json:"run"`
	Job       Job    `json:"job"`
}

// Event is the local OpenLineage-compatible run event shape.
type Event struct {
	EventType string    `json:"eventType"`
	EventTime time.Time `json:"eventTime"`
	Run       Run       `json:"run"`
	Job       Job       `json:"job"`
	Inputs    []Dataset `json:"inputs,omitempty"`
	Outputs   []Dataset `json:"outputs,omitempty"`
	Producer  string    `json:"producer"`
	SchemaURL string    `json:"schemaURL"`
	Error     string    `json:"error,omitempty"`
}

// Emitter emits lineage events.
type Emitter interface {
	Emit(ctx context.Context, event Event) error
}

// EventReader reads persisted lineage events.
type EventReader interface {
	Read(ctx context.Context) (Event, error)
	Close() error
}

// NoopEmitter drops all lineage events.
type NoopEmitter struct{}

// Emit drops one lineage event.
func (NoopEmitter) Emit(ctx context.Context, event Event) error {
	return ctx.Err()
}

// FileEmitter writes lineage events as newline-delimited JSON.
type FileEmitter struct {
	Path string
}

// Emit appends one lineage event to an NDJSON file.
func (e FileEmitter) Emit(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if e.Path == "" {
		return fmt.Errorf("lineage file path is required")
	}
	if err := os.MkdirAll(filepath.Dir(e.Path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(e.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := file.Write(body); err != nil {
		return err
	}
	_, err = file.Write([]byte("\n"))
	return err
}

// Config defines lineage emission settings.
type Config struct {
	Output     string
	File       string
	Namespace  string
	Producer   string
	SchemaURL  string
	MarquezURL string
	Timeout    time.Duration
}

// FromEnv builds lineage configuration from environment variables.
func FromEnv() Config {
	return Config{
		Output:     envString("EVENTFLOW_LINEAGE_OUTPUT", envString("DATASCAPE_LINEAGE_OUTPUT", "noop")),
		File:       envString("EVENTFLOW_LINEAGE_FILE", envString("DATASCAPE_LINEAGE_FILE", "var/eventflow/lineage/openlineage.ndjson")),
		Namespace:  envString("EVENTFLOW_LINEAGE_NAMESPACE", envString("DATASCAPE_LINEAGE_NAMESPACE", "eventflow")),
		Producer:   envString("EVENTFLOW_LINEAGE_PRODUCER", envString("DATASCAPE_LINEAGE_PRODUCER", DefaultProducer)),
		SchemaURL:  envString("EVENTFLOW_LINEAGE_SCHEMA_URL", envString("DATASCAPE_LINEAGE_SCHEMA_URL", DefaultSchemaURL)),
		MarquezURL: envString("EVENTFLOW_MARQUEZ_URL", envString("DATASCAPE_MARQUEZ_URL", "http://localhost:5000")),
		Timeout:    envDuration("EVENTFLOW_MARQUEZ_TIMEOUT", envDuration("DATASCAPE_MARQUEZ_TIMEOUT", 10*time.Second)),
	}
}

// NewEmitter constructs a lineage emitter from configuration.
func NewEmitter(config Config) (Emitter, error) {
	switch strings.ToLower(strings.TrimSpace(config.Output)) {
	case "", "noop":
		return NoopEmitter{}, nil
	case "file":
		file := config.File
		if file == "" {
			file = "var/eventflow/lineage/openlineage.ndjson"
		}
		return FileEmitter{Path: file}, nil
	default:
		return nil, fmt.Errorf("unsupported lineage output %q", config.Output)
	}
}

// NewEvent constructs a lineage run event.
func NewEvent(eventType string, namespace string, jobName string, runID string, inputs []Dataset, outputs []Dataset, err error, now func() time.Time) Event {
	if now == nil {
		now = time.Now
	}
	event := Event{
		EventType: eventType,
		EventTime: now().UTC(),
		Run:       Run{RunID: runID},
		Job:       Job{Namespace: namespace, Name: jobName},
		Inputs:    append([]Dataset(nil), inputs...),
		Outputs:   append([]Dataset(nil), outputs...),
		Producer:  DefaultProducer,
		SchemaURL: DefaultSchemaURL,
	}
	if err != nil {
		event.Error = err.Error()
	}
	return event
}

// WithProducer returns a copy of the event with configured producer metadata.
func WithProducer(event Event, producer string, schemaURL string) Event {
	if producer == "" {
		producer = DefaultProducer
	}
	if schemaURL == "" {
		schemaURL = DefaultSchemaURL
	}
	event.Producer = producer
	event.SchemaURL = schemaURL
	if event.Run.Facets != nil {
		if parent, ok := event.Run.Facets["parent"].(ParentRunFacet); ok {
			parent.Producer = producer
			event.Run.Facets["parent"] = parent
		}
	}
	return event
}

// WithParent returns a copy of the event linked to a parent OpenLineage run.
func WithParent(event Event, parentJob Job, parentRunID string) Event {
	if parentRunID == "" || parentJob.Name == "" {
		return event
	}
	if event.Run.Facets == nil {
		event.Run.Facets = map[string]any{}
	}
	event.Run.Facets["parent"] = ParentRunFacet{
		Producer:  DefaultProducer,
		SchemaURL: "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
		Run:       Run{RunID: parentRunID},
		Job:       parentJob,
	}
	return event
}

// EmitLifecycle emits START and then COMPLETE or FAIL around a completed operation.
func EmitLifecycle(ctx context.Context, emitter Emitter, namespace string, jobName string, runID string, inputs []Dataset, outputs []Dataset, runErr error, now func() time.Time) error {
	if emitter == nil {
		emitter = NoopEmitter{}
	}
	if err := emitter.Emit(ctx, NewEvent("START", namespace, jobName, runID, inputs, outputs, nil, now)); err != nil {
		return err
	}
	eventType := "COMPLETE"
	if runErr != nil {
		eventType = "FAIL"
	}
	return emitter.Emit(ctx, NewEvent(eventType, namespace, jobName, runID, inputs, outputs, runErr, now))
}

// RedpandaDataset returns a stable dataset identifier for a Redpanda topic.
func RedpandaDataset(brokers []string, topic string) Dataset {
	broker := "localhost:19092"
	if len(brokers) > 0 && brokers[0] != "" {
		broker = brokers[0]
	}
	return Dataset{Namespace: "redpanda://" + broker, Name: topic}
}

// FileDataset returns a stable dataset identifier for a local file boundary.
func FileDataset(dir string, name string) Dataset {
	return Dataset{Namespace: "file://" + filepath.ToSlash(filepath.Clean(dir)), Name: name}
}

// DuckDBDataset returns a stable dataset identifier for a local DuckDB table.
func DuckDBDataset(path string, table string) Dataset {
	if path == "" {
		path = "var/eventflow/eventflow.duckdb"
	}
	if path != ":memory:" && !filepath.IsAbs(path) {
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
	}
	return Dataset{Namespace: "duckdb://" + filepath.ToSlash(filepath.Clean(path)), Name: table}
}

// NDJSONReader reads lineage events from newline-delimited JSON.
type NDJSONReader struct {
	scanner *bufio.Scanner
	closer  io.Closer
}

// NewNDJSONReader constructs a lineage reader from an io.Reader.
func NewNDJSONReader(reader io.Reader) *NDJSONReader {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	return &NDJSONReader{scanner: scanner}
}

// NewNDJSONFileReader opens an NDJSON lineage file for replay.
func NewNDJSONFileReader(path string) (*NDJSONReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader := NewNDJSONReader(file)
	reader.closer = file
	return reader, nil
}

// Read returns the next persisted lineage event.
func (r *NDJSONReader) Read(ctx context.Context) (Event, error) {
	select {
	case <-ctx.Done():
		return Event{}, ctx.Err()
	default:
	}
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return Event{}, err
		}
		return Event{}, io.EOF
	}
	var event Event
	if err := json.Unmarshal(r.scanner.Bytes(), &event); err != nil {
		return Event{}, fmt.Errorf("decode lineage event: %w", err)
	}
	return event, nil
}

// Close releases the underlying file when the reader owns one.
func (r *NDJSONReader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

// DefaultProducer identifies this project as the producer of lineage metadata.
const DefaultProducer = "github.com/rezarajan/eventflow"

// DefaultSchemaURL identifies the OpenLineage schema version used for emitted events.
const DefaultSchemaURL = "https://openlineage.io/spec/1-0-5/OpenLineage.json"

// envString returns an environment variable value or a fallback.
func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// envDuration returns a duration environment variable value or a fallback.
func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
