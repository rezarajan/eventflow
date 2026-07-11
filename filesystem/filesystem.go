// Package filesystem provides file-based Eventflow emitters and receivers.
package filesystem

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/resource"
)

// Mode controls how events are stored.
type Mode string

const (
	// ModeNDJSON writes newline-delimited structured CloudEvents.
	ModeNDJSON Mode = "ndjson"
	// ModeFiles writes one structured CloudEvent per file.
	ModeFiles Mode = "files"
)

// Config defines filesystem adapter settings.
type Config struct {
	Path         string
	Mode         Mode
	Atomic       bool
	CommitMarker string
	Stdin        io.Reader
	Stdout       io.Writer
	Deduplicate  bool
}

type ResourceSpec struct {
	Path         string `yaml:"path" json:"path"`
	Format       string `yaml:"format,omitempty" json:"format,omitempty"`
	Atomic       bool   `yaml:"atomic,omitempty" json:"atomic,omitempty"`
	CommitMarker string `yaml:"commitMarker,omitempty" json:"commitMarker,omitempty"`
	Deduplicate  bool   `yaml:"deduplicate,omitempty" json:"deduplicate,omitempty"`
}

func Register(catalog *resource.Catalog) error {
	if err := resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK: resource.GVK("FilesystemEmitter"),
		Default: func(spec *ResourceSpec) error {
			if spec.Format == "" {
				spec.Format = string(ModeNDJSON)
			}
			return nil
		},
		Validate: validateResourceSpec,
		Build: func(_ context.Context, _ resource.BuildContext, spec ResourceSpec) (any, error) {
			return NewEmitter(Config{Path: spec.Path, Mode: Mode(spec.Format), Atomic: spec.Atomic, CommitMarker: spec.CommitMarker, Deduplicate: spec.Deduplicate}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityEmitter, resource.CapabilityBatchEmission},
	}); err != nil {
		return err
	}
	return resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK: resource.GVK("FilesystemReceiver"),
		Default: func(spec *ResourceSpec) error {
			if spec.Format == "" {
				spec.Format = string(ModeNDJSON)
			}
			return nil
		},
		Validate: validateResourceSpec,
		Build: func(_ context.Context, _ resource.BuildContext, spec ResourceSpec) (any, error) {
			return NewReceiver(Config{Path: spec.Path, Mode: Mode(spec.Format), Deduplicate: spec.Deduplicate}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityReceiver},
	})
}

func validateResourceSpec(_ context.Context, spec ResourceSpec) error {
	switch Mode(spec.Format) {
	case "", ModeNDJSON, ModeFiles:
		return nil
	default:
		return fmt.Errorf("unsupported format %q", spec.Format)
	}
}

// Emitter writes events to a file, directory, stdout, or injected writer.
type Emitter struct {
	config Config
	file   *os.File
	writer io.Writer
	mu     sync.Mutex
	seen   map[string]bool
}

// NewEmitter constructs a filesystem emitter.
func NewEmitter(config Config) *Emitter {
	return &Emitter{config: config, seen: map[string]bool{}}
}

// Name returns the adapter name.
func (*Emitter) Name() string { return "filesystem" }

// Open prepares the target path.
func (e *Emitter) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.config.Stdout != nil {
		e.writer = e.config.Stdout
		return nil
	}
	if e.mode() == ModeFiles {
		return os.MkdirAll(e.config.Path, 0o755)
	}
	if e.config.Path == "" || e.config.Path == "-" {
		e.writer = os.Stdout
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(e.config.Path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(e.config.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	e.file = file
	e.writer = file
	return nil
}

// Emit writes one event.
func (e *Emitter) Emit(ctx context.Context, event eventflow.Event) error {
	return e.EmitBatch(ctx, []eventflow.Event{event})
}

// EmitBatch writes a batch of events.
func (e *Emitter) EmitBatch(ctx context.Context, events []eventflow.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return eventflow.ValidationError("validate cloudevent", err)
		}
		if e.config.Deduplicate && e.seen[event.ID()] {
			continue
		}
		if e.mode() == ModeFiles {
			if err := e.writeOneFile(event); err != nil {
				return err
			}
		} else {
			if e.writer == nil {
				return fmt.Errorf("filesystem emitter is not open")
			}
			if err := json.NewEncoder(e.writer).Encode(event); err != nil {
				return err
			}
		}
		e.seen[event.ID()] = true
	}
	return nil
}

// Close releases resources and optionally writes a commit marker.
func (e *Emitter) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.config.CommitMarker != "" && e.config.Path != "" && e.config.Path != "-" {
		marker := e.config.CommitMarker
		if !filepath.IsAbs(marker) {
			base := e.config.Path
			if e.mode() != ModeFiles {
				base = filepath.Dir(base)
			}
			marker = filepath.Join(base, marker)
		}
		if err := os.WriteFile(marker, []byte("committed\n"), 0o644); err != nil {
			return err
		}
	}
	if e.file != nil {
		err := e.file.Close()
		e.file = nil
		return err
	}
	return nil
}

func (e *Emitter) writeOneFile(event eventflow.Event) error {
	if e.config.Path == "" {
		return fmt.Errorf("filesystem directory path is required")
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	name := safeName(event.ID()) + ".json"
	path := filepath.Join(e.config.Path, name)
	if e.config.Atomic {
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, append(body, '\n'), 0o644); err != nil {
			return err
		}
		return os.Rename(tmp, path)
	}
	return os.WriteFile(path, append(body, '\n'), 0o644)
}

func (e *Emitter) mode() Mode {
	if e.config.Mode == "" {
		return ModeNDJSON
	}
	return e.config.Mode
}

// Receiver reads events from stdin, NDJSON files, or one-event-per-file directories.
type Receiver struct {
	config  Config
	file    *os.File
	reader  io.Reader
	scanner *bufio.Scanner
	paths   []string
	next    int
	seen    map[string]bool
}

// NewReceiver constructs a filesystem receiver.
func NewReceiver(config Config) *Receiver {
	return &Receiver{config: config, seen: map[string]bool{}}
}

// Name returns the adapter name.
func (*Receiver) Name() string { return "filesystem" }

// Open prepares the source.
func (r *Receiver) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.config.Stdin != nil {
		r.reader = r.config.Stdin
		return r.openScanner()
	}
	if r.config.Path == "" || r.config.Path == "-" {
		r.reader = os.Stdin
		return r.openScanner()
	}
	info, err := os.Stat(r.config.Path)
	if err != nil {
		return err
	}
	if info.IsDir() || r.config.Mode == ModeFiles {
		paths, err := filepath.Glob(filepath.Join(r.config.Path, "*.json"))
		if err != nil {
			return err
		}
		sort.Strings(paths)
		r.paths = paths
		return nil
	}
	file, err := os.Open(r.config.Path)
	if err != nil {
		return err
	}
	r.file = file
	r.reader = file
	return r.openScanner()
}

// Receive reads one event.
func (r *Receiver) Receive(ctx context.Context) (eventflow.Event, error) {
	if err := ctx.Err(); err != nil {
		return eventflow.Event{}, err
	}
	if len(r.paths) > 0 {
		return r.receiveFile(ctx)
	}
	if r.scanner == nil {
		return eventflow.Event{}, fmt.Errorf("filesystem receiver is not open")
	}
	for r.scanner.Scan() {
		event, err := decodeEvent(r.scanner.Bytes())
		if err != nil {
			return eventflow.Event{}, err
		}
		if r.config.Deduplicate && r.seen[event.ID()] {
			continue
		}
		r.seen[event.ID()] = true
		return event, nil
	}
	if err := r.scanner.Err(); err != nil {
		return eventflow.Event{}, err
	}
	return eventflow.Event{}, io.EOF
}

// ReceiveBatch reads up to maxEvents events.
func (r *Receiver) ReceiveBatch(ctx context.Context, maxEvents int) ([]eventflow.Event, error) {
	if maxEvents <= 0 {
		maxEvents = 100
	}
	events := make([]eventflow.Event, 0, maxEvents)
	for len(events) < maxEvents {
		event, err := r.Receive(ctx)
		if err != nil {
			if err == io.EOF && len(events) > 0 {
				return events, nil
			}
			return events, err
		}
		events = append(events, event)
	}
	return events, nil
}

// Close closes the open source file.
func (r *Receiver) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}

func (r *Receiver) openScanner() error {
	r.scanner = bufio.NewScanner(r.reader)
	r.scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	return nil
}

func (r *Receiver) receiveFile(ctx context.Context) (eventflow.Event, error) {
	for r.next < len(r.paths) {
		if err := ctx.Err(); err != nil {
			return eventflow.Event{}, err
		}
		path := r.paths[r.next]
		r.next++
		if strings.HasSuffix(path, ".tmp") {
			continue
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return eventflow.Event{}, err
		}
		event, err := decodeEvent(body)
		if err != nil {
			return eventflow.Event{}, err
		}
		if r.config.Deduplicate && r.seen[event.ID()] {
			continue
		}
		r.seen[event.ID()] = true
		return event, nil
	}
	return eventflow.Event{}, io.EOF
}

func decodeEvent(body []byte) (eventflow.Event, error) {
	var event eventflow.Event
	if err := json.Unmarshal(body, &event); err != nil {
		return eventflow.Event{}, fmt.Errorf("decode CloudEvent: %w", err)
	}
	if err := event.Validate(); err != nil {
		return eventflow.Event{}, eventflow.ValidationError("validate cloudevent", err)
	}
	return event, nil
}

func safeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "event"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(value)
}
