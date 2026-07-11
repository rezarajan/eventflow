// Package httpflow provides HTTP Eventflow emitters and receivers.
package httpflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/cloudevent"
	"github.com/rezarajan/eventflow/resource"
)

// Mode controls HTTP event representation.
type Mode string

const (
	// ModeStructuredCloudEvents posts structured CloudEvents JSON.
	ModeStructuredCloudEvents Mode = "structured-cloudevents"
	// ModeBinaryCloudEvents posts CloudEvents binary-mode HTTP requests.
	ModeBinaryCloudEvents Mode = "binary-cloudevents"
	// ModeNativeOpenLineage posts raw OpenLineage JSON.
	ModeNativeOpenLineage Mode = "native-openlineage"
)

// EmitterConfig defines HTTP emitter settings.
type EmitterConfig struct {
	URL               string
	Mode              Mode
	Client            *http.Client
	RetryMax          int
	RetryBackoff      time.Duration
	IdempotencyHeader string
}

// HTTPEmitterSpec is the declarative spec for HTTPEmitter.
type HTTPEmitterSpec struct {
	URL               string `yaml:"url" json:"url"`
	Mode              string `yaml:"mode,omitempty" json:"mode,omitempty"`
	RetryMax          int    `yaml:"retryMax,omitempty" json:"retryMax,omitempty"`
	RetryBackoff      string `yaml:"retryBackoff,omitempty" json:"retryBackoff,omitempty"`
	IdempotencyHeader string `yaml:"idempotencyHeader,omitempty" json:"idempotencyHeader,omitempty"`
}

// HTTPReceiverSpec configures the HTTP handler component.
//
// The current HTTPReceiver resource is an http.Handler integration point, not a
// pull-based eventflow.Receiver. It is therefore not a valid EventFlow receiverRef.
type HTTPReceiverSpec struct {
	MaxBody int64 `yaml:"maxBody,omitempty" json:"maxBody,omitempty"`
}

// Register adds HTTPEmitter and HTTPReceiver resource definitions.
func Register(catalog *resource.Catalog) error {
	if err := resource.Register(catalog, resource.Definition[HTTPEmitterSpec]{
		GVK: resource.GVK("HTTPEmitter"),
		Default: func(spec *HTTPEmitterSpec) error {
			if spec.Mode == "" {
				spec.Mode = string(ModeStructuredCloudEvents)
			}
			return nil
		},
		Validate: func(_ context.Context, spec HTTPEmitterSpec) error {
			if spec.URL == "" {
				return fmt.Errorf("url is required")
			}
			if _, err := parseBackoff(spec.RetryBackoff); err != nil {
				return err
			}
			switch Mode(spec.Mode) {
			case ModeStructuredCloudEvents, ModeBinaryCloudEvents, ModeNativeOpenLineage:
				return nil
			default:
				return fmt.Errorf("unsupported mode %q", spec.Mode)
			}
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec HTTPEmitterSpec) (any, error) {
			backoff, err := parseBackoff(spec.RetryBackoff)
			if err != nil {
				return nil, err
			}
			return NewEmitter(EmitterConfig{URL: spec.URL, Mode: Mode(spec.Mode), RetryMax: spec.RetryMax, RetryBackoff: backoff, IdempotencyHeader: spec.IdempotencyHeader}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityEmitter},
	}); err != nil {
		return err
	}
	return resource.Register(catalog, resource.Definition[HTTPReceiverSpec]{
		GVK: resource.GVK("HTTPReceiver"),
		Build: func(_ context.Context, _ resource.BuildContext, spec HTTPReceiverSpec) (any, error) {
			return NewReceiver(ReceiverConfig{MaxBody: spec.MaxBody}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent},
	})
}

func parseBackoff(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("retryBackoff: %w", err)
	}
	return duration, nil
}

// Emitter posts events to HTTP endpoints.
type Emitter struct {
	config EmitterConfig
	client *http.Client
}

// NewEmitter constructs an HTTP emitter.
func NewEmitter(config EmitterConfig) *Emitter { return &Emitter{config: config} }

// Name returns the adapter name.
func (*Emitter) Name() string { return "http" }

// Open validates configuration.
func (e *Emitter) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.config.URL == "" {
		return fmt.Errorf("http emitter URL is required")
	}
	e.client = e.config.Client
	if e.client == nil {
		e.client = &http.Client{Timeout: 30 * time.Second}
	}
	return nil
}

// Emit posts one event.
func (e *Emitter) Emit(ctx context.Context, event eventflow.Event) error {
	if err := event.Validate(); err != nil {
		return eventflow.ValidationError("validate cloudevent", err)
	}
	body, contentType, err := e.body(event)
	if err != nil {
		return err
	}
	attempts := e.config.RetryMax + 1
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.URL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("content-type", contentType)
		if e.config.Mode == ModeBinaryCloudEvents {
			cloudevent.AddBinaryHTTPHeaders(req.Header, event)
		}
		if e.config.IdempotencyHeader != "" {
			req.Header.Set(e.config.IdempotencyHeader, event.ID())
		}
		resp, err := e.client.Do(req)
		if err == nil && resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			err = fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
			if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
				return err
			}
		}
		lastErr = err
		if attempt+1 < attempts && e.config.RetryBackoff > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(e.config.RetryBackoff):
			}
		}
	}
	return lastErr
}

// Close releases resources.
func (*Emitter) Close(ctx context.Context) error { return ctx.Err() }

func (e *Emitter) body(event eventflow.Event) ([]byte, string, error) {
	switch e.config.Mode {
	case "", ModeStructuredCloudEvents, ModeBinaryCloudEvents:
		body, err := json.Marshal(event)
		contentType := "application/cloudevents+json"
		if e.config.Mode == ModeBinaryCloudEvents {
			body = event.Data()
			contentType = event.DataContentType()
			if contentType == "" {
				contentType = "application/json"
			}
		}
		return body, contentType, err
	case ModeNativeOpenLineage:
		return event.Data(), "application/json", nil
	default:
		return nil, "", eventflow.UnsupportedError("http emitter mode", fmt.Errorf("%s", e.config.Mode))
	}
}

// ReceiverConfig defines an HTTP receiver.
type ReceiverConfig struct {
	Handler   eventflow.EventHandler
	Validator eventflow.Validator
	Mode      eventflow.ValidationMode
	MaxBody   int64
}

// Receiver is an http.Handler that validates and dispatches received events.
type Receiver struct {
	config ReceiverConfig
}

// NewReceiver constructs an HTTP receiver handler.
func NewReceiver(config ReceiverConfig) *Receiver { return &Receiver{config: config} }

// ServeHTTP accepts structured CloudEvents, binary CloudEvents, and raw OpenLineage JSON.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/healthz" || req.URL.Path == "/readyz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	event, err := r.decode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.config.Validator != nil {
		mode := r.config.Mode
		if mode == "" {
			mode = eventflow.ValidationStrict
		}
		if err := r.config.Validator.Validate(req.Context(), event, mode); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if r.config.Handler == nil {
		http.Error(w, "event handler is required", http.StatusInternalServerError)
		return
	}
	if err := r.config.Handler.Handle(req.Context(), event); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (r *Receiver) decode(req *http.Request) (eventflow.Event, error) {
	maxBody := r.config.MaxBody
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	req.Body = http.MaxBytesReader(nil, req.Body, maxBody)
	contentType := req.Header.Get("content-type")
	if req.Header.Get("ce-type") != "" {
		return cloudevent.FromBinaryHTTPRequest(req)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return eventflow.Event{}, err
	}
	if contentType == "application/cloudevents+json" {
		var event eventflow.Event
		if err := json.Unmarshal(body, &event); err != nil {
			return eventflow.Event{}, err
		}
		if err := event.Validate(); err != nil {
			return eventflow.Event{}, eventflow.ValidationError("validate cloudevent", err)
		}
		return event, nil
	}
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetType("io.openlineage.run-event.v1")
	event.SetSource("urn:eventflow:http")
	event.SetDataContentType("application/json")
	if err := event.SetData("application/json", body); err != nil {
		return eventflow.Event{}, err
	}
	if err := event.Validate(); err != nil {
		return eventflow.Event{}, err
	}
	return event, nil
}
