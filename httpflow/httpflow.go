// Package httpflow implements OpenLineage admission and quarantine gateway HTTP transport.
package httpflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/admission"
	"github.com/rezarajan/eventflow/resource"
)

type HTTPReceiverSpec struct {
	Address           string   `yaml:"address,omitempty" json:"address,omitempty"`
	Path              string   `yaml:"path,omitempty" json:"path,omitempty"`
	MaxBody           int64    `yaml:"maxBody,omitempty" json:"maxBody,omitempty"`
	PrincipalHeader   string   `yaml:"principalHeader,omitempty" json:"principalHeader,omitempty"`
	TrustedProxyCIDRs []string `yaml:"trustedProxyCidrs,omitempty" json:"trustedProxyCidrs,omitempty"`
	ReadTimeout       string   `yaml:"readTimeout,omitempty" json:"readTimeout,omitempty"`
	WriteTimeout      string   `yaml:"writeTimeout,omitempty" json:"writeTimeout,omitempty"`
	IdleTimeout       string   `yaml:"idleTimeout,omitempty" json:"idleTimeout,omitempty"`
}

type HTTPEmitterSpec struct {
	URL               string `yaml:"url" json:"url"`
	Timeout           string `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	IdempotencyHeader string `yaml:"idempotencyHeader,omitempty" json:"idempotencyHeader,omitempty"`
}

func Register(catalog *resource.Catalog) error {
	if err := resource.Register(catalog, resource.Definition[HTTPReceiverSpec]{
		GVK: resource.GVK("HTTPReceiver"),
		Default: func(spec *HTTPReceiverSpec) error {
			if spec.Address == "" {
				spec.Address = ":8080"
			}
			if spec.Path == "" {
				spec.Path = "/events"
			}
			return nil
		},
		Validate: validateReceiverSpec,
		Build: func(_ context.Context, _ resource.BuildContext, spec HTTPReceiverSpec) (any, error) {
			readTimeout, _ := time.ParseDuration(spec.ReadTimeout)
			writeTimeout, _ := time.ParseDuration(spec.WriteTimeout)
			idleTimeout, _ := time.ParseDuration(spec.IdleTimeout)
			return NewReceiver(ReceiverConfig{Address: spec.Address, Path: spec.Path, MaxBody: spec.MaxBody, PrincipalHeader: spec.PrincipalHeader, TrustedProxyCIDRs: spec.TrustedProxyCIDRs, ReadTimeout: readTimeout, WriteTimeout: writeTimeout, IdleTimeout: idleTimeout}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityHTTPReceiver},
	}); err != nil {
		return err
	}
	return resource.Register(catalog, resource.Definition[HTTPEmitterSpec]{
		GVK: resource.GVK("HTTPEmitter"),
		Validate: func(_ context.Context, spec HTTPEmitterSpec) error {
			if spec.URL == "" {
				return fmt.Errorf("url is required")
			}
			if spec.Timeout != "" {
				if _, err := time.ParseDuration(spec.Timeout); err != nil {
					return fmt.Errorf("timeout: %w", err)
				}
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec HTTPEmitterSpec) (any, error) {
			timeout := 30 * time.Second
			if spec.Timeout != "" {
				timeout, _ = time.ParseDuration(spec.Timeout)
			}
			return &Emitter{url: spec.URL, client: &http.Client{Timeout: timeout}, idempotencyHeader: spec.IdempotencyHeader}, nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityHTTPEmitter},
	})
}

func validateReceiverSpec(_ context.Context, spec HTTPReceiverSpec) error {
	for field, value := range map[string]string{"readTimeout": spec.ReadTimeout, "writeTimeout": spec.WriteTimeout, "idleTimeout": spec.IdleTimeout} {
		if value == "" {
			continue
		}
		if _, err := time.ParseDuration(value); err != nil {
			return fmt.Errorf("%s: %w", field, err)
		}
	}
	for _, cidr := range spec.TrustedProxyCIDRs {
		if cidr == "*" {
			continue
		}
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("trustedProxyCidrs: %w", err)
		}
	}
	return nil
}

type ReceiverConfig struct {
	Address           string
	Path              string
	MaxBody           int64
	PrincipalHeader   string
	TrustedProxyCIDRs []string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

type Receiver struct {
	config ReceiverConfig
	server *http.Server
	events chan httpReceived
}

type httpReceived struct {
	event     eventflow.Event
	raw       []byte
	principal string
	done      chan error
}

func NewReceiver(config ReceiverConfig) *Receiver { return &Receiver{config: config} }

func (r *Receiver) Open(ctx context.Context) error {
	if r.config.Address == "" {
		r.config.Address = ":8080"
	}
	if r.config.Path == "" {
		r.config.Path = "/events"
	}
	r.events = make(chan httpReceived)
	mux := http.NewServeMux()
	mux.Handle(r.config.Path, r)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	r.server = &http.Server{Addr: r.config.Address, Handler: mux, ReadTimeout: r.config.ReadTimeout, WriteTimeout: r.config.WriteTimeout, IdleTimeout: r.config.IdleTimeout}
	go func() { _ = r.server.ListenAndServe() }()
	return ctx.Err()
}

func (r *Receiver) Receive(ctx context.Context) (eventflow.ReceivedEvent, error) {
	select {
	case <-ctx.Done():
		return eventflow.ReceivedEvent{}, ctx.Err()
	case received := <-r.events:
		return eventflow.ReceivedEvent{
			Event:     received.event,
			Raw:       received.raw,
			Principal: received.principal,
			Ack:       func(context.Context) error { received.done <- nil; return nil },
			Nack:      func(context.Context) error { received.done <- fmt.Errorf("event rejected"); return nil },
		}, nil
	}
}

func (r *Receiver) Close(ctx context.Context) error {
	if r.server == nil {
		return nil
	}
	return r.server.Shutdown(ctx)
}

func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != r.config.Path {
		http.NotFound(w, req)
		return
	}
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	maxBody := r.config.MaxBody
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	event, err := eventFromRequest(req, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	done := make(chan error, 1)
	select {
	case <-req.Context().Done():
		http.Error(w, req.Context().Err().Error(), http.StatusServiceUnavailable)
	case r.events <- httpReceived{event: event, raw: body, principal: r.principal(req), done: done}:
		err := <-done
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func eventFromRequest(req *http.Request, body []byte) (eventflow.Event, error) {
	if strings.Contains(req.Header.Get("content-type"), "application/cloudevents+json") {
		var event eventflow.Event
		if err := json.Unmarshal(body, &event); err != nil {
			return eventflow.Event{}, err
		}
		return event, nil
	}
	var ol admission.OpenLineageEvent
	if err := json.Unmarshal(body, &ol); err != nil {
		return eventflow.Event{}, err
	}
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetType(admission.CloudEventType)
	event.SetSource("urn:eventflow:http")
	if ol.Run.RunID != "" {
		event.SetID(ol.Run.RunID)
	} else {
		event.SetID(fmt.Sprintf("http-%d", time.Now().UnixNano()))
	}
	event.SetSubject(ol.Job.Namespace + "/" + ol.Job.Name)
	_ = event.SetData(sdk.ApplicationJSON, body)
	return event, nil
}

// TestEventFromBytes wraps raw OpenLineage JSON exactly as HTTP ingress does.
func TestEventFromBytes(body []byte) (eventflow.Event, error) {
	req, _ := http.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	return eventFromRequest(req, body)
}

func (r *Receiver) principal(req *http.Request) string {
	if req.TLS != nil && len(req.TLS.PeerCertificates) > 0 {
		return req.TLS.PeerCertificates[0].Subject.String()
	}
	if r.config.PrincipalHeader == "" || !r.trustedProxy(req.RemoteAddr) {
		return ""
	}
	return req.Header.Get(r.config.PrincipalHeader)
}

func (r *Receiver) trustedProxy(remote string) bool {
	if len(r.config.TrustedProxyCIDRs) == 0 {
		return false
	}
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(host)
	for _, cidr := range r.config.TrustedProxyCIDRs {
		if cidr == "*" {
			return true
		}
		_, network, err := net.ParseCIDR(cidr)
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}

type Emitter struct {
	url               string
	client            *http.Client
	idempotencyHeader string
}

func (e *Emitter) Open(context.Context) error  { return nil }
func (e *Emitter) Close(context.Context) error { return nil }

func (e *Emitter) Emit(ctx context.Context, event eventflow.Event) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(event.Data()))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	if e.idempotencyHeader != "" {
		req.Header.Set(e.idempotencyHeader, event.ID())
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http status %d", resp.StatusCode)
	}
	return nil
}
