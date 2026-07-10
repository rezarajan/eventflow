// Package httpapi exposes producer-friendly HTTP endpoints for domain event ingress.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rezarajan/project-datascape/internal/app/ingest"
)

const eventPrefix = "/v1/events/"

// Handler accepts plain JSON domain payloads and delegates publication to the ingress service.
type Handler struct {
	Service Service
	MaxBody int64
}

// Service publishes producer-submitted domain events.
type Service interface {
	Publish(rctx context.Context, request ingest.PublishRequest) (ingest.PublishResult, error)
}

// ServeHTTP handles ingress requests for typed event routes.
func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	eventType, ok := strings.CutPrefix(r.URL.Path, eventPrefix)
	if !ok || eventType == "" || strings.Contains(eventType, "/") {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("use %s{event_type}", eventPrefix))
		return
	}
	payload, err := decodePayload(r.Body, h.maxBody())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if h.Service == nil {
		writeError(w, http.StatusInternalServerError, "not_configured", "ingress service is not configured")
		return
	}
	result, err := h.Service.Publish(r.Context(), ingest.PublishRequest{
		EventType:     eventType,
		Source:        headerAlias(r, "X-Eventflow-Source", "X-Datascape-Source"),
		Subject:       headerAlias(r, "X-Eventflow-Subject", "X-Datascape-Subject"),
		RunID:         headerAlias(r, "X-Eventflow-Run-ID", "X-Datascape-Run-ID"),
		CorrelationID: header(r, "X-Correlation-ID"),
		CausationID:   header(r, "X-Causation-ID"),
		Tenant:        headerAlias(r, "X-Eventflow-Tenant", "X-Datascape-Tenant"),
		Payload:       payload,
	})
	if err != nil {
		if ingest.IsValidationError(err) {
			writeError(w, http.StatusBadRequest, "validation_failed", err.Error())
			return
		}
		writeError(w, http.StatusServiceUnavailable, "publish_failed", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(result)
}

// decodePayload reads one JSON object from a request body.
func decodePayload(reader io.Reader, maxBody int64) (map[string]any, error) {
	limited := io.LimitReader(reader, maxBody)
	decoder := json.NewDecoder(limited)
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode JSON object: %w", err)
	}
	if payload == nil {
		return nil, fmt.Errorf("payload must be a JSON object")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("request body must contain one JSON object")
	}
	return payload, nil
}

// header returns a trimmed request header value.
func header(r *http.Request, key string) string {
	return strings.TrimSpace(r.Header.Get(key))
}

// headerAlias returns the first present header from a preferred and deprecated name.
func headerAlias(r *http.Request, preferred string, deprecated string) string {
	if value := header(r, preferred); value != "" {
		return value
	}
	return header(r, deprecated)
}

// maxBody returns the request body limit in bytes.
func (h Handler) maxBody() int64 {
	if h.MaxBody > 0 {
		return h.MaxBody
	}
	return 1 << 20
}

// writeError writes a structured JSON error response.
func writeError(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":   code,
		"message": message,
	})
}
