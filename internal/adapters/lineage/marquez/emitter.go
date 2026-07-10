// Package marquez emits OpenLineage events to Marquez over HTTP.
package marquez

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/datascape/eventflow/internal/lineage"
)

// Config defines Marquez lineage emission settings.
type Config struct {
	URL     string
	Timeout time.Duration
}

// Emitter sends OpenLineage run events to a Marquez-compatible HTTP endpoint.
type Emitter struct {
	url    string
	client HTTPClient
}

// HTTPClient sends HTTP requests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// New constructs a Marquez emitter with the standard HTTP client.
func New(config Config) *Emitter {
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return NewWithClient(config, &http.Client{Timeout: timeout})
}

// NewWithClient constructs a Marquez emitter with an injected HTTP client for tests.
func NewWithClient(config Config, client HTTPClient) *Emitter {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	url := strings.TrimRight(config.URL, "/")
	if url == "" {
		url = "http://localhost:5000"
	}
	return &Emitter{url: url + "/api/v1/lineage", client: client}
}

// Emit posts one OpenLineage event to Marquez.
func (e *Emitter) Emit(ctx context.Context, event lineage.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal OpenLineage event: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create Marquez lineage request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("post OpenLineage event to Marquez: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("post OpenLineage event to Marquez: status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
}

// URL returns the resolved OpenLineage endpoint URL.
func (e *Emitter) URL() string {
	return e.url
}
