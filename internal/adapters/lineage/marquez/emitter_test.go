package marquez

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/project-datascape/internal/lineage"
)

// TestEmitterPostsOpenLineageEvent verifies Marquez emission uses the OpenLineage endpoint.
func TestEmitterPostsOpenLineageEvent(t *testing.T) {
	client := &fakeHTTPClient{status: http.StatusCreated}
	emitter := NewWithClient(Config{URL: "http://marquez:5000/"}, client)
	event := lineage.NewEvent("START", "datascape", "job", "run-1", nil, nil, nil, fixedTime)
	if err := emitter.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}
	if client.method != http.MethodPost || client.url != "http://marquez:5000/api/v1/lineage" {
		t.Fatalf("unexpected request: method=%s url=%s", client.method, client.url)
	}
	if !strings.Contains(client.requestBody, `"eventType":"START"`) || !strings.Contains(client.requestBody, `"producer"`) || !strings.Contains(client.requestBody, `"schemaURL"`) {
		t.Fatalf("unexpected request body: %s", client.requestBody)
	}
}

// TestEmitterReturnsHTTPError verifies non-success responses are surfaced.
func TestEmitterReturnsHTTPError(t *testing.T) {
	client := &fakeHTTPClient{status: http.StatusBadRequest, body: "bad event"}
	emitter := NewWithClient(Config{URL: "http://marquez:5000"}, client)
	err := emitter.Emit(context.Background(), lineage.NewEvent("START", "datascape", "job", "run-1", nil, nil, nil, fixedTime))
	if err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("expected status error, got %v", err)
	}
}

// fakeHTTPClient records one HTTP request.
type fakeHTTPClient struct {
	status      int
	body        string
	method      string
	url         string
	requestBody string
}

// Do records a request and returns a fake response.
func (c *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	requestBody, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	c.method = req.Method
	c.url = req.URL.String()
	c.requestBody = string(requestBody)
	return &http.Response{
		StatusCode: c.status,
		Body:       io.NopCloser(strings.NewReader(c.body)),
	}, nil
}

// fixedTime returns a stable timestamp for tests.
func fixedTime() time.Time {
	return time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
}
