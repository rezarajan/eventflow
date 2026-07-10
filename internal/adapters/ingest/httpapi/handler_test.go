package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/datascape/lakehouse-poc/internal/app/ingest"
)

func TestHandlerPublishesTypedRoute(t *testing.T) {
	service := &fakeService{result: ingest.PublishResult{EventID: "evt-1", EventType: "attendance.submitted.v1", Source: "urn:test", Topic: "attendance.events.v1"}}
	req := httptest.NewRequest(http.MethodPost, "/v1/events/attendance.submitted.v1", strings.NewReader(`{"attendance_id":"att-1"}`))
	req.Header.Set("X-Datascape-Subject", "student-1")
	req.Header.Set("X-Correlation-ID", "corr-1")
	rec := httptest.NewRecorder()
	Handler{Service: service}.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if service.request.EventType != "attendance.submitted.v1" || service.request.Subject != "student-1" || service.request.CorrelationID != "corr-1" {
		t.Fatalf("unexpected publish request: %+v", service.request)
	}
}

func TestHandlerReturnsBadRequestForInvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/events/attendance.submitted.v1", strings.NewReader(`[]`))
	rec := httptest.NewRecorder()
	Handler{Service: &fakeService{}}.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlerReturnsBadRequestForValidationErrors(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/events/attendance.submitted.v1", strings.NewReader(`{"attendance_id":"att-1"}`))
	rec := httptest.NewRecorder()
	Handler{Service: &fakeService{err: ingest.ValidationError{Message: "schema failed"}}}.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlerReturnsUnavailableForPublishErrors(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/events/attendance.submitted.v1", strings.NewReader(`{"attendance_id":"att-1"}`))
	rec := httptest.NewRecorder()
	Handler{Service: &fakeService{err: errors.New("broker unavailable")}}.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

type fakeService struct {
	request ingest.PublishRequest
	result  ingest.PublishResult
	err     error
}

func (s *fakeService) Publish(ctx context.Context, request ingest.PublishRequest) (ingest.PublishResult, error) {
	s.request = request
	return s.result, s.err
}
