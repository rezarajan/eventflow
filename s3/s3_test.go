package s3

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rezarajan/eventflow/filesystem"
	"github.com/rezarajan/eventflow/resource"
)

func TestNotificationFileSourceAndMapper(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifications.ndjson")
	body := `{"bucket":"school-uploads","key":"incoming/a.pdf","eventName":"ObjectCreated:Put","etag":"abc","size":123,"time":"2026-07-11T10:00:00Z"}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	source := NotificationFileSource{Path: path}
	notifications, err := source.OpenNotifications(context.Background())
	if err != nil {
		t.Fatalf("OpenNotifications() error = %v", err)
	}
	observer := NewObserver(Config{Bucket: "school-uploads", Prefix: "incoming/"}, notifications)
	if err := observer.Open(context.Background()); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	observation, err := observer.Observe(context.Background())
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	mapper := ObjectCreatedMapper{Spec: ObjectCreatedMapperSpec{
		Type:            "document.upload.detected.v1",
		Source:          "urn:test:s3",
		SubjectTemplate: "s3://{{bucket}}/{{key}}",
		Data: MapperData{
			IncludeBucket: true,
			IncludeKey:    true,
			IncludeEvent:  true,
			IncludeETag:   true,
			IncludeSize:   true,
		},
	}}
	event, err := mapper.MapObservation(context.Background(), observation)
	if err != nil {
		t.Fatalf("MapObservation() error = %v", err)
	}
	if event.Type() != "document.upload.detected.v1" {
		t.Fatalf("type = %s, want document.upload.detected.v1", event.Type())
	}
	if event.Subject() != "s3://school-uploads/incoming/a.pdf" {
		t.Fatalf("subject = %s", event.Subject())
	}
	if !strings.Contains(string(event.Data()), `"size":123`) {
		t.Fatalf("data = %s, want size", event.Data())
	}
}

func TestObservationFlowManifestWithS3Resources(t *testing.T) {
	dir := t.TempDir()
	notificationsPath := filepath.Join(dir, "notifications.ndjson")
	outputPath := filepath.Join(dir, "out.ndjson")
	body := `{"bucket":"school-uploads","key":"incoming/a.pdf","eventName":"ObjectCreated:Put","time":"2026-07-11T10:00:00Z"}` + "\n"
	if err := os.WriteFile(notificationsPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "eventflow.yaml")
	config := `apiVersion: eventflow.dev/v1alpha1
kind: S3NotificationFileSource
metadata: {name: source}
spec:
  path: ` + notificationsPath + `
---
apiVersion: eventflow.dev/v1alpha1
kind: S3NotificationObserver
metadata: {name: observer}
spec:
  bucket: school-uploads
  prefix: incoming/
  sourceRef: {kind: S3NotificationFileSource, name: source}
---
apiVersion: eventflow.dev/v1alpha1
kind: S3ObjectCreatedMapper
metadata: {name: mapper}
spec:
  type: document.upload.detected.v1
  source: urn:test:s3
  subjectTemplate: s3://{{bucket}}/{{key}}
  data:
    includeBucket: true
    includeKey: true
---
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata: {name: contract}
spec:
  type: document.upload.detected.v1
---
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemEmitter
metadata: {name: output}
spec:
  path: ` + outputPath + `
  format: ndjson
---
apiVersion: eventflow.dev/v1alpha1
kind: ObservationFlow
metadata: {name: observed}
spec:
  observerRef: {kind: S3NotificationObserver, name: observer}
  mapperRef: {kind: S3ObjectCreatedMapper, name: mapper}
  contractRefs:
    - {kind: EventContract, name: contract}
  emitterRefs:
    - {kind: FilesystemEmitter, name: output}
`
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	catalog := resource.NewCatalog()
	if err := Register(catalog); err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Register(catalog); err != nil {
		t.Fatal(err)
	}
	docs, err := resource.LoadFiles(configPath)
	if err != nil {
		t.Fatalf("LoadFiles() error = %v", err)
	}
	compiled, err := resource.Compile(context.Background(), catalog, docs)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(compiled.ObservationFlows) != 1 {
		t.Fatalf("observation flows = %d, want 1", len(compiled.ObservationFlows))
	}
	flow := compiled.ObservationFlows[0]
	for _, emitter := range flow.Emitters {
		if err := emitter.Open(context.Background()); err != nil {
			t.Fatalf("emitter.Open() error = %v", err)
		}
		defer emitter.Close(context.Background())
	}
	if err := flow.Runtime.Run(context.Background()); err != nil {
		t.Fatalf("ObservationRuntime.Run() error = %v", err)
	}
	output, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !strings.Contains(string(output), `"type":"document.upload.detected.v1"`) {
		t.Fatalf("output = %s", output)
	}
}
