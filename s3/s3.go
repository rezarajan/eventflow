// Package s3 provides S3-compatible Eventflow adapters without provisioning buckets.
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/resource"
)

// PutObjectInput is the minimal request shape needed from an S3-compatible client.
type PutObjectInput struct {
	Bucket         string
	Key            string
	Body           io.Reader
	ContentType    string
	Metadata       map[string]string
	Tags           map[string]string
	ChecksumSHA256 string
}

// GetObjectInput is the minimal object retrieval request.
type GetObjectInput struct {
	Bucket string
	Key    string
}

// GetObjectOutput is the minimal object retrieval response.
type GetObjectOutput struct {
	Body io.ReadCloser
}

// Client is implemented by AWS SDK v2 wrappers and S3-compatible fakes.
type Client interface {
	PutObject(ctx context.Context, input PutObjectInput) error
	GetObject(ctx context.Context, input GetObjectInput) (GetObjectOutput, error)
}

// Config defines S3-compatible adapter settings.
type Config struct {
	Endpoint           string
	Region             string
	Bucket             string
	Prefix             string
	PathStyle          bool
	Metadata           map[string]string
	Tags               map[string]string
	OneEventPerObject  bool
	MultipartThreshold int64
	Client             Client
}

type ResourceSpec struct {
	Endpoint           string            `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Region             string            `yaml:"region,omitempty" json:"region,omitempty"`
	Bucket             string            `yaml:"bucket" json:"bucket"`
	Prefix             string            `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	PathStyle          bool              `yaml:"pathStyle,omitempty" json:"pathStyle,omitempty"`
	Metadata           map[string]string `yaml:"metadata,omitempty" json:"metadata,omitempty"`
	Tags               map[string]string `yaml:"tags,omitempty" json:"tags,omitempty"`
	OneEventPerObject  bool              `yaml:"oneEventPerObject,omitempty" json:"oneEventPerObject,omitempty"`
	MultipartThreshold int64             `yaml:"multipartThreshold,omitempty" json:"multipartThreshold,omitempty"`
	FetchObjects       bool              `yaml:"fetchObjects,omitempty" json:"fetchObjects,omitempty"`
}

func Register(catalog *resource.Catalog) error {
	if err := resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK:      resource.GVK("S3Emitter"),
		Validate: validateResource,
		Build: func(_ context.Context, _ resource.BuildContext, spec ResourceSpec) (any, error) {
			return NewEmitter(configFromSpec(spec)), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityEmitter, resource.CapabilityBatchEmission},
	}); err != nil {
		return err
	}
	return resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK:      resource.GVK("S3NotificationObserver"),
		Validate: validateResource,
		Build: func(_ context.Context, _ resource.BuildContext, spec ResourceSpec) (any, error) {
			observer := NewObserver(configFromSpec(spec), nil)
			observer.FetchObjects = spec.FetchObjects
			return observer, nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityObserver},
	})
}

func validateResource(_ context.Context, spec ResourceSpec) error {
	if spec.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	return nil
}

func configFromSpec(spec ResourceSpec) Config {
	return Config{
		Endpoint: spec.Endpoint, Region: spec.Region, Bucket: spec.Bucket, Prefix: spec.Prefix,
		PathStyle: spec.PathStyle, Metadata: spec.Metadata, Tags: spec.Tags,
		OneEventPerObject: spec.OneEventPerObject, MultipartThreshold: spec.MultipartThreshold,
	}
}

// Emitter writes events to S3-compatible object storage.
type Emitter struct {
	config Config
}

// NewEmitter constructs an S3 emitter.
func NewEmitter(config Config) *Emitter { return &Emitter{config: config} }

// Name returns the adapter name.
func (*Emitter) Name() string { return "s3" }

// Open validates configuration.
func (e *Emitter) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.config.Client == nil {
		return fmt.Errorf("s3 client is required")
	}
	if e.config.Bucket == "" {
		return fmt.Errorf("s3 bucket is required")
	}
	return nil
}

// Emit writes one structured CloudEvent object.
func (e *Emitter) Emit(ctx context.Context, event eventflow.Event) error {
	return e.EmitBatch(ctx, []eventflow.Event{event})
}

// EmitBatch writes events as one object per event or one NDJSON batch object.
func (e *Emitter) EmitBatch(ctx context.Context, events []eventflow.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.config.OneEventPerObject || len(events) == 1 {
		for _, event := range events {
			if err := e.putEvent(ctx, event); err != nil {
				return err
			}
		}
		return nil
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return eventflow.ValidationError("validate cloudevent", err)
		}
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	key := path.Join(e.config.Prefix, fmt.Sprintf("batch-%d.ndjson", time.Now().UTC().UnixNano()))
	return e.config.Client.PutObject(ctx, PutObjectInput{
		Bucket:      e.config.Bucket,
		Key:         key,
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: "application/x-ndjson",
		Metadata:    e.config.Metadata,
		Tags:        e.config.Tags,
	})
}

// Close releases resources.
func (*Emitter) Close(ctx context.Context) error { return ctx.Err() }

func (e *Emitter) putEvent(ctx context.Context, event eventflow.Event) error {
	if err := event.Validate(); err != nil {
		return eventflow.ValidationError("validate cloudevent", err)
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	key := path.Join(e.config.Prefix, safeKey(event.ID())+".json")
	return e.config.Client.PutObject(ctx, PutObjectInput{
		Bucket:      e.config.Bucket,
		Key:         key,
		Body:        bytes.NewReader(body),
		ContentType: "application/cloudevents+json",
		Metadata:    e.config.Metadata,
		Tags:        e.config.Tags,
	})
}

// Notification describes one S3 or MinIO object notification.
type Notification struct {
	Bucket string
	Key    string
	Time   time.Time
}

// Observer decodes object notifications into observations and can fetch CloudEvents.
type Observer struct {
	config        Config
	Notifications <-chan Notification
	FetchObjects  bool
}

// NewObserver constructs an S3 observer.
func NewObserver(config Config, notifications <-chan Notification) *Observer {
	return &Observer{config: config, Notifications: notifications}
}

// Open validates observer dependencies.
func (o *Observer) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.Notifications == nil {
		return fmt.Errorf("s3 notification channel is required")
	}
	if o.FetchObjects && o.config.Client == nil {
		return fmt.Errorf("s3 client is required when FetchObjects is enabled")
	}
	return nil
}

// Observe reads one object notification.
func (o *Observer) Observe(ctx context.Context) (eventflow.Observation, error) {
	select {
	case <-ctx.Done():
		return eventflow.Observation{}, ctx.Err()
	case notification, ok := <-o.Notifications:
		if !ok {
			return eventflow.Observation{}, io.EOF
		}
		observation := eventflow.Observation{
			Source:     "s3://" + notification.Bucket,
			Subject:    notification.Key,
			Time:       notification.Time,
			Attributes: map[string]string{"bucket": notification.Bucket, "key": notification.Key},
		}
		if o.FetchObjects {
			event, err := o.fetchEvent(ctx, notification)
			if err != nil {
				return eventflow.Observation{}, err
			}
			observation.Event = &event
		}
		return observation, nil
	}
}

// Close releases observer resources.
func (*Observer) Close(ctx context.Context) error { return ctx.Err() }

func (o *Observer) fetchEvent(ctx context.Context, notification Notification) (eventflow.Event, error) {
	out, err := o.config.Client.GetObject(ctx, GetObjectInput{Bucket: notification.Bucket, Key: notification.Key})
	if err != nil {
		return eventflow.Event{}, err
	}
	defer out.Body.Close()
	var event eventflow.Event
	if err := json.NewDecoder(out.Body).Decode(&event); err != nil {
		return eventflow.Event{}, err
	}
	if err := event.Validate(); err != nil {
		return eventflow.Event{}, eventflow.ValidationError("validate cloudevent", err)
	}
	return event, nil
}

func safeKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "event"
	}
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(value)
}
