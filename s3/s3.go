// Package s3 provides S3-compatible Eventflow adapters without provisioning buckets.
package s3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"

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

// EmitterSpec is the declarative spec for S3Emitter.
//
// S3Emitter writes structured CloudEvents as JSON objects or NDJSON batch
// objects to an existing S3-compatible bucket. The adapter does not provision
// buckets, credentials, notifications, or IAM policies.
type EmitterSpec struct {
	Endpoint           string            `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Region             string            `yaml:"region,omitempty" json:"region,omitempty"`
	Bucket             string            `yaml:"bucket" json:"bucket"`
	Prefix             string            `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	PathStyle          bool              `yaml:"pathStyle,omitempty" json:"pathStyle,omitempty"`
	Metadata           map[string]string `yaml:"metadata,omitempty" json:"metadata,omitempty"`
	Tags               map[string]string `yaml:"tags,omitempty" json:"tags,omitempty"`
	OneEventPerObject  bool              `yaml:"oneEventPerObject,omitempty" json:"oneEventPerObject,omitempty"`
	MultipartThreshold int64             `yaml:"multipartThreshold,omitempty" json:"multipartThreshold,omitempty"`
}

// ObserverSpec is the declarative spec for S3NotificationObserver.
//
// The observer consumes object notifications from sourceRef and produces
// Eventflow observations. It does not fetch object contents or emit CloudEvents
// by itself; an ObservationFlow pairs it with an observation mapper.
type ObserverSpec struct {
	Bucket    string             `yaml:"bucket" json:"bucket"`
	Prefix    string             `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	SourceRef resource.Reference `yaml:"sourceRef" json:"sourceRef"`
}

// NotificationFileSourceSpec is the declarative spec for S3NotificationFileSource.
//
// The file source is intended for tests, demos, and local runs. Each line in the
// file must be one JSON-encoded Notification.
type NotificationFileSourceSpec struct {
	Path string `yaml:"path" json:"path"`
}

// ObjectCreatedMapperSpec is the declarative spec for S3ObjectCreatedMapper.
//
// The mapper converts S3 object notifications into CloudEvents. Type is the
// resulting CloudEvents type, Source defaults to urn:eventflow:s3, and
// SubjectTemplate may use {{bucket}}, {{key}}, {{eventName}}, {{etag}}, and
// {{size}} placeholders.
type ObjectCreatedMapperSpec struct {
	Type            string     `yaml:"type" json:"type"`
	Source          string     `yaml:"source,omitempty" json:"source,omitempty"`
	SubjectTemplate string     `yaml:"subjectTemplate,omitempty" json:"subjectTemplate,omitempty"`
	Data            MapperData `yaml:"data,omitempty" json:"data,omitempty"`
}

// MapperData controls which notification attributes are copied into event data.
type MapperData struct {
	IncludeBucket bool `yaml:"includeBucket,omitempty" json:"includeBucket,omitempty"`
	IncludeKey    bool `yaml:"includeKey,omitempty" json:"includeKey,omitempty"`
	IncludeEvent  bool `yaml:"includeEvent,omitempty" json:"includeEvent,omitempty"`
	IncludeETag   bool `yaml:"includeETag,omitempty" json:"includeETag,omitempty"`
	IncludeSize   bool `yaml:"includeSize,omitempty" json:"includeSize,omitempty"`
}

// NotificationSource opens a stream of S3-compatible notifications.
//
// Production integrations typically implement NotificationSource using SQS,
// EventBridge, MinIO bucket notifications, or another queue fed by object
// storage. NotificationFileSource is a local file implementation.
type NotificationSource interface {
	OpenNotifications(ctx context.Context) (<-chan Notification, error)
}

// Register adds S3Emitter, S3NotificationObserver, S3NotificationFileSource,
// and S3ObjectCreatedMapper resource definitions.
func Register(catalog *resource.Catalog) error {
	if err := resource.Register(catalog, resource.Definition[EmitterSpec]{
		GVK:      resource.GVK("S3Emitter"),
		Validate: validateEmitterResource,
		Build: func(_ context.Context, _ resource.BuildContext, spec EmitterSpec) (any, error) {
			return NewEmitter(configFromEmitterSpec(spec)), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityEmitter, resource.CapabilityBatchEmission},
	}); err != nil {
		return err
	}
	if err := resource.Register(catalog, resource.Definition[ObserverSpec]{
		GVK:      resource.GVK("S3NotificationObserver"),
		Validate: validateObserverResource,
		References: func(spec ObserverSpec) []resource.Reference {
			ref := spec.SourceRef
			ref.Capability = resource.CapabilityObservationSource
			return []resource.Reference{ref}
		},
		Build: func(_ context.Context, bctx resource.BuildContext, spec ObserverSpec) (any, error) {
			sourceObj, err := bctx.Get(spec.SourceRef)
			if err != nil {
				return nil, err
			}
			source, ok := sourceObj.(NotificationSource)
			if !ok {
				return nil, fmt.Errorf("%s does not provide S3 notifications", spec.SourceRef.Key())
			}
			observer := NewObserver(Config{Bucket: spec.Bucket, Prefix: spec.Prefix}, nil)
			observer.Source = source
			return observer, nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityObserver},
	}); err != nil {
		return err
	}
	if err := resource.Register(catalog, resource.Definition[NotificationFileSourceSpec]{
		GVK: resource.GVK("S3NotificationFileSource"),
		Validate: func(_ context.Context, spec NotificationFileSourceSpec) error {
			if strings.TrimSpace(spec.Path) == "" {
				return fmt.Errorf("path is required")
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec NotificationFileSourceSpec) (any, error) {
			return NotificationFileSource{Path: spec.Path}, nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityObservationSource},
	}); err != nil {
		return err
	}
	return resource.Register(catalog, resource.Definition[ObjectCreatedMapperSpec]{
		GVK: resource.GVK("S3ObjectCreatedMapper"),
		Default: func(spec *ObjectCreatedMapperSpec) error {
			if spec.Source == "" {
				spec.Source = "urn:eventflow:s3"
			}
			if spec.SubjectTemplate == "" {
				spec.SubjectTemplate = "s3://{{bucket}}/{{key}}"
			}
			return nil
		},
		Validate: func(_ context.Context, spec ObjectCreatedMapperSpec) error {
			if strings.TrimSpace(spec.Type) == "" {
				return fmt.Errorf("type is required")
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec ObjectCreatedMapperSpec) (any, error) {
			return ObjectCreatedMapper{Spec: spec}, nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityObservationMapper},
	})
}

func validateEmitterResource(_ context.Context, spec EmitterSpec) error {
	if spec.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	return nil
}

func validateObserverResource(_ context.Context, spec ObserverSpec) error {
	if spec.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if spec.SourceRef.Kind == "" || spec.SourceRef.Name == "" {
		return fmt.Errorf("sourceRef kind and name are required")
	}
	return nil
}

func configFromEmitterSpec(spec EmitterSpec) Config {
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
	Bucket    string    `json:"bucket" yaml:"bucket"`
	Key       string    `json:"key" yaml:"key"`
	EventName string    `json:"eventName,omitempty" yaml:"eventName,omitempty"`
	ETag      string    `json:"etag,omitempty" yaml:"etag,omitempty"`
	Size      int64     `json:"size,omitempty" yaml:"size,omitempty"`
	Time      time.Time `json:"time,omitempty" yaml:"time,omitempty"`
}

// Observer decodes object notifications into observations.
type Observer struct {
	config        Config
	Notifications <-chan Notification
	Source        NotificationSource
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
	if o.Notifications == nil && o.Source != nil {
		notifications, err := o.Source.OpenNotifications(ctx)
		if err != nil {
			return err
		}
		o.Notifications = notifications
	}
	if o.Notifications == nil {
		return fmt.Errorf("s3 notification source is required")
	}
	return nil
}

// Observe reads one object notification.
func (o *Observer) Observe(ctx context.Context) (eventflow.Observation, error) {
	for {
		select {
		case <-ctx.Done():
			return eventflow.Observation{}, ctx.Err()
		case notification, ok := <-o.Notifications:
			if !ok {
				return eventflow.Observation{}, io.EOF
			}
			if !o.matches(notification) {
				continue
			}
			observation := eventflow.Observation{
				Source:     "s3://" + notification.Bucket,
				Subject:    notification.Key,
				Time:       notification.Time,
				Attributes: notificationAttributes(notification),
			}
			return observation, nil
		}
	}
}

// Close releases observer resources.
func (*Observer) Close(ctx context.Context) error { return ctx.Err() }

func (o *Observer) matches(notification Notification) bool {
	if o.config.Bucket != "" && notification.Bucket != o.config.Bucket {
		return false
	}
	if o.config.Prefix != "" && !strings.HasPrefix(notification.Key, o.config.Prefix) {
		return false
	}
	return true
}

type NotificationFileSource struct {
	Path string
}

// OpenNotifications reads all configured file notifications and streams them.
func (s NotificationFileSource) OpenNotifications(ctx context.Context) (<-chan Notification, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var notifications []Notification
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var notification Notification
		if err := json.Unmarshal(scanner.Bytes(), &notification); err != nil {
			return nil, fmt.Errorf("decode s3 notification %s: %w", s.Path, err)
		}
		notifications = append(notifications, notification)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read s3 notifications %s: %w", s.Path, err)
	}
	out := make(chan Notification)
	go func() {
		defer close(out)
		for _, notification := range notifications {
			select {
			case <-ctx.Done():
				return
			case out <- notification:
			}
		}
	}()
	return out, nil
}

type ObjectCreatedMapper struct {
	Spec ObjectCreatedMapperSpec
}

// MapObservation converts one S3 observation into a CloudEvent.
func (m ObjectCreatedMapper) MapObservation(ctx context.Context, observation eventflow.Observation) (eventflow.Event, error) {
	if err := ctx.Err(); err != nil {
		return eventflow.Event{}, err
	}
	bucket := observation.Attributes["bucket"]
	key := observation.Attributes["key"]
	if bucket == "" || key == "" {
		return eventflow.Event{}, fmt.Errorf("s3 observation requires bucket and key attributes")
	}
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID(stableEventID(bucket, key, observation.Time))
	event.SetType(m.Spec.Type)
	event.SetSource(m.Spec.Source)
	event.SetSubject(renderSubject(m.Spec.SubjectTemplate, observation.Attributes))
	if !observation.Time.IsZero() {
		event.SetTime(observation.Time)
	}
	data := map[string]any{}
	if m.Spec.Data.IncludeBucket {
		data["bucket"] = bucket
	}
	if m.Spec.Data.IncludeKey {
		data["key"] = key
	}
	if m.Spec.Data.IncludeEvent && observation.Attributes["eventName"] != "" {
		data["eventName"] = observation.Attributes["eventName"]
	}
	if m.Spec.Data.IncludeETag && observation.Attributes["etag"] != "" {
		data["etag"] = observation.Attributes["etag"]
	}
	if m.Spec.Data.IncludeSize && observation.Attributes["size"] != "" {
		size, err := strconv.ParseInt(observation.Attributes["size"], 10, 64)
		if err != nil {
			return eventflow.Event{}, fmt.Errorf("invalid s3 object size %q", observation.Attributes["size"])
		}
		data["size"] = size
	}
	if len(data) == 0 {
		data["bucket"] = bucket
		data["key"] = key
	}
	if err := event.SetData(sdk.ApplicationJSON, data); err != nil {
		return eventflow.Event{}, err
	}
	if err := event.Validate(); err != nil {
		return eventflow.Event{}, eventflow.ValidationError("validate mapped s3 event", err)
	}
	return event, nil
}

func notificationAttributes(notification Notification) map[string]string {
	attrs := map[string]string{"bucket": notification.Bucket, "key": notification.Key}
	if notification.EventName != "" {
		attrs["eventName"] = notification.EventName
	}
	if notification.ETag != "" {
		attrs["etag"] = notification.ETag
	}
	if notification.Size > 0 {
		attrs["size"] = strconv.FormatInt(notification.Size, 10)
	}
	return attrs
}

func renderSubject(template string, attrs map[string]string) string {
	out := template
	for _, key := range []string{"bucket", "key", "eventName", "etag", "size"} {
		out = strings.ReplaceAll(out, "{{"+key+"}}", attrs[key])
	}
	return out
}

func stableEventID(bucket string, key string, t time.Time) string {
	if t.IsZero() {
		return safeKey(bucket + "/" + key)
	}
	return safeKey(bucket + "/" + key + "/" + t.UTC().Format(time.RFC3339Nano))
}

func safeKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "event"
	}
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(value)
}
