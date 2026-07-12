# Extending Eventflow

This guide shows how to add new declarative resource kinds and how each kind
fits into the runtime. Eventflow has no global adapter registry. Applications
create a `resource.Catalog`, register the kinds they allow, load YAML, and
compile resources into Go interfaces.

## Extension Model

A resource kind has three parts:

| Part | Purpose |
| --- | --- |
| Spec struct | The strict YAML shape under `spec`. |
| Runtime object | The Go value built from the spec. |
| Definition | Decode, default, validate, reference, build, and capability rules. |

Definitions are registered explicitly:

```go
catalog := resource.NewCatalog()
if err := filesystem.Register(catalog); err != nil {
	return err
}
if err := myadapter.Register(catalog); err != nil {
	return err
}

docs, err := resource.LoadFiles("eventflow.yaml")
if err != nil {
	return err
}
compiled, err := resource.Compile(ctx, catalog, docs)
if err != nil {
	return err
}
```

`resource.NewCatalog` registers core kinds: `EventContract`, `EventFlow`, and
`ObservationFlow`. Adapter packages register their own kinds.

## Capabilities

Declare only capabilities the built object actually supports.

| Capability | Runtime expectation |
| --- | --- |
| `CapabilityEmitter` | Implements `eventflow.Emitter`. |
| `CapabilityBatchEmission` | Implements `eventflow.BatchEmitter`. |
| `CapabilityReceiver` | Implements `eventflow.Receiver`. |
| `CapabilityObserver` | Implements `eventflow.Observer`. |
| `CapabilityObservationMapper` | Implements `eventflow.ObservationMapper`. |
| `CapabilityObservationSource` | Provides an adapter-specific source interface, such as `s3.NotificationSource`. |
| `CapabilityValidator` | Implements `eventflow.Validator`. |
| `CapabilityCodec` | Implements `eventflow.Codec`. |

The compiler checks references against these capabilities before it builds a
runtime. For example, `EventFlow.receiverRef` must point to a resource that
declares `CapabilityReceiver`.

## Event Contracts

Developers add contracts declaratively. A contract is not an adapter and does
not read or write events; it defines the accepted CloudEvents envelope.

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: invoice-posted
spec:
  type: invoice.posted.v1
  sourceRegex: '^urn:billing:'
  dataContentType: application/json
  dataSchema: ./contracts/invoice-posted.v1.schema.json
  requiredExtensions:
    - correlationid
```

Add the contract to an `EventFlow` or `ObservationFlow` with `contractRefs`.
Use a new versioned event type for breaking payload changes.

## Event Flows

Use `EventFlow` when the source already produces CloudEvents through an
`eventflow.Receiver`.

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: billing-events
spec:
  receiverRef:
    kind: RedpandaReceiver
    name: billing-input
  contractRefs:
    - kind: EventContract
      name: invoice-posted
  emitterRefs:
    - kind: FilesystemEmitter
      name: accepted-events
  invalidEmitterRef:
    kind: FilesystemEmitter
    name: rejected-events
```

An `EventFlow` needs one receiver and at least one emitter. The receiver,
contracts, emitters, and invalid-event target are all explicit references.

## Observation Flows

Use `ObservationFlow` when the source reports platform activity that is not yet
a domain event. S3 object-created notifications are the canonical example.

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: ObservationFlow
metadata:
  name: uploads
spec:
  observerRef:
    kind: S3NotificationObserver
    name: upload-observer
  mapperRef:
    kind: S3ObjectCreatedMapper
    name: upload-mapper
  contractRefs:
    - kind: EventContract
      name: upload-detected
  emitterRefs:
    - kind: RedpandaEmitter
      name: upload-events
```

The observer emits `eventflow.Observation` values. The mapper converts each
observation into an `eventflow.Event`. The resulting event then follows the
same validation and emission path as an event from `EventFlow`.

## Emitters

An emitter sends CloudEvents to a destination. Implement `eventflow.Emitter`
and register a resource that declares `CapabilityEmitter`.

```go
type HTTPSinkSpec struct {
	URL string `yaml:"url" json:"url"`
}

func Register(catalog *resource.Catalog) error {
	return resource.Register(catalog, resource.Definition[HTTPSinkSpec]{
		GVK: resource.GVK("HTTPSink"),
		Validate: func(_ context.Context, spec HTTPSinkSpec) error {
			if strings.TrimSpace(spec.URL) == "" {
				return fmt.Errorf("url is required")
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec HTTPSinkSpec) (any, error) {
			return NewEmitter(EmitterConfig{URL: spec.URL}), nil
		},
		Capabilities: []resource.Capability{
			resource.CapabilityComponent,
			resource.CapabilityEmitter,
		},
	})
}
```

If the emitter supports atomic or efficient batches, implement
`eventflow.BatchEmitter` and also declare `CapabilityBatchEmission`.

## Receivers

A receiver pulls CloudEvents from a source. It must implement
`eventflow.Receiver` to be usable as `EventFlow.receiverRef`.

```go
type QueueReceiverSpec struct {
	Queue string `yaml:"queue" json:"queue"`
}

func Register(catalog *resource.Catalog) error {
	return resource.Register(catalog, resource.Definition[QueueReceiverSpec]{
		GVK: resource.GVK("QueueReceiver"),
		Validate: func(_ context.Context, spec QueueReceiverSpec) error {
			if spec.Queue == "" {
				return fmt.Errorf("queue is required")
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec QueueReceiverSpec) (any, error) {
			return NewReceiver(ReceiverConfig{Queue: spec.Queue}), nil
		},
		Capabilities: []resource.Capability{
			resource.CapabilityComponent,
			resource.CapabilityReceiver,
		},
	})
}
```

HTTP handlers are usually push endpoints, not pull receivers. Register them as
components unless they implement `Open`, `Receive`, and `Close`.

## Observers

An observer reads platform activity and returns observations. It does not decide
the domain event type by itself.

```go
type WebhookObserverSpec struct {
	SourceRef resource.Reference `yaml:"sourceRef" json:"sourceRef"`
}
```

Observers commonly depend on an adapter-specific source resource. The definition
uses `References` to require that dependency and `BuildContext.Get` to retrieve
it:

```go
References: func(spec WebhookObserverSpec) []resource.Reference {
	ref := spec.SourceRef
	ref.Capability = resource.CapabilityObservationSource
	return []resource.Reference{ref}
},
Build: func(_ context.Context, bctx resource.BuildContext, spec WebhookObserverSpec) (any, error) {
	source, err := bctx.Get(spec.SourceRef)
	if err != nil {
		return nil, err
	}
	typedSource, ok := source.(WebhookSource)
	if !ok {
		return nil, fmt.Errorf("%s does not provide webhook events", spec.SourceRef.Key())
	}
	return NewObserver(typedSource), nil
},
Capabilities: []resource.Capability{
	resource.CapabilityComponent,
	resource.CapabilityObserver,
},
```

## Observation Sources

An observation source is adapter-specific input plumbing. The S3 package uses
`s3.NotificationSource`; another adapter might use a queue, webhook server, or
file reader. Register source resources with `CapabilityObservationSource` and
have observers reference them.

Keep source interfaces narrow and local to the adapter package. The core SDK
does not need to know about SQS, SNS, EventBridge, MinIO, or any other backing
service.

## Observation Mappers

An observation mapper converts an observation into a CloudEvent. It owns the
domain event type, source, subject, and payload mapping.

```go
type UploadMapperSpec struct {
	Type   string `yaml:"type" json:"type"`
	Source string `yaml:"source,omitempty" json:"source,omitempty"`
}

type UploadMapper struct {
	Spec UploadMapperSpec
}

func (m UploadMapper) MapObservation(ctx context.Context, obs eventflow.Observation) (eventflow.Event, error) {
	if err := ctx.Err(); err != nil {
		return eventflow.Event{}, err
	}
	event := cloudevents.NewEvent(cloudevents.VersionV1)
	event.SetID(obs.Subject)
	event.SetType(m.Spec.Type)
	event.SetSource(m.Spec.Source)
	event.SetSubject(obs.Subject)
	if !obs.Time.IsZero() {
		event.SetTime(obs.Time)
	}
	if err := event.SetData(cloudevents.ApplicationJSON, obs.Attributes); err != nil {
		return eventflow.Event{}, err
	}
	return event, event.Validate()
}
```

Register the mapper with `CapabilityObservationMapper`. Then use it as
`ObservationFlow.mapperRef`.

## Validators And Codecs

Validators and codecs are extension points for applications that need custom
validation or representation handling.

| Kind to build | Implement | Declare |
| --- | --- | --- |
| Validator resource | `eventflow.Validator` | `CapabilityValidator` |
| Codec resource | `eventflow.Codec` | `CapabilityCodec` |

The core `EventFlow` spec already accepts `validatorRefs` and `codecRefs`, but
the current runtime composition uses the built-in contract validator. Treat
custom validator and codec resources as SDK extension points until a flow kind
or command wires them into runtime execution.

## Defaults And Validation

Use `Default` for deterministic defaults and `Validate` for semantic checks.
Unknown fields are rejected by default through `resource.DecodeStrict`.

```go
Default: func(spec *MySpec) error {
	if spec.Mode == "" {
		spec.Mode = "structured"
	}
	return nil
},
Validate: func(_ context.Context, spec MySpec) error {
	if spec.Name == "" {
		return fmt.Errorf("name is required")
	}
	return nil
},
```

Validation should fail early for missing required fields, unsupported modes,
invalid durations, impossible reference combinations, and missing constructor
dependencies.

## Value Sources

Use `resource.ValueSource[T]` only for fields where indirect values are safe.
It supports exactly one literal, environment variable, or file reference. It
does not support unrestricted interpolation.

```go
type TokenSpec struct {
	Token resource.ValueSource[string] `yaml:"token" json:"token"`
}
```

Resolve the value in `Build` with a parser:

```go
token, err := spec.Token.Resolve(func(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("token is empty")
	}
	return value, nil
})
```

## Testing

At minimum, test:

- strict decoding rejects unknown fields,
- defaults are applied,
- invalid specs fail validation,
- missing references fail,
- capability mismatches fail,
- the built object implements the declared capability,
- flow manifests compile and run against fakes or local test sources.

Use constructor-injected clients for networked adapters. Eventflow resource
definitions should build components; they should not provision infrastructure or
hide global clients.
