# Eventflow Concepts

Eventflow is built around a small set of ports and declarative resources. The
ports are Go interfaces. The resources are YAML documents that describe which
components exist and how they are connected.

## Core Model

An Eventflow system has four layers:

| Layer | Role |
| --- | --- |
| Event | A CloudEvent carried through the system. |
| Contract | Rules for which CloudEvents a flow accepts. |
| Component | An adapter instance such as a filesystem receiver or Redpanda emitter. |
| EventFlow | The wiring that connects components and contracts into a runtime. |

The root package defines the runtime ports:

| Port | Purpose |
| --- | --- |
| `Emitter` | Sends events to a destination. |
| `BatchEmitter` | Sends event batches when the destination supports it. |
| `Receiver` | Pulls events from a source. |
| `Observer` | Turns platform activity into observations. |
| `Validator` | Validates CloudEvents before handling. |
| `Codec` | Encodes and decodes event representations. |
| `Runtime` | Runs a receiver, validator, and handler together. |

The `resource` package compiles YAML resources into these ports.

## Resource Envelope

Every resource uses the same envelope:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemEmitter
metadata:
  name: local-output
spec:
  path: ./events.ndjson
  format: ndjson
```

`apiVersion` selects the declarative API version. `kind` selects the registered
resource definition. `metadata.name` gives the resource a stable identity.
`spec` is kind-specific and is decoded strictly; unknown fields are rejected.

## Catalogs And Registration

Resource kinds are not global. A caller creates a catalog and registers the
resource definitions it wants to allow:

```go
catalog := resource.NewCatalog()
_ = filesystem.Register(catalog)
_ = redpanda.Register(catalog)
```

The `cmd/eventflow` command uses `adapters/bundled` to register the in-repo
adapter kinds. SDK users can register only the adapters they need.

Adapter packages own their resource definitions. For example, the filesystem
package owns `FilesystemEmitter` and `FilesystemReceiver`.

## Capabilities

Every resource definition declares what it can do. Eventflow uses capabilities
to reject invalid wiring before runtime starts.

Common capabilities:

| Capability | Meaning |
| --- | --- |
| `emitter` | Can emit events. |
| `receiver` | Can receive events. |
| `observer` | Can observe platform activity. |
| `validator` | Can validate events. |
| `codec` | Can encode or decode events. |
| `eventContract` | Is an event contract. |
| `eventFlow` | Is a compiled flow. |

If an `EventFlow.receiverRef` points at an emitter, validation fails with a
capability mismatch.

## References

Resources link together with explicit references:

```yaml
receiverRef:
  kind: FilesystemReceiver
  name: local-input
emitterRefs:
  - kind: FilesystemEmitter
    name: local-output
```

References are explicit. Eventflow does not use selectors, labels, global
adapter maps, or implicit name lookup. The referenced resource must exist in
the same loaded document set.

## Event Contracts

An `EventContract` declares the CloudEvents rules for one event type:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: attendance-submitted
spec:
  type: attendance.submitted.v1
  sourceRegex: '^urn:school:'
  dataContentType: application/json
  payloadSchema: ./contracts/events/payloads/attendance-submitted.v1.schema.json
  requiredExtensions:
    - correlationid
  validationMode: strict
```

Important fields:

| Field | Purpose |
| --- | --- |
| `type` | Required CloudEvents `type`. |
| `source` | Exact required CloudEvents `source`. |
| `sourceRegex` | Regex constraint for CloudEvents `source`. |
| `subject` | Exact required CloudEvents `subject`. |
| `dataContentType` | Required CloudEvents data content type. |
| `payloadSchema` | Domain payload schema reference. |
| `requiredExtensions` | CloudEvents extension names that must be present. |
| `validationMode` | `strict`, `compatible`, `permissive`, or `disabled`. |

Use stable, versioned event type names such as `student.enrolled.v1`. Treat a
breaking payload change as a new event type version.

## Event Flows

An `EventFlow` composes resources:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: school-file-flow
spec:
  receiverRef:
    kind: FilesystemReceiver
    name: school-events-in
  contractRefs:
    - kind: EventContract
      name: attendance-submitted
  emitterRefs:
    - kind: FilesystemEmitter
      name: school-events-out
```

An event flow must have exactly one `receiverRef` or `observerRef`. It must have
at least one emitter. Contracts are optional but recommended; without contracts,
there is no declarative domain boundary for accepted event types.

Flow startup validates:

- every referenced resource exists,
- every reference points to a compatible capability,
- resource identities are unique,
- dependency cycles are absent,
- every spec matches its registered schema surface.

## Invalid Event Routing

Use `invalidEventRef` to send invalid events to a separate emitter when a flow
handles observation-driven events:

```yaml
invalidEventRef:
  kind: FilesystemEmitter
  name: rejected-events
```

The invalid-event target must be an emitter. This is useful for quarantine
files, dead-letter topics, or audit sinks.

## Practical Guide: Add A New Contract

1. Pick a versioned event type, for example `grade.recorded.v1`.
2. Write or reference a JSON Schema for the event data payload.
3. Add an `EventContract` resource:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: grade-recorded
spec:
  type: grade.recorded.v1
  dataContentType: application/json
  payloadSchema: ./contracts/events/payloads/grade-recorded.v1.schema.json
```

4. Add the contract to an `EventFlow.contractRefs` list.
5. Run validation:

```bash
go run ./cmd/eventflow validate --config eventflow.yaml
```

## Practical Guide: Define A Filesystem Flow

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemReceiver
metadata:
  name: input
spec:
  path: ./events.ndjson
  format: ndjson
---
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemEmitter
metadata:
  name: output
spec:
  path: ./accepted.ndjson
  format: ndjson
---
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: student-enrolled
spec:
  type: student.enrolled.v1
---
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: local-validation
spec:
  receiverRef:
    kind: FilesystemReceiver
    name: input
  contractRefs:
    - kind: EventContract
      name: student-enrolled
  emitterRefs:
    - kind: FilesystemEmitter
      name: output
```

Run it:

```bash
go run ./cmd/eventflow validate --config eventflow.yaml
go run ./cmd/eventflow inspect --config eventflow.yaml
go run ./cmd/eventflow run --config eventflow.yaml
```

## Practical Guide: Define A Redpanda Flow

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: RedpandaReceiver
metadata:
  name: input-topic
spec:
  brokers:
    - localhost:19092
  topic: school.events.v1
  groupId: school-eventflow
---
apiVersion: eventflow.dev/v1alpha1
kind: RedpandaEmitter
metadata:
  name: output-topic
spec:
  brokers:
    - localhost:19092
  topic: school.accepted.v1
---
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: redpanda-routing
spec:
  receiverRef:
    kind: RedpandaReceiver
    name: input-topic
  emitterRefs:
    - kind: RedpandaEmitter
      name: output-topic
```

Create topics outside Eventflow. Eventflow adapters connect to existing backing
services; they do not provision infrastructure.

## Practical Guide: Build A New Adapter Resource

1. Define a public adapter config and constructor.
2. Define a resource spec struct in the adapter package.
3. Implement `Register(catalog *resource.Catalog) error`.
4. Declare capabilities honestly.
5. Validate semantic requirements in the definition.
6. Build the existing adapter in the definition `Build` function.

Minimal shape:

```go
type MyEmitterSpec struct {
	URL string `yaml:"url" json:"url"`
}

func Register(catalog *resource.Catalog) error {
	return resource.Register(catalog, resource.Definition[MyEmitterSpec]{
		GVK: resource.GVK("MyEmitter"),
		Validate: func(_ context.Context, spec MyEmitterSpec) error {
			if spec.URL == "" {
				return fmt.Errorf("url is required")
			}
			return nil
		},
		Build: func(context.Context, resource.BuildContext, MyEmitterSpec) (any, error) {
			return NewEmitter(...), nil
		},
		Capabilities: []resource.Capability{
			resource.CapabilityComponent,
			resource.CapabilityEmitter,
		},
	})
}
```

Do not register from `init()`. Let the caller decide which resource kinds are
available.
