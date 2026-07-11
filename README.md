# Eventflow

Eventflow is a Go SDK and declarative runtime for CloudEvents pipelines. It
loads `eventflow.dev/v1alpha1` YAML resources, validates their graph, compiles
them into small Go interfaces, and runs receiver-based event flows or
observer-based platform activity flows.

Module path:

```go
github.com/rezarajan/eventflow
```

Eventflow does not provide a control plane, API server, reconciler, provisioning
system, secrets manager, or global adapter registry. Applications and commands
explicitly register the resource kinds they support.

## Repository Contents

| Path | Purpose |
| --- | --- |
| `eventflow.go` | Public SDK ports: `Emitter`, `Receiver`, `Observer`, `ObservationMapper`, `Validator`, `Codec`, and runtimes. |
| `resource/` | Declarative resource loader, validator, graph builder, and compiler. |
| `filesystem/` | Filesystem emitter and receiver resources. |
| `httpflow/` | HTTP emitter and handler component. |
| `redpanda/` | Redpanda/Kafka emitter and receiver resources. |
| `s3/` | S3-compatible emitter and notification observer types. |
| `duckdb/` | DuckDB emitter for Eventflow-owned raw event storage. |
| `lineage/` | Public OpenLineage helpers and resource wrapper. |
| `adapters/bundled/` | Convenience registration for bundled resource kinds. |
| `cmd/eventflow/` | Declarative CLI: `validate`, `inspect`, and `run`. |
| `cmd/eventflow-*` | Small utility commands for emit, receive, relay, and lineage replay. |
| `examples/school/` | Example resource config, payload schemas, and SQL DDL. |
| `CONCEPTS.md` | Core concepts and practical authoring guide. |
| `docs/extending.md` | SDK guide for adding contracts, flows, adapters, observers, mappers, validators, and codecs. |

## Resource Model

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

Core kinds:

| Kind | Purpose |
| --- | --- |
| `EventContract` | Declares accepted CloudEvents type/envelope rules and optional payload schema references. |
| `EventFlow` | Links one receiver, contracts, emitters, and optional invalid-event routing. |
| `ObservationFlow` | Links one observer, one mapper, contracts, emitters, and optional invalid-event routing. |

Bundled adapter kinds:

| Kind | Notes |
| --- | --- |
| `FilesystemEmitter` | Emits to stdout, NDJSON, or one JSON file per event. |
| `FilesystemReceiver` | Receives from stdin, NDJSON, or one JSON file per event. |
| `HTTPEmitter` | Emits CloudEvents to an HTTP endpoint. |
| `HTTPReceiver` | Builds an HTTP handler component for embedding in an app. |
| `RedpandaEmitter` | Emits to an existing Redpanda/Kafka topic. |
| `RedpandaReceiver` | Receives from an existing Redpanda/Kafka topic. |
| `S3Emitter` | Emits to S3-compatible storage with an injected client. |
| `S3NotificationFileSource` | Reads local/test S3 notification JSON lines. |
| `S3NotificationObserver` | Converts S3 object notifications into observations. |
| `S3ObjectCreatedMapper` | Maps S3 observations into CloudEvents for `ObservationFlow`. |
| `DuckDBEmitter` | Writes events into Eventflow-owned DuckDB raw tables. |
| `DuckDBReceiver` | Registered placeholder; receiver behavior is not implemented. |
| `OpenLineageEmitter` | Wraps another emitter for OpenLineage CloudEvents. |

The compiler rejects unknown envelope fields, unknown spec fields, duplicate
resource identities, missing references, dependency cycles, and capability
mismatches.

## Quickstart: Filesystem Flow

Create a resource file:

```bash
cat > /tmp/eventflow-files.yaml <<'YAML'
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemReceiver
metadata:
  name: local-input
spec:
  path: /tmp/eventflow-input.ndjson
  format: ndjson
---
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemEmitter
metadata:
  name: local-output
spec:
  path: /tmp/eventflow-output.ndjson
  format: ndjson
---
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: example-created
spec:
  type: example.created.v1
---
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: local-copy
spec:
  receiverRef:
    kind: FilesystemReceiver
    name: local-input
  contractRefs:
    - kind: EventContract
      name: example-created
  emitterRefs:
    - kind: FilesystemEmitter
      name: local-output
YAML
```

Create one structured CloudEvent:

```bash
cat > /tmp/eventflow-input.ndjson <<'JSON'
{"specversion":"1.0","id":"quickstart-1","type":"example.created.v1","source":"urn:eventflow:quickstart","datacontenttype":"application/json","data":{"message":"hello"}}
JSON
```

Validate, inspect, and run:

```bash
go run ./cmd/eventflow validate --config /tmp/eventflow-files.yaml
go run ./cmd/eventflow inspect --config /tmp/eventflow-files.yaml
go run ./cmd/eventflow run --config /tmp/eventflow-files.yaml
cat /tmp/eventflow-output.ndjson
```

## School Example

The school example is now a declarative resource file:

```bash
go run ./cmd/eventflow validate --config examples/school/eventflow.yaml
go run ./cmd/eventflow inspect --config examples/school/eventflow.yaml
```

It includes filesystem resources, `EventContract` definitions, payload schema
references, and one `EventFlow`.

## Redpanda Example

Start local Redpanda:

```bash
just up
just topic school.events.v1
```

Use `RedpandaReceiver` and `RedpandaEmitter` resources with explicit broker and
topic settings. Eventflow connects to existing topics; it does not create or
manage broker infrastructure at runtime.

## S3 Notification Example

S3 notifications are observations first, then mapped to CloudEvents. A local
notification source can be used for development and tests:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: S3NotificationFileSource
metadata:
  name: upload-notifications
spec:
  path: ./notifications.ndjson
---
apiVersion: eventflow.dev/v1alpha1
kind: S3NotificationObserver
metadata:
  name: upload-observer
spec:
  bucket: school-uploads
  prefix: incoming/
  sourceRef:
    kind: S3NotificationFileSource
    name: upload-notifications
---
apiVersion: eventflow.dev/v1alpha1
kind: S3ObjectCreatedMapper
metadata:
  name: upload-event-mapper
spec:
  type: document.upload.detected.v1
  subjectTemplate: s3://{{bucket}}/{{key}}
  data:
    includeBucket: true
    includeKey: true
---
apiVersion: eventflow.dev/v1alpha1
kind: ObservationFlow
metadata:
  name: uploads-to-events
spec:
  observerRef:
    kind: S3NotificationObserver
    name: upload-observer
  mapperRef:
    kind: S3ObjectCreatedMapper
    name: upload-event-mapper
  emitterRefs:
    - kind: FilesystemEmitter
      name: accepted-uploads
```

See [CONCEPTS.md](CONCEPTS.md) for the full example with contract and emitter
resources.

## Developer Guides

- [CONCEPTS.md](CONCEPTS.md): core model, manifests, observers, and practical flow authoring.
- [docs/extending.md](docs/extending.md): SDK extension guide for every supported kind of component.
- [docs/adapters.md](docs/adapters.md): bundled adapter resource reference.
- [docs/validation.md](docs/validation.md): validation modes and compiler checks.

## SDK Usage

Direct composition:

```go
type emitHandler struct {
	Emitter eventflow.Emitter
}

func (h emitHandler) Handle(ctx context.Context, event eventflow.Event) error {
	return h.Emitter.Emit(ctx, event)
}

receiver := filesystem.NewReceiver(filesystem.Config{Path: "in.ndjson"})
emitter := filesystem.NewEmitter(filesystem.Config{Path: "out.ndjson"})

runtime := eventflow.Runtime{
	Receiver: receiver,
	Handler:  emitHandler{Emitter: emitter},
}
```

Declarative composition:

```go
catalog := resource.NewCatalog()
_ = filesystem.Register(catalog)
_ = redpanda.Register(catalog)

docs, err := resource.LoadFiles("eventflow.yaml")
compiled, err := resource.Compile(context.Background(), catalog, docs)
_ = compiled
_ = err
```

There is no global catalog and no hidden adapter registration.

## Commands

Primary declarative command:

```bash
go run ./cmd/eventflow validate --config eventflow.yaml
go run ./cmd/eventflow inspect --config eventflow.yaml
go run ./cmd/eventflow run --config eventflow.yaml
```

Utility commands:

| Command | Purpose |
| --- | --- |
| `cmd/eventflow-emit` | Read one structured CloudEvent from stdin and emit it to filesystem or HTTP. |
| `cmd/eventflow-receive` | Read structured CloudEvents from filesystem/stdin and write them to stdout. |
| `cmd/eventflow-relay` | Relay events between filesystem paths. |
| `cmd/eventflow-lineage-replay` | Replay OpenLineage NDJSON to `noop`, file, or Marquez lineage output. |

Run any command with `-help` for flags.

## Lineage

The public `lineage` package builds OpenLineage run events and wraps them as
CloudEvents. The replay command can send OpenLineage NDJSON to Marquez:

```bash
just up-marquez

EVENTFLOW_LINEAGE_OUTPUT=marquez \
EVENTFLOW_MARQUEZ_URL=http://localhost:5000 \
go run ./cmd/eventflow-lineage-replay \
  --file var/eventflow/lineage/openlineage.ndjson
```

The Compose file exposes Marquez UI at `http://localhost:3000`.

## Learn More

Read [CONCEPTS.md](CONCEPTS.md) for the component model, reference semantics,
contract authoring, EventFlow authoring, and adapter resource guidance.

## Development

```bash
go test ./...
go test -race ./...
go vet ./...
```

If the default Go cache is not writable:

```bash
GOCACHE=/tmp/eventflow-go-build-cache go test ./...
```
