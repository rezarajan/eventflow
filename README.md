# Eventflow

Eventflow is a standalone Go SDK and a small set of runtime commands for
CloudEvents-based event pipelines. It provides transport-neutral ports,
declarative resource loading, validation, compilation, local adapters, and
OpenLineage helpers.

## What Is In This Repo

- Public SDK ports in the root package: `Emitter`, `BatchEmitter`, `Receiver`,
  `BatchReceiver`, `Observer`, `Validator`, `Codec`, and `Runtime`.
- Declarative resource compiler in `resource` for
  `eventflow.dev/v1alpha1` YAML documents.
- Adapter packages for filesystem, HTTP, Redpanda/Kafka, S3-compatible object
  storage, DuckDB, and OpenLineage.
- A declarative CLI at `cmd/eventflow` with `validate`, `inspect`, and `run`.
- Compatibility commands for the older registry-based demo/runtime flows.
- School-domain example contracts and generator tooling under `examples/school`.

Eventflow does not run a control plane, Kubernetes reconciler, secrets manager,
or provisioning system. Callers own dependency construction and adapter
registration.

## Resource Model

Declarative resources use this envelope:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemEmitter
metadata:
  name: local-output
spec:
  path: ./events.ndjson
  format: ndjson
```

Core resource kinds:

| Kind | Purpose |
| --- | --- |
| `EventContract` | Declares CloudEvents type rules, optional payload schema reference, required extensions, and validation mode. |
| `EventFlow` | Wires one receiver or observer, contracts, emitters, and optional invalid-event routing. |

Bundled adapter resource kinds:

| Kind | Capability |
| --- | --- |
| `FilesystemEmitter` | Emits events to NDJSON, stdout, or one JSON file per event. |
| `FilesystemReceiver` | Receives events from NDJSON, stdin, or one JSON file per event. |
| `HTTPEmitter` | Emits CloudEvents to an HTTP endpoint. |
| `HTTPReceiver` | Registered as an HTTP handler component; use the HTTP ingress command for server mode. |
| `RedpandaEmitter` | Emits to Redpanda/Kafka. |
| `RedpandaReceiver` | Receives from Redpanda/Kafka. |
| `S3Emitter` | SDK resource for S3-compatible writes; requires an injected client before opening. |
| `S3NotificationObserver` | SDK observer resource; requires an injected notification channel. |
| `DuckDBEmitter` | Writes events through the DuckDB adapter. |
| `DuckDBReceiver` | Placeholder component; receiver mode is not implemented. |
| `OpenLineageEmitter` | Wraps another Eventflow emitter for OpenLineage CloudEvents. |

The compiler rejects unknown envelope fields, unknown spec fields, duplicate
resource identities, missing references, dependency cycles, and incompatible
capabilities.

## Quickstart: Local Filesystem Flow

This quickstart needs no external services.

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

Validate and inspect the config:

```bash
go run ./cmd/eventflow validate --config /tmp/eventflow-files.yaml
go run ./cmd/eventflow inspect --config /tmp/eventflow-files.yaml
```

Run the flow:

```bash
go run ./cmd/eventflow run --config /tmp/eventflow-files.yaml
cat /tmp/eventflow-output.ndjson
```

## Quickstart: Redpanda Flow

Start local services:

```bash
just up
```

Create a topic:

```bash
docker compose exec redpanda \
  rpk topic create example.events.v1 -X brokers=localhost:9092 --partitions 3 --replicas 1
```

Use Redpanda resources:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: RedpandaReceiver
metadata:
  name: input-topic
spec:
  brokers: [localhost:19092]
  topic: example.events.v1
  groupId: eventflow-quickstart
---
apiVersion: eventflow.dev/v1alpha1
kind: FilesystemEmitter
metadata:
  name: local-output
spec:
  path: /tmp/redpanda-events.ndjson
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
  name: redpanda-to-file
spec:
  receiverRef:
    kind: RedpandaReceiver
    name: input-topic
  contractRefs:
    - kind: EventContract
      name: example-created
  emitterRefs:
    - kind: FilesystemEmitter
      name: local-output
```

Validate and run it:

```bash
go run ./cmd/eventflow validate --config redpanda-flow.yaml
go run ./cmd/eventflow run --config redpanda-flow.yaml
```

Stop local services when done:

```bash
just down
```

## SDK Usage

Use the root package when you want direct Go composition instead of YAML:

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

For declarative composition, create a catalog and explicitly register adapters:

```go
catalog := resource.NewCatalog()
_ = filesystem.Register(catalog)
_ = redpanda.Register(catalog)

docs, err := resource.LoadFiles("eventflow.yaml")
compiled, err := resource.Compile(context.Background(), catalog, docs)
_ = compiled
_ = err
```

There is no global registry and no `init()` adapter registration.

## Commands

The preferred declarative command is:

```bash
go run ./cmd/eventflow validate --config eventflow.yaml
go run ./cmd/eventflow inspect --config eventflow.yaml
go run ./cmd/eventflow run --config eventflow.yaml
```

Compatibility and utility commands are still present:

| Command | Purpose |
| --- | --- |
| `cmd/eventflow-emit` | Emit one structured CloudEvent to filesystem or HTTP. |
| `cmd/eventflow-receive` | Read structured CloudEvents from filesystem/stdin. |
| `cmd/eventflow-relay` | Copy events from one filesystem source to another. |
| `cmd/eventflow-ingress-http` | Legacy registry-based HTTP ingress to Redpanda/Kafka. |
| `cmd/eventflow-fanout` | Legacy stdin fan-out to log/stdout/discard/Redpanda outputs. |
| `cmd/eventflow-consume` | Legacy Redpanda consumer with jsonl/object/DuckDB handlers. |
| `cmd/eventflow-registry` | Validate old `eventflow.registry.v2` files and render AsyncAPI. |
| `cmd/eventflow-lineage-replay` | Replay OpenLineage NDJSON to the configured lineage backend. |

Run any command with `-help` for its flags.

## Legacy Registry Compatibility

The old registry format remains for compatibility commands and the school demo:

```yaml
version: eventflow.registry.v2
events:
  - kind: example.created.v1
    payload_schema: ./schemas/example-created.v1.schema.json
    channel:
      name: example.events.v1
      protocol: redpanda
      topic: example.events.v1
```

Validate a legacy registry:

```bash
go run ./cmd/eventflow-registry validate --registry examples/school/eventflow.yaml
```

Render AsyncAPI:

```bash
go run ./cmd/eventflow-registry asyncapi \
  --registry examples/school/eventflow.yaml \
  --output -
```

New runtime configuration should prefer `eventflow.dev/v1alpha1` resources.

## Lineage

The public `lineage` package builds OpenLineage run events and can wrap them as
CloudEvents. Legacy runtime commands can also write or replay OpenLineage NDJSON.

Write lineage to a local file while running a legacy command:

```bash
EVENTFLOW_LINEAGE_OUTPUT=file \
EVENTFLOW_LINEAGE_FILE=var/eventflow/lineage/openlineage.ndjson \
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
go run ./cmd/eventflow-ingress-http
```

Replay local lineage to Marquez:

```bash
just up-marquez

EVENTFLOW_LINEAGE_OUTPUT=marquez \
EVENTFLOW_MARQUEZ_URL=http://localhost:5000 \
go run ./cmd/eventflow-lineage-replay \
  --file var/eventflow/lineage/openlineage.ndjson
```

The provided Compose file exposes Marquez UI at `http://localhost:3000`.

## Examples

`examples/school` contains sample event contracts, payload schemas, SQL DDL,
and local demo tooling. Generator code is example/test tooling, not a public
SDK or runtime capability.

## Development

Run tests:

```bash
go test ./...
```

Run the broader verification used for this repo:

```bash
go test -race ./...
go vet ./...
staticcheck ./...
```

If the default Go cache is not writable in a sandbox, set a writable cache:

```bash
GOCACHE=/tmp/eventflow-go-build-cache go test ./...
```
