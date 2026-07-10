# Eventflow

Eventflow is a standalone Go SDK plus thin runtime commands for standards-based event ingress, fan-out, consumption, local projection, and lineage emission.

Domain teams bring their own event registry and JSON Schemas. Eventflow validates producer payloads, wraps them as CloudEvents, publishes them to configured channels, and emits OpenLineage metadata separately. The public module path is `github.com/rezarajan/project-datascape`.

## Principles

- Configuration comes from environment variables or command flags.
- Domain contracts are external registry files, not compiled defaults.
- Commands are small and composable.
- Public packages are importable and transport-neutral.
- Logs go to stderr; data goes to stdout where a command streams data.
- Backing services such as Redpanda, DuckDB, and Marquez are attached by config.

## SDK Packages

The root package defines the public ports: `Emitter`, `BatchEmitter`,
`Receiver`, `BatchReceiver`, `Observer`, `EventHandler`, `Validator`, `Codec`,
and `Runtime`.

Importable adapters and helpers:

| Package | Purpose |
| --- | --- |
| `contract` | `eventflow.registry.v2` loader, v1 migration, registry-backed validator. |
| `cloudevent` | Structured JSON codec and HTTP binary binding helpers. |
| `lineage` | OpenLineage run events, lifecycle helpers, CloudEvents wrapping. |
| `filesystem` | NDJSON, stdin/stdout, one-event-per-file, atomic writes, commit markers. |
| `httpflow` | HTTP emitter and receiver for CloudEvents and native OpenLineage endpoints. |
| `redpanda` | Importable Redpanda/Kafka emitter and receiver. |
| `s3` | S3-compatible emitter and notification observer using injected clients. |
| `duckdb` | Eventflow-owned raw table and registry-driven projections. |
| `adaptertest` | Reusable conformance tests for adapter authors. |

## Event Registry

Every runtime path that needs domain knowledge reads `EVENTFLOW_REGISTRY`.

```yaml
version: eventflow.registry.v2
events:
  - kind: example.created.v1
    payload_schema: ./schemas/example-created.v1.schema.json
    channel:
      name: example.events.v1
      protocol: redpanda
      topic: example.events.v1
    projection:
      table: examples
```

`payload_schema` points to a JSON Schema document. `channel` is the canonical
broker topic/channel. `projection.table` is optional; DuckDB always writes
`_raw_events` and creates typed tables only when this field is present. Existing
`eventflow.registry.v1` files are accepted as migration input.

## Quickstart

Validate the example registry:

```bash
go run ./cmd/eventflow-registry validate --registry examples/school/eventflow.yaml
```

Start local infrastructure:

```bash
just up
```

Create a topic for the example event:

```bash
docker compose exec redpanda rpk topic create attendance.events.v1 -X brokers=localhost:9092 --partitions 3 --replicas 1
```

Start HTTP ingress:

```bash
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
EVENTFLOW_REDPANDA_TOPIC_MODE=registry \
go run ./cmd/eventflow-ingress-http
```

Publish a plain JSON payload:

```bash
curl -i -X POST http://localhost:8080/v1/events/attendance.submitted.v1 \
  -H 'content-type: application/json' \
  -H 'X-Eventflow-Subject: student-22222222' \
  -H 'X-Correlation-ID: quickstart-1' \
  -d '{
    "attendance_id": "11111111-1111-1111-1111-111111111111",
    "student_id": "22222222-2222-2222-2222-222222222222",
    "class_id": "33333333-3333-3333-3333-333333333333",
    "school_id": "44444444-4444-4444-4444-444444444444",
    "attendance_date": "2026-07-09",
    "status_code": "PRESENT",
    "submitted_at": "2026-07-09T01:00:00Z"
  }'
```

Consume into DuckDB:

```bash
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
EVENTFLOW_REDPANDA_TOPIC=attendance.events.v1 \
EVENTFLOW_CONSUME_HANDLERS=duckdb \
EVENTFLOW_CONSUME_MAX_EVENTS=1 \
go run ./cmd/eventflow-consume
```

Stop infrastructure:

```bash
just down
```

## AsyncAPI

Render an AsyncAPI 3.1 document from any registry:

```bash
go run ./cmd/eventflow-registry asyncapi \
  --registry examples/school/eventflow.yaml \
  --output examples/school/contracts/asyncapi/asyncapi.yaml
```

Use `--output -` or omit `--output` to write to stdout.

## Lineage And Marquez

Commands emit OpenLineage independently from event publication. Set
`EVENTFLOW_LINEAGE_OUTPUT=file` while running ingress, fanout, consume, or
generate to keep an append-only local lineage file:

```bash
EVENTFLOW_LINEAGE_OUTPUT=file \
EVENTFLOW_LINEAGE_FILE=var/eventflow/lineage/openlineage.ndjson \
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
go run ./cmd/eventflow-ingress-http
```

Start Marquez locally:

```bash
just up-marquez
```

Replay the local OpenLineage file into Marquez:

```bash
EVENTFLOW_LINEAGE_OUTPUT=marquez \
EVENTFLOW_MARQUEZ_URL=http://localhost:5000 \
go run ./cmd/eventflow-lineage-replay \
  --file var/eventflow/lineage/openlineage.ndjson
```

The same Marquez settings can be used directly on runtime commands to post
lineage as they run:

```bash
EVENTFLOW_LINEAGE_OUTPUT=marquez \
EVENTFLOW_MARQUEZ_URL=http://localhost:5000 \
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
go run ./cmd/eventflow-ingress-http
```

Marquez UI is exposed at `http://localhost:3000` by the provided Compose file.

## Commands

Each command has a detailed `-help` page. The examples below use `go run`, but
the same flags apply to installed binaries.

### `eventflow-emit`, `eventflow-receive`, `eventflow-relay`

These thin commands stream structured CloudEvents through the public SDK:

```bash
go run ./cmd/eventflow-receive --path events.ndjson --max-events 10
go run ./cmd/eventflow-emit --adapter http --url http://localhost:8080/events < event.json
go run ./cmd/eventflow-relay --in events.ndjson --out copied.ndjson
```

### `eventflow-registry`

Validate registry structure and local schema file references:

```bash
go run ./cmd/eventflow-registry validate --registry examples/school/eventflow.yaml
```

Emit AsyncAPI:

```bash
go run ./cmd/eventflow-registry asyncapi \
  --registry examples/school/eventflow.yaml \
  --output -
```

Flags:

| Flag | Environment | Default | Purpose |
| --- | --- | --- | --- |
| `--registry` | `EVENTFLOW_REGISTRY` | none | Registry YAML path. |
| `--output` | none | `-` | AsyncAPI output path for the `asyncapi` command. |

### `eventflow-ingress-http`

Accept producer JSON over HTTP, validate it against the registered schema, wrap
it as a CloudEvent, and publish it to Redpanda/Kafka:

```bash
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
EVENTFLOW_REDPANDA_TOPIC_MODE=registry \
go run ./cmd/eventflow-ingress-http --addr :8080
```

Producer endpoint:

```text
POST /v1/events/{event_type}
Content-Type: application/json
X-Eventflow-Subject: optional subject
X-Correlation-ID: optional correlation id
```

Flags:

| Flag | Environment | Default | Purpose |
| --- | --- | --- | --- |
| `--addr` | `EVENTFLOW_INGRESS_HTTP_ADDR` | `:8080` | HTTP listen address. |
| `--registry` | `EVENTFLOW_REGISTRY` | none | Registry YAML path. |
| `--max-body` | `EVENTFLOW_INGRESS_MAX_BODY` | `1048576` | Maximum request body size in bytes. |

### `eventflow-fanout`

Read CloudEvents JSON Lines from stdin and publish them to one or more outputs:

```bash
EVENTFLOW_OUTPUTS=redpanda \
EVENTFLOW_REDPANDA_TOPIC_MODE=registry \
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
go run ./cmd/eventflow-fanout < events.ndjson
```

Flags:

| Flag | Environment | Default | Purpose |
| --- | --- | --- | --- |
| `--outputs` | `EVENTFLOW_OUTPUTS` | `log` | Comma-separated outputs: `log`, `stdout`, `discard`, `redpanda`. |
| `--run-id` | `EVENTFLOW_RUN_ID` | generated | Lineage run id. |
| `--batch-size` | `EVENTFLOW_FANOUT_BATCH_SIZE` | `100` | Batch size for batch-capable outputs. |

### `eventflow-consume`

Consume CloudEvents and apply handlers:

```bash
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
EVENTFLOW_REDPANDA_TOPIC=attendance.events.v1 \
EVENTFLOW_CONSUME_HANDLERS=duckdb \
go run ./cmd/eventflow-consume --max-events 1
```

Flags:

| Flag | Environment | Default | Purpose |
| --- | --- | --- | --- |
| `--source` | `EVENTFLOW_CONSUME_SOURCE` | `redpanda` | Source adapter. |
| `--handlers` | `EVENTFLOW_CONSUME_HANDLERS` | `jsonl` | Comma-separated handlers: `jsonl`, `objects`, `duckdb`. |
| `--run-id` | `EVENTFLOW_RUN_ID` | generated | Lineage run id. |
| `--batch-size` | `EVENTFLOW_CONSUME_BATCH_SIZE` | `100` | Events read and handled per batch. |
| `--max-events` | `EVENTFLOW_CONSUME_MAX_EVENTS` | `0` | Maximum events before exit; `0` means unbounded. |

### `eventflow-lineage-replay`

Replay OpenLineage NDJSON to the configured lineage backend:

```bash
EVENTFLOW_LINEAGE_OUTPUT=marquez \
EVENTFLOW_MARQUEZ_URL=http://localhost:5000 \
go run ./cmd/eventflow-lineage-replay \
  --file var/eventflow/lineage/openlineage.ndjson
```

Flags:

| Flag | Environment | Default | Purpose |
| --- | --- | --- | --- |
| `--file` | `EVENTFLOW_LINEAGE_FILE` | `var/eventflow/lineage/openlineage.ndjson` | OpenLineage NDJSON file to replay. |
| `--limit` | `EVENTFLOW_LINEAGE_REPLAY_LIMIT` | `0` | Maximum events to replay; `0` means all. |

### `eventflow-generate`

Stream CloudEvents JSON Lines from a registered generator. The core runtime does
not compile in domain generators by default; downstream examples or services
should register their own generators.

```bash
go run ./cmd/eventflow-generate --generator domain-generator --param count=10
```

Flags:

| Flag | Environment | Default | Purpose |
| --- | --- | --- | --- |
| `--generator` | `EVENTFLOW_GENERATOR` | none | Registered generator name. |
| `--run-id` | `EVENTFLOW_RUN_ID` | generated | Lineage run id. |
| `--seed` | `EVENTFLOW_SEED` | `42` | Deterministic generator seed. |
| `--source` | `EVENTFLOW_EVENT_SOURCE` | `urn:eventflow:generate` | CloudEvents source. |
| `--param` | none | none | Generator parameter as `key=value`; repeatable. |

## Configuration

Preferred environment variables use the `EVENTFLOW_` prefix. Existing `DATASCAPE_` names are accepted as deprecated aliases during the transition.

```text
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml
EVENTFLOW_REDPANDA_BROKERS=localhost:19092
EVENTFLOW_REDPANDA_TOPIC=attendance.events.v1
EVENTFLOW_REDPANDA_TOPIC_MODE=registry
EVENTFLOW_CONSUME_HANDLERS=duckdb
EVENTFLOW_DUCKDB_PATH=var/eventflow/eventflow.duckdb
EVENTFLOW_LINEAGE_OUTPUT=noop
EVENTFLOW_LINEAGE_FILE=var/eventflow/lineage/openlineage.ndjson
```

## Examples

The school domain under `examples/school` is an example registration, not a runtime default. It contains payload schemas, sample DDL, a sample registry, and example commands.

## Development

```bash
just test
```

If the default Go cache is not writable in a sandbox, use:

```bash
GOCACHE=/tmp/eventflow-go-build-cache go test ./...
```
