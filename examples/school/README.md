# School Example

This example shows how a domain registers its own event schemas with Eventflow.

The core runtime does not know about these event types. They are available only when `EVENTFLOW_REGISTRY=examples/school/eventflow.yaml` is configured.

## Validate The Registry

```bash
go run ../../cmd/eventflow-registry validate --registry eventflow.yaml
```

## Emit AsyncAPI

```bash
go run ../../cmd/eventflow-registry asyncapi \
  --registry eventflow.yaml \
  --output contracts/asyncapi/asyncapi.yaml
```

## Publish Through HTTP Ingress

Start Redpanda from the repository root:

```bash
just up
```

Create the example topic:

```bash
docker compose exec redpanda rpk topic create attendance.events.v1 -X brokers=localhost:9092 --partitions 3 --replicas 1
```

Start ingress from the repository root:

```bash
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
EVENTFLOW_REDPANDA_TOPIC_MODE=registry \
EVENTFLOW_LINEAGE_OUTPUT=file \
go run ./cmd/eventflow-ingress-http
```

Publish a domain payload:

```bash
curl -i -X POST http://localhost:8080/v1/events/attendance.submitted.v1 \
  -H 'content-type: application/json' \
  -H 'X-Eventflow-Subject: student-22222222' \
  -H 'X-Correlation-ID: school-example-1' \
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

## Consume Into DuckDB

```bash
EVENTFLOW_REGISTRY=examples/school/eventflow.yaml \
EVENTFLOW_REDPANDA_TOPIC=attendance.events.v1 \
EVENTFLOW_CONSUME_HANDLERS=duckdb \
EVENTFLOW_CONSUME_MAX_EVENTS=1 \
go run ./cmd/eventflow-consume
```

## Replay Lineage To Marquez

From the repository root, start Marquez:

```bash
just up-marquez
```

Replay captured OpenLineage events:

```bash
EVENTFLOW_LINEAGE_OUTPUT=marquez \
EVENTFLOW_MARQUEZ_URL=http://localhost:5000 \
go run ./cmd/eventflow-lineage-replay \
  --file var/eventflow/lineage/openlineage.ndjson
```

Open the Marquez UI at `http://localhost:3000`.
