# Adapters

Public adapters are importable packages:

| Package | Capability |
| --- | --- |
| `filesystem` | Structured CloudEvents NDJSON, one-event-per-file, stdin/stdout, atomic file writes, commit markers, deduplication hooks. |
| `httpflow` | HTTP emitter and receiver for structured CloudEvents, binary CloudEvents, and native OpenLineage endpoint posts. |
| `redpanda` | Redpanda/Kafka emitter and receiver using existing `kafka-go` internals. |
| `s3` | S3-compatible emitter, local notification source, notification observer, and object-created mapper. |
| `duckdb` | Eventflow-owned append-only raw event table. |

Adapters do not create brokers, buckets, credentials, identities, or governance
resources.
