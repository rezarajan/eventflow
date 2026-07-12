# Operations

## Health And Readiness

HTTP gateway mode exposes:

- `/healthz`
- `/readyz`
- `/metrics`

Readiness means the process can accept requests. Destination health is reflected through dispatch metrics and journal state.

## Metrics

Eventflow exposes Prometheus text metrics with bounded labels:

- `eventflow_events_received_total`
- `eventflow_events_accepted_total`
- `eventflow_events_rejected_total`
- `eventflow_events_quarantined_total`
- `eventflow_journal_append_failures_total`
- `eventflow_dispatch_attempts_total`
- `eventflow_dispatch_failures_total`
- `eventflow_dispatch_delivered_total`
- `eventflow_runtime_info`

Do not add event ID, run ID, dataset name, error text or arbitrary event type as labels.

## Logging

Structured logs should include flow, event ID, event type, source, contract, receiver, emitter, journal record ID, attempt, delivery status and error class. Full event payload logging is disabled by default.

## Capacity

Size SQLite storage for the accepted-event journal plus retained delivery state. Bound HTTP request size, Kafka batch size, dispatcher concurrency and retry delays.

## Backup And Recovery

Back up the SQLite database file while the gateway is stopped or using a SQLite-safe backup mechanism. After restore, pending and failed delivery rows can be replayed or retried.

## Incident Replay

Use `eventflow replay --dry-run` first, then replay to a named destination. Replayed records preserve CloudEvents identity and can create duplicates downstream.
