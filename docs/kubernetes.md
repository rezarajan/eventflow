# Kubernetes

Eventflow does not require or provide an operator.

## Modes

- Gateway Deployment: `eventflow run --config /etc/eventflow/resources.yaml`
- Delivery worker Deployment: `eventflow run --config /etc/eventflow/worker.yaml`
- Replay Job: `eventflow replay --config /etc/eventflow/worker.yaml --destination ...`
- Validate Job: `eventflow validate --config /etc/eventflow/resources.yaml`

## Configuration

Mount resources through ConfigMaps. Mount secrets through environment variables or files referenced by adapter configuration.

## Probes

Use `/healthz` for liveness and `/readyz` for readiness on HTTP gateway pods.

## Metrics

Scrape `/metrics` with Prometheus.

## Storage

SQLite requires a PersistentVolume for durable gateway state. SQLite mode is single-writer/single-instance; do not scale multiple replicas against the same SQLite file.

## Scaling

Scale Kafka consumers through consumer groups only when each replica has an independent durable journal or when a future distributed journal is implemented. PostgreSQL/distributed journals are future work.

## Pod Guidance

Use non-root execution, read-only root filesystem where practical, resource requests/limits, termination grace periods that exceed dispatcher drain timeout and a PodDisruptionBudget for singleton gateways.
