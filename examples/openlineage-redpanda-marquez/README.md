# OpenLineage Redpanda Example

This example demonstrates Eventflow as an OpenLineage admission and quarantine gateway:

1. authorized valid OpenLineage is accepted and published to Redpanda;
2. an unauthorized job namespace is rejected with `EF1202_JOB_NAMESPACE_NOT_ALLOWED`;
3. malformed OpenLineage is rejected with `EF1101_OPENLINEAGE_SCHEMA_INVALID`;
4. rejected events are retained in quarantine;
5. an operator validates and replays a corrected record.

Run:

```bash
just demo
```

The Kafka-to-Marquez worker configuration in `worker.yaml` shows the downstream delivery mode where Kafka remains the durable source of truth and Eventflow commits offsets after Marquez delivery.
