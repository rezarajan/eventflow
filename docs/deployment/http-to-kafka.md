# HTTP To Kafka

HTTP-to-Kafka mode accepts OpenLineage HTTP input, validates CloudEvents and OpenLineage policy, then publishes to Redpanda or Kafka.

Acknowledgement boundary:

1. Decode request.
2. Resolve contract and policy.
3. Validate CloudEvents, OpenLineage payload, and typed policy.
4. Publish to the broker.
5. Return success only after broker acknowledgement.

Kafka or Redpanda is the durable source of truth. The default broker outage behavior is `brokerUnavailablePolicy: reject`, which rejects and quarantines with `EF1501_BROKER_UNAVAILABLE`.

Optional local spooling requires:

```yaml
brokerUnavailablePolicy: spool
spoolRef:
  kind: SQLiteSpool
  name: local-spool
```

SQLite spooling is single-instance and not highly available.
