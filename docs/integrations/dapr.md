# Dapr Integration

Dapr is optional and complementary.

Dapr provides service invocation, application connectivity, mTLS, service discovery and Pub/Sub abstractions. Eventflow provides contract validation, durable journaling, quarantine and replay.

## Pattern A: Dapr Service Invocation

```text
Dagster
  -> local Dapr sidecar
  -> Dapr service invocation
  -> Eventflow HTTP endpoint
```

Eventflow still exposes its application HTTP route. Dapr forwards requests to that route.

## Pattern B: Dapr Pub/Sub

```text
producer
  -> Dapr Pub/Sub
  -> Eventflow subscriber endpoint
  -> validation and journal
  -> downstream destination
```

If Dapr Pub/Sub already provides broker delivery and retries, Eventflow should not duplicate that merely for transport abstraction. Eventflow remains justified when a governed contract boundary, durable event journal, quarantine or operator-controlled replay is required.

For a simple direct Dagster-to-Eventflow deployment, Dapr is unnecessary.

No Dapr SDK dependency is required for these patterns; use HTTP and Dapr component configuration.
