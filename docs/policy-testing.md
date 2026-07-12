# Policy Testing

Use `eventflow policy test` in CI to verify policy outcomes.

```bash
eventflow policy test \
  --config examples/openlineage-redpanda-marquez/ingress.yaml \
  --event examples/openlineage-redpanda-marquez/unauthorized-job-namespace.json \
  --principal spiffe://example/dagster \
  --expect-outcome REJECT \
  --expect-reason EF1202_JOB_NAMESPACE_NOT_ALLOWED
```

A failing expectation exits non-zero. This command is deterministic and does not contact brokers or sinks.

Declarative suites are supported:

```yaml
tests:
  - name: unauthorized-job-namespace
    event: examples/openlineage-redpanda-marquez/unauthorized-job-namespace.json
    principal: spiffe://example/dagster
    expectOutcome: REJECT
    expectReason: EF1202_JOB_NAMESPACE_NOT_ALLOWED
```

Run:

```bash
eventflow policy test --config examples/openlineage-redpanda-marquez/ingress.yaml --suite examples/openlineage-redpanda-marquez/policy-tests.yaml
```
