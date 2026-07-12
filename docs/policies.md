# Policies

`OpenLineagePolicy` is a typed, deterministic policy resource. It has no expression language, scripting engine, CEL, Rego, JavaScript, or transformation DSL.

Supported checks include allowed schema versions, allowed producers, principal-to-job namespace rules, principal-to-dataset namespace rules, required and prohibited facets, custom facet controls, dataset URI schemes, event and facet size limits, required CloudEvents extensions, tenants, environments, naming patterns, and per-principal rate limits.

Authentication is an integration boundary. Eventflow accepts a verified principal from mTLS, SPIFFE, a trusted reverse proxy header, Kubernetes workload identity, Dapr, or a service mesh. Arbitrary identity headers must not be trusted unless the request came through a configured trusted proxy or verified transport.

Stable reason codes:

- `EF1001_CLOUDEVENT_INVALID`
- `EF1002_EVENT_TYPE_UNSUPPORTED`
- `EF1101_OPENLINEAGE_SCHEMA_INVALID`
- `EF1102_OPENLINEAGE_VERSION_UNSUPPORTED`
- `EF1201_PRODUCER_NOT_ALLOWED`
- `EF1202_JOB_NAMESPACE_NOT_ALLOWED`
- `EF1203_DATASET_NAMESPACE_NOT_ALLOWED`
- `EF1301_REQUIRED_FACET_MISSING`
- `EF1302_FACET_NOT_ALLOWED`
- `EF1303_EVENT_TOO_LARGE`
- `EF1401_RATE_LIMIT_EXCEEDED`
- `EF1501_BROKER_UNAVAILABLE`
