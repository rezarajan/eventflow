# Contracts

The public registry package is `github.com/rezarajan/eventflow/contract`.
New contracts use `eventflow.registry.v2`.

```yaml
version: eventflow.registry.v2
events:
  - kind: attendance.submitted.v1
    payload_schema: ./contracts/events/payloads/attendance-submitted.v1.schema.json
    channel:
      name: attendance.events.v1
      protocol: redpanda
      topic: attendance.events.v1
    validation:
      mode: strict
    required_extensions:
      - correlationid
    projection:
      table: attendance_submitted
```

The loader validates duplicate event types, unsupported validation modes,
unsupported codecs, invalid regexes, missing required fields, and unresolved
local schema files during validation. `eventflow.registry.v1` remains accepted
as a migration input and is normalized to v2.

