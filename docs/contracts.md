# Contracts

`EventContract` defines the CloudEvents type and optional constraints an event must satisfy before it can be journaled.

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: openlineage-run-event
spec:
  type: io.openlineage.run-event.v1
  dataContentType: application/json
  dataSchema: ./schemas/openlineage-run-event.json
```

## Envelope Rules

Eventflow validates CloudEvents using the CloudEvents SDK. Contracts may additionally constrain:

- `type`
- `source`
- `sourceRegex`
- `subject`
- `dataContentType`
- required extension attributes

## Payload Schema

Use `dataSchema` for JSON Schema payload validation. `payloadSchema` remains a deprecated compatibility alias.

Schema files are loaded locally at validation time. Eventflow does not fetch schemas from a registry.

## Versioning

Use versioned event types such as `example.created.v1` or profile-specific types such as `io.openlineage.run-event.v1`. Additive compatible changes should keep the same type only when existing consumers remain valid.

## Invalid Events

Invalid events are rejected. If `invalidEmitterRef` is configured, invalid events can be routed to quarantine. HTTP ingress returns non-2xx for invalid producer input unless `invalidPolicy: acceptAndQuarantine` is explicitly configured.
