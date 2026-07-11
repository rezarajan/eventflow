# Contracts

Eventflow contracts are declarative `EventContract` resources. They describe
the CloudEvents type and envelope rules a flow should accept.

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: attendance-submitted
spec:
  type: attendance.submitted.v1
  sourceRegex: '^urn:school:'
  dataContentType: application/json
  payloadSchema: ./contracts/events/payloads/attendance-submitted.v1.schema.json
  requiredExtensions:
    - correlationid
  validationMode: strict
```

Contracts are linked into an `EventFlow` by reference:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: school-flow
spec:
  receiverRef:
    kind: FilesystemReceiver
    name: input
  contractRefs:
    - kind: EventContract
      name: attendance-submitted
  emitterRefs:
    - kind: FilesystemEmitter
      name: output
```

Resource validation rejects missing references, incompatible capabilities,
duplicate resource identities, unknown kinds, dependency cycles, and unknown
spec fields.
