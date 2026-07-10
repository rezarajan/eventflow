# Validation

Eventflow validation runs in this order:

1. Decode transport representation.
2. Validate CloudEvents or OpenLineage syntax.
3. Resolve the registry entry.
4. Validate envelope attributes and required extensions.
5. Validate JSON payload schema.
6. Validate OpenLineage semantics for `io.openlineage.run-event.v1`.
7. Apply custom validators.
8. Normalize and dispatch or emit.

`strict` is the default mode. `compatible`, `permissive`, and `disabled` must be
selected explicitly by SDK option, command flag, environment config, or
per-event registry policy.

