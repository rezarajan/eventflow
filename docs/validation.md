# Validation

Eventflow validates declarative resources before runtime startup:

1. Load one or more `eventflow.dev/v1alpha1` YAML documents.
2. Validate the resource envelope and strict kind-specific spec fields.
3. Decode and apply resource defaults.
4. Run semantic validation for each resource definition.
5. Build the dependency graph.
6. Reject duplicate identities, unknown kinds, missing references, dependency
   cycles, and capability mismatches.
7. Compile resources into `EventFlow` and `ObservationFlow` runtimes.

At runtime, CloudEvents validation is contract-driven:

1. Validate CloudEvents syntax.
2. Match the event type against linked `EventContract` resources.
3. Validate envelope constraints such as source, subject, content type, and
   required extensions.
4. Validate payload presence when a payload schema reference is declared.
5. Dispatch accepted events to configured emitters.

`strict` is the default mode. `compatible`, `permissive`, and `disabled` are
explicit resource or SDK choices.
