# ADR 0001: Product Boundary

## Status

Accepted.

## Context

The repository previously described a broader SDK and declarative runtime.

## Decision

Eventflow is a schema-enforcing CloudEvents gateway for validation, normalization, durable journaling, quarantine, dispatch and replay.

## Consequences

Docs, examples and commands prioritize gateway use cases. Workflow, stream processing, cataloging and infrastructure provisioning remain out of scope.

## Rejected Alternatives

- General integration platform.
- Dapr replacement.
- Kubernetes control plane.
