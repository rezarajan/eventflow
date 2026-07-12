# ADR 0006: Dapr Is Optional

## Status

Accepted.

## Context

Dapr can be useful for service invocation and Pub/Sub but is not required for event governance.

## Decision

Document Dapr as optional and complementary. Do not add a Dapr SDK dependency.

## Consequences

Eventflow remains focused on contracts, journal, quarantine, dispatch and replay.

## Rejected Alternatives

- Dapr sidecar requirement.
- Dapr replacement behavior.
