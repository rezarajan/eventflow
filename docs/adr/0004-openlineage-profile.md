# ADR 0004: OpenLineage Profile

## Status

Accepted.

## Context

OpenLineage is the primary production scenario.

## Decision

Implement OpenLineage as a profile over the generic CloudEvents gateway, not as a separate runtime.

## Consequences

Eventflow preserves producer-supplied lineage semantics and does not reconstruct runtime meaning.

## Rejected Alternatives

- Marquez-specific runtime.
- Synthesizing missing jobs, datasets or facets.
