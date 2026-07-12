# ADR 0002: At-Least-Once Delivery

## Status

Accepted.

## Context

Producers need acknowledgement without depending on destination availability.

## Decision

Eventflow guarantees at-least-once delivery after durable journal append. HTTP acknowledgement and Kafka offset commit happen after validation and journal append.

## Consequences

Duplicates are possible. Sinks must be idempotent.

## Rejected Alternatives

- Exactly-once delivery.
- Acknowledgement before journaling.
