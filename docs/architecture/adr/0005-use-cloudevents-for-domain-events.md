# ADR: Use CloudEvents for Domain Events

## Status

Accepted for standards-only increment.

## Context

Future event bus integration requires a stable event envelope for domain events such as AttendanceSubmitted and ExamPaperUploaded.

## Decision

Use CloudEvents as the envelope for project domain events.

## What We Will Not Customize

Do not create a custom event envelope.

## Swap-Out Implication

Redpanda, Kafka, HTTP, or another transport can be used later without changing the event envelope.

## Accepted Limitations for PoC

Payload schemas remain project-specific and are validated with JSON Schema.
