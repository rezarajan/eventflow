# ADR: Use OpenTelemetry for Observability

## Status

Accepted for standards-only increment.

## Context

Technical telemetry must not be confused with audit records or lineage events.

## Decision

Use OpenTelemetry for traces, metrics, and logs.

## What We Will Not Customize

Do not create a custom telemetry protocol.

## Swap-Out Implication

Telemetry backends can be replaced if they accept OpenTelemetry signals.

## Accepted Limitations for PoC

The first increment defines the profile only.
