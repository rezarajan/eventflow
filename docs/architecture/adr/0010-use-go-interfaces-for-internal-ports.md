# ADR: Use Go Interfaces for Internal Ports

## Status

Accepted for standards-only increment.

## Context

The codebase should use ports and adapters while avoiding premature service/API creation.

## Decision

Use Go interfaces for internal ports.

## What We Will Not Customize

Do not expose internal ports as network APIs by default.

## Swap-Out Implication

Adapters can change without changing domain or pipeline logic.

## Accepted Limitations for PoC

Interfaces should be small, stable, and aligned to external standards.
