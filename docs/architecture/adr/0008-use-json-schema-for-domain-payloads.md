# ADR: Use JSON Schema for Domain Payloads

## Status

Accepted for standards-only increment.

## Context

Internal payloads require validation without creating network APIs.

## Decision

Use JSON Schema for event payloads, generator configuration, quality results, audit records, and conformance reports.

## What We Will Not Customize

Do not embed validation rules only in code.

## Swap-Out Implication

Schemas can be reused by Go validation libraries, AsyncAPI messages, tests, and documentation.

## Accepted Limitations for PoC

Schemas must remain backward-compatible within a major version.
