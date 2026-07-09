# ADR: Use SQL DDL as the Sample Database Contract

## Status

Accepted for standards-only increment.

## Context

The synthetic operational source database is internal to the PoC but needs a clear and reviewable contract.

## Decision

Use SQL DDL files as the source database contract.

## What We Will Not Customize

Do not create a custom source database API.

## Swap-Out Implication

PostgreSQL can be replaced later if the schema contract and extraction semantics are preserved.

## Accepted Limitations for PoC

SQL dialect should remain conservative where practical.
