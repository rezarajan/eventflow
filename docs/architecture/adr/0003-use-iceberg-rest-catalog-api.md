# ADR: Use Iceberg REST Catalog API

## Status

Accepted for standards-only increment.

## Context

Catalog operations should be exposed through an existing table-catalog standard rather than a project-specific metadata API.

## Decision

Use the Iceberg REST Catalog API for namespace and table catalog operations.

## What We Will Not Customize

Do not create a custom catalog API.

## Swap-Out Implication

The local catalog can be replaced by another Iceberg REST-compatible catalog.

## Accepted Limitations for PoC

The compatibility profile is intentionally minimal for the PoC.
