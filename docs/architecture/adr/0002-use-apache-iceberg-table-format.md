# ADR: Use Apache Iceberg Table Format

## Status

Accepted for standards-only increment.

## Context

The lakehouse requires an open table format with snapshots, schema tracking, and compute-engine portability.

## Decision

Use Apache Iceberg as the table format.

## What We Will Not Customize

Do not create a custom lakehouse table metadata format.

## Swap-Out Implication

DuckDB, Spark, Trino, Dremio, or another engine can be introduced later if it can read/write the required Iceberg profile.

## Accepted Limitations for PoC

The first increment only defines the contract; no table engine is implemented.
