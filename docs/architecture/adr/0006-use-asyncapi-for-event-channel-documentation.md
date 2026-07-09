# ADR: Use AsyncAPI for Event Channel Documentation

## Status

Accepted for standards-only increment.

## Context

Event bus topics/channels should be documented using an event-driven API standard.

## Decision

Use AsyncAPI to document future CloudEvents channels and messages.

## What We Will Not Customize

Do not create a custom topic/channel documentation format.

## Swap-Out Implication

The event broker can change later while the channel documentation remains stable.

## Accepted Limitations for PoC

This increment documents channels only; it does not require an event broker.
