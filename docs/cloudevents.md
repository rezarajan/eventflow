# CloudEvents

Eventflow uses CloudEvents v1.0 as its canonical event envelope. The public
`cloudevent` package provides:

- Structured JSON codec for `application/cloudevents+json`.
- HTTP binary binding decode and header helpers.
- Extension validation.

Domain payloads are JSON. `EventContract` resources can reference payload
schemas and constrain CloudEvents envelope attributes.
