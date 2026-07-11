# Adding An Adapter

This page is the short checklist. The complete SDK extension guide is
[extending.md](extending.md).

Implement the smallest capability interface needed:

- `eventflow.Emitter` or `BatchEmitter` for outbound events.
- `eventflow.Receiver` or `BatchReceiver` for explicit inbound events.
- `eventflow.Observer` for inferred platform activity.
- `eventflow.ObservationMapper` for converting platform observations into CloudEvents.
- `eventflow.Codec` for representation conversion.

Add typed config, constructor-based dependencies, and conformance tests using
`adaptertest.RunEmitterContract`, `RunReceiverContract`, `RunObserverContract`,
or `RunCodecContract`.

Adapters must not use package-level mutable registries or hidden global clients.
