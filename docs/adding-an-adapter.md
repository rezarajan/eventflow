# Adding An Adapter

Implement the smallest capability interface needed:

- `eventflow.Emitter` or `BatchEmitter` for outbound events.
- `eventflow.Receiver` or `BatchReceiver` for explicit inbound events.
- `eventflow.Observer` for inferred platform activity.
- `eventflow.Codec` for representation conversion.

Add typed config, constructor-based dependencies, and conformance tests using
`adaptertest.RunEmitterContract`, `RunReceiverContract`, `RunObserverContract`,
or `RunCodecContract`.

Adapters must not use package-level mutable registries or hidden global clients.

