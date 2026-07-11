# SDK

Import the root SDK for ports and runtime composition:

```go
import eventflow "github.com/rezarajan/eventflow"
```

Primary interfaces are `Emitter`, `BatchEmitter`, `Receiver`, `BatchReceiver`,
`Observer`, `EventHandler`, `ObservationHandler`, `Validator`, `Codec`, and
`Closer`. All methods accept `context.Context` and return typed errors where
validation or unsupported capabilities are involved.

```go
runtime := eventflow.Runtime{
    Receiver:  receiver,
    Validator: validator,
    Handler:   handler,
    Mode:      eventflow.ValidationStrict,
}
err := runtime.Run(ctx)
```

Strict validation is the default. Other modes are explicit:
`compatible`, `permissive`, and `disabled`.

