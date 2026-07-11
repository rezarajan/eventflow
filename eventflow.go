// Package eventflow defines the public SDK ports and runtime primitives.
//
// The SDK is transport-neutral: adapters implement small capability interfaces,
// validation is explicit, and callers own dependency construction.
package eventflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Event is the canonical in-memory Eventflow event representation.
//
// It is intentionally the CloudEvents SDK event type so adapters can preserve
// CloudEvents metadata without conversion loss.
type Event = cloudevents.Event

// Kind names a registered event kind/type.
type Kind string

// ValidationMode controls how contract and payload validation are applied.
type ValidationMode string

const (
	// ValidationStrict rejects unknown event types and all schema/envelope failures.
	ValidationStrict ValidationMode = "strict"
	// ValidationCompatible keeps strict envelope validation while allowing compatible contract evolution.
	ValidationCompatible ValidationMode = "compatible"
	// ValidationPermissive reports producer-correctable issues where possible but lets runtime dispatch continue.
	ValidationPermissive ValidationMode = "permissive"
	// ValidationDisabled bypasses contract and payload validation.
	ValidationDisabled ValidationMode = "disabled"
)

var (
	// ErrValidation marks validation failures that producers or contract authors can fix.
	ErrValidation = errors.New("eventflow validation failed")
	// ErrNotFound marks unresolved contracts, references, or missing transport resources.
	ErrNotFound = errors.New("eventflow resource not found")
	// ErrUnsupported marks unsupported codecs, adapters, or validation modes.
	ErrUnsupported = errors.New("eventflow unsupported capability")
)

// TypedError carries a stable kind for errors returned by SDK components.
type TypedError struct {
	Kind error
	Op   string
	Err  error
}

// Error returns a human-readable error.
func (e TypedError) Error() string {
	if e.Op == "" {
		return e.Err.Error()
	}
	return e.Op + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e TypedError) Unwrap() error { return e.Err }

// Is reports whether this error matches the requested stable kind.
func (e TypedError) Is(target error) bool { return target == e.Kind }

// ValidationError wraps a concrete validation failure.
func ValidationError(op string, err error) error {
	if err == nil {
		return nil
	}
	return TypedError{Kind: ErrValidation, Op: op, Err: err}
}

// UnsupportedError wraps an unsupported capability failure.
func UnsupportedError(op string, err error) error {
	if err == nil {
		return nil
	}
	return TypedError{Kind: ErrUnsupported, Op: op, Err: err}
}

// Emitter sends one event to a transport or storage target.
//
// Implementations must be safe for sequential use after Open. Concurrent use is
// adapter-specific and documented by each adapter.
type Emitter interface {
	Open(ctx context.Context) error
	Emit(ctx context.Context, event Event) error
	Close(ctx context.Context) error
}

// Named is implemented by adapters that expose a stable adapter name.
type Named interface {
	Name() string
}

// BatchEmitter emits event batches atomically when the underlying transport supports it.
type BatchEmitter interface {
	Emitter
	EmitBatch(ctx context.Context, events []Event) error
}

// Receiver reads explicit events from a transport or storage source.
type Receiver interface {
	Open(ctx context.Context) error
	Receive(ctx context.Context) (Event, error)
	Close(ctx context.Context) error
}

// BatchReceiver reads bounded event batches.
type BatchReceiver interface {
	Receiver
	ReceiveBatch(ctx context.Context, maxEvents int) ([]Event, error)
}

// Observer turns platform activity, such as object storage notifications, into observations.
type Observer interface {
	Open(ctx context.Context) error
	Observe(ctx context.Context) (Observation, error)
	Close(ctx context.Context) error
}

// Observation describes inferred platform activity.
type Observation struct {
	Source     string
	Subject    string
	Attributes map[string]string
	Event      *Event
	Time       time.Time
}

// EventHandler handles one normalized event.
type EventHandler interface {
	Handle(ctx context.Context, event Event) error
}

// ObservationHandler handles one platform observation.
type ObservationHandler interface {
	HandleObservation(ctx context.Context, observation Observation) error
}

// ObservationMapper converts platform activity into a normalized Eventflow event.
type ObservationMapper interface {
	MapObservation(ctx context.Context, observation Observation) (Event, error)
}

// Validator validates an event in a chosen validation mode.
type Validator interface {
	Validate(ctx context.Context, event Event, mode ValidationMode) error
}

// Codec encodes and decodes event representations.
type Codec interface {
	Encode(ctx context.Context, writer io.Writer, event Event) error
	Decode(ctx context.Context, reader io.Reader) (Event, error)
}

// Closer releases resources.
type Closer interface {
	Close(ctx context.Context) error
}

// Runtime composes receiver, validator, and handler without transport-specific logic.
type Runtime struct {
	Receiver  Receiver
	Validator Validator
	Handler   EventHandler
	Mode      ValidationMode
}

// Run receives events until EOF, cancellation, or handler failure.
func (r Runtime) Run(ctx context.Context) error {
	if r.Receiver == nil {
		return fmt.Errorf("receiver is required")
	}
	if r.Handler == nil {
		return fmt.Errorf("handler is required")
	}
	mode := r.Mode
	if mode == "" {
		mode = ValidationStrict
	}
	if err := r.Receiver.Open(ctx); err != nil {
		return err
	}
	defer r.Receiver.Close(ctx)
	for {
		evt, err := r.Receiver.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if r.Validator != nil {
			if err := r.Validator.Validate(ctx, evt, mode); err != nil {
				return err
			}
		}
		if err := r.Handler.Handle(ctx, evt); err != nil {
			return err
		}
	}
}

// ObservationRuntime composes an observer, mapper, validator, and handler.
type ObservationRuntime struct {
	Observer       Observer
	Mapper         ObservationMapper
	Validator      Validator
	Handler        EventHandler
	InvalidHandler EventHandler
	Mode           ValidationMode
}

// Run observes platform activity until EOF, cancellation, or handler failure.
func (r ObservationRuntime) Run(ctx context.Context) error {
	if r.Observer == nil {
		return fmt.Errorf("observer is required")
	}
	if r.Mapper == nil {
		return fmt.Errorf("observation mapper is required")
	}
	if r.Handler == nil {
		return fmt.Errorf("handler is required")
	}
	mode := r.Mode
	if mode == "" {
		mode = ValidationStrict
	}
	if err := r.Observer.Open(ctx); err != nil {
		return err
	}
	defer r.Observer.Close(ctx)
	for {
		observation, err := r.Observer.Observe(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		event, err := r.Mapper.MapObservation(ctx, observation)
		if err != nil {
			return err
		}
		if r.Validator != nil {
			if err := r.Validator.Validate(ctx, event, mode); err != nil {
				if r.InvalidHandler != nil {
					if emitErr := r.InvalidHandler.Handle(ctx, event); emitErr != nil {
						return emitErr
					}
					continue
				}
				return err
			}
		}
		if err := r.Handler.Handle(ctx, event); err != nil {
			return err
		}
	}
}
