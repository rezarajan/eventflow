package resource

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	eventflow "github.com/rezarajan/eventflow"
)

// EventContractSpec defines the CloudEvents contract for one event type.
//
// The contract is intentionally transport-neutral. It describes the event type,
// optional CloudEvents source/subject/content-type constraints, required
// extension attributes, and payload schema text used by the core validator.
type EventContractSpec struct {
	Type               string            `yaml:"type" json:"type"`
	Source             string            `yaml:"source,omitempty" json:"source,omitempty"`
	SourceRegex        string            `yaml:"sourceRegex,omitempty" json:"sourceRegex,omitempty"`
	Subject            string            `yaml:"subject,omitempty" json:"subject,omitempty"`
	DataContentType    string            `yaml:"dataContentType,omitempty" json:"dataContentType,omitempty"`
	PayloadSchema      string            `yaml:"payloadSchema,omitempty" json:"payloadSchema,omitempty"`
	OpenLineage        map[string]any    `yaml:"openLineage,omitempty" json:"openLineage,omitempty"`
	RequiredExtensions []string          `yaml:"requiredExtensions,omitempty" json:"requiredExtensions,omitempty"`
	ValidationMode     string            `yaml:"validationMode,omitempty" json:"validationMode,omitempty"`
	Metadata           map[string]string `yaml:"metadata,omitempty" json:"metadata,omitempty"`
}

// EventFlowSpec connects a pull-based receiver to contracts and emitters.
//
// EventFlow is for sources that already produce CloudEvents and implement
// eventflow.Receiver. Platform activity sources, such as S3 notifications, use
// ObservationFlow because they require an observer and mapper before validation.
type EventFlowSpec struct {
	ReceiverRef     *Reference  `yaml:"receiverRef,omitempty" json:"receiverRef,omitempty"`
	ObserverRef     *Reference  `yaml:"observerRef,omitempty" json:"observerRef,omitempty"`
	ContractRefs    []Reference `yaml:"contractRefs,omitempty" json:"contractRefs,omitempty"`
	ValidatorRefs   []Reference `yaml:"validatorRefs,omitempty" json:"validatorRefs,omitempty"`
	CodecRefs       []Reference `yaml:"codecRefs,omitempty" json:"codecRefs,omitempty"`
	EmitterRefs     []Reference `yaml:"emitterRefs,omitempty" json:"emitterRefs,omitempty"`
	InvalidEventRef *Reference  `yaml:"invalidEventRef,omitempty" json:"invalidEventRef,omitempty"`
	Mode            string      `yaml:"mode,omitempty" json:"mode,omitempty"`
}

// ObservationFlowSpec connects an observer and mapper to contracts and emitters.
//
// ObservationFlow is for activity that is not already a CloudEvent. The observer
// reads platform activity, the mapper converts each observation into an event,
// and the resulting event is validated and emitted like any other Eventflow
// event.
type ObservationFlowSpec struct {
	ObserverRef     Reference   `yaml:"observerRef" json:"observerRef"`
	MapperRef       Reference   `yaml:"mapperRef" json:"mapperRef"`
	ContractRefs    []Reference `yaml:"contractRefs,omitempty" json:"contractRefs,omitempty"`
	EmitterRefs     []Reference `yaml:"emitterRefs,omitempty" json:"emitterRefs,omitempty"`
	InvalidEventRef *Reference  `yaml:"invalidEventRef,omitempty" json:"invalidEventRef,omitempty"`
	Mode            string      `yaml:"mode,omitempty" json:"mode,omitempty"`
}

// Flow is the compiled runtime form of an EventFlow resource.
type Flow struct {
	Name           string
	Runtime        eventflow.Runtime
	Contracts      []EventContractSpec
	Emitters       []eventflow.Emitter
	InvalidEmitter eventflow.Emitter
}

// ObservationFlow is the compiled runtime form of an ObservationFlow resource.
type ObservationFlow struct {
	Name           string
	Runtime        eventflow.ObservationRuntime
	Contracts      []EventContractSpec
	Emitters       []eventflow.Emitter
	InvalidEmitter eventflow.Emitter
}

// RegisterCore registers EventContract, EventFlow, and ObservationFlow.
//
// NewCatalog calls RegisterCore automatically. Call RegisterCore directly only
// when constructing a custom Catalog implementation path.
func RegisterCore(catalog *Catalog) {
	_ = Register(catalog, Definition[EventContractSpec]{
		GVK: GVK("EventContract"),
		Validate: func(_ context.Context, spec EventContractSpec) error {
			if strings.TrimSpace(spec.Type) == "" {
				return fmt.Errorf("type is required")
			}
			if spec.SourceRegex != "" {
				if _, err := regexp.Compile(spec.SourceRegex); err != nil {
					return fmt.Errorf("sourceRegex: %w", err)
				}
			}
			if spec.ValidationMode != "" {
				if _, err := validationMode(spec.ValidationMode); err != nil {
					return err
				}
			}
			return nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityEventContract},
	})
	_ = Register(catalog, Definition[EventFlowSpec]{
		GVK: GVK("EventFlow"),
		Validate: func(_ context.Context, spec EventFlowSpec) error {
			if spec.ObserverRef != nil {
				return fmt.Errorf("EventFlow does not support observerRef; use ObservationFlow")
			}
			if spec.ReceiverRef == nil {
				return fmt.Errorf("receiverRef is required")
			}
			if len(spec.EmitterRefs) == 0 {
				return fmt.Errorf("at least one emitterRef is required")
			}
			if spec.Mode != "" {
				if _, err := validationMode(spec.Mode); err != nil {
					return err
				}
			}
			return nil
		},
		References: func(spec EventFlowSpec) []Reference {
			var refs []Reference
			if spec.ReceiverRef != nil {
				ref := *spec.ReceiverRef
				ref.Capability = CapabilityReceiver
				refs = append(refs, ref)
			}
			for _, ref := range spec.ContractRefs {
				ref.Capability = CapabilityEventContract
				refs = append(refs, ref)
			}
			for _, ref := range spec.ValidatorRefs {
				ref.Capability = CapabilityValidator
				refs = append(refs, ref)
			}
			for _, ref := range spec.CodecRefs {
				ref.Capability = CapabilityCodec
				refs = append(refs, ref)
			}
			for _, ref := range spec.EmitterRefs {
				ref.Capability = CapabilityEmitter
				refs = append(refs, ref)
			}
			if spec.InvalidEventRef != nil {
				ref := *spec.InvalidEventRef
				ref.Capability = CapabilityEmitter
				refs = append(refs, ref)
			}
			return refs
		},
		Build: func(_ context.Context, bctx BuildContext, spec EventFlowSpec) (any, error) {
			flow := Flow{Name: "eventflow"}
			var err error
			if spec.ReceiverRef != nil {
				flow.Runtime.Receiver, err = bctx.Receiver(*spec.ReceiverRef)
				if err != nil {
					return Flow{}, err
				}
			}
			for _, ref := range spec.ContractRefs {
				obj, err := bctx.Get(ref)
				if err != nil {
					return Flow{}, err
				}
				flow.Contracts = append(flow.Contracts, obj.(EventContractSpec))
			}
			for _, ref := range spec.EmitterRefs {
				emitter, err := bctx.Emitter(ref)
				if err != nil {
					return Flow{}, err
				}
				flow.Emitters = append(flow.Emitters, emitter)
			}
			if spec.InvalidEventRef != nil {
				flow.InvalidEmitter, err = bctx.Emitter(*spec.InvalidEventRef)
				if err != nil {
					return Flow{}, err
				}
			}
			flow.Runtime.Validator = contractValidator{contracts: flow.Contracts}
			flow.Runtime.Handler = emitterHandler{emitters: flow.Emitters}
			flow.Runtime.Mode, err = validationMode(spec.Mode)
			if err != nil {
				return Flow{}, err
			}
			return flow, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityEventFlow},
	})
	_ = Register(catalog, Definition[ObservationFlowSpec]{
		GVK: GVK("ObservationFlow"),
		Validate: func(_ context.Context, spec ObservationFlowSpec) error {
			if spec.ObserverRef.Kind == "" || spec.ObserverRef.Name == "" {
				return fmt.Errorf("observerRef kind and name are required")
			}
			if spec.MapperRef.Kind == "" || spec.MapperRef.Name == "" {
				return fmt.Errorf("mapperRef kind and name are required")
			}
			if len(spec.EmitterRefs) == 0 {
				return fmt.Errorf("at least one emitterRef is required")
			}
			if spec.Mode != "" {
				if _, err := validationMode(spec.Mode); err != nil {
					return err
				}
			}
			return nil
		},
		References: func(spec ObservationFlowSpec) []Reference {
			observerRef := spec.ObserverRef
			observerRef.Capability = CapabilityObserver
			mapperRef := spec.MapperRef
			mapperRef.Capability = CapabilityObservationMapper
			refs := []Reference{observerRef, mapperRef}
			for _, ref := range spec.ContractRefs {
				ref.Capability = CapabilityEventContract
				refs = append(refs, ref)
			}
			for _, ref := range spec.EmitterRefs {
				ref.Capability = CapabilityEmitter
				refs = append(refs, ref)
			}
			if spec.InvalidEventRef != nil {
				ref := *spec.InvalidEventRef
				ref.Capability = CapabilityEmitter
				refs = append(refs, ref)
			}
			return refs
		},
		Build: func(_ context.Context, bctx BuildContext, spec ObservationFlowSpec) (any, error) {
			flow := ObservationFlow{Name: "observationflow"}
			var err error
			flow.Runtime.Observer, err = bctx.Observer(spec.ObserverRef)
			if err != nil {
				return ObservationFlow{}, err
			}
			flow.Runtime.Mapper, err = bctx.ObservationMapper(spec.MapperRef)
			if err != nil {
				return ObservationFlow{}, err
			}
			for _, ref := range spec.ContractRefs {
				obj, err := bctx.Get(ref)
				if err != nil {
					return ObservationFlow{}, err
				}
				flow.Contracts = append(flow.Contracts, obj.(EventContractSpec))
			}
			for _, ref := range spec.EmitterRefs {
				emitter, err := bctx.Emitter(ref)
				if err != nil {
					return ObservationFlow{}, err
				}
				flow.Emitters = append(flow.Emitters, emitter)
			}
			if spec.InvalidEventRef != nil {
				flow.InvalidEmitter, err = bctx.Emitter(*spec.InvalidEventRef)
				if err != nil {
					return ObservationFlow{}, err
				}
			}
			flow.Runtime.Validator = contractValidator{contracts: flow.Contracts}
			flow.Runtime.Handler = emitterHandler{emitters: flow.Emitters}
			if flow.InvalidEmitter != nil {
				flow.Runtime.InvalidHandler = emitterHandler{emitters: []eventflow.Emitter{flow.InvalidEmitter}}
			}
			flow.Runtime.Mode, err = validationMode(spec.Mode)
			if err != nil {
				return ObservationFlow{}, err
			}
			return flow, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityObservationFlow},
	})
}

func validationMode(value string) (eventflow.ValidationMode, error) {
	switch mode := eventflow.ValidationMode(strings.TrimSpace(value)); mode {
	case "":
		return eventflow.ValidationStrict, nil
	case eventflow.ValidationStrict, eventflow.ValidationCompatible, eventflow.ValidationPermissive, eventflow.ValidationDisabled:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported validation mode %q", value)
	}
}

type emitterHandler struct{ emitters []eventflow.Emitter }

func (h emitterHandler) Handle(ctx context.Context, event eventflow.Event) error {
	for _, emitter := range h.emitters {
		if err := emitter.Emit(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

type contractValidator struct{ contracts []EventContractSpec }

func (v contractValidator) Validate(ctx context.Context, event eventflow.Event, mode eventflow.ValidationMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mode == eventflow.ValidationDisabled {
		return nil
	}
	if err := event.Validate(); err != nil {
		return eventflow.ValidationError("validate cloudevent", err)
	}
	for _, contract := range v.contracts {
		if contract.Type != event.Type() {
			continue
		}
		return validateContract(contract, event, mode)
	}
	if mode == eventflow.ValidationPermissive {
		return nil
	}
	return eventflow.ValidationError("resolve event contract", fmt.Errorf("%w: %s", eventflow.ErrNotFound, event.Type()))
}

func validateContract(contract EventContractSpec, event eventflow.Event, mode eventflow.ValidationMode) error {
	if contract.Source != "" && contract.Source != event.Source() {
		return eventflow.ValidationError("validate source", fmt.Errorf("source %q does not match %q", event.Source(), contract.Source))
	}
	if contract.SourceRegex != "" {
		matched, err := regexp.MatchString(contract.SourceRegex, event.Source())
		if err != nil {
			return err
		}
		if !matched {
			return eventflow.ValidationError("validate source", fmt.Errorf("source %q does not match regex %q", event.Source(), contract.SourceRegex))
		}
	}
	if contract.Subject != "" && contract.Subject != event.Subject() {
		return eventflow.ValidationError("validate subject", fmt.Errorf("subject %q does not match %q", event.Subject(), contract.Subject))
	}
	if contract.DataContentType != "" && contract.DataContentType != event.DataContentType() {
		return eventflow.ValidationError("validate datacontenttype", fmt.Errorf("datacontenttype %q does not match %q", event.DataContentType(), contract.DataContentType))
	}
	for _, extension := range contract.RequiredExtensions {
		if _, ok := event.Extensions()[strings.ToLower(strings.TrimSpace(extension))]; !ok {
			return eventflow.ValidationError("validate extensions", fmt.Errorf("required extension %q is missing", extension))
		}
	}
	if mode == eventflow.ValidationPermissive || contract.PayloadSchema == "" {
		return nil
	}
	if len(event.Data()) == 0 {
		return eventflow.ValidationError("validate payload", io.ErrUnexpectedEOF)
	}
	return nil
}
