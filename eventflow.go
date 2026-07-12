// Package eventflow implements an OpenLineage admission and quarantine gateway for shared data-platform infrastructure.
package eventflow

import (
	"context"
	"errors"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Event is the CloudEvents representation admitted by Eventflow.
type Event = cloudevents.Event

var ErrValidation = errors.New("eventflow validation failed")

type TypedError struct {
	Kind error
	Op   string
	Err  error
}

func (e TypedError) Error() string {
	if e.Op == "" {
		return e.Err.Error()
	}
	return e.Op + ": " + e.Err.Error()
}
func (e TypedError) Unwrap() error        { return e.Err }
func (e TypedError) Is(target error) bool { return target == e.Kind }

func ValidationError(op string, err error) error {
	if err == nil {
		return nil
	}
	return TypedError{Kind: ErrValidation, Op: op, Err: err}
}

// Outcome is the stable admission result.
type Outcome string

const (
	OutcomeAccept     Outcome = "ACCEPT"
	OutcomeReject     Outcome = "REJECT"
	OutcomeQuarantine Outcome = "QUARANTINE"
)

const (
	ReasonCloudEventInvalid             = "EF1001_CLOUDEVENT_INVALID"
	ReasonEventTypeUnsupported          = "EF1002_EVENT_TYPE_UNSUPPORTED"
	ReasonOpenLineageSchemaInvalid      = "EF1101_OPENLINEAGE_SCHEMA_INVALID"
	ReasonOpenLineageVersionUnsupported = "EF1102_OPENLINEAGE_VERSION_UNSUPPORTED"
	ReasonProducerNotAllowed            = "EF1201_PRODUCER_NOT_ALLOWED"
	ReasonJobNamespaceNotAllowed        = "EF1202_JOB_NAMESPACE_NOT_ALLOWED"
	ReasonDatasetNamespaceNotAllowed    = "EF1203_DATASET_NAMESPACE_NOT_ALLOWED"
	ReasonRequiredFacetMissing          = "EF1301_REQUIRED_FACET_MISSING"
	ReasonFacetNotAllowed               = "EF1302_FACET_NOT_ALLOWED"
	ReasonEventTooLarge                 = "EF1303_EVENT_TOO_LARGE"
	ReasonRateLimitExceeded             = "EF1401_RATE_LIMIT_EXCEEDED"
	ReasonBrokerUnavailable             = "EF1501_BROKER_UNAVAILABLE"
)

// Decision is the stable external admission decision contract.
type Decision struct {
	Outcome       Outcome `json:"outcome"`
	ReasonCode    string  `json:"reasonCode,omitempty"`
	Message       string  `json:"message,omitempty"`
	Field         string  `json:"field,omitempty"`
	Principal     string  `json:"principal,omitempty"`
	PolicyName    string  `json:"policyName,omitempty"`
	PolicyVersion string  `json:"policyVersion,omitempty"`
}

// Accepted returns an accept decision.
func Accepted(principal string, policyName string, policyVersion string) Decision {
	return Decision{Outcome: OutcomeAccept, Principal: principal, PolicyName: policyName, PolicyVersion: policyVersion}
}

// Rejected returns a reject decision.
func Rejected(code string, message string, field string, principal string, policyName string, policyVersion string) Decision {
	return Decision{Outcome: OutcomeReject, ReasonCode: code, Message: message, Field: field, Principal: principal, PolicyName: policyName, PolicyVersion: policyVersion}
}

// Emitter publishes an admitted event to a destination.
type Emitter interface {
	Open(context.Context) error
	Emit(context.Context, Event) error
	Close(context.Context) error
}

// Receiver receives events from a source.
type Receiver interface {
	Open(context.Context) error
	Receive(context.Context) (ReceivedEvent, error)
	Close(context.Context) error
}

// ReceivedEvent contains decoded event data and source acknowledgement hooks.
type ReceivedEvent struct {
	Event     Event
	Raw       []byte
	Principal string
	Ack       func(context.Context) error
	Nack      func(context.Context) error
}
