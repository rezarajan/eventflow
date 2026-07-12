// Package journal defines durable accepted-event storage and delivery state.
package journal

import (
	"context"
	"errors"
	"io"
	"time"

	eventflow "github.com/rezarajan/eventflow"
)

// RecordID identifies one journaled accepted event.
type RecordID int64

// DestinationID identifies a configured delivery target.
type DestinationID string

// State describes journal or per-destination delivery state.
type State string

const (
	StateAccepted    State = "ACCEPTED"
	StateJournaled   State = "JOURNALED"
	StatePending     State = "PENDING"
	StateDelivering  State = "DELIVERING"
	StateDelivered   State = "DELIVERED"
	StateFailed      State = "FAILED"
	StateQuarantined State = "QUARANTINED"
)

// Record is an immutable accepted event plus its journal identity.
type Record struct {
	ID        RecordID
	Flow      string
	Event     eventflow.Event
	EventID   string
	EventType string
	Source    string
	EventTime time.Time
	FirstSeen time.Time
	State     State
}

// Delivery is the per-destination state for a record.
type Delivery struct {
	RecordID     RecordID
	Destination  DestinationID
	State        State
	AttemptCount int
	LastError    string
	FirstSeen    time.Time
	LastAttempt  time.Time
	NextAttempt  time.Time
}

// AppendOptions describes the destination rows to create with an accepted event.
type AppendOptions struct {
	Flow         string
	Destinations []DestinationID
}

// ReplayFilter selects records for replay or inspection.
type ReplayFilter struct {
	Flow        string
	Destination DestinationID
	State       State
	EventID     string
	Since       time.Time
	Until       time.Time
	Limit       int
}

// PendingFilter selects deliveries ready for dispatch.
type PendingFilter struct {
	Flow  string
	Now   time.Time
	Limit int
}

// Iterator streams journal records.
type Iterator interface {
	Next(context.Context) (Record, error)
	Close() error
}

// Journal stores accepted events and per-destination delivery state.
type Journal interface {
	Open(ctx context.Context) error
	Append(ctx context.Context, event eventflow.Event, options AppendOptions) (Record, error)
	MarkAttempt(ctx context.Context, recordID RecordID, destination DestinationID, nextAttempt time.Time) error
	MarkDelivered(ctx context.Context, recordID RecordID, destination DestinationID) error
	MarkFailed(ctx context.Context, recordID RecordID, destination DestinationID, cause error, terminal bool, nextAttempt time.Time) error
	Query(ctx context.Context, filter ReplayFilter) (Iterator, error)
	Pending(ctx context.Context, filter PendingFilter) ([]Delivery, error)
	Get(ctx context.Context, recordID RecordID) (Record, error)
	Close(ctx context.Context) error
}

// ErrNotFound marks a missing journal record.
var ErrNotFound = errors.New("journal record not found")

// IsEOF reports whether err is the end of an iterator.
func IsEOF(err error) bool { return errors.Is(err, io.EOF) }
