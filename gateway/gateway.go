// Package gateway composes validation, journaling, quarantine, and dispatch.
package gateway

import (
	"context"
	"fmt"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/journal"
	"github.com/rezarajan/eventflow/observability/metrics"
)

// JournalHandler appends accepted events before acknowledgement.
type JournalHandler struct {
	Flow         string
	Journal      journal.Journal
	Destinations []journal.DestinationID
}

// Handle appends an accepted event to the durable journal.
func (h JournalHandler) Handle(ctx context.Context, event eventflow.Event) error {
	if h.Journal == nil {
		return fmt.Errorf("journal is required")
	}
	_, err := h.Journal.Append(ctx, event, journal.AppendOptions{Flow: h.Flow, Destinations: h.Destinations})
	if err != nil {
		metrics.Inc("eventflow_journal_append_failures_total", map[string]string{"flow": h.Flow})
	}
	return err
}
