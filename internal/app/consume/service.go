// Package consume coordinates bounded CloudEvents consumption and handler dispatch.
package consume

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/eventflow/internal/contracts/event"
	"github.com/datascape/eventflow/internal/lineage"
	port "github.com/datascape/eventflow/internal/ports/consume"
)

// Service reads CloudEvents from a source and applies them to configured handlers.
type Service struct {
	Source    port.EventSource
	Handlers  []port.EventHandler
	Logger    *slog.Logger
	Lineage   lineage.Emitter
	LineageNS string
	BatchSize int
	MaxEvents int
	Now       func() time.Time
}

// Run opens the source and handlers, consumes bounded event batches, and closes all resources.
func (s Service) Run(ctx context.Context, runID string) (summary event.Summary, err error) {
	started := s.now().UTC()
	if s.Source == nil {
		return event.Summary{}, fmt.Errorf("event source is required")
	}
	if len(s.Handlers) == 0 {
		return event.Summary{}, fmt.Errorf("at least one event handler is required")
	}
	logger := s.logger()
	logger.Info("consume_started", "run_id", runID, "source", s.Source.Name(), "handlers", handlerNames(s.Handlers), "max_events", s.MaxEvents)
	openedHandlers, openErr := s.openAll(ctx)
	if openErr != nil {
		_ = closeHandlers(ctx, openedHandlers)
		_ = s.Source.Close(ctx)
		return event.Summary{}, openErr
	}
	defer func() {
		if closeErr := closeHandlers(ctx, openedHandlers); closeErr != nil && err == nil {
			err = closeErr
		}
		if closeErr := s.Source.Close(ctx); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	stats := newConsumeStats()
	for s.shouldContinue(stats.events()) {
		limit := s.nextBatchLimit(stats.events())
		events, readErr := s.Source.ReadBatch(ctx, limit)
		if readErr != nil && readErr != io.EOF {
			return event.Summary{}, fmt.Errorf("read events from %s: %w", s.Source.Name(), readErr)
		}
		if len(events) > 0 {
			if err := s.handleBatch(ctx, runID, events, stats); err != nil {
				return event.Summary{}, err
			}
		}
		if readErr == io.EOF || len(events) == 0 {
			break
		}
	}
	completed := s.now().UTC()
	summary = stats.summary(runID, handlerNames(s.Handlers), started, completed)
	logger.Info("consume_completed", "run_id", runID, "events", summary.Events, "handlers", summary.OutputNames, "by_type", summary.ByType, "duration_ms", summary.DurationMS)
	return summary, nil
}

// openAll opens the source and then every configured handler.
func (s Service) openAll(ctx context.Context) ([]port.EventHandler, error) {
	if err := s.Source.Open(ctx); err != nil {
		return nil, fmt.Errorf("open source %s: %w", s.Source.Name(), err)
	}
	opened := make([]port.EventHandler, 0, len(s.Handlers))
	for _, handler := range s.Handlers {
		if err := handler.Open(ctx); err != nil {
			return opened, fmt.Errorf("open handler %s: %w", handler.Name(), err)
		}
		opened = append(opened, handler)
	}
	return opened, nil
}

// handleBatch applies one consumed batch to every handler.
func (s Service) handleBatch(ctx context.Context, runID string, events []cloudevents.Event, stats *consumeStats) error {
	for _, handler := range s.Handlers {
		projectorRunID := fmt.Sprintf("%s-%s-%d", runID, handler.Name(), stats.events()+len(events))
		if err := s.emitLineage(ctx, "START", handler.Name(), projectorRunID, []lineage.Dataset{s.Source.Dataset()}, []lineage.Dataset{handler.Dataset()}, nil); err != nil {
			return err
		}
		var handleErr error
		if batchHandler, ok := handler.(port.BatchEventHandler); ok {
			handleErr = batchHandler.HandleBatch(ctx, events)
		} else {
			for _, evt := range events {
				if err := handler.Handle(ctx, evt); err != nil {
					handleErr = err
					break
				}
			}
		}
		eventType := "COMPLETE"
		if handleErr != nil {
			eventType = "FAIL"
		}
		if err := s.emitLineage(ctx, eventType, handler.Name(), projectorRunID, []lineage.Dataset{s.Source.Dataset()}, []lineage.Dataset{handler.Dataset()}, handleErr); err != nil {
			return err
		}
		if handleErr != nil {
			return fmt.Errorf("handle batch with %s: %w", handler.Name(), handleErr)
		}
	}
	for _, evt := range events {
		stats.add(evt.Type())
	}
	return nil
}

// emitLineage emits one projector lineage event when lineage is configured.
func (s Service) emitLineage(ctx context.Context, eventType string, handlerName string, runID string, inputs []lineage.Dataset, outputs []lineage.Dataset, runErr error) error {
	if s.Lineage == nil {
		return nil
	}
	jobName := "eventflow-" + handlerName + "-projector"
	namespace := s.LineageNS
	if namespace == "" {
		namespace = "eventflow"
	}
	return s.Lineage.Emit(ctx, lineage.NewEvent(eventType, namespace, jobName, runID, inputs, outputs, runErr, s.Now))
}

// shouldContinue reports whether the service should request another batch.
func (s Service) shouldContinue(consumed int) bool {
	return s.MaxEvents <= 0 || consumed < s.MaxEvents
}

// nextBatchLimit returns the next source read size after applying configured bounds.
func (s Service) nextBatchLimit(consumed int) int {
	limit := s.batchSize()
	if s.MaxEvents <= 0 {
		return limit
	}
	remaining := s.MaxEvents - consumed
	if remaining < limit {
		return remaining
	}
	return limit
}

// batchSize returns the configured batch size or a safe default.
func (s Service) batchSize() int {
	if s.BatchSize > 0 {
		return s.BatchSize
	}
	return 100
}

// logger returns a usable structured logger.
func (s Service) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// now returns the current time using the service clock.
func (s Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// closeHandlers closes handlers in reverse open order.
func closeHandlers(ctx context.Context, handlers []port.EventHandler) error {
	var firstErr error
	for i := len(handlers) - 1; i >= 0; i-- {
		handler := handlers[i]
		if err := handler.Close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close handler %s: %w", handler.Name(), err)
		}
	}
	return firstErr
}

// handlerNames returns handler names in configured order.
func handlerNames(handlers []port.EventHandler) []string {
	names := make([]string, 0, len(handlers))
	for _, handler := range handlers {
		names = append(names, handler.Name())
	}
	return names
}

// consumeStats tracks consumed event counts safely.
type consumeStats struct {
	sync.Mutex
	Events int
	ByType map[string]int
}

// newConsumeStats constructs an empty consumer statistics collector.
func newConsumeStats() *consumeStats {
	return &consumeStats{ByType: map[string]int{}}
}

// add records one consumed event after handlers have applied it successfully.
func (s *consumeStats) add(eventType string) {
	s.Lock()
	defer s.Unlock()
	s.Events++
	s.ByType[eventType]++
}

// events returns the current consumed event count.
func (s *consumeStats) events() int {
	s.Lock()
	defer s.Unlock()
	return s.Events
}

// summary returns an immutable consumer summary snapshot.
func (s *consumeStats) summary(runID string, handlers []string, started time.Time, completed time.Time) event.Summary {
	s.Lock()
	defer s.Unlock()
	byType := make(map[string]int, len(s.ByType))
	for key, value := range s.ByType {
		byType[key] = value
	}
	return event.Summary{RunID: runID, Events: s.Events, ByType: byType, OutputNames: handlers, StartedAt: started, CompletedAt: completed, DurationMS: completed.Sub(started).Milliseconds()}
}
