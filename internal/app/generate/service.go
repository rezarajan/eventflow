// Package generate coordinates generator execution and CloudEvents emission.
package generate

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/eventflow/internal/contracts/event"
	"github.com/datascape/eventflow/internal/ports/generator"
)

// Service runs a selected generator and emits CloudEvents JSONL to an output writer.
type Service struct {
	Factory generator.Factory
	Logger  *slog.Logger
	Buffer  int
	Now     func() time.Time
}

// Request defines one generator execution.
type Request struct {
	Generator string
	Config    generator.Config
	Source    string
}

// Run executes a generator, maps generated facts into SDK CloudEvents, and writes JSONL output.
func (s Service) Run(ctx context.Context, request Request, writer io.Writer) (event.Summary, error) {
	started := s.now().UTC()
	logger := s.logger()
	if s.Factory == nil {
		return event.Summary{}, fmt.Errorf("generator factory is required")
	}
	selected, err := s.Factory.Create(request.Generator)
	if err != nil {
		return event.Summary{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	facts := make(chan event.Fact, s.buffer())
	events := make(chan cloudevents.Event, s.buffer())
	errCh := make(chan error, 3)
	stats := newRunStats()
	factory := event.NewFactory(request.Config.RunID, request.Source, s.Now)
	var wg sync.WaitGroup
	logger.Info("generation_started", "run_id", request.Config.RunID, "generator", selected.Name())
	wg.Add(1)
	go s.runGenerator(runCtx, &wg, errCh, selected, request.Config, facts)
	wg.Add(1)
	go s.mapFacts(runCtx, &wg, errCh, factory, facts, events, stats)
	wg.Add(1)
	go s.writeEvents(runCtx, &wg, errCh, writer, events)
	go closeErrorsAfterWait(&wg, errCh)
	for runErr := range errCh {
		if runErr != nil {
			cancel()
			return event.Summary{}, runErr
		}
	}
	completed := s.now().UTC()
	summary := stats.summary(request.Config.RunID, selected.Name(), started, completed)
	logger.Info("generation_completed", "run_id", summary.RunID, "generator", summary.Generator, "events", summary.Events, "by_type", summary.ByType, "duration_ms", summary.DurationMS)
	return summary, nil
}

// runGenerator streams domain-neutral facts from the selected generator.
func (s Service) runGenerator(ctx context.Context, wg *sync.WaitGroup, errCh chan<- error, selected generator.Port, config generator.Config, facts chan<- event.Fact) {
	defer wg.Done()
	if err := selected.Generate(ctx, config, facts); err != nil {
		errCh <- fmt.Errorf("generate facts: %w", err)
	}
}

// mapFacts converts facts into CloudEvents while preserving streaming behavior.
func (s Service) mapFacts(ctx context.Context, wg *sync.WaitGroup, errCh chan<- error, factory event.Factory, facts <-chan event.Fact, events chan<- cloudevents.Event, stats *runStats) {
	defer wg.Done()
	defer close(events)
	sequence := 0
	for fact := range facts {
		sequence++
		evt, err := factory.FromFact(sequence, fact)
		if err != nil {
			errCh <- fmt.Errorf("map fact to CloudEvent: %w", err)
			return
		}
		stats.add(evt.Type())
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		case events <- evt:
		}
	}
}

// writeEvents writes CloudEvents as JSONL to the configured writer.
func (s Service) writeEvents(ctx context.Context, wg *sync.WaitGroup, errCh chan<- error, writer io.Writer, events <-chan cloudevents.Event) {
	defer wg.Done()
	if err := event.EncodeJSONL(ctx, writer, events); err != nil {
		errCh <- fmt.Errorf("write CloudEvents: %w", err)
	}
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

// buffer returns the bounded channel size for streaming coordination.
func (s Service) buffer() int {
	if s.Buffer > 0 {
		return s.Buffer
	}
	return 64
}

// runStats tracks generation counts safely across streaming steps.
type runStats struct {
	sync.Mutex
	Facts  int
	Events int
	ByType map[string]int
}

// newRunStats constructs an empty generation statistics collector.
func newRunStats() *runStats {
	return &runStats{ByType: map[string]int{}}
}

// add records a generated event type in the run statistics.
func (s *runStats) add(eventType string) {
	s.Lock()
	defer s.Unlock()
	s.Facts++
	s.Events++
	s.ByType[eventType]++
}

// summary returns an immutable summary snapshot.
func (s *runStats) summary(runID string, generatorName string, started time.Time, completed time.Time) event.Summary {
	s.Lock()
	defer s.Unlock()
	byType := make(map[string]int, len(s.ByType))
	for key, value := range s.ByType {
		byType[key] = value
	}
	return event.Summary{RunID: runID, Generator: generatorName, Events: s.Events, Facts: s.Facts, ByType: byType, StartedAt: started, CompletedAt: completed, DurationMS: completed.Sub(started).Milliseconds()}
}

// closeErrorsAfterWait closes the error channel after all service goroutines finish.
func closeErrorsAfterWait(wg *sync.WaitGroup, errCh chan error) {
	wg.Wait()
	close(errCh)
}
