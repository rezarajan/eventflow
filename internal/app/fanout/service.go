// Package fanout coordinates generic CloudEvents fan-out to configured publishers.
package fanout

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/eventflow/internal/contracts/event"
	port "github.com/datascape/eventflow/internal/ports/fanout"
)

// Service reads CloudEvents JSONL and publishes each event to every configured output.
type Service struct {
	Publishers []port.Publisher
	Logger     *slog.Logger
	Buffer     int
	BatchSize  int
	Now        func() time.Time
}

// Run opens every publisher, reads events from the input stream, publishes each event, and closes publishers.
func (s Service) Run(ctx context.Context, runID string, reader io.Reader) (summary event.Summary, err error) {
	started := s.now().UTC()
	if len(s.Publishers) == 0 {
		return event.Summary{}, fmt.Errorf("at least one publisher is required")
	}
	logger := s.logger()
	logger.Info("fanout_started", "run_id", runID, "outputs", publisherNames(s.Publishers))
	opened, openErr := openPublishers(ctx, s.Publishers)
	if openErr != nil {
		_ = closePublishers(ctx, opened)
		return event.Summary{}, openErr
	}
	defer func() {
		if closeErr := closePublishers(ctx, opened); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	input := make(chan cloudevents.Event, s.buffer())
	decodeErrCh := make(chan error, 1)
	go s.decode(runCtx, decodeErrCh, reader, input)
	stats := newFanoutStats()
	workers := s.startWorkers(runCtx, cancel, opened, stats)
	broadcastErr := s.broadcast(runCtx, input, workers)
	closeWorkerInputs(workers)
	waitForWorkers(workers)
	workerErr := firstWorkerError(workers)
	decodeErr := <-decodeErrCh
	if workerErr != nil {
		return event.Summary{}, workerErr
	}
	if decodeErr != nil {
		return event.Summary{}, decodeErr
	}
	if broadcastErr != nil {
		return event.Summary{}, broadcastErr
	}
	completed := s.now().UTC()
	summary = stats.summary(runID, publisherNames(s.Publishers), started, completed)
	logger.Info("fanout_completed", "run_id", runID, "events", summary.Events, "outputs", summary.OutputNames, "by_type", summary.ByType, "duration_ms", summary.DurationMS)
	return summary, nil
}

// decode reads CloudEvents from the input stream and records the decode result.
func (s Service) decode(ctx context.Context, errCh chan<- error, reader io.Reader, out chan<- cloudevents.Event) {
	if err := event.DecodeJSONL(ctx, reader, out); err != nil {
		errCh <- fmt.Errorf("decode CloudEvents: %w", err)
		return
	}
	errCh <- nil
}

// startWorkers starts one independent publishing worker for each configured publisher.
func (s Service) startWorkers(ctx context.Context, cancel context.CancelFunc, publishers []port.Publisher, stats *fanoutStats) []*publisherWorker {
	workers := make([]*publisherWorker, 0, len(publishers))
	for _, publisher := range publishers {
		worker := &publisherWorker{publisher: publisher, input: make(chan cloudevents.Event, s.buffer()), batchSize: s.batchSize(), stats: stats, errCh: make(chan error, 1)}
		worker.wg.Add(1)
		go worker.run(ctx, cancel)
		workers = append(workers, worker)
	}
	return workers
}

// broadcast copies each input event to every publisher worker queue.
func (s Service) broadcast(ctx context.Context, input <-chan cloudevents.Event, workers []*publisherWorker) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-input:
			if !ok {
				return nil
			}
			for _, worker := range workers {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case worker.input <- evt:
				}
			}
		}
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
	return 256
}

// batchSize returns the maximum number of events sent to batch-capable publishers at once.
func (s Service) batchSize() int {
	if s.BatchSize > 0 {
		return s.BatchSize
	}
	return 100
}

// publisherWorker owns the publishing loop for one output adapter.
type publisherWorker struct {
	publisher port.Publisher
	input     chan cloudevents.Event
	batchSize int
	stats     *fanoutStats
	errCh     chan error
	wg        sync.WaitGroup
}

// run publishes all queued events for one output adapter and records the first error.
func (w *publisherWorker) run(ctx context.Context, cancel context.CancelFunc) {
	defer w.wg.Done()
	if batchPublisher, ok := w.publisher.(port.BatchPublisher); ok {
		w.recordErr(w.publishBatches(ctx, batchPublisher), cancel)
		return
	}
	w.recordErr(w.publishOneByOne(ctx), cancel)
}

// publishOneByOne publishes events through an adapter that only supports individual events.
func (w *publisherWorker) publishOneByOne(ctx context.Context) error {
	for evt := range w.input {
		if err := w.publisher.Publish(ctx, evt); err != nil {
			return fmt.Errorf("publish to %s: %w", w.publisher.Name(), err)
		}
		w.stats.add(evt.Type())
	}
	return nil
}

// publishBatches publishes events through an adapter that supports transport-level batching.
func (w *publisherWorker) publishBatches(ctx context.Context, publisher port.BatchPublisher) error {
	batch := make([]cloudevents.Event, 0, w.batchSize)
	for evt := range w.input {
		batch = append(batch, evt)
		if len(batch) >= w.batchSize {
			if err := w.flushBatch(ctx, publisher, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := w.flushBatch(ctx, publisher, batch); err != nil {
			return err
		}
	}
	return nil
}

// flushBatch publishes one batch and records statistics only after the batch succeeds.
func (w *publisherWorker) flushBatch(ctx context.Context, publisher port.BatchPublisher, batch []cloudevents.Event) error {
	if err := publisher.PublishBatch(ctx, batch); err != nil {
		return fmt.Errorf("publish batch to %s: %w", publisher.Name(), err)
	}
	for _, evt := range batch {
		w.stats.add(evt.Type())
	}
	return nil
}

// recordErr stores the worker result and cancels the run when an error occurs.
func (w *publisherWorker) recordErr(err error, cancel context.CancelFunc) {
	if err != nil {
		cancel()
	}
	w.errCh <- err
}

// wait blocks until a publishing worker has stopped.
func (w *publisherWorker) wait() {
	w.wg.Wait()
}

// err returns the worker result after the worker has stopped.
func (w *publisherWorker) err() error {
	return <-w.errCh
}

// fanoutStats tracks fan-out counts safely across processing steps.
type fanoutStats struct {
	sync.Mutex
	Events int
	ByType map[string]int
}

// newFanoutStats constructs an empty fan-out statistics collector.
func newFanoutStats() *fanoutStats {
	return &fanoutStats{ByType: map[string]int{}}
}

// add records one event published by one publisher.
func (s *fanoutStats) add(eventType string) {
	s.Lock()
	defer s.Unlock()
	s.Events++
	s.ByType[eventType]++
}

// summary returns an immutable fan-out summary snapshot.
func (s *fanoutStats) summary(runID string, outputs []string, started time.Time, completed time.Time) event.Summary {
	s.Lock()
	defer s.Unlock()
	byType := make(map[string]int, len(s.ByType))
	for key, value := range s.ByType {
		byType[key] = value
	}
	return event.Summary{RunID: runID, Events: s.Events, ByType: byType, OutputNames: outputs, StartedAt: started, CompletedAt: completed, DurationMS: completed.Sub(started).Milliseconds()}
}

// openPublishers opens all configured publishers and returns the publishers that opened successfully.
func openPublishers(ctx context.Context, publishers []port.Publisher) ([]port.Publisher, error) {
	opened := make([]port.Publisher, 0, len(publishers))
	for _, publisher := range publishers {
		if err := publisher.Open(ctx); err != nil {
			return opened, fmt.Errorf("open publisher %s: %w", publisher.Name(), err)
		}
		opened = append(opened, publisher)
	}
	return opened, nil
}

// closePublishers closes all opened publishers and returns the first close error.
func closePublishers(ctx context.Context, publishers []port.Publisher) error {
	var firstErr error
	for i := len(publishers) - 1; i >= 0; i-- {
		publisher := publishers[i]
		if err := publisher.Close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close publisher %s: %w", publisher.Name(), err)
		}
	}
	return firstErr
}

// closeWorkerInputs closes every publisher worker input channel.
func closeWorkerInputs(workers []*publisherWorker) {
	for _, worker := range workers {
		close(worker.input)
	}
}

// waitForWorkers waits for every publisher worker to stop.
func waitForWorkers(workers []*publisherWorker) {
	for _, worker := range workers {
		worker.wait()
	}
}

// firstWorkerError returns the first non-nil publisher worker error.
func firstWorkerError(workers []*publisherWorker) error {
	for _, worker := range workers {
		if err := worker.err(); err != nil {
			return err
		}
	}
	return nil
}

// publisherNames returns publisher names in configured order.
func publisherNames(publishers []port.Publisher) []string {
	names := make([]string, 0, len(publishers))
	for _, publisher := range publishers {
		names = append(names, publisher.Name())
	}
	return names
}
