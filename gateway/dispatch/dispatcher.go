// Package dispatch delivers journaled events to configured destinations.
package dispatch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/journal"
	"github.com/rezarajan/eventflow/observability/metrics"
)

// Config controls dispatcher retry and concurrency behavior.
type Config struct {
	Flow                 string
	MaxAttempts          int
	InitialRetryDelay    time.Duration
	MaxRetryDelay        time.Duration
	WorkerConcurrency    int
	DispatchTimeout      time.Duration
	ShutdownDrainTimeout time.Duration
	PollInterval         time.Duration
	BatchSize            int
}

// Dispatcher drains pending journal deliveries to emitters.
type Dispatcher struct {
	config       Config
	journal      journal.Journal
	destinations map[journal.DestinationID]eventflow.Emitter
}

// New constructs a dispatcher.
func New(config Config, j journal.Journal, destinations map[journal.DestinationID]eventflow.Emitter) *Dispatcher {
	config = normalize(config)
	return &Dispatcher{config: config, journal: j, destinations: destinations}
}

// Run dispatches pending work until ctx is canceled.
func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.config.PollInterval)
	defer ticker.Stop()
	for {
		if err := d.DispatchReady(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return d.drain()
		case <-ticker.C:
		}
	}
}

// DispatchReady dispatches one batch of ready pending deliveries.
func (d *Dispatcher) DispatchReady(ctx context.Context) error {
	if d.journal == nil {
		return fmt.Errorf("journal is required")
	}
	deliveries, err := d.journal.Pending(ctx, journal.PendingFilter{Flow: d.config.Flow, Now: time.Now().UTC(), Limit: d.config.BatchSize})
	if err != nil {
		return err
	}
	if len(deliveries) == 0 {
		return nil
	}
	workers := d.config.WorkerConcurrency
	if workers > len(deliveries) {
		workers = len(deliveries)
	}
	work := make(chan journal.Delivery)
	errs := make(chan error, len(deliveries))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for delivery := range work {
				if err := d.dispatchOne(ctx, delivery); err != nil {
					errs <- err
				}
			}
		}()
	}
	for _, delivery := range deliveries {
		select {
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return ctx.Err()
		case work <- delivery:
		}
	}
	close(work)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (d *Dispatcher) dispatchOne(ctx context.Context, delivery journal.Delivery) error {
	emitter := d.destinations[delivery.Destination]
	if emitter == nil {
		return d.journal.MarkFailed(ctx, delivery.RecordID, delivery.Destination, fmt.Errorf("destination %s is not configured", delivery.Destination), true, time.Time{})
	}
	next := d.nextAttempt(delivery.AttemptCount + 1)
	if err := d.journal.MarkAttempt(ctx, delivery.RecordID, delivery.Destination, next); err != nil {
		return err
	}
	record, err := d.journal.Get(ctx, delivery.RecordID)
	if err != nil {
		return err
	}
	dispatchCtx := ctx
	cancel := func() {}
	if d.config.DispatchTimeout > 0 {
		dispatchCtx, cancel = context.WithTimeout(ctx, d.config.DispatchTimeout)
	}
	defer cancel()
	err = emitter.Emit(dispatchCtx, record.Event)
	metrics.Inc("eventflow_dispatch_attempts_total", map[string]string{"flow": d.config.Flow, "emitter": string(delivery.Destination)})
	if err == nil {
		metrics.Inc("eventflow_dispatch_delivered_total", map[string]string{"flow": d.config.Flow, "emitter": string(delivery.Destination)})
		return d.journal.MarkDelivered(ctx, delivery.RecordID, delivery.Destination)
	}
	metrics.Inc("eventflow_dispatch_failures_total", map[string]string{"flow": d.config.Flow, "emitter": string(delivery.Destination)})
	attempts := delivery.AttemptCount + 1
	terminal := d.config.MaxAttempts > 0 && attempts >= d.config.MaxAttempts
	if terminal {
		next = time.Time{}
	}
	return d.journal.MarkFailed(ctx, delivery.RecordID, delivery.Destination, err, terminal, next)
}

func (d *Dispatcher) nextAttempt(attempt int) time.Time {
	delay := d.config.InitialRetryDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= d.config.MaxRetryDelay {
			delay = d.config.MaxRetryDelay
			break
		}
	}
	if delay <= 0 {
		delay = time.Second
	}
	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	return time.Now().UTC().Add(delay + jitter)
}

func (d *Dispatcher) drain() error {
	if d.config.ShutdownDrainTimeout <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.config.ShutdownDrainTimeout)
	defer cancel()
	for {
		if err := d.DispatchReady(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		deliveries, err := d.journal.Pending(ctx, journal.PendingFilter{Flow: d.config.Flow, Now: time.Now().UTC(), Limit: 1})
		if err != nil {
			return err
		}
		if len(deliveries) == 0 {
			return nil
		}
	}
}

func normalize(config Config) Config {
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 5
	}
	if config.InitialRetryDelay <= 0 {
		config.InitialRetryDelay = time.Second
	}
	if config.MaxRetryDelay <= 0 {
		config.MaxRetryDelay = 30 * time.Second
	}
	if config.WorkerConcurrency <= 0 {
		config.WorkerConcurrency = 4
	}
	if config.DispatchTimeout <= 0 {
		config.DispatchTimeout = 30 * time.Second
	}
	if config.ShutdownDrainTimeout <= 0 {
		config.ShutdownDrainTimeout = 30 * time.Second
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	return config
}
