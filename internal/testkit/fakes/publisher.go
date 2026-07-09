// Package fakes provides in-memory test doubles for ports.
package fakes

import (
	"context"
	"fmt"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Publisher is an in-memory publisher test double.
type Publisher struct {
	PublisherName string
	OpenErr       error
	Err           error
	CloseErr      error
	PublishDelay  time.Duration
	PublishGate   <-chan struct{}
	Events        []cloudevents.Event
	Opened        bool
	Closed        bool
	Calls         []string
	mu            sync.Mutex
}

// Name returns the configured publisher name.
func (p *Publisher) Name() string {
	if p.PublisherName == "" {
		return "fake.publisher"
	}
	return p.PublisherName
}

// Open records that the publisher lifecycle was opened.
func (p *Publisher) Open(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Calls = append(p.Calls, "open")
	if p.OpenErr != nil {
		return p.OpenErr
	}
	p.Opened = true
	return nil
}

// Publish records a CloudEvent in memory.
func (p *Publisher) Publish(ctx context.Context, evt cloudevents.Event) error {
	if err := p.waitBeforePublish(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Calls = append(p.Calls, "publish")
	if p.Err != nil {
		return p.Err
	}
	p.Events = append(p.Events, evt)
	return nil
}

// waitBeforePublish applies optional blocking behavior for concurrency tests.
func (p *Publisher) waitBeforePublish(ctx context.Context) error {
	if p.PublishDelay > 0 {
		timer := time.NewTimer(p.PublishDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if p.PublishGate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.PublishGate:
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Close marks the publisher as closed.
func (p *Publisher) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Calls = append(p.Calls, "close")
	if p.CloseErr != nil {
		return p.CloseErr
	}
	p.Closed = true
	return nil
}

// Count returns the number of recorded CloudEvents.
func (p *Publisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.Events)
}

// SnapshotCalls returns a copy of the recorded lifecycle and publish calls.
func (p *Publisher) SnapshotCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.Calls))
	copy(out, p.Calls)
	return out
}

// FailingPublisher constructs a publisher that always returns an error.
func FailingPublisher(name string) *Publisher {
	return &Publisher{PublisherName: name, Err: fmt.Errorf("publisher failed")}
}

// BatchPublisher is an in-memory batch-capable publisher test double.
type BatchPublisher struct {
	Publisher
	Batches [][]cloudevents.Event
}

// PublishBatch records a batch of CloudEvents in memory.
func (p *BatchPublisher) PublishBatch(ctx context.Context, events []cloudevents.Event) error {
	if err := p.waitBeforePublish(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Calls = append(p.Calls, "publish_batch")
	if p.Err != nil {
		return p.Err
	}
	batch := make([]cloudevents.Event, len(events))
	copy(batch, events)
	p.Batches = append(p.Batches, batch)
	p.Events = append(p.Events, events...)
	return nil
}
