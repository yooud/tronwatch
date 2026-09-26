package publish

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"tronwatch/internal/model"
)

type outbox interface {
	Pending(destination string, limit int) ([]model.Event, error)
	Ack(destination, eventID string) error
	AckBatch(destination string, eventIDs []string) error
}

type batchPublisher interface {
	PublishBatch(context.Context, []Event) error
}

type Observer interface {
	PublisherSuccess(count int)
	PublisherFailure()
}

// Dispatcher drains one publisher's durable outbox independently.
type Dispatcher struct {
	queue     outbox
	publisher Publisher
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
	observer  Observer
}

// SetObserver adds operational counters without affecting delivery semantics.
func (d *Dispatcher) SetObserver(observer Observer) { d.observer = observer }

func NewDispatcher(queue outbox, publisher Publisher, interval time.Duration, batchSize int, logger *slog.Logger) (*Dispatcher, error) {
	if queue == nil || publisher == nil {
		return nil, errors.New("dispatcher queue and publisher are required")
	}
	if interval <= 0 || batchSize <= 0 {
		return nil, errors.New("dispatcher interval and batch size must be positive")
	}
	return &Dispatcher{queue: queue, publisher: publisher, interval: interval, batchSize: batchSize, logger: logger}, nil
}

// DispatchOnce attempts one bounded outbox batch and stops at the first failed delivery.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	events, err := d.queue.Pending(d.publisher.Name(), d.batchSize)
	if err != nil {
		return 0, fmt.Errorf("loading %q outbox: %w", d.publisher.Name(), err)
	}
	if len(events) == 0 {
		return 0, nil
	}
	if publisher, ok := d.publisher.(batchPublisher); ok {
		if err := publisher.PublishBatch(ctx, events); err != nil {
			return 0, fmt.Errorf("publishing batch to %q: %w", d.publisher.Name(), err)
		}
		eventIDs := make([]string, len(events))
		for index, event := range events {
			eventIDs[index] = event.ID
		}
		if err := d.queue.AckBatch(d.publisher.Name(), eventIDs); err != nil {
			return 0, fmt.Errorf("acknowledging batch for %q: %w", d.publisher.Name(), err)
		}
		return len(events), nil
	}
	processed := 0
	for _, event := range events {
		if err := d.publisher.Publish(ctx, event); err != nil {
			return processed, fmt.Errorf("publishing event %s to %q: %w", event.ID, d.publisher.Name(), err)
		}
		if err := d.queue.Ack(d.publisher.Name(), event.ID); err != nil {
			return processed, fmt.Errorf("acknowledging event %s for %q: %w", event.ID, d.publisher.Name(), err)
		}
		processed++
	}
	return processed, nil
}

// Run retries pending events until cancellation. Destination failures never stop P2P intake.
func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		processed, err := d.DispatchOnce(ctx)
		if d.observer != nil {
			if err != nil && !errors.Is(err, context.Canceled) {
				d.observer.PublisherFailure()
			} else if processed > 0 {
				d.observer.PublisherSuccess(processed)
			}
		}
		if err != nil && !errors.Is(err, context.Canceled) && d.logger != nil {
			d.logger.Warn("publisher delivery deferred", "publisher", d.publisher.Name(), "error", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err == nil && processed == d.batchSize {
			continue
		}
		timer := time.NewTimer(d.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
