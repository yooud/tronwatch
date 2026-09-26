package publish

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"tronwatch/internal/model"
	"tronwatch/internal/store"
)

func TestDispatcherAcknowledgesOnlySuccessfulDelivery(t *testing.T) {
	event := NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Now())
	queue := &stubQueue{events: []Event{event}}
	publisher := &stubPublisher{name: "hook"}
	dispatcher, err := NewDispatcher(queue, publisher, time.Millisecond, 10, nil)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	processed, err := dispatcher.DispatchOnce(context.Background())
	if err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if processed != 1 || len(queue.acked) != 1 || queue.acked[0] != event.ID {
		t.Fatalf("processed=%d acked=%v", processed, queue.acked)
	}
}

func TestDispatcherLeavesFailedDeliveryPending(t *testing.T) {
	event := NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Now())
	queue := &stubQueue{events: []Event{event}}
	publisher := &stubPublisher{name: "hook", err: errors.New("down")}
	dispatcher, err := NewDispatcher(queue, publisher, time.Millisecond, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.DispatchOnce(context.Background()); err == nil {
		t.Fatal("DispatchOnce() error = nil, want delivery failure")
	}
	if len(queue.acked) != 0 {
		t.Fatalf("acked = %v, want none", queue.acked)
	}
}

func TestDispatcherDrainsDurableStoreIntoJSONL(t *testing.T) {
	directory := t.TempDir()
	database, err := store.OpenWithDestinations(filepath.Join(directory, "transactions.db"), []string{"archive"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.Put(model.Record{TxID: "abc", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewJSONL("archive", filepath.Join(directory, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })
	dispatcher, err := NewDispatcher(database, publisher, time.Millisecond, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := dispatcher.DispatchOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("DispatchOnce() = %d, %v", processed, err)
	}
	if pending, err := database.Pending("archive", 10); err != nil || len(pending) != 0 {
		t.Fatalf("Pending() = %d, %v", len(pending), err)
	}
}

func TestDispatcherPublishesAndAcknowledgesBatchOnce(t *testing.T) {
	events := []Event{
		NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Unix(10, 0)),
		NewEvent(model.Record{TxID: "def", Source: "mempool"}, time.Unix(11, 0)),
	}
	queue := &stubQueue{events: events}
	publisher := &stubBatchPublisher{stubPublisher: stubPublisher{name: "archive"}}
	dispatcher, err := NewDispatcher(queue, publisher, time.Millisecond, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := dispatcher.DispatchOnce(context.Background())
	if err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if processed != 2 || publisher.batchCalls != 1 || queue.batchAckCalls != 1 {
		t.Fatalf(
			"processed=%d publish batches=%d ack batches=%d; want 2, 1, 1",
			processed, publisher.batchCalls, queue.batchAckCalls,
		)
	}
	if want := []string{events[0].ID, events[1].ID}; !reflect.DeepEqual(queue.acked, want) {
		t.Fatalf("acked = %v, want %v", queue.acked, want)
	}
}

func TestDispatcherLeavesFailedBatchPending(t *testing.T) {
	events := []Event{
		NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Unix(10, 0)),
		NewEvent(model.Record{TxID: "def", Source: "mempool"}, time.Unix(11, 0)),
	}
	queue := &stubQueue{events: events}
	publisher := &stubBatchPublisher{
		stubPublisher: stubPublisher{name: "archive", err: errors.New("disk full")},
	}
	dispatcher, err := NewDispatcher(queue, publisher, time.Millisecond, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.DispatchOnce(context.Background()); err == nil {
		t.Fatal("DispatchOnce() error = nil, want batch failure")
	}
	if queue.batchAckCalls != 0 || len(queue.acked) != 0 {
		t.Fatalf("batch ack calls=%d acked=%v, want none", queue.batchAckCalls, queue.acked)
	}
}

func TestDispatcherRunDrainsFullBatchBeforeCancellation(t *testing.T) {
	event := NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Unix(10, 0))
	ctx, cancel := context.WithCancel(context.Background())
	queue := &runQueue{event: event, cancel: cancel}
	dispatcher, err := NewDispatcher(
		queue,
		&stubPublisher{name: "hook"},
		time.Hour,
		1,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if queue.pendingCalls != 2 {
		t.Fatalf("Pending() calls = %d, want 2", queue.pendingCalls)
	}
	if want := []string{event.ID}; !reflect.DeepEqual(queue.acked, want) {
		t.Fatalf("acked = %v, want %v", queue.acked, want)
	}
}

type stubQueue struct {
	events        []Event
	acked         []string
	batchAckCalls int
}

type runQueue struct {
	event        Event
	cancel       context.CancelFunc
	pendingCalls int
	acked        []string
}

func (q *runQueue) Pending(string, int) ([]model.Event, error) {
	q.pendingCalls++
	if q.pendingCalls == 1 {
		return []model.Event{q.event}, nil
	}
	q.cancel()
	return nil, nil
}
func (q *runQueue) Ack(_ string, eventID string) error {
	q.acked = append(q.acked, eventID)
	return nil
}
func (q *runQueue) AckBatch(_ string, eventIDs []string) error {
	q.acked = append(q.acked, eventIDs...)
	return nil
}

func (q *stubQueue) Pending(string, int) ([]model.Event, error) { return q.events, nil }
func (q *stubQueue) Ack(_ string, eventID string) error {
	q.acked = append(q.acked, eventID)
	return nil
}
func (q *stubQueue) AckBatch(_ string, eventIDs []string) error {
	q.batchAckCalls++
	q.acked = append(q.acked, eventIDs...)
	return nil
}

type stubBatchPublisher struct {
	stubPublisher
	batchCalls int
}

func (p *stubBatchPublisher) PublishBatch(_ context.Context, events []Event) error {
	p.batchCalls++
	if len(events) != 2 {
		return errors.New("unexpected batch size")
	}
	return p.err
}
