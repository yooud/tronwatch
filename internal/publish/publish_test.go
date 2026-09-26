package publish

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tronwatch/internal/model"
)

func TestEventIDIsStablePerObservation(t *testing.T) {
	block := int64(10)
	record := model.Record{TxID: "abc", Source: "block", BlockNumber: &block}
	first := NewEvent(record, time.Unix(10, 0))
	second := NewEvent(record, time.Unix(20, 0))
	if first.ID != second.ID || first.ID == "" {
		t.Fatalf("event IDs = %q and %q", first.ID, second.ID)
	}
}

func TestJSONLPublisherAppendsEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	publisher, err := NewJSONL("archive", path)
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	event := NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Unix(10, 0))
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	if err := json.Unmarshal(bytes.TrimSpace(data), &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.ID != event.ID || got.Record.TxID != "abc" {
		t.Fatalf("event = %+v", got)
	}
}

func TestJSONLPublisherWritesBatchInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	publisher, err := NewJSONL("archive", path)
	if err != nil {
		t.Fatal(err)
	}
	batch, ok := publisher.(interface {
		PublishBatch(context.Context, []Event) error
	})
	if !ok {
		t.Fatal("JSONL publisher does not support batch publication")
	}
	events := []Event{
		NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Unix(10, 0)),
		NewEvent(model.Record{TxID: "def", Source: "mempool"}, time.Unix(11, 0)),
	}
	if err := batch.PublishBatch(context.Background(), events); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	for index, want := range events {
		var got Event
		if err := decoder.Decode(&got); err != nil {
			t.Fatalf("Decode(event %d) error = %v", index, err)
		}
		if got.ID != want.ID {
			t.Fatalf("event %d ID = %q, want %q", index, got.ID, want.ID)
		}
	}
}

func TestJSONLPublisherRejectsCanceledBatchWithoutWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	publisher, err := NewJSONL("archive", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })
	batch := publisher.(interface {
		PublishBatch(context.Context, []Event) error
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = batch.PublishBatch(ctx, []Event{NewEvent(model.Record{TxID: "abc"}, time.Now())})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishBatch() error = %v, want context.Canceled", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("JSONL size = %d, want 0", info.Size())
	}
}

func TestJSONLPublisherRotatesAndBoundsFileCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	publisher, err := NewRotatingJSONL("archive", path, 220, 2)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 8; index++ {
		event := NewEvent(model.Record{TxID: strings.Repeat(fmt.Sprintf("%x", index), 64), Source: "mempool"}, time.Unix(int64(index+1), 0))
		if err := publisher.Publish(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{path, path + ".1", path + ".2"} {
		if _, err := os.Stat(expected); err != nil {
			t.Fatalf("expected rotated file %s: %v", expected, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected unbounded rotation file: %v", err)
	}
}

func TestWebhookRetriesServerFailure(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		if requests == 1 {
			http.Error(response, "retry", http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	publisher, err := NewWebhook("hook", server.URL, "", time.Second, 2, time.Millisecond, true)
	if err != nil {
		t.Fatalf("NewWebhook() error = %v", err)
	}
	if err := publisher.Publish(context.Background(), NewEvent(model.Record{TxID: "abc"}, time.Now())); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestRedisStreamPublishesStableEnvelope(t *testing.T) {
	writer := &fakeStreamWriter{}
	publisher := &redisStreamPublisher{name: "stream", stream: "tronwatch:events", writer: writer}
	event := NewEvent(model.Record{TxID: "abc", Source: "mempool"}, time.Now())
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if writer.stream != "tronwatch:events" || writer.values["event_id"] != event.ID {
		t.Fatalf("stream=%q values=%v", writer.stream, writer.values)
	}
}

type fakeStreamWriter struct {
	stream string
	values map[string]any
}

func (w *fakeStreamWriter) Add(_ context.Context, stream string, values map[string]any) error {
	w.stream, w.values = stream, values
	return nil
}
func (w *fakeStreamWriter) Close() error { return nil }

type stubPublisher struct {
	name string
	err  error
}

func (s *stubPublisher) Name() string                         { return s.name }
func (s *stubPublisher) Publish(context.Context, Event) error { return s.err }
func (s *stubPublisher) Close() error                         { return nil }
