package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yooud/tronwatch/internal/filter"
	"github.com/yooud/tronwatch/internal/model"
)

func TestPutMergesMempoolAndBlockObservation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "transactions.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	firstSeen := time.UnixMilli(1_700_000_000_000).UTC()
	initial := model.Record{
		TxID:      "abc123",
		FirstSeen: firstSeen,
		Matches: []filter.Match{{
			Address: "411111111111111111111111111111111111111111",
			Role:    filter.RoleOwner,
		}},
		RawTransaction: []byte{0x0a, 0x00},
	}
	if err := db.Put(initial); err != nil {
		t.Fatalf("Put(initial) error = %v", err)
	}
	blockNumber := int64(70_000_001)
	blockTime := time.UnixMilli(1_700_000_003_000).UTC()
	confirmed := initial
	confirmed.BlockNumber = &blockNumber
	confirmed.BlockTime = &blockTime
	confirmedSeen := time.UnixMilli(1_700_000_003_250).UTC()
	confirmed.ConfirmedSeen = &confirmedSeen
	if err := db.Put(confirmed); err != nil {
		t.Fatalf("Put(confirmed) error = %v", err)
	}

	got, err := db.Get("abc123")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.BlockNumber == nil || *got.BlockNumber != blockNumber {
		t.Fatalf("BlockNumber = %v, want %d", got.BlockNumber, blockNumber)
	}
	if !got.FirstSeen.Equal(firstSeen) {
		t.Fatalf("FirstSeen = %v, want %v", got.FirstSeen, firstSeen)
	}
	if got.ConfirmedSeen == nil || !got.ConfirmedSeen.Equal(confirmedSeen) {
		t.Fatalf("ConfirmedSeen = %v, want %v", got.ConfirmedSeen, confirmedSeen)
	}
}

func TestConcurrentPeerDuplicatesProduceOneLogicalEvent(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "transactions.db"), Options{
		Destinations: []string{"hook"}, PersistAllPeers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}
	var wait sync.WaitGroup
	errorsChannel := make(chan error, 64)
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			duplicate := record
			duplicate.Peers = []string{[]string{"peer-a:18888", "peer-b:18888"}[index%2]}
			errorsChannel <- db.Put(duplicate)
		}(index)
	}
	wait.Wait()
	close(errorsChannel)
	for putErr := range errorsChannel {
		if putErr != nil {
			t.Fatalf("Put() error = %v", putErr)
		}
	}
	events, err := db.Pending("hook", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("pending events = %d, want 1", len(events))
	}
	blockNumber := int64(42)
	confirmedAt := time.Unix(12, 0).UTC()
	confirmErrors := make(chan error, 64)
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			confirmed := record
			confirmed.Source = "block"
			confirmed.BlockNumber = &blockNumber
			confirmed.ConfirmedSeen = &confirmedAt
			confirmed.Peers = []string{[]string{"peer-a:18888", "peer-b:18888"}[index%2]}
			confirmErrors <- db.Put(confirmed)
		}(index)
	}
	wait.Wait()
	close(confirmErrors)
	for putErr := range confirmErrors {
		if putErr != nil {
			t.Fatalf("Put(confirmed) error = %v", putErr)
		}
	}
	events, err = db.Pending("hook", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("pending lifecycle events = %d, want pending and confirmed", len(events))
	}
	got, err := db.Get(record.TxID)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"peer-a:18888", "peer-b:18888"}; !reflect.DeepEqual(got.Peers, want) {
		t.Fatalf("Peers = %v, want %v", got.Peers, want)
	}
}

func TestGetMissingRecord(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "transactions.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Get("missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() error = %v, want %v", err, ErrNotFound)
	}
}

func TestOpenRejectsDirectoryAsDatabase(t *testing.T) {
	_, err := Open(t.TempDir())
	if err == nil {
		t.Fatal("Open(directory) error = nil, want error")
	}
}

func TestOpenConfiguresFreelistSync(t *testing.T) {
	optimized, err := Open(filepath.Join(t.TempDir(), "optimized.db"))
	if err != nil {
		t.Fatal(err)
	}
	if !optimized.db.NoFreelistSync {
		t.Fatal("NoFreelistSync = false, want optimized default true")
	}
	if err := optimized.Close(); err != nil {
		t.Fatal(err)
	}

	synced, err := OpenWithOptions(filepath.Join(t.TempDir(), "synced.db"), Options{SyncFreelist: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = synced.Close() }()
	if synced.db.NoFreelistSync {
		t.Fatal("NoFreelistSync = true, want explicit freelist sync")
	}
}

func TestPutAtomicallyQueuesTransitionPerDestination(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook", "stream"})
	if err != nil {
		t.Fatalf("OpenWithDestinations() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := model.Record{
		TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool",
		Matches: []filter.Match{{Address: "411111111111111111111111111111111111111111", Role: filter.RoleOwner}},
	}
	if err := db.Put(record); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	for _, destination := range []string{"hook", "stream"} {
		events, err := db.Pending(destination, 10)
		if err != nil {
			t.Fatalf("Pending(%q) error = %v", destination, err)
		}
		if len(events) != 1 || events[0].Record.TxID != record.TxID {
			t.Fatalf("Pending(%q) = %+v, want one event", destination, events)
		}
	}
	events, _ := db.Pending("hook", 10)
	if err := db.Ack("hook", events[0].ID); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if remaining, _ := db.Pending("hook", 10); len(remaining) != 0 {
		t.Fatalf("hook pending = %d, want 0", len(remaining))
	}
	if remaining, _ := db.Pending("stream", 10); len(remaining) != 1 {
		t.Fatalf("stream pending = %d, want independent delivery state", len(remaining))
	}
}

func TestOutboxStoresOneSharedPayloadUntilEveryDestinationAcknowledges(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook", "stream"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool", RawTransaction: []byte{0x0a, 0x00}}
	if err := db.Put(record); err != nil {
		t.Fatal(err)
	}
	event := model.NewEvent(record, record.FirstSeen)
	key := outboxStorageKey(event)
	if err := db.db.View(func(tx *bolt.Tx) error {
		payloads := tx.Bucket(outboxPayloadsBucket)
		if payloads == nil || payloads.Stats().KeyN != 1 || payloads.Get(key) == nil {
			t.Fatalf("shared payload bucket = %v, want exactly one payload", payloads)
		}
		outbox := tx.Bucket(outboxBucket)
		for _, destination := range []string{"hook", "stream"} {
			if got := outbox.Bucket([]byte(destination)).Get(key); !bytes.Equal(got, outboxReferenceValue) {
				t.Fatalf("%s outbox value = %x, want reference %x", destination, got, outboxReferenceValue)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Ack("hook", event.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(outboxPayloadsBucket).Get(key) == nil {
			t.Fatal("shared payload removed before final destination ACK")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Ack("stream", event.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.db.View(func(tx *bolt.Tx) error {
		if got := tx.Bucket(outboxPayloadsBucket).Stats().KeyN; got != 0 {
			t.Fatalf("shared payload count = %d, want 0 after final ACK", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxSingleDestinationStoresPayloadInline(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool", RawTransaction: []byte{0x0a, 0x00}}
	if err := db.Put(record); err != nil {
		t.Fatal(err)
	}
	event := model.NewEvent(record, record.FirstSeen)
	if err := db.db.View(func(tx *bolt.Tx) error {
		if got := tx.Bucket(outboxPayloadsBucket).Stats().KeyN; got != 0 {
			t.Fatalf("shared payload count = %d, want inline fast path", got)
		}
		value := tx.Bucket(outboxBucket).Bucket([]byte("hook")).Get(outboxStorageKey(event))
		if len(value) == 0 || bytes.Equal(value, outboxReferenceValue) {
			t.Fatalf("single-destination value = %x, want inline JSON", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSharedPayloadRetainsReferenceForDestinationRemovedFromConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.db")
	db, err := OpenWithDestinations(path, []string{"hook", "stream"})
	if err != nil {
		t.Fatal(err)
	}
	record := model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}
	if err := db.Put(record); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenWithDestinations(path, []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	event := model.NewEvent(record, record.FirstSeen)
	key := outboxStorageKey(event)
	if err := db.Ack("hook", event.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(outboxPayloadsBucket).Get(key) == nil {
			t.Fatal("shared payload removed while persisted stream reference remains")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Ack("stream", event.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(outboxPayloadsBucket).Get(key) != nil {
			t.Fatal("shared payload remains after persisted stream reference ACK")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPendingReadsLegacyInlineOutboxPayload(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	event := model.NewEvent(model.Record{TxID: "legacy", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}, time.Unix(10, 0))
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(outboxBucket).Bucket([]byte("hook")).Put(outboxStorageKey(event), encoded)
	}); err != nil {
		t.Fatal(err)
	}
	events, err := db.Pending("hook", 10)
	if err != nil || len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("Pending() = %+v, %v; want legacy event %s", events, err, event.ID)
	}
}

func TestPendingRejectsMissingSharedPayload(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook", "stream"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}
	if err := db.Put(record); err != nil {
		t.Fatal(err)
	}
	event := model.NewEvent(record, record.FirstSeen)
	if err := db.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(outboxPayloadsBucket).Delete(outboxStorageKey(event))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pending("hook", 10); err == nil || !strings.Contains(err.Error(), "shared outbox payload") {
		t.Fatalf("Pending() error = %v, want missing shared payload error", err)
	}
}

func TestPutWithoutDestinationsDoesNotCreateOrphanOutboxPayload(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "transactions.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Put(model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}); err != nil {
		t.Fatal(err)
	}
	if err := db.db.View(func(tx *bolt.Tx) error {
		if got := tx.Bucket(outboxPayloadsBucket).Stats().KeyN; got != 0 {
			t.Fatalf("shared payload count = %d, want 0 without publishers", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPutDoesNotRequeueAcknowledgedNoOp(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}
	if err := db.Put(record); err != nil {
		t.Fatal(err)
	}
	events, _ := db.Pending("hook", 10)
	if err := db.Ack("hook", events[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(record); err != nil {
		t.Fatal(err)
	}
	if events, _ := db.Pending("hook", 10); len(events) != 0 {
		t.Fatalf("pending duplicate events = %d, want 0", len(events))
	}
}

func TestPutIfChangedSkipsExactDuplicateAndCanPersistPeerEnrichment(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "transactions.db"), Options{
		Destinations: []string{"hook"}, PersistAllPeers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := model.Record{
		TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool",
		Peers: []string{"peer-a:18888"},
	}
	changed, err := db.PutIfChanged(record)
	if err != nil || !changed {
		t.Fatalf("PutIfChanged(first) = %v, %v; want true, nil", changed, err)
	}
	changed, err = db.PutIfChanged(record)
	if err != nil || changed {
		t.Fatalf("PutIfChanged(duplicate) = %v, %v; want false, nil", changed, err)
	}
	enriched := record
	enriched.Peers = []string{"peer-b:18888"}
	changed, err = db.PutIfChanged(enriched)
	if err != nil || !changed {
		t.Fatalf("PutIfChanged(enriched) = %v, %v; want true, nil", changed, err)
	}
	got, err := db.Get(record.TxID)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"peer-a:18888", "peer-b:18888"}; !reflect.DeepEqual(got.Peers, want) {
		t.Fatalf("Peers = %v, want %v", got.Peers, want)
	}
	if events, err := db.Pending("hook", 10); err != nil || len(events) != 1 {
		t.Fatalf("Pending() = %d, %v; want one lifecycle event", len(events), err)
	}
}

func TestPutIfChangedSkipsPeerOnlyEnrichmentByDefault(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	pending := model.Record{
		TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool",
		Peers: []string{"peer-a:18888"},
	}
	if changed, err := db.PutIfChanged(pending); err != nil || !changed {
		t.Fatalf("PutIfChanged(first) = %v, %v; want true, nil", changed, err)
	}
	duplicate := pending
	duplicate.Peers = []string{"peer-b:18888"}
	if changed, err := db.PutIfChanged(duplicate); err != nil || changed {
		t.Fatalf("PutIfChanged(peer-only) = %v, %v; want false, nil", changed, err)
	}

	blockNumber := int64(42)
	confirmedAt := time.Unix(12, 0).UTC()
	confirmed := pending
	confirmed.Source = "block"
	confirmed.BlockNumber = &blockNumber
	confirmed.ConfirmedSeen = &confirmedAt
	confirmed.Peers = []string{"peer-b:18888"}
	if changed, err := db.PutIfChanged(confirmed); err != nil || !changed {
		t.Fatalf("PutIfChanged(confirmed) = %v, %v; want true, nil", changed, err)
	}
	confirmedDuplicate := confirmed
	confirmedDuplicate.Peers = []string{"peer-c:18888"}
	if changed, err := db.PutIfChanged(confirmedDuplicate); err != nil || changed {
		t.Fatalf("PutIfChanged(confirmed peer-only) = %v, %v; want false, nil", changed, err)
	}

	got, err := db.Get(pending.TxID)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"peer-a:18888", "peer-b:18888"}; !reflect.DeepEqual(got.Peers, want) {
		t.Fatalf("Peers = %v, want transition peers %v", got.Peers, want)
	}
	if events, err := db.Pending("hook", 10); err != nil || len(events) != 2 {
		t.Fatalf("Pending() = %d, %v; want pending and confirmed events", len(events), err)
	}
}

func TestAckBatchDeletesAllEventsAndRejectsEmptyID(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, txID := range []string{"abc123", "def456"} {
		if err := db.Put(model.Record{TxID: txID, FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := db.Pending("hook", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AckBatch("hook", []string{events[0].ID, events[1].ID}); err != nil {
		t.Fatalf("AckBatch() error = %v", err)
	}
	if remaining, err := db.Pending("hook", 10); err != nil || len(remaining) != 0 {
		t.Fatalf("Pending() = %d, %v; want empty", len(remaining), err)
	}
	if err := db.AckBatch("hook", []string{""}); err == nil {
		t.Fatal("AckBatch(empty ID) error = nil, want validation error")
	}
}

func TestOutboxPreservesMempoolBeforeBlockOrder(t *testing.T) {
	db, err := OpenWithDestinations(filepath.Join(t.TempDir(), "transactions.db"), []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initial := model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}
	if err := db.Put(initial); err != nil {
		t.Fatal(err)
	}
	block := int64(42)
	confirmedAt := time.Unix(12, 0).UTC()
	confirmed := initial
	confirmed.Source = "block"
	confirmed.BlockNumber = &block
	confirmed.ConfirmedSeen = &confirmedAt
	if err := db.Put(confirmed); err != nil {
		t.Fatal(err)
	}
	events, err := db.Pending("hook", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Record.Source != "mempool" || events[1].Record.Source != "block" {
		t.Fatalf("event order = %+v, want mempool then block", events)
	}
}

func TestPutDoesNotDowngradeConfirmedSourceOnLateMempoolObservation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "transactions.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	block := int64(42)
	confirmedAt := time.Unix(12, 0).UTC()
	confirmed := model.Record{
		TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "block",
		BlockNumber: &block, ConfirmedSeen: &confirmedAt,
	}
	if err := db.Put(confirmed); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(model.Record{TxID: "abc123", FirstSeen: time.Unix(13, 0).UTC(), Source: "mempool"}); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get("abc123")
	if err != nil {
		t.Fatal(err)
	}
	if got.Source != "block" || got.ConfirmedSeen == nil || got.ChainState != model.ChainStateLegacyUnverified {
		t.Fatalf("record = %+v, want confirmed block source", got)
	}
}

func TestOutboxSurvivesDatabaseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.db")
	db, err := OpenWithDestinations(path, []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(model.Record{TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenWithDestinations(path, []string{"hook"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	events, err := db.Pending("hook", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != "abc123:mempool:pending" {
		t.Fatalf("events after reopen = %+v", events)
	}
}

func BenchmarkPutIfChangedDuplicate(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "transactions.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	record := model.Record{
		TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool",
		Peers: []string{"peer-a:18888"}, RawTransaction: []byte{0x0a, 0x00},
	}
	if changed, err := db.PutIfChanged(record); err != nil || !changed {
		b.Fatalf("PutIfChanged(first) = %v, %v", changed, err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if changed, err := db.PutIfChanged(record); err != nil || changed {
			b.Fatalf("PutIfChanged(duplicate) = %v, %v", changed, err)
		}
	}
}

func BenchmarkOutboxFanout(b *testing.B) {
	for _, destinationCount := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("destinations-%d", destinationCount), func(b *testing.B) {
			destinations := make([]string, destinationCount)
			for index := range destinations {
				destinations[index] = fmt.Sprintf("destination-%d", index)
			}
			db, err := OpenWithOptions(filepath.Join(b.TempDir(), "transactions.db"), Options{Destinations: destinations})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = db.Close() })
			rawTransaction := make([]byte, 1024)

			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				record := model.Record{
					TxID: fmt.Sprintf("%064x", index), FirstSeen: time.Unix(int64(index+1), 0).UTC(),
					Source: "block", RawTransaction: rawTransaction, Revision: 1,
				}
				event := model.NewLifecycleEvent(record, model.TransitionIncluded, record.FirstSeen)
				if err := db.db.Update(func(tx *bolt.Tx) error { return db.enqueuePreparedEvent(tx, event) }); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			var payloadLeafBytes, destinationLeafBytes int
			if err := db.db.View(func(tx *bolt.Tx) error {
				payloadLeafBytes = tx.Bucket(outboxPayloadsBucket).Stats().LeafInuse
				outbox := tx.Bucket(outboxBucket)
				for _, destination := range destinations {
					destinationLeafBytes += outbox.Bucket([]byte(destination)).Stats().LeafInuse
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(payloadLeafBytes)/float64(b.N), "payload-leaf-B/event")
			b.ReportMetric(float64(destinationLeafBytes)/float64(b.N), "destination-leaf-B/event")
		})
	}
}

func BenchmarkLifecyclePersistence(b *testing.B) {
	db, err := OpenWithDestinations(filepath.Join(b.TempDir(), "transactions.db"), []string{"archive"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	rawTransaction := make([]byte, 256)
	statsBefore := db.db.Stats()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		txID := fmt.Sprintf("%064x", index)
		firstSeen := time.Unix(int64(index+1), 0).UTC()
		pending := model.Record{
			TxID: txID, FirstSeen: firstSeen, Source: "mempool",
			RawTransaction: rawTransaction,
		}
		for _, peer := range []string{"peer-a:18888", "peer-b:18888", "peer-c:18888"} {
			observation := pending
			observation.Peers = []string{peer}
			if err := db.Put(observation); err != nil {
				b.Fatal(err)
			}
		}

		blockNumber := int64(index + 1)
		confirmedSeen := firstSeen.Add(3 * time.Second)
		confirmed := pending
		confirmed.Source = "block"
		confirmed.BlockNumber = &blockNumber
		confirmed.ConfirmedSeen = &confirmedSeen
		for _, peer := range []string{"peer-a:18888", "peer-b:18888", "peer-c:18888"} {
			observation := confirmed
			observation.Peers = []string{peer}
			if err := db.Put(observation); err != nil {
				b.Fatal(err)
			}
		}

		events, err := db.Pending("archive", 2)
		if err != nil {
			b.Fatal(err)
		}
		if len(events) != 2 {
			b.Fatalf("Pending() returned %d events, want 2", len(events))
		}
		eventIDs := []string{events[0].ID, events[1].ID}
		if err := db.AckBatch("archive", eventIDs); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	statsAfter := db.db.Stats()
	statsDelta := statsAfter.Sub(&statsBefore)
	b.ReportMetric(float64(statsDelta.TxStats.GetWrite())/float64(b.N), "bolt-writes/op")
	b.ReportMetric(float64(statsDelta.TxStats.GetPageAlloc())/float64(b.N), "bolt-page-B/op")
}

func BenchmarkFinalizedRetention(b *testing.B) {
	for _, scenario := range []struct {
		name      string
		retention int64
	}{
		{name: "unbounded", retention: 0},
		{name: "bounded-32", retention: 32},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "transactions.db")
			db, err := OpenWithOptions(path, Options{
				Destinations: []string{"archive"}, FinalizedRetentionBlocks: scenario.retention,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = db.Close() })
			rawTransaction := make([]byte, 256)
			parent := "genesis"
			seen := time.Unix(1_700_000_000, 0).UTC()

			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				height := int64(index + 1)
				blockID := fmt.Sprintf("block-%08d", height)
				records := make([]model.Record, 8)
				for recordIndex := range records {
					records[recordIndex] = model.Record{
						TxID: fmt.Sprintf("%056x%08x", index, recordIndex), FirstSeen: seen,
						Source: "block", RawTransaction: rawTransaction,
					}
				}
				block := model.Block{
					ID: blockID, ParentID: parent, Number: height,
					Time: seen.Add(time.Duration(index) * 3 * time.Second), ObservedAt: seen,
				}
				if _, err := db.ApplyBlock(block, records); err != nil {
					b.Fatal(err)
				}
				if _, err := db.Finalize(model.SolidBlock{ID: blockID, Number: height, ObservedAt: seen}); err != nil {
					b.Fatal(err)
				}
				events, err := db.Pending("archive", 100)
				if err != nil || len(events) != 16 {
					b.Fatalf("Pending() = %d events, %v, want 16", len(events), err)
				}
				eventIDs := make([]string, len(events))
				for eventIndex := range events {
					eventIDs[eventIndex] = events[eventIndex].ID
				}
				if err := db.AckBatch("archive", eventIDs); err != nil {
					b.Fatal(err)
				}
				parent = blockID
			}
			b.StopTimer()

			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			liveRecords := 0
			if err := db.db.View(func(tx *bolt.Tx) error {
				liveRecords = tx.Bucket(transactionsBucket).Stats().KeyN
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(info.Size()), "db-size-B")
			b.ReportMetric(float64(liveRecords), "live-records")
		})
	}
}
