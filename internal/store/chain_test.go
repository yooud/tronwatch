package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yooud/tronwatch/internal/model"
)

func TestApplyBlockReorgsOnlyToLongerConnectedBranch(t *testing.T) {
	db := openChainTestDB(t)
	seen := time.Unix(1_700_000_000, 0).UTC()

	applyBlock(t, db, testBlock("a10", "a09", 10, seen), testRecord("anchor", seen))
	applyBlock(t, db, testBlock("a11", "a10", 11, seen.Add(3*time.Second)),
		testRecord("removed", seen), testRecord("shared", seen))
	update := applyBlock(t, db, testBlock("b11", "a10", 11, seen.Add(4*time.Second)),
		testRecord("replacement", seen), testRecord("shared", seen))
	if update.Reorg || update.Canonical {
		t.Fatalf("equal-height fork update = %+v, want stored candidate", update)
	}

	update = applyBlock(t, db, testBlock("b12", "b11", 12, seen.Add(7*time.Second)))
	if !update.Reorg || update.ReorgDepth != 1 || update.OldTipID != "a11" || update.NewTipID != "b12" {
		t.Fatalf("longer fork update = %+v", update)
	}
	removed, err := db.Get("removed")
	if err != nil {
		t.Fatal(err)
	}
	if removed.ChainState != model.ChainStateOrphaned || removed.BlockID != "a11" {
		t.Fatalf("removed transaction = %+v, want orphaned from a11", removed)
	}
	replacement, err := db.Get("replacement")
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ChainState != model.ChainStateIncluded || replacement.BlockID != "b11" {
		t.Fatalf("replacement transaction = %+v, want included in b11", replacement)
	}
	shared, err := db.Get("shared")
	if err != nil {
		t.Fatal(err)
	}
	if shared.ChainState != model.ChainStateIncluded || shared.BlockID != "b11" || shared.Revision < 2 {
		t.Fatalf("shared transaction = %+v, want reincluded in b11", shared)
	}

	events, err := db.Pending("hook", 100)
	if err != nil {
		t.Fatal(err)
	}
	assertEventType(t, events, "removed", "tron.transaction.orphaned.v2")
	assertEventType(t, events, "replacement", "tron.transaction.included.v2")
	assertEventType(t, events, "shared", "tron.transaction.reincluded.v2")
}

func TestFinalizeRequiresExactCanonicalSolidBlockAndSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.db")
	db, err := OpenWithOptions(path, Options{Destinations: []string{"hook"}})
	if err != nil {
		t.Fatal(err)
	}
	seen := time.Unix(1_700_000_000, 0).UTC()
	applyBlock(t, db, testBlock("a10", "a09", 10, seen), testRecord("tx10", seen))
	applyBlock(t, db, testBlock("a11", "a10", 11, seen.Add(3*time.Second)), testRecord("tx11", seen))

	if _, err := db.Finalize(model.SolidBlock{ID: "wrong", Number: 11, ObservedAt: seen.Add(time.Minute)}); !errors.Is(err, ErrSolidBlockMismatch) {
		t.Fatalf("Finalize(wrong hash) error = %v, want ErrSolidBlockMismatch", err)
	}
	result, err := db.Finalize(model.SolidBlock{ID: "a11", Number: 11, ObservedAt: seen.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalizedBlocks != 2 || result.FinalizedTransactions != 2 {
		t.Fatalf("Finalize() = %+v, want two blocks and transactions", result)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenWithOptions(path, Options{Destinations: []string{"hook"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	status, err := db.ChainStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.TipID != "a11" || status.TipNumber != 11 || status.FinalizedID != "a11" || status.FinalizedNumber != 11 {
		t.Fatalf("status after reopen = %+v", status)
	}
	record, err := db.Get("tx11")
	if err != nil {
		t.Fatal(err)
	}
	if record.ChainState != model.ChainStateFinalized || record.FinalizedSeen == nil {
		t.Fatalf("finalized record = %+v", record)
	}
	update := applyBlock(t, db, testBlock("evil12", "fork10", 12, seen.Add(2*time.Minute)))
	if !update.Unresolved {
		t.Fatalf("disconnected future block update = %+v, want unresolved", update)
	}
}

func TestCompactLifecyclePayloadKeepsIncludedFullAndCompactsFinalized(t *testing.T) {
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "transactions.db"), Options{
		Destinations: []string{"hook"}, EventPayloadMode: model.EventPayloadCompactLifecycle,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	seen := time.Unix(1_700_000_000, 0).UTC()
	record := testRecord("tx10", seen)
	record.Peers = []string{"peer-a:18888"}
	applyBlock(t, db, testBlock("a10", "a09", 10, seen), record)
	if _, err := db.Finalize(model.SolidBlock{ID: "a10", Number: 10, ObservedAt: seen.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	events, err := db.Pending("hook", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("Pending() returned %d events, want included and finalized", len(events))
	}
	if events[0].Type != "tron.transaction.included.v2" || len(events[0].Record.RawTransaction) == 0 || len(events[0].Record.Peers) == 0 {
		t.Fatalf("included event was compacted: %+v", events[0])
	}
	if events[1].Type != "tron.transaction.finalized.v2" || len(events[1].Record.RawTransaction) != 0 || len(events[1].Record.Peers) != 0 {
		t.Fatalf("finalized event was not compacted: %+v", events[1])
	}
}

func TestReorgBelowFinalizedCheckpointIsRejected(t *testing.T) {
	db := openChainTestDB(t)
	seen := time.Unix(1_700_000_000, 0).UTC()
	applyBlock(t, db, testBlock("a10", "a09", 10, seen))
	applyBlock(t, db, testBlock("a11", "a10", 11, seen.Add(3*time.Second)))
	applyBlock(t, db, testBlock("b11", "a10", 11, seen.Add(4*time.Second)))
	if _, err := db.Finalize(model.SolidBlock{ID: "a11", Number: 11, ObservedAt: seen.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	_, err := db.ApplyBlock(testBlock("b12", "b11", 12, seen.Add(7*time.Second)), nil)
	if !errors.Is(err, ErrFinalizedFork) {
		t.Fatalf("ApplyBlock(finalized fork) error = %v, want ErrFinalizedFork", err)
	}
}

func TestFinalizePrunesPayloadsAndKeepsCanonicalIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.db")
	db, err := OpenWithOptions(path, Options{
		Destinations: []string{"hook"}, FinalizedRetentionBlocks: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	seen := time.Unix(1_700_000_000, 0).UTC()
	parent := "a09"
	for height := int64(10); height <= 14; height++ {
		id := fmt.Sprintf("a%d", height)
		applyBlock(t, db, testBlock(id, parent, height, seen.Add(time.Duration(height-10)*3*time.Second)),
			testRecord(fmt.Sprintf("tx%d", height), seen))
		if height == 11 {
			applyBlock(t, db, testBlock("b11", "a10", 11, seen.Add(4*time.Second)), testRecord("fork-tx", seen))
		}
		parent = id
	}

	result, err := db.Finalize(model.SolidBlock{ID: "a14", Number: 14, ObservedAt: seen.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalizedBlocks != 5 || result.FinalizedTransactions != 5 || result.PrunedBlocks != 4 || result.PrunedTransactions != 3 {
		t.Fatalf("Finalize() = %+v, want finalized 5/5 and pruned 4/3", result)
	}
	for _, txID := range []string{"tx10", "tx11", "tx12"} {
		if _, err := db.Get(txID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%s) error = %v, want ErrNotFound", txID, err)
		}
	}
	for _, txID := range []string{"tx13", "tx14"} {
		record, err := db.Get(txID)
		if err != nil || record.ChainState != model.ChainStateFinalized {
			t.Fatalf("Get(%s) = %+v, %v, want retained finalized record", txID, record, err)
		}
	}
	events, err := db.Pending("hook", 100)
	if err != nil {
		t.Fatal(err)
	}
	assertEventType(t, events, "tx10", "tron.transaction.finalized.v2")

	if err := db.db.View(func(tx *bolt.Tx) error {
		if got := string(tx.Bucket(canonicalBucket).Get(heightKey(10))); got != "a10" {
			t.Fatalf("canonical block 10 = %q, want a10", got)
		}
		if tx.Bucket(blocksBucket).Get([]byte("a10")) != nil || tx.Bucket(blocksBucket).Get([]byte("b11")) != nil {
			t.Fatal("pruned canonical or fork payload remains")
		}
		if tx.Bucket(candidatesBucket).Get([]byte("b11")) != nil {
			t.Fatal("pruned fork remains in candidates index")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	before := db.db.Stats()
	update := applyBlock(t, db, testBlock("a10", "a09", 10, seen), testRecord("tx10", seen))
	after := db.db.Stats()
	delta := after.Sub(&before)
	if !update.Duplicate || !update.Canonical || delta.TxStats.GetWrite() != 0 {
		t.Fatalf("pruned canonical duplicate = %+v, writes=%d", update, delta.TxStats.GetWrite())
	}
	if _, err := db.ApplyBlock(testBlock("evil10", "a09", 10, seen), nil); !errors.Is(err, ErrFinalizedFork) {
		t.Fatalf("ApplyBlock(finalized historical fork) error = %v, want ErrFinalizedFork", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenWithOptions(path, Options{Destinations: []string{"hook"}, FinalizedRetentionBlocks: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	update = applyBlock(t, db, testBlock("a15", "a14", 15, seen.Add(15*time.Second)), testRecord("tx15", seen))
	if !update.Canonical {
		t.Fatalf("post-reopen extension = %+v, want canonical", update)
	}
}

func TestDuplicateCanonicalBlockSkipsBoltWrite(t *testing.T) {
	db := openChainTestDB(t)
	seen := time.Unix(1_700_000_000, 0).UTC()
	block := testBlock("a10", "a09", 10, seen)
	record := testRecord("tx10", seen)
	applyBlock(t, db, block, record)
	before := db.db.Stats()
	update := applyBlock(t, db, block, record)
	after := db.db.Stats()
	delta := after.Sub(&before)
	if !update.Duplicate || !update.Canonical {
		t.Fatalf("duplicate update = %+v", update)
	}
	if writes := delta.TxStats.GetWrite(); writes != 0 {
		t.Fatalf("duplicate Bolt writes = %d, want 0", writes)
	}
}

func TestApplyBlockRollsBackChainRecordAndOutboxTogether(t *testing.T) {
	db := openChainTestDB(t)
	if err := db.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(outboxBucket).DeleteBucket([]byte("hook"))
	}); err != nil {
		t.Fatal(err)
	}
	seen := time.Unix(1_700_000_000, 0).UTC()
	if _, err := db.ApplyBlock(testBlock("a10", "a09", 10, seen), []model.Record{testRecord("tx10", seen)}); err == nil {
		t.Fatal("ApplyBlock() error = nil, want missing outbox failure")
	}
	status, err := db.ChainStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Anchored {
		t.Fatalf("chain anchored after rolled-back transaction: %+v", status)
	}
	if _, err := db.Get("tx10"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(tx10) error = %v, want ErrNotFound", err)
	}
}

func openChainTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "transactions.db"), Options{Destinations: []string{"hook"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testBlock(id, parent string, number int64, observedAt time.Time) model.Block {
	return model.Block{ID: id, ParentID: parent, Number: number, Time: observedAt, ObservedAt: observedAt, Peer: "peer-a:18888"}
}

func testRecord(txID string, seen time.Time) model.Record {
	return model.Record{TxID: txID, FirstSeen: seen, Source: "block", RawTransaction: []byte(txID)}
}

func applyBlock(t *testing.T, db *DB, block model.Block, records ...model.Record) model.ChainUpdate {
	t.Helper()
	update, err := db.ApplyBlock(block, records)
	if err != nil {
		t.Fatalf("ApplyBlock(%s) error = %v", block.ID, err)
	}
	return update
}

func assertEventType(t *testing.T, events []model.Event, txID, eventType string) {
	t.Helper()
	for _, event := range events {
		if event.Record.TxID == txID && event.Type == eventType {
			return
		}
	}
	t.Fatalf("missing %s for %s in %+v", eventType, txID, events)
}
