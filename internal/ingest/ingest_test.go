package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/yooud/tronwatch/internal/filter"
	"github.com/yooud/tronwatch/internal/model"
	"github.com/yooud/tronwatch/internal/p2p"
	"github.com/yooud/tronwatch/internal/protocol"
)

func TestHandleTransactionStoresExactMatchedRecord(t *testing.T) {
	owner := mustAddress(t, "411111111111111111111111111111111111111111")
	recipient := mustAddress(t, "412222222222222222222222222222222222222222")
	parameter, err := anypb.New(&protocol.TransferContract{
		OwnerAddress: owner,
		ToAddress:    recipient,
		Amount:       10,
	})
	if err != nil {
		t.Fatalf("anypb.New() error = %v", err)
	}
	transaction := &protocol.Transaction{RawData: &protocol.TransactionRaw{
		Timestamp: 1_700_000_000_000,
		Contract:  []*protocol.TransactionContract{{Parameter: parameter}},
	}}
	watches, err := filter.NewSet([]string{hex.EncodeToString(recipient)}, nil)
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}
	sink := &recordingStore{}
	seenAt := time.UnixMilli(1_700_000_001_000).UTC()
	ingestor := New(staticWatches{set: watches}, sink, func() time.Time { return seenAt })
	blockNumber := int64(70_000_001)
	blockTime := time.UnixMilli(1_700_000_003_000).UTC()

	err = ingestor.HandleTransaction(context.Background(), transaction, p2p.Observation{
		Source:      p2p.SourceBlock,
		Peer:        "127.0.0.1:18888",
		ObservedAt:  seenAt.Add(-time.Second),
		BlockNumber: &blockNumber,
		BlockTime:   &blockTime,
	})
	if err != nil {
		t.Fatalf("HandleTransaction() error = %v", err)
	}
	if len(sink.records) != 1 {
		t.Fatalf("stored records = %d, want 1", len(sink.records))
	}
	rawData, err := proto.Marshal(transaction.RawData)
	if err != nil {
		t.Fatalf("Marshal(raw data) error = %v", err)
	}
	wantID := sha256.Sum256(rawData)
	got := sink.records[0]
	if got.TxID != hex.EncodeToString(wantID[:]) {
		t.Fatalf("TxID = %q, want %x", got.TxID, wantID)
	}
	if got.Source != string(p2p.SourceBlock) || got.BlockNumber == nil || *got.BlockNumber != blockNumber {
		t.Fatalf("record observation = %+v, want confirmed block", got)
	}
	if !got.FirstSeen.Equal(seenAt.Add(-time.Second)) {
		t.Fatalf("FirstSeen = %v, want observation timestamp", got.FirstSeen)
	}
	if got.ConfirmedSeen == nil || !got.ConfirmedSeen.Equal(seenAt.Add(-time.Second)) {
		t.Fatalf("ConfirmedSeen = %v, want observation timestamp", got.ConfirmedSeen)
	}
	if len(got.Matches) != 1 || got.Matches[0].Role != filter.RoleRecipient {
		t.Fatalf("Matches = %+v, want recipient", got.Matches)
	}
	if len(got.Peers) != 1 || got.Peers[0] != "127.0.0.1:18888" {
		t.Fatalf("Peers = %v, want observation peer", got.Peers)
	}
}

func TestHandleTransactionSkipsUnmatched(t *testing.T) {
	watches, err := filter.NewSet([]string{"413333333333333333333333333333333333333333"}, nil)
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}
	sink := &recordingStore{}
	ingestor := New(staticWatches{set: watches}, sink, time.Now)
	transaction := &protocol.Transaction{RawData: &protocol.TransactionRaw{}}

	if err := ingestor.HandleTransaction(
		context.Background(),
		transaction,
		p2p.Observation{Source: p2p.SourceMempool},
	); err != nil {
		t.Fatalf("HandleTransaction() error = %v", err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("stored records = %d, want 0", len(sink.records))
	}
}

func TestHandleTransactionRejectsMissingRawData(t *testing.T) {
	watches, err := filter.NewSet(nil, nil)
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}
	ingestor := New(staticWatches{set: watches}, &recordingStore{}, time.Now)
	if err := ingestor.HandleTransaction(
		context.Background(),
		&protocol.Transaction{},
		p2p.Observation{Source: p2p.SourceMempool},
	); err == nil {
		t.Fatal("HandleTransaction() error = nil, want malformed transaction error")
	}
}

func TestHandleTransactionConfirmsPreviouslyMatchedAfterWatchRemoval(t *testing.T) {
	recipient := "412222222222222222222222222222222222222222"
	transaction := testTransfer(t, "411111111111111111111111111111111111111111", recipient)
	initial, err := filter.NewSet([]string{recipient}, nil)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := filter.NewSet(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	watches := &mutableWatches{set: initial}
	sink := &recordingStore{}
	ingestor := New(watches, sink, time.Now)
	if err := ingestor.HandleTransaction(context.Background(), transaction, p2p.Observation{Source: p2p.SourceMempool}); err != nil {
		t.Fatal(err)
	}
	watches.set = empty
	block := int64(42)
	if err := ingestor.HandleTransaction(context.Background(), transaction, p2p.Observation{Source: p2p.SourceBlock, BlockNumber: &block}); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 2 || sink.records[1].ConfirmedSeen == nil {
		t.Fatalf("records = %+v, want a confirmation update", sink.records)
	}
}

func TestHandleBlockPersistsOneAtomicBoundaryIncludingEmptyBlock(t *testing.T) {
	watches, err := filter.NewSet([]string{"413333333333333333333333333333333333333333"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingStore{}
	ingestor := New(staticWatches{set: watches}, sink, time.Now)
	observation := p2p.BlockObservation{
		ID: "block42", ParentID: "block41", Number: 42,
		Time: time.Unix(42, 0).UTC(), ObservedAt: time.Unix(43, 0).UTC(), Peer: "peer-a:18888",
	}
	if err := ingestor.HandleBlock(context.Background(), observation, nil); err != nil {
		t.Fatal(err)
	}
	if len(sink.blocks) != 1 || sink.blocks[0].ID != "block42" || len(sink.blockRecords[0]) != 0 {
		t.Fatalf("stored blocks = %+v records = %+v", sink.blocks, sink.blockRecords)
	}
}

type staticWatches struct {
	set *filter.Set
}

func (s staticWatches) Current() *filter.Set {
	return s.set
}

type recordingStore struct {
	records      []model.Record
	blocks       []model.Block
	blockRecords [][]model.Record
}

func (s *recordingStore) Put(record model.Record) error {
	s.records = append(s.records, record)
	return nil
}

func (s *recordingStore) Exists(txID string) (bool, error) {
	for _, record := range s.records {
		if record.TxID == txID {
			return true, nil
		}
	}
	return false, nil
}

func (s *recordingStore) ApplyBlock(block model.Block, records []model.Record) (model.ChainUpdate, error) {
	s.blocks = append(s.blocks, block)
	s.blockRecords = append(s.blockRecords, append([]model.Record(nil), records...))
	return model.ChainUpdate{BlockID: block.ID, Canonical: true}, nil
}

type mutableWatches struct{ set *filter.Set }

func (w *mutableWatches) Current() *filter.Set { return w.set }

func testTransfer(t *testing.T, owner, recipient string) *protocol.Transaction {
	t.Helper()
	parameter, err := anypb.New(&protocol.TransferContract{
		OwnerAddress: mustAddress(t, owner), ToAddress: mustAddress(t, recipient), Amount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &protocol.Transaction{RawData: &protocol.TransactionRaw{
		Contract: []*protocol.TransactionContract{{Parameter: parameter}},
	}}
}

func mustAddress(t *testing.T, value string) []byte {
	t.Helper()
	address, err := filter.ParseAddress(value)
	if err != nil {
		t.Fatalf("ParseAddress() error = %v", err)
	}
	return address[:]
}
