// Package ingest turns decoded P2P transactions into persistent matched records.
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/yooud/tronwatch/internal/filter"
	"github.com/yooud/tronwatch/internal/model"
	"github.com/yooud/tronwatch/internal/p2p"
	"github.com/yooud/tronwatch/internal/protocol"
)

type watchSource interface {
	Current() *filter.Set
}

type recordStore interface {
	Put(model.Record) error
	Exists(string) (bool, error)
}

type blockStore interface {
	ApplyBlock(model.Block, []model.Record) (model.ChainUpdate, error)
}

// Stats is a point-in-time transaction processing summary.
type Stats struct {
	Seen                   int64
	Matched                int64
	Blocks                 int64
	DuplicateBlocks        int64
	UnresolvedBlocks       int64
	Reorgs                 int64
	OrphanedTransactions   int64
	ReincludedTransactions int64
}

// Ingestor filters decoded transactions and stores matches idempotently.
type Ingestor struct {
	watches                watchSource
	store                  recordStore
	now                    func() time.Time
	seen                   atomic.Int64
	matched                atomic.Int64
	blocks                 atomic.Int64
	duplicateBlocks        atomic.Int64
	unresolvedBlocks       atomic.Int64
	reorgs                 atomic.Int64
	orphanedTransactions   atomic.Int64
	reincludedTransactions atomic.Int64
}

// HandleBlock filters a complete block before one atomic canonical-chain update.
func (i *Ingestor) HandleBlock(
	ctx context.Context,
	observation p2p.BlockObservation,
	transactions []*protocol.Transaction,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if i.watches == nil || i.store == nil {
		return errors.New("ingestor is not fully configured")
	}
	chain, ok := i.store.(blockStore)
	if !ok {
		return errors.New("record store does not support atomic blocks")
	}
	watches := i.watches.Current()
	if watches == nil {
		return errors.New("watch snapshot is nil")
	}
	records := make([]model.Record, 0)
	for _, transaction := range transactions {
		i.seen.Add(1)
		record, selected, err := i.blockRecord(transaction, observation, watches)
		if err != nil {
			return err
		}
		if selected {
			records = append(records, record)
		}
	}
	update, err := chain.ApplyBlock(model.Block{
		ID: observation.ID, ParentID: observation.ParentID, Number: observation.Number,
		Time: observation.Time, ObservedAt: observation.ObservedAt, Peer: observation.Peer,
	}, records)
	if err != nil {
		return fmt.Errorf("applying block %s: %w", observation.ID, err)
	}
	i.blocks.Add(1)
	i.matched.Add(int64(len(records)))
	if update.Duplicate {
		i.duplicateBlocks.Add(1)
	}
	if update.Unresolved {
		i.unresolvedBlocks.Add(1)
	}
	if update.Reorg {
		i.reorgs.Add(1)
	}
	i.orphanedTransactions.Add(int64(update.OrphanedTransactions))
	i.reincludedTransactions.Add(int64(update.ReincludedTransactions))
	return nil
}

func (i *Ingestor) blockRecord(transaction *protocol.Transaction, observation p2p.BlockObservation, watches *filter.Set) (model.Record, bool, error) {
	if transaction == nil || transaction.RawData == nil {
		return model.Record{}, false, errors.New("transaction is missing raw data")
	}
	matches := watches.MatchTransaction(transaction)
	rawData, err := proto.Marshal(transaction.RawData)
	if err != nil {
		return model.Record{}, false, fmt.Errorf("marshalling transaction raw data: %w", err)
	}
	txID := sha256.Sum256(rawData)
	txIDString := hex.EncodeToString(txID[:])
	if len(matches) == 0 {
		exists, err := i.store.Exists(txIDString)
		if err != nil {
			return model.Record{}, false, fmt.Errorf("checking matched transaction %s: %w", txIDString, err)
		}
		if !exists {
			return model.Record{}, false, nil
		}
	}
	rawTransaction, err := proto.Marshal(transaction)
	if err != nil {
		return model.Record{}, false, fmt.Errorf("marshalling transaction: %w", err)
	}
	observedAt := observation.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = i.now().UTC()
	}
	record := model.Record{
		TxID: txIDString, FirstSeen: observedAt, ContractTypes: contractTypes(transaction),
		Matches: matches, RawTransaction: rawTransaction, Source: string(p2p.SourceBlock),
		BlockID: observation.ID, ParentBlockID: observation.ParentID,
	}
	if observation.Peer != "" {
		record.Peers = []string{observation.Peer}
	}
	return record, true, nil
}

// New creates a transaction ingestor.
func New(watches watchSource, store recordStore, now func() time.Time) *Ingestor {
	if now == nil {
		now = time.Now
	}
	return &Ingestor{watches: watches, store: store, now: now}
}

// HandleTransaction filters and persists one decoded P2P transaction.
func (i *Ingestor) HandleTransaction(
	ctx context.Context,
	transaction *protocol.Transaction,
	observation p2p.Observation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if transaction == nil || transaction.RawData == nil {
		return errors.New("transaction is missing raw data")
	}
	if i.watches == nil || i.store == nil {
		return errors.New("ingestor is not fully configured")
	}
	i.seen.Add(1)
	watches := i.watches.Current()
	if watches == nil {
		return errors.New("watch snapshot is nil")
	}
	matches := watches.MatchTransaction(transaction)
	if len(matches) == 0 && observation.Source != p2p.SourceBlock {
		return nil
	}
	rawData, err := proto.Marshal(transaction.RawData)
	if err != nil {
		return fmt.Errorf("marshalling transaction raw data: %w", err)
	}
	txID := sha256.Sum256(rawData)
	txIDString := hex.EncodeToString(txID[:])
	if len(matches) == 0 {
		exists, err := i.store.Exists(txIDString)
		if err != nil {
			return fmt.Errorf("checking matched transaction %s: %w", txIDString, err)
		}
		if !exists {
			return nil
		}
	}
	rawTransaction, err := proto.Marshal(transaction)
	if err != nil {
		return fmt.Errorf("marshalling transaction: %w", err)
	}
	observedAt := observation.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = i.now().UTC()
	}
	record := model.Record{
		TxID:           txIDString,
		FirstSeen:      observedAt,
		BlockNumber:    observation.BlockNumber,
		BlockTime:      observation.BlockTime,
		ContractTypes:  contractTypes(transaction),
		Matches:        matches,
		RawTransaction: rawTransaction,
		Source:         string(observation.Source),
	}
	if observation.Peer != "" {
		record.Peers = []string{observation.Peer}
	}
	if observation.Source == p2p.SourceBlock {
		record.ConfirmedSeen = &observedAt
	}
	if err := i.store.Put(record); err != nil {
		return fmt.Errorf("storing matched transaction %s: %w", record.TxID, err)
	}
	i.matched.Add(1)
	return nil
}

// Stats returns lock-free processing counters.
func (i *Ingestor) Stats() Stats {
	return Stats{
		Seen: i.seen.Load(), Matched: i.matched.Load(), Blocks: i.blocks.Load(),
		DuplicateBlocks: i.duplicateBlocks.Load(), UnresolvedBlocks: i.unresolvedBlocks.Load(),
		Reorgs: i.reorgs.Load(), OrphanedTransactions: i.orphanedTransactions.Load(),
		ReincludedTransactions: i.reincludedTransactions.Load(),
	}
}

func contractTypes(transaction *protocol.Transaction) []string {
	var result []string
	for _, contract := range transaction.RawData.Contract {
		if contract == nil || contract.Parameter == nil {
			continue
		}
		name := path.Base(contract.Parameter.TypeUrl)
		if separator := strings.LastIndexByte(name, '.'); separator >= 0 {
			name = name[separator+1:]
		}
		if name != "" && !slices.Contains(result, name) {
			result = append(result, name)
		}
	}
	return result
}
