// Package model contains persistent transaction records.
package model

import (
	"time"

	"github.com/yooud/tronwatch/internal/filter"
)

// ChainState separates a fast block observation from canonical and solidified state.
type ChainState string

const (
	ChainStatePending          ChainState = "pending"
	ChainStateIncluded         ChainState = "included"
	ChainStateOrphaned         ChainState = "orphaned"
	ChainStateFinalized        ChainState = "finalized"
	ChainStateLegacyUnverified ChainState = "legacy_unverified"
)

// Record is a matched P2P transaction and its latest confirmation state.
type Record struct {
	TxID           string         `json:"tx_id"`
	FirstSeen      time.Time      `json:"first_seen"`
	ConfirmedSeen  *time.Time     `json:"confirmed_seen,omitempty"`
	BlockNumber    *int64         `json:"block_number,omitempty"`
	BlockTime      *time.Time     `json:"block_time,omitempty"`
	ContractTypes  []string       `json:"contract_types"`
	Matches        []filter.Match `json:"matches"`
	RawTransaction []byte         `json:"raw_transaction,omitempty"`
	Source         string         `json:"source"`
	Peers          []string       `json:"peers,omitempty"`
	BlockID        string         `json:"block_id,omitempty"`
	ParentBlockID  string         `json:"parent_block_id,omitempty"`
	ChainState     ChainState     `json:"chain_state,omitempty"`
	Revision       uint64         `json:"revision,omitempty"`
	OrphanedSeen   *time.Time     `json:"orphaned_seen,omitempty"`
	FinalizedSeen  *time.Time     `json:"finalized_seen,omitempty"`
}
