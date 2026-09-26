package model

import "time"

// Block is the minimum persisted identity needed to follow competing TRON branches.
type Block struct {
	ID         string    `json:"block_id"`
	ParentID   string    `json:"parent_block_id"`
	Number     int64     `json:"number"`
	Time       time.Time `json:"time"`
	ObservedAt time.Time `json:"observed_at"`
	Peer       string    `json:"peer,omitempty"`
}

// BlockRef is the durable block identity used to build a P2P chain locator.
type BlockRef struct {
	ID     string
	Number int64
}

// SolidBlock is a checkpoint reported by a trusted TRON Solidity endpoint.
type SolidBlock struct {
	ID         string
	Number     int64
	ObservedAt time.Time
}

// ChainUpdate describes the durable result of applying one block.
type ChainUpdate struct {
	BlockID                string
	Canonical              bool
	Duplicate              bool
	Unresolved             bool
	Reorg                  bool
	ReorgDepth             int
	OldTipID               string
	NewTipID               string
	OrphanedBlocks         int
	OrphanedTransactions   int
	IncludedTransactions   int
	ReincludedTransactions int
}

// FinalizeResult describes one solid-checkpoint advancement.
type FinalizeResult struct {
	FinalizedBlocks       int
	FinalizedTransactions int
	PrunedBlocks          int
	PrunedTransactions    int
}

// ChainStatus is the persisted canonical/finality state exposed to health checks.
type ChainStatus struct {
	Anchored         bool
	AnchorNumber     int64
	TipID            string
	TipNumber        int64
	TipTime          time.Time
	FinalizedID      string
	FinalizedNumber  int64
	UnresolvedBlocks int
	IntegrityFailure string
}
