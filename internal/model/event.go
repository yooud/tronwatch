package model

import (
	"fmt"
	"time"
)

// LifecycleTransition is a durable canonical-chain state change.
type LifecycleTransition string

// EventPayloadMode controls how much transaction data is copied into durable events.
type EventPayloadMode string

const (
	TransitionIncluded   LifecycleTransition = "included"
	TransitionOrphaned   LifecycleTransition = "orphaned"
	TransitionReincluded LifecycleTransition = "reincluded"
	TransitionFinalized  LifecycleTransition = "finalized"

	EventPayloadFull             EventPayloadMode = "full"
	EventPayloadCompactLifecycle EventPayloadMode = "compact_lifecycle"
)

// Event is one durable publication attempt for a transaction observation.
type Event struct {
	ID        string    `json:"event_id"`
	Type      string    `json:"type"`
	EmittedAt time.Time `json:"emitted_at"`
	Record    Record    `json:"record"`
}

// NewEvent creates a stable event identity. Re-delivery keeps the same ID.
func NewEvent(record Record, emittedAt time.Time) Event {
	block := "pending"
	eventType := "tron.transaction.pending.v1"
	if record.BlockNumber != nil {
		block = fmt.Sprintf("%d", *record.BlockNumber)
		eventType = "tron.transaction.confirmed.v1"
	}
	return Event{
		ID:        record.TxID + ":" + record.Source + ":" + block,
		Type:      eventType,
		EmittedAt: emittedAt.UTC(),
		Record:    record,
	}
}

// NewLifecycleEvent creates an ordered, idempotent v2 chain-lifecycle event.
func NewLifecycleEvent(record Record, transition LifecycleTransition, emittedAt time.Time) Event {
	blockID := record.BlockID
	if blockID == "" {
		blockID = "none"
	}
	return Event{
		ID:        fmt.Sprintf("%s:r:%020d:%s:%s", record.TxID, record.Revision, transition, blockID),
		Type:      fmt.Sprintf("tron.transaction.%s.v2", transition),
		EmittedAt: emittedAt.UTC(),
		Record:    record,
	}
}

// ProjectEvent removes repeated heavy fields from non-initial lifecycle events.
// Included events stay complete so a consumer always receives matching context.
func ProjectEvent(event Event, mode EventPayloadMode) Event {
	if mode != EventPayloadCompactLifecycle {
		return event
	}
	switch event.Type {
	case "tron.transaction.orphaned.v2", "tron.transaction.reincluded.v2", "tron.transaction.finalized.v2":
		event.Record.RawTransaction = nil
		event.Record.Peers = nil
	}
	return event
}
