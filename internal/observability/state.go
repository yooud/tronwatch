package observability

import (
	"sync"
	"sync/atomic"
	"time"
)

// State collects low-cost counters shared by adapters and the HTTP snapshot.
type State struct {
	mu                  sync.RWMutex
	peers               map[string]bool
	sessions            atomic.Int64
	reconnects          atomic.Int64
	finalityFetches     atomic.Int64
	finalityFailures    atomic.Int64
	publisherSuccesses  atomic.Int64
	publisherFailures   atomic.Int64
	watchFailures       atomic.Int64
	prunedBlocks        atomic.Int64
	prunedTransactions  atomic.Int64
	catchupBlocks       atomic.Int64
	catchupFailovers    atomic.Int64
	catchupFailures     atomic.Int64
	finalityMu          sync.RWMutex
	finalityLastSuccess time.Time
	finalizedNumber     int64
	catchupMu           sync.RWMutex
	catchupActive       bool
	catchupCurrent      int64
	catchupTarget       int64
}

func NewState(peers []string) *State {
	state := &State{peers: make(map[string]bool, len(peers))}
	for _, peer := range peers {
		state.peers[peer] = false
	}
	return state
}

func (s *State) PeerConnected(string) {}

func (s *State) PeerHandshaked(peer string, _, _ int64) {
	s.mu.Lock()
	s.peers[peer] = true
	s.mu.Unlock()
	s.sessions.Add(1)
}

func (s *State) PeerDisconnected(peer string, _ error) {
	s.mu.Lock()
	s.peers[peer] = false
	s.mu.Unlock()
	s.reconnects.Add(1)
}

func (s *State) FinalitySuccess(number int64, at time.Time) {
	s.finalityMu.Lock()
	s.finalityLastSuccess = at.UTC()
	s.finalizedNumber = number
	s.finalityMu.Unlock()
}

func (s *State) FinalityFailure() {
	s.finalityFailures.Add(1)
}

func (s *State) FinalityFetch() { s.finalityFetches.Add(1) }

func (s *State) PublisherSuccess(count int) { s.publisherSuccesses.Add(int64(count)) }
func (s *State) PublisherFailure()          { s.publisherFailures.Add(1) }
func (s *State) WatchFailure()              { s.watchFailures.Add(1) }

func (s *State) StoragePruned(blocks, transactions int) {
	s.prunedBlocks.Add(int64(blocks))
	s.prunedTransactions.Add(int64(transactions))
}

// CatchupState records whether historical synchronization is required and its heights.
func (s *State) CatchupState(active bool, _ string, current, target int64) {
	s.catchupMu.Lock()
	s.catchupActive = active
	s.catchupCurrent = current
	s.catchupTarget = target
	s.catchupMu.Unlock()
}

// CatchupBlocks records successfully applied historical blocks.
func (s *State) CatchupBlocks(count int) { s.catchupBlocks.Add(int64(count)) }

// CatchupFailover records a released sync leader that needs replacement.
func (s *State) CatchupFailover() { s.catchupFailovers.Add(1) }

// CatchupFailure records a failed historical synchronization session.
func (s *State) CatchupFailure() { s.catchupFailures.Add(1) }

func (s *State) Snapshot() Snapshot {
	s.mu.RLock()
	peers := make(map[string]bool, len(s.peers))
	connected := 0
	for peer, active := range s.peers {
		peers[peer] = active
		if active {
			connected++
		}
	}
	s.mu.RUnlock()
	s.finalityMu.RLock()
	lastFinality, finalized := s.finalityLastSuccess, s.finalizedNumber
	s.finalityMu.RUnlock()
	s.catchupMu.RLock()
	catchupActive, catchupCurrent, catchupTarget := s.catchupActive, s.catchupCurrent, s.catchupTarget
	s.catchupMu.RUnlock()
	return Snapshot{
		ConfiguredPeers: len(peers), ConnectedPeers: connected, PeerConnected: peers,
		Sessions: s.sessions.Load(), Reconnects: s.reconnects.Load(),
		FinalityFetches: s.finalityFetches.Load(), FinalityFailures: s.finalityFailures.Load(),
		FinalityLastSuccess: lastFinality, FinalizedNumber: finalized,
		PublisherSuccesses: s.publisherSuccesses.Load(), PublisherFailures: s.publisherFailures.Load(),
		WatchRefreshFailures: s.watchFailures.Load(),
		PrunedBlocks:         s.prunedBlocks.Load(), PrunedTransactions: s.prunedTransactions.Load(),
		CatchupActive: catchupActive, CatchupCurrentHeight: catchupCurrent, CatchupTargetHeight: catchupTarget,
		CatchupBlocks: s.catchupBlocks.Load(), CatchupFailovers: s.catchupFailovers.Load(), CatchupFailures: s.catchupFailures.Load(),
	}
}
