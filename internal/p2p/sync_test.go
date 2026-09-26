package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/yooud/tronwatch/internal/model"
	"github.com/yooud/tronwatch/internal/protocol"
)

func TestValidateChainInventoryReturnsContiguousMissingRange(t *testing.T) {
	locator := []*protocol.BlockID{syncTestID(0x10, 10), syncTestID(0x20, 20)}
	inventory := &protocol.ChainInventory{
		Ids:       []*protocol.BlockID{syncTestID(0x20, 20), syncTestID(0x21, 21), syncTestID(0x22, 22)},
		RemainNum: 5,
	}
	missing, target, err := validateChainInventory(inventory, locator)
	if err != nil {
		t.Fatal(err)
	}
	if target != 27 || len(missing) != 2 || missing[0].Number != 21 || missing[1].Number != 22 {
		t.Fatalf("validateChainInventory() missing=%+v target=%d, want heights 21,22 target 27", missing, target)
	}
}

func TestValidateChainInventoryRejectsMalformedPeerResponses(t *testing.T) {
	locator := []*protocol.BlockID{syncTestID(0x20, 20)}
	oversized := make([]*protocol.BlockID, maxChainInventoryIDs+1)
	for index := range oversized {
		oversized[index] = syncTestID(byte(index), int64(20+index))
	}
	badPrefix := syncTestID(0x20, 20)
	badPrefix.Hash[0] = 0xff
	tests := []struct {
		name      string
		inventory *protocol.ChainInventory
	}{
		{name: "empty", inventory: &protocol.ChainInventory{}},
		{name: "unknown common block", inventory: &protocol.ChainInventory{Ids: []*protocol.BlockID{syncTestID(0x30, 20)}}},
		{name: "non-contiguous", inventory: &protocol.ChainInventory{Ids: []*protocol.BlockID{syncTestID(0x20, 20), syncTestID(0x22, 22)}}},
		{name: "negative remaining", inventory: &protocol.ChainInventory{Ids: []*protocol.BlockID{syncTestID(0x20, 20)}, RemainNum: -1}},
		{name: "oversized", inventory: &protocol.ChainInventory{Ids: oversized}},
		{name: "invalid height prefix", inventory: &protocol.ChainInventory{Ids: []*protocol.BlockID{badPrefix}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := validateChainInventory(test.inventory, locator); err == nil {
				t.Fatal("validateChainInventory() error = nil, want malformed response error")
			}
		})
	}
}

func TestCatchupDoesNotBackfillAnEmptyDatabase(t *testing.T) {
	chain := &testChainReader{}
	client, err := New(Config{
		Peer: "peer-a:18888", NetworkID: 201910292, AdvertiseIP: "127.0.0.1",
		CatchupEnabled: true, CatchupBatchSize: 100, CatchupRequestTimeout: time.Second, Chain: chain,
	}, HandlerFunc(func(_ context.Context, _ *protocol.Transaction, _ Observation) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	session := catchupSession{}
	err = client.maybeStartCatchup(nil, &protocol.TronHello{HeadBlockId: syncTestID(0x20, 20)}, &session)
	if err != nil {
		t.Fatal(err)
	}
	if session.active || client.catchup.needed() {
		t.Fatal("empty database started a genesis backfill")
	}
}

func TestObserveAdvertisedBlockAdvancesSessionHead(t *testing.T) {
	hello := &protocol.TronHello{HeadBlockId: syncTestID(0x10, 10)}
	payload, err := proto.Marshal(&protocol.Inventory{
		Type: protocol.Inventory_BLOCK,
		Ids:  [][]byte{syncTestID(0x11, 11).Hash, syncTestID(0x12, 12).Hash},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observeAdvertisedHead(payload, hello); err != nil {
		t.Fatal(err)
	}
	if hello.HeadBlockId.Number != 12 || !bytes.Equal(hello.HeadBlockId.Hash, syncTestID(0x12, 12).Hash) {
		t.Fatalf("session head = %+v, want advertised block 12", hello.HeadBlockId)
	}
}

func TestShouldStartCatchupFromInventory(t *testing.T) {
	tests := []struct {
		name          string
		status        model.ChainStatus
		head          int64
		alreadyNeeded bool
		want          bool
	}{
		{name: "single live successor", status: model.ChainStatus{Anchored: true, TipNumber: 10}, head: 11, want: false},
		{name: "multiple missing blocks", status: model.ChainStatus{Anchored: true, TipNumber: 10}, head: 12, want: true},
		{name: "unresolved parent", status: model.ChainStatus{Anchored: true, TipNumber: 10, UnresolvedBlocks: 1}, head: 11, want: true},
		{name: "catch-up already needed", status: model.ChainStatus{Anchored: true, TipNumber: 10}, head: 11, alreadyNeeded: true, want: true},
		{name: "empty database", status: model.ChainStatus{}, head: 12, want: false},
		{name: "stale advertisement", status: model.ChainStatus{Anchored: true, TipNumber: 12}, head: 11, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldStartCatchupFromInventory(test.status, test.head, test.alreadyNeeded); got != test.want {
				t.Fatalf("shouldStartCatchupFromInventory() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestCatchupFetchDelayRespectsPeerRateLimit(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	if delay := catchupFetchDelay(time.Time{}, now); delay != 0 {
		t.Fatalf("first fetch delay = %v, want zero", delay)
	}
	if delay := catchupFetchDelay(now.Add(-100*time.Millisecond), now); delay != 250*time.Millisecond {
		t.Fatalf("burst fetch delay = %v, want 250ms", delay)
	}
	if delay := catchupFetchDelay(now.Add(-time.Second), now); delay != 0 {
		t.Fatalf("paced fetch delay = %v, want zero", delay)
	}
}

func TestCatchupWaitsForPeerThatCanServePersistedTip(t *testing.T) {
	chain := &testChainReader{refs: []model.BlockRef{{ID: syncTestIDHex(10), Number: 10}}}
	observer := &recordingCatchupObserver{}
	client, err := New(Config{
		Peer: "peer-a:18888", NetworkID: 201910292, AdvertiseIP: "127.0.0.1",
		CatchupEnabled: true, CatchupBatchSize: 100, CatchupRequestTimeout: time.Second,
		Chain: chain, Observer: observer,
	}, HandlerFunc(func(_ context.Context, _ *protocol.Transaction, _ Observation) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	session := catchupSession{}
	err = client.maybeStartCatchup(nil, &protocol.TronHello{
		HeadBlockId: syncTestID(0x20, 20), LowestBlockNum: 12,
	}, &session)
	if err != nil {
		t.Fatal(err)
	}
	if session.active || !client.catchup.needed() || !observer.active {
		t.Fatalf("session=%+v observer=%+v, want waiting catch-up", session, observer)
	}
}

func TestCatchupCoordinatorFailsOverWithoutDuplicatingLeader(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	observer := &recordingCatchupObserver{}
	coordinator := newCatchupCoordinator(10*time.Second, observer)
	coordinator.now = func() time.Time { return now }
	coordinator.observe(10, 20)
	if !coordinator.tryAcquire("peer-a") || coordinator.tryAcquire("peer-b") {
		t.Fatal("coordinator did not keep exactly one catch-up leader")
	}
	coordinator.progress("peer-a", 15, 5)
	coordinator.fail("peer-a")
	if coordinator.tryAcquire("peer-a") {
		t.Fatal("failed peer reacquired before cooldown")
	}
	if !coordinator.tryAcquire("peer-b") {
		t.Fatal("healthy peer did not take over catch-up")
	}
	coordinator.complete("peer-b", 20)
	if coordinator.needed() {
		t.Fatal("catch-up remains active after reaching target")
	}
	if observer.blocks != 5 || observer.failovers != 1 || observer.failures != 1 || observer.active {
		t.Fatalf("observer = %+v, want 5 blocks, one failure/failover, inactive", observer)
	}
}

func TestCatchupCoordinatorKeepsHigherObservedTarget(t *testing.T) {
	observer := &recordingCatchupObserver{}
	coordinator := newCatchupCoordinator(time.Second, observer)
	coordinator.observe(10, 100)
	if !coordinator.tryAcquire("peer-a") {
		t.Fatal("peer-a did not acquire catch-up")
	}
	coordinator.observe(10, 120)
	if coordinator.complete("peer-a", 100) {
		t.Fatal("lower-head peer completed catch-up below the highest observed target")
	}
	if !coordinator.needed() || observer.target != 120 || !observer.active {
		t.Fatalf("observer=%+v, want catch-up active through 120", observer)
	}
	if observer.failovers != 1 || coordinator.tryAcquire("peer-a") {
		t.Fatalf("lower-head peer was not cooled down after failover: observer=%+v", observer)
	}
	if !coordinator.tryAcquire("peer-b") {
		t.Fatal("higher-head peer did not acquire the remaining catch-up")
	}
}

func TestCatchupCoordinatorDoesNotRegressConcurrentProgress(t *testing.T) {
	observer := &recordingCatchupObserver{}
	coordinator := newCatchupCoordinator(time.Second, observer)
	coordinator.observe(50, 100)
	if !coordinator.tryAcquire("peer-a") {
		t.Fatal("peer-a did not acquire catch-up")
	}
	coordinator.progress("peer-a", 80, 30)
	coordinator.observe(20, 100)
	if observer.current != 80 {
		t.Fatalf("catch-up current height regressed to %d, want 80", observer.current)
	}
}

type recordingCatchupObserver struct {
	active    bool
	peer      string
	current   int64
	target    int64
	blocks    int
	failovers int
	failures  int
}

func (o *recordingCatchupObserver) CatchupState(active bool, peer string, current, target int64) {
	o.active, o.peer, o.current, o.target = active, peer, current, target
}

func (o *recordingCatchupObserver) CatchupBlocks(count int) { o.blocks += count }
func (o *recordingCatchupObserver) CatchupFailover()        { o.failovers++ }
func (o *recordingCatchupObserver) CatchupFailure()         { o.failures++ }
func (o *recordingCatchupObserver) PeerConnected(string)    {}
func (o *recordingCatchupObserver) PeerHandshaked(string, int64, int64) {
}
func (o *recordingCatchupObserver) PeerDisconnected(string, error) {}

func syncTestID(fill byte, number int64) *protocol.BlockID {
	hash := bytes.Repeat([]byte{fill}, 32)
	binary.BigEndian.PutUint64(hash[:8], uint64(number))
	return &protocol.BlockID{Hash: hash, Number: number}
}

func syncTestIDHex(number int64) string {
	return fmt.Sprintf("%x", syncTestID(byte(number), number).Hash)
}
