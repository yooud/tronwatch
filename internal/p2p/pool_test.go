package p2p

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yooud/tronwatch/internal/protocol"
)

func TestSerializedHandlerProcessesOneObservationAtATime(t *testing.T) {
	var active, maximum, calls atomic.Int64
	handler, err := newSerializedHandler(HandlerFunc(func(context.Context, *protocol.Transaction, Observation) error {
		current := active.Add(1)
		defer active.Add(-1)
		calls.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	errorsChannel := make(chan error, 50)
	for index := 0; index < 50; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsChannel <- handler.HandleTransaction(context.Background(), &protocol.Transaction{}, Observation{})
		}()
	}
	wait.Wait()
	close(errorsChannel)
	for handlerErr := range errorsChannel {
		if handlerErr != nil {
			t.Fatal(handlerErr)
		}
	}
	if calls.Load() != 50 || maximum.Load() != 1 {
		t.Fatalf("calls = %d, maximum concurrency = %d; want 50, 1", calls.Load(), maximum.Load())
	}
}

func TestSerializedHandlerCancellationAndErrorsReleaseStream(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	handler, err := newSerializedHandler(HandlerFunc(func(context.Context, *protocol.Transaction, Observation) error {
		call := calls.Add(1)
		if call == 1 {
			close(entered)
			<-release
			return errors.New("first failed")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		first <- handler.HandleTransaction(context.Background(), &protocol.Transaction{}, Observation{})
	}()
	<-entered
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handler.HandleTransaction(canceled, &protocol.Transaction{}, Observation{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v", err)
	}
	close(release)
	if err := <-first; err == nil {
		t.Fatal("first call error = nil")
	}
	if err := handler.HandleTransaction(context.Background(), &protocol.Transaction{}, Observation{}); err != nil {
		t.Fatalf("call after error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("underlying calls = %d, want 2", calls.Load())
	}
}

func TestPoolSharesNodeID(t *testing.T) {
	pool, err := NewPool([]string{"127.0.0.1:18888", "127.0.0.2:18888"}, Config{
		NetworkID: 201910292, AdvertiseIP: "127.0.0.1",
	}, HandlerFunc(func(context.Context, *protocol.Transaction, Observation) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if pool.Size() != 2 || !bytes.Equal(pool.clients[0].nodeID, pool.clients[1].nodeID) {
		t.Fatalf("pool size/node identities are not shared: %+v", pool.clients)
	}
}

func TestNewPoolRejectsInvalidPeers(t *testing.T) {
	handler := HandlerFunc(func(context.Context, *protocol.Transaction, Observation) error { return nil })
	for name, peers := range map[string][]string{
		"empty":     nil,
		"malformed": {"127.0.0.1"},
		"no host":   {":18888"},
		"bad port":  {"127.0.0.1:not-a-port"},
		"duplicate": {"127.0.0.1:18888", "127.0.0.1:18888"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPool(peers, Config{NetworkID: 201910292, AdvertiseIP: "127.0.0.1"}, handler); err == nil {
				t.Fatal("NewPool() error = nil, want validation error")
			}
		})
	}
}

func TestPoolKeepsHealthyPeerRunningWhenAnotherIsUnavailable(t *testing.T) {
	healthy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = healthy.Close() })
	unavailable, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unavailableAddress := unavailable.Addr().String()
	if err := unavailable.Close(); err != nil {
		t.Fatal(err)
	}
	headHash := bytes.Repeat([]byte{0x33}, 32)
	block := &protocol.Block{BlockHeader: &protocol.BlockHeader{RawData: &protocol.BlockHeaderRaw{
		Number: 70_000_001, Timestamp: 1_700_000_003_000, ParentHash: headHash,
	}}, Transactions: []*protocol.Transaction{{RawData: &protocol.TransactionRaw{Timestamp: 1_700_000_003_000}}}}
	blockID, err := blockID(block.BlockHeader.RawData)
	if err != nil {
		t.Fatal(err)
	}
	serverErrors := make(chan error, 1)
	go func() {
		connection, acceptErr := healthy.Accept()
		if acceptErr != nil {
			serverErrors <- acceptErr
			return
		}
		serverErrors <- serveOneBlock(connection, headHash, blockID, block)
	}()
	observations := make(chan Observation, 1)
	pool, err := NewPool([]string{healthy.Addr().String(), unavailableAddress}, Config{
		NetworkID: 201910292, AdvertiseIP: "127.0.0.1", ReconnectMin: 10 * time.Millisecond,
	}, HandlerFunc(func(_ context.Context, _ *protocol.Transaction, observation Observation) error {
		observations <- observation
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	select {
	case observation := <-observations:
		if observation.Peer != healthy.Addr().String() || observation.ObservedAt.IsZero() {
			t.Fatalf("observation = %+v, want healthy peer provenance", observation)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for healthy peer")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := <-serverErrors; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("mock peer error = %v", err)
	}
}
