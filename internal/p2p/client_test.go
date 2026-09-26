package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/yooud/tronwatch/internal/model"
	"github.com/yooud/tronwatch/internal/protocol"
)

func TestBlockIDUsesTRONHeaderHashFormat(t *testing.T) {
	header := &protocol.BlockHeaderRaw{Number: 70_000_001, Timestamp: 1_700_000_003_000, ParentHash: bytes.Repeat([]byte{0x22}, 32)}
	encoded, err := proto.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(encoded)
	binary.BigEndian.PutUint64(want[:8], uint64(header.Number))
	got, err := blockID(header)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("blockID() = %x, want %x", got, want)
	}
}

func TestHandleBlockCallsAtomicHandlerForEmptyBlock(t *testing.T) {
	called := 0
	handler := &recordingBlockHandler{onBlock: func(_ context.Context, block BlockObservation, transactions []*protocol.Transaction) error {
		called++
		if block.Number != 42 || block.ID == "" || block.ParentID != fmt.Sprintf("%x", bytes.Repeat([]byte{0x11}, 32)) {
			t.Fatalf("block observation = %+v", block)
		}
		if len(transactions) != 0 {
			t.Fatalf("transactions = %d, want empty", len(transactions))
		}
		return nil
	}}
	client, err := New(Config{NetworkID: 201910292, AdvertiseIP: "127.0.0.1"}, handler)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(&protocol.Block{BlockHeader: &protocol.BlockHeader{RawData: &protocol.BlockHeaderRaw{
		Number: 42, ParentHash: bytes.Repeat([]byte{0x11}, 32), Timestamp: 1_700_000_000_000,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.handleBlock(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("block handler calls = %d, want 1", called)
	}
}

func TestHandleRequestedBlockRejectsInventoryMismatchBeforeCallback(t *testing.T) {
	called := false
	handler := &recordingBlockHandler{onBlock: func(context.Context, BlockObservation, []*protocol.Transaction) error {
		called = true
		return nil
	}}
	client, err := New(Config{NetworkID: 201910292, AdvertiseIP: "127.0.0.1"}, handler)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(&protocol.Block{BlockHeader: &protocol.BlockHeader{RawData: &protocol.BlockHeaderRaw{
		Number: 42, ParentHash: bytes.Repeat([]byte{0x11}, 32), Timestamp: 1_700_000_000_000,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	requested := map[string]time.Time{string(append([]byte{byte(protocol.Inventory_BLOCK)}, bytes.Repeat([]byte{0xff}, 32)...)): time.Now()}
	if err := client.handleRequestedBlock(context.Background(), payload, requested); err == nil {
		t.Fatal("handleRequestedBlock() error = nil, want inventory mismatch")
	}
	if called {
		t.Fatal("handler called for unadvertised block")
	}
}

type recordingBlockHandler struct {
	onBlock func(context.Context, BlockObservation, []*protocol.Transaction) error
}

func (h *recordingBlockHandler) HandleTransaction(context.Context, *protocol.Transaction, Observation) error {
	return nil
}

func (h *recordingBlockHandler) HandleBlock(ctx context.Context, block BlockObservation, transactions []*protocol.Transaction) error {
	return h.onBlock(ctx, block, transactions)
}

func TestRunSessionMirrorsHeadAndFetchesAdvertisedBlock(t *testing.T) {
	clientConnection, serverConnection := net.Pipe()
	t.Cleanup(func() {
		if err := clientConnection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("client Close() error = %v", err)
		}
		if err := serverConnection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("server Close() error = %v", err)
		}
	})

	headHash := bytes.Repeat([]byte{0x33}, 32)
	blockTimestamp := int64(1_700_000_003_000)
	block := &protocol.Block{
		BlockHeader: &protocol.BlockHeader{RawData: &protocol.BlockHeaderRaw{
			Number:     70_000_001,
			Timestamp:  blockTimestamp,
			ParentHash: headHash,
		}},
		Transactions: []*protocol.Transaction{{RawData: &protocol.TransactionRaw{Timestamp: blockTimestamp}}},
	}
	blockID, err := blockID(block.BlockHeader.RawData)
	if err != nil {
		t.Fatal(err)
	}

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- serveOneBlock(serverConnection, headHash, blockID, block)
	}()

	observations := make(chan Observation, 1)
	client, err := New(Config{
		NetworkID:   201910292,
		AdvertiseIP: "127.0.0.1",
	}, HandlerFunc(func(_ context.Context, _ *protocol.Transaction, observation Observation) error {
		observations <- observation
		return nil
	}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type sessionResult struct {
		completed bool
		err       error
	}
	clientDone := make(chan sessionResult, 1)
	go func() {
		completed, sessionErr := client.runSessionOutcome(ctx, clientConnection)
		clientDone <- sessionResult{completed: completed, err: sessionErr}
	}()

	select {
	case got := <-observations:
		if got.Source != SourceBlock || got.BlockNumber == nil || *got.BlockNumber != 70_000_001 {
			t.Fatalf("observation = %+v, want block 70000001", got)
		}
		if got.BlockTime == nil || got.BlockTime.UnixMilli() != blockTimestamp {
			t.Fatalf("BlockTime = %v, want %d", got.BlockTime, blockTimestamp)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for block observation")
	}

	if err := <-serverErrors; err != nil {
		t.Fatalf("mock peer error = %v", err)
	}
	result := <-clientDone
	if !result.completed {
		t.Fatal("runSessionOutcome() completed = false, want successful handshake")
	}
	if result.err != nil && !errors.Is(result.err, io.EOF) && !errors.Is(result.err, net.ErrClosed) {
		t.Fatalf("runSessionOutcome() error = %v", result.err)
	}
}

func TestRunSessionFetchesSingleLiveSuccessorWithoutCatchup(t *testing.T) {
	clientConnection, serverConnection := net.Pipe()
	t.Cleanup(func() {
		_ = clientConnection.Close()
		_ = serverConnection.Close()
	})

	headHash := syncBlockID(70_000_000, 0x33)
	blockTimestamp := int64(1_700_000_003_000)
	block := syncTestBlock(70_000_001, headHash, blockTimestamp)
	blockID, err := blockID(block.BlockHeader.RawData)
	if err != nil {
		t.Fatal(err)
	}
	chain := &testChainReader{refs: []model.BlockRef{{ID: fmt.Sprintf("%x", headHash), Number: 70_000_000}}}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- serveOneBlock(serverConnection, headHash, blockID, block) }()

	client, err := New(Config{
		Peer: "mock:18888", NetworkID: 201910292, AdvertiseIP: "127.0.0.1",
		CatchupEnabled: true, CatchupBatchSize: 100, CatchupRequestTimeout: time.Second,
		Chain: chain,
	}, &catchupBlockHandler{chain: chain, numbers: make(chan int64, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	completed, sessionErr := client.runSessionOutcome(ctx, clientConnection)
	if !completed {
		t.Fatal("runSessionOutcome() completed = false, want successful handshake")
	}
	if sessionErr != nil && !errors.Is(sessionErr, io.EOF) && !errors.Is(sessionErr, net.ErrClosed) {
		t.Fatalf("runSessionOutcome() error = %v", sessionErr)
	}
	if err := <-serverErrors; err != nil {
		t.Fatalf("mock peer error = %v", err)
	}
	status, err := chain.ChainStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.TipNumber != 70_000_001 || client.catchup.needed() {
		t.Fatalf("chain status = %+v, catch-up needed = %t", status, client.catchup.needed())
	}
}

func TestRunSessionCatchesUpPersistedTipBeforeLive(t *testing.T) {
	clientConnection, serverConnection := net.Pipe()
	t.Cleanup(func() {
		_ = clientConnection.Close()
		_ = serverConnection.Close()
	})

	baseID := syncBlockID(10, 0x10)
	block11 := syncTestBlock(11, baseID, 1_700_000_003_000)
	id11, err := blockID(block11.BlockHeader.RawData)
	if err != nil {
		t.Fatal(err)
	}
	block12 := syncTestBlock(12, id11, 1_700_000_006_000)
	id12, err := blockID(block12.BlockHeader.RawData)
	if err != nil {
		t.Fatal(err)
	}
	chain := &testChainReader{refs: []model.BlockRef{{ID: fmt.Sprintf("%x", baseID), Number: 10}}}
	handler := &catchupBlockHandler{chain: chain, numbers: make(chan int64, 2)}
	observer := &recordingCatchupObserver{}

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- serveCatchupBlocks(serverConnection, baseID, id11, id12, block11, block12)
	}()
	client, err := New(Config{
		Peer: "mock:18888", NetworkID: 201910292, AdvertiseIP: "127.0.0.1",
		CatchupEnabled: true, CatchupBatchSize: 2, CatchupRequestTimeout: time.Second,
		Chain: chain, Observer: observer,
	}, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	completed, sessionErr := client.runSessionOutcome(ctx, clientConnection)
	if !completed {
		t.Fatal("runSessionOutcome() completed = false, want successful handshake")
	}
	if sessionErr != nil && !errors.Is(sessionErr, io.EOF) && !errors.Is(sessionErr, net.ErrClosed) {
		t.Fatalf("runSessionOutcome() error = %v", sessionErr)
	}
	if err := <-serverErrors; err != nil {
		t.Fatalf("mock peer error = %v", err)
	}
	close(handler.numbers)
	var got []int64
	for number := range handler.numbers {
		got = append(got, number)
	}
	if len(got) != 2 || got[0] != 11 || got[1] != 12 {
		t.Fatalf("caught-up blocks = %v, want [11 12]", got)
	}
	status, err := chain.ChainStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.TipNumber != 12 || observer.active || observer.blocks != 2 {
		t.Fatalf("catch-up status=%+v observer=%+v, want completed at 12", status, observer)
	}
}

func TestReconnectDelayHonorsRecentDisconnectCooldown(t *testing.T) {
	rejected := fmt.Errorf("handshake failed: %w", &transportRejectError{code: transportRecentDisconnect})
	if got := reconnectDelay(rejected, 10*time.Second); got != recentDisconnectRetryDelay {
		t.Fatalf("reconnectDelay(recent disconnect) = %v, want %v", got, recentDisconnectRetryDelay)
	}
	if got := reconnectDelay(&transportRejectError{code: 1}, 10*time.Second); got != 10*time.Second {
		t.Fatalf("reconnectDelay(too many peers) = %v, want 10s", got)
	}
}

func TestAdvanceBackoffResetsOnlyAfterHandshake(t *testing.T) {
	if got := advanceBackoff(false, time.Second, time.Second, 30*time.Second); got != 2*time.Second {
		t.Fatalf("advanceBackoff(rejected) = %v, want 2s", got)
	}
	if got := advanceBackoff(true, 16*time.Second, time.Second, 30*time.Second); got != time.Second {
		t.Fatalf("advanceBackoff(established) = %v, want 1s", got)
	}
}

func TestValidateTransportHelloReturnsTypedReject(t *testing.T) {
	client, err := New(Config{NetworkID: 11111, AdvertiseIP: "127.0.0.1"}, HandlerFunc(
		func(context.Context, *protocol.Transaction, Observation) error { return nil },
	))
	if err != nil {
		t.Fatal(err)
	}
	hello, err := proto.Marshal(&protocol.TransportHello{Code: transportRecentDisconnect})
	if err != nil {
		t.Fatal(err)
	}
	err = client.validateTransportHello(append([]byte{transportHello}, hello...))
	var rejected *transportRejectError
	if !errors.As(err, &rejected) || rejected.code != transportRecentDisconnect {
		t.Fatalf("validateTransportHello() error = %v, want typed code %d", err, transportRecentDisconnect)
	}
}

func serveOneBlock(connection net.Conn, headHash, blockID []byte, block *protocol.Block) error {
	transportFrame, err := readFrame(connection, maxMessageSize)
	if err != nil {
		return fmt.Errorf("reading transport hello: %w", err)
	}
	if len(transportFrame) == 0 || transportFrame[0] != transportHello {
		return fmt.Errorf("transport message type = %x, want fd", transportFrame)
	}
	var clientHello protocol.TransportHello
	if err := proto.Unmarshal(transportFrame[1:], &clientHello); err != nil {
		return fmt.Errorf("decoding transport hello: %w", err)
	}
	nodeID := bytes.Repeat([]byte{0x22}, 64)
	nodeTransport := &protocol.TransportHello{
		From:      &protocol.Endpoint{Address: []byte("172.18.0.3"), Port: 18888, NodeId: nodeID},
		NetworkId: clientHello.NetworkId,
		Timestamp: clientHello.Timestamp,
		Version:   1,
	}
	if err := sendPlainProto(connection, transportHello, nodeTransport); err != nil {
		return err
	}
	checkpoint := func(hash []byte, number int64) *protocol.BlockID {
		return &protocol.BlockID{Hash: hash, Number: number}
	}
	nodeTronHello := &protocol.TronHello{
		From:           nodeTransport.From,
		Version:        clientHello.NetworkId,
		Timestamp:      clientHello.Timestamp,
		GenesisBlockId: checkpoint(bytes.Repeat([]byte{0x11}, 32), 0),
		SolidBlockId:   checkpoint(bytes.Repeat([]byte{0x32}, 32), 69_999_990),
		HeadBlockId:    checkpoint(headHash, 70_000_000),
		CodeVersion:    []byte("4.8.2.1.PQ1_build1"),
	}
	if err := sendCompressedProto(connection, messageHello, nodeTronHello); err != nil {
		return err
	}
	clientTronFrame, err := receiveCompressed(connection)
	if err != nil {
		return fmt.Errorf("reading TRON hello: %w", err)
	}
	if len(clientTronFrame) == 0 || clientTronFrame[0] != messageHello {
		return fmt.Errorf("TRON message type = %x, want 20", clientTronFrame)
	}
	var clientTronHello protocol.TronHello
	if err := proto.Unmarshal(clientTronFrame[1:], &clientTronHello); err != nil {
		return fmt.Errorf("decoding TRON hello: %w", err)
	}
	if !proto.Equal(clientTronHello.HeadBlockId, nodeTronHello.HeadBlockId) {
		return fmt.Errorf("client head = %v, want %v", clientTronHello.HeadBlockId, nodeTronHello.HeadBlockId)
	}
	if err := sendCompressedProto(connection, messageInventory, &protocol.Inventory{
		Type: protocol.Inventory_BLOCK,
		Ids:  [][]byte{blockID},
	}); err != nil {
		return err
	}
	fetchFrame, err := receiveCompressed(connection)
	if err != nil {
		return fmt.Errorf("reading fetch: %w", err)
	}
	if len(fetchFrame) == 0 || fetchFrame[0] != messageFetchData {
		return fmt.Errorf("fetch type = %x, want 07", fetchFrame)
	}
	var fetch protocol.Inventory
	if err := proto.Unmarshal(fetchFrame[1:], &fetch); err != nil {
		return fmt.Errorf("decoding fetch: %w", err)
	}
	if fetch.Type != protocol.Inventory_BLOCK || len(fetch.Ids) != 1 || !bytes.Equal(fetch.Ids[0], blockID) {
		return fmt.Errorf("fetch = %v, want advertised block", &fetch)
	}
	if err := sendCompressedProto(connection, messageBlock, block); err != nil {
		return err
	}
	return connection.Close()
}

func sendPlainProto(connection net.Conn, messageType byte, message proto.Message) error {
	encoded, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return writeFrame(connection, append([]byte{messageType}, encoded...))
}

func sendCompressedProto(connection net.Conn, messageType byte, message proto.Message) error {
	encoded, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	packed, err := wrapCompressed(append([]byte{messageType}, encoded...))
	if err != nil {
		return err
	}
	return writeFrame(connection, packed)
}

func receiveCompressed(connection net.Conn) ([]byte, error) {
	frame, err := readFrame(connection, maxMessageSize)
	if err != nil {
		return nil, err
	}
	return unwrapCompressed(frame)
}

type testChainReader struct {
	mu   sync.Mutex
	refs []model.BlockRef
}

func (r *testChainReader) ChainStatus() (model.ChainStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.refs) == 0 {
		return model.ChainStatus{}, nil
	}
	tip := r.refs[len(r.refs)-1]
	return model.ChainStatus{Anchored: true, AnchorNumber: r.refs[0].Number, TipID: tip.ID, TipNumber: tip.Number}, nil
}

func (r *testChainReader) ChainLocator(int) ([]model.BlockRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]model.BlockRef(nil), r.refs...), nil
}

func (r *testChainReader) append(block BlockObservation) {
	r.mu.Lock()
	r.refs = append(r.refs, model.BlockRef{ID: block.ID, Number: block.Number})
	r.mu.Unlock()
}

type catchupBlockHandler struct {
	chain   *testChainReader
	numbers chan int64
}

func (h *catchupBlockHandler) HandleTransaction(context.Context, *protocol.Transaction, Observation) error {
	return nil
}

func (h *catchupBlockHandler) HandleBlock(_ context.Context, block BlockObservation, _ []*protocol.Transaction) error {
	h.chain.append(block)
	h.numbers <- block.Number
	return nil
}

func serveCatchupBlocks(
	connection net.Conn,
	baseID, id11, id12 []byte,
	block11, block12 *protocol.Block,
) error {
	transportFrame, err := readFrame(connection, maxMessageSize)
	if err != nil {
		return err
	}
	var clientHello protocol.TransportHello
	if len(transportFrame) < 2 || transportFrame[0] != transportHello {
		return fmt.Errorf("transport hello = %x", transportFrame)
	}
	if err := proto.Unmarshal(transportFrame[1:], &clientHello); err != nil {
		return err
	}
	endpoint := &protocol.Endpoint{Address: []byte("172.18.0.3"), Port: 18888, NodeId: bytes.Repeat([]byte{0x22}, 64)}
	if err := sendPlainProto(connection, transportHello, &protocol.TransportHello{
		From: endpoint, NetworkId: clientHello.NetworkId, Timestamp: clientHello.Timestamp, Version: 1,
	}); err != nil {
		return err
	}
	genesis := syncBlockID(0, 0x01)
	if err := sendCompressedProto(connection, messageHello, &protocol.TronHello{
		From: endpoint, Version: clientHello.NetworkId, Timestamp: clientHello.Timestamp,
		GenesisBlockId: &protocol.BlockID{Hash: genesis, Number: 0},
		SolidBlockId:   &protocol.BlockID{Hash: baseID, Number: 10},
		HeadBlockId:    &protocol.BlockID{Hash: id12, Number: 12},
		CodeVersion:    []byte("test"),
	}); err != nil {
		return err
	}
	if _, err := receiveCompressed(connection); err != nil {
		return err
	}
	firstSync, err := receiveCompressed(connection)
	if err != nil {
		return err
	}
	if len(firstSync) == 0 || firstSync[0] != messageSyncBlockChain {
		return fmt.Errorf("first sync message = %x", firstSync)
	}
	var locator protocol.BlockInventory
	if err := proto.Unmarshal(firstSync[1:], &locator); err != nil {
		return err
	}
	if len(locator.Ids) < 2 || !bytes.Equal(locator.Ids[len(locator.Ids)-1].Hash, baseID) {
		return fmt.Errorf("locator = %+v, want persisted tip", &locator)
	}
	if err := sendCompressedProto(connection, messageBlockChainInventory, &protocol.ChainInventory{Ids: []*protocol.BlockID{
		{Hash: baseID, Number: 10}, {Hash: id11, Number: 11}, {Hash: id12, Number: 12},
	}}); err != nil {
		return err
	}
	fetchFrame, err := receiveCompressed(connection)
	if err != nil {
		return err
	}
	var fetch protocol.Inventory
	if len(fetchFrame) == 0 || fetchFrame[0] != messageFetchData {
		return fmt.Errorf("fetch message = %x", fetchFrame)
	}
	if err := proto.Unmarshal(fetchFrame[1:], &fetch); err != nil {
		return err
	}
	if len(fetch.Ids) != 2 || !bytes.Equal(fetch.Ids[0], id11) || !bytes.Equal(fetch.Ids[1], id12) {
		return fmt.Errorf("fetch ids = %x, want blocks 11 and 12", fetch.Ids)
	}
	if err := sendCompressedProto(connection, messageBlock, block11); err != nil {
		return err
	}
	if err := sendCompressedProto(connection, messageBlock, block12); err != nil {
		return err
	}
	finalSync, err := receiveCompressed(connection)
	if err != nil {
		return err
	}
	if len(finalSync) == 0 || finalSync[0] != messageSyncBlockChain {
		return fmt.Errorf("final sync message = %x", finalSync)
	}
	if err := proto.Unmarshal(finalSync[1:], &locator); err != nil {
		return err
	}
	if !bytes.Equal(locator.Ids[len(locator.Ids)-1].Hash, id12) {
		return fmt.Errorf("final locator tip = %x, want %x", locator.Ids[len(locator.Ids)-1].Hash, id12)
	}
	if err := sendCompressedProto(connection, messageBlockChainInventory, &protocol.ChainInventory{
		Ids: []*protocol.BlockID{{Hash: id12, Number: 12}},
	}); err != nil {
		return err
	}
	return connection.Close()
}

func syncTestBlock(number int64, parent []byte, timestamp int64) *protocol.Block {
	return &protocol.Block{BlockHeader: &protocol.BlockHeader{RawData: &protocol.BlockHeaderRaw{
		Number: number, ParentHash: bytes.Clone(parent), Timestamp: timestamp,
	}}}
}

func syncBlockID(number int64, fill byte) []byte {
	result := bytes.Repeat([]byte{fill}, 32)
	binary.BigEndian.PutUint64(result[:8], uint64(number))
	return result
}
