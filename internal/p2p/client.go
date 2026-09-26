package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/yooud/tronwatch/internal/model"
	"github.com/yooud/tronwatch/internal/protocol"
	appversion "github.com/yooud/tronwatch/internal/version"
)

const (
	transportVersion           = int32(1)
	transportRecentDisconnect  = int32(3)
	nodeIDLength               = 64
	maxFetchIDs                = 100
	maxSyncLocatorIDs          = 30
	recentDisconnectRetryDelay = 65 * time.Second
)

type transportRejectError struct {
	code int32
}

func (e *transportRejectError) Error() string {
	return fmt.Sprintf("transport handshake rejected with code %d", e.code)
}

// Source identifies whether a transaction was first observed before or in a block.
type Source string

const (
	SourceMempool Source = "mempool"
	SourceBlock   Source = "block"
)

// Observation carries P2P context for a decoded transaction.
type Observation struct {
	Source      Source
	Peer        string
	ObservedAt  time.Time
	BlockNumber *int64
	BlockTime   *time.Time
}

// BlockObservation carries the complete identity of one block so downstream
// processing can atomically decide whether it belongs to the canonical branch.
type BlockObservation struct {
	ID         string
	ParentID   string
	Number     int64
	Time       time.Time
	Peer       string
	ObservedAt time.Time
}

// Handler consumes transactions synchronously. Returning an error reconnects the peer.
type Handler interface {
	HandleTransaction(context.Context, *protocol.Transaction, Observation) error
}

// BlockHandler receives a whole block under one stream serialization lock.
// Handlers that do not implement it retain the legacy transaction callback.
type BlockHandler interface {
	HandleBlock(context.Context, BlockObservation, []*protocol.Transaction) error
}

// HandlerFunc adapts a function into a Handler.
type HandlerFunc func(context.Context, *protocol.Transaction, Observation) error

// HandleTransaction invokes f.
func (f HandlerFunc) HandleTransaction(
	ctx context.Context,
	transaction *protocol.Transaction,
	observation Observation,
) error {
	return f(ctx, transaction, observation)
}

// Config configures one outbound TRON peer connection.
type Config struct {
	Peer                  string
	NetworkID             int32
	AdvertiseIP           string
	AdvertisePort         int32
	NodeID                []byte
	DialTimeout           time.Duration
	HandshakeTimeout      time.Duration
	ReconnectMin          time.Duration
	ReconnectMax          time.Duration
	Logger                *slog.Logger
	Observer              PeerObserver
	CatchupEnabled        bool
	CatchupBatchSize      int
	CatchupRequestTimeout time.Duration
	Chain                 ChainReader
	coordinator           *catchupCoordinator
}

// ChainReader exposes the durable canonical cursor needed for historical synchronization.
type ChainReader interface {
	ChainStatus() (model.ChainStatus, error)
	ChainLocator(maxIDs int) ([]model.BlockRef, error)
}

// PeerObserver receives bounded operational session state without transaction identifiers.
type PeerObserver interface {
	PeerConnected(peer string)
	PeerHandshaked(peer string, headNumber, solidNumber int64)
	PeerDisconnected(peer string, cause error)
}

// Client is a reconnecting, outbound-only TRON P2P client.
type Client struct {
	config  Config
	handler Handler
	nodeID  []byte
	logger  *slog.Logger
	catchup *catchupCoordinator
}

// New validates configuration and creates a client.
func New(config Config, handler Handler) (*Client, error) {
	if config.NetworkID <= 0 {
		return nil, errors.New("network id must be positive")
	}
	if parsed := net.ParseIP(config.AdvertiseIP); parsed == nil || parsed.To4() == nil {
		return nil, fmt.Errorf("advertise IP %q is not IPv4", config.AdvertiseIP)
	}
	if handler == nil {
		return nil, errors.New("transaction handler is nil")
	}
	nodeID, err := prepareNodeID(config.NodeID)
	if err != nil {
		return nil, err
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = 3 * time.Second
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if config.ReconnectMin <= 0 {
		config.ReconnectMin = time.Second
	}
	if config.ReconnectMax <= 0 {
		config.ReconnectMax = 30 * time.Second
	}
	if config.ReconnectMax < config.ReconnectMin {
		return nil, errors.New("reconnect maximum is below minimum")
	}
	if config.CatchupEnabled {
		if config.Chain == nil {
			return nil, errors.New("catch-up chain reader is nil")
		}
		if config.CatchupBatchSize < 1 || config.CatchupBatchSize > maxFetchIDs {
			return nil, fmt.Errorf("catch-up batch size must be between 1 and %d", maxFetchIDs)
		}
		if config.CatchupRequestTimeout <= 0 {
			return nil, errors.New("catch-up request timeout must be positive")
		}
		if config.coordinator == nil {
			config.coordinator = newCatchupCoordinator(config.CatchupRequestTimeout, config.Observer)
		}
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		config: config, handler: handler, nodeID: nodeID, logger: logger,
		catchup: config.coordinator,
	}, nil
}

// Run reconnects to the configured peer until the context is canceled.
func (c *Client) Run(ctx context.Context) error {
	if c.config.Peer == "" {
		return errors.New("peer address is empty")
	}
	backoff := c.config.ReconnectMin
	for {
		handshakeCompleted := false
		dialer := net.Dialer{Timeout: c.config.DialTimeout}
		connection, err := dialer.DialContext(ctx, "tcp", c.config.Peer)
		if err == nil {
			c.logger.Info("P2P peer connected", "peer", c.config.Peer)
			if c.config.Observer != nil {
				c.config.Observer.PeerConnected(c.config.Peer)
			}
			handshakeCompleted, err = c.runSessionOutcome(ctx, connection)
			if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				err = errors.Join(err, closeErr)
			}
		}
		if ctx.Err() != nil {
			if c.config.Observer != nil {
				c.config.Observer.PeerDisconnected(c.config.Peer, ctx.Err())
			}
			return nil
		}
		if c.config.Observer != nil {
			c.config.Observer.PeerDisconnected(c.config.Peer, err)
		}
		if handshakeCompleted {
			backoff = c.config.ReconnectMin
		}
		delay := reconnectDelay(err, backoff)
		c.logger.Warn("P2P session ended; reconnecting", "peer", c.config.Peer, "error", err, "after", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil
		case <-timer.C:
		}
		backoff = advanceBackoff(
			handshakeCompleted, backoff, c.config.ReconnectMin, c.config.ReconnectMax,
		)
	}
}

func (c *Client) runSessionOutcome(ctx context.Context, connection net.Conn) (bool, error) {
	stopClose := context.AfterFunc(ctx, func() {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.logger.Debug("closing canceled P2P connection", "error", err)
		}
	})
	defer stopClose()
	if err := connection.SetDeadline(time.Now().Add(c.config.HandshakeTimeout)); err != nil {
		return false, fmt.Errorf("setting handshake deadline: %w", err)
	}
	startedAt := time.Now().UnixMilli()
	if err := c.sendTransportHello(connection, startedAt); err != nil {
		return false, err
	}
	transportFrame, err := readFrame(connection, maxMessageSize)
	if err != nil {
		return false, fmt.Errorf("reading transport hello: %w", err)
	}
	if err := c.validateTransportHello(transportFrame); err != nil {
		return false, err
	}
	nodeHello, err := c.receiveTronHello(connection)
	if err != nil {
		return false, err
	}
	if err := c.sendTronHello(connection, startedAt, nodeHello); err != nil {
		return false, err
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return false, fmt.Errorf("clearing handshake deadline: %w", err)
	}
	c.logger.Info(
		"TRON handshake completed",
		"peer", c.config.Peer,
		"head", nodeHello.HeadBlockId.Number,
		"version", string(nodeHello.CodeVersion),
	)
	if c.config.Observer != nil {
		c.config.Observer.PeerHandshaked(c.config.Peer, nodeHello.HeadBlockId.Number, nodeHello.SolidBlockId.Number)
	}
	return true, c.readMessages(ctx, connection, nodeHello)
}

func reconnectDelay(err error, backoff time.Duration) time.Duration {
	var rejected *transportRejectError
	if errors.As(err, &rejected) && rejected.code == transportRecentDisconnect {
		return max(backoff, recentDisconnectRetryDelay)
	}
	return backoff
}

func advanceBackoff(
	handshakeCompleted bool,
	current time.Duration,
	minimum time.Duration,
	maximum time.Duration,
) time.Duration {
	if handshakeCompleted {
		return minimum
	}
	return min(current*2, maximum)
}

func (c *Client) sendTransportHello(connection net.Conn, timestamp int64) error {
	hello := &protocol.TransportHello{
		From: &protocol.Endpoint{
			Address: []byte(c.config.AdvertiseIP),
			Port:    c.config.AdvertisePort,
			NodeId:  c.nodeID,
		},
		NetworkId: c.config.NetworkID,
		Timestamp: timestamp,
		Version:   transportVersion,
	}
	return sendUncompressedMessage(connection, transportHello, hello)
}

func (c *Client) validateTransportHello(frame []byte) error {
	if len(frame) < 2 || frame[0] != transportHello {
		return fmt.Errorf("unexpected transport handshake message %x", frame)
	}
	var hello protocol.TransportHello
	if err := proto.Unmarshal(frame[1:], &hello); err != nil {
		return fmt.Errorf("decoding transport hello: %w", err)
	}
	if hello.Code != 0 {
		return &transportRejectError{code: hello.Code}
	}
	if hello.NetworkId != c.config.NetworkID && hello.Version != c.config.NetworkID {
		return fmt.Errorf("peer network id is %d, want %d", hello.NetworkId, c.config.NetworkID)
	}
	if hello.From == nil || len(hello.From.NodeId) != nodeIDLength {
		return errors.New("peer transport hello has invalid node id")
	}
	return nil
}

func (c *Client) receiveTronHello(connection net.Conn) (*protocol.TronHello, error) {
	for {
		message, err := receiveCompressedMessage(connection)
		if err != nil {
			return nil, fmt.Errorf("reading TRON hello: %w", err)
		}
		if len(message) == 0 {
			return nil, errors.New("empty post-handshake message")
		}
		switch message[0] {
		case transportPing:
			if err := sendTransportPong(connection); err != nil {
				return nil, err
			}
		case transportDisconnect:
			return nil, errors.New("peer disconnected during transport handshake")
		case messagePing:
			if err := sendCompressedRaw(connection, append([]byte{messagePong}, 0xc0)); err != nil {
				return nil, err
			}
		case messageHello:
			var hello protocol.TronHello
			if err := proto.Unmarshal(message[1:], &hello); err != nil {
				return nil, fmt.Errorf("decoding TRON hello: %w", err)
			}
			if err := c.validateTronHello(&hello); err != nil {
				return nil, err
			}
			return &hello, nil
		default:
			return nil, fmt.Errorf("unexpected message %02x before TRON hello", message[0])
		}
	}
}

func (c *Client) validateTronHello(hello *protocol.TronHello) error {
	if hello.Version != c.config.NetworkID {
		return fmt.Errorf("TRON P2P version is %d, want %d", hello.Version, c.config.NetworkID)
	}
	for name, blockID := range map[string]*protocol.BlockID{
		"genesis": hello.GenesisBlockId,
		"solid":   hello.SolidBlockId,
		"head":    hello.HeadBlockId,
	} {
		if blockID == nil || len(blockID.Hash) != 32 {
			return fmt.Errorf("peer %s block id is invalid", name)
		}
	}
	return nil
}

func (c *Client) sendTronHello(
	connection net.Conn,
	timestamp int64,
	peerHello *protocol.TronHello,
) error {
	hello := &protocol.TronHello{
		From: &protocol.Endpoint{
			Address: []byte(c.config.AdvertiseIP),
			Port:    c.config.AdvertisePort,
			NodeId:  c.nodeID,
		},
		Version:        c.config.NetworkID,
		Timestamp:      timestamp,
		GenesisBlockId: cloneBlockID(peerHello.GenesisBlockId),
		SolidBlockId:   cloneBlockID(peerHello.SolidBlockId),
		HeadBlockId:    cloneBlockID(peerHello.HeadBlockId),
		CodeVersion:    []byte("tronwatch/" + appversion.Value),
	}
	return sendCompressedMessage(connection, messageHello, hello)
}

func cloneBlockID(blockID *protocol.BlockID) *protocol.BlockID {
	return &protocol.BlockID{Hash: bytes.Clone(blockID.Hash), Number: blockID.Number}
}

func (c *Client) readMessages(ctx context.Context, connection net.Conn, peerHello *protocol.TronHello) error {
	requested := make(map[string]time.Time)
	session := catchupSession{}
	if err := c.maybeStartCatchup(connection, peerHello, &session); err != nil {
		return err
	}
	defer func() {
		if session.active && c.catchup != nil && c.catchup.isLeader(c.config.Peer) {
			if ctx.Err() == nil {
				c.catchup.fail(c.config.Peer)
			} else {
				c.catchup.stop(c.config.Peer)
			}
		}
	}()
	for {
		message, err := receiveCompressedMessage(connection)
		if err != nil {
			return err
		}
		if len(message) == 0 {
			return errors.New("peer sent an empty message")
		}
		switch message[0] {
		case transportPing:
			err = sendTransportPong(connection)
		case transportDisconnect, messageDisconnect:
			return fmt.Errorf("peer disconnected with message %x", message)
		case messagePing:
			err = sendCompressedRaw(connection, append([]byte{messagePong}, 0xc0))
		case messageInventory:
			err = observeAdvertisedHead(message[1:], peerHello)
			if err == nil && !session.active {
				err = c.maybeStartCatchupFromInventory(connection, peerHello, &session)
			}
			if err == nil {
				allowBlocks := c.catchup == nil || !c.catchup.needed()
				err = c.fetchInventory(connection, message[1:], requested, allowBlocks)
			}
		case messageBlockChainInventory:
			err = c.handleChainInventory(connection, message[1:], requested, peerHello, &session)
		case messageBlock:
			var block BlockObservation
			block, err = c.handleRequestedBlockObservation(ctx, message[1:], requested)
			if err == nil && session.active {
				err = c.catchupBlockApplied(connection, requested, peerHello, &session, block)
			}
		case messageTransactions:
			err = c.handleTransactions(ctx, message[1:])
		case messageTransaction:
			err = c.handleTransaction(ctx, message[1:], Observation{Source: SourceMempool})
		case messageItemNotFound:
			if session.active {
				err = errors.New("peer could not provide a requested catch-up block")
			}
		default:
			continue
		}
		if err != nil {
			return err
		}
	}
}

func (c *Client) fetchInventory(
	connection net.Conn,
	payload []byte,
	requested map[string]time.Time,
	allowBlocks bool,
) error {
	var inventory protocol.Inventory
	if err := proto.Unmarshal(payload, &inventory); err != nil {
		return fmt.Errorf("decoding inventory: %w", err)
	}
	if inventory.Type != protocol.Inventory_TRX && inventory.Type != protocol.Inventory_BLOCK {
		return fmt.Errorf("unsupported inventory type %d", inventory.Type)
	}
	for _, id := range inventory.Ids {
		if len(id) != 32 {
			return fmt.Errorf("inventory id length is %d, want 32", len(id))
		}
	}
	if inventory.Type == protocol.Inventory_BLOCK && !allowBlocks {
		return nil
	}
	trimRequested(requested, time.Now())
	for start := 0; start < len(inventory.Ids); {
		fetch := &protocol.Inventory{Type: inventory.Type}
		for start < len(inventory.Ids) && len(fetch.Ids) < maxFetchIDs {
			id := inventory.Ids[start]
			start++
			key := string(append([]byte{byte(inventory.Type)}, id...))
			if _, exists := requested[key]; exists {
				continue
			}
			requested[key] = time.Now()
			fetch.Ids = append(fetch.Ids, bytes.Clone(id))
		}
		if len(fetch.Ids) > 0 {
			if err := sendCompressedMessage(connection, messageFetchData, fetch); err != nil {
				return fmt.Errorf("requesting inventory: %w", err)
			}
		}
	}
	return nil
}

func trimRequested(requested map[string]time.Time, now time.Time) {
	if len(requested) < 10_000 {
		return
	}
	cutoff := now.Add(-10 * time.Minute)
	for key, requestedAt := range requested {
		if requestedAt.Before(cutoff) {
			delete(requested, key)
		}
	}
}

func (c *Client) handleBlock(ctx context.Context, payload []byte) error {
	return c.handleRequestedBlock(ctx, payload, nil)
}

func (c *Client) handleRequestedBlock(ctx context.Context, payload []byte, requested map[string]time.Time) error {
	_, err := c.handleRequestedBlockObservation(ctx, payload, requested)
	return err
}

func (c *Client) handleRequestedBlockObservation(
	ctx context.Context,
	payload []byte,
	requested map[string]time.Time,
) (BlockObservation, error) {
	var block protocol.Block
	if err := proto.Unmarshal(payload, &block); err != nil {
		return BlockObservation{}, fmt.Errorf("decoding block: %w", err)
	}
	if block.BlockHeader == nil || block.BlockHeader.RawData == nil {
		return BlockObservation{}, errors.New("block is missing its header")
	}
	header := block.BlockHeader.RawData
	id, err := blockID(header)
	if err != nil {
		return BlockObservation{}, err
	}
	if requested != nil {
		key := string(append([]byte{byte(protocol.Inventory_BLOCK)}, id...))
		if _, exists := requested[key]; !exists {
			return BlockObservation{}, fmt.Errorf("peer sent unadvertised block %x", id)
		}
		delete(requested, key)
	}
	observedAt := time.Now().UTC()
	blockNumber := header.Number
	blockTime := time.UnixMilli(header.Timestamp).UTC()
	if blockTime.After(observedAt.Add(2 * time.Minute)) {
		return BlockObservation{}, fmt.Errorf("block timestamp %s is too far in the future", blockTime)
	}
	blockObservation := BlockObservation{
		ID: hex.EncodeToString(id), ParentID: hex.EncodeToString(header.ParentHash),
		Number: blockNumber, Time: blockTime, Peer: c.config.Peer, ObservedAt: observedAt,
	}
	if handler, ok := c.handler.(BlockHandler); ok {
		if err := handler.HandleBlock(ctx, blockObservation, block.Transactions); err != nil {
			return BlockObservation{}, fmt.Errorf("handling block: %w", err)
		}
		return blockObservation, nil
	}
	observation := Observation{
		Source: SourceBlock, Peer: c.config.Peer, ObservedAt: observedAt,
		BlockNumber: &blockNumber, BlockTime: &blockTime,
	}
	for _, transaction := range block.Transactions {
		if err := c.handler.HandleTransaction(ctx, transaction, observation); err != nil {
			return BlockObservation{}, fmt.Errorf("handling block transaction: %w", err)
		}
	}
	return blockObservation, nil
}

func blockID(header *protocol.BlockHeaderRaw) ([]byte, error) {
	if header == nil {
		return nil, errors.New("block header raw data is nil")
	}
	if header.Number < 0 {
		return nil, fmt.Errorf("block number %d is negative", header.Number)
	}
	if len(header.ParentHash) != 32 {
		return nil, fmt.Errorf("parent block id length is %d, want 32", len(header.ParentHash))
	}
	encoded, err := proto.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encoding block header: %w", err)
	}
	hash := sha256.Sum256(encoded)
	binary.BigEndian.PutUint64(hash[:8], uint64(header.Number))
	return hash[:], nil
}

func (c *Client) handleTransactions(ctx context.Context, payload []byte) error {
	var transactions protocol.Transactions
	if err := proto.Unmarshal(payload, &transactions); err != nil {
		return fmt.Errorf("decoding transactions: %w", err)
	}
	observation := Observation{Source: SourceMempool, Peer: c.config.Peer, ObservedAt: time.Now().UTC()}
	for _, transaction := range transactions.Transactions {
		if err := c.handler.HandleTransaction(ctx, transaction, observation); err != nil {
			return fmt.Errorf("handling mempool transaction: %w", err)
		}
	}
	return nil
}

func (c *Client) handleTransaction(
	ctx context.Context,
	payload []byte,
	observation Observation,
) error {
	var transaction protocol.Transaction
	if err := proto.Unmarshal(payload, &transaction); err != nil {
		return fmt.Errorf("decoding transaction: %w", err)
	}
	if observation.Peer == "" {
		observation.Peer = c.config.Peer
	}
	if observation.ObservedAt.IsZero() {
		observation.ObservedAt = time.Now().UTC()
	}
	return c.handler.HandleTransaction(ctx, &transaction, observation)
}

func sendUncompressedMessage(connection net.Conn, messageType byte, message proto.Message) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshalling message %02x: %w", messageType, err)
	}
	if err := writeFrame(connection, append([]byte{messageType}, payload...)); err != nil {
		return fmt.Errorf("sending message %02x: %w", messageType, err)
	}
	return nil
}

func sendCompressedMessage(connection net.Conn, messageType byte, message proto.Message) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshalling message %02x: %w", messageType, err)
	}
	return sendCompressedRaw(connection, append([]byte{messageType}, payload...))
}

func sendCompressedRaw(connection net.Conn, payload []byte) error {
	packed, err := wrapCompressed(payload)
	if err != nil {
		return err
	}
	if err := writeFrame(connection, packed); err != nil {
		return fmt.Errorf("sending compressed message: %w", err)
	}
	return nil
}

func receiveCompressedMessage(connection net.Conn) ([]byte, error) {
	frame, err := readFrame(connection, maxMessageSize)
	if err != nil {
		return nil, fmt.Errorf("reading frame: %w", err)
	}
	payload, err := unwrapCompressed(frame)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func sendTransportPong(connection net.Conn) error {
	return sendCompressedMessage(connection, transportPong, &protocol.KeepAliveMessage{
		Timestamp: time.Now().UnixMilli(),
	})
}
