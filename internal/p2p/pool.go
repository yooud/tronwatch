package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/yooud/tronwatch/internal/protocol"
)

// Pool runs independent reconnecting peer clients into one serialized transaction stream.
type Pool struct {
	clients []*Client
}

// NewPool creates one client per unique peer and gives all clients the same node identity.
func NewPool(peers []string, base Config, handler Handler) (*Pool, error) {
	if len(peers) == 0 {
		return nil, errors.New("peer list is empty")
	}
	serialized, err := newSerializedHandler(handler)
	if err != nil {
		return nil, err
	}
	nodeID, err := prepareNodeID(base.NodeID)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(peers))
	pool := &Pool{clients: make([]*Client, 0, len(peers))}
	for _, endpoint := range peers {
		endpoint = strings.TrimSpace(endpoint)
		host, portText, splitErr := net.SplitHostPort(endpoint)
		if splitErr != nil {
			return nil, fmt.Errorf("invalid peer %q: %w", endpoint, splitErr)
		}
		port, portErr := strconv.Atoi(portText)
		if strings.TrimSpace(host) == "" || portErr != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("invalid peer %q: host and port are required", endpoint)
		}
		if _, exists := seen[endpoint]; exists {
			return nil, fmt.Errorf("peer %q is duplicated", endpoint)
		}
		seen[endpoint] = struct{}{}
		peerConfig := base
		peerConfig.Peer = endpoint
		peerConfig.NodeID = nodeID
		client, newErr := New(peerConfig, serialized)
		if newErr != nil {
			return nil, fmt.Errorf("creating peer %q: %w", endpoint, newErr)
		}
		pool.clients = append(pool.clients, client)
	}
	return pool, nil
}

// Size reports the number of configured peer clients.
func (p *Pool) Size() int { return len(p.clients) }

// Run starts every peer client and stops them together when ctx is canceled.
func (p *Pool) Run(ctx context.Context) error {
	group, groupContext := errgroup.WithContext(ctx)
	for _, client := range p.clients {
		client := client
		group.Go(func() error { return client.Run(groupContext) })
	}
	return group.Wait()
}

type serializedHandler struct {
	next  Handler
	token chan struct{}
}

func newSerializedHandler(next Handler) (*serializedHandler, error) {
	if next == nil {
		return nil, errors.New("transaction handler is nil")
	}
	return &serializedHandler{next: next, token: make(chan struct{}, 1)}, nil
}

func (h *serializedHandler) HandleTransaction(
	ctx context.Context,
	transaction *protocol.Transaction,
	observation Observation,
) error {
	select {
	case h.token <- struct{}{}:
		defer func() { <-h.token }()
	case <-ctx.Done():
		return ctx.Err()
	}
	return h.next.HandleTransaction(ctx, transaction, observation)
}

// HandleBlock keeps every block boundary intact even when several peers deliver concurrently.
func (h *serializedHandler) HandleBlock(
	ctx context.Context,
	block BlockObservation,
	transactions []*protocol.Transaction,
) error {
	select {
	case h.token <- struct{}{}:
		defer func() { <-h.token }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if next, ok := h.next.(BlockHandler); ok {
		return next.HandleBlock(ctx, block, transactions)
	}
	blockNumber := block.Number
	blockTime := block.Time
	observation := Observation{
		Source: SourceBlock, Peer: block.Peer, ObservedAt: block.ObservedAt,
		BlockNumber: &blockNumber, BlockTime: &blockTime,
	}
	for _, transaction := range transactions {
		if err := h.next.HandleTransaction(ctx, transaction, observation); err != nil {
			return err
		}
	}
	return nil
}

func prepareNodeID(configured []byte) ([]byte, error) {
	nodeID := bytes.Clone(configured)
	if len(nodeID) == 0 {
		nodeID = make([]byte, nodeIDLength)
		if _, err := io.ReadFull(rand.Reader, nodeID); err != nil {
			return nil, fmt.Errorf("generating node id: %w", err)
		}
	}
	if len(nodeID) != nodeIDLength {
		return nil, fmt.Errorf("node id length is %d, want %d", len(nodeID), nodeIDLength)
	}
	return nodeID, nil
}
