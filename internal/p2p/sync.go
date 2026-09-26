package p2p

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/yooud/tronwatch/internal/model"
	"github.com/yooud/tronwatch/internal/protocol"
)

const (
	maxChainInventoryIDs = 2001
	catchupFetchInterval = 350 * time.Millisecond
)

type catchupSession struct {
	active             bool
	awaitingInventory  bool
	locator            []*protocol.BlockID
	queue              []*protocol.BlockID
	pending            map[string]*protocol.BlockID
	target             int64
	downloaded         int64
	lastFetch          time.Time
	historyUnavailable bool
}

type catchupObserver interface {
	CatchupState(active bool, peer string, current, target int64)
	CatchupBlocks(count int)
	CatchupFailover()
	CatchupFailure()
}

type catchupCoordinator struct {
	mu          sync.RWMutex
	leader      string
	neededValue bool
	current     int64
	target      int64
	cooldown    time.Duration
	failedUntil map[string]time.Time
	observer    catchupObserver
	now         func() time.Time
}

func newCatchupCoordinator(cooldown time.Duration, observer any) *catchupCoordinator {
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}
	result := &catchupCoordinator{
		cooldown: cooldown, failedUntil: make(map[string]time.Time), now: time.Now,
	}
	result.observer, _ = observer.(catchupObserver)
	return result
}

func (c *catchupCoordinator) observe(current, target int64) {
	c.mu.Lock()
	if current > c.current {
		c.current = current
	}
	if target > c.target {
		c.target = target
	}
	c.neededValue = c.target > c.current
	active, peer, observedCurrent, observedTarget := c.neededValue, c.leader, c.current, c.target
	c.mu.Unlock()
	c.notifyState(active, peer, observedCurrent, observedTarget)
}

func (c *catchupCoordinator) tryAcquire(peer string) bool {
	c.mu.Lock()
	if !c.neededValue || (c.leader != "" && c.leader != peer) || c.now().Before(c.failedUntil[peer]) {
		c.mu.Unlock()
		return false
	}
	c.leader = peer
	current, target := c.current, c.target
	c.mu.Unlock()
	c.notifyState(true, peer, current, target)
	return true
}

func (c *catchupCoordinator) progress(peer string, current int64, blocks int) {
	c.mu.Lock()
	if c.leader != peer {
		c.mu.Unlock()
		return
	}
	c.current = current
	currentTarget := c.target
	c.mu.Unlock()
	if c.observer != nil {
		c.observer.CatchupBlocks(blocks)
	}
	c.notifyState(true, peer, current, currentTarget)
}

func (c *catchupCoordinator) fail(peer string) {
	c.mu.Lock()
	if c.leader != peer {
		c.mu.Unlock()
		return
	}
	c.leader = ""
	c.failedUntil[peer] = c.now().Add(c.cooldown)
	active, current, target := c.neededValue, c.current, c.target
	c.mu.Unlock()
	if c.observer != nil {
		c.observer.CatchupFailure()
		c.observer.CatchupFailover()
	}
	c.notifyState(active, "", current, target)
}

func (c *catchupCoordinator) complete(peer string, current int64) bool {
	c.mu.Lock()
	if c.leader != peer {
		c.mu.Unlock()
		return false
	}
	c.current = current
	if current < c.target {
		c.leader = ""
		c.failedUntil[peer] = c.now().Add(c.cooldown)
		target := c.target
		c.mu.Unlock()
		if c.observer != nil {
			c.observer.CatchupFailover()
		}
		c.notifyState(true, "", current, target)
		return false
	}
	c.target = current
	c.neededValue = false
	c.leader = ""
	target := c.target
	c.mu.Unlock()
	c.notifyState(false, "", current, target)
	return true
}

func (c *catchupCoordinator) stop(peer string) {
	c.mu.Lock()
	if c.leader != peer {
		c.mu.Unlock()
		return
	}
	c.leader = ""
	active, current, target := c.neededValue, c.current, c.target
	c.mu.Unlock()
	c.notifyState(active, "", current, target)
}

func (c *catchupCoordinator) needed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.neededValue
}

func (c *catchupCoordinator) isLeader(peer string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader == peer
}

func (c *catchupCoordinator) notifyState(active bool, peer string, current, target int64) {
	if c.observer != nil {
		c.observer.CatchupState(active, peer, current, target)
	}
}

func validateChainInventory(
	inventory *protocol.ChainInventory,
	locator []*protocol.BlockID,
) ([]*protocol.BlockID, int64, error) {
	if inventory == nil || len(inventory.Ids) == 0 {
		return nil, 0, errors.New("chain inventory is empty")
	}
	if len(inventory.Ids) > maxChainInventoryIDs {
		return nil, 0, fmt.Errorf("chain inventory contains %d ids, limit is %d", len(inventory.Ids), maxChainInventoryIDs)
	}
	if inventory.RemainNum < 0 {
		return nil, 0, errors.New("chain inventory remaining count is negative")
	}
	for index, id := range inventory.Ids {
		if err := validateSyncBlockID(id); err != nil {
			return nil, 0, fmt.Errorf("chain inventory id %d is invalid", index)
		}
		if index > 0 && id.Number != inventory.Ids[index-1].Number+1 {
			return nil, 0, errors.New("chain inventory heights are not contiguous")
		}
	}
	common := inventory.Ids[0]
	linked := false
	for _, requested := range locator {
		if requested != nil && requested.Number == common.Number && bytes.Equal(requested.Hash, common.Hash) {
			linked = true
			break
		}
	}
	if !linked {
		return nil, 0, errors.New("chain inventory does not link to the requested locator")
	}
	last := inventory.Ids[len(inventory.Ids)-1]
	if inventory.RemainNum > (1<<63-1)-last.Number {
		return nil, 0, errors.New("chain inventory target height overflows")
	}
	missing := make([]*protocol.BlockID, 0, len(inventory.Ids)-1)
	for _, id := range inventory.Ids[1:] {
		missing = append(missing, cloneBlockID(id))
	}
	return missing, last.Number + inventory.RemainNum, nil
}

func validateSyncBlockID(id *protocol.BlockID) error {
	if id == nil || id.Number < 0 || len(id.Hash) != 32 {
		return errors.New("block id shape is invalid")
	}
	if binary.BigEndian.Uint64(id.Hash[:8]) != uint64(id.Number) {
		return errors.New("block id height prefix does not match its number")
	}
	return nil
}

func observeAdvertisedHead(payload []byte, hello *protocol.TronHello) error {
	if hello == nil {
		return errors.New("peer hello is nil")
	}
	var inventory protocol.Inventory
	if err := proto.Unmarshal(payload, &inventory); err != nil {
		return fmt.Errorf("decoding inventory: %w", err)
	}
	if inventory.Type != protocol.Inventory_BLOCK {
		return nil
	}
	for _, hash := range inventory.Ids {
		if len(hash) != 32 {
			return fmt.Errorf("inventory id length is %d, want 32", len(hash))
		}
		number := int64(binary.BigEndian.Uint64(hash[:8]))
		if number < 0 {
			return errors.New("advertised block height overflows int64")
		}
		if hello.HeadBlockId == nil || number > hello.HeadBlockId.Number {
			hello.HeadBlockId = &protocol.BlockID{Hash: bytes.Clone(hash), Number: number}
		}
	}
	return nil
}

func shouldStartCatchupFromInventory(status model.ChainStatus, head int64, alreadyNeeded bool) bool {
	if !status.Anchored || head <= status.TipNumber {
		return false
	}
	return alreadyNeeded || status.UnresolvedBlocks > 0 || head-status.TipNumber > 1
}

func (c *Client) maybeStartCatchupFromInventory(
	connection net.Conn,
	peerHello *protocol.TronHello,
	session *catchupSession,
) error {
	if !c.config.CatchupEnabled || c.catchup == nil || peerHello == nil || peerHello.HeadBlockId == nil {
		return nil
	}
	status, err := c.config.Chain.ChainStatus()
	if err != nil {
		return fmt.Errorf("reading live catch-up cursor: %w", err)
	}
	if !shouldStartCatchupFromInventory(status, peerHello.HeadBlockId.Number, c.catchup.needed()) {
		return nil
	}
	return c.startCatchup(connection, peerHello, session, status)
}

func (c *Client) maybeStartCatchup(
	connection net.Conn,
	peerHello *protocol.TronHello,
	session *catchupSession,
) error {
	if !c.config.CatchupEnabled || session.active || c.catchup == nil || peerHello == nil || peerHello.HeadBlockId == nil {
		return nil
	}
	status, err := c.config.Chain.ChainStatus()
	if err != nil {
		return fmt.Errorf("reading catch-up cursor: %w", err)
	}
	return c.startCatchup(connection, peerHello, session, status)
}

func (c *Client) startCatchup(
	connection net.Conn,
	peerHello *protocol.TronHello,
	session *catchupSession,
	status model.ChainStatus,
) error {
	if !status.Anchored || peerHello.HeadBlockId.Number <= status.TipNumber {
		return nil
	}
	c.catchup.observe(status.TipNumber, peerHello.HeadBlockId.Number)
	if peerHello.LowestBlockNum > 0 && status.TipNumber < peerHello.LowestBlockNum-1 {
		if !session.historyUnavailable {
			c.logger.Warn(
				"peer cannot serve catch-up range",
				"peer", c.config.Peer, "local_head", status.TipNumber,
				"peer_lowest", peerHello.LowestBlockNum,
			)
			session.historyUnavailable = true
		}
		return nil
	}
	if !c.catchup.tryAcquire(c.config.Peer) {
		return nil
	}
	session.active = true
	session.pending = make(map[string]*protocol.BlockID)
	c.logger.Info(
		"historical P2P catch-up started",
		"peer", c.config.Peer, "from", status.TipNumber, "target", peerHello.HeadBlockId.Number,
	)
	if err := c.requestChainInventory(connection, peerHello, session); err != nil {
		c.catchup.fail(c.config.Peer)
		session.active = false
		return err
	}
	return nil
}

func (c *Client) requestChainInventory(
	connection net.Conn,
	peerHello *protocol.TronHello,
	session *catchupSession,
) error {
	refs, err := c.config.Chain.ChainLocator(maxSyncLocatorIDs - 1)
	if err != nil {
		return fmt.Errorf("building catch-up locator: %w", err)
	}
	if len(refs) == 0 {
		return errors.New("catch-up locator is empty")
	}
	if err := validateSyncBlockID(peerHello.GenesisBlockId); err != nil {
		return fmt.Errorf("peer genesis block id is invalid: %w", err)
	}
	locator := make([]*protocol.BlockID, 0, len(refs)+1)
	locator = append(locator, cloneBlockID(peerHello.GenesisBlockId))
	for _, ref := range refs {
		hash, decodeErr := hex.DecodeString(ref.ID)
		if decodeErr != nil || len(hash) != 32 {
			return fmt.Errorf("decoding canonical block %d id %q", ref.Number, ref.ID)
		}
		id := &protocol.BlockID{Hash: hash, Number: ref.Number}
		if err := validateSyncBlockID(id); err != nil {
			return fmt.Errorf("canonical block %d id is invalid: %w", ref.Number, err)
		}
		if id.Number == locator[len(locator)-1].Number && bytes.Equal(id.Hash, locator[len(locator)-1].Hash) {
			continue
		}
		locator = append(locator, id)
	}
	if len(locator) > maxSyncLocatorIDs {
		return fmt.Errorf("catch-up locator contains %d ids, limit is %d", len(locator), maxSyncLocatorIDs)
	}
	if err := sendCompressedMessage(connection, messageSyncBlockChain, &protocol.BlockInventory{
		Ids: locator, Type: protocol.BlockInventory_SYNC,
	}); err != nil {
		return fmt.Errorf("requesting chain inventory: %w", err)
	}
	session.locator = locator
	session.awaitingInventory = true
	return c.setCatchupDeadline(connection)
}

func (c *Client) handleChainInventory(
	connection net.Conn,
	payload []byte,
	requested map[string]time.Time,
	peerHello *protocol.TronHello,
	session *catchupSession,
) error {
	if !session.active || !session.awaitingInventory {
		return errors.New("unsolicited chain inventory")
	}
	var inventory protocol.ChainInventory
	if err := proto.Unmarshal(payload, &inventory); err != nil {
		return fmt.Errorf("decoding chain inventory: %w", err)
	}
	missing, target, err := validateChainInventory(&inventory, session.locator)
	if err != nil {
		return err
	}
	status, err := c.config.Chain.ChainStatus()
	if err != nil {
		return fmt.Errorf("reading chain status after inventory: %w", err)
	}
	common := inventory.Ids[0]
	if common.Number < status.AnchorNumber {
		return fmt.Errorf("chain inventory common height %d is below local anchor %d", common.Number, status.AnchorNumber)
	}
	session.awaitingInventory = false
	session.target = target
	c.catchup.observe(status.TipNumber, target)
	if len(missing) == 0 {
		commonID := hex.EncodeToString(common.Hash)
		if common.Number != status.TipNumber || commonID != status.TipID || target > status.TipNumber {
			return errors.New("chain inventory ended before the persisted tip was confirmed")
		}
		if err := connection.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, net.ErrClosed) {
			c.logger.Debug("clearing catch-up deadline failed", "peer", c.config.Peer, "error", err)
		}
		completed := c.catchup.complete(c.config.Peer, status.TipNumber)
		session.active = false
		if completed {
			c.logger.Info(
				"historical P2P catch-up completed",
				"peer", c.config.Peer, "height", status.TipNumber, "downloaded_blocks", session.downloaded,
			)
		} else {
			c.logger.Info(
				"historical P2P catch-up peer exhausted",
				"peer", c.config.Peer, "height", status.TipNumber, "downloaded_blocks", session.downloaded,
			)
		}
		return nil
	}
	session.queue = missing
	clear(session.pending)
	return c.requestNextCatchupBatch(connection, requested, peerHello, session)
}

func (c *Client) requestNextCatchupBatch(
	connection net.Conn,
	requested map[string]time.Time,
	peerHello *protocol.TronHello,
	session *catchupSession,
) error {
	if len(session.pending) != 0 {
		return errors.New("cannot request catch-up batch while blocks are pending")
	}
	if len(session.queue) == 0 {
		return c.requestChainInventory(connection, peerHello, session)
	}
	count := min(c.config.CatchupBatchSize, len(session.queue))
	batch := session.queue[:count]
	session.queue = session.queue[count:]
	if delay := catchupFetchDelay(session.lastFetch, time.Now()); delay > 0 {
		time.Sleep(delay)
	}
	fetch := &protocol.Inventory{Type: protocol.Inventory_BLOCK, Ids: make([][]byte, 0, count)}
	requestedAt := time.Now()
	for _, id := range batch {
		key := hex.EncodeToString(id.Hash)
		session.pending[key] = id
		requested[string(append([]byte{byte(protocol.Inventory_BLOCK)}, id.Hash...))] = requestedAt
		fetch.Ids = append(fetch.Ids, bytes.Clone(id.Hash))
	}
	if err := sendCompressedMessage(connection, messageFetchData, fetch); err != nil {
		return fmt.Errorf("requesting catch-up blocks: %w", err)
	}
	session.lastFetch = time.Now()
	return c.setCatchupDeadline(connection)
}

func catchupFetchDelay(lastFetch, now time.Time) time.Duration {
	if lastFetch.IsZero() {
		return 0
	}
	return max(0, catchupFetchInterval-now.Sub(lastFetch))
}

func (c *Client) catchupBlockApplied(
	connection net.Conn,
	requested map[string]time.Time,
	peerHello *protocol.TronHello,
	session *catchupSession,
	block BlockObservation,
) error {
	expected, exists := session.pending[block.ID]
	if !exists {
		return nil
	}
	if expected.Number != block.Number {
		return fmt.Errorf("catch-up block %s height is %d, want %d", block.ID, block.Number, expected.Number)
	}
	delete(session.pending, block.ID)
	session.downloaded++
	status, err := c.config.Chain.ChainStatus()
	if err != nil {
		return fmt.Errorf("reading catch-up progress: %w", err)
	}
	c.catchup.progress(c.config.Peer, status.TipNumber, 1)
	if session.downloaded%1000 == 0 {
		c.logger.Info(
			"historical P2P catch-up progress",
			"peer", c.config.Peer, "height", status.TipNumber, "target", session.target,
		)
	}
	if len(session.pending) == 0 {
		return c.requestNextCatchupBatch(connection, requested, peerHello, session)
	}
	return c.setCatchupDeadline(connection)
}

func (c *Client) setCatchupDeadline(connection net.Conn) error {
	if err := connection.SetReadDeadline(time.Now().Add(c.config.CatchupRequestTimeout)); err != nil {
		return fmt.Errorf("setting catch-up deadline: %w", err)
	}
	return nil
}
