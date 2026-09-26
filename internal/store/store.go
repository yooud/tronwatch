// Package store persists matched transactions in BoltDB.
package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yooud/tronwatch/internal/filter"
	"github.com/yooud/tronwatch/internal/model"
)

var (
	transactionsBucket   = []byte("transactions")
	outboxBucket         = []byte("outbox")
	outboxPayloadsBucket = []byte("outbox_payloads_v1")
	blocksBucket         = []byte("chain_blocks_v1")
	canonicalBucket      = []byte("canonical_blocks_v1")
	candidatesBucket     = []byte("chain_candidates_v1")
	chainMetaBucket      = []byte("chain_meta_v1")
	outboxReferenceValue = []byte{1}

	// ErrNotFound indicates that a transaction is absent from the database.
	ErrNotFound = errors.New("transaction not found")
	// ErrSolidBlockMismatch prevents height-only finalization on a different branch.
	ErrSolidBlockMismatch = errors.New("solid block does not match canonical block")
	// ErrFinalizedFork indicates a branch that would rewrite a solidified checkpoint.
	ErrFinalizedFork = errors.New("fork diverges below finalized checkpoint")
	// ErrForkLimit bounds disk and CPU use from unresolved or malicious branches.
	ErrForkLimit = errors.New("fork candidate limit reached")
)

// DB is a durable, concurrency-safe transaction store.
type DB struct {
	db                       *bolt.DB
	destinations             []string
	persistAllPeers          bool
	maxForkBlocks            int
	finalizedRetentionBlocks int64
	eventPayloadMode         model.EventPayloadMode
	putMu                    sync.Mutex
}

// Options controls durable storage behavior.
type Options struct {
	Destinations             []string
	PersistAllPeers          bool
	SyncFreelist             bool
	MaxForkBlocks            int
	FinalizedRetentionBlocks int64
	EventPayloadMode         model.EventPayloadMode
}

// Open opens a database and creates its parent directory and schema.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, Options{})
}

// OpenWithDestinations opens the database and prepares an independent durable outbox per publisher.
func OpenWithDestinations(path string, destinations []string) (*DB, error) {
	return OpenWithOptions(path, Options{Destinations: destinations})
}

// OpenWithOptions opens the database with explicit retention settings.
func OpenWithOptions(path string, options Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("database path is empty")
	}
	if options.FinalizedRetentionBlocks < 0 {
		return nil, errors.New("finalized retention blocks cannot be negative")
	}
	if options.EventPayloadMode == "" {
		options.EventPayloadMode = model.EventPayloadFull
	}
	if options.EventPayloadMode != model.EventPayloadFull && options.EventPayloadMode != model.EventPayloadCompactLifecycle {
		return nil, fmt.Errorf("unsupported event payload mode %q", options.EventPayloadMode)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("creating database directory: %w", err)
	}
	database, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout:        time.Second,
		NoFreelistSync: !options.SyncFreelist,
	})
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	if options.MaxForkBlocks <= 0 {
		options.MaxForkBlocks = 2048
	}
	result := &DB{
		db: database, destinations: slices.Clone(options.Destinations),
		persistAllPeers: options.PersistAllPeers, maxForkBlocks: options.MaxForkBlocks,
		finalizedRetentionBlocks: options.FinalizedRetentionBlocks,
		eventPayloadMode:         options.EventPayloadMode,
	}
	if err := database.Update(func(transaction *bolt.Tx) error {
		if _, createErr := transaction.CreateBucketIfNotExists(transactionsBucket); createErr != nil {
			return createErr
		}
		outbox, createErr := transaction.CreateBucketIfNotExists(outboxBucket)
		if createErr != nil {
			return createErr
		}
		if _, createErr := transaction.CreateBucketIfNotExists(outboxPayloadsBucket); createErr != nil {
			return createErr
		}
		for _, destination := range options.Destinations {
			if destination == "" {
				return errors.New("publisher destination is empty")
			}
			if _, createErr := outbox.CreateBucketIfNotExists([]byte(destination)); createErr != nil {
				return createErr
			}
		}
		for _, name := range [][]byte{blocksBucket, canonicalBucket, candidatesBucket, chainMetaBucket} {
			if _, createErr := transaction.CreateBucketIfNotExists(name); createErr != nil {
				return createErr
			}
		}
		return nil
	}); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, errors.Join(fmt.Errorf("creating schema: %w", err), closeErr)
		}
		return nil, fmt.Errorf("creating schema: %w", err)
	}
	return result, nil
}

// Close flushes and closes the database.
func (d *DB) Close() error {
	if err := d.db.Close(); err != nil {
		return fmt.Errorf("closing database: %w", err)
	}
	return nil
}

// Check runs BoltDB's structural consistency checker in a read transaction.
func (d *DB) Check() error {
	return d.db.View(func(transaction *bolt.Tx) error {
		var result error
		for checkErr := range transaction.Check() {
			result = errors.Join(result, checkErr)
		}
		return result
	})
}

// Put inserts or idempotently enriches a transaction record.
func (d *DB) Put(record model.Record) error {
	_, err := d.PutIfChanged(record)
	return err
}

// PutIfChanged inserts or enriches a record and reports whether durable state changed.
func (d *DB) PutIfChanged(record model.Record) (bool, error) {
	if record.TxID == "" {
		return false, errors.New("transaction id is empty")
	}
	normalizeRecordState(&record)
	d.putMu.Lock()
	defer d.putMu.Unlock()

	transition := true
	var existingEncoded []byte
	err := d.db.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(transactionsBucket)
		if bucket == nil {
			return errors.New("transactions bucket is missing")
		}
		if existingBytes := bucket.Get([]byte(record.TxID)); existingBytes != nil {
			var existing model.Record
			if err := json.Unmarshal(existingBytes, &existing); err != nil {
				return fmt.Errorf("decoding existing transaction %s: %w", record.TxID, err)
			}
			normalizeRecordState(&existing)
			existingEncoded = bytes.Clone(existingBytes)
			transition = isTransition(existing, record)
			record = mergeRecords(existing, record, d.persistAllPeers || transition)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("encoding transaction %s: %w", record.TxID, err)
	}
	if bytes.Equal(existingEncoded, encoded) {
		return false, nil
	}
	err = d.db.Update(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(transactionsBucket)
		if bucket == nil {
			return errors.New("transactions bucket is missing")
		}
		if err := bucket.Put([]byte(record.TxID), encoded); err != nil {
			return fmt.Errorf("storing transaction %s: %w", record.TxID, err)
		}
		if transition {
			if err := d.enqueueEvent(transaction, record); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// Exists reports whether a transaction was already selected by an earlier observation.
func (d *DB) Exists(txID string) (bool, error) {
	var exists bool
	err := d.db.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(transactionsBucket)
		if bucket == nil {
			return errors.New("transactions bucket is missing")
		}
		exists = bucket.Get([]byte(txID)) != nil
		return nil
	})
	return exists, err
}

// Pending returns durable events awaiting acknowledgement for one publisher.
func (d *DB) Pending(destination string, limit int) ([]model.Event, error) {
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	events := make([]model.Event, 0, min(limit, 128))
	err := d.db.View(func(transaction *bolt.Tx) error {
		bucket, err := destinationBucket(transaction, destination)
		if err != nil {
			return err
		}
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil && len(events) < limit; key, value = cursor.Next() {
			encoded := value
			if bytes.Equal(value, outboxReferenceValue) {
				payloads := transaction.Bucket(outboxPayloadsBucket)
				if payloads == nil {
					return errors.New("shared outbox payload bucket is missing")
				}
				encoded = payloads.Get(key)
				if encoded == nil {
					return fmt.Errorf("shared outbox payload %x is missing", key)
				}
			}
			var event model.Event
			if err := json.Unmarshal(encoded, &event); err != nil {
				return fmt.Errorf("decoding outbox event %s: %w", key, err)
			}
			events = append(events, event)
		}
		return nil
	})
	return events, err
}

// OutboxDepth returns the total durable backlog across publisher destinations.
func (d *DB) OutboxDepth() (int, error) {
	total := 0
	err := d.db.View(func(transaction *bolt.Tx) error {
		outbox := transaction.Bucket(outboxBucket)
		if outbox == nil {
			return errors.New("outbox bucket is missing")
		}
		return outbox.ForEach(func(name, value []byte) error {
			if value == nil {
				if bucket := outbox.Bucket(name); bucket != nil {
					total += bucket.Stats().KeyN
				}
			}
			return nil
		})
	})
	return total, err
}

// Ack permanently removes a successfully delivered event from one publisher outbox.
func (d *DB) Ack(destination, eventID string) error {
	return d.AckBatch(destination, []string{eventID})
}

// AckBatch permanently removes successfully delivered events in one transaction.
func (d *DB) AckBatch(destination string, eventIDs []string) error {
	for _, eventID := range eventIDs {
		if eventID == "" {
			return errors.New("event id is empty")
		}
	}
	if len(eventIDs) == 0 {
		return nil
	}
	return d.db.Update(func(transaction *bolt.Tx) error {
		bucket, err := destinationBucket(transaction, destination)
		if err != nil {
			return err
		}
		for _, eventID := range eventIDs {
			key := outboxStorageKeyForID(eventID)
			if bytes.Equal(bucket.Get(key), outboxReferenceValue) {
				outbox := transaction.Bucket(outboxBucket)
				payloads := transaction.Bucket(outboxPayloadsBucket)
				if outbox == nil || payloads == nil {
					return errors.New("outbox schema is missing")
				}
				if err := deleteSharedOutboxReference(outbox, payloads, bucket, key); err != nil {
					return err
				}
			} else if err := bucket.Delete(key); err != nil {
				return err
			}
			// Compatibility with pre-0.2 development databases that keyed directly by event ID.
			if err := bucket.Delete([]byte(eventID)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Get loads a transaction by ID.
func (d *DB) Get(txID string) (model.Record, error) {
	var record model.Record
	err := d.db.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(transactionsBucket)
		if bucket == nil {
			return errors.New("transactions bucket is missing")
		}
		encoded := bucket.Get([]byte(txID))
		if encoded == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(encoded, &record); err != nil {
			return fmt.Errorf("decoding transaction %s: %w", txID, err)
		}
		normalizeRecordState(&record)
		return nil
	})
	if err != nil {
		return model.Record{}, err
	}
	return record, nil
}

// List returns up to limit records in transaction-ID order.
func (d *DB) List(limit int) ([]model.Record, error) {
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	records := make([]model.Record, 0, min(limit, 128))
	err := d.db.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(transactionsBucket)
		if bucket == nil {
			return errors.New("transactions bucket is missing")
		}
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil && len(records) < limit; key, value = cursor.Next() {
			var record model.Record
			if err := json.Unmarshal(value, &record); err != nil {
				return fmt.Errorf("decoding transaction %s: %w", key, err)
			}
			normalizeRecordState(&record)
			records = append(records, record)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func mergeRecords(existing, incoming model.Record, mergePeers bool) model.Record {
	if existing.FirstSeen.IsZero() || (!incoming.FirstSeen.IsZero() && incoming.FirstSeen.Before(existing.FirstSeen)) {
		existing.FirstSeen = incoming.FirstSeen
	}
	if incoming.BlockNumber != nil {
		existing.BlockNumber = incoming.BlockNumber
	}
	if incoming.ConfirmedSeen != nil && (existing.ConfirmedSeen == nil || incoming.ConfirmedSeen.Before(*existing.ConfirmedSeen)) {
		existing.ConfirmedSeen = incoming.ConfirmedSeen
	}
	if incoming.BlockTime != nil {
		existing.BlockTime = incoming.BlockTime
	}
	if incoming.BlockID != "" {
		existing.BlockID = incoming.BlockID
		existing.ParentBlockID = incoming.ParentBlockID
	}
	if incoming.ChainState != "" {
		if existing.ChainState == "" || existing.ChainState == model.ChainStatePending ||
			(incoming.ChainState != model.ChainStatePending && incoming.ChainState != model.ChainStateLegacyUnverified) {
			existing.ChainState = incoming.ChainState
		}
	}
	if incoming.Revision > existing.Revision {
		existing.Revision = incoming.Revision
	}
	if incoming.OrphanedSeen != nil {
		existing.OrphanedSeen = incoming.OrphanedSeen
	}
	if incoming.FinalizedSeen != nil {
		existing.FinalizedSeen = incoming.FinalizedSeen
	}
	if len(incoming.RawTransaction) > 0 {
		existing.RawTransaction = incoming.RawTransaction
	}
	if incoming.Source != "" && (existing.ConfirmedSeen == nil || incoming.ConfirmedSeen != nil || incoming.BlockNumber != nil) {
		existing.Source = incoming.Source
	}
	for _, contractType := range incoming.ContractTypes {
		if !slices.Contains(existing.ContractTypes, contractType) {
			existing.ContractTypes = append(existing.ContractTypes, contractType)
		}
	}
	for _, match := range incoming.Matches {
		if !containsMatch(existing.Matches, match) {
			existing.Matches = append(existing.Matches, match)
		}
	}
	if mergePeers {
		for _, peer := range incoming.Peers {
			if peer != "" && !slices.Contains(existing.Peers, peer) {
				existing.Peers = append(existing.Peers, peer)
			}
		}
	}
	slices.Sort(existing.Peers)
	return existing
}

func normalizeRecordState(record *model.Record) {
	if record.ChainState != "" {
		return
	}
	if record.BlockNumber == nil {
		record.ChainState = model.ChainStatePending
		return
	}
	if record.BlockID == "" {
		record.ChainState = model.ChainStateLegacyUnverified
		return
	}
	record.ChainState = model.ChainStateIncluded
}

func isTransition(existing, incoming model.Record) bool {
	if existing.ConfirmedSeen == nil && incoming.ConfirmedSeen != nil {
		return true
	}
	if incoming.BlockNumber != nil && (existing.BlockNumber == nil || *existing.BlockNumber != *incoming.BlockNumber) {
		return true
	}
	return false
}

func (d *DB) enqueueEvent(transaction *bolt.Tx, record model.Record) error {
	emittedAt := record.FirstSeen
	if record.ConfirmedSeen != nil {
		emittedAt = *record.ConfirmedSeen
	}
	event := model.NewEvent(record, emittedAt)
	return d.enqueuePreparedEvent(transaction, event)
}

func (d *DB) enqueuePreparedEvent(transaction *bolt.Tx, event model.Event) error {
	if len(d.destinations) == 0 {
		return nil
	}
	event = model.ProjectEvent(event, d.eventPayloadMode)
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encoding outbox event %s: %w", event.ID, err)
	}
	outbox := transaction.Bucket(outboxBucket)
	if outbox == nil {
		return errors.New("outbox bucket is missing")
	}
	key := outboxStorageKey(event)
	if len(d.destinations) == 1 {
		name := []byte(d.destinations[0])
		bucket := outbox.Bucket(name)
		if bucket == nil {
			return fmt.Errorf("outbox destination %q is missing", name)
		}
		if err := bucket.Put(key, encoded); err != nil {
			return fmt.Errorf("queueing event %s for %s: %w", event.ID, name, err)
		}
		return nil
	}
	payloads := transaction.Bucket(outboxPayloadsBucket)
	if payloads == nil {
		return errors.New("shared outbox payload bucket is missing")
	}
	if existing := payloads.Get(key); existing != nil && !bytes.Equal(existing, encoded) {
		return fmt.Errorf("outbox event %s has a conflicting payload", event.ID)
	}
	if err := payloads.Put(key, encoded); err != nil {
		return fmt.Errorf("storing shared outbox event %s: %w", event.ID, err)
	}
	for _, destination := range d.destinations {
		name := []byte(destination)
		bucket := outbox.Bucket(name)
		if bucket == nil {
			return fmt.Errorf("outbox destination %q is missing", name)
		}
		if err := bucket.Put(key, outboxReferenceValue); err != nil {
			return fmt.Errorf("queueing event %s for %s: %w", event.ID, name, err)
		}
	}
	return nil
}

func deleteSharedOutboxReference(outbox, payloads, destination *bolt.Bucket, key []byte) error {
	if err := destination.Delete(key); err != nil {
		return err
	}
	referenced := false
	if err := outbox.ForEach(func(name, value []byte) error {
		if value != nil {
			return nil
		}
		bucket := outbox.Bucket(name)
		if bucket != nil && bytes.Equal(bucket.Get(key), outboxReferenceValue) {
			referenced = true
		}
		return nil
	}); err != nil {
		return err
	}
	if referenced {
		return nil
	}
	return payloads.Delete(key)
}

func outboxStorageKey(event model.Event) []byte {
	if revision, ok := eventRevision(event.ID); ok {
		return []byte(event.Record.TxID + "\x00r" + revision + "\x00" + event.ID)
	}
	rank := "0"
	if event.Record.BlockNumber != nil {
		rank = "1"
	}
	return []byte(event.Record.TxID + "\x00" + rank + "\x00" + event.ID)
}

func outboxStorageKeyForID(eventID string) []byte {
	if revision, ok := eventRevision(eventID); ok {
		txID, _, _ := strings.Cut(eventID, ":")
		return []byte(txID + "\x00r" + revision + "\x00" + eventID)
	}
	rank := "0"
	if strings.Contains(eventID, ":block:") {
		rank = "1"
	}
	txID, _, _ := strings.Cut(eventID, ":")
	return []byte(txID + "\x00" + rank + "\x00" + eventID)
}

func eventRevision(eventID string) (string, bool) {
	const marker = ":r:"
	start := strings.Index(eventID, marker)
	if start < 0 {
		return "", false
	}
	start += len(marker)
	end := strings.IndexByte(eventID[start:], ':')
	if end != 20 {
		return "", false
	}
	return eventID[start : start+end], true
}

func destinationBucket(transaction *bolt.Tx, destination string) (*bolt.Bucket, error) {
	outbox := transaction.Bucket(outboxBucket)
	if outbox == nil {
		return nil, errors.New("outbox bucket is missing")
	}
	bucket := outbox.Bucket([]byte(destination))
	if bucket == nil {
		return nil, fmt.Errorf("outbox destination %q is missing", destination)
	}
	return bucket, nil
}

func containsMatch(matches []filter.Match, candidate filter.Match) bool {
	return slices.Contains(matches, candidate)
}
