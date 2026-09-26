package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yooud/tronwatch/internal/model"
)

var (
	metaTipID           = []byte("tip_id")
	metaTipNumber       = []byte("tip_number")
	metaAnchorNumber    = []byte("anchor_number")
	metaFinalizedID     = []byte("finalized_id")
	metaFinalizedNumber = []byte("finalized_number")
	metaPrunedThrough   = []byte("pruned_through")
)

type storedBlock struct {
	model.Block
	Canonical        bool           `json:"canonical"`
	TransactionIDs   []string       `json:"transaction_ids,omitempty"`
	CandidateRecords []model.Record `json:"candidate_records,omitempty"`
}

func (d *DB) knownBlockNoop(block model.Block, records []model.Record) (bool, bool, error) {
	noop, canonical := false, false
	err := d.db.View(func(tx *bolt.Tx) error {
		blocks, canonicalBlocks, meta, err := chainBuckets(tx)
		if err != nil {
			return err
		}
		stored, exists, err := loadStoredBlock(blocks, block.ID)
		if err != nil {
			return err
		}
		if !exists {
			finalized, hasFinalized := readHeight(meta.Get(metaFinalizedNumber))
			if !hasFinalized || block.Number > finalized {
				return nil
			}
			canonicalID := string(canonicalBlocks.Get(heightKey(block.Number)))
			if canonicalID == "" {
				return nil
			}
			if canonicalID != block.ID {
				return ErrFinalizedFork
			}
			noop, canonical = true, true
			return nil
		}
		if stored.Number != block.Number || stored.ParentID != block.ParentID {
			return fmt.Errorf("block %s identity changed", block.ID)
		}
		canonical = stored.Canonical
		if len(records) == 0 {
			noop = true
			return nil
		}
		if stored.Canonical {
			transactions := tx.Bucket(transactionsBucket)
			for _, incoming := range records {
				existing, exists, err := loadRecord(transactions, incoming.TxID)
				if err != nil || !exists || existing.BlockID != stored.ID {
					return err
				}
				merged := mergeRecords(existing, incoming, false)
				if !recordsEqual(existing, merged) {
					return nil
				}
			}
			noop = true
			return nil
		}
		for _, incoming := range records {
			found := false
			for _, existing := range stored.CandidateRecords {
				if existing.TxID == incoming.TxID {
					found = recordsEqual(existing, mergeRecords(existing, incoming, false))
					break
				}
			}
			if !found {
				return nil
			}
		}
		noop = true
		return nil
	})
	return noop, canonical, err
}

func recordsEqual(left, right model.Record) bool {
	leftEncoded, leftErr := json.Marshal(left)
	rightEncoded, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftEncoded) == string(rightEncoded)
}

// ApplyBlock persists one block and atomically updates canonical records and outboxes.
func (d *DB) ApplyBlock(block model.Block, records []model.Record) (model.ChainUpdate, error) {
	if block.ID == "" || block.Number < 0 {
		return model.ChainUpdate{}, errors.New("block id and non-negative number are required")
	}
	if block.Number > 0 && block.ParentID == "" {
		return model.ChainUpdate{}, errors.New("non-genesis block parent id is empty")
	}
	if block.ObservedAt.IsZero() {
		block.ObservedAt = time.Now().UTC()
	} else {
		block.ObservedAt = block.ObservedAt.UTC()
	}
	block.Time = block.Time.UTC()
	for index := range records {
		if records[index].TxID == "" {
			return model.ChainUpdate{}, errors.New("block transaction id is empty")
		}
	}

	d.putMu.Lock()
	defer d.putMu.Unlock()
	result := model.ChainUpdate{BlockID: block.ID}
	if !d.persistAllPeers {
		noop, canonical, err := d.knownBlockNoop(block, records)
		if err != nil {
			return result, err
		}
		if noop {
			result.Duplicate = true
			result.Canonical = canonical
			return result, nil
		}
	}
	err := d.db.Update(func(tx *bolt.Tx) error {
		blocks, canonical, meta, err := chainBuckets(tx)
		if err != nil {
			return err
		}
		candidates := tx.Bucket(candidatesBucket)
		if candidates == nil {
			return errors.New("chain candidates schema is missing")
		}
		tipID := string(meta.Get(metaTipID))
		tipNumber, anchored := readHeight(meta.Get(metaTipNumber))
		finalizedNumber, finalized := readHeight(meta.Get(metaFinalizedNumber))
		if finalized && block.Number <= finalizedNumber {
			canonicalID := string(canonical.Get(heightKey(block.Number)))
			if canonicalID != "" && canonicalID != block.ID {
				return ErrFinalizedFork
			}
			if canonicalID == block.ID && blocks.Get([]byte(block.ID)) == nil {
				result.Duplicate = true
				result.Canonical = true
				return nil
			}
		}

		current, exists, err := loadStoredBlock(blocks, block.ID)
		if err != nil {
			return err
		}
		if exists {
			result.Duplicate = true
			if current.Number != block.Number || current.ParentID != block.ParentID {
				return fmt.Errorf("block %s identity changed", block.ID)
			}
			if current.Canonical {
				included, _, err := d.applyCanonicalRecords(tx, &current, records, model.TransitionIncluded, block.ObservedAt)
				if err != nil {
					return err
				}
				result.Canonical = true
				result.IncludedTransactions = included
				return saveStoredBlock(blocks, current)
			}
			current.CandidateRecords = mergeCandidateRecords(current.CandidateRecords, records, d.persistAllPeers)
			if err := saveStoredBlock(blocks, current); err != nil {
				return err
			}
		} else {
			current = storedBlock{Block: block, CandidateRecords: slices.Clone(records)}
			if err := saveStoredBlock(blocks, current); err != nil {
				return err
			}
		}
		if !current.Canonical {
			if candidates.Get([]byte(current.ID)) == nil && candidates.Stats().KeyN >= d.maxForkBlocks &&
				!(anchored && current.ParentID == tipID && current.Number == tipNumber+1) {
				return ErrForkLimit
			}
			if err := candidates.Put([]byte(current.ID), heightKey(current.Number)); err != nil {
				return err
			}
		}

		if !anchored {
			if err := d.promoteAnchor(tx, blocks, canonical, candidates, meta, &current, &result); err != nil {
				return err
			}
			return nil
		}
		if finalized && block.Number <= finalizedNumber {
			canonicalID := string(canonical.Get(heightKey(block.Number)))
			if canonicalID != "" && canonicalID != block.ID {
				return ErrFinalizedFork
			}
		}

		candidate, added, ancestor, unresolved, err := bestBranch(blocks, canonical, candidates, tipNumber)
		if err != nil {
			return err
		}
		if candidate == nil {
			result.Unresolved = unresolvedForBlock(blocks, canonical, current)
			return nil
		}
		if ancestor.Number < finalizedNumber {
			return ErrFinalizedFork
		}
		if ancestor.ID == tipID {
			return d.extendCanonicalBranch(tx, blocks, canonical, candidates, meta, added, &result)
		}
		result.Unresolved = unresolved
		return d.switchCanonical(tx, blocks, canonical, candidates, meta, tipID, tipNumber, ancestor, added, &result)
	})
	return result, err
}

func (d *DB) extendCanonicalBranch(
	tx *bolt.Tx,
	blocks, canonical, candidates, meta *bolt.Bucket,
	added []storedBlock,
	result *model.ChainUpdate,
) error {
	if len(added) == 0 {
		return errors.New("canonical extension is empty")
	}
	slices.Reverse(added)
	for index := range added {
		step := model.ChainUpdate{BlockID: added[index].ID}
		if err := d.extendCanonical(tx, blocks, canonical, candidates, meta, &added[index], &step); err != nil {
			return err
		}
		result.Canonical = true
		result.NewTipID = step.NewTipID
		result.IncludedTransactions += step.IncludedTransactions
		result.ReincludedTransactions += step.ReincludedTransactions
	}
	return nil
}

// Finalize advances the immutable checkpoint only when height and block ID both match.
func (d *DB) Finalize(solid model.SolidBlock) (model.FinalizeResult, error) {
	if solid.ID == "" || solid.Number < 0 {
		return model.FinalizeResult{}, errors.New("solid block id and non-negative number are required")
	}
	if solid.ObservedAt.IsZero() {
		solid.ObservedAt = time.Now().UTC()
	} else {
		solid.ObservedAt = solid.ObservedAt.UTC()
	}
	d.putMu.Lock()
	defer d.putMu.Unlock()
	result := model.FinalizeResult{}
	err := d.db.Update(func(tx *bolt.Tx) error {
		blocks, canonical, meta, err := chainBuckets(tx)
		if err != nil {
			return err
		}
		canonicalID := string(canonical.Get(heightKey(solid.Number)))
		if canonicalID == "" || canonicalID != solid.ID {
			return ErrSolidBlockMismatch
		}
		previous, hasPrevious := readHeight(meta.Get(metaFinalizedNumber))
		if hasPrevious && solid.Number <= previous {
			if solid.Number == previous && string(meta.Get(metaFinalizedID)) != solid.ID {
				return ErrSolidBlockMismatch
			}
			if solid.Number == previous {
				prunedBlocks, prunedTransactions, err := d.pruneFinalizedHistory(tx, solid.Number)
				result.PrunedBlocks = prunedBlocks
				result.PrunedTransactions = prunedTransactions
				return err
			}
			return nil
		}
		anchor, ok := readHeight(meta.Get(metaAnchorNumber))
		if !ok {
			return errors.New("canonical anchor is missing")
		}
		start := anchor
		if hasPrevious {
			start = previous + 1
		}
		transactions := tx.Bucket(transactionsBucket)
		for height := start; height <= solid.Number; height++ {
			id := string(canonical.Get(heightKey(height)))
			if id == "" {
				return fmt.Errorf("canonical block %d is missing", height)
			}
			block, exists, err := loadStoredBlock(blocks, id)
			if err != nil || !exists {
				if err == nil {
					err = fmt.Errorf("canonical block %s is missing", id)
				}
				return err
			}
			for _, txID := range block.TransactionIDs {
				record, exists, err := loadRecord(transactions, txID)
				if err != nil {
					return err
				}
				if !exists || record.BlockID != block.ID || record.ChainState == model.ChainStateFinalized {
					continue
				}
				record.ChainState = model.ChainStateFinalized
				record.FinalizedSeen = timePointer(solid.ObservedAt)
				record.Revision++
				if err := saveRecord(transactions, record); err != nil {
					return err
				}
				if err := d.enqueuePreparedEvent(tx, model.NewLifecycleEvent(record, model.TransitionFinalized, solid.ObservedAt)); err != nil {
					return err
				}
				result.FinalizedTransactions++
			}
			result.FinalizedBlocks++
		}
		if err := meta.Put(metaFinalizedID, []byte(solid.ID)); err != nil {
			return err
		}
		if err := meta.Put(metaFinalizedNumber, heightKey(solid.Number)); err != nil {
			return err
		}
		prunedBlocks, prunedTransactions, err := d.pruneFinalizedHistory(tx, solid.Number)
		result.PrunedBlocks = prunedBlocks
		result.PrunedTransactions = prunedTransactions
		return err
	})
	return result, err
}

// pruneFinalizedHistory bounds heavy local payloads while retaining the compact
// canonical height-to-ID index used to reject historical finalized forks.
func (d *DB) pruneFinalizedHistory(tx *bolt.Tx, finalized int64) (int, int, error) {
	if d.finalizedRetentionBlocks <= 0 || finalized < d.finalizedRetentionBlocks {
		return 0, 0, nil
	}
	blocks, canonical, meta, err := chainBuckets(tx)
	if err != nil {
		return 0, 0, err
	}
	transactions := tx.Bucket(transactionsBucket)
	if transactions == nil {
		return 0, 0, errors.New("transactions bucket is missing")
	}
	pruneThrough := finalized - d.finalizedRetentionBlocks
	start, hasPrevious := readHeight(meta.Get(metaPrunedThrough))
	if hasPrevious {
		start++
	} else {
		start, _ = readHeight(meta.Get(metaAnchorNumber))
	}
	if start > pruneThrough {
		return 0, 0, nil
	}

	prunedBlocks, prunedTransactions := 0, 0
	for height := start; height <= pruneThrough; height++ {
		id := string(canonical.Get(heightKey(height)))
		if id == "" {
			return prunedBlocks, prunedTransactions, fmt.Errorf("canonical block %d is missing during retention pruning", height)
		}
		block, exists, err := loadStoredBlock(blocks, id)
		if err != nil {
			return prunedBlocks, prunedTransactions, err
		}
		if !exists {
			return prunedBlocks, prunedTransactions, fmt.Errorf("canonical block %s is missing during retention pruning", id)
		}
		deleted, err := pruneBlockRecords(transactions, block)
		if err != nil {
			return prunedBlocks, prunedTransactions, err
		}
		if err := blocks.Delete([]byte(id)); err != nil {
			return prunedBlocks, prunedTransactions, err
		}
		prunedBlocks++
		prunedTransactions += deleted
	}

	candidates := tx.Bucket(candidatesBucket)
	if candidates == nil {
		return prunedBlocks, prunedTransactions, errors.New("chain candidates schema is missing")
	}
	cursor := candidates.Cursor()
	for id, encodedHeight := cursor.First(); id != nil; id, encodedHeight = cursor.Next() {
		height, ok := readHeight(encodedHeight)
		if !ok {
			return prunedBlocks, prunedTransactions, fmt.Errorf("candidate block %s has invalid height", id)
		}
		if height <= pruneThrough {
			block, exists, err := loadStoredBlock(blocks, string(id))
			if err != nil {
				return prunedBlocks, prunedTransactions, err
			}
			if exists {
				deleted, err := pruneBlockRecords(transactions, block)
				if err != nil {
					return prunedBlocks, prunedTransactions, err
				}
				if err := blocks.Delete(id); err != nil {
					return prunedBlocks, prunedTransactions, err
				}
				prunedBlocks++
				prunedTransactions += deleted
			}
			if err := cursor.Delete(); err != nil {
				return prunedBlocks, prunedTransactions, err
			}
		}
	}
	if err := meta.Put(metaPrunedThrough, heightKey(pruneThrough)); err != nil {
		return prunedBlocks, prunedTransactions, err
	}
	return prunedBlocks, prunedTransactions, nil
}

func pruneBlockRecords(transactions *bolt.Bucket, block storedBlock) (int, error) {
	deleted := 0
	for _, txID := range block.TransactionIDs {
		record, exists, err := loadRecord(transactions, txID)
		if err != nil {
			return deleted, err
		}
		if !exists || record.BlockID != block.ID {
			continue
		}
		if err := transactions.Delete([]byte(txID)); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

// ChainStatus returns a consistent persisted chain snapshot.
func (d *DB) ChainStatus() (model.ChainStatus, error) {
	var result model.ChainStatus
	err := d.db.View(func(tx *bolt.Tx) error {
		blocks, canonical, meta, err := chainBuckets(tx)
		if err != nil {
			return err
		}
		result.TipID = string(meta.Get(metaTipID))
		result.TipNumber, result.Anchored = readHeight(meta.Get(metaTipNumber))
		result.AnchorNumber, _ = readHeight(meta.Get(metaAnchorNumber))
		result.FinalizedID = string(meta.Get(metaFinalizedID))
		result.FinalizedNumber, _ = readHeight(meta.Get(metaFinalizedNumber))
		if result.Anchored {
			block, exists, err := loadStoredBlock(blocks, result.TipID)
			if err != nil {
				return err
			}
			if !exists || string(canonical.Get(heightKey(result.TipNumber))) != result.TipID {
				result.IntegrityFailure = "canonical tip metadata is inconsistent"
			} else {
				result.TipTime = block.Time
			}
		}
		candidates := tx.Bucket(candidatesBucket)
		if candidates == nil {
			return errors.New("chain candidates schema is missing")
		}
		return candidates.ForEach(func(id, _ []byte) error {
			block, exists, err := loadStoredBlock(blocks, string(id))
			if err != nil {
				return err
			}
			if exists && unresolvedForBlock(blocks, canonical, block) {
				result.UnresolvedBlocks++
			}
			return nil
		})
	})
	return result, err
}

// ChainLocator returns a sparse, ascending view of the retained canonical chain.
// It starts at the finalized checkpoint when available and always includes the tip.
func (d *DB) ChainLocator(maxIDs int) ([]model.BlockRef, error) {
	if maxIDs < 2 {
		return nil, errors.New("chain locator limit must be at least two")
	}
	var result []model.BlockRef
	err := d.db.View(func(tx *bolt.Tx) error {
		_, canonical, meta, err := chainBuckets(tx)
		if err != nil {
			return err
		}
		tip, anchored := readHeight(meta.Get(metaTipNumber))
		if !anchored {
			return nil
		}
		low, ok := readHeight(meta.Get(metaFinalizedNumber))
		if !ok || len(meta.Get(metaFinalizedID)) == 0 {
			low, ok = readHeight(meta.Get(metaAnchorNumber))
			if !ok {
				return errors.New("canonical anchor is missing")
			}
		}
		if low > tip {
			return errors.New("canonical locator floor is above the tip")
		}

		heights := make([]int64, 0, maxIDs)
		for height := low; ; {
			heights = append(heights, height)
			if height == tip {
				break
			}
			height += (tip - height + 2) / 2
		}
		if len(heights) > maxIDs {
			heights = append([]int64{heights[0]}, heights[len(heights)-maxIDs+1:]...)
		}
		result = make([]model.BlockRef, 0, len(heights))
		for _, height := range heights {
			id := string(canonical.Get(heightKey(height)))
			if id == "" {
				return fmt.Errorf("canonical block %d is missing", height)
			}
			result = append(result, model.BlockRef{ID: id, Number: height})
		}
		return nil
	})
	return result, err
}

func (d *DB) promoteAnchor(tx *bolt.Tx, blocks, canonical, candidates, meta *bolt.Bucket, block *storedBlock, result *model.ChainUpdate) error {
	block.Canonical = true
	included, _, err := d.applyCanonicalRecords(tx, block, block.CandidateRecords, model.TransitionIncluded, block.ObservedAt)
	if err != nil {
		return err
	}
	block.CandidateRecords = nil
	if err := saveStoredBlock(blocks, *block); err != nil {
		return err
	}
	if err := canonical.Put(heightKey(block.Number), []byte(block.ID)); err != nil {
		return err
	}
	if err := candidates.Delete([]byte(block.ID)); err != nil {
		return err
	}
	if err := setTip(meta, block.ID, block.Number); err != nil {
		return err
	}
	if err := meta.Put(metaAnchorNumber, heightKey(block.Number)); err != nil {
		return err
	}
	result.Canonical = true
	result.NewTipID = block.ID
	result.IncludedTransactions = included
	return nil
}

func (d *DB) extendCanonical(tx *bolt.Tx, blocks, canonical, candidates, meta *bolt.Bucket, block *storedBlock, result *model.ChainUpdate) error {
	block.Canonical = true
	included, reincluded, err := d.applyCanonicalRecords(tx, block, block.CandidateRecords, model.TransitionIncluded, block.ObservedAt)
	if err != nil {
		return err
	}
	block.CandidateRecords = nil
	if err := saveStoredBlock(blocks, *block); err != nil {
		return err
	}
	if err := canonical.Put(heightKey(block.Number), []byte(block.ID)); err != nil {
		return err
	}
	if err := candidates.Delete([]byte(block.ID)); err != nil {
		return err
	}
	if err := setTip(meta, block.ID, block.Number); err != nil {
		return err
	}
	result.Canonical = true
	result.NewTipID = block.ID
	result.IncludedTransactions = included
	result.ReincludedTransactions = reincluded
	return nil
}

func (d *DB) switchCanonical(tx *bolt.Tx, blocks, canonical, candidates, meta *bolt.Bucket, tipID string, tipNumber int64, ancestor storedBlock, added []storedBlock, result *model.ChainUpdate) error {
	removed := make([]storedBlock, 0, tipNumber-ancestor.Number)
	currentID := tipID
	for currentID != ancestor.ID {
		block, exists, err := loadStoredBlock(blocks, currentID)
		if err != nil || !exists {
			if err == nil {
				err = fmt.Errorf("canonical block %s is missing", currentID)
			}
			return err
		}
		removed = append(removed, block)
		currentID = block.ParentID
	}
	addedTx := make(map[string]struct{})
	for _, block := range added {
		for _, record := range block.CandidateRecords {
			addedTx[record.TxID] = struct{}{}
		}
	}
	transactions := tx.Bucket(transactionsBucket)
	for index := range removed {
		block := &removed[index]
		block.Canonical = false
		block.CandidateRecords = nil
		for _, txID := range block.TransactionIDs {
			record, exists, err := loadRecord(transactions, txID)
			if err != nil {
				return err
			}
			if exists && record.BlockID == block.ID {
				block.CandidateRecords = append(block.CandidateRecords, record)
			}
			if _, movesToNewBranch := addedTx[txID]; movesToNewBranch {
				continue
			}
			if !exists || record.BlockID != block.ID || record.ChainState == model.ChainStateFinalized {
				continue
			}
			record.ChainState = model.ChainStateOrphaned
			record.OrphanedSeen = timePointer(added[len(added)-1].ObservedAt)
			record.FinalizedSeen = nil
			record.Revision++
			if err := saveRecord(transactions, record); err != nil {
				return err
			}
			if err := d.enqueuePreparedEvent(tx, model.NewLifecycleEvent(record, model.TransitionOrphaned, added[len(added)-1].ObservedAt)); err != nil {
				return err
			}
			result.OrphanedTransactions++
		}
		if err := canonical.Delete(heightKey(block.Number)); err != nil {
			return err
		}
		if err := saveStoredBlock(blocks, *block); err != nil {
			return err
		}
		if err := candidates.Put([]byte(block.ID), heightKey(block.Number)); err != nil {
			return err
		}
	}
	slices.Reverse(added)
	for index := range added {
		block := &added[index]
		block.Canonical = true
		for _, candidate := range block.CandidateRecords {
			transition := model.TransitionIncluded
			if _, reincluded := addedTx[candidate.TxID]; reincluded {
				for _, old := range removed {
					if slices.Contains(old.TransactionIDs, candidate.TxID) {
						transition = model.TransitionReincluded
						break
					}
				}
			}
			included, reincluded, err := d.applyCanonicalRecords(tx, block, []model.Record{candidate}, transition, block.ObservedAt)
			if err != nil {
				return err
			}
			result.IncludedTransactions += included
			result.ReincludedTransactions += reincluded
		}
		block.CandidateRecords = nil
		if err := saveStoredBlock(blocks, *block); err != nil {
			return err
		}
		if err := canonical.Put(heightKey(block.Number), []byte(block.ID)); err != nil {
			return err
		}
		if err := candidates.Delete([]byte(block.ID)); err != nil {
			return err
		}
	}
	newTip := added[len(added)-1]
	if err := setTip(meta, newTip.ID, newTip.Number); err != nil {
		return err
	}
	result.Canonical = true
	result.Reorg = true
	result.ReorgDepth = len(removed)
	result.OrphanedBlocks = len(removed)
	result.OldTipID = tipID
	result.NewTipID = newTip.ID
	return nil
}

func (d *DB) applyCanonicalRecords(tx *bolt.Tx, block *storedBlock, records []model.Record, requested model.LifecycleTransition, at time.Time) (int, int, error) {
	transactions := tx.Bucket(transactionsBucket)
	included, reincluded := 0, 0
	for _, incoming := range records {
		existing, exists, err := loadRecord(transactions, incoming.TxID)
		if err != nil {
			return included, reincluded, err
		}
		transition := requested
		if exists && existing.BlockID == block.ID && (existing.ChainState == model.ChainStateIncluded || existing.ChainState == model.ChainStateFinalized) {
			merged := mergeRecords(existing, incoming, d.persistAllPeers)
			if err := saveRecord(transactions, merged); err != nil {
				return included, reincluded, err
			}
			if !slices.Contains(block.TransactionIDs, incoming.TxID) {
				block.TransactionIDs = append(block.TransactionIDs, incoming.TxID)
			}
			continue
		}
		if requested == model.TransitionIncluded && exists && existing.ChainState == model.ChainStateOrphaned {
			transition = model.TransitionReincluded
		}
		record := incoming
		if exists {
			record = mergeRecords(existing, incoming, true)
		}
		number, blockTime, confirmed := block.Number, block.Time, at.UTC()
		record.BlockNumber = &number
		record.BlockTime = &blockTime
		record.BlockID = block.ID
		record.ParentBlockID = block.ParentID
		record.ChainState = model.ChainStateIncluded
		record.Source = "block"
		record.ConfirmedSeen = &confirmed
		record.OrphanedSeen = nil
		record.FinalizedSeen = nil
		record.Revision++
		if err := saveRecord(transactions, record); err != nil {
			return included, reincluded, err
		}
		if !slices.Contains(block.TransactionIDs, record.TxID) {
			block.TransactionIDs = append(block.TransactionIDs, record.TxID)
		}
		if err := d.enqueuePreparedEvent(tx, model.NewLifecycleEvent(record, transition, at)); err != nil {
			return included, reincluded, err
		}
		if transition == model.TransitionReincluded {
			reincluded++
		} else {
			included++
		}
	}
	slices.Sort(block.TransactionIDs)
	return included, reincluded, nil
}

func bestBranch(blocks, canonical, candidates *bolt.Bucket, tipNumber int64) (*storedBlock, []storedBlock, storedBlock, bool, error) {
	var best *storedBlock
	var bestAdded []storedBlock
	var bestAncestor storedBlock
	unresolved := false
	err := candidates.ForEach(func(id, _ []byte) error {
		candidate, exists, err := loadStoredBlock(blocks, string(id))
		if err != nil {
			return err
		}
		if !exists || candidate.Canonical || candidate.Number <= tipNumber {
			return nil
		}
		added, ancestor, connected, err := branchToCanonical(blocks, canonical, candidate)
		if err != nil {
			return err
		}
		if !connected {
			unresolved = true
			return nil
		}
		if best == nil || candidate.Number > best.Number || (candidate.Number == best.Number && candidate.ID < best.ID) {
			copy := candidate
			best = &copy
			bestAdded = added
			bestAncestor = ancestor
		}
		return nil
	})
	return best, bestAdded, bestAncestor, unresolved, err
}

func branchToCanonical(blocks, canonical *bolt.Bucket, tip storedBlock) ([]storedBlock, storedBlock, bool, error) {
	added := []storedBlock{tip}
	current := tip
	for {
		if current.Number == 0 {
			return nil, storedBlock{}, false, nil
		}
		canonicalID := string(canonical.Get(heightKey(current.Number - 1)))
		if canonicalID == current.ParentID {
			ancestor, exists, err := loadStoredBlock(blocks, canonicalID)
			return added, ancestor, exists, err
		}
		parent, exists, err := loadStoredBlock(blocks, current.ParentID)
		if err != nil || !exists {
			return nil, storedBlock{}, false, err
		}
		if parent.Number != current.Number-1 {
			return nil, storedBlock{}, false, fmt.Errorf("block %s parent height is not contiguous", current.ID)
		}
		added = append(added, parent)
		current = parent
	}
}

func unresolvedForBlock(blocks, canonical *bolt.Bucket, block storedBlock) bool {
	if block.Canonical {
		return false
	}
	_, _, connected, err := branchToCanonical(blocks, canonical, block)
	return err != nil || !connected
}

func mergeCandidateRecords(existing, incoming []model.Record, mergePeers bool) []model.Record {
	result := slices.Clone(existing)
	for _, record := range incoming {
		found := false
		for index := range result {
			if result[index].TxID == record.TxID {
				result[index] = mergeRecords(result[index], record, mergePeers)
				found = true
				break
			}
		}
		if !found {
			result = append(result, record)
		}
	}
	return result
}

func chainBuckets(tx *bolt.Tx) (*bolt.Bucket, *bolt.Bucket, *bolt.Bucket, error) {
	blocks, canonical, meta := tx.Bucket(blocksBucket), tx.Bucket(canonicalBucket), tx.Bucket(chainMetaBucket)
	if blocks == nil || canonical == nil || meta == nil {
		return nil, nil, nil, errors.New("chain schema is missing")
	}
	return blocks, canonical, meta, nil
}

func loadStoredBlock(bucket *bolt.Bucket, id string) (storedBlock, bool, error) {
	encoded := bucket.Get([]byte(id))
	if encoded == nil {
		return storedBlock{}, false, nil
	}
	var block storedBlock
	if err := json.Unmarshal(encoded, &block); err != nil {
		return storedBlock{}, false, fmt.Errorf("decoding block %s: %w", id, err)
	}
	return block, true, nil
}

func saveStoredBlock(bucket *bolt.Bucket, block storedBlock) error {
	encoded, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("encoding block %s: %w", block.ID, err)
	}
	return bucket.Put([]byte(block.ID), encoded)
}

func loadRecord(bucket *bolt.Bucket, txID string) (model.Record, bool, error) {
	encoded := bucket.Get([]byte(txID))
	if encoded == nil {
		return model.Record{}, false, nil
	}
	var record model.Record
	if err := json.Unmarshal(encoded, &record); err != nil {
		return model.Record{}, false, fmt.Errorf("decoding transaction %s: %w", txID, err)
	}
	normalizeRecordState(&record)
	return record, true, nil
}

func saveRecord(bucket *bolt.Bucket, record model.Record) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding transaction %s: %w", record.TxID, err)
	}
	return bucket.Put([]byte(record.TxID), encoded)
}

func setTip(meta *bolt.Bucket, id string, number int64) error {
	if err := meta.Put(metaTipID, []byte(id)); err != nil {
		return err
	}
	return meta.Put(metaTipNumber, heightKey(number))
}

func heightKey(number int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(number))
	return key
}

func readHeight(value []byte) (int64, bool) {
	if len(value) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(value)), true
}

func timePointer(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
