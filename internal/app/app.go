// Package app wires the daemon's independent ingestion, watch, storage, and delivery components.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"tronwatch/internal/config"
	"tronwatch/internal/finality"
	"tronwatch/internal/ingest"
	"tronwatch/internal/model"
	"tronwatch/internal/observability"
	"tronwatch/internal/p2p"
	"tronwatch/internal/publish"
	"tronwatch/internal/store"
	"tronwatch/internal/watch"
)

// App is a configured tronwatch daemon.
type App struct {
	config      config.Config
	logger      *slog.Logger
	watches     *watch.Manager
	database    *store.DB
	ingestor    *ingest.Ingestor
	peers       *p2p.Pool
	publishers  []publish.Publisher
	dispatchers []*publish.Dispatcher
	finality    *finality.SoliditySource
	operations  *observability.State
	monitor     *observability.Server
}

// New validates external adapters and constructs the complete daemon graph.
func New(configuration config.Config, stdout io.Writer, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	destinationNames := make([]string, 0, len(configuration.Publishers))
	for _, publisherConfig := range configuration.Publishers {
		destinationNames = append(destinationNames, publisherConfig.Name)
	}
	database, err := store.OpenWithOptions(configuration.Storage.Path, store.Options{
		Destinations: destinationNames, PersistAllPeers: configuration.Storage.PersistAllPeers,
		SyncFreelist:             configuration.Storage.SyncFreelist,
		FinalizedRetentionBlocks: configuration.Storage.FinalizedRetentionBlocks,
		EventPayloadMode:         configuration.Storage.EventPayloadMode,
	})
	if err != nil {
		return nil, err
	}
	fail := func(cause error, manager *watch.Manager, publishers []publish.Publisher) (*App, error) {
		var cleanup error
		for _, publisher := range publishers {
			cleanup = errors.Join(cleanup, publisher.Close())
		}
		if manager != nil {
			cleanup = errors.Join(cleanup, manager.Close())
		}
		cleanup = errors.Join(cleanup, database.Close())
		return nil, errors.Join(cause, cleanup)
	}

	sources, err := buildSources(configuration.Watch.Sources)
	if err != nil {
		closeSources(sources)
		return fail(err, nil, nil)
	}
	manager, err := watch.Open(context.Background(), sources)
	if err != nil {
		return fail(err, nil, nil)
	}
	for _, failure := range manager.InitialFailures() {
		logger.Warn("optional watch source unavailable at startup", "source", failure.Source, "error", failure.Err)
	}
	publishers, err := buildPublishers(configuration.Publishers, stdout)
	if err != nil {
		return fail(err, manager, publishers)
	}
	var finalitySource *finality.SoliditySource
	if configuration.Finality.Enabled() {
		finalitySource, err = finality.NewSoliditySource(
			configuration.Finality.URL, configuration.Finality.Token,
			configuration.Finality.Timeout.Duration(), configuration.Finality.AllowInsecureHTTP,
		)
		if err != nil {
			return fail(err, manager, publishers)
		}
	}

	repository := &loggedRepository{next: database, logger: logger}
	ingestor := ingest.New(manager, repository, time.Now)
	operations := observability.NewState(configuration.P2P.PeerList())
	peerPool, err := p2p.NewPool(configuration.P2P.PeerList(), p2p.Config{
		NetworkID:   configuration.P2P.NetworkID,
		AdvertiseIP: configuration.P2P.AdvertiseIP, Logger: logger, Observer: operations,
	}, ingestor)
	if err != nil {
		return fail(err, manager, publishers)
	}
	application := &App{
		config: configuration, logger: logger, watches: manager, database: database,
		ingestor: ingestor, peers: peerPool, publishers: publishers,
		finality: finalitySource, operations: operations,
	}
	handler := observability.NewHandler(application.observabilitySnapshot, observability.Thresholds{
		MinimumPeers:         configuration.Observability.MinimumPeers,
		BlockStaleAfter:      configuration.Observability.BlockStaleAfter.Duration(),
		FinalityMaxStaleness: configuration.Finality.MaxStaleness.Duration(),
		OutboxWarnDepth:      configuration.Observability.OutboxWarnDepth,
		MinimumFreeBytes:     configuration.Runtime.MinimumFreeBytes,
	}, time.Now, logger)
	application.monitor = observability.NewServer(configuration.Observability.Listen, handler)
	publisherConfigurations := make(map[string]config.PublisherConfig, len(configuration.Publishers))
	for _, publisherConfig := range configuration.Publishers {
		publisherConfigurations[publisherConfig.Name] = publisherConfig
	}
	for _, publisher := range publishers {
		publisherConfig := publisherConfigurations[publisher.Name()]
		dispatcher, err := publish.NewDispatcher(
			database, publisher, publisherConfig.DeliveryInterval(), publisherConfig.BatchSize, logger,
		)
		if err != nil {
			return fail(err, manager, publishers)
		}
		dispatcher.SetObserver(operations)
		application.dispatchers = append(application.dispatchers, dispatcher)
	}
	return application, nil
}

// Run starts P2P intake, watch refreshes, stats, and independent outbox dispatchers.
func (a *App) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	runContext := ctx
	if maximum := a.config.Runtime.MaxDuration.Duration(); maximum > 0 {
		var cancel context.CancelFunc
		runContext, cancel = context.WithTimeout(ctx, maximum)
		defer cancel()
		a.logger.Info("runtime deadline enabled", "duration", maximum)
	}
	addresses, contracts := a.watches.Current().Sizes()
	a.logger.Info(
		"watch sources loaded",
		"sources", len(a.config.Watch.Sources),
		"addresses", addresses,
		"contracts", contracts,
		"peers", a.peers.Size(),
	)
	group, groupContext := errgroup.WithContext(runContext)
	group.Go(func() error { return a.peers.Run(groupContext) })
	group.Go(func() error { return a.refreshWatches(groupContext) })
	group.Go(func() error { return a.reportStats(groupContext) })
	group.Go(func() error { return a.monitor.Run(groupContext) })
	if a.finality != nil {
		group.Go(func() error { return a.trackFinality(groupContext) })
	}
	if a.config.Runtime.MinimumFreeBytes > 0 {
		group.Go(func() error { return a.guardDisk(groupContext) })
	}
	for _, dispatcher := range a.dispatchers {
		dispatcher := dispatcher
		group.Go(func() error { return dispatcher.Run(groupContext) })
	}
	return group.Wait()
}

func (a *App) guardDisk(ctx context.Context) error {
	check := func() error {
		free, err := diskFreeBytes(filepath.Dir(a.config.Storage.Path))
		if err != nil {
			return err
		}
		if free < a.config.Runtime.MinimumFreeBytes {
			return fmt.Errorf("disk guard stopped ingestion: free bytes %d below minimum %d", free, a.config.Runtime.MinimumFreeBytes)
		}
		return nil
	}
	if err := check(); err != nil {
		return err
	}
	ticker := time.NewTicker(a.config.Runtime.DiskCheckInterval.Duration())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := check(); err != nil {
				return err
			}
		}
	}
}

func diskFreeBytes(path string) (uint64, error) {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		return 0, fmt.Errorf("checking free disk space for %s: %w", path, err)
	}
	return stats.Bavail * uint64(stats.Bsize), nil
}

// Close flushes publishers and closes network sources and BoltDB.
func (a *App) Close() error {
	var result error
	for _, publisher := range a.publishers {
		result = errors.Join(result, publisher.Close())
	}
	result = errors.Join(result, a.watches.Close(), a.database.Close())
	return result
}

func (a *App) refreshWatches(ctx context.Context) error {
	ticker := time.NewTicker(a.config.Watch.RefreshInterval.Duration())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			changed, failures := a.watches.Refresh(ctx)
			for _, failure := range failures {
				a.operations.WatchFailure()
				a.logger.Warn("watch source update rejected; keeping last good snapshot", "source", failure.Source, "error", failure.Err)
			}
			if changed {
				addresses, contracts := a.watches.Current().Sizes()
				a.logger.Info("watch union reloaded", "addresses", addresses, "contracts", contracts)
			}
		}
	}
}

func (a *App) reportStats(ctx context.Context) error {
	ticker := time.NewTicker(a.config.Logging.StatsInterval.Duration())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			stats := a.ingestor.Stats()
			chain, _ := a.database.ChainStatus()
			outbox, _ := a.database.OutboxDepth()
			peerState := a.operations.Snapshot()
			a.logger.Info("operational totals",
				"seen", stats.Seen, "matched_observations", stats.Matched, "blocks", stats.Blocks,
				"reorgs", stats.Reorgs, "connected_peers", peerState.ConnectedPeers,
				"head", chain.TipNumber, "finalized", chain.FinalizedNumber,
				"unresolved_blocks", chain.UnresolvedBlocks, "outbox_depth", outbox,
			)
		}
	}
}

func (a *App) trackFinality(ctx context.Context) error {
	ticker := time.NewTicker(a.config.Finality.PollInterval.Duration())
	defer ticker.Stop()
	for {
		a.pollFinality(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (a *App) pollFinality(ctx context.Context) {
	a.operations.FinalityFetch()
	checkpoint, err := a.finality.Fetch(ctx)
	if err != nil {
		if ctx.Err() == nil {
			a.operations.FinalityFailure()
			a.logger.Warn("solid block fetch failed", "error", err)
		}
		return
	}
	result, err := a.database.Finalize(checkpoint)
	if err != nil {
		chain, statusErr := a.database.ChainStatus()
		if statusErr != nil || !errors.Is(err, store.ErrSolidBlockMismatch) ||
			(chain.Anchored && checkpoint.Number >= chain.AnchorNumber && checkpoint.Number <= chain.TipNumber) {
			a.operations.FinalityFailure()
			a.logger.Error("solid block rejected", "height", checkpoint.Number, "error", errors.Join(err, statusErr))
		} else {
			a.logger.Debug("solid block is outside local canonical window", "height", checkpoint.Number, "head", chain.TipNumber)
		}
		return
	}
	a.operations.FinalitySuccess(checkpoint.Number, checkpoint.ObservedAt)
	a.operations.StoragePruned(result.PrunedBlocks, result.PrunedTransactions)
	if result.FinalizedBlocks > 0 || result.PrunedBlocks > 0 {
		a.logger.Info("solid checkpoint advanced", "height", checkpoint.Number,
			"finalized_blocks", result.FinalizedBlocks, "finalized_transactions", result.FinalizedTransactions,
			"pruned_blocks", result.PrunedBlocks, "pruned_transactions", result.PrunedTransactions)
	}
}

func (a *App) observabilitySnapshot() observability.Snapshot {
	snapshot := a.operations.Snapshot()
	stats := a.ingestor.Stats()
	snapshot.SeenTransactions = stats.Seen
	snapshot.MatchedTransactions = stats.Matched
	snapshot.Blocks = stats.Blocks
	snapshot.DuplicateBlocks = stats.DuplicateBlocks
	snapshot.UnresolvedBlockObservations = stats.UnresolvedBlocks
	snapshot.Reorgs = stats.Reorgs
	snapshot.OrphanedTransactions = stats.OrphanedTransactions
	snapshot.ReincludedTransactions = stats.ReincludedTransactions
	snapshot.FinalityRequired = a.config.Finality.Required
	chain, err := a.database.ChainStatus()
	if err != nil {
		snapshot.IntegrityFailure = err.Error()
	} else {
		snapshot.ChainAnchored = chain.Anchored
		snapshot.HeadNumber = chain.TipNumber
		snapshot.HeadTime = chain.TipTime
		snapshot.FinalizedNumber = chain.FinalizedNumber
		snapshot.UnresolvedBlocks = chain.UnresolvedBlocks
		snapshot.IntegrityFailure = chain.IntegrityFailure
	}
	outbox, err := a.database.OutboxDepth()
	if err != nil {
		if snapshot.IntegrityFailure == "" {
			snapshot.IntegrityFailure = err.Error()
		} else {
			snapshot.IntegrityFailure += "; " + err.Error()
		}
	} else {
		snapshot.OutboxDepth = outbox
	}
	free, err := diskFreeBytes(filepath.Dir(a.config.Storage.Path))
	if err != nil {
		if snapshot.IntegrityFailure == "" {
			snapshot.IntegrityFailure = err.Error()
		}
	} else {
		snapshot.DiskFreeBytes = free
	}
	return snapshot
}

func buildSources(configurations []config.SourceConfig) ([]watch.Source, error) {
	sources := make([]watch.Source, 0, len(configurations))
	for _, sourceConfig := range configurations {
		var source watch.Source
		var err error
		switch sourceConfig.Type {
		case "inline":
			source = watch.NewInlineSource(sourceConfig.Name, watch.Snapshot{
				Addresses: sourceConfig.Addresses, Contracts: sourceConfig.Contracts,
			}, sourceConfig.IsRequired())
		case "file":
			source = watch.NewFileSource(sourceConfig.Name, sourceConfig.Path, sourceConfig.IsRequired())
		case "http":
			source, err = watch.NewHTTPSource(
				sourceConfig.Name, sourceConfig.URL, sourceConfig.Token, sourceConfig.IsRequired(),
				sourceConfig.Timeout.Duration(), sourceConfig.AllowInsecureHTTP,
			)
		case "redis_sets":
			source, err = watch.NewRedisSetsSource(
				sourceConfig.Name, sourceConfig.URL, sourceConfig.AddressesKey, sourceConfig.ContractsKey,
				sourceConfig.IsRequired(), sourceConfig.Timeout.Duration(),
			)
		default:
			err = fmt.Errorf("unsupported watch source type %q", sourceConfig.Type)
		}
		if err != nil {
			return sources, fmt.Errorf("constructing watch source %q: %w", sourceConfig.Name, err)
		}
		sources = append(sources, source)
	}
	return sources, nil
}

func buildPublishers(configurations []config.PublisherConfig, stdout io.Writer) ([]publish.Publisher, error) {
	publishers := make([]publish.Publisher, 0, len(configurations))
	for _, publisherConfig := range configurations {
		var publisher publish.Publisher
		var err error
		switch publisherConfig.Type {
		case "stdout":
			publisher = publish.NewStdout(publisherConfig.Name, stdout)
		case "jsonl":
			publisher, err = publish.NewRotatingJSONL(
				publisherConfig.Name, publisherConfig.Path, publisherConfig.MaxBytes, publisherConfig.MaxFiles,
			)
		case "webhook":
			publisher, err = publish.NewWebhook(
				publisherConfig.Name, publisherConfig.URL, publisherConfig.Token,
				publisherConfig.Timeout.Duration(), publisherConfig.Attempts,
				publisherConfig.RetryBackoff.Duration(), publisherConfig.AllowInsecureHTTP,
			)
		case "redis_stream":
			publisher, err = publish.NewRedisStream(
				publisherConfig.Name, publisherConfig.URL, publisherConfig.Stream, publisherConfig.Timeout.Duration(),
			)
		default:
			err = fmt.Errorf("unsupported publisher type %q", publisherConfig.Type)
		}
		if err != nil {
			return publishers, fmt.Errorf("constructing publisher %q: %w", publisherConfig.Name, err)
		}
		publishers = append(publishers, publisher)
	}
	return publishers, nil
}

func closeSources(sources []watch.Source) {
	for _, source := range sources {
		if closer, ok := source.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
}

type loggedRepository struct {
	next   *store.DB
	logger *slog.Logger
}

func (r *loggedRepository) Exists(txID string) (bool, error) { return r.next.Exists(txID) }
func (r *loggedRepository) Put(record model.Record) error {
	changed, err := r.next.PutIfChanged(record)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	r.logger.Debug("matched transaction stored", "tx_id", record.TxID, "source", record.Source, "block", record.BlockNumber, "matches", len(record.Matches))
	return nil
}

func (r *loggedRepository) ApplyBlock(block model.Block, records []model.Record) (model.ChainUpdate, error) {
	update, err := r.next.ApplyBlock(block, records)
	if err != nil {
		r.logger.Error("block rejected", "block_id", block.ID, "parent_id", block.ParentID, "height", block.Number, "peer", block.Peer, "error", err)
		return update, err
	}
	if update.Reorg {
		r.logger.Warn("canonical chain reorganized",
			"old_tip", update.OldTipID, "new_tip", update.NewTipID,
			"depth", update.ReorgDepth, "orphaned_blocks", update.OrphanedBlocks,
			"orphaned_transactions", update.OrphanedTransactions,
			"reincluded_transactions", update.ReincludedTransactions,
		)
	} else if update.Unresolved {
		r.logger.Warn("block has unresolved parent", "block_id", block.ID, "parent_id", block.ParentID, "height", block.Number, "peer", block.Peer)
	} else if !update.Duplicate {
		r.logger.Debug("canonical block applied", "block_id", block.ID, "height", block.Number, "matched_transactions", len(records), "peer", block.Peer)
	}
	return update, nil
}
