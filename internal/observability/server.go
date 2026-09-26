// Package observability serves bounded-cardinality Prometheus metrics and health endpoints.
package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Snapshot is one point-in-time view of daemon state. IDs are intentionally absent.
type Snapshot struct {
	ConfiguredPeers             int
	ConnectedPeers              int
	PeerConnected               map[string]bool
	Sessions                    int64
	Reconnects                  int64
	SeenTransactions            int64
	MatchedTransactions         int64
	Blocks                      int64
	DuplicateBlocks             int64
	UnresolvedBlockObservations int64
	UnresolvedBlocks            int
	Reorgs                      int64
	OrphanedTransactions        int64
	ReincludedTransactions      int64
	ChainAnchored               bool
	HeadNumber                  int64
	HeadTime                    time.Time
	FinalizedNumber             int64
	FinalityRequired            bool
	FinalityLastSuccess         time.Time
	FinalityFetches             int64
	FinalityFailures            int64
	IntegrityFailure            string
	OutboxDepth                 int
	PublisherSuccesses          int64
	PublisherFailures           int64
	WatchRefreshFailures        int64
	PrunedBlocks                int64
	PrunedTransactions          int64
	CatchupActive               bool
	CatchupCurrentHeight        int64
	CatchupTargetHeight         int64
	CatchupBlocks               int64
	CatchupFailovers            int64
	CatchupFailures             int64
	DiskFreeBytes               uint64
}

type Thresholds struct {
	MinimumPeers         int
	BlockStaleAfter      time.Duration
	FinalityMaxStaleness time.Duration
	OutboxWarnDepth      int
	MinimumFreeBytes     uint64
}

type snapshotProvider func() Snapshot

type handler struct {
	provider   snapshotProvider
	thresholds Thresholds
	now        func() time.Time
	logger     *slog.Logger
	started    time.Time
	mu         sync.Mutex
	lastReady  *bool
}

// NewHandler constructs the HTTP surface independently of its listener.
func NewHandler(provider func() Snapshot, thresholds Thresholds, now func() time.Time, logger *slog.Logger) http.Handler {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	h := &handler{provider: provider, thresholds: thresholds, now: now, logger: logger, started: now()}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", h.metrics)
	mux.HandleFunc("/health/live", h.live)
	mux.HandleFunc("/healthz", h.live)
	mux.HandleFunc("/health/ready", h.ready)
	mux.HandleFunc("/readyz", h.ready)
	return mux
}

func (h *handler) live(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *handler) ready(writer http.ResponseWriter, _ *http.Request) {
	snapshot := h.provider()
	now := h.now()
	reasons := make([]string, 0, 6)
	if h.thresholds.MinimumPeers > 0 && snapshot.ConnectedPeers < h.thresholds.MinimumPeers {
		reasons = append(reasons, "no_handshaked_peer")
	}
	if !snapshot.ChainAnchored {
		reasons = append(reasons, "chain_not_anchored")
	}
	if h.thresholds.BlockStaleAfter > 0 && (snapshot.HeadTime.IsZero() || now.Sub(snapshot.HeadTime) > h.thresholds.BlockStaleAfter) {
		reasons = append(reasons, "chain_head_stale")
	}
	if snapshot.UnresolvedBlocks > 0 {
		reasons = append(reasons, "unresolved_chain_gap")
	}
	if snapshot.CatchupActive {
		reasons = append(reasons, "historical_catchup")
	}
	if snapshot.IntegrityFailure != "" {
		reasons = append(reasons, "chain_integrity_failure")
	}
	if snapshot.FinalityRequired && h.thresholds.FinalityMaxStaleness > 0 &&
		(snapshot.FinalityLastSuccess.IsZero() || now.Sub(snapshot.FinalityLastSuccess) > h.thresholds.FinalityMaxStaleness) {
		reasons = append(reasons, "finality_stale")
	}
	if h.thresholds.OutboxWarnDepth > 0 && snapshot.OutboxDepth > h.thresholds.OutboxWarnDepth {
		reasons = append(reasons, "outbox_backlog")
	}
	if h.thresholds.MinimumFreeBytes > 0 && snapshot.DiskFreeBytes < h.thresholds.MinimumFreeBytes {
		reasons = append(reasons, "disk_space_low")
	}
	ready := len(reasons) == 0
	h.logReadinessTransition(ready, reasons)
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(writer, status, map[string]any{"ready": ready, "reasons": reasons})
}

func (h *handler) logReadinessTransition(ready bool, reasons []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastReady != nil && *h.lastReady == ready {
		return
	}
	value := ready
	h.lastReady = &value
	if ready {
		h.logger.Info("readiness recovered")
	} else {
		h.logger.Warn("readiness degraded", "reasons", reasons)
	}
}

func (h *handler) metrics(writer http.ResponseWriter, _ *http.Request) {
	snapshot := h.provider()
	now := h.now()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var output strings.Builder
	metric(&output, "tronwatch_process_uptime_seconds", now.Sub(h.started).Seconds())
	metric(&output, "tronwatch_go_goroutines", runtime.NumGoroutine())
	metric(&output, "tronwatch_go_memory_alloc_bytes", memory.Alloc)
	metric(&output, "tronwatch_p2p_configured_peers", snapshot.ConfiguredPeers)
	metric(&output, "tronwatch_p2p_connected_peers", snapshot.ConnectedPeers)
	for peer, connected := range snapshot.PeerConnected {
		value := 0
		if connected {
			value = 1
		}
		fmt.Fprintf(&output, "tronwatch_p2p_connected{peer=%s} %d\n", strconv.Quote(peer), value)
	}
	metric(&output, "tronwatch_p2p_sessions_total", snapshot.Sessions)
	metric(&output, "tronwatch_p2p_reconnects_total", snapshot.Reconnects)
	metric(&output, "tronwatch_transactions_seen_total", snapshot.SeenTransactions)
	metric(&output, "tronwatch_transactions_matched_total", snapshot.MatchedTransactions)
	metric(&output, "tronwatch_blocks_received_total", snapshot.Blocks)
	metric(&output, "tronwatch_blocks_duplicate_total", snapshot.DuplicateBlocks)
	metric(&output, "tronwatch_blocks_unresolved_total", snapshot.UnresolvedBlockObservations)
	metric(&output, "tronwatch_chain_unresolved_blocks", snapshot.UnresolvedBlocks)
	metric(&output, "tronwatch_reorgs_total", snapshot.Reorgs)
	metric(&output, "tronwatch_reorg_orphaned_transactions_total", snapshot.OrphanedTransactions)
	metric(&output, "tronwatch_reorg_reincluded_transactions_total", snapshot.ReincludedTransactions)
	metric(&output, "tronwatch_chain_head_height", snapshot.HeadNumber)
	metric(&output, "tronwatch_chain_finalized_height", snapshot.FinalizedNumber)
	catchupActive := 0
	if snapshot.CatchupActive {
		catchupActive = 1
	}
	metric(&output, "tronwatch_catchup_active", catchupActive)
	metric(&output, "tronwatch_catchup_current_height", snapshot.CatchupCurrentHeight)
	metric(&output, "tronwatch_catchup_target_height", snapshot.CatchupTargetHeight)
	metric(&output, "tronwatch_catchup_lag_blocks", max(0, snapshot.CatchupTargetHeight-snapshot.CatchupCurrentHeight))
	metric(&output, "tronwatch_catchup_blocks_total", snapshot.CatchupBlocks)
	metric(&output, "tronwatch_catchup_failovers_total", snapshot.CatchupFailovers)
	metric(&output, "tronwatch_catchup_failures_total", snapshot.CatchupFailures)
	headAge := 0.0
	if !snapshot.HeadTime.IsZero() {
		headAge = max(0, now.Sub(snapshot.HeadTime).Seconds())
	}
	metric(&output, "tronwatch_chain_head_age_seconds", headAge)
	metric(&output, "tronwatch_finality_fetches_total", snapshot.FinalityFetches)
	metric(&output, "tronwatch_finality_failures_total", snapshot.FinalityFailures)
	metric(&output, "tronwatch_outbox_depth", snapshot.OutboxDepth)
	metric(&output, "tronwatch_publisher_successes_total", snapshot.PublisherSuccesses)
	metric(&output, "tronwatch_publisher_failures_total", snapshot.PublisherFailures)
	metric(&output, "tronwatch_watch_refresh_failures_total", snapshot.WatchRefreshFailures)
	metric(&output, "tronwatch_storage_pruned_blocks_total", snapshot.PrunedBlocks)
	metric(&output, "tronwatch_storage_pruned_transactions_total", snapshot.PrunedTransactions)
	metric(&output, "tronwatch_disk_free_bytes", snapshot.DiskFreeBytes)
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(output.String()))
}

func metric(output *strings.Builder, name string, value any) {
	fmt.Fprintf(output, "%s %v\n", name, value)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

// Server owns the local observability listener and shuts it down with the app context.
type Server struct {
	listen string
	server *http.Server
}

func NewServer(listen string, handler http.Handler) *Server {
	return &Server{listen: listen, server: &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}}
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.listen)
	if err != nil {
		return fmt.Errorf("listening for observability: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr := s.server.Shutdown(shutdownContext)
		serveErr := <-done
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
