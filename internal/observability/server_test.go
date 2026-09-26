package observability

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadyReportsEveryBlockingReasonAndRecovers(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	snapshot := Snapshot{
		ConnectedPeers: 0, ChainAnchored: false, HeadTime: now.Add(-time.Minute),
		FinalityRequired: true, OutboxDepth: 101, CatchupActive: true,
	}
	handler := NewHandler(func() Snapshot { return snapshot }, Thresholds{
		MinimumPeers: 1, BlockStaleAfter: 30 * time.Second, FinalityMaxStaleness: time.Minute, OutboxWarnDepth: 100,
	}, func() time.Time { return now }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d", response.Code)
	}
	for _, reason := range []string{"no_handshaked_peer", "chain_not_anchored", "chain_head_stale", "historical_catchup", "finality_stale", "outbox_backlog"} {
		if !strings.Contains(response.Body.String(), reason) {
			t.Fatalf("readiness body %q missing %q", response.Body.String(), reason)
		}
	}

	snapshot.ConnectedPeers = 2
	snapshot.ChainAnchored = true
	snapshot.HeadTime = now.Add(-time.Second)
	snapshot.FinalityLastSuccess = now.Add(-time.Second)
	snapshot.OutboxDepth = 0
	snapshot.CatchupActive = false
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("recovered readiness status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestMetricsExposeBoundedOperationalState(t *testing.T) {
	snapshot := Snapshot{
		ConnectedPeers: 2, ConfiguredPeers: 3, PeerConnected: map[string]bool{"peer-a:18888": true},
		SeenTransactions: 10, MatchedTransactions: 2, Blocks: 4, Reorgs: 1,
		HeadNumber: 99, FinalizedNumber: 80, OutboxDepth: 7, PrunedBlocks: 12, PrunedTransactions: 34,
		CatchupActive: true, CatchupCurrentHeight: 90, CatchupTargetHeight: 100,
		CatchupBlocks: 8, CatchupFailovers: 2, CatchupFailures: 1,
	}
	handler := NewHandler(func() Snapshot { return snapshot }, Thresholds{}, time.Now, nil)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}
	body := response.Body.String()
	for _, metric := range []string{"tronwatch_p2p_connected", "tronwatch_transactions_seen_total 10", "tronwatch_reorgs_total 1", "tronwatch_outbox_depth 7", "tronwatch_storage_pruned_blocks_total 12", "tronwatch_storage_pruned_transactions_total 34", "tronwatch_catchup_active 1", "tronwatch_catchup_lag_blocks 10", "tronwatch_catchup_blocks_total 8", "tronwatch_catchup_failovers_total 2", "tronwatch_catchup_failures_total 1"} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics missing %q:\n%s", metric, body)
		}
	}
}

func TestStateRecordsCatchupProgress(t *testing.T) {
	state := NewState(nil)
	state.CatchupState(true, "peer-a", 100, 120)
	state.CatchupBlocks(7)
	state.CatchupFailover()
	state.CatchupFailure()

	snapshot := state.Snapshot()
	if !snapshot.CatchupActive || snapshot.CatchupCurrentHeight != 100 || snapshot.CatchupTargetHeight != 120 {
		t.Fatalf("catch-up state = %+v", snapshot)
	}
	if snapshot.CatchupBlocks != 7 || snapshot.CatchupFailovers != 1 || snapshot.CatchupFailures != 1 {
		t.Fatalf("catch-up counters = %+v", snapshot)
	}
}
