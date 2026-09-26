package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yooud/tronwatch/internal/config"
	"github.com/yooud/tronwatch/internal/model"
)

func TestConfiguredAppStartsAndStopsWithoutDialOnCanceledContext(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.json")
	data := []byte(`{
		"p2p":{"peers":["127.0.0.1:18888","127.0.0.2:18888"]},
		"storage":{"path":"` + filepath.Join(directory, "transactions.db") + `"},
		"watch":{"sources":[{"name":"inline","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
		"publishers":[{"name":"stdout","type":"stdout"}]
	}`)
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	application, err := New(configuration, &bytes.Buffer{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if application.peers.Size() != 2 {
		t.Fatalf("peer pool size = %d, want 2", application.peers.Size())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := application.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := application.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConfiguredPublisherBatchLimitIsWiredToDispatcher(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.json")
	data := []byte(`{
		"storage":{"path":"` + filepath.Join(directory, "transactions.db") + `"},
		"watch":{"sources":[{"name":"inline","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
		"publishers":[{"name":"archive","type":"jsonl","path":"` + filepath.Join(directory, "events.jsonl") + `","batch_size":1,"flush_interval":"2s","max_delivery_latency":"1s"}]
	}`)
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	application, err := New(configuration, io.Discard, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Close() })
	for _, txID := range []string{"abc", "def"} {
		if err := application.database.Put(model.Record{TxID: txID, FirstSeen: time.Unix(10, 0).UTC(), Source: "mempool"}); err != nil {
			t.Fatal(err)
		}
	}
	processed, err := application.dispatchers[0].DispatchOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("DispatchOnce() = %d, %v; want one configured event", processed, err)
	}
	depth, err := application.database.OutboxDepth()
	if err != nil || depth != 1 {
		t.Fatalf("OutboxDepth() = %d, %v; want one remaining event", depth, err)
	}
}

func TestFinalityPollingSnapshotAndDiskGuard(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"blockID":"b10","block_header":{"raw_data":{"number":10,"timestamp":1700000000000}}}`))
	}))
	defer server.Close()
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.json")
	data := []byte(`{
		"storage":{"path":"` + filepath.Join(directory, "transactions.db") + `"},
		"watch":{"sources":[{"name":"inline","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
		"finality":{"url":"` + server.URL + `","allow_insecure_http":true,"poll_interval":"1s","timeout":"1s","max_staleness":"1m","required":true},
		"observability":{"listen":"127.0.0.1:19464","block_stale_after":"30s","minimum_peers":1,"outbox_warn_depth":100}
	}`)
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	application, err := New(configuration, io.Discard, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Close() })
	blockTime := time.Now().UTC()
	if _, err := application.database.ApplyBlock(model.Block{
		ID: "b10", ParentID: "b09", Number: 10, Time: blockTime, ObservedAt: blockTime,
	}, nil); err != nil {
		t.Fatal(err)
	}
	application.pollFinality(context.Background())
	snapshot := application.observabilitySnapshot()
	if !snapshot.ChainAnchored || snapshot.FinalizedNumber != 10 || snapshot.FinalityLastSuccess.IsZero() || snapshot.DiskFreeBytes == 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	application.config.Runtime.MinimumFreeBytes = math.MaxUint64
	if err := application.guardDisk(context.Background()); err == nil {
		t.Fatal("guardDisk() error = nil, want low-space stop")
	}
}
