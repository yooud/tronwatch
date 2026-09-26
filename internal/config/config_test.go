package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsAndResolvesSecrets(t *testing.T) {
	t.Setenv("TRONWATCH_SOURCE_TOKEN", "source-secret")
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"watch": {"sources": [
			{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]},
			{"name":"api","type":"http","url":"https://example.test/watch","token_env":"TRONWATCH_SOURCE_TOKEN"}
		]},
		"publishers": [{"name":"stdout","type":"stdout"}]
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.P2P.Peer != "127.0.0.1:18888" || got.P2P.NetworkID != 201910292 {
		t.Fatalf("P2P defaults = %+v", got.P2P)
	}
	if !got.P2P.Catchup.Enabled || got.P2P.Catchup.BatchSize != 100 || got.P2P.Catchup.RequestTimeout.Duration() != 10*time.Second {
		t.Fatalf("P2P catch-up defaults = %+v", got.P2P.Catchup)
	}
	if got.P2P.NodeIDFile != got.Storage.Path+".node-id" {
		t.Fatalf("P2P node ID file = %q, want storage-derived path", got.P2P.NodeIDFile)
	}
	if got.Watch.RefreshInterval.Duration() != time.Second {
		t.Fatalf("refresh interval = %v, want 1s", got.Watch.RefreshInterval.Duration())
	}
	if got.Watch.Sources[1].Token != "source-secret" {
		t.Fatalf("resolved token = %q", got.Watch.Sources[1].Token)
	}
	if got.Storage.PersistAllPeers {
		t.Fatal("storage.persist_all_peers = true, want optimized default false")
	}
}

func TestLoadConfiguresNodeIDFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "config.json")
	nodeIDPath := filepath.Join(directory, "identity", "node-id")
	data := []byte(`{
		"p2p":{"node_id_file":"` + nodeIDPath + `"},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.P2P.NodeIDFile != nodeIDPath {
		t.Fatalf("P2P node ID file = %q, want %q", got.P2P.NodeIDFile, nodeIDPath)
	}
}

func TestLoadConfiguresAndValidatesCatchup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"p2p":{"catchup":{"enabled":true,"batch_size":64,"request_timeout":"7s"}},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !got.P2P.Catchup.Enabled || got.P2P.Catchup.BatchSize != 64 || got.P2P.Catchup.RequestTimeout.Duration() != 7*time.Second {
		t.Fatalf("catch-up configuration = %+v", got.P2P.Catchup)
	}

	if err := os.WriteFile(path, []byte(`{
		"p2p":{"catchup":{"enabled":true,"batch_size":101,"request_timeout":"7s"}},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "catchup batch_size") {
		t.Fatalf("Load() error = %v, want catchup batch size error", err)
	}
}

func TestLoadEnablesFullPeerHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"storage":{"persist_all_peers":true,"sync_freelist":true},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !got.Storage.PersistAllPeers {
		t.Fatal("storage.persist_all_peers = false, want true")
	}
	if !got.Storage.SyncFreelist {
		t.Fatal("storage.sync_freelist = false, want true")
	}
}

func TestLoadConfiguresBoundedFinalizedRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"storage":{"finalized_retention_blocks":256},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Storage.FinalizedRetentionBlocks != 256 {
		t.Fatalf("storage.finalized_retention_blocks = %d, want 256", got.Storage.FinalizedRetentionBlocks)
	}
}

func TestLoadRejectsNegativeFinalizedRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"storage":{"finalized_retention_blocks":-1},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("Load() error = %v, want negative retention error", err)
	}
}

func TestLoadAcceptsMultiplePeers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"p2p":{"peers":["127.0.0.1:18888","10.0.0.2:18888"]},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []string{"127.0.0.1:18888", "10.0.0.2:18888"}
	if !reflect.DeepEqual(got.P2P.PeerList(), want) {
		t.Fatalf("PeerList() = %v, want %v", got.P2P.PeerList(), want)
	}
}

func TestLoadRetainsLegacySinglePeer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"p2p":{"peer":"127.0.0.2:18888"},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := []string{"127.0.0.2:18888"}; !reflect.DeepEqual(got.P2P.PeerList(), want) {
		t.Fatalf("PeerList() = %v, want %v", got.P2P.PeerList(), want)
	}
}

func TestLoadRejectsPeerAndPeersTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"p2p":{"peer":"127.0.0.1:18888","peers":["127.0.0.2:18888"]},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("Load() error = %v, want mutually exclusive fields", err)
	}
}

func TestLoadRejectsDuplicateOrMalformedPeers(t *testing.T) {
	for name, peers := range map[string]string{
		"duplicate": `"127.0.0.1:18888","127.0.0.1:18888"`,
		"malformed": `"127.0.0.1"`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := []byte(`{"p2p":{"peers":[` + peers + `]},"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]}}`)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("Load() error = nil, want invalid peers error")
			}
		})
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want unknown field error")
	}
}

func TestLoadRejectsInsecureHTTPSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"watch":{"sources":[{"name":"api","type":"http","url":"http://example.test/watch"}]},
		"publishers":[{"name":"stdout","type":"stdout"}]
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want insecure URL error")
	}
}

func TestLoadConfiguresSolidityFinalityAndObservability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
		"finality":{"url":"https://api.example.test/walletsolidity/getnowblock","poll_interval":"12s","max_staleness":"2m","required":true},
		"observability":{"listen":"127.0.0.1:9464","block_stale_after":"30s"}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Finality.URL == "" || got.Finality.PollInterval.Duration() != 12*time.Second || !got.Finality.Required {
		t.Fatalf("Finality = %+v", got.Finality)
	}
	if got.Observability.Listen != "127.0.0.1:9464" || got.Observability.BlockStaleAfter.Duration() != 30*time.Second {
		t.Fatalf("Observability = %+v", got.Observability)
	}
}

func TestLoadConfiguresRuntimeAndJSONLRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
		"publishers":[{"name":"archive","type":"jsonl","path":"events.jsonl","max_bytes":1048576,"max_files":3}],
		"runtime":{"max_duration":"24h","minimum_free_bytes":10737418240,"disk_check_interval":"30s"}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Runtime.MaxDuration.Duration() != 24*time.Hour || got.Runtime.MinimumFreeBytes != 10<<30 {
		t.Fatalf("Runtime = %+v", got.Runtime)
	}
	if got.Publishers[0].MaxBytes != 1<<20 || got.Publishers[0].MaxFiles != 3 {
		t.Fatalf("Publisher = %+v", got.Publishers[0])
	}
}

func TestLoadConfiguresPayloadAndPublisherBatching(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"storage":{"event_payload_mode":"compact_lifecycle"},
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
		"publishers":[
			{"name":"fast","type":"stdout","batch_size":40,"flush_interval":"2s","max_delivery_latency":"750ms"},
			{"name":"compatible","type":"stdout"}
		]
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Storage.EventPayloadMode != "compact_lifecycle" {
		t.Fatalf("storage.event_payload_mode = %q, want compact_lifecycle", got.Storage.EventPayloadMode)
	}
	if got.Publishers[0].BatchSize != 40 || got.Publishers[0].DeliveryInterval() != 750*time.Millisecond {
		t.Fatalf("fast publisher batching = %+v, want size 40 and effective 750ms", got.Publishers[0])
	}
	if got.Publishers[1].BatchSize != 100 || got.Publishers[1].FlushInterval.Duration() != 250*time.Millisecond || got.Publishers[1].MaxDeliveryLatency.Duration() != 250*time.Millisecond {
		t.Fatalf("compatible publisher defaults = %+v", got.Publishers[1])
	}
}

func TestLoadRejectsInvalidPayloadAndPublisherBatching(t *testing.T) {
	tests := []struct {
		name      string
		storage   string
		publisher string
		want      string
	}{
		{name: "payload", storage: `"event_payload_mode":"tiny"`, publisher: `{"name":"out","type":"stdout"}`, want: "event_payload_mode"},
		{name: "batch size", publisher: `{"name":"out","type":"stdout","batch_size":-1}`, want: "batch settings"},
		{name: "flush", publisher: `{"name":"out","type":"stdout","flush_interval":"-1s"}`, want: "batch settings"},
		{name: "latency", publisher: `{"name":"out","type":"stdout","max_delivery_latency":"-1s"}`, want: "batch settings"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := []byte(`{
				"storage":{` + test.storage + `},
				"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
				"publishers":[` + test.publisher + `]
			}`)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}
