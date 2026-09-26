package model

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"tronwatch/internal/filter"
)

func TestProjectEventPayload(t *testing.T) {
	record := Record{
		TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "block",
		ContractTypes:  []string{"TransferContract"},
		Matches:        []filter.Match{{Address: "411111111111111111111111111111111111111111", Role: filter.RoleOwner}},
		RawTransaction: []byte{0x0a, 0x00}, Peers: []string{"peer-a:18888"},
	}

	tests := []struct {
		name      string
		event     Event
		mode      EventPayloadMode
		wantRaw   []byte
		wantPeers []string
	}{
		{
			name: "full finalized", event: NewLifecycleEvent(record, TransitionFinalized, time.Unix(20, 0)),
			mode: EventPayloadFull, wantRaw: record.RawTransaction, wantPeers: record.Peers,
		},
		{
			name: "compact included remains complete", event: NewLifecycleEvent(record, TransitionIncluded, time.Unix(20, 0)),
			mode: EventPayloadCompactLifecycle, wantRaw: record.RawTransaction, wantPeers: record.Peers,
		},
		{
			name: "compact finalized", event: NewLifecycleEvent(record, TransitionFinalized, time.Unix(20, 0)),
			mode: EventPayloadCompactLifecycle,
		},
		{
			name: "compact orphaned", event: NewLifecycleEvent(record, TransitionOrphaned, time.Unix(20, 0)),
			mode: EventPayloadCompactLifecycle,
		},
		{
			name: "compact reincluded", event: NewLifecycleEvent(record, TransitionReincluded, time.Unix(20, 0)),
			mode: EventPayloadCompactLifecycle,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ProjectEvent(test.event, test.mode)
			if !reflect.DeepEqual(got.Record.RawTransaction, test.wantRaw) || !reflect.DeepEqual(got.Record.Peers, test.wantPeers) {
				t.Fatalf("ProjectEvent() raw/peers = %v/%v, want %v/%v", got.Record.RawTransaction, got.Record.Peers, test.wantRaw, test.wantPeers)
			}
			if got.ID != test.event.ID || got.Type != test.event.Type || got.Record.TxID != record.TxID || !reflect.DeepEqual(got.Record.Matches, record.Matches) || !reflect.DeepEqual(got.Record.ContractTypes, record.ContractTypes) {
				t.Fatalf("ProjectEvent() changed identity or matching metadata: %+v", got)
			}
		})
	}
	if !reflect.DeepEqual(record.RawTransaction, []byte{0x0a, 0x00}) || !reflect.DeepEqual(record.Peers, []string{"peer-a:18888"}) {
		t.Fatalf("source record was mutated: %+v", record)
	}
}

func BenchmarkEventEncoding(b *testing.B) {
	record := Record{
		TxID: "abc123", FirstSeen: time.Unix(10, 0).UTC(), Source: "block",
		ContractTypes:  []string{"TransferContract"},
		Matches:        []filter.Match{{Address: "411111111111111111111111111111111111111111", Role: filter.RoleOwner}},
		RawTransaction: make([]byte, 512), Peers: []string{"peer-a:18888", "peer-b:18888", "peer-c:18888"},
	}
	full := NewLifecycleEvent(record, TransitionFinalized, time.Unix(20, 0))
	compact := ProjectEvent(full, EventPayloadCompactLifecycle)
	for _, scenario := range []struct {
		name  string
		event Event
	}{
		{name: "full", event: full},
		{name: "compact-lifecycle", event: compact},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			encoded, err := json.Marshal(scenario.event)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := json.Marshal(scenario.event); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(encoded)), "wire-B/event")
		})
	}
}
