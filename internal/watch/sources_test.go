package watch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/yooud/tronwatch/internal/filter"
	"github.com/yooud/tronwatch/internal/protocol"
)

func TestHTTPSourceLoadsAuthenticatedSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(response).Encode(Snapshot{
			Addresses: []string{"411111111111111111111111111111111111111111"},
		})
	}))
	defer server.Close()

	source, err := NewHTTPSource("api", server.URL, "secret", true, time.Second, true)
	if err != nil {
		t.Fatalf("NewHTTPSource() error = %v", err)
	}
	got, err := source.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(got.Addresses) != 1 {
		t.Fatalf("addresses = %v", got.Addresses)
	}
}

func TestRedisSetsSourceLoadsBothSets(t *testing.T) {
	reader := &fakeRedisSets{values: map[string][]string{
		"addresses": {"411111111111111111111111111111111111111111"},
		"contracts": {"412222222222222222222222222222222222222222"},
	}}
	source := &redisSetsSource{
		name: "redis", addressesKey: "addresses", contractsKey: "contracts", required: true, reader: reader,
	}
	snapshot, err := source.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(snapshot.Addresses) != 1 || len(snapshot.Contracts) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

type fakeRedisSets struct{ values map[string][]string }

func (r *fakeRedisSets) SMembers(_ context.Context, key string) ([]string, error) {
	return r.values[key], nil
}
func (r *fakeRedisSets) Close() error { return nil }

func makeTransfer(t *testing.T, owner, recipient string) *protocol.Transaction {
	t.Helper()
	ownerAddress, err := filter.ParseAddress(owner)
	if err != nil {
		t.Fatal(err)
	}
	recipientAddress, err := filter.ParseAddress(recipient)
	if err != nil {
		t.Fatal(err)
	}
	parameter, err := anypb.New(&protocol.TransferContract{
		OwnerAddress: ownerAddress[:], ToAddress: recipientAddress[:], Amount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &protocol.Transaction{RawData: &protocol.TransactionRaw{
		Contract: []*protocol.TransactionContract{{Parameter: parameter}},
	}}
}
