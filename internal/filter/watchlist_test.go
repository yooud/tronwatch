package filter

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/yooud/tronwatch/internal/protocol"
)

func TestWatchlistRefreshSwapsValidSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchlist.json")
	writeWatchlist(t, path, `{"addresses":["`+ownerHex+`"],"contracts":[]}`)
	watchlist, err := OpenWatchlist(path)
	if err != nil {
		t.Fatalf("OpenWatchlist() error = %v", err)
	}
	writeWatchlist(t, path, `{"addresses":["`+recipientHex+`"],"contracts":[]}`)

	changed, err := watchlist.Refresh()
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if !changed {
		t.Fatal("Refresh() changed = false, want true")
	}
	parameter, err := anypb.New(&protocol.TransferContract{
		OwnerAddress: mustHexAddress(t, ownerHex),
		ToAddress:    mustHexAddress(t, recipientHex),
	})
	if err != nil {
		t.Fatalf("anypb.New() error = %v", err)
	}
	got := watchlist.Current().MatchTransaction(transactionWith(parameter))
	assertMatches(t, got, []Match{{Address: recipientHex, Role: RoleRecipient, ContractIndex: 0}})
}

func TestWatchlistRefreshKeepsLastGoodSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchlist.json")
	writeWatchlist(t, path, `{"addresses":["`+ownerHex+`"],"contracts":[]}`)
	watchlist, err := OpenWatchlist(path)
	if err != nil {
		t.Fatalf("OpenWatchlist() error = %v", err)
	}
	writeWatchlist(t, path, `{"addresses":[`) // interrupted atomic replacement or bad edit

	changed, err := watchlist.Refresh()
	if err == nil {
		t.Fatal("Refresh() error = nil, want malformed JSON error")
	}
	if changed {
		t.Fatal("Refresh() changed = true, want false")
	}
	if addresses, contracts := watchlist.Current().Sizes(); addresses != 1 || contracts != 0 {
		t.Fatalf("Sizes() = (%d, %d), want (1, 0)", addresses, contracts)
	}
}

func TestOpenWatchlistRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchlist.json")
	writeWatchlist(t, path, `{"address":["`+ownerHex+`"]}`)
	_, err := OpenWatchlist(path)
	if err == nil {
		t.Fatal("OpenWatchlist() error = nil, want unknown field error")
	}
}

func writeWatchlist(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}
