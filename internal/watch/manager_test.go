package watch

import (
	"context"
	"errors"
	"testing"

	"github.com/yooud/tronwatch/internal/protocol"
)

func TestManagerUnionsSourcesAndKeepsLastGoodSnapshot(t *testing.T) {
	first := &stubSource{name: "accounts", snapshots: []Snapshot{{
		Addresses: []string{"411111111111111111111111111111111111111111"},
	}, {}}, errors: []error{nil, errors.New("temporary failure")}}
	second := &stubSource{name: "contracts", snapshots: []Snapshot{{
		Contracts: []string{"412222222222222222222222222222222222222222"},
	}, {
		Contracts: []string{"413333333333333333333333333333333333333333"},
	}}}

	manager, err := Open(context.Background(), []Source{first, second})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	addresses, contracts := manager.Current().Sizes()
	if addresses != 1 || contracts != 1 {
		t.Fatalf("initial sizes = (%d,%d), want (1,1)", addresses, contracts)
	}

	changed, failures := manager.Refresh(context.Background())
	if !changed || len(failures) != 1 || failures[0].Source != "accounts" {
		t.Fatalf("Refresh() = changed %v, failures %+v", changed, failures)
	}
	addresses, contracts = manager.Current().Sizes()
	if addresses != 1 || contracts != 1 {
		t.Fatalf("refreshed sizes = (%d,%d), want last-good account and new contract", addresses, contracts)
	}
	transaction := transferTo(t, "411111111111111111111111111111111111111111")
	if matches := manager.Current().MatchTransaction(transaction); len(matches) != 1 {
		t.Fatalf("last-good matches = %d, want 1", len(matches))
	}
}

func TestOpenFailsWhenRequiredSourceHasNoInitialSnapshot(t *testing.T) {
	source := &stubSource{name: "required", required: true, errors: []error{errors.New("down")}}
	if _, err := Open(context.Background(), []Source{source}); err == nil {
		t.Fatal("Open() error = nil, want required source error")
	}
}

type stubSource struct {
	name      string
	required  bool
	snapshots []Snapshot
	errors    []error
	index     int
}

func (s *stubSource) Name() string   { return s.name }
func (s *stubSource) Required() bool { return s.required }
func (s *stubSource) Load(context.Context) (Snapshot, error) {
	index := s.index
	s.index++
	var snapshot Snapshot
	if index < len(s.snapshots) {
		snapshot = s.snapshots[index]
	}
	if index < len(s.errors) {
		return snapshot, s.errors[index]
	}
	return snapshot, nil
}

func transferTo(t *testing.T, recipient string) *protocol.Transaction {
	t.Helper()
	return makeTransfer(t, "414444444444444444444444444444444444444444", recipient)
}
