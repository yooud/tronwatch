package filter

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/anypb"

	"tronwatch/internal/protocol"
)

const (
	ownerHex     = "411111111111111111111111111111111111111111"
	recipientHex = "412222222222222222222222222222222222222222"
	contractHex  = "413333333333333333333333333333333333333333"
)

func TestMatchTransactionNativeTransfer(t *testing.T) {
	owner := mustHexAddress(t, ownerHex)
	recipient := mustHexAddress(t, recipientHex)
	parameter, err := anypb.New(&protocol.TransferContract{
		OwnerAddress: owner,
		ToAddress:    recipient,
		Amount:       42,
	})
	if err != nil {
		t.Fatalf("anypb.New() error = %v", err)
	}
	tx := transactionWith(parameter)
	watches, err := NewSet([]string{recipientHex}, nil)
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}

	got := watches.MatchTransaction(tx)
	assertMatches(t, got, []Match{{Address: recipientHex, Role: RoleRecipient, ContractIndex: 0}})
}

func TestMatchTransactionTriggerAndTRC20Recipient(t *testing.T) {
	owner := mustHexAddress(t, ownerHex)
	contract := mustHexAddress(t, contractHex)
	recipient := mustHexAddress(t, recipientHex)
	calldata := append([]byte{0xa9, 0x05, 0x9c, 0xbb}, make([]byte, 12)...)
	calldata = append(calldata, recipient[1:]...)
	calldata = append(calldata, make([]byte, 32)...)
	parameter, err := anypb.New(&protocol.TriggerSmartContract{
		OwnerAddress:    owner,
		ContractAddress: contract,
		Data:            calldata,
	})
	if err != nil {
		t.Fatalf("anypb.New() error = %v", err)
	}
	tx := transactionWith(parameter)
	watches, err := NewSet([]string{recipientHex}, []string{contractHex})
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}

	got := watches.MatchTransaction(tx)
	assertMatches(t, got, []Match{
		{Address: contractHex, Role: RoleContract, ContractIndex: 0},
		{Address: recipientHex, Role: RoleCalldataRecipient, ContractIndex: 0},
	})
}

func TestNewSetRejectsBadChecksum(t *testing.T) {
	_, err := NewSet([]string{"TJRabPrwbZy45sbavfcjinPJC18kjpRTv9"}, nil)
	if err == nil {
		t.Fatal("NewSet() error = nil, want invalid checksum error")
	}
}

func TestMatchTransactionIgnoresUnwatchedAddress(t *testing.T) {
	parameter, err := anypb.New(&protocol.TransferContract{
		OwnerAddress: mustHexAddress(t, ownerHex),
		ToAddress:    mustHexAddress(t, recipientHex),
	})
	if err != nil {
		t.Fatalf("anypb.New() error = %v", err)
	}
	watches, err := NewSet([]string{contractHex}, nil)
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}

	if got := watches.MatchTransaction(transactionWith(parameter)); len(got) != 0 {
		t.Fatalf("MatchTransaction() = %+v, want no matches", got)
	}
}

func TestMatchTransactionTransferAsset(t *testing.T) {
	owner := mustHexAddress(t, ownerHex)
	recipient := mustHexAddress(t, recipientHex)
	var payload []byte
	payload = protowire.AppendTag(payload, 1, protowire.BytesType)
	payload = protowire.AppendBytes(payload, []byte("1002000"))
	payload = protowire.AppendTag(payload, 2, protowire.BytesType)
	payload = protowire.AppendBytes(payload, owner)
	payload = protowire.AppendTag(payload, 3, protowire.BytesType)
	payload = protowire.AppendBytes(payload, recipient)
	parameter := &anypb.Any{
		TypeUrl: "type.googleapis.com/protocol.TransferAssetContract",
		Value:   payload,
	}
	watches, err := NewSet([]string{recipientHex}, nil)
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}

	got := watches.MatchTransaction(transactionWith(parameter))
	assertMatches(t, got, []Match{{Address: recipientHex, Role: RoleRecipient, ContractIndex: 0}})
}

func TestMatchTransactionIgnoresMalformedContractPayload(t *testing.T) {
	watches, err := NewSet([]string{ownerHex}, nil)
	if err != nil {
		t.Fatalf("NewSet() error = %v", err)
	}
	parameter := &anypb.Any{
		TypeUrl: "type.googleapis.com/protocol.DelegateResourceContract",
		Value:   []byte{0xff},
	}
	if got := watches.MatchTransaction(transactionWith(parameter)); len(got) != 0 {
		t.Fatalf("MatchTransaction() = %+v, want no matches", got)
	}
}

func FuzzParseAddressNeverPanics(f *testing.F) {
	f.Add(ownerHex)
	f.Add("TJRabPrwbZy45sbavfcjinPJC18kjpRTv9")
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = ParseAddress(input)
	})
}

func transactionWith(parameter *anypb.Any) *protocol.Transaction {
	return &protocol.Transaction{RawData: &protocol.TransactionRaw{
		Contract: []*protocol.TransactionContract{{Parameter: parameter}},
	}}
}

func assertMatches(t *testing.T, got, want []Match) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("matches = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("matches[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func mustHexAddress(t *testing.T, value string) []byte {
	t.Helper()
	parsed, err := ParseAddress(value)
	if err != nil {
		t.Fatalf("ParseAddress(%q) error = %v", value, err)
	}
	return bytes.Clone(parsed[:])
}
