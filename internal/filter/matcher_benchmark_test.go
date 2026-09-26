package filter

import (
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/yooud/tronwatch/internal/protocol"
)

var benchmarkMatches []Match

func BenchmarkMatchTransactionTrigger(b *testing.B) {
	ownerAddress, err := ParseAddress(ownerHex)
	if err != nil {
		b.Fatal(err)
	}
	contractAddress, err := ParseAddress(contractHex)
	if err != nil {
		b.Fatal(err)
	}
	recipientAddress, err := ParseAddress(recipientHex)
	if err != nil {
		b.Fatal(err)
	}
	owner := ownerAddress[:]
	contract := contractAddress[:]
	recipient := recipientAddress[:]
	calldata := append([]byte{0xa9, 0x05, 0x9c, 0xbb}, make([]byte, 12)...)
	calldata = append(calldata, recipient[1:]...)
	calldata = append(calldata, make([]byte, 32)...)
	parameter, err := anypb.New(&protocol.TriggerSmartContract{
		OwnerAddress: owner, ContractAddress: contract, Data: calldata,
	})
	if err != nil {
		b.Fatal(err)
	}
	watches, err := NewSet([]string{recipientHex}, []string{contractHex})
	if err != nil {
		b.Fatal(err)
	}
	transaction := transactionWith(parameter)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkMatches = watches.MatchTransaction(transaction)
	}
}
