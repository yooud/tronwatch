package filter

import (
	"encoding/hex"
	"path"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/yooud/tronwatch/internal/protocol"
)

// Role describes how an address participates in a transaction.
type Role string

const (
	RoleOwner             Role = "owner"
	RoleRecipient         Role = "recipient"
	RoleContract          Role = "contract"
	RoleCalldataSender    Role = "calldata_sender"
	RoleCalldataRecipient Role = "calldata_recipient"
	RoleCalldataSpender   Role = "calldata_spender"
)

type participantField struct {
	number   protowire.Number
	role     Role
	contract bool
}

var participantFields = map[string][]participantField{
	"AccountCreateContract":           {{1, RoleOwner, false}, {2, RoleRecipient, false}},
	"AccountUpdateContract":           {{2, RoleOwner, false}},
	"SetAccountIdContract":            {{2, RoleOwner, false}},
	"AccountPermissionUpdateContract": {{1, RoleOwner, false}},
	"AssetIssueContract":              {{1, RoleOwner, false}},
	"TransferAssetContract":           {{2, RoleOwner, false}, {3, RoleRecipient, false}},
	"UnfreezeAssetContract":           {{1, RoleOwner, false}},
	"UpdateAssetContract":             {{1, RoleOwner, false}},
	"ParticipateAssetIssueContract":   {{1, RoleOwner, false}, {2, RoleRecipient, false}},
	"FreezeBalanceContract":           {{1, RoleOwner, false}, {15, RoleRecipient, false}},
	"UnfreezeBalanceContract":         {{1, RoleOwner, false}, {15, RoleRecipient, false}},
	"WithdrawBalanceContract":         {{1, RoleOwner, false}},
	"FreezeBalanceV2Contract":         {{1, RoleOwner, false}},
	"UnfreezeBalanceV2Contract":       {{1, RoleOwner, false}},
	"WithdrawExpireUnfreezeContract":  {{1, RoleOwner, false}},
	"DelegateResourceContract":        {{1, RoleOwner, false}, {4, RoleRecipient, false}},
	"UnDelegateResourceContract":      {{1, RoleOwner, false}, {4, RoleRecipient, false}},
	"CancelAllUnfreezeV2Contract":     {{1, RoleOwner, false}},
	"WitnessCreateContract":           {{1, RoleOwner, false}},
	"WitnessUpdateContract":           {{1, RoleOwner, false}},
	"VoteAssetContract":               {{1, RoleOwner, false}, {2, RoleRecipient, false}},
	"ProposalApproveContract":         {{1, RoleOwner, false}},
	"ProposalCreateContract":          {{1, RoleOwner, false}},
	"ProposalDeleteContract":          {{1, RoleOwner, false}},
	"ExchangeCreateContract":          {{1, RoleOwner, false}},
	"ExchangeInjectContract":          {{1, RoleOwner, false}},
	"ExchangeWithdrawContract":        {{1, RoleOwner, false}},
	"ExchangeTransactionContract":     {{1, RoleOwner, false}},
	"MarketSellAssetContract":         {{1, RoleOwner, false}},
	"MarketCancelOrderContract":       {{1, RoleOwner, false}},
	"CreateSmartContract":             {{1, RoleOwner, false}},
	"VoteWitnessContract":             {{1, RoleOwner, false}},
	"BuyStorageBytesContract":         {{1, RoleOwner, false}},
	"BuyStorageContract":              {{1, RoleOwner, false}},
	"SellStorageContract":             {{1, RoleOwner, false}},
	"UpdateBrokerageContract":         {{1, RoleOwner, false}},
	"ShieldedTransferContract":        {{1, RoleOwner, false}, {6, RoleRecipient, false}},
	"ClearABIContract":                {{1, RoleOwner, false}, {2, RoleContract, true}},
	"UpdateSettingContract":           {{1, RoleOwner, false}, {2, RoleContract, true}},
	"UpdateEnergyLimitContract":       {{1, RoleOwner, false}, {2, RoleContract, true}},
}

// Match is one watched-address occurrence in a transaction contract.
type Match struct {
	Address       string `json:"address"`
	Role          Role   `json:"role"`
	ContractIndex int    `json:"contract_index"`
}

// Set is an immutable snapshot of watched accounts and smart contracts.
type Set struct {
	addresses map[[tronAddressLength]byte]struct{}
	contracts map[[tronAddressLength]byte]struct{}
}

// NewSet validates addresses and builds an immutable watch snapshot.
func NewSet(addresses, contracts []string) (*Set, error) {
	set := &Set{
		addresses: make(map[[tronAddressLength]byte]struct{}, len(addresses)),
		contracts: make(map[[tronAddressLength]byte]struct{}, len(contracts)),
	}
	for _, value := range addresses {
		address, err := ParseAddress(value)
		if err != nil {
			return nil, err
		}
		set.addresses[address] = struct{}{}
	}
	for _, value := range contracts {
		address, err := ParseAddress(value)
		if err != nil {
			return nil, err
		}
		set.contracts[address] = struct{}{}
	}
	return set, nil
}

// MatchTransaction returns all unique matches in contract order.
func (s *Set) MatchTransaction(transaction *protocol.Transaction) []Match {
	if transaction == nil || transaction.RawData == nil {
		return nil
	}
	var matches []Match
	seen := make(map[Match]struct{})
	add := func(raw []byte, role Role, contractIndex int, watched map[[tronAddressLength]byte]struct{}) {
		if len(raw) != tronAddressLength {
			return
		}
		var address [tronAddressLength]byte
		copy(address[:], raw)
		if _, ok := watched[address]; !ok {
			return
		}
		match := Match{Address: hex.EncodeToString(raw), Role: role, ContractIndex: contractIndex}
		if _, exists := seen[match]; exists {
			return
		}
		seen[match] = struct{}{}
		matches = append(matches, match)
	}

	for index, contract := range transaction.RawData.Contract {
		if contract == nil || contract.Parameter == nil {
			continue
		}
		typeName := path.Base(contract.Parameter.TypeUrl)
		if separator := strings.LastIndexByte(typeName, '.'); separator >= 0 {
			typeName = typeName[separator+1:]
		}
		switch typeName {
		case "TransferContract":
			var transfer protocol.TransferContract
			if proto.Unmarshal(contract.Parameter.Value, &transfer) != nil {
				continue
			}
			add(transfer.OwnerAddress, RoleOwner, index, s.addresses)
			add(transfer.ToAddress, RoleRecipient, index, s.addresses)
		case "TriggerSmartContract":
			var trigger protocol.TriggerSmartContract
			if proto.Unmarshal(contract.Parameter.Value, &trigger) != nil {
				continue
			}
			add(trigger.OwnerAddress, RoleOwner, index, s.addresses)
			add(trigger.ContractAddress, RoleContract, index, s.contracts)
			for _, participant := range calldataParticipants(trigger.Data) {
				add(participant.address, participant.role, index, s.addresses)
			}
		default:
			fields, ok := participantFields[typeName]
			if !ok {
				continue
			}
			participants, valid := extractParticipants(contract.Parameter.Value, fields)
			if !valid {
				continue
			}
			for _, participant := range participants {
				watched := s.addresses
				if participant.contract {
					watched = s.contracts
				}
				add(participant.address, participant.role, index, watched)
			}
		}
	}
	return matches
}

type wireParticipant struct {
	address  []byte
	role     Role
	contract bool
}

func extractParticipants(payload []byte, fields []participantField) ([]wireParticipant, bool) {
	var participants []wireParticipant
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return nil, false
		}
		payload = payload[tagLength:]
		if wireType == protowire.BytesType {
			value, valueLength := protowire.ConsumeBytes(payload)
			if valueLength < 0 {
				return nil, false
			}
			for _, field := range fields {
				if field.number == number {
					participants = append(participants, wireParticipant{
						address: value, role: field.role, contract: field.contract,
					})
				}
			}
			payload = payload[valueLength:]
			continue
		}
		valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
		if valueLength < 0 {
			return nil, false
		}
		payload = payload[valueLength:]
	}
	return participants, true
}

type calldataParticipant struct {
	address []byte
	role    Role
}

func calldataParticipants(data []byte) []calldataParticipant {
	if len(data) < 4 {
		return nil
	}
	addressAt := func(argument int) []byte {
		start := 4 + argument*32
		if start+32 > len(data) {
			return nil
		}
		for _, padding := range data[start : start+12] {
			if padding != 0 {
				return nil
			}
		}
		address := make([]byte, tronAddressLength)
		address[0] = tronAddressPrefix
		copy(address[1:], data[start+12:start+32])
		return address
	}
	selector := hex.EncodeToString(data[:4])
	switch selector {
	case "a9059cbb": // transfer(address,uint256)
		return []calldataParticipant{{address: addressAt(0), role: RoleCalldataRecipient}}
	case "23b872dd": // transferFrom(address,address,uint256)
		return []calldataParticipant{
			{address: addressAt(0), role: RoleCalldataSender},
			{address: addressAt(1), role: RoleCalldataRecipient},
		}
	case "095ea7b3": // approve(address,uint256)
		return []calldataParticipant{{address: addressAt(0), role: RoleCalldataSpender}}
	default:
		return nil
	}
}
