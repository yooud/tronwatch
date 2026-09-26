// Package filter matches watched TRON addresses against transaction participants.
package filter

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

const (
	tronAddressLength = 21
	tronAddressPrefix = byte(0x41)
)

var errInvalidAddress = errors.New("invalid TRON address")

// ParseAddress validates a Base58Check or 41-prefixed hex TRON address.
func ParseAddress(value string) ([tronAddressLength]byte, error) {
	var address [tronAddressLength]byte
	value = strings.TrimSpace(value)
	if len(value) == tronAddressLength*2 {
		decoded, err := hex.DecodeString(value)
		if err != nil {
			return address, fmt.Errorf("%w: malformed hex: %v", errInvalidAddress, err)
		}
		if decoded[0] != tronAddressPrefix {
			return address, fmt.Errorf("%w: hex prefix must be 41", errInvalidAddress)
		}
		copy(address[:], decoded)
		return address, nil
	}

	decoded, err := decodeBase58(value)
	if err != nil {
		return address, fmt.Errorf("%w: %v", errInvalidAddress, err)
	}
	if len(decoded) != tronAddressLength+4 {
		return address, fmt.Errorf("%w: decoded length is %d", errInvalidAddress, len(decoded))
	}
	payload, checksum := decoded[:tronAddressLength], decoded[tronAddressLength:]
	wantChecksum := doubleSHA256(payload)
	if !equalBytes(checksum, wantChecksum[:4]) {
		return address, fmt.Errorf("%w: checksum mismatch", errInvalidAddress)
	}
	if payload[0] != tronAddressPrefix {
		return address, fmt.Errorf("%w: network prefix is %02x", errInvalidAddress, payload[0])
	}
	copy(address[:], payload)
	return address, nil
}

func normalizeAddress(value []byte) (string, bool) {
	if len(value) != tronAddressLength || value[0] != tronAddressPrefix {
		return "", false
	}
	return hex.EncodeToString(value), true
}

func decodeBase58(value string) ([]byte, error) {
	if value == "" {
		return nil, errors.New("empty Base58 string")
	}
	alphabet := "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	number := new(big.Int)
	base := big.NewInt(58)
	for _, char := range value {
		index := strings.IndexRune(alphabet, char)
		if index < 0 {
			return nil, fmt.Errorf("invalid Base58 character %q", char)
		}
		number.Mul(number, base)
		number.Add(number, big.NewInt(int64(index)))
	}
	decoded := number.Bytes()
	leadingZeroes := 0
	for leadingZeroes < len(value) && value[leadingZeroes] == alphabet[0] {
		leadingZeroes++
	}
	result := make([]byte, leadingZeroes+len(decoded))
	copy(result[leadingZeroes:], decoded)
	return result, nil
}

func doubleSHA256(payload []byte) [32]byte {
	first := sha256.Sum256(payload)
	return sha256.Sum256(first[:])
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for i := range left {
		difference |= left[i] ^ right[i]
	}
	return difference == 0
}
