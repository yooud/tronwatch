// Package p2p implements the minimal TCP subset of the java-tron peer protocol.
package p2p

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/proto"

	"tronwatch/internal/protocol"
)

const (
	maxMessageSize = 5 * 1024 * 1024

	messageTransaction  = byte(0x01)
	messageBlock        = byte(0x02)
	messageTransactions = byte(0x03)
	messageInventory    = byte(0x06)
	messageFetchData    = byte(0x07)
	messageHello        = byte(0x20)
	messageDisconnect   = byte(0x21)
	messagePing         = byte(0x22)
	messagePong         = byte(0x23)

	transportPing       = byte(0xff)
	transportPong       = byte(0xfe)
	transportHello      = byte(0xfd)
	transportDisconnect = byte(0xfb)
)

var (
	errFrameTooLarge      = errors.New("p2p frame is too large")
	errMalformedFrameSize = errors.New("malformed p2p frame size")
)

func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) >= maxMessageSize {
		return fmt.Errorf("%w: %d bytes", errFrameTooLarge, len(payload))
	}
	var prefix [binary.MaxVarintLen32]byte
	prefixLength := binary.PutUvarint(prefix[:], uint64(len(payload)))
	if err := writeAll(w, prefix[:prefixLength]); err != nil {
		return fmt.Errorf("writing frame size: %w", err)
	}
	if err := writeAll(w, payload); err != nil {
		return fmt.Errorf("writing frame payload: %w", err)
	}
	return nil
}

func readFrame(r io.Reader, limit int) ([]byte, error) {
	length, err := readUvarint32(r)
	if err != nil {
		return nil, err
	}
	if length >= uint64(limit) {
		return nil, fmt.Errorf("%w: %d bytes", errFrameTooLarge, length)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("reading frame payload: %w", err)
	}
	return payload, nil
}

func readUvarint32(r io.Reader) (uint64, error) {
	var value uint64
	for shift := uint(0); shift < 35; shift += 7 {
		var current [1]byte
		if _, err := io.ReadFull(r, current[:]); err != nil {
			return 0, fmt.Errorf("reading frame size: %w", err)
		}
		value |= uint64(current[0]&0x7f) << shift
		if current[0] < 0x80 {
			return value, nil
		}
	}
	return 0, errMalformedFrameSize
}

func writeAll(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := w.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func wrapCompressed(data []byte) ([]byte, error) {
	if len(data) >= maxMessageSize {
		return nil, fmt.Errorf("%w: %d bytes", errFrameTooLarge, len(data))
	}
	compressed := snappy.Encode(nil, data)
	envelope := &protocol.CompressMessage{Data: data}
	if len(compressed) < len(data) {
		envelope.Type = protocol.CompressMessage_SNAPPY
		envelope.Data = compressed
	}
	packed, err := proto.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshalling compression envelope: %w", err)
	}
	return packed, nil
}

func unwrapCompressed(payload []byte) ([]byte, error) {
	var envelope protocol.CompressMessage
	if err := proto.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshalling compression envelope: %w", err)
	}
	switch envelope.Type {
	case protocol.CompressMessage_UNCOMPRESSED:
		if len(envelope.Data) >= maxMessageSize {
			return nil, fmt.Errorf("%w: %d bytes", errFrameTooLarge, len(envelope.Data))
		}
		return envelope.Data, nil
	case protocol.CompressMessage_SNAPPY:
		length, err := snappy.DecodedLen(envelope.Data)
		if err != nil {
			return nil, fmt.Errorf("reading Snappy size: %w", err)
		}
		if length >= maxMessageSize {
			return nil, fmt.Errorf("%w: %d bytes", errFrameTooLarge, length)
		}
		decoded, err := snappy.Decode(nil, envelope.Data)
		if err != nil {
			return nil, fmt.Errorf("decoding Snappy payload: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported compression type %d", envelope.Type)
	}
}
