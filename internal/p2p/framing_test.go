package p2p

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	want := []byte{messageInventory, 0x12, 0x20, 0x01}
	var stream bytes.Buffer

	if err := writeFrame(&stream, want); err != nil {
		t.Fatalf("writeFrame() error = %v", err)
	}
	got, err := readFrame(&stream, maxMessageSize)
	if err != nil {
		t.Fatalf("readFrame() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("readFrame() = %x, want %x", got, want)
	}
}

func TestReadFrameRejectsOversizedMessage(t *testing.T) {
	stream := bytes.NewBuffer([]byte{0x81, 0x01})
	_, err := readFrame(stream, 128)
	if !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("readFrame() error = %v, want %v", err, errFrameTooLarge)
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	want := bytes.Repeat([]byte("tron-inventory-"), 100)
	packed, err := wrapCompressed(want)
	if err != nil {
		t.Fatalf("wrapCompressed() error = %v", err)
	}
	got, err := unwrapCompressed(packed)
	if err != nil {
		t.Fatalf("unwrapCompressed() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("unwrapCompressed() returned different bytes")
	}
}
