package p2p

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLoadOrCreateNodeIDPersistsIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity", "node-id")
	first, err := LoadOrCreateNodeID(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreateNodeID(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != nodeIDLength || !bytes.Equal(first, second) {
		t.Fatalf("node IDs have lengths %d/%d or differ across reload", len(first), len(second))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("node ID permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestLoadOrCreateNodeIDConcurrentlyUsesOneIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity", "node-id")
	const callers = 16
	results := make(chan []byte, callers)
	errorsFound := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			nodeID, err := LoadOrCreateNodeID(path)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- nodeID
		}()
	}
	group.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("LoadOrCreateNodeID() error = %v", err)
	}
	var expected []byte
	for nodeID := range results {
		if expected == nil {
			expected = nodeID
			continue
		}
		if !bytes.Equal(nodeID, expected) {
			t.Fatal("concurrent callers returned different node IDs")
		}
	}
	if len(expected) != nodeIDLength {
		t.Fatalf("node ID length = %d, want %d", len(expected), nodeIDLength)
	}
}

func TestLoadOrCreateNodeIDRejectsInvalidExistingIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-id")
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateNodeID(path); err == nil {
		t.Fatal("LoadOrCreateNodeID() error = nil, want invalid length error")
	}
}
