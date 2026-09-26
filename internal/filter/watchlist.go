package filter

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type watchlistConfig struct {
	Addresses []string `json:"addresses"`
	Contracts []string `json:"contracts"`
}

// Watchlist atomically publishes validated snapshots loaded from a JSON file.
type Watchlist struct {
	path    string
	mu      sync.Mutex
	digest  [sha256.Size]byte
	current atomic.Pointer[Set]
}

// OpenWatchlist loads and validates the initial watchlist.
func OpenWatchlist(path string) (*Watchlist, error) {
	set, digest, err := loadWatchlist(path)
	if err != nil {
		return nil, err
	}
	watchlist := &Watchlist{path: path, digest: digest}
	watchlist.current.Store(set)
	return watchlist, nil
}

// Current returns the latest validated immutable snapshot.
func (w *Watchlist) Current() *Set {
	return w.current.Load()
}

// Refresh publishes a new snapshot when file content changes.
// Invalid updates return an error and leave the last good snapshot active.
func (w *Watchlist) Refresh() (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	set, digest, err := loadWatchlist(w.path)
	if err != nil {
		return false, err
	}
	if digest == w.digest {
		return false, nil
	}
	w.digest = digest
	w.current.Store(set)
	return true, nil
}

// Sizes returns the number of watched accounts and smart contracts.
func (s *Set) Sizes() (addresses, contracts int) {
	return len(s.addresses), len(s.contracts)
}

func loadWatchlist(path string) (*Set, [sha256.Size]byte, error) {
	var emptyDigest [sha256.Size]byte
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, emptyDigest, fmt.Errorf("reading watchlist: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config watchlistConfig
	if err := decoder.Decode(&config); err != nil {
		return nil, emptyDigest, fmt.Errorf("decoding watchlist: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, emptyDigest, err
	}
	set, err := NewSet(config.Addresses, config.Contracts)
	if err != nil {
		return nil, emptyDigest, fmt.Errorf("validating watchlist: %w", err)
	}
	return set, sha256.Sum256(data), nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return errors.New("watchlist contains multiple JSON values")
	} else if err != io.EOF {
		return fmt.Errorf("decoding trailing watchlist data: %w", err)
	}
	return nil
}
