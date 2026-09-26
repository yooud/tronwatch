package watch

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/yooud/tronwatch/internal/filter"
)

// Failure identifies a source whose refresh failed while its last good snapshot stayed active.
type Failure struct {
	Source string
	Err    error
}

// Manager atomically exposes the validated union of all last-good source snapshots.
type Manager struct {
	mu      sync.Mutex
	sources []Source
	states  []Snapshot
	loaded  []bool
	digest  [sha256.Size]byte
	current atomic.Pointer[filter.Set]
	initial []Failure
}

// Open performs the initial load. Optional unavailable sources begin empty.
func Open(ctx context.Context, sources []Source) (*Manager, error) {
	if len(sources) == 0 {
		return nil, errors.New("no watch sources configured")
	}
	manager := &Manager{
		sources: slices.Clone(sources), states: make([]Snapshot, len(sources)), loaded: make([]bool, len(sources)),
	}
	for index, source := range manager.sources {
		snapshot, err := source.Load(ctx)
		if err != nil {
			if source.Required() {
				_ = manager.Close()
				return nil, fmt.Errorf("loading required watch source %q: %w", source.Name(), err)
			}
			manager.initial = append(manager.initial, Failure{Source: source.Name(), Err: err})
			continue
		}
		if _, err := filter.NewSet(snapshot.Addresses, snapshot.Contracts); err != nil {
			if source.Required() {
				_ = manager.Close()
				return nil, fmt.Errorf("validating required watch source %q: %w", source.Name(), err)
			}
			manager.initial = append(manager.initial, Failure{Source: source.Name(), Err: err})
			continue
		}
		manager.states[index] = snapshot
		manager.loaded[index] = true
	}
	set, digest, err := manager.aggregate()
	if err != nil {
		_ = manager.Close()
		return nil, err
	}
	manager.digest = digest
	manager.current.Store(set)
	return manager, nil
}

// Current returns the latest immutable union.
func (m *Manager) Current() *filter.Set { return m.current.Load() }

// InitialFailures returns optional sources that started without a valid snapshot.
func (m *Manager) InitialFailures() []Failure {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.initial)
}

// Refresh polls every source once and publishes a union of successful/last-good snapshots.
func (m *Manager) Refresh(ctx context.Context) (bool, []Failure) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var failures []Failure
	for index, source := range m.sources {
		snapshot, err := source.Load(ctx)
		if err == nil {
			_, err = filter.NewSet(snapshot.Addresses, snapshot.Contracts)
		}
		if err != nil {
			failures = append(failures, Failure{Source: source.Name(), Err: err})
			continue
		}
		m.states[index] = snapshot
		m.loaded[index] = true
	}
	set, digest, err := m.aggregate()
	if err != nil {
		failures = append(failures, Failure{Source: "aggregate", Err: err})
		return false, failures
	}
	if digest == m.digest {
		return false, failures
	}
	m.digest = digest
	m.current.Store(set)
	return true, failures
}

// Close closes network-backed sources.
func (m *Manager) Close() error {
	var result error
	for _, source := range m.sources {
		if closer, ok := source.(interface{ Close() error }); ok {
			result = errors.Join(result, closer.Close())
		}
	}
	return result
}

func (m *Manager) aggregate() (*filter.Set, [sha256.Size]byte, error) {
	var addresses, contracts []string
	for index, snapshot := range m.states {
		if !m.loaded[index] {
			continue
		}
		addresses = append(addresses, snapshot.Addresses...)
		contracts = append(contracts, snapshot.Contracts...)
	}
	set, err := filter.NewSet(addresses, contracts)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("validating aggregated watchlist: %w", err)
	}
	canonicalAddresses := canonical(addresses)
	canonicalContracts := canonical(contracts)
	payload := strings.Join(canonicalAddresses, "\n") + "\x00" + strings.Join(canonicalContracts, "\n")
	return set, sha256.Sum256([]byte(payload)), nil
}

func canonical(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		parsed, err := filter.ParseAddress(value)
		if err != nil {
			continue
		}
		normalized := fmt.Sprintf("%x", parsed)
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	slices.Sort(result)
	return result
}
