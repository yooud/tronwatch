package p2p

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadOrCreateNodeID returns a stable node identity stored in a private file.
func LoadOrCreateNodeID(path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("node ID file path is empty")
	}
	if nodeID, err := readNodeID(path); err == nil {
		return nodeID, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("creating node ID directory: %w", err)
	}
	nodeID, err := prepareNodeID(nil)
	if err != nil {
		return nil, err
	}
	temporary, err := os.CreateTemp(directory, ".node-id-*")
	if err != nil {
		return nil, fmt.Errorf("creating temporary node ID file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := func() error {
		if removeErr := os.Remove(temporaryPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return removeErr
		}
		return nil
	}
	if err := temporary.Chmod(0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("setting node ID permissions: %w", err), temporary.Close(), removeTemporary())
	}
	if _, err := temporary.Write(nodeID); err != nil {
		return nil, errors.Join(fmt.Errorf("writing node ID: %w", err), temporary.Close(), removeTemporary())
	}
	if err := temporary.Sync(); err != nil {
		return nil, errors.Join(fmt.Errorf("syncing node ID: %w", err), temporary.Close(), removeTemporary())
	}
	if err := temporary.Close(); err != nil {
		return nil, errors.Join(fmt.Errorf("closing node ID: %w", err), removeTemporary())
	}

	if err := os.Link(temporaryPath, path); err != nil {
		cleanupErr := removeTemporary()
		if errors.Is(err, os.ErrExist) {
			existing, readErr := readNodeID(path)
			return existing, errors.Join(readErr, cleanupErr)
		}
		return nil, errors.Join(fmt.Errorf("installing node ID: %w", err), cleanupErr)
	}
	if err := removeTemporary(); err != nil {
		return nil, fmt.Errorf("removing temporary node ID file: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return nil, err
	}
	return nodeID, nil
}

func readNodeID(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading node ID: %w", err)
	}
	nodeID, err := prepareNodeID(data)
	if err != nil {
		return nil, fmt.Errorf("validating node ID file %q: %w", path, err)
	}
	return nodeID, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening node ID directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing node ID directory: %w", err), directory.Close())
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("closing node ID directory: %w", err)
	}
	return nil
}
