// Package watch aggregates dynamic address and contract sources.
package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxSnapshotBytes = 4 << 20

// Snapshot is the common interchange format accepted by file and HTTP sources.
type Snapshot struct {
	Addresses []string `json:"addresses"`
	Contracts []string `json:"contracts"`
}

// Source loads complete snapshots. Successful snapshots replace that source's prior state.
type Source interface {
	Name() string
	Required() bool
	Load(context.Context) (Snapshot, error)
}

type staticSource struct {
	name     string
	required bool
	snapshot Snapshot
}

func NewInlineSource(name string, snapshot Snapshot, required bool) Source {
	return &staticSource{name: name, required: required, snapshot: snapshot}
}

func (s *staticSource) Name() string                           { return s.name }
func (s *staticSource) Required() bool                         { return s.required }
func (s *staticSource) Load(context.Context) (Snapshot, error) { return s.snapshot, nil }

type fileSource struct {
	name     string
	path     string
	required bool
}

func NewFileSource(name, path string, required bool) Source {
	return &fileSource{name: name, path: path, required: required}
}

func (s *fileSource) Name() string   { return s.name }
func (s *fileSource) Required() bool { return s.required }
func (s *fileSource) Load(context.Context) (Snapshot, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return Snapshot{}, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()
	return decodeSnapshot(io.LimitReader(file, maxSnapshotBytes+1))
}

type httpSource struct {
	name     string
	url      string
	token    string
	required bool
	client   *http.Client
}

func NewHTTPSource(name, endpoint, token string, required bool, timeout time.Duration, allowInsecure bool) (Source, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("invalid HTTP source URL %q", endpoint)
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && allowInsecure) {
		return nil, errors.New("HTTP source URL must use https")
	}
	if timeout <= 0 {
		return nil, errors.New("HTTP source timeout must be positive")
	}
	return &httpSource{
		name: name, url: endpoint, token: token, required: required,
		client: &http.Client{Timeout: timeout},
	}, nil
}

func (s *httpSource) Name() string   { return s.name }
func (s *httpSource) Required() bool { return s.required }
func (s *httpSource) Load(ctx context.Context) (Snapshot, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return Snapshot{}, fmt.Errorf("creating request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if s.token != "" {
		request.Header.Set("Authorization", "Bearer "+s.token)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return Snapshot{}, fmt.Errorf("requesting snapshot: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return Snapshot{}, fmt.Errorf("snapshot endpoint returned HTTP %d", response.StatusCode)
	}
	return decodeSnapshot(io.LimitReader(response.Body, maxSnapshotBytes+1))
}

type redisSetsReader interface {
	SMembers(context.Context, string) ([]string, error)
	Close() error
}

type goRedisSetsReader struct{ client *redis.Client }

func (r *goRedisSetsReader) SMembers(ctx context.Context, key string) ([]string, error) {
	return r.client.SMembers(ctx, key).Result()
}
func (r *goRedisSetsReader) Close() error { return r.client.Close() }

type redisSetsSource struct {
	name, addressesKey, contractsKey string
	required                         bool
	reader                           redisSetsReader
}

func NewRedisSetsSource(name, redisURL, addressesKey, contractsKey string, required bool, timeout time.Duration) (Source, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parsing Redis URL: %w", err)
	}
	if timeout <= 0 {
		return nil, errors.New("Redis timeout must be positive")
	}
	options.DialTimeout = timeout
	options.ReadTimeout = timeout
	options.WriteTimeout = timeout
	return &redisSetsSource{
		name: name, addressesKey: addressesKey, contractsKey: contractsKey, required: required,
		reader: &goRedisSetsReader{client: redis.NewClient(options)},
	}, nil
}

func (s *redisSetsSource) Name() string   { return s.name }
func (s *redisSetsSource) Required() bool { return s.required }
func (s *redisSetsSource) Load(ctx context.Context) (Snapshot, error) {
	var snapshot Snapshot
	if s.addressesKey != "" {
		addresses, err := s.reader.SMembers(ctx, s.addressesKey)
		if err != nil {
			return Snapshot{}, fmt.Errorf("reading address set %q: %w", s.addressesKey, err)
		}
		snapshot.Addresses = addresses
	}
	if s.contractsKey != "" {
		contracts, err := s.reader.SMembers(ctx, s.contractsKey)
		if err != nil {
			return Snapshot{}, fmt.Errorf("reading contract set %q: %w", s.contractsKey, err)
		}
		snapshot.Contracts = contracts
	}
	return snapshot, nil
}
func (s *redisSetsSource) Close() error { return s.reader.Close() }

func decodeSnapshot(reader io.Reader) (Snapshot, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading snapshot: %w", err)
	}
	if len(data) > maxSnapshotBytes {
		return Snapshot{}, fmt.Errorf("snapshot exceeds %d bytes", maxSnapshotBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var snapshot Snapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("decoding snapshot: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return Snapshot{}, errors.New("snapshot contains multiple JSON values")
	} else if err != io.EOF {
		return Snapshot{}, fmt.Errorf("decoding trailing snapshot data: %w", err)
	}
	for index := range snapshot.Addresses {
		snapshot.Addresses[index] = strings.TrimSpace(snapshot.Addresses[index])
	}
	for index := range snapshot.Contracts {
		snapshot.Contracts[index] = strings.TrimSpace(snapshot.Contracts[index])
	}
	return snapshot, nil
}
