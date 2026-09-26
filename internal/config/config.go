// Package config loads and validates tronwatch daemon configuration.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"tronwatch/internal/filter"
	"tronwatch/internal/model"
)

// Duration is a human-readable JSON duration such as "2s".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("duration must be a string")
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value, err)
	}
	*d = Duration(parsed)
	return nil
}

// Duration returns the standard-library duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

type Config struct {
	P2P           P2PConfig           `json:"p2p"`
	Storage       StorageConfig       `json:"storage"`
	Watch         WatchConfig         `json:"watch"`
	Publishers    []PublisherConfig   `json:"publishers"`
	Logging       LoggingConfig       `json:"logging"`
	Finality      FinalityConfig      `json:"finality"`
	Observability ObservabilityConfig `json:"observability"`
	Runtime       RuntimeConfig       `json:"runtime"`
}

type P2PConfig struct {
	Peer        string   `json:"peer,omitempty"`
	Peers       []string `json:"peers,omitempty"`
	NetworkID   int32    `json:"network_id"`
	AdvertiseIP string   `json:"advertise_ip"`
}

// PeerList returns the configured endpoints. Peer is retained as a legacy single-peer alias.
func (c P2PConfig) PeerList() []string {
	if len(c.Peers) > 0 {
		return append([]string(nil), c.Peers...)
	}
	if c.Peer == "" {
		return nil
	}
	return []string{c.Peer}
}

type StorageConfig struct {
	Path                     string                 `json:"path"`
	PersistAllPeers          bool                   `json:"persist_all_peers,omitempty"`
	SyncFreelist             bool                   `json:"sync_freelist,omitempty"`
	FinalizedRetentionBlocks int64                  `json:"finalized_retention_blocks,omitempty"`
	EventPayloadMode         model.EventPayloadMode `json:"event_payload_mode,omitempty"`
}

type WatchConfig struct {
	RefreshInterval Duration       `json:"refresh_interval"`
	Sources         []SourceConfig `json:"sources"`
}

type SourceConfig struct {
	Name              string   `json:"name"`
	Type              string   `json:"type"`
	Required          *bool    `json:"required,omitempty"`
	Path              string   `json:"path,omitempty"`
	URL               string   `json:"url,omitempty"`
	URLEnv            string   `json:"url_env,omitempty"`
	TokenEnv          string   `json:"token_env,omitempty"`
	Token             string   `json:"-"`
	Timeout           Duration `json:"timeout,omitempty"`
	AllowInsecureHTTP bool     `json:"allow_insecure_http,omitempty"`
	AddressesKey      string   `json:"addresses_key,omitempty"`
	ContractsKey      string   `json:"contracts_key,omitempty"`
	Addresses         []string `json:"addresses,omitempty"`
	Contracts         []string `json:"contracts,omitempty"`
}

// IsRequired reports whether startup must fail without an initial source snapshot.
func (s SourceConfig) IsRequired() bool { return s.Required == nil || *s.Required }

type PublisherConfig struct {
	Name               string   `json:"name"`
	Type               string   `json:"type"`
	Path               string   `json:"path,omitempty"`
	URL                string   `json:"url,omitempty"`
	URLEnv             string   `json:"url_env,omitempty"`
	TokenEnv           string   `json:"token_env,omitempty"`
	Token              string   `json:"-"`
	Stream             string   `json:"stream,omitempty"`
	Timeout            Duration `json:"timeout,omitempty"`
	Attempts           int      `json:"attempts,omitempty"`
	RetryBackoff       Duration `json:"retry_backoff,omitempty"`
	AllowInsecureHTTP  bool     `json:"allow_insecure_http,omitempty"`
	MaxBytes           int64    `json:"max_bytes,omitempty"`
	MaxFiles           int      `json:"max_files,omitempty"`
	BatchSize          int      `json:"batch_size,omitempty"`
	FlushInterval      Duration `json:"flush_interval,omitempty"`
	MaxDeliveryLatency Duration `json:"max_delivery_latency,omitempty"`
}

// DeliveryInterval returns the polling interval capped by the latency budget.
func (p PublisherConfig) DeliveryInterval() time.Duration {
	return min(p.FlushInterval.Duration(), p.MaxDeliveryLatency.Duration())
}

type LoggingConfig struct {
	JSON          bool     `json:"json"`
	StatsInterval Duration `json:"stats_interval"`
}

type FinalityConfig struct {
	URL               string   `json:"url,omitempty"`
	URLEnv            string   `json:"url_env,omitempty"`
	TokenEnv          string   `json:"token_env,omitempty"`
	Token             string   `json:"-"`
	PollInterval      Duration `json:"poll_interval"`
	Timeout           Duration `json:"timeout"`
	MaxStaleness      Duration `json:"max_staleness"`
	Required          bool     `json:"required,omitempty"`
	AllowInsecureHTTP bool     `json:"allow_insecure_http,omitempty"`
}

// Enabled reports whether an external solid-block oracle is configured.
func (c FinalityConfig) Enabled() bool { return strings.TrimSpace(c.URL) != "" }

type ObservabilityConfig struct {
	Listen          string   `json:"listen"`
	AllowPublic     bool     `json:"allow_public,omitempty"`
	BlockStaleAfter Duration `json:"block_stale_after"`
	MinimumPeers    int      `json:"minimum_peers"`
	OutboxWarnDepth int      `json:"outbox_warn_depth"`
}

type RuntimeConfig struct {
	MaxDuration       Duration `json:"max_duration,omitempty"`
	MinimumFreeBytes  uint64   `json:"minimum_free_bytes,omitempty"`
	DiskCheckInterval Duration `json:"disk_check_interval"`
}

// Load reads a strict JSON configuration and resolves referenced environment variables.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("reading config: %w", err)
	}
	configuration := defaults()
	peerSet, peersSet, err := configuredPeerFields(data)
	if err != nil {
		return Config{}, err
	}
	if peerSet && peersSet {
		return Config{}, errors.New("p2p.peer and p2p.peers cannot be used together")
	}
	if peersSet {
		configuration.P2P.Peer = ""
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configuration); err != nil {
		return Config{}, fmt.Errorf("decoding config: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return Config{}, err
	}
	if err := configuration.resolveAndValidate(); err != nil {
		return Config{}, err
	}
	return configuration, nil
}

func defaults() Config {
	return Config{
		P2P:     P2PConfig{Peer: "127.0.0.1:18888", NetworkID: 201910292, AdvertiseIP: "127.0.0.1"},
		Storage: StorageConfig{Path: "data/transactions.db", EventPayloadMode: model.EventPayloadFull},
		Watch:   WatchConfig{RefreshInterval: Duration(time.Second)},
		Logging: LoggingConfig{StatsInterval: Duration(30 * time.Second)},
		Finality: FinalityConfig{
			PollInterval: Duration(15 * time.Second), Timeout: Duration(5 * time.Second),
			MaxStaleness: Duration(2 * time.Minute),
		},
		Observability: ObservabilityConfig{
			Listen: "127.0.0.1:9464", BlockStaleAfter: Duration(30 * time.Second),
			MinimumPeers: 1, OutboxWarnDepth: 10_000,
		},
		Runtime: RuntimeConfig{DiskCheckInterval: Duration(30 * time.Second)},
	}
}

func (c *Config) resolveAndValidate() error {
	peers := c.P2P.PeerList()
	if len(peers) == 0 {
		return errors.New("p2p.peers is empty")
	}
	seenPeers := make(map[string]struct{}, len(peers))
	for index, peer := range peers {
		peer = strings.TrimSpace(peer)
		if err := validatePeer(peer); err != nil {
			return fmt.Errorf("p2p peer %q: %w", peer, err)
		}
		if _, exists := seenPeers[peer]; exists {
			return fmt.Errorf("p2p peer %q is duplicated", peer)
		}
		seenPeers[peer] = struct{}{}
		peers[index] = peer
	}
	if len(c.P2P.Peers) > 0 {
		c.P2P.Peers = peers
	} else {
		c.P2P.Peer = peers[0]
	}
	if c.P2P.NetworkID <= 0 {
		return errors.New("p2p.network_id must be positive")
	}
	if net.ParseIP(c.P2P.AdvertiseIP) == nil {
		return fmt.Errorf("p2p.advertise_ip %q is invalid", c.P2P.AdvertiseIP)
	}
	if strings.TrimSpace(c.Storage.Path) == "" {
		return errors.New("storage.path is empty")
	}
	if c.Storage.FinalizedRetentionBlocks < 0 {
		return errors.New("storage.finalized_retention_blocks cannot be negative")
	}
	if c.Storage.EventPayloadMode != model.EventPayloadFull && c.Storage.EventPayloadMode != model.EventPayloadCompactLifecycle {
		return fmt.Errorf("storage.event_payload_mode %q is unsupported", c.Storage.EventPayloadMode)
	}
	if c.Watch.RefreshInterval.Duration() <= 0 {
		return errors.New("watch.refresh_interval must be positive")
	}
	if len(c.Watch.Sources) == 0 {
		return errors.New("watch.sources is empty")
	}
	names := make(map[string]struct{}, len(c.Watch.Sources))
	for index := range c.Watch.Sources {
		source := &c.Watch.Sources[index]
		if err := validateName("watch source", source.Name, names); err != nil {
			return err
		}
		if source.Timeout == 0 {
			source.Timeout = Duration(3 * time.Second)
		}
		if source.Timeout.Duration() <= 0 {
			return fmt.Errorf("watch source %q timeout must be positive", source.Name)
		}
		if err := resolveURLAndToken(&source.URL, source.URLEnv, &source.Token, source.TokenEnv); err != nil {
			return fmt.Errorf("watch source %q: %w", source.Name, err)
		}
		if err := validateSource(*source); err != nil {
			return fmt.Errorf("watch source %q: %w", source.Name, err)
		}
	}
	names = make(map[string]struct{}, len(c.Publishers))
	for index := range c.Publishers {
		publisher := &c.Publishers[index]
		if err := validateName("publisher", publisher.Name, names); err != nil {
			return err
		}
		if publisher.Timeout == 0 {
			publisher.Timeout = Duration(3 * time.Second)
		}
		if publisher.Attempts == 0 {
			publisher.Attempts = 3
		}
		if publisher.RetryBackoff == 0 {
			publisher.RetryBackoff = Duration(250 * time.Millisecond)
		}
		if publisher.BatchSize == 0 {
			publisher.BatchSize = 100
		}
		if publisher.FlushInterval == 0 {
			publisher.FlushInterval = Duration(250 * time.Millisecond)
		}
		if publisher.MaxDeliveryLatency == 0 {
			publisher.MaxDeliveryLatency = Duration(250 * time.Millisecond)
		}
		if publisher.Timeout.Duration() <= 0 || publisher.RetryBackoff.Duration() <= 0 || publisher.Attempts <= 0 {
			return fmt.Errorf("publisher %q retry settings must be positive", publisher.Name)
		}
		if publisher.BatchSize <= 0 || publisher.FlushInterval.Duration() <= 0 || publisher.MaxDeliveryLatency.Duration() <= 0 {
			return fmt.Errorf("publisher %q batch settings must be positive", publisher.Name)
		}
		if err := resolveURLAndToken(&publisher.URL, publisher.URLEnv, &publisher.Token, publisher.TokenEnv); err != nil {
			return fmt.Errorf("publisher %q: %w", publisher.Name, err)
		}
		if err := validatePublisher(*publisher); err != nil {
			return fmt.Errorf("publisher %q: %w", publisher.Name, err)
		}
	}
	if c.Logging.StatsInterval.Duration() <= 0 {
		return errors.New("logging.stats_interval must be positive")
	}
	if err := resolveURLAndToken(&c.Finality.URL, c.Finality.URLEnv, &c.Finality.Token, c.Finality.TokenEnv); err != nil {
		return fmt.Errorf("finality: %w", err)
	}
	if c.Finality.Required && !c.Finality.Enabled() {
		return errors.New("finality.url is required when finality.required is true")
	}
	if c.Finality.Enabled() {
		if c.Finality.PollInterval.Duration() <= 0 || c.Finality.Timeout.Duration() <= 0 || c.Finality.MaxStaleness.Duration() <= 0 {
			return errors.New("finality intervals must be positive")
		}
		if err := validateHTTPURL(c.Finality.URL, c.Finality.AllowInsecureHTTP); err != nil {
			return fmt.Errorf("finality: %w", err)
		}
	}
	if err := validateObservability(c.Observability); err != nil {
		return err
	}
	if c.Runtime.MaxDuration.Duration() < 0 {
		return errors.New("runtime.max_duration cannot be negative")
	}
	if c.Runtime.MinimumFreeBytes > 0 && c.Runtime.DiskCheckInterval.Duration() <= 0 {
		return errors.New("runtime.disk_check_interval must be positive when disk guard is enabled")
	}
	return nil
}

func validateObservability(configuration ObservabilityConfig) error {
	host, portText, err := net.SplitHostPort(configuration.Listen)
	if err != nil {
		return fmt.Errorf("observability.listen: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("observability.listen has invalid port")
	}
	if !configuration.AllowPublic {
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("observability.listen must be loopback unless allow_public is true")
		}
	}
	if configuration.BlockStaleAfter.Duration() <= 0 || configuration.MinimumPeers < 1 || configuration.OutboxWarnDepth < 1 {
		return errors.New("observability thresholds must be positive")
	}
	return nil
}

func configuredPeerFields(data []byte) (bool, bool, error) {
	var root struct {
		P2P map[string]json.RawMessage `json:"p2p"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		return false, false, fmt.Errorf("decoding config: %w", err)
	}
	_, peerSet := root.P2P["peer"]
	_, peersSet := root.P2P["peers"]
	return peerSet, peersSet, nil
}

func validatePeer(peer string) error {
	host, portText, err := net.SplitHostPort(peer)
	if err != nil {
		return fmt.Errorf("must be host:port: %w", err)
	}
	if strings.TrimSpace(host) == "" {
		return errors.New("host is empty")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("port %q is invalid", portText)
	}
	return nil
}

func validateSource(source SourceConfig) error {
	switch source.Type {
	case "inline":
		if len(source.Addresses) == 0 && len(source.Contracts) == 0 {
			return errors.New("inline source is empty")
		}
		if _, err := filter.NewSet(source.Addresses, source.Contracts); err != nil {
			return fmt.Errorf("invalid inline snapshot: %w", err)
		}
	case "file":
		if source.Path == "" {
			return errors.New("path is empty")
		}
	case "http":
		return validateHTTPURL(source.URL, source.AllowInsecureHTTP)
	case "redis_sets":
		if err := validateRedisURL(source.URL); err != nil {
			return err
		}
		if source.AddressesKey == "" && source.ContractsKey == "" {
			return errors.New("at least one Redis set key is required")
		}
	default:
		return fmt.Errorf("unsupported type %q", source.Type)
	}
	return nil
}

func validatePublisher(publisher PublisherConfig) error {
	switch publisher.Type {
	case "stdout":
		return nil
	case "jsonl":
		if publisher.Path == "" {
			return errors.New("path is empty")
		}
		if (publisher.MaxBytes == 0) != (publisher.MaxFiles == 0) || publisher.MaxBytes < 0 || publisher.MaxFiles < 0 {
			return errors.New("max_bytes and max_files must both be positive or both zero")
		}
	case "webhook":
		return validateHTTPURL(publisher.URL, publisher.AllowInsecureHTTP)
	case "redis_stream":
		if err := validateRedisURL(publisher.URL); err != nil {
			return err
		}
		if publisher.Stream == "" {
			return errors.New("stream is empty")
		}
	default:
		return fmt.Errorf("unsupported type %q", publisher.Type)
	}
	return nil
}

func validateHTTPURL(value string, allowInsecure bool) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("invalid URL %q", value)
	}
	if parsed.Scheme == "https" {
		return nil
	}
	if parsed.Scheme == "http" && allowInsecure {
		return nil
	}
	return errors.New("URL must use https (or set allow_insecure_http explicitly)")
}

func validateRedisURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") {
		return fmt.Errorf("invalid Redis URL %q", value)
	}
	return nil
}

func validateName(kind, value string, seen map[string]struct{}) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s name is empty", kind)
	}
	if _, exists := seen[value]; exists {
		return fmt.Errorf("duplicate %s name %q", kind, value)
	}
	seen[value] = struct{}{}
	return nil
}

func resolveURLAndToken(destination *string, urlEnv string, token *string, tokenEnv string) error {
	if *destination != "" && urlEnv != "" {
		return errors.New("url and url_env are mutually exclusive")
	}
	if urlEnv != "" {
		value, ok := os.LookupEnv(urlEnv)
		if !ok || value == "" {
			return fmt.Errorf("environment variable %s is unset", urlEnv)
		}
		*destination = value
	}
	if tokenEnv != "" {
		value, ok := os.LookupEnv(tokenEnv)
		if !ok {
			return fmt.Errorf("environment variable %s is unset", tokenEnv)
		}
		*token = value
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return errors.New("config contains multiple JSON values")
	} else if err != io.EOF {
		return fmt.Errorf("decoding trailing config data: %w", err)
	}
	return nil
}
