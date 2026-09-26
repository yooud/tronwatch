// Package finality reads TRON solidified checkpoints from a trusted Solidity endpoint.
package finality

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tronwatch/internal/model"
)

// SoliditySource fetches /walletsolidity/getnowblock without treating height alone as finality.
type SoliditySource struct {
	endpoint string
	token    string
	client   *http.Client
}

func NewSoliditySource(endpoint, token string, timeout time.Duration, allowInsecure bool) (*SoliditySource, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Solidity URL %q", endpoint)
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && allowInsecure) {
		return nil, errors.New("Solidity URL must use https")
	}
	if timeout <= 0 {
		return nil, errors.New("Solidity timeout must be positive")
	}
	return &SoliditySource{endpoint: endpoint, token: token, client: &http.Client{Timeout: timeout}}, nil
}

// Fetch returns the exact solid block ID and height reported by the endpoint.
func (s *SoliditySource) Fetch(ctx context.Context) (model.SolidBlock, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader([]byte("{}")))
	if err != nil {
		return model.SolidBlock{}, fmt.Errorf("creating Solidity request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		request.Header.Set("TRON-PRO-API-KEY", s.token)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return model.SolidBlock{}, fmt.Errorf("fetching solid block: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return model.SolidBlock{}, fmt.Errorf("Solidity endpoint returned HTTP %d", response.StatusCode)
	}
	var payload struct {
		BlockID string `json:"blockID"`
		Header  struct {
			Raw struct {
				Number    int64 `json:"number"`
				Timestamp int64 `json:"timestamp"`
			} `json:"raw_data"`
		} `json:"block_header"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(&payload); err != nil {
		return model.SolidBlock{}, fmt.Errorf("decoding solid block: %w", err)
	}
	payload.BlockID = strings.ToLower(strings.TrimSpace(payload.BlockID))
	if payload.BlockID == "" || payload.Header.Raw.Number < 0 {
		return model.SolidBlock{}, errors.New("Solidity response has invalid block identity")
	}
	return model.SolidBlock{ID: payload.BlockID, Number: payload.Header.Raw.Number, ObservedAt: time.Now().UTC()}, nil
}
