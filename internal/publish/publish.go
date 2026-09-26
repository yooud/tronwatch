// Package publish delivers durable transaction events to external systems.
package publish

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"tronwatch/internal/model"
)

type Event = model.Event

func NewEvent(record model.Record, emittedAt time.Time) Event {
	return model.NewEvent(record, emittedAt)
}

// Publisher sends events to one named destination.
type Publisher interface {
	Name() string
	Publish(context.Context, Event) error
	Close() error
}

type stdoutPublisher struct {
	name    string
	mu      sync.Mutex
	encoder *json.Encoder
}

func NewStdout(name string, output io.Writer) Publisher {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return &stdoutPublisher{name: name, encoder: encoder}
}
func (p *stdoutPublisher) Name() string { return p.name }
func (p *stdoutPublisher) Publish(ctx context.Context, event Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.encoder.Encode(event)
}
func (p *stdoutPublisher) Close() error { return nil }

type jsonlPublisher struct {
	name     string
	path     string
	file     *os.File
	mu       sync.Mutex
	writer   *bufio.Writer
	size     int64
	maxBytes int64
	maxFiles int
}

func NewJSONL(name, path string) (Publisher, error) {
	return NewRotatingJSONL(name, path, 0, 0)
}

// NewRotatingJSONL creates a durable JSONL publisher with bounded rotated files.
// maxFiles is the number of retained path.N files in addition to the active file.
func NewRotatingJSONL(name, path string, maxBytes int64, maxFiles int) (Publisher, error) {
	if path == "" {
		return nil, errors.New("JSONL path is empty")
	}
	if (maxBytes == 0) != (maxFiles == 0) || maxBytes < 0 || maxFiles < 0 {
		return nil, errors.New("JSONL max_bytes and max_files must both be positive or both zero")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("creating JSONL directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening JSONL output: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("reading JSONL size: %w", err)
	}
	return &jsonlPublisher{
		name: name, path: path, file: file, writer: bufio.NewWriter(file), size: info.Size(),
		maxBytes: maxBytes, maxFiles: maxFiles,
	}, nil
}
func (p *jsonlPublisher) Name() string { return p.name }
func (p *jsonlPublisher) Publish(ctx context.Context, event Event) error {
	return p.PublishBatch(ctx, []Event{event})
}
func (p *jsonlPublisher) PublishBatch(ctx context.Context, events []Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	var batch bytes.Buffer
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encoding event: %w", err)
		}
		batch.Write(encoded)
		batch.WriteByte('\n')
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.maxBytes > 0 && p.size > 0 && p.size+int64(batch.Len()) > p.maxBytes {
		if err := p.rotate(); err != nil {
			return err
		}
	}
	if _, err := p.writer.Write(batch.Bytes()); err != nil {
		return fmt.Errorf("writing JSONL batch: %w", err)
	}
	if err := p.writer.Flush(); err != nil {
		return fmt.Errorf("flushing JSONL batch: %w", err)
	}
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("syncing JSONL batch: %w", err)
	}
	p.size += int64(batch.Len())
	return nil
}

func (p *jsonlPublisher) rotate() error {
	if err := errors.Join(p.writer.Flush(), p.file.Sync(), p.file.Close()); err != nil {
		return fmt.Errorf("closing JSONL for rotation: %w", err)
	}
	oldest := fmt.Sprintf("%s.%d", p.path, p.maxFiles)
	if err := os.Remove(oldest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing oldest JSONL rotation: %w", err)
	}
	for index := p.maxFiles; index >= 1; index-- {
		source := p.path
		if index > 1 {
			source = fmt.Sprintf("%s.%d", p.path, index-1)
		}
		destination := fmt.Sprintf("%s.%d", p.path, index)
		if err := os.Rename(source, destination); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("rotating JSONL %s: %w", source, err)
		}
	}
	file, err := os.OpenFile(p.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening rotated JSONL output: %w", err)
	}
	p.file = file
	p.writer = bufio.NewWriter(file)
	p.size = 0
	return nil
}
func (p *jsonlPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return errors.Join(p.writer.Flush(), p.file.Sync(), p.file.Close())
}

type webhookPublisher struct {
	name, endpoint, token string
	client                *http.Client
	attempts              int
	backoff               time.Duration
}

func NewWebhook(name, endpoint, token string, timeout time.Duration, attempts int, backoff time.Duration, allowInsecure bool) (Publisher, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("invalid webhook URL %q", endpoint)
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && allowInsecure) {
		return nil, errors.New("webhook URL must use https")
	}
	if timeout <= 0 || attempts <= 0 || backoff <= 0 {
		return nil, errors.New("webhook timeout, attempts, and backoff must be positive")
	}
	return &webhookPublisher{
		name: name, endpoint: endpoint, token: token, client: &http.Client{Timeout: timeout},
		attempts: attempts, backoff: backoff,
	}, nil
}
func (p *webhookPublisher) Name() string { return p.name }
func (p *webhookPublisher) Publish(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encoding webhook event: %w", err)
	}
	var lastError error
	for attempt := 1; attempt <= p.attempts; attempt++ {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(payload))
		if requestErr != nil {
			return fmt.Errorf("creating webhook request: %w", requestErr)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", event.ID)
		if p.token != "" {
			request.Header.Set("Authorization", "Bearer "+p.token)
		}
		response, requestErr := p.client.Do(request)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
			lastError = fmt.Errorf("webhook returned HTTP %d", response.StatusCode)
			if response.StatusCode < 500 && response.StatusCode != http.StatusRequestTimeout && response.StatusCode != http.StatusTooManyRequests {
				return lastError
			}
		} else {
			lastError = requestErr
		}
		if attempt < p.attempts {
			if err := wait(ctx, p.backoff*time.Duration(attempt)); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("webhook delivery failed after %d attempts: %w", p.attempts, lastError)
}
func (p *webhookPublisher) Close() error { return nil }

type redisStreamWriter interface {
	Add(context.Context, string, map[string]any) error
	Close() error
}

type goRedisStreamWriter struct{ client *redis.Client }

func (w *goRedisStreamWriter) Add(ctx context.Context, stream string, values map[string]any) error {
	return w.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err()
}
func (w *goRedisStreamWriter) Close() error { return w.client.Close() }

type redisStreamPublisher struct {
	name, stream string
	writer       redisStreamWriter
}

func NewRedisStream(name, redisURL, stream string, timeout time.Duration) (Publisher, error) {
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
	return &redisStreamPublisher{
		name: name, stream: stream, writer: &goRedisStreamWriter{client: redis.NewClient(options)},
	}, nil
}
func (p *redisStreamPublisher) Name() string { return p.name }
func (p *redisStreamPublisher) Publish(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encoding Redis event: %w", err)
	}
	return p.writer.Add(ctx, p.stream, map[string]any{
		"event_id": event.ID, "type": event.Type, "payload": string(payload),
	})
}
func (p *redisStreamPublisher) Close() error { return p.writer.Close() }

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
