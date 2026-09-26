package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tronwatch/internal/store"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(version) code = %d, want 0; stderr=%q", code, stderr.String())
	}
	if got := stdout.String(); got != version+"\n" {
		t.Fatalf("version output = %q, want %q", got, version+"\n")
	}
}

func TestRunHelpExitsSuccessfully(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(--help) code = %d, want 0; stderr=%q", code, stderr.String())
	}
}

func TestRunRejectsUnexpectedArgument(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"unexpected"}, &stdout, &stderr); code != 1 {
		t.Fatalf("run(unexpected) code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unexpected positional argument") {
		t.Fatalf("stderr = %q, want unexpected positional argument", stderr.String())
	}
}

func TestRunCheckConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := []byte(`{
		"watch":{"sources":[{"name":"local","type":"inline","addresses":["411111111111111111111111111111111111111111"]}]},
		"publishers":[{"name":"stdout","type":"stdout"}]
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"check-config", "--config", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(check-config) code = %d; stderr=%q", code, stderr.String())
	}
	if stdout.String() != "configuration valid\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestStringListFlagKeepsEveryPeer(t *testing.T) {
	var peers stringListFlag
	if err := peers.Set("127.0.0.1:18888"); err != nil {
		t.Fatal(err)
	}
	if err := peers.Set("127.0.0.2:18888"); err != nil {
		t.Fatal(err)
	}
	if len(peers) != 2 || peers[0] == peers[1] {
		t.Fatalf("peers = %v, want two endpoints", peers)
	}
}

func TestRunCheckDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.db")
	database, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"check-db", "--db", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(check-db) code = %d stderr=%q", code, stderr.String())
	}
	if stdout.String() != "database valid\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"probe", "--url", server.URL}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(probe) code = %d stderr=%q", code, stderr.String())
	}
	if stdout.String() != "ready\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}
