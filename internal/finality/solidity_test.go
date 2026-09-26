package finality

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSolidityFetchReturnsExactBlockIDAndHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", request.Method)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"blockID":"ABCDEF","block_header":{"raw_data":{"number":42,"timestamp":1700000000000}}}`))
	}))
	defer server.Close()
	source, err := NewSoliditySource(server.URL, "", time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := source.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.ID != "abcdef" || checkpoint.Number != 42 || checkpoint.ObservedAt.IsZero() {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
}

func TestSoliditySourceRejectsInsecureHTTPByDefaultAndMalformedResponse(t *testing.T) {
	if _, err := NewSoliditySource("http://example.test", "", time.Second, false); err == nil {
		t.Fatal("NewSoliditySource(http) error = nil")
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"blockID":""}`))
	}))
	defer server.Close()
	source, err := NewSoliditySource(server.URL, "", time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Fetch(context.Background()); err == nil {
		t.Fatal("Fetch(malformed) error = nil")
	}
}
