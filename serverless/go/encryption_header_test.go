// Property test for the remote encryption key header, implementing the
// `encryption_header` entry of serverless/conformance/differential/spec/ops.json:
// for any key K drawn from the spec's alphabet, a driver configured with K
// sends `x-turso-encryption-key: K` on every HTTP request — pipeline and
// cursor endpoints alike — and a driver with no key never sends the header
// (PROTOCOL.md section 3.1). Runs against a local stub server that records
// request headers; needs no live database and never skips.

package turso

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
)

// Parameters of the `encryption_header` spec entry.
const (
	encHeaderName     = "x-turso-encryption-key"
	encKeyAlphabet    = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	encKeyMinLen      = 1
	encKeyMaxLen      = 64
	encHeaderExamples = 20
)

// recordedRequest is one HTTP request seen by the stub server.
type recordedRequest struct {
	path      string
	hasHeader bool
	key       string
}

// stubServer speaks just enough of the SQL over HTTP protocol for a
// statement to complete, recording the encryption key header of every
// request.
type stubServer struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	requests []recordedRequest
}

func newStubServer(t *testing.T) *stubServer {
	s := &stubServer{t: t}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *stubServer) handle(w http.ResponseWriter, r *http.Request) {
	values := r.Header.Values(encHeaderName)
	s.mu.Lock()
	s.requests = append(s.requests, recordedRequest{
		path:      r.URL.Path,
		hasHeader: len(values) > 0,
		key:       r.Header.Get(encHeaderName),
	})
	s.mu.Unlock()

	switch r.URL.Path {
	case "/v3/pipeline":
		s.handlePipeline(w, r)
	case "/v3/cursor":
		s.handleCursor(w, r)
	default:
		s.t.Errorf("stub server: unexpected path %q", r.URL.Path)
		http.NotFound(w, r)
	}
}

// handlePipeline answers a pipeline request (PROTOCOL.md section 5) with one
// ok result per request, in request order.
func (s *stubServer) handlePipeline(w http.ResponseWriter, r *http.Request) {
	var preq pipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&preq); err != nil {
		s.t.Errorf("stub server: invalid pipeline request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	results := make([]any, len(preq.Requests))
	for i, req := range preq.Requests {
		response := map[string]any{"type": req.Type}
		switch req.Type {
		case "get_autocommit":
			response["is_autocommit"] = true
		case "describe":
			response["result"] = map[string]any{"params": []any{}}
		case "execute":
			response["result"] = map[string]any{
				"cols":               []any{},
				"rows":               []any{},
				"affected_row_count": 0,
				"last_insert_rowid":  nil,
			}
		}
		results[i] = map[string]any{"type": "ok", "response": response}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"baton":    "stub-baton",
		"base_url": nil,
		"results":  results,
	}); err != nil {
		s.t.Errorf("stub server: failed to encode pipeline response: %v", err)
	}
}

// handleCursor answers a cursor request (PROTOCOL.md section 7) with a
// newline-separated body: the cursor response line, then step_begin, row,
// and step_end entries for every step. All conditions (including the
// trailing autocommit probe the driver appends) are treated as satisfied.
func (s *stubServer) handleCursor(w http.ResponseWriter, r *http.Request) {
	var creq cursorRequest
	if err := json.NewDecoder(r.Body).Decode(&creq); err != nil {
		s.t.Errorf("stub server: invalid cursor request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	lines := []any{
		map[string]any{"baton": "stub-baton", "base_url": nil},
	}
	for i := range creq.Batch.Steps {
		lines = append(lines,
			map[string]any{
				"type": "step_begin",
				"step": i,
				"cols": []any{map[string]any{"name": "a", "decltype": nil}},
			},
			map[string]any{
				"type": "row",
				"row":  []any{map[string]any{"type": "integer", "value": "1"}},
			},
			map[string]any{
				"type":               "step_end",
				"affected_row_count": 1,
				"last_insert_rowid":  1,
			},
		)
	}
	enc := json.NewEncoder(w)
	for _, line := range lines {
		if err := enc.Encode(line); err != nil {
			s.t.Errorf("stub server: failed to encode cursor entry: %v", err)
			return
		}
	}
}

// randomEncryptionKey draws a key per the spec entry: a length in
// [encKeyMinLen, encKeyMaxLen] over encKeyAlphabet, plus optional trailing
// base64 '=' padding.
func randomEncryptionKey(rng *rand.Rand) string {
	n := encKeyMinLen + rng.Intn(encKeyMaxLen-encKeyMinLen+1)
	key := make([]byte, 0, n+2)
	for i := 0; i < n; i++ {
		key = append(key, encKeyAlphabet[rng.Intn(len(encKeyAlphabet))])
	}
	for i := rng.Intn(3); i > 0; i-- {
		key = append(key, '=')
	}
	return string(key)
}

func TestEncryptionHeaderProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(20260807))
	for i := 0; i < encHeaderExamples; i++ {
		key := randomEncryptionKey(rng)
		viaDSN := i%2 == 0
		t.Run(fmt.Sprintf("key_%02d", i), func(t *testing.T) {
			runEncryptionHeaderExample(t, key, viaDSN)
		})
	}
	t.Run("no_key_dsn", func(t *testing.T) { runEncryptionHeaderExample(t, "", true) })
	t.Run("no_key_connector", func(t *testing.T) { runEncryptionHeaderExample(t, "", false) })
}

// runEncryptionHeaderExample opens a database configured with key (or with
// no key when empty), runs one exec-style statement and one query, and
// asserts every request the stub saw carried (or did not carry) the header.
func runEncryptionHeaderExample(t *testing.T, key string, viaDSN bool) {
	stub := newStubServer(t)

	var db *sql.DB
	if viaDSN {
		dsn := stub.srv.URL
		if key != "" {
			dsn += "?remote_encryption_key=" + url.QueryEscape(key)
		}
		var err error
		db, err = sql.Open("turso-serverless", dsn)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		connector := NewConnector(stub.srv.URL, "")
		if key != "" {
			connector = connector.WithRemoteEncryptionKey(key)
		}
		db = sql.OpenDB(connector)
	}

	// A multi-statement exec runs as a sequence request on /v3/pipeline; a
	// query runs on /v3/cursor.
	if _, err := db.Exec("CREATE TABLE t (a); INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	var v int64
	if err := db.QueryRow("SELECT a FROM t").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("got %d, want 1", v)
	}
	// Closing sends the stream close request on /v3/pipeline.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) < 3 {
		t.Fatalf("stub saw %d requests, want at least 3", len(stub.requests))
	}
	seen := map[string]bool{}
	for _, req := range stub.requests {
		seen[req.path] = true
		if key == "" {
			if req.hasHeader {
				t.Errorf("%s: unexpected %s header %q", req.path, encHeaderName, req.key)
			}
		} else if !req.hasHeader || req.key != key {
			t.Errorf("%s: got %s header %q (present=%v), want %q",
				req.path, encHeaderName, req.key, req.hasHeader, key)
		}
	}
	if !seen["/v3/pipeline"] || !seen["/v3/cursor"] {
		t.Fatalf("both endpoints must be exercised, saw %v", seen)
	}
}
