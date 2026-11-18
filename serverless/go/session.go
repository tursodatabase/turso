// HTTP session management: one session per connection, tracking the
// stream baton, base URL, and transaction state.

package turso

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var ErrSessionLost = errors.New("turso: backend session lost; open a new connection")

// EncryptionKeyHeader carries the customer-managed database encryption key.
const EncryptionKeyHeader = "x-turso-encryption-key"

// normalizeURL rewrites libsql:// and turso:// URLs to https:// and strips
// any trailing slashes, since endpoint paths are appended with a leading
// slash.
func normalizeURL(url string) string {
	if rest, ok := strings.CutPrefix(url, "libsql://"); ok {
		url = "https://" + rest
	} else if rest, ok := strings.CutPrefix(url, "turso://"); ok {
		url = "https://" + rest
	}
	return strings.TrimRight(url, "/")
}

// session manages one server-side stream: the baton, the base URL, and the
// server-reported transaction state.
type session struct {
	requestHeaders http.Header
	retainSession  bool
	broken         bool
	client         *http.Client
	authToken      string
	// remoteEncryptionKey is the customer-managed key for an encrypted
	// database, sent as the EncryptionKeyHeader header on every request
	// (section 3.1). Empty when the database is not encrypted.
	remoteEncryptionKey string
	baseURL             string
	baton               *string
	// autocommit is whether the connection is in autocommit mode, from the
	// server's most recent authoritative answer. A non-null baton does NOT
	// imply a transaction (section 4.3), so this is tracked explicitly:
	// every pipeline carries a `get_autocommit` request and every cursor
	// batch a trailing step gated on `is_autocommit`.
	autocommit      bool
	lastInsertRowid int64
}

func newSession(url, authToken, remoteEncryptionKey string) *session {
	return &session{
		client:              &http.Client{},
		authToken:           authToken,
		remoteEncryptionKey: remoteEncryptionKey,
		baseURL:             normalizeURL(url),
		autocommit:          true,
	}
}

// markBroken prevents reuse of a session whose state is unknown.
func (s *session) markBroken() { s.broken = true }

func (s *session) updateStream(baton *string, baseURL *string) error {
	if baton == nil && (s.retainSession || !s.autocommit) {
		s.broken = true
		return ErrSessionLost
	}
	s.baton = baton
	if baseURL != nil && *baseURL != "" {
		s.baseURL = normalizeURL(*baseURL)
	}
	return nil
}

// HTTPError is a non-2xx HTTP response from the server.
// Message contains the JSON error or message field, if available, never the raw body.
type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("turso: HTTP status %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("turso: HTTP status %d", e.StatusCode)
}

// httpError builds an error from a non-2xx response, surfacing the JSON
// error message when the body carries one (section 9.3).
func httpError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var parsed struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		message := parsed.Error
		if message == "" {
			message = parsed.Message
		}
		if message != "" {
			return &HTTPError{StatusCode: resp.StatusCode, Message: message}
		}
	}
	return &HTTPError{StatusCode: resp.StatusCode}
}

// post sends a request carrying the current baton. Failed requests are not retried.
// Retained sessions and open transactions cannot recover on a new stream.
func (s *session) post(ctx context.Context, path string, body []byte) (*http.Response, error) {
	url := s.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		s.resetAfterRequestFailure()
		return nil, fmt.Errorf("turso: request to %s failed: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.authToken)
	}
	if s.remoteEncryptionKey != "" {
		req.Header.Set(EncryptionKeyHeader, s.remoteEncryptionKey)
	}
	for name, values := range s.requestHeaders {
		if name == "Host" {
			req.Host = values[0]
		} else {
			req.Header[name] = append([]string(nil), values...)
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.resetAfterRequestFailure()
		return nil, fmt.Errorf("turso: request to %s failed: %w", url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		s.resetAfterRequestFailure()
		return nil, httpError(resp)
	}
	return resp, nil
}

func (s *session) resetAfterRequestFailure() {
	if s.retainSession || !s.autocommit {
		s.markBroken()
		return
	}
	s.baton = nil
}

// executePipeline executes a pipeline (section 5). When trackAutocommit is
// set, a `get_autocommit` request is appended and its answer refreshes the
// cached transaction state; the returned results cover only the caller's
// requests.
func (s *session) executePipeline(ctx context.Context, requests []streamRequest, trackAutocommit bool) ([]streamResult, error) {
	if s.broken {
		return nil, ErrSessionLost
	}
	reqs := requests
	if trackAutocommit {
		reqs = append(append([]streamRequest{}, requests...), streamRequest{Type: "get_autocommit"})
	}
	body, err := json.Marshal(pipelineRequest{Baton: s.baton, Requests: reqs})
	if err != nil {
		return nil, fmt.Errorf("turso: failed to encode request: %w", err)
	}
	resp, err := s.post(ctx, "/v3/pipeline", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var pr pipelineResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		s.markBroken()
		return nil, fmt.Errorf("turso: invalid pipeline response: %w", err)
	}

	results := pr.Results
	// The protocol guarantees one result per request (section 5.2); a
	// mismatch would misattribute results to the wrong requests, and
	// leaves the stream state unknown.
	if len(results) != len(reqs) {
		s.markBroken()
		return nil, fmt.Errorf("turso: pipeline response has %d results for %d requests", len(results), len(reqs))
	}
	for i, result := range results {
		validError := result.Type == "error" && result.Error != nil && result.Response == nil
		validResponse := result.Type == "ok" && result.Error == nil && result.Response != nil && result.Response.Type == reqs[i].Type
		if !validError && !validResponse {
			s.markBroken()
			return nil, fmt.Errorf("turso: invalid pipeline result for %s", reqs[i].Type)
		}
	}
	if trackAutocommit {
		result := results[len(results)-1]
		results = results[:len(results)-1]
		if result.Error != nil {
			s.markBroken()
			return nil, serverError(result.Error)
		}
		if result.Response == nil || result.Response.Type != "get_autocommit" || result.Response.IsAutocommit == nil {
			s.markBroken()
			return nil, fmt.Errorf("turso: expected get_autocommit result in pipeline response")
		}
		s.autocommit = *result.Response.IsAutocommit
	}
	if err := s.updateStream(pr.Baton, pr.BaseURL); err != nil {
		return nil, err
	}
	return results, nil
}

// refreshAutocommit refreshes the cached transaction state with a
// standalone `get_autocommit` request. Failures are swallowed: this runs
// on error paths where the original failure must not be masked. Session recovery
// follows the same rules as other pipeline requests.
func (s *session) refreshAutocommit(ctx context.Context) {
	_, _ = s.executePipeline(ctx, nil, true)
}

// probeStep is the index of the autocommit probe step appended to every
// cursor batch.
const probeStep = 1

// autocommitProbeStep is the trailing batch step appended to every cursor
// batch: a no-op statement gated on `is_autocommit`. The cursor endpoint
// cannot carry a `get_autocommit` request, so whether this step executed
// tells us the connection's transaction state without an extra round trip.
func autocommitProbeStep() batchStep {
	return batchStep{
		Condition: &batchCond{Type: "is_autocommit"},
		Stmt:      rawStmt("SELECT 1"),
	}
}

// cursorDecoder folds streamed cursor entries (section 7.2) into a
// statement result, tracking the trailing autocommit probe step separately
// from the caller's step.
type cursorDecoder struct {
	output          StatementResult
	lastInsertRowid *int64
	stepError       *Error
	fatal           *Error
	probeExecuted   bool
	probeUnreliable bool
	ended           bool
	terminated      bool
	stepOpen        bool
	inProbe         bool
}

func (d *cursorDecoder) feed(entry *cursorEntry) error {
	if d.terminated {
		return fmt.Errorf("turso: cursor entry after terminator")
	}
	switch entry.Type {
	case "replication_index":
		if !d.ended || d.stepOpen {
			return fmt.Errorf("turso: cursor terminated before statement completed")
		}
		d.terminated = true
	case "step_begin":
		if d.stepOpen || entry.Step == nil || (*entry.Step != 0 && *entry.Step != probeStep) || (*entry.Step == 0 && d.ended) || (*entry.Step == probeStep && !d.ended) {
			return fmt.Errorf("turso: unexpected cursor step")
		}
		d.stepOpen = true
		if entry.Step != nil && *entry.Step == probeStep {
			d.inProbe = true
			d.probeExecuted = true
		} else {
			d.inProbe = false
			for _, col := range entry.Cols {
				d.output.Columns = append(d.output.Columns, Column{Name: col.Name, Decltype: col.Decltype})
			}
		}
	case "row":
		if !d.stepOpen {
			return fmt.Errorf("turso: cursor row outside a step")
		}
		if d.inProbe || d.stepError != nil {
			return nil
		}
		if len(entry.Row) != len(d.output.Columns) {
			return fmt.Errorf("turso: cursor row does not match column count")
		}
		row := make([]any, len(entry.Row))
		for i, pv := range entry.Row {
			v, err := decodeValue(pv)
			if err != nil {
				return err
			}
			row[i] = v
		}
		d.output.Rows = append(d.output.Rows, row)
	case "step_end":
		if !d.stepOpen {
			return fmt.Errorf("turso: cursor step_end outside a step")
		}
		d.stepOpen = false
		if d.inProbe {
			return nil
		}
		d.ended = true
		d.output.RowsAffected = entry.AffectedRowCount
		rowid, err := parseRowid(entry.LastInsertRowid)
		if err != nil {
			return err
		}
		if rowid != nil {
			d.lastInsertRowid = rowid
		}
	case "step_error":
		d.stepOpen = false
		if entry.Step != nil && *entry.Step == probeStep {
			d.probeUnreliable = true
		} else if d.stepError == nil {
			d.ended = true
			d.stepError = serverError(entry.Error)
		}
		d.inProbe = false
	case "error":
		d.fatal = serverError(entry.Error)
	}
	// Unknown extension entries are ignored.
	return nil
}

// executeStmt executes a single statement on the cursor endpoint
// (section 7), decoding the streamed entries into a buffered result.
func (s *session) executeStmt(ctx context.Context, stmt stmtBody) (*StatementResult, error) {
	if s.broken {
		return nil, ErrSessionLost
	}
	steps := []batchStep{{Stmt: stmt}, autocommitProbeStep()}
	body, err := json.Marshal(cursorRequest{Baton: s.baton, Batch: cursorBatch{Steps: steps}})
	if err != nil {
		return nil, fmt.Errorf("turso: failed to encode request: %w", err)
	}
	resp, err := s.post(ctx, "/v3/cursor", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	firstLine, err := nextLine(reader)
	if err != nil {
		s.markBroken()
		return nil, err
	}
	if firstLine == nil {
		s.markBroken()
		return nil, fmt.Errorf("turso: cursor response body ended before the cursor response line")
	}
	var cr cursorResponse
	if err := json.Unmarshal(firstLine, &cr); err != nil {
		s.markBroken()
		return nil, fmt.Errorf("turso: invalid cursor response: %w", err)
	}
	if cr.Baton == nil {
		s.markBroken()
		return nil, ErrSessionLost
	}
	if err := s.updateStream(cr.Baton, cr.BaseURL); err != nil {
		return nil, err
	}

	decoder := &cursorDecoder{}
	for decoder.fatal == nil {
		line, err := nextLine(reader)
		if err != nil {
			// The body failed mid-stream; the stream is unusable.
			s.markBroken()
			if decoder.stepError != nil {
				return nil, decoder.stepError
			}
			return nil, err
		}
		if line == nil {
			break
		}
		var entry cursorEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			s.markBroken()
			return nil, fmt.Errorf("turso: invalid cursor entry: %w", err)
		}
		if err := decoder.feed(&entry); err != nil {
			// A malformed value from the server; abandon the stream
			// rather than resume it in an unknown position.
			s.markBroken()
			return nil, err
		}
	}

	if (!decoder.ended || decoder.stepOpen) && decoder.fatal == nil {
		s.markBroken()
		return nil, fmt.Errorf("turso: cursor ended before statement completed")
	}
	if decoder.fatal != nil {
		// The cursor died but the stream may survive; ask the server for
		// the authoritative transaction state.
		s.refreshAutocommit(ctx)
		if decoder.stepError != nil {
			return nil, decoder.stepError
		}
		return nil, decoder.fatal
	}
	if decoder.probeUnreliable {
		s.refreshAutocommit(ctx)
	} else {
		// TODO: If HTTP completion is signaled only by closing the connection and
		// neither the probe nor a cursor terminator arrived, verify autocommit
		// with the same baton. EOF may mean truncation rather than a skipped probe;
		// invalidate the connection if that state check fails.
		s.autocommit = decoder.probeExecuted
	}
	if decoder.stepError != nil {
		return nil, decoder.stepError
	}
	if decoder.lastInsertRowid != nil {
		s.lastInsertRowid = *decoder.lastInsertRowid
	}
	return &decoder.output, nil
}

// nextLine reads the next non-empty newline-separated line from a cursor
// response body (section 7.2), returning nil at a clean end of body.
func nextLine(reader *bufio.Reader) ([]byte, error) {
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("turso: cursor stream failed: %w", err)
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			return trimmed, nil
		}
		if err == io.EOF {
			return nil, nil
		}
	}
}

// close closes the stream (section 6.8). Errors are ignored: the stream
// may already have expired.
func (s *session) close() {
	if s.baton != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		body, _ := json.Marshal(pipelineRequest{Baton: s.baton, Requests: []streamRequest{{Type: "close"}}})
		resp, err := s.post(ctx, "/v3/pipeline", body)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
		}
	}
	s.baton = nil
	s.broken = true
}
