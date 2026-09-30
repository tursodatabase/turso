// HTTP session management: one session per connection, tracking the
// stream baton, base URL, and transaction state.

package turso

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// EncryptionKeyHeader is the HTTP header that carries the customer-managed
// encryption key for an encrypted database.
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

// stmtOutput is the decoded output of one executed statement.
type stmtOutput struct {
	columns      []string
	decltypes    []string
	rows         [][]any
	rowsAffected int64
}

// session manages one server-side stream: the baton, the base URL, and the
// server-reported transaction state.
type session struct {
	client    *http.Client
	authToken string
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

// resetStream forgets the stream. Any transaction on it was (or will be)
// rolled back by the server, so the connection is back in autocommit mode.
func (s *session) resetStream() {
	s.baton = nil
	s.autocommit = true
}

func (s *session) updateStream(baton *string, baseURL *string) {
	s.baton = baton
	if baseURL != nil && *baseURL != "" {
		s.baseURL = normalizeURL(*baseURL)
	}
}

// httpError builds an error from a non-200 response, surfacing the JSON
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
			return fmt.Errorf("turso: HTTP status %d: %s", resp.StatusCode, message)
		}
	}
	return fmt.Errorf("turso: HTTP status %d", resp.StatusCode)
}

// post sends a request that carries the current baton. Any failure,
// including a non-200 response, is fatal for the stream (section 9.3): the
// error surfaces to the caller and the next statement starts a fresh
// stream. The driver never re-sends a request on its own; whether
// re-running a failed statement is safe is the application's call.
func (s *session) post(ctx context.Context, path string, body []byte) (*http.Response, error) {
	url := s.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		s.resetStream()
		return nil, fmt.Errorf("turso: request to %s failed: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.authToken)
	}
	if s.remoteEncryptionKey != "" {
		req.Header.Set(EncryptionKeyHeader, s.remoteEncryptionKey)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.resetStream()
		return nil, fmt.Errorf("turso: request to %s failed: %w", url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		s.resetStream()
		return nil, httpError(resp)
	}
	return resp, nil
}

// executePipeline executes a pipeline (section 5). When trackAutocommit is
// set, a `get_autocommit` request is appended and its answer refreshes the
// cached transaction state; the returned results cover only the caller's
// requests.
func (s *session) executePipeline(ctx context.Context, requests []streamRequest, trackAutocommit bool) ([]streamResult, error) {
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
		s.resetStream()
		return nil, fmt.Errorf("turso: invalid pipeline response: %w", err)
	}
	s.updateStream(pr.Baton, pr.BaseURL)
	results := pr.Results
	// The protocol guarantees one result per request (section 5.2); a
	// mismatch would misattribute results to the wrong requests, and
	// leaves the stream state unknown.
	if len(results) != len(reqs) {
		s.resetStream()
		return nil, fmt.Errorf("turso: pipeline response has %d results for %d requests", len(results), len(reqs))
	}
	if trackAutocommit {
		result := results[len(results)-1]
		results = results[:len(results)-1]
		if result.Error != nil {
			return nil, serverError(result.Error)
		}
		if result.Response == nil || result.Response.Type != "get_autocommit" || result.Response.IsAutocommit == nil {
			s.resetStream()
			return nil, fmt.Errorf("turso: expected get_autocommit result in pipeline response")
		}
		s.autocommit = *result.Response.IsAutocommit
	}
	return results, nil
}

// refreshAutocommit refreshes the cached transaction state with a
// standalone `get_autocommit` request. Failures are swallowed: this runs
// on error paths where the original failure must not be masked, and a dead
// stream already reset the state to autocommit.
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
	output          stmtOutput
	lastInsertRowid *int64
	stepError       *Error
	fatal           *Error
	probeExecuted   bool
	probeUnreliable bool
	inProbe         bool
}

func (d *cursorDecoder) feed(entry *cursorEntry) error {
	switch entry.Type {
	case "step_begin":
		if entry.Step != nil && *entry.Step == probeStep {
			d.inProbe = true
			d.probeExecuted = true
		} else {
			d.inProbe = false
			for _, col := range entry.Cols {
				name := ""
				if col.Name != nil {
					name = *col.Name
				}
				decltype := ""
				if col.Decltype != nil {
					decltype = *col.Decltype
				}
				d.output.columns = append(d.output.columns, name)
				d.output.decltypes = append(d.output.decltypes, decltype)
			}
		}
	case "row":
		if d.inProbe || d.stepError != nil {
			return nil
		}
		row := make([]any, len(entry.Row))
		for i, pv := range entry.Row {
			v, err := decodeValue(pv)
			if err != nil {
				return err
			}
			row[i] = v
		}
		d.output.rows = append(d.output.rows, row)
	case "step_end":
		if d.inProbe {
			return nil
		}
		d.output.rowsAffected = entry.AffectedRowCount
		rowid, err := parseRowid(entry.LastInsertRowid)
		if err != nil {
			return err
		}
		if rowid != nil {
			d.lastInsertRowid = rowid
		}
	case "step_error":
		if entry.Step != nil && *entry.Step == probeStep {
			d.probeUnreliable = true
		} else if d.stepError == nil {
			d.stepError = serverError(entry.Error)
		}
		d.inProbe = false
	case "error":
		d.fatal = serverError(entry.Error)
	}
	// replication_index (section 7.2.6) and unknown entries are ignored;
	// a cleanly ended body is complete without a terminator.
	return nil
}

// executeStmt executes a single statement on the cursor endpoint
// (section 7), decoding the streamed entries into a buffered result.
func (s *session) executeStmt(ctx context.Context, stmt stmtBody) (*stmtOutput, error) {
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
		s.resetStream()
		return nil, err
	}
	if firstLine == nil {
		s.resetStream()
		return nil, fmt.Errorf("turso: cursor response body ended before the cursor response line")
	}
	var cr cursorResponse
	if err := json.Unmarshal(firstLine, &cr); err != nil {
		s.resetStream()
		return nil, fmt.Errorf("turso: invalid cursor response: %w", err)
	}
	s.updateStream(cr.Baton, cr.BaseURL)

	decoder := &cursorDecoder{}
	for decoder.fatal == nil {
		line, err := nextLine(reader)
		if err != nil {
			// The body failed mid-stream; the stream is unusable.
			s.resetStream()
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
			s.resetStream()
			return nil, fmt.Errorf("turso: invalid cursor entry: %w", err)
		}
		if err := decoder.feed(&entry); err != nil {
			// A malformed value from the server; abandon the stream
			// rather than resume it in an unknown position.
			s.resetStream()
			return nil, err
		}
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
		_, _ = s.executePipeline(context.Background(), []streamRequest{{Type: "close"}}, false)
	}
	s.resetStream()
}
