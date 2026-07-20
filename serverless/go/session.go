package tursogo_serverless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// encryptionKeyHeader carries the base64 key for encrypted Turso Cloud DBs.
const encryptionKeyHeader = "x-turso-encryption-key"

// normalizeURL rewrites turso:// and libsql:// schemes to https://.
func normalizeURL(rawURL string) string {
	if rest, ok := strings.CutPrefix(rawURL, "libsql://"); ok {
		return "https://" + rest
	}
	if rest, ok := strings.CutPrefix(rawURL, "turso://"); ok {
		return "https://" + rest
	}
	return rawURL
}

// autocommitProbeStep is a trailing batch step gated on is_autocommit,
// appended to every cursor request. The cursor endpoint cannot carry a
// get_autocommit probe, so whether this step executed tells us the
// connection's transaction state without an extra round trip.
func autocommitProbeStep() batchStep {
	return batchStep{
		Stmt: stmtBody{
			SQL:       "SELECT 1",
			Args:      []protoValue{},
			NamedArgs: []namedArg{},
			WantRows:  false,
		},
		Condition: condIsAutocommit(),
	}
}

// session manages connection state (baton, autocommit) with a remote server.
//
// Transaction state is server-authoritative: every cursor request carries the
// is_autocommit probe above and every pipeline request a get_autocommit
// request, so inTransaction reflects sqlite3_get_autocommit() on the server
// rather than any guess from the SQL text. The baton is always persisted — a
// non-null baton does NOT imply an open transaction.
type session struct {
	client         *http.Client
	authToken      string
	baton          *string
	baseURL        string
	autocommit     bool
	requestHeaders map[string]string
	encryptionKey  string
	queryTimeoutMs int
}

func newSession(url, authToken string, requestHeaders map[string]string, encryptionKey string, queryTimeoutMs int) *session {
	return &session{
		client:         &http.Client{},
		authToken:      authToken,
		baseURL:        normalizeURL(url),
		autocommit:     true,
		requestHeaders: requestHeaders,
		encryptionKey:  encryptionKey,
		queryTimeoutMs: queryTimeoutMs,
	}
}

// inTransaction reports whether a transaction is open, from the server's last
// authoritative autocommit answer.
func (s *session) inTransaction() bool { return !s.autocommit }

func (s *session) authHeader() string {
	if s.authToken == "" {
		return ""
	}
	return "Bearer " + s.authToken
}

func (s *session) applyHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if auth := s.authHeader(); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if s.encryptionKey != "" {
		req.Header.Set(encryptionKeyHeader, s.encryptionKey)
	}
	for k, v := range s.requestHeaders {
		req.Header.Set(k, v)
	}
}

// ctxWithTimeout applies the session query timeout unless the context already
// carries a deadline.
func (s *session) ctxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.queryTimeoutMs > 0 {
		if _, ok := ctx.Deadline(); !ok {
			return context.WithTimeout(ctx, time.Duration(s.queryTimeoutMs)*time.Millisecond)
		}
	}
	return ctx, func() {}
}

// executeCursor sends a cursor request without context (used by Commit/Rollback).
func (s *session) executeCursor(steps []batchStep) ([]cursorEntry, error) {
	return s.executeCursorCtx(context.Background(), steps)
}

// executeCursorCtx sends a cursor request (streaming NDJSON), appends the
// autocommit probe, refreshes the cached transaction state, always persists
// the baton, and returns the caller's entries with the probe filtered out.
func (s *session) executeCursorCtx(ctx context.Context, steps []batchStep) ([]cursorEntry, error) {
	ctx, cancel := s.ctxWithTimeout(ctx)
	defer cancel()

	probeIdx := len(steps)
	allSteps := append(append([]batchStep{}, steps...), autocommitProbeStep())
	req := cursorRequest{Baton: s.baton, Batch: cursorBatch{Steps: allSteps}}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("turso: json marshal error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", s.baseURL+"/v3/cursor", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("turso: request error: %w", err)
	}
	s.applyHeaders(httpReq)

	resp, err := s.client.Do(httpReq)
	if err != nil {
		s.baton = nil
		s.autocommit = true
		return nil, fmt.Errorf("turso: http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.baton = nil
		s.autocommit = true
		return nil, s.extractHTTPError(resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	if !scanner.Scan() {
		s.baton = nil
		s.autocommit = true
		return nil, fmt.Errorf("turso: no cursor response received")
	}
	var cr cursorResponse
	if err := json.Unmarshal(scanner.Bytes(), &cr); err != nil {
		return nil, fmt.Errorf("turso: failed to parse cursor response: %w", err)
	}
	// Always persist the baton; it does not imply an open transaction.
	s.baton = cr.Baton
	if cr.BaseURL != nil {
		s.baseURL = *cr.BaseURL
	}

	var entries []cursorEntry
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var entry cursorEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("turso: failed to parse cursor entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("turso: scanner error: %w", err)
	}

	return s.trackAutocommit(ctx, entries, probeIdx), nil
}

// trackAutocommit filters the probe step's entries out of a cursor stream and
// updates the cached transaction state from whether the probe executed. The
// probe is always the last step, so everything after its step_begin belongs to
// it. If the stream ended abnormally the answer is unreliable and the state is
// refreshed with a fallback pipeline request.
func (s *session) trackAutocommit(ctx context.Context, entries []cursorEntry, probeIdx int) []cursorEntry {
	real := make([]cursorEntry, 0, len(entries))
	sawProbe := false
	unreliable := false
	skippingProbe := false
	for _, e := range entries {
		if e.Type == "step_begin" && e.Step != nil && *e.Step == probeIdx {
			sawProbe = true
			skippingProbe = true
			continue
		}
		if skippingProbe && (e.Type == "row" || e.Type == "step_end") {
			continue
		}
		if e.Type == "error" || (e.Type == "step_error" && e.Step != nil && *e.Step == probeIdx) {
			unreliable = true
			if e.Type == "step_error" {
				continue
			}
		}
		real = append(real, e)
	}

	if !unreliable {
		s.autocommit = sawProbe
	} else {
		s.refreshAutocommit(ctx)
	}
	return real
}

// refreshAutocommit refreshes the cached transaction state with a standalone
// get_autocommit pipeline request. Errors are swallowed — a dead stream means
// the server rolled back, so executePipelineCtx resets to autocommit.
func (s *session) refreshAutocommit(ctx context.Context) {
	_, _ = s.executePipelineCtx(ctx, nil, true)
}

// updateAutocommit reads the get_autocommit answer out of a pipeline response.
func (s *session) updateAutocommit(pr *pipelineResponse) {
	for _, r := range pr.Results {
		if r.Type == "ok" && r.Response != nil &&
			r.Response.Type == "get_autocommit" && r.Response.IsAutocommit != nil {
			s.autocommit = *r.Response.IsAutocommit
			return
		}
	}
}

// executePipeline sends a pipeline request without context.
func (s *session) executePipeline(requests []pipelineRequestEntry) (*pipelineResponse, error) {
	return s.executePipelineCtx(context.Background(), requests, true)
}

// executePipelineCtx sends a pipeline request, optionally appending a
// get_autocommit probe, always persisting the baton and refreshing the cached
// transaction state.
func (s *session) executePipelineCtx(ctx context.Context, requests []pipelineRequestEntry, appendAutocommit bool) (*pipelineResponse, error) {
	ctx, cancel := s.ctxWithTimeout(ctx)
	defer cancel()

	reqs := append([]pipelineRequestEntry{}, requests...)
	if appendAutocommit {
		reqs = append(reqs, pipelineRequestEntry{Type: "get_autocommit"})
	}
	req := pipelineRequest{Baton: s.baton, Requests: reqs}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("turso: json marshal error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", s.baseURL+"/v3/pipeline", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("turso: request error: %w", err)
	}
	s.applyHeaders(httpReq)

	resp, err := s.client.Do(httpReq)
	if err != nil {
		s.baton = nil
		s.autocommit = true
		return nil, fmt.Errorf("turso: http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.baton = nil
		s.autocommit = true
		return nil, s.extractHTTPError(resp)
	}

	var pr pipelineResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("turso: failed to parse pipeline response: %w", err)
	}
	s.baton = pr.Baton
	if pr.BaseURL != nil {
		s.baseURL = *pr.BaseURL
	}
	if appendAutocommit {
		s.updateAutocommit(&pr)
	}
	return &pr, nil
}

// close sends a close request if a baton is active.
func (s *session) close() error {
	if s.baton != nil {
		_, _ = s.executePipelineCtx(context.Background(), []pipelineRequestEntry{{Type: "close"}}, false)
	}
	s.baton = nil
	s.baseURL = ""
	s.autocommit = true
	return nil
}

// extractHTTPError tries to parse a JSON error message from the response body.
func (s *session) extractHTTPError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if len(body) > 0 {
		var errBody struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(body, &errBody) == nil && errBody.Message != "" {
			return fmt.Errorf("turso: %s", errBody.Message)
		}
	}
	return fmt.Errorf("turso: HTTP error %d", resp.StatusCode)
}
