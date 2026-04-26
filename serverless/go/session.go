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
)

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

// session manages connection state (baton) with a remote Turso server.
type session struct {
	client    *http.Client
	authToken string
	baton     *string
	baseURL   string
}

func newSession(url, authToken string) *session {
	return &session{
		client:    &http.Client{},
		authToken: authToken,
		baseURL:   normalizeURL(url),
	}
}

func (s *session) authHeader() string {
	if s.authToken == "" {
		return ""
	}
	return "Bearer " + s.authToken
}

// executeCursor sends a cursor request without context (used by Commit/Rollback).
func (s *session) executeCursor(steps []batchStep) ([]cursorEntry, error) {
	return s.executeCursorCtx(context.Background(), steps)
}

// executeCursorCtx sends a cursor request (streaming NDJSON) and returns parsed entries.
func (s *session) executeCursorCtx(ctx context.Context, steps []batchStep) ([]cursorEntry, error) {
	req := cursorRequest{
		Baton: s.baton,
		Batch: cursorBatch{Steps: steps},
	}
	s.baton = nil

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("turso: json marshal error: %w", err)
	}

	url := s.baseURL + "/v3/cursor"
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("turso: request error: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if auth := s.authHeader(); auth != "" {
		httpReq.Header.Set("Authorization", auth)
	}

	resp, err := s.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("turso: http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, s.extractHTTPError(resp)
	}

	// Parse NDJSON: first line is CursorResponse, rest are CursorEntry
	scanner := bufio.NewScanner(resp.Body)
	// Allow large lines (up to 10MB)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	// First line: cursor response with baton
	if !scanner.Scan() {
		return nil, fmt.Errorf("turso: no cursor response received")
	}
	var cr cursorResponse
	if err := json.Unmarshal(scanner.Bytes(), &cr); err != nil {
		return nil, fmt.Errorf("turso: failed to parse cursor response: %w", err)
	}
	s.baton = cr.Baton
	if cr.BaseURL != nil {
		s.baseURL = *cr.BaseURL
	}

	// Remaining lines: cursor entries
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

	return entries, nil
}

// executePipeline sends a pipeline request without context.
func (s *session) executePipeline(requests []pipelineRequestEntry) (*pipelineResponse, error) {
	return s.executePipelineCtx(context.Background(), requests)
}

// executePipelineCtx sends a pipeline request and returns the response.
func (s *session) executePipelineCtx(ctx context.Context, requests []pipelineRequestEntry) (*pipelineResponse, error) {
	req := pipelineRequest{
		Baton:    s.baton,
		Requests: requests,
	}
	s.baton = nil

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("turso: json marshal error: %w", err)
	}

	url := s.baseURL + "/v3/pipeline"
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("turso: request error: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if auth := s.authHeader(); auth != "" {
		httpReq.Header.Set("Authorization", auth)
	}

	resp, err := s.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("turso: http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
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

	return &pr, nil
}

// close sends a close request if a baton is active.
func (s *session) close() error {
	if s.baton == nil {
		return nil
	}
	_, _ = s.executePipeline([]pipelineRequestEntry{{Type: "close"}})
	s.baton = nil
	return nil
}

// extractHTTPError tries to parse a JSON error message from the response body.
func (s *session) extractHTTPError(resp *http.Response) error {
	// Limit error body reading to 1MB to prevent unbounded memory usage
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
