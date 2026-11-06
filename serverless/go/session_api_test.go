package turso

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDescribeRoutingAndRetention(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+r.Host {
			t.Errorf("credential crossed hosts: %s", r.Host)
		}
		var req pipelineRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		results := []any{}
		for _, request := range req.Requests {
			response := map[string]any{"type": request.Type}
			switch request.Type {
			case "store_sql":
				if req.Baton != nil || request.SQLID == nil {
					t.Error("retention did not open stream")
				}
			case "describe":
				if req.Baton == nil || *req.Baton != r.Host {
					t.Error("lost session")
				}
				response["result"] = map[string]any{"params": []any{map[string]any{"name": ":p"}, map[string]any{"name": nil}}, "cols": []any{map[string]any{"name": "value", "decltype": "INTEGER"}}, "is_readonly": true}
			case "get_autocommit":
				response["is_autocommit"] = true
			case "close":
			default:
				t.Errorf("executed instead of described: %s", request.Type)
			}
			results = append(results, map[string]any{"type": "ok", "response": response})
		}
		json.NewEncoder(w).Encode(map[string]any{"baton": r.Host, "results": results})
	}))
	defer server.Close()
	for _, host := range []string{"project-a", "project-b"} {
		c, err := NewConnector(server.URL, host).WithRequestHeaders(http.Header{"Host": {host}}).WithHTTPClient(server.Client()).WithSessionRetention().OpenConnection(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			desc, err := c.DescribeContext(context.Background(), "select :p, ?")
			if err != nil {
				t.Fatal(err)
			}
			if len(desc.Params) != 2 || *desc.Params[0].Name != ":p" || desc.Params[1].Name != nil || *desc.Columns[0].Decltype != "INTEGER" || !desc.IsReadOnly {
				t.Fatalf("bad description: %+v", desc)
			}
		}
		stmt, err := c.PrepareContext(context.Background(), "select :p, ?")
		if err != nil || stmt.NumInput() != 2 {
			t.Fatalf("prepare: %v %v", stmt, err)
		}
		stmt.Close()
		c.Close()
	}
	if calls.Load() != 10 {
		t.Fatal(calls.Load())
	}
}

func TestSessionLossNeverRestarts(t *testing.T) {
	for _, body := range []string{"", `{"baton":"next"}` + "\n" + `{"type":"step_begin","step":0,"cols":[]}`} {
		t.Run(fmt.Sprintf("body=%d", len(body)), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprintln(w, body) }))
			defer server.Close()
			c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())

			if _, err := c.ExecContext(context.Background(), "BEGIN", nil); err == nil {
				t.Fatal("accepted incomplete cursor")
			}
			if _, err := c.ExecContext(context.Background(), "INSERT INTO t VALUES(1)", nil); !errors.Is(err, ErrSessionLost) {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("restarted session")
			}
			c.Close()
		})
	}
}

func TestBufferedQueryUpdatesSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/pipeline" {
			fmt.Fprintln(w, `{}`)
			return
		}
		var req cursorRequest
		json.NewDecoder(r.Body).Decode(&req)
		if req.Batch.Steps[0].Stmt.SQL == "second" && (req.Baton == nil || *req.Baton != "next") {
			t.Error("baton not reused")
		}
		fmt.Fprintln(w, `{"baton":"next"}`)
		fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
		fmt.Fprintln(w, `{"type":"step_end","affected_row_count":0}`)
		fmt.Fprintln(w, `{"type":"step_begin","step":1,"cols":[]}`)
		fmt.Fprintln(w, `{"type":"step_end","affected_row_count":0}`)
		fmt.Fprintln(w, `{"type":"replication_index","replication_index":null}`)
	}))
	defer server.Close()
	c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())
	defer c.Close()
	for _, query := range []string{"first", "second"} {
		rows, err := c.QueryContext(context.Background(), query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Next(nil); err != io.EOF {
			t.Fatal(err)
		}
		rows.Close()
		if auto, err := c.Autocommit(); err != nil || !auto {
			t.Fatalf("autocommit %v %v", auto, err)
		}
	}
}

func TestPipelineTransactionBaton(t *testing.T) {
	for _, auto := range []bool{false, true} {
		t.Run(fmt.Sprint(auto), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"baton":null,"results":[{"type":"ok","response":{"type":"sequence"}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":%v}}]}`, auto)
			}))
			defer server.Close()
			c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())
			c.sess.autocommit = !auto
			_, err := c.ExecContext(context.Background(), "BEGIN; SELECT 1", nil)
			if auto && err != nil {
				t.Fatal(err)
			}
			if !auto && !errors.Is(err, ErrSessionLost) {
				t.Fatalf("transaction without baton accepted: %v", err)
			}
			if !auto {
				if _, err = c.ExecContext(context.Background(), "insert", nil); !errors.Is(err, ErrSessionLost) {
					t.Fatal(err)
				}
			}
			c.Close()
		})
	}
}

func TestCursorFailureModes(t *testing.T) {
	for _, tail := range []string{
		"",
		`{"type":"row","row":[{"type":"unknown"}]}`,
		`{"type":"error","error":{"message":"failed"}}`,
		`{"type":"step_error","step":0,"error":{"message":"failed"}}` + "\n" + `{"type":"replication_index"}`,
	} {
		t.Run(fmt.Sprintf("tail=%s", tail), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v3/pipeline" {
					fmt.Fprintln(w, `{"results":[{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}}]}`)
					return
				}
				fmt.Fprintln(w, `{"baton":"x"}`)
				fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
				fmt.Fprintln(w, tail)
			}))
			defer server.Close()
			c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())
			defer c.Close()
			_, err := c.QueryContext(context.Background(), "select", nil)
			if err == nil || err == io.EOF {
				t.Fatalf("accepted invalid response: %v", err)
			}
		})
	}
}

type countingTransport struct {
	base  http.RoundTripper
	calls atomic.Int32
}

func (t *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return t.base.RoundTrip(r)
}

func TestHTTPRedirectPolicy(t *testing.T) {
	for _, policy := range []string{"default", "allow", "deny"} {
		t.Run(policy, func(t *testing.T) {
			stub := &stubServer{t: t}
			var received atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1); stub.handle(w, r) }))
			defer destination.Close()
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, destination.URL+r.URL.Path, http.StatusTemporaryRedirect)
			}))
			defer source.Close()
			transport := &countingTransport{base: http.DefaultTransport}
			var redirects atomic.Int32
			client := &http.Client{Transport: transport}
			if policy != "default" {
				client.CheckRedirect = func(*http.Request, []*http.Request) error {
					redirects.Add(1)
					if policy == "deny" {
						return http.ErrUseLastResponse
					}
					return nil
				}
			}
			c, err := NewConnector(source.URL, "").WithHTTPClient(client).OpenConnection(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_, err = c.DescribeContext(context.Background(), "select")
			if policy == "deny" {
				if err == nil || received.Load() != 0 || transport.calls.Load() != 1 {
					t.Fatalf("redirect was not rejected: %v", err)
				}
			} else if err != nil || received.Load() != 1 || transport.calls.Load() != 2 {
				t.Fatalf("redirect was not followed: %v", err)
			}
			if policy != "default" && redirects.Load() != 1 {
				t.Fatal("custom redirect policy was replaced")
			}
		})
	}
}

func TestServerBaseURLRouting(t *testing.T) {
	stub := &stubServer{t: t}
	var received atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		if r.Host != "project" || r.Header.Get("Authorization") != "Bearer token" {
			t.Error("routing headers lost")
		}
		stub.handle(w, r)
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := httptest.NewRecorder()
		stub.handlePipeline(recorder, r)
		var response map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Error(err)
			return
		}
		response["base_url"] = destination.URL
		json.NewEncoder(w).Encode(response)
	}))
	defer source.Close()
	c, err := NewConnector(source.URL, "token").WithRequestHeaders(http.Header{"Host": {"project"}}).WithSessionRetention().OpenConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeContext(context.Background(), "select"); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if received.Load() != 2 {
		t.Fatalf("expected describe and cleanup at new base URL, got %d", received.Load())
	}
}

func TestRetainedSessionExpiry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			fmt.Fprintln(w, `{"baton":"kept","results":[{"type":"ok","response":{"type":"store_sql"}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}}]}`)
			return
		}
		var req pipelineRequest
		json.NewDecoder(r.Body).Decode(&req)
		if req.Baton == nil || *req.Baton != "kept" {
			t.Error("expiry retried with new stream")
		}
		http.Error(w, "expired", http.StatusNotFound)
	}))
	defer server.Close()
	c, err := NewConnector(server.URL, "").WithSessionRetention().OpenConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeContext(context.Background(), "select"); err == nil {
		t.Fatal("expiry accepted")
	}
	if _, err := c.DescribeContext(context.Background(), "select"); !errors.Is(err, ErrSessionLost) {
		t.Fatal(err)
	}
	if c.IsValid() {
		t.Fatal("expired connection can be pooled")
	}
	if calls.Load() != 2 {
		t.Fatal("retried expired session")
	}
	c.Close()
}

func TestRequestHeadersOverrideDefaultsAndAreCopied(t *testing.T) {
	stub := &stubServer{t: t}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Host != "project.example" {
			t.Errorf("Host: %q", r.Host)
		}
		for name, want := range map[string]string{"Authorization": "Bearer explicit", "Content-Type": "application/custom+json", EncryptionKeyHeader: "explicit-key"} {
			if got := r.Header.Get(name); got != want {
				t.Errorf("%s: got %q, want %q", name, got, want)
			}
		}
		if got := r.Header.Values("X-Project-Tag"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
			t.Errorf("multi-value header: %v", got)
		}
		stub.handle(w, r)
	}))
	defer server.Close()
	headers := http.Header{"hOsT": {"project.example"}, "authorization": {"Bearer explicit"}, "content-type": {"application/custom+json"}, EncryptionKeyHeader: {"explicit-key"}, "x-project-tag": {"first", "second"}}
	connector := NewConnector(server.URL, "default-token").WithRemoteEncryptionKey("default-key").WithRequestHeaders(headers)
	headers["hOsT"][0] = "mutated.example"
	headers["x-project-tag"][0] = "mutated"
	headers.Set("Authorization", "Bearer mutated")
	c, err := connector.OpenConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	connector.WithRequestHeaders(http.Header{"Host": {"other.example"}})
	if _, err := c.DescribeContext(context.Background(), "select"); err != nil {
		t.Fatal(err)
	}
	rows, err := c.QueryContext(context.Background(), "select", nil)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	c.Close()
	if calls.Load() != 3 {
		t.Fatalf("expected describe, query and cleanup; got %d", calls.Load())
	}
}

func TestRequestHeadersRejectAmbiguousHost(t *testing.T) {
	for _, headers := range []http.Header{
		{"Host": nil}, {"Host": {""}}, {"Host": {"a", "b"}}, {"Host": {"a"}, "host": {"b"}},
	} {
		if _, err := NewConnector("http://unused", "").WithRequestHeaders(headers).OpenConnection(context.Background()); err == nil {
			t.Fatalf("accepted invalid headers: %v", headers)
		}
	}
}

func TestExecuteContextReturnsRawBufferedResult(t *testing.T) {
	const query = "UPDATE t SET enabled = ? WHERE label = :label RETURNING *"
	const timestamp = "2026-11-04 12:34:56.123000"
	args := []driver.NamedValue{{Ordinal: 1, Value: int64(1)}, {Ordinal: 2, Name: "label", Value: "example"}}
	var executions atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/pipeline" {
			var req pipelineRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				return
			}
			for _, request := range req.Requests {
				if request.Type != "close" {
					t.Errorf("unexpected extra request: %s", request.Type)
				}
			}
			fmt.Fprintln(w, `{}`)
			return
		}
		executions.Add(1)
		var req cursorRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		stmt := req.Batch.Steps[0].Stmt
		if stmt.SQL != query || len(stmt.Args) != 1 || string(stmt.Args[0].Value) != `"1"` || len(stmt.NamedArgs) != 1 || stmt.NamedArgs[0].Name != "label" || string(stmt.NamedArgs[0].Value.Value) != `"example"` {
			t.Errorf("unexpected statement: %+v", stmt)
		}
		fmt.Fprintln(w, `{"baton":"result"}`)
		fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[{"name":"created","decltype":"TIMESTAMP"},{"name":"enabled","decltype":"BOOLEAN"},{"name":null,"decltype":null},{"name":"","decltype":""},{"name":"bytes","decltype":"BLOB"},{"name":"missing","decltype":null}]}`)
		if stmt.WantRows {
			fmt.Fprintf(w, "{\"type\":\"row\",\"row\":[{\"type\":\"text\",\"value\":%q},{\"type\":\"integer\",\"value\":\"1\"},{\"type\":\"float\",\"value\":1.5},{\"type\":\"text\",\"value\":\"\"},{\"type\":\"blob\",\"base64\":\"AP8=\"},{\"type\":\"null\"}]}\n", timestamp)
		}
		fmt.Fprintln(w, `{"type":"step_end","affected_row_count":1,"last_insert_rowid":"42"}`)
		fmt.Fprintln(w, `{"type":"step_begin","step":1,"cols":[]}`)
		fmt.Fprintln(w, `{"type":"step_end","affected_row_count":0}`)
		fmt.Fprintln(w, `{"type":"replication_index"}`)
	}))
	defer server.Close()
	c, err := NewConnector(server.URL, "").OpenConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result, err := c.ExecuteContext(context.Background(), query, args)
	if err != nil {
		t.Fatal(err)
	}
	if executions.Load() != 1 {
		t.Fatal("statement executed more than once")
	}
	if result.RowsAffected != 1 {
		t.Fatalf("affected count: %d", result.RowsAffected)
	}
	want := [][]any{{timestamp, int64(1), float64(1.5), "", []byte{0, 255}, nil}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("raw rows: %#v", result.Rows)
	}
	if len(result.Columns) != 6 || result.Columns[0].Name == nil || result.Columns[0].Decltype == nil || result.Columns[1].Decltype == nil || *result.Columns[0].Name != "created" || *result.Columns[0].Decltype != "TIMESTAMP" || *result.Columns[1].Decltype != "BOOLEAN" || result.Columns[2].Name != nil || result.Columns[2].Decltype != nil || result.Columns[3].Name == nil || *result.Columns[3].Name != "" || result.Columns[3].Decltype == nil || *result.Columns[3].Decltype != "" {
		t.Fatalf("execution columns: %+v", result.Columns)
	}
	db := sql.OpenDB(NewConnector(server.URL, ""))
	defer db.Close()
	var date time.Time
	var enabled int64
	var number float64
	var empty string
	var blob []byte
	var missing any
	err = db.QueryRowContext(context.Background(), query, int64(1), sql.Named("label", "example")).Scan(&date, &enabled, &number, &empty, &blob, &missing)
	if err != nil {
		t.Fatal(err)
	}
	if !date.Equal(time.Date(2026, 11, 4, 12, 34, 56, 123000000, time.UTC)) || enabled != 1 || number != 1.5 || empty != "" || !reflect.DeepEqual(blob, []byte{0, 255}) || missing != nil {
		t.Fatal("database/sql conversion changed")
	}
	exec, err := db.ExecContext(context.Background(), query, int64(1), sql.Named("label", "example"))
	if err != nil {
		t.Fatal(err)
	}
	count, _ := exec.RowsAffected()
	rowid, _ := exec.LastInsertId()
	if count != 1 || rowid != 42 {
		t.Fatalf("database/sql execution counts: %d %d", count, rowid)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatal("subsequent calls changed raw result")
	}
}

func TestExecuteContextEmptyResultsAndErrors(t *testing.T) {
	for _, scenario := range []string{"no-columns", "no-rows", "late-error", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v3/pipeline" {
					(&stubServer{t: t}).handlePipeline(w, r)
					return
				}
				fmt.Fprintln(w, `{"baton":"result"}`)
				if scenario == "no-columns" {
					fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
				} else {
					fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[{"name":"n","decltype":"INTEGER"}]}`)
				}
				if scenario == "late-error" || scenario == "truncated" {
					fmt.Fprintln(w, `{"type":"row","row":[{"type":"integer","value":"9"}]}`)
				}
				if scenario == "truncated" {
					return
				}
				if scenario == "late-error" {
					fmt.Fprintln(w, `{"type":"step_error","step":0,"error":{"message":"failed","code":"SQLITE_ERROR"}}`)
				} else {
					fmt.Fprintln(w, `{"type":"step_end","affected_row_count":7}`)
				}
				fmt.Fprintln(w, `{"type":"step_begin","step":1,"cols":[]}`)
				fmt.Fprintln(w, `{"type":"step_end","affected_row_count":0}`)
				fmt.Fprintln(w, `{"type":"replication_index"}`)
			}))
			defer server.Close()
			c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())
			defer c.Close()
			result, err := c.ExecuteContext(context.Background(), "statement", nil)
			if scenario == "late-error" || scenario == "truncated" {
				if err == nil || result != nil {
					t.Fatalf("returned partial result: %+v, %v", result, err)
				}
				if scenario == "late-error" {
					var serverErr *Error
					if !errors.As(err, &serverErr) || serverErr.Code != "SQLITE_ERROR" {
						t.Fatal(err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 0 || result.RowsAffected != 7 {
				t.Fatalf("empty result: %+v", result)
			}
			if scenario == "no-columns" && len(result.Columns) != 0 {
				t.Fatal(result.Columns)
			}
			if scenario == "no-rows" && (len(result.Columns) != 1 || *result.Columns[0].Decltype != "INTEGER") {
				t.Fatal(result.Columns)
			}
		})
	}
}

func TestExecuteContextRejectsUnavailableConnection(t *testing.T) {
	for _, closed := range []bool{false, true} {
		c, _ := NewConnector("http://unused", "").OpenConnection(context.Background())
		want := ErrSessionLost
		if closed {
			c.Close()
			want = ErrTursoConnClosed
		} else {
			c.sess.broken = true
		}
		result, err := c.ExecuteContext(context.Background(), "select 1", nil)
		if result != nil || !errors.Is(err, want) {
			t.Fatalf("unavailable connection: %+v, %v", result, err)
		}
	}
}

func TestCloseReusesSharedHTTPConnection(t *testing.T) {
	stub := &stubServer{t: t}
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(stub.handle))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	for i := range 20 {
		project := fmt.Sprintf("project-%d", i)
		conn, err := NewConnector(server.URL, project).
			WithHTTPClient(client).
			WithRequestHeaders(http.Header{"Host": {project}}).
			WithSessionRetention().OpenConnection(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("20 sequential sessions opened %d TCP connections, want 1", got)
	}
}

func TestHTTPErrorBodies(t *testing.T) {
	for _, tc := range []struct{ name, body, message string }{
		{"error", `{"error":"denied","message":"other"}`, "denied"},
		{"message", `{"message":"unavailable"}`, "unavailable"},
		{"fallback", `{"error":"","message":"denied"}`, "denied"},
		{"empty", "", ""},
		{"missing", `{}`, ""},
		{"null", `null`, ""},
		{"malformed", `{"message":"secret"`, ""},
		{"html", `<html>proxy details</html>`, ""},
		{"wrong-type", `{"error":{},"message":"secret"}`, ""},
		{"oversized", `{"message":"` + strings.Repeat("x", 1<<20) + `"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.NewReader(tc.body)
			err := httpError(&http.Response{StatusCode: 503, Body: io.NopCloser(body)})
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != 503 || httpErr.Message != tc.message {
				t.Fatalf("unexpected HTTP error: %T %v", err, err)
			}
			want := "turso: HTTP status 503"
			if tc.message != "" {
				want += ": " + tc.message
			}
			if err.Error() != want {
				t.Fatalf("got %q, want %q", err, want)
			}
			if consumed := len(tc.body) - body.Len(); consumed > 1<<20 {
				t.Fatalf("read %d bytes, exceeding body limit", consumed)
			}
		})
	}
}

func TestHTTPErrorPublicAPIs(t *testing.T) {
	for _, status := range []int{301, 401, 403, 500, 502, 503} {
		for _, operation := range []string{"open-retained", "ping", "describe", "sequence", "exec", "query", "execute", "sql-ping"} {
			t.Run(fmt.Sprintf("%d/%s", status, operation), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("X-API-Key") != "test-secret" {
						t.Error("missing custom authentication header")
					}
					wantPath := "/v3/cursor"
					if operation == "open-retained" || operation == "describe" || operation == "sequence" {
						wantPath = "/v3/pipeline"
					}
					if r.URL.Path != wantPath {
						t.Errorf("path = %s, want %s", r.URL.Path, wantPath)
					}
					w.WriteHeader(status)
					fmt.Fprint(w, `{"message":"request rejected"}`)
				}))
				defer server.Close()
				connector := NewConnector(server.URL, "").WithRequestHeaders(http.Header{"X-API-Key": {"test-secret"}})
				ctx := context.Background()
				var err error
				if operation == "open-retained" {
					_, err = connector.WithSessionRetention().OpenConnection(ctx)
				} else if operation == "sql-ping" {
					db := sql.OpenDB(connector)
					defer db.Close()
					err = db.PingContext(ctx)
				} else {
					conn, openErr := connector.OpenConnection(ctx)
					if openErr != nil {
						t.Fatal(openErr)
					}
					defer conn.Close()
					switch operation {
					case "ping":
						err = conn.Ping(ctx)
					case "describe":
						_, err = conn.DescribeContext(ctx, "SELECT 1")
					case "sequence":
						_, err = conn.ExecContext(ctx, "SELECT 1; SELECT 2", nil)
					case "exec":
						_, err = conn.ExecContext(ctx, "SELECT 1", nil)
					case "query":
						_, err = conn.QueryContext(ctx, "SELECT 1", nil)
					case "execute":
						_, err = conn.ExecuteContext(ctx, "SELECT 1", nil)
					}
				}
				var httpErr *HTTPError
				if !errors.As(fmt.Errorf("preflight: %w", err), &httpErr) || httpErr.StatusCode != status || httpErr.Message != "request rejected" {
					t.Fatalf("unexpected HTTP error: %T %v", err, err)
				}
				var protocolErr *Error
				if errors.As(err, &protocolErr) {
					t.Fatal("HTTP failure classified as a protocol error")
				}
			})
		}
	}
}

func TestTransportFailureIsNotHTTPError(t *testing.T) {
	for _, retained := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		connector := NewConnector("http://unused", "")
		var err error
		if retained {
			_, err = connector.WithSessionRetention().OpenConnection(ctx)
		} else {
			conn, openErr := connector.OpenConnection(ctx)
			if openErr != nil {
				t.Fatal(openErr)
			}
			err = conn.Ping(ctx)
			conn.Close()
		}
		var httpErr *HTTPError
		if errors.As(err, &httpErr) || !errors.Is(err, context.Canceled) {
			t.Fatalf("transport error lost its cause: %T %v", err, err)
		}
	}
}

func TestHTTPErrorBodyReadFailure(t *testing.T) {
	err := httpError(&http.Response{StatusCode: 401, Body: io.NopCloser(failingHTTPBody{})})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 401 || httpErr.Message != "" {
		t.Fatalf("lost HTTP status after body read failure: %T %v", err, err)
	}
}

type failingHTTPBody struct{}

func (failingHTTPBody) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}
