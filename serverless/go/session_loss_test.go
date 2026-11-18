package turso

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestBufferedTruncatedCursorInvalidatesConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"baton":"x"}`)
		fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
	}))
	defer server.Close()
	c := newConn(server.URL, "", "")
	defer c.Close()
	if _, err := c.ExecContext(context.Background(), "BEGIN", nil); err == nil {
		t.Fatal("truncated BEGIN was accepted")
	}
	if _, err := c.ExecContext(context.Background(), "INSERT INTO t VALUES(1)", nil); err == nil {
		t.Fatal("continued after uncertain BEGIN")
	}
}

func TestCursorCompletionWithoutReplicationIndex(t *testing.T) {
	entries := []string{
		`{"type":"step_begin","step":0,"cols":[{"name":"?column?","decltype":null}]}`,
		`{"type":"row","row":[{"type":"integer","value":"1"}]}`,
		`{"type":"step_end","affected_row_count":0,"last_insert_rowid":null}`,
		`{"type":"step_begin","step":1,"cols":[{"name":"?column?","decltype":null}]}`,
		`{"type":"step_end","affected_row_count":0,"last_insert_rowid":null}`,
	}
	for count := 0; count <= len(entries); count++ {
		t.Run(fmt.Sprintf("entries=%d", count), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v3/pipeline" {
					(&stubServer{t: t}).handlePipeline(w, r)
					return
				}
				fmt.Fprintln(w, `{"baton":"fixture"}`)
				for _, entry := range entries[:count] {
					fmt.Fprintln(w, entry)
				}
			}))
			defer server.Close()
			c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())
			defer c.Close()
			result, err := c.ExecuteContext(context.Background(), "SELECT 1", nil)
			if count != 3 && count != len(entries) {
				if err == nil || result != nil || c.IsValid() {
					t.Fatalf("accepted incomplete response: %+v, %v", result, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || result.Rows[0][0] != int64(1) || len(result.Columns) != 1 || *result.Columns[0].Name != "?column?" || result.Columns[0].Decltype != nil {
				t.Fatalf("unexpected result: %+v", result)
			}
			if auto, err := c.Autocommit(); err != nil || auto != (count == len(entries)) {
				t.Fatalf("autocommit: %v %v", auto, err)
			}
		})
	}
}

func TestCursorWithoutTerminatorSkipsExtraRequests(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/pipeline" {
			var req pipelineRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				return
			}
			if len(req.Requests) == 1 && req.Requests[0].Type == "close" {
				fmt.Fprintln(w, `{}`)
				return
			}
			if req.Baton == nil || *req.Baton != "transaction" || len(req.Requests) != 1 || req.Requests[0].Type != "get_autocommit" {
				t.Errorf("unexpected probe: %+v", req)
			}
			probes.Add(1)
			fmt.Fprintln(w, `{"baton":"transaction","results":[{"type":"ok","response":{"type":"get_autocommit","is_autocommit":false}}]}`)
			return
		}
		fmt.Fprintln(w, `{"baton":"transaction"}`)
		fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
		fmt.Fprintln(w, `{"type":"step_end","affected_row_count":0}`)
	}))
	defer server.Close()
	c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())
	defer c.Close()
	if _, err := c.ExecContext(context.Background(), "BEGIN", nil); err != nil {
		t.Fatal(err)
	}
	if auto, err := c.Autocommit(); err != nil || auto {
		t.Fatalf("transaction state: %v %v", auto, err)
	}
	if probes.Load() != 0 {
		t.Fatalf("expected no state checks, got %d", probes.Load())
	}
}

func TestAutocommitRefreshFailurePreservesStatementResult(t *testing.T) {
	for _, mode := range []string{"autocommit", "retained", "transaction"} {
		for _, outcome := range []string{"success", "statement-error", "cursor-error"} {
			for _, refreshFailure := range []string{"http", "malformed"} {
				t.Run(mode+"/"+outcome+"/"+refreshFailure, func(t *testing.T) {
					var executions, refreshes atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/v3/pipeline" {
							var req pipelineRequest
							if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
								t.Error(err)
								return
							}
							switch req.Requests[0].Type {
							case "store_sql":
								fmt.Fprintln(w, `{"baton":"retained","results":[{"type":"ok","response":{"type":"store_sql"}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}}]}`)
							case "close":
								fmt.Fprintln(w, `{}`)
							case "get_autocommit":
								refreshes.Add(1)
								if refreshFailure == "http" {
									http.Error(w, "expired", http.StatusNotFound)
								} else {
									fmt.Fprintln(w, `invalid json`)
								}
							default:
								t.Errorf("unexpected pipeline request: %+v", req)
							}
							return
						}
						var req cursorRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
							return
						}
						begin := req.Batch.Steps[0].Stmt.SQL == "BEGIN"
						if begin {
							fmt.Fprintln(w, `{"baton":"transaction"}`)
							fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
							fmt.Fprintln(w, `{"type":"step_end"}`)
							return
						}
						first := executions.Add(1) == 1
						if !first && req.Baton != nil {
							t.Error("recovery reused the failed baton")
						}
						fmt.Fprintln(w, `{"baton":"result"}`)
						if first && outcome == "cursor-error" {
							fmt.Fprintln(w, `{"type":"error","error":{"message":"server overloaded","code":"OVERLOADED"}}`)
							return
						}
						fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
						if first && outcome == "statement-error" {
							fmt.Fprintln(w, `{"type":"step_error","step":0,"error":{"message":"statement failed","code":"SQLITE_CONSTRAINT"}}`)
						} else {
							fmt.Fprintln(w, `{"type":"step_end","affected_row_count":3,"last_insert_rowid":"42"}`)
						}
						fmt.Fprintln(w, `{"type":"step_begin","step":1,"cols":[]}`)
						if first {
							fmt.Fprintln(w, `{"type":"step_error","step":1,"error":{"message":"probe failed"}}`)
						} else {
							fmt.Fprintln(w, `{"type":"step_end"}`)
						}
						fmt.Fprintln(w, `{"type":"replication_index"}`)
					}))
					defer server.Close()
					connector := NewConnector(server.URL, "")
					if mode == "retained" {
						connector.WithSessionRetention()
					}
					c, err := connector.OpenConnection(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer c.Close()
					if mode == "transaction" {
						if _, err := c.ExecContext(context.Background(), "BEGIN", nil); err != nil {
							t.Fatal(err)
						}
					}
					result, err := c.ExecContext(context.Background(), "INSERT INTO t VALUES(1)", nil)
					if outcome == "success" {
						if err != nil {
							t.Fatal(err)
						}
						count, _ := result.RowsAffected()
						id, _ := result.LastInsertId()
						if count != 3 || id != 42 {
							t.Fatalf("lost execution result: %d %d", count, id)
						}
					} else {
						want := "SQLITE_CONSTRAINT"
						if outcome == "cursor-error" {
							want = "OVERLOADED"
						}
						var sqlErr *Error
						if result != nil || !errors.As(err, &sqlErr) || sqlErr.Code != want {
							t.Fatalf("lost statement error: %v", err)
						}
					}
					if executions.Load() != 1 || refreshes.Load() != 1 {
						t.Fatal("statement retried or refresh missing")
					}
					_, err = c.ExecContext(context.Background(), "INSERT INTO t VALUES(2)", nil)
					if mode == "autocommit" && refreshFailure == "http" {
						if err != nil || executions.Load() != 2 || !c.IsValid() {
							t.Fatalf("default connection did not recover: %v", err)
						}
					} else if !errors.Is(err, ErrSessionLost) || executions.Load() != 1 || c.IsValid() {
						t.Fatalf("unusable session recovered: %v", err)
					}
				})
			}
		}
	}
}

func TestHeldConnectionRecoveryAfterHTTPFailure(t *testing.T) {
	for _, mode := range []string{"autocommit", "retained", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v3/pipeline" {
					(&stubServer{t: t}).handlePipeline(w, r)
					return
				}
				var req cursorRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				begin := req.Batch.Steps[0].Stmt.SQL == "BEGIN"
				if !begin {
					if requests.Add(1) == 1 {
						http.Error(w, "temporary failure", 500)
						return
					}
					if req.Baton != nil {
						t.Error("recovery reused the failed stream's baton")
					}
				}
				fmt.Fprintln(w, `{"baton":"session"}`)
				fmt.Fprintln(w, `{"type":"step_begin","step":0,"cols":[]}`)
				fmt.Fprintln(w, `{"type":"step_end","affected_row_count":1}`)
				if !begin {
					fmt.Fprintln(w, `{"type":"step_begin","step":1,"cols":[]}`)
					fmt.Fprintln(w, `{"type":"step_end"}`)
				}
				fmt.Fprintln(w, `{"type":"replication_index"}`)
			}))
			defer server.Close()
			connector := NewConnector(server.URL, "")
			if mode == "retained" {
				connector.WithSessionRetention()
			}
			db := sql.OpenDB(connector)
			defer db.Close()
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if mode == "transaction" {
				if _, err := conn.ExecContext(context.Background(), "BEGIN"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := conn.ExecContext(context.Background(), "INSERT INTO t VALUES(1)"); err == nil {
				t.Fatal("failed request reported success")
			}
			_, err = conn.ExecContext(context.Background(), "INSERT INTO t VALUES(2)")
			if mode == "autocommit" {
				if err != nil || requests.Load() != 2 {
					t.Fatalf("held autocommit connection did not recover: %v (%d requests)", err, requests.Load())
				}
			} else if !errors.Is(err, ErrSessionLost) || requests.Load() != 1 {
				t.Fatalf("stateful connection restarted: %v (%d requests)", err, requests.Load())
			}
		})
	}
}

func TestCursorWithIncompleteHTTPBodyInvalidatesConnection(t *testing.T) {
	const body = "{\"baton\":\"fixture\"}\n{\"type\":\"step_begin\",\"step\":0,\"cols\":[]}\n{\"type\":\"step_end\"}\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)+100))
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	c, _ := NewConnector(server.URL, "").OpenConnection(context.Background())
	defer c.Close()
	if result, err := c.ExecuteContext(context.Background(), "BEGIN", nil); result != nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("accepted cut-off HTTP response: %+v, %v", result, err)
	}
	if c.IsValid() {
		t.Fatal("cut-off response left connection usable")
	}
}
