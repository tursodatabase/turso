package turso

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Busy Timeout DSN Parsing Tests (no server required) ---

func TestSyncDSNParsing(t *testing.T) {
	tests := []struct {
		name            string
		dsn             string
		expectedPath    string
		expectedTimeout int
	}{
		{
			name:            "simple path",
			dsn:             "mydb.db",
			expectedPath:    "mydb.db",
			expectedTimeout: 0,
		},
		{
			name:            "path with busy timeout",
			dsn:             "mydb.db?_busy_timeout=10000",
			expectedPath:    "mydb.db",
			expectedTimeout: 10000,
		},
		{
			name:            "path with negative busy timeout",
			dsn:             "mydb.db?_busy_timeout=-1",
			expectedPath:    "mydb.db",
			expectedTimeout: -1,
		},
		{
			name:            "memory db with timeout",
			dsn:             ":memory:?_busy_timeout=5000",
			expectedPath:    ":memory:",
			expectedTimeout: 5000,
		},
		{
			name:            "path with other options",
			dsn:             "/path/to/db.db?other=value&_busy_timeout=3000",
			expectedPath:    "/path/to/db.db",
			expectedTimeout: 3000,
		},
		{
			name:            "path with zero timeout",
			dsn:             "test.db?_busy_timeout=0",
			expectedPath:    "test.db",
			expectedTimeout: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, opts := parseSyncDSN(tt.dsn)
			require.Equal(t, tt.expectedPath, path)
			require.Equal(t, tt.expectedTimeout, opts.BusyTimeout)
		})
	}
}

func TestSyncBusyTimeoutConfigPrecedence(t *testing.T) {
	// Test that explicit BusyTimeout in config takes precedence over DSN
	t.Run("config overrides DSN", func(t *testing.T) {
		// This test verifies the logic without actually creating a database
		config := TursoSyncDbConfig{
			Path:        "test.db?_busy_timeout=1000",
			BusyTimeout: 5000, // Explicit config should win
		}

		// Parse DSN like NewTursoSyncDb does
		_, dsnOpts := parseSyncDSN(config.Path)
		busyTimeout := config.BusyTimeout
		if busyTimeout == 0 {
			if dsnOpts.BusyTimeout != 0 {
				busyTimeout = dsnOpts.BusyTimeout
			}
		}

		// Explicit config should take precedence
		require.Equal(t, 5000, busyTimeout)
	})

	t.Run("DSN used when config not set", func(t *testing.T) {
		config := TursoSyncDbConfig{
			Path:        "test.db?_busy_timeout=3000",
			BusyTimeout: 0, // Not set
		}

		_, dsnOpts := parseSyncDSN(config.Path)
		busyTimeout := config.BusyTimeout
		if busyTimeout == 0 {
			if dsnOpts.BusyTimeout != 0 {
				busyTimeout = dsnOpts.BusyTimeout
			}
		}

		// DSN timeout should be used
		require.Equal(t, 3000, busyTimeout)
	})

	t.Run("default used when neither set", func(t *testing.T) {
		config := TursoSyncDbConfig{
			Path:        "test.db",
			BusyTimeout: 0,
		}

		_, dsnOpts := parseSyncDSN(config.Path)
		busyTimeout := config.BusyTimeout
		if busyTimeout == 0 {
			if dsnOpts.BusyTimeout != 0 {
				busyTimeout = dsnOpts.BusyTimeout
			}
		}

		// Should be 0 at this point, default applied in Connect()
		require.Equal(t, 0, busyTimeout)
	})
}

func randomString() string {
	return fmt.Sprintf("r-%v", rand.Intn(1000_000_000))
}

var (
	AdminUrl = "http://localhost:8081"
	UserUrl  = "http://localhost:8080"
)

type TursoServer struct {
	DbUrl   string
	userUrl string
	host    string
	server  *os.Process
}

func NewTursoServer() (*TursoServer, error) {
	if localSyncServer, ok := os.LookupEnv("LOCAL_SYNC_SERVER"); ok {
		port := 10_000 + rand.Intn(65536-10_000)
		server, err := os.StartProcess(
			localSyncServer,
			[]string{localSyncServer, "--sync-server", fmt.Sprintf("0.0.0.0:%v", port)},
			&os.ProcAttr{Files: []*os.File{
				os.Stdin,
				os.Stdout,
				os.Stderr,
			}},
		)
		if err != nil {
			return nil, err
		}
		turso := &TursoServer{
			userUrl: fmt.Sprintf("http://localhost:%v", port),
			DbUrl:   fmt.Sprintf("http://localhost:%v", port),
			host:    "",
			server:  server,
		}
		for {
			_, err := http.Get(turso.userUrl)
			if err == nil {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		return turso, nil
	} else {
		name := randomString()
		err := handleResponse(http.Post(fmt.Sprintf("%v/v1/tenants/%v", AdminUrl, name), "application/json", nil))
		if err != nil {
			return nil, err
		}
		err = handleResponse(http.Post(fmt.Sprintf("%v/v1/tenants/%v/groups/%v", AdminUrl, name, name), "application/json", nil))
		if err != nil {
			return nil, err
		}
		err = handleResponse(http.Post(fmt.Sprintf("%v/v1/tenants/%v/groups/%v/databases/%v", AdminUrl, name, name, name), "application/json", nil))
		if err != nil {
			return nil, err
		}
		userUrl := strings.Split(UserUrl, "://")
		turso := &TursoServer{
			userUrl: UserUrl,
			DbUrl:   fmt.Sprintf("%v://%v--%v--%v.%v", userUrl[0], name, name, name, userUrl[1]),
			host:    fmt.Sprintf("%v--%v--%v.localhost", name, name, name),
		}
		return turso, nil
	}
}

func (s *TursoServer) Close() {
	if s.server != nil {
		s.server.Kill()
		s.server.Wait()
	}
}

func handleResponse(response *http.Response, err error) error {
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	text := string(body)
	if response.StatusCode == 200 || response.StatusCode == 400 && strings.Contains(text, "already exists") {
		return nil
	}
	return fmt.Errorf("http failed: %v %v", response.StatusCode, text)
}

func (s *TursoServer) DbSql(sql string) ([][]any, error) {
	data := map[string]any{
		"requests": []map[string]any{
			{"type": "execute", "stmt": map[string]any{"sql": sql}},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequest("POST", fmt.Sprintf("%v/v2/pipeline", s.userUrl), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	r.Host = s.host

	response, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("bad response: %v", response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	if result["results"].([]any)[0].(map[string]any)["type"] != "ok" {
		return nil, fmt.Errorf("bad response: %+v", result)
	}

	inner := result["results"].([]any)[0].(map[string]any)["response"].(map[string]any)["result"].(map[string]any)["rows"].([]any)
	rows := make([][]any, 0)
	for _, innerRow := range inner {
		row := make([]any, 0)
		for _, cell := range innerRow.([]any) {
			row = append(row, cell.(map[string]any)["value"])
		}
		rows = append(rows, row)
	}
	return rows, nil
}

var (
	SYNC_TEST_RUN = os.Getenv("SYNC_TEST_RUN") == "true"
)

func TestSyncBootstrap(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	_, err = server.DbSql("CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)
	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values := make([]string, 0)
	for rows.Next() {
		var value string
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []string{"hello", "turso", "sync-go"})
}

func TestSyncConfigPersistence(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	_, err = server.DbSql("CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql("INSERT INTO t VALUES (42)")
	require.Nil(t, err)

	dir := t.TempDir()
	local := path.Join(dir, "local.db")

	db1, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       local,
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
	})
	require.Nil(t, err)
	conn, err := db1.Connect(context.Background())
	require.Nil(t, err)
	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values := make([]int64, 0)
	for rows.Next() {
		var value int64
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []int64{42})
	rows.Close()
	conn.Close()

	_, err = server.DbSql("INSERT INTO t VALUES (41)")
	require.Nil(t, err)

	db2, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:      local,
		RemoteUrl: server.DbUrl,
	})
	require.Nil(t, err)

	_, err = db2.Pull(context.Background())
	require.Nil(t, err)

	conn, err = db2.Connect(context.Background())
	require.Nil(t, err)
	rows, err = conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values = make([]int64, 0)
	for rows.Next() {
		var value int64
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []int64{42, 41})
}

func TestSyncBootstrapPersistent(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	_, err = server.DbSql("CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	dir, err := os.MkdirTemp(".", "test-sync-")
	require.Nil(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:      filepath.Join(dir, "local.db"),
		RemoteUrl: server.DbUrl,
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)
	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values := make([]string, 0)
	for rows.Next() {
		var value string
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []string{"hello", "turso", "sync-go"})
}

func TestSyncPull(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	_, err = server.DbSql("CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	_, err = server.DbSql("INSERT INTO t VALUES ('pull-works')")
	require.Nil(t, err)

	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values := make([]string, 0)
	for rows.Next() {
		var value string
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []string{"hello", "turso", "sync-go"})
	rows.Close()

	changes, err := db.Pull(context.Background())
	require.Nil(t, err)
	require.True(t, changes)

	changes, err = db.Pull(context.Background())
	require.Nil(t, err)
	require.False(t, changes)

	rows, err = conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values = make([]string, 0)
	for rows.Next() {
		var value string
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []string{"hello", "turso", "sync-go", "pull-works"})
	rows.Close()
}

func TestSyncPullDoNotPush(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	_, err = server.DbSql("CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	_, err = server.DbSql("INSERT INTO t VALUES ('pull-works')")
	require.Nil(t, err)

	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values := make([]string, 0)
	for rows.Next() {
		var value string
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []string{"hello", "turso", "sync-go"})
	rows.Close()

	_, err = conn.Exec("INSERT INTO t VALUES ('push-is-local')")
	require.Nil(t, err)

	changes, err := db.Pull(context.Background())
	require.Nil(t, err)
	require.True(t, changes)

	changes, err = db.Pull(context.Background())
	require.Nil(t, err)
	require.False(t, changes)

	rows, err = conn.QueryContext(context.Background(), "SELECT * FROM t")
	require.Nil(t, err)
	values = make([]string, 0)
	for rows.Next() {
		var value string
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []string{"hello", "turso", "sync-go", "pull-works", "push-is-local"})
	rows.Close()

	remote, err := server.DbSql("SELECT * FROM t")
	require.Nil(t, err)
	require.Equal(t, remote, [][]any{{"hello"}, {"turso"}, {"sync-go"}, {"pull-works"}})
}

func TestSyncPush(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	_, err = server.DbSql("CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	_, err = conn.Exec("INSERT INTO t VALUES ('push-works')")
	require.Nil(t, err)

	rows, err := server.DbSql("SELECT * FROM t")
	require.Nil(t, err)
	require.Equal(t, rows, [][]any{{"hello"}, {"turso"}, {"sync-go"}})

	require.Nil(t, db.Push(context.Background()))

	rows, err = server.DbSql("SELECT * FROM t")
	require.Nil(t, err)
	require.Equal(t, rows, [][]any{{"hello"}, {"turso"}, {"sync-go"}, {"push-works"}})
}

func TestSyncCheckpoint(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	_, err = conn.Exec("CREATE TABLE t(x)")
	require.Nil(t, err)

	for i := 0; i < 1024; i++ {
		_, err = conn.Exec(fmt.Sprintf("INSERT INTO t VALUES (%v)", i))
		require.Nil(t, err)
	}

	stats1, err := db.Stats(context.Background())
	require.Nil(t, err)
	require.Nil(t, db.Checkpoint(context.Background()))
	stats2, err := db.Stats(context.Background())
	require.Nil(t, err)

	require.Greater(t, stats1.MainWalSize, int64(1024*1024))
	require.Equal(t, stats1.RevertWalSize, int64(0))

	require.Equal(t, stats2.MainWalSize, int64(0))
	require.Less(t, stats2.RevertWalSize, int64(8*1024))

	require.Nil(t, db.Push(context.Background()))

	rows, err := server.DbSql("SELECT SUM(x) FROM t")
	require.Nil(t, err)
	require.Equal(t, rows, [][]any{{fmt.Sprintf("%v", 1024*1023/2)}})
}

func TestSyncPartial(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	_, err = server.DbSql("CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 1024)")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
		PartialSyncExperimental: TursoPartialSyncConfig{
			BootstrapStrategyPrefix: 128 * 1024,
		},
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	rows, err := conn.QueryContext(context.Background(), "SELECT LENGTH(x) FROM t LIMIT 1")
	require.Nil(t, err)
	values := make([]int, 0)
	for rows.Next() {
		var value int
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []int{1024})
	rows.Close()

	stats1, err := db.Stats(context.Background())
	require.Nil(t, err)
	require.Less(t, stats1.NetworkReceivedBytes, int64(256*1024))

	rows, err = conn.QueryContext(context.Background(), "SELECT SUM(LENGTH(x)) FROM t")
	require.Nil(t, err)
	values = make([]int, 0)
	for rows.Next() {
		var value int
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []int{1024 * 1024})
	rows.Close()

	stats2, err := db.Stats(context.Background())
	require.Nil(t, err)
	require.Greater(t, stats2.NetworkReceivedBytes, int64(1024*1024))
}

// TestSyncLargeSchema tests syncing a database where the schema table spans multiple pages.
// This reproduces a bug where make_from_btree does blocking IO during database opening,
// but the caller (sync engine) has no way to spin the external IO loop to fetch
// overflow pages that aren't loaded yet.
func TestSyncLargeSchema(t *testing.T) {
	server, err := NewTursoServer()
	require.Nil(t, err)
	t.Cleanup(func() { server.Close() })

	// Create many tables with long column definitions to make sqlite_schema span multiple pages.
	// Each CREATE TABLE statement will be stored in the schema table.
	// SQLite page usable space is ~4000 bytes, so creating tables with ~500 byte definitions
	// means we need ~8+ tables to overflow to another page.
	numTables := 50
	for i := 0; i < numTables; i++ {
		// Create a table with many columns to make a long CREATE statement (~1KB each)
		columns := ""
		for j := 0; j < 20; j++ {
			if j > 0 {
				columns += ", "
			}
			columns += fmt.Sprintf("column_%d_%d_with_a_very_long_name_to_increase_size INTEGER DEFAULT 0", i, j)
		}
		sql := fmt.Sprintf("CREATE TABLE large_schema_table_%d (%s)", i, columns)
		_, err = server.DbSql(sql)
		require.Nil(t, err)
	}

	// Insert some data into the first table
	_, err = server.DbSql("INSERT INTO large_schema_table_0 (column_0_0_with_a_very_long_name_to_increase_size) VALUES (42)")
	require.Nil(t, err)

	// Use partial sync with a small bootstrap prefix to ensure not all schema pages are fetched upfront.
	// The bug manifests when opening the database: make_from_btree needs to read the full schema,
	// but overflow pages for the schema table aren't loaded yet, and blocking IO can't be driven
	// by the sync engine's coroutine-based IO loop.
	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl,
		PartialSyncExperimental: TursoPartialSyncConfig{
			// Use a small prefix to ensure we don't fetch all schema pages upfront
			BootstrapStrategyPrefix: 8 * 1024,
		},
	})
	require.Nil(t, err)

	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	// Verify we can query one of the tables created with the large schema
	rows, err := conn.QueryContext(context.Background(), "SELECT column_0_0_with_a_very_long_name_to_increase_size FROM large_schema_table_0")
	require.Nil(t, err)
	values := make([]int, 0)
	for rows.Next() {
		var value int
		require.Nil(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.Equal(t, values, []int{42})
	rows.Close()

	// Also verify that all tables are accessible in the schema
	rows, err = conn.QueryContext(context.Background(), "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'large_schema_table_%'")
	require.Nil(t, err)
	var count int
	require.True(t, rows.Next())
	require.Nil(t, rows.Scan(&count))
	require.Equal(t, count, numTables)
	rows.Close()
}
