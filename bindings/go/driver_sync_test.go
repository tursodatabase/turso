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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
