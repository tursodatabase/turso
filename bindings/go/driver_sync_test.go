package turso

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type TursoServer struct {
	AdminUrl string
	UserUrl  string
}

func randomString() string {
	return fmt.Sprintf("r-%v", rand.Intn(1000_000_000))
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

func (s *TursoServer) CreateTenant(tenant string) error {
	return handleResponse(http.Post(fmt.Sprintf("%v/v1/tenants/%v", s.AdminUrl, tenant), "application/json", nil))
}

func (s *TursoServer) CreateGroup(tenant, group string) error {
	return handleResponse(http.Post(fmt.Sprintf("%v/v1/tenants/%v/groups/%v", s.AdminUrl, tenant, group), "application/json", nil))
}

func (s *TursoServer) CreateDb(tenant, group, db string) error {
	return handleResponse(http.Post(fmt.Sprintf("%v/v1/tenants/%v/groups/%v/databases/%v", s.AdminUrl, tenant, group, db), "application/json", nil))
}

func (s *TursoServer) DbUrl(tenant, group, db string) string {
	tokens := strings.Split(s.UserUrl, "://")
	return fmt.Sprintf("%v://%v--%v--%v.%v", tokens[0], db, tenant, group, tokens[1])
}

func (s *TursoServer) DbSql(tenant, group, db string, sql string) ([][]any, error) {
	data := map[string]any{
		"requests": []map[string]any{
			{"type": "execute", "stmt": map[string]any{"sql": sql}},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequest("POST", fmt.Sprintf("%v/v2/pipeline", s.UserUrl), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	r.Host = fmt.Sprintf("%v--%v--%v.localhost", tenant, group, db)

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

func TestSyncBootstrap(t *testing.T) {
	server := TursoServer{AdminUrl: "http://localhost:8081", UserUrl: "http://localhost:8080"}
	name := randomString()
	require.Nil(t, server.CreateTenant(name))
	require.Nil(t, server.CreateGroup(name, name))
	require.Nil(t, server.CreateDb(name, name, name))
	_, err := server.DbSql(name, name, name, "CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql(name, name, name, "INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl(name, name, name),
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
	server := TursoServer{AdminUrl: "http://localhost:8081", UserUrl: "http://localhost:8080"}
	name := randomString()
	require.Nil(t, server.CreateTenant(name))
	require.Nil(t, server.CreateGroup(name, name))
	require.Nil(t, server.CreateDb(name, name, name))
	_, err := server.DbSql(name, name, name, "CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql(name, name, name, "INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl(name, name, name),
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	_, err = server.DbSql(name, name, name, "INSERT INTO t VALUES ('pull-works')")
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

	require.Nil(t, db.Pull(context.Background()))

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

func TestSyncPush(t *testing.T) {
	server := TursoServer{AdminUrl: "http://localhost:8081", UserUrl: "http://localhost:8080"}
	name := randomString()
	require.Nil(t, server.CreateTenant(name))
	require.Nil(t, server.CreateGroup(name, name))
	require.Nil(t, server.CreateDb(name, name, name))
	_, err := server.DbSql(name, name, name, "CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql(name, name, name, "INSERT INTO t VALUES ('hello'), ('turso'), ('sync-go')")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl(name, name, name),
	})
	require.Nil(t, err)
	conn, err := db.Connect(context.Background())
	require.Nil(t, err)

	_, err = conn.Exec("INSERT INTO t VALUES ('push-works')")
	require.Nil(t, err)

	rows, err := server.DbSql(name, name, name, "SELECT * FROM t")
	require.Nil(t, err)
	require.Equal(t, rows, [][]any{{"hello"}, {"turso"}, {"sync-go"}})

	require.Nil(t, db.Push(context.Background()))

	rows, err = server.DbSql(name, name, name, "SELECT * FROM t")
	require.Nil(t, err)
	require.Equal(t, rows, [][]any{{"hello"}, {"turso"}, {"sync-go"}, {"push-works"}})
}

func TestSyncCheckpoint(t *testing.T) {
	server := TursoServer{AdminUrl: "http://localhost:8081", UserUrl: "http://localhost:8080"}
	name := randomString()
	require.Nil(t, server.CreateTenant(name))
	require.Nil(t, server.CreateGroup(name, name))
	require.Nil(t, server.CreateDb(name, name, name))

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:       ":memory:",
		ClientName: "turso-sync-go",
		RemoteUrl:  server.DbUrl(name, name, name),
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

	rows, err := server.DbSql(name, name, name, "SELECT SUM(x) FROM t")
	require.Nil(t, err)
	require.Equal(t, rows, [][]any{{fmt.Sprintf("%v", 1024*1023/2)}})
}

func TestSyncPartial(t *testing.T) {
	server := TursoServer{AdminUrl: "http://localhost:8081", UserUrl: "http://localhost:8080"}
	name := randomString()
	require.Nil(t, server.CreateTenant(name))
	require.Nil(t, server.CreateGroup(name, name))
	require.Nil(t, server.CreateDb(name, name, name))
	_, err := server.DbSql(name, name, name, "CREATE TABLE t(x)")
	require.Nil(t, err)
	_, err = server.DbSql(name, name, name, "INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 1024)")
	require.Nil(t, err)

	db, err := NewTursoSyncDb(context.Background(), TursoSyncDbConfig{
		Path:                          ":memory:",
		ClientName:                    "turso-sync-go",
		RemoteUrl:                     server.DbUrl(name, name, name),
		PartialBoostrapStrategyPrefix: 128 * 1024,
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
