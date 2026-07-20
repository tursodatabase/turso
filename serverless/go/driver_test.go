package tursogo_serverless

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
)

func serverURL() string {
	if v := os.Getenv("TURSO_DATABASE_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("turso-serverless", serverURL())
	if err != nil {
		t.Skipf("cannot open: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("server not reachable: %v", err)
	}
	return db
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

func TestQuerySingleValue(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var val int64
	if err := db.QueryRow("SELECT 42").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Fatalf("got %d, want 42", val)
	}
}

func TestQuerySingleRow(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var one int64
	var two string
	var three float64
	err := db.QueryRow("SELECT 1 AS one, 'two' AS two, 0.5 AS three").Scan(&one, &two, &three)
	if err != nil {
		t.Fatal(err)
	}
	if one != 1 || two != "two" || three != 0.5 {
		t.Fatalf("got (%d, %s, %f), want (1, two, 0.5)", one, two, three)
	}
}

func TestQueryMultipleRows(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("VALUES (1, 'one'), (2, 'two'), (3, 'three')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expected := []struct {
		n int64
		s string
	}{{1, "one"}, {2, "two"}, {3, "three"}}

	i := 0
	for rows.Next() {
		var n int64
		var s string
		if err := rows.Scan(&n, &s); err != nil {
			t.Fatal(err)
		}
		if i >= len(expected) {
			t.Fatal("too many rows")
		}
		if n != expected[i].n || s != expected[i].s {
			t.Fatalf("row %d: got (%d, %s), want (%d, %s)", i, n, s, expected[i].n, expected[i].s)
		}
		i++
	}
	if i != len(expected) {
		t.Fatalf("got %d rows, want %d", i, len(expected))
	}
}

func TestQueryErrorOnInvalidSQL(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Query("SELECT foobar")
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestInsertReturning(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_ret")
	db.Exec("CREATE TABLE t_go_ret (a)")

	var x int64
	var y string
	err := db.QueryRow("INSERT INTO t_go_ret VALUES (1) RETURNING 42 AS x, 'foo' AS y").Scan(&x, &y)
	if err != nil {
		t.Fatal(err)
	}
	if x != 42 || y != "foo" {
		t.Fatalf("got (%d, %s), want (42, foo)", x, y)
	}
}

// ---------------------------------------------------------------------------
// Rows affected
// ---------------------------------------------------------------------------

func TestRowsAffectedInsert(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_ins")
	db.Exec("CREATE TABLE t_go_ins (a)")

	result, err := db.Exec("INSERT INTO t_go_ins VALUES (1), (2)")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 2 {
		t.Fatalf("got %d rows affected, want 2", affected)
	}
}

func TestRowsAffectedDelete(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_del")
	db.Exec("CREATE TABLE t_go_del (a)")
	db.Exec("INSERT INTO t_go_del VALUES (1), (2), (3), (4), (5)")

	result, err := db.Exec("DELETE FROM t_go_del WHERE a >= 3")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 3 {
		t.Fatalf("got %d rows affected, want 3", affected)
	}
}

// ---------------------------------------------------------------------------
// Value roundtrip
// ---------------------------------------------------------------------------

func TestValueRoundtripString(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var val string
	if err := db.QueryRow("SELECT ?", "boomerang").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != "boomerang" {
		t.Fatalf("got %q, want %q", val, "boomerang")
	}
}

func TestValueRoundtripUnicode(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	unicode := "žluťoučký kůň úpěl ďábelské ódy"
	var val string
	if err := db.QueryRow("SELECT ?", unicode).Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != unicode {
		t.Fatalf("got %q, want %q", val, unicode)
	}
}

func TestValueRoundtripInteger(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var val int64
	if err := db.QueryRow("SELECT ?", int64(-2023)).Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != -2023 {
		t.Fatalf("got %d, want -2023", val)
	}
}

func TestValueRoundtripFloat(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var val float64
	if err := db.QueryRow("SELECT ?", 12.345).Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != 12.345 {
		t.Fatalf("got %f, want 12.345", val)
	}
}

func TestValueRoundtripNull(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var val sql.NullString
	if err := db.QueryRow("SELECT NULL").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val.Valid {
		t.Fatal("expected null")
	}
}

func TestValueRoundtripBoolTrue(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var val int64
	if err := db.QueryRow("SELECT ?", true).Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != 1 {
		t.Fatalf("got %d, want 1", val)
	}
}

func TestValueRoundtripBoolFalse(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var val int64
	if err := db.QueryRow("SELECT ?", false).Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != 0 {
		t.Fatalf("got %d, want 0", val)
	}
}

func TestValueRoundtripBlob(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	blob := make([]byte, 256)
	for i := range blob {
		blob[i] = byte(i) ^ 0xab
	}

	var val []byte
	if err := db.QueryRow("SELECT ?", blob).Scan(&val); err != nil {
		t.Fatal(err)
	}
	if len(val) != len(blob) {
		t.Fatalf("got blob len %d, want %d", len(val), len(blob))
	}
	for i := range blob {
		if val[i] != blob[i] {
			t.Fatalf("blob mismatch at index %d: got %d, want %d", i, val[i], blob[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

func TestParametersPositional(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var a, b string
	err := db.QueryRow("SELECT ?, ?", "one", "two").Scan(&a, &b)
	if err != nil {
		t.Fatal(err)
	}
	if a != "one" || b != "two" {
		t.Fatalf("got (%s, %s), want (one, two)", a, b)
	}
}

func TestParametersNamed(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var a, b string
	err := db.QueryRow("SELECT :a, :b",
		sql.Named("a", "one"),
		sql.Named("b", "two"),
	).Scan(&a, &b)
	if err != nil {
		t.Fatal(err)
	}
	if a != "one" || b != "two" {
		t.Fatalf("got (%s, %s), want (one, two)", a, b)
	}
}

// ---------------------------------------------------------------------------
// Multi-statement exec
// ---------------------------------------------------------------------------

func TestMultiStatementExec(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(`
		DROP TABLE IF EXISTS t_go_batch;
		CREATE TABLE t_go_batch (a);
		INSERT INTO t_go_batch VALUES (1), (2), (4), (8);
	`)
	if err != nil {
		t.Fatal(err)
	}

	var sum int64
	if err := db.QueryRow("SELECT SUM(a) FROM t_go_batch").Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if sum != 15 {
		t.Fatalf("got sum %d, want 15", sum)
	}
}

func TestMultiStatementErrorStops(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(`
		DROP TABLE IF EXISTS t_go_batch_err;
		CREATE TABLE t_go_batch_err (a);
		INSERT INTO t_go_batch_err VALUES (1), (2), (4);
		INSERT INTO t_go_batch_err VALUES (foo());
		INSERT INTO t_go_batch_err VALUES (8), (16);
	`)
	if err == nil {
		t.Fatal("expected error from invalid statement")
	}

	var sum int64
	if err := db.QueryRow("SELECT SUM(a) FROM t_go_batch_err").Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if sum != 7 {
		t.Fatalf("got sum %d, want 7", sum)
	}
}

func TestMultiStatementManualTransaction(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(`
		DROP TABLE IF EXISTS t_go_batch_tx;
		CREATE TABLE t_go_batch_tx (a);
		BEGIN;
		INSERT INTO t_go_batch_tx VALUES (1), (2), (4);
		INSERT INTO t_go_batch_tx VALUES (8), (16);
		COMMIT;
	`)
	if err != nil {
		t.Fatal(err)
	}

	var sum int64
	if err := db.QueryRow("SELECT SUM(a) FROM t_go_batch_tx").Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if sum != 31 {
		t.Fatalf("got sum %d, want 31", sum)
	}
}

// ---------------------------------------------------------------------------
// Transaction (db.Begin)
// ---------------------------------------------------------------------------

func TestTransactionCommit(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_tx_commit")
	db.Exec("CREATE TABLE t_go_tx_commit (a)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	tx.Exec("INSERT INTO t_go_tx_commit VALUES ('one')")
	tx.Exec("INSERT INTO t_go_tx_commit VALUES ('two')")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM t_go_tx_commit").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("got count %d, want 2", count)
	}
}

func TestTransactionRollback(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_tx_rb")
	db.Exec("CREATE TABLE t_go_tx_rb (a)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	tx.Exec("INSERT INTO t_go_tx_rb VALUES ('one')")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM t_go_tx_rb").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("got count %d, want 0", count)
	}
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestErrorNonexistentTable(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Query("SELECT * FROM nonexistent_table_go")
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

func TestErrorRecoveryAfterError(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Query("SELECT foobar")
	if err == nil {
		t.Fatal("expected error")
	}

	// Connection should still be usable
	var val int64
	if err := db.QueryRow("SELECT 42").Scan(&val); err != nil {
		t.Fatalf("connection not usable after error: %v", err)
	}
	if val != 42 {
		t.Fatalf("got %d, want 42", val)
	}
}

func TestErrorPKConstraint(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_pk_err")
	db.Exec("CREATE TABLE t_go_pk_err (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO t_go_pk_err VALUES (1, 'first')")

	_, err := db.Exec("INSERT INTO t_go_pk_err VALUES (1, 'duplicate')")
	if err == nil {
		t.Fatal("expected PK constraint error")
	}
}

func TestErrorUniqueConstraint(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_uq_err")
	db.Exec("CREATE TABLE t_go_uq_err (id INTEGER, name TEXT UNIQUE)")
	db.Exec("INSERT INTO t_go_uq_err VALUES (1, 'unique_name')")

	_, err := db.Exec("INSERT INTO t_go_uq_err VALUES (2, 'unique_name')")
	if err == nil {
		t.Fatal("expected UNIQUE constraint error")
	}
}

// ---------------------------------------------------------------------------
// database/sql compliance
// ---------------------------------------------------------------------------

func TestPing(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestScanTypes(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_scan")
	db.Exec("CREATE TABLE t_go_scan (i INTEGER, f REAL, t TEXT, b BLOB)")
	db.Exec("INSERT INTO t_go_scan VALUES (42, 3.14, 'hello', X'deadbeef')")

	var i int64
	var f float64
	var s string
	var b []byte
	if err := db.QueryRow("SELECT i, f, t, b FROM t_go_scan").Scan(&i, &f, &s, &b); err != nil {
		t.Fatal(err)
	}
	if i != 42 {
		t.Fatalf("int: got %d, want 42", i)
	}
	if f != 3.14 {
		t.Fatalf("float: got %f, want 3.14", f)
	}
	if s != "hello" {
		t.Fatalf("text: got %q, want %q", s, "hello")
	}
	if len(b) != 4 || b[0] != 0xde || b[1] != 0xad || b[2] != 0xbe || b[3] != 0xef {
		t.Fatalf("blob: got %x, want deadbeef", b)
	}
}

func TestPrepareAndQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS t_go_prep")
	db.Exec("CREATE TABLE t_go_prep (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO t_go_prep VALUES (1, 'Alice'), (2, 'Bob')")

	stmt, err := db.Prepare("SELECT name FROM t_go_prep WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	var name string
	if err := stmt.QueryRow(int64(1)).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Alice" {
		t.Fatalf("got %q, want Alice", name)
	}
}

func TestRowsIteration(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("VALUES (1), (2), (3)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("got %d rows, want 3", count)
	}
}

func TestCloseConnection(t *testing.T) {
	db := openDB(t)

	var val int64
	if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// DSN parsing
// ---------------------------------------------------------------------------

func TestDSNParseHTTP(t *testing.T) {
	u, token, err := parseDSN("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	if u != "http://localhost:8080" {
		t.Fatalf("url: got %q", u)
	}
	if token != "" {
		t.Fatalf("token: got %q, want empty", token)
	}
}

func TestDSNParseWithAuthToken(t *testing.T) {
	u, token, err := parseDSN("http://localhost:8080?auth_token=mytoken")
	if err != nil {
		t.Fatal(err)
	}
	if u != "http://localhost:8080" {
		t.Fatalf("url: got %q", u)
	}
	if token != "mytoken" {
		t.Fatalf("token: got %q, want mytoken", token)
	}
}

func TestDSNParseTursoScheme(t *testing.T) {
	u, token, err := parseDSN("turso://my-db.turso.io?auth_token=xyz")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://my-db.turso.io" {
		t.Fatalf("url: got %q", u)
	}
	if token != "xyz" {
		t.Fatalf("token: got %q, want xyz", token)
	}
}

func TestDSNParseLibsqlScheme(t *testing.T) {
	u, token, err := parseDSN("libsql://my-db.turso.io?auth_token=abc")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://my-db.turso.io" {
		t.Fatalf("url: got %q", u)
	}
	if token != "abc" {
		t.Fatalf("token: got %q, want abc", token)
	}
}

func TestDSNParseHTTPS(t *testing.T) {
	u, token, err := parseDSN("https://my-db.turso.io")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://my-db.turso.io" {
		t.Fatalf("url: got %q", u)
	}
	if token != "" {
		t.Fatalf("token: got %q, want empty", token)
	}
}

func TestDSNParseTursoSchemeWithPort(t *testing.T) {
	u, token, err := parseDSN("turso://my-db.turso.io:443?auth_token=tok")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://my-db.turso.io:443" {
		t.Fatalf("url: got %q", u)
	}
	if token != "tok" {
		t.Fatalf("token: got %q, want tok", token)
	}
}

func TestDSNParseLibsqlSchemeWithPort(t *testing.T) {
	u, token, err := parseDSN("libsql://my-db.turso.io:8080")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://my-db.turso.io:8080" {
		t.Fatalf("url: got %q", u)
	}
	if token != "" {
		t.Fatalf("token: got %q, want empty", token)
	}
}

func TestDSNParseWithPath(t *testing.T) {
	u, token, err := parseDSN("https://my-db.turso.io/v1/db")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://my-db.turso.io/v1/db" {
		t.Fatalf("url: got %q", u)
	}
	if token != "" {
		t.Fatalf("token: got %q, want empty", token)
	}
}

func TestDSNParseAuthTokenWithOtherParams(t *testing.T) {
	u, token, err := parseDSN("http://localhost:8080?auth_token=tok&other=val")
	if err != nil {
		t.Fatal(err)
	}
	if u != "http://localhost:8080?other=val" {
		t.Fatalf("url: got %q", u)
	}
	if token != "tok" {
		t.Fatalf("token: got %q, want tok", token)
	}
}

func TestDSNParseEmptyAuthToken(t *testing.T) {
	u, token, err := parseDSN("http://localhost:8080?auth_token=")
	if err != nil {
		t.Fatal(err)
	}
	if u != "http://localhost:8080" {
		t.Fatalf("url: got %q", u)
	}
	if token != "" {
		t.Fatalf("token: got %q, want empty", token)
	}
}

func TestDSNParseEmptyDSN(t *testing.T) {
	_, _, err := parseDSN("")
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestDSNParseWSPassthrough(t *testing.T) {
	u, token, err := parseDSN("ws://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	if u != "ws://localhost:8080" {
		t.Fatalf("url: got %q", u)
	}
	if token != "" {
		t.Fatalf("token: got %q, want empty", token)
	}
}

func TestDSNParseWSSPassthrough(t *testing.T) {
	u, token, err := parseDSN("wss://my-db.turso.io")
	if err != nil {
		t.Fatal(err)
	}
	if u != "wss://my-db.turso.io" {
		t.Fatalf("url: got %q", u)
	}
	if token != "" {
		t.Fatalf("token: got %q, want empty", token)
	}
}

func TestNormalizeURLLibsql(t *testing.T) {
	if got := normalizeURL("libsql://db.turso.io"); got != "https://db.turso.io" {
		t.Fatalf("got %q", got)
	}
}

func TestNormalizeURLTurso(t *testing.T) {
	if got := normalizeURL("turso://db.turso.io"); got != "https://db.turso.io" {
		t.Fatalf("got %q", got)
	}
}

func TestNormalizeURLHTTPSPassthrough(t *testing.T) {
	if got := normalizeURL("https://db.turso.io"); got != "https://db.turso.io" {
		t.Fatalf("got %q", got)
	}
}

func TestNormalizeURLHTTPPassthrough(t *testing.T) {
	if got := normalizeURL("http://localhost:8080"); got != "http://localhost:8080" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Constructor API
// ---------------------------------------------------------------------------

func TestNewConnectorConnect(t *testing.T) {
	connector, err := NewConnector(serverURL())
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("server not reachable: %v", err)
	}

	var val int64
	if err := db.QueryRow("SELECT 42").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Fatalf("got %d, want 42", val)
	}
}

func TestConnector(t *testing.T) {
	connector, err := NewConnector(serverURL())
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("server not reachable: %v", err)
	}

	var val int64
	if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != 1 {
		t.Fatalf("got %d, want 1", val)
	}
}

// ---------------------------------------------------------------------------
// Server-authoritative transaction state + connector options
// ---------------------------------------------------------------------------

func TestInTransactionReflectsServerState(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if inTx, err := InTransaction(conn); err != nil || inTx {
		t.Fatalf("fresh connection should not be in a transaction (inTx=%v err=%v)", inTx, err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if inTx, _ := InTransaction(conn); !inTx {
		t.Fatal("expected in transaction after BEGIN")
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if inTx, _ := InTransaction(conn); inTx {
		t.Fatal("expected not in transaction after COMMIT")
	}
}

func TestConnectorWithOptions(t *testing.T) {
	connector, err := NewConnector(serverURL(),
		WithQueryTimeout(10000),
		WithRequestHeaders(map[string]string{"X-Turso-Test": "1"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("server not reachable: %v", err)
	}
	var v int64
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("got %d, want 1", v)
	}
}

func TestNewConnectorRejectsHostHeaderOption(t *testing.T) {
	if _, err := NewConnector(serverURL(), WithRequestHeaders(map[string]string{"Host": "evil"})); err == nil {
		t.Fatal("expected error rejecting Host header")
	}
}

func TestBatchAtomicRoundTrip(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS t_go_batch")
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t_go_batch (a)"); err != nil {
		t.Fatal(err)
	}
	results, err := Batch(ctx, conn, []Statement{
		{SQL: "INSERT INTO t_go_batch VALUES (1)"},
		{SQL: "INSERT INTO t_go_batch VALUES (2)"},
		{SQL: "SELECT count(*) FROM t_go_batch"},
	}, "immediate")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results", len(results))
	}
	if got := results[2].Rows[0][0].(int64); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	if inTx, _ := InTransaction(conn); inTx {
		t.Fatal("atomic batch should have committed")
	}
}

// TestBatchAtomicRollbackOnFailure asserts that an atomic batch with a failing
// statement rolls the whole thing back (no rows committed) and leaves the
// connection in autocommit.
func TestBatchAtomicRollbackOnFailure(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS t_go_batch_fail")
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t_go_batch_fail (v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	// The third statement fails (wrong arity); the atomic batch must roll the
	// whole thing back via its condition chain.
	_, err = Batch(ctx, conn, []Statement{
		{SQL: "INSERT INTO t_go_batch_fail VALUES (10)"},
		{SQL: "INSERT INTO t_go_batch_fail VALUES (20)"},
		{SQL: "INSERT INTO t_go_batch_fail VALUES (1, 2, 3)"},
		{SQL: "INSERT INTO t_go_batch_fail VALUES (30)"},
	}, "immediate")
	if err == nil {
		t.Fatal("expected the atomic batch to fail")
	}
	var count int64
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM t_go_batch_fail").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (atomic batch must roll back entirely)", count)
	}
	if inTx, _ := InTransaction(conn); inTx {
		t.Fatal("connection should be back in autocommit after a failed atomic batch")
	}
}

// TestBatchRowsMatchQuery asserts that Batch() returns rows in the same
// positional representation a normal query does — [][]any of decoded values,
// index-addressable — so batch and query never diverge in row shape.
//
// We don't compare against the embedded driver here: this module is pure
// database/sql (no native dependency), and database/sql's Scan yields
// driver-independent driver.Value types by contract, so Go rows can't deviate
// across drivers the way JS/Python row shapes can. Full serverless-vs-embedded
// conformance is covered by the hegel harness (testing/hegel/go), which imports
// both drivers.
func TestBatchRowsMatchQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const q = "SELECT 10 AS a, 20 AS b"
	results, err := Batch(ctx, conn, []Statement{{SQL: q}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || len(results[0].Rows) != 1 {
		t.Fatalf("unexpected batch shape: %+v", results)
	}
	batchRow := results[0].Rows[0]

	var a, b int64
	if err := conn.QueryRowContext(ctx, q).Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if len(batchRow) != 2 {
		t.Fatalf("batch row has %d columns, want 2", len(batchRow))
	}
	if batchRow[0].(int64) != a || batchRow[1].(int64) != b {
		t.Fatalf("batch row %v does not match query row [%d %d]", batchRow, a, b)
	}
	if a != 10 || b != 20 {
		t.Fatalf("got %d,%d want 10,20", a, b)
	}
}

// ---------------------------------------------------------------------------
// Transaction() helper (incl. BEGIN CONCURRENT) + batch-in-transaction — JS parity
// ---------------------------------------------------------------------------

// skipIfNoConcurrent skips when the server can't parse BEGIN CONCURRENT (a
// Turso-engine feature; classic libsql-server rejects it).
func skipIfNoConcurrent(t *testing.T, conn *sql.Conn, ctx context.Context) {
	t.Helper()
	if _, err := conn.ExecContext(ctx, "BEGIN CONCURRENT"); err != nil {
		t.Skipf("server does not support BEGIN CONCURRENT: %v", err)
	}
	_, _ = conn.ExecContext(ctx, "ROLLBACK")
}

func TestTransactionHelperConcurrent(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	skipIfNoConcurrent(t, conn, ctx)

	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS t_go_conc")
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t_go_conc (a)"); err != nil {
		t.Fatal(err)
	}
	err = Transaction(ctx, conn, "concurrent", func(ctx context.Context) error {
		_, e := conn.ExecContext(ctx, "INSERT INTO t_go_conc VALUES (1)")
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM t_go_conc").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}
	if inTx, _ := InTransaction(conn); inTx {
		t.Fatal("Transaction helper should have committed")
	}
}

func TestTransactionHelperRollsBackOnError(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS t_go_rb")
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t_go_rb (a)"); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("boom")
	err = Transaction(ctx, conn, "", func(ctx context.Context) error {
		if _, e := conn.ExecContext(ctx, "INSERT INTO t_go_rb VALUES (1)"); e != nil {
			return e
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want boom", err)
	}
	var n int64
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM t_go_rb").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("count = %d, want 0 (rolled back)", n)
	}
}

func TestBatchInsideTransactionReusesOuter(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS t_go_btx")
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t_go_btx (a)"); err != nil {
		t.Fatal(err)
	}
	err = Transaction(ctx, conn, "", func(ctx context.Context) error {
		// a mode-carrying batch inside a transaction must not nest a BEGIN
		_, e := Batch(ctx, conn, []Statement{
			{SQL: "INSERT INTO t_go_btx VALUES (1)"},
			{SQL: "INSERT INTO t_go_btx VALUES (2)"},
		}, "immediate")
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM t_go_btx").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}
}
