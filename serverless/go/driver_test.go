// Integration tests for the serverless driver against a live server.
//
// Configure with TURSO_DATABASE_URL and (optionally) TURSO_AUTH_TOKEN; the
// tests that need a server skip themselves when TURSO_DATABASE_URL is
// unset. Tests that exercise only local code run unconditionally.

package turso

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TURSO_DATABASE_URL")
	if url == "" {
		t.Skip("TURSO_DATABASE_URL is not set")
	}
	db := sql.OpenDB(NewConnector(url, os.Getenv("TURSO_AUTH_TOKEN")))
	t.Cleanup(func() { db.Close() })
	return db
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

func TestQuerySingleValue(t *testing.T) {
	db := openDB(t)

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

	rows, err := db.Query("SELECT 1 AS one, 'two' AS two, 0.5 AS three")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cols, []string{"one", "two", "three"}) {
		t.Fatalf("columns: got %v", cols)
	}

	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var one int64
	var two string
	var three float64
	if err := rows.Scan(&one, &two, &three); err != nil {
		t.Fatal(err)
	}
	if one != 1 || two != "two" || three != 0.5 {
		t.Fatalf("got (%d, %q, %v)", one, two, three)
	}
}

func TestQueryMultipleRows(t *testing.T) {
	db := openDB(t)

	rows, err := db.Query("VALUES (1, 'one'), (2, 'two'), (3, 'three')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	want := []struct {
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
		if i >= len(want) || n != want[i].n || s != want[i].s {
			t.Fatalf("row %d: got (%d, %q)", i, n, s)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(want) {
		t.Fatalf("got %d rows, want %d", i, len(want))
	}
}

func TestQueryErrorOnInvalidSQL(t *testing.T) {
	db := openDB(t)

	if _, err := db.Query("SELECT foobar"); err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestInsertReturning(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_ret")
	mustExec(t, db, "CREATE TABLE t_go_ret (a)")

	var x int64
	var y string
	err := db.QueryRow("INSERT INTO t_go_ret VALUES (1) RETURNING 42 AS x, 'foo' AS y").Scan(&x, &y)
	if err != nil {
		t.Fatal(err)
	}
	if x != 42 || y != "foo" {
		t.Fatalf("got (%d, %q)", x, y)
	}
}

// ---------------------------------------------------------------------------
// Exec results
// ---------------------------------------------------------------------------

func TestRowsAffected(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_aff")
	mustExec(t, db, "CREATE TABLE t_go_aff (a)")

	result := mustExec(t, db, "INSERT INTO t_go_aff VALUES (1), (2), (3), (4), (5)")
	if n, _ := result.RowsAffected(); n != 5 {
		t.Fatalf("insert: got %d rows affected, want 5", n)
	}

	result = mustExec(t, db, "DELETE FROM t_go_aff WHERE a >= 3")
	if n, _ := result.RowsAffected(); n != 3 {
		t.Fatalf("delete: got %d rows affected, want 3", n)
	}
}

func TestLastInsertId(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_rowid")
	mustExec(t, db, "CREATE TABLE t_go_rowid (id INTEGER PRIMARY KEY, a)")

	result := mustExec(t, db, "INSERT INTO t_go_rowid VALUES (7, 'x')")
	if id, _ := result.LastInsertId(); id != 7 {
		t.Fatalf("got rowid %d, want 7", id)
	}
}

// ---------------------------------------------------------------------------
// Value roundtrip
// ---------------------------------------------------------------------------

func TestValueRoundtrip(t *testing.T) {
	db := openDB(t)

	t.Run("text", func(t *testing.T) {
		var val string
		if err := db.QueryRow("SELECT ?", "žluťoučký kůň").Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != "žluťoučký kůň" {
			t.Fatalf("got %q", val)
		}
	})
	t.Run("integer", func(t *testing.T) {
		var val int64
		if err := db.QueryRow("SELECT ?", int64(-2023)).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != -2023 {
			t.Fatalf("got %d", val)
		}
	})
	t.Run("large integer", func(t *testing.T) {
		var val int64
		if err := db.QueryRow("SELECT ?", int64(math.MaxInt64)).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != math.MaxInt64 {
			t.Fatalf("got %d", val)
		}
	})
	t.Run("float", func(t *testing.T) {
		var val float64
		if err := db.QueryRow("SELECT ?", 12.345).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != 12.345 {
			t.Fatalf("got %v", val)
		}
	})
	t.Run("null", func(t *testing.T) {
		var val sql.NullString
		if err := db.QueryRow("SELECT NULL").Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val.Valid {
			t.Fatal("expected null")
		}
	})
	t.Run("bool", func(t *testing.T) {
		var vtrue, vfalse int64
		if err := db.QueryRow("SELECT ?, ?", true, false).Scan(&vtrue, &vfalse); err != nil {
			t.Fatal(err)
		}
		if vtrue != 1 || vfalse != 0 {
			t.Fatalf("got (%d, %d), want (1, 0)", vtrue, vfalse)
		}
	})
	t.Run("blob", func(t *testing.T) {
		blob := make([]byte, 256)
		for i := range blob {
			blob[i] = byte(i) ^ 0xab
		}
		var val []byte
		if err := db.QueryRow("SELECT ?", blob).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(val, blob) {
			t.Fatalf("blob mismatch: got %x", val)
		}
	})
	t.Run("nan binds as null", func(t *testing.T) {
		var val sql.NullFloat64
		if err := db.QueryRow("SELECT ?", math.NaN()).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val.Valid {
			t.Fatalf("expected NULL, got %v", val.Float64)
		}
	})
	t.Run("infinity is rejected", func(t *testing.T) {
		if err := db.QueryRow("SELECT ?", math.Inf(1)).Scan(new(float64)); err == nil {
			t.Fatal("expected error for infinite float")
		}
	})
	t.Run("non-finite result decodes as NaN", func(t *testing.T) {
		var val float64
		if err := db.QueryRow("SELECT 1e308 * 10").Scan(&val); err != nil {
			t.Fatal(err)
		}
		if !math.IsNaN(val) && !math.IsInf(val, 1) {
			t.Fatalf("got %v, want a non-finite float", val)
		}
	})
}

func TestTimeRoundtrip(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_time")
	mustExec(t, db, "CREATE TABLE t_go_time (ts DATETIME)")

	want := time.Date(2026, 7, 27, 12, 34, 56, 0, time.UTC)
	mustExec(t, db, "INSERT INTO t_go_time VALUES (?)", want)

	var got time.Time
	if err := db.QueryRow("SELECT ts FROM t_go_time").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

func TestParametersPositional(t *testing.T) {
	db := openDB(t)

	var a, b string
	if err := db.QueryRow("SELECT ?, ?", "one", "two").Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if a != "one" || b != "two" {
		t.Fatalf("got (%q, %q)", a, b)
	}
}

func TestParametersNamed(t *testing.T) {
	db := openDB(t)

	var a, b string
	err := db.QueryRow("SELECT :a, :b", sql.Named("a", "one"), sql.Named("b", "two")).Scan(&a, &b)
	if err != nil {
		t.Fatal(err)
	}
	if a != "one" || b != "two" {
		t.Fatalf("got (%q, %q)", a, b)
	}
}

// ---------------------------------------------------------------------------
// Multi-statement exec
// ---------------------------------------------------------------------------

func TestMultiStatementExec(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, `
		DROP TABLE IF EXISTS t_go_batch;
		CREATE TABLE t_go_batch (a);
		INSERT INTO t_go_batch VALUES (1), (2), (4), (8);
	`)

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

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

func TestTransactionCommit(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_tx_commit")
	mustExec(t, db, "CREATE TABLE t_go_tx_commit (a)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t_go_tx_commit VALUES ('one')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t_go_tx_commit VALUES ('two')"); err != nil {
		t.Fatal(err)
	}
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

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_tx_rb")
	mustExec(t, db, "CREATE TABLE t_go_tx_rb (a)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t_go_tx_rb VALUES ('one')"); err != nil {
		t.Fatal(err)
	}
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

func TestTransactionQueryInside(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_tx_q")
	mustExec(t, db, "CREATE TABLE t_go_tx_q (a)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t_go_tx_q VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
	// Uncommitted writes are visible inside the transaction.
	var count int64
	if err := tx.QueryRow("SELECT COUNT(*) FROM t_go_tx_q").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("got count %d inside transaction, want 2", count)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionErrorInsideKeepsStream(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_tx_err")
	mustExec(t, db, "CREATE TABLE t_go_tx_err (a)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t_go_tx_err VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	// A failed statement does not abort the transaction; later statements
	// and the commit still apply.
	if _, err := tx.Exec("INSERT INTO t_go_tx_err VALUES (foo())"); err == nil {
		t.Fatal("expected error from invalid statement")
	}
	if _, err := tx.Exec("INSERT INTO t_go_tx_err VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM t_go_tx_err").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("got count %d, want 2", count)
	}
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestErrorRecoveryAfterError(t *testing.T) {
	db := openDB(t)

	if _, err := db.Query("SELECT foobar"); err == nil {
		t.Fatal("expected error")
	}

	var val int64
	if err := db.QueryRow("SELECT 42").Scan(&val); err != nil {
		t.Fatalf("connection not usable after error: %v", err)
	}
	if val != 42 {
		t.Fatalf("got %d, want 42", val)
	}
}

func TestErrorConstraintViolation(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_uq")
	mustExec(t, db, "CREATE TABLE t_go_uq (id INTEGER, name TEXT UNIQUE)")
	mustExec(t, db, "INSERT INTO t_go_uq VALUES (1, 'unique_name')")

	_, err := db.Exec("INSERT INTO t_go_uq VALUES (2, 'unique_name')")
	if err == nil {
		t.Fatal("expected UNIQUE constraint error")
	}
	var serr *Error
	if !errors.As(err, &serr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if serr.Code != "" && !strings.HasPrefix(serr.Code, "SQLITE_CONSTRAINT") {
		t.Fatalf("got code %q, want SQLITE_CONSTRAINT*", serr.Code)
	}
	if serr.Code == "" && !strings.Contains(strings.ToUpper(serr.Message), "UNIQUE") {
		t.Fatalf("got message %q, want a UNIQUE constraint message", serr.Message)
	}
}

// ---------------------------------------------------------------------------
// database/sql compliance
// ---------------------------------------------------------------------------

func TestPing(t *testing.T) {
	db := openDB(t)

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestScanTypes(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_scan")
	mustExec(t, db, "CREATE TABLE t_go_scan (i INTEGER, f REAL, t TEXT, b BLOB)")
	mustExec(t, db, "INSERT INTO t_go_scan VALUES (42, 3.14, 'hello', X'deadbeef')")

	var i int64
	var f float64
	var s string
	var b []byte
	if err := db.QueryRow("SELECT i, f, t, b FROM t_go_scan").Scan(&i, &f, &s, &b); err != nil {
		t.Fatal(err)
	}
	if i != 42 || f != 3.14 || s != "hello" {
		t.Fatalf("got (%d, %v, %q)", i, f, s)
	}
	if !reflect.DeepEqual(b, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("blob: got %x, want deadbeef", b)
	}
}

func TestPrepareAndQuery(t *testing.T) {
	db := openDB(t)

	mustExec(t, db, "DROP TABLE IF EXISTS t_go_prep")
	mustExec(t, db, "CREATE TABLE t_go_prep (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO t_go_prep VALUES (1, 'Alice'), (2, 'Bob')")

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

func TestPrepareWrongArgCount(t *testing.T) {
	db := openDB(t)

	stmt, err := db.Prepare("SELECT ?, ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// NumInput comes from the describe request, so database/sql rejects
	// the mismatch client-side.
	if _, err := stmt.Query("only-one"); err == nil {
		t.Fatal("expected argument count mismatch error")
	}
}

func TestOpenDSN(t *testing.T) {
	url := os.Getenv("TURSO_DATABASE_URL")
	if url == "" {
		t.Skip("TURSO_DATABASE_URL is not set")
	}
	dsn := url
	if token := os.Getenv("TURSO_AUTH_TOKEN"); token != "" {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		dsn += sep + "auth_token=" + token
	}
	db, err := sql.Open("turso-serverless", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var val int64
	if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val != 1 {
		t.Fatalf("got %d, want 1", val)
	}
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) sql.Result {
	t.Helper()
	result, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result
}

// ---------------------------------------------------------------------------
// URL normalization and DSN parsing (no server needed)
// ---------------------------------------------------------------------------

func TestNormalizeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"libsql://db.turso.io", "https://db.turso.io"},
		{"turso://db.turso.io", "https://db.turso.io"},
		{"https://db.turso.io", "https://db.turso.io"},
		{"http://localhost:8080", "http://localhost:8080"},
		{"https://db.turso.io/", "https://db.turso.io"},
		{"libsql://db.turso.io/", "https://db.turso.io"},
		{"turso://db.turso.io/", "https://db.turso.io"},
	}
	for _, c := range cases {
		if got := normalizeURL(c.in); got != c.want {
			t.Errorf("normalizeURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseDSN(t *testing.T) {
	cases := []struct {
		dsn       string
		wantURL   string
		wantToken string
		wantKey   string
	}{
		{"http://localhost:8080", "http://localhost:8080", "", ""},
		{"http://localhost:8080?auth_token=mytoken", "http://localhost:8080", "mytoken", ""},
		{"turso://my-db.turso.io?auth_token=xyz", "https://my-db.turso.io", "xyz", ""},
		{"libsql://my-db.turso.io?auth_token=abc", "https://my-db.turso.io", "abc", ""},
		{"turso://my-db.turso.io:443?auth_token=tok", "https://my-db.turso.io:443", "tok", ""},
		{"https://my-db.turso.io/v1/db", "https://my-db.turso.io/v1/db", "", ""},
		{"http://localhost:8080?auth_token=tok&other=val", "http://localhost:8080?other=val", "tok", ""},
		{"http://localhost:8080?auth_token=", "http://localhost:8080", "", ""},
		{"turso://my-db.turso.io?remote_encryption_key=c2VjcmV0", "https://my-db.turso.io", "", "c2VjcmV0"},
		{"turso://my-db.turso.io?auth_token=tok&remote_encryption_key=a%2Bb%2Fc%3D", "https://my-db.turso.io", "tok", "a+b/c="},
		{"http://localhost:8080?remote_encryption_key=", "http://localhost:8080", "", ""},
	}
	for _, c := range cases {
		url, token, key, err := parseDSN(c.dsn)
		if err != nil {
			t.Errorf("parseDSN(%q): %v", c.dsn, err)
			continue
		}
		if url != c.wantURL || token != c.wantToken || key != c.wantKey {
			t.Errorf("parseDSN(%q) = (%q, %q, %q), want (%q, %q, %q)",
				c.dsn, url, token, key, c.wantURL, c.wantToken, c.wantKey)
		}
	}
}

func TestParseDSNEmpty(t *testing.T) {
	if _, _, _, err := parseDSN(""); err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestParseDSNErrorRedactsSecrets(t *testing.T) {
	dsn := "turso://my-db.turso.io\x7f?auth_token=SECRETTOKEN&remote_encryption_key=SECRETKEY"
	_, _, _, err := parseDSN(dsn)
	if err == nil {
		t.Fatal("expected error for DSN with control character")
	}
	for _, secret := range []string{"SECRETTOKEN", "SECRETKEY"} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error message leaks %s: %q", secret, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Value encoding and decoding (no server needed)
// ---------------------------------------------------------------------------

func TestEncodeValue(t *testing.T) {
	cases := []struct {
		in   driver.Value
		want string
	}{
		{nil, `{"type":"null"}`},
		{int64(42), `{"type":"integer","value":"42"}`},
		{1.5, `{"type":"float","value":1.5}`},
		{math.NaN(), `{"type":"null"}`},
		{true, `{"type":"integer","value":"1"}`},
		{false, `{"type":"integer","value":"0"}`},
		{"hello", `{"type":"text","value":"hello"}`},
		{[]byte{0xde, 0xad}, `{"type":"blob","base64":"3q0="}`},
	}
	for _, c := range cases {
		pv, err := encodeValue(c.in)
		if err != nil {
			t.Errorf("encodeValue(%v): %v", c.in, err)
			continue
		}
		got, _ := json.Marshal(pv)
		if string(got) != c.want {
			t.Errorf("encodeValue(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestEncodeValueInfinity(t *testing.T) {
	for _, v := range []float64{math.Inf(1), math.Inf(-1)} {
		if _, err := encodeValue(v); err == nil {
			t.Errorf("encodeValue(%v): expected error", v)
		}
	}
}

func TestDecodeValue(t *testing.T) {
	cases := []struct {
		in   string
		want any
	}{
		{`{"type":"null"}`, nil},
		{`{"type":"integer","value":"42"}`, int64(42)},
		{`{"type":"integer","value":"-9223372036854775808"}`, int64(math.MinInt64)},
		{`{"type":"integer","value":7}`, int64(7)},
		{`{"type":"float","value":1.5}`, 1.5},
		{`{"type":"text","value":"hi"}`, "hi"},
		{`{"type":"blob","base64":"3q2+7w=="}`, []byte{0xde, 0xad, 0xbe, 0xef}},
		{`{"type":"blob","base64":"3q2+7w"}`, []byte{0xde, 0xad, 0xbe, 0xef}},
	}
	for _, c := range cases {
		var pv protoValue
		if err := json.Unmarshal([]byte(c.in), &pv); err != nil {
			t.Fatal(err)
		}
		got, err := decodeValue(pv)
		if err != nil {
			t.Errorf("decodeValue(%s): %v", c.in, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("decodeValue(%s) = %v (%T), want %v (%T)", c.in, got, got, c.want, c.want)
		}
	}
}

func TestDecodeValueNullFloatIsNaN(t *testing.T) {
	var pv protoValue
	if err := json.Unmarshal([]byte(`{"type":"float","value":null}`), &pv); err != nil {
		t.Fatal(err)
	}
	got, err := decodeValue(pv)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := got.(float64)
	if !ok || !math.IsNaN(f) {
		t.Fatalf("got %v (%T), want NaN", got, got)
	}
}

func TestDecodeValueErrors(t *testing.T) {
	cases := []string{
		`{"type":"integer","value":"not-a-number"}`,
		`{"type":"blob"}`,
		`{"type":"mystery"}`,
	}
	for _, c := range cases {
		var pv protoValue
		if err := json.Unmarshal([]byte(c), &pv); err != nil {
			t.Fatal(err)
		}
		if _, err := decodeValue(pv); err == nil {
			t.Errorf("decodeValue(%s): expected error", c)
		}
	}
}

func TestParseRowid(t *testing.T) {
	cases := []struct {
		in   string
		want *int64
	}{
		{`null`, nil},
		{`"42"`, ptr(int64(42))},
		{`42`, ptr(int64(42))},
		{`"9223372036854775807"`, ptr(int64(math.MaxInt64))},
		{`9223372036854775807`, ptr(int64(math.MaxInt64))},
	}
	for _, c := range cases {
		got, err := parseRowid(json.RawMessage(c.in))
		if err != nil {
			t.Errorf("parseRowid(%s): %v", c.in, err)
			continue
		}
		if (got == nil) != (c.want == nil) || (got != nil && *got != *c.want) {
			t.Errorf("parseRowid(%s) = %v, want %v", c.in, got, c.want)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"SELECT 1", []string{"SELECT 1"}},
		{"SELECT 1;", []string{"SELECT 1"}},
		{"SELECT 1; SELECT 2", []string{"SELECT 1", "SELECT 2"}},
		{"SELECT 'a;b'; SELECT 2", []string{"SELECT 'a;b'", "SELECT 2"}},
		{`SELECT "a;b"`, []string{`SELECT "a;b"`}},
	}
	for _, c := range cases {
		if got := splitStatements(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitStatements(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
