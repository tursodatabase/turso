// Shared behavioral tests for the Go turso and turso-serverless database/sql
// drivers.
//
// Set TURSO_DRIVERS to control which drivers are tested:
//   - "turso"              — local only
//   - "turso-serverless"       — remote only
//   - "turso,turso-serverless" — both (default)
//
// For the remote driver, set TURSO_DATABASE_URL (default http://localhost:8080).
package drivers_test

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
	turso "turso.tech/database/tursogo"
	_ "turso.tech/database/tursogo-serverless"
)

// ---------------------------------------------------------------------------
// Library init + connection helpers
// ---------------------------------------------------------------------------

func init() {
	turso.InitLibrary(turso_libs.LoadTursoLibraryConfig{LoadStrategy: "mixed"})
	turso.Setup(turso.TursoConfig{})
}

func openDB(t *testing.T, driver string) *sql.DB {
	t.Helper()
	switch driver {
	case "turso":
		db, err := sql.Open("turso", ":memory:")
		if err != nil {
			t.Fatalf("local driver not available: %v", err)
		}
		if err = db.Ping(); err != nil {
			db.Close()
			t.Fatalf("local driver not usable: %v", err)
		}
		return db
	case "turso-serverless":
		url := os.Getenv("TURSO_DATABASE_URL")
		if url == "" {
			url = "http://localhost:8080"
		}
		db, err := sql.Open("turso-serverless", url)
		if err != nil {
			t.Fatalf("remote driver not available: %v", err)
		}
		if err := db.Ping(); err != nil {
			db.Close()
			t.Fatalf("server not reachable at %s: %v", url, err)
		}
		return db
	default:
		t.Fatalf("unknown driver %q", driver)
		return nil
	}
}

// ---------------------------------------------------------------------------
// Driver helpers
// ---------------------------------------------------------------------------

func drivers(t *testing.T) []string {
	t.Helper()
	env := os.Getenv("TURSO_DRIVERS")
	if env == "" {
		return []string{"turso", "turso-serverless"}
	}
	var out []string
	for _, d := range strings.Split(env, ",") {
		d = strings.TrimSpace(d)
		if d != "" {
			out = append(out, d)
		}
	}
	return out
}

func forEachDriver(t *testing.T, fn func(t *testing.T, db *sql.DB)) {
	for _, drv := range drivers(t) {
		t.Run(drv, func(t *testing.T) {
			db := openDB(t, drv)
			defer db.Close()
			fn(t, db)
		})
	}
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

func TestQuerySingleValue(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val int64
		if err := db.QueryRow("SELECT 42").Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != 42 {
			t.Fatalf("got %d, want 42", val)
		}
	})
}

func TestQuerySingleRow(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
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
	})
}

func TestQueryMultipleRows(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
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
	})
}

func TestQueryErrorOnInvalidSQL(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Query("SELECT foobar")
		if err == nil {
			t.Fatal("expected error for invalid SQL")
		}
	})
}

func TestInsertReturning(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_ret")
		db.Exec("CREATE TABLE t_shared_ret (a)")

		var x int64
		var y string
		err := db.QueryRow("INSERT INTO t_shared_ret VALUES (1) RETURNING 42 AS x, 'foo' AS y").Scan(&x, &y)
		if err != nil {
			t.Fatal(err)
		}
		if x != 42 || y != "foo" {
			t.Fatalf("got (%d, %s), want (42, foo)", x, y)
		}
	})
}

// ---------------------------------------------------------------------------
// Rows affected
// ---------------------------------------------------------------------------

func TestRowsAffectedInsert(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_ins")
		db.Exec("CREATE TABLE t_shared_ins (a)")

		result, err := db.Exec("INSERT INTO t_shared_ins VALUES (1), (2)")
		if err != nil {
			t.Fatal(err)
		}
		affected, _ := result.RowsAffected()
		if affected != 2 {
			t.Fatalf("got %d rows affected, want 2", affected)
		}
	})
}

func TestRowsAffectedDelete(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_del")
		db.Exec("CREATE TABLE t_shared_del (a)")
		db.Exec("INSERT INTO t_shared_del VALUES (1), (2), (3), (4), (5)")

		result, err := db.Exec("DELETE FROM t_shared_del WHERE a >= 3")
		if err != nil {
			t.Fatal(err)
		}
		affected, _ := result.RowsAffected()
		if affected != 3 {
			t.Fatalf("got %d rows affected, want 3", affected)
		}
	})
}

// ---------------------------------------------------------------------------
// Value roundtrip
// ---------------------------------------------------------------------------

func TestValueRoundtripString(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val string
		if err := db.QueryRow("SELECT ?", "boomerang").Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != "boomerang" {
			t.Fatalf("got %q, want %q", val, "boomerang")
		}
	})
}

func TestValueRoundtripUnicode(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		unicode := "žluťoučký kůň úpěl ďábelské ódy"
		var val string
		if err := db.QueryRow("SELECT ?", unicode).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != unicode {
			t.Fatalf("got %q, want %q", val, unicode)
		}
	})
}

func TestValueRoundtripInteger(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val int64
		if err := db.QueryRow("SELECT ?", int64(-2023)).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != -2023 {
			t.Fatalf("got %d, want -2023", val)
		}
	})
}

func TestValueRoundtripFloat(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val float64
		if err := db.QueryRow("SELECT ?", 12.345).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != 12.345 {
			t.Fatalf("got %f, want 12.345", val)
		}
	})
}

func TestValueRoundtripNull(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val sql.NullString
		if err := db.QueryRow("SELECT NULL").Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val.Valid {
			t.Fatal("expected null")
		}
	})
}

func TestValueRoundtripBoolTrue(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val int64
		if err := db.QueryRow("SELECT ?", true).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != 1 {
			t.Fatalf("got %d, want 1", val)
		}
	})
}

func TestValueRoundtripBoolFalse(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val int64
		if err := db.QueryRow("SELECT ?", false).Scan(&val); err != nil {
			t.Fatal(err)
		}
		if val != 0 {
			t.Fatalf("got %d, want 0", val)
		}
	})
}

func TestValueRoundtripBlob(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
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
	})
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

func TestParametersPositional(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var a, b string
		err := db.QueryRow("SELECT ?, ?", "one", "two").Scan(&a, &b)
		if err != nil {
			t.Fatal(err)
		}
		if a != "one" || b != "two" {
			t.Fatalf("got (%s, %s), want (one, two)", a, b)
		}
	})
}

func TestParametersNamed(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
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
	})
}

// ---------------------------------------------------------------------------
// Multi-statement exec
// ---------------------------------------------------------------------------

func TestMultiStatementExec(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec(`
			DROP TABLE IF EXISTS t_shared_batch;
			CREATE TABLE t_shared_batch (a);
			INSERT INTO t_shared_batch VALUES (1), (2), (4), (8);
		`)
		if err != nil {
			t.Fatal(err)
		}

		var sum int64
		if err := db.QueryRow("SELECT SUM(a) FROM t_shared_batch").Scan(&sum); err != nil {
			t.Fatal(err)
		}
		if sum != 15 {
			t.Fatalf("got sum %d, want 15", sum)
		}
	})
}

func TestMultiStatementErrorStops(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec(`
			DROP TABLE IF EXISTS t_shared_batch_err;
			CREATE TABLE t_shared_batch_err (a);
			INSERT INTO t_shared_batch_err VALUES (1), (2), (4);
			INSERT INTO t_shared_batch_err VALUES (foo());
			INSERT INTO t_shared_batch_err VALUES (8), (16);
		`)
		if err == nil {
			t.Fatal("expected error from invalid statement")
		}

		var sum int64
		if err := db.QueryRow("SELECT SUM(a) FROM t_shared_batch_err").Scan(&sum); err != nil {
			t.Fatal(err)
		}
		if sum != 7 {
			t.Fatalf("got sum %d, want 7", sum)
		}
	})
}

func TestMultiStatementManualTransaction(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec(`
			DROP TABLE IF EXISTS t_shared_batch_tx;
			CREATE TABLE t_shared_batch_tx (a);
			BEGIN;
			INSERT INTO t_shared_batch_tx VALUES (1), (2), (4);
			INSERT INTO t_shared_batch_tx VALUES (8), (16);
			COMMIT;
		`)
		if err != nil {
			t.Fatal(err)
		}

		var sum int64
		if err := db.QueryRow("SELECT SUM(a) FROM t_shared_batch_tx").Scan(&sum); err != nil {
			t.Fatal(err)
		}
		if sum != 31 {
			t.Fatalf("got sum %d, want 31", sum)
		}
	})
}

// ---------------------------------------------------------------------------
// Transaction (db.Begin)
// ---------------------------------------------------------------------------

func TestTransactionCommit(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_tx_commit")
		db.Exec("CREATE TABLE t_shared_tx_commit (a)")

		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		tx.Exec("INSERT INTO t_shared_tx_commit VALUES ('one')")
		tx.Exec("INSERT INTO t_shared_tx_commit VALUES ('two')")
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		var count int64
		if err := db.QueryRow("SELECT COUNT(*) FROM t_shared_tx_commit").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("got count %d, want 2", count)
		}
	})
}

func TestTransactionRollback(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_tx_rb")
		db.Exec("CREATE TABLE t_shared_tx_rb (a)")

		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		tx.Exec("INSERT INTO t_shared_tx_rb VALUES ('one')")
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}

		var count int64
		if err := db.QueryRow("SELECT COUNT(*) FROM t_shared_tx_rb").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("got count %d, want 0", count)
		}
	})
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestErrorNonexistentTable(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Query("SELECT * FROM nonexistent_table_shared")
		if err == nil {
			t.Fatal("expected error for nonexistent table")
		}
	})
}

func TestErrorRecoveryAfterError(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Query("SELECT foobar")
		if err == nil {
			t.Fatal("expected error")
		}

		var val int64
		if err := db.QueryRow("SELECT 42").Scan(&val); err != nil {
			t.Fatalf("connection not usable after error: %v", err)
		}
		if val != 42 {
			t.Fatalf("got %d, want 42", val)
		}
	})
}

func TestErrorPKConstraint(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_pk_err")
		db.Exec("CREATE TABLE t_shared_pk_err (id INTEGER PRIMARY KEY, name TEXT)")
		db.Exec("INSERT INTO t_shared_pk_err VALUES (1, 'first')")

		_, err := db.Exec("INSERT INTO t_shared_pk_err VALUES (1, 'duplicate')")
		if err == nil {
			t.Fatal("expected PK constraint error")
		}
	})
}

func TestErrorUniqueConstraint(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_uq_err")
		db.Exec("CREATE TABLE t_shared_uq_err (id INTEGER, name TEXT UNIQUE)")
		db.Exec("INSERT INTO t_shared_uq_err VALUES (1, 'unique_name')")

		_, err := db.Exec("INSERT INTO t_shared_uq_err VALUES (2, 'unique_name')")
		if err == nil {
			t.Fatal("expected UNIQUE constraint error")
		}
	})
}

// ---------------------------------------------------------------------------
// database/sql compliance
// ---------------------------------------------------------------------------

func TestPing(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		if err := db.Ping(); err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
	})
}

func TestScanTypes(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_scan")
		db.Exec("CREATE TABLE t_shared_scan (i INTEGER, f REAL, t TEXT, b BLOB)")
		db.Exec("INSERT INTO t_shared_scan VALUES (42, 3.14, 'hello', X'deadbeef')")

		var i int64
		var f float64
		var s string
		var b []byte
		if err := db.QueryRow("SELECT i, f, t, b FROM t_shared_scan").Scan(&i, &f, &s, &b); err != nil {
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
	})
}

func TestPrepareAndQuery(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		db.Exec("DROP TABLE IF EXISTS t_shared_prep")
		db.Exec("CREATE TABLE t_shared_prep (id INTEGER PRIMARY KEY, name TEXT)")
		db.Exec("INSERT INTO t_shared_prep VALUES (1, 'Alice'), (2, 'Bob')")

		stmt, err := db.Prepare("SELECT name FROM t_shared_prep WHERE id = ?")
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
	})
}

func TestRowsIteration(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
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
	})
}

func TestCloseConnection(t *testing.T) {
	forEachDriver(t, func(t *testing.T, db *sql.DB) {
		var val int64
		if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Fatal(err)
		}
		// db is closed by forEachDriver's defer
	})
}
