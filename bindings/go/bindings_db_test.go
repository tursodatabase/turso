package turso

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"
)

// helper to require a loaded library for integration tests
func requireLibLoaded(t *testing.T) {
	t.Helper()
	// If the init() in bindings.go succeeded, at least one of the function pointers should be non-nil.
	// Choose one arbitrarily.
	if c_turso_database_new == nil {
		t.Skip("Turso dynamic library is not loaded; set TURSO_LIB_PATH to the shared library to run integration tests")
	}
}

// helper to open an in-memory database and return db, conn
func openMemoryDB(t *testing.T) (TursoDatabase, TursoConnection) {
	t.Helper()
	requireLibLoaded(t)

	if err := turso_setup(TursoConfig{}); err != nil {
		t.Fatalf("turso_setup failed: %v", err)
	}

	db, err := turso_database_new(TursoDatabaseConfig{
		Path:    ":memory:",
		AsyncIO: false,
	})
	if err != nil {
		t.Fatalf("turso_database_new failed: %v", err)
	}
	if err := turso_database_open(db); err != nil {
		turso_database_deinit(db)
		t.Fatalf("turso_database_open failed: %v", err)
	}

	conn, err := turso_database_connect(db)
	if err != nil {
		turso_database_deinit(db)
		t.Fatalf("turso_database_connect failed: %v", err)
	}

	return db, conn
}

func closeDB(t *testing.T, db TursoDatabase, conn TursoConnection) {
	t.Helper()
	_ = turso_connection_close(conn)
	turso_connection_deinit(conn)
	turso_database_deinit(db)
}

func TestSetupAndOpenMemoryDB(t *testing.T) {
	db, conn := openMemoryDB(t)
	defer closeDB(t, db, conn)

	// Autocommit should be true on a new connection
	if ac := turso_connection_get_autocommit(conn); !ac {
		t.Fatalf("expected autocommit to be true on a new connection")
	}
}

func TestCreateInsertSelectRoundtrip(t *testing.T) {
	db, conn := openMemoryDB(t)
	defer closeDB(t, db, conn)

	// Create table
	stmt, err := turso_connection_prepare_single(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val REAL, data BLOB, n NULL)")
	if err != nil {
		t.Fatalf("prepare create table failed: %v", err)
	}
	if _, err := turso_statement_execute(stmt); err != nil {
		turso_statement_deinit(stmt)
		t.Fatalf("execute create table failed: %v", err)
	}
	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize create table failed: %v", err)
	}
	turso_statement_deinit(stmt)

	// Insert row with all types
	stmt, err = turso_connection_prepare_single(conn, "INSERT INTO t (id, name, val, data, n) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		t.Fatalf("prepare insert failed: %v", err)
	}
	if err := turso_statement_bind_positional_int(stmt, 1, 1); err != nil {
		t.Fatalf("bind int failed: %v", err)
	}
	if err := turso_statement_bind_positional_text(stmt, 2, "alice"); err != nil {
		t.Fatalf("bind text failed: %v", err)
	}
	if err := turso_statement_bind_positional_double(stmt, 3, 3.14); err != nil {
		t.Fatalf("bind double failed: %v", err)
	}
	if err := turso_statement_bind_positional_blob(stmt, 4, []byte{1, 2, 3}); err != nil {
		t.Fatalf("bind blob failed: %v", err)
	}
	if err := turso_statement_bind_positional_null(stmt, 5); err != nil {
		t.Fatalf("bind null failed: %v", err)
	}
	rows, err := turso_statement_execute(stmt)
	if err != nil {
		t.Fatalf("execute insert failed: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected 1 row changed, got %d", rows)
	}
	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize insert failed: %v", err)
	}
	turso_statement_deinit(stmt)

	// Select row and verify values
	stmt, err = turso_connection_prepare_single(conn, "SELECT id, name, val, data, n FROM t WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare select failed: %v", err)
	}
	if err := turso_statement_bind_positional_int(stmt, 1, 1); err != nil {
		t.Fatalf("bind int (select) failed: %v", err)
	}
	status, err := turso_statement_step(stmt)
	if err != nil {
		t.Fatalf("step select failed: %v", err)
	}
	if status != TURSO_ROW {
		t.Fatalf("expected TURSO_ROW, got %v", status)
	}

	if got := turso_statement_column_count(stmt); got != 5 {
		t.Fatalf("expected 5 columns, got %d", got)
	}

	// Check column names are non-empty
	for i := 0; i < 5; i++ {
		name := turso_statement_column_name(stmt, i)
		if name == "" {
			t.Fatalf("column %d name is empty", i)
		}
	}

	// id
	if kind := turso_statement_row_value_kind(stmt, 0); kind != TURSO_TYPE_INTEGER {
		t.Fatalf("id kind expected INTEGER, got %v", kind)
	}
	if id := turso_statement_row_value_int(stmt, 0); id != 1 {
		t.Fatalf("id expected 1, got %d", id)
	}

	// name
	if kind := turso_statement_row_value_kind(stmt, 1); kind != TURSO_TYPE_TEXT {
		t.Fatalf("name kind expected TEXT, got %v", kind)
	}
	if n := turso_statement_row_value_bytes_count(stmt, 1); n != int64(len("alice")) {
		t.Fatalf("name bytes count expected %d, got %d", len("alice"), n)
	}
	{
		n := turso_statement_row_value_bytes_count(stmt, 1)
		ptr := turso_statement_row_value_bytes_ptr(stmt, 1)
		if ptr == nil {
			t.Fatalf("name bytes ptr is 0")
		}
		b := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), n)
		// copy to detach before next step
		name := string(append([]byte{}, b...))
		if name != "alice" {
			t.Fatalf("name expected 'alice', got %q", name)
		}
	}

	// val
	if kind := turso_statement_row_value_kind(stmt, 2); kind != TURSO_TYPE_REAL {
		t.Fatalf("val kind expected REAL, got %v", kind)
	}
	if v := turso_statement_row_value_double(stmt, 2); (v < 3.139) || (v > 3.141) {
		t.Fatalf("val expected ~3.14, got %v", v)
	}

	// data
	if kind := turso_statement_row_value_kind(stmt, 3); kind != TURSO_TYPE_BLOB {
		t.Fatalf("data kind expected BLOB, got %v", kind)
	}
	if n := turso_statement_row_value_bytes_count(stmt, 3); n != 3 {
		t.Fatalf("data bytes count expected 3, got %d", n)
	}
	{
		n := turso_statement_row_value_bytes_count(stmt, 3)
		ptr := turso_statement_row_value_bytes_ptr(stmt, 3)
		if ptr == nil {
			t.Fatalf("data bytes ptr is 0")
		}
		b := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), n)
		got := append([]byte{}, b...)
		if !bytes.Equal(got, []byte{1, 2, 3}) {
			t.Fatalf("data expected [1 2 3], got %v", got)
		}
	}

	// n (NULL)
	if kind := turso_statement_row_value_kind(stmt, 4); kind != TURSO_TYPE_NULL {
		t.Fatalf("n kind expected NULL, got %v", kind)
	}
	if n := turso_statement_row_value_bytes_count(stmt, 4); n != -1 {
		t.Fatalf("n bytes count expected -1, got %d", n)
	}
	if p := turso_statement_row_value_bytes_ptr(stmt, 4); p != nil {
		t.Fatalf("n bytes ptr expected 0, got %v", p)
	}
	if i := turso_statement_row_value_int(stmt, 4); i != 0 {
		t.Fatalf("n int expected 0, got %d", i)
	}
	if d := turso_statement_row_value_double(stmt, 4); d != 0 {
		t.Fatalf("n double expected 0, got %v", d)
	}

	// next step should be DONE
	status, err = turso_statement_step(stmt)
	if err != nil {
		t.Fatalf("second step select failed: %v", err)
	}
	if status != TURSO_DONE {
		t.Fatalf("expected TURSO_DONE, got %v", status)
	}

	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize select failed: %v", err)
	}
	turso_statement_deinit(stmt)
}

func TestPrepareFirstMultipleStatements(t *testing.T) {
	db, conn := openMemoryDB(t)
	defer closeDB(t, db, conn)

	sql := "CREATE TABLE y(x INTEGER); INSERT INTO y(x) VALUES (42); SELECT x FROM y;"

	// First statement: CREATE TABLE
	stmt, tail, err := turso_connection_prepare_first(conn, sql)
	if err != nil {
		t.Fatalf("prepare_first 1 failed: %v", err)
	}
	if stmt == nil {
		t.Fatalf("expected a statement for first part")
	}
	if _, err := turso_statement_execute(stmt); err != nil {
		t.Fatalf("execute create table failed: %v", err)
	}
	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize create failed: %v", err)
	}
	turso_statement_deinit(stmt)

	// Second statement: INSERT
	stmt, tail2, err := turso_connection_prepare_first(conn, sql[tail:])
	if err != nil {
		t.Fatalf("prepare_first 2 failed: %v", err)
	}
	if stmt == nil {
		t.Fatalf("expected a statement for second part")
	}
	// tail2 is relative to the substring
	if tail2 == 0 {
		t.Fatalf("expected non-zero tail for second part")
	}
	rows, err := turso_statement_execute(stmt)
	if err != nil {
		t.Fatalf("execute insert failed: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected 1 row changed from insert, got %d", rows)
	}
	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize insert failed: %v", err)
	}
	turso_statement_deinit(stmt)

	// Third statement: SELECT
	offset := tail + tail2
	stmt, _, err = turso_connection_prepare_first(conn, sql[offset:])
	if err != nil {
		t.Fatalf("prepare_first 3 failed: %v", err)
	}
	if stmt == nil {
		t.Fatalf("expected a statement for third part")
	}
	status, err := turso_statement_step(stmt)
	if err != nil {
		t.Fatalf("step select failed: %v", err)
	}
	if status != TURSO_ROW {
		t.Fatalf("expected TURSO_ROW, got %v", status)
	}
	if c := turso_statement_column_count(stmt); c != 1 {
		t.Fatalf("expected 1 column, got %d", c)
	}
	if kind := turso_statement_row_value_kind(stmt, 0); kind != TURSO_TYPE_INTEGER {
		t.Fatalf("expected INTEGER kind, got %v", kind)
	}
	if x := turso_statement_row_value_int(stmt, 0); x != 42 {
		t.Fatalf("expected x=42, got %d", x)
	}
	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize select failed: %v", err)
	}
	turso_statement_deinit(stmt)
}

func TestNamedPosition(t *testing.T) {
	db, conn := openMemoryDB(t)
	defer closeDB(t, db, conn)

	stmt, err := turso_connection_prepare_single(conn, "SELECT :x + ?")
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	pos := turso_statement_named_position(stmt, "x")
	if pos <= 0 {
		t.Fatalf("expected named position > 0 for :x, got %d", pos)
	}
	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize failed: %v", err)
	}
	turso_statement_deinit(stmt)
}

func TestStatementReset(t *testing.T) {
	db, conn := openMemoryDB(t)
	defer closeDB(t, db, conn)

	// Create table and populate
	{
		stmt, err := turso_connection_prepare_single(conn, "CREATE TABLE z(i INTEGER)")
		if err != nil {
			t.Fatalf("prepare create failed: %v", err)
		}
		if _, err := turso_statement_execute(stmt); err != nil {
			t.Fatalf("execute create failed: %v", err)
		}
		if err := turso_statement_finalize(stmt); err != nil {
			t.Fatalf("finalize create failed: %v", err)
		}
		turso_statement_deinit(stmt)
	}
	{
		stmt, err := turso_connection_prepare_single(conn, "INSERT INTO z(i) VALUES (1), (2)")
		if err != nil {
			t.Fatalf("prepare insert failed: %v", err)
		}
		if _, err := turso_statement_execute(stmt); err != nil {
			t.Fatalf("execute insert failed: %v", err)
		}
		if err := turso_statement_finalize(stmt); err != nil {
			t.Fatalf("finalize insert failed: %v", err)
		}
		turso_statement_deinit(stmt)
	}

	// Select with reset in between
	stmt, err := turso_connection_prepare_single(conn, "SELECT i FROM z ORDER BY i")
	if err != nil {
		t.Fatalf("prepare select failed: %v", err)
	}

	// Step first row
	status, err := turso_statement_step(stmt)
	if err != nil || status != TURSO_ROW {
		t.Fatalf("first step expected ROW, got %v, err=%v", status, err)
	}
	if turso_statement_row_value_int(stmt, 0) != 1 {
		t.Fatalf("expected first row 1")
	}

	// Reset and step again; should start from first row again
	if err := turso_statement_reset(stmt); err != nil {
		t.Fatalf("reset failed: %v", err)
	}
	status, err = turso_statement_step(stmt)
	if err != nil || status != TURSO_ROW {
		t.Fatalf("after reset, expected ROW, got %v, err=%v", status, err)
	}
	if turso_statement_row_value_int(stmt, 0) != 1 {
		t.Fatalf("expected first row 1 after reset")
	}

	if err := turso_statement_finalize(stmt); err != nil {
		t.Fatalf("finalize select failed: %v", err)
	}
	turso_statement_deinit(stmt)
}

func TestStatusErrorMapping(t *testing.T) {
	// Ensure mapping without error message ptr
	tests := []struct {
		code TursoStatusCode
		err  error
	}{
		{TURSO_BUSY, TursoBusyErr},
		{TURSO_INTERRUPT, TursoInterruptErr},
		{TURSO_MISUSE, TursoMisuseErr},
		{TURSO_CONSTRAINT, TursoConstraintErr},
		{TURSO_READONLY, TursoReadonlyErr},
		{TURSO_DATABASE_FULL, TursoDatabaseFullErr},
		{TURSO_NOTADB, TursoNotADbErr},
		{TURSO_CORRUPT, TursoCorruptErr},
		{TURSO_ERROR, TursoErrorErr},
	}
	for _, tt := range tests {
		err := statusToError(tt.code, "")
		if !errors.Is(err, tt.err) {
			t.Fatalf("status %v expected %v, got %v", tt.code, tt.err, err)
		}
	}
}
