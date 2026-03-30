package turso

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dbConn struct {
	db   TursoDatabase
	conn TursoConnection
}

func openInMemory(t *testing.T) (*dbConn, func()) {
	t.Helper()

	db, err := turso_database_new(TursoDatabaseConfig{
		Path:                 ":memory:",
		ExperimentalFeatures: "",
		AsyncIO:              false,
	})
	require.NoError(t, err)
	require.NotNil(t, db)

	require.NoError(t, turso_database_open(db))

	conn, err := turso_database_connect(db)
	require.NoError(t, err)
	require.NotNil(t, conn)

	cleanup := func() {
		_ = turso_connection_close(conn)
		turso_connection_deinit(conn)
		turso_database_deinit(db)
	}
	return &dbConn{db: db, conn: conn}, cleanup
}

func prepExec(t *testing.T, conn TursoConnection, sql string) uint64 {
	t.Helper()
	stmt, err := turso_connection_prepare_single(conn, sql)
	require.NoError(t, err)
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()
	_, changes, err := turso_statement_execute(stmt)
	require.NoError(t, err)
	return changes
}

func prepStmt(t *testing.T, conn TursoConnection, sql string) TursoStatement {
	t.Helper()
	stmt, err := turso_connection_prepare_single(conn, sql)
	require.NoError(t, err)
	return stmt
}

func stepRow(t *testing.T, stmt TursoStatement) bool {
	t.Helper()
	code, err := turso_statement_step(stmt)
	require.NoError(t, err)
	if code == TURSO_ROW {
		return true
	}
	require.Equal(t, TURSO_DONE, code)
	return false
}

func TestSetupAndOpenMemory(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	ac := turso_connection_get_autocommit(conn.conn)
	assert.True(t, ac, "autocommit should be true for new connection")

	// simple sanity DDL
	changes := prepExec(t, conn.conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)")
	assert.Equal(t, uint64(0), changes)
}

func TestPrepareFirstMultipleStatements(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	sql := "CREATE TABLE t(a INT); INSERT INTO t(a) VALUES(1); SELECT a FROM t;"
	start := 0
	for {
		sub := sql[start:]
		stmt, tail, err := turso_connection_prepare_first(conn.conn, sub)
		require.NoError(t, err)
		if stmt == nil {
			break
		}
		if tail <= 0 {
			break
		}

		// Execute or step depending on statement type
		colCount := turso_statement_column_count(stmt)
		if colCount == 0 {
			_, _, err := turso_statement_execute(stmt)
			require.NoError(t, err)
		} else {
			require.True(t, stepRow(t, stmt))
			v := turso_statement_row_value_int(stmt, 0)
			assert.Equal(t, int64(1), v)
			require.False(t, stepRow(t, stmt))
		}
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
		start += tail
	}
	// Verify result
	stmt := prepStmt(t, conn.conn, "SELECT a FROM t")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()
	require.True(t, stepRow(t, stmt))
	assert.Equal(t, int64(1), turso_statement_row_value_int(stmt, 0))
	require.False(t, stepRow(t, stmt))
}

func TestInsertReturningMultiplePartialFetchCommits(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t(a INT)")

	stmt := prepStmt(t, conn.conn, "INSERT INTO t(a) VALUES (1),(2) RETURNING a")
	require.True(t, stepRow(t, stmt))
	first := turso_statement_row_value_int(stmt, 0)
	assert.Equal(t, int64(1), first)

	// Do not consume all rows; finalize early
	_ = turso_statement_finalize(stmt)
	turso_statement_deinit(stmt)

	// Ensure both rows were inserted
	stmt2 := prepStmt(t, conn.conn, "SELECT COUNT(*) FROM t")
	defer func() {
		_ = turso_statement_finalize(stmt2)
		turso_statement_deinit(stmt2)
	}()
	require.True(t, stepRow(t, stmt2))
	cnt := turso_statement_row_value_int(stmt2, 0)
	assert.Equal(t, int64(2), cnt)
	require.False(t, stepRow(t, stmt2))
}

func TestInsertReturningWithExplicitTransactionAndPartialFetch(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t(a INT)")
	prepExec(t, conn.conn, "BEGIN")
	stmt := prepStmt(t, conn.conn, "INSERT INTO t(a) VALUES (10),(20) RETURNING a")
	require.True(t, stepRow(t, stmt))
	v := turso_statement_row_value_int(stmt, 0)
	assert.Equal(t, int64(10), v)
	// finalize without consuming all rows
	_ = turso_statement_finalize(stmt)
	turso_statement_deinit(stmt)
	// Commit should still succeed
	prepExec(t, conn.conn, "COMMIT")

	// Verify data
	stmt2 := prepStmt(t, conn.conn, "SELECT COUNT(*) FROM t")
	defer func() {
		_ = turso_statement_finalize(stmt2)
		turso_statement_deinit(stmt2)
	}()
	require.True(t, stepRow(t, stmt2))
	assert.Equal(t, int64(2), turso_statement_row_value_int(stmt2, 0))
}

func TestOnConflictDoNothingReturning(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t(a INT PRIMARY KEY)")
	prepExec(t, conn.conn, "INSERT INTO t(a) VALUES(1)")

	stmt := prepStmt(t, conn.conn, "INSERT INTO t(a) VALUES(1) ON CONFLICT(a) DO NOTHING RETURNING a")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()
	// Should produce no rows and be done
	code, err := turso_statement_step(stmt)
	require.NoError(t, err)
	assert.Equal(t, TURSO_DONE, code)

	// Ensure count unchanged
	stmt2 := prepStmt(t, conn.conn, "SELECT COUNT(*) FROM t")
	defer func() {
		_ = turso_statement_finalize(stmt2)
		turso_statement_deinit(stmt2)
	}()
	require.True(t, stepRow(t, stmt2))
	assert.Equal(t, int64(1), turso_statement_row_value_int(stmt2, 0))
}

func TestSubqueries(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t(a INT)")
	prepExec(t, conn.conn, "INSERT INTO t(a) VALUES (1),(2),(3),(4)")

	stmt := prepStmt(t, conn.conn, "SELECT a FROM (SELECT a FROM t WHERE a > 1) WHERE a < 4 ORDER BY a")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()

	var got []int64
	for {
		if !stepRow(t, stmt) {
			break
		}
		got = append(got, turso_statement_row_value_int(stmt, 0))
	}
	assert.Equal(t, []int64{2, 3}, got)
}

func TestJoin(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t1(id INT PRIMARY KEY, name TEXT)")
	prepExec(t, conn.conn, "CREATE TABLE t2(id INT PRIMARY KEY, age INT)")
	prepExec(t, conn.conn, "INSERT INTO t1(id, name) VALUES (1,'a'),(2,'b'),(3,'c')")
	prepExec(t, conn.conn, "INSERT INTO t2(id, age) VALUES (1,10),(3,30)")

	stmt := prepStmt(t, conn.conn, "SELECT t1.id, t1.name, t2.age FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()

	var rows [][3]int64
	var names []string
	for {
		if !stepRow(t, stmt) {
			break
		}
		id := turso_statement_row_value_int(stmt, 0)
		// name as TEXT
		name := turso_statement_row_value_text(stmt, 1)
		age := turso_statement_row_value_int(stmt, 2)
		rows = append(rows, [3]int64{id, int64(len(name)), age})
		names = append(names, name)
	}
	assert.Equal(t, [][3]int64{{1, 1, 10}, {3, 1, 30}}, rows)
	assert.Equal(t, []string{"a", "c"}, names)
}

func TestAlterTable(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t(id INT PRIMARY KEY)")
	prepExec(t, conn.conn, "ALTER TABLE t ADD COLUMN name TEXT")
	// Insert with new column present
	prepExec(t, conn.conn, "INSERT INTO t(id, name) VALUES(1, 'hello')")

	stmt := prepStmt(t, conn.conn, "SELECT name FROM t WHERE id = 1")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()
	require.True(t, stepRow(t, stmt))
	assert.Equal(t, "hello", turso_statement_row_value_text(stmt, 0))
	require.False(t, stepRow(t, stmt))
}

func TestGenerateSeries(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	stmt := prepStmt(t, conn.conn, "SELECT value FROM generate_series(1,5)")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()

	var got []int64
	for {
		if !stepRow(t, stmt) {
			break
		}
		got = append(got, turso_statement_row_value_int(stmt, 0))
	}
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, got)
}

func TestJSONFunctionsBindings(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	stmt := prepStmt(t, conn.conn, "SELECT json_extract('{\"x\": [1,2,3]}', '$.x[1]'), json_array_length('[1,2,3]')")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()

	require.True(t, stepRow(t, stmt))
	kind0 := turso_statement_row_value_kind(stmt, 0)
	kind1 := turso_statement_row_value_kind(stmt, 1)
	assert.Equal(t, TURSO_TYPE_INTEGER, kind0)
	assert.Equal(t, TURSO_TYPE_INTEGER, kind1)
	assert.Equal(t, int64(2), turso_statement_row_value_int(stmt, 0))
	assert.Equal(t, int64(3), turso_statement_row_value_int(stmt, 1))
	require.False(t, stepRow(t, stmt))
}

func TestBindingsPositionalAndNamed(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t(i INTEGER, r REAL, s TEXT, b BLOB, n NULL)")

	// Positional parameters: ?1..?
	stmt := prepStmt(t, conn.conn, "INSERT INTO t(i,r,s,b,n) VALUES (?1,?2,?3,?4,?5)")
	require.NoError(t, turso_statement_bind_positional_int(stmt, 1, 42))
	require.NoError(t, turso_statement_bind_positional_double(stmt, 2, 3.14))
	require.NoError(t, turso_statement_bind_positional_text(stmt, 3, "hello"))
	require.NoError(t, turso_statement_bind_positional_blob(stmt, 4, []byte{0xde, 0xad, 0xbe, 0xef}))
	require.NoError(t, turso_statement_bind_positional_null(stmt, 5))
	_, _, err := turso_statement_execute(stmt)
	require.NoError(t, err)
	_ = turso_statement_finalize(stmt)
	turso_statement_deinit(stmt)

	// Named parameters mapped to positional via named_position
	stmt2 := prepStmt(t, conn.conn, "INSERT INTO t(i,r,s,b,n) VALUES (:i,:r,:s,:b,:n)")
	defer func() {
		_ = turso_statement_finalize(stmt2)
		turso_statement_deinit(stmt2)
	}()
	posI := turso_statement_named_position(stmt2, ":i")
	posR := turso_statement_named_position(stmt2, ":r")
	posS := turso_statement_named_position(stmt2, ":s")
	posB := turso_statement_named_position(stmt2, ":b")
	posN := turso_statement_named_position(stmt2, ":n")
	require.Equal(t, posI, int64(1))
	require.Equal(t, posR, int64(2))
	require.Equal(t, posS, int64(3))
	require.Equal(t, posB, int64(4))
	require.Equal(t, posN, int64(5))

	require.NoError(t, turso_statement_bind_positional_int(stmt2, int(posI), 7))
	require.NoError(t, turso_statement_bind_positional_double(stmt2, int(posR), -1.5))
	require.NoError(t, turso_statement_bind_positional_text(stmt2, int(posS), "world"))
	require.NoError(t, turso_statement_bind_positional_blob(stmt2, int(posB), []byte{})) // empty blob
	require.NoError(t, turso_statement_bind_positional_null(stmt2, int(posN)))
	_, _, err = turso_statement_execute(stmt2)
	require.NoError(t, err)

	// Verify retrieved values using row value helpers
	stmt3 := prepStmt(t, conn.conn, "SELECT i,r,s,b,n FROM t")
	defer func() {
		_ = turso_statement_finalize(stmt3)
		turso_statement_deinit(stmt3)
	}()

	// first row
	require.True(t, stepRow(t, stmt3))
	assert.Equal(t, TURSO_TYPE_INTEGER, turso_statement_row_value_kind(stmt3, 0))
	assert.Equal(t, int64(42), turso_statement_row_value_int(stmt3, 0))
	assert.Equal(t, TURSO_TYPE_REAL, turso_statement_row_value_kind(stmt3, 1))
	assert.InDelta(t, 3.14, turso_statement_row_value_double(stmt3, 1), 1e-9)
	assert.Equal(t, TURSO_TYPE_TEXT, turso_statement_row_value_kind(stmt3, 2))
	assert.Equal(t, "hello", turso_statement_row_value_text(stmt3, 2))
	assert.Equal(t, TURSO_TYPE_BLOB, turso_statement_row_value_kind(stmt3, 3))
	assert.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, turso_statement_row_value_bytes(stmt3, 3))
	assert.Equal(t, TURSO_TYPE_NULL, turso_statement_row_value_kind(stmt3, 4))

	// second row
	require.True(t, stepRow(t, stmt3))
	assert.Equal(t, int64(7), turso_statement_row_value_int(stmt3, 0))
	assert.InDelta(t, -1.5, turso_statement_row_value_double(stmt3, 1), 1e-9)
	assert.Equal(t, "world", turso_statement_row_value_text(stmt3, 2))
	// empty blob
	assert.Nil(t, turso_statement_row_value_bytes(stmt3, 3))
	require.False(t, stepRow(t, stmt3))
}

func TestColumnMetadata(t *testing.T) {
	conn, cleanup := openInMemory(t)
	defer cleanup()

	prepExec(t, conn.conn, "CREATE TABLE t(id INT, name TEXT)")
	prepExec(t, conn.conn, "INSERT INTO t(id, name) VALUES (1, 'alice')")

	stmt := prepStmt(t, conn.conn, "SELECT id, name FROM t")
	defer func() {
		_ = turso_statement_finalize(stmt)
		turso_statement_deinit(stmt)
	}()
	cc := turso_statement_column_count(stmt)
	require.Equal(t, int64(2), cc)
	n0 := turso_statement_column_name(stmt, 0)
	n1 := turso_statement_column_name(stmt, 1)
	assert.Equal(t, "id", n0)
	assert.Equal(t, "name", n1)

	require.True(t, stepRow(t, stmt))
	assert.Equal(t, int64(1), turso_statement_row_value_int(stmt, 0))
	assert.Equal(t, "alice", turso_statement_row_value_text(stmt, 1))
	require.False(t, stepRow(t, stmt))
}
