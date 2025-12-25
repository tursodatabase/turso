package turso

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
)

var (
	conn *sql.DB
)

func openMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("turso", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMain(m *testing.M) {
	InitLibrary(turso_libs.LoadTursoLibraryConfig{LoadStrategy: "mixed"})
	var err error
	conn, err = sql.Open("turso", ":memory:")
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	err = conn.Ping()
	if err != nil {
		log.Fatalf("Error pinging database: %v", err)
	}
	defer conn.Close()
	err = createTable(conn)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	m.Run()
}

func TestEncryption(t *testing.T) {
	t.Run("encryption=disabled", func(t *testing.T) {
		tmp := t.TempDir()
		dbPath := path.Join(tmp, "local.db")
		conn, err := sql.Open("turso", dbPath)
		require.Nil(t, err)
		require.Nil(t, conn.Ping())
		_, err = conn.Exec("CREATE TABLE t(x)")
		require.Nil(t, err)
		_, err = conn.Exec("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)")
		require.Nil(t, err)
		_, err = conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		require.Nil(t, err)
		content, err := os.ReadFile(dbPath)
		require.Nil(t, err)
		require.True(t, bytes.Contains(content, []byte("secret")))
	})
	t.Run("encryption=enabled", func(t *testing.T) {
		tmp := t.TempDir()
		dbPath := path.Join(tmp, "local.db")
		conn, err := sql.Open("turso", fmt.Sprintf("%v?experimental=encryption&encryption_cipher=aegis256&encryption_hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327", dbPath))
		require.Nil(t, err)
		require.Nil(t, conn.Ping())
		_, err = conn.Exec("CREATE TABLE t(x)")
		require.Nil(t, err)
		_, err = conn.Exec("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)")
		require.Nil(t, err)
		_, err = conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		require.Nil(t, err)
		content, err := os.ReadFile(dbPath)
		require.Nil(t, err)
		require.False(t, bytes.Contains(content, []byte("secret")))
	})
}

func TestInsertData(t *testing.T) {
	err := insertData(conn)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}
}

func TestQuery(t *testing.T) {
	query := "SELECT * FROM test;"
	stmt, err := conn.Prepare(query)
	if err != nil {
		t.Fatalf("Error preparing query: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()

	expectedCols := []string{"foo", "bar", "baz"}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Error getting columns: %v", err)
	}
	if len(cols) != len(expectedCols) {
		t.Fatalf("Expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, col := range cols {
		if col != expectedCols[i] {
			t.Errorf("Expected column %d to be %s, got %s", i, expectedCols[i], col)
		}
	}
	i := 1
	for rows.Next() {
		var a int
		var b string
		var c []byte
		err = rows.Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		if a != i || b != rowsMap[i] || !slicesAreEq(c, []byte(rowsMap[i])) {
			t.Fatalf("Expected %d, %s, %s, got %d, %s, %s", i, rowsMap[i], rowsMap[i], a, b, string(c))
		}
		fmt.Println("RESULTS: ", a, b, string(c))
		i++
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
}

func TestFunctions(t *testing.T) {
	insert := "INSERT INTO test (foo, bar, baz) VALUES (?, ?, zeroblob(?));"
	stmt, err := conn.Prepare(insert)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	_, err = stmt.Exec(60, "TestFunction", 400)
	if err != nil {
		t.Fatalf("Error executing statement with arguments: %v", err)
	}
	stmt.Close()
	stmt, err = conn.Prepare("SELECT baz FROM test where foo = ?")
	if err != nil {
		t.Fatalf("Error preparing select stmt: %v", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(60)
	if err != nil {
		t.Fatalf("Error executing select stmt: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		err = rows.Scan(&b)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		if len(b) != 400 {
			t.Fatalf("Expected 100 bytes, got %d", len(b))
		}
	}
	sql := "SELECT uuid4_str();"
	stmt, err = conn.Prepare(sql)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	defer stmt.Close()
	rows, err = stmt.Query()
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()
	var i int
	for rows.Next() {
		var b string
		err = rows.Scan(&b)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		if len(b) != 36 {
			t.Fatalf("Expected 36 bytes, got %d", len(b))
		}
		i++
		fmt.Printf("uuid: %s\n", b)
	}
	if i != 1 {
		t.Fatalf("Expected 1 row, got %d", i)
	}
	fmt.Println("zeroblob + uuid functions passed")
}

func TestDuplicateConnection(t *testing.T) {
	newConn := openMem(t)
	err := createTable(newConn)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}
	err = insertData(newConn)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}
	query := "SELECT * FROM test;"
	rows, err := newConn.Query(query)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var a int
		var b string
		var c []byte
		err = rows.Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		fmt.Println("RESULTS: ", a, b, string(c))
	}
}

func TestDuplicateConnection2(t *testing.T) {
	newConn := openMem(t)
	sql := "CREATE TABLE test (foo INTEGER, bar INTEGER, baz BLOB);"
	newConn.Exec(sql)
	sql = "INSERT INTO test (foo, bar, baz) VALUES (?, ?, uuid4());"
	stmt, err := newConn.Prepare(sql)
	require.Nil(t, err)
	stmt.Exec(242345, 2342434)
	defer stmt.Close()
	query := "SELECT * FROM test;"
	rows, err := newConn.Query(query)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var a int
		var b int
		var c []byte
		err = rows.Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		fmt.Println("RESULTS: ", a, b, string(c))
		if len(c) != 16 {
			t.Fatalf("Expected 16 bytes, got %d", len(c))
		}
	}
}

func TestConnectionError(t *testing.T) {
	newConn := openMem(t)
	sql := "CREATE TABLE test (foo INTEGER, bar INTEGER, baz BLOB);"
	newConn.Exec(sql)
	sql = "INSERT INTO test (foo, bar, baz) VALUES (?, ?, notafunction(?));"
	_, err := newConn.Prepare(sql)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
	expectedErr := "turso: error: Parse error: unknown function notafunction"
	if err.Error() != expectedErr {
		t.Fatalf("Error test failed, expected: %s, found: %v", expectedErr, err)
	}
	fmt.Println("Connection error test passed")
}

func TestStatementError(t *testing.T) {
	newConn := openMem(t)
	sql := "CREATE TABLE test (foo INTEGER, bar INTEGER, baz BLOB);"
	newConn.Exec(sql)
	sql = "INSERT INTO test (foo, bar, baz) VALUES (?, ?, ?);"
	stmt, err := newConn.Prepare(sql)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	_, err = stmt.Exec(1, 2)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
	if err.Error() != "sql: expected 3 arguments, got 2" {
		t.Fatalf("Unexpected : %v\n", err)
	}
	fmt.Println("Statement error test passed")
}

func TestDriverRowsErrorMessages(t *testing.T) {
	db := openMem(t)
	_, err := db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (id, name) VALUES (?, ?)", 1, "Alice")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM test")
	if err != nil {
		t.Fatalf("failed to query table: %v", err)
	}

	if !rows.Next() {
		t.Fatalf("expected at least one row")
	}
	var id int
	var name string
	err = rows.Scan(&name, &id)
	if err == nil {
		t.Fatalf("expected error scanning wrong type: %v", err)
	}
	t.Log("Rows error behavior test passed")
}

func TestTransaction(t *testing.T) {
	// Open database connection
	db := openMem(t)
	// Create a test table
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'Initial')")
	if err != nil {
		t.Fatalf("Error inserting initial data: %v", err)
	}

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Error starting transaction: %v", err)
	}

	// Insert data within the transaction
	_, err = tx.Exec("INSERT INTO test (id, name) VALUES (2, 'Transaction')")
	if err != nil {
		t.Fatalf("Error inserting data in transaction: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error committing transaction: %v", err)
	}

	// Verify both rows are visible after commit
	rows, err := db.Query("SELECT id, name FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("Error querying data after commit: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id   int
		name string
	}{
		{1, "Initial"},
		{2, "Transaction"},
	}

	i := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}

		if id != expected[i].id || name != expected[i].name {
			t.Errorf("Row %d: expected (%d, %s), got (%d, %s)",
				i, expected[i].id, expected[i].name, id, name)
		}
		i++
	}

	if i != 2 {
		t.Fatalf("Expected 2 rows, got %d", i)
	}

	t.Log("Transaction test passed")
}

func TestVectorOperations(t *testing.T) {
	db := openMem(t)
	// Test creating table with vector columns
	_, err := db.Exec(`CREATE TABLE vector_test (id INTEGER PRIMARY KEY, embedding F32_BLOB(64))`)
	if err != nil {
		t.Fatalf("Error creating vector table: %v", err)
	}

	// Test vector insertion
	_, err = db.Exec(`INSERT INTO vector_test VALUES (1, vector('[0.1, 0.2, 0.3, 0.4, 0.5]'))`)
	if err != nil {
		t.Fatalf("Error inserting vector: %v", err)
	}

	// Test vector similarity calculation
	var similarity float64
	err = db.QueryRow(`SELECT vector_distance_cos(embedding, vector('[0.2, 0.3, 0.4, 0.5, 0.6]')) FROM vector_test WHERE id = 1`).Scan(&similarity)
	if err != nil {
		t.Fatalf("Error calculating vector similarity: %v", err)
	}
	if similarity <= 0 || similarity > 1 {
		t.Fatalf("Expected similarity between 0 and 1, got %f", similarity)
	}

	// Test vector extraction
	var extracted string
	err = db.QueryRow(`SELECT vector_extract(embedding) FROM vector_test WHERE id = 1`).Scan(&extracted)
	if err != nil {
		t.Fatalf("Error extracting vector: %v", err)
	}
	fmt.Printf("Extracted vector: %s\n", extracted)
}

func TestSQLFeatures(t *testing.T) {
	db := openMem(t)

	// Create test tables
	_, err := db.Exec(`
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        )`)
	if err != nil {
		t.Fatalf("Error creating customers table: %v", err)
	}

	_, err = db.Exec(`
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount REAL,
            date TEXT
        )`)
	if err != nil {
		t.Fatalf("Error creating orders table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
        INSERT INTO customers VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 40)`)
	if err != nil {
		t.Fatalf("Error inserting customers: %v", err)
	}

	_, err = db.Exec(`
        INSERT INTO orders VALUES
            (1, 1, 100.50, '2024-01-01'),
            (2, 1, 200.75, '2024-02-01'),
            (3, 2, 50.25, '2024-01-15'),
            (4, 3, 300.00, '2024-02-10')`)
	if err != nil {
		t.Fatalf("Error inserting orders: %v", err)
	}

	// Test JOIN
	rows, err := db.Query(`
        SELECT c.name, o.amount
        FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        ORDER BY o.amount DESC`)
	if err != nil {
		t.Fatalf("Error executing JOIN: %v", err)
	}
	defer rows.Close()

	// Check JOIN results
	expectedResults := []struct {
		name   string
		amount float64
	}{
		{"Charlie", 300.00},
		{"Alice", 200.75},
		{"Alice", 100.50},
		{"Bob", 50.25},
	}

	i := 0
	for rows.Next() {
		var name string
		var amount float64
		if err := rows.Scan(&name, &amount); err != nil {
			t.Fatalf("Error scanning JOIN result: %v", err)
		}
		if i >= len(expectedResults) {
			t.Fatalf("Too many rows returned from JOIN")
		}
		if name != expectedResults[i].name || amount != expectedResults[i].amount {
			t.Fatalf("Row %d: expected (%s, %.2f), got (%s, %.2f)",
				i, expectedResults[i].name, expectedResults[i].amount, name, amount)
		}
		i++
	}

	// Test GROUP BY with aggregation
	var count int
	var total float64
	err = db.QueryRow(`
        SELECT COUNT(*), SUM(amount)
        FROM orders
        WHERE customer_id = 1
        GROUP BY customer_id`).Scan(&count, &total)
	if err != nil {
		t.Fatalf("Error executing GROUP BY: %v", err)
	}
	if count != 2 || total != 301.25 {
		t.Fatalf("GROUP BY gave wrong results: count=%d, total=%.2f", count, total)
	}
}

func TestDateTimeFunctions(t *testing.T) {
	db := openMem(t)
	// Test date()
	var dateStr string
	err := db.QueryRow(`SELECT date('now')`).Scan(&dateStr)
	if err != nil {
		t.Fatalf("Error with date() function: %v", err)
	}
	fmt.Printf("Current date: %s\n", dateStr)

	// Test date arithmetic
	err = db.QueryRow(`SELECT date('2024-01-01', '+1 month')`).Scan(&dateStr)
	if err != nil {
		t.Fatalf("Error with date arithmetic: %v", err)
	}
	if dateStr != "2024-02-01" {
		t.Fatalf("Expected '2024-02-01', got '%s'", dateStr)
	}

	// Test strftime
	var formatted string
	err = db.QueryRow(`SELECT strftime('%Y-%m-%d', '2024-01-01')`).Scan(&formatted)
	if err != nil {
		t.Fatalf("Error with strftime function: %v", err)
	}
	if formatted != "2024-01-01" {
		t.Fatalf("Expected '2024-01-01', got '%s'", formatted)
	}
}

func TestMathFunctions(t *testing.T) {
	db := openMem(t)
	// Test basic math functions
	var result float64
	err := db.QueryRow(`SELECT abs(-15.5)`).Scan(&result)
	if err != nil {
		t.Fatalf("Error with abs function: %v", err)
	}
	if result != 15.5 {
		t.Fatalf("abs(-15.5) should be 15.5, got %f", result)
	}

	// Test trigonometric functions
	err = db.QueryRow(`SELECT round(sin(radians(30)), 4)`).Scan(&result)
	if err != nil {
		t.Fatalf("Error with sin function: %v", err)
	}
	if math.Abs(result-0.5) > 0.0001 {
		t.Fatalf("sin(30 degrees) should be about 0.5, got %f", result)
	}

	// Test power functions
	err = db.QueryRow(`SELECT pow(2, 3)`).Scan(&result)
	if err != nil {
		t.Fatalf("Error with pow function: %v", err)
	}
	if result != 8 {
		t.Fatalf("2^3 should be 8, got %f", result)
	}
}

func TestJSONFunctions(t *testing.T) {
	db := openMem(t)
	// Test json function
	var valid int
	err := db.QueryRow(`SELECT json_valid('{"name":"John","age":30}')`).Scan(&valid)
	if err != nil {
		t.Fatalf("Error with json_valid function: %v", err)
	}
	if valid != 1 {
		t.Fatalf("Expected valid JSON to return 1, got %d", valid)
	}

	// Test json_extract
	var name string
	err = db.QueryRow(`SELECT json_extract('{"name":"John","age":30}', '$.name')`).Scan(&name)
	if err != nil {
		t.Fatalf("Error with json_extract function: %v", err)
	}
	if name != "John" {
		t.Fatalf("Expected 'John', got '%s'", name)
	}

	// Test JSON shorthand
	var age int
	err = db.QueryRow(`SELECT '{"name":"John","age":30}' -> '$.age'`).Scan(&age)
	if err != nil {
		t.Fatalf("Error with JSON shorthand: %v", err)
	}
	if age != 30 {
		t.Fatalf("Expected 30, got %d", age)
	}
}

func TestParameterOrdering(t *testing.T) {
	newConn := openMem(t)
	sql := "CREATE TABLE test (a,b,c);"
	newConn.Exec(sql)

	// Test inserting with parameters in a different order than
	// the table definition.
	sql = "INSERT INTO test (b, c ,a) VALUES (?, ?, ?);"
	expectedValues := []int{1, 2, 3}
	stmt, err := newConn.Prepare(sql)
	require.Nil(t, err)
	_, err = stmt.Exec(expectedValues[1], expectedValues[2], expectedValues[0])
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	// check that the values are in the correct order
	query := "SELECT a,b,c FROM test;"
	rows, err := newConn.Query(query)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	for rows.Next() {
		var a, b, c int
		err := rows.Scan(&a, &b, &c)
		if err != nil {
			t.Fatal("Error scanning row: ", err)
		}
		result := []int{a, b, c}
		for i := range 3 {
			if result[i] != expectedValues[i] {
				fmt.Printf("RESULTS: %d, %d, %d\n", a, b, c)
				fmt.Printf("EXPECTED: %d, %d, %d\n", expectedValues[0], expectedValues[1], expectedValues[2])
			}
		}
	}

	// -- part 2 --
	// mixed parameters and regular values
	sql2 := "CREATE TABLE test2 (a,b,c);"
	newConn.Exec(sql2)
	expectedValues2 := []int{1, 2, 3}

	// Test inserting with parameters in a different order than
	// the table definition, with a mixed regular parameter included
	sql2 = "INSERT INTO test2 (a, b ,c) VALUES (1, ?, ?);"
	_, err = newConn.Exec(sql2, expectedValues2[1], expectedValues2[2])
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	// check that the values are in the correct order
	query2 := "SELECT a,b,c FROM test2;"
	rows2, err := newConn.Query(query2)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	for rows2.Next() {
		var a, b, c int
		err := rows2.Scan(&a, &b, &c)
		if err != nil {
			t.Fatal("Error scanning row: ", err)
		}
		result := []int{a, b, c}

		fmt.Printf("RESULTS: %d, %d, %d\n", a, b, c)
		fmt.Printf("EXPECTED: %d, %d, %d\n", expectedValues[0], expectedValues[1], expectedValues[2])
		for i := range 3 {
			if result[i] != expectedValues[i] {
				t.Fatalf("Expected %d, got %d", expectedValues[i], result[i])
			}
		}
	}
}

func TestLimitOffsetParameters(t *testing.T) {
	newConn := openMem(t)
	sql := "CREATE TABLE test (a, b);"
	_, err := newConn.Exec(sql)
	if err != nil {
		t.Fatal("Error creating table")
	}
	sql = "INSERT INTO test (a, b) VALUES (1, 'a'), (2,'b'), (3,'c'), (4,'c'), (5,'d');"
	_, err = newConn.Exec(sql)
	if err != nil {
		t.Fatal("Error inserting data")
	}
	sql = "SELECT a, b FROM test ORDER BY b DESC LIMIT ? OFFSET ?;"
	query, err := newConn.Prepare(sql)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	limit := 2
	offset := 1
	expected := []int{4, 3}
	rows, err := query.Query(limit, offset)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	var a int
	var b string
	for rows.Next() {
		rows.Scan(&a, &b)
		if a != expected[0] && a != expected[1] {
			t.Fatalf("Expected %d or %d, got %d", expected[0], expected[1], a)
		}
	}
}

func TestIndex(t *testing.T) {
	newConn := openMem(t)
	sql := "CREATE TABLE users (name TEXT PRIMARY KEY, email TEXT)"
	_, err := newConn.Exec(sql)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}
	sql = "CREATE INDEX email_idx ON users(email)"
	_, err = newConn.Exec(sql)
	if err != nil {
		t.Fatalf("Error creating index: %v", err)
	}

	// Test inserting with parameters in a different order than
	// the table definition.
	sql = "INSERT INTO users VALUES ('alice', 'a@b.c'), ('bob', 'b@d.e')"
	_, err = newConn.Exec(sql)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	for filter, row := range map[string][]string{
		"a@b.c": {"alice", "a@b.c"},
		"b@d.e": {"bob", "b@d.e"},
	} {
		query := "SELECT * FROM users WHERE email = ?"
		rows, err := newConn.Query(query, filter)
		if err != nil {
			t.Fatalf("Error executing query: %v", err)
		}
		for rows.Next() {
			var name, email string
			err := rows.Scan(&name, &email)
			t.Log("name,email:", name, email)
			if err != nil {
				t.Fatal("Error scanning row: ", err)
			}
			if !slices.Equal([]string{name, email}, row) {
				t.Fatal("Unexpected result", row, []string{name, email})
			}
		}
	}
}

func slicesAreEq(a, b []byte) bool {
	if len(a) != len(b) {
		fmt.Printf("LENGTHS NOT EQUAL: %d != %d\n", len(a), len(b))
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			fmt.Printf("SLICES NOT EQUAL: %v != %v\n", a, b)
			return false
		}
	}
	return true
}

var rowsMap = map[int]string{1: "hello", 2: "world", 3: "foo", 4: "bar", 5: "baz"}

func createTable(conn *sql.DB) error {
	insert := "CREATE TABLE test (foo INT, bar TEXT, baz BLOB);"
	stmt, err := conn.Prepare(insert)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	return err
}

func insertData(conn *sql.DB) error {
	for i := 1; i <= 5; i++ {
		insert := "INSERT INTO test (foo, bar, baz) VALUES (?, ?, ?);"
		stmt, err := conn.Prepare(insert)
		if err != nil {
			return err
		}
		defer stmt.Close()
		if _, err = stmt.Exec(i, rowsMap[i], []byte(rowsMap[i])); err != nil {
			return err
		}
	}
	return nil
}

func TestNullHandling(t *testing.T) {
	db := openMem(t)
	_, err := db.Exec(`
		CREATE TABLE null_test (
			id INTEGER PRIMARY KEY,
			text_val TEXT,
			int_val INTEGER,
			real_val REAL,
			blob_val BLOB
		)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	testCases := []struct {
		name     string
		query    string
		args     []any
		expected []any
	}{
		{"all nulls", "INSERT INTO null_test (id) VALUES (?)", []any{1}, []any{1, nil, nil, nil, nil}},
		{"mixed nulls", "INSERT INTO null_test VALUES (?, ?, ?, ?, ?)", []any{2, "text", nil, 3.14, nil}, []any{2, "text", nil, 3.14, nil}},
		{"no nulls", "INSERT INTO null_test VALUES (?, ?, ?, ?, ?)", []any{3, "full", 42, 2.718, []byte("data")}, []any{3, "full", 42, 2.718, []byte("data")}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(tc.query, tc.args...)
			if err != nil {
				t.Fatalf("Error inserting: %v", err)
			}
		})
	}

	rows, err := db.Query("SELECT * FROM null_test ORDER BY id")
	if err != nil {
		t.Fatalf("Error querying: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var id sql.NullInt64
		var textVal sql.NullString
		var intVal sql.NullInt64
		var realVal sql.NullFloat64
		var blobVal []byte

		err := rows.Scan(&id, &textVal, &intVal, &realVal, &blobVal)
		if err != nil {
			t.Fatalf("Error scanning: %v", err)
		}

		if !id.Valid {
			t.Errorf("ID should always be valid")
		}
		i++
	}

	if i != 3 {
		t.Fatalf("Expected 3 rows, got %d", i)
	}
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) sql.Result {
	t.Helper()
	res, err := db.Exec(q, args...)
	if err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
	return res
}

func TestLastInsertIDAndRowsAffected(t *testing.T) {
	db := openMem(t)
	mustExec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)`)
	res := mustExec(t, db, `INSERT INTO t(name) VALUES ('alice')`)
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId: %v", err)
	}
	if id == 0 {
		t.Fatalf("expected non-zero last insert id")
	}
	res = mustExec(t, db, `UPDATE t SET name='ALICE' WHERE id = ?`, id)
	ra, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if ra != 1 {
		t.Fatalf("expected 1 row affected, got %d", ra)
	}
}

func TestDataTypes(t *testing.T) {
	db, err := sql.Open("turso", ":memory:")
	if err != nil {
		t.Fatalf("Error opening connection: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE types_test (
			col_integer INTEGER,
			col_real REAL,
			col_text TEXT,
			col_blob BLOB,
			col_numeric NUMERIC,
			col_boolean BOOLEAN,
			col_date DATE,
			col_datetime DATETIME,
			col_timestamp TIMESTAMP
		)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert test data
	now := time.Now()
	_, err = db.Exec(`
		INSERT INTO types_test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		42,
		3.14159,
		"Hello, 世界",
		[]byte{0x01, 0x02, 0x03},
		"123.456",
		true,
		now.Format("2006-01-02"),
		now.Format("2006-01-02 15:04:05"),
		now.Unix(),
	)
	if err != nil {
		t.Fatalf("Error inserting: %v", err)
	}

	// Query and verify each type
	var (
		colInt       int
		colReal      float64
		colText      string
		colBlob      []byte
		colNumeric   string
		colBool      bool
		colDate      string
		colDateTime  string
		colTimestamp int64
	)

	err = db.QueryRow("SELECT * FROM types_test").Scan(
		&colInt, &colReal, &colText, &colBlob, &colNumeric,
		&colBool, &colDate, &colDateTime, &colTimestamp,
	)
	if err != nil {
		t.Fatalf("Error scanning: %v", err)
	}

	// Verify values
	if colInt != 42 {
		t.Errorf("Integer mismatch: got %d", colInt)
	}
	if math.Abs(colReal-3.14159) > 0.00001 {
		t.Errorf("Real mismatch: got %f", colReal)
	}
	if colText != "Hello, 世界" {
		t.Errorf("Text mismatch: got %s", colText)
	}
	if !slices.Equal(colBlob, []byte{0x01, 0x02, 0x03}) {
		t.Errorf("Blob mismatch: got %v", colBlob)
	}
}

func createDatabasesTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS databases (
		id INTEGER PRIMARY KEY,
		created_at TEXT,
		updated_at TEXT,
		deleted_at TEXT,
		hostname TEXT UNIQUE,
		namespace TEXT,
		fly_app TEXT,
		address TEXT,
		primary_address TEXT,
		cloud_cluster_name TEXT,
		local BOOLEAN,
		allowed_ips TEXT
	)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func TestUpsertReturning_databaseSQL_Prepared(t *testing.T) {
	db := openMem(t)
	createDatabasesTable(t, db)

	const stmtText = `
	INSERT INTO databases
		(created_at,updated_at,deleted_at,hostname,namespace,fly_app,address,primary_address,cloud_cluster_name,local,allowed_ips)
	VALUES (?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT (hostname) DO UPDATE SET
		updated_at=excluded.updated_at,
		deleted_at=excluded.deleted_at,
		hostname=excluded.hostname,
		namespace=excluded.namespace,
		fly_app=excluded.fly_app,
		address=excluded.address,
		primary_address=excluded.primary_address,
		cloud_cluster_name=excluded.cloud_cluster_name,
		local=excluded.local,
		allowed_ips=excluded.allowed_ips
	RETURNING id`

	now := time.Now()
	args := []any{
		now,                      // created_at (driver will send RFC3339 string)
		now,                      // updated_at
		nil,                      // deleted_at
		"host-1.local",           // hostname (unique)
		"ns-123",                 // namespace
		"app-xyz",                // fly_app
		"http://127.0.0.1:10000", // address
		"",                       // primary_address
		"local",                  // cloud_cluster_name
		false,                    // local (bool -> int 0/1 in your marshaler)
		nil,                      // allowed_ips (NULL)
	}

	stmt, err := db.Prepare(stmtText)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	var returnedID int64
	if err := stmt.QueryRow(args...).Scan(&returnedID); err != nil {
		t.Fatalf("queryrow/scan: %v", err)
	}
	t.Logf("returned id: %d", returnedID)
	cpy := returnedID

	// Re-run to trigger ON CONFLICT path and ensure still binds 12 args and returns id
	args[1] = time.Now() // updated_at
	if err := stmt.QueryRow(args...).Scan(&returnedID); err != nil {
		t.Fatalf("queryrow/scan (conflict): %v", err)
	}
	if returnedID != cpy {
		t.Fatalf("expected same id on conflict, got %d then %d", cpy, returnedID)
	}
}

func TestInsertReturning(t *testing.T) {
	db := openMem(t)
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (x)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	var returnedID int64
	if err := db.QueryRow("INSERT INTO t VALUES (1) RETURNING x").Scan(&returnedID); err != nil {
		t.Fatalf("queryrow/scan: %v", err)
	}
	if returnedID != 1 {
		t.Fatalf("unexpected returnedId: %v", err)
	}
	t.Log(returnedID)
	if err := db.QueryRow("SELECT * FROM t").Scan(&returnedID); err != nil {
		t.Fatalf("queryrow/scan (conflict): %v", err)
	}
	if returnedID != 1 {
		t.Fatalf("unexpected returnedId: %v", err)
	}
	t.Log(returnedID)
}

func TestUpsertReturning_databaseSQL_Prepared_ArgCountMismatch(t *testing.T) {
	db := openMem(t)
	createDatabasesTable(t, db)

	const stmtText = `
	INSERT INTO databases
		(created_at,updated_at,deleted_at,hostname,namespace,fly_app,address,primary_address,cloud_cluster_name,local,allowed_ips,id)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT (hostname) DO UPDATE SET
		updated_at=excluded.updated_at,
		deleted_at=excluded.deleted_at,
		hostname=excluded.hostname,
		namespace=excluded.namespace,
		fly_app=excluded.fly_app,
		address=excluded.address,
		primary_address=excluded.primary_address,
		cloud_cluster_name=excluded.cloud_cluster_name,
		local=excluded.local,
		allowed_ips=excluded.allowed_ips
	RETURNING id`

	stmt, err := db.Prepare(stmtText)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	now := time.Now()
	args := []any{
		now, now, nil, "host-2.local", "ns", "app", "addr", "", "local", false, nil, 22,
	}
	// Append a bogus 13th arg to force the exact database/sql error you saw
	args = append(args, 999)

	var id int64
	if err := stmt.QueryRow(args...).Scan(&id); err == nil {
		t.Fatal("expected argument count error, got nil")
	}
}

func TestMultiStatementExecution(t *testing.T) {
	db := openMem(t)

	t.Run("BasicMultiStatement", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			INSERT INTO users (name) VALUES ('Alice');
			INSERT INTO users (name) VALUES ('Bob');
		`)
		if err != nil {
			t.Fatalf("Failed to execute multi-statement: %v", err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})

	t.Run("StringsWithSemicolons", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE messages (id INTEGER PRIMARY KEY, text TEXT);
			INSERT INTO messages (text) VALUES ('Hello; World');
			INSERT INTO messages (text) VALUES ('Test; Message; Multiple');
		`)
		if err != nil {
			t.Fatalf("Failed to execute with semicolons in strings: %v", err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}

		rows, err := db.Query("SELECT text FROM messages ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query messages: %v", err)
		}
		defer rows.Close()

		expected := []string{"Hello; World", "Test; Message; Multiple"}
		i := 0
		for rows.Next() {
			var text string
			if err := rows.Scan(&text); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			if text != expected[i] {
				t.Errorf("Row %d: expected %q, got %q", i, expected[i], text)
			}
			i++
		}
	})

	t.Run("EscapedQuotes", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE names (id INTEGER PRIMARY KEY, name TEXT);
			INSERT INTO names (name) VALUES ('O''Brien');
			INSERT INTO names (name) VALUES ('It''s working');
		`)
		if err != nil {
			t.Fatalf("Failed to execute with escaped quotes: %v", err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM names").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}

		var name string
		err = db.QueryRow("SELECT name FROM names WHERE id = 1").Scan(&name)
		if err != nil {
			t.Fatalf("Failed to query name: %v", err)
		}
		if name != "O'Brien" {
			t.Errorf("Expected \"O'Brien\", got %q", name)
		}
	})

	t.Run("EmptyStatements", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE test_empty (id INTEGER);;;
			INSERT INTO test_empty (id) VALUES (1);;
		`)
		if err != nil {
			t.Fatalf("Failed to execute with empty statements: %v", err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_empty").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	t.Run("FailureInMiddle", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE partial (id INTEGER PRIMARY KEY);
			INSERT INTO partial (id) VALUES (1);
			INSERT INTO partial (id) VALUES (1);
		`)
		if err == nil {
			t.Fatal("Expected error for duplicate key, got nil")
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM partial").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row (from first INSERT before failure), got %d", count)
		}
	})

	t.Run("WithParameters", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE param_test (id INTEGER, name TEXT);`)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		_, err = db.Exec("INSERT INTO param_test (id, name) VALUES (?, ?)", 1, "Test")
		if err != nil {
			t.Fatalf("Failed to insert with parameters: %v", err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM param_test").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})
}
