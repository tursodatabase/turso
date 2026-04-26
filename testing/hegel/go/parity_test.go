// Hegel property-based parity tests for the Go turso and turso-serverless
// database/sql drivers.
//
// Generates random sequences of database operations and asserts that both
// drivers produce structurally identical results: same success/failure,
// same column counts/names, same row counts, same value types, and same
// actual cell values.
//
// Requires a running libsql-server for the remote driver (throttling disabled
// so property tests don't hit 429s):
//
//	docker run -d -p 8080:8080 \
//	  -e SQLD_MAX_CONCURRENT_REQUESTS=1024 \
//	  -e SQLD_MAX_CONCURRENT_CONNECTIONS=1024 \
//	  -e SQLD_DISABLE_INTELLIGENT_THROTTLING=true \
//	  ghcr.io/tursodatabase/libsql-server:latest
package parity_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
	turso "turso.tech/database/tursogo"
	_ "turso.tech/database/tursogo-serverless"

	"hegel.dev/go/hegel"
)

// ---------------------------------------------------------------------------
// Load spec from ops.json at startup
// ---------------------------------------------------------------------------

type opsSpec struct {
	Constants struct {
		NumTables      int      `json:"num_tables"`
		ColTypes       []string `json:"col_types"`
		MaxOpsPerCase  int      `json:"max_ops_per_case"`
		MaxDynamicCols int      `json:"max_dynamic_cols"`
		ErrorSqls      []string `json:"error_sqls"`
	} `json:"constants"`
	Values         []json.RawMessage `json:"values"`
	UnicodeOptions []string          `json:"unicode_options"`
	Ops            []struct {
		ID          string   `json:"id"`
		SQL         string   `json:"sql"`
		NamedParams []string `json:"named_params,omitempty"`
	} `json:"ops"`
}

type valueSpec struct {
	ID           string    `json:"id"`
	Random       *struct {
		Min int `json:"min"`
		Max int `json:"max"`
	} `json:"random,omitempty"`
	RandomScaled *struct {
		Min     int     `json:"min"`
		Max     int     `json:"max"`
		Divisor float64 `json:"divisor"`
	} `json:"random_scaled,omitempty"`
	RandomString *struct {
		MaxLen int `json:"max_len"`
	} `json:"random_string,omitempty"`
	RandomBytes *struct {
		MaxLen int `json:"max_len"`
	} `json:"random_bytes,omitempty"`
	Oneof      []int64   `json:"oneof,omitempty"`
	OneofFloat []float64 `json:"oneof_float,omitempty"`
	Literal    *string   `json:"literal,omitempty"`
	LiteralFloat *float64 `json:"literal_float,omitempty"`
	LiteralBytes *string  `json:"literal_bytes,omitempty"`
	FillBytes  *struct {
		Byte int `json:"byte"`
		Len  int `json:"len"`
	} `json:"fill_bytes,omitempty"`
	RepeatChar *struct {
		Char string `json:"char"`
		Len  int    `json:"len"`
	} `json:"repeat_char,omitempty"`
}

var spec opsSpec
var valueSpecs []valueSpec

func init() {
	_, thisFile, _, _ := runtime.Caller(0)
	specPath := filepath.Join(filepath.Dir(thisFile), "..", "spec", "ops.json")
	data, err := os.ReadFile(specPath)
	if err != nil {
		panic(fmt.Sprintf("failed to read ops.json at %s: %v", specPath, err))
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		panic(fmt.Sprintf("invalid ops.json: %v", err))
	}
	valueSpecs = make([]valueSpec, len(spec.Values))
	for i, raw := range spec.Values {
		if err := json.Unmarshal(raw, &valueSpecs[i]); err != nil {
			panic(fmt.Sprintf("invalid value spec at index %d: %v", i, err))
		}
	}
}

// ---------------------------------------------------------------------------
// Operation vocabulary
// ---------------------------------------------------------------------------

type Op struct {
	Kind        string
	Table       string
	Cols        []ColDef // for create_dynamic
	Values      []any
	SQL         string
	Params      []any
	Expr        string
	Value       any      // for update_returning / update_affected
	ParamsSets  [][]any  // for prepared_reuse
	NamedParams []struct {
		Name  string
		Value any
	}
}

type ColDef struct {
	Name string
	Type string
}

type OpResult struct {
	Success         bool
	ColumnCount     int
	ColumnNames     []string
	RowCount        int
	ValueTypes      [][]string
	Values          [][]any
	AffectedRows    int64
	HasAffected     bool
	LastInsertRowid int64
	HasRowid        bool
}

// ---------------------------------------------------------------------------
// Value comparison with float epsilon tolerance
// ---------------------------------------------------------------------------

func cellEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			return av == bv
		case float64:
			return math.Abs(float64(av)-bv) < 1e-12
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			max := math.Max(math.Abs(av), math.Abs(bv))
			if max == 0 {
				return true
			}
			return math.Abs(av-bv)/max < 1e-12
		case int64:
			return math.Abs(av-float64(bv)) < 1e-12
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Equal(av, bv)
		}
	}
	return false
}

func resultsEqual(a, b OpResult) bool {
	if a.Success != b.Success {
		return false
	}
	if a.ColumnCount != b.ColumnCount {
		return false
	}
	if a.RowCount != b.RowCount {
		return false
	}
	if a.HasAffected != b.HasAffected {
		return false
	}
	if a.HasAffected && a.AffectedRows != b.AffectedRows {
		return false
	}
	if a.HasRowid != b.HasRowid {
		return false
	}
	if a.HasRowid && a.LastInsertRowid != b.LastInsertRowid {
		return false
	}

	// Column names
	if len(a.ColumnNames) != len(b.ColumnNames) {
		return false
	}
	for i := range a.ColumnNames {
		if a.ColumnNames[i] != b.ColumnNames[i] {
			return false
		}
	}

	// Value types
	if len(a.ValueTypes) != len(b.ValueTypes) {
		return false
	}
	for i := range a.ValueTypes {
		if len(a.ValueTypes[i]) != len(b.ValueTypes[i]) {
			return false
		}
		for j := range a.ValueTypes[i] {
			if a.ValueTypes[i][j] != b.ValueTypes[i][j] {
				return false
			}
		}
	}

	// Actual cell values
	if len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Values {
		if len(a.Values[i]) != len(b.Values[i]) {
			return false
		}
		for j := range a.Values[i] {
			if !cellEqual(a.Values[i][j], b.Values[i][j]) {
				return false
			}
		}
	}

	return true
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

func genTableName(ht *hegel.T, prefix int) string {
	idx := hegel.Draw(ht, hegel.Integers(0, spec.Constants.NumTables-1))
	return fmt.Sprintf("t_%d_%d", prefix, idx)
}

func genValue(ht *hegel.T) any {
	variant := hegel.Draw(ht, hegel.Integers(0, len(valueSpecs)-1))
	vs := &valueSpecs[variant]

	switch vs.ID {
	case "null":
		return nil
	case "integer":
		return int64(hegel.Draw(ht, hegel.Integers(vs.Random.Min, vs.Random.Max)))
	case "float":
		v := hegel.Draw(ht, hegel.Integers(vs.RandomScaled.Min, vs.RandomScaled.Max))
		return float64(v) / vs.RandomScaled.Divisor
	case "ascii":
		length := hegel.Draw(ht, hegel.Integers(0, vs.RandomString.MaxLen))
		b := make([]byte, length)
		for i := range b {
			b[i] = byte(hegel.Draw(ht, hegel.Integers(32, 126)))
		}
		return string(b)
	case "blob":
		length := hegel.Draw(ht, hegel.Integers(0, vs.RandomBytes.MaxLen))
		b := make([]byte, length)
		for i := range b {
			b[i] = byte(hegel.Draw(ht, hegel.Integers(0, 255)))
		}
		return b
	case "int_extreme":
		pick := hegel.Draw(ht, hegel.Integers(0, len(vs.Oneof)-1))
		return vs.Oneof[pick]
	case "float_extreme":
		pick := hegel.Draw(ht, hegel.Integers(0, len(vs.OneofFloat)-1))
		return vs.OneofFloat[pick]
	case "empty_string":
		return *vs.Literal
	case "empty_blob":
		return []byte{}
	case "large_or_unicode":
		pick := hegel.Draw(ht, hegel.Integers(0, 1))
		if pick == 0 {
			b := make([]byte, 256)
			for i := range b {
				b[i] = 0xAB
			}
			return b
		}
		idx := hegel.Draw(ht, hegel.Integers(0, len(spec.UnicodeOptions)-1))
		return spec.UnicodeOptions[idx]
	case "negative_zero":
		return float64(math.Copysign(0, -1))
	case "zero_blob", "high_bit_blob", "large_blob":
		return bytes.Repeat([]byte{byte(vs.FillBytes.Byte)}, vs.FillBytes.Len)
	case "large_string":
		return strings.Repeat(vs.RepeatChar.Char, vs.RepeatChar.Len)
	default:
		// literal string values (null_bytes_string, sql_chars_string, etc.)
		if vs.Literal != nil {
			return *vs.Literal
		}
		if vs.LiteralBytes != nil {
			return []byte{}
		}
		if vs.LiteralFloat != nil {
			return *vs.LiteralFloat
		}
		panic(fmt.Sprintf("unknown value id in ops.json: %s", vs.ID))
	}
}

func genDynamicCols(ht *hegel.T) []ColDef {
	count := hegel.Draw(ht, hegel.Integers(1, spec.Constants.MaxDynamicCols))
	cols := make([]ColDef, count)
	for i := range cols {
		typeIdx := hegel.Draw(ht, hegel.Integers(0, len(spec.Constants.ColTypes)-1))
		cols[i] = ColDef{
			Name: fmt.Sprintf("c%d", i),
			Type: spec.Constants.ColTypes[typeIdx],
		}
	}
	return cols
}

func genBatchSQL(ht *hegel.T, prefix int) string {
	tbl := hegel.Draw(ht, hegel.Integers(0, spec.Constants.NumTables-1))
	a := hegel.Draw(ht, hegel.Integers(-1000, 1000))
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS t_%d_%d (a INTEGER, b TEXT); INSERT INTO t_%d_%d VALUES (%d, 'batch')",
		prefix, tbl, prefix, tbl, a,
	)
}

// genValuesForTable generates values matching a table's column count (default 2 if unknown).
// Always draws 5 values to keep the hegel draw sequence deterministic, then slices.
func genValuesForTable(ht *hegel.T, table string, tableCols map[string]int) []any {
	vals := make([]any, 5)
	for i := range vals {
		vals[i] = genValue(ht)
	}
	n := 2
	if c, ok := tableCols[table]; ok {
		n = c
	}
	return vals[:n]
}

func genErrorSQL(ht *hegel.T, prefix int) string {
	errorSqls := spec.Constants.ErrorSqls
	pick := hegel.Draw(ht, hegel.Integers(0, len(errorSqls)-1))
	sql := errorSqls[pick]
	return strings.ReplaceAll(sql, "{prefix}", fmt.Sprintf("%d", prefix))
}

func genOp(ht *hegel.T, tableCols map[string]int, prefix int) Op {
	variant := hegel.Draw(ht, hegel.Integers(0, len(spec.Ops)-1))
	opSpec := &spec.Ops[variant]

	switch opSpec.ID {
	case "create":
		table := genTableName(ht, prefix)
		tableCols[table] = 2
		return Op{Kind: "create", Table: table}
	case "insert":
		table := genTableName(ht, prefix)
		return Op{
			Kind:   "insert",
			Table:  table,
			Values: genValuesForTable(ht, table, tableCols),
		}
	case "select":
		return Op{Kind: "select", Table: genTableName(ht, prefix)}
	case "select_value":
		v := hegel.Draw(ht, hegel.Integers(-1000, 1000))
		return Op{Kind: "select_value", Expr: fmt.Sprintf("%d", v)}
	case "begin":
		return Op{Kind: "begin"}
	case "commit":
		return Op{Kind: "commit"}
	case "rollback":
		return Op{Kind: "rollback"}
	case "invalid":
		return Op{Kind: "invalid", SQL: opSpec.SQL}
	case "param":
		return Op{
			Kind:   "param",
			SQL:    opSpec.SQL,
			Params: []any{genValue(ht), genValue(ht)},
		}
	case "create_dynamic":
		table := genTableName(ht, prefix)
		cols := genDynamicCols(ht)
		tableCols[table] = len(cols)
		return Op{
			Kind:  "create_dynamic",
			Table: table,
			Cols:  cols,
		}
	case "insert_returning":
		table := genTableName(ht, prefix)
		return Op{
			Kind:   "insert_returning",
			Table:  table,
			Values: genValuesForTable(ht, table, tableCols),
		}
	case "delete_returning":
		return Op{Kind: "delete_returning", Table: genTableName(ht, prefix)}
	case "update_returning":
		return Op{Kind: "update_returning", Table: genTableName(ht, prefix), Value: genValue(ht)}
	case "named_param":
		named := make([]struct {
			Name  string
			Value any
		}, len(opSpec.NamedParams))
		for i, name := range opSpec.NamedParams {
			named[i] = struct {
				Name  string
				Value any
			}{name, genValue(ht)}
		}
		return Op{Kind: "named_param", NamedParams: named}
	case "numbered_param":
		return Op{
			Kind:   "numbered_param",
			Params: []any{genValue(ht), genValue(ht)},
		}
	case "insert_affected":
		table := genTableName(ht, prefix)
		return Op{
			Kind:   "insert_affected",
			Table:  table,
			Values: genValuesForTable(ht, table, tableCols),
		}
	case "delete_affected":
		return Op{Kind: "delete_affected", Table: genTableName(ht, prefix)}
	case "update_affected":
		return Op{Kind: "update_affected", Table: genTableName(ht, prefix), Value: genValue(ht)}
	case "insert_rowid":
		table := genTableName(ht, prefix)
		return Op{
			Kind:   "insert_rowid",
			Table:  table,
			Values: genValuesForTable(ht, table, tableCols),
		}
	case "batch":
		return Op{Kind: "batch", SQL: genBatchSQL(ht, prefix)}
	case "select_limit":
		return Op{Kind: "select_limit", Table: genTableName(ht, prefix)}
	case "select_count":
		return Op{Kind: "select_count", Table: genTableName(ht, prefix)}
	case "select_expr":
		return Op{Kind: "select_expr", Value: genValue(ht)}
	case "error_check":
		return Op{Kind: "error_check", SQL: genErrorSQL(ht, prefix)}
	case "prepared_reuse":
		return Op{
			Kind: "prepared_reuse",
			ParamsSets: [][]any{
				{genValue(ht), genValue(ht)},
				{genValue(ht), genValue(ht)},
				{genValue(ht), genValue(ht)},
			},
		}
	case "create_trigger":
		return Op{
			Kind:  "create_trigger",
			Table: genTableName(ht, prefix),
		}
	default:
		panic(fmt.Sprintf("unknown op id in ops.json: %s", opSpec.ID))
	}
}

// ---------------------------------------------------------------------------
// Execute operations — same adapter for both drivers via database/sql
// ---------------------------------------------------------------------------

// placeholders returns a string like "?, ?, ?" for n parameters.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	p := make([]string, n)
	for i := range p {
		p[i] = "?"
	}
	return strings.Join(p, ", ")
}

func executeOp(db *sql.DB, op Op) OpResult {
	switch op.Kind {
	case "create":
		s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (a INTEGER, b TEXT)", op.Table)
		_, err := db.Exec(s)
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, RowCount: 0}

	case "create_dynamic":
		var colDefs []string
		for _, c := range op.Cols {
			colDefs = append(colDefs, fmt.Sprintf("%s %s", c.Name, c.Type))
		}
		s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", op.Table, strings.Join(colDefs, ", "))
		_, err := db.Exec(s)
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, RowCount: 0}

	case "insert":
		s := fmt.Sprintf("INSERT INTO %s VALUES (%s)", op.Table, placeholders(len(op.Values)))
		_, err := db.Exec(s, op.Values...)
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, RowCount: 1}

	case "insert_returning":
		s := fmt.Sprintf("INSERT INTO %s VALUES (%s) RETURNING *", op.Table, placeholders(len(op.Values)))
		return queryResultWithParams(db, s, op.Values)

	case "delete_returning":
		s := fmt.Sprintf("DELETE FROM %s RETURNING *", op.Table)
		return queryResult(db, s)

	case "update_returning":
		s := fmt.Sprintf("UPDATE %s SET a = ? RETURNING *", op.Table)
		return queryResultWithParams(db, s, []any{op.Value})

	case "select":
		s := fmt.Sprintf("SELECT * FROM %s", op.Table)
		return queryResult(db, s)

	case "select_value":
		s := fmt.Sprintf("SELECT %s", op.Expr)
		return queryResult(db, s)

	case "begin":
		_, err := db.Exec("BEGIN")
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, RowCount: 0}

	case "commit":
		_, err := db.Exec("COMMIT")
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, RowCount: 0}

	case "rollback":
		_, err := db.Exec("ROLLBACK")
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, RowCount: 0}

	case "invalid":
		return queryResult(db, op.SQL)

	case "param":
		return queryResultWithParams(db, op.SQL, op.Params)

	case "insert_affected":
		s := fmt.Sprintf("INSERT INTO %s VALUES (%s)", op.Table, placeholders(len(op.Values)))
		return execAffected(db, s, op.Values)

	case "delete_affected":
		s := fmt.Sprintf("DELETE FROM %s", op.Table)
		return execAffected(db, s, nil)

	case "update_affected":
		s := fmt.Sprintf("UPDATE %s SET a = ?", op.Table)
		return execAffected(db, s, []any{op.Value})

	case "insert_rowid":
		s := fmt.Sprintf("INSERT INTO %s VALUES (%s)", op.Table, placeholders(len(op.Values)))
		result, err := db.Exec(s, op.Values...)
		if err != nil {
			return OpResult{Success: false}
		}
		rowid, err := result.LastInsertId()
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, LastInsertRowid: rowid, HasRowid: true}

	case "batch":
		_, err := db.Exec(op.SQL)
		if err != nil {
			return OpResult{Success: false}
		}
		return OpResult{Success: true, RowCount: 0}

	case "select_limit":
		s := fmt.Sprintf("SELECT * FROM %s LIMIT 1", op.Table)
		return queryResult(db, s)

	case "select_count":
		s := fmt.Sprintf("SELECT COUNT(*), SUM(a) FROM %s", op.Table)
		return queryResult(db, s)

	case "select_expr":
		s := "SELECT 1+1, 'hello'||'world', NULL, CAST(3.14 AS INTEGER), typeof(?)"
		return queryResultWithParams(db, s, []any{op.Value})

	case "error_check":
		_, err := db.Exec(op.SQL)
		return OpResult{Success: err == nil}

	case "prepared_reuse":
		stmt, err := db.Prepare("SELECT ?, ?")
		if err != nil {
			return OpResult{Success: false}
		}
		defer stmt.Close()

		var allTypes [][]string
		var allValues [][]any
		for _, params := range op.ParamsSets {
			rows, err := stmt.Query(params...)
			if err != nil {
				return OpResult{Success: false}
			}
			cols, err := rows.Columns()
			if err != nil {
				rows.Close()
				return OpResult{Success: false}
			}
			for rows.Next() {
				values := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range values {
					ptrs[i] = &values[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					rows.Close()
					return OpResult{Success: false}
				}
				types := make([]string, len(cols))
				rowVals := make([]any, len(cols))
				for i, v := range values {
					types[i] = goValueType(v)
					rowVals[i] = v
				}
				allTypes = append(allTypes, types)
				allValues = append(allValues, rowVals)
			}
			rows.Close()
		}
		return OpResult{
			Success:     true,
			ColumnCount: 2,
			ColumnNames: []string{"?", "?"},
			RowCount:    len(allValues),
			ValueTypes:  allTypes,
			Values:      allValues,
		}

	case "named_param":
		var cols []string
		var args []any
		for _, np := range op.NamedParams {
			cols = append(cols, fmt.Sprintf(":%s", np.Name))
			// The local driver's FFI expects the colon prefix in the name.
			args = append(args, sql.Named(":"+np.Name, np.Value))
		}
		s := fmt.Sprintf("SELECT %s", strings.Join(cols, ", "))
		return queryResultWithParams(db, s, args)

	case "numbered_param":
		var cols []string
		for i := range op.Params {
			cols = append(cols, fmt.Sprintf("?%d", i+1))
		}
		s := fmt.Sprintf("SELECT %s", strings.Join(cols, ", "))
		return queryResultWithParams(db, s, op.Params)

	case "create_trigger":
		// Create audit table, create trigger with BEGIN...END (internal semicolons),
		// fire the trigger, then read the audit log.
		audit := op.Table + "_audit"
		db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (src TEXT, val)", audit))
		triggerSQL := fmt.Sprintf(
			"CREATE TRIGGER IF NOT EXISTS tr_%s_ins AFTER INSERT ON %s "+
				"BEGIN "+
				"INSERT INTO %s VALUES ('%s', NEW.a); "+
				"INSERT INTO %s VALUES ('%s', NEW.a * 2); "+
				"END",
			op.Table, op.Table, audit, op.Table, audit, op.Table)
		_, err := db.Exec(triggerSQL)
		if err != nil {
			return OpResult{Success: false}
		}
		db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (42, 'trigger_test')", op.Table))
		return queryResult(db, fmt.Sprintf("SELECT * FROM %s ORDER BY rowid", audit))

	default:
		return OpResult{Success: false}
	}
}

func execAffected(db *sql.DB, query string, params []any) OpResult {
	result, err := db.Exec(query, params...)
	if err != nil {
		return OpResult{Success: false}
	}
	n, err := result.RowsAffected()
	if err != nil {
		return OpResult{Success: false}
	}
	return OpResult{Success: true, AffectedRows: n, HasAffected: true}
}

func queryResult(db *sql.DB, query string) OpResult {
	return queryResultWithParams(db, query, nil)
}

func queryResultWithParams(db *sql.DB, query string, params []any) OpResult {
	rows, err := db.Query(query, params...)
	if err != nil {
		return OpResult{Success: false}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return OpResult{Success: false}
	}

	var allTypes [][]string
	var allValues [][]any
	rowCount := 0

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return OpResult{Success: false}
		}

		types := make([]string, len(cols))
		rowVals := make([]any, len(cols))
		for i, v := range values {
			types[i] = goValueType(v)
			rowVals[i] = v
		}
		allTypes = append(allTypes, types)
		allValues = append(allValues, rowVals)
		rowCount++
	}

	return OpResult{
		Success:     true,
		ColumnCount: len(cols),
		ColumnNames: cols,
		RowCount:    rowCount,
		ValueTypes:  allTypes,
		Values:      allValues,
	}
}

func goValueType(v any) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case int64:
		return "integer"
	case float64:
		return "real"
	case string:
		return "text"
	case []byte:
		return "blob"
	default:
		return fmt.Sprintf("unknown(%T)", v)
	}
}

// ---------------------------------------------------------------------------
// Connection helpers
// ---------------------------------------------------------------------------

func init() {
	// Use "mixed" strategy: try embedded first, fall back to system library path.
	// Panic here if the native library is missing — these tests require both drivers.
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
// The property test
// ---------------------------------------------------------------------------

func TestAPIParity(t *testing.T) {
	// Delete hypothesis example database to prevent stale cached failures
	// from causing flaky "non-deterministic" errors after test logic changes.
	// The Go hegel API doesn't expose a database=None option.
	os.RemoveAll(".hypothesis")

	t.Run("parity", hegel.Case(func(ht *hegel.T) {
		localDB := openDB(t, "turso")
		defer localDB.Close()
		remoteDB := openDB(t, "turso-serverless")
		defer remoteDB.Close()

		prefix := hegel.Draw(ht, hegel.Integers(0, 65535))

		// Drop any leftover tables for this prefix (from prior runs or Hegel replays).
		for i := 0; i < spec.Constants.NumTables; i++ {
			remoteDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS t_%d_%d", prefix, i))
			remoteDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS t_%d_%d_audit", prefix, i))
			remoteDB.Exec(fmt.Sprintf("DROP TRIGGER IF EXISTS tr_t_%d_%d_ins", prefix, i))
		}

		tableCols := make(map[string]int)
		numOps := hegel.Draw(ht, hegel.Integers(1, spec.Constants.MaxOpsPerCase))

		// Accumulate an operation trace so failures show the full history.
		var trace []string

		for i := 0; i < numOps; i++ {
			op := genOp(ht, tableCols, prefix)

			localResult := executeOp(localDB, op)
			remoteResult := executeOp(remoteDB, op)

			trace = append(trace, fmt.Sprintf(
				"  op[%d]: kind=%s table=%q\n    local:  ok=%v cols=%d rows=%d affected=%d\n    remote: ok=%v cols=%d rows=%d affected=%d",
				i, op.Kind, op.Table,
				localResult.Success, localResult.ColumnCount, localResult.RowCount, localResult.AffectedRows,
				remoteResult.Success, remoteResult.ColumnCount, remoteResult.RowCount, remoteResult.AffectedRows,
			))
			traceDump := strings.Join(trace, "\n")

			// ErrorCheck only compares success/failure -- error messages legitimately differ.
			if op.Kind == "error_check" {
				if localResult.Success != remoteResult.Success {
					ht.Fatalf(
						"Parity violation on op #%d %+v:\n  local.Success:  %v\n  remote.Success: %v\n\nFull trace (prefix=%d):\n%s",
						i, op, localResult.Success, remoteResult.Success, prefix, traceDump,
					)
				}
				continue
			}

			if !resultsEqual(localResult, remoteResult) {
				ht.Fatalf(
					"Parity violation on op #%d %+v:\n  local:  %+v\n  remote: %+v\n\nFull trace (prefix=%d):\n%s",
					i, op, localResult, remoteResult, prefix, traceDump,
				)
			}

			// If both failed, stop — continuing with diverged implicit
			// transaction state leads to false positives.
			if !localResult.Success {
				break
			}
		}
	}))

	// Error recovery property: errors must never prevent subsequent commands
	t.Run("error_recovery", hegel.Case(func(ht *hegel.T) {
		remoteDB2 := openDB(t, "turso-serverless")
		defer remoteDB2.Close()

		prefix := hegel.Draw(ht, hegel.Integers(100000, 165535))
		errorSQL := genErrorSQL(ht, prefix)

		// Send the error-inducing SQL (expected to fail)
		_, _ = remoteDB2.Exec(errorSQL)

		// The critical assertion: SELECT 1 must succeed afterward
		var val int
		err := remoteDB2.QueryRow("SELECT 1").Scan(&val)
		if err != nil {
			ht.Fatalf("SELECT 1 failed after error SQL %q: %v", errorSQL, err)
		}
		if val != 1 {
			ht.Fatalf("SELECT 1 returned %d after error SQL %q", val, errorSQL)
		}
	}))

	// DDL visibility in transactions: CREATE TABLE must be visible to
	// subsequent statements within the same transaction.
	t.Run("ddl_in_transaction", hegel.Case(func(ht *hegel.T) {
		localDB := openDB(t, "turso")
		defer localDB.Close()
		remoteDB := openDB(t, "turso-serverless")
		defer remoteDB.Close()

		prefix := hegel.Draw(ht, hegel.Integers(200000, 265535))
		tableIdx := hegel.Draw(ht, hegel.Integers(0, 5))
		table := fmt.Sprintf("t_%d_%d", prefix, tableIdx)
		val := hegel.Draw(ht, hegel.Integers(-1000, 1000))

		// Drop any leftover from prior runs so the INSERT produces exactly 1 row.
		remoteDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

		for _, tc := range []struct {
			label string
			db    *sql.DB
		}{
			{"local", localDB},
			{"remote", remoteDB},
		} {
			_, err := tc.db.Exec("BEGIN")
			if err != nil {
				ht.Fatalf("%s: BEGIN failed: %v", tc.label, err)
			}

			_, err = tc.db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (a INTEGER, b TEXT)", table))
			if err != nil {
				ht.Fatalf("%s: CREATE TABLE failed: %v", tc.label, err)
			}

			_, err = tc.db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (?, 'txn_ddl')", table), val)
			if err != nil {
				ht.Fatalf("%s: INSERT failed: %v", tc.label, err)
			}

			result := queryResult(tc.db, fmt.Sprintf("SELECT a FROM %s", table))
			if !result.Success {
				ht.Fatalf("%s: SELECT inside txn failed after CREATE+INSERT", tc.label)
			}
			if result.RowCount != 1 {
				ht.Fatalf("%s: expected 1 row, got %d", tc.label, result.RowCount)
			}

			_, err = tc.db.Exec("COMMIT")
			if err != nil {
				ht.Fatalf("%s: COMMIT failed: %v", tc.label, err)
			}
		}
	}))

	// DDL + Prepare() in transactions: Prepare() calls describe which must
	// see tables created earlier in the same transaction (issue #6562).
	t.Run("ddl_prepare_in_transaction", hegel.Case(func(ht *hegel.T) {
		localDB := openDB(t, "turso")
		defer localDB.Close()
		remoteDB := openDB(t, "turso-serverless")
		defer remoteDB.Close()

		prefix := hegel.Draw(ht, hegel.Integers(300000, 365535))
		tableIdx := hegel.Draw(ht, hegel.Integers(0, 5))
		table := fmt.Sprintf("t_%d_%d", prefix, tableIdx)
		val := hegel.Draw(ht, hegel.Integers(-1000, 1000))

		// Drop any leftover from prior runs so the INSERT produces exactly 1 row.
		remoteDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

		for _, tc := range []struct {
			label string
			db    *sql.DB
		}{
			{"local", localDB},
			{"remote", remoteDB},
		} {
			_, err := tc.db.Exec("BEGIN")
			if err != nil {
				ht.Fatalf("%s: BEGIN failed: %v", tc.label, err)
			}

			_, err = tc.db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (a INTEGER, b TEXT)", table))
			if err != nil {
				ht.Fatalf("%s: CREATE TABLE failed: %v", tc.label, err)
			}

			// Use Prepare() instead of Exec() — this exercises the describe
			// path which must see DDL from the current transaction.
			stmt, err := tc.db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?, 'txn_ddl')", table))
			if err != nil {
				ht.Fatalf("%s: Prepare(INSERT) failed after CREATE in same txn: %v", tc.label, err)
			}
			_, err = stmt.Exec(val)
			stmt.Close()
			if err != nil {
				ht.Fatalf("%s: prepared INSERT Exec failed: %v", tc.label, err)
			}

			result := queryResult(tc.db, fmt.Sprintf("SELECT a FROM %s", table))
			if !result.Success {
				ht.Fatalf("%s: SELECT inside txn failed after CREATE+Prepare(INSERT)", tc.label)
			}
			if result.RowCount != 1 {
				ht.Fatalf("%s: expected 1 row, got %d", tc.label, result.RowCount)
			}

			_, err = tc.db.Exec("COMMIT")
			if err != nil {
				ht.Fatalf("%s: COMMIT failed: %v", tc.label, err)
			}
		}
	}))
}

// ---------------------------------------------------------------------------
// API surface parity: local driver.Conn methods must exist on remote
// ---------------------------------------------------------------------------

func TestAPIMethodParity(t *testing.T) {
	localDB := openDB(t, "turso")
	remoteDB := openDB(t, "turso-serverless")
	defer localDB.Close()
	defer remoteDB.Close()

	ctx := context.Background()

	localConn, err := localDB.Conn(ctx)
	if err != nil {
		t.Fatalf("local Conn: %v", err)
	}
	defer localConn.Close()

	remoteConn, err := remoteDB.Conn(ctx)
	if err != nil {
		t.Fatalf("remote Conn: %v", err)
	}
	defer remoteConn.Close()

	// Methods that only make sense on the local driver (FFI, busy timeout).
	localOnly := map[string]bool{
		"GetBusyTimeout": true,
		"SetBusyTimeout": true,
	}

	var localMethods []string
	localConn.Raw(func(dc any) error {
		t := reflect.TypeOf(dc)
		for i := 0; i < t.NumMethod(); i++ {
			localMethods = append(localMethods, t.Method(i).Name)
		}
		return nil
	})

	remoteSet := map[string]bool{}
	remoteConn.Raw(func(dc any) error {
		t := reflect.TypeOf(dc)
		for i := 0; i < t.NumMethod(); i++ {
			remoteSet[t.Method(i).Name] = true
		}
		return nil
	})

	sort.Strings(localMethods)

	var missing []string
	for _, m := range localMethods {
		if !remoteSet[m] && !localOnly[m] {
			missing = append(missing, m)
		}
	}

	if len(missing) > 0 {
		t.Errorf("Remote driver.Conn missing methods present in local driver.Conn: %v", missing)
	}
}
