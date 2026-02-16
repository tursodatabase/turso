// Copyright 2023-2025 the Limbo authors. All rights reserved. MIT license.

use std::sync::Arc;
use turso_ext::*;

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ColumnDef {
    name: String,
    ty: String,
    hidden: bool,
}

#[derive(Clone, Debug, PartialEq)]
enum DuckDbMode {
    Table,
    Query,
}

#[derive(Clone)]
struct DuckDbConfig {
    database: String,
    table: Option<String>,
    query: Option<String>,
    schema_name: String,
    column_defs: Vec<ColumnDef>,
    mode: DuckDbMode,
}

// ---------------------------------------------------------------------------
// Column definition parsing (shared with HTTP extension pattern)
// ---------------------------------------------------------------------------

/// Parse "id INTEGER, name TEXT, city TEXT HIDDEN" into Vec<ColumnDef>
fn parse_column_defs(s: &str) -> Result<Vec<ColumnDef>, ResultCode> {
    if s.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut defs = Vec::new();
    for part in s.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(ResultCode::InvalidArgs);
        }
        let name = tokens[0].to_string();
        let ty = tokens[1].to_uppercase();
        let hidden = tokens.len() > 2 && tokens[2].eq_ignore_ascii_case("HIDDEN");
        defs.push(ColumnDef { name, ty, hidden });
    }
    Ok(defs)
}

/// Generate CREATE TABLE x (...) schema string with HIDDEN markers in the type.
fn column_defs_to_create_table_sql(column_defs: &[ColumnDef]) -> String {
    let cols: Vec<String> = column_defs
        .iter()
        .map(|c| {
            if c.hidden {
                format!("{} {} HIDDEN", c.name, c.ty)
            } else {
                format!("{} {}", c.name, c.ty)
            }
        })
        .collect();
    format!("CREATE TABLE x ({})", cols.join(", "))
}

// ---------------------------------------------------------------------------
// DuckDbConfig parsing
// ---------------------------------------------------------------------------

impl DuckDbConfig {
    fn parse(args: &[Value]) -> Result<Self, ResultCode> {
        let mut database = None;
        let mut table = None;
        let mut query = None;
        let mut schema_name = "main".to_string();
        let mut columns_str = None;

        for arg in args {
            let text = arg.to_text().ok_or(ResultCode::InvalidArgs)?;
            let parts: Vec<&str> = text.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(ResultCode::InvalidArgs);
            }
            let (key, value) = (parts[0].trim(), parts[1].trim());
            match key {
                "database" => database = Some(value.to_string()),
                "table" => table = Some(value.to_string()),
                "query" => query = Some(value.to_string()),
                "schema" => schema_name = value.to_string(),
                "columns" => columns_str = Some(value.to_string()),
                _ => return Err(ResultCode::InvalidArgs),
            }
        }

        let database = database.ok_or(ResultCode::InvalidArgs)?;
        if table.is_none() && query.is_none() {
            return Err(ResultCode::InvalidArgs);
        }

        let mode = if table.is_some() {
            DuckDbMode::Table
        } else {
            DuckDbMode::Query
        };
        let column_defs = parse_column_defs(columns_str.as_deref().unwrap_or(""))?;

        Ok(DuckDbConfig {
            database,
            table,
            query,
            schema_name,
            column_defs,
            mode,
        })
    }

    fn to_create_table_sql(&self) -> String {
        column_defs_to_create_table_sql(&self.column_defs)
    }
}

// ---------------------------------------------------------------------------
// CellValue — owned cell for caching DuckDB results (turso_ext::Value is NOT Clone)
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum CellValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl CellValue {
    fn to_ext_value(&self) -> Value {
        match self {
            CellValue::Null => Value::null(),
            CellValue::Integer(i) => Value::from_integer(*i),
            CellValue::Float(f) => Value::from_float(*f),
            CellValue::Text(s) => Value::from_text(s.clone()),
            CellValue::Blob(b) => Value::from_blob(b.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// Auto-schema detection from DuckDB information_schema
// ---------------------------------------------------------------------------

fn introspect_duckdb_schema(
    conn: &duckdb::Connection,
    config: &DuckDbConfig,
) -> Result<(String, Vec<ColumnDef>), ResultCode> {
    let table_name = config.table.as_ref().ok_or(ResultCode::Error)?;
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        )
        .map_err(|_| ResultCode::Error)?;

    let rows = stmt
        .query_map(
            duckdb::params![&config.schema_name, table_name.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|_| ResultCode::Error)?;

    let mut column_defs = Vec::new();
    for row in rows {
        let (col_name, duckdb_type) = row.map_err(|_| ResultCode::Error)?;
        let turso_type = match duckdb_type.to_uppercase().as_str() {
            "BIGINT" | "INTEGER" | "SMALLINT" | "TINYINT" | "BOOLEAN" => "INTEGER",
            "DOUBLE" | "FLOAT" | "DECIMAL" | "REAL" => "REAL",
            "BLOB" => "BLOB",
            _ => "TEXT", // VARCHAR, DATE, TIMESTAMP, LIST, STRUCT, MAP, etc.
        };
        column_defs.push(ColumnDef {
            name: col_name,
            ty: turso_type.to_string(),
            hidden: false,
        });
    }

    if column_defs.is_empty() {
        return Err(ResultCode::Error);
    }

    let schema = column_defs_to_create_table_sql(&column_defs);
    Ok((schema, column_defs))
}

// ---------------------------------------------------------------------------
// Type mapping helpers
// ---------------------------------------------------------------------------

/// Owned parameter value for passing to DuckDB queries.
/// Needed because turso_ext::Value is not Clone and we need owned types for query().
enum OwnedParam {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl duckdb::types::ToSql for OwnedParam {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        match self {
            OwnedParam::Null => duckdb::types::Null.to_sql(),
            OwnedParam::Integer(v) => v.to_sql(),
            OwnedParam::Float(v) => v.to_sql(),
            OwnedParam::Text(v) => v.to_sql(),
            OwnedParam::Blob(v) => v.to_sql(),
        }
    }
}

/// Convert a turso_ext::Value to an OwnedParam for DuckDB binding.
fn turso_value_to_param(value: &Value) -> OwnedParam {
    match value.value_type() {
        ValueType::Integer => OwnedParam::Integer(value.to_integer().unwrap_or(0)),
        ValueType::Float => OwnedParam::Float(value.to_float().unwrap_or(0.0)),
        ValueType::Text => OwnedParam::Text(value.to_text().unwrap_or("").to_string()),
        ValueType::Blob => OwnedParam::Blob(value.to_blob().unwrap_or_default()),
        _ => OwnedParam::Null,
    }
}

/// Fetch DuckDB result rows into Vec<Vec<CellValue>>.
/// When has_rowid is true, column 0 in the result set is the DuckDB rowid
/// pseudocolumn — it's extracted separately into Vec<i64>.
fn fetch_duckdb_rows(
    stmt: &mut duckdb::Statement,
    column_defs: &[ColumnDef],
    has_rowid: bool,
    params: &[&dyn duckdb::types::ToSql],
) -> (Vec<Vec<CellValue>>, Vec<i64>) {
    let mut rows_out = Vec::new();
    let mut row_ids = Vec::new();
    let col_offset: usize = if has_rowid { 1 } else { 0 };

    let mut rows = match stmt.query(params) {
        Ok(r) => r,
        Err(_) => return (Vec::new(), Vec::new()),
    };
    while let Ok(Some(row)) = rows.next() {
        let rid = if has_rowid {
            row.get::<_, i64>(0).unwrap_or(0)
        } else {
            0
        };
        let mut cells = Vec::with_capacity(column_defs.len());
        for (i, col) in column_defs.iter().enumerate() {
            let db_col_idx = i + col_offset;
            let cell = match col.ty.to_uppercase().as_str() {
                "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" => {
                    match row.get::<_, Option<i64>>(db_col_idx) {
                        Ok(Some(v)) => CellValue::Integer(v),
                        _ => CellValue::Null,
                    }
                }
                "BOOLEAN" | "BOOL" => match row.get::<_, Option<bool>>(db_col_idx) {
                    Ok(Some(v)) => CellValue::Integer(v as i64),
                    _ => CellValue::Null,
                },
                "REAL" | "DOUBLE" | "FLOAT" | "DECIMAL" => {
                    match row.get::<_, Option<f64>>(db_col_idx) {
                        Ok(Some(v)) => CellValue::Float(v),
                        _ => CellValue::Null,
                    }
                }
                "BLOB" => match row.get::<_, Option<Vec<u8>>>(db_col_idx) {
                    Ok(Some(v)) => CellValue::Blob(v),
                    _ => CellValue::Null,
                },
                _ => {
                    // TEXT, VARCHAR, DATE, TIMESTAMP, etc.
                    match row.get::<_, Option<String>>(db_col_idx) {
                        Ok(Some(v)) => CellValue::Text(v),
                        _ => CellValue::Null,
                    }
                }
            };
            cells.push(cell);
        }
        row_ids.push(rid);
        rows_out.push(cells);
    }
    (rows_out, row_ids)
}

/// Map a ConstraintOp repr(u8) byte to a SQL operator string.
fn op_byte_to_sql(op: u8) -> Option<&'static str> {
    match op {
        2 => Some("="),
        4 => Some("<"),
        8 => Some("<="),
        16 => Some(">"),
        32 => Some(">="),
        68 => Some("!="),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// VTabModule: DuckDbVTabModule
// ---------------------------------------------------------------------------

#[derive(Debug, VTabModuleDerive, Default)]
struct DuckDbVTabModule;

impl VTabModule for DuckDbVTabModule {
    type Table = DuckDbTable;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "duckdb";
    const READONLY: bool = false;

    fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let config = DuckDbConfig::parse(args)?;
        let conn = duckdb::Connection::open(&config.database).map_err(|_| ResultCode::Error)?;

        // Auto-detect schema if columns not specified and table mode
        let (schema_sql, column_defs) = if config.column_defs.is_empty() {
            introspect_duckdb_schema(&conn, &config)?
        } else {
            (config.to_create_table_sql(), config.column_defs.clone())
        };

        let mut final_config = config;
        final_config.column_defs = column_defs;

        Ok((
            schema_sql,
            DuckDbTable {
                config: final_config,
                conn,
            },
        ))
    }
}

// ---------------------------------------------------------------------------
// VTable: DuckDbTable
// ---------------------------------------------------------------------------

struct DuckDbTable {
    config: DuckDbConfig,
    conn: duckdb::Connection,
}

impl VTable for DuckDbTable {
    type Cursor = DuckDbCursor;
    type Error = String;

    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
        Ok(DuckDbCursor {
            config: self.config.clone(),
            // Share connection via raw pointer (DuckDB Connection is not Clone).
            // Safe: cursor lifetime <= table lifetime.
            conn_ptr: &self.conn as *const duckdb::Connection,
            rows: Vec::new(),
            row_ids: Vec::new(),
            current_row: 0,
        })
    }

    // Static method — accept comparison constraints for pushdown.
    // Only Eq, Lt, Le, Gt, Ge, Ne are passed by the optimizer.
    fn best_index(
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let mut usages = Vec::with_capacity(constraints.len());
        let mut consumed = Vec::new();
        let mut next_argv = 1u32;

        for c in constraints {
            if c.usable
                && matches!(
                    c.op,
                    ConstraintOp::Eq
                        | ConstraintOp::Lt
                        | ConstraintOp::Le
                        | ConstraintOp::Gt
                        | ConstraintOp::Ge
                        | ConstraintOp::Ne
                )
            {
                usages.push(ConstraintUsage {
                    argv_index: Some(next_argv),
                    omit: true,
                });
                consumed.push((c.column_index, c.op as u8));
                next_argv += 1;
            } else {
                usages.push(ConstraintUsage {
                    argv_index: None,
                    omit: false,
                });
            }
        }

        // Encode column_index:op pairs in idx_str.
        // Format: "0:2,3:16" = column 0 with Eq(2), column 3 with Gt(16).
        let idx_str = if consumed.is_empty() {
            None
        } else {
            Some(
                consumed
                    .iter()
                    .map(|(col, op)| format!("{col}:{op}"))
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };

        Ok(IndexInfo {
            idx_num: 0,
            idx_str,
            order_by_consumed: false,
            estimated_cost: if consumed.is_empty() {
                1_000_000.0
            } else {
                1000.0
            },
            estimated_rows: if consumed.is_empty() { u32::MAX } else { 1000 },
            constraint_usages: usages,
        })
    }

    fn insert(&mut self, args: &[Value]) -> Result<i64, Self::Error> {
        if self.config.mode != DuckDbMode::Table {
            return Err("INSERT not supported in query mode".into());
        }
        let table_name = self.config.table.as_ref().unwrap();
        let placeholders = (0..args.len()).map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("INSERT INTO {table_name} VALUES ({placeholders})");
        let owned_params: Vec<OwnedParam> = args.iter().map(turso_value_to_param).collect();
        let param_refs: Vec<&dyn duckdb::types::ToSql> = owned_params
            .iter()
            .map(|p| p as &dyn duckdb::types::ToSql)
            .collect();
        self.conn
            .execute(&sql, param_refs.as_slice())
            .map_err(|e| e.to_string())?;
        // DuckDB doesn't support last_insert_rowid() or RETURNING rowid on pseudo-column.
        // Return a sequential ID; cursor.rowid() returns actual DuckDB rowid for UPDATE/DELETE.
        Ok(0)
    }

    fn delete(&mut self, rowid: i64) -> Result<(), Self::Error> {
        if self.config.mode != DuckDbMode::Table {
            return Err("DELETE not supported in query mode".into());
        }
        let table_name = self.config.table.as_ref().unwrap();
        let sql = format!("DELETE FROM {table_name} WHERE rowid = ?");
        self.conn
            .execute(&sql, [rowid])
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn update(&mut self, rowid: i64, args: &[Value]) -> Result<(), Self::Error> {
        if self.config.mode != DuckDbMode::Table {
            return Err("UPDATE not supported in query mode".into());
        }
        let table_name = self.config.table.as_ref().unwrap();
        let sets: Vec<String> = self
            .config
            .column_defs
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{} = ?{}", col.name, i + 1))
            .collect();
        let rowid_param_idx = args.len() + 1;
        let sql = format!(
            "UPDATE {table_name} SET {} WHERE rowid = ?{rowid_param_idx}",
            sets.join(", "),
        );
        let mut owned_params: Vec<OwnedParam> = args.iter().map(turso_value_to_param).collect();
        owned_params.push(OwnedParam::Integer(rowid));
        let param_refs: Vec<&dyn duckdb::types::ToSql> = owned_params
            .iter()
            .map(|p| p as &dyn duckdb::types::ToSql)
            .collect();
        self.conn
            .execute(&sql, param_refs.as_slice())
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// VTabCursor: DuckDbCursor
// ---------------------------------------------------------------------------

struct DuckDbCursor {
    config: DuckDbConfig,
    conn_ptr: *const duckdb::Connection,
    rows: Vec<Vec<CellValue>>,
    row_ids: Vec<i64>,
    current_row: usize,
}

// SAFETY: DuckDB connection is thread-safe.
unsafe impl Send for DuckDbCursor {}
unsafe impl Sync for DuckDbCursor {}

impl VTabCursor for DuckDbCursor {
    type Error = String;

    fn filter(&mut self, args: &[Value], idx_info: Option<(&str, i32)>) -> ResultCode {
        let conn = unsafe { &*self.conn_ptr };

        let sql = match self.config.mode {
            DuckDbMode::Query => {
                // Fixed query mode — no pushdown
                self.config.query.clone().unwrap()
            }
            DuckDbMode::Table => {
                // Build SELECT with WHERE from pushed constraints.
                // Include rowid for UPDATE/DELETE support.
                let cols = self
                    .config
                    .column_defs
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let table = self.config.table.as_ref().unwrap();
                let mut sql = format!("SELECT rowid, {cols} FROM {table}");

                // Decode idx_str: "0:2,3:16" -> [(col0, Eq), (col3, Gt)]
                if let Some((idx_str, _)) = idx_info {
                    let clauses: Vec<String> = idx_str
                        .split(',')
                        .enumerate()
                        .filter_map(|(i, part)| {
                            let parts: Vec<&str> = part.split(':').collect();
                            if parts.len() != 2 {
                                return None;
                            }
                            let col_idx: usize = parts[0].parse().ok()?;
                            let op: u8 = parts[1].parse().ok()?;
                            let col_name = &self.config.column_defs[col_idx].name;
                            let op_str = op_byte_to_sql(op)?;
                            Some(format!("{col_name} {op_str} ?{}", i + 1))
                        })
                        .collect();
                    if !clauses.is_empty() {
                        sql.push_str(" WHERE ");
                        sql.push_str(&clauses.join(" AND "));
                    }
                }
                sql
            }
        };

        // Execute DuckDB query with bound parameters
        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(_) => {
                self.rows.clear();
                return ResultCode::EOF;
            }
        };

        // Convert args to owned params for query()
        let owned_params: Vec<OwnedParam> = args.iter().map(turso_value_to_param).collect();
        let param_refs: Vec<&dyn duckdb::types::ToSql> = owned_params
            .iter()
            .map(|p| p as &dyn duckdb::types::ToSql)
            .collect();

        let has_rowid = matches!(self.config.mode, DuckDbMode::Table);
        let (rows, row_ids) = fetch_duckdb_rows(
            &mut stmt,
            &self.config.column_defs,
            has_rowid,
            param_refs.as_slice(),
        );
        self.rows = rows;
        self.row_ids = row_ids;
        self.current_row = 0;

        if self.rows.is_empty() {
            ResultCode::EOF
        } else {
            ResultCode::OK
        }
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        self.rows
            .get(self.current_row)
            .and_then(|row| row.get(idx as usize))
            .map(|cell| cell.to_ext_value())
            .ok_or_else(|| "column out of bounds".to_string())
    }

    fn eof(&self) -> bool {
        self.current_row >= self.rows.len()
    }

    fn next(&mut self) -> ResultCode {
        self.current_row += 1;
        if self.current_row >= self.rows.len() {
            ResultCode::EOF
        } else {
            ResultCode::OK
        }
    }

    fn rowid(&self) -> i64 {
        // Return actual DuckDB rowid (NOT sequential index).
        // Used by the engine for UPDATE/DELETE via RowId instruction.
        self.row_ids
            .get(self.current_row)
            .copied()
            .unwrap_or(self.current_row as i64)
    }
}

// ---------------------------------------------------------------------------
// Extension registration
// ---------------------------------------------------------------------------

register_extension! {
    vtabs: { DuckDbVTabModule }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Config parsing tests ----

    #[test]
    fn test_parse_duckdb_config_table_mode() {
        let args = vec![
            Value::from_text("database=/tmp/test.duckdb".into()),
            Value::from_text("table=sales".into()),
            Value::from_text("columns=id INTEGER, product TEXT, amount REAL".into()),
        ];
        let config = DuckDbConfig::parse(&args).unwrap();
        assert_eq!(config.database, "/tmp/test.duckdb");
        assert_eq!(config.table, Some("sales".to_string()));
        assert_eq!(config.mode, DuckDbMode::Table);
        assert_eq!(config.column_defs.len(), 3);
    }

    #[test]
    fn test_parse_duckdb_config_query_mode() {
        let args = vec![
            Value::from_text("database=:memory:".into()),
            Value::from_text("query=SELECT 1 AS n, 'hello' AS msg".into()),
            Value::from_text("columns=n INTEGER, msg TEXT".into()),
        ];
        let config = DuckDbConfig::parse(&args).unwrap();
        assert_eq!(config.mode, DuckDbMode::Query);
        assert!(config.query.is_some());
        assert!(config.table.is_none());
    }

    #[test]
    fn test_parse_duckdb_config_missing_database_fails() {
        let args = vec![Value::from_text("table=sales".into())];
        assert!(DuckDbConfig::parse(&args).is_err());
    }

    #[test]
    fn test_parse_duckdb_config_missing_table_and_query_fails() {
        let args = vec![Value::from_text("database=:memory:".into())];
        assert!(DuckDbConfig::parse(&args).is_err());
    }

    #[test]
    fn test_parse_duckdb_config_with_schema() {
        let args = vec![
            Value::from_text("database=/tmp/test.duckdb".into()),
            Value::from_text("table=sales".into()),
            Value::from_text("schema=analytics".into()),
            Value::from_text("columns=id INTEGER".into()),
        ];
        let config = DuckDbConfig::parse(&args).unwrap();
        assert_eq!(config.schema_name, "analytics");
    }

    // ---- CellValue tests ----

    #[test]
    fn test_cell_value_integer() {
        let cell = CellValue::Integer(42);
        let val = cell.to_ext_value();
        assert_eq!(val.to_integer(), Some(42));
    }

    #[test]
    fn test_cell_value_float() {
        let cell = CellValue::Float(99.5);
        let val = cell.to_ext_value();
        assert!((val.to_float().unwrap() - 99.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cell_value_text() {
        let cell = CellValue::Text("hello".to_string());
        let val = cell.to_ext_value();
        assert_eq!(val.to_text(), Some("hello"));
    }

    #[test]
    fn test_cell_value_null() {
        let cell = CellValue::Null;
        let val = cell.to_ext_value();
        assert_eq!(val.value_type(), ValueType::Null);
    }

    #[test]
    fn test_cell_value_blob() {
        let cell = CellValue::Blob(vec![1, 2, 3]);
        let val = cell.to_ext_value();
        assert_eq!(val.to_blob(), Some(vec![1, 2, 3]));
    }

    // ---- best_index tests ----

    #[test]
    fn test_best_index_eq_constraint() {
        let constraints = vec![ConstraintInfo {
            column_index: 0,
            op: ConstraintOp::Eq,
            usable: true,
            index: 0,
        }];
        let result = DuckDbTable::best_index(&constraints, &[]).unwrap();
        assert_eq!(result.idx_str, Some("0:2".to_string()));
        assert_eq!(result.constraint_usages[0].argv_index, Some(1));
        assert!(result.constraint_usages[0].omit);
    }

    #[test]
    fn test_best_index_mixed_constraints() {
        let constraints = vec![
            ConstraintInfo {
                column_index: 0,
                op: ConstraintOp::Eq,
                usable: true,
                index: 0,
            },
            ConstraintInfo {
                column_index: 3,
                op: ConstraintOp::Gt,
                usable: true,
                index: 1,
            },
        ];
        let result = DuckDbTable::best_index(&constraints, &[]).unwrap();
        assert_eq!(result.idx_str, Some("0:2,3:16".to_string()));
        assert_eq!(result.constraint_usages[0].argv_index, Some(1));
        assert_eq!(result.constraint_usages[1].argv_index, Some(2));
    }

    #[test]
    fn test_best_index_no_constraints() {
        let result = DuckDbTable::best_index(&[], &[]).unwrap();
        assert_eq!(result.idx_str, None);
        assert_eq!(result.estimated_cost, 1_000_000.0);
    }

    #[test]
    fn test_best_index_skips_like() {
        let constraints = vec![ConstraintInfo {
            column_index: 0,
            op: ConstraintOp::Like,
            usable: true,
            index: 0,
        }];
        let result = DuckDbTable::best_index(&constraints, &[]).unwrap();
        assert_eq!(result.idx_str, None);
        assert_eq!(result.constraint_usages[0].argv_index, None);
    }

    #[test]
    fn test_best_index_all_comparison_ops() {
        let constraints = vec![
            ConstraintInfo {
                column_index: 0,
                op: ConstraintOp::Eq,
                usable: true,
                index: 0,
            },
            ConstraintInfo {
                column_index: 1,
                op: ConstraintOp::Lt,
                usable: true,
                index: 1,
            },
            ConstraintInfo {
                column_index: 2,
                op: ConstraintOp::Le,
                usable: true,
                index: 2,
            },
            ConstraintInfo {
                column_index: 3,
                op: ConstraintOp::Gt,
                usable: true,
                index: 3,
            },
            ConstraintInfo {
                column_index: 4,
                op: ConstraintOp::Ge,
                usable: true,
                index: 4,
            },
            ConstraintInfo {
                column_index: 5,
                op: ConstraintOp::Ne,
                usable: true,
                index: 5,
            },
        ];
        let result = DuckDbTable::best_index(&constraints, &[]).unwrap();
        assert_eq!(
            result.idx_str,
            Some("0:2,1:4,2:8,3:16,4:32,5:68".to_string())
        );
        // All 6 should have argv_index
        for (i, usage) in result.constraint_usages.iter().enumerate() {
            assert_eq!(usage.argv_index, Some((i + 1) as u32));
            assert!(usage.omit);
        }
    }

    // ---- op_byte_to_sql tests ----

    #[test]
    fn test_op_byte_mappings() {
        assert_eq!(op_byte_to_sql(2), Some("="));
        assert_eq!(op_byte_to_sql(4), Some("<"));
        assert_eq!(op_byte_to_sql(8), Some("<="));
        assert_eq!(op_byte_to_sql(16), Some(">"));
        assert_eq!(op_byte_to_sql(32), Some(">="));
        assert_eq!(op_byte_to_sql(68), Some("!="));
        assert_eq!(op_byte_to_sql(99), None);
    }

    // ---- Schema generation tests ----

    #[test]
    fn test_create_table_sql_generation() {
        let defs = vec![
            ColumnDef {
                name: "id".into(),
                ty: "INTEGER".into(),
                hidden: false,
            },
            ColumnDef {
                name: "name".into(),
                ty: "TEXT".into(),
                hidden: false,
            },
        ];
        assert_eq!(
            column_defs_to_create_table_sql(&defs),
            "CREATE TABLE x (id INTEGER, name TEXT)"
        );
    }

    // ---- DuckDB integration tests (use in-memory DB) ----

    #[test]
    fn test_duckdb_auto_schema_detection() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR, amount DOUBLE, active BOOLEAN)",
        )
        .unwrap();

        let config = DuckDbConfig {
            database: ":memory:".into(),
            table: Some("test_table".into()),
            query: None,
            schema_name: "main".into(),
            column_defs: Vec::new(),
            mode: DuckDbMode::Table,
        };

        let (schema, defs) = introspect_duckdb_schema(&conn, &config).unwrap();
        assert_eq!(defs.len(), 4);
        assert_eq!(defs[0].name, "id");
        assert_eq!(defs[0].ty, "INTEGER");
        assert_eq!(defs[1].name, "name");
        assert_eq!(defs[1].ty, "TEXT");
        assert_eq!(defs[2].name, "amount");
        assert_eq!(defs[2].ty, "REAL");
        assert_eq!(defs[3].name, "active");
        assert_eq!(defs[3].ty, "INTEGER"); // BOOLEAN maps to INTEGER
        assert!(schema.contains("id INTEGER"));
    }

    #[test]
    fn test_duckdb_fetch_rows() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE test_data (id INTEGER, name VARCHAR, amount DOUBLE);
             INSERT INTO test_data VALUES (1, 'Alice', 9.99);
             INSERT INTO test_data VALUES (2, 'Bob', 24.99);",
        )
        .unwrap();

        let column_defs = vec![
            ColumnDef {
                name: "id".into(),
                ty: "INTEGER".into(),
                hidden: false,
            },
            ColumnDef {
                name: "name".into(),
                ty: "TEXT".into(),
                hidden: false,
            },
            ColumnDef {
                name: "amount".into(),
                ty: "REAL".into(),
                hidden: false,
            },
        ];

        let mut stmt = conn
            .prepare("SELECT rowid, id, name, amount FROM test_data ORDER BY id")
            .unwrap();
        let (rows, row_ids) = fetch_duckdb_rows(&mut stmt, &column_defs, true, &[]);

        assert_eq!(rows.len(), 2);
        assert_eq!(row_ids.len(), 2);

        // Check first row
        match &rows[0][0] {
            CellValue::Integer(v) => assert_eq!(*v, 1),
            _ => panic!("expected Integer"),
        }
        match &rows[0][1] {
            CellValue::Text(v) => assert_eq!(v, "Alice"),
            _ => panic!("expected Text"),
        }
        match &rows[0][2] {
            CellValue::Float(v) => assert!((v - 9.99).abs() < 0.001),
            _ => panic!("expected Float"),
        }

        // Check second row
        match &rows[1][0] {
            CellValue::Integer(v) => assert_eq!(*v, 2),
            _ => panic!("expected Integer"),
        }
    }

    #[test]
    fn test_duckdb_fetch_rows_with_nulls() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE test_nulls (id INTEGER, name VARCHAR);
             INSERT INTO test_nulls VALUES (1, NULL);",
        )
        .unwrap();

        let column_defs = vec![
            ColumnDef {
                name: "id".into(),
                ty: "INTEGER".into(),
                hidden: false,
            },
            ColumnDef {
                name: "name".into(),
                ty: "TEXT".into(),
                hidden: false,
            },
        ];

        let mut stmt = conn
            .prepare("SELECT rowid, id, name FROM test_nulls")
            .unwrap();
        let (rows, _) = fetch_duckdb_rows(&mut stmt, &column_defs, true, &[]);

        assert_eq!(rows.len(), 1);
        match &rows[0][1] {
            CellValue::Null => {} // expected
            _ => panic!("expected Null"),
        }
    }

    #[test]
    fn test_duckdb_insert_and_query() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test_insert (id INTEGER, name VARCHAR)")
            .unwrap();

        conn.execute(
            "INSERT INTO test_insert VALUES (?, ?)",
            duckdb::params![42, "test"],
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test_insert", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let name: String = conn
            .query_row("SELECT name FROM test_insert WHERE id = ?", [42], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(name, "test");
    }

    #[test]
    fn test_duckdb_delete_by_rowid() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE test_delete (id INTEGER, name VARCHAR);
             INSERT INTO test_delete VALUES (1, 'Alice');
             INSERT INTO test_delete VALUES (2, 'Bob');",
        )
        .unwrap();

        // Get rowid of first row
        let rowid: i64 = conn
            .query_row("SELECT rowid FROM test_delete WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();

        conn.execute("DELETE FROM test_delete WHERE rowid = ?", [rowid])
            .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test_delete", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_query_mode_rejects_writes() {
        let config = DuckDbConfig {
            database: ":memory:".into(),
            table: None,
            query: Some("SELECT 1".into()),
            schema_name: "main".into(),
            column_defs: vec![],
            mode: DuckDbMode::Query,
        };
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let mut table = DuckDbTable { config, conn };

        let args = vec![Value::from_integer(1)];
        assert!(table.insert(&args).is_err());
        assert!(table.delete(1).is_err());
        assert!(table.update(1, &args).is_err());
    }

    // ---- Column definition tests ----

    #[test]
    fn test_column_defs_parsing() {
        let defs = parse_column_defs("id INTEGER, name TEXT, city TEXT HIDDEN").unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].name, "id");
        assert!(!defs[0].hidden);
        assert_eq!(defs[2].name, "city");
        assert!(defs[2].hidden);
    }

    #[test]
    fn test_empty_column_defs() {
        let defs = parse_column_defs("").unwrap();
        assert!(defs.is_empty());
    }
}
