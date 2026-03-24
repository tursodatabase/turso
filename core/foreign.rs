//! Foreign Data Wrapper (FDW) support for Turso.
//!
//! Provides a trait-based abstraction for querying external data sources through
//! SQL, inspired by PostgreSQL's SQL/MED (Management of External Data) and
//! Steampipe's key column pushdown pattern.
//!
//! # Architecture
//!
//! A `ForeignDataWrapper` implementation provides:
//! - Schema declaration (column names and types)
//! - Key column metadata (which columns support server-side filtering)
//! - Query planning (cost estimation and constraint acceptance)
//! - Row scanning (fetching data from the external source)
//!
//! The adapter [`ForeignTableAdapter`] bridges any `ForeignDataWrapper` to
//! Turso's internal `InternalVirtualTable` interface, making foreign tables
//! queryable with standard SQL including JOINs with local tables.
//!
//! # Query Pushdown
//!
//! Key columns declare which SQL WHERE constraints can be pushed down to the
//! external data source. For example, a GMail FDW might declare that
//! `from_address` supports `Eq` and `Like`, allowing:
//!
//! ```sql
//! SELECT * FROM gmail_email WHERE from_address = 'alice@example.com'
//! ```
//!
//! to translate into a targeted API call rather than fetching all emails.

use crate::sync::{Arc, RwLock};
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor};
use crate::{Connection, LimboError, Result, Value};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

// ============================================================================
// Core FDW traits
// ============================================================================

/// A foreign data wrapper that can fetch rows from an external data source.
///
/// Implementations translate SQL query constraints into external API calls
/// (REST, MCP, gRPC, etc.) and return results as rows of [`Value`]s.
pub trait ForeignDataWrapper: std::fmt::Debug + Send + Sync {
    /// Declare which columns support server-side filtering and which
    /// operators each column accepts.
    ///
    /// Columns not listed here will be filtered client-side by Turso after
    /// the scan returns rows.
    fn key_columns(&self) -> &[KeyColumn];

    /// Schema declaration as a CREATE TABLE statement.
    ///
    /// Example: `"CREATE TABLE gmail_email(msg_id TEXT, subject TEXT, ...)"`
    fn schema_sql(&self) -> String;

    /// Open a new cursor for scanning rows.
    ///
    /// Called once per query execution. The cursor will receive `filter()`
    /// with the pushed-down constraint values before iteration begins.
    fn open_cursor(&self) -> Result<Box<dyn ForeignCursor>>;
}

/// Column-level query pushdown declaration.
///
/// Inspired by [Steampipe's KeyColumn](https://steampipe.io/docs/guides/key-columns):
/// each key column declares which SQL operators the external source can handle
/// server-side, avoiding full scans.
#[derive(Debug, Clone)]
pub struct KeyColumn {
    /// Column name (must match a column in `schema_sql()`).
    pub name: String,
    /// Zero-based column index in the schema.
    pub column_index: u32,
    /// SQL operators this column supports for server-side filtering.
    pub operators: Vec<ConstraintOp>,
    /// If true, queries without this qualifier return an error instead of
    /// attempting a full scan. Use for APIs that cannot enumerate all records
    /// (e.g., a user lookup API that requires a login name).
    pub required: bool,
}

impl KeyColumn {
    pub fn new(name: impl Into<String>, column_index: u32, operators: Vec<ConstraintOp>) -> Self {
        Self {
            name: name.into(),
            column_index,
            operators,
            required: false,
        }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    fn supports(&self, op: ConstraintOp) -> bool {
        self.operators.contains(&op)
    }
}

/// Cursor for iterating over rows returned by a foreign data source.
///
/// The lifecycle is: `filter()` → repeated `next()`/`column()` until `eof()`.
pub trait ForeignCursor: Send + Sync {
    /// Apply pushed-down constraints and begin fetching rows.
    ///
    /// `constraints` contains `(column_index, operator, value)` triples for
    /// each constraint that was accepted during planning. The implementation
    /// should translate these into API parameters and fetch the first batch.
    ///
    /// Returns `true` if there is at least one row, `false` if empty.
    fn filter(&mut self, constraints: &[PushedConstraint]) -> Result<bool>;

    /// Advance to the next row. Returns `true` if a row is available.
    fn next(&mut self) -> Result<bool>;

    /// Return the value for the given column index in the current row.
    fn column(&self, idx: usize) -> Result<Value>;

    /// Return a synthetic row ID for the current row.
    fn rowid(&self) -> i64;
}

/// A constraint that was pushed down to the foreign data source.
#[derive(Debug, Clone)]
pub struct PushedConstraint {
    pub column_index: u32,
    pub op: ConstraintOp,
    pub value: Value,
}

// ============================================================================
// ForeignTableAdapter: bridges FDW → InternalVirtualTable
// ============================================================================

/// Adapts any [`ForeignDataWrapper`] to Turso's [`InternalVirtualTable`] trait.
///
/// This is the glue that makes foreign tables queryable through standard SQL.
/// Register via [`Connection::register_foreign_table()`].
#[derive(Debug)]
pub struct ForeignTableAdapter {
    fdw: Arc<dyn ForeignDataWrapper>,
}

impl ForeignTableAdapter {
    pub fn new(fdw: Arc<dyn ForeignDataWrapper>) -> Self {
        Self { fdw }
    }
}

impl InternalVirtualTable for ForeignTableAdapter {
    fn name(&self) -> String {
        // Extracted from schema_sql() — the adapter doesn't store name separately
        // because the VirtualTable wrapper already handles naming.
        String::new()
    }

    fn sql(&self) -> String {
        self.fdw.schema_sql()
    }

    fn open(&self, _conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        let cursor = self.fdw.open_cursor()?;
        Ok(Arc::new(RwLock::new(ForeignCursorAdapter {
            inner: cursor,
        })))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let key_columns = self.fdw.key_columns();

        // Check required key columns are present
        for kc in key_columns {
            if kc.required {
                let has_usable = constraints
                    .iter()
                    .any(|c| c.usable && c.column_index == kc.column_index && kc.supports(c.op));
                if !has_usable {
                    return Err(ResultCode::ConstraintViolation);
                }
            }
        }

        let mut usages = Vec::with_capacity(constraints.len());
        let mut argv_idx = 1u32;
        let mut estimated_cost = 1_000_000.0f64;
        // Encode which constraints we accepted as a comma-separated string
        // of "column_index:op" pairs, passed to filter via idx_str.
        let mut accepted: Vec<String> = Vec::new();

        for c in constraints {
            let kc = key_columns
                .iter()
                .find(|kc| kc.column_index == c.column_index && c.usable && kc.supports(c.op));

            if let Some(_kc) = kc {
                accepted.push(format!("{}:{}", c.column_index, c.op as u8));
                usages.push(ConstraintUsage {
                    argv_index: Some(argv_idx),
                    omit: true,
                });
                argv_idx += 1;
                estimated_cost /= 10.0;
            } else {
                usages.push(ConstraintUsage {
                    argv_index: None,
                    omit: false,
                });
            }
        }

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: if accepted.is_empty() {
                None
            } else {
                Some(accepted.join(","))
            },
            order_by_consumed: false,
            estimated_cost,
            estimated_rows: 100,
            constraint_usages: usages,
        })
    }
}

// ============================================================================
// ForeignCursorAdapter: bridges ForeignCursor → InternalVirtualTableCursor
// ============================================================================

struct ForeignCursorAdapter {
    inner: Box<dyn ForeignCursor>,
}

// SAFETY: ForeignCursor is required to be Send + Sync.
unsafe impl Send for ForeignCursorAdapter {}
unsafe impl Sync for ForeignCursorAdapter {}

impl InternalVirtualTableCursor for ForeignCursorAdapter {
    fn filter(&mut self, args: &[Value], idx_str: Option<String>, _idx_num: i32) -> Result<bool> {
        // Parse the idx_str to reconstruct which constraints were accepted.
        // Format: "col_idx:op_u8,col_idx:op_u8,..."
        let mut constraints = Vec::new();

        if let Some(ref spec) = idx_str {
            for (i, part) in spec.split(',').enumerate() {
                if i >= args.len() {
                    break;
                }
                let mut parts = part.split(':');
                let col_idx: u32 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
                    LimboError::InternalError(format!("Invalid idx_str segment: {part}"))
                })?;
                let op_u8: u8 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
                    LimboError::InternalError(format!("Invalid idx_str op: {part}"))
                })?;
                let op = constraint_op_from_u8(op_u8).ok_or_else(|| {
                    LimboError::InternalError(format!("Unknown ConstraintOp: {op_u8}"))
                })?;

                constraints.push(PushedConstraint {
                    column_index: col_idx,
                    op,
                    value: args[i].clone(),
                });
            }
        }

        self.inner.filter(&constraints)
    }

    fn next(&mut self) -> Result<bool> {
        self.inner.next()
    }

    fn column(&self, idx: usize) -> Result<Value> {
        self.inner.column(idx)
    }

    fn rowid(&self) -> i64 {
        self.inner.rowid()
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn constraint_op_from_u8(v: u8) -> Option<ConstraintOp> {
    match v {
        2 => Some(ConstraintOp::Eq),
        4 => Some(ConstraintOp::Lt),
        8 => Some(ConstraintOp::Le),
        16 => Some(ConstraintOp::Gt),
        32 => Some(ConstraintOp::Ge),
        64 => Some(ConstraintOp::Match),
        65 => Some(ConstraintOp::Like),
        66 => Some(ConstraintOp::Glob),
        67 => Some(ConstraintOp::Regexp),
        68 => Some(ConstraintOp::Ne),
        69 => Some(ConstraintOp::IsNot),
        70 => Some(ConstraintOp::IsNotNull),
        71 => Some(ConstraintOp::IsNull),
        72 => Some(ConstraintOp::Is),
        73 => Some(ConstraintOp::In),
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial FDW that returns static rows, filtering by equality on column 0.
    #[derive(Debug)]
    struct StaticFdw {
        rows: Vec<Vec<Value>>,
        key_columns: Vec<KeyColumn>,
    }

    impl ForeignDataWrapper for StaticFdw {
        fn key_columns(&self) -> &[KeyColumn] {
            &self.key_columns
        }

        fn schema_sql(&self) -> String {
            "CREATE TABLE test_fdw(id TEXT, name TEXT, score INTEGER)".to_string()
        }

        fn open_cursor(&self) -> Result<Box<dyn ForeignCursor>> {
            Ok(Box::new(StaticCursor {
                rows: self.rows.clone(),
                index: 0,
                started: false,
            }))
        }
    }

    #[derive(Debug)]
    struct StaticCursor {
        rows: Vec<Vec<Value>>,
        index: usize,
        started: bool,
    }

    impl ForeignCursor for StaticCursor {
        fn filter(&mut self, constraints: &[PushedConstraint]) -> Result<bool> {
            // Filter rows by pushed constraints (simple equality only)
            if !constraints.is_empty() {
                self.rows.retain(|row| {
                    constraints.iter().all(|c| {
                        let col = c.column_index as usize;
                        if col < row.len() && c.op == ConstraintOp::Eq {
                            row[col] == c.value
                        } else {
                            true
                        }
                    })
                });
            }
            self.index = 0;
            self.started = true;
            Ok(!self.rows.is_empty())
        }

        fn next(&mut self) -> Result<bool> {
            if !self.started {
                return Ok(false);
            }
            self.index += 1;
            Ok(self.index < self.rows.len())
        }

        fn column(&self, idx: usize) -> Result<Value> {
            let row = &self.rows[self.index];
            if idx < row.len() {
                Ok(row[idx].clone())
            } else {
                Ok(Value::Null)
            }
        }

        fn rowid(&self) -> i64 {
            self.index as i64
        }
    }

    fn make_test_rows() -> Vec<Vec<Value>> {
        vec![
            vec![
                Value::build_text("1"),
                Value::build_text("alice"),
                Value::from_i64(90),
            ],
            vec![
                Value::build_text("2"),
                Value::build_text("bob"),
                Value::from_i64(75),
            ],
            vec![
                Value::build_text("3"),
                Value::build_text("carol"),
                Value::from_i64(85),
            ],
        ]
    }

    #[test]
    fn key_column_supports_operator() {
        let kc = KeyColumn::new("id", 0, vec![ConstraintOp::Eq, ConstraintOp::Like]);
        assert!(kc.supports(ConstraintOp::Eq));
        assert!(kc.supports(ConstraintOp::Like));
        assert!(!kc.supports(ConstraintOp::Gt));
    }

    #[test]
    fn key_column_required() {
        let kc = KeyColumn::new("id", 0, vec![ConstraintOp::Eq]).required();
        assert!(kc.required);
    }

    #[test]
    fn best_index_accepts_matching_constraints() {
        let fdw = Arc::new(StaticFdw {
            rows: make_test_rows(),
            key_columns: vec![KeyColumn::new("id", 0, vec![ConstraintOp::Eq])],
        });
        let adapter = ForeignTableAdapter::new(fdw);

        let constraints = vec![ConstraintInfo {
            column_index: 0,
            op: ConstraintOp::Eq,
            usable: true,
            index: 0,
        }];

        let info = adapter.best_index(&constraints, &[]).unwrap();
        assert_eq!(info.constraint_usages.len(), 1);
        assert_eq!(info.constraint_usages[0].argv_index, Some(1));
        assert!(info.constraint_usages[0].omit);
        assert!(info.estimated_cost < 1_000_000.0);
        assert!(info.idx_str.is_some());
    }

    #[test]
    fn best_index_ignores_unsupported_operator() {
        let fdw = Arc::new(StaticFdw {
            rows: make_test_rows(),
            key_columns: vec![KeyColumn::new("id", 0, vec![ConstraintOp::Eq])],
        });
        let adapter = ForeignTableAdapter::new(fdw);

        let constraints = vec![ConstraintInfo {
            column_index: 0,
            op: ConstraintOp::Gt, // not supported
            usable: true,
            index: 0,
        }];

        let info = adapter.best_index(&constraints, &[]).unwrap();
        assert!(info.constraint_usages[0].argv_index.is_none());
        assert!(!info.constraint_usages[0].omit);
    }

    #[test]
    fn best_index_rejects_missing_required() {
        let fdw = Arc::new(StaticFdw {
            rows: make_test_rows(),
            key_columns: vec![KeyColumn::new("id", 0, vec![ConstraintOp::Eq]).required()],
        });
        let adapter = ForeignTableAdapter::new(fdw);

        // No constraints at all — required "id" is missing
        let result = adapter.best_index(&[], &[]);
        assert_eq!(result.unwrap_err(), ResultCode::ConstraintViolation);
    }

    #[test]
    fn cursor_filter_with_equality() {
        let fdw = StaticFdw {
            rows: make_test_rows(),
            key_columns: vec![KeyColumn::new("id", 0, vec![ConstraintOp::Eq])],
        };

        let mut cursor = fdw.open_cursor().unwrap();
        let has_rows = cursor
            .filter(&[PushedConstraint {
                column_index: 0,
                op: ConstraintOp::Eq,
                value: Value::build_text("2"),
            }])
            .unwrap();

        assert!(has_rows);
        assert_eq!(cursor.column(1).unwrap(), Value::build_text("bob"));
        assert!(!cursor.next().unwrap()); // only one match
    }

    #[test]
    fn cursor_filter_no_match() {
        let fdw = StaticFdw {
            rows: make_test_rows(),
            key_columns: vec![KeyColumn::new("id", 0, vec![ConstraintOp::Eq])],
        };

        let mut cursor = fdw.open_cursor().unwrap();
        let has_rows = cursor
            .filter(&[PushedConstraint {
                column_index: 0,
                op: ConstraintOp::Eq,
                value: Value::build_text("999"),
            }])
            .unwrap();

        assert!(!has_rows);
    }

    #[test]
    fn cursor_unfiltered_returns_all() {
        let fdw = StaticFdw {
            rows: make_test_rows(),
            key_columns: vec![],
        };

        let mut cursor = fdw.open_cursor().unwrap();
        let has_rows = cursor.filter(&[]).unwrap();
        assert!(has_rows);

        // First row
        assert_eq!(cursor.column(1).unwrap(), Value::build_text("alice"));

        // Second row
        assert!(cursor.next().unwrap());
        assert_eq!(cursor.column(1).unwrap(), Value::build_text("bob"));

        // Third row
        assert!(cursor.next().unwrap());
        assert_eq!(cursor.column(1).unwrap(), Value::build_text("carol"));

        // No more
        assert!(!cursor.next().unwrap());
    }

    #[test]
    fn constraint_op_roundtrip() {
        let ops = [
            ConstraintOp::Eq,
            ConstraintOp::Lt,
            ConstraintOp::Le,
            ConstraintOp::Gt,
            ConstraintOp::Ge,
            ConstraintOp::Like,
            ConstraintOp::Ne,
            ConstraintOp::IsNull,
            ConstraintOp::IsNotNull,
        ];
        for op in ops {
            let u = op as u8;
            let roundtripped = constraint_op_from_u8(u).unwrap();
            assert_eq!(roundtripped, op);
        }
    }

    #[test]
    fn idx_str_encoding_and_parsing() {
        let fdw = Arc::new(StaticFdw {
            rows: make_test_rows(),
            key_columns: vec![
                KeyColumn::new("id", 0, vec![ConstraintOp::Eq]),
                KeyColumn::new("score", 2, vec![ConstraintOp::Gt, ConstraintOp::Ge]),
            ],
        });
        let adapter = ForeignTableAdapter::new(fdw.clone());

        let constraints = vec![
            ConstraintInfo {
                column_index: 0,
                op: ConstraintOp::Eq,
                usable: true,
                index: 0,
            },
            ConstraintInfo {
                column_index: 2,
                op: ConstraintOp::Gt,
                usable: true,
                index: 1,
            },
        ];

        let info = adapter.best_index(&constraints, &[]).unwrap();
        let idx_str = info.idx_str.unwrap();

        // Should encode both constraints
        assert_eq!(
            idx_str,
            format!("0:{},2:{}", ConstraintOp::Eq as u8, ConstraintOp::Gt as u8)
        );

        // Now test that cursor adapter can parse it back
        let mut cursor_adapter = ForeignCursorAdapter {
            inner: fdw.open_cursor().unwrap(),
        };

        let args = vec![Value::build_text("2"), Value::from_i64(80)];
        let has_rows = cursor_adapter.filter(&args, Some(idx_str), 0).unwrap();
        assert!(has_rows);
        // Should have filtered to id="2" AND score > 80 — but our StaticCursor
        // only handles Eq, so it filters to id="2" (bob, score=75).
        // The Gt constraint is passed but StaticCursor ignores non-Eq.
        assert_eq!(
            cursor_adapter.inner.column(1).unwrap(),
            Value::build_text("bob")
        );
    }
}
