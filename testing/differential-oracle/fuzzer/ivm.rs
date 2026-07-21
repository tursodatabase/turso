//! Incremental view maintenance (IVM) differential checking.
//!
//! In IVM mode the fuzzer periodically creates materialized views over the
//! generated schema and, after every statement, verifies the fundamental IVM
//! invariant: the contents of a materialized view must equal a fresh
//! evaluation of its defining query. SQLite has no materialized views, so the
//! view DDL executes on Turso only and both sides of the comparison run on
//! the same Turso connection — any divergence is an incremental-maintenance
//! bug, never a semantics ambiguity. The defining query itself is still
//! pinned to SQLite semantics transitively, because the base tables it reads
//! are covered by the regular differential oracle.
//!
//! Row comparison goes through `SqlValue`, whose equality is type-strict
//! (`Integer(30) != Real(30.0)`), so divergences like an aggregate losing
//! SQLite's integer result typing are caught, not coerced away.

use std::fmt::Write as _;
use std::sync::Arc;

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use sql_gen::{ColumnDef, DataType, Schema, Table};
use sql_gen_prop::result::diff_results;

use crate::oracle::{DifferentialOracle, OracleResult, QueryResult};

/// Maximum number of materialized views to keep alive during a run.
const MAX_VIEWS: usize = 8;
/// Attempt to create a new view every this many statements (until MAX_VIEWS).
const CREATE_ATTEMPT_INTERVAL: usize = 10;

/// A materialized view created by the fuzzer, with its defining query.
#[derive(Debug, Clone)]
pub struct IvmView {
    pub name: String,
    pub definition: String,
}

/// Outcome of a view creation attempt, for test.sql logging.
#[derive(Debug)]
pub enum IvmCreateOutcome {
    Created { sql: String },
    Rejected { sql: String, error: String },
}

/// Tracks the materialized views created during an IVM-mode run.
#[derive(Debug, Default)]
pub struct IvmState {
    views: Vec<IvmView>,
    next_id: usize,
}

impl IvmState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn views(&self) -> &[IvmView] {
        &self.views
    }

    /// Whether the runner should attempt to create a view at this statement
    /// index. Creation is additionally gated on autocommit by the caller so a
    /// generated ROLLBACK can never undo a tracked view.
    pub fn wants_view(&self, stmt_index: usize) -> bool {
        self.views.len() < MAX_VIEWS && stmt_index % CREATE_ATTEMPT_INTERVAL == 0
    }

    /// Generate and execute one CREATE MATERIALIZED VIEW on Turso.
    ///
    /// Returns `None` when the schema has no usable tables. A rejected
    /// creation (e.g. a shape the IVM engine does not support) is reported to
    /// the caller for logging but is not an oracle failure.
    pub fn try_create_view(
        &mut self,
        turso_conn: &Arc<turso_core::Connection>,
        schema: &Schema,
        rng: &mut ChaCha8Rng,
    ) -> Option<IvmCreateOutcome> {
        let definition = generate_definition(schema, rng)?;
        let name = format!("ivm_v{}", self.next_id);
        self.next_id += 1;
        let sql = format!("CREATE MATERIALIZED VIEW \"{name}\" AS {definition}");

        match DifferentialOracle::execute_turso(turso_conn, &sql) {
            QueryResult::Error(error) => Some(IvmCreateOutcome::Rejected { sql, error }),
            _ => {
                self.views.push(IvmView { name, definition });
                Some(IvmCreateOutcome::Created { sql })
            }
        }
    }

    /// Verify the IVM invariant for every tracked view: the materialized
    /// contents (as a user would read them) must equal a fresh evaluation of
    /// the defining query, as multisets.
    pub fn check_views(&self, turso_conn: &Arc<turso_core::Connection>) -> OracleResult {
        for view in &self.views {
            let view_sql = format!("SELECT * FROM \"{}\"", view.name);
            let view_result = DifferentialOracle::execute_turso(turso_conn, &view_sql);
            let query_result = DifferentialOracle::execute_turso(turso_conn, &view.definition);

            let (view_rows, query_rows) = match (view_result, query_result) {
                (QueryResult::Rows(v), QueryResult::Rows(q)) => (v, q),
                (QueryResult::Ok, QueryResult::Ok) => continue,
                (QueryResult::Rows(v), QueryResult::Ok) => (v, Vec::new()),
                (QueryResult::Ok, QueryResult::Rows(q)) => (Vec::new(), q),
                (QueryResult::Error(e), _) => {
                    return OracleResult::Fail(format!(
                        "IVM check: reading materialized view {} failed: {e}\n  definition: {}",
                        view.name, view.definition
                    ));
                }
                (_, QueryResult::Error(e)) => {
                    return OracleResult::Fail(format!(
                        "IVM check: re-evaluating defining query of {} failed: {e}\n  definition: {}",
                        view.name, view.definition
                    ));
                }
            };

            let diff = diff_results(&view_rows, &query_rows);
            if !diff.is_empty() {
                return OracleResult::Fail(format!(
                    "IVM invariant violated for {}:\n  definition: {}\n  only in materialized view: {:?}\n  only in fresh query: {:?}",
                    view.name, view.definition, diff.only_in_first, diff.only_in_second
                ));
            }
        }
        OracleResult::Pass
    }
}

/// Generate a view definition over the current schema, or `None` when no
/// usable main-database table exists yet.
fn generate_definition(schema: &Schema, rng: &mut ChaCha8Rng) -> Option<String> {
    // Materialized views over attached/temp databases are rejected by the
    // engine, so only main-database tables qualify.
    let tables: Vec<&Table> = schema
        .tables
        .iter()
        .filter(|t| t.database.is_none() && !t.columns.is_empty())
        .collect();
    if tables.is_empty() {
        return None;
    }

    let table = tables[rng.random_range(0..tables.len())];
    match rng.random_range(0..12u32) {
        0..=2 => Some(projection(table, rng, false)),
        3..=4 => Some(projection(table, rng, true)),
        5..=7 => Some(aggregate(table, rng)),
        8..=9 => Some(scalar_aggregate(table, rng)),
        _ => join(&tables, rng).or_else(|| Some(projection(table, rng, true))),
    }
}

fn quoted(name: &str) -> String {
    format!("\"{name}\"")
}

/// Pick a non-empty subset of columns, preserving declaration order.
fn column_subset<'t>(table: &'t Table, rng: &mut ChaCha8Rng) -> Vec<&'t ColumnDef> {
    let subset: Vec<&ColumnDef> = table
        .columns
        .iter()
        .filter(|_| rng.random_bool(0.7))
        .collect();
    if subset.is_empty() {
        table.columns.iter().collect()
    } else {
        subset
    }
}

/// `SELECT [DISTINCT] cols FROM t [WHERE pred]`
fn projection(table: &Table, rng: &mut ChaCha8Rng, with_filter: bool) -> String {
    let cols = column_subset(table, rng)
        .iter()
        .map(|c| quoted(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let distinct = if rng.random_bool(0.25) {
        "DISTINCT "
    } else {
        ""
    };
    let mut sql = format!("SELECT {distinct}{cols} FROM {}", quoted(&table.name));
    if with_filter {
        let col = &table.columns[rng.random_range(0..table.columns.len())];
        let _ = write!(sql, " WHERE {}", predicate(col, rng));
    }
    sql
}

fn predicate(col: &ColumnDef, rng: &mut ChaCha8Rng) -> String {
    let name = quoted(&col.name);
    if rng.random_bool(0.2) {
        return format!("{name} IS NOT NULL");
    }
    let op = ["<", ">=", "<>"][rng.random_range(0..3)];
    match col.data_type {
        DataType::Integer | DataType::Real => {
            format!("{name} {op} {}", rng.random_range(-5..=5))
        }
        DataType::Text => {
            let letter = (b'a' + rng.random_range(0..26u8)) as char;
            format!("{name} {op} '{letter}'")
        }
        _ => format!("{name} IS NOT NULL"),
    }
}

/// `SELECT g, COUNT(*) AS cnt, SUM(n) AS agg1, ... FROM t GROUP BY g [HAVING ...]`
fn aggregate(table: &Table, rng: &mut ChaCha8Rng) -> String {
    let group_col = &table.columns[rng.random_range(0..table.columns.len())];
    let numeric_cols: Vec<&ColumnDef> = table
        .columns
        .iter()
        .filter(|c| matches!(c.data_type, DataType::Integer | DataType::Real))
        .collect();

    let mut exprs = vec![format!("COUNT(*) AS cnt")];
    if !numeric_cols.is_empty() {
        for (i, func) in ["SUM", "AVG", "MIN", "MAX"].iter().enumerate() {
            if rng.random_bool(0.5) {
                let col = numeric_cols[rng.random_range(0..numeric_cols.len())];
                let distinct = if rng.random_bool(0.3) {
                    "DISTINCT "
                } else {
                    ""
                };
                exprs.push(format!("{func}({distinct}{}) AS agg{i}", quoted(&col.name)));
            }
        }
        if rng.random_bool(0.3) {
            let col = numeric_cols[rng.random_range(0..numeric_cols.len())];
            exprs.push(format!("COUNT(DISTINCT {}) AS agg_cd", quoted(&col.name)));
        }
    }

    let having = if rng.random_bool(0.4) {
        let g = quoted(&group_col.name);
        match rng.random_range(0..4u8) {
            0 => format!(" HAVING COUNT(*) > {}", rng.random_range(0..3)),
            1 => format!(" HAVING {g} IS NOT NULL"),
            2 if !numeric_cols.is_empty() => {
                let col = numeric_cols[rng.random_range(0..numeric_cols.len())];
                format!(" HAVING SUM({}) IS NOT NULL", quoted(&col.name))
            }
            _ if !numeric_cols.is_empty() => {
                let col = numeric_cols[rng.random_range(0..numeric_cols.len())];
                format!(
                    " HAVING MAX({}) >= {}",
                    quoted(&col.name),
                    rng.random_range(-50..50)
                )
            }
            _ => format!(" HAVING COUNT(*) >= {}", rng.random_range(1..3)),
        }
    } else {
        String::new()
    };

    format!(
        "SELECT {g}, {exprs} FROM {t} GROUP BY {g}{having}",
        g = quoted(&group_col.name),
        exprs = exprs.join(", "),
        t = quoted(&table.name),
    )
}

/// `SELECT COUNT(*) AS cnt, SUM(n) AS agg1, ... FROM t [WHERE ...] [HAVING ...]`
///
/// Scalar aggregates: no GROUP BY, one always-present row (unless HAVING
/// suppresses it).
fn scalar_aggregate(table: &Table, rng: &mut ChaCha8Rng) -> String {
    let numeric_cols: Vec<&ColumnDef> = table
        .columns
        .iter()
        .filter(|c| matches!(c.data_type, DataType::Integer | DataType::Real))
        .collect();

    let mut exprs = vec![format!("COUNT(*) AS cnt")];
    if !numeric_cols.is_empty() {
        for (i, func) in ["SUM", "AVG", "MIN", "MAX"].iter().enumerate() {
            if rng.random_bool(0.4) {
                let col = numeric_cols[rng.random_range(0..numeric_cols.len())];
                let distinct = if rng.random_bool(0.3) {
                    "DISTINCT "
                } else {
                    ""
                };
                exprs.push(format!("{func}({distinct}{}) AS agg{i}", quoted(&col.name)));
            }
        }
    }

    let mut sql = format!(
        "SELECT {exprs} FROM {t}",
        exprs = exprs.join(", "),
        t = quoted(&table.name),
    );
    if rng.random_bool(0.4) {
        let col = &table.columns[rng.random_range(0..table.columns.len())];
        let _ = write!(sql, " WHERE {}", predicate(col, rng));
    }
    if rng.random_bool(0.4) {
        let _ = write!(sql, " HAVING COUNT(*) > {}", rng.random_range(0..3));
    }
    sql
}

/// `SELECT l.a AS l_a, r.b AS r_b FROM t1 AS l JOIN t2 AS r ON l.j = r.k`
///
/// Picks any two tables (possibly the same one — self-joins exercise reading
/// a table's delta and btree on both sides) with a same-typed column pair.
/// Returns `None` when no such pair exists.
fn join(tables: &[&Table], rng: &mut ChaCha8Rng) -> Option<String> {
    let mut pairs = Vec::new();
    for left in tables.iter() {
        for right in tables.iter() {
            for lc in &left.columns {
                for rc in &right.columns {
                    if lc.data_type == rc.data_type
                        && matches!(
                            lc.data_type,
                            DataType::Integer | DataType::Real | DataType::Text
                        )
                    {
                        pairs.push((*left, *right, lc, rc));
                    }
                }
            }
        }
    }
    if pairs.is_empty() {
        return None;
    }
    let (left, right, lc, rc) = pairs[rng.random_range(0..pairs.len())];

    let mut cols = Vec::new();
    for c in column_subset(left, rng).iter().take(2) {
        cols.push(format!("l.{} AS l_{}", quoted(&c.name), c.name));
    }
    for c in column_subset(right, rng).iter().take(2) {
        cols.push(format!("r.{} AS r_{}", quoted(&c.name), c.name));
    }

    // Sometimes aggregate over the join instead of projecting it.
    if rng.random_bool(0.4) {
        let group_col = &left.columns[rng.random_range(0..left.columns.len())];
        let mut exprs = vec![
            format!("l.{} AS g", quoted(&group_col.name)),
            "COUNT(*) AS cnt".to_string(),
        ];
        let r_numeric: Vec<&ColumnDef> = right
            .columns
            .iter()
            .filter(|c| matches!(c.data_type, DataType::Integer | DataType::Real))
            .collect();
        if !r_numeric.is_empty() && rng.random_bool(0.7) {
            let col = r_numeric[rng.random_range(0..r_numeric.len())];
            exprs.push(format!("SUM(r.{}) AS s", quoted(&col.name)));
        }
        return Some(format!(
            "SELECT {exprs} FROM {lt} AS l JOIN {rt} AS r ON l.{lc} = r.{rc} GROUP BY l.{g}",
            exprs = exprs.join(", "),
            lt = quoted(&left.name),
            rt = quoted(&right.name),
            lc = quoted(&lc.name),
            rc = quoted(&rc.name),
            g = quoted(&group_col.name),
        ));
    }

    Some(format!(
        "SELECT {cols} FROM {lt} AS l JOIN {rt} AS r ON l.{lc} = r.{rc}",
        cols = cols.join(", "),
        lt = quoted(&left.name),
        rt = quoted(&right.name),
        lc = quoted(&lc.name),
        rc = quoted(&rc.name),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use sql_gen::SchemaBuilder;
    use turso_core::{Database, SqliteDialect};

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .table(Table::new(
                "t1",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("num", DataType::Integer),
                    ColumnDef::new("txt", DataType::Text),
                ],
            ))
            .table(Table::new(
                "t2",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("val", DataType::Real),
                ],
            ))
            .build()
    }

    fn open_turso_with_views(name: &str) -> Arc<turso_core::Connection> {
        let io = Arc::new(crate::memory::MemorySimIO::new(42));
        let db = Database::open_file_with_flags(
            io,
            name,
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new().with_views(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect().unwrap()
    }

    #[test]
    fn definitions_are_deterministic_per_seed() {
        let schema = test_schema();
        let gen_all = || {
            let mut rng = ChaCha8Rng::seed_from_u64(7);
            (0..20)
                .map(|_| generate_definition(&schema, &mut rng))
                .collect::<Vec<_>>()
        };
        assert_eq!(gen_all(), gen_all());
    }

    #[test]
    fn generated_definitions_are_valid_selects() {
        let schema = test_schema();
        let mut rng = ChaCha8Rng::seed_from_u64(3);
        for _ in 0..50 {
            let def = generate_definition(&schema, &mut rng).unwrap();
            assert!(def.starts_with("SELECT "), "unexpected definition: {def}");
        }
    }

    #[test]
    fn check_views_passes_after_dml_and_catches_stale_definition() {
        let conn = open_turso_with_views("ivm-check-test.db");
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, num INTEGER)",
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
            "CREATE MATERIALIZED VIEW v AS SELECT id, num FROM t WHERE num >= 20",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.push(IvmView {
            name: "v".into(),
            definition: "SELECT id, num FROM t WHERE num >= 20".into(),
        });

        assert!(state.check_views(&conn).is_pass());

        // Maintenance must keep the view in sync through inserts and deletes.
        for sql in ["INSERT INTO t VALUES (4, 40)", "DELETE FROM t WHERE id = 2"] {
            assert!(!DifferentialOracle::execute_turso(&conn, sql).is_error());
        }
        assert!(state.check_views(&conn).is_pass());

        // A deliberately wrong tracked definition must be reported: this
        // exercises the comparison itself.
        state.views[0].definition = "SELECT id, num FROM t WHERE num >= 40".into();
        assert!(state.check_views(&conn).is_fail());
    }
}
