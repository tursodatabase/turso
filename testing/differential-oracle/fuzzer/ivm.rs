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
    Created {
        sql: String,
        /// Deterministic no-op updates executed on both engines. Probing every
        /// main table guarantees that every leaf of the generated view is
        /// driven at least once; unrelated tables are harmless extra probes.
        leaf_probes: Vec<String>,
    },
    Rejected {
        sql: String,
        error: String,
        must_create: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedDefinition {
    sql: String,
    /// This template is part of the supported compositional surface. Treat a
    /// CREATE rejection as an oracle failure rather than silently reducing
    /// fuzz coverage.
    must_create: bool,
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
    /// Returns `None` when the schema has no usable tables. Rejections are
    /// reported with their capability expectation: unsupported compositions
    /// are coverage, while rejection of a `must_create` composition is an
    /// oracle failure in the runner.
    pub fn try_create_view(
        &mut self,
        turso_conn: &Arc<turso_core::Connection>,
        schema: &Schema,
        rng: &mut ChaCha8Rng,
    ) -> Option<IvmCreateOutcome> {
        // Exercise topologically scheduled view dependencies. A rejection is
        // an oracle failure: accepted upstream views are valid relation
        // inputs and their changes must propagate through the same log.
        let definition = if !self.views.is_empty() && rng.random_bool(0.1) {
            let upstream = &self.views[rng.random_range(0..self.views.len())];
            let source = quoted(&upstream.name);
            GeneratedDefinition {
                sql: match rng.random_range(0..3) {
                    0 => format!("SELECT * FROM {source}"),
                    1 => format!("SELECT COUNT(*) FROM {source}"),
                    _ => format!("SELECT DISTINCT * FROM {source}"),
                },
                must_create: true,
            }
        } else {
            generate_definition(schema, rng)?
        };
        let name = format!("ivm_v{}", self.next_id);
        self.next_id += 1;
        let sql = format!("CREATE MATERIALIZED VIEW \"{name}\" AS {}", definition.sql);

        match DifferentialOracle::execute_turso(turso_conn, &sql) {
            QueryResult::Error(error) => Some(IvmCreateOutcome::Rejected {
                sql,
                error,
                must_create: definition.must_create,
            }),
            _ => {
                self.views.push(IvmView {
                    name,
                    definition: definition.sql,
                });
                let leaf_probes = schema
                    .tables
                    .iter()
                    .filter(|table| table.database.is_none())
                    .filter_map(|table| {
                        table.columns.first().map(|column| {
                            format!(
                                "UPDATE {} SET {} = {}",
                                quoted(&table.name),
                                quoted(&column.name),
                                quoted(&column.name)
                            )
                        })
                    })
                    .collect();
                Some(IvmCreateOutcome::Created { sql, leaf_probes })
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
fn generate_definition(schema: &Schema, rng: &mut ChaCha8Rng) -> Option<GeneratedDefinition> {
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
    let (sql, must_create) = match rng.random_range(0..30u32) {
        0..=2 => (Some(projection(table, rng, false)), false),
        3..=4 => (Some(projection(table, rng, true)), false),
        5..=7 => (Some(aggregate(table, rng)), false),
        8..=9 => (Some(scalar_aggregate(table, rng)), false),
        10 => (union(&tables, rng), false),
        11 => (
            compound_all(&tables, rng).or_else(|| Some(projection(table, rng, true))),
            false,
        ),
        12..=14 => (
            join(&tables, rng).or_else(|| Some(projection(table, rng, true))),
            false,
        ),
        // Recursive relational generation: operators compose by consuming
        // the typed output of generated children rather than selecting a
        // named end-to-end shape.
        15..=27 => (
            compositional_definition(&tables, rng, 3).map(|relation| relation.sql),
            true,
        ),
        // FULL remains an explicit capability probe and is wrapped by a
        // downstream operator so rejection is checked compositionally.
        28 => (full_outer_composition(&tables, rng, true), false),
        _ => (full_outer_composition(&tables, rng, false), false),
    };
    sql.map(|sql| GeneratedDefinition { sql, must_create })
}

fn compatible_pair<'a>(
    tables: &[&'a Table],
    rng: &mut ChaCha8Rng,
) -> Option<(&'a Table, &'a Table, &'a ColumnDef, &'a ColumnDef)> {
    let mut pairs = Vec::new();
    for left in tables {
        for right in tables {
            for left_column in &left.columns {
                for right_column in &right.columns {
                    if left_column.data_type == right_column.data_type
                        && matches!(
                            left_column.data_type,
                            DataType::Integer | DataType::Real | DataType::Text
                        )
                    {
                        pairs.push((*left, *right, left_column, right_column));
                    }
                }
            }
        }
    }
    (!pairs.is_empty()).then(|| pairs[rng.random_range(0..pairs.len())])
}

/// A one-column relational expression used by the recursive composition
/// generator. Every operator consumes this same typed contract and publishes
/// it again, except COUNT which deliberately changes the type to INTEGER.
struct ComposedRelation {
    sql: String,
    data_type: DataType,
}

fn compositional_definition(
    tables: &[&Table],
    rng: &mut ChaCha8Rng,
    depth: usize,
) -> Option<ComposedRelation> {
    let types = [DataType::Integer, DataType::Real, DataType::Text]
        .into_iter()
        .filter(|data_type| {
            tables.iter().any(|table| {
                table
                    .columns
                    .iter()
                    .any(|column| column.data_type == *data_type)
            })
        })
        .collect::<Vec<_>>();
    if types.is_empty() {
        return None;
    }
    let data_type = types[rng.random_range(0..types.len())];
    let mut next_alias = 0;
    compose_relation(tables, rng, data_type, depth, &mut next_alias)
}

fn compose_relation(
    tables: &[&Table],
    rng: &mut ChaCha8Rng,
    data_type: DataType,
    depth: usize,
    next_alias: &mut usize,
) -> Option<ComposedRelation> {
    let base = |rng: &mut ChaCha8Rng| {
        let leaves = tables
            .iter()
            .flat_map(|table| {
                table
                    .columns
                    .iter()
                    .filter(move |column| column.data_type == data_type)
                    .map(move |column| (*table, column))
            })
            .collect::<Vec<_>>();
        let (table, column) = leaves.get(rng.random_range(0..leaves.len()))?;
        Some(ComposedRelation {
            sql: format!(
                "SELECT {} AS value FROM {}",
                quoted(&column.name),
                quoted(&table.name)
            ),
            data_type,
        })
    };

    if depth == 0 || rng.random_bool(0.2) {
        return base(rng);
    }

    let alias = |next_alias: &mut usize, prefix: &str| {
        let alias = format!("{prefix}{}", *next_alias);
        *next_alias += 1;
        alias
    };
    match rng.random_range(0..8u8) {
        0 => base(rng),
        1 => {
            let child = compose_relation(tables, rng, data_type, depth - 1, next_alias)?;
            let child_alias = alias(next_alias, "f");
            Some(ComposedRelation {
                sql: format!(
                    "SELECT value FROM ({}) AS {} WHERE value IS NOT NULL",
                    child.sql,
                    quoted(&child_alias)
                ),
                data_type: child.data_type,
            })
        }
        2 => {
            let child = compose_relation(tables, rng, data_type, depth - 1, next_alias)?;
            let child_alias = alias(next_alias, "d");
            Some(ComposedRelation {
                sql: format!(
                    "SELECT DISTINCT value FROM ({}) AS {}",
                    child.sql,
                    quoted(&child_alias)
                ),
                data_type: child.data_type,
            })
        }
        3 if data_type == DataType::Integer => {
            let child_type = [DataType::Integer, DataType::Real, DataType::Text]
                .into_iter()
                .filter(|child_type| {
                    tables.iter().any(|table| {
                        table
                            .columns
                            .iter()
                            .any(|column| column.data_type == *child_type)
                    })
                })
                .collect::<Vec<_>>();
            let child_type = child_type[rng.random_range(0..child_type.len())];
            let child = compose_relation(tables, rng, child_type, depth - 1, next_alias)?;
            let child_alias = alias(next_alias, "a");
            Some(ComposedRelation {
                sql: format!(
                    "SELECT COUNT(*) AS value FROM ({}) AS {}",
                    child.sql,
                    quoted(&child_alias)
                ),
                data_type: DataType::Integer,
            })
        }
        3..=5 => {
            let left = compose_relation(tables, rng, data_type, depth - 1, next_alias)?;
            let right = compose_relation(tables, rng, data_type, depth - 1, next_alias)?;
            let left_alias = alias(next_alias, "s");
            let right_alias = alias(next_alias, "s");
            let operator = ["UNION ALL", "UNION", "INTERSECT", "EXCEPT"][rng.random_range(0..4)];
            Some(ComposedRelation {
                sql: format!(
                    "SELECT value FROM ({}) AS {} {operator} \
                     SELECT value FROM ({}) AS {}",
                    left.sql,
                    quoted(&left_alias),
                    right.sql,
                    quoted(&right_alias)
                ),
                data_type,
            })
        }
        6 | 7 => {
            let left = compose_relation(tables, rng, data_type, depth - 1, next_alias)?;
            let right = compose_relation(tables, rng, data_type, depth - 1, next_alias)?;
            let join = if rng.random_bool(0.3) {
                "LEFT JOIN"
            } else {
                "JOIN"
            };
            Some(ComposedRelation {
                sql: format!(
                    "SELECT l.value AS value FROM ({}) AS l {join} ({}) AS r \
                     ON l.value = r.value",
                    left.sql, right.sql
                ),
                data_type,
            })
        }
        _ => unreachable!(),
    }
}

fn full_outer_composition(
    tables: &[&Table],
    rng: &mut ChaCha8Rng,
    aggregate: bool,
) -> Option<String> {
    let (left, right, left_column, right_column) = compatible_pair(tables, rng)?;
    let output = if aggregate {
        "COUNT(*)".to_string()
    } else {
        format!(
            "DISTINCT l.{}, r.{}",
            quoted(&left_column.name),
            quoted(&right_column.name)
        )
    };
    Some(format!(
        "SELECT {output} FROM {left} AS l FULL OUTER JOIN {right} AS r \
         ON l.{left_column} = r.{right_column}",
        left = quoted(&left.name),
        right = quoted(&right.name),
        left_column = quoted(&left_column.name),
        right_column = quoted(&right_column.name),
    ))
}

/// `SELECT <aggs> FROM t1 UNION ALL SELECT <aggs> FROM t2` — a pure UNION ALL
/// whose branches are scalar aggregates (one row per branch, maintained by
/// each branch's own aggregate sub-program). Both branches use the same
/// aggregate arity so the compound is valid.
fn compound_all(tables: &[&Table], rng: &mut ChaCha8Rng) -> Option<String> {
    let left = tables[rng.random_range(0..tables.len())];
    let right = tables[rng.random_range(0..tables.len())];
    let numeric = |t: &Table| -> Vec<String> {
        t.columns
            .iter()
            .filter(|c| matches!(c.data_type, DataType::Integer | DataType::Real))
            .map(|c| quoted(&c.name))
            .collect()
    };
    let (ln, rn) = (numeric(left), numeric(right));
    // A second aggregate column (a SUM) only if both branches have a numeric
    // column to sum, keeping the arities equal.
    let with_sum = !ln.is_empty() && !rn.is_empty() && rng.random_bool(0.6);

    fn branch(t: &Table, cols: &[String], with_sum: bool, rng: &mut ChaCha8Rng) -> String {
        let mut exprs = vec!["COUNT(*)".to_string()];
        if with_sum {
            exprs.push(format!("SUM({})", cols[rng.random_range(0..cols.len())]));
        }
        let mut sql = format!("SELECT {} FROM {}", exprs.join(", "), quoted(&t.name));
        if rng.random_bool(0.4) {
            let col = &t.columns[rng.random_range(0..t.columns.len())];
            let _ = write!(sql, " WHERE {}", predicate(col, rng));
        }
        sql
    }

    let left_sql = branch(left, &ln, with_sum, rng);
    let right_sql = branch(right, &rn, with_sum, rng);
    Some(format!("{left_sql} UNION ALL {right_sql}"))
}

/// `SELECT c FROM t1 [WHERE ...] UNION [ALL] SELECT c FROM t2 [WHERE ...]`
///
/// Picks two (possibly identical) tables with same-typed columns.
fn union(tables: &[&Table], rng: &mut ChaCha8Rng) -> Option<String> {
    let mut pairs = Vec::new();
    for left in tables.iter() {
        for right in tables.iter() {
            for lc in &left.columns {
                for rc in &right.columns {
                    if lc.data_type == rc.data_type {
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
    let op = match rng.random_range(0..6u8) {
        0 | 1 => "UNION",
        2 | 3 => "UNION ALL",
        4 => "INTERSECT",
        _ => "EXCEPT",
    };
    let mut sql = format!("SELECT {} FROM {}", quoted(&lc.name), quoted(&left.name));
    if rng.random_bool(0.4) {
        let _ = write!(sql, " WHERE {}", predicate(lc, rng));
    }
    let _ = write!(
        sql,
        " {op} SELECT {} FROM {}",
        quoted(&rc.name),
        quoted(&right.name)
    );
    if rng.random_bool(0.4) {
        let _ = write!(sql, " WHERE {}", predicate(rc, rng));
    }
    // Sometimes extend into a mixed three-branch chain.
    if rng.random_bool(0.3) {
        let (third, tc) = {
            let t = tables[rng.random_range(0..tables.len())];
            let candidates: Vec<_> = t
                .columns
                .iter()
                .filter(|c| c.data_type == lc.data_type)
                .collect();
            if candidates.is_empty() {
                return Some(sql);
            }
            (t, candidates[rng.random_range(0..candidates.len())])
        };
        let op2 = match rng.random_range(0..4u8) {
            0 => "UNION",
            1 => "UNION ALL",
            2 => "INTERSECT",
            _ => "EXCEPT",
        };
        let _ = write!(
            sql,
            " {op2} SELECT {} FROM {}",
            quoted(&tc.name),
            quoted(&third.name)
        );
        if rng.random_bool(0.4) {
            let _ = write!(sql, " WHERE {}", predicate(tc, rng));
        }
    }
    Some(sql)
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
        // Expressions over aggregate results (and the group column),
        // evaluated on the finalized group row.
        if rng.random_bool(0.4) {
            let col = numeric_cols[rng.random_range(0..numeric_cols.len())];
            let c = quoted(&col.name);
            let g = quoted(&group_col.name);
            exprs.push(match rng.random_range(0..4u8) {
                0 => format!("SUM({c}) + COUNT(*) AS xmix"),
                1 => format!("MAX({c}) - MIN({c}) AS xspread"),
                2 => format!("COALESCE(SUM({c}), 0) AS xcoal"),
                _ => format!("CASE WHEN COUNT(*) > 1 THEN {g} ELSE NULL END AS xcase"),
            });
        }
    } else if rng.random_bool(0.4) {
        exprs.push("COUNT(*) * 2 + 1 AS xcnt".to_string());
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
        if rng.random_bool(0.3) {
            let col = numeric_cols[rng.random_range(0..numeric_cols.len())];
            let c = quoted(&col.name);
            exprs.push(match rng.random_range(0..2u8) {
                0 => format!("SUM({c}) + COUNT(*) AS xmix"),
                _ => format!("COALESCE(MAX({c}), -1) AS xcoal"),
            });
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

    // Sometimes extend to a three-way join through a second same-typed pair.
    if rng.random_bool(0.25) {
        let third = tables[rng.random_range(0..tables.len())];
        let mut second_pairs = Vec::new();
        for rc2 in &right.columns {
            for tc in &third.columns {
                if rc2.data_type == tc.data_type
                    && matches!(
                        rc2.data_type,
                        DataType::Integer | DataType::Real | DataType::Text
                    )
                {
                    second_pairs.push((rc2, tc));
                }
            }
        }
        if !second_pairs.is_empty() {
            let (rc2, tc) = second_pairs[rng.random_range(0..second_pairs.len())];
            return Some(format!(
                "SELECT l.{lcol} AS a, r.{rcol} AS b, t.{tcol} AS c \
                 FROM {lt} AS l JOIN {rt} AS r ON l.{lc} = r.{rc} \
                 JOIN {tt} AS t ON r.{rc2} = t.{tc}",
                lcol = quoted(&left.columns[0].name),
                rcol = quoted(&right.columns[0].name),
                tcol = quoted(&third.columns[0].name),
                lt = quoted(&left.name),
                rt = quoted(&right.name),
                tt = quoted(&third.name),
                lc = quoted(&lc.name),
                rc = quoted(&rc.name),
                rc2 = quoted(&rc2.name),
                tc = quoted(&tc.name),
            ));
        }
    }

    // Sometimes make it a LEFT JOIN: unmatched left rows appear NULL-padded,
    // exercising the per-left-row match bookkeeping. Sometimes deduplicate it
    // (DISTINCT), which groups the padded rows by their output content.
    if rng.random_bool(0.25) {
        let distinct = if rng.random_bool(0.4) {
            "DISTINCT "
        } else {
            ""
        };
        let mut sql = format!(
            "SELECT {distinct}{cols} FROM {lt} AS l LEFT JOIN {rt} AS r ON l.{lc} = r.{rc}",
            cols = cols.join(", "),
            lt = quoted(&left.name),
            rt = quoted(&right.name),
            lc = quoted(&lc.name),
            rc = quoted(&rc.name),
        );
        if distinct.is_empty() && rng.random_bool(0.3) {
            let _ = write!(sql, " WHERE r.{} IS NULL", quoted(&rc.name));
        }
        return Some(sql);
    }

    // Sometimes make it a RIGHT JOIN: unmatched right rows appear NULL-padded
    // on the left. It is maintained as the swapped LEFT JOIN, so this
    // exercises the same bookkeeping from the other side. ON-based only, since
    // the engine rejects RIGHT JOIN with USING/NATURAL.
    if rng.random_bool(0.25) {
        let mut sql = format!(
            "SELECT {cols} FROM {lt} AS l RIGHT JOIN {rt} AS r ON l.{lc} = r.{rc}",
            cols = cols.join(", "),
            lt = quoted(&left.name),
            rt = quoted(&right.name),
            lc = quoted(&lc.name),
            rc = quoted(&rc.name),
        );
        if rng.random_bool(0.3) {
            let _ = write!(sql, " WHERE l.{} IS NULL", quoted(&lc.name));
        }
        return Some(sql);
    }

    // When the matched columns share a name (and the tables differ), the
    // join can be spelled with USING: bare * merges the shared column.
    if lc.name.eq_ignore_ascii_case(&rc.name)
        && !left.name.eq_ignore_ascii_case(&right.name)
        && rng.random_bool(0.5)
    {
        return Some(format!(
            "SELECT * FROM {lt} JOIN {rt} USING ({c})",
            lt = quoted(&left.name),
            rt = quoted(&right.name),
            c = quoted(&lc.name),
        ));
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

    // Plain projection, sometimes deduplicated with DISTINCT (grouping by
    // every output column over the join).
    let distinct = if rng.random_bool(0.3) {
        "DISTINCT "
    } else {
        ""
    };
    Some(format!(
        "SELECT {distinct}{cols} FROM {lt} AS l JOIN {rt} AS r ON l.{lc} = r.{rc}",
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
            assert!(
                def.sql.starts_with("SELECT "),
                "unexpected definition: {}",
                def.sql
            );
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

    #[test]
    fn compositional_oracle_mutates_every_leaf() {
        let conn = open_turso_with_views("ivm-composition-test.db");
        let definition = "SELECT COUNT(*) FROM a JOIN b ON a.k = b.k \
                          UNION ALL SELECT COUNT(*) FROM c";
        for sql in [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, k INTEGER)",
            "INSERT INTO a VALUES (1, 10)",
            "INSERT INTO b VALUES (1, 10)",
            "INSERT INTO c VALUES (1, 10)",
            &format!("CREATE MATERIALIZED VIEW v_composed AS {definition}"),
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.push(IvmView {
            name: "v_composed".into(),
            definition: definition.into(),
        });
        assert!(state.check_views(&conn).is_pass());

        for sql in [
            "INSERT INTO a VALUES (2, 20)",
            "INSERT INTO b VALUES (2, 20)",
            "UPDATE c SET k = 11 WHERE id = 1",
            "DELETE FROM a WHERE id = 1",
            "DELETE FROM b WHERE id = 2",
            "DELETE FROM c WHERE id = 1",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "mutation failed: {sql}"
            );
            assert!(
                state.check_views(&conn).is_pass(),
                "IVM diverged after mutating a composition leaf: {sql}"
            );
        }
    }

    #[test]
    fn join_to_set_op_oracle_mutates_every_leaf() {
        let conn = open_turso_with_views("ivm-join-setop-test.db");
        let definition = "SELECT a.k FROM a JOIN b ON a.k = b.k UNION SELECT k FROM c";
        for sql in [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, k INTEGER)",
            "INSERT INTO a VALUES (1, 10), (2, 20)",
            "INSERT INTO b VALUES (1, 10), (2, 20)",
            "INSERT INTO c VALUES (1, 10), (2, 30)",
            &format!("CREATE MATERIALIZED VIEW v_join_setop AS {definition}"),
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.push(IvmView {
            name: "v_join_setop".into(),
            definition: definition.into(),
        });
        assert!(state.check_views(&conn).is_pass());

        for sql in [
            "INSERT INTO a VALUES (3, 40)",
            "INSERT INTO b VALUES (3, 40)",
            "DELETE FROM c WHERE id = 1",
            "UPDATE b SET k = 50 WHERE id = 2",
            "INSERT INTO c VALUES (4, 20)",
            "DELETE FROM a WHERE id = 1",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "mutation failed: {sql}"
            );
            assert!(
                state.check_views(&conn).is_pass(),
                "IVM diverged after mutating join -> set-op leaf: {sql}"
            );
        }
    }

    #[test]
    fn set_op_to_aggregate_oracle_mutates_every_leaf() {
        let conn = open_turso_with_views("ivm-setop-aggregate-test.db");
        let definition = "SELECT value, COUNT(*), SUM(value) FROM (\
                          SELECT k AS value FROM a \
                          UNION SELECT k AS value FROM b) \
                          GROUP BY value";
        for sql in [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, k INTEGER)",
            "INSERT INTO a VALUES (1, 10), (2, 20)",
            "INSERT INTO b VALUES (1, 10), (2, 30)",
            &format!("CREATE MATERIALIZED VIEW v_setop_aggregate AS {definition}"),
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.push(IvmView {
            name: "v_setop_aggregate".into(),
            definition: definition.into(),
        });
        assert!(state.check_views(&conn).is_pass());

        for sql in [
            "INSERT INTO a VALUES (3, 30)",
            "INSERT INTO b VALUES (3, 20)",
            "UPDATE a SET k = 40 WHERE id = 1",
            "DELETE FROM b WHERE id = 1",
            "DELETE FROM a WHERE id = 2",
            "UPDATE b SET k = 50 WHERE id = 2",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "mutation failed: {sql}"
            );
            assert!(
                state.check_views(&conn).is_pass(),
                "IVM diverged after mutating set-op -> aggregate leaf: {sql}"
            );
        }
    }

    #[test]
    fn mixed_set_op_to_aggregate_oracle_mutates_every_leaf() {
        let conn = open_turso_with_views("ivm-mixed-setop-aggregate-test.db");
        let definition = "SELECT value, COUNT(*) FROM (\
                          SELECT k AS value FROM a \
                          UNION SELECT k AS value FROM b \
                          UNION ALL SELECT k AS value FROM c) \
                          GROUP BY value";
        for sql in [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, k INTEGER)",
            "INSERT INTO a VALUES (1, 10), (2, 20)",
            "INSERT INTO b VALUES (1, 10), (2, 30)",
            "INSERT INTO c VALUES (1, 10), (2, 20)",
            &format!("CREATE MATERIALIZED VIEW v_mixed_setop_aggregate AS {definition}"),
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.push(IvmView {
            name: "v_mixed_setop_aggregate".into(),
            definition: definition.into(),
        });
        assert!(state.check_views(&conn).is_pass());

        for sql in [
            "UPDATE a SET k = 40 WHERE id = 1",
            "DELETE FROM b WHERE id = 1",
            "INSERT INTO c VALUES (3, 30)",
            "DELETE FROM a WHERE id = 2",
            "UPDATE b SET k = 50 WHERE id = 2",
            "DELETE FROM c WHERE id = 1",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "mutation failed: {sql}"
            );
            assert!(
                state.check_views(&conn).is_pass(),
                "IVM diverged after mutating mixed set-op -> aggregate leaf: {sql}"
            );
        }
    }

    #[test]
    fn aggregate_to_set_op_oracle_mutates_every_leaf() {
        let conn = open_turso_with_views("ivm-aggregate-setop-test.db");
        let definition = "SELECT k, COUNT(*) AS n FROM a GROUP BY k \
                          UNION \
                          SELECT k, COUNT(*) AS n FROM b GROUP BY k";
        for sql in [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, k INTEGER)",
            "INSERT INTO a VALUES (1, 10), (2, 10), (3, 20)",
            "INSERT INTO b VALUES (1, 10), (2, 30)",
            &format!("CREATE MATERIALIZED VIEW v_aggregate_setop AS {definition}"),
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.push(IvmView {
            name: "v_aggregate_setop".into(),
            definition: definition.into(),
        });
        assert!(state.check_views(&conn).is_pass());

        for sql in [
            "INSERT INTO a VALUES (4, 30)",
            "INSERT INTO b VALUES (3, 10)",
            "UPDATE a SET k = 40 WHERE id = 1",
            "DELETE FROM b WHERE id = 1",
            "DELETE FROM a WHERE id = 3",
            "UPDATE b SET k = 50 WHERE id = 2",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "mutation failed: {sql}"
            );
            assert!(
                state.check_views(&conn).is_pass(),
                "IVM diverged after mutating aggregate -> set-op leaf: {sql}"
            );
        }
    }

    #[test]
    fn arranged_input_to_join_oracle_mutates_every_leaf() {
        let conn = open_turso_with_views("ivm-arranged-join-input-test.db");
        let definition = "SELECT d.k, d.n, r.label FROM (\
                          SELECT k, COUNT(*) AS n FROM a GROUP BY k) AS d \
                          JOIN r ON d.k = r.k";
        for sql in [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, k INTEGER)",
            "CREATE TABLE r(k INTEGER PRIMARY KEY, label TEXT)",
            "INSERT INTO a VALUES (1, 10), (2, 10), (3, 20)",
            "INSERT INTO r VALUES (10, 'ten'), (20, 'twenty'), (30, 'thirty')",
            &format!("CREATE MATERIALIZED VIEW v_arranged_join AS {definition}"),
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.push(IvmView {
            name: "v_arranged_join".into(),
            definition: definition.into(),
        });
        assert!(state.check_views(&conn).is_pass());

        for sql in [
            "INSERT INTO a VALUES (4, 30)",
            "UPDATE a SET k = 20 WHERE id = 1",
            "DELETE FROM r WHERE k = 10",
            "UPDATE r SET label = 'TWENTY' WHERE k = 20",
            "DELETE FROM a WHERE id = 3",
            "INSERT INTO r VALUES (10, 'ten-again')",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "mutation failed: {sql}"
            );
            assert!(
                state.check_views(&conn).is_pass(),
                "IVM diverged after mutating an arranged join-input leaf: {sql}"
            );
        }
    }
}
