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
//! Row comparison goes through `QueryValue`, whose equality is type-strict
//! (`Integer(30) != Real(30.0)`), so divergences like an aggregate losing
//! SQLite's integer result typing are caught, not coerced away. The one
//! exception is a column where the view chose its value among several that
//! compare equal, which SQL leaves open (see `check_views`).

use std::fmt::Write as _;
use std::sync::Arc;

use query_result_oracle::{ChosenValueCollation, diff_result_sets_with_chosen_values};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use sql_gen::{ColumnDef, DataType, Schema, Table};

use crate::oracle::{DifferentialOracle, OracleResult, QueryResult};

/// Maximum number of materialized views to keep alive during a run.
const MAX_VIEWS: usize = 8;
/// Attempt to create a new view every this many statements (until MAX_VIEWS).
const CREATE_ATTEMPT_INTERVAL: usize = 10;
/// Maximum number of plain (non-materialized) views to keep alive during a
/// run. They are only ever sources for materialized views, never checked
/// directly, so a much smaller cap than `MAX_VIEWS` is enough.
const MAX_PLAIN_VIEWS: usize = 6;

/// Name of the table bootstrapped once per IVM run to exercise collated keys
/// (`NOCASE`/`RTRIM`) and mixed INTEGER/REAL/NULL numeric data. See
/// `collation_probe_bootstrap_sql` for its shape and seed rows.
const COLLATION_PROBE_TABLE: &str = "ivm_collate_probe";

/// Name of a second table, sharing a column name (and type) with
/// `COLLATION_PROBE_TABLE`, bootstrapped alongside it. The regular schema
/// generator gives every table unique, randomly generated column names, so
/// without this companion `NATURAL JOIN` would never find a shared-name pair
/// to join on.
const NATURAL_PROBE_TABLE: &str = "ivm_natural_probe";

/// Deterministic sequence of INSERT/UPDATE/DELETE statements that change
/// which equal-but-not-identical row the collation/mixed-type probe table
/// shows for a GROUP BY/DISTINCT/MIN/MAX/set-op key: moving a row between
/// collation classes, and inserting/deleting the sole REAL or INTEGER member
/// of a `mix_num` group (the "delete the last REAL row" case for
/// `TOTAL`/`AVG`). Cycled round-robin by `IvmState::next_collation_mutation`.
const COLLATION_MUTATIONS: &[&str] = &[
    "UPDATE \"ivm_collate_probe\" SET nocase_key = 'A' WHERE id = 1",
    "UPDATE \"ivm_collate_probe\" SET nocase_key = 'a' WHERE id = 1",
    "DELETE FROM \"ivm_collate_probe\" WHERE id = 2",
    "INSERT INTO \"ivm_collate_probe\" (id, nocase_key, rtrim_key, plain_key, mix_num) \
     VALUES (2, 'A', 'x  ', 'A', 2.0)",
    "UPDATE \"ivm_collate_probe\" SET mix_num = 2.0 WHERE id = 1",
    "UPDATE \"ivm_collate_probe\" SET mix_num = 2 WHERE id = 1",
    "DELETE FROM \"ivm_collate_probe\" WHERE id = 4",
    "INSERT INTO \"ivm_collate_probe\" (id, nocase_key, rtrim_key, plain_key, mix_num) \
     VALUES (4, 'B', 'X  ', 'B', 0.0)",
    "UPDATE \"ivm_collate_probe\" SET rtrim_key = 'y ' WHERE id = 1",
    "UPDATE \"ivm_collate_probe\" SET rtrim_key = 'x' WHERE id = 1",
];

/// The `CREATE TABLE` and seed `INSERT` statements for the collation/mixed-
/// type probe table and its `NATURAL JOIN` companion, run once at the start
/// of every IVM run (on both engines, so the regular statement generator can
/// also read and write them like any other table). Returns
/// `(create_statements, insert_statements)`; the caller must re-introspect
/// the schema between the two groups so a post-INSERT snapshot check covers
/// the new tables.
///
/// `mix_num` is declared with no type (BLOB/NONE affinity) so that inserting
/// the INTEGER `2` and the REAL `2.0` keeps them as distinct storage classes
/// instead of NUMERIC affinity folding one into the other.
///
/// Seed rows group into three collation/type classes compared equal but not
/// identical: `nocase_key` groups `{a, A}`/`{B, b}`/`{C, c}`; `rtrim_key`
/// groups `{x, "x  "}`/`{X, "X  "}`/`{y, "y ", "y  "}`; `mix_num` groups
/// `{2, 2.0}`/`{0, 0.0, -0.0}`.
pub fn collation_probe_bootstrap_sql() -> (Vec<String>, Vec<String>) {
    let create = vec![
        format!(
            "CREATE TABLE \"{COLLATION_PROBE_TABLE}\" (\
             id INTEGER PRIMARY KEY, \
             nocase_key TEXT COLLATE NOCASE, \
             rtrim_key TEXT COLLATE RTRIM, \
             plain_key TEXT, \
             mix_num BLOB)"
        ),
        format!(
            "CREATE TABLE \"{NATURAL_PROBE_TABLE}\" (\
             id INTEGER PRIMARY KEY, \
             nocase_key TEXT, \
             tag TEXT)"
        ),
    ];
    let insert = vec![
        format!(
            "INSERT INTO \"{COLLATION_PROBE_TABLE}\" \
             (id, nocase_key, rtrim_key, plain_key, mix_num) VALUES \
             (1, 'a', 'x', 'a', 2), \
             (2, 'A', 'x  ', 'A', 2.0), \
             (3, 'a  ', 'X', 'a  ', 0), \
             (4, 'B', 'X  ', 'B', 0.0), \
             (5, 'b', 'y', 'b', -0.0), \
             (6, 'C', 'y ', 'C', 5), \
             (7, NULL, NULL, NULL, NULL), \
             (8, 'c', 'y  ', 'c', 5.0)"
        ),
        format!(
            "INSERT INTO \"{NATURAL_PROBE_TABLE}\" (id, nocase_key, tag) VALUES \
             (1, 'a', 'alpha'), \
             (2, 'B', 'bravo'), \
             (3, 'zzz', 'zulu'), \
             (4, NULL, 'none')"
        ),
    ];
    (create, insert)
}

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
        /// Statements already executed (Turso-only, e.g. a plain `CREATE
        /// VIEW` the materialized view above reads from) that must be
        /// logged before `sql` so a replay of test.sql can resolve it.
        extra_setup: Vec<String>,
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
    /// Names of plain (non-materialized) views created as relation sources
    /// for materialized views. Turso-only, never checked directly.
    plain_views: Vec<String>,
    next_plain_view_id: usize,
    /// Round-robin cursor into `COLLATION_MUTATIONS`.
    collation_mutation_index: usize,
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

    /// Whether the runner should run the next collation/mixed-type probe
    /// mutation (see `COLLATION_MUTATIONS`) at this statement index.
    pub fn wants_collation_mutation(&self, stmt_index: usize) -> bool {
        stmt_index > 0 && stmt_index % CREATE_ATTEMPT_INTERVAL == 3
    }

    /// The next deterministic mutation of the collation/mixed-type probe
    /// table, cycling through `COLLATION_MUTATIONS`.
    pub fn next_collation_mutation(&mut self) -> String {
        let sql = COLLATION_MUTATIONS[self.collation_mutation_index % COLLATION_MUTATIONS.len()];
        self.collation_mutation_index += 1;
        sql.to_string()
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
        // Statements already executed (Turso-only) that a materialized-view
        // definition below depends on and that must be logged before it.
        let mut extra_setup = Vec::new();

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
        } else if !self.plain_views.is_empty() && rng.random_bool(0.1) {
            // A materialized view over an already-created plain view: the
            // plain view's own SELECT was already validated when it was
            // created, so a rejection here is an oracle failure.
            let source = quoted(&self.plain_views[rng.random_range(0..self.plain_views.len())]);
            GeneratedDefinition {
                sql: match rng.random_range(0..3) {
                    0 => format!("SELECT * FROM {source}"),
                    1 => format!("SELECT COUNT(*) FROM {source}"),
                    _ => format!("SELECT DISTINCT * FROM {source}"),
                },
                must_create: true,
            }
        } else if rng.random_bool(0.12) {
            // Create a brand-new plain view (possibly itself over another
            // plain view) and wrap it in the materialized view, instead of
            // reading tables directly.
            match self.try_create_plain_view(turso_conn, schema, rng) {
                Some((pv_sql, pv_name)) => {
                    extra_setup.push(pv_sql);
                    GeneratedDefinition {
                        sql: format!("SELECT * FROM {}", quoted(&pv_name)),
                        must_create: true,
                    }
                }
                None => generate_definition(schema, rng)?,
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
                Some(IvmCreateOutcome::Created {
                    sql,
                    leaf_probes,
                    extra_setup,
                })
            }
        }
    }

    /// Create a plain (non-materialized) `CREATE VIEW` as a relation source
    /// for a materialized view, executed on Turso only: nothing ever queries
    /// it against SQLite, only the materialized view wrapping it is checked.
    /// Sometimes built over an already-tracked plain view instead of a table
    /// (view over view). Returns `None` (not a rejection) when there is
    /// nothing to build it from, or the engine rejects the attempt.
    fn try_create_plain_view(
        &mut self,
        turso_conn: &Arc<turso_core::Connection>,
        schema: &Schema,
        rng: &mut ChaCha8Rng,
    ) -> Option<(String, String)> {
        let select = if !self.plain_views.is_empty() && rng.random_bool(0.4) {
            let source = quoted(&self.plain_views[rng.random_range(0..self.plain_views.len())]);
            match rng.random_range(0..3) {
                0 => format!("SELECT * FROM {source}"),
                1 => format!("SELECT DISTINCT * FROM {source}"),
                _ => format!("SELECT * FROM {source} WHERE 1"),
            }
        } else {
            let tables: Vec<&Table> = schema
                .tables
                .iter()
                .filter(|t| t.database.is_none() && !t.columns.is_empty())
                .collect();
            if tables.is_empty() {
                return None;
            }
            let table = tables[rng.random_range(0..tables.len())];
            let with_filter = rng.random_bool(0.4);
            projection(table, rng, with_filter)
        };

        let name = format!("ivm_pv{}", self.next_plain_view_id);
        self.next_plain_view_id += 1;
        let sql = format!("CREATE VIEW \"{name}\" AS {select}");
        match DifferentialOracle::execute_turso(turso_conn, &sql) {
            QueryResult::Error(_) => None,
            _ => {
                self.plain_views.push(name.clone());
                if self.plain_views.len() > MAX_PLAIN_VIEWS {
                    self.plain_views.remove(0);
                }
                Some((sql, name))
            }
        }
    }

    /// Whether the runner should run a FAIL-mode transaction (see
    /// `fail_mode_transaction`) at this statement index.
    pub fn wants_fail_mode_transaction(&self, stmt_index: usize) -> bool {
        !self.views.is_empty()
            && stmt_index % CREATE_ATTEMPT_INTERVAL == CREATE_ATTEMPT_INTERVAL / 2
    }

    /// Build an explicit transaction whose statements end with a FAIL-mode
    /// error partway through their rows, mixed with a savepoint:
    ///
    /// ```sql
    /// BEGIN;
    /// INSERT OR FAIL INTO t(rowid, ..) <copy of row r1 under a new rowid>
    ///                                  UNION ALL <row r2 under its own rowid>;
    /// SAVEPOINT s;
    /// UPDATE OR FAIL t SET rowid = .. WHERE rowid IN (r1, r2);
    /// INSERT INTO t(rowid, ..) <new copy of r1> UNION ALL <r2 again>;
    /// ROLLBACK TO s;   -- or RELEASE s
    /// COMMIT;          -- or ROLLBACK
    /// ```
    ///
    /// FAIL keeps the rows written before the conflict, so those rows must
    /// reach the views even though the statement fails; the plain INSERT
    /// aborts and must leave nothing behind. `r1 < r2` are the table's two
    /// smallest rowids, read from Turso (both engines hold the same rows).
    /// Returns `None` when no main-database table has two rows.
    pub fn fail_mode_transaction(
        &self,
        turso_conn: &Arc<turso_core::Connection>,
        schema: &Schema,
        rng: &mut ChaCha8Rng,
    ) -> Option<Vec<String>> {
        let tables: Vec<&Table> = schema
            .tables
            .iter()
            .filter(|table| table.database.is_none())
            .collect();
        if tables.is_empty() {
            return None;
        }
        let table = tables[rng.random_range(0..tables.len())];
        let name = quoted(&table.name);
        let rowids_sql = format!("SELECT rowid FROM {name} ORDER BY rowid LIMIT 2");
        let QueryResult::Rows(rows) = DifferentialOracle::execute_turso(turso_conn, &rowids_sql)
        else {
            return None;
        };
        let rowids: Vec<i64> = rows
            .rows()
            .iter()
            .filter_map(|row| match row.0.first() {
                Some(query_result_oracle::Value::Integer(rowid)) => Some(*rowid),
                _ => None,
            })
            .collect();
        let [r1, r2] = rowids[..] else {
            return None;
        };
        // New rowids far above the generated ones, so only the deliberate
        // conflicts below can collide.
        let offset = 1_000_000_000 + rng.random_range(0..1_000_000i64);
        let moved_r1 = r1.checked_add(offset)?;
        let copied_r1 = r1.checked_add(offset.checked_mul(2)?)?;
        let aborted_r1 = r1.checked_add(offset.checked_mul(3)?)?;

        // The INTEGER PRIMARY KEY column is the rowid, which is set
        // explicitly. A copy of a UNIQUE column would conflict on the first
        // row already, so copy NULL instead where the column allows it.
        let unique_index_columns: Vec<&String> = schema
            .indexes
            .iter()
            .filter(|index| {
                index.unique && index.database.is_none() && index.table_name == table.name
            })
            .flat_map(|index| index.columns.iter())
            .collect();
        let columns: Vec<&ColumnDef> = table
            .columns
            .iter()
            .filter(|column| !(column.primary_key && column.data_type == DataType::Integer))
            .collect();
        let mut target = String::from("rowid");
        let mut copied = String::new();
        for column in &columns {
            let column_name = quoted(&column.name);
            target.push_str(", ");
            target.push_str(&column_name);
            copied.push_str(", ");
            let unique = column.unique || unique_index_columns.contains(&&column.name);
            if unique && column.nullable {
                copied.push_str("NULL");
            } else {
                copied.push_str(&column_name);
            }
        }
        let copy_r1_then_conflict_on_r2 = |new_rowid: i64| {
            format!(
                "INSERT {{conflict}}INTO {name} ({target}) \
                 SELECT {new_rowid}{copied} FROM {name} WHERE rowid = {r1} \
                 UNION ALL SELECT rowid{copied} FROM {name} WHERE rowid = {r2}"
            )
        };
        let savepoint = "ivm_fail_sp";
        Some(vec![
            "BEGIN".to_string(),
            copy_r1_then_conflict_on_r2(moved_r1).replace("{conflict}", "OR FAIL "),
            format!("SAVEPOINT {savepoint}"),
            // Rows are visited in rowid order: r1 moves to a new rowid, then
            // r2 tries to take the rowid the FAIL insert just used (it
            // conflicts when that insert kept its first row).
            format!(
                "UPDATE OR FAIL {name} SET rowid = CASE rowid WHEN {r1} THEN {copied_r1} \
                 ELSE {moved_r1} END WHERE rowid IN ({r1}, {r2})"
            ),
            copy_r1_then_conflict_on_r2(aborted_r1).replace("{conflict}", ""),
            if rng.random_bool(0.5) {
                format!("ROLLBACK TO {savepoint}")
            } else {
                format!("RELEASE {savepoint}")
            },
            if rng.random_bool(0.8) {
                "COMMIT".to_string()
            } else {
                "ROLLBACK".to_string()
            },
        ])
    }

    /// Verify the IVM invariant for every tracked view: the materialized
    /// contents (as a user would read them) must equal a fresh evaluation of
    /// the defining query, as multisets.
    pub fn check_views(&self, turso_conn: &Arc<turso_core::Connection>) -> OracleResult {
        let mut failures = Vec::new();
        for view in &self.views {
            match turso_conn.materialized_view_rows_depend_on_chosen_values(&view.name) {
                Ok(false) => {}
                Ok(true) => continue,
                Err(error) => {
                    failures.push(format!(
                        "IVM check: planning {} failed: {error}\n  definition: {}",
                        view.name, view.definition
                    ));
                    continue;
                }
            }
            let view_sql = format!("SELECT * FROM \"{}\"", view.name);
            let view_result = DifferentialOracle::execute_turso(turso_conn, &view_sql);
            let query_result = DifferentialOracle::execute_turso(turso_conn, &view.definition);

            let (view_rows, query_rows) = match (view_result, query_result) {
                (QueryResult::Rows(v), QueryResult::Rows(q)) => (v, q),
                (QueryResult::Error(e), _) => {
                    failures.push(format!(
                        "IVM check: reading materialized view {} failed: {e}\n  definition: {}",
                        view.name, view.definition
                    ));
                    continue;
                }
                (_, QueryResult::Error(e)) => {
                    failures.push(format!(
                        "IVM check: re-evaluating defining query of {} failed: {e}\n  definition: {}",
                        view.name, view.definition
                    ));
                    continue;
                }
                (QueryResult::Ok, QueryResult::Ok) => {
                    failures.push(format!(
                        "IVM check: both SELECTs for {} returned no result columns\n  definition: {}",
                        view.name, view.definition
                    ));
                    continue;
                }
                (QueryResult::Rows(rows), QueryResult::Ok) => {
                    failures.push(format!(
                        "IVM check: materialized view {} returned {} columns but its defining SELECT returned no result columns\n  definition: {}",
                        view.name,
                        rows.column_count(),
                        view.definition
                    ));
                    continue;
                }
                (QueryResult::Ok, QueryResult::Rows(rows)) => {
                    failures.push(format!(
                        "IVM check: materialized view {} returned no result columns but its defining SELECT returned {}\n  definition: {}",
                        view.name,
                        rows.column_count(),
                        view.definition
                    ));
                    continue;
                }
            };

            // SQL leaves open which of several equal values a GROUP BY key,
            // DISTINCT row, MIN/MAX or compound row shows ('a' or 'A' under
            // NOCASE). In the columns where the view made that choice, accept
            // any value equal under the column's collation; compare every
            // other column exactly.
            let chosen = match chosen_value_collations(turso_conn, &view.name) {
                Ok(chosen) => chosen,
                Err(error) => {
                    failures.push(format!(
                        "IVM check: {error}\n  definition: {}",
                        view.definition
                    ));
                    continue;
                }
            };
            let diff = diff_result_sets_with_chosen_values(&view_rows, &query_rows, &chosen);
            if !diff.is_empty() {
                failures.push(format!(
                    "IVM invariant violated for {}:\n  definition: {}\n{}",
                    view.name,
                    view.definition,
                    diff.describe("materialized view", "fresh query", 20)
                ));
            }
        }
        if failures.is_empty() {
            OracleResult::Pass
        } else {
            OracleResult::Fail(failures.join("\n\n"))
        }
    }
}

/// Per column of view `view_name`, the collation under which the view chose
/// the value it shows among several that compare equal, or `None` when the
/// column's value is exact.
fn chosen_value_collations(
    turso_conn: &Arc<turso_core::Connection>,
    view_name: &str,
) -> Result<Vec<Option<ChosenValueCollation>>, String> {
    turso_conn
        .materialized_view_chosen_value_collations(view_name)
        .map_err(|error| {
            format!("reading the chosen-value columns of {view_name} failed: {error}")
        })?
        .into_iter()
        .map(|collation| {
            collation
                .map(|name| {
                    ChosenValueCollation::from_name(&name).ok_or_else(|| {
                        format!("{view_name} chose a value under unknown collation {name}")
                    })
                })
                .transpose()
        })
        .collect()
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
    let (sql, must_create) = match rng.random_range(0..48u32) {
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
            true,
        ),
        // Recursive relational generation: operators compose by consuming
        // the typed output of generated children rather than selecting a
        // named end-to-end shape.
        15..=27 => (
            compositional_definition(&tables, rng, 3).map(|relation| relation.sql),
            true,
        ),
        // FULL JOIN under an aggregate or DISTINCT consumer.
        28 => (full_outer_composition(&tables, rng, true), true),
        // GROUP BY/DISTINCT/MIN-MAX/set-op keys under NOCASE/RTRIM collation
        // (declared on the probe table's columns, or applied explicitly).
        // Falls back to a plain projection when the probe table isn't part
        // of this schema (e.g. unit tests, or it was somehow dropped).
        29..=31 => (
            collation_view(schema, rng).or_else(|| Some(projection(table, rng, true))),
            true,
        ),
        // TOTAL()/AVG() over mixed INTEGER/REAL/NULL data.
        32..=33 => (
            total_avg_view(schema, rng).or_else(|| Some(projection(table, rng, true))),
            true,
        ),
        // NATURAL JOIN and CROSS JOIN: neither is produced by `join()`.
        34..=36 => (
            natural_or_cross_join(&tables, rng).or_else(|| Some(projection(table, rng, true))),
            true,
        ),
        // Runs of three to five inner joins, maintained as one delta join.
        42..=47 => (
            inner_join_chain(&tables, rng).or_else(|| Some(projection(table, rng, true))),
            true,
        ),
        _ => (full_outer_composition(&tables, rng, false), true),
    };
    sql.map(|sql| GeneratedDefinition { sql, must_create })
}

/// `GROUP BY`/`DISTINCT`/`MIN`/`MAX`/set-op views over the probe table's
/// collated text columns, exercising both a column's own declared collation
/// (`nocase_key`, `rtrim_key`) and an explicit `COLLATE` clause applied to a
/// column with none declared (`plain_key`). `None` when the probe table has
/// been dropped (it never is in IVM mode: DROP/ALTER TABLE are disabled, see
/// `SqlGenBackend::new_with_options`) or wasn't bootstrapped.
fn collation_view(schema: &Schema, rng: &mut ChaCha8Rng) -> Option<String> {
    schema.get_table(COLLATION_PROBE_TABLE)?;
    let collation = if rng.random_bool(0.5) {
        "NOCASE"
    } else {
        "RTRIM"
    };
    let key = if rng.random_bool(0.5) {
        match collation {
            "NOCASE" => "\"nocase_key\"".to_string(),
            _ => "\"rtrim_key\"".to_string(),
        }
    } else {
        format!("\"plain_key\" COLLATE {collation}")
    };
    let table = quoted(COLLATION_PROBE_TABLE);
    Some(match rng.random_range(0..5u8) {
        0 => format!("SELECT {key} AS k, COUNT(*) AS cnt FROM {table} GROUP BY {key}"),
        1 => format!("SELECT DISTINCT {key} AS k FROM {table}"),
        2 => format!("SELECT MIN({key}) AS k FROM {table}"),
        3 => format!("SELECT MAX({key}) AS k FROM {table}"),
        _ => format!(
            "SELECT {key} AS k FROM {table} \
             UNION SELECT {key} FROM {table} WHERE id <> 1"
        ),
    })
}

/// `TOTAL()`/`AVG()` over the probe table's mixed INTEGER/REAL/NULL column.
/// `aggregate()`/`scalar_aggregate()` never pick this column: it is declared
/// with no type (BLOB/NONE affinity) precisely so NUMERIC affinity cannot
/// fold `2` and `2.0` into one storage class, which also makes it a non-
/// numeric `DataType` to every generic numeric-column filter in this file.
fn total_avg_view(schema: &Schema, rng: &mut ChaCha8Rng) -> Option<String> {
    schema.get_table(COLLATION_PROBE_TABLE)?;
    let table = quoted(COLLATION_PROBE_TABLE);
    let col = "\"mix_num\"";
    Some(if rng.random_bool(0.4) {
        format!("SELECT TOTAL({col}) AS tot, AVG({col}) AS avg_val, COUNT(*) AS cnt FROM {table}")
    } else {
        format!(
            "SELECT \"nocase_key\" AS k, TOTAL({col}) AS tot, AVG({col}) AS avg_val \
             FROM {table} GROUP BY \"nocase_key\""
        )
    })
}

/// An inner join of three to five tables (a table may appear more than
/// once), each joined to an earlier one on columns of the same type,
/// sometimes on two columns, with a filter, or under GROUP BY. A run of inner
/// joins is maintained as one delta join, so these views exercise every
/// phase and every way a level finds its rows.
fn inner_join_chain(tables: &[&Table], rng: &mut ChaCha8Rng) -> Option<String> {
    let keyable = |column: &ColumnDef| {
        matches!(
            column.data_type,
            DataType::Integer | DataType::Real | DataType::Text
        )
    };
    let count = rng.random_range(3..=5usize);
    let chosen = (0..count)
        .map(|_| tables[rng.random_range(0..tables.len())])
        .collect::<Vec<_>>();
    let mut from = format!("{} AS j0", quoted(&chosen[0].name));
    for position in 1..count {
        let mut pairs = Vec::new();
        for earlier in 0..position {
            for earlier_column in &chosen[earlier].columns {
                for column in &chosen[position].columns {
                    if keyable(column) && earlier_column.data_type == column.data_type {
                        pairs.push((earlier, earlier_column, column));
                    }
                }
            }
        }
        if pairs.is_empty() {
            return None;
        }
        let mut on = Vec::new();
        for _ in 0..if rng.random_bool(0.2) { 2 } else { 1 } {
            let (earlier, earlier_column, column) = pairs[rng.random_range(0..pairs.len())];
            on.push(format!(
                "j{earlier}.{} = j{position}.{}",
                quoted(&earlier_column.name),
                quoted(&column.name)
            ));
        }
        let _ = write!(
            from,
            " JOIN {} AS j{position} ON {}",
            quoted(&chosen[position].name),
            on.join(" AND ")
        );
    }
    let filter = if rng.random_bool(0.3) {
        let position = rng.random_range(0..count);
        let column = &chosen[position].columns[rng.random_range(0..chosen[position].columns.len())];
        format!(" WHERE j{position}.{} IS NOT NULL", quoted(&column.name))
    } else {
        String::new()
    };
    let first_column = |position: usize| quoted(&chosen[position].columns[0].name);
    let random_column = |rng: &mut ChaCha8Rng| {
        let position = rng.random_range(0..count);
        let columns = &chosen[position].columns;
        format!(
            "j{position}.{}",
            quoted(&columns[rng.random_range(0..columns.len())].name)
        )
    };
    Some(if rng.random_bool(0.25) {
        let group = random_column(rng);
        let aggregated = random_column(rng);
        format!(
            "SELECT {group} AS g, COUNT(*) AS n, MAX({aggregated}) AS m \
             FROM {from}{filter} GROUP BY {group}"
        )
    } else {
        let columns = (0..count)
            .map(|position| format!("j{position}.{} AS c{position}", first_column(position)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("SELECT {columns} FROM {from}{filter}")
    })
}

/// `CROSS JOIN` (no predicate at all) and `NATURAL JOIN` (merges every
/// same-named, same-typed column): neither is produced by `join()`, which
/// always joins on an explicit `ON`/`USING` predicate. NATURAL JOIN needs a
/// shared column name between two different tables (most generated tables
/// share an `id` primary key, and so does the collation probe table).
fn natural_or_cross_join(tables: &[&Table], rng: &mut ChaCha8Rng) -> Option<String> {
    if rng.random_bool(0.5) {
        let left = tables[rng.random_range(0..tables.len())];
        let right = tables[rng.random_range(0..tables.len())];
        let mut cols = Vec::new();
        for c in column_subset(left, rng).iter().take(2) {
            cols.push(format!("l.{} AS l_{}", quoted(&c.name), c.name));
        }
        for c in column_subset(right, rng).iter().take(2) {
            cols.push(format!("r.{} AS r_{}", quoted(&c.name), c.name));
        }
        if cols.is_empty() {
            return None;
        }
        Some(format!(
            "SELECT {cols} FROM {lt} AS l CROSS JOIN {rt} AS r",
            cols = cols.join(", "),
            lt = quoted(&left.name),
            rt = quoted(&right.name),
        ))
    } else {
        let mut candidates = Vec::new();
        for left in tables {
            for right in tables {
                if left.name.eq_ignore_ascii_case(&right.name) {
                    continue;
                }
                let shares_column = left.columns.iter().any(|lc| {
                    right.columns.iter().any(|rc| {
                        lc.name.eq_ignore_ascii_case(&rc.name) && lc.data_type == rc.data_type
                    })
                });
                if shares_column {
                    candidates.push((*left, *right));
                }
            }
        }
        if candidates.is_empty() {
            return None;
        }
        let (left, right) = candidates[rng.random_range(0..candidates.len())];
        Some(format!(
            "SELECT * FROM {} NATURAL JOIN {}",
            quoted(&left.name),
            quoted(&right.name)
        ))
    }
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
            // The value column comes from a side the join never pads.
            let (join, value) = match rng.random_range(0..9u8) {
                0..=2 => ("JOIN", "l.value"),
                3 => ("LEFT JOIN", "l.value"),
                4 => ("RIGHT JOIN", "r.value"),
                5 => ("FULL JOIN", "COALESCE(l.value, r.value)"),
                // EXISTS, IN, and NOT EXISTS: the planner turns these into
                // semi and anti joins.
                kind => {
                    let right = &right.sql;
                    let test = match kind {
                        6 => {
                            format!("EXISTS (SELECT 1 FROM ({right}) AS r WHERE r.value = l.value)")
                        }
                        7 => format!(
                            "l.value IN (SELECT r.value FROM ({right}) AS r WHERE r.value >= l.value)"
                        ),
                        _ => format!(
                            "NOT EXISTS (SELECT 1 FROM ({right}) AS r WHERE r.value = l.value)"
                        ),
                    };
                    return Some(ComposedRelation {
                        sql: format!(
                            "SELECT l.value AS value FROM ({}) AS l WHERE {test}",
                            left.sql
                        ),
                        data_type,
                    });
                }
            };
            Some(ComposedRelation {
                sql: format!(
                    "SELECT {value} AS value FROM ({}) AS l {join} ({}) AS r \
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
            let kinds = ["JOIN", "JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"];
            let first = kinds[rng.random_range(0..kinds.len())];
            let second = kinds[rng.random_range(0..kinds.len())];
            let mut sql = format!(
                "SELECT l.{lcol} AS a, r.{rcol} AS b, t.{tcol} AS c \
                 FROM {lt} AS l {first} {rt} AS r ON l.{lc} = r.{rc} \
                 {second} {tt} AS t ON r.{rc2} = t.{tc}",
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
            );
            // A WHERE term on the first table must run after any later join
            // that can fill that table with NULLs.
            if rng.random_bool(0.3) {
                let _ = write!(
                    sql,
                    " WHERE l.{lc} IS NULL OR l.{lc} >= r.{rc}",
                    lc = quoted(&lc.name),
                    rc = quoted(&rc.name)
                );
            }
            return Some(sql);
        }
    }

    // Sometimes keep only the left rows that have (or lack) a match, spelled
    // as EXISTS, IN, or NOT EXISTS; the planner turns these into semi and
    // anti joins.
    if rng.random_bool(0.2) {
        let (rt, lc, rc) = (quoted(&right.name), quoted(&lc.name), quoted(&rc.name));
        let test = match rng.random_range(0..3u8) {
            0 => format!("EXISTS (SELECT 1 FROM {rt} AS r WHERE r.{rc} = l.{lc})"),
            1 => format!("l.{lc} IN (SELECT r.{rc} FROM {rt} AS r WHERE r.{rc} >= l.{lc})"),
            _ => format!("NOT EXISTS (SELECT 1 FROM {rt} AS r WHERE r.{rc} = l.{lc})"),
        };
        let left_cols = column_subset(left, rng)
            .iter()
            .take(2)
            .map(|c| format!("l.{} AS l_{}", quoted(&c.name), c.name))
            .collect::<Vec<_>>();
        return Some(format!(
            "SELECT {cols} FROM {lt} AS l WHERE {test}",
            cols = left_cols.join(", "),
            lt = quoted(&left.name),
        ));
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

    // Sometimes make it a RIGHT or FULL JOIN: unmatched rows of the right
    // (and for FULL, also the left) side appear NULL-padded.
    if rng.random_bool(0.3) {
        let join = if rng.random_bool(0.5) {
            "RIGHT JOIN"
        } else {
            "FULL JOIN"
        };
        // USING merges the column: the right copy for RIGHT, COALESCE for
        // FULL.
        if lc.name.eq_ignore_ascii_case(&rc.name)
            && !left.name.eq_ignore_ascii_case(&right.name)
            && rng.random_bool(0.4)
        {
            return Some(format!(
                "SELECT * FROM {lt} {join} {rt} USING ({c})",
                lt = quoted(&left.name),
                rt = quoted(&right.name),
                c = quoted(&lc.name),
            ));
        }
        let mut sql = format!(
            "SELECT {cols} FROM {lt} AS l {join} {rt} AS r ON l.{lc} = r.{rc}",
            cols = cols.join(", "),
            lt = quoted(&left.name),
            rt = quoted(&right.name),
            lc = quoted(&lc.name),
            rc = quoted(&rc.name),
        );
        match rng.random_range(0..3u8) {
            0 => {
                let _ = write!(sql, " WHERE l.{} IS NULL", quoted(&lc.name));
            }
            // A WHERE term reading both sides must run after the padding.
            1 => {
                let _ = write!(
                    sql,
                    " WHERE l.{lc} IS NULL OR l.{lc} >= r.{rc}",
                    lc = quoted(&lc.name),
                    rc = quoted(&rc.name)
                );
            }
            _ => {}
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
    use turso_core::{ConnectOpts, Database, SqliteDialect};

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
        db.connect(ConnectOpts::new()).unwrap()
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
    fn check_views_reports_every_exact_mismatch() {
        let conn = open_turso_with_views("ivm-exact-mismatch-test.db");
        for sql in [
            "CREATE TABLE t(x INTEGER)",
            "INSERT INTO t VALUES (1), (1)",
            "CREATE MATERIALIZED VIEW v_multiplicity AS SELECT x FROM t",
            "CREATE MATERIALIZED VIEW v_null AS SELECT NULL AS value FROM t",
            "CREATE MATERIALIZED VIEW v_type AS SELECT x FROM t",
            "CREATE MATERIALIZED VIEW v_arity AS SELECT x FROM t WHERE 0",
        ] {
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "setup failed: {sql}"
            );
        }

        let mut state = IvmState::new();
        state.views.extend([
            IvmView {
                name: "v_multiplicity".into(),
                definition: "SELECT DISTINCT x FROM t".into(),
            },
            IvmView {
                name: "v_null".into(),
                definition: "SELECT '' FROM t".into(),
            },
            IvmView {
                name: "v_type".into(),
                definition: "SELECT CAST(x AS REAL) FROM t".into(),
            },
            IvmView {
                name: "v_arity".into(),
                definition: "SELECT x, x FROM t WHERE 0".into(),
            },
        ]);

        let OracleResult::Fail(failure) = state.check_views(&conn) else {
            panic!("exact mismatches unexpectedly passed");
        };
        for name in ["v_multiplicity", "v_null", "v_type", "v_arity"] {
            assert!(
                failure.contains(name),
                "missing failure for {name}: {failure}"
            );
        }
        assert!(failure.contains("1 row occurrence(s)"));
        assert!(failure.contains("null"));
        assert!(failure.contains("text(\"\")"));
        assert!(failure.contains("integer(1)"));
        assert!(failure.contains("real(1.0)"));
        assert!(
            failure.contains("column count differs: materialized view has 1, fresh query has 2")
        );
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
