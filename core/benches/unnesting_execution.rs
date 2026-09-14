//! Prepared execution across correlation cardinalities and physical alternatives.
//! Each fixture is checked against SQLite before measurement. Set
//! TURSO_BENCH_PLAN_DIR to retain the SQL, data configuration and physical plan.

use divan::{black_box, AllocProfiler, Bencher};
use mimalloc::MiMalloc;
use std::sync::Arc;
#[cfg(feature = "simulator")]
use turso_core::SubqueryUnnestingMode;
use turso_core::{Connection, Database, MemoryIO, SqliteDialect, Statement, StepResult};

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

const CASES: &[&str] = &[
    "exists_outer_16",
    "exists_outer_256",
    "exists_outer_1024",
    "exists_distinct_16",
    "exists_distinct_256",
    "exists_indexed",
    "exists_low_selectivity",
    "exists_high_selectivity",
    "exists_nulls",
    "exists_skewed",
    "anti_or_nulls",
    "inequality",
    "inequality_indexed",
    "scalar_sum_equality",
    "scalar_sum_inequality",
    "scalar_first_ordered",
    "nested_depth_2",
    "nested_depth_4",
    "derived_limit",
    "joined_input_equality",
    "joined_input_inequality",
    "joined_input_anti",
    "scalar_count_indexed_small",
    "nested_local_depth_2",
    "nested_local_depth_4",
    "nested_local_anti",
    "membership_in_inequality",
    "membership_in_indexed",
    "membership_not_in_inequality",
    "membership_row_not_in_nulls",
    "membership_in_small_indexed",
    "membership_in_outer_projection",
    "membership_not_in_outer_projection",
    "membership_row_not_in_outer_projection",
    "membership_in_joined_projection",
    "membership_not_in_joined_projection",
    "membership_row_not_in_joined_projection",
    "membership_in_joined_projection_small_indexed",
    "exists_result_with_exists_filter",
    "in_result_with_exists_filter",
    "row_not_in_result_with_exists_filter",
    "left_join_with_exists_filter",
    "left_join_rewritten_input",
];

fn main() {
    divan::main();
}

#[turso_macros::divan_bench(args = CASES)]
fn automatic(bencher: Bencher, case: &str) {
    bench_execution(bencher, case, "auto", |_| {});
}

#[cfg(feature = "simulator")]
#[turso_macros::divan_bench(args = CASES)]
fn forced(bencher: Bencher, case: &str) {
    bench_execution(bencher, case, "forced", |conn| {
        conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Forced);
    });
}

#[cfg(feature = "simulator")]
#[turso_macros::divan_bench(args = CASES)]
fn disabled(bencher: Bencher, case: &str) {
    bench_execution(bencher, case, "disabled", |conn| {
        conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Disabled);
    });
}

fn bench_execution(bencher: Bencher, name: &str, mode_name: &str, configure: fn(&Connection)) {
    let case = Case::named(name);
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in case.setup() {
        sqlite.execute_batch(&sql).unwrap();
        let mut stmt = conn.prepare(&sql).unwrap();
        collect_rows(&db, &mut stmt, |_| ());
    }
    configure(&conn);
    let sql = case.query(name);
    let expected = sqlite
        .prepare(&sql)
        .unwrap()
        .query_map([], |row| {
            (0..row.as_ref().column_count())
                .map(|column| row.get::<_, Option<i64>>(column))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let mut stmt = conn.prepare(&sql).unwrap();
    let actual = collect_rows(&db, &mut stmt, |row| {
        (0..row.len())
            .map(|column| match row.get_value(column) {
                turso_core::Value::Null => None,
                turso_core::Value::Numeric(turso_core::Numeric::Integer(value)) => Some(*value),
                value => panic!("execution fixture returned a non-integer value: {value:?}"),
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(actual, expected, "{name}/{mode_name}: {sql}");
    retain_plan(&db, &conn, &case, name, mode_name, &sql, expected.len());
    bencher.bench_local(|| measure_execution(black_box(&db), black_box(&mut stmt)));
}

#[derive(serde::Serialize)]
struct Case {
    outer_rows: usize,
    outer_distinct: usize,
    inner_rows: usize,
    inner_distinct: usize,
    null_every: Option<usize>,
    indexed: bool,
    skewed: bool,
}

impl Case {
    fn named(name: &str) -> Self {
        let mut case = Self {
            outer_rows: 256,
            outer_distinct: 64,
            inner_rows: 256,
            inner_distinct: 32,
            null_every: None,
            indexed: false,
            skewed: false,
        };
        match name {
            "exists_outer_16" => case.outer_rows = 16,
            "exists_outer_256" => {}
            "exists_outer_1024" => case.outer_rows = 1024,
            "exists_distinct_16" => case.outer_distinct = 16,
            "exists_distinct_256" => case.outer_distinct = 256,
            "exists_indexed" | "inequality_indexed" => case.indexed = true,
            "membership_in_indexed" => case.indexed = true,
            "exists_low_selectivity" => case.inner_distinct = 4,
            "exists_high_selectivity" => case.inner_distinct = 64,
            "exists_nulls" | "anti_or_nulls" => case.null_every = Some(4),
            "exists_skewed" => case.skewed = true,
            "scalar_count_indexed_small" => {
                case.outer_rows = 16;
                case.inner_rows = 4096;
                case.indexed = true;
            }
            "membership_in_small_indexed" | "membership_in_joined_projection_small_indexed" => {
                case.outer_rows = 16;
                case.inner_rows = 4096;
                case.indexed = true;
            }
            "membership_row_not_in_nulls"
            | "membership_row_not_in_outer_projection"
            | "membership_row_not_in_joined_projection"
            | "in_result_with_exists_filter"
            | "row_not_in_result_with_exists_filter"
            | "left_join_with_exists_filter"
            | "left_join_rewritten_input" => {
                case.null_every = Some(4);
            }
            "nested_local_depth_2" | "nested_local_depth_4" | "nested_local_anti" => {
                case.outer_rows = 64;
                case.inner_rows = 128;
            }
            "inequality"
            | "exists_result_with_exists_filter"
            | "scalar_sum_equality"
            | "scalar_sum_inequality"
            | "scalar_first_ordered"
            | "nested_depth_2"
            | "nested_depth_4"
            | "derived_limit"
            | "joined_input_equality"
            | "joined_input_inequality"
            | "joined_input_anti"
            | "membership_in_inequality"
            | "membership_not_in_inequality"
            | "membership_in_outer_projection"
            | "membership_not_in_outer_projection"
            | "membership_in_joined_projection"
            | "membership_not_in_joined_projection" => {}
            _ => panic!("unknown execution workload: {name}"),
        }
        case
    }

    fn setup(&self) -> Vec<String> {
        let mut setup = vec![
            "CREATE TABLE outer_rows(id INTEGER PRIMARY KEY, k INTEGER)".to_owned(),
            "CREATE TABLE inner_rows(k INTEGER, v INTEGER)".to_owned(),
        ];
        for (table, rows, distinct) in [
            ("outer_rows", self.outer_rows, self.outer_distinct),
            ("inner_rows", self.inner_rows, self.inner_distinct),
        ] {
            let values = (0..rows)
                .map(|id| {
                    let key = if self.null_every.is_some_and(|every| id % every == 0) {
                        "NULL".to_owned()
                    } else if self.skewed && id % 10 != 0 {
                        "0".to_owned()
                    } else {
                        (id % distinct).to_string()
                    };
                    if table == "outer_rows" {
                        format!("({id},{key})")
                    } else {
                        format!("({key},{})", id % 11)
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            setup.push(format!("INSERT INTO {table} VALUES {values}"));
        }
        if self.indexed {
            setup.push("CREATE INDEX inner_key ON inner_rows(k, v)".to_owned());
        }
        setup.push("ANALYZE".to_owned());
        setup
    }

    fn query(&self, name: &str) -> String {
        let left_join_input = match name {
            "left_join_with_exists_filter" => Some("inner_rows r"),
            "left_join_rewritten_input" => Some(
                "(SELECT i.k,i.v FROM inner_rows i WHERE EXISTS
                 (SELECT 1 FROM inner_rows j WHERE j.v>i.v) ORDER BY i.k,i.v LIMIT 16) r",
            ),
            _ => None,
        };
        if let Some(input) = left_join_input {
            return format!(
                "SELECT o.id,r.k,r.v FROM outer_rows o LEFT JOIN {input} ON o.k=r.k
                 WHERE o.id>=0 AND o.id>=0 AND EXISTS
                 (SELECT 1 FROM inner_rows w WHERE w.v>o.k) ORDER BY o.id,r.k,r.v"
            );
        }
        let marked_result = match name {
            "exists_result_with_exists_filter" => {
                Some("EXISTS (SELECT 1 FROM inner_rows m WHERE m.k=o.k AND m.v<o.k)")
            }
            "in_result_with_exists_filter" => {
                Some("o.id%7 IN (SELECT m.k FROM inner_rows m WHERE m.v<o.k)")
            }
            "row_not_in_result_with_exists_filter" => {
                Some("(o.k,o.id%11) NOT IN (SELECT m.k,m.v FROM inner_rows m WHERE m.v>=o.k)")
            }
            _ => None,
        };
        if let Some(marked_result) = marked_result {
            return format!(
                "SELECT o.id,{marked_result} FROM outer_rows o
                 WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.v>o.k) ORDER BY o.id"
            );
        }
        let predicate = match name {
            "anti_or_nulls" => {
                "NOT EXISTS (SELECT 1 FROM inner_rows i WHERE i.k < o.k OR i.k IS o.k)".to_owned()
            }
            "inequality" | "inequality_indexed" => {
                "EXISTS (SELECT 1 FROM inner_rows i WHERE i.k > o.k)".to_owned()
            }
            "scalar_sum_equality" => {
                "(SELECT sum(i.v) FROM inner_rows i WHERE i.k = o.k) > 20".to_owned()
            }
            "scalar_sum_inequality" => {
                "(SELECT sum(i.v) FROM inner_rows i WHERE i.k > o.k) > 20".to_owned()
            }
            "scalar_count_indexed_small" => {
                "(SELECT count(*) FROM inner_rows i WHERE i.k = o.k) > 2".to_owned()
            }
            "membership_in_inequality"
            | "membership_in_indexed"
            | "membership_in_small_indexed" => {
                "o.k IN (SELECT i.k FROM inner_rows i WHERE i.v > o.k)".to_owned()
            }
            "membership_not_in_inequality" => {
                "o.k NOT IN (SELECT i.k FROM inner_rows i WHERE i.v > o.k)".to_owned()
            }
            "membership_row_not_in_nulls" => {
                "(o.k, o.id % 11) NOT IN (SELECT i.k, i.v FROM inner_rows i WHERE i.v > o.k)"
                    .to_owned()
            }
            "membership_in_outer_projection" => {
                "o.k+1 IN (SELECT i.k+o.id%11 FROM inner_rows i)".to_owned()
            }
            "membership_not_in_outer_projection" => {
                "o.k NOT IN (SELECT i.k+o.id%11 FROM inner_rows i WHERE i.v>o.k)".to_owned()
            }
            "membership_row_not_in_outer_projection" => "(o.k,o.id%11) NOT IN
                 (SELECT i.k+o.id%11,i.v FROM inner_rows i WHERE i.v>o.k)"
                .to_owned(),
            "membership_in_joined_projection" => "o.k+1 IN
                 (SELECT i.k+o.id%11+j.v-i.v FROM inner_rows i JOIN inner_rows j
                  ON i.k=j.k AND i.v=j.v)"
                .to_owned(),
            "membership_not_in_joined_projection" => "o.k NOT IN
                 (SELECT i.k+o.id%11+j.v-i.v FROM inner_rows i JOIN inner_rows j
                  ON i.k=j.k AND i.v=j.v WHERE i.v>o.k)"
                .to_owned(),
            "membership_row_not_in_joined_projection" => "(o.k,o.id%11) NOT IN
                 (SELECT i.k+o.id%11,j.v FROM inner_rows i JOIN inner_rows j
                  ON i.k IS j.k AND i.v=j.v WHERE i.v>o.k)"
                .to_owned(),
            "membership_in_joined_projection_small_indexed" => "o.k+o.id%11 IN
                 (SELECT i.k+o.id%11+j.v-i.v FROM inner_rows i JOIN inner_rows j
                  ON i.k=j.k AND i.v=j.v WHERE i.k=o.k)"
                .to_owned(),
            "joined_input_equality" => "EXISTS (SELECT 1 FROM inner_rows i JOIN inner_rows j
                    ON i.k = j.k AND i.v = j.v WHERE i.k = o.k)"
                .to_owned(),
            "joined_input_inequality" => "EXISTS (SELECT 1 FROM inner_rows i JOIN inner_rows j
                    ON i.k = j.k AND i.v = j.v WHERE i.k > o.k)"
                .to_owned(),
            "joined_input_anti" => "NOT EXISTS (SELECT 1 FROM inner_rows i JOIN inner_rows j
                    ON i.k IS j.k AND i.v = j.v WHERE i.k > o.k)"
                .to_owned(),
            "scalar_first_ordered" => {
                "(SELECT i.v FROM inner_rows i WHERE i.k > o.k ORDER BY i.v DESC LIMIT 1) > 5"
                    .to_owned()
            }
            "nested_depth_2" => nested_exists(2, true),
            "nested_depth_4" => nested_exists(4, true),
            "nested_local_depth_2" => nested_exists(2, false),
            "nested_local_depth_4" => nested_exists(4, false),
            "nested_local_anti" => format!("NOT {}", nested_exists(2, false)),
            "derived_limit" => {
                return "SELECT d.id FROM (
                SELECT o.id, o.k FROM outer_rows o
                WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.k > o.k)
                ORDER BY o.id DESC LIMIT 16
            ) d WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.k > d.k) ORDER BY d.id"
                    .to_owned()
            }
            _ => "EXISTS (SELECT 1 FROM inner_rows i WHERE i.k = o.k)".to_owned(),
        };
        format!("SELECT o.id FROM outer_rows o WHERE {predicate} ORDER BY o.id")
    }
}

fn nested_exists(depth: usize, distant: bool) -> String {
    let mut predicate = "1".to_owned();
    for level in (1..=depth).rev() {
        let parent = if level == 1 {
            "o".to_owned()
        } else {
            format!("i{}", level - 1)
        };
        let (comparison, distant_predicate) = if distant {
            ("=", format!(" AND i{level}.v >= o.k"))
        } else {
            (">", String::new())
        };
        predicate = format!(
            "EXISTS (SELECT 1 FROM inner_rows i{level}
            WHERE i{level}.k {comparison} {parent}.k{distant_predicate} AND {predicate})"
        );
    }
    predicate
}

fn collect_rows<T>(
    db: &Database,
    stmt: &mut Statement,
    mut read: impl FnMut(&turso_core::Row) -> T,
) -> Vec<T> {
    let mut rows = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => rows.push(read(stmt.row().unwrap())),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => panic!("unexpected execution result"),
        }
    }
    stmt.reset().unwrap();
    rows
}

fn retain_plan(
    db: &Database,
    conn: &Arc<Connection>,
    case: &Case,
    name: &str,
    mode: &str,
    sql: &str,
    rows: usize,
) {
    let Some(directory) = std::env::var_os("TURSO_BENCH_PLAN_DIR") else {
        return;
    };
    let mut explain = conn
        .prepare(format!("EXPLAIN QUERY PLAN FORMAT=JSON {sql}"))
        .unwrap();
    let plans = collect_rows(db, &mut explain, |row| row.get::<String>(0).unwrap());
    let [plan] = plans.as_slice() else {
        panic!("EXPLAIN should return one plan")
    };
    let record = serde_json::json!({
        "case": name, "mode": mode, "data": case, "sql": sql,
        "ordered_result_rows": rows, "plan": serde_json::from_str::<serde_json::Value>(plan).unwrap(),
    });
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        std::path::Path::new(&directory).join(format!("{name}-{mode}.json")),
        serde_json::to_string_pretty(&record).unwrap(),
    )
    .unwrap();
}

#[inline(never)]
fn measure_execution(db: &Database, stmt: &mut Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => panic!("unexpected execution result"),
        }
    }
    stmt.reset().unwrap();
}
