//! Comprehensive prepare-time benchmarks across SQL statement shapes.
//!
//! Each benchmark times ONLY `Connection::prepare` (parse + plan + codegen, no
//! execution) against a fixed relational schema, covering the statement shapes
//! drivers and ORMs generate most: point lookups, complex predicates, joins,
//! aggregates, window functions, subqueries, CTEs, compound selects, writes,
//! and DDL. Scaling variants (`args = [...]`) grow one dimension at a time
//! (IN-list length, expression depth, join count, compound arms, projection
//! width, bound-parameter count) to surface superlinear behavior in the
//! parser, planner, or code generator.
//!
//! Schema setup happens outside the measured closure; tables stay empty since
//! no statement is ever executed.
//!
//! Run with:
//!   cargo bench --bench prepare_benchmark

use divan::{AllocProfiler, Bencher, black_box};
use mimalloc::MiMalloc;
use std::sync::Arc;
use turso_core::{Connection, Database, MemoryIO, SqliteDialect, StepResult};

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

#[cfg(not(feature = "codspeed"))]
fn main() {
    // config_with_args() applies CLI filters and flags on top of the defaults;
    // without it every command-line argument is silently ignored.
    divan::Divan::default()
        .sample_count(50)
        .config_with_args()
        .main();
}

#[cfg(feature = "codspeed")]
fn main() {
    divan::main();
}

fn execute(db: &Database, conn: &Arc<Connection>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
}

fn open_db() -> (Arc<Database>, Arc<Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    (db, conn)
}

/// Open an in-memory database with a small relational schema (indexes
/// included so the planner has real access paths to choose between).
fn setup() -> (Arc<Database>, Arc<Connection>) {
    let (db, conn) = open_db();
    for sql in [
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            created_at TEXT
        )",
        "CREATE UNIQUE INDEX users_email ON users(email)",
        "CREATE INDEX users_age ON users(age)",
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL,
            name TEXT NOT NULL,
            category_id INTEGER,
            price REAL NOT NULL
        )",
        "CREATE UNIQUE INDEX products_sku ON products(sku)",
        "CREATE INDEX products_category ON products(category_id)",
        "CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id INTEGER
        )",
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            status TEXT NOT NULL,
            placed_at TEXT
        )",
        "CREATE INDEX orders_user ON orders(user_id)",
        "CREATE INDEX orders_product ON orders(product_id)",
        "CREATE INDEX orders_status ON orders(status)",
    ] {
        execute(&db, &conn, sql);
    }
    (db, conn)
}

fn bench_prepare(bencher: Bencher, sql: &str) {
    // The database must outlive every prepare call, so bind it even though
    // only the connection is touched.
    let (_db, conn) = setup();
    // Fail fast on SQL that no longer compiles, and keep first-prepare
    // one-time lazy costs out of the measured samples.
    let _warmup = conn.prepare(sql).unwrap();
    bencher.bench_local(|| {
        black_box(conn.prepare(black_box(sql)).unwrap());
    });
}

// --- Simple and filtered SELECTs ------------------------------------------

#[turso_macros::divan_bench]
fn select_point_lookup_pk(bencher: Bencher) {
    bench_prepare(bencher, "SELECT id, name, email FROM users WHERE id = ?");
}

#[turso_macros::divan_bench]
fn select_index_range_order_limit(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT id, name FROM users WHERE age BETWEEN ? AND ? ORDER BY age LIMIT 50",
    );
}

#[turso_macros::divan_bench]
fn select_complex_predicates(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT id, status, price * quantity AS total FROM orders \
         WHERE (status = 'shipped' OR status = 'pending' OR status IN ('paid', 'refunded')) \
           AND price > ? AND quantity BETWEEN ? AND ? \
           AND placed_at IS NOT NULL AND placed_at LIKE '2026-%' \
           AND NOT (user_id = 0 AND product_id = 0) \
         ORDER BY placed_at DESC, id ASC LIMIT 100 OFFSET 20",
    );
}

#[turso_macros::divan_bench(args = [10, 100, 1000])]
fn select_in_list(bencher: Bencher, n: usize) {
    let mut sql = String::from("SELECT id, name FROM users WHERE id IN (");
    for i in 0..n {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&i.to_string());
    }
    sql.push(')');
    bench_prepare(bencher, &sql);
}

// The parser rejects expression trees deeper than 100 and each nesting step
// below adds more than one depth unit, so 32 is the largest safe step.
#[turso_macros::divan_bench(args = [4, 16, 32])]
fn select_expression_depth(bencher: Bencher, depth: usize) {
    // Nest one level per iteration: ((((age + 0) * 2) CASE...) ...)
    let mut expr = String::from("age");
    for i in 0..depth {
        expr = match i % 3 {
            0 => format!("({expr} + {i})"),
            1 => format!("({expr} * 2)"),
            _ => format!("(CASE WHEN {expr} > {i} THEN 1 ELSE 0 END)"),
        };
    }
    let sql = format!("SELECT {expr} FROM users WHERE id = 1");
    bench_prepare(bencher, &sql);
}

#[turso_macros::divan_bench(args = [8, 32, 128])]
fn select_star_wide_table(bencher: Bencher, cols: usize) {
    let (db, conn) = setup();
    let mut ddl = String::from("CREATE TABLE wide (id INTEGER PRIMARY KEY");
    for c in 0..cols {
        ddl.push_str(&format!(", c{c} INTEGER"));
    }
    ddl.push(')');
    execute(&db, &conn, &ddl);
    let sql = "SELECT * FROM wide WHERE id = ?";
    let _warmup = conn.prepare(sql).unwrap();
    bencher.bench_local(|| {
        black_box(conn.prepare(black_box(sql)).unwrap());
    });
}

// --- Joins -----------------------------------------------------------------

#[turso_macros::divan_bench]
fn join_two_way_indexed(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT u.name, o.price FROM users u \
         JOIN orders o ON o.user_id = u.id WHERE o.status = ?",
    );
}

#[turso_macros::divan_bench]
fn join_four_way_mixed(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT u.name, p.name, c.name, o.quantity, o.price \
         FROM orders o \
         JOIN users u ON u.id = o.user_id \
         JOIN products p ON p.id = o.product_id \
         LEFT JOIN categories c ON c.id = p.category_id \
         WHERE u.age > ? AND o.status = 'shipped' \
         ORDER BY o.placed_at DESC LIMIT 25",
    );
}

#[turso_macros::divan_bench(args = [2, 4, 8])]
fn join_chain(bencher: Bencher, tables: usize) {
    // Chain joins cycling through the schema tables so the planner's join
    // order search grows with the join count.
    let names = ["users", "orders", "products", "categories"];
    let mut sql = format!("SELECT t0.id FROM {} t0", names[0]);
    for i in 1..tables {
        sql.push_str(&format!(
            " JOIN {} t{i} ON t{i}.id = t{}.id",
            names[i % names.len()],
            i - 1
        ));
    }
    sql.push_str(" WHERE t0.id = ?");
    bench_prepare(bencher, &sql);
}

// --- Aggregates and window functions ---------------------------------------

#[turso_macros::divan_bench]
fn agg_group_by_having(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT status, count(*) AS n, sum(price * quantity) AS revenue, \
                avg(price) AS avg_price, min(placed_at), max(placed_at) \
         FROM orders GROUP BY status \
         HAVING count(*) > 10 AND sum(price * quantity) > ? \
         ORDER BY revenue DESC",
    );
}

#[turso_macros::divan_bench]
fn agg_window_functions(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT user_id, price, \
                row_number() OVER (PARTITION BY user_id ORDER BY placed_at) AS seq, \
                sum(price) OVER (PARTITION BY user_id) AS user_total, \
                rank() OVER (ORDER BY price DESC) AS price_rank \
         FROM orders",
    );
}

// --- Subqueries ------------------------------------------------------------

#[turso_macros::divan_bench]
fn subquery_scalar_correlated(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT name, (SELECT count(*) FROM orders WHERE orders.user_id = users.id) \
         FROM users WHERE age > ?",
    );
}

#[turso_macros::divan_bench]
fn subquery_in_select(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT name FROM users WHERE id IN \
         (SELECT user_id FROM orders WHERE status = ? AND price > ?)",
    );
}

#[turso_macros::divan_bench]
fn subquery_exists_correlated(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT u.name FROM users u WHERE EXISTS \
         (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'pending')",
    );
}

#[turso_macros::divan_bench]
fn subquery_from_derived_table(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT t.user_id, t.total FROM \
         (SELECT user_id, sum(price * quantity) AS total FROM orders GROUP BY user_id) t \
         WHERE t.total > ? ORDER BY t.total DESC LIMIT 10",
    );
}

// --- CTEs and compound selects ---------------------------------------------

#[turso_macros::divan_bench]
fn cte_join(bencher: Bencher) {
    bench_prepare(
        bencher,
        "WITH spenders AS ( \
             SELECT user_id, sum(price * quantity) AS total \
             FROM orders GROUP BY user_id HAVING total > ? \
         ) \
         SELECT u.name, s.total FROM spenders s \
         JOIN users u ON u.id = s.user_id ORDER BY s.total DESC",
    );
}

// NOTE: recursive CTEs are not benchmarked because Turso does not support
// them yet; add a `WITH RECURSIVE` scenario once translation lands.
#[turso_macros::divan_bench]
fn cte_chained(bencher: Bencher) {
    bench_prepare(
        bencher,
        "WITH active AS ( \
             SELECT id, name FROM users WHERE age BETWEEN 18 AND 65 \
         ), totals AS ( \
             SELECT o.user_id, sum(o.price * o.quantity) AS total \
             FROM orders o JOIN active a ON a.id = o.user_id \
             GROUP BY o.user_id \
         ) \
         SELECT a.name, t.total FROM active a \
         JOIN totals t ON t.user_id = a.id \
         WHERE t.total > ? ORDER BY t.total DESC LIMIT 20",
    );
}

#[turso_macros::divan_bench(args = [4, 16, 64])]
fn compound_union_all(bencher: Bencher, arms: usize) {
    let mut sql = String::new();
    for i in 0..arms {
        if i > 0 {
            sql.push_str(" UNION ALL ");
        }
        sql.push_str(&format!("SELECT {i} AS n, 'arm{i}' AS label"));
    }
    bench_prepare(bencher, &sql);
}

#[turso_macros::divan_bench]
fn compound_union_distinct(bencher: Bencher) {
    bench_prepare(
        bencher,
        "SELECT user_id FROM orders WHERE status = 'shipped' \
         UNION \
         SELECT id FROM users WHERE age > ? \
         EXCEPT \
         SELECT user_id FROM orders WHERE status = 'refunded'",
    );
}

// --- Writes ----------------------------------------------------------------

#[turso_macros::divan_bench]
fn insert_single_row_params(bencher: Bencher) {
    bench_prepare(
        bencher,
        "INSERT INTO orders (user_id, product_id, quantity, price, status, placed_at) \
         VALUES (?, ?, ?, ?, ?, ?)",
    );
}

#[turso_macros::divan_bench(args = [10, 100])]
fn insert_multirow_params(bencher: Bencher, rows: usize) {
    let mut sql = String::from(
        "INSERT INTO orders (user_id, product_id, quantity, price, status, placed_at) VALUES ",
    );
    for r in 0..rows {
        if r > 0 {
            sql.push(',');
        }
        sql.push_str("(?, ?, ?, ?, ?, ?)");
    }
    bench_prepare(bencher, &sql);
}

#[turso_macros::divan_bench]
fn insert_select(bencher: Bencher) {
    bench_prepare(
        bencher,
        "INSERT INTO orders (user_id, product_id, quantity, price, status) \
         SELECT u.id, p.id, 1, p.price, 'pending' \
         FROM users u JOIN products p ON p.category_id = ? WHERE u.age >= ?",
    );
}

#[turso_macros::divan_bench]
fn insert_upsert_on_conflict(bencher: Bencher) {
    bench_prepare(
        bencher,
        "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET \
             name = excluded.name, email = excluded.email, age = excluded.age",
    );
}

#[turso_macros::divan_bench]
fn update_indexed_filter(bencher: Bencher) {
    bench_prepare(
        bencher,
        "UPDATE orders SET status = ?, price = price * ? \
         WHERE user_id = ? AND status = 'pending'",
    );
}

#[turso_macros::divan_bench]
fn delete_range(bencher: Bencher) {
    bench_prepare(
        bencher,
        "DELETE FROM orders WHERE placed_at < ? AND status IN ('refunded', 'cancelled')",
    );
}

// --- DDL -------------------------------------------------------------------

#[turso_macros::divan_bench]
fn ddl_create_table(bencher: Bencher) {
    bench_prepare(
        bencher,
        "CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY,
            entity TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            action TEXT NOT NULL CHECK (action IN ('insert', 'update', 'delete')),
            payload BLOB,
            created_at TEXT DEFAULT (datetime('now'))
        )",
    );
}

#[turso_macros::divan_bench]
fn ddl_create_index(bencher: Bencher) {
    bench_prepare(
        bencher,
        "CREATE INDEX orders_user_status_placed ON orders(user_id, status, placed_at)",
    );
}

// --- Established corpora: TPC-H, ClickBench, JOB, TPC-DS -------------------
//
// Prepare-only runs over the industry-standard query sets vendored under
// `perf/`, so planner work on realistic analytical SQL is tracked per query.
// Only the schema is needed (statements never execute), so tables stay empty.

/// Standard TPC-H DDL matching the layout of the SQLite conversion the
/// `perf/tpc-h` scripts download (lovasoa/TPCH-sqlite); the vendored queries
/// reference these tables and columns.
const TPCH_SCHEMA: &[&str] = &[
    "CREATE TABLE nation (n_nationkey INTEGER PRIMARY KEY, n_name TEXT NOT NULL, \
     n_regionkey INTEGER NOT NULL, n_comment TEXT)",
    "CREATE TABLE region (r_regionkey INTEGER PRIMARY KEY, r_name TEXT NOT NULL, \
     r_comment TEXT)",
    "CREATE TABLE part (p_partkey INTEGER PRIMARY KEY, p_name TEXT NOT NULL, \
     p_mfgr TEXT NOT NULL, p_brand TEXT NOT NULL, p_type TEXT NOT NULL, \
     p_size INTEGER NOT NULL, p_container TEXT NOT NULL, p_retailprice REAL NOT NULL, \
     p_comment TEXT NOT NULL)",
    "CREATE TABLE supplier (s_suppkey INTEGER PRIMARY KEY, s_name TEXT NOT NULL, \
     s_address TEXT NOT NULL, s_nationkey INTEGER NOT NULL, s_phone TEXT NOT NULL, \
     s_acctbal REAL NOT NULL, s_comment TEXT NOT NULL)",
    "CREATE TABLE partsupp (ps_partkey INTEGER NOT NULL, ps_suppkey INTEGER NOT NULL, \
     ps_availqty INTEGER NOT NULL, ps_supplycost REAL NOT NULL, ps_comment TEXT NOT NULL, \
     PRIMARY KEY (ps_partkey, ps_suppkey))",
    "CREATE TABLE customer (c_custkey INTEGER PRIMARY KEY, c_name TEXT NOT NULL, \
     c_address TEXT NOT NULL, c_nationkey INTEGER NOT NULL, c_phone TEXT NOT NULL, \
     c_acctbal REAL NOT NULL, c_mktsegment TEXT NOT NULL, c_comment TEXT NOT NULL)",
    "CREATE TABLE orders (o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER NOT NULL, \
     o_orderstatus TEXT NOT NULL, o_totalprice REAL NOT NULL, o_orderdate TEXT NOT NULL, \
     o_orderpriority TEXT NOT NULL, o_clerk TEXT NOT NULL, o_shippriority INTEGER NOT NULL, \
     o_comment TEXT NOT NULL)",
    "CREATE TABLE lineitem (l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, \
     l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity REAL NOT NULL, \
     l_extendedprice REAL NOT NULL, l_discount REAL NOT NULL, l_tax REAL NOT NULL, \
     l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipdate TEXT NOT NULL, \
     l_commitdate TEXT NOT NULL, l_receiptdate TEXT NOT NULL, l_shipinstruct TEXT NOT NULL, \
     l_shipmode TEXT NOT NULL, l_comment TEXT NOT NULL, \
     PRIMARY KEY (l_orderkey, l_linenumber))",
];

/// All TPC-H queries except q15, which needs CREATE VIEW (unsupported).
const TPCH_IDS: [usize; 21] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22,
];

fn tpch_sql(n: usize) -> &'static str {
    macro_rules! q {
        ($num:literal) => {
            include_str!(concat!("../../perf/tpc-h/queries/", $num, ".sql"))
        };
    }
    match n {
        1 => q!(1),
        2 => q!(2),
        3 => q!(3),
        4 => q!(4),
        5 => q!(5),
        6 => q!(6),
        7 => q!(7),
        8 => q!(8),
        9 => q!(9),
        10 => q!(10),
        11 => q!(11),
        12 => q!(12),
        13 => q!(13),
        14 => q!(14),
        16 => q!(16),
        17 => q!(17),
        18 => q!(18),
        19 => q!(19),
        20 => q!(20),
        21 => q!(21),
        22 => q!(22),
        _ => unreachable!("q{n} is not benchmarked"),
    }
}

#[turso_macros::divan_bench(args = TPCH_IDS)]
fn corpus_tpch(bencher: Bencher, q: usize) {
    let (db, conn) = open_db();
    for ddl in TPCH_SCHEMA {
        execute(&db, &conn, ddl);
    }
    let sql = tpch_sql(q);
    let _warmup = conn.prepare(sql).unwrap();
    bencher.bench_local(|| {
        black_box(conn.prepare(black_box(sql)).unwrap());
    });
}

/// All ClickBench queries except q29, which needs REGEXP_REPLACE (an
/// extension function, not available in core).
const CLICKBENCH_IDS: [usize; 42] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43,
];

fn clickbench_sql(n: usize) -> &'static str {
    include_str!("../../perf/clickbench/queries.sql")
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .nth(n - 1)
        .unwrap()
}

#[turso_macros::divan_bench(args = CLICKBENCH_IDS)]
fn corpus_clickbench(bencher: Bencher, q: usize) {
    let (db, conn) = open_db();
    execute(&db, &conn, include_str!("../../perf/clickbench/create.sql"));
    let sql = clickbench_sql(q);
    let _warmup = conn.prepare(sql).unwrap();
    bencher.bench_local(|| {
        black_box(conn.prepare(black_box(sql)).unwrap());
    });
}

/// Run every `;`-separated statement of `schema`, then bench preparing `sql`.
fn bench_corpus_query(bencher: Bencher, schema: &str, sql: &str) {
    let (db, conn) = open_db();
    for stmt in schema.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        execute(&db, &conn, stmt);
    }
    // Fail fast on SQL that no longer compiles, and keep first-prepare
    // one-time lazy costs out of the measured samples.
    let _warmup = conn.prepare(sql).unwrap();
    bencher.bench_local(|| {
        black_box(conn.prepare(black_box(sql)).unwrap());
    });
}

macro_rules! corpus_queries {
    ($dir:literal, [$($n:literal),* $(,)?]) => {
        &[$(($n, include_str!(concat!("../../perf/", $dir, "/queries/", $n, ".sql")))),*]
    };
}

fn corpus_sql(queries: &[(&str, &'static str)], name: &str) -> &'static str {
    queries
        .iter()
        .find(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("{name} is not in the corpus"))
        .1
}

/// Join Order Benchmark ("How Good Are Query Optimizers, Really?", VLDB
/// 2015): 113 join-heavy queries over the IMDB schema, the standard corpus
/// for stressing join-order planning. All 113 compile today.
const JOB_QUERIES: &[(&str, &str)] = corpus_queries!(
    "job",
    [
        "1a", "1b", "1c", "1d", "2a", "2b", "2c", "2d", "3a", "3b", "3c", "4a", "4b", "4c", "5a",
        "5b", "5c", "6a", "6b", "6c", "6d", "6e", "6f", "7a", "7b", "7c", "8a", "8b", "8c", "8d",
        "9a", "9b", "9c", "9d", "10a", "10b", "10c", "11a", "11b", "11c", "11d", "12a", "12b",
        "12c", "13a", "13b", "13c", "13d", "14a", "14b", "14c", "15a", "15b", "15c", "15d", "16a",
        "16b", "16c", "16d", "17a", "17b", "17c", "17d", "17e", "17f", "18a", "18b", "18c", "19a",
        "19b", "19c", "19d", "20a", "20b", "20c", "21a", "21b", "21c", "22a", "22b", "22c", "22d",
        "23a", "23b", "23c", "24a", "24b", "25a", "25b", "25c", "26a", "26b", "26c", "27a", "27b",
        "27c", "28a", "28b", "28c", "29a", "29b", "29c", "30a", "30b", "30c", "31a", "31b", "31c",
        "32a", "32b", "33a", "33b", "33c",
    ]
);

const JOB_SCHEMA: &str = concat!(
    include_str!("../../perf/job/schema.sql"),
    include_str!("../../perf/job/fkindexes.sql"),
);

// Several JOB queries take hundreds of milliseconds to plan today, so cap
// the sample count to keep local walltime runs reasonable.
#[turso_macros::divan_bench(args = JOB_QUERIES.iter().map(|(name, _)| *name), sample_count = 10)]
fn corpus_job(bencher: Bencher, q: &str) {
    bench_corpus_query(bencher, JOB_SCHEMA, corpus_sql(JOB_QUERIES, q));
}

/// TPC-DS queries that compile today. Skipped, with the blocking feature:
/// ROLLUP (05, 14, 18, 22, 67, 70, 77, 80, 86), stddev_samp (17, 39),
/// custom window frame specifications (51), parenthesized compound selects
/// (87), FULL OUTER JOIN without an equality condition (97). Remove entries
/// from this skip note as features land.
const TPCDS_QUERIES: &[(&str, &str)] = corpus_queries!(
    "tpc-ds",
    [
        "01", "02", "03", "04", "06", "07", "08", "09", "10", "11", "12", "13", "15", "16", "19",
        "20", "21", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
        "36", "37", "38", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "52",
        "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "68",
        "69", "71", "72", "73", "74", "75", "76", "78", "79", "81", "82", "83", "84", "85", "88",
        "89", "90", "91", "92", "93", "94", "95", "96", "98", "99",
    ]
);

const TPCDS_SCHEMA: &str = include_str!("../../perf/tpc-ds/schema.sql");

#[turso_macros::divan_bench(args = TPCDS_QUERIES.iter().map(|(name, _)| *name), sample_count = 10)]
fn corpus_tpcds(bencher: Bencher, q: &str) {
    bench_corpus_query(bencher, TPCDS_SCHEMA, corpus_sql(TPCDS_QUERIES, q));
}
