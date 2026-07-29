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

use divan::{black_box, AllocProfiler, Bencher};
use mimalloc::MiMalloc;
use std::sync::Arc;
use turso_core::{Connection, Database, MemoryIO, SqliteDialect, StepResult};

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

#[cfg(not(feature = "codspeed"))]
fn main() {
    divan::Divan::default().sample_count(50).main();
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

/// Open an in-memory database with a small relational schema (indexes
/// included so the planner has real access paths to choose between).
fn setup() -> (Arc<Database>, Arc<Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
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
    // Statements must compile; a bench that silently measures error paths is
    // worthless.
    conn.prepare(sql).unwrap();
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
    conn.prepare(sql).unwrap();
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
