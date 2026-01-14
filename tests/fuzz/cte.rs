//! CTE (Common Table Expression) tests.
//!
//! These tests verify CTE behavior by comparing Turso results with SQLite.
//! The tests cover supported CTE features as documented in CTE-status.md.
//!
//! Test types:
//! - `cte_fuzz`: RNG-driven fuzz test that generates random CTE graphs
//! - `cte_*_scenario`: Scenario-based tests with parameterized predefined patterns

#[cfg(test)]
mod cte_tests {
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    use core_tester::common::{
        limbo_exec_rows, limbo_exec_rows_fallible, rng_from_time_or_env, sqlite_exec_rows,
        TempDatabase,
    };

    /// Simple expressions that can be used in CTEs
    const SIMPLE_EXPRS: &[&str] = &[
        "1",
        "42",
        "-1",
        "0",
        "NULL",
        "'hello'",
        "'world'",
        "1.5",
        "-3.14",
        "1 + 2",
        "10 - 5",
        "2 * 3",
        "10 / 2",
        "5 % 3",
        "ABS(-5)",
        "length('hello')",
        "upper('test')",
        "lower('TEST')",
        "COALESCE(NULL, 1)",
        "COALESCE(NULL, NULL, 'x')",
        "IFNULL(NULL, 99)",
        "CAST(1.5 AS INTEGER)",
        "CAST(42 AS TEXT)",
        "typeof(1)",
        "typeof('text')",
        "typeof(1.5)",
        "typeof(NULL)",
        "hex('ab')",
        "1 BETWEEN 0 AND 2",
        "5 BETWEEN 1 AND 10",
        "'hello' LIKE '%ell%'",
        "'world' GLOB '*orl*'",
        "CASE WHEN 1 THEN 'yes' ELSE 'no' END",
        "CASE WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END",
        "1 IN (1, 2, 3)",
        "4 IN (1, 2, 3)",
        "EXISTS (SELECT 1)",
        "NOT EXISTS (SELECT 1 WHERE 0)",
        "1 IS NULL",
        "NULL IS NULL",
        "1 IS NOT NULL",
        "NULL IS NOT NULL",
        "1 = 1",
        "1 != 2",
        "1 < 2",
        "2 > 1",
        "1 <= 1",
        "2 >= 2",
        "1 AND 1",
        "1 OR 0",
        "NOT 0",
        "~1",
        "1 << 2",
        "8 >> 2",
        "1 | 2",
        "3 & 2",
        "substr('hello', 1, 3)",
        "substr('world', 2, 3)",
        "printf('%d', 42)",
        "printf('%s', 'test')",
    ];

    fn random_alias(rng: &mut ChaCha8Rng) -> String {
        let names = ["x", "y", "z", "a", "b", "c", "val", "num", "str", "res"];
        format!(
            "{}{}",
            names[rng.random_range(0..names.len())],
            rng.random_range(1..100)
        )
    }

    fn generate_simple_select(rng: &mut ChaCha8Rng) -> (String, Vec<String>) {
        let num_cols = rng.random_range(1..=4);
        let mut cols = Vec::with_capacity(num_cols);
        let mut aliases = Vec::with_capacity(num_cols);

        for _ in 0..num_cols {
            let expr = SIMPLE_EXPRS[rng.random_range(0..SIMPLE_EXPRS.len())];
            let alias = random_alias(rng);
            cols.push(format!("{expr} AS {alias}"));
            aliases.push(alias);
        }

        (format!("SELECT {}", cols.join(", ")), aliases)
    }

    #[turso_macros::test]
    pub fn cte_dml_scenario(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("cte_dml_scenario seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!("cte_dml_scenario iteration {}/{}", i + 1, ITERATIONS);
            }

            // Create fresh databases for each iteration
            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo_conn = limbo_db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            // Create table
            let create_table = "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)";
            limbo_conn.execute(create_table).unwrap();
            sqlite_conn.execute(create_table, params![]).unwrap();

            // Insert some initial data
            for j in 1..=5 {
                let insert = format!("INSERT INTO t VALUES ({}, {})", j, j * 10);
                limbo_conn.execute(&insert).unwrap();
                sqlite_conn.execute(&insert, params![]).unwrap();
            }

            let dml_type = rng.random_range(0..3);
            let query = match dml_type {
                0 => {
                    // CTE with INSERT
                    let val = rng.random_range(100..200);
                    format!(
                        "WITH cte AS (SELECT {} AS id, {} AS val) INSERT INTO t SELECT * FROM cte",
                        rng.random_range(10..20),
                        val
                    )
                }
                1 => {
                    // CTE with UPDATE
                    let target_id = rng.random_range(1..=5);
                    format!(
                        "WITH cte AS (SELECT {target_id} AS target) UPDATE t SET val = val + 1 WHERE id IN (SELECT target FROM cte)"
                    )
                }
                _ => {
                    // CTE with DELETE
                    let target_id = rng.random_range(1..=5);
                    format!(
                        "WITH cte AS (SELECT {target_id} AS target) DELETE FROM t WHERE id IN (SELECT target FROM cte)"
                    )
                }
            };

            // Execute DML
            let limbo_result = limbo_exec_rows_fallible(&limbo_db, &limbo_conn, &query);
            let sqlite_result = sqlite_conn.execute(&query, params![]);

            match (&limbo_result, &sqlite_result) {
                (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                _ => {
                    panic!(
                        "CTE DML outcome mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
                    );
                }
            }

            // Verify final state matches
            let verify_query = "SELECT id, val FROM t ORDER BY id";
            let limbo_final = limbo_exec_rows(&limbo_conn, verify_query);
            let sqlite_final = sqlite_exec_rows(&sqlite_conn, verify_query);

            assert_eq!(
                limbo_final, sqlite_final,
                "CTE DML state mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_final:?}\nsqlite: {sqlite_final:?}"
            );
        }
    }

    #[turso_macros::test]
    pub fn cte_quoted_names_scenario(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("cte_quoted_names_scenario seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const QUOTED_NAMES: &[&str] = &[
            "\"my cte\"",
            "\"CTE with spaces\"",
            "\"123start\"",
            "\"select\"",
            "\"from\"",
            "\"where\"",
        ];

        // Avoid words that SQLite interprets as literals when not found as identifiers
        // (e.g., "true", "null" fall back to string literals in SQLite)
        const QUOTED_COLS: &[&str] = &[
            "\"my col\"",
            "\"column with spaces\"",
            "\"col 123\"",
            "\"weird_col\"",
            "\"column!\"",
        ];

        const ITERATIONS: usize = 200;
        for i in 0..ITERATIONS {
            if i % 50 == 0 {
                println!(
                    "cte_quoted_names_scenario iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }

            let cte_name = QUOTED_NAMES[rng.random_range(0..QUOTED_NAMES.len())];
            let col_name = QUOTED_COLS[rng.random_range(0..QUOTED_COLS.len())];
            let val = rng.random_range(1..100);

            let query = format!(
                "WITH {cte_name} AS (SELECT {val} AS {col_name}) SELECT {col_name} FROM {cte_name}"
            );

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "CTE quoted names mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests complex feature interactions: CTE chains with compound selects,
    /// aggregates on CTE joins, multiple references with different operations.
    #[turso_macros::test]
    pub fn cte_feature_interaction_scenario(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("cte_feature_interaction_scenario seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const COMPOUND_OPS: &[&str] = &["UNION", "UNION ALL", "INTERSECT", "EXCEPT"];

        const ITERATIONS: usize = 400;
        for i in 0..ITERATIONS {
            if i % 100 == 0 {
                println!(
                    "cte_feature_interaction_scenario iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }

            let scenario = rng.random_range(0..17);
            let query = match scenario {
                0 => {
                    // Compound CTE with transformations on column1
                    let op = COMPOUND_OPS[rng.random_range(0..COMPOUND_OPS.len())];
                    let v1 = rng.random_range(1..10);
                    let v2 = rng.random_range(1..10);
                    let v3 = rng.random_range(1..10);
                    format!(
                        "WITH base AS (VALUES ({v1}) {op} VALUES ({v2}) {op} VALUES ({v3})), \
                         derived AS (SELECT column1, column1 * 2 AS doubled FROM base WHERE column1 > 0) \
                         SELECT * FROM derived ORDER BY column1"
                    )
                }
                1 => {
                    // CTE JOIN + aggregate + GROUP BY + HAVING
                    let n1 = rng.random_range(2..5);
                    let n2 = rng.random_range(2..5);
                    let mut rows1: Vec<String> = Vec::new();
                    let mut rows2: Vec<String> = Vec::new();
                    for j in 0..n1 {
                        rows1.push(format!("({}, {})", j % 3, rng.random_range(10..100)));
                    }
                    for j in 0..n2 {
                        rows2.push(format!("({}, {})", j % 3, rng.random_range(100..200)));
                    }
                    let threshold = rng.random_range(1..3);
                    format!(
                        "WITH left_t AS (VALUES {}), right_t AS (VALUES {}) \
                         SELECT left_t.column1 AS grp, SUM(left_t.column2) AS left_sum, \
                                COUNT(right_t.column2) AS right_cnt \
                         FROM left_t JOIN right_t ON left_t.column1 = right_t.column1 \
                         GROUP BY left_t.column1 \
                         HAVING COUNT(*) >= {} \
                         ORDER BY grp",
                        rows1.join(", "),
                        rows2.join(", "),
                        threshold
                    )
                }
                2 => {
                    // Multiple references: self-join + WHERE IN subquery
                    let val = rng.random_range(1..10);
                    format!(
                        "WITH nums AS (VALUES ({}) UNION VALUES ({}) UNION VALUES ({})) \
                         SELECT t1.column1 AS a, t2.column1 AS b \
                         FROM nums AS t1, nums AS t2 \
                         WHERE t1.column1 IN (SELECT column1 FROM nums WHERE column1 <= {}) \
                         ORDER BY t1.column1, t2.column1",
                        val,
                        val + 1,
                        val + 2,
                        val + 1
                    )
                }
                3 => {
                    // CTE with LEFT JOIN producing NULLs + COALESCE + aggregate
                    let mut left_rows: Vec<String> = Vec::new();
                    let mut right_rows: Vec<String> = Vec::new();
                    for j in 1..=4 {
                        left_rows.push(format!("({j})"));
                    }
                    // Right side has gaps
                    for j in [1, 3] {
                        right_rows.push(format!("({}, {})", j, j * 10));
                    }
                    format!(
                        "WITH left_t AS (VALUES {}), right_t AS (VALUES {}) \
                         SELECT left_t.column1 AS id, \
                                COALESCE(right_t.column2, 0) AS val, \
                                CASE WHEN right_t.column1 IS NULL THEN 'missing' ELSE 'found' END AS status \
                         FROM left_t LEFT JOIN right_t ON left_t.column1 = right_t.column1 \
                         ORDER BY id",
                        left_rows.join(", "),
                        right_rows.join(", ")
                    )
                }
                4 => {
                    // Correlated EXISTS with CTE + aggregate in outer query
                    let mut rows: Vec<String> = Vec::new();
                    for _ in 0..rng.random_range(3..6) {
                        rows.push(format!(
                            "({}, {})",
                            rng.random_range(1..4),
                            rng.random_range(10..50)
                        ));
                    }
                    let threshold = rng.random_range(20..40);
                    format!(
                        "WITH data AS (VALUES {}) \
                         SELECT column1 AS grp, SUM(column2) AS total \
                         FROM data AS d1 \
                         WHERE EXISTS (SELECT 1 FROM data AS d2 WHERE d2.column1 = d1.column1 AND d2.column2 > {}) \
                         GROUP BY column1 \
                         ORDER BY grp",
                        rows.join(", "),
                        threshold
                    )
                }
                5 => {
                    // Nested CTEs: outer CTE contains a subquery with its own WITH
                    let v1 = rng.random_range(1..5);
                    let v2 = rng.random_range(5..10);
                    format!(
                        "WITH outer_cte AS ( \
                             SELECT * FROM ( \
                                 WITH inner_cte AS (VALUES ({v1}) UNION ALL VALUES ({v2})) \
                                 SELECT column1 AS x, column1 + 10 AS y FROM inner_cte \
                             ) \
                         ) \
                         SELECT x, y, x + y AS total FROM outer_cte ORDER BY x"
                    )
                }
                6 => {
                    // CTE chain: filter -> transform -> aggregate pipeline (ETL-style)
                    let mut rows: Vec<String> = Vec::new();
                    for _ in 0..rng.random_range(5..10) {
                        let category = rng.random_range(1..4);
                        let amount = rng.random_range(-50..100);
                        rows.push(format!("({category}, {amount})"));
                    }
                    let min_amount = rng.random_range(0..20);
                    format!(
                        "WITH raw AS (VALUES {}), \
                         filtered AS (SELECT column1 AS cat, column2 AS amt FROM raw WHERE column2 > {}), \
                         transformed AS (SELECT cat, amt, ABS(amt) AS abs_amt FROM filtered), \
                         aggregated AS (SELECT cat, SUM(abs_amt) AS total, COUNT(*) AS cnt FROM transformed GROUP BY cat) \
                         SELECT * FROM aggregated ORDER BY cat",
                        rows.join(", "),
                        min_amount
                    )
                }
                7 => {
                    // Self-join with compound CTE + DISTINCT + ORDER BY
                    let op = COMPOUND_OPS[rng.random_range(0..COMPOUND_OPS.len())];
                    let vals: Vec<i32> = (0..4).map(|_| rng.random_range(1..8)).collect();
                    format!(
                        "WITH nums AS (VALUES ({}) {} VALUES ({}) {} VALUES ({}) {} VALUES ({})) \
                         SELECT DISTINCT t1.column1 AS a, t2.column1 AS b \
                         FROM nums AS t1 JOIN nums AS t2 ON t1.column1 <= t2.column1 \
                         ORDER BY a, b",
                        vals[0], op, vals[1], op, vals[2], op, vals[3]
                    )
                }
                8 => {
                    // NATURAL JOIN between CTEs
                    let mut rows1: Vec<String> = Vec::new();
                    let mut rows2: Vec<String> = Vec::new();
                    for j in 1..=3 {
                        rows1.push(format!("({}, {})", j, j * 10));
                        rows2.push(format!("({}, {})", j, j * 100));
                    }
                    format!(
                        "WITH left_t AS (VALUES {}), right_t AS (VALUES {}) \
                         SELECT * FROM left_t NATURAL JOIN right_t ORDER BY 1",
                        rows1.join(", "),
                        rows2.join(", ")
                    )
                }
                9 => {
                    // CTE defined inside FROM subquery
                    let v1 = rng.random_range(1..10);
                    let v2 = rng.random_range(10..20);
                    format!(
                        "SELECT * FROM (WITH inner AS (SELECT {v1} AS a, {v2} AS b) \
                         SELECT a, b, a + b AS sum FROM inner) ORDER BY 1"
                    )
                }
                10 => {
                    // Empty CTE with WHERE FALSE + LEFT JOIN fallback
                    let default_val = rng.random_range(1..100);
                    format!(
                        "WITH empty AS (SELECT 1 AS x WHERE FALSE), \
                         fallback AS (SELECT {default_val} AS x) \
                         SELECT COALESCE(e.x, f.x) AS result \
                         FROM fallback f LEFT JOIN empty e ON 1=1"
                    )
                }
                11 => {
                    // NULLIF and special NULL handling patterns
                    let val = rng.random_range(1..10);
                    format!(
                        "WITH data AS (VALUES ({}, {}) UNION ALL VALUES ({}, NULL)) \
                         SELECT column1, column2, NULLIF(column1, {}), IFNULL(column2, -1), COALESCE(column2, column1, 0) \
                         FROM data ORDER BY column1",
                        val,
                        val + 1,
                        val + 2,
                        val
                    )
                }
                12 => {
                    // CTE with ORDER BY + LIMIT inside, then aggregated outside
                    let mut rows: Vec<String> = Vec::new();
                    for _ in 0..rng.random_range(5..10) {
                        rows.push(format!("({})", rng.random_range(1..100)));
                    }
                    let limit = rng.random_range(2..5);
                    format!(
                        "WITH ranked AS (SELECT column1 AS val FROM (VALUES {}) ORDER BY column1 DESC LIMIT {}) \
                         SELECT SUM(val) AS top_sum, COUNT(*) AS cnt, AVG(val) AS avg_val FROM ranked",
                        rows.join(", "),
                        limit
                    )
                }
                13 => {
                    // Compound CTE column names propagating to downstream CTEs
                    // Tests that column names from SELECT x AS x UNION... propagate properly
                    let op = COMPOUND_OPS[rng.random_range(0..COMPOUND_OPS.len())];
                    let v1 = rng.random_range(1..20);
                    let v2 = rng.random_range(1..20);
                    let v3 = rng.random_range(1..20);
                    format!(
                        "WITH base AS (SELECT {v1} AS x {op} SELECT {v2} {op} SELECT {v3}), \
                         derived AS (SELECT x, x * 2 AS doubled FROM base WHERE x > 0) \
                         SELECT * FROM derived ORDER BY x"
                    )
                }
                14 => {
                    // Compound SELECT in FROM clause subqueries
                    // Tests that compound SELECTs work directly in FROM clause
                    let op = COMPOUND_OPS[rng.random_range(0..COMPOUND_OPS.len())];
                    let v1 = rng.random_range(1..50);
                    let v2 = rng.random_range(1..50);
                    let v3 = rng.random_range(1..50);
                    format!(
                        "SELECT val, val * 2 AS doubled FROM \
                         (SELECT {v1} AS val {op} SELECT {v2} {op} SELECT {v3}) AS compound_subq \
                         ORDER BY val"
                    )
                }
                15 => {
                    // CTE in scalar subquery in SELECT list
                    // Tests that CTEs are visible to scalar subqueries even without outer FROM clause
                    let v1 = rng.random_range(1..100);
                    let v2 = rng.random_range(1..100);
                    format!(
                        "WITH vals AS (SELECT {v1} AS a, {v2} AS b) \
                         SELECT (SELECT a FROM vals) AS scalar_a, \
                                (SELECT b FROM vals) AS scalar_b, \
                                (SELECT a + b FROM vals) AS scalar_sum"
                    )
                }
                _ => {
                    // Scenario 16: Complex expression CTE with CAST, typeof, CASE, etc.
                    let v = rng.random_range(1..50);
                    format!(
                        "WITH expr_test AS ( \
                             SELECT {v} AS num, \
                                    CAST({v} AS TEXT) AS str, \
                                    typeof({v}) AS type_name, \
                                    ABS(-{v}) AS abs_val, \
                                    CASE WHEN {v} > 25 THEN 'high' ELSE 'low' END AS category, \
                                    {v} BETWEEN 10 AND 40 AS in_range, \
                                    hex('abc') AS hex_val, \
                                    length('test') AS str_len \
                         ) \
                         SELECT * FROM expr_test"
                    )
                }
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "CTE feature interaction mismatch!\nseed: {seed}\niteration: {i}\nscenario: {scenario}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests CTEs with persistent tables and indexes to exercise query planner paths.
    #[turso_macros::test]
    pub fn cte_with_real_tables_scenario(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("cte_with_real_tables_scenario seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!(
                    "cte_with_real_tables_scenario iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }

            let limbo_db = builder.clone().build();
            let sqlite_db = builder.clone().build();
            let limbo_conn = limbo_db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

            // Create tables with various schemas
            let create_orders = "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER, status TEXT)";
            let create_customers =
                "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT)";
            let create_idx_customer = "CREATE INDEX idx_orders_customer ON orders(customer_id)";
            let create_idx_amount = "CREATE INDEX idx_orders_amount ON orders(amount)";
            let create_idx_region = "CREATE INDEX idx_customers_region ON customers(region)";

            limbo_conn.execute(create_orders).unwrap();
            limbo_conn.execute(create_customers).unwrap();
            limbo_conn.execute(create_idx_customer).unwrap();
            limbo_conn.execute(create_idx_amount).unwrap();
            limbo_conn.execute(create_idx_region).unwrap();

            sqlite_conn.execute(create_orders, params![]).unwrap();
            sqlite_conn.execute(create_customers, params![]).unwrap();
            sqlite_conn.execute(create_idx_customer, params![]).unwrap();
            sqlite_conn.execute(create_idx_amount, params![]).unwrap();
            sqlite_conn.execute(create_idx_region, params![]).unwrap();

            // Insert random data
            let regions = ["north", "south", "east", "west"];
            let statuses = ["pending", "shipped", "delivered"];
            let num_customers = rng.random_range(5..15);
            let num_orders = rng.random_range(20..50);

            for cid in 1..=num_customers {
                let name = format!("Customer{cid}");
                let region = regions[rng.random_range(0..regions.len())];
                let insert = format!("INSERT INTO customers VALUES ({cid}, '{name}', '{region}')");
                limbo_conn.execute(&insert).unwrap();
                sqlite_conn.execute(&insert, params![]).unwrap();
            }

            for oid in 1..=num_orders {
                let customer_id = rng.random_range(1..=num_customers);
                let amount = rng.random_range(10..500);
                let status = statuses[rng.random_range(0..statuses.len())];
                let insert = format!(
                    "INSERT INTO orders VALUES ({oid}, {customer_id}, {amount}, '{status}')"
                );
                limbo_conn.execute(&insert).unwrap();
                sqlite_conn.execute(&insert, params![]).unwrap();
            }

            // Generate complex CTE queries over real tables
            let scenario = rng.random_range(0..6);
            let query = match scenario {
                0 => {
                    // CTE filtering indexed column, joined back to original table
                    let min_amount = rng.random_range(100..300);
                    format!(
                        "WITH high_value AS (SELECT id, customer_id, amount FROM orders WHERE amount > {min_amount}) \
                         SELECT c.name, COUNT(h.id) AS order_count, SUM(h.amount) AS total \
                         FROM customers c \
                         JOIN high_value h ON c.id = h.customer_id \
                         GROUP BY c.id, c.name \
                         ORDER BY total DESC, c.name"
                    )
                }
                1 => {
                    // Multiple CTEs with different table sources
                    let region = regions[rng.random_range(0..regions.len())];
                    format!(
                        "WITH regional_customers AS (SELECT id, name FROM customers WHERE region = '{region}'), \
                         customer_orders AS (SELECT o.customer_id, SUM(o.amount) AS total FROM orders o GROUP BY o.customer_id) \
                         SELECT rc.name, COALESCE(co.total, 0) AS order_total \
                         FROM regional_customers rc \
                         LEFT JOIN customer_orders co ON rc.id = co.customer_id \
                         ORDER BY order_total DESC, rc.name"
                    )
                }
                2 => {
                    // CTE used in correlated subquery
                    let status = statuses[rng.random_range(0..statuses.len())];
                    format!(
                        "WITH status_orders AS (SELECT customer_id, amount FROM orders WHERE status = '{status}') \
                         SELECT c.name, c.region, \
                                (SELECT SUM(so.amount) FROM status_orders so WHERE so.customer_id = c.id) AS status_total \
                         FROM customers c \
                         WHERE EXISTS (SELECT 1 FROM status_orders so WHERE so.customer_id = c.id) \
                         ORDER BY c.name"
                    )
                }
                3 => {
                    // CTE chain: aggregate -> filter -> join
                    let min_count = rng.random_range(1..4);
                    format!(
                        "WITH order_stats AS ( \
                             SELECT customer_id, COUNT(*) AS cnt, AVG(amount) AS avg_amt \
                             FROM orders GROUP BY customer_id \
                         ), \
                         active_customers AS ( \
                             SELECT customer_id, cnt, avg_amt FROM order_stats WHERE cnt >= {min_count} \
                         ) \
                         SELECT c.name, c.region, ac.cnt, ac.avg_amt \
                         FROM active_customers ac \
                         JOIN customers c ON ac.customer_id = c.id \
                         ORDER BY ac.cnt DESC, c.name"
                    )
                }
                4 => {
                    // CTE with compound select from real tables
                    let threshold = rng.random_range(100..250);
                    format!(
                        "WITH notable_orders AS ( \
                             SELECT id, customer_id, amount, 'high' AS category FROM orders WHERE amount > {threshold} \
                             UNION ALL \
                             SELECT id, customer_id, amount, 'low' AS category FROM orders WHERE amount <= {threshold} \
                         ) \
                         SELECT category, COUNT(*) AS cnt, SUM(amount) AS total \
                         FROM notable_orders \
                         GROUP BY category \
                         ORDER BY category"
                    )
                }
                _ => {
                    // CTE with aggregates computed from it
                    let min_amount = rng.random_range(50..150);
                    format!(
                        "WITH filtered_orders AS (SELECT * FROM orders WHERE amount > {min_amount}) \
                         SELECT COUNT(*) AS total_count, \
                                SUM(amount) AS total_amount, \
                                AVG(amount) AS avg_amount, \
                                MIN(amount) AS min_amount, \
                                MAX(amount) AS max_amount \
                         FROM filtered_orders"
                    )
                }
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "CTE with real tables mismatch!\nseed: {seed}\niteration: {i}\nscenario: {scenario}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Comprehensive CTE fuzz test combining multiple features in each query.
    #[turso_macros::test]
    pub fn cte_comprehensive_scenario(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("cte_comprehensive_scenario seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 400;
        for i in 0..ITERATIONS {
            if i % 100 == 0 {
                println!(
                    "cte_comprehensive_scenario iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }

            // Generate 2-4 CTEs with various body types
            let num_ctes = rng.random_range(2..=4);
            let mut cte_defs = Vec::new();
            let mut cte_info: Vec<(String, Vec<String>, bool)> = Vec::new(); // (name, cols, is_numeric)

            for j in 0..num_ctes {
                let cte_name = format!("cte{}", j + 1);

                // Choose CTE body type based on what's available
                let body_type = if j == 0 {
                    rng.random_range(0..3) // First CTE: values, simple select, or compound
                } else {
                    rng.random_range(0..5) // Later CTEs can also reference previous
                };

                // Use unique column names per CTE to avoid ambiguity
                let col_a = format!("c{}_a", j + 1);
                let col_b = format!("c{}_b", j + 1);
                let col_single = format!("c{}", j + 1);

                let (body, aliases, is_numeric) = match body_type {
                    0 => {
                        // VALUES with numeric data, aliased to unique column names
                        let num_rows = rng.random_range(2..5);
                        let rows: Vec<String> = (0..num_rows)
                            .map(|_| {
                                format!(
                                    "({}, {})",
                                    rng.random_range(-20..20),
                                    rng.random_range(1..100)
                                )
                            })
                            .collect();
                        (
                            format!(
                                "SELECT column1 AS {}, column2 AS {} FROM (VALUES {})",
                                col_a,
                                col_b,
                                rows.join(", ")
                            ),
                            vec![col_a.clone(), col_b.clone()],
                            true,
                        )
                    }
                    1 => {
                        // Simple SELECT with expressions
                        let (sel, als) = generate_simple_select(&mut rng);
                        (sel, als, false)
                    }
                    2 => {
                        // Simple single-column VALUES with multiple rows
                        let num_rows = rng.random_range(2..5);
                        let rows: Vec<String> = (0..num_rows)
                            .map(|_| format!("({})", rng.random_range(1..100)))
                            .collect();
                        (
                            format!(
                                "SELECT column1 AS {} FROM (VALUES {})",
                                col_single,
                                rows.join(", ")
                            ),
                            vec![col_single.clone()],
                            true,
                        )
                    }
                    3 if !cte_info.is_empty() => {
                        // Reference previous CTE with transformation, renaming to unique columns
                        let prev_idx = rng.random_range(0..cte_info.len());
                        let prev = &cte_info[prev_idx];
                        if prev.2 && prev.1.len() >= 2 {
                            // Numeric: apply transformation with unique column names
                            (
                                format!(
                                    "SELECT {} AS {}, {} + 10 AS {} FROM {}",
                                    prev.1[0], col_a, prev.1[1], col_b, prev.0
                                ),
                                vec![col_a.clone(), col_b.clone()],
                                true,
                            )
                        } else if !prev.1.is_empty() {
                            // Pass through with renamed column
                            (
                                format!("SELECT {} AS {} FROM {}", prev.1[0], col_single, prev.0),
                                vec![col_single.clone()],
                                prev.2,
                            )
                        } else {
                            // Fallback
                            let v = rng.random_range(1..50);
                            (
                                format!("SELECT {v} AS {col_single}"),
                                vec![col_single.clone()],
                                true,
                            )
                        }
                    }
                    4 if !cte_info.is_empty() => {
                        // Reference previous CTE with filter, renaming to unique columns
                        let prev_idx = rng.random_range(0..cte_info.len());
                        let prev = &cte_info[prev_idx];
                        if prev.2 && !prev.1.is_empty() {
                            let threshold = rng.random_range(-10..10);
                            (
                                format!(
                                    "SELECT {} AS {} FROM {} WHERE {} > {}",
                                    prev.1[0], col_single, prev.0, prev.1[0], threshold
                                ),
                                vec![col_single.clone()],
                                true,
                            )
                        } else if !prev.1.is_empty() {
                            (
                                format!("SELECT {} AS {} FROM {}", prev.1[0], col_single, prev.0),
                                vec![col_single.clone()],
                                prev.2,
                            )
                        } else {
                            let v = rng.random_range(1..50);
                            (
                                format!("SELECT {v} AS {col_single}"),
                                vec![col_single.clone()],
                                true,
                            )
                        }
                    }
                    _ => {
                        // Fallback to simple values with unique column name
                        let v = rng.random_range(1..50);
                        (
                            format!("SELECT {v} AS {col_single}"),
                            vec![col_single.clone()],
                            true,
                        )
                    }
                };

                cte_defs.push(format!("{cte_name} AS ({body})"));
                cte_info.push((cte_name, aliases, is_numeric));
            }

            // Build the main query with multiple features
            let last_cte = &cte_info[cte_info.len() - 1];

            // Decide query structure (track types that shouldn't get WHERE modifiers)
            let query_type = rng.random_range(0..5);
            let skip_where_modifier = query_type == 1 || query_type == 2 || query_type == 3;
            let mut query = match query_type {
                0 => {
                    // Simple select with possible aggregate
                    if last_cte.2 && rng.random_bool(0.5) {
                        format!(
                            "WITH {} SELECT COUNT(*), SUM({}) FROM {}",
                            cte_defs.join(", "),
                            last_cte.1[0],
                            last_cte.0
                        )
                    } else {
                        format!(
                            "WITH {} SELECT {} FROM {}",
                            cte_defs.join(", "),
                            last_cte.1.join(", "),
                            last_cte.0
                        )
                    }
                }
                1 if cte_info.len() >= 2 => {
                    // Join two CTEs (with ORDER BY for deterministic results)
                    let cte1 = &cte_info[0];
                    let cte2 = &cte_info[cte_info.len() - 1];
                    if cte1.2 && cte2.2 {
                        format!(
                            "WITH {} SELECT t1.{}, t2.{} FROM {} t1, {} t2 ORDER BY 1, 2",
                            cte_defs.join(", "),
                            cte1.1[0],
                            cte2.1[0],
                            cte1.0,
                            cte2.0
                        )
                    } else {
                        format!(
                            "WITH {} SELECT * FROM {} ORDER BY 1",
                            cte_defs.join(", "),
                            last_cte.0
                        )
                    }
                }
                2 => {
                    // Use CTE in subquery
                    format!(
                        "WITH {} SELECT * FROM {} WHERE {} IN (SELECT {} FROM {})",
                        cte_defs.join(", "),
                        last_cte.0,
                        last_cte.1[0],
                        last_cte.1[0],
                        last_cte.0
                    )
                }
                3 => {
                    // Multiple references with aliases (with ORDER BY for deterministic results)
                    format!(
                        "WITH {} SELECT t1.{} AS a, t2.{} AS b FROM {} t1, {} t2 ORDER BY 1, 2",
                        cte_defs.join(", "),
                        last_cte.1[0],
                        last_cte.1[0],
                        last_cte.0,
                        last_cte.0
                    )
                }
                _ => {
                    format!(
                        "WITH {} SELECT {} FROM {}",
                        cte_defs.join(", "),
                        last_cte.1.join(", "),
                        last_cte.0
                    )
                }
            };

            // Add modifiers (skip WHERE/ORDER BY for queries that already have them)
            let has_order_by = query_type == 1 || query_type == 3;
            if !skip_where_modifier && rng.random_bool(0.4) && last_cte.2 {
                query = format!("{} WHERE {} IS NOT NULL", query, last_cte.1[0]);
            }
            if !has_order_by && rng.random_bool(0.5) {
                let dir = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
                query = format!("{query} ORDER BY 1 {dir}");
            }
            if rng.random_bool(0.3) {
                query = format!("{} LIMIT {}", query, rng.random_range(1..20));
            }

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "CTE comprehensive mismatch!\nseed: {seed}\niteration: {i}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    // =========================================================================
    // RNG-driven CTE fuzzer
    // =========================================================================
    //
    // This test generates truly random CTE queries by making RNG decisions at
    // each step rather than selecting from predefined scenarios. It tracks
    // which CTEs are available for reference and randomly composes features.

    /// Information about a generated CTE
    #[derive(Clone)]
    struct CteInfo {
        name: String,
        columns: Vec<String>,
        has_numeric_cols: bool,
    }

    /// Generates a random literal value
    fn gen_literal(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..5) {
            0 => rng.random_range(-100..100).to_string(),
            1 => format!("{:.2}", rng.random_range(-100.0..100.0_f64)),
            2 => format!("'{}'", ["foo", "bar", "baz", "qux"][rng.random_range(0..4)]),
            3 => "NULL".to_string(),
            _ => rng.random_range(0..1000).to_string(),
        }
    }

    /// Generates a random expression (can be recursive)
    fn gen_expr(rng: &mut ChaCha8Rng, depth: usize) -> String {
        if depth == 0 || rng.random_bool(0.4) {
            return gen_literal(rng);
        }

        match rng.random_range(0..10) {
            // Binary arithmetic
            0 => {
                let op = ["+", "-", "*"][rng.random_range(0..3)];
                format!(
                    "({} {} {})",
                    gen_expr(rng, depth - 1),
                    op,
                    gen_expr(rng, depth - 1)
                )
            }
            // Comparison
            1 => {
                let op = ["=", "!=", "<", ">", "<=", ">="][rng.random_range(0..6)];
                format!(
                    "({} {} {})",
                    gen_expr(rng, depth - 1),
                    op,
                    gen_expr(rng, depth - 1)
                )
            }
            // Function call
            2 => {
                let (func, args) = [
                    ("ABS", 1),
                    ("COALESCE", 2),
                    ("IFNULL", 2),
                    ("NULLIF", 2),
                    ("MAX", 1),
                    ("MIN", 1),
                    ("length", 1),
                ][rng.random_range(0..7)];
                let arg_strs: Vec<String> = (0..args).map(|_| gen_expr(rng, depth - 1)).collect();
                format!("{}({})", func, arg_strs.join(", "))
            }
            // CASE expression
            3 => {
                format!(
                    "CASE WHEN {} THEN {} ELSE {} END",
                    gen_expr(rng, depth - 1),
                    gen_expr(rng, depth - 1),
                    gen_expr(rng, depth - 1)
                )
            }
            // CAST
            4 => {
                let cast_type = ["INTEGER", "TEXT", "REAL"][rng.random_range(0..3)];
                format!("CAST({} AS {})", gen_expr(rng, depth - 1), cast_type)
            }
            // BETWEEN
            5 => {
                let v1 = rng.random_range(0..50);
                let v2 = v1 + rng.random_range(1..50);
                format!("{} BETWEEN {} AND {}", gen_expr(rng, depth - 1), v1, v2)
            }
            // IS NULL / IS NOT NULL
            6 => {
                let suffix = if rng.random_bool(0.5) {
                    "IS NULL"
                } else {
                    "IS NOT NULL"
                };
                format!("({} {})", gen_expr(rng, depth - 1), suffix)
            }
            // Boolean ops
            7 => {
                let op = ["AND", "OR"][rng.random_range(0..2)];
                format!(
                    "({} {} {})",
                    gen_expr(rng, depth - 1),
                    op,
                    gen_expr(rng, depth - 1)
                )
            }
            // IN list
            8 => {
                let vals: Vec<String> = (0..rng.random_range(2..5))
                    .map(|_| gen_literal(rng))
                    .collect();
                format!("({} IN ({}))", gen_expr(rng, depth - 1), vals.join(", "))
            }
            // Simple literal (fallback)
            _ => gen_literal(rng),
        }
    }

    /// Generates a unique column name for a CTE
    fn gen_col_name(cte_idx: usize, col_idx: usize) -> String {
        format!("c{cte_idx}_{col_idx}")
    }

    /// Generates a SELECT list with random expressions
    fn gen_select_list(rng: &mut ChaCha8Rng, cte_idx: usize) -> (String, Vec<String>, bool) {
        let num_cols = rng.random_range(1..=3);
        let mut cols = Vec::new();
        let mut col_names = Vec::new();
        let mut has_numeric = false;

        for col_idx in 0..num_cols {
            let col_name = gen_col_name(cte_idx, col_idx);
            // Bias towards numeric expressions for predicate generation
            let expr = if rng.random_bool(0.7) {
                has_numeric = true;
                rng.random_range(-50..50).to_string()
            } else {
                gen_expr(rng, 2)
            };
            cols.push(format!("{expr} AS {col_name}"));
            col_names.push(col_name);
        }

        (cols.join(", "), col_names, has_numeric)
    }

    /// Generates a VALUES clause
    fn gen_values(rng: &mut ChaCha8Rng, cte_idx: usize) -> (String, Vec<String>, bool) {
        let num_cols = rng.random_range(1..=3);
        let num_rows = rng.random_range(2..=5);
        let col_names: Vec<String> = (0..num_cols).map(|i| gen_col_name(cte_idx, i)).collect();

        let mut rows = Vec::new();
        for _ in 0..num_rows {
            let vals: Vec<String> = (0..num_cols)
                .map(|_| rng.random_range(-50..50).to_string())
                .collect();
            rows.push(format!("({})", vals.join(", ")));
        }

        // Wrap in SELECT to get named columns
        let col_refs: Vec<String> = (0..num_cols)
            .map(|i| format!("column{} AS {}", i + 1, col_names[i]))
            .collect();
        let sql = format!(
            "SELECT {} FROM (VALUES {})",
            col_refs.join(", "),
            rows.join(", ")
        );

        (sql, col_names, true)
    }

    /// Generates a CTE body that references previous CTEs
    fn gen_cte_body_with_ref(
        rng: &mut ChaCha8Rng,
        cte_idx: usize,
        available_ctes: &[CteInfo],
    ) -> (String, Vec<String>, bool) {
        let ref_cte = &available_ctes[rng.random_range(0..available_ctes.len())];
        let col_names: Vec<String> = (0..ref_cte.columns.len().min(3))
            .map(|i| gen_col_name(cte_idx, i))
            .collect();

        // Decide what kind of reference to make
        match rng.random_range(0..5) {
            // Simple passthrough with rename
            0 => {
                let select_cols: Vec<String> = ref_cte
                    .columns
                    .iter()
                    .zip(col_names.iter())
                    .map(|(old, new)| format!("{old} AS {new}"))
                    .collect();
                (
                    format!("SELECT {} FROM {}", select_cols.join(", "), ref_cte.name),
                    col_names,
                    ref_cte.has_numeric_cols,
                )
            }
            // With transformation
            1 if ref_cte.has_numeric_cols => {
                let col = &ref_cte.columns[0];
                let new_col = gen_col_name(cte_idx, 0);
                (
                    format!(
                        "SELECT {} + {} AS {} FROM {}",
                        col,
                        rng.random_range(1..20),
                        new_col,
                        ref_cte.name
                    ),
                    vec![new_col],
                    true,
                )
            }
            // With filter
            2 if ref_cte.has_numeric_cols => {
                let col = &ref_cte.columns[0];
                let new_col = gen_col_name(cte_idx, 0);
                let threshold = rng.random_range(-30..30);
                (
                    format!(
                        "SELECT {} AS {} FROM {} WHERE {} > {}",
                        col, new_col, ref_cte.name, col, threshold
                    ),
                    vec![new_col],
                    true,
                )
            }
            // With aggregate
            3 if ref_cte.has_numeric_cols => {
                let col = &ref_cte.columns[0];
                let agg = ["SUM", "COUNT", "AVG", "MIN", "MAX"][rng.random_range(0..5)];
                let new_col = gen_col_name(cte_idx, 0);
                (
                    format!(
                        "SELECT {}({}) AS {} FROM {}",
                        agg, col, new_col, ref_cte.name
                    ),
                    vec![new_col],
                    true,
                )
            }
            // Self-join (if CTE has data)
            4 => {
                let col = &ref_cte.columns[0];
                let new_col1 = gen_col_name(cte_idx, 0);
                let new_col2 = gen_col_name(cte_idx, 1);
                (
                    format!(
                        "SELECT t1.{} AS {}, t2.{} AS {} FROM {} t1, {} t2",
                        col, new_col1, col, new_col2, ref_cte.name, ref_cte.name
                    ),
                    vec![new_col1, new_col2],
                    ref_cte.has_numeric_cols,
                )
            }
            // Fallback: simple passthrough
            _ => {
                let col = &ref_cte.columns[0];
                let new_col = gen_col_name(cte_idx, 0);
                (
                    format!("SELECT {} AS {} FROM {}", col, new_col, ref_cte.name),
                    vec![new_col],
                    ref_cte.has_numeric_cols,
                )
            }
        }
    }

    /// Generates a compound query (UNION/INTERSECT/EXCEPT)
    fn gen_compound_body(
        rng: &mut ChaCha8Rng,
        cte_idx: usize,
        available_ctes: &[CteInfo],
    ) -> (String, Vec<String>, bool) {
        let op = ["UNION", "UNION ALL", "INTERSECT", "EXCEPT"][rng.random_range(0..4)];
        let num_parts = rng.random_range(2..=4);
        let col_name = gen_col_name(cte_idx, 0);

        // Sometimes reference an existing CTE in the compound
        let use_cte_ref = !available_ctes.is_empty() && rng.random_bool(0.3);
        let ref_cte = if use_cte_ref {
            Some(&available_ctes[rng.random_range(0..available_ctes.len())])
        } else {
            None
        };

        let parts: Vec<String> = (0..num_parts)
            .map(|i| {
                if i == 0 {
                    if let Some(cte) = ref_cte {
                        if cte.has_numeric_cols {
                            return format!(
                                "SELECT {} AS {} FROM {}",
                                cte.columns[0], col_name, cte.name
                            );
                        }
                    }
                    let val = rng.random_range(-20..20);
                    format!("SELECT {val} AS {col_name}")
                } else {
                    let val = rng.random_range(-20..20);
                    format!("SELECT {val}")
                }
            })
            .collect();

        (parts.join(&format!(" {op} ")), vec![col_name], true)
    }

    /// Generates the body of a CTE
    fn gen_cte_body(
        rng: &mut ChaCha8Rng,
        cte_idx: usize,
        available_ctes: &[CteInfo],
    ) -> (String, Vec<String>, bool) {
        // If we have previous CTEs, sometimes reference them
        if !available_ctes.is_empty() && rng.random_bool(0.6) {
            return gen_cte_body_with_ref(rng, cte_idx, available_ctes);
        }

        // Otherwise generate a standalone body
        match rng.random_range(0..7) {
            0 => {
                let (cols, names, has_num) = gen_select_list(rng, cte_idx);
                (format!("SELECT {cols}"), names, has_num)
            }
            1 => gen_values(rng, cte_idx),
            2 => gen_compound_body(rng, cte_idx, available_ctes),
            // Nested CTE (CTE inside FROM subquery)
            3 => {
                let col_name = gen_col_name(cte_idx, 0);
                let inner_col = format!("inner_{col_name}");
                let val = rng.random_range(1..50);
                (
                    format!(
                        "SELECT * FROM (WITH inner_cte AS (SELECT {val} AS {inner_col}) SELECT {inner_col} AS {col_name} FROM inner_cte)"
                    ),
                    vec![col_name],
                    true,
                )
            }
            // Empty CTE (WHERE FALSE) - tests NULL/empty result handling
            4 => {
                let col_name = gen_col_name(cte_idx, 0);
                (
                    format!("SELECT 1 AS {col_name} WHERE 0"),
                    vec![col_name],
                    true,
                )
            }
            // CTE with ORDER BY + LIMIT inside body (using VALUES)
            5 => {
                let col_name = gen_col_name(cte_idx, 0);
                let num_vals = rng.random_range(3..6);
                let vals: Vec<String> = (0..num_vals)
                    .map(|_| format!("({})", rng.random_range(-20..20)))
                    .collect();
                let limit = rng.random_range(1..num_vals);
                (
                    format!(
                        "SELECT column1 AS {} FROM (VALUES {}) ORDER BY 1 LIMIT {}",
                        col_name,
                        vals.join(", "),
                        limit
                    ),
                    vec![col_name],
                    true,
                )
            }
            // Compound SELECT in FROM clause subquery
            _ => {
                let col_name = gen_col_name(cte_idx, 0);
                let v1 = rng.random_range(1..20);
                let v2 = rng.random_range(1..20);
                let op = ["UNION", "UNION ALL"][rng.random_range(0..2)];
                (
                    format!("SELECT * FROM (SELECT {v1} AS {col_name} {op} SELECT {v2})"),
                    vec![col_name],
                    true,
                )
            }
        }
    }

    /// Generates the final SELECT query using the available CTEs
    fn gen_final_query(rng: &mut ChaCha8Rng, ctes: &[CteInfo]) -> String {
        let last_cte = &ctes[ctes.len() - 1];

        match rng.random_range(0..17) {
            // Simple select
            0 => {
                format!(
                    "SELECT {} FROM {}",
                    last_cte.columns.join(", "),
                    last_cte.name
                )
            }
            // With aggregate
            1 if last_cte.has_numeric_cols => {
                let agg = if rng.random_bool(0.5) {
                    "COUNT(*)".to_string()
                } else {
                    format!("SUM({})", last_cte.columns[0])
                };
                format!("SELECT {} FROM {}", agg, last_cte.name)
            }
            // Multiple CTE references (cross product)
            2 if ctes.len() >= 2 => {
                let cte1 = &ctes[0];
                let cte2 = &ctes[ctes.len() - 1];
                format!(
                    "SELECT t1.{}, t2.{} FROM {} t1, {} t2 ORDER BY 1, 2",
                    cte1.columns[0], cte2.columns[0], cte1.name, cte2.name
                )
            }
            // Self-join
            3 => {
                format!(
                    "SELECT t1.{}, t2.{} FROM {} t1, {} t2 ORDER BY 1, 2",
                    last_cte.columns[0], last_cte.columns[0], last_cte.name, last_cte.name
                )
            }
            // Subquery in WHERE (EXISTS)
            4 => {
                format!(
                    "SELECT {} FROM {} t1 WHERE EXISTS (SELECT 1 FROM {} t2 WHERE t1.{} = t2.{})",
                    last_cte.columns[0],
                    last_cte.name,
                    last_cte.name,
                    last_cte.columns[0],
                    last_cte.columns[0]
                )
            }
            // Subquery in WHERE (IN)
            5 if ctes.len() >= 2 => {
                let other_cte = &ctes[rng.random_range(0..ctes.len() - 1)];
                format!(
                    "SELECT * FROM {} WHERE {} IN (SELECT {} FROM {})",
                    last_cte.name, last_cte.columns[0], other_cte.columns[0], other_cte.name
                )
            }
            // Scalar subquery in SELECT
            6 => {
                format!(
                    "SELECT (SELECT COUNT(*) FROM {}) AS cnt, {} FROM {}",
                    last_cte.name, last_cte.columns[0], last_cte.name
                )
            }
            // LEFT JOIN between CTEs
            7 if ctes.len() >= 2 => {
                let other_cte = &ctes[rng.random_range(0..ctes.len() - 1)];
                format!(
                    "SELECT t1.{}, t2.{} FROM {} t1 LEFT JOIN {} t2 ON 1=1 ORDER BY 1, 2",
                    last_cte.columns[0], other_cte.columns[0], last_cte.name, other_cte.name
                )
            }
            // SELECT DISTINCT
            8 => {
                format!(
                    "SELECT DISTINCT {} FROM {} ORDER BY 1",
                    last_cte.columns[0], last_cte.name
                )
            }
            // GROUP BY + HAVING
            9 if last_cte.has_numeric_cols => {
                let col = &last_cte.columns[0];
                let threshold = rng.random_range(1..5);
                format!(
                    "SELECT {}, COUNT(*) AS cnt FROM {} GROUP BY {} HAVING COUNT(*) >= {} ORDER BY 1",
                    col, last_cte.name, col, threshold
                )
            }
            // INNER JOIN with condition between CTEs
            10 if ctes.len() >= 2 && last_cte.has_numeric_cols => {
                let other_cte = &ctes[rng.random_range(0..ctes.len() - 1)];
                if other_cte.has_numeric_cols {
                    format!(
                        "SELECT t1.{}, t2.{} FROM {} t1 INNER JOIN {} t2 ON t1.{} = t2.{} ORDER BY 1, 2",
                        last_cte.columns[0], other_cte.columns[0],
                        last_cte.name, other_cte.name,
                        last_cte.columns[0], other_cte.columns[0]
                    )
                } else {
                    format!(
                        "SELECT {} FROM {} ORDER BY 1",
                        last_cte.columns[0], last_cte.name
                    )
                }
            }
            // Correlated EXISTS across different CTEs
            11 if ctes.len() >= 2 => {
                let other_cte = &ctes[rng.random_range(0..ctes.len() - 1)];
                format!(
                    "SELECT {} FROM {} t1 WHERE EXISTS (SELECT 1 FROM {} t2 WHERE t1.{} = t2.{})",
                    last_cte.columns[0],
                    last_cte.name,
                    other_cte.name,
                    last_cte.columns[0],
                    other_cte.columns[0]
                )
            }
            // GROUP BY without HAVING
            12 if last_cte.has_numeric_cols => {
                let col = &last_cte.columns[0];
                format!(
                    "SELECT {}, SUM({}) AS total FROM {} GROUP BY {} ORDER BY 1",
                    col, col, last_cte.name, col
                )
            }
            // DISTINCT with multiple columns
            13 if last_cte.columns.len() >= 2 => {
                format!(
                    "SELECT DISTINCT {}, {} FROM {} ORDER BY 1, 2",
                    last_cte.columns[0], last_cte.columns[1], last_cte.name
                )
            }
            // NOT EXISTS subquery
            14 => {
                format!(
                    "SELECT {} FROM {} t1 WHERE NOT EXISTS (SELECT 1 FROM {} t2 WHERE t1.{} != t2.{}) ORDER BY 1",
                    last_cte.columns[0],
                    last_cte.name,
                    last_cte.name,
                    last_cte.columns[0],
                    last_cte.columns[0]
                )
            }
            // COALESCE with LEFT JOIN (NULL handling)
            15 if ctes.len() >= 2 => {
                let other_cte = &ctes[rng.random_range(0..ctes.len() - 1)];
                format!(
                    "SELECT t1.{}, COALESCE(t2.{}, -999) AS coalesced FROM {} t1 LEFT JOIN {} t2 ON 1=0 ORDER BY 1",
                    last_cte.columns[0], other_cte.columns[0], last_cte.name, other_cte.name
                )
            }
            // NULLIF pattern
            16 if last_cte.has_numeric_cols => {
                let threshold = rng.random_range(-10..10);
                format!(
                    "SELECT NULLIF({}, {}) AS nullified FROM {} ORDER BY 1",
                    last_cte.columns[0], threshold, last_cte.name
                )
            }
            // Fallback: simple select with ORDER BY
            _ => {
                format!(
                    "SELECT {} FROM {} ORDER BY 1",
                    last_cte.columns[0], last_cte.name
                )
            }
        }
    }

    /// Adds random modifiers to a query (ORDER BY, LIMIT, OFFSET)
    fn add_modifiers(rng: &mut ChaCha8Rng, query: &str, _cte: &CteInfo) -> String {
        let mut q = query.to_string();

        // Add ORDER BY if not already present
        if !q.contains("ORDER BY") && rng.random_bool(0.4) {
            let dir = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
            q = format!("{q} ORDER BY 1 {dir}");
        }

        // Add LIMIT (and sometimes OFFSET)
        if rng.random_bool(0.3) {
            let limit = rng.random_range(1..20);
            if rng.random_bool(0.3) {
                let offset = rng.random_range(0..5);
                q = format!("{q} LIMIT {limit} OFFSET {offset}");
            } else {
                q = format!("{q} LIMIT {limit}");
            }
        }

        q
    }

    /// RNG-driven CTE fuzzer that generates truly random CTE queries
    #[turso_macros::test]
    pub fn cte_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("cte_fuzz seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 500;
        for i in 0..ITERATIONS {
            if i % 100 == 0 {
                println!("cte_fuzz iteration {}/{}", i + 1, ITERATIONS);
            }

            // Decide number of CTEs (1-5)
            let num_ctes = rng.random_range(1..=5);
            let mut ctes: Vec<CteInfo> = Vec::with_capacity(num_ctes);
            let mut cte_defs: Vec<String> = Vec::with_capacity(num_ctes);

            // Generate CTEs
            for cte_idx in 0..num_ctes {
                let cte_name = format!("cte{cte_idx}");
                let (body, columns, has_numeric) = gen_cte_body(&mut rng, cte_idx, &ctes);

                cte_defs.push(format!("{cte_name} AS ({body})"));
                ctes.push(CteInfo {
                    name: cte_name,
                    columns,
                    has_numeric_cols: has_numeric,
                });
            }

            // Generate final query
            let final_query = gen_final_query(&mut rng, &ctes);
            let final_query = add_modifiers(&mut rng, &final_query, &ctes[ctes.len() - 1]);

            let query = format!("WITH {} {final_query}", cte_defs.join(", "));

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "CTE RNG-driven mismatch!\nseed: {seed}\niteration: {i}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }
}
