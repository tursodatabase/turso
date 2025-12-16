//! Fuzzer for ORDER BY collation vs index collation behavior.
//!
//! This fuzzer tests that when ORDER BY uses a different collation than the index,
//! the optimizer correctly falls back to table scan + sort instead of incorrectly
//! using the index (which would produce wrong ordering).

#[cfg(test)]
mod tests {
    use core_tester::common::{
        limbo_exec_rows, maybe_setup_tracing, rng_from_time_or_env, sqlite_exec_rows, TempDatabase,
    };
    use rand::Rng;

    /// Collation sequences supported by SQLite/Limbo
    const COLLATIONS: [&str; 3] = ["BINARY", "NOCASE", "RTRIM"];

    /// Sort orders
    const SORT_ORDERS: [&str; 2] = ["ASC", "DESC"];

    /// String pool designed to expose collation differences:
    /// - Case variations for NOCASE vs BINARY
    /// - Trailing spaces for RTRIM vs BINARY
    const STR_POOL: [&str; 24] = [
        "a", "A", "b", "B", "c", "C", "aa", "AA", "Aa", "aA", "ab", "AB", "Ab", "aB", "a ", "A ",
        "b ", "B ", "a  ", "A  ", " a", " A", "abc", "ABC",
    ];

    /// Test ORDER BY with explicit COLLATE that differs from index collation.
    /// This is the core bug case: if the optimizer incorrectly uses an index
    /// whose collation doesn't match the ORDER BY collation, results will be wrong.
    #[turso_macros::test()]
    pub fn orderby_collation_vs_index_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("orderby_collation_vs_index_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        // Generate table+index combinations with different collations
        let mut schema_variants: Vec<(String, String)> = Vec::new();

        for table_collation in COLLATIONS.iter() {
            for index_collation in COLLATIONS.iter() {
                for index_order in SORT_ORDERS.iter() {
                    let table_ddl =
                        format!("CREATE TABLE t (c1 TEXT COLLATE {table_collation}, c2 INTEGER)");
                    let index_ddl = format!(
                        "CREATE INDEX idx ON t (c1 COLLATE {index_collation} {index_order})"
                    );
                    schema_variants.push((table_ddl, index_ddl));
                }
            }
        }

        // Also test without explicit table collation (defaults to BINARY)
        for index_collation in COLLATIONS.iter() {
            for index_order in SORT_ORDERS.iter() {
                let table_ddl = "CREATE TABLE t (c1 TEXT, c2 INTEGER)".to_string();
                let index_ddl =
                    format!("CREATE INDEX idx ON t (c1 COLLATE {index_collation} {index_order})");
                schema_variants.push((table_ddl, index_ddl));
            }
        }

        const ITERS: usize = 500;
        for iter in 0..ITERS {
            if iter % (ITERS / 10).max(1) == 0 {
                println!(
                    "orderby_collation_vs_index_fuzz: iteration {}/{}",
                    iter + 1,
                    ITERS
                );
            }

            // Pick a random schema variant
            let (table_ddl, index_ddl) =
                &schema_variants[rng.random_range(0..schema_variants.len())];

            // Create separate databases for limbo and sqlite
            let limbo_db = builder.clone().with_init_sql(table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();
            limbo_conn.execute(index_ddl).unwrap();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(table_ddl, ()).unwrap();
            sqlite_conn.execute(index_ddl, ()).unwrap();

            // Insert random data into both databases
            let num_rows = rng.random_range(10..=50);
            for i in 0..num_rows {
                let val = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let insert = format!("INSERT INTO t VALUES ('{}', {})", val.replace("'", "''"), i);
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // Generate ORDER BY with random collation (may or may not match index)
            let orderby_collation = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let orderby_order = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            // Test with and without rowid tiebreaker
            let queries = [
                format!(
                    "SELECT c1, c2 FROM t ORDER BY c1 COLLATE {orderby_collation} {orderby_order}"
                ),
                format!(
                    "SELECT c1, c2 FROM t ORDER BY c1 COLLATE {orderby_collation} {orderby_order}, c2 ASC"
                ),
                format!(
                    "SELECT c1 FROM t ORDER BY c1 COLLATE {orderby_collation} {orderby_order} LIMIT 10"
                ),
            ];

            for query in queries.iter() {
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
                let limbo_rows = limbo_exec_rows(&limbo_conn, query);

                similar_asserts::assert_eq!(
                    Turso: limbo_rows,
                    Sqlite: sqlite_rows,
                    "MISMATCH!\nQuery: {query}\nTable: {table_ddl}\nIndex: {index_ddl}\nSeed: {seed}\nIteration: {iter}\n\
                        Turso ({} rows)\nSQLite ({} rows)",
                    limbo_rows.len(),
                    sqlite_rows.len(),
                );
            }
        }
    }

    /// Test multi-column ORDER BY where some columns match index collation and some don't.
    #[turso_macros::test()]
    pub fn orderby_multicolumn_collation_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("orderby_multicolumn_collation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const ITERS: usize = 300;
        for iter in 0..ITERS {
            if iter % (ITERS / 10).max(1) == 0 {
                println!(
                    "orderby_multicolumn_collation_fuzz: iteration {}/{}",
                    iter + 1,
                    ITERS
                );
            }

            // Random collations for each column
            let col1_table_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let col2_table_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let col1_idx_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let col2_idx_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let idx_order1 = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];
            let idx_order2 = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            let table_ddl = format!(
                "CREATE TABLE t (a TEXT COLLATE {col1_table_coll}, b TEXT COLLATE {col2_table_coll}, c INTEGER)"
            );
            let index_ddl = format!(
                "CREATE INDEX idx ON t (a COLLATE {col1_idx_coll} {idx_order1}, b COLLATE {col2_idx_coll} {idx_order2})"
            );

            // Create separate databases
            let limbo_db = builder.clone().with_init_sql(&table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();
            limbo_conn.execute(&index_ddl).unwrap();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(&table_ddl, ()).unwrap();
            sqlite_conn.execute(&index_ddl, ()).unwrap();

            // Insert random data
            let num_rows = rng.random_range(20..=60);
            for i in 0..num_rows {
                let val_a = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let val_b = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let insert = format!(
                    "INSERT INTO t VALUES ('{}', '{}', {})",
                    val_a.replace("'", "''"),
                    val_b.replace("'", "''"),
                    i
                );
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // Generate ORDER BY with random collations
            let orderby_coll1 = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let orderby_coll2 = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let orderby_order1 = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];
            let orderby_order2 = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            let queries = [
                // Both columns with explicit collation
                format!(
                    "SELECT a, b, c FROM t ORDER BY a COLLATE {orderby_coll1} {orderby_order1}, b COLLATE {orderby_coll2} {orderby_order2}, c ASC"
                ),
                // Only first column with explicit collation
                format!(
                    "SELECT a, b, c FROM t ORDER BY a COLLATE {orderby_coll1} {orderby_order1}, b {orderby_order2}, c ASC"
                ),
                // Only second column with explicit collation
                format!(
                    "SELECT a, b, c FROM t ORDER BY a {orderby_order1}, b COLLATE {orderby_coll2} {orderby_order2}, c ASC"
                ),
            ];

            for query in queries.iter() {
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
                let limbo_rows = limbo_exec_rows(&limbo_conn, query);

                similar_asserts::assert_eq!(
                    Turso: limbo_rows,
                    Sqlite: sqlite_rows,
                    "MISMATCH!\nQuery: {query}\nTable: {table_ddl}\nIndex: {index_ddl}\nSeed: {seed}\nIteration: {iter}\n\
                        Turso ({} rows)\nSQLite ({} rows)",
                    limbo_rows.len(),
                    sqlite_rows.len(),
                );
            }
        }
    }

    /// Test ORDER BY with implicit column collation (no explicit COLLATE in ORDER BY).
    /// The column's declared collation should be used, and if it doesn't match the index,
    /// the index should not be used for ordering.
    #[turso_macros::test()]
    pub fn orderby_implicit_collation_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("orderby_implicit_collation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const ITERS: usize = 300;
        for iter in 0..ITERS {
            if iter % (ITERS / 10).max(1) == 0 {
                println!(
                    "orderby_implicit_collation_fuzz: iteration {}/{}",
                    iter + 1,
                    ITERS
                );
            }

            // Column has one collation, index has another
            let col_collation = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let idx_collation = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let idx_order = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            let table_ddl = format!("CREATE TABLE t (c1 TEXT COLLATE {col_collation}, c2 INTEGER)");
            let index_ddl =
                format!("CREATE INDEX idx ON t (c1 COLLATE {idx_collation} {idx_order})");

            // Create separate databases
            let limbo_db = builder.clone().with_init_sql(&table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();
            limbo_conn.execute(&index_ddl).unwrap();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(&table_ddl, ()).unwrap();
            sqlite_conn.execute(&index_ddl, ()).unwrap();

            // Insert random data
            let num_rows = rng.random_range(15..=40);
            for i in 0..num_rows {
                let val = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let insert = format!("INSERT INTO t VALUES ('{}', {})", val.replace("'", "''"), i);
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // ORDER BY without explicit COLLATE - should use column's collation
            let orderby_order = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            let queries = [
                format!("SELECT c1, c2 FROM t ORDER BY c1 {orderby_order}"),
                format!("SELECT c1, c2 FROM t ORDER BY c1 {orderby_order}, c2 ASC"),
                format!("SELECT c1 FROM t ORDER BY c1 {orderby_order} LIMIT 15"),
            ];

            for query in queries.iter() {
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
                let limbo_rows = limbo_exec_rows(&limbo_conn, query);

                similar_asserts::assert_eq!(
                    Turso: limbo_rows,
                    Sqlite: sqlite_rows,
                    "MISMATCH!\nQuery: {query}\nTable: {table_ddl}\nIndex: {index_ddl}\nCol collation: {col_collation}\nIdx collation: {idx_collation}\nSeed: {seed}\nIteration: {iter}\n\
                        Turso ({} rows)\nSQLite ({} rows)",
                    limbo_rows.len(),
                    sqlite_rows.len(),
                );
            }
        }
    }

    /// Test with multiple indexes having different collations - optimizer should pick
    /// the one that matches ORDER BY collation (if any).
    #[turso_macros::test()]
    pub fn orderby_multiple_indexes_collation_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("orderby_multiple_indexes_collation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const ITERS: usize = 200;
        for iter in 0..ITERS {
            if iter % (ITERS / 10).max(1) == 0 {
                println!(
                    "orderby_multiple_indexes_collation_fuzz: iteration {}/{}",
                    iter + 1,
                    ITERS
                );
            }

            // Create table with multiple indexes, each with different collation
            let table_ddl = "CREATE TABLE t (c1 TEXT, c2 INTEGER)";
            let idx1_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let idx2_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let idx1_order = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];
            let idx2_order = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            let index1_ddl =
                format!("CREATE INDEX idx1 ON t (c1 COLLATE {idx1_coll} {idx1_order})");
            let index2_ddl =
                format!("CREATE INDEX idx2 ON t (c1 COLLATE {idx2_coll} {idx2_order})");

            // Create separate databases
            let limbo_db = builder.clone().with_init_sql(table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();
            limbo_conn.execute(&index1_ddl).unwrap();
            limbo_conn.execute(&index2_ddl).unwrap();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(table_ddl, ()).unwrap();
            sqlite_conn.execute(&index1_ddl, ()).unwrap();
            sqlite_conn.execute(&index2_ddl, ()).unwrap();

            // Insert random data
            let num_rows = rng.random_range(20..=50);
            for i in 0..num_rows {
                let val = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let insert = format!("INSERT INTO t VALUES ('{}', {})", val.replace("'", "''"), i);
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // Test ORDER BY with each collation
            for orderby_coll in COLLATIONS.iter() {
                for orderby_order in SORT_ORDERS.iter() {
                    let query = format!(
                        "SELECT c1, c2 FROM t ORDER BY c1 COLLATE {orderby_coll} {orderby_order}, c2 ASC"
                    );

                    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
                    let limbo_rows = limbo_exec_rows(&limbo_conn, &query);

                    similar_asserts::assert_eq!(
                        Turso: limbo_rows,
                        Sqlite: sqlite_rows,
                        "MISMATCH!\nQuery: {query}\nIndex1: {index1_ddl}\nIndex2: {index2_ddl}\nSeed: {seed}\nIteration: {iter}\n\
                            Turso ({} rows)\nSQLite ({} rows)",
                        limbo_rows.len(),
                        sqlite_rows.len(),
                    );
                }
            }
        }
    }

    /// Test ORDER BY with WHERE clause constraints - the optimizer might use an index
    /// for the WHERE clause but still need to sort for ORDER BY if collations don't match.
    #[turso_macros::test()]
    pub fn orderby_with_where_collation_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("orderby_with_where_collation_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const ITERS: usize = 300;
        for iter in 0..ITERS {
            if iter % (ITERS / 10).max(1) == 0 {
                println!(
                    "orderby_with_where_collation_fuzz: iteration {}/{}",
                    iter + 1,
                    ITERS
                );
            }

            let col_collation = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let idx_collation = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let idx_order = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            let table_ddl = format!(
                "CREATE TABLE t (c1 TEXT COLLATE {col_collation}, c2 TEXT COLLATE {col_collation}, c3 INTEGER)"
            );
            let index_ddl =
                format!("CREATE INDEX idx ON t (c1 COLLATE {idx_collation} {idx_order})");

            // Create separate databases
            let limbo_db = builder.clone().with_init_sql(&table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();
            limbo_conn.execute(&index_ddl).unwrap();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(&table_ddl, ()).unwrap();
            sqlite_conn.execute(&index_ddl, ()).unwrap();

            // Insert random data
            let num_rows = rng.random_range(30..=80);
            for i in 0..num_rows {
                let val1 = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let val2 = STR_POOL[rng.random_range(0..STR_POOL.len())];
                let insert = format!(
                    "INSERT INTO t VALUES ('{}', '{}', {})",
                    val1.replace("'", "''"),
                    val2.replace("'", "''"),
                    i
                );
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // Generate queries with WHERE and ORDER BY using different collations
            let where_val = STR_POOL[rng.random_range(0..STR_POOL.len())];
            let where_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let orderby_coll = COLLATIONS[rng.random_range(0..COLLATIONS.len())];
            let orderby_order = SORT_ORDERS[rng.random_range(0..SORT_ORDERS.len())];

            let queries = [
                // WHERE uses index collation, ORDER BY uses different collation
                format!(
                    "SELECT c1, c2, c3 FROM t WHERE c1 COLLATE {where_coll} = '{}' ORDER BY c1 COLLATE {orderby_coll} {orderby_order}, c3 ASC",
                    where_val.replace("'", "''")
                ),
                // WHERE uses index collation, ORDER BY on different column
                format!(
                    "SELECT c1, c2, c3 FROM t WHERE c1 COLLATE {where_coll} = '{}' ORDER BY c2 COLLATE {orderby_coll} {orderby_order}, c3 ASC",
                    where_val.replace("'", "''")
                ),
                // Range query with ORDER BY
                format!(
                    "SELECT c1, c2, c3 FROM t WHERE c1 COLLATE {where_coll} > '{}' ORDER BY c1 COLLATE {orderby_coll} {orderby_order}, c3 ASC LIMIT 20",
                    where_val.replace("'", "''")
                ),
            ];

            for query in queries.iter() {
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
                let limbo_rows = limbo_exec_rows(&limbo_conn, query);

                similar_asserts::assert_eq!(
                    Turso: limbo_rows,
                    Sqlite: sqlite_rows,
                    "MISMATCH!\nQuery: {query}\nTable: {table_ddl}\nindex: {index_ddl}\nSeed: {seed}\nIteration: {iter}\n\
                        Turso ({} rows)\nSQLite ({} rows)",
                    limbo_rows.len(),
                    sqlite_rows.len(),
                );
            }
        }
    }
}
