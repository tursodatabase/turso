//! Fuzzer for ORDER BY collation vs index collation behavior.
//!
//! This fuzzer tests that when ORDER BY uses a different collation than the index,
//! the optimizer correctly falls back to table scan + sort instead of incorrectly
//! using the index (which would produce wrong ordering).

#[cfg(test)]
mod tests {
    use crate::helpers;
    use core_tester::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
    use rand::Rng;
    use rusqlite::types::Value;

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

    /// Compare two strings using the specified collation.
    /// Returns Ordering::Equal if they are equal under the collation.
    fn compare_with_collation(a: &str, b: &str, collation: &str) -> std::cmp::Ordering {
        match collation {
            "BINARY" => a.cmp(b),
            "NOCASE" => a.to_lowercase().cmp(&b.to_lowercase()),
            "RTRIM" => a.trim_end().cmp(b.trim_end()),
            _ => a.cmp(b),
        }
    }

    /// Extract the first column as a string from a row, if it's text.
    fn get_first_col_text(row: &[Value]) -> Option<&str> {
        row.first().and_then(|v| match v {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        })
    }

    /// Verify that rows are correctly ordered according to the given collation and direction.
    /// This allows for any order among equal elements (unstable sort).
    fn verify_ordering(
        rows: &[Vec<Value>],
        collation: &str,
        descending: bool,
    ) -> Result<(), String> {
        for i in 1..rows.len() {
            let prev_str = get_first_col_text(&rows[i - 1]).unwrap();
            let curr_str = get_first_col_text(&rows[i]).unwrap();

            let cmp = compare_with_collation(prev_str, curr_str, collation);
            let valid = if descending {
                // DESC: prev >= curr (i.e., prev should not be less than curr)
                cmp != std::cmp::Ordering::Less
            } else {
                // ASC: prev <= curr (i.e., prev should not be greater than curr)
                cmp != std::cmp::Ordering::Greater
            };

            if !valid {
                return Err(format!(
                    "Ordering violation at index {i}: '{prev_str}' vs '{curr_str}' (collation: {collation}, desc: {descending})"
                ));
            }
        }
        Ok(())
    }

    /// Verify that two result sets contain the same rows (possibly in different order for equal elements)
    /// and both follow the correct ordering under the given collation.
    fn verify_results_equivalent(
        turso_rows: &[Vec<Value>],
        sqlite_rows: &[Vec<Value>],
        collation: &str,
        descending: bool,
    ) -> Result<(), String> {
        // First check same number of rows
        if turso_rows.len() != sqlite_rows.len() {
            return Err(format!(
                "Row count mismatch: Turso={}, SQLite={}",
                turso_rows.len(),
                sqlite_rows.len()
            ));
        }

        // Verify both are correctly ordered
        verify_ordering(turso_rows, collation, descending)?;
        verify_ordering(sqlite_rows, collation, descending)?;

        // Verify same multiset of rows (same rows, possibly different order for equal elements)
        let mut turso_sorted = turso_rows.to_vec();
        let mut sqlite_sorted = sqlite_rows.to_vec();
        turso_sorted.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        sqlite_sorted.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

        if turso_sorted != sqlite_sorted {
            return Err("Row content mismatch: different rows returned".to_string());
        }

        Ok(())
    }

    /// Test ORDER BY with explicit COLLATE that differs from index collation.
    /// if the optimizer incorrectly uses an index whose collation doesn't match the ORDER BY collation, results will be wrong.
    #[turso_macros::test(mvcc)]
    pub fn orderby_collation_vs_index_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("orderby_collation_vs_index_fuzz");
        let builder = helpers::builder_from_db(&db);

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
            helpers::log_progress("orderby_collation_vs_index_fuzz", iter, ITERS, 10);

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
            let descending = orderby_order == "DESC";

            // Query without secondary sort key - order of equal elements is undefined
            let query = format!(
                "SELECT c1, c2 FROM t ORDER BY c1 COLLATE {orderby_collation} {orderby_order}"
            );

            let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
            let limbo_rows = limbo_exec_rows(&limbo_conn, &query);

            if let Err(e) =
                verify_results_equivalent(&limbo_rows, &sqlite_rows, orderby_collation, descending)
            {
                panic!(
                    "MISMATCH!\n{}\nQuery: {query}\nTable: {table_ddl}\nIndex: {index_ddl}\nSeed: {seed}\nIteration: {iter}\n\
                    Turso ({} rows): {:?}\nSQLite ({} rows): {:?}",
                    e, limbo_rows.len(), limbo_rows, sqlite_rows.len(), sqlite_rows
                );
            }

            // Query with secondary sort key (c2) - order should be deterministic
            let query_with_tiebreaker = format!(
                "SELECT c1, c2 FROM t ORDER BY c1 COLLATE {orderby_collation} {orderby_order}, c2 ASC LIMIT 20"
            );

            let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query_with_tiebreaker);
            let limbo_rows = limbo_exec_rows(&limbo_conn, &query_with_tiebreaker);

            similar_asserts::assert_eq!(
                Turso: limbo_rows,
                Sqlite: sqlite_rows,
                "MISMATCH!\nQuery: {query_with_tiebreaker}\nTable: {table_ddl}\nIndex: {index_ddl}\nSeed: {seed}\nIteration: {iter}",
            );
        }
    }

    /// Test multi-column ORDER BY where some columns match index collation and some don't.
    #[turso_macros::test(mvcc)]
    pub fn orderby_multicolumn_collation_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("orderby_multicolumn_collation_fuzz");
        let builder = helpers::builder_from_db(&db);

        const ITERS: usize = 100;
        for iter in 0..ITERS {
            helpers::log_progress("orderby_multicolumn_collation_fuzz", iter, ITERS, 10);

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
    #[turso_macros::test(mvcc)]
    pub fn orderby_implicit_collation_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("orderby_implicit_collation_fuzz");
        let builder = helpers::builder_from_db(&db);

        const ITERS: usize = 100;
        for iter in 0..ITERS {
            helpers::log_progress("orderby_implicit_collation_fuzz", iter, ITERS, 10);

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
            let descending = orderby_order == "DESC";

            // Query without secondary sort key - order of equal elements is undefined
            let query = format!("SELECT c1, c2 FROM t ORDER BY c1 {orderby_order}");

            let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
            let limbo_rows = limbo_exec_rows(&limbo_conn, &query);

            // Use column's collation for verification
            if let Err(e) =
                verify_results_equivalent(&limbo_rows, &sqlite_rows, col_collation, descending)
            {
                panic!(
                    "MISMATCH!\n{}\nQuery: {query}\nTable: {table_ddl}\nIndex: {index_ddl}\nCol collation: {col_collation}\nIdx collation: {idx_collation}\nSeed: {seed}\nIteration: {iter}\n\
                    Turso ({} rows): {:?}\nSQLite ({} rows): {:?}",
                    e, limbo_rows.len(), limbo_rows, sqlite_rows.len(), sqlite_rows
                );
            }
        }
    }

    /// Test with multiple indexes having different collations - optimizer should pick
    /// the one that matches ORDER BY collation (if any).
    #[turso_macros::test(mvcc)]
    pub fn orderby_multiple_indexes_collation_fuzz(db: TempDatabase) {
        let (mut rng, seed) =
            helpers::init_fuzz_test_tracing("orderby_multiple_indexes_collation_fuzz");
        let builder = helpers::builder_from_db(&db);

        const ITERS: usize = 100;
        for iter in 0..ITERS {
            helpers::log_progress("orderby_multiple_indexes_collation_fuzz", iter, ITERS, 10);

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
    #[turso_macros::test(mvcc)]
    pub fn orderby_with_where_collation_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("orderby_with_where_collation_fuzz");
        let builder = helpers::builder_from_db(&db);

        const ITERS: usize = 100;
        for iter in 0..ITERS {
            helpers::log_progress("orderby_with_where_collation_fuzz", iter, ITERS, 10);

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
