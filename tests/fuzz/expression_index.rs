#[cfg(test)]
mod expression_index_fuzz_tests {
    use crate::helpers;
    use core_tester::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
    use rand::seq::{IndexedRandom, SliceRandom};
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    /// Expression index definitions: (sql_expr, referenced_columns).
    const EXPRESSIONS: &[(&str, &[&str])] = &[
        ("a * 2", &["a"]),
        ("a + b", &["a", "b"]),
        ("ABS(a)", &["a"]),
        ("a - b", &["a", "b"]),
        ("COALESCE(a, 0)", &["a"]),
        ("COALESCE(a, 0) + COALESCE(b, 0)", &["a", "b"]),
        ("UPPER(name)", &["name"]),
        ("LENGTH(name)", &["name"]),
        ("name || '_x'", &["name"]),
        ("CASE WHEN a IS NULL THEN -1 ELSE a * 2 END", &["a"]),
        ("a * b", &["a", "b"]),
        ("ABS(a - b)", &["a", "b"]),
    ];

    /// Pick `count` distinct random expression indexes from the pool.
    fn pick_expressions(
        rng: &mut ChaCha8Rng,
        count: usize,
    ) -> Vec<(usize, &'static str, &'static [&'static str])> {
        let mut indices: Vec<usize> = (0..EXPRESSIONS.len()).collect();
        indices.shuffle(rng);
        indices
            .into_iter()
            .take(count)
            .map(|i| (i, EXPRESSIONS[i].0, EXPRESSIONS[i].1))
            .collect()
    }

    /// Generate a random nullable integer value as a SQL literal.
    fn rand_int_or_null(rng: &mut ChaCha8Rng) -> String {
        helpers::random_nullable_int(rng, -100..=100, 0.2)
    }

    /// Generate a random text value as a SQL literal (with ~20% NULL).
    fn rand_text_or_null(rng: &mut ChaCha8Rng) -> String {
        if rng.random_bool(0.2) {
            "NULL".to_string()
        } else {
            let text = helpers::generate_random_text(rng, 8);
            format!("'{text}'")
        }
    }

    /// Generate a random INSERT statement.
    fn random_insert(rng: &mut ChaCha8Rng, next_id: &mut i64) -> String {
        let id = *next_id;
        *next_id += 1;
        let a = rand_int_or_null(rng);
        let b = rand_int_or_null(rng);
        let name = rand_text_or_null(rng);
        format!("INSERT INTO t(id, a, b, name) VALUES ({id}, {a}, {b}, {name})")
    }

    /// Generate a random INSERT OR REPLACE statement targeting an existing id.
    fn random_insert_or_replace(rng: &mut ChaCha8Rng, last_id: i64) -> String {
        // Pick from existing ids only (1..last_id) to avoid conflicts with future inserts.
        let id = rng.random_range(1..=last_id.max(1));
        let a = rand_int_or_null(rng);
        let b = rand_int_or_null(rng);
        let name = rand_text_or_null(rng);
        format!("INSERT OR REPLACE INTO t(id, a, b, name) VALUES ({id}, {a}, {b}, {name})")
    }

    /// Generate a random UPDATE targeting columns referenced by the chosen expressions.
    fn random_update(rng: &mut ChaCha8Rng, max_id: i64, referenced_cols: &[&str]) -> String {
        // Pick a column to update, biased toward expression-referenced columns.
        let col = if rng.random_bool(0.8) && !referenced_cols.is_empty() {
            *referenced_cols.choose(rng).unwrap()
        } else {
            *["a", "b", "name"].choose(rng).unwrap()
        };
        let val = match col {
            "name" => rand_text_or_null(rng),
            _ => rand_int_or_null(rng),
        };
        let where_clause = random_where(rng, max_id);
        format!("UPDATE t SET {col} = {val} {where_clause}")
    }

    /// Generate a random DELETE statement.
    fn random_delete(rng: &mut ChaCha8Rng, max_id: i64) -> String {
        let where_clause = random_where(rng, max_id);
        format!("DELETE FROM t {where_clause}")
    }

    /// Generate a random WHERE clause.
    fn random_where(rng: &mut ChaCha8Rng, max_id: i64) -> String {
        match rng.random_range(0..4) {
            0 => format!("WHERE id = {}", rng.random_range(1..=max_id.max(1))),
            1 => format!("WHERE id <= {}", rng.random_range(1..=max_id.max(1))),
            2 => format!(
                "WHERE a IS NOT NULL AND id > {}",
                rng.random_range(0..=max_id.max(1))
            ),
            3 => "WHERE name IS NOT NULL".to_string(),
            _ => unreachable!(),
        }
    }

    /// Collect all referenced columns from a set of expressions (deduplicated).
    fn all_referenced_cols<'a>(exprs: &[(usize, &'a str, &'a [&'a str])]) -> Vec<&'a str> {
        let mut cols: Vec<&str> = exprs
            .iter()
            .flat_map(|(_, _, cols)| cols.iter().copied())
            .collect();
        cols.sort();
        cols.dedup();
        cols
    }

    #[turso_macros::test(mvcc)]
    pub fn expression_index_differential_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("expression_index_differential_fuzz");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let iterations = helpers::fuzz_iterations(100);

        for iter in 0..iterations {
            helpers::log_progress("expression_index_differential_fuzz", iter, iterations, 10);

            // --- Schema setup ---
            let create_table = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT, name TEXT)";
            limbo_conn.execute(create_table).unwrap();
            sqlite_conn.execute(create_table, params![]).unwrap();

            // Pick 1-3 random expression indexes.
            let num_indexes = rng.random_range(1..=3);
            let chosen_exprs = pick_expressions(&mut rng, num_indexes);
            let ref_cols = all_referenced_cols(&chosen_exprs);

            for (idx, (_, expr_sql, _)) in chosen_exprs.iter().enumerate() {
                let create_idx = format!("CREATE INDEX idx_{iter}_{idx} ON t(({expr_sql}))");
                limbo_conn.execute(&create_idx).unwrap();
                sqlite_conn.execute(&create_idx, params![]).unwrap();
            }

            // --- Seed data: 50 rows ---
            let mut next_id: i64 = 1;
            for _ in 0..50 {
                let stmt = random_insert(&mut rng, &mut next_id);
                helpers::execute_on_both(
                    &limbo_conn,
                    &sqlite_conn,
                    &stmt,
                    &format!("seed insert\nseed: {seed}\niter: {iter}"),
                );
            }

            // --- DML loop: 20 random operations ---
            let mut history: Vec<String> = Vec::with_capacity(32);
            for _step in 0..20 {
                let action = rng.random_range(0..100);
                let stmt = match action {
                    // 40% INSERT (including INSERT OR REPLACE/IGNORE)
                    0..=29 => random_insert(&mut rng, &mut next_id),
                    30..=39 => random_insert_or_replace(&mut rng, next_id - 1),
                    // 30% UPDATE
                    40..=69 => random_update(&mut rng, next_id, &ref_cols),
                    // 20% DELETE
                    70..=89 => random_delete(&mut rng, next_id),
                    // 10% Query with expression predicate (inline differential check)
                    90..=99 => {
                        let (_, expr_sql, _) = chosen_exprs.choose(&mut rng).unwrap();
                        let query =
                            format!("SELECT * FROM t WHERE ({expr_sql}) IS NOT NULL ORDER BY id");
                        let ctx = format!(
                            "expression query check\nseed: {seed}\niter: {iter}\nhistory:\n{}",
                            helpers::history_tail(&history, 20)
                        );
                        helpers::assert_differential(&limbo_conn, &sqlite_conn, &query, &ctx);
                        // Not a DML statement, continue to next step.
                        continue;
                    }
                    _ => unreachable!(),
                };

                history.push(stmt.clone());
                let ctx = format!(
                    "DML execution\nseed: {seed}\niter: {iter}\nhistory:\n{}",
                    helpers::history_tail(&history, 20)
                );
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, &stmt, &ctx);
            }

            // --- Verification ---
            let verify_ctx = format!(
                "final verification\nseed: {seed}\niter: {iter}\nhistory:\n{}",
                helpers::history_tail(&history, 30)
            );

            // Full table differential.
            helpers::assert_differential(
                &limbo_conn,
                &sqlite_conn,
                "SELECT * FROM t ORDER BY id",
                &verify_ctx,
            );

            // Expression index scan differential (one per index).
            for (_, expr_sql, _) in &chosen_exprs {
                let query = format!("SELECT ({expr_sql}), id FROM t ORDER BY ({expr_sql}), id");
                helpers::assert_differential(&limbo_conn, &sqlite_conn, &query, &verify_ctx);
            }

            // Integrity check on both engines.
            let limbo_ic = limbo_exec_rows(&limbo_conn, "PRAGMA integrity_check");
            let sqlite_ic = sqlite_exec_rows(&sqlite_conn, "PRAGMA integrity_check");
            assert_eq!(
                limbo_ic, sqlite_ic,
                "integrity_check mismatch!\nseed: {seed}\niter: {iter}\nlimbo: {limbo_ic:?}\nsqlite: {sqlite_ic:?}"
            );

            // --- Teardown ---
            limbo_conn.execute("DROP TABLE t").unwrap();
            sqlite_conn.execute("DROP TABLE t", params![]).unwrap();
        }
    }
}
