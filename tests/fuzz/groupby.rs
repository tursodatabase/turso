//! Differential fuzz tests for GROUP BY with aggregate functions.
//!
//! Compares GROUP BY query results between Turso and SQLite using all built-in
//! aggregate functions (excluding JSON and external).

#[cfg(test)]
mod tests {
    use core_tester::common::{
        limbo_exec_rows, maybe_setup_tracing, rng_from_time_or_env, sqlite_exec_rows, TempDatabase,
    };
    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rusqlite::types::Value;

    /// Compare two result sets with approximate float comparison.
    fn results_approximately_equal(
        turso: &[Vec<Value>],
        sqlite: &[Vec<Value>],
    ) -> Result<(), String> {
        if turso.len() != sqlite.len() {
            return Err(format!(
                "Row count mismatch: Turso={}, SQLite={}",
                turso.len(),
                sqlite.len()
            ));
        }

        for (row_idx, (t_row, s_row)) in turso.iter().zip(sqlite.iter()).enumerate() {
            if t_row.len() != s_row.len() {
                return Err(format!(
                    "Column count mismatch at row {row_idx}: Turso={}, SQLite={}",
                    t_row.len(),
                    s_row.len()
                ));
            }

            for (col_idx, (t_val, s_val)) in t_row.iter().zip(s_row.iter()).enumerate() {
                if !values_approximately_equal(t_val, s_val) {
                    return Err(format!(
                        "Value mismatch at row {row_idx}, col {col_idx}: Turso={t_val:?}, SQLite={s_val:?}"
                    ));
                }
            }
        }

        Ok(())
    }

    fn values_approximately_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Real(a), Value::Real(b)) => floats_approximately_equal(*a, *b),
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            _ => false,
        }
    }

    fn floats_approximately_equal(a: f64, b: f64) -> bool {
        if a.is_nan() && b.is_nan() {
            return true;
        }
        if a.is_infinite() && b.is_infinite() {
            return a.signum() == b.signum();
        }
        let abs_diff = (a - b).abs();
        let max_abs = a.abs().max(b.abs());
        // Use absolute tolerance for values near zero, relative tolerance otherwise
        if max_abs < 1e-10 {
            abs_diff < 1e-10
        } else {
            abs_diff / max_abs < 1e-9
        }
    }

    /// Value pool - avoid extreme values that cause overflow when summed.
    const INT_VALUES: [i64; 10] = [0, 1, -1, 42, -42, 100, -100, 1000, -1000, 999];
    #[allow(clippy::approx_constant)]
    const FLOAT_VALUES: [f64; 8] = [0.0, 1.5, -1.5, 3.14159, -3.14159, 100.0, 0.001, -0.001];
    const TEXT_VALUES: [&str; 12] = [
        "a", "b", "c", "A", "B", "C", "hello", "world", "foo", "bar", "123", "3.14",
    ];

    fn random_literal(rng: &mut impl Rng) -> String {
        match rng.random_range(0..4) {
            0 => INT_VALUES[rng.random_range(0..INT_VALUES.len())].to_string(),
            1 => format!("{}", FLOAT_VALUES[rng.random_range(0..FLOAT_VALUES.len())]),
            2 => format!("'{}'", TEXT_VALUES[rng.random_range(0..TEXT_VALUES.len())]),
            _ => "NULL".to_string(),
        }
    }

    fn random_column(rng: &mut impl Rng) -> &'static str {
        ["ival", "fval", "sval"].choose(rng).unwrap()
    }

    fn random_group_column(rng: &mut impl Rng) -> &'static str {
        ["g1", "g2"].choose(rng).unwrap()
    }

    /// Generate a random aggregate expression.
    fn random_agg(rng: &mut impl Rng) -> String {
        let col = random_column(rng);
        let funcs: &[(&str, bool)] = &[
            ("COUNT", true),
            ("SUM", true),
            ("AVG", true),
            ("MIN", false),
            ("MAX", false),
            ("TOTAL", true),
        ];
        let (func, supports_distinct) = funcs.choose(rng).unwrap();
        let use_distinct = *supports_distinct && rng.random_bool(0.25);

        if use_distinct {
            format!("{func}(DISTINCT {col})")
        } else {
            format!("{func}({col})")
        }
    }

    /// Generate random GROUP BY columns/expressions.
    /// Returns (group_cols, order_cols) where order_cols includes MIN(id) as final tiebreaker.
    fn random_group_by(rng: &mut impl Rng) -> (String, String) {
        let num_cols = rng.random_range(1..=2);
        let mut cols = Vec::new();
        let mut order_cols = Vec::new();

        for _ in 0..num_cols {
            let use_expr = rng.random_bool(0.2);
            let col = if use_expr {
                let base = random_group_column(rng);
                let exprs = [
                    format!("{base} % 3"),
                    format!("{base} / 2"),
                    format!("abs({base})"),
                ];
                exprs.choose(rng).unwrap().clone()
            } else {
                random_group_column(rng).to_string()
            };
            order_cols.push(format!("{col} IS NULL, {col}"));
            cols.push(col);
        }

        // Add MIN(id) as final tiebreaker for deterministic order
        order_cols.push("MIN(id)".to_string());

        (cols.join(", "), order_cols.join(", "))
    }

    /// Generate a random WHERE clause.
    fn random_where(rng: &mut impl Rng) -> Option<String> {
        if !rng.random_bool(0.4) {
            return None;
        }

        let col = random_column(rng);
        let conditions = [
            format!("{col} > 0"),
            format!("{col} < 0"),
            format!("{col} IS NOT NULL"),
            format!("{col} IS NULL"),
            format!("{col} BETWEEN -100 AND 100"),
            "g2 IN (1, 2, 3)".to_string(),
            "g2 > 2".to_string(),
        ];
        Some(conditions.choose(rng).unwrap().clone())
    }

    /// Generate a random HAVING clause.
    fn random_having(rng: &mut impl Rng) -> Option<String> {
        if !rng.random_bool(0.3) {
            return None;
        }

        let conditions = [
            "COUNT(*) > 2",
            "COUNT(*) > 5",
            "SUM(ival) > 0",
            "SUM(ival) IS NOT NULL",
            "AVG(ival) < 100",
            "AVG(fval) IS NOT NULL",
            "MIN(ival) > -500",
            "MAX(ival) < 500",
            "TOTAL(ival) != 0",
            "COUNT(DISTINCT ival) > 1",
        ];
        Some(conditions.choose(rng).unwrap().to_string())
    }

    /// Generate a random ORDER BY clause.
    /// Uses CAST(agg AS INTEGER) for float aggregates to avoid precision affecting sort order.
    fn random_order_by(rng: &mut impl Rng, group_order: &str) -> String {
        if rng.random_bool(0.3) {
            let agg = random_agg(rng);
            let dir = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
            // Cast to integer to avoid float precision differences affecting sort
            format!("CAST({agg} AS INTEGER) {dir}, {group_order}")
        } else {
            group_order.to_string()
        }
    }

    /// Generate a random LIMIT clause.
    fn random_limit(rng: &mut impl Rng, max_rows: usize) -> Option<String> {
        if !rng.random_bool(0.4) {
            return None;
        }
        let limit = rng.random_range(1..=max_rows.max(1));
        if rng.random_bool(0.5) {
            let offset = rng.random_range(0..=max_rows / 2);
            Some(format!("LIMIT {limit} OFFSET {offset}"))
        } else {
            Some(format!("LIMIT {limit}"))
        }
    }

    /// Generate random index DDL for testing sort-stream aggregation path.
    fn random_indexes(rng: &mut impl Rng) -> Vec<String> {
        let mut indexes = Vec::new();
        // 50% chance of index on g1
        if rng.random_bool(0.5) {
            indexes.push("CREATE INDEX idx_g1 ON t(g1)".to_string());
        }
        // 50% chance of index on g2
        if rng.random_bool(0.5) {
            indexes.push("CREATE INDEX idx_g2 ON t(g2)".to_string());
        }
        // 30% chance of composite index
        if rng.random_bool(0.3) {
            if rng.random_bool(0.5) {
                indexes.push("CREATE INDEX idx_g1_g2 ON t(g1, g2)".to_string());
            } else {
                indexes.push("CREATE INDEX idx_g2_g1 ON t(g2, g1)".to_string());
            }
        }
        indexes
    }

    /// Build a fully randomized GROUP BY query.
    fn build_random_query(rng: &mut impl Rng, num_rows: usize) -> String {
        // Random number of aggregates (1-5)
        let num_aggs = rng.random_range(1..=5);
        let aggs: Vec<String> = (0..num_aggs)
            .map(|i| format!("{} AS a{i}", random_agg(rng)))
            .collect();

        // Always include COUNT(*)
        let mut select_parts = vec!["COUNT(*) AS cnt".to_string()];
        select_parts.extend(aggs);

        // Decide if we have GROUP BY or just aggregate whole table
        let has_group_by = rng.random_bool(0.85);

        if has_group_by {
            let (group_cols, group_order) = random_group_by(rng);

            // Add group columns to SELECT
            for col in group_cols.split(", ") {
                select_parts.insert(0, col.to_string());
            }

            let select = select_parts.join(", ");
            let where_clause = random_where(rng)
                .map(|w| format!(" WHERE {w}"))
                .unwrap_or_default();
            let having_clause = random_having(rng)
                .map(|h| format!(" HAVING {h}"))
                .unwrap_or_default();
            let order_by = random_order_by(rng, &group_order);
            let limit_clause = random_limit(rng, num_rows)
                .map(|l| format!(" {l}"))
                .unwrap_or_default();

            format!(
                "SELECT {select} FROM t{where_clause} GROUP BY {group_cols}{having_clause} ORDER BY {order_by}{limit_clause}"
            )
        } else {
            // Aggregate whole table (no GROUP BY)
            let select = select_parts.join(", ");
            let where_clause = random_where(rng)
                .map(|w| format!(" WHERE {w}"))
                .unwrap_or_default();
            format!("SELECT {select} FROM t{where_clause}")
        }
    }

    #[turso_macros::test]
    pub fn groupby_comprehensive_fuzz(db: TempDatabase) {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("groupby_comprehensive_fuzz seed: {seed}");

        let opts = db.db_opts;
        let flags = db.db_flags;
        let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

        const OUTER_ITERS: usize = 5;
        const INNER_ITERS: usize = 50;

        for outer in 0..OUTER_ITERS {
            println!(
                "groupby_comprehensive_fuzz: dataset {}/{}",
                outer + 1,
                OUTER_ITERS
            );

            let table_ddl = "CREATE TABLE t (\
                id INTEGER PRIMARY KEY, \
                g1 TEXT, \
                g2 INTEGER, \
                ival INTEGER, \
                fval REAL, \
                sval TEXT)";

            let limbo_db = builder.clone().with_init_sql(table_ddl).build();
            let limbo_conn = limbo_db.connect_limbo();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute(table_ddl, ()).unwrap();

            // Create random indexes to test sort-stream aggregation path
            let indexes = random_indexes(&mut rng);
            for idx_ddl in &indexes {
                sqlite_conn.execute(idx_ddl, ()).unwrap();
                limbo_conn.execute(idx_ddl).unwrap();
            }
            if !indexes.is_empty() {
                println!("  indexes: {}", indexes.join(", "));
            }

            // Limited group values to create meaningful groups
            let g1_vals: Vec<&str> = vec!["'a'", "'b'", "'c'", "'d'", "NULL"];
            let g2_vals: Vec<i64> = vec![1, 2, 3, 4, 5];

            // Insert random data
            let num_rows = rng.random_range(500..=10000);
            const BATCH_SIZE: usize = 100;
            for batch_start in (0..num_rows).step_by(BATCH_SIZE) {
                let batch_end = (batch_start + BATCH_SIZE).min(num_rows);
                let mut values_list = Vec::new();

                for i in batch_start..batch_end {
                    let g1 = g1_vals.choose(&mut rng).unwrap();
                    let g2 = g2_vals.choose(&mut rng).unwrap();
                    let ival = random_literal(&mut rng);
                    let fval = random_literal(&mut rng);
                    let sval = random_literal(&mut rng);
                    values_list.push(format!("({i}, {g1}, {g2}, {ival}, {fval}, {sval})"));
                }

                let insert = format!(
                    "INSERT INTO t (id, g1, g2, ival, fval, sval) VALUES {}",
                    values_list.join(", ")
                );
                sqlite_conn.execute(&insert, ()).unwrap();
                limbo_conn.execute(&insert).unwrap();
            }

            // Run multiple queries against this dataset
            for inner in 0..INNER_ITERS {
                let query = build_random_query(&mut rng, num_rows);

                println!("  query {}/{}: {query}", inner + 1, INNER_ITERS);

                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
                let limbo_rows = limbo_exec_rows(&limbo_conn, &query);

                if let Err(e) = results_approximately_equal(&limbo_rows, &sqlite_rows) {
                    panic!(
                        "MISMATCH!\n{e}\nQuery: {query}\nSeed: {seed}\nDataset: {outer}, Query: {inner}\n\
                         Turso: {limbo_rows:?}\nSQLite: {sqlite_rows:?}"
                    );
                }
            }
        }
    }
}
