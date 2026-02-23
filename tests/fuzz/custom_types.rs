//! Fuzzer for custom type cross-feature interactions.
//!
//! Tests that custom type columns (numeric(10,2)) work correctly with:
//! ORDER BY, GROUP BY, DISTINCT, JOINs (including self-joins), indexes,
//! subqueries, aggregates, CTEs, UNION, CASE WHEN, NULL handling, and
//! multi-column ordering.
//!
//! Queries that fail (unsupported features) are silently skipped, so this
//! test automatically gains coverage as Turso implements new SQL features.
//! Invariant violations are collected and reported at the end.
//!
//! Runs for 10 seconds by default. Override with FUZZ_DURATION_SECS env var.
//! Reproduce a failure: `SEED=<seed> cargo test -p core_tester --test fuzz_tests custom_types -- --nocapture`

#[cfg(test)]
mod tests {
    use core_tester::common::{
        limbo_exec_rows_fallible, maybe_setup_tracing, rng_from_time_or_env, TempDatabase,
    };
    use rand::Rng;
    use rusqlite::types::Value;
    use std::collections::HashSet;

    /// String pool for label/category columns.
    const LABELS: [&str; 4] = ["alpha", "beta", "gamma", "delta"];

    /// Generate a random numeric(10,2) value as a string.
    fn random_numeric(rng: &mut impl Rng) -> String {
        let val: f64 = rng.random_range(-9999999.99..=9999999.99);
        format!("{val:.2}")
    }

    /// Parse a Value as f64 for numeric comparison. Returns None for NULL.
    fn parse_numeric_value(val: &Value) -> Option<f64> {
        match val {
            Value::Text(s) => s.parse::<f64>().ok(),
            Value::Integer(i) => Some(*i as f64),
            Value::Real(r) => Some(*r),
            Value::Null => None,
            Value::Blob(_) => None,
        }
    }

    /// Verify that a column is sorted correctly.
    /// NULLs sort first in ASC, last in DESC (SQLite semantics).
    fn check_sorted(
        rows: &[Vec<Value>],
        col: usize,
        desc: bool,
        context: &str,
    ) -> Result<(), String> {
        for i in 1..rows.len() {
            let prev = parse_numeric_value(&rows[i - 1][col]);
            let curr = parse_numeric_value(&rows[i][col]);
            match (prev, curr) {
                (None, None) => {}
                (None, Some(_)) => {
                    if desc {
                        return Err(format!(
                            "Sort violation at row {i}: NULL before non-NULL in DESC. {context}"
                        ));
                    }
                }
                (Some(_), None) => {
                    if !desc {
                        return Err(format!(
                            "Sort violation at row {i}: non-NULL before NULL in ASC. {context}"
                        ));
                    }
                }
                (Some(p), Some(c)) => {
                    if desc && p < c {
                        return Err(format!(
                            "Sort violation at row {i}: {p} < {c} in DESC. {context}"
                        ));
                    } else if !desc && p > c {
                        return Err(format!(
                            "Sort violation at row {i}: {p} > {c} in ASC. {context}"
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Verify no duplicate values in a column.
    fn check_no_duplicates(rows: &[Vec<Value>], col: usize, context: &str) -> Result<(), String> {
        let mut seen = HashSet::new();
        for (i, row) in rows.iter().enumerate() {
            let key = format!("{:?}", row[col]);
            if !seen.insert(key.clone()) {
                return Err(format!("Duplicate value at row {i}: {key}. {context}"));
            }
        }
        Ok(())
    }

    /// Verify row count <= limit.
    fn check_limit(rows: &[Vec<Value>], limit: usize, context: &str) -> Result<(), String> {
        if rows.len() > limit {
            return Err(format!(
                "Got {} rows, expected <= {limit}. {context}",
                rows.len()
            ));
        }
        Ok(())
    }

    /// Verify GROUP BY count sum equals total rows.
    fn check_group_count_sum(
        rows: &[Vec<Value>],
        key_col: usize,
        count_col: usize,
        total_rows: usize,
        context: &str,
    ) -> Result<(), String> {
        check_no_duplicates(rows, key_col, context)?;
        let mut sum: i64 = 0;
        for r in rows {
            match &r[count_col] {
                Value::Integer(i) => sum += i,
                other => return Err(format!("Expected integer count, got {other:?}. {context}")),
            }
        }
        if sum as usize != total_rows {
            return Err(format!(
                "Group count sum {sum} != total rows {total_rows}. {context}"
            ));
        }
        Ok(())
    }

    /// Verify all non-NULL values in a column satisfy a predicate.
    fn check_all_satisfy(
        rows: &[Vec<Value>],
        col: usize,
        pred: impl Fn(f64) -> bool,
        pred_desc: &str,
        context: &str,
    ) -> Result<(), String> {
        for (i, row) in rows.iter().enumerate() {
            if let Some(v) = parse_numeric_value(&row[col]) {
                if !pred(v) {
                    return Err(format!(
                        "Value {v} at row {i} fails predicate '{pred_desc}'. {context}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Verify join condition: two columns have equal values.
    fn check_join_condition(
        rows: &[Vec<Value>],
        col_a: usize,
        col_b: usize,
        context: &str,
    ) -> Result<(), String> {
        for (i, row) in rows.iter().enumerate() {
            let a = parse_numeric_value(&row[col_a]);
            let b = parse_numeric_value(&row[col_b]);
            if a != b {
                return Err(format!(
                    "Join condition violated at row {i}: {:?} != {:?}. {context}",
                    row[col_a], row[col_b]
                ));
            }
        }
        Ok(())
    }

    /// Verify multi-column sort (text col ASC, then numeric col ASC).
    fn check_multi_column_sort(
        rows: &[Vec<Value>],
        text_col: usize,
        num_col: usize,
        context: &str,
    ) -> Result<(), String> {
        for i in 1..rows.len() {
            let prev_text = match &rows[i - 1][text_col] {
                Value::Text(s) => s.clone(),
                Value::Null => String::new(),
                other => return Err(format!("Expected text in col {text_col}, got {other:?}")),
            };
            let curr_text = match &rows[i][text_col] {
                Value::Text(s) => s.clone(),
                Value::Null => String::new(),
                other => return Err(format!("Expected text in col {text_col}, got {other:?}")),
            };
            match prev_text.cmp(&curr_text) {
                std::cmp::Ordering::Greater => {
                    return Err(format!(
                        "Primary sort violation at row {i}: '{prev_text}' > '{curr_text}'. {context}"
                    ));
                }
                std::cmp::Ordering::Equal => {
                    let prev_num = parse_numeric_value(&rows[i - 1][num_col]);
                    let curr_num = parse_numeric_value(&rows[i][num_col]);
                    match (prev_num, curr_num) {
                        (None, _) => {}
                        (Some(_), None) => {
                            return Err(format!(
                                "Secondary sort violation at row {i}: non-NULL before NULL. {context}"
                            ));
                        }
                        (Some(p), Some(c)) => {
                            if p > c {
                                return Err(format!(
                                    "Secondary sort violation at row {i}: {p} > {c}. {context}"
                                ));
                            }
                        }
                    }
                }
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(())
    }

    #[test]
    fn fuzz_custom_type_invariants() {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("fuzz_custom_type_invariants seed: {seed}");

        let db = TempDatabase::builder()
            .with_opts(
                turso_core::DatabaseOpts::new()
                    .with_index_method(true)
                    .with_encryption(true)
                    .with_triggers(true)
                    .with_attach(true)
                    .with_strict(true)
                    .with_custom_types(true),
            )
            .build();
        let conn = db.connect_limbo();

        // --- Setup schema ---
        conn.execute(
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, val numeric(10,2), label TEXT, n INTEGER) STRICT",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE t2(id INTEGER PRIMARY KEY, amount numeric(10,2), category TEXT) STRICT",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE t3(id INTEGER PRIMARY KEY, val numeric(10,2), extra TEXT) STRICT",
        )
        .unwrap();
        conn.execute("CREATE INDEX idx_t3_val ON t3(val)").unwrap();
        // Multi-column index on t1: exercises composite index with custom type
        conn.execute("CREATE INDEX idx_t1_label_val ON t1(label, val)")
            .unwrap();
        // t4: mutable table for UPDATE/UPSERT/DELETE/INSERT...SELECT patterns
        conn.execute(
            "CREATE TABLE t4(id INTEGER PRIMARY KEY, val numeric(10,2), label TEXT) STRICT",
        )
        .unwrap();
        // Index on t4 so mutations exercise index maintenance paths
        conn.execute("CREATE INDEX idx_t4_val ON t4(val)").unwrap();
        // t4_log: trigger audit log
        conn.execute("CREATE TABLE t4_log(old_val TEXT, new_val TEXT) STRICT")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER tr_t4_update AFTER UPDATE ON t4 BEGIN \
             INSERT INTO t4_log VALUES (CAST(OLD.val AS TEXT), CAST(NEW.val AS TEXT)); \
             END",
        )
        .unwrap();
        // t5: table with custom type names that contain SQLite affinity-triggering
        // substrings. "doubled" contains "DOUB" which SQLite rules would map to REAL,
        // but the BASE type is integer. "charmed" contains "CHAR" which would map to
        // TEXT, but the BASE type is integer. Verify that the BASE type wins.
        conn.execute("CREATE TYPE doubled BASE integer ENCODE (value * 2) DECODE (value / 2)")
            .unwrap();
        conn.execute("CREATE TYPE charmed BASE integer ENCODE (value * 3) DECODE (value / 3)")
            .unwrap();
        conn.execute("CREATE TABLE t5(id INTEGER PRIMARY KEY, d doubled, c charmed) STRICT")
            .unwrap();

        // --- Populate data ---
        let t1_rows: usize = rng.random_range(50..=100);
        let t2_rows: usize = rng.random_range(50..=100);
        let t3_rows: usize = rng.random_range(50..=100);

        let mut t1_vals: Vec<f64> = Vec::new();
        let mut t2_vals: Vec<f64> = Vec::new();

        for i in 0..t1_rows {
            let val = random_numeric(&mut rng);
            let label = LABELS[rng.random_range(0..LABELS.len())];
            let n: i32 = rng.random_range(-1000..=1000);
            conn.execute(format!(
                "INSERT INTO t1 VALUES ({i}, '{val}', '{label}', {n})"
            ))
            .unwrap();
            t1_vals.push(val.parse().unwrap());
        }

        // Insert NULL rows for NULL handling tests
        conn.execute(format!(
            "INSERT INTO t1 VALUES ({t1_rows}, NULL, 'alpha', 0)"
        ))
        .unwrap();
        conn.execute(format!(
            "INSERT INTO t1 VALUES ({}, NULL, 'beta', 1)",
            t1_rows + 1
        ))
        .unwrap();
        let t1_total = t1_rows + 2;

        // Ensure some overlap between t1 and t2 values for JOIN tests
        for i in 0..t2_rows {
            let val = if i < 5 && i < t1_vals.len() {
                format!("{:.2}", t1_vals[i])
            } else {
                random_numeric(&mut rng)
            };
            let cat = LABELS[rng.random_range(0..LABELS.len())];
            conn.execute(format!("INSERT INTO t2 VALUES ({i}, '{val}', '{cat}')"))
                .unwrap();
            t2_vals.push(val.parse().unwrap());
        }

        for i in 0..t3_rows {
            let val = random_numeric(&mut rng);
            let extra = LABELS[rng.random_range(0..LABELS.len())];
            conn.execute(format!("INSERT INTO t3 VALUES ({i}, '{val}', '{extra}')"))
                .unwrap();
        }

        // Populate t5 with integer values for affinity-sensitive custom types
        let t5_rows: usize = rng.random_range(20..=40);
        for i in 0..t5_rows {
            let d_val: i32 = rng.random_range(1..=1000);
            let c_val: i32 = rng.random_range(1..=1000);
            conn.execute(format!("INSERT INTO t5 VALUES ({i}, {d_val}, {c_val})"))
                .unwrap();
        }

        // Helper to repopulate t4 (mutable table) before each mutation pattern
        let t4_size: usize = 20;
        fn repopulate_t4(
            db: &TempDatabase,
            conn: &std::sync::Arc<turso_core::Connection>,
            rng: &mut impl Rng,
            t4_size: usize,
        ) {
            limbo_exec_rows_fallible(db, conn, "DELETE FROM t4").unwrap();
            limbo_exec_rows_fallible(db, conn, "DELETE FROM t4_log").unwrap();
            for i in 0..t4_size {
                let val = random_numeric(rng);
                let label = LABELS[rng.random_range(0..LABELS.len())];
                limbo_exec_rows_fallible(
                    db,
                    conn,
                    &format!("INSERT INTO t4 VALUES ({i}, '{val}', '{label}')"),
                )
                .unwrap();
            }
        }
        repopulate_t4(&db, &conn, &mut rng, t4_size);

        // --- Query loop ---
        // Default 10s time budget; override with FUZZ_DURATION_SECS env var.
        let duration_secs: u64 = std::env::var("FUZZ_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(duration_secs);
        const NUM_PATTERNS: usize = 41;
        let mut executed = 0u64;
        let mut skipped = 0u64;
        let mut violations: Vec<String> = Vec::new();
        let mut iter = 0usize;
        let mut t4_dirty = false; // track whether t4 was mutated

        while std::time::Instant::now() < deadline {
            iter += 1;

            let pattern = rng.random_range(0..NUM_PATTERNS);

            let desc = rng.random_bool(0.5);
            let dir = if desc { "DESC" } else { "ASC" };

            // Repopulate t4 only when a mutation pattern is selected AND t4 was previously mutated
            let is_mutation_pattern =
                (18..=22).contains(&pattern) || pattern == 36 || pattern == 40;
            if is_mutation_pattern && t4_dirty {
                repopulate_t4(&db, &conn, &mut rng, t4_size);
                t4_dirty = false;
            }

            let result: Result<(), String> = (|| {
                match pattern {
                    // --- ORDER BY ---
                    0 => {
                        let query = format!("SELECT val FROM t1 ORDER BY val {dir}");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, desc, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- ORDER BY + LIMIT ---
                    1 => {
                        let limit = rng.random_range(1..=t1_total);
                        let query = format!("SELECT val FROM t1 ORDER BY val {dir} LIMIT {limit}");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, desc, &format!("[{iter}] {query}"))?;
                        check_limit(&rows, limit, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- DISTINCT ---
                    2 => {
                        let query = "SELECT DISTINCT val FROM t1".to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_no_duplicates(&rows, 0, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- GROUP BY + COUNT ---
                    3 => {
                        let query = "SELECT val, COUNT(*) as cnt FROM t1 GROUP BY val".to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_group_count_sum(&rows, 0, 1, t1_total, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- GROUP BY category ---
                    4 => {
                        let query = "SELECT category, COUNT(*) as cnt FROM t2 GROUP BY category"
                            .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_group_count_sum(&rows, 0, 1, t2_rows, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- MIN / MAX ---
                    5 => {
                        let query = "SELECT MIN(val), MAX(val) FROM t1".to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        if rows.len() != 1 {
                            return Err(format!("[{iter}] MIN/MAX returned {} rows", rows.len()));
                        }
                        let min_val = parse_numeric_value(&rows[0][0]);
                        let max_val = parse_numeric_value(&rows[0][1]);
                        if let (Some(min), Some(max)) = (min_val, max_val) {
                            if min > max {
                                return Err(format!("[{iter}] MIN({min}) > MAX({max})"));
                            }
                            for &v in &t1_vals {
                                if v < min {
                                    return Err(format!("[{iter}] Value {v} < MIN({min})"));
                                }
                                if v > max {
                                    return Err(format!("[{iter}] Value {v} > MAX({max})"));
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- JOIN ---
                    6 => {
                        let query =
                            "SELECT t1.val, t2.amount FROM t1 JOIN t2 ON t1.val = t2.amount"
                                .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_join_condition(&rows, 0, 1, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- Subquery IN ---
                    7 => {
                        let query =
                            "SELECT val FROM t1 WHERE val IN (SELECT amount FROM t2) ORDER BY val"
                                .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        let t2_set: HashSet<String> =
                            t2_vals.iter().map(|v| format!("{v:.2}")).collect();
                        for (i, row) in rows.iter().enumerate() {
                            if let Some(v) = parse_numeric_value(&row[0]) {
                                let formatted = format!("{v:.2}");
                                if !t2_set.contains(&formatted) {
                                    return Err(format!("[{iter}] Row {i}: val {v} not in t2"));
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- Scalar subquery ---
                    8 => {
                        let t2_min = t2_vals.iter().cloned().fold(f64::INFINITY, f64::min);
                        let query = "SELECT val FROM t1 WHERE val = (SELECT MIN(amount) FROM t2)"
                            .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        for (i, row) in rows.iter().enumerate() {
                            if let Some(v) = parse_numeric_value(&row[0]) {
                                if (v - t2_min).abs() >= 0.015 {
                                    return Err(format!(
                                        "[{iter}] Row {i}: val {v} != min({t2_min})"
                                    ));
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- UNION ---
                    9 => {
                        let query =
                            "SELECT val FROM t1 WHERE val IS NOT NULL UNION SELECT amount FROM t2 ORDER BY 1"
                                .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        check_no_duplicates(&rows, 0, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- CTE ---
                    10 => {
                        let query = format!(
                            "WITH cte AS (SELECT val FROM t1 ORDER BY val {dir}) SELECT val FROM cte"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, desc, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- Index ORDER BY (t3 has index) ---
                    11 => {
                        let query = format!("SELECT val FROM t3 ORDER BY val {dir}");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, desc, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- Index WHERE + ORDER BY ---
                    12 => {
                        let threshold = random_numeric(&mut rng);
                        let query =
                            format!("SELECT val FROM t3 WHERE val > '{threshold}' ORDER BY val");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        let t: f64 = threshold.parse().unwrap();
                        check_all_satisfy(
                            &rows,
                            0,
                            |v| v > t,
                            &format!("val > {threshold}"),
                            &format!("[{iter}] {query}"),
                        )?;
                        executed += 1;
                    }
                    // --- Index range query ---
                    13 => {
                        let a = random_numeric(&mut rng);
                        let b = random_numeric(&mut rng);
                        let (lo, hi) = if a.parse::<f64>().unwrap() <= b.parse::<f64>().unwrap() {
                            (a, b)
                        } else {
                            (b, a)
                        };
                        let query = format!(
                            "SELECT val FROM t3 WHERE val >= '{lo}' AND val <= '{hi}' ORDER BY val"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        let lo_f: f64 = lo.parse().unwrap();
                        let hi_f: f64 = hi.parse().unwrap();
                        check_all_satisfy(
                            &rows,
                            0,
                            |v| v >= lo_f && v <= hi_f,
                            &format!("val in [{lo}, {hi}]"),
                            &format!("[{iter}] {query}"),
                        )?;
                        executed += 1;
                    }
                    // --- WHERE with custom comparison (no index) ---
                    14 => {
                        let threshold = random_numeric(&mut rng);
                        let query =
                            format!("SELECT val FROM t1 WHERE val > '{threshold}' ORDER BY val");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        let t: f64 = threshold.parse().unwrap();
                        check_all_satisfy(
                            &rows,
                            0,
                            |v| v > t,
                            &format!("val > {threshold}"),
                            &format!("[{iter}] {query}"),
                        )?;
                        executed += 1;
                    }
                    // --- CASE WHEN ---
                    15 => {
                        let threshold = random_numeric(&mut rng);
                        let t: f64 = threshold.parse().unwrap();
                        let query = format!(
                            "SELECT CASE WHEN val > '{threshold}' THEN 'high' ELSE 'low' END, val FROM t1 WHERE val IS NOT NULL"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        for (i, row) in rows.iter().enumerate() {
                            let cat = match &row[0] {
                                Value::Text(s) => s.as_str(),
                                other => {
                                    return Err(format!(
                                        "[{iter}] Expected text, got {other:?} at row {i}"
                                    ));
                                }
                            };
                            if let Some(v) = parse_numeric_value(&row[1]) {
                                let expected = if v > t { "high" } else { "low" };
                                if cat != expected {
                                    return Err(format!(
                                        "[{iter}] CASE mismatch row {i}: val={v}, threshold={t}, got '{cat}', expected '{expected}'"
                                    ));
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- NULL filtering ---
                    16 => {
                        let query =
                            "SELECT val FROM t1 WHERE val IS NOT NULL ORDER BY val".to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        if rows.len() != t1_rows {
                            return Err(format!(
                                "[{iter}] Expected {t1_rows} non-NULL rows, got {}",
                                rows.len()
                            ));
                        }
                        for (i, row) in rows.iter().enumerate() {
                            if matches!(row[0], Value::Null) {
                                return Err(format!(
                                    "[{iter}] NULL at row {i} after IS NOT NULL filter"
                                ));
                            }
                        }
                        executed += 1;
                    }
                    // --- Multi-column ORDER BY ---
                    17 => {
                        let query =
                            "SELECT label, val FROM t1 WHERE val IS NOT NULL ORDER BY label, val"
                                .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_multi_column_sort(&rows, 0, 1, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- UPDATE custom type column + verify ---
                    18 => {
                        let new_val = random_numeric(&mut rng);
                        let target_id = rng.random_range(0..t4_size);
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("UPDATE t4 SET val = '{new_val}' WHERE id = {target_id}"),
                        )
                        .map_err(|_| String::new())?;
                        let query = format!("SELECT val FROM t4 WHERE id = {target_id}");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        if rows.len() != 1 {
                            return Err(format!(
                                "[{iter}] UPDATE: expected 1 row, got {}",
                                rows.len()
                            ));
                        }
                        let got = parse_numeric_value(&rows[0][0]);
                        let expected: f64 = new_val.parse().unwrap();
                        if let Some(g) = got {
                            if (g - expected).abs() >= 0.015 {
                                return Err(format!(
                                    "[{iter}] UPDATE: expected {expected}, got {g}"
                                ));
                            }
                        }
                        executed += 1;
                    }
                    // --- UPDATE + trigger log verification ---
                    19 => {
                        let new_val = random_numeric(&mut rng);
                        let target_id = rng.random_range(0..t4_size);
                        // Get old value first
                        let old_query = format!("SELECT val FROM t4 WHERE id = {target_id}");
                        let old_rows = limbo_exec_rows_fallible(&db, &conn, &old_query)
                            .map_err(|_| String::new())?;
                        let old_val = old_rows.first().and_then(|r| parse_numeric_value(&r[0]));
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("UPDATE t4 SET val = '{new_val}' WHERE id = {target_id}"),
                        )
                        .map_err(|_| String::new())?;
                        // Check trigger logged the change with decoded values
                        let log_rows = limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            "SELECT old_val, new_val FROM t4_log",
                        )
                        .map_err(|_| String::new())?;
                        if log_rows.is_empty() {
                            return Err(format!("[{iter}] Trigger log is empty after UPDATE"));
                        }
                        // Verify new_val in trigger log matches what we set
                        let last_log = log_rows.last().unwrap();
                        if let Value::Text(logged_new) = &last_log[1] {
                            let logged: f64 = logged_new.parse().map_err(|_| String::new())?;
                            let expected: f64 = new_val.parse().unwrap();
                            if (logged - expected).abs() >= 0.015 {
                                return Err(format!(
                                    "[{iter}] Trigger log new_val: expected {expected}, got {logged}"
                                ));
                            }
                        }
                        // Verify old_val in trigger log matches what was there before
                        if let (Some(old), Value::Text(logged_old)) = (old_val, &last_log[0]) {
                            let logged: f64 = logged_old.parse().map_err(|_| String::new())?;
                            if (logged - old).abs() >= 0.015 {
                                return Err(format!(
                                    "[{iter}] Trigger log old_val: expected {old}, got {logged}"
                                ));
                            }
                        }
                        executed += 1;
                    }
                    // --- UPSERT (INSERT ... ON CONFLICT DO UPDATE) ---
                    20 => {
                        let new_val = random_numeric(&mut rng);
                        let target_id = rng.random_range(0..t4_size);
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!(
                                "INSERT INTO t4 VALUES ({target_id}, '{new_val}', 'upserted') \
                                 ON CONFLICT(id) DO UPDATE SET val = excluded.val, label = excluded.label"
                            ),
                        )
                        .map_err(|_| String::new())?;
                        let query = format!("SELECT val, label FROM t4 WHERE id = {target_id}");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        if rows.len() != 1 {
                            return Err(format!(
                                "[{iter}] UPSERT: expected 1 row, got {}",
                                rows.len()
                            ));
                        }
                        let got = parse_numeric_value(&rows[0][0]);
                        let expected: f64 = new_val.parse().unwrap();
                        if let Some(g) = got {
                            if (g - expected).abs() >= 0.015 {
                                return Err(format!(
                                    "[{iter}] UPSERT: expected {expected}, got {g}"
                                ));
                            }
                        }
                        if let Value::Text(lbl) = &rows[0][1] {
                            if lbl != "upserted" {
                                return Err(format!(
                                    "[{iter}] UPSERT: expected label 'upserted', got '{lbl}'"
                                ));
                            }
                        }
                        executed += 1;
                    }
                    // --- DELETE + verify count ---
                    21 => {
                        // Get count before delete
                        let before =
                            limbo_exec_rows_fallible(&db, &conn, "SELECT COUNT(*) FROM t4")
                                .map_err(|_| String::new())?;
                        let count_before = match &before[0][0] {
                            Value::Integer(n) => *n as usize,
                            _ => return Err(format!("[{iter}] DELETE: bad count")),
                        };
                        if count_before == 0 {
                            // Nothing to delete
                            executed += 1;
                            return Ok(());
                        }
                        let target_id = rng.random_range(0..t4_size);
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("DELETE FROM t4 WHERE id = {target_id}"),
                        )
                        .map_err(|_| String::new())?;
                        // Verify deleted row is gone
                        let check = limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("SELECT val FROM t4 WHERE id = {target_id}"),
                        )
                        .map_err(|_| String::new())?;
                        if !check.is_empty() {
                            return Err(format!("[{iter}] DELETE: row {target_id} still exists"));
                        }
                        executed += 1;
                    }
                    // --- INSERT...SELECT between custom type tables ---
                    22 => {
                        limbo_exec_rows_fallible(&db, &conn, "DELETE FROM t4")
                            .map_err(|_| String::new())?;
                        let limit = rng.random_range(5..=15);
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!(
                                "INSERT INTO t4(id, val, label) \
                                 SELECT id, val, label FROM t1 WHERE val IS NOT NULL LIMIT {limit}"
                            ),
                        )
                        .map_err(|_| String::new())?;
                        // Verify the inserted rows have valid decoded values
                        let rows =
                            limbo_exec_rows_fallible(&db, &conn, "SELECT val FROM t4 ORDER BY val")
                                .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] INSERT...SELECT"))?;
                        check_limit(&rows, limit, &format!("[{iter}] INSERT...SELECT"))?;
                        executed += 1;
                    }
                    // --- LEFT JOIN (NULLs on custom type column) ---
                    23 => {
                        // Use a value that likely doesn't exist in t2
                        let fake_val = "9999999.01";
                        let query = format!(
                            "SELECT t1.val, t2.amount FROM t1 LEFT JOIN t2 ON t1.val = t2.amount \
                             WHERE t1.val = '{fake_val}' OR t2.amount IS NULL"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        // Every row should have either matching vals or NULL t2.amount
                        for (i, row) in rows.iter().enumerate() {
                            let t1_v = parse_numeric_value(&row[0]);
                            let t2_v = parse_numeric_value(&row[1]);
                            match (t1_v, t2_v) {
                                (Some(a), Some(b)) if (a - b).abs() >= 0.015 => {
                                    return Err(format!(
                                        "[{iter}] LEFT JOIN row {i}: t1.val={a} != t2.amount={b}"
                                    ));
                                }
                                _ => {} // NULL t2.amount is fine for LEFT JOIN
                            }
                        }
                        executed += 1;
                    }
                    // --- HAVING on custom type ---
                    24 => {
                        let query =
                            "SELECT val, COUNT(*) as cnt FROM t1 GROUP BY val HAVING COUNT(*) > 1"
                                .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        // Every returned row must have count > 1
                        for (i, row) in rows.iter().enumerate() {
                            if let Value::Integer(cnt) = &row[1] {
                                if *cnt <= 1 {
                                    return Err(format!(
                                        "[{iter}] HAVING: row {i} has count {cnt} <= 1"
                                    ));
                                }
                            }
                        }
                        // All vals should be unique (GROUP BY)
                        check_no_duplicates(&rows, 0, &format!("[{iter}] HAVING"))?;
                        executed += 1;
                    }
                    // --- Comparison operators: <, >=, <=, !=, BETWEEN ---
                    25 => {
                        let threshold = random_numeric(&mut rng);
                        let t: f64 = threshold.parse().unwrap();
                        let op_choice = rng.random_range(0..5);
                        let (op, pred): (&str, Box<dyn Fn(f64) -> bool>) = match op_choice {
                            0 => ("<", Box::new(move |v| v < t)),
                            1 => (">=", Box::new(move |v| v >= t)),
                            2 => ("<=", Box::new(move |v| v <= t)),
                            3 => ("!=", Box::new(move |v| (v - t).abs() >= 0.005)),
                            _ => ("=", Box::new(move |v| (v - t).abs() < 0.005)),
                        };
                        let query =
                            format!("SELECT val FROM t1 WHERE val {op} '{threshold}' ORDER BY val");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        check_all_satisfy(
                            &rows,
                            0,
                            &*pred,
                            &format!("val {op} {threshold}"),
                            &format!("[{iter}] {query}"),
                        )?;
                        executed += 1;
                    }
                    // --- LIMIT + OFFSET ---
                    26 => {
                        let limit = rng.random_range(1..=20);
                        let offset = rng.random_range(0..=t1_total.saturating_sub(1));
                        let query = format!(
                            "SELECT val FROM t1 ORDER BY val ASC LIMIT {limit} OFFSET {offset}"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        check_limit(&rows, limit, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- CAST to/from custom type ---
                    27 => {
                        let val = random_numeric(&mut rng);
                        let query = format!("SELECT CAST('{val}' AS numeric(10,2))");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        if rows.len() != 1 {
                            return Err(format!(
                                "[{iter}] CAST: expected 1 row, got {}",
                                rows.len()
                            ));
                        }
                        let got = parse_numeric_value(&rows[0][0]);
                        let expected: f64 = val.parse().unwrap();
                        if let Some(g) = got {
                            if (g - expected).abs() >= 0.015 {
                                return Err(format!("[{iter}] CAST: expected {expected}, got {g}"));
                            }
                        }
                        executed += 1;
                    }
                    // --- INTERSECT ---
                    28 => {
                        let query = "SELECT val FROM t1 WHERE val IS NOT NULL \
                             INTERSECT SELECT amount FROM t2 ORDER BY 1"
                            .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        check_no_duplicates(&rows, 0, &format!("[{iter}] {query}"))?;
                        // Every result must exist in both t1 and t2
                        let t1_set: HashSet<String> =
                            t1_vals.iter().map(|v| format!("{v:.2}")).collect();
                        let t2_set: HashSet<String> =
                            t2_vals.iter().map(|v| format!("{v:.2}")).collect();
                        for (i, row) in rows.iter().enumerate() {
                            if let Some(v) = parse_numeric_value(&row[0]) {
                                let formatted = format!("{v:.2}");
                                if !t1_set.contains(&formatted) || !t2_set.contains(&formatted) {
                                    return Err(format!(
                                        "[{iter}] INTERSECT row {i}: {v} not in both tables"
                                    ));
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- EXCEPT ---
                    29 => {
                        let query = "SELECT val FROM t1 WHERE val IS NOT NULL \
                             EXCEPT SELECT amount FROM t2 ORDER BY 1"
                            .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        check_no_duplicates(&rows, 0, &format!("[{iter}] {query}"))?;
                        // No result should exist in t2
                        let t2_set: HashSet<String> =
                            t2_vals.iter().map(|v| format!("{v:.2}")).collect();
                        for (i, row) in rows.iter().enumerate() {
                            if let Some(v) = parse_numeric_value(&row[0]) {
                                let formatted = format!("{v:.2}");
                                if t2_set.contains(&formatted) {
                                    return Err(format!(
                                        "[{iter}] EXCEPT row {i}: {v} should not be in t2"
                                    ));
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- Multi-column index: ORDER BY (label, val) ---
                    30 => {
                        let query =
                            format!("SELECT label, val FROM t1 ORDER BY label {dir}, val {dir}");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        // Check sorted by (label, val) pair
                        for i in 1..rows.len() {
                            let prev_label = match &rows[i - 1][0] {
                                Value::Text(s) => s.clone(),
                                Value::Null => String::new(),
                                _ => {
                                    return Err(format!("[{iter}] multi-col sort: bad label type"))
                                }
                            };
                            let cur_label = match &rows[i][0] {
                                Value::Text(s) => s.clone(),
                                Value::Null => String::new(),
                                _ => {
                                    return Err(format!("[{iter}] multi-col sort: bad label type"))
                                }
                            };
                            let prev_val = parse_numeric_value(&rows[i - 1][1]);
                            let cur_val = parse_numeric_value(&rows[i][1]);
                            let label_cmp = if desc {
                                prev_label.cmp(&cur_label).reverse()
                            } else {
                                prev_label.cmp(&cur_label)
                            };
                            if label_cmp == std::cmp::Ordering::Greater {
                                return Err(format!(
                                    "[{iter}] multi-col sort: labels out of order: '{prev_label}' vs '{cur_label}'"
                                ));
                            }
                            if label_cmp == std::cmp::Ordering::Equal {
                                if let (Some(pv), Some(cv)) = (prev_val, cur_val) {
                                    let val_ok = if desc { pv >= cv } else { pv <= cv };
                                    if !val_ok {
                                        return Err(format!(
                                            "[{iter}] multi-col sort: vals out of order within label '{cur_label}': {pv} vs {cv}"
                                        ));
                                    }
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- Multi-column index: WHERE label = ? ORDER BY val ---
                    31 => {
                        let label = LABELS[rng.random_range(0..LABELS.len())];
                        let query = format!(
                            "SELECT val FROM t1 WHERE label = '{label}' ORDER BY val {dir}"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, desc, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- Multi-column index: WHERE label = ? AND val > threshold ---
                    32 => {
                        let label = LABELS[rng.random_range(0..LABELS.len())];
                        let threshold = random_numeric(&mut rng);
                        let t: f64 = threshold.parse().unwrap();
                        let query = format!(
                            "SELECT val FROM t1 WHERE label = '{label}' AND val > '{threshold}' ORDER BY val"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        check_all_satisfy(
                            &rows,
                            0,
                            |v| v > t,
                            &format!("val > {threshold}"),
                            &format!("[{iter}] {query}"),
                        )?;
                        executed += 1;
                    }
                    // --- Index on t4: ORDER BY val (exercises index after mutations) ---
                    33 => {
                        let query = format!("SELECT val FROM t4 ORDER BY val {dir}");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, desc, &format!("[{iter}] {query}"))?;
                        executed += 1;
                    }
                    // --- Index on t4: WHERE val > threshold (index seek after mutations) ---
                    34 => {
                        let threshold = random_numeric(&mut rng);
                        let t: f64 = threshold.parse().unwrap();
                        let query =
                            format!("SELECT val FROM t4 WHERE val > '{threshold}' ORDER BY val");
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        check_all_satisfy(
                            &rows,
                            0,
                            |v| v > t,
                            &format!("val > {threshold}"),
                            &format!("[{iter}] {query}"),
                        )?;
                        executed += 1;
                    }
                    // --- Index on t4: range query (index range scan after mutations) ---
                    35 => {
                        let a = random_numeric(&mut rng);
                        let b = random_numeric(&mut rng);
                        let (lo, hi) = if a.parse::<f64>().unwrap() <= b.parse::<f64>().unwrap() {
                            (a, b)
                        } else {
                            (b, a)
                        };
                        let query = format!(
                            "SELECT val FROM t4 WHERE val >= '{lo}' AND val <= '{hi}' ORDER BY val"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        check_sorted(&rows, 0, false, &format!("[{iter}] {query}"))?;
                        let lo_f: f64 = lo.parse().unwrap();
                        let hi_f: f64 = hi.parse().unwrap();
                        check_all_satisfy(
                            &rows,
                            0,
                            |v| v >= lo_f && v <= hi_f,
                            &format!("val in [{lo}, {hi}]"),
                            &format!("[{iter}] {query}"),
                        )?;
                        executed += 1;
                    }
                    // --- Mutate t4 then verify index consistency ---
                    36 => {
                        // Update a row, then verify index-ordered scan still correct
                        let new_val = random_numeric(&mut rng);
                        let target_id = rng.random_range(0..t4_size);
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("UPDATE t4 SET val = '{new_val}' WHERE id = {target_id}"),
                        )
                        .map_err(|_| String::new())?;
                        // Index scan must still be sorted after the update
                        let rows =
                            limbo_exec_rows_fallible(&db, &conn, "SELECT val FROM t4 ORDER BY val")
                                .map_err(|_| String::new())?;
                        check_sorted(
                            &rows,
                            0,
                            false,
                            &format!("[{iter}] t4 index after UPDATE id={target_id}"),
                        )?;
                        // Verify the updated value is findable via index seek
                        let seek = limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("SELECT val FROM t4 WHERE val = '{new_val}'"),
                        )
                        .map_err(|_| String::new())?;
                        let expected: f64 = new_val.parse().unwrap();
                        let found = seek.iter().any(|row| {
                            parse_numeric_value(&row[0])
                                .map(|v| (v - expected).abs() < 0.005)
                                .unwrap_or(false)
                        });
                        if !found {
                            return Err(format!(
                                "[{iter}] t4 index seek: updated val '{new_val}' not found via WHERE"
                            ));
                        }
                        executed += 1;
                    }
                    // --- Custom type name does not affect column affinity ---
                    // "doubled" contains "DOUB" and "charmed" contains "CHAR".
                    // Verify that typeof() returns "integer" (the BASE type),
                    // not "real" or "text" (from SQLite's name-based rules).
                    37 => {
                        let target_id = rng.random_range(0..t5_rows);
                        let query = format!(
                            "SELECT typeof(d), typeof(c), d, c FROM t5 WHERE id = {target_id}"
                        );
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        if rows.len() != 1 {
                            return Err(format!(
                                "[{iter}] t5 typeof: expected 1 row, got {}",
                                rows.len()
                            ));
                        }
                        let row = &rows[0];
                        // typeof(d) and typeof(c) must be "integer"
                        for (col_idx, col_name) in [(0, "d"), (1, "c")] {
                            match &row[col_idx] {
                                Value::Text(s) if s == "integer" => {}
                                other => {
                                    return Err(format!(
                                        "[{iter}] t5 typeof({col_name}): expected 'integer', got {other:?}"
                                    ));
                                }
                            }
                        }
                        // The decoded values must be integers (not floats)
                        for (col_idx, col_name) in [(2, "d"), (3, "c")] {
                            match &row[col_idx] {
                                Value::Integer(_) => {}
                                other => {
                                    return Err(format!(
                                        "[{iter}] t5 {col_name} value: expected integer, got {other:?}"
                                    ));
                                }
                            }
                        }
                        executed += 1;
                    }
                    // --- RETURNING must agree with SELECT ---
                    // INSERT a row with RETURNING, then SELECT the same row and
                    // verify both paths produce identical values.
                    38 => {
                        let val = random_numeric(&mut rng);
                        let next_id = t4_size + iter;
                        let ret_rows = limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!(
                                "INSERT INTO t4 VALUES ({next_id}, '{val}', 'ret') RETURNING id, val, label"
                            ),
                        )
                        .map_err(|_| String::new())?;
                        let sel_rows = limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("SELECT id, val, label FROM t4 WHERE id = {next_id}"),
                        )
                        .map_err(|_| String::new())?;
                        if ret_rows.len() != 1 || sel_rows.len() != 1 {
                            return Err(format!(
                                "[{iter}] INSERT RETURNING vs SELECT: row count mismatch ({} vs {})",
                                ret_rows.len(),
                                sel_rows.len()
                            ));
                        }
                        if ret_rows[0] != sel_rows[0] {
                            return Err(format!(
                                "[{iter}] INSERT RETURNING disagrees with SELECT: {:?} vs {:?}",
                                ret_rows[0], sel_rows[0]
                            ));
                        }
                        // Clean up
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("DELETE FROM t4 WHERE id = {next_id}"),
                        )
                        .map_err(|_| String::new())?;
                        executed += 1;
                    }
                    // --- Self-join on custom type column ---
                    // Joins t1 with itself using aliases. The optimizer builds
                    // an ephemeral auto-index for the inner copy. Previously
                    // this failed because the seek-key encoder searched by alias
                    // while the index stored the base table name.
                    39 => {
                        let query = "SELECT a.val, b.val FROM t1 a JOIN t1 b ON a.val = b.val \
                             WHERE a.val IS NOT NULL"
                            .to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        // Every row must satisfy the join condition: a.val == b.val
                        check_join_condition(&rows, 0, 1, &format!("[{iter}] {query}"))?;
                        // The result must have at least t1_rows rows (each row
                        // matches at least itself), and exactly
                        // sum(count(v)^2) rows for each distinct value v.
                        if rows.len() < t1_rows {
                            return Err(format!(
                                "[{iter}] Self-join: expected >= {t1_rows} rows, got {}. {query}",
                                rows.len()
                            ));
                        }
                        executed += 1;
                    }
                    // --- Multi-row UPDATE with constant SET value ---
                    // Updates ALL rows in t4 with a single constant. Previously
                    // the encode expression overwrote the constant register,
                    // causing each successive row to be double-encoded.
                    40 => {
                        let new_val = random_numeric(&mut rng);
                        limbo_exec_rows_fallible(
                            &db,
                            &conn,
                            &format!("UPDATE t4 SET val = '{new_val}'"),
                        )
                        .map_err(|_| String::new())?;
                        let query = "SELECT val FROM t4".to_string();
                        let rows = limbo_exec_rows_fallible(&db, &conn, &query)
                            .map_err(|_| String::new())?;
                        let expected: f64 = new_val.parse().unwrap();
                        for (i, row) in rows.iter().enumerate() {
                            let got = parse_numeric_value(&row[0]);
                            if let Some(g) = got {
                                if (g - expected).abs() >= 0.015 {
                                    return Err(format!(
                                        "[{iter}] Multi-row UPDATE: row {i} expected {expected}, got {g} (double-encode bug)"
                                    ));
                                }
                            }
                        }
                        executed += 1;
                    }
                    _ => unreachable!(),
                }
                Ok(())
            })();

            if is_mutation_pattern {
                t4_dirty = true;
            }

            match result {
                Ok(()) => {}
                Err(msg) if msg.is_empty() => skipped += 1, // SQL execution error
                Err(msg) => {
                    println!("VIOLATION: {msg}");
                    violations.push(msg);
                }
            }
        }

        println!(
            "fuzz_custom_type_invariants: done. seed={seed}, iterations={iter}, executed={executed}, skipped={skipped}, violations={}, duration={duration_secs}s",
            violations.len()
        );

        if !violations.is_empty() {
            let mut summary = format!(
                "\n=== {} INVARIANT VIOLATION(S) FOUND (seed={seed}) ===\n",
                violations.len()
            );
            // Deduplicate by pattern prefix
            let mut seen_patterns: HashSet<String> = HashSet::new();
            for v in &violations {
                // Extract pattern type (text before the first ']')
                let pattern_key = v
                    .split(']')
                    .nth(1)
                    .and_then(|s| s.split("FROM").next())
                    .unwrap_or(v)
                    .trim()
                    .to_string();
                if seen_patterns.insert(pattern_key) {
                    summary.push_str(&format!("  - {v}\n"));
                }
            }
            if violations.len() > seen_patterns.len() {
                summary.push_str(&format!(
                    "  ({} more similar violations omitted)\n",
                    violations.len() - seen_patterns.len()
                ));
            }
            panic!("{summary}");
        }
    }
}
