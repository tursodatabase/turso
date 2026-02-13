//! Fuzzer for custom type cross-feature interactions.
//!
//! Tests that custom type columns (numeric(10,2)) work correctly with:
//! ORDER BY, GROUP BY, DISTINCT, JOINs, indexes, subqueries, aggregates,
//! CTEs, UNION, CASE WHEN, NULL handling, and multi-column ordering.
//!
//! Queries that fail (unsupported features) are silently skipped, so this
//! test automatically gains coverage as Turso implements new SQL features.
//! Invariant violations are collected and reported at the end.
//!
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
                    .with_strict(true),
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

        // --- Query loop ---
        const ITERS: usize = 5000;
        const NUM_PATTERNS: usize = 18;
        let mut executed = 0u64;
        let mut skipped = 0u64;
        let mut violations: Vec<String> = Vec::new();

        for iter in 0..ITERS {
            if iter % 50 == 0 {
                println!(
                    "fuzz_custom_type_invariants: iteration {}/{ITERS} (executed: {executed}, skipped: {skipped}, violations: {})",
                    iter + 1,
                    violations.len()
                );
            }

            let pattern = rng.random_range(0..NUM_PATTERNS);
            let desc = rng.random_bool(0.5);
            let dir = if desc { "DESC" } else { "ASC" };

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
                    _ => unreachable!(),
                }
                Ok(())
            })();

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
            "fuzz_custom_type_invariants: done. seed={seed}, executed={executed}, skipped={skipped}, violations={}",
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
