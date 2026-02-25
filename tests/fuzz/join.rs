#[cfg(test)]
mod join_fuzz_tests {
    use crate::helpers;
    use core_tester::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    fn join_fuzz_inner(db: TempDatabase, add_indexes: bool, iterations: usize, rows: i64) {
        let (mut rng, seed) =
            helpers::init_fuzz_test("join_fuzz_inner (add_indexes={add_indexes})");

        let builder = helpers::builder_from_db(&db);
        let limbo_db = builder.clone().build();
        let sqlite_db = builder.clone().build();
        let limbo_conn = limbo_db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

        let schema = r#"
        CREATE TABLE t1(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t2(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t3(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t4(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);"#;

        sqlite_conn.execute_batch(schema).unwrap();
        limbo_conn.prepare_execute_batch(schema).unwrap();

        if add_indexes {
            let index_ddl = r#"
            CREATE INDEX t1_a_idx ON t1(a);
            CREATE INDEX t1_b_idx ON t1(b);
            CREATE INDEX t1_c_idx ON t1(c);
            CREATE INDEX t1_d_idx ON t1(d);

            CREATE INDEX t2_a_idx ON t2(a);
            CREATE INDEX t2_b_idx ON t2(b);
            CREATE INDEX t2_c_idx ON t2(c);
            CREATE INDEX t2_d_idx ON t2(d);

            CREATE INDEX t3_a_idx ON t3(a);
            CREATE INDEX t3_b_idx ON t3(b);
            CREATE INDEX t3_c_idx ON t3(c);
            CREATE INDEX t3_d_idx ON t3(d);

            CREATE INDEX t4_a_idx ON t4(a);
            CREATE INDEX t4_b_idx ON t4(b);
            CREATE INDEX t4_c_idx ON t4(c);
            CREATE INDEX t4_d_idx ON t4(d);
        "#;
            sqlite_conn.execute_batch(index_ddl).unwrap();
            limbo_conn.prepare_execute_batch(index_ddl).unwrap();
        }

        let tables = ["t1", "t2", "t3", "t4"];
        let mut all_inserts: Vec<String> = Vec::new();
        for (t_idx, tname) in tables.iter().enumerate() {
            for i in 0..rows {
                let id = i + 1 + (t_idx as i64) * 10_000;

                // 25% chance of NULL per column.
                let gen_val = |rng: &mut ChaCha8Rng| {
                    if rng.random_range(0..4) == 0 {
                        None
                    } else {
                        Some(rng.random_range(-10..=20))
                    }
                };
                let a = gen_val(&mut rng);
                let b = gen_val(&mut rng);
                let c = gen_val(&mut rng);
                let d = gen_val(&mut rng);

                let fmt_val = |v: Option<i32>| match v {
                    Some(x) => x.to_string(),
                    None => "NULL".to_string(),
                };

                let stmt = format!(
                    "INSERT INTO {tname}(id,a,b,c,d) VALUES ({id}, {a}, {b}, {c}, {d})",
                    a = fmt_val(a),
                    b = fmt_val(b),
                    c = fmt_val(c),
                    d = fmt_val(d),
                );

                sqlite_conn.execute(&stmt, params![]).unwrap();
                limbo_conn.execute(&stmt).unwrap();
                all_inserts.push(stmt);
            }
        }

        let _non_pk_cols = ["a", "b", "c", "d"];

        // Helper to generate the inner SELECT for a derived table or CTE body.
        let gen_inner_select = |rng: &mut ChaCha8Rng, table: &str| -> (String, Vec<&str>) {
            let kind = rng.random_range(0..4);
            match kind {
                0 => {
                    // Simple passthrough
                    (format!("SELECT * FROM {table}"), vec!["a", "b", "c", "d"])
                }
                1 => {
                    // Select specific columns with expression
                    (
                        format!("SELECT a, b, c, d, c + d AS cd FROM {table}"),
                        vec!["a", "b", "c", "d"],
                    )
                }
                2 => {
                    // With aggregate
                    (
                        format!("SELECT a, sum(b) AS sum_b, max(c) AS max_c, count(*) AS cnt FROM {table} GROUP BY a"),
                        vec!["a"],
                    )
                }
                3 => {
                    // With filter
                    (
                        format!("SELECT * FROM {table} WHERE a IS NOT NULL"),
                        vec!["a", "b", "c", "d"],
                    )
                }
                _ => unreachable!(),
            }
        };

        for iter in 0..iterations {
            if iter % (iterations / 100).max(1) == 0 {
                println!(
                    "join_fuzz_inner(add_indexes={}) iter {}/{}",
                    add_indexes,
                    iter + 1,
                    iterations
                );
            }

            let num_tables = rng.random_range(2..=4);
            let used_tables = &tables[..num_tables];

            // Decide wrapping for each table: direct (0), derived subquery (1), or CTE (2).
            // Probabilities: 50% direct, 20% derived, 30% CTE
            let wrap_kind: Vec<u8> = (0..num_tables)
                .map(|_| {
                    let r = rng.random_range(0..10);
                    if r < 5 {
                        0 // direct
                    } else if r < 7 {
                        1 // derived subquery
                    } else {
                        2 // CTE
                    }
                })
                .collect();

            // Generate table references and collect CTE definitions
            // (from_expr, alias, joinable_cols, is_direct)
            let mut table_refs: Vec<(String, String, Vec<&str>, bool)> = Vec::new();
            let mut cte_defs: Vec<String> = Vec::new();

            for (i, &tname) in used_tables.iter().enumerate() {
                match wrap_kind[i] {
                    1 => {
                        // Derived subquery: (SELECT ... FROM t) AS sub_t
                        let alias = format!("sub_{tname}");
                        let (inner, cols) = gen_inner_select(&mut rng, tname);
                        table_refs.push((format!("({inner}) AS {alias}"), alias, cols, false));
                    }
                    2 => {
                        // CTE: WITH cte_t AS [MATERIALIZED | NOT MATERIALIZED] (SELECT ...)
                        let cte_name = format!("cte_{tname}");
                        let (inner, cols) = gen_inner_select(&mut rng, tname);
                        let mat_hint = match rng.random_range(0..3) {
                            0 => " MATERIALIZED",
                            1 => " NOT MATERIALIZED",
                            _ => "",
                        };
                        cte_defs.push(format!("{cte_name} AS{mat_hint} ({inner})"));
                        table_refs.push((cte_name.clone(), cte_name, cols, false));
                    }
                    _ => {
                        // Direct table reference
                        table_refs.push((
                            tname.to_string(),
                            tname.to_string(),
                            vec!["a", "b", "c", "d"],
                            true,
                        ));
                    }
                }
            }

            let mut select_cols: Vec<String> = Vec::new();
            for (_, alias, cols, is_direct) in table_refs.iter() {
                if *is_direct {
                    select_cols.push(format!("{alias}.id"));
                } else if cols.len() == 1 {
                    // Aggregate CTE/derived — only 'a' available
                    select_cols.push(format!("{alias}.a"));
                } else {
                    select_cols.push(format!("{alias}.a"));
                }
            }
            let select_clause = select_cols.join(", ");

            let mut from_clause = format!("FROM {}", table_refs[0].0);
            for i in 1..num_tables {
                let (_, left_alias, left_cols, _) = &table_refs[i - 1];
                let (right_expr, right_alias, right_cols, right_direct) = &table_refs[i];

                let join_type = if rng.random_bool(0.5) {
                    "JOIN"
                } else {
                    "LEFT JOIN"
                };

                // Find common joinable columns between left and right
                let common_cols: Vec<&str> = left_cols
                    .iter()
                    .filter(|c| right_cols.contains(c))
                    .copied()
                    .collect();

                // If no common columns (e.g., both are aggregates with only 'a'), use 'a'
                let join_cols = if common_cols.is_empty() {
                    vec!["a"]
                } else {
                    common_cols
                };

                let num_preds = rng.random_range(1..=join_cols.len().min(3));
                let mut preds = Vec::new();
                for _ in 0..num_preds {
                    let col = join_cols[rng.random_range(0..join_cols.len())];
                    preds.push(format!("{left_alias}.{col} = {right_alias}.{col}"));
                }
                preds.sort();
                preds.dedup();

                // 30% chance: join predicates with OR instead of AND.
                // Only when indexes exist AND the right side is a direct table
                // (not a derived/CTE), so the multi-index OR scan can use
                // the indexes. Otherwise OR creates pathological full scans.
                let use_or =
                    add_indexes && *right_direct && preds.len() >= 2 && rng.random_bool(0.3);
                let joiner = if use_or { " OR " } else { " AND " };
                let on_clause = preds.join(joiner);
                // 20% chance: wrap multi-predicate ON clause in parentheses
                let on_clause = if preds.len() >= 2 && rng.random_bool(0.2) {
                    format!("({on_clause})")
                } else {
                    on_clause
                };
                from_clause = format!("{from_clause} {join_type} {right_expr} ON {on_clause}");
            }

            // WHERE clause: 0..2 predicates on columns available in each table ref
            let mut where_parts = Vec::new();
            let num_where = rng.random_range(0..=2);
            for _ in 0..num_where {
                let idx = rng.random_range(0..num_tables);
                let (_, alias, cols, _) = &table_refs[idx];
                if cols.is_empty() {
                    continue;
                }
                let col = cols[rng.random_range(0..cols.len())];
                // EXISTS/NOT EXISTS only when indexes exist — without indexes,
                // non-unnested correlated subqueries cause pathological O(n²) scans.
                let max_kind = if add_indexes { 6 } else { 4 };
                let kind = rng.random_range(0..max_kind);
                let cond = match kind {
                    0 => {
                        let val = rng.random_range(-10..=20);
                        format!("{alias}.{col} = {val}")
                    }
                    1 => {
                        let val = rng.random_range(-10..=20);
                        format!("{alias}.{col} <> {val}")
                    }
                    2 => format!("{alias}.{col} IS NULL"),
                    3 => format!("{alias}.{col} IS NOT NULL"),
                    4 | 5 => {
                        // EXISTS / NOT EXISTS correlated subquery
                        let not = if kind == 5 { "NOT " } else { "" };
                        let target_table = tables[rng.random_range(0..tables.len())];
                        let sub_col = ["a", "b", "c", "d"][rng.random_range(0..4)];
                        let extra = if rng.random_bool(0.3) {
                            let val = rng.random_range(-10..=20);
                            format!(" AND {target_table}.{sub_col} > {val}")
                        } else {
                            String::new()
                        };
                        format!(
                            "{not}EXISTS (SELECT 1 FROM {target_table} WHERE {target_table}.{sub_col} = {alias}.{col}{extra})"
                        )
                    }
                    _ => unreachable!(),
                };
                where_parts.push(cond);
            }
            let where_clause = if where_parts.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", where_parts.join(" AND "))
            };
            let order_clause = format!("ORDER BY {}", select_cols.join(", "));
            let limit = 50;

            let with_clause = if cte_defs.is_empty() {
                String::new()
            } else {
                format!("WITH {} ", cte_defs.join(", "))
            };

            let query = format!(
                "{with_clause}SELECT {select_clause} {from_clause} {where_clause} {order_clause} LIMIT {limit}",
            );
            // Print some sample queries to verify generation
            if iter < 10 {
                println!("query[{iter}]: {query}");
            }
            let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
            let limbo_rows = limbo_exec_rows(&limbo_conn, &query);
            if sqlite_rows != limbo_rows {
                // Print DDL and DML for reproduction
                eprintln!("\n=== REPRODUCTION DDL/DML ===");
                eprintln!("{schema}");
                for ins in &all_inserts {
                    eprintln!("{ins};");
                }
                eprintln!("\n=== FAILING QUERY ===");
                eprintln!("{query}");
                eprintln!("=== END ===\n");

                panic!(
                "JOIN FUZZ MISMATCH (add_indexes={})\nseed: {}\niteration: {}\nquery: {}\n\
                 sqlite ({} rows): {:?}\nlimbo ({} rows): {:?}\nsqlite path: {:?}\nlimbo path: {:?}",
                add_indexes,
                seed,
                iter,
                query,
                sqlite_rows.len(),
                sqlite_rows,
                limbo_rows.len(),
                limbo_rows,
                sqlite_db.path,
                limbo_db.path,
            );
            }
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_unindexed_keys(db: TempDatabase) {
        join_fuzz_inner(db, false, helpers::fuzz_iterations(2000), 200);
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_indexed_keys(db: TempDatabase) {
        join_fuzz_inner(db, true, helpers::fuzz_iterations(2000), 200);
    }
}
