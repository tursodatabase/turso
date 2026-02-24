//! Fuzz tests for the join optimizer's GOO (Greedy Operator Ordering) algorithm.

use rand::seq::SliceRandom;
use rand::Rng;

use super::helpers;
use core_tester::common::{limbo_exec_rows, rng_from_time_or_env, TempDatabase};
use rusqlite::types::Value;

/// Generate a star schema with N dimensions.
/// Star schema = a fact table with N dimensions, each dimension table references the fact table.
/// Simple example:
/// CREATE TABLE t1(a01, a02, a03)
/// CREATE TABLE x01(b01 PRIMARY KEY, c01)
/// CREATE TABLE x02(b02 PRIMARY KEY, c02)
/// CREATE TABLE x03(b03 PRIMARY KEY, c03)
/// SELECT * FROM t1 JOIN x01 ON t1.a01 = x01.b01 JOIN x02 ON t1.a02 = x02.b02 JOIN x03 ON t1.a03 = x03.b03
fn generate_star_schema(num_dimensions: usize) -> (Vec<String>, String, Vec<String>) {
    let mut statements = Vec::new();

    let fact_columns: Vec<String> = (1..=num_dimensions).map(|i| format!("a{i:02}")).collect();
    statements.push(format!("CREATE TABLE t1({})", fact_columns.join(", ")));

    let mut dimension_names = Vec::new();
    for i in 1..=num_dimensions {
        let table_name = format!("x{i:02}");
        statements.push(format!(
            "CREATE TABLE {table_name}(b{i:02} PRIMARY KEY, c{i:02})"
        ));
        dimension_names.push(table_name);
    }

    (statements, "t1".to_string(), dimension_names)
}

/// Generate a star query with randomized FROM clause order.
/// Regardless of the user-provided join order, the optimizer should always place the fact table first.
fn generate_star_query_randomized<R: Rng>(
    rng: &mut R,
    fact_table: &str,
    dimension_tables: &[String],
) -> String {
    let mut all_tables: Vec<&str> = vec![fact_table];
    all_tables.extend(dimension_tables.iter().map(|s| s.as_str()));
    all_tables.shuffle(rng);

    let from_clause = all_tables.join(", ");

    let join_conditions: Vec<String> = (1..=dimension_tables.len())
        .map(|i| format!("t1.a{i:02} = x{i:02}.b{i:02}"))
        .collect();
    let where_clause = join_conditions.join(" AND ");

    let select_cols: Vec<String> = (1..=dimension_tables.len())
        .map(|i| format!("c{i:02}"))
        .collect();

    format!(
        "SELECT {} FROM {} WHERE {}",
        select_cols.join(", "),
        from_clause,
        where_clause
    )
}

/// Check if the EXPLAIN QUERY PLAN contains a SCAN on the given table.
fn has_scan_on_table(eqp_rows: &[Vec<rusqlite::types::Value>], table_name: &str) -> bool {
    for row in eqp_rows {
        assert!(row.len() >= 4);
        if let rusqlite::types::Value::Text(detail) = &row[3] {
            if detail.contains(&format!("SCAN {table_name}")) {
                return true;
            }
        }
    }
    false
}

/// Check if the EXPLAIN QUERY PLAN contains an INDEX SEARCH on the given table.
fn has_index_search_on_table(eqp_rows: &[Vec<rusqlite::types::Value>], table_name: &str) -> bool {
    for row in eqp_rows {
        assert!(row.len() >= 4);
        if let rusqlite::types::Value::Text(detail) = &row[3] {
            if detail.contains(&format!("SEARCH {table_name} USING INDEX")) {
                return true;
            }
        }
    }
    false
}

/// Fuzz test: star schema with randomized FROM clause order.
/// Verifies the optimizer always SCANs the fact table regardless of table ordering,
/// and index scans the dimension tables. Both the DP and greedy algos should be able
/// to find this kind of plan.
#[test]
fn test_star_schema_fuzz() {
    let (mut rng, seed) = rng_from_time_or_env();
    println!("seed: {seed}");

    // Test sizes including greedy threshold (12) and up to 62 dimensions (63 total tables)
    let test_sizes = [2, 4, 8, 12, 16, 32, 62];

    for &num_dimensions in &test_sizes {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();

        let (create_stmts, fact_table, dimension_tables) = generate_star_schema(num_dimensions);
        for stmt in &create_stmts {
            limbo_exec_rows(&conn, stmt);
        }

        for i in 1..=num_dimensions {
            limbo_exec_rows(
                &conn,
                &format!("INSERT INTO x{i:02} VALUES ({i}, 'dim{i}')"),
            );
        }

        let fact_values: Vec<String> = (1..=num_dimensions).map(|i| i.to_string()).collect();
        limbo_exec_rows(
            &conn,
            &format!("INSERT INTO t1 VALUES ({})", fact_values.join(", ")),
        );

        for _ in 0..helpers::fuzz_iterations(24) {
            let query = generate_star_query_randomized(&mut rng, &fact_table, &dimension_tables);
            let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));

            assert!(
                has_scan_on_table(&eqp_rows, "t1"),
                "Expected SCAN on fact table t1 for {num_dimensions}-way join. Seed: {seed}"
            );
            for dim_num in 1..=num_dimensions {
                let dim_tbl_name = format!("x{dim_num:02}");
                assert!(
                    has_index_search_on_table(&eqp_rows, &dim_tbl_name),
                    "Expected INDEX SEARCH on dimension table {dim_tbl_name} for {num_dimensions}-way join. Seed: {seed}"
                );
            }

            let result = limbo_exec_rows(&conn, &query);
            assert_eq!(result.len(), 1, "Expected 1 row from star query");
        }
    }
}

/// Fuzz test: chain join pattern (t1 -> t2 -> t3 -> ... -> tN).
/// We don't assert on getting a great plan out of this, but we do assert on the query completing and producing a result,
/// i.e. that for example a 62-way-join is able to be planned and executed.
#[test]
fn test_chain_join_fuzz() {
    let (mut rng, seed) = rng_from_time_or_env();
    println!("seed: {seed}");

    let test_sizes = [2, 4, 8, 12, 16, 32, 62];

    for &chain_length in &test_sizes {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();

        // Create chain: each table has (join_col, next_join_col) using numeric column names
        let mut tables = Vec::new();
        for i in 1..=chain_length {
            let table_name = format!("t{i}");
            if i == chain_length {
                limbo_exec_rows(&conn, &format!("CREATE TABLE {table_name}(c{i}, data)"));
            } else {
                limbo_exec_rows(
                    &conn,
                    &format!("CREATE TABLE {table_name}(c{i}, c{})", i + 1),
                );
            }
            tables.push(table_name);
        }

        // Create indexes on the chain
        for i in 1..=chain_length {
            limbo_exec_rows(&conn, &format!("CREATE INDEX idx_t{i} ON t{i}(c{i})"));
        }

        // Insert data forming a chain
        for i in 1..=chain_length {
            if i == chain_length {
                limbo_exec_rows(&conn, &format!("INSERT INTO t{i} VALUES (1, 'end')"));
            } else {
                limbo_exec_rows(&conn, &format!("INSERT INTO t{i} VALUES (1, 1)"));
            }
        }

        // Join conditions: t1.c2 = t2.c2 AND t2.c3 = t3.c3 AND ...
        let join_conditions: Vec<String> = (1..chain_length)
            .map(|i| format!("t{i}.c{} = t{}.c{}", i + 1, i + 1, i + 1))
            .collect();

        let mut table_refs: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();
        table_refs.shuffle(&mut rng);

        // For table counts less than 12 (greedy threshold), we expect to find a plan where there is exactly one SCAN and n-1 INDEX SEARCHes,
        // because we are using the DP algorithm that should be always able to find such a plan in the presence of indexes.
        if chain_length < 12 {
            let eqp_rows = limbo_exec_rows(
                &conn,
                &format!(
                    "EXPLAIN QUERY PLAN SELECT * FROM {} WHERE {}",
                    table_refs.join(", "),
                    join_conditions.join(" AND ")
                ),
            );
            let scans = eqp_rows
                .iter()
                .filter(|row| {
                    let Value::Text(detail) = &row[3] else {
                        panic!("Expected TEXT value for detail in EXPLAIN QUERY PLAN");
                    };
                    detail.contains("SCAN")
                })
                .count();
            let index_searches = eqp_rows
                .iter()
                .filter(|row| {
                    let Value::Text(detail) = &row[3] else {
                        panic!("Expected TEXT value for detail in EXPLAIN QUERY PLAN");
                    };
                    detail.contains("SEARCH") && detail.contains("USING INDEX")
                })
                .count();
            assert_eq!(scans, 1, "Expected 1 SCAN in EXPLAIN QUERY PLAN for a {chain_length}-way chain join, got {scans}");
            assert_eq!(index_searches, chain_length - 1, "Expected {} INDEX SEARCHes in EXPLAIN QUERY PLAN for a {chain_length}-way chain join, got {index_searches}", chain_length - 1);
        }

        let query = format!(
            "SELECT t{chain_length}.data FROM {} WHERE {}",
            table_refs.join(", "),
            join_conditions.join(" AND ")
        );

        // Primary assertion: query completes and produces correct result
        let result = limbo_exec_rows(&conn, &query);
        assert_eq!(
            result.len(),
            1,
            "Expected 1 row from {chain_length}-way chain join"
        );
    }
}

/// Test: clique join where every table joins to every other.
/// Just verify that the plan can be constructed in a reasonable time and executed with a valid result.
#[test]
fn test_clique_join() {
    let (mut rng, seed) = rng_from_time_or_env();
    println!("seed: {seed}");

    // Clique has O(n^2) join conditions, test multiple sizes up to 62
    for clique_size in [2, 4, 8, 12, 16, 32, 62] {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();

        let mut tables = Vec::new();
        for i in 1..=clique_size {
            limbo_exec_rows(&conn, &format!("CREATE TABLE t{i}(key, data)"));
            limbo_exec_rows(&conn, &format!("INSERT INTO t{i} VALUES (1, 'v{i}')"));
            tables.push(format!("t{i}"));
        }

        // Create indexes on the clique
        for i in 1..=clique_size {
            limbo_exec_rows(&conn, &format!("CREATE INDEX idx_t{i} ON t{i}(key)"));
        }

        // Every pair of tables joined on key
        let mut conditions = Vec::new();
        for i in 1..clique_size {
            for j in (i + 1)..=clique_size {
                conditions.push(format!("t{i}.key = t{j}.key"));
            }
        }

        let mut table_refs: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();
        table_refs.shuffle(&mut rng);

        let query = format!(
            "SELECT t1.data FROM {} WHERE {}",
            table_refs.join(", "),
            conditions.join(" AND ")
        );

        let result = limbo_exec_rows(&conn, &query);
        assert_eq!(
            result.len(),
            1,
            "Expected 1 row from {clique_size}-way clique join"
        );
    }
}

/// Extract table access order from EXPLAIN QUERY PLAN.
/// Returns table names in the order they appear in the plan (SCAN/SEARCH lines).
fn extract_table_order(eqp_rows: &[Vec<Value>]) -> Vec<String> {
    let mut tables = Vec::new();
    for row in eqp_rows {
        if let Value::Text(detail) = &row[3] {
            // Match "SCAN t1" or "SEARCH t1 USING ..."
            if let Some(rest) = detail.strip_prefix("SCAN ") {
                let table = rest.split_whitespace().next().unwrap();
                tables.push(table.to_string());
            } else if let Some(rest) = detail.strip_prefix("SEARCH ") {
                let table = rest.split_whitespace().next().unwrap();
                tables.push(table.to_string());
            }
        }
    }
    tables
}

/// Fuzz test: LEFT JOIN ordering invariant.
/// The RHS of a LEFT JOIN must never be reordered before its LHS in the query plan.
/// We generate random chains of LEFT JOINs and verify the plan respects this.
#[test]
fn test_left_join_ordering() {
    let (mut rng, seed) = rng_from_time_or_env();
    println!("seed: {seed}");

    for num_tables in [4, 8, 12, 16, 32] {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();

        // Create tables: t1 -> t2 -> t3 -> ... (LEFT JOIN chain)
        for i in 1..=num_tables {
            if i == 1 {
                limbo_exec_rows(&conn, &format!("CREATE TABLE t{i}(id PRIMARY KEY, val)"));
            } else {
                limbo_exec_rows(
                    &conn,
                    &format!("CREATE TABLE t{i}(id PRIMARY KEY, fk, val)"),
                );
                limbo_exec_rows(&conn, &format!("CREATE INDEX idx_t{i}_fk ON t{i}(fk)"));
            }
        }

        // Insert data: t1 has 2 rows, others have 1 row matching only t1.id=1
        limbo_exec_rows(&conn, "INSERT INTO t1 VALUES (1, 'v1'), (2, 'v2')");
        for i in 2..=num_tables {
            limbo_exec_rows(&conn, &format!("INSERT INTO t{i} VALUES (1, 1, 'v{i}')"));
        }

        // Build LEFT JOIN chain: t1 LEFT JOIN t2 ON t1.id = t2.fk LEFT JOIN t3 ON t2.id = t3.fk ...
        let joins: Vec<String> = (2..=num_tables)
            .map(|i| format!("LEFT JOIN t{i} ON t{}.id = t{i}.fk", i - 1))
            .collect();

        let query = format!("SELECT t1.val FROM t1 {}", joins.join(" "));
        let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
        let plan_order = extract_table_order(&eqp_rows);

        // Invariant: for each LEFT JOIN (ti-1 LEFT JOIN ti), ti-1 must appear before ti in plan.
        // Since it's a chain, this means t1 < t2 < t3 < ... in plan order.
        for i in 2..=num_tables {
            let lhs = format!("t{}", i - 1);
            let rhs = format!("t{i}");
            let lhs_pos = plan_order.iter().position(|t| t == &lhs);
            let rhs_pos = plan_order.iter().position(|t| t == &rhs);
            assert!(
                lhs_pos < rhs_pos,
                "LEFT JOIN invariant violated: {lhs} must come before {rhs} in plan.\n\
                 Plan order: {plan_order:?}\nSeed: {seed}"
            );
        }

        // Verify correctness: 2 rows, second row has NULLs for all joined tables
        let result = limbo_exec_rows(&conn, &query);
        assert_eq!(
            result.len(),
            2,
            "Expected 2 rows for {num_tables}-table LEFT JOIN chain"
        );
    }

    // Also test with randomized INNER JOINs interspersed
    for num_tables in [8, 16, 32] {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();

        for i in 1..=num_tables {
            if i == 1 {
                limbo_exec_rows(&conn, &format!("CREATE TABLE t{i}(id PRIMARY KEY, val)"));
            } else {
                limbo_exec_rows(
                    &conn,
                    &format!("CREATE TABLE t{i}(id PRIMARY KEY, fk, val)"),
                );
                limbo_exec_rows(&conn, &format!("CREATE INDEX idx_t{i}_fk ON t{i}(fk)"));
            }
        }

        limbo_exec_rows(&conn, "INSERT INTO t1 VALUES (1, 'v1')");
        for i in 2..=num_tables {
            limbo_exec_rows(&conn, &format!("INSERT INTO t{i} VALUES (1, 1, 'v{i}')"));
        }

        // Randomly choose LEFT or INNER join for each position.
        // In this chain pattern (each table references only its predecessor),
        // a LEFT JOIN at position i is simplified to INNER when any subsequent
        // position j > i is an INNER JOIN (the null-rejection cascades backwards).
        // Only LEFT JOINs where ALL subsequent positions are also LEFT JOINs survive.
        // So we generate join types, then compute which LEFT JOINs survive.
        let mut left_join_positions = Vec::new();
        let joins: Vec<String> = (2..=num_tables)
            .map(|i| {
                // Bias towards LEFT JOIN at the end so some survive simplification
                let is_left = if i > num_tables - 3 {
                    true
                } else {
                    rng.random_bool(0.5)
                };
                if is_left {
                    left_join_positions.push(i);
                }
                let join_type = if is_left { "LEFT JOIN" } else { "JOIN" };
                format!("{join_type} t{i} ON t{}.id = t{i}.fk", i - 1)
            })
            .collect();

        let query = format!("SELECT t1.val FROM t1 {}", joins.join(" "));
        let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
        let plan_order = extract_table_order(&eqp_rows);

        // A LEFT JOIN at position i in this chain pattern gets simplified to INNER
        // when any subsequent position j > i is an INNER JOIN, because the null-rejection
        // cascades backwards. Only LEFT JOINs where ALL subsequent positions are also
        // LEFT JOINs survive simplification and must preserve LHS-before-RHS ordering.
        let left_set: std::collections::HashSet<usize> =
            left_join_positions.iter().copied().collect();
        let surviving_left_joins: Vec<usize> = left_join_positions
            .iter()
            .copied()
            .filter(|&i| (i + 1..=num_tables).all(|j| left_set.contains(&j)))
            .collect();

        for &i in &surviving_left_joins {
            let lhs = format!("t{}", i - 1);
            let rhs = format!("t{i}");
            let lhs_pos = plan_order.iter().position(|t| t == &lhs);
            let rhs_pos = plan_order.iter().position(|t| t == &rhs);
            assert!(
                lhs_pos < rhs_pos,
                "LEFT JOIN invariant violated: {lhs} must come before {rhs} in plan.\n\
                 Plan order: {plan_order:?}\nSurviving left joins: {surviving_left_joins:?}\n\
                 All left joins: {left_join_positions:?}\nSeed: {seed}"
            );
        }

        let result = limbo_exec_rows(&conn, &query);
        assert_eq!(result.len(), 1);
    }
}
