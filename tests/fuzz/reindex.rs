#[cfg(test)]
mod reindex_fuzz_tests {
    use crate::helpers;
    use core_tester::common::TempDatabase;
    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;
    use std::sync::Arc;

    const COLLATIONS: &[&str] = &["BINARY", "NOCASE", "RTRIM"];
    const DIRECTIONS: &[&str] = &["ASC", "DESC"];

    #[derive(Clone, Copy)]
    struct ExprSpec {
        sql: &'static str,
        where_sql: &'static str,
        order_sql: &'static str,
    }

    const EXPRESSIONS: &[ExprSpec] = &[
        ExprSpec {
            sql: "abs(e)",
            where_sql: "abs(e) IS NOT NULL",
            order_sql: "abs(e), id DESC",
        },
        ExprSpec {
            sql: "coalesce(a, 0) + coalesce(b, 0)",
            where_sql: "coalesce(a, 0) + coalesce(b, 0) IS NOT NULL",
            order_sql: "coalesce(a, 0) + coalesce(b, 0), id DESC",
        },
        ExprSpec {
            sql: "length(c)",
            where_sql: "length(c) IS NOT NULL",
            order_sql: "length(c), id DESC",
        },
        ExprSpec {
            sql: "lower(c)",
            where_sql: "lower(c) IS NOT NULL",
            order_sql: "lower(c), id DESC",
        },
        ExprSpec {
            sql: "substr(d, 1, 1)",
            where_sql: "substr(d, 1, 1) IS NOT NULL",
            order_sql: "substr(d, 1, 1), id DESC",
        },
    ];

    const PARTIAL_PREDICATES: &[&str] = &[
        "b IS NOT NULL AND a IS NOT NULL",
        "c IS NOT NULL",
        "e BETWEEN -10 AND 10",
        "d IS NOT NULL AND b >= 0",
    ];

    struct Scenario {
        name: String,
        schema: Vec<String>,
        reindex_targets: Vec<String>,
        verify_queries: Vec<(String, String)>,
        a_order: String,
        b_order: String,
        c_collation: String,
        c_order: String,
        d_collation: String,
        d_order: String,
        expr: ExprSpec,
        partial_predicate: String,
    }

    fn build_scenario(rng: &mut ChaCha8Rng, scenario_num: usize) -> Scenario {
        let implicit_c_collation = choose_str(rng, COLLATIONS);
        let implicit_d_collation = choose_str(rng, COLLATIONS);
        let c_collation = choose_str(rng, COLLATIONS);
        let d_collation = choose_str(rng, COLLATIONS);
        let a_order = choose_str(rng, DIRECTIONS);
        let b_order = choose_str(rng, DIRECTIONS);
        let c_order = choose_str(rng, DIRECTIONS);
        let d_order = choose_str(rng, DIRECTIONS);
        let expr = *EXPRESSIONS.choose(rng).unwrap();
        let partial_predicate = choose_str(rng, PARTIAL_PREDICATES);

        let schema = vec![
            format!(
                "CREATE TABLE t(
                    id INTEGER PRIMARY KEY,
                    a INTEGER,
                    b INTEGER,
                    c TEXT COLLATE {implicit_c_collation},
                    d TEXT COLLATE {implicit_d_collation},
                    e INTEGER
                )"
            ),
            format!("CREATE INDEX idx_a ON t(a {a_order}, id ASC)"),
            format!("CREATE INDEX idx_b ON t(b {b_order}, id ASC)"),
            format!("CREATE INDEX idx_c_text ON t(c COLLATE {c_collation} {c_order}, id ASC)"),
            format!("CREATE INDEX idx_d_text ON t(d COLLATE {d_collation} {d_order}, id ASC)"),
            format!("CREATE INDEX idx_expr ON t({}, id DESC)", expr.sql),
            format!(
                "CREATE INDEX idx_partial ON t(a {a_order}, c COLLATE {c_collation}, id) WHERE {partial_predicate}"
            ),
            format!("CREATE UNIQUE INDEX idx_unique_c_id ON t(c COLLATE {c_collation}, id)"),
        ];

        let verify_queries = vec![
            (
                "table".to_string(),
                "SELECT id, a, b, c, quote(d), e FROM t ORDER BY id".to_string(),
            ),
            (
                "idx_a".to_string(),
                format!(
                    "SELECT id, a FROM t INDEXED BY idx_a
                        WHERE a IS NOT NULL
                        ORDER BY a {a_order}, id
                        LIMIT 40"
                ),
            ),
            (
                "idx_b".to_string(),
                format!(
                    "SELECT id, b FROM t INDEXED BY idx_b
                        WHERE b IS NOT NULL
                        ORDER BY b {b_order}, id
                        LIMIT 40"
                ),
            ),
            (
                "idx_c_text".to_string(),
                format!(
                    "SELECT id, c FROM t INDEXED BY idx_c_text
                        WHERE c COLLATE {c_collation} >= ''
                        ORDER BY c COLLATE {c_collation} {c_order}, id
                        LIMIT 40"
                ),
            ),
            (
                "idx_d_text".to_string(),
                format!(
                    "SELECT id, quote(d) FROM t INDEXED BY idx_d_text
                        WHERE d COLLATE {d_collation} >= ''
                        ORDER BY d COLLATE {d_collation} {d_order}, id
                        LIMIT 40"
                ),
            ),
            (
                "idx_expr".to_string(),
                format!(
                    "SELECT id, a, b, c, quote(d), e FROM t INDEXED BY idx_expr
                        WHERE {}
                        ORDER BY {}
                        LIMIT 40",
                    expr.where_sql, expr.order_sql
                ),
            ),
            (
                "idx_partial".to_string(),
                format!(
                    "SELECT id, a, c FROM t INDEXED BY idx_partial
                        WHERE {partial_predicate}
                        ORDER BY a {a_order}, c COLLATE {c_collation}, id
                        LIMIT 40"
                ),
            ),
            (
                "idx_unique_c_id".to_string(),
                format!(
                    "SELECT id, c FROM t INDEXED BY idx_unique_c_id
                        WHERE c IS NOT NULL
                        ORDER BY c COLLATE {c_collation}, id
                        LIMIT 40"
                ),
            ),
        ];

        Scenario {
            name: format!(
                "scenario {scenario_num}: c={implicit_c_collation}/{c_collation} {c_order}, d={implicit_d_collation}/{d_collation} {d_order}, expr={}",
                expr.sql
            ),
            schema,
            reindex_targets: vec![
                "REINDEX".to_string(),
                "REINDEX t".to_string(),
                "REINDEX main.t".to_string(),
                "REINDEX idx_a".to_string(),
                "REINDEX main.idx_a".to_string(),
                "REINDEX idx_b".to_string(),
                "REINDEX idx_c_text".to_string(),
                "REINDEX idx_d_text".to_string(),
                "REINDEX idx_expr".to_string(),
                "REINDEX idx_partial".to_string(),
                "REINDEX idx_unique_c_id".to_string(),
                format!("REINDEX {c_collation}"),
                format!("REINDEX {d_collation}"),
                "REINDEX BINARY".to_string(),
                "REINDEX NOCASE".to_string(),
                "REINDEX RTRIM".to_string(),
            ],
            verify_queries,
            a_order: a_order.to_string(),
            b_order: b_order.to_string(),
            c_collation: c_collation.to_string(),
            c_order: c_order.to_string(),
            d_collation: d_collation.to_string(),
            d_order: d_order.to_string(),
            expr,
            partial_predicate: partial_predicate.to_string(),
        }
    }

    fn choose_str(rng: &mut ChaCha8Rng, values: &'static [&'static str]) -> &'static str {
        values.choose(rng).unwrap()
    }

    fn int_or_null(rng: &mut ChaCha8Rng) -> String {
        helpers::random_nullable_int(rng, -30..=30, 0.18)
    }

    fn text_or_null(rng: &mut ChaCha8Rng, allow_trailing_spaces: bool) -> String {
        if rng.random_bool(0.18) {
            return "NULL".to_string();
        }

        let mut text = helpers::generate_random_text(rng, 8);
        if allow_trailing_spaces {
            for _ in 0..rng.random_range(0..=3) {
                text.push(' ');
            }
        }
        format!("'{text}'")
    }

    fn text_literal(rng: &mut ChaCha8Rng) -> String {
        format!("'{}'", helpers::generate_random_text(rng, 4))
    }

    fn insert_stmt(rng: &mut ChaCha8Rng, next_id: &mut i64) -> String {
        let id = *next_id;
        *next_id += 1;
        format!(
            "INSERT INTO t(id, a, b, c, d, e) VALUES ({}, {}, {}, {}, {}, {})",
            id,
            int_or_null(rng),
            int_or_null(rng),
            text_or_null(rng, false),
            text_or_null(rng, true),
            int_or_null(rng)
        )
    }

    fn insert_or_replace_stmt(rng: &mut ChaCha8Rng, max_id: i64) -> String {
        let id = rng.random_range(1..=max_id.max(1));
        format!(
            "INSERT OR REPLACE INTO t(id, a, b, c, d, e) VALUES ({}, {}, {}, {}, {}, {})",
            id,
            int_or_null(rng),
            int_or_null(rng),
            text_or_null(rng, false),
            text_or_null(rng, true),
            int_or_null(rng)
        )
    }

    fn update_stmt(rng: &mut ChaCha8Rng, max_id: i64) -> String {
        let column = *["a", "b", "c", "d", "e"].choose(rng).unwrap();
        let value = match column {
            "c" => text_or_null(rng, false),
            "d" => text_or_null(rng, true),
            _ => int_or_null(rng),
        };
        format!(
            "UPDATE t SET {column} = {value} {}",
            where_clause(rng, max_id)
        )
    }

    fn delete_stmt(rng: &mut ChaCha8Rng, max_id: i64) -> String {
        format!("DELETE FROM t {}", where_clause(rng, max_id))
    }

    fn where_clause(rng: &mut ChaCha8Rng, max_id: i64) -> String {
        match rng.random_range(0..6) {
            0 => format!("WHERE id = {}", rng.random_range(1..=max_id.max(1))),
            1 => format!("WHERE id <= {}", rng.random_range(1..=max_id.max(1))),
            2 => format!("WHERE a = {}", rng.random_range(-30..=30)),
            3 => "WHERE b IS NOT NULL".to_string(),
            4 => "WHERE c IS NOT NULL".to_string(),
            5 => "WHERE d IS NULL OR e IS NULL".to_string(),
            _ => unreachable!(),
        }
    }

    fn random_probe_query(rng: &mut ChaCha8Rng, scenario: &Scenario) -> (String, String) {
        let limit = rng.random_range(1..=25);
        match rng.random_range(0..7) {
            0 => (
                "probe_idx_a".to_string(),
                format!(
                    "SELECT id, a, b FROM t INDEXED BY idx_a
                        WHERE a {} {}
                        ORDER BY a {}, id
                        LIMIT {limit}",
                    choose_str(rng, &["=", "<=", ">="]),
                    rng.random_range(-30..=30),
                    scenario.a_order
                ),
            ),
            1 => (
                "probe_idx_b".to_string(),
                format!(
                    "SELECT id, a, b FROM t INDEXED BY idx_b
                        WHERE b {} {}
                        ORDER BY b {}, id
                        LIMIT {limit}",
                    choose_str(rng, &["=", "<=", ">="]),
                    rng.random_range(-30..=30),
                    scenario.b_order
                ),
            ),
            2 => (
                "probe_idx_c_text".to_string(),
                format!(
                    "SELECT id, c FROM t INDEXED BY idx_c_text
                        WHERE c COLLATE {} >= {}
                        ORDER BY c COLLATE {} {}, id
                        LIMIT {limit}",
                    scenario.c_collation,
                    text_literal(rng),
                    scenario.c_collation,
                    scenario.c_order
                ),
            ),
            3 => (
                "probe_idx_d_text".to_string(),
                format!(
                    "SELECT id, quote(d) FROM t INDEXED BY idx_d_text
                        WHERE d COLLATE {} <= {}
                        ORDER BY d COLLATE {} {}, id
                        LIMIT {limit}",
                    scenario.d_collation,
                    text_literal(rng),
                    scenario.d_collation,
                    scenario.d_order
                ),
            ),
            4 => (
                "probe_idx_expr".to_string(),
                format!(
                    "SELECT id, a, b, c, quote(d), e FROM t INDEXED BY idx_expr
                        WHERE {}
                        ORDER BY {}
                        LIMIT {limit}",
                    scenario.expr.where_sql, scenario.expr.order_sql
                ),
            ),
            5 => (
                "probe_idx_partial".to_string(),
                format!(
                    "SELECT id, a, c FROM t INDEXED BY idx_partial
                        WHERE ({}) AND a >= {}
                        ORDER BY a {}, c COLLATE {}, id
                        LIMIT {limit}",
                    scenario.partial_predicate,
                    rng.random_range(-30..=30),
                    scenario.a_order,
                    scenario.c_collation
                ),
            ),
            6 => {
                let lo = rng.random_range(1..=60);
                let hi = lo + rng.random_range(0..=50);
                (
                    "probe_idx_unique_c_id".to_string(),
                    format!(
                        "SELECT id, c FROM t INDEXED BY idx_unique_c_id
                            WHERE id BETWEEN {lo} AND {hi}
                            ORDER BY c COLLATE {}, id
                            LIMIT {limit}",
                        scenario.c_collation
                    ),
                )
            }
            _ => unreachable!(),
        }
    }

    fn execute_on_both(
        limbo_conn: &Arc<turso_core::Connection>,
        sqlite_conn: &rusqlite::Connection,
        stmt: &str,
        context: &str,
    ) {
        if let Err(err) = sqlite_conn.execute(stmt, params![]) {
            panic!("SQLite execution failed\n{context}\nstmt: {stmt}\nerror: {err}");
        }
        if let Err(err) = limbo_conn.execute(stmt) {
            panic!("Limbo execution failed\n{context}\nstmt: {stmt}\nerror: {err}");
        }
    }

    fn verify_reindex_state(
        limbo_conn: &Arc<turso_core::Connection>,
        sqlite_conn: &rusqlite::Connection,
        scenario: &Scenario,
        rng: &mut ChaCha8Rng,
        context: &str,
    ) {
        for (label, query) in &scenario.verify_queries {
            helpers::assert_differential(
                limbo_conn,
                sqlite_conn,
                query,
                &format!("{label} verification\n{context}"),
            );
        }
        for _ in 0..4 {
            let (label, query) = random_probe_query(rng, scenario);
            helpers::assert_differential(
                limbo_conn,
                sqlite_conn,
                &query,
                &format!("{label} verification\n{context}"),
            );
        }
        helpers::assert_differential(limbo_conn, sqlite_conn, "PRAGMA integrity_check", context);
    }

    #[turso_macros::test]
    pub fn reindex_differential_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("reindex_differential_fuzz");
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let scenario_count = helpers::fuzz_iterations(6);
        let iterations = helpers::fuzz_iterations(80);
        for scenario_num in 0..scenario_count {
            let scenario = build_scenario(&mut rng, scenario_num);
            println!("reindex_differential_fuzz {}", scenario.name);

            for ddl in &scenario.schema {
                execute_on_both(&limbo_conn, &sqlite_conn, ddl, &scenario.name);
            }

            let mut next_id = 1;
            let mut history = Vec::new();
            for _ in 0..60 {
                let stmt = insert_stmt(&mut rng, &mut next_id);
                history.push(stmt.clone());
                execute_on_both(&limbo_conn, &sqlite_conn, &stmt, "seed rows");
            }

            for iter in 0..iterations {
                helpers::log_progress("reindex_differential_fuzz", iter, iterations, 10);

                let action = rng.random_range(0..100);
                let stmt = match action {
                    0..=29 => insert_stmt(&mut rng, &mut next_id),
                    30..=44 => insert_or_replace_stmt(&mut rng, next_id - 1),
                    45..=69 => update_stmt(&mut rng, next_id - 1),
                    70..=84 => delete_stmt(&mut rng, next_id - 1),
                    85..=99 => scenario.reindex_targets.choose(&mut rng).unwrap().clone(),
                    _ => unreachable!(),
                };

                history.push(stmt.clone());
                let context = format!(
                    "{}\nseed: {seed}\nscenario: {scenario_num}\niteration: {iter}\nhistory:\n{}",
                    scenario.name,
                    helpers::history_tail(&history, 40)
                );
                execute_on_both(&limbo_conn, &sqlite_conn, &stmt, &context);

                if stmt.starts_with("REINDEX") || iter % 20 == 0 {
                    verify_reindex_state(&limbo_conn, &sqlite_conn, &scenario, &mut rng, &context);
                }
            }

            let context = format!(
                "final verification\n{}\nseed: {seed}\nscenario: {scenario_num}\nhistory:\n{}",
                scenario.name,
                helpers::history_tail(&history, 60)
            );
            verify_reindex_state(&limbo_conn, &sqlite_conn, &scenario, &mut rng, &context);

            execute_on_both(&limbo_conn, &sqlite_conn, "DROP TABLE t", &context);
        }
    }
}
