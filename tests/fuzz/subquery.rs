//! Subquery fuzz tests.
//!
//! These tests verify subquery behavior by comparing Turso results with SQLite.
//! Tests cover scalar subqueries in various expression contexts, FROM clause
//! subqueries, and subquery combinations with CTEs.

#[cfg(test)]
mod subquery_tests {
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;

    use core_tester::common::{
        limbo_exec_rows, rng_from_time_or_env, sqlite_exec_rows, TempDatabase,
    };

    /// Scalar values that can be used in subqueries
    const SCALAR_VALUES: &[&str] = &[
        "1", "42", "-1", "0", "NULL", "'hello'", "'world'", "1.5", "-3.14",
    ];

    /// Functions that take subquery results as arguments
    const SCALAR_FUNCTIONS: &[(&str, usize)] = &[
        ("ABS", 1),
        ("LENGTH", 1),
        ("UPPER", 1),
        ("LOWER", 1),
        ("TYPEOF", 1),
        ("COALESCE", 2),
        ("IFNULL", 2),
        ("NULLIF", 2),
        ("MAX", 2),
        ("MIN", 2),
        ("INSTR", 2),
        ("SUBSTR", 3),
        ("REPLACE", 3),
        ("IIF", 3),
    ];

    /// Generate a simple scalar subquery
    fn gen_scalar_subquery(rng: &mut ChaCha8Rng) -> String {
        let val = SCALAR_VALUES[rng.random_range(0..SCALAR_VALUES.len())];
        format!("(SELECT {val})")
    }

    /// Tests scalar subqueries as function arguments
    #[turso_macros::test]
    pub fn subquery_in_function_args(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_in_function_args seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!(
                    "subquery_in_function_args iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }

            let (func_name, arg_count) =
                SCALAR_FUNCTIONS[rng.random_range(0..SCALAR_FUNCTIONS.len())];

            let args: Vec<String> = (0..arg_count)
                .map(|_| gen_scalar_subquery(&mut rng))
                .collect();

            let query = format!("SELECT {}({})", func_name, args.join(", "));

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "Function argument mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests scalar subqueries in arithmetic expressions
    #[turso_macros::test]
    pub fn subquery_in_arithmetic(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_in_arithmetic seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const OPS: &[&str] = &["+", "-", "*", "/", "%", "|", "&", "<<", ">>"];
        const INTEGERS: &[i32] = &[1, 2, 3, 5, 7, 10, 42, 100];

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!("subquery_in_arithmetic iteration {}/{}", i + 1, ITERATIONS);
            }

            let op = OPS[rng.random_range(0..OPS.len())];
            let v1 = INTEGERS[rng.random_range(0..INTEGERS.len())];
            let v2 = INTEGERS[rng.random_range(0..INTEGERS.len())];

            // Avoid division by zero
            let v2 = if (op == "/" || op == "%") && v2 == 0 {
                1
            } else {
                v2
            };
            // Avoid large shifts
            let v2 = if op == "<<" || op == ">>" {
                v2 % 32
            } else {
                v2
            };

            let query = format!("SELECT (SELECT {v1}) {op} (SELECT {v2})");

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "Arithmetic mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests scalar subqueries in comparison expressions
    #[turso_macros::test]
    pub fn subquery_in_comparisons(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_in_comparisons seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const COMP_OPS: &[&str] = &["=", "!=", "<>", "<", ">", "<=", ">=", "IS", "IS NOT"];
        const VALUES: &[&str] = &["1", "2", "NULL", "'a'", "'b'"];

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!("subquery_in_comparisons iteration {}/{}", i + 1, ITERATIONS);
            }

            let op = COMP_OPS[rng.random_range(0..COMP_OPS.len())];
            let v1 = VALUES[rng.random_range(0..VALUES.len())];
            let v2 = VALUES[rng.random_range(0..VALUES.len())];

            let query = format!("SELECT (SELECT {v1}) {op} (SELECT {v2})");

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "Comparison mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests CASE expressions with subqueries
    #[turso_macros::test]
    pub fn subquery_in_case(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_in_case seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!("subquery_in_case iteration {}/{}", i + 1, ITERATIONS);
            }

            let scenario = rng.random_range(0..4);
            let query = match scenario {
                0 => {
                    // CASE WHEN with subquery condition
                    let cond_val = rng.random_range(0..2);
                    format!("SELECT CASE WHEN (SELECT {cond_val}) THEN 'true' ELSE 'false' END")
                }
                1 => {
                    // Simple CASE with subquery operand
                    let val = rng.random_range(1..4);
                    format!(
                        "SELECT CASE (SELECT {val}) WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"
                    )
                }
                2 => {
                    // CASE with subquery in THEN
                    let val = rng.random_range(1..100);
                    format!("SELECT CASE 1 WHEN 1 THEN (SELECT {val}) ELSE 0 END")
                }
                3 => {
                    // CASE with subquery in ELSE
                    let val = rng.random_range(1..100);
                    format!("SELECT CASE 1 WHEN 2 THEN 0 ELSE (SELECT {val}) END")
                }
                _ => unreachable!(),
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "CASE mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests BETWEEN with subqueries
    #[turso_macros::test]
    pub fn subquery_in_between(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_in_between seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 50;
        for i in 0..ITERATIONS {
            if i % 10 == 0 {
                println!("subquery_in_between iteration {}/{}", i + 1, ITERATIONS);
            }

            let val = rng.random_range(1..20);
            let low = rng.random_range(1..10);
            let high = rng.random_range(10..20);
            let not = if rng.random_bool(0.3) { "NOT " } else { "" };

            let query =
                format!("SELECT (SELECT {val}) {not}BETWEEN (SELECT {low}) AND (SELECT {high})");

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "BETWEEN mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests LIKE/GLOB with subqueries
    #[turso_macros::test]
    pub fn subquery_in_like_glob(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_in_like_glob seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const STRINGS: &[&str] = &["'hello'", "'world'", "'test'", "'abc'", "'xyz'"];
        const LIKE_PATTERNS: &[&str] = &["'%ell%'", "'h%'", "'%d'", "'t%t'", "'___'"];
        const GLOB_PATTERNS: &[&str] = &["'*ell*'", "'h*'", "'*d'", "'t*t'", "'???'"];

        const ITERATIONS: usize = 50;
        for i in 0..ITERATIONS {
            if i % 10 == 0 {
                println!("subquery_in_like_glob iteration {}/{}", i + 1, ITERATIONS);
            }

            let str_val = STRINGS[rng.random_range(0..STRINGS.len())];
            let use_glob = rng.random_bool(0.5);

            let query = if use_glob {
                let pattern = GLOB_PATTERNS[rng.random_range(0..GLOB_PATTERNS.len())];
                format!("SELECT (SELECT {str_val}) GLOB (SELECT {pattern})")
            } else {
                let pattern = LIKE_PATTERNS[rng.random_range(0..LIKE_PATTERNS.len())];
                format!("SELECT (SELECT {str_val}) LIKE (SELECT {pattern})")
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "LIKE/GLOB mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests FROM clause subqueries with various operations
    #[turso_macros::test]
    pub fn subquery_in_from_clause(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_in_from_clause seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const COMPOUND_OPS: &[&str] = &["UNION", "UNION ALL", "INTERSECT", "EXCEPT"];

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!("subquery_in_from_clause iteration {}/{}", i + 1, ITERATIONS);
            }

            let scenario = rng.random_range(0..6);
            let query = match scenario {
                0 => {
                    // Simple FROM subquery
                    let v1 = rng.random_range(1..100);
                    let v2 = rng.random_range(1..100);
                    format!("SELECT * FROM (SELECT {v1} a, {v2} b)")
                }
                1 => {
                    // FROM subquery with compound
                    let op = COMPOUND_OPS[rng.random_range(0..COMPOUND_OPS.len())];
                    let v1 = rng.random_range(1..10);
                    let v2 = rng.random_range(1..10);
                    let v3 = rng.random_range(1..10);
                    format!("SELECT * FROM (SELECT {v1} x {op} SELECT {v2} {op} SELECT {v3}) ORDER BY 1")
                }
                2 => {
                    // Nested FROM subqueries
                    let val = rng.random_range(1..100);
                    format!("SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT {val} x)))")
                }
                3 => {
                    // FROM subquery with WHERE
                    let v1 = rng.random_range(1..50);
                    let v2 = rng.random_range(50..100);
                    let threshold = rng.random_range(25..75);
                    format!(
                        "SELECT * FROM (SELECT {v1} x UNION ALL SELECT {v2}) WHERE x > {threshold}"
                    )
                }
                4 => {
                    // FROM subquery with ORDER BY outside (ORDER BY inside compound not supported)
                    let v1 = rng.random_range(1..10);
                    let v2 = rng.random_range(1..10);
                    let v3 = rng.random_range(1..10);
                    format!(
                        "SELECT * FROM (SELECT {v3} x UNION SELECT {v1} UNION SELECT {v2}) ORDER BY 1"
                    )
                }
                5 => {
                    // FROM subquery with LIMIT
                    let n = rng.random_range(1..5);
                    format!(
                        "SELECT * FROM (SELECT 1 x UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 LIMIT {n}) ORDER BY 1"
                    )
                }
                _ => unreachable!(),
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "FROM clause mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests joins between subqueries
    #[turso_macros::test]
    pub fn subquery_joins(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_joins seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 50;
        for i in 0..ITERATIONS {
            if i % 10 == 0 {
                println!("subquery_joins iteration {}/{}", i + 1, ITERATIONS);
            }

            let scenario = rng.random_range(0..4);
            let query = match scenario {
                0 => {
                    // INNER JOIN between subqueries
                    let v1 = rng.random_range(1..5);
                    let v2 = rng.random_range(1..5);
                    format!(
                        "SELECT * FROM (SELECT {v1} a, 'x' b) x JOIN (SELECT {v2} c, 'y' d) y ON x.a = y.c"
                    )
                }
                1 => {
                    // LEFT JOIN between subqueries
                    let v1 = rng.random_range(1..5);
                    let v2 = rng.random_range(1..5);
                    format!(
                        "SELECT x.a, y.c FROM (SELECT {v1} a) x LEFT JOIN (SELECT {v2} c) y ON x.a = y.c"
                    )
                }
                2 => {
                    // CROSS JOIN (cartesian product)
                    let v1 = rng.random_range(1..3);
                    let v2 = rng.random_range(1..3);
                    format!(
                        "SELECT * FROM (SELECT {v1} a UNION SELECT {}) x, (SELECT {v2} b UNION SELECT {}) y ORDER BY a, b",
                        v1 + 1,
                        v2 + 1
                    )
                }
                3 => {
                    // NATURAL JOIN
                    let val = rng.random_range(1..10);
                    format!(
                        "SELECT * FROM (SELECT {val} x, 'a' y) NATURAL JOIN (SELECT {val} x, 'b' z)"
                    )
                }
                _ => unreachable!(),
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "JOIN mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests CTE combined with scalar subqueries
    #[turso_macros::test]
    pub fn cte_with_scalar_subqueries(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("cte_with_scalar_subqueries seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            if i % 25 == 0 {
                println!(
                    "cte_with_scalar_subqueries iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }

            let scenario = rng.random_range(0..5);
            let query = match scenario {
                0 => {
                    // CTE referenced in scalar subquery in SELECT
                    let v1 = rng.random_range(1..10);
                    let v2 = rng.random_range(1..10);
                    let v3 = rng.random_range(1..10);
                    format!(
                        "WITH t(x) AS (VALUES({v1}),({v2}),({v3})) \
                         SELECT (SELECT MAX(x) FROM t), (SELECT MIN(x) FROM t), (SELECT SUM(x) FROM t)"
                    )
                }
                1 => {
                    // CTE used both directly and in subquery
                    let v1 = rng.random_range(1..10);
                    let v2 = rng.random_range(1..10);
                    format!(
                        "WITH t(x) AS (VALUES({v1}),({v2})) \
                         SELECT t.x, (SELECT SUM(x) FROM t) as total FROM t ORDER BY x"
                    )
                }
                2 => {
                    // CTE in EXISTS subquery
                    let v1 = rng.random_range(1..5);
                    let v2 = rng.random_range(1..5);
                    let v3 = rng.random_range(1..5);
                    format!(
                        "WITH data(x) AS (VALUES({v1}),({v2}),({v3})) \
                         SELECT * FROM data d WHERE EXISTS (SELECT 1 FROM data WHERE x > d.x) ORDER BY x"
                    )
                }
                3 => {
                    // CTE in FROM subquery
                    let v1 = rng.random_range(1..10);
                    let v2 = rng.random_range(1..10);
                    format!(
                        "WITH t(x) AS (VALUES({v1}),({v2})) \
                         SELECT * FROM (SELECT x * 2 as doubled FROM t) ORDER BY doubled"
                    )
                }
                4 => {
                    // Multiple scalar subquery references to same CTE
                    let val = rng.random_range(10..100);
                    let materialized = if rng.random_bool(0.5) {
                        " MATERIALIZED"
                    } else {
                        ""
                    };
                    format!(
                        "WITH t AS{materialized} (SELECT {val} as x) \
                         SELECT (SELECT x FROM t) + (SELECT x FROM t) + (SELECT x FROM t)"
                    )
                }
                _ => unreachable!(),
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "CTE+subquery mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests empty subquery edge cases
    #[turso_macros::test]
    pub fn subquery_empty_results(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_empty_results seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 50;
        for i in 0..ITERATIONS {
            if i % 10 == 0 {
                println!("subquery_empty_results iteration {}/{}", i + 1, ITERATIONS);
            }

            let scenario = rng.random_range(0..5);
            let query = match scenario {
                0 => {
                    // Empty subquery in comparison
                    let val = rng.random_range(1..100);
                    format!("SELECT {val} WHERE {val} = (SELECT 1 WHERE FALSE)")
                }
                1 => {
                    // Aggregate on empty subquery
                    "SELECT (SELECT SUM(x) FROM (SELECT 1 as x WHERE FALSE))".to_string()
                }
                2 => {
                    // COUNT on empty subquery (returns 0, not NULL)
                    "SELECT (SELECT COUNT(*) FROM (SELECT 1 WHERE FALSE))".to_string()
                }
                3 => {
                    // Empty subquery in COALESCE
                    let default = rng.random_range(1..100);
                    format!("SELECT COALESCE((SELECT 1 WHERE FALSE), {default})")
                }
                4 => {
                    // LIMIT 0 subquery
                    "SELECT * FROM (SELECT 1 UNION SELECT 2 LIMIT 0)".to_string()
                }
                _ => unreachable!(),
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "Empty subquery mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }

    /// Tests NOT IN with NULL handling edge cases
    #[turso_macros::test]
    pub fn subquery_not_in_null_handling(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("subquery_not_in_null_handling seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        const ITERATIONS: usize = 50;
        for i in 0..ITERATIONS {
            if i % 10 == 0 {
                println!(
                    "subquery_not_in_null_handling iteration {}/{}",
                    i + 1,
                    ITERATIONS
                );
            }

            let scenario = rng.random_range(0..4);
            let query = match scenario {
                0 => {
                    // NOT IN with NULL in subquery result
                    "SELECT 1 WHERE 1 NOT IN (SELECT NULL)".to_string()
                }
                1 => {
                    // NOT IN with NULL and value
                    let val = rng.random_range(1..10);
                    format!(
                        "SELECT 1 WHERE {val} NOT IN (SELECT x FROM (SELECT 1 x UNION SELECT NULL UNION SELECT 2))"
                    )
                }
                2 => {
                    // IN with NULL - should still match if value present
                    let val = rng.random_range(1..3);
                    format!(
                        "SELECT 1 WHERE {val} IN (SELECT x FROM (SELECT 1 x UNION SELECT NULL UNION SELECT 2))"
                    )
                }
                3 => {
                    // Comparison with empty subquery result (NULL)
                    let val = rng.random_range(1..100);
                    format!("SELECT (SELECT 1 WHERE FALSE) = {val}")
                }
                _ => unreachable!(),
            };

            let limbo_result = limbo_exec_rows(&limbo_conn, &query);
            let sqlite_result = sqlite_exec_rows(&sqlite_conn, &query);

            assert_eq!(
                limbo_result, sqlite_result,
                "NOT IN NULL mismatch!\nseed: {seed}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
            );
        }
    }
}
