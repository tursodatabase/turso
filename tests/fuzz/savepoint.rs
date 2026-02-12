#[cfg(test)]
mod savepoint_tests {
    use std::panic::AssertUnwindSafe;

    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    use core_tester::common::{
        limbo_exec_rows, limbo_exec_rows_fallible, rng_from_time_or_env, sqlite_exec_rows,
        TempDatabase,
    };

    const SAVEPOINT_NAMES: [&str; 8] = ["sp0", "sp1", "outer", "inner", "alpha", "beta", "x", "y"];
    const TAG_POOL: [&str; 8] = ["a", "b", "c", "d", "e", "foo", "bar", "baz"];

    fn random_savepoint_name(rng: &mut ChaCha8Rng) -> &'static str {
        SAVEPOINT_NAMES.choose(rng).unwrap()
    }

    fn random_where_clause(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..5) {
            0 => format!("WHERE id = {}", rng.random_range(1..=30)),
            1 => format!("WHERE grp = {}", rng.random_range(-3..=3)),
            2 => format!("WHERE v = {}", rng.random_range(1..=30)),
            3 => format!("WHERE tag = '{}'", TAG_POOL.choose(rng).unwrap()),
            4 => "WHERE (id % 2) = 0".to_string(),
            _ => unreachable!(),
        }
    }

    fn random_nullable_int(rng: &mut ChaCha8Rng, range: std::ops::RangeInclusive<i64>) -> String {
        if rng.random_bool(0.2) {
            "NULL".to_string()
        } else {
            rng.random_range(range).to_string()
        }
    }

    fn random_dml_stmt(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..4) {
            0 => {
                let id = rng.random_range(1..=30);
                let grp = random_nullable_int(rng, -3..=3);
                let v = random_nullable_int(rng, 1..=30);
                let tag = TAG_POOL.choose(rng).unwrap();
                match rng.random_range(0..4) {
                    0 => {
                        format!("INSERT INTO t(id, grp, v, tag) VALUES ({id}, {grp}, {v}, '{tag}')")
                    }
                    1 => format!("INSERT INTO t(grp, v, tag) VALUES ({grp}, {v}, '{tag}')"),
                    2 => format!(
                        "INSERT OR IGNORE INTO t(id, grp, v, tag) VALUES ({id}, {grp}, {v}, '{tag}')"
                    ),
                    3 => format!(
                        "INSERT OR REPLACE INTO t(id, grp, v, tag) VALUES ({id}, {grp}, {v}, '{tag}')"
                    ),
                    _ => unreachable!(),
                }
            }
            1 => {
                let set_clause = match rng.random_range(0..4) {
                    0 => format!("grp = {}", random_nullable_int(rng, -3..=3)),
                    1 => format!("v = {}", random_nullable_int(rng, 1..=30)),
                    2 => format!("tag = '{}'", TAG_POOL.choose(rng).unwrap()),
                    3 => format!(
                        "grp = {}, v = {}",
                        random_nullable_int(rng, -3..=3),
                        random_nullable_int(rng, 1..=30)
                    ),
                    _ => unreachable!(),
                };
                format!("UPDATE t SET {set_clause} {}", random_where_clause(rng))
            }
            2 => {
                if rng.random_bool(0.2) {
                    "DELETE FROM t".to_string()
                } else {
                    format!("DELETE FROM t {}", random_where_clause(rng))
                }
            }
            3 => {
                let grp = random_nullable_int(rng, -3..=3);
                let v = random_nullable_int(rng, 1..=30);
                let tag = TAG_POOL.choose(rng).unwrap();
                format!("INSERT INTO t(grp, v, tag) VALUES ({grp}, {v}, '{tag}')")
            }
            _ => unreachable!(),
        }
    }

    fn history_tail(history: &[String], max_lines: usize) -> String {
        let start = history.len().saturating_sub(max_lines);
        history[start..]
            .iter()
            .enumerate()
            .map(|(i, stmt)| format!("{:04}: {}", start + i + 1, stmt))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[turso_macros::test(mvcc)]
    pub fn named_savepoint_differential_fuzz(db: TempDatabase) {
        let _ = env_logger::try_init();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("named_savepoint_differential_fuzz seed: {seed}");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let schema = "CREATE TABLE t (id INTEGER PRIMARY KEY, grp INT, v INT UNIQUE, tag TEXT)";
        limbo_conn.execute(schema).unwrap();
        sqlite_conn.execute(schema, params![]).unwrap();

        for id in 1..=8 {
            let seed_stmt = format!(
                "INSERT INTO t(id, grp, v, tag) VALUES ({id}, {}, {}, 'seed{}')",
                id % 4,
                id * 10,
                id % 3
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        const STEPS: usize = 2000;
        let mut history = Vec::with_capacity(STEPS + 16);
        let verify_query = "SELECT id, grp, v, tag FROM t ORDER BY id, grp, v, tag";

        for step in 0..STEPS {
            if step % 250 == 0 {
                println!(
                    "named_savepoint_differential_fuzz step {}/{}",
                    step + 1,
                    STEPS
                );
            }

            let stmt = match rng.random_range(0..100) {
                0..=39 => random_dml_stmt(&mut rng),
                40..=69 => format!("SAVEPOINT {}", random_savepoint_name(&mut rng)),
                70..=84 => {
                    let name = random_savepoint_name(&mut rng);
                    if rng.random_bool(0.5) {
                        format!("RELEASE {name}")
                    } else {
                        format!("RELEASE SAVEPOINT {name}")
                    }
                }
                85..=99 => {
                    let name = random_savepoint_name(&mut rng);
                    if rng.random_bool(0.5) {
                        format!("ROLLBACK TO {name}")
                    } else {
                        format!("ROLLBACK TO SAVEPOINT {name}")
                    }
                }
                _ => unreachable!(),
            };

            history.push(stmt.clone());

            let sqlite_res = sqlite_conn.execute(&stmt, params![]);
            let limbo_res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                limbo_exec_rows_fallible(&db, &limbo_conn, &stmt)
            }));
            let limbo_res = match limbo_res {
                Ok(res) => res,
                Err(_) => {
                    panic!(
                        "limbo panicked while executing statement\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent statements:\n{}",
                        history_tail(&history, 50)
                    );
                }
            };

            match (sqlite_res, limbo_res) {
                (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
                (sqlite_outcome, limbo_outcome) => {
                    panic!(
                        "named savepoint outcome mismatch\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nsqlite: {sqlite_outcome:?}\nlimbo: {limbo_outcome:?}\nrecent statements:\n{}",
                        history_tail(&history, 50)
                    );
                }
            }

            let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify_query);
            let limbo_rows = std::panic::catch_unwind(AssertUnwindSafe(|| {
                limbo_exec_rows(&limbo_conn, verify_query)
            }));
            let limbo_rows = match limbo_rows {
                Ok(rows) => rows,
                Err(_) => {
                    panic!(
                        "limbo panicked while verifying state\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent statements:\n{}",
                        history_tail(&history, 50)
                    );
                }
            };
            assert_eq!(
                limbo_rows,
                sqlite_rows,
                "named savepoint state mismatch\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent statements:\n{}",
                history_tail(&history, 50)
            );
        }
    }
}
