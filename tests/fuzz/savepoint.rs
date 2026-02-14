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

    fn random_fk_pid_value(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..10) {
            0..=1 => "NULL".to_string(),
            2..=7 => rng.random_range(1..=40).to_string(),
            8..=9 => rng.random_range(41..=70).to_string(),
            _ => unreachable!(),
        }
    }

    fn random_fk_dml_stmt(rng: &mut ChaCha8Rng) -> String {
        match rng.random_range(0..8) {
            0 => {
                let id = rng.random_range(1..=40);
                let grp = random_nullable_int(rng, -3..=3);
                let tag = TAG_POOL.choose(rng).unwrap();
                match rng.random_range(0..3) {
                    0 => format!("INSERT INTO p(id, grp, tag) VALUES ({id}, {grp}, '{tag}')"),
                    1 => format!(
                        "INSERT OR IGNORE INTO p(id, grp, tag) VALUES ({id}, {grp}, '{tag}')"
                    ),
                    2 => format!(
                        "INSERT OR REPLACE INTO p(id, grp, tag) VALUES ({id}, {grp}, '{tag}')"
                    ),
                    _ => unreachable!(),
                }
            }
            1 => {
                let id = rng.random_range(1..=40);
                let set_clause = match rng.random_range(0..3) {
                    0 => format!("grp = {}", random_nullable_int(rng, -3..=3)),
                    1 => format!("tag = '{}'", TAG_POOL.choose(rng).unwrap()),
                    2 => format!(
                        "grp = {}, tag = '{}'",
                        random_nullable_int(rng, -3..=3),
                        TAG_POOL.choose(rng).unwrap()
                    ),
                    _ => unreachable!(),
                };
                format!("UPDATE p SET {set_clause} WHERE id = {id}")
            }
            2 => {
                if rng.random_bool(0.1) {
                    "DELETE FROM p".to_string()
                } else {
                    format!("DELETE FROM p WHERE id = {}", rng.random_range(1..=40))
                }
            }
            3 => {
                let id = rng.random_range(1..=80);
                let pid = random_fk_pid_value(rng);
                let note = TAG_POOL.choose(rng).unwrap();
                match rng.random_range(0..3) {
                    0 => format!("INSERT INTO c(id, pid, note) VALUES ({id}, {pid}, '{note}')"),
                    1 => format!(
                        "INSERT OR IGNORE INTO c(id, pid, note) VALUES ({id}, {pid}, '{note}')"
                    ),
                    2 => format!(
                        "INSERT OR REPLACE INTO c(id, pid, note) VALUES ({id}, {pid}, '{note}')"
                    ),
                    _ => unreachable!(),
                }
            }
            4 => {
                let id = rng.random_range(1..=80);
                if rng.random_bool(0.5) {
                    let pid = random_fk_pid_value(rng);
                    format!("UPDATE c SET pid = {pid} WHERE id = {id}")
                } else {
                    format!(
                        "UPDATE c SET note = '{}' WHERE id = {id}",
                        TAG_POOL.choose(rng).unwrap()
                    )
                }
            }
            5 => {
                if rng.random_bool(0.1) {
                    "DELETE FROM c".to_string()
                } else {
                    format!("DELETE FROM c WHERE id = {}", rng.random_range(1..=80))
                }
            }
            6 => {
                let old_id = rng.random_range(1..=40);
                let new_id = rng.random_range(1..=40);
                format!("UPDATE p SET id = {new_id} WHERE id = {old_id}")
            }
            7 => {
                let id = rng.random_range(1..=80);
                let pid = random_fk_pid_value(rng);
                let note = TAG_POOL.choose(rng).unwrap();
                format!("INSERT INTO c(pid, note, id) VALUES ({pid}, '{note}', {id})")
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

        limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite_conn
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();

        for schema in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, grp INT, v INT UNIQUE, tag TEXT)",
            "CREATE TABLE p (id INTEGER PRIMARY KEY, grp INT, tag TEXT)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT, note TEXT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            limbo_conn.execute(schema).unwrap();
            sqlite_conn.execute(schema, params![]).unwrap();
        }

        for id in 1..=500 {
            let seed_stmt = format!(
                "INSERT INTO t(id, grp, v, tag) VALUES ({id}, {}, {}, 'seed{}')",
                id % 4,
                id * 10,
                id % 3
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        for id in 1..=40 {
            let seed_stmt = format!(
                "INSERT INTO p(id, grp, tag) VALUES ({id}, {}, 'pseed{}')",
                id % 5,
                id % 3
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        for id in 1..=60 {
            let pid = if id % 7 == 0 {
                "NULL".to_string()
            } else {
                ((id % 40) + 1).to_string()
            };
            let seed_stmt = format!(
                "INSERT INTO c(id, pid, note) VALUES ({id}, {pid}, 'cseed{}')",
                id % 5
            );
            limbo_conn.execute(&seed_stmt).unwrap();
            sqlite_conn.execute(&seed_stmt, params![]).unwrap();
        }

        const STEPS: usize = 2000;
        let mut history = Vec::with_capacity(STEPS + 16);
        let verify_queries = [
            (
                "t",
                "SELECT id, grp, v, tag FROM t ORDER BY id, grp, v, tag",
            ),
            ("p", "SELECT id, grp, tag FROM p ORDER BY id, grp, tag"),
            ("c", "SELECT id, pid, note FROM c ORDER BY id, pid, note"),
        ];

        for step in 0..STEPS {
            if step % 250 == 0 {
                println!(
                    "named_savepoint_differential_fuzz step {}/{}",
                    step + 1,
                    STEPS
                );
            }

            let stmt = match rng.random_range(0..100) {
                0..=29 => random_dml_stmt(&mut rng),
                30..=49 => random_fk_dml_stmt(&mut rng),
                50..=74 => format!("SAVEPOINT {}", random_savepoint_name(&mut rng)),
                75..=87 => {
                    let name = random_savepoint_name(&mut rng);
                    if rng.random_bool(0.5) {
                        format!("RELEASE {name}")
                    } else {
                        format!("RELEASE SAVEPOINT {name}")
                    }
                }
                88..=99 => {
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

            for (label, verify_query) in verify_queries {
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify_query);
                let limbo_rows = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    limbo_exec_rows(&limbo_conn, verify_query)
                }));
                let limbo_rows = match limbo_rows {
                    Ok(rows) => rows,
                    Err(_) => {
                        panic!(
                            "limbo panicked while verifying state ({label})\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent statements:\n{}",
                            history_tail(&history, 50)
                        );
                    }
                };
                assert_eq!(
                    limbo_rows,
                    sqlite_rows,
                    "named savepoint state mismatch ({label})\nseed: {seed}\nstep: {step}\nstmt: {stmt}\nrecent statements:\n{}",
                    history_tail(&history, 50)
                );
            }
        }
    }

    #[turso_macros::test(mvcc)]
    fn deferred_fk_repair_on_replace_child_delete_matches_sqlite(db: TempDatabase) {
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite_conn
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();

        for schema in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            limbo_conn.execute(schema).unwrap();
            sqlite_conn.execute(schema, params![]).unwrap();
        }

        for stmt in [
            "INSERT INTO p(id) VALUES (1)",
            "INSERT INTO c(id, pid) VALUES (1, 1)",
            "SAVEPOINT s",
            "DELETE FROM p WHERE id = 1",
            "INSERT OR REPLACE INTO c(id, pid) VALUES (1, NULL)",
        ] {
            let sqlite_res = sqlite_conn.execute(stmt, params![]);
            let limbo_res = limbo_exec_rows_fallible(&db, &limbo_conn, stmt);
            assert!(
                sqlite_res.is_ok() == limbo_res.is_ok(),
                "statement outcome mismatch for `{stmt}`\nsqlite: {sqlite_res:?}\nlimbo: {limbo_res:?}"
            );
        }

        let sqlite_release = sqlite_conn.execute("RELEASE s", params![]);
        let limbo_release = limbo_exec_rows_fallible(&db, &limbo_conn, "RELEASE s");
        assert!(
            sqlite_release.is_ok() == limbo_release.is_ok(),
            "release outcome mismatch\nsqlite: {sqlite_release:?}\nlimbo: {limbo_release:?}"
        );
    }

    #[turso_macros::test(mvcc)]
    fn deferred_fk_autocommit_violation_matches_sqlite(db: TempDatabase) {
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite_conn
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();

        for schema in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            limbo_conn.execute(schema).unwrap();
            sqlite_conn.execute(schema, params![]).unwrap();
        }

        let stmt = "INSERT INTO c(id, pid) VALUES (1, 999)";
        let sqlite_res = sqlite_conn.execute(stmt, params![]);
        let limbo_res = limbo_exec_rows_fallible(&db, &limbo_conn, stmt);
        assert!(
            sqlite_res.is_ok() == limbo_res.is_ok(),
            "autocommit deferred-fk outcome mismatch\nsqlite: {sqlite_res:?}\nlimbo: {limbo_res:?}"
        );
    }

    #[turso_macros::test(mvcc)]
    fn release_root_deferred_fk_failure_can_recover_with_rollback_to(db: TempDatabase) {
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite_conn
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();

        for schema in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            limbo_conn.execute(schema).unwrap();
            sqlite_conn.execute(schema, params![]).unwrap();
        }

        for stmt in ["SAVEPOINT s", "INSERT INTO c(id, pid) VALUES (1, 999)"] {
            let sqlite_res = sqlite_conn.execute(stmt, params![]);
            let limbo_res = limbo_exec_rows_fallible(&db, &limbo_conn, stmt);
            assert!(
                sqlite_res.is_ok() == limbo_res.is_ok(),
                "statement outcome mismatch for `{stmt}`\nsqlite: {sqlite_res:?}\nlimbo: {limbo_res:?}"
            );
        }

        let sqlite_release = sqlite_conn.execute("RELEASE s", params![]);
        let limbo_release = limbo_exec_rows_fallible(&db, &limbo_conn, "RELEASE s");
        assert!(
            sqlite_release.is_ok() == limbo_release.is_ok(),
            "release outcome mismatch\nsqlite: {sqlite_release:?}\nlimbo: {limbo_release:?}"
        );
        assert!(
            sqlite_release.is_err(),
            "expected deferred FK error while releasing root savepoint"
        );

        for stmt in ["ROLLBACK TO s", "RELEASE s"] {
            let sqlite_res = sqlite_conn.execute(stmt, params![]);
            let limbo_res = limbo_exec_rows_fallible(&db, &limbo_conn, stmt);
            assert!(
                sqlite_res.is_ok() == limbo_res.is_ok(),
                "statement outcome mismatch for `{stmt}`\nsqlite: {sqlite_res:?}\nlimbo: {limbo_res:?}"
            );
        }

        let sqlite_rows = sqlite_exec_rows(&sqlite_conn, "SELECT count(*) FROM c");
        let limbo_rows = limbo_exec_rows(&limbo_conn, "SELECT count(*) FROM c");
        assert_eq!(
            limbo_rows, sqlite_rows,
            "final table state mismatch after rollback-to recovery"
        );
    }
}
