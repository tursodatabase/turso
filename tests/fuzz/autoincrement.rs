#[cfg(test)]
mod autoincrement_fuzz_tests {
    use crate::helpers;
    use core_tester::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rusqlite::types::Value;

    fn single_int(rows: &[Vec<Value>], label: &str) -> i64 {
        assert_eq!(rows.len(), 1, "{label}: expected 1 row, got {}", rows.len());
        assert_eq!(
            rows[0].len(),
            1,
            "{label}: expected 1 column, got {}",
            rows[0].len()
        );
        match rows[0][0] {
            Value::Integer(v) => v,
            ref v => panic!("{label}: expected integer, got {v:?}"),
        }
    }

    fn read_limbo_seq(conn: &std::sync::Arc<turso_core::Connection>) -> i64 {
        single_int(
            &limbo_exec_rows(
                conn,
                "SELECT COALESCE(MAX(seq), 0) FROM sqlite_sequence WHERE name='t'",
            ),
            "limbo sqlite_sequence max(seq)",
        )
    }

    fn read_sqlite_seq(conn: &rusqlite::Connection) -> i64 {
        single_int(
            &sqlite_exec_rows(
                conn,
                "SELECT COALESCE(MAX(seq), 0) FROM sqlite_sequence WHERE name='t'",
            ),
            "sqlite sqlite_sequence max(seq)",
        )
    }

    fn fetch_limbo_ids(conn: &std::sync::Arc<turso_core::Connection>) -> Vec<i64> {
        let rows = limbo_exec_rows(conn, "SELECT id FROM t ORDER BY id");
        rows.iter()
            .map(|row| match row[0] {
                Value::Integer(v) => v,
                ref v => panic!("expected integer id, got {v:?}"),
            })
            .collect()
    }

    fn maybe_random_existing_id(
        rng: &mut impl rand::Rng,
        conn: &std::sync::Arc<turso_core::Connection>,
    ) -> Option<i64> {
        let ids = fetch_limbo_ids(conn);
        ids.choose(rng).copied()
    }

    #[turso_macros::test(mvcc)]
    fn autoincrement_monotonic_differential_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("autoincrement_monotonic_differential_fuzz");

        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let create_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)";
        helpers::execute_on_both(&limbo_conn, &sqlite_conn, create_sql, "create table");

        let iterations = std::env::var("FUZZ_ITERATIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| helpers::fuzz_iterations(250));

        let mut last_auto_id: i64 = 0;
        let mut last_seq: i64 = 0;
        let mut history: Vec<String> = Vec::with_capacity(iterations + 16);

        for step in 0..iterations {
            helpers::log_progress(
                "autoincrement_monotonic_differential_fuzz",
                step,
                iterations,
                8,
            );
            let action = rng.random_range(0..100);
            let stmt = if action < 55 {
                // Auto-assigned id path (main invariant target).
                format!("INSERT INTO t(payload) VALUES ('auto_{step}') RETURNING id")
            } else if action < 75 {
                // Explicit id insert can move sqlite_sequence forward if id is larger.
                let explicit_id = match rng.random_range(0..4) {
                    0 => rng.random_range(1..=(last_seq + 1).max(1)),
                    1 => last_seq + rng.random_range(1..=32),
                    2 => rng.random_range(-50..=50),
                    _ => rng.random_range(1..=200),
                };
                format!("INSERT OR IGNORE INTO t(id, payload) VALUES ({explicit_id}, 'explicit_{step}')")
            } else if action < 88 {
                match maybe_random_existing_id(&mut rng, &limbo_conn) {
                    Some(id) => format!("DELETE FROM t WHERE id = {id}"),
                    None => continue,
                }
            } else {
                match maybe_random_existing_id(&mut rng, &limbo_conn) {
                    Some(id) => {
                        let delta = rng.random_range(-5..=20);
                        let new_id = id.saturating_add(delta);
                        format!("UPDATE OR IGNORE t SET id = {new_id} WHERE id = {id}")
                    }
                    None => continue,
                }
            };

            history.push(stmt.clone());
            let ctx = format!(
                "seed={seed} step={step}\nstmt={stmt}\nhistory:\n{}",
                helpers::history_tail(&history, 24)
            );

            if stmt.contains("RETURNING id") {
                let limbo_rows = limbo_exec_rows(&limbo_conn, &stmt);
                let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &stmt);
                assert_eq!(
                    limbo_rows, sqlite_rows,
                    "RETURNING mismatch\n{}\nlimbo={:?}\nsqlite={:?}",
                    ctx, limbo_rows, sqlite_rows
                );
                let inserted_id = single_int(&limbo_rows, "RETURNING id");
                assert!(
                    inserted_id > last_auto_id,
                    "auto rowid regressed: prev={} new={}\n{}",
                    last_auto_id,
                    inserted_id,
                    ctx
                );
                last_auto_id = inserted_id;
            } else {
                helpers::execute_on_both(&limbo_conn, &sqlite_conn, &stmt, &ctx);
            }

            let limbo_seq = read_limbo_seq(&limbo_conn);
            let sqlite_seq = read_sqlite_seq(&sqlite_conn);
            assert_eq!(
                limbo_seq, sqlite_seq,
                "sqlite_sequence parity mismatch\n{}\nlimbo_seq={}\nsqlite_seq={}",
                ctx, limbo_seq, sqlite_seq
            );
            assert!(
                limbo_seq >= last_seq,
                "sqlite_sequence decreased: prev={} new={}\n{}",
                last_seq,
                limbo_seq,
                ctx
            );
            assert!(
                limbo_seq >= last_auto_id,
                "sqlite_sequence below last auto id: seq={} last_auto={}\n{}",
                limbo_seq,
                last_auto_id,
                ctx
            );
            last_seq = limbo_seq;

            helpers::assert_differential(
                &limbo_conn,
                &sqlite_conn,
                "SELECT id, payload FROM t ORDER BY id",
                &ctx,
            );
        }

        helpers::assert_differential(
            &limbo_conn,
            &sqlite_conn,
            "PRAGMA integrity_check",
            &format!("seed={seed} final"),
        );
    }

    #[turso_macros::test(mvcc)]
    fn autoincrement_concurrent_commit_fuzz(db: TempDatabase) {
        if !db.enable_mvcc {
            return;
        }
        let (mut rng, seed) = helpers::init_fuzz_test("autoincrement_concurrent_commit_fuzz");
        let admin = db.connect_limbo();
        let conn1 = db.connect_limbo();
        let conn2 = db.connect_limbo();

        admin
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)")
            .unwrap();

        let rounds = std::env::var("FUZZ_ITERATIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| helpers::fuzz_iterations(150));

        let mut last_seq: i64 = 0;
        let mut generated_ids: Vec<i64> = Vec::with_capacity(rounds * 4);

        for round in 0..rounds {
            helpers::log_progress("autoincrement_concurrent_commit_fuzz", round, rounds, 8);

            conn1.execute("BEGIN CONCURRENT").unwrap();
            conn2.execute("BEGIN CONCURRENT").unwrap();

            let n1 = rng.random_range(1..=3);
            let n2 = rng.random_range(1..=3);
            for i in 0..n1 {
                let rows = limbo_exec_rows(
                    &conn1,
                    &format!("INSERT INTO t(payload) VALUES ('c1_{round}_{i}') RETURNING id"),
                );
                generated_ids.push(single_int(&rows, "conn1 returning id"));
            }
            for i in 0..n2 {
                let rows = limbo_exec_rows(
                    &conn2,
                    &format!("INSERT INTO t(payload) VALUES ('c2_{round}_{i}') RETURNING id"),
                );
                generated_ids.push(single_int(&rows, "conn2 returning id"));
            }

            let commit_order_conn1_first = rng.random_bool(0.5);
            let first = if commit_order_conn1_first {
                (&conn1, "conn1")
            } else {
                (&conn2, "conn2")
            };
            let second = if commit_order_conn1_first {
                (&conn2, "conn2")
            } else {
                (&conn1, "conn1")
            };

            first.0.execute("COMMIT").unwrap_or_else(|e| {
                panic!(
                    "{} commit failed (seed={seed}, round={round}): {e:?}",
                    first.1
                )
            });
            second.0.execute("COMMIT").unwrap_or_else(|e| {
                panic!(
                    "{} commit failed (seed={seed}, round={round}): {e:?}",
                    second.1
                )
            });

            if rng.random_bool(0.30) {
                let explicit = last_seq + rng.random_range(1..=16);
                admin
                    .execute(format!(
                        "INSERT OR IGNORE INTO t(id, payload) VALUES ({explicit}, 'admin_{round}')"
                    ))
                    .unwrap();
            }
            if rng.random_bool(0.25) {
                if let Some(id) = maybe_random_existing_id(&mut rng, &admin) {
                    admin
                        .execute(format!("DELETE FROM t WHERE id = {id}"))
                        .unwrap();
                }
            }

            let seq = read_limbo_seq(&admin);
            assert!(
                seq >= last_seq,
                "sqlite_sequence decreased (seed={seed}, round={round}): {} -> {}",
                last_seq,
                seq
            );
            last_seq = seq;
        }

        for pair in generated_ids.windows(2) {
            assert!(
                pair[1] > pair[0],
                "auto rowid allocation regressed (seed={seed}): {:?}",
                pair
            );
        }

        let max_generated = generated_ids.iter().copied().max().unwrap_or(0);
        let final_seq = read_limbo_seq(&admin);
        assert!(
            final_seq >= max_generated,
            "final sqlite_sequence below max generated id (seed={seed}): seq={} max_generated={}",
            final_seq,
            max_generated
        );

        let integrity = limbo_exec_rows(&admin, "PRAGMA integrity_check");
        assert_eq!(
            integrity,
            vec![vec![Value::Text("ok".to_string())]],
            "integrity_check failed (seed={seed})"
        );
    }
}
