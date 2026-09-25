#[cfg(test)]
mod interrupt_tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use turso_core::{Connection, StepResult};

    use core_tester::common::{maybe_setup_tracing, rng_from_time_or_env, TempDatabase};

    const ITERATIONS: usize = 10_000;
    const DROPPED_READ: &str =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100) SELECT x FROM c";

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Outcome {
        Done,
        Interrupted,
    }

    enum Change {
        None,
        Insert { first_id: i64, values: Vec<i64> },
        Update { id: i64, v: i64 },
        DeleteBelow { id: i64 },
        Rollback,
    }

    /// Another thread calls `Connection::interrupt()` while statements start
    /// and finish. Once that thread is idle again, the next statement must
    /// not be interrupted, and every statement that finished must be visible.
    #[test]
    fn interrupt_from_another_thread_only_stops_statements_it_overlapped() {
        maybe_setup_tracing();
        let (mut rng, seed) = rng_from_time_or_env();
        println!("interrupt_from_another_thread_only_stops_statements_it_overlapped seed: {seed}");

        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();

        let statement_running = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(2));
        let end = Arc::new(Barrier::new(2));
        let interrupter = {
            let conn = conn.clone();
            let statement_running = statement_running.clone();
            let shutdown = shutdown.clone();
            let start = start.clone();
            let end = end.clone();
            let mut rng = ChaCha8Rng::seed_from_u64(seed.wrapping_add(1));
            std::thread::spawn(move || loop {
                start.wait();
                if shutdown.load(Ordering::SeqCst) {
                    return;
                }
                for _ in 0..rng.random_range(0..2_000) {
                    std::hint::spin_loop();
                }
                while statement_running.load(Ordering::SeqCst) {
                    conn.interrupt();
                }
                end.wait();
            })
        };

        let mut committed = BTreeMap::new();
        let mut working = BTreeMap::new();
        let mut next_id = 1;
        let mut checks_with_flag_set = 0;
        for iteration in 0..ITERATIONS {
            let (sql, change) = random_statement(&mut rng, &conn, &working, &mut next_id);

            statement_running.store(true, Ordering::SeqCst);
            start.wait();
            let mut stmt = conn.prepare(sql.as_str()).unwrap();
            let max_rows = if sql == DROPPED_READ {
                rng.random_range(1..50)
            } else {
                usize::MAX
            };
            let outcome = step_rows(&mut stmt, max_rows);
            drop(stmt);
            statement_running.store(false, Ordering::SeqCst);
            end.wait();

            if outcome == Outcome::Done {
                apply(&change, &mut working, &committed);
            }
            if conn.get_auto_commit() {
                if outcome == Outcome::Done {
                    committed = working.clone();
                } else {
                    working = committed.clone();
                }
            } else if outcome == Outcome::Interrupted && change.writes() {
                // SQLite rolls the transaction back here; Turso keeps it open
                // with the statement's partial writes and refuses to commit it.
                run_uninterrupted(&conn, "ROLLBACK", seed, &sql, |_| {});
                working = committed.clone();
            }

            if conn.is_interrupted() || iteration % 500 == 0 {
                checks_with_flag_set += usize::from(conn.is_interrupted());
                assert_table_matches(&conn, &working, seed, &sql);
            }
        }

        shutdown.store(true, Ordering::SeqCst);
        start.wait();
        interrupter.join().unwrap();
        assert_table_matches(&conn, &working, seed, "the last statement");
        println!("{checks_with_flag_set} checks ran while an interrupt request was left over");
    }

    fn random_statement(
        rng: &mut ChaCha8Rng,
        conn: &Arc<Connection>,
        working: &BTreeMap<i64, i64>,
        next_id: &mut i64,
    ) -> (String, Change) {
        let existing_id = |rng: &mut ChaCha8Rng| {
            if working.is_empty() {
                0
            } else {
                *working
                    .keys()
                    .nth(rng.random_range(0..working.len()))
                    .unwrap()
            }
        };
        let in_transaction = !conn.get_auto_commit();
        match rng.random_range(0..100) {
            0..20 => ("SELECT count(*), sum(v) FROM t".to_string(), Change::None),
            20..30 => (DROPPED_READ.to_string(), Change::None),
            30..50 => {
                let id = *next_id;
                *next_id += 1;
                let v = rng.random_range(0..1_000);
                (
                    format!("INSERT INTO t(id, v) VALUES ({id}, {v})"),
                    Change::Insert {
                        first_id: id,
                        values: vec![v],
                    },
                )
            }
            50..58 => {
                let first_id = *next_id;
                let n = rng.random_range(2..60);
                *next_id += n;
                (
                    format!(
                        "INSERT INTO t(id, v) WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) SELECT {first_id} + x - 1, x FROM c"
                    ),
                    Change::Insert {
                        first_id,
                        values: (1..=n).collect(),
                    },
                )
            }
            58..73 => {
                let id = existing_id(rng);
                let v = rng.random_range(0..1_000);
                (
                    format!("UPDATE t SET v = {v} WHERE id = {id}"),
                    Change::Update { id, v },
                )
            }
            73..80 => {
                let id = existing_id(rng);
                (
                    format!("DELETE FROM t WHERE id < {id}"),
                    Change::DeleteBelow { id },
                )
            }
            80..90 if !in_transaction => ("BEGIN".to_string(), Change::None),
            80..90 => ("COMMIT".to_string(), Change::None),
            _ if in_transaction => ("ROLLBACK".to_string(), Change::Rollback),
            _ => (
                "SELECT v FROM t ORDER BY v DESC LIMIT 3".to_string(),
                Change::None,
            ),
        }
    }

    impl Change {
        fn writes(&self) -> bool {
            matches!(
                self,
                Change::Insert { .. } | Change::Update { .. } | Change::DeleteBelow { .. }
            )
        }
    }

    fn apply(change: &Change, working: &mut BTreeMap<i64, i64>, committed: &BTreeMap<i64, i64>) {
        match change {
            Change::None => {}
            Change::Insert { first_id, values } => {
                for (id, v) in (*first_id..).zip(values) {
                    assert!(working.insert(id, *v).is_none());
                }
            }
            Change::Update { id, v } => {
                if let Some(old) = working.get_mut(id) {
                    *old = *v;
                }
            }
            Change::DeleteBelow { id } => working.retain(|k, _| k >= id),
            Change::Rollback => *working = committed.clone(),
        }
    }

    fn assert_table_matches(
        conn: &Arc<Connection>,
        expected: &BTreeMap<i64, i64>,
        seed: u64,
        previous_sql: &str,
    ) {
        let mut rows = BTreeMap::new();
        run_uninterrupted(
            conn,
            "SELECT id, v FROM t ORDER BY id",
            seed,
            previous_sql,
            |row| {
                rows.insert(row.get::<i64>(0).unwrap(), row.get::<i64>(1).unwrap());
            },
        );
        assert_eq!(
            &rows, expected,
            "seed {seed}: table contents differ from the statements that finished; previous statement: {previous_sql}"
        );
    }

    fn run_uninterrupted(
        conn: &Arc<Connection>,
        sql: &str,
        seed: u64,
        previous_sql: &str,
        mut on_row: impl FnMut(&turso_core::Row),
    ) {
        let mut stmt = conn.prepare(sql).unwrap();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => on_row(stmt.row().unwrap()),
                StepResult::IO => stmt._io().step().unwrap(),
                StepResult::Done => return,
                StepResult::Interrupt => panic!(
                    "seed {seed}: {sql} was interrupted although no interrupt() call overlapped it; previous statement: {previous_sql}"
                ),
                other => panic!("unexpected step result {other:?}"),
            }
        }
    }

    fn step_rows(stmt: &mut turso_core::Statement, max_rows: usize) -> Outcome {
        let mut rows = 0;
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    rows += 1;
                    if rows == max_rows {
                        return Outcome::Done;
                    }
                }
                StepResult::IO => stmt._io().step().unwrap(),
                StepResult::Done => return Outcome::Done,
                StepResult::Interrupt => return Outcome::Interrupted,
                other => panic!("unexpected step result {other:?}"),
            }
        }
    }
}
