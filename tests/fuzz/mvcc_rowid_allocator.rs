#[cfg(test)]
mod mvcc_rowid_allocator_fuzz_tests {
    use crate::helpers;
    use core_tester::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::{params, types::Value};
    use std::sync::Arc;

    #[derive(Clone, Copy, Debug)]
    enum CommitOrder {
        ExplicitFirst,
        AutoFirst,
    }

    struct IndexedTable {
        name: String,
        idx_k: String,
        idx_u: String,
        idx_v_after: String,
        idx_ku_after: String,
        after_indexes_created: bool,
        next_unique: i64,
        history: Vec<String>,
    }

    impl IndexedTable {
        fn new(name: String) -> Self {
            Self {
                idx_k: format!("{name}_k"),
                idx_u: format!("{name}_u"),
                idx_v_after: format!("{name}_v_after"),
                idx_ku_after: format!("{name}_ku_after"),
                name,
                after_indexes_created: false,
                next_unique: 1,
                history: Vec::new(),
            }
        }

        fn next_unique(&mut self) -> i64 {
            let value = self.next_unique;
            self.next_unique += 1;
            value
        }
    }

    fn sql_text(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn integer_value(value: &Value) -> i64 {
        let Value::Integer(value) = value else {
            panic!("expected integer value, got {value:?}");
        };
        *value
    }

    fn interesting_rowid(rng: &mut ChaCha8Rng) -> i64 {
        const VALUES: &[i64] = &[-250, -100, -17, -5, -1, 0, 1, 2, 5, 17, 100, 250];
        if rng.random_bool(0.75) {
            VALUES[rng.random_range(0..VALUES.len())]
        } else {
            rng.random_range(-500..=500)
        }
    }

    fn positive_explicit_rowid(rng: &mut ChaCha8Rng) -> i64 {
        const VALUES: &[i64] = &[2, 3, 5, 8, 13, 21];
        if rng.random_bool(0.8) {
            VALUES[rng.random_range(0..VALUES.len())]
        } else {
            rng.random_range(2..=24)
        }
    }

    fn create_base_schema(conn: &Arc<turso_core::Connection>, table: &IndexedTable) {
        conn.execute(format!(
            "CREATE TABLE {}(id INTEGER PRIMARY KEY, v TEXT NOT NULL, k INTEGER NOT NULL, u INTEGER NOT NULL)",
            table.name
        ))
        .unwrap();
        conn.execute(format!("CREATE INDEX {} ON {}(k)", table.idx_k, table.name))
            .unwrap();
        conn.execute(format!(
            "CREATE UNIQUE INDEX {} ON {}(u)",
            table.idx_u, table.name
        ))
        .unwrap();
    }

    fn create_after_indexes(
        conn: &Arc<turso_core::Connection>,
        table: &mut IndexedTable,
        seed: u64,
        iter: usize,
        step: usize,
    ) {
        if table.after_indexes_created {
            return;
        }
        conn.execute(format!(
            "CREATE INDEX {} ON {}(v)",
            table.idx_v_after, table.name
        ))
        .unwrap_or_else(|err| {
            panic!(
                "failed creating normal index after rows: {err:?}\nseed: {seed}\niter: {iter}\nstep: {step}\nhistory:\n{}",
                helpers::history_tail(&table.history, 40)
            )
        });
        conn.execute(format!(
            "CREATE UNIQUE INDEX {} ON {}(k, u)",
            table.idx_ku_after, table.name
        ))
        .unwrap_or_else(|err| {
            panic!(
                "failed creating unique index after rows: {err:?}\nseed: {seed}\niter: {iter}\nstep: {step}\nhistory:\n{}",
                helpers::history_tail(&table.history, 40)
            )
        });
        table.after_indexes_created = true;
    }

    fn current_values(conn: &Arc<turso_core::Connection>, table: &str, column: &str) -> Vec<i64> {
        limbo_exec_rows(
            conn,
            &format!("SELECT {column} FROM {table} ORDER BY {column}"),
        )
        .into_iter()
        .map(|row| integer_value(&row[0]))
        .collect()
    }

    fn current_rowids(conn: &Arc<turso_core::Connection>, table: &str) -> Vec<i64> {
        current_values(conn, table, "id")
    }

    fn random_existing_rowid(
        conn: &Arc<turso_core::Connection>,
        table: &str,
        rng: &mut ChaCha8Rng,
    ) -> Option<i64> {
        let rowids = current_rowids(conn, table);
        (!rowids.is_empty()).then(|| rowids[rng.random_range(0..rowids.len())])
    }

    fn random_fresh_rowid(
        conn: &Arc<turso_core::Connection>,
        table: &str,
        rng: &mut ChaCha8Rng,
    ) -> i64 {
        let existing = current_rowids(conn, table);
        for _ in 0..16 {
            let rowid = interesting_rowid(rng);
            if !existing.contains(&rowid) {
                return rowid;
            }
        }
        existing.iter().copied().max().unwrap_or(0) + 1
    }

    fn execute_limbo(
        conn: &Arc<turso_core::Connection>,
        table: &mut IndexedTable,
        stmt: String,
        seed: u64,
        iter: usize,
        step: usize,
    ) {
        conn.execute(&stmt).unwrap_or_else(|err| {
            panic!(
                "statement failed: {err:?}\nseed: {seed}\niter: {iter}\nstep: {step}\nstmt: {stmt}\nhistory:\n{}",
                helpers::history_tail(&table.history, 40)
            )
        });
        table.history.push(stmt);
    }

    fn verify_mvcc_invariants(
        conn: &Arc<turso_core::Connection>,
        table: &IndexedTable,
        seed: u64,
        iter: usize,
        step: usize,
    ) {
        let integrity = limbo_exec_rows(conn, "PRAGMA integrity_check");
        assert_eq!(
            integrity,
            vec![vec![Value::Text("ok".to_string())]],
            "integrity_check failed\nseed: {seed}\niter: {iter}\nstep: {step}\ntable: {}\nhistory:\n{}",
            table.name,
            helpers::history_tail(&table.history, 40)
        );

        let duplicate_rowids = limbo_exec_rows(
            conn,
            &format!(
                "SELECT id, COUNT(*) FROM {} GROUP BY id HAVING COUNT(*) > 1",
                table.name
            ),
        );
        assert!(
            duplicate_rowids.is_empty(),
            "duplicate visible rowids\nseed: {seed}\niter: {iter}\nstep: {step}\ntable: {}\nduplicates: {duplicate_rowids:?}\nhistory:\n{}",
            table.name,
            helpers::history_tail(&table.history, 40)
        );

        let Some(sample) = limbo_exec_rows(
            conn,
            &format!("SELECT id, v, k, u FROM {} ORDER BY id LIMIT 1", table.name),
        )
        .into_iter()
        .next() else {
            return;
        };
        let v = match &sample[1] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text value in sample row, got {other:?}"),
        };
        let k = integer_value(&sample[2]);
        let u = integer_value(&sample[3]);

        assert_index_matches_table(
            conn,
            table,
            &table.idx_k,
            &format!("k = {k}"),
            seed,
            iter,
            step,
        );
        assert_index_matches_table(
            conn,
            table,
            &table.idx_u,
            &format!("u = {u}"),
            seed,
            iter,
            step,
        );
        if table.after_indexes_created {
            assert_index_matches_table(
                conn,
                table,
                &table.idx_v_after,
                &format!("v = {}", sql_text(&v)),
                seed,
                iter,
                step,
            );
            assert_index_matches_table(
                conn,
                table,
                &table.idx_ku_after,
                &format!("k = {k} AND u = {u}"),
                seed,
                iter,
                step,
            );
        }
    }

    fn assert_index_matches_table(
        conn: &Arc<turso_core::Connection>,
        table: &IndexedTable,
        index: &str,
        predicate: &str,
        seed: u64,
        iter: usize,
        step: usize,
    ) {
        let indexed_query = format!(
            "SELECT id, v, k, u FROM {} INDEXED BY {index} WHERE {predicate} ORDER BY id",
            table.name
        );
        let table_query = format!(
            "SELECT id, v, k, u FROM {} NOT INDEXED WHERE {predicate} ORDER BY id",
            table.name
        );
        let indexed = limbo_exec_rows(conn, &indexed_query);
        let table_rows = limbo_exec_rows(conn, &table_query);
        assert_eq!(
            indexed,
            table_rows,
            "indexed lookup disagrees with table lookup\nseed: {seed}\niter: {iter}\nstep: {step}\nindexed_query: {indexed_query}\ntable_query: {table_query}\nhistory:\n{}",
            helpers::history_tail(&table.history, 40)
        );
    }

    fn insert_auto_stmt(table: &mut IndexedTable, rng: &mut ChaCha8Rng) -> String {
        let u = table.next_unique();
        format!(
            "INSERT INTO {}(v, k, u) VALUES({}, {}, {u})",
            table.name,
            sql_text(&format!("v{u}")),
            rng.random_range(-8..=8)
        )
    }

    fn insert_explicit_stmt(
        conn: &Arc<turso_core::Connection>,
        table: &mut IndexedTable,
        rng: &mut ChaCha8Rng,
    ) -> String {
        let rowid = random_fresh_rowid(conn, &table.name, rng);
        let u = table.next_unique();
        format!(
            "INSERT INTO {}(id, v, k, u) VALUES({rowid}, {}, {}, {u})",
            table.name,
            sql_text(&format!("v{u}")),
            rng.random_range(-8..=8)
        )
    }

    fn delete_stmt(
        conn: &Arc<turso_core::Connection>,
        table: &IndexedTable,
        rng: &mut ChaCha8Rng,
    ) -> Option<String> {
        let target = random_existing_rowid(conn, &table.name, rng)?;
        if rng.random_bool(0.7) {
            Some(format!("DELETE FROM {} WHERE id = {target}", table.name))
        } else {
            Some(format!(
                "DELETE FROM {} WHERE k = {}",
                table.name,
                rng.random_range(-8..=8)
            ))
        }
    }

    fn update_rowid_stmt(
        conn: &Arc<turso_core::Connection>,
        table: &mut IndexedTable,
        rng: &mut ChaCha8Rng,
    ) -> Option<String> {
        let target = random_existing_rowid(conn, &table.name, rng)?;
        let new_rowid = random_fresh_rowid(conn, &table.name, rng);
        let u = table.next_unique();
        Some(format!(
            "UPDATE {} SET id = {new_rowid}, v = {}, k = {}, u = {u} WHERE id = {target}",
            table.name,
            sql_text(&format!("v{u}")),
            rng.random_range(-8..=8)
        ))
    }

    fn insert_or_replace_stmt(
        conn: &Arc<turso_core::Connection>,
        table: &mut IndexedTable,
        rng: &mut ChaCha8Rng,
    ) -> String {
        let existing_rowid = random_existing_rowid(conn, &table.name, rng);
        let existing_u = current_values(conn, &table.name, "u");
        let u = table.next_unique();
        let v = sql_text(&format!("v{u}"));
        let k = rng.random_range(-8..=8);

        match rng.random_range(0..4) {
            0 => format!(
                "INSERT OR REPLACE INTO {}(v, k, u) VALUES({v}, {k}, {u})",
                table.name
            ),
            1 if existing_rowid.is_some() => {
                let rowid = existing_rowid.unwrap();
                format!(
                    "INSERT OR REPLACE INTO {}(id, v, k, u) VALUES({rowid}, {v}, {k}, {u})",
                    table.name
                )
            }
            2 if !existing_u.is_empty() => {
                let conflicting_u = existing_u[rng.random_range(0..existing_u.len())];
                format!(
                    "INSERT OR REPLACE INTO {}(v, k, u) VALUES({v}, {k}, {conflicting_u})",
                    table.name
                )
            }
            _ => {
                let rowid = random_fresh_rowid(conn, &table.name, rng);
                format!(
                    "INSERT OR REPLACE INTO {}(id, v, k, u) VALUES({rowid}, {v}, {k}, {u})",
                    table.name
                )
            }
        }
    }

    fn run_negative_rowid_sqlite_parity_case(
        limbo_conn: &Arc<turso_core::Connection>,
        sqlite_conn: &rusqlite::Connection,
        name: &str,
        explicit_rowid: i64,
        seed: u64,
        iter: usize,
    ) {
        let create_table = format!("CREATE TABLE {name}(id INTEGER PRIMARY KEY, v TEXT)");
        let create_index = format!("CREATE UNIQUE INDEX {name}_v ON {name}(v)");
        limbo_conn.execute(&create_table).unwrap();
        sqlite_conn.execute(&create_table, params![]).unwrap();
        limbo_conn.execute(&create_index).unwrap();
        sqlite_conn.execute(&create_index, params![]).unwrap();

        let explicit_insert =
            format!("INSERT INTO {name}(id, v) VALUES({explicit_rowid}, 'manual')");
        let auto_insert = format!("INSERT INTO {name}(v) VALUES('auto')");
        limbo_conn.execute(&explicit_insert).unwrap();
        sqlite_conn.execute(&explicit_insert, params![]).unwrap();
        limbo_conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        limbo_conn.execute(&auto_insert).unwrap();
        sqlite_conn.execute(&auto_insert, params![]).unwrap();

        let query = format!("SELECT id, v FROM {name} ORDER BY id");
        let limbo_rows = limbo_exec_rows(limbo_conn, &query);
        let sqlite_rows = sqlite_exec_rows(sqlite_conn, &query);
        assert_eq!(
            limbo_rows, sqlite_rows,
            "negative rowid allocation mismatch\nseed: {seed}\niter: {iter}\nexplicit_rowid: {explicit_rowid}\nquery: {query}",
        );
    }

    fn run_concurrent_mini_scenario(
        db: &TempDatabase,
        name: String,
        rng: &mut ChaCha8Rng,
        seed: u64,
        iter: usize,
        step: usize,
    ) {
        let mut table = IndexedTable::new(name);
        let setup = db.connect_limbo();
        create_base_schema(&setup, &table);

        let explicit_rowid = positive_explicit_rowid(rng);
        let explicit_k = 100_000_i64 + iter as i64 * 100 + step as i64;
        let commit_order = if rng.random_bool(0.5) {
            CommitOrder::ExplicitFirst
        } else {
            CommitOrder::AutoFirst
        };

        let conn1 = db.connect_limbo();
        let conn2 = db.connect_limbo();

        conn1.execute("BEGIN CONCURRENT").unwrap();
        conn1
            .execute(format!(
                "INSERT INTO {}(id, v, k, u) VALUES({explicit_rowid}, 'explicit', {explicit_k}, 1)",
                table.name
            ))
            .unwrap();

        conn2.execute("BEGIN CONCURRENT").unwrap();
        conn2
            .execute(format!(
                "INSERT INTO {}(v, k, u) VALUES('auto_tx', -1, 2)",
                table.name
            ))
            .unwrap();

        match commit_order {
            CommitOrder::ExplicitFirst => {
                conn1.execute("COMMIT").unwrap();
                conn2.execute("COMMIT").unwrap();
            }
            CommitOrder::AutoFirst => {
                conn2.execute("COMMIT").unwrap();
                conn1.execute("COMMIT").unwrap();
            }
        }

        setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        create_after_indexes(&setup, &mut table, seed, iter, step);

        for n in 0..explicit_rowid {
            setup
                .execute(format!(
                    "INSERT INTO {}(v, k, u) VALUES('post_{n}', {}, {})",
                    table.name,
                    10_000 + n,
                    100 + n
                ))
                .unwrap();
        }

        verify_mvcc_invariants(&setup, &table, seed, iter, step);

        let table_query = format!(
            "SELECT id, v, k, u FROM {} WHERE id = {explicit_rowid}",
            table.name
        );
        let indexed_query = format!(
            "SELECT id, v, k, u FROM {} INDEXED BY {} WHERE k = {explicit_k}",
            table.name, table.idx_k
        );
        let table_rows = limbo_exec_rows(&setup, &table_query);
        let indexed_rows = limbo_exec_rows(&setup, &indexed_query);
        assert_eq!(
            table_rows,
            vec![vec![
                Value::Integer(explicit_rowid),
                Value::Text("explicit".to_string()),
                Value::Integer(explicit_k),
                Value::Integer(1),
            ]],
            "explicit row was overwritten\nseed: {seed}\niter: {iter}\nstep: {step}\ncommit_order: {commit_order:?}\nquery: {table_query}",
        );
        assert_eq!(
            indexed_rows, table_rows,
            "explicit row index lookup disagrees\nseed: {seed}\niter: {iter}\nstep: {step}\ncommit_order: {commit_order:?}\nquery: {indexed_query}",
        );
    }

    #[test]
    pub fn mvcc_rowid_allocator_fuzz() {
        let (mut rng, seed) = helpers::init_fuzz_test("mvcc_rowid_allocator_fuzz");
        let iterations = helpers::fuzz_iterations(10);
        let steps = std::env::var("FUZZ_ROWID_STEPS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(28);

        for iter in 0..iterations {
            helpers::log_progress("mvcc_rowid_allocator_fuzz", iter, iterations, 10);

            let db = TempDatabase::builder().with_mvcc(true).build();
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

            run_negative_rowid_sqlite_parity_case(
                &limbo_conn,
                &sqlite_conn,
                &format!("rowid_sqlite_parity_{iter}"),
                -positive_explicit_rowid(&mut rng),
                seed,
                iter,
            );

            let mut table = IndexedTable::new(format!("rowid_fuzz_{iter}"));
            create_base_schema(&limbo_conn, &table);

            for step in 0..steps {
                let stmt = match step {
                    0 => Some(insert_explicit_stmt(&limbo_conn, &mut table, &mut rng)),
                    1 => {
                        limbo_conn
                            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
                            .unwrap();
                        None
                    }
                    2 => Some(insert_auto_stmt(&mut table, &mut rng)),
                    3 => Some(insert_or_replace_stmt(&limbo_conn, &mut table, &mut rng)),
                    4 => update_rowid_stmt(&limbo_conn, &mut table, &mut rng),
                    5 => delete_stmt(&limbo_conn, &table, &mut rng),
                    6 => {
                        run_concurrent_mini_scenario(
                            &db,
                            format!("rowid_concurrent_{iter}_{step}"),
                            &mut rng,
                            seed,
                            iter,
                            step,
                        );
                        None
                    }
                    7 => {
                        create_after_indexes(&limbo_conn, &mut table, seed, iter, step);
                        None
                    }
                    _ => match rng.random_range(0..100) {
                        0..=24 => Some(insert_auto_stmt(&mut table, &mut rng)),
                        25..=44 => Some(insert_explicit_stmt(&limbo_conn, &mut table, &mut rng)),
                        45..=57 => delete_stmt(&limbo_conn, &table, &mut rng),
                        58..=72 => update_rowid_stmt(&limbo_conn, &mut table, &mut rng),
                        73..=84 => Some(insert_or_replace_stmt(&limbo_conn, &mut table, &mut rng)),
                        85..=89 => {
                            limbo_conn
                                .execute("PRAGMA wal_checkpoint(TRUNCATE)")
                                .unwrap();
                            None
                        }
                        90..=94 => {
                            create_after_indexes(&limbo_conn, &mut table, seed, iter, step);
                            None
                        }
                        95..=99 => {
                            run_concurrent_mini_scenario(
                                &db,
                                format!("rowid_concurrent_{iter}_{step}"),
                                &mut rng,
                                seed,
                                iter,
                                step,
                            );
                            None
                        }
                        _ => unreachable!(),
                    },
                };

                if let Some(stmt) = stmt {
                    execute_limbo(&limbo_conn, &mut table, stmt, seed, iter, step);
                }

                if step % 7 == 6 {
                    limbo_conn
                        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        .unwrap();
                }
                verify_mvcc_invariants(&limbo_conn, &table, seed, iter, step);
            }

            create_after_indexes(&limbo_conn, &mut table, seed, iter, steps);
            limbo_conn
                .execute("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            verify_mvcc_invariants(&limbo_conn, &table, seed, iter, steps);
        }
    }
}
