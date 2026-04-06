#[cfg(test)]
mod temp_table_fuzz_tests {
    use std::sync::Arc;

    use crate::helpers;
    use core_tester::common::{
        do_flush, limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase,
    };
    use rand::seq::IndexedRandom;
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::types::Value;

    struct ConnPair {
        limbo: Arc<turso_core::Connection>,
        sqlite: rusqlite::Connection,
        temp_store: &'static str,
        next_temp_id: i64,
        next_shadow_id: i64,
    }

    struct StatementSpec {
        sql: String,
        returns_rows: bool,
    }

    fn random_text(rng: &mut ChaCha8Rng, prefix: &str) -> String {
        format!("'{prefix}_{}'", helpers::generate_random_text(rng, 8))
    }

    fn sqlite_query_rows_fallible(
        conn: &rusqlite::Connection,
        query: &str,
    ) -> rusqlite::Result<Vec<Vec<Value>>> {
        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut result = Vec::new();
            for i in 0.. {
                let column: Value = match row.get(i) {
                    Ok(column) => column,
                    Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                    Err(err) => return Err(err),
                };
                result.push(column);
            }
            results.push(result);
        }
        Ok(results)
    }

    fn execute_sqlite_statement_fallible(
        conn: &rusqlite::Connection,
        spec: &StatementSpec,
    ) -> rusqlite::Result<Vec<Vec<Value>>> {
        if spec.returns_rows {
            sqlite_query_rows_fallible(conn, &spec.sql)
        } else {
            conn.execute_batch(&spec.sql)?;
            Ok(Vec::new())
        }
    }

    fn existing_tag_expr(table_name: &str, descending: bool) -> String {
        let dir = if descending { "DESC" } else { "ASC" };
        format!("(SELECT tag FROM {table_name} WHERE tag IS NOT NULL ORDER BY id {dir} LIMIT 1)")
    }

    fn verify_connection_views(conn_idx: usize, pair: &ConnPair, context: &str) {
        let queries = [
            (
                "main table rows",
                "SELECT id, owner_conn, v, tag, note FROM main.shared ORDER BY id",
            ),
            (
                "temp table rows",
                "SELECT id, v, tag, note FROM temp_data ORDER BY id",
            ),
            (
                "shadow table rows",
                "SELECT id, v, tag, note FROM shared ORDER BY id",
            ),
            (
                "temp/main join",
                "SELECT t.id, t.v, t.tag, m.owner_conn \
                 FROM temp_data AS t \
                 LEFT JOIN main.shared AS m ON m.id = t.id \
                 ORDER BY t.id, t.tag, m.owner_conn",
            ),
            (
                "temp aggregate",
                "SELECT count(*), COALESCE(sum(v), 0) FROM temp_data",
            ),
            (
                "shadow aggregate",
                "SELECT count(*), COALESCE(sum(v), 0) FROM shared",
            ),
            (
                "main schema count for shared",
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'shared'",
            ),
            (
                "main schema count for temp_data",
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'temp_data'",
            ),
            (
                "temp schema inventory",
                "SELECT name, type, tbl_name \
                 FROM temp.sqlite_master \
                 WHERE type IN ('table', 'index') \
                 ORDER BY type, name",
            ),
        ];

        for (label, query) in queries {
            let limbo_rows = limbo_exec_rows(&pair.limbo, query);
            let sqlite_rows = sqlite_exec_rows(&pair.sqlite, query);
            similar_asserts::assert_eq!(
                Turso: limbo_rows,
                Sqlite: sqlite_rows,
                "temp-table differential mismatch\nconnection: {conn_idx}\ntemp_store: {}\n{context}\nquery[{label}]: {query}",
                pair.temp_store,
            );
        }
    }

    fn random_temp_table_dml(
        rng: &mut ChaCha8Rng,
        table_name: &str,
        next_id: &mut i64,
        prefix: &str,
    ) -> StatementSpec {
        match rng.random_range(0..20) {
            0 => {
                let id = *next_id;
                *next_id += 1;
                let v = rng.random_range(-50..=200);
                StatementSpec {
                    sql: format!(
                        "INSERT INTO {table_name}(id, v, tag, note) VALUES ({id}, {v}, {}, {})",
                        random_text(rng, &format!("{prefix}_ins")),
                        random_text(rng, &format!("{prefix}_note")),
                    ),
                    returns_rows: false,
                }
            }
            1 => {
                let id1 = *next_id;
                let id2 = *next_id + 1;
                *next_id += 2;
                StatementSpec {
                    sql: format!(
                        "INSERT INTO {table_name}(id, v, tag, note) VALUES \
                         ({id1}, {}, {}, {}), \
                         ({id2}, {}, {}, {})",
                        rng.random_range(-50..=200),
                        random_text(rng, &format!("{prefix}_multi_tag")),
                        random_text(rng, &format!("{prefix}_multi_note")),
                        rng.random_range(-50..=200),
                        random_text(rng, &format!("{prefix}_multi_tag")),
                        random_text(rng, &format!("{prefix}_multi_note")),
                    ),
                    returns_rows: false,
                }
            }
            2 => StatementSpec {
                sql: format!(
                    "INSERT OR IGNORE INTO {table_name}(id, v, tag, note) \
                     VALUES (1, {}, {}, {})",
                    rng.random_range(-50..=200),
                    random_text(rng, &format!("{prefix}_ignore_tag")),
                    random_text(rng, &format!("{prefix}_ignore_note")),
                ),
                returns_rows: false,
            },
            3 => {
                let id = rng.random_range(1..=(*next_id).max(2));
                StatementSpec {
                    sql: format!(
                        "INSERT OR REPLACE INTO {table_name}(id, v, tag, note) \
                         VALUES ({id}, {}, {}, {})",
                        rng.random_range(-50..=200),
                        random_text(rng, &format!("{prefix}_repl_tag")),
                        random_text(rng, &format!("{prefix}_repl_note")),
                    ),
                    returns_rows: false,
                }
            }
            4 => {
                let id = rng.random_range(1..=(*next_id).max(2));
                StatementSpec {
                    sql: format!(
                        "REPLACE INTO {table_name}(id, v, tag, note) \
                         VALUES ({id}, {}, {}, {})",
                        rng.random_range(-50..=200),
                        existing_tag_expr(table_name, false),
                        random_text(rng, &format!("{prefix}_replace_into")),
                    ),
                    returns_rows: false,
                }
            }
            5 => {
                let id = *next_id;
                *next_id += 1;
                StatementSpec {
                    sql: format!(
                        "INSERT INTO {table_name}(id, v, tag, note) \
                         SELECT {id}, COALESCE(v, 0) + 1, {}, COALESCE(note, 'seed') \
                         FROM {table_name} ORDER BY id LIMIT 1",
                        random_text(rng, &format!("{prefix}_selfcopy")),
                    ),
                    returns_rows: false,
                }
            }
            6 => {
                let id = *next_id;
                *next_id += 1;
                StatementSpec {
                    sql: format!(
                        "INSERT INTO {table_name}(id, v, tag, note) \
                         SELECT {id}, COALESCE(v, 0) + 7, {}, COALESCE(note, 'from_main') \
                         FROM main.shared ORDER BY id LIMIT 1",
                        random_text(rng, &format!("{prefix}_maincopy")),
                    ),
                    returns_rows: false,
                }
            }
            7 => {
                let id = rng.random_range(1..=(*next_id).max(2));
                StatementSpec {
                    sql: format!(
                        "INSERT INTO {table_name}(id, v, tag, note) \
                         VALUES ({id}, {}, {}, {}) ON CONFLICT(id) DO NOTHING",
                        rng.random_range(-50..=200),
                        random_text(rng, &format!("{prefix}_donothing_tag")),
                        random_text(rng, &format!("{prefix}_donothing_note")),
                    ),
                    returns_rows: false,
                }
            }
            8 => {
                let id = rng.random_range(1..=(*next_id).max(2));
                StatementSpec {
                    sql: format!(
                        "INSERT INTO {table_name}(id, v, tag, note) \
                         VALUES ({id}, {}, {}, {}) \
                         ON CONFLICT(id) DO UPDATE SET \
                             v = excluded.v + 100, \
                             tag = excluded.tag, \
                             note = excluded.note",
                        rng.random_range(-50..=200),
                        random_text(rng, &format!("{prefix}_upsert_tag")),
                        random_text(rng, &format!("{prefix}_upsert_note")),
                    ),
                    returns_rows: false,
                }
            }
            9 => {
                let id = *next_id;
                *next_id += 1;
                StatementSpec {
                    sql: format!(
                        "INSERT INTO {table_name}(id, v, tag, note) \
                         VALUES ({id}, {}, {}, {}) \
                         RETURNING id, v, tag, note",
                        rng.random_range(-50..=200),
                        random_text(rng, &format!("{prefix}_ret_tag")),
                        random_text(rng, &format!("{prefix}_ret_note")),
                    ),
                    returns_rows: true,
                }
            }
            10 => {
                let target = rng.random_range(1..=(*next_id).max(2));
                StatementSpec {
                    sql: format!(
                        "UPDATE {table_name} \
                         SET v = COALESCE(v, 0) + {}, note = {} \
                         WHERE id = {target}",
                        rng.random_range(1..=25),
                        random_text(rng, &format!("{prefix}_upd")),
                    ),
                    returns_rows: false,
                }
            }
            11 => StatementSpec {
                sql: format!(
                    "UPDATE OR IGNORE {table_name} \
                     SET tag = {} \
                     WHERE id = (SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1)",
                    existing_tag_expr(table_name, false),
                ),
                returns_rows: false,
            },
            12 => StatementSpec {
                sql: format!(
                    "UPDATE OR REPLACE {table_name} \
                     SET tag = {} \
                     WHERE id = (SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1)",
                    existing_tag_expr(table_name, false),
                ),
                returns_rows: false,
            },
            13 => StatementSpec {
                sql: format!(
                    "UPDATE OR FAIL {table_name} SET v = NULL \
                     WHERE id = (SELECT id FROM {table_name} ORDER BY id LIMIT 1)"
                ),
                returns_rows: false,
            },
            14 => StatementSpec {
                sql: format!(
                    "UPDATE OR ABORT {table_name} SET v = NULL \
                     WHERE id = (SELECT id FROM {table_name} ORDER BY id LIMIT 1)"
                ),
                returns_rows: false,
            },
            15 => StatementSpec {
                sql: format!(
                    "UPDATE OR ROLLBACK {table_name} SET v = NULL \
                     WHERE id = (SELECT id FROM {table_name} ORDER BY id LIMIT 1)"
                ),
                returns_rows: false,
            },
            16 => StatementSpec {
                sql: format!(
                    "UPDATE OR REPLACE {table_name} SET v = NULL \
                     WHERE id = (SELECT id FROM {table_name} ORDER BY id LIMIT 1)"
                ),
                returns_rows: false,
            },
            17 => StatementSpec {
                sql: format!(
                    "UPDATE {table_name} \
                     SET v = COALESCE(v, 0) + {}, note = {} \
                     WHERE id IN (SELECT id FROM {table_name} ORDER BY id LIMIT 2) \
                     RETURNING id, v, tag, note",
                    rng.random_range(1..=25),
                    random_text(rng, &format!("{prefix}_upd_ret")),
                ),
                returns_rows: true,
            },
            18 => {
                if rng.random_bool(0.5) {
                    StatementSpec {
                        sql: format!(
                            "DELETE FROM {table_name} \
                             WHERE id = (SELECT id FROM {table_name} ORDER BY id LIMIT 1)"
                        ),
                        returns_rows: false,
                    }
                } else {
                    StatementSpec {
                        sql: format!(
                            "DELETE FROM {table_name} \
                             WHERE id IN (SELECT id FROM {table_name} ORDER BY id DESC LIMIT 2)"
                        ),
                        returns_rows: false,
                    }
                }
            }
            19 => StatementSpec {
                sql: format!(
                    "DELETE FROM {table_name} \
                     WHERE id = (SELECT id FROM {table_name} ORDER BY id LIMIT 1) \
                     RETURNING id, v, tag, note"
                ),
                returns_rows: true,
            },
            _ => unreachable!(),
        }
    }

    fn random_temp_dml(rng: &mut ChaCha8Rng, pair: &mut ConnPair) -> StatementSpec {
        random_temp_table_dml(rng, "temp_data", &mut pair.next_temp_id, "tmp")
    }

    fn random_shadow_dml(rng: &mut ChaCha8Rng, pair: &mut ConnPair) -> StatementSpec {
        random_temp_table_dml(rng, "shared", &mut pair.next_shadow_id, "shadow")
    }

    fn random_main_dml(
        rng: &mut ChaCha8Rng,
        conn_idx: usize,
        next_main_id: &mut i64,
    ) -> StatementSpec {
        match rng.random_range(0..8) {
            0 => {
                let id = *next_main_id;
                *next_main_id += 1;
                StatementSpec {
                    sql: format!(
                        "INSERT INTO main.shared(id, owner_conn, v, tag, note) \
                         VALUES ({id}, {conn_idx}, {}, {}, {})",
                        rng.random_range(-100..=500),
                        random_text(rng, "main_tag"),
                        random_text(rng, "main_note"),
                    ),
                    returns_rows: false,
                }
            }
            1 => StatementSpec {
                sql: format!(
                    "UPDATE main.shared SET v = {} WHERE id = {}",
                    rng.random_range(-100..=500),
                    rng.random_range(1..=(*next_main_id).max(2)),
                ),
                returns_rows: false,
            },
            2 => StatementSpec {
                sql: format!(
                    "UPDATE main.shared SET tag = {}, note = {} WHERE owner_conn = {}",
                    random_text(rng, "main_upd_tag"),
                    random_text(rng, "main_upd_note"),
                    rng.random_range(0..=conn_idx as i64),
                ),
                returns_rows: false,
            },
            3 => {
                if rng.random_bool(0.15) {
                    StatementSpec {
                        sql: format!("DELETE FROM main.shared WHERE owner_conn = {conn_idx}"),
                        returns_rows: false,
                    }
                } else {
                    StatementSpec {
                        sql: format!(
                            "DELETE FROM main.shared WHERE id = {}",
                            rng.random_range(1..=(*next_main_id).max(2)),
                        ),
                        returns_rows: false,
                    }
                }
            }
            4 => {
                let id = *next_main_id;
                *next_main_id += 1;
                StatementSpec {
                    sql: format!(
                        "INSERT INTO main.shared(id, owner_conn, v, tag, note) \
                         SELECT {id}, {conn_idx}, v, tag, note FROM temp_data ORDER BY id LIMIT 1"
                    ),
                    returns_rows: false,
                }
            }
            5 => {
                let id = *next_main_id;
                *next_main_id += 1;
                StatementSpec {
                    sql: format!(
                        "INSERT INTO main.shared(id, owner_conn, v, tag, note) \
                         SELECT {id}, {conn_idx}, v, tag, note FROM shared ORDER BY id LIMIT 1"
                    ),
                    returns_rows: false,
                }
            }
            6 => StatementSpec {
                sql: format!(
                    "INSERT INTO main.shared(id, owner_conn, v, tag, note) \
                     VALUES ({}, {conn_idx}, {}, {}, {}) \
                     ON CONFLICT(id) DO UPDATE SET \
                         v = excluded.v, tag = excluded.tag, note = excluded.note",
                    rng.random_range(1..=(*next_main_id).max(2)),
                    rng.random_range(-100..=500),
                    random_text(rng, "main_upsert_tag"),
                    random_text(rng, "main_upsert_note"),
                ),
                returns_rows: false,
            },
            7 => StatementSpec {
                sql: format!(
                    "UPDATE main.shared SET note = {} \
                     WHERE id IN (SELECT id FROM main.shared ORDER BY id LIMIT 2) \
                     RETURNING id, owner_conn, v, tag, note",
                    random_text(rng, "main_ret_note"),
                ),
                returns_rows: true,
            },
            _ => unreachable!(),
        }
    }

    #[turso_macros::test]
    fn temp_table_differential_fuzz(db: TempDatabase) {
        let (mut rng, seed) = helpers::init_fuzz_test("temp_table_differential_fuzz");

        let limbo_db = helpers::builder_from_db(&db)
            .with_db_name(format!("temp_tables_fuzz_{seed}.db"))
            .build();
        let sqlite_path = limbo_db.path.with_extension("sqlite");

        let num_connections = rng.random_range(2..=4);
        let iterations = helpers::fuzz_iterations(300);

        let limbo_root = limbo_db.connect_limbo();
        let sqlite_root = rusqlite::Connection::open(&sqlite_path).unwrap();

        let create_main = "CREATE TABLE main.shared(\
            id INTEGER PRIMARY KEY,\
            owner_conn INTEGER NOT NULL,\
            v INTEGER,\
            tag TEXT,\
            note TEXT NOT NULL DEFAULT 'main_note'\
        )";
        limbo_root.execute(create_main).unwrap();
        sqlite_root.execute_batch(create_main).unwrap();
        do_flush(&limbo_root, &limbo_db).unwrap();

        let mut pairs = Vec::with_capacity(num_connections);
        for conn_idx in 0..num_connections {
            let limbo = limbo_db.connect_limbo();
            let sqlite = rusqlite::Connection::open(&sqlite_path).unwrap();
            let temp_store = if rng.random_bool(0.5) {
                "MEMORY"
            } else {
                "FILE"
            };

            let pragma = format!("PRAGMA temp_store = {temp_store}");
            limbo.execute(&pragma).unwrap();
            sqlite.execute_batch(&pragma).unwrap();

            for stmt in [
                "CREATE TEMP TABLE temp_data(id INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0, tag TEXT UNIQUE, note TEXT NOT NULL DEFAULT 'temp_note')",
                "CREATE TEMP TABLE shared(id INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0, tag TEXT UNIQUE, note TEXT NOT NULL DEFAULT 'shadow_note')",
                "CREATE INDEX temp_idx_v ON temp_data(v)",
                "CREATE UNIQUE INDEX temp_idx_tag ON temp_data(tag)",
                "CREATE INDEX shadow_idx_v ON shared(v)",
                "CREATE UNIQUE INDEX shadow_idx_tag ON shared(tag)",
            ] {
                limbo.execute(stmt).unwrap();
                sqlite.execute_batch(stmt).unwrap();
            }

            for seed_row in 0..3 {
                let temp_stmt = format!(
                    "INSERT INTO temp_data(id, v, tag, note) \
                     VALUES ({}, {}, 't{}_{}', 'tn{}_{}')",
                    seed_row + 1,
                    (conn_idx as i64 * 10) + seed_row as i64,
                    conn_idx,
                    seed_row,
                    conn_idx,
                    seed_row
                );
                let shadow_stmt = format!(
                    "INSERT INTO shared(id, v, tag, note) \
                     VALUES ({}, {}, 's{}_{}', 'sn{}_{}')",
                    seed_row + 1,
                    (conn_idx as i64 * 100) + seed_row as i64,
                    conn_idx,
                    seed_row,
                    conn_idx,
                    seed_row
                );
                limbo.execute(&temp_stmt).unwrap();
                sqlite.execute_batch(&temp_stmt).unwrap();
                limbo.execute(&shadow_stmt).unwrap();
                sqlite.execute_batch(&shadow_stmt).unwrap();
            }
            do_flush(&limbo, &limbo_db).unwrap();

            pairs.push(ConnPair {
                limbo,
                sqlite,
                temp_store,
                next_temp_id: 10,
                next_shadow_id: 10,
            });

            verify_connection_views(
                conn_idx,
                pairs.last().unwrap(),
                &format!("initial setup\nseed: {seed}\nconnection: {conn_idx}"),
            );
        }

        let mut next_main_id = 1i64;
        for conn_idx in 0..num_connections {
            let stmt = format!(
                "INSERT INTO main.shared(id, owner_conn, v, tag, note) \
                 VALUES ({next_main_id}, {conn_idx}, {}, 'seed_main_{conn_idx}', 'seed_main_note_{conn_idx}')",
                rng.random_range(1..=50),
            );
            limbo_root.execute(&stmt).unwrap();
            sqlite_root.execute_batch(&stmt).unwrap();
            next_main_id += 1;
        }
        do_flush(&limbo_root, &limbo_db).unwrap();

        let mut history = vec![create_main.to_string()];

        for step in 0..iterations {
            helpers::log_progress("temp_table_differential_fuzz", step, iterations, 10);

            let conn_idx = rng.random_range(0..pairs.len());
            let op_kind = rng.random_range(0..7);
            let spec = {
                let pair = &mut pairs[conn_idx];
                match op_kind {
                    0..=2 => random_temp_dml(&mut rng, pair),
                    3..=4 => random_shadow_dml(&mut rng, pair),
                    5..=6 => random_main_dml(&mut rng, conn_idx, &mut next_main_id),
                    _ => unreachable!(),
                }
            };

            history.push(format!("conn[{conn_idx}] {}", spec.sql));
            let context = format!(
                "seed: {seed}\nstep: {step}\nconn: {conn_idx}\nhistory:\n{}",
                helpers::history_tail(&history, 50)
            );

            let pair = &pairs[conn_idx];
            let sqlite_res = execute_sqlite_statement_fallible(&pair.sqlite, &spec);
            let limbo_res = limbo_exec_rows_fallible(&limbo_db, &pair.limbo, &spec.sql);
            helpers::assert_outcome_parity(&sqlite_res, &limbo_res, &spec.sql, &context);

            if let (Ok(sqlite_rows), Ok(limbo_rows)) = (&sqlite_res, &limbo_res) {
                similar_asserts::assert_eq!(
                    Turso: limbo_rows,
                    Sqlite: sqlite_rows,
                    "statement result mismatch\n{context}\nstmt: {}",
                    spec.sql,
                );
                do_flush(&pair.limbo, &limbo_db).unwrap();
            }

            verify_connection_views(conn_idx, &pairs[conn_idx], &context);

            let observer_idx = if pairs.len() > 1 {
                let choices: Vec<usize> = (0..pairs.len()).filter(|idx| *idx != conn_idx).collect();
                *choices.choose(&mut rng).unwrap()
            } else {
                conn_idx
            };
            verify_connection_views(observer_idx, &pairs[observer_idx], &context);

            let main_query = "SELECT id, owner_conn, v, tag, note FROM main.shared ORDER BY id";
            let limbo_main = limbo_exec_rows(&pairs[observer_idx].limbo, main_query);
            let sqlite_main = sqlite_exec_rows(&pairs[observer_idx].sqlite, main_query);
            similar_asserts::assert_eq!(
                Turso: limbo_main,
                Sqlite: sqlite_main,
                "shared main-state mismatch across observer connection\n{context}\nobserver_conn: {observer_idx}",
            );
        }
    }
}
