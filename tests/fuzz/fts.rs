#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use core_tester::common::{limbo_exec_rows, TempDatabase};
    use rand::{
        seq::{IndexedRandom, IteratorRandom},
        Rng,
    };
    use rand_chacha::ChaCha8Rng;
    use rusqlite::types::Value;
    use turso_core::Connection;

    use crate::helpers;

    const TOKENS: &[&str] = &[
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
    ];

    type Model = BTreeMap<i64, Option<String>>;

    fn random_content(rng: &mut ChaCha8Rng) -> Option<String> {
        if rng.random_ratio(1, 12) {
            return None;
        }

        let count = rng.random_range(1..=4);
        let mut tokens = TOKENS
            .choose_multiple(rng, count)
            .copied()
            .collect::<Vec<_>>();
        tokens.sort_unstable();
        Some(tokens.join(" "))
    }

    fn sql_text(content: &Option<String>) -> String {
        content
            .as_ref()
            .map(|content| format!("'{content}'"))
            .unwrap_or_else(|| "NULL".to_string())
    }

    fn expected_ids(model: &Model, token: &str) -> Vec<i64> {
        model
            .iter()
            .filter_map(|(id, content)| {
                content
                    .as_deref()
                    .is_some_and(|content| content.split_ascii_whitespace().any(|t| t == token))
                    .then_some(*id)
            })
            .collect()
    }

    fn actual_ids(conn: &Arc<Connection>, token: &str) -> Vec<i64> {
        limbo_exec_rows(
            conn,
            &format!("SELECT id FROM docs WHERE fts_match(content, '{token}') ORDER BY id"),
        )
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Integer(id)] => *id,
            _ => panic!("FTS query returned malformed row: {row:?}"),
        })
        .collect()
    }

    fn assert_matches_model(
        conn: &Arc<Connection>,
        model: &Model,
        token: &str,
        seed: u64,
        history: &[String],
        connection_name: &str,
    ) {
        let expected = expected_ids(model, token);
        let actual = actual_ids(conn, token);
        assert_eq!(
            actual,
            expected,
            "FTS model mismatch on {connection_name} for token {token:?}; seed={seed}\n{}",
            helpers::history_tail(history, 40)
        );
    }

    fn execute(conn: &Arc<Connection>, sql: String, history: &mut Vec<String>) {
        conn.execute(&sql)
            .unwrap_or_else(|error| panic!("statement failed: {sql}\nerror: {error}"));
        history.push(sql);
    }

    /// Model-based FTS churn fuzzing.
    ///
    /// The vocabulary and queries are deliberately simple so the oracle does not
    /// duplicate Tantivy's query parser. The generated operation stream stresses
    /// index maintenance, transaction/savepoint rollback, cache invalidation,
    /// explicit optimization, and visibility across connections.
    #[test]
    fn fts_index_maintenance_model_fuzz() {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("fts_index_maintenance_model_fuzz");
        let db = TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .build();
        let writer = db.connect_limbo();
        let observer = db.connect_limbo();

        writer
            .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
            .unwrap();
        writer
            .execute("CREATE INDEX docs_fts ON docs USING fts(content)")
            .unwrap();

        let mut committed = Model::new();
        let mut visible = Model::new();
        let mut in_transaction = false;
        let mut next_id = 1i64;
        let mut history = vec![
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)".to_string(),
            "CREATE INDEX docs_fts ON docs USING fts(content)".to_string(),
        ];

        let iterations = helpers::fuzz_iterations(300);
        for iteration in 0..iterations {
            helpers::log_progress(
                "fts_index_maintenance_model_fuzz",
                iteration,
                iterations,
                20,
            );

            match rng.random_range(0..100) {
                0..=7 if !in_transaction => {
                    execute(&writer, "BEGIN".to_string(), &mut history);
                    in_transaction = true;
                }
                0..=3 if in_transaction => {
                    execute(&writer, "COMMIT".to_string(), &mut history);
                    committed.clone_from(&visible);
                    in_transaction = false;
                }
                4..=7 if in_transaction => {
                    execute(&writer, "ROLLBACK".to_string(), &mut history);
                    visible.clone_from(&committed);
                    in_transaction = false;
                }
                8..=37 => {
                    let content = random_content(&mut rng);
                    let sql = format!(
                        "INSERT INTO docs(id, content) VALUES ({next_id}, {})",
                        sql_text(&content)
                    );
                    execute(&writer, sql, &mut history);
                    visible.insert(next_id, content);
                    next_id += 1;
                    if !in_transaction {
                        committed.clone_from(&visible);
                    }
                }
                38..=57 if !visible.is_empty() => {
                    let id = *visible.keys().choose(&mut rng).unwrap();
                    let content = random_content(&mut rng);
                    let sql = format!(
                        "UPDATE docs SET content = {} WHERE id = {id}",
                        sql_text(&content)
                    );
                    execute(&writer, sql, &mut history);
                    visible.insert(id, content);
                    if !in_transaction {
                        committed.clone_from(&visible);
                    }
                }
                58..=69 if !visible.is_empty() => {
                    let id = *visible.keys().choose(&mut rng).unwrap();
                    execute(
                        &writer,
                        format!("DELETE FROM docs WHERE id = {id}"),
                        &mut history,
                    );
                    visible.remove(&id);
                    if !in_transaction {
                        committed.clone_from(&visible);
                    }
                }
                70..=79 => {
                    let token = TOKENS.choose(&mut rng).unwrap();
                    let content = random_content(&mut rng);
                    let sql = format!(
                        "UPDATE docs SET content = {} WHERE fts_match(content, '{token}')",
                        sql_text(&content)
                    );
                    execute(&writer, sql, &mut history);
                    for old_content in visible.values_mut() {
                        if old_content.as_deref().is_some_and(|old_content| {
                            old_content
                                .split_ascii_whitespace()
                                .any(|old_token| old_token == *token)
                        }) {
                            old_content.clone_from(&content);
                        }
                    }
                    if !in_transaction {
                        committed.clone_from(&visible);
                    }
                }
                80..=87 => {
                    let token = TOKENS.choose(&mut rng).unwrap();
                    execute(
                        &writer,
                        format!("DELETE FROM docs WHERE fts_match(content, '{token}')"),
                        &mut history,
                    );
                    visible.retain(|_, content| {
                        !content.as_deref().is_some_and(|content| {
                            content
                                .split_ascii_whitespace()
                                .any(|old_token| old_token == *token)
                        })
                    });
                    if !in_transaction {
                        committed.clone_from(&visible);
                    }
                }
                88..=94 => {
                    let before_savepoint = visible.clone();
                    execute(&writer, "SAVEPOINT fts_fuzz".to_string(), &mut history);
                    let content = random_content(&mut rng);
                    let id = next_id;
                    next_id += 1;
                    execute(
                        &writer,
                        format!(
                            "INSERT INTO docs(id, content) VALUES ({id}, {})",
                            sql_text(&content)
                        ),
                        &mut history,
                    );
                    visible.insert(id, content);

                    if rng.random_bool(0.5) {
                        execute(&writer, "ROLLBACK TO fts_fuzz".to_string(), &mut history);
                        visible = before_savepoint;
                    }
                    execute(&writer, "RELEASE fts_fuzz".to_string(), &mut history);
                    if !in_transaction {
                        committed.clone_from(&visible);
                    }
                }
                _ => {
                    execute(&writer, "OPTIMIZE INDEX docs_fts".to_string(), &mut history);
                }
            }

            let token = TOKENS.choose(&mut rng).unwrap();
            assert_matches_model(&writer, &visible, token, seed, &history, "writer");

            if iteration % 11 == 0 {
                let observer_model = if in_transaction { &committed } else { &visible };
                assert_matches_model(&observer, observer_model, token, seed, &history, "observer");
            }

            if iteration % 37 == 0 {
                for token in TOKENS {
                    assert_matches_model(&writer, &visible, token, seed, &history, "writer");
                }
            }
        }

        if in_transaction {
            execute(&writer, "ROLLBACK".to_string(), &mut history);
            visible = committed;
        }
        for token in TOKENS {
            assert_matches_model(&writer, &visible, token, seed, &history, "writer");
            assert_matches_model(&observer, &visible, token, seed, &history, "observer");
        }
    }

    /// Run a query on a corrupted store and pull integer ids out of the first
    /// column. A clean `Err` from anywhere (prepare, step, row decode) is a
    /// legitimate corruption outcome; a panic never is.
    fn try_query_ids(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<i64>> {
        let mut stmt = conn.prepare(sql)?;
        let mut ids = Vec::new();
        stmt.run_with_row_callback(|row| {
            let id = row.get::<i64>(0)?;
            ids.push(id);
            Ok(())
        })?;
        Ok(ids)
    }

    /// End-to-end corruption fuzzing: flip bits inside FTS-owned regions of a
    /// checkpointed db file, reopen, and run queries and one insert. Every
    /// statement must either succeed with model-correct results or return a
    /// clean error — never panic, never silently return wrong rows.
    ///
    /// The corpus has no deletes or updates, so no tombstone rows exist:
    /// tombstone doc ids live in row keys with no checksum, so a flip there
    /// would silently change results (a known format limitation). Everything
    /// this test flips is covered by a checksum (control/descriptor blobs),
    /// the per-file Tantivy footer crc (chunk bytes), or path/structure
    /// validation.
    #[test]
    fn fts_bit_flip_corruption_fuzz() {
        let (mut rng, seed) = helpers::init_fuzz_test_tracing("fts_bit_flip_corruption_fuzz");
        let opts = || turso_core::DatabaseOpts::new().with_index_method(true);
        let db = TempDatabase::builder().with_opts(opts()).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(content)")
            .unwrap();

        // Insert-only corpus, one statement each so several segments publish
        // (and auto-merge runs at least once before the checkpoint).
        let mut model = Model::new();
        for id in 1..=60i64 {
            let content = random_content(&mut rng);
            conn.execute(format!(
                "INSERT INTO docs(id, content) VALUES ({id}, {})",
                sql_text(&content)
            ))
            .unwrap();
            model.insert(id, content);
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
        let pristine_path = db.path.clone();
        drop(db);
        let pristine = std::fs::read(&pristine_path).unwrap();

        // Every FTS row key starts with "fts2/", so occurrences mark the
        // FTS-owned regions of the file (live rows, plus freed space from
        // merged-away segments — flips there must be harmless).
        let needle = b"fts2/";
        let regions: Vec<usize> = (0..pristine.len().saturating_sub(needle.len()))
            .filter(|&start| &pristine[start..start + needle.len()] == needle)
            .collect();
        assert!(
            !regions.is_empty(),
            "checkpointed db file must contain FTS2 rows; seed={seed}"
        );

        let iterations = helpers::fuzz_iterations(30);
        for iteration in 0..iterations {
            helpers::log_progress("fts_bit_flip_corruption_fuzz", iteration, iterations, 10);
            let mut mutated = pristine.clone();
            let mut flips = Vec::new();
            for _ in 0..rng.random_range(1..=4) {
                let region = *regions.choose(&mut rng).unwrap();
                let offset = (region + rng.random_range(0..256)).min(mutated.len() - 1);
                let bit = rng.random_range(0..8u8);
                mutated[offset] ^= 1 << bit;
                flips.push(format!("offset={offset} bit={bit}"));
            }
            let context = format!(
                "seed={seed} iteration={iteration} flips=[{}]",
                flips.join(", ")
            );

            let mutated_path = pristine_path.with_extension(format!("mut{iteration}"));
            std::fs::write(&mutated_path, &mutated).unwrap();
            // Open fallibly: rejecting the corrupted file outright is a
            // legitimate outcome, so no TempDatabase (its builder unwraps).
            let io: Arc<dyn turso_core::IO + Send> =
                Arc::new(turso_core::PlatformIO::new().unwrap());
            let Ok(reopened) = turso_core::Database::open_file_with_flags(
                io,
                mutated_path.to_str().unwrap(),
                turso_core::OpenFlags::default(),
                opts(),
                None,
                Arc::new(turso_core::SqliteDialect),
            ) else {
                let _ = std::fs::remove_file(&mutated_path);
                continue;
            };
            let Ok(conn) = reopened.connect() else {
                let _ = std::fs::remove_file(&mutated_path);
                continue;
            };

            for token in TOKENS {
                if let Ok(ids) = try_query_ids(
                    &conn,
                    &format!("SELECT id FROM docs WHERE fts_match(content, '{token}') ORDER BY id"),
                ) {
                    assert_eq!(
                        ids,
                        expected_ids(&model, token),
                        "corrupted store silently returned wrong rows for {token:?}; {context}"
                    );
                }
            }

            // One score query: scores have no oracle, but the matched id set
            // still must be model-correct when the query succeeds.
            let token = TOKENS.choose(&mut rng).unwrap();
            if let Ok(ids) = try_query_ids(
                &conn,
                &format!(
                    "SELECT id, fts_score(content, '{token}') FROM docs \
                     WHERE fts_match(content, '{token}') ORDER BY id"
                ),
            ) {
                assert_eq!(
                    ids,
                    expected_ids(&model, token),
                    "corrupted store silently returned wrong scored rows for {token:?}; {context}"
                );
            }

            // One insert. Writes are allowed one extra outcome queries are
            // not: a B-tree write that detects a corrupted page stops with
            // an invariant assert instead of returning an error (the
            // crash-don't-corrupt policy; queries above must still never
            // panic). What a write must never do is claim success and then
            // serve wrong rows — so a successful insert is verified.
            let insert_id = 100_000 + iteration as i64;
            let insert_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                conn.execute(format!(
                    "INSERT INTO docs(id, content) VALUES ({insert_id}, 'alpha bravo')"
                ))
            }));
            if let Ok(Ok(_)) = insert_result {
                let mut model_after = model.clone();
                model_after.insert(insert_id, Some("alpha bravo".to_string()));
                if let Ok(ids) = try_query_ids(
                    &conn,
                    "SELECT id FROM docs WHERE fts_match(content, 'alpha') ORDER BY id",
                ) {
                    assert_eq!(
                        ids,
                        expected_ids(&model_after, "alpha"),
                        "post-insert query returned wrong rows; {context}"
                    );
                }
            }

            drop(conn);
            drop(reopened);
            let _ = std::fs::remove_file(&mutated_path);
            let _ = std::fs::remove_file(format!("{}-wal", mutated_path.display()));
        }
    }
}
