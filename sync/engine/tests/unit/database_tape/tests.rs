use std::sync::Arc;
use turso_core::SqliteDialect;

use tempfile::NamedTempFile;

use crate::{
    database_tape::{
        run_stmt_once, DatabaseChangesIteratorOpts, DatabaseReplaySessionOpts, DatabaseTape,
    },
    types::{
        Coro, DatabaseSchemaKind, DatabaseSchemaReplay, DatabaseStatementReplay,
        DatabaseTapeOperation, DatabaseTapeRowChange, DatabaseTapeRowChangeType,
    },
};

#[test]
pub fn test_database_tape_connect() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));
    let mut gen = genawaiter::sync::Gen::new({
        let db1 = db1.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db1.connect(&coro).await.unwrap();
            let mut stmt = conn.prepare("SELECT * FROM turso_cdc").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    assert_eq!(rows, vec![] as Vec<Vec<turso_core::Value>>);
}

#[test]
pub fn test_database_tape_stmt_replay_allows_zero_bind_dml() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(x)").unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db.start_replay_session(&coro, opts).await.unwrap();
                session
                    .replay(
                        &coro,
                        DatabaseTapeOperation::StmtReplay(DatabaseStatementReplay {
                            sql: "INSERT INTO t VALUES (42)".to_string(),
                            values: Vec::new(),
                        }),
                    )
                    .await
                    .unwrap();
                session
                    .replay(&coro, DatabaseTapeOperation::Commit)
                    .await
                    .unwrap();
            }
            let mut stmt = conn.prepare("SELECT x FROM t").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    assert_eq!(rows, vec![vec![turso_core::Value::from_i64(42)]]);
}

#[test]
pub fn test_schema_refresh_create_table_is_idempotent() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)")
                .unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db.start_replay_session(&coro, opts).await.unwrap();
                session
                    .replay(
                        &coro,
                        DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Refresh {
                            kind: DatabaseSchemaKind::Table,
                            name: "t".to_string(),
                            sql: "CREATE TABLE t(x INTEGER PRIMARY KEY, note TEXT)".to_string(),
                        }),
                    )
                    .await
                    .unwrap();
                session
                    .replay(
                        &coro,
                        DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Create {
                            sql: "CREATE INDEX t_note_idx ON t(note)".to_string(),
                        }),
                    )
                    .await
                    .unwrap();
                session
                    .replay(&coro, DatabaseTapeOperation::Commit)
                    .await
                    .unwrap();
            }
        }
    });
    while let genawaiter::GeneratorState::Yielded(..) = gen.resume_with(Ok(())) {
        io.step().unwrap()
    }
}

#[test]
pub fn test_implicit_rowid_replay_upserts_primary_key_rows() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
                .unwrap();
            conn.execute("INSERT INTO t(id, value) VALUES (1, 'old')")
                .unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: true,
                };
                let mut session = db.start_replay_session(&coro, opts).await.unwrap();
                session
                    .replay(
                        &coro,
                        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                            change_id: 1,
                            change_time: 1,
                            table_name: "t".to_string(),
                            id: 1,
                            change: DatabaseTapeRowChangeType::Insert {
                                after: crate::alloc::vec![
                                    turso_core::Value::Null,
                                    turso_core::Value::build_text("new"),
                                ],
                            },
                        }),
                    )
                    .await
                    .unwrap();
                session
                    .replay(&coro, DatabaseTapeOperation::Commit)
                    .await
                    .unwrap();
            }
            let mut stmt = conn.prepare("SELECT id, value FROM t ORDER BY id").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::from_i64(1),
            turso_core::Value::build_text("new")
        ]]
    );
}

#[test]
pub fn test_implicit_rowid_replay_prefers_explicit_primary_key() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(x TEXT PRIMARY KEY, value TEXT)")
                .unwrap();
            conn.execute("INSERT INTO t(rowid, x, value) VALUES (4, 'remote', 'kept')")
                .unwrap();
            conn.execute("INSERT INTO t(rowid, x, value) VALUES (5, 'local', 'old')")
                .unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: true,
                };
                let mut session = db.start_replay_session(&coro, opts).await.unwrap();
                session
                    .replay(
                        &coro,
                        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                            change_id: 1,
                            change_time: 1,
                            table_name: "t".to_string(),
                            id: 4,
                            change: DatabaseTapeRowChangeType::Insert {
                                after: crate::alloc::vec![
                                    turso_core::Value::build_text("local"),
                                    turso_core::Value::build_text("new"),
                                ],
                            },
                        }),
                    )
                    .await
                    .unwrap();
                session
                    .replay(&coro, DatabaseTapeOperation::Commit)
                    .await
                    .unwrap();
            }
            let mut stmt = conn
                .prepare("SELECT rowid, x, value FROM t ORDER BY x")
                .unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    assert_eq!(
        rows,
        vec![
            vec![
                turso_core::Value::from_i64(5),
                turso_core::Value::build_text("local"),
                turso_core::Value::build_text("new")
            ],
            vec![
                turso_core::Value::from_i64(4),
                turso_core::Value::build_text("remote"),
                turso_core::Value::build_text("kept")
            ],
        ]
    );
}

#[test]
pub fn test_database_tape_replay_composite_primary_key() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE z(x TEXT, y TEXT, payload TEXT, PRIMARY KEY(y, x))")
                .unwrap();
            conn.execute("INSERT INTO z VALUES ('1', '2', 'old'), ('4', '2', 'untouched')")
                .unwrap();

            let opts = DatabaseReplaySessionOpts {
                use_implicit_rowid: false,
            };
            let mut session = db.start_replay_session(&coro, opts).await.unwrap();
            session
                .replay(
                    &coro,
                    DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                        change_id: 1,
                        change_time: 1,
                        table_name: "z".to_string(),
                        id: 1,
                        change: DatabaseTapeRowChangeType::Insert {
                            after: crate::alloc::vec![
                                turso_core::Value::build_text("1"),
                                turso_core::Value::build_text("2"),
                                turso_core::Value::build_text("inserted"),
                            ],
                        },
                    }),
                )
                .await
                .unwrap();
            session
                .replay(
                    &coro,
                    DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                        change_id: 2,
                        change_time: 2,
                        table_name: "z".to_string(),
                        id: 1,
                        change: DatabaseTapeRowChangeType::Update {
                            before: crate::alloc::vec![
                                turso_core::Value::build_text("1"),
                                turso_core::Value::build_text("2"),
                                turso_core::Value::build_text("inserted"),
                            ],
                            after: crate::alloc::vec![
                                turso_core::Value::build_text("1"),
                                turso_core::Value::build_text("2"),
                                turso_core::Value::build_text("updated"),
                            ],
                            updates: Some(crate::alloc::vec![
                                turso_core::Value::from_i64(0),
                                turso_core::Value::from_i64(0),
                                turso_core::Value::from_i64(1),
                                turso_core::Value::Null,
                                turso_core::Value::Null,
                                turso_core::Value::build_text("updated"),
                            ]),
                        },
                    }),
                )
                .await
                .unwrap();
            session
                .replay(
                    &coro,
                    DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                        change_id: 3,
                        change_time: 3,
                        table_name: "z".to_string(),
                        id: 1,
                        change: DatabaseTapeRowChangeType::Delete {
                            before: crate::alloc::vec![
                                turso_core::Value::build_text("1"),
                                turso_core::Value::build_text("2"),
                                turso_core::Value::build_text("updated"),
                            ],
                            key: None,
                        },
                    }),
                )
                .await
                .unwrap();
            session
                .replay(&coro, DatabaseTapeOperation::Commit)
                .await
                .unwrap();

            let mut stmt = conn
                .prepare("SELECT x, y, payload FROM z ORDER BY x")
                .unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::build_text("4"),
            turso_core::Value::build_text("2"),
            turso_core::Value::build_text("untouched"),
        ]]
    );
}

#[test]
pub fn test_database_tape_replay_delete_key_rules() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE q(x TEXT PRIMARY KEY, y TEXT UNIQUE, z TEXT UNIQUE)")
                .unwrap();
            conn.execute(
                "INSERT INTO q(rowid, x, y, z) VALUES
                        (7, '1', '2', '3'),
                        (8, '4', '5', '6')",
            )
            .unwrap();
            conn.execute("CREATE TABLE nopk(a TEXT, b TEXT)").unwrap();
            conn.execute("INSERT INTO nopk(rowid, a, b) VALUES (3, 'r3', 'v3')")
                .unwrap();

            let opts = DatabaseReplaySessionOpts {
                use_implicit_rowid: true,
            };
            let mut session = db.start_replay_session(&coro, opts).await.unwrap();
            session
                .replay(
                    &coro,
                    DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                        change_id: 1,
                        change_time: 1,
                        table_name: "q".to_string(),
                        // The remote rowid can differ after an earlier PK upsert.
                        // The portable primary-key projection must win.
                        id: 99,
                        change: DatabaseTapeRowChangeType::Delete {
                            before: crate::alloc::vec![],
                            key: Some(crate::alloc::vec![turso_core::Value::build_text("1")]),
                        },
                    }),
                )
                .await
                .unwrap();
            // A delete without projection or before image on a table whose
            // PRIMARY KEY is not the rowid must be refused: the local
            // rowid may not match the remote's, so a rowid-based delete
            // could remove the wrong row.
            let refused = session
                .replay(
                    &coro,
                    DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                        change_id: 2,
                        change_time: 2,
                        table_name: "q".to_string(),
                        id: 8,
                        change: DatabaseTapeRowChangeType::Delete {
                            before: crate::alloc::vec![],
                            key: None,
                        },
                    }),
                )
                .await;
            let err = format!("{:?}", refused.expect_err("rowid fallback must be refused"));
            assert!(
                err.contains("refusing rowid-based replay"),
                "unexpected error for refused rowid fallback: {err}"
            );
            // Tables with no PRIMARY KEY have the rowid as their only
            // identity: the fallback is exact and stays allowed.
            session
                .replay(
                    &coro,
                    DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                        change_id: 3,
                        change_time: 3,
                        table_name: "nopk".to_string(),
                        id: 3,
                        change: DatabaseTapeRowChangeType::Delete {
                            before: crate::alloc::vec![],
                            key: None,
                        },
                    }),
                )
                .await
                .unwrap();
            session
                .replay(&coro, DatabaseTapeOperation::Commit)
                .await
                .unwrap();

            let mut stmt = conn.prepare("SELECT x FROM q ORDER BY x").unwrap();
            let mut q_rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                q_rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            let mut stmt = conn.prepare("SELECT a FROM nopk").unwrap();
            let nopk_empty = run_stmt_once(&coro, &mut stmt).await.unwrap().is_none();
            (q_rows, nopk_empty)
        }
    });
    let (q_rows, nopk_empty) = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    // The key-based delete removed x='1'; the refused rowid delete left
    // x='4' in place; the no-PK rowid delete emptied nopk.
    assert_eq!(q_rows, vec![vec![turso_core::Value::build_text("4")]]);
    assert!(nopk_empty);
}

#[test]
pub fn test_database_tape_iterate_changes() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let mut gen = genawaiter::sync::Gen::new({
        let db1 = db1.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db1.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(x)").unwrap();
            conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
            let opts = Default::default();
            let mut iterator = db1.iterate_changes(opts).unwrap();
            let mut changes = Vec::new();
            while let Some(change) = iterator.next(&coro).await.unwrap() {
                changes.push(change);
            }
            changes
        }
    });
    let changes = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    tracing::info!("changes: {:?}", changes);
    assert_eq!(changes.len(), 5);
    // CREATE TABLE emits a COMMIT record (schema INSERT is filtered by ignore_schema_changes)
    assert!(matches!(changes[0], DatabaseTapeOperation::Commit));
    assert!(matches!(
        changes[1],
        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
            change_id: 3,
            id: 1,
            ref table_name,
            change: DatabaseTapeRowChangeType::Insert { .. },
            ..
        }) if table_name == "t"
    ));
    assert!(matches!(
        changes[2],
        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
            change_id: 4,
            id: 2,
            ref table_name,
            change: DatabaseTapeRowChangeType::Insert { .. },
            ..
        }) if table_name == "t"
    ));
    assert!(matches!(
        changes[3],
        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
            change_id: 5,
            id: 3,
            ref table_name,
            change: DatabaseTapeRowChangeType::Insert { .. },
            ..
        }) if table_name == "t"
    ));
    assert!(matches!(changes[4], DatabaseTapeOperation::Commit));
}

#[test]
pub fn test_database_tape_iterate_changes_in_mvcc_mode() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    db1.connect()
        .unwrap()
        .execute("PRAGMA journal_mode = 'mvcc'")
        .unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let mut gen = genawaiter::sync::Gen::new({
        let db1 = db1.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db1.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(x)").unwrap();
            conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
            let opts = Default::default();
            let mut iterator = db1.iterate_changes(opts).unwrap();
            let mut changes = Vec::new();
            while let Some(change) = iterator.next(&coro).await.unwrap() {
                changes.push(change);
            }
            changes
        }
    });
    let changes = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };
    tracing::info!("changes: {:?}", changes);
    assert_eq!(changes.len(), 5);
    assert!(matches!(changes[0], DatabaseTapeOperation::Commit));
    assert!(matches!(
        changes[1],
        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
            change_id: 3,
            id: 1,
            ref table_name,
            change: DatabaseTapeRowChangeType::Insert { .. },
            ..
        }) if table_name == "t"
    ));
    assert!(matches!(
        changes[2],
        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
            change_id: 4,
            id: 2,
            ref table_name,
            change: DatabaseTapeRowChangeType::Insert { .. },
            ..
        }) if table_name == "t"
    ));
    assert!(matches!(
        changes[3],
        DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
            change_id: 5,
            id: 3,
            ref table_name,
            change: DatabaseTapeRowChangeType::Insert { .. },
            ..
        }) if table_name == "t"
    ));
    assert!(matches!(changes[4], DatabaseTapeOperation::Commit));
}

/// in MVCC mode the CDC `change_id` is drawn from the CDC
/// table's AUTOINCREMENT sequence, so ids are never reused after CDC rows are
/// pruned, and `read_cdc_sequence_watermark` reports the exclusive safe upper
/// bound the push loop scans up to. Bounding the scan by that watermark is
/// what stops the push loop from skipping a change id a concurrent
/// transaction commits below the current max under snapshot isolation.
#[test]
pub fn test_mvcc_cdc_change_id_sequence_backed_and_watermark_bounds_scan() {
    fn row_change_ids(changes: &[DatabaseTapeOperation]) -> Vec<i64> {
        changes
            .iter()
            .filter_map(|change| match change {
                DatabaseTapeOperation::RowChange(change) => Some(change.change_id),
                _ => None,
            })
            .collect()
    }

    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    db1.connect()
        .unwrap()
        .execute("PRAGMA journal_mode = 'mvcc'")
        .unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let mut gen = genawaiter::sync::Gen::new({
        let db1 = db1.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db1.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(x)").unwrap();
            conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

            // No in-flight allocations: watermark == max(change_id) + 1.
            let watermark = crate::database_sync_operations::read_cdc_sequence_watermark(
                &coro,
                &conn,
                db1.cdc_table(),
            )
            .await
            .unwrap();

            let mut opts = DatabaseChangesIteratorOpts {
                ignore_schema_changes: false,
                ..Default::default()
            };
            let mut unbounded = Vec::new();
            let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
            while let Some(change) = iterator.next(&coro).await.unwrap() {
                unbounded.push(change);
            }

            // Bounded scan stops strictly below the bound.
            opts.max_change_id_exclusive = Some(4);
            let mut bounded = Vec::new();
            let mut iterator = db1.iterate_changes(opts).unwrap();
            while let Some(change) = iterator.next(&coro).await.unwrap() {
                bounded.push(change);
            }

            // Prune the CDC table, then write again: the new change id must
            // continue past the old high-water mark, not reuse a low id.
            conn.execute("DELETE FROM turso_cdc").unwrap();
            conn.execute("INSERT INTO t VALUES (4)").unwrap();
            let mut after_prune = Vec::new();
            let mut iterator = db1
                .iterate_changes(DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                })
                .unwrap();
            while let Some(change) = iterator.next(&coro).await.unwrap() {
                after_prune.push(change);
            }

            (watermark, unbounded, bounded, after_prune)
        }
    });
    let (watermark, unbounded, bounded, after_prune) = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };

    // CREATE TABLE row (change_id 1) + COMMIT (2) + three inserts (3,4,5) +
    // COMMIT (6). Watermark is the first unallocated id: 7.
    assert_eq!(watermark, Some(7));
    assert_eq!(row_change_ids(&unbounded), vec![1, 3, 4, 5]);
    // Bound of 4 keeps only change ids < 4 (the schema row 1 and insert 3).
    assert_eq!(row_change_ids(&bounded), vec![1, 3]);
    // After pruning, the reinserted row's change id continues at 7 (the old
    // watermark), never reusing an id at or below the previously pushed max.
    assert_eq!(row_change_ids(&after_prune), vec![7]);
}

#[test]
pub fn test_database_tape_replay_changes_preserve_rowid() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        let db1 = db1.clone();
        let db2 = db2.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1.execute("CREATE TABLE t(x)").unwrap();
            conn1
                .execute("INSERT INTO t(rowid, x) VALUES (10, 1), (20, 2)")
                .unwrap();
            let conn2 = db2.connect(&coro).await.unwrap();
            conn2.execute("CREATE TABLE t(x)").unwrap();
            conn2
                .execute("INSERT INTO t(rowid, x) VALUES (1, -1), (2, -2)")
                .unwrap();

            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: true,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();
                let opts = Default::default();
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut stmt = conn2.prepare("SELECT rowid, x FROM t").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(rows) => break rows,
        }
    };
    tracing::info!("rows: {:?}", rows);
    assert_eq!(
        rows,
        vec![
            vec![
                turso_core::Value::from_i64(1),
                turso_core::Value::from_i64(-1)
            ],
            vec![
                turso_core::Value::from_i64(2),
                turso_core::Value::from_i64(-2)
            ],
            vec![
                turso_core::Value::from_i64(10),
                turso_core::Value::from_i64(1)
            ],
            vec![
                turso_core::Value::from_i64(20),
                turso_core::Value::from_i64(2)
            ]
        ]
    );
}

#[test]
pub fn test_database_tape_replay_changes_do_not_preserve_rowid() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        let db1 = db1.clone();
        let db2 = db2.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1.execute("CREATE TABLE t(x)").unwrap();
            conn1
                .execute("INSERT INTO t(rowid, x) VALUES (10, 1), (20, 2)")
                .unwrap();
            let conn2 = db2.connect(&coro).await.unwrap();
            conn2.execute("CREATE TABLE t(x)").unwrap();
            conn2
                .execute("INSERT INTO t(rowid, x) VALUES (1, -1), (2, -2)")
                .unwrap();

            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();
                let opts = Default::default();
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut stmt = conn2.prepare("SELECT rowid, x FROM t").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(rows) => break rows,
        }
    };
    tracing::info!("rows: {:?}", rows);
    assert_eq!(
        rows,
        vec![
            vec![
                turso_core::Value::from_i64(1),
                turso_core::Value::from_i64(-1)
            ],
            vec![
                turso_core::Value::from_i64(2),
                turso_core::Value::from_i64(-2)
            ],
            vec![
                turso_core::Value::from_i64(3),
                turso_core::Value::from_i64(1)
            ],
            vec![
                turso_core::Value::from_i64(4),
                turso_core::Value::from_i64(2)
            ]
        ]
    );
}

#[test]
pub fn test_database_tape_replay_changes_delete() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        let db1 = db1.clone();
        let db2 = db2.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1.execute("CREATE TABLE t(x TEXT PRIMARY KEY)").unwrap();
            conn1.execute("INSERT INTO t(x) VALUES ('a')").unwrap();
            conn1.execute("DELETE FROM t").unwrap();
            let conn2 = db2.connect(&coro).await.unwrap();
            conn2.execute("CREATE TABLE t(x TEXT PRIMARY KEY)").unwrap();
            conn2.execute("INSERT INTO t(x) VALUES ('b')").unwrap();

            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();
                let opts = Default::default();
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut stmt = conn2.prepare("SELECT rowid, x FROM t").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(rows) => break rows,
        }
    };
    tracing::info!("rows: {:?}", rows);
    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::from_i64(1),
            turso_core::Value::Text(turso_core::types::Text::new("b"))
        ]]
    );
}

#[test]
pub fn test_database_tape_replay_schema_changes() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();
    let temp_file3 = NamedTempFile::new().unwrap();
    let db_path3 = temp_file3.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let db3 =
        turso_core::Database::open_file(io.clone(), db_path3, Arc::new(SqliteDialect)).unwrap();
    let db3 = Arc::new(DatabaseTape::new(db3));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y)")
                .unwrap();
            conn1
                .execute("INSERT INTO t(x, y) VALUES ('a', 10)")
                .unwrap();
            let conn2 = db2.connect(&coro).await.unwrap();
            conn2
                .execute("CREATE TABLE q(x TEXT PRIMARY KEY, y)")
                .unwrap();
            conn2
                .execute("INSERT INTO q(x, y) VALUES ('b', 20)")
                .unwrap();

            let conn3 = db3.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db3.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
                let mut iterator = db2.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut rows = Vec::new();
            let mut stmt = conn3.prepare("SELECT rowid, x, y FROM t").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![vec![
                    turso_core::Value::from_i64(1),
                    turso_core::Value::Text(turso_core::types::Text::new("a")),
                    turso_core::Value::from_i64(10),
                ]]
            );

            let mut rows = Vec::new();
            let mut stmt = conn3.prepare("SELECT rowid, x, y FROM q").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![vec![
                    turso_core::Value::from_i64(1),
                    turso_core::Value::Text(turso_core::types::Text::new("b")),
                    turso_core::Value::from_i64(20),
                ]]
            );
            let mut rows = Vec::new();
            let mut stmt = conn3
                .prepare(
                    // Exclude sequence backing tables created implicitly
                    // for AUTOINCREMENT (e.g. for turso_cdc) so this test
                    // remains focused on user-created tables.
                    "SELECT * FROM sqlite_schema \
                         WHERE name NOT IN ('turso_cdc', 'turso_cdc_version') \
                         AND name NOT LIKE '\\_\\_turso\\_internal\\_seq\\_%' ESCAPE '\\' \
                         AND type = 'table'",
                )
                .unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("table")),
                        turso_core::Value::Text(turso_core::types::Text::new("sqlite_sequence")),
                        turso_core::Value::Text(turso_core::types::Text::new("sqlite_sequence")),
                        turso_core::Value::from_i64(2),
                        turso_core::Value::Text(turso_core::types::Text::new(
                            "CREATE TABLE sqlite_sequence(name,seq)"
                        )),
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("table")),
                        turso_core::Value::Text(turso_core::types::Text::new("t")),
                        turso_core::Value::Text(turso_core::types::Text::new("t")),
                        turso_core::Value::from_i64(7),
                        turso_core::Value::Text(turso_core::types::Text::new(
                            "CREATE TABLE t (x TEXT PRIMARY KEY, y)"
                        )),
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("table")),
                        turso_core::Value::Text(turso_core::types::Text::new("q")),
                        turso_core::Value::Text(turso_core::types::Text::new("q")),
                        turso_core::Value::from_i64(9),
                        turso_core::Value::Text(turso_core::types::Text::new(
                            "CREATE TABLE q (x TEXT PRIMARY KEY, y)"
                        )),
                    ]
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

#[test]
pub fn test_database_tape_replay_create_index() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y)")
                .unwrap();
            conn1.execute("CREATE INDEX t_idx ON t(y)").unwrap();

            let conn2 = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut rows = Vec::new();
            let mut stmt = conn2
                .prepare("SELECT * FROM sqlite_schema WHERE name IN ('t', 't_idx')")
                .unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("table")),
                        turso_core::Value::Text(turso_core::types::Text::new("t")),
                        turso_core::Value::Text(turso_core::types::Text::new("t")),
                        turso_core::Value::from_i64(7),
                        turso_core::Value::Text(turso_core::types::Text::new(
                            "CREATE TABLE t (x TEXT PRIMARY KEY, y)"
                        )),
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("index")),
                        turso_core::Value::Text(turso_core::types::Text::new("t_idx")),
                        turso_core::Value::Text(turso_core::types::Text::new("t")),
                        turso_core::Value::from_i64(9),
                        turso_core::Value::Text(turso_core::types::Text::new(
                            "CREATE INDEX t_idx ON t (y)"
                        )),
                    ]
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

#[test]
pub fn test_database_tape_replay_quoted_create_index_idempotent() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")
                .unwrap();

            let opts = DatabaseReplaySessionOpts {
                use_implicit_rowid: false,
            };
            let mut session = db.start_replay_session(&coro, opts).await.unwrap();
            let sql = "CREATE INDEX \"t remote mixed idx 93136628163651980\" ON t(payload)";
            session
                .replay(
                    &coro,
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Create {
                        sql: sql.to_string(),
                    }),
                )
                .await
                .unwrap();
            session
                .replay(
                    &coro,
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Create {
                        sql: sql.to_string(),
                    }),
                )
                .await
                .unwrap();
            session
                .replay(&coro, DatabaseTapeOperation::Commit)
                .await
                .unwrap();

            let mut stmt = conn
                .prepare("SELECT type, name, sql FROM sqlite_schema ORDER BY type, name")
                .unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert!(
                rows.iter().any(|row| row
                    == &vec![
                        turso_core::Value::build_text("index"),
                        turso_core::Value::build_text("t remote mixed idx 93136628163651980"),
                        turso_core::Value::build_text(
                            "CREATE INDEX \"t remote mixed idx 93136628163651980\" ON t (payload)"
                        ),
                    ]),
                "quoted index schema row missing; rows={rows:?}"
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

#[test]
pub fn test_database_tape_replay_alter_table() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y)")
                .unwrap();
            conn1.execute("ALTER TABLE t ADD COLUMN z").unwrap();
            conn1.execute("ALTER TABLE t DROP COLUMN y").unwrap();

            let conn2 = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut rows = Vec::new();
            let mut stmt = conn2
                .prepare("SELECT * FROM sqlite_schema WHERE name IN ('t')")
                .unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![vec![
                    turso_core::Value::Text(turso_core::types::Text::new("table")),
                    turso_core::Value::Text(turso_core::types::Text::new("t")),
                    turso_core::Value::Text(turso_core::types::Text::new("t")),
                    turso_core::Value::from_i64(7),
                    turso_core::Value::Text(turso_core::types::Text::new(
                        "CREATE TABLE t (x TEXT PRIMARY KEY, z)"
                    )),
                ]]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

#[test]
pub fn test_database_tape_replay_non_overlapping_updates() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();
    let temp_file3 = NamedTempFile::new().unwrap();
    let db_path3 = temp_file3.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let db3 =
        turso_core::Database::open_file(io.clone(), db_path3, Arc::new(SqliteDialect)).unwrap();
    let db3 = Arc::new(DatabaseTape::new(db3));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('turso', 1, 2)")
                .unwrap();
            conn1
                .execute("UPDATE t SET y = 10 WHERE x = 'turso'")
                .unwrap();

            let conn2 = db2.connect_untracked().unwrap();
            conn2
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                .unwrap();
            conn2
                .execute("INSERT INTO t VALUES ('turso', 1, 2)")
                .unwrap();

            let conn2 = db2.connect(&coro).await.unwrap();
            conn2
                .execute("UPDATE t SET z = 20 WHERE x = 'turso'")
                .unwrap();

            let conn3 = db3.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db3.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }

                let mut iterator = db2.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut rows = Vec::new();
            let mut stmt = conn3.prepare("SELECT * FROM t").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![vec![
                    turso_core::Value::Text(turso_core::types::Text::new("turso")),
                    turso_core::Value::from_i64(10),
                    turso_core::Value::from_i64(20),
                ]]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

#[test]
pub fn test_database_tape_replay_ddl_changes_idempotent() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();
    let temp_file3 = NamedTempFile::new().unwrap();
    let db_path3 = temp_file3.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let db3 =
        turso_core::Database::open_file(io.clone(), db_path3, Arc::new(SqliteDialect)).unwrap();
    let db3 = Arc::new(DatabaseTape::new(db3));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                .unwrap();
            conn1.execute("CREATE INDEX t_idx ON t(y, z)").unwrap();

            let conn2 = db2.connect(&coro).await.unwrap();
            conn2
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                .unwrap();
            conn2.execute("CREATE INDEX t_idx ON t(y, z)").unwrap();

            let conn3 = db3.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db3.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    tracing::info!("1. operation: {:?}", operation);
                    session.replay(&coro, operation).await.unwrap();
                }

                let mut iterator = db2.iterate_changes(opts.clone()).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    tracing::info!("2. operation: {:?}", operation);
                    session.replay(&coro, operation).await.unwrap();
                }
            }
            let mut rows = Vec::new();
            let mut stmt = conn3.prepare("SELECT name FROM sqlite_master").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_value(0).to_text().unwrap().to_string());
            }
            assert_eq!(
                rows,
                vec![
                    "sqlite_sequence".to_string(),
                    "turso_cdc".to_string(),
                    // Implicit AUTOINCREMENT backing table for turso_cdc.
                    "__turso_internal_seq___turso_internal_autoincrement_turso_cdc".to_string(),
                    "turso_cdc_version".to_string(),
                    "sqlite_autoindex_turso_cdc_version_1".to_string(),
                    "t".to_string(),
                    "sqlite_autoindex_t_1".to_string(),
                    "t_idx".to_string()
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

// Tests for the "explicitly list table columns in the replay generator" commit.
// These test that CDC records captured before ALTER TABLE ADD COLUMN can be
// correctly replayed into a schema that has the extra column.

/// Bootstrap from empty: CREATE TABLE → INSERT → ALTER TABLE ADD COLUMN → INSERT.
/// Target DB starts empty and receives all changes (including DDL) via replay.
/// Verifies schema has new column and all data rows are correct.
#[test]
pub fn test_database_tape_replay_alter_table_add_column_after_inserts() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('a', 'alpha')")
                .unwrap();
            conn1.execute("INSERT INTO t VALUES ('b', 'beta')").unwrap();
            conn1
                .execute("ALTER TABLE t ADD COLUMN z TEXT DEFAULT NULL")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('c', 'gamma', 'extra')")
                .unwrap();

            let conn2 = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }

            // Verify schema
            let mut stmt = conn2
                .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
                .unwrap();
            let row = run_stmt_once(&coro, &mut stmt).await.unwrap().unwrap();
            let sql = row.get_value(0).to_text().unwrap().to_string();
            assert!(
                sql.contains("z"),
                "schema should contain z column after ALTER TABLE: {sql}"
            );

            // Verify data
            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y, z FROM t ORDER BY x").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("a")),
                        turso_core::Value::Text(turso_core::types::Text::new("alpha")),
                        turso_core::Value::Null,
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("b")),
                        turso_core::Value::Text(turso_core::types::Text::new("beta")),
                        turso_core::Value::Null,
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("c")),
                        turso_core::Value::Text(turso_core::types::Text::new("gamma")),
                        turso_core::Value::Text(turso_core::types::Text::new("extra")),
                    ],
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// Pre-ALTER INSERT records replayed into a target that already has the post-ALTER schema.
/// Source: CREATE TABLE t(x PK, y) → INSERT 2 rows (2 cols each).
/// Target: already has t(x PK, y, z) — the post-ALTER schema.
/// Replay with ignore_schema_changes: true (data only).
/// Without the fix, INSERT INTO t VALUES (?,?) fails on a 3-column table.
/// With the fix, INSERT INTO t(x, y) VALUES (?,?) works.
#[test]
pub fn test_database_tape_replay_pre_alter_inserts_into_post_alter_schema() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            // Source: pre-ALTER schema with 2 columns
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('a', 'alpha')")
                .unwrap();
            conn1.execute("INSERT INTO t VALUES ('b', 'beta')").unwrap();

            // Target: post-ALTER schema with 3 columns (set up without CDC)
            let conn2 = db2.connect_untracked().unwrap();
            conn2
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT, z TEXT DEFAULT NULL)")
                .unwrap();

            let _conn2_tracked = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: true,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }

            // Verify data — pre-ALTER rows should have NULL for the new column
            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y, z FROM t ORDER BY x").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("a")),
                        turso_core::Value::Text(turso_core::types::Text::new("alpha")),
                        turso_core::Value::Null,
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("b")),
                        turso_core::Value::Text(turso_core::types::Text::new("beta")),
                        turso_core::Value::Null,
                    ],
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// Pre-ALTER UPDATE records replayed into post-ALTER target schema.
/// Source: CREATE TABLE t(x PK, y) → INSERT → UPDATE y.
/// Target: already has t(x PK, y, z) — the post-ALTER schema with existing data.
/// Replay with ignore_schema_changes: true (data only).
/// Without the fix, update_query indexes out of bounds into the `columns` bool slice.
/// With the fix, only the columns present in the CDC record are referenced.
#[test]
pub fn test_database_tape_replay_pre_alter_updates_into_post_alter_schema() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            // Source: pre-ALTER schema — insert + update
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('a', 'alpha')")
                .unwrap();
            conn1
                .execute("UPDATE t SET y = 'ALPHA' WHERE x = 'a'")
                .unwrap();

            // Target: post-ALTER schema with the row already present
            let conn2 = db2.connect_untracked().unwrap();
            conn2
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT, z TEXT DEFAULT NULL)")
                .unwrap();
            conn2
                .execute("INSERT INTO t VALUES ('a', 'alpha', 'z-val')")
                .unwrap();

            let _conn2_tracked = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: true,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }

            // Verify: y should be updated to 'ALPHA', z should stay 'z-val'
            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y, z FROM t ORDER BY x").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![vec![
                    turso_core::Value::Text(turso_core::types::Text::new("a")),
                    turso_core::Value::Text(turso_core::types::Text::new("ALPHA")),
                    turso_core::Value::Text(turso_core::types::Text::new("z-val")),
                ]]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// Mixed pre-ALTER and post-ALTER CDC records replayed into post-ALTER target.
/// Source: CREATE TABLE → INSERT (2 cols) → ALTER TABLE ADD COLUMN → INSERT (3 cols) → UPDATE (3 cols).
/// Target: already has post-ALTER schema. Replay data only.
/// Tests that both pre-ALTER (2-col) and post-ALTER (3-col) records work correctly.
#[test]
pub fn test_database_tape_replay_mixed_pre_and_post_alter_records() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn1 = db1.connect(&coro).await.unwrap();
            // Pre-ALTER: 2 columns
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('a', 'alpha')")
                .unwrap();
            // ALTER TABLE — adds z column
            conn1
                .execute("ALTER TABLE t ADD COLUMN z TEXT DEFAULT NULL")
                .unwrap();
            // Post-ALTER: 3 columns
            conn1
                .execute("INSERT INTO t VALUES ('b', 'beta', 'b-extra')")
                .unwrap();
            conn1
                .execute("UPDATE t SET z = 'a-extra' WHERE x = 'a'")
                .unwrap();

            // Target: post-ALTER schema (set up without CDC tracking)
            let conn2 = db2.connect_untracked().unwrap();
            conn2
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT, z TEXT DEFAULT NULL)")
                .unwrap();

            let _conn2_tracked = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: true,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }

            // Verify all rows
            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y, z FROM t ORDER BY x").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("a")),
                        turso_core::Value::Text(turso_core::types::Text::new("alpha")),
                        turso_core::Value::Text(turso_core::types::Text::new("a-extra")),
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("b")),
                        turso_core::Value::Text(turso_core::types::Text::new("beta")),
                        turso_core::Value::Text(turso_core::types::Text::new("b-extra")),
                    ],
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// Pre-ALTER DELETE records replayed into post-ALTER target schema.
/// Source: CREATE TABLE t(x PK, y) → INSERT → DELETE.
/// Target: already has t(x PK, y, z) with the row present.
/// Replay data changes — delete should work via PK regardless of column count mismatch.
#[test]
pub fn test_database_tape_replay_pre_alter_deletes_into_post_alter_schema() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            // Source: pre-ALTER schema — insert then delete
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('a', 'alpha')")
                .unwrap();
            conn1.execute("INSERT INTO t VALUES ('b', 'beta')").unwrap();
            conn1.execute("DELETE FROM t WHERE x = 'a'").unwrap();

            // Target: post-ALTER schema with both rows present
            let conn2 = db2.connect_untracked().unwrap();
            conn2
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT, z TEXT DEFAULT NULL)")
                .unwrap();
            conn2
                .execute("INSERT INTO t VALUES ('a', 'alpha', 'z1')")
                .unwrap();
            conn2
                .execute("INSERT INTO t VALUES ('b', 'beta', 'z2')")
                .unwrap();

            let _conn2_tracked = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: true,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }

            // Verify: 'a' should be upserted then deleted, 'b' upserted.
            // The pre-ALTER upsert for 'b' uses ON CONFLICT(x) DO UPDATE SET x=.., y=..
            // which doesn't touch z, so the pre-existing z='z2' is preserved.
            let mut rows = Vec::new();
            let mut stmt = conn2.prepare("SELECT x, y, z FROM t ORDER BY x").unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![vec![
                    turso_core::Value::Text(turso_core::types::Text::new("b")),
                    turso_core::Value::Text(turso_core::types::Text::new("beta")),
                    turso_core::Value::Text(turso_core::types::Text::new("z2")),
                ]]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// Pre-ALTER INSERT with use_implicit_rowid=true into post-ALTER target.
/// Tests the rowid-preserving path: INSERT INTO t(col1, col2, rowid) VALUES (?,?,?).
#[test]
pub fn test_database_tape_replay_pre_alter_inserts_preserve_rowid() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            // Source: no explicit PK, uses implicit rowid
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1.execute("CREATE TABLE t(a TEXT, b TEXT)").unwrap();
            conn1
                .execute("INSERT INTO t(rowid, a, b) VALUES (10, 'x', 'y')")
                .unwrap();
            conn1
                .execute("INSERT INTO t(rowid, a, b) VALUES (20, 'p', 'q')")
                .unwrap();

            // Target: post-ALTER schema with extra column
            let conn2 = db2.connect_untracked().unwrap();
            conn2
                .execute("CREATE TABLE t(a TEXT, b TEXT, c TEXT DEFAULT NULL)")
                .unwrap();

            let _conn2_tracked = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: true,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: true,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }

            // Verify data — rowids should be preserved
            let mut rows = Vec::new();
            let mut stmt = conn2
                .prepare("SELECT rowid, a, b, c FROM t ORDER BY rowid")
                .unwrap();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![
                        turso_core::Value::from_i64(10),
                        turso_core::Value::Text(turso_core::types::Text::new("x")),
                        turso_core::Value::Text(turso_core::types::Text::new("y")),
                        turso_core::Value::Null,
                    ],
                    vec![
                        turso_core::Value::from_i64(20),
                        turso_core::Value::Text(turso_core::types::Text::new("p")),
                        turso_core::Value::Text(turso_core::types::Text::new("q")),
                        turso_core::Value::Null,
                    ],
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// ALTER TABLE ADD COLUMN replayed into a target that already has the column.
/// This simulates the case where both local and remote independently added
/// the same column, and the pull replay must be idempotent.
#[test]
pub fn test_database_tape_replay_alter_table_add_column_idempotent() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();

            // db1: CREATE TABLE then ADD COLUMN z (captured by CDC)
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn1
                .execute("ALTER TABLE t ADD COLUMN z TEXT DEFAULT NULL")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('a', 'alpha', 'extra')")
                .unwrap();

            // db2: already has the column z (simulating independent ADD COLUMN)
            let conn2_setup = db2.connect_untracked().unwrap();
            conn2_setup
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT, z TEXT DEFAULT NULL)")
                .unwrap();

            let conn2 = db2.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                let opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut iterator = db1.iterate_changes(opts).unwrap();
                while let Some(operation) = iterator.next(&coro).await.unwrap() {
                    session.replay(&coro, operation).await.unwrap();
                }
            }

            // Verify schema is correct
            let mut stmt = conn2
                .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
                .unwrap();
            let row = run_stmt_once(&coro, &mut stmt).await.unwrap().unwrap();
            let sql = row.get_value(0).to_text().unwrap().to_string();
            assert!(
                sql.contains("z"),
                "schema should still contain z column: {sql}"
            );

            // Verify data was replayed
            let mut stmt = conn2.prepare("SELECT x, y, z FROM t").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![vec![
                    turso_core::Value::Text(turso_core::types::Text::new("a")),
                    turso_core::Value::Text(turso_core::types::Text::new("alpha")),
                    turso_core::Value::Text(turso_core::types::Text::new("extra")),
                ]]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// Both databases independently add the same column, then replay each other's
/// changes. This tests bidirectional idempotency of ALTER TABLE ADD COLUMN.
#[test]
pub fn test_database_tape_replay_alter_table_add_column_both_sides() {
    let temp_file1 = NamedTempFile::new().unwrap();
    let db_path1 = temp_file1.path().to_str().unwrap();
    let temp_file2 = NamedTempFile::new().unwrap();
    let db_path2 = temp_file2.path().to_str().unwrap();
    let temp_file3 = NamedTempFile::new().unwrap();
    let db_path3 = temp_file3.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db1 =
        turso_core::Database::open_file(io.clone(), db_path1, Arc::new(SqliteDialect)).unwrap();
    let db1 = Arc::new(DatabaseTape::new(db1));

    let db2 =
        turso_core::Database::open_file(io.clone(), db_path2, Arc::new(SqliteDialect)).unwrap();
    let db2 = Arc::new(DatabaseTape::new(db2));

    let db3 =
        turso_core::Database::open_file(io.clone(), db_path3, Arc::new(SqliteDialect)).unwrap();
    let db3 = Arc::new(DatabaseTape::new(db3));

    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();

            // db1: CREATE TABLE then ADD COLUMN z
            let conn1 = db1.connect(&coro).await.unwrap();
            conn1
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn1
                .execute("ALTER TABLE t ADD COLUMN z TEXT DEFAULT NULL")
                .unwrap();
            conn1
                .execute("INSERT INTO t VALUES ('a', 'alpha', 'one')")
                .unwrap();

            // db2: same base table, independently adds the same column z
            let conn2 = db2.connect(&coro).await.unwrap();
            conn2
                .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
                .unwrap();
            conn2
                .execute("ALTER TABLE t ADD COLUMN z TEXT DEFAULT NULL")
                .unwrap();
            conn2
                .execute("INSERT INTO t VALUES ('b', 'beta', 'two')")
                .unwrap();

            // db3: merge both — replay db1 then db2 changes
            let conn3 = db3.connect(&coro).await.unwrap();
            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: false,
                };
                let mut session = db3.start_replay_session(&coro, opts).await.unwrap();

                let iter_opts = DatabaseChangesIteratorOpts {
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut it1 = db1.iterate_changes(iter_opts.clone()).unwrap();
                while let Some(op) = it1.next(&coro).await.unwrap() {
                    session.replay(&coro, op).await.unwrap();
                }

                let mut it2 = db2.iterate_changes(iter_opts).unwrap();
                while let Some(op) = it2.next(&coro).await.unwrap() {
                    session.replay(&coro, op).await.unwrap();
                }
            }

            // Verify merged data
            let mut stmt = conn3.prepare("SELECT x, y, z FROM t ORDER BY x").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            assert_eq!(
                rows,
                vec![
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("a")),
                        turso_core::Value::Text(turso_core::types::Text::new("alpha")),
                        turso_core::Value::Text(turso_core::types::Text::new("one")),
                    ],
                    vec![
                        turso_core::Value::Text(turso_core::types::Text::new("b")),
                        turso_core::Value::Text(turso_core::types::Text::new("beta")),
                        turso_core::Value::Text(turso_core::types::Text::new("two")),
                    ],
                ]
            );
            crate::Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }
}

/// End state of a primary-key swap replayed as one logical transaction.
///
/// The remote needed three statements and a temporary key to swap the keys of
/// two rows; the logical stream carries the coalesced result as a delete of
/// each row's old key plus an upsert of its new image. Replaying every delete
/// before every upsert is what keeps both rows: interleaved per-row order
/// would have the second delete remove the row the first upsert wrote.
#[test]
pub fn test_pk_swap_in_one_txn_replays_both_rows() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(io.clone(), db_path, Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            conn.execute("CREATE TABLE t(x, y, z, PRIMARY KEY (x, y))")
                .unwrap();
            conn.execute("INSERT INTO t VALUES ('a', '1', 'left'), ('b', '2', 'right')")
                .unwrap();

            fn text(value: &str) -> turso_core::Value {
                turso_core::Value::Text(turso_core::types::Text::new(value.to_string()))
            }
            let key = |x: &str, y: &str| Some(turso_core::alloc::vec![text(x), text(y)]);
            let image =
                |x: &str, y: &str, z: &str| turso_core::alloc::vec![text(x), text(y), text(z)];
            let change = |id: i64, change: DatabaseTapeRowChangeType| DatabaseTapeRowChange {
                change_id: 0,
                change_time: 0,
                change,
                table_name: "t".to_string(),
                id,
            };

            {
                let opts = DatabaseReplaySessionOpts {
                    use_implicit_rowid: true,
                };
                let mut session = db.start_replay_session(&coro, opts).await.unwrap();
                // Deletes first, then upserts — the order the decoder emits.
                for operation in [
                    change(
                        1,
                        DatabaseTapeRowChangeType::Delete {
                            before: turso_core::alloc::vec![],
                            key: key("a", "1"),
                        },
                    ),
                    change(
                        2,
                        DatabaseTapeRowChangeType::Delete {
                            before: turso_core::alloc::vec![],
                            key: key("b", "2"),
                        },
                    ),
                    change(
                        1,
                        DatabaseTapeRowChangeType::Insert {
                            after: image("b", "2", "left"),
                        },
                    ),
                    change(
                        2,
                        DatabaseTapeRowChangeType::Insert {
                            after: image("a", "1", "right"),
                        },
                    ),
                ] {
                    session
                        .replay(&coro, DatabaseTapeOperation::RowChange(operation))
                        .await
                        .unwrap();
                }
                session
                    .replay(&coro, DatabaseTapeOperation::Commit)
                    .await
                    .unwrap();
            }

            let mut stmt = conn.prepare("SELECT x, y, z FROM t ORDER BY x, y").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(
                    row.get_values()
                        .map(|value| format!("{value}"))
                        .collect::<Vec<_>>()
                        .join("|"),
                );
            }
            rows
        }
    });
    let rows = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };

    assert_eq!(
        rows,
        vec!["a|1|right".to_string(), "b|2|left".to_string()],
        "the swap must keep both rows"
    );
}
