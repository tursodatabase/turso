use super::*;
use crate::alloc::TryClone;
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResultOr};
use crate::{
    Completion, Connection, Database, IOResult, MemoryIO, Numeric, OpenOptions, Register,
    SqliteDialect, Statement, StepResult, Value,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use turso_ext::{ConstraintOp, ResultCode, VTabCursor, VTabModule, VTable};

#[test]
fn scalar_calls_resume_independently_and_create_once() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_scalar(
                "delayed",
                2,
                false,
                DelayedScalar {
                    queue: queue.clone(),
                    created: created.clone(),
                    dropped: dropped.clone(),
                },
            )
            .unwrap(),
    );
    let mut first = conn.prepare("SELECT delayed(7, 3)").unwrap();
    let other = conn.db.connect().unwrap();
    let mut second = other.prepare("SELECT delayed(2, 9)").unwrap();
    assert!(matches!(first.step().unwrap(), StepResult::IO));
    assert!(matches!(second.step().unwrap(), StepResult::IO));
    assert!(matches!(first.step().unwrap(), StepResult::IO));
    assert_eq!(created.load(Ordering::SeqCst), 2);
    assert_eq!(queue.lock().len(), 2);
    assert_eq!(
        collect(&mut second, &queue),
        vec![vec![Value::from_i64(29)]]
    );
    assert_eq!(collect(&mut first, &queue), vec![vec![Value::from_i64(73)]]);
    assert_eq!(created.load(Ordering::SeqCst), 2);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[test]
fn resetting_a_pending_scalar_drops_it_and_starts_a_new_call() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_scalar(
                "delayed",
                2,
                false,
                DelayedScalar {
                    queue: queue.clone(),
                    created: created.clone(),
                    dropped: dropped.clone(),
                },
            )
            .unwrap(),
    );
    let mut stmt = conn.prepare("SELECT delayed(4, 1)").unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    stmt.reset().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(collect(&mut stmt, &queue), vec![vec![Value::from_i64(41)]]);
    assert_eq!(created.load(Ordering::SeqCst), 2);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[test]
fn scalar_error_after_io_drops_the_call_and_preserves_the_error() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(AtomicUsize::new(0));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_scalar(
                "delayed",
                2,
                false,
                DelayedScalar {
                    queue: queue.clone(),
                    created: Arc::new(AtomicUsize::new(0)),
                    dropped: dropped.clone(),
                },
            )
            .unwrap(),
    );
    let mut stmt = conn.prepare("SELECT delayed(-1, 8)").unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    queue.lock().pop().unwrap().complete(0);
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    queue.lock().pop().unwrap().complete(0);
    let error = stmt.step().unwrap_err();
    assert!(error.to_string().contains("negative first argument"));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn failed_completion_releases_the_pending_scalar() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(AtomicUsize::new(0));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_scalar(
                "delayed",
                2,
                false,
                DelayedScalar {
                    queue: queue.clone(),
                    created: Arc::new(AtomicUsize::new(0)),
                    dropped: dropped.clone(),
                },
            )
            .unwrap(),
    );
    let mut stmt = conn.prepare("SELECT delayed(7, 2)").unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    queue
        .lock()
        .pop()
        .unwrap()
        .error(crate::CompletionError::Aborted);
    assert!(matches!(
        stmt.step().unwrap_err(),
        LimboError::CompletionError(crate::CompletionError::Aborted)
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn abort_releases_nested_trigger_calls_and_restores_trigger_state() {
    for fail_completion in [true, false] {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let dropped = Arc::new(AtomicUsize::new(0));
        let conn = connection(
            OpenOptions::new(Arc::new(SqliteDialect))
                .native_scalar(
                    "delayed",
                    2,
                    false,
                    DelayedScalar {
                        queue: queue.clone(),
                        created: Arc::new(AtomicUsize::new(0)),
                        dropped: dropped.clone(),
                    },
                )
                .unwrap(),
        );
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO audit VALUES (501), (502)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER child_insert AFTER INSERT ON child BEGIN \
             INSERT INTO audit VALUES (97); SELECT delayed(NEW.id, 8); END",
        )
        .unwrap();
        conn.execute(
            "CREATE TRIGGER parent_insert AFTER INSERT ON parent BEGIN \
             INSERT INTO child VALUES (NEW.id + 20); END",
        )
        .unwrap();
        let mut stmt = conn.prepare("INSERT INTO parent VALUES (7)").unwrap();
        assert!(matches!(stmt.step().unwrap(), StepResult::IO));
        assert_eq!(conn.executing_triggers.read().len(), 2);
        assert_eq!(conn.last_insert_rowid(), 97);
        if fail_completion {
            queue
                .lock()
                .pop()
                .unwrap()
                .error(crate::CompletionError::Aborted);
            assert!(matches!(
                stmt.step().unwrap_err(),
                LimboError::CompletionError(crate::CompletionError::Aborted)
            ));
        } else {
            stmt.reset().unwrap();
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(conn.executing_triggers.read().is_empty());
        assert_eq!(conn.last_insert_rowid(), 7);
        assert!(conn
            .prepare("SELECT id FROM parent")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .is_empty());
        assert!(conn
            .prepare("SELECT id FROM child")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .is_empty());
        assert_eq!(
            conn.prepare("SELECT id FROM audit ORDER BY id")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(501)], vec![Value::from_i64(502)]]
        );
    }
}

#[test]
fn aggregates_resume_steps_and_finalization_for_each_group() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_aggregate(
                "weighted",
                2,
                WeightedSum {
                    queue: queue.clone(),
                    created: created.clone(),
                },
            )
            .unwrap(),
    );
    let mut stmt = conn
        .prepare(
            "WITH t(g, x, y) AS (VALUES (1, 2, 3), (1, 5, 7), (2, -4, 11)) \
         SELECT g, weighted(x, y), weighted(y, x + 1) FROM t GROUP BY g ORDER BY g",
        )
        .unwrap();
    assert_eq!(
        collect(&mut stmt, &queue),
        vec![
            vec![Value::from_i64(1), Value::from_i64(41), Value::from_i64(51)],
            vec![
                Value::from_i64(2),
                Value::from_i64(-44),
                Value::from_i64(-33)
            ],
        ]
    );
    assert_eq!(created.load(Ordering::SeqCst), 4);
    let other = conn.db.connect().unwrap();
    let mut empty = other.prepare("SELECT weighted(3, 7) WHERE 0").unwrap();
    assert_eq!(
        collect(&mut empty, &queue),
        vec![vec![Value::from_i64(-99)]]
    );
    assert_eq!(created.load(Ordering::SeqCst), 5);
    assert!(conn
        .prepare("SELECT weighted(3, 7) OVER ()")
        .unwrap_err()
        .to_string()
        .contains("cannot be used as a window function"));
}

#[test]
fn native_variadic_aggregates_use_callsite_argument_count() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_aggregate(
                "weighted",
                -1,
                WeightedSum {
                    queue: queue.clone(),
                    created: Arc::new(AtomicUsize::new(0)),
                },
            )
            .unwrap(),
    );
    let mut stmt = conn
        .prepare("SELECT weighted(3, 11), weighted(5, 2)")
        .unwrap();
    assert_eq!(
        collect(&mut stmt, &queue),
        vec![vec![Value::from_i64(33), Value::from_i64(10)]]
    );
}

#[test]
fn copied_aggregate_accumulators_do_not_share_mutable_state() {
    let factory = WeightedSum {
        queue: Arc::new(Mutex::new(Vec::new())),
        created: Arc::new(AtomicUsize::new(0)),
    };
    let mut original = Register::Value(Value::Null);
    finish(&factory.queue, || {
        step_aggregate(
            &mut original,
            &factory,
            &[Value::from_i64(2), Value::from_i64(3)],
        )
    });
    let mut copied = original.try_clone().unwrap();
    finish(&factory.queue, || {
        step_aggregate(
            &mut original,
            &factory,
            &[Value::from_i64(5), Value::from_i64(7)],
        )
    });
    finish(&factory.queue, || {
        step_aggregate(
            &mut copied,
            &factory,
            &[Value::from_i64(11), Value::from_i64(2)],
        )
    });
    assert_eq!(
        finish(&factory.queue, || finalize_aggregate(
            &mut original,
            &factory
        )),
        Value::from_i64(41)
    );
    assert_eq!(
        finish(&factory.queue, || finalize_aggregate(&mut copied, &factory)),
        Value::from_i64(28)
    );
}

#[test]
fn pending_aggregates_release_state_on_reset_and_failed_completion() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_aggregate(
                "weighted",
                2,
                WeightedSum {
                    queue: queue.clone(),
                    created: created.clone(),
                },
            )
            .unwrap(),
    );
    let mut stmt = conn.prepare("SELECT weighted(4, 7)").unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    stmt.reset().unwrap();
    assert_eq!(Arc::strong_count(&queue), 2);
    assert_eq!(collect(&mut stmt, &queue), vec![vec![Value::from_i64(28)]]);
    assert_eq!(created.load(Ordering::SeqCst), 2);
    stmt.reset().unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    queue
        .lock()
        .pop()
        .unwrap()
        .error(crate::CompletionError::Aborted);
    assert!(matches!(
        stmt.step().unwrap_err(),
        LimboError::CompletionError(crate::CompletionError::Aborted)
    ));
    assert_eq!(Arc::strong_count(&queue), 2);
}

#[test]
fn virtual_table_filter_next_and_column_resume_without_skipping_rows() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(vec![(1, 4), (2, 9), (3, 17)]));
    let conn = connection(OpenOptions::new(Arc::new(SqliteDialect)).native_module(
        "native_rows",
        VTabKind::TableValuedFunction,
        RowsModule {
            queue: queue.clone(),
            rows,
            events: Arc::new(Mutex::new(Vec::new())),
            writable: false,
        },
    ));
    let mut stmt = conn
        .prepare("SELECT rowid, value FROM native_rows(8)")
        .unwrap();
    assert_eq!(
        collect(&mut stmt, &queue),
        vec![
            vec![Value::from_i64(2), Value::from_i64(9)],
            vec![Value::from_i64(3), Value::from_i64(17)],
        ]
    );
    let mut empty = conn.prepare("SELECT value FROM native_rows(18)").unwrap();
    assert!(collect(&mut empty, &queue).is_empty());
    let mut outer = conn
        .prepare("SELECT value FROM (SELECT 1 AS n) LEFT JOIN native_rows(18) ON 1")
        .unwrap();
    assert_eq!(collect(&mut outer, &queue), vec![vec![Value::Null]]);
}

#[test]
fn native_table_functions_survive_mvcc_schema_refresh_and_other_connection_ddl() {
    for mvcc in [false, true] {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let conn = connection(OpenOptions::new(Arc::new(SqliteDialect)).native_module(
            "native_rows",
            VTabKind::TableValuedFunction,
            RowsModule {
                queue: queue.clone(),
                rows: Arc::new(Mutex::new(vec![(1, 4), (2, 9), (3, 17)])),
                events: Arc::new(Mutex::new(Vec::new())),
                writable: false,
            },
        ));
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        let mut stmt = conn.prepare("SELECT value FROM native_rows(8)").unwrap();
        assert_eq!(
            collect(&mut stmt, &queue),
            vec![vec![Value::from_i64(9)], vec![Value::from_i64(17)]]
        );
        let other = conn.db.connect().unwrap();
        other.execute("CREATE TABLE unrelated(value)").unwrap();
        stmt.reset().unwrap();
        assert_eq!(
            collect(&mut stmt, &queue),
            vec![vec![Value::from_i64(9)], vec![Value::from_i64(17)]]
        );
        let mut shared = other.prepare("SELECT value FROM native_rows(16)").unwrap();
        assert_eq!(
            collect(&mut shared, &queue),
            vec![vec![Value::from_i64(17)]]
        );
    }
}

#[test]
fn native_cursors_close_at_done_in_explicit_transactions_and_triggers() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let conn = connection(OpenOptions::new(Arc::new(SqliteDialect)).native_module(
        "native_rows",
        VTabKind::TableValuedFunction,
        RowsModule {
            queue: queue.clone(),
            rows: Arc::new(Mutex::new(vec![(1, 4), (2, 9)])),
            events: Arc::new(Mutex::new(Vec::new())),
            writable: false,
        },
    ));
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    {
        let mut schema = conn.schema.write();
        let schema = crate::schema::Schema::try_make_mut(&mut schema).unwrap();
        let table = schema.tables.get_mut("native_rows").unwrap();
        let crate::schema::Table::Virtual(table) = Arc::make_mut(table) else {
            unreachable!();
        };
        Arc::make_mut(table).innocuous = true;
    }
    conn.execute("CREATE TRIGGER parent_insert AFTER INSERT ON parent BEGIN SELECT value FROM native_rows(8) LIMIT 1; END").unwrap();
    conn.execute("BEGIN").unwrap();
    let references = Arc::strong_count(&queue);
    let mut stmt = conn
        .prepare("SELECT value FROM native_rows(8) LIMIT 1")
        .unwrap();
    assert_eq!(collect(&mut stmt, &queue), vec![vec![Value::from_i64(9)]]);
    assert_eq!(Arc::strong_count(&queue), references);
    stmt.reset().unwrap();
    assert_eq!(collect(&mut stmt, &queue), vec![vec![Value::from_i64(9)]]);
    assert_eq!(Arc::strong_count(&queue), references);
    let mut insert = conn.prepare("INSERT INTO parent VALUES (7), (12)").unwrap();
    assert!(collect(&mut insert, &queue).is_empty());
    assert_eq!(Arc::strong_count(&queue), references);
    conn.execute("COMMIT").unwrap();
    assert_eq!(Arc::strong_count(&queue), references);
}

#[test]
fn native_writes_yield_and_keep_existing_transaction_callbacks() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let conn = connection(OpenOptions::new(Arc::new(SqliteDialect)).native_module(
        "native_store",
        VTabKind::VirtualTable,
        RowsModule {
            queue: queue.clone(),
            rows: rows.clone(),
            events: events.clone(),
            writable: true,
        },
    ));
    conn.execute("CREATE VIRTUAL TABLE store USING native_store")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for sql in [
        "INSERT INTO store(value) VALUES (13)",
        "INSERT INTO store(value) VALUES (27)",
    ] {
        let mut insert = conn.prepare(sql).unwrap();
        assert!(collect(&mut insert, &queue).is_empty());
    }
    conn.execute("COMMIT").unwrap();
    assert_eq!(*rows.lock(), vec![(1, 13), (2, 27)]);
    assert_eq!(*events.lock(), vec!["begin", "write", "write", "commit"]);
    events.lock().clear();
    conn.execute("BEGIN").unwrap();
    let mut insert = conn
        .prepare("INSERT INTO store(value) VALUES (49)")
        .unwrap();
    collect(&mut insert, &queue);
    assert_eq!(*events.lock(), vec!["begin", "write"]);
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(*events.lock(), vec!["begin", "write", "rollback"]);
    assert_eq!(*rows.lock(), vec![(1, 13), (2, 27)]);
    for sql in [
        "UPDATE store SET value = value + 1 WHERE rowid = 1",
        "DELETE FROM store WHERE rowid = 2",
    ] {
        let mut stmt = conn.prepare(sql).unwrap();
        assert!(collect(&mut stmt, &queue).is_empty());
    }
    assert_eq!(*rows.lock(), vec![(1, 14)]);
    conn.execute("DROP TABLE store").unwrap();
    assert_eq!(events.lock().last(), Some(&"destroy"));
}

#[test]
fn abandoned_native_write_does_not_apply_the_pending_update() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let conn = connection(OpenOptions::new(Arc::new(SqliteDialect)).native_module(
        "native_store",
        VTabKind::VirtualTable,
        RowsModule {
            queue: queue.clone(),
            rows: rows.clone(),
            events: events.clone(),
            writable: true,
        },
    ));
    conn.execute("CREATE VIRTUAL TABLE store USING native_store")
        .unwrap();
    let mut stmt = conn
        .prepare("INSERT INTO store(value) VALUES (55)")
        .unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    stmt.reset().unwrap();
    assert!(rows.lock().is_empty());
    for completion in queue.lock().drain(..) {
        completion.complete(0);
    }
    assert!(rows.lock().is_empty());
    assert_eq!(*events.lock(), vec!["begin", "rollback"]);
    collect(&mut stmt, &queue);
    assert_eq!(*rows.lock(), vec![(1, 55)]);
    assert_eq!(
        *events.lock(),
        vec!["begin", "rollback", "begin", "write", "commit"]
    );
    events.lock().clear();
    stmt.reset().unwrap();
    conn.execute("BEGIN").unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::IO));
    stmt.reset().unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(*events.lock(), vec!["begin", "commit"]);
    assert_eq!(*rows.lock(), vec![(1, 55)]);
}

#[test]
fn failed_native_write_rolls_back_earlier_rows() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(vec![(1, 13), (2, 27)]));
    let events = Arc::new(Mutex::new(Vec::new()));
    let conn = connection(OpenOptions::new(Arc::new(SqliteDialect)).native_module(
        "native_store",
        VTabKind::VirtualTable,
        RowsModule {
            queue: queue.clone(),
            rows: rows.clone(),
            events: events.clone(),
            writable: true,
        },
    ));
    conn.execute("CREATE VIRTUAL TABLE store USING native_store")
        .unwrap();
    let mut stmt = conn
        .prepare("UPDATE store SET value = CASE WHEN rowid = 1 THEN 31 ELSE -1 END")
        .unwrap();
    let error = loop {
        match stmt.step() {
            Err(error) => break error,
            Ok(StepResult::IO) => {
                for completion in queue.lock().drain(..) {
                    completion.complete(0);
                }
            }
            other => panic!("unexpected step result {other:?}"),
        }
    };
    assert!(error.to_string().contains("negative value"));
    assert_eq!(*rows.lock(), vec![(1, 13), (2, 27)]);
    assert_eq!(*events.lock(), vec!["begin", "write", "rollback"]);
}

#[test]
fn pending_native_write_blocks_another_writer_and_commit() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let conn = connection(OpenOptions::new(Arc::new(SqliteDialect)).native_module(
        "native_store",
        VTabKind::VirtualTable,
        RowsModule {
            queue: queue.clone(),
            rows: rows.clone(),
            events: events.clone(),
            writable: true,
        },
    ));
    conn.execute("CREATE VIRTUAL TABLE store USING native_store")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let mut first = conn
        .prepare("INSERT INTO store(value) VALUES (13)")
        .unwrap();
    let mut second = conn
        .prepare("INSERT INTO store(value) VALUES (27)")
        .unwrap();
    assert!(matches!(first.step().unwrap(), StepResult::IO));
    assert!(matches!(
        second.step().unwrap_err(),
        LimboError::StatementsInProgress(_)
    ));
    assert!(matches!(
        conn.execute("COMMIT").unwrap_err(),
        LimboError::StatementsInProgress(_)
    ));
    assert!(matches!(first.step().unwrap(), StepResult::IO));
    assert!(rows.lock().is_empty());
    collect(&mut first, &queue);
    second.reset().unwrap();
    collect(&mut second, &queue);
    conn.execute("COMMIT").unwrap();
    assert_eq!(*rows.lock(), vec![(1, 13), (2, 27)]);
    assert_eq!(*events.lock(), vec!["begin", "write", "write", "commit"]);
}

#[test]
fn scalar_calls_and_table_updates_share_one_statement_state() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let rows = Arc::new(Mutex::new(vec![(1, 4), (2, 9)]));
    let events = Arc::new(Mutex::new(Vec::new()));
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .native_scalar(
            "delayed",
            2,
            false,
            DelayedScalar {
                queue: queue.clone(),
                created: created.clone(),
                dropped: dropped.clone(),
            },
        )
        .unwrap();
    let conn = connection(options.native_module(
        "native_store",
        VTabKind::VirtualTable,
        RowsModule {
            queue: queue.clone(),
            rows: rows.clone(),
            events: events.clone(),
            writable: true,
        },
    ));
    conn.execute("CREATE VIRTUAL TABLE store USING native_store")
        .unwrap();
    let mut stmt = conn
        .prepare("UPDATE store SET value = delayed(value, 3)")
        .unwrap();
    assert!(collect(&mut stmt, &queue).is_empty());
    assert_eq!(*rows.lock(), vec![(1, 43), (2, 93)]);
    assert_eq!(created.load(Ordering::SeqCst), 2);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert_eq!(*events.lock(), vec!["begin", "write", "write", "commit"]);
}

#[test]
fn c_table_inserts_with_native_arguments_block_other_writes_and_transaction_end() {
    let queue = Arc::new(Mutex::new(Vec::new()));
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_scalar(
                "delayed",
                2,
                false,
                DelayedScalar {
                    queue: queue.clone(),
                    created: Arc::new(AtomicUsize::new(0)),
                    dropped: Arc::new(AtomicUsize::new(0)),
                },
            )
            .unwrap(),
    );
    let api = unsafe { conn._build_turso_ext() };
    let code = unsafe { CStoreModule::register_CStoreModule(&api) };
    unsafe {
        conn._free_extension_ctx(api);
    }
    assert_eq!(code, ResultCode::OK);
    conn.execute("CREATE VIRTUAL TABLE c_store USING c_store_module")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let mut first = conn
        .prepare("INSERT INTO c_store VALUES(delayed(4, 3))")
        .unwrap();
    let mut second = conn.prepare("INSERT INTO c_store VALUES(92)").unwrap();
    assert!(matches!(first.step().unwrap(), StepResult::IO));
    for sql in ["COMMIT", "ROLLBACK"] {
        assert!(matches!(
            conn.execute(sql).unwrap_err(),
            LimboError::StatementsInProgress(_)
        ));
    }
    assert!(matches!(
        second.step().unwrap_err(),
        LimboError::StatementsInProgress(_)
    ));
    assert!(collect(&mut first, &queue).is_empty());
    second.reset().unwrap();
    assert!(collect(&mut second, &queue).is_empty());
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.prepare("SELECT value FROM c_store")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(43)], vec![Value::from_i64(92)]]
    );
    conn.execute("BEGIN").unwrap();
    first.reset().unwrap();
    assert!(collect(&mut first, &queue).is_empty());
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        conn.prepare("SELECT value FROM c_store")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(43)], vec![Value::from_i64(92)]]
    );
}

#[test]
fn c_extensions_still_execute_with_native_registration() {
    let conn = connection(
        OpenOptions::new(Arc::new(SqliteDialect))
            .native_aggregate(
                "weighted",
                2,
                WeightedSum {
                    queue: Arc::new(Mutex::new(Vec::new())),
                    created: Arc::new(AtomicUsize::new(0)),
                },
            )
            .unwrap(),
    );
    let api = unsafe { conn._build_turso_ext() };
    let name = std::ffi::CString::new("c_double").unwrap();
    let code = unsafe {
        (api.register_scalar_function)(api.ctx, name.as_ptr(), 1, true, 0, c_double, None, None)
    };
    unsafe {
        conn._free_extension_ctx(api);
    }
    assert_eq!(code, ResultCode::OK);
    assert_eq!(
        conn.prepare("SELECT c_double(6)")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(12)]]
    );
    #[cfg(feature = "series")]
    assert_eq!(
        conn.prepare("SELECT value FROM generate_series(2, 8, 3)")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![
            vec![Value::from_i64(2)],
            vec![Value::from_i64(5)],
            vec![Value::from_i64(8)]
        ]
    );
}

fn connection(options: OpenOptions) -> Arc<Connection> {
    Database::open(Arc::new(MemoryIO::new()), ":memory:", options)
        .unwrap()
        .connect()
        .unwrap()
}

fn collect(stmt: &mut Statement, queue: &Arc<Mutex<Vec<Completion>>>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for _ in 0..500 {
        match stmt.step().unwrap() {
            StepResult::Row => rows.push(stmt.row().unwrap().get_values().cloned().collect()),
            StepResult::Done => return rows,
            StepResult::IO => {
                for completion in queue.lock().drain(..) {
                    completion.complete(0);
                }
            }
            other => panic!("unexpected step result {other:?}"),
        }
    }
    panic!("statement did not finish");
}

fn finish<T>(
    queue: &Arc<Mutex<Vec<Completion>>>,
    mut operation: impl FnMut() -> IOResultOr<T>,
) -> T {
    for _ in 0..20 {
        match operation().unwrap() {
            IOResult::Done(value) => return value,
            IOResult::IO(_) => {
                for completion in queue.lock().drain(..) {
                    completion.complete(0);
                }
            }
        }
    }
    panic!("operation did not finish");
}

#[derive(Clone, Debug)]
struct Gate {
    queue: Arc<Mutex<Vec<Completion>>>,
    pending: Option<Completion>,
    remaining: u8,
}

impl Gate {
    fn new(queue: Arc<Mutex<Vec<Completion>>>) -> Self {
        Self {
            queue,
            pending: None,
            remaining: 2,
        }
    }

    fn wait(&mut self) -> Option<IOCompletions> {
        if let Some(pending) = &self.pending {
            if !pending.finished() {
                return Some(IOCompletions(pending.clone()));
            }
            self.pending = None;
            self.remaining -= 1;
        }
        if self.remaining == 0 {
            return None;
        }
        let completion = Completion::new_wait();
        self.queue.lock().push(completion.clone());
        self.pending = Some(completion.clone());
        Some(IOCompletions(completion))
    }

    fn reset(&mut self) {
        assert!(self.pending.is_none());
        self.remaining = 2;
    }
}

#[derive(Debug)]
struct DelayedScalar {
    queue: Arc<Mutex<Vec<Completion>>>,
    created: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl ScalarFunction for DelayedScalar {
    type Call = DelayedCall;

    fn create_call(&self, args: Vec<Value>) -> Result<Self::Call> {
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(DelayedCall {
            args,
            gate: Gate::new(self.queue.clone()),
            dropped: self.dropped.clone(),
        })
    }
}

struct DelayedCall {
    args: Vec<Value>,
    gate: Gate,
    dropped: Arc<AtomicUsize>,
}

impl ScalarCall for DelayedCall {
    fn step(&mut self) -> IOResultOr<Value> {
        if let Some(io) = self.gate.wait() {
            return Ok(IOResult::IO(io));
        }
        let left = integer(&self.args[0]);
        if left < 0 {
            return Err(LimboError::ExtensionError("negative first argument".into()).into());
        }
        Ok(IOResult::Done(Value::from_i64(
            left * 10 + integer(&self.args[1]),
        )))
    }
}

impl Drop for DelayedCall {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct WeightedSum {
    queue: Arc<Mutex<Vec<Completion>>>,
    created: Arc<AtomicUsize>,
}

impl AggregateFunction for WeightedSum {
    type Accumulator = WeightedAccumulator;

    fn create_accumulator(&self) -> Result<Self::Accumulator> {
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(WeightedAccumulator {
            total: 0,
            count: 0,
            gate: Gate::new(self.queue.clone()),
        })
    }
}

#[derive(Clone, Debug)]
struct WeightedAccumulator {
    total: i64,
    count: usize,
    gate: Gate,
}

impl Aggregate for WeightedAccumulator {
    fn step(&mut self, args: &[Value]) -> IOResultOr<()> {
        if let Some(io) = self.gate.wait() {
            return Ok(IOResult::IO(io));
        }
        self.total += integer(&args[0]) * integer(&args[1]);
        self.count += 1;
        self.gate.reset();
        Ok(IOResult::Done(()))
    }

    fn finalize(&mut self) -> IOResultOr<Value> {
        if let Some(io) = self.gate.wait() {
            return Ok(IOResult::IO(io));
        }
        Ok(IOResult::Done(Value::from_i64(if self.count == 0 {
            -99
        } else {
            self.total
        })))
    }
}

#[derive(Debug)]
struct RowsModule {
    queue: Arc<Mutex<Vec<Completion>>>,
    rows: Arc<Mutex<Vec<(i64, i64)>>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    writable: bool,
}

impl VirtualTableModule for RowsModule {
    type Table = RowsTable;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE x(value INTEGER, lower_bound INTEGER HIDDEN)".into())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(RowsTable {
            queue: self.queue.clone(),
            rows: self.rows.clone(),
            events: self.events.clone(),
            writable: self.writable,
            before: None,
        })
    }
}

#[derive(Debug)]
struct RowsTable {
    queue: Arc<Mutex<Vec<Completion>>>,
    rows: Arc<Mutex<Vec<(i64, i64)>>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    writable: bool,
    before: Option<Vec<(i64, i64)>>,
}

impl VirtualTable for RowsTable {
    type Cursor = RowsCursor;

    fn open(&self, _connection: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(RowsCursor {
            rows: self.rows.clone(),
            selected: Vec::new(),
            index: 0,
            gate: Gate::new(self.queue.clone()),
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let mut info = IndexInfo::default();
        info.idx_num = 7;
        info.idx_str = Some("lower".into());
        info.constraint_usages = constraints
            .iter()
            .map(|c| ConstraintUsage {
                argv_index: (c.column_index == 1 && c.op == ConstraintOp::Eq && c.usable)
                    .then_some(1),
                omit: c.column_index == 1 && c.op == ConstraintOp::Eq && c.usable,
            })
            .collect();
        Ok(info)
    }

    fn readonly(&self) -> bool {
        !self.writable
    }

    fn create_update(&mut self, args: Vec<Value>) -> Result<Box<dyn TableUpdate>> {
        Ok(Box::new(RowsUpdate {
            args,
            rows: self.rows.clone(),
            events: self.events.clone(),
            gate: Gate::new(self.queue.clone()),
        }))
    }

    fn begin(&mut self) -> Result<()> {
        assert!(self.before.is_none());
        self.before = Some(self.rows.lock().clone());
        self.events.lock().push("begin");
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        assert!(self.before.take().is_some());
        self.events.lock().push("commit");
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        *self.rows.lock() = self.before.take().unwrap();
        self.events.lock().push("rollback");
        Ok(())
    }

    fn destroy(&mut self) -> Result<()> {
        self.events.lock().push("destroy");
        Ok(())
    }
}

struct RowsCursor {
    rows: Arc<Mutex<Vec<(i64, i64)>>>,
    selected: Vec<(i64, i64)>,
    index: usize,
    gate: Gate,
}

impl VirtualTableCursor for RowsCursor {
    fn filter(&mut self, args: &[Value], idx_str: Option<&str>, idx_num: i32) -> IOResultOr<bool> {
        assert_eq!(idx_str, Some("lower"));
        assert_eq!(idx_num, 7);
        if let Some(io) = self.gate.wait() {
            return Ok(IOResult::IO(io));
        }
        let lower = args.first().map(integer).unwrap_or(i64::MIN);
        self.selected = self
            .rows
            .lock()
            .iter()
            .copied()
            .filter(|(_, value)| *value >= lower)
            .collect();
        self.index = 0;
        self.gate.reset();
        Ok(IOResult::Done(!self.selected.is_empty()))
    }

    fn next(&mut self) -> IOResultOr<bool> {
        if let Some(io) = self.gate.wait() {
            return Ok(IOResult::IO(io));
        }
        self.index += 1;
        self.gate.reset();
        Ok(IOResult::Done(self.index < self.selected.len()))
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        if let Some(io) = self.gate.wait() {
            return Ok(IOResult::IO(io));
        }
        self.gate.reset();
        Ok(IOResult::Done(match column {
            0 => Value::from_i64(self.selected[self.index].1),
            1 => Value::Null,
            _ => unreachable!("unknown column"),
        }))
    }

    fn rowid(&self) -> i64 {
        self.selected[self.index].0
    }
}

struct RowsUpdate {
    args: Vec<Value>,
    rows: Arc<Mutex<Vec<(i64, i64)>>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    gate: Gate,
}

impl TableUpdate for RowsUpdate {
    fn step(&mut self) -> IOResultOr<Option<i64>> {
        if let Some(io) = self.gate.wait() {
            return Ok(IOResult::IO(io));
        }
        if self.args[0] != Value::Null && self.args[1] == Value::Null {
            let mut rows = self.rows.lock();
            let index = rows
                .iter()
                .position(|(id, _)| *id == integer(&self.args[0]))
                .unwrap();
            rows.remove(index);
            self.events.lock().push("write");
            return Ok(IOResult::Done(None));
        }
        let value = integer(&self.args[2]);
        if value < 0 {
            return Err(LimboError::ExtensionError("negative value".into()).into());
        }
        let mut rows = self.rows.lock();
        if self.args[0] != Value::Null {
            let row = rows
                .iter_mut()
                .find(|(id, _)| *id == integer(&self.args[0]))
                .unwrap();
            *row = (integer(&self.args[1]), value);
            self.events.lock().push("write");
            return Ok(IOResult::Done(None));
        }
        let rowid = rows.len() as i64 + 1;
        rows.push((rowid, value));
        self.events.lock().push("write");
        Ok(IOResult::Done(Some(rowid)))
    }
}

fn integer(value: &Value) -> i64 {
    let Value::Numeric(Numeric::Integer(value)) = value else {
        panic!("expected integer, got {value:?}");
    };
    *value
}

#[derive(turso_ext::VTabModuleDerive)]
struct CStoreModule;

impl VTabModule for CStoreModule {
    type Table = CStoreTable;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "c_store_module";
    const READONLY: bool = false;

    fn create(
        _args: &[turso_ext::Value],
    ) -> std::result::Result<(String, Self::Table), ResultCode> {
        Ok((
            "CREATE TABLE x(value INTEGER)".into(),
            CStoreTable {
                rows: Vec::new(),
                before: None,
            },
        ))
    }
}

struct CStoreTable {
    rows: Vec<i64>,
    before: Option<Vec<i64>>,
}

impl VTable for CStoreTable {
    type Cursor = CStoreCursor;
    type Error = String;

    fn open(
        &self,
        _conn: Option<std::sync::Arc<turso_ext::Connection>>,
    ) -> std::result::Result<Self::Cursor, String> {
        Ok(CStoreCursor {
            rows: self.rows.clone(),
            index: 0,
        })
    }

    fn begin(&mut self) -> std::result::Result<(), String> {
        assert!(self.before.is_none());
        self.before = Some(self.rows.clone());
        Ok(())
    }

    fn commit(&mut self) -> std::result::Result<(), String> {
        assert!(self.before.take().is_some());
        Ok(())
    }

    fn rollback(&mut self) -> std::result::Result<(), String> {
        self.rows = self.before.take().unwrap();
        Ok(())
    }

    fn insert(&mut self, args: &[turso_ext::Value]) -> std::result::Result<i64, String> {
        if self.before.is_none() {
            return Err("insert without an active transaction".into());
        }
        self.rows.push(args[0].to_integer().unwrap());
        Ok(self.rows.len() as i64)
    }
}

struct CStoreCursor {
    rows: Vec<i64>,
    index: usize,
}

impl VTabCursor for CStoreCursor {
    type Error = String;

    fn filter(&mut self, _args: &[turso_ext::Value], _idx: Option<(&str, i32)>) -> ResultCode {
        self.index = 0;
        if self.eof() {
            ResultCode::EOF
        } else {
            ResultCode::OK
        }
    }

    fn rowid(&self) -> i64 {
        self.index as i64 + 1
    }

    fn column(&self, idx: u32) -> std::result::Result<turso_ext::Value, String> {
        assert_eq!(idx, 0);
        Ok(turso_ext::Value::from_integer(self.rows[self.index]))
    }

    fn eof(&self) -> bool {
        self.index >= self.rows.len()
    }

    fn next(&mut self) -> ResultCode {
        self.index += 1;
        if self.eof() {
            ResultCode::EOF
        } else {
            ResultCode::OK
        }
    }
}

unsafe extern "C" fn c_double(
    _context: usize,
    argc: i32,
    argv: *const turso_ext::Value,
    _context_destructor: Option<turso_ext::ContextDestructor>,
    _value_destructor: Option<turso_ext::ValueDestructor>,
) -> turso_ext::Value {
    assert_eq!(argc, 1);
    let value = unsafe { &*argv }.to_integer().unwrap();
    turso_ext::Value::from_integer(value * 2)
}
