use std::sync::Arc;
use std::task::Waker;
use turso_core::security::roles::SessionRole;
use turso_core::vdbe;
use turso_core::LimboError;
use turso_core::StatementStatusCounter;
use turso_core::StepResult;
use turso_core::Value;
use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, IO};
use turso_sqlite::SqliteDialect;

#[test]
fn cloned_halt_retains_constraint_context() {
    assert_cloned_constraint_context(
        "INSERT INTO parent VALUES (1, 'b', 1)",
        turso_core::ConstraintKind::PrimaryKey,
        "parent",
        "id",
    );
}

#[test]
fn cloned_halt_if_null_retains_constraint_context() {
    assert_cloned_constraint_context(
        "INSERT INTO parent VALUES (2, NULL, 1)",
        turso_core::ConstraintKind::NotNull,
        "parent",
        "code",
    );
}

#[test]
fn cloned_fk_counter_retains_constraint_context() {
    assert_cloned_constraint_context(
        "UPDATE child SET pid = 99",
        turso_core::ConstraintKind::ForeignKey,
        "child",
        "pid",
    );
}

fn assert_cloned_constraint_context(
    sql: &str,
    kind: turso_core::ConstraintKind,
    table: &str,
    column: &str,
) {
    use turso_core::vdbe::{execute, insn::Insn};

    let conn = constraint_test_connection();
    let mut stmt = conn.prepare(sql).unwrap();
    let insn = stmt
        .program
        .insns
        .iter()
        .find_map(|(insn, _)| {
            let matches = match kind {
                turso_core::ConstraintKind::PrimaryKey => matches!(
                    insn,
                    Insn::Halt {
                        err_code: turso_core::error::SQLITE_CONSTRAINT_PRIMARYKEY,
                        ..
                    }
                ),
                turso_core::ConstraintKind::NotNull => matches!(insn, Insn::HaltIfNull { .. }),
                turso_core::ConstraintKind::ForeignKey => matches!(
                    insn,
                    Insn::FkCounter {
                        increment_value: 1,
                        deferred: false,
                        ..
                    }
                ),
                _ => unreachable!(),
            };
            matches.then(|| insn.clone())
        })
        .expect("constraint instruction");
    for index in 0..stmt.state.column_count() {
        stmt.state
            .set_register(index, vdbe::Register::Value(Value::from_i64(99)));
    }
    let error = match &insn {
        Insn::Halt { .. } => execute::op_halt(&stmt.program, &mut stmt.state, &insn, &stmt.pager)
            .err()
            .expect("constraint error"),
        Insn::HaltIfNull { target_reg, .. } => {
            stmt.state
                .set_register(*target_reg, vdbe::Register::Value(Value::Null));
            execute::op_halt_if_null(&stmt.program, &mut stmt.state, &insn, &stmt.pager)
                .err()
                .expect("constraint error")
        }
        Insn::FkCounter { .. } => {
            execute::op_fk_counter(&stmt.program, &mut stmt.state, &insn, &stmt.pager).unwrap();
            Box::new(stmt.state.immediate_foreign_key_error())
        }
        _ => unreachable!(),
    };
    let LimboError::ConstraintViolation(violation) = *error else {
        panic!("expected structured constraint: {error:?}");
    };
    assert_eq!(violation.kind, kind);
    assert_eq!(violation.table_name.as_deref(), Some(table));
    assert_eq!(violation.column_names, vec![column]);
    assert!(!violation.values.is_empty());
}

#[test]
fn internal_constraint_errors_are_structured() {
    for (sql, kind, message, foreign_key, extended_code) in constraint_error_cases() {
        let conn = constraint_test_connection();
        let mut stmt = conn.prepare(sql).unwrap();
        let error = loop {
            match stmt.step_with_structured_errors() {
                Err(error) => break error,
                Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                    stmt.pager.io.step().unwrap();
                }
                Ok(result) => panic!("expected constraint error, got {result:?}: {sql}"),
            }
        };
        let LimboError::ConstraintViolation(violation) = &error else {
            panic!("{sql}: {error:?}");
        };
        assert_eq!(violation.kind, kind, "{sql}");
        assert_eq!(error.to_string(), message, "{sql}");
        assert_eq!(error.is_foreign_key_constraint(), foreign_key, "{sql}");
        assert_eq!(error.sqlite_result_code(), 19, "{sql}");
        assert_eq!(error.sqlite_extended_result_code(), extended_code, "{sql}");
    }
}

#[test]
fn public_constraint_errors_preserve_sqlite_variants() {
    for (sql, _, message, foreign_key, _) in constraint_error_cases() {
        for with_waker in [false, true] {
            let conn = constraint_test_connection();
            let mut stmt = conn.prepare(sql).unwrap();
            let error = loop {
                let result = if with_waker {
                    stmt.step_with_waker(Waker::noop())
                } else {
                    stmt.step()
                };
                match result {
                    Err(error) => break error,
                    Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                        stmt.pager.io.step().unwrap();
                    }
                    Ok(result) => panic!("expected constraint error, got {result:?}: {sql}"),
                }
            };
            if foreign_key {
                assert!(
                    matches!(error, LimboError::ForeignKeyConstraint(_)),
                    "{sql}: {error:?}"
                );
            } else {
                assert!(
                    matches!(error, LimboError::Constraint(_)),
                    "{sql}: {error:?}"
                );
            }
            assert_eq!(error.to_string(), message, "{sql}");
            assert_eq!(error.sqlite_result_code(), 19, "{sql}");
        }
    }
}

#[test]
fn public_helper_statement_errors_preserve_sqlite_variants() {
    let conn = constraint_test_connection();
    let mut stmt = conn
        .prepare_internal("INSERT INTO parent VALUES (1, 'b', 1)")
        .unwrap();
    let error = stmt.run_ignore_rows().unwrap_err();
    assert!(matches!(error, LimboError::Constraint(_)), "{error:?}");
    assert_eq!(error.to_string(), "UNIQUE constraint failed: parent.id");
    assert_eq!(error.sqlite_result_code(), 19);
}

#[test]
fn public_reset_preserves_sqlite_foreign_key_error() {
    let conn = constraint_test_connection();
    let mut stmt = conn
        .prepare("INSERT INTO deferred_child VALUES (99) RETURNING pid")
        .unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                stmt.pager.io.step().unwrap();
            }
            result => panic!("expected RETURNING row, got {result:?}"),
        }
    }
    let error = stmt.reset().unwrap_err();
    assert!(
        matches!(error, LimboError::ForeignKeyConstraint(_)),
        "{error:?}"
    );
    assert_eq!(error.to_string(), "FOREIGN KEY constraint failed");
    assert_eq!(error.sqlite_result_code(), 19);
}

fn constraint_error_cases() -> [(
    &'static str,
    turso_core::ConstraintKind,
    &'static str,
    bool,
    i32,
); 13] {
    [
        (
            "INSERT INTO parent VALUES (1, 'b', 1)",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: parent.id",
            false,
            1555,
        ),
        (
            "INSERT INTO parent VALUES (2, 'a', 1)",
            turso_core::ConstraintKind::Unique,
            "UNIQUE constraint failed: parent.code",
            false,
            2067,
        ),
        (
            "INSERT INTO parent VALUES (2, NULL, 1)",
            turso_core::ConstraintKind::NotNull,
            "NOT NULL constraint failed: parent.code",
            false,
            1299,
        ),
        (
            "INSERT INTO parent VALUES (2, 'b', -1)",
            turso_core::ConstraintKind::Check,
            "CHECK constraint failed: positive",
            false,
            275,
        ),
        (
            "INSERT INTO child VALUES (99)",
            turso_core::ConstraintKind::ForeignKey,
            "FOREIGN KEY constraint failed",
            true,
            787,
        ),
        (
            "DELETE FROM parent",
            turso_core::ConstraintKind::ForeignKey,
            "FOREIGN KEY constraint failed",
            true,
            787,
        ),
        (
            "INSERT INTO deferred_child VALUES (99)",
            turso_core::ConstraintKind::ForeignKey,
            "FOREIGN KEY constraint failed",
            true,
            787,
        ),
        (
            "INSERT INTO trigger_source VALUES (1)",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: parent.id",
            false,
            1555,
        ),
        (
            "INSERT INTO text_key VALUES ('k')",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: text_key.k",
            false,
            1555,
        ),
        (
            "INSERT INTO pair_key VALUES (1, 1)",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: pair_key.a, pair_key.b",
            false,
            1555,
        ),
        (
            "UPDATE pair_key SET b = 1 WHERE b = 2",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: pair_key.a, pair_key.b",
            false,
            1555,
        ),
        (
            "INSERT INTO pair_key VALUES (1, 2) ON CONFLICT DO UPDATE SET b = 1",
            turso_core::ConstraintKind::PrimaryKey,
            "UNIQUE constraint failed: pair_key.a, pair_key.b",
            false,
            1555,
        ),
        (
            "INSERT INTO strict_int VALUES ('x')",
            turso_core::ConstraintKind::Datatype,
            "cannot store TEXT value in INTEGER column strict_int.a",
            false,
            3091,
        ),
    ]
}

/// RAISE and a constraint's own ON CONFLICT clause halt with their own
/// resolve type, and the error still reports the halt's result code.
#[test]
fn errors_with_their_own_resolve_type_keep_the_extended_result_code() {
    for (sql, message, extended_code) in [
        ("INSERT INTO raise_abort VALUES (1)", "nope", 1811),
        (
            "INSERT INTO unique_rollback VALUES (1)",
            "UNIQUE constraint failed: unique_rollback.a",
            2067,
        ),
    ] {
        let conn = constraint_test_connection();
        let mut stmt = conn.prepare(sql).unwrap();
        let error = loop {
            match stmt.step_with_structured_errors() {
                Err(error) => break error,
                Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                    stmt.pager.io.step().unwrap();
                }
                Ok(result) => panic!("expected constraint error, got {result:?}: {sql}"),
            }
        };
        assert!(matches!(error, LimboError::Raise(..)), "{sql}: {error:?}");
        assert_eq!(error.to_string(), message, "{sql}");
        assert_eq!(error.sqlite_result_code(), 19, "{sql}");
        assert_eq!(error.sqlite_extended_result_code(), extended_code, "{sql}");
    }
}

fn constraint_test_connection() -> Arc<turso_core::Connection> {
    let conn = open_test_connection().unwrap();
    conn.set_foreign_keys_enabled(true);
    for sql in [
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE, score INTEGER CONSTRAINT positive CHECK (score > 0))",
        "CREATE TABLE child (pid INTEGER REFERENCES parent(id))",
        "CREATE TABLE deferred_child (pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE trigger_source (id INTEGER)",
        "CREATE TRIGGER duplicate_parent AFTER INSERT ON trigger_source BEGIN INSERT INTO parent VALUES (NEW.id, 'b', 1); END",
        "INSERT INTO parent VALUES (1, 'a', 1)",
        "INSERT INTO child VALUES (1)",
        "CREATE TABLE text_key (k TEXT PRIMARY KEY)",
        "INSERT INTO text_key VALUES ('k')",
        "CREATE TABLE pair_key (a, b, PRIMARY KEY (a, b))",
        "INSERT INTO pair_key VALUES (1, 1), (1, 2)",
        "CREATE TABLE strict_int (a INTEGER) STRICT",
        "CREATE TABLE raise_abort (a)",
        "CREATE TRIGGER raise_abort_trigger BEFORE INSERT ON raise_abort BEGIN SELECT RAISE(ABORT, 'nope'); END",
        "CREATE TABLE unique_rollback (a UNIQUE ON CONFLICT ROLLBACK)",
        "INSERT INTO unique_rollback VALUES (1)",
    ] {
        conn.execute(sql).unwrap();
    }
    conn
}

fn open_test_connection() -> turso_core::Result<Arc<turso_core::Connection>> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    db.connect(ConnectOpts::new(SessionRole::Superuser))
}

/// A hash join inside a correlated subquery is built once and probed once per
/// outer row. The budget is forced down so the build spills in every build
/// profile; the second and third passes then go through the grace phase and
/// must count the same rows as the first (#9774).
#[test]
fn test_spilled_hash_join_counts_the_same_on_every_probe_pass() {
    let conn = open_test_connection().unwrap();
    for sql in [
        "CREATE TABLE a(x INTEGER PRIMARY KEY, y INT)",
        "CREATE TABLE c(k INT, v INT)",
        "CREATE TABLE d(k INT, w INT)",
        "INSERT INTO a VALUES (1, 0), (2, 1000000), (3, 1000000)",
        "INSERT INTO c SELECT value % 50, value FROM generate_series(1, 1000)",
        "INSERT INTO d SELECT value % 70, value FROM generate_series(1, 1000)",
    ] {
        conn.execute(sql).unwrap();
    }
    let mut stmt = conn
        .prepare("SELECT a.x, (SELECT count(*) FROM c JOIN d ON c.k = d.k AND d.w < a.y) FROM a")
        .unwrap();
    let mut hash_builds = 0;
    for (insn, _) in &mut Arc::make_mut(&mut stmt.program.prepared).insns {
        if let vdbe::insn::Insn::HashBuild { data } = insn {
            data.mem_budget = 32 * 1024;
            hash_builds += 1;
        }
    }
    assert_eq!(hash_builds, 1, "the join must be built once and reused");

    let rows = stmt.run_collect_rows().unwrap();
    let expected: Vec<Vec<Value>> = [[1, 0], [2, 14400], [3, 14400]]
        .iter()
        .map(|row| row.iter().copied().map(Value::from_i64).collect())
        .collect();
    assert_eq!(rows, expected);

    let metrics = stmt.metrics().hash_join;
    assert!(metrics.spill_chunks > 0, "build did not spill: {metrics:?}");
    assert!(
        metrics.grace_partitions_processed > 0,
        "no pass went through grace: {metrics:?}"
    );
}

#[test]
fn test_expanded_sql() {
    let conn = open_test_connection().unwrap();
    let mut stmt = conn
        .prepare("SELECT ?1, :nm, ':nm' /* ? */, -- ?\n?")
        .unwrap();

    // Unbound parameters render as NULL; markers inside string
    // literals and comments are untouched.
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT NULL, NULL, ':nm' /* ? */, -- ?\nNULL"
    );

    stmt.bind_at(1.try_into().unwrap(), Value::from_i64(42))
        .unwrap();
    let nm = stmt.parameter_index(":nm").unwrap();
    stmt.bind_at(nm, Value::build_text("it's")).unwrap();
    stmt.bind_at(3.try_into().unwrap(), Value::from_slice(&[1, 2]).unwrap())
        .unwrap();
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT 42, 'it''s', ':nm' /* ? */, -- ?\nx'0102'"
    );

    // Bindings survive reset; clear_bindings reverts them to NULL.
    stmt.reset().unwrap();
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT 42, 'it''s', ':nm' /* ? */, -- ?\nx'0102'"
    );
    stmt.clear_bindings();
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT NULL, NULL, ':nm' /* ? */, -- ?\nNULL"
    );

    // A marker can occur several times and renders its value at every
    // occurrence; a bare ? after ?2 takes index 3.
    let mut stmt = conn.prepare("SELECT ?2, :a, ?2, ?").unwrap();
    stmt.bind_at(2.try_into().unwrap(), Value::from_i64(7))
        .unwrap();
    stmt.bind_at(3.try_into().unwrap(), Value::build_text("x"))
        .unwrap();
    stmt.bind_at(4.try_into().unwrap(), Value::from_i64(9))
        .unwrap();
    assert_eq!(stmt.expanded_sql(), "SELECT 7, 'x', 7, 9");
}

#[test]
fn test_tcl_style_parameter_names_bind_and_expand() {
    // The TCL binding passes namespace-qualified variables ($::x,
    // $ns::y) and array elements ($arr(k)) as parameter names. Each
    // spelling is one parameter, found by its full text, and expanded
    // SQL — which re-lexes the statement text — sees the same markers
    // the parse did, so the bound values land in the right places.
    let conn = open_test_connection().unwrap();
    let mut stmt = conn
        .prepare("SELECT $::x, $ns::y, $arr(k), $::x, '$::x'")
        .unwrap();
    let x = stmt.parameter_index("$::x").unwrap();
    let y = stmt.parameter_index("$ns::y").unwrap();
    let k = stmt.parameter_index("$arr(k)").unwrap();
    assert_eq!(stmt.parameters_count(), 3);
    stmt.bind_at(x, Value::from_i64(1)).unwrap();
    stmt.bind_at(y, Value::build_text("two")).unwrap();
    stmt.bind_at(k, Value::from_i64(3)).unwrap();

    assert_eq!(stmt.expanded_sql(), "SELECT 1, 'two', 3, 1, '$::x'");
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::from_i64(1),
            Value::build_text("two"),
            Value::from_i64(3),
            Value::from_i64(1),
            Value::build_text("$::x"),
        ]]
    );
}

#[test]
fn finish_seek_counts_a_deferred_table_seek() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT)")
        .unwrap();
    conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, 10), (2, 2, 20)")
        .unwrap();

    // The index finds the row, and FinishSeek moves the table cursor to it.
    let mut stmt = conn.prepare("UPDATE t SET b = 5 WHERE a = 1").unwrap();
    stmt.run_ignore_rows().unwrap();
    assert_eq!(stmt.metrics().btree_deferred_seeks, 1);
    assert_eq!(stmt.metrics().btree_table_seeks, 1);
}

#[test]
fn test_metrics_persist_across_reset() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.metrics.write().reset();

    let mut stmt = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
    stmt.run_ignore_rows().unwrap();
    assert_eq!(stmt.metrics().rows_written, 1);

    stmt.reset().unwrap();
    assert_eq!(stmt.metrics().rows_written, 1);

    stmt.run_ignore_rows().unwrap();
    assert_eq!(stmt.metrics().rows_written, 2);

    stmt.reset_metrics();
    assert_eq!(stmt.metrics().rows_written, 0);
}

#[test]
fn test_seek_metrics_separate_index_and_table_work() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(a, b)").unwrap();
    conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let mut stmt = conn.prepare("SELECT b FROM t WHERE a = 2").unwrap();
    stmt.run_collect_rows().unwrap();
    let metrics = stmt.metrics();

    assert_eq!(metrics.btree_seeks, 2);
    assert_eq!(metrics.btree_table_seeks, 1);
    assert_eq!(metrics.btree_index_seeks, 1);
    assert_eq!(metrics.btree_deferred_seeks, 1);
}

#[test]
fn test_correlated_subquery_runs_after_selective_join() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE outer_rows(id INTEGER PRIMARY KEY, allowed_id INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE allowed(id INTEGER PRIMARY KEY, enabled INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE inner_rows(outer_id INTEGER, value INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX inner_outer_id ON inner_rows(outer_id)")
        .unwrap();
    conn.execute("INSERT INTO allowed VALUES (1, 0), (2, 1)")
        .unwrap();
    conn.execute("INSERT INTO outer_rows VALUES (1, 1), (2, 1), (3, 2)")
        .unwrap();
    conn.execute(
        "INSERT INTO inner_rows
         SELECT id, CASE WHEN id = 3 THEN 0 ELSE id END FROM outer_rows",
    )
    .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT o.id
             FROM outer_rows o CROSS JOIN allowed a
             WHERE a.id = o.allowed_id
               AND a.enabled = 1
               AND EXISTS (
                   SELECT 1 FROM inner_rows i
                   WHERE i.outer_id = o.id AND i.value <> o.id
               )",
        )
        .unwrap();
    let rows = stmt.run_collect_rows().unwrap();

    assert_eq!(rows, vec![vec![Value::from_i64(3)]]);
    assert_eq!(stmt.metrics().btree_index_seeks, 1);
}

#[test]
fn test_run_with_row_callback_nonblock_collects_all_rows() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let io = conn.db.io.clone();
    let mut stmt = conn.prepare("SELECT x FROM t ORDER BY x").unwrap();

    // Drive the non-blocking runner via the IOResult loop, exactly as a
    // state-machine caller would: collect into an accumulator that persists
    // across yields and wait on each yielded completion.
    let mut collected: Vec<i64> = Vec::new();
    loop {
        let res = stmt
            .run_with_row_callback_nonblock(|row| {
                collected.push(row.get::<i64>(0)?);
                Ok(())
            })
            .unwrap();
        match res {
            turso_core::IOResult::Done(()) => break,
            turso_core::IOResult::IO(c) => c.wait(io.as_ref()).unwrap(),
        }
    }
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_run_ignore_rows_nonblock_completes() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();

    let io = conn.db.io.clone();
    let mut stmt = conn.prepare("INSERT INTO t VALUES (1), (2)").unwrap();
    loop {
        match stmt.run_ignore_rows_nonblock().unwrap() {
            turso_core::IOResult::Done(()) => break,
            turso_core::IOResult::IO(c) => c.wait(io.as_ref()).unwrap(),
        }
    }
    assert_eq!(stmt.metrics().rows_written, 2);
}

#[test]
fn stmt_status_run_counts_each_run() {
    let conn = open_test_connection().unwrap();
    let mut stmt = conn.prepare("SELECT 1").unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Run), 0);

    // A run is counted on its first step, not when it finishes.
    assert!(stmt
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .is_some());
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Run), 1);
    assert!(stmt
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .is_none());
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Run), 1);

    stmt.reset().unwrap();
    stmt.run_ignore_rows().unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Run), 2);
    stmt.reset_stmt_status(StatementStatusCounter::Run);
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Run), 0);

    stmt.reset().unwrap();
    stmt.run_ignore_rows().unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Run), 1);
}

/// Like SQLite, each trigger or foreign key action program that a
/// statement starts counts as a run of that statement.
#[test]
fn stmt_status_run_counts_trigger_and_foreign_key_action_programs() {
    let conn = open_test_connection().unwrap();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("CREATE TABLE log(x)").unwrap();
    conn.execute("CREATE TABLE log2(x)").unwrap();
    // A trigger program with two statements is still one run.
    conn.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         INSERT INTO log VALUES (NEW.x); INSERT INTO log VALUES (NEW.x); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER tr_log AFTER INSERT ON log BEGIN INSERT INTO log2 VALUES (NEW.x); END",
    )
    .unwrap();
    conn.execute("CREATE TABLE p(a PRIMARY KEY)").unwrap();
    conn.execute("CREATE TABLE c(b REFERENCES p(a) ON DELETE CASCADE)")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO c VALUES (1), (2)").unwrap();

    let mut insert = conn.prepare("INSERT INTO t VALUES (1), (2)").unwrap();
    insert.run_ignore_rows().unwrap();
    // 1 for the INSERT, 2 for `tr`, 4 for `tr_log`.
    assert_eq!(insert.stmt_status(StatementStatusCounter::Run), 7);
    let vm_steps_one_run = insert.stmt_status(StatementStatusCounter::VmStep);

    // Resetting the statement keeps the counts of the trigger programs.
    insert.reset().unwrap();
    insert.run_ignore_rows().unwrap();
    assert_eq!(insert.stmt_status(StatementStatusCounter::Run), 14);
    assert_eq!(
        insert.stmt_status(StatementStatusCounter::VmStep),
        2 * vm_steps_one_run
    );

    let mut delete = conn.prepare("DELETE FROM p").unwrap();
    delete.run_ignore_rows().unwrap();
    assert_eq!(delete.stmt_status(StatementStatusCounter::Run), 3);
}

#[test]
fn test_metrics_include_subprogram_writes() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE src(x)").unwrap();
    conn.execute("CREATE TABLE log(x)").unwrap();
    conn.execute(
        "CREATE TRIGGER src_log AFTER INSERT ON src BEGIN INSERT INTO log VALUES (new.x); END",
    )
    .unwrap();

    let mut stmt = conn.prepare("INSERT INTO src VALUES (1), (2)").unwrap();
    stmt.run_ignore_rows().unwrap();

    assert_eq!(
        stmt.metrics().rows_written,
        6,
        "cumulative metrics should include root and trigger writes"
    );
}
