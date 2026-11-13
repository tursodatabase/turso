use turso_core::alloc::TryClone;
use turso_core::io::MemoryIO;
use turso_core::namespace::NamespaceId;
use turso_core::schema::{Table, SQLITE_SEQUENCE_TABLE_NAME};
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::translate::*;
use turso_core::vdbe::builder::ProgramBuilder;
use turso_core::vdbe::builder::ProgramBuilderOpts;
use turso_core::vdbe::builder::QueryMode;
use turso_core::vdbe::insn::{ClearBtreeCount, Insn};
use turso_core::ConnectOpts;
use turso_core::Database;
use turso_core::SymbolTable;
use turso_parser::ast;
use turso_sqlite::SqliteDialect;

#[test]
fn view_expansion_restores_context_after_errors() {
    let mut program =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 0, 0));
    program.push_cte_being_defined("caller".into());

    let result = program.with_view_expansion(0, "v", |program| {
        assert!(!program.is_cte_being_defined("caller"));
        program.push_cte_being_defined("outer_view".into());

        // The same view name in another schema is not a circular reference.
        program.with_view_expansion(1, "v", |program| {
            assert!(!program.is_cte_being_defined("outer_view"));
            Ok(())
        })?;
        assert!(program.is_cte_being_defined("outer_view"));

        // An indirect circular reference must unwind both expansion scopes.
        program.with_view_expansion(0, "nested", |program| {
            program.with_view_expansion(0, "v", |_| -> turso_core::Result<()> {
                panic!("a circular view must not be expanded")
            })?;
            Ok(())
        })?;
        Ok(())
    });
    assert!(
        matches!(result, Err(turso_core::LimboError::ParseError(ref message))
        if message == "view v is circularly defined")
    );
    assert!(program.is_cte_being_defined("caller"));
    assert!(!program.is_cte_being_defined("outer_view"));

    // Both names can be expanded again after the failed expansion.
    program
        .with_view_expansion(0, "v", |program| {
            program.with_view_expansion(0, "nested", |_| Ok(()))
        })
        .unwrap();
    assert!(program.is_cte_being_defined("caller"));
}

/// Verify that REGEXP produces the correct error when no regexp function is registered.
#[test]
fn test_regexp_no_function_registered() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let schema = db.schema.lock().clone();
    let pager = conn.pager.load().clone();

    // Use an empty SymbolTable so regexp() is not available.
    let empty_syms = SymbolTable::new();
    let cmd = turso_core::dialect::parse_statement(&SqliteDialect, "SELECT 'x' REGEXP 'y'")
        .transpose()
        .unwrap()
        .unwrap();
    let stmt = match cmd {
        ast::Cmd::Stmt(s) => s,
        _ => panic!("expected statement"),
    };

    let result = translate(
        &schema,
        stmt,
        pager,
        conn,
        &empty_syms,
        QueryMode::Normal,
        "",
        turso_core::statement::StatementOrigin::Root,
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("no such function: regexp"),
        "expected 'no such function: regexp', got: {err}"
    );
}

#[test]
fn nth_value_reads_from_saved_rows_without_an_accumulator() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE items(value)").unwrap();

    let statement = conn
        .prepare("SELECT nth_value(value, 2) OVER (ORDER BY value) FROM items")
        .unwrap();
    let instructions = &statement.get_program().insns;

    let missing_row_target = instructions
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::SeekRowid { target_pc, .. } => Some(target_pc.as_offset_int() as usize),
            _ => None,
        })
        .expect("nth_value must read its answer from the saved window rows");
    assert!(
        matches!(
            &instructions[missing_row_target].0,
            Insn::Halt { on_error: None, .. }
        ),
        "a missing saved row must stop execution instead of returning NULL"
    );
    assert!(
        instructions.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::AggStep { .. } | Insn::AggValue { .. }
        )),
        "nth_value must not keep another copy of saved values in an accumulator"
    );
}

#[test]
fn delete_without_where_clears_table_and_index_btrees() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX items_name ON items(name)")
        .unwrap();
    conn.execute("CREATE INDEX items_score ON items(score) WHERE score > 0")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(1, 'one', -1), (2, 'two', 0), (3, 'three', 1)")
        .unwrap();

    let mut statement = conn.prepare("DELETE FROM items").unwrap();
    let instructions = &statement.get_program().insns;
    let clears = instructions
        .iter()
        .filter_map(|(instruction, _)| match instruction {
            Insn::ClearBtree { count, .. } => Some(*count),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        clears,
        vec![
            ClearBtreeCount::RowsWritten,
            ClearBtreeCount::RowsWritten,
            ClearBtreeCount::ChangesAndRowsWritten,
        ],
        "both indexes and then the table must be cleared, and only the table counts changes"
    );
    assert!(
        instructions
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Delete { .. })),
        "a whole-table delete must not emit the row-at-a-time Delete opcode"
    );

    statement.run_ignore_rows().unwrap();
    assert_eq!(statement.n_change(), 3);
    assert_eq!(
        statement.metrics().rows_written,
        7,
        "metrics must count table, full-index, and partial-index entries"
    );

    statement.reset().unwrap();
    statement.reset_metrics();
    statement.run_ignore_rows().unwrap();
    assert_eq!(statement.n_change(), 0);
    assert_eq!(statement.metrics().rows_written, 0);

    statement.reset().unwrap();
    statement.reset_metrics();
    conn.execute("INSERT INTO items VALUES(4, 'four', -1), (5, 'five', 1)")
        .unwrap();
    statement.run_ignore_rows().unwrap();
    assert_eq!(statement.n_change(), 2);
    assert_eq!(
        statement.metrics().rows_written,
        5,
        "reused statements must recount table and index entries"
    );

    statement.reset().unwrap();
    statement.reset_metrics();
    conn.execute(
        "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM c WHERE x < 3000) \
         INSERT INTO items SELECT x, printf('name-%0200d', x), x % 2 FROM c",
    )
    .unwrap();
    statement.run_ignore_rows().unwrap();
    assert_eq!(statement.n_change(), 3000);
    assert_eq!(
        statement.metrics().rows_written,
        3000 + 3000 + 1500,
        "interior cells of the index B-trees are entries, interior cells of the table are not"
    );
}

#[test]
fn delete_keeps_row_loop_when_rows_need_individual_processing() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    for sql in [
        "DELETE FROM items WHERE 1",
        "DELETE FROM items RETURNING id",
    ] {
        let statement = conn.prepare(sql).unwrap();
        assert!(
            statement
                .get_program()
                .insns
                .iter()
                .all(|(instruction, _)| !matches!(instruction, Insn::ClearBtree { .. })),
            "{sql} must process rows individually"
        );
    }

    conn.execute("CREATE TABLE deleted_items(id)").unwrap();
    conn.execute(
        "CREATE TRIGGER log_item_delete AFTER DELETE ON items \
         BEGIN INSERT INTO deleted_items VALUES(old.id); END",
    )
    .unwrap();
    let statement = conn.prepare("DELETE FROM items").unwrap();
    assert!(
        statement
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::ClearBtree { .. })),
        "DELETE triggers require the row loop"
    );

    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parents(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE children(parent_id REFERENCES parents(id))")
        .unwrap();
    let statement = conn.prepare("DELETE FROM parents").unwrap();
    assert!(
        statement
            .get_program()
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::ClearBtree { .. })),
        "foreign-key actions require the row loop"
    );
}

#[test]
fn test_insert_autoincrement_with_malformed_sqlite_sequence_is_corrupt() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();

    let mut schema = db.schema.lock().as_ref().try_clone().unwrap();
    let seq_root_page = schema
        .get_btree_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME)
        .expect("sqlite_sequence should exist after creating AUTOINCREMENT table")
        .root_page;
    let malformed_seq = crate::table_from_sql("CREATE TABLE sqlite_sequence(name)", seq_root_page)
        .expect("malformed sqlite_sequence SQL should parse");
    schema.insert_table(
        NamespaceId::ROOT,
        SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        Arc::new(Table::BTree(Arc::new(malformed_seq))),
    );

    let pager = conn.pager.load().clone();
    let syms = SymbolTable::new();

    let cmd = turso_core::dialect::parse_statement(&SqliteDialect, "INSERT INTO t(v) VALUES('x')")
        .transpose()
        .unwrap()
        .unwrap();
    let stmt = match cmd {
        ast::Cmd::Stmt(s) => s,
        _ => panic!("expected statement"),
    };

    let err = translate(
        &schema,
        stmt,
        pager,
        conn,
        &syms,
        QueryMode::Normal,
        "",
        turso_core::statement::StatementOrigin::Root,
    )
    .expect_err("translation should fail with malformed sqlite_sequence");
    match err {
        turso_core::LimboError::Corrupt(msg) => {
            assert!(
                msg.contains("sqlite_sequence"),
                "expected sqlite_sequence corruption error, got: {msg}"
            );
        }
        other => panic!("expected LimboError::Corrupt, got: {other}"),
    }
}

#[test]
fn test_insert_autoincrement_with_missing_sqlite_sequence_is_corrupt() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();

    let mut schema = db.schema.lock().as_ref().try_clone().unwrap();
    schema.take_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME);

    let pager = conn.pager.load().clone();
    let syms = SymbolTable::new();

    let cmd = turso_core::dialect::parse_statement(&SqliteDialect, "INSERT INTO t(v) VALUES('x')")
        .transpose()
        .unwrap()
        .unwrap();
    let stmt = match cmd {
        ast::Cmd::Stmt(s) => s,
        _ => panic!("expected statement"),
    };

    let err = translate(
        &schema,
        stmt,
        pager,
        conn,
        &syms,
        QueryMode::Normal,
        "",
        turso_core::statement::StatementOrigin::Root,
    )
    .expect_err("translation should fail with missing sqlite_sequence");
    match err {
        turso_core::LimboError::Corrupt(msg) => {
            assert!(
                msg.contains("missing sqlite_sequence"),
                "expected missing sqlite_sequence error, got: {msg}"
            );
        }
        other => panic!("expected LimboError::Corrupt, got: {other}"),
    }
}

#[test]
fn test_trigger_compile_error_does_not_poison_future_insert_compilation() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE ref(x);").unwrap();
    conn.execute("CREATE TABLE t(a INTEGER);").unwrap();
    conn.execute("CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT * FROM ref; END;")
        .unwrap();
    conn.execute("DROP TABLE ref;").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1);")
        .expect_err("single-row insert should fail while trigger references dropped table");
    assert!(
        err.to_string().contains("no such table: ref"),
        "expected missing-table error, got: {err}"
    );

    let err = conn
        .execute("INSERT INTO t VALUES (2), (3);")
        .expect_err("multi-row insert should still fail instead of skipping the poisoned trigger");
    assert!(
        err.to_string().contains("no such table: ref"),
        "expected missing-table error, got: {err}"
    );
}
