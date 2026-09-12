use super::*;
use crate::alloc::TryClone;
use crate::io::MemoryIO;
use crate::schema::{BTreeTable, Table, SQLITE_SEQUENCE_TABLE_NAME};
use crate::vdbe::insn::Insn;
use crate::Database;
use crate::SqliteDialect;

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
            program.with_view_expansion(0, "v", |_| -> crate::Result<()> {
                panic!("a circular view must not be expanded")
            })?;
            Ok(())
        })?;
        Ok(())
    });
    assert!(
        matches!(result, Err(crate::LimboError::ParseError(ref message))
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
    let conn = db.connect().unwrap();
    let schema = db.schema.lock().clone();
    let pager = conn.pager.load().clone();

    // Use an empty SymbolTable so regexp() is not available.
    let empty_syms = SymbolTable::new();
    let mut parser = turso_parser::parser::Parser::new(b"SELECT 'x' REGEXP 'y'");
    let cmd = parser.next().unwrap().unwrap();
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
        crate::statement::StatementOrigin::Root,
        &crate::connection::PrepareOptions::default(),
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
    let conn = db.connect().unwrap();
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
fn test_insert_autoincrement_with_malformed_sqlite_sequence_is_corrupt() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();

    let mut schema = db.schema.lock().as_ref().try_clone().unwrap();
    let seq_root_page = schema
        .get_btree_table(SQLITE_SEQUENCE_TABLE_NAME)
        .expect("sqlite_sequence should exist after creating AUTOINCREMENT table")
        .root_page;
    let malformed_seq = BTreeTable::from_sql("CREATE TABLE sqlite_sequence(name)", seq_root_page)
        .expect("malformed sqlite_sequence SQL should parse");
    schema.tables.insert(
        SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        Arc::new(Table::BTree(Arc::new(malformed_seq))),
    );

    let pager = conn.pager.load().clone();
    let syms = SymbolTable::new();

    let mut parser = turso_parser::parser::Parser::new(b"INSERT INTO t(v) VALUES('x')");
    let cmd = parser.next().unwrap().unwrap();
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
        crate::statement::StatementOrigin::Root,
        &crate::connection::PrepareOptions::default(),
    )
    .expect_err("translation should fail with malformed sqlite_sequence");
    match err {
        crate::LimboError::Corrupt(msg) => {
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
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();

    let mut schema = db.schema.lock().as_ref().try_clone().unwrap();
    schema.tables.remove(SQLITE_SEQUENCE_TABLE_NAME);

    let pager = conn.pager.load().clone();
    let syms = SymbolTable::new();

    let mut parser = turso_parser::parser::Parser::new(b"INSERT INTO t(v) VALUES('x')");
    let cmd = parser.next().unwrap().unwrap();
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
        crate::statement::StatementOrigin::Root,
        &crate::connection::PrepareOptions::default(),
    )
    .expect_err("translation should fail with missing sqlite_sequence");
    match err {
        crate::LimboError::Corrupt(msg) => {
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
    let conn = db.connect().unwrap();

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
