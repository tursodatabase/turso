//! Properties for DML RETURNING row images.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Schema},
    sync::Arc,
    translate::semantic::{analyze, context::SemanticContext, AnalyzeInput},
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts},
        insn::Insn,
    },
    QueryMode, SymbolTable,
};

fn parse_statement(sql: &str) -> ast::Stmt {
    let command = Parser::new(sql.as_bytes())
        .next_cmd()
        .expect("generated SQL parses")
        .expect("generated SQL contains a statement");
    let ast::Cmd::Stmt(statement) = command else {
        panic!("generated SQL contains a statement");
    };
    statement
}

fn program() -> ProgramBuilder {
    ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 8))
}

// Examples: `INSERT ... RETURNING rowid, c0 + 7` and
// `UPDATE ... RETURNING rowid, c0 + 7` must evaluate against the complete NEW
// HIR register row and emit exactly two fields only after the table Insert.
// The result therefore describes a row that was actually written, not OLD or
// an intermediate assignment image.
#[hegel::test]
fn insert_and_update_return_the_written_hir_row(tc: hegel::TestCase) {
    let update = tc.draw(generators::booleans());
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let table = BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER)", 12)
        .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if update {
        format!("UPDATE items SET c0 = c0 + 1 RETURNING rowid, c0 + {offset}")
    } else {
        format!("INSERT INTO items VALUES (1) RETURNING rowid, c0 + {offset}")
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DML has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("DML RETURNING lowers without a catalog");

    let write = program
        .insns
        .iter()
        .rposition(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items")
        })
        .expect("the NEW row is written");
    let result = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 2, .. }))
        .expect("two RETURNING fields are emitted");
    assert!(write < result);
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Add { .. })));
}
