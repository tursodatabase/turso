//! Properties for shared HIR row constraints.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    error::SQLITE_CONSTRAINT_CHECK,
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

// Examples: with `CHECK(c0 > 7)`, both `INSERT INTO items VALUES (9)` and
// `UPDATE items SET c0 = c0 + 1` must evaluate the frozen HIR CHECK against
// the complete NEW row. NULL and true branch past the error; false halts before
// any table or index mutation. Schema removal before emission proves the CHECK
// text is not parsed or rebound by the physical layer.
#[hegel::test]
fn insert_and_update_check_the_complete_hir_row_before_writing(tc: hegel::TestCase) {
    let update = tc.draw(generators::booleans());
    let threshold = i64::from(tc.draw(generators::integers::<u8>().max_value(31)));
    let table = BTreeTable::from_sql(
        &format!("CREATE TABLE items(c0 INTEGER CHECK(c0 > {threshold}))"),
        12,
    )
    .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if update {
        "UPDATE items SET c0 = c0 + 1"
    } else {
        "INSERT INTO items VALUES (99)"
    };
    let statement = parse_statement(sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DML has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("CHECK-constrained DML lowers without a catalog");
    program
        .resolve_labels()
        .expect("all CHECK branches are closed");

    let position_of = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .position(|(instruction, _)| predicate(instruction))
            .expect("expected instruction exists")
    };
    let null_pass = position_of(&|instruction| matches!(instruction, Insn::IsNull { .. }));
    let true_pass = position_of(&|instruction| matches!(instruction, Insn::If { .. }));
    let failure = position_of(
        &|instruction| matches!(instruction, Insn::Halt { err_code, .. } if *err_code == SQLITE_CONSTRAINT_CHECK),
    );
    let first_write = position_of(&|instruction| {
        matches!(
            instruction,
            Insn::Insert { .. }
                | Insn::Delete { .. }
                | Insn::IdxInsert { .. }
                | Insn::IdxDelete { .. }
        )
    });
    assert!(null_pass < true_pass && true_pass < failure && failure < first_write);
}
