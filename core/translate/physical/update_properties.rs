//! Properties for stable-rowset UPDATE emission from closed HIR.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Schema},
    sync::Arc,
    translate::semantic::{
        analyze,
        context::SemanticContext,
        hir::{Expr, HirRoot, TargetColumn},
        AnalyzeInput,
    },
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

// Example: for `items(c0, c1, c2 AS (c0 + c1) VIRTUAL)`,
// `UPDATE items SET c0 = c1 + 7 WHERE c0` must first collect every matching
// rowid, then seek each stable rowid, evaluate the assignment against OLD c1,
// and recompute c2 from the NEW c0/c1 registers. No table write may happen
// while the WHERE scan is still adding rowids.
#[hegel::test]
fn update_uses_a_stable_rowset_and_recomputes_the_hir_row(tc: hegel::TestCase) {
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let table = BTreeTable::from_sql(
        "CREATE TABLE items(\
         c0 INTEGER, \
         c1 INTEGER, \
         c2 INTEGER GENERATED ALWAYS AS (c0 + c1) VIRTUAL)",
        12,
    )
    .expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("UPDATE items SET c0 = c1 + {offset} WHERE c0"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UPDATE has valid SQL meaning");
    let HirRoot::Update(update) = &document.root else {
        panic!("UPDATE syntax produces an UPDATE HIR root");
    };
    assert!(matches!(
        update.assignments[0].columns.as_slice(),
        [TargetColumn::Column(0)]
    ));
    let Expr::Binary { lhs, .. } = &update.assignments[0].value else {
        panic!("assignment is the resolved addition");
    };
    let Expr::Column(reference) = lhs.as_ref() else {
        panic!("assignment reads one resolved source column");
    };
    assert_eq!(reference.source, update.target);
    assert_eq!(reference.column, 1);
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("UPDATE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all stable-rowset branches are closed");

    let position_of = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .position(|(instruction, _)| predicate(instruction))
            .expect("expected instruction exists")
    };
    let add_to_rowset = position_of(&|instruction| matches!(instruction, Insn::RowSetAdd { .. }));
    let read_rowset = position_of(&|instruction| matches!(instruction, Insn::RowSetRead { .. }));
    let seek_row = position_of(&|instruction| matches!(instruction, Insn::NotExists { .. }));
    let delete = position_of(&|instruction| {
        matches!(
            instruction,
            Insn::Delete {
                is_part_of_update: true,
                ..
            }
        )
    });
    let insert = position_of(
        &|instruction| matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items"),
    );
    assert!(add_to_rowset < read_rowset);
    assert!(read_rowset < seek_row && seek_row < delete && delete < insert);
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
            .count(),
        2,
        "one addition evaluates the assignment and one recomputes c2"
    );
    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::MakeRecord { count: 2, .. })));
}
