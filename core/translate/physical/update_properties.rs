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

// Examples:
// - `UPDATE items SET c1 = incoming.c1 + 7 FROM incoming
//   WHERE items.c0 = incoming.c0` must evaluate the SET value while both exact
//   SourceIds are bound, then save it under the target rowid before any write.
// - If several incoming rows match one item, inserting candidates by target
//   rowid leaves one stable value for the later write phase; FROM cursors are
//   no longer needed when the table and its indexes are changed.
// - RETURNING reads the rebuilt target NEW row, not the materialized FROM row.
#[hegel::test]
fn update_from_materializes_hir_assignment_values_before_writing(tc: hegel::TestCase) {
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let items = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER)", 12)
            .expect("fixture target SQL is valid"),
    );
    let incoming = Arc::new(
        BTreeTable::from_sql("CREATE TABLE incoming(c0 INTEGER, c1 INTEGER)", 13)
            .expect("fixture source SQL is valid"),
    );
    let mut schema = Schema::new();
    schema.add_btree_table(items).expect("items is unique");
    schema
        .add_btree_table(incoming)
        .expect("incoming is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "UPDATE items SET c1 = incoming.c1 + {offset} FROM incoming \
         WHERE items.c0 = incoming.c0 RETURNING c0, c1"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UPDATE FROM has valid SQL meaning");
    let HirRoot::Update(update) = &document.root else {
        panic!("fixture produces UPDATE HIR");
    };
    let from = update.from.as_ref().expect("UPDATE has FROM");
    let mut assignment_sources = Vec::new();
    update.assignments[0].value.walk(&mut |expression| {
        if let Expr::Column(column) = expression {
            assignment_sources.push(column.source);
        }
    });
    assert_eq!(assignment_sources, vec![from.first]);
    assert_ne!(from.first, update.target);
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("UPDATE FROM lowers without a catalog");
    program
        .resolve_labels()
        .expect("all UPDATE FROM branches are closed");

    let candidate_insert = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name.starts_with("update_from_"))
        })
        .expect("FROM values are materialized by target rowid");
    let delete = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Delete {
                    is_part_of_update: true,
                    ..
                }
            )
        })
        .expect("target OLD row is deleted");
    let target_insert = program
        .insns
        .iter()
        .enumerate()
        .skip(delete)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items")
                .then_some(position)
        })
        .expect("target NEW row is inserted");
    let returning = program
        .insns
        .iter()
        .enumerate()
        .skip(target_insert)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::ResultRow { .. }).then_some(position)
        })
        .expect("UPDATE FROM returns target NEW");
    let assignment = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
        .expect("SET expression is evaluated");
    assert!(assignment < candidate_insert && candidate_insert < delete);
    assert!(delete < target_insert && target_insert < returning);
    assert!(!program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::RowSetAdd { .. })));
}
