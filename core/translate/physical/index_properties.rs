//! Properties for shared HIR secondary-index maintenance.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    schema::{BTreeTable, Index, Schema},
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

// Examples:
// - `INSERT INTO items VALUES (1, 2)` with `UNIQUE INDEX i ON items(c0)`
//   must probe i, insert i's packed key, then insert the table record.
// - `UPDATE items SET c0 = c0 + 1` with
//   `INDEX i ON items(c0 + 7) WHERE c1 > 3` must build OLD and NEW expression
//   keys from frozen HIR, probe the NEW unique key when needed, then replace
//   the index key before either overwriting the stable table row or moving it.
//   For example, `UPDATE items SET c0 = 7 WHERE rowid = 5` keeps rowid 5 and
//   skips table Delete, while `UPDATE items SET rowid = 9 WHERE rowid = 5`
//   deletes rowid 5 before inserting rowid 9.
// - `DELETE FROM items WHERE c1` must perform IdxDelete before table Delete.
// Dropping the schema before emission proves ordinary, expression, and partial
// index behavior comes from the closed HIR document.
#[hegel::test]
fn dml_maintains_frozen_hir_indexes_in_sqlite_order(tc: hegel::TestCase) {
    let operation = tc.draw(generators::integers::<u8>().max_value(2));
    let expression_index = tc.draw(generators::booleans());
    let partial_index = tc.draw(generators::booleans());
    let unique = tc.draw(generators::booleans());
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let threshold = i64::from(tc.draw(generators::integers::<u8>().max_value(31)));

    let table = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(c0 INTEGER, c1 INTEGER)", 12)
            .expect("fixture table SQL is valid"),
    );
    let symbols = SymbolTable::new();
    let key = if expression_index {
        format!("c0 + {offset}")
    } else {
        "c0".to_string()
    };
    let predicate = partial_index
        .then(|| format!(" WHERE c1 > {threshold}"))
        .unwrap_or_default();
    let unique_sql = if unique { "UNIQUE " } else { "" };
    let index = Index::from_sql(
        &symbols,
        &format!("CREATE {unique_sql}INDEX items_i ON items({key}){predicate}"),
        13,
        &table,
    )
    .expect("generated index SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(table).expect("items is unique");
    schema
        .add_index(Arc::new(index))
        .expect("items_i is unique");
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = match operation {
        0 => "INSERT INTO items VALUES (1, 2)",
        1 => "UPDATE items SET c0 = c0 + 1 WHERE c1",
        _ => "DELETE FROM items WHERE c1",
    };
    let statement = parse_statement(sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DML has valid SQL meaning");
    let target = match &document.root {
        crate::translate::semantic::hir::HirRoot::Insert(insert) => insert.target,
        crate::translate::semantic::hir::HirRoot::Update(update) => update.target,
        crate::translate::semantic::hir::HirRoot::Delete(delete) => delete.target,
        _ => panic!("fixture produces a DML root"),
    };
    let source = document.source(target).expect("DML target exists");
    assert_eq!(source.index_expressions.len(), 1);
    assert_eq!(
        source.index_expressions[0].columns[0].is_some(),
        expression_index
    );
    assert_eq!(
        source.index_expressions[0].predicate.is_some(),
        partial_index
    );
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("indexed DML lowers without a catalog");
    program
        .resolve_labels()
        .expect("all index-maintenance branches are closed");

    let positions = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (instruction, _))| predicate(instruction).then_some(position))
            .collect::<Vec<_>>()
    };
    let index_deletes = positions(&|instruction| matches!(instruction, Insn::IdxDelete { .. }));
    let index_inserts = positions(&|instruction| matches!(instruction, Insn::IdxInsert { .. }));
    let table_deletes = positions(&|instruction| matches!(instruction, Insn::Delete { .. }));
    let table_inserts = positions(
        &|instruction| matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items"),
    );

    match operation {
        0 => {
            assert!(index_deletes.is_empty());
            assert_eq!(index_inserts.len(), 1);
            assert_eq!(table_inserts.len(), 1);
            assert!(index_inserts[0] < table_inserts[0]);
            assert_eq!(
                positions(&|instruction| matches!(instruction, Insn::NoConflict { .. })).len(),
                usize::from(unique)
            );
        }
        1 => {
            assert_eq!(index_deletes.len(), 1);
            assert_eq!(index_inserts.len(), 1);
            assert_eq!(table_deletes.len(), 1);
            // The bytecode has mutually exclusive inserts: one moves a changed
            // rowid and the other overwrites an unchanged rowid.
            assert_eq!(table_inserts.len(), 2);
            assert!(
                index_deletes[0] < table_deletes[0]
                    && table_deletes[0] < index_inserts[0]
                    && table_inserts
                        .iter()
                        .all(|table_insert| index_inserts[0] < *table_insert)
            );
            assert_eq!(
                positions(&|instruction| matches!(instruction, Insn::NoConflict { .. })).len(),
                usize::from(unique)
            );
        }
        _ => {
            assert_eq!(index_deletes.len(), 1);
            assert!(index_inserts.is_empty());
            assert_eq!(table_deletes.len(), 1);
            assert!(index_deletes[0] < table_deletes[0]);
        }
    }
    assert_eq!(
        positions(&|instruction| matches!(instruction, Insn::Add { .. })).len(),
        usize::from(expression_index) * if operation == 1 { 2 } else { 1 }
            + usize::from(operation == 1)
    );
    assert_eq!(
        positions(&|instruction| matches!(instruction, Insn::IfNot { .. })).len(),
        usize::from(operation != 0)
            + if partial_index {
                match operation {
                    0 => 1 + usize::from(unique),
                    1 => 2 + usize::from(unique),
                    _ => 1,
                }
            } else {
                0
            }
    );
}
