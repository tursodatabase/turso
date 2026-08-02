//! Properties for direct INSERT emission from closed HIR.

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
        hir::{ColumnReadExpression, HirRoot, InsertSource, TargetColumn},
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

// Example: for
// `items(c0, c1 DEFAULT 11, c2, c3 AS (c0 + c1) VIRTUAL)`,
// `INSERT INTO items(c0, c2) VALUES (7, 9), (8, 10)` must build each complete
// logical row from the exact HIR positions: supplied c0/c2, frozen default c1,
// then frozen generated c3. Once the schema is dropped, emission must still
// compute the complete four-field logical row, then make one three-field
// stored record and one table insert for every VALUES row.
#[hegel::test]
fn values_insert_builds_complete_rows_from_hir(tc: hegel::TestCase) {
    let default_value =
        i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31))) + 40;
    let row_count = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(5)));
    let rows = (0..row_count)
        .map(|position| format!("({}, {})", 100 + position, 200 + position))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(
        &format!(
            "CREATE TABLE items(\
             c0 INTEGER, \
             c1 INTEGER DEFAULT {default_value}, \
             c2 INTEGER, \
             c3 INTEGER GENERATED ALWAYS AS (c0 + c1) VIRTUAL)"
        ),
        9,
    )
    .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("INSERT INTO items(c0, c2) VALUES {rows}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated INSERT has valid SQL meaning");
    let HirRoot::Insert(insert) = &document.root else {
        panic!("INSERT syntax produces an INSERT HIR root");
    };
    assert!(matches!(
        insert.columns.as_slice(),
        [TargetColumn::Column(0), TargetColumn::Column(2)]
    ));
    assert!(insert.defaults.iter().any(|default| default.column == 1));
    assert!(matches!(&insert.source, InsertSource::Values(values) if values.len() == row_count));
    let target = document
        .source(insert.target)
        .expect("target source exists");
    assert!(matches!(
        target.generated_expressions[3],
        ColumnReadExpression::Planned(_)
    ));
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("VALUES INSERT lowers without a catalog");

    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::NewRowid { .. }))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::Integer { value, .. } if *value == default_value
            ))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::MakeRecord { count: 3, .. }))
            .count(),
        row_count
    );
    assert_eq!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(
                instruction,
                Insn::Insert { table_name, .. } if table_name == "items"
            ))
            .count(),
        row_count
    );
}

// Examples:
// - `INSERT INTO items(rowid, c0) VALUES (7, 1)` must validate rowid 7 as an
//   integer and reject an existing table key before any secondary-index write.
// - `INSERT INTO items(id, c0) VALUES (NULL, 1)` for
//   `id INTEGER PRIMARY KEY` must generate a rowid, copy it into logical id,
//   store NULL in id's record field, and use that rowid as the table key.
#[hegel::test]
fn explicit_rowid_and_integer_primary_key_share_one_key_path(tc: hegel::TestCase) {
    let alias = tc.draw(generators::booleans());
    let null_key = tc.draw(generators::booleans());
    let key = if null_key { "NULL" } else { "7" };
    let create = if alias {
        "CREATE TABLE items(id INTEGER PRIMARY KEY, c0 INTEGER)"
    } else {
        "CREATE TABLE items(c0 INTEGER)"
    };
    let insert = if alias {
        format!("INSERT INTO items(id, c0) VALUES ({key}, 1)")
    } else {
        format!("INSERT INTO items(rowid, c0) VALUES ({key}, 1)")
    };
    let table = BTreeTable::from_sql(create, 9).expect("fixture table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&insert);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated INSERT has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("explicit-rowid INSERT lowers without a catalog");
    program
        .resolve_labels()
        .expect("all rowid branches are closed");

    let positions = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (instruction, _))| predicate(instruction).then_some(position))
            .collect::<Vec<_>>()
    };
    let new_rowid = positions(&|instruction| matches!(instruction, Insn::NewRowid { .. }));
    let must_be_int = positions(&|instruction| matches!(instruction, Insn::MustBeInt { .. }));
    let uniqueness = positions(&|instruction| matches!(instruction, Insn::NotExists { .. }));
    let table_insert = positions(&|instruction| matches!(instruction, Insn::Insert { .. }));
    assert_eq!(new_rowid.len(), 1);
    assert_eq!(must_be_int.len(), 1);
    assert_eq!(uniqueness.len(), 1);
    assert_eq!(table_insert.len(), 1);
    assert!(uniqueness[0] < table_insert[0]);
    if alias {
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SoftNull { .. })));
    }
}
