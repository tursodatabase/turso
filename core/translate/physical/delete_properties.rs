//! Properties for direct DELETE emission from closed HIR.

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
        hir::{ColumnReadExpression, Expr, HirRoot},
        AnalyzeInput,
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts},
        insn::{Insn, RegisterOrLiteral},
        BranchOffset,
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

// Example: after binding `DELETE FROM items WHERE c7`, direct emission must
// read position seven from the resolved target cursor, skip false or NULL
// rows, delete matching rows, and advance that same cursor. Dropping `Schema`
// first proves that target and column names are never resolved again.
#[hegel::test]
fn a_simple_delete_emits_only_from_closed_hir(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let predicate_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!("DELETE FROM items WHERE c{predicate_position}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DELETE has valid SQL meaning");

    let HirRoot::Delete(delete) = &document.root else {
        panic!("DELETE syntax produces a DELETE HIR root");
    };
    let Some(Expr::Column(predicate)) = &delete.predicate else {
        panic!("the generated WHERE is one bound column");
    };
    assert_eq!(predicate.source, delete.target);
    assert_eq!(predicate.column, predicate_position);

    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("simple DELETE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all direct-emission branches are closed");

    let (open_position, cursor) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(7),
                db: 0,
            } => Some((position, *cursor_id)),
            _ => None,
        })
        .expect("the resolved target is opened for writing");
    let position_of = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .position(|(instruction, _)| predicate(instruction))
            .expect("expected instruction exists")
    };
    let rewind_position = position_of(
        &|instruction| matches!(instruction, Insn::Rewind { cursor_id, .. } if *cursor_id == cursor),
    );
    let column_position = position_of(&|instruction| {
        matches!(
            instruction,
            Insn::Column {
                cursor_id,
                column,
                ..
            } if *cursor_id == cursor && *column == predicate_position
        )
    });
    let filter_position = position_of(&|instruction| matches!(instruction, Insn::IfNot { .. }));
    let delete_position = position_of(&|instruction| {
        matches!(
            instruction,
            Insn::Delete {
                cursor_id,
                table_name,
                is_part_of_update: false,
            } if *cursor_id == cursor && table_name == "items"
        )
    });
    let next_position = position_of(
        &|instruction| matches!(instruction, Insn::Next { cursor_id, .. } if *cursor_id == cursor),
    );
    let close_position = position_of(
        &|instruction| matches!(instruction, Insn::Close { cursor_id } if *cursor_id == cursor),
    );

    assert!(
        open_position < rewind_position
            && rewind_position < column_position
            && column_position < filter_position
            && filter_position < delete_position
            && delete_position < next_position
            && next_position < close_position
    );
    assert!(matches!(
        &program.insns[rewind_position].0,
        Insn::Rewind {
            pc_if_empty: BranchOffset::Offset(target),
            ..
        } if *target as usize == close_position
    ));
    assert!(matches!(
        &program.insns[filter_position].0,
        Insn::IfNot {
            target_pc: BranchOffset::Offset(target),
            jump_if_null: true,
            ..
        } if *target as usize == next_position
    ));
    assert!(matches!(
        &program.insns[next_position].0,
        Insn::Next {
            pc_if_next: BranchOffset::Offset(target),
            ..
        } if *target as usize == column_position
    ));
}

// Example: `DELETE FROM items WHERE c2 RETURNING c7` carries the exact
// RETURNING position in HIR, but this first DELETE slice must refuse the whole
// root before emitting `OpenWrite` or any other instruction. This prevents a
// caller from accidentally running a mutation with an omitted row image.
#[hegel::test]
fn an_unsupported_delete_obligation_cannot_emit_a_partial_program(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1;
    let predicate_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let returning_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let table = BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 7)
        .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(&format!(
        "DELETE FROM items WHERE c{predicate_position} RETURNING c{returning_position}"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DELETE has valid SQL meaning");

    let HirRoot::Delete(delete) = &document.root else {
        panic!("DELETE syntax produces a DELETE HIR root");
    };
    let returning = delete
        .returning
        .as_ref()
        .expect("RETURNING remains an explicit HIR obligation");
    let Expr::Column(returned) = &returning.outputs[0].expr else {
        panic!("the generated RETURNING output is one bound column");
    };
    assert_eq!(returned.source, delete.target);
    assert_eq!(returned.column, returning_position);

    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    let error = emit_root(&plan, &mut program).expect_err("RETURNING is not emitted yet");

    assert!(matches!(
        error,
        PhysicalRootError::Delete(PhysicalDeleteError::Unsupported("RETURNING"))
    ));
    assert!(program.insns.is_empty());
}

// Example: for
// `c1 GENERATED ALWAYS AS (c0 + 7) VIRTUAL, c2 INTEGER DEFAULT 11`, either
// `DELETE FROM items WHERE c1` or `DELETE FROM items WHERE c2` must evaluate
// the stored HIR read rule before deciding to delete. c1 reads c0 and adds 7;
// c2 branches on record width and computes 11 for an old short record. Neither
// case may resolve or parse the stored SQL after the live schema is dropped.
#[hegel::test]
fn delete_predicates_execute_stored_column_hir(tc: hegel::TestCase) {
    let use_generated = tc.draw(generators::booleans());
    let generated_offset =
        i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let default_value =
        i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31))) + 40;
    let table = BTreeTable::from_sql(
        &format!(
            "CREATE TABLE items(\
             c0 INTEGER, \
             c1 INTEGER GENERATED ALWAYS AS (c0 + {generated_offset}) VIRTUAL, \
             c2 INTEGER DEFAULT {default_value})"
        ),
        7,
    )
    .expect("generated table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(table))
        .expect("items is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let predicate_column = if use_generated { 1 } else { 2 };
    let statement = parse_statement(&format!("DELETE FROM items WHERE c{predicate_column}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("stored-expression DELETE has valid SQL meaning");
    let HirRoot::Delete(delete) = &document.root else {
        panic!("DELETE syntax produces a DELETE HIR root");
    };
    let source = document
        .source(delete.target)
        .expect("DELETE target source exists");
    assert!(matches!(
        source.generated_expressions[1],
        ColumnReadExpression::Planned(_)
    ));
    assert!(matches!(
        source.default_expressions[2],
        ColumnReadExpression::Planned(_)
    ));
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("stored-expression DELETE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all stored-expression branches are closed");

    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Delete { .. })));
    if use_generated {
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Column { column: 0, .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Add { .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Column { column: 1, .. })));
    } else {
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::ColumnHasField { column: 1, .. })));
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::Integer { value, .. } if *value == default_value
        )));
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::Column {
                column: 1,
                default: None,
                ..
            }
        )));
    }
}
