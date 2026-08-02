//! Properties for direct DELETE emission from closed HIR.

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    error::SQLITE_CONSTRAINT_FOREIGNKEY,
    schema::{BTreeTable, Index, Schema},
    sync::Arc,
    translate::semantic::{
        analyze,
        context::{DmlPolicy, SemanticContext},
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

// Examples: deleting `parents.p4 = 7` with `children.c2 REFERENCES
// parents(p4)` must scan child position two before deleting the parent. NO
// ACTION counts every match using the declaration's immediate/deferred mode;
// RESTRICT halts at the first match. Varying both positions proves the emitter
// consumes the frozen HIR offsets and action instead of resolving names again.
#[hegel::test]
fn delete_parent_checks_children_before_removing_the_row(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(10)));
    let child_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let parent_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let restrict = tc.draw(generators::booleans());
    let deferred = tc.draw(generators::booleans());
    let parent_columns = (0..width)
        .map(|position| {
            if position == parent_position {
                format!("p{position} INTEGER PRIMARY KEY")
            } else {
                format!("p{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let child_columns = (0..width)
        .map(|position| {
            if position == child_position {
                format!(
                    "c{position} INTEGER REFERENCES parents(p{parent_position}) ON DELETE {}{}",
                    if restrict { "RESTRICT" } else { "NO ACTION" },
                    if deferred {
                        " DEFERRABLE INITIALLY DEFERRED"
                    } else {
                        ""
                    }
                )
            } else {
                format!("c{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parent = BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 23)
        .expect("parent table SQL is valid");
    let child = BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 29)
        .expect("child table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(parent))
        .expect("parents is unique");
    schema
        .add_btree_table(Arc::new(child))
        .expect("children is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect)
        .with_dml_policy(DmlPolicy::new(false, false, false, false, true));
    let statement = parse_statement("DELETE FROM parents");
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated parent DELETE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed FK DELETE has a physical plan");
    let mut program = program();
    emit_root_delete(&plan, &mut program).expect("parent FK DELETE emits without a resolver");
    program
        .resolve_labels()
        .expect("all parent FK DELETE branches are closed");

    let (child_read, child_cursor) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::OpenRead {
                cursor_id,
                root_page: 29,
                db: 0,
            } => Some((position, *cursor_id)),
            _ => None,
        })
        .expect("the frozen child table is scanned");
    let child_column = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Column { cursor_id, column, .. } if *cursor_id == child_cursor && *column == child_position)
        })
        .expect("the frozen child position is read");
    let parent_delete = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Delete { table_name, .. } if table_name == "parents")
        })
        .expect("the parent row is deleted after its checks");
    assert!(child_read < child_column && child_column < parent_delete);
    if restrict {
        assert!(program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::Halt { err_code, .. } if *err_code == SQLITE_CONSTRAINT_FOREIGNKEY)
        }));
        assert!(!program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::FkCounter { .. })));
    } else {
        assert!(program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::FkCounter { increment_value: 1, deferred: actual } if *actual == deferred)
        }));
    }
}

// Example: deleting `children.c5 = 99` from a deferred foreign key removes
// one previously counted missing-parent violation before deleting the child;
// an immediate constraint has no old counter to repair. Varying the child and
// parent positions proves the OLD key probe is driven by frozen HIR offsets.
#[hegel::test]
fn delete_child_repairs_only_deferred_old_violations(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(10)));
    let child_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let parent_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let deferred = tc.draw(generators::booleans());
    let parent_columns = (0..width)
        .map(|position| {
            if position == parent_position {
                format!("p{position} INTEGER PRIMARY KEY")
            } else {
                format!("p{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let child_columns = (0..width)
        .map(|position| {
            if position == child_position {
                format!(
                    "c{position} INTEGER REFERENCES parents(p{parent_position}){}",
                    if deferred {
                        " DEFERRABLE INITIALLY DEFERRED"
                    } else {
                        ""
                    }
                )
            } else {
                format!("c{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parent = BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 23)
        .expect("parent table SQL is valid");
    let child = BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 29)
        .expect("child table SQL is valid");
    let mut schema = Schema::new();
    schema
        .add_btree_table(Arc::new(parent))
        .expect("parents is unique");
    schema
        .add_btree_table(Arc::new(child))
        .expect("children is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect)
        .with_dml_policy(DmlPolicy::new(false, false, false, false, true));
    let statement = parse_statement("DELETE FROM children");
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated child DELETE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed FK DELETE has a physical plan");
    let mut program = program();
    emit_root_delete(&plan, &mut program).expect("child FK DELETE emits without a resolver");
    program
        .resolve_labels()
        .expect("all child FK DELETE branches are closed");
    assert_eq!(
        program.insns.iter().any(|(instruction, _)| {
            matches!(
                instruction,
                Insn::FkCounter {
                    increment_value: -1,
                    deferred: true
                }
            )
        }),
        deferred
    );
}

// Example: after binding `DELETE FROM items WHERE c7`, direct emission must
// first read position seven while collecting stable target rowids, then seek
// and delete those rows in a separate write pass. Dropping `Schema` first
// proves that target and column names are never resolved again.
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
    let table = Arc::new(table);
    let symbols = SymbolTable::new();
    let index = Index::from_sql(
        &symbols,
        "CREATE INDEX items_generated ON items(c0)",
        8,
        &table,
    )
    .expect("generated-column index SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(table).expect("items is unique");
    schema
        .add_index(Arc::new(index))
        .expect("items_generated is unique");
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
    let (rowids_open_position, rowids_cursor) = program
        .insns
        .iter()
        .enumerate()
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            } => Some((position, *cursor_id)),
            _ => None,
        })
        .expect("DELETE freezes selected rowids before writing");
    let position_of = |predicate: &dyn Fn(&Insn) -> bool| {
        program
            .insns
            .iter()
            .position(|(instruction, _)| predicate(instruction))
            .expect("expected instruction exists")
    };
    let scan_rewind_position = position_of(
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
    let scan_next_position = position_of(
        &|instruction| matches!(instruction, Insn::Next { cursor_id, .. } if *cursor_id == cursor),
    );
    let write_rewind_position = position_of(
        &|instruction| matches!(instruction, Insn::Rewind { cursor_id, .. } if *cursor_id == rowids_cursor),
    );
    let seek_position = position_of(
        &|instruction| matches!(instruction, Insn::NotExists { cursor: target_cursor, .. } if *target_cursor == cursor),
    );
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
    let write_next_position = position_of(
        &|instruction| matches!(instruction, Insn::Next { cursor_id, .. } if *cursor_id == rowids_cursor),
    );
    let close_position = position_of(
        &|instruction| matches!(instruction, Insn::Close { cursor_id } if *cursor_id == cursor),
    );

    assert!(
        open_position < rowids_open_position
            && rowids_open_position < scan_rewind_position
            && scan_rewind_position < column_position
            && column_position < filter_position
            && filter_position < scan_next_position
            && scan_next_position < write_rewind_position
            && write_rewind_position < seek_position
            && seek_position < delete_position
            && delete_position < write_next_position
            && write_next_position < close_position
    );
    assert!(matches!(
        &program.insns[scan_rewind_position].0,
        Insn::Rewind {
            pc_if_empty: BranchOffset::Offset(target),
            ..
        } if *target as usize > filter_position
    ));
    assert!(matches!(
        &program.insns[filter_position].0,
        Insn::IfNot {
            target_pc: BranchOffset::Offset(target),
            jump_if_null: true,
            ..
        } if *target as usize == scan_next_position
    ));
    assert!(matches!(
        &program.insns[scan_next_position].0,
        Insn::Next {
            pc_if_next: BranchOffset::Offset(target),
            ..
        } if *target as usize == column_position
    ));
}

// Example: `DELETE FROM items WHERE c2 RETURNING c7` must capture old c7 while
// the target cursor still names the row, delete its indexes and table record,
// then emit the captured result. RETURNING may not read the invalidated cursor
// after Delete, and it may not expose a result for a write that did not happen.
#[hegel::test]
fn delete_returning_captures_old_hir_before_the_write(tc: hegel::TestCase) {
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
    emit_root(&plan, &mut program).expect("DELETE RETURNING lowers without a catalog");
    let delete_position = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Delete { .. }))
        .expect("the row is deleted");
    let result_position = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::ResultRow { count: 1, .. }))
        .expect("one RETURNING field is emitted");
    let returned_read = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Column { column, .. } if *column == returning_position)
                .then_some(position)
        })
        .last()
        .expect("RETURNING reads its resolved old column");
    assert!(returned_read < delete_position && delete_position < result_position);
}

// Example: for
// `c1 GENERATED ALWAYS AS (c0 + 7) VIRTUAL, c2 INTEGER DEFAULT 11`, either
// `DELETE FROM items WHERE c1` or `DELETE FROM items WHERE c2` must evaluate
// the stored HIR read rule before deciding to delete. c1 reads c0 and adds 7;
// c2 branches on record width and computes 11 for an old short record. Neither
// case may resolve or parse the stored SQL after the live schema is dropped.
// `CREATE INDEX items_generated ON items(c1)` also requires DELETE to compute
// the generated old key from that same frozen HIR before removing the index row.
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
    let table = Arc::new(table);
    let symbols = SymbolTable::new();
    let index = Index::from_sql(
        &symbols,
        "CREATE INDEX items_generated ON items(c1)",
        8,
        &table,
    )
    .expect("generated-column index SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(table).expect("items is unique");
    schema
        .add_index(Arc::new(index))
        .expect("items_generated is unique");
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
