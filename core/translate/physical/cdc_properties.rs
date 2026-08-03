//! Properties for CDC facts and row-image lowering across the HIR boundary.

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
    CaptureDataChangesInfo, CaptureDataChangesMode, CdcVersion, QueryMode, SymbolTable, MAIN_DB_ID,
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
// - `UPDATE items SET c2 = 9` over four columns produces
//   `[false, false, true, false, NULL, NULL, c2, NULL]`.
// - Updating `c0` in a one-column table produces `[true, c0]`.
// For every generated width and target position, the CDC update record marks
// exactly the selected HIR destination and copies its NEW runtime slot.
#[hegel::test]
fn update_records_keep_hir_positions_aligned_with_runtime_columns(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)));
    let position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let mut program = program();
    let logical = RegisterRange::new(program.alloc_registers(width), width);
    let assignment = crate::translate::semantic::hir::Assignment {
        columns: vec![crate::translate::semantic::hir::TargetColumn::Column(
            position,
        )],
        value: crate::translate::semantic::hir::Expr::Literal(ast::Literal::Null),
    };

    update_record(&mut program, width, &[assignment], logical, None);

    let selected_copy = program.insns.iter().find_map(|(instruction, _)| {
        let Insn::Copy {
            src_reg,
            dst_reg,
            extra_amount: 0,
        } = instruction
        else {
            return None;
        };
        (*src_reg == logical.first.0 + position).then_some(*dst_reg)
    });
    let selected_copy = selected_copy.expect("the selected NEW value is copied once");
    let record = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::MakeRecord {
                start_reg, count, ..
            } if *count as usize == width * 2 => Some(*start_reg as usize),
            _ => None,
        })
        .expect("the update mask is serialized");
    assert_eq!(selected_copy, record + width + position);
}

// Examples:
// - `ALTER TABLE items RENAME COLUMN a TO b` updates sqlite_schema.sql, but
//   CDC's changed value is the original ALTER text rather than generated SQL.
// - If `sql` is field 4, only CDC slot `width + 4` receives that text; the
//   ordinary NEW-row register must not overwrite it.
// For every generated table width and sql-field position, the frozen internal
// schema override lands in the matching CDC value slot.
#[hegel::test]
fn internal_schema_updates_keep_the_user_ddl_in_the_sql_cdc_slot(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)));
    let position = usize::from(
        tc.draw(generators::integers::<u8>().max_value(u8::try_from(width - 1).unwrap())),
    );
    let mut program = program();
    let logical = RegisterRange::new(program.alloc_registers(width), width);
    let assignment = crate::translate::semantic::hir::Assignment {
        columns: vec![crate::translate::semantic::hir::TargetColumn::Column(
            position,
        )],
        value: crate::translate::semantic::hir::Expr::Literal(ast::Literal::Null),
    };
    let ddl = "ALTER TABLE items RENAME COLUMN a TO b";

    update_record(
        &mut program,
        width,
        &[assignment],
        logical,
        Some((position, ddl)),
    );

    let record = program
        .insns
        .iter()
        .find_map(|(instruction, _)| match instruction {
            Insn::MakeRecord {
                start_reg, count, ..
            } if *count as usize == width * 2 => Some(*start_reg as usize),
            _ => None,
        })
        .expect("the update mask is serialized");
    assert!(program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::String8 { value, dest }
            if value == ddl && *dest == record + width + position
    )));
    assert!(!program.insns.iter().any(|(instruction, _)| matches!(
        instruction,
        Insn::Copy { src_reg, dst_reg, .. }
            if *src_reg == logical.first.0 + position
                && *dst_reg == record + width + position
    )));
}

// Examples:
// - `INSERT INTO items VALUES (1)` freezes `turso_cdc` as a main-schema table.
// - `UPDATE items SET id = id` and `DELETE FROM items` carry the same CDC
//   identity even after the live schema is no longer available to emission.
// The operation kind is drawn so every DML root proves CDC binding happens in
// semantic analysis and not through a Resolver in physical lowering.
#[hegel::test]
fn every_dml_root_freezes_the_cdc_table_in_its_catalog_snapshot(tc: hegel::TestCase) {
    let operation = tc.draw(generators::integers::<u8>().max_value(2));
    let target = Arc::new(
        BTreeTable::from_sql("CREATE TABLE items(id INTEGER PRIMARY KEY)", 2)
            .expect("fixture target is valid"),
    );
    let cdc = Arc::new(
        BTreeTable::from_sql(
            "CREATE TABLE turso_cdc(change_id INTEGER PRIMARY KEY, change_time, change_txn_id, change_type, table_name, id, before, after, updates)",
            3,
        )
        .expect("fixture CDC table is valid"),
    );
    let mut schema = Schema::new();
    schema.add_btree_table(target).expect("items is unique");
    schema.add_btree_table(cdc).expect("turso_cdc is unique");
    let symbols = SymbolTable::new();
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect)
        .with_capture_data_changes(Some(CaptureDataChangesInfo {
            mode: CaptureDataChangesMode::Full,
            table: "turso_cdc".to_string(),
            version: Some(CdcVersion::V2),
        }));
    let sql = match operation {
        0 => "INSERT INTO items VALUES (1)",
        1 => "UPDATE items SET id = id",
        _ => "DELETE FROM items",
    };
    let statement = parse_statement(sql);

    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DML has valid SQL meaning");
    let cdc = document.cdc.expect("CDC metadata is frozen for DML");

    assert_eq!(cdc.table.snapshot(), document.snapshot);
    assert_eq!(cdc.table.database().map(|db| db.index()), Some(MAIN_DB_ID));
    assert_eq!(cdc.table.value().get_name(), "turso_cdc");
}
