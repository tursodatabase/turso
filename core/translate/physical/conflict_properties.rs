//! Properties for HIR DML conflict routing.

use hegel::generators;
use turso_parser::{
    ast::{self, ResolveType},
    parser::Parser,
};

use super::*;
use crate::{
    dialect::{Dialect, SqliteDialect},
    error::{
        SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY,
        SQLITE_CONSTRAINT_UNIQUE,
    },
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
// - `INSERT OR FAIL INTO items VALUES (1)` with `c0 NOT NULL` must halt with
//   FAIL before the table write.
// - `UPDATE OR ROLLBACK items SET c0 = c0 + 1` with `CHECK(c0 > 0)` must
//   preserve ROLLBACK on the CHECK failure path.
// - `INSERT OR IGNORE INTO items VALUES (1)` with a UNIQUE index must branch
//   around every table and index write instead of halting.
// ABORT, FAIL, ROLLBACK, and IGNORE are drawn across INSERT and UPDATE and
// across NOT NULL, CHECK, and UNIQUE, so each row-local failure uses the one
// statement policy selected by semantic analysis.
#[hegel::test]
fn dml_constraint_failures_follow_the_hir_conflict_policy(tc: hegel::TestCase) {
    let update = tc.draw(generators::booleans());
    let policy_number = tc.draw(generators::integers::<u8>().max_value(3));
    let constraint_number = tc.draw(generators::integers::<u8>().max_value(2));
    let (policy_sql, policy) = match policy_number {
        0 => ("ABORT", ResolveType::Abort),
        1 => ("FAIL", ResolveType::Fail),
        2 => ("ROLLBACK", ResolveType::Rollback),
        _ => ("IGNORE", ResolveType::Ignore),
    };
    let (table_sql, constraint_code) = match constraint_number {
        0 => (
            "CREATE TABLE items(c0 INTEGER NOT NULL)",
            SQLITE_CONSTRAINT_NOTNULL,
        ),
        1 => (
            "CREATE TABLE items(c0 INTEGER CHECK(c0 > 0))",
            SQLITE_CONSTRAINT_CHECK,
        ),
        _ => ("CREATE TABLE items(c0 INTEGER)", SQLITE_CONSTRAINT_UNIQUE),
    };

    let table = Arc::new(BTreeTable::from_sql(table_sql, 12).expect("fixture table SQL is valid"));
    let symbols = SymbolTable::new();
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("items is unique");
    if constraint_number == 2 {
        let index = Index::from_sql(
            &symbols,
            "CREATE UNIQUE INDEX items_c0 ON items(c0)",
            13,
            &table,
        )
        .expect("fixture index SQL is valid");
        schema
            .add_index(Arc::new(index))
            .expect("items_c0 is unique");
    }
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let sql = if update {
        format!("UPDATE OR {policy_sql} items SET c0 = c0 + 1")
    } else {
        format!("INSERT OR {policy_sql} INTO items VALUES (1)")
    };
    let statement = parse_statement(&sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated DML has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("conflict-aware DML lowers without a catalog");
    program
        .resolve_labels()
        .expect("all conflict branches are closed");

    let first_write = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Insert { .. }
                    | Insn::Delete { .. }
                    | Insn::IdxInsert { .. }
                    | Insn::IdxDelete { .. }
            )
        })
        .expect("DML has a write path");
    let matching_halts = program
        .insns
        .iter()
        .enumerate()
        .filter_map(|(position, (instruction, _))| match instruction {
            Insn::Halt {
                err_code, on_error, ..
            } if *err_code == constraint_code => Some((position, *on_error)),
            _ => None,
        })
        .collect::<Vec<_>>();

    if policy == ResolveType::Ignore {
        assert!(matching_halts.is_empty());
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| match instruction {
                Insn::Goto { target_pc } | Insn::IsNull { target_pc, .. } => {
                    target_pc.is_offset() && target_pc.as_offset_int() as usize > first_write
                }
                _ => false,
            }));
    } else {
        assert_eq!(matching_halts.len(), 1);
        assert_eq!(matching_halts[0].1, Some(policy));
        assert!(matching_halts[0].0 < first_write);
    }
}

// Examples:
// - `INSERT INTO items VALUES (1) ON CONFLICT(c0) DO NOTHING` where c0 is an
//   INTEGER PRIMARY KEY must skip the whole row when that rowid exists.
// - The same clause targeting `UNIQUE INDEX items_c0` must skip both the index
//   and table writes when its packed key already exists.
// - `ON CONFLICT DO NOTHING` is the catch-all form and must route either kind
//   of conflict without resolving an index name during physical emission.
#[hegel::test]
fn upsert_do_nothing_routes_the_resolved_hir_conflict_target(tc: hegel::TestCase) {
    let rowid_target = tc.draw(generators::booleans());
    let catch_all = tc.draw(generators::booleans());
    let table_sql = if rowid_target {
        "CREATE TABLE items(c0 INTEGER PRIMARY KEY)"
    } else {
        "CREATE TABLE items(c0 INTEGER)"
    };
    let table = Arc::new(BTreeTable::from_sql(table_sql, 12).expect("fixture table SQL is valid"));
    let symbols = SymbolTable::new();
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("items is unique");
    if !rowid_target {
        let index = Index::from_sql(
            &symbols,
            "CREATE UNIQUE INDEX items_c0 ON items(c0)",
            13,
            &table,
        )
        .expect("fixture index SQL is valid");
        schema
            .add_index(Arc::new(index))
            .expect("items_c0 is unique");
    }
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let target = if catch_all { "" } else { "(c0)" };
    let statement = parse_statement(&format!(
        "INSERT INTO items VALUES (1) ON CONFLICT{target} DO NOTHING"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UPSERT has valid SQL meaning");
    let crate::translate::semantic::hir::HirRoot::Insert(insert) = &document.root else {
        panic!("fixture produces INSERT HIR");
    };
    assert_eq!(insert.upserts.len(), 1);
    assert_eq!(insert.upserts[0].target.is_none(), catch_all);
    if let Some(target) = &insert.upserts[0].target {
        assert_eq!(target.matched_index.is_none(), rowid_target);
    }
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("DO NOTHING lowers without a catalog");
    program
        .resolve_labels()
        .expect("all DO NOTHING branches are closed");

    let constraint_code = if rowid_target {
        SQLITE_CONSTRAINT_PRIMARYKEY
    } else {
        SQLITE_CONSTRAINT_UNIQUE
    };
    assert!(!program.insns.iter().any(|(instruction, _)| {
        matches!(instruction, Insn::Halt { err_code, .. } if *err_code == constraint_code)
    }));
    let first_write = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(instruction, Insn::Insert { .. } | Insn::IdxInsert { .. })
        })
        .expect("UPSERT retains its success write path");
    assert!(program.insns[..first_write].iter().any(|(instruction, _)| {
        matches!(instruction, Insn::Goto { target_pc } if target_pc.is_offset() && target_pc.as_offset_int() as usize > first_write)
    }));
}
