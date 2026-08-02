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

// Examples:
// - `... DO UPDATE SET c1 = excluded.c1 + c1` must read `excluded.c1` from
//   the completed proposed row and unqualified `c1` from the conflicting OLD
//   table row; the two names must never collapse to one runtime source.
// - With `c2 AS (c1 + 7)`, the updated generated value and RETURNING output
//   must be built after SET and before the replacement write.
// - A false `WHERE excluded.c1 > 0` skips the update, while a true one seeks
//   the conflicting row and performs old-index delete -> table delete ->
//   new-index insert -> table insert.
#[hegel::test]
fn upsert_do_update_keeps_target_and_excluded_as_distinct_hir_rows(tc: hegel::TestCase) {
    let rowid_target = tc.draw(generators::booleans());
    let generated_offset =
        i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let key = if rowid_target {
        "c0 INTEGER PRIMARY KEY"
    } else {
        "c0 INTEGER"
    };
    let table = Arc::new(
        BTreeTable::from_sql(
            &format!(
                "CREATE TABLE items({key}, c1 INTEGER, c2 INTEGER AS (c1 + {generated_offset}))"
            ),
            12,
        )
        .expect("fixture table SQL is valid"),
    );
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
    let statement = parse_statement(
        "INSERT INTO items(c0, c1) VALUES (1, 9) \
         ON CONFLICT(c0) DO UPDATE SET c1 = excluded.c1 + c1 \
         WHERE excluded.c1 > 0 RETURNING c1, c2",
    );
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UPSERT has valid SQL meaning");
    let crate::translate::semantic::hir::HirRoot::Insert(insert) = &document.root else {
        panic!("fixture produces INSERT HIR");
    };
    let excluded = insert.excluded_source.expect("UPSERT has excluded");
    let crate::translate::semantic::hir::UpsertAction::Update { assignments, .. } =
        &insert.upserts[0].action
    else {
        panic!("fixture produces DO UPDATE HIR");
    };
    let mut sources = Vec::new();
    assignments[0].value.walk(&mut |expression| {
        if let crate::translate::semantic::hir::Expr::Column(column) = expression {
            sources.push(column.source);
        }
    });
    assert!(sources.contains(&insert.target));
    assert!(sources.contains(&excluded));
    assert_ne!(insert.target, excluded);
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("DO UPDATE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all DO UPDATE branches are closed");

    let seek = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
        .expect("DO UPDATE seeks the conflicting row");
    let delete = program
        .insns
        .iter()
        .enumerate()
        .skip(seek)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Delete { .. }).then_some(position)
        })
        .expect("DO UPDATE deletes the old table record");
    let table_insert = program
        .insns
        .iter()
        .enumerate()
        .skip(delete)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items")
                .then_some(position)
        })
        .expect("DO UPDATE inserts the replacement record");
    let returning = program
        .insns
        .iter()
        .enumerate()
        .skip(table_insert)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::ResultRow { .. }).then_some(position)
        })
        .expect("DO UPDATE returns the written NEW row");
    assert!(program.insns[seek..delete]
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Column { .. })));
    assert!(
        program.insns[seek..delete]
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::Add { .. }))
            .count()
            >= 2
    );
    assert!(seek < delete && delete < table_insert && table_insert < returning);
    if !rowid_target {
        let old_index_delete = program.insns[seek..delete]
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::IdxDelete { .. }))
            .map(|position| position + seek)
            .expect("DO UPDATE removes the old unique key");
        let new_index_insert = program.insns[delete..table_insert]
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::IdxInsert { .. }))
            .map(|position| position + delete)
            .expect("DO UPDATE inserts the new unique key");
        assert!(old_index_delete < delete && delete < new_index_insert);
    }
}

// Examples:
// - `INSERT OR REPLACE INTO items VALUES (1)` on an INTEGER PRIMARY KEY must
//   seek and delete the conflicting table row before inserting the new one.
// - The same statement on a UNIQUE index must delete every OLD index key,
//   delete the table row, then insert the NEW index key and table row.
// - `INSERT OR REPLACE ... VALUES (NULL)` for
//   `c0 NOT NULL DEFAULT 7` substitutes the frozen HIR default; if that default
//   were NULL, the remaining NOT NULL failure would use ABORT rather than
//   treating a row-local value failure as a row replacement.
#[hegel::test]
fn insert_replace_deletes_conflicting_rows_and_uses_frozen_not_null_defaults(tc: hegel::TestCase) {
    let conflict_kind = tc.draw(generators::integers::<u8>().max_value(2));
    let table_sql = match conflict_kind {
        0 => "CREATE TABLE items(c0 INTEGER PRIMARY KEY)",
        1 => "CREATE TABLE items(c0 INTEGER)",
        _ => "CREATE TABLE items(c0 INTEGER NOT NULL DEFAULT 7)",
    };
    let table = Arc::new(BTreeTable::from_sql(table_sql, 12).expect("fixture table SQL is valid"));
    let symbols = SymbolTable::new();
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("items is unique");
    if conflict_kind == 1 {
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
    let value = if conflict_kind == 2 { "NULL" } else { "1" };
    let statement = parse_statement(&format!("INSERT OR REPLACE INTO items VALUES ({value})"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated REPLACE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("REPLACE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all REPLACE branches are closed");

    if conflict_kind == 2 {
        let default = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Integer { value: 7, .. }))
            .expect("REPLACE emits the frozen NOT NULL default");
        let fallback = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Halt { err_code, on_error: Some(ResolveType::Abort), .. } if *err_code == SQLITE_CONSTRAINT_NOTNULL)
            })
            .expect("a NULL default falls back to ABORT");
        assert!(default < fallback);
        return;
    }

    let delete = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Delete { .. }))
        .expect("REPLACE has a conflicting-row delete path");
    let replacement = program
        .insns
        .iter()
        .enumerate()
        .skip(delete)
        .find_map(|(position, (instruction, _))| {
            matches!(instruction, Insn::Insert { table_name, .. } if table_name == "items")
                .then_some(position)
        })
        .expect("REPLACE inserts the new table row");
    assert!(delete < replacement);
    assert!(!program.insns.iter().any(|(instruction, _)| {
        matches!(instruction, Insn::Halt { err_code, .. } if *err_code == SQLITE_CONSTRAINT_PRIMARYKEY || *err_code == SQLITE_CONSTRAINT_UNIQUE)
    }));
    if conflict_kind == 1 {
        let old_index = program.insns[..delete]
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::IdxDelete { .. }))
            .expect("REPLACE deletes the OLD unique key");
        let new_index = program.insns[delete..replacement]
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::IdxInsert { .. }))
            .map(|position| position + delete)
            .expect("REPLACE inserts the NEW unique key");
        assert!(old_index < delete && delete < new_index && new_index < replacement);
    }
}
