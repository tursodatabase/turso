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
        insn::{InsertFlags, Insn},
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
    program.flags.set_has_statement_conflict(true);
    program.set_resolve_type(policy);
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
        assert_eq!(matching_halts[0].1, None);
        assert_eq!(program.resolve_type, policy);
        assert!(matching_halts[0].0 < first_write);
    }
}

// Examples:
// - `INSERT OR IGNORE INTO dst SELECT * FROM src` must advance the materialized
//   source cursor when a UNIQUE-index conflict skips one selected row.
// - The same statement with an `INTEGER PRIMARY KEY` conflict must use the
//   identical row-done boundary. Jumping to the row start retries forever.
// Across generated widths and key positions, every IGNORE branch in the
// INSERT-SELECT write loop must land on `Next`, whose success branch returns
// to the start of the following source row.
#[hegel::test]
fn insert_select_ignore_advances_past_each_conflicting_source_row(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let key_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let rowid_key = tc.draw(generators::booleans());
    let source_columns = (0..width)
        .map(|position| format!("c{position} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let target_columns = (0..width)
        .map(|position| {
            if rowid_key && position == key_position {
                format!("c{position} INTEGER PRIMARY KEY")
            } else {
                format!("c{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let source = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE src({source_columns})"), 10)
            .expect("generated source table SQL is valid"),
    );
    let target = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE dst({target_columns})"), 12)
            .expect("generated target table SQL is valid"),
    );
    let symbols = SymbolTable::new();
    let mut schema = Schema::new();
    schema.add_btree_table(source).expect("src is unique");
    schema
        .add_btree_table(target.clone())
        .expect("dst is unique");
    if !rowid_key {
        let index = Index::from_sql(
            &symbols,
            &format!("CREATE UNIQUE INDEX dst_key ON dst(c{key_position})"),
            13,
            &target,
        )
        .expect("generated unique index SQL is valid");
        schema
            .add_index(Arc::new(index))
            .expect("dst_key is unique");
    }
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement("INSERT OR IGNORE INTO dst SELECT * FROM src");
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated INSERT-SELECT has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("INSERT-SELECT IGNORE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all INSERT-SELECT conflict branches are closed");

    let probe = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            if rowid_key {
                matches!(instruction, Insn::NotExists { .. })
            } else {
                matches!(instruction, Insn::NoConflict { .. })
            }
        })
        .expect("the generated key has a conflict probe");
    let Insn::Goto { target_pc } = &program.insns[probe + 1].0 else {
        panic!("IGNORE follows its conflict probe with a branch");
    };
    let (next_position, next_row) = program
        .insns
        .iter()
        .enumerate()
        .skip(probe + 2)
        .find_map(|(position, (instruction, _))| match instruction {
            Insn::Next { pc_if_next, .. } => Some((position, pc_if_next)),
            _ => None,
        })
        .expect("the materialized INSERT source advances after each row");
    assert_eq!(target_pc.as_offset_int() as usize, next_position);
    assert!(next_row.as_offset_int() as usize <= probe);
}

// Examples:
// - `INSERT INTO items VALUES (NULL)` with
//   `c0 NOT NULL ON CONFLICT IGNORE` branches around the write.
// - The same column with `ON CONFLICT REPLACE DEFAULT 7` substitutes 7 and
//   falls back to ABORT only if that frozen default is still NULL.
// - `c0 INTEGER PRIMARY KEY ON CONFLICT IGNORE` controls a rowid collision
//   when the INSERT has no statement-level `OR ...` override.
// The policy belongs to the resolved table constraint and must survive after
// semantic analysis without a mini-binder or catalog name lookup.
#[hegel::test]
fn column_conflict_policies_are_used_without_a_statement_override(tc: hegel::TestCase) {
    let kind = tc.draw(generators::integers::<u8>().max_value(2));
    let (table_sql, statement_sql) = match kind {
        0 => (
            "CREATE TABLE items(c0 INTEGER NOT NULL ON CONFLICT IGNORE)",
            "INSERT INTO items VALUES (NULL)",
        ),
        1 => (
            "CREATE TABLE items(c0 INTEGER NOT NULL ON CONFLICT REPLACE DEFAULT 7)",
            "UPDATE items SET c0 = NULL",
        ),
        _ => (
            "CREATE TABLE items(c0 INTEGER PRIMARY KEY ON CONFLICT IGNORE)",
            "INSERT INTO items VALUES (1)",
        ),
    };
    let table = Arc::new(BTreeTable::from_sql(table_sql, 12).expect("fixture table SQL is valid"));
    let symbols = SymbolTable::new();
    let mut schema = Schema::new();
    schema.add_btree_table(table).expect("items is unique");
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let statement = parse_statement(statement_sql);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("DML with a column policy has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("column conflict policy emits from frozen metadata");
    program
        .resolve_labels()
        .expect("all column-policy branches are closed");

    match kind {
        0 => assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IsNull { .. }))),
        1 => assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Integer { value: 7, .. }))),
        _ => {
            let collision = program
                .insns
                .iter()
                .position(|(instruction, _)| matches!(instruction, Insn::NotExists { .. }))
                .expect("rowid uniqueness is checked");
            assert!(program.insns[collision + 1..]
                .iter()
                .any(|(instruction, _)| matches!(instruction, Insn::Goto { .. })));
        }
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
// - `INSERT OR REPLACE ... VALUES (NULL)` for `c0 NOT NULL` has no default to
//   substitute and must emit the normal NOT NULL ABORT, not a planning error.
#[hegel::test]
fn insert_replace_deletes_conflicting_rows_and_uses_frozen_not_null_defaults(tc: hegel::TestCase) {
    let conflict_kind = tc.draw(generators::integers::<u8>().max_value(3));
    let table_sql = match conflict_kind {
        0 => "CREATE TABLE items(c0 INTEGER PRIMARY KEY)",
        1 => "CREATE TABLE items(c0 INTEGER)",
        2 => "CREATE TABLE items(c0 INTEGER NOT NULL DEFAULT 7)",
        _ => "CREATE TABLE items(c0 INTEGER NOT NULL)",
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
    let value = if conflict_kind >= 2 { "NULL" } else { "1" };
    let statement = parse_statement(&format!("INSERT OR REPLACE INTO items VALUES ({value})"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated REPLACE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    program.flags.set_has_statement_conflict(true);
    program.set_resolve_type(ResolveType::Replace);
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
                matches!(instruction, Insn::Halt { err_code, on_error: None, .. } if *err_code == SQLITE_CONSTRAINT_NOTNULL)
            })
            .expect("a NULL default falls back to ABORT");
        assert!(default < fallback);
        return;
    }
    if conflict_kind == 3 {
        assert!(!program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Integer { value: 7, .. })));
        assert!(program.insns.iter().any(|(instruction, _)| {
            matches!(instruction, Insn::Halt { err_code, on_error: None, .. } if *err_code == SQLITE_CONSTRAINT_NOTNULL)
        }));
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

// Examples:
// - `INSERT OR REPLACE INTO items VALUES (9999, 'a134')` must seek the table
//   again after deleting the row whose UNIQUE key is `a134`; that replacement
//   leaves the table cursor positioned at the removed row, not rowid 9999.
// - `c0 INTEGER PRIMARY KEY ON CONFLICT REPLACE` and
//   `c0 INTEGER UNIQUE ON CONFLICT REPLACE` require the same table seek even
//   when the INSERT has no statement-level `OR REPLACE`.
// Across generated table widths and key positions, both rowid and secondary-
// index replacement paths must mark the final table Insert as requiring a seek.
#[hegel::test]
fn insert_replace_reseeks_table_after_conflicting_row_delete(tc: hegel::TestCase) {
    let width = usize::from(tc.draw(generators::integers::<u8>().max_value(7))) + 1;
    let key_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
    let rowid_key = tc.draw(generators::booleans());
    let statement_replace = tc.draw(generators::booleans());
    let columns = (0..width)
        .map(|position| {
            if rowid_key && position == key_position {
                let conflict = (!statement_replace)
                    .then_some(" ON CONFLICT REPLACE")
                    .unwrap_or_default();
                format!("c{position} INTEGER PRIMARY KEY{conflict}")
            } else {
                format!("c{position} INTEGER")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let table = Arc::new(
        BTreeTable::from_sql(&format!("CREATE TABLE items({columns})"), 12)
            .expect("generated table SQL is valid"),
    );
    let symbols = SymbolTable::new();
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("items is unique");
    if !rowid_key {
        let mut index = Index::from_sql(
            &symbols,
            &format!("CREATE UNIQUE INDEX items_key ON items(c{key_position})"),
            13,
            &table,
        )
        .expect("generated unique index SQL is valid");
        if !statement_replace {
            index.on_conflict = Some(ResolveType::Replace);
        }
        schema
            .add_index(Arc::new(index))
            .expect("items_key is unique");
    }
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let values = (0..width).map(|_| "1").collect::<Vec<_>>().join(", ");
    let or_replace = statement_replace
        .then_some(" OR REPLACE")
        .unwrap_or_default();
    let statement = parse_statement(&format!("INSERT{or_replace} INTO items VALUES ({values})"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated REPLACE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    if statement_replace {
        program.flags.set_has_statement_conflict(true);
        program.set_resolve_type(ResolveType::Replace);
    }
    emit_root(&plan, &mut program).expect("REPLACE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all REPLACE branches are closed");

    let delete = program
        .insns
        .iter()
        .position(|(instruction, _)| matches!(instruction, Insn::Delete { .. }))
        .expect("REPLACE has a conflicting-row delete path");
    let requires_seek = program
        .insns
        .iter()
        .skip(delete)
        .find_map(|(instruction, _)| match instruction {
            Insn::Insert {
                table_name, flag, ..
            } if table_name == "items" => Some(flag.has(InsertFlags::REQUIRE_SEEK)),
            _ => None,
        })
        .expect("REPLACE inserts the new table row");
    assert!(requires_seek);
}

// Examples:
// - `UPDATE OR REPLACE items SET c0 = 7` with a UNIQUE index must ignore the
//   current row's own key, delete a different conflicting row and all of its
//   OLD index keys, seek back to the current row, then perform the update.
// - `UPDATE OR REPLACE items SET c0 = NULL` for
//   `c0 NOT NULL DEFAULT 7` must use the frozen default before the normal NEW
//   row checks, exactly like INSERT OR REPLACE.
#[hegel::test]
fn update_replace_uses_frozen_defaults_and_removes_other_unique_rows(tc: hegel::TestCase) {
    let not_null_default = tc.draw(generators::booleans());
    let table_sql = if not_null_default {
        "CREATE TABLE items(c0 INTEGER NOT NULL DEFAULT 7)"
    } else {
        "CREATE TABLE items(c0 INTEGER, c1 INTEGER)"
    };
    let table = Arc::new(BTreeTable::from_sql(table_sql, 12).expect("fixture table SQL is valid"));
    let symbols = SymbolTable::new();
    let mut schema = Schema::new();
    schema
        .add_btree_table(table.clone())
        .expect("items is unique");
    if !not_null_default {
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
    let assignment = if not_null_default { "NULL" } else { "7" };
    let statement = parse_statement(&format!("UPDATE OR REPLACE items SET c0 = {assignment}"));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated UPDATE OR REPLACE has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed HIR has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("UPDATE OR REPLACE lowers without a catalog");
    program
        .resolve_labels()
        .expect("all UPDATE OR REPLACE branches are closed");

    if not_null_default {
        let default = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Integer { value: 7, .. }))
            .expect("UPDATE REPLACE emits the frozen NOT NULL default");
        let check = program
            .insns
            .iter()
            .enumerate()
            .skip(default + 1)
            .position(|(_, (instruction, _))| matches!(instruction, Insn::NotNull { .. }))
            .map(|position| position + default + 1)
            .expect("the substituted value is checked");
        assert!(default < check);
        return;
    }

    assert!(program
        .insns
        .iter()
        .any(|(instruction, _)| matches!(instruction, Insn::Eq { .. })));
    assert!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
            .count()
            >= 2,
        "replacement must seek the conflict and then restore the current row"
    );
    let conflicting_delete = program
        .insns
        .iter()
        .position(|(instruction, _)| {
            matches!(
                instruction,
                Insn::Delete {
                    is_part_of_update: false,
                    ..
                }
            )
        })
        .expect("UPDATE REPLACE deletes the conflicting row");
    let current_delete = program
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
        .expect("UPDATE then replaces its current row");
    assert!(conflicting_delete < current_delete);
}

// Examples:
// - `... ON CONFLICT(c0) DO UPDATE SET rowid = excluded.id + 4` must probe the
//   proposed key, restore the conflicting OLD row, and insert NEW under that
//   proposed key rather than the conflict rowid.
// - Assigning the `INTEGER PRIMARY KEY` alias follows the identical path, so
//   `SET id = ...` and `SET rowid = ...` cannot drift apart.
#[hegel::test]
fn upsert_rowid_assignment_separates_the_conflict_and_written_keys(tc: hegel::TestCase) {
    let use_alias = tc.draw(generators::booleans());
    let offset = i64::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(31)));
    let symbols = SymbolTable::new();
    let table = Arc::new(
        BTreeTable::from_sql(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, c0 INTEGER, c1 INTEGER)",
            12,
        )
        .expect("fixture table SQL is valid"),
    );
    let index = Index::from_sql(
        &symbols,
        "CREATE UNIQUE INDEX items_c0 ON items(c0)",
        13,
        &table,
    )
    .expect("fixture index SQL is valid");
    let mut schema = Schema::new();
    schema.add_btree_table(table).expect("items is unique");
    schema
        .add_index(Arc::new(index))
        .expect("items_c0 is unique");
    let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
    let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect);
    let target = if use_alias { "id" } else { "rowid" };
    let statement = parse_statement(&format!(
        "INSERT INTO items(id, c0, c1) VALUES (1, 5, 9) \
         ON CONFLICT(c0) DO UPDATE SET {target} = excluded.id + {offset} RETURNING id"
    ));
    let document = analyze(&context, AnalyzeInput::Statement(&statement))
        .expect("generated rowid UPSERT has valid SQL meaning");
    drop(context);
    drop(schema);
    drop(symbols);

    let plan = PhysicalPlan::new(&document).expect("closed rowid UPSERT has a physical plan");
    let mut program = program();
    emit_root(&plan, &mut program).expect("rowid UPSERT emits without a resolver");
    program
        .resolve_labels()
        .expect("all rowid UPSERT branches are closed");

    let written_key = program
        .insns
        .iter()
        .rev()
        .find_map(|(instruction, _)| match instruction {
            Insn::Insert {
                key_reg,
                table_name,
                ..
            } if table_name == "items" => Some(*key_reg),
            _ => None,
        })
        .expect("UPSERT writes the NEW table row");
    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(instruction, Insn::MustBeInt { reg, .. } if *reg == written_key)
    }));
    assert!(program.insns.iter().any(|(instruction, _)| {
        matches!(instruction, Insn::NotExists { rowid_reg, .. } if *rowid_reg == written_key)
    }));
    assert!(
        program
            .insns
            .iter()
            .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
            .count()
            >= 2
    );
}
