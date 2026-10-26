//! Finds indexes that an older version of Turso built under other rules.
//!
//! An index stores values: column values, results of key expressions, and
//! the result of a partial index's WHERE clause. With custom types enabled,
//! those values and their order follow Turso's rules for custom types, and
//! those rules are experimental and still change. An index built under older
//! rules holds keys that the current rules never produce, so a later DELETE
//! or UPDATE can't find its entry and a lookup can miss rows.
//!
//! A database whose indexes were built under the current rules has an empty
//! table named `..._custom_type_rules_v{RULES_VERSION}`. The table is created
//! by the statement that gives a database its first such index, and by
//! REINDEX, which rebuilds all of them. Databases without such indexes never
//! get the table. In a database without the table, statements fail instead
//! of using such an index. A database with a higher version's table was
//! changed by a newer version of Turso, and statements on it fail: this
//! version can't tell which of its indexes follow the newer rules. A
//! connection without custom types doesn't touch a database with the table.

use crate::function::{Func, ScalarFunc};
use crate::namespace::NamespaceId;
use crate::schema::{BTreeTable, Column, Index, Schema, TURSO_INTERNAL_PREFIX};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{Cookie, Insn, RegisterOrLiteral};
use crate::Connection;
use crate::HashMap;
use crate::{bail_parse_error, Result};
use turso_parser::ast::{Expr, LikeOperator, Type};

/// Increase this in every change that alters the value or the order of
/// anything an index stores under custom types, so that indexes built before
/// the change must be rebuilt before use. A change may also make more indexes
/// count as depending on the rules, but never fewer: an index this version
/// trusts in an unmarked database must mean the same in every version.
const RULES_VERSION: u32 = 1;

/// Fails if the statement opens a table or an index of a database it must
/// not touch, or an index that depends on the rules in a database whose
/// indexes were built under older rules. Called for every statement after it
/// is compiled, so it sees every cursor the statement opens, including
/// through triggers and foreign key actions, whose programs are compiled the
/// same way. REINDEX passes `rebuilds_old_indexes`, because it opens old
/// indexes only to rebuild them.
///
/// A connection without custom types computes index keys with SQLite's rules,
/// so it doesn't touch a database that has the marker: such a database has
/// indexes that depend on the custom-type rules.
pub(crate) fn fail_on_index_built_by_other_rules(
    program: &ProgramBuilder,
    resolver: &Resolver,
    rebuilds_old_indexes: bool,
) -> Result<()> {
    let mut rules_versions: HashMap<usize, Option<u32>> = HashMap::default();
    for (insn, _) in &program.insns {
        let (cursor_id, database_id) = match insn {
            Insn::OpenRead { cursor_id, db, .. } | Insn::OpenWrite { cursor_id, db, .. } => {
                (*cursor_id, *db)
            }
            _ => continue,
        };
        let rules_version = *rules_versions
            .entry(database_id)
            .or_insert_with(|| resolver.with_schema(database_id, newest_rules_version));
        if rules_version.is_some_and(|version| version > RULES_VERSION) {
            bail_parse_error!(
                "this database's indexes were built by a newer version of Turso's experimental custom types. Use that version or a later one"
            );
        }
        if !resolver.enable_custom_types {
            if rules_version.is_some() {
                bail_parse_error!(
                    "this database has indexes that depend on Turso's experimental custom types. Enable custom types to use it"
                );
            }
            continue;
        }
        if rebuilds_old_indexes || rules_version == Some(RULES_VERSION) {
            continue;
        }
        let CursorType::BTreeIndex(index) = &program.cursor_ref[cursor_id].1 else {
            continue;
        };
        resolver.with_schema(database_id, |schema| -> Result<()> {
            if !is_stored_index_depending_on_rules(schema, index) {
                return Ok(());
            }
            let mut names: Vec<&str> = indexes_depending_on_rules(schema)
                .map(|(_, index)| index.name.as_str())
                .collect();
            names.sort_unstable();
            bail_parse_error!(
                "indexes built by an older version of Turso's experimental custom types must be rebuilt before use: {}. Run REINDEX, or drop these indexes and create them again",
                names.join(", ")
            )
        })?;
    }
    Ok(())
}

/// Called for every statement except REINDEX. In a database without the
/// marker and without indexes that depend on the rules, every index that
/// depends on the rules after this statement was built by this statement:
/// creating an index, or rebuilding one for ALTER TABLE, writes all of its
/// keys under the current rules. So when the statement changes the schema of
/// such a database, it ends with a check of the changed schema, and creates
/// the marker if the database now has such an index. Databases that never get
/// one never get the marker.
pub(crate) fn record_rules_version_on_schema_change(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
) -> Result<()> {
    if !resolver.enable_custom_types {
        return Ok(());
    }
    let mut changed_database_ids: Vec<usize> = program
        .insns
        .iter()
        .filter_map(|(insn, _)| match insn {
            Insn::SetCookie {
                db,
                cookie: Cookie::SchemaVersion,
                ..
            } => Some(*db),
            _ => None,
        })
        .collect();
    changed_database_ids.sort_unstable();
    changed_database_ids.dedup();
    for database_id in changed_database_ids {
        let can_mark = resolver.with_schema(database_id, |schema| -> Result<bool> {
            fail_if_built_by_newer_version(schema)?;
            Ok(!built_under_current_rules(schema)
                && indexes_depending_on_rules(schema).next().is_none())
        })?;
        if can_mark {
            emit_rules_version_table_if_index_depends(program, resolver, database_id)?;
        }
    }
    Ok(())
}

/// Runs while a statement executes, after it has changed the schema.
pub(crate) fn has_index_depending_on_rules(connection: &Connection, database_id: usize) -> bool {
    connection.with_schema(database_id, |schema| {
        indexes_depending_on_rules(schema).next().is_some()
    })
}

/// Called by REINDEX after it has rebuilt every index that depends on the
/// rules in `database_ids`. A database that has no such index stays
/// unmarked.
pub(crate) fn record_rules_version_after_reindex(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_ids: &[usize],
) -> Result<()> {
    if !resolver.enable_custom_types {
        return Ok(());
    }
    for &database_id in database_ids {
        let is_current = resolver.with_schema(database_id, |schema| -> Result<bool> {
            fail_if_built_by_newer_version(schema)?;
            Ok(built_under_current_rules(schema))
        })?;
        if !is_current {
            emit_rules_version_table_if_index_depends(program, resolver, database_id)?;
        }
    }
    Ok(())
}

pub(crate) fn built_under_current_rules(schema: &Schema) -> bool {
    newest_rules_version(schema) == Some(RULES_VERSION)
}

/// A newer version's indexes follow rules this version doesn't know, and a
/// newer version may count more indexes as depending on the rules than this
/// one does. Any index this version builds would be trusted by the newer
/// version. So this version doesn't read, write or change the database.
fn fail_if_built_by_newer_version(schema: &Schema) -> Result<()> {
    if newest_rules_version(schema).is_some_and(|version| version > RULES_VERSION) {
        bail_parse_error!(
            "this database's indexes were built by a newer version of Turso's experimental custom types. Use that version or a later one"
        );
    }
    Ok(())
}

/// Whether the name is one of this module's marker tables, which only this
/// module writes.
pub(crate) fn is_rules_version_table(name: &str) -> bool {
    rules_version_of_table(name).is_some()
}

fn newest_rules_version(schema: &Schema) -> Option<u32> {
    schema
        .table_names(NamespaceId::ROOT)
        .filter_map(rules_version_of_table)
        .max()
}

fn rules_version_of_table(name: &str) -> Option<u32> {
    name.strip_prefix(TURSO_INTERNAL_PREFIX)?
        .strip_prefix(VERSION_TABLE_PREFIX)?
        .parse()
        .ok()
}

/// Every index of the database that depends on the rules, with its table.
pub(crate) fn indexes_depending_on_rules(
    schema: &Schema,
) -> impl Iterator<Item = (Arc<BTreeTable>, &Arc<Index>)> {
    schema.namespace_ids().flat_map(move |namespace| {
        schema.indexes(namespace).filter_map(move |index| {
            let table = schema.get_btree_table(namespace, &index.table_name)?;
            index_depends_on_rules(index, &table).then_some((table, index))
        })
    })
}

/// Whether `index` is one of the database's indexes, rather than one this
/// statement creates, and depends on the rules.
pub(crate) fn is_stored_index_depending_on_rules(schema: &Schema, index: &Index) -> bool {
    schema.namespace_ids().any(|namespace| {
        let Some(stored) = schema.get_index(namespace, &index.table_name, &index.name) else {
            return false;
        };
        stored.root_page == index.root_page
            && schema
                .get_btree_table(namespace, &index.table_name)
                .is_some_and(|table| index_depends_on_rules(stored, &table))
    })
}

/// Whether the index stores a value or an order that the rules may decide.
/// Indexes with an index method store their own format.
///
/// The answer comes from the schema's text alone, never from the registry of
/// custom types: the registry differs between databases and changes with
/// CREATE TYPE and DROP TYPE, and an index built under one registry would
/// stop counting under another. An index counts as plain only if SQLite
/// itself could have built it with the same meaning, so the set of plain
/// indexes must never grow in a later version of Turso.
fn index_depends_on_rules(index: &Index, table: &BTreeTable) -> bool {
    if index.index_method.is_some() {
        return false;
    }
    index
        .where_clause
        .iter()
        .any(|expr| expr_depends_on_rules(expr, table))
        || index.columns.iter().any(|column| match &column.expr {
            Some(expr) => expr_depends_on_rules(expr, table),
            None => table
                .columns()
                .get(column.pos_in_table)
                .is_some_and(|column| column_depends_on_rules(column, table)),
        })
}

/// Column references are resolved against `table`: by name in index
/// expressions, and by position in generated column expressions, which the
/// schema binds to the table. Anything that can't appear in an index SQLite
/// builds counts as depending on the rules.
fn expr_depends_on_rules(expr: &Expr, table: &BTreeTable) -> bool {
    let mut depends = false;
    let _ = walk_expr(expr, &mut |node: &Expr| -> Result<WalkControl> {
        if depends {
            return Ok(WalkControl::SkipChildren);
        }
        depends = match node {
            Expr::Id(name)
            | Expr::Name(name)
            | Expr::Qualified(_, name)
            | Expr::DoublyQualified(_, _, name) => match table.get_column(name.as_str()) {
                Some((_, column)) => column_depends_on_rules(column, table),
                None => true,
            },
            Expr::Column {
                table: owner,
                column,
                ..
            } if owner.is_self_table() => match table.columns().get(*column) {
                Some(column) => column_depends_on_rules(column, table),
                None => true,
            },
            Expr::Cast { type_name, .. } => !type_name.as_ref().is_some_and(is_sqlite_cast_type),
            Expr::FunctionCall { name, args, .. } => {
                !is_sqlite_function_call(name.as_str(), args.len())
            }
            Expr::FunctionCallStar { name, .. } => !is_sqlite_function_call(name.as_str(), 0),
            Expr::Between { .. }
            | Expr::Binary(..)
            | Expr::Case { .. }
            | Expr::Collate(..)
            | Expr::InList { .. }
            | Expr::IsNull(_)
            | Expr::Like {
                op: LikeOperator::Like | LikeOperator::Glob,
                ..
            }
            | Expr::Literal(_)
            | Expr::NotNull(_)
            | Expr::Parenthesized(_)
            | Expr::RowId { .. }
            | Expr::Unary(..) => false,
            Expr::Column { .. }
            | Expr::Array { .. }
            | Expr::Default
            | Expr::Exists(_)
            | Expr::FieldAccess { .. }
            | Expr::InSelect { .. }
            | Expr::InTable { .. }
            // REGEXP and MATCH call functions that SQLite doesn't have.
            | Expr::Like { .. }
            | Expr::MergedColumn(_)
            | Expr::Raise(..)
            | Expr::Register(_)
            | Expr::Subquery(_)
            | Expr::SubqueryResult { .. }
            | Expr::Subscript { .. }
            | Expr::Variable(_) => true,
        };
        Ok(WalkControl::Continue)
    });
    depends
}

/// Only STRICT tables have custom column types, and SQLite allows exactly
/// these types in them. Array columns are encoded in any table. A generated
/// column's value comes from its expression.
fn column_depends_on_rules(column: &Column, table: &BTreeTable) -> bool {
    let is_sqlite_strict_type = ["INT", "INTEGER", "REAL", "TEXT", "BLOB", "ANY"]
        .iter()
        .any(|ty| column.ty_str.eq_ignore_ascii_case(ty));
    column.is_array()
        || (table.is_strict && !is_sqlite_strict_type)
        || column
            .generated_expr()
            .is_some_and(|expr| expr_depends_on_rules(expr, table))
}

/// CAST to these names means the same with and without custom types; any
/// other name may be a custom type now or later.
fn is_sqlite_cast_type(ty: &Type) -> bool {
    ty.size.is_none()
        && ty.array_dimensions == 0
        && ["INT", "INTEGER", "REAL", "TEXT", "BLOB"]
            .iter()
            .any(|name| ty.name.eq_ignore_ascii_case(name))
}

/// Whether SQLite has the function, so that its result never depends on
/// Turso's custom-type rules.
fn is_sqlite_function_call(name: &str, arg_count: usize) -> bool {
    matches!(
        Func::resolve_function(name, arg_count),
        Ok(Some(func)) if is_sqlite_function(&func)
    )
}

fn is_sqlite_function(func: &Func) -> bool {
    match func {
        Func::Scalar(func) => matches!(
            func,
            ScalarFunc::Cast
                | ScalarFunc::Changes
                | ScalarFunc::Char
                | ScalarFunc::Coalesce
                | ScalarFunc::Concat
                | ScalarFunc::ConcatWs
                | ScalarFunc::Glob
                | ScalarFunc::IfNull
                | ScalarFunc::Iif
                | ScalarFunc::Instr
                | ScalarFunc::Like
                | ScalarFunc::Abs
                | ScalarFunc::Upper
                | ScalarFunc::Lower
                | ScalarFunc::Random
                | ScalarFunc::RandomBlob
                | ScalarFunc::Trim
                | ScalarFunc::LTrim
                | ScalarFunc::RTrim
                | ScalarFunc::Round
                | ScalarFunc::Length
                | ScalarFunc::OctetLength
                | ScalarFunc::Min
                | ScalarFunc::Max
                | ScalarFunc::Nullif
                | ScalarFunc::Sign
                | ScalarFunc::Substr
                | ScalarFunc::Substring
                | ScalarFunc::Soundex
                | ScalarFunc::Date
                | ScalarFunc::Time
                | ScalarFunc::TotalChanges
                | ScalarFunc::DateTime
                | ScalarFunc::Subtype
                | ScalarFunc::Typeof
                | ScalarFunc::Unicode
                | ScalarFunc::Unistr
                | ScalarFunc::UnistrQuote
                | ScalarFunc::Quote
                | ScalarFunc::SqliteVersion
                | ScalarFunc::SqliteSourceId
                | ScalarFunc::UnixEpoch
                | ScalarFunc::JulianDay
                | ScalarFunc::Hex
                | ScalarFunc::Unhex
                | ScalarFunc::ZeroBlob
                | ScalarFunc::LastInsertRowid
                | ScalarFunc::Replace
                | ScalarFunc::StrfTime
                | ScalarFunc::Printf
                | ScalarFunc::Likely
                | ScalarFunc::TimeDiff
                | ScalarFunc::Likelihood
                | ScalarFunc::Unlikely
        ),
        Func::Math(_) => true,
        #[cfg(feature = "json")]
        Func::Json(_) => true,
        _ => false,
    }
}

const VERSION_TABLE_PREFIX: &str = "custom_type_rules_v";

fn version_table_name() -> String {
    format!("{TURSO_INTERNAL_PREFIX}{VERSION_TABLE_PREFIX}{RULES_VERSION}")
}

/// The check runs when the statement executes, on the schema the statement
/// has changed.
fn emit_rules_version_table_if_index_depends(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
) -> Result<()> {
    let skip = program.allocate_label();
    program.emit_insn(Insn::IfNoIndexDependsOnCustomTypeRules {
        db: database_id,
        target_pc: skip,
    });
    emit_rules_version_table(program, resolver, database_id)?;
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

fn emit_rules_version_table(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
) -> Result<()> {
    let (sqlite_schema, schema_version) = resolver.with_schema(database_id, |schema| {
        let sqlite_schema = schema
            .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
            .expect("every database has sqlite_schema");
        (sqlite_schema, schema.schema_version)
    });
    let name = version_table_name();
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
        db: database_id,
    });
    let root_page_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: database_id,
        root: root_page_reg,
        flags: CreateBTreeFlags::new_table(),
    });
    emit_schema_entry(
        program,
        resolver,
        cursor_id,
        None,
        SchemaEntryType::Table,
        &name,
        &name,
        root_page_reg,
        Some(format!("CREATE TABLE {name}(unused)")),
    )?;
    program.emit_insn(Insn::Close { cursor_id });
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(format!("tbl_name = '{name}' AND type = 'table'")),
        trigger_target_database_id: None,
    });
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });
    Ok(())
}
