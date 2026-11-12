//! `CREATE FUNCTION` and `DROP FUNCTION`.
//!
//! The engine has no functions that SQL defines. The database's dialect
//! decides whether it has them, what it stores for one and what a call does.
//! The engine opens the write transaction, lets the dialect emit the writes,
//! and sets the new schema version, as for every other schema change.

use std::sync::Arc;

use turso_parser::ast;

use crate::namespace::NamespaceId;
use crate::schema::Schema;
use crate::translate::emitter::Resolver;
use crate::translate::schema::{
    emit_schema_row, emit_schema_rows_delete, SchemaEntryType, SQLITE_TABLEID,
};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{Cookie, Insn};
use crate::{Connection, LimboError, Result, MAIN_DB_ID};

pub fn translate_create_function(
    function: &ast::CreateFunction,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    input: &str,
) -> Result<()> {
    translate_function_change(resolver, program, |program, schema| {
        resolver
            .catalog
            .dialect
            .translate_create_function(program, schema, connection, function, input)
    })
}

pub fn translate_drop_function(
    function: &ast::DropFunction,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    translate_function_change(resolver, program, |program, schema| {
        resolver
            .catalog
            .dialect
            .translate_drop_function(program, schema, connection, function)
    })
}

fn translate_function_change(
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    emit_writes: impl FnOnce(&mut ProgramBuilder, &Schema) -> Result<()>,
) -> Result<()> {
    let schema_version = resolver.with_schema(MAIN_DB_ID, |schema| schema.schema_version);
    program.begin_write_on_database(MAIN_DB_ID, schema_version)?;
    emit_writes(program, resolver.schema())?;
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });
    Ok(())
}

/// Opens `sqlite_schema` of the main database for writing rows of a type
/// that only the dialect knows, such as the PostgreSQL frontend's functions.
/// The engine skips rows of a type it does not know when it loads the
/// schema, so the dialect reads them back itself, as with
/// [`crate::Dialect::schema_state_query`].
pub fn open_schema_table_for_dialect_rows(
    program: &mut ProgramBuilder,
    schema: &Schema,
) -> Result<usize> {
    let table = schema
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .ok_or_else(|| LimboError::InternalError(format!("the schema has no {SQLITE_TABLEID}")))?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: 1i64.into(),
        db: MAIN_DB_ID,
    });
    Ok(cursor_id)
}

/// The `tbl_name` of each `sqlite_schema` row that a dialect stores. The
/// engine keeps the names with this prefix for itself, so no table has this
/// name. `DROP TABLE` deletes the rows whose `tbl_name` is the name of its
/// table. With this `tbl_name`, a dialect row is never one of them, also
/// when it has the name of a table.
const DIALECT_ROW_TABLE_NAME: &str = "__turso_internal_dialect_row";

/// Inserts the `sqlite_schema` row
/// `(row_type, name, DIALECT_ROW_TABLE_NAME, 0, sql)` through the cursor
/// that [`open_schema_table_for_dialect_rows`] opened.
///
/// The engine skips the row when it loads the schema, and the `ALTER TABLE`
/// statements do not change it.
pub fn emit_dialect_schema_row_insert(
    program: &mut ProgramBuilder,
    schema_cursor_id: usize,
    row_type: &'static str,
    name: &str,
    sql: &str,
) {
    emit_schema_row(
        program,
        schema_cursor_id,
        SchemaEntryType::Dialect(row_type),
        name,
        DIALECT_ROW_TABLE_NAME,
        0,
        Some(sql.to_string()),
    );
}

/// Deletes the `sqlite_schema` rows of type `row_type` named `name` through
/// the cursor that [`open_schema_table_for_dialect_rows`] opened.
pub fn emit_dialect_schema_row_delete(
    program: &mut ProgramBuilder,
    schema_cursor_id: usize,
    row_type: &'static str,
    name: &str,
) {
    emit_schema_rows_delete(
        program,
        schema_cursor_id,
        SchemaEntryType::Dialect(row_type),
        name,
    );
}
