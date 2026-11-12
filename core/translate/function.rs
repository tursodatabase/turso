//! Functions that SQL defines: `CREATE FUNCTION`, `DROP FUNCTION` and calls.
//!
//! The engine has no functions that SQL defines. The database's dialect
//! decides whether it has them, what it stores for one and what a call does.
//! For the two statements, the engine opens the write transaction, lets the
//! dialect emit the writes, and sets the new schema version, as for every
//! other schema change. For a call, the engine asks the dialect for the
//! expression that the call stands for, before it translates the statement.

use std::sync::Arc;

use turso_parser::ast;

use crate::namespace::NamespaceId;
use crate::schema::Schema;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{walk_expr_mut, WalkControl};
use crate::translate::schema::{
    emit_schema_row, emit_schema_rows_delete, SchemaEntryType, SQLITE_TABLEID,
};
use crate::translate::trigger_exec::{
    rewrite_from_clause_expressions, rewrite_select_expressions, rewrite_window_expressions,
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

/// Replaces each call of a function that the dialect defines as an
/// expression of the call's arguments with that expression
/// ([`crate::Dialect::expand_function_call`]), in the statements that run a
/// query or change data: SELECT, INSERT, UPDATE, DELETE and CREATE TABLE AS.
/// The rest of translation only sees the expression. Returns whether it
/// replaced a call.
pub(crate) fn expand_function_calls(
    stmt: &mut ast::Stmt,
    resolver: &Resolver,
    connection: &Connection,
) -> Result<bool> {
    let dialect = &resolver.catalog.dialect;
    if !dialect.expands_function_calls(resolver.schema()) {
        return Ok(false);
    }
    let mut expanded_a_call = false;
    let mut expand = |call: &ast::Expr| {
        let expansion = dialect.expand_function_call(resolver.schema(), connection, call)?;
        expanded_a_call |= expansion.is_some();
        Ok(expansion)
    };
    expand_in_statement(stmt, &mut expand)?;
    Ok(expanded_a_call)
}

fn expand_in_statement(stmt: &mut ast::Stmt, expand: &mut ExpandCall) -> Result<()> {
    match stmt {
        ast::Stmt::Select(select) => expand_in_select(select, expand),
        ast::Stmt::Insert {
            with,
            body,
            returning,
            ..
        } => {
            expand_in_with(with.as_mut(), expand)?;
            if let ast::InsertBody::Select(select, upsert) = body {
                expand_in_select(select, expand)?;
                expand_in_upsert(upsert.as_deref_mut(), expand)?;
            }
            expand_in_result_columns(returning, expand)
        }
        ast::Stmt::Update(update) => {
            expand_in_with(update.with.as_mut(), expand)?;
            for set in &mut update.sets {
                expand_in_expr(&mut set.expr, expand)?;
            }
            if let Some(from) = &mut update.from {
                rewrite_from_clause_expressions(from, &mut |expr| {
                    expand_call_or_subquery(expr, expand)
                })?;
            }
            if let Some(where_clause) = &mut update.where_clause {
                expand_in_expr(where_clause, expand)?;
            }
            expand_in_result_columns(&mut update.returning, expand)
        }
        ast::Stmt::Delete {
            with,
            where_clause,
            returning,
            ..
        } => {
            expand_in_with(with.as_mut(), expand)?;
            if let Some(where_clause) = where_clause {
                expand_in_expr(where_clause, expand)?;
            }
            expand_in_result_columns(returning, expand)
        }
        ast::Stmt::CreateTable {
            body: ast::CreateTableBody::AsSelect(select),
            ..
        } => expand_in_select(select, expand),
        _ => Ok(()),
    }
}

/// The expression that replaces a call, or None to keep the call.
type ExpandCall<'a> = dyn FnMut(&ast::Expr) -> Result<Option<ast::Expr>> + 'a;

fn expand_in_with(with: Option<&mut ast::With>, expand: &mut ExpandCall) -> Result<()> {
    for cte in with.into_iter().flat_map(|with| &mut with.ctes) {
        expand_in_select(&mut cte.select, expand)?;
    }
    Ok(())
}

fn expand_in_select(select: &mut ast::Select, expand: &mut ExpandCall) -> Result<()> {
    rewrite_select_expressions(select, &mut |expr| expand_call_or_subquery(expr, expand))
}

fn expand_in_result_columns(
    columns: &mut [ast::ResultColumn],
    expand: &mut ExpandCall,
) -> Result<()> {
    for column in columns {
        if let ast::ResultColumn::Expr(expr, _) = column {
            expand_in_expr(expr, expand)?;
        }
    }
    Ok(())
}

fn expand_in_upsert(mut upsert: Option<&mut ast::Upsert>, expand: &mut ExpandCall) -> Result<()> {
    while let Some(clause) = upsert {
        if let Some(index) = &mut clause.index {
            for target in &mut index.targets {
                expand_in_expr(&mut target.expr, expand)?;
            }
            if let Some(where_clause) = &mut index.where_clause {
                expand_in_expr(where_clause, expand)?;
            }
        }
        if let ast::UpsertDo::Set { sets, where_clause } = &mut clause.do_clause {
            for set in sets {
                expand_in_expr(&mut set.expr, expand)?;
            }
            if let Some(where_clause) = where_clause {
                expand_in_expr(where_clause, expand)?;
            }
        }
        upsert = clause.next.as_deref_mut();
    }
    Ok(())
}

/// Expands the calls in `expr` and in its subqueries.
fn expand_in_expr(expr: &mut ast::Expr, expand: &mut ExpandCall) -> Result<()> {
    walk_expr_mut(expr, &mut |expr| expand_call_or_subquery(expr, expand))?;
    Ok(())
}

/// Expands the calls in the subquery that `expr` is, or the call that `expr`
/// is. The arguments of a call are expanded before the call, so the dialect
/// sees the arguments as they will run.
fn expand_call_or_subquery(expr: &mut ast::Expr, expand: &mut ExpandCall) -> Result<WalkControl> {
    match expr {
        ast::Expr::Subquery(select) | ast::Expr::Exists(select) => {
            expand_in_select(select, expand)?;
        }
        ast::Expr::InSelect { rhs, .. } => expand_in_select(rhs, expand)?,
        ast::Expr::FunctionCall {
            args,
            order_by,
            within_group,
            filter_over,
            ..
        } => {
            for arg in args.iter_mut() {
                expand_in_expr(arg, expand)?;
            }
            for sorted_column in order_by.iter_mut().chain(within_group.iter_mut()) {
                expand_in_expr(&mut sorted_column.expr, expand)?;
            }
            if let Some(filter) = &mut filter_over.filter_clause {
                expand_in_expr(filter, expand)?;
            }
            if let Some(ast::Over::Window(window)) = filter_over.over_clause.as_deref_mut() {
                rewrite_window_expressions(window, &mut |expr| {
                    expand_call_or_subquery(expr, expand)
                })?;
            }
            if let Some(expansion) = expand(expr)? {
                *expr = expansion;
            }
            return Ok(WalkControl::SkipChildren);
        }
        _ => {}
    }
    Ok(WalkControl::Continue)
}
