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
use turso_parser::parser::MAX_EXPR_DEPTH;

use crate::namespace::NamespaceId;
use crate::schema::Schema;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{walk_expr, walk_expr_mut, WalkControl};
use crate::translate::schema::{
    emit_schema_row, emit_schema_rows_delete, SchemaEntryType, SQLITE_TABLEID,
};
use crate::translate::trigger_exec::{
    rewrite_from_clause_expressions, rewrite_select_expressions, rewrite_window_expressions,
};
use crate::util::walk_select_expressions;
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
///
/// A statement that stores an expression in the schema, such as CREATE VIEW,
/// CREATE TRIGGER or a column default, keeps the call.
pub(crate) fn expand_function_calls(
    stmt: &mut ast::Stmt,
    resolver: &Resolver,
    connection: &Connection,
) -> Result<bool> {
    let dialect = &resolver.catalog.dialect;
    if !dialect.expands_function_calls(resolver.schema()) {
        return Ok(false);
    }
    let mut replacement =
        |call: &ast::Expr| dialect.expand_function_call(resolver.schema(), connection, call);
    let mut expand = ExpandCall {
        replacement: &mut replacement,
        replaced_calls: 0,
    };
    expand_in_statement(stmt, &mut expand)?;
    Ok(expand.replaced_calls > 0)
}

fn expand_in_statement(stmt: &mut ast::Stmt, expand: &mut ExpandCall) -> Result<()> {
    match stmt {
        ast::Stmt::Select(select) => expand_in_select(select, expand),
        ast::Stmt::Insert {
            with,
            or_conflict: _,
            tbl_name: _,
            columns: _,
            body,
            returning,
        } => {
            expand_in_with(with.as_mut(), expand)?;
            if let ast::InsertBody::Select(select, upsert) = body {
                expand_in_select(select, expand)?;
                expand_in_upsert(upsert.as_deref_mut(), expand)?;
            }
            expand_in_result_columns(returning, expand)
        }
        ast::Stmt::Update(ast::Update {
            with,
            or_conflict: _,
            tbl_name: _,
            indexed: _,
            sets,
            from,
            where_clause,
            returning,
        }) => {
            expand_in_with(with.as_mut(), expand)?;
            for set in sets {
                expand_in_expr(&mut set.expr, expand)?;
            }
            if let Some(from) = from {
                rewrite_from_clause_expressions(from, &mut |expr| {
                    expand_in_clause_expr(expr, expand)
                })?;
            }
            if let Some(where_clause) = where_clause {
                expand_in_expr(where_clause, expand)?;
            }
            expand_in_result_columns(returning, expand)
        }
        ast::Stmt::Delete {
            with,
            tbl_name: _,
            indexed: _,
            where_clause,
            returning,
            truncate: _,
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
        // The engine does not expand the calls of the other statements.
        // Those that have expressions store them in the schema, or use them
        // for a name or a setting (ATTACH, PRAGMA, VACUUM INTO).
        ast::Stmt::AlterTable(_)
        | ast::Stmt::Analyze { .. }
        | ast::Stmt::Attach { .. }
        | ast::Stmt::Begin { .. }
        | ast::Stmt::Commit { .. }
        | ast::Stmt::CreateIndex { .. }
        | ast::Stmt::CreateTable { .. }
        | ast::Stmt::CreateTrigger { .. }
        | ast::Stmt::CreateView { .. }
        | ast::Stmt::CreatePolicy(_)
        | ast::Stmt::DropPolicy { .. }
        | ast::Stmt::CreateMaterializedView { .. }
        | ast::Stmt::CreateVirtualTable(_)
        | ast::Stmt::CreateType { .. }
        | ast::Stmt::CreateDomain { .. }
        | ast::Stmt::Detach { .. }
        | ast::Stmt::DropIndex { .. }
        | ast::Stmt::DropTable { .. }
        | ast::Stmt::DropTrigger { .. }
        | ast::Stmt::DropView { .. }
        | ast::Stmt::DropType { .. }
        | ast::Stmt::DropDomain { .. }
        | ast::Stmt::Pragma { .. }
        | ast::Stmt::Reindex { .. }
        | ast::Stmt::Release { .. }
        | ast::Stmt::Rollback { .. }
        | ast::Stmt::Savepoint { .. }
        | ast::Stmt::Vacuum { .. }
        | ast::Stmt::Optimize { .. }
        | ast::Stmt::CreateSequence { .. }
        | ast::Stmt::DropSequence { .. }
        | ast::Stmt::AlterSequenceOwnedBy { .. }
        | ast::Stmt::CreateSchema { .. }
        | ast::Stmt::CreateRole { .. }
        | ast::Stmt::Grant(_)
        | ast::Stmt::DropSchema { .. }
        | ast::Stmt::CreateFunction(_)
        | ast::Stmt::DropFunction(_) => Ok(()),
    }
}

/// The expansion of the calls of one statement.
struct ExpandCall<'a> {
    /// The expression that replaces a call, or None to keep the call.
    replacement: &'a mut dyn FnMut(&ast::Expr) -> Result<Option<ast::Expr>>,
    replaced_calls: usize,
}

fn expand_in_with(with: Option<&mut ast::With>, expand: &mut ExpandCall) -> Result<()> {
    for cte in with.into_iter().flat_map(|with| &mut with.ctes) {
        expand_in_select(&mut cte.select, expand)?;
    }
    Ok(())
}

fn expand_in_select(select: &mut ast::Select, expand: &mut ExpandCall) -> Result<()> {
    rewrite_select_expressions(select, &mut |expr| expand_in_clause_expr(expr, expand))
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

/// [`expand_in_expr`] for a walker of the expressions of a clause, which
/// then does not walk the parts of `expr`.
fn expand_in_clause_expr(expr: &mut ast::Expr, expand: &mut ExpandCall) -> Result<WalkControl> {
    expand_in_expr(expr, expand)?;
    Ok(WalkControl::SkipChildren)
}

/// Expands the calls in `expr` and in its subqueries. Fails when `expr` is
/// then deeper than the parser permits: the engine and the dialect walk an
/// expression with recursion.
fn expand_in_expr(expr: &mut ast::Expr, expand: &mut ExpandCall) -> Result<()> {
    let replaced_calls_before = expand.replaced_calls;
    walk_expr_mut(expr, &mut |expr| expand_call_or_subquery(expr, expand))?;
    if expand.replaced_calls > replaced_calls_before && is_deeper_than(expr, MAX_EXPR_DEPTH) {
        return Err(expression_tree_is_too_large());
    }
    Ok(())
}

/// Expands the calls in the subquery that `expr` is, or the call that `expr`
/// is. The arguments of a call are expanded before the call, so the dialect
/// sees the arguments as they will run. No argument that the dialect gets,
/// and no expression that it gives for a call, is deeper than the parser
/// permits.
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
                    expand_in_clause_expr(expr, expand)
                })?;
            }
            if let Some(expansion) = (expand.replacement)(expr)? {
                if is_deeper_than(&expansion, MAX_EXPR_DEPTH) {
                    return Err(expression_tree_is_too_large());
                }
                *expr = expansion;
                expand.replaced_calls += 1;
            }
            return Ok(WalkControl::SkipChildren);
        }
        _ => {}
    }
    Ok(WalkControl::Continue)
}

/// Whether `expr` has more than `limit` levels. The expressions of a
/// subquery are one level below the subquery, as the parser counts them.
fn is_deeper_than(expr: &ast::Expr, limit: usize) -> bool {
    has_level_above(expr, 1, limit)
}

/// Whether `expr`, which is at level `level`, has a part at a level above
/// `limit`.
fn has_level_above(expr: &ast::Expr, level: usize, limit: usize) -> bool {
    let mut pending = vec![(expr, level)];
    while let Some((expr, level)) = pending.pop() {
        if level > limit {
            return true;
        }
        let mut is_expr = true;
        let _ = walk_expr(expr, &mut |part: &ast::Expr| {
            if std::mem::take(&mut is_expr) {
                return Ok(WalkControl::Continue);
            }
            pending.push((part, level + 1));
            Ok(WalkControl::SkipChildren)
        });
        let subquery = match expr {
            ast::Expr::Subquery(select) | ast::Expr::Exists(select) => Some(select),
            ast::Expr::InSelect { rhs, .. } => Some(rhs),
            _ => None,
        };
        let Some(subquery) = subquery else {
            continue;
        };
        // The walk gives each expression of the clauses of the subquery,
        // and this function then looks at its parts.
        let mut is_too_deep = false;
        let _ = walk_select_expressions(subquery, &mut |part: &ast::Expr| {
            is_too_deep = is_too_deep || has_level_above(part, level + 1, limit);
            Ok(WalkControl::SkipChildren)
        });
        if is_too_deep {
            return true;
        }
    }
    false
}

/// The error of the parser for an expression that is too deep.
fn expression_tree_is_too_large() -> LimboError {
    LimboError::ParseError(format!(
        "Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})"
    ))
}
