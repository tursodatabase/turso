//! The VDBE bytecode code generator.
//!
//! This module is responsible for translating the SQL AST into a sequence of
//! instructions for the VDBE. The VDBE is a register-based virtual machine that
//! executes bytecode instructions. This code generator is responsible for taking
//! the SQL AST and generating the corresponding VDBE instructions. For example,
//! a SELECT statement will be translated into a sequence of instructions that
//! will read rows from the database and filter them according to a WHERE clause.

pub(crate) mod aggregation;
pub(crate) mod alter;
pub(crate) mod analyze;
pub(crate) mod attach;
pub(crate) mod collate;
mod compound_select;
pub(crate) mod delete;
pub(crate) mod display;
pub(crate) mod emitter;
pub(crate) mod expr;
pub(crate) mod group_by;
pub(crate) mod index;
pub(crate) mod insert;
pub(crate) mod integrity_check;
pub(crate) mod logical;
pub(crate) mod main_loop;
pub(crate) mod optimizer;
pub(crate) mod order_by;
pub(crate) mod plan;
pub(crate) mod planner;
pub(crate) mod pragma;
pub(crate) mod result_row;
pub(crate) mod rollback;
pub(crate) mod schema;
pub(crate) mod select;
pub(crate) mod subquery;
pub(crate) mod transaction;
pub(crate) mod update;
pub(crate) mod upsert;
mod values;
pub(crate) mod view;

use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::translate::delete::translate_delete;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::Program;
use crate::{bail_parse_error, Connection, Result, SymbolTable};
use alter::translate_alter_table;
use analyze::translate_analyze;
use index::{translate_create_index, translate_drop_index};
use insert::translate_insert;
use rollback::translate_rollback;
use schema::{translate_create_table, translate_create_virtual_table, translate_drop_table};
use select::translate_select;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{instrument, Level};
use transaction::{translate_tx_begin, translate_tx_commit};
use turso_parser::ast::{self, Indexed};
use update::translate_update;

#[instrument(skip_all, level = Level::DEBUG)]
#[allow(clippy::too_many_arguments)]
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    pager: Rc<Pager>,
    connection: Arc<Connection>,
    syms: &SymbolTable,
    query_mode: QueryMode,
    input: &str,
) -> Result<Program> {
    tracing::trace!("querying {}", input);
    let change_cnt_on = matches!(
        stmt,
        ast::Stmt::CreateIndex { .. }
            | ast::Stmt::Delete { .. }
            | ast::Stmt::Insert { .. }
            | ast::Stmt::Update { .. }
    );

    let mut program = ProgramBuilder::new(
        query_mode,
        connection.get_capture_data_changes().clone(),
        // These options will be extended whithin each translate program
        ProgramBuilderOpts {
            num_cursors: 1,
            approx_num_insns: 32,
            approx_num_labels: 2,
        },
    );

    program.prologue();

    program = match stmt {
        // There can be no nesting with pragma, so lift it up here
        ast::Stmt::Pragma { name, body } => pragma::translate_pragma(
            schema,
            syms,
            &name,
            body,
            pager,
            connection.clone(),
            program,
        )?,
        stmt => translate_inner(schema, stmt, syms, program, &connection, input)?,
    };

    program.epilogue(schema);

    Ok(program.build(connection, change_cnt_on, input))
}

// TODO: for now leaving the return value as a Program. But ideally to support nested parsing of arbitraty
// statements, we would have to return a program builder instead
/// Translate SQL statement into bytecode program.
pub fn translate_inner(
    schema: &Schema,
    stmt: ast::Stmt,
    syms: &SymbolTable,
    program: ProgramBuilder,
    connection: &Arc<Connection>,
    input: &str,
) -> Result<ProgramBuilder> {
    let is_write = matches!(
        stmt,
        ast::Stmt::AlterTable { .. }
            | ast::Stmt::CreateIndex { .. }
            | ast::Stmt::CreateTable { .. }
            | ast::Stmt::CreateTrigger { .. }
            | ast::Stmt::CreateView { .. }
            | ast::Stmt::CreateMaterializedView { .. }
            | ast::Stmt::CreateVirtualTable(..)
            | ast::Stmt::Delete { .. }
            | ast::Stmt::DropIndex { .. }
            | ast::Stmt::DropTable { .. }
            | ast::Stmt::DropView { .. }
            | ast::Stmt::Reindex { .. }
            | ast::Stmt::Update { .. }
            | ast::Stmt::Insert { .. }
    );

    if is_write && connection.get_query_only() {
        bail_parse_error!("Cannot execute write statement in query_only mode")
    }

    let is_select = matches!(stmt, ast::Stmt::Select { .. });

    let mut program = match stmt {
        ast::Stmt::AlterTable(alter) => {
            translate_alter_table(alter, syms, schema, program, connection, input)?
        }
        ast::Stmt::Analyze { name } => translate_analyze(name, schema, syms, program)?,
        ast::Stmt::Attach { expr, db_name, key } => {
            attach::translate_attach(&expr, &db_name, &key, schema, syms, program)?
        }
        ast::Stmt::Begin { typ, name } => translate_tx_begin(typ, name, schema, program)?,
        ast::Stmt::Commit { name } => translate_tx_commit(name, program)?,
        ast::Stmt::CreateIndex {
            unique,
            if_not_exists,
            idx_name,
            tbl_name,
            columns,
            where_clause,
        } => {
            if where_clause.is_some() {
                bail_parse_error!("Partial indexes are not supported");
            }
            translate_create_index(
                (unique, if_not_exists),
                idx_name.name.as_str(),
                tbl_name.as_str(),
                &columns,
                schema,
                syms,
                program,
            )?
        }
        ast::Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => translate_create_table(
            tbl_name,
            temporary,
            body,
            if_not_exists,
            schema,
            syms,
            connection,
            program,
        )?,
        ast::Stmt::CreateTrigger { .. } => bail_parse_error!("CREATE TRIGGER not supported yet"),
        ast::Stmt::CreateView {
            view_name,
            select,
            columns,
            ..
        } => view::translate_create_view(
            schema,
            view_name.name.as_str(),
            &select,
            &columns,
            connection.clone(),
            syms,
            program,
        )?,
        ast::Stmt::CreateMaterializedView {
            view_name, select, ..
        } => view::translate_create_materialized_view(
            schema,
            view_name.name.as_str(),
            &select,
            connection.clone(),
            syms,
            program,
        )?,
        ast::Stmt::CreateVirtualTable(vtab) => {
            translate_create_virtual_table(vtab, schema, syms, program)?
        }
        ast::Stmt::Delete {
            tbl_name,
            where_clause,
            limit,
            returning,
            indexed,
            order_by,
            with,
        } => {
            if with.is_some() {
                bail_parse_error!("WITH clause is not supported in DELETE");
            }
            if indexed.is_some_and(|i| matches!(i, Indexed::IndexedBy(_))) {
                bail_parse_error!("INDEXED BY clause is not supported in DELETE");
            }
            if !order_by.is_empty() {
                bail_parse_error!("ORDER BY clause is not supported in DELETE");
            }
            translate_delete(
                schema,
                &tbl_name,
                where_clause,
                limit,
                returning,
                syms,
                program,
                connection,
            )?
        }
        ast::Stmt::Detach { name } => attach::translate_detach(&name, schema, syms, program)?,
        ast::Stmt::DropIndex {
            if_exists,
            idx_name,
        } => translate_drop_index(idx_name.name.as_str(), if_exists, schema, syms, program)?,
        ast::Stmt::DropTable {
            if_exists,
            tbl_name,
        } => translate_drop_table(tbl_name, if_exists, schema, syms, program)?,
        ast::Stmt::DropTrigger { .. } => bail_parse_error!("DROP TRIGGER not supported yet"),
        ast::Stmt::DropView {
            if_exists,
            view_name,
        } => view::translate_drop_view(schema, view_name.name.as_str(), if_exists, program)?,
        ast::Stmt::Pragma { .. } => {
            bail_parse_error!("PRAGMA statement cannot be evaluated in a nested context")
        }
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Release { .. } => bail_parse_error!("RELEASE not supported yet"),
        ast::Stmt::Rollback {
            tx_name,
            savepoint_name,
        } => translate_rollback(schema, syms, program, tx_name, savepoint_name)?,
        ast::Stmt::Savepoint { .. } => bail_parse_error!("SAVEPOINT not supported yet"),
        ast::Stmt::Select(select) => {
            translate_select(
                schema,
                select,
                syms,
                program,
                plan::QueryDestination::ResultRows,
                connection,
            )?
            .program
        }
        ast::Stmt::Update(mut update) => {
            translate_update(schema, &mut update, syms, program, connection)?
        }
        ast::Stmt::Vacuum { .. } => bail_parse_error!("VACUUM not supported yet"),
        ast::Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        } => translate_insert(
            schema,
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
            syms,
            program,
            connection,
        )?,
    };

    // Indicate write operations so that in the epilogue we can emit the correct type of transaction
    if is_write {
        program.begin_write_operation();
    }

    // Indicate read operations so that in the epilogue we can emit the correct type of transaction
    if is_select && !program.table_references.is_empty() {
        program.begin_read_operation();
    }

    Ok(program)
}
