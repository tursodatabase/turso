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
mod values;

use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::translate::delete::translate_delete;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::Program;
use crate::{bail_parse_error, Connection, Result, SymbolTable};
use alter::translate_alter_table;
use index::{translate_create_index, translate_drop_index};
use insert::translate_insert;
use rollback::translate_rollback;
use schema::{translate_create_table, translate_create_virtual_table, translate_drop_table};
use select::translate_select;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{instrument, Level};
use transaction::{translate_tx_begin, translate_tx_commit};
use turso_sqlite3_parser::ast::{self, Delete, Insert};
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
            | ast::Stmt::Delete(..)
            | ast::Stmt::Insert(..)
            | ast::Stmt::Update(..)
    );

    let mut program = ProgramBuilder::new(
        query_mode,
        connection.get_capture_data_changes().clone(),
        // These options will be extended whithin each translate program
        ProgramBuilderOpts {
            num_cursors: 1,
            approx_num_insns: 2,
            approx_num_labels: 2,
        },
    );

    program.prologue();

    program = match stmt {
        // There can be no nesting with pragma, so lift it up here
        ast::Stmt::Pragma(name, body) => pragma::translate_pragma(
            schema,
            &name,
            body.map(|b| *b),
            pager,
            connection.clone(),
            program,
        )?,
        stmt => translate_inner(schema, stmt, syms, program, &connection)?,
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
    mut program: ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<ProgramBuilder> {
    // Indicate write operations so that in the epilogue we can emit the correct type of transaction
    if matches!(
        stmt,
        ast::Stmt::AlterTable(..)
            | ast::Stmt::CreateIndex { .. }
            | ast::Stmt::CreateTable { .. }
            | ast::Stmt::CreateTrigger { .. }
            | ast::Stmt::CreateView { .. }
            | ast::Stmt::CreateVirtualTable(..)
            | ast::Stmt::Delete(..)
            | ast::Stmt::DropIndex { .. }
            | ast::Stmt::DropTable { .. }
            | ast::Stmt::Reindex { .. }
            | ast::Stmt::Update(..)
            | ast::Stmt::Insert(..)
    ) {
        program.begin_write_operation();
    }
    // Indicate read operations so that in the epilogue we can emit the correct type of transaction
    if matches!(stmt, ast::Stmt::Select { .. }) {
        program.begin_read_operation();
    }

    let program = match stmt {
        ast::Stmt::AlterTable(alter) => {
            translate_alter_table(*alter, syms, schema, program, connection)?
        }
        ast::Stmt::Analyze(_) => bail_parse_error!("ANALYZE not supported yet"),
        ast::Stmt::Attach { expr, db_name, key } => {
            attach::translate_attach(&expr, &db_name, &key, schema, syms, program)?
        }
        ast::Stmt::Begin(tx_type, tx_name) => {
            translate_tx_begin(tx_type, tx_name, schema, program)?
        }
        ast::Stmt::Commit(tx_name) => translate_tx_commit(tx_name, program)?,
        ast::Stmt::CreateIndex {
            unique,
            if_not_exists,
            idx_name,
            tbl_name,
            columns,
            ..
        } => translate_create_index(
            (unique, if_not_exists),
            idx_name.name.as_str(),
            tbl_name.as_str(),
            &columns,
            schema,
            program,
        )?,
        ast::Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => translate_create_table(tbl_name, temporary, *body, if_not_exists, schema, program)?,
        ast::Stmt::CreateTrigger { .. } => bail_parse_error!("CREATE TRIGGER not supported yet"),
        ast::Stmt::CreateView { .. } => bail_parse_error!("CREATE VIEW not supported yet"),
        ast::Stmt::CreateVirtualTable(vtab) => {
            translate_create_virtual_table(*vtab, schema, syms, program)?
        }
        ast::Stmt::Delete(delete) => {
            let Delete {
                tbl_name,
                where_clause,
                limit,
                returning,
                ..
            } = *delete;
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
        ast::Stmt::Detach(expr) => attach::translate_detach(&expr, schema, syms, program)?,
        ast::Stmt::DropIndex {
            if_exists,
            idx_name,
        } => translate_drop_index(idx_name.name.as_str(), if_exists, schema, program)?,
        ast::Stmt::DropTable {
            if_exists,
            tbl_name,
        } => translate_drop_table(tbl_name, if_exists, schema, program)?,
        ast::Stmt::DropTrigger { .. } => bail_parse_error!("DROP TRIGGER not supported yet"),
        ast::Stmt::DropView { .. } => bail_parse_error!("DROP VIEW not supported yet"),
        ast::Stmt::Pragma(..) => {
            bail_parse_error!("PRAGMA statement cannot be evaluated in a nested context")
        }
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Release(_) => bail_parse_error!("RELEASE not supported yet"),
        ast::Stmt::Rollback {
            tx_name,
            savepoint_name,
        } => translate_rollback(schema, syms, program, tx_name, savepoint_name)?,
        ast::Stmt::Savepoint(_) => bail_parse_error!("SAVEPOINT not supported yet"),
        ast::Stmt::Select(select) => {
            translate_select(
                schema,
                *select,
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
        ast::Stmt::Vacuum(_, _) => bail_parse_error!("VACUUM not supported yet"),
        ast::Stmt::Insert(insert) => {
            let Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } = *insert;
            translate_insert(
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
            )?
        }
    };

    Ok(program)
}
