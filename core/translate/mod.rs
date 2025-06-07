//! The VDBE bytecode code generator.
//!
//! This module is responsible for translating the SQL AST into a sequence of
//! instructions for the VDBE. The VDBE is a register-based virtual machine that
//! executes bytecode instructions. This code generator is responsible for taking
//! the SQL AST and generating the corresponding VDBE instructions. For example,
//! a SELECT statement will be translated into a sequence of instructions that
//! will read rows from the database and filter them according to a WHERE clause.

pub(crate) mod aggregation;
pub(crate) mod collate;
pub(crate) mod delete;
pub(crate) mod emitter;
pub(crate) mod expr;
pub(crate) mod group_by;
pub(crate) mod index;
pub(crate) mod insert;
pub(crate) mod main_loop;
pub(crate) mod optimizer;
pub(crate) mod order_by;
pub(crate) mod plan;
pub(crate) mod planner;
pub(crate) mod pragma;
pub(crate) mod result_row;
pub(crate) mod schema;
pub(crate) mod select;
pub(crate) mod subquery;
pub(crate) mod transaction;
pub(crate) mod update;
mod values;

use crate::fast_lock::SpinLock;
use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::translate::delete::translate_delete;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::Program;
use crate::{bail_parse_error, Connection, LimboError, Result, SymbolTable};
use fallible_iterator::FallibleIterator as _;
use index::{translate_create_index, translate_drop_index};
use insert::translate_insert;
use limbo_sqlite3_parser::ast::{self, Delete, Insert};
use limbo_sqlite3_parser::lexer::sql::Parser;
use schema::{
    translate_create_table, translate_create_virtual_table, translate_drop_table, ParseSchema,
    SQLITE_TABLEID,
};
use select::translate_select;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use tracing::{instrument, Level};
use transaction::{translate_tx_begin, translate_tx_commit};
use update::translate_update;

#[instrument(skip_all, level = Level::TRACE)]
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    database_header: Arc<SpinLock<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<Connection>,
    syms: &SymbolTable,
    query_mode: QueryMode,
    _input: &str, // TODO: going to be used for CREATE VIEW
) -> Result<Program> {
    let change_cnt_on = matches!(
        stmt,
        ast::Stmt::CreateIndex { .. } | ast::Stmt::Delete(..) | ast::Stmt::Insert(..)
    );

    // These options will be extended whithin each translate program
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 2,
        approx_num_labels: 2,
    });

    program.prologue();

    program = match stmt {
        // There can be no nesting with pragma, so lift it up here
        ast::Stmt::Pragma(name, body) => pragma::translate_pragma(
            query_mode,
            schema,
            &name,
            body.map(|b| *b),
            database_header.clone(),
            pager,
            connection.clone(),
            program,
        )?,
        stmt => translate_inner(schema, stmt, syms, query_mode, program)?,
    };

    // TODO: bring epilogue here when I can sort out what instructions correspond to a Write or a Read transaction

    Ok(program.build(database_header, connection, change_cnt_on))
}

// TODO: for now leaving the return value as a Program. But ideally to support nested parsing of arbitraty
// statements, we would have to return a program builder instead
/// Translate SQL statement into bytecode program.
pub fn translate_inner(
    schema: &Schema,
    stmt: ast::Stmt,
    syms: &SymbolTable,
    query_mode: QueryMode,
    program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let program = match stmt {
        ast::Stmt::AlterTable(a) => {
            let (table_name, alter_table) = a.as_ref();

            match alter_table {
                ast::AlterTableBody::RenameTo(name) => {
                    let rename = &name.0;
                    let name = &table_name.name.0;

                    let Some(table) = schema.tables.get(name) else {
                        return Err(LimboError::ParseError(format!("no such table: {name}")));
                    };

                    if schema.tables.contains_key(rename) {
                        return Err(LimboError::ParseError(format!(
                            "there is already another table or index with this name: {rename}"
                        )));
                    };

                    let Some(btree) = table.btree() else { todo!() };

                    let mut btree = (*btree).clone();
                    btree.name = rename.clone();

                    let sql = btree.to_sql();

                    let stmt = format!(
                        r#"
                            UPDATE {SQLITE_TABLEID}
                            SET name = '{rename}'
                              , tbl_name = '{rename}'
                              , sql = '{sql}'
                            WHERE tbl_name = '{name}'
                        "#,
                    );

                    let mut parser = Parser::new(stmt.as_bytes());
                    let Some(ast::Cmd::Stmt(ast::Stmt::Update(mut update))) = parser.next()? else {
                        unreachable!();
                    };

                    translate_update(
                        QueryMode::Normal,
                        schema,
                        &mut update,
                        syms,
                        ParseSchema::Reload,
                        program,
                    )?
                }
                _ => todo!(),
            }
        }
        ast::Stmt::Analyze(_) => bail_parse_error!("ANALYZE not supported yet"),
        ast::Stmt::Attach { .. } => bail_parse_error!("ATTACH not supported yet"),
        ast::Stmt::Begin(tx_type, tx_name) => translate_tx_begin(tx_type, tx_name, program)?,
        ast::Stmt::Commit(tx_name) => translate_tx_commit(tx_name, program)?,
        ast::Stmt::CreateIndex {
            unique,
            if_not_exists,
            idx_name,
            tbl_name,
            columns,
            ..
        } => translate_create_index(
            query_mode,
            (unique, if_not_exists),
            &idx_name.name.0,
            &tbl_name.0,
            &columns,
            schema,
            program,
        )?,
        ast::Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => translate_create_table(
            query_mode,
            tbl_name,
            temporary,
            *body,
            if_not_exists,
            schema,
            program,
        )?,
        ast::Stmt::CreateTrigger { .. } => bail_parse_error!("CREATE TRIGGER not supported yet"),
        ast::Stmt::CreateView { .. } => bail_parse_error!("CREATE VIEW not supported yet"),
        ast::Stmt::CreateVirtualTable(vtab) => {
            translate_create_virtual_table(*vtab, schema, query_mode, &syms, program)?
        }
        ast::Stmt::Delete(delete) => {
            let Delete {
                tbl_name,
                where_clause,
                limit,
                ..
            } = *delete;
            translate_delete(
                query_mode,
                schema,
                &tbl_name,
                where_clause,
                limit,
                syms,
                program,
            )?
        }
        ast::Stmt::Detach(_) => bail_parse_error!("DETACH not supported yet"),
        ast::Stmt::DropIndex {
            if_exists,
            idx_name,
        } => translate_drop_index(query_mode, &idx_name.name.0, if_exists, schema, program)?,
        ast::Stmt::DropTable {
            if_exists,
            tbl_name,
        } => translate_drop_table(query_mode, tbl_name, if_exists, schema, program)?,
        ast::Stmt::DropTrigger { .. } => bail_parse_error!("DROP TRIGGER not supported yet"),
        ast::Stmt::DropView { .. } => bail_parse_error!("DROP VIEW not supported yet"),
        ast::Stmt::Pragma(..) => {
            bail_parse_error!("PRAGMA statement cannot be evaluated in a nested context")
        }
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Release(_) => bail_parse_error!("RELEASE not supported yet"),
        ast::Stmt::Rollback { .. } => bail_parse_error!("ROLLBACK not supported yet"),
        ast::Stmt::Savepoint(_) => bail_parse_error!("SAVEPOINT not supported yet"),
        ast::Stmt::Select(select) => {
            translate_select(
                query_mode,
                schema,
                *select,
                syms,
                program,
                plan::QueryDestination::ResultRows,
            )?
            .program
        }
        ast::Stmt::Update(mut update) => translate_update(
            query_mode,
            schema,
            &mut update,
            syms,
            ParseSchema::None,
            program,
        )?,
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
                query_mode,
                schema,
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
                syms,
                program,
            )?
        }
    };

    Ok(program)
}
