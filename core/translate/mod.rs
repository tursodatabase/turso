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
pub(crate) mod expression_index;
pub(crate) mod fkeys;
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
pub(crate) mod trigger;
pub(crate) mod trigger_exec;
pub(crate) mod update;
pub(crate) mod upsert;
pub(crate) mod vacuum;
mod values;
pub(crate) mod view;
mod window;

use crate::authorizer::{self, action};
use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::sync::Arc;
use crate::translate::delete::translate_delete;
use crate::translate::emitter::Resolver;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::Program;
use crate::{bail_parse_error, Connection, LimboError, Result, SymbolTable};
use alter::translate_alter_table;
use analyze::translate_analyze;
use index::{translate_create_index, translate_drop_index, translate_optimize};
use insert::translate_insert;
use rollback::{translate_release, translate_rollback, translate_savepoint};
use schema::{translate_create_table, translate_create_virtual_table, translate_drop_table};
use select::translate_select;
use tracing::{instrument, Level};
use transaction::{translate_tx_begin, translate_tx_commit};
use turso_parser::ast::{self, Indexed};
use update::translate_update;

#[instrument(skip_all, level = Level::DEBUG)]
#[allow(clippy::too_many_arguments)]
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    pager: Arc<Pager>,
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
        connection.get_capture_data_changes_info().clone(),
        // These options will be extended whithin each translate program
        ProgramBuilderOpts {
            num_cursors: 1,
            approx_num_insns: 32,
            approx_num_labels: 2,
        },
    );

    program.prologue();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        connection.attached_databases(),
        syms,
    );

    match stmt {
        // There can be no nesting with pragma, so lift it up here
        ast::Stmt::Pragma { name, body } => {
            let pragma_name = name.name.as_str();
            let pragma_arg = body.as_ref().map(|b| match b {
                ast::PragmaBody::Equals(v) | ast::PragmaBody::Call(v) => format!("{v}"),
            });
            check_auth(
                &connection,
                action::SQLITE_PRAGMA,
                Some(pragma_name),
                pragma_arg.as_deref(),
                Some("main"),
                None,
            )?;
            pragma::translate_pragma(
                &resolver,
                &name,
                body,
                pager,
                connection.clone(),
                &mut program,
            )?;
        }
        stmt => translate_inner(stmt, &mut resolver, &mut program, &connection, input)?,
    };

    program.epilogue(schema);

    program.build(connection, change_cnt_on, input)
}

/// Check the authorizer and return an error if the action is denied.
/// Returns Ok(true) if the action is allowed, Ok(false) if ignored (SQLITE_IGNORE).
fn check_auth(
    connection: &Connection,
    action_code: i32,
    arg3: Option<&str>,
    arg4: Option<&str>,
    db_name: Option<&str>,
    trigger_name: Option<&str>,
) -> Result<bool> {
    let rc = connection.auth_check(action_code, arg3, arg4, db_name, trigger_name);
    match rc {
        authorizer::SQLITE_OK => Ok(true),
        authorizer::SQLITE_DENY => Err(LimboError::AuthDenied("not authorized".to_string())),
        authorizer::SQLITE_IGNORE => Ok(false),
        _ => Err(LimboError::AuthDenied("authorizer malfunction".to_string())),
    }
}

// TODO: for now leaving the return value as a Program. But ideally to support nested parsing of arbitraty
// statements, we would have to return a program builder instead
/// Translate SQL statement into bytecode program.
pub fn translate_inner(
    stmt: ast::Stmt,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    input: &str,
) -> Result<()> {
    let is_write = matches!(
        stmt,
        ast::Stmt::AlterTable { .. }
            | ast::Stmt::Analyze { .. }
            | ast::Stmt::CreateIndex { .. }
            | ast::Stmt::CreateTable { .. }
            | ast::Stmt::CreateTrigger { .. }
            | ast::Stmt::CreateView { .. }
            | ast::Stmt::CreateMaterializedView { .. }
            | ast::Stmt::CreateVirtualTable(..)
            | ast::Stmt::CreateType { .. }
            | ast::Stmt::Delete { .. }
            | ast::Stmt::DropIndex { .. }
            | ast::Stmt::DropTable { .. }
            | ast::Stmt::DropType { .. }
            | ast::Stmt::DropView { .. }
            | ast::Stmt::Reindex { .. }
            | ast::Stmt::Optimize { .. }
            | ast::Stmt::Update { .. }
            | ast::Stmt::Insert { .. }
    );

    if is_write && connection.get_query_only() {
        bail_parse_error!("Cannot execute write statement in query_only mode")
    }

    let is_select = matches!(stmt, ast::Stmt::Select { .. });

    match stmt {
        ast::Stmt::AlterTable(ref alter) => {
            let tbl_name = alter.name.name.as_str();
            let db_name = alter.name.db_name.as_ref().map(|n| n.as_str());
            check_auth(
                connection,
                action::SQLITE_ALTER_TABLE,
                db_name.or(Some("main")),
                Some(tbl_name),
                db_name.or(Some("main")),
                None,
            )?;
            let ast::Stmt::AlterTable(alter) = stmt else {
                unreachable!()
            };
            translate_alter_table(alter, resolver, program, connection, input)?;
        }
        ast::Stmt::Analyze { ref name } => {
            let tbl = name.as_ref().map(|n| n.name.as_str());
            check_auth(
                connection,
                action::SQLITE_ANALYZE,
                tbl,
                None,
                Some("main"),
                None,
            )?;
            let ast::Stmt::Analyze { name } = stmt else {
                unreachable!()
            };
            translate_analyze(name, resolver, program)?
        }
        ast::Stmt::Attach {
            ref expr,
            ref db_name,
            ref key,
        } => {
            check_auth(connection, action::SQLITE_ATTACH, None, None, None, None)?;
            attach::translate_attach(expr, resolver, db_name, key, program, connection.clone())?;
        }
        ast::Stmt::Begin { typ, name } => {
            check_auth(
                connection,
                action::SQLITE_TRANSACTION,
                Some("BEGIN"),
                None,
                None,
                None,
            )?;
            translate_tx_begin(typ, name, resolver.schema(), program)?
        }
        ast::Stmt::Commit { name } => {
            check_auth(
                connection,
                action::SQLITE_TRANSACTION,
                Some("COMMIT"),
                None,
                None,
                None,
            )?;
            translate_tx_commit(name, resolver.schema(), resolver, program)?
        }
        ast::Stmt::CreateIndex { .. } => {
            // Auth check happens inside translate_create_index which has access to the index details
            check_auth(
                connection,
                action::SQLITE_CREATE_INDEX,
                None,
                None,
                Some("main"),
                None,
            )?;
            translate_create_index(program, connection, resolver, stmt)?;
        }
        ast::Stmt::CreateTable {
            temporary,
            ref tbl_name,
            ..
        } => {
            let action_code = if temporary {
                action::SQLITE_CREATE_TEMP_TABLE
            } else {
                action::SQLITE_CREATE_TABLE
            };
            let db_name = tbl_name.db_name.as_ref().map(|n| n.as_str());
            check_auth(
                connection,
                action_code,
                Some(tbl_name.name.as_str()),
                None,
                db_name.or(Some("main")),
                None,
            )?;
            let ast::Stmt::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } = stmt
            else {
                unreachable!()
            };
            translate_create_table(
                tbl_name,
                resolver,
                temporary,
                if_not_exists,
                body,
                program,
                connection,
            )?
        }
        ast::Stmt::CreateTrigger {
            temporary,
            if_not_exists,
            ref trigger_name,
            time,
            ref event,
            ref tbl_name,
            for_each_row,
            ref when_clause,
            ref commands,
        } => {
            let action_code = if temporary {
                action::SQLITE_CREATE_TEMP_TRIGGER
            } else {
                action::SQLITE_CREATE_TRIGGER
            };
            check_auth(
                connection,
                action_code,
                Some(trigger_name.name.as_str()),
                Some(tbl_name.name.as_str()),
                Some("main"),
                None,
            )?;
            // Reconstruct SQL for storage
            let sql = trigger::create_trigger_to_sql(
                temporary,
                if_not_exists,
                trigger_name,
                time,
                event,
                tbl_name,
                for_each_row,
                when_clause.as_deref(),
                commands,
            );
            let ast::Stmt::CreateTrigger {
                trigger_name,
                tbl_name,
                ..
            } = stmt
            else {
                unreachable!()
            };
            trigger::translate_create_trigger(
                trigger_name,
                resolver,
                temporary,
                if_not_exists,
                time,
                tbl_name,
                program,
                sql,
                connection.clone(),
            )?
        }
        ast::Stmt::CreateView {
            ref view_name,
            ref select,
            ref columns,
            ..
        } => {
            check_auth(
                connection,
                action::SQLITE_CREATE_VIEW,
                Some(view_name.name.as_str()),
                None,
                Some("main"),
                None,
            )?;
            view::translate_create_view(view_name, resolver, select, columns, program, connection)?
        }
        ast::Stmt::CreateMaterializedView {
            ref view_name,
            ref select,
            ..
        } => {
            check_auth(
                connection,
                action::SQLITE_CREATE_VIEW,
                Some(view_name.name.as_str()),
                None,
                Some("main"),
                None,
            )?;
            view::translate_create_materialized_view(
                view_name,
                resolver,
                select,
                connection.clone(),
                program,
            )?
        }
        ast::Stmt::CreateVirtualTable(ref vtab) => {
            check_auth(
                connection,
                action::SQLITE_CREATE_VTABLE,
                Some(vtab.tbl_name.name.as_str()),
                Some(vtab.module_name.as_str()),
                Some("main"),
                None,
            )?;
            let ast::Stmt::CreateVirtualTable(vtab) = stmt else {
                unreachable!()
            };
            translate_create_virtual_table(vtab, resolver, program, connection)?
        }
        ast::Stmt::Delete {
            ref tbl_name,
            where_clause,
            limit,
            returning,
            indexed,
            order_by,
            with,
        } => {
            check_auth(
                connection,
                action::SQLITE_DELETE,
                Some(tbl_name.name.as_str()),
                None,
                tbl_name
                    .db_name
                    .as_ref()
                    .map(|n| n.as_str())
                    .or(Some("main")),
                None,
            )?;
            if indexed.is_some_and(|i| matches!(i, Indexed::IndexedBy(_))) {
                bail_parse_error!("INDEXED BY clause is not supported in DELETE");
            }
            if !order_by.is_empty() {
                bail_parse_error!("ORDER BY clause is not supported in DELETE");
            }
            translate_delete(
                tbl_name,
                resolver,
                where_clause,
                limit,
                returning,
                with,
                program,
                connection,
            )?
        }
        ast::Stmt::Detach { ref name } => {
            let detach_name = format!("{name}");
            check_auth(
                connection,
                action::SQLITE_DETACH,
                Some(&detach_name),
                None,
                None,
                None,
            )?;
            let ast::Stmt::Detach { name } = stmt else {
                unreachable!()
            };
            attach::translate_detach(&name, resolver, program, connection.clone())?
        }
        ast::Stmt::DropIndex {
            if_exists,
            ref idx_name,
        } => {
            check_auth(
                connection,
                action::SQLITE_DROP_INDEX,
                Some(idx_name.name.as_str()),
                None,
                Some("main"),
                None,
            )?;
            translate_drop_index(idx_name, resolver, if_exists, program)?
        }
        ast::Stmt::DropTable { ref tbl_name, .. } => {
            let db_name = tbl_name.db_name.as_ref().map(|n| n.as_str());
            check_auth(
                connection,
                action::SQLITE_DROP_TABLE,
                Some(tbl_name.name.as_str()),
                None,
                db_name.or(Some("main")),
                None,
            )?;
            let ast::Stmt::DropTable {
                if_exists,
                tbl_name,
            } = stmt
            else {
                unreachable!()
            };
            translate_drop_table(tbl_name, resolver, if_exists, program, connection)?
        }
        ast::Stmt::DropTrigger {
            if_exists,
            ref trigger_name,
        } => {
            check_auth(
                connection,
                action::SQLITE_DROP_TRIGGER,
                Some(trigger_name.name.as_str()),
                None,
                Some("main"),
                None,
            )?;
            trigger::translate_drop_trigger(connection, resolver, trigger_name, if_exists, program)?
        }
        ast::Stmt::DropView {
            if_exists,
            ref view_name,
        } => {
            check_auth(
                connection,
                action::SQLITE_DROP_VIEW,
                Some(view_name.name.as_str()),
                None,
                Some("main"),
                None,
            )?;
            view::translate_drop_view(resolver, view_name, if_exists, program)?
        }
        ast::Stmt::CreateType {
            if_not_exists,
            ref type_name,
            ref body,
        } => {
            if !connection.experimental_custom_types_enabled() {
                bail_parse_error!("Custom types require --experimental-custom-types flag");
            }
            schema::translate_create_type(type_name, body, if_not_exists, resolver, program)?
        }
        ast::Stmt::DropType {
            if_exists,
            ref type_name,
        } => {
            if !connection.experimental_custom_types_enabled() {
                bail_parse_error!("Custom types require --experimental-custom-types flag");
            }
            schema::translate_drop_type(type_name, if_exists, resolver, program)?
        }
        ast::Stmt::Pragma { .. } => {
            bail_parse_error!("PRAGMA statement cannot be evaluated in a nested context")
        }
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Optimize { .. } => {
            let ast::Stmt::Optimize { idx_name } = stmt else {
                unreachable!()
            };
            translate_optimize(idx_name, resolver, program, connection)?
        }
        ast::Stmt::Release { name } => {
            check_auth(
                connection,
                action::SQLITE_SAVEPOINT,
                Some("RELEASE"),
                Some(name.as_str()),
                None,
                None,
            )?;
            translate_release(program, name)?
        }
        ast::Stmt::Rollback { .. } => {
            check_auth(
                connection,
                action::SQLITE_TRANSACTION,
                Some("ROLLBACK"),
                None,
                None,
                None,
            )?;
            let ast::Stmt::Rollback {
                tx_name,
                savepoint_name,
            } = stmt
            else {
                unreachable!()
            };
            translate_rollback(program, tx_name, savepoint_name)?
        }
        ast::Stmt::Savepoint { ref name } => {
            check_auth(
                connection,
                action::SQLITE_SAVEPOINT,
                Some("BEGIN"),
                Some(name.as_str()),
                None,
                None,
            )?;
            let ast::Stmt::Savepoint { name } = stmt else {
                unreachable!()
            };
            translate_savepoint(program, name)?
        }
        ast::Stmt::Select(select) => {
            check_auth(connection, action::SQLITE_SELECT, None, None, None, None)?;
            translate_select(
                select,
                resolver,
                program,
                plan::QueryDestination::ResultRows,
                connection,
            )?;
        }
        ast::Stmt::Update(ref update) => {
            check_auth(
                connection,
                action::SQLITE_UPDATE,
                Some(update.tbl_name.name.as_str()),
                None,
                update
                    .tbl_name
                    .db_name
                    .as_ref()
                    .map(|n| n.as_str())
                    .or(Some("main")),
                None,
            )?;
            let ast::Stmt::Update(update) = stmt else {
                unreachable!()
            };
            translate_update(update, resolver, program, connection)?
        }
        ast::Stmt::Vacuum { name, into } => {
            vacuum::translate_vacuum(program, name.as_ref(), into.as_deref())?
        }
        ast::Stmt::Insert { ref tbl_name, .. } => {
            check_auth(
                connection,
                action::SQLITE_INSERT,
                Some(tbl_name.name.as_str()),
                None,
                tbl_name
                    .db_name
                    .as_ref()
                    .map(|n| n.as_str())
                    .or(Some("main")),
                None,
            )?;
            let ast::Stmt::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } = stmt
            else {
                unreachable!()
            };
            translate_insert(
                resolver,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
                with,
                program,
                connection,
            )?
        }
    };

    // Indicate write operations so that in the epilogue we can emit the correct type of transaction
    if is_write {
        program.begin_write_operation();
    }

    // Indicate read operations so that in the epilogue we can emit the correct type of transaction
    if is_select && !program.table_references.is_empty() {
        program.begin_read_operation();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemoryIO;
    use crate::Database;

    /// Verify that REGEXP produces the correct error when no regexp function is registered.
    #[test]
    fn test_regexp_no_function_registered() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();
        let schema = db.schema.lock().clone();
        let pager = conn.pager.load().clone();

        // Use an empty SymbolTable so regexp() is not available.
        let empty_syms = SymbolTable::new();
        let mut parser = turso_parser::parser::Parser::new(b"SELECT 'x' REGEXP 'y'");
        let cmd = parser.next().unwrap().unwrap();
        let stmt = match cmd {
            ast::Cmd::Stmt(s) => s,
            _ => panic!("expected statement"),
        };

        let result = translate(
            &schema,
            stmt,
            pager,
            conn,
            &empty_syms,
            QueryMode::Normal,
            "",
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no such function: regexp"),
            "expected 'no such function: regexp', got: {err}"
        );
    }
}
