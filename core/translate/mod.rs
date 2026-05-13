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
pub(crate) mod stmt_journal;
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

use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::sync::Arc;
use crate::translate::delete::translate_delete;
use crate::translate::emitter::Resolver;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::Program;
use crate::{bail_parse_error, Connection, Result, SymbolTable};
use alter::translate_alter_table;
use analyze::translate_analyze;
use index::{translate_create_index, translate_drop_index, translate_optimize};
use insert::translate_insert;
use rollback::{translate_release, translate_rollback, translate_savepoint};
use schema::{translate_create_table, translate_create_virtual_table, translate_drop_table};
use select::translate_select;
use tracing::{instrument, Level};
use transaction::{translate_tx_begin, translate_tx_commit};
use turso_parser::ast;
use update::translate_update;

#[instrument(skip_all, level = Level::DEBUG)]
#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
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
    validate_stmt_expr_depth(&stmt)?;

    let change_cnt_on = matches!(
        stmt,
        ast::Stmt::CreateIndex { .. }
            | ast::Stmt::Delete { .. }
            | ast::Stmt::Insert { .. }
            | ast::Stmt::Update { .. }
    );

    // Boxed so the ~800 B builder sits on the heap instead of the prepare frame.
    let mut program = Box::new(ProgramBuilder::new(
        query_mode,
        connection.get_capture_data_changes_info().clone(),
        // These options will be extended whithin each translate program
        ProgramBuilderOpts::new(1, 32, 2),
    ));

    program.prologue();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
    );

    match stmt {
        // There can be no nesting with pragma, so lift it up here
        ast::Stmt::Pragma { name, body } => {
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

fn validate_stmt_expr_depth(stmt: &ast::Stmt) -> Result<()> {
    match stmt {
        ast::Stmt::AlterTable(alter) => validate_alter_table_expr_depth(alter),
        ast::Stmt::Attach { expr, db_name, key } => {
            expr::validate_expr_depth(expr)?;
            expr::validate_expr_depth(db_name)?;
            if let Some(key) = key.as_deref() {
                expr::validate_expr_depth(key)?;
            }
            Ok(())
        }
        ast::Stmt::CreateIndex {
            columns,
            with_clause,
            where_clause,
            ..
        } => {
            expr::validate_sorted_columns_expr_depth(columns)?;
            for (_, expr) in with_clause {
                expr::validate_expr_depth(expr)?;
            }
            if let Some(where_clause) = where_clause.as_deref() {
                expr::validate_expr_depth(where_clause)?;
            }
            Ok(())
        }
        ast::Stmt::CreateTable { body, .. } => validate_create_table_body_expr_depth(body),
        ast::Stmt::CreateTrigger {
            when_clause,
            commands,
            ..
        } => {
            if let Some(when_clause) = when_clause.as_deref() {
                expr::validate_expr_depth(when_clause)?;
            }
            for command in commands {
                validate_trigger_command_expr_depth(command)?;
            }
            Ok(())
        }
        ast::Stmt::CreateView { select, .. }
        | ast::Stmt::CreateMaterializedView { select, .. }
        | ast::Stmt::Select(select) => expr::validate_select_expr_depth(select),
        ast::Stmt::CreateType { body, .. } => validate_create_type_body_expr_depth(body),
        ast::Stmt::CreateDomain {
            default,
            constraints,
            ..
        } => {
            if let Some(default) = default.as_deref() {
                expr::validate_expr_depth(default)?;
            }
            for constraint in constraints {
                expr::validate_expr_depth(&constraint.check)?;
            }
            Ok(())
        }
        ast::Stmt::Delete {
            with,
            where_clause,
            returning,
            order_by,
            limit,
            ..
        } => {
            validate_with_expr_depth(with.as_ref())?;
            if let Some(where_clause) = where_clause.as_deref() {
                expr::validate_expr_depth(where_clause)?;
            }
            expr::validate_result_columns_expr_depth(returning)?;
            expr::validate_sorted_columns_expr_depth(order_by)?;
            if let Some(limit) = limit {
                expr::validate_limit_expr_depth(limit)?;
            }
            Ok(())
        }
        ast::Stmt::Detach { name } => expr::validate_expr_depth(name),
        ast::Stmt::Insert {
            with,
            body,
            returning,
            ..
        } => {
            validate_with_expr_depth(with.as_ref())?;
            validate_insert_body_expr_depth(body)?;
            expr::validate_result_columns_expr_depth(returning)
        }
        ast::Stmt::Pragma { body, .. } => {
            if let Some(body) = body {
                validate_pragma_body_expr_depth(body)?;
            }
            Ok(())
        }
        ast::Stmt::Update(update) => validate_update_expr_depth(update),
        ast::Stmt::Vacuum { into, .. } => {
            if let Some(into) = into.as_deref() {
                expr::validate_expr_depth(into)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_alter_table_expr_depth(alter: &ast::AlterTable) -> Result<()> {
    match &alter.body {
        ast::AlterTableBody::AddColumn(column)
        | ast::AlterTableBody::AlterColumn { new: column, .. } => {
            validate_column_definition_expr_depth(column)
        }
        ast::AlterTableBody::RenameTo(_)
        | ast::AlterTableBody::RenameColumn { .. }
        | ast::AlterTableBody::DropColumn(_) => Ok(()),
    }
}

fn validate_create_table_body_expr_depth(body: &ast::CreateTableBody) -> Result<()> {
    match body {
        ast::CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            ..
        } => {
            for column in columns {
                validate_column_definition_expr_depth(column)?;
            }
            for constraint in constraints {
                validate_table_constraint_expr_depth(&constraint.constraint)?;
            }
            Ok(())
        }
        ast::CreateTableBody::AsSelect(select) => expr::validate_select_expr_depth(select),
    }
}

fn validate_column_definition_expr_depth(column: &ast::ColumnDefinition) -> Result<()> {
    if let Some(ty) = &column.col_type {
        validate_type_expr_depth(ty)?;
    }
    for constraint in &column.constraints {
        validate_column_constraint_expr_depth(&constraint.constraint)?;
    }
    Ok(())
}

fn validate_column_constraint_expr_depth(constraint: &ast::ColumnConstraint) -> Result<()> {
    match constraint {
        ast::ColumnConstraint::Check(expr)
        | ast::ColumnConstraint::Default(expr)
        | ast::ColumnConstraint::Generated { expr, .. } => expr::validate_expr_depth(expr),
        ast::ColumnConstraint::PrimaryKey { .. }
        | ast::ColumnConstraint::NotNull { .. }
        | ast::ColumnConstraint::Unique(_)
        | ast::ColumnConstraint::Collate { .. }
        | ast::ColumnConstraint::ForeignKey { .. } => Ok(()),
    }
}

fn validate_table_constraint_expr_depth(constraint: &ast::TableConstraint) -> Result<()> {
    match constraint {
        ast::TableConstraint::PrimaryKey { columns, .. }
        | ast::TableConstraint::Unique { columns, .. } => {
            expr::validate_sorted_columns_expr_depth(columns)
        }
        ast::TableConstraint::Check(expr) => expr::validate_expr_depth(expr),
        ast::TableConstraint::ForeignKey { .. } => Ok(()),
    }
}

fn validate_create_type_body_expr_depth(body: &ast::CreateTypeBody) -> Result<()> {
    match body {
        ast::CreateTypeBody::CustomType {
            encode,
            decode,
            default,
            ..
        } => {
            for custom_expr in [encode.as_deref(), decode.as_deref(), default.as_deref()]
                .into_iter()
                .flatten()
            {
                expr::validate_expr_depth(custom_expr)?;
            }
            Ok(())
        }
        ast::CreateTypeBody::Struct(fields) | ast::CreateTypeBody::Union(fields) => {
            for field in fields {
                validate_type_expr_depth(&field.field_type)?;
            }
            Ok(())
        }
    }
}

fn validate_type_expr_depth(ty: &ast::Type) -> Result<()> {
    match &ty.size {
        Some(ast::TypeSize::MaxSize(expr)) => expr::validate_expr_depth(expr),
        Some(ast::TypeSize::TypeSize(lhs, rhs)) => {
            expr::validate_expr_depth(lhs)?;
            expr::validate_expr_depth(rhs)
        }
        None => Ok(()),
    }
}

fn validate_insert_body_expr_depth(body: &ast::InsertBody) -> Result<()> {
    match body {
        ast::InsertBody::Select(select, upsert) => {
            expr::validate_select_expr_depth(select)?;
            if let Some(upsert) = upsert.as_deref() {
                validate_upsert_expr_depth(upsert)?;
            }
            Ok(())
        }
        ast::InsertBody::DefaultValues => Ok(()),
    }
}

fn validate_update_expr_depth(update: &ast::Update) -> Result<()> {
    validate_with_expr_depth(update.with.as_ref())?;
    for set in &update.sets {
        expr::validate_expr_depth(&set.expr)?;
    }
    if let Some(from) = &update.from {
        expr::validate_from_clause_expr_depth(from, &mut Vec::new())?;
    }
    if let Some(where_clause) = update.where_clause.as_deref() {
        expr::validate_expr_depth(where_clause)?;
    }
    expr::validate_result_columns_expr_depth(&update.returning)?;
    expr::validate_sorted_columns_expr_depth(&update.order_by)?;
    if let Some(limit) = &update.limit {
        expr::validate_limit_expr_depth(limit)?;
    }
    Ok(())
}

fn validate_with_expr_depth(with: Option<&ast::With>) -> Result<()> {
    if let Some(with) = with {
        for cte in &with.ctes {
            expr::validate_select_expr_depth(&cte.select)?;
        }
    }
    Ok(())
}

fn validate_upsert_expr_depth(upsert: &ast::Upsert) -> Result<()> {
    let mut upsert = Some(upsert);
    while let Some(current) = upsert {
        if let Some(index) = &current.index {
            expr::validate_sorted_columns_expr_depth(&index.targets)?;
            if let Some(where_clause) = index.where_clause.as_deref() {
                expr::validate_expr_depth(where_clause)?;
            }
        }
        match &current.do_clause {
            ast::UpsertDo::Set { sets, where_clause } => {
                for set in sets {
                    expr::validate_expr_depth(&set.expr)?;
                }
                if let Some(where_clause) = where_clause.as_deref() {
                    expr::validate_expr_depth(where_clause)?;
                }
            }
            ast::UpsertDo::Nothing => {}
        }
        upsert = current.next.as_deref();
    }
    Ok(())
}

fn validate_pragma_body_expr_depth(body: &ast::PragmaBody) -> Result<()> {
    match body {
        ast::PragmaBody::Equals(expr) | ast::PragmaBody::Call(expr) => {
            expr::validate_expr_depth(expr)
        }
    }
}

fn validate_trigger_command_expr_depth(command: &ast::TriggerCmd) -> Result<()> {
    match command {
        ast::TriggerCmd::Update {
            sets,
            from,
            where_clause,
            ..
        } => {
            for set in sets {
                expr::validate_expr_depth(&set.expr)?;
            }
            if let Some(from) = from {
                expr::validate_from_clause_expr_depth(from, &mut Vec::new())?;
            }
            if let Some(where_clause) = where_clause.as_deref() {
                expr::validate_expr_depth(where_clause)?;
            }
            Ok(())
        }
        ast::TriggerCmd::Insert {
            select,
            upsert,
            returning,
            ..
        } => {
            expr::validate_select_expr_depth(select)?;
            if let Some(upsert) = upsert.as_deref() {
                validate_upsert_expr_depth(upsert)?;
            }
            expr::validate_result_columns_expr_depth(returning)
        }
        ast::TriggerCmd::Delete { where_clause, .. } => {
            if let Some(where_clause) = where_clause.as_deref() {
                expr::validate_expr_depth(where_clause)?;
            }
            Ok(())
        }
        ast::TriggerCmd::Select(select) => expr::validate_select_expr_depth(select),
    }
}

// TODO: for now leaving the return value as a Program. But ideally to support nested parsing of arbitraty
// statements, we would have to return a program builder instead
/// Translate SQL statement into bytecode program.
#[turso_macros::trace_stack(detail = stmt_kind(&stmt))]
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
            | ast::Stmt::CreateDomain { .. }
            | ast::Stmt::Delete { .. }
            | ast::Stmt::DropIndex { .. }
            | ast::Stmt::DropTable { .. }
            | ast::Stmt::DropType { .. }
            | ast::Stmt::DropDomain { .. }
            | ast::Stmt::DropView { .. }
            | ast::Stmt::Reindex { .. }
            | ast::Stmt::Optimize { .. }
            | ast::Stmt::Update { .. }
            | ast::Stmt::Insert { .. }
    );
    let is_vacuum = matches!(stmt, ast::Stmt::Vacuum { .. });

    if is_vacuum && connection.get_query_only() {
        bail_parse_error!("Cannot execute VACUUM in query_only mode")
    }

    if is_write && connection.get_query_only() {
        bail_parse_error!("Cannot execute write statement in query_only mode")
    }

    let is_select = matches!(stmt, ast::Stmt::Select { .. });

    match stmt {
        ast::Stmt::AlterTable(alter) => {
            translate_alter_table(alter, resolver, program, connection, input)?;
        }
        ast::Stmt::Analyze { name } => translate_analyze(name, resolver, program)?,
        ast::Stmt::Attach { expr, db_name, key } => {
            attach::translate_attach(&expr, resolver, &db_name, &key, program, connection.clone())?;
        }
        ast::Stmt::Begin { typ, name } => translate_tx_begin(typ, name, resolver, program)?,
        ast::Stmt::Commit { name } => {
            translate_tx_commit(name, resolver.schema(), resolver, program)?
        }
        ast::Stmt::CreateIndex { .. } => {
            translate_create_index(program, connection, resolver, stmt)?;
        }
        ast::Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => translate_create_table(
            tbl_name,
            resolver,
            temporary,
            if_not_exists,
            body,
            program,
            connection,
        )?,
        ast::Stmt::CreateTrigger {
            temporary,
            if_not_exists,
            trigger_name,
            time,
            event,
            tbl_name,
            for_each_row,
            when_clause,
            commands,
        } => {
            // Reconstruct SQL for storage
            let sql = trigger::create_trigger_to_sql(
                temporary,
                if_not_exists,
                &trigger_name,
                time,
                &event,
                &tbl_name,
                for_each_row,
                when_clause.as_deref(),
                &commands,
            );
            trigger::translate_create_trigger(
                trigger_name,
                resolver,
                temporary,
                if_not_exists,
                time,
                tbl_name,
                program,
                sql,
                &commands,
                when_clause.as_deref(),
            )?
        }
        ast::Stmt::CreateView {
            view_name,
            select,
            columns,
            ..
        } => view::translate_create_view(&view_name, resolver, &select, &columns, program)?,
        ast::Stmt::CreateMaterializedView {
            view_name, select, ..
        } => view::translate_create_materialized_view(
            &view_name,
            resolver,
            &select,
            connection.clone(),
            program,
        )?,
        ast::Stmt::CreateVirtualTable(vtab) => {
            translate_create_virtual_table(vtab, resolver, program, connection)?
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
            if !order_by.is_empty() {
                bail_parse_error!("ORDER BY clause is not supported in DELETE");
            }
            if where_clause.is_none() && connection.get_dml_require_where() {
                bail_parse_error!(
                    "DELETE without a WHERE clause is not allowed when require_where (or i_am_a_dummy) is enabled"
                );
            }
            translate_delete(
                &tbl_name,
                resolver,
                where_clause,
                limit,
                returning,
                indexed,
                with,
                program,
                connection,
            )?
        }
        ast::Stmt::Detach { name } => {
            attach::translate_detach(&name, resolver, program, connection.clone())?
        }
        ast::Stmt::DropIndex {
            if_exists,
            idx_name,
        } => translate_drop_index(&idx_name, resolver, if_exists, program)?,
        ast::Stmt::DropTable {
            if_exists,
            tbl_name,
        } => translate_drop_table(tbl_name, resolver, if_exists, program, connection)?,
        ast::Stmt::DropTrigger {
            if_exists,
            trigger_name,
        } => trigger::translate_drop_trigger(resolver, &trigger_name, if_exists, program)?,
        ast::Stmt::DropView {
            if_exists,
            view_name,
        } => view::translate_drop_view(resolver, &view_name, if_exists, program)?,
        ast::Stmt::CreateType {
            if_not_exists,
            type_name,
            body,
        } => {
            if !connection.experimental_custom_types_enabled() {
                bail_parse_error!("Custom types require --experimental-custom-types flag");
            }
            schema::translate_create_type(&type_name, &body, if_not_exists, resolver, program)?
        }
        ast::Stmt::CreateDomain {
            if_not_exists,
            domain_name,
            base_type,
            default,
            not_null,
            constraints,
        } => {
            if !connection.experimental_custom_types_enabled() {
                bail_parse_error!("Custom types require --experimental-custom-types flag");
            }
            schema::translate_create_domain(
                &domain_name,
                &base_type,
                not_null,
                &constraints,
                default,
                if_not_exists,
                resolver,
                program,
            )?
        }
        ast::Stmt::DropType {
            if_exists,
            type_name,
        } => {
            if !connection.experimental_custom_types_enabled() {
                bail_parse_error!("Custom types require --experimental-custom-types flag");
            }
            schema::translate_drop_type(&type_name, if_exists, false, resolver, program)?
        }
        ast::Stmt::DropDomain {
            if_exists,
            domain_name,
        } => {
            if !connection.experimental_custom_types_enabled() {
                bail_parse_error!("Custom types require --experimental-custom-types flag");
            }
            schema::translate_drop_type(&domain_name, if_exists, true, resolver, program)?
        }
        ast::Stmt::Pragma { .. } => {
            bail_parse_error!("PRAGMA statement cannot be evaluated in a nested context")
        }
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Optimize { idx_name } => {
            translate_optimize(idx_name, resolver, program, connection)?
        }
        ast::Stmt::Release { name } => translate_release(program, name)?,
        ast::Stmt::Rollback {
            tx_name,
            savepoint_name,
        } => translate_rollback(program, tx_name, savepoint_name)?,
        ast::Stmt::Savepoint { name } => translate_savepoint(program, name)?,
        ast::Stmt::Select(select) => {
            translate_select(
                select,
                resolver,
                program,
                plan::QueryDestination::ResultRows,
                connection,
            )?;
        }
        ast::Stmt::Update(update) => {
            if update.where_clause.is_none() && connection.get_dml_require_where() {
                bail_parse_error!(
                    "UPDATE without a WHERE clause is not allowed when require_where (or i_am_a_dummy) is enabled"
                );
            }
            translate_update(update, resolver, program, connection)?
        }
        ast::Stmt::Vacuum { name, into } => {
            vacuum::translate_vacuum(program, name.as_ref(), into.as_deref(), connection.clone())?
        }
        ast::Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        } => translate_insert(
            resolver,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
            with,
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

    Ok(())
}

fn stmt_kind(stmt: &ast::Stmt) -> &'static str {
    match stmt {
        ast::Stmt::AlterTable(_) => "alter_table",
        ast::Stmt::Analyze { .. } => "analyze",
        ast::Stmt::Attach { .. } => "attach",
        ast::Stmt::Begin { .. } => "begin",
        ast::Stmt::Commit { .. } => "commit",
        ast::Stmt::CreateIndex { .. } => "create_index",
        ast::Stmt::CreateTable { .. } => "create_table",
        ast::Stmt::CreateTrigger { .. } => "create_trigger",
        ast::Stmt::CreateView { .. } => "create_view",
        ast::Stmt::CreateMaterializedView { .. } => "create_materialized_view",
        ast::Stmt::CreateVirtualTable(_) => "create_virtual_table",
        ast::Stmt::CreateType { .. } => "create_type",
        ast::Stmt::CreateDomain { .. } => "create_domain",
        ast::Stmt::Delete { .. } => "delete",
        ast::Stmt::Detach { .. } => "detach",
        ast::Stmt::DropIndex { .. } => "drop_index",
        ast::Stmt::DropTable { .. } => "drop_table",
        ast::Stmt::DropType { .. } => "drop_type",
        ast::Stmt::DropDomain { .. } => "drop_domain",
        ast::Stmt::DropTrigger { .. } => "drop_trigger",
        ast::Stmt::DropView { .. } => "drop_view",
        ast::Stmt::Insert { .. } => "insert",
        ast::Stmt::Pragma { .. } => "pragma",
        ast::Stmt::Reindex { .. } => "reindex",
        ast::Stmt::Release { .. } => "release",
        ast::Stmt::Rollback { .. } => "rollback",
        ast::Stmt::Savepoint { .. } => "savepoint",
        ast::Stmt::Select { .. } => "select",
        ast::Stmt::Update { .. } => "update",
        ast::Stmt::Vacuum { .. } => "vacuum",
        ast::Stmt::Optimize { .. } => "optimize",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemoryIO;
    use crate::schema::{BTreeTable, Table, SQLITE_SEQUENCE_TABLE_NAME};
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

    #[test]
    fn test_insert_autoincrement_with_malformed_sqlite_sequence_is_corrupt() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
            .unwrap();

        let mut schema = db.schema.lock().as_ref().clone();
        let seq_root_page = schema
            .get_btree_table(SQLITE_SEQUENCE_TABLE_NAME)
            .expect("sqlite_sequence should exist after creating AUTOINCREMENT table")
            .root_page;
        let malformed_seq =
            BTreeTable::from_sql("CREATE TABLE sqlite_sequence(name)", seq_root_page)
                .expect("malformed sqlite_sequence SQL should parse");
        schema.tables.insert(
            SQLITE_SEQUENCE_TABLE_NAME.to_string(),
            Arc::new(Table::BTree(Arc::new(malformed_seq))),
        );

        let pager = conn.pager.load().clone();
        let syms = SymbolTable::new();

        let mut parser = turso_parser::parser::Parser::new(b"INSERT INTO t(v) VALUES('x')");
        let cmd = parser.next().unwrap().unwrap();
        let stmt = match cmd {
            ast::Cmd::Stmt(s) => s,
            _ => panic!("expected statement"),
        };

        let err = translate(&schema, stmt, pager, conn, &syms, QueryMode::Normal, "")
            .expect_err("translation should fail with malformed sqlite_sequence");
        match err {
            crate::LimboError::Corrupt(msg) => {
                assert!(
                    msg.contains("sqlite_sequence"),
                    "expected sqlite_sequence corruption error, got: {msg}"
                );
            }
            other => panic!("expected LimboError::Corrupt, got: {other}"),
        }
    }

    #[test]
    fn test_insert_autoincrement_with_missing_sqlite_sequence_is_corrupt() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
            .unwrap();

        let mut schema = db.schema.lock().as_ref().clone();
        schema.tables.remove(SQLITE_SEQUENCE_TABLE_NAME);

        let pager = conn.pager.load().clone();
        let syms = SymbolTable::new();

        let mut parser = turso_parser::parser::Parser::new(b"INSERT INTO t(v) VALUES('x')");
        let cmd = parser.next().unwrap().unwrap();
        let stmt = match cmd {
            ast::Cmd::Stmt(s) => s,
            _ => panic!("expected statement"),
        };

        let err = translate(&schema, stmt, pager, conn, &syms, QueryMode::Normal, "")
            .expect_err("translation should fail with missing sqlite_sequence");
        match err {
            crate::LimboError::Corrupt(msg) => {
                assert!(
                    msg.contains("missing sqlite_sequence"),
                    "expected missing sqlite_sequence error, got: {msg}"
                );
            }
            other => panic!("expected LimboError::Corrupt, got: {other}"),
        }
    }

    #[test]
    fn test_trigger_compile_error_does_not_poison_future_insert_compilation() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE ref(x);").unwrap();
        conn.execute("CREATE TABLE t(a INTEGER);").unwrap();
        conn.execute("CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT * FROM ref; END;")
            .unwrap();
        conn.execute("DROP TABLE ref;").unwrap();

        let err = conn
            .execute("INSERT INTO t VALUES (1);")
            .expect_err("single-row insert should fail while trigger references dropped table");
        assert!(
            err.to_string().contains("no such table: ref"),
            "expected missing-table error, got: {err}"
        );

        let err = conn.execute("INSERT INTO t VALUES (2), (3);").expect_err(
            "multi-row insert should still fail instead of skipping the poisoned trigger",
        );
        assert!(
            err.to_string().contains("no such table: ref"),
            "expected missing-table error, got: {err}"
        );
    }
}
