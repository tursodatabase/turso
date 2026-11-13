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
pub(crate) mod catalog;
pub mod collate;
mod compound_select;
pub(crate) mod delete;
pub(crate) mod display;
pub mod emitter;
pub mod eqp;
pub mod expr;
pub mod expr_context;
pub(crate) mod expression_index;
pub(crate) mod fkeys;
pub(crate) mod group_by;
pub mod index;
pub(crate) mod insert;
pub(crate) mod integrity_check;
pub(crate) mod main_loop;
pub mod optimizer;
pub(crate) mod order_by;
pub mod plan;
pub mod planner;
pub(crate) mod pragma;
pub(crate) mod recursive_cte;
pub(crate) mod result_column_types;
pub(crate) mod result_row;
pub mod roles;
pub(crate) mod rollback;
pub mod row_security;
pub(crate) mod schema;
pub(crate) mod select;
pub(crate) mod sequence;
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
use crate::translate::expr::WalkControl;
use crate::util::walk_stmt_expressions;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::Program;
use crate::{bail_parse_error, Connection, Result, SymbolTable};
use alter::translate_alter_table;
use analyze::translate_analyze;
use index::{translate_create_index, translate_drop_index, translate_optimize, translate_reindex};
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
#[allow(clippy::too_many_arguments)]
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    pager: Arc<Pager>,
    connection: Arc<Connection>,
    syms: &SymbolTable,
    query_mode: QueryMode,
    input: &str,
    origin: crate::statement::StatementOrigin,
) -> Result<Program> {
    tracing::trace!("querying {}", input);
    let change_cnt_on = matches!(
        stmt,
        ast::Stmt::Delete { .. } | ast::Stmt::Insert { .. } | ast::Stmt::Update { .. }
    );

    let capture_data_changes_info = if connection.is_mvcc_bootstrap_connection() {
        None
    } else {
        connection.get_capture_data_changes_info().clone()
    };
    // Boxed so the ~800 B builder sits on the heap instead of the prepare frame.
    let mut program = Box::new(ProgramBuilder::new(
        query_mode,
        capture_data_changes_info,
        // These options will be extended whithin each translate program
        ProgramBuilderOpts::new(1, 32, 2),
    ));
    program.set_mvcc_enabled(connection.mvcc_enabled());
    program.flags.set_reads_stored_values(matches!(
        origin,
        crate::statement::StatementOrigin::InternalHelper
    ));
    program
        .flags
        .set_writes_stored_values(connection.writes_stored_values());

    program.prologue();
    // Engine-generated statements, helpers and reads of the engine's own
    // metadata, are always SQLite text and must resolve functions with
    // SQLite semantics regardless of the database's dialect — the same
    // invariant as unmarked schema rows. They look names up in every
    // database, like SQLite does, whatever the connection's search path
    // says.
    let is_engine_statement = matches!(
        origin,
        crate::statement::StatementOrigin::InternalHelper
            | crate::statement::StatementOrigin::EngineMetadataRead
            | crate::statement::StatementOrigin::EngineSchemaWrite
    );
    let search_path = if is_engine_statement {
        None
    } else {
        connection.search_path()
    };
    let qualifiers = connection.qualifiers(schema);
    // Only a statement of the connection's user is limited by row-level
    // security. The engine's own statements read every row.
    let row_security_role = if matches!(origin, crate::statement::StatementOrigin::Root) {
        connection.current_role()
    } else {
        crate::security::roles::RoleId::SUPERUSER
    };
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &qualifiers,
        syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        if is_engine_statement {
            Arc::new(crate::dialect::DefaultDialect::new(connection.dialect()))
                as Arc<dyn crate::dialect::Dialect>
        } else {
            connection.dialect()
        },
        search_path.as_deref(),
        row_security_role,
    );
    resolver.automatic_index = connection.automatic_index();
    #[cfg(feature = "simulator")]
    resolver.set_subquery_unnesting_mode(connection.subquery_unnesting_mode());

    walk_stmt_expressions(&stmt, &mut |expr| {
        if let ast::Expr::Variable(variable) = expr {
            program.register_variable(variable);
        }
        Ok(WalkControl::Continue)
    })?;

    let check_privileges = matches!(origin, crate::statement::StatementOrigin::Root);
    if check_privileges
        && !resolver
            .catalog
            .dialect
            .role_is_superuser(resolver.schema(), connection.current_role())
    {
        resolver.role_is_not_superuser = true;
        roles::check_statement_privileges(&stmt, &resolver, &connection)?;
    }

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
    // Subqueries are numbered per program, and a trigger body or foreign-key
    // action is compiled into a program of its own with the resolver of the
    // statement that fires it. So the same number can name a subquery of
    // each, and the statement's row-value and IN subqueries are set aside while
    // another statement is compiled.
    let callers_row_value_subqueries = std::mem::take(&mut resolver.row_value_subqueries);
    let callers_in_subquery_column_types = std::mem::take(&mut resolver.in_subquery_column_types);
    let result = translate_statement(stmt, resolver, program, connection, input);
    resolver.row_value_subqueries = callers_row_value_subqueries;
    resolver.in_subquery_column_types = callers_in_subquery_column_types;
    result
}

fn translate_statement(
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
            | ast::Stmt::CreateRole { .. }
            | ast::Stmt::Grant(_)
            | ast::Stmt::Delete { .. }
            | ast::Stmt::DropIndex { .. }
            | ast::Stmt::DropTable { .. }
            | ast::Stmt::DropTrigger { .. }
            | ast::Stmt::DropType { .. }
            | ast::Stmt::DropDomain { .. }
            | ast::Stmt::DropView { .. }
            | ast::Stmt::Optimize { .. }
            | ast::Stmt::Update { .. }
            | ast::Stmt::Insert { .. }
            | ast::Stmt::CreateSequence { .. }
            | ast::Stmt::DropSequence { .. }
            | ast::Stmt::AlterSequenceOwnedBy { .. }
            | ast::Stmt::CreateSchema { .. }
            | ast::Stmt::DropSchema { .. }
            | ast::Stmt::CreatePolicy(_)
            | ast::Stmt::DropPolicy { .. }
    );
    let is_vacuum = matches!(stmt, ast::Stmt::Vacuum { .. });

    if is_vacuum && connection.get_query_only() {
        bail_parse_error!("Cannot execute VACUUM in query_only mode")
    }

    if is_write && connection.get_query_only() {
        bail_parse_error!("Cannot execute write statement in query_only mode")
    }

    let is_select = matches!(stmt, ast::Stmt::Select { .. });
    let is_dml = matches!(
        stmt,
        ast::Stmt::Delete { .. } | ast::Stmt::Insert { .. } | ast::Stmt::Update { .. }
    );

    // PRAGMA count_changes: top-level INSERT/UPDATE/DELETE statements return one row
    // with the number of rows they changed. Trigger bodies, foreign key actions and
    // engine-internal nested statements never return count rows, matching SQLite.
    let count_changes_column = if connection.get_count_changes()
        && !connection.is_nested_stmt()
        && !program.flags.is_subprogram()
    {
        match &stmt {
            ast::Stmt::Insert { .. } => Some("rows inserted"),
            ast::Stmt::Update { .. } => Some("rows updated"),
            ast::Stmt::Delete { .. } => Some("rows deleted"),
            _ => None,
        }
    } else {
        None
    };

    match stmt {
        ast::Stmt::AlterTable(alter) => {
            translate_alter_table(alter, resolver, program, connection, input)?;
        }
        ast::Stmt::Analyze { name } => translate_analyze(name, resolver, program)?,
        ast::Stmt::Attach { expr, db_name, key } => {
            crate::attach::translate_attach(
                &expr,
                resolver,
                &db_name,
                &key,
                program,
                connection.clone(),
            )?;
        }
        ast::Stmt::Begin {
            typ,
            name,
            access_mode,
        } => translate_tx_begin(typ, name, access_mode, resolver, program)?,
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
            input,
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
        } => trigger::translate_create_trigger(
            trigger_name,
            resolver,
            temporary,
            if_not_exists,
            time,
            &event,
            tbl_name,
            for_each_row,
            connection,
            program,
            &commands,
            when_clause.as_deref(),
        )?,
        ast::Stmt::CreateView {
            view_name,
            select,
            columns,
            if_not_exists,
            temporary,
        } => view::translate_create_view(
            &view_name,
            resolver,
            &select,
            &columns,
            temporary,
            if_not_exists,
            connection,
            program,
        )?,
        ast::Stmt::CreateMaterializedView {
            view_name,
            select,
            columns,
            if_not_exists,
        } => view::translate_create_materialized_view(
            &view_name,
            resolver,
            &select,
            &columns,
            if_not_exists,
            connection.clone(),
            program,
        )?,
        ast::Stmt::CreateVirtualTable(vtab) => {
            translate_create_virtual_table(vtab, resolver, program, connection)?
        }
        ast::Stmt::Delete {
            tbl_name,
            where_clause,
            returning,
            indexed,
            with,
            truncate,
        } => {
            if where_clause.is_none() && connection.get_dml_require_where() {
                bail_parse_error!(
                    "DELETE without a WHERE clause is not allowed when require_where (or i_am_a_dummy) is enabled"
                );
            }
            translate_delete(
                &tbl_name,
                resolver,
                where_clause,
                returning,
                indexed,
                with,
                truncate,
                program,
                connection,
            )?
        }
        ast::Stmt::Detach { name } => {
            crate::attach::translate_detach(&name, resolver, program, connection.clone())?
        }
        ast::Stmt::DropIndex {
            if_exists,
            idx_name,
        } => translate_drop_index(&idx_name, resolver, if_exists, connection, program)?,
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
        } => view::translate_drop_view(resolver, &view_name, if_exists, connection, program)?,
        ast::Stmt::CreateRole { role_name, options } => {
            roles::translate_create_role(role_name, options, resolver, program, connection)?
        }
        ast::Stmt::Grant(grant) => roles::translate_grant(&grant, resolver, program, connection)?,
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
        ast::Stmt::Reindex { name } => translate_reindex(name, resolver, program, connection)?,
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
        ast::Stmt::Vacuum { name, into } => vacuum::translate_vacuum(
            program,
            resolver,
            name.as_ref(),
            into.as_deref(),
            connection.clone(),
        )?,
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
        ast::Stmt::CreateSequence {
            if_not_exists,
            seq_name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
        } => {
            sequence::translate_create_sequence(
                &seq_name,
                if_not_exists,
                &start,
                &increment,
                &min_value,
                &max_value,
                cycle,
                resolver,
                connection,
                program,
            )?;
        }
        ast::Stmt::DropSequence {
            if_exists,
            seq_name,
        } => {
            sequence::translate_drop_sequence(&seq_name, if_exists, resolver, connection, program)?;
        }
        ast::Stmt::AlterSequenceOwnedBy { seq_name, owner } => {
            sequence::translate_alter_sequence_owned_by(
                &seq_name,
                owner.as_ref(),
                resolver,
                connection,
                program,
            )?;
        }
        ast::Stmt::CreateSchema { name } => {
            schema::translate_create_schema(&name, resolver, program)?;
        }
        ast::Stmt::DropSchema { name } => {
            schema::translate_drop_schema(&name, resolver, program)?;
        }
        ast::Stmt::CreatePolicy(policy) => {
            row_security::translate_create_policy(&policy, resolver, program, connection)?;
        }
        ast::Stmt::DropPolicy {
            if_exists,
            policy_name,
            tbl_name,
        } => {
            row_security::translate_drop_policy(
                &policy_name,
                &tbl_name,
                if_exists,
                resolver,
                program,
                connection,
            )?;
        }
    };

    if is_write {
        if is_dml {
            // DML translators register their actual write database. Keep a
            // main read transaction to coordinate connection-level autocommit,
            // without upgrading an unrelated main snapshot for attached-only
            // writes.
            program.begin_read_operation()?;
        } else {
            program.begin_write_operation()?;
        }
    }

    // Indicate read operations so that in the epilogue we can emit the correct type of transaction
    if is_select && !program.table_references.is_empty() {
        program.begin_read_operation()?;
    }

    // A statement with RETURNING already produces rows, so it never also returns
    // a count row (same as SQLite).
    if let Some(column_name) = count_changes_column {
        if program.result_columns.is_empty() {
            let reg = program.alloc_register();
            program.emit_insn(crate::vdbe::insn::Insn::ChangeCount { dest: reg });
            program.emit_result_row(reg, 1);
            program.add_pragma_result_column(column_name.to_string());
        }
    }

    if connection.experimental_custom_types_enabled() {
        program.result_column_types = result_column_types::result_column_types(
            &program.result_columns,
            &program.table_references,
            resolver,
        );
    }

    // After translation so that name resolution errors come first, as in
    // PostgreSQL, which rejects read-only violations when the command starts
    // executing.
    if is_write
        && connection.is_tx_read_only()
        && matches!(program.get_query_mode(), QueryMode::Normal)
    {
        return Err(crate::LimboError::ReadOnly);
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
        ast::Stmt::CreateRole { .. } => "create_role",
        ast::Stmt::Grant(_) => "grant",
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
        ast::Stmt::CreateSequence { .. } => "create_sequence",
        ast::Stmt::DropSequence { .. } => "drop_sequence",
        ast::Stmt::AlterSequenceOwnedBy { .. } => "alter_sequence_owned_by",
        ast::Stmt::CreateSchema { .. } => "create_schema",
        ast::Stmt::DropSchema { .. } => "drop_schema",
        ast::Stmt::CreatePolicy(_) => "create_policy",
        ast::Stmt::DropPolicy { .. } => "drop_policy",
    }
}
