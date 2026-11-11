//! Row-level security limits which rows of a table a role may access.
//!
//! The statements that read and write rows call the hooks of this module,
//! which decide which rows of a table the role may access from whether the
//! table has row-level security, [`crate::Dialect::table_rls_enabled`], and
//! who owns it, and limit the statement to those rows. The role is the one
//! the statement is compiled for, [`Resolver::row_security_role`].
//!
//! Which tables have row-level security, and their policies, are the
//! dialect's schema state, like the owner of a table. The module compiles the
//! statements that change them into a [`RelationChange`] for the dialect to
//! record.

use std::sync::Arc;

use turso_parser::ast;

use crate::{
    bail_parse_error,
    dialect::RelationChange,
    function::Func,
    namespace::{NamespaceId, NamespaceRef},
    schema::{collect_column_dependencies_of_expr, BTreeTable, Column, Schema, Table},
    security::permission::{Action, Object, PermissionDenied},
    security::policy::{Command, Policy, RowPolicy},
    security::roles::{RoleCatalog, RoleId},
    translate::{
        emitter::{with_columns_in_registers, Resolver},
        expr::{
            bind_and_rewrite_expr, translate_expr, translate_expr_no_constant_opt, walk_expr,
            walk_expr_mut, BindingBehavior, NoConstantOptReason, WalkControl,
        },
        plan::{JoinInfo, JoinOrigin, JoinedTable, TableReferences, WhereTerm, WhereTermOrigin},
        planner::break_predicate_at_and_boundaries,
        roles,
        schema::emit_schema_cookie,
    },
    util::normalize_ident,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    vtab::{VirtualTable, VirtualTableType},
    Connection, Dialect, HashSet, LimboError, Result, MAIN_DB_ID,
};

/// Limits the rows of the tables in `table_references` that the statement
/// reads, updates or deletes to the rows the role may access. The policies of
/// `commands` decide: SELECT for a read, and UPDATE or DELETE for the table a
/// statement writes, with SELECT too when the statement reads the table's
/// columns, as in PostgreSQL.
///
/// Each such table gets the condition its rows must pass as
/// [`JoinedTable::row_filter`], which the loop over the table checks before
/// any other condition, so no expression of the statement sees a row the
/// role may not access. On the right side of a LEFT JOIN a row that fails it
/// reads as NULLs, like a row that does not exist.
///
/// The parts of the condition are also added to `where_clause` so that the
/// optimizer can use them, for example to seek an index on the column a
/// policy compares with the role. Rows that fail them were already skipped.
/// They are not added for a table that a RIGHT or FULL JOIN can extend with
/// NULLs, because there they would also remove the NULL-extended rows that the
/// join keeps.
pub(crate) fn filter_rows(
    commands: &[Command],
    table_references: &mut TableReferences,
    where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let mut terms = Vec::new();
    for index in 0..table_references.joined_tables().len() {
        let table = &table_references.joined_tables()[index];
        let Table::BTree(btree) = &table.table else {
            continue;
        };
        let mut conditions = Vec::new();
        for command in commands {
            match policy(resolver, table.namespace, &btree.declared_name, *command) {
                Policy::Allow => break,
                Policy::Apply(policies) => {
                    conditions.extend(conditions_of(&policies, using_condition));
                }
            }
        }
        let Some(filter) = conditions
            .into_iter()
            .map(|condition| condition.expr)
            .reduce(|all, expr| combine(Some(all), ast::Operator::And, expr))
        else {
            continue;
        };
        let filter = bind_policy_expr(&filter, table, btree, resolver, program)?;
        let join_info = table.join_info.as_ref();
        let extended_with_nulls_by_right_join = join_info.is_some_and(JoinInfo::keeps_right_rows)
            || table_references.is_left_of_right_or_full_join(table.internal_id);
        if !extended_with_nulls_by_right_join {
            let origin = if join_info.is_some_and(JoinInfo::is_outer) {
                WhereTermOrigin::Join(JoinOrigin::Outer(table.internal_id))
            } else {
                WhereTermOrigin::Where
            };
            let mut parts: Vec<ast::Expr> = Vec::new();
            break_predicate_at_and_boundaries(&filter, &mut parts);
            terms.extend(parts.into_iter().map(|expr| WhereTerm {
                expr,
                origin,
                consumed: false,
            }));
        }
        let used_columns = columns_of_table(&filter, table.internal_id)?;
        let table = &mut table_references.joined_tables_mut()[index];
        for column in used_columns {
            table.mark_column_used(column);
        }
        table.row_filter = Some(filter);
    }
    where_clause.splice(0..0, terms);
    Ok(())
}

/// A check that each row a statement writes must pass, or the statement
/// fails.
#[derive(Debug, Clone)]
pub(crate) struct RowCheck {
    condition: Condition,
    /// The condition is a USING condition checked on the existing row.
    using_condition: bool,
}

/// The checks that the new rows `command` writes to `table_name` of
/// `namespace` must pass for the role, as in PostgreSQL: the WITH CHECK
/// conditions of the command's policies, or their USING conditions where
/// they have none, and the USING conditions of the SELECT policies when the
/// statement reads the table's columns, so that a role never writes a row it
/// could not read back that way.
pub(crate) fn new_row_checks(
    resolver: &Resolver,
    namespace: NamespaceRef,
    table_name: &str,
    command: Command,
    reads_columns: bool,
) -> Vec<RowCheck> {
    let Policy::Apply(policies) = policy(resolver, namespace, table_name, command) else {
        return Vec::new();
    };
    let mut conditions = conditions_of(&policies, with_check_condition);
    if reads_columns {
        let Policy::Apply(select_policies) =
            policy(resolver, namespace, table_name, Command::Select)
        else {
            unreachable!("row-level security limits every command of a role or none");
        };
        conditions.extend(conditions_of(&select_policies, using_condition));
    }
    conditions
        .into_iter()
        .map(|condition| RowCheck {
            condition,
            using_condition: false,
        })
        .collect()
}

/// The checks of row-level security for `INSERT ... ON CONFLICT DO UPDATE`
/// into `table_name` of `namespace`.
#[derive(Debug, Clone, Default)]
pub(crate) struct UpsertRowChecks {
    /// The checks of the existing row that conflicts, before it is updated:
    /// the USING conditions of the UPDATE and SELECT policies. Unlike a
    /// plain UPDATE, a row that fails them is an error, not skipped, so that
    /// the row the role proposed is never dropped without a word.
    pub(crate) conflicting_row: Vec<RowCheck>,
    /// The checks of the row the update writes, as for a plain UPDATE that
    /// reads the table's columns.
    pub(crate) updated_row: Vec<RowCheck>,
}

/// The checks of `INSERT ... ON CONFLICT DO UPDATE` into `table_name` of
/// `namespace` for the role, as in PostgreSQL.
pub(crate) fn upsert_row_checks(
    resolver: &Resolver,
    namespace: NamespaceRef,
    table_name: &str,
) -> UpsertRowChecks {
    let Policy::Apply(update_policies) = policy(resolver, namespace, table_name, Command::Update)
    else {
        return UpsertRowChecks::default();
    };
    let Policy::Apply(select_policies) = policy(resolver, namespace, table_name, Command::Select)
    else {
        unreachable!("row-level security limits every command of a role or none");
    };
    let conflicting_row = conditions_of(&update_policies, using_condition)
        .into_iter()
        .chain(conditions_of(&select_policies, using_condition))
        .map(|condition| RowCheck {
            condition,
            using_condition: true,
        })
        .collect();
    UpsertRowChecks {
        conflicting_row,
        updated_row: new_row_checks(resolver, namespace, table_name, Command::Update, true),
    }
}

/// Emits `checks` on a row that the statement writes to the first table of
/// `table_references`, whose rowid is in `rowid_reg` and whose columns are in
/// the registers `columns` gives. The statement fails at the first check that
/// the row does not pass; a condition that is NULL fails too, as in
/// PostgreSQL.
pub(crate) fn emit_row_checks<'a>(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    checks: &[RowCheck],
    table_references: &TableReferences,
    rowid_reg: usize,
    columns: impl Iterator<Item = (&'a Column, usize)>,
) -> Result<()> {
    if checks.is_empty() {
        return Ok(());
    }
    let table = &table_references.joined_tables()[0];
    let btree = table
        .btree()
        .expect("row-level security checks rows of B-tree tables");
    let mut read_columns = HashSet::default();
    for check in checks {
        read_columns.extend(collect_column_dependencies_of_expr(
            &check.condition.expr,
            &[],
        ));
    }
    with_columns_in_registers(
        program,
        resolver,
        &btree,
        rowid_reg,
        columns,
        Some(table_references),
        |name| read_columns.contains(&normalize_ident(name)),
        |program, resolver, _| {
            for check in checks {
                let expr =
                    bind_policy_expr(&check.condition.expr, table, &btree, resolver, program)?;
                let result = program.alloc_register();
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    &expr,
                    result,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                let passed = program.allocate_label();
                program.emit_insn(Insn::If {
                    reg: result,
                    target_pc: passed,
                    jump_if_null: false,
                });
                program.emit_insn(Insn::RowSecurityViolation {
                    denial: Box::new(PermissionDenied::policy(
                        Object::Table(btree.declared_name.clone()),
                        check.condition.policy.clone(),
                        check.using_condition,
                    )),
                });
                program.preassign_label_to_next_insn(passed);
            }
            Ok(())
        },
    )
}

/// Checks that the role may read the virtual table. A virtual table that
/// shows the database file would show the rows that row-level security
/// hides, so only a superuser may read it, like a PRAGMA.
pub(crate) fn check_virtual_table(table: &VirtualTable, resolver: &Resolver) -> Result<()> {
    let superuser_only = matches!(
        &table.vtab_type,
        VirtualTableType::Internal(internal) if internal.read().superuser_only()
    );
    if superuser_only && resolver.role_is_not_superuser {
        return Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::privilege(Action::Read, Object::Table(table.name.clone())),
        )));
    }
    Ok(())
}

/// Checks that the current role may open a BLOB handle on a row of `table`
/// of the root namespace, which reads and writes the row without a
/// statement, so without the conditions of policies.
pub(crate) fn check_blob_open(
    connection: &Arc<Connection>,
    table_name: &str,
    read_write: bool,
) -> Result<()> {
    let (command, action) = if read_write {
        (Command::Update, Action::Write)
    } else {
        (Command::Select, Action::Read)
    };
    let dialect = connection.dialect();
    let policy = connection.with_schema(MAIN_DB_ID, |schema| {
        policy_for(
            dialect.as_ref(),
            &schema.roles,
            schema,
            connection.current_role(),
            NamespaceId::ROOT,
            table_name,
            command,
        )
    });
    match policy {
        Policy::Allow => Ok(()),
        Policy::Apply(_) => Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::privilege(action, Object::Table(table_name.to_string())),
        ))),
    }
}

/// Which rows of table `table_name` of `namespace` the role may access for
/// `command`. Inside a view, the view's owner reads the table, as for
/// privileges. Whether the table has row-level security is a question for
/// the table's own database, so a table of an attached database with
/// row-level security is limited too. The roles are those of the main
/// database, which the role was set from.
fn policy(
    resolver: &Resolver,
    namespace: NamespaceRef,
    table_name: &str,
    command: Command,
) -> Policy {
    let roles = resolver.schema().roles.clone();
    // A statement the engine compiles as the superuser stays unlimited
    // inside a view.
    let role = if roles.is_superuser(resolver.row_security_role) {
        resolver.row_security_role
    } else {
        roles::table_reader(resolver, resolver.row_security_role)
    };
    resolver.with_schema(namespace.database_id, |schema| {
        policy_for(
            resolver.catalog.dialect.as_ref(),
            &roles,
            schema,
            role,
            namespace.id,
            table_name,
            command,
        )
    })
}

/// Whether row-level security limits the rows of table `table_name` of
/// `namespace` in `schema` that `role` may access for `command`, and the
/// policies that then decide. As in PostgreSQL's `check_enable_rls`,
/// row-level security does not limit superusers or the table's owner.
fn policy_for(
    dialect: &dyn Dialect,
    roles: &RoleCatalog,
    schema: &Schema,
    role: RoleId,
    namespace: NamespaceId,
    table_name: &str,
    command: Command,
) -> Policy {
    let table_name = normalize_ident(table_name);
    if roles.is_superuser(role)
        || !dialect.table_rls_enabled(schema, namespace, &table_name)
        || dialect.relation_owner(schema, namespace, &table_name) == Some(role)
    {
        return Policy::Allow;
    }
    // As in PostgreSQL, row-level security does not apply to TRUNCATE.
    if command == Command::Truncate {
        return Policy::Allow;
    }
    Policy::Apply(
        dialect
            .row_security_policies(schema, namespace, &table_name)
            .iter()
            .filter(|policy| policy.applies_to(command, role))
            .cloned()
            .collect(),
    )
}

/// A condition of a table's policies that rows must pass.
#[derive(Debug, Clone)]
struct Condition {
    expr: ast::Expr,
    /// The restrictive policy that the condition comes from, which an error
    /// names. The permissive policies are checked together, without a name.
    policy: Option<String>,
}

/// The conditions that `policies`, the policies of one command that apply
/// to a role, put on rows, as PostgreSQL builds them: the conditions of the
/// permissive policies combined with OR, and the condition of each
/// restrictive policy. Without a permissive policy that has a condition, no
/// row passes. `condition_of` gives a policy's condition, if it has one.
fn conditions_of(
    policies: &[RowPolicy],
    condition_of: fn(&RowPolicy) -> Option<&ast::Expr>,
) -> Vec<Condition> {
    let permissive = policies
        .iter()
        .filter(|policy| policy.permissive)
        .filter_map(condition_of)
        .cloned()
        .reduce(|any, expr| combine(Some(any), ast::Operator::Or, expr));
    let Some(permissive) = permissive else {
        return vec![Condition {
            expr: no_rows(),
            policy: None,
        }];
    };
    let restrictive = policies
        .iter()
        .filter(|policy| !policy.permissive)
        .filter_map(|policy| {
            condition_of(policy).map(|expr| Condition {
                expr: expr.clone(),
                policy: Some(policy.name.clone()),
            })
        });
    std::iter::once(Condition {
        expr: permissive,
        policy: None,
    })
    .chain(restrictive)
    .collect()
}

/// Which existing rows a policy lets a role access.
fn using_condition(policy: &RowPolicy) -> Option<&ast::Expr> {
    policy.using_expr.as_ref()
}

/// Which new rows a policy lets a role write: its WITH CHECK condition, or
/// its USING condition when it has none.
fn with_check_condition(policy: &RowPolicy) -> Option<&ast::Expr> {
    policy.check_expr.as_ref().or(policy.using_expr.as_ref())
}

fn combine(left: Option<ast::Expr>, operator: ast::Operator, right: ast::Expr) -> ast::Expr {
    match left {
        None => right,
        Some(left) => ast::Expr::Binary(
            Box::new(parenthesized(left)),
            operator,
            Box::new(parenthesized(right)),
        ),
    }
}

/// Binds a policy condition to `table`, the table it protects. The condition
/// sees only that table, whatever the statement around it has in scope.
fn bind_policy_expr(
    expr: &ast::Expr,
    table: &JoinedTable,
    btree: &BTreeTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<ast::Expr> {
    let mut policy_table = table.clone();
    policy_table.identifier = normalize_ident(&btree.declared_name);
    policy_table.has_alias = false;
    policy_table.join_info = None;
    let mut table_references = TableReferences::new(vec![policy_table], Vec::new());
    let mut expr = expr.clone();
    compute_scalar_selects_once(&mut expr, &mut table_references, resolver, program)?;
    bind_and_rewrite_expr(
        &mut expr,
        Some(&mut table_references),
        None,
        resolver,
        BindingBehavior::ResultColumnsNotAllowed,
    )?;
    Ok(expr)
}

/// Replaces each `(SELECT <expr>)` of a policy condition by the register that
/// holds its value, computed once when the statement starts, as PostgreSQL
/// computes such a subquery once per query. `auth.uid()` is then called once
/// instead of for every row. A `<expr>` that reads a column of the row is
/// computed for every row, as in PostgreSQL.
fn compute_scalar_selects_once(
    expr: &mut ast::Expr,
    table_references: &mut TableReferences,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| {
        let ast::Expr::Subquery(select) = expr else {
            return Ok(WalkControl::Continue);
        };
        let mut value = single_value_of(select)
            .expect("CREATE POLICY stores no other subqueries")
            .clone();
        compute_scalar_selects_once(&mut value, table_references, resolver, program)?;
        if reads_a_column(&value)? {
            *expr = parenthesized(value);
            return Ok(WalkControl::SkipChildren);
        }
        bind_and_rewrite_expr(
            &mut value,
            Some(table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        let register = program.alloc_register();
        program.emit_once_per_statement(|program| {
            translate_expr(program, Some(table_references), &value, register, resolver)
        })?;
        *expr = ast::Expr::Register {
            register,
            custom_type: None,
        };
        Ok(WalkControl::SkipChildren)
    })?;
    Ok(())
}

fn reads_a_column(expr: &ast::Expr) -> Result<bool> {
    let mut reads_column = false;
    walk_expr(expr, &mut |expr| {
        if matches!(
            expr,
            ast::Expr::Id(_)
                | ast::Expr::Qualified(..)
                | ast::Expr::DoublyQualified(..)
                | ast::Expr::Column { .. }
                | ast::Expr::RowId { .. }
        ) {
            reads_column = true;
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(reads_column)
}

/// The columns of the table `internal_id` that the bound `expr` reads.
fn columns_of_table(expr: &ast::Expr, internal_id: ast::TableInternalId) -> Result<Vec<usize>> {
    let mut columns = Vec::new();
    walk_expr(expr, &mut |expr| {
        if let ast::Expr::Column { table, column, .. } = expr {
            if *table == internal_id {
                columns.push(*column);
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(columns)
}

/// A condition that no row passes.
fn no_rows() -> ast::Expr {
    ast::Expr::Literal(ast::Literal::Numeric("0".to_string()))
}

/// ALTER TABLE ... ENABLE ROW LEVEL SECURITY, or DISABLE when `enabled` is
/// false. The dialect records it with the table, so it follows the table
/// when the table is renamed and goes away when the table is dropped.
/// [`crate::translate::roles::check_statement_privileges`] lets the table's
/// owner and superusers run it.
pub(crate) fn translate_set_row_security(
    qualified_name: &ast::QualifiedName,
    enabled: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let (namespace, table) = row_security_table(qualified_name, resolver)?;
    translate_change(
        RelationChange::SetRowSecurity {
            namespace: namespace.id,
            name: normalize_ident(&table.declared_name),
            enabled,
        },
        resolver,
        program,
        connection,
    )
}

/// CREATE POLICY. The dialect stores the policy with the table, so it
/// follows the table when the table is renamed and goes away when the table
/// is dropped.
///
/// The conditions may refer to the columns of the table, written as `column`
/// or `table.column`, call scalar functions, and use `(SELECT <expr>)`
/// without FROM, which Supabase recommends for function calls because
/// PostgreSQL computes it once per query. The conditions are stored with
/// unqualified column names.
/// [`crate::translate::roles::check_statement_privileges`] lets the table's
/// owner and superusers run it.
pub(crate) fn translate_create_policy(
    policy: &ast::CreatePolicy,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let (namespace, table) = row_security_table(&policy.tbl_name, resolver)?;
    if policy.check_expr.is_some()
        && matches!(
            policy.command,
            ast::PolicyCommand::Select | ast::PolicyCommand::Delete
        )
    {
        bail_parse_error!("WITH CHECK cannot be applied to SELECT or DELETE");
    }
    if policy.using_expr.is_some() && policy.command == ast::PolicyCommand::Insert {
        bail_parse_error!("only WITH CHECK expression allowed for INSERT");
    }
    // The policy name is kept as the parser gives it, as in PostgreSQL,
    // where a quoted name keeps its case, so "Own" and own are two policies.
    let mut stored = policy.clone();
    for expr in [&mut stored.using_expr, &mut stored.check_expr]
        .into_iter()
        .flatten()
    {
        prepare_policy_expr(expr, namespace, &table, resolver)?;
    }
    translate_change(
        RelationChange::CreatePolicy {
            namespace: namespace.id,
            name: normalize_ident(&table.declared_name),
            policy: Box::new(stored),
        },
        resolver,
        program,
        connection,
    )
}

/// DROP POLICY. With IF EXISTS, a missing table or policy is not an error,
/// as in PostgreSQL.
pub(crate) fn translate_drop_policy(
    policy_name: &ast::Name,
    tbl_name: &ast::QualifiedName,
    if_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let table_exists = resolver
        .resolve_existing_table(tbl_name)
        .is_ok_and(|namespace| {
            resolver.with_schema(namespace.database_id, |schema| {
                schema
                    .get_table(namespace.id, tbl_name.name.as_str())
                    .is_some()
            })
        });
    if if_exists && !table_exists {
        return Ok(());
    }
    let (namespace, table) = row_security_table(tbl_name, resolver)?;
    translate_change(
        RelationChange::DropPolicy {
            namespace: namespace.id,
            name: normalize_ident(&table.declared_name),
            policy_name: policy_name.as_str().to_string(),
            if_exists,
        },
        resolver,
        program,
        connection,
    )
}

/// Policies refer to the columns of their table by name, so a column that a
/// policy uses cannot be renamed, dropped or redefined.
pub(crate) fn reject_change_of_policy_column(
    resolver: &Resolver,
    namespace: NamespaceRef,
    table_name: &str,
    column_name: &str,
) -> Result<()> {
    if namespace.database_id != MAIN_DB_ID {
        return Ok(());
    }
    let table_name = normalize_ident(table_name);
    let column_name = normalize_ident(column_name);
    let used_by = resolver
        .catalog
        .dialect
        .row_security_policies(resolver.schema(), namespace.id, &table_name)
        .iter()
        .find(|policy| policy_uses_column(policy, &column_name))
        .map(|policy| policy.name.clone());
    if let Some(policy_name) = used_by {
        bail_parse_error!(
            "cannot change column \"{column_name}\" of table \"{table_name}\" because policy \"{policy_name}\" uses it"
        );
    }
    Ok(())
}

fn policy_uses_column(policy: &RowPolicy, column_name: &str) -> bool {
    [&policy.using_expr, &policy.check_expr]
        .into_iter()
        .flatten()
        .any(|expr| {
            let mut expr = expr.clone();
            replace_scalar_selects(&mut expr);
            collect_column_dependencies_of_expr(&expr, &[]).contains(column_name)
        })
}

/// The table of the main database that row-level security statements name.
/// Views, materialized views and the engine's own tables cannot have
/// row-level security.
fn row_security_table(
    qualified_name: &ast::QualifiedName,
    resolver: &Resolver,
) -> Result<(NamespaceRef, Arc<BTreeTable>)> {
    let namespace = resolver.resolve_existing_table(qualified_name)?;
    let table_name = normalize_ident(qualified_name.name.as_str());
    if namespace.database_id != MAIN_DB_ID {
        bail_parse_error!("row-level security is only supported for tables in the main database");
    }
    let schema = resolver.schema();
    let is_view = schema.get_view(namespace.id, &table_name).is_some()
        || schema
            .get_materialized_view(namespace.id, &table_name)
            .is_some();
    if is_view {
        bail_parse_error!("\"{table_name}\" is not a table");
    }
    match schema.get_btree_table(namespace.id, &table_name) {
        Some(table) => Ok((namespace, table)),
        None if schema.get_table(namespace.id, &table_name).is_some() => {
            bail_parse_error!("\"{table_name}\" is not a table")
        }
        None => bail_parse_error!(
            "relation \"{}\" does not exist",
            crate::util::table_name_for_error(qualified_name)
        ),
    }
}

/// Records `change` through the dialect, in a write transaction that bumps
/// the schema cookie so that other connections load it.
fn translate_change(
    change: RelationChange,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let schema = resolver.schema();
    program.begin_write_on_database(MAIN_DB_ID, schema.schema_version)?;
    resolver
        .catalog
        .dialect
        .translate_relation_changes(program, schema, connection, &[change])?;
    emit_schema_cookie(resolver, program);
    Ok(())
}

/// Checks a policy condition of `table` of `namespace` and puts it in the
/// form it is stored in: columns lose their schema and table names, so the
/// condition still holds after the table is renamed, and are quoted, so a
/// column named like a keyword, such as `true`, is still read as the column.
/// `(SELECT <expr>)` without FROM stays, because it is computed once per
/// statement.
fn prepare_policy_expr(
    expr: &mut ast::Expr,
    namespace: NamespaceRef,
    table: &BTreeTable,
    resolver: &Resolver,
) -> Result<()> {
    let table_name = normalize_ident(&table.declared_name);
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| {
        match expr {
            ast::Expr::Subquery(select) => {
                let Some(value) = single_value_of_mut(select) else {
                    bail_parse_error!("subqueries in policies are not supported")
                };
                prepare_policy_expr(value, namespace, table, resolver)?;
                return Ok(WalkControl::SkipChildren);
            }
            ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries in policies are not supported")
            }
            ast::Expr::Qualified(qualifier, name) => {
                if normalize_ident(qualifier.as_str()) != table_name {
                    bail_parse_error!(
                        "a policy of table \"{table_name}\" can only refer to the columns of that table"
                    );
                }
                *expr = policy_column(name, table)?;
            }
            ast::Expr::DoublyQualified(schema, qualifier, name) => {
                if resolver.namespace_named(&normalize_ident(schema.as_str())) != Some(namespace)
                    || normalize_ident(qualifier.as_str()) != table_name
                {
                    bail_parse_error!(
                        "a policy of table \"{table_name}\" can only refer to the columns of that table"
                    );
                }
                *expr = policy_column(name, table)?;
            }
            ast::Expr::Id(name) => {
                *expr = policy_column(name, table)?;
            }
            ast::Expr::Variable(_) => {
                bail_parse_error!("there is no parameter $1")
            }
            ast::Expr::FunctionCall { name, args, .. } => {
                check_policy_function(name.as_str(), args.len(), resolver)?;
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                check_policy_function(name.as_str(), 0, resolver)?;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Column `name` of `table` as a policy stores it: unqualified and quoted.
fn policy_column(name: &ast::Name, table: &BTreeTable) -> Result<ast::Expr> {
    let Some((_, column)) = table.get_column(name.as_str()) else {
        bail_parse_error!("column \"{}\" does not exist", name.as_str());
    };
    if column.is_virtual_generated() {
        bail_parse_error!("policies cannot use generated column \"{}\"", name.as_str());
    }
    Ok(ast::Expr::Id(ast::Name::from_string(format!(
        "\"{}\"",
        name.as_str().replace('"', "\"\"")
    ))))
}

/// A policy condition is checked on one row at a time, so it can only call
/// scalar functions, as in PostgreSQL.
fn check_policy_function(name: &str, arg_count: usize, resolver: &Resolver) -> Result<()> {
    match resolver.resolve_function(name, arg_count)? {
        None => bail_parse_error!("function {name} does not exist"),
        Some(Func::Agg(_)) => {
            bail_parse_error!("aggregate functions are not allowed in policy expressions")
        }
        Some(Func::Window(_)) => {
            bail_parse_error!("window functions are not allowed in policy expressions")
        }
        Some(_) => Ok(()),
    }
}

/// Replaces each `(SELECT <expr>)` without FROM or any other clause by
/// `(<expr>)`.
fn replace_scalar_selects(expr: &mut ast::Expr) {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| {
        if let ast::Expr::Subquery(select) = expr {
            if let Some(value) = single_value_of(select) {
                *expr = parenthesized(value.clone());
            }
        }
        Ok(WalkControl::Continue)
    })
    .expect("the walk never fails");
}

fn parenthesized(expr: ast::Expr) -> ast::Expr {
    ast::Expr::Parenthesized(vec![Box::new(expr)])
}

/// The expression of `SELECT <expr>` without FROM or any other clause.
fn single_value_of(select: &ast::Select) -> Option<&ast::Expr> {
    if select.with.is_some()
        || !select.body.compounds.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
    {
        return None;
    }
    match &select.body.select {
        ast::OneSelect::Select {
            distinctness: None,
            columns,
            from: None,
            where_clause: None,
            group_by: None,
            window_clause,
        } if window_clause.is_empty() => match columns.as_slice() {
            [ast::ResultColumn::Expr(expr, _)] => Some(expr),
            _ => None,
        },
        _ => None,
    }
}

/// [`single_value_of`], to change the expression.
fn single_value_of_mut(select: &mut ast::Select) -> Option<&mut ast::Expr> {
    single_value_of(select)?;
    match &mut select.body.select {
        ast::OneSelect::Select { columns, .. } => match columns.as_mut_slice() {
            [ast::ResultColumn::Expr(expr, _)] => Some(expr),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustc_hash::FxHashMap;
    use turso_parser::ast;

    use super::{
        conditions_of, policy, using_condition, with_check_condition, Command, Condition, Policy,
    };
    use crate::dialect::{Dialect, EngineDialect};
    use crate::function::Func;
    use crate::namespace::{NamespaceId, NamespaceRef};
    use crate::schema::{BTreeTable, Schema};
    use crate::security::policy::RowPolicy;
    use crate::security::roles::{RoleCatalog, RoleId};
    use crate::translate::emitter::{DoubleQuotedDml, Resolver};
    use crate::{Database, DatabaseCatalog, MemoryIO, Qualifiers, RwLock, SymbolTable, Value};

    /// The engine dialect, except that every table of a schema with dialect
    /// schema state has row-level security, and table `documents` has a
    /// policy that lets every role access every row.
    struct RowSecurityInSchemasWithState;

    static ACCESS_EVERY_ROW: std::sync::LazyLock<Vec<RowPolicy>> = std::sync::LazyLock::new(|| {
        vec![RowPolicy {
            name: "everything".to_string(),
            permissive: true,
            command: ast::PolicyCommand::All,
            roles: vec![RoleId::PUBLIC],
            using_expr: Some(ast::Expr::Literal(ast::Literal::True)),
            check_expr: Some(ast::Expr::Literal(ast::Literal::True)),
        }]
    });

    impl Dialect for RowSecurityInSchemasWithState {
        fn name(&self) -> &'static str {
            EngineDialect.name()
        }

        fn parse(&self, sql: &str) -> crate::Result<(Option<ast::Cmd>, usize)> {
            EngineDialect.parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            EngineDialect.parse_table_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<ast::Stmt> {
            EngineDialect.parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            EngineDialect.table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            tbl_name: &ast::QualifiedName,
            body: &ast::CreateTableBody,
        ) -> crate::Result<String> {
            EngineDialect.format_table_sql(input, tbl_name, body)
        }

        fn register_catalog(
            &self,
            schema: &mut Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            EngineDialect.register_catalog(schema, enable_custom_types)
        }

        fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<Func>> {
            EngineDialect.resolve_function(name, arg_count)
        }

        fn table_rls_enabled(&self, schema: &Schema, _namespace: NamespaceId, _name: &str) -> bool {
            schema.dialect_schema.is_some()
        }

        fn row_security_policies<'a>(
            &self,
            _schema: &'a Schema,
            _namespace: NamespaceId,
            name: &str,
        ) -> &'a [RowPolicy] {
            if name == "documents" {
                &ACCESS_EVERY_ROW
            } else {
                &[]
            }
        }
    }

    /// The permissive policies' conditions are one condition, combined with
    /// OR, and each restrictive policy's condition is one more, named after
    /// the policy, as in PostgreSQL.
    #[test]
    fn conditions_combine_permissive_policies_and_keep_restrictive_ones_apart() {
        let policies = [
            row_policy("a", true, Some("a = 1"), None),
            row_policy("b", true, Some("b = 1"), Some("b = 2")),
            row_policy("r", false, Some("r = 1"), None),
        ];

        assert_eq!(
            texts(&conditions_of(&policies, using_condition)),
            vec![
                (None, "(a = 1) OR (b = 1)".to_string()),
                (Some("r".to_string()), "r = 1".to_string()),
            ]
        );
        assert_eq!(
            texts(&conditions_of(&policies, with_check_condition)),
            vec![
                (None, "(a = 1) OR (b = 2)".to_string()),
                (Some("r".to_string()), "r = 1".to_string()),
            ]
        );
    }

    /// Without a permissive policy that has the condition, no row passes,
    /// whatever the restrictive policies say.
    #[test]
    fn conditions_without_a_permissive_policy_pass_no_row() {
        let policies = [
            row_policy("insert_only", true, None, Some("a = 1")),
            row_policy("r", false, Some("r = 1"), None),
        ];

        assert_eq!(
            texts(&conditions_of(&policies, using_condition)),
            vec![(None, "0".to_string())]
        );
        assert_eq!(
            texts(&conditions_of(&[], with_check_condition)),
            vec![(None, "0".to_string())]
        );
    }

    fn row_policy(
        name: &str,
        permissive: bool,
        using_expr: Option<&str>,
        check_expr: Option<&str>,
    ) -> RowPolicy {
        let expr = |sql: &str| {
            let Some(ast::Cmd::Stmt(ast::Stmt::Select(select))) =
                EngineDialect.parse(&format!("SELECT {sql}")).unwrap().0
            else {
                panic!("{sql} is not an expression");
            };
            let ast::OneSelect::Select { columns, .. } = select.body.select else {
                panic!("{sql} is not an expression");
            };
            let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
                panic!("{sql} is not an expression");
            };
            *expr.clone()
        };
        RowPolicy {
            name: name.to_string(),
            permissive,
            command: ast::PolicyCommand::All,
            roles: vec![RoleId::PUBLIC],
            using_expr: using_expr.map(expr),
            check_expr: check_expr.map(expr),
        }
    }

    fn texts(conditions: &[Condition]) -> Vec<(Option<String>, String)> {
        conditions
            .iter()
            .map(|condition| (condition.policy.clone(), condition.expr.to_string()))
            .collect()
    }

    /// Foreign-key actions see every row, as in PostgreSQL, so a cascading
    /// delete also removes the rows that row-level security hides.
    #[test]
    fn row_security_limits_nothing_compiled_as_superuser() {
        let (main, alice) = main_schema_with_role_alice();
        with_resolver(&main, FxHashMap::default(), alice, |resolver| {
            let root = NamespaceRef::root_of(crate::MAIN_DB_ID);

            let inside = resolver
                .as_superuser(|resolver| policy(resolver, root, "documents", Command::Delete));

            assert_eq!(inside, Policy::Allow);
            assert_eq!(
                policy(resolver, root, "documents", Command::Delete),
                Policy::Apply(ACCESS_EVERY_ROW.clone())
            );
        });
    }

    /// Whether a table of an attached database has row-level security is a
    /// question for the attached database, not for the main database.
    #[test]
    fn table_of_an_attached_database_with_row_security_denies_a_role() {
        let (mut main, alice) = main_schema_with_role_alice();
        main.dialect_schema = None;
        let mut attached = Schema::default();
        attached.dialect_schema = Some(Arc::new(()));
        let attached_database_id = 2;
        let mut database_schemas = FxHashMap::default();
        database_schemas.insert(attached_database_id, Arc::new(attached));

        let policy = with_resolver(&main, database_schemas, alice, |resolver| {
            policy(
                resolver,
                NamespaceRef::root_of(attached_database_id),
                "t",
                Command::Select,
            )
        });

        assert_eq!(policy, Policy::Apply(Vec::new()));
    }

    /// A main database whose tables have row-level security, and the role
    /// alice, who is not a superuser.
    fn main_schema_with_role_alice() -> (Schema, RoleId) {
        let roles = RoleCatalog::new(&[vec![
            Value::from_i64(1),
            Value::build_text("alice"),
            Value::from_i64(0),
        ]])
        .unwrap();
        let alice = roles.get_by_name("alice").unwrap().id;
        let mut main = Schema::default();
        main.roles = Arc::new(roles);
        main.dialect_schema = Some(Arc::new(()));
        (main, alice)
    }

    /// Calls `f` with a resolver that compiles statements for `role` under
    /// [`RowSecurityInSchemasWithState`].
    fn with_resolver<T>(
        main: &Schema,
        attached_schemas: FxHashMap<usize, Arc<Schema>>,
        role: RoleId,
        f: impl FnOnce(&mut Resolver) -> T,
    ) -> T {
        let database_schemas = RwLock::new(attached_schemas);
        let temp_database = RwLock::new(None);
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let qualifiers = Qualifiers::default();
        let syms = SymbolTable::new();
        let mut resolver = Resolver::new(
            main,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &qualifiers,
            &syms,
            true,
            DoubleQuotedDml::Enabled,
            Arc::new(RowSecurityInSchemasWithState),
            None,
            role,
        );
        f(&mut resolver)
    }

    /// A materialized view stores the rows of its query when it is refreshed,
    /// so policies on it could not limit them.
    #[test]
    fn row_security_statements_reject_a_materialized_view() {
        let db = Database::open_file_with_flags(
            Arc::new(MemoryIO::new()),
            "row-security-materialized-view.db",
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new().with_views(true),
            None,
            Arc::new(EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT x FROM t")
            .unwrap();
        let mv = || ast::QualifiedName::single(ast::Name::exact("mv".to_string()));
        let statements = [
            ast::Stmt::AlterTable(ast::AlterTable {
                name: mv(),
                body: ast::AlterTableBody::EnableRowSecurity,
            }),
            ast::Stmt::CreatePolicy(Box::new(ast::CreatePolicy {
                policy_name: ast::Name::exact("p".to_string()),
                tbl_name: mv(),
                permissive: true,
                command: ast::PolicyCommand::All,
                roles: vec![ast::Grantee::Public],
                using_expr: None,
                check_expr: None,
            })),
        ];

        for statement in statements {
            let sql = statement.to_string();
            let error = conn
                .prepare_translated_cmd(ast::Cmd::Stmt(statement), &sql)
                .unwrap_err();

            assert!(
                error.to_string().contains("\"mv\" is not a table"),
                "{error}"
            );
        }
    }

    #[test]
    fn enabling_row_security_fails_under_a_dialect_without_it() {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (x)").unwrap();
        let enable = ast::Cmd::Stmt(ast::Stmt::AlterTable(ast::AlterTable {
            name: ast::QualifiedName::single(ast::Name::exact("t".to_string())),
            body: ast::AlterTableBody::EnableRowSecurity,
        }));

        let error = conn
            .prepare_translated_cmd(enable, "ALTER TABLE t ENABLE ROW LEVEL SECURITY")
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("row-level security is not supported"),
            "{error}"
        );
    }
}
