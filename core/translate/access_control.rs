use std::sync::Arc;

use turso_parser::ast;

use crate::{
    access_control::{
        AccessControlCatalog, AccessControlChange, Policy, PolicyCondition,
        ACCESS_CONTROL_TABLE_NAME, ACCESS_CONTROL_TABLE_SQL,
    },
    bail_parse_error,
    schema::{BTreeTable, Table},
    storage::pager::CreateBTreeFlags,
    translate::{
        emitter::Resolver,
        plan::{TableReferences, WhereTerm},
        schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
    },
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral},
    },
    Result, MAIN_DB_ID,
};

pub fn translate_create_role(
    role_name: &ast::Name,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let role = normalize_ident(role_name.as_str());
    if role == "public" {
        bail_parse_error!("role name \"public\" is reserved");
    }
    if catalog(resolver).has_role(&role) {
        bail_parse_error!("role \"{role}\" already exists");
    }
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_insert(program, &["role", &role, "", "", ""]);
    emit_catalog_update(program, resolver, AccessControlChange::CreateRole(role));
    Ok(())
}

pub fn translate_drop_role(
    role_name: &ast::Name,
    if_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let role = normalize_ident(role_name.as_str());
    if !catalog(resolver).has_role(&role) {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("role \"{role}\" does not exist");
    }
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_delete(program, &[("role", 0), (&role, 1)]);
    emit_catalog_update(program, resolver, AccessControlChange::DropRole(role));
    Ok(())
}

/// SET ROLE and RESET ROLE change the role when the statement runs, not
/// when it is prepared. The read transaction makes the statement check that
/// the schema is current first, so a role created by another process is
/// found.
pub fn translate_set_role(
    role_name: Option<&ast::Name>,
    program: &mut ProgramBuilder,
) -> Result<()> {
    program.begin_read_operation()?;
    program.emit_insn(Insn::SetRole {
        role: role_name.map(|name| normalize_ident(name.as_str())),
    });
    Ok(())
}

pub fn translate_row_security_change(
    tbl_name: &ast::QualifiedName,
    database_id: usize,
    enable: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if database_id != MAIN_DB_ID {
        bail_parse_error!("row-level security is only supported for tables in the main database");
    }
    let table = normalize_ident(tbl_name.name.as_str());
    if catalog(resolver).has_row_security(&table) == enable {
        return Ok(());
    }
    let rows = AccessControlRows::open(resolver, program)?;
    if enable {
        rows.emit_insert(program, &["row_security", "", &table, "", ""]);
    } else {
        rows.emit_delete(program, &[("row_security", 0), (&table, 2)]);
    }
    emit_catalog_update(
        program,
        resolver,
        AccessControlChange::SetRowSecurity {
            table,
            enabled: enable,
        },
    );
    Ok(())
}

/// CREATE POLICY for the forms supported so far: a permissive SELECT (or
/// ALL) policy for every role, whose USING condition shows every row, no
/// row, or the rows whose owner column equals the current role.
pub fn translate_create_policy(
    policy: &ast::CreatePolicy,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let table = main_btree_table(&policy.tbl_name, resolver)?;
    let name = normalize_ident(policy.policy_name.as_str());
    if catalog(resolver)
        .policies(&table.name)
        .iter()
        .any(|existing| existing.name == name)
    {
        bail_parse_error!(
            "policy \"{name}\" for table \"{}\" already exists",
            table.name
        );
    }
    if policy.restrictive {
        bail_parse_error!("RESTRICTIVE policies are not supported");
    }
    if !matches!(
        policy.command,
        ast::PolicyCommand::All | ast::PolicyCommand::Select
    ) {
        bail_parse_error!("only FOR SELECT and FOR ALL policies are supported");
    }
    if !policy.roles.is_empty() {
        bail_parse_error!("only policies TO PUBLIC are supported");
    }
    if policy.check_expr.is_some() {
        bail_parse_error!("WITH CHECK is not supported");
    }
    let Some(using_expr) = &policy.using_expr else {
        bail_parse_error!("a policy needs a USING condition");
    };
    let condition = policy_condition(using_expr, &table)?;

    let (condition_value, column_name) = condition.to_row_values();
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_insert(
        program,
        &["policy", &name, &table.name, condition_value, column_name],
    );
    emit_catalog_update(
        program,
        resolver,
        AccessControlChange::CreatePolicy {
            table: normalize_ident(&table.name),
            policy: Policy { name, condition },
        },
    );
    Ok(())
}

pub fn translate_drop_policy(
    policy_name: &ast::Name,
    tbl_name: &ast::QualifiedName,
    if_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let table = main_btree_table(tbl_name, resolver)?;
    let name = normalize_ident(policy_name.as_str());
    let exists = catalog(resolver)
        .policies(&table.name)
        .iter()
        .any(|policy| policy.name == name);
    if !exists {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!(
            "policy \"{name}\" for table \"{}\" does not exist",
            table.name
        );
    }
    let table = normalize_ident(&table.name);
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_delete(program, &[("policy", 0), (&name, 1), (&table, 2)]);
    emit_catalog_update(
        program,
        resolver,
        AccessControlChange::DropPolicy { table, name },
    );
    Ok(())
}

/// DROP TABLE removes the table's row-level security and policies.
pub fn emit_drop_table_access_control_cleanup(
    table_name: &str,
    database_id: usize,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if database_id != MAIN_DB_ID || !has_access_control(table_name, resolver) {
        return Ok(());
    }
    let table = normalize_ident(table_name);
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_delete(program, &[("row_security", 0), (&table, 2)]);
    rows.emit_delete(program, &[("policy", 0), (&table, 2)]);
    emit_catalog_update(program, resolver, AccessControlChange::DropTable(table));
    Ok(())
}

pub fn reject_rename_of_table_with_row_security(
    table_name: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    if database_id == MAIN_DB_ID && has_access_control(table_name, resolver) {
        bail_parse_error!(
            "cannot rename table \"{table_name}\": renaming tables with row-level security or policies is not supported"
        );
    }
    Ok(())
}

/// Policies refer to their owner column by name, so it cannot be renamed,
/// dropped or redefined.
pub fn reject_change_of_policy_column(
    table_name: &str,
    database_id: usize,
    column_name: &str,
    resolver: &Resolver,
) -> Result<()> {
    if database_id != MAIN_DB_ID {
        return Ok(());
    }
    let column_name = normalize_ident(column_name);
    let used_by_policy = catalog(resolver).policies(table_name).iter().any(|policy| {
        matches!(&policy.condition, PolicyCondition::OwnedByRole(column) if *column == column_name)
    });
    if used_by_policy {
        bail_parse_error!(
            "cannot change column \"{column_name}\" of table \"{table_name}\": a row-level security policy uses it"
        );
    }
    Ok(())
}

/// A role sees only the rows of a table with row-level security that one of
/// the table's policies shows: every such table in the FROM clause gets a
/// filter that combines the policies with OR, and that is always false
/// without policies. For the right side of an outer join the filter is part
/// of the join condition, so the hidden rows produce NULLs like rows that do
/// not exist. The tables are recorded so the optimizer does not choose
/// access methods that evaluate the query's expressions on a row before its
/// filter.
pub fn add_select_row_security_filters(
    table_references: &mut TableReferences,
    where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver,
) -> Result<()> {
    let mut filters = Vec::new();
    let mut filtered_tables = Vec::new();
    let mut used_columns = Vec::new();
    for table in table_references.joined_tables() {
        let Table::BTree(btree) = &table.table else {
            continue;
        };
        if !row_security_applies(&btree.name, table.database_id, resolver)? {
            continue;
        }
        let role = resolver
            .role
            .as_deref()
            .expect("row security applies to roles");
        if table_references
            .joined_tables()
            .iter()
            .any(|table| table.join_info.as_ref().is_some_and(|j| j.is_full_outer()))
        {
            bail_parse_error!(
                "FULL JOIN with table \"{}\" that has row-level security is not supported",
                btree.name
            );
        }
        let catalog = catalog(resolver);
        let policies = catalog.policies(&btree.name);
        if policies
            .iter()
            .any(|policy| policy.condition == PolicyCondition::AllRows)
        {
            continue;
        }
        let mut visible = None;
        for policy in policies {
            let PolicyCondition::OwnedByRole(column) = &policy.condition else {
                continue;
            };
            let (index, column) = btree
                .get_column(column)
                .expect("policy columns cannot be dropped");
            used_columns.push((table.internal_id, index));
            let owned = owned_by_role(table.internal_id, index, column.is_rowid_alias(), role);
            visible = Some(match visible {
                None => owned,
                Some(previous) => {
                    ast::Expr::Binary(Box::new(previous), ast::Operator::Or, Box::new(owned))
                }
            });
        }
        filtered_tables.push(table.internal_id);
        filters.push(WhereTerm {
            expr: visible
                .unwrap_or_else(|| ast::Expr::Literal(ast::Literal::Numeric("0".to_string()))),
            from_outer_join: table
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.is_outer())
                .then_some(table.internal_id),
            consumed: false,
        });
    }
    for internal_id in filtered_tables {
        table_references.mark_row_security_filtered(internal_id);
    }
    for (internal_id, column) in used_columns {
        table_references.mark_column_used(internal_id, column);
    }
    where_clause.splice(0..0, filters);
    Ok(())
}

/// `column = '<role>' COLLATE BINARY`: the column's own collation must not
/// make a different role's name match.
fn owned_by_role(
    table: ast::TableInternalId,
    column: usize,
    is_rowid_alias: bool,
    role: &str,
) -> ast::Expr {
    ast::Expr::Binary(
        Box::new(ast::Expr::Column {
            database: None,
            table,
            column,
            is_rowid_alias,
        }),
        ast::Operator::Equals,
        Box::new(ast::Expr::Collate(
            Box::new(ast::Expr::Literal(ast::Literal::String(format!(
                "'{}'",
                role.replace('\'', "''")
            )))),
            ast::Name::exact("BINARY".to_string()),
        )),
    )
}

/// A role cannot write to a table with row-level security yet.
pub fn reject_write_with_row_security(
    table_name: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    if row_security_applies(table_name, database_id, resolver)? {
        bail_parse_error!(
            "writing to table \"{table_name}\" with row-level security is not supported for roles"
        );
    }
    Ok(())
}

/// Virtual tables that read the database file directly would show the rows
/// row-level security hides.
pub fn reject_raw_storage_for_roles(table_name: &str, resolver: &Resolver) -> Result<()> {
    if resolver.role.is_some()
        && ["sqlite_dbpage", "btree_dump"]
            .iter()
            .any(|name| name.eq_ignore_ascii_case(table_name))
    {
        bail_parse_error!("permission denied: {table_name} requires the superuser");
    }
    Ok(())
}

/// Whether row-level security restricts what the statement being compiled
/// may do with `table_name`. The catalog only covers the main database, so
/// tables of attached databases that have their own catalog are rejected
/// instead of being treated as unprotected.
fn row_security_applies(table_name: &str, database_id: usize, resolver: &Resolver) -> Result<bool> {
    if resolver.role.is_none() {
        return Ok(false);
    }
    if database_id != MAIN_DB_ID {
        let has_catalog = resolver.with_schema(database_id, |schema| {
            schema.get_btree_table(ACCESS_CONTROL_TABLE_NAME).is_some()
        });
        if has_catalog {
            bail_parse_error!(
                "table \"{table_name}\" is in an attached database with access control, which is not supported"
            );
        }
        return Ok(false);
    }
    Ok(catalog(resolver).has_row_security(table_name))
}

/// A connection acting as a role may only read and write rows. Everything
/// else, such as changing the schema, ATTACH, VACUUM or setting a PRAGMA,
/// needs the superuser.
pub fn reject_statement_not_allowed_for_roles(stmt: &ast::Stmt) -> Result<()> {
    let allowed = match stmt {
        ast::Stmt::Select(_)
        | ast::Stmt::Insert { .. }
        | ast::Stmt::Update(_)
        | ast::Stmt::Delete { .. }
        | ast::Stmt::Begin { .. }
        | ast::Stmt::Commit { .. }
        | ast::Stmt::Rollback { .. }
        | ast::Stmt::Savepoint { .. }
        | ast::Stmt::Release { .. }
        | ast::Stmt::SetRole { .. } => true,
        ast::Stmt::Pragma { body, .. } => body.is_none(),
        _ => false,
    };
    if !allowed {
        bail_parse_error!(
            "permission denied: {} requires the superuser",
            crate::translate::stmt_kind(stmt)
                .replace('_', " ")
                .to_uppercase()
        );
    }
    Ok(())
}

/// The condition of a supported USING expression: `true`, `false`, or
/// `<column> = current_user` in either order, where `current_user` may also
/// be written as `current_user()` or `(SELECT current_user)`.
fn policy_condition(expr: &ast::Expr, table: &BTreeTable) -> Result<PolicyCondition> {
    let expr = unwrap_parens(expr);
    match expr {
        ast::Expr::Literal(ast::Literal::True) => return Ok(PolicyCondition::AllRows),
        ast::Expr::Literal(ast::Literal::False) => return Ok(PolicyCondition::NoRows),
        ast::Expr::Literal(ast::Literal::Numeric(n)) if n == "1" => {
            return Ok(PolicyCondition::AllRows)
        }
        ast::Expr::Literal(ast::Literal::Numeric(n)) if n == "0" => {
            return Ok(PolicyCondition::NoRows)
        }
        ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) => {
            let (lhs, rhs) = (unwrap_parens(lhs), unwrap_parens(rhs));
            for (column, role) in [(lhs, rhs), (rhs, lhs)] {
                if is_current_user(role, table) {
                    if let Some(column) = column_of(column, table)? {
                        return Ok(PolicyCondition::OwnedByRole(column));
                    }
                }
            }
        }
        _ => {}
    }
    bail_parse_error!(
        "unsupported policy condition {expr}: only true, false and <column> = current_user are supported"
    )
}

fn unwrap_parens(expr: &ast::Expr) -> &ast::Expr {
    match expr {
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => unwrap_parens(&exprs[0]),
        _ => expr,
    }
}

fn is_current_user(expr: &ast::Expr, table: &BTreeTable) -> bool {
    match expr {
        ast::Expr::FunctionCall {
            name,
            distinctness: None,
            args,
            order_by,
            within_group,
            filter_over,
        } => {
            name.as_str().eq_ignore_ascii_case("current_user")
                && args.is_empty()
                && order_by.is_empty()
                && within_group.is_empty()
                && filter_over.filter_clause.is_none()
                && filter_over.over_clause.is_none()
        }
        ast::Expr::Id(name) => {
            name.as_str().eq_ignore_ascii_case("current_user")
                && table.get_column(name.as_str()).is_none()
        }
        ast::Expr::Subquery(select) => single_value_of(select)
            .is_some_and(|value| is_current_user(unwrap_parens(value), table)),
        _ => false,
    }
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

/// The name of the column `expr` refers to, written as `column` or
/// `table.column`. Virtual generated columns are rejected.
fn column_of(expr: &ast::Expr, table: &BTreeTable) -> Result<Option<String>> {
    let name = match expr {
        ast::Expr::Id(name) => name,
        ast::Expr::Qualified(qualifier, name)
            if normalize_ident(qualifier.as_str()) == normalize_ident(&table.name) =>
        {
            name
        }
        _ => return Ok(None),
    };
    let Some((_, column)) = table.get_column(name.as_str()) else {
        bail_parse_error!("no such column: {}", name.as_str());
    };
    if column.is_virtual_generated() {
        bail_parse_error!(
            "generated column \"{}\" cannot be a policy owner column",
            name.as_str()
        );
    }
    Ok(Some(normalize_ident(name.as_str())))
}

fn has_access_control(table_name: &str, resolver: &Resolver) -> bool {
    let catalog = catalog(resolver);
    catalog.has_row_security(table_name) || !catalog.policies(table_name).is_empty()
}

fn main_btree_table(name: &ast::QualifiedName, resolver: &Resolver) -> Result<Arc<BTreeTable>> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(name)?;
    if database_id != MAIN_DB_ID {
        bail_parse_error!("row-level security is only supported for tables in the main database");
    }
    match resolver.schema().get_btree_table(name.name.as_str()) {
        Some(table) => Ok(table),
        None => bail_parse_error!("no such table: {}", name.name.as_str()),
    }
}

fn catalog(resolver: &Resolver) -> Arc<AccessControlCatalog> {
    resolver.with_schema(MAIN_DB_ID, |schema| schema.access_control.clone())
}

/// Bumps the schema cookie and then applies `change` to the in-memory catalog.
/// The cookie goes first because writing it is what fails when the
/// transaction cannot change the schema, and a failed statement must not
/// leave the in-memory catalog changed.
fn emit_catalog_update(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    change: AccessControlChange,
) {
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });
    program.emit_insn(Insn::UpdateAccessControl {
        db: MAIN_DB_ID,
        change: Box::new(change),
    });
}

/// Write cursor on `__turso_internal_access_control`, which is created on
/// first use.
struct AccessControlRows {
    cursor_id: usize,
}

impl AccessControlRows {
    fn open(resolver: &Resolver, program: &mut ProgramBuilder) -> Result<Self> {
        let (table, root_page) = match resolver.schema().get_btree_table(ACCESS_CONTROL_TABLE_NAME)
        {
            Some(table) => {
                let root_page = RegisterOrLiteral::Literal(table.root_page);
                (table, root_page)
            }
            None => Self::emit_create_table(resolver, program)?,
        };
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page,
            db: MAIN_DB_ID,
        });
        Ok(Self { cursor_id })
    }

    fn emit_create_table(
        resolver: &Resolver,
        program: &mut ProgramBuilder,
    ) -> Result<(Arc<BTreeTable>, RegisterOrLiteral<i64>)> {
        let root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: MAIN_DB_ID,
            root: root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let schema_table = resolver
            .schema()
            .get_btree_table(SQLITE_TABLEID)
            .expect("sqlite_schema exists");
        let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: schema_cursor_id,
            root_page: 1i64.into(),
            db: MAIN_DB_ID,
        });
        emit_schema_entry(
            program,
            resolver,
            schema_cursor_id,
            None,
            SchemaEntryType::Table,
            ACCESS_CONTROL_TABLE_NAME,
            ACCESS_CONTROL_TABLE_NAME,
            root_reg,
            Some(ACCESS_CONTROL_TABLE_SQL.to_string()),
        )?;
        program.emit_insn(Insn::ParseSchema {
            db: schema_cursor_id,
            where_clause: Some(format!(
                "tbl_name = '{ACCESS_CONTROL_TABLE_NAME}' AND type != 'trigger'"
            )),
            trigger_target_database_id: None,
        });
        let table = Arc::new(BTreeTable::from_sql(ACCESS_CONTROL_TABLE_SQL, 0)?);
        Ok((table, RegisterOrLiteral::Register(root_reg)))
    }

    fn emit_insert(&self, program: &mut ProgramBuilder, values: &[&str]) {
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor_id,
            rowid_reg,
            prev_largest_reg: 0,
        });
        let first_reg = program.alloc_registers(values.len());
        for (i, value) in values.iter().enumerate() {
            program.emit_insn(Insn::String8 {
                dest: first_reg + i,
                value: value.to_string(),
            });
        }
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(first_reg),
            count: to_u32(values.len()),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.cursor_id,
            key_reg: rowid_reg,
            record_reg,
            flag: InsertFlags::new(),
            table_name: ACCESS_CONTROL_TABLE_NAME.to_string(),
        });
    }

    /// Deletes every row whose columns equal the given `(value, column)` pairs.
    fn emit_delete(&self, program: &mut ProgramBuilder, matches: &[(&str, usize)]) {
        let done = program.allocate_label();
        let loop_start = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor_id,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_start);
        let next = program.allocate_label();
        for (value, column) in matches {
            let column_reg = program.alloc_register();
            program.emit_column_or_rowid(self.cursor_id, *column, column_reg);
            let value_reg = program.emit_string8_new_reg(value.to_string());
            program.emit_insn(Insn::Ne {
                lhs: column_reg,
                rhs: value_reg,
                target_pc: next,
                flags: CmpInsFlags::default(),
                collation: None,
            });
        }
        program.emit_insn(Insn::Delete {
            cursor_id: self.cursor_id,
            table_name: ACCESS_CONTROL_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });
        program.preassign_label_to_next_insn(next);
        program.emit_insn(Insn::Next {
            cursor_id: self.cursor_id,
            pc_if_next: loop_start,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(done);
    }
}
