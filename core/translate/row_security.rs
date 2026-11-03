use std::sync::Arc;

use turso_parser::ast;

use crate::{
    bail_parse_error,
    namespace::{mangle_name, NamespaceId, NamespaceRef},
    schema::{BTreeTable, Table},
    security::row_security::{
        Policy, RowSecurityCatalog, RowSecurityChange, RowSecurityRow,
        CREATE_ROW_SECURITY_TABLE_SQL, ROW_SECURITY_TABLE_NAME,
    },
    storage::pager::CreateBTreeFlags,
    translate::{
        collate::CollationSeq,
        emitter::Resolver,
        expr::{bind_and_rewrite_expr, walk_expr, BindingBehavior, WalkControl},
        plan::{JoinOrigin, JoinedTable, TableReferences, WhereTerm, WhereTermOrigin},
        schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
    },
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral},
    },
    Result, MAIN_DB_ID,
};

pub fn translate_row_security_change(
    namespace: NamespaceRef,
    table_name: &str,
    enable: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !resolver.catalog.dialect.supports_roles() {
        bail_parse_error!("row-level security is not supported");
    }
    reject_table_outside_main(namespace)?;
    let table = stored_table_name(namespace, table_name);
    if catalog(resolver).has_row_security(&table) == enable {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    if enable {
        rows.emit_insert(program, &RowSecurityRow::for_row_security(&table));
    } else {
        rows.emit_delete(program, &[("row_security", 0), (&table, 2)]);
    }
    emit_catalog_update(
        program,
        resolver,
        RowSecurityChange::SetRowSecurity {
            table,
            enabled: enable,
        },
    );
    Ok(())
}

/// CREATE POLICY. The policy's conditions may refer only to columns of the
/// table. Policies for INSERT, UPDATE and DELETE and WITH CHECK conditions
/// are stored but have no effect yet, because roles cannot write to tables
/// with row-level security.
pub fn translate_create_policy(
    policy: &ast::CreatePolicy,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let (namespace, table) = main_btree_table(&policy.tbl_name, resolver)?;
    let stored_name = stored_table_name(namespace, &table.declared_name);
    let name = normalize_ident(policy.policy_name.as_str());
    if catalog(resolver)
        .policies(&stored_name)
        .iter()
        .any(|existing| existing.name == name)
    {
        bail_parse_error!(
            "policy \"{name}\" for table \"{}\" already exists",
            table.declared_name
        );
    }
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
    for role in &policy.roles {
        if resolver.schema().roles.get_by_name(role.as_str()).is_none() {
            bail_parse_error!("role \"{}\" does not exist", role.as_str());
        }
    }

    let row = RowSecurityRow::for_policy(&stored_name, policy);
    let change = RowSecurityChange::from_row(&row)?;
    let RowSecurityChange::CreatePolicy { policy, .. } = &change else {
        unreachable!("a policy row is a policy");
    };
    for expr in [&policy.using_expr, &policy.check_expr]
        .into_iter()
        .flatten()
    {
        check_policy_expr(expr, &table)?;
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    rows.emit_insert(program, &row);
    emit_catalog_update(program, resolver, change);
    Ok(())
}

pub fn translate_drop_policy(
    policy_name: &ast::Name,
    tbl_name: &ast::QualifiedName,
    if_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let (namespace, table) = main_btree_table(tbl_name, resolver)?;
    let stored_name = stored_table_name(namespace, &table.declared_name);
    let name = normalize_ident(policy_name.as_str());
    let exists = catalog(resolver)
        .policies(&stored_name)
        .iter()
        .any(|policy| policy.name == name);
    if !exists {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!(
            "policy \"{name}\" for table \"{}\" does not exist",
            table.declared_name
        );
    }
    let table = stored_name;
    let rows = RowSecurityRows::open(resolver, program)?;
    rows.emit_delete(program, &[("policy", 0), (&name, 1), (&table, 2)]);
    emit_catalog_update(
        program,
        resolver,
        RowSecurityChange::DropPolicy { table, name },
    );
    Ok(())
}

/// DROP TABLE removes the table's row-level security and policies.
pub fn emit_drop_table_row_security_cleanup(
    table_name: &str,
    namespace: NamespaceRef,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let table = stored_table_name(namespace, table_name);
    if namespace.database_id != MAIN_DB_ID || !has_row_security(&table, resolver) {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    rows.emit_delete(program, &[("row_security", 0), (&table, 2)]);
    rows.emit_delete(program, &[("policy", 0), (&table, 2)]);
    emit_catalog_update(program, resolver, RowSecurityChange::DropTable(table));
    Ok(())
}

pub fn reject_rename_of_table_with_row_security(
    table_name: &str,
    namespace: NamespaceRef,
    resolver: &Resolver,
) -> Result<()> {
    if namespace.database_id == MAIN_DB_ID
        && has_row_security(&stored_table_name(namespace, table_name), resolver)
    {
        bail_parse_error!(
            "cannot rename table \"{table_name}\": renaming tables with row-level security or policies is not supported"
        );
    }
    Ok(())
}

/// Policies refer to columns by name, so a column that a policy uses cannot
/// be renamed, dropped or redefined.
pub fn reject_change_of_policy_column(
    table_name: &str,
    namespace: NamespaceRef,
    column_name: &str,
    resolver: &Resolver,
) -> Result<()> {
    if namespace.database_id != MAIN_DB_ID {
        return Ok(());
    }
    let column_name = normalize_ident(column_name);
    let stored_name = stored_table_name(namespace, table_name);
    let used_by_policy = catalog(resolver)
        .policies(&stored_name)
        .iter()
        .any(|policy| policy_uses_column(policy, &column_name));
    if used_by_policy {
        bail_parse_error!(
            "cannot change column \"{column_name}\" of table \"{table_name}\": a row-level security policy uses it"
        );
    }
    Ok(())
}

/// A role sees only the rows of a table with row-level security that the
/// table's policies for that role show. Every such table in the FROM clause
/// gets a filter that combines the USING conditions of the permissive
/// policies with OR and those of the restrictive policies with AND, as
/// PostgreSQL does: a row must pass at least one permissive policy and every
/// restrictive one. Without a permissive policy no row passes. For the right
/// side of an outer join the filter is part of the join condition, so the
/// hidden rows produce NULLs like rows that do not exist. The filters go before the query's own
/// conditions, so no expression of the query sees a hidden row. The tables
/// are recorded so the optimizer does not choose access methods that
/// evaluate the query's expressions on a row before its filter.
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
        if !row_security_applies(&btree.declared_name, table.namespace, resolver)? {
            continue;
        }
        if table_references
            .joined_tables()
            .iter()
            .any(|table| table.join_info.as_ref().is_some_and(|j| j.is_full_outer()))
        {
            bail_parse_error!(
                "FULL JOIN with table \"{}\" that has row-level security is not supported",
                btree.declared_name
            );
        }
        let role = resolver
            .row_security_role
            .as_deref()
            .expect("row security applies to roles");
        let catalog = catalog(resolver);
        let policies = catalog.policies(&stored_table_name(table.namespace, &btree.declared_name));
        let mut permissive = None;
        let mut restrictive = None;
        for policy in policies
            .iter()
            .filter(|policy| policy.applies_to_reads() && policy.applies_to_role(role))
        {
            let Some(using_expr) = &policy.using_expr else {
                continue;
            };
            let shown = bind_policy_expr(using_expr, table, btree, resolver)?;
            collect_used_columns(&shown, table.internal_id, &mut used_columns)?;
            if policy.permissive {
                permissive = Some(combine(permissive, ast::Operator::Or, shown));
            } else {
                restrictive = Some(combine(restrictive, ast::Operator::And, shown));
            }
        }
        let visible = match (permissive, restrictive) {
            (None, _) => ast::Expr::Literal(ast::Literal::Numeric("0".to_string())),
            (Some(permissive), None) => permissive,
            (Some(permissive), Some(restrictive)) => {
                combine(Some(permissive), ast::Operator::And, restrictive)
            }
        };
        filtered_tables.push(table.internal_id);
        let is_outer = table
            .join_info
            .as_ref()
            .is_some_and(|join_info| join_info.is_outer());
        filters.push(WhereTerm {
            expr: visible,
            origin: if is_outer {
                WhereTermOrigin::Join(JoinOrigin::Outer(table.internal_id))
            } else {
                WhereTermOrigin::Where
            },
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

fn combine(left: Option<ast::Expr>, operator: ast::Operator, right: ast::Expr) -> ast::Expr {
    match left {
        None => right,
        Some(left) => ast::Expr::Binary(Box::new(left), operator, Box::new(right)),
    }
}

/// Binds a policy condition to the table it protects. The condition sees
/// only that table, under its own name, whatever alias the query gives it.
fn bind_policy_expr(
    expr: &ast::Expr,
    table: &JoinedTable,
    btree: &BTreeTable,
    resolver: &Resolver,
) -> Result<ast::Expr> {
    let mut policy_table = table.clone();
    policy_table.identifier = normalize_ident(&btree.declared_name);
    policy_table.has_alias = false;
    policy_table.join_info = None;
    let mut table_references = TableReferences::new(vec![policy_table], Vec::new());
    let mut expr = expr.clone();
    bind_and_rewrite_expr(
        &mut expr,
        Some(&mut table_references),
        None,
        resolver,
        BindingBehavior::ResultColumnsNotAllowed,
    )?;
    Ok(expr)
}

fn collect_used_columns(
    expr: &ast::Expr,
    internal_id: ast::TableInternalId,
    used_columns: &mut Vec<(ast::TableInternalId, usize)>,
) -> Result<()> {
    walk_expr(expr, &mut |expr| {
        if let ast::Expr::Column { table, column, .. } = expr {
            if *table == internal_id {
                used_columns.push((*table, *column));
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// A role cannot write to a table with row-level security yet.
pub fn reject_write_with_row_security(
    table_name: &str,
    namespace: NamespaceRef,
    resolver: &Resolver,
) -> Result<()> {
    if row_security_applies(table_name, namespace, resolver)? {
        bail_parse_error!(
            "writing to table \"{table_name}\" with row-level security is not supported for roles"
        );
    }
    Ok(())
}

/// Virtual tables that read the database file directly would show the rows
/// row-level security hides.
pub fn reject_raw_storage_for_roles(table_name: &str, resolver: &Resolver) -> Result<()> {
    if resolver.row_security_role.is_some()
        && ["sqlite_dbpage", "btree_dump"]
            .iter()
            .any(|name| name.eq_ignore_ascii_case(table_name))
    {
        bail_parse_error!("permission denied: {table_name} requires the superuser");
    }
    Ok(())
}

/// Whether row-level security restricts what the statement being compiled
/// may do with `table_name`. The catalog only covers the main database.
/// Tables of attached databases that have their own catalog are rejected
/// instead of being treated as unprotected.
fn row_security_applies(
    table_name: &str,
    namespace: NamespaceRef,
    resolver: &Resolver,
) -> Result<bool> {
    if resolver.row_security_role.is_none() {
        return Ok(false);
    }
    if namespace.database_id != MAIN_DB_ID {
        let has_catalog = resolver.with_schema(namespace.database_id, |schema| {
            schema
                .get_btree_table(NamespaceId::ROOT, ROW_SECURITY_TABLE_NAME)
                .is_some()
        });
        if has_catalog {
            bail_parse_error!(
                "table \"{table_name}\" is in an attached database with row-level security, which is not supported"
            );
        }
        return Ok(false);
    }
    Ok(catalog(resolver).has_row_security(&stored_table_name(namespace, table_name)))
}

fn reject_table_outside_main(namespace: NamespaceRef) -> Result<()> {
    if namespace.database_id != MAIN_DB_ID {
        bail_parse_error!("row-level security is only supported for tables in the main database");
    }
    Ok(())
}

/// The name the catalog knows a table by: the name it is stored under in the
/// main database, which tells apart tables of the same name in different
/// schemas.
fn stored_table_name(namespace: NamespaceRef, table_name: &str) -> String {
    mangle_name(namespace.id, &normalize_ident(table_name))
}

/// A policy condition may refer to the columns of its table, written as
/// `column` or `table.column`, and call functions. Subqueries are not
/// supported, except `(SELECT <expr>)` without FROM, which the catalog has
/// already replaced by its expression. Virtual generated columns are not
/// supported either.
fn check_policy_expr(expr: &ast::Expr, table: &BTreeTable) -> Result<()> {
    walk_expr(expr, &mut |expr| {
        let column_name = match expr {
            ast::Expr::Id(name) => name,
            ast::Expr::Qualified(qualifier, name) => {
                if normalize_ident(qualifier.as_str()) != normalize_ident(&table.declared_name) {
                    bail_parse_error!(
                        "a policy of table \"{}\" can only refer to the columns of that table",
                        table.declared_name
                    );
                }
                name
            }
            ast::Expr::DoublyQualified(..) => bail_parse_error!(
                "a policy of table \"{}\" can only refer to the columns of that table",
                table.declared_name
            ),
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries in policies are not supported")
            }
            ast::Expr::Variable(_) => bail_parse_error!("parameters in policies are not supported"),
            _ => return Ok(WalkControl::Continue),
        };
        let Some((_, column)) = table.get_column(column_name.as_str()) else {
            bail_parse_error!("no such column: {}", column_name.as_str());
        };
        if column.is_virtual_generated() {
            bail_parse_error!(
                "policies cannot use generated column \"{}\"",
                column_name.as_str()
            );
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn policy_uses_column(policy: &Policy, column_name: &str) -> bool {
    let mut uses_column = false;
    for expr in [&policy.using_expr, &policy.check_expr]
        .into_iter()
        .flatten()
    {
        walk_expr(expr, &mut |expr| {
            if let ast::Expr::Id(name) | ast::Expr::Qualified(_, name) = expr {
                uses_column |= normalize_ident(name.as_str()) == column_name;
            }
            Ok(WalkControl::Continue)
        })
        .expect("the walk never fails");
    }
    uses_column
}

/// Whether the table has row-level security or policies.
fn has_row_security(table_name: &str, resolver: &Resolver) -> bool {
    let catalog = catalog(resolver);
    catalog.has_row_security(table_name) || !catalog.policies(table_name).is_empty()
}

fn main_btree_table(
    name: &ast::QualifiedName,
    resolver: &Resolver,
) -> Result<(NamespaceRef, Arc<BTreeTable>)> {
    if !resolver.catalog.dialect.supports_roles() {
        bail_parse_error!("row-level security is not supported");
    }
    let namespace = resolver.resolve_existing_table(name)?;
    reject_table_outside_main(namespace)?;
    match resolver
        .schema()
        .get_btree_table(namespace.id, name.name.as_str())
    {
        Some(table) => Ok((namespace, table)),
        None => bail_parse_error!("no such table: {}", name.name.as_str()),
    }
}

fn catalog(resolver: &Resolver) -> Arc<RowSecurityCatalog> {
    resolver.with_schema(MAIN_DB_ID, |schema| schema.row_security.clone())
}

/// Bumps the schema cookie and then applies `change` to the in-memory catalog.
/// The cookie goes first because writing it is what fails when the
/// transaction cannot change the schema, and a failed statement must not
/// leave the in-memory catalog changed.
fn emit_catalog_update(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    change: RowSecurityChange,
) {
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });
    program.emit_insn(Insn::UpdateRowSecurity {
        db: MAIN_DB_ID,
        change: Box::new(change),
    });
}

/// Write cursor on `__turso_internal_row_security`, which is created on
/// first use.
struct RowSecurityRows {
    cursor_id: usize,
}

impl RowSecurityRows {
    fn open(resolver: &Resolver, program: &mut ProgramBuilder) -> Result<Self> {
        let (table, root_page) = match resolver
            .schema()
            .get_btree_table(NamespaceId::ROOT, ROW_SECURITY_TABLE_NAME)
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
            .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
            .expect("sqlite_schema always exists");
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
            ROW_SECURITY_TABLE_NAME,
            ROW_SECURITY_TABLE_NAME,
            root_reg,
            Some(CREATE_ROW_SECURITY_TABLE_SQL.to_string()),
        )?;
        program.emit_insn(Insn::ParseSchema {
            db: schema_cursor_id,
            where_clause: Some(format!(
                "tbl_name = '{ROW_SECURITY_TABLE_NAME}' AND type != 'trigger'"
            )),
            trigger_target_database_id: None,
        });
        let table = Arc::new(BTreeTable::from_sql(CREATE_ROW_SECURITY_TABLE_SQL, 0)?);
        Ok((table, RegisterOrLiteral::Register(root_reg)))
    }

    fn emit_insert(&self, program: &mut ProgramBuilder, row: &RowSecurityRow) {
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor_id,
            rowid_reg,
            prev_largest_reg: 0,
        });
        // Column positions as in CREATE_ROW_SECURITY_TABLE_SQL.
        const COLUMN_COUNT: usize = 8;
        const PERMISSIVE_COLUMN: usize = 3;
        let texts = [
            (0, &row.kind),
            (1, &row.name),
            (2, &row.tbl_name),
            (4, &row.command),
            (5, &row.roles),
            (6, &row.using_expr),
            (7, &row.check_expr),
        ];
        let first_reg = program.alloc_registers(COLUMN_COUNT);
        for (column, text) in texts {
            program.emit_insn(Insn::String8 {
                dest: first_reg + column,
                value: text.clone(),
            });
        }
        program.emit_insn(Insn::Integer {
            value: i64::from(row.permissive),
            dest: first_reg + PERMISSIVE_COLUMN,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(first_reg),
            count: to_u32(COLUMN_COUNT),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.cursor_id,
            key_reg: rowid_reg,
            record_reg,
            flag: InsertFlags::new().skip_last_rowid(),
            table_name: ROW_SECURITY_TABLE_NAME.to_string(),
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
                collation: CollationSeq::for_internal_values(),
            });
        }
        program.emit_insn(Insn::Delete {
            cursor_id: self.cursor_id,
            table_name: ROW_SECURITY_TABLE_NAME.to_string(),
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
