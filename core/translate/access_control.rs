use std::sync::Arc;

use turso_parser::ast;

use crate::{
    access_control::{
        AccessControlCatalog, AccessControlChange, ACCESS_CONTROL_TABLE_NAME,
        ACCESS_CONTROL_TABLE_SQL,
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

/// DROP TABLE removes the table's row-level security.
pub fn emit_drop_table_access_control_cleanup(
    table_name: &str,
    database_id: usize,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if database_id != MAIN_DB_ID || !catalog(resolver).has_row_security(table_name) {
        return Ok(());
    }
    let table = normalize_ident(table_name);
    let rows = AccessControlRows::open(resolver, program)?;
    rows.emit_delete(program, &[("row_security", 0), (&table, 2)]);
    emit_catalog_update(program, resolver, AccessControlChange::DropTable(table));
    Ok(())
}

pub fn reject_rename_of_table_with_row_security(
    table_name: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    if database_id == MAIN_DB_ID && catalog(resolver).has_row_security(table_name) {
        bail_parse_error!(
            "cannot rename table \"{table_name}\": renaming tables with row-level security is not supported"
        );
    }
    Ok(())
}

/// A role sees no rows of a table with row-level security: every such table
/// in the FROM clause gets a filter that is always false. For the right side
/// of an outer join the filter is part of the join condition, so the hidden
/// rows produce NULLs like rows that do not exist. The tables are recorded
/// so the optimizer does not choose access methods that evaluate the query's
/// expressions on a row before its filter.
pub fn add_select_row_security_filters(
    table_references: &mut TableReferences,
    where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver,
) -> Result<()> {
    let mut filters = Vec::new();
    let mut filtered_tables = Vec::new();
    for table in table_references.joined_tables() {
        let Table::BTree(btree) = &table.table else {
            continue;
        };
        if !row_security_applies(&btree.name, table.database_id, resolver)? {
            continue;
        }
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
        filtered_tables.push(table.internal_id);
        filters.push(WhereTerm {
            expr: ast::Expr::Literal(ast::Literal::Numeric("0".to_string())),
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
    where_clause.splice(0..0, filters);
    Ok(())
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
