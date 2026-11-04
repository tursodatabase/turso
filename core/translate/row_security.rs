//! Row-level security limits which rows of a table a role may access.
//!
//! The statements that read and write rows call the hooks of this module,
//! which ask [`PolicyCatalog::policy_for`] which rows the role may access and
//! limit the statement to them. The role is the one the statement is compiled
//! for, [`Resolver::row_security_role`].
//!
//! The module also compiles the statements that enable and disable row-level
//! security, and keeps the stored tables with row-level security up to date
//! when a table is renamed or dropped.

use std::sync::Arc;

use turso_parser::ast;

use crate::{
    bail_parse_error,
    namespace::{NamespaceId, NamespaceRef},
    schema::{BTreeTable, Table},
    security::permission::{Action, Object, PermissionDenied},
    security::policy::{
        Command, Policy, PolicyCatalog, CREATE_ROW_SECURITY_TABLE_SQL, ROW_SECURITY_TABLE_NAME,
    },
    translate::{
        collate::CollationSeq,
        emitter::Resolver,
        plan::{JoinOrigin, TableReferences, WhereTerm, WhereTermOrigin},
        schema::{emit_create_internal_table, emit_schema_cookie},
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral},
    },
    vtab::{VirtualTable, VirtualTableType},
    Connection, LimboError, Result, MAIN_DB_ID,
};

/// Limits the rows of the tables in `table_references` that the statement
/// reads, updates or deletes to the rows the role may access, by adding a
/// filter to `where_clause` for each table that the role may not access in
/// full.
///
/// For the right side of a LEFT JOIN the filter is part of the join
/// condition, so a hidden row reads as NULLs like a row that does not exist.
/// The filters go before the statement's own conditions.
pub(crate) fn filter_rows(
    command: Command,
    table_references: &TableReferences,
    where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver,
) -> Result<()> {
    let mut filters = Vec::new();
    for table in table_references.joined_tables() {
        let Table::BTree(btree) = &table.table else {
            continue;
        };
        match policy(resolver, table.namespace, &btree.declared_name, command) {
            Policy::Allow => continue,
            Policy::Deny => {}
        }
        reject_joins_that_keep_right_rows(table_references, &btree.declared_name)?;
        let is_outer = table
            .join_info
            .as_ref()
            .is_some_and(|join_info| join_info.is_outer());
        filters.push(WhereTerm {
            expr: no_rows(),
            origin: if is_outer {
                WhereTermOrigin::Join(JoinOrigin::Outer(table.internal_id))
            } else {
                WhereTermOrigin::Where
            },
            consumed: false,
        });
    }
    where_clause.splice(0..0, filters);
    Ok(())
}

/// Checks that the role may insert rows into `table`.
pub(crate) fn check_insert(
    table: &BTreeTable,
    namespace: NamespaceRef,
    resolver: &Resolver,
) -> Result<()> {
    match policy(resolver, namespace, &table.declared_name, Command::Insert) {
        Policy::Allow => Ok(()),
        Policy::Deny => Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::policy(Object::Table(table.declared_name.clone())),
        ))),
    }
}

/// Checks that the role may read the virtual table. A virtual table that
/// shows the database file would show the rows that row-level security
/// hides, so only a superuser may read it.
pub(crate) fn check_virtual_table(table: &VirtualTable, resolver: &Resolver) -> Result<()> {
    let reads_raw_storage = matches!(
        &table.vtab_type,
        VirtualTableType::Internal(internal) if internal.read().reads_raw_storage()
    );
    if reads_raw_storage
        && !resolver
            .schema()
            .roles
            .is_superuser(resolver.row_security_role)
    {
        return Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::privilege(Action::Read, Object::Table(table.name.clone())),
        )));
    }
    Ok(())
}

/// Checks that the current role may open a BLOB handle on a row of `table`
/// of the root namespace, which reads and writes the row without a
/// statement.
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
    let policy = connection.with_schema(MAIN_DB_ID, |schema| {
        schema.policies.policy_for(
            &schema.roles,
            connection.current_role(),
            NamespaceId::ROOT,
            table_name,
            command,
        )
    });
    match policy {
        Policy::Allow => Ok(()),
        Policy::Deny => Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::privilege(action, Object::Table(table_name.to_string())),
        ))),
    }
}

/// Which rows of table `table_name` of `namespace` the role may access for
/// `command`. The policies are those of the table's own database, so a table
/// of an attached database with row-level security is limited too. The roles
/// are those of the main database, which the role was set from.
fn policy(
    resolver: &Resolver,
    namespace: NamespaceRef,
    table_name: &str,
    command: Command,
) -> Policy {
    let roles = resolver.schema().roles.clone();
    resolver.with_schema(namespace.database_id, |schema| {
        schema.policies.policy_for(
            &roles,
            resolver.row_security_role,
            namespace.id,
            table_name,
            command,
        )
    })
}

/// A RIGHT or FULL JOIN emits the unmatched rows of its right side after the
/// join loop, without the filters of the tables on its left. A filter on
/// either side would then hide too many rows or too few.
fn reject_joins_that_keep_right_rows(
    table_references: &TableReferences,
    table_name: &str,
) -> Result<()> {
    for join_info in table_references
        .joined_tables()
        .iter()
        .filter_map(|table| table.join_info.as_ref())
    {
        if join_info.is_full_outer() {
            bail_parse_error!(
                "FULL JOIN with table \"{table_name}\" that has row-level security is not supported"
            );
        }
        if join_info.keeps_right_rows() {
            bail_parse_error!(
                "RIGHT JOIN with table \"{table_name}\" that has row-level security is not supported"
            );
        }
    }
    Ok(())
}

/// A condition that no row passes.
fn no_rows() -> ast::Expr {
    ast::Expr::Literal(ast::Literal::Numeric("0".to_string()))
}

/// ALTER TABLE ... ENABLE ROW LEVEL SECURITY, or DISABLE when `enabled` is
/// false. Enabling it for a table that already has it, or disabling it for a
/// table that does not, changes nothing.
pub fn translate_set_row_security(
    namespace: NamespaceRef,
    table_name: &str,
    enabled: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !resolver.catalog.dialect.supports_roles() {
        bail_parse_error!("row-level security is not supported");
    }
    if namespace.database_id != MAIN_DB_ID {
        bail_parse_error!("row-level security is only supported for tables in the main database");
    }
    if resolver
        .schema()
        .policies
        .is_enabled(namespace.id, table_name)
        == enabled
    {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    let stored_name = PolicyCatalog::stored_table_name(namespace.id, table_name);
    if enabled {
        rows.emit_insert(program, &stored_name);
    } else {
        rows.emit_delete(program, &stored_name);
    }
    emit_catalog_update(program, resolver, namespace.id, table_name, enabled);
    Ok(())
}

/// Moves the row-level security of a renamed table to its new name.
pub fn emit_rename_table(
    namespace: NamespaceRef,
    old_name: &str,
    new_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !has_row_security(namespace, old_name, resolver) {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    rows.emit_delete(
        program,
        &PolicyCatalog::stored_table_name(namespace.id, old_name),
    );
    rows.emit_insert(
        program,
        &PolicyCatalog::stored_table_name(namespace.id, new_name),
    );
    emit_catalog_update(program, resolver, namespace.id, old_name, false);
    emit_catalog_update(program, resolver, namespace.id, new_name, true);
    Ok(())
}

/// Removes the row-level security of a dropped table, so that a new table
/// with the same name does not start with it.
pub fn emit_drop_table(
    namespace: NamespaceRef,
    table_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !has_row_security(namespace, table_name, resolver) {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    rows.emit_delete(
        program,
        &PolicyCatalog::stored_table_name(namespace.id, table_name),
    );
    emit_catalog_update(program, resolver, namespace.id, table_name, false);
    Ok(())
}

fn has_row_security(namespace: NamespaceRef, table_name: &str, resolver: &Resolver) -> bool {
    namespace.database_id == MAIN_DB_ID
        && resolver
            .schema()
            .policies
            .is_enabled(namespace.id, table_name)
}

/// Bumps the schema cookie and then changes the in-memory catalog. The cookie
/// goes first because writing it is what fails when the transaction cannot
/// change the schema, and a failed statement must not leave the in-memory
/// catalog changed.
fn emit_catalog_update(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    namespace: NamespaceId,
    table_name: &str,
    enabled: bool,
) {
    emit_schema_cookie(resolver, program);
    program.emit_insn(Insn::SetRowSecurity {
        db: MAIN_DB_ID,
        namespace,
        table_name: table_name.to_string(),
        enabled,
    });
}

/// A write cursor on `__turso_internal_row_security`, which is created on
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
            None => {
                let created = emit_create_internal_table(
                    program,
                    resolver,
                    ROW_SECURITY_TABLE_NAME,
                    CREATE_ROW_SECURITY_TABLE_SQL,
                    &[],
                )?;
                (created.table, created.root_page)
            }
        };
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page,
            db: MAIN_DB_ID,
        });
        Ok(Self { cursor_id })
    }

    fn emit_insert(&self, program: &mut ProgramBuilder, table_name: &str) {
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor_id,
            rowid_reg,
            prev_largest_reg: 0,
        });
        let table_name_reg = program.emit_string8_new_reg(table_name.to_string());
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(table_name_reg),
            count: to_u32(1),
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

    /// Deletes every row of the table stored as `table_name`.
    fn emit_delete(&self, program: &mut ProgramBuilder, table_name: &str) {
        let done = program.allocate_label();
        let loop_start = program.allocate_label();
        let next = program.allocate_label();
        let table_name_reg = program.emit_string8_new_reg(table_name.to_string());
        let column_reg = program.alloc_register();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor_id,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_start);
        program.emit_column_or_rowid(self.cursor_id, 0, column_reg);
        program.emit_insn(Insn::Ne {
            lhs: column_reg,
            rhs: table_name_reg,
            target_pc: next,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustc_hash::FxHashMap;
    use turso_parser::ast;

    use super::{policy, Command, Policy};
    use crate::dialect::EngineDialect;
    use crate::namespace::{NamespaceId, NamespaceRef};
    use crate::schema::Schema;
    use crate::security::roles::RoleCatalog;
    use crate::translate::emitter::{DoubleQuotedDml, Resolver};
    use crate::{Database, DatabaseCatalog, MemoryIO, Qualifiers, RwLock, SymbolTable, Value};

    /// The policies of a table of an attached database are those of the
    /// attached database, not of the main database.
    #[test]
    fn table_of_an_attached_database_with_row_security_denies_a_role() {
        let roles = RoleCatalog::new(&[vec![
            Value::from_i64(1),
            Value::build_text("alice"),
            Value::from_i64(0),
        ]])
        .unwrap();
        let alice = roles.get_by_name("alice").unwrap().id;
        let mut main = Schema::default();
        main.roles = Arc::new(roles);
        let mut attached = Schema::default();
        Arc::make_mut(&mut attached.policies).enable(NamespaceId::ROOT, "t");
        let attached_database_id = 2;
        let mut database_schemas = FxHashMap::default();
        database_schemas.insert(attached_database_id, Arc::new(attached));
        let database_schemas = RwLock::new(database_schemas);
        let temp_database = RwLock::new(None);
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let qualifiers = Qualifiers::default();
        let syms = SymbolTable::new();
        let resolver = Resolver::new(
            &main,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &qualifiers,
            &syms,
            true,
            DoubleQuotedDml::Enabled,
            Arc::new(EngineDialect),
            None,
            alice,
        );

        let policy = policy(
            &resolver,
            NamespaceRef::root_of(attached_database_id),
            "t",
            Command::Select,
        );

        assert_eq!(policy, Policy::Deny);
    }

    #[test]
    fn enabling_row_security_fails_under_a_dialect_without_roles() {
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
