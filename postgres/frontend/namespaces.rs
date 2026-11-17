//! Privileges on schemas, PostgreSQL's `pg_namespace.nspacl`, in the
//! `__turso_internal_namespace_acls` table of the main database.
//!
//! A schema has two privileges: `USAGE` lets a role look up the objects in
//! the schema, and `CREATE` lets it make new ones there. The engine stores
//! the schemas themselves, as namespaces; this table holds the access
//! control list of each schema that a `GRANT` or `REVOKE` has changed, under
//! the namespace's id. A schema without a row has its default list.
//!
//! Only a superuser creates a schema, so every schema is owned as
//! `pg_namespace.nspowner` shows it: `public` by `pg_database_owner`, any
//! other schema by the built-in superuser.
//!
//! The frontend creates the table when it opens a database. The engine loads
//! the rows as part of the frontend's schema state, see
//! [`crate::schema_state`]. `GRANT` and `REVOKE` write a row and update the
//! in-memory lists in the same statement, and `DROP SCHEMA` deletes the row
//! of the schema.

use std::collections::BTreeMap;
use std::sync::Arc;

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::security::permission::{Action, Object, PermissionDenied};
use turso_core::security::roles::RoleId;
use turso_core::{
    to_u32, CatalogChange, Connection, CursorType, GrantTarget, InsertFlags, Insn, LimboError,
    Numeric, ProgramBuilder, RegisterOrLiteral, Value, MAIN_DB_ID,
};
use turso_parser::ast;

use crate::acl::{Acl, Privileges, PUBLIC_OID};
use crate::catalog::{role_oid, PG_DATABASE_OWNER_OID};
use crate::classes::resolve_grantee;
use crate::roles::schema_roles;
use crate::schema_state::schema_state;

/// The table that stores the access control lists of schemas.
pub(crate) const NAMESPACE_ACLS_TABLE_NAME: &str = "__turso_internal_namespace_acls";

/// Creates [`NAMESPACE_ACLS_TABLE_NAME`]. The rowid of a row is the id of
/// its namespace, and `nspacl` is the list in the stored form of
/// [`Acl::stored_text`].
pub(crate) const CREATE_NAMESPACE_ACLS_TABLE_SQL: &str =
    "CREATE TABLE __turso_internal_namespace_acls (\
    nspid INTEGER PRIMARY KEY, \
    nspacl TEXT NOT NULL)";

/// The number of columns of [`NAMESPACE_ACLS_TABLE_NAME`].
const NAMESPACE_ACLS_COLUMN_COUNT: usize = 2;

/// The query that reads the lists for the frontend's schema state, or None
/// when the database has no table for them.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    schema.get_btree_table(NamespaceId::ROOT, NAMESPACE_ACLS_TABLE_NAME)?;
    Some(format!(
        "SELECT nspid, nspacl FROM {NAMESPACE_ACLS_TABLE_NAME}"
    ))
}

/// The access control lists that `GRANT` and `REVOKE` stored for the schemas
/// of a database, in memory.
#[derive(Debug, Clone, Default)]
pub(crate) struct NamespaceAcls {
    by_namespace: BTreeMap<NamespaceId, Acl>,
}

impl NamespaceAcls {
    /// Returns the lists of the rows read from the table, in the columns of
    /// [`schema_state_query`].
    pub(crate) fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut acls = Self::default();
        for row in rows {
            let corrupt = || {
                LimboError::Corrupt(format!(
                    "{NAMESPACE_ACLS_TABLE_NAME} has an invalid row: {row:?}"
                ))
            };
            let [Value::Numeric(Numeric::Integer(id)), Value::Text(acl)] = row.as_slice() else {
                return Err(corrupt());
            };
            let id = u32::try_from(*id).map_err(|_| corrupt())?;
            let acl = Acl::parse(acl.as_str()).ok_or_else(corrupt)?;
            acls.by_namespace.insert(NamespaceId::new(id), acl);
        }
        Ok(acls)
    }

    /// Applies what a statement wrote to the stored rows. `values` are what
    /// the emitted instruction read back: the list a grant leaves.
    pub(crate) fn apply(
        &mut self,
        change: &CatalogChange,
        values: &[Value],
    ) -> turso_core::Result<()> {
        match change {
            CatalogChange::Grant {
                target: GrantTarget::Namespace(namespace),
                ..
            } => {
                let [Value::Text(acl)] = values else {
                    return Err(LimboError::InternalError(format!(
                        "a grant on a schema read back {values:?} where its list was written"
                    )));
                };
                let acl = Acl::parse(acl.as_str()).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "a grant on a schema read back an invalid access control list {acl:?}"
                    ))
                })?;
                self.by_namespace.insert(*namespace, acl);
            }
            CatalogChange::DropNamespace { namespace } => {
                self.by_namespace.remove(namespace);
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "{change:?} reached the schema privileges"
                )));
            }
        }
        Ok(())
    }
}

/// The access control list of schema `namespace`, which is called `name`:
/// the stored one, or the default one before any `GRANT` or `REVOKE`. By
/// default the owner has `USAGE` and `CREATE`, and every role also has
/// `USAGE` on `public`, as in PostgreSQL.
pub(crate) fn namespace_acl(schema: &Schema, namespace: NamespaceId, name: &str) -> Acl {
    if let Some(acl) = stored_namespace_acl(schema, namespace) {
        return acl.clone();
    }
    let owner = namespace_owner_oid(name);
    let mut acl = Acl::of_owner(owner, Privileges::SCHEMA);
    if name == PUBLIC_SCHEMA_NAME {
        acl.grant(PUBLIC_OID, Privileges::USAGE, owner);
    }
    acl
}

/// [`turso_core::Dialect::role_has_namespace_privilege`] for the PostgreSQL
/// frontend: a role has the privileges the schema's list gives it, by oid or
/// through PUBLIC, together with those it gives the roles whose privileges
/// the role inherits, and a superuser has every privilege. A namespace
/// without a name is not a schema, and only a superuser uses it.
pub(crate) fn role_has_namespace_privilege(
    schema: &Schema,
    namespace: NamespaceId,
    role: RoleId,
    privilege: ast::Privilege,
) -> bool {
    let roles = schema_roles(schema);
    if roles.is_superuser(role) {
        return true;
    }
    let Some(name) = schema.namespace_name(namespace) else {
        return false;
    };
    let wanted = Privileges::named_among(Privileges::SCHEMA, &[privilege]);
    !wanted.is_empty()
        && namespace_acl(schema, namespace, name).grants(&roles.oids_of_roles_used_by(role), wanted)
}

/// Whether the current role of `conn` has `USAGE` on the schema called
/// `name`. Every role uses the schemas of the system catalogs, which are not
/// namespaces.
pub(crate) fn current_role_may_use_schema(conn: &Connection, name: &str) -> bool {
    let Some(namespace) = conn.qualifier(name) else {
        return true;
    };
    if namespace.is_database() {
        return true;
    }
    let role = conn.current_role();
    conn.with_schema(MAIN_DB_ID, |schema| {
        role_has_namespace_privilege(schema, namespace.target.id, role, ast::Privilege::Usage)
    })
}

/// The access control list that a `GRANT` or `REVOKE` stored for schema
/// `namespace`, None while the schema has its default one.
pub(crate) fn stored_namespace_acl(schema: &Schema, namespace: NamespaceId) -> Option<&Acl> {
    schema_state(schema)?
        .namespace_acls
        .by_namespace
        .get(&namespace)
}

/// The oid of the role that owns the schema called `name`, as
/// `pg_namespace.nspowner` shows it.
pub(crate) fn namespace_owner_oid(name: &str) -> i64 {
    if name == PUBLIC_SCHEMA_NAME {
        PG_DATABASE_OWNER_OID
    } else {
        role_oid(RoleId::SUPERUSER)
    }
}

const PUBLIC_SCHEMA_NAME: &str = "public";

/// `GRANT` and `REVOKE` on schemas, and `DROP SCHEMA`, for
/// [`turso_core::Dialect::translate_catalog_changes`]. `GRANT` adds to the
/// schema's access control list when a role with the owner's privileges
/// runs it; anyone else grants nothing when the list gives it something, as
/// in PostgreSQL, which only warns, and is refused otherwise. `DROP SCHEMA`
/// deletes the stored list.
pub(crate) fn translate_namespace_changes(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    changes: &[CatalogChange],
) -> turso_core::Result<()> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, NAMESPACE_ACLS_TABLE_NAME) else {
        if changes
            .iter()
            .all(|change| matches!(change, CatalogChange::DropNamespace { .. }))
        {
            return Ok(());
        }
        return Err(LimboError::ParseError(
            "the database has no table for the privileges on schemas".to_string(),
        ));
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    for change in changes {
        match change {
            CatalogChange::Grant {
                revoke,
                target: GrantTarget::Namespace(namespace),
                privileges,
                grantees,
            } => {
                let name = schema.namespace_name(*namespace).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "namespace {} of a grant has no name",
                        namespace.as_u32()
                    ))
                })?;
                let grantees = grantees
                    .iter()
                    .map(|grantee| resolve_grantee(schema, connection, grantee))
                    .collect::<turso_core::Result<Vec<_>>>()?;
                let mut acl = namespace_acl(schema, *namespace, name);
                let roles = schema_roles(schema);
                let current = connection.current_role();
                if !roles.has_privileges_of(current, RoleId::SUPERUSER) {
                    if acl.gives_anything_to(&roles.oids_of_roles_used_by(current)) {
                        continue;
                    }
                    return Err(LimboError::PermissionDenied(Box::new(
                        PermissionDenied::privilege(Action::Read, Object::Schema(name.to_string())),
                    )));
                }
                let owner = namespace_owner_oid(name);
                let named = Privileges::named_among(Privileges::SCHEMA, privileges);
                for grantee in &grantees {
                    if *revoke {
                        acl.revoke(*grantee, named, owner);
                    } else {
                        acl.grant(*grantee, named, owner);
                    }
                }
                if stored_namespace_acl(schema, *namespace) == Some(&acl) {
                    continue;
                }
                emit_namespace_acl_write(program, cursor_id, *namespace, &acl, change);
            }
            CatalogChange::DropNamespace { namespace } => {
                emit_namespace_acl_delete(program, cursor_id, *namespace, change);
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "{change:?} reached the schema privileges"
                )));
            }
        }
    }
    Ok(())
}

/// Stores `acl` as the list of `namespace`, in place of the row the
/// namespace has, if any.
fn emit_namespace_acl_write(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    namespace: NamespaceId,
    acl: &Acl,
    change: &CatalogChange,
) {
    let id_reg = emit_namespace_id(program, namespace);
    let insert = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: cursor_id,
        rowid_reg: id_reg,
        target_pc: insert,
    });
    if program.is_mvcc_enabled() {
        program.emit_insn(Insn::Delete {
            cursor_id,
            table_name: NAMESPACE_ACLS_TABLE_NAME.to_string(),
            is_part_of_update: true,
        });
    }
    program.preassign_label_to_next_insn(insert);

    // The id is the rowid, so the record keeps NULL for it, as SQLite does
    // for an INTEGER PRIMARY KEY column.
    let first_column_reg = program.alloc_registers(NAMESPACE_ACLS_COLUMN_COUNT);
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    let acl_reg = first_column_reg + 1;
    program.emit_insn(Insn::String8 {
        value: acl.stored_text(),
        dest: acl_reg,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(NAMESPACE_ACLS_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: id_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_all_change_counts()
            .require_seek(),
        table_name: NAMESPACE_ACLS_TABLE_NAME.to_string(),
    });
    program.emit_insn(Insn::CatalogChange {
        db: MAIN_DB_ID,
        change: Box::new(change.clone()),
        value_regs: vec![acl_reg],
    });
}

/// Deletes the row of `namespace`, if it has one.
fn emit_namespace_acl_delete(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    namespace: NamespaceId,
    change: &CatalogChange,
) {
    let id_reg = emit_namespace_id(program, namespace);
    let missing = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: cursor_id,
        rowid_reg: id_reg,
        target_pc: missing,
    });
    // Marked as part of an update so that changes() does not count it.
    program.emit_insn(Insn::Delete {
        cursor_id,
        table_name: NAMESPACE_ACLS_TABLE_NAME.to_string(),
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(missing);
    program.emit_insn(Insn::CatalogChange {
        db: MAIN_DB_ID,
        change: Box::new(change.clone()),
        value_regs: vec![],
    });
}

fn emit_namespace_id(program: &mut ProgramBuilder, namespace: NamespaceId) -> usize {
    let id_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: id_reg,
    });
    id_reg
}
