//! Default privileges: the privileges that `ALTER DEFAULT PRIVILEGES` stores
//! for relations that are created later. They are kept in the
//! `__turso_internal_default_acl` table of the main database, one row per
//! role, schema and kind of relation, with the columns and values of
//! PostgreSQL's `pg_default_acl`, except that `defaclrole` holds the engine's
//! role id and `defaclnamespace` the namespace id, NULL for every schema.
//!
//! The engine loads the rows into [`DefaultAcls`] as part of the frontend's
//! schema state, see [`crate::schema_state`], so creating a relation reads
//! them from memory, and `pg_default_acl` shows them.
//!
//! As in PostgreSQL, an entry for every schema replaces the list a new
//! relation has by default, so it holds the owner's own entry too, while an
//! entry for one schema adds to it. An entry that would give a new relation
//! the list it has anyway is not stored.

use std::collections::BTreeMap;
use std::sync::Arc;

use turso_core::{
    namespace::NamespaceId,
    schema::{BTreeTable, RelationKind, Schema},
    security::roles::RoleId,
    to_u32, CatalogChange, Connection, CursorType, InsertFlags, Insn, LimboError, Numeric,
    ProgramBuilder, RegisterOrLiteral, Value, MAIN_DB_ID, SQLITE_ERROR,
};
use turso_pg_parser::sqlstate::SQLSTATE_INSUFFICIENT_PRIVILEGE;

use crate::{
    acl::{Acl, Privileges},
    catalog::role_oid,
    classes::{
        relation_kind_of_letter, relkind_letter, resolve_grantee, resolve_role,
        FIRST_NORMAL_OBJECT_ID,
    },
    error::sqlstate_error,
    schema_state::schema_state,
};

pub(crate) const DEFAULT_ACL_TABLE_NAME: &str = "__turso_internal_default_acl";

pub(crate) const CREATE_DEFAULT_ACL_TABLE_SQL: &str = "CREATE TABLE __turso_internal_default_acl (\
    oid INTEGER PRIMARY KEY, \
    defaclrole INTEGER NOT NULL, \
    defaclnamespace INTEGER, \
    defaclobjtype TEXT NOT NULL, \
    defaclacl TEXT NOT NULL)";

const DEFAULT_ACL_COLUMN_COUNT: usize = 5;

/// The query that reads the default privileges for the frontend's schema
/// state, or None when the database has no default privileges table, as in a
/// read-only open of a database made by an earlier version.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    schema.get_btree_table(NamespaceId::ROOT, DEFAULT_ACL_TABLE_NAME)?;
    Some(format!(
        "SELECT oid, defaclrole, defaclnamespace, defaclobjtype, defaclacl \
         FROM {DEFAULT_ACL_TABLE_NAME}"
    ))
}

/// The default privileges of `schema`, none when the frontend's schema state
/// is not loaded.
pub(crate) fn schema_default_acls(schema: &Schema) -> &Arc<DefaultAcls> {
    static NONE: std::sync::LazyLock<Arc<DefaultAcls>> = std::sync::LazyLock::new(Arc::default);
    match schema_state(schema) {
        Some(state) => &state.default_acls,
        None => &NONE,
    }
}

/// The privileges that relations of one kind get when `role` creates them
/// later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DefaultAcl {
    pub(crate) oid: i64,
    pub(crate) role: RoleId,
    /// None for every schema.
    pub(crate) namespace: Option<NamespaceId>,
    /// [`RelationKind::Table`] for tables, views and materialized views, or
    /// [`RelationKind::Sequence`].
    pub(crate) kind: RelationKind,
    pub(crate) acl: Acl,
}

/// Every stored default privilege of a database, by oid.
#[derive(Debug, Clone, Default)]
pub(crate) struct DefaultAcls {
    by_oid: BTreeMap<i64, DefaultAcl>,
}

impl DefaultAcls {
    /// Returns the entries of the rows that [`schema_state_query`] read.
    pub(crate) fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut acls = Self::default();
        for row in rows {
            let entry = entry_from_row(row)?;
            if acls.get(entry.role, entry.namespace, entry.kind).is_some() {
                return Err(LimboError::Corrupt(format!(
                    "{DEFAULT_ACL_TABLE_NAME} has a duplicate entry: {entry:?}"
                )));
            }
            acls.by_oid.insert(entry.oid, entry);
        }
        Ok(acls)
    }

    /// The entries in oid order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &DefaultAcl> {
        self.by_oid.values()
    }

    fn get(
        &self,
        role: RoleId,
        namespace: Option<NamespaceId>,
        kind: RelationKind,
    ) -> Option<&DefaultAcl> {
        self.by_oid
            .values()
            .find(|entry| entry.role == role && entry.namespace == namespace && entry.kind == kind)
    }

    /// The list that a relation of `kind` gets when `owner` creates it in
    /// `namespace`, or None when it gets the list every relation has before
    /// a `GRANT`.
    pub(crate) fn acl_for_new_relation(
        &self,
        owner: RoleId,
        namespace: NamespaceId,
        kind: RelationKind,
    ) -> Option<Acl> {
        let kind = default_acl_kind(kind)?;
        let every_schema = self.get(owner, None, kind);
        let one_schema = self.get(owner, Some(namespace), kind);
        if every_schema.is_none() && one_schema.is_none() {
            return None;
        }
        let mut default = Acl::default_for(kind, role_oid(owner));
        let mut acl = every_schema
            .map(|entry| entry.acl.clone())
            .unwrap_or_else(|| default.clone());
        if let Some(entry) = one_schema {
            acl.merge(&entry.acl);
        }
        acl.sort();
        default.sort();
        (acl != default).then_some(acl)
    }

    /// Applies what [`emit_entry_write`] wrote: the values `[oid, role,
    /// namespace, kind, acl]`, with a NULL acl for a removed entry.
    pub(crate) fn apply(&mut self, values: &[Value]) -> turso_core::Result<()> {
        let [oid, role, namespace, kind, acl] = values else {
            return Err(LimboError::InternalError(
                "a default privileges change carries one entry".to_string(),
            ));
        };
        if matches!(acl, Value::Null) {
            let Value::Numeric(Numeric::Integer(oid)) = oid else {
                return Err(LimboError::InternalError(format!(
                    "a default privileges change has the oid {oid:?}"
                )));
            };
            self.by_oid.remove(oid);
            return Ok(());
        }
        let entry = entry_from_row(&[
            oid.clone(),
            role.clone(),
            namespace.clone(),
            kind.clone(),
            acl.clone(),
        ])?;
        self.by_oid.insert(entry.oid, entry);
        Ok(())
    }

    /// The oid for a new entry: one more than the largest, and never below
    /// the first oid of a user object.
    fn next_oid(&self) -> i64 {
        self.by_oid
            .keys()
            .next_back()
            .map_or(FIRST_NORMAL_OBJECT_ID, |largest| {
                (largest + 1).max(FIRST_NORMAL_OBJECT_ID)
            })
    }
}

/// The kind of default privileges that a relation of `kind` gets: those of
/// tables for tables, views and materialized views, as in PostgreSQL. An
/// index has no privileges.
fn default_acl_kind(kind: RelationKind) -> Option<RelationKind> {
    match kind {
        RelationKind::Table | RelationKind::View | RelationKind::MaterializedView => {
            Some(RelationKind::Table)
        }
        RelationKind::Sequence => Some(RelationKind::Sequence),
        RelationKind::Index => None,
    }
}

fn entry_from_row(row: &[Value]) -> turso_core::Result<DefaultAcl> {
    let corrupt = || {
        LimboError::Corrupt(format!(
            "{DEFAULT_ACL_TABLE_NAME} has an invalid row: {row:?}"
        ))
    };
    let [Value::Numeric(Numeric::Integer(oid)), Value::Numeric(Numeric::Integer(role)), namespace, Value::Text(kind), Value::Text(acl)] =
        row
    else {
        return Err(corrupt());
    };
    let namespace = match namespace {
        Value::Null => None,
        Value::Numeric(Numeric::Integer(id)) => {
            Some(NamespaceId::new(u32::try_from(*id).map_err(|_| corrupt())?))
        }
        _ => return Err(corrupt()),
    };
    let kind = relation_kind_of_letter(kind.as_str())
        .filter(|kind| matches!(kind, RelationKind::Table | RelationKind::Sequence))
        .ok_or_else(corrupt)?;
    Ok(DefaultAcl {
        oid: *oid,
        role: RoleId::from_stored(*role),
        namespace,
        kind,
        acl: Acl::parse(acl.as_str()).ok_or_else(corrupt)?,
    })
}

/// `ALTER DEFAULT PRIVILEGES ... GRANT`: adds the privileges to the entry of
/// each role and schema, as in PostgreSQL. The current
/// role may change its own default privileges, and a superuser those of
/// every role.
pub(crate) fn translate_alter_default_privileges(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    change: &CatalogChange,
) -> turso_core::Result<()> {
    let CatalogChange::DefaultPrivileges {
        roles,
        namespaces,
        on_sequences,
        privileges,
        grantees,
    } = change
    else {
        unreachable!("only ALTER DEFAULT PRIVILEGES is translated here");
    };
    // Opening a database creates the table, except for an older database
    // opened read-only, which no statement may write to anyway.
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, DEFAULT_ACL_TABLE_NAME) else {
        assert!(
            connection.is_readonly(MAIN_DB_ID),
            "a database opened for writing has {DEFAULT_ACL_TABLE_NAME}"
        );
        return Err(LimboError::ReadOnly);
    };
    // As in PostgreSQL, the grantees are checked before the roles.
    let grantees = grantees
        .iter()
        .map(|grantee| resolve_grantee(schema, connection, grantee))
        .collect::<turso_core::Result<Vec<_>>>()?;
    let mut target_roles = Vec::new();
    if roles.is_empty() {
        target_roles.push(connection.current_role());
    }
    for role in roles {
        let role = resolve_role(schema, connection, role)?;
        if !target_roles.contains(&role) {
            target_roles.push(role);
        }
    }
    for role in &target_roles {
        if *role != connection.current_role() && !connection.current_role_is_superuser() {
            return Err(sqlstate_error(
                SQLSTATE_INSUFFICIENT_PRIVILEGE,
                "permission denied to change default privileges",
            ));
        }
    }
    let mut target_namespaces: Vec<Option<NamespaceId>> = Vec::new();
    if namespaces.is_empty() {
        target_namespaces.push(None);
    }
    for namespace in namespaces {
        if !target_namespaces.contains(&Some(*namespace)) {
            target_namespaces.push(Some(*namespace));
        }
    }
    let kind = if *on_sequences {
        RelationKind::Sequence
    } else {
        RelationKind::Table
    };
    let granted = Privileges::granted_on(kind, privileges);
    let acls = schema_default_acls(schema);

    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    let mut next_oid = acls.next_oid();
    for role in &target_roles {
        let role_oid = role_oid(*role);
        for namespace in &target_namespaces {
            let existing = acls.get(*role, *namespace, kind);
            // An entry for every schema starts from the list a new relation
            // has by default, and an entry for one schema from nothing.
            let mut default = match namespace {
                None => Acl::default_for(kind, role_oid),
                Some(_) => Acl::default(),
            };
            let mut acl = existing
                .map(|entry| entry.acl.clone())
                .unwrap_or_else(|| default.clone());
            for grantee in &grantees {
                acl.grant(*grantee, granted, role_oid);
            }
            acl.sort();
            default.sort();
            let acl = (acl != default).then_some(acl);
            if existing.map(|entry| &entry.acl) == acl.as_ref() {
                continue;
            }
            let oid = match existing {
                Some(entry) => entry.oid,
                None => {
                    next_oid += 1;
                    next_oid - 1
                }
            };
            emit_entry_write(
                program,
                &table,
                cursor_id,
                EntryWrite {
                    oid,
                    exists: existing.is_some(),
                    role: *role,
                    namespace: *namespace,
                    kind,
                    acl: acl.as_ref(),
                },
                change,
            );
        }
    }
    Ok(())
}

/// One entry that [`emit_entry_write`] writes.
struct EntryWrite<'a> {
    oid: i64,
    /// Whether the table has a row with `oid` already.
    exists: bool,
    role: RoleId,
    namespace: Option<NamespaceId>,
    kind: RelationKind,
    /// None to remove the entry.
    acl: Option<&'a Acl>,
}

/// Writes the row of one entry, or removes it, and tells the in-memory
/// entries.
fn emit_entry_write(
    program: &mut ProgramBuilder,
    table: &Arc<BTreeTable>,
    cursor_id: usize,
    entry: EntryWrite<'_>,
    change: &CatalogChange,
) {
    let first_column_reg = program.alloc_registers(DEFAULT_ACL_COLUMN_COUNT);
    // The oid is the rowid, so the record keeps NULL for it, as SQLite does
    // for an INTEGER PRIMARY KEY column.
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::Integer {
        value: entry.role.get(),
        dest: first_column_reg + 1,
    });
    match entry.namespace {
        Some(namespace) => program.emit_insn(Insn::Integer {
            value: i64::from(namespace.as_u32()),
            dest: first_column_reg + 2,
        }),
        None => program.emit_insn(Insn::Null {
            dest: first_column_reg + 2,
            dest_end: None,
        }),
    }
    program.emit_insn(Insn::String8 {
        value: relkind_letter(entry.kind).to_string(),
        dest: first_column_reg + 3,
    });
    match entry.acl {
        Some(acl) => program.emit_insn(Insn::String8 {
            value: acl.stored_text(),
            dest: first_column_reg + 4,
        }),
        None => program.emit_insn(Insn::Null {
            dest: first_column_reg + 4,
            dest_end: None,
        }),
    }
    let oid_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: entry.oid,
        dest: oid_reg,
    });

    if entry.exists {
        // Puts the cursor on the row, which the delete before an overwrite
        // needs.
        let found = program.allocate_label();
        let missing = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: cursor_id,
            rowid_reg: oid_reg,
            target_pc: missing,
        });
        program.emit_insn(Insn::Goto { target_pc: found });
        program.preassign_label_to_next_insn(missing);
        program.emit_insn(Insn::Halt {
            constraint_info: None,
            err_code: SQLITE_ERROR,
            description: format!("{DEFAULT_ACL_TABLE_NAME} has no row with oid {}", entry.oid),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(found);
        if entry.acl.is_none() || program.is_mvcc_enabled() {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: DEFAULT_ACL_TABLE_NAME.to_string(),
                is_part_of_update: true,
            });
        }
    }
    if entry.acl.is_some() {
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(first_column_reg),
            count: to_u32(DEFAULT_ACL_COLUMN_COUNT),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: oid_reg,
            record_reg,
            flag: InsertFlags::new()
                .skip_last_rowid()
                .skip_all_change_counts(),
            table_name: table.name.clone(),
        });
    }
    program.emit_insn(Insn::CatalogChange {
        db: MAIN_DB_ID,
        change: Box::new(change.clone()),
        value_regs: vec![
            oid_reg,
            first_column_reg + 1,
            first_column_reg + 2,
            first_column_reg + 3,
            first_column_reg + 4,
        ],
    });
}
