//! Roles, PostgreSQL's `pg_authid` rows, in the `__turso_internal_authid`
//! table of the main database.
//!
//! The table has the `pg_authid` columns that tursopg uses: `rolname`,
//! `rolsuper`, `rolinherit` and `rolbypassrls`. Its `id` column is the
//! engine's [`RoleId`] of the role, which
//! other catalog rows store, such as `relowner` of the relation catalog.
//! [`crate::catalog::role_oid`] gives the role's PostgreSQL oid.
//!
//! In PostgreSQL, `pg_authid` belongs to the whole server and every database
//! sees the same roles. A tursopg database is one file, so its roles belong
//! to that database alone.
//!
//! The frontend creates the table when it opens a database, next to the
//! relation catalog. The engine loads its rows into a [`RoleCatalog`] as part
//! of the frontend's schema state, see [`crate::schema_state`], so looking up
//! a role never reads the database. `CREATE ROLE` inserts a row and updates
//! the in-memory catalog in the same statement, and bumps the schema cookie
//! so that other connections load the change.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use turso_core::ast::{self, RoleOptions};
use turso_core::namespace::NamespaceId;
use turso_core::schema::{RelationKind, Schema};
use turso_core::security::roles::RoleId;
use turso_core::{
    to_u32, CatalogApply, Connection, CursorType, InsertFlags, Insn, LimboError, Numeric,
    ProgramBuilder, RegisterOrLiteral, Value, MAIN_DB_ID, SQLITE_ERROR,
};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST, SQLSTATE_DUPLICATE_OBJECT,
    SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_OBJECT_IN_USE, SQLSTATE_UNDEFINED_OBJECT,
};

use crate::auth_members::{emit_memberships_delete, Membership, Memberships};
use crate::catalog::{pg_role_by_name, relation_name, PgRole};
use crate::classes::class_catalog;
use crate::error::sqlstate_error;
use crate::schema_state::{change_schema_state, emit_catalog_apply};
use crate::unique_index::emit_unique_index_insert;

/// The table that stores the roles created with `CREATE ROLE`.
pub(crate) const AUTHID_TABLE_NAME: &str = "__turso_internal_authid";

/// The unique index on the role names of [`AUTHID_TABLE_NAME`].
const AUTHID_NAME_INDEX_NAME: &str = "__turso_internal_authid_rolname";

/// Creates [`AUTHID_TABLE_NAME`]. The rowid of a row is the id of its role.
pub(crate) const CREATE_AUTHID_TABLE_SQL: &str = "CREATE TABLE __turso_internal_authid (\
    id INTEGER PRIMARY KEY, \
    rolname TEXT NOT NULL, \
    rolsuper INTEGER NOT NULL, \
    rolinherit INTEGER NOT NULL, \
    rolbypassrls INTEGER NOT NULL)";

/// Keeps role names unique in the stored table, whatever writes it.
pub(crate) const CREATE_AUTHID_NAME_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX __turso_internal_authid_rolname ON __turso_internal_authid (rolname)";

/// The number of columns of [`AUTHID_TABLE_NAME`].
const AUTHID_COLUMN_COUNT: usize = 5;

/// The query that reads the roles for the frontend's schema state, or None
/// when the database has no roles table, as in a read-only open of an empty
/// database.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    schema.get_btree_table(NamespaceId::ROOT, AUTHID_TABLE_NAME)?;
    Some(format!(
        "SELECT id, rolname, rolsuper, rolinherit, rolbypassrls FROM {AUTHID_TABLE_NAME}"
    ))
}

/// The roles of the main database as `conn` sees them. Outside a transaction,
/// this includes roles that other connections committed.
pub(crate) fn connection_roles(conn: &Connection) -> Arc<RoleCatalog> {
    conn.maybe_update_schema();
    conn.with_schema(MAIN_DB_ID, |schema| schema_roles(schema).clone())
}

/// The roles of `schema`, none when the frontend's schema state is not
/// loaded.
pub(crate) fn schema_roles(schema: &Schema) -> &Arc<RoleCatalog> {
    static NO_ROLES: std::sync::LazyLock<Arc<RoleCatalog>> = std::sync::LazyLock::new(Arc::default);
    match crate::schema_state::schema_state(schema) {
        Some(state) => &state.roles,
        None => &NO_ROLES,
    }
}

/// A role stored in [`AUTHID_TABLE_NAME`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Role {
    /// The id of the role, its rowid in the roles table.
    pub(crate) id: RoleId,
    /// The name of the role. Names are case-sensitive and unique.
    pub(crate) name: String,
    /// A superuser passes every privilege check.
    pub(crate) superuser: bool,
    /// Whether the memberships granted to the role inherit the privileges
    /// of their roles. GRANT reads it when it stores a membership.
    pub(crate) inherit: bool,
    /// Row-level security does not limit a role with `bypassrls`.
    pub(crate) bypassrls: bool,
}

/// The roles stored in the roles table of a database and their
/// memberships, in memory. The built-in role ids have no entry.
#[derive(Debug, Clone, Default)]
pub(crate) struct RoleCatalog {
    roles: BTreeMap<RoleId, Role>,
    ids_by_name: HashMap<String, RoleId>,
    memberships: Memberships,
}

impl RoleCatalog {
    /// Returns a catalog of the rows read from the roles table and the
    /// membership table. Each role row is `(id, rolname, rolsuper,
    /// rolinherit, rolbypassrls)`; the membership rows are those of
    /// [`crate::auth_members::schema_state_query`].
    pub(crate) fn new(
        rows: &[Vec<Value>],
        membership_rows: &[Vec<Value>],
    ) -> turso_core::Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            let role = role_from_row(row)?;
            if catalog.roles.contains_key(&role.id) || catalog.ids_by_name.contains_key(&role.name)
            {
                return Err(LimboError::Corrupt(format!(
                    "{AUTHID_TABLE_NAME} has a duplicate role: {role:?}"
                )));
            }
            catalog.add(role);
        }
        catalog.memberships = Memberships::new(membership_rows)?;
        let exists = |id| id == RoleId::SUPERUSER || catalog.roles.contains_key(&id);
        if let Some(membership) = catalog.memberships.iter().find(|membership| {
            !exists(membership.role) || !exists(membership.member) || !exists(membership.grantor)
        }) {
            return Err(LimboError::Corrupt(format!(
                "a role membership names a role that does not exist: {membership:?}"
            )));
        }
        Ok(catalog)
    }

    /// The stored role with id `id`. None for a built-in id.
    pub(crate) fn get(&self, id: RoleId) -> Option<&Role> {
        self.roles.get(&id)
    }

    /// The stored role named `name`. Role names are case-sensitive.
    pub(crate) fn get_by_name(&self, name: &str) -> Option<&Role> {
        self.ids_by_name.get(name).and_then(|id| self.roles.get(id))
    }

    /// Returns all roles in the order of their ids.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Role> {
        self.roles.values()
    }

    /// Whether role `id` passes every privilege check. [RoleId::SUPERUSER]
    /// does. Any other role that is not in the catalog does not.
    pub(crate) fn is_superuser(&self, id: RoleId) -> bool {
        id == RoleId::SUPERUSER || self.get(id).is_some_and(|role| role.superuser)
    }

    /// Whether row-level security does not limit role `id`: a superuser or a
    /// role with `bypassrls`, as in PostgreSQL.
    pub(crate) fn bypass_rls(&self, id: RoleId) -> bool {
        self.is_superuser(id) || self.get(id).is_some_and(|role| role.bypassrls)
    }

    /// Whether a session whose current role is `member` may switch to `role`
    /// with `SET ROLE`: a superuser may switch to any role, and any other
    /// role to the roles it is a member of, directly or through other roles,
    /// whether or not it inherits their privileges. No session may switch to
    /// `public`.
    pub(crate) fn can_set_role(&self, member: RoleId, role: RoleId) -> bool {
        role != RoleId::PUBLIC
            && (member == role
                || self.is_superuser(member)
                || self
                    .memberships
                    .reached_from(member, |membership| membership.set_option)
                    .contains(&role))
    }

    /// Whether `member` has the privileges of `role`, as in PostgreSQL: it
    /// is `role` or a superuser, or reaches `role` through memberships that
    /// inherit privileges.
    pub(crate) fn has_privileges_of(&self, member: RoleId, role: RoleId) -> bool {
        member == role || self.is_superuser(member) || self.roles_used_by(member).contains(&role)
    }

    /// `role` and every role whose privileges it has without `SET ROLE`:
    /// the roles it reaches through memberships that inherit privileges.
    pub(crate) fn roles_used_by(&self, role: RoleId) -> Vec<RoleId> {
        self.memberships
            .reached_from(role, |membership| membership.inherit_option)
    }

    /// The PostgreSQL oids of [`Self::roles_used_by`], as access control
    /// lists name roles.
    pub(crate) fn oids_of_roles_used_by(&self, role: RoleId) -> Vec<i64> {
        self.roles_used_by(role)
            .into_iter()
            .map(crate::catalog::role_oid)
            .collect()
    }

    /// The memberships of the roles.
    pub(crate) fn memberships(&self) -> &Memberships {
        &self.memberships
    }

    pub(crate) fn memberships_mut(&mut self) -> &mut Memberships {
        &mut self.memberships
    }

    /// Removes role `id` from the catalog. Panics if it is not there.
    fn remove(&mut self, id: RoleId) {
        let role = self
            .roles
            .remove(&id)
            .unwrap_or_else(|| panic!("DROP ROLE removed role {id:?}, which is not loaded"));
        self.ids_by_name.remove(&role.name);
    }

    /// Adds `role` to the catalog. Panics if its id or name is already in
    /// use.
    fn add(&mut self, role: Role) {
        assert!(
            !self.roles.contains_key(&role.id),
            "role id {:?} is already in use",
            role.id
        );
        let previous = self.ids_by_name.insert(role.name.clone(), role.id);
        assert!(previous.is_none(), "role {} already exists", role.name);
        self.roles.insert(role.id, role);
    }
}

fn role_from_row(row: &[Value]) -> turso_core::Result<Role> {
    let [id, name, superuser, inherit, bypassrls] = row else {
        return Err(corrupt_row(row));
    };
    match (id, name, superuser, inherit, bypassrls) {
        (
            Value::Numeric(Numeric::Integer(id)),
            Value::Text(name),
            Value::Numeric(Numeric::Integer(superuser @ (0 | 1))),
            Value::Numeric(Numeric::Integer(inherit @ (0 | 1))),
            Value::Numeric(Numeric::Integer(bypassrls @ (0 | 1))),
        ) if *id > 0 => Ok(Role {
            id: RoleId::from_stored(*id),
            name: name.as_str().to_string(),
            superuser: *superuser == 1,
            inherit: *inherit == 1,
            bypassrls: *bypassrls == 1,
        }),
        _ => Err(corrupt_row(row)),
    }
}

fn corrupt_row(row: &[Value]) -> LimboError {
    LimboError::Corrupt(format!("{AUTHID_TABLE_NAME} has an invalid row: {row:?}"))
}

/// `CREATE ROLE name`: inserts a role that is not a superuser and cannot log
/// in, with `options`, and tells the engine to add it to the in-memory
/// catalog. Fails with PostgreSQL's 42710 when a role of that name exists.
pub(crate) fn translate_create_role(
    program: &mut ProgramBuilder,
    schema: &Schema,
    name: &str,
    options: &RoleOptions,
) -> turso_core::Result<()> {
    if schema_roles(schema).get_by_name(name).is_some() {
        return Err(sqlstate_error(
            SQLSTATE_DUPLICATE_OBJECT,
            format!("role \"{name}\" already exists"),
            None,
        ));
    }
    let (table_cursor_id, index_cursor_id) = open_authid_cursors(program, schema)?;

    let id_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: table_cursor_id,
        rowid_reg: id_reg,
        prev_largest_reg: 0,
    });
    let first_column_reg = program.alloc_registers(AUTHID_COLUMN_COUNT);
    // The id column is the rowid, which the record stores as NULL.
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: first_column_reg + 1,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_column_reg + 2,
    });
    program.emit_insn(Insn::Integer {
        value: options.inherit as i64,
        dest: first_column_reg + 3,
    });
    program.emit_insn(Insn::Integer {
        value: options.bypassrls as i64,
        dest: first_column_reg + 4,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(AUTHID_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: table_cursor_id,
        key_reg: id_reg,
        record_reg,
        flag: InsertFlags::new().skip_last_rowid(),
        table_name: AUTHID_TABLE_NAME.to_string(),
    });
    emit_name_index_insert(program, index_cursor_id, name, id_reg);

    RoleCreated {
        name: name.to_string(),
        options: options.clone(),
    }
    .emit(program, id_reg);
    Ok(())
}

/// A role that `CREATE ROLE` inserted.
#[derive(Debug)]
struct RoleCreated {
    name: String,
    options: RoleOptions,
}

impl RoleCreated {
    /// `id_reg` holds the id the role was stored under.
    fn emit(self, program: &mut ProgramBuilder, id_reg: usize) {
        emit_catalog_apply(program, self, vec![id_reg]);
    }

    fn apply_to(&self, roles: &mut RoleCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [Value::Numeric(Numeric::Integer(id))] = values else {
            return Err(LimboError::InternalError(format!(
                "CREATE ROLE read back {values:?} where the role's id was written"
            )));
        };
        roles.add(Role {
            id: RoleId::from_stored(*id),
            name: self.name.clone(),
            superuser: false,
            inherit: self.options.inherit,
            bypassrls: self.options.bypassrls,
        });
        Ok(())
    }
}

impl CatalogApply for RoleCreated {
    fn apply(&self, schema: &mut Schema, values: &[Value]) -> turso_core::Result<()> {
        change_schema_state(schema, |state| {
            self.apply_to(Arc::make_mut(&mut state.roles), values)
        })
    }
}

/// The most objects that the DETAIL of a role that other objects depend on
/// lists, as in PostgreSQL.
const MAX_REPORTED_DEPENDENTS: usize = 100;

/// `DROP ROLE roles`. The roles are checked as PostgreSQL checks them, in
/// two passes. First each role must be named, not given as `CURRENT_USER`
/// or the like, exist unless `IF EXISTS` skips it, and not be the current or
/// the session role. Then each must not be needed by the database system,
/// own no relation, have no privileges on one and be no policy's role. A
/// role the statement names twice is dropped once. The roles' memberships
/// go with them. Only a superuser reaches this; core refuses the statement
/// for any other role.
pub(crate) fn translate_drop_role(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    if_exists: bool,
    role_names: &[ast::Grantee],
) -> turso_core::Result<()> {
    let roles = schema_roles(schema);
    // The roles that pass the checks PostgreSQL makes first, with the name
    // the statement gives each. None stands for a role that the database
    // system needs, which the second checks refuse.
    let mut named: Vec<(&str, Option<&Role>)> = Vec::new();
    for role_name in role_names {
        let ast::Grantee::Role(ast::RoleSpec::Name(name)) = role_name else {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "cannot use special role specifier in DROP ROLE",
                None,
            ));
        };
        let role = match pg_role_by_name(roles, name) {
            Some(PgRole::Engine(id)) => {
                if id == connection.current_role() {
                    return Err(sqlstate_error(
                        SQLSTATE_OBJECT_IN_USE,
                        "current user cannot be dropped",
                        None,
                    ));
                }
                if id == connection.session_role() {
                    return Err(sqlstate_error(
                        SQLSTATE_OBJECT_IN_USE,
                        "session user cannot be dropped",
                        None,
                    ));
                }
                // The built-in superuser is not stored, so it is not found.
                roles.get(id)
            }
            Some(PgRole::DatabaseOwner) => None,
            // tursopg does not have PostgreSQL's other predefined roles, but
            // PostgreSQL would find them and refuse to drop them.
            None if is_predefined_role(name) => None,
            None if if_exists => continue,
            None => {
                return Err(sqlstate_error(
                    SQLSTATE_UNDEFINED_OBJECT,
                    format!("role \"{name}\" does not exist"),
                    None,
                ))
            }
        };
        if !named.iter().any(|(other, _)| *other == name.as_str()) {
            named.push((name, role));
        }
    }
    let mut dropped: Vec<&Role> = Vec::new();
    for (name, role) in named {
        let Some(role) = role else {
            return Err(sqlstate_error(
                SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
                format!("cannot drop role {name} because it is required by the database system"),
                None,
            ));
        };
        let dependents = role_dependents(schema, connection, role.id);
        if !dependents.is_empty() {
            return Err(sqlstate_error(
                SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
                format!("role \"{name}\" cannot be dropped because some objects depend on it"),
                Some(dependents_detail(dependents)),
            ));
        }
        dropped.push(role);
    }
    if dropped.is_empty() {
        return Ok(());
    }

    let memberships: Vec<Membership> = roles
        .memberships()
        .iter()
        .filter(|membership| dropped.iter().any(|role| membership.names(role.id)))
        .cloned()
        .collect();
    emit_memberships_delete(program, schema, &memberships)?;

    let (table_cursor_id, index_cursor_id) = open_authid_cursors(program, schema)?;
    for role in dropped {
        emit_role_delete(program, table_cursor_id, index_cursor_id, role);
    }
    Ok(())
}

/// Whether `name` is one of PostgreSQL 18's predefined roles.
fn is_predefined_role(name: &str) -> bool {
    matches!(
        name,
        "pg_database_owner"
            | "pg_read_all_data"
            | "pg_write_all_data"
            | "pg_monitor"
            | "pg_read_all_settings"
            | "pg_read_all_stats"
            | "pg_stat_scan_tables"
            | "pg_read_server_files"
            | "pg_write_server_files"
            | "pg_execute_server_program"
            | "pg_signal_backend"
            | "pg_checkpoint"
            | "pg_maintain"
            | "pg_use_reserved_connections"
            | "pg_create_subscription"
            | "pg_signal_autovacuum_worker"
    )
}

/// What keeps role `role` from being dropped, one line per object, as
/// PostgreSQL describes them: the relations it owns, those whose access
/// control list names it, and the policies that apply to it. Indexes are
/// left out, as their tables stand for them.
fn role_dependents(schema: &Schema, connection: &Connection, role: RoleId) -> Vec<String> {
    let Some(catalog) = class_catalog(schema) else {
        return Vec::new();
    };
    let role_oid = crate::catalog::role_oid(role);
    // PostgreSQL lists the objects by oid, which is the order they were
    // created in. Relations and policies take their oids from one counter.
    let mut dependents: Vec<(i64, String)> = Vec::new();
    for class in catalog.classes() {
        if class.relkind == RelationKind::Index {
            continue;
        }
        let name = relation_name(connection, schema, class.relnamespace, &class.relname);
        let kind = relation_kind_name(class.relkind);
        if class.relowner == role {
            dependents.push((class.oid, format!("owner of {kind} {name}")));
        } else if class
            .relacl
            .as_ref()
            .is_some_and(|acl| acl.names_role(role_oid))
        {
            dependents.push((class.oid, format!("privileges for {kind} {name}")));
        }
        for (details, policy) in class.relpolicies.iter() {
            // A policy that names PUBLIC applies to PUBLIC alone, as
            // PostgreSQL keeps no other role for it.
            if policy.roles.contains(&role) && !policy.roles.contains(&RoleId::PUBLIC) {
                dependents.push((
                    details.oid,
                    format!("target of policy {} on {kind} {name}", policy.name),
                ));
            }
        }
    }
    dependents.sort_by_key(|(oid, _)| *oid);
    dependents.into_iter().map(|(_, line)| line).collect()
}

/// The DETAIL that lists `dependents`, at most [`MAX_REPORTED_DEPENDENTS`]
/// of them.
fn dependents_detail(dependents: Vec<String>) -> String {
    if dependents.len() <= MAX_REPORTED_DEPENDENTS {
        return dependents.join("\n");
    }
    // PostgreSQL writes the whole list to the server log.
    tracing::info!(
        "objects that depend on the role:\n{}",
        dependents.join("\n")
    );
    let others = dependents.len() - MAX_REPORTED_DEPENDENTS;
    let mut lines = dependents;
    lines.truncate(MAX_REPORTED_DEPENDENTS);
    let objects = if others == 1 { "object" } else { "objects" };
    lines.push(format!(
        "and {others} other {objects} (see server log for list)"
    ));
    lines.join("\n")
}

fn relation_kind_name(kind: RelationKind) -> &'static str {
    match kind {
        RelationKind::Table => "table",
        RelationKind::View => "view",
        RelationKind::MaterializedView => "materialized view",
        RelationKind::Sequence => "sequence",
        RelationKind::Index => "index",
    }
}

/// Opens [`AUTHID_TABLE_NAME`] and its name index for writing, and returns
/// the cursor of each.
fn open_authid_cursors(
    program: &mut ProgramBuilder,
    schema: &Schema,
) -> turso_core::Result<(usize, usize)> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, AUTHID_TABLE_NAME) else {
        return Err(LimboError::ParseError(
            "the database has no roles table".to_string(),
        ));
    };
    let name_index = schema
        .get_indices(NamespaceId::ROOT, AUTHID_TABLE_NAME)
        .find(|index| index.name == AUTHID_NAME_INDEX_NAME)
        .cloned()
        .ok_or_else(|| LimboError::Corrupt(format!("{AUTHID_TABLE_NAME} has no name index")))?;
    let table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    let index_cursor_id = program.alloc_cursor_index(None, &name_index)?;
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor_id,
        root_page: RegisterOrLiteral::Literal(name_index.root_page),
        db: MAIN_DB_ID,
    });
    Ok((table_cursor_id, index_cursor_id))
}

/// Deletes the row of `role` and its name index entry, and tells the engine
/// to remove the role from the in-memory catalog. The catalog was loaded
/// from the table, so the row must be there.
fn emit_role_delete(
    program: &mut ProgramBuilder,
    table_cursor_id: usize,
    index_cursor_id: usize,
    role: &Role,
) {
    let id = role.id.get();
    let id_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: id,
        dest: id_reg,
    });
    let found = program.allocate_label();
    let missing = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: table_cursor_id,
        rowid_reg: id_reg,
        target_pc: missing,
    });
    program.emit_insn(Insn::Goto { target_pc: found });
    program.preassign_label_to_next_insn(missing);
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: SQLITE_ERROR,
        description: format!("{AUTHID_TABLE_NAME} has no role with id {id}"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(found);
    let key_reg = program.alloc_registers(2);
    program.emit_insn(Insn::String8 {
        value: role.name.clone(),
        dest: key_reg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: id_reg,
        dst_reg: key_reg + 1,
        extra_amount: 0,
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: key_reg,
        num_regs: 2,
        cursor_id: index_cursor_id,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: table_cursor_id,
        table_name: AUTHID_TABLE_NAME.to_string(),
        is_part_of_update: true,
    });
    RoleDropped.emit(program, id_reg);
}

/// A role that `DROP ROLE` deleted. Its memberships were deleted before it.
#[derive(Debug)]
struct RoleDropped;

impl RoleDropped {
    /// `id_reg` holds the id of the deleted role.
    fn emit(self, program: &mut ProgramBuilder, id_reg: usize) {
        emit_catalog_apply(program, self, vec![id_reg]);
    }

    fn apply_to(&self, roles: &mut RoleCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [Value::Numeric(Numeric::Integer(id))] = values else {
            return Err(LimboError::InternalError(format!(
                "DROP ROLE read back {values:?} where the role's id was written"
            )));
        };
        roles.remove(RoleId::from_stored(*id));
        Ok(())
    }
}

impl CatalogApply for RoleDropped {
    fn apply(&self, schema: &mut Schema, values: &[Value]) -> turso_core::Result<()> {
        change_schema_state(schema, |state| {
            self.apply_to(Arc::make_mut(&mut state.roles), values)
        })
    }
}

/// Adds the role's name to the name index, failing if another role has the
/// name. The in-memory catalog has already rejected a name it knows, so this
/// only fails if the stored table has a role the catalog does not.
fn emit_name_index_insert(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    name: &str,
    id_reg: usize,
) {
    let key_reg = program.alloc_registers(2);
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: key_reg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: id_reg,
        dst_reg: key_reg + 1,
        extra_amount: 0,
    });
    emit_unique_index_insert(
        program,
        cursor_id,
        AUTHID_NAME_INDEX_NAME,
        key_reg,
        2,
        format!("role \"{name}\" already exists"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependents_detail_counts_the_objects_it_leaves_out() {
        let dependents = |count: usize| {
            (1..=count)
                .map(|i| format!("privileges for table t{i}"))
                .collect::<Vec<_>>()
        };

        let one_more = dependents_detail(dependents(MAX_REPORTED_DEPENDENTS + 1));
        assert_eq!(
            one_more.lines().last(),
            Some("and 1 other object (see server log for list)")
        );
        let two_more = dependents_detail(dependents(MAX_REPORTED_DEPENDENTS + 2));
        assert_eq!(
            two_more.lines().last(),
            Some("and 2 other objects (see server log for list)")
        );
    }

    #[test]
    fn superuser_is_a_superuser_without_being_in_the_catalog() {
        let catalog = RoleCatalog::new(&[], &[]).unwrap();

        assert!(catalog.get(RoleId::SUPERUSER).is_none());
        assert!(catalog.is_superuser(RoleId::SUPERUSER));
        assert!(!catalog.is_superuser(RoleId::PUBLIC));
    }

    #[test]
    fn role_created_adds_the_role_under_the_id_it_reads_back() {
        let mut catalog = RoleCatalog::new(&[], &[]).unwrap();
        let created = RoleCreated {
            name: "alice".to_string(),
            options: RoleOptions {
                inherit: false,
                bypassrls: true,
            },
        };

        created
            .apply_to(&mut catalog, &[Value::from_i64(5)])
            .unwrap();

        let alice = catalog.get_by_name("alice").unwrap();
        assert_eq!(alice.id, RoleId::from_stored(5));
        assert!(!alice.superuser);
        assert!(!alice.inherit);
        assert!(alice.bypassrls);

        let no_id = created.apply_to(&mut catalog, &[]);
        assert!(matches!(no_id, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn new_reads_the_stored_roles() {
        let catalog =
            RoleCatalog::new(&[role_row(1, "alice", 0), role_row(2, "Alice", 1)], &[]).unwrap();

        let names: Vec<_> = catalog.iter().map(|role| role.name.as_str()).collect();
        assert_eq!(names, vec!["alice", "Alice"]);
        let alice = catalog.get_by_name("alice").unwrap();
        assert!(!alice.superuser);
        assert!(!catalog.is_superuser(alice.id));
        assert!(catalog.is_superuser(catalog.get_by_name("Alice").unwrap().id));
    }

    #[test]
    fn new_rejects_a_duplicate_role_name() {
        let error =
            RoleCatalog::new(&[role_row(1, "alice", 0), role_row(2, "alice", 0)], &[]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn new_rejects_an_invalid_row() {
        let error = RoleCatalog::new(&[vec![Value::from_i64(1)]], &[]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn superusers_and_bypassrls_roles_bypass_rls() {
        let catalog = RoleCatalog::new(
            &[
                role_row(1, "alice", 0),
                role_row(2, "admin", 1),
                bypassrls_role_row(3, "reader"),
            ],
            &[],
        )
        .unwrap();
        let id = |name| catalog.get_by_name(name).unwrap().id;

        assert!(catalog.bypass_rls(RoleId::SUPERUSER));
        assert!(catalog.bypass_rls(id("admin")));
        assert!(catalog.bypass_rls(id("reader")));
        assert!(!catalog.is_superuser(id("reader")));
        assert!(!catalog.bypass_rls(id("alice")));
        assert!(!catalog.bypass_rls(RoleId::PUBLIC));
    }

    #[test]
    fn a_role_keeps_the_inherit_value_of_its_row() {
        let catalog = RoleCatalog::new(
            &[role_row(1, "alice", 0), noinherit_role_row(2, "bob")],
            &[],
        )
        .unwrap();

        assert!(catalog.get_by_name("alice").unwrap().inherit);
        assert!(!catalog.get_by_name("bob").unwrap().inherit);
    }

    #[test]
    fn a_member_may_set_the_roles_it_reaches_through_memberships() {
        // carol is a member of bob without inheriting, and bob of alice.
        let catalog = RoleCatalog::new(
            &[
                role_row(1, "alice", 0),
                role_row(2, "bob", 0),
                noinherit_role_row(3, "carol"),
                role_row(4, "dave", 0),
            ],
            &[membership_row(1, 1, 2, 1), membership_row(2, 2, 3, 0)],
        )
        .unwrap();
        let id = |name| catalog.get_by_name(name).unwrap().id;

        assert!(catalog.can_set_role(id("bob"), id("alice")));
        assert!(catalog.can_set_role(id("carol"), id("bob")));
        assert!(catalog.can_set_role(id("carol"), id("alice")));
        assert!(!catalog.can_set_role(id("alice"), id("bob")));
        assert!(!catalog.can_set_role(id("dave"), id("alice")));
        assert!(!catalog.can_set_role(id("carol"), RoleId::PUBLIC));
    }

    #[test]
    fn a_member_has_the_privileges_of_the_roles_it_inherits_from() {
        // carol is a member of bob without inheriting, and bob of alice.
        let catalog = RoleCatalog::new(
            &[
                role_row(1, "alice", 0),
                role_row(2, "bob", 0),
                noinherit_role_row(3, "carol"),
                role_row(4, "admin", 1),
            ],
            &[membership_row(1, 1, 2, 1), membership_row(2, 2, 3, 0)],
        )
        .unwrap();
        let id = |name| catalog.get_by_name(name).unwrap().id;

        assert!(catalog.has_privileges_of(id("bob"), id("alice")));
        assert!(catalog.has_privileges_of(id("carol"), id("carol")));
        assert!(!catalog.has_privileges_of(id("carol"), id("bob")));
        assert!(!catalog.has_privileges_of(id("carol"), id("alice")));
        assert!(!catalog.has_privileges_of(id("alice"), id("bob")));
        assert!(catalog.has_privileges_of(id("admin"), id("carol")));
        assert_eq!(
            catalog.roles_used_by(id("bob")),
            vec![id("bob"), id("alice")]
        );
    }

    #[test]
    fn only_a_superuser_may_set_another_role_and_no_one_may_set_public() {
        let catalog =
            RoleCatalog::new(&[role_row(1, "alice", 0), role_row(2, "bob", 0)], &[]).unwrap();
        let alice = catalog.get_by_name("alice").unwrap().id;
        let bob = catalog.get_by_name("bob").unwrap().id;

        assert!(catalog.can_set_role(RoleId::SUPERUSER, alice));
        assert!(catalog.can_set_role(alice, alice));
        assert!(!catalog.can_set_role(alice, bob));
        assert!(!catalog.can_set_role(alice, RoleId::SUPERUSER));
        assert!(!catalog.can_set_role(RoleId::SUPERUSER, RoleId::PUBLIC));
    }

    fn role_row(id: i64, name: &str, superuser: i64) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::build_text(name.to_string()),
            Value::from_i64(superuser),
            Value::from_i64(1),
            Value::from_i64(0),
        ]
    }

    fn bypassrls_role_row(id: i64, name: &str) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::build_text(name.to_string()),
            Value::from_i64(0),
            Value::from_i64(1),
            Value::from_i64(1),
        ]
    }

    /// A membership row of `member` in `role`, granted by the superuser.
    fn membership_row(id: i64, role: i64, member: i64, inherit: i64) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::from_i64(role),
            Value::from_i64(member),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(inherit),
            Value::from_i64(1),
        ]
    }

    fn noinherit_role_row(id: i64, name: &str) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::build_text(name.to_string()),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(0),
        ]
    }
}
