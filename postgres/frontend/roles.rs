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

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::security::roles::RoleId;
use turso_core::{
    to_u32, CatalogChange, Connection, CursorType, IdxInsertFlags, InsertFlags, Insn, LimboError,
    Numeric, ProgramBuilder, RegisterOrLiteral, Value, MAIN_DB_ID, SQLITE_CONSTRAINT_UNIQUE,
};
use turso_parser::ast::RoleOptions;
use turso_pg_parser::sqlstate::SQLSTATE_DUPLICATE_OBJECT;

use crate::auth_members::Memberships;
use crate::error::sqlstate_error;

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

    /// The memberships of the roles.
    pub(crate) fn memberships(&self) -> &Memberships {
        &self.memberships
    }

    /// Applies what `GRANT role TO role` or `REVOKE role FROM role` wrote,
    /// one membership at a time.
    pub(crate) fn apply_grant_role(
        &mut self,
        revoke: bool,
        values: &[Value],
    ) -> turso_core::Result<()> {
        if revoke {
            self.memberships.apply_revoke(values)
        } else {
            self.memberships.apply_grant(values)
        }
    }

    /// Adds the role that `CREATE ROLE name` stored, under the id that the
    /// statement read back.
    pub(crate) fn apply_create_role(
        &mut self,
        name: &str,
        options: &RoleOptions,
        values: &[Value],
    ) -> turso_core::Result<()> {
        let [Value::Numeric(Numeric::Integer(id))] = values else {
            return Err(LimboError::InternalError(format!(
                "CREATE ROLE read back {values:?} where the role's id was written"
            )));
        };
        self.add(Role {
            id: RoleId::from_stored(*id),
            name: name.to_string(),
            superuser: false,
            inherit: options.inherit,
            bypassrls: options.bypassrls,
        });
        Ok(())
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
        ));
    }
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

    program.emit_insn(Insn::CatalogChange {
        db: MAIN_DB_ID,
        change: Box::new(CatalogChange::CreateRole {
            name: name.to_string(),
            options: options.clone(),
        }),
        value_regs: vec![id_reg],
    });
    Ok(())
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
    let no_conflict = program.allocate_label();
    program.emit_insn(Insn::NoConflict {
        cursor_id,
        target_pc: no_conflict,
        record_reg: key_reg,
        num_regs: 1,
    });
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: SQLITE_CONSTRAINT_UNIQUE,
        description: format!("role \"{name}\" already exists"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(no_conflict);
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(key_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: Some(AUTHID_NAME_INDEX_NAME.to_string()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id,
        record_reg,
        unpacked_start: Some(key_reg),
        unpacked_count: Some(2),
        flags: IdxInsertFlags::new(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superuser_is_a_superuser_without_being_in_the_catalog() {
        let catalog = RoleCatalog::new(&[], &[]).unwrap();

        assert!(catalog.get(RoleId::SUPERUSER).is_none());
        assert!(catalog.is_superuser(RoleId::SUPERUSER));
        assert!(!catalog.is_superuser(RoleId::PUBLIC));
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
