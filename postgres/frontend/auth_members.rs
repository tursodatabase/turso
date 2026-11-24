//! Role memberships, PostgreSQL's `pg_auth_members` rows, in the
//! `__turso_internal_auth_members` table of the main database.
//!
//! `GRANT role TO member` makes `member` a member of `role`: the member may
//! `SET ROLE` to the role, and has its privileges when the membership
//! inherits them. Roles and their members are stored by the engine's
//! [`RoleId`], as the roles table stores them.
//!
//! The frontend creates the table when it opens a database, next to the
//! roles table. A database that an earlier version made has no table until
//! it is opened for writing, and has no memberships until then. The engine
//! loads the rows into the [`crate::roles::RoleCatalog`] as part of the
//! frontend's schema state, see [`crate::schema_state`]. `GRANT` and
//! `REVOKE` write the rows and update the in-memory catalog in the same
//! statement, and bump the schema cookie so that other connections load the
//! change.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use turso_core::ast;
use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::security::roles::RoleId;
use turso_core::{
    to_u32, CatalogApply, Connection, CursorType, InsertFlags, Insn, LimboError, Numeric,
    ProgramBuilder, RegisterOrLiteral, Value, MAIN_DB_ID, SQLITE_ERROR,
};
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INSUFFICIENT_PRIVILEGE,
    SQLSTATE_INVALID_GRANT_OPERATION, SQLSTATE_UNDEFINED_OBJECT,
};

use crate::catalog::{pg_role_by_name, PgRole, PG_DATABASE_OWNER_NAME};
use crate::error::sqlstate_error;
use crate::functions::role_name_in;
use crate::roles::{schema_roles, RoleCatalog};
use crate::schema_init::TableInit;
use crate::schema_state::{change_schema_state, emit_catalog_apply};
use crate::unique_index::emit_unique_index_insert;

/// The table that stores the memberships that `GRANT role TO role` makes.
pub(crate) const AUTH_MEMBERS_TABLE_NAME: &str = "__turso_internal_auth_members";

/// Creates [`AUTH_MEMBERS_TABLE_NAME`]. The rowid of a row is the id of its
/// membership.
const CREATE_AUTH_MEMBERS_TABLE_SQL: &str = "CREATE TABLE __turso_internal_auth_members (\
    id INTEGER PRIMARY KEY, \
    roleid INTEGER NOT NULL, \
    member INTEGER NOT NULL, \
    grantor INTEGER NOT NULL, \
    admin_option INTEGER NOT NULL, \
    inherit_option INTEGER NOT NULL, \
    set_option INTEGER NOT NULL)";

/// The unique index on the role, member and grantor of
/// [`AUTH_MEMBERS_TABLE_NAME`], PostgreSQL's
/// `pg_auth_members_role_member_index`.
const AUTH_MEMBERS_ROLE_MEMBER_INDEX_NAME: &str = "__turso_internal_auth_members_role_member";

/// Keeps a membership unique in the stored table, whatever writes it: the
/// in-memory catalog refuses a table with a duplicate, which would leave the
/// database unable to open.
const CREATE_AUTH_MEMBERS_ROLE_MEMBER_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX __turso_internal_auth_members_role_member \
     ON __turso_internal_auth_members (roleid, member, grantor)";

/// The number of columns of [`AUTH_MEMBERS_TABLE_NAME`].
const AUTH_MEMBERS_COLUMN_COUNT: usize = 7;

pub(crate) const TABLE_INIT: TableInit = TableInit {
    table: AUTH_MEMBERS_TABLE_NAME,
    statements: &[
        CREATE_AUTH_MEMBERS_TABLE_SQL,
        CREATE_AUTH_MEMBERS_ROLE_MEMBER_INDEX_SQL,
    ],
};

/// The query that reads the memberships for the frontend's schema state, or
/// None when the database has no membership table, as one that an earlier
/// version made and that is opened read-only.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    schema.get_btree_table(NamespaceId::ROOT, AUTH_MEMBERS_TABLE_NAME)?;
    Some(format!(
        "SELECT id, roleid, member, grantor, admin_option, inherit_option, set_option \
         FROM {AUTH_MEMBERS_TABLE_NAME}"
    ))
}

/// A row of [`AUTH_MEMBERS_TABLE_NAME`]: `member` is a member of `role`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Membership {
    /// The id of the membership, its rowid in the table.
    pub(crate) id: i64,
    pub(crate) role: RoleId,
    pub(crate) member: RoleId,
    /// The role that granted the membership. Only a superuser grants, and
    /// PostgreSQL records such a grant as made by its bootstrap superuser.
    pub(crate) grantor: RoleId,
    /// Whether the member may grant the role to others. GRANT cannot give
    /// it, so it is always false.
    pub(crate) admin_option: bool,
    /// Whether the member has the privileges of the role without `SET
    /// ROLE`. GRANT takes it from the member's `INHERIT` attribute.
    pub(crate) inherit_option: bool,
    /// Whether the member may `SET ROLE` to the role. GRANT cannot take it
    /// away, so it is always true.
    pub(crate) set_option: bool,
}

impl Membership {
    /// Whether the membership names role `role` as the role, the member or
    /// the grantor.
    pub(crate) fn names(&self, role: RoleId) -> bool {
        self.role == role || self.member == role || self.grantor == role
    }
}

/// The memberships of a database, in memory.
#[derive(Debug, Clone, Default)]
pub(crate) struct Memberships {
    by_id: BTreeMap<i64, Membership>,
}

impl Memberships {
    /// Returns the memberships of the rows read from the membership table,
    /// in the columns of [`schema_state_query`].
    pub(crate) fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut memberships = Self::default();
        for row in rows {
            let membership = membership_from_row(row)?;
            if memberships.iter().any(|other| {
                (other.role, other.member, other.grantor)
                    == (membership.role, membership.member, membership.grantor)
            }) {
                return Err(LimboError::Corrupt(format!(
                    "{AUTH_MEMBERS_TABLE_NAME} has a duplicate membership: {membership:?}"
                )));
            }
            memberships.add(membership);
        }
        Ok(memberships)
    }

    /// Returns all memberships in the order of their ids.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Membership> {
        self.by_id.values()
    }

    /// The membership of `member` in `role`, if it is one.
    pub(crate) fn get(&self, role: RoleId, member: RoleId) -> Option<&Membership> {
        self.by_id
            .values()
            .find(|membership| membership.role == role && membership.member == member)
    }

    /// `member` and every role that it reaches through memberships that
    /// `follow` accepts: the roles `member` is a member of, the roles those
    /// are members of, and so on.
    pub(crate) fn reached_from(
        &self,
        member: RoleId,
        follow: impl Fn(&Membership) -> bool,
    ) -> Vec<RoleId> {
        let mut reached = vec![member];
        let mut queue = VecDeque::from([member]);
        while let Some(next) = queue.pop_front() {
            for membership in self.by_id.values() {
                if membership.member == next
                    && follow(membership)
                    && !reached.contains(&membership.role)
                {
                    reached.push(membership.role);
                    queue.push_back(membership.role);
                }
            }
        }
        reached
    }

    /// Adds `membership`. Panics if its id is already in use.
    fn add(&mut self, membership: Membership) {
        let previous = self.by_id.insert(membership.id, membership);
        assert!(previous.is_none(), "membership id is already in use");
    }
}

fn membership_from_row(row: &[Value]) -> turso_core::Result<Membership> {
    let role_id = |value: &Value| match value {
        Value::Numeric(Numeric::Integer(id)) if *id >= 0 => Some(RoleId::from_stored(*id)),
        _ => None,
    };
    let flag = |value: &Value| match value {
        Value::Numeric(Numeric::Integer(flag @ (0 | 1))) => Some(*flag == 1),
        _ => None,
    };
    let membership = (|| {
        let [Value::Numeric(Numeric::Integer(id)), role, member, grantor, admin_option, inherit_option, set_option] =
            row
        else {
            return None;
        };
        Some(Membership {
            id: *id,
            role: role_id(role)?,
            member: role_id(member)?,
            grantor: role_id(grantor)?,
            admin_option: flag(admin_option)?,
            inherit_option: flag(inherit_option)?,
            set_option: flag(set_option)?,
        })
    })();
    membership.ok_or_else(|| {
        LimboError::Corrupt(format!(
            "{AUTH_MEMBERS_TABLE_NAME} has an invalid row: {row:?}"
        ))
    })
}

/// A role that GRANT or REVOKE names: one of the engine's, or
/// `pg_database_owner`, which `pg_roles` lists without its being a role of
/// the engine.
#[derive(Debug, Clone)]
struct NamedRole {
    name: String,
    /// None for `pg_database_owner`.
    id: Option<RoleId>,
}

/// `GRANT roles TO grantees` or `REVOKE roles FROM grantees`, in the order
/// PostgreSQL checks them: the grantees must exist, then each role must
/// exist, be one that can have members, and the current role must be a
/// superuser. Granting a role that the grantee already has, or revoking one
/// it does not have, does nothing. A grant that would make a role a member
/// of itself, directly or through other roles, fails.
pub(crate) fn translate_grant_role(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    revoke: bool,
    role_names: &[String],
    grantees: &[ast::Grantee],
) -> turso_core::Result<()> {
    let roles = schema_roles(schema);
    let statement = if revoke { "revoke" } else { "grant" };
    let grantees = grantees
        .iter()
        .map(|grantee| resolve_grantee(roles, connection, grantee))
        .collect::<turso_core::Result<Vec<_>>>()?;

    let cursors = open_cursors(program, schema)?;
    // The memberships as the statement leaves them so far, so that a later
    // pair of the same statement sees what an earlier pair granted.
    let mut memberships = roles.memberships().clone();
    // The rowid of a granted membership is not known until the row is
    // inserted. Nothing reads it before then, so it gets a negative id here
    // that no stored membership has.
    let mut granted_ids = (1..).map(|n: i64| -n);
    for role_name in role_names {
        let role = resolve_role_name(roles, role_name)?;
        if role.id.is_none() && !revoke {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!("role \"{PG_DATABASE_OWNER_NAME}\" cannot have explicit members"),
                None,
            ));
        }
        if !roles.is_superuser(connection.current_role()) {
            return Err(sqlstate_error(
                SQLSTATE_INSUFFICIENT_PRIVILEGE,
                format!("permission denied to {statement} role \"{}\"", role.name),
                None,
            ));
        }
        // pg_database_owner has no stored members to revoke.
        let Some(role_id) = role.id else {
            continue;
        };
        for grantee in &grantees {
            if revoke {
                let Some(member_id) = grantee.id else {
                    continue;
                };
                let Some(membership) = memberships.get(role_id, member_id) else {
                    continue;
                };
                let membership = membership.clone();
                emit_membership_delete(program, &cursors, &membership);
                memberships.by_id.remove(&membership.id);
                continue;
            }
            let Some(member_id) = grantee.id else {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!("role \"{PG_DATABASE_OWNER_NAME}\" cannot be a member of any role"),
                    None,
                ));
            };
            // A role that `role` already reaches cannot become its member:
            // that would make a loop, or a role a member of itself.
            if memberships
                .reached_from(role_id, |_| true)
                .contains(&member_id)
            {
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_GRANT_OPERATION,
                    format!(
                        "role \"{}\" is a member of role \"{}\"",
                        role.name, grantee.name
                    ),
                    None,
                ));
            }
            if memberships.get(role_id, member_id).is_some() {
                continue;
            }
            let membership = Membership {
                id: granted_ids.next().expect("the ids do not run out"),
                role: role_id,
                member: member_id,
                grantor: RoleId::SUPERUSER,
                admin_option: false,
                // As in PostgreSQL, the member's `INHERIT` attribute decides.
                inherit_option: roles.get(member_id).is_none_or(|role| role.inherit),
                set_option: true,
            };
            emit_membership_insert(program, &cursors, &membership);
            memberships.add(membership);
        }
    }
    Ok(())
}

/// The role that a grantee names. `PUBLIC` is not a role that can have
/// memberships, and PostgreSQL reports it as a role that does not exist.
fn resolve_grantee(
    roles: &RoleCatalog,
    connection: &Arc<Connection>,
    grantee: &ast::Grantee,
) -> turso_core::Result<NamedRole> {
    let id = match grantee {
        ast::Grantee::Public => return Err(role_does_not_exist("public")),
        ast::Grantee::Role(ast::RoleSpec::Name(name)) => return resolve_role_name(roles, name),
        ast::Grantee::Role(ast::RoleSpec::CurrentRole) => connection.current_role(),
        ast::Grantee::Role(ast::RoleSpec::SessionUser) => connection.session_role(),
    };
    Ok(NamedRole {
        name: role_name_in(roles, id)?,
        id: Some(id),
    })
}

fn resolve_role_name(roles: &RoleCatalog, name: &str) -> turso_core::Result<NamedRole> {
    let id = match pg_role_by_name(roles, name) {
        Some(PgRole::Engine(id)) => Some(id),
        Some(PgRole::DatabaseOwner) => None,
        None => return Err(role_does_not_exist(name)),
    };
    Ok(NamedRole {
        name: name.to_string(),
        id,
    })
}

fn role_does_not_exist(name: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("role \"{name}\" does not exist"),
        None,
    )
}

/// Deletes `memberships` from the membership table and from the in-memory
/// catalog, as DROP ROLE does for the memberships of the roles it drops.
pub(crate) fn emit_memberships_delete(
    program: &mut ProgramBuilder,
    schema: &Schema,
    memberships: &[Membership],
) -> turso_core::Result<()> {
    if memberships.is_empty() {
        return Ok(());
    }
    let cursors = open_cursors(program, schema)?;
    for membership in memberships {
        emit_membership_delete(program, &cursors, membership);
    }
    Ok(())
}

/// The cursors that GRANT, REVOKE and DROP ROLE write the membership table
/// through.
struct Cursors {
    table: usize,
    role_member_index: usize,
}

/// Opens [`Cursors`] for writing the membership table of `schema`.
fn open_cursors(program: &mut ProgramBuilder, schema: &Schema) -> turso_core::Result<Cursors> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, AUTH_MEMBERS_TABLE_NAME) else {
        return Err(LimboError::ParseError(
            "the database has no role membership table".to_string(),
        ));
    };
    let role_member_index = schema
        .get_indices(NamespaceId::ROOT, AUTH_MEMBERS_TABLE_NAME)
        .find(|index| index.name == AUTH_MEMBERS_ROLE_MEMBER_INDEX_NAME)
        .cloned()
        .ok_or_else(|| {
            LimboError::Corrupt(format!(
                "{AUTH_MEMBERS_TABLE_NAME} has no role and member index"
            ))
        })?;
    let table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    let index_cursor_id = program.alloc_cursor_index(None, &role_member_index)?;
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor_id,
        root_page: RegisterOrLiteral::Literal(role_member_index.root_page),
        db: MAIN_DB_ID,
    });
    Ok(Cursors {
        table: table_cursor_id,
        role_member_index: index_cursor_id,
    })
}

/// Inserts `membership` under a new rowid and tells the engine to add it to
/// the in-memory catalog.
fn emit_membership_insert(
    program: &mut ProgramBuilder,
    cursors: &Cursors,
    membership: &Membership,
) {
    let id_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cursors.table,
        rowid_reg: id_reg,
        prev_largest_reg: 0,
    });
    let first_column_reg = program.alloc_registers(AUTH_MEMBERS_COLUMN_COUNT);
    // The id column is the rowid, which the record stores as NULL.
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    let columns = [
        membership.role.get(),
        membership.member.get(),
        membership.grantor.get(),
        membership.admin_option as i64,
        membership.inherit_option as i64,
        membership.set_option as i64,
    ];
    for (offset, value) in columns.into_iter().enumerate() {
        program.emit_insn(Insn::Integer {
            value,
            dest: first_column_reg + 1 + offset,
        });
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(AUTH_MEMBERS_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursors.table,
        key_reg: id_reg,
        record_reg,
        flag: InsertFlags::new().skip_last_rowid(),
        table_name: AUTH_MEMBERS_TABLE_NAME.to_string(),
    });
    emit_role_member_index_insert(program, cursors, membership, id_reg);
    MembershipGranted.emit(program, id_reg, first_column_reg);
}

/// Adds `membership`, stored under the rowid in `id_reg`, to the role and
/// member index, failing if the table already has the membership. The
/// in-memory catalog has already skipped a membership it knows, so this
/// only fails if the stored table has one the catalog does not.
fn emit_role_member_index_insert(
    program: &mut ProgramBuilder,
    cursors: &Cursors,
    membership: &Membership,
    id_reg: usize,
) {
    let key_reg = emit_role_member_key(program, membership, id_reg);
    emit_unique_index_insert(
        program,
        cursors.role_member_index,
        AUTH_MEMBERS_ROLE_MEMBER_INDEX_NAME,
        key_reg,
        4,
        format!("{AUTH_MEMBERS_TABLE_NAME} already has the membership"),
    );
}

/// Deletes `membership` and tells the engine to remove it from the
/// in-memory catalog. The catalog was loaded from the table, so the row must
/// be there.
fn emit_membership_delete(
    program: &mut ProgramBuilder,
    cursors: &Cursors,
    membership: &Membership,
) {
    let id = membership.id;
    let id_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: id,
        dest: id_reg,
    });
    let found = program.allocate_label();
    let missing = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: cursors.table,
        rowid_reg: id_reg,
        target_pc: missing,
    });
    program.emit_insn(Insn::Goto { target_pc: found });
    program.preassign_label_to_next_insn(missing);
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: SQLITE_ERROR,
        description: format!("{AUTH_MEMBERS_TABLE_NAME} has no membership with id {id}"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(found);
    let key_reg = emit_role_member_key(program, membership, id_reg);
    program.emit_insn(Insn::IdxDelete {
        start_reg: key_reg,
        num_regs: 4,
        cursor_id: cursors.role_member_index,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: cursors.table,
        table_name: AUTH_MEMBERS_TABLE_NAME.to_string(),
        is_part_of_update: true,
    });
    MembershipRevoked.emit(program, id_reg);
}

/// A membership that `GRANT` inserted.
#[derive(Debug)]
struct MembershipGranted;

impl MembershipGranted {
    /// `id_reg` holds the id the membership was stored under, and the
    /// registers from `first_column_reg` hold the columns of its record.
    /// The values are the columns of [`schema_state_query`].
    fn emit(self, program: &mut ProgramBuilder, id_reg: usize, first_column_reg: usize) {
        let value_regs = std::iter::once(id_reg)
            .chain((1..AUTH_MEMBERS_COLUMN_COUNT).map(|offset| first_column_reg + offset))
            .collect();
        emit_catalog_apply(program, self, value_regs);
    }

    fn apply_to(&self, memberships: &mut Memberships, values: &[Value]) -> turso_core::Result<()> {
        let membership = membership_from_row(values).map_err(|_| {
            LimboError::InternalError(format!(
                "GRANT read back {values:?} where a membership was written"
            ))
        })?;
        memberships.add(membership);
        Ok(())
    }
}

/// A membership that `REVOKE` deleted.
#[derive(Debug)]
struct MembershipRevoked;

impl MembershipRevoked {
    /// `id_reg` holds the id of the deleted membership.
    fn emit(self, program: &mut ProgramBuilder, id_reg: usize) {
        emit_catalog_apply(program, self, vec![id_reg]);
    }

    fn apply_to(&self, memberships: &mut Memberships, values: &[Value]) -> turso_core::Result<()> {
        let [Value::Numeric(Numeric::Integer(id))] = values else {
            return Err(LimboError::InternalError(format!(
                "REVOKE read back {values:?} where a membership id was written"
            )));
        };
        let removed = memberships.by_id.remove(id);
        assert!(
            removed.is_some(),
            "REVOKE removed membership {id}, which is not loaded"
        );
        Ok(())
    }
}

macro_rules! apply_to_memberships {
    ($($change:ty),*) => {
        $(
            impl CatalogApply for $change {
                fn apply(&self, schema: &mut Schema, values: &[Value]) -> turso_core::Result<()> {
                    change_schema_state(schema, |state| {
                        self.apply_to(Arc::make_mut(&mut state.roles).memberships_mut(), values)
                    })
                }
            }
        )*
    };
}

apply_to_memberships!(MembershipGranted, MembershipRevoked);

/// Fills four new registers with the role and member index key of
/// `membership`, stored under the rowid in `id_reg`, and returns the first.
fn emit_role_member_key(
    program: &mut ProgramBuilder,
    membership: &Membership,
    id_reg: usize,
) -> usize {
    let key_reg = program.alloc_registers(4);
    for (offset, role) in [membership.role, membership.member, membership.grantor]
        .into_iter()
        .enumerate()
    {
        program.emit_insn(Insn::Integer {
            value: role.get(),
            dest: key_reg + offset,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: id_reg,
        dst_reg: key_reg + 3,
        extra_amount: 0,
    });
    key_reg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reached_from_follows_chains_of_memberships() {
        // 5 is a member of 4, which is a member of 3 without inheriting its
        // privileges, which is a member of 2.
        let memberships = memberships(&[(1, 2, 3, true), (2, 3, 4, false), (3, 4, 5, true)]);
        let id = RoleId::from_stored;

        assert_eq!(
            memberships.reached_from(id(5), |_| true),
            vec![id(5), id(4), id(3), id(2)]
        );
        assert_eq!(
            memberships.reached_from(id(5), |membership| membership.inherit_option),
            vec![id(5), id(4)]
        );
        assert_eq!(memberships.reached_from(id(2), |_| true), vec![id(2)]);
    }

    #[test]
    fn new_rejects_a_duplicate_membership() {
        let error = Memberships::new(&[row(1, 2, 3, true), row(2, 2, 3, true)]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn new_rejects_an_invalid_row() {
        let mut invalid = row(1, 2, 3, true);
        invalid[5] = Value::from_i64(2);

        let error = Memberships::new(&[invalid]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn apply_grant_and_revoke_change_the_memberships() {
        let mut memberships = memberships(&[(1, 2, 3, true)]);

        MembershipGranted
            .apply_to(&mut memberships, &row(7, 4, 3, false))
            .unwrap();
        MembershipRevoked
            .apply_to(&mut memberships, &[Value::from_i64(1)])
            .unwrap();

        let ids: Vec<_> = memberships.iter().map(|membership| membership.id).collect();
        assert_eq!(ids, vec![7]);
        assert!(memberships
            .get(RoleId::from_stored(4), RoleId::from_stored(3))
            .is_some());
    }

    /// Memberships of `(id, role, member, inherit_option)`.
    fn memberships(rows: &[(i64, i64, i64, bool)]) -> Memberships {
        let rows: Vec<_> = rows
            .iter()
            .map(|&(id, role, member, inherit)| row(id, role, member, inherit))
            .collect();
        Memberships::new(&rows).unwrap()
    }

    fn row(id: i64, role: i64, member: i64, inherit: bool) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::from_i64(role),
            Value::from_i64(member),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(inherit as i64),
            Value::from_i64(1),
        ]
    }
}
