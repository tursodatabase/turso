//! Bytecode for role statements, and the privilege checks that run when a
//! statement is prepared.
//!
//! Privileges are checked when a statement is prepared, against the role that
//! is current at that time. Changing the role makes prepared statements
//! prepare again, so a statement always runs with the privileges of the
//! current role.
//!
//! A statement needs a privilege on every object it uses, which the object's
//! access control list grants, or it must be run by the object's owner or a
//! superuser. Every role may read the catalog: the schema table, the types
//! table and the role table.
//!
//! Only statements from the user are checked. Statements that the engine
//! prepares for itself run SQL that the engine wrote, such as reads of
//! sequence state or table statistics to load the schema. The one such
//! statement whose rows reach the user, the PRAGMA that a PRAGMA table
//! function runs, is checked when the function is compiled.

use crate::namespace::{NamespaceId, NamespaceRef};
use turso_ext::VTabKind;
use turso_parser::ast;

use std::sync::Arc;

use crate::schema::{BTreeTable, Schema, SEQ_BACKING_TABLE_PREFIX};
use crate::security::acl_catalog::{ObjectKey, ACL_TABLE_NAME, CREATE_ACL_TABLE_SQL};
use crate::security::permission::{Action, Object, PermissionDenied};
use crate::security::privilege::{ObjectAccess, ObjectKind, Privilege, Privileges};
use crate::security::roles::{
    RoleId, CREATE_ROLES_NAME_INDEX_SQL, CREATE_ROLES_TABLE_SQL, ROLES_NAME_INDEX_NAME,
    ROLES_TABLE_NAME,
};
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_create_internal_table, emit_schema_cookie};
use crate::vdbe::builder::{CursorType, ProgramBuilder, ViewRef};
use crate::vdbe::insn::{to_u32, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vtab::{VirtualTable, VirtualTableType};
use crate::{bail_parse_error, Connection, LimboError, Result, MAIN_DB_ID};
use turso_parser::ast::TableInternalId;

const MAIN_SCHEMA_NAME: &str = "main";

/// Fails if the current role may not run `stmt` at all. Statements that pass
/// are checked again by [`check_storage_access`] once they are compiled.
pub fn check_statement_privileges(stmt: &ast::Stmt, resolver: &Resolver) -> Result<()> {
    let Some(role) = resolver.checked_role else {
        return Ok(());
    };
    let Some(requirement) = statement_requirement(stmt, resolver)? else {
        return Ok(());
    };
    if requirement.is_met_by(role, resolver) {
        Ok(())
    } else {
        Err(permission_denied(requirement.denial))
    }
}

/// What a statement needs from the current role on one object.
enum Need {
    /// Any one of these privileges.
    Privileges(Privileges),
    /// To own the object.
    Owner,
    /// To be the superuser. Used for what no privilege covers, such as
    /// maintenance of the whole database.
    Superuser,
}

struct Requirement {
    database_id: usize,
    key: ObjectKey,
    need: Need,
    /// Reported when the requirement is not met.
    denial: PermissionDenied,
}

impl Requirement {
    fn privilege(
        database_id: usize,
        key: ObjectKey,
        privilege: Privilege,
        denial: PermissionDenied,
    ) -> Self {
        Self {
            database_id,
            key,
            need: Need::Privileges(Privileges::of(privilege)),
            denial,
        }
    }

    fn owner(database_id: usize, key: ObjectKey, denial: PermissionDenied) -> Self {
        Self {
            database_id,
            key,
            need: Need::Owner,
            denial,
        }
    }

    fn superuser(denial: PermissionDenied) -> Self {
        Self {
            database_id: MAIN_DB_ID,
            key: ObjectKey::root(ObjectKind::Database, MAIN_SCHEMA_NAME),
            need: Need::Superuser,
            denial,
        }
    }

    /// Whether `role`, which is not a superuser, meets the requirement.
    fn is_met_by(&self, role: RoleId, resolver: &Resolver) -> bool {
        let access = object_access(resolver, self.database_id, &self.key);
        match self.need {
            Need::Privileges(privileges) => !access
                .acl
                .privileges_of(role)
                .intersection(privileges)
                .is_empty(),
            Need::Owner => access.is_owner(role),
            Need::Superuser => false,
        }
    }
}

/// The owner and list of the object `key` in database `database_id`. An
/// object without a list is owned by the superuser and has the default list
/// of its kind.
fn object_access(resolver: &Resolver, database_id: usize, key: &ObjectKey) -> ObjectAccess {
    resolver
        .with_schema(database_id, |schema| schema.acls.get(key).cloned())
        .unwrap_or_else(|| ObjectAccess::default_for(key.kind, RoleId::SUPERUSER))
}

/// Returns what `stmt` needs before it is compiled, or `None` for a
/// statement whose accesses are all checked on the compiled program.
fn statement_requirement(stmt: &ast::Stmt, resolver: &Resolver) -> Result<Option<Requirement>> {
    let requirement = match stmt {
        ast::Stmt::Select(_)
        | ast::Stmt::Insert { .. }
        | ast::Stmt::Update(_)
        | ast::Stmt::Delete { .. }
        | ast::Stmt::Begin { .. }
        | ast::Stmt::Commit { .. }
        | ast::Stmt::Rollback { .. }
        | ast::Stmt::Savepoint { .. }
        | ast::Stmt::Release { .. } => return Ok(None),
        // Checked when it is translated, against the owner and the grant
        // options of the object.
        ast::Stmt::Grant(_) => return Ok(None),
        ast::Stmt::CreateTable {
            temporary: true, ..
        }
        | ast::Stmt::CreateView {
            temporary: true, ..
        }
        | ast::Stmt::CreateTrigger {
            temporary: true, ..
        } => Requirement::privilege(
            MAIN_DB_ID,
            ObjectKey::root(ObjectKind::Database, MAIN_SCHEMA_NAME),
            Privilege::Temporary,
            PermissionDenied::privilege(
                Action::CreateTemporary,
                Object::Database(MAIN_SCHEMA_NAME.to_string()),
            ),
        ),
        ast::Stmt::CreateTable { tbl_name, .. } => create_in_schema(resolver, tbl_name)?,
        ast::Stmt::CreateView { view_name, .. }
        | ast::Stmt::CreateMaterializedView { view_name, .. } => {
            create_in_schema(resolver, view_name)?
        }
        ast::Stmt::CreateVirtualTable(create) => create_in_schema(resolver, &create.tbl_name)?,
        ast::Stmt::CreateSequence { seq_name, .. } => create_in_schema(resolver, seq_name)?,
        ast::Stmt::CreateType { type_name, .. } => create_in_schema(
            resolver,
            &ast::QualifiedName::single(ast::Name::exact(type_name.clone())),
        )?,
        ast::Stmt::CreateDomain { domain_name, .. } => create_in_schema(
            resolver,
            &ast::QualifiedName::single(ast::Name::exact(domain_name.clone())),
        )?,
        ast::Stmt::CreateIndex { tbl_name, .. } => {
            let Ok(namespace) = resolver.resolve_existing_table_named(tbl_name.as_str()) else {
                return Ok(None);
            };
            Requirement::owner(
                namespace.database_id,
                ObjectKey::new(ObjectKind::Table, namespace.id, tbl_name.as_str()),
                PermissionDenied::owner(
                    Action::Alter,
                    Object::Table(tbl_name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::CreateTrigger { tbl_name, .. } => {
            let Ok(namespace) = resolver.resolve_existing_table(tbl_name) else {
                return Ok(None);
            };
            Requirement::privilege(
                namespace.database_id,
                ObjectKey::new(ObjectKind::Table, namespace.id, tbl_name.name.as_str()),
                Privilege::Trigger,
                PermissionDenied::privilege(
                    Action::CreateTrigger,
                    Object::Table(tbl_name.name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::AlterTable(alter) => {
            let Ok(namespace) = resolver.resolve_existing_table(&alter.name) else {
                return Ok(None);
            };
            Requirement::owner(
                namespace.database_id,
                ObjectKey::new(ObjectKind::Table, namespace.id, alter.name.name.as_str()),
                PermissionDenied::owner(
                    Action::Alter,
                    Object::Table(alter.name.name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::DropTable { tbl_name, .. } => {
            let Ok(namespace) = resolver.resolve_existing_table(tbl_name) else {
                return Ok(None);
            };
            Requirement::owner(
                namespace.database_id,
                ObjectKey::new(ObjectKind::Table, namespace.id, tbl_name.name.as_str()),
                PermissionDenied::owner(
                    Action::Drop,
                    Object::Table(tbl_name.name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::DropView { view_name, .. } => {
            let Ok(namespace) = resolver.resolve_existing_table(view_name) else {
                return Ok(None);
            };
            Requirement::owner(
                namespace.database_id,
                ObjectKey::new(ObjectKind::View, namespace.id, view_name.name.as_str()),
                PermissionDenied::owner(
                    Action::Drop,
                    Object::View(view_name.name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::DropIndex { idx_name, .. } => {
            let Ok(namespace) = resolver.resolve_existing_index(idx_name) else {
                return Ok(None);
            };
            let Some(table_name) = resolver.with_schema(namespace.database_id, |schema| {
                table_of_index(schema, namespace.id, idx_name.name.as_str())
            }) else {
                return Ok(None);
            };
            Requirement::owner(
                namespace.database_id,
                ObjectKey::new(ObjectKind::Table, namespace.id, table_name),
                PermissionDenied::owner(
                    Action::Drop,
                    Object::Index(idx_name.name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::DropTrigger { trigger_name, .. } => {
            let Ok(namespace) = resolver.resolve_existing_trigger(trigger_name) else {
                return Ok(None);
            };
            let Some(table_name) = resolver.with_schema(namespace.database_id, |schema| {
                schema
                    .get_trigger(namespace.id, trigger_name.name.as_str())
                    .map(|trigger| trigger.table_name.clone())
            }) else {
                return Ok(None);
            };
            Requirement::owner(
                namespace.database_id,
                ObjectKey::new(ObjectKind::Table, namespace.id, table_name),
                PermissionDenied::owner(
                    Action::Drop,
                    Object::Trigger(trigger_name.name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::DropType { type_name, .. } => Requirement::owner(
            MAIN_DB_ID,
            ObjectKey::root(ObjectKind::Type, type_name.to_string()),
            PermissionDenied::owner(Action::Drop, Object::Type(type_name.to_string())),
        ),
        ast::Stmt::DropDomain { domain_name, .. } => Requirement::owner(
            MAIN_DB_ID,
            ObjectKey::root(ObjectKind::Type, domain_name.to_string()),
            PermissionDenied::owner(Action::Drop, Object::Type(domain_name.to_string())),
        ),
        ast::Stmt::DropSequence { seq_name, .. } => {
            let Ok(namespace) = resolver.resolve_existing_sequence(seq_name.name.as_str()) else {
                return Ok(None);
            };
            Requirement::owner(
                namespace.database_id,
                ObjectKey::new(ObjectKind::Sequence, namespace.id, seq_name.name.as_str()),
                PermissionDenied::owner(
                    Action::Drop,
                    Object::Sequence(seq_name.name.as_str().to_string()),
                ),
            )
        }
        ast::Stmt::CreateSchema { .. } => Requirement::privilege(
            MAIN_DB_ID,
            ObjectKey::root(ObjectKind::Database, MAIN_SCHEMA_NAME),
            Privilege::Create,
            database_denial(Action::Create),
        ),
        ast::Stmt::DropSchema { name } => Requirement::owner(
            MAIN_DB_ID,
            ObjectKey::root(ObjectKind::Schema, name.as_str()),
            PermissionDenied::owner(Action::Drop, Object::Schema(name.as_str().to_string())),
        ),
        ast::Stmt::CreateRole { role_name } => Requirement::superuser(PermissionDenied::privilege(
            Action::Create,
            Object::Role(role_name.clone()),
        )),
        ast::Stmt::Pragma { name, body } => {
            let setting = name.name.as_str().to_lowercase();
            let (privilege, action) = match body {
                Some(_) => (Privilege::Set, Action::Write),
                None => (Privilege::Select, Action::Read),
            };
            Requirement::privilege(
                MAIN_DB_ID,
                ObjectKey::root(ObjectKind::Setting, setting.clone()),
                privilege,
                PermissionDenied::privilege(action, Object::Setting(setting)),
            )
        }
        ast::Stmt::Analyze { .. } => Requirement::superuser(database_denial(Action::Analyze)),
        ast::Stmt::Vacuum { .. } => Requirement::superuser(database_denial(Action::Vacuum)),
        ast::Stmt::Reindex { .. } => Requirement::superuser(database_denial(Action::Reindex)),
        ast::Stmt::Optimize { .. } => Requirement::superuser(database_denial(Action::Optimize)),
        ast::Stmt::Attach { .. } => Requirement::superuser(database_denial(Action::Attach)),
        ast::Stmt::Detach { .. } => Requirement::superuser(database_denial(Action::Detach)),
    };
    Ok(Some(requirement))
}

/// Fails if the statement is checked against a role that may not change
/// `sequence` with `nextval()` or `setval()`. The check runs when the call
/// is compiled, so a call in a view is checked too.
pub(crate) fn check_sequence_change(
    resolver: &Resolver,
    namespace: NamespaceRef,
    sequence: &str,
) -> Result<()> {
    let Some(role) = resolver.checked_role else {
        return Ok(());
    };
    let key = ObjectKey::new(ObjectKind::Sequence, namespace.id, sequence);
    let privileges = Privileges::from_iter([Privilege::Usage, Privilege::Update]);
    if holds_any(resolver, role, namespace.database_id, &key, privileges) {
        return Ok(());
    }
    Err(permission_denied(PermissionDenied::privilege(
        Action::Write,
        Object::Sequence(sequence.to_string()),
    )))
}

/// Fails if the statement is checked against a role that may not read a
/// table-valued PRAGMA function. The function runs `pragma_name` as a
/// statement of the engine's own, which is not checked, so the PRAGMA is
/// checked here as if it was run directly.
pub(crate) fn check_pragma_function(resolver: &Resolver, pragma_name: &str) -> Result<()> {
    let Some(role) = resolver.checked_role else {
        return Ok(());
    };
    let key = ObjectKey::root(ObjectKind::Setting, pragma_name.to_lowercase());
    if holds_any(
        resolver,
        role,
        MAIN_DB_ID,
        &key,
        Privileges::of(Privilege::Select),
    ) {
        return Ok(());
    }
    Err(permission_denied(PermissionDenied::privilege(
        Action::Read,
        Object::Setting(pragma_name.to_string()),
    )))
}

/// Whether `role` holds any of `privileges` on the object `key`.
fn holds_any(
    resolver: &Resolver,
    role: RoleId,
    database_id: usize,
    key: &ObjectKey,
    privileges: Privileges,
) -> bool {
    !object_access(resolver, database_id, key)
        .acl
        .privileges_of(role)
        .intersection(privileges)
        .is_empty()
}

/// The requirement for creating an object named `name`: the `CREATE`
/// privilege on the schema it is created in.
fn create_in_schema(resolver: &Resolver, name: &ast::QualifiedName) -> Result<Requirement> {
    let namespace = resolver.resolve_namespace_for_create(name)?;
    let schema_name = schema_name_of(resolver, namespace);
    Ok(Requirement::privilege(
        namespace.database_id,
        ObjectKey::root(ObjectKind::Schema, schema_name.clone()),
        Privilege::Create,
        PermissionDenied::privilege(Action::Create, Object::Schema(schema_name)),
    ))
}

/// The name of a namespace: the database's name for its root, such as
/// `main`, or the schema's name.
fn schema_name_of(resolver: &Resolver, namespace: NamespaceRef) -> String {
    if namespace.id.is_root() {
        return resolver
            .namespace_name(namespace.database_id)
            .unwrap_or_else(|| MAIN_SCHEMA_NAME.to_string());
    }
    resolver
        .with_schema(namespace.database_id, |schema| {
            schema.namespace_name(namespace.id).map(str::to_string)
        })
        .unwrap_or_else(|| MAIN_SCHEMA_NAME.to_string())
}

fn table_of_index(schema: &Schema, namespace: NamespaceId, index_name: &str) -> Option<String> {
    schema.tables(namespace).find_map(|table| {
        let btree = table.btree()?;
        schema
            .get_indices(namespace, &btree.name)
            .any(|index| index.name == index_name)
            .then(|| btree.name.clone())
    })
}

fn database_denial(action: Action) -> PermissionDenied {
    PermissionDenied::privilege(action, Object::Database(MAIN_SCHEMA_NAME.to_string()))
}

/// The privilege a statement needs on a table it writes.
pub fn write_privilege(stmt: &ast::Stmt) -> Privilege {
    match stmt {
        ast::Stmt::Update(_) => Privilege::Update,
        ast::Stmt::Delete { .. } => Privilege::Delete,
        _ => Privilege::Insert,
    }
}

/// Fails if the compiled program reads or writes a database object that the
/// current role has no privileges on. `write` is the privilege the program
/// needs on a table it writes.
pub fn check_storage_access(
    program: &ProgramBuilder,
    resolver: &Resolver,
    write: Privilege,
) -> Result<()> {
    match storage_denial(program, resolver, write) {
        None => Ok(()),
        Some(denial) => Err(permission_denied(denial)),
    }
}

/// Returns what the compiled program reads or writes that the current role
/// has no privileges on. Every access to stored data goes through one of the
/// instructions checked here, so a statement cannot reach an object without
/// being checked. A trigger or foreign-key action is compiled into a
/// subprogram, whose denial the subprogram's compiler records in `program`
/// and which is returned after the program's own accesses pass.
pub fn storage_denial(
    program: &ProgramBuilder,
    resolver: &Resolver,
    write: Privilege,
) -> Option<PermissionDenied> {
    let current_role = resolver.checked_role?;
    // The query of a view runs with the privileges of the view's owner, as
    // in PostgreSQL, so a role may read a table through a view it may read
    // without reading the table itself.
    let owner_of_view = |view: &ViewRef| {
        let key = ObjectKey::new(ObjectKind::View, view.namespace.id, view.name.as_str());
        object_access(resolver, view.namespace.database_id, &key).owner
    };
    let role_for_table_reference = |table_reference: Option<TableInternalId>| {
        table_reference
            .and_then(|id| program.tables_read_through_views.get(&id))
            .map_or(current_role, owner_of_view)
    };
    let allows = |role: RoleId, database_id: usize, key: &ObjectKey, privileges: Privileges| {
        resolver.schema().roles.is_superuser(role)
            || !object_access(resolver, database_id, key)
                .acl
                .privileges_of(role)
                .intersection(privileges)
                .is_empty()
    };
    for read in &program.referenced_views {
        let role = read.through.as_ref().map_or(current_role, owner_of_view);
        let key = ObjectKey::new(
            ObjectKind::View,
            read.view.namespace.id,
            read.view.name.as_str(),
        );
        if !allows(
            role,
            read.view.namespace.database_id,
            &key,
            Privileges::of(Privilege::Select),
        ) {
            return Some(PermissionDenied::privilege(
                Action::Read,
                Object::View(read.view.name.clone()),
            ));
        }
    }
    for virtual_table in &program.written_virtual_tables {
        let key = ObjectKey::root(ObjectKind::Table, virtual_table.name.as_str());
        if !allows(current_role, MAIN_DB_ID, &key, Privileges::of(write)) {
            return Some(PermissionDenied::privilege(
                Action::Write,
                Object::Table(virtual_table.name.clone()),
            ));
        }
    }
    for (insn, _) in &program.insns {
        let (action, db, root_page, cursor_id) = match insn {
            Insn::OpenRead {
                cursor_id,
                db,
                root_page,
                ..
            } => (Action::Read, *db, *root_page, Some(*cursor_id)),
            Insn::OpenWrite {
                cursor_id,
                db,
                root_page: RegisterOrLiteral::Literal(root_page),
                ..
            } => (Action::Write, *db, *root_page, Some(*cursor_id)),
            Insn::ClearBtree { db, root, .. } => (Action::Write, *db, *root, None),
            // A table being created or destroyed was checked with the
            // statement, and its root page is not in the schema yet.
            _ => continue,
        };
        let table_reference = cursor_id
            .and_then(|cursor_id| program.cursor_ref.get(cursor_id))
            .and_then(|(key, _)| key.as_ref())
            .map(|key| key.table_reference_id);
        let role = role_for_table_reference(table_reference);
        let stored = resolver.with_schema(db, |schema| object_with_root_page(schema, root_page));
        let (key, object) = match stored {
            StoredObject::Object { key, object } => (key, object),
            StoredObject::EngineOwned => continue,
            StoredObject::Unknown => {
                return Some(PermissionDenied::privilege(action, Object::Storage))
            }
        };
        if action == Action::Write && crate::schema::is_system_table(&key.name) {
            // The engine writes the schema and its own bookkeeping tables
            // on behalf of a statement that was checked already.
            continue;
        }
        let privileges = match (key.kind, action) {
            (ObjectKind::Sequence, Action::Read) => {
                Privileges::from_iter([Privilege::Usage, Privilege::Select])
            }
            (ObjectKind::Sequence, _) => {
                Privileges::from_iter([Privilege::Usage, Privilege::Update])
            }
            (_, Action::Read) => Privileges::of(Privilege::Select),
            (_, _) => Privileges::of(write),
        };
        if !allows(role, db, &key, privileges) {
            return Some(PermissionDenied::privilege(action, object));
        }
    }
    for (table_reference, virtual_table) in &program.opened_virtual_tables {
        let role = role_for_table_reference(*table_reference);
        let (key, privilege, denial) = virtual_table_requirement(resolver, virtual_table);
        if !allows(role, MAIN_DB_ID, &key, Privileges::of(privilege)) {
            return Some(denial);
        }
    }
    program.subprogram_denial.clone()
}

/// What reading a virtual table needs. A `pragma_*()` function is the
/// setting it shows. A table-valued function is a function, unless the
/// dialect registered the privileges of a table under its name, as it does
/// for a catalog table. Anything else is a table.
fn virtual_table_requirement(
    resolver: &Resolver,
    virtual_table: &VirtualTable,
) -> (ObjectKey, Privilege, PermissionDenied) {
    let name = virtual_table.name.as_str();
    if let VirtualTableType::Pragma(_) = &virtual_table.vtab_type {
        let setting = name.strip_prefix("pragma_").unwrap_or(name).to_lowercase();
        return (
            ObjectKey::root(ObjectKind::Setting, setting),
            Privilege::Select,
            PermissionDenied::privilege(Action::Execute, Object::Function(name.to_string())),
        );
    }
    let (namespace, bare_name) = resolver.schema().demangle_name(name);
    let table_key = ObjectKey::new(ObjectKind::Table, namespace, bare_name);
    let registered_as_table = resolver.schema().acls.get(&table_key).is_some();
    if virtual_table.kind == VTabKind::TableValuedFunction && !registered_as_table {
        return (
            ObjectKey::root(ObjectKind::Function, name),
            Privilege::Execute,
            PermissionDenied::privilege(Action::Execute, Object::Function(name.to_string())),
        );
    }
    (
        table_key,
        Privilege::Select,
        PermissionDenied::privilege(Action::Read, Object::Table(bare_name.to_string())),
    )
}

fn permission_denied(denial: PermissionDenied) -> LimboError {
    LimboError::PermissionDenied(Box::new(denial))
}

/// What a root page holds, for a privilege check.
enum StoredObject {
    Object {
        key: ObjectKey,
        /// How a denial names the object.
        object: Object,
    },
    /// Bookkeeping the engine keeps for other objects, such as the state of
    /// a materialized view, which the privileges of those objects cover.
    EngineOwned,
    /// No object of the schema has this root page.
    Unknown,
}

/// Returns the object stored at `root_page`. An index is reported as the
/// table it belongs to, and the backing table of a sequence as the sequence.
fn object_with_root_page(schema: &Schema, root_page: i64) -> StoredObject {
    let found = schema.namespace_ids().find_map(|namespace| {
        schema.tables(namespace).find_map(|table| {
            let btree = table.btree()?;
            let has_root_page = btree.root_page == root_page
                || schema
                    .get_indices(namespace, &btree.name)
                    .any(|index| index.root_page == root_page);
            has_root_page.then(|| (namespace, btree.name.clone()))
        })
    });
    let Some((namespace, table_name)) = found else {
        return StoredObject::Unknown;
    };
    if let Some(sequence_name) = table_name.strip_prefix(SEQ_BACKING_TABLE_PREFIX) {
        let (sequence_namespace, sequence_name) = schema.demangle_name(sequence_name);
        return StoredObject::Object {
            key: ObjectKey::new(ObjectKind::Sequence, sequence_namespace, sequence_name),
            object: Object::Sequence(sequence_name.to_string()),
        };
    }
    if crate::schema::is_materialized_view_state_table(&table_name)
        || table_name == crate::schema::SQLITE_SEQUENCE_TABLE_NAME
    {
        return StoredObject::EngineOwned;
    }
    if schema.is_materialized_view(namespace, &table_name) {
        return StoredObject::Object {
            key: ObjectKey::new(ObjectKind::View, namespace, table_name.as_str()),
            object: Object::MaterializedView(table_name),
        };
    }
    StoredObject::Object {
        key: ObjectKey::new(ObjectKind::Table, namespace, table_name.as_str()),
        object: Object::Table(table_name),
    }
}

/// Creates a role that is not a superuser and cannot log in.
pub fn translate_create_role(
    role_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !resolver.catalog.dialect.supports_roles() {
        bail_parse_error!("CREATE ROLE is not supported");
    }
    if resolver.schema().roles.get_by_name(role_name).is_some() {
        bail_parse_error!("role \"{role_name}\" already exists");
    }
    let superuser = false;

    let existing = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, ROLES_TABLE_NAME);
    let (roles_table, roles_root_page, (name_index, name_index_root_page)) = match existing {
        Some(table) => {
            let name_index = resolver
                .schema()
                .get_indices(NamespaceId::ROOT, ROLES_TABLE_NAME)
                .find(|index| index.name == ROLES_NAME_INDEX_NAME)
                .cloned()
                .ok_or_else(|| {
                    LimboError::Corrupt(format!("{ROLES_TABLE_NAME} has no name index"))
                })?;
            emit_schema_cookie(resolver, program);
            let root_page = RegisterOrLiteral::Literal(table.root_page);
            let name_index_root_page = RegisterOrLiteral::Literal(name_index.root_page);
            (table, root_page, (name_index, name_index_root_page))
        }
        // Creating the table bumps the schema cookie too.
        None => {
            let mut created = emit_create_internal_table(
                program,
                resolver,
                ROLES_TABLE_NAME,
                CREATE_ROLES_TABLE_SQL,
                &[CREATE_ROLES_NAME_INDEX_SQL],
            )?;
            let name_index = created.indexes.pop().expect("one index was created");
            (created.table, created.root_page, name_index)
        }
    };
    let roles_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(roles_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: roles_cursor_id,
        root_page: roles_root_page,
        db: MAIN_DB_ID,
    });
    let name_index_cursor_id = program.alloc_cursor_index(None, &name_index)?;
    program.emit_insn(Insn::OpenWrite {
        cursor_id: name_index_cursor_id,
        root_page: name_index_root_page,
        db: MAIN_DB_ID,
    });

    let id_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: roles_cursor_id,
        rowid_reg: id_reg,
        prev_largest_reg: 0,
    });
    let first_column_reg = program.alloc_registers(4);
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::String8 {
        value: role_name.to_string(),
        dest: first_column_reg + 1,
    });
    program.emit_insn(Insn::Integer {
        value: superuser as i64,
        dest: first_column_reg + 2,
    });
    let dropped = false;
    program.emit_insn(Insn::Integer {
        value: dropped as i64,
        dest: first_column_reg + 3,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(4),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: roles_cursor_id,
        key_reg: id_reg,
        record_reg,
        flag: InsertFlags::new().skip_last_rowid(),
        table_name: ROLES_TABLE_NAME.to_string(),
    });
    emit_role_name_index_entry(
        program,
        name_index_cursor_id,
        &name_index.name,
        role_name,
        id_reg,
    );

    program.emit_insn(Insn::AddRole {
        db: MAIN_DB_ID,
        id_reg,
        name: role_name.to_string(),
        superuser,
    });
    Ok(())
}

/// Adds the role's name to the name index, failing if another role has the
/// name. The in-memory catalog has already rejected a name it knows, so this
/// only fails if the stored table has a role the catalog does not.
fn emit_role_name_index_entry(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    index_name: &str,
    role_name: &str,
    id_reg: usize,
) {
    let key_reg = program.alloc_registers(2);
    program.emit_insn(Insn::String8 {
        value: role_name.to_string(),
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
        err_code: crate::error::SQLITE_CONSTRAINT_UNIQUE,
        description: format!("role \"{role_name}\" already exists"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(no_conflict);
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(key_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: Some(index_name.to_string()),
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

/// Grants or revokes privileges on objects. The grantor is the current role,
/// which must own the object or hold the grant option for every privilege.
/// A superuser grants as the owner. The new list is written to the ACL
/// table, replacing the object's row if it has one, and the in-memory
/// catalog is updated when the statement runs.
pub fn translate_grant(
    grant: &ast::GrantStmt,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Connection,
) -> Result<()> {
    let verb = if grant.is_grant { "GRANT" } else { "REVOKE" };
    if !resolver.catalog.dialect.supports_roles() {
        bail_parse_error!("{verb} is not supported");
    }
    let roles = &resolver.schema().roles;
    let grantor = connection.current_role();
    let grantor_is_superuser = roles.is_superuser(grantor);
    let grantees = grant
        .grantees
        .iter()
        .map(|grantee| match grantee {
            ast::Grantee::Public => Ok(RoleId::PUBLIC),
            ast::Grantee::Role(name) => roles
                .get_by_name(name)
                .map(|role| role.id)
                .ok_or_else(|| LimboError::ParseError(format!("role \"{name}\" does not exist"))),
        })
        .collect::<Result<Vec<RoleId>>>()?;

    // Resolve and check every object before emitting anything, so that a
    // statement that fails changes nothing.
    let mut changes = Vec::with_capacity(grant.objects.len());
    for object in &grant.objects {
        let (database_id, key, display) = resolve_grant_object(resolver, object)?;
        if database_id != MAIN_DB_ID {
            bail_parse_error!("{verb} on an attached database is not supported");
        }
        let privileges = privileges_for(grant, key.kind)?;
        let mut access = object_access(resolver, database_id, &key);
        let may_grant = access.is_owner(grantor)
            || access
                .acl
                .grant_options_of(grantor)
                .contains_all(privileges);
        let effective_grantor = if grantor_is_superuser {
            access.owner
        } else if may_grant {
            grantor
        } else {
            return Err(permission_denied(PermissionDenied::privilege(
                Action::Grant,
                display,
            )));
        };
        for grantee in &grantees {
            if grant.is_grant {
                let grant_option = if grant.grant_option {
                    privileges
                } else {
                    Privileges::NONE
                };
                access
                    .acl
                    .grant(*grantee, privileges, grant_option, effective_grantor);
            } else {
                access
                    .acl
                    .revoke(*grantee, privileges, grant.grant_option, effective_grantor);
            }
        }
        let rowid = resolver.schema().acls.stored_rowid(&key);
        changes.push((key, access, rowid));
    }

    let (acl_table, acl_root_page) = emit_acl_table(resolver, program)?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(acl_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: acl_root_page,
        db: MAIN_DB_ID,
    });
    for (key, access, rowid) in changes {
        let id_reg = program.alloc_register();
        match rowid {
            // The object has a row: seek to it, so that the insert below
            // replaces it.
            Some(rowid) => {
                program.emit_insn(Insn::Integer {
                    value: rowid,
                    dest: id_reg,
                });
                let missing = program.allocate_label();
                let write = program.allocate_label();
                program.emit_insn(Insn::NotExists {
                    cursor: cursor_id,
                    rowid_reg: id_reg,
                    target_pc: missing,
                });
                program.emit_insn(Insn::Goto { target_pc: write });
                program.preassign_label_to_next_insn(missing);
                program.emit_insn(Insn::Halt {
                    constraint_info: None,
                    err_code: crate::error::SQLITE_ERROR,
                    description: format!(
                        "{ACL_TABLE_NAME} has no row {rowid} for {} {}",
                        key.kind.stored_name(),
                        key.name
                    ),
                    on_error: None,
                    description_reg: None,
                });
                program.preassign_label_to_next_insn(write);
            }
            None => program.emit_insn(Insn::NewRowid {
                cursor: cursor_id,
                rowid_reg: id_reg,
                prev_largest_reg: 0,
            }),
        }
        let first_column_reg = program.alloc_registers(6);
        program.emit_insn(Insn::Null {
            dest: first_column_reg,
            dest_end: None,
        });
        program.emit_insn(Insn::String8 {
            value: key.kind.stored_name().to_string(),
            dest: first_column_reg + 1,
        });
        program.emit_insn(Insn::Integer {
            value: key.namespace.as_u32() as i64,
            dest: first_column_reg + 2,
        });
        program.emit_insn(Insn::String8 {
            value: key.name.clone(),
            dest: first_column_reg + 3,
        });
        program.emit_insn(Insn::Integer {
            value: access.owner.get(),
            dest: first_column_reg + 4,
        });
        program.emit_insn(Insn::String8 {
            value: access.acl.to_stored(),
            dest: first_column_reg + 5,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(first_column_reg),
            count: to_u32(6),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: id_reg,
            record_reg,
            flag: InsertFlags::new().skip_last_rowid(),
            table_name: ACL_TABLE_NAME.to_string(),
        });
        program.emit_insn(Insn::SetAcl {
            db: MAIN_DB_ID,
            id_reg,
            key,
            access,
        });
    }
    Ok(())
}

/// The privileges a GRANT or REVOKE names, checked against what the kind of
/// object can have. An empty list means `ALL`.
fn privileges_for(grant: &ast::GrantStmt, kind: ObjectKind) -> Result<Privileges> {
    let all = kind.all_privileges();
    if grant.privileges.is_empty() {
        return Ok(all);
    }
    grant
        .privileges
        .iter()
        .map(|name| {
            Privilege::from_name(name)
                .filter(|privilege| all.contains(*privilege))
                .ok_or_else(|| {
                    // PostgreSQL calls a table or view a relation here.
                    let kind_name = match kind {
                        ObjectKind::Table | ObjectKind::View => "relation",
                        kind => kind.stored_name(),
                    };
                    LimboError::ParseError(format!(
                        "invalid privilege type {} for {kind_name}",
                        name.to_uppercase()
                    ))
                })
        })
        .collect()
}

/// Resolves an object of GRANT or REVOKE to the database it is in, its key
/// in the ACL catalog, and how a denial names it.
fn resolve_grant_object(
    resolver: &Resolver,
    object: &ast::GrantObject,
) -> Result<(usize, ObjectKey, Object)> {
    Ok(match object {
        ast::GrantObject::Table(name) => {
            let namespace = resolver.resolve_existing_table(name)?;
            let table_name = name.name.as_str();
            let (kind, canonical_name, display) = resolver
                .with_schema(namespace.database_id, |schema| {
                    if let Some(view) = schema.get_view(namespace.id, table_name) {
                        return Some((
                            ObjectKind::View,
                            view.name.clone(),
                            Object::View(view.name.clone()),
                        ));
                    }
                    let table = schema.get_table(namespace.id, table_name)?;
                    let canonical = table.get_name().to_string();
                    let display = if schema.is_materialized_view(namespace.id, &canonical) {
                        Object::MaterializedView(canonical.clone())
                    } else {
                        Object::Table(canonical.clone())
                    };
                    let kind = if schema.is_materialized_view(namespace.id, &canonical) {
                        ObjectKind::View
                    } else {
                        ObjectKind::Table
                    };
                    Some((kind, canonical, display))
                })
                .ok_or_else(|| LimboError::ParseError(format!("no such table: {table_name}")))?;
            (
                namespace.database_id,
                ObjectKey::new(kind, namespace.id, canonical_name),
                display,
            )
        }
        ast::GrantObject::Sequence(name) => {
            let sequence_name = name.name.as_str();
            let namespace = resolver.resolve_existing_sequence(sequence_name)?;
            let canonical = resolver
                .with_schema(namespace.database_id, |schema| {
                    schema
                        .get_sequence(namespace.id, sequence_name)
                        .map(|sequence| sequence.name.clone())
                })
                .ok_or_else(|| {
                    LimboError::ParseError(format!("sequence \"{sequence_name}\" does not exist"))
                })?;
            (
                namespace.database_id,
                ObjectKey::new(ObjectKind::Sequence, namespace.id, canonical.as_str()),
                Object::Sequence(canonical),
            )
        }
        ast::GrantObject::Function(name) => {
            let function_name = name.as_str().to_lowercase();
            let is_table_valued = resolver
                .schema()
                .get_table(NamespaceId::ROOT, &function_name)
                .is_some();
            let is_scalar = (0..=3).any(|arg_count| {
                matches!(
                    resolver.resolve_function(&function_name, arg_count),
                    Ok(Some(_))
                )
            });
            if !is_table_valued && !is_scalar {
                bail_parse_error!("function {function_name} does not exist");
            }
            (
                MAIN_DB_ID,
                ObjectKey::root(ObjectKind::Function, function_name.as_str()),
                Object::Function(function_name),
            )
        }
        ast::GrantObject::Schema(name) => {
            let schema_name = crate::util::normalize_ident(name.as_str());
            let Some(namespace) = resolver.namespace_named(&schema_name) else {
                bail_parse_error!("schema \"{schema_name}\" does not exist");
            };
            let canonical = schema_name_of(resolver, namespace);
            (
                namespace.database_id,
                ObjectKey::root(ObjectKind::Schema, canonical.as_str()),
                Object::Schema(canonical),
            )
        }
        ast::GrantObject::Database => (
            MAIN_DB_ID,
            ObjectKey::root(ObjectKind::Database, MAIN_SCHEMA_NAME),
            Object::Database(MAIN_SCHEMA_NAME.to_string()),
        ),
        ast::GrantObject::Type(name) => {
            let type_name = name.as_str().to_string();
            if resolver
                .schema()
                .get_type_def_unchecked(&type_name)
                .is_none()
            {
                bail_parse_error!("type \"{type_name}\" does not exist");
            }
            (
                MAIN_DB_ID,
                ObjectKey::root(ObjectKind::Type, type_name.as_str()),
                Object::Type(type_name),
            )
        }
        ast::GrantObject::Setting(name) => {
            let setting = name.as_str().to_lowercase();
            (
                MAIN_DB_ID,
                ObjectKey::root(ObjectKind::Setting, setting.as_str()),
                Object::Setting(setting),
            )
        }
    })
}

/// Returns the ACL table and its root page, and bumps the schema cookie. If
/// the table does not exist yet, emits bytecode that creates it, which bumps
/// the cookie too, and returns the register that will hold its root page.
fn emit_acl_table(
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<(Arc<BTreeTable>, RegisterOrLiteral<i64>)> {
    if let Some(table) = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, ACL_TABLE_NAME)
    {
        emit_schema_cookie(resolver, program);
        let root_page = RegisterOrLiteral::Literal(table.root_page);
        return Ok((table, root_page));
    }
    let created =
        emit_create_internal_table(program, resolver, ACL_TABLE_NAME, CREATE_ACL_TABLE_SQL, &[])?;
    Ok((created.table, created.root_page))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use turso_parser::ast;

    use turso_ext::{
        ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabCursor, VTabKind, VTabModule,
        VTabModuleDerive, VTable,
    };

    use crate::native_ext::{TableUpdate, VirtualTable, VirtualTableCursor, VirtualTableModule};
    use crate::security::permission::{Action, Object, PermissionDenied};
    use crate::security::test_support::{
        become_new_role, connect_with_roles, connect_with_roles_and, SqliteWithRoles,
    };
    use crate::types::IOResultOr;
    use crate::{
        Connection, Database, IOResult, LimboError, MemoryIO, OpenOptions, SqliteDialect, Value,
    };

    #[test]
    fn create_role_stores_a_role_that_is_not_dropped() {
        let conn = connect_with_roles();

        become_new_role(&conn, "alice");

        conn.set_current_role(crate::security::roles::RoleId::SUPERUSER);
        let mut stmt = conn
            .prepare("SELECT id, name, superuser, dropped FROM __turso_internal_roles")
            .unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![
                Value::from_i64(1),
                Value::build_text("alice"),
                Value::from_i64(0),
                Value::from_i64(0),
            ]]
        );
    }

    #[test]
    fn create_role_fails_under_a_dialect_without_roles() {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(crate::ConnectOpts::new(crate::SessionRole::Superuser))
            .unwrap();

        let error = create_role(&conn, "alice").unwrap_err();

        assert!(
            error.to_string().contains("CREATE ROLE is not supported"),
            "{error}"
        );
    }

    #[test]
    fn role_without_privileges_may_use_only_virtual_tables_that_allow_it() {
        let conn = connect_with_roles();
        become_new_role(&conn, "alice");

        #[cfg(feature = "cli_only")]
        {
            let dbpage = conn.execute("SELECT * FROM sqlite_dbpage").unwrap_err();
            assert_eq!(
                denial(dbpage),
                PermissionDenied::privilege(
                    Action::Execute,
                    Object::Function("sqlite_dbpage".to_string())
                )
            );
        }
        #[cfg(feature = "json")]
        conn.execute("SELECT * FROM json_each('[1, 2]')").unwrap();
    }

    #[test]
    fn a_virtual_table_is_a_table_and_a_table_valued_function_is_a_function() {
        let options = OpenOptions::new(Arc::new(SqliteWithRoles))
            .native_module("rows", VTabKind::VirtualTable, EmptyModule)
            .native_module("rows_of", VTabKind::TableValuedFunction, EmptyModule);
        let conn = connect_with_roles_and(options);
        conn.execute("CREATE VIRTUAL TABLE rows_table USING rows")
            .unwrap();
        become_new_role(&conn, "alice");

        // Nothing granted SELECT on the table; every role may execute a
        // function by default.
        let table = conn.execute("SELECT * FROM rows_table").unwrap_err();
        assert_eq!(
            denial(table),
            PermissionDenied::privilege(Action::Read, Object::Table("rows_table".to_string()))
        );
        conn.execute("SELECT * FROM rows_of()").unwrap();
    }

    #[test]
    fn an_extension_table_valued_function_is_a_function_too() {
        // A table-valued function registered through a connection is added
        // to the database's schema, which the next connection starts from.
        let registering_conn = connect_with_roles();
        registering_conn.register_static_extension(|api| unsafe {
            ExtensionRows::register_ExtensionRows(api);
        });
        let conn = registering_conn
            .db
            .connect(crate::ConnectOpts::new(crate::SessionRole::Superuser))
            .unwrap();
        become_new_role(&conn, "alice");

        conn.execute("SELECT * FROM extension_rows()").unwrap();
        #[cfg(feature = "series")]
        conn.execute("SELECT * FROM generate_series(1, 3)").unwrap();
    }

    #[test]
    fn functions_registered_as_superuser_only_are_denied() {
        let conn = connect_with_roles();
        conn.set_load_extension_enabled(true);
        become_new_role(&conn, "alice");

        let error = conn.execute("SELECT load_extension('x')").unwrap_err();
        assert_eq!(
            denial(error),
            PermissionDenied::privilege(
                Action::Execute,
                Object::Function("load_extension".to_string())
            )
        );
    }

    #[test]
    fn every_role_may_read_the_catalog_tables() {
        let conn = connect_with_roles();
        conn.execute("CREATE TABLE t (x)").unwrap();
        become_new_role(&conn, "alice");

        conn.execute("SELECT name FROM sqlite_schema").unwrap();
        conn.execute("SELECT name FROM __turso_internal_roles")
            .unwrap();
        let error = conn.execute("SELECT x FROM t").unwrap_err();
        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()))
        );
    }

    #[test]
    fn storage_access_in_trigger_and_foreign_key_subprograms_is_denied() {
        let conn = connect_with_roles();
        conn.execute("PRAGMA foreign_keys = ON").unwrap();
        conn.execute("CREATE TABLE secret (x)").unwrap();
        conn.execute("CREATE TABLE log (x)").unwrap();
        conn.execute(
            "CREATE TRIGGER read_secret AFTER INSERT ON log BEGIN SELECT x FROM secret; END",
        )
        .unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (pid REFERENCES parent(id) ON DELETE CASCADE)")
            .unwrap();
        let alice = become_new_role(&conn, "alice");

        assert_eq!(
            subprogram_denial(&conn, alice, "INSERT INTO log VALUES (1)"),
            Some(PermissionDenied::privilege(
                Action::Read,
                Object::Table("secret".to_string())
            ))
        );
        assert_eq!(
            subprogram_denial(&conn, alice, "DELETE FROM parent"),
            Some(PermissionDenied::privilege(
                Action::Write,
                Object::Table("child".to_string())
            ))
        );
        assert_eq!(
            subprogram_denial(&conn, alice, "INSERT INTO secret VALUES (1)"),
            None
        );
    }

    /// Compiles `sql` as `translate()` does, up to the privilege checks, and
    /// returns the denial recorded from the statement's subprograms. The
    /// statement's own accesses are denied first, so the subprograms can only
    /// be checked this way until privileges can be granted.
    fn subprogram_denial(
        conn: &Arc<Connection>,
        role: crate::security::roles::RoleId,
        sql: &str,
    ) -> Option<PermissionDenied> {
        use crate::translate::emitter::Resolver;
        use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};

        let (Some(ast::Cmd::Stmt(stmt)), _) = conn.dialect().parse(sql).unwrap() else {
            panic!("{sql} is not a statement");
        };
        let schema = conn.schema.read().clone();
        let syms = conn.syms.read();
        let qualifiers = conn.qualifiers(&schema);
        let mut resolver = Resolver::new(
            &schema,
            conn.database_schemas(),
            &conn.temp.database,
            conn.attached_databases(),
            &qualifiers,
            &syms,
            conn.experimental_custom_types_enabled(),
            conn.get_dqs_dml().into(),
            conn.dialect(),
            None,
        );
        resolver.checked_role = Some(role);
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 2));
        program.set_mvcc_enabled(conn.mvcc_enabled());
        program.prologue();
        crate::translate::translate_inner(stmt, &mut resolver, &mut program, conn, sql).unwrap();
        program.subprogram_denial
    }

    #[test]
    fn role_without_privileges_cannot_change_virtual_tables() {
        let options = OpenOptions::new(Arc::new(SqliteWithRoles)).native_module(
            "open_rows",
            VTabKind::VirtualTable,
            EmptyModule,
        );
        let conn = connect_with_roles_and(options);
        conn.execute("CREATE VIRTUAL TABLE open_table USING open_rows")
            .unwrap();
        become_new_role(&conn, "alice");

        for sql in [
            "INSERT INTO open_table VALUES (1)",
            "UPDATE open_table SET value = 2",
            "DELETE FROM open_table",
        ] {
            let error = conn.execute(sql).unwrap_err();
            assert_eq!(
                denial(error),
                PermissionDenied::privilege(Action::Write, Object::Table("open_table".to_string())),
                "{sql}"
            );
        }
    }

    #[test]
    fn role_without_privileges_cannot_read_currval() {
        let conn = connect_with_roles();
        conn.execute("CREATE SEQUENCE s").unwrap();
        conn.execute("SELECT nextval('s')").unwrap();
        become_new_role(&conn, "alice");

        let error = conn.execute("SELECT currval('s')").unwrap_err();

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Use, Object::Sequence("s".to_string()))
        );
    }

    #[test]
    fn role_without_privileges_cannot_read_the_sequence_watermark() {
        let conn = connect_with_roles();
        conn.execute("CREATE SEQUENCE s").unwrap();
        become_new_role(&conn, "alice");

        let error = conn
            .execute("SELECT sequence_watermark_experimental('s')")
            .unwrap_err();

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Use, Object::Sequence("s".to_string()))
        );
    }

    #[test]
    fn grant_select_lets_a_role_read_a_table_and_revoke_takes_it_back() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        create_role(&conn, "alice").unwrap();
        grant(&conn, true, &["SELECT"], table("t"), &["alice"], false).unwrap();

        set_role(&conn, "alice").unwrap();
        assert_eq!(
            query_rows(&conn, "SELECT x FROM t"),
            vec![vec![Value::from_i64(1)]]
        );
        let insert = conn.execute("INSERT INTO t VALUES (2)").unwrap_err();
        assert_eq!(
            denial(insert),
            PermissionDenied::privilege(Action::Write, Object::Table("t".to_string()))
        );

        reset_role(&conn).unwrap();
        grant(&conn, false, &["SELECT"], table("t"), &["alice"], false).unwrap();
        set_role(&conn, "alice").unwrap();
        let select = conn.execute("SELECT x FROM t").unwrap_err();
        assert_eq!(
            denial(select),
            PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()))
        );
    }

    #[test]
    fn a_view_is_read_with_the_privileges_of_its_owner() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("CREATE VIEW v AS SELECT x FROM t").unwrap();
        conn.execute("CREATE VIEW vv AS SELECT x FROM v").unwrap();
        create_role(&conn, "alice").unwrap();
        grant(&conn, true, &["SELECT"], table("v"), &["alice"], false).unwrap();
        grant(&conn, true, &["SELECT"], table("vv"), &["alice"], false).unwrap();

        set_role(&conn, "alice").unwrap();

        assert_eq!(
            query_rows(&conn, "SELECT x FROM v"),
            vec![vec![Value::from_i64(1)]]
        );
        assert_eq!(
            query_rows(&conn, "SELECT x FROM vv"),
            vec![vec![Value::from_i64(1)]]
        );
        let direct = conn.execute("SELECT x FROM t").unwrap_err();
        assert_eq!(
            denial(direct),
            PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()))
        );
        let joined = conn
            .execute("SELECT v.x FROM v JOIN t ON t.x = v.x")
            .unwrap_err();
        assert_eq!(
            denial(joined),
            PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()))
        );
    }

    #[test]
    fn granted_privileges_survive_a_schema_reload() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        grant(
            &conn,
            true,
            &["SELECT", "INSERT"],
            table("t"),
            &["alice"],
            false,
        )
        .unwrap();

        conn.reparse_schema().unwrap();

        set_role(&conn, "alice").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(
            query_rows(&conn, "SELECT x FROM t"),
            vec![vec![Value::from_i64(1)]]
        );
        let delete = conn.execute("DELETE FROM t").unwrap_err();
        assert_eq!(
            denial(delete),
            PermissionDenied::privilege(Action::Write, Object::Table("t".to_string()))
        );
    }

    #[test]
    fn a_grant_to_public_applies_to_every_role() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        grant(&conn, true, &["SELECT"], table("t"), &["public"], false).unwrap();

        set_role(&conn, "alice").unwrap();

        conn.execute("SELECT x FROM t").unwrap();
    }

    #[test]
    fn revoking_a_registered_privilege_hides_a_catalog_table() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        grant(
            &conn,
            false,
            &["SELECT"],
            table("sqlite_schema"),
            &["public"],
            false,
        )
        .unwrap();

        set_role(&conn, "alice").unwrap();

        let error = conn.execute("SELECT name FROM sqlite_schema").unwrap_err();
        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Read, Object::Table("sqlite_schema".to_string()))
        );
    }

    #[test]
    fn grant_usage_on_a_sequence_lets_a_role_advance_it() {
        let conn = open_connection();
        conn.execute("CREATE SEQUENCE s").unwrap();
        create_role(&conn, "alice").unwrap();
        grant(
            &conn,
            true,
            &["USAGE"],
            ast::GrantObject::Sequence(ast::QualifiedName::single(ast::Name::exact("s".into()))),
            &["alice"],
            false,
        )
        .unwrap();

        set_role(&conn, "alice").unwrap();

        assert_eq!(
            query_rows(&conn, "SELECT nextval('s')"),
            vec![vec![Value::from_i64(1)]]
        );
        assert_eq!(
            query_rows(&conn, "SELECT currval('s')"),
            vec![vec![Value::from_i64(1)]]
        );
    }

    #[test]
    fn grant_execute_on_a_restricted_function_lets_a_role_call_it() {
        let conn = open_connection();
        conn.set_load_extension_enabled(true);
        create_role(&conn, "alice").unwrap();
        grant(
            &conn,
            true,
            &["EXECUTE"],
            ast::GrantObject::Function(ast::Name::exact("load_extension".into())),
            &["alice"],
            false,
        )
        .unwrap();

        set_role(&conn, "alice").unwrap();

        // The call fails because there is no such extension, not because of
        // the role.
        let error = conn
            .execute("SELECT load_extension('/nonexistent')")
            .unwrap_err();
        assert!(!matches!(error, LimboError::PermissionDenied(_)), "{error}");
    }

    #[test]
    fn grant_create_on_a_schema_lets_a_role_create_a_table() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        grant(
            &conn,
            true,
            &["CREATE"],
            ast::GrantObject::Schema(ast::Name::exact("main".into())),
            &["alice"],
            false,
        )
        .unwrap();

        set_role(&conn, "alice").unwrap();

        conn.execute("CREATE TABLE made_by_alice (x)").unwrap();
        conn.execute("CREATE SEQUENCE s").unwrap();
    }

    #[test]
    fn only_the_owner_or_a_role_with_the_grant_option_may_grant() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        create_role(&conn, "bob").unwrap();
        grant(&conn, true, &["SELECT"], table("t"), &["alice"], false).unwrap();

        set_role(&conn, "alice").unwrap();
        let error = grant(&conn, true, &["SELECT"], table("t"), &["bob"], false).unwrap_err();
        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Grant, Object::Table("t".to_string()))
        );

        reset_role(&conn).unwrap();
        grant(&conn, true, &["SELECT"], table("t"), &["alice"], true).unwrap();
        set_role(&conn, "alice").unwrap();
        grant(&conn, true, &["SELECT"], table("t"), &["bob"], false).unwrap();
        set_role(&conn, "bob").unwrap();
        conn.execute("SELECT x FROM t").unwrap();
    }

    #[test]
    fn grant_rejects_a_privilege_the_object_cannot_have_and_unknown_names() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();

        let execute = grant(&conn, true, &["EXECUTE"], table("t"), &["alice"], false).unwrap_err();
        assert!(
            execute
                .to_string()
                .contains("invalid privilege type EXECUTE for relation"),
            "{execute}"
        );
        let nobody = grant(&conn, true, &["SELECT"], table("t"), &["nobody"], false).unwrap_err();
        assert!(
            nobody
                .to_string()
                .contains("role \"nobody\" does not exist"),
            "{nobody}"
        );
        let missing = grant(
            &conn,
            true,
            &["SELECT"],
            table("missing"),
            &["alice"],
            false,
        )
        .unwrap_err();
        assert!(missing.to_string().contains("no such table"), "{missing}");
    }

    fn table(name: &str) -> ast::GrantObject {
        ast::GrantObject::Table(ast::QualifiedName::single(ast::Name::exact(name.into())))
    }

    fn grant(
        conn: &Arc<Connection>,
        is_grant: bool,
        privileges: &[&str],
        object: ast::GrantObject,
        grantees: &[&str],
        grant_option: bool,
    ) -> crate::Result<()> {
        let stmt = ast::Stmt::Grant(ast::GrantStmt {
            is_grant,
            privileges: privileges.iter().map(|p| p.to_string()).collect(),
            objects: vec![object],
            grantees: grantees
                .iter()
                .map(|name| match *name {
                    "public" => ast::Grantee::Public,
                    name => ast::Grantee::Role(name.to_string()),
                })
                .collect(),
            grant_option,
        });
        let sql = stmt.to_string();
        let mut statement = conn.prepare_translated_cmd(ast::Cmd::Stmt(stmt), &sql)?;
        statement.run_ignore_rows()
    }

    fn query_rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
        conn.prepare(sql).unwrap().run_collect_rows().unwrap()
    }

    fn open_connection() -> Arc<Connection> {
        connect_with_roles()
    }

    /// Makes `name` the current role, as SET ROLE does.
    fn set_role(conn: &Arc<Connection>, name: &str) -> crate::Result<()> {
        let role = conn
            .role_catalog()
            .get_by_name(name)
            .map(|role| role.id)
            .ok_or_else(|| {
                LimboError::InvalidArgument(format!("role \"{name}\" does not exist"))
            })?;
        conn.set_current_role(role);
        Ok(())
    }

    /// Makes the superuser the current role again, as RESET ROLE does.
    fn reset_role(conn: &Arc<Connection>) -> crate::Result<()> {
        conn.set_current_role(crate::security::roles::RoleId::SUPERUSER);
        Ok(())
    }

    fn denial(error: LimboError) -> PermissionDenied {
        match error {
            LimboError::PermissionDenied(denial) => *denial,
            other => panic!("expected a permission error, got: {other}"),
        }
    }

    fn create_role(conn: &Arc<Connection>, name: &str) -> crate::Result<()> {
        let create_role = ast::Cmd::Stmt(ast::Stmt::CreateRole {
            role_name: name.to_string(),
        });
        conn.prepare_translated_cmd(create_role, &format!("CREATE ROLE {name}"))?
            .run_ignore_rows()
    }

    #[derive(Debug)]
    struct EmptyModule;

    impl VirtualTableModule for EmptyModule {
        type Table = EmptyTable;

        fn schema(&self, _args: &[Value]) -> crate::Result<String> {
            Ok("CREATE TABLE x(value INTEGER)".to_string())
        }

        fn create(&self, _args: &[Value]) -> crate::Result<Self::Table> {
            Ok(EmptyTable)
        }
    }

    #[derive(Debug)]
    struct EmptyTable;

    impl VirtualTable for EmptyTable {
        type Cursor = EmptyCursor;

        fn open(&self, _connection: Arc<Connection>) -> crate::Result<Self::Cursor> {
            Ok(EmptyCursor)
        }

        fn best_index(
            &self,
            _constraints: &[ConstraintInfo],
            _order_by: &[OrderByInfo],
        ) -> Result<IndexInfo, ResultCode> {
            Ok(IndexInfo::default())
        }

        fn readonly(&self) -> bool {
            false
        }

        fn create_update(&mut self, _args: Vec<Value>) -> crate::Result<Box<dyn TableUpdate>> {
            Ok(Box::new(IgnoredUpdate))
        }
    }

    struct IgnoredUpdate;

    impl TableUpdate for IgnoredUpdate {
        fn step(&mut self) -> IOResultOr<Option<i64>> {
            Ok(IOResult::Done(None))
        }
    }

    struct EmptyCursor;

    impl VirtualTableCursor for EmptyCursor {
        fn filter(
            &mut self,
            _args: &[Value],
            _idx_str: Option<&str>,
            _idx_num: i32,
        ) -> IOResultOr<bool> {
            Ok(IOResult::Done(false))
        }

        fn next(&mut self) -> IOResultOr<bool> {
            Ok(IOResult::Done(false))
        }

        fn column(&mut self, _column: usize) -> IOResultOr<Value> {
            unreachable!("an empty table has no rows")
        }

        fn rowid(&self) -> i64 {
            unreachable!("an empty table has no rows")
        }
    }

    /// A table-valued function of a C extension.
    #[derive(Debug, Default, VTabModuleDerive)]
    struct ExtensionRows;

    impl VTabModule for ExtensionRows {
        type Table = ExtensionTable;
        const NAME: &'static str = "extension_rows";
        const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;

        fn create(_args: &[turso_ext::Value]) -> Result<(String, Self::Table), ResultCode> {
            Ok(("CREATE TABLE x(value INTEGER)".to_string(), ExtensionTable))
        }
    }

    struct ExtensionTable;

    impl VTable for ExtensionTable {
        type Cursor = ExtensionCursor;
        type Error = ResultCode;

        fn open(
            &self,
            _conn: Option<Arc<turso_ext::Connection>>,
        ) -> Result<Self::Cursor, Self::Error> {
            Ok(ExtensionCursor)
        }
    }

    struct ExtensionCursor;

    impl VTabCursor for ExtensionCursor {
        type Error = ResultCode;

        fn filter(
            &mut self,
            _args: &[turso_ext::Value],
            _idx_info: Option<(&str, i32)>,
        ) -> ResultCode {
            ResultCode::OK
        }

        fn rowid(&self) -> i64 {
            0
        }

        fn column(&self, _idx: u32) -> Result<turso_ext::Value, Self::Error> {
            Ok(turso_ext::Value::null())
        }

        fn eof(&self) -> bool {
            true
        }

        fn next(&mut self) -> ResultCode {
            ResultCode::EOF
        }
    }
}
