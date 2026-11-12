//! The relation catalog: one row per user relation, in the
//! `__turso_internal_classes` table of the main database.
//!
//! `sqlite_schema` stays the source of truth for storage: type, name, root
//! page and SQL text. The catalog holds what `sqlite_schema` does not: a
//! stable `oid` that survives creating and dropping other relations, the
//! owning role, and the privileges `GRANT` gave. The columns use
//! PostgreSQL's `pg_class` names and value conventions, because when
//! `sqlite_schema` itself moves behind the engine's dialect hooks, this
//! table becomes `pg_class`, by renaming and adding columns.
//!
//! Core knows nothing of this table. [`crate::catalog::PostgresDialect`]
//! maintains it through the engine's relation hooks: every statement that
//! adds, renames or removes a relation calls back here to write the matching
//! row in the same transaction as its `sqlite_schema` rows, so the two can
//! never disagree after a crash, and the engine loads the rows back as its
//! opaque dialect schema state whenever it loads the schema.
//!
//! The frontend creates the table when it opens a database, before it holds
//! any relation. A database that already has relations but no catalog was
//! written by an earlier tursopg; the frontend refuses such files instead of
//! migrating them: tursopg has made no compatibility promises yet, and a
//! refusal is better than inventing oids and owners for relations it did not
//! see created.
//!
//! Oids start at 16384, PostgreSQL's `FirstNormalObjectId`, and are never
//! reused inside one database, even after the relation is dropped: the
//! highest oid ever used is kept in `sqlite_sequence` under the catalog's
//! own name, like an AUTOINCREMENT table's watermark.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use turso_core::namespace::NamespaceId;
use turso_core::schema::{
    is_user_relation_name, BTreeTable, RelationKind, Schema, SQLITE_SEQUENCE_TABLE_NAME,
};
use turso_core::security::permission::{Action, Object, PermissionDenied};
use turso_core::security::roles::RoleId;
use turso_core::{
    emit_sqlite_sequence_row_read, emit_sqlite_sequence_row_write, to_u32, CmpInsFlags,
    CollationSeq, Connection, CursorType, IdxInsertFlags, InsertFlags, Insn, LimboError, NewOwner,
    Numeric, ProgramBuilder, RegisterOrLiteral, RelationChange, Value, SQLITE_CONSTRAINT_UNIQUE,
};
use turso_parser::ast;

use turso_core::MAIN_DB_ID;
use turso_pg_parser::sqlstate::{SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_GRANT_OPERATION};
use turso_pg_parser::translator::BOOTSTRAP_SUPERUSER_NAME;

use crate::acl::{Acl, Privileges, PUBLIC_OID};
use crate::catalog::{role_oid, role_oid_by_name};
use crate::error::sqlstate_error;
use crate::sql_functions::{
    FunctionCatalog, FunctionChange, FUNCTION_RELKIND, FUNCTION_SCHEMA_ROW_TYPE,
};

pub(crate) const CLASSES_TABLE_NAME: &str = "__turso_internal_classes";

pub(crate) const CLASSES_NAME_INDEX_NAME: &str = "__turso_internal_classes_name";

/// PostgreSQL's `FirstNormalObjectId`: the first oid given to a user
/// relation. Smaller oids belong to built-in catalog objects.
pub(crate) const FIRST_NORMAL_OBJECT_ID: i64 = 16384;

/// The columns the rest of this module reads and writes. `relownedby` is
/// the oid of the table whose serial column owns the relation, 0 when no
/// relation owns it: what PostgreSQL records in `pg_depend` as an automatic
/// dependency, kept here until there is a `pg_depend`. `relacl` is the
/// access control list in the text form of PostgreSQL's `aclitem[]`, NULL
/// until the first `GRANT`, when the owner has every privilege and nobody
/// else has any. `relrowsecurity` is 1 when the table has row-level
/// security, 0 otherwise.
const CREATE_CLASSES_TABLE_SQL: &str = "CREATE TABLE __turso_internal_classes (\
    oid INTEGER PRIMARY KEY, \
    relname TEXT NOT NULL, \
    relnamespace INTEGER NOT NULL, \
    relkind TEXT NOT NULL, \
    relowner INTEGER NOT NULL, \
    relownedby INTEGER NOT NULL, \
    relacl TEXT, \
    relrowsecurity INTEGER NOT NULL DEFAULT 0)";

const ACL_COLUMN_NAME: &str = "relacl";

const ROW_SECURITY_COLUMN_NAME: &str = "relrowsecurity";

/// Adds `relacl` to a catalog made before it had the column.
const ADD_ACL_COLUMN_SQL: &str = "ALTER TABLE __turso_internal_classes ADD COLUMN relacl TEXT";

/// Adds `relrowsecurity` to a catalog made before it had the column.
const ADD_ROW_SECURITY_COLUMN_SQL: &str =
    "ALTER TABLE __turso_internal_classes ADD COLUMN relrowsecurity INTEGER NOT NULL DEFAULT 0";

const CLASS_COLUMN_COUNT: usize = 8;

const CREATE_CLASSES_NAME_INDEX_SQL: &str = "CREATE UNIQUE INDEX __turso_internal_classes_name \
     ON __turso_internal_classes (relnamespace, relname)";

/// PostgreSQL's `pg_class.relkind` letter for `kind`.
pub(crate) fn relkind_letter(kind: RelationKind) -> &'static str {
    match kind {
        RelationKind::Table => "r",
        RelationKind::View => "v",
        RelationKind::MaterializedView => "m",
        RelationKind::Sequence => "S",
        RelationKind::Index => "i",
    }
}

/// The relation kind that the `pg_class.relkind` letter `letter` stands for.
pub(crate) fn relation_kind_of_letter(letter: &str) -> Option<RelationKind> {
    const KINDS: [RelationKind; 5] = [
        RelationKind::Table,
        RelationKind::View,
        RelationKind::MaterializedView,
        RelationKind::Sequence,
        RelationKind::Index,
    ];
    KINDS
        .into_iter()
        .find(|kind| relkind_letter(*kind) == letter)
}

/// One row of the catalog: the identity and ownership of a relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationClass {
    pub(crate) oid: i64,
    /// The relation's bare name, with the namespace in `relnamespace`.
    pub(crate) relname: String,
    pub(crate) relnamespace: NamespaceId,
    pub(crate) relkind: RelationKind,
    pub(crate) relowner: RoleId,
    /// The oid of the table whose serial column owns this sequence; 0 when
    /// no relation owns it.
    pub(crate) relownedby: i64,
    /// The privileges `GRANT` gave, None before the first `GRANT`.
    pub(crate) relacl: Option<Acl>,
    /// Whether the table has row-level security.
    pub(crate) relrowsecurity: bool,
}

/// The class rows of a database, loaded as the engine's opaque dialect
/// schema state.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClassCatalog {
    by_oid: BTreeMap<i64, RelationClass>,
    oids_by_name: HashMap<(NamespaceId, String), i64>,
    /// The functions, whose class rows have the kind
    /// [`crate::sql_functions::FUNCTION_RELKIND`].
    pub(crate) functions: FunctionCatalog,
}

impl ClassCatalog {
    /// Returns a catalog of the rows that [`schema_state_query`] read. A row
    /// of the classes table is `(oid, relname, relnamespace, relkind,
    /// relowner, relownedby, relacl, relrowsecurity, NULL)`. A row of
    /// `sqlite_schema` that stores a function is `(NULL, name, ..., sql)`.
    fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut catalog = Self::default();
        let mut function_rows = Vec::new();
        let mut function_sql = HashMap::new();
        for row in rows {
            let [class_columns @ .., sql] = row.as_slice() else {
                return Err(corrupt_row(row));
            };
            if let [Value::Null, Value::Text(name), ..] = class_columns {
                let Value::Text(sql) = sql else {
                    return Err(corrupt_row(row));
                };
                function_sql.insert(name.as_str().to_string(), sql.as_str().to_string());
                continue;
            }
            if matches!(class_columns.get(3), Some(Value::Text(kind)) if kind.as_str() == FUNCTION_RELKIND)
            {
                function_rows.push(class_columns);
                continue;
            }
            let class = class_from_row(class_columns)?;
            if catalog.by_oid.contains_key(&class.oid)
                || catalog
                    .get_by_name(class.relnamespace, &class.relname)
                    .is_some()
            {
                return Err(LimboError::Corrupt(format!(
                    "{CLASSES_TABLE_NAME} has a duplicate relation: {class:?}"
                )));
            }
            catalog.add(class);
        }
        catalog.functions = FunctionCatalog::new(&function_rows, &function_sql)?;
        Ok(catalog)
    }

    pub(crate) fn get_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RelationClass> {
        self.oids_by_name
            .get(&(namespace, name.to_string()))
            .and_then(|oid| self.by_oid.get(oid))
    }

    /// The relations the table with oid `table_oid` owns: the sequences of
    /// its serial columns.
    fn owned_by(&self, table_oid: i64) -> impl Iterator<Item = &RelationClass> {
        self.by_oid
            .values()
            .filter(move |class| class.relownedby == table_oid)
    }

    /// Applies what a statement wrote to the stored rows. `values` are what
    /// the emitted instruction read back: a created relation's oid and
    /// owner, a changed owner with the list it leaves, or a granted list.
    fn apply(&mut self, change: &RelationChange, values: &[Value]) -> turso_core::Result<()> {
        match change {
            RelationChange::Create {
                namespace,
                name,
                kind,
                ..
            } => {
                let [oid, owner] = values else {
                    return Err(LimboError::InternalError(
                        "a created relation carries its oid and owner".to_string(),
                    ));
                };
                self.add(RelationClass {
                    oid: read_back_integer(oid)?,
                    relname: name.clone(),
                    relnamespace: *namespace,
                    relkind: *kind,
                    relowner: RoleId::from_stored(read_back_integer(owner)?),
                    relownedby: 0,
                    relacl: None,
                    relrowsecurity: false,
                });
            }
            RelationChange::Drop { namespace, name } => {
                // What the dropped relation owned is nobody's now.
                if let Some(dropped) = self.remove(*namespace, name) {
                    for class in self.by_oid.values_mut() {
                        if class.relownedby == dropped.oid {
                            class.relownedby = 0;
                        }
                    }
                }
            }
            RelationChange::ChangeOwner {
                namespace,
                name,
                owner: NewOwner::Table(table),
            } => {
                let table_oid = table
                    .as_ref()
                    .and_then(|table| self.oids_by_name.get(&(*namespace, table.clone())))
                    .copied()
                    .unwrap_or(0);
                if let Some(oid) = self.oids_by_name.get(&(*namespace, name.clone())) {
                    self.by_oid
                        .get_mut(oid)
                        .expect("both maps hold the row")
                        .relownedby = table_oid;
                }
            }
            RelationChange::Rename {
                namespace,
                old_name,
                new_name,
            } => {
                if let Some(mut class) = self.remove(*namespace, old_name) {
                    class.relname.clone_from(new_name);
                    self.add(class);
                }
            }
            RelationChange::ChangeOwner {
                namespace,
                name,
                owner: NewOwner::Role(_),
            } => {
                let [owner, acl] = values else {
                    return Err(LimboError::InternalError(
                        "an owner change carries the new owner and the list it leaves".to_string(),
                    ));
                };
                if let Some(oid) = self.oids_by_name.get(&(*namespace, name.to_string())) {
                    let class = self.by_oid.get_mut(oid).expect("both maps hold the row");
                    class.relowner = RoleId::from_stored(read_back_integer(owner)?);
                    class.relacl = read_back_acl(acl)?;
                }
            }
            RelationChange::Grant {
                namespace, name, ..
            } => {
                let [acl] = values else {
                    return Err(LimboError::InternalError(
                        "a grant carries the list it leaves".to_string(),
                    ));
                };
                if let Some(oid) = self.oids_by_name.get(&(*namespace, name.to_string())) {
                    self.by_oid
                        .get_mut(oid)
                        .expect("both maps hold the row")
                        .relacl = read_back_acl(acl)?;
                }
            }
            RelationChange::SetRowSecurity {
                namespace,
                name,
                enabled,
            } => {
                if let Some(oid) = self.oids_by_name.get(&(*namespace, name.to_string())) {
                    self.by_oid
                        .get_mut(oid)
                        .expect("both maps hold the row")
                        .relrowsecurity = *enabled;
                }
            }
            RelationChange::DialectObject(change) => {
                let change = change.downcast_ref::<FunctionChange>().ok_or_else(|| {
                    LimboError::InternalError(
                        "the dialect object change is not a function change".to_string(),
                    )
                })?;
                self.functions.apply(change, values)?;
            }
        }
        Ok(())
    }

    fn add(&mut self, class: RelationClass) {
        assert!(
            !self.by_oid.contains_key(&class.oid),
            "oid {} is already in use",
            class.oid
        );
        let previous = self
            .oids_by_name
            .insert((class.relnamespace, class.relname.clone()), class.oid);
        assert!(
            previous.is_none(),
            "relation {} already has a class row",
            class.relname
        );
        self.by_oid.insert(class.oid, class);
    }

    fn remove(&mut self, namespace: NamespaceId, name: &str) -> Option<RelationClass> {
        let oid = self.oids_by_name.remove(&(namespace, name.to_string()))?;
        self.by_oid.remove(&oid)
    }
}

fn class_from_row(row: &[Value]) -> turso_core::Result<RelationClass> {
    let [oid, relname, relnamespace, relkind, relowner, relownedby, relacl, relrowsecurity] = row
    else {
        return Err(corrupt_row(row));
    };
    match (
        oid,
        relname,
        relnamespace,
        relkind,
        relowner,
        relownedby,
        relrowsecurity,
    ) {
        (
            Value::Numeric(Numeric::Integer(oid)),
            Value::Text(relname),
            Value::Numeric(Numeric::Integer(relnamespace)),
            Value::Text(relkind),
            Value::Numeric(Numeric::Integer(relowner)),
            Value::Numeric(Numeric::Integer(relownedby)),
            Value::Numeric(Numeric::Integer(relrowsecurity @ (0 | 1))),
        ) if *oid >= FIRST_NORMAL_OBJECT_ID
            && u32::try_from(*relnamespace).is_ok()
            && *relowner >= 0
            && *relownedby >= 0 =>
        {
            let Some(relkind) = relation_kind_of_letter(relkind.as_str()) else {
                return Err(corrupt_row(row));
            };
            let relacl = match relacl {
                Value::Null => None,
                Value::Text(text) => {
                    Some(Acl::parse(text.as_str()).ok_or_else(|| corrupt_row(row))?)
                }
                _ => return Err(corrupt_row(row)),
            };
            Ok(RelationClass {
                oid: *oid,
                relname: relname.as_str().to_string(),
                relnamespace: NamespaceId::new(*relnamespace as u32),
                relkind,
                relowner: RoleId::from_stored(*relowner),
                relownedby: *relownedby,
                relacl,
                relrowsecurity: *relrowsecurity == 1,
            })
        }
        _ => Err(corrupt_row(row)),
    }
}

pub(crate) fn corrupt_row(row: &[Value]) -> LimboError {
    LimboError::Corrupt(format!("{CLASSES_TABLE_NAME} has an invalid row: {row:?}"))
}

/// The catalog that the engine loaded as this database's dialect schema
/// state, or None when it is not loaded, as in a read-only open of an empty
/// database.
pub(crate) fn class_catalog(schema: &Schema) -> Option<&ClassCatalog> {
    schema.dialect_schema.as_ref()?.downcast_ref()
}

/// [`turso_core::Dialect::schema_state_query`] for the PostgreSQL frontend.
/// A catalog made before it had `relacl` or `relrowsecurity` and opened
/// read-only, which cannot add them, reads as having no grants and no
/// row-level security.
/// It also reads the rows of `sqlite_schema` that store the functions, which
/// the engine skips when it loads the schema.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    let table = schema.get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME)?;
    let relacl = if has_acl_column(&table) {
        "relacl"
    } else {
        "NULL AS relacl"
    };
    let relrowsecurity = if has_row_security_column(&table) {
        "relrowsecurity"
    } else {
        "0 AS relrowsecurity"
    };
    Some(format!(
        "SELECT oid, relname, relnamespace, relkind, relowner, relownedby, {relacl}, \
         {relrowsecurity}, NULL AS sql FROM {CLASSES_TABLE_NAME} \
         UNION ALL \
         SELECT NULL, name, NULL, NULL, NULL, NULL, NULL, NULL, sql FROM sqlite_schema \
         WHERE type = '{FUNCTION_SCHEMA_ROW_TYPE}'"
    ))
}

fn has_acl_column(table: &BTreeTable) -> bool {
    table.get_column(ACL_COLUMN_NAME).is_some()
}

fn has_row_security_column(table: &BTreeTable) -> bool {
    table.get_column(ROW_SECURITY_COLUMN_NAME).is_some()
}

/// [`turso_core::Dialect::build_schema_state`] for the PostgreSQL frontend:
/// the loaded catalog, after checking that every user relation has a row.
pub(crate) fn build_schema_state(
    schema: &Schema,
    rows: &[Vec<Value>],
) -> turso_core::Result<Arc<dyn Any + Send + Sync>> {
    let catalog = ClassCatalog::new(rows)?;
    for (namespace, name, _) in schema.user_relations() {
        if catalog.get_by_name(namespace, &name).is_none() {
            return Err(LimboError::Corrupt(format!(
                "relation \"{name}\" of namespace {} has no row in {CLASSES_TABLE_NAME}",
                namespace.as_u32()
            )));
        }
    }
    Ok(Arc::new(catalog))
}

/// [`turso_core::Dialect::apply_relation_change`] for the PostgreSQL
/// frontend: keeps the in-memory catalog in step with the stored rows the
/// running statement wrote.
pub(crate) fn apply_relation_change(
    schema: &mut Schema,
    change: &RelationChange,
    values: &[Value],
) -> turso_core::Result<()> {
    let Some(state) = schema.dialect_schema.take() else {
        return Ok(());
    };
    let mut catalog: Arc<ClassCatalog> = state.downcast().map_err(|_| {
        LimboError::InternalError("the dialect schema state is not the relation catalog".into())
    })?;
    Arc::make_mut(&mut catalog).apply(change, values)?;
    schema.dialect_schema = Some(catalog);
    Ok(())
}

/// [`turso_core::Dialect::relation_owner`] for the PostgreSQL frontend.
pub(crate) fn relation_owner(
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
) -> Option<RoleId> {
    Some(
        class_catalog(schema)?
            .get_by_name(namespace, name)?
            .relowner,
    )
}

/// [`turso_core::Dialect::role_has_privilege`] for the PostgreSQL frontend:
/// the owner has every privilege, and another role has the ones the list
/// gives it, by oid or through PUBLIC. A table without a class row is one of
/// the engine's own, such as `__turso_internal_roles` or `sqlite_sequence`,
/// and only a superuser uses it, because nothing says who else may.
pub(crate) fn role_has_privilege(
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
    role: RoleId,
    privilege: ast::Privilege,
) -> bool {
    let Some(class) =
        class_catalog(schema).and_then(|catalog| catalog.get_by_name(namespace, name))
    else {
        return false;
    };
    if class.relowner == role {
        return true;
    }
    let wanted = Privileges::granted_on(class.relkind, &[privilege]);
    !wanted.is_empty()
        && class
            .relacl
            .as_ref()
            .is_some_and(|acl| acl.grants(role_oid(role), wanted))
}

/// The access control list of relation `name` of `namespace`, or None when
/// no `GRANT` has touched it.
pub(crate) fn relation_acl(schema: &Schema, namespace: NamespaceId, name: &str) -> Option<Acl> {
    class_catalog(schema)?
        .get_by_name(namespace, name)?
        .relacl
        .clone()
}

/// [`turso_core::Dialect::table_rls_enabled`] for the PostgreSQL frontend:
/// whether table `name` of `namespace` has row-level security, as
/// `pg_class.relrowsecurity` shows it. A relation without a class row is
/// the engine's, such as a catalog table, and has none; nor does any table
/// while the catalog is not loaded, when no relation has an owner either.
pub(crate) fn table_rls_enabled(schema: &Schema, namespace: NamespaceId, name: &str) -> bool {
    class_catalog(schema)
        .and_then(|catalog| catalog.get_by_name(namespace, name))
        .is_some_and(|class| class.relrowsecurity)
}

/// An integer the emitted instruction read back from a register.
pub(crate) fn read_back_integer(value: &Value) -> turso_core::Result<i64> {
    match value {
        Value::Numeric(Numeric::Integer(value)) => Ok(*value),
        other => Err(LimboError::InternalError(format!(
            "the relation change read back {other:?} where an integer was written"
        ))),
    }
}

/// An access control list, or NULL, that the emitted instruction read back
/// from a register.
fn read_back_acl(value: &Value) -> turso_core::Result<Option<Acl>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Acl::parse(text.as_str()).map(Some).ok_or_else(|| {
            LimboError::InternalError(format!(
                "the relation change read back an invalid access control list {text:?}"
            ))
        }),
        other => Err(LimboError::InternalError(format!(
            "the relation change read back {other:?} where an access control list was written"
        ))),
    }
}

/// [`turso_core::Dialect::translate_relation_changes`] for the PostgreSQL
/// frontend: a class row per created relation, owned by the current role,
/// which is what PostgreSQL's `GetUserId()` gives after `SET ROLE`; no row
/// for a dropped one; a rename keeps the oid and owner; `ALTER ... OWNER TO`
/// sets the owner, with PostgreSQL's errors for a missing role (42704) and
/// a catalog relation (42501). A sequence a serial column owns changes
/// owner with its table only, as in PostgreSQL: on its own it fails with
/// `cannot change owner of sequence` (0A000). `GRANT` adds to the access
/// control list when the owner or a superuser runs it; anyone else grants
/// nothing, as in PostgreSQL, which only warns. `ALTER TABLE ... ENABLE ROW
/// LEVEL SECURITY` sets `relrowsecurity` of the table's row, which a catalog
/// table does not have (42501).
pub(crate) fn translate_relation_changes(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    changes: &[RelationChange],
) -> turso_core::Result<()> {
    let allocates_oids = changes
        .iter()
        .any(|change| matches!(change, RelationChange::Create { .. }));
    let changes_owner_or_privileges = changes.iter().any(|change| {
        matches!(
            change,
            RelationChange::ChangeOwner {
                owner: NewOwner::Role(_),
                ..
            } | RelationChange::Grant { .. }
                | RelationChange::SetRowSecurity { .. }
        )
    });
    let Some(cursors) = open_class_table_for_writing(program, schema, allocates_oids)? else {
        if changes_owner_or_privileges {
            return Err(LimboError::ParseError(
                "the database has no relation catalog".to_string(),
            ));
        }
        return Ok(());
    };
    for change in changes {
        match change {
            RelationChange::Create {
                namespace,
                name,
                kind,
                table,
            } => {
                // Relation names are unique per schema across kinds.
                if is_user_relation_name(*kind, name)
                    && class_catalog(schema)
                        .is_some_and(|catalog| catalog.get_by_name(*namespace, name).is_some())
                {
                    return Err(LimboError::ParseError(format!(
                        "relation \"{name}\" already exists"
                    )));
                }
                // An index is owned by its table's owner, anything else by
                // the current role, which is what PostgreSQL's GetUserId()
                // gives after SET ROLE.
                let owner = table
                    .as_deref()
                    .and_then(|table| relation_owner(schema, *namespace, table))
                    .unwrap_or_else(|| connection.current_role());
                emit_class_row_insert(
                    program,
                    &cursors,
                    *namespace,
                    name,
                    *kind,
                    table.as_deref(),
                    owner,
                )
            }
            RelationChange::Drop { namespace, name } => {
                // An engine relation has no row and nothing to do.
                let Some(owned) = class_catalog(schema).and_then(|catalog| {
                    let class = catalog.get_by_name(*namespace, name)?;
                    Some(
                        catalog
                            .owned_by(class.oid)
                            .map(|owned| (owned.relnamespace, owned.relname.clone()))
                            .collect::<Vec<_>>(),
                    )
                }) else {
                    continue;
                };
                // What the dropped relation owned, such as the sequences of
                // a table's serial columns, stays and is nobody's now.
                for (owned_namespace, owned_name) in owned {
                    emit_class_row_set_owned_by(program, &cursors, owned_namespace, &owned_name, 0);
                }
                emit_class_row_delete(program, &cursors, *namespace, name)
            }
            RelationChange::ChangeOwner {
                namespace,
                name,
                owner: NewOwner::Table(table),
            } => {
                let table_oid = match table {
                    Some(table) => class_catalog(schema)
                        .and_then(|catalog| catalog.get_by_name(*namespace, table))
                        .map(|class| class.oid)
                        .ok_or_else(|| {
                            LimboError::ParseError(format!("relation \"{table}\" does not exist"))
                        })?,
                    None => 0,
                };
                emit_class_row_set_owned_by(program, &cursors, *namespace, name, table_oid);
                program.emit_insn(Insn::RelationChange {
                    db: MAIN_DB_ID,
                    change: Box::new(change.clone()),
                    value_regs: vec![],
                });
            }
            RelationChange::Rename {
                namespace,
                old_name,
                new_name,
            } => {
                let has_row = class_catalog(schema)
                    .is_some_and(|catalog| catalog.get_by_name(*namespace, old_name).is_some());
                if has_row {
                    emit_class_row_rename(program, &cursors, *namespace, old_name, new_name);
                }
            }
            RelationChange::ChangeOwner {
                namespace,
                name,
                owner: NewOwner::Role(new_owner),
            } => {
                let owner = resolve_new_owner(schema, connection, *namespace, name, new_owner)?;
                let Some(class) =
                    class_catalog(schema).and_then(|catalog| catalog.get_by_name(*namespace, name))
                else {
                    continue;
                };
                // As in PostgreSQL: nothing changes when the owner stays, an
                // index's owner is its table's, and a serial column's
                // sequence changes owner with its table only.
                if class.relowner == owner || class.relkind == RelationKind::Index {
                    continue;
                }
                if class.relownedby != 0 {
                    return Err(LimboError::ParseError(format!(
                        "cannot change owner of sequence \"{name}\""
                    )));
                }
                // A table's indexes and what it owns, such as the sequences
                // of its serial columns, go with it.
                let mut with_it = vec![(*namespace, name.to_string())];
                with_it.extend(
                    class_catalog(schema)
                        .into_iter()
                        .flat_map(|catalog| catalog.owned_by(class.oid))
                        .map(|owned| (owned.relnamespace, owned.relname.clone())),
                );
                with_it.extend(
                    schema
                        .get_indices(*namespace, name)
                        .filter(|index| {
                            is_user_relation_name(RelationKind::Index, &index.name)
                                && !index.is_backing_btree_index()
                        })
                        .map(|index| (*namespace, index.name.clone())),
                );
                for (other_namespace, other_name) in with_it {
                    // The new owner takes the old one's place in the list.
                    let acl = class_catalog(schema)
                        .and_then(|catalog| catalog.get_by_name(other_namespace, &other_name))
                        .and_then(|other| other.relacl.clone())
                        .map(|mut acl| {
                            acl.change_owner(role_oid(class.relowner), role_oid(owner));
                            acl
                        });
                    emit_class_row_set_owner(
                        program,
                        &cursors,
                        other_namespace,
                        &other_name,
                        owner,
                        new_owner,
                        acl.as_ref(),
                    );
                }
            }
            RelationChange::Grant {
                namespace,
                name,
                privileges,
                grantees,
            } => {
                let grantees = grantees
                    .iter()
                    .map(|grantee| resolve_grantee(schema, connection, grantee))
                    .collect::<turso_core::Result<Vec<_>>>()?;
                // A relation without a class row has no list to store the
                // privileges in. Every role reads the pg_catalog views
                // already, so a GRANT on one changes nothing. Only a
                // superuser uses the engine's own tables, so a GRANT on one
                // would claim access it does not give, and is refused.
                let Some(class) =
                    class_catalog(schema).and_then(|catalog| catalog.get_by_name(*namespace, name))
                else {
                    if schema.get_btree_table(*namespace, name).is_some() {
                        return Err(sqlstate_error(
                            SQLSTATE_FEATURE_NOT_SUPPORTED,
                            format!("GRANT on internal table {name} is not supported"),
                        ));
                    }
                    continue;
                };
                // `ON TABLE` may name a sequence, so USAGE is refused here,
                // for a relation that is not one, as in PostgreSQL.
                if class.relkind != RelationKind::Sequence
                    && privileges.contains(&ast::Privilege::Usage)
                {
                    return Err(sqlstate_error(
                        SQLSTATE_INVALID_GRANT_OPERATION,
                        "invalid privilege type USAGE for table",
                    ));
                }
                let owner = role_oid(class.relowner);
                let mut acl = class
                    .relacl
                    .clone()
                    .unwrap_or_else(|| Acl::default_for(class.relkind, owner));
                // Only the owner may grant, and a superuser grants as the
                // owner. As in PostgreSQL, anyone else is refused when the
                // list gives it nothing on the relation, and grants nothing
                // otherwise, for want of a grant option.
                let may_grant = connection.current_role_is_superuser()
                    || class.relowner == connection.current_role();
                if !may_grant {
                    if acl.gives_anything_to(role_oid(connection.current_role())) {
                        continue;
                    }
                    return Err(LimboError::PermissionDenied(Box::new(
                        PermissionDenied::privilege(Action::Read, relation_object(class)),
                    )));
                }
                // As in PostgreSQL, a GRANT that gives nothing on the
                // relation, such as INSERT on a sequence, leaves its list
                // as it is, even when it has none yet.
                let granted = Privileges::granted_on(class.relkind, privileges);
                if granted.is_empty() {
                    continue;
                }
                for grantee in &grantees {
                    acl.grant(*grantee, granted, owner);
                }
                if class.relacl.as_ref() == Some(&acl) {
                    continue;
                }
                emit_class_row_set_acl(program, &cursors, *namespace, name, &acl, change);
            }
            RelationChange::SetRowSecurity {
                namespace,
                name,
                enabled,
            } => {
                let Some(class) =
                    class_catalog(schema).and_then(|catalog| catalog.get_by_name(*namespace, name))
                else {
                    return Err(LimboError::ParseError(format!(
                        "permission denied: \"{name}\" is a system catalog"
                    )));
                };
                if class.relrowsecurity == *enabled {
                    continue;
                }
                emit_class_row_set_row_security(program, &cursors, *namespace, name, change);
            }
            RelationChange::DialectObject(_) => {
                return Err(LimboError::InternalError(
                    "only the bytecode of the dialect carries a dialect object change".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// The relation of `class` as a permission error names it.
fn relation_object(class: &RelationClass) -> Object {
    match class.relkind {
        RelationKind::View | RelationKind::MaterializedView => Object::View(class.relname.clone()),
        RelationKind::Sequence => Object::Sequence(class.relname.clone()),
        RelationKind::Table | RelationKind::Index => Object::Table(class.relname.clone()),
    }
}

/// The role oid a `GRANT` stores for `grantee`, [`PUBLIC_OID`] for
/// everyone. A named role must exist.
fn resolve_grantee(
    schema: &Schema,
    connection: &Arc<Connection>,
    grantee: &ast::Grantee,
) -> turso_core::Result<i64> {
    Ok(match grantee {
        ast::Grantee::Public => PUBLIC_OID,
        ast::Grantee::Role(ast::RoleSpec::CurrentRole) => role_oid(connection.current_role()),
        ast::Grantee::Role(ast::RoleSpec::SessionUser) => role_oid(connection.session_role()),
        ast::Grantee::Role(ast::RoleSpec::Name(name)) => role_oid_by_name(&schema.roles, name)
            .ok_or_else(|| LimboError::ParseError(format!("role \"{name}\" does not exist")))?,
    })
}

/// The role `ALTER ... OWNER TO new_owner` gives relation `name` of
/// `namespace` to. A relation without a class row is the engine's.
fn resolve_new_owner(
    schema: &Schema,
    connection: &Arc<Connection>,
    namespace: NamespaceId,
    name: &str,
    new_owner: &ast::RoleSpec,
) -> turso_core::Result<RoleId> {
    let has_row =
        class_catalog(schema).is_some_and(|catalog| catalog.get_by_name(namespace, name).is_some());
    if !has_row {
        return Err(LimboError::ParseError(format!(
            "permission denied: \"{name}\" is a system catalog"
        )));
    }
    Ok(match new_owner {
        ast::RoleSpec::CurrentRole => connection.current_role(),
        ast::RoleSpec::SessionUser => connection.session_role(),
        ast::RoleSpec::Name(role_name) => {
            if role_name == BOOTSTRAP_SUPERUSER_NAME {
                RoleId::SUPERUSER
            } else {
                match schema.roles.get_by_name(role_name) {
                    Some(role) => role.id,
                    None => {
                        return Err(LimboError::ParseError(format!(
                            "role \"{role_name}\" does not exist"
                        )))
                    }
                }
            }
        }
    })
}

/// The cursors of one statement's class row writes: the classes table, its
/// name index, and `sqlite_sequence`, which keeps the highest oid ever used
/// under the classes table's own name.
pub(crate) struct ClassTableCursors {
    table_cursor_id: usize,
    index_cursor_id: usize,
    index_name: String,
    /// False for a catalog made before it had `relacl` and opened read-only,
    /// which has no column to read; the write that follows fails anyway.
    has_acl_column: bool,
    /// The same for `relrowsecurity`.
    has_row_security_column: bool,
    /// Only opened when the statement inserts a row, which allocates an oid.
    seq_cursor_id: Option<usize>,
    seq_table: Option<Arc<BTreeTable>>,
}

/// Opens the classes table of the main database for writing, or returns None
/// when the database has no catalog, which only happens outside the
/// frontend's own open path, such as a core-level open for tests.
/// `allocates_oids` also opens `sqlite_sequence`, which inserting a class
/// row updates.
pub(crate) fn open_class_table_for_writing(
    program: &mut ProgramBuilder,
    schema: &Schema,
    allocates_oids: bool,
) -> turso_core::Result<Option<ClassTableCursors>> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME) else {
        return Ok(None);
    };
    let name_index = schema
        .get_indices(NamespaceId::ROOT, CLASSES_TABLE_NAME)
        .find(|index| index.name == CLASSES_NAME_INDEX_NAME)
        .cloned()
        .ok_or_else(|| LimboError::Corrupt(format!("{CLASSES_TABLE_NAME} has no name index")))?;

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
    let (seq_cursor_id, seq_table) = if allocates_oids {
        let Some(seq_table) = schema.get_btree_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME)
        else {
            return Err(LimboError::Corrupt(format!(
                "a database with {CLASSES_TABLE_NAME} has no {SQLITE_SEQUENCE_TABLE_NAME}"
            )));
        };
        let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));
        program.emit_insn(Insn::OpenRead {
            cursor_id: seq_cursor_id,
            root_page: seq_table.root_page,
            db: MAIN_DB_ID,
        });
        (Some(seq_cursor_id), Some(seq_table))
    } else {
        (None, None)
    };
    Ok(Some(ClassTableCursors {
        table_cursor_id,
        index_cursor_id,
        index_name: name_index.name.clone(),
        has_acl_column: has_acl_column(&table),
        has_row_security_column: has_row_security_column(&table),
        seq_cursor_id,
        seq_table,
    }))
}

/// Inserts the class row of relation `name` of `namespace` under the next
/// oid, owned by `owner`; `table` is the table an index is on. Engine
/// relations get no row.
fn emit_class_row_insert(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    kind: RelationKind,
    table: Option<&str>,
    owner: RoleId,
) {
    if !is_user_relation_name(kind, name) {
        return;
    }
    let row = emit_class_row_insert_of_kind(
        program,
        cursors,
        namespace,
        name,
        relkind_letter(kind),
        owner,
    );
    program.emit_insn(Insn::RelationChange {
        db: MAIN_DB_ID,
        change: Box::new(RelationChange::Create {
            namespace,
            name: name.to_string(),
            kind,
            table: table.map(str::to_string),
        }),
        value_regs: vec![row.oid_reg, row.owner_reg],
    });
}

/// The registers that hold the oid and the owner of an inserted class row.
pub(crate) struct InsertedClassRow {
    pub(crate) oid_reg: usize,
    pub(crate) owner_reg: usize,
}

/// Inserts the class row `(relname, namespace, relkind, owner)` under the
/// next oid. The caller tells the in-memory catalog.
pub(crate) fn emit_class_row_insert_of_kind(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    relkind: &str,
    owner: RoleId,
) -> InsertedClassRow {
    let seq_cursor_id = cursors
        .seq_cursor_id
        .expect("the caller opened the cursors with allocates_oids");

    // oid = max(largest oid in the table + 1, highest oid ever used + 1,
    // FIRST_NORMAL_OBJECT_ID), so an oid is never reused even after the
    // relation with the largest oid is dropped.
    let watermark_reg = emit_read_oid_watermark(program, seq_cursor_id);
    let oid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cursors.table_cursor_id,
        rowid_reg: oid_reg,
        prev_largest_reg: 0,
    });
    let floor_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: FIRST_NORMAL_OBJECT_ID - 1,
        dest: floor_reg,
    });
    program.emit_insn(Insn::MemMax {
        dest_reg: watermark_reg,
        src_reg: floor_reg,
    });
    program.emit_insn(Insn::AddImm {
        register: watermark_reg,
        value: 1,
    });
    program.emit_insn(Insn::MemMax {
        dest_reg: oid_reg,
        src_reg: watermark_reg,
    });

    emit_class_name_index_insert(program, cursors, namespace, name, oid_reg);

    // The oid is the rowid, so the record keeps NULL for it, as SQLite does
    // for an INTEGER PRIMARY KEY column.
    let first_column_reg = program.alloc_registers(CLASS_COLUMN_COUNT);
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: first_column_reg + 1,
    });
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: first_column_reg + 2,
    });
    program.emit_insn(Insn::String8 {
        value: relkind.to_string(),
        dest: first_column_reg + 3,
    });
    program.emit_insn(Insn::Integer {
        value: owner.get(),
        dest: first_column_reg + 4,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_column_reg + 5,
    });
    // No GRANT has touched a new relation, and it has no row-level security.
    program.emit_insn(Insn::Null {
        dest: first_column_reg + 6,
        dest_end: None,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_column_reg + 7,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(CLASS_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursors.table_cursor_id,
        key_reg: oid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_all_change_counts(),
        table_name: CLASSES_TABLE_NAME.to_string(),
    });

    emit_sqlite_sequence_row_write(
        program,
        cursors
            .seq_table
            .clone()
            .expect("the caller opened the cursors with allocates_oids"),
        MAIN_DB_ID,
        CLASSES_TABLE_NAME,
        oid_reg,
    );

    InsertedClassRow {
        oid_reg,
        owner_reg: first_column_reg + 4,
    }
}

/// Makes the table with oid `table_oid` the owner of relation `name` of
/// `namespace`, its `relownedby`; 0 makes the relation nobody's. The
/// caller tells the in-memory catalog.
fn emit_class_row_set_owned_by(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    table_oid: i64,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::LiteralInt(table_oid),
            ClassRecordColumn::Register(row.relacl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
}

/// Deletes the class row of relation `name` of `namespace`.
fn emit_class_row_delete(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
) {
    emit_class_row_removal(program, cursors, namespace, name);
    program.emit_insn(Insn::RelationChange {
        db: MAIN_DB_ID,
        change: Box::new(RelationChange::Drop {
            namespace,
            name: name.to_string(),
        }),
        value_regs: vec![],
    });
}

/// Deletes the class row named `name` of `namespace`. The caller tells the
/// in-memory catalog.
pub(crate) fn emit_class_row_removal(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        emit_class_name_index_delete(program, cursors, namespace, name, row.rowid_reg);
        // Marked as part of an update so that changes() does not count it.
        program.emit_insn(Insn::Delete {
            cursor_id: cursors.table_cursor_id,
            table_name: CLASSES_TABLE_NAME.to_string(),
            is_part_of_update: true,
        });
    });
}

/// Renames the class row of relation `old_name` of `namespace` to
/// `new_name`, keeping its oid and owner.
fn emit_class_row_rename(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    old_name: &str,
    new_name: &str,
) {
    emit_find_class_row(program, cursors, namespace, old_name, |program, row| {
        emit_class_name_index_delete(program, cursors, namespace, old_name, row.rowid_reg);
        emit_class_name_index_insert(program, cursors, namespace, new_name, row.rowid_reg);
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(new_name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(row.relacl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    program.emit_insn(Insn::RelationChange {
        db: MAIN_DB_ID,
        change: Box::new(RelationChange::Rename {
            namespace,
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
        }),
        value_regs: vec![],
    });
}

/// Sets the owner of the class row of relation `name` of `namespace` to
/// `owner`, which `new_owner` named, and its access control list to `acl`.
fn emit_class_row_set_owner(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    owner: RoleId,
    new_owner: &ast::RoleSpec,
    acl: Option<&Acl>,
) {
    let owner_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: owner.get(),
        dest: owner_reg,
    });
    let acl_reg = emit_acl(program, acl);
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(owner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(acl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    program.emit_insn(Insn::RelationChange {
        db: MAIN_DB_ID,
        change: Box::new(RelationChange::ChangeOwner {
            namespace,
            name: name.to_string(),
            owner: NewOwner::Role(new_owner.clone()),
        }),
        value_regs: vec![owner_reg, acl_reg],
    });
}

/// Sets the access control list of the class row of relation `name` of
/// `namespace` to `acl`, which `change` granted.
fn emit_class_row_set_acl(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    acl: &Acl,
    change: &RelationChange,
) {
    let acl_reg = emit_acl(program, Some(acl));
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(acl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    program.emit_insn(Insn::RelationChange {
        db: MAIN_DB_ID,
        change: Box::new(change.clone()),
        value_regs: vec![acl_reg],
    });
}

/// Sets `relrowsecurity` of the class row of table `name` of `namespace` as
/// `change` says.
fn emit_class_row_set_row_security(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    change: &RelationChange,
) {
    let RelationChange::SetRowSecurity { enabled, .. } = change else {
        unreachable!("the caller passes a row-level security change");
    };
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(row.relacl_reg),
            ClassRecordColumn::LiteralInt(i64::from(*enabled)),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    program.emit_insn(Insn::RelationChange {
        db: MAIN_DB_ID,
        change: Box::new(change.clone()),
        value_regs: vec![],
    });
}

/// Puts `acl` in its stored form, or NULL for none, in a new register.
fn emit_acl(program: &mut ProgramBuilder, acl: Option<&Acl>) -> usize {
    match acl {
        Some(acl) => program.emit_string8_new_reg(acl.stored_text()),
        None => {
            let acl_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: acl_reg,
                dest_end: None,
            });
            acl_reg
        }
    }
}

/// The registers that [`emit_find_class_row`] fills for the found row.
struct FoundClassRow {
    rowid_reg: usize,
    relkind_reg: usize,
    relowner_reg: usize,
    relownedby_reg: usize,
    relacl_reg: usize,
    relrowsecurity_reg: usize,
}

/// Scans the classes table for the row of relation `name` of `namespace` and
/// runs `found` with the cursor on it. Emits nothing after the scan when the
/// row does not exist, so a relation without a row is tolerated.
fn emit_find_class_row(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    found: impl FnOnce(&mut ProgramBuilder, &FoundClassRow),
) {
    let name_reg = program.emit_string8_new_reg(name.to_string());
    let namespace_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: namespace_reg,
    });

    let loop_end = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursors.table_cursor_id,
        pc_if_empty: loop_end,
    });
    let loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);

    let next = program.allocate_label();
    let column_reg = program.alloc_register();
    program.emit_column_or_rowid(cursors.table_cursor_id, 2, column_reg);
    program.emit_insn(Insn::Ne {
        lhs: column_reg,
        rhs: namespace_reg,
        target_pc: next,
        flags: CmpInsFlags::default().jump_if_null(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_column_or_rowid(cursors.table_cursor_id, 1, column_reg);
    program.emit_insn(Insn::Ne {
        lhs: column_reg,
        rhs: name_reg,
        target_pc: next,
        flags: CmpInsFlags::default().jump_if_null(),
        collation: CollationSeq::NOCASE,
    });

    let row = FoundClassRow {
        rowid_reg: program.alloc_register(),
        relkind_reg: program.alloc_register(),
        relowner_reg: program.alloc_register(),
        relownedby_reg: program.alloc_register(),
        relacl_reg: program.alloc_register(),
        relrowsecurity_reg: program.alloc_register(),
    };
    program.emit_insn(Insn::RowId {
        cursor_id: cursors.table_cursor_id,
        dest: row.rowid_reg,
    });
    program.emit_column_or_rowid(cursors.table_cursor_id, 3, row.relkind_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 4, row.relowner_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 5, row.relownedby_reg);
    if cursors.has_acl_column {
        program.emit_column_or_rowid(cursors.table_cursor_id, 6, row.relacl_reg);
    } else {
        program.emit_insn(Insn::Null {
            dest: row.relacl_reg,
            dest_end: None,
        });
    }
    if cursors.has_row_security_column {
        program.emit_column_or_rowid(cursors.table_cursor_id, 7, row.relrowsecurity_reg);
    } else {
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: row.relrowsecurity_reg,
        });
    }
    found(program, &row);
    program.emit_insn(Insn::Goto { target_pc: done });

    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursors.table_cursor_id,
        pc_if_next: loop_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(loop_end);
    program.preassign_label_to_next_insn(done);
}

/// What a column of a rewritten class record is built from.
enum ClassRecordColumn {
    Literal(String),
    LiteralInt(i64),
    Register(usize),
}

/// Builds a class record `(NULL, relname, relnamespace, relkind, relowner,
/// relownedby, relacl, relrowsecurity)` and returns its register.
#[allow(clippy::too_many_arguments)]
fn emit_class_record(
    program: &mut ProgramBuilder,
    relname: ClassRecordColumn,
    relnamespace: ClassRecordColumn,
    relkind: ClassRecordColumn,
    relowner: ClassRecordColumn,
    relownedby: ClassRecordColumn,
    relacl: ClassRecordColumn,
    relrowsecurity: ClassRecordColumn,
) -> usize {
    let first_column_reg = program.alloc_registers(CLASS_COLUMN_COUNT);
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    for (offset, column) in [
        relname,
        relnamespace,
        relkind,
        relowner,
        relownedby,
        relacl,
        relrowsecurity,
    ]
    .into_iter()
    .enumerate()
    {
        let dest = first_column_reg + 1 + offset;
        match column {
            ClassRecordColumn::Literal(value) => {
                program.emit_insn(Insn::String8 { value, dest });
            }
            ClassRecordColumn::LiteralInt(value) => {
                program.emit_insn(Insn::Integer { value, dest });
            }
            ClassRecordColumn::Register(src_reg) => {
                program.emit_insn(Insn::Copy {
                    src_reg,
                    dst_reg: dest,
                    extra_amount: 0,
                });
            }
        }
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(CLASS_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    record_reg
}

/// Replaces the class row at `rowid_reg` with `record_reg`. The cursor is on
/// the row. The delete first ends the old version under MVCC, where an
/// update is a delete and an insert.
fn emit_class_row_overwrite(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    rowid_reg: usize,
    record_reg: usize,
) {
    if program.is_mvcc_enabled() {
        program.emit_insn(Insn::Delete {
            cursor_id: cursors.table_cursor_id,
            table_name: CLASSES_TABLE_NAME.to_string(),
            is_part_of_update: true,
        });
    }
    program.emit_insn(Insn::Insert {
        cursor: cursors.table_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_all_change_counts()
            .require_seek(),
        table_name: CLASSES_TABLE_NAME.to_string(),
    });
}

/// Adds `(namespace, name, rowid)` to the name index, failing if another
/// relation of the namespace has the name. The schema has already rejected a
/// name it knows, so this only fails if the stored table has a relation the
/// schema does not.
fn emit_class_name_index_insert(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    rowid_reg: usize,
) {
    let key_reg = program.alloc_registers(3);
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: key_reg,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: key_reg + 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: key_reg + 2,
        extra_amount: 0,
    });
    let no_conflict = program.allocate_label();
    program.emit_insn(Insn::NoConflict {
        cursor_id: cursors.index_cursor_id,
        target_pc: no_conflict,
        record_reg: key_reg,
        num_regs: 2,
    });
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: SQLITE_CONSTRAINT_UNIQUE,
        description: format!("relation \"{name}\" already has a class row"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(no_conflict);
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(key_reg),
        count: to_u32(3),
        dest_reg: to_u32(record_reg),
        index_name: Some(cursors.index_name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: cursors.index_cursor_id,
        record_reg,
        unpacked_start: Some(key_reg),
        unpacked_count: Some(3),
        flags: IdxInsertFlags::new(),
    });
}

/// Removes `(namespace, name, rowid)` from the name index.
fn emit_class_name_index_delete(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    rowid_reg: usize,
) {
    let key_reg = program.alloc_registers(3);
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: key_reg,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: key_reg + 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: key_reg + 2,
        extra_amount: 0,
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: key_reg,
        num_regs: 3,
        cursor_id: cursors.index_cursor_id,
        raise_error_if_no_matching_entry: true,
    });
}

/// Reads the highest oid ever used from the `sqlite_sequence` row named
/// after the classes table into the returned register: an integer, zero
/// when the row does not exist.
fn emit_read_oid_watermark(program: &mut ProgramBuilder, seq_cursor_id: usize) -> usize {
    let name_reg = program.emit_string8_new_reg(CLASSES_TABLE_NAME.to_string());
    let watermark_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();
    emit_sqlite_sequence_row_read(program, seq_cursor_id, name_reg, watermark_reg, rowid_reg);
    watermark_reg
}

/// Creates the classes table, its name index, the `sqlite_sequence` table
/// and the oid watermark row in one write transaction, unless the table
/// exists. A table made before it had `relacl` or `relrowsecurity` gets the
/// missing columns. A database
/// that already has relations without the table is refused, and a read-only
/// open can neither create nor need it: the database is empty. After this
/// returns, every relation the database will ever hold has a class row from
/// its CREATE.
pub(crate) fn ensure_classes_table(
    db: &Arc<turso_core::Database>,
    read_only: bool,
) -> crate::Result<()> {
    let conn = db.connect(turso_core::ConnectOpts::new())?;
    if let Some(table) = classes_table(&conn) {
        if (has_acl_column(&table) && has_row_security_column(&table)) || read_only {
            return Ok(());
        }
        return in_write_transaction(&conn, add_missing_columns);
    }
    refuse_relations_without_catalog(&conn)?;
    if read_only {
        return Ok(());
    }
    in_write_transaction(&conn, create_classes_table)?;
    // The open loaded the schema before the table existed, so the in-memory
    // catalog is still unloaded.
    conn.reload_dialect_schema().map_err(Into::into)
}

/// Runs `change` in a write transaction of its own. Two processes can race
/// to open the same file; BEGIN IMMEDIATE serializes them, and `change`
/// finds what the winner did.
fn in_write_transaction(
    conn: &Arc<Connection>,
    change: fn(&Arc<Connection>) -> crate::Result<()>,
) -> crate::Result<()> {
    run_internal(conn, "BEGIN IMMEDIATE")?;
    match change(conn) {
        Ok(()) => run_internal(conn, "COMMIT"),
        Err(error) => {
            let _ = run_internal(conn, "ROLLBACK");
            Err(error)
        }
    }
}

/// Refuses a database that has relations but no catalog to describe them.
fn refuse_relations_without_catalog(conn: &Arc<Connection>) -> crate::Result<()> {
    if conn.current_schema().user_relations().is_empty() {
        return Ok(());
    }
    Err(LimboError::ParseError(
        "the database has relations but no relation catalog, as an earlier version of tursopg \
         made it; this version does not open it"
            .to_string(),
    )
    .into())
}

fn create_classes_table(conn: &Arc<Connection>) -> crate::Result<()> {
    if table_exists(conn, CLASSES_TABLE_NAME)? {
        return Ok(());
    }
    run_internal(conn, CREATE_CLASSES_TABLE_SQL)?;
    run_internal(conn, CREATE_CLASSES_NAME_INDEX_SQL)?;
    if !table_exists(conn, "sqlite_sequence")? {
        run_internal(conn, "CREATE TABLE sqlite_sequence(name,seq)")?;
    }
    run_internal(
        conn,
        &format!(
            "INSERT INTO sqlite_sequence (name, seq) VALUES ('{CLASSES_TABLE_NAME}', {})",
            FIRST_NORMAL_OBJECT_ID - 1
        ),
    )
}

/// Adds the columns that a catalog made by an earlier version lacks. The
/// columns are checked again inside the write transaction, because another
/// process may have added them first.
fn add_missing_columns(conn: &Arc<Connection>) -> crate::Result<()> {
    let Some(table) = classes_table(conn) else {
        return Ok(());
    };
    if !has_acl_column(&table) {
        run_internal(conn, ADD_ACL_COLUMN_SQL)?;
    }
    if !has_row_security_column(&table) {
        run_internal(conn, ADD_ROW_SECURITY_COLUMN_SQL)?;
    }
    Ok(())
}

fn classes_table(conn: &Arc<Connection>) -> Option<Arc<BTreeTable>> {
    conn.current_schema()
        .get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME)
}

fn table_exists(conn: &Arc<Connection>, name: &str) -> crate::Result<bool> {
    Ok(conn
        .current_schema()
        .get_btree_table(NamespaceId::ROOT, name)
        .is_some())
}

fn run_internal(conn: &Arc<Connection>, sql: &str) -> crate::Result<()> {
    conn.prepare_engine_schema_statement(sql)?
        .run_ignore_rows()
        .map_err(Into::into)
}
