//! The relation catalog: stored user rows and binary-defined built-in rows.
//!
//! `sqlite_schema` describes the engine's storage: type, name, root page
//! and SQL. A private table stores user relations and hidden fields for
//! engine names, ownership dependencies and row-level security policies.
//!
//! Core knows nothing of this table. [`crate::catalog::PostgresDialect`]
//! maintains it through the engine's catalog hooks: every statement that
//! adds, renames or removes a relation calls back here to write the matching
//! row in the same transaction as its `sqlite_schema` rows, so the two can
//! never disagree after a crash, and the engine loads the rows back with
//! the rest of the frontend's schema state, see [`crate::schema_state`],
//! whenever it loads the schema.
//!
//! The PostgreSQL opening functions create these tables before
//! returning the database. Every user relation receives a row when created.
//!
//! Oids start at 16384, PostgreSQL's `FirstNormalObjectId`, and are never
//! reused inside one database, even after the relation is dropped: the
//! highest oid ever used is kept in `sqlite_sequence` under the catalog's
//! own name, like an AUTOINCREMENT table's watermark. Row-level security
//! policies take their oids from the same counter.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use turso_core::alloc::TryClone;
use turso_core::ast;
use turso_core::namespace::NamespaceId;
use turso_core::schema::{
    is_user_relation_name, BTreeTable, RelationKind, Schema, UniqueSet, SQLITE_SEQUENCE_TABLE_NAME,
};
use turso_core::security::permission::{Action, Object, PermissionDenied};
use turso_core::security::roles::RoleId;
use turso_core::{
    emit_sqlite_sequence_row_read, emit_sqlite_sequence_row_write, to_u32, AccessStatement,
    Catalog, CatalogApply, CatalogCmd, CmdContext, CmdOrigin, CmpInsFlags, CollationSeq,
    Connection, CreateTable, CursorType, DropColumn, InsertFlags, Insn, LimboError, LoadStep,
    NewOwner, Numeric, OwnerColumn, ProgramBuilder, RegisterOrLiteral, RelationName, SqliteCatalog,
    Value,
};

use turso_core::MAIN_DB_ID;
use turso_pg_parser::sqlstate::{
    SQLSTATE_DUPLICATE_OBJECT, SQLSTATE_DUPLICATE_TABLE, SQLSTATE_FEATURE_NOT_SUPPORTED,
    SQLSTATE_INSUFFICIENT_PRIVILEGE, SQLSTATE_INVALID_GRANT_OPERATION,
    SQLSTATE_READ_ONLY_SQL_TRANSACTION, SQLSTATE_UNDEFINED_OBJECT, SQLSTATE_UNDEFINED_TABLE,
};

use crate::acl::{Acl, Privileges, PUBLIC_OID};
use crate::auth_members::role_does_not_exist;
use crate::catalog::{engine_role_by_name, role_oid, role_oid_by_name};
use crate::descriptions::{emit_delete_comments, Dropped};
use crate::error::{not_supported, sqlstate_error, sqlstate_error_with_hint};
use crate::policies::{self, TablePolicies};
use crate::roles::schema_roles;
use crate::schema_init::{is_stored_catalog_table, TableInit};
use crate::schema_state::{change_schema_state, emit_catalog_apply, schema_state};
use crate::unique_index::emit_unique_index_insert;
use turso_core::security::policy::RowPolicy;

pub(crate) const CLASSES_TABLE_NAME: &str = "__turso_internal_classes";

pub(crate) const CLASSES_NAME_INDEX_NAME: &str = "__turso_internal_classes_name";

/// PostgreSQL's `FirstNormalObjectId`: the first oid given to a user
/// relation. Smaller oids belong to built-in catalog objects.
pub(crate) const FIRST_NORMAL_OBJECT_ID: i64 = 16384;

/// Persistent relation identity, ownership, privileges and policies. Public
/// pg_class fields that follow from the schema are computed by the virtual table.
pub(crate) const CREATE_CLASSES_TABLE_SQL: &str = "CREATE TABLE __turso_internal_classes (\
    oid INTEGER PRIMARY KEY, \
    relname TEXT NOT NULL, \
    relenginenamespace INTEGER NOT NULL, \
    relkind TEXT NOT NULL, \
    relowner INTEGER NOT NULL, \
    relownedby INTEGER NOT NULL, \
    relprivileges TEXT, \
    relrowsecurity BOOLEAN NOT NULL, \
    relpolicies TEXT, \
    relownedbycolumn TEXT, \
    relenginename TEXT, \
    relindrelid INTEGER NOT NULL)";

const CLASS_COLUMNS: [&str; 12] = [
    "oid",
    "relname",
    "relenginenamespace",
    "relkind",
    "relowner",
    "relownedby",
    "relprivileges",
    "relrowsecurity",
    "relpolicies",
    "relownedbycolumn",
    "relenginename",
    "relindrelid",
];

const CLASS_COLUMN_COUNT: usize = CLASS_COLUMNS.len();

const COLUMN_RELNAME: usize = column_position("relname");
const COLUMN_RELENGINENAMESPACE: usize = column_position("relenginenamespace");
const COLUMN_RELPRIVILEGES: usize = column_position("relprivileges");
const COLUMN_RELOWNER: usize = column_position("relowner");
const COLUMN_RELROWSECURITY: usize = column_position("relrowsecurity");
const COLUMN_RELOWNEDBY: usize = column_position("relownedby");
const COLUMN_RELPOLICIES: usize = column_position("relpolicies");
const COLUMN_RELOWNEDBYCOLUMN: usize = column_position("relownedbycolumn");
const COLUMN_RELENGINENAME: usize = column_position("relenginename");
const COLUMN_RELINDRELID: usize = column_position("relindrelid");

const fn column_position(name: &str) -> usize {
    let mut position = 0;
    while position < CLASS_COLUMNS.len() {
        if same_text(CLASS_COLUMNS[position], name) {
            return position;
        }
        position += 1;
    }
    panic!("not a column of the classes table");
}

const fn same_text(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

const CREATE_CLASSES_NAME_INDEX_SQL: &str = "CREATE UNIQUE INDEX __turso_internal_classes_name \
     ON __turso_internal_classes (relenginenamespace, relname)";

/// Fails for a database that has relations and no class table, as an
/// earlier version of tursopg made it.
pub(crate) fn refuse_relations_without_class_table(schema: &Schema) -> turso_core::Result<()> {
    if schema
        .get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME)
        .is_none()
        && !schema.user_relations().is_empty()
    {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "the database has relations but no relation catalog, as an earlier version of tursopg \
             made it; this version does not open it",
            None,
        ));
    }
    Ok(())
}

/// A database that the SQLite frontend made can have this table already.
const CREATE_SEQUENCE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS sqlite_sequence(name,seq)";

/// Stores the highest oid ever used. No oid is used yet, so it is the one
/// before [`FIRST_NORMAL_OBJECT_ID`].
const INSERT_HIGHEST_OID_SQL: &str =
    "INSERT INTO sqlite_sequence (name, seq) VALUES ('__turso_internal_classes', 16383)";

// The statement above spells the oid so that it can be a constant.
const _: () = assert!(FIRST_NORMAL_OBJECT_ID - 1 == 16383);

pub(crate) const TABLE_INIT: TableInit = TableInit {
    table: CLASSES_TABLE_NAME,
    statements: &[
        CREATE_CLASSES_TABLE_SQL,
        CREATE_CLASSES_NAME_INDEX_SQL,
        CREATE_SEQUENCE_TABLE_SQL,
        INSERT_HIGHEST_OID_SQL,
    ],
};

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
    /// The name of the column that owns this sequence, None when no relation
    /// owns it.
    pub(crate) relownedbycolumn: Option<String>,
    /// The name the engine's schema gives the relation, None for the primary
    /// key index of a rowid table, which the schema does not have.
    pub(crate) engine_name: Option<String>,
    /// The oid of the table of an index, 0 for any other relation.
    pub(crate) relindrelid: i64,
    /// The privileges `GRANT` gave, None before the first `GRANT`.
    pub(crate) relacl: Option<Acl>,
    /// Whether the table has row-level security.
    pub(crate) relrowsecurity: bool,
    /// The table's row-level security policies.
    pub(crate) relpolicies: TablePolicies,
}

impl RelationClass {
    fn is_owned_by_column(&self, column: &str) -> bool {
        self.relownedbycolumn
            .as_deref()
            .is_some_and(|owner| owner.eq_ignore_ascii_case(column))
    }
}

/// The class rows of a database, loaded as the engine's opaque dialect
/// schema state.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClassCatalog {
    by_oid: BTreeMap<i64, RelationClass>,
    oids_by_engine_name: HashMap<(NamespaceId, String), i64>,
    oids_by_relname: HashMap<(NamespaceId, String), i64>,
    index_oids_by_table: HashMap<i64, BTreeSet<i64>>,
}

impl ClassCatalog {
    /// Returns the catalog of the rows read from the classes table for
    /// `schema`, after checking that every user relation of `schema` has a
    /// row.
    pub(crate) fn build(schema: &Schema, rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut catalog = Self::new(rows)?;
        let relations = RelationSchemas::new(schema);
        for (namespace, name) in relations.kinds.keys() {
            if catalog.get_by_name(*namespace, name).is_none() {
                return Err(LimboError::Corrupt(format!(
                    "relation \"{name}\" of namespace {} has no row in {CLASSES_TABLE_NAME}",
                    namespace.as_u32()
                )));
            }
        }
        for class in catalog.classes() {
            catalog.relation_metadata(schema, &relations, class)?;
        }
        // The file stores only user relations. Built-in identities belong to the binary.
        for &(oid, name, kind, _) in crate::catalog::BUILTIN_CATALOGS {
            catalog.add(RelationClass {
                oid,
                relname: name.to_string(),
                relnamespace: NamespaceId::ROOT,
                relkind: kind,
                relowner: RoleId::SUPERUSER,
                relownedby: 0,
                relownedbycolumn: None,
                engine_name: Some(name.to_string()),
                relindrelid: 0,
                relacl: None,
                relrowsecurity: false,
                relpolicies: TablePolicies::default(),
            });
        }
        Ok(catalog)
    }

    /// Checks stored identities against the schema and derives relation metadata.
    /// Stored rows are input from disk, so an invalid identity is corruption.
    fn relation_metadata(
        &self,
        schema: &Schema,
        relations: &RelationSchemas,
        class: &RelationClass,
    ) -> turso_core::Result<(usize, bool, usize)> {
        let invalid = || {
            LimboError::Corrupt(format!(
                "stored class row for \"{}\" has no matching relation",
                class.relname
            ))
        };
        if class.oid < FIRST_NORMAL_OBJECT_ID {
            return Err(invalid());
        }
        let (natts, has_index, checks) = {
            if let Some(name) = &class.engine_name {
                if relations.kinds.get(&(class.relnamespace, name.clone())) != Some(&class.relkind)
                {
                    return Err(invalid());
                }
            } else if class.relkind != RelationKind::Index {
                return Err(invalid());
            }
            match class.relkind {
                RelationKind::Table | RelationKind::MaterializedView => {
                    let table = schema
                        .get_btree_table(class.relnamespace, &class.relname)
                        .ok_or_else(invalid)?;
                    let rowid_indexes = self
                        .indexes_of(class.oid)
                        .filter(|index| index.engine_name.is_none())
                        .count();
                    if rowid_indexes != usize::from(table.get_rowid_alias_column().is_some()) {
                        return Err(invalid());
                    }
                    (
                        table.columns().len(),
                        rowid_indexes != 0
                            || schema
                                .get_indices(class.relnamespace, &class.relname)
                                .next()
                                .is_some(),
                        table.check_constraints.len(),
                    )
                }
                RelationKind::View => {
                    let view = schema
                        .get_view(class.relnamespace, &class.relname)
                        .ok_or_else(invalid)?;
                    (view.columns.len(), false, 0)
                }
                RelationKind::Sequence => (3, false, 0),
                RelationKind::Index => {
                    let table_class = self.get_by_oid(class.relindrelid).ok_or_else(invalid)?;
                    if table_class.relnamespace != class.relnamespace
                        || table_class.relowner != class.relowner
                        || !matches!(
                            table_class.relkind,
                            RelationKind::Table | RelationKind::MaterializedView
                        )
                    {
                        return Err(invalid());
                    }
                    let table = schema
                        .get_btree_table(class.relnamespace, &table_class.relname)
                        .ok_or_else(invalid)?;
                    let count = if let Some(name) = &class.engine_name {
                        let (table_name, count) = relations
                            .index_columns
                            .get(&(class.relnamespace, name.clone()))
                            .ok_or_else(invalid)?;
                        if table_name != &table.name {
                            return Err(invalid());
                        }
                        *count
                    } else {
                        if table.get_rowid_alias_column().is_none()
                            || self
                                .indexes_of(table_class.oid)
                                .filter(|index| index.engine_name.is_none())
                                .count()
                                != 1
                        {
                            return Err(invalid());
                        }
                        1
                    };
                    (count, false, 0)
                }
            }
        };
        Ok((natts, has_index, checks))
    }

    /// Returns a catalog of the rows read from the classes table.
    fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            if row.len() != CLASS_COLUMN_COUNT {
                return Err(corrupt_row(row));
            }
            let class = class_from_row(row)?;
            let engine_name_is_taken = class
                .engine_name
                .as_ref()
                .is_some_and(|name| catalog.get_by_name(class.relnamespace, name).is_some());
            if catalog.by_oid.contains_key(&class.oid)
                || catalog.name_is_taken(class.relnamespace, &class.relname)
                || engine_name_is_taken
            {
                return Err(LimboError::Corrupt(format!(
                    "{CLASSES_TABLE_NAME} has a duplicate relation: {class:?}"
                )));
            }
            catalog.add(class);
        }
        Ok(catalog)
    }

    /// Public pg_class rows from persistent identities and this schema snapshot.
    pub(crate) fn rows(&self, schema: &Schema) -> Vec<Vec<Value>> {
        let relations = RelationSchemas::new(schema);
        self.classes()
            // CTAS allocates its identity before running the source query, but
            // publishes its schema entry afterwards. Like PostgreSQL, that
            // query must not see the relation it is still creating. File rows
            // have already been checked by build; only live DDL has this state.
            .filter(|class| {
                class.oid < FIRST_NORMAL_OBJECT_ID
                    || class.engine_name.as_ref().is_none_or(|name| {
                        relations.kinds.get(&(class.relnamespace, name.clone()))
                            == Some(&class.relkind)
                    })
            })
            .map(|class| {
                let (natts, has_index, checks, shared) = if class.oid < FIRST_NORMAL_OBJECT_ID {
                    let &(_, name, _, shared) = crate::catalog::BUILTIN_CATALOGS
                        .iter()
                        .find(|&&(oid, _, _, _)| oid == class.oid)
                        .expect("built-in identity");
                    (
                        crate::catalog::builtin_catalog_columns(schema, name).len(),
                        // FIXME(pg-catalog): Catalog indexes are not implemented.
                        // Derive this flag from their identities when they are.
                        false,
                        0,
                        shared,
                    )
                } else {
                    let (natts, has_index, checks) = self
                        .relation_metadata(schema, &relations, class)
                        .expect("loaded relation identities agree with the schema");
                    (natts, has_index, checks, false)
                };
                public_class_row(schema, class, natts, has_index, checks, shared)
            })
            .collect()
    }

    /// The class rows, ordered by oid.
    pub(crate) fn classes(&self) -> impl Iterator<Item = &RelationClass> {
        self.by_oid.values()
    }

    pub(crate) fn get_by_oid(&self, oid: i64) -> Option<&RelationClass> {
        self.by_oid.get(&oid)
    }

    /// The row of the relation that the engine's schema calls `name`.
    pub(crate) fn get_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RelationClass> {
        self.oids_by_engine_name
            .get(&(namespace, name.to_ascii_lowercase()))
            .and_then(|oid| self.by_oid.get(oid))
    }

    /// Whether a relation of `namespace` has the PostgreSQL name `name`.
    pub(crate) fn name_is_taken(&self, namespace: NamespaceId, name: &str) -> bool {
        self.oids_by_relname
            .contains_key(&(namespace, name.to_string()))
    }

    /// The rows of the indexes of the table with oid `table_oid`, ordered by
    /// oid.
    pub(crate) fn indexes_of(&self, table_oid: i64) -> impl Iterator<Item = &RelationClass> {
        self.index_oids_by_table
            .get(&table_oid)
            .into_iter()
            .flatten()
            .map(|oid| self.by_oid.get(oid).expect("indexed class exists"))
    }

    /// The relations the table with oid `table_oid` owns: the sequences of
    /// its serial columns.
    fn owned_by(&self, table_oid: i64) -> impl Iterator<Item = &RelationClass> {
        self.by_oid
            .values()
            .filter(move |class| class.relownedby == table_oid)
    }

    fn get_mut_by_name(
        &mut self,
        namespace: NamespaceId,
        name: &str,
    ) -> Option<&mut RelationClass> {
        let oid = self
            .oids_by_engine_name
            .get(&(namespace, name.to_string()))?;
        Some(self.by_oid.get_mut(oid).expect("all maps hold the row"))
    }

    fn add(&mut self, class: RelationClass) {
        assert!(
            !self.by_oid.contains_key(&class.oid),
            "oid {} is already in use",
            class.oid
        );
        let previous = self
            .oids_by_relname
            .insert((class.relnamespace, class.relname.clone()), class.oid);
        assert!(
            previous.is_none(),
            "relation {} already has a class row",
            class.relname
        );
        if let Some(engine_name) = &class.engine_name {
            let previous = self
                .oids_by_engine_name
                .insert((class.relnamespace, engine_name.clone()), class.oid);
            assert!(
                previous.is_none(),
                "relation {engine_name} already has a class row"
            );
        }
        if class.relkind == RelationKind::Index {
            assert!(self
                .index_oids_by_table
                .entry(class.relindrelid)
                .or_default()
                .insert(class.oid));
        }
        self.by_oid.insert(class.oid, class);
    }

    fn remove(&mut self, namespace: NamespaceId, engine_name: &str) -> Option<RelationClass> {
        let oid = self
            .oids_by_engine_name
            .get(&(namespace, engine_name.to_string()))
            .copied()?;
        self.remove_by_oid(oid)
    }

    fn remove_by_oid(&mut self, oid: i64) -> Option<RelationClass> {
        let class = self.by_oid.remove(&oid)?;
        if class.relkind == RelationKind::Index {
            let indexes = self
                .index_oids_by_table
                .get_mut(&class.relindrelid)
                .expect("index has a table entry");
            assert!(indexes.remove(&oid));
            if indexes.is_empty() {
                self.index_oids_by_table.remove(&class.relindrelid);
            }
        }
        self.oids_by_relname
            .remove(&(class.relnamespace, class.relname.clone()));
        if let Some(engine_name) = &class.engine_name {
            self.oids_by_engine_name
                .remove(&(class.relnamespace, engine_name.clone()));
        }
        Some(class)
    }
}

/// Schema lookups built once per load or scan, rather than searching every
/// relation and every index for each catalog row. The stored tables of the
/// catalog tables and their indexes are the frontend's, not the user's, so
/// they are not among the relations.
struct RelationSchemas {
    kinds: HashMap<(NamespaceId, String), RelationKind>,
    index_columns: HashMap<(NamespaceId, String), (String, usize)>,
}

impl RelationSchemas {
    fn new(schema: &Schema) -> Self {
        let index_columns: HashMap<(NamespaceId, String), (String, usize)> = schema
            .namespace_ids()
            .flat_map(|ns| {
                schema
                    .indexes(ns)
                    .filter(move |index| !is_stored_catalog_table(ns, &index.table_name))
                    .map(move |index| {
                        (
                            (ns, index.name.clone()),
                            (index.table_name.clone(), index.columns.len()),
                        )
                    })
            })
            .collect();
        let kinds = schema
            .user_relations()
            .into_iter()
            .filter(|(ns, name, kind)| match kind {
                RelationKind::Index => index_columns.contains_key(&(*ns, name.clone())),
                _ => !is_stored_catalog_table(*ns, name),
            })
            .map(|(ns, name, kind)| ((ns, name), kind))
            .collect();
        Self {
            kinds,
            index_columns,
        }
    }
}

/// The public columns, in PostgreSQL order. Schema metadata and ACL names are
/// derived from the same snapshot as the relation's persistent identity.
fn public_class_row(
    schema: &Schema,
    class: &RelationClass,
    natts: usize,
    has_index: bool,
    checks: usize,
    shared: bool,
) -> Vec<Value> {
    // Views have no storage and use PostgreSQL's unknown row estimate (-1).
    // FIXME(pg-statistics): Stored relations use PostgreSQL's initial estimates.
    // Collected planner statistics should replace the estimates for stored relations.
    let (access_method, replica_identity, relpages, reltuples) = match class.relkind {
        RelationKind::Table | RelationKind::MaterializedView => (2, "d", 0, -1.0),
        RelationKind::Index => (403, "d", 1, 0.0),
        RelationKind::Sequence => (0, "n", 1, 1.0),
        RelationKind::View => (0, "n", 0, -1.0),
    };
    let acl = class.relacl.as_ref().map_or(Value::Null, |acl| {
        Value::build_text(acl.display_text(&|oid| {
            crate::catalog::role_name_by_oid(schema_roles(schema), oid)
                .unwrap_or_else(|| oid.to_string())
        }))
    });
    vec![
        Value::from_i64(class.oid),               // oid
        Value::build_text(class.relname.clone()), // relname
        Value::from_i64(crate::catalog::namespace_oid(schema, class.relnamespace)),
        Value::from_i64(0), // reltype
        Value::from_i64(0), // reloftype
        Value::from_i64(role_oid(class.relowner)),
        Value::from_i64(access_method), // relam
        Value::from_i64(0),             // relfilenode
        Value::from_i64(0),             // reltablespace
        Value::from_i64(relpages),
        Value::from_f64(reltuples),
        Value::from_i64(0), // relallvisible
        Value::from_i64(0), // relallfrozen
        Value::from_i64(0), // reltoastrelid
        Value::from_i64(i64::from(has_index)),
        Value::from_i64(i64::from(shared)),
        Value::build_text("p"), // relpersistence
        Value::build_text(relkind_letter(class.relkind)),
        Value::from_i64(natts as i64),
        Value::from_i64(checks as i64),
        // FIXME(pg-rewrite): Views have an INSTEAD SELECT _RETURN rule in
        // PostgreSQL. A stored rule catalog should supply relhasrules.
        Value::from_i64(i64::from(class.relkind == RelationKind::View)),
        Value::from_i64(0), // relhastriggers
        Value::from_i64(0), // relhassubclass
        Value::from_i64(i64::from(class.relrowsecurity)),
        Value::from_i64(0), // relforcerowsecurity
        Value::from_i64(1), // relispopulated
        Value::build_text(replica_identity),
        Value::from_i64(0), // relispartition
        Value::from_i64(0), // relrewrite
        Value::from_i64(0), // relfrozenxid
        Value::from_i64(0), // relminmxid
        acl,
        Value::Null, // reloptions
        Value::Null, // relpartbound
    ]
}

fn class_from_row(row: &[Value]) -> turso_core::Result<RelationClass> {
    if row.len() != CLASS_COLUMN_COUNT {
        return Err(corrupt_row(row));
    }
    let oid = &row[column_position("oid")];
    let relname = &row[column_position("relname")];
    let relnamespace = &row[column_position("relenginenamespace")];
    let relkind = &row[column_position("relkind")];
    let relowner = &row[column_position("relowner")];
    let relownedby = &row[column_position("relownedby")];
    let relacl = &row[column_position("relprivileges")];
    let relrowsecurity = &row[column_position("relrowsecurity")];
    let relpolicies = &row[column_position("relpolicies")];
    let relownedbycolumn = &row[column_position("relownedbycolumn")];
    let relenginename = &row[column_position("relenginename")];
    let relindrelid = &row[column_position("relindrelid")];
    match (
        oid,
        relname,
        relnamespace,
        relkind,
        relowner,
        relownedby,
        relrowsecurity,
        relindrelid,
    ) {
        (
            Value::Numeric(Numeric::Integer(oid)),
            Value::Text(relname),
            Value::Numeric(Numeric::Integer(relnamespace)),
            Value::Text(relkind),
            Value::Numeric(Numeric::Integer(relowner)),
            Value::Numeric(Numeric::Integer(relownedby)),
            Value::Numeric(Numeric::Integer(relrowsecurity @ (0 | 1))),
            Value::Numeric(Numeric::Integer(relindrelid)),
        ) if *oid > 0
            && u32::try_from(*relnamespace).is_ok()
            && *relowner >= 0
            && *relownedby >= 0
            && *relindrelid >= 0 =>
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
            let relpolicies = match relpolicies {
                Value::Null => TablePolicies::default(),
                Value::Text(text) => {
                    policies::parse(text.as_str()).ok_or_else(|| corrupt_row(row))?
                }
                _ => return Err(corrupt_row(row)),
            };
            let relownedbycolumn = match relownedbycolumn {
                Value::Null => None,
                Value::Text(text) if *relownedby != 0 => Some(text.as_str().to_string()),
                _ => return Err(corrupt_row(row)),
            };
            let engine_name = match relenginename {
                Value::Null => None,
                Value::Text(text) => Some(text.as_str().to_string()),
                _ => return Err(corrupt_row(row)),
            };
            Ok(RelationClass {
                oid: *oid,
                relname: relname.as_str().to_string(),
                engine_name,
                relindrelid: *relindrelid,
                relnamespace: if *oid < FIRST_NORMAL_OBJECT_ID {
                    NamespaceId::ROOT
                } else {
                    NamespaceId::new(*relnamespace as u32)
                },
                relkind,
                relowner: crate::catalog::role_id_from_oid(*relowner)
                    .filter(|id| *id != RoleId::PUBLIC)
                    .ok_or_else(|| corrupt_row(row))?,
                relownedby: *relownedby,
                relownedbycolumn,
                relacl,
                relrowsecurity: *relrowsecurity == 1,
                relpolicies,
            })
        }
        _ => Err(corrupt_row(row)),
    }
}

fn corrupt_row(row: &[Value]) -> LimboError {
    LimboError::Corrupt(format!("{CLASSES_TABLE_NAME} has an invalid row: {row:?}"))
}

/// The catalog that the engine loaded with the frontend's schema state, or
/// None when it is not loaded, as in a read-only open of an empty database.
pub(crate) fn class_catalog(schema: &Schema) -> Option<&ClassCatalog> {
    schema_state(schema).map(|state| state.classes.as_ref())
}

/// The query that reads the relation catalog for the frontend's schema
/// state, or None when the database has no classes table.
pub(crate) fn schema_state_query(schema: &Schema) -> turso_core::Result<Option<String>> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME) else {
        return Ok(None);
    };
    if table.columns().len() != CLASS_COLUMN_COUNT
        || table
            .columns()
            .iter()
            .zip(CLASS_COLUMNS)
            .any(|(column, name)| column.name.as_deref() != Some(name))
    {
        return Err(not_supported("the database has a relation catalog from an earlier version of tursopg; this version does not open it"));
    }
    Ok(Some(format!(
        "SELECT {} FROM {CLASSES_TABLE_NAME}",
        CLASS_COLUMNS.join(", ")
    )))
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

/// [`turso_core::Dialect::owned_sequences`] for the PostgreSQL frontend.
pub(crate) fn owned_sequences(
    schema: &Schema,
    namespace: NamespaceId,
    table_name: &str,
    column: Option<&str>,
) -> Vec<String> {
    let Some(catalog) = class_catalog(schema) else {
        return Vec::new();
    };
    let Some(table) = catalog.get_by_name(namespace, table_name) else {
        return Vec::new();
    };
    catalog
        .owned_by(table.oid)
        .filter(|owned| owned.relkind == RelationKind::Sequence)
        .filter(|owned| column.is_none_or(|column| owned.is_owned_by_column(column)))
        .map(|owned| owned.relname.clone())
        .collect()
}

/// [`turso_core::Dialect::role_has_privilege`] for the PostgreSQL frontend:
/// a role has the privileges the relation's list gives it, by oid or through
/// PUBLIC, together with those it gives the roles whose privileges the role
/// inherits. A relation without a list has the default one, which gives the
/// owner every privilege and no one else any. As in PostgreSQL, an owner
/// that revoked a privilege from itself no longer has it. A table without a
/// class row is one of the engine's own, such as `__turso_internal_authid` or
/// `sqlite_sequence`, and only a superuser uses it, because nothing says who
/// else may.
pub(crate) fn role_has_privilege(
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
    role: RoleId,
    privilege: ast::Privilege,
) -> bool {
    if is_stored_catalog_table(namespace, name) {
        return privilege == ast::Privilege::Select;
    }
    let Some(class) =
        class_catalog(schema).and_then(|catalog| catalog.get_by_name(namespace, name))
    else {
        return false;
    };
    let roles = schema_roles(schema);
    let Some(acl) = &class.relacl else {
        return roles.has_privileges_of(role, class.relowner);
    };
    let wanted = Privileges::named_on(class.relkind, &[privilege]);
    !wanted.is_empty() && acl.grants(&roles.oids_of_roles_used_by(role), wanted)
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

/// [`turso_core::Dialect::row_security_policies`] for the PostgreSQL
/// frontend.
pub(crate) fn row_security_policies<'a>(
    schema: &'a Schema,
    namespace: NamespaceId,
    name: &str,
) -> &'a [RowPolicy] {
    class_catalog(schema)
        .and_then(|catalog| catalog.get_by_name(namespace, name))
        .map_or(&[], |class| class.relpolicies.as_slice())
}

/// The catalog of a PostgreSQL database: the engine's `sqlite_schema` rows,
/// and a class row for each relation of the main database.
#[derive(Debug)]
pub(crate) struct PostgresCatalog;

impl Catalog for PostgresCatalog {
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        cmd: &CatalogCmd<'_>,
    ) -> turso_core::Result<()> {
        SqliteCatalog.emit(program, ctx, cmd)?;
        match cmd {
            CatalogCmd::CreateTable(table) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    emit_new_table_rows(program, &rows, table)?;
                }
            }
            CatalogCmd::CreateIndex(index) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    let table_oid = rows.oid_of(index.namespace, index.table);
                    rows.new_relation(
                        program,
                        NewRelation {
                            namespace: index.namespace,
                            name: index.written_name.as_str(),
                            engine_name: Some(index.name),
                            kind: RelationKind::Index,
                            table: Some(index.table),
                            indexed_table: IndexedTable::Oid(table_oid),
                        },
                    )?;
                }
            }
            CatalogCmd::DropIndex(index) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.dropped_relation(program, index.namespace, index.name)?;
                }
            }
            CatalogCmd::CreateSequence(sequence) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        NewRelation::of_relation(
                            sequence.namespace,
                            sequence.name,
                            RelationKind::Sequence,
                        ),
                    )?;
                }
            }
            CatalogCmd::DropSequence(sequence) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.dropped_relation(program, sequence.namespace, sequence.name)?;
                }
            }
            CatalogCmd::DropTable(table) => {
                // A virtual table has no class row, in any database.
                if table.is_virtual {
                    return Ok(());
                }
                // The class rows that go away with the table: its own and
                // those of its indexes.
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    let table_oid = rows.oid_of(table.namespace, table.name);
                    rows.dropped_relation(program, table.namespace, table.name)?;
                    for index in table.indexes {
                        rows.dropped_relation(program, table.namespace, index)?;
                    }
                    rows.dropped_rowid_primary_key_index(program, table.namespace, table_oid)?;
                }
            }
            CatalogCmd::CreateView(view) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        NewRelation {
                            name: view.written_name.as_str(),
                            ..NewRelation::of_relation(
                                view.namespace,
                                view.name,
                                RelationKind::View,
                            )
                        },
                    )?;
                }
            }
            CatalogCmd::CreateMaterializedView(view) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        NewRelation {
                            name: view.written_name.as_str(),
                            ..NewRelation::of_relation(
                                view.namespace,
                                view.name,
                                RelationKind::MaterializedView,
                            )
                        },
                    )?;
                }
            }
            CatalogCmd::DropView(view) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.dropped_relation(program, view.namespace, view.name)?;
                }
            }
            CatalogCmd::RenameTable(table) => {
                // A virtual table has no class row, in any database.
                if table.is_virtual {
                    return Ok(());
                }
                // The automatic indexes get their new names with the table.
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.renamed_relation(
                        program,
                        table.namespace,
                        table.old_name,
                        table.written_new_name,
                    )?;
                    for (old_name, new_name) in table.renamed_automatic_indexes() {
                        rows.renamed_engine_name(program, table.namespace, old_name, &new_name);
                    }
                }
            }
            CatalogCmd::RenameColumn(column) => {
                rename_owner_column(
                    program,
                    ctx,
                    column.namespace,
                    column.table,
                    column.old_name,
                    column.new_name.as_str(),
                )?;
            }
            CatalogCmd::AlterColumn(column) => {
                rename_owner_column(
                    program,
                    ctx,
                    column.namespace,
                    column.table,
                    column.old_name,
                    column.definition.col_name.as_str(),
                )?;
            }
            // Column metadata is derived from the schema by the virtual catalog.
            CatalogCmd::AddColumn(column) => {
                if ctx.database_id == MAIN_DB_ID {
                    refuse_changed_view_width(
                        ctx.schema,
                        column.namespace,
                        column.name,
                        column.table,
                    )?;
                }
            }
            // The statement drops the sequences that the column owns
            // itself, each with its class row.
            CatalogCmd::DropColumn(column) => {
                if ctx.database_id != MAIN_DB_ID {
                    return Ok(());
                }
                refuse_changed_view_width(ctx.schema, column.namespace, column.name, column.table)?;
                if ctx.origin != CmdOrigin::Engine && ctx.database_id == MAIN_DB_ID {
                    emit_dropped_column_comments(program, ctx, column)?;
                }
            }
            // A trigger has no class row, and neither has a virtual table,
            // a type, a domain or a schema.
            CatalogCmd::CreateTrigger(_)
            | CatalogCmd::DropTrigger(_)
            | CatalogCmd::CreateVirtualTable(_)
            | CatalogCmd::CreateType(_)
            | CatalogCmd::DropType(_)
            | CatalogCmd::CreateSchema(_)
            | CatalogCmd::DropSchema(_) => {}
        }
        Ok(())
    }

    fn emit_access(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        statement: &AccessStatement<'_>,
    ) -> turso_core::Result<()> {
        match statement {
            AccessStatement::CreateRole { name, options } => {
                crate::roles::translate_create_role(program, ctx.schema, name, options)
            }
            AccessStatement::DropRole { if_exists, roles } => crate::roles::translate_drop_role(
                program,
                ctx.schema,
                ctx.connection,
                *if_exists,
                roles,
            ),
            AccessStatement::GrantRole {
                revoke,
                roles,
                grantees,
            } => crate::auth_members::translate_grant_role(
                program,
                ctx.schema,
                ctx.connection,
                *revoke,
                roles,
                grantees,
            ),
            // The owner column of a sequence is in the sequence's class row.
            AccessStatement::ChangeOwner {
                owner: NewOwner::Column(_),
                ..
            } if !writes_class_rows(ctx)? => Ok(()),
            _ => translate_access_statement(program, ctx.schema, ctx.connection, statement),
        }
    }

    fn load_steps(&self) -> Vec<Box<dyn LoadStep>> {
        crate::schema_state::load_steps()
    }
}

/// Refuses a column change that would change an existing view's width.
fn refuse_changed_view_width(
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
    changed: &BTreeTable,
) -> turso_core::Result<()> {
    let views: Vec<_> = schema
        .namespace_ids()
        .flat_map(|view_namespace| {
            schema.views(view_namespace).filter_map(move |(_, view)| {
                schema
                    .referenced_tables_of(view_namespace, &view.select_stmt)
                    .contains(&(namespace, name.to_string()))
                    .then_some((view_namespace, view))
            })
        })
        .collect();
    if views.is_empty() {
        return Ok(());
    }
    let mut after = schema.try_clone()?;
    let (_, _, table) = after
        .all_tables_mut()
        .find(|(ns, table_name, _)| *ns == namespace && *table_name == name)
        .expect("ALTER TABLE has a table");
    *table = Arc::new(turso_core::schema::Table::BTree(Arc::new(
        changed.try_clone()?,
    )));
    for (view_namespace, view) in views {
        let columns = turso_core::schema::view::extract_view_columns(
            view_namespace,
            &view.select_stmt,
            &after,
        )?;
        if columns.columns.len() != view.columns.len() {
            // FIXME(pg-rewrite): PostgreSQL expands SELECT * when it creates
            // a view. Store a resolved view query to allow this ALTER without
            // changing the view's output columns.
            return Err(sqlstate_error_with_hint(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "cannot alter table \"{name}\" because it would change the number of columns returned by view \"{}\"",
                    view.name
                ),
                format!(
                    "Drop view \"{}\", alter table \"{name}\", and then recreate the view.",
                    view.name
                ),
            ));
        }
    }
    Ok(())
}

/// What a statement tells [`ClassRowWriter::new_relation`] about a new
/// relation.
struct NewRelation<'a> {
    namespace: NamespaceId,
    /// The PostgreSQL name.
    name: &'a str,
    /// The name of the object in the schema, None when there is none.
    engine_name: Option<&'a str>,
    kind: RelationKind,
    /// The table whose owner owns the relation, for an index.
    table: Option<&'a str>,
    indexed_table: IndexedTable,
}

impl<'a> NewRelation<'a> {
    /// A relation that is not an index, named the same in PostgreSQL and in
    /// the schema.
    fn of_relation(namespace: NamespaceId, name: &'a str, kind: RelationKind) -> Self {
        Self {
            namespace,
            name,
            engine_name: Some(name),
            kind,
            table: None,
            indexed_table: IndexedTable::None,
        }
    }
}

/// Where the oid of an index's table comes from.
#[derive(Clone, Copy)]
enum IndexedTable {
    /// The relation is not an index.
    None,
    Oid(i64),
    /// The register of a table that the same statement creates.
    Register(usize),
}

/// Inserts the class rows of a table and of the indexes of its PRIMARY KEY
/// and UNIQUE constraints. PostgreSQL names such an index after its
/// constraint, and the schema names it `sqlite_autoindex_<table>_<n>`. The
/// primary key of a rowid table has no object in the schema.
fn emit_new_table_rows(
    program: &mut ProgramBuilder,
    rows: &ClassRowWriter<'_>,
    table: &CreateTable<'_>,
) -> turso_core::Result<()> {
    let Some(table_oid_reg) = rows.new_relation(
        program,
        NewRelation {
            name: table.written_name.as_str(),
            ..NewRelation::of_relation(table.namespace, table.name, RelationKind::Table)
        },
    )?
    else {
        return Ok(());
    };
    let btree = table.table;
    let mut automatic_indexes = table.automatic_indexes.iter();
    let mut chosen_names = vec![table.written_name.as_str().to_string()];
    let mut new_index = |program: &mut ProgramBuilder,
                         key: ConstraintKey<'_>,
                         engine_name: Option<&str>|
     -> turso_core::Result<()> {
        let name = constraint_index_name(rows.schema, table, key, &chosen_names)?;
        chosen_names.push(name.clone());
        rows.new_relation(
            program,
            NewRelation {
                namespace: table.namespace,
                name: &name,
                engine_name,
                kind: RelationKind::Index,
                table: Some(table.name),
                indexed_table: IndexedTable::Register(table_oid_reg),
            },
        )?;
        Ok(())
    };
    if btree.get_rowid_alias_column().is_some() {
        new_index(program, ConstraintKey::PrimaryKey, None)?;
    }
    for set in &btree.unique_sets {
        if btree.get_rowid_alias_column().is_some() && set.is_primary_key {
            continue;
        }
        let index = automatic_indexes
            .next()
            .expect("every constraint index of a table has an automatic index");
        let key = if crate::catalog::unique_set_is_primary_key(btree, set) {
            ConstraintKey::PrimaryKey
        } else {
            ConstraintKey::Unique(set)
        };
        new_index(program, key, Some(&index.name))?;
    }
    Ok(())
}

/// A PRIMARY KEY or UNIQUE constraint of a table.
#[derive(Clone, Copy)]
enum ConstraintKey<'a> {
    PrimaryKey,
    Unique(&'a UniqueSet),
}

/// The name PostgreSQL gives the index of a constraint: the constraint's
/// name when the statement wrote one, otherwise `<table>_pkey` or
/// `<table>_<columns>_key`, followed by the first number that makes it
/// unused in the namespace.
fn constraint_index_name(
    schema: &Schema,
    table: &CreateTable<'_>,
    key: ConstraintKey<'_>,
    chosen_names: &[String],
) -> turso_core::Result<String> {
    use turso_pg_parser::translator::available_object_name;

    let table_name = table.written_name.as_str();
    let (written, columns, label) = match key {
        ConstraintKey::PrimaryKey => (
            table.table.primary_key_constraint_name.clone(),
            None,
            "pkey",
        ),
        ConstraintKey::Unique(set) => {
            let columns: Vec<&str> = set
                .columns
                .iter()
                .map(|c| {
                    table
                        .table
                        .get_column(&c.name)
                        .expect("constraint columns exist")
                        .1
                        .name
                        .as_deref()
                        .expect("table columns have names")
                })
                .collect();
            (set.constraint_name.clone(), Some(columns.join("_")), "key")
        }
    };
    let is_taken = |name: &str| {
        chosen_names.iter().any(|chosen| chosen == name)
            || class_catalog(schema)
                .is_some_and(|catalog| catalog.name_is_taken(table.namespace, name))
    };
    if let Some(name) = written {
        if is_taken(&name) {
            return Err(sqlstate_error(
                SQLSTATE_DUPLICATE_TABLE,
                format!("relation \"{name}\" already exists"),
                None,
            ));
        }
        return Ok(name);
    }
    available_object_name(table_name, columns.as_deref(), label, |name| {
        Ok(is_taken(name))
    })
}

/// Deletes the comment of the column that `column.table` no longer has, and
/// moves the comments of the later columns to the numbers those columns now
/// have.
fn emit_dropped_column_comments(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    column: &DropColumn<'_>,
) -> turso_core::Result<()> {
    // An engine table has no row and no comments.
    let Some(class) = class_catalog(ctx.schema)
        .and_then(|catalog| catalog.get_by_name(column.namespace, column.name))
    else {
        return Ok(());
    };
    let Some(table_before) = ctx.schema.get_btree_table(column.namespace, column.name) else {
        return Ok(());
    };
    let remaining = column.table.columns();
    let position = table_before
        .columns()
        .iter()
        .zip(remaining)
        .position(|(before, after)| before.name != after.name)
        .unwrap_or(remaining.len());
    let column_number = position as i64 + 1;
    emit_delete_comments(
        program,
        ctx.schema,
        class.oid,
        Dropped::Column(column_number),
    )
}

/// Stores the new name of column `old_name` of `table` in the class rows
/// of the sequences that the column owns. A column that keeps its name owns
/// the same sequences as before.
fn rename_owner_column(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    namespace: NamespaceId,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> turso_core::Result<()> {
    if old_name == new_name {
        return Ok(());
    }
    if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
        rows.renamed_owner_column(program, namespace, table, old_name, new_name);
    }
    Ok(())
}

/// Writes the class rows that one statement changes, with cursors that it
/// opens once.
struct ClassRowWriter<'a> {
    schema: &'a Schema,
    connection: &'a Arc<Connection>,
    cursors: ClassTableCursors,
}

impl<'a> ClassRowWriter<'a> {
    /// Opens the class table for the statement of `ctx`. None when the
    /// relations of the statement have no class rows, see
    /// [`writes_class_rows`], or the database has no class table.
    /// `allocates_oids` says that the statement adds a class row.
    fn open(
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'a>,
        allocates_oids: bool,
    ) -> turso_core::Result<Option<Self>> {
        if !writes_class_rows(ctx)? {
            return Ok(None);
        }
        let cursors = open_class_table_for_writing(program, ctx.schema, allocates_oids)?;
        Ok(cursors.map(|cursors| Self {
            schema: ctx.schema,
            connection: ctx.connection,
            cursors,
        }))
    }

    /// Inserts the class row of a relation that the statement creates, and
    /// returns the register that holds its oid, or None for an engine
    /// relation, which has no row. `name` is the relation's PostgreSQL name
    /// and `engine_name` the name its object has in the schema.
    /// `table` is the table of an index, in the same namespace: its owner
    /// owns the index. Anything else is owned by the current role, which is
    /// what PostgreSQL's `GetUserId()` gives after `SET ROLE`.
    fn new_relation(
        &self,
        program: &mut ProgramBuilder,
        new: NewRelation<'_>,
    ) -> turso_core::Result<Option<usize>> {
        // Relation names are unique per schema across kinds.
        if is_user_relation_name(new.kind, new.name)
            && class_catalog(self.schema)
                .is_some_and(|catalog| catalog.name_is_taken(new.namespace, new.name))
        {
            return Err(sqlstate_error(
                SQLSTATE_DUPLICATE_TABLE,
                format!("relation \"{}\" already exists", new.name),
                None,
            ));
        }
        let owner = new
            .table
            .and_then(|table| relation_owner(self.schema, new.namespace, table))
            .unwrap_or_else(|| self.connection.current_role());
        Ok(emit_class_row_insert(program, &self.cursors, new, owner))
    }

    /// The oid of the class row of the relation that the schema calls
    /// `name`, 0 when it has none.
    fn oid_of(&self, namespace: NamespaceId, name: &str) -> i64 {
        class_catalog(self.schema)
            .and_then(|catalog| catalog.get_by_name(namespace, name))
            .map_or(0, |class| class.oid)
    }

    /// Deletes the class row of a relation that the statement drops.
    fn dropped_relation(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        name: &str,
    ) -> turso_core::Result<()> {
        // An engine relation has no row and nothing to do.
        let Some(class) =
            class_catalog(self.schema).and_then(|catalog| catalog.get_by_name(namespace, name))
        else {
            return Ok(());
        };
        emit_delete_comments(program, self.schema, class.oid, Dropped::Relation)?;
        emit_class_row_delete(program, &self.cursors, namespace, name);
        Ok(())
    }

    /// Deletes the class row of the primary key index of the rowid table
    /// with oid `table_oid`, which the schema has no object for.
    fn dropped_rowid_primary_key_index(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        table_oid: i64,
    ) -> turso_core::Result<()> {
        let Some(class) = class_catalog(self.schema).and_then(|catalog| {
            catalog
                .indexes_of(table_oid)
                .find(|class| class.engine_name.is_none())
        }) else {
            return Ok(());
        };
        emit_delete_comments(program, self.schema, class.oid, Dropped::Relation)?;
        emit_class_row_delete_rowid_primary_key_index(program, &self.cursors, namespace, table_oid);
        Ok(())
    }

    /// Stores the new name of a relation. Its row keeps its oid and owner.
    fn renamed_relation(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        old_name: &str,
        new_name: &str,
    ) -> turso_core::Result<()> {
        let has_row = class_catalog(self.schema)
            .is_some_and(|catalog| catalog.get_by_name(namespace, old_name).is_some());
        if has_row {
            if class_catalog(self.schema)
                .is_some_and(|catalog| catalog.name_is_taken(namespace, new_name))
            {
                return Err(sqlstate_error(
                    SQLSTATE_DUPLICATE_TABLE,
                    format!("relation \"{new_name}\" already exists"),
                    None,
                ));
            }
            emit_class_row_rename(program, &self.cursors, namespace, old_name, new_name);
        }
        Ok(())
    }

    /// Stores the new name that the schema gives the index of a constraint
    /// when its table is renamed. PostgreSQL keeps the index's own name.
    fn renamed_engine_name(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        old_name: &str,
        new_name: &str,
    ) {
        let has_row = class_catalog(self.schema)
            .is_some_and(|catalog| catalog.get_by_name(namespace, old_name).is_some());
        if has_row {
            emit_class_row_rename_engine_name(
                program,
                &self.cursors,
                namespace,
                old_name,
                new_name,
            );
        }
    }

    /// Stores the new name of column `old_name` of `table` in the class
    /// rows of the sequences that the column owns.
    fn renamed_owner_column(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        table: &str,
        old_name: &str,
        new_name: &str,
    ) {
        let owned: Vec<(String, i64)> = class_catalog(self.schema)
            .and_then(|catalog| {
                let table = catalog.get_by_name(namespace, table)?;
                Some(
                    catalog
                        .owned_by(table.oid)
                        .filter(|owned| owned.is_owned_by_column(old_name))
                        .map(|owned| (owned.relname.clone(), table.oid))
                        .collect(),
                )
            })
            .unwrap_or_default();
        if owned.is_empty() {
            return;
        }
        for (sequence, table_oid) in owned {
            emit_class_row_set_owned_by(
                program,
                &self.cursors,
                namespace,
                &sequence,
                table_oid,
                Some(new_name),
            );
        }
        OwnerColumnRenamed {
            namespace,
            table: table.to_string(),
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
        }
        .emit(program);
    }
}

/// Whether the relations of the statement get class rows. Only the
/// relations a user creates in the main database do. A statement of the
/// engine copies or replays a database whose class rows are stored as data
/// already, and the temp database has no class table. Fails for a user's
/// statement against an attached database: the class table covers only the
/// main database, so a database file with relations made while it was
/// attached could not be opened later.
fn writes_class_rows(ctx: &CmdContext<'_>) -> turso_core::Result<bool> {
    if ctx.origin == CmdOrigin::Engine {
        return Ok(false);
    }
    if turso_core::is_attached_db(ctx.database_id) {
        return Err(not_supported(
            "creating, renaming or dropping relations in an attached database",
        ));
    }
    Ok(ctx.database_id == MAIN_DB_ID)
}

/// Emits the bytecode of the [`AccessStatement`]s that change a class row.
/// `ALTER ... OWNER TO` sets the owner, with PostgreSQL's errors for a
/// missing role (42704) and a catalog relation (42501). A sequence a serial column owns changes owner with its table
/// only, as in PostgreSQL: on its own it fails with `cannot change owner of
/// sequence` (0A000). `GRANT` adds to the access control list when the owner
/// or a superuser runs it; anyone else grants nothing, as in PostgreSQL,
/// which only warns. `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` sets
/// `relrowsecurity` of the table's row, which a catalog table does not have
/// (42501).
pub(crate) fn translate_access_statement(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    statement: &AccessStatement<'_>,
) -> turso_core::Result<()> {
    let allocates_oids = matches!(statement, AccessStatement::CreatePolicy { .. });
    let Some(cursors) = open_class_table_for_writing(program, schema, allocates_oids)? else {
        return match statement {
            AccessStatement::ChangeOwner {
                owner: NewOwner::Column(_),
                ..
            } => Ok(()),
            AccessStatement::Grant { relations: [], .. } => Ok(()),
            _ => Err(sqlstate_error(
                SQLSTATE_READ_ONLY_SQL_TRANSACTION,
                "the database has no relation catalog",
                None,
            )),
        };
    };
    match statement {
        AccessStatement::ChangeOwner {
            namespace,
            name,
            owner: NewOwner::Column(owner),
        } => {
            let table_oid = match owner {
                Some(owner) => class_catalog(schema)
                    .and_then(|catalog| catalog.get_by_name(*namespace, &owner.table))
                    .map(|class| class.oid)
                    .ok_or_else(|| {
                        sqlstate_error(
                            SQLSTATE_UNDEFINED_TABLE,
                            format!("relation \"{}\" does not exist", owner.table),
                            None,
                        )
                    })?,
                None => 0,
            };
            emit_class_row_set_owned_by(
                program,
                &cursors,
                *namespace,
                name,
                table_oid,
                owner.as_ref().map(|owner| owner.column.as_str()),
            );
            OwnerColumnSet {
                namespace: *namespace,
                name: name.to_string(),
                owner: owner.clone(),
            }
            .emit(program);
        }
        AccessStatement::ChangeOwner {
            namespace,
            name,
            owner: NewOwner::Role(new_owner),
        } => {
            let owner = resolve_new_owner(schema, connection, *namespace, name, new_owner)?;
            let Some(class) =
                class_catalog(schema).and_then(|catalog| catalog.get_by_name(*namespace, name))
            else {
                return Ok(());
            };
            // As in PostgreSQL: nothing changes when the owner stays, an
            // index's owner is its table's, and a serial column's
            // sequence changes owner with its table only.
            if class.relowner == owner || class.relkind == RelationKind::Index {
                return Ok(());
            }
            if class.relownedby != 0 {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!("cannot change owner of sequence \"{name}\""),
                    None,
                ));
            }
            // A table's indexes and what it owns, such as the sequences
            // of its serial columns, go with it.
            let catalog = class_catalog(schema).expect("the relation has a class catalog");
            let with_it = std::iter::once(class)
                .chain(catalog.owned_by(class.oid))
                .chain(catalog.indexes_of(class.oid));
            for other in with_it {
                // The new owner takes the old one's place in the list.
                let acl = other.relacl.clone().map(|mut acl| {
                    acl.change_owner(role_oid(other.relowner), role_oid(owner));
                    acl
                });
                emit_class_row_set_owner(program, &cursors, other.oid, owner, acl.as_ref());
            }
        }
        AccessStatement::Grant {
            revoke,
            relations,
            privileges,
            grantees: statement_grantees,
        } => {
            let statement = if *revoke { "REVOKE" } else { "GRANT" };
            let grantees = statement_grantees
                .iter()
                .map(|grantee| resolve_grantee(schema, connection, grantee))
                .collect::<turso_core::Result<Vec<_>>>()?;
            for RelationName { namespace, name } in *relations {
                // A relation without a class row has no list to store the
                // privileges in. Only a superuser uses the engine's own
                // tables, so a GRANT or REVOKE on one would claim a change it
                // does not make, and is refused. Every role reads the catalog
                // relations, such as pg_class, so a GRANT on one changes nothing,
                // but a REVOKE would leave every role reading it, and is
                // refused too.
                let Some(class) = class_catalog(schema)
                    .and_then(|catalog| catalog.get_by_name(*namespace, name))
                    .filter(|class| !class.relnamespace.is_root())
                else {
                    // Public catalogs already grant SELECT to every role,
                    // whether their rows are stored or virtual.
                    if namespace.is_root()
                        && (name.eq_ignore_ascii_case("pg_description")
                            || name.eq_ignore_ascii_case("pg_class"))
                    {
                        if !revoke && privileges.iter().all(|p| *p == ast::Privilege::Select) {
                            continue;
                        }
                        return Err(not_supported(format!(
                            "{statement} on system catalog {name}"
                        )));
                    }
                    if schema.get_btree_table(*namespace, name).is_some() {
                        return Err(not_supported(format!(
                            "{statement} on internal table {name}"
                        )));
                    }
                    if *revoke {
                        return Err(not_supported(format!(
                            "REVOKE on system catalog {}",
                            catalog_view_name(name)
                        )));
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
                        None,
                    ));
                }
                let owner = role_oid(class.relowner);
                let mut acl = class
                    .relacl
                    .clone()
                    .unwrap_or_else(|| Acl::default_for(class.relkind, owner));
                // Only the owner may grant or revoke, and a superuser or a
                // role with the owner's privileges does so as the owner. As
                // in PostgreSQL, anyone else is refused when the list gives
                // it nothing on the relation, and changes nothing otherwise,
                // for want of a grant option.
                let roles = schema_roles(schema);
                let current = connection.current_role();
                if !roles.has_privileges_of(current, class.relowner) {
                    if acl.gives_anything_to(&roles.oids_of_roles_used_by(current)) {
                        continue;
                    }
                    return Err(LimboError::PermissionDenied(Box::new(
                        PermissionDenied::privilege(Action::Read, relation_object(class)),
                    )));
                }
                // As in PostgreSQL, a statement that names no privilege of
                // the relation, such as INSERT on a sequence, leaves its list
                // as it is, even when it has none yet. Otherwise a relation
                // without a list gets the owner's default one first, so a
                // REVOKE that takes nothing away still stores it.
                let named = Privileges::named_on(class.relkind, privileges);
                if named.is_empty() {
                    continue;
                }
                for grantee in &grantees {
                    if *revoke {
                        acl.revoke(*grantee, named, owner);
                    } else {
                        acl.grant(*grantee, named, owner);
                    }
                }
                if class.relacl.as_ref() == Some(&acl) {
                    continue;
                }
                emit_class_row_set_acl(program, &cursors, *namespace, name, &acl);
            }
        }
        AccessStatement::SetRowSecurity {
            namespace,
            name,
            enabled,
        } => {
            let class = user_relation_class(schema, *namespace, name)?;
            if class.relrowsecurity == *enabled {
                return Ok(());
            }
            emit_class_row_set_row_security(program, &cursors, *namespace, name, *enabled);
        }
        AccessStatement::CreatePolicy {
            namespace,
            name,
            policy,
        } => {
            let class = user_relation_class(schema, *namespace, name)?;
            let policy_name = policy.policy_name.as_str();
            if class.relpolicies.contains(policy_name) {
                return Err(sqlstate_error(
                    SQLSTATE_DUPLICATE_OBJECT,
                    format!("policy \"{policy_name}\" for table \"{name}\" already exists"),
                    None,
                ));
            }
            let roles = policy
                .roles
                .iter()
                .map(|role| resolve_policy_role(schema, connection, role))
                .collect::<turso_core::Result<Vec<_>>>()?;
            let (before_oid, after_oid) = class.relpolicies.stored_text_around_new_oid(
                RowPolicy {
                    name: policy_name.to_string(),
                    permissive: policy.permissive,
                    command: policy.command,
                    roles,
                    using_expr: policy.using_expr.as_deref().cloned(),
                    check_expr: policy.check_expr.as_deref().cloned(),
                },
                policy.using_sql.clone(),
                policy.check_sql.clone(),
            );
            // The catalog reads the conditions back from the stored form,
            // so a condition that does not read back is refused here
            // rather than leaving a database that cannot be opened.
            let some_oid = FIRST_NORMAL_OBJECT_ID;
            if policies::parse(&format!("{before_oid}{some_oid}{after_oid}")).is_none() {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!("the conditions of policy \"{policy_name}\" cannot be stored"),
                    None,
                ));
            }
            emit_class_row_set_policies(
                program,
                &cursors,
                *namespace,
                name,
                StoredPolicies::WithNewOid {
                    before_oid,
                    after_oid,
                },
            );
        }
        AccessStatement::DropPolicy {
            namespace,
            name,
            policy_name,
            if_exists,
        } => {
            let class = class_catalog(schema)
                .and_then(|catalog| catalog.get_by_name(*namespace, name))
                .ok_or_else(|| system_catalog_permission_denied(name))?;
            if !class.relpolicies.contains(policy_name) {
                if *if_exists {
                    return Ok(());
                }
                return Err(sqlstate_error(
                    SQLSTATE_UNDEFINED_OBJECT,
                    format!("policy \"{policy_name}\" for table \"{name}\" does not exist"),
                    None,
                ));
            }
            user_relation_class(schema, *namespace, name)?;
            let relpolicies = class.relpolicies.without(policy_name);
            let stored = if relpolicies.is_empty() {
                StoredPolicies::None
            } else {
                StoredPolicies::Text(relpolicies.stored_text())
            };
            emit_class_row_set_policies(program, &cursors, *namespace, name, stored);
        }
        AccessStatement::CreateRole { .. }
        | AccessStatement::DropRole { .. }
        | AccessStatement::GrantRole { .. } => {
            unreachable!("statements about roles do not change a class row")
        }
    }
    Ok(())
}

/// The name a catalog view has in PostgreSQL: the engine names
/// `information_schema.schemata` `pg_information_schema_schemata`.
fn catalog_view_name(name: &str) -> String {
    match name.strip_prefix("pg_information_schema_") {
        Some(view) => format!("information_schema.{view}"),
        None => name.to_string(),
    }
}

/// The class row of a user relation that access-control statements may change.
fn user_relation_class<'a>(
    schema: &'a Schema,
    namespace: NamespaceId,
    name: &str,
) -> turso_core::Result<&'a RelationClass> {
    class_catalog(schema)
        .and_then(|catalog| catalog.get_by_name(namespace, name))
        .filter(|class| !class.relnamespace.is_root())
        .ok_or_else(|| system_catalog_permission_denied(name))
}

/// PostgreSQL's error for a statement that changes system catalog `name`.
fn system_catalog_permission_denied(name: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_INSUFFICIENT_PRIVILEGE,
        format!("permission denied: \"{name}\" is a system catalog"),
        None,
    )
}

/// The role a policy's TO clause names.
fn resolve_policy_role(
    schema: &Schema,
    connection: &Arc<Connection>,
    role: &ast::Grantee,
) -> turso_core::Result<RoleId> {
    match role {
        ast::Grantee::Public => Ok(RoleId::PUBLIC),
        ast::Grantee::Role(role) => resolve_role(schema, connection, role),
    }
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
        ast::Grantee::Role(ast::RoleSpec::Name(name)) => {
            role_oid_by_name(schema_roles(schema), name).ok_or_else(|| role_does_not_exist(name))?
        }
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
    user_relation_class(schema, namespace, name)?;
    resolve_role(schema, connection, new_owner)
}

/// The role that `role` names. A named role must exist.
fn resolve_role(
    schema: &Schema,
    connection: &Arc<Connection>,
    role: &ast::RoleSpec,
) -> turso_core::Result<RoleId> {
    Ok(match role {
        ast::RoleSpec::CurrentRole => connection.current_role(),
        ast::RoleSpec::SessionUser => connection.session_role(),
        ast::RoleSpec::Name(name) => engine_role_by_name(schema_roles(schema), name)
            .ok_or_else(|| role_does_not_exist(name))?,
    })
}

/// PostgreSQL's error for a role name that names no role.
/// The cursors of one statement's class row writes: the classes table, its
/// name index, and `sqlite_sequence`, which keeps the highest oid ever used
/// under the classes table's own name.
struct ClassTableCursors {
    table_cursor_id: usize,
    index_cursor_id: usize,
    index_name: String,
    /// Only opened when the statement inserts a row, which allocates an oid.
    seq_cursor_id: Option<usize>,
    seq_table: Option<Arc<BTreeTable>>,
}

/// Opens the classes table of the main database for writing, or returns None
/// when the database has no catalog, which only happens outside the
/// frontend's own open path, such as a core-level open for tests.
/// `allocates_oids` also opens `sqlite_sequence`, which inserting a class
/// row updates.
fn open_class_table_for_writing(
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
        seq_cursor_id,
        seq_table,
    }))
}

/// Inserts the class row of a new relation under the next oid, owned by
/// `owner`, and returns the register of the oid. Engine relations get no
/// row, and no register.
fn emit_class_row_insert(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    new: NewRelation<'_>,
    owner: RoleId,
) -> Option<usize> {
    if !is_user_relation_name(new.kind, new.engine_name.unwrap_or(new.name)) {
        return None;
    }
    let oid_reg = emit_allocate_oid(program, cursors);

    emit_class_name_index_insert(program, cursors, new.namespace, new.name, oid_reg);

    let first_column_reg = program.alloc_registers(CLASS_COLUMN_COUNT);
    for (column, value) in new_class_values(&new, owner).into_iter().enumerate() {
        emit_class_value(program, value, first_column_reg + column);
    }
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

    let engine_owner_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: owner.get(),
        dest: engine_owner_reg,
    });
    ClassCreated {
        namespace: new.namespace,
        name: new.name.to_string(),
        engine_name: new.engine_name.map(str::to_string),
        kind: new.kind,
    }
    .emit(
        program,
        oid_reg,
        engine_owner_reg,
        first_column_reg + COLUMN_RELINDRELID,
    );
    Some(oid_reg)
}

/// A value written into a column of a new class row.
#[derive(Clone)]
enum ClassValue {
    Null,
    Int(i64),
    Text(String),
    Register(usize),
}

/// The columns of a new class row, in table order. The oid is the rowid, so
/// the record keeps NULL for it, as SQLite does for an INTEGER PRIMARY KEY
/// column. No GRANT has touched a new relation, so `relacl` is NULL, and it
/// has no row-level security and no policies.
fn new_class_values(new: &NewRelation<'_>, owner: RoleId) -> Vec<ClassValue> {
    let mut values = vec![ClassValue::Null; CLASS_COLUMN_COUNT];
    let mut set = |column: &str, value: ClassValue| values[column_position(column)] = value;
    set("relname", ClassValue::Text(new.name.to_string()));
    set(
        "relenginenamespace",
        ClassValue::Int(i64::from(new.namespace.as_u32())),
    );
    set(
        "relkind",
        ClassValue::Text(relkind_letter(new.kind).to_string()),
    );
    set("relowner", ClassValue::Int(role_oid(owner)));
    set("relownedby", ClassValue::Int(0));
    set("relrowsecurity", ClassValue::Int(0));
    set(
        "relenginename",
        new.engine_name
            .map_or(ClassValue::Null, |name| ClassValue::Text(name.to_string())),
    );
    set(
        "relindrelid",
        match new.indexed_table {
            IndexedTable::None => ClassValue::Int(0),
            IndexedTable::Oid(oid) => ClassValue::Int(oid),
            IndexedTable::Register(register) => ClassValue::Register(register),
        },
    );
    values
}

fn emit_class_value(program: &mut ProgramBuilder, value: ClassValue, dest: usize) {
    match value {
        ClassValue::Null => program.emit_insn(Insn::Null {
            dest,
            dest_end: None,
        }),
        ClassValue::Int(value) => program.emit_insn(Insn::Integer { value, dest }),
        ClassValue::Text(value) => program.emit_insn(Insn::String8 { value, dest }),
        ClassValue::Register(src_reg) => program.emit_insn(Insn::Copy {
            src_reg,
            dst_reg: dest,
            extra_amount: 0,
        }),
    }
}

/// Puts the next oid in the returned register and records it as the
/// highest oid ever used.
fn emit_allocate_oid(program: &mut ProgramBuilder, cursors: &ClassTableCursors) -> usize {
    let seq_cursor_id = cursors
        .seq_cursor_id
        .expect("the caller opened the cursors with allocates_oids");

    // oid = max(largest oid in the table + 1, highest oid ever used + 1,
    // FIRST_NORMAL_OBJECT_ID), so an oid is never reused even after the
    // relation or policy with the largest oid is dropped.
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
    oid_reg
}

/// Makes column `column` of the table with oid `table_oid` the owner of
/// relation `name` of `namespace`: its `relownedby` and `relownedbycolumn`.
/// 0 and None make the relation nobody's. The caller tells the in-memory
/// catalog.
fn emit_class_row_set_owned_by(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    table_oid: i64,
    column: Option<&str>,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            row,
            vec![
                (COLUMN_RELOWNEDBY, ClassRecordColumn::LiteralInt(table_oid)),
                (
                    COLUMN_RELOWNEDBYCOLUMN,
                    match column {
                        Some(column) => ClassRecordColumn::Literal(column.to_string()),
                        None => ClassRecordColumn::Null,
                    },
                ),
            ],
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
}

/// Deletes the class row of the relation that the schema calls `name` in
/// `namespace`.
fn emit_class_row_delete(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        emit_class_row_delete_found(program, cursors, namespace, row);
    });
    ClassDropped::EngineName {
        namespace,
        name: name.to_string(),
    }
    .emit(program);
}

/// Deletes the class row of the primary key index of the rowid table with
/// oid `table_oid`.
fn emit_class_row_delete_rowid_primary_key_index(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    table_oid: i64,
) {
    emit_find_class_row_by(
        program,
        cursors,
        ClassKey::RowidPrimaryKeyIndexOf(table_oid),
        |program, row| emit_class_row_delete_found(program, cursors, namespace, row),
    );
    ClassDropped::RowidPrimaryKeyIndexOf { table_oid }.emit(program);
}

fn emit_class_row_delete_found(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    row: &FoundClassRow,
) {
    emit_class_name_index_delete(
        program,
        cursors,
        namespace,
        row.first_column_reg + COLUMN_RELNAME,
        row.rowid_reg,
    );
    // Marked as part of an update so that changes() does not count it.
    program.emit_insn(Insn::Delete {
        cursor_id: cursors.table_cursor_id,
        table_name: CLASSES_TABLE_NAME.to_string(),
        is_part_of_update: true,
    });
}

/// Renames the class row of relation `old_name` of `namespace` to
/// `new_name`, in PostgreSQL and in the schema, keeping its oid and owner.
fn emit_class_row_rename(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    old_name: &str,
    new_name: &str,
) {
    emit_find_class_row(program, cursors, namespace, old_name, |program, row| {
        emit_class_name_index_delete(
            program,
            cursors,
            namespace,
            row.first_column_reg + COLUMN_RELNAME,
            row.rowid_reg,
        );
        emit_class_name_index_insert(program, cursors, namespace, new_name, row.rowid_reg);
        let record_reg = emit_class_record(
            program,
            row,
            vec![
                (
                    COLUMN_RELNAME,
                    ClassRecordColumn::Literal(new_name.to_string()),
                ),
                (
                    COLUMN_RELENGINENAME,
                    ClassRecordColumn::Literal(new_name.to_ascii_lowercase()),
                ),
            ],
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    ClassRenamed {
        namespace,
        old_name: old_name.to_string(),
        new_name: new_name.to_string(),
    }
    .emit(program);
}

/// Changes the name that the schema gives the relation `old_name` of
/// `namespace` to `new_name`. The relation keeps its PostgreSQL name.
fn emit_class_row_rename_engine_name(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    old_name: &str,
    new_name: &str,
) {
    emit_find_class_row(program, cursors, namespace, old_name, |program, row| {
        let record_reg = emit_class_record(
            program,
            row,
            vec![(
                COLUMN_RELENGINENAME,
                ClassRecordColumn::Literal(new_name.to_string()),
            )],
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    ClassEngineNameChanged {
        namespace,
        old_name: old_name.to_string(),
        new_name: new_name.to_string(),
    }
    .emit(program);
}

/// Sets the owner of the class row of relation `name` of `namespace` to
/// `owner`, and its access control list to `acl`.
fn emit_class_row_set_owner(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    oid: i64,
    owner: RoleId,
    acl: Option<&Acl>,
) {
    let owner_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: owner.get(),
        dest: owner_reg,
    });
    let acl_reg = emit_acl(program, acl);
    emit_find_class_row_by(program, cursors, ClassKey::Oid(oid), |program, row| {
        let record_reg = emit_class_record(
            program,
            row,
            vec![
                (
                    COLUMN_RELOWNER,
                    ClassRecordColumn::LiteralInt(role_oid(owner)),
                ),
                (COLUMN_RELPRIVILEGES, ClassRecordColumn::Register(acl_reg)),
            ],
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    OwnerSet { oid }.emit(program, owner_reg, acl_reg);
}

/// Sets the access control list of the class row of relation `name` of
/// `namespace` to `acl`.
fn emit_class_row_set_acl(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    acl: &Acl,
) {
    let acl_reg = emit_acl(program, Some(acl));
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            row,
            vec![(COLUMN_RELPRIVILEGES, ClassRecordColumn::Register(acl_reg))],
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    AclSet {
        namespace,
        name: name.to_string(),
    }
    .emit(program, acl_reg);
}

/// Sets `relrowsecurity` of the class row of table `name` of `namespace` to
/// `enabled`.
fn emit_class_row_set_row_security(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    enabled: bool,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            row,
            vec![(
                COLUMN_RELROWSECURITY,
                ClassRecordColumn::LiteralInt(i64::from(enabled)),
            )],
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    RowSecuritySet {
        namespace,
        name: name.to_string(),
        enabled,
    }
    .emit(program);
}

/// What a statement stores in `relpolicies`.
enum StoredPolicies {
    /// NULL, for no policies.
    None,
    Text(String),
    /// The stored form of policies, one of which is new and gets the next
    /// oid, which goes between the two parts.
    WithNewOid {
        before_oid: String,
        after_oid: String,
    },
}

/// Sets `relpolicies` of the class row of table `name` of `namespace` to
/// `stored`.
fn emit_class_row_set_policies(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    stored: StoredPolicies,
) {
    let policies_reg = program.alloc_register();
    match stored {
        StoredPolicies::None => {
            program.emit_insn(Insn::Null {
                dest: policies_reg,
                dest_end: None,
            });
        }
        StoredPolicies::Text(text) => {
            program.emit_insn(Insn::String8 {
                value: text,
                dest: policies_reg,
            });
        }
        StoredPolicies::WithNewOid {
            before_oid,
            after_oid,
        } => {
            let oid_reg = emit_allocate_oid(program, cursors);
            program.emit_insn(Insn::String8 {
                value: before_oid,
                dest: policies_reg,
            });
            program.emit_insn(Insn::Concat {
                lhs: policies_reg,
                rhs: oid_reg,
                dest: policies_reg,
            });
            let after_oid_reg = program.emit_string8_new_reg(after_oid);
            program.emit_insn(Insn::Concat {
                lhs: policies_reg,
                rhs: after_oid_reg,
                dest: policies_reg,
            });
        }
    }
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            row,
            vec![(
                COLUMN_RELPOLICIES,
                ClassRecordColumn::Register(policies_reg),
            )],
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    PoliciesSet {
        namespace,
        name: name.to_string(),
    }
    .emit(program, policies_reg);
}

/// A class row that the statement inserted.
#[derive(Debug)]
struct ClassCreated {
    namespace: NamespaceId,
    name: String,
    engine_name: Option<String>,
    kind: RelationKind,
}

impl ClassCreated {
    /// `oid_reg` holds the oid the row was stored under, `owner_reg` its
    /// `relowner` and `indexed_table_reg` its `relindrelid`.
    fn emit(
        self,
        program: &mut ProgramBuilder,
        oid_reg: usize,
        owner_reg: usize,
        indexed_table_reg: usize,
    ) {
        emit_catalog_apply(program, self, vec![oid_reg, owner_reg, indexed_table_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [oid, owner, indexed_table] = values else {
            return Err(LimboError::InternalError(
                "a created relation carries its oid, owner and indexed table".to_string(),
            ));
        };
        classes.add(RelationClass {
            oid: read_back_integer(oid)?,
            relname: self.name.clone(),
            engine_name: self.engine_name.clone(),
            relindrelid: read_back_integer(indexed_table)?,
            relnamespace: self.namespace,
            relkind: self.kind,
            relowner: RoleId::from_stored(read_back_integer(owner)?),
            relownedby: 0,
            relownedbycolumn: None,
            relacl: None,
            relrowsecurity: false,
            relpolicies: TablePolicies::default(),
        });
        Ok(())
    }
}

/// A class row that the statement deleted.
#[derive(Debug)]
enum ClassDropped {
    EngineName {
        namespace: NamespaceId,
        name: String,
    },
    RowidPrimaryKeyIndexOf {
        table_oid: i64,
    },
}

impl ClassDropped {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        match self {
            Self::EngineName { namespace, name } => {
                classes.remove(*namespace, name);
            }
            Self::RowidPrimaryKeyIndexOf { table_oid } => {
                let oid = classes
                    .indexes_of(*table_oid)
                    .find(|class| class.engine_name.is_none())
                    .map(|class| class.oid);
                if let Some(oid) = oid {
                    classes.remove_by_oid(oid);
                }
            }
        }
        Ok(())
    }
}

/// A class row whose `relname` and engine name the statement changed.
#[derive(Debug)]
struct ClassRenamed {
    namespace: NamespaceId,
    old_name: String,
    new_name: String,
}

impl ClassRenamed {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        if let Some(mut class) = classes.remove(self.namespace, &self.old_name) {
            class.relname.clone_from(&self.new_name);
            class.engine_name = Some(self.new_name.to_ascii_lowercase());
            classes.add(class);
        }
        Ok(())
    }
}

/// A class row whose engine name the statement changed, and whose `relname`
/// stays.
#[derive(Debug)]
struct ClassEngineNameChanged {
    namespace: NamespaceId,
    old_name: String,
    new_name: String,
}

impl ClassEngineNameChanged {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        if let Some(mut class) = classes.remove(self.namespace, &self.old_name) {
            class.engine_name = Some(self.new_name.clone());
            classes.add(class);
        }
        Ok(())
    }
}

/// A class row whose `relownedby` and `relownedbycolumn` the statement set
/// to `owner`, or to no column.
#[derive(Debug)]
struct OwnerColumnSet {
    namespace: NamespaceId,
    name: String,
    owner: Option<OwnerColumn>,
}

impl OwnerColumnSet {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        let table_oid = self
            .owner
            .as_ref()
            .and_then(|owner| classes.get_by_name(self.namespace, &owner.table))
            .map_or(0, |table| table.oid);
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relownedby = table_oid;
            class.relownedbycolumn = self.owner.as_ref().map(|owner| owner.column.clone());
        }
        Ok(())
    }
}

/// The class rows that column `old_name` of `table` owned, whose
/// `relownedbycolumn` the statement set to `new_name`.
#[derive(Debug)]
struct OwnerColumnRenamed {
    namespace: NamespaceId,
    table: String,
    old_name: String,
    new_name: String,
}

impl OwnerColumnRenamed {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        let Some(table_oid) = classes
            .get_by_name(self.namespace, &self.table)
            .map(|table| table.oid)
        else {
            return Ok(());
        };
        for class in classes.by_oid.values_mut() {
            if class.relownedby == table_oid && class.is_owned_by_column(&self.old_name) {
                class.relownedbycolumn = Some(self.new_name.clone());
            }
        }
        Ok(())
    }
}

/// A class row whose `relowner` and `relacl` the statement set.
#[derive(Debug)]
struct OwnerSet {
    oid: i64,
}

impl OwnerSet {
    /// `owner_reg` holds the new `relowner` and `acl_reg` the new `relacl`.
    fn emit(self, program: &mut ProgramBuilder, owner_reg: usize, acl_reg: usize) {
        emit_catalog_apply(program, self, vec![owner_reg, acl_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [owner, acl] = values else {
            return Err(LimboError::InternalError(
                "an owner change carries the new owner and the list it leaves".to_string(),
            ));
        };
        if let Some(class) = classes.by_oid.get_mut(&self.oid) {
            class.relowner = RoleId::from_stored(read_back_integer(owner)?);
            class.relacl = read_back_acl(acl)?;
        }
        Ok(())
    }
}

/// A class row whose `relacl` the statement set.
#[derive(Debug)]
struct AclSet {
    namespace: NamespaceId,
    name: String,
}

impl AclSet {
    /// `acl_reg` holds the new `relacl`.
    fn emit(self, program: &mut ProgramBuilder, acl_reg: usize) {
        emit_catalog_apply(program, self, vec![acl_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [acl] = values else {
            return Err(LimboError::InternalError(
                "a grant carries the list it leaves".to_string(),
            ));
        };
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relacl = read_back_acl(acl)?;
        }
        Ok(())
    }
}

/// A class row whose `relrowsecurity` the statement set to `enabled`.
#[derive(Debug)]
struct RowSecuritySet {
    namespace: NamespaceId,
    name: String,
    enabled: bool,
}

impl RowSecuritySet {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relrowsecurity = self.enabled;
        }
        Ok(())
    }
}

/// A class row whose `relpolicies` the statement set.
#[derive(Debug)]
struct PoliciesSet {
    namespace: NamespaceId,
    name: String,
}

impl PoliciesSet {
    /// `policies_reg` holds the new `relpolicies`.
    fn emit(self, program: &mut ProgramBuilder, policies_reg: usize) {
        emit_catalog_apply(program, self, vec![policies_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [relpolicies] = values else {
            return Err(LimboError::InternalError(
                "a policy change carries the policies it leaves".to_string(),
            ));
        };
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relpolicies = read_back_policies(relpolicies)?;
        }
        Ok(())
    }
}

macro_rules! apply_to_class_catalog {
    ($($change:ty),* $(,)?) => {
        $(
            impl CatalogApply for $change {
                fn apply(&self, schema: &mut Schema, values: &[Value]) -> turso_core::Result<()> {
                    change_schema_state(schema, |state| {
                        self.apply_to(Arc::make_mut(&mut state.classes), values)
                    })
                }
            }
        )*
    };
}

apply_to_class_catalog!(
    ClassCreated,
    ClassDropped,
    ClassRenamed,
    ClassEngineNameChanged,
    OwnerColumnSet,
    OwnerColumnRenamed,
    OwnerSet,
    AclSet,
    RowSecuritySet,
    PoliciesSet,
);

/// An integer the emitted instruction read back from a register.
fn read_back_integer(value: &Value) -> turso_core::Result<i64> {
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

fn read_back_policies(value: &Value) -> turso_core::Result<TablePolicies> {
    match value {
        Value::Null => Ok(TablePolicies::default()),
        Value::Text(text) => policies::parse(text.as_str()).ok_or_else(|| {
            LimboError::InternalError(format!(
                "the relation change read back invalid policies {text:?}"
            ))
        }),
        other => Err(LimboError::InternalError(format!(
            "the relation change read back {other:?} where policies were written"
        ))),
    }
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

/// The registers that [`emit_find_class_row`] fills for the found row: its
/// rowid, and its columns from `first_column_reg` on, in table order.
struct FoundClassRow {
    rowid_reg: usize,
    first_column_reg: usize,
}

/// Scans the classes table for the row of the relation that the schema calls
/// `name` in `namespace` and runs `found` with the cursor on it. Emits nothing
/// after the scan when the row does not exist, so a relation without a row is
/// tolerated.
fn emit_find_class_row(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    found: impl FnOnce(&mut ProgramBuilder, &FoundClassRow),
) {
    emit_find_class_row_by(
        program,
        cursors,
        ClassKey::EngineName(namespace, name),
        found,
    );
}

/// How a scan of the classes table recognizes its row.
#[derive(Clone, Copy)]
enum ClassKey<'a> {
    Oid(i64),
    /// The relation that the schema calls this in the namespace.
    EngineName(NamespaceId, &'a str),
    /// The primary key index of the rowid table with this oid, which has no
    /// name in the schema.
    RowidPrimaryKeyIndexOf(i64),
}

fn emit_find_class_row_by(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    key: ClassKey<'_>,
    found: impl FnOnce(&mut ProgramBuilder, &FoundClassRow),
) {
    let loop_start = program.allocate_label();
    let loop_end = program.allocate_label();
    let done = program.allocate_label();
    let next = program.allocate_label();
    let column_reg = program.alloc_register();
    match key {
        ClassKey::Oid(oid) => {
            program.emit_insn(Insn::Integer {
                value: oid,
                dest: column_reg,
            });
            program.emit_insn(Insn::SeekRowid {
                cursor_id: cursors.table_cursor_id,
                src_reg: column_reg,
                target_pc: loop_end,
            });
            program.preassign_label_to_next_insn(loop_start);
        }
        ClassKey::EngineName(namespace, name) => {
            let name_reg = program.emit_string8_new_reg(name.to_string());
            let namespace_reg = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: i64::from(namespace.as_u32()),
                dest: namespace_reg,
            });
            program.emit_insn(Insn::Rewind {
                cursor_id: cursors.table_cursor_id,
                pc_if_empty: loop_end,
            });
            program.preassign_label_to_next_insn(loop_start);
            program.emit_column_or_rowid(
                cursors.table_cursor_id,
                COLUMN_RELENGINENAMESPACE,
                column_reg,
            );
            program.emit_insn(Insn::Ne {
                lhs: column_reg,
                rhs: namespace_reg,
                target_pc: next,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: CollationSeq::for_internal_values(),
            });
            program.emit_column_or_rowid(cursors.table_cursor_id, COLUMN_RELENGINENAME, column_reg);
            program.emit_insn(Insn::Ne {
                lhs: column_reg,
                rhs: name_reg,
                target_pc: next,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: CollationSeq::NOCASE,
            });
        }
        ClassKey::RowidPrimaryKeyIndexOf(table_oid) => {
            let table_oid_reg = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: table_oid,
                dest: table_oid_reg,
            });
            program.emit_insn(Insn::Rewind {
                cursor_id: cursors.table_cursor_id,
                pc_if_empty: loop_end,
            });
            program.preassign_label_to_next_insn(loop_start);
            program.emit_column_or_rowid(cursors.table_cursor_id, COLUMN_RELINDRELID, column_reg);
            program.emit_insn(Insn::Ne {
                lhs: column_reg,
                rhs: table_oid_reg,
                target_pc: next,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: CollationSeq::for_internal_values(),
            });
            program.emit_column_or_rowid(cursors.table_cursor_id, COLUMN_RELENGINENAME, column_reg);
            program.emit_insn(Insn::NotNull {
                reg: column_reg,
                target_pc: next,
            });
        }
    }

    let row = FoundClassRow {
        rowid_reg: program.alloc_register(),
        first_column_reg: program.alloc_registers(CLASS_COLUMN_COUNT),
    };
    program.emit_insn(Insn::RowId {
        cursor_id: cursors.table_cursor_id,
        dest: row.rowid_reg,
    });
    for column in COLUMN_RELNAME..CLASS_COLUMN_COUNT {
        program.emit_column_or_rowid(
            cursors.table_cursor_id,
            column,
            row.first_column_reg + column,
        );
    }
    found(program, &row);
    program.emit_insn(Insn::Goto { target_pc: done });

    program.preassign_label_to_next_insn(next);
    if !matches!(key, ClassKey::Oid(_)) {
        program.emit_insn(Insn::Next {
            cursor_id: cursors.table_cursor_id,
            pc_if_next: loop_start,
            fullscan: false,
            is_index: false,
        });
    }
    program.preassign_label_to_next_insn(loop_end);
    program.preassign_label_to_next_insn(done);
}

/// What a column of a rewritten class record is built from.
enum ClassRecordColumn {
    Literal(String),
    LiteralInt(i64),
    Null,
    Register(usize),
}

/// Builds the record of the found class row with the columns of `changes`
/// replaced, and returns its register. The oid is the rowid, so the record
/// keeps NULL for it.
struct ClassRecord {
    record_reg: usize,
}

fn emit_class_record(
    program: &mut ProgramBuilder,
    row: &FoundClassRow,
    changes: Vec<(usize, ClassRecordColumn)>,
) -> ClassRecord {
    let first_column_reg = program.alloc_registers(CLASS_COLUMN_COUNT);
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    let mut changes: HashMap<usize, ClassRecordColumn> = changes.into_iter().collect();
    for column in COLUMN_RELNAME..CLASS_COLUMN_COUNT {
        let dest = first_column_reg + column;
        match changes.remove(&column) {
            None => {
                program.emit_insn(Insn::Copy {
                    src_reg: row.first_column_reg + column,
                    dst_reg: dest,
                    extra_amount: 0,
                });
            }
            Some(ClassRecordColumn::Literal(value)) => {
                program.emit_insn(Insn::String8 { value, dest });
            }
            Some(ClassRecordColumn::LiteralInt(value)) => {
                program.emit_insn(Insn::Integer { value, dest });
            }
            Some(ClassRecordColumn::Null) => {
                program.emit_insn(Insn::Null {
                    dest,
                    dest_end: None,
                });
            }
            Some(ClassRecordColumn::Register(src_reg)) => {
                program.emit_insn(Insn::Copy {
                    src_reg,
                    dst_reg: dest,
                    extra_amount: 0,
                });
            }
        }
    }
    assert!(
        changes.is_empty(),
        "a change names a column of no class row"
    );
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(CLASS_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    ClassRecord { record_reg }
}

/// Replaces the class row at `rowid_reg` with `record_reg`. The cursor is on
/// the row. The delete first ends the old version under MVCC, where an
/// update is a delete and an insert.
fn emit_class_row_overwrite(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    rowid_reg: usize,
    record: ClassRecord,
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
        record_reg: record.record_reg,
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
    emit_unique_index_insert(
        program,
        cursors.index_cursor_id,
        &cursors.index_name,
        key_reg,
        3,
        format!("relation \"{name}\" already has a class row"),
    );
}

/// Removes `(namespace, name, rowid)` from the name index, with the name in
/// register `name_reg`.
fn emit_class_name_index_delete(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name_reg: usize,
    rowid_reg: usize,
) {
    let key_reg = program.alloc_registers(3);
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: key_reg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: name_reg,
        dst_reg: key_reg + 1,
        extra_amount: 0,
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

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE_OID: i64 = FIRST_NORMAL_OBJECT_ID;
    const SEQUENCE_OID: i64 = FIRST_NORMAL_OBJECT_ID + 1;
    const ALICE: i64 = 7;
    const BOB: i64 = 8;

    #[test]
    fn class_created_adds_a_row_with_the_oid_and_owner_it_reads_back() {
        let classes = table_and_sequence();

        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.oid, TABLE_OID);
        assert_eq!(table.relkind, RelationKind::Table);
        assert_eq!(table.relowner, RoleId::from_stored(ALICE));
        assert_eq!(table.relownedby, 0);
        assert_eq!(table.relacl, None);
        assert!(!table.relrowsecurity);
    }

    #[test]
    fn class_created_refuses_values_that_are_not_an_oid_an_owner_and_a_table() {
        let created = ClassCreated {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
            engine_name: Some("t".to_string()),
            kind: RelationKind::Table,
        };
        let mut classes = ClassCatalog::default();

        let missing_owner = created.apply_to(&mut classes, &[Value::from_i64(TABLE_OID)]);
        let text_oid = created.apply_to(
            &mut classes,
            &[
                Value::build_text("t"),
                Value::from_i64(ALICE),
                Value::from_i64(0),
            ],
        );

        assert!(matches!(missing_owner, Err(LimboError::InternalError(_))));
        assert!(matches!(text_oid, Err(LimboError::InternalError(_))));
        assert_eq!(classes.classes().count(), 0);
    }

    #[test]
    fn class_renamed_keeps_the_oid_under_the_new_name() {
        let mut classes = table_and_sequence();

        ClassRenamed {
            namespace: NamespaceId::ROOT,
            old_name: "t".to_string(),
            new_name: "u".to_string(),
        }
        .apply_to(&mut classes, &[])
        .unwrap();

        assert!(classes.get_by_name(NamespaceId::ROOT, "t").is_none());
        let renamed = classes.get_by_name(NamespaceId::ROOT, "u").unwrap();
        assert_eq!(renamed.oid, TABLE_OID);
        assert_eq!(renamed.relname, "u");
    }

    #[test]
    fn class_dropped_removes_the_row() {
        let mut classes = table_and_sequence();

        ClassDropped::EngineName {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
        }
        .apply_to(&mut classes, &[])
        .unwrap();

        assert!(classes.get_by_name(NamespaceId::ROOT, "t").is_none());
        assert!(classes.get_by_name(NamespaceId::ROOT, "s").is_some());
    }

    #[test]
    fn owner_column_changes_follow_the_table_and_the_column_name() {
        let mut classes = table_and_sequence();
        let owned_by = |owner| OwnerColumnSet {
            namespace: NamespaceId::ROOT,
            name: "s".to_string(),
            owner,
        };

        owned_by(Some(OwnerColumn {
            table: "t".to_string(),
            column: "a".to_string(),
        }))
        .apply_to(&mut classes, &[])
        .unwrap();
        let sequence = classes.get_by_name(NamespaceId::ROOT, "s").unwrap();
        assert_eq!(sequence.relownedby, TABLE_OID);
        assert_eq!(sequence.relownedbycolumn.as_deref(), Some("a"));

        OwnerColumnRenamed {
            namespace: NamespaceId::ROOT,
            table: "t".to_string(),
            old_name: "a".to_string(),
            new_name: "b".to_string(),
        }
        .apply_to(&mut classes, &[])
        .unwrap();
        let sequence = classes.get_by_name(NamespaceId::ROOT, "s").unwrap();
        assert_eq!(sequence.relownedby, TABLE_OID);
        assert_eq!(sequence.relownedbycolumn.as_deref(), Some("b"));

        owned_by(None).apply_to(&mut classes, &[]).unwrap();
        let sequence = classes.get_by_name(NamespaceId::ROOT, "s").unwrap();
        assert_eq!(sequence.relownedby, 0);
        assert_eq!(sequence.relownedbycolumn, None);
    }

    #[test]
    fn owner_set_reads_back_the_owner_and_the_access_control_list() {
        let mut classes = table_and_sequence();
        let acl = Acl::default_for(RelationKind::Table, role_oid(RoleId::from_stored(BOB)));
        let owner_set = OwnerSet { oid: TABLE_OID };

        owner_set
            .apply_to(
                &mut classes,
                &[Value::from_i64(BOB), Value::build_text(acl.stored_text())],
            )
            .unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.relowner, RoleId::from_stored(BOB));
        assert_eq!(table.relacl.as_ref(), Some(&acl));

        owner_set
            .apply_to(&mut classes, &[Value::from_i64(ALICE), Value::Null])
            .unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.relowner, RoleId::from_stored(ALICE));
        assert_eq!(table.relacl, None);

        let only_owner = owner_set.apply_to(&mut classes, &[Value::from_i64(BOB)]);
        assert!(matches!(only_owner, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn acl_set_reads_back_the_access_control_list() {
        let mut classes = table_and_sequence();
        let acl = Acl::default_for(RelationKind::Table, role_oid(RoleId::from_stored(ALICE)));
        let acl_set = AclSet {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
        };

        acl_set
            .apply_to(&mut classes, &[Value::build_text(acl.stored_text())])
            .unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.relacl.as_ref(), Some(&acl));

        let not_a_list = acl_set.apply_to(&mut classes, &[Value::from_i64(1)]);
        assert!(matches!(not_a_list, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn row_security_set_changes_only_the_named_table() {
        let mut classes = table_and_sequence();

        RowSecuritySet {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
            enabled: true,
        }
        .apply_to(&mut classes, &[])
        .unwrap();

        assert!(
            classes
                .get_by_name(NamespaceId::ROOT, "t")
                .unwrap()
                .relrowsecurity
        );
        assert!(
            !classes
                .get_by_name(NamespaceId::ROOT, "s")
                .unwrap()
                .relrowsecurity
        );
    }

    #[test]
    fn policies_set_reads_back_the_policies() {
        let mut classes = table_and_sequence();
        let policies_set = PoliciesSet {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
        };

        policies_set.apply_to(&mut classes, &[Value::Null]).unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert!(table.relpolicies.is_empty());

        let not_policies = policies_set.apply_to(&mut classes, &[Value::build_text("?")]);
        let no_value = policies_set.apply_to(&mut classes, &[]);
        assert!(matches!(not_policies, Err(LimboError::InternalError(_))));
        assert!(matches!(no_value, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn a_change_to_a_relation_without_a_row_does_nothing() {
        let mut classes = table_and_sequence();

        RowSecuritySet {
            namespace: NamespaceId::ROOT,
            name: "missing".to_string(),
            enabled: true,
        }
        .apply_to(&mut classes, &[])
        .unwrap();
        AclSet {
            namespace: NamespaceId::ROOT,
            name: "missing".to_string(),
        }
        .apply_to(&mut classes, &[Value::Null])
        .unwrap();

        assert_eq!(classes.classes().count(), 2);
    }

    /// Table `t` and sequence `s`, both owned by `ALICE`.
    fn table_and_sequence() -> ClassCatalog {
        let mut classes = ClassCatalog::default();
        for (name, kind, oid) in [
            ("t", RelationKind::Table, TABLE_OID),
            ("s", RelationKind::Sequence, SEQUENCE_OID),
        ] {
            ClassCreated {
                namespace: NamespaceId::ROOT,
                name: name.to_string(),
                engine_name: Some(name.to_string()),
                kind,
            }
            .apply_to(
                &mut classes,
                &[
                    Value::from_i64(oid),
                    Value::from_i64(ALICE),
                    Value::from_i64(0),
                ],
            )
            .unwrap();
        }
        classes
    }

    /// A table that a user can name in an attached database is in no
    /// schema, as in a file that the SQLite frontend made.
    #[test]
    fn alter_table_in_an_attached_database_refuses_renames_only() {
        use turso_core::{ConnectOpts, SessionRole};

        let dir = tempfile::tempdir().unwrap();
        let attached_path = dir.path().join("attached.db");
        let io = Arc::new(turso_core::PlatformIO::new().unwrap());
        let opts = turso_core::DatabaseOpts::new().with_attach(true);
        let attached = turso_core::Database::open_file_with_flags(
            io.clone(),
            attached_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            opts.with_mvcc(true),
            None,
            Arc::new(turso_sqlite::SqliteDialect),
        )
        .unwrap()
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
        attached
            .execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER)")
            .unwrap();
        attached.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        attached.close().unwrap();
        drop(attached);

        let conn = crate::session::open_database_with_io(
            io,
            dir.path().join("main.db").to_str().unwrap(),
            turso_core::OpenFlags::default(),
            opts,
        )
        .unwrap()
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
        conn.execute(format!("ATTACH '{}' AS aux", attached_path.display()))
            .unwrap();

        for rename in [
            "ALTER TABLE aux.t RENAME TO t2",
            "ALTER TABLE aux.t RENAME COLUMN b TO z",
        ] {
            let err = conn.execute(rename).unwrap_err();
            assert_eq!(
                err.to_string(),
                "creating, renaming or dropping relations in an attached database is not supported",
                "{rename}"
            );
        }
        conn.execute("ALTER TABLE aux.t RENAME COLUMN b TO b")
            .unwrap();
        conn.execute("ALTER TABLE aux.t ADD COLUMN d INT").unwrap();
        conn.execute("ALTER TABLE aux.t DROP COLUMN c").unwrap();
        conn.execute("INSERT INTO aux.t (a, b, d) VALUES (1, 2, 3)")
            .unwrap();
    }
}
