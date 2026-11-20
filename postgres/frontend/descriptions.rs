//! Comments, PostgreSQL's `pg_description` rows, in the `pg_description`
//! table of the main database: one row per comment that `COMMENT ON` stored.
//!
//! A row names its object as PostgreSQL does: `objoid` is the oid of the
//! object, `classoid` the oid of the catalog that lists it, and `objsubid`
//! the number of a column, 0 for the object itself. Relations, their columns
//! and schemas have comments. `COMMENT ON` any other kind of object is
//! refused.
//!
//! Nothing in memory holds these rows. `COMMENT ON` writes them with
//! statements of the engine in the user's transaction, and
//! `obj_description` and `col_description` are queries on the table.

use std::sync::Arc;

use turso_core::namespace::NamespaceId;
use turso_core::schema::{RelationKind, Schema};
use turso_core::{Connection, Result, MAIN_DB_ID};
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INSUFFICIENT_PRIVILEGE, SQLSTATE_INVALID_SCHEMA_NAME,
    SQLSTATE_UNDEFINED_COLUMN, SQLSTATE_UNDEFINED_TABLE, SQLSTATE_WRONG_OBJECT_TYPE,
};
use turso_pg_parser::translator::{
    is_catalog_table_name, PgCommentObject, PgCommentOnStmt, PG_CLASS_OID, PG_NAMESPACE_OID,
};

use crate::catalog::{key_constraint_names, namespace_oid, PUBLIC_NAMESPACE_OID};
use crate::classes::{class_catalog, RelationClass};
use crate::error::sqlstate_error;
use crate::roles::schema_roles;
use crate::session::relation_lookup_namespaces;

pub(crate) const DESCRIPTION_TABLE_NAME: &str = "pg_description";

/// Whether `name` in the root namespace is a catalog table that this
/// frontend stores. Every role reads such a table and no user statement
/// writes it.
pub(crate) fn is_stored_catalog_table(namespace: NamespaceId, name: &str) -> bool {
    namespace.is_root() && name.eq_ignore_ascii_case(DESCRIPTION_TABLE_NAME)
}

/// The statements that create the table, with the comment that PostgreSQL
/// gives the public schema. None when `schema` has the table.
pub(crate) fn initialize_statements(schema: &Schema) -> Vec<String> {
    if schema
        .get_btree_table(NamespaceId::ROOT, DESCRIPTION_TABLE_NAME)
        .is_some()
    {
        return Vec::new();
    }
    let public_schema = DescribedObject {
        objoid: PUBLIC_NAMESPACE_OID,
        classoid: PG_NAMESPACE_OID,
        objsubid: 0,
    };
    vec![
        format!(
            "CREATE TABLE {DESCRIPTION_TABLE_NAME} \
             (objoid INTEGER, classoid INTEGER, objsubid INTEGER, description TEXT)"
        ),
        public_schema.insert_statement("standard public schema"),
    ]
}

/// The engine statements that `stmt` runs: they replace the comment of its
/// object, or delete it.
pub(crate) fn comment_statements(
    conn: &Arc<Connection>,
    stmt: &PgCommentOnStmt,
) -> Result<Vec<String>> {
    let object = described_object(conn, &stmt.object)?;
    let mut statements = vec![object.delete_statement()];
    if let Some(comment) = &stmt.comment {
        statements.push(object.insert_statement(comment));
    }
    Ok(statements)
}

/// What `pg_description` names an object by.
struct DescribedObject {
    objoid: i64,
    classoid: i64,
    objsubid: i64,
}

impl DescribedObject {
    fn delete_statement(&self) -> String {
        let Self {
            objoid,
            classoid,
            objsubid,
        } = self;
        format!(
            "DELETE FROM {DESCRIPTION_TABLE_NAME} \
             WHERE objoid = {objoid} AND classoid = {classoid} AND objsubid = {objsubid}"
        )
    }

    fn insert_statement(&self, comment: &str) -> String {
        let Self {
            objoid,
            classoid,
            objsubid,
        } = self;
        format!(
            "INSERT INTO {DESCRIPTION_TABLE_NAME} (objoid, classoid, objsubid, description) \
             VALUES ({objoid}, {classoid}, {objsubid}, '{}')",
            comment.replace('\'', "''")
        )
    }
}

/// Finds the object that a COMMENT ON statement names. Fails as PostgreSQL
/// does when the object does not exist, is of another kind than the
/// statement says, or belongs to a role whose privileges the current role
/// does not have.
fn described_object(conn: &Arc<Connection>, object: &PgCommentObject) -> Result<DescribedObject> {
    let schema = conn.current_schema();
    match object {
        PgCommentObject::Relation {
            kind,
            schema: schema_name,
            name,
        } => {
            let (_, class) = find_relation(conn, &schema, schema_name.as_deref(), name)?;
            if class.relkind != *kind {
                return Err(sqlstate_error(
                    SQLSTATE_WRONG_OBJECT_TYPE,
                    format!("\"{name}\" is not {}", kind.with_article()),
                    None,
                ));
            }
            check_owner(conn, &schema, class, kind.name())?;
            Ok(DescribedObject {
                objoid: class.oid,
                classoid: PG_CLASS_OID,
                objsubid: 0,
            })
        }
        PgCommentObject::Column {
            schema: schema_name,
            table,
            column,
        } => {
            let (namespace, class) = find_relation(conn, &schema, schema_name.as_deref(), table)?;
            let stored_table = match class.relkind {
                RelationKind::Table => schema.get_btree_table(namespace, &table.to_lowercase()),
                _ => None,
            };
            let Some(stored_table) = stored_table else {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "COMMENT ON COLUMN of {} is not supported",
                        class.relkind.with_article()
                    ),
                    None,
                ));
            };
            let Some((position, _)) = stored_table.get_column(column) else {
                return Err(sqlstate_error(
                    SQLSTATE_UNDEFINED_COLUMN,
                    format!("column \"{column}\" of relation \"{table}\" does not exist"),
                    None,
                ));
            };
            check_owner(conn, &schema, class, "relation")?;
            Ok(DescribedObject {
                objoid: class.oid,
                classoid: PG_CLASS_OID,
                objsubid: position as i64 + 1,
            })
        }
        PgCommentObject::Schema(name) => {
            let folded_name = name.to_lowercase();
            let Some(namespace) = conn
                .qualifier(&folded_name)
                .filter(|namespace| !namespace.is_database())
            else {
                if crate::catalog::namespace_exists(conn, &folded_name) {
                    return Err(sqlstate_error(
                        SQLSTATE_FEATURE_NOT_SUPPORTED,
                        format!("COMMENT ON SCHEMA {name} is not supported"),
                        None,
                    ));
                }
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_SCHEMA_NAME,
                    format!("schema \"{name}\" does not exist"),
                    None,
                ));
            };
            if !conn.current_role_is_superuser() {
                return Err(sqlstate_error(
                    SQLSTATE_INSUFFICIENT_PRIVILEGE,
                    format!("must be owner of schema {name}"),
                    None,
                ));
            }
            Ok(DescribedObject {
                objoid: namespace_oid(&schema, namespace.target.id),
                classoid: PG_NAMESPACE_OID,
                objsubid: 0,
            })
        }
        PgCommentObject::Unsupported(kind) => Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("COMMENT ON {kind} is not supported"),
            None,
        )),
    }
}

/// The relation `name` of the main database, in `schema_name` or else in
/// the first namespace of the search path that has it, with its namespace.
fn find_relation<'a>(
    conn: &Arc<Connection>,
    schema: &'a Schema,
    schema_name: Option<&str>,
    name: &str,
) -> Result<(NamespaceId, &'a RelationClass)> {
    if let Some(schema_name) = schema_name {
        if !crate::catalog::namespace_exists(conn, &schema_name.to_lowercase()) {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                format!("schema \"{schema_name}\" does not exist"),
                None,
            ));
        }
    }
    let classes = class_catalog(schema);
    let folded_name = name.to_lowercase();
    let namespaces: Vec<NamespaceId> = relation_lookup_namespaces(conn, schema_name)
        .into_iter()
        .filter(|namespace| namespace.database_id() == MAIN_DB_ID)
        .map(|namespace| namespace.target.id)
        .collect();
    namespaces
        .iter()
        .find_map(|namespace| Some((*namespace, classes?.get_by_name(*namespace, &folded_name)?)))
        .ok_or_else(|| {
            if is_catalog_relation(schema_name, name) {
                return sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "COMMENT ON a system catalog relation is not supported".to_string(),
                    None,
                );
            }
            if is_constraint_index(schema, &namespaces, &folded_name) {
                return sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "COMMENT ON the index of a constraint is not supported".to_string(),
                    None,
                );
            }
            let name = match schema_name {
                Some(schema_name) => format!("{schema_name}.{name}"),
                None => name.to_string(),
            };
            sqlstate_error(
                SQLSTATE_UNDEFINED_TABLE,
                format!("relation \"{name}\" does not exist"),
                None,
            )
        })
}

fn is_catalog_relation(schema_name: Option<&str>, name: &str) -> bool {
    match schema_name.map(str::to_lowercase).as_deref() {
        Some("information_schema") => true,
        Some("pg_catalog") | None => is_catalog_table_name(name),
        Some(_) => false,
    }
}

/// Whether `name` is the PostgreSQL name of the index of a PRIMARY KEY or
/// UNIQUE constraint of a table in one of `namespaces`.
fn is_constraint_index(schema: &Schema, namespaces: &[NamespaceId], name: &str) -> bool {
    let Some(classes) = class_catalog(schema) else {
        return false;
    };
    classes
        .classes()
        .filter(|class| {
            class.relkind == RelationKind::Table && namespaces.contains(&class.relnamespace)
        })
        .filter_map(|class| schema.get_btree_table(class.relnamespace, &class.relname))
        .flat_map(|table| key_constraint_names(&table))
        .any(|constraint| constraint.eq_ignore_ascii_case(name))
}

/// Fails unless the current role has the privileges of the owner of
/// `class`, which the error names as `kind_name` and its name.
fn check_owner(
    conn: &Connection,
    schema: &Schema,
    class: &RelationClass,
    kind_name: &str,
) -> Result<()> {
    if conn.current_role_is_superuser()
        || schema_roles(schema).has_privileges_of(conn.current_role(), class.relowner)
    {
        return Ok(());
    }
    Err(sqlstate_error(
        SQLSTATE_INSUFFICIENT_PRIVILEGE,
        format!("must be owner of {kind_name} {}", class.relname),
        None,
    ))
}
