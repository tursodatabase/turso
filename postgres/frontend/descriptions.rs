//! Comments, PostgreSQL's `pg_description` rows, in the `pg_description`
//! table of the main database: one row per comment.
//!
//! A row names its object as PostgreSQL does: `objoid` is the oid of the
//! object, `classoid` the oid of the catalog that lists it, and `objsubid`
//! the number of a column, 0 for the object itself.
//!
//! Nothing in memory holds these rows. A query reads the table.

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_pg_parser::translator::PG_NAMESPACE_OID;

use crate::catalog::PUBLIC_NAMESPACE_OID;

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

/// What `pg_description` names an object by.
struct DescribedObject {
    objoid: i64,
    classoid: i64,
    objsubid: i64,
}

impl DescribedObject {
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
