//! Tests of how turso_core parses, plans and translates SQLite SQL. They
//! need a dialect that can parse, so they are here and not in turso_core.

use turso_core::schema::BTreeTable;

/// Builds a table from the SQLite text of its `CREATE TABLE`.
fn table_from_sql(sql: &str, root_page: i64) -> turso_core::Result<BTreeTable> {
    BTreeTable::from_sql(sql, root_page)
}

pub mod collate;
pub mod dialect;
pub mod expr_type;
pub mod index;
pub mod optimizer;
pub mod optimizer_join;
pub mod optimizer_multi_index;
pub mod roles;
pub mod row_security;
pub mod schema;
pub mod translate;
pub mod typed_value_order;
pub mod util;
