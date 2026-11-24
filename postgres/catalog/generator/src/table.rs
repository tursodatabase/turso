//! What the generator writes for one catalog, worked out once: the fields,
//! the key, the SQL of the table and the rows in the order of the table.
//! The struct, the table, the SQL and the seed all come from it, so they
//! cannot disagree on the columns or on the order of the rows.

use std::collections::BTreeSet;

use crate::bki::{Catalog, Token};
use crate::column::{oid_value, ColumnKind, Field};
use crate::sql::TableSql;
use crate::SqlValue;

/// The catalogs whose built-in rows the generated file leaves out, each
/// with the reason, which the documentation of its table gives.
const ROWS_LEFT_OUT: [(&str, &str); 3] = [
    (
        "pg_class",
        "`postgres.bki` has rows for only the four catalogs that PostgreSQL needs to start. \
         `initdb` makes the other rows",
    ),
    (
        "pg_attribute",
        "`postgres.bki` has rows for only the four catalogs that PostgreSQL needs to start. \
         `initdb` makes the other rows",
    ),
    (
        "pg_database",
        "its row has the encoding and the locale of the cluster, which only `initdb` knows",
    ),
];

pub(crate) struct Table<'a> {
    pub(crate) catalog: &'a Catalog,
    /// The columns of the catalog, in the order of the file. The struct,
    /// `CREATE_SQL` and the values of a seed row have them in this order.
    pub(crate) fields: Vec<Field<'a>>,
    /// Whether the first column is an oid that identifies a row: a unique
    /// index is on it alone. The table is then in the order of that oid.
    pub(crate) has_key: bool,
    /// Whether the first column is `oid` and the `INTEGER PRIMARY KEY`.
    pub(crate) oid_is_rowid: bool,
    pub(crate) sql: TableSql,
    /// Why the rows of the catalog are left out, if they are.
    pub(crate) left_out: Option<&'static str>,
    /// The rows of the table, in the order of the key, or in the order of
    /// the file when the catalog has no key.
    pub(crate) rows: Vec<&'a [Token]>,
}

impl<'a> Table<'a> {
    pub(crate) fn of_catalog(catalog: &'a Catalog) -> Result<Self, String> {
        let in_catalog = |message: String| format!("{}: {message}", catalog.name);
        let fields = Field::of_catalog(catalog).map_err(in_catalog)?;

        let has_key = fields.first().is_some_and(|first| {
            first.kind == ColumnKind::Oid
                && !first.nullable
                && catalog.has_unique_index_on(first.name)
        });
        // The column `oid` is the rowid when a unique index is on it alone,
        // as the oid then identifies a row.
        let oid_is_rowid = catalog.has_unique_index_on("oid");
        if oid_is_rowid && !(has_key && fields[0].name == "oid") {
            return Err(in_catalog(
                "a unique index is on oid alone, and oid is not the first column".to_string(),
            ));
        }
        let rowid_column = oid_is_rowid.then_some("oid");
        let sql = TableSql::of_catalog(catalog, &fields, rowid_column).map_err(in_catalog)?;

        let left_out = ROWS_LEFT_OUT
            .iter()
            .find(|(name, _)| *name == catalog.name)
            .map(|(_, reason)| *reason);
        let mut rows: Vec<&[Token]> = match left_out {
            Some(_) => Vec::new(),
            None => catalog.rows.iter().map(Vec::as_slice).collect(),
        };
        if has_key {
            let mut keys = Vec::with_capacity(rows.len());
            for row in &rows {
                keys.push((oid_value(&row[0]).map_err(in_catalog)?, *row));
            }
            // The lookup is a binary search. The sort is stable, so equal
            // keys keep their order until the check below refuses them.
            keys.sort_by_key(|(key, _)| *key);
            let distinct: BTreeSet<u32> = keys.iter().map(|(key, _)| *key).collect();
            if distinct.len() != keys.len() {
                return Err(in_catalog(format!(
                    "two rows have the same {}",
                    fields[0].name
                )));
            }
            rows = keys.into_iter().map(|(_, row)| row).collect();
        }
        Ok(Self {
            catalog,
            fields,
            has_key,
            oid_is_rowid,
            sql,
            left_out,
            rows,
        })
    }

    /// The field that identifies a row, if there is one.
    pub(crate) fn key(&self) -> Option<&Field<'a>> {
        self.has_key.then(|| &self.fields[0])
    }

    /// The SQL values of the rows, in the order of the table, as the seed of
    /// the catalog writes them.
    pub(crate) fn sql_rows(&self) -> Result<Vec<Vec<SqlValue>>, String> {
        self.rows
            .iter()
            .map(|row| {
                self.fields
                    .iter()
                    .zip(row.iter())
                    .map(|(field, token)| {
                        field.sql_value(token).map_err(|message| {
                            format!("{}: column {}: {message}", self.catalog.name, field.name)
                        })
                    })
                    .collect()
            })
            .collect()
    }
}
