//! The layout of a `sqlite_schema` record: (type, name, tbl_name, rootpage, sql).

use crate::schema::TURSO_TYPES_TABLE_NAME;
use crate::types::{
    ImmutableRecord, ImmutableRecordRef, TextRef, TextSubtype, ValueIterator, ValueRef,
};
use crate::{LimboError, Numeric, Result};

pub(crate) const SQLITE_SCHEMA_COLUMN_COUNT: usize = 5;

/// The columns of one `sqlite_schema` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqliteSchemaRow<'a> {
    /// "table", "index", "view", "trigger", ...
    pub(crate) row_type: &'a str,
    pub(crate) name: &'a str,
    pub(crate) table_name: &'a str,
    /// Zero for an object without a B-tree. Negative for a table or index
    /// that no checkpoint has written yet; the value is then its MVCC table id.
    pub(crate) root_page: i64,
    /// `None` when the column is NULL, as it is for an automatic index.
    pub(crate) sql: Option<&'a str>,
}

impl<'a> SqliteSchemaRow<'a> {
    pub(crate) fn from_record_bytes(record: &'a [u8]) -> Result<Self> {
        let column_count = ImmutableRecordRef::from_bin_record(record).column_count();
        if column_count < SQLITE_SCHEMA_COLUMN_COUNT {
            return Err(LimboError::Corrupt(format!(
                "sqlite_schema row must have at least {SQLITE_SCHEMA_COLUMN_COUNT} columns, got {column_count}",
            )));
        }
        let mut columns = ValueIterator::new(record)?;
        let mut next_column = || {
            columns.next().unwrap_or_else(|| {
                Err(LimboError::Corrupt(
                    "sqlite_schema row ends before its last column".to_string(),
                ))
            })
        };

        let row_type = text_column(next_column()?, "type")?;
        let name = text_column(next_column()?, "name")?;
        let table_name = text_column(next_column()?, "tbl_name")?;
        let root_page = match next_column()? {
            ValueRef::Numeric(Numeric::Integer(root_page)) => root_page,
            other => {
                return Err(LimboError::Corrupt(format!(
                    "sqlite_schema root_page must be integer, got {other:?}"
                )));
            }
        };
        let sql = match next_column()? {
            ValueRef::Text(sql) => Some(sql.as_str()),
            _ => None,
        };
        Ok(Self {
            row_type,
            name,
            table_name,
            root_page,
            sql,
        })
    }

    /// This row with `root_page` as its rootpage.
    pub(crate) fn with_root_page(self, root_page: i64) -> Self {
        Self { root_page, ..self }
    }

    pub(crate) fn to_record(self) -> Result<ImmutableRecord> {
        let text = |value: &'a str| ValueRef::Text(TextRef::new(value, TextSubtype::Text));
        let columns = [
            text(self.row_type),
            text(self.name),
            text(self.table_name),
            ValueRef::Numeric(Numeric::Integer(self.root_page)),
            self.sql.map_or(ValueRef::Null, text),
        ];
        ImmutableRecord::from_values(columns, columns.len())
    }

    fn is_types_table(&self) -> bool {
        self.row_type == "table" && self.name == TURSO_TYPES_TABLE_NAME
    }
}

fn text_column<'a>(value: ValueRef<'a>, column: &str) -> Result<&'a str> {
    match value {
        ValueRef::Text(text) => Ok(text.as_str()),
        other => Err(LimboError::Corrupt(format!(
            "sqlite_schema {column} must be text, got {other:?}"
        ))),
    }
}

/// The root page of the types table, if one of the `sqlite_schema` records
/// in `records` is its row.
pub(crate) fn types_table_root_page<'a>(
    records: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Option<i64>> {
    for record in records {
        let row = SqliteSchemaRow::from_record_bytes(record)?;
        if row.is_types_table() {
            return Ok(Some(row.root_page));
        }
    }
    Ok(None)
}
