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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    fn record(values: &[Value]) -> ImmutableRecord {
        ImmutableRecord::from_values(values, values.len()).unwrap()
    }

    fn table_record(name: &str, root_page: i64) -> ImmutableRecord {
        record(&[
            Value::build_text("table"),
            Value::build_text(name.to_string()),
            Value::build_text(name.to_string()),
            Value::from_i64(root_page),
            Value::build_text(format!("CREATE TABLE {name}(x)")),
        ])
    }

    #[test]
    fn reads_every_column() {
        let record = table_record("t", -2);
        let row = SqliteSchemaRow::from_record_bytes(record.get_payload()).unwrap();
        assert_eq!(
            row,
            SqliteSchemaRow {
                row_type: "table",
                name: "t",
                table_name: "t",
                root_page: -2,
                sql: Some("CREATE TABLE t(x)"),
            }
        );
    }

    #[test]
    fn null_sql_is_none() {
        let record = record(&[
            Value::build_text("index"),
            Value::build_text("sqlite_autoindex_t_1"),
            Value::build_text("t"),
            Value::from_i64(3),
            Value::Null,
        ]);
        let row = SqliteSchemaRow::from_record_bytes(record.get_payload()).unwrap();
        assert_eq!(row.sql, None);
    }

    #[test]
    fn record_with_too_few_columns_is_corrupt() {
        let record = record(&[
            Value::build_text("table"),
            Value::build_text("t"),
            Value::build_text("t"),
            Value::from_i64(2),
        ]);
        let err = SqliteSchemaRow::from_record_bytes(record.get_payload()).unwrap_err();
        assert!(matches!(err, LimboError::Corrupt(_)), "{err:?}");
    }

    #[test]
    fn empty_record_is_corrupt() {
        let err = SqliteSchemaRow::from_record_bytes(&[]).unwrap_err();
        assert!(matches!(err, LimboError::Corrupt(_)), "{err:?}");
    }

    #[test]
    fn column_of_the_wrong_type_is_corrupt() {
        let integer_type = record(&[
            Value::from_i64(1),
            Value::build_text("t"),
            Value::build_text("t"),
            Value::from_i64(2),
            Value::Null,
        ]);
        let text_root_page = record(&[
            Value::build_text("table"),
            Value::build_text("t"),
            Value::build_text("t"),
            Value::build_text("2"),
            Value::Null,
        ]);
        for record in [integer_type, text_root_page] {
            let err = SqliteSchemaRow::from_record_bytes(record.get_payload()).unwrap_err();
            assert!(matches!(err, LimboError::Corrupt(_)), "{err:?}");
        }
    }

    #[test]
    fn finds_the_types_table_root_page() {
        let user_table = table_record("t", 2);
        let types_table = table_record(TURSO_TYPES_TABLE_NAME, 7);
        assert_eq!(
            types_table_root_page([user_table.get_payload()]).unwrap(),
            None
        );
        assert_eq!(
            types_table_root_page([user_table.get_payload(), types_table.get_payload()]).unwrap(),
            Some(7)
        );
    }

    #[test]
    fn to_record_gives_back_the_decoded_record() {
        let with_sql = table_record("t", -2);
        let without_sql = record(&[
            Value::build_text("index"),
            Value::build_text("sqlite_autoindex_t_1"),
            Value::build_text("t"),
            Value::from_i64(3),
            Value::Null,
        ]);
        for record in [with_sql, without_sql] {
            let row = SqliteSchemaRow::from_record_bytes(record.get_payload()).unwrap();
            assert_eq!(row.to_record().unwrap().get_payload(), record.get_payload());
        }
    }

    #[test]
    fn with_root_page_changes_only_the_root_page() {
        let record = table_record("t", -2);
        let row = SqliteSchemaRow::from_record_bytes(record.get_payload()).unwrap();
        assert_eq!(
            row.with_root_page(5),
            SqliteSchemaRow {
                root_page: 5,
                ..row
            }
        );
    }
}
