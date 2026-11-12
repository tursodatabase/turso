//! Reading and rewriting the rows of the schema table (`sqlite_schema`).
//!
//! Only this file knows the column layout. Code outside `core/schema` reads
//! these records through [`super::SqliteSchemaStore`].

use crate::types::{ImmutableRecord, ImmutableRecordRef, Value, ValueRef};
use crate::{LimboError, Numeric, Result};

/// Number of columns in a SQLite schema record: type, name, tbl_name,
/// rootpage, sql.
pub(super) const SCHEMA_RECORD_COLUMN_COUNT: usize = 5;

const TYPE_COLUMN: usize = 0;
const NAME_COLUMN: usize = 1;
const ROOT_PAGE_COLUMN: usize = 3;

/// What kind of object a schema record describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaRowType {
    Table,
    Index,
    Trigger,
    View,
    /// Any other type string.
    Other,
}

impl SchemaRowType {
    fn from_text(text: &str) -> Self {
        if text.eq_ignore_ascii_case("table") {
            Self::Table
        } else if text.eq_ignore_ascii_case("index") {
            Self::Index
        } else if text.eq_ignore_ascii_case("trigger") {
            Self::Trigger
        } else if text.eq_ignore_ascii_case("view") {
            Self::View
        } else {
            Self::Other
        }
    }
}

/// The columns of a schema record that callers outside the schema code need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRecord {
    pub row_type: SchemaRowType,
    pub name: String,
    /// Root page of the object's B-tree. Zero when the object has none
    /// (views, triggers) or when the B-tree is not created yet.
    pub root_page: i64,
}

impl SchemaRecord {
    /// Decode a schema record from its serialized bytes.
    pub(super) fn decode(record_bytes: &[u8]) -> Result<Self> {
        let record = ImmutableRecordRef::from_bin_record(record_bytes);
        let column_count = record.column_count();
        if column_count < SCHEMA_RECORD_COLUMN_COUNT {
            return Err(LimboError::Corrupt(format!(
                "schema record must have at least {SCHEMA_RECORD_COLUMN_COUNT} columns, got {column_count}",
            )));
        }
        let (row_type, name, root_page) =
            record.get_three_values(TYPE_COLUMN, NAME_COLUMN, ROOT_PAGE_COLUMN)?;
        let ValueRef::Text(row_type) = row_type else {
            return Err(LimboError::Corrupt(format!(
                "schema record type must be text, got {row_type:?}"
            )));
        };
        let ValueRef::Text(name) = name else {
            return Err(LimboError::Corrupt(format!(
                "schema record name must be text, got {name:?}"
            )));
        };
        let ValueRef::Numeric(Numeric::Integer(root_page)) = root_page else {
            return Err(LimboError::Corrupt(format!(
                "schema record rootpage must be integer, got {root_page:?}"
            )));
        };
        Ok(Self {
            row_type: SchemaRowType::from_text(row_type.as_str()),
            name: name.as_str().to_string(),
            root_page,
        })
    }

    /// True if this record describes a table or an index that has its own
    /// B-tree.
    pub fn is_btree_table_or_index(&self) -> bool {
        self.root_page != 0 && matches!(self.row_type, SchemaRowType::Table | SchemaRowType::Index)
    }
}

/// Return a copy of a schema record with its root page replaced.
pub(super) fn schema_record_with_root_page(
    record_bytes: &[u8],
    root_page: i64,
) -> Result<ImmutableRecord> {
    let mut values = ImmutableRecordRef::from_bin_record(record_bytes).get_values_owned()?;
    if values.len() <= ROOT_PAGE_COLUMN {
        return Err(LimboError::Corrupt(format!(
            "schema record must have at least {SCHEMA_RECORD_COLUMN_COUNT} columns, got {}",
            values.len()
        )));
    }
    values[ROOT_PAGE_COLUMN] = Value::from_i64(root_page);
    ImmutableRecord::from_values(&values, values.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(values: &[Value]) -> Vec<u8> {
        ImmutableRecord::from_values(values, values.len())
            .unwrap()
            .get_payload()
            .to_vec()
    }

    fn schema_values(row_type: &str, name: &str, root_page: i64) -> Vec<Value> {
        vec![
            Value::build_text(row_type.to_string()),
            Value::build_text(name.to_string()),
            Value::build_text(name.to_string()),
            Value::from_i64(root_page),
            Value::build_text("sql".to_string()),
        ]
    }

    #[test]
    fn decode_reads_type_name_and_root_page() {
        let bytes = record(&schema_values("index", "idx", 7));
        let decoded = SchemaRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.row_type, SchemaRowType::Index);
        assert_eq!(decoded.name, "idx");
        assert_eq!(decoded.root_page, 7);
        assert!(decoded.is_btree_table_or_index());
    }

    #[test]
    fn decode_rejects_short_record() {
        let mut values = schema_values("table", "t", 2);
        values.pop();
        assert!(SchemaRecord::decode(&record(&values)).is_err());
    }

    #[test]
    fn view_and_zero_root_page_have_no_btree() {
        let view = SchemaRecord::decode(&record(&schema_values("view", "v", 0))).unwrap();
        assert!(!view.is_btree_table_or_index());
        let table = SchemaRecord::decode(&record(&schema_values("table", "t", 0))).unwrap();
        assert!(!table.is_btree_table_or_index());
    }

    #[test]
    fn rewriting_root_page_keeps_other_columns() {
        let bytes = record(&schema_values("table", "t", 0));
        let rewritten = schema_record_with_root_page(&bytes, 9).unwrap();
        let decoded = SchemaRecord::decode(rewritten.get_payload()).unwrap();
        assert_eq!(decoded.root_page, 9);
        assert_eq!(decoded.name, "t");
        assert_eq!(decoded.row_type, SchemaRowType::Table);
    }
}
