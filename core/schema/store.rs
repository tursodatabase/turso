//! How a dialect stores its schema.
//!
//! The B-tree at page 1 holds a row for every other B-tree of the database,
//! with its root page. MVCC reads and rewrites those rows: it decodes them
//! to recover B-tree identity and log order, and rewrites the root page when
//! a checkpoint gives a new B-tree its real page. A [`SchemaStore`] says how
//! the rows of that B-tree are laid out, so MVCC does not have to know.
//!
//! The B-tree is always at page 1, whichever store the dialect has: MVCC
//! names it by the fixed table id `-1`, and that id is written in the
//! logical log.

use std::fmt::Debug;

use super::record::{schema_record_with_root_page, SCHEMA_RECORD_COLUMN_COUNT};
use super::SchemaRecord;
use crate::types::ImmutableRecord;
use crate::Result;

/// The storage of a dialect's schema. [`crate::Dialect::schema_store`]
/// returns the one of a database's dialect.
pub trait SchemaStore: Send + Sync + Debug {
    /// The number of columns of a row of the B-tree at page 1.
    fn root_record_column_count(&self) -> usize;

    /// The object a row of the B-tree at page 1 describes.
    fn decode_root_record(&self, record: &[u8]) -> Result<SchemaRecord>;

    /// A copy of a row of the B-tree at page 1 with its root page replaced.
    fn with_root_page(&self, record: &[u8], root_page: i64) -> Result<ImmutableRecord>;
}

/// The SQLite schema store: `sqlite_schema` rows of `type, name, tbl_name,
/// rootpage, sql`.
#[derive(Debug)]
pub struct SqliteSchemaStore;

impl SchemaStore for SqliteSchemaStore {
    fn root_record_column_count(&self) -> usize {
        SCHEMA_RECORD_COLUMN_COUNT
    }

    fn decode_root_record(&self, record: &[u8]) -> Result<SchemaRecord> {
        SchemaRecord::decode(record)
    }

    fn with_root_page(&self, record: &[u8], root_page: i64) -> Result<ImmutableRecord> {
        schema_record_with_root_page(record, root_page)
    }
}
