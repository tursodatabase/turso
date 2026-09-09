//! # Tantivy-Columnar
//!
//! `tantivy-columnar`provides a columnar storage for tantivy.
//! The crate allows for efficient read operations on specific columns rather than entire records.
//!
//! ## Overview
//!
//! - **columnar**: Reading, writing, and merging multiple columns:
//!   - **[ColumnarWriter]**: Makes it possible to create a new columnar.
//!   - **[ColumnarReader]**: The ColumnarReader makes it possible to access a set of columns
//!     associated to field names.
//!   - **[merge_columnar]**: Contains the functionalities to merge multiple ColumnarReader or
//!     segments into a single one.
//!
//! - **column**: A single column, which contains
//!     - [column_index]: Resolves the rows for a document id. Manages the cardinality of the
//!       column.
//!     - [column_values]: Stores the values of a column in a dense format.

#[cfg(test)]
#[macro_use]
extern crate more_asserts;

use std::fmt::Display;
use std::io;

mod block_accessor;
mod column;
pub mod column_index;
pub mod column_values;
mod columnar;
mod dictionary;
mod dynamic_column;
mod iterable;
pub(crate) mod utils;
mod value;

pub use block_accessor::ColumnBlockAccessor;
pub use column::{BytesColumn, Column, StrColumn};
pub use column_index::ColumnIndex;
pub use column_values::{
    ColumnValues, EmptyColumnValues, MonotonicallyMappableToU64, MonotonicallyMappableToU128,
};
pub use columnar::{
    CURRENT_VERSION, ColumnType, ColumnarReader, ColumnarWriter, HasAssociatedColumnType,
    MergeRowOrder, ShuffleMergeOrder, StackMergeOrder, Version, merge_columnar,
};
use sstable::VoidSSTable;
pub use value::{NumericalType, NumericalValue};

pub use self::dynamic_column::{ColumnSpaceUsage, DynamicColumn, DynamicColumnHandle};

pub type RowId = u32;
pub type DocId = u32;

#[derive(Clone, Copy, Debug)]
pub struct RowAddr {
    pub segment_ord: u32,
    pub row_id: RowId,
}

pub use sstable::{Dictionary, TermOrdHit};
pub type Streamer<'a> = sstable::Streamer<'a, VoidSSTable>;

pub use common::DateTime;

#[derive(Copy, Clone, Debug)]
pub struct InvalidData;

impl From<InvalidData> for io::Error {
    fn from(_: InvalidData) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, "Invalid data")
    }
}

/// Enum describing the number of values that can exist per document
/// (or per row if you will).
///
/// The cardinality must fit on 2 bits.
#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Cardinality {
    /// All documents contain exactly one value.
    /// `Full` is the default for auto-detecting the Cardinality, since it is the most strict.
    #[default]
    Full = 0,
    /// All documents contain at most one value.
    Optional = 1,
    /// All documents may contain any number of values.
    Multivalued = 2,
}

impl Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let short_str = match self {
            Cardinality::Full => "full",
            Cardinality::Optional => "opt",
            Cardinality::Multivalued => "mult",
        };
        write!(f, "{short_str}")
    }
}

impl Cardinality {
    pub fn is_optional(&self) -> bool {
        matches!(self, Cardinality::Optional)
    }
    pub fn is_multivalue(&self) -> bool {
        matches!(self, Cardinality::Multivalued)
    }
    pub fn is_full(&self) -> bool {
        matches!(self, Cardinality::Full)
    }
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }
    pub(crate) fn try_from_code(code: u8) -> Result<Cardinality, InvalidData> {
        match code {
            0 => Ok(Cardinality::Full),
            1 => Ok(Cardinality::Optional),
            2 => Ok(Cardinality::Multivalued),
            _ => Err(InvalidData),
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compat_tests;
