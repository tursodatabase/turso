//! The `index` module in Tantivy contains core components to read and write indexes.
//!
//! It contains `Index` and `Segment`, where a `Index` consists of one or more `Segment`s.

mod index;
mod index_meta;
mod inverted_index_reader;
mod segment;
mod segment_component;
mod segment_id;
mod segment_reader;

pub use self::index::{Index, IndexBuilder};
pub(crate) use self::index_meta::SegmentMetaInventory;
pub use self::index_meta::{IndexMeta, IndexSettings, Order, SegmentMeta};
pub use self::inverted_index_reader::InvertedIndexReader;
pub use self::segment::Segment;
pub use self::segment_component::SegmentComponent;
pub use self::segment_id::SegmentId;
pub use self::segment_reader::{FieldMetadata, SegmentReader};
