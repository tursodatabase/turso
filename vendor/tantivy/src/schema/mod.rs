//! Schema definition for tantivy's indices.
//!
//! # Setting your schema in Tantivy
//!
//! Tantivy has a very strict schema.
//! The schema defines information about the fields your index contains, that is, for each field:
//!
//! - the field name (may contain any character, can't start with a `-` and can't be empty. Some
//!   characters may require escaping when using the query parser).
//! - the type of the field (currently `text`, `u64`, `i64`, `f64`, `bool`, `date`, `IpAddr`,
//!   facets, bytes and json are supported)
//! - how the field should be indexed / stored.
//!
//! This very last point is critical as it will enable / disable some of the functionality
//! for your index.
//!
//! Tantivy's schema is stored within the `meta.json` file at the root of your
//! directory.
//!
//!
//!
//! # Building a schema "programmatically"
//!
//!
//! ## Setting a text field
//!
//! ### Example
//!
//! ```
//! use tantivy::schema::*;
//! let mut schema_builder = Schema::builder();
//! let title_options = TextOptions::default()
//!     .set_stored()
//!     .set_indexing_options(TextFieldIndexing::default()
//!     .set_tokenizer("default")
//!     .set_index_option(IndexRecordOption::WithFreqsAndPositions));
//! schema_builder.add_text_field("title", title_options);
//! let schema = schema_builder.build();
//! ```
//!
//! We can split the problem of generating a search result page into two phases:
//!
//! - identifying the list of 10 or so documents to be displayed (Conceptually `query -> doc_ids[]`)
//! - for each of these documents, retrieving the information required to generate the search
//!   results page. (`doc_ids[] -> Document[]`)
//!
//! In the first phase, the ability to search for documents by the given field is determined by the
//! [`IndexRecordOption`] of our [`TextOptions`].
//!
//! The effect of each possible setting is described more in detail in [`TextOptions`].
//!
//! On the other hand setting the field as stored or not determines whether the field should be
//! returned when [`Searcher::doc()`](crate::Searcher::doc) is called.
//!
//!
//! ## Setting a u64, a i64 or a f64 field
//!
//! ### Example
//!
//! ```
//! use tantivy::schema::*;
//! let mut schema_builder = Schema::builder();
//! let num_stars_options = NumericOptions::default()
//!     .set_stored()
//!     .set_indexed();
//! schema_builder.add_u64_field("num_stars", num_stars_options);
//! let schema = schema_builder.build();
//! ```
//!
//! Just like for Text fields (see above),
//! setting the field as stored defines whether the field will be
//! returned when [`Searcher::doc()`](crate::Searcher::doc) is called,
//! and setting the field as indexed means that we will be able perform queries such as
//! `num_stars:10`. Note that unlike text fields, numeric fields can only be indexed in one way for
//! the moment.
//!
//! ### Shortcuts
//!
//!
//! For convenience, it is possible to define your field indexing options by combining different
//! flags using the  `|` operator.
//!
//! For instance, a schema containing the two fields defined in the example above could be
//! rewritten:
//!
//! ```
//! use tantivy::schema::*;
//! let mut schema_builder = Schema::builder();
//! schema_builder.add_u64_field("num_stars", INDEXED | STORED);
//! schema_builder.add_text_field("title", TEXT | STORED);
//! let schema = schema_builder.build();
//! ```
//!
//! ### Fast fields
//! This functionality is somewhat similar to Lucene's `DocValues`.
//!
//! Fields that are indexed as [`FAST`] will be stored in a special data structure that will
//! make it possible to access the value given the doc id rapidly. This is useful if the value
//! of the field is required during scoring or collection for instance.
//!
//! Some queries may leverage Fast fields when run on a field that is not indexed. This can be
//! handy if that kind of request is infrequent, however note that searching on a Fast field is
//! generally much slower than searching in an index.
//!
//! ```
//! use tantivy::schema::*;
//! let mut schema_builder = Schema::builder();
//! schema_builder.add_u64_field("population", STORED | FAST);
//! schema_builder.add_text_field("zip_code", STRING | FAST);
//! let schema = schema_builder.build();
//! ```

pub mod document;
mod facet;
mod facet_options;
mod schema;
pub(crate) mod term;

mod field_entry;
mod field_type;

mod bytes_options;
mod date_time_options;
mod field;
mod flags;
mod index_record_option;
mod ip_options;
mod json_object_options;
mod named_field_document;
mod numeric_options;
mod text_options;

use columnar::ColumnType;

pub use self::bytes_options::BytesOptions;
pub use self::date_time_options::{DateOptions, DateTimePrecision, DATE_TIME_PRECISION_INDEXED};
pub use self::document::{DocParsingError, Document, OwnedValue, TantivyDocument, Value};
pub(crate) use self::facet::FACET_SEP_BYTE;
pub use self::facet::{Facet, FacetParseError};
pub use self::facet_options::FacetOptions;
pub use self::field::Field;
pub use self::field_entry::FieldEntry;
pub use self::field_type::{FieldType, Type};
pub use self::flags::{COERCE, FAST, INDEXED, STORED};
pub use self::index_record_option::IndexRecordOption;
pub use self::ip_options::{IntoIpv6Addr, IpAddrOptions};
pub use self::json_object_options::JsonObjectOptions;
pub use self::named_field_document::NamedFieldDocument;
pub use self::numeric_options::NumericOptions;
pub use self::schema::{Schema, SchemaBuilder};
pub use self::term::{Term, ValueBytes};
pub use self::text_options::{TextFieldIndexing, TextOptions, STRING, TEXT};

/// Validator for a potential `field_name`.
/// Returns true if the name can be use for a field name.
///
/// A field name can be any character, must have at least one character
/// and must not start with a `-`.
pub fn is_valid_field_name(field_name: &str) -> bool {
    !field_name.is_empty() && !field_name.starts_with('-')
}

pub(crate) fn value_type_to_column_type(typ: Type) -> Option<ColumnType> {
    match typ {
        Type::Str => Some(ColumnType::Str),
        Type::U64 => Some(ColumnType::U64),
        Type::I64 => Some(ColumnType::I64),
        Type::F64 => Some(ColumnType::F64),
        Type::Bool => Some(ColumnType::Bool),
        Type::Date => Some(ColumnType::DateTime),
        Type::Facet => Some(ColumnType::Str),
        Type::Bytes => Some(ColumnType::Bytes),
        Type::IpAddr => Some(ColumnType::IpAddr),
        Type::Json => None,
    }
}

#[cfg(test)]
mod tests {

    use super::is_valid_field_name;

    #[test]
    fn test_is_valid_name() {
        assert!(is_valid_field_name("シャボン玉"));
        assert!(!is_valid_field_name("-fieldname"));
        assert!(!is_valid_field_name(""));
    }
}
