//! Representations for the space usage of various parts of a Tantivy index.
//!
//! This can be used programmatically, and will also be exposed in a human readable fashion in
//! tantivy-cli.
//!
//! One important caveat for all of this functionality is that none of it currently takes
//! storage-level details into consideration. For example, if your file system block size is 4096
//! bytes, we can under-count actual resultant space usage by up to 4095 bytes per file.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use columnar::ColumnSpaceUsage;
use common::ByteCount;
use serde::{Deserialize, Serialize};

use crate::index::SegmentComponent;

/// Enum containing any of the possible space usage results for segment components.
pub enum ComponentSpaceUsage {
    /// Data is stored per field in a uniform way
    PerField(PerFieldSpaceUsage),
    /// Data is stored in separate pieces in the store
    Store(StoreSpaceUsage),
    /// Some sort of raw byte count
    Basic(ByteCount),
}

/// Represents combined space usage of an entire searcher and its component segments.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearcherSpaceUsage {
    segments: Vec<SegmentSpaceUsage>,
    total: ByteCount,
}

impl SearcherSpaceUsage {
    pub(crate) fn new() -> SearcherSpaceUsage {
        SearcherSpaceUsage {
            segments: Vec::new(),
            total: Default::default(),
        }
    }

    /// Add a segment, to `self`.
    /// Performs no deduplication or other intelligence.
    pub(crate) fn add_segment(&mut self, segment: SegmentSpaceUsage) {
        self.total += segment.total();
        self.segments.push(segment);
    }

    /// Per segment space usage
    pub fn segments(&self) -> &[SegmentSpaceUsage] {
        &self.segments[..]
    }

    /// Returns total byte usage of this searcher, including all large subcomponents.
    /// Does not account for smaller things like `meta.json`.
    pub fn total(&self) -> ByteCount {
        self.total
    }
}

/// Represents combined space usage for all of the large components comprising a segment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentSpaceUsage {
    num_docs: u32,

    termdict: PerFieldSpaceUsage,
    postings: PerFieldSpaceUsage,
    positions: PerFieldSpaceUsage,
    fast_fields: PerFieldSpaceUsage,
    fieldnorms: PerFieldSpaceUsage,

    store: StoreSpaceUsage,

    deletes: ByteCount,

    total: ByteCount,
}

impl SegmentSpaceUsage {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        num_docs: u32,
        termdict: PerFieldSpaceUsage,
        postings: PerFieldSpaceUsage,
        positions: PerFieldSpaceUsage,
        fast_fields: PerFieldSpaceUsage,
        fieldnorms: PerFieldSpaceUsage,
        store: StoreSpaceUsage,
        deletes: ByteCount,
    ) -> SegmentSpaceUsage {
        let total = termdict.total()
            + postings.total()
            + positions.total()
            + fast_fields.total()
            + fieldnorms.total()
            + store.total()
            + deletes;
        SegmentSpaceUsage {
            num_docs,
            termdict,
            postings,
            positions,
            fast_fields,
            fieldnorms,
            store,
            deletes,
            total,
        }
    }

    /// Space usage for the given component
    ///
    /// Clones the underlying data.
    /// Use the components directly if this is somehow in performance critical code.
    pub fn component(&self, component: SegmentComponent) -> ComponentSpaceUsage {
        use self::ComponentSpaceUsage::*;
        use crate::index::SegmentComponent::*;
        match component {
            Postings => PerField(self.postings().clone()),
            Positions => PerField(self.positions().clone()),
            FastFields => PerField(self.fast_fields().clone()),
            FieldNorms => PerField(self.fieldnorms().clone()),
            Terms => PerField(self.termdict().clone()),
            SegmentComponent::Store => ComponentSpaceUsage::Store(self.store().clone()),
            Delete => Basic(self.deletes()),
        }
    }

    /// Num docs in segment
    pub fn num_docs(&self) -> u32 {
        self.num_docs
    }

    /// Space usage for term dictionary
    pub fn termdict(&self) -> &PerFieldSpaceUsage {
        &self.termdict
    }

    /// Space usage for postings list
    pub fn postings(&self) -> &PerFieldSpaceUsage {
        &self.postings
    }

    /// Space usage for positions
    pub fn positions(&self) -> &PerFieldSpaceUsage {
        &self.positions
    }

    /// Space usage for fast fields
    pub fn fast_fields(&self) -> &PerFieldSpaceUsage {
        &self.fast_fields
    }

    /// Space usage for field norms
    pub fn fieldnorms(&self) -> &PerFieldSpaceUsage {
        &self.fieldnorms
    }

    /// Space usage for stored documents
    pub fn store(&self) -> &StoreSpaceUsage {
        &self.store
    }

    /// Space usage for document deletions
    pub fn deletes(&self) -> ByteCount {
        self.deletes
    }

    /// Total space usage in bytes for this segment.
    pub fn total(&self) -> ByteCount {
        self.total
    }
}

/// Represents space usage for the Store for this segment.
///
/// This is composed of two parts.
/// `data` represents the compressed data itself.
/// `offsets` represents a lookup to find the start of a block
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoreSpaceUsage {
    data: ByteCount,
    offsets: ByteCount,
}

impl StoreSpaceUsage {
    pub(crate) fn new(data: ByteCount, offsets: ByteCount) -> StoreSpaceUsage {
        StoreSpaceUsage { data, offsets }
    }

    /// Space usage for the data part of the store
    pub fn data_usage(&self) -> ByteCount {
        self.data
    }

    /// Space usage for the offsets part of the store (doc ID -> offset)
    pub fn offsets_usage(&self) -> ByteCount {
        self.offsets
    }

    /// Total space usage in bytes for this Store
    pub fn total(&self) -> ByteCount {
        self.data + self.offsets
    }
}

/// Represents space usage for all of the (field, index) pairs that appear in a `CompositeFile`.
///
/// A field can appear with a single index (typically 0) or with multiple indexes.
/// Multiple indexes are used to handle variable length things, where
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerFieldSpaceUsage {
    fields: BTreeMap<String, FieldUsage>,
    total: ByteCount,
}

impl PerFieldSpaceUsage {
    pub(crate) fn new(fields: Vec<FieldUsage>) -> PerFieldSpaceUsage {
        let mut total = ByteCount::default();
        let mut field_usage_map: BTreeMap<String, FieldUsage> = BTreeMap::new();
        for field_usage in fields {
            total += field_usage.total();
            let field_name = field_usage.field_name().to_string();
            match field_usage_map.entry(field_name) {
                Entry::Vacant(entry) => {
                    entry.insert(field_usage);
                }
                Entry::Occupied(mut entry) => {
                    entry.get_mut().merge(field_usage);
                }
            }
        }
        PerFieldSpaceUsage {
            fields: field_usage_map,
            total,
        }
    }

    /// Per field space usage
    pub fn fields(&self) -> impl Iterator<Item = &FieldUsage> {
        self.fields.values()
    }

    /// Bytes used by the represented file
    pub fn total(&self) -> ByteCount {
        self.total
    }
}

/// Represents space usage of a given field, breaking it down into the (field, index) pairs that
/// comprise it.
///
/// See documentation for [`PerFieldSpaceUsage`] for slightly more information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldUsage {
    field_name: String,
    num_bytes: ByteCount,
    /// A field can be composed of more than one piece.
    /// These pieces are indexed by arbitrary numbers starting at zero.
    /// `self.num_bytes` includes all of `self.sub_num_bytes`.
    sub_num_bytes: Vec<Option<ByteCount>>,
    /// Space usage of the column for fast fields, if relevant.
    column_space_usage: Option<ColumnSpaceUsage>,
}

impl FieldUsage {
    pub(crate) fn empty(field_name: impl Into<String>) -> FieldUsage {
        FieldUsage {
            field_name: field_name.into(),
            num_bytes: Default::default(),
            sub_num_bytes: Vec::new(),
            column_space_usage: None,
        }
    }

    pub(crate) fn add_field_idx(&mut self, idx: usize, size: ByteCount) {
        if self.sub_num_bytes.len() < idx + 1 {
            self.sub_num_bytes.resize(idx + 1, None);
        }
        assert!(self.sub_num_bytes[idx].is_none());
        self.sub_num_bytes[idx] = Some(size);
        self.num_bytes += size
    }

    pub(crate) fn set_column_usage(&mut self, column_space_usage: ColumnSpaceUsage) {
        self.num_bytes += column_space_usage.total_num_bytes();
        self.column_space_usage = Some(column_space_usage);
    }

    /// Field
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Space usage for each index
    pub fn sub_num_bytes(&self) -> &[Option<ByteCount>] {
        &self.sub_num_bytes[..]
    }

    /// Returns the number of bytes used by the column payload, if the field is columnar.
    pub fn column_num_bytes(&self) -> Option<ByteCount> {
        self.column_space_usage
            .as_ref()
            .map(ColumnSpaceUsage::column_num_bytes)
    }

    /// Returns the number of bytes used by the dictionary for dictionary-encoded columns.
    pub fn dictionary_num_bytes(&self) -> Option<ByteCount> {
        self.column_space_usage
            .as_ref()
            .and_then(ColumnSpaceUsage::dictionary_num_bytes)
    }

    /// Returns the space usage of the column, if any.
    pub fn column_space_usage(&self) -> Option<&ColumnSpaceUsage> {
        self.column_space_usage.as_ref()
    }

    /// Total bytes used for this field in this context
    pub fn total(&self) -> ByteCount {
        self.num_bytes
    }

    fn merge(&mut self, other: FieldUsage) {
        assert_eq!(self.field_name, other.field_name);
        self.num_bytes += other.num_bytes;
        if other.sub_num_bytes.len() > self.sub_num_bytes.len() {
            self.sub_num_bytes.resize(other.sub_num_bytes.len(), None);
        }
        for (idx, num_bytes_opt) in other.sub_num_bytes.into_iter().enumerate() {
            if let Some(num_bytes) = num_bytes_opt {
                match self.sub_num_bytes[idx] {
                    Some(existing) => self.sub_num_bytes[idx] = Some(existing + num_bytes),
                    None => self.sub_num_bytes[idx] = Some(num_bytes),
                }
            }
        }
        self.column_space_usage =
            merge_column_space_usage(self.column_space_usage.take(), other.column_space_usage);
    }
}

fn merge_column_space_usage(
    left: Option<ColumnSpaceUsage>,
    right: Option<ColumnSpaceUsage>,
) -> Option<ColumnSpaceUsage> {
    match (left, right) {
        (Some(lhs), Some(rhs)) => Some(lhs.merge(&rhs)),
        (Some(space), None) | (None, Some(space)) => Some(space),
        (None, None) => None,
    }
}

#[cfg(test)]
mod test {
    use crate::index::Index;
    use crate::schema::{Schema, FAST, INDEXED, STORED, TEXT};
    use crate::space_usage::PerFieldSpaceUsage;
    use crate::{IndexWriter, Term};

    #[test]
    fn test_empty() {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let searcher_space_usage = searcher.space_usage().unwrap();
        assert_eq!(searcher_space_usage.total(), 0u64);
    }

    fn expect_single_field(
        field_space: &PerFieldSpaceUsage,
        field: &str,
        min_size: u64,
        max_size: u64,
    ) {
        assert!(field_space.total() >= min_size);
        assert!(field_space.total() <= max_size);
        assert_eq!(
            vec![(field.to_string(), field_space.total())],
            field_space
                .fields()
                .map(|usage| (usage.field_name().to_string(), usage.total()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_fast_indexed() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_u64_field("name", FAST | INDEXED);
        let schema = schema_builder.build();
        let field_name = schema.get_field_name(name).to_string();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(name => 1u64))?;
            index_writer.add_document(doc!(name => 2u64))?;
            index_writer.add_document(doc!(name => 10u64))?;
            index_writer.add_document(doc!(name => 20u64))?;
            index_writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let searcher_space_usage = searcher.space_usage()?;
        assert!(searcher_space_usage.total() > 0);
        assert_eq!(1, searcher_space_usage.segments().len());

        let segment = &searcher_space_usage.segments()[0];
        assert!(segment.total() > 0);

        assert_eq!(4, segment.num_docs());

        expect_single_field(segment.termdict(), &field_name, 1, 512);
        expect_single_field(segment.postings(), &field_name, 1, 512);
        assert_eq!(segment.positions().total(), 0);
        expect_single_field(segment.fast_fields(), &field_name, 1, 512);
        expect_single_field(segment.fieldnorms(), &field_name, 1, 512);
        // TODO: understand why the following fails
        //        assert_eq!(0, segment.store().total());
        assert_eq!(segment.deletes(), 0);
        Ok(())
    }

    #[test]
    fn test_text() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", TEXT);
        let schema = schema_builder.build();
        let field_name = schema.get_field_name(name).to_string();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(name => "hi"))?;
            index_writer.add_document(doc!(name => "this is a test"))?;
            index_writer.add_document(
                doc!(name => "some more documents with some word overlap with the other test"),
            )?;
            index_writer.add_document(doc!(name => "hello hi goodbye"))?;
            index_writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let searcher_space_usage = searcher.space_usage()?;
        assert!(searcher_space_usage.total() > 0);
        assert_eq!(1, searcher_space_usage.segments().len());

        let segment = &searcher_space_usage.segments()[0];
        assert!(segment.total() > 0);

        assert_eq!(4, segment.num_docs());

        expect_single_field(segment.termdict(), &field_name, 1, 512);
        expect_single_field(segment.postings(), &field_name, 1, 512);
        expect_single_field(segment.positions(), &field_name, 1, 512);
        assert_eq!(segment.fast_fields().total(), 0);
        expect_single_field(segment.fieldnorms(), &field_name, 1, 512);
        // TODO: understand why the following fails
        //        assert_eq!(0, segment.store().total());
        assert_eq!(segment.deletes(), 0);
        Ok(())
    }

    #[test]
    fn test_store() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(name => "hi"))?;
            index_writer.add_document(doc!(name => "this is a test"))?;
            index_writer.add_document(
                doc!(name => "some more documents with some word overlap with the other test"),
            )?;
            index_writer.add_document(doc!(name => "hello hi goodbye"))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let searcher_space_usage = searcher.space_usage()?;
        assert!(searcher_space_usage.total() > 0);
        assert_eq!(1, searcher_space_usage.segments().len());

        let segment = &searcher_space_usage.segments()[0];
        assert!(segment.total() > 0);

        assert_eq!(4, segment.num_docs());

        assert_eq!(segment.termdict().total(), 0);
        assert!(segment.termdict().fields().next().is_none());
        assert_eq!(segment.postings().total(), 0);
        assert!(segment.postings().fields().next().is_none());
        assert_eq!(segment.positions().total(), 0);
        assert!(segment.positions().fields().next().is_none());
        assert_eq!(segment.fast_fields().total(), 0);
        assert!(segment.fast_fields().fields().next().is_none());
        assert_eq!(segment.fieldnorms().total(), 0);
        assert!(segment.fieldnorms().fields().next().is_none());
        assert!(segment.store().total() > 0);
        assert!(segment.store().total() < 512);
        assert_eq!(segment.deletes(), 0);
        Ok(())
    }

    #[test]
    fn test_deletes() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_u64_field("name", INDEXED);
        let schema = schema_builder.build();
        let field_name = schema.get_field_name(name).to_string();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(name => 1u64))?;
            index_writer.add_document(doc!(name => 2u64))?;
            index_writer.add_document(doc!(name => 3u64))?;
            index_writer.add_document(doc!(name => 4u64))?;
            index_writer.commit()?;
        }

        {
            let mut index_writer2: IndexWriter = index.writer(50_000_000)?;
            index_writer2.delete_term(Term::from_field_u64(name, 2u64));
            index_writer2.delete_term(Term::from_field_u64(name, 3u64));
            // ok, now we should have a deleted doc
            index_writer2.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let searcher_space_usage = searcher.space_usage()?;
        assert!(searcher_space_usage.total() > 0);
        assert_eq!(1, searcher_space_usage.segments().len());

        let segment_space_usage = &searcher_space_usage.segments()[0];
        assert!(segment_space_usage.total() > 0);

        assert_eq!(2, segment_space_usage.num_docs());

        expect_single_field(segment_space_usage.termdict(), &field_name, 1, 512);
        expect_single_field(segment_space_usage.postings(), &field_name, 1, 512);
        assert_eq!(segment_space_usage.positions().total(), 0u64);
        assert_eq!(segment_space_usage.fast_fields().total(), 0u64);
        expect_single_field(segment_space_usage.fieldnorms(), &field_name, 1, 512);
        assert!(segment_space_usage.deletes() > 0);
        Ok(())
    }
}
