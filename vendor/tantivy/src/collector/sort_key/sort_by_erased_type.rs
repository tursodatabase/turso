use columnar::{ColumnType, MonotonicallyMappableToU64};

use crate::collector::sort_key::{
    NaturalComparator, SortByBytes, SortBySimilarityScore, SortByStaticFastValue, SortByString,
};
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::fastfield::FastFieldNotAvailableError;
use crate::schema::OwnedValue;
use crate::{DateTime, DocId, Score};

/// Sort by the boxed / OwnedValue representation of either a fast field, or of the score.
///
/// Using the OwnedValue representation allows for type erasure, and can be useful when sort orders
/// are not known until runtime. But it comes with a performance cost: wherever possible, prefer to
/// use a SortKeyComputer implementation with a known-type at compile time.
#[derive(Debug, Clone)]
pub enum SortByErasedType {
    /// Sort by a fast field
    Field(String),
    /// Sort by score
    Score,
}

impl SortByErasedType {
    /// Creates a new sort key computer which will sort by the given fast field column, with type
    /// erasure.
    pub fn for_field(column_name: impl ToString) -> Self {
        Self::Field(column_name.to_string())
    }

    /// Creates a new sort key computer which will sort by score, with type erasure.
    pub fn for_score() -> Self {
        Self::Score
    }
}

trait ErasedSegmentSortKeyComputer: Send + Sync {
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Option<u64>;
    fn convert_segment_sort_key(&self, sort_key: Option<u64>) -> OwnedValue;
}

struct ErasedSegmentSortKeyComputerWrapper<C, F> {
    inner: C,
    converter: F,
}

impl<C, F> ErasedSegmentSortKeyComputer for ErasedSegmentSortKeyComputerWrapper<C, F>
where
    C: SegmentSortKeyComputer<SegmentSortKey = Option<u64>> + Send + Sync,
    F: Fn(C::SortKey) -> OwnedValue + Send + Sync + 'static,
{
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Option<u64> {
        self.inner.segment_sort_key(doc, score)
    }

    fn convert_segment_sort_key(&self, sort_key: Option<u64>) -> OwnedValue {
        let val = self.inner.convert_segment_sort_key(sort_key);
        (self.converter)(val)
    }
}

struct ScoreSegmentSortKeyComputer {
    segment_computer: SortBySimilarityScore,
}

impl ErasedSegmentSortKeyComputer for ScoreSegmentSortKeyComputer {
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Option<u64> {
        let score_value: f64 = self.segment_computer.segment_sort_key(doc, score).into();
        Some(score_value.to_u64())
    }

    fn convert_segment_sort_key(&self, sort_key: Option<u64>) -> OwnedValue {
        let score_value: u64 = sort_key.expect("This implementation always produces a score.");
        OwnedValue::F64(f64::from_u64(score_value))
    }
}

impl SortKeyComputer for SortByErasedType {
    type SortKey = OwnedValue;
    type Child = ErasedColumnSegmentSortKeyComputer;
    type Comparator = NaturalComparator;

    fn requires_scoring(&self) -> bool {
        matches!(self, Self::Score)
    }

    fn segment_sort_key_computer(
        &self,
        segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let inner: Box<dyn ErasedSegmentSortKeyComputer> = match self {
            Self::Field(column_name) => {
                let fast_fields = segment_reader.fast_fields();
                // TODO: We currently double-open the column to avoid relying on the implementation
                // details of `SortByString` or `SortByStaticFastValue`. Once
                // https://github.com/quickwit-oss/tantivy/issues/2776 is resolved, we should
                // consider directly constructing the appropriate `SegmentSortKeyComputer` type for
                // the column that we open here.
                let (_column, column_type) =
                    fast_fields.u64_lenient(column_name)?.ok_or_else(|| {
                        FastFieldNotAvailableError {
                            field_name: column_name.to_owned(),
                        }
                    })?;

                match column_type {
                    ColumnType::Str => {
                        let computer = SortByString::for_field(column_name);
                        let inner = computer.segment_sort_key_computer(segment_reader)?;
                        Box::new(ErasedSegmentSortKeyComputerWrapper {
                            inner,
                            converter: |val: Option<String>| {
                                val.map(OwnedValue::Str).unwrap_or(OwnedValue::Null)
                            },
                        })
                    }
                    ColumnType::Bytes => {
                        let computer = SortByBytes::for_field(column_name);
                        let inner = computer.segment_sort_key_computer(segment_reader)?;
                        Box::new(ErasedSegmentSortKeyComputerWrapper {
                            inner,
                            converter: |val: Option<Vec<u8>>| {
                                val.map(OwnedValue::Bytes).unwrap_or(OwnedValue::Null)
                            },
                        })
                    }
                    ColumnType::U64 => {
                        let computer = SortByStaticFastValue::<u64>::for_field(column_name);
                        let inner = computer.segment_sort_key_computer(segment_reader)?;
                        Box::new(ErasedSegmentSortKeyComputerWrapper {
                            inner,
                            converter: |val: Option<u64>| {
                                val.map(OwnedValue::U64).unwrap_or(OwnedValue::Null)
                            },
                        })
                    }
                    ColumnType::I64 => {
                        let computer = SortByStaticFastValue::<i64>::for_field(column_name);
                        let inner = computer.segment_sort_key_computer(segment_reader)?;
                        Box::new(ErasedSegmentSortKeyComputerWrapper {
                            inner,
                            converter: |val: Option<i64>| {
                                val.map(OwnedValue::I64).unwrap_or(OwnedValue::Null)
                            },
                        })
                    }
                    ColumnType::F64 => {
                        let computer = SortByStaticFastValue::<f64>::for_field(column_name);
                        let inner = computer.segment_sort_key_computer(segment_reader)?;
                        Box::new(ErasedSegmentSortKeyComputerWrapper {
                            inner,
                            converter: |val: Option<f64>| {
                                val.map(OwnedValue::F64).unwrap_or(OwnedValue::Null)
                            },
                        })
                    }
                    ColumnType::Bool => {
                        let computer = SortByStaticFastValue::<bool>::for_field(column_name);
                        let inner = computer.segment_sort_key_computer(segment_reader)?;
                        Box::new(ErasedSegmentSortKeyComputerWrapper {
                            inner,
                            converter: |val: Option<bool>| {
                                val.map(OwnedValue::Bool).unwrap_or(OwnedValue::Null)
                            },
                        })
                    }
                    ColumnType::DateTime => {
                        let computer = SortByStaticFastValue::<DateTime>::for_field(column_name);
                        let inner = computer.segment_sort_key_computer(segment_reader)?;
                        Box::new(ErasedSegmentSortKeyComputerWrapper {
                            inner,
                            converter: |val: Option<DateTime>| {
                                val.map(OwnedValue::Date).unwrap_or(OwnedValue::Null)
                            },
                        })
                    }
                    column_type => {
                        return Err(crate::TantivyError::SchemaError(format!(
                            "Field `{}` is of type {column_type:?}, which is not supported for \
                             sorting by owned value yet.",
                            column_name
                        )))
                    }
                }
            }
            Self::Score => Box::new(ScoreSegmentSortKeyComputer {
                segment_computer: SortBySimilarityScore,
            }),
        };
        Ok(ErasedColumnSegmentSortKeyComputer { inner })
    }
}

pub struct ErasedColumnSegmentSortKeyComputer {
    inner: Box<dyn ErasedSegmentSortKeyComputer>,
}

impl SegmentSortKeyComputer for ErasedColumnSegmentSortKeyComputer {
    type SortKey = OwnedValue;
    type SegmentSortKey = Option<u64>;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Option<u64> {
        self.inner.segment_sort_key(doc, score)
    }

    fn convert_segment_sort_key(&self, segment_sort_key: Self::SegmentSortKey) -> OwnedValue {
        self.inner.convert_segment_sort_key(segment_sort_key)
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::sort_key::{ComparatorEnum, SortByErasedType};
    use crate::collector::TopDocs;
    use crate::query::AllQuery;
    use crate::schema::{OwnedValue, Schema, FAST, TEXT};
    use crate::Index;

    #[test]
    fn test_sort_by_owned_u64() {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc!(id_field => 10u64)).unwrap();
        writer.add_document(doc!(id_field => 2u64)).unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let collector = TopDocs::with_limit(10)
            .order_by((SortByErasedType::for_field("id"), ComparatorEnum::Natural));
        let top_docs = searcher.search(&AllQuery, &collector).unwrap();

        let values: Vec<OwnedValue> = top_docs.into_iter().map(|(key, _)| key).collect();

        assert_eq!(
            values,
            vec![OwnedValue::U64(10), OwnedValue::U64(2), OwnedValue::Null]
        );

        let collector = TopDocs::with_limit(10).order_by((
            SortByErasedType::for_field("id"),
            ComparatorEnum::ReverseNoneLower,
        ));
        let top_docs = searcher.search(&AllQuery, &collector).unwrap();

        let values: Vec<OwnedValue> = top_docs.into_iter().map(|(key, _)| key).collect();

        assert_eq!(
            values,
            vec![OwnedValue::U64(2), OwnedValue::U64(10), OwnedValue::Null]
        );
    }

    #[test]
    fn test_sort_by_owned_string() {
        let mut schema_builder = Schema::builder();
        let city_field = schema_builder.add_text_field("city", FAST | TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc!(city_field => "tokyo")).unwrap();
        writer.add_document(doc!(city_field => "austin")).unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let collector = TopDocs::with_limit(10).order_by((
            SortByErasedType::for_field("city"),
            ComparatorEnum::ReverseNoneLower,
        ));
        let top_docs = searcher.search(&AllQuery, &collector).unwrap();

        let values: Vec<OwnedValue> = top_docs.into_iter().map(|(key, _)| key).collect();

        assert_eq!(
            values,
            vec![
                OwnedValue::Str("austin".to_string()),
                OwnedValue::Str("tokyo".to_string()),
                OwnedValue::Null
            ]
        );
    }

    #[test]
    fn test_sort_by_owned_bytes() {
        let mut schema_builder = Schema::builder();
        let data_field = schema_builder.add_bytes_field("data", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(data_field => vec![0x03u8, 0x00]))
            .unwrap();
        writer
            .add_document(doc!(data_field => vec![0x01u8, 0x00]))
            .unwrap();
        writer
            .add_document(doc!(data_field => vec![0x02u8, 0x00]))
            .unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // Sort descending (Natural - highest first)
        let collector = TopDocs::with_limit(10)
            .order_by((SortByErasedType::for_field("data"), ComparatorEnum::Natural));
        let top_docs = searcher.search(&AllQuery, &collector).unwrap();

        let values: Vec<OwnedValue> = top_docs.into_iter().map(|(key, _)| key).collect();

        assert_eq!(
            values,
            vec![
                OwnedValue::Bytes(vec![0x03, 0x00]),
                OwnedValue::Bytes(vec![0x02, 0x00]),
                OwnedValue::Bytes(vec![0x01, 0x00]),
                OwnedValue::Null
            ]
        );

        // Sort ascending (ReverseNoneLower - lowest first, nulls last)
        let collector = TopDocs::with_limit(10).order_by((
            SortByErasedType::for_field("data"),
            ComparatorEnum::ReverseNoneLower,
        ));
        let top_docs = searcher.search(&AllQuery, &collector).unwrap();

        let values: Vec<OwnedValue> = top_docs.into_iter().map(|(key, _)| key).collect();

        assert_eq!(
            values,
            vec![
                OwnedValue::Bytes(vec![0x01, 0x00]),
                OwnedValue::Bytes(vec![0x02, 0x00]),
                OwnedValue::Bytes(vec![0x03, 0x00]),
                OwnedValue::Null
            ]
        );
    }

    #[test]
    fn test_sort_by_owned_reverse() {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc!(id_field => 10u64)).unwrap();
        writer.add_document(doc!(id_field => 2u64)).unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let collector = TopDocs::with_limit(10)
            .order_by((SortByErasedType::for_field("id"), ComparatorEnum::Reverse));
        let top_docs = searcher.search(&AllQuery, &collector).unwrap();

        let values: Vec<OwnedValue> = top_docs.into_iter().map(|(key, _)| key).collect();

        assert_eq!(
            values,
            vec![OwnedValue::Null, OwnedValue::U64(2), OwnedValue::U64(10)]
        );
    }

    #[test]
    fn test_sort_by_owned_score() {
        let mut schema_builder = Schema::builder();
        let body_field = schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc!(body_field => "a a")).unwrap();
        writer.add_document(doc!(body_field => "a")).unwrap();
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = crate::query::QueryParser::for_index(&index, vec![body_field]);
        let query = query_parser.parse_query("a").unwrap();

        // Sort by score descending (Natural)
        let collector = TopDocs::with_limit(10)
            .order_by((SortByErasedType::for_score(), ComparatorEnum::Natural));
        let top_docs = searcher.search(&query, &collector).unwrap();

        let values: Vec<f64> = top_docs
            .into_iter()
            .map(|(key, _)| match key {
                OwnedValue::F64(val) => val,
                _ => panic!("Wrong type {key:?}"),
            })
            .collect();

        assert_eq!(values.len(), 2);
        assert!(values[0] > values[1]);

        // Sort by score ascending (ReverseNoneLower)
        let collector = TopDocs::with_limit(10).order_by((
            SortByErasedType::for_score(),
            ComparatorEnum::ReverseNoneLower,
        ));
        let top_docs = searcher.search(&query, &collector).unwrap();

        let values: Vec<f64> = top_docs
            .into_iter()
            .map(|(key, _)| match key {
                OwnedValue::F64(val) => val,
                _ => panic!("Wrong type {key:?}"),
            })
            .collect();

        assert_eq!(values.len(), 2);
        assert!(values[0] < values[1]);
    }
}
