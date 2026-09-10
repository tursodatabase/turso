use columnar::{BytesColumn, Column};

use super::*;
use crate::query::{AllQuery, QueryParser};
use crate::schema::{Schema, FAST, TEXT};
use crate::time::format_description::well_known::Rfc3339;
use crate::time::OffsetDateTime;
use crate::{DateTime, DocAddress, Index, Searcher, TantivyDocument};

pub const TEST_COLLECTOR_WITH_SCORE: TestCollector = TestCollector {
    compute_score: true,
};

pub const TEST_COLLECTOR_WITHOUT_SCORE: TestCollector = TestCollector {
    compute_score: true,
};

#[test]
pub fn test_filter_collector() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT);
    let price = schema_builder.add_u64_field("price", FAST);
    let date = schema_builder.add_date_field("date", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    index_writer.add_document(doc!(title => "The Name of the Wind", price => 30_200u64, date => DateTime::from_utc(OffsetDateTime::parse("1898-04-09T00:00:00+00:00", &Rfc3339).unwrap())))?;
    index_writer.add_document(doc!(title => "The Diary of Muadib", price => 29_240u64, date => DateTime::from_utc(OffsetDateTime::parse("2020-04-09T00:00:00+00:00", &Rfc3339).unwrap())))?;
    index_writer.add_document(doc!(title => "The Diary of Anne Frank", price => 18_240u64, date => DateTime::from_utc(OffsetDateTime::parse("2019-04-20T00:00:00+00:00", &Rfc3339).unwrap())))?;
    index_writer.add_document(doc!(title => "A Dairy Cow", price => 21_240u64, date => DateTime::from_utc(OffsetDateTime::parse("2019-04-09T00:00:00+00:00", &Rfc3339).unwrap())))?;
    index_writer.add_document(doc!(title => "The Diary of a Young Girl", price => 20_120u64, date => DateTime::from_utc(OffsetDateTime::parse("2018-04-09T00:00:00+00:00", &Rfc3339).unwrap())))?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![title]);
    let query = query_parser.parse_query("diary")?;
    let filter_some_collector = FilterCollector::new(
        "price".to_string(),
        &|value: u64| value > 20_120u64,
        TopDocs::with_limit(2).order_by_score(),
    );
    let top_docs = searcher.search(&query, &filter_some_collector)?;

    assert_eq!(top_docs.len(), 1);
    assert_eq!(top_docs[0].1, DocAddress::new(0, 1));

    let filter_all_collector: FilterCollector<_, _, u64> = FilterCollector::new(
        "price".to_string(),
        &|value| value < 5u64,
        TopDocs::with_limit(2).order_by_score(),
    );
    let filtered_top_docs = searcher.search(&query, &filter_all_collector).unwrap();

    assert_eq!(filtered_top_docs.len(), 0);

    fn date_filter(value: DateTime) -> bool {
        (value.into_utc() - OffsetDateTime::parse("2019-04-09T00:00:00+00:00", &Rfc3339).unwrap())
            .whole_weeks()
            > 0
    }

    let filter_dates_collector = FilterCollector::new(
        "date".to_string(),
        &date_filter,
        TopDocs::with_limit(5).order_by_score(),
    );
    let filtered_date_docs = searcher.search(&query, &filter_dates_collector)?;

    assert_eq!(filtered_date_docs.len(), 2);
    Ok(())
}

/// Stores all of the doc ids.
/// This collector is only used for tests.
/// It is unusable in practise, as it does
/// not store the segment ordinals
pub struct TestCollector {
    pub compute_score: bool,
}

pub struct TestSegmentCollector {
    segment_id: SegmentOrdinal,
    fruit: TestFruit,
}

#[derive(Default)]
pub struct TestFruit {
    docs: Vec<DocAddress>,
    scores: Vec<Score>,
}

impl TestFruit {
    /// Return the list of matching documents exhaustively.
    pub fn docs(&self) -> &[DocAddress] {
        &self.docs[..]
    }
    pub fn scores(&self) -> &[Score] {
        &self.scores[..]
    }
}

impl Collector for TestCollector {
    type Fruit = TestFruit;
    type Child = TestSegmentCollector;

    fn for_segment(
        &self,
        segment_id: SegmentOrdinal,
        _reader: &SegmentReader,
    ) -> crate::Result<TestSegmentCollector> {
        Ok(TestSegmentCollector {
            segment_id,
            fruit: TestFruit::default(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.compute_score
    }

    fn merge_fruits(&self, mut children: Vec<TestFruit>) -> crate::Result<TestFruit> {
        children.sort_by_key(|fruit| {
            if fruit.docs().is_empty() {
                0
            } else {
                fruit.docs()[0].segment_ord
            }
        });
        let mut docs = vec![];
        let mut scores = vec![];
        for child in children {
            docs.extend(child.docs());
            scores.extend(child.scores);
        }
        Ok(TestFruit { docs, scores })
    }
}

impl SegmentCollector for TestSegmentCollector {
    type Fruit = TestFruit;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.fruit.docs.push(DocAddress::new(self.segment_id, doc));
        self.fruit.scores.push(score);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        self.fruit
    }
}

/// Collects in order all of the fast fields for all of the
/// doc in the `DocSet`
///
/// This collector is mainly useful for tests.
pub struct FastFieldTestCollector {
    field: String,
}

pub struct FastFieldSegmentCollector {
    vals: Vec<u64>,
    reader: Column,
}

impl FastFieldTestCollector {
    pub fn for_field(field: impl ToString) -> FastFieldTestCollector {
        FastFieldTestCollector {
            field: field.to_string(),
        }
    }
}

impl Collector for FastFieldTestCollector {
    type Fruit = Vec<u64>;
    type Child = FastFieldSegmentCollector;

    fn for_segment(
        &self,
        _: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> crate::Result<FastFieldSegmentCollector> {
        let reader = segment_reader
            .fast_fields()
            .u64(&self.field)
            .expect("Requested field is not a fast field.");
        Ok(FastFieldSegmentCollector {
            vals: Vec::new(),
            reader,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Vec<u64>>) -> crate::Result<Vec<u64>> {
        Ok(children.into_iter().flat_map(|v| v.into_iter()).collect())
    }
}

impl SegmentCollector for FastFieldSegmentCollector {
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        self.vals.extend(self.reader.values_for_doc(doc));
    }

    fn harvest(self) -> Vec<u64> {
        self.vals
    }
}

/// Collects in order all of the fast field bytes for all of the
/// docs in the `DocSet`
///
/// This collector is mainly useful for tests.
/// It is very slow.
pub struct BytesFastFieldTestCollector {
    field: String,
}

pub struct BytesFastFieldSegmentCollector {
    vals: Vec<u8>,
    column_opt: Option<BytesColumn>,
    buffer: Vec<u8>,
}

impl BytesFastFieldTestCollector {
    pub fn for_field(field: impl ToString) -> BytesFastFieldTestCollector {
        BytesFastFieldTestCollector {
            field: field.to_string(),
        }
    }
}

impl Collector for BytesFastFieldTestCollector {
    type Fruit = Vec<u8>;
    type Child = BytesFastFieldSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<BytesFastFieldSegmentCollector> {
        let column_opt = segment_reader.fast_fields().bytes(&self.field)?;
        Ok(BytesFastFieldSegmentCollector {
            vals: Vec::new(),
            column_opt,
            buffer: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Vec<u8>>) -> crate::Result<Vec<u8>> {
        Ok(children.into_iter().flat_map(|c| c.into_iter()).collect())
    }
}

impl SegmentCollector for BytesFastFieldSegmentCollector {
    type Fruit = Vec<u8>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if let Some(column) = self.column_opt.as_ref() {
            for term_ord in column.term_ords(doc) {
                let (vals, buffer) = (&mut self.vals, &mut self.buffer);
                if column.ord_to_bytes(term_ord, buffer).unwrap() {
                    vals.extend(&buffer[..]);
                }
            }
        }
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        self.vals
    }
}

fn make_test_searcher() -> crate::Result<Searcher> {
    let schema = Schema::builder().build();
    let index = Index::create_in_ram(schema);
    let mut index_writer = index.writer_for_tests()?;
    index_writer.add_document(TantivyDocument::default())?;
    index_writer.add_document(TantivyDocument::default())?;
    index_writer.commit()?;
    Ok(index.reader()?.searcher())
}

#[test]
fn test_option_collector_some() -> crate::Result<()> {
    let searcher = make_test_searcher()?;
    let counts = searcher.search(&AllQuery, &Some(Count))?;
    assert_eq!(counts, Some(2));
    Ok(())
}

#[test]
fn test_option_collector_none() -> crate::Result<()> {
    let searcher = make_test_searcher()?;
    let none_collector: Option<Count> = None;
    let counts = searcher.search(&AllQuery, &none_collector)?;
    assert_eq!(counts, None);
    Ok(())
}
