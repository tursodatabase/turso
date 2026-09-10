use std::io;
use std::ops::Bound;

use common::bounds::{map_bound, BoundsRange};
use common::BitSet;

use super::range_query_fastfield::FastFieldRangeWeight;
use crate::index::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::range_query::is_type_valid_for_fastfield_range_query;
use crate::query::{BitSetDocSet, ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::schema::{Field, IndexRecordOption, Term, Type};
use crate::termdict::{TermDictionary, TermStreamer};
use crate::{DocId, Score};

/// `RangeQuery` matches all documents that have at least one term within a defined range.
///
/// Matched document will all get a constant `Score` of one.
///
/// # Implementation
///
/// ## Default
/// The default implementation collects all documents _upfront_ into a `BitSet`.
/// This is done by iterating over the terms within the range and loading all docs for each
/// `TermInfo` from the inverted index (posting list) and put them into a `BitSet`.
/// Depending on the number of terms matched, this is a potentially expensive operation.
///
/// ## IP fast field
/// For IP fast fields a custom variant is used, by scanning the fast field. Unlike the default
/// variant we can walk in a lazy fashion over it, since the fastfield is implicit orderered by
/// DocId.
///
///
/// # Example
///
/// ```rust
/// use tantivy::collector::Count;
/// use tantivy::query::RangeQuery;
/// use tantivy::Term;
/// use tantivy::schema::{Schema, INDEXED};
/// use tantivy::{doc, Index, IndexWriter};
/// use std::ops::Bound;
/// # fn test() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let year_field = schema_builder.add_u64_field("year", INDEXED);
/// let schema = schema_builder.build();
///
/// let index = Index::create_in_ram(schema);
/// let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
/// for year in 1950u64..2017u64 {
///     let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
///     for _ in 0..num_docs_within_year {
///       index_writer.add_document(doc!(year_field => year))?;
///     }
/// }
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
/// let docs_in_the_sixties = RangeQuery::new(
///     Bound::Included(Term::from_field_u64(year_field, 1960)),
///     Bound::Excluded(Term::from_field_u64(year_field, 1970)),
/// );
/// let num_60s_books = searcher.search(&docs_in_the_sixties, &Count)?;
/// assert_eq!(num_60s_books, 2285);
/// Ok(())
/// # }
/// # assert!(test().is_ok());
/// ```
#[derive(Clone, Debug)]
pub struct RangeQuery {
    bounds: BoundsRange<Term>,
}

impl RangeQuery {
    /// Creates a new `RangeQuery` from bounded start and end terms.
    ///
    /// If the value type is not correct, something may go terribly wrong when
    /// the `Weight` object is created.
    pub fn new(lower_bound: Bound<Term>, upper_bound: Bound<Term>) -> RangeQuery {
        RangeQuery {
            bounds: BoundsRange::new(lower_bound, upper_bound),
        }
    }

    /// Field to search over
    pub fn field(&self) -> Field {
        self.get_term().field()
    }

    /// The value type of the field
    pub fn value_type(&self) -> Type {
        self.get_term().typ()
    }

    pub(crate) fn get_term(&self) -> &Term {
        self.bounds
            .get_inner()
            .expect("At least one bound must be set")
    }
}

impl Query for RangeQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let schema = enable_scoring.schema();
        let field_type = schema.get_field_entry(self.field()).field_type();

        if field_type.is_fast() && is_type_valid_for_fastfield_range_query(self.value_type()) {
            Ok(Box::new(FastFieldRangeWeight::new(self.bounds.clone())))
        } else {
            if field_type.is_json() {
                return Err(crate::TantivyError::InvalidArgument(
                    "RangeQuery on JSON is only supported for fast fields currently".to_string(),
                ));
            }
            Ok(Box::new(InvertedIndexRangeWeight::new(
                self.field(),
                &self.bounds.lower_bound,
                &self.bounds.upper_bound,
                None,
            )))
        }
    }
}

#[derive(Clone, Debug)]
/// `InvertedIndexRangeQuery` is the same as [RangeQuery] but only uses the inverted index
pub struct InvertedIndexRangeQuery {
    bounds: BoundsRange<Term>,
    limit: Option<u64>,
}
impl InvertedIndexRangeQuery {
    /// Create new `InvertedIndexRangeQuery`
    pub fn new(lower_bound: Bound<Term>, upper_bound: Bound<Term>) -> InvertedIndexRangeQuery {
        InvertedIndexRangeQuery {
            bounds: BoundsRange::new(lower_bound, upper_bound),
            limit: None,
        }
    }
    /// Limit the number of term the `RangeQuery` will go through.
    ///
    /// This does not limit the number of matching document, only the number of
    /// different terms that get matched.
    pub fn limit(&mut self, limit: u64) {
        self.limit = Some(limit);
    }
}

impl Query for InvertedIndexRangeQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let field = self
            .bounds
            .get_inner()
            .expect("At least one bound must be set")
            .field();

        Ok(Box::new(InvertedIndexRangeWeight::new(
            field,
            &self.bounds.lower_bound,
            &self.bounds.upper_bound,
            self.limit,
        )))
    }
}

/// Range weight on the inverted index
pub struct InvertedIndexRangeWeight {
    field: Field,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    limit: Option<u64>,
}

impl InvertedIndexRangeWeight {
    /// Creates a new RangeWeight
    ///
    /// Note: The limit is only enabled with the quickwit feature flag.
    pub fn new(
        field: Field,
        lower_bound: &Bound<Term>,
        upper_bound: &Bound<Term>,
        limit: Option<u64>,
    ) -> Self {
        let verify_and_unwrap_term = |val: &Term| val.serialized_value_bytes().to_owned();
        Self {
            field,
            lower_bound: map_bound(lower_bound, verify_and_unwrap_term),
            upper_bound: map_bound(upper_bound, verify_and_unwrap_term),
            limit,
        }
    }

    fn term_range<'a>(&self, term_dict: &'a TermDictionary) -> io::Result<TermStreamer<'a>> {
        use std::ops::Bound::*;
        let mut term_stream_builder = term_dict.range();
        term_stream_builder = match self.lower_bound {
            Included(ref term_val) => term_stream_builder.ge(term_val),
            Excluded(ref term_val) => term_stream_builder.gt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder = match self.upper_bound {
            Included(ref term_val) => term_stream_builder.le(term_val),
            Excluded(ref term_val) => term_stream_builder.lt(term_val),
            Unbounded => term_stream_builder,
        };
        #[cfg(feature = "quickwit")]
        if let Some(limit) = self.limit {
            term_stream_builder = term_stream_builder.limit(limit);
        }
        term_stream_builder.into_stream()
    }
}

impl Weight for InvertedIndexRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_range = self.term_range(term_dict)?;
        let mut processed_count = 0;
        while term_range.advance() {
            if let Some(limit) = self.limit {
                if limit <= processed_count {
                    break;
                }
            }
            processed_count += 1;
            let term_info = term_range.value();
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            loop {
                let docs = block_segment_postings.docs();
                if docs.is_empty() {
                    break;
                }
                for &doc in block_segment_postings.docs() {
                    doc_bitset.insert(doc);
                }
                block_segment_postings.advance();
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("RangeQuery", 1.0))
    }
}

#[cfg(test)]
mod tests {

    use std::net::IpAddr;
    use std::ops::Bound;
    use std::str::FromStr;

    use rand::seq::SliceRandom;

    use super::RangeQuery;
    use crate::collector::{Count, TopDocs};
    use crate::indexer::NoMergePolicy;
    use crate::query::range_query::fast_field_range_doc_set::RangeDocSet;
    use crate::query::range_query::range_query::InvertedIndexRangeQuery;
    use crate::query::{AllScorer, ConstScorer, EmptyScorer, EnableScoring, Query, QueryParser};
    use crate::schema::{
        Field, IntoIpv6Addr, Schema, TantivyDocument, FAST, INDEXED, STORED, TEXT,
    };
    use crate::{Index, IndexWriter, Term};

    #[test]
    fn test_range_query_simple() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let year_field = schema_builder.add_u64_field("year", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            for year in 1950u64..2017u64 {
                let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
                for _ in 0..num_docs_within_year {
                    index_writer.add_document(doc!(year_field => year))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let docs_in_the_sixties = InvertedIndexRangeQuery::new(
            Bound::Included(Term::from_field_u64(year_field, 1960)),
            Bound::Excluded(Term::from_field_u64(year_field, 1970)),
        );

        // ... or `1960..=1969` if inclusive range is enabled.
        let count = searcher.search(&docs_in_the_sixties, &Count)?;
        assert_eq!(count, 2285);
        Ok(())
    }

    #[test]
    fn test_range_query_with_limit() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let year_field = schema_builder.add_u64_field("year", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            for year in 1950u64..2017u64 {
                if year == 1963 {
                    continue;
                }
                let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
                for _ in 0..num_docs_within_year {
                    index_writer.add_document(doc!(year_field => year))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let mut docs_in_the_sixties = InvertedIndexRangeQuery::new(
            Bound::Included(Term::from_field_u64(year_field, 1960)),
            Bound::Excluded(Term::from_field_u64(year_field, 1970)),
        );
        docs_in_the_sixties.limit(5);

        // due to the limit and no docs in 1963, it's really only 1960..=1965
        let count = searcher.search(&docs_in_the_sixties, &Count)?;
        assert_eq!(count, 836);
        Ok(())
    }

    #[test]
    fn test_range_query() -> crate::Result<()> {
        let int_field: Field;
        let schema = {
            let mut schema_builder = Schema::builder();
            int_field = schema_builder.add_i64_field("intfield", INDEXED);
            schema_builder.build()
        };

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 60_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));

            for i in 1..100 {
                let mut doc = TantivyDocument::new();
                for j in 1..100 {
                    if i % j == 0 {
                        doc.add_i64(int_field, j as i64);
                    }
                }
                index_writer.add_document(doc)?;
                if i == 10 {
                    index_writer.commit()?;
                }
            }

            index_writer.commit()?;
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Included(Term::from_field_i64(int_field, 10)),
                Bound::Excluded(Term::from_field_i64(int_field, 11)),
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Included(Term::from_field_i64(int_field, 10)),
                Bound::Included(Term::from_field_i64(int_field, 11)),
            )),
            18
        );
        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Excluded(Term::from_field_i64(int_field, 9)),
                Bound::Included(Term::from_field_i64(int_field, 10)),
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Included(Term::from_field_i64(int_field, 9)),
                Bound::Unbounded
            )),
            91
        );
        Ok(())
    }

    #[test]
    fn test_range_float() -> crate::Result<()> {
        let float_field: Field;
        let schema = {
            let mut schema_builder = Schema::builder();
            float_field = schema_builder.add_f64_field("floatfield", INDEXED);
            schema_builder.build()
        };

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 60_000_000).unwrap();
            let mut docs = Vec::new();
            for i in 1..100 {
                let mut doc = TantivyDocument::new();
                for j in 1..100 {
                    if i % j == 0 {
                        doc.add_f64(float_field, j as f64);
                    }
                }
                docs.push(doc);
            }

            docs.shuffle(&mut rand::rng());
            let mut docs_it = docs.into_iter();
            for doc in (&mut docs_it).take(50) {
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
            for doc in docs_it {
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Included(Term::from_field_f64(float_field, 10.0)),
                Bound::Excluded(Term::from_field_f64(float_field, 11.0)),
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Included(Term::from_field_f64(float_field, 10.0)),
                Bound::Included(Term::from_field_f64(float_field, 11.0)),
            )),
            18
        );
        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Excluded(Term::from_field_f64(float_field, 9.0)),
                Bound::Included(Term::from_field_f64(float_field, 10.0)),
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new(
                Bound::Included(Term::from_field_f64(float_field, 9.0)),
                Bound::Unbounded
            )),
            91
        );
        Ok(())
    }

    #[test]
    fn test_bug_reproduce_range_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_i64_field("year", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests()?;
        let title = schema.get_field("title").unwrap();
        let year = schema.get_field("year").unwrap();
        index_writer.add_document(doc!(
          title => "hemoglobin blood",
          year => 1990_i64
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![title]);
        let query = query_parser.parse_query("hemoglobin AND year:[1970 TO 1990]")?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
        assert_eq!(top_docs.len(), 1);
        Ok(())
    }

    #[test]
    fn search_ip_range_test_posting_list() {
        search_ip_range_test_opt(false);
    }

    #[test]
    fn search_ip_range_test() {
        search_ip_range_test_opt(true);
    }

    fn search_ip_range_test_opt(with_fast_field: bool) {
        let mut schema_builder = Schema::builder();
        let ip_field = if with_fast_field {
            schema_builder.add_ip_addr_field("ip", INDEXED | STORED | FAST)
        } else {
            schema_builder.add_ip_addr_field("ip", INDEXED | STORED)
        };
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let ip_addr_1 = IpAddr::from_str("127.0.0.10").unwrap().into_ipv6_addr();
        let ip_addr_2 = IpAddr::from_str("127.0.0.20").unwrap().into_ipv6_addr();

        {
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            for _ in 0..1_000 {
                index_writer
                    .add_document(doc!(
                        ip_field => ip_addr_1,
                        text_field => "BLUBBER"
                    ))
                    .unwrap();
            }
            for _ in 0..1_000 {
                index_writer
                    .add_document(doc!(
                        ip_field => ip_addr_2,
                        text_field => "BLOBBER"
                    ))
                    .unwrap();
            }
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let get_num_hits = |query| {
            let (_top_docs, count) = searcher
                .search(&query, &(TopDocs::with_limit(10).order_by_score(), Count))
                .unwrap();
            count
        };
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        // Inclusive range
        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.1 TO 127.0.0.20]")),
            2000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.10 TO 127.0.0.20]")),
            2000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.11 TO 127.0.0.20]")),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.11 TO 127.0.0.19]")),
            0
        );

        assert_eq!(get_num_hits(query_from_text("ip:[127.0.0.11 TO *]")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:[127.0.0.21 TO *]")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:[* TO 127.0.0.9]")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:[* TO 127.0.0.10]")), 1000);

        // Exclusive range
        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.1 TO 127.0.0.20}")),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.1 TO 127.0.0.21}")),
            2000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.10 TO 127.0.0.20}")),
            0
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.11 TO 127.0.0.20}")),
            0
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.11 TO 127.0.0.19}")),
            0
        );

        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.11 TO *}")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.10 TO *}")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.21 TO *}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.20 TO *}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.19 TO *}")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:{* TO 127.0.0.9}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{* TO 127.0.0.10}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{* TO 127.0.0.11}")), 1000);

        // Inclusive/Exclusive range
        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.1 TO 127.0.0.20}")),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.1 TO 127.0.0.20]")),
            2000
        );

        // Intersection
        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLUBBER AND ip:[127.0.0.10 TO 127.0.0.10]"
            )),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLOBBER AND ip:[127.0.0.10 TO 127.0.0.10]"
            )),
            0
        );

        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLOBBER AND ip:[127.0.0.20 TO 127.0.0.20]"
            )),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLUBBER AND ip:[127.0.0.20 TO 127.0.0.20]"
            )),
            0
        );
    }

    #[test]
    fn test_range_query_simplified() {
        // This test checks that if the targeted column values are entirely
        // within the range, and the column is full, we end up with a AllScorer.
        let mut schema_builder = Schema::builder();
        let u64_field = schema_builder.add_u64_field("u64_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(u64_field=> 2u64)).unwrap();
        index_writer.add_document(doc!(u64_field=> 4u64)).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let make_term = |value: u64| Term::from_field_u64(u64_field, value);
        let make_scorer = move |lower_bound: Bound<u64>, upper_bound: Bound<u64>| {
            let lower_bound_term = lower_bound.map(make_term);
            let upper_bound_term = upper_bound.map(make_term);
            let range_query = RangeQuery::new(lower_bound_term, upper_bound_term);
            let range_weight = range_query
                .weight(EnableScoring::disabled_from_schema(&schema))
                .unwrap();
            let range_scorer = range_weight
                .scorer(&searcher.segment_readers()[0], 1.0f32)
                .unwrap();
            range_scorer
        };
        let range_scorer = make_scorer(Bound::Included(1), Bound::Included(4));
        assert!(range_scorer.is::<AllScorer>());
        let range_scorer = make_scorer(Bound::Included(0), Bound::Included(2));
        assert!(range_scorer.is::<ConstScorer<RangeDocSet<u64>>>());
        let range_scorer = make_scorer(Bound::Included(3), Bound::Included(10));
        assert!(range_scorer.is::<ConstScorer<RangeDocSet<u64>>>());
        let range_scorer = make_scorer(Bound::Included(10), Bound::Included(12));
        assert!(range_scorer.is::<ConstScorer<RangeDocSet<u64>>>());
        let range_scorer = make_scorer(Bound::Included(0), Bound::Included(1));
        assert!(range_scorer.is::<EmptyScorer>());
        let range_scorer = make_scorer(Bound::Included(0), Bound::Excluded(2));
        assert!(range_scorer.is::<EmptyScorer>());
    }
}
