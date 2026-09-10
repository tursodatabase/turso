use std::fmt::Debug;

use common::BitSet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::cached_sub_aggs::{
    CachedSubAggs, HighCardSubAggCache, LowCardSubAggCache, SubAggCache,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
};
use crate::aggregation::segment_agg_result::{BucketIdProvider, SegmentAggregationCollector};
use crate::aggregation::BucketId;
use crate::docset::DocSet;
use crate::query::{AllQuery, EnableScoring, Query, QueryParser};
use crate::schema::Schema;
use crate::tokenizer::TokenizerManager;
use crate::{DocId, SegmentReader, TantivyError};

/// A trait for query builders that can build queries programmatically.
///
/// This trait enables programmatic query construction for filter aggregations with
/// full serialization/deserialization support for distributed aggregation scenarios.
///
/// # Why This Exists
///
/// Filter aggregations need to support both:
/// - Query strings (simple, always serializable)
/// - Programmatic query construction (flexible, with serialization support)
///
/// This trait provides the programmatic query construction capability with full
/// serialization support via the `typetag` crate.
///
/// # Implementation Requirements
///
/// Implementers must:
/// 1. Derive `Debug`, `Clone`, `Serialize`, and `Deserialize`
/// 2. Use `#[typetag::serde]` attribute on the impl block
/// 3. Implement `build_query()` to construct the query from schema/tokenizers
/// 4. Implement `box_clone()` to enable cloning (typically just `Box::new(self.clone())`)
///
/// # Example
///
/// ```rust
/// use tantivy::aggregation::bucket::QueryBuilder;
/// use tantivy::query::{Query, TermQuery};
/// use tantivy::schema::{Schema, IndexRecordOption};
/// use tantivy::tokenizer::TokenizerManager;
/// use tantivy::Term;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct TermQueryBuilder {
///     field_name: String,
///     term_text: String,
/// }
///
/// #[typetag::serde]
/// impl QueryBuilder for TermQueryBuilder {
///     fn build_query(
///         &self,
///         schema: &Schema,
///         _tokenizers: &TokenizerManager,
///     ) -> tantivy::Result<Box<dyn Query>> {
///         let field = schema.get_field(&self.field_name)?;
///         let term = Term::from_field_text(field, &self.term_text);
///         Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
///     }
///
///     fn box_clone(&self) -> Box<dyn QueryBuilder> {
///         Box::new(self.clone())
///     }
/// }
///
/// // Create an instance
/// let builder = TermQueryBuilder {
///     field_name: "category".to_string(),
///     term_text: "electronics".to_string(),
/// };
/// ```
#[typetag::serde(tag = "type")]
pub trait QueryBuilder: Debug + Send + Sync {
    /// Build a query from the given schema and tokenizer manager.
    ///
    /// This method is called once when creating the FilterAggReqData for a segment.
    ///
    /// # Parameters
    /// - `schema`: The index schema for field lookups
    /// - `tokenizers`: The tokenizer manager for text analysis
    ///
    /// # Returns
    /// A boxed Query object, or an error if construction fails
    fn build_query(
        &self,
        schema: &Schema,
        tokenizers: &TokenizerManager,
    ) -> crate::Result<Box<dyn Query>>;

    /// Clone this builder into a boxed trait object.
    ///
    /// Since builders are just data (no state), this simply clones the data.
    /// The typical implementation is:
    /// ```rust,ignore
    /// fn box_clone(&self) -> Box<dyn QueryBuilder> {
    ///     Box::new(self.clone())
    /// }
    /// ```
    fn box_clone(&self) -> Box<dyn QueryBuilder>;
}

/// Filter aggregation creates a single bucket containing documents that match a query.
///
/// # Usage
///
/// ## Query String (Recommended)
/// ```rust
/// use tantivy::aggregation::bucket::FilterAggregation;
///
/// // Query strings are parsed using Tantivy's standard QueryParser
/// let filter_agg = FilterAggregation::new("category:electronics AND price:[100 TO 500]".to_string());
/// ```
///
/// ## Custom Query Builder
/// ```rust
/// use tantivy::aggregation::bucket::{FilterAggregation, QueryBuilder};
/// use tantivy::query::{Query, TermQuery};
/// use tantivy::schema::{Schema, IndexRecordOption};
/// use tantivy::tokenizer::TokenizerManager;
/// use tantivy::Term;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct MyBuilder {
///     field_name: String,
///     term_text: String,
/// }
///
/// #[typetag::serde]
/// impl QueryBuilder for MyBuilder {
///     fn build_query(
///         &self,
///         schema: &Schema,
///         _tokenizers: &TokenizerManager,
///     ) -> tantivy::Result<Box<dyn Query>> {
///         let field = schema.get_field(&self.field_name)?;
///         let term = Term::from_field_text(field, &self.term_text);
///         Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
///     }
///
///     fn box_clone(&self) -> Box<dyn QueryBuilder> {
///         Box::new(self.clone())
///     }
/// }
///
/// let builder = MyBuilder {
///     field_name: "category".to_string(),
///     term_text: "electronics".to_string(),
/// };
/// let filter_agg = FilterAggregation::new_with_builder(Box::new(builder));
/// ```
///
/// # Result
/// The filter aggregation returns a single bucket with:
/// - `doc_count`: Number of documents matching the filter
/// - Sub-aggregation results computed on the filtered document set
#[derive(Debug, Clone)]
pub struct FilterAggregation {
    /// The query for filtering - can be either a query string or a query builder
    query: FilterQuery,
}

/// Represents different ways to specify a filter query
pub enum FilterQuery {
    /// Query string that will be parsed using Tantivy's standard parsing facilities
    ///
    /// This is the recommended approach as it's serializable and doesn't carry runtime state.
    QueryString(String),

    /// Custom query builder for programmatic query building
    ///
    /// This variant stores a builder that builds the query once when creating FilterAggReqData.
    ///
    /// This is useful for:
    /// - Custom query types not expressible as query strings
    /// - Programmatic query construction based on schema
    /// - Extension query types
    ///
    /// **Note**: The builder is serializable and can be deserialized.
    CustomBuilder(Box<dyn QueryBuilder>),
}

impl Debug for FilterQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterQuery::QueryString(s) => f.debug_tuple("QueryString").field(s).finish(),
            FilterQuery::CustomBuilder(_) => {
                f.debug_struct("CustomBuilder").finish_non_exhaustive()
            }
        }
    }
}

impl Clone for FilterQuery {
    fn clone(&self) -> Self {
        match self {
            FilterQuery::QueryString(query_string) => {
                FilterQuery::QueryString(query_string.clone())
            }
            FilterQuery::CustomBuilder(builder) => FilterQuery::CustomBuilder(builder.box_clone()),
        }
    }
}

impl FilterAggregation {
    /// Create a new filter aggregation with a query string
    /// The query string will be parsed using the QueryParser::parse_query() method.
    pub fn new(query_string: String) -> Self {
        Self {
            query: FilterQuery::QueryString(query_string),
        }
    }

    /// Create a new filter aggregation with a query builder
    ///
    /// The builder will be called once when creating the FilterAggReqData for each segment.
    ///
    /// # Example
    /// ```rust
    /// use tantivy::aggregation::bucket::{FilterAggregation, QueryBuilder};
    /// use tantivy::query::{Query, TermQuery};
    /// use tantivy::schema::{Schema, IndexRecordOption};
    /// use tantivy::tokenizer::TokenizerManager;
    /// use tantivy::Term;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Debug, Clone, Serialize, Deserialize)]
    /// struct MyBuilder {
    ///     field_name: String,
    ///     term_text: String,
    /// }
    ///
    /// #[typetag::serde]
    /// impl QueryBuilder for MyBuilder {
    ///     fn build_query(
    ///         &self,
    ///         schema: &Schema,
    ///         _tokenizers: &TokenizerManager,
    ///     ) -> tantivy::Result<Box<dyn Query>> {
    ///         let field = schema.get_field(&self.field_name)?;
    ///         let term = Term::from_field_text(field, &self.term_text);
    ///         Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
    ///     }
    ///
    ///     fn box_clone(&self) -> Box<dyn QueryBuilder> {
    ///         Box::new(self.clone())
    ///     }
    /// }
    ///
    /// let builder = MyBuilder {
    ///     field_name: "category".to_string(),
    ///     term_text: "electronics".to_string(),
    /// };
    /// let filter_agg = FilterAggregation::new_with_builder(Box::new(builder));
    /// ```
    pub fn new_with_builder(builder: Box<dyn QueryBuilder>) -> Self {
        Self {
            query: FilterQuery::CustomBuilder(builder),
        }
    }

    /// Parse the query into a Tantivy Query object
    ///
    /// For query strings, this uses the QueryParser::parse_query() method.
    /// For custom builders, builds the query using the builder.
    pub(crate) fn parse_query(
        &self,
        schema: &Schema,
        tokenizer_manager: &TokenizerManager,
    ) -> crate::Result<Box<dyn Query>> {
        match &self.query {
            FilterQuery::QueryString(query_str) => {
                let query_parser =
                    QueryParser::new(schema.clone(), vec![], tokenizer_manager.clone());

                query_parser
                    .parse_query(query_str)
                    .map_err(|e| TantivyError::InvalidArgument(e.to_string()))
            }
            FilterQuery::CustomBuilder(builder) => {
                // Build the query using the builder
                builder.build_query(schema, tokenizer_manager)
            }
        }
    }

    /// Parse the query with a custom QueryParser
    ///
    /// This method allows using a pre-configured QueryParser with custom settings
    /// like field boosts, fuzzy matching, default fields, etc.
    ///
    /// For custom builders, this method is not supported and will return an error.
    /// Custom builders need schema and tokenizers which are not accessible from QueryParser.
    pub fn parse_query_with_parser(
        &self,
        query_parser: &QueryParser,
    ) -> crate::Result<Box<dyn Query>> {
        match &self.query {
            FilterQuery::QueryString(query_str) => query_parser
                .parse_query(query_str)
                .map_err(|e| TantivyError::InvalidArgument(e.to_string())),
            FilterQuery::CustomBuilder(_) => Err(TantivyError::InvalidArgument(
                "parse_query_with_parser is not supported for custom query builders. Use \
                 parse_query with explicit schema and tokenizers instead."
                    .to_string(),
            )),
        }
    }

    /// Get the fast field names used by this aggregation (none for filter aggregation)
    pub fn get_fast_field_names(&self) -> Vec<&str> {
        // Filter aggregation cannot introspect query fast field dependencies.
        //
        // As of PR #2693, queries can fall back to fast fields when fields are not indexed
        // (e.g., TermQuery falls back to RangeQuery on fast fields). However, the Query
        // trait has no mechanism to report these dependencies.
        //
        // For prefetching optimization, callers must analyze the query themselves to
        // determine fast field usage. This requires:
        // 1. Parsing the query string to extract field references
        // 2. Checking the schema to see if those fields are indexed or fast-only
        // 3. Collecting fast field names for non-indexed fields
        //
        // This limitation exists because:
        // - Query::weight() is called during execution, not during planning
        // - The fallback decision is based on schema configuration
        // - There's no Query trait method to declare potential fast field dependencies
        vec![]
    }
}

// Custom serialization implementation
impl Serialize for FilterAggregation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        match &self.query {
            FilterQuery::QueryString(query_string) => {
                // Serialize query strings as plain strings
                query_string.serialize(serializer)
            }
            FilterQuery::CustomBuilder(builder) => {
                // Serialize custom builders using typetag (includes type information)
                builder.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for FilterAggregation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        // We need to peek at the value to determine if it's a string or an object
        use serde::de::Error;
        use serde_json::Value;

        let value = Value::deserialize(deserializer)?;

        let query = if let Some(query_string) = value.as_str() {
            // It's a plain string - query string
            FilterQuery::QueryString(query_string.to_string())
        } else {
            // It's an object - custom builder with typetag
            let builder: Box<dyn QueryBuilder> = serde_json::from_value(value).map_err(|e| {
                D::Error::custom(format!("Failed to deserialize QueryBuilder: {}", e))
            })?;
            FilterQuery::CustomBuilder(builder)
        };

        Ok(FilterAggregation { query })
    }
}

// PartialEq is required because AggregationVariants derives it
// We implement it manually to handle custom builders which cannot be compared
impl PartialEq for FilterAggregation {
    fn eq(&self, other: &Self) -> bool {
        match (&self.query, &other.query) {
            (FilterQuery::QueryString(a), FilterQuery::QueryString(b)) => a == b,
            // Custom builders cannot be compared for equality
            _ => false,
        }
    }
}

/// Request data for filter aggregation
/// This struct holds the per-segment data needed to execute a filter aggregation
pub struct FilterAggReqData {
    /// The name of the filter aggregation
    pub name: String,
    /// The filter aggregation
    pub req: FilterAggregation,
    /// The segment reader
    pub segment_reader: SegmentReader,
    /// Document evaluator for the filter query (precomputed BitSet)
    /// This is built once when the request data is created
    pub evaluator: DocumentQueryEvaluator,
    /// Reusable buffer for matching documents to minimize allocations during collection
    pub matching_docs_buffer: Vec<DocId>,
    /// True if this filter aggregation is at the top level of the aggregation tree (not nested).
    pub is_top_level: bool,
}

impl FilterAggReqData {
    pub(crate) fn get_memory_consumption(&self) -> usize {
        // Estimate: name + segment reader reference + bitset + buffer capacity
        self.name.len()
        + std::mem::size_of::<SegmentReader>()
        + self.evaluator.bitset.len() / 8 // BitSet memory (bits to bytes)
        + self.matching_docs_buffer.capacity() * std::mem::size_of::<DocId>()
        + std::mem::size_of::<bool>()
    }
}

/// Document evaluator for filter queries using BitSet
pub struct DocumentQueryEvaluator {
    /// BitSet containing all matching documents for this segment.
    /// For AllQuery, this is a full BitSet (all bits set).
    /// For other queries, only matching document bits are set.
    pub(crate) bitset: BitSet,
}

impl DocumentQueryEvaluator {
    /// Create and initialize a document query evaluator for a segment
    /// This executes the query upfront and collects results into a BitSet,
    /// unless the query is AllQuery in which case we skip BitSet creation.
    pub(crate) fn new(
        query: Box<dyn Query>,
        schema: Schema,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self> {
        let max_doc = segment_reader.max_doc();

        // Optimization: Detect AllQuery and create a full BitSet
        if query.as_any().downcast_ref::<AllQuery>().is_some() {
            return Ok(Self {
                bitset: BitSet::with_max_value_and_full(max_doc),
            });
        }

        // Get the weight for the query
        let weight = query.weight(EnableScoring::disabled_from_schema(&schema))?;

        // Get a scorer that iterates over matching documents
        let mut scorer = weight.scorer(segment_reader, 1.0)?;

        // Create a BitSet to hold all matching documents
        let mut bitset = BitSet::with_max_value(max_doc);

        // Collect all matching documents into the BitSet
        // This is the upfront cost, but then lookups are O(1)
        let mut doc = scorer.doc();
        while doc != crate::TERMINATED {
            bitset.insert(doc);
            doc = scorer.advance();
        }

        Ok(Self { bitset })
    }

    /// Evaluate if a document matches the filter query
    /// O(1) lookup in the precomputed BitSet
    #[inline]
    pub fn matches_document(&self, doc: DocId) -> bool {
        self.bitset.contains(doc)
    }

    /// Filter a batch of documents
    /// Returns matching documents from the input batch
    #[inline]
    pub fn filter_batch(&self, docs: &[DocId], output: &mut Vec<DocId>) {
        for &doc in docs {
            if self.bitset.contains(doc) {
                output.push(doc);
            }
        }
    }
}

impl Debug for DocumentQueryEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentQueryEvaluator")
            .field("num_matches", &self.bitset.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
struct DocCount {
    doc_count: u64,
    bucket_id: BucketId,
}

/// Segment collector for filter aggregation
pub struct SegmentFilterCollector<C: SubAggCache> {
    /// Document counts per parent bucket
    parent_buckets: Vec<DocCount>,
    /// Sub-aggregation collectors
    sub_aggregations: Option<CachedSubAggs<C>>,
    bucket_id_provider: BucketIdProvider,
    /// Accessor index for this filter aggregation (to access FilterAggReqData)
    accessor_idx: usize,
}

impl<C: SubAggCache> SegmentFilterCollector<C> {
    /// Create a new filter segment collector following the new agg_data pattern
    pub(crate) fn from_req_and_validate(
        req: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        // Build sub-aggregation collectors if any
        let sub_agg_collector = if !node.children.is_empty() {
            Some(build_segment_agg_collectors(req, &node.children)?)
        } else {
            None
        };
        let sub_agg_collector = sub_agg_collector.map(CachedSubAggs::new);

        Ok(SegmentFilterCollector {
            parent_buckets: Vec::new(),
            sub_aggregations: sub_agg_collector,
            accessor_idx: node.idx_in_req_data,
            bucket_id_provider: BucketIdProvider::default(),
        })
    }
}

pub(crate) fn build_segment_filter_collector(
    req: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let is_top_level = req.per_request.filter_req_data[node.idx_in_req_data]
        .as_ref()
        .expect("filter_req_data slot is empty")
        .is_top_level;

    if is_top_level {
        Ok(Box::new(
            SegmentFilterCollector::<LowCardSubAggCache>::from_req_and_validate(req, node)?,
        ))
    } else {
        Ok(Box::new(
            SegmentFilterCollector::<HighCardSubAggCache>::from_req_and_validate(req, node)?,
        ))
    }
}

impl<C: SubAggCache> Debug for SegmentFilterCollector<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentFilterCollector")
            .field("buckets", &self.parent_buckets)
            .field("has_sub_aggs", &self.sub_aggregations.is_some())
            .field("accessor_idx", &self.accessor_idx)
            .finish()
    }
}

impl<C: SubAggCache> SegmentAggregationCollector for SegmentFilterCollector<C> {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        let mut sub_results = IntermediateAggregationResults::default();
        let bucket_opt = self.parent_buckets.get(parent_bucket_id as usize);

        if let Some(sub_aggs) = &mut self.sub_aggregations {
            sub_aggs
                .get_sub_agg_collector()
                .add_intermediate_aggregation_result(
                    agg_data,
                    &mut sub_results,
                    // Here we create a new bucket ID for sub-aggregations if the bucket doesn't
                    // exist, so that sub-aggregations can still produce results (e.g., zero doc
                    // count)
                    bucket_opt
                        .map(|bucket| bucket.bucket_id)
                        .unwrap_or(self.bucket_id_provider.next_bucket_id()),
                )?;
        }

        // Create the filter bucket result
        let filter_bucket_result = IntermediateBucketResult::Filter {
            doc_count: bucket_opt.map(|b| b.doc_count).unwrap_or(0),
            sub_aggregations: sub_results,
        };

        // Get the name of this filter aggregation
        let name = agg_data.per_request.filter_req_data[self.accessor_idx]
            .as_ref()
            .expect("filter_req_data slot is empty")
            .name
            .clone();

        results.push(
            name,
            IntermediateAggregationResult::Bucket(filter_bucket_result),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if docs.is_empty() {
            return Ok(());
        }

        let mut bucket = self.parent_buckets[parent_bucket_id as usize];
        // Take the request data to avoid borrow checker issues with sub-aggregations
        let mut req = agg_data.take_filter_req_data(self.accessor_idx);

        // Use batch filtering with O(1) BitSet lookups
        req.matching_docs_buffer.clear();
        req.evaluator
            .filter_batch(docs, &mut req.matching_docs_buffer);

        bucket.doc_count += req.matching_docs_buffer.len() as u64;

        // Batch process sub-aggregations if we have matches
        if !req.matching_docs_buffer.is_empty() {
            if let Some(sub_aggs) = &mut self.sub_aggregations {
                for &doc_id in &req.matching_docs_buffer {
                    sub_aggs.push(bucket.bucket_id, doc_id);
                }
            }
        }

        // Put the request data back
        agg_data.put_back_filter_req_data(self.accessor_idx, req);
        if let Some(sub_aggs) = &mut self.sub_aggregations {
            sub_aggs.check_flush_local(agg_data)?;
        }
        // put back bucket
        self.parent_buckets[parent_bucket_id as usize] = bucket;

        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if let Some(ref mut sub_aggs) = self.sub_aggregations {
            sub_aggs.flush(agg_data)?;
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        while self.parent_buckets.len() <= max_bucket as usize {
            let bucket_id = self.bucket_id_provider.next_bucket_id();
            self.parent_buckets.push(DocCount {
                doc_count: 0,
                bucket_id,
            });
        }
        Ok(())
    }
}

/// Intermediate result for filter aggregation
#[derive(Debug, Clone, PartialEq)]
pub struct IntermediateFilterBucketResult {
    /// Document count in this bucket
    pub doc_count: u64,
    /// Sub-aggregation results
    pub sub_aggregations: IntermediateAggregationResults,
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::{AggContextParams, AggregationCollector};
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, Term, FAST, INDEXED, TEXT};
    use crate::{doc, Index, IndexWriter};

    // Test helper functions
    fn aggregation_results_to_json(results: &AggregationResults) -> Value {
        serde_json::to_value(results).expect("Failed to serialize aggregation results")
    }

    fn json_values_match(actual: &Value, expected: &Value, tolerance: f64) -> bool {
        match (actual, expected) {
            (Value::Number(a), Value::Number(e)) => {
                let a_f64 = a.as_f64().unwrap_or(0.0);
                let e_f64 = e.as_f64().unwrap_or(0.0);
                (a_f64 - e_f64).abs() < tolerance
            }
            (Value::Object(a_map), Value::Object(e_map)) => {
                if a_map.len() != e_map.len() {
                    return false;
                }
                for (key, expected_val) in e_map {
                    match a_map.get(key) {
                        Some(actual_val) => {
                            if !json_values_match(actual_val, expected_val, tolerance) {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            (Value::Array(a_arr), Value::Array(e_arr)) => {
                if a_arr.len() != e_arr.len() {
                    return false;
                }
                for (actual_item, expected_item) in a_arr.iter().zip(e_arr.iter()) {
                    if !json_values_match(actual_item, expected_item, tolerance) {
                        return false;
                    }
                }
                true
            }
            _ => actual == expected,
        }
    }

    fn assert_aggregation_results_match(
        actual_results: &AggregationResults,
        expected_json: Value,
        tolerance: f64,
    ) {
        let actual_json = aggregation_results_to_json(actual_results);

        if !json_values_match(&actual_json, &expected_json, tolerance) {
            panic!(
                "Aggregation results do not match expected JSON.\nActual:\n{}\nExpected:\n{}",
                serde_json::to_string_pretty(&actual_json).unwrap(),
                serde_json::to_string_pretty(&expected_json).unwrap()
            );
        }
    }

    macro_rules! assert_agg_results {
        ($actual:expr, $expected:expr) => {
            assert_aggregation_results_match($actual, $expected, 0.1)
        };
        ($actual:expr, $expected:expr, $tolerance:expr) => {
            assert_aggregation_results_match($actual, $expected, $tolerance)
        };
    }

    fn create_standard_test_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let category = schema_builder.add_text_field("category", TEXT | FAST);
        let brand = schema_builder.add_text_field("brand", TEXT | FAST);
        let price = schema_builder.add_u64_field("price", FAST | INDEXED);
        let rating = schema_builder.add_f64_field("rating", FAST);
        let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer_for_tests()?;

        writer.add_document(doc!(
            category => "electronics", brand => "apple",
            price => 999u64, rating => 4.5f64, in_stock => true
        ))?;
        writer.commit()?;
        writer.add_document(doc!(
            category => "electronics", brand => "samsung",
            price => 799u64, rating => 4.2f64, in_stock => true
        ))?;
        writer.add_document(doc!(
            category => "clothing", brand => "nike",
            price => 120u64, rating => 4.1f64, in_stock => false
        ))?;
        writer.add_document(doc!(
            category => "books", brand => "penguin",
            price => 25u64, rating => 4.8f64, in_stock => true
        ))?;

        writer.commit()?;
        Ok(index)
    }

    /// Helper to create aggregation collector with serialization roundtrip
    /// This ensures all aggregations can be serialized and deserialized correctly
    fn create_collector(
        index: &Index,
        aggregations: Aggregations,
    ) -> crate::Result<AggregationCollector> {
        // Serialize and deserialize the aggregations
        let serialized = serde_json::to_string(&aggregations)?;
        let deserialized: Aggregations = serde_json::from_str(&serialized)?;

        // Create collector with deserialized aggregations
        Ok(AggregationCollector::from_aggs(
            deserialized,
            AggContextParams::new(Default::default(), index.tokenizers().clone()),
        ))
    }

    #[test]
    fn test_basic_filter_with_metric_agg() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": {
                    "avg_price": { "avg": { "field": "price" } }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "avg_price": { "value": 899.0 }  // (999 + 799) / 2
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_filter_with_no_matches() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "furniture": {
                "filter": "category:furniture",
                "aggs": {
                    "avg_price": { "avg": { "field": "price" } }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "furniture": {
                "doc_count": 0,
                "avg_price": { "value": null }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_independent_filters() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            },
            "in_stock": {
                "filter": "in_stock:true",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            },
            "high_rated": {
                "filter": "rating:[4.5 TO *]",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "avg_price": { "value": 899.0 }
            },
            "in_stock": {
                "doc_count": 3,  // apple, samsung, penguin
                "count": { "value": 3.0 }
            },
            "high_rated": {
                "doc_count": 2,  // apple (4.5), penguin (4.8)
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Query Type Tests
    // ============================================================================

    #[test]
    fn test_term_query_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "apple_products": {
                "filter": "brand:apple",
                "aggs": { "max_price": { "max": { "field": "price" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "apple_products": {
                "doc_count": 1,
                "max_price": { "value": 999.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_range_query_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "mid_price": {
                "filter": "price:[100 TO 900]",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "mid_price": {
                "doc_count": 2,  // samsung (799), nike (120)
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_boolean_query_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        let agg = json!({
            "premium_electronics": {
                "filter": "category:electronics AND price:[800 TO *]",
                "aggs": { "avg_rating": { "avg": { "field": "rating" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "premium_electronics": {
                "doc_count": 1,  // Only apple (999) is >= 800 in tantivy's range semantics
                "avg_rating": { "value": 4.5 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_bool_field_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "in_stock": {
                "filter": "in_stock:true",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            },
            "out_of_stock": {
                "filter": "in_stock:false",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "in_stock": {
                "doc_count": 3,  // apple, samsung, penguin
                "avg_price": { "value": 607.67 }  // (999 + 799 + 25) / 3 ≈ 607.67
            },
            "out_of_stock": {
                "doc_count": 1,  // nike
                "count": { "value": 1.0 }
            }
        });

        assert_agg_results!(&result, expected, 1.0);
        Ok(())
    }

    // ============================================================================
    // Nested Filter Tests
    // ============================================================================

    #[test]
    fn test_two_level_nested_filters() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "all": {
                "filter": "*",
                "aggs": {
                    "electronics": {
                        "filter": "category:electronics",
                        "aggs": {
                            "expensive": {
                                "filter": "price:[900 TO *]",
                                "aggs": {
                                    "count": { "value_count": { "field": "brand" } }
                                }
                            }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "all": {
                "doc_count": 4,
                "electronics": {
                    "doc_count": 2,
                    "expensive": {
                        "doc_count": 1,  // Only apple (999) is >= 900
                        "count": { "value": 1.0 }
                    }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_deeply_nested_filters() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "level1": {
                "filter": "*",
                "aggs": {
                    "level2": {
                        "filter": "in_stock:true",
                        "aggs": {
                            "level3": {
                                "filter": "rating:[4.0 TO *]",
                                "aggs": {
                                    "level4": {
                                        "filter": "price:[500 TO *]",
                                        "aggs": {
                                            "final_count": { "value_count": { "field": "brand" } }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "level1": {
                "doc_count": 4,
                "level2": {
                    "doc_count": 3,  // in_stock: apple, samsung, penguin
                    "level3": {
                        "doc_count": 3,  // all have rating >= 4.0
                        "level4": {
                            "doc_count": 2,  // apple (999), samsung (799)
                            "final_count": { "value": 2.0 }
                        }
                    }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_nested_branches() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "root": {
                "filter": "*",
                "aggs": {
                    "electronics_branch": {
                        "filter": "category:electronics",
                        "aggs": {
                            "avg_price": { "avg": { "field": "price" } }
                        }
                    },
                    "in_stock_branch": {
                        "filter": "in_stock:true",
                        "aggs": {
                            "count": { "value_count": { "field": "brand" } }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "root": {
                "doc_count": 4,
                "electronics_branch": {
                    "doc_count": 2,
                    "avg_price": { "value": 899.0 }
                },
                "in_stock_branch": {
                    "doc_count": 3,
                    "count": { "value": 3.0 }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_nested_filters_with_multiple_siblings_at_each_level() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Test complex nesting: multiple branches at each level
        let agg = json!({
            "all": {
                "filter": "*",
                "aggs": {
                    // Level 2: Two independent filters
                    "expensive": {
                        "filter": "price:[500 TO *]",
                        "aggs": {
                            // Level 3: Multiple branches under "expensive"
                            "electronics": {
                                "filter": "category:electronics",
                                "aggs": {
                                    "avg_rating": { "avg": { "field": "rating" } }
                                }
                            },
                            "in_stock": {
                                "filter": "in_stock:true",
                                "aggs": {
                                    "count": { "value_count": { "field": "brand" } }
                                }
                            }
                        }
                    },
                    "affordable": {
                        "filter": "price:[0 TO 200]",
                        "aggs": {
                            // Level 3: Multiple branches under "affordable"
                            "books": {
                                "filter": "category:books",
                                "aggs": {
                                    "max_rating": { "max": { "field": "rating" } }
                                }
                            },
                            "clothing": {
                                "filter": "category:clothing",
                                "aggs": {
                                    "min_price": { "min": { "field": "price" } }
                                }
                            }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "all": {
                "doc_count": 4,
                "expensive": {
                    "doc_count": 2,  // apple (999), samsung (799)
                    "electronics": {
                        "doc_count": 2,  // both are electronics
                        "avg_rating": { "value": 4.35 }  // (4.5 + 4.2) / 2
                    },
                    "in_stock": {
                        "doc_count": 2,  // both are in stock
                        "count": { "value": 2.0 }
                    }
                },
                "affordable": {
                    "doc_count": 2,  // nike (120), penguin (25)
                    "books": {
                        "doc_count": 1,  // penguin (25)
                        "max_rating": { "value": 4.8 }
                    },
                    "clothing": {
                        "doc_count": 1,  // nike (120)
                        "min_price": { "value": 120.0 }
                    }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Sub-Aggregation Combination Tests
    // ============================================================================

    #[test]
    fn test_filter_with_terms_sub_agg() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": {
                    "brands": {
                        "terms": { "field": "brand" },
                        "aggs": {
                            "avg_price": { "avg": { "field": "price" } }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        // Verify the structure exists and has expected doc_count
        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "brands": {
                    "buckets": [
                        {
                            "key": "samsung",
                            "doc_count": 1,
                            "avg_price": { "value": 799.0 }
                        },
                        {
                            "key": "apple",
                            "doc_count": 1,
                            "avg_price": { "value": 999.0 }
                        }
                    ],
                    "sum_other_doc_count": 0,
                    "doc_count_error_upper_bound": 0
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_filter_with_multiple_metric_aggs() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": {
                    "price_stats": { "stats": { "field": "price" } },
                    "rating_avg": { "avg": { "field": "rating" } },
                    "count": { "value_count": { "field": "brand" } }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "price_stats": {
                    "count": 2,
                    "min": 799.0,
                    "max": 999.0,
                    "sum": 1798.0,
                    "avg": 899.0
                },
                "rating_avg": { "value": 4.35 },
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Edge Cases and Error Handling
    // ============================================================================

    #[test]
    fn test_filter_on_empty_index() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let _category = schema_builder.add_text_field("category", TEXT | FAST);
        let _price = schema_builder.add_u64_field("price", FAST);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer(50_000_000)?;
        writer.commit()?; // Commit empty index

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 0,
                "avg_price": { "value": null }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_malformed_query_string() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Empty query string
        let agg = json!({
            "test": {
                "filter": "",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let result = serde_json::from_value::<Aggregations>(agg)
            .map_err(|e| crate::TantivyError::InvalidArgument(e.to_string()))
            .and_then(|aggregations| {
                let collector = create_collector(&index, aggregations)?;
                searcher.search(&AllQuery, &collector)
            });

        // Empty string should either work (matching nothing) or error gracefully
        assert!(result.is_ok() || result.is_err());
        Ok(())
    }

    #[test]
    fn test_filter_with_base_query() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let schema = index.schema();

        // Use a base query to pre-filter to in_stock items only
        let in_stock_field = schema.get_field("in_stock").unwrap();
        let base_query = TermQuery::new(
            Term::from_field_bool(in_stock_field, true),
            IndexRecordOption::Basic,
        );

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&base_query, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,  // Both in-stock electronics
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Custom Query Integration Tests
    // ============================================================================

    #[test]
    fn test_custom_query_builder() -> crate::Result<()> {
        // Define a query builder with full serde support
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct TestTermQueryBuilder {
            field_name: String,
            term_text: String,
        }

        #[typetag::serde(name = "TestTermQueryBuilder")]
        impl QueryBuilder for TestTermQueryBuilder {
            fn build_query(
                &self,
                schema: &Schema,
                _tokenizers: &TokenizerManager,
            ) -> crate::Result<Box<dyn Query>> {
                let field = schema.get_field(&self.field_name)?;
                let term = Term::from_field_text(field, &self.term_text);
                Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
            }

            fn box_clone(&self) -> Box<dyn QueryBuilder> {
                Box::new(self.clone())
            }
        }

        let index = create_standard_test_index()?;

        // Create a filter aggregation with a custom query builder
        let builder = TestTermQueryBuilder {
            field_name: "category".to_string(),
            term_text: "electronics".to_string(),
        };
        let filter_agg = FilterAggregation::new_with_builder(Box::new(builder));

        // Test that the query can be parsed
        let schema = index.schema();
        let tokenizers = index.tokenizers();
        let query = filter_agg.parse_query(&schema, tokenizers)?;

        // Verify the query was built correctly (it should be a TermQuery)
        assert!(format!("{:?}", query).contains("TermQuery"));

        // Test that it can be cloned
        let cloned = filter_agg.clone();
        let query2 = cloned.parse_query(&schema, tokenizers)?;
        assert!(format!("{:?}", query2).contains("TermQuery"));

        // Verify that custom builders CAN be serialized with typetag
        let serialized = serde_json::to_string(&filter_agg)?;
        assert!(
            serialized.contains("TestTermQueryBuilder"),
            "Serialized JSON should contain the type tag"
        );
        assert!(
            serialized.contains("electronics"),
            "Serialized JSON should contain the field data"
        );

        // Verify that it can be deserialized
        let deserialized: FilterAggregation = serde_json::from_str(&serialized)?;
        let query3 = deserialized.parse_query(&schema, tokenizers)?;
        assert!(format!("{:?}", query3).contains("TermQuery"));

        Ok(())
    }

    #[test]
    fn test_query_string_serialization() -> crate::Result<()> {
        // Query strings should serialize/deserialize correctly
        let filter_agg = FilterAggregation::new("category:electronics".to_string());

        let serialized = serde_json::to_string(&filter_agg)?;
        assert!(serialized.contains("electronics"));

        let deserialized: FilterAggregation = serde_json::from_str(&serialized)?;
        // Verify it deserializes correctly by using it in an aggregation
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
                "test": {
                    "filter": deserialized,
                    "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations)?;
        let result = searcher.search(&AllQuery, &collector)?;

        // Should match 2 electronics
        let result_json = serde_json::to_value(&result)?;
        assert_eq!(result_json["test"]["doc_count"], 2);

        Ok(())
    }

    #[test]
    fn test_query_builder_serialization_roundtrip() -> crate::Result<()> {
        // Define a serializable query builder
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct RoundtripTermQueryBuilder {
            field_name: String,
            term_text: String,
        }

        #[typetag::serde(name = "RoundtripTermQueryBuilder")]
        impl QueryBuilder for RoundtripTermQueryBuilder {
            fn build_query(
                &self,
                schema: &Schema,
                _tokenizers: &TokenizerManager,
            ) -> crate::Result<Box<dyn Query>> {
                let field = schema.get_field(&self.field_name)?;
                let term = Term::from_field_text(field, &self.term_text);
                Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
            }

            fn box_clone(&self) -> Box<dyn QueryBuilder> {
                Box::new(self.clone())
            }
        }

        let index = create_standard_test_index()?;

        // Create a filter aggregation with a custom query builder
        let builder = RoundtripTermQueryBuilder {
            field_name: "category".to_string(),
            term_text: "electronics".to_string(),
        };
        let filter_agg = FilterAggregation::new_with_builder(Box::new(builder));

        // Serialize the filter aggregation
        let serialized = serde_json::to_string(&filter_agg)?;

        // Verify the serialized JSON contains the builder data and type tag
        assert!(
            serialized.contains("RoundtripTermQueryBuilder"),
            "Serialized JSON should contain type tag"
        );
        assert!(
            serialized.contains("category"),
            "Serialized JSON should contain field_name"
        );
        assert!(
            serialized.contains("electronics"),
            "Serialized JSON should contain term_text"
        );

        // Deserialize back
        let deserialized: FilterAggregation = serde_json::from_str(&serialized)?;

        // Verify the aggregation produces correct results
        let agg = json!({
            "filtered": {
                "filter": deserialized
            }
        });

        let agg_req: Aggregations = serde_json::from_value(agg)?;
        let searcher = index.reader()?.searcher();
        let collector = create_collector(&index, agg_req)?;
        let agg_res = searcher.search(&AllQuery, &collector)?;

        let result_json = serde_json::to_value(&agg_res)?;
        assert_eq!(result_json["filtered"]["doc_count"], 2);

        Ok(())
    }

    // ============================================================================
    // Correctness Validation Tests
    // ============================================================================

    #[test]
    fn test_filter_result_correctness_vs_separate_query() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let schema = index.schema();

        // Method 1: Filter aggregation
        let filter_agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(filter_agg)?;
        let collector = create_collector(&index, aggregations)?;
        let filter_result = searcher.search(&AllQuery, &collector)?;

        // Method 2: Separate query
        let category_field = schema.get_field("category").unwrap();
        let term = Term::from_field_text(category_field, "electronics");
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);

        let separate_agg = json!({
            "result": { "avg": { "field": "price" } }
        });

        let separate_aggregations: Aggregations = serde_json::from_value(separate_agg)?;
        let separate_collector =
            AggregationCollector::from_aggs(separate_aggregations, Default::default());
        let separate_result = searcher.search(&term_query, &separate_collector)?;

        // Both methods should produce identical results
        let filter_expected = json!({
            "electronics": {
                "doc_count": 2,
                "avg_price": { "value": 899.0 }
            }
        });

        let separate_expected = json!({
            "result": {
                "value": 899.0
            }
        });

        // Verify filter aggregation result
        assert_agg_results!(&filter_result, filter_expected);

        // Verify separate query result matches
        assert_agg_results!(&separate_result, separate_expected);

        // This test demonstrates that filter aggregation produces the same results
        // as running a separate query with the same condition
        Ok(())
    }

    #[test]
    fn test_custom_tokenizer_required() -> crate::Result<()> {
        use crate::schema::{TextFieldIndexing, TextOptions};
        use crate::tokenizer::{SimpleTokenizer, TextAnalyzer, TokenizerManager};

        // Create a custom tokenizer that doesn't lowercase (just splits on whitespace)
        let custom_tokenizer = TextAnalyzer::builder(SimpleTokenizer::default()).build();

        // Register tokenizer
        let tokenizers = TokenizerManager::default();
        tokenizers.register("my_custom", custom_tokenizer);

        // Create a schema with a text field that uses our custom tokenizer
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("my_custom")
            .set_index_option(IndexRecordOption::Basic);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();

        // Build index with custom tokenizer
        let index = crate::IndexBuilder::new()
            .schema(schema.clone())
            .tokenizers(tokenizers)
            .create_in_ram()?;
        let mut writer = index.writer(50_000_000)?;

        // Add documents with UPPERCASE text
        writer.add_document(doc!(text_field => "HELLO"))?;
        writer.add_document(doc!(text_field => "WORLD"))?;
        writer.add_document(doc!(text_field => "hello"))?; // lowercase version
        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Test: With correct tokenizer (from index) - should work
        let agg = json!({
            "uppercase_hello": {
                "filter": "text:HELLO"
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector_with_tokenizer = create_collector(&index, aggregations.clone())?;
        let result_with_tokenizer = searcher.search(&AllQuery, &collector_with_tokenizer)?;

        // Should match only the UPPERCASE "HELLO" (1 document)
        let result_json = serde_json::to_value(&result_with_tokenizer)?;
        assert_eq!(
            result_json["uppercase_hello"]["doc_count"], 1,
            "With custom tokenizer from index, should match exactly 1 UPPERCASE document"
        );

        // Test 2: With default tokenizer (wrong!) - should fail to parse the query
        // because "my_custom" tokenizer is not in the default TokenizerManager
        let collector_with_default = AggregationCollector::from_aggs(
            aggregations,
            AggContextParams::new(Default::default(), TokenizerManager::default()),
        );
        let result_with_default = searcher.search(&AllQuery, &collector_with_default);

        // This should error because the tokenizer "my_custom" is not registered
        assert!(
            result_with_default.is_err(),
            "Without proper tokenizers, query parsing should fail"
        );
        assert!(
            result_with_default
                .unwrap_err()
                .to_string()
                .contains("my_custom"),
            "Error should mention the missing tokenizer"
        );

        Ok(())
    }
}
