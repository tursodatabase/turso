use std::collections::HashMap;
use std::net::Ipv6Addr;

use columnar::{Column, ColumnType, ColumnarReader, DynamicColumn};
use common::json_path_writer::JSON_PATH_SEGMENT_SEP_STR;
use common::DateTime;
use regex::Regex;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{TopHitsMetricResult, TopHitsVecEntry};
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::bucket::Order;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::{AggregationError, BucketId};
use crate::collector::sort_key::ReverseComparator;
use crate::collector::TopNComputer;
use crate::schema::OwnedValue;
use crate::{DocAddress, DocId, SegmentOrdinal};

/// Contains all information required by the TopHitsSegmentCollector to perform the
/// top_hits aggregation on a segment.
#[derive(Default)]
pub struct TopHitsAggReqData {
    /// The accessors to access the fast field values.
    pub accessors: Vec<(Column<u64>, ColumnType)>,
    /// The accessors to access the fast field values for retrieving document fields.
    pub value_accessors: HashMap<String, Vec<DynamicColumn>>,
    /// The ordinal of the segment this request data is for.
    pub segment_ordinal: SegmentOrdinal,
    /// The name of the aggregation.
    pub name: String,
    /// The top_hits aggregation request.
    pub req: TopHitsAggregationReq,
}

impl TopHitsAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// # Top Hits
///
/// The top hits aggregation is a useful tool to answer questions like:
/// - "What are the most recent posts by each author?"
/// - "What are the most popular items in each category?"
///
/// It does so by keeping track of the most relevant document being aggregated,
/// in terms of a sort criterion that can consist of multiple fields and their
/// sort-orders (ascending or descending).
///
/// `top_hits` should not be used as a top-level aggregation. It is intended to be
/// used as a sub-aggregation, inside a `terms` aggregation or a `filters` aggregation,
/// for example.
///
/// Note that this aggregator does not return the actual document addresses, but
/// rather a list of the values of the fields that were requested to be retrieved.
/// These values can be specified in the `docvalue_fields` parameter, which can include
/// a list of fast fields to be retrieved. At the moment, only fast fields are supported
/// but it is possible that we support the `fields` parameter to retrieve any stored
/// field in the future.
///
/// The following example demonstrates a request for the top_hits aggregation:
/// ```JSON
/// {
///     "aggs": {
///         "top_authors": {
///             "terms": {
///                 "field": "author",
///                 "size": 5
///             }
///         },
///         "aggs": {
///             "top_hits": {
///                 "size": 2,
///                 "from": 0
///                 "sort": [
///                     { "date": "desc" }
///                 ]
///                 "docvalue_fields": ["date", "title", "iden"]
///             }
///         }
/// }
/// ```
///
/// This request will return an object containing the top two documents, sorted
/// by the `date` field in descending order. You can also sort by multiple fields, which
/// helps to resolve ties. The aggregation object for each bucket will look like:
/// ```JSON
/// {
///     "hits": [
///         {
///           "score": [<time_u64>],
///           "docvalue_fields": {
///             "date": "<date_RFC3339>",
///             "title": "<title>",
///             "iden": "<iden>"
///           }
///         },
///         {
///           "score": [<time_u64>]
///           "docvalue_fields": {
///             "date": "<date_RFC3339>",
///             "title": "<title>",
///             "iden": "<iden>"
///           }
///         }
///     ]
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopHitsAggregationReq {
    sort: Vec<KeyOrder>,
    size: usize,
    from: Option<usize>,

    #[serde(rename = "docvalue_fields")]
    #[serde(default)]
    doc_value_fields: Vec<String>,

    // Not supported
    _source: Option<serde_json::Value>,
    fields: Option<serde_json::Value>,
    script_fields: Option<serde_json::Value>,
    highlight: Option<serde_json::Value>,
    explain: Option<serde_json::Value>,
    version: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct KeyOrder {
    field: String,
    order: Order,
}

impl Serialize for KeyOrder {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let KeyOrder { field, order } = self;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(field, order)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for KeyOrder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let mut key_order = <HashMap<String, Order>>::deserialize(deserializer)?.into_iter();
        let (field, order) = key_order.next().ok_or(serde::de::Error::custom(
            "Expected exactly one key-value pair in sort parameter of top_hits, found none",
        ))?;
        if key_order.next().is_some() {
            return Err(serde::de::Error::custom(format!(
                "Expected exactly one key-value pair in sort parameter of top_hits, found \
                 {key_order:?}"
            )));
        }
        Ok(Self { field, order })
    }
}

// Transform a glob (`pattern*`, for example) into a regex::Regex (`^pattern.*$`)
fn globbed_string_to_regex(glob: &str) -> Result<Regex, crate::TantivyError> {
    // Replace `*` glob with `.*` regex
    let sanitized = format!("^{}$", regex::escape(glob).replace(r"\*", ".*"));
    Regex::new(&sanitized.replace('*', ".*")).map_err(|e| {
        crate::TantivyError::SchemaError(format!("Invalid regex '{glob}' in docvalue_fields: {e}"))
    })
}

fn use_doc_value_fields_err(parameter: &str) -> crate::Result<()> {
    Err(crate::TantivyError::AggregationError(
        AggregationError::InvalidRequest(format!(
            "The `{parameter}` parameter is not supported, only `docvalue_fields` is supported in \
             `top_hits` aggregation"
        )),
    ))
}
fn unsupported_err(parameter: &str) -> crate::Result<()> {
    Err(crate::TantivyError::AggregationError(
        AggregationError::InvalidRequest(format!(
            "The `{parameter}` parameter is not supported in the `top_hits` aggregation"
        )),
    ))
}

impl TopHitsAggregationReq {
    /// Validate and resolve field retrieval parameters
    pub fn validate_and_resolve_field_names(
        &mut self,
        reader: &ColumnarReader,
    ) -> crate::Result<()> {
        if self._source.is_some() {
            use_doc_value_fields_err("_source")?;
        }
        if self.fields.is_some() {
            use_doc_value_fields_err("fields")?;
        }
        if self.script_fields.is_some() {
            use_doc_value_fields_err("script_fields")?;
        }
        if self.explain.is_some() {
            unsupported_err("explain")?;
        }
        if self.highlight.is_some() {
            unsupported_err("highlight")?;
        }
        if self.version.is_some() {
            unsupported_err("version")?;
        }

        self.doc_value_fields = self
            .doc_value_fields
            .iter()
            .map(|field| {
                if !field.contains('*')
                    && reader
                        .iter_columns()?
                        .any(|(name, _)| name.as_str() == field)
                {
                    return Ok(vec![field.to_owned()]);
                }

                let pattern = globbed_string_to_regex(field)?;
                let fields = reader
                    .iter_columns()?
                    .map(|(name, _)| {
                        // normalize path from internal fast field repr
                        name.replace(JSON_PATH_SEGMENT_SEP_STR, ".")
                    })
                    .filter(|name| pattern.is_match(name))
                    .collect::<Vec<_>>();
                assert!(
                    !fields.is_empty(),
                    "No fields matched the glob '{field}' in docvalue_fields"
                );
                Ok(fields)
            })
            .collect::<crate::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(())
    }

    /// Return fields accessed by the aggregator, in order.
    pub fn field_names(&self) -> Vec<&str> {
        self.sort
            .iter()
            .map(|KeyOrder { field, .. }| field.as_str())
            .chain(self.doc_value_fields.iter().map(|s| s.as_str()))
            .collect()
    }

    /// Return fields accessed by the aggregator's value retrieval.
    pub fn value_field_names(&self) -> Vec<&str> {
        self.doc_value_fields.iter().map(|s| s.as_str()).collect()
    }

    fn get_document_field_data(
        &self,
        accessors: &HashMap<String, Vec<DynamicColumn>>,
        doc_id: DocId,
    ) -> HashMap<String, FastFieldValue> {
        let doc_value_fields = self
            .doc_value_fields
            .iter()
            .map(|field| {
                let accessors = accessors
                    .get(field)
                    .unwrap_or_else(|| panic!("field '{field}' not found in accessors"));

                let values: Vec<FastFieldValue> = accessors
                    .iter()
                    .flat_map(|accessor| match accessor {
                        DynamicColumn::U64(accessor) => accessor
                            .values_for_doc(doc_id)
                            .map(FastFieldValue::U64)
                            .collect::<Vec<_>>(),
                        DynamicColumn::I64(accessor) => accessor
                            .values_for_doc(doc_id)
                            .map(FastFieldValue::I64)
                            .collect::<Vec<_>>(),
                        DynamicColumn::F64(accessor) => accessor
                            .values_for_doc(doc_id)
                            .map(FastFieldValue::F64)
                            .collect::<Vec<_>>(),
                        DynamicColumn::Bytes(accessor) => accessor
                            .term_ords(doc_id)
                            .map(|term_ord| {
                                let mut buffer = vec![];
                                assert!(
                                    accessor
                                        .ord_to_bytes(term_ord, &mut buffer)
                                        .expect("could not read term dictionary"),
                                    "term corresponding to term_ord does not exist"
                                );
                                FastFieldValue::Bytes(buffer)
                            })
                            .collect::<Vec<_>>(),
                        DynamicColumn::Str(accessor) => accessor
                            .term_ords(doc_id)
                            .map(|term_ord| {
                                let mut buffer = vec![];
                                assert!(
                                    accessor
                                        .ord_to_bytes(term_ord, &mut buffer)
                                        .expect("could not read term dictionary"),
                                    "term corresponding to term_ord does not exist"
                                );
                                FastFieldValue::Str(String::from_utf8(buffer).unwrap())
                            })
                            .collect::<Vec<_>>(),
                        DynamicColumn::Bool(accessor) => accessor
                            .values_for_doc(doc_id)
                            .map(FastFieldValue::Bool)
                            .collect::<Vec<_>>(),
                        DynamicColumn::IpAddr(accessor) => accessor
                            .values_for_doc(doc_id)
                            .map(FastFieldValue::IpAddr)
                            .collect::<Vec<_>>(),
                        DynamicColumn::DateTime(accessor) => accessor
                            .values_for_doc(doc_id)
                            .map(FastFieldValue::Date)
                            .collect::<Vec<_>>(),
                    })
                    .collect();

                (field.to_owned(), FastFieldValue::Array(values))
            })
            .collect();
        doc_value_fields
    }
}

/// A retrieved value from a fast field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FastFieldValue {
    /// The str type is used for any text information.
    Str(String),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Bool value
    Bool(bool),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// Arbitrarily sized byte array
    Bytes(Vec<u8>),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
    /// A list of values.
    Array(Vec<Self>),
}

impl From<FastFieldValue> for OwnedValue {
    fn from(value: FastFieldValue) -> Self {
        match value {
            FastFieldValue::Str(s) => OwnedValue::Str(s),
            FastFieldValue::U64(u) => OwnedValue::U64(u),
            FastFieldValue::I64(i) => OwnedValue::I64(i),
            FastFieldValue::F64(f) => OwnedValue::F64(f),
            FastFieldValue::Bool(b) => OwnedValue::Bool(b),
            FastFieldValue::Date(d) => OwnedValue::Date(d),
            FastFieldValue::Bytes(b) => OwnedValue::Bytes(b),
            FastFieldValue::IpAddr(ip) => OwnedValue::IpAddr(ip),
            FastFieldValue::Array(a) => {
                OwnedValue::Array(a.into_iter().map(OwnedValue::from).collect())
            }
        }
    }
}

/// Holds a fast field value in its u64 representation, and the order in which it should be sorted.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct DocValueAndOrder {
    /// A fast field value in its u64 representation.
    value: Option<u64>,
    /// Sort order for the value
    order: Order,
}

impl Ord for DocValueAndOrder {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let invert = |cmp: std::cmp::Ordering| match self.order {
            Order::Asc => cmp,
            Order::Desc => cmp.reverse(),
        };

        match (self.value, other.value) {
            (Some(self_value), Some(other_value)) => invert(self_value.cmp(&other_value)),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for DocValueAndOrder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DocValueAndOrder {
    fn eq(&self, other: &Self) -> bool {
        self.value.cmp(&other.value) == std::cmp::Ordering::Equal
    }
}

impl Eq for DocValueAndOrder {}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct DocSortValuesAndFields {
    sorts: Vec<DocValueAndOrder>,

    #[serde(rename = "docvalue_fields")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    doc_value_fields: HashMap<String, FastFieldValue>,
}

impl Ord for DocSortValuesAndFields {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (self_feature, other_feature) in self.sorts.iter().zip(other.sorts.iter()) {
            let cmp = self_feature.cmp(other_feature);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl PartialOrd for DocSortValuesAndFields {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DocSortValuesAndFields {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for DocSortValuesAndFields {}

/// The TopHitsCollector used for collecting over segments and merging results.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TopHitsTopNComputer {
    req: TopHitsAggregationReq,
    top_n: TopNComputer<DocSortValuesAndFields, DocAddress, ReverseComparator>,
}

impl std::cmp::PartialEq for TopHitsTopNComputer {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl TopHitsTopNComputer {
    /// Create a new TopHitsCollector
    pub fn new(req: &TopHitsAggregationReq) -> Self {
        Self {
            top_n: TopNComputer::new_with_comparator(
                req.size + req.from.unwrap_or(0),
                ReverseComparator,
            ),
            req: req.clone(),
        }
    }

    fn collect(&mut self, features: DocSortValuesAndFields, doc: DocAddress) {
        self.top_n.push(features, doc);
    }

    pub(crate) fn merge_fruits(&mut self, other_fruit: Self) -> crate::Result<()> {
        for doc in other_fruit.top_n.into_vec() {
            self.collect(doc.sort_key, doc.doc);
        }
        Ok(())
    }

    /// Finalize by converting self into the final result form
    pub fn into_final_result(self) -> TopHitsMetricResult {
        let mut hits: Vec<TopHitsVecEntry> = self
            .top_n
            .into_sorted_vec()
            .into_iter()
            .map(|doc| TopHitsVecEntry {
                sort: doc.sort_key.sorts.iter().map(|f| f.value).collect(),
                doc_value_fields: doc
                    .sort_key
                    .doc_value_fields
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            })
            .collect();

        // Remove the first `from` elements
        // Truncating from end would be more efficient, but we need to truncate from the front
        // because `into_sorted_vec` gives us a descending order because of the inverted
        // `Ord` semantics of the heap elements.
        hits.drain(..self.req.from.unwrap_or(0));
        TopHitsMetricResult { hits }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TopHitsSegmentCollector {
    segment_ordinal: SegmentOrdinal,
    accessor_idx: usize,
    buckets: Vec<TopNComputer<Vec<DocValueAndOrder>, DocAddress, ReverseComparator>>,
    num_hits: usize,
}

impl TopHitsSegmentCollector {
    pub fn from_req(
        req: &TopHitsAggregationReq,
        accessor_idx: usize,
        segment_ordinal: SegmentOrdinal,
    ) -> Self {
        let num_hits = req.size + req.from.unwrap_or(0);
        Self {
            num_hits,
            segment_ordinal,
            accessor_idx,
            buckets: vec![TopNComputer::new_with_comparator(num_hits, ReverseComparator); 1],
        }
    }
    fn get_top_hits_computer(
        &mut self,
        parent_bucket_id: BucketId,
        value_accessors: &HashMap<String, Vec<DynamicColumn>>,
        req: &TopHitsAggregationReq,
    ) -> TopHitsTopNComputer {
        if parent_bucket_id as usize >= self.buckets.len() {
            return TopHitsTopNComputer::new(req);
        }
        let top_n = std::mem::replace(
            &mut self.buckets[parent_bucket_id as usize],
            TopNComputer::new(0),
        );
        let mut top_hits_computer = TopHitsTopNComputer::new(req);
        let top_results = top_n.into_vec();

        for res in top_results {
            let doc_value_fields = req.get_document_field_data(value_accessors, res.doc.doc_id);
            top_hits_computer.collect(
                DocSortValuesAndFields {
                    sorts: res.sort_key,
                    doc_value_fields,
                },
                res.doc,
            );
        }

        top_hits_computer
    }
}

impl SegmentAggregationCollector for TopHitsSegmentCollector {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut crate::aggregation::intermediate_agg_result::IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        let req_data = agg_data.get_top_hits_req_data(self.accessor_idx);

        let value_accessors = &req_data.value_accessors;

        let intermediate_result = IntermediateMetricResult::TopHits(self.get_top_hits_computer(
            parent_bucket_id,
            value_accessors,
            &req_data.req,
        ));
        results.push(
            req_data.name.to_string(),
            IntermediateAggregationResult::Metric(intermediate_result),
        )
    }

    /// TODO: Consider a caching layer to reduce the call overhead
    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let top_n = &mut self.buckets[parent_bucket_id as usize];
        let req_data = agg_data.get_top_hits_req_data(self.accessor_idx);
        let req = &req_data.req;
        let accessors = &req_data.accessors;
        for &doc_id in docs {
            // TODO: this is terrible, a new vec is allocated for every doc
            // We can fetch blocks instead
            // We don't need to store the order for every value
            let sorts: Vec<DocValueAndOrder> = req
                .sort
                .iter()
                .enumerate()
                .map(|(idx, KeyOrder { order, .. })| {
                    let order = *order;
                    let value = accessors
                        .get(idx)
                        .expect("could not find field in accessors")
                        .0
                        .values_for_doc(doc_id)
                        .next();
                    DocValueAndOrder { value, order }
                })
                .collect();

            top_n.push(
                sorts,
                DocAddress {
                    segment_ord: self.segment_ordinal,
                    doc_id,
                },
            );
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        self.buckets.resize(
            (max_bucket as usize) + 1,
            TopNComputer::new_with_comparator(self.num_hits, ReverseComparator),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::DateTime;
    use pretty_assertions::assert_eq;
    use serde_json::Value;
    use time::macros::datetime;

    use super::{DocSortValuesAndFields, DocValueAndOrder, Order};
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::bucket::tests::get_test_index_from_docs;
    use crate::aggregation::tests::get_test_index_from_values;
    use crate::aggregation::AggregationCollector;
    use crate::collector::sort_key::ReverseComparator;
    use crate::collector::ComparableDoc;
    use crate::query::AllQuery;
    use crate::schema::OwnedValue;

    fn invert_order(cmp_feature: DocValueAndOrder) -> DocValueAndOrder {
        let DocValueAndOrder { value, order } = cmp_feature;
        let order = match order {
            Order::Asc => Order::Desc,
            Order::Desc => Order::Asc,
        };
        DocValueAndOrder { value, order }
    }

    fn collector_with_capacity(capacity: usize) -> super::TopHitsTopNComputer {
        super::TopHitsTopNComputer {
            top_n: super::TopNComputer::new_with_comparator(capacity, ReverseComparator),
            req: Default::default(),
        }
    }

    fn invert_order_features(mut cmp_features: DocSortValuesAndFields) -> DocSortValuesAndFields {
        cmp_features.sorts = cmp_features
            .sorts
            .into_iter()
            .map(invert_order)
            .collect::<Vec<_>>();
        cmp_features
    }

    #[test]
    fn test_comparable_doc_feature() -> crate::Result<()> {
        let small = DocValueAndOrder {
            value: Some(1),
            order: Order::Asc,
        };
        let big = DocValueAndOrder {
            value: Some(2),
            order: Order::Asc,
        };
        let none = DocValueAndOrder {
            value: None,
            order: Order::Asc,
        };

        assert!(small < big);
        assert!(none < small);
        assert!(none < big);

        let small = invert_order(small);
        let big = invert_order(big);
        let none = invert_order(none);

        assert!(small > big);
        assert!(none < small);
        assert!(none < big);

        Ok(())
    }

    #[test]
    fn test_comparable_doc_features() -> crate::Result<()> {
        let features_1 = DocSortValuesAndFields {
            sorts: vec![DocValueAndOrder {
                value: Some(1),
                order: Order::Asc,
            }],
            doc_value_fields: Default::default(),
        };

        let features_2 = DocSortValuesAndFields {
            sorts: vec![DocValueAndOrder {
                value: Some(2),
                order: Order::Asc,
            }],
            doc_value_fields: Default::default(),
        };

        assert!(features_1 < features_2);

        assert!(invert_order_features(features_1.clone()) > invert_order_features(features_2));

        Ok(())
    }

    #[test]
    fn test_aggregation_top_hits_empty_index() -> crate::Result<()> {
        let values = vec![];

        let index = get_test_index_from_values(false, &values)?;

        let d: Aggregations = serde_json::from_value(json!({
            "top_hits_req": {
                "top_hits": {
                    "size": 2,
                    "sort": [
                        { "date": "desc" }
                    ],
                    "from": 0,
                }
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(d, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(
            &serde_json::to_string(&agg_res).expect("JSON serialization failed"),
        )
        .expect("JSON parsing failed");

        assert_eq!(
            res,
            json!({
                "top_hits_req": {
                    "hits": []
                }
            })
        );

        Ok(())
    }

    #[test]
    fn test_top_hits_collector_single_feature() -> crate::Result<()> {
        let docs = vec![
            ComparableDoc::<_, _> {
                doc: crate::DocAddress {
                    segment_ord: 0,
                    doc_id: 0,
                },
                sort_key: DocSortValuesAndFields {
                    sorts: vec![DocValueAndOrder {
                        value: Some(1),
                        order: Order::Asc,
                    }],
                    doc_value_fields: Default::default(),
                },
            },
            ComparableDoc {
                doc: crate::DocAddress {
                    segment_ord: 0,
                    doc_id: 2,
                },
                sort_key: DocSortValuesAndFields {
                    sorts: vec![DocValueAndOrder {
                        value: Some(3),
                        order: Order::Asc,
                    }],
                    doc_value_fields: Default::default(),
                },
            },
            ComparableDoc {
                doc: crate::DocAddress {
                    segment_ord: 0,
                    doc_id: 1,
                },
                sort_key: DocSortValuesAndFields {
                    sorts: vec![DocValueAndOrder {
                        value: Some(5),
                        order: Order::Asc,
                    }],
                    doc_value_fields: Default::default(),
                },
            },
        ];

        let mut collector = collector_with_capacity(3);
        for doc in docs.clone() {
            collector.collect(doc.sort_key, doc.doc);
        }

        let res = collector.into_final_result();

        assert_eq!(
            res,
            super::TopHitsMetricResult {
                hits: vec![
                    super::TopHitsVecEntry {
                        sort: vec![docs[0].sort_key.sorts[0].value],
                        doc_value_fields: Default::default(),
                    },
                    super::TopHitsVecEntry {
                        sort: vec![docs[1].sort_key.sorts[0].value],
                        doc_value_fields: Default::default(),
                    },
                    super::TopHitsVecEntry {
                        sort: vec![docs[2].sort_key.sorts[0].value],
                        doc_value_fields: Default::default(),
                    },
                ]
            }
        );

        Ok(())
    }

    fn test_aggregation_top_hits(merge_segments: bool) -> crate::Result<()> {
        let docs = vec![
            vec![
                r#"{ "date": "2015-01-02T00:00:00Z", "text": "bbb", "text2": "bbb", "mixed": { "dyn_arr": [1, "2"] } }"#,
                r#"{ "date": "2017-06-15T00:00:00Z", "text": "ccc", "text2": "ddd", "mixed": { "dyn_arr": [3, "4"] } }"#,
            ],
            vec![
                r#"{ "text": "aaa", "text2": "bbb", "date": "2018-01-02T00:00:00Z", "mixed": { "dyn_arr": ["9", 8] } }"#,
                r#"{ "text": "aaa", "text2": "bbb", "date": "2016-01-02T00:00:00Z", "mixed": { "dyn_arr": ["7", 6] } }"#,
            ],
        ];

        let index = get_test_index_from_docs(merge_segments, &docs)?;

        let d: Aggregations = serde_json::from_value(json!({
            "top_hits_req": {
                "top_hits": {
                    "size": 2,
                    "sort": [
                        { "date": "desc" }
                    ],
                    "from": 1,
                    "docvalue_fields": [
                        "date",
                        "tex*",
                        "mixed.*",
                    ],
                }
            }
        }))?;

        let collector = AggregationCollector::from_aggs(d, Default::default());
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg_res =
            serde_json::to_value(searcher.search(&AllQuery, &collector).unwrap()).unwrap();

        let date_2017 = datetime!(2017-06-15 00:00:00 UTC);
        let date_2016 = datetime!(2016-01-02 00:00:00 UTC);

        assert_eq!(
            agg_res["top_hits_req"],
            json!({
                "hits": [
                    {
                        "sort": [common::i64_to_u64(date_2017.unix_timestamp_nanos() as i64)],
                        "docvalue_fields": {
                            "date": [ OwnedValue::Date(DateTime::from_utc(date_2017)) ],
                            "text": [ "ccc" ],
                            "text2": [ "ddd" ],
                            "mixed.dyn_arr": [ 3, "4" ],
                        }
                    },
                    {
                        "sort": [common::i64_to_u64(date_2016.unix_timestamp_nanos() as i64)],
                        "docvalue_fields": {
                            "date": [ OwnedValue::Date(DateTime::from_utc(date_2016)) ],
                            "text": [ "aaa" ],
                            "text2": [ "bbb" ],
                            "mixed.dyn_arr": [ 6, "7" ],
                        }
                    }
                ]
            }),
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_top_hits_single_segment() -> crate::Result<()> {
        test_aggregation_top_hits(true)
    }

    #[test]
    fn test_aggregation_top_hits_multi_segment() -> crate::Result<()> {
        test_aggregation_top_hits(false)
    }
}
