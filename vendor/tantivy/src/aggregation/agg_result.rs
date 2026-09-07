//! Contains the final aggregation tree.
//!
//! This tree can be converted via the `into()` method from `IntermediateAggregationResults`.
//! This conversion computes the final result. For example: The intermediate result contains
//! intermediate average results, which is the sum and the number of values. The actual average is
//! calculated on the step from intermediate to final aggregation result tree.

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::bucket::GetDocCount;
use super::intermediate_agg_result::CompositeIntermediateKey;
use super::metric::{
    ExtendedStats, PercentilesMetricResult, SingleMetricResult, Stats, TopHitsMetricResult,
};
use super::{AggregationError, Key};
use crate::aggregation::bucket::AfterKey;
use crate::TantivyError;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
/// The final aggregation result.
pub struct AggregationResults(pub FxHashMap<String, AggregationResult>);

impl AggregationResults {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        self.0
            .values()
            .map(|agg| agg.get_bucket_count())
            .sum::<u64>()
    }

    pub(crate) fn get_value_from_aggregation(
        &self,
        name: &str,
        agg_property: &str,
    ) -> crate::Result<Option<f64>> {
        if let Some(agg) = self.0.get(name) {
            agg.get_value_from_aggregation(name, agg_property)
        } else {
            // Validation is be done during request parsing, so we can't reach this state.
            Err(TantivyError::InternalError(format!(
                "Can't find aggregation {name:?} in sub-aggregations"
            )))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// An aggregation is either a bucket or a metric.
pub enum AggregationResult {
    /// Bucket result variant.
    BucketResult(BucketResult),
    /// Metric result variant.
    MetricResult(MetricResult),
}

impl AggregationResult {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        match self {
            AggregationResult::BucketResult(bucket) => bucket.get_bucket_count(),
            AggregationResult::MetricResult(_) => 0,
        }
    }

    pub(crate) fn get_value_from_aggregation(
        &self,
        _name: &str,
        agg_property: &str,
    ) -> crate::Result<Option<f64>> {
        match self {
            AggregationResult::BucketResult(_bucket) => Err(TantivyError::InternalError(
                "Tried to retrieve value from bucket aggregation. This is not supported and \
                 should not happen during collection phase, but should be caught during validation"
                    .to_string(),
            )),
            AggregationResult::MetricResult(metric) => metric.get_value(agg_property),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// MetricResult
pub enum MetricResult {
    /// Average metric result.
    Average(SingleMetricResult),
    /// Count metric result.
    Count(SingleMetricResult),
    /// Max metric result.
    Max(SingleMetricResult),
    /// Min metric result.
    Min(SingleMetricResult),
    /// Stats metric result.
    Stats(Stats),
    /// ExtendedStats metric result.
    ExtendedStats(Box<ExtendedStats>),
    /// Sum metric result.
    Sum(SingleMetricResult),
    /// Percentiles metric result.
    Percentiles(PercentilesMetricResult),
    /// Top hits metric result
    TopHits(TopHitsMetricResult),
    /// Cardinality metric result
    Cardinality(SingleMetricResult),
}

impl MetricResult {
    fn get_value(&self, agg_property: &str) -> crate::Result<Option<f64>> {
        match self {
            MetricResult::Average(avg) => Ok(avg.value),
            MetricResult::Count(count) => Ok(count.value),
            MetricResult::Max(max) => Ok(max.value),
            MetricResult::Min(min) => Ok(min.value),
            MetricResult::Stats(stats) => stats.get_value(agg_property),
            MetricResult::ExtendedStats(extended_stats) => extended_stats.get_value(agg_property),
            MetricResult::Sum(sum) => Ok(sum.value),
            MetricResult::Percentiles(_) => Err(TantivyError::AggregationError(
                AggregationError::InvalidRequest("percentiles can't be used to order".to_string()),
            )),
            MetricResult::TopHits(_) => Err(TantivyError::AggregationError(
                AggregationError::InvalidRequest("top_hits can't be used to order".to_string()),
            )),
            MetricResult::Cardinality(card) => Ok(card.value),
        }
    }
}

/// BucketEntry holds bucket aggregation result types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BucketResult {
    /// This is the range entry for a bucket, which contains a key, count, from, to, and optionally
    /// sub-aggregations.
    Range {
        /// The range buckets sorted by range.
        buckets: BucketEntries<RangeBucketEntry>,
    },
    /// This is the histogram entry for a bucket, which contains a key, count, and optionally
    /// sub-aggregations.
    Histogram {
        /// The buckets.
        ///
        /// If there are holes depends on the request, if min_doc_count is 0, then there are no
        /// holes between the first and last bucket.
        /// See [`HistogramAggregation`](super::bucket::HistogramAggregation)
        buckets: BucketEntries<BucketEntry>,
    },
    /// This is the term result
    Terms {
        /// The buckets.
        ///
        /// See [`TermsAggregation`](super::bucket::TermsAggregation)
        buckets: Vec<BucketEntry>,
        /// The number of documents that didn’t make it into to TOP N due to shard_size or size
        sum_other_doc_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// The upper bound error for the doc count of each term.
        doc_count_error_upper_bound: Option<u64>,
    },
    /// This is the filter result - a single bucket with sub-aggregations
    Filter(FilterBucketResult),
    /// This is the composite result
    Composite {
        /// The buckets
        buckets: Vec<CompositeBucketEntry>,
        /// The key to start after when paginating
        #[serde(skip_serializing_if = "FxHashMap::is_empty")]
        after_key: FxHashMap<String, AfterKey>,
    },
}

impl BucketResult {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        match self {
            BucketResult::Range { buckets } => {
                buckets.iter().map(|bucket| bucket.get_bucket_count()).sum()
            }
            BucketResult::Histogram { buckets } => {
                buckets.iter().map(|bucket| bucket.get_bucket_count()).sum()
            }
            BucketResult::Terms {
                buckets,
                sum_other_doc_count: _,
                doc_count_error_upper_bound: _,
            } => buckets.iter().map(|bucket| bucket.get_bucket_count()).sum(),
            BucketResult::Filter(filter_result) => {
                // Filter doesn't add to bucket count - it's not a user-facing bucket
                // Only count sub-aggregation buckets
                filter_result.sub_aggregations.get_bucket_count()
            }
            BucketResult::Composite { buckets, .. } => {
                buckets.iter().map(|bucket| bucket.get_bucket_count()).sum()
            }
        }
    }
}

/// This is the wrapper of buckets entries, which can be vector or hashmap
/// depending on if it's keyed or not.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BucketEntries<T> {
    /// Vector format bucket entries
    Vec(Vec<T>),
    /// HashMap format bucket entries
    HashMap(FxHashMap<String, T>),
}

impl<T> BucketEntries<T> {
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a> {
        match self {
            BucketEntries::Vec(vec) => Box::new(vec.iter()),
            BucketEntries::HashMap(map) => Box::new(map.values()),
        }
    }
}

/// This is the default entry for a bucket, which contains a key, count, and optionally
/// sub-aggregations.
///
/// # JSON Format
/// ```json
/// {
///   ...
///     "my_histogram": {
///       "buckets": [
///         {
///           "key": "2.0",
///           "doc_count": 5
///         },
///         {
///           "key": "4.0",
///           "doc_count": 2
///         },
///         {
///           "key": "6.0",
///           "doc_count": 3
///         }
///       ]
///    }
///    ...
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The string representation of the bucket.
    pub key_as_string: Option<String>,
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// Sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
}
impl BucketEntry {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        1 + self.sub_aggregation.get_bucket_count()
    }
}
impl GetDocCount for &BucketEntry {
    fn doc_count(&self) -> u64 {
        self.doc_count
    }
}
impl GetDocCount for BucketEntry {
    fn doc_count(&self) -> u64 {
        self.doc_count
    }
}

/// This is the range entry for a bucket, which contains a key, count, and optionally
/// sub-aggregations.
///
/// # JSON Format
/// ```json
/// {
///   ...
///     "my_ranges": {
///       "buckets": [
///         {
///           "key": "*-10",
///           "to": 10,
///           "doc_count": 5
///         },
///         {
///           "key": "10-20",
///           "from": 10,
///           "to": 20,
///           "doc_count": 2
///         },
///         {
///           "key": "20-*",
///           "from": 20,
///           "doc_count": 3
///         }
///       ]
///    }
///    ...
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeBucketEntry {
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// Sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
    /// The from range of the bucket. Equals `f64::MIN` when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<f64>,
    /// The to range of the bucket. Equals `f64::MAX` when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<f64>,
    /// The optional string representation for the `from` range.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_as_string: Option<String>,
    /// The optional string representation for the `to` range.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_as_string: Option<String>,
}
impl RangeBucketEntry {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        1 + self.sub_aggregation.get_bucket_count()
    }
}

/// This is the filter bucket result, which contains the document count and sub-aggregations.
///
/// # JSON Format
/// ```json
/// {
///   "electronics_only": {
///     "doc_count": 2,
///     "avg_price": {
///       "value": 150.0
///     }
///   }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterBucketResult {
    /// Number of documents in the filter bucket
    pub doc_count: u64,
    /// Sub-aggregation results
    #[serde(flatten)]
    pub sub_aggregations: AggregationResults,
}

/// Note the type information loss compared to `CompositeIntermediateKey`.
/// Pagination is performed using `AfterKey`, which encodes type information.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompositeKey {
    /// Boolean key
    Bool(bool),
    /// String key
    Str(String),
    /// `i64` key
    I64(i64),
    /// `u64` key
    U64(u64),
    /// `f64` key
    F64(f64),
    /// Null key
    Null,
}
impl Eq for CompositeKey {}
impl std::hash::Hash for CompositeKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Self::Bool(val) => val.hash(state),
            Self::Str(text) => text.hash(state),
            Self::F64(val) => val.to_bits().hash(state),
            Self::U64(val) => val.hash(state),
            Self::I64(val) => val.hash(state),
            Self::Null => {}
        }
    }
}
impl PartialEq for CompositeKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(l), Self::Bool(r)) => l == r,
            (Self::Str(l), Self::Str(r)) => l == r,
            (Self::F64(l), Self::F64(r)) => l.to_bits() == r.to_bits(),
            (Self::I64(l), Self::I64(r)) => l == r,
            (Self::U64(l), Self::U64(r)) => l == r,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}
impl From<CompositeIntermediateKey> for CompositeKey {
    fn from(value: CompositeIntermediateKey) -> Self {
        match value {
            CompositeIntermediateKey::Str(s) => Self::Str(s),
            CompositeIntermediateKey::IpAddr(s) => {
                if let Some(ip) = s.to_ipv4_mapped() {
                    Self::Str(ip.to_string())
                } else {
                    Self::Str(s.to_string())
                }
            }
            CompositeIntermediateKey::F64(f) => Self::F64(f),
            CompositeIntermediateKey::Bool(f) => Self::Bool(f),
            CompositeIntermediateKey::U64(f) => Self::U64(f),
            CompositeIntermediateKey::I64(f) => Self::I64(f),
            CompositeIntermediateKey::DateTime(f) => Self::I64(f / 1_000_000), // ns to ms
            CompositeIntermediateKey::Null => Self::Null,
        }
    }
}

/// Composite bucket entry with a multi-dimensional key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompositeBucketEntry {
    /// The identifier of the bucket.
    pub key: FxHashMap<String, CompositeKey>,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// Sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
}

impl CompositeBucketEntry {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        1 + self.sub_aggregation.get_bucket_count()
    }
}
