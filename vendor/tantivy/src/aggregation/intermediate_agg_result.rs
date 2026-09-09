//! Contains the intermediate aggregation tree, that can be merged.
//! Intermediate aggregation results can be used to merge results between segments or between
//! indices.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::net::Ipv6Addr;

use columnar::ColumnType;
use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::agg_req::{Aggregation, AggregationVariants, Aggregations};
use super::agg_result::{AggregationResult, BucketResult, MetricResult, RangeBucketEntry};
use super::bucket::{
    composite_intermediate_key_ordering, cut_off_buckets, get_agg_name_and_property,
    intermediate_histogram_buckets_to_final_buckets, CompositeAggregation, GetDocCount,
    MissingOrder, Order, OrderTarget, RangeAggregation, TermsAggregation,
};
use super::metric::{
    IntermediateAverage, IntermediateCount, IntermediateExtendedStats, IntermediateMax,
    IntermediateMin, IntermediateStats, IntermediateSum, PercentilesCollector, TopHitsTopNComputer,
};
use super::segment_agg_result::AggregationLimitsGuard;
use super::{format_date, AggregationError, Key, SerializedKey};
use crate::aggregation::agg_result::{
    AggregationResults, BucketEntries, BucketEntry, CompositeBucketEntry, FilterBucketResult,
};
use crate::aggregation::bucket::TermsAggregationInternal;
use crate::aggregation::metric::CardinalityCollector;
use crate::TantivyError;

/// Contains the intermediate aggregation result, which is optimized to be merged with other
/// intermediate results.
///
/// Notice: This struct should not be de/serialized via JSON format.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateAggregationResults {
    pub(crate) aggs_res: FxHashMap<String, IntermediateAggregationResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq)]
/// The key to identify a bucket.
/// This might seem redundant with `Key`, but the point is to have a different
/// Serialize implementation.
pub enum IntermediateKey {
    /// Ip Addr key
    IpAddr(Ipv6Addr),
    /// Bool key
    Bool(bool),
    /// String key
    Str(String),
    /// `f64` key
    F64(f64),
    /// `i64` key
    I64(i64),
    /// `u64` key
    U64(u64),
}
impl From<Key> for IntermediateKey {
    fn from(value: Key) -> Self {
        match value {
            Key::Str(s) => Self::Str(s),
            Key::F64(f) => Self::F64(f),
            Key::U64(f) => Self::U64(f),
            Key::I64(f) => Self::I64(f),
        }
    }
}
impl From<IntermediateKey> for Key {
    fn from(value: IntermediateKey) -> Self {
        match value {
            IntermediateKey::Str(s) => Self::Str(s),
            IntermediateKey::IpAddr(s) => {
                // Prefer to use the IPv4 representation if possible
                if let Some(ip) = s.to_ipv4_mapped() {
                    Self::Str(ip.to_string())
                } else {
                    Self::Str(s.to_string())
                }
            }
            IntermediateKey::F64(f) => Self::F64(f),
            IntermediateKey::Bool(f) => Self::U64(f as u64),
            IntermediateKey::U64(f) => Self::U64(f),
            IntermediateKey::I64(f) => Self::I64(f),
        }
    }
}

impl Eq for IntermediateKey {}

impl std::fmt::Display for IntermediateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntermediateKey::Str(val) => f.write_str(val),
            IntermediateKey::F64(val) => f.write_str(&val.to_string()),
            IntermediateKey::U64(val) => f.write_str(&val.to_string()),
            IntermediateKey::I64(val) => f.write_str(&val.to_string()),
            IntermediateKey::Bool(val) => f.write_str(&val.to_string()),
            IntermediateKey::IpAddr(val) => f.write_str(&val.to_string()),
        }
    }
}

impl std::hash::Hash for IntermediateKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            IntermediateKey::Str(text) => text.hash(state),
            IntermediateKey::F64(val) => val.to_bits().hash(state),
            IntermediateKey::U64(val) => val.hash(state),
            IntermediateKey::I64(val) => val.hash(state),
            IntermediateKey::Bool(val) => val.hash(state),
            IntermediateKey::IpAddr(val) => val.hash(state),
        }
    }
}

impl IntermediateAggregationResults {
    /// Returns a reference to the intermediate aggregation result for the given key.
    pub fn get(&self, key: &str) -> Option<&IntermediateAggregationResult> {
        self.aggs_res.get(key)
    }

    /// Removes and returns the intermediate aggregation result for the given key.
    pub fn remove(&mut self, key: &str) -> Option<IntermediateAggregationResult> {
        self.aggs_res.remove(key)
    }

    /// Returns an iterator over the keys in the intermediate aggregation results.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.aggs_res.keys()
    }

    /// Add a result
    pub fn push(&mut self, key: String, value: IntermediateAggregationResult) -> crate::Result<()> {
        let entry = self.aggs_res.entry(key);
        match entry {
            Entry::Occupied(mut e) => {
                // In case of term aggregation over different types, we need to merge the results.
                e.get_mut().merge_fruits(value)?;
            }
            Entry::Vacant(e) => {
                e.insert(value);
            }
        }
        Ok(())
    }

    /// Convert intermediate result and its aggregation request to the final result.
    pub fn into_final_result(
        self,
        req: Aggregations,
        mut limits: AggregationLimitsGuard,
    ) -> crate::Result<AggregationResults> {
        let res = self.into_final_result_internal(&req, &mut limits)?;
        let bucket_count = res.get_bucket_count() as u32;
        if bucket_count > limits.get_bucket_limit() {
            return Err(TantivyError::AggregationError(
                AggregationError::BucketLimitExceeded {
                    limit: limits.get_bucket_limit(),
                    current: bucket_count,
                },
            ));
        }
        Ok(res)
    }

    /// Convert intermediate result and its aggregation request to the final result.
    pub(crate) fn into_final_result_internal(
        self,
        req: &Aggregations,
        limits: &mut AggregationLimitsGuard,
    ) -> crate::Result<AggregationResults> {
        let mut results: FxHashMap<String, AggregationResult> = FxHashMap::default();
        for (key, agg_res) in self.aggs_res.into_iter() {
            let req = req.get(key.as_str()).unwrap_or_else(|| {
                panic!(
                    "Could not find key {:?} in request keys {:?}. This probably means that \
                     add_intermediate_aggregation_result passed the wrong agg object.",
                    key,
                    req.keys().collect::<Vec<_>>()
                )
            });
            results.insert(key, agg_res.into_final_result(req, limits)?);
        }
        // Handle empty results
        if results.len() != req.len() {
            for (key, req) in req.iter() {
                if !results.contains_key(key) {
                    let empty_res = empty_from_req(req);
                    results.insert(key.to_string(), empty_res.into_final_result(req, limits)?);
                }
            }
        }

        Ok(AggregationResults(results))
    }

    pub(crate) fn empty_from_req(req: &Aggregations) -> Self {
        let mut aggs_res: FxHashMap<String, IntermediateAggregationResult> = FxHashMap::default();
        for (key, req) in req.iter() {
            let empty_res = empty_from_req(req);
            aggs_res.insert(key.to_string(), empty_res);
        }

        Self { aggs_res }
    }

    /// Merge another intermediate aggregation result into this result.
    pub fn merge_fruits(&mut self, mut other: IntermediateAggregationResults) -> crate::Result<()> {
        for (key, left) in self.aggs_res.iter_mut() {
            if let Some(key) = other.aggs_res.remove(key) {
                left.merge_fruits(key)?;
            }
        }
        // Move remainder of other aggs_res into self.
        // Note: Currently we don't expect this to happen, as we create empty intermediate results
        // via [IntermediateAggregationResults::empty_from_req].
        for (key, value) in other.aggs_res {
            self.aggs_res.insert(key, value);
        }
        Ok(())
    }
}

pub(crate) fn empty_from_req(req: &Aggregation) -> IntermediateAggregationResult {
    use AggregationVariants::*;
    match req.agg {
        Terms(_) => IntermediateAggregationResult::Bucket(IntermediateBucketResult::Terms {
            buckets: Default::default(),
        }),
        Range(_) => IntermediateAggregationResult::Bucket(IntermediateBucketResult::Range(
            Default::default(),
        )),
        Histogram(_) => {
            IntermediateAggregationResult::Bucket(IntermediateBucketResult::Histogram {
                buckets: Vec::new(),
                is_date_agg: false,
            })
        }
        DateHistogram(_) => {
            IntermediateAggregationResult::Bucket(IntermediateBucketResult::Histogram {
                buckets: Vec::new(),
                is_date_agg: true,
            })
        }
        Average(_) => IntermediateAggregationResult::Metric(IntermediateMetricResult::Average(
            IntermediateAverage::default(),
        )),
        Count(_) => IntermediateAggregationResult::Metric(IntermediateMetricResult::Count(
            IntermediateCount::default(),
        )),
        Max(_) => IntermediateAggregationResult::Metric(IntermediateMetricResult::Max(
            IntermediateMax::default(),
        )),
        Min(_) => IntermediateAggregationResult::Metric(IntermediateMetricResult::Min(
            IntermediateMin::default(),
        )),
        Stats(_) => IntermediateAggregationResult::Metric(IntermediateMetricResult::Stats(
            IntermediateStats::default(),
        )),
        ExtendedStats(_) => IntermediateAggregationResult::Metric(
            IntermediateMetricResult::ExtendedStats(IntermediateExtendedStats::default()),
        ),
        Sum(_) => IntermediateAggregationResult::Metric(IntermediateMetricResult::Sum(
            IntermediateSum::default(),
        )),
        Percentiles(_) => IntermediateAggregationResult::Metric(
            IntermediateMetricResult::Percentiles(PercentilesCollector::default()),
        ),
        TopHits(ref req) => IntermediateAggregationResult::Metric(
            IntermediateMetricResult::TopHits(TopHitsTopNComputer::new(req)),
        ),
        Cardinality(_) => IntermediateAggregationResult::Metric(
            IntermediateMetricResult::Cardinality(CardinalityCollector::default()),
        ),
        Filter(_) => IntermediateAggregationResult::Bucket(IntermediateBucketResult::Filter {
            doc_count: 0,
            sub_aggregations: IntermediateAggregationResults::default(),
        }),
        Composite(_) => {
            IntermediateAggregationResult::Bucket(IntermediateBucketResult::Composite {
                buckets: IntermediateCompositeBucketResult::default(),
            })
        }
    }
}

/// An aggregation is either a bucket or a metric.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum IntermediateAggregationResult {
    /// Bucket variant
    Bucket(IntermediateBucketResult),
    /// Metric variant
    Metric(IntermediateMetricResult),
}

impl IntermediateAggregationResult {
    pub(crate) fn into_final_result(
        self,
        req: &Aggregation,
        limits: &mut AggregationLimitsGuard,
    ) -> crate::Result<AggregationResult> {
        let res = match self {
            IntermediateAggregationResult::Bucket(bucket) => {
                AggregationResult::BucketResult(bucket.into_final_bucket_result(req, limits)?)
            }
            IntermediateAggregationResult::Metric(metric) => {
                AggregationResult::MetricResult(metric.into_final_metric_result(req))
            }
        };
        Ok(res)
    }
    fn merge_fruits(&mut self, other: IntermediateAggregationResult) -> crate::Result<()> {
        match (self, other) {
            (
                IntermediateAggregationResult::Bucket(b1),
                IntermediateAggregationResult::Bucket(b2),
            ) => b1.merge_fruits(b2),
            (
                IntermediateAggregationResult::Metric(m1),
                IntermediateAggregationResult::Metric(m2),
            ) => m1.merge_fruits(m2),
            _ => panic!("aggregation result type mismatch (mixed metric and buckets)"),
        }
    }
}

/// Holds the intermediate data for metric results
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IntermediateMetricResult {
    /// Intermediate average result.
    Percentiles(PercentilesCollector),
    /// Intermediate average result.
    Average(IntermediateAverage),
    /// Intermediate count result.
    Count(IntermediateCount),
    /// Intermediate max result.
    Max(IntermediateMax),
    /// Intermediate min result.
    Min(IntermediateMin),
    /// Intermediate stats result.
    Stats(IntermediateStats),
    /// Intermediate stats result.
    ExtendedStats(IntermediateExtendedStats),
    /// Intermediate sum result.
    Sum(IntermediateSum),
    /// Intermediate top_hits result
    TopHits(TopHitsTopNComputer),
    /// Intermediate cardinality result
    Cardinality(CardinalityCollector),
}

impl IntermediateMetricResult {
    fn into_final_metric_result(self, req: &Aggregation) -> MetricResult {
        match self {
            IntermediateMetricResult::Average(intermediate_avg) => {
                MetricResult::Average(intermediate_avg.finalize().into())
            }
            IntermediateMetricResult::Count(intermediate_count) => {
                MetricResult::Count(intermediate_count.finalize().into())
            }
            IntermediateMetricResult::Max(intermediate_max) => {
                MetricResult::Max(intermediate_max.finalize().into())
            }
            IntermediateMetricResult::Min(intermediate_min) => {
                MetricResult::Min(intermediate_min.finalize().into())
            }
            IntermediateMetricResult::Stats(intermediate_stats) => {
                MetricResult::Stats(intermediate_stats.finalize())
            }
            IntermediateMetricResult::ExtendedStats(intermediate_stats) => {
                MetricResult::ExtendedStats(intermediate_stats.finalize())
            }
            IntermediateMetricResult::Sum(intermediate_sum) => {
                MetricResult::Sum(intermediate_sum.finalize().into())
            }
            IntermediateMetricResult::Percentiles(percentiles) => MetricResult::Percentiles(
                percentiles
                    .into_final_result(req.agg.as_percentile().expect("unexpected metric type")),
            ),
            IntermediateMetricResult::TopHits(top_hits) => {
                MetricResult::TopHits(top_hits.into_final_result())
            }
            IntermediateMetricResult::Cardinality(cardinality) => {
                MetricResult::Cardinality(cardinality.finalize().into())
            }
        }
    }

    // TODO: this is our top-of-the-chain fruit merge mech
    fn merge_fruits(&mut self, other: IntermediateMetricResult) -> crate::Result<()> {
        match (self, other) {
            (
                IntermediateMetricResult::Average(avg_left),
                IntermediateMetricResult::Average(avg_right),
            ) => {
                avg_left.merge_fruits(avg_right);
            }
            (
                IntermediateMetricResult::Count(count_left),
                IntermediateMetricResult::Count(count_right),
            ) => {
                count_left.merge_fruits(count_right);
            }
            (IntermediateMetricResult::Max(max_left), IntermediateMetricResult::Max(max_right)) => {
                max_left.merge_fruits(max_right);
            }
            (IntermediateMetricResult::Min(min_left), IntermediateMetricResult::Min(min_right)) => {
                min_left.merge_fruits(min_right);
            }
            (
                IntermediateMetricResult::Stats(stats_left),
                IntermediateMetricResult::Stats(stats_right),
            ) => {
                stats_left.merge_fruits(stats_right);
            }
            (
                IntermediateMetricResult::ExtendedStats(extended_stats_left),
                IntermediateMetricResult::ExtendedStats(extended_stats_right),
            ) => {
                extended_stats_left.merge_fruits(extended_stats_right);
            }
            (IntermediateMetricResult::Sum(sum_left), IntermediateMetricResult::Sum(sum_right)) => {
                sum_left.merge_fruits(sum_right);
            }
            (
                IntermediateMetricResult::Percentiles(left),
                IntermediateMetricResult::Percentiles(right),
            ) => {
                left.merge_fruits(right)?;
            }
            (IntermediateMetricResult::TopHits(left), IntermediateMetricResult::TopHits(right)) => {
                left.merge_fruits(right)?;
            }
            (
                IntermediateMetricResult::Cardinality(left),
                IntermediateMetricResult::Cardinality(right),
            ) => {
                left.merge_fruits(right)?;
            }
            _ => {
                panic!("incompatible fruit types in tree or missing merge_fruits handler");
            }
        }

        Ok(())
    }
}

/// The intermediate bucket results. Internally they can be easily merged via the keys of the
/// buckets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IntermediateBucketResult {
    /// This is the range entry for a bucket, which contains a key, count, from, to, and optionally
    /// sub_aggregations.
    Range(IntermediateRangeBucketResult),
    /// This is the histogram entry for a bucket, which contains a key, count, and optionally
    /// sub_aggregations.
    Histogram {
        /// The column_type of the underlying `Column` is DateTime
        is_date_agg: bool,
        /// The histogram buckets
        buckets: Vec<IntermediateHistogramBucketEntry>,
    },
    /// Term aggregation
    Terms {
        /// The term buckets
        buckets: IntermediateTermBucketResult,
    },
    /// Filter aggregation - a single bucket with sub-aggregations
    Filter {
        /// Document count in the filter bucket
        doc_count: u64,
        /// Sub-aggregation results
        sub_aggregations: IntermediateAggregationResults,
    },
    /// Composite aggregation
    Composite {
        /// The composite buckets
        buckets: IntermediateCompositeBucketResult,
    },
}

impl IntermediateBucketResult {
    pub(crate) fn into_final_bucket_result(
        self,
        req: &Aggregation,
        limits: &mut AggregationLimitsGuard,
    ) -> crate::Result<BucketResult> {
        match self {
            IntermediateBucketResult::Range(range_res) => {
                let mut buckets: Vec<RangeBucketEntry> = range_res
                    .buckets
                    .into_values()
                    .map(|bucket| {
                        bucket.into_final_bucket_entry(
                            req.sub_aggregation(),
                            req.agg
                                .as_range()
                                .expect("unexpected aggregation, expected histogram aggregation"),
                            range_res.column_type,
                            limits,
                        )
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                buckets.sort_by(|left, right| {
                    left.from
                        .unwrap_or(f64::MIN)
                        .total_cmp(&right.from.unwrap_or(f64::MIN))
                });

                let is_keyed = req
                    .agg
                    .as_range()
                    .expect("unexpected aggregation, expected range aggregation")
                    .keyed;
                let buckets = if is_keyed {
                    let mut bucket_map =
                        FxHashMap::with_capacity_and_hasher(buckets.len(), Default::default());
                    for bucket in buckets {
                        bucket_map.insert(bucket.key.to_string(), bucket);
                    }
                    BucketEntries::HashMap(bucket_map)
                } else {
                    BucketEntries::Vec(buckets)
                };
                Ok(BucketResult::Range { buckets })
            }
            IntermediateBucketResult::Histogram {
                is_date_agg,
                buckets,
            } => {
                let histogram_req = &req
                    .agg
                    .as_histogram()?
                    .expect("unexpected aggregation, expected histogram aggregation");
                let buckets = intermediate_histogram_buckets_to_final_buckets(
                    buckets,
                    is_date_agg,
                    histogram_req,
                    req.sub_aggregation(),
                    limits,
                )?;

                let buckets = if histogram_req.keyed {
                    let mut bucket_map =
                        FxHashMap::with_capacity_and_hasher(buckets.len(), Default::default());
                    for bucket in buckets {
                        bucket_map.insert(bucket.key.to_string(), bucket);
                    }
                    BucketEntries::HashMap(bucket_map)
                } else {
                    BucketEntries::Vec(buckets)
                };
                Ok(BucketResult::Histogram { buckets })
            }
            IntermediateBucketResult::Terms { buckets: terms } => terms.into_final_result(
                req.agg
                    .as_term()
                    .expect("unexpected aggregation, expected term aggregation"),
                req.sub_aggregation(),
                limits,
            ),
            IntermediateBucketResult::Filter {
                doc_count,
                sub_aggregations,
            } => {
                // Convert sub-aggregation results to final format
                let final_sub_aggregations = sub_aggregations
                    .into_final_result(req.sub_aggregation().clone(), limits.clone())?;
                Ok(BucketResult::Filter(FilterBucketResult {
                    doc_count,
                    sub_aggregations: final_sub_aggregations,
                }))
            }
            IntermediateBucketResult::Composite { buckets } => {
                let composite_req = req
                    .agg
                    .as_composite()
                    .expect("unexpected aggregation, expected composite aggregation");
                buckets.into_final_result(composite_req, req.sub_aggregation(), limits)
            }
        }
    }

    fn merge_fruits(&mut self, other: IntermediateBucketResult) -> crate::Result<()> {
        match (self, other) {
            (
                IntermediateBucketResult::Terms {
                    buckets: term_res_left,
                },
                IntermediateBucketResult::Terms {
                    buckets: term_res_right,
                },
            ) => {
                merge_maps(&mut term_res_left.entries, term_res_right.entries)?;
                term_res_left.sum_other_doc_count += term_res_right.sum_other_doc_count;
                term_res_left.doc_count_error_upper_bound +=
                    term_res_right.doc_count_error_upper_bound;
            }

            (
                IntermediateBucketResult::Range(range_res_left),
                IntermediateBucketResult::Range(range_res_right),
            ) => {
                merge_maps(&mut range_res_left.buckets, range_res_right.buckets)?;
            }
            (
                IntermediateBucketResult::Histogram {
                    buckets: buckets_left,
                    is_date_agg: _,
                },
                IntermediateBucketResult::Histogram {
                    buckets: buckets_right,
                    is_date_agg: _,
                },
            ) => {
                let buckets: Result<Vec<IntermediateHistogramBucketEntry>, TantivyError> =
                    buckets_left
                        .drain(..)
                        .merge_join_by(buckets_right, |left, right| {
                            left.key.partial_cmp(&right.key).unwrap_or(Ordering::Equal)
                        })
                        .map(|either| match either {
                            itertools::EitherOrBoth::Both(mut left, right) => {
                                left.merge_fruits(right)?;
                                Ok(left)
                            }
                            itertools::EitherOrBoth::Left(left) => Ok(left),
                            itertools::EitherOrBoth::Right(right) => Ok(right),
                        })
                        .collect::<Result<_, _>>();

                *buckets_left = buckets?;
            }
            (
                IntermediateBucketResult::Filter {
                    doc_count: doc_count_left,
                    sub_aggregations: sub_aggs_left,
                },
                IntermediateBucketResult::Filter {
                    doc_count: doc_count_right,
                    sub_aggregations: sub_aggs_right,
                },
            ) => {
                *doc_count_left += doc_count_right;
                sub_aggs_left.merge_fruits(sub_aggs_right)?;
            }
            (
                IntermediateBucketResult::Composite {
                    buckets: composite_left,
                },
                IntermediateBucketResult::Composite {
                    buckets: composite_right,
                },
            ) => {
                composite_left.merge_fruits(composite_right)?;
            }
            (IntermediateBucketResult::Range(_), _) => {
                panic!("try merge on different types")
            }
            (IntermediateBucketResult::Histogram { .. }, _) => {
                panic!("try merge on different types")
            }
            (IntermediateBucketResult::Terms { .. }, _) => {
                panic!("try merge on different types")
            }
            (IntermediateBucketResult::Filter { .. }, _) => {
                panic!("try merge on different types")
            }
            (IntermediateBucketResult::Composite { .. }, _) => {
                panic!("try merge on different types")
            }
        }
        Ok(())
    }
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Range aggregation including error counts
pub struct IntermediateRangeBucketResult {
    pub(crate) buckets: FxHashMap<SerializedKey, IntermediateRangeBucketEntry>,
    pub(crate) column_type: Option<ColumnType>,
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Term aggregation including error counts
pub struct IntermediateTermBucketResult {
    pub(crate) entries: FxHashMap<IntermediateKey, IntermediateTermBucketEntry>,
    pub(crate) sum_other_doc_count: u64,
    pub(crate) doc_count_error_upper_bound: u64,
}

impl IntermediateTermBucketResult {
    /// Returns a reference to the map of bucket entries keyed by [`IntermediateKey`].
    pub fn entries(&self) -> &FxHashMap<IntermediateKey, IntermediateTermBucketEntry> {
        &self.entries
    }

    /// Returns the count of documents not included in the returned buckets.
    pub fn sum_other_doc_count(&self) -> u64 {
        self.sum_other_doc_count
    }

    /// Returns the upper bound of the error on document counts in the returned buckets.
    pub fn doc_count_error_upper_bound(&self) -> u64 {
        self.doc_count_error_upper_bound
    }

    pub(crate) fn into_final_result(
        self,
        req: &TermsAggregation,
        sub_aggregation_req: &Aggregations,
        limits: &mut AggregationLimitsGuard,
    ) -> crate::Result<BucketResult> {
        let req = TermsAggregationInternal::from_req(req);
        let mut buckets: Vec<BucketEntry> = self
            .entries
            .into_iter()
            .filter(|bucket| bucket.1.doc_count as u64 >= req.min_doc_count)
            .map(|(key, entry)| {
                let key_as_string = match key {
                    IntermediateKey::Bool(key) => {
                        let val = if key { "true" } else { "false" };
                        Some(val.to_string())
                    }
                    _ => None,
                };
                Ok(BucketEntry {
                    key_as_string,
                    key: key.into(),
                    doc_count: entry.doc_count as u64,
                    sub_aggregation: entry
                        .sub_aggregation
                        .into_final_result_internal(sub_aggregation_req, limits)?,
                })
            })
            .collect::<crate::Result<_>>()?;

        let order = req.order.order;
        match req.order.target {
            OrderTarget::Key => {
                buckets.sort_by(|left, right| {
                    if req.order.order == Order::Asc {
                        left.key.partial_cmp(&right.key)
                    } else {
                        right.key.partial_cmp(&left.key)
                    }
                    .expect("expected type string, which is always sortable")
                });
            }
            OrderTarget::Count => {
                if req.order.order == Order::Desc {
                    buckets.sort_unstable_by_key(|bucket| std::cmp::Reverse(bucket.doc_count()));
                } else {
                    buckets.sort_unstable_by_key(|bucket| bucket.doc_count());
                }
            }
            OrderTarget::SubAggregation(name) => {
                let (agg_name, agg_property) = get_agg_name_and_property(&name);
                let mut buckets_with_val = buckets
                    .into_iter()
                    .map(|bucket| {
                        let val = bucket
                            .sub_aggregation
                            .get_value_from_aggregation(agg_name, agg_property)?
                            .unwrap_or(f64::MIN);
                        Ok((bucket, val))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                buckets_with_val.sort_by(|(_, val1), (_, val2)| match &order {
                    Order::Desc => val2.total_cmp(val1),
                    Order::Asc => val1.total_cmp(val2),
                });
                buckets = buckets_with_val
                    .into_iter()
                    .map(|(bucket, _val)| bucket)
                    .collect_vec();
            }
        }

        // We ignore _term_doc_count_before_cutoff here, because it increases the upperbound error
        // only for terms that didn't make it into the top N.
        //
        // This can be interesting, as a value of quality of the results, but not good to check the
        // actual error count for the returned terms.
        let (_term_doc_count_before_cutoff, sum_other_doc_count) =
            cut_off_buckets(&mut buckets, req.size as usize);

        let doc_count_error_upper_bound = if req.show_term_doc_count_error {
            Some(self.doc_count_error_upper_bound)
        } else {
            None
        };

        Ok(BucketResult::Terms {
            buckets,
            sum_other_doc_count: self.sum_other_doc_count + sum_other_doc_count,
            doc_count_error_upper_bound,
        })
    }
}

trait MergeFruits {
    fn merge_fruits(&mut self, other: Self) -> crate::Result<()>;
}

fn merge_maps<V: MergeFruits + Clone, T: Eq + PartialEq + Hash>(
    entries_left: &mut FxHashMap<T, V>,
    mut entries_right: FxHashMap<T, V>,
) -> crate::Result<()> {
    for (name, entry_left) in entries_left.iter_mut() {
        if let Some(entry_right) = entries_right.remove(name) {
            entry_left.merge_fruits(entry_right)?;
        }
    }

    for (key, res) in entries_right.into_iter() {
        entries_left.entry(key).or_insert(res);
    }
    Ok(())
}

/// This is the histogram entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateHistogramBucketEntry {
    /// The unique the bucket is identified.
    pub key: f64,
    /// The number of documents in the bucket.
    pub doc_count: u64,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation: IntermediateAggregationResults,
}

impl IntermediateHistogramBucketEntry {
    pub(crate) fn into_final_bucket_entry(
        self,
        req: &Aggregations,
        limits: &mut AggregationLimitsGuard,
    ) -> crate::Result<BucketEntry> {
        Ok(BucketEntry {
            key_as_string: None,
            key: Key::F64(self.key),
            doc_count: self.doc_count,
            sub_aggregation: self
                .sub_aggregation
                .into_final_result_internal(req, limits)?,
        })
    }
}

/// This is the range entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateRangeBucketEntry {
    /// The unique key the bucket is identified with.
    pub key: IntermediateKey,
    /// The number of documents in the bucket.
    pub doc_count: u64,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation_res: IntermediateAggregationResults,
    /// The from range of the bucket. Equals `f64::MIN` when `None`.
    pub from: Option<f64>,
    /// The to range of the bucket. Equals `f64::MAX` when `None`.
    pub to: Option<f64>,
}

impl IntermediateRangeBucketEntry {
    pub(crate) fn into_final_bucket_entry(
        self,
        req: &Aggregations,
        _range_req: &RangeAggregation,
        column_type: Option<ColumnType>,
        limits: &mut AggregationLimitsGuard,
    ) -> crate::Result<RangeBucketEntry> {
        let mut range_bucket_entry = RangeBucketEntry {
            key: self.key.into(),
            doc_count: self.doc_count,
            sub_aggregation: self
                .sub_aggregation_res
                .into_final_result_internal(req, limits)?,
            to: self.to,
            from: self.from,
            to_as_string: None,
            from_as_string: None,
        };

        // If we have a date type on the histogram buckets, we add the `key_as_string` field as
        // rfc3339
        if column_type == Some(ColumnType::DateTime) {
            if let Some(val) = range_bucket_entry.to {
                let key_as_string = format_date(val as i64)?;
                range_bucket_entry.to_as_string = Some(key_as_string);
            }
            if let Some(val) = range_bucket_entry.from {
                let key_as_string = format_date(val as i64)?;
                range_bucket_entry.from_as_string = Some(key_as_string);
            }
        }

        Ok(range_bucket_entry)
    }
}

/// This is the term entry for a bucket, which contains a count, and optionally
/// sub_aggregations.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateTermBucketEntry {
    /// The number of documents in the bucket.
    pub doc_count: u64,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation: IntermediateAggregationResults,
}

impl MergeFruits for IntermediateTermBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateTermBucketEntry) -> crate::Result<()> {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation)?;
        Ok(())
    }
}

impl MergeFruits for IntermediateRangeBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateRangeBucketEntry) -> crate::Result<()> {
        self.doc_count += other.doc_count;
        self.sub_aggregation_res
            .merge_fruits(other.sub_aggregation_res)?;
        Ok(())
    }
}

impl MergeFruits for IntermediateHistogramBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateHistogramBucketEntry) -> crate::Result<()> {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation)?;
        Ok(())
    }
}

/// Entry for the composite bucket.
pub type IntermediateCompositeBucketEntry = IntermediateTermBucketEntry;

/// The fully typed key for composite aggregation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CompositeIntermediateKey {
    /// Bool key
    Bool(bool),
    /// String key
    Str(String),
    /// Float key
    F64(f64),
    /// Signed integer key
    I64(i64),
    /// Unsigned integer key
    U64(u64),
    /// DateTime key, nanoseconds since epoch
    DateTime(i64),
    /// IP Address key
    IpAddr(Ipv6Addr),
    /// Missing value key
    Null,
}

impl Eq for CompositeIntermediateKey {}

impl std::hash::Hash for CompositeIntermediateKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            CompositeIntermediateKey::Bool(val) => val.hash(state),
            CompositeIntermediateKey::Str(text) => text.hash(state),
            CompositeIntermediateKey::F64(val) => val.to_bits().hash(state),
            CompositeIntermediateKey::U64(val) => val.hash(state),
            CompositeIntermediateKey::I64(val) => val.hash(state),
            CompositeIntermediateKey::DateTime(val) => val.hash(state),
            CompositeIntermediateKey::IpAddr(val) => val.hash(state),
            CompositeIntermediateKey::Null => {}
        }
    }
}

/// Composite aggregation page.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateCompositeBucketResult {
    pub(crate) entries: FxHashMap<Vec<CompositeIntermediateKey>, IntermediateCompositeBucketEntry>,
    pub(crate) target_size: u32,
    pub(crate) orders: Vec<(Order, MissingOrder)>,
}

impl IntermediateCompositeBucketResult {
    pub(crate) fn into_final_result(
        self,
        req: &CompositeAggregation,
        sub_aggregation_req: &Aggregations,
        limits: &mut AggregationLimitsGuard,
    ) -> crate::Result<BucketResult> {
        let trimmed_entry_vec =
            trim_composite_buckets(self.entries, &self.orders, self.target_size)?;
        let after_key = if trimmed_entry_vec.len() == req.size as usize {
            trimmed_entry_vec
                .last()
                .map(|bucket| {
                    let (intermediate_key, _entry) = bucket;
                    intermediate_key
                        .iter()
                        .enumerate()
                        .map(|(idx, intermediate_key)| {
                            let source = &req.sources[idx];
                            (source.name().to_string(), intermediate_key.clone().into())
                        })
                        .collect()
                })
                .unwrap()
        } else {
            FxHashMap::default()
        };

        let buckets = trimmed_entry_vec
            .into_iter()
            .map(|(intermediate_key, entry)| {
                let key = intermediate_key
                    .into_iter()
                    .enumerate()
                    .map(|(idx, intermediate_key)| {
                        let source = &req.sources[idx];
                        (source.name().to_string(), intermediate_key.into())
                    })
                    .collect();
                Ok(CompositeBucketEntry {
                    key,
                    doc_count: entry.doc_count as u64,
                    sub_aggregation: entry
                        .sub_aggregation
                        .into_final_result_internal(sub_aggregation_req, limits)?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(BucketResult::Composite { after_key, buckets })
    }

    fn merge_fruits(&mut self, other: IntermediateCompositeBucketResult) -> crate::Result<()> {
        merge_maps(&mut self.entries, other.entries)?;
        if self.entries.len() as u32 > 2 * self.target_size {
            self.trim()?;
        }
        Ok(())
    }

    /// Trim the composite buckets to the target size, according to the ordering.
    pub(crate) fn trim(&mut self) -> crate::Result<()> {
        if self.entries.len() as u32 <= self.target_size {
            return Ok(());
        }

        let sorted_entries = trim_composite_buckets(
            std::mem::take(&mut self.entries),
            &self.orders,
            self.target_size,
        )?;

        self.entries = sorted_entries.into_iter().collect();
        Ok(())
    }
}

fn trim_composite_buckets(
    entries: FxHashMap<Vec<CompositeIntermediateKey>, IntermediateCompositeBucketEntry>,
    orders: &[(Order, MissingOrder)],
    target_size: u32,
) -> crate::Result<
    Vec<(
        Vec<CompositeIntermediateKey>,
        IntermediateCompositeBucketEntry,
    )>,
> {
    let mut entries: Vec<_> = entries.into_iter().collect();
    let mut sort_error: Option<TantivyError> = None;
    entries.sort_by(|(left_key, _), (right_key, _)| {
        if sort_error.is_some() {
            return Ordering::Equal;
        }

        for idx in 0..orders.len() {
            match composite_intermediate_key_ordering(
                &left_key[idx],
                &right_key[idx],
                orders[idx].0,
                orders[idx].1,
            ) {
                Ok(ordering) if ordering != Ordering::Equal => return ordering,
                Ok(_) => continue,
                Err(err) => {
                    sort_error = Some(err);
                    break;
                }
            }
        }
        Ordering::Equal
    });

    if let Some(err) = sort_error {
        return Err(err);
    }

    entries.truncate(target_size as usize);
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pretty_assertions::assert_eq;

    use super::*;

    fn get_sub_test_tree(data: &[(String, u64)]) -> IntermediateAggregationResults {
        let mut map = HashMap::new();
        let mut buckets = FxHashMap::default();
        for (key, doc_count) in data {
            buckets.insert(
                key.to_string(),
                IntermediateRangeBucketEntry {
                    key: IntermediateKey::Str(key.to_string()),
                    doc_count: *doc_count,
                    sub_aggregation_res: Default::default(),
                    from: None,
                    to: None,
                },
            );
        }
        map.insert(
            "my_agg_level2".to_string(),
            IntermediateAggregationResult::Bucket(IntermediateBucketResult::Range(
                IntermediateRangeBucketResult {
                    buckets,
                    column_type: None,
                },
            )),
        );
        IntermediateAggregationResults {
            aggs_res: map.into_iter().collect(),
        }
    }

    fn get_intermediate_tree_with_ranges(
        data: &[(String, u64, String, u64)],
    ) -> IntermediateAggregationResults {
        let mut map = HashMap::new();
        let mut buckets: FxHashMap<_, _> = Default::default();
        for (key, doc_count, sub_aggregation_key, sub_aggregation_count) in data {
            buckets.insert(
                key.to_string(),
                IntermediateRangeBucketEntry {
                    key: IntermediateKey::Str(key.to_string()),
                    doc_count: *doc_count,
                    from: None,
                    to: None,
                    sub_aggregation_res: get_sub_test_tree(&[(
                        sub_aggregation_key.to_string(),
                        *sub_aggregation_count,
                    )]),
                },
            );
        }
        map.insert(
            "my_agg_level1".to_string(),
            IntermediateAggregationResult::Bucket(IntermediateBucketResult::Range(
                IntermediateRangeBucketResult {
                    buckets,
                    column_type: None,
                },
            )),
        );
        IntermediateAggregationResults {
            aggs_res: map.into_iter().collect(),
        }
    }

    #[test]
    fn test_merge_fruits_tree_1() {
        let mut tree_left = get_intermediate_tree_with_ranges(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);
        let tree_right = get_intermediate_tree_with_ranges(&[
            ("red".to_string(), 60, "1900".to_string(), 30),
            ("blue".to_string(), 25, "1900".to_string(), 50),
        ]);

        tree_left.merge_fruits(tree_right).unwrap();

        let tree_expected = get_intermediate_tree_with_ranges(&[
            ("red".to_string(), 110, "1900".to_string(), 55),
            ("blue".to_string(), 55, "1900".to_string(), 80),
        ]);

        assert_eq!(tree_left, tree_expected);
    }

    #[test]
    fn test_merge_fruits_tree_2() {
        let mut tree_left = get_intermediate_tree_with_ranges(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);
        let tree_right = get_intermediate_tree_with_ranges(&[
            ("red".to_string(), 60, "1900".to_string(), 30),
            ("green".to_string(), 25, "1900".to_string(), 50),
        ]);

        tree_left.merge_fruits(tree_right).unwrap();

        let tree_expected = get_intermediate_tree_with_ranges(&[
            ("red".to_string(), 110, "1900".to_string(), 55),
            ("blue".to_string(), 30, "1900".to_string(), 30),
            ("green".to_string(), 25, "1900".to_string(), 50),
        ]);

        assert_eq!(tree_left, tree_expected);
    }

    #[test]
    fn test_term_bucket_doc_count_no_u32_overflow() {
        // Two segments each contributing (u32::MAX - 100) docs to the same term. Summing them
        // overflowed when doc_count was u32.
        let per_segment = u32::MAX as u64 - 100;
        let mut entry = IntermediateTermBucketEntry {
            doc_count: per_segment,
            sub_aggregation: Default::default(),
        };
        entry
            .merge_fruits(IntermediateTermBucketEntry {
                doc_count: per_segment,
                sub_aggregation: Default::default(),
            })
            .unwrap();
        assert_eq!(entry.doc_count, per_segment * 2);
    }

    #[test]
    fn test_merge_fruits_tree_empty() {
        let mut tree_left = get_intermediate_tree_with_ranges(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);

        let orig = tree_left.clone();

        tree_left
            .merge_fruits(IntermediateAggregationResults::default())
            .unwrap();

        assert_eq!(tree_left, orig);
    }
}
