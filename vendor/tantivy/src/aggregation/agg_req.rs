//! Contains the aggregation request tree. Used to build an
//! [`AggregationCollector`](super::AggregationCollector).
//!
//! [`Aggregations`] is the top level entry point to create a request, which is a `HashMap<String,
//! Aggregation>`.
//!
//! Requests are compatible with the json format of elasticsearch.
//!
//! # Example
//!
//! ```
//! use tantivy::aggregation::agg_req::Aggregations;
//!
//! let elasticsearch_compatible_json_req = r#"
//! {
//!   "range": {
//!     "range": {
//!       "field": "score",
//!       "ranges": [
//!         { "from": 3.0, "to": 7.0 },
//!         { "from": 7.0, "to": 20.0 }
//!       ]
//!     }
//!   }
//! }"#;
//! let _agg_req: Aggregations = serde_json::from_str(elasticsearch_compatible_json_req).unwrap();
//! ```

use std::collections::HashSet;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::bucket::{
    CompositeAggregation, DateHistogramAggregationReq, FilterAggregation, HistogramAggregation,
    RangeAggregation, TermsAggregation,
};
use super::metric::{
    AverageAggregation, CardinalityAggregationReq, CountAggregation, ExtendedStatsAggregation,
    MaxAggregation, MinAggregation, PercentilesAggregationReq, StatsAggregation, SumAggregation,
    TopHitsAggregationReq,
};

/// The top-level aggregation request structure, which contains [`Aggregation`] and their user
/// defined names. It is also used in buckets aggregations to define sub-aggregations.
///
/// The key is the user defined name of the aggregation.
pub type Aggregations = FxHashMap<String, Aggregation>;

/// Aggregation request.
///
/// An aggregation is either a bucket or a metric.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "AggregationForDeserialization")]
pub struct Aggregation {
    /// The aggregation variant, which can be either a bucket or a metric.
    #[serde(flatten)]
    pub agg: AggregationVariants,
    /// on the document set in the bucket.
    #[serde(rename = "aggs")]
    #[serde(skip_serializing_if = "Aggregations::is_empty")]
    pub sub_aggregation: Aggregations,
}

/// In order to display proper error message, we cannot rely on flattening
/// the json enum. Instead we introduce an intermediary struct to separate
/// the aggregation from the subaggregation.
#[derive(Deserialize)]
struct AggregationForDeserialization {
    #[serde(flatten)]
    pub aggs_remaining_json: serde_json::Value,
    #[serde(rename = "aggs")]
    #[serde(default)]
    pub sub_aggregation: Aggregations,
}

impl TryFrom<AggregationForDeserialization> for Aggregation {
    type Error = serde_json::Error;

    fn try_from(value: AggregationForDeserialization) -> serde_json::Result<Self> {
        let AggregationForDeserialization {
            aggs_remaining_json,
            sub_aggregation,
        } = value;
        let agg: AggregationVariants = serde_json::from_value(aggs_remaining_json)?;
        Ok(Aggregation {
            agg,
            sub_aggregation,
        })
    }
}

impl Aggregation {
    pub(crate) fn sub_aggregation(&self) -> &Aggregations {
        &self.sub_aggregation
    }

    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        fast_field_names.extend(
            self.agg
                .get_fast_field_names()
                .iter()
                .map(|s| s.to_string()),
        );
        fast_field_names.extend(get_fast_field_names(&self.sub_aggregation));
    }
}

/// Extract all fast field names used in the tree.
pub fn get_fast_field_names(aggs: &Aggregations) -> HashSet<String> {
    let mut fast_field_names = Default::default();
    for el in aggs.values() {
        el.get_fast_field_names(&mut fast_field_names)
    }
    fast_field_names
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// All aggregation types.
pub enum AggregationVariants {
    // Bucket aggregation types
    /// Put data into buckets of user-defined ranges.
    #[serde(rename = "range")]
    Range(RangeAggregation),
    /// Put data into a histogram.
    #[serde(rename = "histogram")]
    Histogram(HistogramAggregation),
    /// Put data into a date histogram.
    #[serde(rename = "date_histogram")]
    DateHistogram(DateHistogramAggregationReq),
    /// Put data into buckets of terms.
    #[serde(rename = "terms")]
    Terms(TermsAggregation),
    /// Filter documents into a single bucket.
    #[serde(rename = "filter")]
    Filter(FilterAggregation),
    /// Multi-dimensional, paginable bucket aggregation.
    #[serde(rename = "composite")]
    Composite(CompositeAggregation),

    // Metric aggregation types
    /// Computes the average of the extracted values.
    #[serde(rename = "avg")]
    Average(AverageAggregation),
    /// Counts the number of extracted values.
    #[serde(rename = "value_count")]
    Count(CountAggregation),
    /// Finds the maximum value.
    #[serde(rename = "max")]
    Max(MaxAggregation),
    /// Finds the minimum value.
    #[serde(rename = "min")]
    Min(MinAggregation),
    /// Computes a collection of statistics (`min`, `max`, `sum`, `count`, and `avg`) over the
    /// extracted values.
    #[serde(rename = "stats")]
    Stats(StatsAggregation),
    /// Computes a collection of estended statistics (`min`, `max`, `sum`, `count`, `avg`,
    /// `sum_of_squares`, `variance`, `variance_sampling`, `std_deviation`,
    /// `std_deviation_sampling`) over the  extracted values.
    #[serde(rename = "extended_stats")]
    ExtendedStats(ExtendedStatsAggregation),
    /// Computes the sum of the extracted values.
    #[serde(rename = "sum")]
    Sum(SumAggregation),
    /// Computes the sum of the extracted values.
    #[serde(rename = "percentiles")]
    Percentiles(PercentilesAggregationReq),
    /// Finds the top k values matching some order
    #[serde(rename = "top_hits")]
    TopHits(TopHitsAggregationReq),
    /// Computes an estimate of the number of unique values
    #[serde(rename = "cardinality")]
    Cardinality(CardinalityAggregationReq),
}

impl AggregationVariants {
    /// Returns the name of the fields used by the aggregation.
    pub fn get_fast_field_names(&self) -> Vec<&str> {
        match self {
            AggregationVariants::Terms(terms) => vec![terms.field.as_str()],
            AggregationVariants::Range(range) => vec![range.field.as_str()],
            AggregationVariants::Histogram(histogram) => vec![histogram.field.as_str()],
            AggregationVariants::DateHistogram(histogram) => vec![histogram.field.as_str()],
            AggregationVariants::Filter(filter) => filter.get_fast_field_names(),
            AggregationVariants::Composite(composite) => composite
                .sources
                .iter()
                .map(|source| source.field())
                .collect(),
            AggregationVariants::Average(avg) => vec![avg.field_name()],
            AggregationVariants::Count(count) => vec![count.field_name()],
            AggregationVariants::Max(max) => vec![max.field_name()],
            AggregationVariants::Min(min) => vec![min.field_name()],
            AggregationVariants::Stats(stats) => vec![stats.field_name()],
            AggregationVariants::ExtendedStats(extended_stats) => vec![extended_stats.field_name()],
            AggregationVariants::Sum(sum) => vec![sum.field_name()],
            AggregationVariants::Percentiles(per) => vec![per.field_name()],
            AggregationVariants::TopHits(top_hits) => top_hits.field_names(),
            AggregationVariants::Cardinality(per) => vec![per.field_name()],
        }
    }

    pub(crate) fn as_range(&self) -> Option<&RangeAggregation> {
        match &self {
            AggregationVariants::Range(range) => Some(range),
            _ => None,
        }
    }
    pub(crate) fn as_histogram(&self) -> crate::Result<Option<HistogramAggregation>> {
        match &self {
            AggregationVariants::Histogram(histogram) => Ok(Some(histogram.clone())),
            AggregationVariants::DateHistogram(histogram) => {
                Ok(Some(histogram.to_histogram_req()?))
            }
            _ => Ok(None),
        }
    }
    pub(crate) fn as_term(&self) -> Option<&TermsAggregation> {
        match &self {
            AggregationVariants::Terms(terms) => Some(terms),
            _ => None,
        }
    }
    pub(crate) fn as_composite(&self) -> Option<&CompositeAggregation> {
        match &self {
            AggregationVariants::Composite(composite) => Some(composite),
            _ => None,
        }
    }
    pub(crate) fn as_percentile(&self) -> Option<&PercentilesAggregationReq> {
        match &self {
            AggregationVariants::Percentiles(percentile_req) => Some(percentile_req),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn deser_json_test() {
        let agg_req_json = r#"{
            "price_avg": { "avg": { "field": "price" } },
            "price_count": { "value_count": { "field": "price" } },
            "price_max": { "max": { "field": "price" } },
            "price_min": { "min": { "field": "price" } },
            "price_stats": { "stats": { "field": "price" } },
            "price_sum": { "sum": { "field": "price" } }
        }"#;
        let _agg_req: Aggregations = serde_json::from_str(agg_req_json).unwrap();
    }

    #[test]
    fn deser_json_test_bucket() {
        let agg_req_json = r#"
    {
        "termagg": {
            "terms": {
                "field": "json.mixed_type",
                "order": { "min_price": "desc" }
            },
            "aggs": {
                "min_price": { "min": { "field": "json.mixed_type" } }
            }
        },
        "rangeagg": {
            "range": {
                "field": "json.mixed_type",
                "ranges": [
                    { "to": 3.0 },
                    { "from": 19.0, "to": 20.0 },
                    { "from": 20.0 }
                ]
            },
            "aggs": {
                "average_in_range": { "avg": { "field": "json.mixed_type" } }
            }
        }
    } "#;

        let _agg_req: Aggregations = serde_json::from_str(agg_req_json).unwrap();
    }

    #[test]
    fn test_metric_aggregations_deser() {
        let agg_req_json = r#"{
            "price_avg": { "avg": { "field": "price" } },
            "price_count": { "value_count": { "field": "price" } },
            "price_max": { "max": { "field": "price" } },
            "price_min": { "min": { "field": "price" } },
            "price_stats": { "stats": { "field": "price" } },
            "price_sum": { "sum": { "field": "price" } }
        }"#;
        let agg_req: Aggregations = serde_json::from_str(agg_req_json).unwrap();

        assert!(
            matches!(&agg_req.get("price_avg").unwrap().agg, AggregationVariants::Average(avg) if avg.field == "price")
        );
        assert!(
            matches!(&agg_req.get("price_count").unwrap().agg, AggregationVariants::Count(count) if count.field == "price")
        );
        assert!(
            matches!(&agg_req.get("price_max").unwrap().agg, AggregationVariants::Max(max) if max.field == "price")
        );
        assert!(
            matches!(&agg_req.get("price_min").unwrap().agg, AggregationVariants::Min(min) if min.field == "price")
        );
        assert!(
            matches!(&agg_req.get("price_stats").unwrap().agg, AggregationVariants::Stats(stats) if stats.field == "price")
        );
        assert!(
            matches!(&agg_req.get("price_sum").unwrap().agg, AggregationVariants::Sum(sum) if sum.field == "price")
        );
    }

    #[test]
    fn serialize_to_json_test() {
        let elasticsearch_compatible_json_req = r#"{
  "range": {
    "range": {
      "field": "score",
      "ranges": [
        {
          "to": 3.0
        },
        {
          "from": 3.0,
          "to": 7.0
        },
        {
          "from": 7.0,
          "to": 20.0
        },
        {
          "from": 20.0
        }
      ],
      "keyed": true
    }
  }
}"#;

        let agg_req1: Aggregations =
            { serde_json::from_str(elasticsearch_compatible_json_req).unwrap() };

        let agg_req2: String = serde_json::to_string_pretty(&agg_req1).unwrap();
        assert_eq!(agg_req2, elasticsearch_compatible_json_req);
    }

    #[test]
    fn test_get_fast_field_names() {
        let range_agg: Aggregation = {
            serde_json::from_value(json!({
                "range": {
                    "field": "score",
                    "ranges": [
                        { "to": 3.0 },
                        { "from": 3.0, "to": 7.0 },
                        { "from": 7.0, "to": 20.0 },
                        { "from": 20.0 }
                    ],
                }

            }))
            .unwrap()
        };

        let agg_req1: Aggregations = {
            serde_json::from_value(json!({
                "range1": range_agg,
                "range2":{
                    "range": {
                        "field": "score2",
                        "ranges": [
                            { "to": 3.0 },
                            { "from": 3.0, "to": 7.0 },
                            { "from": 7.0, "to": 20.0 },
                            { "from": 20.0 }
                        ],
                    },
                    "aggs": {
                        "metric": {
                            "avg": {
                                "field": "field123"
                            }
                        }
                    }
                }
            }))
            .unwrap()
        };

        assert_eq!(
            get_fast_field_names(&agg_req1),
            vec![
                "score".to_string(),
                "score2".to_string(),
                "field123".to_string()
            ]
            .into_iter()
            .collect()
        )
    }
}
