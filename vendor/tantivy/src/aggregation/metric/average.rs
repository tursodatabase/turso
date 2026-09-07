use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::*;

/// A single-value metric aggregation that computes the average of numeric values that are
/// extracted from the aggregated documents.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "avg": {
///         "field": "score"
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AverageAggregation {
    /// The field name to compute the average on.
    pub field: String,
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(default, deserialize_with = "deserialize_option_f64")]
    pub missing: Option<f64>,
}

impl AverageAggregation {
    /// Creates a new [`AverageAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        Self {
            field: field_name,
            missing: None,
        }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Intermediate result of the average aggregation that can be combined with other intermediate
/// results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateAverage {
    stats: IntermediateStats,
}

impl IntermediateAverage {
    /// Creates a new [`IntermediateAverage`] instance from a [`SegmentStatsCollector`].
    pub(crate) fn from_stats(stats: IntermediateStats) -> Self {
        Self { stats }
    }

    /// Returns a reference to the underlying [`IntermediateStats`].
    pub fn stats(&self) -> &IntermediateStats {
        &self.stats
    }

    /// Merges the other intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateAverage) {
        self.stats.merge_fruits(other.stats);
    }
    /// Computes the final average value.
    pub fn finalize(&self) -> Option<f64> {
        self.stats.finalize().avg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialization_with_missing_test1() {
        let json = r#"{
            "field": "score",
            "missing": "10.0"
        }"#;
        let avg: AverageAggregation = serde_json::from_str(json).unwrap();
        assert_eq!(avg.field, "score");
        assert_eq!(avg.missing, Some(10.0));
        // no dot
        let json = r#"{
            "field": "score",
            "missing": "10"
        }"#;
        let avg: AverageAggregation = serde_json::from_str(json).unwrap();
        assert_eq!(avg.field, "score");
        assert_eq!(avg.missing, Some(10.0));

        // from value
        let avg: AverageAggregation = serde_json::from_value(json!({
            "field": "score_f64",
            "missing": 10u64,
        }))
        .unwrap();
        assert_eq!(avg.missing, Some(10.0));
        // from value
        let avg: AverageAggregation = serde_json::from_value(json!({
            "field": "score_f64",
            "missing": 10u32,
        }))
        .unwrap();
        assert_eq!(avg.missing, Some(10.0));
        let avg: AverageAggregation = serde_json::from_value(json!({
            "field": "score_f64",
            "missing": 10i8,
        }))
        .unwrap();
        assert_eq!(avg.missing, Some(10.0));
    }

    #[test]
    fn deserialization_with_missing_test_fail() {
        let json = r#"{
            "field": "score",
            "missing": "a"
        }"#;
        let avg: Result<AverageAggregation, _> = serde_json::from_str(json);
        assert!(avg.is_err());
        assert!(avg
            .unwrap_err()
            .to_string()
            .contains("Failed to parse f64 from string: \"a\""));

        // Disallow NaN
        let json = r#"{
            "field": "score",
            "missing": "NaN"
        }"#;
        let avg: Result<AverageAggregation, _> = serde_json::from_str(json);
        assert!(avg.is_err());
        assert!(avg.unwrap_err().to_string().contains("NaN"));
    }
}
