use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;
use crate::TantivyError;

/// # Percentiles
///
/// The percentiles aggregation is a useful tool for understanding the distribution
/// of a data set. It calculates the values below which a given percentage of the
/// data falls. For instance, the 95th percentile indicates the value below which
/// 95% of the data points can be found.
///
/// This aggregation can be particularly interesting for analyzing website or service response
/// times. For example, if the 95th percentile website load time is significantly higher than the
/// median, this indicates that a small percentage of users are experiencing much slower load times
/// than the majority.
///
/// To use the percentiles aggregation, you'll need to provide a field to
/// aggregate on. In the case of website load times, this would typically be a
/// field containing the duration of time it takes for the site to load.
///
/// The following example demonstrates a request for the percentiles of the "load_time"
/// field:
///
/// ```JSON
/// {
///     "percentiles": {
///         "field": "load_time"
///     }
/// }
/// ```
///
/// This request will return an object containing the default percentiles (1, 5,
/// 25, 50 (median), 75, 95, and 99). You can also customize the percentiles you want to
/// calculate by providing an array of values in the "percents" parameter:
///
/// ```JSON
/// {
///     "percentiles": {
///         "field": "load_time",
///         "percents": [10, 20, 30, 40, 50, 60, 70, 80, 90]
///     }
/// }
/// ```
///
/// In this example, the aggregation will return the 10th, 20th, 30th, 40th, 50th,
/// 60th, 70th, 80th, and 90th percentiles of the "load_time" field.
///
/// Analyzing the percentiles of website load times can help you understand the
/// user experience and identify areas for optimization. For example, if the 95th
/// percentile load time is significantly higher than the median, this indicates
/// that a small percentage of users are experiencing much slower load times than
/// the majority.
///
/// # Estimating Percentiles
///
/// While percentiles provide valuable insights into the distribution of data, it's
/// important to understand that they are often estimates. This is because
/// calculating exact percentiles for large data sets can be computationally
/// expensive and time-consuming. As a result, many percentile aggregation
/// algorithms use approximation techniques to provide faster results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PercentilesAggregationReq {
    /// The field name to compute the percentiles on.
    pub field: String,
    /// The percentiles to compute.
    /// Defaults to [1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0]
    pub percents: Option<Vec<f64>>,
    /// Whether to return the percentiles as a hash map
    #[serde(default = "default_as_true")]
    pub keyed: bool,
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_option_f64"
    )]
    pub missing: Option<f64>,
}
fn default_percentiles() -> &'static [f64] {
    &[1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0]
}
fn default_as_true() -> bool {
    true
}

impl PercentilesAggregationReq {
    /// Creates a new [`PercentilesAggregationReq`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        PercentilesAggregationReq {
            field: field_name,
            percents: None,
            keyed: default_as_true(),
            missing: None,
        }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }

    /// Validates the request parameters.
    pub fn validate(&self) -> crate::Result<()> {
        if let Some(percents) = self.percents.as_ref() {
            let all_in_range = percents
                .iter()
                .cloned()
                .all(|percent| (0.0..=100.0).contains(&percent));
            if !all_in_range {
                return Err(TantivyError::AggregationError(
                    AggregationError::InvalidRequest(
                        "All percentiles have to be between 0.0 and 100.0".to_string(),
                    ),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SegmentPercentilesCollector {
    pub(crate) buckets: Vec<PercentilesCollector>,
    pub(crate) accessor_idx: usize,
    /// The type of the field.
    pub field_type: ColumnType,
    /// The missing value normalized to the internal u64 representation of the field type.
    pub missing_u64: Option<u64>,
    /// The column accessor to access the fast field values.
    pub accessor: Column<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
/// The percentiles collector used during segment collection and for merging results.
pub struct PercentilesCollector {
    sketch: sketches_ddsketch::DDSketch,
}
impl Default for PercentilesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for PercentilesCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("IntermediatePercentiles")
            .field("sketch_len", &self.sketch.length())
            .finish()
    }
}
impl PartialEq for PercentilesCollector {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

fn format_percentile(percentile: f64) -> String {
    let mut out = percentile.to_string();
    // Slightly silly way to format trailing decimals
    if !out.contains('.') {
        out.push_str(".0");
    }
    out
}

impl PercentilesCollector {
    /// Convert result into final result. This will query the quantils from the underlying quantil
    /// collector.
    pub fn into_final_result(self, req: &PercentilesAggregationReq) -> PercentilesMetricResult {
        let percentiles: &[f64] = req
            .percents
            .as_ref()
            .map(|el| el.as_ref())
            .unwrap_or(default_percentiles());
        let iter_quantile_and_values = percentiles.iter().cloned().map(|percentile| {
            (
                percentile,
                self.sketch
                    .quantile(percentile / 100.0)
                    .expect(
                        "quantil out of range. This error should have been caught during \
                         validation phase",
                    )
                    .unwrap_or(f64::NAN),
            )
        });

        let values = if req.keyed {
            PercentileValues::HashMap(
                iter_quantile_and_values
                    .map(|(val, quantil)| (format_percentile(val), quantil))
                    .collect(),
            )
        } else {
            PercentileValues::Vec(
                iter_quantile_and_values
                    .map(|(key, value)| PercentileValuesVecEntry { key, value })
                    .collect(),
            )
        };
        PercentilesMetricResult { values }
    }

    fn new() -> Self {
        let ddsketch_config = sketches_ddsketch::Config::defaults();
        let sketch = sketches_ddsketch::DDSketch::new(ddsketch_config);
        Self { sketch }
    }
    fn collect(&mut self, val: f64) {
        self.sketch.add(val);
    }

    /// Encode the underlying DDSketch to Java-compatible binary format
    /// for cross-language serialization with Java consumers.
    pub fn to_sketch_bytes(&self) -> Vec<u8> {
        self.sketch.to_java_bytes()
    }

    pub(crate) fn merge_fruits(&mut self, right: PercentilesCollector) -> crate::Result<()> {
        self.sketch.merge(&right.sketch).map_err(|err| {
            TantivyError::AggregationError(AggregationError::InternalError(format!(
                "Error while merging percentiles {err:?}"
            )))
        })?;

        Ok(())
    }
}

impl SegmentPercentilesCollector {
    pub fn from_req_and_validate(
        field_type: ColumnType,
        missing_u64: Option<u64>,
        accessor: Column<u64>,
        accessor_idx: usize,
    ) -> Self {
        Self {
            buckets: Vec::with_capacity(64),
            field_type,
            missing_u64,
            accessor,
            accessor_idx,
        }
    }
}

impl SegmentAggregationCollector for SegmentPercentilesCollector {
    #[inline]
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        let name = agg_data.get_metric_req_data(self.accessor_idx).name.clone();
        self.prepare_max_bucket(parent_bucket_id, agg_data)?;
        // Swap collector with an empty one to avoid cloning
        let percentiles_collector = std::mem::take(&mut self.buckets[parent_bucket_id as usize]);

        let intermediate_metric_result =
            IntermediateMetricResult::Percentiles(percentiles_collector);

        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_metric_result),
        )?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let percentiles = &mut self.buckets[parent_bucket_id as usize];
        agg_data.column_block_accessor.fetch_block_with_missing(
            docs,
            &self.accessor,
            self.missing_u64,
        );

        for val in agg_data.column_block_accessor.iter_vals() {
            let val1 = f64_from_fastfield_u64(val, self.field_type);
            percentiles.collect(val1);
        }

        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        while self.buckets.len() <= max_bucket as usize {
            self.buckets.push(PercentilesCollector::new());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use more_asserts::{assert_ge, assert_le};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use serde_json::Value;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::tests::{
        exec_request_with_query, get_test_index_from_values, get_test_index_from_values_and_terms,
    };
    use crate::aggregation::AggregationCollector;
    use crate::query::AllQuery;
    use crate::schema::{Schema, FAST};
    use crate::{assert_nearly_equals, Index};

    #[test]
    fn test_aggregation_percentiles_empty_index() -> crate::Result<()> {
        // test index without segments
        let values = vec![];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "percentiles": {
                "percentiles": {
                    "field": "score",
                }
            },
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["percentiles"]["values"],
            json!({
                "1.0": Value::Null,
                "5.0": Value::Null,
                "25.0": Value::Null,
                "50.0": Value::Null,
                "75.0": Value::Null,
                "95.0": Value::Null,
                "99.0": Value::Null,
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_percentile_simple() -> crate::Result<()> {
        let values = vec![10.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "percentiles": {
                "percentiles": {
                    "field": "score",
                }
            },
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        let percents = vec!["1.0", "5.0", "25.0", "50.0", "75.0", "95.0", "99.0"];
        let range = 9.9..10.1;
        for percent in percents {
            let val = res["percentiles"]["values"][percent].as_f64().unwrap();
            assert!(range.contains(&val));
        }

        Ok(())
    }

    #[test]
    fn test_aggregation_percentile_parameters() -> crate::Result<()> {
        let values = vec![10.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_str = r#"
        {
          "mypercentiles": {
            "percentiles": {
              "field": "score",
              "percents": [ 95, 99, 99.9 ]
            }
          }
        } "#;
        let agg_req_1: Aggregations = serde_json::from_str(agg_req_str).unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        let percents = vec!["95.0", "99.0", "99.9"];
        let expected_range = 9.9..10.1;
        for percent in percents {
            let val = res["mypercentiles"]["values"][percent].as_f64().unwrap();
            assert!(expected_range.contains(&val));
        }
        // Keyed false
        //
        let agg_req_str = r#"
        {
          "mypercentiles": {
            "percentiles": {
              "field": "score",
              "percents": [ 95, 99, 99.9 ],
              "keyed": false
            }
          }
        } "#;
        let agg_req_1: Aggregations = serde_json::from_str(agg_req_str).unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        let vals = &res["mypercentiles"]["values"];
        assert_eq!(vals[0]["key"].as_f64().unwrap(), 95.0);
        assert_eq!(vals[1]["key"].as_f64().unwrap(), 99.0);
        assert_eq!(vals[2]["key"].as_f64().unwrap(), 99.9);
        assert_eq!(vals[3]["key"], serde_json::Value::Null);
        assert!(expected_range.contains(&vals[0]["value"].as_f64().unwrap()));
        assert!(expected_range.contains(&vals[1]["value"].as_f64().unwrap()));
        assert!(expected_range.contains(&vals[2]["value"].as_f64().unwrap()));

        Ok(())
    }

    #[test]
    fn test_aggregation_percentiles_single_seg() -> crate::Result<()> {
        test_aggregation_percentiles(true)
    }

    #[test]
    fn test_aggregation_percentiles_multi_seg() -> crate::Result<()> {
        test_aggregation_percentiles(false)
    }

    fn test_aggregation_percentiles(merge_segments: bool) -> crate::Result<()> {
        use rand_distr::Distribution;
        let num_values_in_segment = [100, 30_000, 8000];
        let lg_norm = rand_distr::LogNormal::new(2.996f64, 0.979f64).unwrap();
        let mut rng = StdRng::from_seed([1u8; 32]);

        let segment_data = |i| {
            (0..num_values_in_segment[i])
                .map(|_| lg_norm.sample(&mut rng))
                .collect_vec()
        };

        let values = (0..=2).map(segment_data).collect_vec();

        let mut all_values = values
            .iter()
            .flat_map(|el| el.iter().cloned())
            .collect_vec();
        all_values.sort_unstable_by(|a, b| a.total_cmp(b));

        fn get_exact_quantil(q: f64, all_values: &[f64]) -> f64 {
            let q = q / 100.0;
            assert!((0f64..=1f64).contains(&q));

            let index = (all_values.len() as f64 * q).ceil() as usize;
            let index = index.min(all_values.len() - 1);
            all_values[index]
        }

        let segment_and_values = values
            .into_iter()
            .map(|segment_data| {
                segment_data
                    .into_iter()
                    .map(|val| (val, val.to_string()))
                    .collect_vec()
            })
            .collect_vec();

        let index =
            get_test_index_from_values_and_terms(merge_segments, &segment_and_values).unwrap();

        let reader = index.reader()?;

        let agg_req_str = r#"
        {
          "mypercentiles": {
            "percentiles": {
              "field": "score_f64",
              "percents": [ 95, 99, 99.9 ]
            }
          }
        } "#;
        let agg_req_1: Aggregations = serde_json::from_str(agg_req_str).unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        let vals = &res["mypercentiles"]["values"];

        let check_quantil = |exact_quantil: f64, val: f64| {
            let lower = exact_quantil - exact_quantil * 0.02;
            let upper = exact_quantil + exact_quantil * 0.02;
            assert_le!(val, upper);
            assert_ge!(val, lower);
        };

        let val = vals["95.0"].as_f64().unwrap();
        let exact_quantil = get_exact_quantil(95.0, &all_values);
        check_quantil(exact_quantil, val);

        let val = vals["99.0"].as_f64().unwrap();
        let exact_quantil = get_exact_quantil(99.0, &all_values);
        check_quantil(exact_quantil, val);

        let val = vals["99.9"].as_f64().unwrap();
        let exact_quantil = get_exact_quantil(99.9, &all_values);
        check_quantil(exact_quantil, val);

        Ok(())
    }

    #[test]
    fn test_percentiles_missing_sub_agg() -> crate::Result<()> {
        // This test verifies the `collect` method (in contrast to `collect_block`), which is
        // called when the sub-aggregations are flushed.
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("texts", FAST);
        let score_field_f64 = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                score_field_f64 => 10.0f64,
                text_field => "a"
            ))?;
            index_writer.add_document(doc!(
                score_field_f64 => 10.0f64,
                text_field => "a"
            ))?;

            index_writer.add_document(doc!(text_field => "a"))?;

            index_writer.commit()?;
        }

        let agg_req: Aggregations = {
            serde_json::from_value(json!({
                "range_with_stats": {
                    "terms": {
                        "field": "texts"
                    },
                    "aggs": {
                        "percentiles": {
                            "percentiles": {
                                "field": "score",
                                "missing": 5.0
                            }
                        }
                    }
                }
            }))
            .unwrap()
        };

        let res = exec_request_with_query(agg_req, &index, None)?;
        assert_eq!(res["range_with_stats"]["buckets"][0]["doc_count"], 3);

        assert_nearly_equals!(
            res["range_with_stats"]["buckets"][0]["percentiles"]["values"]["1.0"]
                .as_f64()
                .unwrap(),
            5.0028295751107414
        );
        assert_nearly_equals!(
            res["range_with_stats"]["buckets"][0]["percentiles"]["values"]["99.0"]
                .as_f64()
                .unwrap(),
            10.07469668951144
        );

        Ok(())
    }

    #[test]
    fn test_percentiles_missing() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("texts", FAST);
        let score_field_f64 = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                score_field_f64 => 10.0f64,
                text_field => "a"
            ))?;
            index_writer.add_document(doc!(
                score_field_f64 => 10.0f64,
                text_field => "a"
            ))?;

            index_writer.add_document(doc!(text_field => "a"))?;

            index_writer.commit()?;
        }

        let agg_req: Aggregations = {
            serde_json::from_value(json!({
                "percentiles": {
                    "percentiles": {
                        "field": "score",
                        "missing": 5.0
                    }
                }
            }))
            .unwrap()
        };

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_nearly_equals!(
            res["percentiles"]["values"]["1.0"].as_f64().unwrap(),
            5.0028295751107414
        );
        assert_nearly_equals!(
            res["percentiles"]["values"]["99.0"].as_f64().unwrap(),
            10.07469668951144
        );

        Ok(())
    }
}
