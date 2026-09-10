use std::fmt::Debug;

use columnar::{Column, ColumnType};
use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;
use crate::TantivyError;

/// A multi-value metric aggregation that computes a collection of statistics on numeric values that
/// are extracted from the aggregated documents.
/// See [`Stats`] for returned statistics.
///
/// # JSON Format
/// ```json
/// {
///     "stats": {
///         "field": "score"
///     }
///  }
/// ```

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatsAggregation {
    /// The field name to compute the stats on.
    pub field: String,
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(default, deserialize_with = "deserialize_option_f64")]
    pub missing: Option<f64>,
}

impl StatsAggregation {
    /// Creates a new [`StatsAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        StatsAggregation {
            field: field_name,
            missing: None,
        }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Stats contains a collection of statistics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    /// The number of documents.
    pub count: u64,
    /// The sum of the fast field values.
    pub sum: f64,
    /// The min value of the fast field values.
    pub min: Option<f64>,
    /// The max value of the fast field values.
    pub max: Option<f64>,
    /// The average of the fast field values. `None` if count equals zero.
    pub avg: Option<f64>,
}

impl Stats {
    pub(crate) fn get_value(&self, agg_property: &str) -> crate::Result<Option<f64>> {
        match agg_property {
            "count" => Ok(Some(self.count as f64)),
            "sum" => Ok(Some(self.sum)),
            "min" => Ok(self.min),
            "max" => Ok(self.max),
            "avg" => Ok(self.avg),
            _ => Err(TantivyError::InvalidArgument(format!(
                "Unknown property {agg_property} on stats metric aggregation"
            ))),
        }
    }
}

/// Intermediate result of the stats aggregation that can be combined with other intermediate
/// results.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateStats {
    /// The number of extracted values.
    pub(crate) count: u64,
    /// The sum of the extracted values.
    pub(crate) sum: f64,
    /// delta for sum needed for [Kahan algorithm for summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm)
    pub(crate) delta: f64,
    /// The min value.
    pub(crate) min: f64,
    /// The max value.
    pub(crate) max: f64,
}

impl Default for IntermediateStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            delta: 0.0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
}

impl IntermediateStats {
    /// Returns the number of values collected.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns the sum of all values collected.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Merges the other stats intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateStats) {
        self.count += other.count;

        // kahan algorithm for sum
        let y = other.sum - (self.delta + other.delta);
        let t = self.sum + y;
        self.delta = (t - self.sum) - y;
        self.sum = t;

        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Computes the final stats value.
    pub fn finalize(&self) -> Stats {
        let min = if self.count == 0 {
            None
        } else {
            Some(self.min)
        };
        let max = if self.count == 0 {
            None
        } else {
            Some(self.max)
        };
        let avg = if self.count == 0 {
            None
        } else {
            Some(self.sum / (self.count as f64))
        };
        Stats {
            count: self.count,
            sum: self.sum,
            min,
            max,
            avg,
        }
    }

    #[inline]
    pub(in crate::aggregation::metric) fn collect(&mut self, value: f64) {
        self.count += 1;

        // kahan algorithm for sum
        let y = value - self.delta;
        let t = self.sum + y;
        self.delta = (t - self.sum) - y;
        self.sum = t;

        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
}

/// The type of stats aggregation to perform.
/// Note that not all stats types are supported in the stats aggregation.
#[derive(Clone, Copy, Debug)]
pub enum StatsType {
    /// The average of the values.
    Average,
    /// The count of the values.
    Count,
    /// The maximum value.
    Max,
    /// The minimum value.
    Min,
    /// The stats (count, sum, min, max, avg) of the values.
    Stats,
    /// The extended stats (count, sum, min, max, avg, sum_of_squares, variance, std_deviation,
    ExtendedStats(Option<f64>), // sigma
    /// The sum of the values.
    Sum,
    /// The percentiles of the values.
    Percentiles,
}

fn create_collector<const TYPE_ID: u8>(
    req: &MetricAggReqData,
) -> Box<dyn SegmentAggregationCollector> {
    Box::new(SegmentStatsCollector::<TYPE_ID> {
        name: req.name.clone(),
        collecting_for: req.collecting_for,
        is_number_or_date_type: req.is_number_or_date_type,
        missing_u64: req.missing_u64,
        accessor: req.accessor.clone(),
        buckets: vec![IntermediateStats::default()],
    })
}

/// Build a concrete `SegmentStatsCollector` depending on the column type.
pub(crate) fn build_segment_stats_collector(
    req: &MetricAggReqData,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    match req.field_type {
        ColumnType::I64 => Ok(create_collector::<{ ColumnType::I64 as u8 }>(req)),
        ColumnType::U64 => Ok(create_collector::<{ ColumnType::U64 as u8 }>(req)),
        ColumnType::F64 => Ok(create_collector::<{ ColumnType::F64 as u8 }>(req)),
        ColumnType::Bool => Ok(create_collector::<{ ColumnType::Bool as u8 }>(req)),
        ColumnType::DateTime => Ok(create_collector::<{ ColumnType::DateTime as u8 }>(req)),
        ColumnType::Bytes => Ok(create_collector::<{ ColumnType::Bytes as u8 }>(req)),
        ColumnType::Str => Ok(create_collector::<{ ColumnType::Str as u8 }>(req)),
        ColumnType::IpAddr => Ok(create_collector::<{ ColumnType::IpAddr as u8 }>(req)),
    }
}

#[repr(C)]
#[derive(Clone, Debug)]
pub(crate) struct SegmentStatsCollector<const COLUMN_TYPE_ID: u8> {
    pub(crate) missing_u64: Option<u64>,
    pub(crate) accessor: Column<u64>,
    pub(crate) is_number_or_date_type: bool,
    pub(crate) buckets: Vec<IntermediateStats>,
    pub(crate) name: String,
    pub(crate) collecting_for: StatsType,
}

impl<const COLUMN_TYPE_ID: u8> SegmentAggregationCollector
    for SegmentStatsCollector<COLUMN_TYPE_ID>
{
    #[inline]
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        let name = self.name.clone();

        self.prepare_max_bucket(parent_bucket_id, agg_data)?;
        let stats = self.buckets[parent_bucket_id as usize];
        let intermediate_metric_result = match self.collecting_for {
            StatsType::Average => {
                IntermediateMetricResult::Average(IntermediateAverage::from_stats(stats))
            }
            StatsType::Count => {
                IntermediateMetricResult::Count(IntermediateCount::from_stats(stats))
            }
            StatsType::Max => IntermediateMetricResult::Max(IntermediateMax::from_stats(stats)),
            StatsType::Min => IntermediateMetricResult::Min(IntermediateMin::from_stats(stats)),
            StatsType::Stats => IntermediateMetricResult::Stats(stats),
            StatsType::Sum => IntermediateMetricResult::Sum(IntermediateSum::from_stats(stats)),
            _ => {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported stats type for stats aggregation: {:?}",
                    self.collecting_for
                )))
            }
        };

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
        // TODO: remove once we fetch all values for all bucket ids in one go
        if docs.len() == 1 && self.missing_u64.is_none() {
            collect_stats::<COLUMN_TYPE_ID>(
                &mut self.buckets[parent_bucket_id as usize],
                self.accessor.values_for_doc(docs[0]),
                self.is_number_or_date_type,
            )?;

            return Ok(());
        }
        agg_data.column_block_accessor.fetch_block_with_missing(
            docs,
            &self.accessor,
            self.missing_u64,
        );
        collect_stats::<COLUMN_TYPE_ID>(
            &mut self.buckets[parent_bucket_id as usize],
            agg_data.column_block_accessor.iter_vals(),
            self.is_number_or_date_type,
        )?;

        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let required_buckets = (max_bucket as usize) + 1;
        if self.buckets.len() < required_buckets {
            self.buckets
                .resize_with(required_buckets, IntermediateStats::default);
        }
        Ok(())
    }
}

#[inline]
fn collect_stats<const COLUMN_TYPE_ID: u8>(
    stats: &mut IntermediateStats,
    vals: impl Iterator<Item = u64>,
    is_number_or_date_type: bool,
) -> crate::Result<()> {
    if is_number_or_date_type {
        for val in vals {
            let val1 = convert_to_f64::<COLUMN_TYPE_ID>(val);
            stats.collect(val1);
        }
    } else {
        for _val in vals {
            // we ignore the value and simply record that we got something
            stats.collect(0.0);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use crate::aggregation::agg_req::{Aggregation, Aggregations};
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::tests::{
        exec_request_with_query, get_test_index_2_segments, get_test_index_from_values,
    };
    use crate::aggregation::AggregationCollector;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, FAST};
    use crate::{Index, IndexWriter, Term};

    #[test]
    fn test_aggregation_stats_empty_index() -> crate::Result<()> {
        // test index without segments
        let values = vec![];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats": {
                "stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": Value::Null,
                "count": 0,
                "max": Value::Null,
                "min": Value::Null,
                "sum": 0.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_stats_simple() -> crate::Result<()> {
        let values = vec![10.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats": {
                "stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": 10.0,
                "count": 1,
                "max": 10.0,
                "min": 10.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_stats() -> crate::Result<()> {
        let index = get_test_index_2_segments(false)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let range_agg: Aggregation = {
            serde_json::from_value(json!({
                "range": {
                    "field": "score",
                    "ranges": [ { "from": 3.0f64, "to": 7.0f64 }, { "from": 7.0f64, "to": 19.0f64 }, { "from": 19.0f64, "to": 20.0f64 }  ]
                },
                "aggs": {
                    "stats": {
                        "stats": {
                            "field": "score"
                        }
                    }
                }
            }))
            .unwrap()
        };

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats_i64": {
                "stats": {
                    "field": "score_i64",
                },
            },
            "stats_f64": {
                "stats": {
                    "field": "score_f64",
                },
            },
            "stats": {
                "stats": {
                    "field": "score",
                },
            },
            "count_str": {
                "value_count": {
                    "field": "text",
                },
            },
            "range": range_agg
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": 12.142857142857142,
                "count": 7,
                "max": 44.0,
                "min": 1.0,
                "sum": 85.0
            })
        );

        assert_eq!(
            res["stats_i64"],
            json!({
                "avg": 12.142857142857142,
                "count": 7,
                "max": 44.0,
                "min": 1.0,
                "sum": 85.0
            })
        );

        assert_eq!(
            res["stats_f64"],
            json!({
                "avg":  12.214285714285714,
                "count": 7,
                "max": 44.5,
                "min": 1.0,
                "sum": 85.5
            })
        );

        assert_eq!(
            res["range"]["buckets"][2]["stats"],
            json!({
                "avg": 10.666666666666666,
                "count": 3,
                "max": 14.0,
                "min": 7.0,
                "sum": 32.0
            })
        );

        assert_eq!(
            res["range"]["buckets"][3]["stats"],
            json!({
                "avg": serde_json::Value::Null,
                "count": 0,
                "max": serde_json::Value::Null,
                "min": serde_json::Value::Null,
                "sum": 0.0,
            })
        );

        assert_eq!(
            res["count_str"],
            json!({
                "value": 7.0,
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with empty json
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        // => Segment with json, but no field partially_empty
        index_writer
            .add_document(doc!(json => json!({"different_field": "blue"})))
            .unwrap();
        index_writer.commit().unwrap();
        //// => Segment with field partially_empty
        index_writer
            .add_document(doc!(json => json!({"partially_empty": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  10.0,
                "count": 1,
                "max": 10.0,
                "min": 10.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json_missing() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with empty json
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        // => Segment with json, but no field partially_empty
        index_writer
            .add_document(doc!(json => json!({"different_field": "blue"})))
            .unwrap();
        index_writer.commit().unwrap();
        //// => Segment with field partially_empty
        index_writer
            .add_document(doc!(json => json!({"partially_empty": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty",
                    "missing": 0.0
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  2.5,
                "count": 4,
                "max": 10.0,
                "min": 0.0,
                "sum": 10.0
            })
        );

        // From string
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty",
                    "missing": "0.0"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  2.5,
                "count": 4,
                "max": 10.0,
                "min": 0.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json_missing_sub_agg() -> crate::Result<()> {
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
                        "my_stats": {
                            "stats": {
                                "field": "score",
                                "missing": 0.0
                            }
                        }
                    }
                }
            }))
            .unwrap()
        };

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["count"],
            2
        );
        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["min"],
            0.0
        );
        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["avg"],
            5.0
        );

        Ok(())
    }
}
