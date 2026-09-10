use std::fmt::Debug;
use std::ops::Range;

use columnar::{Column, ColumnType};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::agg_limits::AggregationLimitsGuard;
use crate::aggregation::cached_sub_aggs::{
    CachedSubAggs, HighCardSubAggCache, LowCardCachedSubAggs, LowCardSubAggCache, SubAggCache,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateRangeBucketEntry, IntermediateRangeBucketResult,
};
use crate::aggregation::segment_agg_result::{BucketIdProvider, SegmentAggregationCollector};
use crate::aggregation::*;
use crate::TantivyError;

/// Contains all information required by the SegmentRangeCollector to perform the
/// range aggregation on a segment.
pub struct RangeAggReqData {
    /// The column accessor to access the fast field values.
    pub accessor: Column<u64>,
    /// The type of the fast field.
    pub field_type: ColumnType,
    /// The range aggregation request.
    pub req: RangeAggregation,
    /// The name of the aggregation.
    pub name: String,
    /// Whether this is a top-level aggregation.
    pub is_top_level: bool,
}

impl RangeAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Provide user-defined buckets to aggregate on.
///
/// Two special buckets will automatically be created to cover the whole range of values.
/// The provided buckets have to be continuous.
/// During the aggregation, the values extracted from the fast_field `field` will be checked
/// against each bucket range. Note that this aggregation includes the from value and excludes the
/// to value for each range.
///
/// Result type is [`BucketResult`](crate::aggregation::agg_result::BucketResult) with
/// [`RangeBucketEntry`](crate::aggregation::agg_result::RangeBucketEntry) on the
/// `AggregationCollector`.
///
/// Result type is
/// [`IntermediateBucketResult`](crate::aggregation::intermediate_agg_result::IntermediateBucketResult) with
/// [`IntermediateRangeBucketEntry`](crate::aggregation::intermediate_agg_result::IntermediateRangeBucketEntry) on the
/// `DistributedAggregationCollector`.
///
/// # Limitations/Compatibility
/// Overlapping ranges are not yet supported.
///
/// # Request JSON Format
/// ```json
/// {
///     "my_ranges": {
///         "field": "score",
///         "ranges": [
///             { "to": 3.0 },
///             { "from": 3.0, "to": 7.0 },
///             { "from": 7.0, "to": 20.0 },
///             { "from": 20.0 }
///         ]
///     }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RangeAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// Note that this aggregation includes the from value and excludes the to value for each
    /// range. Extra buckets will be created until the first to, and last from, if necessary.
    pub ranges: Vec<RangeAggregationRange>,
    /// Whether to return the buckets as a hash map
    #[serde(default)]
    pub keyed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// The range for one range bucket.
pub struct RangeAggregationRange {
    /// Custom key for the range bucket
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key: Option<String>,
    /// The from range value, which is inclusive in the range.
    /// `None` equals to an open ended interval.
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_option_f64"
    )]
    pub from: Option<f64>,
    /// The to range value, which is not inclusive in the range.
    /// `None` equals to an open ended interval.
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_option_f64"
    )]
    pub to: Option<f64>,
}

impl From<Range<f64>> for RangeAggregationRange {
    fn from(range: Range<f64>) -> Self {
        let from = if range.start == f64::MIN {
            None
        } else {
            Some(range.start)
        };
        let to = if range.end == f64::MAX {
            None
        } else {
            Some(range.end)
        };
        RangeAggregationRange {
            key: None,
            from,
            to,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Internally used u64 range for one range bucket.
pub(crate) struct InternalRangeAggregationRange {
    /// Custom key for the range bucket
    key: Option<String>,
    /// `u64` range value
    range: Range<u64>,
}

impl From<Range<u64>> for InternalRangeAggregationRange {
    fn from(range: Range<u64>) -> Self {
        InternalRangeAggregationRange { key: None, range }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SegmentRangeAndBucketEntry {
    range: Range<u64>,
    bucket: SegmentRangeBucketEntry,
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
pub struct SegmentRangeCollector<C: SubAggCache> {
    /// The buckets containing the aggregation data.
    /// One for each ParentBucketId
    parent_buckets: Vec<Vec<SegmentRangeAndBucketEntry>>,
    column_type: ColumnType,
    pub(crate) accessor_idx: usize,
    sub_agg: Option<CachedSubAggs<C>>,
    /// Here things get a bit weird. We need to assign unique bucket ids across all
    /// parent buckets. So we keep track of the next available bucket id here.
    /// This allows a kind of flattening of the bucket ids across all parent buckets.
    /// E.g. in nested aggregations:
    /// Term Agg -> Range aggregation -> Stats aggregation
    /// E.g. the Term Agg creates 3 buckets ["INFO", "ERROR", "WARN"], each of these has a Range
    /// aggregation with 4 buckets. The Range aggregation will create buckets with ids:
    /// - INFO: 0,1,2,3
    /// - ERROR: 4,5,6,7
    /// - WARN: 8,9,10,11
    ///
    /// This allows the Stats aggregation to have unique bucket ids to refer to.
    bucket_id_provider: BucketIdProvider,
    limits: AggregationLimitsGuard,
}

impl<C: SubAggCache> Debug for SegmentRangeCollector<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentRangeCollector")
            .field("parent_buckets_len", &self.parent_buckets.len())
            .field("column_type", &self.column_type)
            .field("accessor_idx", &self.accessor_idx)
            .field("has_sub_agg", &self.sub_agg.is_some())
            .finish()
    }
}

/// TODO: Bad naming, there's also SegmentRangeAndBucketEntry
#[derive(Clone)]
pub(crate) struct SegmentRangeBucketEntry {
    pub key: Key,
    pub doc_count: u64,
    // pub sub_aggregation: Option<Box<dyn SegmentAggregationCollector>>,
    pub bucket_id: BucketId,
    /// The from range of the bucket. Equals `f64::MIN` when `None`.
    pub from: Option<f64>,
    /// The to range of the bucket. Equals `f64::MAX` when `None`. Open interval, `to` is not
    /// inclusive.
    pub to: Option<f64>,
}

impl Debug for SegmentRangeBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentRangeBucketEntry")
            .field("key", &self.key)
            .field("doc_count", &self.doc_count)
            .field("from", &self.from)
            .field("to", &self.to)
            .finish()
    }
}
impl SegmentRangeBucketEntry {
    pub(crate) fn into_intermediate_bucket_entry(
        self,
    ) -> crate::Result<IntermediateRangeBucketEntry> {
        let sub_aggregation = IntermediateAggregationResults::default();

        Ok(IntermediateRangeBucketEntry {
            key: self.key.into(),
            doc_count: self.doc_count,
            sub_aggregation_res: sub_aggregation,
            from: self.from,
            to: self.to,
        })
    }
}

impl<C: SubAggCache> SegmentAggregationCollector for SegmentRangeCollector<C> {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(parent_bucket_id, agg_data)?;
        let field_type = self.column_type;
        let name = agg_data
            .get_range_req_data(self.accessor_idx)
            .name
            .to_string();

        let buckets = std::mem::take(&mut self.parent_buckets[parent_bucket_id as usize]);

        let buckets: FxHashMap<SerializedKey, IntermediateRangeBucketEntry> = buckets
            .into_iter()
            .map(|range_bucket| {
                let bucket_id = range_bucket.bucket.bucket_id;
                let mut agg = range_bucket.bucket.into_intermediate_bucket_entry()?;
                if let Some(sub_aggregation) = &mut self.sub_agg {
                    sub_aggregation
                        .get_sub_agg_collector()
                        .add_intermediate_aggregation_result(
                            agg_data,
                            &mut agg.sub_aggregation_res,
                            bucket_id,
                        )?;
                }
                Ok((range_to_string(&range_bucket.range, &field_type)?, agg))
            })
            .collect::<crate::Result<_>>()?;

        let bucket = IntermediateBucketResult::Range(IntermediateRangeBucketResult {
            buckets,
            column_type: Some(self.column_type),
        });

        results.push(name, IntermediateAggregationResult::Bucket(bucket))?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let req = agg_data.take_range_req_data(self.accessor_idx);

        agg_data
            .column_block_accessor
            .fetch_block(docs, &req.accessor);

        let buckets = &mut self.parent_buckets[parent_bucket_id as usize];

        for (doc, val) in agg_data
            .column_block_accessor
            .iter_docid_vals(docs, &req.accessor)
        {
            let bucket_pos = get_bucket_pos(val, buckets);
            let bucket = &mut buckets[bucket_pos];
            bucket.bucket.doc_count += 1;
            if let Some(sub_agg) = self.sub_agg.as_mut() {
                sub_agg.push(bucket.bucket.bucket_id, doc);
            }
        }

        agg_data.put_back_range_req_data(self.accessor_idx, req);
        if let Some(sub_agg) = self.sub_agg.as_mut() {
            sub_agg.check_flush_local(agg_data)?;
        }

        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if let Some(sub_agg) = self.sub_agg.as_mut() {
            sub_agg.flush(agg_data)?;
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        while self.parent_buckets.len() <= max_bucket as usize {
            let new_buckets = self.create_new_buckets(agg_data)?;
            self.parent_buckets.push(new_buckets);
        }

        Ok(())
    }
}
/// Build a concrete `SegmentRangeCollector` with either a Vec- or HashMap-backed
/// bucket storage, depending on the column type and aggregation level.
pub(crate) fn build_segment_range_collector(
    agg_data: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let accessor_idx = node.idx_in_req_data;
    let req_data = agg_data.get_range_req_data(node.idx_in_req_data);
    let field_type = req_data.field_type;

    // TODO: A better metric instead of is_top_level would be the number of buckets expected.
    // E.g. If range agg is not top level, but the parent is a bucket agg with less than 10 buckets,
    // we can are still in low cardinality territory.
    let is_low_card = req_data.is_top_level && req_data.req.ranges.len() <= 64;

    let sub_agg = if !node.children.is_empty() {
        Some(build_segment_agg_collectors(agg_data, &node.children)?)
    } else {
        None
    };

    if is_low_card {
        Ok(Box::new(SegmentRangeCollector::<LowCardSubAggCache> {
            sub_agg: sub_agg.map(LowCardCachedSubAggs::new),
            column_type: field_type,
            accessor_idx,
            parent_buckets: Vec::new(),
            bucket_id_provider: BucketIdProvider::default(),
            limits: agg_data.context.limits.clone(),
        }))
    } else {
        Ok(Box::new(SegmentRangeCollector::<HighCardSubAggCache> {
            sub_agg: sub_agg.map(CachedSubAggs::new),
            column_type: field_type,
            accessor_idx,
            parent_buckets: Vec::new(),
            bucket_id_provider: BucketIdProvider::default(),
            limits: agg_data.context.limits.clone(),
        }))
    }
}

impl<C: SubAggCache> SegmentRangeCollector<C> {
    pub(crate) fn create_new_buckets(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<Vec<SegmentRangeAndBucketEntry>> {
        let field_type = self.column_type;
        let req_data = agg_data.get_range_req_data(self.accessor_idx);
        // The range input on the request is f64.
        // We need to convert to u64 ranges, because we read the values as u64.
        // The mapping from the conversion is monotonic so ordering is preserved.
        let buckets: Vec<_> = extend_validate_ranges(&req_data.req.ranges, &field_type)?
            .iter()
            .map(|range| {
                let bucket_id = self.bucket_id_provider.next_bucket_id();
                let key = range
                    .key
                    .clone()
                    .map(|key| Ok(Key::Str(key)))
                    .unwrap_or_else(|| range_to_key(&range.range, &field_type))?;
                let to = if range.range.end == u64::MAX {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.range.end, field_type))
                };
                let from = if range.range.start == u64::MIN {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.range.start, field_type))
                };
                // let sub_aggregation = sub_agg_prototype.clone();

                Ok(SegmentRangeAndBucketEntry {
                    range: range.range.clone(),
                    bucket: SegmentRangeBucketEntry {
                        doc_count: 0,
                        bucket_id,
                        key,
                        from,
                        to,
                    },
                })
            })
            .collect::<crate::Result<_>>()?;

        self.limits.add_memory_consumed(
            buckets.len() as u64 * std::mem::size_of::<SegmentRangeAndBucketEntry>() as u64,
        )?;
        Ok(buckets)
    }
}
#[inline]
fn get_bucket_pos(val: u64, buckets: &[SegmentRangeAndBucketEntry]) -> usize {
    let pos = buckets
        .binary_search_by_key(&val, |probe| probe.range.start)
        .unwrap_or_else(|pos| pos - 1);
    debug_assert!(buckets[pos].range.contains(&val));
    pos
}

/// Converts the user provided f64 range value to fast field value space.
///
/// Internally fast field values are always stored as u64.
/// If the fast field has u64 `[1, 2, 5]`, these values are stored as is in the fast field.
/// A fast field with f64 `[1.0, 2.0, 5.0]` is converted to u64 space, using a
/// monotonic mapping function, so the order is preserved.
///
/// Consequently, a f64 user range 1.0..3.0 needs to be converted to fast field value space using
/// the same monotonic mapping function, so that the provided ranges contain the u64 values in the
/// fast field.
/// The alternative would be that every value read would be converted to the f64 range, but that is
/// more computational expensive when many documents are hit.
fn to_u64_range(
    range: &RangeAggregationRange,
    field_type: &ColumnType,
) -> crate::Result<InternalRangeAggregationRange> {
    let start = if let Some(from) = range.from {
        f64_to_fastfield_u64(from, field_type)
            .ok_or_else(|| TantivyError::InvalidArgument("invalid field type".to_string()))?
    } else {
        u64::MIN
    };

    let end = if let Some(to) = range.to {
        f64_to_fastfield_u64(to, field_type)
            .ok_or_else(|| TantivyError::InvalidArgument("invalid field type".to_string()))?
    } else {
        u64::MAX
    };

    Ok(InternalRangeAggregationRange {
        key: range.key.clone(),
        range: start..end,
    })
}

/// Extends the provided buckets to contain the whole value range, by inserting buckets at the
/// beginning and end and filling gaps.
fn extend_validate_ranges(
    buckets: &[RangeAggregationRange],
    field_type: &ColumnType,
) -> crate::Result<Vec<InternalRangeAggregationRange>> {
    let mut converted_buckets = buckets
        .iter()
        .map(|range| to_u64_range(range, field_type))
        .collect::<crate::Result<Vec<_>>>()?;

    converted_buckets.sort_by_key(|bucket| bucket.range.start);
    if converted_buckets[0].range.start != u64::MIN {
        converted_buckets.insert(0, (u64::MIN..converted_buckets[0].range.start).into());
    }

    if converted_buckets[converted_buckets.len() - 1].range.end != u64::MAX {
        converted_buckets
            .push((converted_buckets[converted_buckets.len() - 1].range.end..u64::MAX).into());
    }

    // fill up holes in the ranges
    let find_hole = |converted_buckets: &[InternalRangeAggregationRange]| {
        for (pos, ranges) in converted_buckets.windows(2).enumerate() {
            if ranges[0].range.end > ranges[1].range.start {
                return Err(TantivyError::InvalidArgument(format!(
                    "Overlapping ranges not supported range {:?}, range+1 {:?}",
                    ranges[0], ranges[1]
                )));
            }
            if ranges[0].range.end != ranges[1].range.start {
                return Ok(Some(pos));
            }
        }
        Ok(None)
    };

    while let Some(hole_pos) = find_hole(&converted_buckets)? {
        let new_range =
            converted_buckets[hole_pos].range.end..converted_buckets[hole_pos + 1].range.start;
        converted_buckets.insert(hole_pos + 1, new_range.into());
    }

    Ok(converted_buckets)
}

pub(crate) fn range_to_string(
    range: &Range<u64>,
    field_type: &ColumnType,
) -> crate::Result<String> {
    // is_start is there for malformed requests, e.g. ig the user passes the range u64::MIN..0.0,
    // it should be rendered as "*-0" and not "*-*"
    let to_str = |val: u64, is_start: bool| {
        if (is_start && val == u64::MIN) || (!is_start && val == u64::MAX) {
            Ok("*".to_string())
        } else if *field_type == ColumnType::DateTime {
            let val = i64::from_u64(val);
            format_date(val)
        } else {
            Ok(f64_from_fastfield_u64(val, *field_type).to_string())
        }
    };

    Ok(format!(
        "{}-{}",
        to_str(range.start, true)?,
        to_str(range.end, false)?
    ))
}

pub(crate) fn range_to_key(range: &Range<u64>, field_type: &ColumnType) -> crate::Result<Key> {
    Ok(Key::Str(range_to_string(range, field_type)?))
}

#[cfg(test)]
mod tests {

    use serde_json::Value;

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query, get_test_index_2_segments,
        get_test_index_with_num_docs,
    };

    pub fn get_collector_from_ranges(
        ranges: Vec<RangeAggregationRange>,
        field_type: ColumnType,
    ) -> SegmentRangeCollector<HighCardSubAggCache> {
        let req = RangeAggregation {
            field: "dummy".to_string(),
            ranges,
            ..Default::default()
        };
        // Build buckets directly as in from_req_and_validate without AggregationsData
        let buckets: Vec<_> = extend_validate_ranges(&req.ranges, &field_type)
            .expect("unexpected error in extend_validate_ranges")
            .iter()
            .map(|range| {
                let key = range
                    .key
                    .clone()
                    .map(|key| Ok(Key::Str(key)))
                    .unwrap_or_else(|| range_to_key(&range.range, &field_type))
                    .expect("unexpected error in range_to_key");
                let to = if range.range.end == u64::MAX {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.range.end, field_type))
                };
                let from = if range.range.start == u64::MIN {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.range.start, field_type))
                };
                SegmentRangeAndBucketEntry {
                    range: range.range.clone(),
                    bucket: SegmentRangeBucketEntry {
                        doc_count: 0,
                        key,
                        from,
                        to,
                        bucket_id: 0,
                    },
                }
            })
            .collect();

        SegmentRangeCollector {
            parent_buckets: vec![buckets],
            column_type: field_type,
            accessor_idx: 0,
            sub_agg: None,
            bucket_id_provider: Default::default(),
            limits: AggregationLimitsGuard::default(),
        }
    }

    #[test]
    fn range_fraction_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ]
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(res["range"]["buckets"][0]["key"], "*-0");
        assert_eq!(res["range"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["range"]["buckets"][1]["key"], "0-0.1");
        assert_eq!(res["range"]["buckets"][1]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][2]["key"], "0.1-0.2");
        assert_eq!(res["range"]["buckets"][2]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][3]["key"], "0.2-*");
        assert_eq!(res["range"]["buckets"][3]["doc_count"], 80);

        Ok(())
    }

    #[test]
    fn range_fraction_test_with_sub_agg() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let sub_agg_req: Aggregations = serde_json::from_value(json!({
            "avg": { "avg": { "field": "score_f64", } }

        }))
        .unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ]
                },
                "aggs": sub_agg_req
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(res["range"]["buckets"][0]["key"], "*-0");
        assert_eq!(res["range"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["range"]["buckets"][1]["key"], "0-0.1");
        assert_eq!(res["range"]["buckets"][1]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][2]["key"], "0.1-0.2");
        assert_eq!(res["range"]["buckets"][2]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][3]["key"], "0.2-*");
        assert_eq!(res["range"]["buckets"][3]["doc_count"], 80);

        Ok(())
    }

    #[test]
    fn range_keyed_buckets_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ],
                    "keyed": true
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res,
            json!({
                "range": {
                    "buckets": {
                        "*-0": { "key": "*-0", "doc_count": 0, "to": 0.0},
                        "0-0.1": {"key": "0-0.1", "doc_count": 10, "from": 0.0, "to": 0.1},
                        "0.1-0.2": {"key": "0.1-0.2", "doc_count": 10, "from": 0.1, "to": 0.2},
                        "0.2-*": {"key": "0.2-*", "doc_count": 80, "from": 0.2},
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    fn range_custom_key_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"key": "custom-key-0-to-0.1", "from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ],
                    "keyed": false
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res,
            json!({
                "range": {
                    "buckets": [
                        {"key": "*-0", "doc_count": 0, "to": 0.0},
                        {"key": "custom-key-0-to-0.1", "doc_count": 10, "from": 0.0, "to": 0.1},
                        {"key": "0.1-0.2", "doc_count": 10, "from": 0.1, "to": 0.2},
                        {"key": "0.2-*", "doc_count": 80, "from": 0.2}
                    ]
                }
            })
        );

        Ok(())
    }

    #[test]
    fn range_date_test_single_segment() -> crate::Result<()> {
        range_date_test_with_opt(true)
    }

    #[test]
    fn range_date_test_multi_segment() -> crate::Result<()> {
        range_date_test_with_opt(false)
    }

    fn range_date_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "date_ranges": {
                "range": {
                    "field": "date",
                    "ranges": [
                        {"to": 1546300800000000000i64},
                        {"from": 1546300800000000000i64, "to": 1546387200000000000i64},
                    ],
                    "keyed": false
                },
            }
        }))
        .unwrap();

        let agg_res = exec_request(agg_req, &index)?;

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(
            res["date_ranges"]["buckets"][0]["from_as_string"],
            Value::Null
        );
        assert_eq!(
            res["date_ranges"]["buckets"][0]["key"],
            "*-2019-01-01T00:00:00Z"
        );
        assert_eq!(
            res["date_ranges"]["buckets"][1]["from_as_string"],
            "2019-01-01T00:00:00Z"
        );
        assert_eq!(
            res["date_ranges"]["buckets"][1]["to_as_string"],
            "2019-01-02T00:00:00Z"
        );

        assert_eq!(
            res["date_ranges"]["buckets"][2]["from_as_string"],
            "2019-01-02T00:00:00Z"
        );
        assert_eq!(
            res["date_ranges"]["buckets"][2]["to_as_string"],
            Value::Null
        );

        Ok(())
    }

    #[test]
    fn range_custom_key_keyed_buckets_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"key": "custom-key-0-to-0.1", "from": 0.0, "to": 0.1},
                    ],
                    "keyed": true
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res,
            json!({
                "range": {
                    "buckets": {
                        "*-0": { "key": "*-0", "doc_count": 0, "to": 0.0},
                        "custom-key-0-to-0.1": {"key": "custom-key-0-to-0.1", "doc_count": 10, "from": 0.0, "to": 0.1},
                        "0.1-*": {"key": "0.1-*", "doc_count": 90, "from": 0.1},
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    fn bucket_test_extend_range_hole() {
        let buckets = vec![(10f64..20f64).into(), (30f64..40f64).into()];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.parent_buckets[0].clone();
        assert_eq!(buckets[0].range.start, u64::MIN);
        assert_eq!(buckets[0].range.end, 10f64.to_u64());
        assert_eq!(buckets[1].range.start, 10f64.to_u64());
        assert_eq!(buckets[1].range.end, 20f64.to_u64());
        // Added bucket to fill hole
        assert_eq!(buckets[2].range.start, 20f64.to_u64());
        assert_eq!(buckets[2].range.end, 30f64.to_u64());
        assert_eq!(buckets[3].range.start, 30f64.to_u64());
        assert_eq!(buckets[3].range.end, 40f64.to_u64());
    }

    #[test]
    fn bucket_test_range_conversion_special_case() {
        // the monotonic conversion between f64 and u64, does not map f64::MIN.to_u64() ==
        // u64::MIN, but the into trait converts f64::MIN/MAX to None
        let buckets = vec![
            (f64::MIN..10f64).into(),
            (10f64..20f64).into(),
            (20f64..f64::MAX).into(),
        ];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.parent_buckets[0].clone();
        assert_eq!(buckets[0].range.start, u64::MIN);
        assert_eq!(buckets[0].range.end, 10f64.to_u64());
        assert_eq!(buckets[1].range.start, 10f64.to_u64());
        assert_eq!(buckets[1].range.end, 20f64.to_u64());
        assert_eq!(buckets[2].range.start, 20f64.to_u64());
        assert_eq!(buckets[2].range.end, u64::MAX);
        assert_eq!(buckets.len(), 3);
    }

    #[test]
    fn bucket_range_test_negative_vals() {
        let buckets = vec![(-10f64..-1f64).into()];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.parent_buckets[0].clone();
        assert_eq!(&buckets[0].bucket.key.to_string(), "*--10");
        assert_eq!(&buckets[buckets.len() - 1].bucket.key.to_string(), "-1-*");
    }
    #[test]
    fn bucket_range_test_positive_vals() {
        let buckets = vec![(0f64..10f64).into()];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.parent_buckets[0].clone();
        assert_eq!(&buckets[0].bucket.key.to_string(), "*-0");
        assert_eq!(&buckets[buckets.len() - 1].bucket.key.to_string(), "10-*");
    }

    #[test]
    fn range_binary_search_test_u64() {
        let check_ranges = |ranges: Vec<RangeAggregationRange>| {
            let collector = get_collector_from_ranges(ranges, ColumnType::U64);
            let search = |val: u64| get_bucket_pos(val, &collector.parent_buckets[0]);

            assert_eq!(search(u64::MIN), 0);
            assert_eq!(search(9), 0);
            assert_eq!(search(10), 1);
            assert_eq!(search(11), 1);
            assert_eq!(search(99), 1);
            assert_eq!(search(100), 2);
            assert_eq!(search(u64::MAX - 1), 2); // Since the end range is never included, the max
                                                 // value
        };

        let ranges = vec![(10.0..100.0).into()];
        check_ranges(ranges);

        let ranges = vec![
            RangeAggregationRange {
                key: None,
                to: Some(10.0),
                from: None,
            },
            (10.0..100.0).into(),
        ];
        check_ranges(ranges);

        let ranges = vec![
            RangeAggregationRange {
                key: None,
                to: Some(10.0),
                from: None,
            },
            (10.0..100.0).into(),
            RangeAggregationRange {
                key: None,
                to: None,
                from: Some(100.0),
            },
        ];
        check_ranges(ranges);
    }

    #[test]
    fn range_binary_search_test_f64() {
        let ranges = vec![(10.0..100.0).into()];

        let collector = get_collector_from_ranges(ranges, ColumnType::F64);
        let search = |val: u64| get_bucket_pos(val, &collector.parent_buckets[0]);

        assert_eq!(search(u64::MIN), 0);
        assert_eq!(search(9f64.to_u64()), 0);
        assert_eq!(search(10f64.to_u64()), 1);
        assert_eq!(search(11f64.to_u64()), 1);
        assert_eq!(search(99f64.to_u64()), 1);
        assert_eq!(search(100f64.to_u64()), 2);
        assert_eq!(search(u64::MAX - 1), 2); // Since the end range is never included,
                                             // the max value
    }
}
