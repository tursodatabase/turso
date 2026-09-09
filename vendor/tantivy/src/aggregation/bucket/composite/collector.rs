use std::fmt::Debug;
use std::mem;
use std::net::Ipv6Addr;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{
    Column, ColumnType, Dictionary, MonotonicallyMappableToU128, MonotonicallyMappableToU64,
    NumericalValue, StrColumn,
};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::bucket::composite::accessors::{
    CompositeAccessor, CompositeAggReqData, PrecomputedDateInterval,
};
use crate::aggregation::bucket::composite::calendar_interval;
use crate::aggregation::bucket::composite::map::{DynArrayHeapMap, MAX_DYN_ARRAY_SIZE};
use crate::aggregation::bucket::{
    CalendarInterval, CompositeAggregationSource, MissingOrder, Order,
};
use crate::aggregation::cached_sub_aggs::{CachedSubAggs, HighCardSubAggCache};
use crate::aggregation::intermediate_agg_result::{
    CompositeIntermediateKey, IntermediateAggregationResult, IntermediateAggregationResults,
    IntermediateBucketResult, IntermediateCompositeBucketEntry, IntermediateCompositeBucketResult,
};
use crate::aggregation::segment_agg_result::{BucketIdProvider, SegmentAggregationCollector};
use crate::aggregation::BucketId;
use crate::TantivyError;

#[derive(Clone, Debug)]
struct CompositeBucketCollector {
    count: u32,
    bucket_id: BucketId,
}

/// Compact sortable representation of a single source value within a composite key.
///
/// The struct encodes both the column identity and the fast field value in a way
/// that preserves the desired sort order via the derived `Ord` implementation
/// (fields are compared top-to-bottom: `sort_key` first, then `encoded_value`).
///
/// ## `sort_key` encoding
/// - `0` — missing value, sorted first
/// - `1..=254` — present value; the original accessor index is `sort_key - 1`
/// - `u8::MAX` (255) — missing value, sorted last
///
/// ## `encoded_value` encoding
/// - `0` when the field is missing
/// - The raw u64 fast-field representation when order is ascending
/// - Bitwise NOT of the raw u64 when order is descending
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
struct InternalValueRepr {
    /// Column index biased by +1 (so 0 and u8::MAX are reserved for missing sentinels).
    sort_key: u8,
    /// Fast field value, possibly bit-flipped for descending order.
    encoded_value: u64,
}

impl InternalValueRepr {
    #[inline]
    fn new_term(raw: u64, accessor_idx: u8, order: Order) -> Self {
        let encoded_value = match order {
            Order::Asc => raw,
            Order::Desc => !raw,
        };
        InternalValueRepr {
            sort_key: accessor_idx + 1,
            encoded_value,
        }
    }

    /// For histogram sources the column index is irrelevant (always 1).
    #[inline]
    fn new_histogram(raw: u64, order: Order) -> Self {
        let encoded_value = match order {
            Order::Asc => raw,
            Order::Desc => !raw,
        };
        InternalValueRepr {
            sort_key: 1,
            encoded_value,
        }
    }

    #[inline]
    fn new_missing(order: Order, missing_order: MissingOrder) -> Self {
        let sort_key = match (missing_order, order) {
            (MissingOrder::First, _) | (MissingOrder::Default, Order::Asc) => 0,
            (MissingOrder::Last, _) | (MissingOrder::Default, Order::Desc) => u8::MAX,
        };
        InternalValueRepr {
            sort_key,
            encoded_value: 0,
        }
    }

    /// Decode back to `(accessor_idx, raw_value)`.
    /// Returns `None` when the value represents a missing field.
    #[inline]
    fn decode(self, order: Order) -> Option<(u8, u64)> {
        if self.sort_key == 0 || self.sort_key == u8::MAX {
            return None;
        }
        let raw = match order {
            Order::Asc => self.encoded_value,
            Order::Desc => !self.encoded_value,
        };
        Some((self.sort_key - 1, raw))
    }
}

/// The collector puts values from the fast field into the correct buckets and
/// does a conversion to the correct datatype.
#[derive(Debug)]
pub struct SegmentCompositeCollector {
    /// One DynArrayHeapMap per parent bucket.
    parent_buckets: Vec<DynArrayHeapMap<InternalValueRepr, CompositeBucketCollector>>,
    accessor_idx: usize,
    sub_agg: Option<CachedSubAggs<HighCardSubAggCache>>,
    bucket_id_provider: BucketIdProvider,
    /// Number of sources, needed when creating new DynArrayHeapMaps.
    num_sources: usize,
}

impl SegmentAggregationCollector for SegmentCompositeCollector {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        let name = agg_data
            .get_composite_req_data(self.accessor_idx)
            .name
            .clone();

        let buckets = self.add_intermediate_bucket_result(agg_data, parent_bucket_id)?;
        results.push(
            name,
            IntermediateAggregationResult::Bucket(IntermediateBucketResult::Composite { buckets }),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let mem_pre = self.get_memory_consumption(parent_bucket_id);
        let composite_agg_data = agg_data.take_composite_req_data(self.accessor_idx);

        for doc in docs {
            let mut visitor = CompositeKeyVisitor {
                doc_id: *doc,
                composite_agg_data: &composite_agg_data,
                buckets: &mut self.parent_buckets[parent_bucket_id as usize],
                sub_agg: &mut self.sub_agg,
                bucket_id_provider: &mut self.bucket_id_provider,
                sub_level_values: SmallVec::new(),
            };
            visitor.visit(0, true)?;
        }
        agg_data.put_back_composite_req_data(self.accessor_idx, composite_agg_data);

        if let Some(sub_agg) = &mut self.sub_agg {
            sub_agg.check_flush_local(agg_data)?;
        }

        let mem_delta = self.get_memory_consumption(parent_bucket_id) - mem_pre;
        if mem_delta > 0 {
            agg_data.context.limits.add_memory_consumed(mem_delta)?;
        }

        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if let Some(sub_agg) = &mut self.sub_agg {
            sub_agg.flush(agg_data)?;
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let required_len = max_bucket as usize + 1;
        while self.parent_buckets.len() < required_len {
            let map = DynArrayHeapMap::try_new(self.num_sources)?;
            self.parent_buckets.push(map);
        }
        Ok(())
    }
}

impl SegmentCompositeCollector {
    fn get_memory_consumption(&self, parent_bucket_id: BucketId) -> u64 {
        self.parent_buckets[parent_bucket_id as usize].memory_consumption()
    }

    pub(crate) fn from_req_and_validate(
        req_data: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        validate_req(req_data, node.idx_in_req_data)?;

        let has_sub_aggregations = !node.children.is_empty();
        let sub_agg = if has_sub_aggregations {
            let sub_agg_collector = build_segment_agg_collectors(req_data, &node.children)?;
            Some(CachedSubAggs::new(sub_agg_collector))
        } else {
            None
        };

        let composite_req_data = req_data.get_composite_req_data(node.idx_in_req_data);
        let num_sources = composite_req_data.req.sources.len();

        Ok(SegmentCompositeCollector {
            parent_buckets: vec![DynArrayHeapMap::try_new(num_sources)?],
            accessor_idx: node.idx_in_req_data,
            sub_agg,
            bucket_id_provider: BucketIdProvider::default(),
            num_sources,
        })
    }

    #[inline]
    fn add_intermediate_bucket_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        parent_bucket_id: BucketId,
    ) -> crate::Result<IntermediateCompositeBucketResult> {
        let empty_map = DynArrayHeapMap::try_new(self.num_sources)?;
        let heap_map = mem::replace(
            &mut self.parent_buckets[parent_bucket_id as usize],
            empty_map,
        );

        let mut dict: FxHashMap<Vec<CompositeIntermediateKey>, IntermediateCompositeBucketEntry> =
            Default::default();
        dict.reserve(heap_map.size());
        let composite_data = agg_data.get_composite_req_data(self.accessor_idx);
        for (key_internal_repr, agg) in heap_map.into_iter() {
            let key = resolve_key(&key_internal_repr, composite_data)?;
            let mut sub_aggregation_res = IntermediateAggregationResults::default();
            if let Some(sub_agg) = &mut self.sub_agg {
                sub_agg
                    .get_sub_agg_collector()
                    .add_intermediate_aggregation_result(
                        agg_data,
                        &mut sub_aggregation_res,
                        agg.bucket_id,
                    )?;
            }

            dict.insert(
                key,
                IntermediateCompositeBucketEntry {
                    doc_count: agg.count as u64,
                    sub_aggregation: sub_aggregation_res,
                },
            );
        }

        Ok(IntermediateCompositeBucketResult {
            entries: dict,
            target_size: composite_data.req.size,
            orders: composite_data
                .req
                .sources
                .iter()
                .map(|source| match source {
                    CompositeAggregationSource::Terms(t) => (t.order, t.missing_order),
                    CompositeAggregationSource::Histogram(h) => (h.order, h.missing_order),
                    CompositeAggregationSource::DateHistogram(d) => (d.order, d.missing_order),
                })
                .collect(),
        })
    }
}

fn validate_req(req_data: &mut AggregationsSegmentCtx, accessor_idx: usize) -> crate::Result<()> {
    let composite_data = req_data.get_composite_req_data(accessor_idx);
    let req = &composite_data.req;
    if req.sources.is_empty() {
        return Err(TantivyError::InvalidArgument(
            "composite aggregation must have at least one source".to_string(),
        ));
    }
    if req.size == 0 {
        return Err(TantivyError::InvalidArgument(
            "composite aggregation 'size' must be > 0".to_string(),
        ));
    }

    if composite_data.composite_accessors.len() > MAX_DYN_ARRAY_SIZE {
        return Err(TantivyError::InvalidArgument(format!(
            "composite aggregation source supports maximum {MAX_DYN_ARRAY_SIZE} sources",
        )));
    }

    let column_types_for_sources = composite_data.composite_accessors.iter().map(|item| {
        item.accessors
            .iter()
            .map(|a| a.column_type)
            .collect::<Vec<_>>()
    });

    for column_types in column_types_for_sources {
        if column_types.contains(&ColumnType::Bytes) {
            return Err(TantivyError::InvalidArgument(
                "composite aggregation does not support 'bytes' field type".to_string(),
            ));
        }
    }
    Ok(())
}

fn collect_bucket_with_limit(
    doc_id: crate::DocId,
    limit_num_buckets: usize,
    buckets: &mut DynArrayHeapMap<InternalValueRepr, CompositeBucketCollector>,
    key: &[InternalValueRepr],
    sub_agg: &mut Option<CachedSubAggs<HighCardSubAggCache>>,
    bucket_id_provider: &mut BucketIdProvider,
) {
    let mut record_in_bucket = |bucket: &mut CompositeBucketCollector| {
        bucket.count += 1;
        if let Some(sub_agg) = sub_agg {
            sub_agg.push(bucket.bucket_id, doc_id);
        }
    };

    // We still have room for buckets, just insert
    if buckets.size() < limit_num_buckets {
        let bucket = buckets.get_or_insert_with(key, || CompositeBucketCollector {
            count: 0,
            bucket_id: bucket_id_provider.next_bucket_id(),
        });
        record_in_bucket(bucket);
        return;
    }

    // Map is full, but we can still update the bucket if it already exists
    if let Some(bucket) = buckets.get_mut(key) {
        record_in_bucket(bucket);
        return;
    }

    // Check if the item qualifies to enter the top-k, and evict the highest if it does
    if let Some(highest_key) = buckets.peek_highest() {
        if key < highest_key {
            buckets.evict_highest();
            let bucket = buckets.get_or_insert_with(key, || CompositeBucketCollector {
                count: 0,
                bucket_id: bucket_id_provider.next_bucket_id(),
            });
            record_in_bucket(bucket);
        }
    }
}

/// Converts the composite key from its internal column space representation
/// (segment specific) into its intermediate form.
fn resolve_key(
    internal_key: &[InternalValueRepr],
    agg_data: &CompositeAggReqData,
) -> crate::Result<Vec<CompositeIntermediateKey>> {
    internal_key
        .iter()
        .enumerate()
        .map(|(idx, val)| {
            resolve_internal_value_repr(
                *val,
                &agg_data.req.sources[idx],
                &agg_data.composite_accessors[idx].accessors,
            )
        })
        .collect()
}

fn resolve_internal_value_repr(
    internal_value_repr: InternalValueRepr,
    source: &CompositeAggregationSource,
    composite_accessors: &[CompositeAccessor],
) -> crate::Result<CompositeIntermediateKey> {
    let decoded_value_opt = match source {
        CompositeAggregationSource::Terms(source) => internal_value_repr.decode(source.order),
        CompositeAggregationSource::Histogram(source) => internal_value_repr.decode(source.order),
        CompositeAggregationSource::DateHistogram(source) => {
            internal_value_repr.decode(source.order)
        }
    };
    let Some((decoded_accessor_idx, val)) = decoded_value_opt else {
        return Ok(CompositeIntermediateKey::Null);
    };
    let key = match source {
        CompositeAggregationSource::Terms(_) => {
            let CompositeAccessor {
                column_type,
                str_dict_column,
                column,
                ..
            } = &composite_accessors[decoded_accessor_idx as usize];
            resolve_term(val, column_type, str_dict_column, column)?
        }
        CompositeAggregationSource::Histogram(source) => {
            CompositeIntermediateKey::F64(i64::from_u64(val) as f64 * source.interval)
        }
        CompositeAggregationSource::DateHistogram(_) => {
            CompositeIntermediateKey::DateTime(i64::from_u64(val))
        }
    };

    Ok(key)
}

fn resolve_term(
    val: u64,
    column_type: &ColumnType,
    str_dict_column: &Option<StrColumn>,
    column: &Column,
) -> crate::Result<CompositeIntermediateKey> {
    let key = if *column_type == ColumnType::Str {
        let fallback_dict = Dictionary::empty();
        let term_dict = str_dict_column
            .as_ref()
            .map(|el| el.dictionary())
            .unwrap_or_else(|| &fallback_dict);

        let mut buffer = Vec::new();
        term_dict.ord_to_term(val, &mut buffer)?;
        CompositeIntermediateKey::Str(
            String::from_utf8(buffer.to_vec()).expect("could not convert to String"),
        )
    } else if *column_type == ColumnType::DateTime {
        let val = i64::from_u64(val);
        CompositeIntermediateKey::DateTime(val)
    } else if *column_type == ColumnType::Bool {
        let val = bool::from_u64(val);
        CompositeIntermediateKey::Bool(val)
    } else if *column_type == ColumnType::IpAddr {
        let compact_space_accessor = column
            .values
            .clone()
            .downcast_arc::<CompactSpaceU64Accessor>()
            .map_err(|_| {
                TantivyError::AggregationError(crate::aggregation::AggregationError::InternalError(
                    "Type mismatch: Could not downcast to CompactSpaceU64Accessor".to_string(),
                ))
            })?;
        let val: u128 = compact_space_accessor.compact_to_u128(val as u32);
        let val = Ipv6Addr::from_u128(val);
        CompositeIntermediateKey::IpAddr(val)
    } else if *column_type == ColumnType::U64 {
        CompositeIntermediateKey::U64(val)
    } else if *column_type == ColumnType::I64 {
        CompositeIntermediateKey::I64(i64::from_u64(val))
    } else {
        let val = f64::from_u64(val);
        let val: NumericalValue = val.into();

        match val.normalize() {
            NumericalValue::U64(val) => CompositeIntermediateKey::U64(val),
            NumericalValue::I64(val) => CompositeIntermediateKey::I64(val),
            NumericalValue::F64(val) => CompositeIntermediateKey::F64(val),
        }
    };
    Ok(key)
}

/// Browse through the cardinal product obtained by the different values of the doc composite key
/// sources.
///
/// For each of those tuple-key, that are after the limit key, we call collect_bucket_with_limit.
struct CompositeKeyVisitor<'a> {
    doc_id: crate::DocId,
    composite_agg_data: &'a CompositeAggReqData,
    buckets: &'a mut DynArrayHeapMap<InternalValueRepr, CompositeBucketCollector>,
    sub_agg: &'a mut Option<CachedSubAggs<HighCardSubAggCache>>,
    bucket_id_provider: &'a mut BucketIdProvider,
    sub_level_values: SmallVec<[InternalValueRepr; MAX_DYN_ARRAY_SIZE]>,
}

impl CompositeKeyVisitor<'_> {
    /// Depth-first walk of the accessors to build the composite key combinations
    /// and update the buckets.
    ///
    /// `source_idx` is the current source index in the recursion.
    /// `is_on_after_key` tracks whether we still need to consider the after_key
    /// for pruning at this level and below.
    fn visit(&mut self, source_idx: usize, is_on_after_key: bool) -> crate::Result<()> {
        if source_idx == self.composite_agg_data.req.sources.len() {
            if !is_on_after_key {
                collect_bucket_with_limit(
                    self.doc_id,
                    self.composite_agg_data.req.size as usize,
                    self.buckets,
                    &self.sub_level_values,
                    self.sub_agg,
                    self.bucket_id_provider,
                );
            }
            return Ok(());
        }

        let current_level_accessors = &self.composite_agg_data.composite_accessors[source_idx];
        let current_level_source = &self.composite_agg_data.req.sources[source_idx];
        let mut missing = true;
        for (accessor_idx, accessor) in current_level_accessors.accessors.iter().enumerate() {
            let values = accessor.column.values_for_doc(self.doc_id);
            for value in values {
                missing = false;
                match current_level_source {
                    CompositeAggregationSource::Terms(_) => {
                        let preceeds_after_key_type =
                            accessor_idx < current_level_accessors.after_key_accessor_idx;
                        if is_on_after_key && preceeds_after_key_type {
                            break;
                        }
                        let matches_after_key_type =
                            accessor_idx == current_level_accessors.after_key_accessor_idx;

                        if matches_after_key_type && is_on_after_key {
                            let should_skip = match current_level_source.order() {
                                Order::Asc => current_level_accessors.after_key.gt(value),
                                Order::Desc => current_level_accessors.after_key.lt(value),
                            };
                            if should_skip {
                                continue;
                            }
                        }
                        self.sub_level_values.push(InternalValueRepr::new_term(
                            value,
                            accessor_idx as u8,
                            current_level_source.order(),
                        ));
                        let still_on_after_key = matches_after_key_type
                            && current_level_accessors.after_key.equals(value);
                        self.visit(source_idx + 1, is_on_after_key && still_on_after_key)?;
                        self.sub_level_values.pop();
                    }
                    CompositeAggregationSource::Histogram(source) => {
                        let float_value = match accessor.column_type {
                            ColumnType::U64 => value as f64,
                            ColumnType::I64 => i64::from_u64(value) as f64,
                            ColumnType::DateTime => i64::from_u64(value) as f64 / 1_000_000.,
                            ColumnType::F64 => f64::from_u64(value),
                            _ => {
                                panic!(
                                    "unexpected type {:?}. This should not happen",
                                    accessor.column_type
                                )
                            }
                        };
                        let bucket_index = (float_value / source.interval).floor() as i64;
                        let bucket_value = i64::to_u64(bucket_index);
                        if is_on_after_key {
                            let should_skip = match current_level_source.order() {
                                Order::Asc => current_level_accessors.after_key.gt(bucket_value),
                                Order::Desc => current_level_accessors.after_key.lt(bucket_value),
                            };
                            if should_skip {
                                continue;
                            }
                        }
                        self.sub_level_values.push(InternalValueRepr::new_histogram(
                            bucket_value,
                            current_level_source.order(),
                        ));
                        let still_on_after_key =
                            current_level_accessors.after_key.equals(bucket_value);
                        self.visit(source_idx + 1, is_on_after_key && still_on_after_key)?;
                        self.sub_level_values.pop();
                    }
                    CompositeAggregationSource::DateHistogram(_) => {
                        let value_ns = match accessor.column_type {
                            ColumnType::DateTime => i64::from_u64(value),
                            _ => {
                                panic!(
                                    "unexpected type {:?}. This should not happen",
                                    accessor.column_type
                                )
                            }
                        };
                        let bucket_index = match accessor.date_histogram_interval {
                            PrecomputedDateInterval::FixedNanoseconds(fixed_interval_ns) => {
                                (value_ns / fixed_interval_ns) * fixed_interval_ns
                            }
                            PrecomputedDateInterval::Calendar(CalendarInterval::Year) => {
                                calendar_interval::try_year_bucket(value_ns)?
                            }
                            PrecomputedDateInterval::Calendar(CalendarInterval::Month) => {
                                calendar_interval::try_month_bucket(value_ns)?
                            }
                            PrecomputedDateInterval::Calendar(CalendarInterval::Week) => {
                                calendar_interval::week_bucket(value_ns)
                            }
                            PrecomputedDateInterval::NotApplicable => {
                                panic!("interval not precomputed for date histogram source")
                            }
                        };
                        let bucket_value = i64::to_u64(bucket_index);
                        if is_on_after_key {
                            let should_skip = match current_level_source.order() {
                                Order::Asc => current_level_accessors.after_key.gt(bucket_value),
                                Order::Desc => current_level_accessors.after_key.lt(bucket_value),
                            };
                            if should_skip {
                                continue;
                            }
                        }
                        self.sub_level_values.push(InternalValueRepr::new_histogram(
                            bucket_value,
                            current_level_source.order(),
                        ));
                        let still_on_after_key =
                            current_level_accessors.after_key.equals(bucket_value);
                        self.visit(source_idx + 1, is_on_after_key && still_on_after_key)?;
                        self.sub_level_values.pop();
                    }
                };
            }
        }
        if missing && current_level_source.missing_bucket() {
            if is_on_after_key && current_level_accessors.skip_missing {
                return Ok(());
            }
            self.sub_level_values.push(InternalValueRepr::new_missing(
                current_level_source.order(),
                current_level_source.missing_order(),
            ));
            self.visit(
                source_idx + 1,
                is_on_after_key && current_level_accessors.is_after_key_explicit_missing,
            )?;
            self.sub_level_values.pop();
        }
        Ok(())
    }
}
