use std::fmt::Debug;

use super::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::BucketId;
use crate::DocId;

/// A cache for sub-aggregations, storing doc ids per bucket id.
/// Depending on the cardinality of the parent aggregation, we use different
/// storage strategies.
///
/// ## Low Cardinality
/// Cardinality here refers to the number of unique flattened buckets that can be created
/// by the parent aggregation.
/// Flattened buckets are the result of combining all buckets per collector
/// into a single list of buckets, where each bucket is identified by its BucketId.
///
/// ## Usage
/// Since this is caching for sub-aggregations, it is only used by bucket
/// aggregations.
///
/// TODO: consider using a more advanced data structure for high cardinality
/// aggregations.
/// What this datastructure does in general is to group docs by bucket id.
#[derive(Debug)]
pub(crate) struct CachedSubAggs<C: SubAggCache> {
    cache: C,
    sub_agg_collector: Box<dyn SegmentAggregationCollector>,
    num_docs: usize,
}

pub type LowCardCachedSubAggs = CachedSubAggs<LowCardSubAggCache>;
pub type HighCardCachedSubAggs = CachedSubAggs<HighCardSubAggCache>;

const FLUSH_THRESHOLD: usize = 2048;

/// A trait for caching sub-aggregation doc ids per bucket id.
/// Different implementations can be used depending on the cardinality
/// of the parent aggregation.
pub trait SubAggCache: Debug {
    fn new() -> Self;
    fn push(&mut self, bucket_id: BucketId, doc_id: DocId);
    fn flush_local(
        &mut self,
        sub_agg: &mut Box<dyn SegmentAggregationCollector>,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()>;
}

impl<Backend: SubAggCache + Debug> CachedSubAggs<Backend> {
    pub fn new(sub_agg: Box<dyn SegmentAggregationCollector>) -> Self {
        Self {
            cache: Backend::new(),
            sub_agg_collector: sub_agg,
            num_docs: 0,
        }
    }

    pub fn get_sub_agg_collector(&mut self) -> &mut Box<dyn SegmentAggregationCollector> {
        &mut self.sub_agg_collector
    }

    #[inline]
    pub fn push(&mut self, bucket_id: BucketId, doc_id: DocId) {
        self.cache.push(bucket_id, doc_id);
        self.num_docs += 1;
    }

    /// Check if we need to flush based on the number of documents cached.
    /// If so, flushes the cache to the provided aggregation collector.
    pub fn check_flush_local(
        &mut self,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if self.num_docs >= FLUSH_THRESHOLD {
            self.cache
                .flush_local(&mut self.sub_agg_collector, agg_data)?;
            self.num_docs = 0;
        }
        Ok(())
    }

    /// Note: this _does_ flush the sub aggregations.
    pub fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if self.num_docs != 0 {
            self.cache
                .flush_local(&mut self.sub_agg_collector, agg_data)?;
            self.num_docs = 0;
        }
        self.sub_agg_collector.flush(agg_data)?;
        Ok(())
    }
}

/// Number of partitions for high cardinality sub-aggregation cache.
const NUM_PARTITIONS: usize = 16;

#[derive(Debug)]
pub(crate) struct HighCardSubAggCache {
    /// This weird partitioning is used to do some cheap grouping on the bucket ids.
    /// bucket ids are dense, e.g. when we don't detect the cardinality as low cardinality,
    /// but there are just 16 bucket ids, each bucket id will go to its own partition.
    ///
    /// We want to keep this cheap, because high cardinality aggregations can have a lot of
    /// buckets, and there may be nothing to group.
    partitions: Box<[PartitionEntry; NUM_PARTITIONS]>,
}

impl HighCardSubAggCache {
    #[inline]
    fn clear(&mut self) {
        for partition in self.partitions.iter_mut() {
            partition.clear();
        }
    }
}

#[derive(Debug, Clone, Default)]
struct PartitionEntry {
    bucket_ids: Vec<BucketId>,
    docs: Vec<DocId>,
}

impl PartitionEntry {
    #[inline]
    fn clear(&mut self) {
        self.bucket_ids.clear();
        self.docs.clear();
    }
}

impl SubAggCache for HighCardSubAggCache {
    fn new() -> Self {
        Self {
            partitions: Box::new(core::array::from_fn(|_| PartitionEntry::default())),
        }
    }

    fn push(&mut self, bucket_id: BucketId, doc_id: DocId) {
        let idx = bucket_id % NUM_PARTITIONS as u32;
        let slot = &mut self.partitions[idx as usize];
        slot.bucket_ids.push(bucket_id);
        slot.docs.push(doc_id);
    }

    fn flush_local(
        &mut self,
        sub_agg: &mut Box<dyn SegmentAggregationCollector>,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let mut max_bucket = 0u32;
        for partition in self.partitions.iter() {
            if let Some(&local_max) = partition.bucket_ids.iter().max() {
                max_bucket = max_bucket.max(local_max);
            }
        }

        sub_agg.prepare_max_bucket(max_bucket, agg_data)?;

        for slot in self.partitions.iter() {
            if !slot.bucket_ids.is_empty() {
                // Reduce dynamic dispatch overhead by collecting a full partition in one call.
                sub_agg.collect_multiple(&slot.bucket_ids, &slot.docs, agg_data)?;
            }
        }

        self.clear();
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct LowCardSubAggCache {
    /// Cache doc ids per bucket for sub-aggregations.
    ///
    /// The outer Vec is indexed by BucketId.
    per_bucket_docs: Vec<Vec<DocId>>,
}

impl LowCardSubAggCache {
    #[inline]
    fn clear(&mut self) {
        for v in &mut self.per_bucket_docs {
            v.clear();
        }
    }
}

impl SubAggCache for LowCardSubAggCache {
    fn new() -> Self {
        Self {
            per_bucket_docs: Vec::new(),
        }
    }

    fn push(&mut self, bucket_id: BucketId, doc_id: DocId) {
        let idx = bucket_id as usize;
        if self.per_bucket_docs.len() <= idx {
            self.per_bucket_docs.resize_with(idx + 1, Vec::new);
        }
        self.per_bucket_docs[idx].push(doc_id);
    }

    fn flush_local(
        &mut self,
        sub_agg: &mut Box<dyn SegmentAggregationCollector>,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        // Pre-aggregated: call collect per bucket.
        let max_bucket = (self.per_bucket_docs.len() as BucketId).saturating_sub(1);
        sub_agg.prepare_max_bucket(max_bucket, agg_data)?;
        for (bucket_id, docs) in self.per_bucket_docs.iter().enumerate() {
            sub_agg.collect(bucket_id as BucketId, docs, agg_data)?;
        }

        self.clear();
        Ok(())
    }
}
