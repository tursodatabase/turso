use super::agg_req::Aggregations;
use super::agg_result::AggregationResults;
use super::cached_sub_aggs::LowCardCachedSubAggs;
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::AggContextParams;
// group buffering strategy is chosen explicitly by callers; no need to hash-group on the fly.
use crate::aggregation::agg_data::{
    build_aggregations_data_from_req, build_segment_agg_collectors_root, AggregationsSegmentCtx,
};
use crate::collector::{Collector, SegmentCollector};
use crate::index::SegmentReader;
use crate::{DocId, SegmentOrdinal, TantivyError};

/// The default max bucket count, before the aggregation fails.
pub const DEFAULT_BUCKET_LIMIT: u32 = 65000;

/// The default memory limit in bytes before the aggregation fails. 500MB
pub const DEFAULT_MEMORY_LIMIT: u64 = 500_000_000;

/// Collector for aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
pub struct AggregationCollector {
    agg: Aggregations,
    context: AggContextParams,
}

impl AggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// Aggregation fails when the limits in `AggregationLimits` is exceeded. (memory limit and
    /// bucket limit)
    pub fn from_aggs(agg: Aggregations, context: AggContextParams) -> Self {
        Self { agg, context }
    }
}

/// Collector for distributed aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
///
/// # Purpose
/// AggregationCollector returns `IntermediateAggregationResults` and not the final
/// `AggregationResults`, so that results from different indices can be merged and then converted
/// into the final `AggregationResults` via the `into_final_result()` method.
pub struct DistributedAggregationCollector {
    agg: Aggregations,
    context: AggContextParams,
}

impl DistributedAggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// Aggregation fails when the limits in `AggregationLimits` is exceeded. (memory limit and
    /// bucket limit)
    pub fn from_aggs(agg: Aggregations, context: AggContextParams) -> Self {
        Self { agg, context }
    }
}

impl Collector for DistributedAggregationCollector {
    type Fruit = IntermediateAggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        AggregationSegmentCollector::from_agg_req_and_reader(
            &self.agg,
            reader,
            segment_local_id,
            &self.context,
        )
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        merge_fruits(segment_fruits)
    }
}

impl Collector for AggregationCollector {
    type Fruit = AggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        AggregationSegmentCollector::from_agg_req_and_reader(
            &self.agg,
            reader,
            segment_local_id,
            &self.context,
        )
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let res = merge_fruits(segment_fruits)?;
        res.into_final_result(self.agg.clone(), self.context.limits.clone())
    }
}

fn merge_fruits(
    mut segment_fruits: Vec<crate::Result<IntermediateAggregationResults>>,
) -> crate::Result<IntermediateAggregationResults> {
    if let Some(fruit) = segment_fruits.pop() {
        let mut fruit = fruit?;
        for next_fruit in segment_fruits {
            fruit.merge_fruits(next_fruit?)?;
        }
        Ok(fruit)
    } else {
        Ok(IntermediateAggregationResults::default())
    }
}

/// `AggregationSegmentCollector` does the aggregation collection on a segment.
pub struct AggregationSegmentCollector {
    aggs_with_accessor: AggregationsSegmentCtx,
    agg_collector: LowCardCachedSubAggs,
    error: Option<TantivyError>,
}

impl AggregationSegmentCollector {
    /// Creates an `AggregationSegmentCollector from` an [`Aggregations`] request and a segment
    /// reader. Also includes validation, e.g. checking field types and existence.
    pub fn from_agg_req_and_reader(
        agg: &Aggregations,
        reader: &SegmentReader,
        segment_ordinal: SegmentOrdinal,
        context: &AggContextParams,
    ) -> crate::Result<Self> {
        let mut agg_data =
            build_aggregations_data_from_req(agg, reader, segment_ordinal, context.clone())?;
        let mut result =
            LowCardCachedSubAggs::new(build_segment_agg_collectors_root(&mut agg_data)?);
        result
            .get_sub_agg_collector()
            .prepare_max_bucket(0, &agg_data)?; // prepare for bucket zero

        Ok(AggregationSegmentCollector {
            aggs_with_accessor: agg_data,
            agg_collector: result,
            error: None,
        })
    }
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = crate::Result<IntermediateAggregationResults>;

    #[inline]
    fn collect(&mut self, doc: DocId, _score: crate::Score) {
        if self.error.is_some() {
            return;
        }
        self.agg_collector.push(0, doc);
        match self
            .agg_collector
            .check_flush_local(&mut self.aggs_with_accessor)
        {
            Ok(_) => {}
            Err(e) => {
                self.error = Some(e);
            }
        }
    }
    fn collect_block(&mut self, docs: &[DocId]) {
        if self.error.is_some() {
            return;
        }

        match self.agg_collector.get_sub_agg_collector().collect(
            0,
            docs,
            &mut self.aggs_with_accessor,
        ) {
            Ok(_) => {}
            Err(e) => {
                self.error = Some(e);
            }
        }
    }

    fn harvest(mut self) -> Self::Fruit {
        if let Some(err) = self.error {
            return Err(err);
        }
        self.agg_collector.flush(&mut self.aggs_with_accessor)?;

        let mut sub_aggregation_res = IntermediateAggregationResults::default();
        self.agg_collector
            .get_sub_agg_collector()
            .add_intermediate_aggregation_result(
                &self.aggs_with_accessor,
                &mut sub_aggregation_res,
                0,
            )?;

        Ok(sub_aggregation_res)
    }
}
