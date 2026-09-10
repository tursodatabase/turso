use columnar::{Column, ColumnBlockAccessor, ColumnType, StrColumn};
use common::BitSet;
use rustc_hash::FxHashSet;
use serde::Serialize;
use tantivy_fst::Regex;

use crate::aggregation::accessor_helpers::{
    get_all_ff_reader_or_empty, get_dynamic_columns, get_ff_reader, get_missing_val_as_u64_lenient,
    get_numeric_or_date_column_types,
};
use crate::aggregation::agg_req::{Aggregation, AggregationVariants, Aggregations};
use crate::aggregation::bucket::{
    build_segment_filter_collector, build_segment_range_collector, CompositeAggReqData,
    CompositeAggregation, CompositeSourceAccessors, FilterAggReqData, HistogramAggReqData,
    HistogramBounds, IncludeExcludeParam, MissingTermAggReqData, RangeAggReqData,
    SegmentHistogramCollector, TermMissingAgg, TermsAggReqData, TermsAggregation,
    TermsAggregationInternal,
};
use crate::aggregation::metric::{
    build_segment_stats_collector, AverageAggregation, CardinalityAggReqData,
    CardinalityAggregationReq, CountAggregation, ExtendedStatsAggregation, MaxAggregation,
    MetricAggReqData, MinAggregation, SegmentCardinalityCollector, SegmentExtendedStatsCollector,
    SegmentPercentilesCollector, StatsAggregation, StatsType, SumAggregation, TopHitsAggReqData,
    TopHitsSegmentCollector,
};
use crate::aggregation::segment_agg_result::{
    GenericSegmentAggregationResultsCollector, SegmentAggregationCollector,
};
use crate::aggregation::{f64_to_fastfield_u64, AggContextParams, Key};
use crate::{SegmentOrdinal, SegmentReader};

#[derive(Default)]
/// Datastructure holding all request data for executing aggregations on a segment.
/// It is passed to the collectors during collection.
pub struct AggregationsSegmentCtx {
    /// Request data for each aggregation type.
    pub per_request: PerRequestAggSegCtx,
    pub context: AggContextParams,
    pub column_block_accessor: ColumnBlockAccessor<u64>,
}

impl AggregationsSegmentCtx {
    pub(crate) fn push_term_req_data(&mut self, data: TermsAggReqData) -> usize {
        self.per_request.term_req_data.push(Some(Box::new(data)));
        self.per_request.term_req_data.len() - 1
    }
    pub(crate) fn push_cardinality_req_data(&mut self, data: CardinalityAggReqData) -> usize {
        self.per_request.cardinality_req_data.push(data);
        self.per_request.cardinality_req_data.len() - 1
    }
    pub(crate) fn push_metric_req_data(&mut self, data: MetricAggReqData) -> usize {
        self.per_request.stats_metric_req_data.push(data);
        self.per_request.stats_metric_req_data.len() - 1
    }
    pub(crate) fn push_top_hits_req_data(&mut self, data: TopHitsAggReqData) -> usize {
        self.per_request.top_hits_req_data.push(data);
        self.per_request.top_hits_req_data.len() - 1
    }
    pub(crate) fn push_missing_term_req_data(&mut self, data: MissingTermAggReqData) -> usize {
        self.per_request.missing_term_req_data.push(data);
        self.per_request.missing_term_req_data.len() - 1
    }
    pub(crate) fn push_histogram_req_data(&mut self, data: HistogramAggReqData) -> usize {
        self.per_request
            .histogram_req_data
            .push(Some(Box::new(data)));
        self.per_request.histogram_req_data.len() - 1
    }
    pub(crate) fn push_range_req_data(&mut self, data: RangeAggReqData) -> usize {
        self.per_request.range_req_data.push(Some(Box::new(data)));
        self.per_request.range_req_data.len() - 1
    }
    pub(crate) fn push_filter_req_data(&mut self, data: FilterAggReqData) -> usize {
        self.per_request.filter_req_data.push(Some(Box::new(data)));
        self.per_request.filter_req_data.len() - 1
    }
    pub(crate) fn push_composite_req_data(&mut self, data: CompositeAggReqData) -> usize {
        self.per_request
            .composite_req_data
            .push(Some(Box::new(data)));
        self.per_request.composite_req_data.len() - 1
    }

    #[inline]
    pub(crate) fn get_term_req_data(&self, idx: usize) -> &TermsAggReqData {
        self.per_request.term_req_data[idx]
            .as_deref()
            .expect("term_req_data slot is empty (taken)")
    }
    #[inline]
    pub(crate) fn get_cardinality_req_data(&self, idx: usize) -> &CardinalityAggReqData {
        &self.per_request.cardinality_req_data[idx]
    }
    #[inline]
    pub(crate) fn get_metric_req_data(&self, idx: usize) -> &MetricAggReqData {
        &self.per_request.stats_metric_req_data[idx]
    }
    #[inline]
    pub(crate) fn get_top_hits_req_data(&self, idx: usize) -> &TopHitsAggReqData {
        &self.per_request.top_hits_req_data[idx]
    }
    #[inline]
    pub(crate) fn get_missing_term_req_data(&self, idx: usize) -> &MissingTermAggReqData {
        &self.per_request.missing_term_req_data[idx]
    }
    #[inline]
    pub(crate) fn get_histogram_req_data(&self, idx: usize) -> &HistogramAggReqData {
        self.per_request.histogram_req_data[idx]
            .as_deref()
            .expect("histogram_req_data slot is empty (taken)")
    }
    #[inline]
    pub(crate) fn get_range_req_data(&self, idx: usize) -> &RangeAggReqData {
        self.per_request.range_req_data[idx]
            .as_deref()
            .expect("range_req_data slot is empty (taken)")
    }
    #[inline]
    pub(crate) fn get_composite_req_data(&self, idx: usize) -> &CompositeAggReqData {
        self.per_request.composite_req_data[idx]
            .as_deref()
            .expect("composite_req_data slot is empty (taken)")
    }

    // ---------- mutable getters ----------

    #[inline]
    pub(crate) fn get_metric_req_data_mut(&mut self, idx: usize) -> &mut MetricAggReqData {
        &mut self.per_request.stats_metric_req_data[idx]
    }

    #[inline]
    pub(crate) fn get_cardinality_req_data_mut(
        &mut self,
        idx: usize,
    ) -> &mut CardinalityAggReqData {
        &mut self.per_request.cardinality_req_data[idx]
    }

    #[inline]
    pub(crate) fn get_histogram_req_data_mut(&mut self, idx: usize) -> &mut HistogramAggReqData {
        self.per_request.histogram_req_data[idx]
            .as_deref_mut()
            .expect("histogram_req_data slot is empty (taken)")
    }

    // ---------- take / put (terms, histogram, range) ----------

    /// Move out the boxed Histogram request at `idx`, leaving `None`.
    #[inline]
    pub(crate) fn take_histogram_req_data(&mut self, idx: usize) -> Box<HistogramAggReqData> {
        self.per_request.histogram_req_data[idx]
            .take()
            .expect("histogram_req_data slot is empty (taken)")
    }

    /// Put back a Histogram request into an empty slot at `idx`.
    #[inline]
    pub(crate) fn put_back_histogram_req_data(
        &mut self,
        idx: usize,
        value: Box<HistogramAggReqData>,
    ) {
        debug_assert!(self.per_request.histogram_req_data[idx].is_none());
        self.per_request.histogram_req_data[idx] = Some(value);
    }

    /// Move out the boxed Range request at `idx`, leaving `None`.
    #[inline]
    pub(crate) fn take_range_req_data(&mut self, idx: usize) -> Box<RangeAggReqData> {
        self.per_request.range_req_data[idx]
            .take()
            .expect("range_req_data slot is empty (taken)")
    }

    /// Put back a Range request into an empty slot at `idx`.
    #[inline]
    pub(crate) fn put_back_range_req_data(&mut self, idx: usize, value: Box<RangeAggReqData>) {
        debug_assert!(self.per_request.range_req_data[idx].is_none());
        self.per_request.range_req_data[idx] = Some(value);
    }

    /// Move out the boxed Filter request at `idx`, leaving `None`.
    #[inline]
    pub(crate) fn take_filter_req_data(&mut self, idx: usize) -> Box<FilterAggReqData> {
        self.per_request.filter_req_data[idx]
            .take()
            .expect("filter_req_data slot is empty (taken)")
    }

    /// Put back a Filter request into an empty slot at `idx`.
    #[inline]
    pub(crate) fn put_back_filter_req_data(&mut self, idx: usize, value: Box<FilterAggReqData>) {
        debug_assert!(self.per_request.filter_req_data[idx].is_none());
        self.per_request.filter_req_data[idx] = Some(value);
    }

    /// Move out the Composite request at `idx`.
    #[inline]
    pub(crate) fn take_composite_req_data(&mut self, idx: usize) -> Box<CompositeAggReqData> {
        self.per_request.composite_req_data[idx]
            .take()
            .expect("composite_req_data slot is empty (taken)")
    }

    /// Put back a Composite request into an empty slot at `idx`.
    #[inline]
    pub(crate) fn put_back_composite_req_data(
        &mut self,
        idx: usize,
        value: Box<CompositeAggReqData>,
    ) {
        debug_assert!(self.per_request.composite_req_data[idx].is_none());
        self.per_request.composite_req_data[idx] = Some(value);
    }
}

/// Each type of aggregation has its own request data struct. This struct holds
/// all request data to execute the aggregation request on a single segment.
///
/// The request tree is represented by `agg_tree`. Tree nodes contain the index
/// of their context in corresponding request data vector (e.g. `term_req_data`
/// for a node with [AggKind::Terms]).
#[derive(Default)]
pub struct PerRequestAggSegCtx {
    // Box for cheap take/put - Only necessary for bucket aggs that have sub-aggregations
    /// TermsAggReqData contains the request data for a terms aggregation.
    pub term_req_data: Vec<Option<Box<TermsAggReqData>>>,
    /// HistogramAggReqData contains the request data for a histogram aggregation.
    pub histogram_req_data: Vec<Option<Box<HistogramAggReqData>>>,
    /// RangeAggReqData contains the request data for a range aggregation.
    pub range_req_data: Vec<Option<Box<RangeAggReqData>>>,
    /// FilterAggReqData contains the request data for a filter aggregation.
    pub filter_req_data: Vec<Option<Box<FilterAggReqData>>>,
    /// Shared by avg, min, max, sum, stats, extended_stats, count
    pub stats_metric_req_data: Vec<MetricAggReqData>,
    /// CardinalityAggReqData contains the request data for a cardinality aggregation.
    pub cardinality_req_data: Vec<CardinalityAggReqData>,
    /// TopHitsAggReqData contains the request data for a top_hits aggregation.
    pub top_hits_req_data: Vec<TopHitsAggReqData>,
    /// MissingTermAggReqData contains the request data for a missing term aggregation.
    pub missing_term_req_data: Vec<MissingTermAggReqData>,
    /// CompositeAggReqData contains the request data for a composite aggregation.
    pub composite_req_data: Vec<Option<Box<CompositeAggReqData>>>,

    /// Request tree used to build collectors.
    pub agg_tree: Vec<AggRefNode>,
}

impl PerRequestAggSegCtx {
    /// Estimate the memory consumption of this struct in bytes.
    fn get_memory_consumption(&self) -> usize {
        self.term_req_data
            .iter()
            .map(|b| b.as_ref().unwrap().get_memory_consumption())
            .sum::<usize>()
            + self
                .histogram_req_data
                .iter()
                .map(|b| b.as_ref().unwrap().get_memory_consumption())
                .sum::<usize>()
            + self
                .range_req_data
                .iter()
                .map(|b| b.as_ref().unwrap().get_memory_consumption())
                .sum::<usize>()
            + self
                .filter_req_data
                .iter()
                .map(|b| b.as_ref().unwrap().get_memory_consumption())
                .sum::<usize>()
            + self
                .stats_metric_req_data
                .iter()
                .map(|t| t.get_memory_consumption())
                .sum::<usize>()
            + self
                .cardinality_req_data
                .iter()
                .map(|t| t.get_memory_consumption())
                .sum::<usize>()
            + self
                .top_hits_req_data
                .iter()
                .map(|t| t.get_memory_consumption())
                .sum::<usize>()
            + self
                .missing_term_req_data
                .iter()
                .map(|t| t.get_memory_consumption())
                .sum::<usize>()
            + self
                .composite_req_data
                .iter()
                .map(|b| b.as_ref().map(|d| d.get_memory_consumption()).unwrap_or(0))
                .sum::<usize>()
            + self.agg_tree.len() * std::mem::size_of::<AggRefNode>()
    }

    pub fn get_name(&self, node: &AggRefNode) -> &str {
        let idx = node.idx_in_req_data;
        let kind = node.kind;
        match kind {
            AggKind::Terms => self.term_req_data[idx]
                .as_deref()
                .expect("term_req_data slot is empty (taken)")
                .name
                .as_str(),
            AggKind::Cardinality => &self.cardinality_req_data[idx].name,
            AggKind::StatsKind(_) => &self.stats_metric_req_data[idx].name,
            AggKind::TopHits => &self.top_hits_req_data[idx].name,
            AggKind::MissingTerm => &self.missing_term_req_data[idx].name,
            AggKind::Histogram => self.histogram_req_data[idx]
                .as_deref()
                .expect("histogram_req_data slot is empty (taken)")
                .name
                .as_str(),
            AggKind::DateHistogram => self.histogram_req_data[idx]
                .as_deref()
                .expect("histogram_req_data slot is empty (taken)")
                .name
                .as_str(),
            AggKind::Range => self.range_req_data[idx]
                .as_deref()
                .expect("range_req_data slot is empty (taken)")
                .name
                .as_str(),
            AggKind::Filter => self.filter_req_data[idx]
                .as_deref()
                .expect("filter_req_data slot is empty (taken)")
                .name
                .as_str(),
            AggKind::Composite => self.composite_req_data[idx]
                .as_deref()
                .expect("composite_req_data slot is empty (taken)")
                .name
                .as_str(),
        }
    }

    /// Convert the aggregation tree into a serializable struct representation.
    /// Each node contains: { name, kind, children }.
    #[allow(dead_code)]
    pub fn get_view_tree(&self) -> Vec<AggTreeViewNode> {
        fn node_to_view(node: &AggRefNode, pr: &PerRequestAggSegCtx) -> AggTreeViewNode {
            let mut children: Vec<AggTreeViewNode> =
                node.children.iter().map(|c| node_to_view(c, pr)).collect();
            children.sort_by_key(|v| serde_json::to_string(v).unwrap());
            AggTreeViewNode {
                name: pr.get_name(node).to_string(),
                kind: node.kind.as_str().to_string(),
                children,
            }
        }

        let mut roots: Vec<AggTreeViewNode> = self
            .agg_tree
            .iter()
            .map(|n| node_to_view(n, self))
            .collect();
        roots.sort_by_key(|v| serde_json::to_string(v).unwrap());
        roots
    }
}

pub(crate) fn build_segment_agg_collectors_root(
    req: &mut AggregationsSegmentCtx,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    build_segment_agg_collectors_generic(req, &req.per_request.agg_tree.clone())
}

pub(crate) fn build_segment_agg_collectors(
    req: &mut AggregationsSegmentCtx,
    nodes: &[AggRefNode],
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    build_segment_agg_collectors_generic(req, nodes)
}

fn build_segment_agg_collectors_generic(
    req: &mut AggregationsSegmentCtx,
    nodes: &[AggRefNode],
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let mut collectors = Vec::new();
    for node in nodes.iter() {
        collectors.push(build_segment_agg_collector(req, node)?);
    }

    req.context
        .limits
        .add_memory_consumed(req.per_request.get_memory_consumption() as u64)?;
    // Single collector special case
    if collectors.len() == 1 {
        return Ok(collectors.pop().unwrap());
    }
    let agg = GenericSegmentAggregationResultsCollector { aggs: collectors };
    Ok(Box::new(agg))
}

pub(crate) fn build_segment_agg_collector(
    req: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    match node.kind {
        AggKind::Terms => crate::aggregation::bucket::build_segment_term_collector(req, node),
        AggKind::MissingTerm => {
            let req_data = &mut req.per_request.missing_term_req_data[node.idx_in_req_data];
            if req_data.accessors.is_empty() {
                return Err(crate::TantivyError::InternalError(
                    "MissingTerm aggregation requires at least one field accessor.".to_string(),
                ));
            }
            Ok(Box::new(TermMissingAgg::new(req, node)?))
        }
        AggKind::Cardinality => {
            let req_data = &mut req.get_cardinality_req_data_mut(node.idx_in_req_data);
            Ok(Box::new(SegmentCardinalityCollector::from_req(
                req_data.column_type,
                node.idx_in_req_data,
                req_data.accessor.clone(),
                req_data.missing_value_for_accessor,
            )))
        }
        AggKind::StatsKind(stats_type) => {
            let req_data = &mut req.per_request.stats_metric_req_data[node.idx_in_req_data];
            match stats_type {
                StatsType::Sum
                | StatsType::Average
                | StatsType::Count
                | StatsType::Max
                | StatsType::Min
                | StatsType::Stats => build_segment_stats_collector(req_data),
                StatsType::ExtendedStats(sigma) => Ok(Box::new(
                    SegmentExtendedStatsCollector::from_req(req_data, sigma),
                )),
                StatsType::Percentiles => {
                    let req_data = req.get_metric_req_data_mut(node.idx_in_req_data);
                    Ok(Box::new(
                        SegmentPercentilesCollector::from_req_and_validate(
                            req_data.field_type,
                            req_data.missing_u64,
                            req_data.accessor.clone(),
                            node.idx_in_req_data,
                        ),
                    ))
                }
            }
        }
        AggKind::TopHits => {
            let req_data = &mut req.per_request.top_hits_req_data[node.idx_in_req_data];
            Ok(Box::new(TopHitsSegmentCollector::from_req(
                &req_data.req,
                node.idx_in_req_data,
                req_data.segment_ordinal,
            )))
        }
        AggKind::Histogram => Ok(Box::new(SegmentHistogramCollector::from_req_and_validate(
            req, node,
        )?)),
        AggKind::DateHistogram => Ok(Box::new(SegmentHistogramCollector::from_req_and_validate(
            req, node,
        )?)),
        AggKind::Range => Ok(build_segment_range_collector(req, node)?),
        AggKind::Filter => build_segment_filter_collector(req, node),
        AggKind::Composite => Ok(Box::new(
            crate::aggregation::bucket::SegmentCompositeCollector::from_req_and_validate(
                req, node,
            )?,
        )),
    }
}

/// See [PerRequestAggSegCtx]
#[derive(Debug, Clone)]
pub struct AggRefNode {
    pub kind: AggKind,
    pub idx_in_req_data: usize,
    pub children: Vec<AggRefNode>,
}
impl AggRefNode {
    pub fn get_sub_agg(&self, name: &str, pr: &PerRequestAggSegCtx) -> Option<&AggRefNode> {
        self.children
            .iter()
            .find(|&child| pr.get_name(child) == name)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum AggKind {
    Terms,
    Cardinality,
    /// One of: Statistics, Average, Min, Max, Sum, Count, Stats, ExtendedStats
    StatsKind(StatsType),
    TopHits,
    MissingTerm,
    Histogram,
    DateHistogram,
    Range,
    Filter,
    Composite,
}

impl AggKind {
    #[cfg_attr(not(test), allow(dead_code))]
    fn as_str(&self) -> &'static str {
        match self {
            AggKind::Terms => "Terms",
            AggKind::Cardinality => "Cardinality",
            AggKind::StatsKind(_) => "Metric",
            AggKind::TopHits => "TopHits",
            AggKind::MissingTerm => "MissingTerm",
            AggKind::Histogram => "Histogram",
            AggKind::DateHistogram => "DateHistogram",
            AggKind::Range => "Range",
            AggKind::Filter => "Filter",
            AggKind::Composite => "Composite",
        }
    }
}

/// Build AggregationsData by walking the request tree.
pub(crate) fn build_aggregations_data_from_req(
    aggs: &Aggregations,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    context: AggContextParams,
) -> crate::Result<AggregationsSegmentCtx> {
    let mut data = AggregationsSegmentCtx {
        per_request: Default::default(),
        context,
        column_block_accessor: ColumnBlockAccessor::default(),
    };

    for (name, agg) in aggs.iter() {
        let nodes = build_nodes(name, agg, reader, segment_ordinal, &mut data, true)?;
        data.per_request.agg_tree.extend(nodes);
    }
    Ok(data)
}

fn build_nodes(
    agg_name: &str,
    req: &Aggregation,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    data: &mut AggregationsSegmentCtx,
    is_top_level: bool,
) -> crate::Result<Vec<AggRefNode>> {
    use AggregationVariants::*;
    match &req.agg {
        Range(range_req) => {
            let (accessor, field_type) = get_ff_reader(
                reader,
                &range_req.field,
                Some(get_numeric_or_date_column_types()),
            )?;
            let idx_in_req_data = data.push_range_req_data(RangeAggReqData {
                accessor,
                field_type,
                name: agg_name.to_string(),
                req: range_req.clone(),
                is_top_level,
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, data)?;
            Ok(vec![AggRefNode {
                kind: AggKind::Range,
                idx_in_req_data,
                children,
            }])
        }
        Histogram(histo_req) => {
            let (accessor, field_type) = get_ff_reader(
                reader,
                &histo_req.field,
                Some(get_numeric_or_date_column_types()),
            )?;
            let idx_in_req_data = data.push_histogram_req_data(HistogramAggReqData {
                accessor,
                field_type,
                name: agg_name.to_string(),
                req: histo_req.clone(),
                is_date_histogram: false,
                bounds: HistogramBounds {
                    min: f64::MIN,
                    max: f64::MAX,
                },
                offset: 0.0,
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, data)?;
            Ok(vec![AggRefNode {
                kind: AggKind::Histogram,
                idx_in_req_data,
                children,
            }])
        }
        DateHistogram(date_req) => {
            let (accessor, field_type) =
                get_ff_reader(reader, &date_req.field, Some(&[ColumnType::DateTime]))?;
            // Convert to histogram request, normalize to ns precision
            let mut histo_req = date_req.to_histogram_req()?;
            histo_req.normalize_date_time();
            let idx_in_req_data = data.push_histogram_req_data(HistogramAggReqData {
                accessor,
                field_type,
                name: agg_name.to_string(),
                req: histo_req,
                is_date_histogram: true,
                bounds: HistogramBounds {
                    min: f64::MIN,
                    max: f64::MAX,
                },
                offset: 0.0,
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, data)?;
            Ok(vec![AggRefNode {
                kind: AggKind::DateHistogram,
                idx_in_req_data,
                children,
            }])
        }
        Terms(terms_req) => build_terms_or_cardinality_nodes(
            agg_name,
            &terms_req.field,
            &terms_req.missing,
            reader,
            segment_ordinal,
            data,
            &req.sub_aggregation,
            TermsOrCardinalityRequest::Terms(terms_req.clone()),
            is_top_level,
        ),
        Cardinality(card_req) => build_terms_or_cardinality_nodes(
            agg_name,
            &card_req.field,
            &card_req.missing,
            reader,
            segment_ordinal,
            data,
            &req.sub_aggregation,
            TermsOrCardinalityRequest::Cardinality(card_req.clone()),
            is_top_level,
        ),
        Average(AverageAggregation { field, missing, .. })
        | Max(MaxAggregation { field, missing, .. })
        | Min(MinAggregation { field, missing, .. })
        | Stats(StatsAggregation { field, missing, .. })
        | ExtendedStats(ExtendedStatsAggregation { field, missing, .. })
        | Sum(SumAggregation { field, missing, .. })
        | Count(CountAggregation { field, missing, .. }) => {
            let allowed_column_types = if matches!(&req.agg, Count(_)) {
                Some(
                    &[
                        ColumnType::I64,
                        ColumnType::U64,
                        ColumnType::F64,
                        ColumnType::Str,
                        ColumnType::DateTime,
                        ColumnType::Bool,
                        ColumnType::IpAddr,
                    ][..],
                )
            } else {
                Some(get_numeric_or_date_column_types())
            };
            let collecting_for = match &req.agg {
                Average(_) => StatsType::Average,
                Max(_) => StatsType::Max,
                Min(_) => StatsType::Min,
                Stats(_) => StatsType::Stats,
                ExtendedStats(req) => StatsType::ExtendedStats(req.sigma),
                Sum(_) => StatsType::Sum,
                Count(_) => StatsType::Count,
                _ => {
                    return Err(crate::TantivyError::InvalidArgument(
                        "Internal error: unexpected aggregation type in metric aggregation \
                         handling."
                            .to_string(),
                    ))
                }
            };
            let (accessor, field_type) = get_ff_reader(reader, field, allowed_column_types)?;
            let idx_in_req_data = data.push_metric_req_data(MetricAggReqData {
                accessor,
                field_type,
                name: agg_name.to_string(),
                collecting_for,
                missing: *missing,
                missing_u64: (*missing).and_then(|m| f64_to_fastfield_u64(m, &field_type)),
                is_number_or_date_type: matches!(
                    field_type,
                    ColumnType::I64 | ColumnType::U64 | ColumnType::F64 | ColumnType::DateTime
                ),
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, data)?;
            Ok(vec![AggRefNode {
                kind: AggKind::StatsKind(collecting_for),
                idx_in_req_data,
                children,
            }])
        }
        // Percentiles handled as Metric as well
        AggregationVariants::Percentiles(percentiles_req) => {
            percentiles_req.validate()?;
            let (accessor, field_type) = get_ff_reader(
                reader,
                percentiles_req.field_name(),
                Some(get_numeric_or_date_column_types()),
            )?;
            let idx_in_req_data = data.push_metric_req_data(MetricAggReqData {
                accessor,
                field_type,
                name: agg_name.to_string(),
                collecting_for: StatsType::Percentiles,
                missing: percentiles_req.missing,
                missing_u64: percentiles_req
                    .missing
                    .and_then(|m| f64_to_fastfield_u64(m, &field_type)),
                is_number_or_date_type: matches!(
                    field_type,
                    ColumnType::I64 | ColumnType::U64 | ColumnType::F64 | ColumnType::DateTime
                ),
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, data)?;
            Ok(vec![AggRefNode {
                kind: AggKind::StatsKind(StatsType::Percentiles),
                idx_in_req_data,
                children,
            }])
        }
        AggregationVariants::TopHits(top_hits_req) => {
            let mut top_hits = top_hits_req.clone();
            top_hits.validate_and_resolve_field_names(reader.fast_fields().columnar())?;
            let accessors: Vec<(Column<u64>, ColumnType)> = top_hits
                .field_names()
                .iter()
                .map(|field| get_ff_reader(reader, field, Some(get_numeric_or_date_column_types())))
                .collect::<crate::Result<_>>()?;

            let value_accessors = top_hits
                .value_field_names()
                .iter()
                .map(|field_name| {
                    Ok((
                        field_name.to_string(),
                        get_dynamic_columns(reader, field_name)?,
                    ))
                })
                .collect::<crate::Result<_>>()?;

            let idx_in_req_data = data.push_top_hits_req_data(TopHitsAggReqData {
                accessors,
                value_accessors,
                segment_ordinal,
                name: agg_name.to_string(),
                req: top_hits.clone(),
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, data)?;
            Ok(vec![AggRefNode {
                kind: AggKind::TopHits,
                idx_in_req_data,
                children,
            }])
        }
        AggregationVariants::Composite(composite_req) => Ok(vec![build_composite_node(
            agg_name,
            reader,
            segment_ordinal,
            data,
            &req.sub_aggregation,
            composite_req,
        )?]),
        AggregationVariants::Filter(filter_req) => {
            // Build the query and evaluator upfront
            let schema = reader.schema();
            let tokenizers = &data.context.tokenizers;
            let query = filter_req.parse_query(schema, tokenizers)?;
            let evaluator = crate::aggregation::bucket::DocumentQueryEvaluator::new(
                query,
                schema.clone(),
                reader,
            )?;

            // Pre-allocate buffer for batch filtering
            let max_doc = reader.max_doc();
            let buffer_capacity = crate::docset::COLLECT_BLOCK_BUFFER_LEN.min(max_doc as usize);
            let matching_docs_buffer = Vec::with_capacity(buffer_capacity);

            let idx_in_req_data = data.push_filter_req_data(FilterAggReqData {
                name: agg_name.to_string(),
                req: filter_req.clone(),
                segment_reader: reader.clone(),
                evaluator,
                matching_docs_buffer,
                is_top_level,
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, data)?;
            Ok(vec![AggRefNode {
                kind: AggKind::Filter,
                idx_in_req_data,
                children,
            }])
        }
    }
}

fn build_composite_node(
    agg_name: &str,
    reader: &SegmentReader,
    _segment_ordinal: SegmentOrdinal,
    data: &mut AggregationsSegmentCtx,
    sub_aggs: &Aggregations,
    req: &CompositeAggregation,
) -> crate::Result<AggRefNode> {
    let mut composite_accessors = Vec::with_capacity(req.sources.len());
    for source in &req.sources {
        let source_after_key_opt = req.after.get(source.name()).map(|k| &k.0);
        let source_accessor =
            CompositeSourceAccessors::build_for_source(reader, source, source_after_key_opt)?;
        composite_accessors.push(source_accessor);
    }
    let agg = CompositeAggReqData {
        name: agg_name.to_string(),
        req: req.clone(),
        composite_accessors,
    };
    let idx = data.push_composite_req_data(agg);
    let children = build_children(sub_aggs, reader, _segment_ordinal, data)?;
    Ok(AggRefNode {
        kind: AggKind::Composite,
        idx_in_req_data: idx,
        children,
    })
}

fn build_children(
    aggs: &Aggregations,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    data: &mut AggregationsSegmentCtx,
) -> crate::Result<Vec<AggRefNode>> {
    let mut children = Vec::new();
    for (name, agg) in aggs.iter() {
        children.extend(build_nodes(
            name,
            agg,
            reader,
            segment_ordinal,
            data,
            false,
        )?);
    }
    Ok(children)
}

fn get_term_agg_accessors(
    reader: &SegmentReader,
    field_name: &str,
    missing: &Option<Key>,
) -> crate::Result<Vec<(Column<u64>, ColumnType)>> {
    let allowed_column_types = [
        ColumnType::I64,
        ColumnType::U64,
        ColumnType::F64,
        ColumnType::Str,
        ColumnType::DateTime,
        ColumnType::Bool,
        ColumnType::IpAddr,
    ];

    // In case the column is empty we want the shim column to match the missing type
    let fallback_type = missing
        .as_ref()
        .map(|missing| match missing {
            Key::Str(_) => ColumnType::Str,
            Key::F64(_) => ColumnType::F64,
            Key::I64(_) => ColumnType::I64,
            Key::U64(_) => ColumnType::U64,
        })
        .unwrap_or(ColumnType::U64);

    let column_and_types = get_all_ff_reader_or_empty(
        reader,
        field_name,
        Some(&allowed_column_types),
        fallback_type,
    )?;

    Ok(column_and_types)
}

enum TermsOrCardinalityRequest {
    Terms(TermsAggregation),
    Cardinality(CardinalityAggregationReq),
}
impl TermsOrCardinalityRequest {
    fn as_terms(&self) -> Option<&TermsAggregation> {
        match self {
            TermsOrCardinalityRequest::Terms(t) => Some(t),
            _ => None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_terms_or_cardinality_nodes(
    agg_name: &str,
    field_name: &str,
    missing: &Option<Key>,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    data: &mut AggregationsSegmentCtx,
    sub_aggs: &Aggregations,
    req: TermsOrCardinalityRequest,
    is_top_level: bool,
) -> crate::Result<Vec<AggRefNode>> {
    let mut nodes = Vec::new();

    let str_dict_column = reader.fast_fields().str(field_name)?;

    let column_and_types = get_term_agg_accessors(reader, field_name, missing)?;

    // Special handling when missing + multi column or incompatible type on text/date.
    let missing_and_more_than_one_col = column_and_types.len() > 1 && missing.is_some();
    let text_on_non_text_col = column_and_types.len() == 1
        && column_and_types[0].1 != ColumnType::Str
        && matches!(missing, Some(Key::Str(_)));

    let use_special_missing_agg = missing_and_more_than_one_col || text_on_non_text_col;

    // If special missing handling is required, build a MissingTerm node that carries all
    // accessors (across any column types) for existence checks.
    if use_special_missing_agg {
        let fallback_type = missing
            .as_ref()
            .map(|missing| match missing {
                Key::Str(_) => ColumnType::Str,
                Key::F64(_) => ColumnType::F64,
                Key::I64(_) => ColumnType::I64,
                Key::U64(_) => ColumnType::U64,
            })
            .unwrap_or(ColumnType::U64);
        let all_accessors = get_all_ff_reader_or_empty(reader, field_name, None, fallback_type)?
            .into_iter()
            .collect::<Vec<_>>();
        // This case only happens when we have term aggregation, or we fail
        let req = req.as_terms().cloned().ok_or_else(|| {
            crate::TantivyError::InvalidArgument(
                "Cardinality aggregation with missing on non-text/number field is not supported."
                    .to_string(),
            )
        })?;

        let children = build_children(sub_aggs, reader, segment_ordinal, data)?;
        let idx_in_req_data = data.push_missing_term_req_data(MissingTermAggReqData {
            accessors: all_accessors,
            name: agg_name.to_string(),
            req,
        });
        nodes.push(AggRefNode {
            kind: AggKind::MissingTerm,
            idx_in_req_data,
            children,
        });
    }

    // Add one node per accessor
    for (accessor, column_type) in column_and_types {
        let missing_value_for_accessor = if use_special_missing_agg {
            None
        } else if let Some(m) = missing.as_ref() {
            get_missing_val_as_u64_lenient(column_type, accessor.max_value(), m, field_name)?
        } else {
            None
        };

        let children = build_children(sub_aggs, reader, segment_ordinal, data)?;
        let (idx, kind) = match req {
            TermsOrCardinalityRequest::Terms(ref req) => {
                let mut allowed_term_ids = None;
                if req.include.is_some() || req.exclude.is_some() {
                    if column_type != ColumnType::Str {
                        // Skip non-string columns entirely when filtering is requested.
                        // When excluding, the behavior could be to include non-string values
                        continue;
                    }
                    let str_col = str_dict_column
                        .as_ref()
                        .expect("str_dict_column must exist for string column");
                    allowed_term_ids =
                        build_allowed_term_ids_for_str(str_col, &req.include, &req.exclude)?;
                };
                let idx_in_req_data = data.push_term_req_data(TermsAggReqData {
                    accessor,
                    column_type,
                    str_dict_column: str_dict_column.clone(),
                    missing_value_for_accessor,
                    name: agg_name.to_string(),
                    req: TermsAggregationInternal::from_req(req),
                    sug_aggregations: sub_aggs.clone(),
                    allowed_term_ids,
                    is_top_level,
                });
                (idx_in_req_data, AggKind::Terms)
            }
            TermsOrCardinalityRequest::Cardinality(ref req) => {
                let idx_in_req_data = data.push_cardinality_req_data(CardinalityAggReqData {
                    accessor,
                    column_type,
                    str_dict_column: str_dict_column.clone(),
                    missing_value_for_accessor,
                    name: agg_name.to_string(),
                    req: req.clone(),
                });
                (idx_in_req_data, AggKind::Cardinality)
            }
        };
        nodes.push(AggRefNode {
            kind,
            idx_in_req_data: idx,
            children,
        });
    }

    Ok(nodes)
}

/// Builds a single BitSet of allowed term ordinals for a string dictionary column according to
/// include/exclude parameters.
fn build_allowed_term_ids_for_str(
    str_col: &StrColumn,
    include: &Option<IncludeExcludeParam>,
    exclude: &Option<IncludeExcludeParam>,
) -> crate::Result<Option<BitSet>> {
    let mut allowed: Option<BitSet> = None;
    let num_terms = str_col.dictionary().num_terms() as u32;
    if let Some(include) = include {
        // add matches
        allowed = Some(BitSet::with_max_value(num_terms));
        let allowed = allowed.as_mut().unwrap();
        for_each_matching_term_ord(str_col, include, |ord| allowed.insert(ord))?;
    };

    if let Some(exclude) = exclude {
        if allowed.is_none() {
            // Start with all terms allowed
            allowed = Some(BitSet::with_max_value_and_full(num_terms));
        }
        let allowed = allowed.as_mut().unwrap();
        for_each_matching_term_ord(str_col, exclude, |ord| allowed.remove(ord))?;
    }

    Ok(allowed)
}

/// Apply a callback to each matching term ordinal for the given include/exclude parameter.
fn for_each_matching_term_ord(
    str_col: &StrColumn,
    param: &IncludeExcludeParam,
    mut cb: impl FnMut(u32),
) -> crate::Result<()> {
    match param {
        IncludeExcludeParam::Regex(pattern) => {
            let re = Regex::new(pattern).map_err(|e| {
                crate::TantivyError::InvalidArgument(format!("Invalid regex `{}`: {}", pattern, e))
            })?;
            // TODO: we can handle patterns like `^prefix.*` more efficiently
            let mut stream = str_col.dictionary().search(re).into_stream()?;
            while stream.advance() {
                cb(stream.term_ord() as u32);
            }
        }
        IncludeExcludeParam::Values(values) => {
            let set: FxHashSet<&str> = values.iter().map(|s| s.as_str()).collect();
            let mut stream = str_col.dictionary().stream()?;
            while stream.advance() {
                if let Ok(key_str) = std::str::from_utf8(stream.key()) {
                    if set.contains(key_str) {
                        cb(stream.term_ord() as u32);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Convert the aggregation tree to something serializable and easy to read.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct AggTreeViewNode {
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub children: Vec<AggTreeViewNode>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::get_test_index_2_segments;

    fn agg_from_json(val: serde_json::Value) -> crate::aggregation::agg_req::Aggregation {
        serde_json::from_value(val).unwrap()
    }

    #[test]
    fn test_tree_roots_and_expansion_terms_missing_on_numeric() -> crate::Result<()> {
        let index = get_test_index_2_segments(true)?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0u32);

        // Build request with:
        // 1) Terms on numeric field with missing as string => expands to MissingTerm + Terms
        // 2) Avg metric
        // 3) Terms on string with child histogram
        let terms_score_missing = agg_from_json(json!({
            "terms": {"field": "score", "missing": "NA"}
        }));
        let avg_score = agg_from_json(json!({
            "avg": {"field": "score"}
        }));
        let terms_string_with_child = agg_from_json(json!({
            "terms": {"field": "string_id"},
            "aggs": {
                "histo": {"histogram": {"field": "score", "interval": 10.0}}
            }
        }));

        let aggs: Aggregations = vec![
            ("t_score_missing_str".to_string(), terms_score_missing),
            ("avg_score".to_string(), avg_score),
            ("terms_string".to_string(), terms_string_with_child),
        ]
        .into_iter()
        .collect();

        let data = build_aggregations_data_from_req(&aggs, seg_reader, 0u32, Default::default())?;
        let printed_nodes = data.per_request.get_view_tree();
        let printed = serde_json::to_value(&printed_nodes).unwrap();

        let expected = json!([
            {"name": "avg_score", "kind": "Metric"},
            {"name": "t_score_missing_str", "kind": "MissingTerm"},
            {"name": "t_score_missing_str", "kind": "Terms"},
            {"name": "terms_string", "kind": "Terms", "children": [
                {"name": "histo", "kind": "Histogram"}
            ]}
        ]);
        assert_eq!(
            printed,
            expected,
            "tree json:\n{}",
            serde_json::to_string_pretty(&printed).unwrap()
        );

        Ok(())
    }
}
