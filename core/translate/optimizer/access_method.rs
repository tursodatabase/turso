use crate::sync::Arc;
use smallvec::SmallVec;
use std::iter;

use turso_ext::{ConstraintInfo, ConstraintUsage, ResultCode};
use turso_parser::ast::{self, SortOrder};

use crate::alloc::TursoIteratorExt;
use crate::schema::Schema;
use crate::stats::AnalyzeStats;
use crate::translate::optimizer::constraints::{
    convert_to_vtab_constraint_for_prefix, Constraint, ConstraintOperator, RangeConstraintRef,
};
use crate::translate::optimizer::cost::{rows_per_leaf_page_for_index, RowCountEstimate};
use crate::translate::optimizer::cost_params::CostModelParams;
use crate::translate::plan::{BitSet, HashJoinKey, HashJoinType, HirPlannedSource};
use crate::translate::semantic::hir;
use crate::vdbe::affinity::Affinity;
use crate::{
    schema::{Index, IndexColumn, Table},
    translate::plan::IterationDirection,
    vtab::VirtualTable,
    LimboError, Result,
};

use super::{
    constraints::{usable_constraints_for_lhs_mask, TableConstraints},
    cost::{
        estimate_btree_depth, estimate_cost_for_scan_or_seek, estimate_ephemeral_index_build_cost,
        estimate_index_cost, estimate_rows_per_seek, estimate_scan_cost, estimate_sort_cpu_cost,
        AnalyzeCtx, Cost, IndexInfo,
    },
    order::{
        hir_btree_access_order_consumed, EqualityPrefixScope, HirOrderSource, OrderConsumption,
        OrderTarget,
    },
};
use crate::translate::planner::TableMask;

pub(super) trait AccessSource {
    fn table(&self) -> &Table;
    fn index_is_covering(&self, index: &Index) -> bool;
    /// Whether the source is the right side of a RIGHT or FULL JOIN, whose
    /// unmatched rows a pass after the loops reads from the table cursor.
    fn keeps_right_rows(&self) -> bool;
}

impl<T: AccessSource + ?Sized> AccessSource for &T {
    fn table(&self) -> &Table {
        (*self).table()
    }

    fn index_is_covering(&self, index: &Index) -> bool {
        (*self).index_is_covering(index)
    }

    fn keeps_right_rows(&self) -> bool {
        (*self).keeps_right_rows()
    }
}

pub(crate) struct HirAccessSource<'a> {
    planned: &'a HirPlannedSource,
    definition: &'a hir::Source,
}

impl<'a> HirAccessSource<'a> {
    pub(crate) fn new(planned: &'a HirPlannedSource, definition: &'a hir::Source) -> Self {
        Self {
            planned,
            definition,
        }
    }
}

impl AccessSource for HirAccessSource<'_> {
    fn table(&self) -> &Table {
        &self.planned.table
    }

    fn index_is_covering(&self, index: &Index) -> bool {
        self.planned.index_is_covering(self.definition, index)
    }

    fn keeps_right_rows(&self) -> bool {
        self.planned
            .join_info
            .as_ref()
            .is_some_and(|join| matches!(join.kind, hir::JoinKind::Right | hir::JoinKind::Full))
    }
}

pub(super) trait BtreeCandidateSource<I, O>: AccessSource {
    fn order_consumed(
        &self,
        iter_dir: IterationDirection,
        index: Option<&Index>,
        constraint_refs: &[RangeConstraintRef],
        order_target: &OrderTarget<I, O>,
        schema: &Schema,
    ) -> Result<OrderConsumption>;
}

pub(super) trait BtreeAccessSource<I, O>: BtreeCandidateSource<I, O> {
    fn allows_alternative_access(&self) -> bool;
    /// Whether the table may not be read through a temporary index.
    fn forbids_automatic_index(&self) -> bool;
    fn used_column_count(&self) -> usize;
}

impl<'a> BtreeCandidateSource<hir::SourceId, &'a hir::Expr> for HirAccessSource<'a> {
    fn order_consumed(
        &self,
        iter_dir: IterationDirection,
        index: Option<&Index>,
        constraint_refs: &[RangeConstraintRef],
        order_target: &OrderTarget<hir::SourceId, &'a hir::Expr>,
        schema: &Schema,
    ) -> Result<OrderConsumption> {
        hir_btree_access_order_consumed(
            &HirOrderSource::new(self.planned, self.definition),
            iter_dir,
            index,
            constraint_refs,
            order_target,
            0,
            schema,
            EqualityPrefixScope::AnyEquality,
        )
    }
}

impl<'a> BtreeAccessSource<hir::SourceId, &'a hir::Expr> for HirAccessSource<'a> {
    fn allows_alternative_access(&self) -> bool {
        matches!(self.planned.indexed, hir::IndexHint::None)
    }

    /// HIR runs a FULL JOIN like SQLite, as nested loops plus a pass over
    /// the right table for its unmatched rows, so like a RIGHT JOIN its
    /// right side may seek a temporary index instead of being scanned again
    /// for every left row.
    fn forbids_automatic_index(&self) -> bool {
        false
    }

    fn used_column_count(&self) -> usize {
        self.planned
            .table
            .columns()
            .iter()
            .enumerate()
            .filter(|(column_idx, _)| self.planned.col_used_mask.get(*column_idx))
            .count()
    }
}

#[derive(Debug, Clone)]
/// Represents a way to access a table.
pub struct AccessMethod {
    /// The estimated page and CPU work for this path.
    pub cost: Cost,
    /// Estimated rows produced per outer row before applying remaining filters.
    pub estimated_rows_per_outer_row: f64,
    /// WHERE-term indices already accounted for by this access path's row estimate.
    pub consumed_where_terms: BitSet<usize>,
    /// Table-type specific access method details.
    pub params: AccessMethodParams,
}

/// Table‑specific details of how an [`AccessMethod`] operates.
#[derive(Debug, Clone)]
pub enum AccessMethodParams {
    BTreeTable {
        /// The direction of iteration for the access method.
        /// Typically this is backwards only if it helps satisfy an [OrderTarget].
        iter_dir: IterationDirection,
        /// The index that is being used, if any. For rowid based searches (and full table scans), this is None.
        index: Option<Arc<Index>>,
        /// True when this path needs a temporary index.
        /// The index is built after the planner chooses one plan.
        build_index: bool,
        /// The constraint references that are being used, if any.
        /// An empty list of constraint refs means a scan (full table or index);
        /// a non-empty list means a search.
        constraint_refs: Vec<RangeConstraintRef>,
    },
    VirtualTable {
        /// Index identifier returned by the table's `best_index` method.
        idx_num: i32,
        /// Optional index string returned by the table's `best_index` method.
        idx_str: Option<String>,
        /// Constraint descriptors passed to the virtual table’s `filter` method.
        /// Each corresponds to a column/operator pair from the WHERE clause.
        constraints: Vec<ConstraintInfo>,
        /// Information returned by the virtual table's `best_index` method
        /// describing how each constraint will be used.
        constraint_usages: Vec<ConstraintUsage>,
    },
    /// FROM-subquery scan. Coroutine-backed scans run forwards; materialized
    /// subqueries may also be scanned backwards when their intrinsic order
    /// matches the requested extremum order.
    Subquery { iter_dir: IterationDirection },
    /// Materialized subquery with an ephemeral index for seeking.
    /// The subquery results are materialized once into an ephemeral index,
    /// which can then be seeked using join conditions.
    MaterializedSubquery {
        /// The ephemeral index to build and seek into.
        index: Arc<Index>,
        /// The constraint references used for seeking.
        constraint_refs: Vec<RangeConstraintRef>,
        /// The direction to iterate the ephemeral index once positioned.
        iter_dir: IterationDirection,
    },
    HashJoin {
        /// The table to build the hash table from.
        build_table_idx: usize,
        /// The table to probe the hash table with.
        probe_table_idx: usize,
        /// Join key references - each entry contains the where_clause index and which side
        /// of the equality belongs to the build table. Supports expression-based join keys.
        join_keys: Vec<HashJoinKey>,
        /// Whether the build input should be materialized as a rowid list before hash build.
        materialize_build_input: bool,
        /// Join semantics: Inner, LeftOuter, or FullOuter.
        join_type: HashJoinType,
    },
    /// IN-list driven index seek.
    InSeek {
        index: Option<Arc<Index>>,
        affinity: Affinity,
        where_term_idx: usize,
    },
}

/// Result of generic btree candidate selection before it is wrapped into a full
/// [`AccessMethod`].
pub(super) struct ChosenBtreeCandidate {
    pub(super) iter_dir: IterationDirection,
    pub(super) index: Option<Arc<Index>>,
    pub(super) constraint_refs: SmallVec<[RangeConstraintRef; 2]>,
    pub(super) base_row_count: RowCountEstimate,
    pub(super) partial_index_predicate_terms: SmallVec<[usize; 4]>,
    pub(super) cost: Cost,
}

#[derive(Debug, Clone)]
pub(super) struct ChosenInSeekCandidate {
    pub(super) index: Option<Arc<Index>>,
    pub(super) affinity: Affinity,
    pub(super) constraint_idx: usize,
    pub(super) cost: Cost,
    pub(super) estimated_rows_per_outer_row: f64,
    pub(super) partial_index_predicate_terms: SmallVec<[usize; 4]>,
}

/// Describes what a caller needs to read from a branch-local scan.
///
#[allow(clippy::too_many_arguments)]
/// Choose the best ordinary btree lookup candidate for one table under the
/// current join-order prefix.
pub(super) fn choose_best_btree_candidate<C, I, O>(
    rhs_source: &impl BtreeCandidateSource<I, O>,
    rhs_constraints: &TableConstraints<C>,
    lhs_mask: &TableMask,
    rhs_table_idx: usize,
    maybe_order_target: Option<&OrderTarget<I, O>>,
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<ChosenBtreeCandidate>> {
    let keeps_right_rows = rhs_source.keeps_right_rows();
    // Seed the baseline with a table scan only if a rowid candidate exists
    // (i.e. no INDEXED BY has removed it). Otherwise start at infinite cost
    // so the forced index candidate always wins.
    let has_rowid_candidate = rhs_constraints.candidates.iter().any(|c| c.index.is_none());
    let mut has_valid_candidate = false;
    let mut best_cost = if has_rowid_candidate {
        estimate_cost_for_scan_or_seek(
            None,
            &rhs_constraints.constraints,
            &[],
            input_cardinality,
            base_row_count,
            false,
            params,
            None,
        )
    } else {
        Cost(f64::MAX)
    };
    let mut best_choice = ChosenBtreeCandidate {
        iter_dir: IterationDirection::Forwards,
        index: None,
        constraint_refs: SmallVec::new(),
        base_row_count,
        partial_index_predicate_terms: SmallVec::new(),
        cost: best_cost,
    };
    let mut best_adjusted_output = f64::MAX;
    let mut best_is_ordered = false;

    // Build a mask for the rhs table itself.
    let mut rhs_table_mask = TableMask::default();
    rhs_table_mask.set(rhs_table_idx)?;

    // Estimate cost for each candidate index (including the rowid index) and
    // keep the best candidate.
    for candidate in rhs_constraints.candidates.iter() {
        let usable_constraint_refs = usable_constraints_for_lhs_mask(
            &rhs_constraints.constraints,
            &candidate.refs,
            lhs_mask,
            rhs_table_idx,
        );
        // The unmatched-row pass reads expressions from the table cursor after
        // a full index scan ends. An expression-index cursor cannot supply
        // values to that pass. A constrained seek remains safe because the
        // expression is calculated from the matching table row.
        if keeps_right_rows
            && usable_constraint_refs.is_empty()
            && candidate
                .index
                .as_ref()
                .is_some_and(|index| index.is_expression_index())
        {
            continue;
        }
        has_valid_candidate = true;

        let index_info = match candidate.index.as_ref() {
            Some(index) => Some(IndexInfo {
                unique: index.unique,
                covering: rhs_source.index_is_covering(index),
                column_count: index.columns.len(),
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    index.columns.len(),
                    rhs_source.table(),
                    params.rows_per_table_page,
                ),
            }),
            None if usable_constraint_refs.is_empty() => None,
            None => Some(IndexInfo {
                unique: true,
                covering: true,
                column_count: 1,
                rows_per_leaf_page: params.rows_per_table_page,
            }),
        };

        let (iter_dir, is_index_ordered, order_satisfiability_bonus) =
            if let Some(order_target) = maybe_order_target {
                // Reuse the same index-vs-order matching logic as final plan
                // validation, but allow any equality-constrained seek prefix to
                // be skipped here. Candidate scoring only needs to know whether
                // this specific access path can emit rows ordered after its seek
                // key; final global ORDER BY validation is stricter and only
                // skips globally constant prefixes.
                let all_same_direction = rhs_source
                    .order_consumed(
                        IterationDirection::Forwards,
                        candidate.index.as_deref(),
                        &usable_constraint_refs,
                        order_target,
                        schema,
                    )?
                    .consumed
                    == order_target.columns.len();
                let all_opposite_direction = rhs_source
                    .order_consumed(
                        IterationDirection::Backwards,
                        candidate.index.as_deref(),
                        &usable_constraint_refs,
                        order_target,
                        schema,
                    )?
                    .consumed
                    == order_target.columns.len();

                let satisfies_order = all_same_direction || all_opposite_direction;
                if satisfies_order {
                    let sort_cost_saved = estimate_sort_cpu_cost(*base_row_count, params);
                    (
                        if all_same_direction {
                            IterationDirection::Forwards
                        } else {
                            IterationDirection::Backwards
                        },
                        true,
                        sort_cost_saved,
                    )
                } else {
                    (IterationDirection::Forwards, false, Cost(0.0))
                }
            } else {
                (IterationDirection::Forwards, false, Cost(0.0))
            };

        if index_info.is_none() && !is_index_ordered {
            continue;
        }

        let analyze_ctx = AnalyzeCtx {
            table_name: rhs_source.table().get_name(),
            index: candidate.index.as_ref(),
            stats: analyze_stats,
        };
        // For partial indexes, the index physically contains only the rows whose
        // values pass the index's WHERE clause. Discount the row count estimate
        // accordingly so the cost model recognizes the partial index as cheaper
        // than a full table scan.
        let candidate_base_row_count = candidate
            .partial_index
            .as_ref()
            .map(|partial_index| {
                RowCountEstimate::AnalyzeStats(
                    (*base_row_count * partial_index.selectivity.clamp(1e-6, 1.0)).max(1.0),
                )
            })
            .unwrap_or(base_row_count);
        let cost = estimate_cost_for_scan_or_seek(
            index_info,
            &rhs_constraints.constraints,
            &usable_constraint_refs,
            input_cardinality,
            candidate_base_row_count,
            is_index_ordered,
            params,
            Some(&analyze_ctx),
        );

        // Residual filter output adjustment (mirrors SQLite's whereLoopOutputAdjust).
        //
        // When two indexes have the same seek cost, the one whose seek
        // prerequisites already cover more residual WHERE constraints will
        // produce fewer output rows (because those residual filters can be
        // accounted for). This breaks ties correctly: a join-driven seek
        // like fromId=e1.toId (prereqs={e1}) can claim credit for the
        // constant residual label='requires', but a constant seek like
        // label='requires' (prereqs={}) cannot claim credit for the
        // join-dependent residual fromId=e1.toId.
        let loop_prereq_mask = {
            let mut mask = TableMask::default();
            for ucref in usable_constraint_refs.iter() {
                for idx in [
                    ucref.eq.as_ref().map(|e| e.constraint_pos),
                    ucref.lower_bound,
                    ucref.upper_bound,
                ]
                .into_iter()
                .flatten()
                {
                    let c = &rhs_constraints.constraints[idx];
                    mask = mask.iter().chain(c.lhs_mask.iter()).try_collect()?;
                }
            }
            mask
        };
        // Tables whose constraints this loop can account for: the loop's own
        // prerequisite tables plus the current table itself.
        let allowed_mask: TableMask = loop_prereq_mask
            .iter()
            .chain(rhs_table_mask.iter())
            .try_collect()?;

        // Collect which constraint positions are consumed by the index seek.
        let consumed: SmallVec<[usize; 8]> = usable_constraint_refs
            .iter()
            .flat_map(|ucref| {
                [
                    ucref.eq.as_ref().map(|e| e.constraint_pos),
                    ucref.lower_bound,
                    ucref.upper_bound,
                ]
                .into_iter()
                .flatten()
            })
            .collect();

        // Multiply selectivities of residual constraints whose prerequisites
        // are within the allowed mask (i.e. already satisfied by this loop).
        let residual_selectivity: f64 = rhs_constraints
            .constraints
            .iter()
            .enumerate()
            .filter(|(i, c)| {
                !consumed.contains(i)
                    && c.usable
                    && allowed_mask.contains_all_set_bits_of(&c.lhs_mask)
                    && matches!(
                        c.operator,
                        ConstraintOperator::AstNativeOperator(ast::Operator::Equals)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::Greater)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::GreaterEquals)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::Less)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::LessEquals)
                    )
            })
            .map(|(_, c)| c.selectivity)
            .product();

        // Adjusted output: lower means the loop delivers fewer rows downstream.
        let adjusted_output = residual_selectivity;

        // Only apply the order bonus when this candidate satisfies order but
        // the current best does not. When both satisfy order, switching saves
        // no additional sort cost.
        let effective_bonus = if is_index_ordered && !best_is_ordered {
            order_satisfiability_bonus
        } else {
            Cost(0.0)
        };
        let adjusted_best = best_cost + effective_bonus;
        let costs_equal = (cost.0 - adjusted_best.0).abs() < 1e-9;
        if cost < adjusted_best || (costs_equal && adjusted_output < best_adjusted_output - 1e-12) {
            best_cost = cost;
            best_adjusted_output = adjusted_output;
            best_is_ordered = is_index_ordered;
            best_choice = ChosenBtreeCandidate {
                iter_dir,
                index: candidate.index.clone(),
                constraint_refs: usable_constraint_refs,
                base_row_count: candidate_base_row_count,
                partial_index_predicate_terms: candidate
                    .partial_index
                    .as_ref()
                    .map_or_else(SmallVec::new, |partial_index| {
                        partial_index.predicate_terms.clone()
                    }),
                cost,
            };
        }
    }

    // `best_choice` starts as a table scan even when INDEXED BY removed that choice.
    // Do not return that placeholder if all forced candidates were rejected.
    Ok(has_valid_candidate.then_some(best_choice))
}

fn consumed_where_terms_from_constraint_refs<E>(
    constraints: &[Constraint<E>],
    constraint_refs: &[RangeConstraintRef],
) -> Result<BitSet<usize>> {
    let mut consumed = BitSet::default();
    for cref in constraint_refs {
        for constraint_idx in [
            cref.eq.as_ref().map(|eq| eq.constraint_pos),
            cref.lower_bound,
            cref.upper_bound,
        ]
        .into_iter()
        .flatten()
        {
            let where_term_idx = constraints[constraint_idx].where_clause_pos.0;
            consumed.set(where_term_idx)?;
        }
    }
    Ok(consumed)
}

/// Turn a chosen B-tree candidate into the access method consumed by join
/// planning. Candidate discovery is representation-specific; these physical
/// row-count and consumed-predicate facts are not.
pub(super) fn build_btree_access_method<E>(
    source: &impl AccessSource,
    constraints: &TableConstraints<E>,
    chosen: ChosenBtreeCandidate,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> Result<AccessMethod> {
    let ChosenBtreeCandidate {
        iter_dir,
        index,
        constraint_refs,
        base_row_count,
        partial_index_predicate_terms,
        cost,
    } = chosen;
    let estimated_rows_per_outer_row = if constraint_refs.is_empty() {
        *base_row_count
    } else {
        let index_info = match index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: source.index_is_covering(index),
                column_count: index.columns.len(),
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    index.columns.len(),
                    source.table(),
                    params.rows_per_table_page,
                ),
            },
            None => IndexInfo {
                unique: true,
                covering: true,
                column_count: 1,
                rows_per_leaf_page: params.rows_per_table_page,
            },
        };
        let analyze_ctx = AnalyzeCtx {
            table_name: source.table().get_name(),
            index: index.as_ref(),
            stats: analyze_stats,
        };
        estimate_rows_per_seek(
            index_info,
            &constraints.constraints,
            &constraint_refs,
            base_row_count,
            Some(&analyze_ctx),
        )
    };
    let mut consumed_where_terms =
        consumed_where_terms_from_constraint_refs(&constraints.constraints, &constraint_refs)?;
    for term_idx in partial_index_predicate_terms {
        consumed_where_terms.set(term_idx)?;
    }
    Ok(AccessMethod {
        cost,
        estimated_rows_per_outer_row,
        consumed_where_terms,
        params: AccessMethodParams::BTreeTable {
            iter_dir,
            index,
            build_index: false,
            constraint_refs: constraint_refs.into_vec(),
        },
    })
}

#[allow(clippy::too_many_arguments)]
/// Choose and build the best ordinary B-tree access method for one source.
pub(super) fn choose_btree_access_method<C, I, O>(
    source: &impl BtreeCandidateSource<I, O>,
    constraints: &TableConstraints<C>,
    lhs_mask: &TableMask,
    source_position: usize,
    order_target: Option<&OrderTarget<I, O>>,
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<AccessMethod> {
    let chosen = choose_best_btree_candidate(
        source,
        constraints,
        lhs_mask,
        source_position,
        order_target,
        schema,
        analyze_stats,
        input_cardinality,
        base_row_count,
        params,
    )?;
    // The scan candidate is always there, unless INDEXED BY forces an
    // expression index that the right table of a RIGHT or FULL JOIN cannot
    // use: SQLite reports that as no query solution.
    let Some(chosen) = chosen else {
        crate::bail_parse_error!("no query solution");
    };

    build_btree_access_method(source, constraints, chosen, analyze_stats, params)
}

/// The affinity an IN seek compares with: the column's own, except that a
/// numeric column compares as NUMERIC and a column without affinity as BLOB,
/// as a comparison against a value with no affinity does.
fn in_seek_comparison_affinity(column: Affinity) -> Affinity {
    if column.is_numeric() {
        Affinity::Numeric
    } else if column == Affinity::None {
        Affinity::Blob
    } else {
        column
    }
}

#[allow(clippy::too_many_arguments)]
/// Evaluate whether an `IN (...)` predicate should replace the ordinary btree
/// access path with repeated equality seeks.
///
/// This is intentionally separate from `choose_best_btree_candidate()`: the
/// generic btree chooser reasons about a single continuous scan/seek over one
/// candidate, while `InSeek` emits a two-level loop that materializes the RHS
/// into an ephemeral cursor and performs one equality seek per RHS value.
/// Because of that execution shape, only rowid or the first column of an index
/// can drive `InSeek`, and the comparison collation must match the chosen
/// index's first-key collation.
pub(super) fn choose_best_in_seek_candidate<E>(
    rhs_source: &impl AccessSource,
    rhs_constraints: &TableConstraints<E>,
    lhs_mask: &TableMask,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
) -> Result<Option<ChosenInSeekCandidate>> {
    let rhs_table = rhs_source.table();
    let Table::BTree(btree) = rhs_table else {
        return Err(LimboError::InternalError(
            "IN-seek candidate selection called on non-BTree table".into(),
        ));
    };

    let base = *base_row_count;
    let tree_depth = estimate_btree_depth(base, params.rows_per_table_page);
    let mut best_in_seek = None;
    let mut best_in_seek_cost = best_cost;

    for candidate in rhs_constraints.candidates.iter() {
        let first_col_pos = candidate
            .index
            .as_ref()
            .and_then(|idx| idx.columns.first().map(|c| c.pos_in_table));

        let rowid_only = false;
        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rowid_only || rhs_source.index_is_covering(index),
                column_count: index.columns.len(),
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    index.columns.len(),
                    rhs_table,
                    params.rows_per_table_page,
                ),
            },
            None => IndexInfo {
                unique: true,
                covering: false,
                column_count: 1,
                rows_per_leaf_page: params.rows_per_table_page,
            },
        };

        for constraint in &rhs_constraints.constraints {
            let ConstraintOperator::In {
                not,
                estimated_values,
            } = constraint.operator
            else {
                continue;
            };
            // A WHERE-clause IN term on a table an outer join can
            // null-extend must stay a plain filter that also runs on the
            // null-extended row, and an IN term cannot be re-checked after
            // the seek, since `InSeek` iterates the IN list's cursor and the
            // check would probe that same cursor.
            if not
                || !constraint.outer_join_compatible
                || !lhs_mask.contains_all_set_bits_of(&constraint.lhs_mask)
            {
                continue;
            }

            let matches = if candidate.index.is_none() {
                constraint.is_rowid
            } else {
                !constraint.is_rowid
                    && constraint.table_col_pos.is_some()
                    && constraint.table_col_pos == first_col_pos
            };
            if !matches {
                continue;
            }

            // `open_loop` copies the chosen index collation onto the ephemeral
            // IN cursor. Reject mismatches here so a BINARY `IN` comparison
            // cannot silently become `NOCASE`/`RTRIM` just because the index is.
            if let (Some(index), Some(col_pos)) = (&candidate.index, constraint.table_col_pos) {
                let constrained_column = &rhs_table.columns()[col_pos];
                let table_collation = constrained_column.collation();
                let index_collation = index.columns[0].collation.unwrap_or_default();
                if table_collation != index_collation {
                    continue;
                }
                let idx_aff = constrained_column.affinity();
                if !constraint.satisfies_index_affinity(idx_aff) {
                    continue;
                }
            }

            let rows_per_seek = if (index_info.unique && index_info.column_count == 1)
                || candidate.index.is_none()
            {
                1.0
            } else {
                (base * params.sel_eq_indexed).sqrt().max(1.0)
            };
            let in_cost = estimate_index_cost(
                base,
                tree_depth,
                index_info,
                estimated_values * input_cardinality,
                rows_per_seek,
                params,
            );
            if in_cost >= best_in_seek_cost {
                continue;
            }

            let lhs_affinity = if let Some(col_pos) = constraint.table_col_pos {
                btree
                    .columns()
                    .get(col_pos)
                    .map(|col| col.affinity())
                    .unwrap_or(Affinity::Blob)
            } else {
                Affinity::Integer
            };
            let affinity = in_seek_comparison_affinity(lhs_affinity);
            best_in_seek_cost = in_cost;
            best_in_seek = Some(ChosenInSeekCandidate {
                index: candidate.index.clone(),
                affinity,
                constraint_idx: constraint.where_clause_pos.0,
                cost: in_cost,
                estimated_rows_per_outer_row: (constraint.selectivity * base).max(1.0),
                partial_index_predicate_terms: candidate
                    .partial_index
                    .as_ref()
                    .map_or_else(SmallVec::new, |partial_index| {
                        partial_index.predicate_terms.clone()
                    }),
            });
        }
    }

    Ok(best_in_seek)
}

fn build_in_seek_access_method(chosen: ChosenInSeekCandidate) -> Result<AccessMethod> {
    let mut consumed_where_terms: BitSet<usize> =
        iter::once(chosen.constraint_idx).try_collect()?;
    for term_idx in chosen.partial_index_predicate_terms {
        consumed_where_terms.set(term_idx)?;
    }
    Ok(AccessMethod {
        cost: chosen.cost,
        estimated_rows_per_outer_row: chosen.estimated_rows_per_outer_row,
        consumed_where_terms,
        params: AccessMethodParams::InSeek {
            index: chosen.index,
            affinity: chosen.affinity,
            where_term_idx: chosen.constraint_idx,
        },
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn choose_in_seek_access_method<E>(
    source: &impl AccessSource,
    constraints: &TableConstraints<E>,
    lhs_mask: &TableMask,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
) -> Result<Option<AccessMethod>> {
    choose_best_in_seek_candidate(
        source,
        constraints,
        lhs_mask,
        input_cardinality,
        base_row_count,
        params,
        best_cost,
    )?
    .map(build_in_seek_access_method)
    .transpose()
}

#[allow(clippy::too_many_arguments)]
/// The best access method of one HIR B-tree table read once, after the
/// loops of `lhs_mask`, with no order to satisfy: the read of a RIGHT or
/// FULL JOIN's unmatched rows. `source` carries no join, so an INDEXED BY
/// expression index is read like for any table. No automatic index is
/// built for a read that runs once; an IN seek is still considered.
#[allow(clippy::too_many_arguments)]
pub(crate) fn choose_hir_single_read_access_method(
    source: &HirPlannedSource,
    definition: &hir::Source,
    constraints: &TableConstraints<hir::Expr>,
    lhs_mask: &TableMask,
    source_position: usize,
    schema: &Schema,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<AccessMethod> {
    let source = HirAccessSource::new(source, definition);
    let mut best = choose_btree_access_method(
        &source,
        constraints,
        lhs_mask,
        source_position,
        None::<&OrderTarget<hir::SourceId, &hir::Expr>>,
        schema,
        &schema.analyze_stats,
        1.0,
        base_row_count,
        params,
    )?;
    let mut best_cost = best.cost;
    let has_rowid = source.table().btree().is_some_and(|btree| btree.has_rowid);
    if source.allows_alternative_access() && has_rowid {
        if let Some(in_seek) = choose_in_seek_access_method(
            &source,
            constraints,
            lhs_mask,
            1.0,
            base_row_count,
            params,
            best_cost,
        )? {
            replace_if_cheaper(&mut best, &mut best_cost, in_seek, &[], 1.0, params);
        }
    }
    Ok(best)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn choose_single_btree_access_method<E, I, O>(
    source: &impl BtreeAccessSource<I, O>,
    constraints: &TableConstraints<E>,
    lhs_mask: &TableMask,
    source_position: usize,
    order_target: Option<&OrderTarget<I, O>>,
    ready_predicates: &[(usize, usize)],
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<AccessMethod> {
    let mut best = choose_btree_access_method(
        source,
        constraints,
        lhs_mask,
        source_position,
        order_target,
        schema,
        analyze_stats,
        input_cardinality,
        base_row_count,
        params,
    )?;
    let AccessMethodParams::BTreeTable {
        iter_dir: ordinary_iter_dir,
        ..
    } = &best.params
    else {
        unreachable!("ordinary B-tree selection returns BTreeTable access")
    };
    let ordinary_iter_dir = *ordinary_iter_dir;
    let mut best_cost = cost_with_where_work(&best, ready_predicates, input_cardinality, params);

    let uses_full_table_scan = matches!(
        &best.params,
        AccessMethodParams::BTreeTable {
            index: None,
            build_index: false,
            constraint_refs,
            ..
        } if constraint_refs.is_empty()
    );
    // A temporary index is only built for a table joined after another one.
    // A correlated subquery's first table is scanned on each run; the
    // subquery-to-join rewrite in `semantic_unnest` speeds those up. A
    // table a RIGHT or FULL JOIN keeps is read through its own cursor, so
    // the pass over its unmatched rows can key them by rowid.
    if source.allows_alternative_access()
        && uses_full_table_scan
        && !lhs_mask.is_empty()
        && !source.forbids_automatic_index()
        && !source.keeps_right_rows()
    {
        let constraint_refs = usable_constraints_for_lhs_mask(
            &constraints.constraints,
            &constraints.temporary_index_terms,
            lhs_mask,
            source_position,
        );
        if !constraint_refs.is_empty() {
            let column_count = source.used_column_count();
            let index_info = IndexInfo {
                unique: false,
                column_count,
                covering: true,
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    column_count,
                    source.table(),
                    params.rows_per_table_page,
                ),
            };
            let rows_per_seek = estimate_rows_per_seek(
                index_info,
                &constraints.constraints,
                &constraint_refs,
                base_row_count,
                None,
            );
            let scan_cost = estimate_cost_for_scan_or_seek::<E>(
                None,
                &[],
                &[],
                1.0,
                base_row_count,
                false,
                params,
                None,
            );
            let build_cost = estimate_ephemeral_index_build_cost(*base_row_count, params);
            let seek_cost = Cost(
                input_cardinality * params.cpu_cost_per_seek
                    + input_cardinality * rows_per_seek * params.cpu_cost_per_row,
            );
            let temporary_index = AccessMethod {
                cost: scan_cost + build_cost + seek_cost,
                estimated_rows_per_outer_row: rows_per_seek,
                consumed_where_terms: consumed_where_terms_from_constraint_refs(
                    &constraints.constraints,
                    &constraint_refs,
                )?,
                params: AccessMethodParams::BTreeTable {
                    iter_dir: ordinary_iter_dir,
                    index: None,
                    build_index: true,
                    constraint_refs: Vec::new(),
                },
            };
            replace_if_cheaper(
                &mut best,
                &mut best_cost,
                temporary_index,
                ready_predicates,
                input_cardinality,
                params,
            );
        }
    }

    let has_rowid = source.table().btree().is_some_and(|btree| btree.has_rowid);
    if source.allows_alternative_access() && has_rowid {
        if let Some(in_seek) = choose_in_seek_access_method(
            source,
            constraints,
            lhs_mask,
            input_cardinality,
            base_row_count,
            params,
            best_cost,
        )? {
            replace_if_cheaper(
                &mut best,
                &mut best_cost,
                in_seek,
                ready_predicates,
                input_cardinality,
                params,
            );
        }
    }

    Ok(best)
}

/// Add the cost of ready `WHERE` conditions.
pub(super) fn cost_with_where_work(
    method: &AccessMethod,
    ready_where: &[(usize, usize)],
    input_cardinality: f64,
    params: &CostModelParams,
) -> Cost {
    let used_steps: usize = ready_where
        .iter()
        .filter(|(term_idx, _)| method.consumed_where_terms.get(*term_idx))
        .map(|(_, step_count)| step_count)
        .sum();
    let remaining_steps: usize = ready_where
        .iter()
        .filter(|(term_idx, _)| !method.consumed_where_terms.get(*term_idx))
        .map(|(_, step_count)| step_count)
        .sum();
    let output_rows = input_cardinality * method.estimated_rows_per_outer_row;
    let used_rows = input_cardinality.max(output_rows);
    let work = used_rows * used_steps as f64 + output_rows * remaining_steps as f64;
    method.cost + Cost(work * params.cpu_cost_per_where_step)
}

pub(super) fn add_where_cost(
    method: &mut AccessMethod,
    ready_where: &[(usize, usize)],
    input_cardinality: f64,
    params: &CostModelParams,
) {
    method.cost = cost_with_where_work(method, ready_where, input_cardinality, params);
}

fn replace_if_cheaper(
    best_method: &mut AccessMethod,
    best_cost: &mut Cost,
    method: AccessMethod,
    ready_where: &[(usize, usize)],
    input_cardinality: f64,
    params: &CostModelParams,
) {
    let cost = cost_with_where_work(&method, ready_where, input_cardinality, params);
    if cost < *best_cost {
        *best_method = method;
        *best_cost = cost;
    }
}

pub(crate) fn find_best_hir_access_method_for_vtab<E>(
    vtab: &VirtualTable,
    constraints: &[Constraint<E>],
    source_position: usize,
    lhs_mask: &TableMask,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    let vtab_constraints =
        convert_to_vtab_constraint_for_prefix(constraints, source_position, lhs_mask);
    find_best_access_method_for_vtab_constraints(
        vtab,
        constraints,
        vtab_constraints,
        input_cardinality,
        base_row_count,
        params,
    )
}

fn find_best_access_method_for_vtab_constraints<E>(
    vtab: &VirtualTable,
    constraints: &[Constraint<E>],
    vtab_constraints: Vec<ConstraintInfo>,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    // TODO: get proper order_by information to pass to the vtab.
    // maybe encode more info on t_ctx? we need: [col_idx , is_descending]
    let best_index_result = vtab.best_index(&vtab_constraints, &[]);

    match best_index_result {
        Ok(index_info) => {
            if index_info.constraint_usages.len() != vtab_constraints.len() {
                return Err(LimboError::ExtensionError(format!(
                    "Constraint usage count mismatch (expected {}, got {})",
                    vtab_constraints.len(),
                    index_info.constraint_usages.len()
                )));
            }
            let has_row_estimate = index_info.estimated_rows != u32::MAX;
            let estimated_rows_per_outer_row = if has_row_estimate {
                f64::from(index_info.estimated_rows)
            } else {
                *base_row_count
            };
            // A row estimate includes each condition passed to the virtual table.
            // Do not apply the same row cut again in the join planner.
            let consumed_where_terms = if has_row_estimate {
                vtab_constraints
                    .iter()
                    .zip(&index_info.constraint_usages)
                    .filter(|(_, usage)| usage.argv_index.is_some())
                    .map(|(vtab_constraint, _)| {
                        constraints[vtab_constraint.index].where_clause_pos.0
                    })
                    .try_collect()?
            } else {
                BitSet::default()
            };
            Ok(Some(AccessMethod {
                // TODO: Base cost on `IndexInfo::estimated_cost`.
                cost: estimate_cost_for_scan_or_seek::<E>(
                    None,
                    &[],
                    &[],
                    input_cardinality,
                    base_row_count,
                    false,
                    params,
                    None,
                ),
                estimated_rows_per_outer_row,
                consumed_where_terms,
                params: AccessMethodParams::VirtualTable {
                    idx_num: index_info.idx_num,
                    idx_str: index_info.idx_str,
                    constraints: vtab_constraints,
                    constraint_usages: index_info.constraint_usages,
                },
            }))
        }
        Err(ResultCode::ConstraintViolation) => Ok(None),
        Err(e) => Err(LimboError::from(e)),
    }
}

/// Estimate the cost of a hash join between two tables.
///
/// The cost model accounts for:
/// - Build phase: Creating the hash table from the build side (one-time cost)
/// - Probe phase: Looking up each probe row in the hash table (one scan of probe table)
/// - Match phase: Visiting matching rows and scanning unmatched build rows
/// - Memory pressure: Additional IO cost if the hash table spills to disk
pub fn estimate_hash_join_cost(
    build_cardinality: f64,
    probe_cardinality: f64,
    rows_visited: f64,
    join_type: HashJoinType,
    mem_budget: usize,
    probe_multiplier: f64,
    params: &CostModelParams,
) -> Cost {
    // Estimate if the hash table will fit in memory based on actual row counts
    let estimated_hash_table_size =
        (build_cardinality as usize).saturating_mul(params.hash_bytes_per_row as usize);
    let will_spill = estimated_hash_table_size > mem_budget;

    // Build phase: hash and insert all rows from build table (one-time cost)
    // With real ANALYZE stats, this accurately reflects the actual build table size
    let build_cost = build_cardinality * (params.hash_cpu_cost + params.hash_insert_cost);

    let probe_scan_cost = estimate_scan_cost(probe_cardinality, probe_multiplier, params);
    let probe_hash_cost =
        probe_cardinality * (params.hash_cpu_cost + params.hash_lookup_cost) * probe_multiplier;
    let visit_cost = rows_visited * params.cpu_cost_per_row;
    let unmatched_build_cost = if join_type.keeps_unmatched_build_rows() {
        build_cardinality * params.cpu_cost_per_row
    } else {
        0.0
    };

    // Spill cost: if hash table exceeds memory budget, we need to write/read partitions to disk.
    // Grace hash join writes partitions and reads them back, so it's 2x the page IO.
    // Use page-based IO cost (rows / rows_per_page) rather than per-row IO.
    let spill_cost = if will_spill {
        let build_pages = (build_cardinality / params.rows_per_table_page).ceil();
        let probe_pages = (probe_cardinality / params.rows_per_table_page).ceil();
        // Write both sides to partitions, then read back: 2 * (build_pages + probe_pages)
        (build_pages + probe_pages) * 2.0 * probe_multiplier
    } else {
        0.0
    };

    Cost(
        build_cost
            + probe_scan_cost.0
            + probe_hash_cost
            + visit_cost
            + unmatched_build_cost
            + spill_cost,
    )
}

/// The access method of a HIR FROM subquery or CTE: scan its rows, or store
/// them once in a temporary index keyed by the columns earlier loops
/// constrain and seek that index for each outer row, whichever costs less.
/// The rows are stored in a table first and the
/// index is built from it, so setup always costs two passes, except for a
/// CTE that is already stored as a table. A subquery that reads an outer
/// row runs again for each one, so it is only scanned, and so is the right
/// side of a RIGHT or FULL JOIN (`may_seek` false), whose unmatched rows a
/// second pass finds by scanning the stored table.
///
/// Without seek constraints, the scan reads a stored CTE backwards when
/// that gives `order_target`. With them, the temporary index is read in
/// the direction that gives `order_target` after its seek prefix, if one
/// does, and is credited with the sort it saves.
#[allow(clippy::too_many_arguments)]
pub(crate) fn find_best_hir_access_method_for_subquery(
    document: &hir::HirDocument,
    source: hir::SourceId,
    query: hir::QueryId,
    table_materialization_required: bool,
    may_seek: bool,
    order_target: Option<&super::order::HirOrderTarget<'_>>,
    output_order: &[super::order::HirOutputOrder],
    constraints: &TableConstraints<hir::Expr>,
    lhs_mask: &TableMask,
    source_position: usize,
    ready_where: &[(usize, usize)],
    subquery_cost: Cost,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    schema: &Schema,
) -> Result<AccessMethod> {
    use super::constraints::ConstraintRef;
    let scan = |cost| AccessMethod {
        cost,
        estimated_rows_per_outer_row: *base_row_count,
        consumed_where_terms: Default::default(),
        params: AccessMethodParams::Subquery {
            iter_dir: IterationDirection::Forwards,
        },
    };
    let coroutine_scan_cost = estimate_cost_for_scan_or_seek::<hir::Expr>(
        None,
        &[],
        &[],
        input_cardinality,
        base_row_count,
        false,
        params,
        None,
    );
    let coroutine_reexecution_overhead =
        Cost((input_cardinality - 1.0).max(0.0) * *base_row_count * params.cpu_cost_per_seek);
    let coroutine_cost = coroutine_scan_cost
        + coroutine_reexecution_overhead
        + Cost(input_cardinality * subquery_cost.0);
    if document.reads_outer_sources(query) {
        return Ok(scan(coroutine_cost));
    }
    if !may_seek {
        return Ok(scan(if table_materialization_required {
            coroutine_scan_cost + subquery_cost
        } else {
            coroutine_cost
        }));
    }
    let scan_cost = if table_materialization_required {
        coroutine_scan_cost + subquery_cost
    } else {
        coroutine_cost
    };
    let definition = document.source(source).ok_or_else(|| {
        crate::LimboError::InternalError(format!("HIR subquery source {source} is missing"))
    })?;
    let usable: Vec<(usize, &Constraint<hir::Expr>)> = constraints
        .constraints
        .iter()
        .enumerate()
        .filter(|(_, constraint)| {
            matches!(
                constraint.operator.as_ast_operator(),
                Some(
                    ast::Operator::Equals
                        | ast::Operator::Greater
                        | ast::Operator::GreaterEquals
                        | ast::Operator::Less
                        | ast::Operator::LessEquals
                )
            ) && constraint.usable
                && constraint.table_col_pos.is_some_and(|position| {
                    definition
                        .columns
                        .get(position)
                        .is_some_and(|column| constraint.satisfies_index_affinity(column.affinity))
                })
        })
        .collect();
    // A MIN or MAX whose seek constraints are all on its own column can
    // walk the subquery's own order from one end.
    let extremum_constraints_compatible = order_target.is_some_and(|target| {
        target.is_extremum()
            && matches!(
                target.columns.first(),
                Some(column) if column.source == source
                    && matches!(column.target, super::order::ColumnTarget::Column(position)
                        if usable.iter().all(|(_, constraint)| constraint.table_col_pos == Some(position)))
            )
    });
    if extremum_constraints_compatible || usable.is_empty() {
        // Reading forwards gives the subquery's own order; only a stored
        // CTE can be read backwards for the opposite one.
        let direction = order_target.and_then(|target| {
            let consumed = |direction| {
                super::order::hir_subquery_intrinsic_order_consumed(
                    source,
                    output_order,
                    direction,
                    &target.columns,
                )
            };
            if consumed(IterationDirection::Forwards) == target.columns.len() {
                Some(IterationDirection::Forwards)
            } else if table_materialization_required
                && consumed(IterationDirection::Backwards) == target.columns.len()
            {
                Some(IterationDirection::Backwards)
            } else {
                None
            }
        });
        if let Some(iter_dir) = direction {
            let mut method = scan(scan_cost);
            method.params = AccessMethodParams::Subquery { iter_dir };
            return Ok(method);
        }
    }
    if usable.is_empty() {
        return Ok(scan(scan_cost));
    }
    let usable_constraints: Vec<&Constraint<hir::Expr>> =
        usable.iter().map(|(_, constraint)| *constraint).collect();
    let key_col_positions = super::constraints::ordered_ephemeral_key_columns(&usable_constraints);
    if key_col_positions.is_empty() {
        return Ok(scan(scan_cost));
    }
    let mut constraint_refs: Vec<ConstraintRef> = usable
        .iter()
        .map(|(constraint_vec_pos, constraint)| {
            let table_col_pos = constraint
                .table_col_pos
                .expect("usable subquery constraints name a column");
            let index_col_pos = key_col_positions
                .iter()
                .position(|position| *position == table_col_pos)
                .expect("every usable column is a key column");
            ConstraintRef {
                constraint_vec_pos: *constraint_vec_pos,
                index_col_pos,
                sort_order: SortOrder::Asc,
                nulls_order: ast::NullsOrder::First,
            }
        })
        .collect();
    constraint_refs.sort_by_key(|constraint| constraint.index_col_pos);
    let constraint_refs = usable_constraints_for_lhs_mask(
        &constraints.constraints,
        &constraint_refs,
        lhs_mask,
        source_position,
    )
    .into_vec();
    if constraint_refs.is_empty() {
        return Ok(scan(scan_cost));
    }
    let index = hir_materialized_subquery_ephemeral_index(definition, &key_col_positions)?;
    let estimated_rows_per_outer_row =
        if hir_grouped_subquery_lookup_is_unique(document, query, &constraint_refs) {
            1.0
        } else {
            estimate_rows_per_seek(
                IndexInfo {
                    unique: false,
                    column_count: key_col_positions.len(),
                    covering: true,
                    rows_per_leaf_page: params.rows_per_table_page,
                },
                &constraints.constraints,
                &constraint_refs,
                base_row_count,
                None,
            )
        };
    let one_pass_scan_cost = estimate_cost_for_scan_or_seek::<hir::Expr>(
        None,
        &[],
        &[],
        1.0,
        base_row_count,
        false,
        params,
        None,
    );
    let append_build_cost = estimate_ephemeral_index_build_cost(*base_row_count, params);
    let seek_setup_cost = if table_materialization_required {
        one_pass_scan_cost + append_build_cost
    } else {
        one_pass_scan_cost + one_pass_scan_cost + append_build_cost
    };
    let seek_cost = Cost(
        input_cardinality * params.cpu_cost_per_seek
            + input_cardinality * estimated_rows_per_outer_row * params.cpu_cost_per_row,
    );
    // When the index gives the rest of `order_target` from this source's
    // first term on, after its seek prefix, read in the direction that
    // does, and credit the index with the sort it saves. The terms before
    // that one belong to earlier loops, which the join order checks.
    let (iter_dir, order_bonus) = order_target
        .and_then(|target| {
            let start = target
                .columns
                .iter()
                .position(|column| column.source == source)?;
            let gives_order = |direction| {
                super::order::hir_materialized_subquery_order_consumed(
                    source,
                    &index,
                    &constraint_refs,
                    direction,
                    target,
                    start,
                    schema,
                    EqualityPrefixScope::AnyEquality,
                )
                .consumed
                    == target.columns.len() - start
            };
            let direction = if gives_order(IterationDirection::Forwards) {
                IterationDirection::Forwards
            } else if gives_order(IterationDirection::Backwards) {
                IterationDirection::Backwards
            } else {
                return None;
            };
            let n = *base_row_count;
            Some((
                direction,
                Cost(n * n.max(1.0).log2() * params.sort_cpu_per_row),
            ))
        })
        .unwrap_or((IterationDirection::Forwards, Cost(0.0)));
    let scan_method = scan(scan_cost);
    let index_method = AccessMethod {
        cost: subquery_cost + seek_setup_cost + seek_cost,
        estimated_rows_per_outer_row,
        consumed_where_terms: consumed_where_terms_from_constraint_refs(
            &constraints.constraints,
            &constraint_refs,
        )?,
        params: AccessMethodParams::MaterializedSubquery {
            index,
            constraint_refs,
            iter_dir,
        },
    };
    let scan_cost = cost_with_where_work(&scan_method, ready_where, input_cardinality, params);
    let index_cost = cost_with_where_work(&index_method, ready_where, input_cardinality, params);
    Ok(if index_cost >= scan_cost + order_bonus {
        scan_method
    } else {
        index_method
    })
}

/// The temporary index of a stored HIR subquery: the key columns first, then
/// every other column, so the index holds the whole row and the loop never
/// reads the stored table.
fn hir_materialized_subquery_ephemeral_index(
    source: &hir::Source,
    key_col_positions: &[usize],
) -> Result<Arc<Index>> {
    let index_column = |position: usize| {
        let column = &source.columns[position];
        IndexColumn {
            name: column.name.to_string(),
            order: SortOrder::Asc,
            nulls_order: None,
            pos_in_table: position,
            collation: column
                .collation
                .as_ref()
                .map(|collation| *collation.value()),
            default: None,
            expr: None,
        }
    };
    let columns = key_col_positions
        .iter()
        .copied()
        .chain((0..source.columns.len()).filter(|position| !key_col_positions.contains(position)))
        .map(index_column)
        .try_collect()?;
    Ok(Arc::new(Index {
        name: format!("ephemeral_subquery_{}", source.id),
        columns,
        unique: false,
        ephemeral: true,
        table_name: source.name.clone(),
        root_page: 0,
        where_clause: None,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
        constraint_name: None,
    }))
}

/// A grouped query has at most one row for one full group key, so a seek
/// on every key column finds at most one row.
fn hir_grouped_subquery_lookup_is_unique(
    document: &hir::HirDocument,
    query: hir::QueryId,
    constraint_refs: &[RangeConstraintRef],
) -> bool {
    let Some(query) = document.query(query) else {
        return false;
    };
    let [block] = query.blocks.as_slice() else {
        return false;
    };
    let hir::QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    else {
        return false;
    };
    !grouping.keys.is_empty()
        && grouping.keys.iter().all(|key| {
            query
                .output
                .iter()
                .position(|output| {
                    document
                        .output(*output)
                        .is_some_and(|output| output.expr.equivalent(key))
                })
                .is_some_and(|position| {
                    constraint_refs.iter().any(|constraint| {
                        constraint.table_col_pos == Some(position) && constraint.eq.is_some()
                    })
                })
        })
}
