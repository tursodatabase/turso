use super::plan::{
    HirPlannedSource, HirSeekDef, HirWhereTerm, IterationDirection, SeekDef, SeekKey,
};
use crate::alloc::TursoIteratorExt;
use crate::schema::GeneratedType;
use crate::translate::planner::TableMask;
use crate::translate::semantic::hir;
use crate::{
    schema::{Index, IndexColumn, Schema, Table},
    translate::{
        optimizer::{
            access_method::{AccessMethod, AccessMethodParams},
            constraints::{
                HirConstraint, RangeConstraintRef, SeekRangeConstraint, TableConstraints,
            },
            cost::RowCountEstimate,
        },
        plan::{PlannedSource, SeekKeyComponent},
    },
    types::SeekOp,
    vdbe::affinity::Affinity,
    LimboError, Result,
};
use crate::{turso_assert, turso_assert_eq, turso_debug_assert};
use constraints::Constraint;
use cost::Cost;
use smallvec::SmallVec;
use std::{cmp::Ordering, fmt::Display, sync::Arc};
use turso_parser::ast::{self, SortOrder};

pub(crate) mod access_method;
pub(crate) mod constraints;
pub(crate) mod cost;
mod cost_params;
pub(crate) use cost_params::CostModelParams;
pub(crate) mod join;
pub(crate) mod order;

/// Cost model used by the planner: the loaded parameters when the
/// `optimizer_params` feature is on, the built-in defaults otherwise.
pub(crate) fn cost_model_params() -> &'static cost_params::CostModelParams {
    #[cfg(feature = "optimizer_params")]
    return &cost_params::LOADED_PARAMS;
    #[cfg(not(feature = "optimizer_params"))]
    return &cost_params::DEFAULT_PARAMS;
}

/// Derive a base row-count estimate for a table, preferring ANALYZE stats.
pub(crate) fn base_row_estimate(
    schema: &Schema,
    table: &Table,
    params: &cost_params::CostModelParams,
) -> RowCountEstimate {
    match table {
        Table::BTree(btree) => {
            if let Some(stats) = schema.analyze_stats.table_stats(&btree.name) {
                if let Some(rows) = stats.row_count.or_else(|| {
                    stats
                        .index_stats
                        .values()
                        .find_map(|idx_stat| idx_stat.total_rows)
                }) {
                    return RowCountEstimate::AnalyzeStats(rows as f64);
                }
            }
            RowCountEstimate::hardcoded_fallback(params)
        }
        _ => RowCountEstimate::hardcoded_fallback(params),
    }
}

trait AccessPredicate<S: Copy> {
    fn outer_join_source(&self) -> Option<S>;
    fn is_consumed(&self) -> bool;
    fn consume(&mut self);
}

impl AccessPredicate<hir::SourceId> for HirWhereTerm {
    fn outer_join_source(&self) -> Option<hir::SourceId> {
        self.from_outer_join
    }

    fn is_consumed(&self) -> bool {
        self.consumed
    }

    fn consume(&mut self) {
        self.consumed = true;
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SelectedBtreeOperation<E, InSource> {
    Scan {
        iter_dir: IterationDirection,
        index: Option<Arc<Index>>,
    },
    Seek {
        index: Option<Arc<Index>>,
        seek_def: SeekDef<E>,
    },
    RowidEq {
        cmp_expr: E,
    },
    InSeek {
        index: Option<Arc<Index>>,
        source: InSource,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum HirInSeekSource {
    Values {
        values: Vec<hir::ExprId>,
        affinity: Affinity,
    },
    /// The IN subquery whose ephemeral index supplies the seek keys.
    Query(hir::SubqueryExpr),
}

pub(crate) type HirBtreeOperation = SelectedBtreeOperation<hir::ExprId, HirInSeekSource>;

#[derive(Clone, Debug)]
pub(crate) struct HirVirtualTableOperation {
    pub(crate) idx_num: i32,
    pub(crate) idx_str: Option<String>,
    pub(crate) arguments: Vec<hir::ExprId>,
    /// For the right table of a RIGHT or FULL JOIN: the read that finds its
    /// unmatched rows after the main loops, planned from the table-function
    /// arguments alone, as SQLite plans that read from a one-table FROM
    /// clause.
    pub(crate) unmatched_right_rows: Option<Box<HirVirtualTableOperation>>,
}

trait BtreeOperationBuilder<E, P> {
    type InSource;

    fn seek_def(
        &self,
        constraints: &[Constraint<E>],
        constraint_refs: &[RangeConstraintRef],
        iter_dir: IterationDirection,
        predicates: &[P],
    ) -> Result<SeekDef<E>>;

    fn rowid_eq(&self, constraint: &Constraint<E>, predicates: &[P]) -> Result<E>;

    fn in_seek_source(&self, predicate: &P, affinity: Affinity) -> Result<Self::InSource>;
}

struct HirBtreeOperationBuilder<'a> {
    exprs: &'a hir::ExprArena,
}

impl BtreeOperationBuilder<hir::ExprId, HirWhereTerm> for HirBtreeOperationBuilder<'_> {
    type InSource = HirInSeekSource;

    fn seek_def(
        &self,
        constraints: &[HirConstraint],
        constraint_refs: &[RangeConstraintRef],
        iter_dir: IterationDirection,
        predicates: &[HirWhereTerm],
    ) -> Result<HirSeekDef> {
        build_hir_seek_def_from_constraints(
            self.exprs,
            constraints,
            constraint_refs,
            iter_dir,
            predicates,
        )
    }

    fn rowid_eq(
        &self,
        constraint: &HirConstraint,
        predicates: &[HirWhereTerm],
    ) -> Result<hir::ExprId> {
        Ok(constraint
            .get_hir_constraining_expr(self.exprs, predicates)
            .1)
    }

    fn in_seek_source(
        &self,
        predicate: &HirWhereTerm,
        affinity: Affinity,
    ) -> Result<HirInSeekSource> {
        match &self.exprs[predicate.expr] {
            hir::Expr::InList { values, .. } => Ok(HirInSeekSource::Values {
                values: values.clone(),
                affinity,
            }),
            hir::Expr::Subquery(subquery @ hir::SubqueryExpr::In { .. }) => {
                Ok(HirInSeekSource::Query(subquery.clone()))
            }
            _ => Err(LimboError::InternalError(
                "HIR IN-seek term is not an IN list or IN subquery".into(),
            )),
        }
    }
}

fn selected_partial_index_terms<E>(
    constraints: &TableConstraints<E>,
    index: Option<&Arc<Index>>,
) -> SmallVec<[usize; 4]> {
    let Some(index) = index.filter(|index| index.where_clause.is_some()) else {
        return SmallVec::new();
    };
    constraints
        .candidates
        .iter()
        .find_map(|candidate| {
            candidate
                .index
                .as_ref()
                .is_some_and(|candidate_index| Arc::ptr_eq(candidate_index, index))
                .then(|| {
                    candidate
                        .partial_index
                        .as_ref()
                        .expect("selected partial index keeps its proof terms")
                        .predicate_terms
                        .clone()
                })
        })
        .expect("selected partial index remains in the source candidate set")
}

/// Mark predicates used only to prove that a partial index is usable.
fn mark_partial_index_terms_consumed<S: Copy + PartialEq>(
    terms: impl IntoIterator<Item = usize>,
    predicates: &mut [impl AccessPredicate<S>],
    outer_join_source: Option<S>,
) {
    for term in terms {
        let predicate = &mut predicates[term];
        if predicate.is_consumed() {
            continue;
        }
        if outer_join_source.is_some() && predicate.outer_join_source() != outer_join_source {
            continue;
        }
        predicate.consume();
    }
}

/// Mark WHERE clause terms as consumed when they are covered by a seek
/// (index seek, ephemeral auto-index seek, or rowid seek).
///
/// For an outer join, non-ON terms remain available to reject a NULL-extended
/// row. Cross-table constraints on a hash-build-only source are also deferred
/// to the probe side.
fn mark_seek_constraints_consumed<E, S: Copy>(
    constraints: &[Constraint<E>],
    constraint_refs: &[RangeConstraintRef],
    predicates: &mut [impl AccessPredicate<S>],
    outer_join_source: Option<S>,
    defer_cross_table: bool,
) {
    for cref in constraint_refs.iter() {
        for pos in [
            cref.eq.as_ref().map(|e| e.constraint_pos),
            cref.lower_bound,
            cref.upper_bound,
        ] {
            let Some(pos) = pos else { continue };
            let constraint = &constraints[pos];
            let predicate = &mut predicates[constraint.where_clause_pos.0];
            if predicate.is_consumed() {
                continue;
            }
            if outer_join_source.is_some() && predicate.outer_join_source().is_none() {
                continue;
            }
            if defer_cross_table && !constraint.lhs_mask.is_empty() {
                continue;
            }
            predicate.consume();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_selected_btree_access<I, H, E, J, P, B>(
    source: &PlannedSource<I, H, E, J>,
    constraints: &TableConstraints<E>,
    predicates: &mut [P],
    access_method: &mut AccessMethod,
    prior_sources: &TableMask,
    source_position: usize,
    outer_join_source: Option<I>,
    defer_cross_table_constraints: bool,
    builder: &B,
    adjust_index: impl FnOnce(&mut Option<Arc<Index>>),
) -> Result<Option<SelectedBtreeOperation<E, B::InSource>>>
where
    I: Copy + Display + PartialEq,
    P: AccessPredicate<I>,
    B: BtreeOperationBuilder<E, P>,
{
    let params = &mut access_method.params;
    match params {
        AccessMethodParams::BTreeTable {
            iter_dir,
            index,
            build_index,
            constraint_refs,
        } => {
            if *build_index {
                turso_assert!(index.is_none(), "a new temporary index must not exist yet");
                *constraint_refs = constraints::usable_constraints_for_lhs_mask(
                    &constraints.constraints,
                    &constraints.temporary_index_terms,
                    prior_sources,
                    source_position,
                )
                .into_vec();
                turso_assert!(
                    !constraint_refs.is_empty(),
                    "a temporary index must have a search key"
                );
                *index = Some(Arc::new(ephemeral_index_build(source, constraint_refs)?));
                *build_index = false;
            }

            adjust_index(index);
            mark_partial_index_terms_consumed(
                selected_partial_index_terms(constraints, index.as_ref()),
                predicates,
                outer_join_source,
            );

            if constraint_refs.is_empty() {
                return Ok(Some(SelectedBtreeOperation::Scan {
                    iter_dir: *iter_dir,
                    index: index.clone(),
                }));
            }

            mark_seek_constraints_consumed(
                &constraints.constraints,
                constraint_refs,
                predicates,
                outer_join_source,
                defer_cross_table_constraints,
            );
            if let Some(index) = index {
                return Ok(Some(SelectedBtreeOperation::Seek {
                    index: Some(index.clone()),
                    seek_def: builder.seek_def(
                        &constraints.constraints,
                        constraint_refs,
                        *iter_dir,
                        predicates,
                    )?,
                }));
            }

            turso_assert_eq!(
                constraint_refs.len(),
                1,
                "expected exactly one constraint for rowid seek",
                {"constraint_refs": format!("{constraint_refs:?}")}
            );
            if let Some(equality) = &constraint_refs[0].eq {
                return Ok(Some(SelectedBtreeOperation::RowidEq {
                    cmp_expr: builder.rowid_eq(
                        &constraints.constraints[equality.constraint_pos],
                        predicates,
                    )?,
                }));
            }
            Ok(Some(SelectedBtreeOperation::Seek {
                index: None,
                seek_def: builder.seek_def(
                    &constraints.constraints,
                    constraint_refs,
                    *iter_dir,
                    predicates,
                )?,
            }))
        }
        AccessMethodParams::InSeek {
            index,
            affinity,
            where_term_idx,
        } => {
            mark_partial_index_terms_consumed(
                selected_partial_index_terms(constraints, index.as_ref()),
                predicates,
                outer_join_source,
            );
            let source = builder.in_seek_source(&predicates[*where_term_idx], *affinity)?;
            predicates[*where_term_idx].consume();
            Ok(Some(SelectedBtreeOperation::InSeek {
                index: index.clone(),
                source,
            }))
        }
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
/// A table read through one seek per branch of an OR term, such as `a = 1
/// OR b = 2` with an index on each column. The branches' rowids are
/// collected first, so each row is read once.
#[derive(Clone, Debug)]
pub(crate) struct HirOrUnion {
    pub(crate) branches: Vec<HirBtreeOperation>,
    /// For each branch, its conditions that read only the sources before
    /// this one. Each is checked before the branch's seek, so an outer row
    /// the branch rejects seeks nothing.
    pub(crate) branch_filters: Vec<Vec<hir::ExprId>>,
    /// Position of the OR term in the block's predicates.
    pub(crate) term: usize,
    /// Every condition of every branch is applied by that branch's seek, so
    /// the rows the seeks find are exactly the rows the OR term accepts.
    /// Then the term is not checked again on each row, and a subquery in it
    /// is not run a second time.
    pub(crate) covers_term: bool,
}

/// OR-by-union: plan `source` as one seek per branch of one of its OR
/// terms, when every branch can seek an index or the rowid and the
/// seeks together cost less than scanning the table. Unless the seeks apply
/// every condition of the term ([`HirOrUnion::covers_term`]), the term is
/// left in WHERE and checked again on each row read.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_hir_or_union(
    document: &hir::HirDocument,
    layout: &crate::translate::plan::HirFromLayout<'_>,
    source: &HirPlannedSource,
    source_position: usize,
    predicates: &[HirWhereTerm],
    prior_sources: &TableMask,
    base_row_count: RowCountEstimate,
    schema: &Schema,
    params: &cost_params::CostModelParams,
    query_rows: &dyn Fn(hir::QueryId) -> Option<f64>,
) -> Result<Option<HirOrUnion>> {
    Ok(hir_or_union_with_cost(
        document,
        layout,
        source,
        source_position,
        predicates,
        prior_sources,
        base_row_count,
        schema,
        params,
        query_rows,
    )?
    .map(|(union, _, _)| union))
}

/// The OR union [plan_hir_or_union] would choose after `prior_sources`,
/// with its cost and rows for one outer row, so join ordering can weigh it
/// against a scan.
#[allow(clippy::too_many_arguments)]
pub(crate) fn hir_or_union_with_cost(
    document: &hir::HirDocument,
    layout: &crate::translate::plan::HirFromLayout<'_>,
    source: &HirPlannedSource,
    source_position: usize,
    predicates: &[HirWhereTerm],
    prior_sources: &TableMask,
    base_row_count: RowCountEstimate,
    schema: &Schema,
    params: &cost_params::CostModelParams,
    query_rows: &dyn Fn(hir::QueryId) -> Option<f64>,
) -> Result<Option<(HirOrUnion, Cost, f64)>> {
    // An outer join's right side has rows the seeks would not produce.
    if source.join_info.as_ref().is_some_and(|join| {
        matches!(
            join.kind,
            hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full
        )
    }) {
        return Ok(None);
    }
    let definition = document.source(source.internal_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR OR planning references missing source {}",
            source.internal_id
        ))
    })?;
    let mut allowed = crate::alloc::TryClone::try_clone(prior_sources)?;
    allowed.set(source_position)?;
    let scan_cost = cost::estimate_cost_for_scan_or_seek::<hir::ExprId>(
        None,
        &[],
        &[],
        1.0,
        base_row_count,
        false,
        params,
        None,
    );
    fn split(
        exprs: &hir::ExprArena,
        expr: hir::ExprId,
        operator: ast::Operator,
        parts: &mut Vec<hir::ExprId>,
    ) {
        match &exprs[expr] {
            hir::Expr::Binary {
                lhs,
                operator: found,
                rhs,
                ..
            } if *found == operator => {
                split(exprs, *lhs, operator, parts);
                split(exprs, *rhs, operator, parts);
            }
            _ => parts.push(expr),
        }
    }
    for (term_position, term) in predicates
        .iter()
        .enumerate()
        .filter(|(_, term)| !term.consumed && term.from_outer_join.is_none())
    {
        if !matches!(
            &document.exprs[term.expr],
            hir::Expr::Binary {
                operator: ast::Operator::Or,
                ..
            }
        ) {
            continue;
        }
        let mask = layout.table_mask(document, term.expr)?;
        if !mask.get(source_position) || !allowed.contains_all_set_bits_of(&mask) {
            continue;
        }
        let mut disjuncts = Vec::new();
        split(
            &document.exprs,
            term.expr,
            ast::Operator::Or,
            &mut disjuncts,
        );
        let mut branches = Vec::with_capacity(disjuncts.len());
        let mut branch_filters = Vec::with_capacity(disjuncts.len());
        let mut covers_term = true;
        let mut branch_cost = Cost(0.0);
        let mut branch_rows = Vec::with_capacity(disjuncts.len());
        for disjunct in disjuncts {
            let mut conjuncts = Vec::new();
            split(
                &document.exprs,
                disjunct,
                ast::Operator::And,
                &mut conjuncts,
            );
            let mut terms = conjuncts
                .into_iter()
                .map(|expr| HirWhereTerm {
                    expr,
                    from_outer_join: None,
                    consumed: false,
                })
                .collect::<Vec<_>>();
            let constraints = constraints::hir_constraints_for_source(
                document,
                layout,
                &terms,
                constraints::HirConstraintSource::BTree(source),
                schema,
                params,
                query_rows,
            )?;
            let mut access = access_method::choose_single_btree_access_method(
                &access_method::HirAccessSource::new(source, definition, &document.exprs),
                &constraints,
                prior_sources,
                source_position,
                None,
                &[],
                schema,
                &schema.analyze_stats,
                1.0,
                base_row_count,
                params,
            )?;
            let seeks = match &access.params {
                AccessMethodParams::BTreeTable {
                    build_index: false,
                    constraint_refs,
                    ..
                } => !constraint_refs.is_empty(),
                AccessMethodParams::InSeek { .. } => true,
                _ => false,
            };
            if !seeks {
                branches.clear();
                break;
            }
            // A branch only collects rowids, so it reads no table row: the
            // union looks each rowid up once below.
            if let AccessMethodParams::BTreeTable {
                index,
                constraint_refs,
                ..
            } = &access.params
            {
                let index_info = match index.as_ref() {
                    Some(index) => cost::IndexInfo {
                        unique: index.unique,
                        covering: true,
                        column_count: index.columns.len(),
                        rows_per_leaf_page: cost::rows_per_leaf_page_for_index(
                            index.columns.len(),
                            &source.table,
                            params.rows_per_table_page,
                        ),
                    },
                    None => cost::IndexInfo {
                        unique: true,
                        covering: true,
                        column_count: 1,
                        rows_per_leaf_page: params.rows_per_table_page,
                    },
                };
                let analyze_ctx = cost::AnalyzeCtx {
                    table_name: source.table.get_name(),
                    index: index.as_ref(),
                    stats: &schema.analyze_stats,
                };
                access.cost = cost::estimate_cost_for_scan_or_seek(
                    Some(index_info),
                    &constraints.constraints,
                    constraint_refs,
                    1.0,
                    base_row_count,
                    false,
                    params,
                    Some(&analyze_ctx),
                );
            }
            branch_cost = branch_cost + access.cost;
            branch_rows.push(access.estimated_rows_per_outer_row);
            branches.push(apply_hir_selected_btree_access(
                &document.exprs,
                source,
                &constraints,
                &mut terms,
                &mut access,
                prior_sources,
                source_position,
            )?);
            // A branch whose leftover condition runs a subquery keeps the
            // table scanned.
            let runs_subquery = |term: &HirWhereTerm| {
                let mut runs = false;
                document.exprs.at(term.expr).for_each(&mut |expression| {
                    runs |= matches!(expression.node(), hir::Expr::Subquery(_));
                });
                runs
            };
            if terms
                .iter()
                .any(|term| !term.consumed && runs_subquery(term))
            {
                branches.clear();
                break;
            }
            // A leftover condition over the sources before this one is
            // checked before the branch's seek, as the legacy planner does.
            let mut filters = Vec::new();
            for term in &mut terms {
                if term.consumed {
                    continue;
                }
                let mask = layout.table_mask(document, term.expr)?;
                if prior_sources.contains_all_set_bits_of(&mask) {
                    filters.push(term.expr);
                    term.consume();
                }
            }
            branch_filters.push(filters);
            covers_term &= terms.iter().all(|term| term.consumed);
        }
        if branches.is_empty() {
            continue;
        }
        // The branches' rows overlap as independent conditions would:
        // P(A OR B) = 1 - (1 - P(A)) * (1 - P(B)).
        let base_rows = *base_row_count;
        let mut unique_row_ratio = 1.0_f64;
        for rows in &branch_rows {
            unique_row_ratio *= 1.0 - (rows / base_rows).min(1.0);
        }
        let union_rows = base_rows * (1.0 - unique_row_ratio);
        // Every rowid is added to and read from the row set, then the
        // rows are read from the table in rowid order.
        let rows_before_dedup: f64 = branch_rows.iter().sum();
        let row_set_cost = rows_before_dedup * params.cpu_cost_per_row * 2.0;
        let table_pages = (base_rows / params.rows_per_table_page).max(1.0);
        let table_fetch_cost = union_rows / base_rows.max(1.0) * table_pages;
        let union_cost = branch_cost + Cost(row_set_cost + table_fetch_cost);
        if union_cost.0 < scan_cost.0 {
            let union = HirOrUnion {
                branches,
                branch_filters,
                term: term_position,
                covers_term,
            };
            return Ok(Some((union, union_cost, union_rows)));
        }
    }
    Ok(None)
}

pub(crate) fn apply_hir_selected_btree_access(
    exprs: &hir::ExprArena,
    source: &HirPlannedSource,
    constraints: &constraints::HirTableConstraints,
    predicates: &mut [HirWhereTerm],
    access_method: &mut AccessMethod,
    prior_sources: &TableMask,
    source_position: usize,
) -> Result<HirBtreeOperation> {
    let outer_join_source = source.join_info.as_ref().and_then(|join| {
        matches!(
            join.kind,
            hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full
        )
        .then_some(source.internal_id)
    });
    apply_selected_btree_access(
        source,
        constraints,
        predicates,
        access_method,
        prior_sources,
        source_position,
        outer_join_source,
        false,
        &HirBtreeOperationBuilder { exprs },
        |_| {},
    )?
    .ok_or_else(|| {
        LimboError::InternalError("HIR B-tree plan selected a non-B-tree access method".into())
    })
}

/// The seek of a FROM subquery or CTE that
/// [`access_method::find_best_hir_access_method_for_subquery`] chose to read
/// through a temporary index over its stored rows: the seek's constraints
/// are consumed and its key is built like any index seek. `None` when the
/// subquery is scanned.
pub(crate) fn apply_hir_materialized_subquery_access(
    exprs: &hir::ExprArena,
    constraints: &constraints::HirTableConstraints,
    predicates: &mut [HirWhereTerm],
    access_method: &AccessMethod,
    outer_join_source: Option<hir::SourceId>,
) -> Result<Option<HirBtreeOperation>> {
    match &access_method.params {
        AccessMethodParams::Subquery { .. } => Ok(None),
        AccessMethodParams::MaterializedSubquery {
            index,
            constraint_refs,
            iter_dir,
        } => {
            mark_seek_constraints_consumed(
                &constraints.constraints,
                constraint_refs,
                predicates,
                outer_join_source,
                false,
            );
            Ok(Some(SelectedBtreeOperation::Seek {
                index: Some(index.clone()),
                seek_def: HirBtreeOperationBuilder { exprs }.seek_def(
                    &constraints.constraints,
                    constraint_refs,
                    *iter_dir,
                    predicates,
                )?,
            }))
        }
        _ => Err(LimboError::InternalError(
            "HIR subquery source selected a table access method".into(),
        )),
    }
}

pub(crate) fn apply_hir_selected_virtual_access(
    exprs: &hir::ExprArena,
    constraints: &constraints::HirTableConstraints,
    predicates: &mut [HirWhereTerm],
    access_method: &AccessMethod,
) -> Result<HirVirtualTableOperation> {
    let AccessMethodParams::VirtualTable {
        idx_num,
        idx_str,
        constraints: virtual_constraints,
        constraint_usages,
    } = &access_method.params
    else {
        return Err(LimboError::InternalError(
            "HIR virtual table selected a non-virtual access method".into(),
        ));
    };
    if constraint_usages.len() != virtual_constraints.len() {
        return Err(LimboError::ExtensionError(format!(
            "Constraint usage count mismatch (expected {}, got {})",
            virtual_constraints.len(),
            constraint_usages.len()
        )));
    }

    let mut arguments = vec![None; constraint_usages.len()];
    let mut argument_count = 0;
    for (virtual_constraint, usage) in virtual_constraints.iter().zip(constraint_usages) {
        let Some(index) = usage.argv_index else {
            continue;
        };
        if index < 1 || index as usize > constraint_usages.len() {
            return Err(LimboError::ExtensionError(format!(
                "argv_index {index} is out of valid range [1..{}]",
                constraint_usages.len()
            )));
        }
        let argument = &mut arguments[index as usize - 1];
        if argument.is_some() {
            return Err(LimboError::ExtensionError(format!(
                "duplicate argv_index {index}"
            )));
        }
        let constraint = &constraints.constraints[virtual_constraint.index];
        if usage.omit {
            predicates[constraint.where_clause_pos.0].consumed = true;
        }
        *argument = Some(constraint.get_hir_constraining_expr(exprs, predicates).1);
        argument_count += 1;
    }
    let arguments = arguments
        .into_iter()
        .take(argument_count)
        .enumerate()
        .map(|(index, argument)| {
            argument.ok_or_else(|| {
                LimboError::ExtensionError(format!(
                    "argv_index values must form contiguous sequence starting from 1, missing index {}",
                    index + 1
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(HirVirtualTableOperation {
        idx_num: *idx_num,
        idx_str: idx_str.clone(),
        arguments,
        unmatched_right_rows: None,
    })
}

pub(crate) fn ephemeral_index_build<I, H, E, J>(
    source: &PlannedSource<I, H, E, J>,
    constraint_refs: &[RangeConstraintRef],
) -> Result<Index>
where
    I: Display,
{
    let mut ephemeral_columns: crate::alloc::Vec<IndexColumn> = source
        .table
        .columns()
        .iter()
        .enumerate()
        // Only copy columns that the query reads.
        .filter(|(index, _)| source.col_used_mask.get(*index))
        .map(|(i, c)| {
            let expr = match c.generated_type() {
                GeneratedType::Virtual { .. } => c.generated_expr().cloned(),
                GeneratedType::NotGenerated => None,
            };
            IndexColumn {
                name: c.name.clone().unwrap(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: i,
                collation: c.collation_opt(),
                default: c.default.clone(),
                expr: expr.map(Box::new),
            }
        })
        .try_collect()?;
    // sort so that constraints first, then rest in whatever order they were in in the table
    ephemeral_columns.sort_by(|a, b| {
        let a_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == Some(a.pos_in_table));
        let b_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == Some(b.pos_in_table));
        match (a_constraint, b_constraint) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a_idx, _)), Some((b_idx, _))) => a_idx.cmp(&b_idx),
            (None, None) => Ordering::Equal,
        }
    });
    let ephemeral_index = Index {
        name: format!(
            "ephemeral_{}_{}",
            source.table.get_name(),
            source.internal_id
        ),
        columns: ephemeral_columns,
        unique: false,
        ephemeral: true,
        table_name: source.table.get_name().to_string(),
        root_page: 0,
        where_clause: None,
        has_rowid: source.table.btree().is_some_and(|btree| btree.has_rowid),
        index_method: None,
        on_conflict: None,
        constraint_name: None,
    };

    Ok(ephemeral_index)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn build_hir_seek_def_from_constraints(
    exprs: &hir::ExprArena,
    constraints: &[HirConstraint],
    constraint_refs: &[RangeConstraintRef],
    iter_dir: IterationDirection,
    where_clause: &[HirWhereTerm],
) -> Result<HirSeekDef> {
    let key = constraint_refs
        .iter()
        .map(|constraint| constraint.as_hir_seek_range_constraint(exprs, constraints, where_clause))
        .collect();
    build_seek_def(iter_dir, key)
}

/// Build a [SeekDef] for a given [SeekRangeConstraint] and [IterationDirection].
/// To be usable as a seek key, all but potentially the last term must be equalities.
/// The last term can be a nonequality (range with potentially one unbounded range).
///
/// There are two parts to the seek definition:
/// 1. start [SeekKey], which specifies the key that we will use to seek to the first row that matches the index key.
/// 2. end [SeekKey], which specifies the key that we will use to terminate the index scan that follows the seek.
///
/// There are some nuances to how, and which parts of, the index key can be used in the start and end [SeekKey]s,
/// depending on the operator and iteration order. This function explains those nuances inline when dealing with
/// each case.
///
/// But to illustrate the general idea, consider the following examples:
///
/// 1. For example, having two conditions like (x>10 AND y>20) cannot be used as a valid [SeekKey] GT(x:10, y:20)
///    because the first row greater than (x:10, y:20) might be (x:11, y:19), which does not satisfy the where clause.
///    In this case, only GT(x:10) must be used as the [SeekKey], and rows with y <= 20 must be filtered as a regular condition expression for each value of x.
///
/// 2. In contrast, having (x=10 AND y>20) forms a valid index key GT(x:10, y:20) because after the seek, we can simply terminate as soon as x > 10,
///    i.e. use GT(x:10, y:20) as the start [SeekKey] and GT(x:10) as the end.
///
/// The preceding examples are for an ascending index. The logic is similar for descending indexes, but an important distinction is that
/// since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
/// So when you see e.g. a SeekOp::GT below for a descending index, it actually means that we are seeking the first row where the index key is LESS than the seek key.
///
fn build_seek_def<E>(
    iter_dir: IterationDirection,
    mut key: Vec<SeekRangeConstraint<E>>,
) -> Result<SeekDef<E>> {
    if key.is_empty() {
        // Zero-prefix seeks are used for extremum scans over an already ordered
        // source: start at one end of the cursor and stop after the first
        // qualifying row.
        let (start_op, end_op) = match iter_dir {
            IterationDirection::Forwards => (SeekOp::GE { eq_only: true }, SeekOp::GT),
            IterationDirection::Backwards => (SeekOp::LE { eq_only: true }, SeekOp::LT),
        };
        return Ok(SeekDef {
            prefix: Vec::new(),
            iter_dir,
            start: SeekKey {
                last_component: SeekKeyComponent::None,
                op: start_op,
                affinity: Affinity::Blob,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::None,
                op: end_op,
                affinity: Affinity::Blob,
            },
        });
    }
    let last = key.last().unwrap();

    // if we searching for exact key - emit definition immediately with prefix as a full key
    if last.eq.is_some() {
        let (start_op, end_op) = match iter_dir {
            IterationDirection::Forwards => (SeekOp::GE { eq_only: true }, SeekOp::GT),
            IterationDirection::Backwards => (SeekOp::LE { eq_only: true }, SeekOp::LT),
        };
        return Ok(SeekDef {
            prefix: key,
            iter_dir,
            start: SeekKey {
                last_component: SeekKeyComponent::None,
                op: start_op,
                affinity: Affinity::Blob,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::None,
                op: end_op,
                affinity: Affinity::Blob,
            },
        });
    }
    turso_assert!(last.lower_bound.is_some() || last.upper_bound.is_some());

    // pop last key as we will do some form of range search
    let last = key.pop().unwrap();
    let stored_nulls = last.nulls_order;
    // after that all key components must be equality constraints
    turso_debug_assert!(key.iter().all(|k| k.eq.is_some()));

    let has_prefix = !key.is_empty();
    let apply_null_boundaries = |start: &mut SeekKey<E>, end: &mut SeekKey<E>| {
        // Sometimes we must add an extra NULL to the key on purpose.
        // We do this so scans over composite indexes match SQLite exactly.
        //
        // A range with only one bound leaves the other side of the scan
        // without a key: the scan just runs until the prefix stops matching.
        // That is a problem when the NULLs of the range column live on that
        // side, because the scan would walk into them, and a comparison like
        // c2<=999 must not return NULL rows. So we add the NULL key on that
        // side: as a start key it makes the scan begin right after the NULLs,
        // as an end key it makes the scan stop right before them. Which side
        // the NULLs live on depends on the column: before all values with
        // NULLS FIRST, after all values with NULLS LAST.
        if !has_prefix {
            return;
        }
        // 1) Choose a better starting point.
        //
        // Example:
        //   INDEX(c1, c2 ASC)
        //   WHERE c1='a' AND c2<=999
        //
        // If we start from key [c1='a'], we hit rows where c2 is NULL first.
        // For this case we want to start right after that NULL boundary.
        // So we:
        // - use start key [c1='a', NULL]
        // - change start op from GE to GT
        // - for backward scans in the symmetric shape, change LE to LT
        if matches!(start.last_component, SeekKeyComponent::None) {
            match (iter_dir, stored_nulls) {
                (IterationDirection::Forwards, ast::NullsOrder::First) => {
                    start.last_component = SeekKeyComponent::Null;
                    start.op = SeekOp::GT;
                }
                (IterationDirection::Backwards, ast::NullsOrder::Last) => {
                    start.last_component = SeekKeyComponent::Null;
                    start.op = SeekOp::LT;
                }
                _ => {}
            }
        }
        // 2) Choose a better stopping point.
        //
        // Example:
        //   INDEX(c1, c2 DESC)
        //   WHERE c1='a' AND c2<=999
        //
        // The stop check must also respect the NULL boundary for c2.
        // So we:
        // - use stop key [c1='a', NULL]
        // - change end op from GT to GE
        // - for backward scans, change LT to LE
        if matches!(end.last_component, SeekKeyComponent::None) {
            match (iter_dir, stored_nulls) {
                (IterationDirection::Forwards, ast::NullsOrder::Last) => {
                    end.last_component = SeekKeyComponent::Null;
                    end.op = SeekOp::GE { eq_only: false };
                }
                (IterationDirection::Backwards, ast::NullsOrder::First) => {
                    end.last_component = SeekKeyComponent::Null;
                    end.op = SeekOp::LE { eq_only: false };
                }
                _ => {}
            }
        }
    };

    // For the commented examples below, keep in mind that since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
    // Also keep in mind that index keys are compared based on the number of columns given, so for example:
    // - if key is GT(x:10), then (x=10, y=usize::MAX) is not GT because only X is compared. (x=11, y=<any>) is GT.
    // - if key is GT(x:10, y:20), then (x=10, y=21) is GT because both X and Y are compared.
    // - if key is GT(x:10, y:NULL), then (x=10, y=0) is GT because NULL is always LT in index key comparisons.
    Ok(match iter_dir {
        IterationDirection::Forwards => {
            let (mut start, mut end) = match last.sort_order {
                SortOrder::Asc => {
                    let start = match last.lower_bound {
                        // Forwards, Asc, GT: (x=10 AND y>20)
                        // Start key: start from the first GT(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, GE: (x=10 AND y>=20)
                        // Start key: start from the first GE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Forwards, Asc, LT, (x=10 AND y<30)
                        // End key: end at first GE(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, LE, (x=10 AND y<=30)
                        // End key: end at first GT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y>20)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.upper_bound {
                        // Forwards, Desc, LT: (x=10 AND y<30)
                        // Start key: start from the first GT(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Desc, LE: (x=10 AND y<=30)
                        // Start key: start from the first GE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Desc, None: (x=10 AND y>20)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.lower_bound {
                        // Forwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first GE(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, GE, (x=10 AND y>=20)
                        // End key: end at first GT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
            };
            apply_null_boundaries(&mut start, &mut end);
            SeekDef {
                prefix: key,
                iter_dir,
                start,
                end,
            }
        }
        IterationDirection::Backwards => {
            let (mut start, mut end) = match last.sort_order {
                SortOrder::Asc => {
                    let start = match last.upper_bound {
                        // Backwards, Asc, LT: (x=10 AND y<30)
                        // Start key: start from the first LT(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Asc, LT: (x=10 AND y<=30)
                        // Start key: start from the first LE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Asc, None: (x=10 AND y>20)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op)
                        }
                    };
                    let end = match last.lower_bound {
                        // Backwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first LE(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Asc, GT, (x=10 AND y>=20)
                        // End key: end at first LT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.lower_bound {
                        // Backwards, Desc, LT: (x=10 AND y>20)
                        // Start key: start from the first LT(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Desc, LE: (x=10 AND y>=20)
                        // Start key: start from the first LE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Desc, LE: (x=10 AND y<30)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Backwards, Desc, LT, (x=10 AND y<30)
                        // End key: end at first LE(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Desc, LT, (x=10 AND y<=30)
                        // End key: end at first LT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Desc, LT, (x=10 AND y>20)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
            };
            apply_null_boundaries(&mut start, &mut end);
            SeekDef {
                prefix: key,
                iter_dir,
                start,
                end,
            }
        }
    })
}
