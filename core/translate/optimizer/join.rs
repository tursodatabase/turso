use crate::{alloc::TryReserveError, turso_assert};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use smallvec::SmallVec;

use turso_parser::ast::Operator;

use super::{
    access_method::{
        add_where_cost, choose_single_btree_access_method, find_best_hir_access_method_for_vtab,
        AccessMethod, AccessSource, HirAccessSource,
    },
    constraints::{usable_constraints_for_lhs_mask, HirTableConstraints, TableConstraints},
    cost_params::CostModelParams,
    order::HirOrderTarget,
};
use crate::alloc::{TryClone, TursoIteratorExt};
use crate::translate::plan::BitSet;
use crate::{
    schema::{Schema, Table},
    stats::AnalyzeStats,
    translate::{
        optimizer::{
            access_method::{estimate_hash_join_cost, AccessMethodParams},
            cost::{
                estimate_cost_for_scan_or_seek, estimate_rows_per_seek, hir_where_expr_steps,
                index_access_is_unique_point_lookup, rows_per_leaf_page_for_index, AnalyzeCtx,
                Cost, IndexInfo, RowCountEstimate,
            },
        },
        plan::{
            HashJoinKey, HashJoinType, HirFromGroupBoundary, HirFromLayout, HirPlanSource,
            HirWhereTerm, PredicateTerm,
        },
        planner::{table_mask_from_hir_scope, HirFromScope, TableMask},
        semantic::hir::{self, HirDocument},
    },
    LimboError, Result,
};

// Upper bound on rowids to materialize for a hash build input.
// This is a safety limit, not a cost tuning parameter.
const MAX_MATERIALIZED_BUILD_ROWS: f64 = 200_000.0;

/// Estimate how much the remaining `WHERE` terms cut the row count.
///
/// Some callers skip a term because they need the count before that term runs.
#[allow(clippy::too_many_arguments)]
fn constraint_output_multipliers(
    rhs_constraints: &HirTableConstraints,
    lhs_mask: &TableMask,
    rhs_self_mask: TableMask,
    consumed_where_terms: &BitSet<usize>,
    skipped_where_terms: &BitSet<usize>,
    where_clause: &[HirWhereTerm],
    params: &CostModelParams,
) -> f64 {
    constraint_output_multipliers_for(
        rhs_constraints,
        lhs_mask,
        rhs_self_mask,
        consumed_where_terms,
        skipped_where_terms,
        where_clause,
        params,
        |_| true,
    )
}

/// [constraint_output_multipliers] over the constraints `include` keeps.
#[allow(clippy::too_many_arguments)]
fn constraint_output_multipliers_for(
    rhs_constraints: &HirTableConstraints,
    lhs_mask: &TableMask,
    rhs_self_mask: TableMask,
    consumed_where_terms: &BitSet<usize>,
    skipped_where_terms: &BitSet<usize>,
    where_clause: &[HirWhereTerm],
    params: &CostModelParams,
    include: impl Fn(&super::constraints::HirConstraint) -> bool,
) -> f64 {
    let mut multiplier = 1.0;
    let mut bounds: SmallVec<[(Option<usize>, bool, bool); 4]> = SmallVec::new();

    let record_bound = |bounds: &mut SmallVec<[(Option<usize>, bool, bool); 4]>,
                        dominated_col: Option<usize>,
                        is_lower: bool,
                        is_upper: bool| {
        if !(is_lower || is_upper) {
            return;
        }
        if let Some(entry) = bounds.iter_mut().find(|(col, _, _)| *col == dominated_col) {
            entry.1 |= is_lower;
            entry.2 |= is_upper;
        } else {
            bounds.push((dominated_col, is_lower, is_upper));
        }
    };

    for constraint in rhs_constraints.constraints.iter().filter(|constraint| {
        (lhs_mask.contains_all_set_bits_of(&constraint.lhs_mask)
            || constraint.lhs_mask == rhs_self_mask
            || constraint.lhs_mask.is_empty())
            && !consumed_where_terms.get(constraint.where_clause_pos.0)
            && !skipped_where_terms.get(constraint.where_clause_pos.0)
            && !where_clause[constraint.where_clause_pos.0].consumed
            && include(constraint)
    }) {
        multiplier *= constraint.selectivity;

        let dominated_col = constraint.table_col_pos;
        let is_lower = matches!(
            constraint.operator.as_ast_operator(),
            Some(Operator::Greater | Operator::GreaterEquals)
        );
        let is_upper = matches!(
            constraint.operator.as_ast_operator(),
            Some(Operator::Less | Operator::LessEquals)
        );
        record_bound(&mut bounds, dominated_col, is_lower, is_upper);
    }

    for (_, has_lower, has_upper) in &bounds {
        if *has_lower && *has_upper {
            multiplier *= params.closed_range_selectivity_factor;
        }
    }

    multiplier
}

/// Return the row count after one table and its ready filters.
#[allow(clippy::too_many_arguments)]
fn rows_after_join(
    input_cardinality: f64,
    method: &AccessMethod,
    rhs_constraints: &HirTableConstraints,
    lhs_mask: &TableMask,
    rhs_mask: TableMask,
    sources: &[HirPlanSource],
    rhs_position: usize,
    where_clause: &[HirWhereTerm],
    params: &CostModelParams,
) -> f64 {
    use crate::translate::semantic::hir::JoinKind;
    let rhs = &sources[rhs_position];
    let join_kind = rhs.join_info().map(|join| join.kind);
    if matches!(join_kind, Some(JoinKind::Semi | JoinKind::Anti)) {
        return input_cardinality;
    }
    if !matches!(
        join_kind,
        Some(JoinKind::Left | JoinKind::Right | JoinKind::Full)
    ) {
        let remaining_filter_selectivity = constraint_output_multipliers(
            rhs_constraints,
            lhs_mask,
            rhs_mask,
            &method.consumed_where_terms,
            &Default::default(),
            where_clause,
            params,
        );
        return input_cardinality
            * method.estimated_rows_per_outer_row
            * remaining_filter_selectivity;
    }

    let is_on_term = |constraint: &super::constraints::HirConstraint| {
        where_clause[constraint.where_clause_pos.0].from_outer_join == Some(rhs.source())
    };
    let on_selectivity = constraint_output_multipliers_for(
        rhs_constraints,
        lhs_mask,
        rhs_mask.clone(),
        &method.consumed_where_terms,
        &Default::default(),
        where_clause,
        params,
        is_on_term,
    );
    let matching_rows_per_input = method.estimated_rows_per_outer_row * on_selectivity;
    // Statistics give an average match count, but not its distribution.
    // A Poisson model estimates the chance that an input row has no match.
    let unmatched_probability = (-matching_rows_per_input).exp();
    let outer_join_rows_per_input = matching_rows_per_input + unmatched_probability;

    // `column IS NULL` in WHERE on a column of the null-extended side that
    // is never NULL in a stored row keeps only the rows the join added.
    let selects_unmatched_rows = |constraint: &super::constraints::HirConstraint| {
        if is_on_term(constraint)
            || !matches!(constraint.operator.as_ast_operator(), Some(Operator::Is))
        {
            return false;
        }
        let (_, constraining_expr, _) = constraint.get_hir_constraining_expr(where_clause);
        matches!(
            constraining_expr,
            hir::Expr::Literal(turso_parser::ast::Literal::Null)
        ) && constraint.table_col_pos.is_some_and(|column_pos| {
            rhs.btree().is_some_and(|source| {
                source
                    .table
                    .columns()
                    .get(column_pos)
                    .is_some_and(|column| column.is_rowid_alias() || column.notnull())
            })
        })
    };
    let selects_only_unmatched_rows = rhs_constraints.constraints.iter().any(|constraint| {
        !method
            .consumed_where_terms
            .get(constraint.where_clause_pos.0)
            && selects_unmatched_rows(constraint)
    });
    let where_selectivity = constraint_output_multipliers_for(
        rhs_constraints,
        lhs_mask,
        rhs_mask,
        &method.consumed_where_terms,
        &Default::default(),
        where_clause,
        params,
        |constraint| !is_on_term(constraint) && !selects_unmatched_rows(constraint),
    );
    let rows_per_input = if selects_only_unmatched_rows {
        unmatched_probability
    } else {
        outer_join_rows_per_input
    };
    input_cardinality * rows_per_input * where_selectivity
}

/// How many times each correlated subquery of a HIR block runs under a
/// join plan, counted from the rows that reach the join
/// step where the last block source it reads is joined. Each subquery comes
/// with the block sources it reads, as positions, the position of the
/// first predicate it appears in (that predicate and the ones after it run
/// after the subquery, so they cannot reduce its calls), and whether its
/// WHERE term may wait for a later loop. Such a term runs after the later
/// loop that fewest rows reach, which the result names by its source
/// position; a semi-join or anti-join loop passes rows on one at a time,
/// so it is not chosen.
#[allow(clippy::too_many_arguments)]
pub(crate) fn hir_subquery_calls_for_plan(
    plan: &JoinN,
    access_methods: &[AccessMethod],
    constraints: &[HirTableConstraints],
    sources: &[HirPlanSource],
    subqueries: &[(hir::QueryId, TableMask, Option<usize>, bool)],
    where_clause: &[HirWhereTerm],
    initial_input_cardinality: f64,
    params: &CostModelParams,
) -> Result<Vec<(hir::QueryId, f64, Option<usize>)>> {
    let predicate_count = where_clause.len();
    let mut calls = Vec::new();
    if subqueries.is_empty() {
        return Ok(calls);
    }
    let mut prior_sources = TableMask::default();
    let mut input_cardinality = initial_input_cardinality;
    for (step, (source_position, access_method_index)) in plan.data.iter().enumerate() {
        let method = &access_methods[*access_method_index];
        let mut source_mask = TableMask::default();
        source_mask.set(*source_position)?;
        let mut current_sources = prior_sources.clone();
        current_sources.set(*source_position)?;
        let rows_before_filters = input_cardinality * method.estimated_rows_per_outer_row;
        for (query, required, first_predicate, deferrable) in subqueries {
            if required.is_empty()
                || !current_sources.contains_all_set_bits_of(required)
                || prior_sources.contains_all_set_bits_of(required)
            {
                continue;
            }
            let skipped: BitSet<usize> = match first_predicate {
                Some(first) => (*first..predicate_count).try_collect()?,
                None => Default::default(),
            };
            let multiplier = constraint_output_multipliers(
                &constraints[*source_position],
                &prior_sources,
                source_mask.clone(),
                &method.consumed_where_terms,
                &skipped,
                where_clause,
                params,
            );
            let mut subquery_calls = (rows_before_filters * multiplier).max(1.0);
            let mut runs_after = None;
            if *deferrable {
                for (later_step, rows) in
                    plan.prefix_cardinalities.iter().enumerate().skip(step + 1)
                {
                    let later_position = plan.data[later_step].0;
                    if rows < &subquery_calls && !hir_is_semi_or_anti(sources, later_position) {
                        subquery_calls = rows.max(1.0);
                        runs_after = Some(later_position);
                    }
                }
            }
            calls.push((*query, subquery_calls, runs_after));
        }
        input_cardinality = rows_after_join(
            input_cardinality,
            method,
            &constraints[*source_position],
            &prior_sources,
            source_mask,
            sources,
            *source_position,
            where_clause,
            params,
        );
        prior_sources.set(*source_position)?;
    }
    Ok(calls)
}

/// Whether the source at `position` is the right side of a semi-join or
/// anti-join, which passes on each row before it at most once.
fn hir_is_semi_or_anti(sources: &[HirPlanSource], position: usize) -> bool {
    sources[position].join_info().is_some_and(|join| {
        matches!(
            join.kind,
            crate::translate::semantic::hir::JoinKind::Semi
                | crate::translate::semantic::hir::JoinKind::Anti
        )
    })
}

/// Represents an n-ary join, anywhere from 1 table to N tables.
#[derive(Debug, Clone)]
pub struct JoinN {
    /// Tuple: (table_number, access_method_index)
    pub data: Vec<(usize, usize)>,
    /// The estimated number of rows returned by joining these n tables together.
    pub output_cardinality: f64,
    /// The estimated number of rows after each table of `data`, in order.
    pub prefix_cardinalities: Vec<f64>,
    /// Estimated execution cost of this N-ary join.
    pub cost: Cost,
}

struct PredicateWorkInfo {
    table_mask: TableMask,
    extra_steps: usize,
}

impl JoinN {
    #[cfg(test)]
    pub fn table_numbers(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.data.iter().map(|(table_number, _)| *table_number)
    }

    #[cfg(test)]
    pub fn best_access_methods(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.data
            .iter()
            .map(|(_, access_method_index)| *access_method_index)
    }
}

/// Returns true when build-side constraints reference prior tables in ways that
/// are not already consumed by hash-join keys.
fn build_has_uncovered_prior_constraints<E>(
    build_constraints: &TableConstraints<E>,
    join_keys: &[HashJoinKey],
    prior_mask: &TableMask,
    prior_hash_build_mask: &TableMask,
) -> bool {
    let mut join_key_indices = HashSet::default();
    for join_key in join_keys {
        join_key_indices.insert(join_key.where_clause_idx);
    }

    build_constraints.constraints.iter().any(|constraint| {
        if !constraint.lhs_mask.intersects(prior_mask) {
            return false;
        }
        if join_key_indices.contains(&constraint.where_clause_pos.0) {
            return false;
        }
        if constraint.operator != Operator::Equals.into() {
            return true;
        }
        if !constraint.lhs_mask.intersects(prior_hash_build_mask) {
            return true;
        }
        for table_idx in prior_mask.iter() {
            if constraint.lhs_mask.get(table_idx) && !prior_hash_build_mask.get(table_idx) {
                return true;
            }
        }
        false
    })
}

/// Estimates selectivity from prior equality constraints on the build side.
fn build_prior_constraint_selectivity<E>(
    build_constraints: &TableConstraints<E>,
    prior_mask: &TableMask,
) -> f64 {
    let mut selectivity = 1.0;
    let mut saw_constraint = false;
    for constraint in build_constraints.constraints.iter() {
        if constraint.operator == Operator::Equals.into()
            && constraint.lhs_mask.intersects(prior_mask)
        {
            tracing::debug!(
                where_clause_pos = ?constraint.where_clause_pos,
                lhs_mask = ?constraint.lhs_mask,
                prior_mask = ?prior_mask,
                selectivity = constraint.selectivity,
                "prior constraint selectivity contributor"
            );
            selectivity *= constraint.selectivity;
            saw_constraint = true;
        }
    }
    if !saw_constraint {
        return 1.0;
    }
    selectivity.clamp(0.0, 1.0)
}

/// Estimates selectivity from build-side constraints that reference only the build table.
fn build_self_constraint_selectivity<E>(
    build_constraints: &TableConstraints<E>,
    build_table_idx: usize,
) -> f64 {
    let build_only_mask: TableMask = [build_table_idx]
        .into_iter()
        .try_collect()
        .expect("does not heap allocate with just a single value");
    let mut selectivity = 1.0;
    let mut saw_constraint = false;
    for constraint in build_constraints.constraints.iter() {
        if !build_only_mask.contains_all_set_bits_of(&constraint.lhs_mask) {
            continue;
        }
        selectivity *= constraint.selectivity;
        saw_constraint = true;
    }
    if !saw_constraint {
        return 1.0;
    }
    selectivity.clamp(0.0, 1.0)
}

/// The result of a join order search.
#[derive(Debug)]
pub struct BestJoinOrderResult {
    /// The best plan overall.
    pub best_plan: JoinN,
    /// The best plan for the given order target, if it isn't the overall best.
    pub best_ordered_plan: Option<JoinN>,
}

#[derive(Clone, Copy)]
struct JoinOrderingFacts {
    constrained: bool,
    /// The join keeps its right side's unmatched rows: RIGHT or FULL.
    full_outer: bool,
}

/// Join-order dependencies shared by parser-expression and HIR planning.
pub(crate) struct JoinOrderingRestrictions {
    required_lhs_by_table: Option<Vec<TableMask>>,
}

impl JoinOrderingRestrictions {
    /// Whether every source in `order` comes after the sources it must
    /// follow.
    #[cfg(test)]
    fn allows_order(&self, order: &[usize]) -> Result<bool> {
        let mut prefix = TableMask::default();
        for &rhs in order {
            if let Some(required) = self.required_lhs(rhs) {
                if !prefix.contains_all_set_bits_of(required) {
                    return Ok(false);
                }
            }
            prefix.set(rhs)?;
        }
        Ok(true)
    }

    fn required_lhs(&self, table: usize) -> Option<&TableMask> {
        self.required_lhs_by_table
            .as_ref()
            .map(|required| &required[table])
            .filter(|required| !required.is_empty())
    }
}

fn join_ordering_restrictions<T>(
    sources: &[T],
    facts: impl Fn(&T) -> JoinOrderingFacts,
) -> Result<JoinOrderingRestrictions> {
    let source_count = sources.len();
    let ordering_constrained_count = sources
        .iter()
        .filter(|source| facts(source).constrained)
        .count();
    let has_full_outer = sources.iter().any(|source| facts(source).full_outer);
    if ordering_constrained_count == 0 && !has_full_outer {
        return Ok(JoinOrderingRestrictions {
            required_lhs_by_table: None,
        });
    }

    let mut required_lhs_by_table = vec![TableMask::default(); source_count];
    // Every ordering-constrained join requires its RHS source to remain after
    // every source originally on its left.
    for (left, _) in sources.iter().enumerate() {
        for (right, source) in sources.iter().enumerate().skip(left + 1) {
            if facts(source).constrained {
                required_lhs_by_table[right].set(left)?;
            }
        }
    }

    // RIGHT and FULL OUTER are barriers in both directions. Later sources
    // cannot move before them: `(t1 INNER t3) FULL OUTER t2` is not
    // equivalent to the requested `(t1 FULL OUTER t2) INNER t3`, because
    // NULL-extended probe rows can survive differently, and the pass that
    // finds the unmatched right rows runs only the loops after the join.
    for (full, source) in sources.iter().enumerate() {
        if !facts(source).full_outer {
            continue;
        }
        for required_lhs in required_lhs_by_table.iter_mut().skip(full + 1) {
            required_lhs.set(full)?;
        }
    }

    Ok(JoinOrderingRestrictions {
        required_lhs_by_table: Some(required_lhs_by_table),
    })
}

/// The join-ordering restrictions of HIR sources, read from their join kinds.
pub(crate) fn hir_join_ordering_restrictions(
    sources: &[HirPlanSource],
) -> Result<JoinOrderingRestrictions> {
    join_ordering_restrictions(sources, |source| {
        let kind = source.join_info().map(|join| join.kind);
        JoinOrderingFacts {
            // A LATERAL subquery reads the sources on its left, so it
            // stays after them like the right side of a LEFT JOIN.
            constrained: source.is_lateral()
                || matches!(
                    kind,
                    Some(
                        crate::translate::semantic::hir::JoinKind::Cross
                            | crate::translate::semantic::hir::JoinKind::Left
                            | crate::translate::semantic::hir::JoinKind::Right
                            | crate::translate::semantic::hir::JoinKind::Full
                            | crate::translate::semantic::hir::JoinKind::Semi
                            | crate::translate::semantic::hir::JoinKind::Anti
                    )
                ),
            full_outer: matches!(
                kind,
                Some(
                    crate::translate::semantic::hir::JoinKind::Right
                        | crate::translate::semantic::hir::JoinKind::Full
                )
            ),
        }
    })
}

/// One candidate source and access method during greedy HIR join planning.
#[derive(Debug)]
struct HirJoinStep {
    source_position: usize,
    access_method: AccessMethod,
}

/// Keeps flattened FROM-group leaves together while the greedy planner chooses
/// their access order. This preserves syntactic join barriers without making a
/// group into a runtime source.
struct HirJoinBoundaries<'a> {
    groups: &'a [HirFromGroupBoundary],
    ordering_restrictions: &'a JoinOrderingRestrictions,
}

impl HirJoinBoundaries<'_> {
    fn allows_candidate(&self, source_position: usize, joined: &TableMask) -> bool {
        for group in self.groups {
            let started = group
                .source_range
                .clone()
                .any(|position| joined.get(position));
            let complete = group
                .source_range
                .clone()
                .all(|position| joined.get(position));

            if started && !complete && !group.source_range.contains(&source_position) {
                return false;
            }

            if !started && group.source_range.contains(&source_position) {
                let group_requires_outer_prefix = group
                    .join_info
                    .as_ref()
                    .is_some_and(|join| hir_join_kind_is_ordering_constrained(join.kind));
                let nested_group_requires_outer_prefix = self.groups.iter().any(|nested| {
                    group.source_range.start <= nested.source_range.start
                        && nested.source_range.end <= group.source_range.end
                        && nested
                            .join_info
                            .as_ref()
                            .is_some_and(|join| hir_join_kind_is_ordering_constrained(join.kind))
                });
                let leaf_requires_outer_source = group.source_range.clone().any(|position| {
                    self.ordering_restrictions
                        .required_lhs(position)
                        .is_some_and(|required| {
                            required.iter().any(|required_position| {
                                !group.source_range.contains(&required_position)
                                    && !joined.get(required_position)
                            })
                        })
                });
                if ((group_requires_outer_prefix || nested_group_requires_outer_prefix)
                    && (0..group.source_range.start).any(|position| !joined.get(position)))
                    || leaf_requires_outer_source
                {
                    return false;
                }
            }

            if !complete
                && source_position >= group.source_range.end
                && group
                    .join_info
                    .as_ref()
                    .is_some_and(|join| join.kind == hir::JoinKind::Full)
            {
                return false;
            }
        }
        true
    }
}

fn hir_join_kind_is_ordering_constrained(kind: hir::JoinKind) -> bool {
    matches!(
        kind,
        hir::JoinKind::Cross | hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full
    )
}

/// Above this many sources, HIR join ordering is greedy instead of trying
/// every order.
const HIR_EXHAUSTIVE_JOIN_LIMIT: usize = 8;

/// A left-deep HIR join of some sources, ending with one of them.
struct HirPartialJoin {
    steps: Vec<(usize, AccessMethod)>,
    output_cardinality: f64,
    /// The estimated number of rows after each step, in order.
    prefix_cardinalities: Vec<f64>,
    cost: Cost,
}

/// Choose an HIR join order. Small joins try every order,
/// building the cheapest plan for each set of sources and last source from
/// the plans of its subsets, and keep both the cheapest plan and the
/// cheapest one that returns rows in the order target's order. Larger joins
/// use the greedy rules.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_hir_join_order(
    document: &HirDocument,
    sources: &[HirPlanSource],
    groups: &[HirFromGroupBoundary],
    constraints: &[HirTableConstraints],
    where_clause: &[HirWhereTerm],
    base_table_rows: &[RowCountEstimate],
    source_costs: &[Cost],
    order_target: Option<&HirOrderTarget<'_>>,
    initial_input_cardinality: f64,
    access_methods_arena: &mut Vec<AccessMethod>,
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
    or_union: bool,
    hash: &HirHashJoins<'_>,
) -> Result<Option<BestJoinOrderResult>> {
    if sources.len() <= 1 || sources.len() > HIR_EXHAUSTIVE_JOIN_LIMIT {
        return compute_hir_greedy_join_order(
            document,
            sources,
            groups,
            constraints,
            where_clause,
            base_table_rows,
            source_costs,
            order_target,
            initial_input_cardinality,
            access_methods_arena,
            schema,
            analyze_stats,
            params,
            or_union,
            hash,
        );
    }
    let ordering_restrictions = hir_join_ordering_restrictions(sources)?;
    let boundaries = HirJoinBoundaries {
        groups,
        ordering_restrictions: &ordering_restrictions,
    };
    let layout = HirFromLayout::new(sources, groups);
    let predicate_work = build_hir_predicate_work(document, &layout, where_clause)?;
    let step = |position: usize,
                lhs_mask: &TableMask,
                lhs_steps: &[(usize, AccessMethod)],
                input_cardinality: f64| {
        let lhs_steps = lhs_steps
            .iter()
            .map(|(position, method)| (*position, method))
            .collect::<Vec<_>>();
        hir_join_step(
            document,
            sources,
            &layout,
            constraints,
            where_clause,
            &predicate_work,
            base_table_rows,
            source_costs,
            order_target,
            position,
            lhs_mask,
            &lhs_steps,
            input_cardinality,
            schema,
            analyze_stats,
            params,
            or_union,
            hash,
        )
    };
    let allowed = |position: usize, lhs_mask: &TableMask| {
        boundaries.allows_candidate(position, lhs_mask)
            && ordering_restrictions
                .required_lhs(position)
                .is_none_or(|required| lhs_mask.contains_all_set_bits_of(required))
    };

    let mut memo: HashMap<TableMask, HashMap<usize, HirPartialJoin>> = HashMap::default();
    let empty = TableMask::default();
    for (position, source_constraints) in constraints.iter().enumerate().take(sources.len()) {
        if !allowed(position, &empty) {
            continue;
        }
        let Some(first) = step(position, &empty, &[], initial_input_cardinality)? else {
            continue;
        };
        let mut mask = TableMask::default();
        mask.set(position)?;
        let output_cardinality = rows_after_join(
            initial_input_cardinality,
            &first.access_method,
            source_constraints,
            &empty,
            mask.try_clone()?,
            sources,
            position,
            where_clause,
            params,
        );
        let cost = first.access_method.cost;
        memo.entry(mask).or_default().insert(
            position,
            HirPartialJoin {
                steps: vec![(position, first.access_method)],
                output_cardinality,
                prefix_cardinalities: vec![output_cardinality],
                cost,
            },
        );
    }

    // Add `rhs` after the tables of `lhs`.
    let extend = |lhs: &HirPartialJoin,
                  lhs_mask: &TableMask,
                  rhs: usize|
     -> Result<Option<HirPartialJoin>> {
        let Some(next) = step(rhs, lhs_mask, &lhs.steps, lhs.output_cardinality)? else {
            return Ok(None);
        };
        let mut method = next.access_method;
        // A table nothing joins to the tables before it pays for the cross
        // product.
        let connected = predicate_work
            .iter()
            .any(|work| work.table_mask.get(rhs) && work.table_mask.intersects(lhs_mask));
        if !connected {
            let base_rows = base_table_rows
                .get(rhs)
                .copied()
                .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params));
            let self_selectivity = build_self_constraint_selectivity(&constraints[rhs], rhs);
            method.cost =
                method.cost + Cost(lhs.output_cardinality * *base_rows * self_selectivity);
        }
        let mut rhs_mask = TableMask::default();
        rhs_mask.set(rhs)?;
        let output_cardinality = rows_after_join(
            lhs.output_cardinality,
            &method,
            &constraints[rhs],
            lhs_mask,
            rhs_mask,
            sources,
            rhs,
            where_clause,
            params,
        );
        let mut steps = lhs
            .steps
            .iter()
            .map(|(position, method)| (*position, method.clone()))
            .collect::<Vec<_>>();
        steps.push((rhs, method.clone()));
        let mut prefix_cardinalities = lhs.prefix_cardinalities.clone();
        prefix_cardinalities.push(output_cardinality);
        Ok(Some(HirPartialJoin {
            steps,
            output_cardinality,
            prefix_cardinalities,
            cost: lhs.cost + method.cost,
        }))
    };

    // The plan that joins the tables in FROM order goes first and only a
    // strictly cheaper plan replaces it.
    let mut from_order: Option<HirPartialJoin> = memo
        .get(&{
            let mut mask = TableMask::default();
            mask.set(0)?;
            mask
        })
        .and_then(|plans| plans.get(&0))
        .map(|first| HirPartialJoin {
            steps: first.steps.clone(),
            output_cardinality: first.output_cardinality,
            prefix_cardinalities: first.prefix_cardinalities.clone(),
            cost: first.cost,
        });
    let mut from_order_mask = TableMask::default();
    from_order_mask.set(0)?;
    for rhs in 1..sources.len() {
        let Some(lhs) = from_order.take() else {
            break;
        };
        if !allowed(rhs, &from_order_mask) {
            break;
        }
        from_order = extend(&lhs, &from_order_mask, rhs)?;
        from_order_mask.set(rhs)?;
    }

    let mut complete: Vec<HirPartialJoin> = Vec::new();
    complete.extend(from_order);
    for subset_size in 2..=sources.len() {
        for mask in generate_join_bitmasks(sources.len(), subset_size) {
            let mask = mask?;
            let mut best_by_last: HashMap<usize, HirPartialJoin> = HashMap::default();
            for rhs in 0..sources.len() {
                if !mask.get(rhs) {
                    continue;
                }
                let mut lhs_mask = mask.try_clone()?;
                lhs_mask.clear(rhs);
                if !allowed(rhs, &lhs_mask) {
                    continue;
                }
                let Some(lhs_plans) = memo.get(&lhs_mask) else {
                    continue;
                };
                let mut last_positions = lhs_plans.keys().copied().collect::<Vec<_>>();
                last_positions.sort_unstable();
                for last in last_positions {
                    let Some(plan) = extend(&lhs_plans[&last], &lhs_mask, rhs)? else {
                        continue;
                    };
                    if best_by_last
                        .get(&rhs)
                        .is_some_and(|existing| existing.cost.0 <= plan.cost.0)
                    {
                        continue;
                    }
                    best_by_last.insert(rhs, plan);
                }
            }
            if subset_size == sources.len() {
                // Visit the finished plans in the map's order, which is the
                // same on every run, so equally cheap plans resolve the same
                // way.
                complete.extend(best_by_last.into_values());
            } else if !best_by_last.is_empty() {
                memo.insert(mask, best_by_last);
            }
        }
    }

    let into_join = |plan: HirPartialJoin, arena: &mut Vec<AccessMethod>| JoinN {
        data: plan
            .steps
            .into_iter()
            .map(|(position, method)| {
                arena.push(method);
                (position, arena.len() - 1)
            })
            .collect(),
        output_cardinality: plan.output_cardinality,
        prefix_cardinalities: plan.prefix_cardinalities,
        cost: plan.cost,
    };
    let Some(best_position) = (0..complete.len()).reduce(|best, position| {
        if complete[position].cost.0 < complete[best].cost.0 {
            position
        } else {
            best
        }
    }) else {
        return Err(LimboError::PlanningError("no query solution".to_string()));
    };
    // The cheapest plan whose loops already follow the order target, which
    // the caller may prefer to skip a sort.
    let mut best_ordered: Option<(usize, Cost)> = None;
    if let Some(target) = order_target {
        for (position, plan) in complete.iter().enumerate() {
            if best_ordered.is_some_and(|(_, cost)| cost.0 <= plan.cost.0) {
                continue;
            }
            let mut scratch = Vec::new();
            let candidate = into_join(
                HirPartialJoin {
                    steps: plan
                        .steps
                        .iter()
                        .map(|(source, method)| (*source, method.clone()))
                        .collect(),
                    output_cardinality: plan.output_cardinality,
                    prefix_cardinalities: plan.prefix_cardinalities.clone(),
                    cost: plan.cost,
                },
                &mut scratch,
            );
            if super::order::hir_plan_satisfies_order_target(
                &candidate, &scratch, sources, document, target, schema,
            )? {
                best_ordered = Some((position, plan.cost));
            }
        }
    }
    let best_ordered_position = best_ordered
        .map(|(position, _)| position)
        .filter(|position| *position != best_position);
    let mut complete = complete.into_iter().map(Some).collect::<Vec<_>>();
    let best_plan = into_join(
        complete[best_position]
            .take()
            .expect("best HIR join plan is taken once"),
        access_methods_arena,
    );
    let best_ordered_plan = best_ordered_position.map(|position| {
        into_join(
            complete[position]
                .take()
                .expect("ordered HIR join plan is taken once"),
            access_methods_arena,
        )
    });
    Ok(Some(BestJoinOrderResult {
        best_plan,
        best_ordered_plan,
    }))
}

/// Build a left-deep HIR join plan using the existing greedy rules.
///
/// HIR supports B-tree, virtual-table, derived, and CTE scans. Hash joins and
/// OR-by-union seeks are tried only when `hash` and `or_union` allow them.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn compute_hir_greedy_join_order(
    document: &HirDocument,
    sources: &[HirPlanSource],
    groups: &[HirFromGroupBoundary],
    constraints: &[HirTableConstraints],
    where_clause: &[HirWhereTerm],
    base_table_rows: &[RowCountEstimate],
    source_costs: &[Cost],
    order_target: Option<&HirOrderTarget<'_>>,
    initial_input_cardinality: f64,
    access_methods_arena: &mut Vec<AccessMethod>,
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
    or_union: bool,
    hash: &HirHashJoins<'_>,
) -> Result<Option<BestJoinOrderResult>> {
    if sources.is_empty() {
        return Ok(None);
    }

    let ordering_restrictions = hir_join_ordering_restrictions(sources)?;
    let boundaries = HirJoinBoundaries {
        groups,
        ordering_restrictions: &ordering_restrictions,
    };
    let layout = HirFromLayout::new(sources, groups);
    let predicate_work = build_hir_predicate_work(document, &layout, where_clause)?;
    let starting_sources = hir_starting_sources_by_score(
        document,
        sources,
        constraints,
        base_table_rows,
        source_costs,
        &ordering_restrictions,
        &boundaries,
        analyze_stats,
        params,
    )?;

    let mut remaining: TableMask = (0..sources.len()).try_collect()?;
    let mut joined_mask = TableMask::default();
    // A table function whose arguments read another table cannot start
    // the join, so the best starting table its virtual table accepts wins.
    let mut first = None;
    for &position in &starting_sources {
        first = hir_join_step(
            document,
            sources,
            &layout,
            constraints,
            where_clause,
            &predicate_work,
            base_table_rows,
            source_costs,
            order_target,
            position,
            &joined_mask,
            &[],
            initial_input_cardinality,
            schema,
            analyze_stats,
            params,
            or_union,
            hash,
        )?;
        if first.is_some() {
            break;
        }
    }
    let first = first.ok_or_else(|| {
        let name = starting_sources
            .first()
            .and_then(|position| sources[*position].btree())
            .map_or("", |source| source.table.get_name());
        LimboError::PlanningError(format!(
            "virtual table {name} rejected the available constraints"
        ))
    })?;
    let first_position = first.source_position;
    let mut first_mask = TableMask::default();
    first_mask.set(first_position)?;
    let mut output_cardinality = rows_after_join(
        initial_input_cardinality,
        &first.access_method,
        &constraints[first_position],
        &joined_mask,
        first_mask,
        sources,
        first_position,
        where_clause,
        params,
    );
    let mut cost = first.access_method.cost;
    access_methods_arena.push(first.access_method);
    let mut data = vec![(first_position, access_methods_arena.len() - 1)];
    let mut prefix_cardinalities = vec![output_cardinality];
    joined_mask.set(first_position)?;
    remaining.clear(first_position);

    while !remaining.is_empty() {
        let mut has_connected_candidate = false;
        for position in &remaining {
            if !boundaries.allows_candidate(position, &joined_mask) {
                continue;
            }
            if let Some(required) = ordering_restrictions.required_lhs(position) {
                if !joined_mask.contains_all_set_bits_of(required) {
                    continue;
                }
            }
            let connected = predicate_work.iter().any(|work| {
                work.table_mask.get(position) && work.table_mask.intersects(&joined_mask)
            });
            if connected {
                has_connected_candidate = true;
                break;
            }
        }

        let mut best: Option<(usize, AccessMethod, f64, Cost)> = None;
        for position in &remaining {
            if !boundaries.allows_candidate(position, &joined_mask) {
                continue;
            }
            if let Some(required) = ordering_restrictions.required_lhs(position) {
                if !joined_mask.contains_all_set_bits_of(required) {
                    continue;
                }
            }
            let connected = predicate_work.iter().any(|work| {
                work.table_mask.get(position) && work.table_mask.intersects(&joined_mask)
            });
            if has_connected_candidate && !connected {
                continue;
            }

            let lhs_steps = data
                .iter()
                .map(|(position, method)| (*position, &access_methods_arena[*method]))
                .collect::<Vec<_>>();
            let Some(step) = hir_join_step(
                document,
                sources,
                &layout,
                constraints,
                where_clause,
                &predicate_work,
                base_table_rows,
                source_costs,
                order_target,
                position,
                &joined_mask,
                &lhs_steps,
                output_cardinality,
                schema,
                analyze_stats,
                params,
                or_union,
                hash,
            )?
            else {
                continue;
            };
            let position = step.source_position;
            let mut method = step.access_method;
            if !connected {
                let base_rows = base_table_rows
                    .get(position)
                    .copied()
                    .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params));
                let self_selectivity =
                    build_self_constraint_selectivity(&constraints[position], position);
                method.cost =
                    method.cost + Cost(output_cardinality * *base_rows * self_selectivity);
            }

            let candidate_cost = cost + method.cost;
            let mut rhs_mask = TableMask::default();
            rhs_mask.set(position)?;
            let candidate_cardinality = rows_after_join(
                output_cardinality,
                &method,
                &constraints[position],
                &joined_mask,
                rhs_mask,
                sources,
                position,
                where_clause,
                params,
            );
            if best
                .as_ref()
                .is_none_or(|(_, _, _, best_cost)| candidate_cost < *best_cost)
            {
                best = Some((position, method, candidate_cardinality, candidate_cost));
            }
        }

        let (position, method, cardinality, next_cost) = best.ok_or_else(|| {
            LimboError::PlanningError("Greedy HIR join ordering: no valid next source".to_string())
        })?;
        access_methods_arena.push(method);
        data.push((position, access_methods_arena.len() - 1));
        joined_mask.set(position)?;
        remaining.clear(position);
        output_cardinality = cardinality;
        prefix_cardinalities.push(output_cardinality);
        cost = next_cost;
    }

    Ok(Some(BestJoinOrderResult {
        best_plan: JoinN {
            data,
            output_cardinality,
            prefix_cardinalities,
            cost,
        },
        best_ordered_plan: None,
    }))
}

#[allow(clippy::too_many_arguments)]
/// Whether a source is the right side of a RIGHT or FULL JOIN.
fn hir_right_or_full_join(join_info: Option<&crate::translate::plan::HirJoinInfo>) -> bool {
    join_info.is_some_and(|join| matches!(join.kind, hir::JoinKind::Right | hir::JoinKind::Full))
}

#[allow(clippy::too_many_arguments)]
fn hir_join_step(
    document: &HirDocument,
    sources: &[HirPlanSource],
    layout: &HirFromLayout<'_>,
    constraints: &[HirTableConstraints],
    where_clause: &[HirWhereTerm],
    predicate_work: &[PredicateWorkInfo],
    base_table_rows: &[RowCountEstimate],
    source_costs: &[Cost],
    order_target: Option<&HirOrderTarget<'_>>,
    source_position: usize,
    lhs_mask: &TableMask,
    lhs_steps: &[(usize, &AccessMethod)],
    input_cardinality: f64,
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
    or_union: bool,
    hash: &HirHashJoins<'_>,
) -> Result<Option<HirJoinStep>> {
    let source = &sources[source_position];
    let mut joined_mask = lhs_mask.try_clone()?;
    joined_mask.set(source_position)?;
    let ready_where = ready_predicate_work_by_owner(
        where_clause,
        predicate_work,
        &joined_mask,
        source_position,
        |owner| layout.predicate_owner_ready(owner, &joined_mask, source_position),
    );
    let base_row_count = base_table_rows
        .get(source_position)
        .copied()
        .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params));
    let mut access_method = match source {
        HirPlanSource::BTree(source) => match &source.table {
            Table::BTree(_) => {
                let mut method = choose_single_btree_access_method(
                    &HirAccessSource::new(
                        source,
                        document
                            .source(source.internal_id)
                            .expect("validated HIR contains referenced source"),
                    ),
                    &constraints[source_position],
                    lhs_mask,
                    source_position,
                    order_target,
                    &ready_where,
                    schema,
                    analyze_stats,
                    input_cardinality,
                    base_row_count,
                    params,
                )?;
                // A scan may later become one seek per branch of an OR term,
                // so the join order weighs that union's cost. When the union
                // beats a scan or a temporary index, the step is a scan at
                // the union's cost, which access planning then turns into
                // the union.
                let scans_or_builds = matches!(
                    &method.params,
                    AccessMethodParams::BTreeTable {
                        build_index,
                        constraint_refs,
                        ..
                    } if *build_index || constraint_refs.is_empty()
                );
                if or_union && scans_or_builds {
                    if let Some((_, union_cost, union_rows)) = super::hir_or_union_with_cost(
                        document,
                        layout,
                        source,
                        source_position,
                        where_clause,
                        lhs_mask,
                        base_row_count,
                        schema,
                        params,
                    )? {
                        let union_method = AccessMethod {
                            cost: Cost(union_cost.0 * input_cardinality),
                            estimated_rows_per_outer_row: union_rows,
                            consumed_where_terms: Default::default(),
                            params: AccessMethodParams::BTreeTable {
                                iter_dir: crate::translate::plan::IterationDirection::Forwards,
                                index: None,
                                build_index: false,
                                constraint_refs: Vec::new(),
                            },
                        };
                        // Both sides count the WHERE terms the rows they
                        // return still have to pass.
                        let with_where = |method: &AccessMethod| {
                            super::access_method::cost_with_where_work(
                                method,
                                &ready_where,
                                input_cardinality,
                                params,
                            )
                        };
                        if with_where(&union_method) < with_where(&method) {
                            method = union_method;
                        }
                    }
                }
                method
            }
            Table::Virtual(vtab) => {
                // The virtual table cannot be read after these tables, for
                // example when an argument it needs reads a later table.
                let Some(method) = find_best_hir_access_method_for_vtab(
                    vtab,
                    &constraints[source_position].constraints,
                    source_position,
                    lhs_mask,
                    input_cardinality,
                    base_row_count,
                    params,
                )?
                else {
                    return Ok(None);
                };
                method
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR catalog source {} is not a table",
                    source.internal_id
                )))
            }
        },
        HirPlanSource::Derived {
            source,
            query,
            join_info,
            output_order,
            lateral,
        } => super::access_method::find_best_hir_access_method_for_subquery(
            document,
            *source,
            *query,
            false,
            // A LATERAL subquery runs again for each outer row, so its
            // rows are never stored for a seek.
            !hir_right_or_full_join(join_info.as_ref()) && !*lateral,
            order_target,
            output_order,
            &constraints[source_position],
            lhs_mask,
            source_position,
            &ready_where,
            source_costs[source_position],
            input_cardinality,
            base_row_count,
            params,
            schema,
        )?,
        HirPlanSource::Cte {
            source,
            query,
            materialization,
            join_info,
            output_order,
            ..
        } => super::access_method::find_best_hir_access_method_for_subquery(
            document,
            *source,
            *query,
            !matches!(
                materialization,
                crate::translate::plan::HirCteMaterialization::PerReference
            ),
            !hir_right_or_full_join(join_info.as_ref()),
            order_target,
            output_order,
            &constraints[source_position],
            lhs_mask,
            source_position,
            &ready_where,
            source_costs[source_position],
            input_cardinality,
            base_row_count,
            params,
            schema,
        )?,
        HirPlanSource::RecursiveCte { .. } | HirPlanSource::RecursiveInput { .. } => {
            let rows = *base_row_count;
            let scan_cost = estimate_cost_for_scan_or_seek::<hir::Expr>(
                None,
                &[],
                &[],
                input_cardinality,
                base_row_count,
                false,
                params,
                None,
            );
            let reexecution_cost =
                Cost((input_cardinality - 1.0).max(0.0) * rows * params.cpu_cost_per_seek);
            AccessMethod {
                cost: scan_cost
                    + reexecution_cost
                    + Cost(input_cardinality * source_costs[source_position].0),
                estimated_rows_per_outer_row: rows,
                consumed_where_terms: Default::default(),
                params: AccessMethodParams::Subquery {
                    iter_dir: crate::translate::plan::IterationDirection::Forwards,
                },
            }
        }
    };
    add_where_cost(&mut access_method, &ready_where, input_cardinality, params);
    if let Some(hash_join) = hir_hash_join_candidate(
        document,
        sources,
        constraints,
        where_clause,
        base_table_rows,
        source_position,
        lhs_mask,
        lhs_steps,
        &access_method,
        &ready_where,
        input_cardinality,
        hash,
        schema,
        params,
    )? {
        access_method = hash_join;
    }

    Ok(Some(HirJoinStep {
        source_position,
        access_method,
    }))
}

/// Whether and how a HIR block may use hash joins.
pub(crate) struct HirHashJoins<'a> {
    /// Whether the block may hash at all. A block that may run again, such as
    /// a subquery, does not: a hash table that moved rows to disk cannot be
    /// probed again after its waiting lookups ran.
    pub(crate) allowed: bool,
    /// Whether a hash table may be built from the joined rows of the loops
    /// before the probe, a build input
    /// ([crate::translate::semantic_to_plan::HirHashBuildInput]).
    pub(crate) materialize: bool,
    /// The sources a correlated subquery of the block reads. A hash join
    /// never reads them.
    pub(crate) subquery_reads: &'a [hir::SourceId],
}

impl HirHashJoins<'_> {
    pub(crate) const NONE: HirHashJoins<'static> = HirHashJoins {
        allowed: false,
        materialize: false,
        subquery_reads: &[],
    };
}

/// The one source an expression's columns and rowids read, if exactly one.
fn hir_one_source(expression: &hir::Expr) -> Option<hir::SourceId> {
    let mut found = None;
    let mut more = false;
    expression.for_each(&mut |expression| {
        let source = match expression {
            hir::Expr::Column(column) => column.source,
            hir::Expr::RowId(source) => *source,
            _ => return,
        };
        match found {
            Some(known) if known != source => more = true,
            _ => found = Some(source),
        }
    });
    (!more).then_some(found).flatten()
}

fn hir_runs_subquery(expression: &hir::Expr) -> bool {
    let mut runs = false;
    expression.for_each(&mut |expression| {
        runs |= matches!(expression, hir::Expr::Subquery(_));
    });
    runs
}

/// Whether a source is the right side of a USING or NATURAL join.
fn hir_joined_with_using(document: &HirDocument, source: hir::SourceId) -> bool {
    let Some(hir::SourceOwner::QueryBlock(block)) = document.source(source).map(|s| s.owner) else {
        return false;
    };
    let Some(from) = document
        .query_block(block)
        .and_then(|block| block.from.as_ref())
    else {
        return false;
    };
    let mut pending = vec![from];
    while let Some(from) = pending.pop() {
        for join in &from.joins {
            if join.right == source {
                return matches!(
                    join.constraint,
                    hir::JoinConstraint::Using(_) | hir::JoinConstraint::Natural(_)
                );
            }
            if let Some(hir::SourceKind::FromGroup(group)) =
                document.source(join.right).map(|source| &source.kind)
            {
                pending.push(&group.from);
            }
        }
    }
    false
}

/// Whether a HIR source is a rowid B-tree table that the hash lookup code
/// can build from and probe with: no custom types or arrays, not a
/// materialized view.
fn hir_plain_rowid_table(document: &HirDocument, source: &HirPlanSource, schema: &Schema) -> bool {
    let Some(planned) = source.btree() else {
        return false;
    };
    let Some(definition) = document.source(planned.internal_id) else {
        return false;
    };
    matches!(&planned.table, Table::BTree(table)
        if table.has_rowid && !schema.is_materialized_view(&table.name))
        && definition.column_type_programs.iter().all(Option::is_none)
        && definition
            .columns
            .iter()
            .all(|column| !column.type_fact.is_array())
}

/// A hash join of HIR sources that builds a hash table from `build` and
/// scans `probe`, if the two may be hashed together.
#[allow(clippy::too_many_arguments)]
fn hir_try_hash_join_access_method(
    document: &HirDocument,
    sources: &[HirPlanSource],
    constraints: &[HirTableConstraints],
    where_clause: &[HirWhereTerm],
    build: usize,
    probe: usize,
    lhs_mask: &TableMask,
    max_distinct_build_keys: f64,
    build_cardinality: f64,
    probe_cardinality: f64,
    probe_multiplier: f64,
    hash_can_replace_build_index: bool,
    hash: &HirHashJoins<'_>,
    schema: &Schema,
    params: &CostModelParams,
) -> Option<AccessMethod> {
    use crate::translate::semantic::hir::JoinKind;
    let (Some(build_source), Some(probe_source)) = (sources[build].btree(), sources[probe].btree())
    else {
        return None;
    };
    // The hash lookup code reads rowid tables only.
    if !hir_plain_rowid_table(document, &sources[build], schema)
        || !hir_plain_rowid_table(document, &sources[probe], schema)
    {
        return None;
    }
    let probe_kind = probe_source.join_info.as_ref().map(|join| join.kind);
    let build_kind = build_source.join_info.as_ref().map(|join| join.kind);
    // A RIGHT join has no hash form, and a semi-join prefers a nested loop
    // with an index seek, which stops at its first match. An anti-join
    // hashes: the build rows the probe matches are remembered, and the
    // others come out after the probe loop.
    let hash_join_type = match probe_kind {
        Some(JoinKind::Right | JoinKind::Semi) => return None,
        Some(JoinKind::Anti) => HashJoinType::LeftAnti,
        Some(JoinKind::Full) => HashJoinType::FullOuter,
        Some(JoinKind::Left) => HashJoinType::LeftOuter,
        _ => HashJoinType::Inner,
    };
    // Can't build from a table an outer join may fill with NULL, and no
    // hash join for semi/anti-joins.
    if matches!(
        build_kind,
        Some(JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Semi | JoinKind::Anti)
    ) {
        return None;
    }
    // Avoid hash join on self-joins over the same table: a nested loop with
    // an index seek is usually better.
    let (Table::BTree(build_table), Table::BTree(probe_table)) =
        (&build_source.table, &probe_source.table)
    else {
        return None;
    };
    // Two reads of one table are not hashed, except for a FULL OUTER join.
    if build_table.root_page == probe_table.root_page && hash_join_type != HashJoinType::FullOuter {
        return None;
    }
    // INDEXED BY and NOT INDEXED must be honored.
    if !matches!(build_source.indexed, hir::IndexHint::None)
        || !matches!(probe_source.indexed, hir::IndexHint::None)
    {
        return None;
    }
    if hir_joined_with_using(document, build_source.internal_id)
        || hir_joined_with_using(document, probe_source.internal_id)
    {
        return None;
    }
    // Avoid hash joins when a correlated subquery reads the joined tables.
    if hash
        .subquery_reads
        .iter()
        .any(|source| *source == build_source.internal_id || *source == probe_source.internal_id)
    {
        return None;
    }

    // Port of `find_hash_join_keys`: `=` terms whose sides each read one of
    // the two tables. An outer join condition belongs to that join only.
    // An outer hash join takes its keys from its own ON terms only: a WHERE
    // term used as a key would never be checked on the rows
    // the join NULL-fills, which it must reject (`... FULL JOIN u ON t.x =
    // u.x WHERE t.x = u.x` returns the matched rows only).
    let mut join_keys: SmallVec<[HashJoinKey; 2]> = SmallVec::new();
    for (index, term) in where_clause.iter().enumerate() {
        let own_on_term = term.from_outer_join == Some(probe_source.internal_id);
        if term.from_outer_join.is_some() && !own_on_term {
            continue;
        }
        // An anti-join's terms are plain terms that read its inner table.
        if !matches!(hash_join_type, HashJoinType::Inner | HashJoinType::LeftAnti) && !own_on_term {
            continue;
        }
        let hir::Expr::Binary {
            lhs,
            operator: Operator::Equals,
            rhs,
            ..
        } = &term.expr
        else {
            continue;
        };
        let (Some(left), Some(right)) = (hir_one_source(lhs), hir_one_source(rhs)) else {
            continue;
        };
        let build_side = if left == build_source.internal_id && right == probe_source.internal_id {
            super::constraints::BinaryExprSide::Lhs
        } else if right == build_source.internal_id && left == probe_source.internal_id {
            super::constraints::BinaryExprSide::Rhs
        } else {
            continue;
        };
        join_keys.push(HashJoinKey {
            where_clause_idx: index,
            build_side,
        });
    }
    // The hash table needs at least one key; a FULL JOIN without one runs
    // as a nested loop.
    if join_keys.is_empty() {
        return None;
    }
    let sides = |key: &HashJoinKey| {
        let hir::Expr::Binary {
            lhs,
            rhs,
            comparison,
            custom,
            ..
        } = &where_clause[key.where_clause_idx].expr
        else {
            unreachable!("a hash join key is a binary `=`");
        };
        let (build_expr, probe_expr) = match key.build_side {
            super::constraints::BinaryExprSide::Lhs => (lhs.as_ref(), rhs.as_ref()),
            super::constraints::BinaryExprSide::Rhs => (rhs.as_ref(), lhs.as_ref()),
        };
        (build_expr, probe_expr, comparison, custom)
    };
    for key in &join_keys {
        let (build_expr, _, comparison, custom) = sides(key);
        // A key is one value, does not run a subquery, and compares with a
        // built-in collation.
        if custom.is_some()
            || hir_runs_subquery(build_expr)
            || matches!(build_expr, hir::Expr::Row(_))
            || comparison
                .as_ref()
                .is_some_and(|comparison| comparison.components.len() != 1)
        {
            return None;
        }
        let custom_collation = comparison.as_ref().is_some_and(|comparison| {
            comparison.components.iter().any(|component| {
                matches!(
                    component
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value()),
                    Some(crate::translate::collate::CollationSeq::Custom(_))
                )
            })
        });
        if custom_collation {
            return None;
        }
    }

    // Prefer a nested loop when an index can seek the probe's join column
    // with the loops before it joined, or when the build has an index on
    // its join column that the probe's own filters make worth keeping.
    let key_constraint = |position: usize, key: &HashJoinKey| {
        constraints[position]
            .constraints
            .iter()
            .position(|constraint| constraint.where_clause_pos.0 == key.where_clause_idx)
    };
    let simple_column = |expr: &hir::Expr, source: &crate::translate::plan::HirPlannedSource| {
        hir_one_source(expr) == Some(source.internal_id)
            && matches!(expr, hir::Expr::Column(_) | hir::Expr::RowId(_))
    };
    let probe_index_can_seek_join_key = |key: &HashJoinKey| {
        let Some(constraint_position) = key_constraint(probe, key) else {
            return false;
        };
        let probe_constraints = &constraints[probe];
        probe_constraints.candidates.iter().any(|candidate| {
            usable_constraints_for_lhs_mask(
                &probe_constraints.constraints,
                &candidate.refs,
                lhs_mask,
                probe,
            )
            .iter()
            .any(|constraint_ref| {
                constraint_ref
                    .eq
                    .as_ref()
                    .is_some_and(|equality| equality.constraint_pos == constraint_position)
            })
        })
    };
    let build_index_on_join_column = |key: &HashJoinKey| {
        let Some(constraint_position) = key_constraint(build, key) else {
            return false;
        };
        let build_constraints = &constraints[build];
        let Some(column) = build_constraints.constraints[constraint_position].table_col_pos else {
            return false;
        };
        build_source
            .table
            .columns()
            .get(column)
            .is_some_and(|column| column.is_rowid_alias())
            || build_constraints.candidates.iter().any(|candidate| {
                candidate
                    .index
                    .as_ref()
                    .is_some_and(|index| index.column_table_pos_to_index_pos(column).is_some())
            })
    };
    // FULL OUTER must hash.
    if hash_join_type != HashJoinType::FullOuter {
        for key in &join_keys {
            let (build_expr, probe_expr, ..) = sides(key);
            if simple_column(probe_expr, probe_source) && probe_index_can_seek_join_key(key) {
                return None;
            }
            if simple_column(build_expr, build_source)
                && !hash_can_replace_build_index
                && build_index_on_join_column(key)
            {
                return None;
            }
        }
    }

    let join_selectivity = join_keys
        .iter()
        .map(|key| {
            constraints[probe]
                .constraints
                .iter()
                .find(|constraint| constraint.where_clause_pos.0 == key.where_clause_idx)
                .map_or(params.sel_eq_unindexed, |constraint| constraint.selectivity)
        })
        .product::<f64>();
    // Keys that cover the build's rowid or a whole UNIQUE index match each
    // probe row to one build row.
    let keys_cover_unique_build_key = join_keys.iter().any(|key| {
        let (build_expr, ..) = sides(key);
        match build_expr {
            hir::Expr::RowId(source) => *source == build_source.internal_id,
            hir::Expr::Column(column) => {
                column.source == build_source.internal_id
                    && build_source
                        .table
                        .columns()
                        .get(column.column)
                        .is_some_and(|column| column.is_rowid_alias())
            }
            _ => false,
        }
    }) || {
        let build_constraints = &constraints[build];
        let key_terms: SmallVec<[usize; 4]> = join_keys
            .iter()
            .filter(|key| {
                let (build_expr, ..) = sides(key);
                matches!(build_expr, hir::Expr::Column(column) if column.source == build_source.internal_id)
            })
            .map(|key| key.where_clause_idx)
            .collect();
        build_constraints.candidates.iter().any(|candidate| {
            candidate.index.as_ref().is_some_and(|index| {
                index.unique
                    && index.where_clause.is_none()
                    && !index.columns.is_empty()
                    && index
                        .columns
                        .iter()
                        .enumerate()
                        .all(|(index_column, column)| {
                            column.expr.is_none()
                                && candidate.refs.iter().any(|constraint_ref| {
                                    constraint_ref.index_col_pos == index_column
                                        && key_terms.contains(
                                            &build_constraints.constraints
                                                [constraint_ref.constraint_vec_pos]
                                                .where_clause_pos
                                                .0,
                                        )
                                })
                        })
            })
        })
    };
    let rows_per_build_row = if keys_cover_unique_build_key {
        probe_cardinality / max_distinct_build_keys.max(1.0)
    } else {
        probe_cardinality * join_selectivity
    };
    let estimated_rows_per_outer_row = match hash_join_type {
        HashJoinType::Inner => rows_per_build_row,
        HashJoinType::LeftOuter => rows_per_build_row.max(1.0),
        HashJoinType::FullOuter => rows_per_build_row
            .max(1.0)
            .max(probe_cardinality / build_cardinality.max(1.0)),
        // Each build row comes out at most once, without a probe row.
        HashJoinType::LeftAnti => 1.0,
    };
    let cost = estimate_hash_join_cost(
        build_cardinality,
        probe_cardinality,
        build_cardinality * estimated_rows_per_outer_row * probe_multiplier,
        hash_join_type,
        crate::vdbe::hash_table::DEFAULT_MEM_BUDGET,
        probe_multiplier,
        params,
    );
    Some(AccessMethod {
        cost,
        estimated_rows_per_outer_row,
        consumed_where_terms: join_keys
            .iter()
            .map(|key| key.where_clause_idx)
            .try_collect()
            .ok()?,
        params: AccessMethodParams::HashJoin {
            build_table_idx: build,
            probe_table_idx: probe,
            join_keys: join_keys.into_vec(),
            materialize_build_input: false,
            join_type: hash_join_type,
        },
    })
}

/// The hash-join candidate of a join step: when the sources before `rhs`
/// end with a table read by a plain scan, a hash join
/// may build a hash table from that table and scan `rhs`, probing it. It is
/// chosen when it costs less than `nested_loop`, the best way to read `rhs`
/// in a nested loop.
#[allow(clippy::too_many_arguments)]
fn hir_hash_join_candidate(
    document: &HirDocument,
    sources: &[HirPlanSource],
    constraints: &[HirTableConstraints],
    where_clause: &[HirWhereTerm],
    base_table_rows: &[RowCountEstimate],
    rhs: usize,
    lhs_mask: &TableMask,
    lhs_steps: &[(usize, &AccessMethod)],
    nested_loop: &AccessMethod,
    ready_where: &[(usize, usize)],
    input_cardinality: f64,
    hash: &HirHashJoins<'_>,
    schema: &Schema,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    use crate::translate::semantic::hir::JoinKind;
    if !hash.allowed {
        return Ok(None);
    }
    // Left-deep: the last table joined becomes the build side.
    let Some(&(build, build_method)) = lhs_steps.last() else {
        return Ok(None);
    };
    let rhs_has_selective_seek = matches!(
        &nested_loop.params,
        AccessMethodParams::BTreeTable {
            index,
            build_index,
            constraint_refs,
            ..
        } if !constraint_refs.is_empty()
            && !build_index
            && index.as_ref().is_none_or(|index| !index.ephemeral)
    );
    // The probe table must not be the build table of an earlier hash join.
    let probe_is_prior_build = lhs_steps.iter().any(|(_, method)| {
        matches!(&method.params, AccessMethodParams::HashJoin { build_table_idx, .. }
            if *build_table_idx == rhs)
    });
    let Some(build_source) = sources[build].btree() else {
        return Ok(None);
    };
    let build_has_rowid = build_source
        .table
        .btree()
        .is_some_and(|table| table.has_rowid);
    let build_access_method_uses_constraints = matches!(
        &build_method.params,
        AccessMethodParams::BTreeTable { build_index, constraint_refs, .. }
            if *build_index || !constraint_refs.is_empty()
    );
    let build_constraints = &constraints[build];
    let base_rows = |position: usize| {
        base_table_rows
            .get(position)
            .copied()
            .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params))
    };
    let build_base_rows = base_rows(build);
    let build_self_selectivity = build_self_constraint_selectivity(build_constraints, build);
    let build_cardinality = *build_base_rows * build_self_selectivity;
    let probe_cardinality = *base_rows(rhs);
    let mut prior_mask = lhs_mask.try_clone()?;
    prior_mask.clear(build);
    let prior_constraint_selectivity =
        build_prior_constraint_selectivity(build_constraints, &prior_mask);
    let probe_multiplier = if lhs_steps.len() == 1 && lhs_steps[0].0 == build {
        1.0
    } else {
        let join_selectivity = prior_constraint_selectivity.clamp(0.0, 1.0);
        let denom = (build_cardinality * join_selectivity).max(1.0);
        (input_cardinality / denom).max(1.0)
    };
    // The build tables of earlier hash joins whose probe was this build
    // table: their join keys already constrain it.
    let prior_hash_build_mask: TableMask = lhs_steps
        .iter()
        .filter_map(|(_, method)| match &method.params {
            AccessMethodParams::HashJoin {
                build_table_idx,
                probe_table_idx,
                ..
            } if *probe_table_idx == build => Some(*build_table_idx),
            _ => None,
        })
        .try_collect()?;
    let build_has_prior_constraints = build_constraints.constraints.iter().any(|constraint| {
        constraint.lhs_mask.intersects(&prior_mask)
            && (0..64).any(|position| {
                constraint.lhs_mask.get(position)
                    && prior_mask.get(position)
                    && !prior_hash_build_mask.get(position)
            })
    });
    let build_table_is_prior_probe = lhs_steps.iter().any(|(_, method)| {
        matches!(&method.params, AccessMethodParams::HashJoin { probe_table_idx, .. }
            if *probe_table_idx == build)
    });
    let prefix_has_outer = lhs_steps.iter().any(|(position, _)| {
        sources[*position]
            .join_info()
            .is_some_and(|join| matches!(join.kind, JoinKind::Left | JoinKind::Full))
    });
    let chaining_across_outer = build_table_is_prior_probe && prefix_has_outer;
    let build_am_is_plain_table_scan = matches!(
        &build_method.params,
        AccessMethodParams::BTreeTable { build_index, constraint_refs, .. }
            if !build_index && constraint_refs.is_empty()
    );
    let build_read_is_in_seek = matches!(&build_method.params, AccessMethodParams::InSeek { .. });
    let build_read_is_unique_seek = matches!(
        &build_method.params,
        AccessMethodParams::BTreeTable {
            index,
            build_index: false,
            constraint_refs,
            ..
        } if index_access_is_unique_point_lookup(index.as_deref(), constraint_refs)
    );
    // Rows from earlier loops can repeat a key, so the build has at most as
    // many distinct keys as either input.
    let max_distinct_build_keys = if lhs_steps.len() > 1 && build_read_is_unique_seek {
        input_cardinality.min(*build_base_rows)
    } else {
        *build_base_rows
    };
    // An index on the build's join column is kept only when the probe has
    // a constant filter of its own, or the build is read by an IN seek.
    let hash_can_replace_build_index = build_read_is_in_seek
        || !constraints[rhs]
            .constraints
            .iter()
            .any(|constraint| constraint.lhs_mask.is_empty());
    let allow_hash_join = !rhs_has_selective_seek
        && !probe_is_prior_build
        && (!build_has_prior_constraints || build_has_rowid)
        && !chaining_across_outer;
    if !allow_hash_join {
        return Ok(None);
    }
    let Some(mut method) = hir_try_hash_join_access_method(
        document,
        sources,
        constraints,
        where_clause,
        build,
        rhs,
        lhs_mask,
        max_distinct_build_keys,
        build_cardinality,
        probe_cardinality,
        probe_multiplier,
        hash_can_replace_build_index,
        hash,
        schema,
        params,
    ) else {
        return Ok(None);
    };
    let AccessMethodParams::HashJoin { join_keys, .. } = &method.params else {
        unreachable!("a hash join access method");
    };
    // HIR runs the unmatched rows of one RIGHT, FULL or outer hash join per
    // block after its loops: it NULL-fills every loop before the join's
    // right side and runs the loops after it again. So, in a block with a
    // RIGHT or FULL join that is not this one, only an inner hash join of
    // tables on that join's left side hashes.
    let AccessMethodParams::HashJoin { join_type, .. } = &method.params else {
        unreachable!("a hash join access method");
    };
    let other_outer = sources.iter().enumerate().any(|(position, source)| {
        position != rhs
            && source
                .join_info()
                .is_some_and(|join| matches!(join.kind, JoinKind::Right | JoinKind::Full))
            && (*join_type != HashJoinType::Inner || position < rhs || position < build)
    });
    if other_outer {
        return Ok(None);
    }
    // A build read that keeps only some rows (a constraint, a prior join
    // term the keys do not cover, an earlier probe of this table, an IN
    // seek) must build from the rows the loops before the probe keep:
    // the hash table is filled from that join prefix, stored once.
    let needs_materialization = build_has_uncovered_prior_constraints(
        build_constraints,
        join_keys,
        &prior_mask,
        &prior_hash_build_mask,
    ) || build_table_is_prior_probe
        || build_read_is_in_seek
        || build_access_method_uses_constraints;
    let estimated_materialized_rows = input_cardinality;
    let materialization_too_large =
        needs_materialization && estimated_materialized_rows > MAX_MATERIALIZED_BUILD_ROWS;
    let build_is_eligible = build_am_is_plain_table_scan || needs_materialization;
    let is_full_outer = matches!(
        &method.params,
        AccessMethodParams::HashJoin {
            join_type: HashJoinType::FullOuter,
            ..
        }
    );
    // A build input holds the values its terms computed once; a term that
    // reads the outer query's row would be stale for the next outer row.
    let materialization_reads_an_outer_query = needs_materialization
        && where_clause.iter().any(|term| {
            let mut inside = true;
            let mut reads_outer = false;
            document.visit_expr_sources(&term.expr, &mut |source| match sources
                .iter()
                .position(|planned| planned.source() == source)
            {
                Some(position) => inside &= lhs_mask.get(position),
                None => reads_outer = true,
            });
            inside && reads_outer
        });
    if !build_is_eligible
        || (needs_materialization && !build_has_rowid)
        || (materialization_too_large && !is_full_outer)
        || materialization_reads_an_outer_query
    {
        return Ok(None);
    }
    let should_materialize = needs_materialization && build_has_rowid;
    // Under earlier loops, an outer hash join runs its unmatched build rows
    // after each pass of the probe loop, with the earlier loops' rows. That
    // is right for LEFT, whose build rows pair with those rows, but not for
    // the probe rows FULL adds, which no row of the earlier loops may match
    // and must come out once. A build input folds the earlier loops into
    // the hash table, so the probe loop runs once.
    if lhs_steps.len() > 1
        && !should_materialize
        && sources[rhs]
            .join_info()
            .is_some_and(|join| join.kind == JoinKind::Full)
    {
        return Ok(None);
    }
    if should_materialize {
        if !hash.materialize {
            return Ok(None);
        }
        // The hash table is built from the rows of the loops before the
        // probe, stored once, so it holds the rows those loops keep and the
        // probe scan runs once. Between such plans, the tie-breaker prefers
        // the one whose build includes earlier tables.
        let tie_breaker = prior_mask.iter().min().unwrap_or(0) as f64 * 1.0e-6;
        let join_type = match &method.params {
            AccessMethodParams::HashJoin { join_type, .. } => *join_type,
            _ => unreachable!("a hash join access method"),
        };
        method.cost = estimate_hash_join_cost(
            estimated_materialized_rows,
            probe_cardinality,
            estimated_materialized_rows * method.estimated_rows_per_outer_row,
            join_type,
            crate::vdbe::hash_table::DEFAULT_MEM_BUDGET,
            1.0,
            params,
        ) + Cost(estimated_materialized_rows * 0.003)
            + Cost(tie_breaker);
        let AccessMethodParams::HashJoin {
            materialize_build_input,
            ..
        } = &mut method.params
        else {
            unreachable!("a hash join access method");
        };
        *materialize_build_input = true;
    }
    add_where_cost(&mut method, ready_where, input_cardinality, params);
    // FULL OUTER requires a hash join for the unmatched-build scan.
    Ok((is_full_outer || method.cost < nested_loop.cost).then_some(method))
}

/// The tables that may start the join, best first, scored like
/// [find_best_starting_source].
#[allow(clippy::too_many_arguments)]
fn hir_starting_sources_by_score(
    document: &HirDocument,
    sources: &[HirPlanSource],
    constraints: &[HirTableConstraints],
    base_table_rows: &[RowCountEstimate],
    source_costs: &[Cost],
    ordering_restrictions: &JoinOrderingRestrictions,
    boundaries: &HirJoinBoundaries<'_>,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> Result<Vec<usize>> {
    let multipliers = compute_indexed_seek_benefits_optional(
        sources.len(),
        constraints,
        base_table_rows,
        ordering_restrictions,
        analyze_stats,
        params,
        |position| {
            let source = sources[position].btree()?;
            Some(HirAccessSource::new(
                source,
                document
                    .source(source.internal_id)
                    .expect("validated HIR contains referenced source"),
            ))
        },
    )?;

    let empty = TableMask::default();
    let mut candidates: Vec<(usize, f64)> = Vec::new();
    for position in 0..sources.len() {
        if !boundaries.allows_candidate(position, &empty) {
            continue;
        }
        if ordering_restrictions.required_lhs(position).is_some() {
            continue;
        }
        let source_work = source_costs[position].0 + *base_table_rows[position];
        let score = source_work
            * build_self_constraint_selectivity(&constraints[position], position)
            * multipliers[position];
        candidates.push((position, score));
    }
    // Stable, so equal scores keep FROM order as before.
    candidates.sort_by(|(_, left), (_, right)| left.total_cmp(right));
    turso_assert!(!candidates.is_empty(), "no valid starting HIR source");
    Ok(candidates
        .into_iter()
        .map(|(position, _)| position)
        .collect())
}

#[derive(Debug, Default, Clone, Copy)]
struct IndexedSeekBenefit {
    /// measures how much choosing this table first enables indexed seeks on other tables.
    /// Higher is better for a starting table.
    reward: f64,
    /// measures how much this table prefers some other table to be joined first
    /// so it can be reached through an indexed seek.
    /// Higher indicates that it is a poor starting choice.
    penalty: f64,
}

impl IndexedSeekBenefit {
    fn multiplier(&self) -> f64 {
        (1.0 + self.penalty) / (1.0 + self.reward)
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_indexed_seek_benefits_optional<E, A>(
    num_sources: usize,
    constraints: &[TableConstraints<E>],
    base_table_rows: &[RowCountEstimate],
    ordering_restrictions: &JoinOrderingRestrictions,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
    mut access_source: impl FnMut(usize) -> Option<A>,
) -> Result<Vec<f64>>
where
    A: AccessSource,
{
    let mut benefits = vec![IndexedSeekBenefit::default(); num_sources];

    let mut total_constant_score = 0.0;
    let mut constant_scores = vec![0.0; num_sources];
    let empty_lhs_mask = TableMask::default();

    for rhs in 0..num_sources {
        let Some(rhs_source) = access_source(rhs) else {
            continue;
        };
        let rhs_constraints = &constraints[rhs];
        let rhs_base_rows = base_table_rows[rhs];

        if let Some(deps) = ordering_restrictions.required_lhs(rhs) {
            if deps.count() > 1 {
                continue;
            }
            if let Some(dep_t) = deps.iter().next() {
                let mut lhs_mask = TableMask::default();
                lhs_mask.set(dep_t)?;
                let score = get_best_seek_score(
                    rhs_constraints,
                    &lhs_mask,
                    rhs,
                    &rhs_source,
                    rhs_base_rows,
                    analyze_stats,
                    params,
                );
                if score > 0.0 {
                    benefits[dep_t].reward += score;
                    benefits[rhs].penalty += score;
                }
                continue;
            }
        }

        let mut potential_predecessors = TableMask::default();
        for candidate in &rhs_constraints.candidates {
            for cref in &candidate.refs {
                // Only the first index column can be enabled for an index
                // seek by joining a single table first.
                if cref.index_col_pos > 0 {
                    break;
                }
                let c = &rhs_constraints.constraints[cref.constraint_vec_pos];
                if c.lhs_mask.count() != 1 {
                    continue;
                }
                let Some(t) = c.lhs_mask.iter().next() else {
                    continue;
                };
                potential_predecessors.set(t)?;
            }
        }

        let constant_score = get_best_seek_score(
            rhs_constraints,
            &empty_lhs_mask,
            rhs,
            &rhs_source,
            rhs_base_rows,
            analyze_stats,
            params,
        );
        if constant_score > 0.0 {
            total_constant_score += constant_score;
            constant_scores[rhs] = constant_score;
            benefits[rhs].penalty += constant_score * (num_sources.saturating_sub(1)) as f64;
        }

        for t in potential_predecessors.iter() {
            if t == rhs {
                continue;
            }
            let mut lhs_mask = TableMask::default();
            lhs_mask.set(t)?;
            let specific_score = get_best_seek_score(
                rhs_constraints,
                &lhs_mask,
                rhs,
                &rhs_source,
                rhs_base_rows,
                analyze_stats,
                params,
            );
            let delta = specific_score - constant_score;
            if delta != 0.0 {
                benefits[t].reward += delta;
                benefits[rhs].penalty += delta;
            }
        }
    }

    for start in 0..num_sources {
        benefits[start].reward += total_constant_score - constant_scores[start];
    }

    Ok(benefits.into_iter().map(|b| b.multiplier()).collect())
}

/// Estimate how much `lhs_mask` improves indexed access to `rhs`.
///
/// Higher is better, based on estimated row reduction:
///
/// `ln(base_rows / estimated_rows_per_seek)`
fn get_best_seek_score<E>(
    rhs_constraints: &TableConstraints<E>,
    lhs_mask: &TableMask,
    rhs: usize,
    rhs_source: &impl AccessSource,
    base_row_count: RowCountEstimate,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> f64 {
    let rhs_table = rhs_source.table();
    let mut best_score = 0.0;
    for candidate in &rhs_constraints.candidates {
        let usable_constraint_refs = usable_constraints_for_lhs_mask(
            &rhs_constraints.constraints,
            &candidate.refs,
            lhs_mask,
            rhs,
        );
        if usable_constraint_refs.is_empty() {
            continue;
        }

        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_source.index_is_covering(index),
                column_count: index.columns.len(),
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    index.columns.len(),
                    rhs_table,
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
            table_name: rhs_table.get_name(),
            index: candidate.index.as_ref(),
            stats: analyze_stats,
        };
        let estimated_rows = estimate_rows_per_seek(
            index_info,
            &rhs_constraints.constraints,
            &usable_constraint_refs,
            base_row_count,
            Some(&analyze_ctx),
        )
        .max(1.0);
        let base_rows = (*base_row_count).max(1.0);
        let score = (base_rows / estimated_rows).ln().max(0.0);
        if score > best_score {
            best_score = score;
        }
    }
    best_score
}

fn build_hir_predicate_work(
    document: &HirDocument,
    from: &impl HirFromScope,
    terms: &[HirWhereTerm],
) -> Result<Vec<PredicateWorkInfo>> {
    terms
        .iter()
        .map(|term| {
            Ok(PredicateWorkInfo {
                table_mask: table_mask_from_hir_scope(document, from, &term.expr)?,
                extra_steps: hir_where_expr_steps(&term.expr).saturating_sub(1),
            })
        })
        .collect()
}

fn ready_predicate_work_by_owner<'work, E, S: Copy>(
    terms: &[PredicateTerm<E, S>],
    work: impl IntoIterator<Item = &'work PredicateWorkInfo>,
    joined_mask: &TableMask,
    rhs_position: usize,
    owner_is_ready: impl Fn(S) -> bool,
) -> SmallVec<[(usize, usize); 4]> {
    terms
        .iter()
        .zip(work)
        .enumerate()
        .filter_map(|(term_idx, (term, info))| {
            if term.consumed || info.extra_steps == 0 {
                return None;
            }
            let ready = match term.from_outer_join {
                Some(owner) => owner_is_ready(owner),
                None => {
                    info.table_mask.get(rhs_position)
                        && joined_mask.contains_all_set_bits_of(&info.table_mask)
                }
            };
            ready.then_some((term_idx, info.extra_steps))
        })
        .collect()
}

/// Iterator that generates all possible size k bitmasks for a given number of tables.
/// For example, given: 3 tables and k=2, the bitmasks are:
/// - 0b011 (tables 0, 1)
/// - 0b101 (tables 0, 2)
/// - 0b110 (tables 1, 2)
///
/// This is used in the dynamic programming approach to finding the best way to join a subset of N tables.
struct JoinBitmaskIter {
    current: u128,
    max_exclusive: u128,
}

impl JoinBitmaskIter {
    fn new(table_number_max_exclusive: usize, how_many: usize) -> Self {
        Self {
            current: (1 << how_many) - 1, // Start with smallest k-bit number (e.g., 000111 for k=3)
            max_exclusive: 1 << table_number_max_exclusive,
        }
    }
}

impl Iterator for JoinBitmaskIter {
    type Item = Result<TableMask, TryReserveError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.max_exclusive {
            return None;
        }

        let result = match TableMask::try_from(self.current) {
            Ok(res) => res,
            Err(e) => return Some(Err(e)),
        };

        // Gosper's hack: compute next k-bit combination in lexicographic order
        let c = self.current & (!self.current + 1); // rightmost set bit
        let r = self.current + c; // add it to get a carry
        let ones = self.current ^ r; // changed bits
        let ones = (ones >> 2) / c; // right-adjust shifted bits
        self.current = r | ones; // form the next combination

        Some(Ok(result))
    }
}

/// Generate all possible bitmasks of size `how_many` for a given number of tables.
fn generate_join_bitmasks(table_number_max_exclusive: usize, how_many: usize) -> JoinBitmaskIter {
    JoinBitmaskIter::new(table_number_max_exclusive, how_many)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use turso_parser::ast::{self, Expr, Operator};

    use super::*;
    use crate::alloc::TursoSliceExt;
    use crate::{
        schema::{
            BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table,
            Type,
        },
        stats::AnalyzeStats,
        translate::{
            optimizer::{
                access_method::AccessMethodParams,
                constraints::{
                    BinaryExprSide, Constraint, ConstraintOperator, ConstraintRef,
                    ConstraintUseCandidate, RangeConstraintRef,
                },
                cost_params::DEFAULT_PARAMS,
            },
            plan::{
                ColumnUsedMask, HirFromGroupBoundary, HirJoinInfo, HirPlannedSource,
                IterationDirection,
            },
            semantic::hir::{self, IndexHint, JoinKind, SourceId},
        },
    };

    #[test]
    fn hash_join_cost_includes_probe_scan() {
        let params = &DEFAULT_PARAMS;
        let cost = estimate_hash_join_cost(
            100.0,
            1_000.0,
            250.0,
            HashJoinType::Inner,
            usize::MAX,
            1.0,
            params,
        );
        let expected = 100.0 * (params.hash_cpu_cost + params.hash_insert_cost)
            + 1_000.0 / params.rows_per_table_page
            + 1_000.0 * params.cpu_cost_per_row
            + 1_000.0 * (params.hash_cpu_cost + params.hash_lookup_cost)
            + 250.0 * params.cpu_cost_per_row;
        assert!((cost.0 - expected).abs() < f64::EPSILON);
    }

    #[test]
    fn hir_group_predicate_owner_is_ready_only_when_current_leaf_completes_group() -> Result<()> {
        let sources = (0..3)
            .map(|id| {
                HirPlanSource::BTree(_create_hir_source(
                    _create_btree_table(
                        &format!("source_{id}"),
                        _create_column_list(&["value"], Type::Integer),
                    ),
                    None,
                    SourceId::new(id),
                ))
            })
            .collect::<Vec<_>>();
        let outer_group = SourceId::new(10);
        let first_leaf_group = SourceId::new(11);
        let last_leaf_group = SourceId::new(12);
        let groups = vec![
            HirFromGroupBoundary {
                source: first_leaf_group,
                parent: Some(outer_group),
                source_range: 1..2,
                join_info: None,
            },
            HirFromGroupBoundary {
                source: last_leaf_group,
                parent: Some(outer_group),
                source_range: 2..3,
                join_info: None,
            },
            HirFromGroupBoundary {
                source: outer_group,
                parent: None,
                source_range: 1..3,
                join_info: None,
            },
        ];
        let layout = HirFromLayout::new(&sources, &groups);
        let terms = [HirWhereTerm {
            expr: hir::Expr::Literal(ast::Literal::Numeric("1".into())),
            from_outer_join: Some(outer_group),
            consumed: false,
        }];
        let work = [PredicateWorkInfo {
            table_mask: TableMask::default(),
            extra_steps: 1,
        }];

        let mut joined = TableMask::default();
        joined.set(1)?;
        assert!(layout.predicate_owner_ready(first_leaf_group, &joined, 1));
        assert!(!layout.predicate_owner_ready(outer_group, &joined, 1));
        assert!(
            ready_predicate_work_by_owner(&terms, &work, &joined, 1, |owner| {
                layout.predicate_owner_ready(owner, &joined, 1)
            })
            .is_empty()
        );

        joined.set(2)?;
        assert!(layout.predicate_owner_ready(last_leaf_group, &joined, 2));
        assert!(layout.predicate_owner_ready(outer_group, &joined, 2));
        assert_eq!(
            ready_predicate_work_by_owner(&terms, &work, &joined, 2, |owner| {
                layout.predicate_owner_ready(owner, &joined, 2)
            })
            .as_slice(),
            [(0, 1)]
        );
        assert!(!layout.predicate_owner_ready(outer_group, &joined, 0));
        assert!(layout.predicate_owner_ready(SourceId::new(0), &joined, 0));
        assert!(!layout.predicate_owner_ready(SourceId::new(99), &joined, 2));
        Ok(())
    }

    #[test]
    fn later_hir_join_step_uses_constraints_from_joined_sources() -> Result<()> {
        let first = SourceId::new(0);
        let second = SourceId::new(1);
        let first_table =
            _create_btree_table("first", _create_column_list(&["value"], Type::Integer));
        let second_table =
            _create_btree_table("second", _create_column_list(&["value"], Type::Integer));
        let first_source = _create_hir_source(first_table, None, first);
        let mut second_source = _create_hir_source(second_table, Some(JoinKind::Inner), second);
        second_source.col_used_mask.set(0)?;
        let sources = vec![first_source, second_source]
            .into_iter()
            .map(HirPlanSource::BTree)
            .collect::<Vec<_>>();
        let document = HirDocument {
            snapshot: hir::CatalogSnapshot::from_id(0),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: first,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources: vec![
                _create_hir_definition("first", first),
                _create_hir_definition("second", second),
            ],
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        };
        let from = hir::From {
            first,
            joins: vec![hir::Join {
                right: second,
                kind: JoinKind::Inner,
                constraint: hir::JoinConstraint::None,
            }],
        };
        let where_clause = vec![HirWhereTerm {
            expr: hir::Expr::Binary {
                lhs: Box::new(hir::Expr::column(second, 0)),
                operator: Operator::Equals,
                rhs: Box::new(hir::Expr::column(first, 0)),
                array_concat: false,
                custom: None,
                comparison: None,
            },
            from_outer_join: None,
            consumed: false,
        }];
        let constraints = vec![_hir_empty_constraints(), _hir_auto_index_constraints(0, 0)?];
        let mut lhs_mask = TableMask::default();
        lhs_mask.set(0)?;
        let predicate_work = build_hir_predicate_work(&document, &from, &where_clause)?;
        let layout = HirFromLayout::new(&sources, &[]);
        let step = hir_join_step(
            &document,
            &sources,
            &layout,
            &constraints,
            &where_clause,
            &predicate_work,
            &[
                RowCountEstimate::AnalyzeStats(1_000.0),
                RowCountEstimate::AnalyzeStats(10_000.0),
            ],
            &[Cost(0.0); 2],
            None,
            1,
            &lhs_mask,
            &[],
            1_000.0,
            &Schema::new(),
            &AnalyzeStats::default(),
            &DEFAULT_PARAMS,
            false,
            &HirHashJoins::NONE,
        )?
        .expect("a B-tree source always has a join step");

        assert_eq!(step.source_position, 1);
        assert_eq!(
            (&step.access_method.consumed_where_terms)
                .into_iter()
                .collect::<Vec<_>>(),
            [0]
        );
        assert!(matches!(
            step.access_method.params,
            AccessMethodParams::BTreeTable {
                build_index: true,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn greedy_hir_join_order_respects_ordering_and_uses_join_constraints() -> Result<()> {
        let first = SourceId::new(0);
        let second = SourceId::new(1);
        let third = SourceId::new(2);
        let mut second_source = _create_hir_source(
            _create_btree_table("second", _create_column_list(&["value"], Type::Integer)),
            Some(JoinKind::Left),
            second,
        );
        second_source.col_used_mask.set(0)?;
        let mut third_source = _create_hir_source(
            _create_btree_table("third", _create_column_list(&["value"], Type::Integer)),
            Some(JoinKind::Inner),
            third,
        );
        third_source.col_used_mask.set(0)?;
        let sources = vec![
            _create_hir_source(
                _create_btree_table("first", _create_column_list(&["value"], Type::Integer)),
                None,
                first,
            ),
            second_source,
            third_source,
        ]
        .into_iter()
        .map(HirPlanSource::BTree)
        .collect::<Vec<_>>();
        let document = HirDocument {
            snapshot: hir::CatalogSnapshot::from_id(0),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: first,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources: vec![
                _create_hir_definition("first", first),
                _create_hir_definition("second", second),
                _create_hir_definition("third", third),
            ],
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        };
        let equality = |lhs, rhs| {
            HirWhereTerm::from(hir::Expr::Binary {
                lhs: Box::new(hir::Expr::column(lhs, 0)),
                operator: Operator::Equals,
                rhs: Box::new(hir::Expr::column(rhs, 0)),
                array_concat: false,
                custom: None,
                comparison: None,
            })
        };
        let where_clause = vec![equality(second, first), equality(third, second)];
        let constraints = vec![
            _hir_empty_constraints(),
            _hir_auto_index_constraints(0, 0)?,
            _hir_auto_index_constraints(1, 1)?,
        ];
        let mut access_methods = Vec::new();
        let result = compute_hir_greedy_join_order(
            &document,
            &sources,
            &[],
            &constraints,
            &where_clause,
            &[
                RowCountEstimate::AnalyzeStats(1_000.0),
                RowCountEstimate::AnalyzeStats(1.0),
                RowCountEstimate::AnalyzeStats(10_000.0),
            ],
            &[Cost(0.0); 3],
            None,
            1.0,
            &mut access_methods,
            &Schema::new(),
            &AnalyzeStats::default(),
            &DEFAULT_PARAMS,
            false,
            &HirHashJoins::NONE,
        )?
        .expect("three HIR sources produce a join plan");

        assert_eq!(
            result.best_plan.table_numbers().collect::<Vec<_>>(),
            [0, 1, 2]
        );
        let chosen = result
            .best_plan
            .best_access_methods()
            .map(|index| &access_methods[index])
            .collect::<Vec<_>>();
        assert_eq!(
            (&chosen[1].consumed_where_terms)
                .into_iter()
                .collect::<Vec<_>>(),
            [0]
        );
        assert_eq!(
            (&chosen[2].consumed_where_terms)
                .into_iter()
                .collect::<Vec<_>>(),
            [1]
        );
        assert!(chosen[1..].iter().all(|method| matches!(
            method.params,
            AccessMethodParams::BTreeTable {
                build_index: true,
                ..
            }
        )));
        Ok(())
    }

    #[test]
    fn hir_group_boundary_requires_left_side_then_finishes_group() -> Result<()> {
        let groups = [HirFromGroupBoundary {
            source: SourceId::new(10),
            parent: None,
            source_range: 1..3,
            join_info: Some(HirJoinInfo {
                kind: JoinKind::Left,
            }),
        }];
        let restrictions = JoinOrderingRestrictions {
            required_lhs_by_table: None,
        };
        let boundaries = HirJoinBoundaries {
            groups: &groups,
            ordering_restrictions: &restrictions,
        };

        let empty = TableMask::default();
        assert!(boundaries.allows_candidate(0, &empty));
        assert!(!boundaries.allows_candidate(1, &empty));
        assert!(!boundaries.allows_candidate(2, &empty));

        let left: TableMask = [0].into_iter().try_collect()?;
        assert!(boundaries.allows_candidate(1, &left));
        assert!(boundaries.allows_candidate(2, &left));

        let started: TableMask = [0, 2].into_iter().try_collect()?;
        assert!(boundaries.allows_candidate(1, &started));
        assert!(!boundaries.allows_candidate(3, &started));

        let complete: TableMask = [0, 1, 2].into_iter().try_collect()?;
        assert!(boundaries.allows_candidate(3, &complete));
        Ok(())
    }

    #[test]
    fn hir_group_nested_boundary_waits_for_outer_prerequisites() -> Result<()> {
        let outer = SourceId::new(10);
        let groups = [
            HirFromGroupBoundary {
                source: SourceId::new(11),
                parent: Some(outer),
                source_range: 2..4,
                join_info: Some(HirJoinInfo {
                    kind: JoinKind::Left,
                }),
            },
            HirFromGroupBoundary {
                source: outer,
                parent: None,
                source_range: 1..4,
                join_info: None,
            },
        ];
        let restrictions = JoinOrderingRestrictions {
            required_lhs_by_table: None,
        };
        let boundaries = HirJoinBoundaries {
            groups: &groups,
            ordering_restrictions: &restrictions,
        };

        let empty = TableMask::default();
        assert!(!boundaries.allows_candidate(1, &empty));

        let outer_left: TableMask = [0].into_iter().try_collect()?;
        assert!(boundaries.allows_candidate(1, &outer_left));

        let outer_started: TableMask = [0, 1].into_iter().try_collect()?;
        assert!(boundaries.allows_candidate(2, &outer_started));
        assert!(boundaries.allows_candidate(3, &outer_started));
        assert!(!boundaries.allows_candidate(4, &outer_started));
        Ok(())
    }

    #[test]
    fn test_generate_bitmasks() -> std::result::Result<(), TryReserveError> {
        let bitmasks = generate_join_bitmasks(4, 2).collect::<std::result::Result<Vec<_>, _>>()?;
        assert!(bitmasks.contains(&TableMask::try_from(0b0011u128)?)); // {0,1}
        assert!(bitmasks.contains(&TableMask::try_from(0b0101u128)?)); // {0,2}
        assert!(bitmasks.contains(&TableMask::try_from(0b0110u128)?)); // {1,2}
        assert!(bitmasks.contains(&TableMask::try_from(0b1001u128)?)); // {0,3}
        assert!(bitmasks.contains(&TableMask::try_from(0b1010u128)?)); // {1,3}
        assert!(bitmasks.contains(&TableMask::try_from(0b1100u128)?)); // {2,3}
        Ok(())
    }

    struct TestColumn {
        name: String,
        ty: Type,
        is_rowid_alias: bool,
    }

    impl Default for TestColumn {
        fn default() -> Self {
            Self {
                name: "a".to_string(),
                ty: Type::Integer,
                is_rowid_alias: false,
            }
        }
    }

    fn _create_column(c: &TestColumn) -> Column {
        Column::new(
            Some(c.name.clone()),
            c.ty.to_string(),
            None,
            None,
            c.ty,
            None,
            ColDef {
                flags: if c.is_rowid_alias {
                    crate::schema::ColDefFlags::RowIdAlias
                } else {
                    crate::schema::ColDefFlags::empty()
                },
                ..Default::default()
            },
        )
    }
    fn _create_column_of_type(name: &str, ty: Type) -> Column {
        _create_column(&TestColumn {
            name: name.to_string(),
            ty,
            is_rowid_alias: false,
        })
    }

    fn _create_column_list(names: &[&str], ty: Type) -> Vec<Column> {
        names
            .iter()
            .map(|name| _create_column_of_type(name, ty))
            .collect()
    }

    fn _create_column_rowid_alias(name: &str) -> Column {
        _create_column(&TestColumn {
            name: name.to_string(),
            ty: Type::Integer,
            is_rowid_alias: true,
        })
    }

    /// Creates a BTreeTable with the given name and columns
    fn _create_btree_table(name: &str, columns: Vec<Column>) -> Arc<BTreeTable> {
        Arc::new(BTreeTable::new(
            1, // root_page, doesn't matter for tests
            name.to_string(),
            crate::alloc::vec![],
            columns.try_to_vec().expect(crate::alloc::ALLOC_ERR_MSG),
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ))
    }

    fn _create_index(
        name: &str,
        table_name: &str,
        columns: &[(&str, usize)],
        unique: bool,
    ) -> Arc<Index> {
        Arc::new(Index {
            name: name.to_string(),
            table_name: table_name.to_string(),
            where_clause: None,
            columns: columns
                .iter()
                .map(|(name, pos_in_table)| IndexColumn::new((*name).to_string(), *pos_in_table))
                .try_collect()
                .unwrap(),
            unique,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        })
    }

    fn _create_hir_source(
        table: Arc<BTreeTable>,
        join_kind: Option<JoinKind>,
        internal_id: SourceId,
    ) -> HirPlannedSource {
        let table = Table::BTree(table);
        HirPlannedSource {
            table,
            internal_id,
            join_info: join_kind.map(|kind| HirJoinInfo { kind }),
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            indexed: IndexHint::None,
        }
    }

    fn _create_hir_definition(name: &str, internal_id: SourceId) -> hir::Source {
        hir::Source {
            id: internal_id,
            owner: hir::SourceOwner::Root,
            database: None,
            name: name.to_string(),
            alias: None,
            kind: hir::SourceKind::SchemaExpression,
            columns: Vec::new(),
            generated_expressions: Vec::new(),
            default_expressions: Vec::new(),
            column_type_programs: Vec::new(),
            check_constraints: None,
            rowid_available: true,
            index_hint: IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: hir::IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    /// No constraints: the table can only be scanned through its rowid
    /// order, the candidate every table without INDEXED BY has.
    fn _hir_empty_constraints() -> HirTableConstraints {
        HirTableConstraints {
            constraints: Vec::new(),
            candidates: vec![ConstraintUseCandidate {
                index: None,
                refs: Vec::new(),
                partial_index: None,
            }],
            temporary_index_terms: SmallVec::new(),
        }
    }

    fn _hir_auto_index_constraints(
        where_clause_position: usize,
        lhs_position: usize,
    ) -> Result<HirTableConstraints> {
        let mut lhs_mask = TableMask::default();
        lhs_mask.set(lhs_position)?;
        Ok(HirTableConstraints {
            constraints: vec![Constraint {
                where_clause_pos: (where_clause_position, BinaryExprSide::Rhs),
                operator: ConstraintOperator::from(Operator::Equals),
                table_col_pos: Some(0),
                constraining_expr: None,
                lhs_mask,
                selectivity: DEFAULT_PARAMS.sel_eq_unindexed,
                usable: true,
                is_rowid: false,
                comparison_affinity: Some(crate::vdbe::affinity::Affinity::Integer),
                comparison_collation: None,
                null_matching: false,
                outer_join_compatible: true,
            }],
            candidates: vec![ConstraintUseCandidate {
                index: None,
                refs: Vec::new(),
                partial_index: None,
            }],
            temporary_index_terms: smallvec::smallvec![ConstraintRef {
                constraint_vec_pos: 0,
                index_col_pos: 0,
                sort_order: ast::SortOrder::Asc,
                nulls_order: ast::NullsOrder::First,
            }],
        })
    }

    #[test]
    fn preserved_hir_right_join_keeps_its_left_source_first() -> Result<()> {
        let columns = _create_column_list(&["value"], Type::Integer);
        let sources = vec![
            _create_hir_source(
                _create_btree_table("left", columns.clone()),
                None,
                SourceId::new(0),
            ),
            _create_hir_source(
                _create_btree_table("right", columns),
                Some(JoinKind::Right),
                SourceId::new(1),
            ),
        ]
        .into_iter()
        .map(HirPlanSource::BTree)
        .collect::<Vec<_>>();
        let restrictions = hir_join_ordering_restrictions(&sources)?;

        assert!(restrictions.allows_order(&[0, 1])?);
        assert!(!restrictions.allows_order(&[1, 0])?);
        Ok(())
    }

    /// Creates a numeric literal expression
    fn _create_numeric_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::Numeric(value.to_string()))
    }

    fn _as_btree(
        access_method: &AccessMethod,
    ) -> (
        IterationDirection,
        Option<Arc<Index>>,
        &'_ [RangeConstraintRef],
    ) {
        match &access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index,
                constraint_refs,
                ..
            } => (*iter_dir, index.clone(), constraint_refs),
            _ => panic!("expected BTreeTable access method"),
        }
    }
}
