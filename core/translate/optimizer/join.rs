use std::collections::VecDeque;
use std::sync::Arc;

use crate::{turso_assert_eq, turso_assert_greater_than};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use smallvec::SmallVec;

use turso_parser::ast::{Expr, Operator, TableInternalId};

use super::{
    access_method::{find_best_access_method_for_join_order, AccessMethod},
    constraints::TableConstraints,
    cost_params::CostModelParams,
    order::OrderTarget,
    IndexMethodCandidate,
};
use crate::{
    schema::{Index, Schema},
    stats::AnalyzeStats,
    translate::{
        expr::{walk_expr, WalkControl},
        optimizer::{
            access_method::{
                consider_multi_index_intersection, consider_multi_index_union,
                estimate_hash_join_cost, try_hash_join_access_method, AccessMethodParams,
            },
            cost::{Cost, RowCountEstimate},
            order::plan_satisfies_order_target,
        },
        plan::{
            HashJoinKey, HashJoinType, JoinOrderMember, JoinedTable, NonFromClauseSubquery,
            TableReferences, WhereTerm,
        },
        planner::TableMask,
    },
    LimboError, Result,
};

// Upper bound on rowids to materialize for a hash build input.
// This is a safety limit, not a cost tuning parameter.
const MAX_MATERIALIZED_BUILD_ROWS: f64 = 200_000.0;

/// Represents an n-ary join, anywhere from 1 table to N tables.
#[derive(Debug, Clone)]
pub struct JoinN {
    /// Tuple: (table_number, access_method_index)
    pub data: Vec<(usize, usize)>,
    /// The estimated number of rows returned by joining these n tables together.
    pub output_cardinality: usize,
    /// Estimated execution cost of this N-ary join.
    pub cost: Cost,
}

impl JoinN {
    pub fn table_numbers(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.data.iter().map(|(table_number, _)| *table_number)
    }

    pub fn best_access_methods(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.data
            .iter()
            .map(|(_, access_method_index)| *access_method_index)
    }
}

/// Join n-1 tables with the n'th table.
/// Returns None if the plan is worse than the provided cost upper bound or if no valid access method is found.
///
/// Hash-joins:
/// - We only consider hash join once there is a non-empty LHS.
/// - The build side is the most recently joined table (left-deep hash join); the RHS is the probe.
/// - We avoid hash-join shapes that would drop build-side filters unless we can preserve them
///   via materialized build rowids.
/// - Probe->build chaining is only allowed when the build input is materialized from the
///   join prefix; rebuilding from the full table would ignore prior join filters.
#[allow(clippy::too_many_arguments)]
pub fn join_lhs_and_rhs<'a>(
    lhs: Option<&JoinN>,
    rhs_table_reference: &JoinedTable,
    rhs_constraints: &'a TableConstraints,
    all_constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    access_methods_arena: &'a mut Vec<AccessMethod>,
    cost_upper_bound: Cost,
    joined_tables: &[JoinedTable],
    where_clause: &mut [WhereTerm],
    where_term_table_ids: &[HashSet<TableInternalId>],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<JoinN>> {
    // The input cardinality for this join is the output cardinality of the previous join.
    // For example, in a 2-way join, if the left table has 1000 rows, and the right table will return 2 rows for each of the left table's rows,
    // then the output cardinality of the join will be 2000.
    let input_cardinality = lhs.map_or(1, |l| l.output_cardinality);

    let rhs_table_number = join_order.last().unwrap().original_idx;
    let rhs_base_rows = base_table_rows
        .get(rhs_table_number)
        .copied()
        .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params));

    let Some(mut method) = find_best_access_method_for_join_order(
        rhs_table_reference,
        rhs_constraints,
        join_order,
        maybe_order_target,
        input_cardinality as f64,
        rhs_base_rows,
        params,
    )?
    else {
        return Ok(None);
    };

    // Check if this access method will trigger ephemeral index creation.
    if let AccessMethodParams::BTreeTable {
        index: None,
        constraint_refs,
        ..
    } = &method.params
    {
        if constraint_refs.is_empty() {
            // Check if there are usable constraints that will create an ephemeral index
            let lhs_mask_for_ephemeral = lhs.map_or_else(TableMask::new, |l| {
                TableMask::from_table_number_iter(l.table_numbers())
            });
            let has_usable_constraints = rhs_constraints.constraints.iter().any(|c| {
                c.usable
                    && c.table_col_pos.is_some()
                    && lhs_mask_for_ephemeral.contains_all(&c.lhs_mask)
            });

            if has_usable_constraints && lhs.is_some() {
                // Add ephemeral index build cost: scan the table once to build the index
                // This is similar to the build phase of a hash join
                let ephemeral_build_cost = *rhs_base_rows * 0.003;
                method.cost = method.cost + Cost(ephemeral_build_cost);
            }
        }
    }

    let lhs_cost = lhs.map_or(Cost(0.0), |l| l.cost);
    // If we have a previous table, consider hash join as an alternative
    let mut best_access_method = method;

    // Reuse for multi-index scans, hash cost and output cardinality computation
    let lhs_mask = lhs.map_or_else(TableMask::new, |l| {
        TableMask::from_table_number_iter(l.table_numbers())
    });

    // Consider multi-index scans (OR-by-union and AND-by-intersection) for BTree tables with rowids.
    // Cross-table disjuncts are validated against lhs_mask in the respective consider_ functions.
    if rhs_table_reference.btree().is_some_and(|b| b.has_rowid) {
        // Try OR-by-union
        if let Some(multi_idx_method) = consider_multi_index_union(
            rhs_table_reference,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            input_cardinality as f64,
            rhs_base_rows,
            params,
            best_access_method.cost,
            &lhs_mask,
        ) {
            best_access_method = multi_idx_method;
        }

        // Try AND-by-intersection
        if let Some(multi_idx_and_method) = consider_multi_index_intersection(
            rhs_table_reference,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            input_cardinality as f64,
            rhs_base_rows,
            params,
            best_access_method.cost,
            &lhs_mask,
        ) {
            best_access_method = multi_idx_and_method;
        }
    }

    // Self-constraints are conditions comparing columns within the same table
    // (e.g., t.col1 < t.col2). Include them in selectivity since they filter rows.
    let rhs_self_mask = {
        let mut m = TableMask::new();
        m.add_table(rhs_table_number);
        m
    };

    // ============================================================================
    // OUTPUT CARDINALITY ESTIMATION
    // ============================================================================
    //
    // Estimate output rows to compare join orders in the DP algorithm.
    //
    // CONSTRAINT TYPES:
    // 1. EQUI-JOIN: References LHS tables (e.g., t1.a = t2.b). Enables index seeks.
    // 2. LITERAL: Column vs constant (e.g., t2.x > 5). No table references.
    // 3. SELF: Compares columns within same table (e.g., t2.y = t2.z).
    //
    // TWO MULTIPLIERS:
    // - all_multiplier: All constraints. Used for FULL SCANS.
    // - local_filter_multiplier: Only literal + self-constraints. Used for INDEX/ROWID
    //   SEEKS where equi-join selectivity is already accounted for by the fanout
    //   of the seek itself.
    //
    let (output_cardinality_multiplier, local_filter_multiplier) = {
        let mut all_multiplier = 1.0;
        let mut local_multiplier = 1.0;
        // Track range bounds to detect closed ranges like "x > 5 AND x < 10"
        let mut column_bounds: SmallVec<[(Option<usize>, bool, bool); 4]> = SmallVec::new();

        for c in rhs_constraints.constraints.iter().filter(|c| {
            // Include constraint if:
            // - All tables it references are in LHS (equi-join, can use for seek)
            // - OR it's a self-constraint (compares columns within same table)
            // - OR it's a literal constraint (e.g., col > 5, no table refs)
            lhs_mask.contains_all(&c.lhs_mask)
                || c.lhs_mask == rhs_self_mask
                || c.lhs_mask.is_empty()
        }) {
            all_multiplier *= c.selectivity;

            // Local filters: only self-constraints and literals (NOT equi-joins)
            let is_local_filter = c.lhs_mask == rhs_self_mask || c.lhs_mask.is_empty();
            if is_local_filter {
                local_multiplier *= c.selectivity;
            }

            // Track range bounds per column for closed-range detection
            let dominated_col = c.table_col_pos;
            let dominated_col_is_lower = matches!(
                c.operator.as_ast_operator(),
                Some(Operator::Greater | Operator::GreaterEquals)
            );
            let dominated_col_is_upper = matches!(
                c.operator.as_ast_operator(),
                Some(Operator::Less | Operator::LessEquals)
            );
            if dominated_col_is_lower || dominated_col_is_upper {
                if let Some(entry) = column_bounds
                    .iter_mut()
                    .find(|(col, _, _)| *col == dominated_col)
                {
                    entry.1 |= dominated_col_is_lower;
                    entry.2 |= dominated_col_is_upper;
                } else {
                    column_bounds.push((
                        dominated_col,
                        dominated_col_is_lower,
                        dominated_col_is_upper,
                    ));
                }
            }
        }

        // Apply closed-range bonus for columns with both lower and upper bounds
        for (_, has_lower, has_upper) in &column_bounds {
            if *has_lower && *has_upper {
                all_multiplier *= params.closed_range_selectivity_factor;
                local_multiplier *= params.closed_range_selectivity_factor;
            }
        }

        (all_multiplier, local_multiplier)
    };
    let rhs_internal_id = rhs_table_reference.internal_id;
    let lhs_internal_ids: HashSet<TableInternalId> = lhs
        .map(|l| {
            l.table_numbers()
                .map(|table_no| joined_tables[table_no].internal_id)
                .collect()
        })
        .unwrap_or_default();
    let has_join_constraint = lhs.is_some()
        && where_term_table_ids.iter().any(|table_ids| {
            table_ids.contains(&rhs_internal_id)
                && table_ids.iter().any(|id| lhs_internal_ids.contains(id))
        });
    if lhs.is_some() && !has_join_constraint {
        let rhs_self_constraint_selectivity =
            build_self_constraint_selectivity(rhs_constraints, rhs_table_number);
        // Penalize cross products so we don't introduce a table before it can join.
        let effective_rhs_rows = (*rhs_base_rows) * rhs_self_constraint_selectivity;
        let cross_cost = (input_cardinality as f64) * effective_rhs_rows;
        best_access_method.cost = best_access_method.cost + Cost(cross_cost);
    }

    // If we already have a non-empty LHS (at least one table has been joined),
    // consider a hash-join alternative for the current RHS. This is a left-deep
    // join: the last table in the LHS becomes the build side, and the new RHS
    // table is the probe side. We only allow hash joins when:
    //
    // - The would-be build table is accessed via a scan, or we can preserve its
    //   filters by materializing rowids.
    // - The probe table is not using a selective index seek we’d prefer to keep.
    // - The build table has no remaining constraints from prior tables that are
    //   not already consumed as hash-join keys in earlier hash joins.
    if let Some(lhs) = lhs {
        let rhs_table_idx = join_order.last().unwrap().original_idx;
        let last_lhs_table_idx = join_order[join_order.len() - 2].original_idx;
        let lhs_table_numbers: Vec<usize> = lhs.table_numbers().collect();

        let rhs_has_selective_seek = matches!(
            best_access_method.params,
            AccessMethodParams::BTreeTable {
                ref constraint_refs,
                ..
            } if !constraint_refs.is_empty()
        );

        // The probe table must NOT be the build table of any earlier hash join,
        // otherwise we would need to re-probe a table that is already being
        // produced by a hash build.
        let arena = &access_methods_arena;
        let probe_table_is_prior_build = lhs.data.iter().any(|(_, am_idx)| {
            arena.get(*am_idx).is_some_and(|am| {
                if let AccessMethodParams::HashJoin {
                    build_table_idx, ..
                } = &am.params
                {
                    *build_table_idx == rhs_table_idx
                } else {
                    false
                }
            })
        });

        for build_table_idx in lhs_table_numbers {
            if build_table_idx != last_lhs_table_idx {
                continue;
            }
            let build_table = &joined_tables[build_table_idx];
            let build_has_rowid = build_table.btree().is_some_and(|btree| btree.has_rowid);

            // If the chosen access method for the build table already uses constraints,
            // skip hash join to avoid dropping those filters (unless we later decide
            // to materialize the filtered rowids).
            let build_access_method_uses_constraints = lhs
                .data
                .iter()
                .find(|(table_no, _)| *table_no == build_table_idx)
                .map(|(_, am_idx)| *am_idx)
                .map(|am_idx| {
                    let arena = &access_methods_arena;
                    arena.get(am_idx).is_some_and(|am| {
                        if let AccessMethodParams::BTreeTable {
                            constraint_refs, ..
                        } = &am.params
                        {
                            !constraint_refs.is_empty()
                        } else {
                            false
                        }
                    })
                })
                .unwrap_or(false);

            let build_constraints = &all_constraints[build_table_idx];
            let build_base_rows = base_table_rows
                .get(build_table_idx)
                .copied()
                .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params));
            let build_self_selectivity =
                build_self_constraint_selectivity(build_constraints, build_table_idx);
            let build_cardinality = (*build_base_rows) * build_self_selectivity;
            let probe_cardinality = *rhs_base_rows;
            let prior_mask = lhs_mask.without_table(build_table_idx);
            let prior_constraint_selectivity =
                build_prior_constraint_selectivity(build_constraints, &prior_mask);
            let probe_multiplier = if lhs.data.len() == 1 && lhs.data[0].0 == build_table_idx {
                1.0
            } else {
                let join_selectivity = prior_constraint_selectivity.clamp(0.0, 1.0);
                let denom = (build_cardinality * join_selectivity).max(1.0);
                (input_cardinality as f64 / denom).max(1.0)
            };

            // The build table must NOT have any constraints from prior tables that won't be
            // consumed as hash-join keys. When a table becomes a hash build table, its
            // cursor is exhausted after building. If there are constraints like
            // `prior.x = build.x` that aren't part of the build & probe hash join, they
            // can't be evaluated because the build cursor is no longer positioned.
            //
            // HOWEVER: If the constraint references only tables that are the BUILD side
            // of earlier hash joins where this proposed build table was the PROBE, then
            // that equality is already used as a hash-join key. In that case, when we
            // later SeekRowid into the build table for that earlier join, the cursor is
            // correctly positioned and the constraint is effectively "consumed".
            //
            // Example:
            // `SELECT... items JOIN products ON items.name = products.name JOIN order_items ON products.price = order_items.price`:
            // - First hash join: items(build) - products(probe)
            // - When considering: products(build) - order_items(probe)
            // - products has constraint from items, BUT items is build of an earlier hash join where products was probe
            // - So the constraint IS consumed, and products cursor IS positioned via SeekRowid
            // Get the set of tables that are build tables for hash joins where build_table_idx was probe
            let tables_already_hash_joined_as_build: Vec<usize> = {
                let arena = &access_methods_arena;
                lhs.data
                    .iter()
                    .filter_map(|(_, am_idx)| {
                        arena.get(*am_idx).and_then(|am| {
                            if let AccessMethodParams::HashJoin {
                                build_table_idx: prior_build_table_idx,
                                probe_table_idx,
                                ..
                            } = &am.params
                            {
                                if *probe_table_idx == build_table_idx {
                                    Some(*prior_build_table_idx)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                    })
                    .collect()
            };
            let prior_hash_build_mask = TableMask::from_table_number_iter(
                tables_already_hash_joined_as_build.iter().copied(),
            );

            let build_has_prior_constraints = {
                build_constraints.constraints.iter().any(|c| {
                    // Check if this constraint references prior tables that are NOT already
                    // handled by a hash join where we were the probe table
                    if !c.lhs_mask.intersects(&prior_mask) {
                        return false; // Constraint doesn't reference prior tables
                    }
                    // Check if ALL referenced prior tables are already hash-joined with us as probe
                    // If so, the constraint is consumed and we're OK
                    for table_idx in 0..64 {
                        if c.lhs_mask.contains_table(table_idx)
                            && prior_mask.contains_table(table_idx)
                            && !tables_already_hash_joined_as_build.contains(&table_idx)
                        {
                            // This prior table is NOT handled by a hash join, constraint not consumed
                            return true;
                        }
                    }
                    false // All referenced prior tables are hash-joined
                })
            };

            // If this build table was already a probe in a prior hash join, scanning it again
            // for a hash build would ignore the prior join filters. We currently disallow
            // probe-to-build chaining, even with materialization.
            let build_table_is_prior_probe = lhs.data.iter().any(|(_, am_idx)| {
                let arena = &access_methods_arena;
                arena.get(*am_idx).is_some_and(|am| {
                    if let AccessMethodParams::HashJoin {
                        probe_table_idx, ..
                    } = &am.params
                    {
                        *probe_table_idx == build_table_idx
                    } else {
                        false
                    }
                })
            });
            // Avoid probe->build chaining across outer-join boundaries.
            let prefix_has_outer = join_order
                .iter()
                .take(join_order.len().saturating_sub(1))
                .any(|member| member.is_outer);
            let chaining_across_outer = build_table_is_prior_probe && prefix_has_outer;

            // Hash joins are safe only if we won't drop build-side filters:
            // - If the build scan is unconstrained, we can build directly.
            // - If there are prior/self constraints, we need materialization to preserve them.
            // Full index scans are treated as unconstrained for hash-join eligibility.
            //
            // We intentionally do NOT (yet) allow a table that is already the probe side of
            // a hash join to become the build side of another hash join; the second hash join
            // would rebuild from ALL rows of the middle table, not just the matching rows from the first.
            let build_am_is_plain_table_scan = lhs
                .data
                .iter()
                .find(|(table_no, _)| *table_no == build_table_idx)
                .map(|(_, am_idx)| {
                    let arena = &access_methods_arena;
                    arena.get(*am_idx).is_some_and(|am| {
                        matches!(
                            &am.params,
                            AccessMethodParams::BTreeTable {
                                constraint_refs,
                                ..
                            } if constraint_refs.is_empty()
                        )
                    })
                })
                .unwrap_or(false);

            let build_table_is_last = build_table_idx == last_lhs_table_idx;

            // Eligibility gate: prefer nested-loop when uses a selective probe seek.
            // Probe->build chaining is only allowed when the
            // build input is materialized from the join prefix.
            let allow_hash_join = !rhs_has_selective_seek
                && !probe_table_is_prior_build
                && (!build_has_prior_constraints || build_has_rowid)
                && !chaining_across_outer;

            tracing::debug!(
                lhs_table = build_table.table.get_name(),
                rhs_table = rhs_table_reference.table.get_name(),
                allow_hash_join,
                rhs_has_selective_seek,
                probe_table_is_prior_build,
                build_table_is_prior_probe,
                chaining_across_outer,
                build_am_is_plain_table_scan,
                build_has_rowid,
                "hash-join eligibility check"
            );
            if allow_hash_join {
                let lhs_constraints = build_constraints;
                if let Some(hash_join_method) = try_hash_join_access_method(
                    build_table,
                    rhs_table_reference,
                    build_table_idx,
                    rhs_table_idx,
                    lhs_constraints,
                    rhs_constraints,
                    where_clause,
                    build_cardinality,
                    probe_cardinality,
                    probe_multiplier,
                    subqueries,
                    params,
                ) {
                    let mut hash_join_method = hash_join_method;
                    let mut hash_join_allowed = true;
                    let mem_budget = match &hash_join_method.params {
                        AccessMethodParams::HashJoin { mem_budget, .. } => *mem_budget,
                        _ => unreachable!("hash join params expected"),
                    };
                    if let AccessMethodParams::HashJoin {
                        materialize_build_input,
                        use_bloom_filter,
                        join_keys,
                        ..
                    } = &mut hash_join_method.params
                    {
                        let needs_materialization = build_has_uncovered_prior_constraints(
                            lhs_constraints,
                            join_keys,
                            &prior_mask,
                            &prior_hash_build_mask,
                        ) || build_table_is_prior_probe
                            || !build_table_is_last;
                        let estimated_filtered_rows = (*build_base_rows)
                            * build_self_selectivity
                            * prior_constraint_selectivity;

                        // Hard cap: avoid materializing huge lists when materialization is required.
                        let materialization_too_large = needs_materialization
                            && estimated_filtered_rows > MAX_MATERIALIZED_BUILD_ROWS;
                        let can_materialize =
                            build_has_indexable_prior_constraints(lhs_constraints, &prior_mask);
                        let selectivity_threshold = if probe_multiplier > 1.0 {
                            params.hash_nested_probe_selectivity_threshold
                        } else {
                            params.hash_materialize_selectivity_threshold
                        };
                        // When probe is nested under prior loops, require stricter selectivity
                        // to justify materialization.
                        let wants_materialization = needs_materialization
                            || (build_access_method_uses_constraints
                                && prior_constraint_selectivity < selectivity_threshold);

                        let optional_materialization_too_large = !needs_materialization
                            && wants_materialization
                            && estimated_filtered_rows > MAX_MATERIALIZED_BUILD_ROWS;

                        // Build eligibility: a plain scan is always safe; otherwise we need
                        // materialization or existing constraints that make the scan selective.
                        let build_is_eligible = build_am_is_plain_table_scan
                            || needs_materialization
                            || build_access_method_uses_constraints;

                        hash_join_allowed = build_is_eligible
                            && (!needs_materialization || build_has_rowid)
                            && !materialization_too_large;

                        if hash_join_allowed {
                            let should_materialize = if needs_materialization {
                                build_has_rowid
                            } else {
                                wants_materialization
                                    && build_has_rowid
                                    && can_materialize
                                    && !optional_materialization_too_large
                            };
                            let hash_probe_multiplier = if should_materialize {
                                1.0
                            } else {
                                probe_multiplier
                            };
                            let effective_build_cardinality = if should_materialize {
                                estimated_filtered_rows
                            } else {
                                build_cardinality
                            };
                            // Estimate probe filters that apply only to the probe table itself
                            // (not join predicates) to inform the bloom filter heuristic.
                            let probe_self_selectivity = rhs_constraints
                                .constraints
                                .iter()
                                .filter(|c| c.lhs_mask.is_empty())
                                .map(|c| c.selectivity)
                                .product::<f64>();
                            let probe_filtered_rows =
                                (*rhs_base_rows) * probe_self_selectivity.clamp(0.0, 1.0);
                            if probe_filtered_rows > 0.0 {
                                let build_filtered_rows = if build_is_eligible {
                                    effective_build_cardinality
                                } else {
                                    build_cardinality
                                };
                                // Bloom filters help when the probe side is much larger than the build.
                                *use_bloom_filter = build_filtered_rows > 0.0
                                    && probe_filtered_rows / build_filtered_rows >= 2.0;
                            } else {
                                *use_bloom_filter = false;
                            }
                            if should_materialize {
                                hash_join_method.cost = estimate_hash_join_cost(
                                    effective_build_cardinality,
                                    probe_cardinality,
                                    mem_budget,
                                    hash_probe_multiplier,
                                    params,
                                );
                            }
                            if should_materialize {
                                // Materialize build-side rowids so the hash build only includes
                                // rows that already match prior join constraints.
                                *materialize_build_input = true;
                                let materialize_cost = effective_build_cardinality * 0.003;
                                hash_join_method.cost =
                                    hash_join_method.cost + Cost(materialize_cost);
                                // When two materialized hash-join plans have equal cost,
                                // prefer the one that filters by earlier prior tables.
                                //
                                // This is a deterministic tie-breaker that nudges the planner
                                // toward chaining a more selective prefix without changing
                                // the primary cost model.
                                let tie_breaker =
                                    prior_mask.tables_iter().min().unwrap_or(0) as f64 * 1.0e-6;
                                hash_join_method.cost = hash_join_method.cost + Cost(tie_breaker);
                            } else {
                                *materialize_build_input = false;
                            }
                            tracing::debug!(
                                lhs_table = build_table.table.get_name(),
                                rhs_table = rhs_table_reference.table.get_name(),
                                materialize_build_input = *materialize_build_input,
                                needs_materialization,
                                estimated_filtered_rows,
                                prior_constraint_selectivity,
                                materialization_too_large,
                                can_materialize,
                                build_cardinality,
                                effective_build_cardinality,
                                probe_cardinality,
                                probe_multiplier,
                                hash_probe_multiplier,
                                prior_mask = ?prior_mask,
                                lhs_mask = ?lhs_mask,
                                hash_join_cost = ?hash_join_method.cost,
                                "hash-join candidate"
                            );
                        }
                    }
                    // FULL OUTER requires hash join for the unmatched-build scan.
                    let is_full_outer = matches!(
                        &hash_join_method.params,
                        AccessMethodParams::HashJoin {
                            join_type: HashJoinType::FullOuter,
                            ..
                        }
                    );
                    if hash_join_allowed
                        && (is_full_outer || hash_join_method.cost < best_access_method.cost)
                    {
                        best_access_method = hash_join_method;
                    }
                }
            }
        }
    }

    // Check if there's an index method candidate for this table (e.g., FTS)
    // and compare its cost against the current best access method.
    let mut index_method_estimated_rows: Option<u64> = None;
    if let Some(candidate) = index_method_candidates
        .iter()
        .find(|c| c.table_idx == rhs_table_number)
    {
        if let Some(cost_estimate) = &candidate.cost_estimate {
            // FTS cost depends on whether it's the outer table (no LHS) or inner table
            let fts_cost = if lhs.is_none() {
                // Outer table: FTS cost is fixed
                Cost(cost_estimate.estimated_cost)
            } else {
                // Inner table: FTS cost is multiplied by input cardinality
                Cost(cost_estimate.estimated_cost * input_cardinality as f64)
            };

            if fts_cost < best_access_method.cost {
                best_access_method = AccessMethod {
                    cost: fts_cost,
                    params: AccessMethodParams::IndexMethod {
                        query: candidate.to_query(),
                        where_covered: candidate.where_covered,
                    },
                };
                // Track index_method estimated_rows for output cardinality
                index_method_estimated_rows = Some(cost_estimate.estimated_rows);
            }
        }
    }

    // FULL OUTER needs a hash join. If the optimizer couldn't pick one, bail.
    if lhs.is_some() {
        let is_full_outer = rhs_table_reference
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.full_outer);
        if is_full_outer
            && !matches!(
                best_access_method.params,
                AccessMethodParams::HashJoin {
                    join_type: HashJoinType::FullOuter,
                    ..
                }
            )
        {
            // This ordering can't satisfy FULL OUTER. Let the planner try others.
            return Ok(None);
        }
    }

    let cost = lhs_cost + best_access_method.cost;

    if cost > cost_upper_bound {
        return Ok(None);
    }
    // ============================================================================
    // OUTPUT CARDINALITY CALCULATION
    // ============================================================================
    //
    // Formula: output_rows = input_rows × rows_per_lookup × filter_selectivity
    //
    // ACCESS METHOD TYPES:
    //
    // FULL SCAN (no constraints):
    //   Cartesian product filtered by all constraints.
    //   output = input × rhs_table_size × all_selectivities
    //
    // ROWID SEEK (no index, has constraints):
    //   Primary key lookup returns 0 or 1 row.
    //   output = input × 1 × local_filter_selectivity
    //   (local_filter excludes equi-join selectivity since the seek handles that)
    //
    // INDEX SEEK (has index and constraints):
    //   Secondary index lookup. Rows per lookup = "fanout".
    //   output = input × fanout × local_filter_selectivity
    //
    // FANOUT ESTIMATION:
    //   Fanout = expected rows per index lookup.
    //   - With ANALYZE stats: use avg_rows_per_distinct_prefix[matched_cols - 1]
    //   - Without stats: cost_params.fanout_index_seek_per_unmatched_column^(unmatched_columns) heuristic
    //     Example: 3-column index, match 1 column → 4^2 = 16 rows per lookup
    //   - Full key match: cost_params.fanout_index_seek_unique (unique index) or cost_params.fanout_index_seek_non_unique (non-unique)
    //
    let output_cardinality = if let Some(estimated_rows) = index_method_estimated_rows {
        // Special index methods (e.g., FTS) provide their own row estimate
        (input_cardinality as f64 * estimated_rows as f64).ceil() as usize
    } else if let AccessMethodParams::BTreeTable {
        index,
        constraint_refs,
        ..
    } = &best_access_method.params
    {
        if constraint_refs.is_empty() {
            // FULL SCAN: cartesian product filtered by all constraints
            (input_cardinality as f64 * *rhs_base_rows * output_cardinality_multiplier).ceil()
                as usize
        } else if index.is_none() {
            // ROWID SEEK: exactly 1 row per lookup (primary key is unique)
            // Only apply local filters, not equi-join selectivity (that's the seek)
            (input_cardinality as f64 * local_filter_multiplier * params.fanout_index_seek_unique)
                .ceil() as usize
        } else {
            // INDEX SEEK: fanout depends on matched columns
            let index = index.as_ref().unwrap();
            let matched_cols = constraint_refs.len();
            let total_cols = index.columns.len();
            let unmatched = total_cols.saturating_sub(matched_cols);

            // Try to get actual fanout from ANALYZE statistics (sqlite_stat1).
            // Stats give us avg_rows_per_distinct_prefix[i] = avg rows sharing
            // the same values in first i+1 columns.
            let table_name = rhs_table_reference.table.get_name();
            let stats_fanout = analyze_stats
                .table_stats(table_name)
                .and_then(|ts| ts.index_stats.get(&index.name))
                .and_then(|is| {
                    // matched_cols=1 means we use prefix of length 1, which is index 0
                    if matched_cols > 0 && matched_cols <= is.avg_rows_per_distinct_prefix.len() {
                        Some(is.avg_rows_per_distinct_prefix[matched_cols - 1] as f64)
                    } else {
                        None
                    }
                });

            let fanout = if let Some(f) = stats_fanout {
                // Use actual statistics from ANALYZE
                f
            } else if matched_cols >= total_cols {
                // Full key match: very few rows per lookup
                if index.unique {
                    params.fanout_index_seek_unique // Unique index + full key = exactly 1 row
                } else {
                    params.fanout_index_seek_non_unique // Non-unique but full key = probably few duplicates
                }
            } else {
                // Partial key match: use 4^unmatched heuristic
                // Example: 2-column index, match 1 → 4^1 = 4 rows per lookup
                params
                    .fanout_index_seek_per_unmatched_column
                    .powi(unmatched as i32)
            };

            // Final: input_rows × fanout × local_filters
            (input_cardinality as f64 * fanout * local_filter_multiplier).ceil() as usize
        }
    } else if let AccessMethodParams::MultiIndexScan {
        estimated_rows_per_outer_row,
        ..
    } = &best_access_method.params
    {
        (input_cardinality as f64 * estimated_rows_per_outer_row * local_filter_multiplier).ceil()
            as usize
    } else {
        // HashJoin, VirtualTable, Subquery: use full selectivity formula
        (input_cardinality as f64 * *rhs_base_rows * output_cardinality_multiplier).ceil() as usize
    };

    access_methods_arena.push(best_access_method);

    let mut best_access_methods = Vec::with_capacity(join_order.len());
    best_access_methods.extend(lhs.map_or(vec![], |l| l.data.clone()));
    best_access_methods.push((rhs_table_number, access_methods_arena.len() - 1));

    Ok(Some(JoinN {
        data: best_access_methods,
        output_cardinality,
        cost,
    }))
}

/// Returns true when build-side constraints reference prior tables in ways that
/// are not already consumed by hash-join keys.
fn build_has_uncovered_prior_constraints(
    build_constraints: &TableConstraints,
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
        for table_idx in prior_mask.tables_iter() {
            if constraint.lhs_mask.contains_table(table_idx)
                && !prior_hash_build_mask.contains_table(table_idx)
            {
                return true;
            }
        }
        false
    })
}

/// Estimates selectivity from prior equality constraints on the build side.
fn build_prior_constraint_selectivity(
    build_constraints: &TableConstraints,
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
fn build_self_constraint_selectivity(
    build_constraints: &TableConstraints,
    build_table_idx: usize,
) -> f64 {
    let build_only_mask = TableMask::from_table_number_iter([build_table_idx].into_iter());
    let mut selectivity = 1.0;
    let mut saw_constraint = false;
    for constraint in build_constraints.constraints.iter() {
        if !build_only_mask.contains_all(&constraint.lhs_mask) {
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

/// Returns true if any prior constraints can be turned into an index lookup.
fn build_has_indexable_prior_constraints(
    build_constraints: &TableConstraints,
    prior_mask: &TableMask,
) -> bool {
    build_constraints.candidates.iter().any(|candidate| {
        candidate.refs.iter().any(|constraint_ref| {
            let constraint = &build_constraints.constraints[constraint_ref.constraint_vec_pos];
            constraint.usable && constraint.lhs_mask.intersects(prior_mask)
        })
    })
}

/// The result of [compute_best_join_order].
#[derive(Debug)]
pub struct BestJoinOrderResult {
    /// The best plan overall.
    pub best_plan: JoinN,
    /// The best plan for the given order target, if it isn't the overall best.
    pub best_ordered_plan: Option<JoinN>,
}

/// Compute the best way to join a given set of tables.
/// Returns the best [JoinN] if one exists, otherwise returns None.
#[allow(clippy::too_many_arguments)]
pub fn compute_best_join_order<'a>(
    joined_tables: &[JoinedTable],
    maybe_order_target: Option<&OrderTarget>,
    constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    where_clause: &mut [WhereTerm],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<BestJoinOrderResult>> {
    // Skip work if we have no tables to consider.
    if joined_tables.is_empty() {
        return Ok(None);
    }

    let num_tables = joined_tables.len();

    // For large queries, use greedy join ordering instead of exhaustive DP.
    // The DP algorithm has O(2^n) complexity which becomes prohibitively slow
    // beyond ~12 tables. The greedy algorithm is O(n²) and produces good
    // (though not always optimal) plans.
    let where_term_table_ids = build_where_term_table_ids(where_clause, joined_tables);
    if num_tables > GREEDY_JOIN_THRESHOLD {
        return compute_greedy_join_order(
            joined_tables,
            maybe_order_target,
            constraints,
            base_table_rows,
            access_methods_arena,
            where_clause,
            &where_term_table_ids,
            subqueries,
            index_method_candidates,
            params,
            analyze_stats,
            available_indexes,
            table_references,
            schema,
        );
    }

    // Compute naive left-to-right plan to use as pruning threshold
    let naive_plan = compute_naive_left_deep_plan(
        joined_tables,
        maybe_order_target,
        base_table_rows,
        access_methods_arena,
        constraints,
        where_clause,
        &where_term_table_ids,
        subqueries,
        index_method_candidates,
        params,
        analyze_stats,
        available_indexes,
        table_references,
        schema,
    )?;

    // Keep track of both 1. the best plan overall (not considering sorting), and 2. the best ordered plan (which might not be the same).
    // We assign Some Cost (tm) to any required sort operation, so the best ordered plan may end up being
    // the one we choose, if the cost reduction from avoiding sorting brings it below the cost of the overall best one.
    let mut best_ordered_plan: Option<JoinN> = None;
    let mut best_plan_is_also_ordered = match (naive_plan.as_ref(), maybe_order_target) {
        (Some(plan), Some(order_target)) => plan_satisfies_order_target(
            plan,
            access_methods_arena,
            joined_tables,
            order_target,
            schema,
        ),
        _ => false,
    };

    // If we have one table, then the "naive left-to-right plan" is always the best.
    if joined_tables.len() == 1 {
        return match naive_plan {
            Some(plan) => Ok(Some(BestJoinOrderResult {
                best_plan: plan,
                best_ordered_plan: None,
            })),
            None => Err(LimboError::PlanningError(
                "No valid query plan found".to_string(),
            )),
        };
    }
    let mut best_plan = naive_plan;

    // Reuse a single mutable join order to avoid allocating join orders per permutation.
    let mut join_order = Vec::with_capacity(num_tables);
    join_order.push(JoinOrderMember {
        table_id: TableInternalId::default(),
        original_idx: 0,
        is_outer: false,
    });

    // Keep track of the current best cost so we can short-circuit planning for subplans
    // that already exceed the cost of the current best plan.
    let cost_upper_bound = best_plan.as_ref().map_or(Cost(f64::MAX), |plan| plan.cost);

    // Keep track of the best plan for a given subset of tables.
    // Consider this example: we have tables a,b,c,d to join.
    // if we find that 'b JOIN a' is better than 'a JOIN b', then we don't need to even try
    // to do 'a JOIN b JOIN c', because we know 'b JOIN a JOIN c' is going to be better.
    // This is due to the commutativity and associativity of inner joins.
    // Memo table keyed by a subset mask, then by the last table in the join order.
    //
    // We keep multiple plans per subset instead of only the cheapest one. The cheapest
    // subset plan is not always the best foundation for the next join. Keeping variants
    // lets the planner choose a better join order later (e.g. for hash-join chaining).
    let mut best_plan_memo: HashMap<TableMask, HashMap<usize, JoinN>> =
        HashMap::with_capacity_and_hasher(2usize.pow(num_tables as u32 - 1), Default::default());

    // Dynamic programming base case: calculate the best way to access each single table, as if
    // there were no other tables.
    for i in 0..num_tables {
        let mut mask = TableMask::new();
        mask.add_table(i);
        let table_ref = &joined_tables[i];
        join_order[0] = JoinOrderMember {
            table_id: table_ref.internal_id,
            original_idx: i,
            is_outer: false,
        };
        turso_assert_eq!(join_order.len(), 1);
        let rel = join_lhs_and_rhs(
            None,
            table_ref,
            &constraints[i],
            constraints,
            base_table_rows,
            &join_order,
            maybe_order_target,
            access_methods_arena,
            cost_upper_bound,
            joined_tables,
            where_clause,
            &where_term_table_ids,
            subqueries,
            index_method_candidates,
            params,
            analyze_stats,
            available_indexes,
            table_references,
            schema,
        )?;
        if let Some(rel) = rel {
            best_plan_memo.entry(mask).or_default().insert(i, rel);
        }
    }
    join_order.clear();

    // As mentioned, inner joins are commutative. Outer joins are NOT.
    // Example:
    // "a LEFT JOIN b" can NOT be reordered as "b LEFT JOIN a".
    // If there are outer joins in the plan, ensure correct ordering.
    let left_join_illegal_map = {
        let left_join_count = joined_tables
            .iter()
            .filter(|t| t.join_info.as_ref().is_some_and(|j| j.outer))
            .count();
        if left_join_count == 0 {
            None
        } else {
            // map from rhs table index to lhs table index
            let mut left_join_illegal_map: HashMap<usize, TableMask> =
                HashMap::with_capacity_and_hasher(left_join_count, Default::default());
            for (i, _) in joined_tables.iter().enumerate() {
                for (j, joined_table) in joined_tables.iter().enumerate().skip(i + 1) {
                    if joined_table.join_info.as_ref().is_some_and(|j| j.outer) {
                        // bitwise OR the masks
                        if let Some(illegal_lhs) = left_join_illegal_map.get_mut(&i) {
                            illegal_lhs.add_table(j);
                        } else {
                            let mut mask = TableMask::new();
                            mask.add_table(j);
                            left_join_illegal_map.insert(i, mask);
                        }
                    }
                }
            }
            Some(left_join_illegal_map)
        }
    };

    // Now that we have our single-table base cases, we can start considering join subsets of 2 tables and more.
    // Try to join each single table to each other table.
    for subset_size in 2..=num_tables {
        for mask in generate_join_bitmasks(num_tables, subset_size) {
            // Keep track of the best way to join this subset of tables per possible last table.
            // This preserves alternative join orders that may be more expensive for the subset
            // but enable cheaper joins when adding more tables.
            let mut best_for_mask_by_last: HashMap<usize, JoinN> = HashMap::default();
            // Also keep track of the best plan for this subset that orders the rows in an
            // Interesting Way (tm), i.e. allows us to eliminate sort operations downstream.
            let mut best_ordered_for_mask: Option<JoinN> = None;

            // Try to join all subsets (masks) with all other tables.
            // In this block, LHS is always (n-1) tables, and RHS is a single table.
            for rhs_idx in 0..num_tables {
                // If the RHS table isn't a member of this join subset, skip.
                if !mask.contains_table(rhs_idx) {
                    continue;
                }

                // If there are no other tables except RHS, skip.
                let lhs_mask = mask.without_table(rhs_idx);
                if lhs_mask.is_empty() {
                    continue;
                }

                // If this join ordering would violate LEFT JOIN ordering restrictions, skip.
                if let Some(illegal_lhs) = left_join_illegal_map
                    .as_ref()
                    .and_then(|deps| deps.get(&rhs_idx))
                {
                    let legal = !lhs_mask.intersects(illegal_lhs);
                    if !legal {
                        continue; // Don't allow RHS before its LEFT in LEFT JOIN
                    }
                }

                let Some(lhs_variants) = best_plan_memo.get(&lhs_mask) else {
                    continue;
                };

                // Stable iteration keeps tie-breaks consistent across runs.
                let mut lhs_keys: Vec<usize> = lhs_variants.keys().copied().collect();
                lhs_keys.sort_unstable();
                for lhs_key in lhs_keys {
                    let lhs = &lhs_variants[&lhs_key];
                    // Build a JoinOrder out of the table bitmask we are now considering.
                    for table_no in lhs.table_numbers() {
                        join_order.push(JoinOrderMember {
                            table_id: joined_tables[table_no].internal_id,
                            original_idx: table_no,
                            is_outer: joined_tables[table_no]
                                .join_info
                                .as_ref()
                                .is_some_and(|j| j.outer),
                        });
                    }
                    join_order.push(JoinOrderMember {
                        table_id: joined_tables[rhs_idx].internal_id,
                        original_idx: rhs_idx,
                        is_outer: joined_tables[rhs_idx]
                            .join_info
                            .as_ref()
                            .is_some_and(|j| j.outer),
                    });
                    turso_assert_eq!(join_order.len(), subset_size);

                    // Calculate the best way to join LHS with RHS.
                    let rel = join_lhs_and_rhs(
                        Some(lhs),
                        &joined_tables[rhs_idx],
                        &constraints[rhs_idx],
                        constraints,
                        base_table_rows,
                        &join_order,
                        maybe_order_target,
                        access_methods_arena,
                        cost_upper_bound,
                        joined_tables,
                        where_clause,
                        &where_term_table_ids,
                        subqueries,
                        index_method_candidates,
                        params,
                        analyze_stats,
                        available_indexes,
                        table_references,
                        schema,
                    )?;
                    join_order.clear();

                    let Some(rel) = rel else {
                        continue;
                    };

                    let satisfies_order_target = if let Some(order_target) = maybe_order_target {
                        plan_satisfies_order_target(
                            &rel,
                            access_methods_arena,
                            joined_tables,
                            order_target,
                            schema,
                        )
                    } else {
                        false
                    };

                    // If this plan is worse than our overall best, it might still be the best ordered plan.
                    if rel.cost >= cost_upper_bound {
                        // But if it isn't, skip.
                        if !satisfies_order_target {
                            continue;
                        }
                        let existing_ordered_cost: Cost = best_ordered_for_mask
                            .as_ref()
                            .map_or(Cost(f64::MAX), |p: &JoinN| p.cost);
                        if rel.cost < existing_ordered_cost {
                            best_ordered_for_mask = Some(rel);
                        }
                        continue;
                    }

                    let should_replace = match best_for_mask_by_last.get(&rhs_idx) {
                        Some(existing) => rel.cost < existing.cost,
                        None => true,
                    };
                    if should_replace {
                        best_for_mask_by_last.insert(rhs_idx, rel);
                    }
                }
            }

            let has_all_tables = mask.table_count() == num_tables;
            if has_all_tables {
                for rel in best_for_mask_by_last.into_values() {
                    if cost_upper_bound <= rel.cost {
                        continue;
                    }
                    let satisfies_order_target = if let Some(order_target) = maybe_order_target {
                        plan_satisfies_order_target(
                            &rel,
                            access_methods_arena,
                            joined_tables,
                            order_target,
                            schema,
                        )
                    } else {
                        false
                    };
                    if best_plan.as_ref().is_none_or(|plan| rel.cost < plan.cost) {
                        best_plan = Some(rel);
                        best_plan_is_also_ordered = satisfies_order_target;
                    }
                }
                if let Some(rel) = best_ordered_for_mask.take() {
                    let cost = rel.cost;
                    if cost_upper_bound > cost {
                        best_ordered_plan = Some(rel);
                    }
                }
            } else if !best_for_mask_by_last.is_empty() {
                best_plan_memo.insert(mask, best_for_mask_by_last);
            }
        }
    }

    match best_plan {
        Some(best_plan) => Ok(Some(BestJoinOrderResult {
            best_plan,
            best_ordered_plan: if best_plan_is_also_ordered {
                None
            } else {
                best_ordered_plan
            },
        })),
        None => {
            // Give a targeted error for FULL OUTER when no plan was found.
            let has_full_outer = joined_tables
                .iter()
                .any(|t| t.join_info.as_ref().is_some_and(|ji| ji.full_outer));
            if has_full_outer {
                // Distinguish chaining from a missing equi-join condition.
                let build_is_outer = joined_tables.iter().any(|t| {
                    let is_full = t.join_info.as_ref().is_some_and(|ji| ji.full_outer);
                    if !is_full {
                        return false;
                    }
                    // Check if any earlier table (potential build) is also outer.
                    joined_tables.iter().any(|other| {
                        !std::ptr::eq(t, other)
                            && other
                                .join_info
                                .as_ref()
                                .is_some_and(|ji| ji.outer || ji.full_outer)
                    })
                });
                let msg = if build_is_outer {
                    "FULL OUTER JOIN chaining is not yet supported"
                } else {
                    "FULL OUTER JOIN requires an equality condition in the ON clause"
                };
                Err(LimboError::ParseError(msg.to_string()))
            } else {
                Err(LimboError::PlanningError(
                    "No valid query plan found".to_string(),
                ))
            }
        }
    }
}

/// Above this threshold, use greedy O(n²) ordering instead of exhaustive O(2^n) DP.
pub const GREEDY_JOIN_THRESHOLD: usize = 12;

/// Greedy Operator Ordering (GOO) for join optimization. O(n²) time, O(n) space.
///
/// Builds a left-deep join tree by:
/// 1. Starting with the table that has best hub score (enables most index lookups)
/// 2. Greedily adding the remaining table with lowest marginal cost
///
/// Respects outer join ordering constraints.
#[allow(clippy::too_many_arguments)]
pub fn compute_greedy_join_order<'a>(
    joined_tables: &[JoinedTable],
    maybe_order_target: Option<&OrderTarget>,
    constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    where_clause: &mut [WhereTerm],
    where_term_table_ids: &[HashSet<TableInternalId>],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<BestJoinOrderResult>> {
    let num_tables = joined_tables.len();
    if num_tables == 0 {
        return Ok(None);
    }

    // Outer join RHS tables require all preceding tables to be joined first.
    let left_join_deps: HashMap<usize, TableMask> = joined_tables
        .iter()
        .enumerate()
        .filter(|(_, t)| t.join_info.as_ref().is_some_and(|ji| ji.outer))
        .map(|(j, _)| {
            let mut required = TableMask::new();
            for k in 0..j {
                required.add_table(k);
            }
            (j, required)
        })
        .collect();

    let mut remaining: Vec<usize> = (0..num_tables).collect();
    let mut join_order: Vec<JoinOrderMember> = Vec::with_capacity(num_tables);

    // Pick starting table: prefer tables with high "hub score" (referenced by many constraints).
    let first_idx =
        find_best_starting_table(num_tables, constraints, base_table_rows, &left_join_deps);
    let first_table = &joined_tables[first_idx];
    join_order.push(JoinOrderMember {
        table_id: first_table.internal_id,
        original_idx: first_idx,
        is_outer: false, // First table cannot be outer join RHS
    });
    remaining.retain(|&x| x != first_idx);

    let mut current_plan: Option<JoinN> = join_lhs_and_rhs(
        None,
        first_table,
        &constraints[first_idx],
        constraints,
        base_table_rows,
        &join_order,
        maybe_order_target,
        access_methods_arena,
        Cost(f64::MAX),
        joined_tables,
        where_clause,
        where_term_table_ids,
        subqueries,
        index_method_candidates,
        params,
        analyze_stats,
        available_indexes,
        table_references,
        schema,
    )?;

    if current_plan.is_none() {
        return Err(LimboError::PlanningError(
            "No valid query plan found for first table".to_string(),
        ));
    }

    // Greedily add remaining tables, always picking lowest marginal cost.
    while !remaining.is_empty() {
        let current_mask =
            TableMask::from_table_number_iter(join_order.iter().map(|m| m.original_idx));

        // Placeholder for candidate evaluation (avoids cloning)
        join_order.push(JoinOrderMember::default());

        let mut best: Option<(usize, JoinN)> = None;

        let mut has_connected_candidate = false;
        for &idx in &remaining {
            // Outer join RHS requires all preceding tables joined first
            if let Some(required) = left_join_deps.get(&idx) {
                if !current_mask.contains_all(required) {
                    continue;
                }
            }
            let connected = where_term_table_ids.iter().any(|table_ids| {
                let table_id = joined_tables[idx].internal_id;
                table_ids.contains(&table_id)
                    && current_mask
                        .tables_iter()
                        .map(|table_no| joined_tables[table_no].internal_id)
                        .any(|id| table_ids.contains(&id))
            });
            if connected {
                has_connected_candidate = true;
                break;
            }
        }

        for &idx in &remaining {
            // Outer join RHS requires all preceding tables joined first
            if let Some(required) = left_join_deps.get(&idx) {
                if !current_mask.contains_all(required) {
                    continue;
                }
            }
            if has_connected_candidate {
                let connected = where_term_table_ids.iter().any(|table_ids| {
                    let table_id = joined_tables[idx].internal_id;
                    table_ids.contains(&table_id)
                        && current_mask
                            .tables_iter()
                            .map(|table_no| joined_tables[table_no].internal_id)
                            .any(|id| table_ids.contains(&id))
                });
                if !connected {
                    continue;
                }
            }

            let table = &joined_tables[idx];
            let last = join_order.last_mut().unwrap();
            last.table_id = table.internal_id;
            last.original_idx = idx;
            last.is_outer = table.join_info.as_ref().is_some_and(|ji| ji.outer);

            if let Some(plan) = join_lhs_and_rhs(
                current_plan.as_ref(),
                table,
                &constraints[idx],
                constraints,
                base_table_rows,
                &join_order,
                maybe_order_target,
                access_methods_arena,
                Cost(f64::MAX),
                joined_tables,
                where_clause,
                where_term_table_ids,
                subqueries,
                index_method_candidates,
                params,
                analyze_stats,
                available_indexes,
                table_references,
                schema,
            )? {
                if best.as_ref().is_none_or(|(_, b)| plan.cost < b.cost) {
                    best = Some((idx, plan));
                }
            }
        }

        join_order.pop();

        let (next_idx, next_plan) = best.ok_or_else(|| {
            LimboError::PlanningError("Greedy join ordering: no valid next table".to_string())
        })?;

        let next_table = &joined_tables[next_idx];
        join_order.push(JoinOrderMember {
            table_id: next_table.internal_id,
            original_idx: next_idx,
            is_outer: next_table.join_info.as_ref().is_some_and(|ji| ji.outer),
        });
        remaining.retain(|&x| x != next_idx);
        current_plan = Some(next_plan);
    }

    Ok(Some(BestJoinOrderResult {
        best_plan: current_plan.expect("loop invariant: current_plan always Some"),
        best_ordered_plan: None, // Greedy doesn't track ordered variants
    }))
}

/// Select starting table for greedy join ordering.
///
/// Prefers tables with high "hub score": tables referenced by many other tables' usable
/// constraints. Starting with such tables enables index lookups on subsequent joins.
/// E.g., in a star schema, the fact table is referenced by all dimension FKs, so
/// starting there allows all dimensions to use their PK indexes.
///
/// Score = (base_rows * filter_selectivity) / (1 + hub_score)
/// Lower score wins. Outer join RHS tables are excluded (have ordering dependencies).
fn find_best_starting_table(
    num_tables: usize,
    constraints: &[TableConstraints],
    base_table_rows: &[RowCountEstimate],
    left_join_deps: &HashMap<usize, TableMask>,
) -> usize {
    // hub_score[t] = count of usable constraints on OTHER tables that reference t.
    // If we join t first, each such constraint becomes usable for an index lookup.
    let mut hub_score = vec![0usize; num_tables];
    for (t, tc) in constraints.iter().enumerate() {
        for c in &tc.constraints {
            if c.usable && c.table_col_pos.is_some() {
                for other in (0..num_tables).filter(|&x| x != t && c.lhs_mask.contains_table(x)) {
                    hub_score[other] += 1;
                }
            }
        }
    }

    let mut best: Option<(usize, f64)> = None;
    for t in 0..num_tables {
        if left_join_deps.contains_key(&t) {
            continue; // Outer join RHS - cannot be first
        }

        let base_rows = *base_table_rows[t];

        // Self-constraints compare columns within the same table (e.g., t.col1 < t.col2).
        let self_mask = {
            let mut m = TableMask::new();
            m.add_table(t);
            m
        };

        // Include literal constraints (lhs_mask empty) and self-constraints in selectivity
        let selectivity: f64 = constraints[t]
            .constraints
            .iter()
            .filter(|c| c.lhs_mask.is_empty() || c.lhs_mask == self_mask)
            .map(|c| c.selectivity)
            .product();

        let score = base_rows * selectivity / (1.0 + hub_score[t] as f64);

        if best.is_none_or(|(_, s)| score < s) {
            best = Some((t, score));
        }
    }

    // Table 0 can never be outer join RHS, so best is always Some.
    best.expect("no valid starting table").0
}

/// Specialized version of [compute_best_join_order] that just joins tables in the order they are given
/// in the SQL query. This is used as an upper bound for any other plans -- we can give up enumerating
/// permutations if they exceed this cost during enumeration.
#[allow(clippy::too_many_arguments)]
pub fn compute_naive_left_deep_plan<'a>(
    joined_tables: &[JoinedTable],
    maybe_order_target: Option<&OrderTarget>,
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    constraints: &'a [TableConstraints],
    where_clause: &mut [WhereTerm],
    where_term_table_ids: &[HashSet<TableInternalId>],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<JoinN>> {
    let n = joined_tables.len();
    turso_assert_greater_than!(n, 0);

    let join_order = joined_tables
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: t.join_info.as_ref().is_some_and(|j| j.outer),
        })
        .collect::<Vec<_>>();

    // Start with first table
    let mut best_plan = join_lhs_and_rhs(
        None,
        &joined_tables[0],
        &constraints[0],
        constraints,
        base_table_rows,
        &join_order[..1],
        maybe_order_target,
        access_methods_arena,
        Cost(f64::MAX),
        joined_tables,
        where_clause,
        where_term_table_ids,
        subqueries,
        index_method_candidates,
        params,
        analyze_stats,
        available_indexes,
        table_references,
        schema,
    )?;
    if best_plan.is_none() {
        return Ok(None);
    }

    // Add remaining tables one at a time from left to right
    for i in 1..n {
        best_plan = join_lhs_and_rhs(
            best_plan.as_ref(),
            &joined_tables[i],
            &constraints[i],
            constraints,
            base_table_rows,
            &join_order[..=i],
            maybe_order_target,
            access_methods_arena,
            Cost(f64::MAX),
            joined_tables,
            where_clause,
            where_term_table_ids,
            subqueries,
            index_method_candidates,
            params,
            analyze_stats,
            available_indexes,
            table_references,
            schema,
        )?;
        if best_plan.is_none() {
            return Ok(None);
        }
    }

    Ok(best_plan)
}

/// Precompute table IDs referenced by each WHERE term for join-order decisions.
fn build_where_term_table_ids(
    where_clause: &[WhereTerm],
    joined_tables: &[JoinedTable],
) -> Vec<HashSet<TableInternalId>> {
    let joined_ids: HashSet<TableInternalId> =
        joined_tables.iter().map(|t| t.internal_id).collect();
    where_clause
        .iter()
        .map(|term| expr_table_ids_filtered(&term.expr, &joined_ids))
        .collect()
}

/// Collect table IDs from an expression that belong to the joined tables set.
fn expr_table_ids_filtered(
    expr: &Expr,
    joined_ids: &HashSet<TableInternalId>,
) -> HashSet<TableInternalId> {
    let mut tables = HashSet::default();
    let _ = walk_expr(expr, &mut |node| {
        match node {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if joined_ids.contains(table) {
                    tables.insert(*table);
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    tables
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
    type Item = TableMask;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.max_exclusive {
            return None;
        }

        let result = TableMask::from_bits(self.current);

        // Gosper's hack: compute next k-bit combination in lexicographic order
        let c = self.current & (!self.current + 1); // rightmost set bit
        let r = self.current + c; // add it to get a carry
        let ones = self.current ^ r; // changed bits
        let ones = (ones >> 2) / c; // right-adjust shifted bits
        self.current = r | ones; // form the next combination

        Some(result)
    }
}

/// Generate all possible bitmasks of size `how_many` for a given number of tables.
fn generate_join_bitmasks(table_number_max_exclusive: usize, how_many: usize) -> JoinBitmaskIter {
    JoinBitmaskIter::new(table_number_max_exclusive, how_many)
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use turso_parser::ast::{self, Expr, Operator, SortOrder, TableInternalId};

    use super::*;
    use crate::{
        schema::{BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table, Type},
        stats::AnalyzeStats,
        translate::{
            optimizer::{
                access_method::AccessMethodParams,
                constraints::{constraints_from_where_clause, BinaryExprSide, RangeConstraintRef},
                cost_params::DEFAULT_PARAMS,
            },
            plan::{
                ColumnUsedMask, IterationDirection, JoinInfo, Operation, TableReferences, WhereTerm,
            },
            planner::TableMask,
        },
        vdbe::builder::TableRefIdCounter,
    };

    fn default_base_rows(n: usize) -> Vec<RowCountEstimate> {
        vec![RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS); n]
    }

    fn empty_schema() -> Schema {
        Schema::default()
    }

    #[test]
    fn test_generate_bitmasks() {
        let bitmasks = generate_join_bitmasks(4, 2).collect::<Vec<_>>();
        assert!(bitmasks.contains(&TableMask(0b110))); // {0,1} -- first bit is always set to 0 so that a Mask with value 0 means "no tables are referenced".
        assert!(bitmasks.contains(&TableMask(0b1010))); // {0,2}
        assert!(bitmasks.contains(&TableMask(0b1100))); // {1,2}
        assert!(bitmasks.contains(&TableMask(0b10010))); // {0,3}
        assert!(bitmasks.contains(&TableMask(0b10100))); // {1,3}
        assert!(bitmasks.contains(&TableMask(0b11000))); // {2,3}
    }

    #[test]
    /// Test that [compute_best_join_order] returns None when there are no table references.
    fn test_compute_best_join_order_empty() {
        let table_references = TableReferences::new(vec![], vec![]);
        let available_indexes = HashMap::default();
        let mut where_clause = vec![];

        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    /// Test that [compute_best_join_order] returns a table scan access method when the where clause is empty.
    fn test_compute_best_join_order_single_table_no_indexes() {
        let t1 = _create_btree_table("test_table", _create_column_list(&["id"], Type::Integer));
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];
        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = HashMap::default();
        let mut where_clause = vec![];

        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        // SELECT * from test_table
        // expecting best_best_plan() not to do any work due to empty where clause.
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();
        // Should just be a table scan access method
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, _, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
    }

    #[test]
    /// Test that [compute_best_join_order] returns a RowidEq access method when the where clause has an EQ constraint on the rowid alias.
    fn test_compute_best_join_order_single_table_rowid_eq() {
        let t1 = _create_btree_table("test_table", vec![_create_column_rowid_alias("id")]);
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];

        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, true), // table 0, column 0 (rowid)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let available_indexes = HashMap::default();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        // SELECT * FROM test_table WHERE id = 42
        // expecting a RowidEq access method because id is a rowid alias.
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, _, constraint_refs) = _as_btree(access_method);
        assert!(!constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(constraint_refs.len() == 1);
        assert!(
            table_constraints[0].constraints[constraint_refs[0].eq.unwrap()].where_clause_pos
                == (0, BinaryExprSide::Rhs)
        );
    }

    #[test]
    /// Test that [compute_best_join_order] returns an IndexScan access method when the where clause has an EQ constraint on a primary key.
    fn test_compute_best_join_order_single_table_pk_eq() {
        let t1 = _create_btree_table(
            "test_table",
            vec![_create_column_of_type("id", Type::Integer)],
        );
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];

        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, false), // table 0, column 0 (id)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let mut available_indexes = HashMap::default();
        let index = Arc::new(Index {
            name: "sqlite_autoindex_test_table_1".to_string(),
            table_name: "test_table".to_string(),
            where_clause: None,
            columns: vec![IndexColumn {
                name: "id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                collation: None,
                default: None,
                expr: None,
            }],
            unique: true,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
        });
        available_indexes.insert("test_table".to_string(), VecDeque::from([index]));

        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();
        // SELECT * FROM test_table WHERE id = 42
        // expecting an IndexScan access method because id is a primary key with an index
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(!constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "sqlite_autoindex_test_table_1");
        assert!(constraint_refs.len() == 1);
        assert!(
            table_constraints[0].constraints[constraint_refs[0].eq.unwrap()].where_clause_pos
                == (0, BinaryExprSide::Rhs)
        );
    }

    #[test]
    /// Test that [compute_best_join_order] moves the outer table to the inner position when an index can be used on it, but not the original inner table.
    fn test_compute_best_join_order_two_tables() {
        let t1 = _create_btree_table("table1", _create_column_list(&["id"], Type::Integer));
        let t2 = _create_btree_table("table2", _create_column_list(&["id"], Type::Integer));

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    outer: false,
                    full_outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE1: usize = 0;
        const TABLE2: usize = 1;

        let mut available_indexes = HashMap::default();
        // Index on the outer table (table1)
        let index1 = Arc::new(Index {
            name: "index1".to_string(),
            table_name: "table1".to_string(),
            where_clause: None,
            columns: vec![IndexColumn {
                name: "id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                collation: None,
                default: None,
                expr: None,
            }],
            unique: true,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
        });
        available_indexes.insert("table1".to_string(), VecDeque::from([index1]));

        // SELECT * FROM table1 JOIN table2 WHERE table1.id = table2.id
        // expecting table2 to be chosen first due to the index on table1.id
        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[TABLE1].internal_id, 0, false), // table1.id
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE2].internal_id, 0, false), // table2.id
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![1, 0]);
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, _, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        let access_method = &access_methods_arena[best_plan.data[1].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(!constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "index1");
        assert!(constraint_refs.len() == 1);
        assert!(
            table_constraints[TABLE1].constraints[constraint_refs[0].eq.unwrap()].where_clause_pos
                == (0, BinaryExprSide::Rhs)
        );
    }

    #[test]
    /// Test that [compute_best_join_order] returns a sensible order and plan for three tables, each with indexes.
    fn test_compute_best_join_order_three_tables_indexed() {
        let table_orders = _create_btree_table(
            "orders",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("customer_id", Type::Integer),
                _create_column_of_type("total", Type::Integer),
            ],
        );
        let table_customers = _create_btree_table(
            "customers",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("name", Type::Integer),
            ],
        );
        let table_order_items = _create_btree_table(
            "order_items",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("order_id", Type::Integer),
                _create_column_of_type("product_id", Type::Integer),
                _create_column_of_type("quantity", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(table_orders, None, table_id_counter.next()),
            _create_table_reference(
                table_customers,
                Some(JoinInfo {
                    outer: false,
                    full_outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                table_order_items,
                Some(JoinInfo {
                    outer: false,
                    full_outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE_NO_ORDERS: usize = 0;
        const TABLE_NO_CUSTOMERS: usize = 1;
        const TABLE_NO_ORDER_ITEMS: usize = 2;

        let mut available_indexes = HashMap::default();
        ["orders", "customers", "order_items"]
            .iter()
            .for_each(|table_name| {
                // add primary key index called sqlite_autoindex_<tablename>_1
                let index_name = format!("sqlite_autoindex_{table_name}_1");
                let index = Arc::new(Index {
                    name: index_name,
                    where_clause: None,
                    table_name: table_name.to_string(),
                    columns: vec![IndexColumn {
                        name: "id".to_string(),
                        order: SortOrder::Asc,
                        pos_in_table: 0,
                        collation: None,
                        default: None,
                        expr: None,
                    }],
                    unique: true,
                    ephemeral: false,
                    root_page: 1,
                    has_rowid: true,
                    index_method: None,
                });
                available_indexes.insert(table_name.to_string(), VecDeque::from([index]));
            });
        let customer_id_idx = Arc::new(Index {
            name: "orders_customer_id_idx".to_string(),
            table_name: "orders".to_string(),
            where_clause: None,
            columns: vec![IndexColumn {
                name: "customer_id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
                collation: None,
                default: None,
                expr: None,
            }],
            unique: false,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
        });
        let order_id_idx = Arc::new(Index {
            name: "order_items_order_id_idx".to_string(),
            table_name: "order_items".to_string(),
            where_clause: None,
            columns: vec![IndexColumn {
                name: "order_id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
                collation: None,
                default: None,
                expr: None,
            }],
            unique: false,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
        });

        available_indexes
            .entry("orders".to_string())
            .and_modify(|v| v.push_front(customer_id_idx));
        available_indexes
            .entry("order_items".to_string())
            .and_modify(|v| v.push_front(order_id_idx));

        // SELECT * FROM orders JOIN customers JOIN order_items
        // WHERE orders.customer_id = customers.id AND orders.id = order_items.order_id AND customers.id = 42
        // expecting customers to be chosen first due to the index on customers.id and it having a selective filter (=42)
        // then orders to be chosen next due to the index on orders.customer_id
        // then order_items to be chosen last due to the index on order_items.order_id
        let mut where_clause = vec![
            // orders.customer_id = customers.id
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE_NO_ORDERS].internal_id, 1, false), // orders.customer_id
                ast::Operator::Equals,
                _create_column_expr(joined_tables[TABLE_NO_CUSTOMERS].internal_id, 0, false), // customers.id
            ),
            // orders.id = order_items.order_id
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE_NO_ORDERS].internal_id, 0, false), // orders.id
                ast::Operator::Equals,
                _create_column_expr(joined_tables[TABLE_NO_ORDER_ITEMS].internal_id, 1, false), // order_items.order_id
            ),
            // customers.id = 42
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE_NO_CUSTOMERS].internal_id, 0, false), // customers.id
                ast::Operator::Equals,
                _create_numeric_literal("42"),
            ),
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Customers (due to =42 filter) -> Orders (due to index on customer_id) -> Order_items (due to index on order_id)
        assert_eq!(
            best_plan.table_numbers().collect::<Vec<_>>(),
            vec![TABLE_NO_CUSTOMERS, TABLE_NO_ORDERS, TABLE_NO_ORDER_ITEMS]
        );

        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "sqlite_autoindex_customers_1");
        assert!(constraint_refs.len() == 1);
        let constraint =
            &table_constraints[TABLE_NO_CUSTOMERS].constraints[constraint_refs[0].eq.unwrap()];
        assert!(constraint.lhs_mask.is_empty());

        let access_method = &access_methods_arena[best_plan.data[1].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "orders_customer_id_idx");
        assert!(constraint_refs.len() == 1);
        let constraint =
            &table_constraints[TABLE_NO_ORDERS].constraints[constraint_refs[0].eq.unwrap()];
        assert!(constraint.lhs_mask.contains_table(TABLE_NO_CUSTOMERS));

        let access_method = &access_methods_arena[best_plan.data[2].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "order_items_order_id_idx");
        assert!(constraint_refs.len() == 1);
        let constraint =
            &table_constraints[TABLE_NO_ORDER_ITEMS].constraints[constraint_refs[0].eq.unwrap()];
        assert!(constraint.lhs_mask.contains_table(TABLE_NO_ORDERS));
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

    #[test]
    fn test_join_order_three_tables_no_indexes() {
        let t1 = _create_btree_table("t1", _create_column_list(&["id", "foo"], Type::Integer));
        let t2 = _create_btree_table("t2", _create_column_list(&["id", "foo"], Type::Integer));
        let t3 = _create_btree_table("t3", _create_column_list(&["id", "foo"], Type::Integer));

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    outer: false,
                    full_outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                t3,
                Some(JoinInfo {
                    outer: false,
                    full_outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
        ];

        let mut where_clause = vec![
            // t2.foo = 42 (equality filter, more selective)
            _create_binary_expr(
                _create_column_expr(joined_tables[1].internal_id, 1, false), // table 1, column 1 (foo)
                ast::Operator::Equals,
                _create_numeric_literal("42"),
            ),
            // t1.foo > 10 (inequality filter, less selective)
            _create_binary_expr(
                _create_column_expr(joined_tables[0].internal_id, 1, false), // table 0, column 1 (foo)
                ast::Operator::Greater,
                _create_numeric_literal("10"),
            ),
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = HashMap::default();
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify that t2 is chosen first due to its equality filter
        assert_eq!(best_plan.table_numbers().next().unwrap(), 1);
        // Verify table scan is used since there are no indexes
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        // Verify that t1 is chosen next due to its inequality filter
        let access_method = &access_methods_arena[best_plan.data[1].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        // Verify that t3 is chosen last due to no filters
        let access_method = &access_methods_arena[best_plan.data[2].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
    }

    #[test]
    /// Test that [compute_best_join_order] chooses a "fact table" as the outer table,
    /// when it has a foreign key to all dimension tables.
    fn test_compute_best_join_order_star_schema() {
        const NUM_DIM_TABLES: usize = 9;
        const FACT_TABLE_IDX: usize = 9;

        // Create fact table with foreign keys to all dimension tables
        let mut fact_columns = vec![_create_column_rowid_alias("id")];
        for i in 0..NUM_DIM_TABLES {
            fact_columns.push(_create_column_of_type(&format!("dim{i}_id"), Type::Integer));
        }
        let fact_table = _create_btree_table("fact", fact_columns);

        // Create dimension tables, each with an id and value column
        let dim_tables: Vec<_> = (0..NUM_DIM_TABLES)
            .map(|i| {
                _create_btree_table(
                    &format!("dim{i}"),
                    vec![
                        _create_column_rowid_alias("id"),
                        _create_column_of_type("value", Type::Integer),
                    ],
                )
            })
            .collect();

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = {
            let mut refs = vec![_create_table_reference(
                dim_tables[0].clone(),
                None,
                table_id_counter.next(),
            )];
            refs.extend(dim_tables.iter().skip(1).map(|t| {
                _create_table_reference(
                    t.clone(),
                    Some(JoinInfo {
                        outer: false,
                        full_outer: false,
                        using: vec![],
                    }),
                    table_id_counter.next(),
                )
            }));
            refs.push(_create_table_reference(
                fact_table,
                Some(JoinInfo {
                    outer: false,
                    full_outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ));
            refs
        };

        let mut where_clause = vec![];

        // Add join conditions between fact and each dimension table
        for i in 0..NUM_DIM_TABLES {
            let internal_id_fact = joined_tables[FACT_TABLE_IDX].internal_id;
            let internal_id_other = joined_tables[i].internal_id;
            where_clause.push(_create_binary_expr(
                _create_column_expr(internal_id_fact, i + 1, false), // fact.dimX_id
                ast::Operator::Equals,
                _create_column_expr(internal_id_other, 0, true), // dimX.id
            ));
        }

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let available_indexes = HashMap::default();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Expected optimal order: fact table as outer, with rowid seeks in any order on each dimension table
        // Verify fact table is selected as the outer table as all the other tables can use SeekRowid
        assert_eq!(
            best_plan.table_numbers().next().unwrap(),
            FACT_TABLE_IDX,
            "First table should be fact (table {}) due to available index, got table {} instead",
            FACT_TABLE_IDX,
            best_plan.table_numbers().next().unwrap()
        );

        // Verify access methods
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(constraint_refs.is_empty());

        for (table_number, access_method_index) in best_plan.data.iter().skip(1) {
            let access_method = &access_methods_arena[*access_method_index];
            let (iter_dir, index, constraint_refs) = _as_btree(access_method);
            assert!(iter_dir == IterationDirection::Forwards);
            assert!(index.is_none());
            assert!(constraint_refs.len() == 1);
            let constraint =
                &table_constraints[*table_number].constraints[constraint_refs[0].eq.unwrap()];
            assert!(constraint.lhs_mask.contains_table(FACT_TABLE_IDX));
            assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        }
    }

    #[test]
    /// Test that [compute_best_join_order] figures out that the tables form a "linked list" pattern
    /// where a column in each table points to an indexed column in the next table,
    /// and chooses the best order based on that.
    fn test_compute_best_join_order_linked_list() {
        const NUM_TABLES: usize = 5;

        // Create tables t1 -> t2 -> t3 -> t4 -> t5 where there is a foreign key from each table to the next
        let mut tables = Vec::with_capacity(NUM_TABLES);
        for i in 0..NUM_TABLES {
            let mut columns = vec![_create_column_rowid_alias("id")];
            if i < NUM_TABLES - 1 {
                columns.push(_create_column_of_type("next_id", Type::Integer));
            }
            tables.push(_create_btree_table(&format!("t{}", i + 1), columns));
        }

        let available_indexes = HashMap::default();

        let mut table_id_counter = TableRefIdCounter::new();
        // Create table references
        let joined_tables: Vec<_> = tables
            .iter()
            .map(|t| _create_table_reference(t.clone(), None, table_id_counter.next()))
            .collect();

        // Create where clause linking each table to the next
        let mut where_clause = Vec::new();
        for i in 0..NUM_TABLES - 1 {
            let internal_id_left = joined_tables[i].internal_id;
            let internal_id_right = joined_tables[i + 1].internal_id;
            where_clause.push(_create_binary_expr(
                _create_column_expr(internal_id_left, 1, false), // ti.next_id
                ast::Operator::Equals,
                _create_column_expr(internal_id_right, 0, true), // t(i+1).id
            ));
        }

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        // Run the optimizer
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify the join order is exactly t1 -> t2 -> t3 -> t4 -> t5
        for i in 0..NUM_TABLES {
            assert_eq!(
                best_plan.table_numbers().nth(i).unwrap(),
                i,
                "Expected table {} at position {}, got table {} instead",
                i,
                i,
                best_plan.table_numbers().nth(i).unwrap()
            );
        }

        // Verify access methods:
        // - First table should use Table scan
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(constraint_refs.is_empty());

        // all of the rest should use rowid equality
        for (i, table_constraints) in table_constraints
            .iter()
            .enumerate()
            .take(NUM_TABLES)
            .skip(1)
        {
            let access_method = &access_methods_arena[best_plan.data[i].1];
            let (iter_dir, index, constraint_refs) = _as_btree(access_method);
            assert!(iter_dir == IterationDirection::Forwards);
            assert!(index.is_none());
            assert!(constraint_refs.len() == 1);
            let constraint = &table_constraints.constraints[constraint_refs[0].eq.unwrap()];
            assert!(constraint.lhs_mask.contains_table(i - 1));
            assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        }
    }

    #[test]
    /// Test that [compute_best_join_order] figures out that the index can't be used when only the second column is referenced
    fn test_index_second_column_only() {
        let mut joined_tables = Vec::new();

        let mut table_id_counter = TableRefIdCounter::new();

        // Create a table with two columns
        let table = _create_btree_table("t1", _create_column_list(&["x", "y"], Type::Integer));

        // Create a two-column index on (x,y)
        let index = Arc::new(Index {
            name: "idx_xy".to_string(),
            table_name: "t1".to_string(),
            where_clause: None,
            columns: vec![
                IndexColumn {
                    name: "x".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                },
                IndexColumn {
                    name: "y".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                    expr: None,
                },
            ],
            unique: false,
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
            index_method: None,
        });

        let mut available_indexes = HashMap::default();
        available_indexes.insert("t1".to_string(), VecDeque::from([index]));

        let table = Table::BTree(table);
        joined_tables.push(JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            internal_id: table_id_counter.next(),
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
        });

        // Create where clause that only references second column
        let mut where_clause = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: joined_tables[0].internal_id,
                    column: 1,
                    is_rowid_alias: false,
                }),
                ast::Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a scan, not a seek, because the index can't be used when only the second column is referenced
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (_, _, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
    }

    #[test]
    /// Test that an index with a gap in referenced columns (e.g. index on (a,b,c), where clause on a and c)
    /// only uses the prefix before the gap.
    fn test_index_skips_middle_column() {
        let mut table_id_counter = TableRefIdCounter::new();
        let mut joined_tables = Vec::new();
        let mut available_indexes = HashMap::default();

        let columns = _create_column_list(&["c1", "c2", "c3"], Type::Integer);
        let table = _create_btree_table("t1", columns);
        let index = Arc::new(Index {
            name: "idx1".to_string(),
            table_name: "t1".to_string(),
            where_clause: None,
            columns: vec![
                IndexColumn {
                    name: "c1".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                },
                IndexColumn {
                    name: "c2".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                    expr: None,
                },
                IndexColumn {
                    name: "c3".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 2,
                    collation: None,
                    default: None,
                    expr: None,
                },
            ],
            unique: false,
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
            index_method: None,
        });
        available_indexes.insert("t1".to_string(), VecDeque::from([index]));

        let table = Table::BTree(table);
        joined_tables.push(JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            internal_id: table_id_counter.next(),
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
        });

        // Create where clause that references first and third columns
        let mut where_clause = vec![
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 0, // c1
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 2, // c3
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(7.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a seek, and only uses the first column of the index
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (_, index, constraint_refs) = _as_btree(access_method);
        assert!(index.as_ref().is_some_and(|i| i.name == "idx1"));
        assert!(constraint_refs.len() == 1);
        let constraint = &table_constraints[0].constraints[constraint_refs[0].eq.unwrap()];
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        assert!(constraint.table_col_pos == Some(0)); // c1
    }

    #[test]
    /// Test that an index seek stops after a range operator.
    /// e.g. index on (a,b,c), where clause a=1, b>2, c=3. Only a and b should be used for seek.
    fn test_index_stops_at_range_operator() {
        let mut table_id_counter = TableRefIdCounter::new();
        let mut joined_tables = Vec::new();
        let mut available_indexes = HashMap::default();

        let columns = _create_column_list(&["c1", "c2", "c3"], Type::Integer);
        let table = _create_btree_table("t1", columns);
        let index = Arc::new(Index {
            name: "idx1".to_string(),
            table_name: "t1".to_string(),
            where_clause: None,
            columns: vec![
                IndexColumn {
                    name: "c1".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                },
                IndexColumn {
                    name: "c2".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                    expr: None,
                },
                IndexColumn {
                    name: "c3".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 2,
                    collation: None,
                    default: None,
                    expr: None,
                },
            ],
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
            unique: false,
            index_method: None,
        });
        available_indexes.insert("t1".to_string(), VecDeque::from([index]));

        let table = Table::BTree(table);
        joined_tables.push(JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            internal_id: table_id_counter.next(),
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
        });

        // Create where clause: c1 = 5 AND c2 > 10 AND c3 = 7
        let mut where_clause = vec![
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 0, // c1
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 1, // c2
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Greater,
                    Box::new(Expr::Literal(ast::Literal::Numeric(10.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 2, // c3
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(7.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a seek, and uses the first two columns of the index.
        // The third column can't be used because the second is a range query.
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (_, index, constraint_refs) = _as_btree(access_method);
        assert!(index.as_ref().is_some_and(|i| i.name == "idx1"));
        assert!(constraint_refs.len() == 2);
        let constraint = &table_constraints[0].constraints[constraint_refs[0].eq.unwrap()];
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        assert!(constraint.table_col_pos == Some(0)); // c1
        let constraint = &table_constraints[0].constraints[constraint_refs[1].lower_bound.unwrap()];
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Greater));
        assert!(constraint.table_col_pos == Some(1)); // c2
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
                primary_key: false,
                rowid_alias: c.is_rowid_alias,
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
        Arc::new(BTreeTable {
            root_page: 1, // Page number doesn't matter for tests
            name: name.to_string(),
            has_autoincrement: false,
            primary_key_columns: vec![],
            columns,
            has_rowid: true,
            is_strict: false,
            unique_sets: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        })
    }

    /// Creates a TableReference for a BTreeTable
    fn _create_table_reference(
        table: Arc<BTreeTable>,
        join_info: Option<JoinInfo>,
        internal_id: TableInternalId,
    ) -> JoinedTable {
        let name = table.name.clone();
        let table = Table::BTree(table);
        JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            identifier: name,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
        }
    }

    /// Creates a column expression
    fn _create_column_expr(table: TableInternalId, column: usize, is_rowid_alias: bool) -> Expr {
        Expr::Column {
            database: None,
            table,
            column,
            is_rowid_alias,
        }
    }

    /// Creates a binary expression for a WHERE clause
    fn _create_binary_expr(lhs: Expr, op: Operator, rhs: Expr) -> WhereTerm {
        WhereTerm {
            expr: Expr::Binary(Box::new(lhs), op, Box::new(rhs)),
            from_outer_join: None,
            consumed: false,
        }
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
            } => (*iter_dir, index.clone(), constraint_refs),
            _ => panic!("expected BTreeTable access method"),
        }
    }

    #[test]
    /// Test that when an index is available on the join column, the optimizer prefers
    /// index lookup over hash join.
    fn test_prefer_index_lookup_over_hash_join() {
        // CREATE TABLE t1(a,b,c);
        // CREATE TABLE t2(a,b,c);
        // CREATE INDEX idx_t2_a ON t2(a);
        // SELECT * FROM t1 JOIN t2 ON t1.a = t2.a;
        // Expected: SCAN t1, SEARCH t2 USING INDEX idx_t2_a (a=?)
        // Not: HASH JOIN

        let t1 = _create_btree_table("t1", _create_column_list(&["a", "b", "c"], Type::Integer));
        let t2 = _create_btree_table("t2", _create_column_list(&["a", "b", "c"], Type::Integer));

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    outer: false,
                    full_outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE1: usize = 0;
        const TABLE2: usize = 1;

        // Index on t2.a
        let mut available_indexes = HashMap::default();
        let index_t2_a = Arc::new(Index {
            name: "idx_t2_a".to_string(),
            table_name: "t2".to_string(),
            where_clause: None,
            columns: vec![IndexColumn {
                name: "a".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                collation: None,
                default: None,
                expr: None,
            }],
            unique: false, // Non-unique index
            ephemeral: false,
            root_page: 2,
            has_rowid: true,
            index_method: None,
        });
        available_indexes.insert("t2".to_string(), VecDeque::from([index_t2_a]));

        // WHERE t1.a = t2.a
        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[TABLE1].internal_id, 0, false), // t1.a
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE2].internal_id, 0, false), // t2.a
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Expected: t1 first (scan), t2 second (index seek)
        assert_eq!(
            best_plan.table_numbers().collect::<Vec<_>>(),
            vec![TABLE1, TABLE2],
            "Expected join order [t1, t2] to use index on t2.a"
        );

        // t1 should use table scan (no constraints)
        let access_method_t1 = &access_methods_arena[best_plan.data[0].1];
        let (_, _, constraint_refs_t1) = _as_btree(access_method_t1);
        assert!(
            constraint_refs_t1.is_empty(),
            "t1 should use table scan with no constraints"
        );

        // t2 should use index seek, NOT hash join
        let access_method_t2 = &access_methods_arena[best_plan.data[1].1];
        match &access_method_t2.params {
            AccessMethodParams::BTreeTable {
                index,
                constraint_refs,
                ..
            } => {
                assert!(
                    index.is_some(),
                    "t2 should use index idx_t2_a, not a hash join"
                );
                assert_eq!(
                    index.as_ref().unwrap().name,
                    "idx_t2_a",
                    "t2 should use index idx_t2_a"
                );
                assert!(
                    !constraint_refs.is_empty(),
                    "t2 should have constraints for index seek"
                );
            }
            AccessMethodParams::HashJoin { .. } => {
                panic!("Expected index lookup on t2, but got hash join instead");
            }
            _ => panic!("Unexpected access method for t2"),
        }
    }
}
