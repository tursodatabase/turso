use std::collections::HashMap;

use turso_parser::ast::TableInternalId;

use crate::{
    translate::{
        optimizer::{
            access_method::{try_hash_join_access_method, AccessMethodParams},
            cost::{Cost, RowCountEstimate},
            order::plan_satisfies_order_target,
        },
        plan::{JoinOrderMember, JoinedTable, NonFromClauseSubquery, WhereTerm},
        planner::TableMask,
    },
    LimboError, Result,
};

use super::{
    access_method::{find_best_access_method_for_join_order, AccessMethod},
    constraints::TableConstraints,
    cost::ESTIMATED_HARDCODED_ROWS_PER_TABLE,
    order::OrderTarget,
};

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
    subqueries: &[NonFromClauseSubquery],
) -> Result<Option<JoinN>> {
    // The input cardinality for this join is the output cardinality of the previous join.
    // For example, in a 2-way join, if the left table has 1000 rows, and the right table will return 2 rows for each of the left table's rows,
    // then the output cardinality of the join will be 2000.
    let input_cardinality = lhs.map_or(1, |l| l.output_cardinality);

    let rhs_table_number = join_order.last().unwrap().original_idx;
    let rhs_base_rows = base_table_rows.get(rhs_table_number).copied().unwrap_or(
        RowCountEstimate::HardcodedFallback(ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64),
    );

    let Some(mut method) = find_best_access_method_for_join_order(
        rhs_table_reference,
        rhs_constraints,
        join_order,
        maybe_order_target,
        input_cardinality as f64,
        rhs_base_rows,
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
            let lhs_mask_for_ephemeral = lhs.map_or(TableMask::new(), |l| {
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

    // Reuse for both hash cost and output cardinality computation
    let lhs_mask = lhs.map_or(TableMask::new(), |l| {
        TableMask::from_table_number_iter(l.table_numbers())
    });
    let output_cardinality_multiplier = rhs_constraints
        .constraints
        .iter()
        .filter(|c| lhs_mask.contains_all(&c.lhs_mask))
        .map(|c| c.selectivity)
        .product::<f64>();

    // If we already have a non-empty LHS (at least one table has been joined),
    // consider a hash-join alternative for the current RHS. We only allow hash
    // joins when:
    //
    // - The would-be build table is accessed via a scan
    // - Choosing hash join does not destroy a useful ORDER BY the nested-loop plan would satisfy.
    // - The probe table is not using a selective index seek we’d prefer to keep.
    // - The build table has no remaining constraints from prior tables that
    //   are not already consumed as hash-join keys in earlier hash joins.
    if let Some(lhs) = lhs {
        let lhs_table_idx = join_order[join_order.len() - 2].original_idx;
        let rhs_table_idx = join_order.last().unwrap().original_idx;
        let lhs_table = &joined_tables[lhs_table_idx];

        // If the chosen access method for the build table already uses constraints
        // skip hash join to avoid dropping those filters.
        let build_access_method_uses_constraints = lhs
            .data
            .iter()
            .find(|(table_no, _)| *table_no == lhs_table_idx)
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

        // If the query needs a specific ordering, only allow hash join when the nested-loop
        // alternative would NOT satisfy the order target.
        let nested_loop_preserves_order = maybe_order_target.is_some_and(|order_target| {
            // Temporarily add the nested-loop method to the arena to evaluate ordering.
            {
                access_methods_arena.push(best_access_method.clone());
            }
            let am_idx = access_methods_arena.len() - 1;
            let mut data = lhs.data.clone();
            data.push((rhs_table_number, am_idx));
            let tmp_plan = JoinN {
                data,
                output_cardinality: 0,
                cost: Cost(0.0),
            };
            let preserves = plan_satisfies_order_target(
                &tmp_plan,
                access_methods_arena,
                joined_tables,
                order_target,
            );
            access_methods_arena.pop();
            preserves
        });

        let build_cardinality = lhs.output_cardinality as f64;
        let probe_cardinality = *rhs_base_rows;

        let rhs_has_selective_seek = matches!(
            best_access_method.params,
            AccessMethodParams::BTreeTable {
                ref constraint_refs,
                ..
            } if !constraint_refs.is_empty()
        );

        // The probe table must NOT be the build table of any earlier hash join
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

        // The build table must NOT have any constraints from prior tables
        // that won't be consumed as hash join keys. When a table becomes a hash build table,
        // its cursor is exhausted after building. If there are constraints like `prior.x = build.x`
        // that aren't part of the build & probe hash join, they can't be evaluated because the
        // build cursor is no longer positioned.
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
        let build_has_prior_constraints = {
            let build_constraints = &all_constraints[lhs_table_idx];

            // Get the set of tables that are build tables for hash joins where lhs_table_idx was probe
            let tables_already_hash_joined_as_build: Vec<usize> = {
                let arena = &access_methods_arena;
                lhs.data
                    .iter()
                    .filter_map(|(_, am_idx)| {
                        arena.get(*am_idx).and_then(|am| {
                            if let AccessMethodParams::HashJoin {
                                build_table_idx,
                                probe_table_idx,
                                ..
                            } = &am.params
                            {
                                if *probe_table_idx == lhs_table_idx {
                                    Some(*build_table_idx)
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

            build_constraints.constraints.iter().any(|c| {
                // Check if this constraint references prior tables that are NOT already
                // handled by a hash join where we were the probe table
                let prior_mask = lhs_mask;
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

        // Only consider hash join when the build table is using a plain table scan
        // (no index, no constraint-driven search). Otherwise we'd drop useful filters
        // or break the access pattern the planner chose.
        //
        // We intentionally do NOT (yet) allow a table that is already the probe side of
        // a hash join to become the build side of another hash join, the second hash join
        // would rebuild from ALL rows of the middle table, not just the matching rows from the first.
        let build_am_is_plain_table_scan = lhs
            .data
            .iter()
            .find(|(table_no, _)| *table_no == lhs_table_idx)
            .map(|(_, am_idx)| {
                let arena = &access_methods_arena;
                arena.get(*am_idx).is_some_and(|am| {
                    matches!(
                        &am.params,
                        AccessMethodParams::BTreeTable {
                            index: None,
                            constraint_refs,
                            ..
                        } if constraint_refs.is_empty()
                    )
                })
            })
            .unwrap_or(false);
        if !build_access_method_uses_constraints
            && !nested_loop_preserves_order
            && !rhs_has_selective_seek
            && !probe_table_is_prior_build
            && !build_has_prior_constraints
            && build_am_is_plain_table_scan
        {
            let lhs_constraints = &all_constraints[lhs_table_idx];
            if let Some(hash_join_method) = try_hash_join_access_method(
                lhs_table,
                rhs_table_reference,
                lhs_table_idx,
                rhs_table_idx,
                lhs_constraints,
                rhs_constraints,
                where_clause,
                build_cardinality,
                probe_cardinality,
                subqueries,
            ) {
                if hash_join_method.cost < best_access_method.cost {
                    best_access_method = hash_join_method;
                }
            }
        }
    }

    let cost = lhs_cost + best_access_method.cost;

    if cost > cost_upper_bound {
        return Ok(None);
    }
    access_methods_arena.push(best_access_method);

    let mut best_access_methods = Vec::with_capacity(join_order.len());
    best_access_methods.extend(lhs.map_or(vec![], |l| l.data.clone()));

    best_access_methods.push((rhs_table_number, access_methods_arena.len() - 1));

    // Produce a number of rows estimated to be returned when this table is filtered by the WHERE clause.
    // If this table is the rightmost table in the join order, we multiply by the input cardinality,
    // which is the output cardinality of the previous tables.
    let output_cardinality =
        (input_cardinality as f64 * *rhs_base_rows * output_cardinality_multiplier).ceil() as usize;

    Ok(Some(JoinN {
        data: best_access_methods,
        output_cardinality,
        cost,
    }))
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
pub fn compute_best_join_order<'a>(
    joined_tables: &[JoinedTable],
    maybe_order_target: Option<&OrderTarget>,
    constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    where_clause: &mut [WhereTerm],
    subqueries: &[NonFromClauseSubquery],
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
    if num_tables > GREEDY_JOIN_THRESHOLD {
        return compute_greedy_join_order(
            joined_tables,
            maybe_order_target,
            constraints,
            base_table_rows,
            access_methods_arena,
            where_clause,
            subqueries,
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
        subqueries,
    )?;

    // Keep track of both 1. the best plan overall (not considering sorting), and 2. the best ordered plan (which might not be the same).
    // We assign Some Cost (tm) to any required sort operation, so the best ordered plan may end up being
    // the one we choose, if the cost reduction from avoiding sorting brings it below the cost of the overall best one.
    let mut best_ordered_plan: Option<JoinN> = None;
    let mut best_plan_is_also_ordered = match (naive_plan.as_ref(), maybe_order_target) {
        (Some(plan), Some(order_target)) => {
            plan_satisfies_order_target(plan, access_methods_arena, joined_tables, order_target)
        }
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
    let mut best_plan_memo: HashMap<TableMask, JoinN> =
        HashMap::with_capacity(2usize.pow(num_tables as u32 - 1));

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
        assert!(join_order.len() == 1);
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
            subqueries,
        )?;
        if let Some(rel) = rel {
            best_plan_memo.insert(mask, rel);
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
                HashMap::with_capacity(left_join_count);
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
            // Keep track of the best way to join this subset of tables.
            // Take the (a,b,c,d) example from above:
            // E.g. for "a JOIN b JOIN c", the possibilities are (a,b,c), (a,c,b), (b,a,c) and so on.
            // If we find out (b,a,c) is the best way to join these three, then we ONLY need to compute
            // the cost of (b,a,c,d) in the final step, because (a,b,c,d) (and all others) are guaranteed to be worse.
            let mut best_for_mask: Option<JoinN> = None;
            // also keep track of the best plan for this subset that orders the rows in an Interesting Way (tm),
            // i.e. allows us to eliminate sort operations downstream.
            let (mut best_ordered_for_mask, mut best_for_mask_is_also_ordered) = (None, false);

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

                // If the already cached plan for this subset was too crappy to consider,
                // then joining it with RHS won't help. Skip.
                let Some(lhs) = best_plan_memo.get(&lhs_mask) else {
                    continue;
                };

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
                assert!(join_order.len() == subset_size);

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
                    subqueries,
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
                } else if best_for_mask.is_none() || rel.cost < best_for_mask.as_ref().unwrap().cost
                {
                    best_for_mask = Some(rel);
                    best_for_mask_is_also_ordered = satisfies_order_target;
                }
            }

            if let Some(rel) = best_ordered_for_mask.take() {
                let cost = rel.cost;
                let has_all_tables = mask.table_count() == num_tables;
                if has_all_tables && cost_upper_bound > cost {
                    best_ordered_plan = Some(rel);
                }
            }

            if let Some(rel) = best_for_mask.take() {
                let cost = rel.cost;
                let has_all_tables = mask.table_count() == num_tables;
                if has_all_tables {
                    if cost_upper_bound > cost {
                        best_plan = Some(rel);
                        best_plan_is_also_ordered = best_for_mask_is_also_ordered;
                    }
                } else {
                    best_plan_memo.insert(mask, rel);
                }
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
        None => Err(LimboError::PlanningError(
            "No valid query plan found".to_string(),
        )),
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
    subqueries: &[NonFromClauseSubquery],
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
        subqueries,
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

        for &idx in &remaining {
            // Outer join RHS requires all preceding tables joined first
            if let Some(required) = left_join_deps.get(&idx) {
                if !current_mask.contains_all(required) {
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
                subqueries,
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
/// Score = (base_rows * filter_selectivity) * 0.1^hub_score
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
        let selectivity: f64 = constraints[t]
            .constraints
            .iter()
            .filter(|c| c.lhs_mask.is_empty())
            .map(|c| c.selectivity)
            .product();

        let score = base_rows * selectivity * 0.1_f64.powi(hub_score[t] as i32);

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
pub fn compute_naive_left_deep_plan<'a>(
    joined_tables: &[JoinedTable],
    maybe_order_target: Option<&OrderTarget>,
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    constraints: &'a [TableConstraints],
    where_clause: &mut [WhereTerm],
    subqueries: &[NonFromClauseSubquery],
) -> Result<Option<JoinN>> {
    let n = joined_tables.len();
    assert!(n > 0);

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
        subqueries,
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
            subqueries,
        )?;
        if best_plan.is_none() {
            return Ok(None);
        }
    }

    Ok(best_plan)
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
        translate::{
            optimizer::{
                access_method::AccessMethodParams,
                constraints::{constraints_from_where_clause, BinaryExprSide, RangeConstraintRef},
            },
            plan::{
                ColumnUsedMask, IterationDirection, JoinInfo, Operation, TableReferences, WhereTerm,
            },
            planner::TableMask,
        },
        vdbe::builder::TableRefIdCounter,
    };

    fn default_base_rows(n: usize) -> Vec<RowCountEstimate> {
        vec![RowCountEstimate::HardcodedFallback(ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64); n]
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
        let available_indexes = HashMap::new();
        let mut where_clause = vec![];

        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    /// Test that [compute_best_join_order] returns a table scan access method when the where clause is empty.
    fn test_compute_best_join_order_single_table_no_indexes() {
        let t1 = _create_btree_table("test_table", _create_column_list(&["id"], Type::Integer));
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(
            t1.clone(),
            None,
            table_id_counter.next(),
        )];
        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = HashMap::new();
        let mut where_clause = vec![];

        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
        )
        .unwrap();

        // SELECT * from test_table
        // expecting best_best_plan() not to do any work due to empty where clause.
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
        let joined_tables = vec![_create_table_reference(
            t1.clone(),
            None,
            table_id_counter.next(),
        )];

        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, true), // table 0, column 0 (rowid)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let available_indexes = HashMap::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
        )
        .unwrap();

        // SELECT * FROM test_table WHERE id = 42
        // expecting a RowidEq access method because id is a rowid alias.
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
        let joined_tables = vec![_create_table_reference(
            t1.clone(),
            None,
            table_id_counter.next(),
        )];

        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, false), // table 0, column 0 (id)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let mut available_indexes = HashMap::new();
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
        )
        .unwrap();
        // SELECT * FROM test_table WHERE id = 42
        // expecting an IndexScan access method because id is a primary key with an index
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
            _create_table_reference(t1.clone(), None, table_id_counter.next()),
            _create_table_reference(
                t2.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE1: usize = 0;
        const TABLE2: usize = 1;

        let mut available_indexes = HashMap::new();
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
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
            _create_table_reference(table_orders.clone(), None, table_id_counter.next()),
            _create_table_reference(
                table_customers.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                table_order_items.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE_NO_ORDERS: usize = 0;
        const TABLE_NO_CUSTOMERS: usize = 1;
        const TABLE_NO_ORDER_ITEMS: usize = 2;

        let mut available_indexes = HashMap::new();
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
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
            _create_table_reference(t1.clone(), None, table_id_counter.next()),
            _create_table_reference(
                t2.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                t3.clone(),
                Some(JoinInfo {
                    outer: false,
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
        let available_indexes = HashMap::new();
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
                        using: vec![],
                    }),
                    table_id_counter.next(),
                )
            }));
            refs.push(_create_table_reference(
                fact_table.clone(),
                Some(JoinInfo {
                    outer: false,
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
        let available_indexes = HashMap::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
            assert!(constraint.operator == ast::Operator::Equals);
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

        let available_indexes = HashMap::new();

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
        )
        .unwrap();

        // Run the optimizer
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
            assert!(constraint.operator == ast::Operator::Equals);
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

        let mut available_indexes = HashMap::new();
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
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
        let mut available_indexes = HashMap::new();

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
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
        )
        .unwrap()
        .unwrap();

        // Verify access method is a seek, and only uses the first column of the index
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (_, index, constraint_refs) = _as_btree(access_method);
        assert!(index.as_ref().is_some_and(|i| i.name == "idx1"));
        assert!(constraint_refs.len() == 1);
        let constraint = &table_constraints[0].constraints[constraint_refs[0].eq.unwrap()];
        assert!(constraint.operator == ast::Operator::Equals);
        assert!(constraint.table_col_pos == Some(0)); // c1
    }

    #[test]
    /// Test that an index seek stops after a range operator.
    /// e.g. index on (a,b,c), where clause a=1, b>2, c=3. Only a and b should be used for seek.
    fn test_index_stops_at_range_operator() {
        let mut table_id_counter = TableRefIdCounter::new();
        let mut joined_tables = Vec::new();
        let mut available_indexes = HashMap::new();

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
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
        assert!(constraint.operator == ast::Operator::Equals);
        assert!(constraint.table_col_pos == Some(0)); // c1
        let constraint = &table_constraints[0].constraints[constraint_refs[1].lower_bound.unwrap()];
        assert!(constraint.operator == ast::Operator::Greater);
        assert!(constraint.table_col_pos == Some(1)); // c2
    }

    fn _create_column(c: &TestColumn) -> Column {
        Column::new(
            Some(c.name.clone()),
            c.ty.to_string(),
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
            _create_table_reference(t1.clone(), None, table_id_counter.next()),
            _create_table_reference(
                t2.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: vec![],
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE1: usize = 0;
        const TABLE2: usize = 1;

        // Index on t2.a
        let mut available_indexes = HashMap::new();
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
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
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
