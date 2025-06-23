use std::{cell::RefCell, collections::HashMap};

use turso_sqlite3_parser::ast::TableInternalId;

use crate::{
    translate::{
        optimizer::{cost::Cost, order::plan_satisfies_order_target},
        plan::{JoinOrderMember, JoinedTable},
        planner::TableMask,
    },
    Result,
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
/// Returns None if the plan is worse than the provided cost upper bound.
pub fn join_lhs_and_rhs<'a>(
    lhs: Option<&JoinN>,
    rhs_table_reference: &JoinedTable,
    rhs_constraints: &'a TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    access_methods_arena: &'a RefCell<Vec<AccessMethod<'a>>>,
    cost_upper_bound: Cost,
) -> Result<Option<JoinN>> {
    // The input cardinality for this join is the output cardinality of the previous join.
    // For example, in a 2-way join, if the left table has 1000 rows, and the right table will return 2 rows for each of the left table's rows,
    // then the output cardinality of the join will be 2000.
    let input_cardinality = lhs.map_or(1, |l| l.output_cardinality);

    let best_access_method = find_best_access_method_for_join_order(
        rhs_table_reference,
        rhs_constraints,
        join_order,
        maybe_order_target,
        input_cardinality as f64,
    )?;

    let lhs_cost = lhs.map_or(Cost(0.0), |l| l.cost);
    let cost = lhs_cost + best_access_method.cost;

    if cost > cost_upper_bound {
        return Ok(None);
    }

    access_methods_arena.borrow_mut().push(best_access_method);

    let mut best_access_methods = Vec::with_capacity(join_order.len());
    best_access_methods.extend(lhs.map_or(vec![], |l| l.data.clone()));

    let rhs_table_number = join_order.last().unwrap().original_idx;
    best_access_methods.push((rhs_table_number, access_methods_arena.borrow().len() - 1));

    let lhs_mask = lhs.map_or(TableMask::new(), |l| {
        TableMask::from_table_number_iter(l.table_numbers())
    });
    // Output cardinality is reduced by the product of the selectivities of the constraints that can be used with this join order.
    let output_cardinality_multiplier = rhs_constraints
        .constraints
        .iter()
        .filter(|c| lhs_mask.contains_all(&c.lhs_mask))
        .map(|c| c.selectivity)
        .product::<f64>();

    // Produce a number of rows estimated to be returned when this table is filtered by the WHERE clause.
    // If this table is the rightmost table in the join order, we multiply by the input cardinality,
    // which is the output cardinality of the previous tables.
    let output_cardinality = (input_cardinality as f64
        * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64
        * output_cardinality_multiplier)
        .ceil() as usize;

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
    access_methods_arena: &'a RefCell<Vec<AccessMethod<'a>>>,
) -> Result<Option<BestJoinOrderResult>> {
    // Skip work if we have no tables to consider.
    if joined_tables.is_empty() {
        return Ok(None);
    }

    let num_tables = joined_tables.len();

    // Compute naive left-to-right plan to use as pruning threshold
    let naive_plan = compute_naive_left_deep_plan(
        joined_tables,
        maybe_order_target,
        access_methods_arena,
        constraints,
    )?;

    // Keep track of both 1. the best plan overall (not considering sorting), and 2. the best ordered plan (which might not be the same).
    // We assign Some Cost (tm) to any required sort operation, so the best ordered plan may end up being
    // the one we choose, if the cost reduction from avoiding sorting brings it below the cost of the overall best one.
    let mut best_ordered_plan: Option<JoinN> = None;
    let mut best_plan_is_also_ordered = if let Some(order_target) = maybe_order_target {
        plan_satisfies_order_target(
            &naive_plan,
            access_methods_arena,
            joined_tables,
            order_target,
        )
    } else {
        false
    };

    // If we have one table, then the "naive left-to-right plan" is always the best.
    if joined_tables.len() == 1 {
        return Ok(Some(BestJoinOrderResult {
            best_plan: naive_plan,
            best_ordered_plan: None,
        }));
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
    let cost_upper_bound = best_plan.cost;
    let cost_upper_bound_ordered = best_plan.cost;

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
            &join_order,
            maybe_order_target,
            access_methods_arena,
            cost_upper_bound_ordered,
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
                    &join_order,
                    maybe_order_target,
                    access_methods_arena,
                    cost_upper_bound_ordered,
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
                if has_all_tables && cost_upper_bound_ordered > cost {
                    best_ordered_plan = Some(rel);
                }
            }

            if let Some(rel) = best_for_mask.take() {
                let cost = rel.cost;
                let has_all_tables = mask.table_count() == num_tables;
                if has_all_tables {
                    if cost_upper_bound > cost {
                        best_plan = rel;
                        best_plan_is_also_ordered = best_for_mask_is_also_ordered;
                    }
                } else {
                    best_plan_memo.insert(mask, rel);
                }
            }
        }
    }

    Ok(Some(BestJoinOrderResult {
        best_plan,
        best_ordered_plan: if best_plan_is_also_ordered {
            None
        } else {
            best_ordered_plan
        },
    }))
}

/// Specialized version of [compute_best_join_order] that just joins tables in the order they are given
/// in the SQL query. This is used as an upper bound for any other plans -- we can give up enumerating
/// permutations if they exceed this cost during enumeration.
pub fn compute_naive_left_deep_plan<'a>(
    joined_tables: &[JoinedTable],
    maybe_order_target: Option<&OrderTarget>,
    access_methods_arena: &'a RefCell<Vec<AccessMethod<'a>>>,
    constraints: &'a [TableConstraints],
) -> Result<JoinN> {
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
        &join_order[..1],
        maybe_order_target,
        access_methods_arena,
        Cost(f64::MAX),
    )?
    .expect("call to join_lhs_and_rhs in compute_naive_left_deep_plan always returns Some(JoinN)");

    // Add remaining tables one at a time from left to right
    for i in 1..n {
        best_plan = join_lhs_and_rhs(
            Some(&best_plan),
            &joined_tables[i],
            &constraints[i],
            &join_order[..=i],
            maybe_order_target,
            access_methods_arena,
            Cost(f64::MAX),
        )?
        .expect(
            "call to join_lhs_and_rhs in compute_naive_left_deep_plan always returns Some(JoinN)",
        );
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
    use std::{cell::Cell, rc::Rc, sync::Arc};

    use turso_sqlite3_parser::ast::{self, Expr, Operator, SortOrder, TableInternalId};

    use super::*;
    use crate::{
        schema::{BTreeTable, Column, Index, IndexColumn, Table, Type},
        translate::{
            optimizer::constraints::{constraints_from_where_clause, BinaryExprSide},
            plan::{
                ColumnUsedMask, IterationDirection, JoinInfo, Operation, TableReferences, WhereTerm,
            },
            planner::TableMask,
        },
        vdbe::builder::TableRefIdCounter,
    };

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
        let where_clause = vec![];

        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
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
        let where_clause = vec![];

        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        // SELECT * from test_table
        // expecting best_best_plan() not to do any work due to empty where clause.
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();
        // Should just be a table scan access method
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
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

        let where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, true), // table 0, column 0 (rowid)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let access_methods_arena = RefCell::new(Vec::new());
        let available_indexes = HashMap::new();
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        // SELECT * FROM test_table WHERE id = 42
        // expecting a RowidEq access method because id is a rowid alias.
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(!access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.constraint_refs.len() == 1);
        assert!(
            table_constraints[0].constraints[access_method.constraint_refs[0].constraint_vec_pos]
                .where_clause_pos
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

        let where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, false), // table 0, column 0 (id)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let access_methods_arena = RefCell::new(Vec::new());
        let mut available_indexes = HashMap::new();
        let index = Arc::new(Index {
            name: "sqlite_autoindex_test_table_1".to_string(),
            table_name: "test_table".to_string(),
            columns: vec![IndexColumn {
                name: "id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                collation: None,
                default: None,
            }],
            unique: true,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
        });
        available_indexes.insert("test_table".to_string(), vec![index]);

        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();
        // SELECT * FROM test_table WHERE id = 42
        // expecting an IndexScan access method because id is a primary key with an index
        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(!access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.as_ref().unwrap().name == "sqlite_autoindex_test_table_1");
        assert!(access_method.constraint_refs.len() == 1);
        assert!(
            table_constraints[0].constraints[access_method.constraint_refs[0].constraint_vec_pos]
                .where_clause_pos
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
                    using: None,
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
            columns: vec![IndexColumn {
                name: "id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                collation: None,
                default: None,
            }],
            unique: true,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
        });
        available_indexes.insert("table1".to_string(), vec![index1]);

        // SELECT * FROM table1 JOIN table2 WHERE table1.id = table2.id
        // expecting table2 to be chosen first due to the index on table1.id
        let where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[TABLE1].internal_id, 0, false), // table1.id
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE2].internal_id, 0, false), // table2.id
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![1, 0]);
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        let access_method = &access_methods_arena.borrow()[best_plan.data[1].1];
        assert!(!access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.as_ref().unwrap().name == "index1");
        assert!(access_method.constraint_refs.len() == 1);
        assert!(
            table_constraints[TABLE1].constraints
                [access_method.constraint_refs[0].constraint_vec_pos]
                .where_clause_pos
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
                    using: None,
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                table_order_items.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
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
                    table_name: table_name.to_string(),
                    columns: vec![IndexColumn {
                        name: "id".to_string(),
                        order: SortOrder::Asc,
                        pos_in_table: 0,
                        collation: None,
                        default: None,
                    }],
                    unique: true,
                    ephemeral: false,
                    root_page: 1,
                    has_rowid: true,
                });
                available_indexes.insert(table_name.to_string(), vec![index]);
            });
        let customer_id_idx = Arc::new(Index {
            name: "orders_customer_id_idx".to_string(),
            table_name: "orders".to_string(),
            columns: vec![IndexColumn {
                name: "customer_id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
                collation: None,
                default: None,
            }],
            unique: false,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
        });
        let order_id_idx = Arc::new(Index {
            name: "order_items_order_id_idx".to_string(),
            table_name: "order_items".to_string(),
            columns: vec![IndexColumn {
                name: "order_id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
                collation: None,
                default: None,
            }],
            unique: false,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
        });

        available_indexes
            .entry("orders".to_string())
            .and_modify(|v| v.push(customer_id_idx));
        available_indexes
            .entry("order_items".to_string())
            .and_modify(|v| v.push(order_id_idx));

        // SELECT * FROM orders JOIN customers JOIN order_items
        // WHERE orders.customer_id = customers.id AND orders.id = order_items.order_id AND customers.id = 42
        // expecting customers to be chosen first due to the index on customers.id and it having a selective filter (=42)
        // then orders to be chosen next due to the index on orders.customer_id
        // then order_items to be chosen last due to the index on order_items.order_id
        let where_clause = vec![
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
        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Customers (due to =42 filter) -> Orders (due to index on customer_id) -> Order_items (due to index on order_id)
        assert_eq!(
            best_plan.table_numbers().collect::<Vec<_>>(),
            vec![TABLE_NO_CUSTOMERS, TABLE_NO_ORDERS, TABLE_NO_ORDER_ITEMS]
        );

        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(!access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.as_ref().unwrap().name == "sqlite_autoindex_customers_1");
        assert!(access_method.constraint_refs.len() == 1);
        let constraint = &table_constraints[TABLE_NO_CUSTOMERS].constraints
            [access_method.constraint_refs[0].constraint_vec_pos];
        assert!(constraint.lhs_mask.is_empty());

        let access_method = &access_methods_arena.borrow()[best_plan.data[1].1];
        assert!(!access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.as_ref().unwrap().name == "orders_customer_id_idx");
        assert!(access_method.constraint_refs.len() == 1);
        let constraint = &table_constraints[TABLE_NO_ORDERS].constraints
            [access_method.constraint_refs[0].constraint_vec_pos];
        assert!(constraint.lhs_mask.contains_table(TABLE_NO_CUSTOMERS));

        let access_method = &access_methods_arena.borrow()[best_plan.data[2].1];
        assert!(!access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.as_ref().unwrap().name == "order_items_order_id_idx");
        assert!(access_method.constraint_refs.len() == 1);
        let constraint = &table_constraints[TABLE_NO_ORDER_ITEMS].constraints
            [access_method.constraint_refs[0].constraint_vec_pos];
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
                    using: None,
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                t3.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
                table_id_counter.next(),
            ),
        ];

        let where_clause = vec![
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
        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();

        // Verify that t2 is chosen first due to its equality filter
        assert_eq!(best_plan.table_numbers().next().unwrap(), 1);
        // Verify table scan is used since there are no indexes
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.is_none());
        // Verify that t1 is chosen next due to its inequality filter
        let access_method = &access_methods_arena.borrow()[best_plan.data[1].1];
        assert!(access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.is_none());
        // Verify that t3 is chosen last due to no filters
        let access_method = &access_methods_arena.borrow()[best_plan.data[2].1];
        assert!(access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.is_none());
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
                        using: None,
                    }),
                    table_id_counter.next(),
                )
            }));
            refs.push(_create_table_reference(
                fact_table.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
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
        let access_methods_arena = RefCell::new(Vec::new());
        let available_indexes = HashMap::new();
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
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
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.is_none());
        assert!(access_method.constraint_refs.is_empty());

        for (table_number, access_method_index) in best_plan.data.iter().skip(1) {
            let access_method = &access_methods_arena.borrow()[*access_method_index];
            assert!(!access_method.is_scan());
            assert!(access_method.iter_dir == IterationDirection::Forwards);
            assert!(access_method.index.is_none());
            assert!(access_method.constraint_refs.len() == 1);
            let constraint = &table_constraints[*table_number].constraints
                [access_method.constraint_refs[0].constraint_vec_pos];
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
        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        // Run the optimizer
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
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
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(access_method.is_scan());
        assert!(access_method.iter_dir == IterationDirection::Forwards);
        assert!(access_method.index.is_none());
        assert!(access_method.constraint_refs.is_empty());

        // all of the rest should use rowid equality
        for (i, table_constraints) in table_constraints
            .iter()
            .enumerate()
            .take(NUM_TABLES)
            .skip(1)
        {
            let access_method = &access_methods_arena.borrow()[best_plan.data[i].1];
            assert!(!access_method.is_scan());
            assert!(access_method.iter_dir == IterationDirection::Forwards);
            assert!(access_method.index.is_none());
            assert!(access_method.constraint_refs.len() == 1);
            let constraint =
                &table_constraints.constraints[access_method.constraint_refs[0].constraint_vec_pos];
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
            columns: vec![
                IndexColumn {
                    name: "x".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                },
                IndexColumn {
                    name: "y".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                },
            ],
            unique: false,
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
        });

        let mut available_indexes = HashMap::new();
        available_indexes.insert("t1".to_string(), vec![index]);

        joined_tables.push(JoinedTable {
            table: Table::BTree(table),
            internal_id: table_id_counter.next(),
            op: Operation::Scan {
                iter_dir: IterationDirection::Forwards,
                index: None,
            },
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
        });

        // Create where clause that only references second column
        let where_clause = vec![WhereTerm {
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
            consumed: Cell::new(false),
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a scan, not a seek, because the index can't be used when only the second column is referenced
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(access_method.is_scan());
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
            columns: vec![
                IndexColumn {
                    name: "c1".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                },
                IndexColumn {
                    name: "c2".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                },
                IndexColumn {
                    name: "c3".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 2,
                    collation: None,
                    default: None,
                },
            ],
            unique: false,
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
        });
        available_indexes.insert("t1".to_string(), vec![index]);

        joined_tables.push(JoinedTable {
            table: Table::BTree(table),
            internal_id: table_id_counter.next(),
            op: Operation::Scan {
                iter_dir: IterationDirection::Forwards,
                index: None,
            },
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
        });

        // Create where clause that references first and third columns
        let where_clause = vec![
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
                consumed: Cell::new(false),
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
                consumed: Cell::new(false),
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a seek, and only uses the first column of the index
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(!access_method.is_scan());
        assert!(access_method
            .index
            .as_ref()
            .is_some_and(|i| i.name == "idx1"));
        assert!(access_method.constraint_refs.len() == 1);
        let constraint =
            &table_constraints[0].constraints[access_method.constraint_refs[0].constraint_vec_pos];
        assert!(constraint.operator == ast::Operator::Equals);
        assert!(constraint.table_col_pos == 0); // c1
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
            columns: vec![
                IndexColumn {
                    name: "c1".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                },
                IndexColumn {
                    name: "c2".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                },
                IndexColumn {
                    name: "c3".to_string(),
                    order: SortOrder::Asc,
                    pos_in_table: 2,
                    collation: None,
                    default: None,
                },
            ],
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
            unique: false,
        });
        available_indexes.insert("t1".to_string(), vec![index]);

        joined_tables.push(JoinedTable {
            table: Table::BTree(table),
            internal_id: table_id_counter.next(),
            op: Operation::Scan {
                iter_dir: IterationDirection::Forwards,
                index: None,
            },
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
        });

        // Create where clause: c1 = 5 AND c2 > 10 AND c3 = 7
        let where_clause = vec![
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
                consumed: Cell::new(false),
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
                consumed: Cell::new(false),
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
                consumed: Cell::new(false),
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let access_methods_arena = RefCell::new(Vec::new());
        let table_constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            None,
            &table_constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a seek, and uses the first two columns of the index.
        // The third column can't be used because the second is a range query.
        let access_method = &access_methods_arena.borrow()[best_plan.data[0].1];
        assert!(!access_method.is_scan());
        assert!(access_method
            .index
            .as_ref()
            .is_some_and(|i| i.name == "idx1"));
        assert!(access_method.constraint_refs.len() == 2);
        let constraint =
            &table_constraints[0].constraints[access_method.constraint_refs[0].constraint_vec_pos];
        assert!(constraint.operator == ast::Operator::Equals);
        assert!(constraint.table_col_pos == 0); // c1
        let constraint =
            &table_constraints[0].constraints[access_method.constraint_refs[1].constraint_vec_pos];
        assert!(constraint.operator == ast::Operator::Greater);
        assert!(constraint.table_col_pos == 1); // c2
    }

    fn _create_column(c: &TestColumn) -> Column {
        Column {
            name: Some(c.name.clone()),
            ty: c.ty,
            ty_str: c.ty.to_string(),
            is_rowid_alias: c.is_rowid_alias,
            primary_key: false,
            notnull: false,
            default: None,
            unique: false,
            collation: None,
        }
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
    fn _create_btree_table(name: &str, columns: Vec<Column>) -> Rc<BTreeTable> {
        Rc::new(BTreeTable {
            root_page: 1, // Page number doesn't matter for tests
            name: name.to_string(),
            primary_key_columns: vec![],
            columns,
            has_rowid: true,
            is_strict: false,
            unique_sets: None,
        })
    }

    /// Creates a TableReference for a BTreeTable
    fn _create_table_reference(
        table: Rc<BTreeTable>,
        join_info: Option<JoinInfo>,
        internal_id: TableInternalId,
    ) -> JoinedTable {
        let name = table.name.clone();
        JoinedTable {
            table: Table::BTree(table),
            op: Operation::Scan {
                iter_dir: IterationDirection::Forwards,
                index: None,
            },
            identifier: name,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
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
            consumed: Cell::new(false),
        }
    }

    /// Creates a numeric literal expression
    fn _create_numeric_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::Numeric(value.to_string()))
    }
}
