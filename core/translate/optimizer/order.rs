use std::cell::RefCell;

use limbo_sqlite3_parser::ast::{self, SortOrder};

use crate::{
    translate::plan::{GroupBy, IterationDirection, TableReference},
    util::exprs_are_equivalent,
};

use super::{access_method::AccessMethod, join::JoinN};

#[derive(Debug, PartialEq, Clone)]
/// A convenience struct for representing a (table_no, column_no, [SortOrder]) tuple.
pub struct ColumnOrder {
    pub table_no: usize,
    pub column_no: usize,
    pub order: SortOrder,
}

#[derive(Debug, PartialEq, Clone)]
/// If an [OrderTarget] is satisfied, then [EliminatesSort] describes which part of the query no longer requires sorting.
pub enum EliminatesSort {
    GroupBy,
    OrderBy,
    GroupByAndOrderBy,
}

#[derive(Debug, PartialEq, Clone)]
/// An [OrderTarget] is considered in join optimization and index selection,
/// so that if a given join ordering and its access methods satisfy the [OrderTarget],
/// then the join ordering and its access methods are preferred, all other things being equal.
pub struct OrderTarget(pub Vec<ColumnOrder>, pub EliminatesSort);

impl OrderTarget {
    fn maybe_from_iterator<'a>(
        list: impl Iterator<Item = (&'a ast::Expr, SortOrder)> + Clone,
        eliminates_sort: EliminatesSort,
    ) -> Option<Self> {
        if list.clone().count() == 0 {
            return None;
        }
        if list
            .clone()
            .any(|(expr, _)| !matches!(expr, ast::Expr::Column { .. }))
        {
            return None;
        }
        Some(OrderTarget(
            list.map(|(expr, order)| {
                let ast::Expr::Column { table, column, .. } = expr else {
                    unreachable!();
                };
                ColumnOrder {
                    table_no: *table,
                    column_no: *column,
                    order,
                }
            })
            .collect(),
            eliminates_sort,
        ))
    }
}

/// Compute an [OrderTarget] for the join optimizer to use.
/// Ideally, a join order is both efficient in joining the tables
/// but also returns the results in an order that minimizes the amount of
/// sorting that needs to be done later (either in GROUP BY, ORDER BY, or both).
///
/// TODO: this does not currently handle the case where we definitely cannot eliminate
/// the ORDER BY sorter, but we could still eliminate the GROUP BY sorter.
pub fn compute_order_target(
    order_by_opt: &mut Option<Vec<(ast::Expr, SortOrder)>>,
    group_by_opt: Option<&mut GroupBy>,
) -> Option<OrderTarget> {
    match (&order_by_opt, group_by_opt) {
        // No ordering demands - we don't care what order the joined result rows are in
        (None, None) => None,
        // Only ORDER BY - we would like the joined result rows to be in the order specified by the ORDER BY
        (Some(order_by), None) => OrderTarget::maybe_from_iterator(
            order_by.iter().map(|(expr, order)| (expr, *order)),
            EliminatesSort::OrderBy,
        ),
        // Only GROUP BY - we would like the joined result rows to be in the order specified by the GROUP BY
        (None, Some(group_by)) => OrderTarget::maybe_from_iterator(
            group_by.exprs.iter().map(|expr| (expr, SortOrder::Asc)),
            EliminatesSort::GroupBy,
        ),
        // Both ORDER BY and GROUP BY:
        // If the GROUP BY does not contain all the expressions in the ORDER BY,
        // then we must separately sort the result rows for ORDER BY anyway.
        // However, in that case we can use the GROUP BY expressions as the target order for the join,
        // so that we don't have to sort twice.
        //
        // If the GROUP BY contains all the expressions in the ORDER BY,
        // then we again can use the GROUP BY expressions as the target order for the join;
        // however in this case we must take the ASC/DESC from ORDER BY into account.
        (Some(order_by), Some(group_by)) => {
            // Does the group by contain all expressions in the order by?
            let group_by_contains_all = order_by.iter().all(|(expr, _)| {
                group_by
                    .exprs
                    .iter()
                    .any(|group_by_expr| exprs_are_equivalent(expr, group_by_expr))
            });
            // If not, let's try to target an ordering that matches the group by -- we don't care about ASC/DESC
            if !group_by_contains_all {
                return OrderTarget::maybe_from_iterator(
                    group_by.exprs.iter().map(|expr| (expr, SortOrder::Asc)),
                    EliminatesSort::GroupBy,
                );
            }
            // If yes, let's try to target an ordering that matches the GROUP BY columns,
            // but the ORDER BY orderings. First, we need to reorder the GROUP BY columns to match the ORDER BY columns.
            group_by.exprs.sort_by_key(|expr| {
                order_by
                    .iter()
                    .position(|(order_by_expr, _)| exprs_are_equivalent(expr, order_by_expr))
                    .map_or(usize::MAX, |i| i)
            });

            // Now, regardless of whether we can eventually eliminate the sorting entirely in the optimizer,
            // we know that we don't need ORDER BY sorting anyway, because the GROUP BY will sort the result since
            // it contains all the necessary columns required for the ORDER BY, and the GROUP BY columns are now in the correct order.
            // First, however, we need to make sure the GROUP BY sorter's column sort directions match the ORDER BY requirements.
            assert!(group_by.exprs.len() >= order_by.len());
            for (i, (_, order_by_dir)) in order_by.iter().enumerate() {
                group_by
                    .sort_order
                    .as_mut()
                    .expect("GROUP BY should have a sort order before optimization is run")[i] =
                    *order_by_dir;
            }
            // Now we can remove the ORDER BY from the query.
            order_by_opt.take();

            OrderTarget::maybe_from_iterator(
                group_by
                    .exprs
                    .iter()
                    .zip(
                        group_by
                            .sort_order
                            .as_ref()
                            .expect("GROUP BY should have a sort order before optimization is run")
                            .iter(),
                    )
                    .map(|(expr, dir)| (expr, *dir)),
                EliminatesSort::GroupByAndOrderBy,
            )
        }
    }
}

/// Check if the plan's row iteration order matches the [OrderTarget]'s column order.
/// If yes, and this plan is selected, then a sort operation can be eliminated.
pub fn plan_satisfies_order_target(
    plan: &JoinN,
    access_methods_arena: &RefCell<Vec<AccessMethod>>,
    table_references: &[TableReference],
    order_target: &OrderTarget,
) -> bool {
    let mut target_col_idx = 0;
    let num_cols_in_order_target = order_target.0.len();
    for (table_no, access_method_index) in plan.data.iter() {
        let target_col = &order_target.0[target_col_idx];
        let table_ref = &table_references[*table_no];
        let correct_table = target_col.table_no == *table_no;
        if !correct_table {
            return false;
        }

        // Check if this table has an access method that provides the right ordering.
        let access_method = &access_methods_arena.borrow()[*access_method_index];
        let iter_dir = access_method.iter_dir;
        let index = access_method.index.as_ref();
        match index {
            None => {
                // No index, so the next required column must be the rowid alias column.
                let rowid_alias_col = table_ref
                    .table
                    .columns()
                    .iter()
                    .position(|c| c.is_rowid_alias);
                let Some(rowid_alias_col) = rowid_alias_col else {
                    return false;
                };
                let correct_column = target_col.column_no == rowid_alias_col;
                if !correct_column {
                    return false;
                }

                // Btree table rows are always in ascending order of rowid.
                let correct_order = if iter_dir == IterationDirection::Forwards {
                    target_col.order == SortOrder::Asc
                } else {
                    target_col.order == SortOrder::Desc
                };
                if !correct_order {
                    return false;
                }
                target_col_idx += 1;
                // All order columns matched.
                if target_col_idx == num_cols_in_order_target {
                    return true;
                }
            }
            Some(index) => {
                // All of the index columns must match the next required columns in the order target.
                for index_col in index.columns.iter() {
                    let target_col = &order_target.0[target_col_idx];
                    let correct_column = target_col.column_no == index_col.pos_in_table;
                    if !correct_column {
                        return false;
                    }
                    let correct_order = if iter_dir == IterationDirection::Forwards {
                        target_col.order == index_col.order
                    } else {
                        target_col.order != index_col.order
                    };
                    if !correct_order {
                        return false;
                    }
                    target_col_idx += 1;
                    // All order columns matched.
                    if target_col_idx == num_cols_in_order_target {
                        return true;
                    }
                }
            }
        }
    }
    false
}
