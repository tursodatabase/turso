use crate::{
    translate::{
        collate::{get_collseq_from_expr, CollationSeq},
        optimizer::access_method::AccessMethodParams,
        plan::{GroupBy, IterationDirection, JoinedTable, TableReferences},
        planner::table_mask_from_expr,
    },
    util::exprs_are_equivalent,
};
use turso_parser::ast::{self, SortOrder, TableInternalId};

use super::{access_method::AccessMethod, join::JoinN};

/// Target component in an ORDER BY/GROUP BY that may be a plain column or an expression.
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnTarget {
    Column(usize),
    /// We know that the ast lives at least as long as the Statement/Program,
    /// so we store a raw pointer here to avoid cloning yet another ast::Expr
    Expr(*const ast::Expr),
}

/// A convenience struct for representing a (table_no, column_target, [SortOrder]) tuple.
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnOrder {
    pub table_id: TableInternalId,
    pub target: ColumnTarget,
    pub order: SortOrder,
    pub collation: CollationSeq,
}

#[derive(Debug, PartialEq, Clone)]
/// If an [OrderTarget] is satisfied, then [EliminatesSort] describes which part of the query no longer requires sorting.
pub enum EliminatesSortBy {
    Group,
    Order,
    GroupByAndOrder,
}

#[derive(Debug, PartialEq, Clone)]
/// An [OrderTarget] is considered in join optimization and index selection,
/// so that if a given join ordering and its access methods satisfy the [OrderTarget],
/// then the join ordering and its access methods are preferred, all other things being equal.
pub struct OrderTarget(pub Vec<ColumnOrder>, pub EliminatesSortBy);

impl OrderTarget {
    /// Build an `OrderTarget` from a list of expressions if they can all be
    /// satisfied by a single-table ordering (needed for index satisfaction).
    fn maybe_from_iterator<'a>(
        list: impl Iterator<Item = (&'a ast::Expr, SortOrder)> + Clone,
        tables: &crate::translate::plan::TableReferences,
        eliminates_sort: EliminatesSortBy,
    ) -> Option<Self> {
        if list.clone().count() == 0 {
            return None;
        }
        let mut cols = Vec::new();
        for (expr, order) in list {
            let col = expr_to_column_order(expr, order, tables)?;
            cols.push(col);
        }
        Some(OrderTarget(cols, eliminates_sort))
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
    order_by: &mut Vec<(Box<ast::Expr>, SortOrder)>,
    group_by_opt: Option<&mut GroupBy>,
    tables: &TableReferences,
) -> Option<OrderTarget> {
    match (order_by.is_empty(), group_by_opt) {
        // No ordering demands - we don't care what order the joined result rows are in
        (true, None) => None,
        // Only ORDER BY - we would like the joined result rows to be in the order specified by the ORDER BY
        (false, None) => OrderTarget::maybe_from_iterator(
            order_by.iter().map(|(expr, order)| (expr.as_ref(), *order)),
            tables,
            EliminatesSortBy::Order,
        ),
        // Only GROUP BY - we would like the joined result rows to be in the order specified by the GROUP BY
        (true, Some(group_by)) => OrderTarget::maybe_from_iterator(
            group_by.exprs.iter().map(|expr| (expr, SortOrder::Asc)),
            tables,
            EliminatesSortBy::Group,
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
        (false, Some(group_by)) => {
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
                    tables,
                    EliminatesSortBy::Group,
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
            order_by.clear();

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
                tables,
                EliminatesSortBy::GroupByAndOrder,
            )
        }
    }
}

/// Check if the plan's row iteration order matches the [OrderTarget]'s column order.
/// If yes, and this plan is selected, then a sort operation can be eliminated.
pub fn plan_satisfies_order_target(
    plan: &JoinN,
    access_methods_arena: &[AccessMethod],
    joined_tables: &[JoinedTable],
    order_target: &OrderTarget,
) -> bool {
    let mut target_col_idx = 0;
    let num_cols_in_order_target = order_target.0.len();
    for (table_index, access_method_index) in plan.data.iter() {
        let access_method = &access_methods_arena[*access_method_index];
        let table_ref = &joined_tables[*table_index];

        // Check if this table has an access method that provides the right ordering.
        let consumed = match &access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index: index_opt,
                ..
            } => match index_opt {
                None => {
                    // Only rowid order is available without an index.
                    if target_col_idx >= num_cols_in_order_target {
                        continue;
                    }
                    let target_col = &order_target.0[target_col_idx];
                    if target_col.table_id != table_ref.internal_id {
                        return false;
                    }
                    let rowid_alias_col = table_ref
                        .table
                        .columns()
                        .iter()
                        .position(|c| c.is_rowid_alias());
                    let Some(rowid_alias_col) = rowid_alias_col else {
                        return false;
                    };
                    if !matches!(
                        target_col.target,
                        ColumnTarget::Column(col_no) if col_no == rowid_alias_col
                    ) {
                        return false;
                    }
                    let correct_order = if *iter_dir == IterationDirection::Forwards {
                        target_col.order == SortOrder::Asc
                    } else {
                        target_col.order == SortOrder::Desc
                    };
                    if !correct_order {
                        return false;
                    }
                    1
                }
                Some(index) => {
                    let mut col_idx = 0;
                    while target_col_idx + col_idx < num_cols_in_order_target
                        && col_idx < index.columns.len()
                    {
                        let target_col = &order_target.0[target_col_idx + col_idx];
                        if target_col.table_id != table_ref.internal_id {
                            break;
                        }
                        let idx_col = &index.columns[col_idx];
                        let column_matches = match (&target_col.target, &idx_col.expr) {
                            (ColumnTarget::Column(col_no), None) => idx_col.pos_in_table == *col_no,
                            (ColumnTarget::Expr(expr), Some(idx_expr)) => {
                                exprs_are_equivalent(unsafe { &**expr }, idx_expr)
                            }
                            _ => false,
                        };
                        if !column_matches {
                            break;
                        }

                        // If ORDER BY collation doesn't match index collation, this index can't satisfy the ordering
                        if idx_col
                            .collation
                            .is_some_and(|idx_collation| target_col.collation != idx_collation)
                        {
                            break;
                        }

                        let correct_order = if *iter_dir == IterationDirection::Forwards {
                            target_col.order == idx_col.order
                        } else {
                            target_col.order != idx_col.order
                        };
                        if !correct_order {
                            break;
                        }
                        col_idx += 1;
                    }
                    if col_idx == 0 {
                        return false;
                    }
                    col_idx
                }
            },
            _ => return false,
        };

        target_col_idx += consumed;
        if target_col_idx == num_cols_in_order_target {
            return true;
        }
    }
    target_col_idx == num_cols_in_order_target
}

fn expr_to_column_order(
    expr: &ast::Expr,
    order: SortOrder,
    tables: &TableReferences,
) -> Option<ColumnOrder> {
    match expr {
        ast::Expr::Column {
            table: table_id,
            column,
            ..
        } => {
            let table = tables.find_joined_table_by_internal_id(*table_id)?;
            let col = table.columns().get(*column)?;
            return Some(ColumnOrder {
                table_id: *table_id,
                target: ColumnTarget::Column(*column),
                order,
                collation: col.collation(),
            });
        }
        ast::Expr::Collate(expr, collation) => {
            if let ast::Expr::Column {
                table: table_id,
                column,
                ..
            } = expr.as_ref()
            {
                let collation = CollationSeq::new(collation.as_str()).unwrap_or_default();
                return Some(ColumnOrder {
                    table_id: *table_id,
                    target: ColumnTarget::Column(*column),
                    order,
                    collation,
                });
            };
        }
        _ => {}
    }
    let mask = table_mask_from_expr(expr, tables, &[]).ok()?;
    if mask.table_count() != 1 {
        return None;
    }
    let collation = get_collseq_from_expr(expr, tables)
        .ok()?
        .unwrap_or_default();
    let table_no = tables
        .joined_tables()
        .iter()
        .enumerate()
        .find_map(|(i, _)| mask.contains_table(i).then_some(i))?;
    let table_id = tables.joined_tables()[table_no].internal_id;
    Some(ColumnOrder {
        table_id,
        target: ColumnTarget::Expr(expr as *const ast::Expr),
        order,
        collation,
    })
}
