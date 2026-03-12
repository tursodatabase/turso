use crate::turso_assert_greater_than_or_equal;
use crate::{
    schema::{FromClauseSubquery, Index, Schema},
    translate::{
        collate::{get_collseq_from_expr, CollationSeq},
        expression_index::normalize_expr_for_index_matching,
        optimizer::access_method::AccessMethodParams,
        optimizer::constraints::RangeConstraintRef,
        plan::{
            GroupBy, HashJoinType, IterationDirection, JoinedTable, Operation, Plan, Scan,
            SimpleAggregate, TableReferences,
        },
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
    RowId,
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
/// If an [OrderTarget] is satisfied, then [EliminatesSort] describes which part
/// of the query no longer requires sorting.
pub enum EliminatesSortBy {
    Group,
    Order,
    GroupByAndOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderTargetPurpose {
    /// Matching this target lets the planner eliminate a later ORDER BY and/or
    /// GROUP BY sort step.
    EliminatesSort(EliminatesSortBy),
    /// Matching this target enables an extremum fast path, analogous to
    /// SQLite's WHERE_ORDERBY_MIN/MAX planning mode.
    Extremum,
}

#[derive(Debug, PartialEq, Clone)]
/// An [OrderTarget] is considered in join optimization and index selection,
/// so that if a given join ordering and its access methods satisfy the [OrderTarget],
/// then the join ordering and its access methods are preferred, all other things being equal.
pub struct OrderTarget {
    pub columns: Vec<ColumnOrder>,
    pub purpose: OrderTargetPurpose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqualityPrefixScope {
    /// Candidate scoring may skip any equality-constrained seek prefix because
    /// it only reasons about the order within that specific seek.
    AnyEquality,
    /// Final ORDER BY / GROUP BY elimination may only skip globally constant
    /// equality prefixes. Join-dependent equalities vary per outer row and do
    /// not guarantee a globally ordered concatenation of inner scans.
    ConstantEquality,
}

impl OrderTarget {
    /// Build an `OrderTarget` from a list of expressions if they can all be
    /// satisfied by a single-table ordering (needed for index satisfaction).
    fn maybe_from_iterator<'a>(
        list: impl Iterator<Item = (&'a ast::Expr, SortOrder)> + Clone,
        tables: &crate::translate::plan::TableReferences,
        purpose: OrderTargetPurpose,
    ) -> Option<Self> {
        if list.clone().count() == 0 {
            return None;
        }
        let mut cols = Vec::new();
        for (expr, order) in list {
            let col = expr_to_column_order(expr, order, tables)?;
            cols.push(col);
        }
        Some(OrderTarget {
            columns: cols,
            purpose,
        })
    }

    pub fn is_extremum(&self) -> bool {
        matches!(self.purpose, OrderTargetPurpose::Extremum)
    }
}

/// Build the synthetic ordering requirement used by simple MIN/MAX aggregation.
pub fn simple_aggregate_order_target(
    simple_aggregate: &SimpleAggregate,
    tables: &TableReferences,
) -> Option<OrderTarget> {
    let SimpleAggregate::MinMax(min_max) = simple_aggregate else {
        return None;
    };

    let mut target = OrderTarget::maybe_from_iterator(
        std::iter::once((&min_max.argument, min_max.order)),
        tables,
        OrderTargetPurpose::Extremum,
    )?;
    if let Some(coll) = min_max.collation {
        target.columns[0].collation = coll;
    }
    Some(target)
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
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        ),
        // Only GROUP BY - we would like the joined result rows to be in the order specified by the GROUP BY
        (true, Some(group_by)) => OrderTarget::maybe_from_iterator(
            group_by.exprs.iter().map(|expr| (expr, SortOrder::Asc)),
            tables,
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group),
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
                    OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group),
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
            turso_assert_greater_than_or_equal!(group_by.exprs.len(), order_by.len());
            let sort_order = group_by
                .sort_order
                .as_mut()
                .expect("GROUP BY should have a sort order before optimization is run");
            for (i, (_, order_by_dir)) in order_by.iter().enumerate() {
                sort_order[i] = *order_by_dir;
            }
            // The sort_by_key above reordered group_by.exprs but not sort_order,
            // so remaining positions may have stale values. GROUP BY columns not
            // in ORDER BY should default to ASC (matching SQLite's tie-breaking).
            for s in &mut sort_order[order_by.len()..] {
                *s = SortOrder::Asc;
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
                OrderTargetPurpose::EliminatesSort(EliminatesSortBy::GroupByAndOrder),
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
    schema: &Schema,
) -> bool {
    // Outer hash joins emit unmatched rows in hash-bucket order, not scan order.
    for (_, access_method_index) in plan.data.iter() {
        let access_method = &access_methods_arena[*access_method_index];
        if let AccessMethodParams::HashJoin { join_type, .. } = &access_method.params {
            if matches!(join_type, HashJoinType::LeftOuter | HashJoinType::FullOuter) {
                return false;
            }
        }
    }

    let mut target_col_idx = 0;
    let num_cols_in_order_target = order_target.columns.len();
    for (table_index, access_method_index) in plan.data.iter() {
        let access_method = &access_methods_arena[*access_method_index];
        let table_ref = &joined_tables[*table_index];

        // Outer joins can emit an extra row with NULLs on the right-hand side
        // when no match is found. Because that row is produced after the scan or
        // seek, we cannot rely on the right-hand table's access order to satisfy
        // ORDER BY / GROUP BY terms that reference that table.
        if table_ref
            .join_info
            .as_ref()
            .is_some_and(|join_info| join_info.is_outer())
            && order_target.columns[target_col_idx..]
                .iter()
                .any(|target_col| target_col.table_id == table_ref.internal_id)
        {
            return false;
        }

        // Check if this table has an access method that provides the right ordering.
        let consumed = match &access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index: index_opt,
                constraint_refs,
            } => btree_access_order_consumed(
                table_ref,
                *iter_dir,
                index_opt.as_deref(),
                constraint_refs,
                &order_target.columns[target_col_idx..],
                schema,
                EqualityPrefixScope::ConstantEquality,
            ),
            AccessMethodParams::MaterializedSubquery {
                index,
                constraint_refs,
                iter_dir,
            } => btree_access_order_consumed(
                table_ref,
                *iter_dir,
                Some(index.as_ref()),
                constraint_refs,
                &order_target.columns[target_col_idx..],
                schema,
                EqualityPrefixScope::ConstantEquality,
            ),
            AccessMethodParams::Subquery { iter_dir }
                if order_target.is_extremum()
                    && matches!(table_ref.table, crate::schema::Table::FromClauseSubquery(_)) =>
            {
                let crate::schema::Table::FromClauseSubquery(from_clause_subquery) =
                    &table_ref.table
                else {
                    unreachable!("guard above ensured subquery table");
                };
                subquery_intrinsic_order_consumed(
                    table_ref.internal_id,
                    from_clause_subquery,
                    *iter_dir,
                    &order_target.columns[target_col_idx..],
                    schema,
                )
            }
            _ => return false,
        };

        if consumed == 0 {
            return false;
        }
        target_col_idx += consumed;
        if target_col_idx == num_cols_in_order_target {
            return true;
        }
    }
    target_col_idx == num_cols_in_order_target
}

/// Return how many leading target columns a FROM-subquery can provide from its
/// own output order, without fabricating an extra probe index.
///
/// We recognize two sources of intrinsic order:
/// 1. An explicit final `ORDER BY` on the subquery.
/// 2. A simple single-source finalized scan whose output order is already known.
pub fn subquery_intrinsic_order_consumed(
    table_id: TableInternalId,
    subquery: &FromClauseSubquery,
    iter_dir: IterationDirection,
    target: &[ColumnOrder],
    schema: &Schema,
) -> usize {
    let Plan::Select(select_plan) = subquery.plan.as_ref() else {
        return 0;
    };
    if !select_plan.order_by.is_empty() {
        return explicit_subquery_order_consumed(table_id, select_plan, iter_dir, target);
    }
    finalized_scan_subquery_order_consumed(table_id, select_plan, iter_dir, target, schema)
}

/// Match requested order against a subquery's explicit final `ORDER BY`.
///
/// This is the closest analogue to SQLite's `pOrderBy` propagation for
/// materialized subqueries: if the final output already promises an order, the
/// outer query can reuse it directly.
fn explicit_subquery_order_consumed(
    table_id: TableInternalId,
    select_plan: &crate::translate::plan::SelectPlan,
    iter_dir: IterationDirection,
    target: &[ColumnOrder],
) -> usize {
    let mut intrinsic = Vec::with_capacity(select_plan.order_by.len());
    for (order_expr, order) in &select_plan.order_by {
        let Some((col_idx, result_col)) = select_plan
            .result_columns
            .iter()
            .enumerate()
            .find(|(_, result_col)| exprs_are_equivalent(order_expr, &result_col.expr))
        else {
            return 0;
        };
        let Ok(collation) = get_collseq_from_expr(order_expr, &select_plan.table_references) else {
            return 0;
        };
        intrinsic.push(ColumnOrder {
            table_id,
            target: ColumnTarget::Column(col_idx),
            order: *order,
            collation: collation.unwrap_or_else(|| {
                get_collseq_from_expr(&result_col.expr, &select_plan.table_references)
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            }),
        });
    }

    let target_len = target.len().min(intrinsic.len());
    for (intrinsic_col, target_col) in intrinsic.iter().zip(target.iter()).take(target_len) {
        if intrinsic_col.table_id != target_col.table_id
            || intrinsic_col.target != target_col.target
            || intrinsic_col.collation != target_col.collation
        {
            return 0;
        }
        let expected_order = match iter_dir {
            IterationDirection::Forwards => intrinsic_col.order,
            IterationDirection::Backwards => match intrinsic_col.order {
                SortOrder::Asc => SortOrder::Desc,
                SortOrder::Desc => SortOrder::Asc,
            },
        };
        if expected_order != target_col.order {
            return 0;
        }
    }

    target_len
}

/// Derive subquery output order from the finalized inner scan when there is no
/// explicit `ORDER BY`.
///
/// This intentionally starts narrow: single-source, non-aggregate,
/// non-window, non-distinct SELECTs only. Those are the cases where insertion
/// order into the materialized table is just the underlying scan order.
fn finalized_scan_subquery_order_consumed(
    table_id: TableInternalId,
    select_plan: &crate::translate::plan::SelectPlan,
    iter_dir: IterationDirection,
    target: &[ColumnOrder],
    schema: &Schema,
) -> usize {
    if select_plan.group_by.is_some()
        || !select_plan.aggregates.is_empty()
        || select_plan.limit.is_some()
        || select_plan.offset.is_some()
        || select_plan.window.is_some()
        || select_plan.distinctness.is_distinct()
        || !select_plan.values.is_empty()
        || select_plan.join_order.len() != 1
        || select_plan.joined_tables().len() != 1
    {
        return 0;
    }

    let joined_table = &select_plan.joined_tables()[select_plan.join_order[0].original_idx];
    let Operation::Scan(Scan::BTreeTable {
        iter_dir: inner_iter_dir,
        index,
    }) = &joined_table.op
    else {
        return 0;
    };

    // The outer scan direction composes with the direction used to populate the
    // materialized table. Reversing a backwards-populated table restores the
    // original key order.
    let effective_iter_dir = match (inner_iter_dir, iter_dir) {
        (IterationDirection::Forwards, IterationDirection::Forwards)
        | (IterationDirection::Backwards, IterationDirection::Backwards) => {
            IterationDirection::Forwards
        }
        (IterationDirection::Forwards, IterationDirection::Backwards)
        | (IterationDirection::Backwards, IterationDirection::Forwards) => {
            IterationDirection::Backwards
        }
    };

    let mut mapped_target = Vec::with_capacity(target.len());
    for target_col in target {
        if target_col.table_id != table_id {
            return 0;
        }
        let ColumnTarget::Column(result_col_idx) = target_col.target else {
            return 0;
        };
        let Some(result_col) = select_plan.result_columns.get(result_col_idx) else {
            return 0;
        };
        // The outer query sees result columns of the materialized subquery, but
        // the ordering proof has to be checked against the inner scan columns.
        let Some(mut inner_target_col) = expr_to_column_order(
            &result_col.expr,
            target_col.order,
            &select_plan.table_references,
        ) else {
            return 0;
        };
        if inner_target_col.table_id != joined_table.internal_id
            || inner_target_col.collation != target_col.collation
        {
            return 0;
        }
        inner_target_col.order = target_col.order;
        mapped_target.push(inner_target_col);
    }

    btree_access_order_consumed(
        joined_table,
        effective_iter_dir,
        index.as_deref(),
        &[],
        &mapped_target,
        schema,
        EqualityPrefixScope::ConstantEquality,
    )
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
        ast::Expr::RowId { table, .. } => {
            return Some(ColumnOrder {
                table_id: *table,
                target: ColumnTarget::RowId,
                order,
                collation: CollationSeq::default(),
            });
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

fn target_matches_index_column(
    target_col: &ColumnOrder,
    idx_col: &crate::schema::IndexColumn,
    table_ref: &JoinedTable,
) -> bool {
    if target_col.table_id != table_ref.internal_id {
        return false;
    }
    match (&target_col.target, &idx_col.expr) {
        (ColumnTarget::Column(col_no), None) => idx_col.pos_in_table == *col_no,
        (ColumnTarget::Expr(expr), Some(idx_expr)) => {
            let target_expr = unsafe { &**expr };
            if exprs_are_equivalent(target_expr, idx_expr) {
                return true;
            }
            // Expression indexes are compared against the normalized form that
            // was stored in the schema. A query may write the same expression in
            // a slightly different but equivalent way, so normalize before the
            // final comparison.
            let refs = TableReferences::new(vec![table_ref.clone()], Vec::new());
            let normalized = normalize_expr_for_index_matching(target_expr, table_ref, &refs);
            exprs_are_equivalent(&normalized, idx_expr)
        }
        _ => false,
    }
}

/// Return how many leading `order_target` columns this single-table btree
/// access path can satisfy.
///
/// This is shared by both candidate scoring and final ORDER BY / GROUP BY
/// elimination so they use the same column-matching, collation, custom-type,
/// and hidden-rowid-suffix rules. The caller supplies
/// [`EqualityPrefixScope`] because candidate scoring may skip any equality
/// prefix in the chosen seek key, while final global ordering proof may only
/// skip prefixes that are constant across all output rows.
pub(super) fn btree_access_order_consumed(
    table_ref: &JoinedTable,
    iter_dir: IterationDirection,
    index: Option<&Index>,
    constraint_refs: &[RangeConstraintRef],
    order_target: &[ColumnOrder],
    schema: &Schema,
    equality_prefix_scope: EqualityPrefixScope,
) -> usize {
    let Some(first_target_col) = order_target.first() else {
        return 0;
    };

    let rowid_alias_col = table_ref
        .table
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias());

    match index {
        None => {
            // Without an index, only rowid order is available.
            if first_target_col.table_id != table_ref.internal_id {
                return 0;
            }
            match first_target_col.target {
                ColumnTarget::RowId => {}
                ColumnTarget::Column(col_no) => {
                    let Some(rowid_alias_col) = rowid_alias_col else {
                        return 0;
                    };
                    if col_no != rowid_alias_col {
                        return 0;
                    }
                }
                ColumnTarget::Expr(_) => return 0,
            }
            let correct_order = if iter_dir == IterationDirection::Forwards {
                first_target_col.order == SortOrder::Asc
            } else {
                first_target_col.order == SortOrder::Desc
            };
            usize::from(correct_order)
        }
        Some(index) => {
            let mut col_idx = 0;
            let mut idx_pos = 0;
            while col_idx < order_target.len() && idx_pos < index.columns.len() {
                let target_col = &order_target[col_idx];
                if target_col.table_id != table_ref.internal_id {
                    break;
                }

                let idx_col = &index.columns[idx_pos];
                let eq_prefix_usable = constraint_refs.iter().any(|constraint| {
                    constraint.index_col_pos == idx_pos
                        && constraint.eq.as_ref().is_some_and(|eq| {
                            equality_prefix_scope == EqualityPrefixScope::AnyEquality || eq.is_const
                        })
                });
                if eq_prefix_usable {
                    // Equality-constrained prefix columns produce a single value
                    // per seek, so they do not disturb the ordering of the
                    // remaining suffix. If the ORDER BY / GROUP BY also mentions
                    // the same column with the same collation, that target term
                    // is satisfied trivially and can be consumed here too.
                    if target_matches_index_column(target_col, idx_col, table_ref) {
                        let same_collation =
                            target_col.collation == idx_col.collation.unwrap_or_default();
                        if !same_collation {
                            break;
                        }
                        col_idx += 1;
                    }
                    idx_pos += 1;
                    continue;
                }

                if !target_matches_index_column(target_col, idx_col, table_ref) {
                    break;
                }

                // Custom type columns store encoded blobs. The B-tree's bytewise
                // ordering does not match the custom type's semantic ordering, so
                // the index cannot satisfy ORDER BY for those columns.
                if let ColumnTarget::Column(col_no) = &target_col.target {
                    if let Some(col) = table_ref.table.columns().get(*col_no) {
                        if schema
                            .get_type_def(&col.ty_str, table_ref.table.is_strict())
                            .is_some()
                        {
                            break;
                        }
                    }
                }

                if target_col.collation != idx_col.collation.unwrap_or_default() {
                    break;
                }

                let correct_order = if iter_dir == IterationDirection::Forwards {
                    target_col.order == idx_col.order
                } else {
                    target_col.order != idx_col.order
                };
                if !correct_order {
                    break;
                }
                col_idx += 1;
                idx_pos += 1;
            }

            // SQLite-style rowid tables keep equal secondary-index keys ordered
            // by rowid. That implicit suffix can satisfy one extra ORDER BY term.
            if col_idx < order_target.len() && idx_pos == index.columns.len() && index.has_rowid {
                let target_col = &order_target[col_idx];
                let rowid_matches = match target_col.target {
                    ColumnTarget::RowId => true,
                    ColumnTarget::Column(col_no) => {
                        rowid_alias_col.is_some_and(|alias| alias == col_no)
                    }
                    ColumnTarget::Expr(_) => false,
                };
                let correct_order = if iter_dir == IterationDirection::Forwards {
                    target_col.order == SortOrder::Asc
                } else {
                    target_col.order == SortOrder::Desc
                };
                if target_col.table_id == table_ref.internal_id && rowid_matches && correct_order {
                    col_idx += 1;
                }
            }

            col_idx
        }
    }
}
