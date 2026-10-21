use super::{
    expr::{walk_expr, walk_expr_mut},
    plan::{EvalAt, JoinOrderMember, Operation, TableReferences, WhereTerm},
};
use crate::translate::plan::BitSet;
use crate::translate::{
    expr::WalkControl,
    plan::{NonFromClauseSubquery, SubqueryState},
    semantic::hir::{self, HirDocument},
};
use crate::Result;
use turso_parser::ast::{self, Expr};

/// Valid ways to refer to the rowid of a btree table.
pub const ROWID_STRS: [&str; 3] = ["rowid", "_rowid_", "oid"];

pub fn rewrite_between_exprs(expr: &mut Expr) -> Result<()> {
    walk_expr_mut(expr, &mut |e: &mut Expr| -> Result<WalkControl> {
        if let Expr::Between {
            lhs,
            not,
            start,
            end,
        } = e
        {
            let lhs_expr = std::mem::take(lhs.as_mut());
            let start_expr = std::mem::take(start.as_mut());
            let end_expr = std::mem::take(end.as_mut());

            let (lower, upper, combine_op) = if *not {
                (
                    Expr::Binary(
                        Box::new(lhs_expr.clone()),
                        ast::Operator::Less,
                        Box::new(start_expr),
                    ),
                    Expr::Binary(
                        Box::new(lhs_expr),
                        ast::Operator::Greater,
                        Box::new(end_expr),
                    ),
                    ast::Operator::Or,
                )
            } else {
                (
                    Expr::Binary(
                        Box::new(lhs_expr.clone()),
                        ast::Operator::GreaterEquals,
                        Box::new(start_expr),
                    ),
                    Expr::Binary(
                        Box::new(lhs_expr),
                        ast::Operator::LessEquals,
                        Box::new(end_expr),
                    ),
                    ast::Operator::And,
                )
            };
            *e = Expr::Binary(Box::new(lower), combine_op, Box::new(upper));
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/**
  Returns the earliest point at which a WHERE term can be evaluated.
  For expressions referencing tables, this is the innermost loop that contains a row for each
  table referenced in the expression.
  For expressions not referencing any tables (e.g. constants), this is before the main loop is
  opened, because they do not need any table data.
*/
pub fn determine_where_to_eval_term(
    term: &WhereTerm,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    if let Some(table_id) = term.from_outer_join {
        return Ok(EvalAt::Loop(
            join_order
                .iter()
                .position(|t| t.table_id == table_id)
                .unwrap_or(usize::MAX),
        ));
    }

    determine_where_to_eval_expr(&term.expr, join_order, subqueries, table_references)
}

/// A bitmask representing a set of tables in a query plan.
/// Tables are numbered by their index in [SelectPlan::joined_tables].
/// In the bitmask, the first bit is unused so that a mask with all zeros
/// can represent "no tables".
///
/// E.g. table 0 is represented by bit index 1, table 1 by bit index 2, etc.
///
/// Usage in Join Optimization
///
/// In join optimization, [TableMask] is used to:
/// - Generate subsets of tables for dynamic programming in join optimization
/// - Ensure tables are joined in valid orders (e.g., respecting LEFT JOIN order)
///
/// Usage with constraints (WHERE clause)
///
/// [TableMask] helps determine:
/// - Which tables are referenced in a constraint
/// - When a constraint can be applied as a join condition (all referenced tables must be on the left side of the table being joined)
///
/// Note that although [TableReference]s contain an internal ID as well, in join order optimization
/// the [TableMask] refers to the index of the table in the original join order, not the internal ID.
/// This is simply because we want to represent the tables as a contiguous set of bits, and the internal ID
/// might not be contiguous after e.g. subquery unnesting or other transformations.
pub type TableMask = BitSet;

/// Returns a [TableMask] representing the tables referenced in the given expression.
///
/// This includes outer references from subqueries, even if the subquery plan has
/// already been consumed, by relying on the cached outer reference ids.
/// Used in the optimizer for constraint analysis.
pub fn table_mask_from_expr(
    top_level_expr: &Expr,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<TableMask> {
    let mut mask = TableMask::default();
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if let Some(table_idx) = table_references
                    .joined_tables()
                    .iter()
                    .position(|t| t.internal_id == *table)
                {
                    mask.set(table_idx)?;
                } else if table_references
                    .find_outer_query_ref_by_internal_id(*table)
                    .is_none()
                {
                    // Tables from outer query scopes are guaranteed to be 'in scope' for this query,
                    // so they don't need to be added to the table mask. However, if the table is not found
                    // in the outer scope either, then it's an invalid reference.
                    crate::bail_parse_error!("table not found in joined_tables");
                }
            }
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            // Hence, the tables referenced in subqueries must be added to the table mask.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Unevaluated { plan } => {
                        let outer_ref_ids = plan.as_ref().unwrap().used_outer_query_ref_ids();
                        for outer_ref_id in &outer_ref_ids {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == *outer_ref_id)
                            {
                                mask.set(table_idx)?;
                            }
                        }
                    }
                    SubqueryState::Evaluated { outer_ref_ids, .. } => {
                        // Now hash-join plans can now translate some correlated subqueries early, we
                        // still revisit those predicates even though the plan has already been consumed.
                        // Without this cache we'd panic or lose the knowledge that an outer table was required.
                        //
                        // Example: `SELECT t.a FROM t WHERE t.a = (SELECT MAX(x.a) FROM x WHERE x.b = t.b)`.
                        // The outer expression `x.b = t.b` is visited after the subquery is translated,
                        // so we need cached `outer_ref_ids` to realize that `t` must already be in scope.
                        for outer_ref_id in outer_ref_ids {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == *outer_ref_id)
                            {
                                mask.set(table_idx)?;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(mask)
}

/// Source positions and outer-join facts needed by HIR constraint planning.
pub(crate) trait HirFromScope {
    fn source_position(&self, source: hir::SourceId) -> Option<usize>;
    fn outer_join_may_null_extend(&self, source: hir::SourceId) -> bool;
    fn full_join_may_null_extend(&self, source: hir::SourceId) -> bool;
}

impl HirFromScope for hir::From {
    fn source_position(&self, source: hir::SourceId) -> Option<usize> {
        hir::From::source_position(self, source)
    }

    fn outer_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        hir::From::outer_join_may_null_extend(self, source)
    }

    fn full_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        hir::From::full_join_may_null_extend(self, source)
    }
}

pub(crate) fn table_mask_from_hir_scope(
    document: &HirDocument,
    from: &impl HirFromScope,
    expression: &hir::Expr,
) -> Result<TableMask> {
    table_mask_from_hir_positions(document, expression, |source| from.source_position(source))
}

/// Return direct FROM positions for tests and parser-shaped HIR callers.
/// Sources captured from an outer query are absent from the local mask.
#[cfg(test)]
pub(crate) fn table_mask_from_hir_expr(
    document: &HirDocument,
    from: Option<&hir::From>,
    expression: &hir::Expr,
) -> Result<TableMask> {
    table_mask_from_hir_positions(document, expression, |source| {
        from.and_then(|from| from.source_position(source))
    })
}

fn table_mask_from_hir_positions(
    document: &HirDocument,
    expression: &hir::Expr,
    position: impl Fn(hir::SourceId) -> Option<usize>,
) -> Result<TableMask> {
    let mut mask = TableMask::default();
    let mut allocation_error = None;

    document.visit_expr_sources(expression, &mut |source| {
        let Some(position) = position(source) else {
            return;
        };
        if allocation_error.is_none() {
            allocation_error = mask.set(position).err();
        }
    });

    if let Some(error) = allocation_error {
        return Err(error.into());
    }
    Ok(mask)
}

/// Determines the earliest loop where an expression can be safely evaluated.
///
/// When a referenced table is not found in `join_order`, we check if it's a hash-join
/// build table and map the condition to the probe loop where its rows are produced.
/// Subquery references are also respected, even after their plans are consumed.
pub fn determine_where_to_eval_expr(
    top_level_expr: &Expr,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    // If the expression references no tables, it can be evaluated before any table loops are opened.
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                let Some(join_idx) = join_order.iter().position(|t| t.table_id == *table) else {
                    // Table not found in join_order. Check if it's a hash join build table.
                    // If so, we need to evaluate the condition at the probe table's loop position.
                    if let Some(tables) = table_references {
                        for (probe_idx, member) in join_order.iter().enumerate() {
                            let probe_table = &tables.joined_tables()[member.original_idx];
                            if let Operation::HashJoin(ref hj) = probe_table.op {
                                let build_table = &tables.joined_tables()[hj.build_table_idx];
                                if build_table.internal_id == *table {
                                    // This table is the build side of a hash join.
                                    // Evaluate the condition at the probe table's loop position.
                                    eval_at = eval_at.max(EvalAt::Loop(probe_idx));
                                    return Ok(WalkControl::Continue);
                                }
                            }
                        }
                    }
                    // Must be an outer query reference; in that case, the table is already in scope.
                    return Ok(WalkControl::Continue);
                };
                eval_at = eval_at.max(EvalAt::Loop(join_idx));
            }
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Evaluated { evaluated_at, .. } => {
                        eval_at = eval_at.max(*evaluated_at);
                    }
                    SubqueryState::Unevaluated { plan } => {
                        let outer_ref_ids = plan.as_ref().unwrap().used_outer_query_ref_ids();
                        for outer_ref_id in &outer_ref_ids {
                            let join_idx = join_order
                                .iter()
                                .position(|t| t.table_id == *outer_ref_id)
                                .or_else(|| {
                                    let tables = table_references?;
                                    for (probe_idx, member) in join_order.iter().enumerate() {
                                        let probe_table =
                                            &tables.joined_tables()[member.original_idx];
                                        if let Operation::HashJoin(ref hj) = probe_table.op {
                                            let build_table =
                                                &tables.joined_tables()[hj.build_table_idx];
                                            if build_table.internal_id == *outer_ref_id {
                                                return Some(probe_idx);
                                            }
                                        }
                                    }
                                    None
                                });
                            if let Some(join_idx) = join_idx {
                                eval_at = eval_at.max(EvalAt::Loop(join_idx));
                            }
                        }
                        return Ok(WalkControl::Continue);
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(eval_at)
}
