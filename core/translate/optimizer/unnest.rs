//! Unnesting pass: rewrites EXISTS/NOT EXISTS correlated subqueries into semi/anti-joins.
//!
//! A correlated EXISTS subquery:
//!   SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a)
//! is rewritten into a semi-join:
//!   SELECT * FROM t1 SEMI JOIN t2 ON t2.a = t1.a
//!
//! Similarly, NOT EXISTS becomes an anti-join.
//!
//! Base intuition for correctness:
//! - `EXISTS(subquery)` is a yes/no test: did we find at least one inner row?
//! - A semi-join is the same yes/no test, just run as a join loop:
//!   keep the outer row once a matching inner row is found.
//! - For correlated equality predicates (for example `inner.k = outer.k`), the
//!   answer depends only on the key value `k`, not on outer row identity. If two
//!   outer rows have the same `k`, they both either match or do not match.
//!   That is why precomputing/joining by `k` preserves `EXISTS` truth values.
//! - `NOT EXISTS(subquery)` is the opposite yes/no test.
//! - An anti-join does exactly that:
//!   keep the outer row only if no matching inner row is found.
//! - The same key-value argument applies to anti-join: for a given `k`, either
//!   every outer row with `k` survives (no inner match) or none survive.
//!
//! So the rewrite is semantics-preserving when we keep the same notion of
//! "matching row" and do not move predicates across boundaries that change
//! row existence (for example OUTER JOIN null-extension or inner-independent
//! gates under `NOT EXISTS`, e.g. `NOT EXISTS (... WHERE corr AND 0)` is TRUE for every outer row).
//!
//! Canonical references used for blocker rationale in this module:
//! - [SQLITE-EXISTS] https://sqlite.org/lang_expr.html#the_exists_operator
//! - [PG-SUBQUERY] https://www.postgresql.org/docs/current/functions-subquery.html
//! - [PG-JOIN-ORDER] https://www.postgresql.org/docs/current/queries-table-expressions.html
//! - [MYSQL-SEMIJOIN] https://dev.mysql.com/doc/refman/8.4/en/semijoins-antijoins.html

use smallvec::SmallVec;
use turso_parser::ast::{self, Expr, TableInternalId, UnaryOperator};

use crate::function::{Deterministic, Func};
use crate::translate::{
    expr::{walk_expr, WalkControl},
    plan::{JoinInfo, JoinType, SelectPlan, SubqueryState, WhereTerm},
};
use crate::Result;

/// Attempt to unnest EXISTS/NOT EXISTS correlated subqueries into semi/anti-joins.
/// This is called during the optimizer pipeline, after constant condition elimination
/// and before table access optimization.
pub fn unnest_exists_subqueries(plan: &mut SelectPlan) -> Result<()> {
    let mut i = 0;
    while i < plan.non_from_clause_subqueries.len() {
        let subquery = &plan.non_from_clause_subqueries[i];
        // Only consider unevaluated, correlated EXISTS subqueries.
        if !subquery.correlated {
            i += 1;
            continue;
        }
        let is_exists = matches!(subquery.query_type, ast::SubqueryType::Exists { .. });
        if !is_exists {
            i += 1;
            continue;
        }
        if try_unnest_exists(plan, i) {
            // Subquery was removed from the vec; don't increment i.
            continue;
        }
        i += 1;
    }
    Ok(())
}

/// Try to unnest a single EXISTS subquery at index `subquery_idx`.
/// Returns true if the subquery was successfully unnested and removed.
fn try_unnest_exists(plan: &mut SelectPlan, subquery_idx: usize) -> bool {
    // 1. Extract the inner plan (if available).
    let inner_plan = {
        let subquery = &plan.non_from_clause_subqueries[subquery_idx];
        let SubqueryState::Unevaluated { plan: inner } = &subquery.state else {
            return false;
        };
        let Some(inner) = inner.as_ref() else {
            return false;
        };
        inner.clone()
    };
    let subquery_id = plan.non_from_clause_subqueries[subquery_idx].internal_id;

    // 2. Check blockers: the inner plan must be simple enough to unnest.
    if !can_unnest_inner_plan(&inner_plan) {
        return false;
    }

    // 3. Determine if this is EXISTS or NOT EXISTS by scanning the outer WHERE clause.
    let Some(where_info) = find_exists_in_where(&plan.where_clause, subquery_id) else {
        return false;
    };

    let join_type = if where_info.negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    // 4. Extract correlation predicates from the inner WHERE clause.
    // These are predicates of the form `inner_col = outer_col` where one side
    // references an outer query ref and the other side references an inner table.
    let outer_table_ids: Vec<TableInternalId> = inner_plan
        .table_references
        .outer_query_refs()
        .iter()
        .map(|r| r.internal_id)
        .collect();
    let inner_table_ids: Vec<TableInternalId> = inner_plan
        .table_references
        .joined_tables()
        .iter()
        .map(|t| t.internal_id)
        .collect();

    // All inner WHERE terms must be expressible as join predicates or filters
    // on inner tables only. If any term references outer tables in a non-equality
    // context, we bail out.
    for term in &inner_plan.where_clause {
        if !is_valid_unnesting_predicate(&term.expr, &outer_table_ids, &inner_table_ids) {
            return false;
        }
    }

    // For anti-join rewrites, every inner WHERE term must reference an inner table.
    // Principle ([SQLITE-EXISTS], [PG-SUBQUERY]): NOT EXISTS depends on inner-row
    // emptiness, so inner-independent gates must stay under the quantifier.
    // Example: `NOT EXISTS (... WHERE corr AND 0)` is TRUE for every outer row.
    // Hoisting `0` to outer WHERE would reject all rows, so this rewrite is unsafe.
    if join_type == JoinType::Anti {
        for term in &inner_plan.where_clause {
            let refs = collect_table_refs(&term.expr);
            if !refs.iter().any(|t| inner_table_ids.contains(t)) {
                return false;
            }
        }
    }

    // 4b. Block unnesting if any correlation predicate references a table that
    // is on the nullable side of a LEFT/FULL OUTER JOIN in the outer plan.
    // Principle ([PG-JOIN-ORDER]): OUTER JOIN null-extension is defined before
    // WHERE filtering; moving such predicates across the boundary is not safe.
    // Example: correlating to nullable RHS columns can drop rows that should
    // survive as NULL-extended rows.
    let mut nullable_outer_table_ids: Vec<TableInternalId> = Vec::new();
    let joined = plan.table_references.joined_tables();
    for (i, t) in joined.iter().enumerate() {
        if let Some(ji) = &t.join_info {
            if ji.is_outer() || ji.is_full_outer() {
                // Right-side table of LEFT/FULL OUTER JOIN is nullable.
                nullable_outer_table_ids.push(t.internal_id);
            }
            if ji.is_full_outer() && i > 0 {
                // Left-side table of FULL OUTER JOIN is also nullable.
                nullable_outer_table_ids.push(joined[i - 1].internal_id);
            }
        }
    }
    if !nullable_outer_table_ids.is_empty() {
        // Check if any correlation predicate touches a nullable outer table.
        for term in &inner_plan.where_clause {
            let refs = collect_table_refs(&term.expr);
            if refs.iter().any(|t| nullable_outer_table_ids.contains(t)) {
                return false;
            }
        }
    }

    // 5. Perform the rewrite.
    // Move inner tables into the outer plan as semi/anti-joined tables.
    let mut inner_plan = inner_plan;
    let inner_tables = std::mem::take(inner_plan.table_references.joined_tables_mut());
    for (idx, mut table) in inner_tables.into_iter().enumerate() {
        if idx == 0 {
            // First inner table gets the semi/anti-join annotation.
            table.join_info = Some(JoinInfo {
                join_type,
                using: vec![],
            });
        }
        plan.table_references.add_joined_table(table);
    }

    // Move inner WHERE terms to the outer plan's WHERE clause.
    // The outer_query_ref column references in these terms already point to the
    // correct table IDs (they were set up during subquery planning), so they
    // work correctly in the outer scope.
    // Reset `consumed` since the inner optimizer may have marked terms consumed
    // during its own optimization pass; in the outer plan they need re-evaluation.
    for mut term in inner_plan.where_clause {
        term.consumed = false;
        plan.where_clause.push(term);
    }

    // Move any inner non-FROM subqueries to the outer plan.
    for inner_subquery in inner_plan.non_from_clause_subqueries {
        plan.non_from_clause_subqueries.push(inner_subquery);
    }

    // Replace the EXISTS/NOT EXISTS expression in the outer WHERE with a no-op (true).
    // The semi/anti-join handles the filtering.
    replace_exists_with_true(&mut plan.where_clause, where_info.where_term_idx);

    // Remove the subquery from the outer plan's subquery list.
    // Note: subquery_idx may have shifted if we inserted inner subqueries above,
    // but we inserted at the END, so the original index is still valid.
    plan.non_from_clause_subqueries.remove(subquery_idx);

    true
}

/// Check if the inner plan is simple enough to unnest.
fn can_unnest_inner_plan(plan: &SelectPlan) -> bool {
    // Blocker ([MYSQL-SEMIJOIN]): only rewrite simple single-source subqueries.
    // Principle: current VM early-out is loop-local; multi-table inners would
    // need additional state to preserve existential semantics.
    // Example: EXISTS over `i1 JOIN i2` can match only at deeper loop levels.
    if plan.table_references.joined_tables().len() != 1 {
        return false;
    }
    // Blocker ([PG-SUBQUERY], [SQLITE-EXISTS]): LIMIT can change emptiness.
    // Example: `EXISTS(... LIMIT 0)` is always FALSE.
    if plan.limit.is_some() {
        return false;
    }
    // Blocker ([MYSQL-SEMIJOIN]): grouped subqueries require grouped rewrite.
    // Example: GROUP BY/HAVING subquery is not equivalent to row-level semi-join.
    if plan.group_by.is_some() {
        return false;
    }
    // Blocker ([MYSQL-SEMIJOIN]): ORDER BY on a plain EXISTS is semantically
    // irrelevant, but in practice appears with other complex constructs we
    // don't decorrelate here (keep this pass intentionally conservative).
    if !plan.order_by.is_empty() {
        return false;
    }
    // Blocker ([MYSQL-SEMIJOIN]): DISTINCT + existential checks may require
    // duplicate-elimination-aware planning.
    // Example: DISTINCT in inner subquery should not change outer cardinality.
    if !matches!(
        plan.distinctness,
        crate::translate::plan::Distinctness::NonDistinct
    ) {
        return false;
    }
    // Blocker ([MYSQL-SEMIJOIN]): window frames are not row-local filters.
    // Example: window function values depend on partition context.
    if plan.window.is_some() {
        return false;
    }
    // Blocker ([PG-SUBQUERY]): OFFSET changes emptiness independently of joins.
    // Example: `EXISTS(... OFFSET 1000)` may become FALSE even with matches.
    if plan.offset.is_some() {
        return false;
    }
    // Blocker ([MYSQL-SEMIJOIN]): VALUES-based inners are not handled by this
    // table-based rewrite path.
    if !plan.values.is_empty() {
        return false;
    }
    // Blocker ([PG-SUBQUERY]): aggregate subqueries can produce a row even when
    // no base rows match, which breaks existential rewrite assumptions.
    // Example: `EXISTS(SELECT count(*) FROM i WHERE false)` is TRUE.
    if !plan.aggregates.is_empty() {
        return false;
    }
    // Blocker ([MYSQL-SEMIJOIN]): nested correlated subqueries need layered
    // decorrelation ordering not implemented in this pass.
    // Example: inner WHERE contains `EXISTS (SELECT ... correlated to inner)`.
    if plan.non_from_clause_subqueries.iter().any(|s| s.correlated) {
        return false;
    }
    // Blocker ([PG-SUBQUERY]): side-effecting/volatile expressions may be
    // evaluated a different number of times after rewrite.
    // Example: `random()` under EXISTS should keep original evaluation behavior.
    for term in &plan.where_clause {
        if contains_nondeterministic_function(&term.expr) {
            return false;
        }
    }
    true
}

/// Information about where an EXISTS/NOT EXISTS expression appears in the WHERE clause.
struct ExistsWhereInfo {
    /// Index into the WHERE clause vector.
    where_term_idx: usize,
    /// Whether the EXISTS is negated (NOT EXISTS).
    negated: bool,
}

/// Find the WHERE term that references the EXISTS subquery with the given ID.
/// Returns None if the subquery is referenced in a context we can't unnest
/// (e.g., inside OR, or referenced multiple times).
fn find_exists_in_where(
    where_clause: &[WhereTerm],
    subquery_id: TableInternalId,
) -> Option<ExistsWhereInfo> {
    for (idx, term) in where_clause.iter().enumerate() {
        // Blocker ([PG-JOIN-ORDER]): OUTER JOIN ON terms cannot be rewritten as
        // normal WHERE terms without changing null-extension behavior.
        // Example: `LEFT JOIN ... ON EXISTS(...)` must still emit unmatched rows.
        if term.from_outer_join.is_some() {
            continue;
        }
        // Check for direct EXISTS reference: SubqueryResult { Exists }
        if let Expr::SubqueryResult {
            subquery_id: sid,
            query_type: ast::SubqueryType::Exists { .. },
            ..
        } = &term.expr
        {
            if *sid == subquery_id {
                return Some(ExistsWhereInfo {
                    where_term_idx: idx,
                    negated: false,
                });
            }
        }
        // Check for NOT EXISTS: Unary(Not, SubqueryResult { Exists })
        if let Expr::Unary(UnaryOperator::Not, inner) = &term.expr {
            if let Expr::SubqueryResult {
                subquery_id: sid,
                query_type: ast::SubqueryType::Exists { .. },
                ..
            } = inner.as_ref()
            {
                if *sid == subquery_id {
                    return Some(ExistsWhereInfo {
                        where_term_idx: idx,
                        negated: true,
                    });
                }
            }
        }
    }
    None
}

/// Check if a predicate expression is valid for unnesting.
/// Valid predicates are:
/// - Pure inner-table predicates (no outer refs)
/// - Equality predicates between outer and inner columns (correlation predicates)
/// - Any expression that doesn't reference outer tables in non-equality positions
fn is_valid_unnesting_predicate(
    expr: &Expr,
    outer_table_ids: &[TableInternalId],
    inner_table_ids: &[TableInternalId],
) -> bool {
    // Check if the expression references any outer tables.
    let mut has_outer_ref = false;
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        if let Expr::Column { table, .. } = e {
            if outer_table_ids.contains(table) {
                has_outer_ref = true;
            }
        }
        Ok(WalkControl::Continue)
    });

    if !has_outer_ref {
        // Pure inner predicate: always valid.
        return true;
    }

    // For predicates with outer refs, we only support simple equality:
    // inner_col = outer_col or outer_col = inner_col
    is_correlation_equality(expr, outer_table_ids, inner_table_ids)
}

/// Check if an expression is a simple equality between an outer and inner column reference.
fn is_correlation_equality(
    expr: &Expr,
    outer_table_ids: &[TableInternalId],
    inner_table_ids: &[TableInternalId],
) -> bool {
    if let Expr::Binary(lhs, ast::Operator::Equals, rhs) = expr {
        let lhs_tables = collect_table_refs(lhs);
        let rhs_tables = collect_table_refs(rhs);

        // One side references only outer tables, the other only inner tables.
        let lhs_is_outer =
            lhs_tables.iter().all(|t| outer_table_ids.contains(t)) && !lhs_tables.is_empty();
        let lhs_is_inner =
            lhs_tables.iter().all(|t| inner_table_ids.contains(t)) && !lhs_tables.is_empty();
        let rhs_is_outer =
            rhs_tables.iter().all(|t| outer_table_ids.contains(t)) && !rhs_tables.is_empty();
        let rhs_is_inner =
            rhs_tables.iter().all(|t| inner_table_ids.contains(t)) && !rhs_tables.is_empty();

        (lhs_is_outer && rhs_is_inner) || (lhs_is_inner && rhs_is_outer)
    } else {
        false
    }
}

/// Collect all table IDs referenced by column expressions in an expression tree.
fn collect_table_refs(expr: &Expr) -> SmallVec<[TableInternalId; 2]> {
    let mut refs = SmallVec::new();
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        if let Expr::Column { table, .. } = e {
            if !refs.contains(table) {
                refs.push(*table);
            }
        }
        Ok(WalkControl::Continue)
    });
    refs
}

/// Check if an expression tree contains any non-deterministic function calls
/// (e.g. random(), changes(), last_insert_rowid()).
fn contains_nondeterministic_function(expr: &Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        match e {
            Expr::FunctionCall { name, args, .. } => {
                if let Ok(func) = Func::resolve_function(name.as_str(), args.len()) {
                    if !func.is_deterministic() {
                        found = true;
                    }
                }
            }
            Expr::FunctionCallStar { name, .. } => {
                // Star functions like count(*) — resolve with 0 args
                if let Ok(func) = Func::resolve_function(name.as_str(), 0) {
                    if !func.is_deterministic() {
                        found = true;
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    found
}

/// Replace the WHERE term at the given index with a trivially-true expression.
fn replace_exists_with_true(where_clause: &mut [WhereTerm], idx: usize) {
    where_clause[idx].expr = Expr::Literal(ast::Literal::Numeric("1".to_string()));
}
