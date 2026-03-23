//! Decompose cross-table OR predicates into per-table IN/equality terms.
//!
//! Following SQLite's `exprAnalyzeOrTerm` CASE 1 (OR-to-IN), extended to
//! handle AND-branches.

use turso_parser::ast::{Expr, Operator, TableInternalId};

use crate::translate::{
    expr::{walk_expr, WalkControl},
    plan::WhereTerm,
};

use super::{flatten_and_refs, flatten_or_refs};

/// Decompose cross-table OR predicates into per-table IN/equality terms.
///
/// Given `(n1.name = 'X' AND n2.name = 'Y') OR (n1.name = 'Z' AND n2.name = 'W')`,
/// this adds synthetic WHERE terms:
///   - `n1.name IN ('X', 'Z')`
///   - `n2.name IN ('Y', 'W')`
///
/// The original OR is NOT consumed — the synthetic terms are over-approximations
/// that inform the optimizer about per-table selectivity. The OR still evaluates
/// at runtime to enforce correct cross-table value combinations.
pub(crate) fn decompose_cross_table_ors(where_clause: &mut Vec<WhereTerm>) {
    let original_len = where_clause.len();
    let mut i = 0;
    while i < original_len {
        if where_clause[i].consumed {
            i += 1;
            continue;
        }
        if !matches!(where_clause[i].expr, Expr::Binary(_, Operator::Or, _)) {
            i += 1;
            continue;
        }

        let from_outer_join = where_clause[i].from_outer_join;
        let new_terms = extract_per_table_terms(&where_clause[i].expr);

        for term_expr in new_terms {
            // Mark synthetic terms as consumed: they are over-approximations
            // meant only for optimizer selectivity estimation and access method
            // selection, not for runtime evaluation. The original OR handles
            // correctness. Constraint extraction ignores the consumed flag,
            // so the optimizer still sees these terms.
            where_clause.push(WhereTerm {
                expr: term_expr,
                from_outer_join,
                consumed: true,
            });
        }

        i += 1;
    }
}

/// Extract per-table IN/equality terms from an OR expression.
///
/// Returns a list of synthetic expressions to add as new WHERE terms.
fn extract_per_table_terms(or_expr: &Expr) -> Vec<Expr> {
    let branches = flatten_or_refs(or_expr);
    if branches.len() < 2 {
        return vec![];
    }

    // For each branch, flatten AND subterms and collect column = constant equalities.
    // Each entry: Vec of (table_id, col_idx, col_expr, value_expr) per branch.
    let mut branch_equalities: Vec<Vec<(TableInternalId, usize, &Expr, &Expr)>> = Vec::new();

    for branch in &branches {
        let subterms = flatten_and_refs(branch);
        let mut eqs = Vec::new();
        for subterm in subterms {
            if let Some((col_expr, table_id, col_idx, value_expr)) =
                extract_column_eq_constant(subterm)
            {
                eqs.push((table_id, col_idx, col_expr, value_expr));
            }
        }
        branch_equalities.push(eqs);
    }

    // Collect all (table, column) pairs from any branch.
    let mut candidates: Vec<(TableInternalId, usize)> = Vec::new();
    for eqs in &branch_equalities {
        for &(table_id, col_idx, _, _) in eqs {
            if !candidates
                .iter()
                .any(|(t, c)| *t == table_id && *c == col_idx)
            {
                candidates.push((table_id, col_idx));
            }
        }
    }

    let mut result = Vec::new();

    for &(table_id, col_idx) in &candidates {
        // Check that ALL branches have exactly one equality on this (table, column).
        let mut all_branches_have_eq = true;
        let mut col_expr_template: Option<&Expr> = None;
        let mut values: Vec<&Expr> = Vec::new();

        for eqs in &branch_equalities {
            let matching: Vec<_> = eqs
                .iter()
                .filter(|(t, c, _, _)| *t == table_id && *c == col_idx)
                .collect();
            if matching.len() != 1 {
                all_branches_have_eq = false;
                break;
            }
            let (_, _, col_e, val_e) = matching[0];
            col_expr_template = Some(col_e);
            // Deduplicate values using structural equality (PartialEq on Expr).
            if !values.contains(val_e) {
                values.push(val_e);
            }
        }

        if !all_branches_have_eq {
            continue;
        }
        let col_expr = match col_expr_template {
            Some(e) => e,
            None => continue,
        };

        if values.len() == 1 {
            // Single unique value across all branches -> emit equality.
            result.push(Expr::Binary(
                Box::new(col_expr.clone()),
                Operator::Equals,
                Box::new(values[0].clone()),
            ));
        } else {
            // Multiple values -> emit IN list.
            result.push(Expr::InList {
                lhs: Box::new(col_expr.clone()),
                not: false,
                rhs: values.iter().map(|v| Box::new((*v).clone())).collect(),
            });
        }
    }

    result
}

/// Check if an expression is `Column = constant` or `constant = Column`.
///
/// Returns `(column_expr, table_id, column_idx, value_expr)` if so.
/// "Constant" means the value side contains no `Expr::Column` references.
fn extract_column_eq_constant(expr: &Expr) -> Option<(&Expr, TableInternalId, usize, &Expr)> {
    let Expr::Binary(lhs, Operator::Equals, rhs) = expr else {
        return None;
    };

    // Try lhs = Column, rhs = constant
    if let Some((table_id, col_idx)) = extract_column_info(lhs) {
        if !expr_references_any_column(rhs) {
            return Some((lhs, table_id, col_idx, rhs));
        }
    }

    // Try rhs = Column, lhs = constant
    if let Some((table_id, col_idx)) = extract_column_info(rhs) {
        if !expr_references_any_column(lhs) {
            return Some((rhs, table_id, col_idx, lhs));
        }
    }

    None
}

/// Extract table and column info from an expression, unwrapping parentheses.
fn extract_column_info(expr: &Expr) -> Option<(TableInternalId, usize)> {
    match expr {
        Expr::Column { table, column, .. } => Some((*table, *column)),
        Expr::Parenthesized(exprs) if exprs.len() == 1 => extract_column_info(&exprs[0]),
        _ => None,
    }
}

/// Returns true if the expression contains any `Expr::Column` reference.
fn expr_references_any_column(expr: &Expr) -> bool {
    let mut has_column = false;
    let _ = walk_expr(expr, &mut |e| {
        if matches!(e, Expr::Column { .. }) {
            has_column = true;
        }
        Ok(WalkControl::Continue)
    });
    has_column
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_parser::ast::Literal;

    fn col(table_id: usize, column: usize) -> Expr {
        Expr::Column {
            database: None,
            table: TableInternalId::from(table_id),
            column,
            is_rowid_alias: false,
        }
    }

    fn lit_str(s: &str) -> Expr {
        Expr::Literal(Literal::String(s.to_string()))
    }

    fn lit_num(n: &str) -> Expr {
        Expr::Literal(Literal::Numeric(n.to_string()))
    }

    fn eq(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary(Box::new(lhs), Operator::Equals, Box::new(rhs))
    }

    fn and(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary(Box::new(lhs), Operator::And, Box::new(rhs))
    }

    fn or(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary(Box::new(lhs), Operator::Or, Box::new(rhs))
    }

    fn paren(e: Expr) -> Expr {
        Expr::Parenthesized(vec![e.into()])
    }

    #[test]
    fn simple_or_to_in() {
        // (x = 1) OR (x = 2) -> adds x IN (1, 2)
        let x = col(1, 0);
        let or_expr = or(eq(x.clone(), lit_num("1")), eq(x, lit_num("2")));

        let mut wc = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert_eq!(wc.len(), 2, "should add one synthetic term");
        assert!(!wc[0].consumed, "original OR should not be consumed");
        assert!(
            matches!(&wc[1].expr, Expr::InList { lhs, not: false, rhs }
                if matches!(lhs.as_ref(), Expr::Column { column: 0, .. })
                && rhs.len() == 2),
            "should produce x IN (1, 2), got: {:?}",
            wc[1].expr
        );
    }

    #[test]
    fn cross_table_and_branches() {
        // (a.x = 1 AND b.y = 2) OR (a.x = 3 AND b.y = 4)
        // -> adds a.x IN (1, 3) and b.y IN (2, 4)
        let ax = col(1, 0);
        let by = col(2, 0);

        let branch1 = paren(and(
            eq(ax.clone(), lit_num("1")),
            eq(by.clone(), lit_num("2")),
        ));
        let branch2 = paren(and(eq(ax, lit_num("3")), eq(by, lit_num("4"))));
        let or_expr = or(branch1, branch2);

        let mut wc = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert_eq!(wc.len(), 3, "should add two synthetic terms");
        // a.x IN (1, 3)
        assert!(
            matches!(&wc[1].expr, Expr::InList { lhs, not: false, rhs }
                if matches!(lhs.as_ref(), Expr::Column { table, column: 0, .. } if usize::from(*table) == 1)
                && rhs.len() == 2),
            "expected a.x IN (1,3), got: {:?}",
            wc[1].expr
        );
        // b.y IN (2, 4)
        assert!(
            matches!(&wc[2].expr, Expr::InList { lhs, not: false, rhs }
                if matches!(lhs.as_ref(), Expr::Column { table, column: 0, .. } if usize::from(*table) == 2)
                && rhs.len() == 2),
            "expected b.y IN (2,4), got: {:?}",
            wc[2].expr
        );
    }

    #[test]
    fn branch_missing_equality_for_one_table() {
        // (a.x = 1 AND b.y = 2) OR (a.x = 3)
        // -> adds a.x IN (1, 3) only (b.y missing from second branch)
        let ax = col(1, 0);
        let by = col(2, 0);

        let branch1 = paren(and(eq(ax.clone(), lit_num("1")), eq(by, lit_num("2"))));
        let branch2 = eq(ax, lit_num("3"));
        let or_expr = or(branch1, branch2);

        let mut wc = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert_eq!(wc.len(), 2, "should add one synthetic term (a.x only)");
        assert!(
            matches!(&wc[1].expr, Expr::InList { lhs, not: false, rhs }
                if matches!(lhs.as_ref(), Expr::Column { table, column: 0, .. } if usize::from(*table) == 1)
                && rhs.len() == 2),
            "expected a.x IN (1,3), got: {:?}",
            wc[1].expr
        );
    }

    #[test]
    fn non_constant_rhs_skipped() {
        // (a.x = b.y) OR (a.x = 1)
        // -> no IN extracted (b.y is not constant)
        let ax = col(1, 0);
        let by = col(2, 0);

        let or_expr = or(eq(ax.clone(), by), eq(ax, lit_num("1")));

        let mut wc = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert_eq!(wc.len(), 1, "no synthetic terms when RHS is not constant");
    }

    #[test]
    fn duplicate_values_collapsed_to_equality() {
        // (a.x = 1 AND b.y = 2) OR (a.x = 1 AND b.y = 3)
        // -> a.x = 1 (single value) and b.y IN (2, 3)
        let ax = col(1, 0);
        let by = col(2, 0);

        let branch1 = paren(and(
            eq(ax.clone(), lit_num("1")),
            eq(by.clone(), lit_num("2")),
        ));
        let branch2 = paren(and(eq(ax, lit_num("1")), eq(by, lit_num("3"))));
        let or_expr = or(branch1, branch2);

        let mut wc = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert_eq!(wc.len(), 3, "should add two synthetic terms");
        // a.x = 1 (equality, not IN)
        assert!(
            matches!(&wc[1].expr, Expr::Binary(lhs, Operator::Equals, rhs)
                if matches!(lhs.as_ref(), Expr::Column { table, column: 0, .. } if usize::from(*table) == 1)
                && matches!(rhs.as_ref(), Expr::Literal(Literal::Numeric(n)) if n == "1")),
            "expected a.x = 1, got: {:?}",
            wc[1].expr
        );
        // b.y IN (2, 3)
        assert!(
            matches!(&wc[2].expr, Expr::InList { not: false, rhs, .. } if rhs.len() == 2),
            "expected b.y IN (2,3), got: {:?}",
            wc[2].expr
        );
    }

    #[test]
    fn original_or_not_consumed() {
        let x = col(1, 0);
        let or_expr = or(eq(x.clone(), lit_num("1")), eq(x, lit_num("2")));

        let mut wc = vec![WhereTerm {
            expr: or_expr.clone(),
            from_outer_join: None,
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert!(!wc[0].consumed);
        assert_eq!(wc[0].expr, or_expr);
    }

    #[test]
    fn q7_pattern() {
        // (n1.name = 'ROMANIA' AND n2.name = 'INDIA') OR (n1.name = 'INDIA' AND n2.name = 'ROMANIA')
        // -> n1.name IN ('ROMANIA', 'INDIA') and n2.name IN ('INDIA', 'ROMANIA')
        let n1_name = col(5, 1); // table 5, column 1
        let n2_name = col(6, 1); // table 6, column 1

        let branch1 = paren(and(
            eq(n1_name.clone(), lit_str("'ROMANIA'")),
            eq(n2_name.clone(), lit_str("'INDIA'")),
        ));
        let branch2 = paren(and(
            eq(n1_name, lit_str("'INDIA'")),
            eq(n2_name, lit_str("'ROMANIA'")),
        ));
        let or_expr = or(branch1, branch2);

        let mut wc = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert_eq!(wc.len(), 3, "should add n1 IN and n2 IN terms");
        // n1.name IN ('ROMANIA', 'INDIA')
        assert!(
            matches!(&wc[1].expr, Expr::InList { lhs, not: false, rhs }
                if matches!(lhs.as_ref(), Expr::Column { table, column: 1, .. } if usize::from(*table) == 5)
                && rhs.len() == 2),
            "expected n1.name IN (...), got: {:?}",
            wc[1].expr
        );
        // n2.name IN ('INDIA', 'ROMANIA')
        assert!(
            matches!(&wc[2].expr, Expr::InList { lhs, not: false, rhs }
                if matches!(lhs.as_ref(), Expr::Column { table, column: 1, .. } if usize::from(*table) == 6)
                && rhs.len() == 2),
            "expected n2.name IN (...), got: {:?}",
            wc[2].expr
        );
    }

    #[test]
    fn from_outer_join_preserved() {
        let x = col(1, 0);
        let or_expr = or(eq(x.clone(), lit_num("1")), eq(x, lit_num("2")));

        let outer_join_id = TableInternalId::from(42);
        let mut wc = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: Some(outer_join_id),
            consumed: false,
        }];

        decompose_cross_table_ors(&mut wc);

        assert_eq!(wc.len(), 2);
        assert_eq!(wc[1].from_outer_join, Some(outer_join_id));
    }
}
