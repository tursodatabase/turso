use smallvec::SmallVec;
use turso_parser::ast::{self, Expr, TableInternalId};

use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::plan::{JoinType, TableReferences, WhereTerm};

/// Lightweight Copy handle for an Expr::Column, used to avoid cloning full
/// Expr trees during equivalence class construction.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
struct ColRef {
    database: Option<usize>,
    table: TableInternalId,
    column: usize,
    is_rowid_alias: bool,
}

impl ColRef {
    fn extract(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Column {
                database,
                table,
                column,
                is_rowid_alias,
            } => Some(Self {
                database: *database,
                table: *table,
                column: *column,
                is_rowid_alias: *is_rowid_alias,
            }),
            _ => None,
        }
    }

    fn to_expr(self) -> Expr {
        Expr::Column {
            database: self.database,
            table: self.table,
            column: self.column,
            is_rowid_alias: self.is_rowid_alias,
        }
    }
}

/// Derive transitive equalities from column-to-column equality predicates.
///
/// Given `A.x = B.y AND B.y = C.z`, derives `A.x = C.z` (if not already present).
/// Only considers inner-join predicates: skips outer-join ON-clause terms and
/// predicates involving anti-join or semi-join tables (from EXISTS unnesting).
///
/// Returns the number of new WhereTerms added.
pub(crate) fn add_transitive_equalities(
    where_clause: &mut Vec<WhereTerm>,
    table_references: &TableReferences,
) -> usize {
    // Tables joined via Anti or Semi should not participate in transitive closure.
    // Their join predicates have different semantics (exclusion/existence checks).
    let is_eligible = |id: &TableInternalId| -> bool {
        table_references
            .find_joined_table_by_internal_id(*id)
            .is_some_and(|t| {
                t.join_info
                    .as_ref()
                    .is_none_or(|ji| ji.join_type == JoinType::Inner)
            })
    };
    add_transitive_equalities_impl(where_clause, &is_eligible)
}

/// Core implementation that accepts a predicate for table eligibility,
/// allowing tests to bypass TableReferences construction.
fn add_transitive_equalities_impl(
    where_clause: &mut Vec<WhereTerm>,
    is_eligible_table: &dyn Fn(&TableInternalId) -> bool,
) -> usize {
    // Step 1: Collect cross-table column=column equalities from eligible terms.
    let mut equalities: Vec<(ColRef, ColRef)> = Vec::new();
    for term in where_clause.iter() {
        if term.from_outer_join.is_some() || term.consumed {
            continue;
        }
        if let Expr::Binary(lhs, ast::Operator::Equals, rhs) = &term.expr {
            if let (Some(a), Some(b)) = (ColRef::extract(lhs), ColRef::extract(rhs)) {
                if a.table != b.table && is_eligible_table(&a.table) && is_eligible_table(&b.table)
                {
                    equalities.push((a, b));
                }
            }
        }
    }

    if equalities.is_empty() {
        return 0;
    }

    // Step 2: Build equivalence classes.
    // Each class is a set of column refs known to be equal.
    let mut classes: Vec<SmallVec<[ColRef; 4]>> = Vec::new();
    for &(a, b) in &equalities {
        let class_a = classes.iter().position(|c| c.contains(&a));
        let class_b = classes.iter().position(|c| c.contains(&b));
        match (class_a, class_b) {
            (Some(ia), Some(ib)) if ia != ib => {
                // Merge the two classes. Remove the higher index first to avoid shifting.
                let hi = ia.max(ib);
                let lo = ia.min(ib);
                let removed = classes.remove(hi);
                classes[lo].extend(removed);
            }
            (Some(ia), None) => {
                classes[ia].push(b);
            }
            (None, Some(ib)) => {
                classes[ib].push(a);
            }
            (None, None) => {
                classes.push(SmallVec::from_slice(&[a, b]));
            }
            _ => {
                // Same class already — nothing to do.
            }
        }
    }

    // Helper to check if a where term is a column=column equality matching two ColRefs.
    let term_matches_pair = |term: &WhereTerm, a: ColRef, b: ColRef| -> bool {
        if let Expr::Binary(lhs, ast::Operator::Equals, rhs) = &term.expr {
            if let (Some(l), Some(r)) = (ColRef::extract(lhs), ColRef::extract(rhs)) {
                return (l == a && r == b) || (l == b && r == a);
            }
        }
        false
    };

    // Step 3: For each class with 3+ members, generate pairwise equalities
    // that don't already exist.
    let mut derived_count = 0;
    for class in &classes {
        if class.len() < 3 {
            continue;
        }
        for i in 0..class.len() {
            for j in (i + 1)..class.len() {
                let a = class[i];
                let b = class[j];
                let already_exists = where_clause
                    .iter()
                    .any(|term| term_matches_pair(term, a, b));
                if !already_exists {
                    where_clause.push(WhereTerm {
                        expr: Expr::Binary(
                            Box::new(a.to_expr()),
                            ast::Operator::Equals,
                            Box::new(b.to_expr()),
                        ),
                        from_outer_join: None,
                        consumed: false,
                    });
                    derived_count += 1;
                }
            }
        }
    }

    derived_count
}

/// Returns true if the expression contains no column references.
/// Used to identify constant expressions suitable for propagation.
fn is_column_free(expr: &Expr) -> bool {
    let mut ok = true;
    let _ = walk_expr(expr, &mut |e| match e {
        Expr::Column { .. } | Expr::RowId { .. } => {
            ok = false;
            Ok(WalkControl::SkipChildren)
        }
        // walk_expr doesn't descend into subqueries, so reject them explicitly.
        Expr::Exists(_)
        | Expr::Subquery(_)
        | Expr::SubqueryResult { .. }
        | Expr::InSelect { .. } => {
            ok = false;
            Ok(WalkControl::SkipChildren)
        }
        _ => Ok(WalkControl::Continue),
    });
    ok
}

/// Propagate constant values through column equality chains.
///
/// Given `A.x = 5 AND A.x = B.y`, derives `B.y = 5` (if not already present).
/// Should be called after [`add_transitive_equalities`] so that all column=column
/// equivalences are already materialized.
///
/// Returns the number of new WhereTerms added.
pub(crate) fn add_constant_propagation(
    where_clause: &mut Vec<WhereTerm>,
    table_references: &TableReferences,
) -> usize {
    let is_eligible = |id: &TableInternalId| -> bool {
        table_references
            .find_joined_table_by_internal_id(*id)
            .is_some_and(|t| {
                t.join_info
                    .as_ref()
                    .is_none_or(|ji| ji.join_type == JoinType::Inner)
            })
    };
    add_constant_propagation_impl(where_clause, &is_eligible)
}

fn add_constant_propagation_impl(
    where_clause: &mut Vec<WhereTerm>,
    is_eligible_table: &dyn Fn(&TableInternalId) -> bool,
) -> usize {
    // Phase 1: Collect (column, constant_expr) pairs from `col = const` terms.
    let mut col_constants: Vec<(ColRef, Expr)> = Vec::new();
    for term in where_clause.iter() {
        if term.from_outer_join.is_some() || term.consumed {
            continue;
        }
        if let Expr::Binary(lhs, ast::Operator::Equals, rhs) = &term.expr {
            if let Some(col) = ColRef::extract(lhs) {
                if is_eligible_table(&col.table) && is_column_free(rhs) {
                    col_constants.push((col, rhs.as_ref().clone()));
                }
            } else if let Some(col) = ColRef::extract(rhs) {
                if is_eligible_table(&col.table) && is_column_free(lhs) {
                    col_constants.push((col, lhs.as_ref().clone()));
                }
            }
        }
    }

    if col_constants.is_empty() {
        return 0;
    }

    // Phase 2: For each col_a = col_b equality, if one side has a constant, derive
    // the other side = constant. Also track which join terms become redundant
    // (both sides have constants) so we can mark them consumed.
    let mut new_terms: Vec<(ColRef, Expr)> = Vec::new();
    let mut redundant_join_terms: Vec<usize> = Vec::new();
    for (term_idx, term) in where_clause.iter().enumerate() {
        if term.from_outer_join.is_some() || term.consumed {
            continue;
        }
        if let Expr::Binary(lhs, ast::Operator::Equals, rhs) = &term.expr {
            if let (Some(a), Some(b)) = (ColRef::extract(lhs), ColRef::extract(rhs)) {
                if a.table == b.table {
                    continue;
                }
                let a_has_const = col_constants.iter().any(|(col, _)| *col == a);
                let b_has_const = col_constants.iter().any(|(col, _)| *col == b);
                for (col, const_expr) in &col_constants {
                    if *col == a && is_eligible_table(&b.table) {
                        new_terms.push((b, const_expr.clone()));
                    }
                    if *col == b && is_eligible_table(&a.table) {
                        new_terms.push((a, const_expr.clone()));
                    }
                }
                // If both sides have (or will have) constants, the join
                // predicate a = b is implied and can be consumed.
                if a_has_const || b_has_const {
                    // After propagation, both sides will have constants
                    // (the side that didn't have one gets one from new_terms).
                    redundant_join_terms.push(term_idx);
                }
            }
        }
    }

    // Phase 3: Add new terms, skipping duplicates.
    let mut added = 0;
    for (col, const_expr) in new_terms {
        let already_exists = where_clause.iter().any(|term| {
            if let Expr::Binary(lhs, ast::Operator::Equals, rhs) = &term.expr {
                if let Some(c) = ColRef::extract(lhs) {
                    if c == col && *rhs.as_ref() == const_expr {
                        return true;
                    }
                }
                if let Some(c) = ColRef::extract(rhs) {
                    if c == col && *lhs.as_ref() == const_expr {
                        return true;
                    }
                }
            }
            false
        });

        if !already_exists {
            where_clause.push(WhereTerm {
                expr: Expr::Binary(
                    Box::new(col.to_expr()),
                    ast::Operator::Equals,
                    Box::new(const_expr),
                ),
                from_outer_join: None,
                consumed: false,
            });
            added += 1;
        }
    }

    // Phase 4: Mark redundant join predicates as consumed. If a = const and
    // b = const, then a = b is implied and need not be checked at runtime.
    if added > 0 {
        for idx in redundant_join_terms {
            where_clause[idx].consumed = true;
        }
    }

    added
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_parser::ast::{Operator, Variable};

    /// All tables eligible (simulates all-inner-join query).
    fn all_eligible(_: &TableInternalId) -> bool {
        true
    }

    fn col(table: usize, column: usize) -> Expr {
        Expr::Column {
            database: None,
            table: TableInternalId::from(table),
            column,
            is_rowid_alias: false,
        }
    }

    fn eq_term(lhs: Expr, rhs: Expr) -> WhereTerm {
        WhereTerm {
            expr: Expr::Binary(Box::new(lhs), Operator::Equals, Box::new(rhs)),
            from_outer_join: None,
            consumed: false,
        }
    }

    fn is_col_eq(term: &WhereTerm, t1: usize, c1: usize, t2: usize, c2: usize) -> bool {
        if let Expr::Binary(lhs, Operator::Equals, rhs) = &term.expr {
            if let (Some(l), Some(r)) = (ColRef::extract(lhs), ColRef::extract(rhs)) {
                return (l.table == TableInternalId::from(t1)
                    && l.column == c1
                    && r.table == TableInternalId::from(t2)
                    && r.column == c2)
                    || (l.table == TableInternalId::from(t2)
                        && l.column == c2
                        && r.table == TableInternalId::from(t1)
                        && r.column == c1);
            }
        }
        false
    }

    #[test]
    fn no_equalities_no_derivations() {
        let mut wc = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(col(1, 0)),
                Operator::GreaterEquals,
                Box::new(Expr::Literal(ast::Literal::Numeric("5".to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        }];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 1);
    }

    #[test]
    fn two_tables_no_transitive() {
        // A.0 = B.0 — only two columns, no transitive derivation possible
        let mut wc = vec![eq_term(col(1, 0), col(2, 0))];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 1);
    }

    #[test]
    fn three_tables_derives_one() {
        // A.0 = B.0, B.0 = C.0 → derives A.0 = C.0
        let mut wc = vec![eq_term(col(1, 0), col(2, 0)), eq_term(col(2, 0), col(3, 0))];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 1);
        assert_eq!(wc.len(), 3);
        assert!(is_col_eq(&wc[2], 1, 0, 3, 0));
    }

    #[test]
    fn already_exists_no_duplicate() {
        // A.0 = B.0, B.0 = C.0, A.0 = C.0 — all pairs present, nothing to derive
        let mut wc = vec![
            eq_term(col(1, 0), col(2, 0)),
            eq_term(col(2, 0), col(3, 0)),
            eq_term(col(1, 0), col(3, 0)),
        ];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 3);
    }

    #[test]
    fn four_tables_chain() {
        // A=B, B=C, C=D → derives A=C, A=D, B=D (3 new terms)
        let mut wc = vec![
            eq_term(col(1, 0), col(2, 0)),
            eq_term(col(2, 0), col(3, 0)),
            eq_term(col(3, 0), col(4, 0)),
        ];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 3);
        assert_eq!(wc.len(), 6);
    }

    #[test]
    fn consumed_terms_skipped() {
        let mut wc = vec![
            eq_term(col(1, 0), col(2, 0)),
            WhereTerm {
                expr: Expr::Binary(Box::new(col(2, 0)), Operator::Equals, Box::new(col(3, 0))),
                from_outer_join: None,
                consumed: true,
            },
        ];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 2);
    }

    #[test]
    fn outer_join_terms_skipped() {
        let mut wc = vec![
            eq_term(col(1, 0), col(2, 0)),
            WhereTerm {
                expr: Expr::Binary(Box::new(col(2, 0)), Operator::Equals, Box::new(col(3, 0))),
                from_outer_join: Some(TableInternalId::from(3)),
                consumed: false,
            },
        ];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 2);
    }

    #[test]
    fn different_columns_separate_classes() {
        // A.0 = B.0, B.1 = C.1 — different columns, no transitive link
        let mut wc = vec![eq_term(col(1, 0), col(2, 0)), eq_term(col(2, 1), col(3, 1))];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 2);
    }

    #[test]
    fn ineligible_table_excluded() {
        // A.0 = B.0, B.0 = C.0, but table 3 (C) is ineligible (e.g. semi-join)
        let mut wc = vec![eq_term(col(1, 0), col(2, 0)), eq_term(col(2, 0), col(3, 0))];
        let not_table_3 = |id: &TableInternalId| *id != TableInternalId::from(3);
        assert_eq!(add_transitive_equalities_impl(&mut wc, &not_table_3), 0);
        assert_eq!(wc.len(), 2);
    }

    #[test]
    fn same_table_equality_ignored() {
        // A.0 = A.1 — same table, should not be collected
        let mut wc = vec![
            eq_term(col(1, 0), col(1, 1)),
            eq_term(col(1, 0), col(2, 0)),
            eq_term(col(2, 0), col(3, 0)),
        ];
        assert_eq!(add_transitive_equalities_impl(&mut wc, &all_eligible), 1);
        assert_eq!(wc.len(), 4);
        assert!(is_col_eq(&wc[3], 1, 0, 3, 0));
    }

    // --- Constant propagation tests ---

    fn num_lit(n: &str) -> Expr {
        Expr::Literal(ast::Literal::Numeric(n.to_string()))
    }

    fn str_lit(s: &str) -> Expr {
        Expr::Literal(ast::Literal::String(s.to_string()))
    }

    /// Check if a term is `col(t, c) = expr` (or `expr = col(t, c)`).
    fn is_col_const(term: &WhereTerm, t: usize, c: usize, expected: &Expr) -> bool {
        if let Expr::Binary(lhs, Operator::Equals, rhs) = &term.expr {
            if let Some(cr) = ColRef::extract(lhs) {
                if cr.table == TableInternalId::from(t)
                    && cr.column == c
                    && rhs.as_ref() == expected
                {
                    return true;
                }
            }
            if let Some(cr) = ColRef::extract(rhs) {
                if cr.table == TableInternalId::from(t)
                    && cr.column == c
                    && lhs.as_ref() == expected
                {
                    return true;
                }
            }
        }
        false
    }

    #[test]
    fn constant_prop_basic() {
        // A.0 = 5, A.0 = B.0 → derives B.0 = 5
        let five = num_lit("5");
        let mut wc = vec![
            eq_term(col(1, 0), five.clone()),
            eq_term(col(1, 0), col(2, 0)),
        ];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 1);
        assert_eq!(wc.len(), 3);
        assert!(is_col_const(&wc[2], 2, 0, &five));
    }

    #[test]
    fn constant_prop_no_column_equality() {
        // A.0 = 5, no column=column term → nothing to propagate
        let mut wc = vec![eq_term(col(1, 0), num_lit("5"))];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 1);
    }

    #[test]
    fn constant_prop_null_propagated() {
        // A.0 = NULL, A.0 = B.0 → derives B.0 = NULL
        // (harmless: both `a = NULL` and `b = NULL` are always-unknown in WHERE context)
        let null = Expr::Literal(ast::Literal::Null);
        let mut wc = vec![
            eq_term(col(1, 0), null.clone()),
            eq_term(col(1, 0), col(2, 0)),
        ];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 1);
        assert_eq!(wc.len(), 3);
        assert!(is_col_const(&wc[2], 2, 0, &null));
    }

    #[test]
    fn constant_prop_bind_param() {
        // A.0 = ?1, A.0 = B.0 → derives B.0 = ?1
        let param = Expr::Variable(Variable::indexed(1.try_into().unwrap()));
        let mut wc = vec![
            eq_term(col(1, 0), param.clone()),
            eq_term(col(1, 0), col(2, 0)),
        ];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 1);
        assert_eq!(wc.len(), 3);
        assert!(is_col_const(&wc[2], 2, 0, &param));
    }

    #[test]
    fn constant_prop_chain() {
        // A.0 = 5, A.0 = B.0, B.0 = C.0
        // After transitive: adds A.0 = C.0
        // After constant prop: adds B.0 = 5, C.0 = 5
        let five = num_lit("5");
        let mut wc = vec![
            eq_term(col(1, 0), five.clone()),
            eq_term(col(1, 0), col(2, 0)),
            eq_term(col(2, 0), col(3, 0)),
        ];
        // Run transitive first (as the real pipeline does)
        let transitive = add_transitive_equalities_impl(&mut wc, &all_eligible);
        assert_eq!(transitive, 1); // A.0 = C.0
        assert_eq!(wc.len(), 4);

        // Now constant propagation
        let propagated = add_constant_propagation_impl(&mut wc, &all_eligible);
        assert_eq!(propagated, 2); // B.0 = 5, C.0 = 5
        assert_eq!(wc.len(), 6);
        assert!(is_col_const(&wc[4], 2, 0, &five));
        assert!(is_col_const(&wc[5], 3, 0, &five));
    }

    #[test]
    fn constant_prop_already_exists() {
        // A.0 = 5, A.0 = B.0, B.0 = 5 → no new derivation
        let five = num_lit("5");
        let mut wc = vec![
            eq_term(col(1, 0), five.clone()),
            eq_term(col(1, 0), col(2, 0)),
            eq_term(col(2, 0), five),
        ];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 3);
    }

    #[test]
    fn constant_prop_outer_join_skipped() {
        // A.0 = 5 (from outer join ON clause) → skipped
        let mut wc = vec![
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(col(1, 0)),
                    Operator::Equals,
                    Box::new(num_lit("5")),
                ),
                from_outer_join: Some(TableInternalId::from(1)),
                consumed: false,
            },
            eq_term(col(1, 0), col(2, 0)),
        ];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 0);
        assert_eq!(wc.len(), 2);
    }

    #[test]
    fn constant_prop_bidirectional() {
        // 5 = A.0, A.0 = B.0 → derives B.0 = 5 (constant on LHS)
        let five = num_lit("5");
        let mut wc = vec![
            eq_term(five.clone(), col(1, 0)),
            eq_term(col(1, 0), col(2, 0)),
        ];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 1);
        assert_eq!(wc.len(), 3);
        assert!(is_col_const(&wc[2], 2, 0, &five));
    }

    #[test]
    fn constant_prop_string_literal() {
        // A.0 = 'hello', A.0 = B.0 → derives B.0 = 'hello'
        let hello = str_lit("hello");
        let mut wc = vec![
            eq_term(col(1, 0), hello.clone()),
            eq_term(col(1, 0), col(2, 0)),
        ];
        assert_eq!(add_constant_propagation_impl(&mut wc, &all_eligible), 1);
        assert_eq!(wc.len(), 3);
        assert!(is_col_const(&wc[2], 2, 0, &hello));
    }

    #[test]
    fn constant_prop_ineligible_table() {
        // A.0 = 5, A.0 = B.0, but table 2 (B) is ineligible
        let mut wc = vec![
            eq_term(col(1, 0), num_lit("5")),
            eq_term(col(1, 0), col(2, 0)),
        ];
        let not_table_2 = |id: &TableInternalId| *id != TableInternalId::from(2);
        assert_eq!(add_constant_propagation_impl(&mut wc, &not_table_2), 0);
        assert_eq!(wc.len(), 2);
    }
}
