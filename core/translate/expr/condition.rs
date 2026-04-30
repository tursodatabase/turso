use super::*;

/// Core implementation of IN expression logic that can be used in both conditional and expression contexts.
/// This follows SQLite's approach where a single core function handles all InList cases.
///
/// This is extracted from the original conditional implementation to be reusable.
/// The logic exactly matches the original conditional InList implementation.
///
/// An IN expression has one of the following formats:
///  ```sql
///      x IN (y1, y2,...,yN)
///      x IN (subquery) (Not yet implemented)
///  ```
/// The result of an IN operator is one of TRUE, FALSE, or NULL.  A NULL result
/// means that it cannot be determined if the LHS is contained in the RHS due
/// to the presence of NULL values.
///
/// Currently, we do a simple full-scan, yet it's not ideal when there are many rows
/// on RHS. (Check sqlite's in-operator.md)
///
/// Algorithm:
/// 1. Set the null-flag to false
/// 2. For each row in the RHS:
///     - Compare LHS and RHS
///     - If LHS matches RHS, returns TRUE
///     - If the comparison results in NULL, set the null-flag to true
/// 3. If the null-flag is true, return NULL
/// 4. Return FALSE
///
/// A "NOT IN" operator is computed by first computing the equivalent IN
/// operator, then interchanging the TRUE and FALSE results.
/// Compute the affinity for an IN expression.
/// For `x IN (y1, y2, ..., yN)`, the affinity is determined by the LHS expression `x`.
/// This follows SQLite's `exprINAffinity()` function.
pub(super) fn in_expr_affinity(
    lhs: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    // Compute the affinity for the IN comparison based on the LHS expression
    // This follows SQLite's exprINAffinity() approach
    // For parenthesized expressions (vectors), we take the first element's affinity
    // since scalar IN comparisons only use the first element
    match lhs {
        Expr::Parenthesized(exprs) if !exprs.is_empty() => {
            get_expr_affinity(&exprs[0], referenced_tables, resolver)
        }
        _ => get_expr_affinity(lhs, referenced_tables, resolver),
    }
}

#[instrument(skip(program, referenced_tables, expr, resolver), level = Level::DEBUG)]
pub fn translate_condition_expr(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    expr: &ast::Expr,
    condition_metadata: ConditionMetadata,
    resolver: &Resolver,
) -> Result<()> {
    let mut translator = IterativeExprTranslator::new(program, resolver);
    translator.schedule_condition_expr(referenced_tables, expr, condition_metadata);
    translator.run()
}
