//! Expression generation for generated columns.

use std::collections::HashSet;

use rand::Rng;
use turso_parser::ast::{self, Expr, Name, Operator, UnaryOperator};

use crate::model::table::{Column, ColumnType};

/// Generates a type-compatible expression for a generated column.
///
/// This function creates expressions that can reference ANY column in the table
/// (including forward references and other generated columns), matching SQLite behavior.
///
/// Returns (expression, set of column indices referenced).
pub fn generate_column_expr_with_refs<R: Rng + ?Sized>(
    rng: &mut R,
    all_columns: &[Column],
    current_col_idx: usize,
    target_type: &ColumnType,
    max_depth: usize,
) -> (ast::Expr, HashSet<usize>) {
    let mut refs = HashSet::new();
    let expr = generate_expr_inner(
        rng,
        all_columns,
        current_col_idx,
        target_type,
        max_depth,
        &mut refs,
    );
    (expr, refs)
}

fn generate_expr_inner<R: Rng + ?Sized>(
    rng: &mut R,
    all_columns: &[Column],
    current_col_idx: usize,
    target_type: &ColumnType,
    depth: usize,
    refs: &mut HashSet<usize>,
) -> ast::Expr {
    // Find columns that are type-compatible (excluding self)
    let compatible_cols: Vec<(usize, &Column)> = all_columns
        .iter()
        .enumerate()
        .filter(|(idx, col)| {
            *idx != current_col_idx && types_compatible(&col.column_type, target_type)
        })
        .collect();

    // Base case: depth 0 or no compatible columns
    if depth == 0 || compatible_cols.is_empty() {
        // Try a column reference first if we have compatible columns
        if !compatible_cols.is_empty() && rng.random_bool(0.7) {
            let (idx, col) = compatible_cols[rng.random_range(0..compatible_cols.len())];
            refs.insert(idx);
            return Expr::Id(Name::from_string(&col.name));
        }
        // Otherwise, use a literal
        return generate_literal(rng, target_type);
    }

    // Choose what kind of expression to generate
    let choice = rng.random_range(0..10);
    match choice {
        // Column reference (40% chance)
        0..=3 => {
            let (idx, col) = compatible_cols[rng.random_range(0..compatible_cols.len())];
            refs.insert(idx);
            Expr::Id(Name::from_string(&col.name))
        }
        // Binary operation (30% chance)
        4..=6 => {
            let op = pick_binary_op(rng, target_type);
            let lhs = generate_expr_inner(
                rng,
                all_columns,
                current_col_idx,
                target_type,
                depth - 1,
                refs,
            );
            let rhs = generate_expr_inner(
                rng,
                all_columns,
                current_col_idx,
                target_type,
                depth - 1,
                refs,
            );
            // Wrap subexpressions in parentheses to ensure correct evaluation order
            // when the shadow model evaluates the expression tree directly
            let lhs = if matches!(lhs, Expr::Binary(..)) {
                Expr::Parenthesized(vec![Box::new(lhs)])
            } else {
                lhs
            };
            let rhs = if matches!(rhs, Expr::Binary(..)) {
                Expr::Parenthesized(vec![Box::new(rhs)])
            } else {
                rhs
            };
            Expr::Binary(Box::new(lhs), op, Box::new(rhs))
        }
        // Unary operation (15% chance)
        7..=8 => {
            if let Some(op) = pick_unary_op(rng, target_type) {
                let inner = generate_expr_inner(
                    rng,
                    all_columns,
                    current_col_idx,
                    target_type,
                    depth - 1,
                    refs,
                );
                // Wrap binary sub-expressions in parentheses to preserve semantics
                // when the expression is displayed as SQL and re-parsed.
                // E.g., Unary(-, Binary(a, -, b)) means "-(a - b)" but without parens
                // would display as "- a - b" which parses as "(-a) - b"
                let inner = if matches!(inner, Expr::Binary(..)) {
                    Expr::Parenthesized(vec![Box::new(inner)])
                } else {
                    inner
                };
                Expr::Unary(op, Box::new(inner))
            } else {
                // Fall back to literal
                generate_literal(rng, target_type)
            }
        }
        // Parenthesized expression (5% chance)
        9 => {
            let inner = generate_expr_inner(
                rng,
                all_columns,
                current_col_idx,
                target_type,
                depth - 1,
                refs,
            );
            Expr::Parenthesized(vec![Box::new(inner)])
        }
        // Literal (10% implicit from other branches)
        _ => generate_literal(rng, target_type),
    }
}

/// Check if two column types are compatible for expressions.
fn types_compatible(source: &ColumnType, target: &ColumnType) -> bool {
    match (source, target) {
        // Integer and Float are interchangeable
        (ColumnType::Integer, ColumnType::Integer)
        | (ColumnType::Integer, ColumnType::Float)
        | (ColumnType::Float, ColumnType::Integer)
        | (ColumnType::Float, ColumnType::Float) => true,
        // Text only with Text
        (ColumnType::Text, ColumnType::Text) => true,
        // Blob only with Blob
        (ColumnType::Blob, ColumnType::Blob) => true,
        _ => false,
    }
}

/// Generate a type-appropriate literal.
fn generate_literal<R: Rng + ?Sized>(rng: &mut R, target_type: &ColumnType) -> Expr {
    match target_type {
        ColumnType::Integer => {
            // Use smaller integer values to avoid overflow in expressions
            let val = rng.random_range(-1000i64..1000);
            Expr::Literal(ast::Literal::Numeric(val.to_string()))
        }
        ColumnType::Float => {
            let val = rng.random_range(-1000.0f64..1000.0);
            Expr::Literal(ast::Literal::Numeric(format!("{val:.4}")))
        }
        ColumnType::Text => {
            // Generate a simple text literal
            let text = format!("gen_{}", rng.random_range(0..100));
            Expr::Literal(ast::Literal::String(format!("'{text}'")))
        }
        ColumnType::Blob => {
            // Generate a simple blob literal
            let bytes: Vec<u8> = (0..4).map(|_| rng.random()).collect();
            Expr::Literal(ast::Literal::Blob(hex::encode(bytes)))
        }
    }
}

/// Pick an appropriate binary operator for the target type.
fn pick_binary_op<R: Rng + ?Sized>(rng: &mut R, target_type: &ColumnType) -> Operator {
    match target_type {
        ColumnType::Integer | ColumnType::Float => {
            // Numeric operations
            let ops = [Operator::Add, Operator::Subtract, Operator::Multiply];
            ops[rng.random_range(0..ops.len())]
        }
        ColumnType::Text => {
            // Text concatenation
            Operator::Concat
        }
        ColumnType::Blob => {
            // Blob concatenation
            Operator::Concat
        }
    }
}

/// Pick an appropriate unary operator for the target type (if any).
fn pick_unary_op<R: Rng + ?Sized>(rng: &mut R, target_type: &ColumnType) -> Option<UnaryOperator> {
    match target_type {
        ColumnType::Integer | ColumnType::Float => {
            if rng.random_bool(0.5) {
                Some(UnaryOperator::Negative)
            } else {
                Some(UnaryOperator::Positive)
            }
        }
        // No meaningful unary ops for text/blob
        _ => None,
    }
}

/// Extract all column references from an expression.
///
/// Recursively walks the AST to find all `Expr::Id` column references.
pub fn extract_column_refs(expr: &ast::Expr) -> HashSet<String> {
    let mut refs = HashSet::new();
    extract_refs_inner(expr, &mut refs);
    refs
}

fn extract_refs_inner(expr: &ast::Expr, refs: &mut HashSet<String>) {
    match expr {
        Expr::Id(name) => {
            refs.insert(name.as_str().to_string());
        }
        Expr::Qualified(_, name) => {
            refs.insert(name.as_str().to_string());
        }
        Expr::DoublyQualified(_, _, name) => {
            refs.insert(name.as_str().to_string());
        }
        Expr::Binary(lhs, _, rhs) => {
            extract_refs_inner(lhs, refs);
            extract_refs_inner(rhs, refs);
        }
        Expr::Unary(_, inner) => {
            extract_refs_inner(inner, refs);
        }
        Expr::Parenthesized(exprs) => {
            for e in exprs {
                extract_refs_inner(e, refs);
            }
        }
        Expr::Cast { expr, .. } => {
            extract_refs_inner(expr, refs);
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            extract_refs_inner(lhs, refs);
            extract_refs_inner(start, refs);
            extract_refs_inner(end, refs);
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            extract_refs_inner(lhs, refs);
            extract_refs_inner(rhs, refs);
            if let Some(esc) = escape {
                extract_refs_inner(esc, refs);
            }
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(b) = base {
                extract_refs_inner(b, refs);
            }
            for (when, then) in when_then_pairs {
                extract_refs_inner(when, refs);
                extract_refs_inner(then, refs);
            }
            if let Some(e) = else_expr {
                extract_refs_inner(e, refs);
            }
        }
        Expr::FunctionCall {
            args, filter_over, ..
        } => {
            for arg in args {
                extract_refs_inner(arg, refs);
            }
            if let Some(filter) = &filter_over.filter_clause {
                extract_refs_inner(filter, refs);
            }
        }
        Expr::InList { lhs, rhs, .. } => {
            extract_refs_inner(lhs, refs);
            for e in rhs {
                extract_refs_inner(e, refs);
            }
        }
        Expr::InSelect { lhs, .. } => {
            extract_refs_inner(lhs, refs);
            // Don't recurse into subquery for now
        }
        Expr::InTable { lhs, .. } => {
            extract_refs_inner(lhs, refs);
        }
        Expr::IsNull(inner) | Expr::NotNull(inner) => {
            extract_refs_inner(inner, refs);
        }
        Expr::Collate(inner, _) => {
            extract_refs_inner(inner, refs);
        }
        // Literals and other leaf nodes don't have column refs
        Expr::Literal(_) | Expr::Variable(_) | Expr::Raise(..) | Expr::Name(_) => {}
        // Subquery doesn't contribute to direct column refs in the expression context
        Expr::Subquery(_) | Expr::Exists(_) | Expr::FunctionCallStar { .. } => {}
        // Internal AST nodes (Register, Column, RowId, Glob) used after analysis
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_extract_column_refs() {
        // Create a simple expression: a + b
        let expr = Expr::Binary(
            Box::new(Expr::Id(Name::from_string("a"))),
            Operator::Add,
            Box::new(Expr::Id(Name::from_string("b"))),
        );
        let refs = extract_column_refs(&expr);
        assert!(refs.contains("a"));
        assert!(refs.contains("b"));
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn test_generate_column_expr() {
        let mut rng = StdRng::seed_from_u64(42);
        let columns = vec![
            Column {
                name: "col_a".to_string(),
                column_type: ColumnType::Integer,
                constraints: vec![],
            },
            Column {
                name: "col_b".to_string(),
                column_type: ColumnType::Integer,
                constraints: vec![],
            },
            Column {
                name: "col_c".to_string(),
                column_type: ColumnType::Text,
                constraints: vec![],
            },
        ];

        // Generate expression for column at index 1 (col_b)
        let (expr, refs) = generate_column_expr_with_refs(
            &mut rng,
            &columns,
            1, // current column index
            &ColumnType::Integer,
            2,
        );

        // Should not reference itself (col_b at index 1)
        assert!(!refs.contains(&1));

        // Expression should be valid
        assert!(!matches!(expr, Expr::Id(name) if name.as_str() == "col_b"));
    }
}
