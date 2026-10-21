use crate::function::{AggFunc, Func};
use crate::translate::plan::TableReferences;
use turso_parser::ast::{self, Expr};

/// Check if an expression is known to produce an array value.
pub(crate) fn expr_is_array(expr: &Expr, referenced_tables: Option<&TableReferences>) -> bool {
    match expr {
        Expr::Column { table, column, .. } => {
            if let Some(tables) = referenced_tables {
                tables
                    .find_table_by_internal_id(*table)
                    .map(|(_, t)| t)
                    .and_then(|t| t.get_column_at(*column))
                    .is_some_and(|col| col.is_array())
            } else {
                false
            }
        }
        Expr::FunctionCall { name, args, .. } => {
            if let Ok(Some(f)) = Func::resolve_function(name.as_str(), args.len()) {
                match &f {
                    Func::Scalar(sf) if sf.returns_array_blob() => return true,
                    Func::Agg(AggFunc::ArrayAgg) => return true,
                    _ => {}
                }
            }
            // Wrapper functions that pass through an array value
            match name.as_str().to_lowercase().as_str() {
                "coalesce" | "ifnull" | "min" | "max" => {
                    args.iter().any(|a| expr_is_array(a, referenced_tables))
                }
                "iif" => {
                    // args: condition, then_val, else_val
                    args.get(1)
                        .is_some_and(|a| expr_is_array(a, referenced_tables))
                        || args
                            .get(2)
                            .is_some_and(|a| expr_is_array(a, referenced_tables))
                }
                "nullif" => args
                    .first()
                    .is_some_and(|a| expr_is_array(a, referenced_tables)),
                "array_element" => {
                    // Subscripting a multi-dim array yields a lower-dim array
                    if let Some(tables) = referenced_tables {
                        args.first()
                            .is_some_and(|a| expr_array_dimensions(a, tables) > 1)
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
        Expr::Array { .. } | Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
        Expr::Binary(lhs, ast::Operator::Concat, rhs) => {
            expr_is_array(lhs, referenced_tables) || expr_is_array(rhs, referenced_tables)
        }
        Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            when_then_pairs
                .iter()
                .any(|(_, then_expr)| expr_is_array(then_expr, referenced_tables))
                || else_expr
                    .as_ref()
                    .is_some_and(|e| expr_is_array(e, referenced_tables))
        }
        _ => false,
    }
}

/// Return the number of array dimensions for an expression, or 0 for non-array.
pub(super) fn expr_array_dimensions(expr: &Expr, tables: &TableReferences) -> u32 {
    match expr {
        Expr::Column { table, column, .. } => tables
            .find_table_by_internal_id(*table)
            .map(|(_, t)| t)
            .and_then(|t| t.get_column_at(*column))
            .map(|col| col.array_dimensions())
            .unwrap_or(0),
        Expr::FunctionCall { name, args, .. }
            if name.as_str().eq_ignore_ascii_case("array_element") =>
        {
            let d = args
                .first()
                .map(|a| expr_array_dimensions(a, tables))
                .unwrap_or(0);
            d.saturating_sub(1)
        }
        Expr::FunctionCall { name, .. } if name.as_str().eq_ignore_ascii_case("array") => 1,
        Expr::Subscript { .. } | Expr::Array { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
        _ => 0,
    }
}
