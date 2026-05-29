use super::*;
use crate::{translate::alter::literal_default_value, types::ValueType};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExprAffinityInfo {
    affinity: Affinity,
    has_affinity: bool,
}

impl ExprAffinityInfo {
    const fn with_affinity(affinity: Affinity) -> Self {
        Self {
            affinity,
            has_affinity: true,
        }
    }

    const fn no_affinity() -> Self {
        Self {
            affinity: Affinity::Blob,
            has_affinity: false,
        }
    }
}

pub(crate) fn get_expr_affinity_info(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> ExprAffinityInfo {
    match expr {
        ast::Expr::Column { table, column, .. } => {
            if table.is_self_table() {
                if let Some(resolver) = resolver {
                    if let Some(aff) = resolver.self_table_affinity(*column) {
                        return ExprAffinityInfo::with_affinity(aff);
                    }
                }
            }
            if let Some(tables) = referenced_tables {
                if let Some((_, table_ref)) = tables.find_table_by_internal_id(*table) {
                    if let Some(col) = table_ref.get_column_at(*column) {
                        if let Some(btree) = table_ref.btree() {
                            return ExprAffinityInfo::with_affinity(
                                col.affinity_with_strict(btree.is_strict),
                            );
                        }
                        return ExprAffinityInfo::with_affinity(col.affinity());
                    }
                }
            }
            ExprAffinityInfo::no_affinity()
        }
        ast::Expr::RowId { .. } => ExprAffinityInfo::with_affinity(Affinity::Integer),
        ast::Expr::Cast { type_name, .. } => {
            if let Some(type_name) = type_name {
                ExprAffinityInfo::with_affinity(Affinity::affinity(&type_name.name))
            } else {
                ExprAffinityInfo::no_affinity()
            }
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            get_expr_affinity_info(exprs.first().unwrap(), referenced_tables, resolver)
        }
        ast::Expr::Collate(expr, _) => get_expr_affinity_info(expr, referenced_tables, resolver),
        // Literals have NO affinity in SQLite.
        ast::Expr::Literal(_) => ExprAffinityInfo::no_affinity(),
        ast::Expr::Register(reg) => {
            // During UPDATE expression index evaluation, column references are
            // rewritten to Expr::Register. Look up the original column affinity
            // from the resolver's register_affinities map.
            if let Some(resolver) = resolver {
                if let Some(aff) = resolver.register_affinities.get(reg) {
                    return ExprAffinityInfo::with_affinity(*aff);
                }
            }
            ExprAffinityInfo::no_affinity()
        }
        ast::Expr::SubqueryResult {
            subquery_id,
            query_type: ast::SubqueryType::RowValue { num_regs, .. },
            ..
        } if *num_regs == 1 => {
            if let Some(resolver) = resolver {
                if let Some(aff) = resolver.subquery_affinities.borrow().get(subquery_id) {
                    return *aff;
                }
            }
            ExprAffinityInfo::no_affinity()
        }
        _ => ExprAffinityInfo::no_affinity(),
    }
}

pub fn get_expr_affinity(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    get_expr_affinity_info(expr, referenced_tables, resolver).affinity
}

/// Mirrors SQLite's `sqlite3ExprDataType()` (expr.c): a bitmask of the storage
/// classes an expression could yield. Used to combine column affinities across
/// the arms of a compound (UNION/INTERSECT/EXCEPT) subquery.
///
///   0x01 = numeric, 0x02 = text, 0x04 = blob; 0x00 = always NULL.
pub(crate) fn expr_data_type(expr: &ast::Expr, referenced_tables: Option<&TableReferences>) -> u8 {
    match expr {
        ast::Expr::Collate(inner, _) | ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => {
            expr_data_type(inner, referenced_tables)
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            expr_data_type(exprs.first().unwrap(), referenced_tables)
        }
        // A literal's storage class is its data type; reuse the literal->Value
        // inference rather than re-deriving the class from the AST here.
        ast::Expr::Literal(lit) => match literal_default_value(lit).map(|v| v.value_type()) {
            Ok(ValueType::Null) => 0x00,
            Ok(ValueType::Text) => 0x02,
            Ok(ValueType::Blob) => 0x04,
            // Integer/Float (and the fallback for keyword literals) are numeric.
            _ => 0x01,
        },
        ast::Expr::Binary(_, ast::Operator::Concat, _) => 0x06,
        ast::Expr::FunctionCall { .. }
        | ast::Expr::FunctionCallStar { .. }
        | ast::Expr::Variable(_) => 0x07,
        ast::Expr::Column { .. }
        | ast::Expr::RowId { .. }
        | ast::Expr::Cast { .. }
        | ast::Expr::Subquery(_) => {
            let aff = get_expr_affinity(expr, referenced_tables, None);
            if aff.is_numeric() {
                0x05
            } else if matches!(aff, Affinity::Text) {
                0x06
            } else {
                0x07
            }
        }
        ast::Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            let mut res = 0;
            for (_, then) in when_then_pairs {
                res |= expr_data_type(then, referenced_tables);
            }
            if let Some(else_expr) = else_expr {
                res |= expr_data_type(else_expr, referenced_tables);
            }
            res
        }
        _ => 0x01,
    }
}

pub fn comparison_affinity(
    lhs_expr: &ast::Expr,
    rhs_expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    compare_affinity(
        rhs_expr,
        get_expr_affinity_info(lhs_expr, referenced_tables, resolver),
        referenced_tables,
        resolver,
    )
}

pub(super) fn comparison_affinity_from_info(
    lhs: ExprAffinityInfo,
    rhs: ExprAffinityInfo,
) -> Affinity {
    if lhs.has_affinity && rhs.has_affinity {
        // Both sides have affinity - use numeric if either is numeric
        if lhs.affinity.is_numeric() || rhs.affinity.is_numeric() {
            Affinity::Numeric
        } else {
            Affinity::Blob
        }
    } else if lhs.has_affinity {
        lhs.affinity
    } else if rhs.has_affinity {
        rhs.affinity
    } else {
        Affinity::Blob
    }
}

pub(crate) fn compare_affinity(
    expr: &ast::Expr,
    other: ExprAffinityInfo,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    comparison_affinity_from_info(
        other,
        get_expr_affinity_info(expr, referenced_tables, resolver),
    )
}
