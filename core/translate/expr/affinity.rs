use super::*;

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
