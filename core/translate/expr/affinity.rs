use crate::{
    translate::{alter::literal_default_value, expr_context::ExprProperties},
    types::ValueType,
    vdbe::affinity::Affinity,
};
use bitflags::bitflags;
use turso_parser::ast;

bitflags! {
    /// Storage class flags that represent the possible storage classes an expression can yield.
    /// Used to combine column affinities across UNION/INTERSECT/EXCEPT arms.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct StorageClassMask: u8 {
        const NUMERIC = 0x01;
        const TEXT = 0x02;
        const BLOB = 0x04;
    }
}

impl StorageClassMask {
    pub const fn from_numeric() -> Self {
        Self::NUMERIC
    }

    pub const fn from_text() -> Self {
        Self::TEXT
    }

    pub const fn from_blob() -> Self {
        Self::BLOB
    }

    pub const fn from_null() -> Self {
        Self::empty()
    }

    pub fn has_numeric(&self) -> bool {
        self.contains(Self::NUMERIC)
    }

    pub fn has_text(&self) -> bool {
        self.contains(Self::TEXT)
    }

    #[allow(dead_code)]
    pub fn has_blob(&self) -> bool {
        self.contains(Self::BLOB)
    }
}

pub(crate) fn get_expr_affinity(expr: &ast::Expr, metadata: &impl ExprProperties) -> Affinity {
    match expr {
        ast::Expr::Column { table, column, .. } => metadata
            .column_affinity(*table, *column)
            .unwrap_or(Affinity::None),
        ast::Expr::RowId { .. } => Affinity::Integer,
        ast::Expr::Cast { type_name, .. } => {
            if let Some(type_name) = type_name {
                Affinity::of_written_type(&crate::util::dequote(&type_name.name))
            } else {
                Affinity::None
            }
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            get_expr_affinity(exprs.first().unwrap(), metadata)
        }
        ast::Expr::Collate(expr, _) => get_expr_affinity(expr, metadata),
        ast::Expr::MergedColumn(columns) => {
            // A merged USING column takes the affinity of its first column, as
            // that column would. With t1.a INTEGER, `a = 7` converts a text '7'
            // from t2.a to a number before comparing.
            let first_column = columns
                .first()
                .expect("a merged column must have at least two source columns");
            get_expr_affinity(first_column, metadata)
        }
        // Literals have NO affinity in SQLite.
        ast::Expr::Literal(_) => Affinity::None,
        ast::Expr::Register { register: reg, .. } => {
            metadata.register_affinity(*reg).unwrap_or(Affinity::None)
        }
        ast::Expr::SubqueryResult {
            subquery_id,
            query_type:
                ast::SubqueryType::RowValue {
                    result_reg_start,
                    num_regs: 1,
                },
            ..
        } => metadata
            .subquery_affinity(*subquery_id, *result_reg_start)
            .unwrap_or(Affinity::None),
        _ => Affinity::None,
    }
}

/// Mirrors SQLite's `sqlite3ExprDataType()` (expr.c): a bitmask of the storage
/// classes an expression could yield. Used to combine column affinities across
/// the arms of a compound (UNION/INTERSECT/EXCEPT) subquery.
pub(crate) fn expr_data_type(expr: &ast::Expr, metadata: &impl ExprProperties) -> StorageClassMask {
    match expr {
        ast::Expr::Collate(inner, _) | ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => {
            expr_data_type(inner, metadata)
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            expr_data_type(exprs.first().unwrap(), metadata)
        }
        // A literal's storage class is its data type; reuse the literal->Value
        // inference rather than re-deriving the class from the AST here.
        ast::Expr::Literal(lit) => match literal_default_value(lit).map(|v| v.value_type()) {
            Ok(ValueType::Null) => StorageClassMask::from_null(),
            Ok(ValueType::Text) => StorageClassMask::from_text(),
            Ok(ValueType::Blob) => StorageClassMask::from_blob(),
            // Integer/Float (and the fallback for keyword literals) are numeric.
            _ => StorageClassMask::from_numeric(),
        },
        ast::Expr::Binary(_, ast::Operator::Concat, _) => {
            StorageClassMask::TEXT | StorageClassMask::BLOB
        }
        ast::Expr::FunctionCall { .. }
        | ast::Expr::FunctionCallStar { .. }
        | ast::Expr::MergedColumn(_)
        | ast::Expr::Variable(_) => StorageClassMask::all(),
        ast::Expr::Column { .. }
        | ast::Expr::RowId { .. }
        | ast::Expr::Cast { .. }
        | ast::Expr::Subquery(_)
        | ast::Expr::SubqueryResult {
            query_type: ast::SubqueryType::RowValue { .. },
            ..
        } => {
            let aff = get_expr_affinity(expr, metadata);
            if aff.is_numeric() {
                StorageClassMask::NUMERIC | StorageClassMask::BLOB
            } else if matches!(aff, Affinity::Text) {
                StorageClassMask::TEXT | StorageClassMask::BLOB
            } else {
                StorageClassMask::all()
            }
        }
        ast::Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            let mut res = StorageClassMask::from_null();
            for (_, then) in when_then_pairs {
                res |= expr_data_type(then, metadata);
            }
            if let Some(else_expr) = else_expr {
                res |= expr_data_type(else_expr, metadata);
            }
            res
        }
        _ => StorageClassMask::from_numeric(),
    }
}

pub(crate) fn do_comparison_affinity(lhs: Affinity, rhs: Affinity) -> Affinity {
    if lhs != Affinity::None && rhs != Affinity::None {
        // Both sides have affinity - use numeric if either is numeric
        if lhs.is_numeric() || rhs.is_numeric() {
            Affinity::Numeric
        } else {
            Affinity::Blob
        }
    } else if lhs != Affinity::None {
        // A lone numeric affinity still collapses to NUMERIC.
        if lhs.is_numeric() {
            Affinity::Numeric
        } else {
            lhs
        }
    } else if rhs != Affinity::None {
        if rhs.is_numeric() {
            Affinity::Numeric
        } else {
            rhs
        }
    } else {
        Affinity::Blob
    }
}

/// Returns the [Affinity] to be used to compare two terms.
pub fn comparison_affinity(
    lhs: impl HasComparableAffinity,
    rhs: impl HasComparableAffinity,
    metadata: &impl ExprProperties,
) -> Affinity {
    let lhs = lhs.comparison_affinity(metadata);
    let rhs = rhs.comparison_affinity(metadata);
    do_comparison_affinity(lhs, rhs)
}

pub(crate) trait HasComparableAffinity {
    fn comparison_affinity(&self, metadata: &impl ExprProperties) -> Affinity;
}

impl HasComparableAffinity for &ast::Expr {
    fn comparison_affinity(&self, metadata: &impl ExprProperties) -> Affinity {
        get_expr_affinity(self, metadata)
    }
}

impl HasComparableAffinity for Affinity {
    fn comparison_affinity(&self, _metadata: &impl ExprProperties) -> Affinity {
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_sides_have_affinity_numeric_wins() {
        assert_eq!(
            do_comparison_affinity(Affinity::Real, Affinity::Text,),
            Affinity::Numeric
        );
    }

    #[test]
    fn both_sides_have_affinity_neither_numeric_is_blob() {
        assert_eq!(
            do_comparison_affinity(Affinity::Text, Affinity::Blob,),
            Affinity::Blob
        );
    }

    #[test]
    fn only_lhs_has_real_affinity_collapses_to_numeric() {
        assert_eq!(
            do_comparison_affinity(Affinity::Real, Affinity::None,),
            Affinity::Numeric
        );
    }

    #[test]
    fn only_lhs_has_integer_affinity_collapses_to_numeric() {
        assert_eq!(
            do_comparison_affinity(Affinity::Integer, Affinity::None,),
            Affinity::Numeric
        );
    }

    #[test]
    fn only_rhs_has_real_affinity_collapses_to_numeric() {
        assert_eq!(
            do_comparison_affinity(Affinity::None, Affinity::Real,),
            Affinity::Numeric
        );
    }

    #[test]
    fn only_lhs_has_text_affinity_is_preserved() {
        assert_eq!(
            do_comparison_affinity(Affinity::Text, Affinity::None,),
            Affinity::Text
        );
    }

    #[test]
    fn only_rhs_has_text_affinity_is_preserved() {
        assert_eq!(
            do_comparison_affinity(Affinity::None, Affinity::Text,),
            Affinity::Text
        );
    }

    #[test]
    fn neither_side_has_affinity_is_blob() {
        assert_eq!(
            do_comparison_affinity(Affinity::None, Affinity::None,),
            Affinity::Blob
        );
    }
}
