use std::{cmp::Ordering, str::FromStr as _};

use turso_parser::ast::Expr;

use crate::{
    translate::{
        expr::{walk_expr, WalkControl},
        plan::TableReferences,
    },
    Result,
};

// TODO: in the future allow user to define collation sequences
// Will have to meddle with ffi for this
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, strum_macros::EnumString, Default,
)]
#[strum(ascii_case_insensitive)]
/// **Pre defined collation sequences**\
/// Collating functions only matter when comparing string values.
/// Numeric values are always compared numerically, and BLOBs are always compared byte-by-byte using memcmp().
#[repr(u8)]
pub enum CollationSeq {
    Unset = 0,
    #[default]
    Binary = 1,
    NoCase = 2,
    Rtrim = 3,
}

impl CollationSeq {
    pub fn new(collation: &str) -> crate::Result<Self> {
        CollationSeq::from_str(collation).map_err(|_| {
            crate::LimboError::ParseError(format!("no such collation sequence: {collation}"))
        })
    }
    #[inline]
    /// Returns the collation, defaulting to BINARY if unset
    pub const fn from_bits(bits: u8) -> Self {
        match bits {
            2 => CollationSeq::NoCase,
            3 => CollationSeq::Rtrim,
            _ => CollationSeq::Binary,
        }
    }

    #[inline(always)]
    pub fn compare_strings(&self, lhs: &str, rhs: &str) -> Ordering {
        match self {
            CollationSeq::Unset | CollationSeq::Binary => Self::binary_cmp(lhs, rhs),
            CollationSeq::NoCase => Self::nocase_cmp(lhs, rhs),
            CollationSeq::Rtrim => Self::rtrim_cmp(lhs, rhs),
        }
    }

    #[inline(always)]
    fn binary_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline(always)]
    fn nocase_cmp(lhs: &str, rhs: &str) -> Ordering {
        let nocase_lhs = uncased::UncasedStr::new(lhs);
        let nocase_rhs = uncased::UncasedStr::new(rhs);
        nocase_lhs.cmp(nocase_rhs)
    }

    #[inline(always)]
    fn rtrim_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.trim_end().cmp(rhs.trim_end())
    }
}

/// Every column of every table has an associated collating function. If no collating function is explicitly defined,
/// then the collating function defaults to BINARY.
/// The COLLATE clause of the column definition is used to define alternative collating functions for a column.
///
/// The rules for determining which collating function to use for a binary comparison operator (=, <, >, <=, >=, !=, IS, and IS NOT) are as follows:
///
/// If either operand has an explicit collating function assignment using the postfix COLLATE operator,
/// then the explicit collating function is used for comparison, with precedence to the collating function of the left operand.
///
/// If either operand is a column, then the collating function of that column is used with precedence to the left operand.
/// For the purposes of the previous sentence, a column name preceded by one or more unary "+" operators and/or CAST operators is still considered a column name.
///
/// Otherwise, the BINARY collating function is used for comparison.
///
/// An operand of a comparison is considered to have an explicit collating function assignment
/// if any subexpression of the operand uses the postfix COLLATE operator.
/// Thus, if a COLLATE operator is used anywhere in a comparison expression,
/// the collating function defined by that operator is used for string comparison
/// regardless of what table columns might be a part of that expression.
/// If two or more COLLATE operator subexpressions appear anywhere in a comparison,
/// the left most explicit collating function is used regardless of how deeply
/// the COLLATE operators are nested in the expression and regardless of how
/// the expression is parenthesized.
pub fn get_collseq_from_expr(
    top_expr: &Expr,
    referenced_tables: &TableReferences,
) -> Result<Option<CollationSeq>> {
    let mut maybe_column_collseq = None;
    let mut maybe_explicit_collseq = None;

    walk_expr(top_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Collate(_, seq) => {
                // Only store the first (leftmost) COLLATE operator we find
                if maybe_explicit_collseq.is_none() {
                    maybe_explicit_collseq =
                        Some(CollationSeq::new(seq.as_str()).unwrap_or_default());
                }
                // Skip children since we've found a COLLATE operator
                return Ok(WalkControl::SkipChildren);
            }
            Expr::Column { table, column, .. } => {
                let (_, table_ref) = referenced_tables
                    .find_table_by_internal_id(*table)
                    .ok_or_else(|| crate::LimboError::ParseError("table not found".to_string()))?;
                let column = table_ref
                    .get_column_at(*column)
                    .ok_or_else(|| crate::LimboError::ParseError("column not found".to_string()))?;
                if maybe_column_collseq.is_none() {
                    maybe_column_collseq = column.collation_opt();
                }
                return Ok(WalkControl::Continue);
            }
            Expr::RowId { table, .. } => {
                let (_, table_ref) = referenced_tables
                    .find_table_by_internal_id(*table)
                    .ok_or_else(|| crate::LimboError::ParseError("table not found".to_string()))?;
                if let Some(btree) = table_ref.btree() {
                    if let Some((_, rowid_alias_col)) = btree.get_rowid_alias_column() {
                        if maybe_column_collseq.is_none() {
                            maybe_column_collseq = rowid_alias_col.collation_opt();
                        }
                    }
                }
                return Ok(WalkControl::Continue);
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(maybe_explicit_collseq.or(maybe_column_collseq))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use turso_parser::ast::{Literal, Name, Operator, TableInternalId, UnaryOperator};

    use crate::{
        schema::{BTreeTable, Column, Table, Type},
        translate::plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan},
    };

    use super::*;

    #[test]
    fn test_get_collseq_from_expr_single_table_single_column() {
        // plain column
        for collation in [
            None,
            Some(CollationSeq::Binary),
            Some(CollationSeq::NoCase),
            Some(CollationSeq::Rtrim),
        ] {
            let table_references =
                get_table_references_single_table_single_column_with_collation(collation);
            let expr = Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            };
            let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
            assert_eq!(collseq, collation);
        }
    }

    #[test]
    fn test_get_collseq_from_expr_single_table_single_column_with_collate() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::Binary),
        );
        // col COLLATE RTRIM, col COLLATE NOCASE, col COLLATE BINARY
        for collation in ["RTRIM", "NOCASE", "BINARY"] {
            let expected_collation = CollationSeq::new(collation).unwrap();
            let expr = Expr::Collate(
                Box::new(Expr::Column {
                    database: None,
                    table: TableInternalId::from(1),
                    column: 0,
                    is_rowid_alias: false,
                }),
                Name::exact(collation.to_string()),
            );
            let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
            assert_eq!(collseq, Some(expected_collation));
        }
    }

    #[test]
    fn test_get_collseq_from_expr_multiple_collate_leftmost_wins() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::Binary),
        );
        // (col COLLATE NOCASE) COLLATE RTRIM -- RTRIM wins as it is the leftmost AST node with a COLLATE
        let inner = Expr::Collate(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            }),
            Name::exact("NOCASE".to_string()),
        );
        let expr = Expr::Collate(
            Box::new(Expr::Parenthesized(vec![Box::new(inner)])),
            Name::exact("RTRIM".to_string()),
        );
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::Rtrim));
    }

    #[test]
    fn test_get_collseq_from_expr_unary_plus_and_cast_still_column() {
        let table_references = get_table_references_single_table_single_column_with_collation(
            Some(CollationSeq::NoCase),
        );
        // Unary plus on column
        let expr_plus = Expr::unary(
            UnaryOperator::Positive,
            Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            },
        );
        let collseq_plus = get_collseq_from_expr(&expr_plus, &table_references).unwrap();
        assert_eq!(collseq_plus, Some(CollationSeq::NoCase));

        // CAST(column AS TEXT)
        let cast_ty = Some(turso_parser::ast::Type {
            name: "TEXT".to_string(),
            size: None,
        });
        let expr_cast = Expr::cast(
            Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            },
            cast_ty,
        );
        let collseq_cast = get_collseq_from_expr(&expr_cast, &table_references).unwrap();
        assert_eq!(collseq_cast, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_explicit_collate_anywhere_in_operand() {
        let table_references = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            None,
        );
        // RTRIM wins because it's an explicit COLLATE even though it appears on the right side of the expression
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Parenthesized(vec![Box::new(Expr::Collate(
            Box::new(Expr::Literal(Literal::String("x".to_string()))),
            Name::exact("RTRIM".to_string()),
        ))]);
        let expr = Expr::binary(lhs, Operator::Add, rhs);
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::Rtrim));
    }

    #[test]
    fn test_get_collseq_from_expr_column_plus_column_leftside_column_wins() {
        let table_references = get_table_references_two_tables_single_column_with_collations(
            Some(CollationSeq::NoCase),
            Some(CollationSeq::Rtrim),
        );
        // col1 + col2 -- col1's NOCASE collation wins since it's on the left side
        let lhs = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let rhs = Expr::Column {
            database: None,
            table: TableInternalId::from(2),
            column: 0,
            is_rowid_alias: false,
        };
        let expr = Expr::binary(lhs, Operator::Add, rhs);
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_collate_vs_collate_leftside_expr_wins() {
        let table_references = TableReferences::new_empty();
        // (x COLLATE NOCASE) + (y COLLATE RTRIM) -- NOCASE wins since it's on the left side
        let lhs = Expr::Collate(
            Box::new(Expr::Literal(Literal::String("x".to_string()))),
            Name::exact("NOCASE".to_string()),
        );
        let rhs = Expr::Collate(
            Box::new(Expr::Literal(Literal::String("y".to_string()))),
            Name::exact("RTRIM".to_string()),
        );
        let expr = Expr::binary(lhs, Operator::Add, rhs);
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    #[test]
    fn test_get_collseq_from_expr_default_binary_when_no_collate_or_column() {
        let table_references = TableReferences::new_empty();
        let expr = Expr::Literal(Literal::String("abc".to_string()));
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, None);
    }

    #[test]
    fn test_get_collseq_from_expr_rowid_uses_rowid_alias_collation() {
        let table_references = get_table_references_single_table_rowid_alias_with_collation(Some(
            CollationSeq::NoCase,
        ));
        let expr = Expr::RowId {
            database: None,
            table: TableInternalId::from(1),
        };
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(CollationSeq::NoCase));
    }

    // Helpers //

    fn get_table_references_single_table_single_column_with_collation(
        collation: Option<CollationSeq>,
    ) -> TableReferences {
        let mut table_references = TableReferences::new_empty();
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
            identifier: "foo".to_string(),
            internal_id: TableInternalId::from(1),
            join_info: None,
            table: Table::BTree(Arc::new(BTreeTable {
                root_page: 0,
                has_autoincrement: false,
                has_rowid: false,
                is_strict: false,
                name: "foo".to_string(),
                primary_key_columns: vec![],
                columns: vec![Column::new(
                    Some("foo".to_string()),
                    "text".to_string(),
                    None,
                    Type::Text,
                    collation,
                    false,
                    false,
                    false,
                    false,
                    false,
                )],
                unique_sets: vec![],
                foreign_keys: vec![],
            })),
        });

        table_references
    }

    fn get_table_references_two_tables_single_column_with_collations(
        left: Option<CollationSeq>,
        right: Option<CollationSeq>,
    ) -> TableReferences {
        let mut table_references = TableReferences::new_empty();
        // Left table t1(id=1)
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
            identifier: "t1".to_string(),
            internal_id: TableInternalId::from(1),
            join_info: None,
            table: Table::BTree(Arc::new(BTreeTable {
                root_page: 0,
                has_autoincrement: false,
                has_rowid: true,
                is_strict: false,
                name: "t1".to_string(),
                primary_key_columns: vec![],
                columns: vec![Column::new(
                    Some("a".to_string()),
                    "text".to_string(),
                    None,
                    Type::Text,
                    left,
                    false,
                    false,
                    false,
                    false,
                    false,
                )],
                unique_sets: vec![],
                foreign_keys: vec![],
            })),
        });
        // Right table t2(id=2)
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
            identifier: "t2".to_string(),
            internal_id: TableInternalId::from(2),
            join_info: None,
            table: Table::BTree(Arc::new(BTreeTable {
                root_page: 0,
                has_autoincrement: false,
                has_rowid: true,
                is_strict: false,
                name: "t2".to_string(),
                primary_key_columns: vec![],
                columns: vec![Column::new(
                    Some("b".to_string()),
                    "text".to_string(),
                    None,
                    Type::Text,
                    right,
                    false,
                    false,
                    false,
                    false,
                    false,
                )],
                unique_sets: vec![],
                foreign_keys: vec![],
            })),
        });
        table_references
    }

    fn get_table_references_single_table_rowid_alias_with_collation(
        collation: Option<CollationSeq>,
    ) -> TableReferences {
        use turso_parser::ast::SortOrder;
        let mut table_references = TableReferences::new_empty();
        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
            identifier: "bar".to_string(),
            internal_id: TableInternalId::from(1),
            join_info: None,
            table: Table::BTree(Arc::new(BTreeTable {
                root_page: 0,
                has_autoincrement: false,
                has_rowid: true,
                is_strict: false,
                name: "bar".to_string(),
                primary_key_columns: vec![("id".to_string(), SortOrder::Asc)],
                columns: vec![Column::new(
                    Some("id".to_string()),
                    "INTEGER".to_string(),
                    None,
                    Type::Integer,
                    collation,
                    true,
                    true,
                    false,
                    true,
                    false,
                )],
                unique_sets: vec![],
                foreign_keys: vec![],
            })),
        });
        table_references
    }
}
