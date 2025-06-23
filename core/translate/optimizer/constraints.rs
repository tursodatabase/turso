use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use crate::{
    schema::{Column, Index},
    translate::{
        expr::as_binary_components,
        plan::{JoinOrderMember, TableReferences, WhereTerm},
        planner::{table_mask_from_expr, TableMask},
    },
    Result,
};
use turso_sqlite3_parser::ast::{self, SortOrder, TableInternalId};

use super::cost::ESTIMATED_HARDCODED_ROWS_PER_TABLE;

/// Represents a single condition derived from a `WHERE` clause term
/// that constrains a specific column of a table.
///
/// Constraints are precomputed for each table involved in a query. They are used
/// during query optimization to estimate the cost of different access paths (e.g., using an index)
/// and to determine the optimal join order. A constraint can only be applied if all tables
/// referenced in its expression (other than the constrained table itself) are already
/// available in the current join context, i.e. on the left side in the join order
/// relative to the table.
#[derive(Debug, Clone)]
///
pub struct Constraint {
    /// The position of the original `WHERE` clause term this constraint derives from,
    /// and which side of the [ast::Expr::Binary] comparison contains the expression
    /// that constrains the column.
    /// E.g. in SELECT * FROM t WHERE t.x = 10, the constraint is (0, BinaryExprSide::Rhs)
    /// because the RHS '10' is the constraining expression.
    ///
    /// This is tracked so we can:
    ///
    /// 1. Extract the constraining expression for use in an index seek key, and
    /// 2. Remove the relevant binary expression from the WHERE clause, if used as an index seek key.
    pub where_clause_pos: (usize, BinaryExprSide),
    /// The comparison operator (e.g., `=`, `>`, `<`) used in the constraint.
    pub operator: ast::Operator,
    /// The zero-based index of the constrained column within the table's schema.
    pub table_col_pos: usize,
    /// A bitmask representing the set of tables that appear on the *constraining* side
    /// of the comparison expression. For example, in SELECT * FROM t1,t2,t3 WHERE t1.x = t2.x + t3.x,
    /// the lhs_mask contains t2 and t3. Thus, this constraint can only be used if t2 and t3
    /// have already been joined (i.e. are on the left side of the join order relative to t1).
    pub lhs_mask: TableMask,
    /// An estimated selectivity factor (0.0 to 1.0) indicating the fraction of rows
    /// expected to satisfy this constraint. Used for cost and cardinality estimation.
    pub selectivity: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryExprSide {
    Lhs,
    Rhs,
}

impl Constraint {
    /// Get the constraining expression, e.g. '2+3' from 't.x = 2+3'
    pub fn get_constraining_expr(&self, where_clause: &[WhereTerm]) -> ast::Expr {
        let (idx, side) = self.where_clause_pos;
        let where_term = &where_clause[idx];
        let Ok(Some((lhs, _, rhs))) = as_binary_components(&where_term.expr) else {
            panic!("Expected a valid binary expression");
        };
        if side == BinaryExprSide::Lhs {
            lhs.clone()
        } else {
            rhs.clone()
        }
    }
}

#[derive(Debug, Clone)]
/// A reference to a [Constraint] in a [TableConstraints].
///
/// This is used to track which constraints may be used as an index seek key.
pub struct ConstraintRef {
    /// The position of the constraint in the [TableConstraints::constraints] vector.
    pub constraint_vec_pos: usize,
    /// The position of the constrained column in the index. Always 0 for rowid indices.
    pub index_col_pos: usize,
    /// The sort order of the constrained column in the index. Always ascending for rowid indices.
    pub sort_order: SortOrder,
}

impl ConstraintRef {
    /// Convert the constraint to a column usable in a [crate::translate::plan::SeekDef::key].
    pub fn as_seek_key_column(
        &self,
        constraints: &[Constraint],
        where_clause: &[WhereTerm],
    ) -> (ast::Expr, SortOrder) {
        let constraint = &constraints[self.constraint_vec_pos];
        let constraining_expr = constraint.get_constraining_expr(where_clause);
        (constraining_expr, self.sort_order)
    }
}

/// A collection of [ConstraintRef]s for a given index, or if index is None, for the table's rowid index.
/// For example, given a table `T (x,y,z)` with an index `T_I (y desc,z)`, take the following query:
/// ```sql
/// SELECT * FROM T WHERE y = 10 AND z = 20;
/// ```
///
/// This will produce the following [ConstraintUseCandidate]:
///
/// ConstraintUseCandidate {
///     index: Some(T_I)
///     refs: [
///         ConstraintRef {
///             constraint_vec_pos: 0, // y = 10
///             index_col_pos: 0, // y
///             sort_order: SortOrder::Desc,
///         },
///         ConstraintRef {
///             constraint_vec_pos: 1, // z = 20
///             index_col_pos: 1, // z
///             sort_order: SortOrder::Asc,
///         },
///     ],
/// }
///
#[derive(Debug)]
pub struct ConstraintUseCandidate {
    /// The index that may be used to satisfy the constraints. If none, the table's rowid index is used.
    pub index: Option<Arc<Index>>,
    /// References to the constraints that may be used as an access path for the index.
    pub refs: Vec<ConstraintRef>,
}

#[derive(Debug)]
/// A collection of [Constraint]s and their potential [ConstraintUseCandidate]s for a given table.
pub struct TableConstraints {
    /// The internal ID of the [TableReference] that these constraints are for.
    pub table_id: TableInternalId,
    /// The constraints for the table, i.e. any [WhereTerm]s that reference columns from this table.
    pub constraints: Vec<Constraint>,
    /// Candidates for indexes that may use the constraints to perform a lookup.
    pub candidates: Vec<ConstraintUseCandidate>,
}

/// In lieu of statistics, we estimate that an equality filter will reduce the output set to 1% of its size.
const SELECTIVITY_EQ: f64 = 0.01;
/// In lieu of statistics, we estimate that a range filter will reduce the output set to 40% of its size.
const SELECTIVITY_RANGE: f64 = 0.4;
/// In lieu of statistics, we estimate that other filters will reduce the output set to 90% of its size.
const SELECTIVITY_OTHER: f64 = 0.9;

const SELECTIVITY_UNIQUE_EQUALITY: f64 = 1.0 / ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64;

/// Estimate the selectivity of a constraint based on the operator and the column type.
fn estimate_selectivity(column: &Column, op: ast::Operator) -> f64 {
    match op {
        ast::Operator::Equals => {
            if column.is_rowid_alias || column.primary_key {
                SELECTIVITY_UNIQUE_EQUALITY
            } else {
                SELECTIVITY_EQ
            }
        }
        ast::Operator::Greater => SELECTIVITY_RANGE,
        ast::Operator::GreaterEquals => SELECTIVITY_RANGE,
        ast::Operator::Less => SELECTIVITY_RANGE,
        ast::Operator::LessEquals => SELECTIVITY_RANGE,
        _ => SELECTIVITY_OTHER,
    }
}

/// Precompute all potentially usable [Constraints] from a WHERE clause.
/// The resulting list of [TableConstraints] is then used to evaluate the best access methods for various join orders.
pub fn constraints_from_where_clause(
    where_clause: &[WhereTerm],
    table_references: &TableReferences,
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
) -> Result<Vec<TableConstraints>> {
    let mut constraints = Vec::new();

    // For each table, collect all the Constraints and all potential index candidates that may use them.
    for table_reference in table_references.joined_tables() {
        let rowid_alias_column = table_reference
            .columns()
            .iter()
            .position(|c| c.is_rowid_alias);

        let mut cs = TableConstraints {
            table_id: table_reference.internal_id,
            constraints: Vec::new(),
            candidates: available_indexes
                .get(table_reference.table.get_name())
                .map_or(Vec::new(), |indexes| {
                    indexes
                        .iter()
                        .map(|index| ConstraintUseCandidate {
                            index: Some(index.clone()),
                            refs: Vec::new(),
                        })
                        .collect()
                }),
        };
        // Add a candidate for the rowid index, which is always available when the table has a rowid alias.
        cs.candidates.push(ConstraintUseCandidate {
            index: None,
            refs: Vec::new(),
        });

        for (i, term) in where_clause.iter().enumerate() {
            let Some((lhs, operator, rhs)) = as_binary_components(&term.expr)? else {
                continue;
            };

            // Constraints originating from a LEFT JOIN must always be evaluated in that join's RHS table's loop,
            // regardless of which tables the constraint references.
            if let Some(outer_join_tbl) = term.from_outer_join {
                if outer_join_tbl != table_reference.internal_id {
                    continue;
                }
            }

            // If either the LHS or RHS of the constraint is a column from the table, add the constraint.
            match lhs {
                ast::Expr::Column { table, column, .. } => {
                    if *table == table_reference.internal_id {
                        let table_column = &table_reference.table.columns()[*column];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            table_col_pos: *column,
                            lhs_mask: table_mask_from_expr(rhs, table_references)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                ast::Expr::RowId { table, .. } => {
                    // A rowid alias column must exist for the 'rowid' keyword to be considered a valid reference.
                    // This should be a parse error at an earlier stage of the query compilation, but nevertheless,
                    // we check it here.
                    if *table == table_reference.internal_id && rowid_alias_column.is_some() {
                        let table_column =
                            &table_reference.table.columns()[rowid_alias_column.unwrap()];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            table_col_pos: rowid_alias_column.unwrap(),
                            lhs_mask: table_mask_from_expr(rhs, table_references)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                _ => {}
            };
            match rhs {
                ast::Expr::Column { table, column, .. } => {
                    if *table == table_reference.internal_id {
                        let table_column = &table_reference.table.columns()[*column];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            table_col_pos: *column,
                            lhs_mask: table_mask_from_expr(lhs, table_references)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                ast::Expr::RowId { table, .. } => {
                    if *table == table_reference.internal_id && rowid_alias_column.is_some() {
                        let table_column =
                            &table_reference.table.columns()[rowid_alias_column.unwrap()];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            table_col_pos: rowid_alias_column.unwrap(),
                            lhs_mask: table_mask_from_expr(lhs, table_references)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                _ => {}
            };
        }
        // sort equalities first so that index keys will be properly constructed.
        // see e.g.: https://www.solarwinds.com/blog/the-left-prefix-index-rule
        cs.constraints.sort_by(|a, b| {
            if a.operator == ast::Operator::Equals {
                Ordering::Less
            } else if b.operator == ast::Operator::Equals {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });

        // For each constraint we found, add a reference to it for each index that may be able to use it.
        for (i, constraint) in cs.constraints.iter().enumerate() {
            if rowid_alias_column == Some(constraint.table_col_pos) {
                let rowid_candidate = cs
                    .candidates
                    .iter_mut()
                    .find_map(|candidate| {
                        if candidate.index.is_none() {
                            Some(candidate)
                        } else {
                            None
                        }
                    })
                    .unwrap();
                rowid_candidate.refs.push(ConstraintRef {
                    constraint_vec_pos: i,
                    index_col_pos: 0,
                    sort_order: SortOrder::Asc,
                });
            }
            for index in available_indexes
                .get(table_reference.table.get_name())
                .unwrap_or(&Vec::new())
            {
                if let Some(position_in_index) =
                    index.column_table_pos_to_index_pos(constraint.table_col_pos)
                {
                    let index_candidate = cs
                        .candidates
                        .iter_mut()
                        .find_map(|candidate| {
                            if candidate
                                .index
                                .as_ref()
                                .is_some_and(|i| Arc::ptr_eq(index, i))
                            {
                                Some(candidate)
                            } else {
                                None
                            }
                        })
                        .unwrap();
                    index_candidate.refs.push(ConstraintRef {
                        constraint_vec_pos: i,
                        index_col_pos: position_in_index,
                        sort_order: index.columns[position_in_index].order,
                    });
                }
            }
        }

        for candidate in cs.candidates.iter_mut() {
            // Sort by index_col_pos, ascending -- index columns must be consumed in contiguous order.
            candidate.refs.sort_by_key(|cref| cref.index_col_pos);
            // Deduplicate by position, keeping first occurrence (which will be equality if one exists, since the constraints vec is sorted that way)
            candidate.refs.dedup_by_key(|cref| cref.index_col_pos);
            // Truncate at first gap in positions -- again, index columns must be consumed in contiguous order.
            let contiguous_len = candidate
                .refs
                .iter()
                .enumerate()
                .take_while(|(i, cref)| cref.index_col_pos == *i)
                .count();
            candidate.refs.truncate(contiguous_len);

            // Truncate after the first inequality, since the left-prefix rule of indexes requires that all constraints but the last one must be equalities;
            // again see: https://www.solarwinds.com/blog/the-left-prefix-index-rule
            if let Some(first_inequality) = candidate.refs.iter().position(|cref| {
                cs.constraints[cref.constraint_vec_pos].operator != ast::Operator::Equals
            }) {
                candidate.refs.truncate(first_inequality + 1);
            }
        }
        constraints.push(cs);
    }

    Ok(constraints)
}

/// Find which [Constraint]s are usable for a given join order.
/// Returns a slice of the references to the constraints that are usable.
/// A constraint is considered usable for a given table if all of the other tables referenced by the constraint
/// are on the left side in the join order relative to the table.
pub fn usable_constraints_for_join_order<'a>(
    constraints: &'a [Constraint],
    refs: &'a [ConstraintRef],
    join_order: &[JoinOrderMember],
) -> &'a [ConstraintRef] {
    let table_idx = join_order.last().unwrap().original_idx;
    let mut usable_until = 0;
    for cref in refs.iter() {
        let constraint = &constraints[cref.constraint_vec_pos];
        let other_side_refers_to_self = constraint.lhs_mask.contains_table(table_idx);
        if other_side_refers_to_self {
            break;
        }
        let lhs_mask = TableMask::from_table_number_iter(
            join_order
                .iter()
                .take(join_order.len() - 1)
                .map(|j| j.original_idx),
        );
        let all_required_tables_are_on_left_side = lhs_mask.contains_all(&constraint.lhs_mask);
        if !all_required_tables_are_on_left_side {
            break;
        }
        usable_until += 1;
    }
    &refs[..usable_until]
}

fn opposite_cmp_op(op: ast::Operator) -> ast::Operator {
    match op {
        ast::Operator::Equals => ast::Operator::Equals,
        ast::Operator::Greater => ast::Operator::Less,
        ast::Operator::GreaterEquals => ast::Operator::LessEquals,
        ast::Operator::Less => ast::Operator::Greater,
        ast::Operator::LessEquals => ast::Operator::GreaterEquals,
        _ => panic!("unexpected operator: {op:?}"),
    }
}
