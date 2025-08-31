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
use turso_ext::{ConstraintInfo, ConstraintOp};
use turso_parser::ast::{self, SortOrder, TableInternalId};

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
    /// Get the constraining expression and operator, e.g. ('>=', '2+3') from 't.x >= 2+3'
    pub fn get_constraining_expr(&self, where_clause: &[WhereTerm]) -> (ast::Operator, ast::Expr) {
        let (idx, side) = self.where_clause_pos;
        let where_term = &where_clause[idx];
        let Ok(Some((lhs, _, rhs))) = as_binary_components(&where_term.expr) else {
            panic!("Expected a valid binary expression");
        };
        if side == BinaryExprSide::Lhs {
            (self.operator, lhs.clone())
        } else {
            (self.operator, rhs.clone())
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
    /// Refs are sorted by [ConstraintRef::index_col_pos]
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
///
/// This method do not perform much filtering of constraints and delegate this tasks to the consumers of the method
/// Consumers must inspect [TableConstraints] and its candidates and pick best constraints for optimized access
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
        }
        constraints.push(cs);
    }

    Ok(constraints)
}

#[derive(Clone, Debug)]
/// A reference to a [Constraint]s in a [TableConstraints] for single column.
///
/// This is specialized version of [ConstraintRef] which specifically holds range-like constraints:
/// - x = 10 (eq is set)
/// - x >= 10, x > 10 (lower_bound is set)
/// - x <= 10, x < 10 (upper_bound is set)
/// - x > 10 AND x < 20 (both lower_bound and upper_bound are set)
///
/// eq, lower_bound and upper_bound holds None or position of the constraint in the [Constraint] array
pub struct RangeConstraintRef {
    /// position of the column in the table definition
    pub table_col_pos: usize,
    /// position of the column in the index definition
    pub index_col_pos: usize,
    /// sort order for the column in the index definition
    pub sort_order: SortOrder,
    /// equality constraint
    pub eq: Option<usize>,
    /// lower bound constraint (either > or >=)
    pub lower_bound: Option<usize>,
    /// upper bound constraint (either < or <=)
    pub upper_bound: Option<usize>,
}

#[derive(Debug, Clone)]
/// Represent seek range which can be used in query planning to emit range scan over table or index
pub struct SeekRangeConstraint {
    pub sort_order: SortOrder,
    pub eq: Option<(ast::Operator, ast::Expr)>,
    pub lower_bound: Option<(ast::Operator, ast::Expr)>,
    pub upper_bound: Option<(ast::Operator, ast::Expr)>,
}

impl SeekRangeConstraint {
    pub fn new_eq(sort_order: SortOrder, eq: (ast::Operator, ast::Expr)) -> Self {
        Self {
            sort_order,
            eq: Some(eq),
            lower_bound: None,
            upper_bound: None,
        }
    }
    pub fn new_range(
        sort_order: SortOrder,
        lower_bound: Option<(ast::Operator, ast::Expr)>,
        upper_bound: Option<(ast::Operator, ast::Expr)>,
    ) -> Self {
        assert!(lower_bound.is_some() || upper_bound.is_some());
        Self {
            sort_order,
            eq: None,
            lower_bound,
            upper_bound,
        }
    }
}

impl RangeConstraintRef {
    /// Convert the [RangeConstraintRef] to a [SeekRangeConstraint] usable in a [crate::translate::plan::SeekDef::key].
    pub fn as_seek_range_constraint(
        &self,
        constraints: &[Constraint],
        where_clause: &[WhereTerm],
    ) -> SeekRangeConstraint {
        if let Some(eq) = self.eq {
            return SeekRangeConstraint::new_eq(
                self.sort_order,
                constraints[eq].get_constraining_expr(where_clause),
            );
        }
        SeekRangeConstraint::new_range(
            self.sort_order,
            self.lower_bound
                .map(|x| constraints[x].get_constraining_expr(where_clause)),
            self.upper_bound
                .map(|x| constraints[x].get_constraining_expr(where_clause)),
        )
    }
}

/// Find which [Constraint]s are usable for a given join order.
/// Returns a slice of the references to the constraints that are usable.
/// A constraint is considered usable for a given table if all of the other tables referenced by the constraint
/// are on the left side in the join order relative to the table.
pub fn usable_constraints_for_join_order<'a>(
    constraints: &'a [Constraint],
    refs: &'a [ConstraintRef],
    join_order: &[JoinOrderMember],
) -> Vec<RangeConstraintRef> {
    debug_assert!(refs.is_sorted_by_key(|x| x.index_col_pos));

    let table_idx = join_order.last().unwrap().original_idx;
    let lhs_mask = TableMask::from_table_number_iter(
        join_order
            .iter()
            .take(join_order.len() - 1)
            .map(|j| j.original_idx),
    );
    let mut usable: Vec<RangeConstraintRef> = Vec::new();
    let mut last_column_pos = 0;
    for cref in refs.iter() {
        let constraint = &constraints[cref.constraint_vec_pos];
        let other_side_refers_to_self = constraint.lhs_mask.contains_table(table_idx);
        if other_side_refers_to_self {
            break;
        }
        let all_required_tables_are_on_left_side = lhs_mask.contains_all(&constraint.lhs_mask);
        if !all_required_tables_are_on_left_side {
            break;
        }
        if Some(cref.index_col_pos) == usable.last().map(|x| x.index_col_pos) {
            assert_eq!(cref.sort_order, usable.last().unwrap().sort_order);
            assert_eq!(cref.index_col_pos, usable.last().unwrap().index_col_pos);
            assert_eq!(
                constraints[cref.constraint_vec_pos].table_col_pos,
                usable.last().unwrap().table_col_pos
            );
            // if we already have eq constraint - we must not add anything to it
            // otherwise, we can incorrectly consume filters which will not be used in the access path
            if usable.last().unwrap().eq.is_some() {
                continue;
            }
            match constraints[cref.constraint_vec_pos].operator {
                ast::Operator::Greater | ast::Operator::GreaterEquals => {
                    usable.last_mut().unwrap().lower_bound = Some(cref.constraint_vec_pos);
                }
                ast::Operator::Less | ast::Operator::LessEquals => {
                    usable.last_mut().unwrap().upper_bound = Some(cref.constraint_vec_pos);
                }
                _ => {}
            }
            continue;
        }
        if cref.index_col_pos != last_column_pos {
            break;
        }
        if usable.last().is_some_and(|x| x.eq.is_none()) {
            break;
        }
        let constraint_group = match constraints[cref.constraint_vec_pos].operator {
            ast::Operator::Equals => RangeConstraintRef {
                table_col_pos: constraints[cref.constraint_vec_pos].table_col_pos,
                index_col_pos: cref.index_col_pos,
                sort_order: cref.sort_order,
                eq: Some(cref.constraint_vec_pos),
                lower_bound: None,
                upper_bound: None,
            },
            ast::Operator::Greater | ast::Operator::GreaterEquals => RangeConstraintRef {
                table_col_pos: constraints[cref.constraint_vec_pos].table_col_pos,
                index_col_pos: cref.index_col_pos,
                sort_order: cref.sort_order,
                eq: None,
                lower_bound: Some(cref.constraint_vec_pos),
                upper_bound: None,
            },
            ast::Operator::Less | ast::Operator::LessEquals => RangeConstraintRef {
                table_col_pos: constraints[cref.constraint_vec_pos].table_col_pos,
                index_col_pos: cref.index_col_pos,
                sort_order: cref.sort_order,
                eq: None,
                lower_bound: None,
                upper_bound: Some(cref.constraint_vec_pos),
            },
            _ => continue,
        };
        usable.push(constraint_group);
        last_column_pos += 1;
    }
    usable
}

pub fn convert_to_vtab_constraint(
    constraints: &[Constraint],
    join_order: &[JoinOrderMember],
) -> Vec<ConstraintInfo> {
    let table_idx = join_order.last().unwrap().original_idx;
    let lhs_mask = TableMask::from_table_number_iter(
        join_order
            .iter()
            .take(join_order.len() - 1)
            .map(|j| j.original_idx),
    );
    constraints
        .iter()
        .enumerate()
        .filter_map(|(i, constraint)| {
            let other_side_refers_to_self = constraint.lhs_mask.contains_table(table_idx);
            if other_side_refers_to_self {
                return None;
            }
            let all_required_tables_are_on_left_side = lhs_mask.contains_all(&constraint.lhs_mask);
            to_ext_constraint_op(&constraint.operator).map(|op| ConstraintInfo {
                column_index: constraint.table_col_pos as u32,
                op,
                usable: all_required_tables_are_on_left_side,
                index: i,
            })
        })
        .collect()
}

fn to_ext_constraint_op(op: &ast::Operator) -> Option<ConstraintOp> {
    match op {
        ast::Operator::Equals => Some(ConstraintOp::Eq),
        ast::Operator::Less => Some(ConstraintOp::Lt),
        ast::Operator::LessEquals => Some(ConstraintOp::Le),
        ast::Operator::Greater => Some(ConstraintOp::Gt),
        ast::Operator::GreaterEquals => Some(ConstraintOp::Ge),
        ast::Operator::NotEquals => Some(ConstraintOp::Ne),
        _ => None,
    }
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
