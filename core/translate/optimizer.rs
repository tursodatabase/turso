use std::{collections::HashMap, sync::Arc};

use limbo_sqlite3_parser::ast::{self, Expr, SortOrder};

use crate::{
    schema::{Index, Schema},
    translate::plan::TerminationKey,
    types::SeekOp,
    util::exprs_are_equivalent,
    Result,
};

use super::plan::{
    DeletePlan, Direction, GroupBy, IterationDirection, Operation, Plan, Search, SeekDef, SeekKey,
    SelectPlan, TableReference, UpdatePlan, WhereTerm,
};

pub fn optimize_plan(plan: &mut Plan, schema: &Schema) -> Result<()> {
    match plan {
        Plan::Select(plan) => optimize_select_plan(plan, schema),
        Plan::Delete(plan) => optimize_delete_plan(plan, schema),
        Plan::Update(plan) => optimize_update_plan(plan, schema),
    }
}

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
fn optimize_select_plan(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    optimize_subqueries(plan, schema)?;
    rewrite_exprs_select(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &plan.group_by,
    )?;

    eliminate_orderby_like_groupby(plan)?;

    Ok(())
}

fn optimize_delete_plan(plan: &mut DeletePlan, schema: &Schema) -> Result<()> {
    rewrite_exprs_delete(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &None,
    )?;

    Ok(())
}

fn optimize_update_plan(plan: &mut UpdatePlan, schema: &Schema) -> Result<()> {
    rewrite_exprs_update(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }
    use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &None,
    )?;
    Ok(())
}

fn optimize_subqueries(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    for table in plan.table_references.iter_mut() {
        if let Operation::Subquery { plan, .. } = &mut table.op {
            optimize_select_plan(&mut *plan, schema)?;
        }
    }

    Ok(())
}

fn eliminate_orderby_like_groupby(plan: &mut SelectPlan) -> Result<()> {
    if plan.order_by.is_none() | plan.group_by.is_none() {
        return Ok(());
    }
    if plan.table_references.len() == 0 {
        return Ok(());
    }

    let order_by_clauses = plan.order_by.as_mut().unwrap();
    let group_by_clauses = plan.group_by.as_mut().unwrap();

    let mut group_by_insert_position = 0;
    let mut order_index = 0;

    // This function optimizes query execution by eliminating duplicate expressions between ORDER BY and GROUP BY clauses
    // When the same column appears in both clauses, we can avoid redundant sorting operations
    // The function reorders GROUP BY expressions and removes redundant ORDER BY expressions to ensure consistent ordering
    while order_index < order_by_clauses.len() {
        let (order_expr, direction) = &order_by_clauses[order_index];

        // Skip descending orders as they require separate sorting
        if matches!(direction, Direction::Descending) {
            order_index += 1;
            continue;
        }

        // Check if the current ORDER BY expression matches any expression in the GROUP BY clause
        if let Some(group_expr_position) = group_by_clauses
            .exprs
            .iter()
            .position(|expr| exprs_are_equivalent(expr, order_expr))
        {
            // If we found a matching expression in GROUP BY, we need to ensure it's in the correct position
            // to preserve the ordering specified by ORDER BY clauses

            // Move the matching GROUP BY expression to the current insertion position
            // This effectively "bubbles up" the expression to maintain proper ordering
            if group_expr_position != group_by_insert_position {
                let mut current_position = group_expr_position;

                // Swap expressions to move the matching one to the correct position
                while current_position > group_by_insert_position {
                    group_by_clauses
                        .exprs
                        .swap(current_position, current_position - 1);
                    current_position -= 1;
                }
            }

            group_by_insert_position += 1;

            // Remove this expression from ORDER BY since it's now handled by GROUP BY
            order_by_clauses.remove(order_index);
            // Note: We don't increment order_index here because removal shifts all elements
        } else {
            // If not found in GROUP BY, move to next ORDER BY expression
            order_index += 1;
        }
    }
    if order_by_clauses.is_empty() {
        plan.order_by = None
    }
    Ok(())
}

fn eliminate_unnecessary_orderby(
    table_references: &mut [TableReference],
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
    order_by: &mut Option<Vec<(ast::Expr, Direction)>>,
    group_by: &Option<GroupBy>,
) -> Result<()> {
    let Some(order) = order_by else {
        return Ok(());
    };
    let Some(first_table_reference) = table_references.first_mut() else {
        return Ok(());
    };
    let Some(btree_table) = first_table_reference.btree() else {
        return Ok(());
    };
    // If GROUP BY clause is present, we can't rely on already ordered columns because GROUP BY reorders the data
    // This early return prevents the elimination of ORDER BY when GROUP BY exists, as sorting must be applied after grouping
    // And if ORDER BY clause duplicates GROUP BY we handle it later in fn eliminate_orderby_like_groupby
    if group_by.is_some() {
        return Ok(());
    }
    let Operation::Scan {
        index, iter_dir, ..
    } = &mut first_table_reference.op
    else {
        return Ok(());
    };

    assert!(
        index.is_none(),
        "Nothing shouldve transformed the scan to use an index yet"
    );

    // Special case: if ordering by just the rowid, we can remove the ORDER BY clause
    if order.len() == 1 && order[0].0.is_rowid_alias_of(0) {
        *iter_dir = match order[0].1 {
            Direction::Ascending => IterationDirection::Forwards,
            Direction::Descending => IterationDirection::Backwards,
        };
        *order_by = None;
        return Ok(());
    }

    // Find the best matching index for the ORDER BY columns
    let table_name = &btree_table.name;
    let mut best_index = (None, 0);

    for (_, indexes) in available_indexes.iter() {
        for index_candidate in indexes.iter().filter(|i| &i.table_name == table_name) {
            let matching_columns = index_candidate.columns.iter().enumerate().take_while(|(i, c)| {
                            if let Some((Expr::Column { table, column, .. }, _)) = order.get(*i) {
                                let col_idx_in_table = btree_table
                                    .columns
                                    .iter()
                                    .position(|tc| tc.name.as_ref() == Some(&c.name));
                                matches!(col_idx_in_table, Some(col_idx) if *table == 0 && *column == col_idx)
                            } else {
                                false
                            }
                        }).count();

            if matching_columns > best_index.1 {
                best_index = (Some(index_candidate), matching_columns);
            }
        }
    }

    let Some(matching_index) = best_index.0 else {
        return Ok(());
    };
    let match_count = best_index.1;

    // If we found a matching index, use it for scanning
    *index = Some(matching_index.clone());
    // If the order by direction matches the index direction, we can iterate the index in forwards order.
    // If they don't, we must iterate the index in backwards order.
    let index_direction = &matching_index.columns.first().as_ref().unwrap().order;
    *iter_dir = match (index_direction, order[0].1) {
        (SortOrder::Asc, Direction::Ascending) | (SortOrder::Desc, Direction::Descending) => {
            IterationDirection::Forwards
        }
        (SortOrder::Asc, Direction::Descending) | (SortOrder::Desc, Direction::Ascending) => {
            IterationDirection::Backwards
        }
    };

    // If the index covers all ORDER BY columns, and one of the following applies:
    // - the ORDER BY directions exactly match the index orderings,
    // - the ORDER by directions are the exact opposite of the index orderings,
    // we can remove the ORDER BY clause.
    if match_count == order.len() {
        let full_match = {
            let mut all_match_forward = true;
            let mut all_match_reverse = true;
            for (i, (_, direction)) in order.iter().enumerate() {
                match (&matching_index.columns[i].order, direction) {
                    (SortOrder::Asc, Direction::Ascending)
                    | (SortOrder::Desc, Direction::Descending) => {
                        all_match_reverse = false;
                    }
                    (SortOrder::Asc, Direction::Descending)
                    | (SortOrder::Desc, Direction::Ascending) => {
                        all_match_forward = false;
                    }
                }
            }
            all_match_forward || all_match_reverse
        };
        if full_match {
            *order_by = None;
        }
    }

    Ok(())
}

/**
 * Use indexes where possible.
 *
 * When this function is called, condition expressions from both the actual WHERE clause and the JOIN clauses are in the where_clause vector.
 * If we find a condition that can be used to index scan, we pop it off from the where_clause vector and put it into a Search operation.
 * We put it there simply because it makes it a bit easier to track during translation.
 *
 * In this function we also try to eliminate ORDER BY clauses if there is an index that satisfies the ORDER BY clause.
 */
fn use_indexes(
    table_references: &mut [TableReference],
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
    where_clause: &mut Vec<WhereTerm>,
    order_by: &mut Option<Vec<(ast::Expr, Direction)>>,
    group_by: &Option<GroupBy>,
) -> Result<()> {
    // Try to use indexes for eliminating ORDER BY clauses
    eliminate_unnecessary_orderby(table_references, available_indexes, order_by, group_by)?;

    // Try to use indexes for WHERE conditions
    for (table_index, table_reference) in table_references.iter_mut().enumerate() {
        if matches!(table_reference.op, Operation::Scan { .. }) {
            let index = if let Operation::Scan { index, .. } = &table_reference.op {
                Option::clone(index)
            } else {
                None
            };
            match index {
                // If we decided to eliminate ORDER BY using an index, let's constrain our search to only that index
                Some(index) => {
                    let available_indexes = available_indexes
                        .values()
                        .flatten()
                        .filter(|i| i.name == index.name)
                        .cloned()
                        .collect::<Vec<_>>();
                    if let Some(search) = try_extract_index_search_from_where_clause(
                        where_clause,
                        table_index,
                        table_reference,
                        &available_indexes,
                    )? {
                        table_reference.op = Operation::Search(search);
                    }
                }
                None => {
                    let table_name = table_reference.table.get_name();

                    // If we can utilize the rowid alias of the table, let's preferentially always use it for now.
                    let mut i = 0;
                    while i < where_clause.len() {
                        if let Some(search) = try_extract_rowid_search_expression(
                            &mut where_clause[i],
                            table_index,
                            table_reference,
                        )? {
                            where_clause.remove(i);
                            table_reference.op = Operation::Search(search);
                            continue;
                        } else {
                            i += 1;
                        }
                    }
                    if let Some(indexes) = available_indexes.get(table_name) {
                        if let Some(search) = try_extract_index_search_from_where_clause(
                            where_clause,
                            table_index,
                            table_reference,
                            indexes,
                        )? {
                            table_reference.op = Operation::Search(search);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

/// Removes predicates that are always true.
/// Returns a ConstantEliminationResult indicating whether any predicates are always false.
/// This is used to determine whether the query can be aborted early.
fn eliminate_constant_conditions(
    where_clause: &mut Vec<WhereTerm>,
) -> Result<ConstantConditionEliminationResult> {
    let mut i = 0;
    while i < where_clause.len() {
        let predicate = &where_clause[i];
        if predicate.expr.is_always_true()? {
            // true predicates can be removed since they don't affect the result
            where_clause.remove(i);
        } else if predicate.expr.is_always_false()? {
            // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false,
            // except an outer join condition, because that just results in NULLs, not skipping the whole loop
            if predicate.from_outer_join {
                i += 1;
                continue;
            }
            where_clause.truncate(0);
            return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
        } else {
            i += 1;
        }
    }

    Ok(ConstantConditionEliminationResult::Continue)
}

fn rewrite_exprs_select(plan: &mut SelectPlan) -> Result<()> {
    for rc in plan.result_columns.iter_mut() {
        rewrite_expr(&mut rc.expr)?;
    }
    for agg in plan.aggregates.iter_mut() {
        rewrite_expr(&mut agg.original_expr)?;
    }
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    if let Some(group_by) = &mut plan.group_by {
        for expr in group_by.exprs.iter_mut() {
            rewrite_expr(expr)?;
        }
    }
    if let Some(order_by) = &mut plan.order_by {
        for (expr, _) in order_by.iter_mut() {
            rewrite_expr(expr)?;
        }
    }

    Ok(())
}

fn rewrite_exprs_delete(plan: &mut DeletePlan) -> Result<()> {
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    Ok(())
}

fn rewrite_exprs_update(plan: &mut UpdatePlan) -> Result<()> {
    if let Some(rc) = plan.returning.as_mut() {
        for rc in rc.iter_mut() {
            rewrite_expr(&mut rc.expr)?;
        }
    }
    for (_, expr) in plan.set_clauses.iter_mut() {
        rewrite_expr(expr)?;
    }
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    if let Some(order_by) = &mut plan.order_by {
        for (expr, _) in order_by.iter_mut() {
            rewrite_expr(expr)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstantPredicate {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression e.g. '1', returns the constant condition
    fn check_constant(&self) -> Result<Option<ConstantPredicate>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantPredicate::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantPredicate::AlwaysFalse))
    }
    fn is_rowid_alias_of(&self, table_index: usize) -> bool;
    fn is_nonnull(&self) -> bool;
}

impl Optimizable for ast::Expr {
    fn is_rowid_alias_of(&self, table_index: usize) -> bool {
        match self {
            Self::Column {
                table,
                is_rowid_alias,
                ..
            } => *is_rowid_alias && *table == table_index,
            _ => false,
        }
    }
    /// Returns true if the expressions is (verifiably) non-NULL.
    /// It might still be non-NULL even if we return false; we just
    /// weren't able to prove it.
    /// This function is currently very conservative, and will return false
    /// for any expression where we aren't sure and didn't bother to find out
    /// by writing more complex code.
    fn is_nonnull(&self) -> bool {
        match self {
            Expr::Between {
                lhs, start, end, ..
            } => lhs.is_nonnull() && start.is_nonnull() && end.is_nonnull(),
            Expr::Binary(expr, _, expr1) => expr.is_nonnull() && expr1.is_nonnull(),
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
                ..
            } => {
                base.as_ref().map_or(true, |base| base.is_nonnull())
                    && when_then_pairs.iter().all(|(_, then)| then.is_nonnull())
                    && else_expr
                        .as_ref()
                        .map_or(true, |else_expr| else_expr.is_nonnull())
            }
            Expr::Cast { expr, .. } => expr.is_nonnull(),
            Expr::Collate(expr, _) => expr.is_nonnull(),
            Expr::DoublyQualified(..) => {
                panic!("Do not call is_nonnull before DoublyQualified has been rewritten as Column")
            }
            Expr::Exists(..) => false,
            Expr::FunctionCall { .. } => false,
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(..) => panic!("Do not call is_nonnull before Id has been rewritten as Column"),
            Expr::Column { is_rowid_alias, .. } => *is_rowid_alias,
            Expr::RowId { .. } => true,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_nonnull()
                    && rhs
                        .as_ref()
                        .map_or(true, |rhs| rhs.iter().all(|rhs| rhs.is_nonnull()))
            }
            Expr::InSelect { .. } => false,
            Expr::InTable { .. } => false,
            Expr::IsNull(..) => true,
            Expr::Like { lhs, rhs, .. } => lhs.is_nonnull() && rhs.is_nonnull(),
            Expr::Literal(literal) => match literal {
                ast::Literal::Numeric(_) => true,
                ast::Literal::String(_) => true,
                ast::Literal::Blob(_) => true,
                ast::Literal::Keyword(_) => true,
                ast::Literal::Null => false,
                ast::Literal::CurrentDate => true,
                ast::Literal::CurrentTime => true,
                ast::Literal::CurrentTimestamp => true,
            },
            Expr::Name(..) => false,
            Expr::NotNull(..) => true,
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_nonnull()),
            Expr::Qualified(..) => {
                panic!("Do not call is_nonnull before Qualified has been rewritten as Column")
            }
            Expr::Raise(..) => false,
            Expr::Subquery(..) => false,
            Expr::Unary(_, expr) => expr.is_nonnull(),
            Expr::Variable(..) => false,
        }
    }
    fn check_constant(&self) -> Result<Option<ConstantPredicate>> {
        match self {
            Self::Literal(lit) => match lit {
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    Ok(Some(ConstantPredicate::AlwaysFalse))
                }
                _ => Ok(None),
            },
            Self::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial.map(|t| match t {
                        ConstantPredicate::AlwaysTrue => ConstantPredicate::AlwaysFalse,
                        ConstantPredicate::AlwaysFalse => ConstantPredicate::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            Self::InList { lhs: _, not, rhs } => {
                if rhs.is_none() {
                    return Ok(Some(if *not {
                        ConstantPredicate::AlwaysTrue
                    } else {
                        ConstantPredicate::AlwaysFalse
                    }));
                }
                let rhs = rhs.as_ref().unwrap();
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        ConstantPredicate::AlwaysTrue
                    } else {
                        ConstantPredicate::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_constant()?;
                let rhs_trivial = rhs.check_constant()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                            || rhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysFalse));
                        }
                        if lhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                            && rhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                            || rhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysTrue));
                        }
                        if lhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                            && rhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

fn opposite_cmp_op(op: ast::Operator) -> ast::Operator {
    match op {
        ast::Operator::Equals => ast::Operator::Equals,
        ast::Operator::Greater => ast::Operator::Less,
        ast::Operator::GreaterEquals => ast::Operator::LessEquals,
        ast::Operator::Less => ast::Operator::Greater,
        ast::Operator::LessEquals => ast::Operator::GreaterEquals,
        _ => panic!("unexpected operator: {:?}", op),
    }
}

/// Struct used for scoring index scans
/// Currently we just score by the number of index columns that can be utilized
/// in the scan, i.e. no statistics are used.
struct IndexScore {
    index: Option<Arc<Index>>,
    score: usize,
    constraints: Vec<IndexConstraint>,
}

/// Try to extract an index search from the WHERE clause
/// Returns an optional [Search] struct if an index search can be extracted, otherwise returns None.
pub fn try_extract_index_search_from_where_clause(
    where_clause: &mut Vec<WhereTerm>,
    table_index: usize,
    table_reference: &TableReference,
    table_indexes: &[Arc<Index>],
) -> Result<Option<Search>> {
    // If there are no WHERE terms, we can't extract a search
    if where_clause.is_empty() {
        return Ok(None);
    }
    // If there are no indexes, we can't extract a search
    if table_indexes.is_empty() {
        return Ok(None);
    }

    let iter_dir = if let Operation::Scan { iter_dir, .. } = &table_reference.op {
        *iter_dir
    } else {
        return Ok(None);
    };

    // Find all potential index constraints
    // For WHERE terms to be used to constrain an index scan, they must:
    // 1. refer to columns in the table that the index is on
    // 2. be a binary comparison expression
    // 3. constrain the index columns in the order that they appear in the index
    //    - e.g. if the index is on (a,b,c) then we can use all of "a = 1 AND b = 2 AND c = 3" to constrain the index scan,
    //    - but if the where clause is "a = 1 and c = 3" then we can only use "a = 1".
    let mut constraints_cur = vec![];
    let mut best_index = IndexScore {
        index: None,
        score: 0,
        constraints: vec![],
    };

    for index in table_indexes {
        // Check how many terms in the where clause constrain the index in column order
        find_index_constraints(
            where_clause,
            table_index,
            table_reference,
            index,
            &mut constraints_cur,
        )?;
        // naive scoring since we don't have statistics: prefer the index where we can use the most columns
        // e.g. if we can use all columns of an index on (a,b), it's better than an index of (c,d,e) where we can only use c.
        let score = constraints_cur.len();
        if score > best_index.score {
            best_index.index = Some(Arc::clone(index));
            best_index.score = score;
            best_index.constraints.clear();
            best_index.constraints.append(&mut constraints_cur);
        }
    }

    if best_index.index.is_none() {
        return Ok(None);
    }

    // Build the seek definition
    let seek_def =
        build_seek_def_from_index_constraints(&best_index.constraints, iter_dir, where_clause)?;

    // Remove the used terms from the where_clause since they are now part of the seek definition
    // Sort terms by position in descending order to avoid shifting indices during removal
    best_index.constraints.sort_by(|a, b| {
        b.position_in_where_clause
            .0
            .cmp(&a.position_in_where_clause.0)
    });

    for constraint in best_index.constraints.iter() {
        where_clause.remove(constraint.position_in_where_clause.0);
    }

    return Ok(Some(Search::Seek {
        index: best_index.index,
        seek_def,
    }));
}

#[derive(Debug, Clone)]
/// A representation of an expression in a [WhereTerm] that can potentially be used as part of an index seek key.
/// For example, if there is an index on table T(x,y) and another index on table U(z), and the where clause is "WHERE x > 10 AND 20 = z",
/// the index constraints are:
/// - x > 10 ==> IndexConstraint { position_in_where_clause: (0, [BinaryExprSide::Rhs]), operator: [ast::Operator::Greater] }
/// - 20 = z ==> IndexConstraint { position_in_where_clause: (1, [BinaryExprSide::Lhs]), operator: [ast::Operator::Equals] }
pub struct IndexConstraint {
    position_in_where_clause: (usize, BinaryExprSide),
    operator: ast::Operator,
}

/// Helper enum for [IndexConstraint] to indicate which side of a binary comparison expression is being compared to the index column.
/// For example, if the where clause is "WHERE x = 10" and there's an index on x,
/// the [IndexConstraint] for the where clause term "x = 10" will have a [BinaryExprSide::Rhs]
/// because the right hand side expression "10" is being compared to the index column "x".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryExprSide {
    Lhs,
    Rhs,
}

/// Get the position of a column in an index
/// For example, if there is an index on table T(x,y) then y's position in the index is 1.
fn get_column_position_in_index(
    expr: &ast::Expr,
    table_index: usize,
    table_reference: &TableReference,
    index: &Arc<Index>,
) -> Option<usize> {
    let ast::Expr::Column { table, column, .. } = expr else {
        return None;
    };
    if *table != table_index {
        return None;
    }
    let Some(column) = table_reference.table.get_column_at(*column) else {
        return None;
    };
    index
        .columns
        .iter()
        .position(|col| Some(&col.name) == column.name.as_ref())
}

/// Find all [IndexConstraint]s for a given WHERE clause
/// Constraints are appended as long as they constrain the index in column order.
/// E.g. for index (a,b,c) to be fully used, there must be a [WhereTerm] for each of a, b, and c.
/// If e.g. only a and c are present, then only the first column 'a' of the index will be used.
fn find_index_constraints(
    where_clause: &mut Vec<WhereTerm>,
    table_index: usize,
    table_reference: &TableReference,
    index: &Arc<Index>,
    out_constraints: &mut Vec<IndexConstraint>,
) -> Result<()> {
    for position_in_index in 0..index.columns.len() {
        let mut found = false;
        for (position_in_where_clause, term) in where_clause.iter().enumerate() {
            // Skip terms that cannot be evaluated at this table's loop level
            if !term.should_eval_at_loop(table_index) {
                continue;
            }
            // Skip terms that are not binary comparisons
            let ast::Expr::Binary(lhs, operator, rhs) = &term.expr else {
                continue;
            };
            // Only consider index scans for binary ops that are comparisons
            if !matches!(
                *operator,
                ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals
            ) {
                continue;
            }

            // Check if lhs is a column that is in the i'th position of the index
            if Some(position_in_index)
                == get_column_position_in_index(lhs, table_index, table_reference, index)
            {
                out_constraints.push(IndexConstraint {
                    operator: *operator,
                    position_in_where_clause: (position_in_where_clause, BinaryExprSide::Rhs),
                });
                found = true;
                break;
            }
            // Check if rhs is a column that is in the i'th position of the index
            if Some(position_in_index)
                == get_column_position_in_index(rhs, table_index, table_reference, index)
            {
                out_constraints.push(IndexConstraint {
                    operator: opposite_cmp_op(*operator), // swap the operator since e.g. if condition is 5 >= x, we want to use x <= 5
                    position_in_where_clause: (position_in_where_clause, BinaryExprSide::Lhs),
                });
                found = true;
                break;
            }
        }
        if !found {
            // Expressions must constrain index columns in index definition order. If we didn't find a constraint for the i'th index column,
            // then we stop here and return the constraints we have found so far.
            break;
        }
    }

    // In a multicolumn index, only the last term can have a nonequality expression.
    // For example, imagine an index on (x,y) and the where clause is "WHERE x > 10 AND y > 20";
    // We can't use GT(x: 10,y: 20) as the seek key, because the first row greater than (x: 10,y: 20)
    // might be e.g. (x: 10,y: 21), which does not satisfy the where clause, but a row after that e.g. (x: 11,y: 21) does.
    // So:
    // - in this case only GT(x: 10) can be used as the seek key, and we must emit a regular condition expression for y > 20 while scanning.
    // On the other hand, if the where clause is "WHERE x = 10 AND y > 20", we can use GT(x=10,y=20) as the seek key,
    // because any rows where (x=10,y=20) < ROW < (x=11) will match the where clause.
    for i in 0..out_constraints.len() {
        if out_constraints[i].operator != ast::Operator::Equals {
            out_constraints.truncate(i + 1);
            break;
        }
    }

    Ok(())
}

/// Build a [SeekDef] for a given list of [IndexConstraint]s
pub fn build_seek_def_from_index_constraints(
    constraints: &[IndexConstraint],
    iter_dir: IterationDirection,
    where_clause: &mut Vec<WhereTerm>,
) -> Result<SeekDef> {
    assert!(
        !constraints.is_empty(),
        "cannot build seek def from empty list of index constraints"
    );
    // Extract the key values and operators
    let mut key = Vec::with_capacity(constraints.len());

    for constraint in constraints {
        // Extract the other expression from the binary WhereTerm (i.e. the one being compared to the index column)
        let (idx, side) = constraint.position_in_where_clause;
        let where_term = &mut where_clause[idx];
        let ast::Expr::Binary(lhs, _, rhs) = where_term.expr.take_ownership() else {
            crate::bail_parse_error!("expected binary expression");
        };
        let cmp_expr = if side == BinaryExprSide::Lhs {
            *lhs
        } else {
            *rhs
        };
        key.push(cmp_expr);
    }

    // We know all but potentially the last term is an equality, so we can use the operator of the last term
    // to form the SeekOp
    let op = constraints.last().unwrap().operator;

    build_seek_def(op, iter_dir, key)
}

/// Build a [SeekDef] for a given comparison operator and index key.
/// To be usable as a seek key, all but potentially the last term must be equalities.
/// The last term can be a nonequality.
/// The comparison operator referred to by `op` is the operator of the last term.
///
/// There are two parts to the seek definition:
/// 1. The [SeekKey], which specifies the key that we will use to seek to the first row that matches the index key.
/// 2. The [TerminationKey], which specifies the key that we will use to terminate the index scan that follows the seek.
///
/// There are some nuances to how, and which parts of, the index key can be used in the [SeekKey] and [TerminationKey],
/// depending on the operator and iteration direction. This function explains those nuances inline when dealing with
/// each case.
///
/// But to illustrate the general idea, consider the following examples:
///
/// 1. For example, having two conditions like (x>10 AND y>20) cannot be used as a valid [SeekKey] GT(x:10, y:20)
/// because the first row greater than (x:10, y:20) might be (x:10, y:21), which does not satisfy the where clause.
/// In this case, only GT(x:10) must be used as the [SeekKey], and rows with y <= 20 must be filtered as a regular condition expression for each value of x.
///
/// 2. In contrast, having (x=10 AND y>20) forms a valid index key GT(x:10, y:20) because after the seek, we can simply terminate as soon as x > 10,
/// i.e. use GT(x:10, y:20) as the [SeekKey] and GT(x:10) as the [TerminationKey].
///
fn build_seek_def(
    op: ast::Operator,
    iter_dir: IterationDirection,
    key: Vec<ast::Expr>,
) -> Result<SeekDef> {
    let key_len = key.len();
    Ok(match (iter_dir, op) {
        // Forwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: GE(x:10, y:20)
        // Termination key: GT(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                op: SeekOp::GE,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                op: SeekOp::GT,
            }),
        },
        // Forwards, GT:
        // Example: (x=10 AND y>20)
        // Seek key: GT(x:10, y:20)
        // Termination key: GT(x:10)
        (IterationDirection::Forwards, ast::Operator::Greater) => {
            let termination_key_len = key_len - 1;
            SeekDef {
                key,
                iter_dir,
                seek: Some(SeekKey {
                    len: key_len,
                    op: SeekOp::GT,
                }),
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: SeekOp::GT,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, GE:
        // Example: (x=10 AND y>=20)
        // Seek key: GE(x:10, y:20)
        // Termination key: GT(x:10)
        (IterationDirection::Forwards, ast::Operator::GreaterEquals) => {
            let termination_key_len = key_len - 1;
            SeekDef {
                key,
                iter_dir,
                seek: Some(SeekKey {
                    len: key_len,
                    op: SeekOp::GE,
                }),
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: SeekOp::GT,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, LT:
        // Example: (x=10 AND y<20)
        // Seek key: GT(x:10, y: NULL) // NULL is always LT, indicating we only care about x
        // Termination key: GE(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::Less) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len - 1,
                op: SeekOp::GT,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                op: SeekOp::GE,
            }),
        },
        // Forwards, LE:
        // Example: (x=10 AND y<=20)
        // Seek key: GE(x:10, y:NULL) // NULL is always LT, indicating we only care about x
        // Termination key: GT(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::LessEquals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len - 1,
                op: SeekOp::GE,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                op: SeekOp::GT,
            }),
        },
        // Backwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: LE(x:10, y:20)
        // Termination key: LT(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                op: SeekOp::LE,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                op: SeekOp::LT,
            }),
        },
        // Backwards, LT:
        // Example: (x=10 AND y<20)
        // Seek key: LT(x:10, y:20)
        // Termination key: LT(x:10)
        (IterationDirection::Backwards, ast::Operator::Less) => {
            let termination_key_len = key_len - 1;
            SeekDef {
                key,
                iter_dir,
                seek: Some(SeekKey {
                    len: key_len,
                    op: SeekOp::LT,
                }),
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: SeekOp::LT,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, LE:
        // Example: (x=10 AND y<=20)
        // Seek key: LE(x:10, y:20)
        // Termination key: LT(x:10)
        (IterationDirection::Backwards, ast::Operator::LessEquals) => {
            let termination_key_len = key_len - 1;
            SeekDef {
                key,
                iter_dir,
                seek: Some(SeekKey {
                    len: key_len,
                    op: SeekOp::LE,
                }),
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: SeekOp::LT,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, GT:
        // Example: (x=10 AND y>20)
        // Seek key: LE(x:10) // try to find the last row where x = 10, not considering y at all.
        // Termination key: LE(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::Greater) => {
            let seek_key_len = key_len - 1;
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: SeekOp::LE,
                    })
                } else {
                    None
                },
                termination: Some(TerminationKey {
                    len: key_len,
                    op: SeekOp::LE,
                }),
            }
        }
        // Backwards, GE:
        // Example: (x=10 AND y>=20)
        // Seek key: LE(x:10) // try to find the last row where x = 10, not considering y at all.
        // Termination key: LT(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::GreaterEquals) => {
            let seek_key_len = key_len - 1;
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: SeekOp::LE,
                    })
                } else {
                    None
                },
                termination: Some(TerminationKey {
                    len: key_len,
                    op: SeekOp::LT,
                }),
            }
        }
        (_, op) => {
            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
        }
    })
}

pub fn try_extract_rowid_search_expression(
    cond: &mut WhereTerm,
    table_index: usize,
    table_reference: &TableReference,
) -> Result<Option<Search>> {
    let iter_dir = if let Operation::Scan { iter_dir, .. } = &table_reference.op {
        *iter_dir
    } else {
        return Ok(None);
    };
    if !cond.should_eval_at_loop(table_index) {
        return Ok(None);
    }
    match &mut cond.expr {
        ast::Expr::Binary(lhs, operator, rhs) => {
            if lhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        let rhs_owned = rhs.take_ownership();
                        return Ok(Some(Search::RowidEq {
                            cmp_expr: WhereTerm {
                                expr: rhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at: cond.eval_at,
                            },
                        }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        let rhs_owned = rhs.take_ownership();
                        let seek_def = build_seek_def(*operator, iter_dir, vec![rhs_owned])?;
                        return Ok(Some(Search::Seek {
                            index: None,
                            seek_def,
                        }));
                    }
                    _ => {}
                }
            }

            if rhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        let lhs_owned = lhs.take_ownership();
                        return Ok(Some(Search::RowidEq {
                            cmp_expr: WhereTerm {
                                expr: lhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at: cond.eval_at,
                            },
                        }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        let lhs_owned = lhs.take_ownership();
                        let op = opposite_cmp_op(*operator);
                        let seek_def = build_seek_def(op, iter_dir, vec![lhs_owned])?;
                        return Ok(Some(Search::Seek {
                            index: None,
                            seek_def,
                        }));
                    }
                    _ => {}
                }
            }

            Ok(None)
        }
        _ => Ok(None),
    }
}

fn rewrite_expr(expr: &mut ast::Expr) -> Result<()> {
    match expr {
        ast::Expr::Id(id) => {
            // Convert "true" and "false" to 1 and 0
            if id.0.eq_ignore_ascii_case("true") {
                *expr = ast::Expr::Literal(ast::Literal::Numeric(1.to_string()));
                return Ok(());
            }
            if id.0.eq_ignore_ascii_case("false") {
                *expr = ast::Expr::Literal(ast::Literal::Numeric(0.to_string()));
                return Ok(());
            }
            Ok(())
        }
        ast::Expr::Between {
            lhs,
            not,
            start,
            end,
        } => {
            // Convert `y NOT BETWEEN x AND z` to `x > y OR y > z`
            let (lower_op, upper_op) = if *not {
                (ast::Operator::Greater, ast::Operator::Greater)
            } else {
                // Convert `y BETWEEN x AND z` to `x <= y AND y <= z`
                (ast::Operator::LessEquals, ast::Operator::LessEquals)
            };

            rewrite_expr(start)?;
            rewrite_expr(lhs)?;
            rewrite_expr(end)?;

            let start = start.take_ownership();
            let lhs = lhs.take_ownership();
            let end = end.take_ownership();

            let lower_bound = ast::Expr::Binary(Box::new(start), lower_op, Box::new(lhs.clone()));
            let upper_bound = ast::Expr::Binary(Box::new(lhs), upper_op, Box::new(end));

            if *not {
                *expr = ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::Or,
                    Box::new(upper_bound),
                );
            } else {
                *expr = ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::And,
                    Box::new(upper_bound),
                );
            }
            Ok(())
        }
        ast::Expr::Parenthesized(ref mut exprs) => {
            for subexpr in exprs.iter_mut() {
                rewrite_expr(subexpr)?;
            }
            let exprs = std::mem::take(exprs);
            *expr = ast::Expr::Parenthesized(exprs);
            Ok(())
        }
        // Process other expressions recursively
        ast::Expr::Binary(lhs, _, rhs) => {
            rewrite_expr(lhs)?;
            rewrite_expr(rhs)?;
            Ok(())
        }
        ast::Expr::FunctionCall { args, .. } => {
            if let Some(args) = args {
                for arg in args.iter_mut() {
                    rewrite_expr(arg)?;
                }
            }
            Ok(())
        }
        ast::Expr::Unary(_, arg) => {
            rewrite_expr(arg)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

trait TakeOwnership {
    fn take_ownership(&mut self) -> Self;
}

impl TakeOwnership for ast::Expr {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, ast::Expr::Literal(ast::Literal::Null))
    }
}
