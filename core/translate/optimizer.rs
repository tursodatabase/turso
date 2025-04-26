use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use limbo_sqlite3_parser::ast::{self, Expr, SortOrder};

use crate::{
    schema::{Index, IndexColumn, Schema},
    translate::plan::TerminationKey,
    types::SeekOp,
    util::exprs_are_equivalent,
    Result,
};

use super::{
    emitter::Resolver,
    plan::{
        DeletePlan, Direction, EvalAt, GroupBy, IterationDirection, Operation, Plan, Search,
        SeekDef, SeekKey, SelectPlan, TableReference, UpdatePlan, WhereTerm,
    },
    planner::determine_where_to_eval_expr,
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

/// Eliminate unnecessary ORDER BY clauses.
/// Returns true if the ORDER BY clause was eliminated.
fn eliminate_unnecessary_orderby(
    table_references: &mut [TableReference],
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
    order_by: &mut Option<Vec<(ast::Expr, Direction)>>,
    group_by: &Option<GroupBy>,
) -> Result<bool> {
    let Some(order) = order_by else {
        return Ok(false);
    };
    let Some(first_table_reference) = table_references.first_mut() else {
        return Ok(false);
    };
    let Some(btree_table) = first_table_reference.btree() else {
        return Ok(false);
    };
    // If GROUP BY clause is present, we can't rely on already ordered columns because GROUP BY reorders the data
    // This early return prevents the elimination of ORDER BY when GROUP BY exists, as sorting must be applied after grouping
    // And if ORDER BY clause duplicates GROUP BY we handle it later in fn eliminate_orderby_like_groupby
    if group_by.is_some() {
        return Ok(false);
    }
    let Operation::Scan {
        index, iter_dir, ..
    } = &mut first_table_reference.op
    else {
        return Ok(false);
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
        return Ok(true);
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
        return Ok(false);
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

    Ok(order_by.is_none())
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
    let did_eliminate_orderby =
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
                    if did_eliminate_orderby && table_index == 0 {
                        // If we already made the decision to remove ORDER BY based on the Rowid (e.g. ORDER BY id), then skip this.
                        // It would be possible to analyze the index and see if the covering index would retain the ordering guarantee,
                        // but we just don't do that yet.
                        continue;
                    }
                    let placeholder = vec![];
                    let mut usable_indexes_ref = &placeholder;
                    if let Some(indexes) = available_indexes.get(table_name) {
                        usable_indexes_ref = indexes;
                    }
                    if let Some(search) = try_extract_index_search_from_where_clause(
                        where_clause,
                        table_index,
                        table_reference,
                        usable_indexes_ref,
                    )? {
                        table_reference.op = Operation::Search(search);
                    }
                }
            }
        }

        // Finally, if there's no other reason to use an index, if an index covers the columns used in the query, let's use it.
        if let Some(indexes) = available_indexes.get(table_reference.table.get_name()) {
            for index_candidate in indexes.iter() {
                let is_covering = table_reference.index_is_covering(index_candidate);
                if let Operation::Scan { index, .. } = &mut table_reference.op {
                    if index.is_some() {
                        continue;
                    }
                    if is_covering {
                        *index = Some(index_candidate.clone());
                        break;
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
pub enum AlwaysTrueOrFalse {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression that, when evaluated as a condition, is always true or false
    // return a [ConstantPredicate].
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self
            .check_always_true_or_false()?
            .map_or(false, |c| c == AlwaysTrueOrFalse::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self
            .check_always_true_or_false()?
            .map_or(false, |c| c == AlwaysTrueOrFalse::AlwaysFalse))
    }
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool;
    fn is_rowid_alias_of(&self, table_index: usize) -> bool;
    fn is_nonnull(&self, tables: &[TableReference]) -> bool;
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
    fn is_nonnull(&self, tables: &[TableReference]) -> bool {
        match self {
            Expr::Between {
                lhs, start, end, ..
            } => lhs.is_nonnull(tables) && start.is_nonnull(tables) && end.is_nonnull(tables),
            Expr::Binary(expr, _, expr1) => expr.is_nonnull(tables) && expr1.is_nonnull(tables),
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
                ..
            } => {
                base.as_ref().map_or(true, |base| base.is_nonnull(tables))
                    && when_then_pairs
                        .iter()
                        .all(|(_, then)| then.is_nonnull(tables))
                    && else_expr
                        .as_ref()
                        .map_or(true, |else_expr| else_expr.is_nonnull(tables))
            }
            Expr::Cast { expr, .. } => expr.is_nonnull(tables),
            Expr::Collate(expr, _) => expr.is_nonnull(tables),
            Expr::DoublyQualified(..) => {
                panic!("Do not call is_nonnull before DoublyQualified has been rewritten as Column")
            }
            Expr::Exists(..) => false,
            Expr::FunctionCall { .. } => false,
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(..) => panic!("Do not call is_nonnull before Id has been rewritten as Column"),
            Expr::Column {
                table,
                column,
                is_rowid_alias,
                ..
            } => {
                if *is_rowid_alias {
                    return true;
                }

                let table_ref = &tables[*table];
                let columns = table_ref.columns();
                let column = &columns[*column];
                return column.primary_key || column.notnull;
            }
            Expr::RowId { .. } => true,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_nonnull(tables)
                    && rhs
                        .as_ref()
                        .map_or(true, |rhs| rhs.iter().all(|rhs| rhs.is_nonnull(tables)))
            }
            Expr::InSelect { .. } => false,
            Expr::InTable { .. } => false,
            Expr::IsNull(..) => true,
            Expr::Like { lhs, rhs, .. } => lhs.is_nonnull(tables) && rhs.is_nonnull(tables),
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
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_nonnull(tables)),
            Expr::Qualified(..) => {
                panic!("Do not call is_nonnull before Qualified has been rewritten as Column")
            }
            Expr::Raise(..) => false,
            Expr::Subquery(..) => false,
            Expr::Unary(_, expr) => expr.is_nonnull(tables),
            Expr::Variable(..) => false,
        }
    }
    /// Returns true if the expression is a constant i.e. does not depend on variables or columns etc.
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool {
        match self {
            Expr::Between {
                lhs, start, end, ..
            } => {
                lhs.is_constant(resolver)
                    && start.is_constant(resolver)
                    && end.is_constant(resolver)
            }
            Expr::Binary(expr, _, expr1) => {
                expr.is_constant(resolver) && expr1.is_constant(resolver)
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                base.as_ref()
                    .map_or(true, |base| base.is_constant(resolver))
                    && when_then_pairs.iter().all(|(when, then)| {
                        when.is_constant(resolver) && then.is_constant(resolver)
                    })
                    && else_expr
                        .as_ref()
                        .map_or(true, |else_expr| else_expr.is_constant(resolver))
            }
            Expr::Cast { expr, .. } => expr.is_constant(resolver),
            Expr::Collate(expr, _) => expr.is_constant(resolver),
            Expr::DoublyQualified(_, _, _) => {
                panic!("DoublyQualified should have been rewritten as Column")
            }
            Expr::Exists(_) => false,
            Expr::FunctionCall { args, name, .. } => {
                let Some(func) =
                    resolver.resolve_function(&name.0, args.as_ref().map_or(0, |args| args.len()))
                else {
                    return false;
                };
                func.is_deterministic()
                    && args.as_ref().map_or(true, |args| {
                        args.iter().all(|arg| arg.is_constant(resolver))
                    })
            }
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(_) => panic!("Id should have been rewritten as Column"),
            Expr::Column { .. } => false,
            Expr::RowId { .. } => false,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_constant(resolver)
                    && rhs
                        .as_ref()
                        .map_or(true, |rhs| rhs.iter().all(|rhs| rhs.is_constant(resolver)))
            }
            Expr::InSelect { .. } => {
                false // might be constant, too annoying to check subqueries etc. implement later
            }
            Expr::InTable { .. } => false,
            Expr::IsNull(expr) => expr.is_constant(resolver),
            Expr::Like {
                lhs, rhs, escape, ..
            } => {
                lhs.is_constant(resolver)
                    && rhs.is_constant(resolver)
                    && escape
                        .as_ref()
                        .map_or(true, |escape| escape.is_constant(resolver))
            }
            Expr::Literal(_) => true,
            Expr::Name(_) => false,
            Expr::NotNull(expr) => expr.is_constant(resolver),
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_constant(resolver)),
            Expr::Qualified(_, _) => {
                panic!("Qualified should have been rewritten as Column")
            }
            Expr::Raise(_, expr) => expr
                .as_ref()
                .map_or(true, |expr| expr.is_constant(resolver)),
            Expr::Subquery(_) => false,
            Expr::Unary(_, expr) => expr.is_constant(resolver),
            Expr::Variable(_) => false,
        }
    }
    /// Returns true if the expression is a constant expression that, when evaluated as a condition, is always true or false
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>> {
        match self {
            Self::Literal(lit) => match lit {
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(Some(AlwaysTrueOrFalse::AlwaysFalse))
                }
                _ => Ok(None),
            },
            Self::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial.map(|t| match t {
                        AlwaysTrueOrFalse::AlwaysTrue => AlwaysTrueOrFalse::AlwaysFalse,
                        AlwaysTrueOrFalse::AlwaysFalse => AlwaysTrueOrFalse::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            Self::InList { lhs: _, not, rhs } => {
                if rhs.is_none() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }
                let rhs = rhs.as_ref().unwrap();
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_always_true_or_false()?;
                let rhs_trivial = rhs.check_always_true_or_false()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
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
/// Currently we just estimate cost in a really dumb way,
/// i.e. no statistics are used.
struct IndexScore {
    index: Option<Arc<Index>>,
    cost: f64,
    constraints: Vec<IndexConstraint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexInfo {
    unique: bool,
    column_count: usize,
}

const ESTIMATED_HARDCODED_ROWS_PER_TABLE: f64 = 1000.0;

/// Unbelievably dumb cost estimate for rows scanned by an index scan.
fn dumb_cost_estimator(
    index_info: Option<IndexInfo>,
    constraints: &[IndexConstraint],
    is_inner_loop: bool,
    is_ephemeral: bool,
) -> f64 {
    // assume that the outer table always does a full table scan :)
    // this discourages building ephemeral indexes on the outer table
    // (since a scan reads TABLE_ROWS rows, so an ephemeral index on the outer table would both read TABLE_ROWS rows to build the index and then seek the index)
    // but encourages building it on the inner table because it's only built once but the inner loop is run as many times as the outer loop has iterations.
    let loop_multiplier = if is_inner_loop {
        ESTIMATED_HARDCODED_ROWS_PER_TABLE
    } else {
        1.0
    };

    // If we are building an ephemeral index, we assume we will scan the entire source table to build it.
    // Non-ephemeral indexes don't need to be built.
    let cost_to_build_index = is_ephemeral as usize as f64 * ESTIMATED_HARDCODED_ROWS_PER_TABLE;

    let Some(index_info) = index_info else {
        return cost_to_build_index + ESTIMATED_HARDCODED_ROWS_PER_TABLE * loop_multiplier;
    };

    let final_constraint_is_range = constraints
        .last()
        .map_or(false, |c| c.operator != ast::Operator::Equals);
    let equalities_count = constraints
        .iter()
        .take(if final_constraint_is_range {
            constraints.len() - 1
        } else {
            constraints.len()
        })
        .count() as f64;

    let selectivity = match (
        index_info.unique,
        index_info.column_count as f64,
        equalities_count,
    ) {
        // no equalities: let's assume range query selectivity is 0.4. if final constraint is not range and there are no equalities, it means full table scan incoming
        (_, _, 0.0) => {
            if final_constraint_is_range {
                0.4
            } else {
                1.0
            }
        }
        // on an unique index if we have equalities across all index columns, assume very high selectivity
        (true, index_cols, eq_count) if eq_count == index_cols => 0.01 * eq_count,
        // some equalities: let's assume each equality has a selectivity of 0.1 and range query selectivity is 0.4
        (_, _, eq_count) => (eq_count * 0.1) * if final_constraint_is_range { 0.4 } else { 1.0 },
    };
    cost_to_build_index + selectivity * ESTIMATED_HARDCODED_ROWS_PER_TABLE * loop_multiplier
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
    let cost_of_full_table_scan = dumb_cost_estimator(None, &[], table_index != 0, false);
    let mut constraints_cur = vec![];
    let mut best_index = IndexScore {
        index: None,
        cost: cost_of_full_table_scan,
        constraints: vec![],
    };

    for index in table_indexes {
        // Check how many terms in the where clause constrain the index in column order
        find_index_constraints(where_clause, table_index, index, &mut constraints_cur)?;
        // naive scoring since we don't have statistics: prefer the index where we can use the most columns
        // e.g. if we can use all columns of an index on (a,b), it's better than an index of (c,d,e) where we can only use c.
        let cost = dumb_cost_estimator(
            Some(IndexInfo {
                unique: index.unique,
                column_count: index.columns.len(),
            }),
            &constraints_cur,
            table_index != 0,
            false,
        );
        if cost < best_index.cost {
            best_index.index = Some(Arc::clone(index));
            best_index.cost = cost;
            best_index.constraints.clear();
            best_index.constraints.append(&mut constraints_cur);
        }
    }

    // We haven't found a persistent btree index that is any better than a full table scan;
    // let's see if building an ephemeral index would be better.
    if best_index.index.is_none() {
        let (ephemeral_cost, constraints_with_col_idx, mut constraints_without_col_idx) =
            ephemeral_index_estimate_cost(where_clause, table_reference, table_index);
        if ephemeral_cost < best_index.cost {
            // ephemeral index makes sense, so let's build it now.
            // ephemeral columns are: columns from the table_reference, constraints first, then the rest
            let ephemeral_index =
                ephemeral_index_build(table_reference, table_index, &constraints_with_col_idx);
            best_index.index = Some(Arc::new(ephemeral_index));
            best_index.cost = ephemeral_cost;
            best_index.constraints.clear();
            best_index
                .constraints
                .append(&mut constraints_without_col_idx);
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

fn ephemeral_index_estimate_cost(
    where_clause: &mut Vec<WhereTerm>,
    table_reference: &TableReference,
    table_index: usize,
) -> (f64, Vec<(usize, IndexConstraint)>, Vec<IndexConstraint>) {
    let mut constraints_with_col_idx: Vec<(usize, IndexConstraint)> = where_clause
        .iter()
        .enumerate()
        .filter(|(_, term)| is_potential_index_constraint(term, table_index))
        .filter_map(|(i, term)| {
            let Ok(ast::Expr::Binary(lhs, operator, rhs)) = unwrap_parens(&term.expr) else {
                panic!("expected binary expression");
            };
            if let ast::Expr::Column { table, column, .. } = lhs.as_ref() {
                if *table == table_index {
                    return Some((
                        *column,
                        IndexConstraint {
                            position_in_where_clause: (i, BinaryExprSide::Rhs),
                            operator: *operator,
                            index_column_sort_order: SortOrder::Asc,
                        },
                    ));
                }
            }
            if let ast::Expr::Column { table, column, .. } = rhs.as_ref() {
                if *table == table_index {
                    return Some((
                        *column,
                        IndexConstraint {
                            position_in_where_clause: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(*operator),
                            index_column_sort_order: SortOrder::Asc,
                        },
                    ));
                }
            }
            None
        })
        .collect();
    // sort equalities first
    constraints_with_col_idx.sort_by(|a, _| {
        if a.1.operator == ast::Operator::Equals {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    });
    // drop everything after the first inequality
    constraints_with_col_idx.truncate(
        constraints_with_col_idx
            .iter()
            .position(|c| c.1.operator != ast::Operator::Equals)
            .unwrap_or(constraints_with_col_idx.len()),
    );

    let ephemeral_column_count = table_reference
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| table_reference.column_is_used(*i))
        .count();

    let constraints_without_col_idx = constraints_with_col_idx
        .iter()
        .cloned()
        .map(|(_, c)| c)
        .collect::<Vec<_>>();
    let ephemeral_cost = dumb_cost_estimator(
        Some(IndexInfo {
            unique: false,
            column_count: ephemeral_column_count,
        }),
        &constraints_without_col_idx,
        table_index != 0,
        true,
    );
    (
        ephemeral_cost,
        constraints_with_col_idx,
        constraints_without_col_idx,
    )
}

fn ephemeral_index_build(
    table_reference: &TableReference,
    table_index: usize,
    index_constraints: &[(usize, IndexConstraint)],
) -> Index {
    let mut ephemeral_columns: Vec<IndexColumn> = table_reference
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| IndexColumn {
            name: c.name.clone().unwrap(),
            order: SortOrder::Asc,
            pos_in_table: i,
        })
        // only include columns that are used in the query
        .filter(|c| table_reference.column_is_used(c.pos_in_table))
        .collect();
    // sort so that constraints first, then rest in whatever order they were in in the table
    ephemeral_columns.sort_by(|a, b| {
        let a_constraint = index_constraints
            .iter()
            .enumerate()
            .find(|(_, c)| c.0 == a.pos_in_table);
        let b_constraint = index_constraints
            .iter()
            .enumerate()
            .find(|(_, c)| c.0 == b.pos_in_table);
        match (a_constraint, b_constraint) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a_idx, _)), Some((b_idx, _))) => a_idx.cmp(&b_idx),
            (None, None) => Ordering::Equal,
        }
    });
    let ephemeral_index = Index {
        name: format!(
            "ephemeral_{}_{}",
            table_reference.table.get_name(),
            table_index
        ),
        columns: ephemeral_columns,
        unique: false,
        ephemeral: true,
        table_name: table_reference.table.get_name().to_string(),
        root_page: 0,
    };

    ephemeral_index
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
    index_column_sort_order: SortOrder,
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

/// Recursively unwrap parentheses from an expression
/// e.g. (((t.x > 5))) -> t.x > 5
fn unwrap_parens<T>(expr: T) -> Result<T>
where
    T: UnwrapParens,
{
    expr.unwrap_parens()
}

trait UnwrapParens {
    fn unwrap_parens(self) -> Result<Self>
    where
        Self: Sized;
}

impl UnwrapParens for &ast::Expr {
    fn unwrap_parens(self) -> Result<Self> {
        match self {
            ast::Expr::Column { .. } => Ok(self),
            ast::Expr::Parenthesized(exprs) => match exprs.len() {
                1 => unwrap_parens(exprs.first().unwrap()),
                _ => crate::bail_parse_error!("expected single expression in parentheses"),
            },
            _ => Ok(self),
        }
    }
}

impl UnwrapParens for ast::Expr {
    fn unwrap_parens(self) -> Result<Self> {
        match self {
            ast::Expr::Column { .. } => Ok(self),
            ast::Expr::Parenthesized(mut exprs) => match exprs.len() {
                1 => unwrap_parens(exprs.pop().unwrap()),
                _ => crate::bail_parse_error!("expected single expression in parentheses"),
            },
            _ => Ok(self),
        }
    }
}

/// Get the position of a column in an index
/// For example, if there is an index on table T(x,y) then y's position in the index is 1.
fn get_column_position_in_index(
    expr: &ast::Expr,
    table_index: usize,
    index: &Arc<Index>,
) -> Result<Option<usize>> {
    let ast::Expr::Column { table, column, .. } = unwrap_parens(expr)? else {
        return Ok(None);
    };
    if *table != table_index {
        return Ok(None);
    }
    Ok(index.column_table_pos_to_index_pos(*column))
}

fn is_potential_index_constraint(term: &WhereTerm, table_index: usize) -> bool {
    // Skip terms that cannot be evaluated at this table's loop level
    if !term.should_eval_at_loop(table_index) {
        return false;
    }
    // Skip terms that are not binary comparisons
    let Ok(ast::Expr::Binary(lhs, operator, rhs)) = unwrap_parens(&term.expr) else {
        return false;
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
        return false;
    }

    // If both lhs and rhs refer to columns from this table, we can't use this constraint
    // because we can't use the index to satisfy the condition.
    // Examples:
    // - WHERE t.x > t.y
    // - WHERE t.x + 1 > t.y - 5
    // - WHERE t.x = (t.x)
    let Ok(eval_at_left) = determine_where_to_eval_expr(&lhs) else {
        return false;
    };
    let Ok(eval_at_right) = determine_where_to_eval_expr(&rhs) else {
        return false;
    };
    if eval_at_left == EvalAt::Loop(table_index) && eval_at_right == EvalAt::Loop(table_index) {
        return false;
    }
    true
}

/// Find all [IndexConstraint]s for a given WHERE clause
/// Constraints are appended as long as they constrain the index in column order.
/// E.g. for index (a,b,c) to be fully used, there must be a [WhereTerm] for each of a, b, and c.
/// If e.g. only a and c are present, then only the first column 'a' of the index will be used.
fn find_index_constraints(
    where_clause: &mut Vec<WhereTerm>,
    table_index: usize,
    index: &Arc<Index>,
    out_constraints: &mut Vec<IndexConstraint>,
) -> Result<()> {
    for position_in_index in 0..index.columns.len() {
        let mut found = false;
        for (position_in_where_clause, term) in where_clause.iter().enumerate() {
            if !is_potential_index_constraint(term, table_index) {
                continue;
            }

            let ast::Expr::Binary(lhs, operator, rhs) = unwrap_parens(&term.expr)? else {
                panic!("expected binary expression");
            };

            // Check if lhs is a column that is in the i'th position of the index
            if Some(position_in_index) == get_column_position_in_index(lhs, table_index, index)? {
                out_constraints.push(IndexConstraint {
                    operator: *operator,
                    position_in_where_clause: (position_in_where_clause, BinaryExprSide::Rhs),
                    index_column_sort_order: index.columns[position_in_index].order,
                });
                found = true;
                break;
            }
            // Check if rhs is a column that is in the i'th position of the index
            if Some(position_in_index) == get_column_position_in_index(rhs, table_index, index)? {
                out_constraints.push(IndexConstraint {
                    operator: opposite_cmp_op(*operator), // swap the operator since e.g. if condition is 5 >= x, we want to use x <= 5
                    position_in_where_clause: (position_in_where_clause, BinaryExprSide::Lhs),
                    index_column_sort_order: index.columns[position_in_index].order,
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
        let ast::Expr::Binary(lhs, _, rhs) = unwrap_parens(where_term.expr.take_ownership())?
        else {
            crate::bail_parse_error!("expected binary expression");
        };
        let cmp_expr = if side == BinaryExprSide::Lhs {
            *lhs
        } else {
            *rhs
        };
        key.push((cmp_expr, constraint.index_column_sort_order));
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
/// The preceding examples are for an ascending index. The logic is similar for descending indexes, but an important distinction is that
/// since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
/// So when you see e.g. a SeekOp::GT below for a descending index, it actually means that we are seeking the first row where the index key is LESS than the seek key.
///
fn build_seek_def(
    op: ast::Operator,
    iter_dir: IterationDirection,
    key: Vec<(ast::Expr, SortOrder)>,
) -> Result<SeekDef> {
    let key_len = key.len();
    let sort_order_of_last_key = key.last().unwrap().1;

    // For the commented examples below, keep in mind that since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
    // Also keep in mind that index keys are compared based on the number of columns given, so for example:
    // - if key is GT(x:10), then (x=10, y=usize::MAX) is not GT because only X is compared. (x=11, y=<any>) is GT.
    // - if key is GT(x:10, y:20), then (x=10, y=21) is GT because both X and Y are compared.
    // - if key is GT(x:10, y:NULL), then (x=10, y=0) is GT because NULL is always LT in index key comparisons.
    Ok(match (iter_dir, op) {
        // Forwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: start from the first GE(x:10, y:20)
        // Termination key: end at the first GT(x:10, y:20)
        // Ascending vs descending doesn't matter because all the comparisons are equalities.
        (IterationDirection::Forwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                null_pad: false,
                op: SeekOp::GE,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                null_pad: false,
                op: SeekOp::GT,
            }),
        },
        // Forwards, GT:
        // Ascending index example: (x=10 AND y>20)
        // Seek key: start from the first GT(x:10, y:20), e.g. (x=10, y=21)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0)
        //
        // Descending index example: (x=10 AND y>20)
        // Seek key: start from the first LE(x:10), e.g. (x=10, y=usize::MAX), so reversed -> GE(x:10)
        // Termination key: end at the first LE(x:10, y:20), e.g. (x=10, y=20) so reversed -> GE(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::Greater) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::GT, SeekOp::GT)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE.reverse(),
                        SeekOp::LE.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, GE:
        // Ascending index example: (x=10 AND y>=20)
        // Seek key: start from the first GE(x:10, y:20), e.g. (x=10, y=20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0)
        //
        // Descending index example: (x=10 AND y>=20)
        // Seek key: start from the first LE(x:10), e.g. (x=10, y=usize::MAX), so reversed -> GE(x:10)
        // Termination key: end at the first LT(x:10, y:20), e.g. (x=10, y=19), so reversed -> GT(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::GreaterEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::GE, SeekOp::GT)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE.reverse(),
                        SeekOp::LT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, LT:
        // Ascending index example: (x=10 AND y<20)
        // Seek key: start from the first GT(x:10, y: NULL), e.g. (x=10, y=0)
        // Termination key: end at the first GE(x:10, y:20), e.g. (x=10, y=20)
        //
        // Descending index example: (x=10 AND y<20)
        // Seek key: start from the first LT(x:10, y:20), e.g. (x=10, y=19), so reversed -> GT(x:10, y:20)
        // Termination key: end at the first LT(x:10), e.g. (x=9, y=usize::MAX), so reversed -> GE(x:10, NULL); i.e. GE the smallest possible (x=10, y) combination (NULL is always LT)
        (IterationDirection::Forwards, ast::Operator::Less) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::GT, SeekOp::GE)
                } else {
                    (key_len, key_len - 1, SeekOp::GT, SeekOp::GE)
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, LE:
        // Ascending index example: (x=10 AND y<=20)
        // Seek key: start from the first GE(x:10, y:NULL), e.g. (x=10, y=0)
        // Termination key: end at the first GT(x:10, y:20), e.g. (x=10, y=21)
        //
        // Descending index example: (x=10 AND y<=20)
        // Seek key: start from the first LE(x:10, y:20), e.g. (x=10, y=20) so reversed -> GE(x:10, y:20)
        // Termination key: end at the first LT(x:10), e.g. (x=9, y=usize::MAX), so reversed -> GE(x:10, NULL); i.e. GE the smallest possible (x=10, y) combination (NULL is always LT)
        (IterationDirection::Forwards, ast::Operator::LessEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::GT, SeekOp::GT)
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::LE.reverse(),
                        SeekOp::LE.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: start from the last LE(x:10, y:20)
        // Termination key: end at the first LT(x:10, y:20)
        // Ascending vs descending doesn't matter because all the comparisons are equalities.
        (IterationDirection::Backwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                op: SeekOp::LE,
                null_pad: false,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                op: SeekOp::LT,
                null_pad: false,
            }),
        },
        // Backwards, LT:
        // Ascending index example: (x=10 AND y<20)
        // Seek key: start from the last LT(x:10, y:20), e.g. (x=10, y=19)
        // Termination key: end at the first LE(x:10, NULL), e.g. (x=9, y=usize::MAX)
        //
        // Descending index example: (x=10 AND y<20)
        // Seek key: start from the last GT(x:10, y:NULL), e.g. (x=10, y=0) so reversed -> LT(x:10, NULL)
        // Termination key: end at the first GE(x:10, y:20), e.g. (x=10, y=20) so reversed -> LE(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::Less) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::LT, SeekOp::LE)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::GT.reverse(),
                        SeekOp::GE.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, LE:
        // Ascending index example: (x=10 AND y<=20)
        // Seek key: start from the last LE(x:10, y:20), e.g. (x=10, y=20)
        // Termination key: end at the first LT(x:10, NULL), e.g. (x=9, y=usize::MAX)
        //
        // Descending index example: (x=10 AND y<=20)
        // Seek key: start from the last GT(x:10, NULL), e.g. (x=10, y=0) so reversed -> LT(x:10, NULL)
        // Termination key: end at the first GT(x:10, y:20), e.g. (x=10, y=21) so reversed -> LT(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::LessEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::LE, SeekOp::LE)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::GT.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, GT:
        // Ascending index example: (x=10 AND y>20)
        // Seek key: start from the last LE(x:10), e.g. (x=10, y=usize::MAX)
        // Termination key: end at the first LE(x:10, y:20), e.g. (x=10, y=20)
        //
        // Descending index example: (x=10 AND y>20)
        // Seek key: start from the last GT(x:10, y:20), e.g. (x=10, y=21) so reversed -> LT(x:10, y:20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0) so reversed -> LT(x:10)
        (IterationDirection::Backwards, ast::Operator::Greater) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::LE, SeekOp::LE)
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GT.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, GE:
        // Ascending index example: (x=10 AND y>=20)
        // Seek key: start from the last LE(x:10), e.g. (x=10, y=usize::MAX)
        // Termination key: end at the first LT(x:10, y:20), e.g. (x=10, y=19)
        //
        // Descending index example: (x=10 AND y>=20)
        // Seek key: start from the last GE(x:10, y:20), e.g. (x=10, y=20) so reversed -> LE(x:10, y:20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0) so reversed -> LT(x:10)
        (IterationDirection::Backwards, ast::Operator::GreaterEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::LE, SeekOp::LT)
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GE.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
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
            // If both lhs and rhs refer to columns from this table, we can't perform a rowid seek
            // Examples:
            // - WHERE t.x > t.y
            // - WHERE t.x + 1 > t.y - 5
            // - WHERE t.x = (t.x)
            if determine_where_to_eval_expr(lhs)? == EvalAt::Loop(table_index)
                && determine_where_to_eval_expr(rhs)? == EvalAt::Loop(table_index)
            {
                return Ok(None);
            }
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
                        let seek_def =
                            build_seek_def(*operator, iter_dir, vec![(rhs_owned, SortOrder::Asc)])?;
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
                        let seek_def =
                            build_seek_def(op, iter_dir, vec![(lhs_owned, SortOrder::Asc)])?;
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
