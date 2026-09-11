//! Raise a prepared `SelectPlan` into a `Block` tree.

use crate::schema::{FromClauseSubquery, Table};
use crate::sync::Arc;
use crate::translate::plan::{Distinctness, JoinedTable, Plan, SelectPlan};

use crate::translate::expr::{walk_expr_mut, WalkControl};
use crate::translate::plan::{NonFromClauseSubquery, SubqueryState};
use crate::Result;
use turso_parser::ast::{Expr, SubqueryType, TableInternalId};

use super::{
    placeholder_select_plan, Aggregate, Block, DependentJoin, DependentJoinKind, DerivedTable,
    Distinct, Filter, Join, Limit, LogicalPlan, Project, Scan, Sort,
};

/// Move a prepared SELECT into a tree. Return `None` and leave the plan as it
/// is when the tree cannot hold the plan yet.
pub(crate) fn raise_select_plan(plan: &mut SelectPlan) -> Option<Block> {
    if !select_plan_can_be_raised(plan) {
        return None;
    }
    Some(take_block(plan))
}

fn select_plan_can_be_raised(plan: &SelectPlan) -> bool {
    if !plan.values.is_empty()
        || plan.window.is_some()
        || plan.simple_aggregate.is_some()
        || plan.contains_constant_false_condition
    {
        return false;
    }
    if plan.where_clause.iter().any(|term| term.consumed) {
        return false;
    }
    let tables = plan.table_references.joined_tables();
    if plan.join_order.len() != tables.len()
        || plan
            .join_order
            .iter()
            .enumerate()
            .any(|(index, member)| member.original_idx != index)
    {
        return false;
    }
    if tables
        .first()
        .is_some_and(|table| table.join_info.is_some())
    {
        return false;
    }
    if tables.iter().skip(1).any(|table| table.join_info.is_none()) {
        return false;
    }
    let joined_after_first = |id| tables.iter().skip(1).any(|table| table.internal_id == id);
    !plan.where_clause.iter().any(|term| {
        term.from_outer_join
            .is_some_and(|table_id| !joined_after_first(table_id))
    })
}

fn take_block(plan: &mut SelectPlan) -> Block {
    let mut table_references = std::mem::take(&mut plan.table_references);
    let right_join_swapped = table_references.right_join_swapped();
    let outer_query_refs = table_references.take_outer_query_refs();
    let mut tables = std::mem::take(table_references.joined_tables_mut());
    plan.join_order.clear();

    let mut root = if tables.is_empty() {
        LogicalPlan::OneRow
    } else {
        leaf(tables.remove(0))
    };
    for mut table in tables {
        let info = table
            .join_info
            .take()
            .expect("checked: every table after the first has join info");
        root = LogicalPlan::Join(Join {
            left: Box::new(root),
            right: Box::new(leaf(table)),
            info,
        });
    }
    let mut subqueries = std::mem::take(&mut plan.non_from_clause_subqueries);
    let mut raised_ids = Vec::new();
    root = raise_scalar_subqueries(root, &mut subqueries, &mut raised_ids);

    let where_terms = std::mem::take(&mut plan.where_clause);
    if !where_terms.is_empty() {
        root = LogicalPlan::Filter(Filter {
            input: Box::new(root),
            terms: where_terms,
        });
    }

    let group_by = plan.group_by.take();
    let aggregates = std::mem::take(&mut plan.aggregates);
    if group_by.is_some() || !aggregates.is_empty() {
        root = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(root),
            group_by,
            aggregates,
        });
    }
    root = LogicalPlan::Project(Project {
        input: Box::new(root),
        columns: std::mem::take(&mut plan.result_columns),
    });
    let distinctness = std::mem::replace(&mut plan.distinctness, Distinctness::NonDistinct);
    if distinctness.is_distinct() {
        root = LogicalPlan::Distinct(Distinct {
            input: Box::new(root),
            distinctness,
        });
    }
    let keys = std::mem::take(&mut plan.order_by);
    if !keys.is_empty() {
        root = LogicalPlan::Sort(Sort {
            input: Box::new(root),
            keys,
        });
    }
    let limit = plan.limit.take();
    let offset = plan.offset.take();
    if limit.is_some() || offset.is_some() {
        root = LogicalPlan::Limit(Limit {
            input: Box::new(root),
            limit,
            offset,
        });
    }

    if !raised_ids.is_empty() {
        root.for_each_expr_mut(&mut |expr| {
            walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
                if let Expr::SubqueryResult { subquery_id, .. } = expr {
                    if raised_ids.contains(subquery_id) {
                        *expr = Expr::Column {
                            database: None,
                            table: *subquery_id,
                            column: 0,
                            is_rowid_alias: false,
                        };
                        return Ok(WalkControl::SkipChildren);
                    }
                }
                Ok(WalkControl::Continue)
            })
            .map(|_| ())
        })
        .expect("rewriting subquery references cannot fail");
    }

    Block {
        root,
        subqueries,
        outer_query_refs,
        right_join_swapped,
        query_destination: std::mem::replace(
            &mut plan.query_destination,
            crate::translate::plan::QueryDestination::Unset,
        ),
        input_cardinality_hint: plan.input_cardinality_hint.take(),
        phantom_params: std::mem::take(&mut plan.phantom_params),
    }
}

/// Put each correlated scalar subquery of the block under a dependent join
/// above the join tree. The subquery entries leave the list; the join keeps
/// them so the lowering can restore the prepared form.
fn raise_scalar_subqueries(
    mut root: LogicalPlan,
    subqueries: &mut Vec<NonFromClauseSubquery>,
    raised_ids: &mut Vec<TableInternalId>,
) -> LogicalPlan {
    let targets: Vec<TableInternalId> = subqueries
        .iter()
        .filter(|subquery| scalar_subquery_can_be_raised(subquery))
        .map(|subquery| subquery.internal_id)
        .collect();
    if targets.is_empty() {
        return root;
    }
    let entries: Vec<(usize, NonFromClauseSubquery)> =
        std::mem::take(subqueries).into_iter().enumerate().collect();
    let mut raised: Vec<(usize, NonFromClauseSubquery)> = Vec::new();
    for (position, subquery) in entries {
        let is_target = targets.contains(&subquery.internal_id);
        let follows_target = subquery
            .same_query
            .is_some_and(|earlier| targets.contains(&earlier))
            && matches!(
                subquery.query_type,
                SubqueryType::RowValue { num_regs: 1, .. }
            );
        if is_target || follows_target {
            raised.push((position, subquery));
        } else {
            subqueries.push(subquery);
        }
    }
    let mut duplicates_of: Vec<(TableInternalId, Vec<(usize, NonFromClauseSubquery)>)> = Vec::new();
    let mut primaries: Vec<(usize, NonFromClauseSubquery)> = Vec::new();
    for (position, subquery) in raised {
        raised_ids.push(subquery.internal_id);
        match subquery.same_query {
            Some(earlier)
                if targets.contains(&earlier) && !targets.contains(&subquery.internal_id) =>
            {
                match duplicates_of.iter_mut().find(|(id, _)| *id == earlier) {
                    Some((_, list)) => list.push((position, subquery)),
                    None => duplicates_of.push((earlier, vec![(position, subquery)])),
                }
            }
            _ => primaries.push((position, subquery)),
        }
    }
    for (position, mut subquery) in primaries {
        let SubqueryState::Unevaluated { plan } = &mut subquery.state else {
            unreachable!("checked: the subquery has not run")
        };
        let Plan::Select(mut inner) = *plan.take().expect("checked: the subquery has a plan")
        else {
            unreachable!("checked: the subquery is a SELECT")
        };
        let duplicates = duplicates_of
            .iter_mut()
            .find(|(id, _)| *id == subquery.internal_id)
            .map(|(_, list)| std::mem::take(list))
            .unwrap_or_default();
        let right = LogicalPlan::DerivedTable(DerivedTable {
            identifier: format!("scalar_subquery_{}", subquery.internal_id),
            internal_id: subquery.internal_id,
            shell: None,
            value_without_collation: true,
            block: Box::new(take_block(&mut inner)),
        });
        root = LogicalPlan::DependentJoin(DependentJoin {
            left: Box::new(root),
            right: Box::new(right),
            kind: DependentJoinKind::Scalar {
                subquery,
                position,
                duplicates,
            },
        });
    }
    root
}

fn scalar_subquery_can_be_raised(subquery: &NonFromClauseSubquery) -> bool {
    if !subquery.correlated
        || subquery.same_query.is_some()
        || !matches!(
            subquery.query_type,
            SubqueryType::RowValue { num_regs: 1, .. }
        )
    {
        return false;
    }
    let SubqueryState::Unevaluated { plan: Some(plan) } = &subquery.state else {
        return false;
    };
    let Plan::Select(inner) = plan.as_ref() else {
        return false;
    };
    select_plan_can_be_raised(inner)
}

fn leaf(mut table: JoinedTable) -> LogicalPlan {
    let inner = match &mut table.table {
        Table::FromClauseSubquery(subquery) => take_select_plan(subquery),
        Table::BTree(_) | Table::Virtual(_) | Table::RecursiveCteInput(_) => None,
    };
    match inner {
        Some(mut inner) => LogicalPlan::DerivedTable(DerivedTable {
            identifier: table.identifier.clone(),
            internal_id: table.internal_id,
            block: Box::new(take_block(&mut inner)),
            shell: Some(table),
            value_without_collation: false,
        }),
        None => LogicalPlan::Scan(Scan { table }),
    }
}

/// Take the SELECT plan out of a FROM subquery when the tree can hold it.
fn take_select_plan(subquery: &mut Arc<FromClauseSubquery>) -> Option<SelectPlan> {
    let can_be_raised = match subquery.plan.as_ref() {
        Plan::Select(inner) => select_plan_can_be_raised(inner),
        Plan::CompoundSelect { .. } | Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
            false
        }
    };
    if !can_be_raised {
        return None;
    }
    match Arc::get_mut(subquery) {
        Some(subquery) => {
            let placeholder = Plan::Select(Box::new(placeholder_select_plan()));
            let Plan::Select(inner) = std::mem::replace(subquery.plan.as_mut(), placeholder) else {
                unreachable!("checked: the subquery plan is a SELECT")
            };
            Some(*inner)
        }
        None => {
            let Plan::Select(inner) = subquery.plan.as_ref() else {
                unreachable!("checked: the subquery plan is a SELECT")
            };
            Some((**inner).clone())
        }
    }
}
