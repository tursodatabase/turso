//! Raise a prepared `SelectPlan` into a `Block` tree.

use crate::schema::{FromClauseSubquery, Table};
use crate::sync::Arc;
use crate::translate::plan::{Distinctness, JoinedTable, Plan, SelectPlan};

use super::{
    placeholder_select_plan, Aggregate, Block, DerivedTable, Distinct, Filter, Join, Limit,
    LogicalPlan, Project, Scan, Sort,
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

    Block {
        root,
        subqueries: std::mem::take(&mut plan.non_from_clause_subqueries),
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
