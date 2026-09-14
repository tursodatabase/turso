use turso_parser::ast::TableInternalId;

use crate::Result;

use super::{
    binding::{query_column, query_output_ids},
    rewrite, Binding, BindingColumns, ColumnSet, JoinKind, LogicalPlan, Relation, Scalar,
};

pub(super) fn can_unnest(
    left: &Relation,
    right: &Relation,
    lhs: &[Scalar],
    negated: &bool,
    plan: &LogicalPlan,
) -> Result<bool> {
    Ok(decline(left, right, lhs, *negated, plan)?.is_none())
}

pub(super) fn decline(
    left: &Relation,
    right: &Relation,
    lhs: &[Scalar],
    negated: bool,
    plan: &LogicalPlan,
) -> Result<Option<&'static str>> {
    let left_columns = plan.properties(left)?.outputs;
    if left_columns.is_empty() {
        return Ok(Some("membership join requires a left table"));
    }
    if lhs.iter().any(|expr| !expr.can_reorder()) {
        return Ok(Some("membership comparison can change evaluation behavior"));
    }
    if !rewrite::can_reorder(left, plan) {
        return Ok(Some("membership left input cannot move"));
    }
    if !plan.properties(right)?.outer.is_empty() {
        return correlated_projection_decline(right, &left_columns, negated, plan);
    }
    if !rewrite::reorderable_projection(right, plan) {
        return Ok(Some(
            "membership input requires ordered or effectful evaluation",
        ));
    }
    Ok(None)
}

pub(super) fn unnest(
    left: Relation,
    right: Relation,
    lhs: Vec<Scalar>,
    negated: bool,
    subquery: TableInternalId,
    plan: &mut LogicalPlan,
) -> Result<Relation> {
    if can_join_scan(&right, negated) {
        return Ok(join_scan(left, right, lhs, negated));
    }
    if matches!(&right, Relation::Project { outputs, .. } if outputs.iter().any(|output| {
        output.expr.references.iter().any(|reference| matches!(reference.scope, super::Scope::Outer(_)))
    })) {
        return join_projected_input(left, right, lhs, negated, subquery, plan);
    }
    let (right, correlated) = if plan.properties(&right)?.outer.is_empty() {
        (right, Vec::new())
    } else {
        pull_correlated_filter(right, subquery, plan)?
    };
    let columns = query_output_ids(&right);
    let binding_columns: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(position, _)| {
            let mut column = query_column(&right, position).clone();
            column.id.relation = subquery;
            column.id.position = Some(position);
            column
        })
        .collect();
    let mut predicates: Vec<_> = lhs
        .into_iter()
        .zip(&binding_columns)
        .map(|(left, column)| {
            let right = Scalar::result_column(column, None, column.collation);
            left.membership_comparison(right, negated)
        })
        .collect();
    predicates.extend(correlated);
    plan.bindings.push(Binding {
        id: subquery,
        name: format!("membership_input_{subquery}"),
        columns: BindingColumns::Derived(binding_columns),
    });
    Ok(Relation::Join {
        left: Box::new(left),
        right: Box::new(Relation::Subquery {
            binding: subquery,
            input: Box::new(right),
            columns,
        }),
        kind: if negated {
            JoinKind::Anti
        } else {
            JoinKind::Semi
        },
        predicates,
    })
}

fn join_projected_input(
    left: Relation,
    right: Relation,
    lhs: Vec<Scalar>,
    negated: bool,
    subquery: TableInternalId,
    plan: &mut LogicalPlan,
) -> Result<Relation> {
    let Relation::Project { input, outputs } = right else {
        unreachable!("correlated membership has a projection")
    };
    let (mut input, filters) = match *input {
        Relation::Filter { input, predicates } => (input, predicates),
        input => (Box::new(input), Vec::new()),
    };
    let inner_columns = plan.properties(&input)?.outputs;
    let (local, correlated): (Vec<_>, Vec<_>) = filters.into_iter().partition(|predicate| {
        predicate
            .references
            .iter()
            .all(|reference| inner_columns.contains(&reference.column))
    });
    if !local.is_empty() {
        input = Box::new(Relation::Filter {
            input,
            predicates: local,
        });
    }
    let mut projected = Vec::new();
    let mut predicates = Vec::new();
    for (left, output) in lhs.into_iter().zip(outputs) {
        let mut right = output.expr;
        right.project_input_columns(&inner_columns, subquery, &plan.bindings, &mut projected)?;
        right.bind_all_local();
        predicates.push(left.membership_comparison(right, negated));
    }
    for mut predicate in correlated {
        predicate.project_input_columns(
            &inner_columns,
            subquery,
            &plan.bindings,
            &mut projected,
        )?;
        predicate.bind_all_local();
        predicates.push(predicate);
    }
    assert!(
        !projected.is_empty(),
        "correlated membership needs an inner column to project"
    );
    let columns = projected.iter().map(|output| output.column.id).collect();
    let binding_columns = projected
        .iter()
        .enumerate()
        .map(|(position, output)| {
            let mut column = output.column.clone();
            column.id.relation = subquery;
            column.id.position = Some(position);
            column
        })
        .collect();
    plan.bindings.push(Binding {
        id: subquery,
        name: format!("membership_input_{subquery}"),
        columns: BindingColumns::Derived(binding_columns),
    });
    Ok(Relation::Join {
        left: Box::new(left),
        right: Box::new(Relation::Subquery {
            binding: subquery,
            input: Box::new(Relation::Project {
                input,
                outputs: projected,
            }),
            columns,
        }),
        kind: if negated {
            JoinKind::Anti
        } else {
            JoinKind::Semi
        },
        predicates,
    })
}

fn can_join_scan(right: &Relation, negated: bool) -> bool {
    let Relation::Project { input, outputs } = right else {
        return false;
    };
    let (input, predicates) = match input.as_ref() {
        Relation::Filter { input, predicates } => (input.as_ref(), predicates.as_slice()),
        input => (input, &[][..]),
    };
    let Relation::Scan(id) = input else {
        return false;
    };
    !negated
        || outputs
            .iter()
            .map(|output| &output.expr)
            .chain(predicates)
            .all(|expr| {
                expr.references
                    .iter()
                    .any(|reference| reference.column.relation == *id)
            })
}

fn join_scan(left: Relation, right: Relation, lhs: Vec<Scalar>, negated: bool) -> Relation {
    let Relation::Project { input, outputs } = right else {
        unreachable!("membership scan has a projection")
    };
    let (right, mut filters) = match *input {
        Relation::Filter { input, predicates } => (*input, predicates),
        input @ Relation::Scan(_) => (input, Vec::new()),
        _ => unreachable!("membership projection reads a scan"),
    };
    for filter in &mut filters {
        filter.bind_all_local();
    }
    let mut predicates: Vec<_> = lhs
        .into_iter()
        .zip(outputs)
        .map(|(left, mut output)| {
            output.expr.bind_all_local();
            left.membership_comparison(output.expr, negated)
        })
        .collect();
    predicates.extend(filters);
    Relation::Join {
        left: Box::new(left),
        right: Box::new(right),
        kind: if negated {
            JoinKind::Anti
        } else {
            JoinKind::Semi
        },
        predicates,
    }
}

fn correlated_projection_decline(
    right: &Relation,
    left_columns: &ColumnSet,
    negated: bool,
    plan: &LogicalPlan,
) -> Result<Option<&'static str>> {
    let Relation::Project { input, outputs } = right else {
        return Ok(Some("correlated membership requires a projection"));
    };
    let (input, predicates) = match input.as_ref() {
        Relation::Filter { input, predicates } => (input.as_ref(), predicates.as_slice()),
        input => (input, &[][..]),
    };
    let inner = plan.properties(input)?;
    if !inner.outer.is_empty() {
        return Ok(Some("membership input still depends on an outer row"));
    }
    if !rewrite::can_reorder(input, plan) {
        return Ok(Some("correlated membership input cannot move"));
    }
    if outputs
        .iter()
        .any(|output| inner.outputs.contains(&output.column.id))
    {
        return Ok(Some("membership output reuses an input column identity"));
    }
    let mut has_outer_output = false;
    for output in outputs {
        if !output.expr.can_reorder() {
            return Ok(Some("correlated membership output cannot move"));
        }
        for reference in &output.expr.references {
            if inner.outputs.contains(&reference.column) {
                continue;
            }
            if !left_columns.contains(&reference.column) {
                return Ok(Some("membership output column is unavailable"));
            }
            has_outer_output = true;
        }
    }
    let needs_column_mapping = has_outer_output && !can_join_scan(right, negated);
    let mut projects_inner_column = !needs_column_mapping
        || outputs.iter().any(|output| {
            output
                .expr
                .references
                .iter()
                .any(|reference| inner.outputs.contains(&reference.column))
        });
    if needs_column_mapping
        && negated
        && outputs.iter().any(|output| {
            !output
                .expr
                .references
                .iter()
                .any(|reference| inner.outputs.contains(&reference.column))
        })
    {
        return Ok(Some("NOT IN output must read the inner input"));
    }
    for predicate in predicates {
        if !predicate.can_reorder() {
            return Ok(Some("membership filter can change evaluation behavior"));
        }
        let mut uses_inner = false;
        let mut uses_outer = false;
        for reference in &predicate.references {
            if inner.outputs.contains(&reference.column) {
                uses_inner = true;
            } else if left_columns.contains(&reference.column) {
                uses_outer = true;
            } else {
                return Ok(Some("membership correlation column is unavailable"));
            }
        }
        if negated && uses_outer && !uses_inner {
            return Ok(Some("NOT IN correlation must read the inner input"));
        }
        projects_inner_column |= uses_outer && uses_inner;
    }
    if !projects_inner_column {
        return Ok(Some("membership has no inner column to project"));
    }
    Ok(None)
}

fn pull_correlated_filter(
    right: Relation,
    subquery: TableInternalId,
    plan: &LogicalPlan,
) -> Result<(Relation, Vec<Scalar>)> {
    let Relation::Project { input, mut outputs } = right else {
        unreachable!("correlated membership has a projection")
    };
    let Relation::Filter {
        mut input,
        predicates,
    } = *input
    else {
        unreachable!("correlated membership has a filter")
    };
    let inner_columns = plan.properties(&input)?.outputs;
    let (local, mut correlated): (Vec<_>, Vec<_>) = predicates.into_iter().partition(|predicate| {
        predicate
            .references
            .iter()
            .all(|reference| inner_columns.contains(&reference.column))
    });
    if !local.is_empty() {
        input = Box::new(Relation::Filter {
            input,
            predicates: local,
        });
    }
    for predicate in &mut correlated {
        predicate.project_input_columns(&inner_columns, subquery, &plan.bindings, &mut outputs)?;
        predicate.bind_all_local();
    }
    Ok((Relation::Project { input, outputs }, correlated))
}
