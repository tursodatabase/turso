use turso_parser::ast::TableInternalId;

use crate::Result;

use super::{
    binding::{query_column, query_output_ids},
    rewrite, Binding, BindingColumns, JoinKind, LogicalPlan, Relation, Scalar,
};

pub(super) fn can_unnest(
    left: &Relation,
    right: &Relation,
    lhs: &[Scalar],
    plan: &LogicalPlan,
) -> Result<bool> {
    Ok(decline(left, right, lhs, plan)?.is_none())
}

pub(super) fn decline(
    left: &Relation,
    right: &Relation,
    lhs: &[Scalar],
    plan: &LogicalPlan,
) -> Result<Option<&'static str>> {
    if plan.properties(left)?.outputs.is_empty() {
        return Ok(Some("membership join requires a left table"));
    }
    if lhs.iter().any(|expr| !expr.can_reorder()) {
        return Ok(Some("membership comparison can change evaluation behavior"));
    }
    if !rewrite::can_reorder(left, plan) {
        return Ok(Some("membership left input cannot move"));
    }
    if !plan.properties(right)?.outer.is_empty() {
        return Ok(Some("membership input still depends on an outer row"));
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
    let predicates = lhs
        .into_iter()
        .zip(&binding_columns)
        .map(|(left, column)| {
            let right = Scalar::result_column(column, None, column.collation);
            left.membership_comparison(right, negated)
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
