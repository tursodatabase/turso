use super::{query_column, BindError, Builder, Column, ColumnId, Relation, Scalar, SharedInput};
use crate::translate::collate::CollationSeq;
use crate::translate::plan::{Plan, SelectPlan, SubqueryState};
use crate::util::parse_signed_number;
use crate::{Numeric, Value};
use turso_parser::ast::{Expr, SubqueryType, TableInternalId};

pub(super) struct ScalarInput {
    pub(super) id: TableInternalId,
    pub(super) query: Relation,
    pub(super) column: Box<Column>,
}

pub(super) fn inputs(
    builder: &mut Builder<'_, '_>,
    plan: &SelectPlan,
) -> std::result::Result<Vec<ScalarInput>, BindError> {
    let mut inputs = Vec::new();
    for subquery in &plan.non_from_clause_subqueries {
        let SubqueryType::RowValue { num_regs, .. } = &subquery.query_type else {
            continue;
        };
        if *num_regs != 1 {
            return Err(BindError::Unsupported("row-valued projection lowering"));
        }
        if !plan
            .result_columns
            .iter()
            .any(|output| result_id(&output.expr) == Some(subquery.internal_id))
        {
            return Err(BindError::Unsupported(
                "scalar result outside a direct projection",
            ));
        }
        let SubqueryState::Unevaluated { plan: Some(inner) } = &subquery.state else {
            return Err(BindError::Unsupported("already emitted scalar query"));
        };
        let Plan::Select(inner) = inner.as_ref() else {
            return Err(BindError::Unsupported("compound scalar result lowering"));
        };
        let query = builder.select(inner, false)?;
        if !can_repeat(&query, &builder.shared_inputs) {
            return Err(BindError::Unsupported("scalar query evaluation effects"));
        }
        let mut column = Box::new(query_column(&query, 0).clone());
        column.id = ColumnId {
            relation: subquery.internal_id,
            position: Some(0),
        };
        column.nullable = true;
        column.collation = CollationSeq::Unset;
        inputs.push(ScalarInput {
            id: subquery.internal_id,
            query,
            column,
        });
    }
    Ok(inputs)
}

pub(super) fn result_id(expr: &Expr) -> Option<TableInternalId> {
    match expr {
        Expr::SubqueryResult {
            subquery_id,
            lhs: None,
            not_in: false,
            query_type: SubqueryType::RowValue { .. },
        } => Some(*subquery_id),
        _ => None,
    }
}

pub(super) fn can_repeat(query: &Relation, shared: &[SharedInput]) -> bool {
    match query {
        Relation::OneRow | Relation::Scan(_) => true,
        Relation::Values(values) => values.rows.iter().flatten().all(Scalar::can_reorder),
        Relation::SharedRef { input, .. } => {
            let input = shared
                .iter()
                .find(|source| source.id == *input)
                .expect("bound shared reference has a producer");
            can_repeat(&input.input, shared)
        }
        Relation::Subquery { input, .. } | Relation::Distinct { input } => {
            can_repeat(input, shared)
        }
        Relation::Project { input, outputs } => {
            outputs.iter().all(|output| output.expr.can_reorder()) && can_repeat(input, shared)
        }
        Relation::Filter { input, predicates } => {
            predicates.iter().all(Scalar::can_reorder) && can_repeat(input, shared)
        }
        Relation::Join {
            left,
            right,
            predicates,
            ..
        } => {
            predicates.iter().all(Scalar::can_reorder)
                && can_repeat(left, shared)
                && can_repeat(right, shared)
        }
        Relation::Sort { input, keys } => {
            keys.iter().all(|(expr, _, _)| expr.can_reorder()) && can_repeat(input, shared)
        }
        Relation::Limit {
            input,
            limit,
            offset,
        } => {
            limit.iter().chain(offset.iter()).all(|expr| {
                matches!(
                    parse_signed_number(expr.ast()),
                    Ok(Value::Numeric(Numeric::Integer(_)))
                )
            }) && can_repeat(input, shared)
        }
        Relation::Aggregate { .. }
        | Relation::Set { .. }
        | Relation::DependentJoin { .. }
        | Relation::MarkJoin { .. }
        | Relation::ScalarJoin { .. }
        | Relation::Membership { .. } => false,
    }
}
