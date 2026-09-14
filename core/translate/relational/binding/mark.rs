use turso_parser::ast::{self, Expr, TableInternalId};

use crate::translate::collate::CollationSeq;
use crate::translate::plan::{Plan, SelectPlan, SubqueryState};
use crate::vdbe::affinity::Affinity;

use super::{membership_lhs, scalar, BindError, Builder, Column, ColumnId, Relation, Scalar};
use crate::translate::relational::MarkKind;

pub(super) struct MarkInput {
    pub(super) id: TableInternalId,
    pub(super) query: Relation,
    pub(super) kind: Box<MarkKind>,
    pub(super) column: Box<Column>,
}

pub(super) fn inputs(
    builder: &mut Builder<'_, '_>,
    plan: &SelectPlan,
    exists: bool,
) -> std::result::Result<Vec<MarkInput>, BindError> {
    let mut inputs: Vec<MarkInput> = Vec::new();
    for output in &plan.result_columns {
        let Some((id, lhs, negated)) = result(&output.expr) else {
            continue;
        };
        if exists || plan.group_by.is_some() || !plan.aggregates.is_empty() {
            return Err(BindError::Unsupported(
                "marked results across grouping or EXISTS",
            ));
        }
        if inputs.iter().any(|input| input.id == id) {
            continue;
        }
        let subquery = plan
            .non_from_clause_subqueries
            .iter()
            .find(|subquery| subquery.internal_id == id)
            .ok_or_else(|| super::super::invalid("mark result has no subquery"))?;
        let SubqueryState::Unevaluated { plan: Some(inner) } = &subquery.state else {
            return Err(BindError::Unsupported("already emitted mark query"));
        };
        let (query, kind) = if let Some(lhs) = lhs {
            let lhs = membership_lhs(lhs, &plan.table_references, builder.resolver, inner)?;
            if !lhs.iter().all(Scalar::can_reorder) {
                return Err(BindError::Unsupported(
                    "membership result evaluation effects",
                ));
            }
            (builder.query(inner)?, MarkKind::Membership { lhs, negated })
        } else {
            let Plan::Select(inner) = inner.as_ref() else {
                return Err(BindError::Unsupported("compound EXISTS result lowering"));
            };
            (builder.select(inner, true)?, MarkKind::Exists { negated })
        };
        if !scalar::can_repeat(&query, &builder.shared_inputs) {
            return Err(BindError::Unsupported("mark query evaluation constraints"));
        }
        let column = Box::new(Column {
            id: ColumnId {
                relation: id,
                position: Some(0),
            },
            name: format!("subquery_{id}"),
            nullable: matches!(kind, MarkKind::Membership { .. }),
            affinity: Affinity::None,
            collation: CollationSeq::Unset,
        });
        inputs.push(MarkInput {
            id,
            query,
            kind: Box::new(kind),
            column,
        });
    }
    Ok(inputs)
}

pub(super) fn result_id(expr: &Expr) -> Option<TableInternalId> {
    result(expr).map(|(id, _, _)| id)
}

fn result(expr: &Expr) -> Option<(TableInternalId, Option<&Expr>, bool)> {
    match expr {
        Expr::SubqueryResult {
            subquery_id,
            lhs: None,
            not_in: false,
            query_type: ast::SubqueryType::Exists { .. },
        } => Some((*subquery_id, None, false)),
        Expr::Unary(ast::UnaryOperator::Not, expr) => match expr.as_ref() {
            Expr::SubqueryResult {
                subquery_id,
                lhs: None,
                not_in: false,
                query_type: ast::SubqueryType::Exists { .. },
            } => Some((*subquery_id, None, true)),
            _ => None,
        },
        Expr::SubqueryResult {
            subquery_id,
            lhs: Some(lhs),
            not_in,
            query_type: ast::SubqueryType::In { .. },
        } => Some((*subquery_id, Some(lhs.as_ref()), *not_in)),
        _ => None,
    }
}
