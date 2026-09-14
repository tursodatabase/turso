use turso_parser::ast::TableInternalId;

use crate::translate::collate::CollationSeq;
use crate::vdbe::affinity::Affinity;
use crate::Result;

use super::{require, validate_scalar, Column, LogicalPlan, Properties, Relation, Scalar};

#[derive(Clone, Debug)]
pub(crate) enum MarkKind {
    Exists { negated: bool },
    Membership { lhs: Vec<Scalar>, negated: bool },
}

pub(super) fn properties(
    plan: &LogicalPlan,
    left: &Relation,
    right: &Relation,
    subquery: TableInternalId,
    column: &Column,
    kind: &MarkKind,
) -> Result<Properties> {
    let mut left = plan.properties(left)?;
    let right = plan.properties(right)?;
    require(
        left.outputs.is_disjoint(&right.outputs),
        "mark join inputs share column identities",
    )?;
    require(
        column.id.relation == subquery
            && column.id.position == Some(0)
            && column.nullable == matches!(kind, MarkKind::Membership { .. })
            && column.affinity == Affinity::None
            && column.collation == CollationSeq::Unset,
        "mark result has invalid identity or comparison properties",
    )?;
    if let MarkKind::Membership { lhs, .. } = kind {
        require(!lhs.is_empty(), "membership mark has no comparison columns")?;
        require(
            lhs.len() == right.outputs.len(),
            "membership mark inputs have different column counts",
        )?;
        for expr in lhs {
            validate_scalar(expr, &mut left, None)?;
        }
    }
    require(
        left.outer.is_disjoint(&right.outputs),
        "mark join has a reverse dependency",
    )?;
    require(
        !left.outputs.contains(&column.id)
            && !right.outputs.contains(&column.id)
            && !left.outer.contains(&column.id)
            && !right.outer.contains(&column.id),
        "mark result reuses an input identity",
    )?;
    left.outer
        .union_with(right.outer.difference(&left.outputs))?;
    left.outputs.insert(column.id)?;
    Ok(left)
}
