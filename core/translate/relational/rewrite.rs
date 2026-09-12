use crate::Result;

use super::{JoinKind, LogicalPlan, Relation};

#[derive(Default, Debug)]
pub(crate) struct RewriteReport {
    pub applied: usize,
    pub visited: usize,
    pub exhausted: bool,
}

const MAX_VISITS: usize = 4096;

pub(crate) fn normalize(plan: &mut LogicalPlan) -> Result<RewriteReport> {
    let mut report = RewriteReport::default();
    let mut root = std::mem::replace(&mut plan.root, Relation::OneRow);
    rewrite(&mut root, plan, &mut report)?;
    plan.root = root;
    #[cfg(debug_assertions)]
    plan.validate()?;
    Ok(report)
}

fn rewrite(relation: &mut Relation, plan: &LogicalPlan, report: &mut RewriteReport) -> Result<()> {
    if report.visited == MAX_VISITS {
        report.exhausted = true;
        return Ok(());
    }
    report.visited += 1;
    let mut left_visited = false;
    if let Relation::DependentJoin {
        left, right, kind, ..
    } = relation
    {
        rewrite(left, plan, report)?;
        left_visited = true;
        let left_columns = plan.properties(left)?.outputs;
        let right_input = match right.as_ref() {
            Relation::Filter { input, predicates }
                if predicates.iter().all(|predicate| predicate.can_reorder()) =>
            {
                Some((input.as_ref(), predicates.as_slice()))
            }
            Relation::Scan(_) => Some((right.as_ref(), &[][..])),
            _ => None,
        };
        if let Some((Relation::Scan(inner_id), predicates)) = right_input {
            let all_bindings_available = predicates
                .iter()
                .flat_map(|predicate| &predicate.references)
                .all(|reference| {
                    reference.column.relation == *inner_id
                        || left_columns.contains(&reference.column)
                });
            let anti_predicates_use_inner = *kind != JoinKind::Anti
                || predicates.iter().all(|predicate| {
                    predicate
                        .references
                        .iter()
                        .any(|reference| reference.column.relation == *inner_id)
                });
            if !left_columns.is_empty()
                && all_bindings_available
                && anti_predicates_use_inner
                && can_reorder(left, plan)
            {
                let inner_id = *inner_id;
                let kind = *kind;
                let mut predicates = predicates.to_vec();
                for predicate in &mut predicates {
                    predicate.bind_outer_columns(&left_columns);
                }
                let left = std::mem::replace(left, Box::new(Relation::OneRow));
                *relation = Relation::Join {
                    left,
                    right: Box::new(Relation::Scan(inner_id)),
                    kind,
                    predicates,
                };
                report.applied += 1;
            }
        }
    }
    match relation {
        Relation::OneRow | Relation::Scan(_) | Relation::SharedRef { .. } => {}
        Relation::Filter { input, .. }
        | Relation::Project { input, .. }
        | Relation::Sort { input, .. }
        | Relation::Limit { input, .. } => rewrite(input, plan, report)?,
        Relation::Join { left, right, .. } => {
            if !left_visited {
                rewrite(left, plan, report)?;
            }
            rewrite(right, plan, report)?;
        }
        Relation::DependentJoin { right, .. } => rewrite(right, plan, report)?,
    }
    Ok(())
}

fn can_reorder(relation: &Relation, plan: &LogicalPlan) -> bool {
    match relation {
        Relation::OneRow | Relation::Scan(_) => true,
        Relation::SharedRef { input, .. } => {
            let source = plan
                .shared_inputs
                .iter()
                .find(|source| source.id == *input)
                .expect("validated shared reference");
            let Relation::Project { input, outputs } = &source.input else {
                return false;
            };
            outputs.iter().all(|output| output.expr.can_reorder()) && can_reorder(input, plan)
        }
        Relation::Filter { input, predicates } => {
            predicates.iter().all(|predicate| predicate.can_reorder()) && can_reorder(input, plan)
        }
        Relation::Join {
            left,
            right,
            predicates,
            ..
        } => {
            predicates.iter().all(|predicate| predicate.can_reorder())
                && can_reorder(left, plan)
                && can_reorder(right, plan)
        }
        Relation::Project { .. }
        | Relation::DependentJoin { .. }
        | Relation::Sort { .. }
        | Relation::Limit { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausting_the_visit_budget_keeps_the_input_plan_valid() {
        let root = one_row_joins(12);
        let mut plan = LogicalPlan {
            root,
            bindings: Vec::new(),
            shared_inputs: Vec::new(),
            outer_columns: Vec::new(),
            parameters: Vec::new(),
        };
        let report = normalize(&mut plan).unwrap();
        assert!(report.exhausted);
        assert_eq!(report.visited, MAX_VISITS);
        assert_eq!(report.applied, 0);
        plan.validate().unwrap();
        assert_eq!(node_count(&plan.root), 8191);
    }

    fn one_row_joins(depth: usize) -> Relation {
        if depth == 0 {
            return Relation::OneRow;
        }
        Relation::Join {
            left: Box::new(one_row_joins(depth - 1)),
            right: Box::new(one_row_joins(depth - 1)),
            kind: JoinKind::Inner,
            predicates: Vec::new(),
        }
    }

    fn node_count(relation: &Relation) -> usize {
        match relation {
            Relation::OneRow => 1,
            Relation::Join { left, right, .. } => 1 + node_count(left) + node_count(right),
            _ => panic!("budget test only builds one-row joins"),
        }
    }
}
