use crate::Result;

use super::{JoinKind, LogicalPlan, Output, Relation, Scalar, Scope};

mod generated {
    use super::*;
    include!(concat!(env!("OUT_DIR"), "/logical_rules.rs"));
}

#[cfg(test)]
#[path = "rules/compiler.rs"]
mod compiler;

#[derive(Default, Debug)]
pub(crate) struct RewriteReport {
    pub applied: usize,
    pub visited: usize,
    pub exhausted: bool,
    rule_counts: [usize; generated::RULE_COUNT],
}

impl RewriteReport {
    pub(crate) fn rules(&self) -> impl Iterator<Item = (&'static str, usize)> + '_ {
        generated::RULES
            .iter()
            .map(|rule| (rule.name(), self.rule_counts[*rule as usize]))
    }

    pub(crate) fn dependent_filters_pulled(&self) -> usize {
        self.rule_counts[generated::Rule::PullDependentFilter as usize]
    }

    fn record(&mut self, rule: generated::Rule) {
        self.applied += 1;
        self.rule_counts[rule as usize] += 1;
        tracing::trace!(target: "logical_optimizer", rule = rule.name(), "applied logical rule");
    }
}

const MAX_VISITS: usize = 4096;
const MAX_REWRITES: usize = 4096;

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
    if report.visited == MAX_VISITS || report.applied == MAX_REWRITES {
        report.exhausted = true;
        return Ok(());
    }
    report.visited += 1;
    let mut left_visited = false;
    if let Relation::DependentJoin { left, .. } = relation {
        rewrite(left, plan, report)?;
        left_visited = true;
        if !report.exhausted {
            if let Some(rule) = generated::apply_explore(relation, plan)? {
                report.record(rule);
            }
        }
    }
    match relation {
        Relation::OneRow | Relation::Scan(_) | Relation::SharedRef { .. } => {}
        Relation::Filter { input, .. }
        | Relation::Subquery { input, .. }
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
    normalize_node(relation, plan, report)
}

fn normalize_node(
    relation: &mut Relation,
    plan: &LogicalPlan,
    report: &mut RewriteReport,
) -> Result<()> {
    while !report.exhausted {
        if report.applied == MAX_REWRITES {
            report.exhausted = true;
            break;
        }
        let Some(rule) = generated::apply_normalize(relation, plan)? else {
            break;
        };
        report.record(rule);
        if rule == generated::Rule::PushSelectIntoProject {
            let Relation::Project { input, .. } = relation else {
                unreachable!("filter pushdown preserves projection")
            };
            normalize_node(input, plan, report)?;
        }
    }
    Ok(())
}

fn empty_predicates(predicates: &[Scalar], _: &LogicalPlan) -> Result<bool> {
    Ok(predicates.is_empty())
}

fn pure_predicates(predicates: &[Scalar], _: &LogicalPlan) -> Result<bool> {
    Ok(predicates.iter().all(Scalar::can_reorder))
}

fn reorderable_input(input: &Relation, plan: &LogicalPlan) -> Result<bool> {
    Ok(can_reorder(input, plan))
}

fn inner_join(kind: &JoinKind, _: &LogicalPlan) -> Result<bool> {
    Ok(*kind == JoinKind::Inner)
}

fn identity_projection(input: &Relation, outputs: &[Output], plan: &LogicalPlan) -> Result<bool> {
    if outputs.iter().any(|output| {
        output.expr.as_column() != Some(output.column.id)
            || !output.expr.can_reorder()
            || output.alias.is_some()
            || output.implicit_name.is_some()
    }) {
        return Ok(false);
    }
    let columns = plan.output_columns(input)?;
    if columns
        .iter()
        .copied()
        .ne(outputs.iter().map(|output| output.column.id))
    {
        return Ok(false);
    }
    Ok(outputs.iter().all(|output| {
        plan.bindings
            .iter()
            .find(|binding| binding.id == output.column.id.relation)
            .is_some_and(|binding| binding.column(output.column.id) == output.column)
    }))
}

fn passthrough_projection(
    outputs: &[Output],
    predicates: &[Scalar],
    _: &LogicalPlan,
) -> Result<bool> {
    Ok(outputs.iter().all(|output| {
        output.expr.can_reorder()
            && output.expr.as_column().is_some()
            && output.expr.references[0].scope == Scope::Local
    }) && predicates.iter().all(|predicate| {
        predicate.can_reorder()
            && predicate.references.iter().all(|reference| {
                reference.scope != Scope::Local
                    || outputs
                        .iter()
                        .any(|output| output.column.id == reference.column)
            })
    }))
}

fn can_pull_dependent_filter(
    left: &Relation,
    right: &Relation,
    kind: &JoinKind,
    plan: &LogicalPlan,
) -> Result<bool> {
    let (inner, predicates) = match right {
        Relation::Filter { input, predicates } if predicates.iter().all(Scalar::can_reorder) => {
            (input.as_ref(), predicates.as_slice())
        }
        Relation::Scan(_) => (right, &[][..]),
        _ => return Ok(false),
    };
    let Relation::Scan(inner_id) = inner else {
        return Ok(false);
    };
    let left_columns = plan.properties(left)?.outputs;
    let available = predicates
        .iter()
        .flat_map(|predicate| &predicate.references)
        .all(|reference| {
            reference.column.relation == *inner_id || left_columns.contains(&reference.column)
        });
    let anti_uses_inner = *kind != JoinKind::Anti
        || predicates.iter().all(|predicate| {
            predicate
                .references
                .iter()
                .any(|reference| reference.column.relation == *inner_id)
        });
    Ok(!left_columns.is_empty() && available && anti_uses_inner && can_reorder(left, plan))
}

fn concat_predicates(
    mut inner: Vec<Scalar>,
    outer: Vec<Scalar>,
    _: &LogicalPlan,
) -> Result<Vec<Scalar>> {
    inner.extend(outer);
    Ok(inner)
}

fn push_filter_into_project(
    input: Relation,
    outputs: Vec<Output>,
    mut predicates: Vec<Scalar>,
    _: &LogicalPlan,
) -> Result<Relation> {
    for predicate in &mut predicates {
        predicate.substitute_project_columns(&outputs)?;
    }
    Ok(Relation::Project {
        input: Box::new(Relation::Filter {
            input: Box::new(input),
            predicates,
        }),
        outputs,
    })
}

fn pull_dependent_filter(
    left: Relation,
    right: Relation,
    kind: JoinKind,
    _: &LogicalPlan,
) -> Result<Relation> {
    let (right, mut predicates) = match right {
        Relation::Filter { input, predicates } => (input, predicates),
        scan @ Relation::Scan(_) => (Box::new(scan), Vec::new()),
        _ => unreachable!("dependent filter precondition checked the right input"),
    };
    for predicate in &mut predicates {
        predicate.bind_all_local();
    }
    Ok(Relation::Join {
        left: Box::new(left),
        right,
        kind,
        predicates,
    })
}

fn can_reorder(relation: &Relation, plan: &LogicalPlan) -> bool {
    match relation {
        Relation::OneRow | Relation::Scan(_) => true,
        Relation::Subquery { input, .. } => reorderable_projection(input, plan),
        Relation::SharedRef { input, .. } => {
            let source = plan
                .shared_inputs
                .iter()
                .find(|source| source.id == *input)
                .expect("validated shared reference");
            reorderable_projection(&source.input, plan)
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

fn reorderable_projection(relation: &Relation, plan: &LogicalPlan) -> bool {
    let Relation::Project { input, outputs } = relation else {
        return false;
    };
    outputs.iter().all(|output| output.expr.can_reorder()) && can_reorder(input, plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausting_the_rule_budget_keeps_the_last_valid_plan() {
        let mut plan = LogicalPlan {
            root: Relation::Filter {
                input: Box::new(Relation::OneRow),
                predicates: Vec::new(),
            },
            bindings: Vec::new(),
            shared_inputs: Vec::new(),
            outer_columns: Vec::new(),
            parameters: Vec::new(),
        };
        let mut report = RewriteReport {
            applied: MAX_REWRITES,
            ..RewriteReport::default()
        };
        let mut root = std::mem::replace(&mut plan.root, Relation::OneRow);
        normalize_node(&mut root, &plan, &mut report).unwrap();
        plan.root = root;
        plan.validate().unwrap();
        assert!(report.exhausted);
        assert!(matches!(plan.root, Relation::Filter { .. }));
    }

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
