use crate::Result;

use super::membership::{can_unnest as can_unnest_membership, unnest as unnest_membership};
use super::predicates::{
    deduplicate as deduplicate_predicates, has_duplicates as duplicate_predicates,
};
use super::{Binding, BindingColumns, JoinKind, LogicalPlan, Output, Relation, Scalar, Scope};

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
    pub added_nodes: usize,
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
            + self.rule_counts[generated::Rule::PullDependentFilterOverJoin as usize]
    }

    fn record(&mut self, rule: generated::Rule) {
        self.applied += 1;
        self.rule_counts[rule as usize] += 1;
        tracing::trace!(target: "logical_optimizer", rule = rule.name(), "applied logical rule");
    }

    fn reserve_growth(&mut self, nodes: usize) -> bool {
        if nodes > MAX_ADDED_NODES - self.added_nodes {
            self.exhausted = true;
            return false;
        }
        self.added_nodes += nodes;
        true
    }
}

const MAX_VISITS: usize = 4096;
const MAX_REWRITES: usize = 4096;
const MAX_ADDED_NODES: usize = 4096;

pub(crate) fn normalize(plan: &mut LogicalPlan) -> Result<RewriteReport> {
    let mut report = RewriteReport::default();
    for index in 0..plan.shared_inputs.len() {
        let mut input = std::mem::replace(&mut plan.shared_inputs[index].input, Relation::OneRow);
        rewrite(&mut input, plan, &mut report)?;
        plan.shared_inputs[index].input = input;
    }
    let mut root = std::mem::replace(&mut plan.root, Relation::OneRow);
    rewrite(&mut root, plan, &mut report)?;
    plan.root = root;
    #[cfg(debug_assertions)]
    plan.validate()?;
    Ok(report)
}

pub(super) fn normalization_declines(
    relation: &Relation,
    plan: &LogicalPlan,
    declined: impl FnMut(&'static str, &'static str),
) -> Result<()> {
    generated::normalization_declines(relation, plan, declined)
}

fn rewrite(
    relation: &mut Relation,
    plan: &mut LogicalPlan,
    report: &mut RewriteReport,
) -> Result<()> {
    if report.exhausted || report.visited == MAX_VISITS || report.applied == MAX_REWRITES {
        report.exhausted = true;
        return Ok(());
    }
    report.visited += 1;
    let mut left_visited = false;
    if let Relation::DependentJoin { left, .. } | Relation::Membership { left, .. } = relation {
        rewrite(left, plan, report)?;
        left_visited = true;
        if !report.exhausted {
            if let Some(rule) = generated::apply_explore(relation, plan, report)? {
                report.record(rule);
            }
        }
    }
    match relation {
        Relation::OneRow | Relation::Values(_) | Relation::Scan(_) | Relation::SharedRef { .. } => {
        }
        Relation::Filter { input, .. }
        | Relation::Subquery { input, .. }
        | Relation::Project { input, .. }
        | Relation::Distinct { input }
        | Relation::Aggregate { input, .. }
        | Relation::Sort { input, .. }
        | Relation::Limit { input, .. } => rewrite(input, plan, report)?,
        Relation::Join { left, right, .. }
        | Relation::Set { left, right, .. }
        | Relation::ScalarJoin { left, right, .. } => {
            if !left_visited {
                rewrite(left, plan, report)?;
            }
            rewrite(right, plan, report)?;
        }
        Relation::DependentJoin { right, .. } | Relation::Membership { right, .. } => {
            let applied_before = report.applied;
            rewrite(right, plan, report)?;
            if !report.exhausted && report.applied != applied_before {
                if let Some(rule) = generated::apply_explore(relation, plan, report)? {
                    report.record(rule);
                }
            }
        }
    }
    normalize_node(relation, plan, report)
}

fn normalize_node(
    relation: &mut Relation,
    plan: &mut LogicalPlan,
    report: &mut RewriteReport,
) -> Result<()> {
    while !report.exhausted {
        if report.applied == MAX_REWRITES {
            report.exhausted = true;
            break;
        }
        let Some(rule) = generated::apply_normalize(relation, plan, report)? else {
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

fn existence_join(kind: &JoinKind, _: &LogicalPlan) -> Result<bool> {
    Ok(matches!(kind, JoinKind::Semi | JoinKind::Anti))
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
    let reason = dependent_filter_decline(left, right, kind, plan)?;
    if let Some(reason) = reason {
        tracing::trace!(target: "logical_optimizer", rule = generated::Rule::PullDependentFilter.name(), reason, "declined logical rule");
    }
    Ok(reason.is_none())
}

fn can_pull_filter_over_join(
    left: &Relation,
    input: &Relation,
    predicates: &[Scalar],
    kind: &JoinKind,
    plan: &LogicalPlan,
) -> Result<bool> {
    let reason = joined_filter_decline(left, input, predicates, kind, plan)?;
    if let Some(reason) = reason {
        tracing::trace!(target: "logical_optimizer", rule = generated::Rule::PullDependentFilterOverJoin.name(), reason, "declined logical rule");
    }
    Ok(reason.is_none())
}

pub(super) fn dependent_filter_rules(
    left: &Relation,
    right: &Relation,
    kind: &JoinKind,
    plan: &LogicalPlan,
) -> Result<[(&'static str, Option<&'static str>); 2]> {
    let joined = match right {
        Relation::Filter { input, predicates } => {
            joined_filter_decline(left, input, predicates, kind, plan)?
        }
        _ => Some("right_input_shape"),
    };
    Ok([
        (
            generated::Rule::PullDependentFilter.name(),
            dependent_filter_decline(left, right, kind, plan)?,
        ),
        (generated::Rule::PullDependentFilterOverJoin.name(), joined),
    ])
}

fn dependent_filter_decline(
    left: &Relation,
    right: &Relation,
    kind: &JoinKind,
    plan: &LogicalPlan,
) -> Result<Option<&'static str>> {
    let (inner, predicates) = match right {
        Relation::Filter { input, predicates } => {
            if !predicates.iter().all(Scalar::can_reorder) {
                return Ok(Some("predicate_effects"));
            }
            (input.as_ref(), predicates.as_slice())
        }
        Relation::Scan(_) | Relation::SharedRef { .. } | Relation::Subquery { .. } => {
            (right, &[][..])
        }
        _ => return Ok(Some("right_input_shape")),
    };
    let inner_id = match inner {
        Relation::Scan(id) | Relation::SharedRef { binding: id, .. } => *id,
        Relation::Subquery { binding, .. } => {
            if !plan.properties(inner)?.outer.is_empty() {
                return Ok(Some("right_input_dependency"));
            }
            *binding
        }
        _ => return Ok(Some("right_input_shape")),
    };
    let left_columns = plan.properties(left)?.outputs;
    if left_columns.is_empty() {
        return Ok(Some("missing_left_columns"));
    }
    if !predicates
        .iter()
        .flat_map(|predicate| &predicate.references)
        .all(|reference| {
            reference.column.relation == inner_id || left_columns.contains(&reference.column)
        })
    {
        return Ok(Some("unavailable_columns"));
    }
    if *kind == JoinKind::Anti
        && !predicates.iter().all(|predicate| {
            predicate
                .references
                .iter()
                .any(|reference| reference.column.relation == inner_id)
        })
    {
        return Ok(Some("anti_predicate_placement"));
    }
    if !can_reorder(left, plan) {
        return Ok(Some("left_input_evaluation"));
    }
    if !can_reorder(inner, plan) {
        return Ok(Some("right_input_evaluation"));
    }
    Ok(None)
}

fn joined_filter_decline(
    left: &Relation,
    input: &Relation,
    predicates: &[Scalar],
    kind: &JoinKind,
    plan: &LogicalPlan,
) -> Result<Option<&'static str>> {
    if !matches!(input, Relation::Join { .. }) {
        return Ok(Some("right_input_shape"));
    }
    if !predicates.iter().all(Scalar::can_reorder) {
        return Ok(Some("predicate_effects"));
    }
    if !can_reorder(left, plan) {
        return Ok(Some("left_input_evaluation"));
    }
    if !can_reorder(input, plan) {
        return Ok(Some("right_input_evaluation"));
    }
    let inner = plan.properties(input)?;
    let outer = plan.properties(left)?.outputs;
    if !inner.outer.is_empty() {
        return Ok(Some("right_input_dependency"));
    }
    if outer.is_empty() {
        return Ok(Some("missing_left_columns"));
    }
    let mut projects_inner_column = false;
    for predicate in predicates {
        let mut uses_inner = false;
        let mut uses_outer = false;
        for reference in &predicate.references {
            if inner.outputs.contains(&reference.column) {
                uses_inner = true;
            } else if outer.contains(&reference.column) {
                uses_outer = true;
            } else {
                return Ok(Some("unavailable_columns"));
            }
        }
        if uses_outer {
            if *kind == JoinKind::Anti && !uses_inner {
                return Ok(Some("anti_predicate_placement"));
            }
            projects_inner_column |= uses_inner;
        }
    }
    Ok((!projects_inner_column).then_some("missing_correlation_column"))
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
        input @ (Relation::Scan(_) | Relation::SharedRef { .. } | Relation::Subquery { .. }) => {
            (Box::new(input), Vec::new())
        }
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

fn pull_filter_over_join(
    left: Relation,
    mut input: Relation,
    predicates: Vec<Scalar>,
    kind: JoinKind,
    subquery: turso_parser::ast::TableInternalId,
    plan: &mut LogicalPlan,
) -> Result<Relation> {
    let inner_columns = plan.properties(&input)?.outputs;
    let (local, mut correlated): (Vec<_>, Vec<_>) = predicates.into_iter().partition(|predicate| {
        predicate
            .references
            .iter()
            .all(|reference| inner_columns.contains(&reference.column))
    });
    if !local.is_empty() {
        input = Relation::Filter {
            input: Box::new(input),
            predicates: local,
        };
    }
    let mut outputs = Vec::new();
    for predicate in &mut correlated {
        predicate.project_input_columns(&inner_columns, subquery, &plan.bindings, &mut outputs)?;
        predicate.bind_all_local();
    }
    assert!(!outputs.is_empty(), "joined filter needs an inner column");
    assert!(
        plan.bindings.iter().all(|binding| binding.id != subquery),
        "subquery binding is fresh"
    );
    let columns = outputs.iter().map(|output| output.column.id).collect();
    let binding_columns = outputs
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
        name: format!("exists_input_{subquery}"),
        columns: BindingColumns::Derived(binding_columns),
    });
    Ok(Relation::Join {
        left: Box::new(left),
        right: Box::new(Relation::Subquery {
            binding: subquery,
            input: Box::new(Relation::Project {
                input: Box::new(input),
                outputs,
            }),
            columns,
        }),
        kind,
        predicates: correlated,
    })
}

pub(super) fn can_reorder(relation: &Relation, plan: &LogicalPlan) -> bool {
    match relation {
        Relation::OneRow | Relation::Scan(_) => true,
        Relation::Values(values) => values.rows.iter().flatten().all(Scalar::can_reorder),
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
        | Relation::Distinct { .. }
        | Relation::Aggregate { .. }
        | Relation::Set { .. }
        | Relation::DependentJoin { .. }
        | Relation::ScalarJoin { .. }
        | Relation::Membership { .. }
        | Relation::Sort { .. }
        | Relation::Limit { .. } => false,
    }
}

pub(super) fn reorderable_projection(relation: &Relation, plan: &LogicalPlan) -> bool {
    if matches!(relation, Relation::Values(_)) {
        return can_reorder(relation, plan);
    }
    let Relation::Project { input, outputs } = relation else {
        return false;
    };
    outputs.iter().all(|output| output.expr.can_reorder()) && can_reorder(input, plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_exhaustion_keeps_a_valid_parent_after_its_child_is_unnested() {
        let mut plan = super::super::scalar::rewrite_tests::nested_input_plan();
        let bindings = plan.bindings.len();
        let mut report = RewriteReport {
            added_nodes: MAX_ADDED_NODES - 1,
            ..RewriteReport::default()
        };
        let mut root = std::mem::replace(&mut plan.root, Relation::OneRow);
        rewrite(&mut root, &mut plan, &mut report).unwrap();
        plan.root = root;
        assert!(report.exhausted);
        assert_eq!(report.dependent_filters_pulled(), 1);
        assert_eq!(plan.dependent_join_count(), 1);
        assert_eq!(plan.bindings.len(), bindings);
        assert_eq!(report.added_nodes, MAX_ADDED_NODES - 1);
        plan.validate().unwrap();
    }

    #[test]
    fn growth_exhaustion_keeps_the_dependent_input_and_its_bindings() {
        let mut plan = super::super::scalar::rewrite_tests::joined_input_plan();
        let before = format!("{:?}", plan.root);
        let bindings = plan.bindings.len();
        let mut report = RewriteReport {
            added_nodes: MAX_ADDED_NODES - 1,
            ..RewriteReport::default()
        };
        let mut root = std::mem::replace(&mut plan.root, Relation::OneRow);
        rewrite(&mut root, &mut plan, &mut report).unwrap();
        plan.root = root;
        assert!(report.exhausted);
        assert_eq!(report.applied, 0);
        assert_eq!(report.added_nodes, MAX_ADDED_NODES - 1);
        assert_eq!(plan.bindings.len(), bindings);
        assert_eq!(format!("{:?}", plan.root), before);
        assert_eq!(plan.dependent_join_count(), 1);
        let Relation::DependentJoin {
            left, right, kind, ..
        } = &plan.root
        else {
            panic!("growth exhaustion must preserve the dependent join");
        };
        assert!(dependent_filter_rules(left, right, kind, &plan).unwrap()[1]
            .1
            .is_none());
        plan.validate().unwrap();
    }

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
        normalize_node(&mut root, &mut plan, &mut report).unwrap();
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

    #[test]
    fn shared_producers_and_consumers_use_the_same_visit_budget() {
        let mut plan = LogicalPlan {
            root: Relation::Filter {
                input: Box::new(Relation::OneRow),
                predicates: Vec::new(),
            },
            bindings: Vec::new(),
            shared_inputs: vec![super::super::SharedInput {
                id: 0,
                source_binding: 0.into(),
                input: one_row_joins(12),
                columns: Vec::new(),
            }],
            outer_columns: Vec::new(),
            parameters: Vec::new(),
        };
        let report = normalize(&mut plan).unwrap();
        assert!(report.exhausted);
        assert_eq!(report.visited, MAX_VISITS);
        assert_eq!(report.applied, 0);
        assert!(matches!(plan.root, Relation::Filter { .. }));
        assert_eq!(node_count(&plan.shared_inputs[0].input), 8191);
        plan.validate().unwrap();
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
