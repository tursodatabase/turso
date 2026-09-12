use super::tests::{column, plan};
use super::*;
use crate::translate::relational::{rewrite, Column, JoinKind, Output, Relation};

#[test]
fn empty_filter_elimination_keeps_nonempty_predicates() {
    for empty in [true, false] {
        let mut plan = plan(Relation::Filter {
            input: Box::new(Relation::Scan(1.into())),
            predicates: if empty {
                Vec::new()
            } else {
                vec![column(1, Scope::Local)]
            },
        });
        let report = normalize(&mut plan);
        assert_eq!(count(&report, "EliminateSelect"), usize::from(empty));
        assert_eq!(matches!(plan.root, Relation::Scan(_)), empty);
    }
}

#[test]
fn merging_filters_preserves_effectful_evaluation_boundaries() {
    for effectful in [false, true] {
        let mut outer = column(1, Scope::Local);
        outer.can_fail = effectful;
        let mut plan = plan(Relation::Filter {
            input: Box::new(Relation::Filter {
                input: Box::new(Relation::Scan(1.into())),
                predicates: vec![column(1, Scope::Local)],
            }),
            predicates: vec![outer],
        });
        let report = normalize(&mut plan);
        assert_eq!(count(&report, "MergeSelects"), usize::from(!effectful));
        let Relation::Filter { input, predicates } = &plan.root else {
            panic!("expected filter")
        };
        assert_eq!(predicates.len(), if effectful { 1 } else { 2 });
        assert_eq!(matches!(input.as_ref(), Relation::Filter { .. }), effectful);
    }
}

#[test]
fn identity_projection_elimination_preserves_names_and_collation() {
    for changed in ["none", "name", "alias", "collation", "identity", "error"] {
        let mut output = output(1);
        match changed {
            "name" => output.column.name = "renamed".to_owned(),
            "alias" => output.alias = Some("key".to_owned()),
            "collation" => output.column.collation = CollationSeq::NoCase,
            "identity" => output.column.id.relation = 3.into(),
            "error" => output.expr.can_fail = true,
            _ => {}
        }
        let mut plan = plan(Relation::Project {
            input: Box::new(Relation::Scan(1.into())),
            outputs: vec![output],
        });
        let report = normalize(&mut plan);
        assert_eq!(
            count(&report, "EliminateProject"),
            usize::from(changed == "none")
        );
        assert_eq!(matches!(plan.root, Relation::Scan(_)), changed == "none");
    }
}

#[test]
fn pushing_a_filter_rebinds_columns_and_merges_the_new_adjacent_filters() {
    let mut plan = plan(Relation::Filter {
        input: Box::new(Relation::Project {
            input: Box::new(Relation::Filter {
                input: Box::new(Relation::Scan(1.into())),
                predicates: vec![column(1, Scope::Local)],
            }),
            outputs: vec![output(3)],
        }),
        predicates: vec![column(3, Scope::Local)],
    });
    plan.validate().unwrap();
    let report = normalize(&mut plan);
    assert_eq!(count(&report, "PushSelectIntoProject"), 1);
    assert_eq!(count(&report, "MergeSelects"), 1);
    let Relation::Project { input, outputs } = &plan.root else {
        panic!("expected projection")
    };
    assert_eq!(outputs[0].column.id.relation, 3.into());
    let Relation::Filter { predicates, .. } = input.as_ref() else {
        panic!("expected filter")
    };
    assert_eq!(predicates.len(), 2);
    for predicate in predicates {
        assert_eq!(
            predicate.as_column(),
            Some(ColumnId {
                relation: 1.into(),
                position: Some(0)
            })
        );
        assert_eq!(predicate.references[0].column.relation, 1.into());
    }
}

#[test]
fn filter_pushdown_keeps_volatile_and_failing_expressions_in_place() {
    for effect in ["projection error", "predicate error", "volatile predicate"] {
        let mut output = output(3);
        let mut predicate = column(3, Scope::Local);
        match effect {
            "projection error" => output.expr.can_fail = true,
            "predicate error" => predicate.can_fail = true,
            "volatile predicate" => predicate.volatile = true,
            _ => unreachable!(),
        }
        let mut plan = plan(Relation::Filter {
            input: Box::new(Relation::Project {
                input: Box::new(Relation::Scan(1.into())),
                outputs: vec![output],
            }),
            predicates: vec![predicate],
        });
        let report = normalize(&mut plan);
        assert_eq!(count(&report, "PushSelectIntoProject"), 0);
        assert!(matches!(plan.root, Relation::Filter { .. }));
    }
}

#[test]
fn merging_join_filters_requires_an_inner_join_and_pure_predicates() {
    for (kind, effectful) in [
        (JoinKind::Inner, false),
        (JoinKind::Semi, false),
        (JoinKind::Anti, false),
        (JoinKind::Inner, true),
    ] {
        let mut predicate = column(1, Scope::Local);
        predicate.can_fail = effectful;
        let mut plan = plan(Relation::Filter {
            input: Box::new(Relation::Join {
                left: Box::new(Relation::Scan(1.into())),
                right: Box::new(Relation::Scan(2.into())),
                kind,
                predicates: Vec::new(),
            }),
            predicates: vec![predicate],
        });
        let report = normalize(&mut plan);
        let merged = kind == JoinKind::Inner && !effectful;
        assert_eq!(count(&report, "MergeSelectInnerJoin"), usize::from(merged));
        assert_eq!(matches!(plan.root, Relation::Join { .. }), merged);
    }
}

fn normalize(plan: &mut crate::translate::relational::LogicalPlan) -> rewrite::RewriteReport {
    let before = nodes(&plan.root);
    let report = rewrite::normalize(plan).unwrap();
    assert!(nodes(&plan.root) <= before, "normalization grew the plan");
    report
}

fn nodes(relation: &Relation) -> usize {
    1 + match relation {
        Relation::OneRow | Relation::Scan(_) | Relation::SharedRef { .. } => 0,
        Relation::Filter { input, .. }
        | Relation::Project { input, .. }
        | Relation::Sort { input, .. }
        | Relation::Limit { input, .. } => nodes(input),
        Relation::Join { left, right, .. } | Relation::DependentJoin { left, right, .. } => {
            nodes(left) + nodes(right)
        }
    }
}

fn count(report: &rewrite::RewriteReport, rule: &str) -> usize {
    report.rules().find(|(name, _)| *name == rule).unwrap().1
}

fn output(relation: usize) -> Output {
    Output {
        column: Column {
            id: ColumnId {
                relation: relation.into(),
                position: Some(0),
            },
            name: "key".to_owned(),
            nullable: true,
            affinity: Affinity::Integer,
            collation: CollationSeq::Binary,
        },
        expr: column(1, Scope::Local),
        alias: None,
        implicit_name: None,
    }
}
