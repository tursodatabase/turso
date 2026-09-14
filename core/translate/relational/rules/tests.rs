use super::tests::{column, plan};
use super::*;
use crate::translate::relational::{
    rewrite, Binding, BindingColumns, Column, JoinKind, Output, Relation, SetOperation, Values,
};

#[test]
fn normalization_inspection_reports_only_the_first_failed_precondition() {
    let mut predicate = column(1, Scope::Local);
    predicate.can_fail = true;
    let cases = [
        (
            Relation::Filter {
                input: Box::new(Relation::Filter {
                    input: Box::new(Relation::Scan(1.into())),
                    predicates: vec![predicate.clone()],
                }),
                predicates: vec![predicate.clone()],
            },
            "MergeSelects",
            "Pure",
        ),
        (
            Relation::Filter {
                input: Box::new(Relation::Join {
                    left: Box::new(Relation::Scan(1.into())),
                    right: Box::new(Relation::Scan(2.into())),
                    kind: JoinKind::Semi,
                    predicates: vec![predicate.clone()],
                }),
                predicates: vec![predicate],
            },
            "MergeSelectInnerJoin",
            "Inner",
        ),
    ];
    for (root, rule, precondition) in cases {
        let plan = plan(root);
        plan.validate().unwrap();
        let mut failures = Vec::new();
        rewrite::normalization_declines(&plan.root, &plan, |name, failed| {
            if name == rule {
                failures.push(failed);
            }
        })
        .unwrap();
        assert_eq!(failures, [precondition], "{rule}");
    }
}

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
fn set_inputs_normalize_without_changing_their_comparisons() {
    for operator in [
        ast::CompoundOperator::UnionAll,
        ast::CompoundOperator::Union,
        ast::CompoundOperator::Intersect,
        ast::CompoundOperator::Except,
    ] {
        let mut plan = plan(Relation::Set {
            left: Box::new(Relation::Filter {
                input: Box::new(Relation::Scan(1.into())),
                predicates: Vec::new(),
            }),
            right: Box::new(Relation::Filter {
                input: Box::new(Relation::Scan(2.into())),
                predicates: Vec::new(),
            }),
            operation: Box::new(SetOperation {
                operator,
                outputs: vec![output(3).column],
                comparison_collations: vec![CollationSeq::NoCase],
            }),
        });
        let report = normalize(&mut plan);
        assert_eq!(count(&report, "EliminateSelect"), 2);
        let Relation::Set {
            left,
            right,
            operation,
        } = &plan.root
        else {
            panic!("set operation must remain after normalizing its inputs");
        };
        assert_eq!(operation.operator, operator);
        assert_eq!(operation.comparison_collations, [CollationSeq::NoCase]);
        assert!(matches!(left.as_ref(), Relation::Scan(_)));
        assert!(matches!(right.as_ref(), Relation::Scan(_)));
        assert_eq!(
            plan.output_columns(&plan.root).unwrap(),
            vec![output(3).column.id]
        );
        plan.validate().unwrap();
    }
}

#[test]
fn set_validation_rejects_invalid_input_and_output_mappings() {
    for (invalid, message) in [
        ("arity", "different column counts"),
        ("input_identity", "share output identities"),
        ("output_identity", "reuses an input identity"),
        ("collations", "incorrect number of comparison collations"),
    ] {
        let mut left = Relation::Scan(1.into());
        let mut right = Relation::Scan(2.into());
        let mut operation = SetOperation {
            operator: ast::CompoundOperator::Union,
            outputs: vec![output(3).column],
            comparison_collations: vec![CollationSeq::Binary],
        };
        match invalid {
            "arity" => left = Relation::OneRow,
            "input_identity" => right = Relation::Scan(1.into()),
            "output_identity" => operation.outputs[0].id.relation = 1.into(),
            "collations" => operation.comparison_collations.clear(),
            _ => unreachable!(),
        }
        let plan = plan(Relation::Set {
            left: Box::new(left),
            right: Box::new(right),
            operation: Box::new(operation),
        });
        assert!(plan.validate().unwrap_err().to_string().contains(message));
    }
}

#[test]
fn values_validate_outer_references_and_row_widths() {
    let outer = column(1, Scope::Outer(0));
    let mut plan = plan(Relation::Values(Box::new(Values {
        rows: vec![vec![outer.clone()]],
        columns: vec![output(3).column],
    })));
    assert!(plan
        .validate()
        .unwrap_err()
        .to_string()
        .contains("unbound outer reference"));
    plan.outer_columns.push(outer.references[0].column);
    plan.validate().unwrap();
    let Relation::Values(values) = &mut plan.root else {
        unreachable!();
    };
    values.rows.push(Vec::new());
    assert!(plan
        .validate()
        .unwrap_err()
        .to_string()
        .contains("wrong column count"));
    let Relation::Values(values) = &mut plan.root else {
        unreachable!();
    };
    values.rows.pop();
    values.columns[0].id = outer.references[0].column;
    assert!(plan
        .validate()
        .unwrap_err()
        .to_string()
        .contains("reuses an outer identity"));
}

#[test]
fn membership_validates_arity_and_local_comparison_columns() {
    let mut plan = plan(Relation::Membership {
        left: Box::new(Relation::Scan(1.into())),
        right: Box::new(Relation::Project {
            input: Box::new(Relation::Scan(2.into())),
            outputs: vec![Output {
                expr: column(2, Scope::Local),
                ..output(3)
            }],
        }),
        lhs: vec![column(1, Scope::Local)],
        negated: false,
        subquery: 4.into(),
    });
    plan.validate().unwrap();
    let mut rewritten = plan.clone();
    let before = nodes(&rewritten.root);
    let report = rewrite::normalize(&mut rewritten).unwrap();
    assert_eq!(count(&report, "UnnestMembership"), 1);
    assert_eq!(nodes(&rewritten.root), before + 1);
    assert_eq!(report.added_nodes, 1);
    rewritten.validate().unwrap();
    let Relation::Membership { lhs, .. } = &mut plan.root else {
        unreachable!()
    };
    lhs.push(column(1, Scope::Local));
    assert!(plan
        .validate()
        .unwrap_err()
        .to_string()
        .contains("different column counts"));
    let Relation::Membership { lhs, .. } = &mut plan.root else {
        unreachable!()
    };
    *lhs = vec![column(2, Scope::Local)];
    assert!(plan
        .validate()
        .unwrap_err()
        .to_string()
        .contains("outside its input"));
}

#[test]
fn duplicate_filters_keep_the_first_expression_and_order() {
    let first = column(1, Scope::Local);
    let mut second = first.clone();
    second.expr = Expr::IsNull(Box::new(second.expr));
    second.nullable = false;
    second.affinity = Affinity::None;
    let mut plan = plan(Relation::Filter {
        input: Box::new(Relation::Scan(1.into())),
        predicates: vec![
            first.clone(),
            second.clone(),
            first.clone(),
            first.clone(),
            second.clone(),
        ],
    });
    let report = normalize(&mut plan);
    let Relation::Filter { predicates, .. } = &plan.root else {
        panic!("nonempty filter must remain");
    };
    assert_eq!(predicates.len(), 2);
    assert_eq!(predicates[0].ast(), first.ast());
    assert_eq!(predicates[1].ast(), second.ast());
    assert_eq!(count(&report, "DeduplicateSelectFilters"), 1);
    assert_eq!(normalize(&mut plan).applied, 0);
}

#[test]
fn duplicate_filters_keep_between_and_not_between() {
    let mut between = column(1, Scope::Local);
    between.expr = Expr::Between {
        lhs: Box::new(between.expr),
        not: false,
        start: Box::new(Expr::Literal(ast::Literal::Numeric("0".to_owned()))),
        end: Box::new(Expr::Literal(ast::Literal::Numeric("2".to_owned()))),
    };
    between.affinity = Affinity::None;
    let mut not_between = between.clone();
    let Expr::Between { not, .. } = &mut not_between.expr else {
        unreachable!();
    };
    *not = true;
    let third = column(1, Scope::Local);
    let expected = vec![between.clone(), not_between.clone(), third.clone()];
    let mut plan = plan(Relation::Filter {
        input: Box::new(Relation::Scan(1.into())),
        predicates: vec![between.clone(), not_between, third.clone(), between, third],
    });
    let report = normalize(&mut plan);
    let Relation::Filter { predicates, .. } = &plan.root else {
        panic!("different range predicates must remain");
    };
    assert_eq!(*predicates, expected);
    assert_eq!(count(&report, "DeduplicateSelectFilters"), 1);
}

#[test]
fn duplicate_filters_keep_effectful_predicates() {
    for effect in ["error", "volatile"] {
        let first = column(1, Scope::Local);
        let mut second = first.clone();
        second.can_fail = effect == "error";
        second.volatile = effect == "volatile";
        let mut plan = plan(Relation::Filter {
            input: Box::new(Relation::Scan(1.into())),
            predicates: vec![first.clone(), second.clone(), first, second],
        });
        let report = normalize(&mut plan);
        assert_eq!(count(&report, "DeduplicateSelectFilters"), 0);
        let Relation::Filter { predicates, .. } = &plan.root else {
            panic!("effectful filter must remain");
        };
        assert_eq!(predicates.len(), 4);
    }
}

#[test]
fn duplicate_filters_require_identical_comparison_properties() {
    for property in ["affinity", "collation", "nullability", "operand_order"] {
        let first = column(1, Scope::Local);
        let mut second = first.clone();
        match property {
            "affinity" => second.affinity = Affinity::Text,
            "collation" => second.collation = CollationSeq::NoCase,
            "nullability" => second.nullable = false,
            "operand_order" => {
                second.expr = Expr::binary(
                    Expr::Literal(ast::Literal::Numeric("1".to_owned())),
                    ast::Operator::Equals,
                    second.expr,
                );
            }
            _ => unreachable!(),
        }
        let first = if property == "operand_order" {
            Scalar {
                expr: Expr::binary(
                    first.expr,
                    ast::Operator::Equals,
                    Expr::Literal(ast::Literal::Numeric("1".to_owned())),
                ),
                ..first
            }
        } else {
            first
        };
        let mut plan = plan(Relation::Filter {
            input: Box::new(Relation::Scan(1.into())),
            predicates: vec![first, second],
        });
        let report = normalize(&mut plan);
        assert_eq!(count(&report, "DeduplicateSelectFilters"), 0, "{property}");
        let Relation::Filter { predicates, .. } = &plan.root else {
            panic!("different predicates must remain");
        };
        assert_eq!(predicates.len(), 2);
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
        assert_eq!(
            count(&report, "DeduplicateSelectFilters"),
            usize::from(!effectful)
        );
        let Relation::Filter { input, predicates } = &plan.root else {
            panic!("expected filter")
        };
        assert_eq!(predicates.len(), 1);
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
    assert_eq!(count(&report, "DeduplicateSelectFilters"), 1);
    let Relation::Project { input, outputs } = &plan.root else {
        panic!("expected projection")
    };
    assert_eq!(outputs[0].column.id.relation, 3.into());
    let Relation::Filter { predicates, .. } = input.as_ref() else {
        panic!("expected filter")
    };
    assert_eq!(predicates.len(), 1);
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

#[test]
fn pulling_a_left_filter_preserves_semi_and_anti_evaluation() {
    for kind in [JoinKind::Inner, JoinKind::Semi, JoinKind::Anti] {
        for effect in ["none", "filter error", "right volatile", "join error"] {
            let mut predicate = column(1, Scope::Local);
            predicate.can_fail = effect == "filter error";
            let mut right_predicate = column(2, Scope::Local);
            right_predicate.volatile = effect == "right volatile";
            let mut on = column(1, Scope::Local);
            on.can_fail = effect == "join error";
            let mut plan = plan(Relation::Join {
                left: Box::new(Relation::Filter {
                    input: Box::new(Relation::Scan(1.into())),
                    predicates: vec![predicate],
                }),
                right: Box::new(Relation::Filter {
                    input: Box::new(Relation::Scan(2.into())),
                    predicates: vec![right_predicate],
                }),
                kind,
                predicates: vec![on],
            });
            let report = normalize(&mut plan);
            let expected = kind != JoinKind::Inner && effect == "none";
            assert_eq!(count(&report, "PullLeftFilter"), usize::from(expected));
            assert_eq!(matches!(plan.root, Relation::Filter { .. }), expected);
        }
    }
}

#[test]
fn pulling_a_filter_over_a_derived_input_requires_pure_independent_rows() {
    for (dependent, effectful) in [(false, false), (true, false), (false, true)] {
        let mut input = Relation::Scan(2.into());
        if dependent {
            input = Relation::Filter {
                input: Box::new(input),
                predicates: vec![column(1, Scope::Outer(0))],
            };
        }
        let mut projected = output(2);
        projected.expr = column(2, Scope::Local);
        projected.expr.volatile = effectful;
        let mut plan = plan(Relation::DependentJoin {
            left: Box::new(Relation::Scan(1.into())),
            right: Box::new(Relation::Filter {
                input: Box::new(Relation::Subquery {
                    binding: 3.into(),
                    input: Box::new(Relation::Project {
                        input: Box::new(input),
                        outputs: vec![projected],
                    }),
                    columns: vec![column(2, Scope::Local).as_column().unwrap()],
                }),
                predicates: vec![column(3, Scope::Local), column(1, Scope::Outer(0))],
            }),
            kind: JoinKind::Semi,
            subquery: 4.into(),
        });
        plan.bindings.push(Binding {
            id: 3.into(),
            name: "derived".to_owned(),
            columns: BindingColumns::Derived(vec![output(3).column]),
        });
        plan.validate().unwrap();
        let report = normalize(&mut plan);
        let pulled = !dependent && !effectful;
        assert_eq!(count(&report, "PullDependentFilter"), usize::from(pulled));
        assert_eq!(matches!(plan.root, Relation::Join { .. }), pulled);
    }
}

#[test]
fn joined_filter_lowering_maps_only_needed_columns_and_respects_effects() {
    for effectful in [false, true] {
        let mut plan = joined_input_plan();
        let Relation::DependentJoin { right, .. } = &mut plan.root else {
            unreachable!()
        };
        let Relation::Filter { predicates, .. } = right.as_mut() else {
            unreachable!()
        };
        predicates[0].can_fail = effectful;
        let before = nodes(&plan.root);
        let report = rewrite::normalize(&mut plan).unwrap();
        assert_eq!(
            count(&report, "PullDependentFilterOverJoin"),
            usize::from(!effectful)
        );
        assert!(nodes(&plan.root) <= before + report.added_nodes);
        if !effectful {
            let Relation::Join {
                right, predicates, ..
            } = &plan.root
            else {
                panic!("expected a semi join")
            };
            let Relation::Subquery {
                binding, columns, ..
            } = right.as_ref()
            else {
                panic!("expected a joined subquery")
            };
            assert_eq!(*binding, 4.into());
            assert_eq!(columns, &[column(2, Scope::Local).as_column().unwrap()]);
            assert!(predicates[0].references.iter().all(|reference| {
                reference.scope == Scope::Local
                    && [1.into(), 4.into()].contains(&reference.column.relation)
            }));
        }
        plan.validate().unwrap();
    }
}

pub(in crate::translate::relational) fn nested_input_plan(
) -> crate::translate::relational::LogicalPlan {
    let mut plan = joined_input_plan();
    let Relation::DependentJoin { right, .. } = &mut plan.root else {
        unreachable!()
    };
    let Relation::Filter {
        input,
        mut predicates,
    } = std::mem::replace(right.as_mut(), Relation::OneRow)
    else {
        unreachable!()
    };
    let Relation::Join {
        left, right: inner, ..
    } = *input
    else {
        unreachable!()
    };
    let mut correlated = column(3, Scope::Local);
    let outer = column(2, Scope::Outer(0));
    correlated.expr = Expr::Binary(
        Box::new(correlated.expr),
        ast::Operator::Greater,
        Box::new(outer.expr),
    );
    correlated.references.extend(outer.references);
    correlated.affinity = Affinity::Blob;
    correlated.collation = CollationSeq::Unset;
    *right = Box::new(Relation::DependentJoin {
        left: Box::new(Relation::Filter {
            input: left,
            predicates: vec![predicates.remove(0)],
        }),
        right: Box::new(Relation::Filter {
            input: inner,
            predicates: vec![correlated],
        }),
        kind: JoinKind::Semi,
        subquery: 5.into(),
    });
    plan.validate().unwrap();
    plan
}

pub(in crate::translate::relational) fn joined_input_plan(
) -> crate::translate::relational::LogicalPlan {
    let mut predicate = column(2, Scope::Local);
    let outer = column(1, Scope::Outer(0));
    predicate.expr = Expr::Binary(
        Box::new(predicate.expr),
        ast::Operator::Greater,
        Box::new(outer.expr),
    );
    predicate.references.extend(outer.references);
    predicate.affinity = Affinity::Blob;
    predicate.collation = CollationSeq::Unset;
    let mut plan = plan(Relation::DependentJoin {
        left: Box::new(Relation::Scan(1.into())),
        right: Box::new(Relation::Filter {
            input: Box::new(Relation::Join {
                left: Box::new(Relation::Scan(2.into())),
                right: Box::new(Relation::Scan(3.into())),
                kind: JoinKind::Inner,
                predicates: Vec::new(),
            }),
            predicates: vec![predicate, column(3, Scope::Local)],
        }),
        kind: JoinKind::Semi,
        subquery: 4.into(),
    });
    plan.bindings.push(Binding {
        id: 3.into(),
        name: "third".to_owned(),
        columns: BindingColumns::Derived(vec![output(3).column]),
    });
    plan.validate().unwrap();
    plan
}

fn normalize(plan: &mut crate::translate::relational::LogicalPlan) -> rewrite::RewriteReport {
    let before = nodes(&plan.root);
    let report = rewrite::normalize(plan).unwrap();
    assert!(nodes(&plan.root) <= before, "normalization grew the plan");
    report
}

fn nodes(relation: &Relation) -> usize {
    1 + match relation {
        Relation::OneRow | Relation::Values(_) | Relation::Scan(_) | Relation::SharedRef { .. } => {
            0
        }
        Relation::Filter { input, .. }
        | Relation::Subquery { input, .. }
        | Relation::Project { input, .. }
        | Relation::Distinct { input }
        | Relation::Aggregate { input, .. }
        | Relation::Sort { input, .. }
        | Relation::Limit { input, .. } => nodes(input),
        Relation::Join { left, right, .. }
        | Relation::Set { left, right, .. }
        | Relation::Membership { left, right, .. }
        | Relation::ScalarJoin { left, right, .. }
        | Relation::DependentJoin { left, right, .. } => nodes(left) + nodes(right),
    }
}

fn count(report: &rewrite::RewriteReport, rule: &str) -> usize {
    report.rules().find(|(name, _)| *name == rule).unwrap().1
}

fn output(relation: usize) -> Output {
    Output {
        contains_aggregates: false,
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
