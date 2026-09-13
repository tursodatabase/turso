use smallvec::SmallVec;
use turso_parser::ast::{self, Expr};

use crate::translate::{
    collate::{get_collseq_from_expr_with_symbols, CollationSeq},
    emitter::Resolver,
    expr::{
        expr_contains_nondeterministic_scalar_function, expression_node_can_fail_on_input,
        get_expr_affinity, walk_expr, walk_expr_mut, WalkControl,
    },
    plan::{Aggregate, TableReferences},
};
use crate::vdbe::affinity::Affinity;
use crate::Result;

use super::{binding::BindError, Binding, ColumnId, ColumnReference, ColumnSet, Output, Scope};

/// The AST storage is reused only after rejecting names, subqueries and execution
/// resources. Mutation goes through binding or scope changes, never the emitter.
#[derive(Clone, Debug)]
pub(crate) struct Scalar {
    expr: Expr,
    pub references: SmallVec<[ColumnReference; 2]>,
    pub affinity: Affinity,
    pub collation: CollationSeq,
    pub nullable: bool,
    pub can_fail: bool,
    pub volatile: bool,
}

impl Scalar {
    pub(crate) fn bind(
        expr: Expr,
        tables: &TableReferences,
        resolver: &Resolver,
    ) -> std::result::Result<Self, BindError> {
        Self::bind_with_aggregates(expr, tables, resolver, &[])
    }

    pub(crate) fn bind_with_aggregates(
        expr: Expr,
        tables: &TableReferences,
        resolver: &Resolver,
        aggregates: &[Aggregate],
    ) -> std::result::Result<Self, BindError> {
        let mut references = SmallVec::new();
        let mut unsupported = None;
        let mut can_fail = false;
        let mut has_function = false;
        walk_expr(&expr, &mut |expr| -> Result<WalkControl> {
            can_fail |= expression_node_can_fail_on_input(expr);
            has_function |= matches!(
                expr,
                Expr::FunctionCall { .. } | Expr::FunctionCallStar { .. }
            );
            can_fail |= matches!(
                expr,
                Expr::Cast { .. }
                    | Expr::FieldAccess { .. }
                    | Expr::Array { .. }
                    | Expr::Subscript { .. }
            );
            let column = match expr {
                Expr::FunctionCall {
                    distinctness,
                    order_by,
                    within_group,
                    filter_over,
                    ..
                } if distinctness.is_some()
                    || !order_by.is_empty()
                    || !within_group.is_empty()
                    || filter_over.filter_clause.is_some()
                    || filter_over.over_clause.is_some() =>
                {
                    if !aggregates
                        .iter()
                        .any(|aggregate| aggregate.original_expr == *expr)
                    {
                        unsupported = Some("aggregate function modifiers");
                        return Ok(WalkControl::SkipChildren);
                    }
                    None
                }
                Expr::FunctionCallStar { filter_over, .. }
                    if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() =>
                {
                    if !aggregates
                        .iter()
                        .any(|aggregate| aggregate.original_expr == *expr)
                    {
                        unsupported = Some("aggregate function modifiers");
                        return Ok(WalkControl::SkipChildren);
                    }
                    None
                }
                Expr::Column { table, column, .. } => Some(ColumnId {
                    relation: *table,
                    position: Some(*column),
                }),
                Expr::RowId { table, .. } => Some(ColumnId {
                    relation: *table,
                    position: None,
                }),
                Expr::Collate(_, name) => {
                    can_fail |= !matches!(
                        resolver.resolve_collation(name.as_str())?,
                        CollationSeq::Unset
                            | CollationSeq::Binary
                            | CollationSeq::NoCase
                            | CollationSeq::Rtrim
                    );
                    None
                }
                Expr::Register(_) | Expr::SubqueryResult { .. } => {
                    unsupported = Some("scalar execution resource or subquery result");
                    return Ok(WalkControl::SkipChildren);
                }
                Expr::Id(_)
                | Expr::Name(_)
                | Expr::Qualified(_, _)
                | Expr::DoublyQualified(_, _, _)
                | Expr::Exists(_)
                | Expr::Subquery(_)
                | Expr::InSelect { .. }
                | Expr::InTable { .. }
                | Expr::Default => {
                    unsupported = Some("unbound scalar expression");
                    return Ok(WalkControl::SkipChildren);
                }
                _ => None,
            };
            if let Some(column) = column {
                let scope = if tables
                    .find_joined_table_by_internal_id(column.relation)
                    .is_some()
                {
                    Scope::Local
                } else if let Some(outer) = tables.outer_query_refs().iter().find(|outer| {
                    outer.internal_id == column.relation && !outer.cte_definition_only
                }) {
                    Scope::Outer(outer.scope_depth)
                } else {
                    unsupported = Some("column is outside the bound query scope");
                    return Ok(WalkControl::SkipChildren);
                };
                let reference = ColumnReference { column, scope };
                if !references.contains(&reference) {
                    references.push(reference);
                }
            }
            Ok(WalkControl::Continue)
        })?;
        if let Some(reason) = unsupported {
            return Err(BindError::Unsupported(reason));
        }
        let collation =
            get_collseq_from_expr_with_symbols(&expr, tables, Some(resolver.symbol_table))?
                .unwrap_or(CollationSeq::Unset);
        can_fail |= !matches!(
            collation,
            CollationSeq::Unset | CollationSeq::Binary | CollationSeq::NoCase | CollationSeq::Rtrim
        );
        can_fail |= references.iter().any(|reference| {
            reference
                .column
                .position
                .and_then(|index| {
                    tables
                        .find_table_by_internal_id(reference.column.relation)
                        .and_then(|(_, table)| table.get_column_at(index))
                })
                .is_some_and(|column| {
                    column.is_virtual_generated()
                        || !matches!(
                            column.collation(),
                            CollationSeq::Unset
                                | CollationSeq::Binary
                                | CollationSeq::NoCase
                                | CollationSeq::Rtrim
                        )
                })
        });
        Ok(Self {
            affinity: get_expr_affinity(&expr, Some(tables), Some(resolver)),
            collation,
            nullable: nullable(&expr, tables),
            can_fail,
            volatile: has_function
                && expr_contains_nondeterministic_scalar_function(&expr, resolver)?,
            expr,
            references,
        })
    }

    pub(crate) fn ast(&self) -> &Expr {
        &self.expr
    }

    pub(crate) fn into_ast(self) -> Expr {
        self.expr
    }

    pub(crate) fn can_reorder(&self) -> bool {
        !self.can_fail && !self.volatile
    }

    pub(crate) fn bind_all_local(&mut self) {
        for reference in &mut self.references {
            reference.scope = Scope::Local;
        }
    }

    pub(crate) fn as_column(&self) -> Option<ColumnId> {
        column_id(&self.expr)
    }

    pub(crate) fn project_order_columns(
        &mut self,
        outputs: &[Output],
    ) -> std::result::Result<(), BindError> {
        if !self.can_reorder() {
            return Err(BindError::Unsupported("effectful DISTINCT ordering"));
        }
        self.project_output_columns(
            outputs,
            "DISTINCT ordering references an unprojected expression",
        )
    }

    pub(crate) fn project_aggregate_order_columns(
        &mut self,
        outputs: &[Output],
    ) -> std::result::Result<(), BindError> {
        self.project_output_columns(
            outputs,
            "aggregate ordering references an unprojected expression",
        )
    }

    fn project_output_columns(
        &mut self,
        outputs: &[Output],
        missing_reason: &'static str,
    ) -> std::result::Result<(), BindError> {
        walk_expr_mut(&mut self.expr, &mut |expr| {
            if let Some(output) = outputs.iter().find(|output| &*expr == output.expr.ast()) {
                *expr = Expr::Column {
                    database: None,
                    table: output.column.id.relation,
                    column: output.column.id.position.expect("projected column ordinal"),
                    is_rowid_alias: false,
                };
                return Ok(WalkControl::SkipChildren);
            }
            Ok(WalkControl::Continue)
        })?;
        let previous = std::mem::take(&mut self.references);
        let mut missing = false;
        walk_expr(&self.expr, &mut |expr| -> Result<WalkControl> {
            if let Some(column) = column_id(expr) {
                let scope = if outputs.iter().any(|output| output.column.id == column) {
                    Scope::Local
                } else if let Some(reference) = previous.iter().find(|reference| {
                    reference.column == column && matches!(reference.scope, Scope::Outer(_))
                }) {
                    reference.scope
                } else {
                    missing = true;
                    return Ok(WalkControl::SkipChildren);
                };
                let reference = ColumnReference { column, scope };
                if !self.references.contains(&reference) {
                    self.references.push(reference);
                }
            }
            Ok(WalkControl::Continue)
        })?;
        if missing {
            return Err(BindError::Unsupported(missing_reason));
        }
        Ok(())
    }

    pub(crate) fn into_ast_with_project_outputs(mut self, outputs: &[Output]) -> Result<Expr> {
        walk_expr_mut(&mut self.expr, &mut |expr| {
            if let Some(id) = column_id(expr) {
                if let Some(output) = outputs.iter().find(|output| output.column.id == id) {
                    *expr = output.expr.expr.clone();
                    return Ok(WalkControl::SkipChildren);
                }
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(self.expr)
    }

    pub(crate) fn substitute_project_columns(&mut self, outputs: &[super::Output]) -> Result<()> {
        walk_expr_mut(&mut self.expr, &mut |expr| {
            if let Some(id) = column_id(expr) {
                if let Some(output) = outputs.iter().find(|output| output.column.id == id) {
                    *expr = output.expr.expr.clone();
                }
            }
            Ok(WalkControl::Continue)
        })?;
        for reference in &mut self.references {
            if reference.scope == Scope::Local {
                let output = outputs
                    .iter()
                    .find(|output| output.column.id == reference.column)
                    .expect("projection pushdown checked the column mapping");
                reference.column = output.expr.as_column().expect("passthrough expression");
            }
        }
        Ok(())
    }

    pub(crate) fn project_input_columns(
        &mut self,
        input_columns: &ColumnSet,
        binding: ast::TableInternalId,
        bindings: &[Binding],
        outputs: &mut Vec<Output>,
    ) -> Result<()> {
        assert!(
            self.can_reorder(),
            "projected column reads cannot change evaluation effects"
        );
        walk_expr_mut(&mut self.expr, &mut |expr| {
            let Some(id) = column_id(expr).filter(|id| input_columns.contains(id)) else {
                return Ok(WalkControl::Continue);
            };
            let position =
                if let Some(position) = outputs.iter().position(|output| output.column.id == id) {
                    position
                } else {
                    let mut column = bindings
                        .iter()
                        .find(|binding| binding.id == id.relation)
                        .ok_or_else(|| super::invalid("projected column has no binding"))?
                        .column(id);
                    let scalar = Self {
                        expr: expr.clone(),
                        references: smallvec::smallvec![ColumnReference {
                            column: id,
                            scope: Scope::Local
                        }],
                        affinity: column.affinity,
                        collation: column.collation,
                        nullable: column.nullable,
                        can_fail: false,
                        volatile: false,
                    };
                    let position = outputs.len();
                    column.name = format!("column_{position}");
                    outputs.push(Output {
                        contains_aggregates: false,
                        alias: Some(column.name.clone()),
                        column,
                        expr: scalar,
                        implicit_name: None,
                    });
                    position
                };
            *expr = Expr::Column {
                database: None,
                table: binding,
                column: position,
                is_rowid_alias: false,
            };
            Ok(WalkControl::Continue)
        })?;
        for reference in &mut self.references {
            if input_columns.contains(&reference.column) {
                let position = outputs
                    .iter()
                    .position(|output| output.column.id == reference.column)
                    .expect("projected reference has an output");
                reference.column = ColumnId {
                    relation: binding,
                    position: Some(position),
                };
            }
        }
        Ok(())
    }
}

fn column_id(expr: &Expr) -> Option<ColumnId> {
    match expr {
        Expr::Column { table, column, .. } => Some(ColumnId {
            relation: *table,
            position: Some(*column),
        }),
        Expr::RowId { table, .. } => Some(ColumnId {
            relation: *table,
            position: None,
        }),
        _ => None,
    }
}

fn nullable(expr: &Expr, tables: &TableReferences) -> bool {
    match expr {
        Expr::Literal(ast::Literal::Null) | Expr::Variable(_) => true,
        Expr::Literal(_) | Expr::IsNull(_) | Expr::NotNull(_) => false,
        Expr::RowId { table, .. } => {
            tables.find_joined_table_by_internal_id(*table).is_none()
                || tables.outer_join_may_null_extend(*table)
        }
        Expr::Column { table, column, .. } => {
            tables.find_joined_table_by_internal_id(*table).is_none()
                || tables.outer_join_may_null_extend(*table)
                || tables
                    .find_table_by_internal_id(*table)
                    .and_then(|(_, table)| table.get_column_at(*column))
                    .is_none_or(|column| !column.notnull() && !column.is_rowid_alias())
        }
        Expr::Collate(expr, _) | Expr::Unary(_, expr) => nullable(expr, tables),
        Expr::Parenthesized(exprs) if exprs.len() == 1 => nullable(&exprs[0], tables),
        Expr::Binary(_, ast::Operator::Is | ast::Operator::IsNot, _) => false,
        _ => true,
    }
}

#[cfg(test)]
#[path = "rules/tests.rs"]
pub(super) mod rewrite_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::relational::{
        Binding, BindingColumns, Column, JoinKind, LogicalPlan, Relation,
    };

    #[test]
    fn validation_rejects_columns_hidden_by_a_semi_join() {
        let plan = plan(Relation::Filter {
            input: Box::new(Relation::Join {
                left: Box::new(Relation::Scan(1.into())),
                right: Box::new(Relation::Scan(2.into())),
                kind: JoinKind::Semi,
                predicates: Vec::new(),
            }),
            predicates: vec![column(2, Scope::Local)],
        });
        assert!(plan
            .validate()
            .unwrap_err()
            .to_string()
            .contains("outside its input"));
    }

    #[test]
    fn validation_rejects_a_local_column_marked_as_outer() {
        let plan = plan(Relation::Filter {
            input: Box::new(Relation::Scan(1.into())),
            predicates: vec![column(1, Scope::Outer(0))],
        });
        assert!(plan
            .validate()
            .unwrap_err()
            .to_string()
            .contains("outer scalar references a local input"));
    }

    #[test]
    fn projected_ordering_preserves_comparison_operand_collations() {
        let mut expression = column(1, Scope::Local);
        let right_column = column(2, Scope::Local);
        expression
            .references
            .extend(right_column.references.clone());
        let left = Box::new(Expr::Collate(
            Box::new(expression.expr.clone()),
            ast::Name::exact("nocase".to_owned()),
        ));
        let right = Box::new(Expr::Collate(
            Box::new(right_column.expr),
            ast::Name::exact("binary".to_owned()),
        ));
        expression.expr = Expr::Binary(left.clone(), ast::Operator::Equals, right.clone());
        let mut reversed = expression.clone();
        reversed.expr = Expr::Binary(right, ast::Operator::Equals, left);
        let mut projected = plan(Relation::OneRow).bindings[0].column(ColumnId {
            relation: 1.into(),
            position: Some(0),
        });
        projected.id.relation = 3.into();
        let outputs = vec![Output {
            contains_aggregates: false,
            column: projected,
            expr: expression.clone(),
            alias: None,
            implicit_name: None,
        }];
        let original = expression.expr.clone();
        expression.project_order_columns(&outputs).unwrap();
        assert_eq!(expression.as_column(), Some(outputs[0].column.id));
        assert_eq!(expression.references.len(), 1);
        assert_eq!(
            expression.into_ast_with_project_outputs(&outputs).unwrap(),
            original
        );
        assert!(matches!(
            reversed.project_order_columns(&outputs),
            Err(BindError::Unsupported(
                "DISTINCT ordering references an unprojected expression"
            ))
        ));
    }

    #[test]
    fn join_predicates_can_read_both_inputs() {
        for kind in [JoinKind::Inner, JoinKind::Semi, JoinKind::Anti] {
            let plan = plan(Relation::Join {
                left: Box::new(Relation::Scan(1.into())),
                right: Box::new(Relation::Scan(2.into())),
                kind,
                predicates: vec![column(1, Scope::Local), column(2, Scope::Local)],
            });
            plan.validate().unwrap();
            let outputs = plan.properties(&plan.root).unwrap().outputs;
            assert!(outputs.contains(&ColumnId {
                relation: 1.into(),
                position: Some(0),
            }));
            assert_eq!(
                outputs.contains(&ColumnId {
                    relation: 2.into(),
                    position: Some(0),
                }),
                kind == JoinKind::Inner
            );
        }
    }

    #[test]
    fn join_predicates_reject_local_inputs_marked_as_outer() {
        for kind in [JoinKind::Inner, JoinKind::Semi, JoinKind::Anti] {
            for relation in [1, 2] {
                let plan = plan(Relation::Join {
                    left: Box::new(Relation::Scan(1.into())),
                    right: Box::new(Relation::Scan(2.into())),
                    kind,
                    predicates: vec![column(relation, Scope::Outer(0))],
                });
                assert!(plan
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("outer scalar references a local input"));
            }
        }
    }

    #[test]
    fn validation_requires_a_dependent_join_for_outer_columns() {
        let right = Relation::Filter {
            input: Box::new(Relation::Scan(2.into())),
            predicates: vec![column(1, Scope::Outer(0))],
        };
        let dependent = plan(Relation::DependentJoin {
            left: Box::new(Relation::Scan(1.into())),
            right: Box::new(right.clone()),
            kind: JoinKind::Semi,
            subquery: 3.into(),
        });
        dependent.validate().unwrap();
        let ordinary = plan(Relation::Join {
            left: Box::new(Relation::Scan(1.into())),
            right: Box::new(right),
            kind: JoinKind::Semi,
            predicates: Vec::new(),
        });
        assert!(ordinary
            .validate()
            .unwrap_err()
            .to_string()
            .contains("still has a dependency"));
    }

    #[test]
    fn validation_rejects_recursive_shared_inputs() {
        let mut plan = plan(Relation::SharedRef {
            binding: 1.into(),
            input: 7,
        });
        plan.shared_inputs
            .push(crate::translate::relational::SharedInput {
                id: 7,
                source_binding: 1.into(),
                input: Relation::SharedRef {
                    binding: 1.into(),
                    input: 7,
                },
                columns: vec![ColumnId {
                    relation: 1.into(),
                    position: Some(0),
                }],
            });
        assert!(plan
            .validate()
            .unwrap_err()
            .to_string()
            .contains("forward or recursive reference"));
    }

    #[test]
    fn derived_inputs_preserve_column_order_and_hide_child_bindings() {
        let mut plan = plan(Relation::Subquery {
            binding: 2.into(),
            input: Box::new(Relation::Scan(1.into())),
            columns: vec![ColumnId {
                relation: 1.into(),
                position: Some(0),
            }],
        });
        for binding in &mut plan.bindings {
            let BindingColumns::Derived(columns) = &mut binding.columns else {
                unreachable!()
            };
            let mut second = columns[0].clone();
            second.id.position = Some(1);
            second.name = "second".to_owned();
            columns.push(second);
        }
        let Relation::Subquery { columns, .. } = &mut plan.root else {
            unreachable!()
        };
        columns.push(ColumnId {
            relation: 1.into(),
            position: Some(1),
        });
        plan.validate().unwrap();
        assert!(plan
            .properties(&plan.root)
            .unwrap()
            .outputs
            .iter()
            .all(|column| column.relation == 2.into()));

        let Relation::Subquery { columns, .. } = &mut plan.root else {
            unreachable!()
        };
        columns.reverse();
        assert!(plan
            .validate()
            .unwrap_err()
            .to_string()
            .contains("output mapping differs"));
    }

    pub(super) fn plan(root: Relation) -> LogicalPlan {
        LogicalPlan {
            root,
            bindings: [1, 2]
                .into_iter()
                .map(|relation| Binding {
                    id: relation.into(),
                    name: format!("table{relation}"),
                    columns: BindingColumns::Derived(vec![Column {
                        id: ColumnId {
                            relation: relation.into(),
                            position: Some(0),
                        },
                        name: "key".to_owned(),
                        nullable: true,
                        affinity: Affinity::Integer,
                        collation: CollationSeq::Binary,
                    }]),
                })
                .collect(),
            shared_inputs: Vec::new(),
            outer_columns: Vec::new(),
            parameters: Vec::new(),
        }
    }

    pub(super) fn column(relation: usize, scope: Scope) -> Scalar {
        Scalar {
            expr: Expr::Column {
                database: None,
                table: relation.into(),
                column: 0,
                is_rowid_alias: false,
            },
            references: smallvec::smallvec![ColumnReference {
                column: ColumnId {
                    relation: relation.into(),
                    position: Some(0)
                },
                scope,
            }],
            affinity: Affinity::Integer,
            collation: CollationSeq::Binary,
            nullable: true,
            can_fail: false,
            volatile: false,
        }
    }
}
