use smallvec::SmallVec;
use turso_parser::ast::{self, Expr};

use crate::translate::{
    collate::{get_collseq_from_expr_with_symbols, CollationSeq},
    emitter::Resolver,
    expr::{
        expr_contains_nondeterministic_scalar_function, expression_can_fail_on_input,
        get_expr_affinity, walk_expr, WalkControl,
    },
    plan::TableReferences,
};
use crate::vdbe::affinity::Affinity;
use crate::Result;

use super::{binding::BindError, ColumnId, ColumnReference, Scope};

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
        let mut references = SmallVec::new();
        let mut unsupported = None;
        let mut can_fail = expression_can_fail_on_input(&expr);
        walk_expr(&expr, &mut |expr| -> Result<WalkControl> {
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
                    unsupported = Some("aggregate function modifiers");
                    return Ok(WalkControl::SkipChildren);
                }
                Expr::FunctionCallStar { filter_over, .. }
                    if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() =>
                {
                    unsupported = Some("aggregate function modifiers");
                    return Ok(WalkControl::SkipChildren);
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
            volatile: expr_contains_nondeterministic_scalar_function(&expr, resolver)?,
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

    pub(crate) fn bind_outer_columns(&mut self, available: &super::ColumnSet) {
        for reference in &mut self.references {
            if available.contains(&reference.column) {
                reference.scope = Scope::Local;
            }
        }
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

    fn plan(root: Relation) -> LogicalPlan {
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

    fn column(relation: usize, scope: Scope) -> Scalar {
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
