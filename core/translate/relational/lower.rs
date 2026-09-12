use rustc_hash::FxHashMap;
use turso_parser::ast::{self, TableInternalId};

use crate::translate::{
    emitter::Resolver,
    plan::{
        JoinInfo, JoinType, JoinedTable, NonFromClauseSubquery, Plan, ResultSetColumn, SelectPlan,
        SubqueryState, WhereTerm,
    },
};
use crate::Result;

use super::{bind, rewrite, BindError, JoinKind, Relation};

pub(crate) fn rewrite_select(plan: &mut SelectPlan, resolver: &Resolver) -> Result<bool> {
    let mut logical = match bind(plan, resolver) {
        Ok(logical) => logical,
        Err(BindError::Unsupported(_)) => return Ok(false),
        Err(BindError::Error(error)) => return Err(error),
    };
    let report = rewrite::normalize(&mut logical)?;
    if report.applied == 0 {
        return Ok(false);
    }
    let mut context = Lowering::default();
    context.take_resources(plan);
    context.lower(logical.root, plan)?;
    plan.phantom_params = logical.parameters;
    tracing::debug!(target: "logical_optimizer", rule = "pull_dependent_filter", applied = report.applied, visited = report.visited, exhausted = report.exhausted);
    Ok(true)
}

#[derive(Default)]
struct Lowering {
    tables: FxHashMap<TableInternalId, JoinedTable>,
    subqueries: FxHashMap<TableInternalId, NonFromClauseSubquery>,
}

impl Lowering {
    fn take_resources(&mut self, plan: &mut SelectPlan) {
        for table in std::mem::take(plan.table_references.joined_tables_mut()) {
            assert!(self.tables.insert(table.internal_id, table).is_none());
        }
        for mut subquery in std::mem::take(&mut plan.non_from_clause_subqueries) {
            if let SubqueryState::Unevaluated { plan: Some(inner) } = &mut subquery.state {
                let Plan::Select(inner) = inner.as_mut() else {
                    unreachable!("bound EXISTS is a SELECT")
                };
                self.take_resources(inner);
            }
            assert!(self
                .subqueries
                .insert(subquery.internal_id, subquery)
                .is_none());
        }
        plan.where_clause.clear();
        plan.order_by.clear();
        plan.limit = None;
        plan.offset = None;
        plan.join_order.clear();
        plan.contains_constant_false_condition = false;
    }

    fn lower(&mut self, relation: Relation, plan: &mut SelectPlan) -> Result<()> {
        match relation {
            Relation::OneRow => {}
            Relation::Scan(id) | Relation::SharedRef { binding: id, .. } => {
                let table = self.tables.remove(&id).expect("validated scan binding");
                plan.table_references.add_joined_table(table);
            }
            Relation::Filter { input, predicates } => {
                self.lower(*input, plan)?;
                plan.where_clause
                    .extend(predicates.into_iter().map(|expr| WhereTerm {
                        expr: expr.into_ast(),
                        from_outer_join: None,
                        consumed: false,
                    }));
            }
            Relation::Project { input, outputs } => {
                self.lower(*input, plan)?;
                plan.result_columns = outputs
                    .into_iter()
                    .map(|output| ResultSetColumn {
                        expr: output.expr.into_ast(),
                        alias: output.alias,
                        implicit_column_name: output.implicit_name,
                        contains_aggregates: false,
                    })
                    .collect();
            }
            Relation::Join {
                left,
                right,
                kind,
                predicates,
            } => {
                self.lower(*left, plan)?;
                let right_index = plan.table_references.joined_tables().len();
                self.lower(*right, plan)?;
                if kind != JoinKind::Inner {
                    assert_eq!(
                        plan.table_references.joined_tables().len(),
                        right_index + 1,
                        "semi/anti lowering requires a single right input"
                    );
                    plan.table_references.joined_tables_mut()[right_index].join_info =
                        Some(JoinInfo {
                            join_type: if kind == JoinKind::Semi {
                                JoinType::Semi
                            } else {
                                JoinType::Anti
                            },
                            using: Vec::new(),
                            no_reorder: false,
                        });
                }
                plan.where_clause
                    .extend(predicates.into_iter().map(|expr| WhereTerm {
                        expr: expr.into_ast(),
                        from_outer_join: None,
                        consumed: false,
                    }));
            }
            Relation::DependentJoin {
                left,
                right,
                kind,
                subquery,
            } => {
                self.lower(*left, plan)?;
                let mut subquery = self
                    .subqueries
                    .remove(&subquery)
                    .expect("validated dependent input");
                let SubqueryState::Unevaluated { plan: Some(inner) } = &mut subquery.state else {
                    unreachable!("bound subquery must not be emitted")
                };
                let Plan::Select(inner) = inner.as_mut() else {
                    unreachable!("bound EXISTS is a SELECT")
                };
                self.lower(*right, inner)?;
                let mut expr = ast::Expr::SubqueryResult {
                    subquery_id: subquery.internal_id,
                    lhs: None,
                    not_in: false,
                    query_type: subquery.query_type.clone(),
                };
                if kind == JoinKind::Anti {
                    expr = ast::Expr::Unary(ast::UnaryOperator::Not, Box::new(expr));
                }
                plan.where_clause.push(WhereTerm {
                    expr,
                    from_outer_join: None,
                    consumed: false,
                });
                plan.non_from_clause_subqueries.push(subquery);
            }
            Relation::Sort { input, keys } => {
                self.lower(*input, plan)?;
                plan.order_by = keys
                    .into_iter()
                    .map(|(expr, order, nulls)| (Box::new(expr.into_ast()), order, nulls))
                    .collect();
            }
            Relation::Limit {
                input,
                limit,
                offset,
            } => {
                self.lower(*input, plan)?;
                plan.limit = limit.map(|expr| Box::new(expr.into_ast()));
                plan.offset = offset.map(|expr| Box::new(expr.into_ast()));
            }
        }
        Ok(())
    }
}
