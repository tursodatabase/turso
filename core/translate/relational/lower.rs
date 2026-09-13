use rustc_hash::FxHashMap;
use turso_parser::ast::{self, TableInternalId};

use crate::schema::Table;
use crate::sync::Arc;
use crate::translate::{
    emitter::Resolver,
    plan::{
        Distinctness, JoinInfo, JoinType, JoinedTable, NonFromClauseSubquery, Plan,
        QueryDestination, ResultSetColumn, SelectPlan, SubqueryState, WhereTerm,
    },
};
use crate::Result;

use super::{bind, rewrite, BindError, JoinKind, Relation, SharedInput};

pub(crate) fn rewrite_select(plan: &mut SelectPlan, resolver: &Resolver) -> Result<bool> {
    let mut logical = match bind(plan, resolver) {
        Ok(logical) => logical,
        Err(BindError::Unsupported(reason)) => {
            tracing::debug!(target: "logical_optimizer", reason, "logical binding uses a legacy path");
            return Ok(false);
        }
        Err(BindError::Error(error)) => return Err(error),
    };
    let report = rewrite::normalize(&mut logical)?;
    tracing::debug!(
        target: "logical_optimizer",
        applied = report.applied,
        dependent_filters_pulled = report.dependent_filters_pulled(),
        remaining_dependencies = logical.dependent_join_count(),
        visited = report.visited,
        exhausted = report.exhausted,
    );
    if report.applied == 0 {
        return Ok(false);
    }
    let mut context = Lowering::default();
    context.take_resources(plan, &logical.shared_inputs);
    context.lower_shared_inputs(logical.shared_inputs)?;
    context.lower(logical.root, plan)?;
    plan.phantom_params = logical.parameters;
    Ok(true)
}

#[derive(Default)]
struct Lowering {
    tables: FxHashMap<TableInternalId, JoinedTable>,
    subqueries: FxHashMap<TableInternalId, NonFromClauseSubquery>,
    shared_inputs: FxHashMap<usize, Box<Plan>>,
}

impl Lowering {
    fn take_resources(&mut self, plan: &mut SelectPlan, shared: &[SharedInput]) {
        for mut table in std::mem::take(plan.table_references.joined_tables_mut()) {
            if let Table::FromClauseSubquery(query) = &mut table.table {
                if query.requires_table_materialization() {
                    let id = query.cte_id().expect("bound shared input is a CTE");
                    let source = shared
                        .iter()
                        .find(|source| source.id == id)
                        .expect("bound shared input has a producer");
                    if source.source_binding == table.internal_id {
                        let mut inner = query.plan.clone();
                        self.take_query_resources(&mut inner, shared);
                        assert!(self.shared_inputs.insert(id, inner).is_none());
                    }
                } else {
                    self.take_query_resources(Arc::make_mut(query).plan.as_mut(), shared);
                }
            }
            assert!(self.tables.insert(table.internal_id, table).is_none());
        }
        for mut subquery in std::mem::take(&mut plan.non_from_clause_subqueries) {
            if let SubqueryState::Unevaluated { plan: Some(inner) } = &mut subquery.state {
                let Plan::Select(inner) = inner.as_mut() else {
                    unreachable!("bound EXISTS is a SELECT")
                };
                self.take_resources(inner, shared);
            }
            assert!(self
                .subqueries
                .insert(subquery.internal_id, subquery)
                .is_none());
        }
        plan.values.clear();
        plan.where_clause.clear();
        plan.order_by.clear();
        plan.limit = None;
        plan.offset = None;
        plan.distinctness = Distinctness::NonDistinct;
        plan.group_by = None;
        plan.aggregates.clear();
        plan.simple_aggregate = None;
        plan.join_order.clear();
        plan.contains_constant_false_condition = false;
    }

    fn take_query_resources(&mut self, plan: &mut Plan, shared: &[SharedInput]) {
        match plan {
            Plan::Select(plan) => self.take_resources(plan, shared),
            Plan::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                for (plan, _) in left {
                    self.take_resources(plan, shared);
                }
                self.take_resources(right_most, shared);
                *limit = None;
                *offset = None;
                *order_by = None;
            }
            Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
                unreachable!("logical query has an executable SELECT or compound plan")
            }
        }
    }

    fn lower_shared_inputs(&mut self, inputs: Vec<SharedInput>) -> Result<()> {
        for input in inputs {
            let mut plan = self
                .shared_inputs
                .remove(&input.id)
                .expect("bound shared producer has a SELECT plan");
            self.lower_query(input.input, &mut plan)?;
            assert!(self.shared_inputs.insert(input.id, plan).is_none());
        }
        Ok(())
    }

    fn lower_query(&mut self, mut relation: Relation, plan: &mut Plan) -> Result<()> {
        let Plan::CompoundSelect {
            left,
            right_most,
            limit,
            offset,
            order_by,
        } = plan
        else {
            let Plan::Select(plan) = plan else {
                unreachable!("logical query has an executable SELECT or compound plan")
            };
            return self.lower(relation, plan);
        };
        loop {
            relation = match relation {
                Relation::Limit {
                    input,
                    limit: logical_limit,
                    offset: logical_offset,
                } => {
                    *limit = logical_limit.map(|value| Box::new(value.into_ast()));
                    *offset = logical_offset.map(|value| Box::new(value.into_ast()));
                    *input
                }
                Relation::Sort { input, keys } => {
                    let outputs = super::binding::query_output_ids(&input);
                    *order_by = Some(
                        keys.into_iter()
                            .map(|(expression, direction, nulls)| {
                                let (position, collation) =
                                    expression.compound_order_column(&outputs)?;
                                Ok((position, direction, nulls, collation))
                            })
                            .collect::<Result<_>>()?,
                    );
                    *input
                }
                input => {
                    relation = input;
                    break;
                }
            };
        }
        let mut inputs = Vec::with_capacity(left.len());
        while let Relation::Set {
            left,
            right,
            operation,
        } = relation
        {
            inputs.push((*right, operation.operator));
            relation = *left;
        }
        assert_eq!(
            inputs.len(),
            left.len(),
            "compound lowering preserves its input count"
        );
        for ((plan, operator), (next, logical_operator)) in
            left.iter_mut().zip(inputs.into_iter().rev())
        {
            self.lower(relation, plan)?;
            *operator = logical_operator;
            relation = next;
        }
        self.lower(relation, right_most)
    }

    fn lower(&mut self, relation: Relation, plan: &mut SelectPlan) -> Result<()> {
        match relation {
            Relation::OneRow => {}
            Relation::Values(values) => values.lower(plan),
            Relation::Scan(id) => {
                let table = self.tables.remove(&id).expect("validated scan binding");
                plan.table_references.add_joined_table(table);
            }
            Relation::SharedRef { binding, input } => {
                let mut table = self
                    .tables
                    .remove(&binding)
                    .expect("validated shared binding");
                let Table::FromClauseSubquery(query) = &mut table.table else {
                    unreachable!("shared binding has a subquery")
                };
                let source = self
                    .shared_inputs
                    .get(&input)
                    .expect("shared producer is lowered before its references");
                Arc::make_mut(query).plan.clone_from(source);
                plan.table_references.add_joined_table(table);
            }
            Relation::Subquery { binding, input, .. } => {
                let table = self.lower_subquery(binding, *input)?;
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
                        contains_aggregates: output.contains_aggregates,
                    })
                    .collect();
            }
            Relation::Distinct { input } => {
                self.lower(*input, plan)?;
                plan.distinctness = Distinctness::Distinct { ctx: None };
            }
            Relation::Aggregate { input, aggregation } => {
                self.lower(*input, plan)?;
                aggregation.lower(plan);
            }
            Relation::Set { .. } => {
                unreachable!("set operators are lowered through a compound query")
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
                let projected = match input.as_ref() {
                    Relation::Distinct { input } => Some(super::binding::select_outputs(input)),
                    Relation::Aggregate { aggregation, .. } => Some(aggregation.outputs.as_slice()),
                    _ => None,
                };
                let keys = if let Some(outputs) = projected {
                    keys.into_iter()
                        .map(|(expr, order, nulls)| {
                            Ok((
                                Box::new(expr.into_ast_with_project_outputs(outputs)?),
                                order,
                                nulls,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    keys.into_iter()
                        .map(|(expr, order, nulls)| (Box::new(expr.into_ast()), order, nulls))
                        .collect()
                };
                self.lower(*input, plan)?;
                plan.order_by = keys;
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

    fn lower_subquery(&mut self, binding: TableInternalId, input: Relation) -> Result<JoinedTable> {
        if let Some(mut table) = self.tables.remove(&binding) {
            let Table::FromClauseSubquery(query) = &mut table.table else {
                unreachable!("derived binding has a subquery")
            };
            let inner = Arc::get_mut(query)
                .expect("lowering owns the derived input")
                .plan
                .as_mut();
            self.lower_query(input, inner)?;
            return Ok(table);
        }
        let subquery = self
            .subqueries
            .remove(&binding)
            .expect("rewritten input has an EXISTS plan");
        let SubqueryState::Unevaluated { plan: Some(inner) } = subquery.state else {
            unreachable!("rewritten input has not been emitted")
        };
        let Plan::Select(mut inner) = *inner else {
            unreachable!("rewritten joined input is a SELECT")
        };
        self.lower(input, &mut inner)?;
        inner.query_destination = QueryDestination::placeholder_for_subquery();
        inner.table_references.clear_outer_query_refs();
        inner.input_cardinality_hint = None;
        inner.estimated_output_rows = None;
        inner.estimated_cost = None;
        let mut table =
            JoinedTable::new_subquery(format!("exists_input_{binding}"), *inner, None, binding)?;
        for column in 0..table.columns().len() {
            table.mark_column_used(column);
        }
        Ok(table)
    }
}
