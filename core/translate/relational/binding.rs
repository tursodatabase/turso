use turso_parser::ast::{self, Expr, TableInternalId};

use crate::schema::Table;
use crate::translate::{
    emitter::Resolver,
    expr::{walk_expr, WalkControl},
    plan::{Distinctness, JoinType, JoinedTable, Plan, SelectPlan, SubqueryState, TableReferences},
};
use crate::{LimboError, Result};

use super::{
    aggregation::{self, Aggregation},
    Binding, BindingColumns, Column, ColumnId, JoinKind, LogicalPlan, Output, Relation, Scalar,
    SharedInput, Values,
};

mod compound;
mod scalar;

#[derive(Debug)]
pub(crate) enum BindError {
    Unsupported(&'static str),
    Error(LimboError),
}

impl From<LimboError> for BindError {
    fn from(error: LimboError) -> Self {
        Self::Error(error)
    }
}

pub(crate) fn bind(
    plan: &SelectPlan,
    resolver: &Resolver,
) -> std::result::Result<LogicalPlan, BindError> {
    let mut builder = Builder::new(resolver);
    let root = builder.select(plan, false)?;
    finish(builder, root, &plan.table_references)
}

pub(super) fn bind_query(
    plan: &Plan,
    resolver: &Resolver,
) -> std::result::Result<LogicalPlan, BindError> {
    if let Plan::Select(plan) = plan {
        return bind(plan, resolver);
    }
    let mut builder = Builder::new(resolver);
    builder.next_output = Some(highest_query_relation_id(plan) + 1);
    let root = builder.query(plan)?;
    finish(builder, root, plan.select_table_references())
}

fn finish(
    builder: Builder,
    root: Relation,
    tables: &TableReferences,
) -> std::result::Result<LogicalPlan, BindError> {
    let outer_columns = tables
        .outer_query_refs()
        .iter()
        .filter(|outer| !outer.cte_definition_only)
        .flat_map(|outer| {
            (0..outer.columns().len())
                .map(Some)
                .chain(std::iter::once(None))
                .map(|position| ColumnId {
                    relation: outer.internal_id,
                    position,
                })
        })
        .collect();
    let logical = LogicalPlan {
        root,
        bindings: builder.bindings,
        shared_inputs: builder.shared_inputs,
        outer_columns,
        parameters: builder.parameters,
    };
    #[cfg(debug_assertions)]
    logical.validate()?;
    Ok(logical)
}

struct Builder<'a, 'r> {
    resolver: &'a Resolver<'r>,
    bindings: Vec<Binding>,
    shared_inputs: Vec<SharedInput>,
    parameters: Vec<ast::Variable>,
    next_output: Option<usize>,
}

impl<'a, 'r> Builder<'a, 'r> {
    fn new(resolver: &'a Resolver<'r>) -> Self {
        Self {
            resolver,
            bindings: Vec::new(),
            shared_inputs: Vec::new(),
            parameters: Vec::new(),
            next_output: None,
        }
    }

    fn query(&mut self, plan: &Plan) -> std::result::Result<Relation, BindError> {
        match plan {
            Plan::Select(plan) => self.select(plan, false),
            Plan::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => self.compound(
                left,
                right_most,
                limit.as_deref(),
                offset.as_deref(),
                order_by.as_deref(),
            ),
            Plan::RecursiveCte(_) => Err(BindError::Unsupported("recursive CTE lowering")),
            Plan::Delete(_) | Plan::Update(_) => {
                Err(BindError::Unsupported("DML read scope lowering"))
            }
        }
    }

    fn select(
        &mut self,
        plan: &SelectPlan,
        exists: bool,
    ) -> std::result::Result<Relation, BindError> {
        if !plan.values.is_empty() {
            return self.values(plan, exists);
        }
        let aggregate = plan.group_by.is_some() || !plan.aggregates.is_empty();
        if plan.window.is_some() {
            return Err(BindError::Unsupported("window lowering"));
        }
        if plan
            .table_references
            .joined_tables()
            .iter()
            .any(|table| table.join_info.as_ref().is_some_and(|join| join.is_outer()))
        {
            return Err(BindError::Unsupported("outer join lowering"));
        }
        let distinct = !matches!(plan.distinctness, Distinctness::NonDistinct);
        if distinct && exists {
            return Err(BindError::Unsupported("DISTINCT EXISTS output mapping"));
        }
        if plan
            .non_from_clause_subqueries
            .iter()
            .any(|subquery| matches!(subquery.query_type, ast::SubqueryType::RowValue { .. }))
            && (exists || aggregate)
        {
            return Err(BindError::Unsupported(
                "scalar results across grouping or EXISTS",
            ));
        }
        if self.next_output.is_none() {
            self.next_output = Some(highest_relation_id(plan) + 1);
        }
        self.parameters.extend(plan.phantom_params.iter().cloned());

        let tables = &plan.table_references;
        let mut predicates = Vec::new();
        let mut dependent = Vec::new();
        for term in &plan.where_clause {
            if term.from_outer_join.is_some() {
                return Err(BindError::Unsupported("outer join predicate"));
            }
            if let Some((id, kind)) = exists_filter(&term.expr) {
                let subquery = plan
                    .non_from_clause_subqueries
                    .iter()
                    .find(|subquery| subquery.internal_id == id)
                    .ok_or_else(|| super::invalid("EXISTS result has no subquery"))?;
                let SubqueryState::Unevaluated { plan: Some(inner) } = &subquery.state else {
                    return Err(BindError::Unsupported("already emitted subquery"));
                };
                let Plan::Select(inner) = inner.as_ref() else {
                    return Err(BindError::Unsupported("compound EXISTS lowering"));
                };
                dependent.push(Relation::DependentJoin {
                    left: Box::new(Relation::OneRow),
                    right: Box::new(self.select(inner, true)?),
                    kind,
                    subquery: id,
                });
            } else if let Expr::SubqueryResult {
                subquery_id,
                lhs: Some(lhs),
                not_in,
                query_type: ast::SubqueryType::In { .. },
            } = &term.expr
            {
                let subquery = plan
                    .non_from_clause_subqueries
                    .iter()
                    .find(|subquery| subquery.internal_id == *subquery_id)
                    .ok_or_else(|| super::invalid("IN result has no subquery"))?;
                let SubqueryState::Unevaluated { plan: Some(inner) } = &subquery.state else {
                    return Err(BindError::Unsupported("already emitted subquery"));
                };
                dependent.push(Relation::Membership {
                    left: Box::new(Relation::OneRow),
                    right: Box::new(self.query(inner)?),
                    lhs: membership_lhs(lhs, tables, self.resolver, inner)?,
                    negated: *not_in,
                    subquery: *subquery_id,
                });
            } else {
                predicates.push(Scalar::bind(term.expr.clone(), tables, self.resolver)?);
            }
        }
        let scalar_inputs = scalar::inputs(self, plan)?;
        if dependent.len() + scalar_inputs.len() != plan.non_from_clause_subqueries.len() {
            return Err(BindError::Unsupported(
                "subquery outside a direct EXISTS or membership filter",
            ));
        }

        let mut input = Relation::OneRow;
        for (index, table) in tables.joined_tables().iter().enumerate() {
            let scan = self.table(table)?;
            if index == 0 {
                input = scan;
            } else {
                let kind = match table.join_info.as_ref().map(|join| join.join_type) {
                    Some(JoinType::Semi) => JoinKind::Semi,
                    Some(JoinType::Anti) => JoinKind::Anti,
                    _ => JoinKind::Inner,
                };
                let mut on = Vec::new();
                if kind != JoinKind::Inner {
                    let mut rest = Vec::new();
                    for predicate in predicates {
                        if predicate
                            .references
                            .iter()
                            .any(|reference| reference.column.relation == table.internal_id)
                        {
                            on.push(predicate);
                        } else {
                            rest.push(predicate);
                        }
                    }
                    predicates = rest;
                }
                input = Relation::Join {
                    left: Box::new(input),
                    right: Box::new(scan),
                    kind,
                    predicates: on,
                };
            }
        }
        if !predicates.is_empty() {
            input = Relation::Filter {
                input: Box::new(input),
                predicates,
            };
        }
        for mut filter in dependent {
            let (Relation::DependentJoin { left, .. } | Relation::Membership { left, .. }) =
                &mut filter
            else {
                unreachable!("dependent filter has a left input")
            };
            **left = input;
            input = filter;
        }

        if exists {
            for output in &plan.result_columns {
                walk_expr(&output.expr, &mut |expr| -> Result<WalkControl> {
                    if let Expr::Variable(variable) = expr {
                        self.parameters.push(variable.clone());
                    }
                    Ok(WalkControl::Continue)
                })?;
            }
        }
        let mut keys = Vec::new();
        if !plan.order_by.is_empty() {
            keys = plan
                .order_by
                .iter()
                .map(|(expr, order, nulls)| {
                    Ok((
                        Scalar::bind_with_aggregates(
                            *expr.clone(),
                            tables,
                            self.resolver,
                            &plan.aggregates,
                        )?,
                        *order,
                        *nulls,
                    ))
                })
                .collect::<std::result::Result<_, BindError>>()?;
        } else if let Some(group) = &plan.group_by {
            if group.sort_order.contains(&ast::SortOrder::Desc)
                || group.nulls_order.iter().any(Option::is_some)
            {
                assert_eq!(group.exprs.len(), group.sort_order.len());
                assert_eq!(group.exprs.len(), group.nulls_order.len());
                keys = group
                    .exprs
                    .iter()
                    .zip(&group.sort_order)
                    .zip(&group.nulls_order)
                    .map(|((expr, order), nulls)| {
                        Ok((
                            Scalar::bind(expr.clone(), tables, self.resolver)?,
                            *order,
                            *nulls,
                        ))
                    })
                    .collect::<std::result::Result<_, BindError>>()?;
            }
        }
        if !distinct && !aggregate && !keys.is_empty() {
            input = Relation::Sort {
                input: Box::new(input),
                keys: std::mem::take(&mut keys),
            };
        }
        if !distinct && !aggregate && (plan.limit.is_some() || plan.offset.is_some()) {
            input = Relation::Limit {
                input: Box::new(input),
                limit: bind_optional(plan.limit.as_deref(), tables, self.resolver)?,
                offset: bind_optional(plan.offset.as_deref(), tables, self.resolver)?,
            };
        }
        if exists && !aggregate {
            return Ok(input);
        }
        let next_output = self
            .next_output
            .as_mut()
            .expect("SELECT initialized output identities");
        let result_relation = (*next_output).into();
        *next_output += 1;
        let mut outputs = Vec::with_capacity(plan.result_columns.len());
        for (position, output) in plan.result_columns.iter().enumerate() {
            if !aggregate && output.contains_aggregates {
                return Err(BindError::Unsupported("outer aggregate result lowering"));
            }
            let mut expr = if let Some(id) = scalar::result_id(&output.expr) {
                let input = scalar_inputs
                    .iter()
                    .find(|input| input.id == id)
                    .expect("scalar projection has a bound input");
                Scalar::result_column(&input.column, None, input.column.collation)
            } else {
                Scalar::bind_with_aggregates(
                    output.expr.clone(),
                    tables,
                    self.resolver,
                    &plan.aggregates,
                )?
            };
            if !plan.order_by.is_empty()
                || !keys.is_empty()
                || plan.limit.is_some()
                || plan.offset.is_some()
            {
                let can_reorder = if aggregate {
                    aggregation::output_can_reorder(&output.expr, plan, self.resolver)?
                } else {
                    expr.can_reorder()
                };
                if !can_reorder {
                    return Err(BindError::Unsupported(
                        "effectful projection across sort or limit",
                    ));
                }
            }
            if aggregate
                && plan
                    .group_by
                    .as_ref()
                    .is_none_or(|group| group.exprs.is_empty())
            {
                expr.nullable = true;
            }
            outputs.push(Output {
                column: Column {
                    id: ColumnId {
                        relation: result_relation,
                        position: Some(position),
                    },
                    name: output.name_or_expr(tables),
                    nullable: expr.nullable,
                    affinity: expr.affinity,
                    collation: expr.collation,
                },
                expr,
                alias: output.alias.clone(),
                implicit_name: output.implicit_column_name.clone(),
                contains_aggregates: output.contains_aggregates,
            });
        }
        if distinct && !aggregate {
            for (key, _, _) in &mut keys {
                key.project_order_columns(&outputs)?;
            }
        }
        if aggregate {
            for (key, _, _) in &mut keys {
                key.project_aggregate_order_columns(&outputs)?;
            }
        }
        for scalar in scalar_inputs {
            input = Relation::ScalarJoin {
                left: Box::new(input),
                right: Box::new(scalar.query),
                subquery: scalar.id,
                column: scalar.column,
            };
        }
        let mut result = if aggregate {
            Relation::Aggregate {
                input: Box::new(input),
                aggregation: Box::new(Aggregation::bind(plan, self.resolver, outputs)?),
            }
        } else {
            Relation::Project {
                input: Box::new(input),
                outputs,
            }
        };
        if distinct {
            result = Relation::Distinct {
                input: Box::new(result),
            };
        }
        if distinct || aggregate {
            if !keys.is_empty() {
                result = Relation::Sort {
                    input: Box::new(result),
                    keys,
                };
            }
            if plan.limit.is_some() || plan.offset.is_some() {
                result = Relation::Limit {
                    input: Box::new(result),
                    limit: bind_optional(plan.limit.as_deref(), tables, self.resolver)?,
                    offset: bind_optional(plan.offset.as_deref(), tables, self.resolver)?,
                };
            }
        }
        Ok(result)
    }

    fn values(
        &mut self,
        plan: &SelectPlan,
        exists: bool,
    ) -> std::result::Result<Relation, BindError> {
        if exists || !plan.non_from_clause_subqueries.is_empty() {
            return Err(BindError::Unsupported("VALUES subquery result lowering"));
        }
        assert!(plan.table_references.joined_tables().is_empty());
        assert!(plan.where_clause.is_empty());
        assert!(plan.group_by.is_none() && plan.aggregates.is_empty());
        assert!(plan.order_by.is_empty() && plan.window.is_none());
        assert!(matches!(plan.distinctness, Distinctness::NonDistinct));
        let next_output = self
            .next_output
            .get_or_insert_with(|| highest_relation_id(plan) + 1);
        let output = (*next_output).into();
        *next_output += 1;
        self.parameters.extend(plan.phantom_params.iter().cloned());
        let mut input = Relation::Values(Box::new(Values::bind(plan, self.resolver, output)?));
        if plan.limit.is_some() || plan.offset.is_some() {
            input = Relation::Limit {
                input: Box::new(input),
                limit: bind_optional(plan.limit.as_deref(), &plan.table_references, self.resolver)?,
                offset: bind_optional(
                    plan.offset.as_deref(),
                    &plan.table_references,
                    self.resolver,
                )?,
            };
        }
        Ok(input)
    }

    fn table(&mut self, table: &JoinedTable) -> std::result::Result<Relation, BindError> {
        if self
            .bindings
            .iter()
            .any(|binding| binding.id == table.internal_id)
        {
            return Err(BindError::Unsupported(
                "shared CTE template needs fresh relation identities",
            ));
        }
        let (columns, relation) = match &table.table {
            Table::BTree(btree) => (
                BindingColumns::Catalog(btree.clone()),
                Relation::Scan(table.internal_id),
            ),
            Table::FromClauseSubquery(query) if query.requires_table_materialization() => {
                let id = query
                    .cte_id()
                    .ok_or(BindError::Unsupported("non-CTE materialized input"))?;
                if !self.shared_inputs.iter().any(|input| input.id == id) {
                    if query_has_outer_dependencies(&query.plan) {
                        return Err(BindError::Unsupported(
                            "shared input in an outer query scope",
                        ));
                    }
                    let input = self.query(&query.plan)?;
                    let columns = query_output_ids(&input);
                    self.shared_inputs.push(SharedInput {
                        id,
                        source_binding: table.internal_id,
                        input,
                        columns,
                    });
                }
                (
                    BindingColumns::Derived(derived_columns(table)),
                    Relation::SharedRef {
                        binding: table.internal_id,
                        input: id,
                    },
                )
            }
            Table::FromClauseSubquery(query) => {
                let input = self.query(&query.plan)?;
                let columns = query_output_ids(&input);
                (
                    BindingColumns::Derived(derived_columns(table)),
                    Relation::Subquery {
                        binding: table.internal_id,
                        input: Box::new(input),
                        columns,
                    },
                )
            }
            _ => {
                return Err(BindError::Unsupported(
                    "recursive or virtual input lowering",
                ))
            }
        };
        self.bindings.push(Binding {
            id: table.internal_id,
            name: table.identifier.clone(),
            columns,
        });
        Ok(relation)
    }
}

fn query_has_outer_dependencies(plan: &Plan) -> bool {
    match plan {
        Plan::Select(plan) => plan
            .table_references
            .outer_query_refs()
            .iter()
            .any(|outer| !outer.cte_definition_only && outer.is_used()),
        Plan::CompoundSelect {
            left, right_most, ..
        } => left
            .iter()
            .map(|(plan, _)| plan)
            .chain(std::iter::once(right_most.as_ref()))
            .any(|plan| {
                plan.table_references
                    .outer_query_refs()
                    .iter()
                    .any(|outer| !outer.cte_definition_only && outer.is_used())
            }),
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => false,
    }
}

pub(super) fn query_output_ids(relation: &Relation) -> Vec<ColumnId> {
    match relation {
        Relation::Values(values) => values.columns.iter().map(|column| column.id).collect(),
        Relation::Set { operation, .. } => {
            operation.outputs.iter().map(|column| column.id).collect()
        }
        Relation::Sort { input, .. } | Relation::Limit { input, .. } => query_output_ids(input),
        _ => select_outputs(relation)
            .iter()
            .map(|output| output.column.id)
            .collect(),
    }
}

pub(super) fn query_column(relation: &Relation, position: usize) -> &Column {
    match relation {
        Relation::Values(values) => &values.columns[position],
        Relation::Set { operation, .. } => &operation.outputs[position],
        Relation::Project { outputs, .. } => &outputs[position].column,
        Relation::Aggregate { aggregation, .. } => &aggregation.outputs[position].column,
        Relation::Sort { input, .. }
        | Relation::Limit { input, .. }
        | Relation::Distinct { input } => query_column(input, position),
        _ => unreachable!("query has explicit outputs"),
    }
}

pub(super) fn select_outputs(relation: &Relation) -> &[Output] {
    match relation {
        Relation::Project { outputs, .. } => outputs,
        Relation::Aggregate { aggregation, .. } => &aggregation.outputs,
        Relation::Distinct { input }
        | Relation::Sort { input, .. }
        | Relation::Limit { input, .. } => select_outputs(input),
        _ => unreachable!("SELECT has a projection"),
    }
}

fn derived_columns(table: &JoinedTable) -> Vec<Column> {
    table
        .columns()
        .iter()
        .enumerate()
        .map(|(position, column)| Column {
            id: ColumnId {
                relation: table.internal_id,
                position: Some(position),
            },
            name: column.name.clone().unwrap_or_default(),
            nullable: true,
            affinity: column.affinity(),
            collation: column.collation(),
        })
        .collect()
}

fn exists_filter(expr: &Expr) -> Option<(TableInternalId, JoinKind)> {
    match expr {
        Expr::SubqueryResult {
            subquery_id,
            query_type: ast::SubqueryType::Exists { .. },
            ..
        } => Some((*subquery_id, JoinKind::Semi)),
        Expr::Unary(ast::UnaryOperator::Not, expr) => match expr.as_ref() {
            Expr::SubqueryResult {
                subquery_id,
                query_type: ast::SubqueryType::Exists { .. },
                ..
            } => Some((*subquery_id, JoinKind::Anti)),
            _ => None,
        },
        _ => None,
    }
}

fn membership_lhs(
    expr: &Expr,
    tables: &TableReferences,
    resolver: &Resolver,
    inner: &Plan,
) -> std::result::Result<Vec<Scalar>, BindError> {
    let expressions = match expr {
        Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            return membership_lhs(&exprs[0], tables, resolver, inner);
        }
        Expr::Parenthesized(exprs) => exprs.iter().map(|expr| expr.as_ref()).collect::<Vec<_>>(),
        expr => vec![expr],
    };
    let Some(crate::translate::plan::QueryDestination::EphemeralIndex { index, .. }) =
        inner.select_query_destination()
    else {
        return Err(BindError::Unsupported(
            "membership comparison metadata is unavailable",
        ));
    };
    assert_eq!(expressions.len(), index.columns.len());
    expressions
        .into_iter()
        .zip(&index.columns)
        .map(|(expr, column)| {
            Scalar::bind(
                Expr::Collate(
                    Box::new(expr.clone()),
                    ast::Name::exact(column.collation.unwrap_or_default().name()),
                ),
                tables,
                resolver,
            )
        })
        .collect()
}

fn bind_optional(
    expr: Option<&Expr>,
    tables: &TableReferences,
    resolver: &Resolver,
) -> std::result::Result<Option<Box<Scalar>>, BindError> {
    expr.map(|expr| Scalar::bind(expr.clone(), tables, resolver).map(Box::new))
        .transpose()
}

fn highest_relation_id(plan: &SelectPlan) -> usize {
    plan.table_references
        .joined_tables()
        .iter()
        .flat_map(|table| {
            let nested = match &table.table {
                Table::FromClauseSubquery(query) => highest_query_relation_id(&query.plan),
                _ => 0,
            };
            [usize::from(table.internal_id), nested]
        })
        .chain(
            plan.table_references
                .outer_query_refs()
                .iter()
                .map(|table| usize::from(table.internal_id)),
        )
        .chain(plan.non_from_clause_subqueries.iter().flat_map(|subquery| {
            let child = match &subquery.state {
                SubqueryState::Unevaluated { plan: Some(plan) } => highest_query_relation_id(plan),
                _ => 0,
            };
            [usize::from(subquery.internal_id), child]
        }))
        .max()
        .unwrap_or(0)
}

fn highest_query_relation_id(plan: &Plan) -> usize {
    match plan {
        Plan::Select(plan) => highest_relation_id(plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => left
            .iter()
            .map(|(plan, _)| highest_relation_id(plan))
            .chain(std::iter::once(highest_relation_id(right_most)))
            .max()
            .expect("compound query has a final input"),
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => 0,
    }
}
