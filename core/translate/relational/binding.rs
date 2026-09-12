use turso_parser::ast::{self, Expr, TableInternalId};

use crate::schema::Table;
use crate::translate::{
    emitter::Resolver,
    expr::{walk_expr, WalkControl},
    plan::{Distinctness, JoinType, JoinedTable, Plan, SelectPlan, SubqueryState, TableReferences},
};
use crate::{LimboError, Result};

use super::{
    Binding, BindingColumns, Column, ColumnId, JoinKind, LogicalPlan, Output, Relation, Scalar,
    SharedInput,
};

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
    let mut builder = Builder {
        resolver,
        bindings: Vec::new(),
        shared_inputs: Vec::new(),
        parameters: Vec::new(),
        next_output: highest_relation_id(plan) + 1,
    };
    let root = builder.select(plan, false)?;
    let outer_columns = plan
        .table_references
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
    next_output: usize,
}

impl Builder<'_, '_> {
    fn select(
        &mut self,
        plan: &SelectPlan,
        exists: bool,
    ) -> std::result::Result<Relation, BindError> {
        self.parameters.extend(plan.phantom_params.iter().cloned());
        if plan.group_by.is_some() || !plan.aggregates.is_empty() {
            return Err(BindError::Unsupported("aggregate lowering"));
        }
        if plan.window.is_some() || !plan.values.is_empty() {
            return Err(BindError::Unsupported("window or VALUES lowering"));
        }
        if plan
            .table_references
            .joined_tables()
            .iter()
            .any(|table| table.join_info.as_ref().is_some_and(|join| join.is_outer()))
        {
            return Err(BindError::Unsupported("outer join lowering"));
        }
        if !matches!(plan.distinctness, Distinctness::NonDistinct) {
            return Err(BindError::Unsupported("DISTINCT output mapping"));
        }

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
                dependent.push((id, kind, self.select(inner, true)?));
            } else {
                predicates.push(Scalar::bind(term.expr.clone(), tables, self.resolver)?);
            }
        }
        if dependent.len() != plan.non_from_clause_subqueries.len() {
            return Err(BindError::Unsupported(
                "subquery outside a direct EXISTS filter",
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
        for (subquery, kind, right) in dependent {
            input = Relation::DependentJoin {
                left: Box::new(input),
                right: Box::new(right),
                kind,
                subquery,
            };
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
        if !plan.order_by.is_empty() {
            let keys = plan
                .order_by
                .iter()
                .map(|(expr, order, nulls)| {
                    Ok((
                        Scalar::bind(*expr.clone(), tables, self.resolver)?,
                        *order,
                        *nulls,
                    ))
                })
                .collect::<std::result::Result<_, BindError>>()?;
            input = Relation::Sort {
                input: Box::new(input),
                keys,
            };
        }
        if plan.limit.is_some() || plan.offset.is_some() {
            input = Relation::Limit {
                input: Box::new(input),
                limit: bind_optional(plan.limit.as_deref(), tables, self.resolver)?,
                offset: bind_optional(plan.offset.as_deref(), tables, self.resolver)?,
            };
        }
        if exists {
            return Ok(input);
        }
        let result_relation = self.next_output.into();
        self.next_output += 1;
        let mut outputs = Vec::with_capacity(plan.result_columns.len());
        for (position, output) in plan.result_columns.iter().enumerate() {
            let expr = Scalar::bind(output.expr.clone(), tables, self.resolver)?;
            if !expr.can_reorder()
                && (!plan.order_by.is_empty() || plan.limit.is_some() || plan.offset.is_some())
            {
                return Err(BindError::Unsupported(
                    "effectful projection across sort or limit",
                ));
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
            });
        }
        Ok(Relation::Project {
            input: Box::new(input),
            outputs,
        })
    }

    fn table(&mut self, table: &JoinedTable) -> std::result::Result<Relation, BindError> {
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
                    let Plan::Select(source) = query.plan.as_ref() else {
                        return Err(BindError::Unsupported("compound or recursive shared input"));
                    };
                    if source
                        .table_references
                        .outer_query_refs()
                        .iter()
                        .any(|outer| !outer.cte_definition_only)
                    {
                        return Err(BindError::Unsupported(
                            "shared input in an outer query scope",
                        ));
                    }
                    let input = self.select(source, false)?;
                    let Relation::Project { outputs, .. } = &input else {
                        unreachable!("SELECT has a projection")
                    };
                    let columns = outputs.iter().map(|output| output.column.id).collect();
                    self.shared_inputs.push(SharedInput { id, input, columns });
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
                let Plan::Select(source) = query.plan.as_ref() else {
                    return Err(BindError::Unsupported(
                        "compound or recursive derived input",
                    ));
                };
                let input = self.select(source, false)?;
                let Relation::Project { outputs, .. } = &input else {
                    unreachable!("SELECT has a projection")
                };
                let columns = outputs.iter().map(|output| output.column.id).collect();
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
                Table::FromClauseSubquery(query) => match query.plan.as_ref() {
                    Plan::Select(plan) => highest_relation_id(plan),
                    _ => 0,
                },
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
                SubqueryState::Unevaluated { plan: Some(plan) } => match plan.as_ref() {
                    Plan::Select(plan) => highest_relation_id(plan),
                    _ => 0,
                },
                _ => 0,
            };
            [usize::from(subquery.internal_id), child]
        }))
        .max()
        .unwrap_or(0)
}
