use turso_parser::ast::{Expr, Literal, SortOrder};

use crate::function::AggFunc;
use crate::translate::{
    emitter::Resolver,
    expr::{walk_expr_mut, WalkControl},
    plan::{Aggregate, Distinctness, GroupBy, ResultSetColumn, SelectPlan},
};
use crate::Result;

use super::{validate_scalar, BindError, Output, Properties, Scalar};

#[derive(Clone, Debug)]
pub(crate) struct Aggregation {
    pub keys: Option<Vec<Scalar>>,
    pub having: Option<Vec<Scalar>>,
    pub functions: Vec<AggregateFunction>,
    pub outputs: Vec<Output>,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateFunction {
    pub func: AggFunc,
    pub args: Vec<Scalar>,
    pub expr: Scalar,
    pub distinct: bool,
    pub filter: Option<Scalar>,
}

impl Aggregation {
    pub(super) fn bind(
        plan: &SelectPlan,
        resolver: &Resolver,
        outputs: Vec<Output>,
    ) -> std::result::Result<Self, BindError> {
        let bind = |expr: &Expr| {
            Scalar::bind_with_aggregates(
                expr.clone(),
                &plan.table_references,
                resolver,
                &plan.aggregates,
            )
        };
        let keys = plan
            .group_by
            .as_ref()
            .map(|group| group.exprs.iter().map(bind).collect())
            .transpose()?;
        let having = plan
            .group_by
            .as_ref()
            .and_then(|group| group.having.as_ref())
            .map(|predicates| predicates.iter().map(bind).collect())
            .transpose()?;
        let functions = plan
            .aggregates
            .iter()
            .map(|function| {
                Ok(AggregateFunction {
                    func: function.func.clone(),
                    args: function
                        .args
                        .iter()
                        .map(bind)
                        .collect::<std::result::Result<_, BindError>>()?,
                    expr: bind(&function.original_expr)?,
                    distinct: function.is_distinct(),
                    filter: function.filter_expr.as_ref().map(bind).transpose()?,
                })
            })
            .collect::<std::result::Result<_, BindError>>()?;
        Ok(Self {
            keys,
            having,
            functions,
            outputs,
        })
    }

    pub(super) fn validate(&self, input: &mut Properties) -> Result<()> {
        for expression in self
            .keys
            .iter()
            .flatten()
            .chain(self.having.iter().flatten())
        {
            validate_scalar(expression, input, None)?;
        }
        for function in &self.functions {
            validate_scalar(&function.expr, input, None)?;
            for expression in function.args.iter().chain(function.filter.iter()) {
                validate_scalar(expression, input, None)?;
            }
        }
        for output in &self.outputs {
            validate_scalar(&output.expr, input, None)?;
        }
        Ok(())
    }

    pub(super) fn lower(self, plan: &mut SelectPlan) {
        plan.group_by = self.keys.map(|keys| GroupBy {
            sort_order: vec![SortOrder::Asc; keys.len()],
            nulls_order: vec![None; keys.len()],
            exprs: keys.into_iter().map(Scalar::into_ast).collect(),
            sort_elided: false,
            having: self
                .having
                .map(|predicates| predicates.into_iter().map(Scalar::into_ast).collect()),
        });
        plan.aggregates = self
            .functions
            .into_iter()
            .map(|function| Aggregate {
                func: function.func,
                args: function.args.into_iter().map(Scalar::into_ast).collect(),
                original_expr: function.expr.into_ast(),
                distinctness: if function.distinct {
                    Distinctness::Distinct { ctx: None }
                } else {
                    Distinctness::NonDistinct
                },
                filter_expr: function.filter.map(Scalar::into_ast),
                fraction_reg: None,
            })
            .collect();
        plan.result_columns = self
            .outputs
            .into_iter()
            .map(|output| ResultSetColumn {
                expr: output.expr.into_ast(),
                alias: output.alias,
                implicit_column_name: output.implicit_name,
                contains_aggregates: output.contains_aggregates,
            })
            .collect();
    }
}

pub(super) fn output_can_reorder(
    expr: &Expr,
    plan: &SelectPlan,
    resolver: &Resolver,
) -> std::result::Result<bool, BindError> {
    let mut expr = expr.clone();
    walk_expr_mut(&mut expr, &mut |expr| {
        if plan
            .aggregates
            .iter()
            .any(|aggregate| aggregate.original_expr == *expr)
        {
            *expr = Expr::Literal(Literal::Null);
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(Scalar::bind(expr, &plan.table_references, resolver)?.can_reorder())
}
