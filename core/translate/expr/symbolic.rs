use smallvec::SmallVec;
use turso_parser::ast::{Expr, Literal, Operator, TableInternalId};

use crate::{
    schema::BTreeTable,
    translate::{
        alter::literal_default_value,
        compiler::{
            add, compare, constant, pack_values, resolved_comparison, BoxedCompile, ComparisonOp,
            Compile, PackValues, ResolvedComparison, Row, ValueId,
        },
        emitter::Resolver,
        plan::TableReferences,
    },
    types::Value,
    LimboError, Result,
};

use super::{comparison_affinity, comparison_collation};

/// A scalar SQL expression whose runtime semantics can be represented entirely
/// by the symbolic compiler IR.
///
/// SQL-specific resolution happens before this description is constructed.
/// Compiling it later only needs the symbolic row that supplies its columns.
pub(crate) enum ResolvedScalarExpr {
    Column(usize),
    Constant(Value),
    Add(Box<Self>, Box<Self>),
    Compare {
        lhs: Box<Self>,
        rhs: Box<Self>,
        comparison: ResolvedComparison,
    },
}

/// Resolves expressions for one symbolic B-tree row without emitting bytecode.
pub(crate) struct RowExprResolver<'a, 'schema> {
    resolver: &'a Resolver<'schema>,
    database_id: usize,
    table_id: TableInternalId,
    table: &'a BTreeTable,
    referenced_tables: &'a TableReferences,
}

impl<'a, 'schema> RowExprResolver<'a, 'schema> {
    pub(crate) const fn new(
        resolver: &'a Resolver<'schema>,
        database_id: usize,
        table_id: TableInternalId,
        table: &'a BTreeTable,
        referenced_tables: &'a TableReferences,
    ) -> Self {
        Self {
            resolver,
            database_id,
            table_id,
            table,
            referenced_tables,
        }
    }

    /// Returns `None` when any part of the expression still requires the eager
    /// SQL emitter. Resolution is all-or-nothing and never mutates a program.
    pub(crate) fn resolve(&self, expr: &Expr) -> Result<Option<ResolvedScalarExpr>> {
        let expr = unwrap_single_parentheses(expr);
        match expr {
            Expr::Collate(inner, collation) => {
                self.resolver.resolve_collation(collation.as_str())?;
                self.resolve(inner)
            }
            Expr::Column { table, column, .. } => self.resolve_column(*table, *column),
            Expr::Literal(literal) => self.resolve_literal(literal),
            Expr::Binary(lhs, operator, rhs) => self.resolve_binary(lhs, *operator, rhs),
            _ => Ok(None),
        }
    }

    fn resolve_column(
        &self,
        expr_table: TableInternalId,
        column: usize,
    ) -> Result<Option<ResolvedScalarExpr>> {
        if expr_table != self.table_id {
            return Ok(None);
        }
        let column_definition = self.table.columns().get(column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "SELECT plan references column {column} outside table {}",
                self.table.name
            ))
        })?;
        let requires_frontend_decoding = column_definition.is_virtual_generated()
            || column_definition.is_array()
            || self.resolver.with_schema(self.database_id, |schema| {
                schema
                    .get_type_def_unchecked(&column_definition.ty_str)
                    .is_some()
            });
        Ok((!requires_frontend_decoding).then_some(ResolvedScalarExpr::Column(column)))
    }

    fn resolve_literal(&self, literal: &Literal) -> Result<Option<ResolvedScalarExpr>> {
        if matches!(
            literal,
            Literal::Keyword(_)
                | Literal::CurrentDate
                | Literal::CurrentTime
                | Literal::CurrentTimestamp
        ) {
            return Ok(None);
        }
        Ok(Some(ResolvedScalarExpr::Constant(literal_default_value(
            literal,
        )?)))
    }

    fn resolve_binary(
        &self,
        lhs: &Expr,
        operator: Operator,
        rhs: &Expr,
    ) -> Result<Option<ResolvedScalarExpr>> {
        let comparison_op = comparison_op(operator);
        if operator != Operator::Add && comparison_op.is_none() {
            return Ok(None);
        }
        let Some(lhs_resolved) = self.resolve(lhs)? else {
            return Ok(None);
        };
        let Some(rhs_resolved) = self.resolve(rhs)? else {
            return Ok(None);
        };

        if operator == Operator::Add {
            return Ok(Some(ResolvedScalarExpr::Add(
                Box::new(lhs_resolved),
                Box::new(rhs_resolved),
            )));
        }

        let op = comparison_op.expect("comparison operator checked above");
        let affinity =
            comparison_affinity(lhs, rhs, Some(self.referenced_tables), Some(self.resolver));
        let collation =
            comparison_collation(lhs, rhs, Some(self.referenced_tables), Some(self.resolver))?;
        Ok(Some(ResolvedScalarExpr::Compare {
            lhs: Box::new(lhs_resolved),
            rhs: Box::new(rhs_resolved),
            comparison: resolved_comparison(op, affinity, collation),
        }))
    }
}

fn unwrap_single_parentheses(mut expr: &Expr) -> &Expr {
    while let Expr::Parenthesized(expressions) = expr {
        let [inner] = expressions.as_slice() else {
            break;
        };
        expr = inner;
    }
    expr
}

const fn comparison_op(operator: Operator) -> Option<ComparisonOp> {
    match operator {
        Operator::Equals => Some(ComparisonOp::Equal),
        Operator::NotEquals => Some(ComparisonOp::NotEqual),
        Operator::Less => Some(ComparisonOp::Less),
        Operator::LessEquals => Some(ComparisonOp::LessEqual),
        Operator::Greater => Some(ComparisonOp::Greater),
        Operator::GreaterEquals => Some(ComparisonOp::GreaterEqual),
        _ => None,
    }
}

/// Builds a compiler for one resolved expression against a symbolic row.
pub(crate) fn compile_expr(row: Row, expr: &ResolvedScalarExpr) -> BoxedCompile<ValueId> {
    match expr {
        ResolvedScalarExpr::Column(column) => row.column(*column).boxed(),
        ResolvedScalarExpr::Constant(value) => constant(value.clone()).boxed(),
        ResolvedScalarExpr::Add(lhs, rhs) => compile_expr(row, lhs)
            .then(compile_expr(row, rhs))
            .and_then(|(lhs, rhs)| add(lhs, rhs))
            .boxed(),
        ResolvedScalarExpr::Compare {
            lhs,
            rhs,
            comparison,
        } => compile_expr(row, lhs)
            .then(compile_expr(row, rhs))
            .and_then({
                let comparison = *comparison;
                move |(lhs, rhs)| compare(lhs, rhs, comparison)
            })
            .boxed(),
    }
}

/// Compiles expressions in source order into one symbolic register pack.
pub(crate) fn compile_exprs(row: Row, expressions: &[ResolvedScalarExpr]) -> PackValues {
    let mut compilers = SmallVec::with_capacity(expressions.len());
    for expression in expressions {
        compilers.push(compile_expr(row, expression));
    }
    pack_values(compilers)
}

/// Compiles a WHERE-clause conjunction with SQL short-circuit truthiness.
pub(crate) fn compile_conjunction(
    row: Row,
    expressions: &[ResolvedScalarExpr],
) -> BoxedCompile<ValueId> {
    let Some((expression, remaining)) = expressions.split_first() else {
        return constant(Value::from_i64(1)).boxed();
    };
    compile_expr(row, expression)
        .branch(
            compile_conjunction(row, remaining),
            constant(Value::from_i64(0)),
        )
        .boxed()
}
