use smallvec::SmallVec;
use turso_parser::ast::{Expr, Literal, Operator, SubqueryType, TableInternalId, Variable};

use crate::{
    schema::{BTreeTable, Index},
    translate::{
        alter::literal_default_value,
        compiler::{
            add, compare, constant, input, logical, pack_values, parameter, resolved_comparison,
            BoxedCompile, ComparisonOp, Compile, InputId, LogicalOp, PackValues,
            ResolvedComparison, Row, ValueId,
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
    Input(InputId),
    Column(usize),
    RowId,
    IndexRowId,
    Parameter(Variable),
    Constant(Value),
    Add(Box<Self>, Box<Self>),
    Logical {
        op: LogicalOp,
        lhs: Box<Self>,
        rhs: Box<Self>,
    },
    Compare {
        lhs: Box<Self>,
        rhs: Box<Self>,
        comparison: ResolvedComparison,
    },
    Case {
        when_then_pairs: Vec<(Self, Self)>,
        else_expr: Box<Self>,
    },
}

/// Physical source used to read one logical table row.
#[derive(Clone, Copy)]
pub(crate) enum RowLayout<'a> {
    Table,
    CoveringIndex(&'a Index),
}

/// Resolves expressions for one symbolic B-tree row without emitting bytecode.
pub(crate) struct RowExprResolver<'a, 'schema> {
    resolver: &'a Resolver<'schema>,
    database_id: usize,
    table_id: TableInternalId,
    table: &'a BTreeTable,
    layout: RowLayout<'a>,
    referenced_tables: &'a TableReferences,
    subquery_inputs: &'a [(TableInternalId, InputId)],
}

impl<'a, 'schema> RowExprResolver<'a, 'schema> {
    pub(crate) const fn new(
        resolver: &'a Resolver<'schema>,
        database_id: usize,
        table_id: TableInternalId,
        table: &'a BTreeTable,
        layout: RowLayout<'a>,
        referenced_tables: &'a TableReferences,
    ) -> Self {
        Self {
            resolver,
            database_id,
            table_id,
            table,
            layout,
            referenced_tables,
            subquery_inputs: &[],
        }
    }

    pub(crate) const fn with_subquery_inputs(
        mut self,
        subquery_inputs: &'a [(TableInternalId, InputId)],
    ) -> Self {
        self.subquery_inputs = subquery_inputs;
        self
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
            Expr::Column {
                table,
                column,
                is_rowid_alias,
                ..
            } => self.resolve_column(*table, *column, *is_rowid_alias),
            Expr::RowId { table, .. } => self.resolve_rowid(*table),
            Expr::Variable(variable) => Ok(Some(ResolvedScalarExpr::Parameter(variable.clone()))),
            Expr::Literal(literal) => self.resolve_literal(literal),
            Expr::SubqueryResult {
                subquery_id,
                lhs: None,
                not_in: false,
                query_type: SubqueryType::RowValue { num_regs: 1, .. },
            } => Ok(self.subquery_inputs.iter().find_map(|(candidate, input)| {
                (*candidate == *subquery_id).then_some(ResolvedScalarExpr::Input(*input))
            })),
            Expr::Binary(lhs, operator, rhs) => self.resolve_binary(lhs, *operator, rhs),
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => self.resolve_case(base.as_deref(), when_then_pairs, else_expr.as_deref()),
            _ => Ok(None),
        }
    }

    fn resolve_column(
        &self,
        expr_table: TableInternalId,
        column: usize,
        is_rowid_alias: bool,
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
        if requires_frontend_decoding {
            return Ok(None);
        }
        if is_rowid_alias {
            return self.resolve_rowid(expr_table);
        }
        match self.layout {
            RowLayout::Table => Ok(Some(ResolvedScalarExpr::Column(column))),
            RowLayout::CoveringIndex(index) => {
                let index_column =
                    index.column_table_pos_to_index_pos(column).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "covering index {} does not contain column {column} of table {}",
                            index.name, self.table.name
                        ))
                    })?;
                Ok(Some(ResolvedScalarExpr::Column(index_column)))
            }
        }
    }

    fn resolve_rowid(&self, expr_table: TableInternalId) -> Result<Option<ResolvedScalarExpr>> {
        if expr_table != self.table_id || !self.table.has_rowid {
            return Ok(None);
        }
        match self.layout {
            RowLayout::Table => Ok(Some(ResolvedScalarExpr::RowId)),
            RowLayout::CoveringIndex(index) if index.has_rowid => {
                Ok(Some(ResolvedScalarExpr::IndexRowId))
            }
            RowLayout::CoveringIndex(index) => Err(LimboError::InternalError(format!(
                "covering index {} cannot supply rowid for table {}",
                index.name, self.table.name
            ))),
        }
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
        if !matches!(operator, Operator::Add | Operator::And | Operator::Or)
            && comparison_op.is_none()
        {
            return Ok(None);
        }
        let Some(lhs_resolved) = self.resolve(lhs)? else {
            return Ok(None);
        };
        let Some(rhs_resolved) = self.resolve(rhs)? else {
            return Ok(None);
        };

        if matches!(operator, Operator::Add | Operator::And | Operator::Or) {
            let expression = match operator {
                Operator::Add => {
                    ResolvedScalarExpr::Add(Box::new(lhs_resolved), Box::new(rhs_resolved))
                }
                Operator::And | Operator::Or => ResolvedScalarExpr::Logical {
                    op: match operator {
                        Operator::And => LogicalOp::And,
                        Operator::Or => LogicalOp::Or,
                        _ => unreachable!(),
                    },
                    lhs: Box::new(lhs_resolved),
                    rhs: Box::new(rhs_resolved),
                },
                _ => unreachable!(),
            };
            return Ok(Some(expression));
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

    fn resolve_case(
        &self,
        base: Option<&Expr>,
        when_then_pairs: &[(Box<Expr>, Box<Expr>)],
        else_expr: Option<&Expr>,
    ) -> Result<Option<ResolvedScalarExpr>> {
        if base.is_some() {
            return Ok(None);
        }
        let mut resolved_pairs = Vec::with_capacity(when_then_pairs.len());
        for (when_expr, then_expr) in when_then_pairs {
            let Some(when_expr) = self.resolve(when_expr)? else {
                return Ok(None);
            };
            let Some(then_expr) = self.resolve(then_expr)? else {
                return Ok(None);
            };
            resolved_pairs.push((when_expr, then_expr));
        }
        let else_expr = match else_expr {
            Some(else_expr) => {
                let Some(else_expr) = self.resolve(else_expr)? else {
                    return Ok(None);
                };
                else_expr
            }
            None => ResolvedScalarExpr::Constant(Value::Null),
        };
        Ok(Some(ResolvedScalarExpr::Case {
            when_then_pairs: resolved_pairs,
            else_expr: Box::new(else_expr),
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
    try_compile_expr(Some(row), expr)
        .expect("row-backed symbolic expressions must have a column source")
}

/// Builds a compiler for a resolved expression that does not read a row.
pub(crate) fn compile_static_expr(expr: &ResolvedScalarExpr) -> Option<BoxedCompile<ValueId>> {
    try_compile_expr(None, expr)
}

fn try_compile_expr(row: Option<Row>, expr: &ResolvedScalarExpr) -> Option<BoxedCompile<ValueId>> {
    match expr {
        ResolvedScalarExpr::Input(input_id) => Some(input(*input_id).boxed()),
        ResolvedScalarExpr::Column(column) => Some(row?.column(*column).boxed()),
        ResolvedScalarExpr::RowId => Some(row?.rowid().boxed()),
        ResolvedScalarExpr::IndexRowId => Some(row?.index_rowid().boxed()),
        ResolvedScalarExpr::Parameter(variable) => Some(parameter(variable.clone()).boxed()),
        ResolvedScalarExpr::Constant(value) => Some(constant(value.clone()).boxed()),
        ResolvedScalarExpr::Add(lhs, rhs) => Some(
            try_compile_expr(row, lhs)?
                .then(try_compile_expr(row, rhs)?)
                .and_then(|(lhs, rhs)| add(lhs, rhs))
                .boxed(),
        ),
        ResolvedScalarExpr::Logical { op, lhs, rhs } => Some(
            try_compile_expr(row, lhs)?
                .then(try_compile_expr(row, rhs)?)
                .and_then({
                    let op = *op;
                    move |(lhs, rhs)| logical(op, lhs, rhs)
                })
                .boxed(),
        ),
        ResolvedScalarExpr::Compare {
            lhs,
            rhs,
            comparison,
        } => Some(
            try_compile_expr(row, lhs)?
                .then(try_compile_expr(row, rhs)?)
                .and_then({
                    let comparison = *comparison;
                    move |(lhs, rhs)| compare(lhs, rhs, comparison)
                })
                .boxed(),
        ),
        ResolvedScalarExpr::Case {
            when_then_pairs,
            else_expr,
        } => compile_case(row, when_then_pairs, else_expr),
    }
}

fn compile_case(
    row: Option<Row>,
    when_then_pairs: &[(ResolvedScalarExpr, ResolvedScalarExpr)],
    else_expr: &ResolvedScalarExpr,
) -> Option<BoxedCompile<ValueId>> {
    let Some(((when_expr, then_expr), remaining)) = when_then_pairs.split_first() else {
        return try_compile_expr(row, else_expr);
    };
    Some(
        try_compile_expr(row, when_expr)?
            .branch(
                try_compile_expr(row, then_expr)?,
                compile_case(row, remaining, else_expr)?,
            )
            .boxed(),
    )
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
