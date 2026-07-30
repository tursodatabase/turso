use smallvec::SmallVec;
use turso_parser::ast::{Expr, Literal, Operator, SubqueryType, TableInternalId, Variable};

use crate::{
    schema::{BTreeTable, Index},
    translate::{
        alter::literal_default_value,
        collate::resolve_comparison_collseq_with_symbols,
        compiler::{
            arithmetic, compare, constant, input, logical, pack_values, parameter,
            resolved_comparison, ArithmeticOp, BoxedCompile, ComparisonOp, Compile, InputId,
            InputRequirements, InputSlot, LogicalOp, PackValues, ResolvedComparison, Row, ValueId,
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
    Column {
        row: usize,
        column: usize,
    },
    RowId {
        row: usize,
    },
    IndexRowId {
        row: usize,
    },
    Parameter(Variable),
    Constant(Value),
    Arithmetic {
        op: ArithmeticOp,
        lhs: Box<Self>,
        rhs: Box<Self>,
    },
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
    SimpleCase {
        base: Box<Self>,
        when_then_pairs: Vec<ResolvedSimpleCaseArm>,
        else_expr: Box<Self>,
    },
}

pub(crate) struct ResolvedSimpleCaseArm {
    when_expr: ResolvedScalarExpr,
    then_expr: ResolvedScalarExpr,
    comparison: ResolvedComparison,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScalarInputKind {
    Exists,
    RowValue,
}

/// SQL identity retained as opaque source metadata for a scalar region input.
#[derive(Clone, Copy)]
pub(crate) struct ScalarInputSource {
    pub(crate) subquery_id: TableInternalId,
    pub(crate) kind: ScalarInputKind,
}

/// Physical source used to read one logical table row.
#[derive(Clone, Copy)]
pub(crate) enum RowLayout<'a> {
    Table,
    CoveringIndex(&'a Index),
}

#[derive(Clone, Copy)]
struct ResolvedRowSource<'a> {
    database_id: usize,
    table_id: TableInternalId,
    table: &'a BTreeTable,
    layout: RowLayout<'a>,
}

/// The ordered symbolic rows visible while compiling one stream item.
///
/// Positions are compiler-local identities. SQL table identities are resolved
/// to these positions before the compiler description is constructed.
#[derive(Clone)]
pub(crate) struct SymbolicRows {
    rows: SmallVec<[Row; 2]>,
}

impl SymbolicRows {
    pub(crate) fn single(row: Row) -> Self {
        Self {
            rows: smallvec::smallvec![row],
        }
    }

    pub(crate) fn with_row(mut self, row: Row) -> Self {
        self.rows.push(row);
        self
    }

    fn get(&self, position: usize) -> Option<Row> {
        self.rows.get(position).copied()
    }
}

/// Resolves expressions for an ordered set of symbolic B-tree rows without
/// emitting bytecode.
pub(crate) struct RowExprResolver<'a, 'schema> {
    resolver: &'a Resolver<'schema>,
    rows: SmallVec<[ResolvedRowSource<'a>; 2]>,
    referenced_tables: &'a TableReferences,
    scalar_inputs: InputRequirements<ScalarInputSource>,
}

impl<'a, 'schema> RowExprResolver<'a, 'schema> {
    pub(crate) fn new(
        resolver: &'a Resolver<'schema>,
        database_id: usize,
        table_id: TableInternalId,
        table: &'a BTreeTable,
        layout: RowLayout<'a>,
        referenced_tables: &'a TableReferences,
    ) -> Self {
        Self {
            resolver,
            rows: smallvec::smallvec![ResolvedRowSource {
                database_id,
                table_id,
                table,
                layout,
            }],
            referenced_tables,
            scalar_inputs: InputRequirements::new(),
        }
    }

    pub(crate) fn add_source(
        &mut self,
        database_id: usize,
        table_id: TableInternalId,
        table: &'a BTreeTable,
        layout: RowLayout<'a>,
    ) {
        assert!(
            self.rows.iter().all(|source| source.table_id != table_id),
            "a symbolic row source must have a unique SQL table identity"
        );
        self.rows.push(ResolvedRowSource {
            database_id,
            table_id,
            table,
            layout,
        });
    }

    pub(crate) fn into_scalar_inputs(self) -> InputRequirements<ScalarInputSource> {
        self.scalar_inputs
    }

    /// Returns `None` when any part of the expression still requires the eager
    /// SQL emitter. Resolution is all-or-nothing and never mutates a program.
    pub(crate) fn resolve(&mut self, expr: &Expr) -> Result<Option<ResolvedScalarExpr>> {
        let checkpoint = self.scalar_inputs.checkpoint();
        let resolved = self.resolve_inner(expr);
        if !matches!(&resolved, Ok(Some(_))) {
            self.scalar_inputs.restore(checkpoint);
        }
        resolved
    }

    fn resolve_inner(&mut self, expr: &Expr) -> Result<Option<ResolvedScalarExpr>> {
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
                query_type,
            } => match query_type {
                SubqueryType::Exists { .. } => self
                    .resolve_scalar_input(*subquery_id, ScalarInputKind::Exists)
                    .map(Some),
                SubqueryType::RowValue { num_regs: 1, .. } => self
                    .resolve_scalar_input(*subquery_id, ScalarInputKind::RowValue)
                    .map(Some),
                SubqueryType::RowValue { .. } | SubqueryType::In { .. } => Ok(None),
            },
            Expr::Binary(lhs, operator, rhs) => self.resolve_binary(lhs, *operator, rhs),
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => self.resolve_case(base.as_deref(), when_then_pairs, else_expr.as_deref()),
            _ => Ok(None),
        }
    }

    fn resolve_scalar_input(
        &mut self,
        subquery_id: TableInternalId,
        kind: ScalarInputKind,
    ) -> Result<ResolvedScalarExpr> {
        if let Some(requirement) = self
            .scalar_inputs
            .inputs()
            .iter()
            .find(|requirement| requirement.source().subquery_id == subquery_id)
        {
            if requirement.source().kind != kind {
                return Err(LimboError::InternalError(format!(
                    "subquery {subquery_id:?} has conflicting scalar result kinds",
                )));
            }
            let InputSlot::Value(input) = requirement.slot() else {
                unreachable!("scalar expression requirements only contain value inputs");
            };
            return Ok(ResolvedScalarExpr::Input(input));
        }

        let input = self
            .scalar_inputs
            .require_value(ScalarInputSource { subquery_id, kind })?;
        Ok(ResolvedScalarExpr::Input(input))
    }

    fn resolve_column(
        &self,
        expr_table: TableInternalId,
        column: usize,
        is_rowid_alias: bool,
    ) -> Result<Option<ResolvedScalarExpr>> {
        let Some((row, source)) = self
            .rows
            .iter()
            .enumerate()
            .find(|(_, source)| source.table_id == expr_table)
        else {
            return Ok(None);
        };
        let column_definition = source.table.columns().get(column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "SELECT plan references column {column} outside table {}",
                source.table.name
            ))
        })?;
        let requires_frontend_decoding = column_definition.is_virtual_generated()
            || column_definition.is_array()
            || self.resolver.with_schema(source.database_id, |schema| {
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
        match source.layout {
            RowLayout::Table => Ok(Some(ResolvedScalarExpr::Column { row, column })),
            RowLayout::CoveringIndex(index) => {
                let index_column =
                    index.column_table_pos_to_index_pos(column).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "covering index {} does not contain column {column} of table {}",
                            index.name, source.table.name
                        ))
                    })?;
                Ok(Some(ResolvedScalarExpr::Column {
                    row,
                    column: index_column,
                }))
            }
        }
    }

    fn resolve_rowid(&self, expr_table: TableInternalId) -> Result<Option<ResolvedScalarExpr>> {
        let Some((row, source)) = self
            .rows
            .iter()
            .enumerate()
            .find(|(_, source)| source.table_id == expr_table)
        else {
            return Ok(None);
        };
        if !source.table.has_rowid {
            return Ok(None);
        }
        match source.layout {
            RowLayout::Table => Ok(Some(ResolvedScalarExpr::RowId { row })),
            RowLayout::CoveringIndex(index) if index.has_rowid => {
                Ok(Some(ResolvedScalarExpr::IndexRowId { row }))
            }
            RowLayout::CoveringIndex(index) => Err(LimboError::InternalError(format!(
                "covering index {} cannot supply rowid for table {}",
                index.name, source.table.name
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
        &mut self,
        lhs: &Expr,
        operator: Operator,
        rhs: &Expr,
    ) -> Result<Option<ResolvedScalarExpr>> {
        let arithmetic_op = arithmetic_op(operator);
        let logical_op = match operator {
            Operator::And => Some(LogicalOp::And),
            Operator::Or => Some(LogicalOp::Or),
            _ => None,
        };
        let comparison_op = comparison_op(operator);
        if arithmetic_op.is_none() && logical_op.is_none() && comparison_op.is_none() {
            return Ok(None);
        }
        let Some(lhs_resolved) = self.resolve(lhs)? else {
            return Ok(None);
        };
        let Some(rhs_resolved) = self.resolve(rhs)? else {
            return Ok(None);
        };

        if let Some(op) = arithmetic_op {
            return Ok(Some(ResolvedScalarExpr::Arithmetic {
                op,
                lhs: Box::new(lhs_resolved),
                rhs: Box::new(rhs_resolved),
            }));
        }
        if let Some(op) = logical_op {
            return Ok(Some(ResolvedScalarExpr::Logical {
                op,
                lhs: Box::new(lhs_resolved),
                rhs: Box::new(rhs_resolved),
            }));
        }

        let op = comparison_op.expect("comparison operator checked above");
        let affinity =
            comparison_affinity(lhs, rhs, Some(self.referenced_tables), Some(self.resolver));
        let collation =
            comparison_collation(lhs, rhs, Some(self.referenced_tables), Some(self.resolver))?;
        Ok(Some(ResolvedScalarExpr::Compare {
            lhs: Box::new(lhs_resolved),
            rhs: Box::new(rhs_resolved),
            comparison: if matches!(operator, Operator::Is | Operator::IsNot) {
                resolved_comparison(op, affinity, collation).with_null_equality()
            } else {
                resolved_comparison(op, affinity, collation)
            },
        }))
    }

    fn resolve_case(
        &mut self,
        base: Option<&Expr>,
        when_then_pairs: &[(Box<Expr>, Box<Expr>)],
        else_expr: Option<&Expr>,
    ) -> Result<Option<ResolvedScalarExpr>> {
        let Some(base_expr) = base else {
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
            let Some(else_expr) = self.resolve_case_else(else_expr)? else {
                return Ok(None);
            };
            return Ok(Some(ResolvedScalarExpr::Case {
                when_then_pairs: resolved_pairs,
                else_expr: Box::new(else_expr),
            }));
        };

        let Some(base) = self.resolve(base_expr)? else {
            return Ok(None);
        };
        let mut resolved_pairs = Vec::with_capacity(when_then_pairs.len());
        for (when_expr, then_expr) in when_then_pairs {
            let Some(resolved_when) = self.resolve(when_expr)? else {
                return Ok(None);
            };
            let Some(resolved_then) = self.resolve(then_expr)? else {
                return Ok(None);
            };
            let affinity = comparison_affinity(
                base_expr,
                when_expr,
                Some(self.referenced_tables),
                Some(self.resolver),
            );
            let collation = resolve_comparison_collseq_with_symbols(
                base_expr,
                when_expr,
                self.referenced_tables,
                Some(self.resolver.symbol_table),
            )?;
            resolved_pairs.push(ResolvedSimpleCaseArm {
                when_expr: resolved_when,
                then_expr: resolved_then,
                comparison: resolved_comparison(ComparisonOp::Equal, affinity, Some(collation)),
            });
        }
        let Some(else_expr) = self.resolve_case_else(else_expr)? else {
            return Ok(None);
        };
        Ok(Some(ResolvedScalarExpr::SimpleCase {
            base: Box::new(base),
            when_then_pairs: resolved_pairs,
            else_expr: Box::new(else_expr),
        }))
    }

    fn resolve_case_else(
        &mut self,
        else_expr: Option<&Expr>,
    ) -> Result<Option<ResolvedScalarExpr>> {
        match else_expr {
            Some(else_expr) => self.resolve(else_expr),
            None => Ok(Some(ResolvedScalarExpr::Constant(Value::Null))),
        }
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
        Operator::Is => Some(ComparisonOp::Equal),
        Operator::IsNot => Some(ComparisonOp::NotEqual),
        Operator::Less => Some(ComparisonOp::Less),
        Operator::LessEquals => Some(ComparisonOp::LessEqual),
        Operator::Greater => Some(ComparisonOp::Greater),
        Operator::GreaterEquals => Some(ComparisonOp::GreaterEqual),
        _ => None,
    }
}

const fn arithmetic_op(operator: Operator) -> Option<ArithmeticOp> {
    match operator {
        Operator::Add => Some(ArithmeticOp::Add),
        Operator::Subtract => Some(ArithmeticOp::Subtract),
        Operator::Multiply => Some(ArithmeticOp::Multiply),
        Operator::Divide => Some(ArithmeticOp::Divide),
        Operator::Modulus => Some(ArithmeticOp::Remainder),
        Operator::BitwiseAnd => Some(ArithmeticOp::BitAnd),
        Operator::BitwiseOr => Some(ArithmeticOp::BitOr),
        Operator::LeftShift => Some(ArithmeticOp::ShiftLeft),
        Operator::RightShift => Some(ArithmeticOp::ShiftRight),
        _ => None,
    }
}

/// Builds a compiler for one resolved expression against symbolic rows.
pub(crate) fn compile_expr(
    rows: &SymbolicRows,
    expr: &ResolvedScalarExpr,
) -> BoxedCompile<ValueId> {
    try_compile_expr(Some(rows), expr)
        .expect("row-backed symbolic expressions must have a column source")
}

/// Builds a compiler for a resolved expression that does not read a row.
pub(crate) fn compile_static_expr(expr: &ResolvedScalarExpr) -> Option<BoxedCompile<ValueId>> {
    try_compile_expr(None, expr)
}

fn try_compile_expr(
    rows: Option<&SymbolicRows>,
    expr: &ResolvedScalarExpr,
) -> Option<BoxedCompile<ValueId>> {
    match expr {
        ResolvedScalarExpr::Input(input_id) => Some(input(*input_id).boxed()),
        ResolvedScalarExpr::Column { row, column } => {
            Some(rows?.get(*row)?.column(*column).boxed())
        }
        ResolvedScalarExpr::RowId { row } => Some(rows?.get(*row)?.rowid().boxed()),
        ResolvedScalarExpr::IndexRowId { row } => Some(rows?.get(*row)?.index_rowid().boxed()),
        ResolvedScalarExpr::Parameter(variable) => Some(parameter(variable.clone()).boxed()),
        ResolvedScalarExpr::Constant(value) => Some(constant(value.clone()).boxed()),
        ResolvedScalarExpr::Arithmetic { op, lhs, rhs } => Some(
            try_compile_expr(rows, lhs)?
                .then(try_compile_expr(rows, rhs)?)
                .and_then({
                    let op = *op;
                    move |(lhs, rhs)| arithmetic(op, lhs, rhs)
                })
                .boxed(),
        ),
        ResolvedScalarExpr::Logical { op, lhs, rhs } => Some(
            try_compile_expr(rows, lhs)?
                .then(try_compile_expr(rows, rhs)?)
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
            try_compile_expr(rows, lhs)?
                .then(try_compile_expr(rows, rhs)?)
                .and_then({
                    let comparison = *comparison;
                    move |(lhs, rhs)| compare(lhs, rhs, comparison)
                })
                .boxed(),
        ),
        ResolvedScalarExpr::Case {
            when_then_pairs,
            else_expr,
        } => compile_case(rows, when_then_pairs, else_expr),
        ResolvedScalarExpr::SimpleCase {
            base,
            when_then_pairs,
            else_expr,
        } => compile_simple_case(rows, base, when_then_pairs, else_expr),
    }
}

fn compile_case(
    rows: Option<&SymbolicRows>,
    when_then_pairs: &[(ResolvedScalarExpr, ResolvedScalarExpr)],
    else_expr: &ResolvedScalarExpr,
) -> Option<BoxedCompile<ValueId>> {
    let Some(((when_expr, then_expr), remaining)) = when_then_pairs.split_first() else {
        return try_compile_expr(rows, else_expr);
    };
    Some(
        try_compile_expr(rows, when_expr)?
            .branch(
                try_compile_expr(rows, then_expr)?,
                compile_case(rows, remaining, else_expr)?,
            )
            .boxed(),
    )
}

fn compile_simple_case(
    rows: Option<&SymbolicRows>,
    base: &ResolvedScalarExpr,
    when_then_pairs: &[ResolvedSimpleCaseArm],
    else_expr: &ResolvedScalarExpr,
) -> Option<BoxedCompile<ValueId>> {
    let base = try_compile_expr(rows, base)?;
    let mut arms = Vec::with_capacity(when_then_pairs.len());
    for arm in when_then_pairs {
        arms.push((
            try_compile_expr(rows, &arm.when_expr)?,
            try_compile_expr(rows, &arm.then_expr)?,
            arm.comparison,
        ));
    }
    let else_compiler = try_compile_expr(rows, else_expr)?;
    Some(
        base.and_then(move |base| compile_simple_case_arms(base, arms.into_iter(), else_compiler))
            .boxed(),
    )
}

fn compile_simple_case_arms(
    base: ValueId,
    mut arms: std::vec::IntoIter<(
        BoxedCompile<ValueId>,
        BoxedCompile<ValueId>,
        ResolvedComparison,
    )>,
    else_compiler: BoxedCompile<ValueId>,
) -> BoxedCompile<ValueId> {
    let Some((when_compiler, then_compiler, comparison)) = arms.next() else {
        return else_compiler;
    };
    let remaining = compile_simple_case_arms(base, arms, else_compiler);
    when_compiler
        .and_then(move |when| compare(base, when, comparison))
        .branch(then_compiler, remaining)
        .boxed()
}

/// Compiles expressions in source order into one symbolic register pack.
pub(crate) fn compile_exprs(rows: &SymbolicRows, expressions: &[ResolvedScalarExpr]) -> PackValues {
    let mut compilers = SmallVec::with_capacity(expressions.len());
    for expression in expressions {
        compilers.push(compile_expr(rows, expression));
    }
    pack_values(compilers)
}

/// Compiles a WHERE-clause conjunction with SQL short-circuit truthiness.
pub(crate) fn compile_conjunction(
    rows: &SymbolicRows,
    expressions: &[ResolvedScalarExpr],
) -> BoxedCompile<ValueId> {
    let Some((expression, remaining)) = expressions.split_first() else {
        return constant(Value::from_i64(1)).boxed();
    };
    compile_expr(rows, expression)
        .branch(
            compile_conjunction(rows, remaining),
            constant(Value::from_i64(0)),
        )
        .boxed()
}
