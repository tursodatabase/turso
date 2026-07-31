//! Expression analysis and type facts.

use turso_parser::ast;

use super::{
    hir::{self, CatalogObject, DatabaseId, DeclaredType, TypeFact},
    scope::{NamePrecedence, QueryEnvironment, Scope},
    Analyzer, CatalogObjectKind,
};
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::function::FtsFunc;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::{
    function::{AggFunc, Func, MathFunc, ScalarFunc, VectorFunc, WindowFunc},
    schema::{Column, Type, TypeDef},
    sync::Arc,
    vdbe::affinity::Affinity,
    LimboError, Result, MAIN_DB_ID,
};

/// Clause rules that affect expression name and feature visibility.
#[derive(Clone, Debug)]
pub(crate) struct ExprPolicy {
    precedence: NamePrecedence,
    allow_subqueries: bool,
    allow_raise: bool,
    allow_aggregate: bool,
    allow_window: bool,
    allow_dqs_fallback: bool,
    expected_type: Option<hir::ResolvedType>,
}

impl ExprPolicy {
    pub(crate) fn select() -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_subqueries: true,
            allow_raise: false,
            allow_aggregate: true,
            allow_window: true,
            allow_dqs_fallback: true,
            expected_type: None,
        }
    }

    pub(crate) fn source_then_output() -> Self {
        Self {
            precedence: NamePrecedence::SourceThenOutput,
            ..Self::select()
        }
    }

    pub(crate) fn output_then_source() -> Self {
        Self {
            precedence: NamePrecedence::OutputThenSource,
            ..Self::select()
        }
    }

    pub(crate) fn returning() -> Self {
        Self::select()
    }

    pub(crate) fn trigger_predicate() -> Self {
        Self {
            allow_raise: true,
            ..Self::select()
        }
    }

    pub(crate) fn without_subqueries(mut self) -> Self {
        self.allow_subqueries = false;
        self
    }

    pub(crate) fn without_aggregate(mut self) -> Self {
        self.allow_aggregate = false;
        self.allow_window = false;
        self
    }

    pub(crate) fn without_window(mut self) -> Self {
        self.allow_window = false;
        self
    }

    pub(crate) fn without_dqs_fallback(mut self) -> Self {
        self.allow_dqs_fallback = false;
        self
    }

    pub(crate) fn with_expected_type(mut self, expected: Option<hir::ResolvedType>) -> Self {
        self.expected_type = expected;
        self
    }

    pub(crate) fn with_raise(mut self, allow: bool) -> Self {
        self.allow_raise = allow;
        self
    }
}

impl Analyzer<'_, '_> {
    pub(crate) fn analyze_expr(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::Expr> {
        let expression = self.analyze_expr_inner(syntax, scope, policy)?;
        self.require_source_columns_in_expr(&expression);
        Ok(expression)
    }

    fn analyze_expr_inner(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::Expr> {
        match syntax {
            ast::Expr::Between {
                lhs,
                not,
                start,
                end,
            } => Ok(hir::Expr::Between {
                expr: Box::new(self.analyze_expr(lhs, scope, policy.clone())?),
                negated: *not,
                start: Box::new(self.analyze_expr(start, scope, policy.clone())?),
                end: Box::new(self.analyze_expr(end, scope, policy)?),
            }),
            ast::Expr::Binary(lhs, operator, rhs) => {
                let lhs = self.analyze_expr(lhs, scope, policy.clone())?;
                let rhs = self.analyze_expr(rhs, scope, policy)?;
                let custom = self.resolve_custom_binary_operator(&lhs, *operator, &rhs, scope)?;
                Ok(hir::Expr::Binary {
                    lhs: Box::new(lhs),
                    operator: *operator,
                    rhs: Box::new(rhs),
                    custom,
                })
            }
            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                let base = base
                    .as_deref()
                    .map(|expr| self.analyze_expr(expr, scope, policy.clone()))
                    .transpose()?
                    .map(Box::new);
                let mut when_then = Vec::with_capacity(when_then_pairs.len());
                for (when, then) in when_then_pairs {
                    when_then.push((
                        self.analyze_expr(when, scope, policy.clone())?,
                        self.analyze_expr(then, scope, policy.clone())?,
                    ));
                }
                let else_expr = else_expr
                    .as_deref()
                    .map(|expr| self.analyze_expr(expr, scope, policy))
                    .transpose()?
                    .map(Box::new);
                Ok(hir::Expr::Case {
                    base,
                    when_then,
                    else_expr,
                })
            }
            ast::Expr::Cast { expr, type_name } => {
                let target = self.analyze_type_name(type_name.as_ref(), scope, policy.clone())?;
                Ok(hir::Expr::Cast {
                    expr: Box::new(self.analyze_expr(expr, scope, policy)?),
                    target,
                })
            }
            ast::Expr::Collate(expr, name) => Ok(hir::Expr::Collate {
                expr: Box::new(self.analyze_expr(expr, scope, policy)?),
                collation: self.resolve_collation(name.as_str())?,
            }),
            ast::Expr::Exists(select) => {
                self.require_subqueries(&policy)?;
                let query = self.analyze_query(select, self.subquery_environment(scope))?;
                Ok(hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)))
            }
            ast::Expr::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                within_group,
                filter_over,
            } => self.analyze_function(
                name,
                *distinctness,
                args,
                order_by,
                within_group,
                filter_over,
                scope,
                policy,
                false,
            ),
            ast::Expr::FunctionCallStar { name, filter_over } => {
                self.analyze_function(name, None, &[], &[], &[], filter_over, scope, policy, true)
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                if let Some(resolved) =
                    scope.resolve_unqualified(name.as_str(), policy.precedence)?
                {
                    self.validate_existing_expr(&resolved.expr, scope, &policy)?;
                    return Ok(resolved.expr);
                }
                if policy.allow_dqs_fallback
                    && name.quoted_with('"')
                    && self.context().dqs_dml().is_enabled()
                {
                    return Ok(hir::Expr::Literal(ast::Literal::String(name.as_literal())));
                }
                crate::bail_parse_error!("no such column: {}", name.as_str());
            }
            ast::Expr::Qualified(qualifier, column) => {
                if let Some(resolved) =
                    scope.resolve_qualified(qualifier.as_str(), column.as_str())?
                {
                    return Ok(resolved.expr);
                }
                if let Some(base) =
                    Self::resolve_field_base(scope, qualifier.as_str(), policy.precedence)?
                {
                    self.validate_existing_expr(&base.expr, scope, &policy)?;
                    return self.analyze_field_access(base, column.as_str());
                }
                if scope.missing_qualified_name_is_column() {
                    crate::bail_parse_error!(
                        "no such column: {}.{}",
                        qualifier.as_str(),
                        column.as_str()
                    );
                }
                crate::bail_parse_error!(
                    "no such table: {}",
                    crate::util::normalize_ident(qualifier.as_str())
                );
            }
            ast::Expr::DoublyQualified(database, table, column) => {
                let qualified = ast::QualifiedName {
                    db_name: Some(database.clone()),
                    name: table.clone(),
                    alias: None,
                };
                if let Ok(database_id) = self.context().resolve_database_id(&qualified) {
                    if let Some(resolved) = scope.resolve_database_qualified(
                        DatabaseId::new(database_id),
                        table.as_str(),
                        column.as_str(),
                    )? {
                        return Ok(resolved.expr);
                    }
                }

                if let Some(base) = scope.resolve_qualified(database.as_str(), table.as_str())? {
                    return self.analyze_field_access(base, column.as_str());
                }
                if let Some(base) =
                    Self::resolve_field_base(scope, database.as_str(), policy.precedence)?
                {
                    self.validate_existing_expr(&base.expr, scope, &policy)?;
                    let middle = self.analyze_field_access(base, table.as_str())?;
                    let middle_fact = self.expression_type_fact(&middle, scope);
                    return self.analyze_field_access(
                        super::scope::ResolvedScopeExpr {
                            expr: middle,
                            type_fact: middle_fact,
                            affinity: Affinity::Blob,
                            has_affinity: false,
                            collation: None,
                        },
                        column.as_str(),
                    );
                }
                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    database.as_str(),
                    table.as_str(),
                    column.as_str()
                );
            }
            ast::Expr::InList { lhs, not, rhs } => {
                let lhs = Box::new(self.analyze_expr(lhs, scope, policy.clone())?);
                let mut values = Vec::with_capacity(rhs.len());
                for value in rhs {
                    values.push(self.analyze_expr(value, scope, policy.clone())?);
                }
                Ok(hir::Expr::InList {
                    lhs,
                    negated: *not,
                    values,
                })
            }
            ast::Expr::InSelect { lhs, not, rhs } => {
                self.require_subqueries(&policy)?;
                let lhs = Box::new(self.analyze_expr(lhs, scope, policy)?);
                let query = self.analyze_query(rhs, self.subquery_environment(scope))?;
                Ok(hir::Expr::Subquery(hir::SubqueryExpr::In {
                    lhs,
                    query,
                    negated: *not,
                }))
            }
            ast::Expr::InTable {
                lhs,
                not,
                rhs,
                args,
            } => {
                self.require_subqueries(&policy)?;
                let lhs = Box::new(self.analyze_expr(lhs, scope, policy)?);
                let table = if args.is_empty() {
                    ast::SelectTable::Table(rhs.clone(), None, None)
                } else {
                    ast::SelectTable::TableCall(rhs.clone(), args.clone(), None)
                };
                let select = ast::Select {
                    with: None,
                    body: ast::SelectBody {
                        select: ast::OneSelect::Select {
                            distinctness: None,
                            columns: vec![ast::ResultColumn::Star],
                            from: Some(ast::FromClause {
                                select: Box::new(table),
                                joins: Vec::new(),
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: Vec::new(),
                        },
                        compounds: Vec::new(),
                    },
                    order_by: Vec::new(),
                    limit: None,
                };
                let query = self.analyze_query(&select, self.subquery_environment(scope))?;
                Ok(hir::Expr::Subquery(hir::SubqueryExpr::In {
                    lhs,
                    query,
                    negated: *not,
                }))
            }
            ast::Expr::IsNull(expr) => Ok(hir::Expr::IsNull(Box::new(
                self.analyze_expr(expr, scope, policy)?,
            ))),
            ast::Expr::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => {
                let lhs = self.analyze_expr(lhs, scope, policy.clone())?;
                let rhs = self.analyze_expr(rhs, scope, policy.clone())?;
                let escape = escape
                    .as_deref()
                    .map(|expr| self.analyze_expr(expr, scope, policy))
                    .transpose()?
                    .map(Box::new);
                let (function, argument_count) =
                    self.resolve_like_operator_function(*op, &lhs, escape.is_some())?;
                Ok(hir::Expr::Like {
                    lhs: Box::new(lhs),
                    negated: *not,
                    operator: *op,
                    function,
                    argument_count,
                    rhs: Box::new(rhs),
                    escape,
                })
            }
            ast::Expr::Literal(literal) => Ok(hir::Expr::Literal(literal.clone())),
            ast::Expr::NotNull(expr) => Ok(hir::Expr::NotNull(Box::new(
                self.analyze_expr(expr, scope, policy)?,
            ))),
            ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                self.analyze_expr(&expressions[0], scope, policy)
            }
            ast::Expr::Parenthesized(expressions) => {
                let mut row = Vec::with_capacity(expressions.len());
                for expression in expressions {
                    row.push(self.analyze_expr(expression, scope, policy.clone())?);
                }
                Ok(hir::Expr::Row(row))
            }
            ast::Expr::Raise(action, message) => {
                if !policy.allow_raise && *action != ast::ResolveType::Abort {
                    crate::bail_parse_error!("RAISE() may only be used within a trigger-program");
                }
                Ok(hir::Expr::Raise {
                    action: *action,
                    message: message
                        .as_deref()
                        .map(|expr| self.analyze_expr(expr, scope, policy))
                        .transpose()?
                        .map(Box::new),
                })
            }
            ast::Expr::Subquery(select) => {
                self.require_subqueries(&policy)?;
                let query = self.analyze_query(select, self.subquery_environment(scope))?;
                self.subquery_value_expr(query)
            }
            ast::Expr::Unary(operator, expr) => Ok(hir::Expr::Unary {
                operator: *operator,
                expr: Box::new(self.analyze_expr(expr, scope, policy)?),
            }),
            ast::Expr::Variable(variable) => Ok(hir::Expr::Parameter(hir::Parameter {
                index: variable.index,
                name: variable.name.as_deref().map(ToOwned::to_owned),
                type_fact: TypeFact::dynamic(),
            })),
            ast::Expr::Array { elements } => {
                let mut resolved = Vec::with_capacity(elements.len());
                for element in elements {
                    resolved.push(self.analyze_expr(element, scope, policy.clone())?);
                }
                Ok(hir::Expr::Array(resolved))
            }
            ast::Expr::Subscript { base, index } => Ok(hir::Expr::Subscript {
                base: Box::new(self.analyze_expr(base, scope, policy.clone())?),
                index: Box::new(self.analyze_expr(index, scope, policy)?),
            }),
            ast::Expr::FieldAccess { base, field } => {
                let base = self.analyze_expr(base, scope, policy)?;
                let type_fact = self.expression_type_fact(&base, scope);
                self.analyze_field_access(
                    super::scope::ResolvedScopeExpr {
                        expr: base,
                        type_fact,
                        affinity: Affinity::Blob,
                        has_affinity: false,
                        collation: None,
                    },
                    field.as_str(),
                )
            }
            ast::Expr::Default => {
                crate::bail_parse_error!("DEFAULT is only valid in an INSERT value");
            }
        }
    }

    fn resolve_field_base(
        scope: &Scope,
        name: &str,
        precedence: NamePrecedence,
    ) -> Result<Option<super::scope::ResolvedScopeExpr>> {
        match scope.resolve_unqualified(name, precedence) {
            Err(LimboError::ParseError(message)) => {
                let Some(name) = message.strip_prefix("ambiguous column name: ") else {
                    return Err(LimboError::ParseError(message));
                };
                Err(LimboError::ParseError(format!(
                    "ambiguous column reference: {name}"
                )))
            }
            result => result,
        }
    }

    /// Record every source column read by an already-resolved expression.
    /// This also covers expressions built outside the parser-facing analyzer,
    /// such as star expansion and stored schema programs.
    pub(crate) fn require_source_columns_in_expr(&mut self, expression: &hir::Expr) {
        match expression {
            hir::Expr::Column(column) => {
                self.require_source_column(column.source, column.column);
            }
            hir::Expr::MergedColumn(column) => {
                self.require_source_columns_in_expr(&column.left);
                self.require_source_column(column.right.source, column.right.column);
            }
            hir::Expr::Unary { expr, .. }
            | hir::Expr::Collate { expr, .. }
            | hir::Expr::IsNull(expr)
            | hir::Expr::NotNull(expr) => self.require_source_columns_in_expr(expr),
            hir::Expr::Binary {
                lhs, rhs, custom, ..
            } => {
                self.require_source_columns_in_expr(lhs);
                self.require_source_columns_in_expr(rhs);
                if let Some(encoder) = custom
                    .as_ref()
                    .and_then(|custom| custom.literal_encoding.as_ref())
                    .and_then(|encoding| encoding.encoder.as_ref())
                {
                    self.require_source_columns_in_schema_call(encoder);
                }
            }
            hir::Expr::Between {
                expr, start, end, ..
            } => {
                self.require_source_columns_in_expr(expr);
                self.require_source_columns_in_expr(start);
                self.require_source_columns_in_expr(end);
            }
            hir::Expr::Case {
                base,
                when_then,
                else_expr,
            } => {
                if let Some(base) = base {
                    self.require_source_columns_in_expr(base);
                }
                for (when, then) in when_then {
                    self.require_source_columns_in_expr(when);
                    self.require_source_columns_in_expr(then);
                }
                if let Some(else_expr) = else_expr {
                    self.require_source_columns_in_expr(else_expr);
                }
            }
            hir::Expr::Cast { expr, target } => {
                self.require_source_columns_in_expr(expr);
                for parameter in &target.parameters {
                    self.require_source_columns_in_expr(parameter);
                }
                for call in &target.programs.encode {
                    self.require_source_columns_in_schema_call(call);
                }
                if let Some(domain) = &target.programs.domain {
                    for check in &domain.checks {
                        self.require_source_columns_in_schema_call(&check.call);
                    }
                }
            }
            hir::Expr::Function(call) => {
                for argument in &call.arguments {
                    self.require_source_columns_in_expr(argument);
                }
                for term in &call.argument_order {
                    self.require_source_columns_in_expr(&term.expr);
                }
                for term in &call.within_group {
                    self.require_source_columns_in_expr(&term.expr);
                }
                if let Some(filter) = &call.filter {
                    self.require_source_columns_in_expr(filter);
                }
                if let Some(window) = &call.window {
                    self.require_source_columns_in_window(window);
                }
            }
            hir::Expr::InList { lhs, values, .. } => {
                self.require_source_columns_in_expr(lhs);
                for value in values {
                    self.require_source_columns_in_expr(value);
                }
            }
            hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, .. }) => {
                self.require_source_columns_in_expr(lhs);
            }
            hir::Expr::Like {
                lhs, rhs, escape, ..
            } => {
                self.require_source_columns_in_expr(lhs);
                self.require_source_columns_in_expr(rhs);
                if let Some(escape) = escape {
                    self.require_source_columns_in_expr(escape);
                }
            }
            hir::Expr::Row(expressions) | hir::Expr::Array(expressions) => {
                for expression in expressions {
                    self.require_source_columns_in_expr(expression);
                }
            }
            hir::Expr::Subscript { base, index } => {
                self.require_source_columns_in_expr(base);
                self.require_source_columns_in_expr(index);
            }
            hir::Expr::FieldAccess(access) => {
                self.require_source_columns_in_expr(&access.base);
            }
            hir::Expr::Raise { message, .. } => {
                if let Some(message) = message {
                    self.require_source_columns_in_expr(message);
                }
            }
            hir::Expr::Literal(_)
            | hir::Expr::Parameter(_)
            | hir::Expr::RowId(_)
            | hir::Expr::Output(_)
            | hir::Expr::Subquery(hir::SubqueryExpr::Scalar { .. })
            | hir::Expr::Subquery(hir::SubqueryExpr::Exists(_)) => {}
        }
    }

    fn require_source_columns_in_schema_call(&mut self, call: &hir::BoundSchemaCall) {
        for argument in &call.arguments {
            self.require_source_columns_in_expr(argument);
        }
    }

    fn require_source_columns_in_window(&mut self, window: &hir::WindowSpec) {
        for expression in &window.partition_by {
            self.require_source_columns_in_expr(expression);
        }
        for term in &window.order_by {
            self.require_source_columns_in_expr(&term.expr);
        }
        let Some(frame) = &window.frame else {
            return;
        };
        self.require_source_columns_in_window_bound(&frame.start);
        if let Some(end) = &frame.end {
            self.require_source_columns_in_window_bound(end);
        }
    }

    fn require_source_columns_in_window_bound(&mut self, bound: &hir::WindowFrameBound) {
        match bound {
            hir::WindowFrameBound::Following(expression)
            | hir::WindowFrameBound::Preceding(expression) => {
                self.require_source_columns_in_expr(expression);
            }
            hir::WindowFrameBound::CurrentRow
            | hir::WindowFrameBound::UnboundedFollowing
            | hir::WindowFrameBound::UnboundedPreceding => {}
        }
    }

    pub(crate) fn expression_type_fact(&self, expr: &hir::Expr, scope: &Scope) -> TypeFact {
        match expr {
            hir::Expr::Literal(literal) => literal_type_fact(literal),
            hir::Expr::Parameter(parameter) => parameter.type_fact.clone(),
            hir::Expr::Column(reference) => self
                .source(reference.source)
                .and_then(|source| source.columns.get(reference.column))
                .map(|column| column.type_fact.clone())
                .unwrap_or_default(),
            hir::Expr::MergedColumn(column) => column.type_fact.clone(),
            hir::Expr::RowId(_) => TypeFact::known(Type::Integer),
            hir::Expr::Output(output) => scope.output_type(*output).cloned().unwrap_or_default(),
            hir::Expr::Cast { target, .. } => target.type_fact.clone(),
            hir::Expr::Function(call) => call.result_type.clone(),
            hir::Expr::FieldAccess(access) => access.result_type.clone(),
            hir::Expr::Collate { expr, .. } => self.expression_type_fact(expr, scope),
            hir::Expr::Unary {
                operator: ast::UnaryOperator::Not | ast::UnaryOperator::BitwiseNot,
                ..
            } => TypeFact::known(Type::Integer),
            hir::Expr::Unary { expr, .. } => self.expression_type_fact(expr, scope),
            hir::Expr::IsNull(_)
            | hir::Expr::NotNull(_)
            | hir::Expr::Like { .. }
            | hir::Expr::Between { .. }
            | hir::Expr::InList { .. } => TypeFact::known(Type::Integer),
            hir::Expr::Subquery(hir::SubqueryExpr::Exists(_))
            | hir::Expr::Subquery(hir::SubqueryExpr::In { .. }) => TypeFact::known(Type::Integer),
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }) => self
                .query_outputs(*query)
                .ok()
                .and_then(|outputs| outputs.get(*output).map(|output| output.type_fact.clone()))
                .unwrap_or_default(),
            hir::Expr::Array(elements) => TypeFact::array_literal_result(
                elements
                    .iter()
                    .map(|element| self.expression_type_fact(element, scope)),
            ),
            hir::Expr::Subscript { base, .. } => {
                let mut fact = self.expression_type_fact(base, scope);
                if !fact.is_array() {
                    return TypeFact::dynamic();
                }
                fact.array_dimensions = fact.array_dimensions.saturating_sub(1);
                if let Some(declared) = fact.declared.as_mut() {
                    declared.array_dimensions = declared.array_dimensions.saturating_sub(1);
                    declared.storage = if declared.array_dimensions == 0 {
                        storage_type(&declared.name)
                    } else {
                        Type::Blob
                    };
                    fact.storage = Some(declared.storage);
                } else if fact.is_array() {
                    fact.storage = Some(Type::Blob);
                } else {
                    fact.storage = None;
                }
                fact
            }
            hir::Expr::Binary {
                lhs,
                operator:
                    ast::Operator::Add
                    | ast::Operator::Subtract
                    | ast::Operator::Multiply
                    | ast::Operator::Divide,
                rhs,
                ..
            } => {
                let lhs = self.expression_type_fact(lhs, scope);
                let rhs = self.expression_type_fact(rhs, scope);
                TypeFact::arithmetic_result(&lhs, &rhs)
            }
            hir::Expr::Binary {
                operator:
                    ast::Operator::Modulus
                    | ast::Operator::BitwiseAnd
                    | ast::Operator::BitwiseOr
                    | ast::Operator::LeftShift
                    | ast::Operator::RightShift,
                ..
            } => TypeFact::known(Type::Integer),
            hir::Expr::Binary {
                lhs,
                operator: ast::Operator::Concat,
                rhs,
                ..
            } => TypeFact::concat_result(
                &self.expression_type_fact(lhs, scope),
                &self.expression_type_fact(rhs, scope),
            ),
            hir::Expr::Binary {
                operator: ast::Operator::ArrowRight,
                ..
            } => TypeFact::known(Type::Text),
            hir::Expr::Binary { operator, .. }
                if matches!(
                    operator,
                    ast::Operator::And
                        | ast::Operator::Or
                        | ast::Operator::Equals
                        | ast::Operator::NotEquals
                        | ast::Operator::Less
                        | ast::Operator::LessEquals
                        | ast::Operator::Greater
                        | ast::Operator::GreaterEquals
                        | ast::Operator::Is
                        | ast::Operator::IsNot
                ) =>
            {
                TypeFact::known(Type::Integer)
            }
            hir::Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                let mut results = Vec::with_capacity(when_then.len() + else_expr.iter().count());
                results.extend(
                    when_then
                        .iter()
                        .map(|(_, result)| self.expression_type_fact(result, scope)),
                );
                if let Some(else_expr) = else_expr {
                    results.push(self.expression_type_fact(else_expr, scope));
                }
                TypeFact::selected_value_result(&results)
            }
            _ => TypeFact::dynamic(),
        }
    }

    /// Refresh expression semantics after a recursive input source has widened.
    ///
    /// Ordinary analysis is single-pass because source facts are final before
    /// expressions are built. Recursive CTE inputs are the one exception: an
    /// arm can feed a wider fact back into the next iteration. This walk
    /// recomputes cached facts and rebinds operations whose meaning depends on
    /// those facts. Names and catalog identities stay resolved.
    pub(crate) fn refresh_expression_type_fact(
        &mut self,
        expr: &mut hir::Expr,
        scope: &Scope,
    ) -> Result<TypeFact> {
        self.refresh_expression_semantics(expr, scope, false)
    }

    pub(crate) fn finalize_expression_semantics(
        &mut self,
        expr: &mut hir::Expr,
        scope: &Scope,
    ) -> Result<TypeFact> {
        self.refresh_expression_semantics(expr, scope, true)
    }

    fn refresh_expression_semantics(
        &mut self,
        expr: &mut hir::Expr,
        scope: &Scope,
        finalize_custom_operators: bool,
    ) -> Result<TypeFact> {
        match expr {
            hir::Expr::MergedColumn(column) => {
                let left = self.refresh_expression_semantics(
                    &mut column.left,
                    scope,
                    finalize_custom_operators,
                )?;
                let right = self
                    .source(column.right.source)
                    .and_then(|source| source.columns.get(column.right.column))
                    .map(|column| column.type_fact.clone())
                    .unwrap_or_default();
                column.type_fact = match column.value {
                    hir::MergedColumnValue::Left => left,
                    hir::MergedColumnValue::Right => right,
                    hir::MergedColumnValue::Coalesce => {
                        TypeFact::selected_value_result([&left, &right])
                    }
                };
            }
            hir::Expr::Unary { expr, .. }
            | hir::Expr::Collate { expr, .. }
            | hir::Expr::IsNull(expr)
            | hir::Expr::NotNull(expr) => {
                self.refresh_expression_semantics(expr, scope, finalize_custom_operators)?;
            }
            hir::Expr::Binary {
                lhs,
                operator,
                rhs,
                custom,
            } => {
                self.refresh_expression_semantics(lhs, scope, finalize_custom_operators)?;
                self.refresh_expression_semantics(rhs, scope, finalize_custom_operators)?;
                if finalize_custom_operators {
                    *custom = self.resolve_custom_binary_operator(lhs, *operator, rhs, scope)?;
                }
            }
            hir::Expr::Between {
                expr, start, end, ..
            } => {
                self.refresh_expression_semantics(expr, scope, finalize_custom_operators)?;
                self.refresh_expression_semantics(start, scope, finalize_custom_operators)?;
                self.refresh_expression_semantics(end, scope, finalize_custom_operators)?;
            }
            hir::Expr::Case {
                base,
                when_then,
                else_expr,
            } => {
                if let Some(base) = base {
                    self.refresh_expression_semantics(base, scope, finalize_custom_operators)?;
                }
                for (when, then) in when_then {
                    self.refresh_expression_semantics(when, scope, finalize_custom_operators)?;
                    self.refresh_expression_semantics(then, scope, finalize_custom_operators)?;
                }
                if let Some(else_expr) = else_expr {
                    self.refresh_expression_semantics(else_expr, scope, finalize_custom_operators)?;
                }
            }
            hir::Expr::Cast { expr, target } => {
                self.refresh_expression_semantics(expr, scope, finalize_custom_operators)?;
                for parameter in &mut target.parameters {
                    self.refresh_expression_semantics(parameter, scope, finalize_custom_operators)?;
                }
            }
            hir::Expr::Function(call) => {
                for argument in &mut call.arguments {
                    self.refresh_expression_semantics(argument, scope, finalize_custom_operators)?;
                }
                for term in &mut call.argument_order {
                    self.refresh_expression_semantics(
                        &mut term.expr,
                        scope,
                        finalize_custom_operators,
                    )?;
                }
                for term in &mut call.within_group {
                    self.refresh_expression_semantics(
                        &mut term.expr,
                        scope,
                        finalize_custom_operators,
                    )?;
                }
                if let Some(filter) = &mut call.filter {
                    self.refresh_expression_semantics(filter, scope, finalize_custom_operators)?;
                }
                if let Some(window) = &mut call.window {
                    for expression in &mut window.partition_by {
                        self.refresh_expression_semantics(
                            expression,
                            scope,
                            finalize_custom_operators,
                        )?;
                    }
                    for term in &mut window.order_by {
                        self.refresh_expression_semantics(
                            &mut term.expr,
                            scope,
                            finalize_custom_operators,
                        )?;
                    }
                    if let Some(frame) = &mut window.frame {
                        for bound in std::iter::once(&mut frame.start).chain(frame.end.iter_mut()) {
                            match bound {
                                hir::WindowFrameBound::Following(expression)
                                | hir::WindowFrameBound::Preceding(expression) => {
                                    self.refresh_expression_semantics(
                                        expression,
                                        scope,
                                        finalize_custom_operators,
                                    )?;
                                }
                                hir::WindowFrameBound::CurrentRow
                                | hir::WindowFrameBound::UnboundedFollowing
                                | hir::WindowFrameBound::UnboundedPreceding => {}
                            }
                        }
                    }
                }
                let argument_types = call
                    .arguments
                    .iter()
                    .map(|argument| self.expression_type_fact(argument, scope))
                    .collect::<Vec<_>>();
                let ordered_set_type = call
                    .within_group
                    .first()
                    .map(|term| self.expression_type_fact(&term.expr, scope));
                call.result_type = custom_operation_result(&call.custom_type_operation)
                    .unwrap_or_else(|| {
                        builtin_function_result_type(
                            call.function.value(),
                            &argument_types,
                            &call.arguments,
                            ordered_set_type.as_ref(),
                        )
                    });
            }
            hir::Expr::InList { lhs, values, .. } => {
                self.refresh_expression_semantics(lhs, scope, finalize_custom_operators)?;
                for value in values {
                    self.refresh_expression_semantics(value, scope, finalize_custom_operators)?;
                }
            }
            hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, .. }) => {
                self.refresh_expression_semantics(lhs, scope, finalize_custom_operators)?;
            }
            hir::Expr::Like {
                lhs, rhs, escape, ..
            } => {
                self.refresh_expression_semantics(lhs, scope, finalize_custom_operators)?;
                self.refresh_expression_semantics(rhs, scope, finalize_custom_operators)?;
                if let Some(escape) = escape {
                    self.refresh_expression_semantics(escape, scope, finalize_custom_operators)?;
                }
            }
            hir::Expr::Row(expressions) | hir::Expr::Array(expressions) => {
                for expression in expressions {
                    self.refresh_expression_semantics(
                        expression,
                        scope,
                        finalize_custom_operators,
                    )?;
                }
            }
            hir::Expr::Subscript { base, index } => {
                self.refresh_expression_semantics(base, scope, finalize_custom_operators)?;
                self.refresh_expression_semantics(index, scope, finalize_custom_operators)?;
            }
            hir::Expr::FieldAccess(access) => {
                self.refresh_expression_semantics(
                    &mut access.base,
                    scope,
                    finalize_custom_operators,
                )?;
            }
            hir::Expr::Raise { message, .. } => {
                if let Some(message) = message {
                    self.refresh_expression_semantics(message, scope, finalize_custom_operators)?;
                }
            }
            hir::Expr::Literal(_)
            | hir::Expr::Parameter(_)
            | hir::Expr::Column(_)
            | hir::Expr::RowId(_)
            | hir::Expr::Output(_)
            | hir::Expr::Subquery(hir::SubqueryExpr::Scalar { .. })
            | hir::Expr::Subquery(hir::SubqueryExpr::Exists(_)) => {}
        }
        Ok(self.expression_type_fact(expr, scope))
    }

    pub(crate) fn expression_affinity(&self, expr: &hir::Expr, scope: &Scope) -> Affinity {
        match expr {
            hir::Expr::Column(reference) => self
                .source(reference.source)
                .and_then(|source| source.columns.get(reference.column))
                .map_or(Affinity::Blob, |column| column.affinity),
            hir::Expr::MergedColumn(column) => column.affinity,
            hir::Expr::RowId(_) => Affinity::Integer,
            hir::Expr::Output(output) => scope.output_affinity(*output).unwrap_or(Affinity::Blob),
            hir::Expr::Cast { target, .. } => cast_affinity(target),
            hir::Expr::Collate { expr, .. } => self.expression_affinity(expr, scope),
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }) => self
                .query_outputs(*query)
                .ok()
                .and_then(|outputs| outputs.into_iter().nth(*output))
                .map_or(Affinity::Blob, |output| output.affinity),
            _ => Affinity::Blob,
        }
    }

    pub(crate) fn expression_has_affinity(&self, expr: &hir::Expr, scope: &Scope) -> bool {
        match expr {
            hir::Expr::Column(reference) => self
                .source(reference.source)
                .and_then(|source| source.columns.get(reference.column))
                .is_some_and(|column| column.has_affinity),
            hir::Expr::MergedColumn(column) => column.has_affinity,
            hir::Expr::RowId(_) | hir::Expr::Cast { .. } => true,
            hir::Expr::Output(output) => scope.output_has_affinity(*output).unwrap_or(false),
            hir::Expr::Collate { expr, .. } => self.expression_has_affinity(expr, scope),
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }) => self
                .query_outputs(*query)
                .ok()
                .and_then(|outputs| outputs.into_iter().nth(*output))
                .is_some_and(|output| output.has_affinity),
            _ => false,
        }
    }

    pub(crate) fn expression_collation(
        &self,
        expr: &hir::Expr,
        scope: &Scope,
    ) -> Option<hir::ResolvedCollation> {
        let (explicit, implicit) = self.expression_collation_parts(expr, scope);
        explicit.or(implicit)
    }

    pub(crate) fn expression_explicit_collation(
        &self,
        expr: &hir::Expr,
        scope: &Scope,
    ) -> Option<hir::ResolvedCollation> {
        self.expression_collation_parts(expr, scope).0
    }

    fn expression_collation_parts(
        &self,
        expr: &hir::Expr,
        scope: &Scope,
    ) -> (
        Option<hir::ResolvedCollation>,
        Option<hir::ResolvedCollation>,
    ) {
        self.expression_collation_parts_inner(expr, scope, true)
    }

    fn expression_collation_parts_inner(
        &self,
        expr: &hir::Expr,
        scope: &Scope,
        inherit_column_collation: bool,
    ) -> (
        Option<hir::ResolvedCollation>,
        Option<hir::ResolvedCollation>,
    ) {
        // An explicit COLLATE in any subexpression belongs to the operand.
        // A column's declared collation belongs only to that column, or to a
        // transparent unary-plus/CAST wrapper around it.
        fn merge(
            current: &mut (
                Option<hir::ResolvedCollation>,
                Option<hir::ResolvedCollation>,
            ),
            next: (
                Option<hir::ResolvedCollation>,
                Option<hir::ResolvedCollation>,
            ),
        ) {
            if current.0.is_none() {
                current.0 = next.0;
            }
            if current.1.is_none() {
                current.1 = next.1;
            }
        }

        let mut parts = (None, None);
        macro_rules! collect {
            ($child:expr) => {
                merge(
                    &mut parts,
                    self.expression_collation_parts_inner($child, scope, false),
                )
            };
        }
        match expr {
            hir::Expr::Collate { collation, .. } => {
                return (Some(collation.clone()), None);
            }
            hir::Expr::Column(reference) => {
                if inherit_column_collation {
                    parts.1 = self
                        .source(reference.source)
                        .and_then(|source| source.columns.get(reference.column))
                        .and_then(|column| column.collation.clone());
                }
            }
            hir::Expr::MergedColumn(column) => {
                if inherit_column_collation {
                    parts.1 = column.collation.clone();
                }
            }
            hir::Expr::RowId(_) => {}
            hir::Expr::Output(output) => {
                if inherit_column_collation {
                    parts.1 = scope.output_collation(*output).flatten().cloned();
                }
            }
            hir::Expr::Unary {
                operator: ast::UnaryOperator::Positive,
                expr,
            } => {
                return self.expression_collation_parts_inner(
                    expr,
                    scope,
                    inherit_column_collation,
                );
            }
            hir::Expr::Unary { expr, .. } | hir::Expr::IsNull(expr) | hir::Expr::NotNull(expr) => {
                collect!(expr);
            }
            hir::Expr::Binary { lhs, rhs, .. } => {
                collect!(lhs);
                collect!(rhs);
            }
            hir::Expr::Between {
                expr, start, end, ..
            } => {
                collect!(expr);
                collect!(start);
                collect!(end);
            }
            hir::Expr::Case {
                base,
                when_then,
                else_expr,
            } => {
                if let Some(base) = base {
                    collect!(base);
                }
                for (when, then) in when_then {
                    collect!(when);
                    collect!(then);
                }
                if let Some(else_expr) = else_expr {
                    collect!(else_expr);
                }
            }
            hir::Expr::Cast { expr, target } => {
                merge(
                    &mut parts,
                    self.expression_collation_parts_inner(expr, scope, inherit_column_collation),
                );
                for parameter in &target.parameters {
                    collect!(parameter);
                }
            }
            hir::Expr::Function(function) => {
                for argument in &function.arguments {
                    collect!(argument);
                }
                for term in function.argument_order.iter().chain(&function.within_group) {
                    collect!(&term.expr);
                }
                if let Some(filter) = &function.filter {
                    collect!(filter);
                }
                if let Some(window) = &function.window {
                    for expression in &window.partition_by {
                        collect!(expression);
                    }
                    for term in &window.order_by {
                        collect!(&term.expr);
                    }
                    if let Some(frame) = &window.frame {
                        match &frame.start {
                            hir::WindowFrameBound::Following(expr)
                            | hir::WindowFrameBound::Preceding(expr) => collect!(expr),
                            hir::WindowFrameBound::CurrentRow
                            | hir::WindowFrameBound::UnboundedFollowing
                            | hir::WindowFrameBound::UnboundedPreceding => {}
                        }
                        if let Some(end) = &frame.end {
                            match end {
                                hir::WindowFrameBound::Following(expr)
                                | hir::WindowFrameBound::Preceding(expr) => collect!(expr),
                                hir::WindowFrameBound::CurrentRow
                                | hir::WindowFrameBound::UnboundedFollowing
                                | hir::WindowFrameBound::UnboundedPreceding => {}
                            }
                        }
                    }
                }
            }
            hir::Expr::InList { lhs, values, .. } => {
                collect!(lhs);
                for value in values {
                    collect!(value);
                }
            }
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }) => {
                if inherit_column_collation {
                    parts.1 = self
                        .query_outputs(*query)
                        .ok()
                        .and_then(|outputs| outputs.into_iter().nth(*output))
                        .and_then(|output| output.collation);
                }
            }
            hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, .. }) => collect!(lhs),
            hir::Expr::Like {
                lhs, rhs, escape, ..
            } => {
                collect!(lhs);
                collect!(rhs);
                if let Some(escape) = escape {
                    collect!(escape);
                }
            }
            hir::Expr::Row(values) | hir::Expr::Array(values) => {
                for value in values {
                    collect!(value);
                }
            }
            hir::Expr::Subscript { base, index } => {
                collect!(base);
                collect!(index);
            }
            hir::Expr::FieldAccess(access) => collect!(&access.base),
            hir::Expr::Raise {
                message: Some(message),
                ..
            } => collect!(message),
            hir::Expr::Literal(_)
            | hir::Expr::Parameter(_)
            | hir::Expr::Subquery(hir::SubqueryExpr::Exists(_))
            | hir::Expr::Raise { message: None, .. } => {}
        }
        parts
    }

    pub(crate) fn resolve_declared_type_fact(
        &mut self,
        name: &str,
        array_dimensions: u32,
    ) -> Result<TypeFact> {
        self.resolve_declared_type_fact_in_database(name, array_dimensions, MAIN_DB_ID)
    }

    pub(crate) fn resolve_declared_type_fact_in_database(
        &mut self,
        name: &str,
        array_dimensions: u32,
        database_id: usize,
    ) -> Result<TypeFact> {
        let custom_chain = self.resolve_custom_type_chain(name, database_id)?;
        let storage = if array_dimensions > 0 {
            Type::Blob
        } else if let Some(custom) = custom_chain.first() {
            storage_type(custom.value().base())
        } else {
            storage_type(name)
        };
        Ok(TypeFact::declared(DeclaredType {
            name: name.to_string(),
            storage,
            custom_chain,
            array_dimensions,
        }))
    }

    fn analyze_function(
        &mut self,
        name: &ast::Name,
        distinctness: Option<ast::Distinctness>,
        syntax_args: &[Box<ast::Expr>],
        syntax_order: &[ast::SortedColumn],
        syntax_within_group: &[ast::SortedColumn],
        tail: &ast::FunctionTail,
        scope: &Scope,
        policy: ExprPolicy,
        star: bool,
    ) -> Result<hir::Expr> {
        let function_name = crate::util::normalize_ident(name.as_str());
        let lookup_arg_count = if star { 0 } else { syntax_args.len() };
        // Ordered-set calls have a different SQL arity from their physical
        // aggregate. Resolve them from their syntax before consulting the
        // ordinary function table: percentile_disc(x, p) is still the
        // separately registered two-argument aggregate, while
        // percentile_disc(p) WITHIN GROUP (ORDER BY x) becomes the built-in
        // ordered-set aggregate with physical arguments [x, p].
        let ordered_set = match function_name.as_str() {
            "mode" => Some(AggFunc::Mode),
            "percentile_cont" if !syntax_within_group.is_empty() => Some(AggFunc::PercentileCont),
            "percentile_disc" if !syntax_within_group.is_empty() => Some(AggFunc::PercentileDisc),
            _ => None,
        };
        let function = match ordered_set {
            Some(function) => Func::Agg(function),
            None => self
                .context()
                .resolve_function(&function_name, lookup_arg_count)?
                .ok_or_else(|| {
                    LimboError::ParseError(format!("no such function: {function_name}"))
                })?,
        };

        let aggregate = function_is_aggregate(&function);
        let window_only = matches!(&function, Func::Window(_));
        if (window_only || tail.over_clause.is_some()) && !policy.allow_window {
            crate::bail_parse_error!("misuse of window function {}()", function_name);
        }
        if aggregate && !policy.allow_aggregate {
            crate::bail_parse_error!("misuse of aggregate function {}()", function_name);
        }
        if window_only && tail.over_clause.is_none() {
            crate::bail_parse_error!("misuse of window function {}()", function_name);
        }
        if tail.over_clause.is_some() && !aggregate && !window_only {
            crate::bail_parse_error!("{}() may not be used as a window function", function_name);
        }
        if tail.filter_clause.is_some() && !aggregate && !window_only {
            crate::bail_parse_error!(
                "FILTER may not be used with non-aggregate {}()",
                function_name
            );
        }
        if (!syntax_order.is_empty() || !syntax_within_group.is_empty() || distinctness.is_some())
            && !aggregate
        {
            crate::bail_parse_error!(
                "aggregate syntax is not allowed for scalar function {}()",
                function_name
            );
        }
        if star && !aggregate && !function.supports_star_syntax() {
            crate::bail_parse_error!("wrong number of arguments to function {}()", function_name);
        }
        validate_function_call(&function, &function_name, syntax_args)?;

        let union_value_argument_type = if function_name == "union_value" {
            self.union_value_argument_type(syntax_args, policy.expected_type.as_ref())?
        } else {
            None
        };
        let child_policy = if tail.over_clause.is_some() {
            policy.clone().without_window()
        } else if aggregate {
            policy.clone().without_aggregate()
        } else {
            policy.clone()
        }
        .with_expected_type(None);

        let mut arguments = Vec::new();
        if star && function.needs_star_expansion() {
            let expanded = scope.expand_star()?;
            if expanded.is_empty() {
                crate::bail_parse_error!("{}(*) requires a FROM clause", function_name);
            }
            arguments.reserve(expanded.len() * 2);
            for (column_name, expression, ..) in expanded {
                arguments.push(hir::Expr::Literal(ast::Literal::String(format!(
                    "'{}'",
                    column_name.replace('\'', "''")
                ))));
                arguments.push(expression);
            }
        } else {
            arguments.reserve(syntax_args.len());
            for (index, argument) in syntax_args.iter().enumerate() {
                let expected = (index == 1)
                    .then(|| union_value_argument_type.clone())
                    .flatten();
                arguments.push(self.analyze_expr(
                    argument,
                    scope,
                    child_policy.clone().with_expected_type(expected),
                )?);
            }
        }

        let argument_order = self.analyze_order_terms(syntax_order, scope, child_policy.clone())?;
        let within_group =
            self.analyze_order_terms(syntax_within_group, scope, child_policy.clone())?;
        let filter = tail
            .filter_clause
            .as_deref()
            .map(|expr| self.analyze_expr(expr, scope, child_policy.clone()))
            .transpose()?
            .map(Box::new);
        let window = tail
            .over_clause
            .as_ref()
            .map(|over| self.analyze_over(over, scope, child_policy))
            .transpose()?;

        let custom_type_operation = self.resolve_custom_type_operation(
            &function_name,
            syntax_args,
            &arguments,
            scope,
            policy.expected_type.as_ref(),
        )?;
        let sequence_operation = match &function {
            Func::Scalar(ScalarFunc::NextVal) => {
                Some(self.resolve_sequence_operation(
                    hir::SequenceOperationKind::NextValue,
                    syntax_args,
                )?)
            }
            Func::Scalar(ScalarFunc::SetVal) => Some(
                self.resolve_sequence_operation(hir::SequenceOperationKind::SetValue, syntax_args)?,
            ),
            _ => None,
        };
        let result_type = custom_operation_result(&custom_type_operation).unwrap_or_else(|| {
            self.resolved_function_result_type(&function, &arguments, &within_group, scope)
        });
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function {
                argument_count: lookup_arg_count,
            },
            function_name,
        );
        let function = CatalogObject::new(id, self.context().snapshot(), None, Arc::new(function));
        Ok(hir::Expr::Function(hir::FunctionCall {
            function,
            star,
            arguments,
            distinctness,
            argument_order,
            within_group,
            filter,
            window,
            result_type,
            custom_type_operation,
            sequence_operation,
        }))
    }

    fn resolved_function_result_type(
        &self,
        function: &Func,
        arguments: &[hir::Expr],
        within_group: &[hir::OrderTerm],
        scope: &Scope,
    ) -> TypeFact {
        let argument_types = arguments
            .iter()
            .map(|argument| self.expression_type_fact(argument, scope))
            .collect::<Vec<_>>();
        let ordered_set_type = within_group
            .first()
            .map(|term| self.expression_type_fact(&term.expr, scope));
        builtin_function_result_type(
            function,
            &argument_types,
            arguments,
            ordered_set_type.as_ref(),
        )
    }

    fn resolve_sequence_operation(
        &mut self,
        kind: hir::SequenceOperationKind,
        syntax_args: &[Box<ast::Expr>],
    ) -> Result<hir::SequenceOperation> {
        let user_name = match syntax_args.first().map(Box::as_ref) {
            Some(ast::Expr::Literal(ast::Literal::String(name))) => {
                name.trim_matches('\'').to_string()
            }
            _ => crate::bail_parse_error!("expected a string literal argument"),
        };
        self.resolve_sequence_catalog_operation(kind, user_name)
    }

    fn analyze_order_terms(
        &mut self,
        terms: &[ast::SortedColumn],
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<Vec<hir::OrderTerm>> {
        let mut resolved = Vec::with_capacity(terms.len());
        for term in terms {
            resolved.push(hir::OrderTerm {
                expr: self.analyze_expr(&term.expr, scope, policy.clone())?,
                order: term.order.unwrap_or(ast::SortOrder::Asc),
                nulls: term.nulls,
            });
        }
        Ok(resolved)
    }

    fn analyze_over(
        &mut self,
        over: &ast::Over,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::WindowSpec> {
        match over {
            ast::Over::Name(name) => scope.window(name.as_str()).cloned().ok_or_else(|| {
                LimboError::ParseError(format!("no such window: {}", name.as_str()))
            }),
            ast::Over::Window(window) => self.analyze_window(window, scope, policy),
        }
    }

    pub(crate) fn analyze_window(
        &mut self,
        window: &ast::Window,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::WindowSpec> {
        let policy = policy.without_window();
        let mut resolved = if let Some(base) = &window.base {
            scope.window(base.as_str()).cloned().ok_or_else(|| {
                LimboError::ParseError(format!("no such window: {}", base.as_str()))
            })?
        } else {
            hir::WindowSpec {
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            }
        };

        if !window.partition_by.is_empty() {
            if !resolved.partition_by.is_empty() {
                crate::bail_parse_error!("cannot override PARTITION clause of window");
            }
            for expression in &window.partition_by {
                resolved
                    .partition_by
                    .push(self.analyze_expr(expression, scope, policy.clone())?);
            }
        }
        if !window.order_by.is_empty() {
            if !resolved.order_by.is_empty() {
                crate::bail_parse_error!("cannot override ORDER BY clause of window");
            }
            resolved.order_by =
                self.analyze_order_terms(&window.order_by, scope, policy.clone())?;
        }
        if let Some(frame) = &window.frame_clause {
            if resolved.frame.is_some() {
                crate::bail_parse_error!("cannot override frame specification of window");
            }
            resolved.frame = Some(hir::WindowFrame {
                mode: frame.mode,
                start: self.analyze_frame_bound(&frame.start, scope, policy.clone())?,
                end: frame
                    .end
                    .as_ref()
                    .map(|bound| self.analyze_frame_bound(bound, scope, policy.clone()))
                    .transpose()?,
                exclude: frame.exclude.clone(),
            });
        }
        Ok(resolved)
    }

    fn analyze_frame_bound(
        &mut self,
        bound: &ast::FrameBound,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::WindowFrameBound> {
        Ok(match bound {
            ast::FrameBound::CurrentRow => hir::WindowFrameBound::CurrentRow,
            ast::FrameBound::Following(expr) => {
                hir::WindowFrameBound::Following(Box::new(self.analyze_expr(expr, scope, policy)?))
            }
            ast::FrameBound::Preceding(expr) => {
                hir::WindowFrameBound::Preceding(Box::new(self.analyze_expr(expr, scope, policy)?))
            }
            ast::FrameBound::UnboundedFollowing => hir::WindowFrameBound::UnboundedFollowing,
            ast::FrameBound::UnboundedPreceding => hir::WindowFrameBound::UnboundedPreceding,
        })
    }

    fn analyze_type_name(
        &mut self,
        syntax: Option<&ast::Type>,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::TypeName> {
        let Some(syntax) = syntax else {
            let type_fact = TypeFact::dynamic();
            let programs = self.bind_cast_programs(&type_fact, &[], scope)?;
            return Ok(hir::TypeName {
                name: String::new(),
                parameters: Vec::new(),
                array_dimensions: 0,
                type_fact,
                programs,
            });
        };
        let mut parameters = Vec::new();
        match &syntax.size {
            Some(ast::TypeSize::MaxSize(expr)) => {
                parameters.push(self.analyze_expr(expr, scope, policy.clone())?);
            }
            Some(ast::TypeSize::TypeSize(first, second)) => {
                parameters.push(self.analyze_expr(first, scope, policy.clone())?);
                parameters.push(self.analyze_expr(second, scope, policy)?);
            }
            None => {}
        }
        let type_fact = self.resolve_declared_type_fact(&syntax.name, syntax.array_dimensions)?;
        let programs = self.bind_cast_programs(&type_fact, &parameters, scope)?;
        Ok(hir::TypeName {
            name: syntax.name.clone(),
            parameters,
            array_dimensions: syntax.array_dimensions,
            type_fact,
            programs,
        })
    }

    pub(super) fn resolve_collation(&mut self, name: &str) -> Result<hir::ResolvedCollation> {
        let normalized = crate::util::normalize_ident(name);
        let collation = self.context().resolve_collation(&normalized)?;
        let id = self.catalog_object_id(None, CatalogObjectKind::Collation, normalized);
        Ok(CatalogObject::new(
            id,
            self.context().snapshot(),
            None,
            Arc::new(collation),
        ))
    }

    /// Resolve custom-type operator dispatch while source identities and
    /// declared type facts are still available. The returned metadata is
    /// closed over catalog handles, so planning and emission never repeat a
    /// schema lookup by operator or function name.
    pub(super) fn resolve_custom_binary_operator(
        &mut self,
        lhs: &hir::Expr,
        operator: ast::Operator,
        rhs: &hir::Expr,
        scope: &Scope,
    ) -> Result<Option<hir::CustomBinaryOperator>> {
        let Some(operator_name) = custom_binary_operator_name(operator) else {
            return Ok(None);
        };
        let lhs_custom = self.custom_type_operand(lhs, scope);
        let rhs_custom = self.custom_type_operand(rhs, scope);

        let find_operator = |type_def: &TypeDef| -> Option<(String, bool, bool)> {
            for definition in type_def.operators() {
                if definition.op == operator_name {
                    // A naked declaration explicitly asks for SQLite's normal
                    // operator behavior; do not derive another implementation.
                    return definition
                        .func_name
                        .as_ref()
                        .map(|name| (name.clone(), false, false));
                }
            }

            let find = |name: &str| {
                type_def
                    .operators()
                    .iter()
                    .find(|definition| definition.op == name)
                    .and_then(|definition| definition.func_name.clone())
            };
            match operator {
                ast::Operator::Greater => find("<").map(|name| (name, true, false)),
                ast::Operator::GreaterEquals => find("<").map(|name| (name, false, true)),
                ast::Operator::LessEquals => find("<").map(|name| (name, true, true)),
                ast::Operator::NotEquals => find("=").map(|name| (name, false, true)),
                _ => None,
            }
        };

        let selected = match (&lhs_custom, &rhs_custom) {
            (Some(lhs_type), Some(rhs_type)) if lhs_type == rhs_type => {
                find_operator(lhs_type.value()).map(|(function, swap_args, negate)| {
                    (function, swap_args, negate, None, lhs_type.clone())
                })
            }
            (Some(_), Some(_)) => None,
            (Some(lhs_type), None) => hir_literal_type_name(rhs)
                .filter(|literal| {
                    custom_literal_is_compatible(literal, lhs_type.value().value_input_type())
                })
                .and_then(|_| find_operator(lhs_type.value()))
                .map(|(function, swap_args, negate)| -> Result<_> {
                    let column = self.custom_literal_column(lhs, scope);
                    let encoder = self.bind_literal_encoder(column.as_ref(), lhs_type)?;
                    Ok((
                        function,
                        swap_args,
                        negate,
                        Some(hir::CustomBinaryLiteralEncoding {
                            // The operand is named in the original SQL order;
                            // emission applies swap_args afterward.
                            operand: hir::BinaryOperand::Right,
                            encoder,
                        }),
                        lhs_type.clone(),
                    ))
                })
                .transpose()?,
            (None, Some(rhs_type)) => hir_literal_type_name(lhs)
                .filter(|literal| {
                    custom_literal_is_compatible(literal, rhs_type.value().value_input_type())
                })
                .and_then(|_| find_operator(rhs_type.value()))
                .map(|(function, swap_args, negate)| -> Result<_> {
                    let column = self.custom_literal_column(rhs, scope);
                    let encoder = self.bind_literal_encoder(column.as_ref(), rhs_type)?;
                    Ok((
                        function,
                        swap_args,
                        negate,
                        Some(hir::CustomBinaryLiteralEncoding {
                            // The operand is named in the original SQL order;
                            // emission applies swap_args afterward.
                            operand: hir::BinaryOperand::Left,
                            encoder,
                        }),
                        rhs_type.clone(),
                    ))
                })
                .transpose()?,
            (None, None) => None,
        };

        let Some((function_name, swap_args, negate, literal_encoding, defining_type)) = selected
        else {
            return Ok(None);
        };
        let function = self
            .context()
            .resolve_function(&function_name, 2)?
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "custom operator function '{function_name}' for type '{}' is missing",
                    defining_type.value().name
                ))
            })?;
        let normalized = crate::util::normalize_ident(&function_name);
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function { argument_count: 2 },
            normalized,
        );
        Ok(Some(hir::CustomBinaryOperator {
            function: CatalogObject::new(id, self.context().snapshot(), None, Arc::new(function)),
            swap_args,
            negate,
            literal_encoding,
        }))
    }

    pub(super) fn resolve_like_operator_function(
        &mut self,
        operator: ast::LikeOperator,
        lhs: &hir::Expr,
        has_escape: bool,
    ) -> Result<(hir::ResolvedFunction, usize)> {
        let (name, argument_count) = match operator {
            ast::LikeOperator::Like => ("like", if has_escape { 3 } else { 2 }),
            ast::LikeOperator::Glob => ("glob", if has_escape { 3 } else { 2 }),
            ast::LikeOperator::Regexp => {
                if has_escape {
                    crate::bail_parse_error!("wrong number of arguments to function regexp()");
                }
                ("regexp", 2)
            }
            ast::LikeOperator::Match => {
                if has_escape {
                    crate::bail_parse_error!("wrong number of arguments to function fts_match()");
                }
                let columns = match lhs {
                    hir::Expr::Row(columns) => columns.len(),
                    _ => 1,
                };
                ("fts_match", columns + 1)
            }
        };
        let function = self
            .context()
            .resolve_function(name, argument_count)?
            .ok_or_else(|| match operator {
                ast::LikeOperator::Match => {
                    LimboError::ParseError("MATCH requires the 'fts' feature to be enabled".into())
                }
                _ => LimboError::ParseError(format!("no such function: {name}")),
            })?;
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function { argument_count },
            name.to_string(),
        );
        Ok((
            CatalogObject::new(id, self.context().snapshot(), None, Arc::new(function)),
            argument_count,
        ))
    }

    fn custom_type_operand(&self, expr: &hir::Expr, scope: &Scope) -> Option<hir::ResolvedType> {
        let type_fact = self.expression_type_fact(expr, scope);
        type_fact.declared.as_ref()?.custom().cloned()
    }

    /// Recover column type arguments only when a literal must be encoded for
    /// a custom operator. Operator selection itself depends solely on the
    /// semantic type fact, so derived and CTE columns retain their type.
    fn custom_literal_column(&self, expr: &hir::Expr, scope: &Scope) -> Option<Column> {
        self.custom_literal_column_inner(expr, scope, 0)
    }

    fn custom_literal_column_inner(
        &self,
        expr: &hir::Expr,
        scope: &Scope,
        depth: usize,
    ) -> Option<Column> {
        if depth >= 64 {
            return None;
        }
        let next_depth = depth + 1;
        match expr {
            hir::Expr::Column(reference) => {
                let source = self.source(reference.source)?;
                match &source.kind {
                    hir::SourceKind::Table(table)
                    | hir::SourceKind::TableFunction { table, .. }
                    | hir::SourceKind::Pseudo { table, .. } => {
                        table.value().columns().get(reference.column).cloned()
                    }
                    hir::SourceKind::SchemaExpression => self
                        .source_catalog_table(reference.source)
                        .and_then(|table| table.columns().get(reference.column))
                        .cloned(),
                    hir::SourceKind::Derived(query) => {
                        let outputs = self.query_outputs(*query).ok()?;
                        let output = outputs.get(reference.column)?;
                        self.custom_literal_column_inner(&output.expr, scope, next_depth)
                    }
                    hir::SourceKind::Cte(cte) | hir::SourceKind::RecursiveInput(cte) => {
                        let query = match &self.cte(*cte)?.body {
                            hir::CteBody::Query(query) => *query,
                            hir::CteBody::Recursive(recursive) => recursive.seed,
                        };
                        let outputs = self.query_outputs(query).ok()?;
                        let output = outputs.get(reference.column)?;
                        self.custom_literal_column_inner(&output.expr, scope, next_depth)
                    }
                }
            }
            hir::Expr::Output(output) => {
                if let Some(expression) = scope.output_expr(*output) {
                    return self.custom_literal_column_inner(expression, scope, next_depth);
                }
                let hir::OutputOwner::QueryBlock(block) = output.owner else {
                    return None;
                };
                let expression = &self
                    .query(block.query)?
                    .blocks
                    .get(block.index)?
                    .outputs
                    .get(output.index)?
                    .expr;
                self.custom_literal_column_inner(expression, scope, next_depth)
            }
            hir::Expr::MergedColumn(column) => match column.value {
                hir::MergedColumnValue::Left => {
                    self.custom_literal_column_inner(&column.left, scope, next_depth)
                }
                hir::MergedColumnValue::Right => self.custom_literal_column_inner(
                    &hir::Expr::Column(column.right),
                    scope,
                    next_depth,
                ),
                hir::MergedColumnValue::Coalesce => None,
            },
            hir::Expr::Collate { expr, .. }
            | hir::Expr::Unary {
                operator: ast::UnaryOperator::Positive,
                expr,
            } => self.custom_literal_column_inner(expr, scope, next_depth),
            _ => None,
        }
    }

    fn resolve_custom_type_chain(
        &mut self,
        name: &str,
        database_id: usize,
    ) -> Result<Vec<hir::ResolvedType>> {
        let resolved = self
            .context()
            .schema(database_id)
            .map(|schema| schema.resolve_type_unchecked(name))
            .transpose()?
            .flatten();
        let Some(resolved) = resolved else {
            return Ok(Vec::new());
        };
        let mut chain = Vec::with_capacity(resolved.chain.len());
        for definition in resolved.chain {
            let id = self.catalog_object_id(
                Some(database_id),
                CatalogObjectKind::Type,
                crate::util::normalize_ident(&definition.name),
            );
            chain.push(CatalogObject::new(
                id,
                self.context().snapshot(),
                Some(DatabaseId::new(database_id)),
                definition,
            ));
        }
        Ok(chain)
    }

    fn analyze_field_access(
        &mut self,
        base: super::scope::ResolvedScopeExpr,
        field_name: &str,
    ) -> Result<hir::Expr> {
        let column_name = match &base.expr {
            hir::Expr::Column(reference) => self
                .source(reference.source)
                .and_then(|source| source.columns.get(reference.column))
                .map(|column| column.name.clone()),
            _ => None,
        };
        let container = base
            .type_fact
            .declared
            .as_ref()
            .and_then(|declared| declared.custom().cloned())
            .filter(|ty| ty.value().is_struct() || ty.value().is_union())
            .ok_or_else(|| {
                let message = match &column_name {
                    Some(name) => format!("column '{name}' is not a STRUCT or UNION type"),
                    None => format!(
                        "cannot extract field '{}' from a value without a known struct or union type",
                        field_name
                    ),
                };
                LimboError::ParseError(message)
            })?;

        let database_id = container
            .database()
            .map(DatabaseId::index)
            .unwrap_or(MAIN_DB_ID);
        let (kind, result_type) = if let Some((field_index, field)) =
            container.value().find_struct_field(field_name)
        {
            let type_name = field.type_name.clone();
            (
                hir::FieldAccessKind::Struct { field_index },
                self.resolve_declared_type_fact_in_database(
                    &type_name,
                    field.array_dimensions,
                    database_id,
                )?,
            )
        } else if let Some((tag_index, variant)) = container.value().find_union_variant(field_name)
        {
            let type_name = variant.type_name.clone();
            (
                hir::FieldAccessKind::Union { tag_index },
                self.resolve_declared_type_fact_in_database(
                    &type_name,
                    variant.array_dimensions,
                    database_id,
                )?,
            )
        } else if container.value().is_struct() {
            crate::bail_parse_error!(
                "no such field '{}' in struct type '{}'",
                field_name,
                container.value().name
            );
        } else {
            debug_assert!(container.value().is_union());
            crate::bail_parse_error!(
                "no such variant '{}' in union type '{}'",
                field_name,
                container.value().name
            );
        };

        Ok(hir::Expr::FieldAccess(hir::FieldAccess {
            base: Box::new(base.expr),
            field_name: crate::util::normalize_ident(field_name),
            kind,
            container_type: container,
            result_type,
        }))
    }

    fn resolve_custom_type_operation(
        &mut self,
        function_name: &str,
        syntax_args: &[Box<ast::Expr>],
        arguments: &[hir::Expr],
        scope: &Scope,
        expected_type: Option<&hir::ResolvedType>,
    ) -> Result<Option<hir::CustomTypeOperation>> {
        let string_argument = |index: usize, ordinal: &str| -> Result<&str> {
            let Some(ast::Expr::Literal(ast::Literal::String(value))) =
                syntax_args.get(index).map(AsRef::as_ref)
            else {
                return Err(LimboError::ParseError(format!(
                    "{}() {} argument must be a string literal",
                    function_name, ordinal
                )));
            };
            Ok(value.trim_matches('\''))
        };

        match function_name {
            "union_value" => {
                if syntax_args.len() != 2 {
                    crate::bail_parse_error!("union_value() requires exactly 2 arguments");
                }
                let tag_name = string_argument(0, "first")?;
                let union_type = expected_type.filter(|ty| ty.value().is_union()).cloned().ok_or_else(
                    || LimboError::ParseError(
                        "union_value() can only be used in INSERT/UPDATE targeting a union-typed column"
                            .to_string(),
                    ),
                )?;
                let (tag_index, _) =
                    union_type
                        .value()
                        .find_union_variant(tag_name)
                        .ok_or_else(|| {
                            LimboError::ParseError(format!(
                                "unknown variant '{}' in union type '{}'",
                                tag_name,
                                union_type.value().name
                            ))
                        })?;
                let database_id = union_type
                    .database()
                    .map(DatabaseId::index)
                    .unwrap_or(MAIN_DB_ID);
                let result_type = self.resolve_declared_type_fact_in_database(
                    &union_type.value().name,
                    0,
                    database_id,
                )?;
                Ok(Some(hir::CustomTypeOperation::UnionValue {
                    union_type,
                    tag_index,
                    result_type,
                }))
            }
            "union_tag" => {
                if arguments.len() != 1 {
                    crate::bail_parse_error!("union_tag() requires exactly 1 argument");
                }
                let union_type = self
                    .expression_type_fact(&arguments[0], scope)
                    .declared
                    .and_then(|declared| declared.custom().cloned())
                    .filter(|ty| ty.value().is_union())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "union_tag() argument must have a known union type".to_string(),
                        )
                    })?;
                let tag_names = Arc::clone(
                    &union_type
                        .value()
                        .union_def()
                        .expect("union type has a union definition")
                        .tag_names,
                );
                Ok(Some(hir::CustomTypeOperation::UnionTag {
                    union_type,
                    tag_names,
                }))
            }
            "union_extract" => {
                if arguments.len() != 2 {
                    crate::bail_parse_error!("union_extract() requires exactly 2 arguments");
                }
                let tag_name = string_argument(1, "second")?;
                let union_type = self
                    .expression_type_fact(&arguments[0], scope)
                    .declared
                    .and_then(|declared| declared.custom().cloned())
                    .filter(|ty| ty.value().is_union())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "union_extract() first argument must have a known union type"
                                .to_string(),
                        )
                    })?;
                let (tag_index, variant) = union_type
                    .value()
                    .find_union_variant(tag_name)
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "unknown variant '{}' in union type '{}'",
                            tag_name,
                            union_type.value().name
                        ))
                    })?;
                let database_id = union_type
                    .database()
                    .map(DatabaseId::index)
                    .unwrap_or(MAIN_DB_ID);
                let type_name = variant.type_name.clone();
                let result_type = self.resolve_declared_type_fact_in_database(
                    &type_name,
                    variant.array_dimensions,
                    database_id,
                )?;
                Ok(Some(hir::CustomTypeOperation::UnionExtract {
                    union_type,
                    tag_index,
                    result_type,
                }))
            }
            "struct_extract" => {
                if arguments.len() != 2 {
                    crate::bail_parse_error!("struct_extract() requires exactly 2 arguments");
                }
                let field_name = string_argument(1, "second")?;
                let struct_type = self
                    .expression_type_fact(&arguments[0], scope)
                    .declared
                    .and_then(|declared| declared.custom().cloned())
                    .filter(|ty| ty.value().is_struct())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "struct_extract() first argument must have a known struct type"
                                .to_string(),
                        )
                    })?;
                let (field_index, field) = struct_type
                    .value()
                    .find_struct_field(field_name)
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "cannot resolve struct field '{}' for struct_extract",
                            field_name
                        ))
                    })?;
                let database_id = struct_type
                    .database()
                    .map(DatabaseId::index)
                    .unwrap_or(MAIN_DB_ID);
                let type_name = field.type_name.clone();
                let result_type = self.resolve_declared_type_fact_in_database(
                    &type_name,
                    field.array_dimensions,
                    database_id,
                )?;
                Ok(Some(hir::CustomTypeOperation::StructExtract {
                    struct_type,
                    field_index,
                    result_type,
                }))
            }
            _ => Ok(None),
        }
    }

    fn union_value_argument_type(
        &mut self,
        syntax_args: &[Box<ast::Expr>],
        expected_type: Option<&hir::ResolvedType>,
    ) -> Result<Option<hir::ResolvedType>> {
        let Some(union_type) = expected_type.filter(|ty| ty.value().is_union()) else {
            return Ok(None);
        };
        let Some(ast::Expr::Literal(ast::Literal::String(tag))) =
            syntax_args.first().map(AsRef::as_ref)
        else {
            return Ok(None);
        };
        let tag = tag.trim_matches('\'');
        let Some((_, variant)) = union_type.value().find_union_variant(tag) else {
            return Ok(None);
        };
        let type_name = variant.type_name.clone();
        let database = union_type
            .database()
            .map(DatabaseId::index)
            .unwrap_or(MAIN_DB_ID);
        Ok(self
            .resolve_declared_type_fact_in_database(&type_name, variant.array_dimensions, database)?
            .declared
            .and_then(|declared| declared.custom().cloned()))
    }

    fn require_subqueries(&self, policy: &ExprPolicy) -> Result<()> {
        if policy.allow_subqueries {
            Ok(())
        } else {
            crate::bail_parse_error!("subqueries are prohibited in this expression");
        }
    }

    pub(crate) fn validate_existing_expr(
        &self,
        expr: &hir::Expr,
        scope: &Scope,
        policy: &ExprPolicy,
    ) -> Result<()> {
        let features = self.expression_features(expr, scope);
        if !policy.allow_window {
            if let Some(function) = features.window {
                crate::bail_parse_error!("misuse of window function {}()", function);
            }
        }
        if !policy.allow_aggregate {
            if let Some(function) = features.aggregate {
                crate::bail_parse_error!("misuse of aggregate function {}()", function);
            }
        }
        Ok(())
    }

    fn expression_features(&self, expr: &hir::Expr, scope: &Scope) -> ExpressionFeatures {
        let mut features = ExpressionFeatures::default();
        self.collect_expression_features(expr, scope, &mut features);
        features
    }

    fn collect_expression_features(
        &self,
        expr: &hir::Expr,
        scope: &Scope,
        features: &mut ExpressionFeatures,
    ) {
        match expr {
            hir::Expr::Function(call) => {
                let name = call.function.value().to_string();
                if function_is_aggregate(call.function.value()) && features.aggregate.is_none() {
                    features.aggregate = Some(name.clone());
                }
                if (call.window.is_some() || matches!(call.function.value(), Func::Window(_)))
                    && features.window.is_none()
                {
                    features.window = Some(name);
                }
                for argument in &call.arguments {
                    self.collect_expression_features(argument, scope, features);
                }
                for term in &call.argument_order {
                    self.collect_expression_features(&term.expr, scope, features);
                }
                for term in &call.within_group {
                    self.collect_expression_features(&term.expr, scope, features);
                }
                if let Some(filter) = &call.filter {
                    self.collect_expression_features(filter, scope, features);
                }
                if let Some(window) = &call.window {
                    for expression in &window.partition_by {
                        self.collect_expression_features(expression, scope, features);
                    }
                    for term in &window.order_by {
                        self.collect_expression_features(&term.expr, scope, features);
                    }
                }
            }
            hir::Expr::Output(output) => {
                if let Some(output) = scope.output_expr(*output) {
                    self.collect_expression_features(output, scope, features);
                }
            }
            hir::Expr::MergedColumn(column) => {
                self.collect_expression_features(&column.left, scope, features);
            }
            hir::Expr::Unary { expr, .. }
            | hir::Expr::IsNull(expr)
            | hir::Expr::NotNull(expr)
            | hir::Expr::Collate { expr, .. } => {
                self.collect_expression_features(expr, scope, features);
            }
            hir::Expr::Binary { lhs, rhs, .. } => {
                self.collect_expression_features(lhs, scope, features);
                self.collect_expression_features(rhs, scope, features);
            }
            hir::Expr::Between {
                expr, start, end, ..
            } => {
                self.collect_expression_features(expr, scope, features);
                self.collect_expression_features(start, scope, features);
                self.collect_expression_features(end, scope, features);
            }
            hir::Expr::Case {
                base,
                when_then,
                else_expr,
            } => {
                if let Some(base) = base {
                    self.collect_expression_features(base, scope, features);
                }
                for (when, then) in when_then {
                    self.collect_expression_features(when, scope, features);
                    self.collect_expression_features(then, scope, features);
                }
                if let Some(else_expr) = else_expr {
                    self.collect_expression_features(else_expr, scope, features);
                }
            }
            hir::Expr::Cast { expr, target } => {
                self.collect_expression_features(expr, scope, features);
                for parameter in &target.parameters {
                    self.collect_expression_features(parameter, scope, features);
                }
            }
            hir::Expr::InList { lhs, values, .. } => {
                self.collect_expression_features(lhs, scope, features);
                for value in values {
                    self.collect_expression_features(value, scope, features);
                }
            }
            hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, .. }) => {
                self.collect_expression_features(lhs, scope, features);
            }
            hir::Expr::Like {
                lhs, rhs, escape, ..
            } => {
                self.collect_expression_features(lhs, scope, features);
                self.collect_expression_features(rhs, scope, features);
                if let Some(escape) = escape {
                    self.collect_expression_features(escape, scope, features);
                }
            }
            hir::Expr::Row(expressions) | hir::Expr::Array(expressions) => {
                for expression in expressions {
                    self.collect_expression_features(expression, scope, features);
                }
            }
            hir::Expr::Subscript { base, index } => {
                self.collect_expression_features(base, scope, features);
                self.collect_expression_features(index, scope, features);
            }
            hir::Expr::FieldAccess(access) => {
                self.collect_expression_features(&access.base, scope, features);
            }
            hir::Expr::Raise { message, .. } => {
                if let Some(message) = message {
                    self.collect_expression_features(message, scope, features);
                }
            }
            hir::Expr::Literal(_)
            | hir::Expr::Parameter(_)
            | hir::Expr::Column(_)
            | hir::Expr::RowId(_)
            | hir::Expr::Subquery(hir::SubqueryExpr::Scalar { .. })
            | hir::Expr::Subquery(hir::SubqueryExpr::Exists(_)) => {}
        }
    }

    fn subquery_environment(&self, scope: &Scope) -> QueryEnvironment {
        QueryEnvironment::for_subquery(scope)
    }

    /// Represent every output of a query-valued expression explicitly. A
    /// one-column query is scalar; a wider query is a row value whose members
    /// share one query identity and select distinct output registers.
    pub(super) fn subquery_value_expr(&self, query: hir::QueryId) -> Result<hir::Expr> {
        let width = self.query_outputs(query)?.len();
        if width == 0 {
            return Err(LimboError::InternalError(format!(
                "subquery {query} has no output columns"
            )));
        }
        let mut values = (0..width)
            .map(|output| hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }))
            .collect::<Vec<_>>();
        if values.len() == 1 {
            Ok(values.pop().expect("one subquery output was created"))
        } else {
            Ok(hir::Expr::Row(values))
        }
    }
}

#[derive(Default)]
struct ExpressionFeatures {
    aggregate: Option<String>,
    window: Option<String>,
}

fn custom_binary_operator_name(operator: ast::Operator) -> Option<&'static str> {
    match operator {
        ast::Operator::Add => Some("+"),
        ast::Operator::Subtract => Some("-"),
        ast::Operator::Multiply => Some("*"),
        ast::Operator::Divide => Some("/"),
        ast::Operator::Modulus => Some("%"),
        ast::Operator::Less => Some("<"),
        ast::Operator::LessEquals => Some("<="),
        ast::Operator::Greater => Some(">"),
        ast::Operator::GreaterEquals => Some(">="),
        ast::Operator::Equals => Some("="),
        ast::Operator::NotEquals => Some("!="),
        _ => None,
    }
}

fn hir_literal_type_name(expr: &hir::Expr) -> Option<&'static str> {
    let hir::Expr::Literal(literal) = expr else {
        return None;
    };
    match literal {
        ast::Literal::Numeric(value)
            if value
                .as_bytes()
                .iter()
                .any(|byte| matches!(byte, b'.' | b'e' | b'E')) =>
        {
            Some("real")
        }
        ast::Literal::Numeric(_) | ast::Literal::True | ast::Literal::False => Some("integer"),
        ast::Literal::String(_) => Some("text"),
        ast::Literal::Blob(_) => Some("blob"),
        ast::Literal::Null
        | ast::Literal::Keyword(_)
        | ast::Literal::CurrentDate
        | ast::Literal::CurrentTime
        | ast::Literal::CurrentTimestamp => None,
    }
}

fn custom_literal_is_compatible(literal_type: &str, value_input_type: &str) -> bool {
    value_input_type.eq_ignore_ascii_case("any")
        || literal_type.eq_ignore_ascii_case(value_input_type)
}

fn literal_type_fact(literal: &ast::Literal) -> TypeFact {
    match literal {
        ast::Literal::Numeric(value)
            if value
                .as_bytes()
                .iter()
                .any(|byte| matches!(byte, b'.' | b'e' | b'E')) =>
        {
            TypeFact::known(Type::Real)
        }
        ast::Literal::Numeric(_) | ast::Literal::True | ast::Literal::False => {
            TypeFact::known(Type::Integer)
        }
        ast::Literal::String(_)
        | ast::Literal::Keyword(_)
        | ast::Literal::CurrentDate
        | ast::Literal::CurrentTime
        | ast::Literal::CurrentTimestamp => TypeFact::known(Type::Text),
        ast::Literal::Blob(_) => TypeFact::known(Type::Blob),
        ast::Literal::Null => TypeFact::known(Type::Null),
    }
}

fn storage_type(name: &str) -> Type {
    match Affinity::affinity(name) {
        Affinity::Integer => Type::Integer,
        Affinity::Text => Type::Text,
        Affinity::Blob => Type::Blob,
        Affinity::Real => Type::Real,
        Affinity::Numeric => Type::Numeric,
    }
}

fn cast_affinity(target: &hir::TypeName) -> Affinity {
    if target.programs.apply_builtin_affinity {
        if target.name.is_empty() {
            Affinity::Numeric
        } else {
            Affinity::affinity(&target.name)
        }
    } else {
        type_fact_affinity(&target.type_fact)
    }
}

fn type_fact_affinity(fact: &TypeFact) -> Affinity {
    if fact.is_array() {
        return Affinity::Blob;
    }
    let Some(declared) = &fact.declared else {
        return fact
            .storage
            .map_or(Affinity::Blob, |storage| match storage {
                Type::Null | Type::Blob => Affinity::Blob,
                Type::Text => Affinity::Text,
                Type::Numeric => Affinity::Numeric,
                Type::Integer => Affinity::Integer,
                Type::Real => Affinity::Real,
            });
    };
    if declared.custom().is_some() {
        match declared.storage {
            Type::Null | Type::Blob => Affinity::Blob,
            Type::Text => Affinity::Text,
            Type::Numeric => Affinity::Numeric,
            Type::Integer => Affinity::Integer,
            Type::Real => Affinity::Real,
        }
    } else {
        Affinity::affinity(&declared.name)
    }
}

pub(super) fn builtin_function_result_type(
    function: &Func,
    argument_types: &[TypeFact],
    arguments: &[hir::Expr],
    ordered_set_type: Option<&TypeFact>,
) -> TypeFact {
    match function {
        Func::Agg(function) => {
            aggregate_function_result_type(function, argument_types, ordered_set_type)
        }
        Func::Window(function) => window_function_result_type(function, argument_types),
        Func::Scalar(function) => scalar_function_result_type(function, argument_types, arguments),
        Func::Math(function) => math_function_result_type(function, argument_types),
        Func::Vector(function) => vector_function_result_type(function),
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        Func::Fts(function) => fts_function_result_type(function),
        #[cfg(feature = "json")]
        Func::Json(function) => json_function_result_type(function, arguments.len()),
        Func::AlterTable(_) | Func::External(_) | Func::Dialect(_) => TypeFact::dynamic(),
    }
}

fn aggregate_function_result_type(
    function: &AggFunc,
    argument_types: &[TypeFact],
    ordered_set_type: Option<&TypeFact>,
) -> TypeFact {
    match function {
        AggFunc::Count | AggFunc::Count0 => TypeFact::known(Type::Integer),
        // SQLite's SUM can return either integer or real. A physical lowering
        // may choose a narrower representation, but the semantic fact cannot.
        AggFunc::Sum => TypeFact::known(Type::Numeric),
        AggFunc::Avg | AggFunc::Total | AggFunc::PercentileCont => TypeFact::known(Type::Real),
        AggFunc::Min | AggFunc::Max => argument_types
            .first()
            .cloned()
            .unwrap_or_else(TypeFact::dynamic),
        AggFunc::GroupConcat | AggFunc::StringAgg => TypeFact::known(Type::Text),
        AggFunc::ArrayAgg => array_aggregate_result_type(argument_types.first()),
        AggFunc::Mode | AggFunc::PercentileDisc => {
            ordered_set_type.cloned().unwrap_or_else(TypeFact::dynamic)
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonGroupObject => TypeFact::known(Type::Text),
        #[cfg(feature = "json")]
        AggFunc::JsonbGroupArray | AggFunc::JsonbGroupObject => TypeFact::known(Type::Blob),
        AggFunc::External(_) => TypeFact::dynamic(),
    }
}

fn window_function_result_type(function: &WindowFunc, argument_types: &[TypeFact]) -> TypeFact {
    match function {
        WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank | WindowFunc::Ntile => {
            TypeFact::known(Type::Integer)
        }
        WindowFunc::PercentRank | WindowFunc::CumeDist => TypeFact::known(Type::Real),
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue => argument_types
            .first()
            .cloned()
            .unwrap_or_else(TypeFact::dynamic),
        WindowFunc::Lag | WindowFunc::Lead => selected_value_result(
            argument_types
                .first()
                .into_iter()
                .chain(argument_types.get(2)),
        ),
        WindowFunc::External(_) => TypeFact::dynamic(),
    }
}

fn scalar_function_result_type(
    function: &ScalarFunc,
    argument_types: &[TypeFact],
    arguments: &[hir::Expr],
) -> TypeFact {
    match function {
        ScalarFunc::Changes
        | ScalarFunc::Glob
        | ScalarFunc::Instr
        | ScalarFunc::Like
        | ScalarFunc::Random
        | ScalarFunc::Length
        | ScalarFunc::OctetLength
        | ScalarFunc::Sign
        | ScalarFunc::TotalChanges
        | ScalarFunc::Unicode
        | ScalarFunc::GetByte
        | ScalarFunc::LastInsertRowid
        | ScalarFunc::ConnTxnId
        | ScalarFunc::IsAutocommit
        | ScalarFunc::SequenceWatermark
        | ScalarFunc::TestUintLt
        | ScalarFunc::TestUintEq
        | ScalarFunc::Gcd
        | ScalarFunc::Lcm
        | ScalarFunc::BooleanToInt
        | ScalarFunc::NumericLt
        | ScalarFunc::NumericEq
        | ScalarFunc::ArrayLength
        | ScalarFunc::ArrayContains
        | ScalarFunc::ArrayPosition
        | ScalarFunc::ArrayOverlap
        | ScalarFunc::ArrayContainsAll
        | ScalarFunc::NextVal
        | ScalarFunc::CurrVal
        | ScalarFunc::SetVal => TypeFact::known(Type::Integer),
        #[cfg(feature = "test_helper")]
        ScalarFunc::TestNondetCounter => TypeFact::known(Type::Integer),
        ScalarFunc::Char
        | ScalarFunc::Concat
        | ScalarFunc::ConcatWs
        | ScalarFunc::Upper
        | ScalarFunc::Lower
        | ScalarFunc::Trim
        | ScalarFunc::LTrim
        | ScalarFunc::RTrim
        | ScalarFunc::Soundex
        | ScalarFunc::Date
        | ScalarFunc::Time
        | ScalarFunc::DateTime
        | ScalarFunc::Typeof
        | ScalarFunc::Unistr
        | ScalarFunc::UnistrQuote
        | ScalarFunc::Quote
        | ScalarFunc::SqliteVersion
        | ScalarFunc::TursoVersion
        | ScalarFunc::SqliteSourceId
        | ScalarFunc::Hex
        | ScalarFunc::Replace
        | ScalarFunc::StrfTime
        | ScalarFunc::Printf
        | ScalarFunc::TimeDiff
        | ScalarFunc::TableColumnsJsonArray
        | ScalarFunc::BinRecordJsonObject
        | ScalarFunc::StatGet
        | ScalarFunc::TestUintEncode
        | ScalarFunc::TestUintAdd
        | ScalarFunc::TestUintSub
        | ScalarFunc::TestUintMul
        | ScalarFunc::TestUintDiv
        | ScalarFunc::StringReverse
        | ScalarFunc::Repeat
        | ScalarFunc::Lpad
        | ScalarFunc::Rpad
        | ScalarFunc::IntToBoolean
        | ScalarFunc::ValidateIpAddr
        | ScalarFunc::NumericDecode
        | ScalarFunc::NumericAdd
        | ScalarFunc::NumericSub
        | ScalarFunc::NumericMul
        | ScalarFunc::NumericDiv
        | ScalarFunc::ArrayToString
        | ScalarFunc::UnionTagFunc => TypeFact::known(Type::Text),
        ScalarFunc::Round | ScalarFunc::JulianDay => TypeFact::known(Type::Real),
        ScalarFunc::RandomBlob
        | ScalarFunc::Unhex
        | ScalarFunc::SetByte
        | ScalarFunc::ZeroBlob
        | ScalarFunc::StatInit
        | ScalarFunc::StatPush
        | ScalarFunc::NumericEncode
        | ScalarFunc::StructPack => TypeFact::known(Type::Blob),
        ScalarFunc::Array => TypeFact::array_literal_result(argument_types.iter().cloned()),
        ScalarFunc::StringToArray => TypeFact::known_array(1),
        ScalarFunc::Coalesce | ScalarFunc::IfNull | ScalarFunc::Min | ScalarFunc::Max => {
            selected_value_result(argument_types)
        }
        ScalarFunc::Iif => iif_result_type(argument_types),
        ScalarFunc::Abs => abs_result_type(argument_types.first()),
        ScalarFunc::Nullif
        | ScalarFunc::Likely
        | ScalarFunc::Likelihood
        | ScalarFunc::Unlikely
        | ScalarFunc::TestUintDecode => argument_types
            .first()
            .cloned()
            .unwrap_or_else(TypeFact::dynamic),
        ScalarFunc::Substr | ScalarFunc::Substring => substring_result_type(argument_types.first()),
        ScalarFunc::UnixEpoch => unixepoch_result_type(arguments),
        ScalarFunc::ArrayElement => array_element_result_type(argument_types.first()),
        ScalarFunc::ArraySetElement => argument_types
            .first()
            .zip(argument_types.get(2))
            .map_or_else(
                || TypeFact::known_array(1),
                |(container, element)| TypeFact::array_with_element_result(container, element),
            ),
        ScalarFunc::ArrayAppend => argument_types
            .first()
            .zip(argument_types.get(1))
            .map_or_else(
                || TypeFact::known_array(1),
                |(container, element)| TypeFact::array_with_element_result(container, element),
            ),
        ScalarFunc::ArrayPrepend => argument_types
            .first()
            .zip(argument_types.get(1))
            .map_or_else(
                || TypeFact::known_array(1),
                |(element, container)| TypeFact::array_with_element_result(container, element),
            ),
        ScalarFunc::ArrayRemove | ScalarFunc::ArraySlice => {
            array_result_type(argument_types.first())
        }
        ScalarFunc::ArrayCat => argument_types
            .first()
            .zip(argument_types.get(1))
            .map_or_else(
                || TypeFact::known_array(1),
                |(lhs, rhs)| TypeFact::array_concat_result(lhs, rhs),
            ),
        ScalarFunc::Attach | ScalarFunc::Detach => TypeFact::known(Type::Null),
        ScalarFunc::Cast
        | ScalarFunc::StructExtractFunc
        | ScalarFunc::UnionValueFunc
        | ScalarFunc::UnionExtractFunc => TypeFact::dynamic(),
        #[cfg(feature = "fs")]
        #[cfg(not(target_family = "wasm"))]
        ScalarFunc::LoadExtension => TypeFact::known(Type::Null),
    }
}

fn math_function_result_type(function: &MathFunc, argument_types: &[TypeFact]) -> TypeFact {
    match function {
        MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc => {
            match argument_types.first().and_then(|fact| fact.storage) {
                Some(Type::Integer) => TypeFact::known(Type::Integer),
                Some(Type::Null) => TypeFact::known(Type::Null),
                Some(Type::Real) | Some(Type::Text) | Some(Type::Blob) => {
                    TypeFact::known(Type::Real)
                }
                Some(Type::Numeric) | None => TypeFact::dynamic(),
            }
        }
        MathFunc::Acos
        | MathFunc::Acosh
        | MathFunc::Asin
        | MathFunc::Asinh
        | MathFunc::Atan
        | MathFunc::Atan2
        | MathFunc::Atanh
        | MathFunc::Cos
        | MathFunc::Cosh
        | MathFunc::Degrees
        | MathFunc::Exp
        | MathFunc::Ln
        | MathFunc::Log
        | MathFunc::Log10
        | MathFunc::Log2
        | MathFunc::Mod
        | MathFunc::Pi
        | MathFunc::Pow
        | MathFunc::Power
        | MathFunc::Radians
        | MathFunc::Sin
        | MathFunc::Sinh
        | MathFunc::Sqrt
        | MathFunc::Tan
        | MathFunc::Tanh => TypeFact::known(Type::Real),
    }
}

fn vector_function_result_type(function: &VectorFunc) -> TypeFact {
    match function {
        VectorFunc::Vector
        | VectorFunc::Vector32
        | VectorFunc::Vector32Sparse
        | VectorFunc::Vector64
        | VectorFunc::Vector8
        | VectorFunc::Vector1Bit
        | VectorFunc::VectorConcat
        | VectorFunc::VectorSlice => TypeFact::known(Type::Blob),
        VectorFunc::VectorExtract => TypeFact::known(Type::Text),
        VectorFunc::VectorDistanceCos
        | VectorFunc::VectorDistanceL2
        | VectorFunc::VectorDistanceJaccard
        | VectorFunc::VectorDistanceDot => TypeFact::known(Type::Real),
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
fn fts_function_result_type(function: &FtsFunc) -> TypeFact {
    match function {
        FtsFunc::Score => TypeFact::known(Type::Real),
        FtsFunc::Match => TypeFact::known(Type::Integer),
        FtsFunc::Highlight => TypeFact::known(Type::Text),
    }
}

#[cfg(feature = "json")]
fn json_function_result_type(function: &JsonFunc, argument_count: usize) -> TypeFact {
    match function {
        JsonFunc::Json
        | JsonFunc::JsonArray
        | JsonFunc::JsonObject
        | JsonFunc::JsonPatch
        | JsonFunc::JsonRemove
        | JsonFunc::JsonReplace
        | JsonFunc::JsonInsert
        | JsonFunc::JsonPretty
        | JsonFunc::JsonSet
        | JsonFunc::JsonQuote
        | JsonFunc::JsonType
        | JsonFunc::JsonArrowExtract => TypeFact::known(Type::Text),
        JsonFunc::Jsonb
        | JsonFunc::JsonbArray
        | JsonFunc::JsonbObject
        | JsonFunc::JsonbPatch
        | JsonFunc::JsonbRemove
        | JsonFunc::JsonbReplace
        | JsonFunc::JsonbInsert
        | JsonFunc::JsonbSet => TypeFact::known(Type::Blob),
        JsonFunc::JsonArrayLength | JsonFunc::JsonErrorPosition | JsonFunc::JsonValid => {
            TypeFact::known(Type::Integer)
        }
        JsonFunc::JsonExtract if argument_count > 2 => TypeFact::known(Type::Text),
        JsonFunc::JsonbExtract if argument_count > 2 => TypeFact::known(Type::Blob),
        JsonFunc::JsonExtract | JsonFunc::JsonbExtract | JsonFunc::JsonArrowShiftExtract => {
            TypeFact::dynamic()
        }
    }
}

fn selected_value_result<'a>(facts: impl IntoIterator<Item = &'a TypeFact>) -> TypeFact {
    TypeFact::selected_value_result(facts)
}

fn iif_result_type(argument_types: &[TypeFact]) -> TypeFact {
    let last = argument_types.len().checked_sub(1);
    selected_value_result(
        argument_types
            .iter()
            .enumerate()
            .filter_map(|(index, fact)| {
                (index % 2 == 1 || (argument_types.len() % 2 == 1 && Some(index) == last))
                    .then_some(fact)
            }),
    )
}

fn abs_result_type(argument: Option<&TypeFact>) -> TypeFact {
    match argument.and_then(|fact| fact.storage) {
        Some(Type::Null) => TypeFact::known(Type::Null),
        Some(Type::Integer) => TypeFact::known(Type::Integer),
        Some(Type::Real) | Some(Type::Text) | Some(Type::Blob) => TypeFact::known(Type::Real),
        Some(Type::Numeric) => TypeFact::known(Type::Numeric),
        None => TypeFact::dynamic(),
    }
}

fn substring_result_type(argument: Option<&TypeFact>) -> TypeFact {
    match argument.and_then(|fact| fact.storage) {
        Some(Type::Null) => TypeFact::known(Type::Null),
        Some(Type::Blob) => TypeFact::known(Type::Blob),
        Some(_) => TypeFact::known(Type::Text),
        None => TypeFact::dynamic(),
    }
}

fn unixepoch_result_type(arguments: &[hir::Expr]) -> TypeFact {
    let mut has_dynamic_modifier = false;
    for argument in arguments.iter().skip(1) {
        let hir::Expr::Literal(ast::Literal::String(modifier)) = argument else {
            has_dynamic_modifier = true;
            continue;
        };
        let modifier = modifier.trim_matches('\'');
        if modifier.eq_ignore_ascii_case("subsec") || modifier.eq_ignore_ascii_case("subsecond") {
            return TypeFact::known(Type::Real);
        }
    }
    if has_dynamic_modifier {
        TypeFact::dynamic()
    } else {
        TypeFact::known(Type::Integer)
    }
}

fn array_result_type(argument: Option<&TypeFact>) -> TypeFact {
    argument
        .filter(|fact| fact.is_array())
        .cloned()
        .unwrap_or_else(|| TypeFact::known_array(1))
}

fn array_element_result_type(argument: Option<&TypeFact>) -> TypeFact {
    let Some(mut result) = argument.cloned() else {
        return TypeFact::dynamic();
    };
    if !result.is_array() {
        return TypeFact::dynamic();
    }
    result.array_dimensions = result.array_dimensions.saturating_sub(1);
    if let Some(declared) = result.declared.as_mut() {
        declared.array_dimensions = declared.array_dimensions.saturating_sub(1);
        declared.storage = if declared.array_dimensions == 0 {
            storage_type(&declared.name)
        } else {
            Type::Blob
        };
        result.storage = Some(declared.storage);
    } else if result.is_array() {
        result.storage = Some(Type::Blob);
    } else {
        result.storage = None;
    }
    result
}

fn array_aggregate_result_type(argument: Option<&TypeFact>) -> TypeFact {
    let Some(mut result) = argument.cloned() else {
        return TypeFact::known_array(1);
    };
    result.array_rank_unbounded |= result.storage.is_none();
    result.array_dimensions = result
        .array_dimensions
        .checked_add(1)
        .expect("array rank overflow during semantic function analysis");
    if let Some(declared) = result.declared.as_mut() {
        declared.array_dimensions = result.array_dimensions;
        declared.storage = Type::Blob;
    }
    result.storage = Some(Type::Blob);
    result
}

fn custom_operation_result(operation: &Option<hir::CustomTypeOperation>) -> Option<TypeFact> {
    match operation {
        Some(hir::CustomTypeOperation::UnionValue { result_type, .. })
        | Some(hir::CustomTypeOperation::UnionExtract { result_type, .. })
        | Some(hir::CustomTypeOperation::StructExtract { result_type, .. }) => {
            Some(result_type.clone())
        }
        Some(hir::CustomTypeOperation::UnionTag { .. }) => Some(TypeFact::known(Type::Text)),
        None => None,
    }
}

fn validate_function_call(
    function: &Func,
    function_name: &str,
    syntax_args: &[Box<ast::Expr>],
) -> Result<()> {
    let argument_count = syntax_args.len();
    let valid_arity = match function {
        Func::Scalar(ScalarFunc::Coalesce | ScalarFunc::Iif) => argument_count >= 2,
        Func::Scalar(ScalarFunc::TableColumnsJsonArray) => argument_count == 1,
        Func::Scalar(ScalarFunc::BinRecordJsonObject) => argument_count == 2,
        Func::Scalar(
            ScalarFunc::Cast
            | ScalarFunc::StatInit
            | ScalarFunc::StatPush
            | ScalarFunc::StatGet
            | ScalarFunc::Attach
            | ScalarFunc::Detach
            | ScalarFunc::ConnTxnId
            | ScalarFunc::IsAutocommit,
        ) => true,
        Func::Scalar(function) => arity_list_accepts(function.arities(), argument_count),
        Func::Agg(AggFunc::External(function)) => function.matches_arg_count(argument_count),
        Func::Agg(AggFunc::Mode) => argument_count == 0,
        Func::Agg(AggFunc::PercentileCont | AggFunc::PercentileDisc) => argument_count == 1,
        Func::Agg(function) => arity_list_accepts(function.arities(), argument_count),
        Func::Window(WindowFunc::External(function)) => function.matches_arg_count(argument_count),
        Func::Window(function) => arity_list_accepts(function.arities(), argument_count),
        Func::Math(function) => arity_list_accepts(function.arities(), argument_count),
        Func::Vector(function) => arity_list_accepts(function.arities(), argument_count),
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        Func::Fts(function) => arity_list_accepts(function.arities(), argument_count),
        #[cfg(feature = "json")]
        Func::Json(function) => arity_list_accepts(function.arities(), argument_count),
        Func::External(function) => function.func.matches_arg_count(argument_count),
        Func::AlterTable(_) | Func::Dialect(_) => true,
    };
    if !valid_arity {
        let required_arity = match function {
            Func::Scalar(ScalarFunc::UnionTagFunc) => Some(1),
            Func::Scalar(
                ScalarFunc::UnionValueFunc
                | ScalarFunc::UnionExtractFunc
                | ScalarFunc::StructExtractFunc,
            ) => Some(2),
            _ => None,
        };
        if let Some(required_arity) = required_arity {
            let argument = if required_arity == 1 {
                "argument"
            } else {
                "arguments"
            };
            crate::bail_parse_error!(
                "{}() requires exactly {} {}",
                function_name,
                required_arity,
                argument
            );
        }
        crate::bail_parse_error!("wrong number of arguments to function {}()", function_name);
    }

    if matches!(function, Func::Scalar(ScalarFunc::Likelihood)) {
        validate_likelihood_probability(&syntax_args[1])?;
    }
    Ok(())
}

fn arity_list_accepts(arities: &[i32], argument_count: usize) -> bool {
    arities
        .iter()
        .any(|arity| *arity < 0 || *arity as usize == argument_count)
}

fn validate_likelihood_probability(argument: &ast::Expr) -> Result<()> {
    let ast::Expr::Literal(ast::Literal::Numeric(value)) = argument else {
        crate::bail_parse_error!(
            "second argument to likelihood() must be a constant between 0.0 and 1.0"
        );
    };
    let probability = value.parse::<f64>().map_err(|_| {
        LimboError::ParseError(
            "second argument to likelihood() must be a floating point constant".to_string(),
        )
    })?;
    if !(0.0..=1.0).contains(&probability) {
        crate::bail_parse_error!(
            "second argument to likelihood() must be a constant between 0.0 and 1.0"
        );
    }
    if !value.contains('.') {
        crate::bail_parse_error!(
            "second argument to likelihood() must be a floating point number with decimal point"
        );
    }
    Ok(())
}

fn function_is_aggregate(function: &Func) -> bool {
    match function {
        Func::Agg(_) => true,
        Func::External(function) => function.func.is_aggregate(),
        _ => false,
    }
}
