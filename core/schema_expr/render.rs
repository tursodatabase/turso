use crate::{LimboError, Result};
use turso_parser::ast;

use super::{SchemaExpr, SchemaExprNode, SchemaTypeName, SchemaTypeSize, ValidSchemaExpr};

impl SchemaExpr {
    /// Render with the owning table's current column names. Unresolved syntax
    /// is rendered exactly from the parser tree retained at schema load.
    pub(crate) fn render<T: AsRef<str>>(&self, column_names: &[T]) -> Result<String> {
        Ok(self.render_syntax(column_names)?.to_string())
    }

    /// Rebuild parser syntax from positional column identities using the
    /// owning table's current column names.
    pub(crate) fn render_syntax<T: AsRef<str>>(&self, column_names: &[T]) -> Result<ast::Expr> {
        self.render_syntax_with(|position| {
            column_names
                .get(position)
                .map(|name| name.as_ref().to_owned())
        })
    }

    /// Render with a caller-provided positional column lookup.
    pub(crate) fn render_with(
        &self,
        column_name: impl FnMut(usize) -> Option<String>,
    ) -> Result<String> {
        Ok(self.render_syntax_with(column_name)?.to_string())
    }

    pub(crate) fn render_syntax_with(
        &self,
        mut column_name: impl FnMut(usize) -> Option<String>,
    ) -> Result<ast::Expr> {
        match self {
            Self::Valid(expr) => to_syntax(&expr.root, &mut column_name),
            Self::Unresolved(expr) => Ok((*expr.syntax).clone()),
        }
    }
}

impl ValidSchemaExpr {
    pub(crate) fn render<T: AsRef<str>>(&self, column_names: &[T]) -> Result<String> {
        self.render_with(&mut |position| {
            column_names
                .get(position)
                .map(|name| name.as_ref().to_owned())
        })
    }

    pub(crate) fn render_with(
        &self,
        column_name: &mut impl FnMut(usize) -> Option<String>,
    ) -> Result<String> {
        Ok(to_syntax(&self.root, column_name)?.to_string())
    }
}

fn to_syntax(
    expr: &SchemaExprNode,
    column_name: &mut impl FnMut(usize) -> Option<String>,
) -> Result<ast::Expr> {
    match expr {
        SchemaExprNode::Between {
            lhs,
            not,
            start,
            end,
        } => Ok(ast::Expr::Between {
            lhs: Box::new(to_syntax(lhs, column_name)?),
            not: *not,
            start: Box::new(to_syntax(start, column_name)?),
            end: Box::new(to_syntax(end, column_name)?),
        }),
        SchemaExprNode::Binary(lhs, operator, rhs) => Ok(ast::Expr::Binary(
            Box::new(to_syntax(lhs, column_name)?),
            *operator,
            Box::new(to_syntax(rhs, column_name)?),
        )),
        SchemaExprNode::Case {
            base,
            when_then_pairs,
            else_expr,
        } => Ok(ast::Expr::Case {
            base: base
                .as_deref()
                .map(|expr| to_syntax(expr, column_name).map(Box::new))
                .transpose()?,
            when_then_pairs: when_then_pairs
                .iter()
                .map(|(when, then)| {
                    Ok((
                        Box::new(to_syntax(when, column_name)?),
                        Box::new(to_syntax(then, column_name)?),
                    ))
                })
                .collect::<Result<_>>()?,
            else_expr: else_expr
                .as_deref()
                .map(|expr| to_syntax(expr, column_name).map(Box::new))
                .transpose()?,
        }),
        SchemaExprNode::Cast {
            expr, type_name, ..
        } => Ok(ast::Expr::Cast {
            expr: Box::new(to_syntax(expr, column_name)?),
            type_name: Some(render_type_name(type_name, column_name)?),
        }),
        SchemaExprNode::Collate { expr, name, .. } => Ok(ast::Expr::Collate(
            Box::new(to_syntax(expr, column_name)?),
            name.clone(),
        )),
        SchemaExprNode::FieldAccess { base, field, .. } => Ok(ast::Expr::FieldAccess {
            base: Box::new(to_syntax(base, column_name)?),
            field: field.clone(),
        }),
        SchemaExprNode::CustomTypeFunction { call, .. } => to_syntax(call, column_name),
        SchemaExprNode::Function {
            name,
            distinctness,
            args,
            star,
            ..
        } => {
            let filter_over = ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            };
            if *star {
                Ok(ast::Expr::FunctionCallStar {
                    name: name.clone(),
                    filter_over,
                })
            } else {
                Ok(ast::Expr::FunctionCall {
                    name: name.clone(),
                    distinctness: *distinctness,
                    args: args
                        .iter()
                        .map(|arg| to_syntax(arg, column_name).map(Box::new))
                        .collect::<Result<_>>()?,
                    order_by: Vec::new(),
                    within_group: Vec::new(),
                    filter_over,
                })
            }
        }
        SchemaExprNode::SelfColumn(column) => {
            let Some(name) = column_name(column.position()) else {
                return Err(LimboError::InternalError(format!(
                    "cannot render stored expression: column position {} does not exist",
                    column.position()
                )));
            };
            Ok(ast::Expr::Id(ast::Name::exact(name)))
        }
        SchemaExprNode::SelfRowId => Ok(ast::Expr::Id(ast::Name::exact("rowid".to_string()))),
        SchemaExprNode::DomainValue => Ok(ast::Expr::Id(ast::Name::exact("value".to_string()))),
        SchemaExprNode::TypeParameter { name, .. } => {
            Ok(ast::Expr::Id(ast::Name::exact(name.clone())))
        }
        SchemaExprNode::InList { lhs, not, rhs } => Ok(ast::Expr::InList {
            lhs: Box::new(to_syntax(lhs, column_name)?),
            not: *not,
            rhs: rhs
                .iter()
                .map(|expr| to_syntax(expr, column_name).map(Box::new))
                .collect::<Result<_>>()?,
        }),
        SchemaExprNode::IsNull(expr) => {
            Ok(ast::Expr::IsNull(Box::new(to_syntax(expr, column_name)?)))
        }
        SchemaExprNode::Like {
            lhs,
            not,
            op,
            rhs,
            escape,
        } => Ok(ast::Expr::Like {
            lhs: Box::new(to_syntax(lhs, column_name)?),
            not: *not,
            op: *op,
            rhs: Box::new(to_syntax(rhs, column_name)?),
            escape: escape
                .as_deref()
                .map(|expr| to_syntax(expr, column_name).map(Box::new))
                .transpose()?,
        }),
        SchemaExprNode::Literal(literal) => Ok(ast::Expr::Literal(literal.clone())),
        SchemaExprNode::NotNull(expr) => {
            Ok(ast::Expr::NotNull(Box::new(to_syntax(expr, column_name)?)))
        }
        SchemaExprNode::Parenthesized(exprs) => Ok(ast::Expr::Parenthesized(
            exprs
                .iter()
                .map(|expr| to_syntax(expr, column_name).map(Box::new))
                .collect::<Result<_>>()?,
        )),
        SchemaExprNode::Unary(operator, expr) => Ok(ast::Expr::Unary(
            *operator,
            Box::new(to_syntax(expr, column_name)?),
        )),
        SchemaExprNode::Array(elements) => Ok(ast::Expr::Array {
            elements: elements
                .iter()
                .map(|expr| to_syntax(expr, column_name).map(Box::new))
                .collect::<Result<_>>()?,
        }),
        SchemaExprNode::Subscript { base, index } => Ok(ast::Expr::Subscript {
            base: Box::new(to_syntax(base, column_name)?),
            index: Box::new(to_syntax(index, column_name)?),
        }),
        SchemaExprNode::Raise { action, message } => Ok(ast::Expr::Raise(
            *action,
            message
                .as_deref()
                .map(|message| to_syntax(message, column_name).map(Box::new))
                .transpose()?,
        )),
    }
}

fn render_type_name(
    type_name: &SchemaTypeName,
    column_name: &mut impl FnMut(usize) -> Option<String>,
) -> Result<ast::Type> {
    let size = match &type_name.size {
        None => None,
        Some(SchemaTypeSize::MaxSize(size)) => Some(ast::TypeSize::MaxSize(Box::new(to_syntax(
            size,
            column_name,
        )?))),
        Some(SchemaTypeSize::TypeSize(precision, scale)) => Some(ast::TypeSize::TypeSize(
            Box::new(to_syntax(precision, column_name)?),
            Box::new(to_syntax(scale, column_name)?),
        )),
    };
    Ok(ast::Type {
        name: type_name.name.clone(),
        size,
        array_dimensions: type_name.array_dimensions,
    })
}
