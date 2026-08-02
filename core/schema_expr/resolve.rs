use crate::function::{Deterministic, Func, MathFunc, ScalarFunc};
use crate::{LimboError, Result};
use turso_parser::ast::{self, Literal, Operator, UnaryOperator};

use super::{
    ResolutionMode, SchemaColumn, SchemaCustomTypeFunction, SchemaExpr, SchemaExprContext,
    SchemaExprNode, SchemaExprProfile, SchemaExprResolver, SchemaFieldAccess, SchemaTable,
    SchemaTypeName, SchemaTypeParameter, SchemaTypeSize, SchemaValueType, SelfColumn,
    UnresolvedSchemaExpr, ValidSchemaExpr,
};

const ROWID_NAMES: [&str; 3] = ["rowid", "_rowid_", "oid"];

pub(super) fn resolve(
    syntax: &ast::Expr,
    profile: SchemaExprProfile,
    context: SchemaExprContext<'_>,
    resolver: &dyn SchemaExprResolver,
    mode: ResolutionMode,
) -> Result<SchemaExpr> {
    let table = context.table();
    if profile.allows_table_columns() && table.is_none() {
        return Err(LimboError::InternalError(format!(
            "resolving stored {} requires its owning table",
            profile.description()
        )));
    }

    let state = ResolverState {
        profile,
        table,
        expected_type: context.expected_type().map(ToOwned::to_owned),
        type_parameters: context.type_parameters(),
        default_column_name: context.default_column_name(),
        resolver,
    };
    let resolved = state.root(syntax).and_then(|root| {
        if matches!(profile, SchemaExprProfile::Check { strict_types: true }) {
            StrictCheck::new(
                table.expect("CHECK resolution requires an owning table"),
                resolver,
            )
            .validate(&root)?;
        }
        Ok(ValidSchemaExpr { profile, root })
    });

    match resolved {
        Ok(expr) => Ok(SchemaExpr::Valid(expr)),
        Err(error)
            if mode == ResolutionMode::PreserveUnresolved
                && matches!(&error, LimboError::ParseError(_)) =>
        {
            Ok(SchemaExpr::Unresolved(UnresolvedSchemaExpr {
                profile,
                syntax: Box::new(syntax.clone()),
                error: Some(error),
            }))
        }
        Err(error) => Err(error),
    }
}

struct ResolverState<'a> {
    profile: SchemaExprProfile,
    table: Option<&'a SchemaTable>,
    expected_type: Option<String>,
    type_parameters: Option<&'a [SchemaTypeParameter]>,
    default_column_name: Option<&'a str>,
    resolver: &'a dyn SchemaExprResolver,
}

impl ResolverState<'_> {
    fn root(&self, expr: &ast::Expr) -> Result<SchemaExprNode> {
        // SQLite's DEFAULT-name compatibility rule applies only to the bare
        // root expression. Once the name is parenthesized or nested inside
        // another expression it is a forbidden column reference.
        if self.profile == SchemaExprProfile::Default {
            if let ast::Expr::Id(name) | ast::Expr::Name(name) = expr {
                return Ok(SchemaExprNode::Literal(Literal::String(name.as_literal())));
            }
        }
        self.node(expr)
    }

    fn node(&self, expr: &ast::Expr) -> Result<SchemaExprNode> {
        match expr {
            ast::Expr::Between {
                lhs,
                not,
                start,
                end,
            } => Ok(SchemaExprNode::Between {
                lhs: Box::new(self.node(lhs)?),
                not: *not,
                start: Box::new(self.node(start)?),
                end: Box::new(self.node(end)?),
            }),
            ast::Expr::Binary(lhs, operator, rhs) => Ok(SchemaExprNode::Binary(
                Box::new(self.node(lhs)?),
                *operator,
                Box::new(self.node(rhs)?),
            )),
            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => Ok(SchemaExprNode::Case {
                base: base
                    .as_deref()
                    .map(|expr| self.node(expr).map(Box::new))
                    .transpose()?,
                when_then_pairs: when_then_pairs
                    .iter()
                    .map(|(when, then)| {
                        Ok((Box::new(self.node(when)?), Box::new(self.node(then)?)))
                    })
                    .collect::<Result<_>>()?,
                else_expr: else_expr
                    .as_deref()
                    .map(|expr| self.node(expr).map(Box::new))
                    .transpose()?,
            }),
            ast::Expr::Cast { expr, type_name } => {
                let Some(type_name) = type_name else {
                    return parse_error(format!(
                        "stored {} contains CAST without a target type",
                        self.profile.description()
                    ));
                };
                let resolved_type = self.resolver.resolve_type(&type_name.name)?;
                Ok(SchemaExprNode::Cast {
                    expr: Box::new(self.node(expr)?),
                    type_name: self.type_name(type_name)?,
                    resolved_type,
                })
            }
            ast::Expr::Collate(expr, name) => {
                let collation = self.resolver.resolve_collation(name.as_str())?;
                if collation.is_custom() {
                    return parse_error(match self.profile {
                        SchemaExprProfile::IndexKey | SchemaExprProfile::PartialIndexPredicate => {
                            "custom collations are not supported in indexes".to_string()
                        }
                        _ => {
                            "custom collations are not supported in schema definitions".to_string()
                        }
                    });
                }
                Ok(SchemaExprNode::Collate {
                    expr: Box::new(self.node(expr)?),
                    name: name.clone(),
                    collation,
                })
            }
            ast::Expr::FieldAccess { base, field, .. } => {
                let base = self.node(base)?;
                self.field_access(base, field)
            }
            ast::Expr::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                within_group,
                filter_over,
            } => {
                self.validate_function_tail(
                    name.as_str(),
                    order_by.is_empty(),
                    within_group.is_empty(),
                    filter_over,
                )?;
                if is_custom_type_function(name.as_str()) {
                    return self.custom_type_function(name, *distinctness, args);
                }
                let function = self.resolve_function(name.as_str(), args.len(), args, false)?;
                Ok(SchemaExprNode::Function {
                    name: name.clone(),
                    function,
                    distinctness: *distinctness,
                    args: args
                        .iter()
                        .map(|arg| self.node_with_expected(arg, None))
                        .collect::<Result<_>>()?,
                    star: false,
                })
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                self.validate_function_tail(name.as_str(), true, true, filter_over)?;
                let function = self.resolve_function(name.as_str(), 0, &[], false)?;
                Ok(SchemaExprNode::Function {
                    name: name.clone(),
                    function,
                    distinctness: None,
                    args: Vec::new(),
                    star: true,
                })
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => self.unqualified(name),
            ast::Expr::Qualified(qualifier, column) => self.qualified(qualifier, column),
            ast::Expr::DoublyQualified(first, second, third) => {
                self.doubly_qualified(first, second, third)
            }
            ast::Expr::InList { lhs, not, rhs } => Ok(SchemaExprNode::InList {
                lhs: Box::new(self.node(lhs)?),
                not: *not,
                rhs: rhs
                    .iter()
                    .map(|expr| self.node(expr))
                    .collect::<Result<_>>()?,
            }),
            ast::Expr::IsNull(expr) => Ok(SchemaExprNode::IsNull(Box::new(self.node(expr)?))),
            ast::Expr::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => Ok(SchemaExprNode::Like {
                lhs: Box::new(self.node(lhs)?),
                not: *not,
                op: *op,
                rhs: Box::new(self.node(rhs)?),
                escape: escape
                    .as_deref()
                    .map(|expr| self.node(expr).map(Box::new))
                    .transpose()?,
            }),
            ast::Expr::Literal(literal) => {
                if self.profile.rejects_current_time_literals()
                    && matches!(
                        literal,
                        Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp
                    )
                {
                    return self.nondeterministic_error();
                }
                Ok(SchemaExprNode::Literal(literal.clone()))
            }
            ast::Expr::NotNull(expr) => Ok(SchemaExprNode::NotNull(Box::new(self.node(expr)?))),
            ast::Expr::Parenthesized(exprs) => Ok(SchemaExprNode::Parenthesized(
                exprs
                    .iter()
                    .map(|expr| self.node(expr))
                    .collect::<Result<_>>()?,
            )),
            ast::Expr::Unary(operator, expr) => {
                Ok(SchemaExprNode::Unary(*operator, Box::new(self.node(expr)?)))
            }
            ast::Expr::Array { elements } => Ok(SchemaExprNode::Array(
                elements
                    .iter()
                    .map(|expr| self.node(expr))
                    .collect::<Result<_>>()?,
            )),
            ast::Expr::Subscript { base, index } => Ok(SchemaExprNode::Subscript {
                base: Box::new(self.node(base)?),
                index: Box::new(self.node(index)?),
            }),
            ast::Expr::Variable(_) => self.variable_error(),
            ast::Expr::Exists(_)
            | ast::Expr::InSelect { .. }
            | ast::Expr::InTable { .. }
            | ast::Expr::Subquery(_) => self.subquery_error(),
            ast::Expr::Raise(action, message) => {
                if self.profile != SchemaExprProfile::TypeTransform {
                    return parse_error(format!(
                        "RAISE() is prohibited in stored {}",
                        self.profile.description()
                    ));
                }
                Ok(SchemaExprNode::Raise {
                    action: *action,
                    message: message
                        .as_deref()
                        .map(|message| self.node(message).map(Box::new))
                        .transpose()?,
                })
            }
            ast::Expr::Default => parse_error(format!(
                "DEFAULT is prohibited in stored {}",
                self.profile.description()
            )),
            ast::Expr::Register(_)
            | ast::Expr::Column { .. }
            | ast::Expr::RowId { .. }
            | ast::Expr::SubqueryResult { .. } => Err(LimboError::InternalError(
                "stored expression resolution received an already-bound expression".to_string(),
            )),
        }
    }

    fn type_name(&self, type_name: &ast::Type) -> Result<SchemaTypeName> {
        let size = match &type_name.size {
            None => None,
            Some(ast::TypeSize::MaxSize(size)) => {
                Some(SchemaTypeSize::MaxSize(Box::new(self.node(size)?)))
            }
            Some(ast::TypeSize::TypeSize(precision, scale)) => Some(SchemaTypeSize::TypeSize(
                Box::new(self.node(precision)?),
                Box::new(self.node(scale)?),
            )),
        };
        Ok(SchemaTypeName {
            name: type_name.name.clone(),
            size,
            array_dimensions: type_name.array_dimensions,
        })
    }

    fn unqualified(&self, name: &ast::Name) -> Result<SchemaExprNode> {
        if self.profile == SchemaExprProfile::Default {
            return self.column_not_allowed(name.as_str());
        }
        if self.profile == SchemaExprProfile::TypeTransform {
            let Some(parameters) = self.type_parameters else {
                return Err(LimboError::InternalError(
                    "resolving a type transform requires its parameters".to_string(),
                ));
            };
            let Some((position, parameter)) = parameters
                .iter()
                .enumerate()
                .find(|(_, parameter)| parameter.name().eq_ignore_ascii_case(name.as_str()))
            else {
                return parse_error(format!("no such type parameter: {}", name.as_str()));
            };
            return Ok(SchemaExprNode::TypeParameter {
                position,
                name: parameter.name().to_string(),
            });
        }
        if self.profile == SchemaExprProfile::DomainCheck {
            if name.as_str().eq_ignore_ascii_case("value") {
                return Ok(SchemaExprNode::DomainValue);
            }
            return parse_error(format!("no such column: {}", name.as_str()));
        }
        if !self.profile.allows_table_columns() {
            return self.column_not_allowed(name.as_str());
        }

        let table = self.table.expect("column profile requires a table");
        if let Some((position, column)) = table.find_column(name.as_str()) {
            return Ok(SchemaExprNode::SelfColumn(SelfColumn::new(
                position,
                column.is_rowid_alias(),
            )));
        }
        if is_rowid_name(name.as_str()) {
            return self.rowid(name.as_str());
        }
        parse_error(format!("no such column: {}", name.as_str()))
    }

    fn qualified(&self, qualifier: &ast::Name, column_name: &ast::Name) -> Result<SchemaExprNode> {
        match self.profile {
            SchemaExprProfile::GeneratedColumn => {
                return parse_error("the \".\" operator prohibited in generated columns");
            }
            SchemaExprProfile::Default => {
                return self.column_not_allowed(column_name.as_str());
            }
            SchemaExprProfile::DomainCheck => {
                return parse_error(format!(
                    "no such column: {}.{}",
                    qualifier.as_str(),
                    column_name.as_str()
                ));
            }
            SchemaExprProfile::TypeTransform => {
                return parse_error(format!(
                    "no such type parameter: {}.{}",
                    qualifier.as_str(),
                    column_name.as_str()
                ));
            }
            _ => {}
        }

        let table = self.table.expect("column profile requires a table");
        if table.is_own_name(qualifier.as_str()) {
            return self.own_column(column_name);
        }

        // Parser syntax cannot know whether `a.b` names a table column or a
        // field of a custom-typed column. Table qualification wins; only a
        // non-table qualifier falls back to field access.
        if let Some((position, column)) = table.find_column(qualifier.as_str()) {
            let base =
                SchemaExprNode::SelfColumn(SelfColumn::new(position, column.is_rowid_alias()));
            return self.field_access(base, column_name);
        }

        parse_error(format!(
            "no such column: {}.{}",
            qualifier.as_str(),
            column_name.as_str()
        ))
    }

    fn doubly_qualified(
        &self,
        first: &ast::Name,
        second: &ast::Name,
        third: &ast::Name,
    ) -> Result<SchemaExprNode> {
        match self.profile {
            SchemaExprProfile::GeneratedColumn => {
                return parse_error("the \".\" operator prohibited in generated columns");
            }
            SchemaExprProfile::Default => return self.column_not_allowed(third.as_str()),
            SchemaExprProfile::DomainCheck => {
                return parse_error(format!(
                    "no such column: {}.{}.{}",
                    first.as_str(),
                    second.as_str(),
                    third.as_str()
                ));
            }
            SchemaExprProfile::TypeTransform => {
                return parse_error(format!(
                    "no such type parameter: {}.{}.{}",
                    first.as_str(),
                    second.as_str(),
                    third.as_str()
                ));
            }
            _ => {}
        }

        let table = self.table.expect("column profile requires a table");

        // `database.table.column` is considered before field-access fallback.
        // CHECK expressions reject database qualification, matching their
        // existing schema rules. Index profiles accept an owning-table name in
        // the middle position and still store only the column position.
        if table.is_own_name(second.as_str()) {
            if matches!(self.profile, SchemaExprProfile::Check { .. }) {
                return parse_error(format!(
                    "no such column: {}.{}",
                    second.as_str(),
                    third.as_str()
                ));
            }
            return self.own_column(third);
        }

        // `table.column.field`.
        if table.is_own_name(first.as_str()) {
            let base = self.own_column(second)?;
            return self.field_access(base, third);
        }

        // `column.field.nested_field`.
        if let Some((position, column)) = table.find_column(first.as_str()) {
            let base =
                SchemaExprNode::SelfColumn(SelfColumn::new(position, column.is_rowid_alias()));
            let middle = self.field_access(base, second)?;
            return self.field_access(middle, third);
        }

        parse_error(format!(
            "no such column: {}.{}.{}",
            first.as_str(),
            second.as_str(),
            third.as_str()
        ))
    }

    fn own_column(&self, column_name: &ast::Name) -> Result<SchemaExprNode> {
        let table = self.table.expect("column profile requires a table");
        if let Some((position, column)) = table.find_column(column_name.as_str()) {
            return Ok(SchemaExprNode::SelfColumn(SelfColumn::new(
                position,
                column.is_rowid_alias(),
            )));
        }
        if is_rowid_name(column_name.as_str()) {
            return self.rowid(column_name.as_str());
        }
        parse_error(format!("no such column: {}", column_name.as_str()))
    }

    fn field_access(&self, base: SchemaExprNode, field: &ast::Name) -> Result<SchemaExprNode> {
        if self.profile == SchemaExprProfile::GeneratedColumn {
            return parse_error("the \".\" operator prohibited in generated columns");
        }

        let Some(type_name) = self.declared_type(&base)? else {
            return parse_error(format!(
                "cannot extract field '{}' from a value without a known struct or union type",
                field.as_str()
            ));
        };
        let Some(type_def) = self.resolver.resolve_custom_type(&type_name)? else {
            return parse_error(format!(
                "cannot extract field '{}' from a value without a known struct or union type",
                field.as_str()
            ));
        };

        let resolution = if let Some((field_index, _)) = type_def.find_struct_field(field.as_str())
        {
            SchemaFieldAccess::StructField { field_index }
        } else if let Some((tag_index, _)) = type_def.find_union_variant(field.as_str()) {
            SchemaFieldAccess::UnionVariant { tag_index }
        } else {
            return parse_error(format!(
                "unknown field '{}' in type '{}'",
                field.as_str(),
                type_def.name
            ));
        };

        Ok(SchemaExprNode::FieldAccess {
            base: Box::new(base),
            field: field.clone(),
            resolution,
        })
    }

    fn rowid(&self, spelling: &str) -> Result<SchemaExprNode> {
        let table = self.table.expect("rowid resolution requires a table");
        if self.profile.allows_rowid() && table.has_rowid() {
            Ok(SchemaExprNode::SelfRowId)
        } else {
            parse_error(format!("no such column: {spelling}"))
        }
    }

    fn resolve_function(
        &self,
        name: &str,
        argument_count: usize,
        args: &[Box<ast::Expr>],
        has_custom_type_identity: bool,
    ) -> Result<Func> {
        let Some(function) = self.resolver.resolve_function(name, argument_count)? else {
            return parse_error(format!("no such function: {name}"));
        };

        if matches!(&function, Func::Agg(_))
            || matches!(&function, Func::External(function) if function.func.is_aggregate())
        {
            return self.aggregate_error(name);
        }
        if matches!(&function, Func::Window(_)) {
            return self.window_error(name);
        }
        if !has_custom_type_identity
            && matches!(
                &function,
                Func::Scalar(
                    ScalarFunc::UnionValueFunc
                        | ScalarFunc::UnionTagFunc
                        | ScalarFunc::UnionExtractFunc
                        | ScalarFunc::StructExtractFunc
                )
            )
        {
            return parse_error(format!(
                "custom-type function {name}() has no resolved type identity"
            ));
        }
        if self.profile.requires_deterministic_function_calls()
            && !(self.profile == SchemaExprProfile::TypeTransform
                && matches!(&function, Func::External(_)))
            && !is_deterministic_schema_function_call(&function, args)
        {
            return self.nondeterministic_error();
        }
        Ok(function)
    }

    fn custom_type_function(
        &self,
        name: &ast::Name,
        distinctness: Option<ast::Distinctness>,
        syntax_args: &[Box<ast::Expr>],
    ) -> Result<SchemaExprNode> {
        let function_name = name.as_str();
        let normalized = crate::util::normalize_ident(function_name);

        let (args, resolution) = match normalized.as_str() {
            "union_value" => {
                self.require_argument_count(function_name, syntax_args, 2)?;
                let tag_name = self.string_argument(function_name, syntax_args, 0)?;
                let expected_type = self.expected_type.as_deref().ok_or_else(|| {
                    LimboError::ParseError(
                        "union_value() can only be used where a union-typed destination is known"
                            .to_string(),
                    )
                })?;
                let union_type = self
                    .resolver
                    .resolve_custom_type(expected_type)?
                    .filter(|type_def| type_def.is_union())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "union_value() can only be used where a union-typed destination is known"
                                .to_string(),
                        )
                    })?;
                let (tag_index, variant) =
                    union_type.find_union_variant(tag_name).ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "unknown variant '{}' in union type '{}'",
                            tag_name, union_type.name
                        ))
                    })?;
                let args = vec![
                    self.node_with_expected(&syntax_args[0], None)?,
                    self.node_with_expected(&syntax_args[1], Some(&variant.type_name))?,
                ];
                (
                    args,
                    SchemaCustomTypeFunction::UnionValue {
                        tag_index,
                        tag_name: tag_name.to_string(),
                        result_type: union_type.name.clone(),
                    },
                )
            }
            "union_tag" => {
                self.require_argument_count(function_name, syntax_args, 1)?;
                let args = vec![self.node_with_expected(&syntax_args[0], None)?];
                let union_type = self
                    .custom_type_of(&args[0])?
                    .filter(|ty| ty.is_union())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "union_tag() argument must have a known union type".to_string(),
                        )
                    })?;
                let tag_names = union_type
                    .union_def()
                    .expect("union type has a union definition")
                    .variants
                    .iter()
                    .map(|variant| variant.tag_name.clone())
                    .collect();
                (args, SchemaCustomTypeFunction::UnionTag { tag_names })
            }
            "union_extract" => {
                self.require_argument_count(function_name, syntax_args, 2)?;
                let tag_name = self.string_argument(function_name, syntax_args, 1)?;
                let args = syntax_args
                    .iter()
                    .map(|arg| self.node_with_expected(arg, None))
                    .collect::<Result<Vec<_>>>()?;
                let union_type = self
                    .custom_type_of(&args[0])?
                    .filter(|ty| ty.is_union())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "union_extract() first argument must have a known union type"
                                .to_string(),
                        )
                    })?;
                let (tag_index, variant) =
                    union_type.find_union_variant(tag_name).ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "unknown variant '{}' in union type '{}'",
                            tag_name, union_type.name
                        ))
                    })?;
                (
                    args,
                    SchemaCustomTypeFunction::UnionExtract {
                        tag_index,
                        tag_name: tag_name.to_string(),
                        result_type: variant.type_name.clone(),
                        result_array_dimensions: 0,
                    },
                )
            }
            "struct_extract" => {
                self.require_argument_count(function_name, syntax_args, 2)?;
                let field_name = self.string_argument(function_name, syntax_args, 1)?;
                let args = syntax_args
                    .iter()
                    .map(|arg| self.node_with_expected(arg, None))
                    .collect::<Result<Vec<_>>>()?;
                let struct_type = self
                    .custom_type_of(&args[0])?
                    .filter(|ty| ty.is_struct())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "struct_extract() first argument must have a known struct type"
                                .to_string(),
                        )
                    })?;
                let (field_index, field) =
                    struct_type.find_struct_field(field_name).ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "unknown field '{}' in struct type '{}'",
                            field_name, struct_type.name
                        ))
                    })?;
                (
                    args,
                    SchemaCustomTypeFunction::StructExtract {
                        field_index,
                        field_name: field_name.to_string(),
                        result_type: field.type_name.clone(),
                        result_array_dimensions: 0,
                    },
                )
            }
            _ => unreachable!("custom type function name was checked by the caller"),
        };
        let function =
            self.resolve_function(function_name, syntax_args.len(), syntax_args, true)?;

        let call = SchemaExprNode::Function {
            name: name.clone(),
            function,
            distinctness,
            args,
            star: false,
        };
        Ok(SchemaExprNode::CustomTypeFunction {
            call: Box::new(call),
            resolution,
        })
    }

    fn node_with_expected(
        &self,
        expr: &ast::Expr,
        expected_type: Option<&str>,
    ) -> Result<SchemaExprNode> {
        ResolverState {
            profile: self.profile,
            table: self.table,
            expected_type: expected_type.map(ToOwned::to_owned),
            type_parameters: self.type_parameters,
            default_column_name: self.default_column_name,
            resolver: self.resolver,
        }
        .node(expr)
    }

    fn require_argument_count(
        &self,
        function_name: &str,
        args: &[Box<ast::Expr>],
        expected: usize,
    ) -> Result<()> {
        if args.len() == expected {
            Ok(())
        } else {
            parse_error(format!(
                "{function_name}() expects exactly {expected} argument{}",
                if expected == 1 { "" } else { "s" }
            ))
        }
    }

    fn string_argument<'b>(
        &self,
        function_name: &str,
        args: &'b [Box<ast::Expr>],
        index: usize,
    ) -> Result<&'b str> {
        let Some(ast::Expr::Literal(ast::Literal::String(value))) =
            args.get(index).map(AsRef::as_ref)
        else {
            return parse_error(format!(
                "{function_name}() argument {} must be a string literal",
                index + 1
            ));
        };
        Ok(value.trim_matches('\''))
    }

    fn custom_type_of(
        &self,
        expr: &SchemaExprNode,
    ) -> Result<Option<crate::sync::Arc<crate::schema::TypeDef>>> {
        let Some(type_name) = self.declared_type(expr)? else {
            return Ok(None);
        };
        self.resolver.resolve_custom_type(&type_name)
    }

    fn declared_type(&self, expr: &SchemaExprNode) -> Result<Option<String>> {
        match expr {
            SchemaExprNode::SelfColumn(column) => Ok(self
                .table
                .and_then(|table| table.column(column.position()))
                .and_then(SchemaColumn::declared_type)
                .map(ToOwned::to_owned)),
            SchemaExprNode::TypeParameter { position, name } => {
                let Some(parameter) = self
                    .type_parameters
                    .and_then(|parameters| parameters.get(*position))
                    .filter(|parameter| parameter.name().eq_ignore_ascii_case(name))
                else {
                    return Err(LimboError::SchemaUpdated);
                };
                Ok(parameter.declared_type().map(ToOwned::to_owned))
            }
            SchemaExprNode::Cast { type_name, .. } => Ok(Some(type_name.name.clone())),
            SchemaExprNode::Collate { expr, .. } => self.declared_type(expr),
            SchemaExprNode::Parenthesized(exprs) if exprs.len() == 1 => {
                self.declared_type(&exprs[0])
            }
            SchemaExprNode::FieldAccess {
                base,
                field,
                resolution,
            } => {
                let Some(container) = self.custom_type_of(base)? else {
                    return Ok(None);
                };
                let result_type = match resolution {
                    SchemaFieldAccess::StructField { field_index } => container
                        .find_struct_field(field.as_str())
                        .filter(|(resolved, _)| resolved == field_index)
                        .map(|(_, field)| field.type_name.clone()),
                    SchemaFieldAccess::UnionVariant { tag_index } => container
                        .find_union_variant(field.as_str())
                        .filter(|(resolved, _)| resolved == tag_index)
                        .map(|(_, variant)| variant.type_name.clone()),
                };
                Ok(result_type)
            }
            SchemaExprNode::CustomTypeFunction { resolution, .. } => Ok(match resolution {
                SchemaCustomTypeFunction::UnionValue { result_type, .. }
                | SchemaCustomTypeFunction::UnionExtract { result_type, .. }
                | SchemaCustomTypeFunction::StructExtract { result_type, .. } => {
                    Some(result_type.clone())
                }
                SchemaCustomTypeFunction::UnionTag { .. } => None,
            }),
            _ => Ok(None),
        }
    }

    fn validate_function_tail(
        &self,
        name: &str,
        order_by_is_empty: bool,
        within_group_is_empty: bool,
        tail: &ast::FunctionTail,
    ) -> Result<()> {
        if tail.over_clause.is_some() {
            return self.window_error(name);
        }
        if !order_by_is_empty || !within_group_is_empty || tail.filter_clause.is_some() {
            return parse_error(format!(
                "aggregate function syntax is prohibited in stored {}",
                self.profile.description()
            ));
        }
        Ok(())
    }

    fn column_not_allowed<T>(&self, name: &str) -> Result<T> {
        match self.profile {
            SchemaExprProfile::Default => match self.default_column_name {
                Some(column) => parse_error(format!(
                    "default value of column [{column}] is not constant"
                )),
                None => parse_error(format!("default value is not constant: {name}")),
            },
            _ => parse_error(format!(
                "column references are prohibited in stored {}",
                self.profile.description()
            )),
        }
    }

    fn aggregate_error<T>(&self, name: &str) -> Result<T> {
        match self.profile {
            SchemaExprProfile::GeneratedColumn => {
                parse_error("aggregate functions prohibited in generated columns")
            }
            _ => parse_error(format!("misuse of aggregate function {name}()")),
        }
    }

    fn window_error<T>(&self, name: &str) -> Result<T> {
        match self.profile {
            SchemaExprProfile::GeneratedColumn => {
                parse_error("window functions prohibited in generated columns")
            }
            _ => parse_error(format!("misuse of window function {name}()")),
        }
    }

    fn nondeterministic_error<T>(&self) -> Result<T> {
        match self.profile {
            SchemaExprProfile::GeneratedColumn => {
                parse_error("non-deterministic functions prohibited in generated columns")
            }
            SchemaExprProfile::IndexKey => {
                parse_error("non-deterministic functions prohibited in index expressions")
            }
            SchemaExprProfile::PartialIndexPredicate => {
                parse_error("non-deterministic functions prohibited in partial index predicates")
            }
            _ => parse_error(format!(
                "non-deterministic functions prohibited in stored {}",
                self.profile.description()
            )),
        }
    }

    fn variable_error<T>(&self) -> Result<T> {
        match self.profile {
            SchemaExprProfile::GeneratedColumn => {
                parse_error("bind parameters prohibited in generated columns")
            }
            SchemaExprProfile::Check { .. } | SchemaExprProfile::DomainCheck => {
                parse_error("parameters prohibited in CHECK constraints")
            }
            _ => parse_error(format!(
                "bind parameters prohibited in stored {}",
                self.profile.description()
            )),
        }
    }

    fn subquery_error<T>(&self) -> Result<T> {
        match self.profile {
            SchemaExprProfile::GeneratedColumn => {
                parse_error("subqueries prohibited in generated columns")
            }
            SchemaExprProfile::Check { .. } | SchemaExprProfile::DomainCheck => {
                parse_error("subqueries prohibited in CHECK constraints")
            }
            _ => parse_error(format!(
                "subqueries prohibited in stored {}",
                self.profile.description()
            )),
        }
    }
}

fn is_rowid_name(name: &str) -> bool {
    let name = crate::util::normalize_ident(name);
    ROWID_NAMES
        .iter()
        .any(|rowid| rowid.eq_ignore_ascii_case(&name))
}

fn is_custom_type_function(name: &str) -> bool {
    matches!(
        crate::util::normalize_ident(name).as_str(),
        "union_value" | "union_tag" | "union_extract" | "struct_extract"
    )
}

fn is_deterministic_schema_function_call(function: &Func, args: &[Box<ast::Expr>]) -> bool {
    match function {
        Func::Scalar(
            ScalarFunc::Date
            | ScalarFunc::Time
            | ScalarFunc::DateTime
            | ScalarFunc::UnixEpoch
            | ScalarFunc::JulianDay
            | ScalarFunc::StrfTime
            | ScalarFunc::TimeDiff,
        ) => is_deterministic_datetime_call(function, args),
        _ => function.is_deterministic(),
    }
}

fn is_deterministic_datetime_call(function: &Func, args: &[Box<ast::Expr>]) -> bool {
    match function {
        Func::Scalar(ScalarFunc::Date)
        | Func::Scalar(ScalarFunc::Time)
        | Func::Scalar(ScalarFunc::DateTime)
        | Func::Scalar(ScalarFunc::UnixEpoch)
        | Func::Scalar(ScalarFunc::JulianDay) => {
            !args.is_empty()
                && !is_current_time_expr(&args[0])
                && !args[1..].iter().any(|arg| is_unsafe_datetime_modifier(arg))
        }
        Func::Scalar(ScalarFunc::StrfTime) => {
            args.len() >= 2
                && !is_current_time_expr(&args[1])
                && !args[2..].iter().any(|arg| is_unsafe_datetime_modifier(arg))
        }
        Func::Scalar(ScalarFunc::TimeDiff) => !args.iter().any(|arg| is_current_time_expr(arg)),
        _ => unreachable!("datetime validation received a different function"),
    }
}

fn is_current_time_expr(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Literal(Literal::String(value))
            if value.trim_matches('\'').eq_ignore_ascii_case("now")
    ) || matches!(
        expr,
        ast::Expr::Literal(Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp)
    )
}

fn is_unsafe_datetime_modifier(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Literal(Literal::String(value))
            if value.trim_matches('\'').eq_ignore_ascii_case("localtime")
                || value.trim_matches('\'').eq_ignore_ascii_case("utc")
    ) || is_current_time_expr(expr)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CheckType {
    Integer,
    Real,
    Text,
    Blob,
    Any,
    Null,
    Custom(String),
}

impl CheckType {
    fn from_schema(value_type: &SchemaValueType) -> Self {
        match value_type {
            SchemaValueType::Integer => Self::Integer,
            SchemaValueType::Real => Self::Real,
            SchemaValueType::Text => Self::Text,
            SchemaValueType::Blob => Self::Blob,
            SchemaValueType::Any => Self::Any,
            SchemaValueType::Custom(name) => Self::Custom(name.to_ascii_lowercase()),
        }
    }

    fn is_numeric(&self) -> bool {
        matches!(self, Self::Integer | Self::Real)
    }

    fn is_compatible_with(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Null, _) | (_, Self::Null) | (Self::Any, _) | (_, Self::Any)
        ) || self == other
            || (self.is_numeric() && other.is_numeric())
    }

    fn display_name(&self) -> &str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
            Self::Any => "ANY",
            Self::Null => "NULL",
            Self::Custom(name) => name,
        }
    }
}

struct StrictCheck<'a> {
    table: &'a SchemaTable,
    resolver: &'a dyn SchemaExprResolver,
}

impl<'a> StrictCheck<'a> {
    fn new(table: &'a SchemaTable, resolver: &'a dyn SchemaExprResolver) -> Self {
        Self { table, resolver }
    }

    fn validate(&self, expr: &SchemaExprNode) -> Result<()> {
        match expr {
            SchemaExprNode::Binary(lhs, operator, rhs) => {
                if matches!(
                    operator,
                    Operator::Equals
                        | Operator::NotEquals
                        | Operator::Less
                        | Operator::LessEquals
                        | Operator::Greater
                        | Operator::GreaterEquals
                ) {
                    self.require_compatible(
                        &self.expr_type(lhs)?,
                        &self.expr_type(rhs)?,
                        "CHECK constraint",
                    )?;
                }
                self.validate(lhs)?;
                self.validate(rhs)
            }
            SchemaExprNode::Between {
                lhs, start, end, ..
            } => {
                let lhs_type = self.expr_type(lhs)?;
                self.require_compatible(&lhs_type, &self.expr_type(start)?, "CHECK BETWEEN")?;
                self.require_compatible(&lhs_type, &self.expr_type(end)?, "CHECK BETWEEN")?;
                self.validate(lhs)?;
                self.validate(start)?;
                self.validate(end)
            }
            SchemaExprNode::InList { lhs, rhs, .. } => {
                let lhs_type = self.expr_type(lhs)?;
                for item in rhs {
                    self.require_compatible(&lhs_type, &self.expr_type(item)?, "CHECK IN list")?;
                }
                self.validate(lhs)?;
                for item in rhs {
                    self.validate(item)?;
                }
                Ok(())
            }
            SchemaExprNode::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                if let Some(base) = base {
                    self.validate(base)?;
                }
                for (when, then) in when_then_pairs {
                    self.validate(when)?;
                    self.validate(then)?;
                }
                if let Some(else_expr) = else_expr {
                    self.validate(else_expr)?;
                }
                Ok(())
            }
            SchemaExprNode::Cast {
                expr, type_name, ..
            } => {
                self.validate(expr)?;
                match &type_name.size {
                    Some(SchemaTypeSize::MaxSize(size)) => self.validate(size),
                    Some(SchemaTypeSize::TypeSize(precision, scale)) => {
                        self.validate(precision)?;
                        self.validate(scale)
                    }
                    None => Ok(()),
                }
            }
            SchemaExprNode::Collate { expr, .. }
            | SchemaExprNode::FieldAccess { base: expr, .. }
            | SchemaExprNode::CustomTypeFunction { call: expr, .. }
            | SchemaExprNode::IsNull(expr)
            | SchemaExprNode::NotNull(expr)
            | SchemaExprNode::Unary(_, expr) => self.validate(expr),
            SchemaExprNode::Function { args, .. }
            | SchemaExprNode::Parenthesized(args)
            | SchemaExprNode::Array(args) => {
                for arg in args {
                    self.validate(arg)?;
                }
                Ok(())
            }
            SchemaExprNode::Like {
                lhs, rhs, escape, ..
            } => {
                self.validate(lhs)?;
                self.validate(rhs)?;
                if let Some(escape) = escape {
                    self.validate(escape)?;
                }
                Ok(())
            }
            SchemaExprNode::Subscript { base, index } => {
                self.validate(base)?;
                self.validate(index)
            }
            SchemaExprNode::SelfColumn(_)
            | SchemaExprNode::SelfRowId
            | SchemaExprNode::DomainValue
            | SchemaExprNode::TypeParameter { .. }
            | SchemaExprNode::Literal(_) => Ok(()),
            SchemaExprNode::Raise { message, .. } => {
                if let Some(message) = message {
                    self.validate(message)?;
                }
                Ok(())
            }
        }
    }

    fn require_compatible(&self, lhs: &CheckType, rhs: &CheckType, context: &str) -> Result<()> {
        if lhs.is_compatible_with(rhs) {
            return Ok(());
        }
        parse_error(format!(
            "type mismatch in {context}: cannot compare {} with {}",
            lhs.display_name(),
            rhs.display_name()
        ))
    }

    fn expr_type(&self, expr: &SchemaExprNode) -> Result<CheckType> {
        match expr {
            SchemaExprNode::SelfColumn(column) => {
                let column = self.table.column(column.position()).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "strict CHECK refers to missing column position {}",
                        column.position()
                    ))
                })?;
                self.column_type(column)
            }
            SchemaExprNode::SelfRowId => Ok(CheckType::Integer),
            SchemaExprNode::DomainValue => {
                parse_error("cannot determine type of domain value in table CHECK constraint")
            }
            SchemaExprNode::TypeParameter { .. } | SchemaExprNode::Raise { .. } => {
                Ok(CheckType::Any)
            }
            SchemaExprNode::Literal(literal) => Ok(match literal {
                Literal::Numeric(value) => {
                    if value.contains('.') || value.contains('e') || value.contains('E') {
                        CheckType::Real
                    } else {
                        CheckType::Integer
                    }
                }
                Literal::String(_) => CheckType::Text,
                Literal::Blob(_) => CheckType::Blob,
                Literal::Null => CheckType::Null,
                Literal::True | Literal::False => CheckType::Integer,
                Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp => {
                    CheckType::Text
                }
                Literal::Keyword(value) => {
                    return parse_error(format!(
                        "cannot determine type of '{value}' in CHECK constraint; use CAST"
                    ));
                }
            }),
            SchemaExprNode::Parenthesized(exprs) if exprs.len() == 1 => self.expr_type(&exprs[0]),
            SchemaExprNode::Parenthesized(_) => {
                parse_error("cannot determine type of expression in CHECK constraint; use CAST")
            }
            SchemaExprNode::Cast {
                type_name,
                resolved_type,
                ..
            } => {
                let Some(resolved_type) = resolved_type else {
                    return parse_error(format!(
                        "unknown type '{}' in CHECK constraint",
                        type_name.name
                    ));
                };
                self.check_type_from_schema(resolved_type)
            }
            SchemaExprNode::Unary(operator, inner) => match operator {
                UnaryOperator::Negative | UnaryOperator::Positive => {
                    let inner_type = self.expr_type(inner)?;
                    if !inner_type.is_numeric() && inner_type != CheckType::Null {
                        return parse_error(format!(
                            "unary minus/plus requires a numeric type, got {}",
                            inner_type.display_name()
                        ));
                    }
                    Ok(inner_type)
                }
                UnaryOperator::BitwiseNot | UnaryOperator::Not => Ok(CheckType::Integer),
            },
            SchemaExprNode::Binary(lhs, operator, rhs) => match operator {
                Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                    let lhs = self.expr_type(lhs)?;
                    let rhs = self.expr_type(rhs)?;
                    if lhs == CheckType::Null || rhs == CheckType::Null {
                        return Ok(CheckType::Null);
                    }
                    if !lhs.is_numeric() || !rhs.is_numeric() {
                        return parse_error(format!(
                            "arithmetic requires numeric types, got {} and {}",
                            lhs.display_name(),
                            rhs.display_name()
                        ));
                    }
                    if lhs == CheckType::Real || rhs == CheckType::Real {
                        Ok(CheckType::Real)
                    } else {
                        Ok(CheckType::Integer)
                    }
                }
                Operator::Modulus
                | Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::LeftShift
                | Operator::RightShift
                | Operator::And
                | Operator::Or
                | Operator::Equals
                | Operator::NotEquals
                | Operator::Less
                | Operator::LessEquals
                | Operator::Greater
                | Operator::GreaterEquals
                | Operator::Is
                | Operator::IsNot => Ok(CheckType::Integer),
                Operator::Concat => Ok(CheckType::Text),
                _ => {
                    parse_error("cannot determine type of expression in CHECK constraint; use CAST")
                }
            },
            SchemaExprNode::Between { .. }
            | SchemaExprNode::InList { .. }
            | SchemaExprNode::IsNull(_)
            | SchemaExprNode::NotNull(_)
            | SchemaExprNode::Like { .. } => Ok(CheckType::Integer),
            SchemaExprNode::Collate { expr, .. } => self.expr_type(expr),
            SchemaExprNode::Function { function, args, .. } => self.function_type(function, args),
            SchemaExprNode::CustomTypeFunction { resolution, .. } => Ok(match resolution {
                SchemaCustomTypeFunction::UnionTag { .. } => CheckType::Text,
                SchemaCustomTypeFunction::UnionValue { result_type, .. }
                | SchemaCustomTypeFunction::UnionExtract { result_type, .. }
                | SchemaCustomTypeFunction::StructExtract { result_type, .. } => {
                    CheckType::Custom(result_type.to_ascii_lowercase())
                }
            }),
            SchemaExprNode::Case {
                when_then_pairs,
                else_expr,
                ..
            } => {
                let mut result = CheckType::Null;
                for (_, then) in when_then_pairs {
                    result = common_type(result, self.expr_type(then)?);
                }
                if let Some(else_expr) = else_expr {
                    result = common_type(result, self.expr_type(else_expr)?);
                }
                Ok(result)
            }
            SchemaExprNode::FieldAccess { .. }
            | SchemaExprNode::Array(_)
            | SchemaExprNode::Subscript { .. } => Ok(CheckType::Any),
        }
    }

    fn column_type(&self, column: &SchemaColumn) -> Result<CheckType> {
        let Some(type_name) = column.declared_type() else {
            return Ok(CheckType::Any);
        };
        let Some(value_type) = self.resolver.resolve_type(type_name)? else {
            return parse_error(format!("unknown type '{type_name}' in CHECK constraint"));
        };
        self.check_type_from_schema(&value_type)
    }

    fn check_type_from_schema(&self, value_type: &SchemaValueType) -> Result<CheckType> {
        let mut value_type = value_type.clone();
        let mut visited_domains = std::collections::HashSet::new();

        loop {
            let SchemaValueType::Custom(type_name) = &value_type else {
                return Ok(CheckType::from_schema(&value_type));
            };
            let Some(definition) = self.resolver.resolve_custom_type(type_name)? else {
                return Ok(CheckType::from_schema(&value_type));
            };
            if !definition.is_domain {
                return Ok(CheckType::from_schema(&value_type));
            }

            let domain_name = crate::util::normalize_ident(&definition.name);
            if !visited_domains.insert(domain_name.clone()) {
                return parse_error(format!("circular type dependency detected: {domain_name}"));
            }

            let base_type = definition.base();
            let Some(resolved_base) = self.resolver.resolve_type(base_type)? else {
                return parse_error(format!("unknown type '{base_type}' in CHECK constraint"));
            };
            value_type = resolved_base;
        }
    }

    fn function_type(&self, function: &Func, args: &[SchemaExprNode]) -> Result<CheckType> {
        match function {
            Func::Scalar(function) => self.scalar_function_type(function, args),
            Func::Math(function) => Ok(match function {
                MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc => {
                    CheckType::Integer
                }
                _ => CheckType::Real,
            }),
            #[cfg(feature = "json")]
            Func::Json(function) => Ok(json_function_type(function)),
            Func::Agg(_) | Func::Window(_) => {
                unreachable!("aggregate and window functions are rejected during resolution")
            }
            _ => Ok(CheckType::Any),
        }
    }

    fn scalar_function_type(
        &self,
        function: &ScalarFunc,
        args: &[SchemaExprNode],
    ) -> Result<CheckType> {
        Ok(match function {
            ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Instr
            | ScalarFunc::Unicode
            | ScalarFunc::Sign
            | ScalarFunc::Random
            | ScalarFunc::Changes
            | ScalarFunc::TotalChanges
            | ScalarFunc::LastInsertRowid
            | ScalarFunc::Glob
            | ScalarFunc::Like
            | ScalarFunc::Likely
            | ScalarFunc::Unlikely
            | ScalarFunc::Likelihood
            | ScalarFunc::BooleanToInt
            | ScalarFunc::IntToBoolean
            | ScalarFunc::IsAutocommit
            | ScalarFunc::ConnTxnId
            | ScalarFunc::TestUintLt
            | ScalarFunc::TestUintEq
            | ScalarFunc::NumericLt
            | ScalarFunc::NumericEq
            | ScalarFunc::ValidateIpAddr
            | ScalarFunc::GetByte
            | ScalarFunc::UnixEpoch => CheckType::Integer,
            ScalarFunc::Upper
            | ScalarFunc::Lower
            | ScalarFunc::Trim
            | ScalarFunc::LTrim
            | ScalarFunc::RTrim
            | ScalarFunc::Hex
            | ScalarFunc::Soundex
            | ScalarFunc::Quote
            | ScalarFunc::Replace
            | ScalarFunc::Substr
            | ScalarFunc::Substring
            | ScalarFunc::Char
            | ScalarFunc::Concat
            | ScalarFunc::ConcatWs
            | ScalarFunc::Typeof
            | ScalarFunc::SqliteVersion
            | ScalarFunc::TursoVersion
            | ScalarFunc::SqliteSourceId
            | ScalarFunc::Date
            | ScalarFunc::Time
            | ScalarFunc::DateTime
            | ScalarFunc::StrfTime
            | ScalarFunc::TimeDiff
            | ScalarFunc::Printf
            | ScalarFunc::StringReverse => CheckType::Text,
            ScalarFunc::Round | ScalarFunc::JulianDay => CheckType::Real,
            ScalarFunc::RandomBlob
            | ScalarFunc::ZeroBlob
            | ScalarFunc::Unhex
            | ScalarFunc::SetByte
            | ScalarFunc::TestUintEncode
            | ScalarFunc::TestUintDecode
            | ScalarFunc::TestUintAdd
            | ScalarFunc::TestUintSub
            | ScalarFunc::TestUintMul
            | ScalarFunc::TestUintDiv
            | ScalarFunc::NumericEncode
            | ScalarFunc::NumericDecode
            | ScalarFunc::NumericAdd
            | ScalarFunc::NumericSub
            | ScalarFunc::NumericMul
            | ScalarFunc::NumericDiv => CheckType::Blob,
            ScalarFunc::Abs | ScalarFunc::Nullif | ScalarFunc::Min | ScalarFunc::Max => args
                .first()
                .map_or(Ok(CheckType::Any), |arg| self.expr_type(arg))?,
            ScalarFunc::Coalesce | ScalarFunc::IfNull => {
                let mut result = CheckType::Null;
                for arg in args {
                    let arg_type = self.expr_type(arg)?;
                    if arg_type != CheckType::Null {
                        result = arg_type;
                        break;
                    }
                }
                result
            }
            ScalarFunc::Iif if args.len() >= 2 => self.expr_type(&args[1])?,
            _ => CheckType::Any,
        })
    }
}

fn common_type(lhs: CheckType, rhs: CheckType) -> CheckType {
    if lhs == CheckType::Null {
        rhs
    } else if rhs == CheckType::Null || lhs == rhs {
        lhs
    } else if lhs.is_numeric() && rhs.is_numeric() {
        CheckType::Real
    } else {
        CheckType::Any
    }
}

#[cfg(feature = "json")]
fn json_function_type(function: &crate::function::JsonFunc) -> CheckType {
    use crate::function::JsonFunc;
    match function {
        JsonFunc::Json
        | JsonFunc::JsonArray
        | JsonFunc::JsonObject
        | JsonFunc::JsonPatch
        | JsonFunc::JsonRemove
        | JsonFunc::JsonReplace
        | JsonFunc::JsonInsert
        | JsonFunc::JsonSet
        | JsonFunc::JsonPretty
        | JsonFunc::JsonQuote
        | JsonFunc::JsonType => CheckType::Text,
        JsonFunc::Jsonb
        | JsonFunc::JsonbArray
        | JsonFunc::JsonbObject
        | JsonFunc::JsonbPatch
        | JsonFunc::JsonbRemove
        | JsonFunc::JsonbReplace
        | JsonFunc::JsonbInsert
        | JsonFunc::JsonbSet => CheckType::Blob,
        JsonFunc::JsonArrayLength | JsonFunc::JsonErrorPosition | JsonFunc::JsonValid => {
            CheckType::Integer
        }
        JsonFunc::JsonExtract
        | JsonFunc::JsonbExtract
        | JsonFunc::JsonArrowExtract
        | JsonFunc::JsonArrowShiftExtract => CheckType::Any,
    }
}

fn parse_error<T>(message: impl Into<String>) -> Result<T> {
    Err(LimboError::ParseError(message.into()))
}
