use crate::{LimboError, Result, WalkControl};
use turso_parser::ast;

use super::{
    DropRemap, SchemaDependencies, SchemaExpr, SchemaExprNode, SchemaExprProfile, SchemaTypeSize,
    SelfColumn, ValidSchemaExpr,
};

impl ValidSchemaExpr {
    /// Return dependencies derived from the positional expression tree.
    pub(crate) fn dependencies(&self) -> SchemaDependencies {
        let mut dependencies = SchemaDependencies::default();
        visit(&self.root, &mut |expr| match expr {
            SchemaExprNode::SelfColumn(column) => {
                dependencies.columns.push(column.position());
            }
            SchemaExprNode::SelfRowId => dependencies.uses_rowid = true,
            SchemaExprNode::DomainValue => dependencies.uses_domain_value = true,
            _ => {}
        });
        dependencies.columns.sort_unstable();
        dependencies.columns.dedup();
        dependencies
    }

    /// Replace a domain's semantic `value` input with one table column.
    pub(crate) fn specialize_domain_value(&self, column: SelfColumn) -> Result<Self> {
        if self.profile != SchemaExprProfile::DomainCheck {
            return Err(LimboError::InternalError(
                "only a domain CHECK expression has a domain value".to_string(),
            ));
        }
        let mut specialized = self.clone();
        visit_mut(&mut specialized.root, &mut |expr| {
            if matches!(expr, SchemaExprNode::DomainValue) {
                *expr = SchemaExprNode::SelfColumn(column);
            }
            Ok(())
        })?;
        Ok(specialized)
    }

    /// Bind a type transform's positional inputs to columns of a synthetic
    /// semantic source. Column zero is `value`; remaining columns follow the
    /// declared user-parameter order recorded during resolution.
    pub(crate) fn specialize_type_parameters(&self) -> Result<Self> {
        if self.profile != SchemaExprProfile::TypeTransform {
            return Err(LimboError::InternalError(
                "only a type transform expression has type parameters".to_string(),
            ));
        }
        let mut specialized = self.clone();
        visit_mut(&mut specialized.root, &mut |expr| {
            let SchemaExprNode::TypeParameter { position, .. } = expr else {
                return Ok(());
            };
            let position = *position;
            *expr = SchemaExprNode::SelfColumn(SelfColumn::new(position, false));
            Ok(())
        })?;
        Ok(specialized)
    }

    /// Replace references to one owning-table column with another stored
    /// expression. ADD COLUMN uses this to evaluate a new column's CHECK
    /// constraints against the value that existing rows receive.
    pub(crate) fn substitute_self_column(
        &self,
        column: usize,
        replacement: &ValidSchemaExpr,
    ) -> Result<Self> {
        if replacement.profile != SchemaExprProfile::Default {
            return Err(LimboError::InternalError(
                "a stored column can only be substituted with a default expression".to_string(),
            ));
        }
        if !replacement.dependencies().columns().is_empty() {
            return Err(LimboError::InternalError(
                "a default expression unexpectedly refers to a table column".to_string(),
            ));
        }

        let mut substituted = self.clone();
        visit_mut(&mut substituted.root, &mut |expr| {
            let SchemaExprNode::SelfColumn(reference) = expr else {
                return Ok(());
            };
            if reference.position() == column {
                *expr = replacement.root.clone();
            }
            Ok(())
        })?;
        Ok(substituted)
    }

    /// Whether any NULL literal appears in the expression tree.
    pub(crate) fn contains_null_literal(&self) -> bool {
        let mut contains_null = false;
        visit(&self.root, &mut |expr| {
            if matches!(expr, SchemaExprNode::Literal(ast::Literal::Null)) {
                contains_null = true;
            }
        });
        contains_null
    }

    fn remap_after_drop(&mut self, dropped_column: usize) -> Result<()> {
        let description = self.profile.description();
        visit_mut(&mut self.root, &mut |expr| {
            let SchemaExprNode::SelfColumn(column) = expr else {
                return Ok(());
            };
            if column.position() == dropped_column {
                return Err(LimboError::InternalError(format!(
                    "stored {description} still refers to dropped column position {dropped_column}"
                )));
            }
            if column.position() > dropped_column {
                *column = SelfColumn::new(column.position() - 1, column.is_rowid_alias());
            }
            Ok(())
        })
    }
}

impl SchemaExpr {
    /// Return dependencies for a compilable expression.
    pub(crate) fn dependencies(&self) -> Result<SchemaDependencies> {
        Ok(self.as_valid()?.dependencies())
    }

    pub(crate) fn contains_null_literal(&self) -> Result<bool> {
        Ok(self.as_valid()?.contains_null_literal())
    }

    /// Column identity is positional in valid expressions. Only unresolved
    /// syntax needs its spelling updated after a rename. Qualified syntax uses
    /// the same table-first, field-access-second precedence as resolution.
    pub(crate) fn rename_column(&mut self, table_name: &str, from: &str, to: &str) -> Result<()> {
        let Self::Unresolved(expr) = self else {
            return Ok(());
        };
        if !expr.profile.allows_table_columns() {
            return Ok(());
        }

        // Work on a copy so an ambiguity never leaves a partly renamed tree.
        let profile = expr.profile;
        let mut renamed = expr.syntax.clone();
        crate::walk_expr_mut(&mut renamed, &mut |syntax| {
            match syntax {
                ast::Expr::Id(name) | ast::Expr::Name(name)
                    if name.as_str().eq_ignore_ascii_case(from) =>
                {
                    *name = ast::Name::exact(to.to_owned());
                }
                ast::Expr::Qualified(qualifier, column) => {
                    if qualifier.as_str().eq_ignore_ascii_case(table_name) {
                        if column.as_str().eq_ignore_ascii_case(from) {
                            *column = ast::Name::exact(to.to_owned());
                        }
                    } else if qualifier.as_str().eq_ignore_ascii_case(from) {
                        *qualifier = ast::Name::exact(to.to_owned());
                    } else if column.as_str().eq_ignore_ascii_case(from) {
                        return Err(ambiguous_rename(
                            profile,
                            from,
                            format!("{}.{}", qualifier.as_ident(), column.as_ident()),
                        ));
                    }
                }
                ast::Expr::DoublyQualified(first, second, third) => {
                    if second.as_str().eq_ignore_ascii_case(table_name) {
                        if third.as_str().eq_ignore_ascii_case(from) {
                            *third = ast::Name::exact(to.to_owned());
                        }
                    } else if first.as_str().eq_ignore_ascii_case(table_name) {
                        if second.as_str().eq_ignore_ascii_case(from) {
                            *second = ast::Name::exact(to.to_owned());
                        }
                    } else if first.as_str().eq_ignore_ascii_case(from) {
                        *first = ast::Name::exact(to.to_owned());
                    } else if second.as_str().eq_ignore_ascii_case(from)
                        || third.as_str().eq_ignore_ascii_case(from)
                    {
                        return Err(ambiguous_rename(
                            profile,
                            from,
                            format!(
                                "{}.{}.{}",
                                first.as_ident(),
                                second.as_ident(),
                                third.as_ident()
                            ),
                        ));
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        expr.syntax = renamed;
        Ok(())
    }

    /// Update table qualifiers retained by unresolved syntax after a table
    /// rename. Valid expressions use positional source identity and therefore
    /// contain no table spelling to rewrite.
    pub(crate) fn rename_table_references(&mut self, from: &str, to: &str) -> Result<()> {
        let Self::Unresolved(expr) = self else {
            return Ok(());
        };
        if !expr.profile.allows_table_columns() {
            return Ok(());
        }

        let mut renamed = expr.syntax.clone();
        crate::util::rewrite_check_expr_table_refs(&mut renamed, from, to);
        expr.syntax = renamed;
        Ok(())
    }

    /// Shift positional references after DROP COLUMN. Unresolved syntax has no
    /// trustworthy positions, so it is preserved byte-for-byte and reported.
    pub(crate) fn remap_after_drop(&mut self, dropped_column: usize) -> Result<DropRemap> {
        match self {
            Self::Valid(expr) => {
                expr.remap_after_drop(dropped_column)?;
                Ok(DropRemap::Remapped)
            }
            Self::Unresolved(_) => Ok(DropRemap::UnresolvedSyntaxPreserved),
        }
    }
}

fn ambiguous_rename(profile: SchemaExprProfile, column: &str, syntax: String) -> LimboError {
    LimboError::ParseError(format!(
        "cannot safely rename column '{column}' in unresolved stored {}: {syntax}",
        profile.description()
    ))
}

fn visit(expr: &SchemaExprNode, visitor: &mut impl FnMut(&SchemaExprNode)) {
    visitor(expr);
    match expr {
        SchemaExprNode::Between {
            lhs, start, end, ..
        } => {
            visit(lhs, visitor);
            visit(start, visitor);
            visit(end, visitor);
        }
        SchemaExprNode::Binary(lhs, _, rhs) => {
            visit(lhs, visitor);
            visit(rhs, visitor);
        }
        SchemaExprNode::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                visit(base, visitor);
            }
            for (when, then) in when_then_pairs {
                visit(when, visitor);
                visit(then, visitor);
            }
            if let Some(else_expr) = else_expr {
                visit(else_expr, visitor);
            }
        }
        SchemaExprNode::Cast {
            expr, type_name, ..
        } => {
            visit(expr, visitor);
            match &type_name.size {
                Some(SchemaTypeSize::MaxSize(size)) => visit(size, visitor),
                Some(SchemaTypeSize::TypeSize(precision, scale)) => {
                    visit(precision, visitor);
                    visit(scale, visitor);
                }
                None => {}
            }
        }
        SchemaExprNode::Collate { expr, .. }
        | SchemaExprNode::FieldAccess { base: expr, .. }
        | SchemaExprNode::CustomTypeFunction { call: expr, .. }
        | SchemaExprNode::IsNull(expr)
        | SchemaExprNode::NotNull(expr)
        | SchemaExprNode::Unary(_, expr) => visit(expr, visitor),
        SchemaExprNode::Function { args, .. }
        | SchemaExprNode::Parenthesized(args)
        | SchemaExprNode::Array(args) => {
            for arg in args {
                visit(arg, visitor);
            }
        }
        SchemaExprNode::InList { lhs, rhs, .. } => {
            visit(lhs, visitor);
            for item in rhs {
                visit(item, visitor);
            }
        }
        SchemaExprNode::Like {
            lhs, rhs, escape, ..
        } => {
            visit(lhs, visitor);
            visit(rhs, visitor);
            if let Some(escape) = escape {
                visit(escape, visitor);
            }
        }
        SchemaExprNode::Subscript { base, index } => {
            visit(base, visitor);
            visit(index, visitor);
        }
        SchemaExprNode::Raise { message, .. } => {
            if let Some(message) = message {
                visit(message, visitor);
            }
        }
        SchemaExprNode::SelfColumn(_)
        | SchemaExprNode::SelfRowId
        | SchemaExprNode::DomainValue
        | SchemaExprNode::TypeParameter { .. }
        | SchemaExprNode::Literal(_) => {}
    }
}

fn visit_mut(
    expr: &mut SchemaExprNode,
    visitor: &mut impl FnMut(&mut SchemaExprNode) -> Result<()>,
) -> Result<()> {
    visitor(expr)?;
    match expr {
        SchemaExprNode::Between {
            lhs, start, end, ..
        } => {
            visit_mut(lhs, visitor)?;
            visit_mut(start, visitor)?;
            visit_mut(end, visitor)
        }
        SchemaExprNode::Binary(lhs, _, rhs) => {
            visit_mut(lhs, visitor)?;
            visit_mut(rhs, visitor)
        }
        SchemaExprNode::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                visit_mut(base, visitor)?;
            }
            for (when, then) in when_then_pairs {
                visit_mut(when, visitor)?;
                visit_mut(then, visitor)?;
            }
            if let Some(else_expr) = else_expr {
                visit_mut(else_expr, visitor)?;
            }
            Ok(())
        }
        SchemaExprNode::Cast {
            expr, type_name, ..
        } => {
            visit_mut(expr, visitor)?;
            match &mut type_name.size {
                Some(SchemaTypeSize::MaxSize(size)) => visit_mut(size, visitor),
                Some(SchemaTypeSize::TypeSize(precision, scale)) => {
                    visit_mut(precision, visitor)?;
                    visit_mut(scale, visitor)
                }
                None => Ok(()),
            }
        }
        SchemaExprNode::Collate { expr, .. }
        | SchemaExprNode::FieldAccess { base: expr, .. }
        | SchemaExprNode::CustomTypeFunction { call: expr, .. }
        | SchemaExprNode::IsNull(expr)
        | SchemaExprNode::NotNull(expr)
        | SchemaExprNode::Unary(_, expr) => visit_mut(expr, visitor),
        SchemaExprNode::Function { args, .. }
        | SchemaExprNode::Parenthesized(args)
        | SchemaExprNode::Array(args) => {
            for arg in args {
                visit_mut(arg, visitor)?;
            }
            Ok(())
        }
        SchemaExprNode::InList { lhs, rhs, .. } => {
            visit_mut(lhs, visitor)?;
            for item in rhs {
                visit_mut(item, visitor)?;
            }
            Ok(())
        }
        SchemaExprNode::Like {
            lhs, rhs, escape, ..
        } => {
            visit_mut(lhs, visitor)?;
            visit_mut(rhs, visitor)?;
            if let Some(escape) = escape {
                visit_mut(escape, visitor)?;
            }
            Ok(())
        }
        SchemaExprNode::Subscript { base, index } => {
            visit_mut(base, visitor)?;
            visit_mut(index, visitor)
        }
        SchemaExprNode::Raise { message, .. } => {
            if let Some(message) = message {
                visit_mut(message, visitor)?;
            }
            Ok(())
        }
        SchemaExprNode::SelfColumn(_)
        | SchemaExprNode::SelfRowId
        | SchemaExprNode::DomainValue
        | SchemaExprNode::TypeParameter { .. }
        | SchemaExprNode::Literal(_) => Ok(()),
    }
}
