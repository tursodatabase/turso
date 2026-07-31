use crate::alloc::TursoVecExt;
use crate::function::{Deterministic, Func, MathFunc, ScalarFunc};
use crate::sync::Arc;
use crate::vdbe::builder::ProgramBuilder;

use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::num::NonZero;
use turso_parser::ast::{self, JoinConstraint, SortOrder, SortedColumn, TableInternalId};

use super::emitter::Resolver;
use super::expr::{unwrap_parens, walk_expr, walk_expr_mut, WalkControl};
use super::plan::{BitSet, ColumnMask, JoinInfo, TableReferences};
use super::planner::parse_row_id;
use crate::schema::{
    is_deterministic_schema_function_call, BTreeTable, Column, GeneratedType, Index, IndexColumn,
    Schema, Table, TypeDef, EXPR_INDEX_SENTINEL,
};
use crate::util::normalize_ident;
use crate::Result;

/// Take ownership of an expression, leaving a NULL literal in its place.
/// The caller is expected to overwrite the original slot immediately after.
fn take_expr(expr: &mut ast::Expr) -> ast::Expr {
    std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
}

fn rewrite_between_node(expr: &mut ast::Expr) -> bool {
    let ast::Expr::Between {
        lhs,
        not,
        start,
        end,
    } = expr
    else {
        return false;
    };
    let lhs = take_expr(lhs);
    let start = take_expr(start);
    let end = take_expr(end);
    let (lower, upper, combine) = if *not {
        (
            ast::Expr::Binary(Box::new(lhs.clone()), ast::Operator::Less, Box::new(start)),
            ast::Expr::Binary(Box::new(lhs), ast::Operator::Greater, Box::new(end)),
            ast::Operator::Or,
        )
    } else {
        (
            ast::Expr::Binary(
                Box::new(lhs.clone()),
                ast::Operator::GreaterEquals,
                Box::new(start),
            ),
            ast::Expr::Binary(Box::new(lhs), ast::Operator::LessEquals, Box::new(end)),
            ast::Operator::And,
        )
    };
    *expr = ast::Expr::Binary(Box::new(lower), combine, Box::new(upper));
    true
}

fn rewrite_between_expressions(expr: &mut ast::Expr) {
    let _ = walk_expr_mut(expr, &mut |expr| {
        rewrite_between_node(expr);
        Ok(WalkControl::Continue)
    });
}

/// Bind the synthetic `value` name in a domain CHECK to its concrete column.
pub fn bind_domain_check(expr: &ast::Expr, column_name: &str) -> Box<ast::Expr> {
    let mut bound = expr.clone();
    let _ = walk_expr_mut(&mut bound, &mut |expr| {
        if let ast::Expr::Id(name) = expr {
            if name.as_str().eq_ignore_ascii_case("value") {
                *expr = ast::Expr::Id(ast::Name::exact(column_name.to_string()));
            }
        }
        Ok(WalkControl::Continue)
    });
    Box::new(bound)
}

/// Shift stored generated-column bindings after a table column is removed.
pub fn shift_generated_columns_after_drop(
    table: &mut BTreeTable,
    dropped_column: usize,
) -> Result<()> {
    if !table.has_virtual_columns {
        return Ok(());
    }

    let mut columns = table.columns_mut();
    for column in columns.iter_mut() {
        let Some(expr) = column.generated_expr_mut() else {
            continue;
        };
        shift_schema_expr_after_drop(expr, dropped_column, true)?;
    }
    Ok(())
}

/// Shift a stored schema expression's bound column positions after DROP COLUMN.
pub fn shift_schema_expr_after_drop(
    expr: &mut ast::Expr,
    dropped_column: usize,
    reject_dropped_reference: bool,
) -> Result<()> {
    walk_expr_mut(expr, &mut |expr| {
        if let ast::Expr::Column { table, column, .. } = expr {
            if table.is_self_table() {
                if reject_dropped_reference && *column == dropped_column {
                    return Err(crate::LimboError::InternalError(
                        "dropped column remained referenced by generated column".to_string(),
                    ));
                }
                if *column > dropped_column {
                    *column -= 1;
                }
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Shift an index's bound keys and predicate after DROP COLUMN.
pub fn shift_index_after_drop(index: &mut Index, dropped_column: usize) -> Result<()> {
    for index_column in index.columns.iter_mut() {
        if index_column.pos_in_table != EXPR_INDEX_SENTINEL
            && index_column.pos_in_table > dropped_column
        {
            index_column.pos_in_table -= 1;
        }
        if let Some(expr) = &mut index_column.expr {
            shift_schema_expr_after_drop(expr, dropped_column, false)?;
        }
    }
    if let Some(predicate) = &mut index.where_clause {
        shift_schema_expr_after_drop(predicate, dropped_column, false)?;
    }
    Ok(())
}

/// Render a bound schema expression using the table's current column names.
pub fn render_schema_expr(expr: &ast::Expr, columns: &[Column]) -> Result<String> {
    let mut unbound = expr.clone();
    walk_expr_mut(&mut unbound, &mut |expr| {
        match expr {
            ast::Expr::Column { table, column, .. } if table.is_self_table() => {
                if let Some(name) = columns.get(*column).and_then(|column| column.name.as_ref()) {
                    *expr = ast::Expr::Id(ast::Name::exact(name.clone()));
                }
            }
            ast::Expr::RowId { table, .. } if table.is_self_table() => {
                *expr = ast::Expr::Id(ast::Name::exact(super::planner::ROWID_STRS[0].to_string()));
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(unbound.to_string())
}

/// Rename raw column identifiers left in a leniently loaded schema expression.
pub fn rename_schema_expr_identifiers(expr: &mut ast::Expr, from: &str, to: &str) {
    let _ = walk_expr_mut(expr, &mut |expr| {
        match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name)
                if name.as_str().eq_ignore_ascii_case(from) =>
            {
                *expr = ast::Expr::Id(ast::Name::exact(to.to_owned()));
            }
            ast::Expr::Qualified(table, column) if column.as_str().eq_ignore_ascii_case(from) => {
                *expr = ast::Expr::Qualified(table.clone(), ast::Name::exact(to.to_owned()));
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

/// Substitute one table column in a CHECK expression with its ADD COLUMN default.
pub fn bind_check_column_default(
    expr: &ast::Expr,
    table_name: &str,
    column_name: &str,
    default_expr: &ast::Expr,
) -> ast::Expr {
    let normalized_table = normalize_ident(table_name);
    let normalized_column = normalize_ident(column_name);
    let mut bound = expr.clone();
    let _ = walk_expr_mut(&mut bound, &mut |expr| match expr {
        ast::Expr::Id(name) if normalize_ident(name.as_str()) == normalized_column => {
            *expr = default_expr.clone();
            Ok(WalkControl::SkipChildren)
        }
        ast::Expr::Qualified(table, column)
            if normalize_ident(table.as_str()) == normalized_table
                && normalize_ident(column.as_str()) == normalized_column =>
        {
            *expr = default_expr.clone();
            Ok(WalkControl::SkipChildren)
        }
        _ => Ok(WalkControl::Continue),
    });
    bound
}

/// Reject a column rename when a direct reference to the renamed table
/// participates in a JOIN USING the old column name.
///
/// A USING name identifies columns on both sides of the join. Rewriting the
/// unresolved name would guess which side owns it, while leaving it unchanged
/// would make the renamed table fail the join. Rejecting the rename keeps us
/// from persisting a schema whose column identity was guessed.
pub fn validate_column_rename_using_clause(
    from: &Option<ast::FromClause>,
    target_table: &str,
    old_column: &str,
) -> Result<()> {
    let Some(from) = from else {
        return Ok(());
    };

    let is_target = |table: &ast::SelectTable| {
        matches!(
            table,
            ast::SelectTable::Table(name, _, _)
                if name.name.as_str().eq_ignore_ascii_case(target_table)
        )
    };

    let mut target_is_on_left = is_target(&from.select);
    for join in &from.joins {
        let target_is_on_right = is_target(&join.table);
        let uses_old_column = matches!(
            &join.constraint,
            Some(ast::JoinConstraint::Using(columns))
                if columns
                    .iter()
                    .any(|column| column.as_str().eq_ignore_ascii_case(old_column))
        );
        if uses_old_column && (target_is_on_left || target_is_on_right) {
            crate::bail_parse_error!(
                "cannot join using column {} - column not present in both tables",
                old_column
            );
        }
        target_is_on_left |= target_is_on_right;
    }

    Ok(())
}

/// Point SELF_TABLE references in a stored schema expression at a concrete
/// table reference. This does not reject unresolved identifiers because ALTER
/// validation may deliberately bind them through an expression register cache.
pub fn rebase_schema_expr(expr: &mut ast::Expr, internal_id: ast::TableInternalId) {
    let _ = walk_expr_mut(expr, &mut |expr| {
        match expr {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. }
                if table.is_self_table() =>
            {
                *table = internal_id;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

/// Clone a stored schema expression and bind its SELF_TABLE references to a
/// concrete table reference.
pub fn bind_schema_expr(expr: &ast::Expr, internal_id: ast::TableInternalId) -> Result<ast::Expr> {
    let mut bound = expr.clone();
    if let Some(name) = first_unbound_identifier(&bound) {
        crate::bail_parse_error!("no such column: {}", name);
    }
    rebase_schema_expr(&mut bound, internal_id);
    Ok(bound)
}

/// Bind every generated-column name to a SELF_TABLE column position.
pub fn bind_generated_column_expr(expr: &mut ast::Expr, columns: &[Column]) -> Result<()> {
    walk_expr_mut(expr, &mut |expr| match expr {
        ast::Expr::Id(name)
        | ast::Expr::Qualified(_, name)
        | ast::Expr::DoublyQualified(_, _, name) => {
            let column_name = normalize_ident(name.as_str());
            let (column, definition) = columns
                .iter()
                .enumerate()
                .find(|(_, column)| {
                    column
                        .name
                        .as_ref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&column_name))
                })
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such column: {column_name}"))
                })?;
            *expr = ast::Expr::Column {
                database: None,
                table: ast::TableInternalId::SELF_TABLE,
                column,
                is_rowid_alias: definition.is_rowid_alias(),
            };
            Ok(WalkControl::Continue)
        }
        _ => Ok(WalkControl::Continue),
    })?;
    Ok(())
}

/// Resolve a generated expression's column dependencies to table positions.
pub fn bind_generated_column_dependencies(
    expr: &ast::Expr,
    columns: &[Column],
    dependencies: &mut BitSet,
) -> Result<()> {
    walk_expr(expr, &mut |expr| {
        match expr {
            ast::Expr::Column { table, column, .. } if table.is_self_table() => {
                dependencies.set(*column)?;
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                if let Some(column) = columns.iter().position(|column| {
                    column
                        .name
                        .as_ref()
                        .is_some_and(|column_name| column_name.eq_ignore_ascii_case(name.as_str()))
                }) {
                    dependencies.set(column)?;
                }
            }
            ast::Expr::Qualified(_, name) | ast::Expr::DoublyQualified(_, _, name) => {
                if let Some(column) = columns.iter().position(|column| {
                    column
                        .name
                        .as_ref()
                        .is_some_and(|column_name| column_name.eq_ignore_ascii_case(name.as_str()))
                }) {
                    dependencies.set(column)?;
                }
            }
            ast::Expr::Subquery(_)
            | ast::Expr::Exists(_)
            | ast::Expr::InTable { .. }
            | ast::Expr::SubqueryResult { .. } => {
                unreachable!("generated columns cannot contain subqueries")
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Resolve columns read by a stored index expression to table positions.
pub fn bind_index_expression_columns(
    table: &Table,
    columns: &mut ColumnMask,
    expr: &ast::Expr,
) -> Result<()> {
    walk_expr(expr, &mut |expr| {
        match expr {
            ast::Expr::Id(name) => {
                if let Some((column, _)) = table.get_column_by_name(&normalize_ident(name.as_str()))
                {
                    columns.set(column)?;
                } else if super::planner::ROWID_STRS
                    .iter()
                    .any(|rowid| rowid.eq_ignore_ascii_case(name.as_str()))
                {
                    if let Some(rowid_column) = table
                        .btree()
                        .and_then(|table| table.get_rowid_alias_column().map(|(column, _)| column))
                    {
                        columns.set(rowid_column)?;
                    }
                }
            }
            ast::Expr::Qualified(namespace, name)
            | ast::Expr::DoublyQualified(_, namespace, name) => {
                if normalize_ident(namespace.as_str())
                    .eq_ignore_ascii_case(&normalize_ident(table.get_name()))
                {
                    if let Some((column, _)) =
                        table.get_column_by_name(&normalize_ident(name.as_str()))
                    {
                        columns.set(column)?;
                    }
                }
            }
            ast::Expr::Column { column, .. } => columns.set(*column)?,
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Bind index keys, partial-index predicates, and CHECK expressions to
/// SELF_TABLE positions. Unknown names remain unbound so stale schema entries
/// can load and fail when the expression is used.
pub fn bind_index_schema_expr(expr: &mut ast::Expr, table: &BTreeTable) {
    let table_name = normalize_ident(table.name.as_str());
    let _ = walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        let resolved = match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                bind_self_table_leaf(&normalize_ident(name.as_str()), table)
            }
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column)
                if normalize_ident(namespace.as_str()).eq_ignore_ascii_case(&table_name) =>
            {
                bind_self_table_leaf(&normalize_ident(column.as_str()), table)
            }
            _ => None,
        };
        if let Some(resolved) = resolved {
            *expr = resolved;
        }
        Ok(WalkControl::Continue)
    });
}

/// Bind CREATE INDEX key expressions to columns of the indexed table.
pub fn bind_index_columns(
    table: &BTreeTable,
    columns: &[SortedColumn],
    resolver: Option<&Resolver>,
) -> Result<crate::alloc::Vec<IndexColumn>> {
    super::index::reject_explicit_nulls(columns)?;
    let mut bound =
        <crate::alloc::Vec<_> as crate::alloc::TursoTryWithCapacityExt>::try_with_capacity_ext(
            columns.len(),
        )?;
    for sorted_column in columns {
        let order = sorted_column.order.unwrap_or(SortOrder::Asc);
        let (explicit_collation, base_expr) =
            extract_index_collation(sorted_column.expr.as_ref(), resolver)?;
        let unwrapped_expr = unwrap_parens(base_expr)?;
        if let Some((position, column_name, column)) = bind_index_column(unwrapped_expr, table) {
            let collation = explicit_collation.or_else(|| column.collation_opt());
            let expr = match column.generated_type() {
                GeneratedType::Virtual { expr, .. } => Some(expr.clone()),
                GeneratedType::NotGenerated => None,
            };
            bound
                .push_within_capacity(IndexColumn {
                    name: column_name,
                    order,
                    pos_in_table: position,
                    collation,
                    default: column.default.clone(),
                    expr,
                })
                .expect("bound index columns vector was preallocated to columns.len()");
            continue;
        }
        if !is_valid_index_expression(unwrapped_expr, table) {
            crate::bail_parse_error!(
                "Error: invalid expression in CREATE INDEX: {}",
                sorted_column.expr
            );
        }
        let mut key_expr = sorted_column.expr.clone();
        bind_index_schema_expr(&mut key_expr, table);
        bound
            .push_within_capacity(IndexColumn {
                name: sorted_column.expr.to_string(),
                order,
                pos_in_table: EXPR_INDEX_SENTINEL,
                collation: explicit_collation,
                default: None,
                expr: Some(key_expr),
            })
            .expect("bound index columns vector was preallocated to columns.len()");
    }
    Ok(bound)
}

/// Validate that a partial-index predicate only refers to its indexed table.
pub fn validate_partial_index_predicate(index: &Index, table: &Table) -> bool {
    let Some(predicate) = &index.where_clause else {
        return true;
    };

    let has_column = |name: &str| {
        table.columns().iter().any(|column| {
            column
                .name
                .as_ref()
                .is_some_and(|column_name| column_name.eq_ignore_ascii_case(name))
        })
    };
    let is_table = |name: &str| normalize_ident(name) == index.table_name;
    let is_deterministic_function = |name: &str, arg_count: usize| {
        let name = normalize_ident(name);
        Func::resolve_function(&name, arg_count)
            .is_ok_and(|function| function.is_some_and(|function| function.is_deterministic()))
    };

    let mut valid = true;
    let _ = walk_expr(
        predicate.as_ref(),
        &mut |expr: &ast::Expr| -> Result<WalkControl> {
            if !valid {
                return Ok(WalkControl::SkipChildren);
            }
            match expr {
                ast::Expr::Literal(_) | ast::Expr::RowId { .. } => {}
                ast::Expr::Id(name) => {
                    if !super::planner::ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(name.as_str()))
                        && !has_column(name.as_str())
                    {
                        valid = false;
                    }
                }
                ast::Expr::Qualified(namespace, column)
                | ast::Expr::DoublyQualified(_, namespace, column) => {
                    if !is_table(namespace.as_str()) || !has_column(column.as_str()) {
                        valid = false;
                    }
                }
                ast::Expr::FunctionCall {
                    name, filter_over, ..
                }
                | ast::Expr::FunctionCallStar {
                    name, filter_over, ..
                } => {
                    if filter_over.over_clause.is_some() {
                        valid = false;
                    } else {
                        let arg_count = match expr {
                            ast::Expr::FunctionCall { args, .. } => args.len(),
                            ast::Expr::FunctionCallStar { .. } => 0,
                            _ => unreachable!(),
                        };
                        if !is_deterministic_function(name.as_str(), arg_count) {
                            valid = false;
                        }
                    }
                }
                ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::Subquery(_)
                | ast::Expr::Raise { .. }
                | ast::Expr::Variable(_) => valid = false,
                _ => {}
            }
            Ok(if valid {
                WalkControl::Continue
            } else {
                WalkControl::SkipChildren
            })
        },
    );
    valid
}

/// Bind and validate a CHECK constraint against its table columns.
pub(crate) fn bind_check_constraint(
    expr: &ast::Expr,
    table_name: &str,
    column_names: &[&str],
    resolver: &Resolver,
) -> Result<()> {
    let normalized_table = normalize_ident(table_name);
    walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                let normalized_name = normalize_ident(name.as_str());
                if !column_names
                    .iter()
                    .any(|column| normalize_ident(column) == normalized_name)
                    && !super::planner::ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(&normalized_name))
                {
                    crate::bail_parse_error!("no such column: {}", name.as_str());
                }
            }
            ast::Expr::Qualified(table, column) => {
                if normalize_ident(table.as_str()) != normalized_table {
                    crate::bail_parse_error!(
                        "no such column: {}.{}",
                        table.as_str(),
                        column.as_str()
                    );
                }
                let column_name = normalize_ident(column.as_str());
                if !column_names
                    .iter()
                    .any(|column| normalize_ident(column) == column_name)
                    && !super::planner::ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(&column_name))
                {
                    crate::bail_parse_error!("no such column: {}", column.as_str());
                }
            }
            ast::Expr::DoublyQualified(database, table, column) => {
                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    database.as_str(),
                    table.as_str(),
                    column.as_str()
                );
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                let Some(function) = resolver.resolve_function(name.as_str(), args.len())? else {
                    crate::bail_parse_error!("no such function: {}", name.as_str());
                };
                if matches!(function, Func::Agg(..)) {
                    crate::bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                }
                if matches!(function, Func::Window(..)) {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                if filter_over.over_clause.is_some() {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                let Some(function) = resolver.resolve_function(name.as_str(), 0)? else {
                    crate::bail_parse_error!("no such function: {}", name.as_str());
                };
                if matches!(function, Func::Agg(..)) {
                    crate::bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                }
                if matches!(function, Func::Window(..)) {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
            }
            ast::Expr::Variable(_) => {
                crate::bail_parse_error!("parameters prohibited in CHECK constraints");
            }
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                crate::bail_parse_error!("subqueries prohibited in CHECK constraints");
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
enum CheckExprType {
    Integer,
    Real,
    Text,
    Blob,
    Any,
    Null,
    CustomType(String),
}

impl CheckExprType {
    fn is_numeric(&self) -> bool {
        matches!(self, Self::Integer | Self::Real)
    }

    fn is_compatible_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => true,
            (Self::Any, _) | (_, Self::Any) => true,
            (left, right) if left == right => true,
            (left, right) if left.is_numeric() && right.is_numeric() => true,
            _ => false,
        }
    }

    fn display_name(&self) -> &str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
            Self::Any => "ANY",
            Self::Null => "NULL",
            Self::CustomType(name) => name.as_str(),
        }
    }
}

/// Bind raw CHECK references while validating STRICT-table comparison types.
pub(crate) fn bind_strict_check_constraint(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<()> {
    use ast::Operator;
    match expr {
        ast::Expr::Binary(lhs, op, rhs) => match op {
            Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals => {
                let left_type = resolve_check_expr_type(lhs, columns, resolver)?;
                let right_type = resolve_check_expr_type(rhs, columns, resolver)?;
                if !left_type.is_compatible_with(&right_type) {
                    crate::bail_parse_error!(
                        "type mismatch in CHECK constraint: cannot compare {} with {}",
                        left_type.display_name(),
                        right_type.display_name()
                    );
                }
            }
            _ => {
                bind_strict_check_constraint(lhs, columns, resolver)?;
                bind_strict_check_constraint(rhs, columns, resolver)?;
            }
        },
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            let lhs_type = resolve_check_expr_type(lhs, columns, resolver)?;
            let start_type = resolve_check_expr_type(start, columns, resolver)?;
            let end_type = resolve_check_expr_type(end, columns, resolver)?;
            if !lhs_type.is_compatible_with(&start_type) {
                crate::bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lhs_type.display_name(),
                    start_type.display_name()
                );
            }
            if !lhs_type.is_compatible_with(&end_type) {
                crate::bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lhs_type.display_name(),
                    end_type.display_name()
                );
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            let lhs_type = resolve_check_expr_type(lhs, columns, resolver)?;
            for item in rhs {
                let item_type = resolve_check_expr_type(item, columns, resolver)?;
                if !lhs_type.is_compatible_with(&item_type) {
                    crate::bail_parse_error!(
                        "type mismatch in CHECK IN list: cannot compare {} with {}",
                        lhs_type.display_name(),
                        item_type.display_name()
                    );
                }
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            for expr in exprs {
                bind_strict_check_constraint(expr, columns, resolver)?;
            }
        }
        ast::Expr::Unary(_, inner) => {
            bind_strict_check_constraint(inner, columns, resolver)?;
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                bind_strict_check_constraint(base, columns, resolver)?;
            }
            for (when_expr, then_expr) in when_then_pairs {
                bind_strict_check_constraint(when_expr, columns, resolver)?;
                bind_strict_check_constraint(then_expr, columns, resolver)?;
            }
            if let Some(else_expr) = else_expr {
                bind_strict_check_constraint(else_expr, columns, resolver)?;
            }
        }
        ast::Expr::FunctionCall { args, .. } => {
            for arg in args {
                bind_strict_check_constraint(arg, columns, resolver)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_check_expr_type(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    use ast::{Literal, Operator, UnaryOperator};
    match expr {
        ast::Expr::Id(name) | ast::Expr::Name(name) => {
            let name = normalize_ident(name.as_str());
            if super::planner::ROWID_STRS
                .iter()
                .any(|rowid| rowid.eq_ignore_ascii_case(&name))
            {
                return Ok(CheckExprType::Integer);
            }
            let Some(column) = columns
                .iter()
                .find(|column| normalize_ident(column.col_name.as_str()) == name)
            else {
                crate::bail_parse_error!("no such column: {}", name);
            };
            resolve_check_column_type(column, resolver)
        }
        ast::Expr::Qualified(_, column) => {
            let column_name = normalize_ident(column.as_str());
            if super::planner::ROWID_STRS
                .iter()
                .any(|rowid| rowid.eq_ignore_ascii_case(&column_name))
            {
                return Ok(CheckExprType::Integer);
            }
            let Some(column) = columns
                .iter()
                .find(|column| normalize_ident(column.col_name.as_str()) == column_name)
            else {
                crate::bail_parse_error!("no such column: {}", column_name);
            };
            resolve_check_column_type(column, resolver)
        }
        ast::Expr::Literal(literal) => match literal {
            Literal::Numeric(value) => {
                if value.contains('.') || value.contains('e') || value.contains('E') {
                    Ok(CheckExprType::Real)
                } else {
                    Ok(CheckExprType::Integer)
                }
            }
            Literal::String(_) => Ok(CheckExprType::Text),
            Literal::Blob(_) => Ok(CheckExprType::Blob),
            Literal::Null => Ok(CheckExprType::Null),
            Literal::True | Literal::False => Ok(CheckExprType::Integer),
            Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp => {
                Ok(CheckExprType::Text)
            }
            Literal::Keyword(value) => crate::bail_parse_error!(
                "cannot determine type of '{}' in CHECK constraint; use CAST",
                value
            ),
        },
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            resolve_check_expr_type(&exprs[0], columns, resolver)
        }
        ast::Expr::Parenthesized(_) => crate::bail_parse_error!(
            "cannot determine type of expression in CHECK constraint; use CAST"
        ),
        ast::Expr::Cast { type_name, .. } => {
            let Some(type_name) = type_name else {
                crate::bail_parse_error!(
                    "cannot determine type of CAST in CHECK constraint; use CAST with explicit type"
                );
            };
            resolve_check_type_name(&type_name.name, resolver)
        }
        ast::Expr::Unary(op, inner) => match op {
            UnaryOperator::Negative | UnaryOperator::Positive => {
                let inner_type = resolve_check_expr_type(inner, columns, resolver)?;
                if !inner_type.is_numeric() && inner_type != CheckExprType::Null {
                    crate::bail_parse_error!(
                        "unary minus/plus requires a numeric type, got {}",
                        inner_type.display_name()
                    );
                }
                Ok(inner_type)
            }
            UnaryOperator::BitwiseNot | UnaryOperator::Not => Ok(CheckExprType::Integer),
        },
        ast::Expr::Binary(lhs, op, rhs) => match op {
            Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                let left_type = resolve_check_expr_type(lhs, columns, resolver)?;
                let right_type = resolve_check_expr_type(rhs, columns, resolver)?;
                if left_type == CheckExprType::Null || right_type == CheckExprType::Null {
                    return Ok(CheckExprType::Null);
                }
                if !left_type.is_numeric() || !right_type.is_numeric() {
                    crate::bail_parse_error!(
                        "arithmetic requires numeric types, got {} and {}",
                        left_type.display_name(),
                        right_type.display_name()
                    );
                }
                if left_type == CheckExprType::Real || right_type == CheckExprType::Real {
                    Ok(CheckExprType::Real)
                } else {
                    Ok(CheckExprType::Integer)
                }
            }
            Operator::Modulus
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::LeftShift
            | Operator::RightShift => Ok(CheckExprType::Integer),
            Operator::Concat => Ok(CheckExprType::Text),
            Operator::And | Operator::Or => {
                bind_strict_check_constraint(lhs, columns, resolver)?;
                bind_strict_check_constraint(rhs, columns, resolver)?;
                Ok(CheckExprType::Integer)
            }
            Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals => {
                let left_type = resolve_check_expr_type(lhs, columns, resolver)?;
                let right_type = resolve_check_expr_type(rhs, columns, resolver)?;
                if !left_type.is_compatible_with(&right_type) {
                    crate::bail_parse_error!(
                        "type mismatch in CHECK constraint: cannot compare {} with {}",
                        left_type.display_name(),
                        right_type.display_name()
                    );
                }
                Ok(CheckExprType::Integer)
            }
            Operator::Is | Operator::IsNot => Ok(CheckExprType::Integer),
            _ => crate::bail_parse_error!(
                "cannot determine type of expression in CHECK constraint; use CAST"
            ),
        },
        ast::Expr::NotNull(_) | ast::Expr::IsNull(_) => Ok(CheckExprType::Integer),
        ast::Expr::FunctionCall { name, args, .. } => {
            let Some(function) = resolver.resolve_function(name.as_str(), args.len())? else {
                crate::bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            };
            resolve_check_function_type(&function, name.as_str(), args, columns, resolver)
        }
        ast::Expr::FunctionCallStar { name, .. } => {
            let Some(function) = resolver.resolve_function(name.as_str(), 0)? else {
                crate::bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            };
            resolve_check_function_type(&function, name.as_str(), &[], columns, resolver)
        }
        _ => crate::bail_parse_error!(
            "cannot determine type of expression in CHECK constraint; use CAST"
        ),
    }
}

fn resolve_check_function_type(
    function: &Func,
    name: &str,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match function {
        Func::Scalar(function) => {
            resolve_check_scalar_function_type(function, args, columns, resolver)
        }
        Func::Math(function) => Ok(resolve_check_math_function_type(function)),
        #[cfg(feature = "json")]
        Func::Json(function) => Ok(resolve_check_json_function_type(function)),
        Func::Agg(_) => crate::bail_parse_error!("misuse of aggregate function {}()", name),
        Func::External(_) => crate::bail_parse_error!(
            "cannot determine return type of function {}() in CHECK constraint; \
             wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
            name,
            name
        ),
        _ => Ok(CheckExprType::Any),
    }
}

fn resolve_check_scalar_function_type(
    function: &ScalarFunc,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match function {
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
        | ScalarFunc::UnixEpoch => Ok(CheckExprType::Integer),
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
        | ScalarFunc::StringReverse => Ok(CheckExprType::Text),
        ScalarFunc::Round | ScalarFunc::JulianDay => Ok(CheckExprType::Real),
        ScalarFunc::RandomBlob | ScalarFunc::ZeroBlob | ScalarFunc::Unhex | ScalarFunc::SetByte => {
            Ok(CheckExprType::Blob)
        }
        ScalarFunc::Abs | ScalarFunc::Nullif => {
            args.first().map_or(Ok(CheckExprType::Any), |arg| {
                resolve_check_expr_type(arg, columns, resolver)
            })
        }
        ScalarFunc::Coalesce | ScalarFunc::IfNull => {
            for arg in args {
                let arg_type = resolve_check_expr_type(arg, columns, resolver)?;
                if arg_type != CheckExprType::Null {
                    return Ok(arg_type);
                }
            }
            Ok(CheckExprType::Null)
        }
        ScalarFunc::Min | ScalarFunc::Max => args.first().map_or(Ok(CheckExprType::Any), |arg| {
            resolve_check_expr_type(arg, columns, resolver)
        }),
        ScalarFunc::Iif if args.len() >= 2 => resolve_check_expr_type(&args[1], columns, resolver),
        ScalarFunc::Iif => Ok(CheckExprType::Any),
        ScalarFunc::TestUintEncode
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
        | ScalarFunc::NumericDiv => Ok(CheckExprType::Blob),
        _ => Ok(CheckExprType::Any),
    }
}

fn resolve_check_math_function_type(function: &MathFunc) -> CheckExprType {
    match function {
        MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc => {
            CheckExprType::Integer
        }
        _ => CheckExprType::Real,
    }
}

#[cfg(feature = "json")]
fn resolve_check_json_function_type(function: &crate::function::JsonFunc) -> CheckExprType {
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
        | JsonFunc::JsonType => CheckExprType::Text,
        JsonFunc::Jsonb
        | JsonFunc::JsonbArray
        | JsonFunc::JsonbObject
        | JsonFunc::JsonbPatch
        | JsonFunc::JsonbRemove
        | JsonFunc::JsonbReplace
        | JsonFunc::JsonbInsert
        | JsonFunc::JsonbSet => CheckExprType::Blob,
        JsonFunc::JsonArrayLength | JsonFunc::JsonErrorPosition | JsonFunc::JsonValid => {
            CheckExprType::Integer
        }
        JsonFunc::JsonExtract
        | JsonFunc::JsonbExtract
        | JsonFunc::JsonArrowExtract
        | JsonFunc::JsonArrowShiftExtract => CheckExprType::Any,
    }
}

fn resolve_check_column_type(
    column: &ast::ColumnDefinition,
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match &column.col_type {
        Some(column_type) => resolve_check_type_name(&column_type.name, resolver),
        None => Ok(CheckExprType::Any),
    }
}

fn resolve_check_type_name(type_name: &str, resolver: &Resolver) -> Result<CheckExprType> {
    let resolved = turso_macros::match_ignore_ascii_case!(match type_name.as_bytes() {
        b"INT" | b"INTEGER" => Some(CheckExprType::Integer),
        b"REAL" | b"FLOAT" | b"DOUBLE" => Some(CheckExprType::Real),
        b"TEXT" => Some(CheckExprType::Text),
        b"BLOB" => Some(CheckExprType::Blob),
        b"ANY" => Some(CheckExprType::Any),
        _ => None,
    });
    if let Some(resolved) = resolved {
        return Ok(resolved);
    }
    if let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) {
        if resolved.is_domain() {
            return resolve_check_type_name(&resolved.primitive, resolver);
        }
        return Ok(CheckExprType::CustomType(type_name.to_lowercase()));
    }
    crate::bail_parse_error!("unknown type '{}' in CHECK constraint", type_name);
}

fn extract_index_collation<'a>(
    expr: &'a ast::Expr,
    resolver: Option<&Resolver>,
) -> Result<(Option<super::collate::CollationSeq>, &'a ast::Expr)> {
    let mut current = expr;
    let mut collation = None;
    loop {
        current = unwrap_parens(current)?;
        match current {
            ast::Expr::Collate(inner, name) => {
                if collation.is_none() {
                    let resolved = match resolver {
                        Some(resolver) => resolver.resolve_collation(name.as_str())?,
                        None => super::collate::CollationSeq::new(name.as_str())?,
                    };
                    if resolved.is_custom() {
                        crate::bail_parse_error!("custom collations are not supported in indexes");
                    }
                    collation = Some(resolved);
                }
                current = inner.as_ref();
            }
            _ => return Ok((collation, current)),
        }
    }
}

fn bind_index_column<'a>(
    expr: &'a ast::Expr,
    table: &'a BTreeTable,
) -> Option<(usize, String, &'a Column)> {
    let (position, column) = match expr {
        ast::Expr::Id(column_name) | ast::Expr::Name(column_name) => {
            table.get_column(column_name.as_str())?
        }
        // SQLite keeps this backwards-compatibility behavior for a bare string key.
        ast::Expr::Literal(ast::Literal::String(column_name)) => {
            table.get_column(column_name.trim_matches('\''))?
        }
        ast::Expr::Qualified(_, column) | ast::Expr::DoublyQualified(_, _, column) => {
            table.get_column(column.as_str())?
        }
        ast::Expr::RowId { .. } => table.get_rowid_alias_column()?,
        _ => return None,
    };
    let column_name = column
        .name
        .as_ref()
        .expect("indexed column must have a name")
        .clone();
    Some((position, column_name, column))
}

fn is_valid_index_expression(expr: &ast::Expr, table: &BTreeTable) -> bool {
    if matches!(expr, ast::Expr::Literal(ast::Literal::String(_))) {
        return false;
    }

    let table_name = normalize_ident(table.name.as_str());
    let has_column = |name: &str| {
        let name = normalize_ident(name);
        table.columns().iter().any(|column| {
            column
                .name
                .as_ref()
                .is_some_and(|column_name| normalize_ident(column_name) == name)
        })
    };
    let is_table = |name: &str| normalize_ident(name).eq_ignore_ascii_case(&table_name);
    let is_deterministic_function = |name: &str, args: &[Box<ast::Expr>]| {
        let name = normalize_ident(name);
        Func::resolve_function(&name, args.len()).is_ok_and(|function| {
            function.is_some_and(|function| is_deterministic_schema_function_call(&function, args))
        })
    };

    let mut valid = true;
    let _ = walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        if !valid {
            return Ok(WalkControl::SkipChildren);
        }
        match expr {
            ast::Expr::Literal(
                ast::Literal::CurrentDate
                | ast::Literal::CurrentTime
                | ast::Literal::CurrentTimestamp,
            ) => valid = false,
            ast::Expr::Literal(_) | ast::Expr::RowId { .. } => {}
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                if !has_column(name.as_str()) {
                    valid = false;
                }
            }
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column) => {
                if !is_table(namespace.as_str()) || !has_column(column.as_str()) {
                    valid = false;
                }
            }
            ast::Expr::FunctionCall {
                name, filter_over, ..
            }
            | ast::Expr::FunctionCallStar {
                name, filter_over, ..
            } => {
                if filter_over.over_clause.is_some() {
                    valid = false;
                } else {
                    let args = match expr {
                        ast::Expr::FunctionCall { args, .. } => args.as_slice(),
                        ast::Expr::FunctionCallStar { .. } => &[] as &[Box<ast::Expr>],
                        _ => unreachable!(),
                    };
                    if !is_deterministic_function(name.as_str(), args) {
                        valid = false;
                    }
                }
            }
            ast::Expr::Exists(_)
            | ast::Expr::InSelect { .. }
            | ast::Expr::Subquery(_)
            | ast::Expr::Raise { .. }
            | ast::Expr::Variable(_) => valid = false,
            _ => {}
        }
        Ok(if valid {
            WalkControl::Continue
        } else {
            WalkControl::SkipChildren
        })
    });
    valid
}

fn bind_self_table_leaf(name: &str, table: &BTreeTable) -> Option<ast::Expr> {
    if let Some((column, definition)) = table.get_column(name) {
        return Some(ast::Expr::Column {
            database: None,
            table: ast::TableInternalId::SELF_TABLE,
            column,
            is_rowid_alias: definition.is_rowid_alias(),
        });
    }
    if super::planner::ROWID_STRS
        .iter()
        .any(|rowid| rowid.eq_ignore_ascii_case(name))
    {
        return Some(ast::Expr::RowId {
            database: None,
            table: ast::TableInternalId::SELF_TABLE,
        });
    }
    None
}

fn first_unbound_identifier(expr: &ast::Expr) -> Option<String> {
    let mut found = None;
    let _ = walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        if found.is_some() {
            return Ok(WalkControl::SkipChildren);
        }
        match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                found = Some(name.as_str().to_string());
                Ok(WalkControl::SkipChildren)
            }
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column) => {
                found = Some(format!("{}.{}", namespace.as_str(), column.as_str()));
                Ok(WalkControl::SkipChildren)
            }
            _ => Ok(WalkControl::Continue),
        }
    });
    found
}

pub struct LogicalScopeColumn<'a> {
    pub name: &'a str,
    pub database: Option<&'a str>,
    pub table: Option<&'a str>,
    pub table_alias: Option<&'a str>,
}

pub struct BoundLogicalColumn {
    pub name: String,
    pub table: Option<String>,
}

pub enum BoundLogicalSource {
    CommonTableExpression {
        name: String,
    },
    Table {
        database: Option<String>,
        name: String,
        table: Arc<Table>,
    },
}

/// Bind a logical-planner table source against visible CTEs and the schema.
pub fn bind_logical_source<'a>(
    source: &ast::QualifiedName,
    cte_names: impl IntoIterator<Item = &'a str>,
    schema: &Schema,
) -> Result<BoundLogicalSource> {
    let database = source.db_name.as_ref().map(|name| name.as_str());
    let name = source.name.as_str();

    if database.is_none() {
        if let Some(cte_name) = cte_names
            .into_iter()
            .find(|cte_name| cte_name.eq_ignore_ascii_case(name))
        {
            return Ok(BoundLogicalSource::CommonTableExpression {
                name: cte_name.to_string(),
            });
        }
    }

    let Some(table) = schema.get_table(name) else {
        let qualified_name =
            database.map_or_else(|| name.to_string(), |database| format!("{database}.{name}"));
        crate::bail_parse_error!("no such table: {}", qualified_name);
    };
    let name = table.get_name().to_string();
    Ok(BoundLogicalSource::Table {
        database: database.map(str::to_string),
        name,
        table,
    })
}

/// Bind a raw logical-planner column expression against its input schema.
pub fn bind_logical_column<'a>(
    expr: &ast::Expr,
    columns: impl IntoIterator<Item = LogicalScopeColumn<'a>>,
) -> Result<Option<BoundLogicalColumn>> {
    let (database, table, column) = match expr {
        ast::Expr::Id(column) | ast::Expr::Name(column) => (None, None, column.as_str()),
        ast::Expr::Qualified(table, column) => (None, Some(table.as_str()), column.as_str()),
        ast::Expr::DoublyQualified(database, table, column) => (
            Some(database.as_str()),
            Some(table.as_str()),
            column.as_str(),
        ),
        _ => return Ok(None),
    };

    let mut matches = columns
        .into_iter()
        .filter(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .filter(|candidate| {
            let table_matches = table.is_none_or(|table| {
                candidate
                    .table_alias
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(table))
                    || candidate
                        .table
                        .is_some_and(|name| name.eq_ignore_ascii_case(table))
            });
            let database_matches = database.is_none_or(|database| {
                candidate
                    .database
                    .is_some_and(|name| name.eq_ignore_ascii_case(database))
            });
            table_matches && database_matches
        });
    let Some(bound) = matches.next() else {
        let name = match (database, table) {
            (Some(database), Some(table)) => format!("{database}.{table}.{column}"),
            (None, Some(table)) => format!("{table}.{column}"),
            _ => column.to_string(),
        };
        crate::bail_parse_error!("no such column: {}", name);
    };
    if matches.next().is_some() {
        crate::bail_parse_error!("ambiguous column name: {}", column);
    }

    let table = if let (Some(database), Some(table)) = (bound.database, bound.table) {
        Some(format!("{database}.{table}"))
    } else if let Some(alias) = bound.table_alias {
        Some(alias.to_string())
    } else {
        bound.table.map(str::to_string)
    };
    Ok(Some(BoundLogicalColumn {
        name: bound.name.to_string(),
        table,
    }))
}

fn bind_table_index_expressions(
    resolver: &Resolver,
    database_id: usize,
    table_name: &str,
    internal_id: ast::TableInternalId,
) -> Vec<super::plan::BoundIndexExpressions> {
    resolver
        .with_schema(database_id, |schema| {
            schema.get_indices(table_name).cloned().collect::<Vec<_>>()
        })
        .into_iter()
        .map(|index| {
            let mut columns = index
                .columns
                .iter()
                .map(|column| column.expr.clone())
                .collect::<Vec<_>>();
            let mut where_clause = index.where_clause.clone();
            for expr in columns.iter_mut().flatten() {
                rebase_schema_expr(expr, internal_id);
                rewrite_between_expressions(expr);
            }
            if let Some(expr) = where_clause.as_mut() {
                rebase_schema_expr(expr, internal_id);
                rewrite_between_expressions(expr);
            }
            super::plan::BoundIndexExpressions {
                index_name: index.name.clone(),
                columns,
                where_clause,
            }
        })
        .collect()
}

/// Validate a referenced CTE's explicit column list against its SELECT's
/// result column count. SQLite defers this check until the CTE is actually
/// referenced, so unreferenced CTEs with mismatched counts don't error.
fn validate_cte_explicit_columns(name: &str, cte: &CteEntry) -> Result<()> {
    if !cte.explicit_columns.is_empty()
        && cte.result_column_count != 0
        && cte.explicit_columns.len() != cte.result_column_count
    {
        crate::bail_parse_error!(
            "table {} has {} values for {} columns",
            name,
            cte.result_column_count,
            cte.explicit_columns.len()
        );
    }
    Ok(())
}

// ── IdGenerator ─────────────────────────────────────────────────────────

pub trait IdGenerator {
    fn next_table_id(&mut self) -> TableInternalId;
    fn next_cte_id(&mut self) -> usize;
}

impl IdGenerator for ProgramBuilder {
    fn next_table_id(&mut self) -> ast::TableInternalId {
        self.table_reference_counter.next()
    }

    fn next_cte_id(&mut self) -> usize {
        self.alloc_cte_id()
    }
}

// ── BindTable ───────────────────────────────────────────────────────────

/// Trait for table metadata needed during binding (column name resolution).
pub trait BindTable {
    fn column_count(&self) -> usize;
    fn column_name(&self, idx: usize) -> Option<&str>;
    fn column_is_rowid_alias(&self, idx: usize) -> bool;
    fn column_is_hidden(&self, idx: usize) -> bool;
}

/// Validate the identifier leaves of an expression that binds against no
/// tables at all, such as single-row INSERT VALUES and a recursive CTE's
/// body-level LIMIT. DQS does not apply to these expressions.
fn bind_scopeless_expr(expr: &mut ast::Expr, resolver: &Resolver) -> Result<()> {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        match expr {
            ast::Expr::Id(id) => {
                crate::bail_parse_error!("no such column: {}", id.as_str());
            }
            ast::Expr::Qualified(tbl, id) => {
                crate::bail_parse_error!("no such column: {}.{}", tbl.as_str(), id.as_str());
            }
            ast::Expr::DoublyQualified(db, tbl, id) => {
                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    db.as_str(),
                    tbl.as_str(),
                    id.as_str()
                );
            }
            ast::Expr::FunctionCall { name, args, .. } => {
                super::expr::validate_custom_type_function_call(name.as_str(), args, resolver)?;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

impl dyn BindTable {
    /// Create a column iterator for any `dyn BindTable`.
    pub fn columns(&self) -> BindColumnIter<'_, Self> {
        BindColumnIter {
            table: self,
            idx: 0,
        }
    }
}

pub struct BindColumnIter<'a, T: BindTable + ?Sized> {
    table: &'a T,
    idx: usize,
}

pub struct BindColumnRef<'a> {
    pub idx: usize,
    pub name: &'a str,
    pub is_rowid_alias: bool,
    pub is_hidden: bool,
}

impl<'a, T: BindTable + ?Sized> Iterator for BindColumnIter<'a, T> {
    type Item = BindColumnRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // Unnamed columns (e.g. unaliased expressions in an ephemeral scratch
        // table) cannot be referenced by name and are skipped.
        while self.idx < self.table.column_count() {
            let i = self.idx;
            self.idx += 1;
            if let Some(name) = self.table.column_name(i) {
                return Some(BindColumnRef {
                    idx: i,
                    name,
                    is_rowid_alias: self.table.column_is_rowid_alias(i),
                    is_hidden: self.table.column_is_hidden(i),
                });
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.table.column_count() - self.idx))
    }
}

impl BindTable for Table {
    fn column_count(&self) -> usize {
        self.columns().len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns().get(idx).and_then(|c| c.name.as_deref())
    }

    fn column_is_rowid_alias(&self, idx: usize) -> bool {
        self.columns().get(idx).is_some_and(|c| c.is_rowid_alias())
    }

    fn column_is_hidden(&self, idx: usize) -> bool {
        self.columns().get(idx).is_some_and(|c| c.hidden())
    }
}

/// Lightweight table for CTEs — just column names, no schema object.
pub struct CteTable {
    pub columns: Vec<String>,
}

impl BindTable for CteTable {
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| s.as_str())
    }

    fn column_is_rowid_alias(&self, _idx: usize) -> bool {
        false
    }

    fn column_is_hidden(&self, _idx: usize) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct DerivedTable {
    pub columns: Vec<String>,
}

impl BindTable for DerivedTable {
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| s.as_str())
    }

    fn column_is_rowid_alias(&self, _idx: usize) -> bool {
        false
    }

    fn column_is_hidden(&self, _idx: usize) -> bool {
        false
    }
}

// ── BindPhase ────────────────────────────────────────────────────────────

/// Controls alias visibility per SQL clause.
///
/// Replaces `BindingBehavior`. The phase is set on the [`BindContext`]
/// before binding each clause rather than passed per-call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindPhase {
    /// Phases 1–4: CTE, FROM, Window definitions, SELECT expressions.
    /// Only table columns visible; aliases not accessible.
    NoAliases,
    /// Phase 5: WHERE clause.
    /// Table columns first; SELECT aliases as fallback.
    TableFirst,
    /// Phases 6–8: GROUP BY, HAVING, ORDER BY.
    /// SELECT aliases first; table columns as fallback.
    AliasFirst,
}

// ── ScopeTable ───────────────────────────────────────────────────────────

/// A table visible within a single query scope.
///
/// Cheap to clone (table metadata is Arc-wrapped).
#[derive(Clone)]
pub struct ScopeTable {
    /// The name used to refer to this table in the query (original name or alias).
    pub identifier: String,
    /// Opaque ID used in `Expr::Column` to reference this table.
    pub internal_id: TableInternalId,
    /// Planner-facing source data for producing `TableReferences`.
    pub source: ScopeTableSource,
    /// Table metadata for column resolution. Clone is an Arc bump.
    pub table: Arc<dyn BindTable>,
    /// Join constraint info (USING clause for dedup during unqualified lookup).
    pub join_info: Option<JoinInfo>,
    /// Database ID for attached database support (0 = main).
    pub database_id: usize,
    /// INDEXED BY / NOT INDEXED hint from the FROM clause (real tables only).
    pub indexed: Option<ast::Indexed>,
    /// Custom index-method patterns bound to `internal_id`.
    pub bound_index_method_patterns: Vec<super::plan::BoundIndexMethodPattern>,
    /// Schema index expressions rebound to `internal_id`.
    pub bound_index_expressions: Vec<super::plan::BoundIndexExpressions>,
}

#[derive(Clone)]
pub enum ScopeTableSource {
    Table(Arc<Table>),
    Cte { name: String },
    Derived {},
}

// ── BindScope ────────────────────────────────────────────────────────────

#[derive(Clone)]
/// Snapshot of all tables visible at one query level.
///
/// Analogous to DataFusion's `DFSchema`. Owned, Arc-wrapped for cheap
/// sharing when pushed onto the outer-scope stack.
pub struct BindScope {
    pub tables: Vec<ScopeTable>,
    /// Whether tables were swapped for a RIGHT→LEFT JOIN rewrite
    /// (affects star expansion order).
    pub right_join_swapped: bool,
}

pub type BindScopeRef = Arc<BindScope>;

impl BindScope {
    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            right_join_swapped: false,
        }
    }

    /// Find an unqualified column by name across all tables in scope.
    ///
    /// Returns `(table_internal_id, column_index, is_rowid_alias)` or `None`.
    /// Errors on ambiguity (unless deduplicated by USING clause).
    pub fn find_column_unqualified(
        &self,
        name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool)>> {
        let normalized = normalize_ident(name);
        let mut result: Option<(TableInternalId, usize, bool)> = None;

        for st in &self.tables {
            let col_idx = st
                .table
                .columns()
                .position(|col| col.name.eq_ignore_ascii_case(&normalized));

            if let Some(idx) = col_idx {
                if result.is_some() {
                    let in_using = st.join_info.as_ref().is_some_and(|ji| {
                        ji.using
                            .iter()
                            .any(|u| u.as_str().eq_ignore_ascii_case(&normalized))
                    });
                    if !in_using {
                        crate::bail_parse_error!("ambiguous column name: {}", name);
                    }
                } else {
                    result = Some((st.internal_id, idx, st.table.column_is_rowid_alias(idx)));
                }
            }
        }

        Ok(result)
    }

    /// Find a qualified column (`table.column`) in this scope.
    ///
    /// SQLite allows the same table name/alias to appear more than once in a
    /// FROM clause; every table whose identifier matches is a candidate, and
    /// a column present on more than one candidate is ambiguous (unless the
    /// duplicate is deduplicated by a USING/NATURAL join on that column).
    ///
    /// Returns `None` if no table matches the identifier (caller can try
    /// outer scopes). Errors if a table matches but the column doesn't.
    pub fn find_column_qualified(
        &self,
        table_name: &str,
        col_name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool)>> {
        let normalized_table = normalize_ident(table_name);
        let normalized_col = normalize_ident(col_name);

        let mut identifier_matched = false;
        let mut result: Option<(TableInternalId, usize, bool)> = None;
        for st in self
            .tables
            .iter()
            .filter(|t| t.identifier == normalized_table)
        {
            identifier_matched = true;
            let Some(idx) = st
                .table
                .columns()
                .position(|col| col.name.eq_ignore_ascii_case(&normalized_col))
            else {
                continue;
            };
            if result.is_some() {
                let in_using = st.join_info.as_ref().is_some_and(|ji| {
                    ji.using
                        .iter()
                        .any(|u| u.as_str().eq_ignore_ascii_case(&normalized_col))
                });
                if !in_using {
                    crate::bail_parse_error!("ambiguous column name: {}.{}", table_name, col_name);
                }
                continue;
            }
            result = Some((st.internal_id, idx, st.table.column_is_rowid_alias(idx)));
        }

        if !identifier_matched {
            return Ok(None);
        }
        let Some(found) = result else {
            crate::bail_parse_error!("no such column: {}.{}", table_name, col_name);
        };
        Ok(Some(found))
    }

    /// Find a table by its identifier (name or alias).
    pub fn find_table_by_identifier(&self, name: &str) -> Option<&ScopeTable> {
        let normalized = normalize_ident(name);
        self.tables.iter().find(|t| t.identifier == normalized)
    }
}

// ── BoundColumn ─────────────────────────────────────────────────────────

/// A resolved result column from a SELECT list.
/// Used for alias resolution in later phases (WHERE, GROUP BY, ORDER BY)
/// and for propagating column names to CTEs/subqueries.
#[derive(Clone)]
pub struct BoundColumn {
    /// The column name — explicit alias or inferred from the expression.
    pub name: String,
    /// The original expression (before binding), cloned into alias references.
    pub expr: ast::Expr,
    /// True if the name comes from an explicit AS alias (not inferred from expr).
    pub is_explicit_alias: bool,
}

/// A subquery expression that was bound during binding.
/// The inner `ast::Select` is already bound (column refs resolved).
/// The planner uses this to plan the subquery without re-binding.
pub struct BoundSubquery {
    /// The bound inner SELECT.
    pub select: ast::Select,
    /// Inner binding results (scopes → table references).
    pub inner_bound: BoundSelect,
}

/// Bound arms of a recursive CTE body, produced at bind time and consumed by
/// the recursive-CTE planner.
pub struct RecursiveCteBinding {
    /// The initial (non-recursive) arms as one bound SELECT, including the
    /// body-level WITH clause. SQLite takes the CTE's column names and arity
    /// from the left-most arm, which cannot reference the recursive table.
    pub initial: BoundSubquery,
    /// Each recursive arm bound as its own single-arm SELECT. The body-level
    /// WITH clause is cloned into each arm, matching the per-arm nested CTE
    /// planning of compound SELECTs on the raw path.
    pub recursive_arms: Vec<BoundSubquery>,
    /// Index of the first recursive arm (arm 0 is the body's first SELECT),
    /// as returned by `validate_recursive_cte_structure`.
    pub first_recursive_arm_index: usize,
    /// The table id every self-reference in the recursive arms was bound to.
    /// All recursive arms read the same recursive input table, so the planner
    /// creates that table with this id.
    pub input_id: ast::TableInternalId,
    /// Body-level ORDER BY resolved to output-column positions.
    pub queue_order: Option<Vec<super::plan::CompoundOrderByKey>>,
}

/// Set while a recursive CTE's recursive arms are being bound: the CTE's own
/// name resolves to the recursive input table instead of raising a circular
/// reference.
struct RecursiveSelfRef {
    /// Identity of the CTE being bound (a shadowing nested CTE with the same
    /// name has a different id and resolves normally).
    cte_id: usize,
    /// Shared id for every self-reference (see [RecursiveCteBinding::input_id]).
    input_id: ast::TableInternalId,
    /// Column metadata for resolving references to the recursive table.
    table: Arc<CteTable>,
}

pub struct BoundSelect {
    pub result_columns: Arc<Vec<BoundColumn>>,
    /// Result-column metadata for each compound arm after the first.
    pub compound_result_columns: Vec<Arc<Vec<BoundColumn>>>,
    /// Compound ORDER BY resolved to output-column positions.
    pub compound_order_by: Option<Vec<super::plan::CompoundOrderByKey>>,
    pub main_scope: BindScope,
    pub compound_scopes: Vec<BindScope>,
    pub tracking: BindTracking,
    /// Expression subqueries (EXISTS, scalar subquery, IN SELECT) keyed by
    /// the `subquery_id` stored in the corresponding `Expr::SubqueryResult`.
    pub subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    /// CTE definitions from the WITH clause, in definition order.
    /// Populated only for the top-level select that owns the WITH clause.
    pub cte_definitions: Vec<(String, CteEntry)>,
    /// FROM-clause subqueries (derived tables), keyed by the scope table's
    /// `internal_id`. Planned before `into_table_references` and looked up
    /// by the `Derived` arm in `scope_to_table_references`.
    pub derived_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
}

fn ordinal(n: usize) -> String {
    let suffix = match (n % 10, n % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}

fn resolve_compound_order_by_expr(
    expr: &ast::Expr,
    result_column_arms: &[&[BoundColumn]],
    term_number: usize,
) -> Result<(usize, Option<super::collate::CollationSeq>)> {
    let num_result_columns = result_column_arms
        .first()
        .expect("compound SELECT must have a first arm")
        .len();
    match expr {
        ast::Expr::Collate(inner, collation_name) => {
            let (column, _) =
                resolve_compound_order_by_expr(inner, result_column_arms, term_number)?;
            Ok((
                column,
                Some(super::collate::CollationSeq::new(collation_name.as_str())?),
            ))
        }
        ast::Expr::Literal(ast::Literal::Numeric(number)) => {
            let Ok(column_number) = number.parse::<i32>() else {
                crate::bail_parse_error!(
                    "{} ORDER BY term does not match any column in the result set",
                    ordinal(term_number)
                );
            };
            if column_number <= 0 || column_number as usize > num_result_columns {
                crate::bail_parse_error!(
                    "{} ORDER BY term out of range - should be between 1 and {}",
                    column_number,
                    num_result_columns
                );
            }
            Ok((column_number as usize - 1, None))
        }
        ast::Expr::Id(name) => {
            let normalized = normalize_ident(name.as_str());
            for result_columns in result_column_arms {
                if let Some((column, _)) = result_columns.iter().enumerate().find(|(_, column)| {
                    column.is_explicit_alias && column.name.eq_ignore_ascii_case(&normalized)
                }) {
                    return Ok((column, None));
                }
                if let Some((column, _)) = result_columns.iter().enumerate().find(|(_, column)| {
                    !column.name.is_empty() && column.name.eq_ignore_ascii_case(&normalized)
                }) {
                    return Ok((column, None));
                }
            }
            crate::bail_parse_error!(
                "{} ORDER BY term does not match any column in the result set",
                ordinal(term_number)
            );
        }
        _ => crate::bail_parse_error!(
            "{} ORDER BY term does not match any column in the result set",
            ordinal(term_number)
        ),
    }
}

fn resolve_compound_order_by(
    order_by: &[ast::SortedColumn],
    result_column_arms: &[&[BoundColumn]],
) -> Result<Option<Vec<super::plan::CompoundOrderByKey>>> {
    if order_by.is_empty() {
        return Ok(None);
    }
    order_by
        .iter()
        .enumerate()
        .map(|(index, term)| {
            let (column, collation) =
                resolve_compound_order_by_expr(&term.expr, result_column_arms, index + 1)?;
            Ok((
                column,
                term.order.unwrap_or(ast::SortOrder::Asc),
                term.nulls,
                collation,
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

pub struct BoundUpdate {
    /// Scope containing only the target table (with alias/INDEXED BY).
    pub target_scope: BindScope,
    /// Scope for the UPDATE ... FROM clause tables, if present.
    pub from_scope: Option<BindScope>,
    pub tracking: BindTracking,
    pub subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    pub derived_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    pub cte_definitions: Vec<(String, CteEntry)>,
    /// Database the target table lives in (0 = main).
    pub database_id: usize,
    /// The validated target table.
    pub table: Arc<Table>,
    /// `OR <conflict>` clause taken off the statement during binding.
    pub or_conflict: Option<ast::ResolveType>,
}

impl BoundUpdate {
    /// Convert the target scope into a single-table `TableReferences`.
    pub fn target_table_references(
        &self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
    ) -> Result<TableReferences> {
        BoundSelect::scope_to_table_references(
            self.target_scope.clone(),
            &self.tracking,
            planned_ctes,
            &mut HashMap::default(),
            Vec::new(),
        )
    }

    /// Convert the FROM-clause scope (if any) into `TableReferences`.
    #[allow(clippy::wrong_self_convention)]
    pub fn from_table_references(
        &mut self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
        planned_derived: &mut HashMap<ast::TableInternalId, super::plan::JoinedTable>,
    ) -> Result<TableReferences> {
        let Some(scope) = self.from_scope.take() else {
            return Ok(TableReferences::new_empty());
        };
        BoundSelect::scope_to_table_references(
            scope,
            &self.tracking,
            planned_ctes,
            planned_derived,
            Vec::new(),
        )
    }
}

pub struct BoundDelete {
    pub scope: BindScope,
    pub tracking: BindTracking,
    pub subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    pub cte_definitions: Vec<(String, CteEntry)>,
    /// Database the target table lives in (0 = main).
    pub database_id: usize,
    /// The validated target table.
    pub table: Arc<Table>,
}

impl BoundDelete {
    pub fn into_table_references(
        self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
    ) -> Result<TableReferences> {
        BoundSelect::scope_to_table_references(
            self.scope,
            &self.tracking,
            planned_ctes,
            &mut HashMap::default(),
            Vec::new(),
        )
    }
}

// ── Statement-level binding ──────────────────────────────────────────────

/// Bind a SELECT statement before planning or emission.
pub fn bind_select_stmt(
    select: &mut ast::Select,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<BoundSelect> {
    let mut binder = BindContext::new(resolver, program);
    binder.bind_select(select)
}

/// Bind a DELETE statement up front: validate the target table and resolve
/// all names in WHERE and RETURNING. Planning consumes the result without
/// re-resolving anything.
#[allow(clippy::too_many_arguments)]
pub fn bind_delete_stmt(
    tbl_name: &ast::QualifiedName,
    indexed: Option<ast::Indexed>,
    where_clause: &mut Option<Box<ast::Expr>>,
    returning: &mut Vec<ast::ResultColumn>,
    with: &mut Option<ast::With>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<BoundDelete> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(tbl_name)?;
    let normalized_table_name = normalize_ident(tbl_name.name.as_str());
    let table = validate_delete(
        resolver,
        &normalized_table_name,
        database_id,
        program,
        connection,
    )?;
    let mut binder = BindContext::new(resolver, program);
    binder.bind_delete(
        tbl_name,
        indexed,
        where_clause,
        returning,
        with,
        database_id,
        table,
    )
}

/// Bind an UPDATE statement up front: validate the target table and resolve
/// all names in FROM/SET/WHERE/RETURNING. Planning consumes the result
/// without re-resolving anything.
pub fn bind_update_stmt(
    body: &mut ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    is_internal_schema_change: bool,
) -> Result<BoundUpdate> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(&body.tbl_name)?;
    let target_name = &body.tbl_name.name;
    let table = match resolver.with_schema(database_id, |s| s.get_table(target_name.as_str())) {
        Some(table) => table,
        None => crate::bail_parse_error!("Parse error: no such table: {}", target_name),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!(
            "unsafe use of virtual table \"{}\"",
            body.tbl_name.name.as_str()
        );
    }
    if table.btree().is_some_and(|bt| !bt.has_rowid) {
        crate::bail_parse_error!("UPDATE of WITHOUT ROWID tables is not supported");
    }
    validate_update(
        resolver.schema(),
        body,
        target_name.as_str(),
        is_internal_schema_change,
        connection,
    )?;
    let mut binder = BindContext::new(resolver, program);
    binder.bind_update(body, database_id, table)
}

/// Validate the DELETE target, returning the underlying table if validation
/// passes.
fn validate_delete(
    resolver: &Resolver,
    tbl_name: &str,
    database_id: usize,
    program: &ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<Arc<Table>> {
    // Check if this is a system table that should be protected from direct writes
    if !connection.is_nested_stmt()
        && !connection.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(tbl_name)
    {
        crate::bail_parse_error!("table {tbl_name} may not be modified");
    }
    let table = match resolver.with_schema(database_id, |s| s.get_table(tbl_name)) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", tbl_name),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!("unsafe use of virtual table \"{}\"", tbl_name);
    }
    if table.btree().is_some_and(|bt| !bt.has_rowid) {
        crate::bail_parse_error!("DELETE from WITHOUT ROWID tables is not supported");
    }

    // Check if this is a materialized view
    if resolver.schema().is_materialized_view(tbl_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", tbl_name);
    }

    // Check if this table has any incompatible dependent views
    resolver.schema().with_incompatible_dependent_views(tbl_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot DELETE from table '{tbl_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().fold(String::new(), |_, s| s.to_string() + ", "),
        );
    }
    Ok(())
    })?;
    Ok(table)
}

pub enum BoundUpsertDo {
    Nothing,
    Update {
        sets: Vec<(usize, Box<ast::Expr>)>,
        where_clause: Option<Box<ast::Expr>>,
    },
}

pub type BoundUpsertAction = (
    super::upsert::ResolvedUpsertTarget,
    crate::vdbe::BranchOffset,
    BoundUpsertDo,
);

/// Output of the statement-level bind phase for INSERT.
///
/// Single-row VALUES bind scope-less because they have no FROM clause.
/// UPSERT conflict targets bind against the schema table and `DO UPDATE`
/// expressions bind against the target row plus the EXCLUDED pseudo-table.
/// Multi-row/SELECT sources are fully bound here. Virtual table inserts still
/// consume their restricted VALUES body directly.
pub struct BoundInsert {
    #[allow(clippy::vec_box)]
    pub values: Vec<Box<ast::Expr>>,
    pub upsert_actions: Vec<BoundUpsertAction>,
    pub inserting_multiple_rows: bool,
    /// Database the target table lives in (0 = main).
    pub database_id: usize,
    /// The validated target table.
    pub table: Arc<Table>,
    /// Bound source for INSERT SELECT and VALUES paths that use a coroutine.
    pub source_select: Option<BoundSelect>,
    /// WITH-clause definitions visible to RETURNING subqueries.
    pub returning_cte_definitions: Vec<(String, CteEntry)>,
    /// Subqueries extracted while binding RETURNING expressions.
    pub returning_subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,
    /// ID used by bound references to the current target row.
    pub target_table_id: ast::TableInternalId,
    /// ID used by bound references to the would-be inserted row.
    pub excluded_table_id: ast::TableInternalId,
    /// Stored index keys and predicates bound to the target table reference.
    pub bound_index_expressions: Vec<super::plan::BoundIndexExpressions>,
}

fn insert_value_types(
    table: &Table,
    columns: &[ast::Name],
    resolver: &Resolver,
) -> Result<Vec<Option<Arc<TypeDef>>>> {
    if columns.is_empty() {
        return Ok(table
            .columns()
            .iter()
            .filter(|column| !column.hidden() && !column.is_generated())
            .map(|column| {
                resolver
                    .schema()
                    .get_type_def_unchecked(&column.ty_str)
                    .cloned()
            })
            .collect());
    }

    columns
        .iter()
        .map(|name| {
            let name = normalize_ident(name.as_str());
            if let Some((_, column)) = table.get_column_by_name(&name) {
                column.ensure_not_generated("INSERT into", &name)?;
                Ok(resolver
                    .schema()
                    .get_type_def_unchecked(&column.ty_str)
                    .cloned())
            } else if super::planner::ROWID_STRS
                .iter()
                .any(|rowid| rowid.eq_ignore_ascii_case(&name))
            {
                Ok(None)
            } else {
                crate::bail_parse_error!("table {} has no column named {}", table.get_name(), name)
            }
        })
        .collect()
}

fn upsert_scope_table(
    identifier: String,
    internal_id: ast::TableInternalId,
    database_id: usize,
    table: &Arc<Table>,
) -> ScopeTable {
    ScopeTable {
        identifier,
        internal_id,
        source: ScopeTableSource::Table(Arc::clone(table)),
        table: Arc::clone(table) as Arc<dyn BindTable>,
        join_info: None,
        database_id,
        indexed: None,
        bound_index_method_patterns: Vec::new(),
        bound_index_expressions: Vec::new(),
    }
}

fn bind_upsert_conflict_target<G: IdGenerator>(
    binder: &mut BindContext<'_, G>,
    upsert: &mut ast::Upsert,
    database_id: usize,
    table: &Arc<Table>,
) -> Result<()> {
    let Some(target) = upsert.index.as_mut() else {
        return Ok(());
    };
    let scope = BindScope {
        tables: vec![upsert_scope_table(
            normalize_ident(table.get_name()),
            ast::TableInternalId::SELF_TABLE,
            database_id,
            table,
        )],
        right_join_swapped: false,
    };
    binder.with_phase(BindPhase::NoAliases, |binder| {
        for target in &mut target.targets {
            binder.bind_expr(&mut target.expr, &scope)?;
        }
        if let Some(where_clause) = target.where_clause.as_mut() {
            binder.bind_expr(where_clause, &scope)?;
        }
        Ok(())
    })?;

    // Schema expressions use SELF_TABLE without a database qualifier. Keep
    // conflict targets in that same canonical form before comparing them.
    walk_expr_mut_in_upsert_target(target, &mut |expr| {
        match expr {
            ast::Expr::Column {
                database, table, ..
            }
            | ast::Expr::RowId { database, table }
                if table.is_self_table() =>
            {
                *database = None;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn walk_expr_mut_in_upsert_target(
    target: &mut ast::UpsertIndex,
    visitor: &mut impl FnMut(&mut ast::Expr) -> Result<WalkControl>,
) -> Result<()> {
    for column in &mut target.targets {
        walk_expr_mut(&mut column.expr, visitor)?;
    }
    if let Some(where_clause) = target.where_clause.as_mut() {
        walk_expr_mut(where_clause, visitor)?;
    }
    Ok(())
}

fn collect_upsert_set_clauses(
    table: &Table,
    set_items: Vec<ast::Set>,
) -> Result<Vec<(usize, Box<ast::Expr>)>> {
    let lookup: HashMap<String, usize> = table
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            column
                .name
                .as_ref()
                .map(|name| (name.to_lowercase(), index))
        })
        .collect();
    let mut result = Vec::new();

    for set in set_items {
        let values = match *set.expr {
            ast::Expr::Parenthesized(values) => values,
            expr => vec![Box::new(expr)],
        };
        if set.col_names.len() != values.len() {
            crate::bail_parse_error!(
                "{} columns assigned {} values",
                set.col_names.len(),
                values.len()
            );
        }
        for (name, expr) in set.col_names.iter().zip(values) {
            let Some(index) = lookup.get(&normalize_ident(name.as_str())).copied() else {
                crate::bail_parse_error!("no such column: {}", name);
            };
            table.columns()[index].ensure_not_generated("UPDATE", name.as_str())?;
            if let Some(existing) = result
                .iter_mut()
                .find(|(existing_index, _)| *existing_index == index)
            {
                existing.1 = expr;
            } else {
                result.push((index, expr));
            }
        }
    }
    Ok(result)
}

fn assignment_type(
    table: &Table,
    column_name: &ast::Name,
    resolver: &Resolver,
) -> Result<Option<Arc<TypeDef>>> {
    let name = normalize_ident(column_name.as_str());
    let Some((_, column)) = table.get_column_by_name(&name) else {
        crate::bail_parse_error!("no such column: {}", column_name);
    };
    Ok(resolver
        .schema()
        .get_type_def_unchecked(&column.ty_str)
        .cloned())
}

fn bind_update_set<G: IdGenerator>(
    binder: &mut BindContext<'_, G>,
    set: &mut ast::Set,
    scope: &BindScope,
    table: &Table,
) -> Result<()> {
    match set.expr.as_mut() {
        ast::Expr::Parenthesized(values) => {
            if set.col_names.len() != values.len() {
                crate::bail_parse_error!(
                    "{} columns assigned {} values",
                    set.col_names.len(),
                    values.len()
                );
            }
            for (column_name, expr) in set.col_names.iter().zip(values) {
                let expected_type = assignment_type(table, column_name, binder.resolver)?;
                binder.bind_expr_with_expected_type(expr, scope, expected_type.as_deref())?;
            }
        }
        expr => {
            if set.col_names.len() != 1 {
                crate::bail_parse_error!("{} columns assigned 1 values", set.col_names.len());
            }
            let expected_type = assignment_type(table, &set.col_names[0], binder.resolver)?;
            binder.bind_expr_with_expected_type(expr, scope, expected_type.as_deref())?;
        }
    }
    Ok(())
}

fn bind_upsert_do<G: IdGenerator>(
    binder: &mut BindContext<'_, G>,
    do_clause: ast::UpsertDo,
    target_identifier: String,
    target_table_id: ast::TableInternalId,
    excluded_table_id: ast::TableInternalId,
    database_id: usize,
    table: &Arc<Table>,
) -> Result<BoundUpsertDo> {
    let ast::UpsertDo::Set {
        sets,
        mut where_clause,
    } = do_clause
    else {
        return Ok(BoundUpsertDo::Nothing);
    };
    let mut sets = collect_upsert_set_clauses(table, sets)?;
    let target_scope = BindScope {
        tables: vec![upsert_scope_table(
            target_identifier,
            target_table_id,
            database_id,
            table,
        )],
        right_join_swapped: false,
    };
    let excluded_scope = BindScope {
        tables: vec![upsert_scope_table(
            "excluded".to_string(),
            excluded_table_id,
            database_id,
            table,
        )],
        right_join_swapped: false,
    };
    #[expect(clippy::arc_with_non_send_sync)]
    binder.append_outer_query_scope(Arc::new(excluded_scope), Arc::new(Vec::new()));
    let bind_result = binder.with_phase(BindPhase::NoAliases, |binder| {
        for (column_index, expr) in &mut sets {
            let expected_type = binder
                .resolver
                .schema()
                .get_type_def_unchecked(&table.columns()[*column_index].ty_str)
                .cloned();
            binder.bind_expr_with_expected_type(expr, &target_scope, expected_type.as_deref())?;
        }
        if let Some(where_clause) = where_clause.as_mut() {
            binder.bind_expr(where_clause, &target_scope)?;
        }
        Ok(())
    });
    binder.pop_outer_query_scope();
    bind_result?;

    Ok(BoundUpsertDo::Update { sets, where_clause })
}

/// Bind an INSERT statement up front: validate the target table, resolve
/// defaults in VALUES rows, bind source and RETURNING expressions, and bind
/// every UPSERT conflict target and action.
#[turso_macros::trace_stack]
pub fn bind_insert_stmt(
    tbl_name: &ast::QualifiedName,
    columns: &[ast::Name],
    body: &mut ast::InsertBody,
    returning: &mut Vec<ast::ResultColumn>,
    with_for_returning: &mut Option<ast::With>,
    on_conflict: ast::ResolveType,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<BoundInsert> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(tbl_name)?;
    let table_name = &tbl_name.name;
    let table = match resolver.with_schema(database_id, |s| s.get_table(table_name.as_str())) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", table_name),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!("unsafe use of virtual table \"{}\"", tbl_name.name.as_str());
    }
    validate_insert(table_name.as_str(), resolver, connection)?;
    if table.virtual_table().is_some() {
        return Ok(BoundInsert {
            values: vec![],
            upsert_actions: vec![],
            inserting_multiple_rows: false,
            database_id,
            table,
            source_select: None,
            returning_cte_definitions: Vec::new(),
            returning_subquery_bindings: HashMap::default(),
            target_table_id: program.table_reference_counter.next(),
            excluded_table_id: program.table_reference_counter.next(),
            bound_index_expressions: Vec::new(),
        });
    }

    let target_table_id = program.table_reference_counter.next();
    let excluded_table_id = program.table_reference_counter.next();
    let value_types = insert_value_types(&table, columns, resolver)?;

    let mut values: Vec<Box<ast::Expr>> = vec![];
    let mut upsert: Option<Box<ast::Upsert>> = None;
    let mut upsert_actions: Vec<BoundUpsertAction> = Vec::new();
    let mut inserting_multiple_rows = false;
    match body {
        ast::InsertBody::DefaultValues => {
            // Generate default values for the table.
            // Check column-level default first, then type-level default.
            let is_strict = table.is_strict();
            values = table
                .columns()
                .iter()
                .filter(|c| !c.hidden() && !c.is_generated())
                .map(|c| {
                    c.default.clone().unwrap_or_else(|| {
                        if let Ok(Some(resolved)) =
                            resolver.schema().resolve_type(&c.ty_str, is_strict)
                        {
                            if let Some(default_expr) = resolved.default_expr() {
                                return Box::new(default_expr.clone());
                            }
                        }
                        Box::new(ast::Expr::Literal(ast::Literal::Null))
                    })
                })
                .collect();
        }
        ast::InsertBody::Select(select, upsert_opt) => {
            // Resolve Expr::Default in all VALUES rows before any compilation.
            if let ast::OneSelect::Values(values_expr) = &mut select.body.select {
                for row in values_expr.iter_mut() {
                    resolve_defaults_in_row(row, &table, columns, resolver);
                }
            }
            for compound in select.body.compounds.iter_mut() {
                if let ast::OneSelect::Values(values_expr) = &mut compound.select {
                    for row in values_expr.iter_mut() {
                        resolve_defaults_in_row(row, &table, columns, resolver);
                    }
                }
            }
            if select.body.compounds.is_empty() {
                match &mut select.body.select {
                    // TODO see how to avoid clone
                    ast::OneSelect::Values(values_expr) if values_expr.len() <= 1 => {
                        if values_expr.is_empty() {
                            crate::bail_parse_error!("no values to insert");
                        }
                        // Check if any VALUES expression contains a subquery.
                        // If so, route through multi-row path which handles subqueries.
                        let has_subquery = values_expr
                            .iter()
                            .any(|row| row.iter().any(|expr| expr_contains_subquery(expr)));
                        if has_subquery {
                            inserting_multiple_rows = true;
                        } else {
                            for expr in values_expr.iter_mut().flat_map(|v| v.iter_mut()) {
                                match expr.as_mut() {
                                    ast::Expr::Id(name) => {
                                        if name.quoted_with('"') && resolver.dqs_dml.is_enabled() {
                                            *expr = ast::Expr::Literal(ast::Literal::String(
                                                name.as_literal(),
                                            ))
                                            .into();
                                        } else {
                                            crate::bail_parse_error!("no such column: {name}");
                                        }
                                    }
                                    ast::Expr::Qualified(first_name, second_name) => {
                                        // an INSERT INTO ... VALUES (...) cannot reference columns
                                        crate::bail_parse_error!(
                                            "no such column: {first_name}.{second_name}"
                                        );
                                    }
                                    _ => {}
                                }
                            }
                            values = values_expr.pop().unwrap_or_else(Vec::new);
                        }
                    }
                    _ => inserting_multiple_rows = true,
                }
            } else {
                inserting_multiple_rows = true;
            }
            upsert = upsert_opt.take();
        }
    }
    if !values.is_empty() {
        let empty_scope = BindScope::empty();
        let function_binder = BindContext::new(resolver, program);
        for (index, expr) in values.iter_mut().enumerate() {
            bind_scopeless_expr(expr, resolver)?;
            function_binder.bind_custom_type_function_calls(
                expr,
                &empty_scope,
                value_types.get(index).and_then(Option::as_deref),
            )?;
        }
    }
    let source_select = if inserting_multiple_rows {
        let ast::InsertBody::Select(select, _) = body else {
            unreachable!("only INSERT SELECT can use the multi-row source path")
        };
        let mut binder = BindContext::new(resolver, program);
        Some(binder.bind_select_with_expected_types(select, &value_types)?)
    } else {
        None
    };
    if let ast::ResolveType::Ignore = on_conflict {
        program.set_resolve_type(ast::ResolveType::Ignore);
        upsert.replace(Box::new(ast::Upsert {
            do_clause: ast::UpsertDo::Nothing,
            index: None,
            next: None,
        }));
    } else {
        program.set_resolve_type(on_conflict);
    }
    let target_identifier =
        normalize_ident(tbl_name.alias.as_ref().unwrap_or(&tbl_name.name).as_str());
    while let Some(mut upsert_opt) = upsert.take() {
        let next = upsert_opt.next.take();
        let action_label = program.allocate_label();
        let mut binder = BindContext::new(resolver, program);
        bind_upsert_conflict_target(&mut binder, &mut upsert_opt, database_id, &table)?;
        let resolved_target = binder.resolver.with_schema(database_id, |schema| {
            super::upsert::resolve_upsert_target(schema, &table, &upsert_opt)
        })?;
        let bound_do = bind_upsert_do(
            &mut binder,
            upsert_opt.do_clause,
            target_identifier.clone(),
            target_table_id,
            excluded_table_id,
            database_id,
            &table,
        )?;
        if !binder.subquery_bindings.is_empty() {
            crate::bail_parse_error!("Subquery is not supported in this position");
        }
        upsert_actions.push((resolved_target, action_label, bound_do));
        upsert = next;
    }
    let (returning_cte_definitions, returning_subquery_bindings) = {
        let mut binder = BindContext::new(resolver, program);
        binder.bind_insert_returning(
            &tbl_name.name,
            target_table_id,
            returning,
            with_for_returning,
            database_id,
        )?
    };
    let bound_index_expressions =
        bind_table_index_expressions(resolver, database_id, table.get_name(), target_table_id);
    Ok(BoundInsert {
        values,
        upsert_actions,
        inserting_multiple_rows,
        database_id,
        table,
        source_select,
        returning_cte_definitions,
        returning_subquery_bindings,
        target_table_id,
        excluded_table_id,
        bound_index_expressions,
    })
}

/// Validate the INSERT target table.
fn validate_insert(
    table_name: &str,
    resolver: &Resolver,
    conn: &Arc<crate::Connection>,
) -> Result<()> {
    // Check if this is a system table that should be protected from direct writes
    if !conn.is_nested_stmt()
        && !conn.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(table_name)
    {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    // Check if this is a materialized view
    if resolver.schema().is_materialized_view(table_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", table_name);
    }
    // Check if this table has any incompatible dependent views
    resolver.schema().with_incompatible_dependent_views(table_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot DELETE from table '{table_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().fold(String::new(), |_, s| s.to_string() + ", "),
        );
    }
    Ok(())
    })
}

/// Resolve `Expr::Default` in a VALUES row by replacing it with the column's
/// default expression from the schema.
fn resolve_defaults_in_row(
    row: &mut [Box<ast::Expr>],
    table: &Table,
    columns: &[ast::Name],
    resolver: &Resolver,
) {
    let is_strict = table.is_strict();
    for (i, expr) in row.iter_mut().enumerate() {
        if !matches!(expr.as_ref(), ast::Expr::Default) {
            continue;
        }
        let col = if columns.is_empty() {
            // No column list — position maps to non-hidden columns in order
            table.columns().iter().filter(|c| !c.hidden()).nth(i)
        } else {
            // Column list — map by name
            columns.get(i).and_then(|name| {
                let name = crate::util::normalize_ident(name.as_str());
                table.get_column_by_name(&name).map(|(_, col)| col)
            })
        };
        *expr = match col {
            Some(col) => col.default.clone().unwrap_or_else(|| {
                if let Ok(Some(resolved)) = resolver.schema().resolve_type(&col.ty_str, is_strict) {
                    if let Some(default_expr) = resolved.default_expr() {
                        return Box::new(default_expr.clone());
                    }
                }
                Box::new(ast::Expr::Literal(ast::Literal::Null))
            }),
            None => Box::new(ast::Expr::Literal(ast::Literal::Null)),
        };
    }
}

/// Check if an expression contains a subquery (Subquery, InSelect, or Exists).
/// Used to detect when single-row VALUES should be routed through the
/// multi-row path, which has proper subquery handling.
fn expr_contains_subquery(expr: &ast::Expr) -> bool {
    let mut found_subquery = false;
    let _ = walk_expr(expr, &mut |e| {
        if matches!(
            e,
            ast::Expr::Subquery(_) | ast::Expr::InSelect { .. } | ast::Expr::Exists(_)
        ) {
            found_subquery = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found_subquery
}

/// Validate the UPDATE target and statement shape.
fn validate_update(
    schema: &crate::schema::Schema,
    body: &ast::Update,
    table_name: &str,
    is_internal_schema_change: bool,
    conn: &Arc<crate::Connection>,
) -> Result<()> {
    // Check if this is a system table that should be protected from direct writes
    if !is_internal_schema_change
        && !conn.is_nested_stmt()
        && !conn.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(table_name)
    {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    if !body.order_by.is_empty() {
        crate::bail_parse_error!("ORDER BY is not supported in UPDATE");
    }
    // Check if this is a materialized view
    if schema.is_materialized_view(table_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", table_name);
    }

    // Check if this table has any incompatible dependent views
    schema.with_incompatible_dependent_views(table_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot UPDATE table '{table_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().map(|view| view.as_str()).collect::<Vec<_>>().join(", "),
        );
    }
    Ok(())
    })
}

#[derive(Clone)]
struct OuterQueryFrame {
    scope: BindScopeRef,
    aliases: Arc<Vec<BoundColumn>>,
}

impl BoundSelect {
    /// Convert the bound scopes into `TableReferences` (one per SELECT core,
    /// main scope first). Outer query references are set on each scope's
    /// `TableReferences` so that column usage tracking for correlated columns
    /// can find their target tables.
    pub fn into_table_references_with_outer_refs(
        self,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
        planned_derived: &mut HashMap<ast::TableInternalId, super::plan::JoinedTable>,
        outer_query_refs: Vec<super::plan::OuterQueryReference>,
    ) -> Result<Vec<TableReferences>> {
        let mut all = Vec::with_capacity(1 + self.compound_scopes.len());

        // Compound scopes get the same outer refs as the main scope: a
        // correlated compound subquery (e.g. `x IN (... UNION ...)`) can
        // reference the outer scope from any of its constituent SELECTs.
        for scope in self.compound_scopes {
            all.push(Self::scope_to_table_references(
                scope,
                &self.tracking,
                planned_ctes,
                planned_derived,
                outer_query_refs.clone(),
            )?);
        }
        let main_refs = Self::scope_to_table_references(
            self.main_scope,
            &self.tracking,
            planned_ctes,
            planned_derived,
            outer_query_refs,
        )?;
        all.insert(0, main_refs);

        Ok(all)
    }

    fn scope_to_table_references(
        scope: BindScope,
        tracking: &BindTracking,
        planned_ctes: &mut HashMap<String, super::plan::JoinedTable>,
        planned_derived: &mut HashMap<ast::TableInternalId, super::plan::JoinedTable>,
        outer_query_refs: Vec<super::plan::OuterQueryReference>,
    ) -> Result<TableReferences> {
        let right_join_swapped = scope.right_join_swapped;
        let joined_tables = scope
            .tables
            .into_iter()
            .map(|scope_table| match scope_table.source {
                ScopeTableSource::Table(table) => Ok(super::plan::JoinedTable {
                    op: super::plan::Operation::default_scan_for(&table),
                    column_use_counts: vec![0; table.columns().len()],
                    table: (*table).clone(),
                    identifier: scope_table.identifier,
                    internal_id: scope_table.internal_id,
                    join_info: scope_table.join_info,
                    col_used_mask: Default::default(),
                    expression_index_usages: Vec::new(),
                    database_id: scope_table.database_id,
                    indexed: scope_table.indexed,
                    bound_index_method_patterns: scope_table.bound_index_method_patterns,
                    bound_index_expressions: scope_table.bound_index_expressions,
                }),
                ScopeTableSource::Cte { name, .. } => {
                    // Clone rather than remove: the same CTE may be referenced
                    // multiple times (e.g. FROM cte t1 JOIN cte t2).
                    let mut cte_table = planned_ctes.get(&name).cloned().ok_or_else(|| {
                        crate::LimboError::InternalError(format!(
                            "CTE '{name}' was not planned before into_table_references"
                        ))
                    })?;
                    cte_table.identifier = scope_table.identifier;
                    cte_table.internal_id = scope_table.internal_id;
                    cte_table.join_info = scope_table.join_info;
                    // CTE's FromClauseSubquery.name is already set to the CTE
                    // definition name during plan_bound_ctes — don't overwrite
                    // with the alias.
                    Ok(cte_table)
                }
                ScopeTableSource::Derived { .. } => {
                    let mut derived_table = planned_derived
                        .remove(&scope_table.internal_id)
                        .ok_or_else(|| {
                            crate::LimboError::InternalError(format!(
                                "derived table '{}' was not planned before into_table_references",
                                scope_table.identifier
                            ))
                        })?;
                    derived_table.identifier = scope_table.identifier.clone();
                    derived_table.internal_id = scope_table.internal_id;
                    derived_table.join_info = scope_table.join_info;
                    // Also update the inner FromClauseSubquery name so that
                    // index_seek_affinities can match the table by name.
                    if let Table::FromClauseSubquery(subq) = &mut derived_table.table {
                        Arc::make_mut(subq).name = scope_table.identifier;
                    }
                    Ok(derived_table)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let mut table_references = TableReferences::new(joined_tables, outer_query_refs);
        if right_join_swapped {
            // The planner's select_star consults this to restore the original
            // column order when RIGHT JOIN planning swapped the tables.
            table_references.set_right_join_swapped();
        }
        tracking.flush(&mut table_references);
        Ok(table_references)
    }
}

// ── BindTracking ─────────────────────────────────────────────────────────

/// Records what was accessed during binding.
///
/// Applied to `TableReferences` in a single flush after binding completes.
#[derive(Debug, Default)]
pub struct BindTracking {
    /// `(table_id, column_index)` pairs for columns referenced in the current scope.
    pub columns_used: Vec<(TableInternalId, usize)>,
    /// Tables whose rowid was referenced.
    pub rowids_used: Vec<TableInternalId>,
    /// `(table_id, column_index)` pairs for columns referenced from outer scopes.
    pub outer_refs_used: Vec<(TableInternalId, usize)>,
}

impl BindTracking {
    pub fn record_column(&mut self, table_id: TableInternalId, col_idx: usize) {
        self.columns_used.push((table_id, col_idx));
    }

    pub fn record_rowid(&mut self, table_id: TableInternalId) {
        self.rowids_used.push(table_id);
    }

    pub fn record_outer_ref(&mut self, table_id: TableInternalId, col_idx: usize) {
        self.outer_refs_used.push((table_id, col_idx));
    }

    /// Apply recorded usage back to `TableReferences`.
    ///
    /// Tracking spans the whole statement (main scope, compound scopes,
    /// subquery scopes), but each `TableReferences` only holds one scope's
    /// tables — usages recorded for other scopes are skipped here and flushed
    /// when their own scope is converted.
    pub fn flush(&self, table_references: &mut TableReferences) {
        let contains = |tr: &TableReferences, id: TableInternalId| {
            tr.find_joined_table_by_internal_id(id).is_some()
                || tr.find_outer_query_ref_by_internal_id(id).is_some()
        };
        for &(table_id, col_idx) in &self.columns_used {
            if contains(table_references, table_id) {
                table_references.mark_column_used(table_id, col_idx);
            }
        }
        for &table_id in &self.rowids_used {
            table_references.mark_rowid_referenced(table_id);
        }
        for &(table_id, col_idx) in &self.outer_refs_used {
            if contains(table_references, table_id) {
                table_references.mark_column_used(table_id, col_idx);
            }
        }
    }
}

// ── CteEntry ─────────────────────────────────────────────────────────────

/// A CTE definition stored in the binding context.
///
/// `Clone` copies metadata (name, columns, IDs) but sets `inner_bound` to `None`.
/// This is intentional: `with_query` clones CTEs for subquery scoping, where only
/// the column/name info is needed for resolution, not the full binding output.
pub struct CteEntry {
    /// The bound AST (column refs resolved).
    pub select: ast::Select,
    /// Explicit column names from `WITH t(a, b) AS (...)`.
    pub explicit_columns: Vec<String>,
    /// Globally unique CTE identity for materialization tracking.
    pub cte_id: usize,
    /// Result column names, populated after binding the CTE body.
    /// If explicit_columns is non-empty, equals explicit_columns.
    /// Otherwise, extracted from the SELECT result columns.
    pub resolved_columns: Vec<String>,
    /// Number of result columns produced by the CTE body's SELECT.
    /// Used to validate explicit column names when the CTE is referenced
    /// (SQLite defers this check until an actual reference).
    pub result_column_count: usize,
    /// Inner binding results (scopes, tracking, subquery bindings).
    pub inner_bound: Option<BoundSelect>,
    /// Indexes of CTEs (in definition order) that this CTE directly references.
    pub referenced_cte_indices: SmallVec<[usize; 2]>,
    /// True if `AS MATERIALIZED` was specified, forcing materialization.
    pub materialize_hint: bool,
    /// True if the body references its own name (a recursive CTE, whether or
    /// not the RECURSIVE keyword was written). `select` keeps the raw body
    /// (the planner reads compound operators and LIMIT from it); bound arms
    /// and resolved queue ordering live in `recursive_binding`.
    pub recursive: bool,
    /// Bound arms of a recursive CTE body (`recursive` is true), consumed by
    /// the recursive-CTE planner. `None` for non-recursive entries.
    pub recursive_binding: Option<RecursiveCteBinding>,
    /// Binding error deferred until the CTE is referenced. SQLite never
    /// resolves the body of an unused CTE, so errors in unused bodies (bad
    /// columns, circular references) must not surface eagerly.
    pub bind_error: Option<String>,
}

impl Clone for CteEntry {
    fn clone(&self) -> Self {
        Self {
            select: self.select.clone(),
            explicit_columns: self.explicit_columns.clone(),
            cte_id: self.cte_id,
            resolved_columns: self.resolved_columns.clone(),
            result_column_count: self.result_column_count,
            inner_bound: None,
            referenced_cte_indices: self.referenced_cte_indices.clone(),
            materialize_hint: self.materialize_hint,
            recursive: self.recursive,
            recursive_binding: None,
            bind_error: self.bind_error.clone(),
        }
    }
}

// ── BindContext ───────────────────────────────────────────────────────────

/// Scope-aware binding context, analogous to DataFusion's `PlannerContext`.
///
/// Manages the outer-scope stack for correlated subquery resolution,
/// CTE definitions, SELECT aliases, and binding phase tracking.
///
/// Does **not** borrow `TableReferences`. Column usage is recorded in
/// [`BindTracking`] and flushed back after binding completes.
pub struct BindContext<'a, G: IdGenerator> {
    /// Function and schema resolver.
    pub resolver: &'a Resolver<'a>,

    /// Generates unique table IDs for scope tables.
    id_gen: &'a mut G,

    /// Stack of outer query scopes plus visible aliases.
    outer_query_frames: Vec<OuterQueryFrame>,

    /// Index into `outer_query_frames` below which frames are invisible to
    /// column resolution. GROUP BY expressions cannot reference the outer
    /// query scope (SQLite rejects them with "no such column"), but the
    /// frames must stay physically present so error messages can still name
    /// the outer table (`no such column: t2.d`, not `no such table: t2`).
    outer_frame_floor: usize,

    /// Outer FROM schema for LATERAL join support.
    outer_from_scope: Option<BindScopeRef>,

    /// CTE definitions visible in the current query.
    ctes: HashMap<String, CteEntry>,

    /// `(cte_id, name)` of CTEs whose bodies are currently being bound. A
    /// reference to one of these from inside another CTE body is a circular
    /// reference. Tracked by id so a shadowing nested CTE with the same name
    /// is not mistaken for the in-flight one.
    ctes_being_bound: Vec<(usize, String)>,

    /// SELECT result columns for alias resolution in later phases.
    /// Populated after binding the SELECT list. Arc-shared so pushing a
    /// subquery frame or switching phases never deep-clones the column
    /// expressions (that cost scales with SELECT-list width).
    aliases: Arc<Vec<BoundColumn>>,

    /// Current binding phase — controls alias visibility.
    phase: BindPhase,

    /// When true, unresolved identifiers are left as-is instead of erroring.
    /// Used for UPSERT DO UPDATE SET/WHERE where `EXCLUDED.col` can't be
    /// resolved at bind time.
    allow_unbound: bool,

    /// Records column/rowid usage for post-binding flush.
    pub tracking: BindTracking,

    /// Expression subqueries bound during this query, keyed by subquery_id.
    /// Moved into `BoundSelect` when binding completes.
    subquery_bindings: HashMap<ast::TableInternalId, BoundSubquery>,

    /// Scalar subqueries shared between GROUP BY and the SELECT list of the
    /// current SELECT core. Each entry holds the raw (pre-bind) expression and
    /// the subquery id assigned when the first occurrence is bound; later
    /// occurrences are rewritten to the same id so they share one evaluation
    /// (and compare equal for GROUP BY expression matching), mirroring the
    /// planner's scalar-subquery CSE.
    shared_subqueries: Vec<(ast::Expr, Option<ast::TableInternalId>)>,

    /// FROM-clause subqueries (derived tables) bound during this query,
    /// keyed by the scope table's `internal_id`.
    derived_bindings: HashMap<ast::TableInternalId, BoundSubquery>,

    /// Set while binding a recursive CTE's recursive arms: the CTE's own name
    /// resolves to the recursive input table instead of raising a circular
    /// reference.
    recursive_self: Option<RecursiveSelfRef>,

    /// NEW/OLD values visible while binding a trigger WHEN clause.
    trigger_columns: Option<TriggerColumnBindings>,
}

struct TriggerColumnBindings {
    table: Arc<BTreeTable>,
    new_registers: Option<Vec<usize>>,
    old_registers: Option<Vec<usize>>,
}

#[derive(Debug)]
struct TriggerParameterAllocator {
    new_columns: Vec<Option<NonZero<usize>>>,
    new_rowid: Option<NonZero<usize>>,
    old_columns: Vec<Option<NonZero<usize>>>,
    old_rowid: Option<NonZero<usize>>,
    next_parameter: usize,
}

impl TriggerParameterAllocator {
    fn new(column_count: usize, has_new: bool, has_old: bool) -> Self {
        Self {
            new_columns: if has_new {
                vec![None; column_count]
            } else {
                Vec::new()
            },
            new_rowid: None,
            old_columns: if has_old {
                vec![None; column_count]
            } else {
                Vec::new()
            },
            old_rowid: None,
            next_parameter: 1,
        }
    }

    fn allocate(slot: &mut Option<NonZero<usize>>, next_parameter: &mut usize) -> NonZero<usize> {
        *slot.get_or_insert_with(|| {
            let parameter = NonZero::new(*next_parameter).expect("parameter indices start at one");
            *next_parameter += 1;
            parameter
        })
    }

    fn new_column(&mut self, index: usize) -> NonZero<usize> {
        Self::allocate(&mut self.new_columns[index], &mut self.next_parameter)
    }

    fn new_rowid(&mut self) -> NonZero<usize> {
        Self::allocate(&mut self.new_rowid, &mut self.next_parameter)
    }

    fn old_column(&mut self, index: usize) -> NonZero<usize> {
        Self::allocate(&mut self.old_columns[index], &mut self.next_parameter)
    }

    fn old_rowid(&mut self) -> NonZero<usize> {
        Self::allocate(&mut self.old_rowid, &mut self.next_parameter)
    }

    fn count(&self) -> usize {
        self.next_parameter - 1
    }
}

/// Binds trigger-body NEW/OLD references to subprogram parameters.
pub struct TriggerProgramBinder {
    parameters: RefCell<TriggerParameterAllocator>,
    has_new: bool,
    has_old: bool,
    table: Arc<BTreeTable>,
    override_conflict: Option<ast::ResolveType>,
    database_name: Option<ast::Name>,
}

impl TriggerProgramBinder {
    pub fn new(
        table: Arc<BTreeTable>,
        has_new: bool,
        has_old: bool,
        override_conflict: Option<ast::ResolveType>,
        database_name: Option<ast::Name>,
    ) -> Self {
        Self {
            parameters: RefCell::new(TriggerParameterAllocator::new(
                table.columns().len(),
                has_new,
                has_old,
            )),
            has_new,
            has_old,
            table,
            override_conflict,
            database_name,
        }
    }

    pub fn bind_command(&self, command: &ast::TriggerCmd) -> Result<ast::Stmt> {
        bind_trigger_command(command, self)
    }

    /// Map each allocated subprogram parameter back to its parent row register.
    pub fn parameter_registers(
        &self,
        new_registers: Option<&[usize]>,
        old_registers: Option<&[usize]>,
    ) -> Vec<usize> {
        let parameters = self.parameters.borrow();
        let mut registers = vec![0; parameters.count()];

        if let Some(new_registers) = new_registers {
            for (column, parameter) in parameters.new_columns.iter().enumerate() {
                if let Some(parameter) = parameter {
                    registers[parameter.get() - 1] = new_registers[column];
                }
            }
            if let Some(parameter) = parameters.new_rowid {
                registers[parameter.get() - 1] = *new_registers
                    .last()
                    .expect("NEW registers include the rowid");
            }
        }

        if let Some(old_registers) = old_registers {
            for (column, parameter) in parameters.old_columns.iter().enumerate() {
                if let Some(parameter) = parameter {
                    registers[parameter.get() - 1] = old_registers[column];
                }
            }
            if let Some(parameter) = parameters.old_rowid {
                registers[parameter.get() - 1] = *old_registers
                    .last()
                    .expect("OLD registers include the rowid");
            }
        }

        registers
    }

    fn new_column_parameter(&self, index: usize) -> Option<NonZero<usize>> {
        self.has_new
            .then(|| self.parameters.borrow_mut().new_column(index))
    }

    fn new_rowid_parameter(&self) -> Option<NonZero<usize>> {
        self.has_new
            .then(|| self.parameters.borrow_mut().new_rowid())
    }

    fn old_column_parameter(&self, index: usize) -> Option<NonZero<usize>> {
        self.has_old
            .then(|| self.parameters.borrow_mut().old_column(index))
    }

    fn old_rowid_parameter(&self) -> Option<NonZero<usize>> {
        self.has_old
            .then(|| self.parameters.borrow_mut().old_rowid())
    }
}

fn trigger_parameter_expr(index: NonZero<usize>, column_type: Option<&str>) -> ast::Expr {
    let index = u32::try_from(index.get())
        .ok()
        .and_then(std::num::NonZeroU32::new)
        .expect("trigger parameter index must fit into NonZeroU32");
    match column_type {
        Some(column_type) => ast::Expr::Variable(ast::Variable::indexed_typed(index, column_type)),
        None => ast::Expr::Variable(ast::Variable::indexed(index)),
    }
}

fn bind_trigger_expression(expr: &mut ast::Expr, binder: &TriggerProgramBinder) -> Result<()> {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        bind_trigger_expression_node(expr, binder)?;
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn bind_trigger_expression_node(expr: &mut ast::Expr, binder: &TriggerProgramBinder) -> Result<()> {
    match expr {
        ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
            bind_trigger_select_expressions(select, binder)?;
        }
        ast::Expr::InSelect { rhs, .. } => {
            bind_trigger_select_expressions(rhs, binder)?;
        }
        ast::Expr::Qualified(namespace, column)
        | ast::Expr::DoublyQualified(_, namespace, column) => {
            let namespace = normalize_ident(namespace.as_str());
            let column = normalize_ident(column.as_str());
            let column_definition = binder.table.get_column(&column);
            let is_rowid = super::planner::ROWID_STRS
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column));

            let (parameter, column_type) = if namespace.eq_ignore_ascii_case("new") {
                if !binder.has_new {
                    crate::bail_parse_error!(
                        "NEW references are only valid in INSERT and UPDATE triggers"
                    );
                }
                if let Some((index, definition)) = column_definition {
                    let parameter = if definition.is_rowid_alias() {
                        binder.new_rowid_parameter()
                    } else {
                        binder.new_column_parameter(index)
                    };
                    (parameter, Some(definition.ty_str.as_str()))
                } else if is_rowid {
                    (binder.new_rowid_parameter(), None)
                } else {
                    crate::bail_parse_error!("no such column: {}.{}", namespace, column);
                }
            } else if namespace.eq_ignore_ascii_case("old") {
                if !binder.has_old {
                    crate::bail_parse_error!(
                        "OLD references are only valid in UPDATE and DELETE triggers"
                    );
                }
                if let Some((index, definition)) = column_definition {
                    let parameter = if definition.is_rowid_alias() {
                        binder.old_rowid_parameter()
                    } else {
                        binder.old_column_parameter(index)
                    };
                    (parameter, Some(definition.ty_str.as_str()))
                } else if is_rowid {
                    (binder.old_rowid_parameter(), None)
                } else {
                    crate::bail_parse_error!("no such column: {}.{}", namespace, column);
                }
            } else {
                return Ok(());
            };

            *expr = trigger_parameter_expr(
                parameter.expect("trigger row parameters must be available"),
                column_type,
            );
        }
        _ => {}
    }
    Ok(())
}

fn bind_trigger_upsert(
    upsert: &mut Option<Box<ast::Upsert>>,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    let mut current = upsert.as_mut();
    while let Some(upsert) = current {
        if let ast::UpsertDo::Set { sets, where_clause } = &mut upsert.do_clause {
            for set in sets {
                bind_trigger_expression(&mut set.expr, binder)?;
            }
            if let Some(where_clause) = where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
        }
        if let Some(index) = &mut upsert.index {
            if let Some(where_clause) = &mut index.where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
        }
        current = upsert.next.as_mut();
    }
    Ok(())
}

fn bind_trigger_command(
    command: &ast::TriggerCmd,
    binder: &TriggerProgramBinder,
) -> Result<ast::Stmt> {
    match command {
        ast::TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        } => {
            let mut select = select.clone();
            bind_trigger_select_expressions(&mut select, binder)?;
            let mut upsert = upsert.clone();
            bind_trigger_upsert(&mut upsert, binder)?;
            Ok(ast::Stmt::Insert {
                with: None,
                or_conflict: binder.override_conflict.or(*or_conflict),
                tbl_name: ast::QualifiedName {
                    db_name: binder.database_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                columns: col_names.clone(),
                body: ast::InsertBody::Select(select, upsert),
                returning: returning.clone(),
            })
        }
        ast::TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
        } => {
            let mut sets = sets.clone();
            for set in &mut sets {
                bind_trigger_expression(&mut set.expr, binder)?;
            }
            let mut from = from.clone();
            if let Some(from) = &mut from {
                bind_trigger_from_expressions(from, binder)?;
            }
            let mut where_clause = where_clause.clone();
            if let Some(where_clause) = &mut where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
            Ok(ast::Stmt::Update(ast::Update {
                with: None,
                or_conflict: binder.override_conflict.or(*or_conflict),
                tbl_name: ast::QualifiedName {
                    db_name: binder.database_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                indexed: None,
                sets,
                from,
                where_clause,
                returning: Vec::new(),
                order_by: Vec::new(),
                limit: None,
            }))
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            let mut where_clause = where_clause.clone();
            if let Some(where_clause) = &mut where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
            Ok(ast::Stmt::Delete {
                tbl_name: ast::QualifiedName {
                    db_name: binder.database_name.clone(),
                    name: tbl_name.clone(),
                    alias: None,
                },
                where_clause,
                limit: None,
                returning: Vec::new(),
                indexed: None,
                order_by: Vec::new(),
                with: None,
            })
        }
        ast::TriggerCmd::Select(select) => {
            let mut select = select.clone();
            bind_trigger_select_expressions(&mut select, binder)?;
            Ok(ast::Stmt::Select(select))
        }
    }
}

fn bind_trigger_select_expressions(
    select: &mut ast::Select,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            bind_trigger_select_expressions(&mut cte.select, binder)?;
        }
    }
    bind_trigger_one_select_expressions(&mut select.body.select, binder)?;
    for compound in &mut select.body.compounds {
        bind_trigger_one_select_expressions(&mut compound.select, binder)?;
    }
    for order_by in &mut select.order_by {
        bind_trigger_expression(&mut order_by.expr, binder)?;
    }
    if let Some(limit) = &mut select.limit {
        bind_trigger_expression(&mut limit.expr, binder)?;
        if let Some(offset) = &mut limit.offset {
            bind_trigger_expression(offset, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_one_select_expressions(
    select: &mut ast::OneSelect,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    match select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    bind_trigger_expression(expr, binder)?;
                }
            }
            if let Some(from) = from {
                bind_trigger_from_expressions(from, binder)?;
            }
            if let Some(where_clause) = where_clause {
                bind_trigger_expression(where_clause, binder)?;
            }
            if let Some(group_by) = group_by {
                for expr in &mut group_by.exprs {
                    bind_trigger_expression(expr, binder)?;
                }
                if let Some(having) = &mut group_by.having {
                    bind_trigger_expression(having, binder)?;
                }
            }
            for window in window_clause {
                bind_trigger_window_expressions(&mut window.window, binder)?;
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_trigger_expression(expr, binder)?;
                }
            }
        }
    }
    Ok(())
}

fn bind_trigger_from_expressions(
    from: &mut ast::FromClause,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    bind_trigger_select_table_expressions(&mut from.select, binder)?;
    for join in &mut from.joins {
        bind_trigger_select_table_expressions(&mut join.table, binder)?;
        if let Some(ast::JoinConstraint::On(expr)) = &mut join.constraint {
            bind_trigger_expression(expr, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_select_table_expressions(
    table: &mut ast::SelectTable,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(..) => {}
        ast::SelectTable::TableCall(_, arguments, _) => {
            for argument in arguments {
                bind_trigger_expression(argument, binder)?;
            }
        }
        ast::SelectTable::Select(select, _) => {
            bind_trigger_select_expressions(select, binder)?;
        }
        ast::SelectTable::Sub(from, _) => {
            bind_trigger_from_expressions(from, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_window_expressions(
    window: &mut ast::Window,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    for expr in &mut window.partition_by {
        bind_trigger_expression(expr, binder)?;
    }
    for order_by in &mut window.order_by {
        bind_trigger_expression(&mut order_by.expr, binder)?;
    }
    if let Some(frame) = &mut window.frame_clause {
        bind_trigger_frame_bound(&mut frame.start, binder)?;
        if let Some(end) = &mut frame.end {
            bind_trigger_frame_bound(end, binder)?;
        }
    }
    Ok(())
}

fn bind_trigger_frame_bound(
    bound: &mut ast::FrameBound,
    binder: &TriggerProgramBinder,
) -> Result<()> {
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            bind_trigger_expression(expr, binder)
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => Ok(()),
    }
}

impl<'a, G: IdGenerator> BindContext<'a, G> {
    pub fn new(resolver: &'a Resolver<'a>, id_gen: &'a mut G) -> Self {
        Self {
            resolver,
            id_gen,
            outer_query_frames: Vec::new(),
            outer_frame_floor: 0,
            outer_from_scope: None,
            ctes: HashMap::default(),
            ctes_being_bound: Vec::new(),
            aliases: Arc::new(Vec::new()),
            phase: BindPhase::NoAliases,
            allow_unbound: false,
            tracking: BindTracking::default(),
            subquery_bindings: HashMap::default(),
            shared_subqueries: Vec::new(),
            derived_bindings: HashMap::default(),
            recursive_self: None,
            trigger_columns: None,
        }
    }

    // ── Outer scope stack (mirrors DataFusion PlannerContext) ─────────

    /// Push a scope onto the outer-scope stack (entering a subquery).
    fn append_outer_query_scope(&mut self, scope: BindScopeRef, aliases: Arc<Vec<BoundColumn>>) {
        self.outer_query_frames
            .push(OuterQueryFrame { scope, aliases });
    }

    /// Pop the most recent outer scope (exiting a subquery).
    fn pop_outer_query_scope(&mut self) -> Option<OuterQueryFrame> {
        self.outer_query_frames.pop()
    }

    /// Iterate outer scopes innermost-first (reversed storage order).
    /// Matches column lookup precedence: nearest enclosing query first.
    /// Frames below `outer_frame_floor` are hidden (see the field docs).
    fn outer_scopes_iter(&self) -> impl Iterator<Item = &BindScopeRef> {
        self.outer_query_frames[self.outer_frame_floor..]
            .iter()
            .rev()
            .map(|frame| &frame.scope)
    }

    fn outer_query_frames_iter(&self) -> impl Iterator<Item = &OuterQueryFrame> {
        self.outer_query_frames[self.outer_frame_floor..]
            .iter()
            .rev()
    }

    /// Iterate ALL outer scopes, including frames hidden by
    /// `outer_frame_floor`. Only for error reporting (naming a table that
    /// exists but is not referenceable from the current clause).
    fn all_outer_scopes_iter(&self) -> impl Iterator<Item = &BindScopeRef> {
        self.outer_query_frames
            .iter()
            .rev()
            .map(|frame| &frame.scope)
    }

    // ── CTEs ─────────────────────────────────────────────────────────

    pub fn insert_cte(&mut self, name: String, entry: CteEntry) {
        self.ctes.insert(name, entry);
    }

    #[cfg(test)]
    fn get_cte(&self, name: &str) -> Option<&CteEntry> {
        self.ctes.get(name)
    }

    // ── Phase and aliases ────────────────────────────────────────────

    fn phase(&self) -> BindPhase {
        self.phase
    }

    fn set_aliases(&mut self, aliases: Arc<Vec<BoundColumn>>) {
        self.aliases = aliases;
    }

    fn aliases(&self) -> &[BoundColumn] {
        &self.aliases
    }

    /// Run `f` with a fresh per-select-core state (phase, aliases).
    /// Saves and restores on exit so individual SELECT cores in the same
    /// compound query do not clobber each other.
    fn with_scope<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let saved_aliases = std::mem::take(&mut self.aliases);
        let saved_phase = self.phase;
        let saved_shared = std::mem::take(&mut self.shared_subqueries);

        let result = f(self);

        self.aliases = saved_aliases;
        self.phase = saved_phase;
        self.shared_subqueries = saved_shared;

        result
    }

    /// Run `f` with a fresh query state, restoring CTE/alias/phase state on exit.
    ///
    /// This mirrors DataFusion's per-query PlannerContext cloning semantics:
    /// subqueries inherit outer CTEs, but their own WITH items remain private.
    fn with_query<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        // Swap out current CTEs (preserving inner_bound) and give the inner
        // query a clone (inner_bound = None is fine — inner queries don't plan
        // outer CTEs, they only need names/columns for resolution).
        let mut saved_ctes = self.ctes.clone(); // clone for inner query
        std::mem::swap(&mut self.ctes, &mut saved_ctes); // saved_ctes now has originals
        let saved_aliases = std::mem::take(&mut self.aliases);
        let saved_phase = self.phase;
        let saved_outer_from_scope = self.outer_from_scope.clone();
        let saved_tracking = std::mem::take(&mut self.tracking);
        let saved_floor = self.outer_frame_floor;
        let saved_subquery_bindings = std::mem::take(&mut self.subquery_bindings);
        let saved_derived_bindings = std::mem::take(&mut self.derived_bindings);

        let result = f(self);

        self.ctes = saved_ctes;
        self.aliases = saved_aliases;
        self.phase = saved_phase;
        self.outer_from_scope = saved_outer_from_scope;
        self.tracking = saved_tracking;
        self.outer_frame_floor = saved_floor;
        self.subquery_bindings = saved_subquery_bindings;
        self.derived_bindings = saved_derived_bindings;

        result
    }

    /// Run `f` with a temporary phase, restoring the previous phase on exit.
    fn with_phase<T>(
        &mut self,
        phase: BindPhase,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let saved = self.phase;
        self.phase = phase;
        let result = f(self);
        self.phase = saved;
        result
    }

    /// Extract result columns from a SELECT list before the main bind pass.
    ///
    /// Captures the name and a bound expression for each result column.
    /// For identifiers and star expansions, the expression is resolved
    /// to `Expr::Column` immediately. For complex expressions, the
    /// original AST is cloned and bound via `bind_expr`.
    ///
    /// Must be called before `bind_select_list` rewrites the AST in-place,
    /// since we need the raw identifiers to infer column names.
    fn extract_bound_columns(
        &mut self,
        columns: &mut [ast::ResultColumn],
        scope: &BindScope,
        expected_types: &[Option<Arc<TypeDef>>],
    ) -> Result<Vec<BoundColumn>> {
        let mut result = Vec::with_capacity(columns.len());
        let mut output_index = 0;
        for col in columns {
            match col {
                ast::ResultColumn::Expr(expr, alias) => {
                    // Determine the column name. Implicit column names (the
                    // original SQL text preserved by the parser for unaliased
                    // expressions) are only a naming fallback, not an alias.
                    let explicit_alias = alias.as_ref().filter(|a| a.is_explicit());
                    let name = if let Some(a) = explicit_alias {
                        normalize_ident(a.name().as_str())
                    } else {
                        let inferred = match expr.as_ref() {
                            ast::Expr::Id(id) => normalize_ident(id.as_str()),
                            ast::Expr::Qualified(_, id) => normalize_ident(id.as_str()),
                            ast::Expr::DoublyQualified(_, _, id) => normalize_ident(id.as_str()),
                            // After star expansion, columns are Expr::Column with
                            // table internal_id and column index. Look up the name
                            // from the scope.
                            ast::Expr::Column {
                                table: table_id,
                                column: col_idx,
                                ..
                            } => scope
                                .tables
                                .iter()
                                .find(|st| st.internal_id == *table_id)
                                .and_then(|st| st.table.column_name(*col_idx))
                                .map(|n| n.to_string())
                                .unwrap_or_default(),
                            // Complex expressions without an alias can't be
                            // referenced by name from outer queries.
                            _ => String::new(),
                        };
                        if inferred.is_empty() {
                            // Fall back to the implicit column name (original
                            // expression text), matching ResultSetColumn::name.
                            alias
                                .as_ref()
                                .map(|a| a.name().as_str().to_string())
                                .unwrap_or_default()
                        } else {
                            inferred
                        }
                    };
                    let is_explicit_alias = explicit_alias.is_some();
                    // Resolve the expression
                    self.bind_expr_with_expected_type(
                        expr,
                        scope,
                        expected_types.get(output_index).and_then(Option::as_deref),
                    )?;
                    result.push(BoundColumn {
                        name,
                        expr: expr.as_ref().clone(),
                        is_explicit_alias,
                    });
                    output_index += 1;
                }
                ast::ResultColumn::Star => {
                    // The star stays unexpanded in the AST (the planner's
                    // `select_star` produces the plan columns), but its names
                    // and arity are needed here for alias resolution and
                    // compound-select checks. Mirror `select_star`'s
                    // visibility rules exactly: ordering under RIGHT JOIN
                    // swapping, semi/anti-join exclusion, ambiguity on
                    // duplicate identifiers, hidden columns, USING dedup.
                    let table_iter: Vec<&ScopeTable> = if scope.right_join_swapped {
                        scope.tables.iter().rev().collect()
                    } else {
                        scope.tables.iter().collect()
                    };
                    for st in table_iter {
                        if st.join_info.as_ref().is_some_and(|ji| ji.is_semi_or_anti()) {
                            continue;
                        }
                        // If this table's identifier appears more than once in
                        // the FROM clause, expanding * would produce ambiguous
                        // column references (matches SQLite). Columns
                        // deduplicated by USING/NATURAL are not ambiguous.
                        let has_duplicate_identifier = scope
                            .tables
                            .iter()
                            .filter(|t| t.identifier == st.identifier)
                            .count()
                            > 1;
                        if has_duplicate_identifier {
                            let using_cols: Vec<&str> = scope
                                .tables
                                .iter()
                                .filter(|t| t.identifier == st.identifier)
                                .filter_map(|t| t.join_info.as_ref())
                                .flat_map(|ji| ji.using.iter().map(|u| u.as_str()))
                                .collect();
                            for col_ref in st.table.columns() {
                                if col_ref.is_hidden {
                                    continue;
                                }
                                let in_using = using_cols
                                    .iter()
                                    .any(|u| u.eq_ignore_ascii_case(col_ref.name));
                                if !in_using {
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}.{}",
                                        st.identifier,
                                        col_ref.name
                                    );
                                }
                            }
                        }
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            // USING dedup: skip right-table columns named in USING
                            if let Some(ji) = &st.join_info {
                                if ji
                                    .using
                                    .iter()
                                    .any(|u| u.as_str().eq_ignore_ascii_case(col_ref.name))
                                {
                                    continue;
                                }
                            }
                            result.push(BoundColumn {
                                name: col_ref.name.to_string(),
                                expr: ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                },
                                is_explicit_alias: false,
                            });
                            output_index += 1;
                        }
                    }
                }
                ast::ResultColumn::TableStar(table_name) => {
                    let Some(st) = scope.find_table_by_identifier(table_name.as_str()) else {
                        crate::bail_parse_error!("no such table: {}", table_name);
                    };
                    for col_ref in st.table.columns() {
                        if col_ref.is_hidden {
                            continue;
                        }
                        result.push(BoundColumn {
                            name: col_ref.name.to_string(),
                            expr: ast::Expr::Column {
                                database: None,
                                table: st.internal_id,
                                column: col_ref.idx,
                                is_rowid_alias: col_ref.is_rowid_alias,
                            },
                            is_explicit_alias: false,
                        });
                        output_index += 1;
                    }
                }
            }
        }
        Ok(result)
    }

    /// Bind a SELECT statement, resolving all name references in-place.
    /// Returns the bound query result needed by planning.
    pub fn bind_select(&mut self, select: &mut ast::Select) -> Result<BoundSelect> {
        self.bind_select_with_expected_types(select, &[])
    }

    /// Bind a SELECT used as an INSERT source. Each result expression receives
    /// the type of the destination column at the same position.
    fn bind_select_with_expected_types(
        &mut self,
        select: &mut ast::Select,
        expected_types: &[Option<Arc<TypeDef>>],
    ) -> Result<BoundSelect> {
        self.with_query(|ctx| {
            // 1. Bind CTEs from WITH clause
            if let Some(with) = &mut select.with {
                ctx.bind_cte(with)?;
            }

            // 2. Bind the main OneSelect. Its aliases and FROM scope are the ones
            // visible to the query-level ORDER BY.
            let (result_columns, main_scope) =
                ctx.bind_one_select(&mut select.body.select, expected_types)?;

            // 3. Bind compound selects (UNION, INTERSECT, EXCEPT)
            let mut compound_scopes = Vec::with_capacity(select.body.compounds.len());
            let mut compound_result_columns = Vec::with_capacity(select.body.compounds.len());
            for compound in &mut select.body.compounds {
                let (compound_columns, compound_scope) =
                    ctx.bind_one_select(&mut compound.select, expected_types)?;
                compound_result_columns.push(compound_columns);
                compound_scopes.push(compound_scope);
            }

            // 4. Bind ORDER BY (AliasFirst phase — aliases take priority).
            ctx.set_aliases(Arc::clone(&result_columns));
            let compound_order_by = if select.body.compounds.is_empty() {
                ctx.with_phase(BindPhase::AliasFirst, |ctx| {
                    for sort_col in &mut select.order_by {
                        ctx.replace_column_number(&mut sort_col.expr)?;
                        // Optimize trivial subqueries like (SELECT alias) by inlining
                        // the alias expression. This avoids creating a "correlated"
                        // subquery that's really just an alias reference.
                        ctx.try_inline_trivial_subquery(&mut sort_col.expr, &main_scope);
                        ctx.bind_expr(&mut sort_col.expr, &main_scope)?;
                    }
                    Ok(())
                })?;
                None
            } else {
                let right_most_count = compound_result_columns
                    .last()
                    .expect("compound SELECT must have a right-most arm")
                    .len();
                let mut left_counts = std::iter::once(result_columns.len())
                    .chain(
                        compound_result_columns[..compound_result_columns.len() - 1]
                            .iter()
                            .map(|columns| columns.len()),
                    )
                    .zip(select.body.compounds.iter().map(|compound| compound.operator));
                if let Some((_, operator)) =
                    left_counts.find(|(column_count, _)| *column_count != right_most_count)
                {
                    crate::bail_parse_error!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        operator
                    );
                }

                let result_column_arms: Vec<&[BoundColumn]> =
                    std::iter::once(result_columns.as_slice())
                        .chain(
                            compound_result_columns
                                .iter()
                                .map(|columns| columns.as_slice()),
                        )
                        .collect();
                let resolved =
                    resolve_compound_order_by(&select.order_by, &result_column_arms)?;
                select.order_by.clear();
                resolved
            };

            // 5. Bind LIMIT/OFFSET (no scope — these are standalone expressions)
            if let Some(limit) = select.limit.as_mut() {
                let empty = BindScope::empty();
                ctx.bind_expr(&mut limit.expr, &empty)?;
                if let Some(offset) = limit.offset.as_mut() {
                    ctx.bind_expr(offset, &empty)?;
                }
            }

            // 6. Extract CTE definitions in definition order before with_query
            //    restores them. Using definition order is critical because
            //    referenced_cte_indices are offsets into this order.
            let cte_definitions: Vec<(String, CteEntry)> = if let Some(with) = &select.with {
                let mut ctes = std::mem::take(&mut ctx.ctes);
                with.ctes
                    .iter()
                    .filter_map(|cte| {
                        let name = normalize_ident(cte.tbl_name.as_str());
                        ctes.remove(&name).map(|entry| (name, entry))
                    })
                    .collect()
            } else {
                vec![]
            };

            Ok(BoundSelect {
                result_columns,
                compound_result_columns,
                compound_order_by,
                main_scope,
                compound_scopes,
                tracking: std::mem::take(&mut ctx.tracking),
                subquery_bindings: std::mem::take(&mut ctx.subquery_bindings),
                cte_definitions,
                derived_bindings: std::mem::take(&mut ctx.derived_bindings),
            })
        })
    }

    /// Bind a single SELECT (not compound). Returns bound result columns,
    /// the scope, and the join order.
    fn bind_one_select(
        &mut self,
        one: &mut ast::OneSelect,
        expected_types: &[Option<Arc<TypeDef>>],
    ) -> Result<(Arc<Vec<BoundColumn>>, BindScope)> {
        self.with_scope(|ctx| {
            match one {
                ast::OneSelect::Select {
                    columns,
                    from,
                    where_clause,
                    group_by,
                    window_clause,
                    ..
                } => {
                    // 1. Bind FROM → build scope
                    let scope = match from {
                        Some(from) => ctx.bind_from(from)?,
                        None => {
                            // Check for Star/TableStar without FROM before expansion
                            for col in columns.iter() {
                                if matches!(col, ast::ResultColumn::Star) {
                                    crate::bail_parse_error!("no tables specified");
                                }
                            }
                            BindScope::empty()
                        }
                    };

                    // 2. Star/TableStar result columns stay unexpanded in the
                    // AST: `extract_bound_columns` derives their names and
                    // arity for alias resolution, and the planner's
                    // `select_star` fast path expands them into plan columns
                    // without materializing per-column AST nodes (which is
                    // wasteful for wide tables).

                    // 3. Bind WINDOW definitions (NoAliases — same phase as SELECT list)
                    ctx.with_phase(BindPhase::NoAliases, |ctx| {
                        ctx.bind_window_defs(window_clause, &scope)
                    })?;

                    // 5a. Detect scalar subqueries shared between GROUP BY and
                    //     the SELECT list before either is bound. Shared
                    //     occurrences get one subquery id (see bind_expr).
                    ctx.collect_shared_subqueries(columns, group_by.as_ref());

                    // 5. Extract bound columns (names + resolved exprs) before
                    //    the main bind pass rewrites the AST in-place.
                    let bound_columns =
                        Arc::new(ctx.extract_bound_columns(columns, &scope, expected_types)?);

                    // 6. Store as aliases for later phases (WHERE, GROUP BY, ORDER BY)
                    ctx.set_aliases(Arc::clone(&bound_columns));

                    // 7. Bind SELECT expressions in-place (NoAliases phase)
                    ctx.with_phase(BindPhase::NoAliases, |ctx| {
                        ctx.bind_select_list(columns, &scope)
                    })?;

                    // 8. Bind WHERE (TableFirst phase — table columns first, aliases as fallback)
                    if let Some(where_expr) = where_clause {
                        ctx.with_phase(BindPhase::TableFirst, |ctx| {
                            ctx.bind_expr(where_expr, &scope)
                        })?;
                    }

                    // 9. Bind GROUP BY and HAVING. In GROUP BY, real columns
                    //    take precedence over SELECT aliases (TableFirst);
                    //    HAVING prefers aliases (AliasFirst) — matching SQLite.
                    if let Some(group_by) = group_by {
                        ctx.with_phase(BindPhase::TableFirst, |ctx| {
                            ctx.bind_group_by(group_by, &scope)
                        })?;
                    }

                    Ok((bound_columns, scope))
                }
                ast::OneSelect::Values(rows) => {
                    let scope = BindScope::empty();
                    // Generate column1, column2, ... names from the arity
                    // of the first VALUES row (matching SQLite behavior).
                    let num_cols = rows.first().map_or(0, |row| row.len());
                    let bound_columns: Arc<Vec<BoundColumn>> = Arc::new(
                        (0..num_cols)
                            .map(|i| BoundColumn {
                                name: format!("column{}", i + 1),
                                expr: ast::Expr::Literal(ast::Literal::Numeric(i.to_string())),
                                is_explicit_alias: false,
                            })
                            .collect(),
                    );
                    for row in rows.iter_mut() {
                        for (index, expr) in row.iter_mut().enumerate() {
                            ctx.bind_expr_with_expected_type(
                                expr,
                                &scope,
                                expected_types.get(index).and_then(Option::as_deref),
                            )?;
                        }
                    }
                    Ok((bound_columns, scope))
                }
            }
        })
    }

    /// Bind expressions in the SELECT list.
    fn bind_select_list(
        &mut self,
        columns: &mut [ast::ResultColumn],
        scope: &BindScope,
    ) -> Result<()> {
        for col in columns.iter_mut() {
            match col {
                ast::ResultColumn::Expr(expr, _) => {
                    self.bind_expr(expr, scope)?;
                }
                // Star and TableStar don't contain expressions to bind
                ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {}
            }
        }
        Ok(())
    }

    /// Bind WINDOW definition expressions (PARTITION BY, ORDER BY).
    fn bind_window_defs(
        &mut self,
        window_defs: &mut [ast::WindowDef],
        scope: &BindScope,
    ) -> Result<()> {
        for def in window_defs.iter_mut() {
            for expr in &mut def.window.partition_by {
                self.bind_expr(expr, scope)?;
            }
            for sorted_col in &mut def.window.order_by {
                self.bind_expr(&mut sorted_col.expr, scope)?;
            }
        }
        Ok(())
    }

    /// Inline trivial subqueries like `(SELECT alias_name)` by resolving
    /// the inner expression against the current alias list. This avoids
    /// creating correlated subqueries for ORDER BY / HAVING expressions
    /// that are really just alias references wrapped in a subquery.
    /// Inline trivial subqueries like `(SELECT alias_name)` by resolving
    /// the inner expression against the current alias list. Only inlines if
    /// the name ONLY matches an alias and NOT a source column (SQLite gives
    /// source columns priority inside subquery context).
    fn try_inline_trivial_subquery(&self, expr: &mut ast::Expr, scope: &BindScope) {
        // Inline at any depth: e.g. `HAVING (SELECT s) > 15` wraps the trivial
        // subquery inside a comparison.
        let _ = walk_expr_mut(expr, &mut |e: &mut ast::Expr| {
            self.try_inline_trivial_subquery_at(e, scope);
            Ok(WalkControl::Continue)
        });
    }

    fn try_inline_trivial_subquery_at(&self, expr: &mut ast::Expr, scope: &BindScope) {
        if let ast::Expr::Subquery(select) = expr {
            // Only inline if there's no FROM, no WHERE, no compounds, no LIMIT
            if select.with.is_none()
                && select.body.compounds.is_empty()
                && select.order_by.is_empty()
                && select.limit.is_none()
            {
                if let ast::OneSelect::Select {
                    columns,
                    from: None,
                    where_clause: None,
                    group_by: None,
                    ..
                } = &select.body.select
                {
                    if columns.len() == 1 {
                        // An implicit column name (preserved SQL text) is not a
                        // user alias, so it doesn't disqualify inlining.
                        if let ast::ResultColumn::Expr(inner_expr, alias) = &columns[0] {
                            if alias.as_ref().is_some_and(|a| a.is_explicit()) {
                                return;
                            }
                            if let ast::Expr::Id(name) = inner_expr.as_ref() {
                                // Only inline if the name doesn't match any source column.
                                // If a source column exists, the subquery should
                                // resolve it as a correlated column reference, not an alias.
                                if scope
                                    .find_column_unqualified(name.as_str())
                                    .ok()
                                    .flatten()
                                    .is_none()
                                {
                                    if let Some(alias_expr) = self.resolve_alias(name.as_str()) {
                                        *expr = alias_expr;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Replace a numeric literal (e.g. `1`, `2`) with the corresponding
    /// SELECT result column expression, mirroring SQLite: only positive
    /// integer literals are column references (floats stay constant
    /// expressions); an explicit COLLATE or a single-element parenthesized
    /// wrapper is looked through; `+2` counts as an ordinal while `-2` is out
    /// of range.
    fn replace_column_number(&self, expr: &mut ast::Expr) -> Result<()> {
        self.replace_column_number_inner(expr, "ORDER BY")
    }

    fn replace_column_number_inner(&self, expr: &mut ast::Expr, clause_name: &str) -> Result<()> {
        match expr {
            ast::Expr::Collate(inner, _) => {
                return self.replace_column_number_inner(inner, clause_name);
            }
            ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
                let inner = exprs[0].as_mut();
                return self.replace_column_number_inner(inner, clause_name);
            }
            _ => {}
        }

        let num_str = match expr {
            ast::Expr::Literal(ast::Literal::Numeric(num)) => Some(num.clone()),
            ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => {
                if let ast::Expr::Literal(ast::Literal::Numeric(num)) = inner.as_ref() {
                    Some(num.clone())
                } else {
                    None
                }
            }
            ast::Expr::Unary(ast::UnaryOperator::Negative, inner) => {
                if let ast::Expr::Literal(ast::Literal::Numeric(num)) = inner.as_ref() {
                    if num.parse::<i32>().is_ok() {
                        crate::bail_parse_error!(
                            "1st {} term out of range - should be between 1 and {}",
                            clause_name,
                            self.aliases().len()
                        );
                    }
                }
                None
            }
            _ => None,
        };
        if let Some(num) = num_str {
            // Mirroring SQLite's sqlite3ExprIsInteger, only literals that fit
            // a 32-bit int count as column positions; larger integers (and
            // floats) are ordinary constant expressions.
            if let Ok(column_number) = num.parse::<i32>() {
                let aliases = self.aliases();
                if column_number <= 0 || column_number as usize > aliases.len() {
                    crate::bail_parse_error!(
                        "1st {} term out of range - should be between 1 and {}",
                        clause_name,
                        aliases.len()
                    );
                }
                *expr = aliases[column_number as usize - 1].expr.clone();
            }
        }
        Ok(())
    }

    /// Bind GROUP BY expressions and HAVING clause.
    fn bind_group_by(&mut self, group_by: &mut ast::GroupBy, scope: &BindScope) -> Result<()> {
        // GROUP BY expressions in a correlated subquery cannot reference the
        // outer query scope. Raise the frame floor so resolution can't see
        // enclosing scopes (subqueries inside the GROUP BY expr still push and
        // see frames above the floor), while error messages can still name
        // outer tables correctly.
        let saved_floor = self.outer_frame_floor;
        self.outer_frame_floor = self.outer_query_frames.len();
        let group_result: Result<()> = (|| {
            for expr in &mut group_by.exprs {
                self.replace_column_number_inner(expr, "GROUP BY")?;
                self.bind_expr(expr, scope)?;
            }
            Ok(())
        })();
        self.outer_frame_floor = saved_floor;
        group_result?;
        if let Some(having) = &mut group_by.having {
            // Before alias resolution replaces identifiers with their
            // underlying expressions, reject identifiers inside an aggregate's
            // arguments that resolve to an aggregate alias, matching SQLite's
            // "misuse of aliased aggregate" (NC_AllowAgg in resolve.c).
            self.check_aliased_aggregate_misuse(having)?;
            self.with_phase(BindPhase::AliasFirst, |ctx| {
                ctx.try_inline_trivial_subquery(having, scope);
                ctx.bind_expr(having, scope)
            })?;
        }
        Ok(())
    }

    /// Reject `HAVING agg(... alias ...)` where `alias` names an aggregate
    /// result column (e.g. `SELECT min(x) AS m ... HAVING max(m+5) < 10`).
    fn check_aliased_aggregate_misuse(&self, expr: &ast::Expr) -> Result<()> {
        let expr_contains_aggregate = |e: &ast::Expr| {
            let mut found = false;
            let _ = walk_expr(e, &mut |n: &ast::Expr| {
                match n {
                    ast::Expr::FunctionCall { name, args, .. } => {
                        if matches!(
                            Func::resolve_function(name.as_str(), args.len()),
                            Ok(Some(Func::Agg(_)))
                        ) {
                            found = true;
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    ast::Expr::FunctionCallStar { name, .. } => {
                        if matches!(
                            Func::resolve_function(name.as_str(), 0),
                            Ok(Some(Func::Agg(_)))
                        ) {
                            found = true;
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    _ => {}
                }
                Ok(WalkControl::Continue)
            });
            found
        };
        walk_expr(expr, &mut |e: &ast::Expr| {
            let is_agg = match e {
                ast::Expr::FunctionCall { name, args, .. } => matches!(
                    Func::resolve_function(name.as_str(), args.len()),
                    Ok(Some(Func::Agg(_)))
                ),
                ast::Expr::FunctionCallStar { name, .. } => matches!(
                    Func::resolve_function(name.as_str(), 0),
                    Ok(Some(Func::Agg(_)))
                ),
                _ => false,
            };
            if !is_agg {
                return Ok(WalkControl::Continue);
            }
            if let ast::Expr::FunctionCall { args, .. } = e {
                for arg in args.iter() {
                    walk_expr(arg, &mut |n: &ast::Expr| {
                        if let ast::Expr::Id(id) = n {
                            let normalized = normalize_ident(id.as_str());
                            for bc in self.aliases().iter() {
                                if bc.is_explicit_alias
                                    && bc.name.eq_ignore_ascii_case(&normalized)
                                    && expr_contains_aggregate(&bc.expr)
                                {
                                    crate::bail_parse_error!(
                                        "misuse of aliased aggregate {}",
                                        normalized
                                    );
                                }
                            }
                        }
                        Ok(WalkControl::Continue)
                    })?;
                }
            }
            Ok(WalkControl::SkipChildren)
        })?;
        Ok(())
    }

    /// Expand `Star` and `TableStar` result columns in-place (RETURNING only —
    /// SELECT lists keep stars unexpanded and go through the planner's
    /// `select_star` fast path instead).
    ///
    /// After this, the `columns` vec contains only `ResultColumn::Expr` entries.
    /// Handles USING dedup, hidden columns, semi/anti-join filtering, and
    /// right_join_swapped ordering — matching the planner's `select_star`.
    fn expand_stars(
        &mut self,
        columns: &mut Vec<ast::ResultColumn>,
        scope: &BindScope,
    ) -> Result<()> {
        let mut expanded = Vec::with_capacity(columns.len());
        for col in columns.drain(..) {
            match col {
                ast::ResultColumn::Star => {
                    let table_iter: Vec<&ScopeTable> = if scope.right_join_swapped {
                        scope.tables.iter().rev().collect()
                    } else {
                        scope.tables.iter().collect()
                    };
                    for st in table_iter {
                        // Semi/anti-join tables don't contribute to SELECT *
                        if st.join_info.as_ref().is_some_and(|ji| ji.is_semi_or_anti()) {
                            continue;
                        }
                        // If this table's identifier appears more than once in
                        // the FROM clause, expanding * would produce ambiguous
                        // column references (matches SQLite). Columns
                        // deduplicated by USING/NATURAL are not ambiguous.
                        let has_duplicate_identifier = scope
                            .tables
                            .iter()
                            .filter(|t| t.identifier == st.identifier)
                            .count()
                            > 1;
                        if has_duplicate_identifier {
                            let using_cols: Vec<&str> = scope
                                .tables
                                .iter()
                                .filter(|t| t.identifier == st.identifier)
                                .filter_map(|t| t.join_info.as_ref())
                                .flat_map(|ji| ji.using.iter().map(|u| u.as_str()))
                                .collect();
                            for col_ref in st.table.columns() {
                                if col_ref.is_hidden {
                                    continue;
                                }
                                let in_using = using_cols
                                    .iter()
                                    .any(|u| u.eq_ignore_ascii_case(col_ref.name));
                                if !in_using {
                                    crate::bail_parse_error!(
                                        "ambiguous column name: {}.{}",
                                        st.identifier,
                                        col_ref.name
                                    );
                                }
                            }
                        }
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            // USING dedup: skip columns from right table that are in USING
                            if let Some(ji) = &st.join_info {
                                if ji
                                    .using
                                    .iter()
                                    .any(|u| u.as_str().eq_ignore_ascii_case(col_ref.name))
                                {
                                    continue;
                                }
                            }
                            self.tracking.record_column(st.internal_id, col_ref.idx);
                            expanded.push(ast::ResultColumn::Expr(
                                Box::new(ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                }),
                                Some(ast::As::As(ast::Name::exact(col_ref.name.to_string()))),
                            ));
                        }
                    }
                }
                ast::ResultColumn::TableStar(ref name) => {
                    let normalized = normalize_ident(name.as_str());
                    if let Some(st) = scope
                        .tables
                        .iter()
                        .find(|t| t.identifier.eq_ignore_ascii_case(&normalized))
                    {
                        for col_ref in st.table.columns() {
                            if col_ref.is_hidden {
                                continue;
                            }
                            self.tracking.record_column(st.internal_id, col_ref.idx);
                            expanded.push(ast::ResultColumn::Expr(
                                Box::new(ast::Expr::Column {
                                    database: None,
                                    table: st.internal_id,
                                    column: col_ref.idx,
                                    is_rowid_alias: col_ref.is_rowid_alias,
                                }),
                                Some(ast::As::As(ast::Name::exact(col_ref.name.to_string()))),
                            ));
                        }
                    } else {
                        // Table not found — leave as-is, planner will error
                        expanded.push(col);
                    }
                }
                other => expanded.push(other),
            }
        }
        *columns = expanded;
        Ok(())
    }

    fn bind_cte(&mut self, with: &mut ast::With) -> Result<()> {
        // Collect CTE names in definition order for referenced_cte_indices lookup.
        let mut cte_names: Vec<String> = Vec::with_capacity(with.ctes.len());
        let mut referenced_tables_by_cte: Vec<Vec<String>> = Vec::with_capacity(with.ctes.len());

        // Pass 1: register all CTE names and allocate IDs. Bodies are not
        // bound yet — SQLite resolves a CTE body only when the CTE is
        // referenced, so binding errors are deferred via `bind_error`.
        for cte in &with.ctes {
            let cte_name = normalize_ident(cte.tbl_name.as_str());
            // Check for duplicates within the same WITH clause only.
            // Inner WITH clauses are allowed to shadow outer CTE names.
            if cte_names.contains(&cte_name) {
                crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
            }
            let explicit_columns: Vec<String> = cte
                .columns
                .iter()
                .map(|c| normalize_ident(c.col_name.as_str()))
                .collect();

            let cte_id = self.id_gen.next_cte_id();
            let materialize_hint = cte.materialized == turso_parser::ast::Materialized::Yes;

            // Table names this body references (schema-qualified names are
            // excluded — they can never refer to a CTE). Sibling dependency
            // edges are computed from these once all names are known.
            let mut referenced_tables = Vec::new();
            crate::translate::planner::collect_from_clause_table_refs(
                &cte.select,
                &mut referenced_tables,
            );

            // A body that references its own name is a recursive CTE (the
            // RECURSIVE keyword is not required, matching SQLite) — unless the
            // reference is in the first arm, which is a circular reference.
            let (recursive, first_arm_self_ref) =
                crate::translate::planner::cte_self_reference_info(&cte_name, &cte.select);
            let bind_error = first_arm_self_ref
                .then(|| format!("circular reference: {}", cte.tbl_name.as_str()));

            referenced_tables_by_cte.push(referenced_tables);
            cte_names.push(cte_name.clone());
            self.insert_cte(
                cte_name,
                CteEntry {
                    select: cte.select.clone(),
                    // Explicit columns are the reference-visible columns and
                    // are known up front — forward references to this CTE can
                    // bind before its body does.
                    resolved_columns: explicit_columns.clone(),
                    explicit_columns,
                    cte_id,
                    result_column_count: 0,
                    inner_bound: None,
                    referenced_cte_indices: SmallVec::new(),
                    materialize_hint,
                    recursive,
                    recursive_binding: None,
                    bind_error,
                },
            );
        }

        // Sibling dependency edges. Forward references are included: SQLite
        // allows a CTE to reference one defined later in the same WITH clause.
        for (idx, referenced_tables) in referenced_tables_by_cte.iter().enumerate() {
            let indices: SmallVec<[usize; 2]> = (0..cte_names.len())
                .filter(|&i| i != idx && referenced_tables.contains(&cte_names[i]))
                .collect();
            self.ctes
                .get_mut(&cte_names[idx])
                .unwrap()
                .referenced_cte_indices = indices;
        }

        // Pass 2: bind bodies dependency-first so referenced siblings (in
        // either direction) have their result columns resolved before any
        // body that reads them. We collect inner_bound values separately
        // because bind_select calls with_query which clones self.ctes
        // (setting inner_bound = None via the custom Clone impl), then
        // restores them — destroying inner_bound values set in prior
        // iterations.
        let mut inner_bounds: Vec<(String, BoundSelect)> = Vec::with_capacity(with.ctes.len());
        let mut recursive_bindings: Vec<(String, RecursiveCteBinding)> = Vec::new();
        let mut done = vec![false; with.ctes.len()];
        for idx in 0..with.ctes.len() {
            self.bind_one_cte(
                with,
                &cte_names,
                idx,
                &mut done,
                &mut inner_bounds,
                &mut recursive_bindings,
            )?;
        }
        // Assign inner_bound values after all binding is done.
        for (cte_name, bound) in inner_bounds {
            self.ctes.get_mut(&cte_name).unwrap().inner_bound = Some(bound);
        }
        for (cte_name, binding) in recursive_bindings {
            self.ctes.get_mut(&cte_name).unwrap().recursive_binding = Some(binding);
        }
        Ok(())
    }

    /// Bind one CTE body (dependencies first). Errors don't propagate: they
    /// are stored on the entry and surface when the CTE is referenced,
    /// matching SQLite's lazy resolution of CTE bodies.
    fn bind_one_cte(
        &mut self,
        with: &mut ast::With,
        cte_names: &[String],
        idx: usize,
        done: &mut [bool],
        inner_bounds: &mut Vec<(String, BoundSelect)>,
        recursive_bindings: &mut Vec<(String, RecursiveCteBinding)>,
    ) -> Result<()> {
        if done[idx] {
            return Ok(());
        }
        done[idx] = true;
        let cte_name = cte_names[idx].clone();
        let Some(entry) = self.ctes.get(&cte_name) else {
            return Ok(());
        };
        if entry.bind_error.is_some() {
            return Ok(());
        }
        let cte_id = entry.cte_id;
        let deps = entry.referenced_cte_indices.clone();
        let is_recursive = entry.recursive;

        self.ctes_being_bound.push((cte_id, cte_name.clone()));
        let result: Result<()> = (|| {
            for &dep in &deps {
                let dep_id = self.ctes.get(&cte_names[dep]).map(|e| e.cte_id);
                if dep_id.is_some_and(|id| self.ctes_being_bound.iter().any(|(b, _)| *b == id)) {
                    crate::bail_parse_error!("circular reference: {}", cte_names[dep]);
                }
                self.bind_one_cte(with, cte_names, dep, done, inner_bounds, recursive_bindings)?;
            }
            let cte = &mut with.ctes[idx];
            if is_recursive {
                // Structure errors (circular reference, multiple recursive
                // references) match the recursive planner's exactly.
                let first_recursive_idx =
                    crate::translate::planner::validate_recursive_cte_structure(
                        &cte_name,
                        &cte.select,
                    )?;
                // Bind the initial (non-recursive) arms as one SELECT,
                // including the body-level WITH. SQLite takes the CTE's
                // column names and arity from the left-most arm, which
                // cannot reference the recursive table.
                let mut initial = ast::Select {
                    with: cte.select.with.clone(),
                    body: ast::SelectBody {
                        select: cte.select.body.select.clone(),
                        compounds: cte.select.body.compounds[..first_recursive_idx - 1].to_vec(),
                    },
                    order_by: vec![],
                    limit: None,
                };
                let initial_bound = self.bind_select(&mut initial)?;
                let entry = self.ctes.get_mut(&cte_name).unwrap();
                entry.result_column_count = initial_bound.result_columns.len();
                if entry.explicit_columns.is_empty() {
                    entry.resolved_columns = initial_bound
                        .result_columns
                        .iter()
                        .map(|bc| bc.name.clone())
                        .collect();
                }
                let self_table = Arc::new(CteTable {
                    columns: entry.resolved_columns.clone(),
                });

                // Bind each recursive arm as its own single-arm SELECT with
                // the CTE's own name resolving to the recursive input table.
                // Every self-reference shares input_id.
                let input_id = self.id_gen.next_table_id();
                let saved_self = self.recursive_self.replace(RecursiveSelfRef {
                    cte_id,
                    input_id,
                    table: self_table,
                });
                let arms_result = (|| -> Result<Vec<BoundSubquery>> {
                    let cte = &with.ctes[idx];
                    let mut arms = Vec::with_capacity(
                        cte.select.body.compounds.len() - (first_recursive_idx - 1),
                    );
                    for compound in &cte.select.body.compounds[first_recursive_idx - 1..] {
                        let mut arm = ast::Select {
                            with: cte.select.with.clone(),
                            body: ast::SelectBody {
                                select: compound.select.clone(),
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        };
                        let inner_bound = self.bind_select(&mut arm)?;
                        arms.push(BoundSubquery {
                            select: arm,
                            inner_bound,
                        });
                    }
                    Ok(arms)
                })();
                self.recursive_self = saved_self;
                let recursive_arms = arms_result?;
                let last_recursive_count = recursive_arms
                    .last()
                    .expect("recursive CTE must have a recursive arm")
                    .inner_bound
                    .result_columns
                    .len();
                if recursive_arms[..recursive_arms.len() - 1]
                    .iter()
                    .any(|arm| arm.inner_bound.result_columns.len() != last_recursive_count)
                {
                    crate::bail_parse_error!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        ast::CompoundOperator::UnionAll
                    );
                }
                let recursive_operator =
                    with.ctes[idx].select.body.compounds[first_recursive_idx - 1].operator;
                if initial_bound.result_columns.len() != last_recursive_count {
                    crate::bail_parse_error!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        recursive_operator
                    );
                }

                let mut result_column_arms: Vec<&[BoundColumn]> = Vec::new();
                result_column_arms.push(initial_bound.result_columns.as_slice());
                result_column_arms.extend(
                    initial_bound
                        .compound_result_columns
                        .iter()
                        .map(|columns| columns.as_slice()),
                );
                result_column_arms.extend(
                    recursive_arms
                        .iter()
                        .map(|arm| arm.inner_bound.result_columns.as_slice()),
                );
                let queue_order = resolve_compound_order_by(
                    &self.ctes.get(&cte_name).unwrap().select.order_by,
                    &result_column_arms,
                )?;
                // Bind the body-level LIMIT/OFFSET up front. They are
                // scope-less (identifiers cannot resolve against the body's
                // tables), and the recursive-CTE planner consumes them as
                // already bound.
                let resolver = self.resolver;
                let mut limit = {
                    let entry = self.ctes.get_mut(&cte_name).unwrap();
                    entry.select.order_by.clear();
                    entry.select.limit.take()
                };
                if let Some(limit) = limit.as_mut() {
                    let empty_scope = BindScope::empty();
                    bind_scopeless_expr(&mut limit.expr, resolver)?;
                    self.bind_custom_type_function_calls(&mut limit.expr, &empty_scope, None)?;
                    if let Some(offset) = limit.offset.as_mut() {
                        bind_scopeless_expr(offset, resolver)?;
                        self.bind_custom_type_function_calls(offset, &empty_scope, None)?;
                    }
                }
                self.ctes.get_mut(&cte_name).unwrap().select.limit = limit;
                recursive_bindings.push((
                    cte_name.clone(),
                    RecursiveCteBinding {
                        initial: BoundSubquery {
                            select: initial,
                            inner_bound: initial_bound,
                        },
                        recursive_arms,
                        first_recursive_arm_index: first_recursive_idx,
                        input_id,
                        queue_order,
                    },
                ));
            } else {
                let bound = self.bind_select(&mut cte.select)?;
                let entry = self.ctes.get_mut(&cte_name).unwrap();
                entry.result_column_count = bound.result_columns.len();
                if entry.explicit_columns.is_empty() {
                    entry.resolved_columns = bound
                        .result_columns
                        .iter()
                        .map(|bc| bc.name.clone())
                        .collect();
                }
                entry.select = cte.select.clone();
                inner_bounds.push((cte_name.clone(), bound));
            }
            Ok(())
        })();
        self.ctes_being_bound.pop();
        if let Err(err) = result {
            let msg = match err {
                crate::LimboError::ParseError(m) => m,
                other => other.to_string(),
            };
            if let Some(entry) = self.ctes.get_mut(&cte_name) {
                entry.bind_error = Some(msg);
            }
        }
        Ok(())
    }

    fn resolve_select_table(
        &mut self,
        table: &mut ast::SelectTable,
        lateral_scope: Option<&BindScope>,
        position: usize,
    ) -> Result<ScopeTable> {
        match table {
            // Named table: CTE lookup first, then schema lookup
            ast::SelectTable::Table(name, alias, indexed) => {
                let table_name = normalize_ident(name.name.as_str());
                // 1. Determine identifier (alias or table name)
                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| table_name.clone());

                // 2. Check self.ctes for a CTE match. Schema-qualified names
                // (e.g. main.t) always refer to schema objects, never CTEs.
                if let Some(cte) = self
                    .ctes
                    .get(&table_name)
                    .filter(|_| name.db_name.is_none())
                {
                    // Referencing a CTE whose body is currently being bound is
                    // a circular reference (identity-checked by cte_id, so a
                    // shadowing nested CTE with the same name is unaffected) —
                    // unless we are binding that CTE's own recursive arms, where
                    // the self-reference resolves to the recursive input table.
                    if self
                        .ctes_being_bound
                        .iter()
                        .any(|(id, _)| *id == cte.cte_id)
                    {
                        if let Some(recursive_self) = self
                            .recursive_self
                            .as_ref()
                            .filter(|recursive_self| recursive_self.cte_id == cte.cte_id)
                        {
                            return Ok(ScopeTable {
                                identifier,
                                internal_id: recursive_self.input_id,
                                source: ScopeTableSource::Cte { name: table_name },
                                table: recursive_self.table.clone(),
                                join_info: None,
                                database_id: 0,
                                indexed: None,
                                bound_index_method_patterns: Vec::new(),
                                bound_index_expressions: Vec::new(),
                            });
                        }
                        crate::bail_parse_error!("circular reference: {}", table_name);
                    }
                    // Surface any binding error deferred from the (lazy) CTE
                    // body bind.
                    if let Some(msg) = &cte.bind_error {
                        return Err(crate::LimboError::ParseError(msg.clone()));
                    }
                    validate_cte_explicit_columns(&table_name, cte)?;
                    //    - resolved_columns was populated by bind_cte pass 2
                    //    - Build Arc<CteTable> as the BindTable
                    let cte_table = Arc::new(CteTable {
                        columns: cte.resolved_columns.clone(),
                    });
                    // 4. Generate internal_id via self.id_gen.next_table_id()
                    return Ok(ScopeTable {
                        identifier,
                        internal_id: self.id_gen.next_table_id(),
                        source: ScopeTableSource::Cte { name: table_name },
                        table: cte_table,
                        join_info: None,
                        database_id: 0,
                        indexed: None,
                        bound_index_method_patterns: Vec::new(),
                        bound_index_expressions: Vec::new(),
                    });
                }

                // 3. Otherwise, schema lookup via resolver
                //    - Handle cross-database references (e.g. aux.t1)
                let database_id = self
                    .resolver
                    .resolve_existing_table_database_id_qualified(name)?;

                // 3a. Check for views — expand them as derived tables (subqueries)
                if let Some(view) = self
                    .resolver
                    .with_schema(database_id, |s| s.get_view(&table_name))
                {
                    view.process()?;
                    // Clone what we need before releasing the view reference.
                    // Keep Arc to original so we can call done() after binding.
                    let view_ref = view.clone(); // Arc clone, not View clone
                    let view_columns = view.columns.clone();
                    let mut view_select = view.select_stmt.clone();
                    // Apply view column aliases to the SELECT result columns
                    if let ast::OneSelect::Select {
                        ref mut columns, ..
                    } = view_select.body.select
                    {
                        for (col, result_col) in view_columns.iter().zip(columns.iter_mut()) {
                            if let (Some(name_str), ast::ResultColumn::Expr(_, ref mut col_alias)) =
                                (&col.name, result_col)
                            {
                                *col_alias = Some(ast::As::As(ast::Name::exact(name_str.clone())));
                            }
                        }
                    }

                    // Bind the view's SELECT as a derived table (subquery).
                    // Views resolve against the schema only — CTEs from the
                    // calling query must not leak into the view body.
                    let saved_ctes = std::mem::take(&mut self.ctes);
                    let bound_select = self.bind_select(&mut view_select);
                    self.ctes = saved_ctes;
                    // Reset view state so nested view-on-view chains don't
                    // falsely detect circular definitions during ALTER TABLE.
                    view_ref.done();
                    let bound_select = bound_select?;
                    let subquery_columns: Vec<String> = bound_select
                        .result_columns
                        .iter()
                        .map(|bc| bc.name.clone())
                        .collect();
                    let subquery_table = Arc::new(DerivedTable {
                        columns: subquery_columns,
                    });

                    let internal_id = self.id_gen.next_table_id();

                    self.derived_bindings.insert(
                        internal_id,
                        BoundSubquery {
                            select: view_select,
                            inner_bound: bound_select,
                        },
                    );

                    return Ok(ScopeTable {
                        identifier,
                        internal_id,
                        source: ScopeTableSource::Derived {},
                        table: subquery_table,
                        join_info: None,
                        database_id: 0,
                        indexed: None,
                        bound_index_method_patterns: Vec::new(),
                        bound_index_expressions: Vec::new(),
                    });
                }

                // 3b. Materialized views with storage are treated as
                // regular BTree tables.
                let matview = self.resolver.with_schema(database_id, |schema| {
                    schema.get_materialized_view(&table_name)
                });
                if let Some(view) = matview {
                    let has_compatible_state = self.resolver.with_schema(database_id, |schema| {
                        schema.has_compatible_dbsp_state_table(&table_name)
                    });
                    if !has_compatible_state {
                        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
                        return Err(crate::LimboError::InternalError(format!(
                            "Materialized view '{table_name}' has an incompatible version. \n\
                             The current version is {DBSP_CIRCUIT_VERSION}, but the view was created with a different version. \n\
                             Please DROP and recreate the view to use it."
                        )));
                    }
                    let view_guard = view.lock();
                    let root_page = view_guard.get_root_page();
                    if root_page == 0 {
                        drop(view_guard);
                        return Err(crate::LimboError::InternalError(
                            "Materialized view has no storage allocated".to_string(),
                        ));
                    }
                    let columns = view_guard.column_schema.flat_columns();
                    let btree_table = Arc::new(crate::schema::BTreeTable::new(
                        root_page,
                        view_guard.name().to_string(),
                        crate::alloc::vec![],
                        columns,
                        crate::schema::BTreeCharacteristics::HAS_ROWID,
                        crate::alloc::vec![],
                        crate::alloc::vec![],
                        crate::alloc::vec![],
                        None,
                    ));
                    drop(view_guard);
                    let table = Arc::new(Table::BTree(btree_table));
                    return Ok(ScopeTable {
                        identifier,
                        internal_id: self.id_gen.next_table_id(),
                        source: ScopeTableSource::Table(table.clone()),
                        table,
                        join_info: None,
                        database_id,
                        indexed: None,
                        bound_index_method_patterns: Vec::new(),
                        bound_index_expressions: Vec::new(),
                    });
                }

                // 3c. Regular table lookup
                let Some(schema_table) = self
                    .resolver
                    .with_schema(database_id, |s| s.get_table(&table_name))
                else {
                    // Incompatible materialized view?
                    let is_incompatible = self.resolver.with_schema(database_id, |schema| {
                        schema.incompatible_views.contains(&table_name)
                    });
                    if is_incompatible {
                        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
                        crate::bail_parse_error!(
                            "Materialized view '{}' has an incompatible version. \n\
                             The view was created with a different DBSP version than the current version ({}). \n\
                             Please DROP and recreate the view to use it.",
                            table_name,
                            DBSP_CIRCUIT_VERSION
                        );
                    }
                    // A view row whose stored SQL failed to parse at schema load
                    let is_broken_view = self.resolver.with_schema(database_id, |schema| {
                        schema.broken_views.contains(&table_name)
                    });
                    if is_broken_view {
                        crate::bail_parse_error!(
                            "view '{}' could not be loaded: its SQL in sqlite_schema does not parse. \n\
                             Use DROP VIEW to remove it, then recreate it.",
                            table_name
                        );
                    }
                    crate::bail_parse_error!("no such table: {table_name}");
                };

                // 4. Generate internal_id via self.id_gen.next_table_id()
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Table(schema_table.clone()),
                    table: schema_table,
                    join_info: None,
                    database_id,
                    indexed: indexed.clone(),
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
            // Inline subquery in FROM: SELECT ... FROM (SELECT ...)
            ast::SelectTable::Select(subselect, alias) => {
                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| format!("(subquery-{position})"));

                // FROM subqueries don't correlate with the query being built.
                let bound_select = self.bind_select(subselect)?;

                let subquery_columns: Vec<String> = bound_select
                    .result_columns
                    .iter()
                    .map(|bc| bc.name.clone())
                    .collect();
                let subquery_table = Arc::new(DerivedTable {
                    columns: subquery_columns,
                });

                let internal_id = self.id_gen.next_table_id();

                // Store the binding for planning before into_table_references.
                self.derived_bindings.insert(
                    internal_id,
                    BoundSubquery {
                        select: subselect.clone(),
                        inner_bound: bound_select,
                    },
                );

                Ok(ScopeTable {
                    identifier,
                    internal_id,
                    source: ScopeTableSource::Derived {},
                    table: subquery_table,
                    join_info: None,
                    database_id: 0,
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
            // Virtual table function call: SELECT ... FROM table_func(args)
            ast::SelectTable::TableCall(name, args, alias) => {
                let table_name = normalize_ident(name.name.as_str());
                // Call arguments on a CTE are an error.
                if name.db_name.is_none() && self.ctes.contains_key(&table_name) && !args.is_empty()
                {
                    // A recursive self-reference gets SQLite's table-valued
                    // function message; a plain CTE reference gets the
                    // not-a-function message.
                    let is_recursive_self = self
                        .recursive_self
                        .as_ref()
                        .zip(self.ctes.get(&table_name))
                        .is_some_and(|(recursive_self, cte)| recursive_self.cte_id == cte.cte_id);
                    if is_recursive_self {
                        crate::bail_parse_error!(
                            "too many arguments on {}() - max 0",
                            name.name.as_str()
                        );
                    }
                    crate::bail_parse_error!("'{}' is not a function", name.name.as_str());
                }
                // 1. Look up the virtual table via resolver
                let schema_table =
                    self.resolver
                        .schema()
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!("no such table: {table_name}"))
                        })?;
                // Call arguments on a plain table are an error too — only
                // virtual tables (table-valued functions) accept them.
                if !args.is_empty() && schema_table.btree().is_some() {
                    crate::bail_parse_error!("'{}' is not a function", name.name.as_str());
                }

                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| table_name.clone());

                // 2. Bind argument expressions. Use lateral scope if available so that
                // table function args can reference previously-joined tables
                // (e.g. SELECT * FROM generate_series(0,2) s JOIN json_tree(..., s.value)).
                // Use allow_unbound so that forward references to later FROM tables
                // (e.g. FROM func(s.col), s) are left unresolved for the emitter.
                let empty_scope = BindScope::empty();
                let arg_scope = lateral_scope.unwrap_or(&empty_scope);
                let saved_allow_unbound = self.allow_unbound;
                self.allow_unbound = true;
                for arg in args.iter_mut() {
                    self.bind_expr(arg, arg_scope)?;
                }
                self.allow_unbound = saved_allow_unbound;

                // 3. Build ScopeTable from the virtual table's columns
                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Table(schema_table.clone()),
                    table: schema_table,
                    join_info: None,
                    database_id: 0, // Virtual tables are always in main schema
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
            // Parenthesized FROM subclause: SELECT ... FROM (t1 JOIN t2 ON ...)
            ast::SelectTable::Sub(from_clause, alias) => {
                // 1. Recursively bind_from(from_clause)
                let inner_scope = self.bind_from(from_clause)?;

                // 2-3. Collect all column names from inner scope tables
                let all_columns: Vec<String> = inner_scope
                    .tables
                    .iter()
                    .flat_map(|table| table.table.columns())
                    .map(|col| col.name.to_string())
                    .collect();

                let identifier = alias
                    .as_ref()
                    .map(|a| normalize_ident(a.name().as_str()))
                    .unwrap_or_else(|| format!("(subquery-{position})"));

                // If alias is present, wrap all columns under that alias
                // If no alias, flatten tables into parent scope
                let sub_table = Arc::new(DerivedTable {
                    columns: all_columns,
                });

                Ok(ScopeTable {
                    identifier,
                    internal_id: self.id_gen.next_table_id(),
                    source: ScopeTableSource::Derived {},
                    table: sub_table,
                    join_info: None,
                    database_id: 0,
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                })
            }
        }
    }

    fn resolve_alias(&self, name: &str) -> Option<ast::Expr> {
        let normalized = normalize_ident(name);
        let aliases = self.aliases();
        // Prefer explicit AS aliases over inferred column names.
        // Among explicit aliases, first match wins (SQLite behavior).
        // e.g. SELECT -a AS b, a, t.b ORDER BY b → resolves to -a (explicit AS b)
        // e.g. SELECT a, -b AS a ORDER BY a → resolves to -b (explicit AS a, wins over inferred a)
        if let Some(alias) = aliases
            .iter()
            .find(|a| a.is_explicit_alias && a.name.eq_ignore_ascii_case(&normalized))
        {
            return Some(alias.expr.clone());
        }
        // Fallback: inferred names (first match wins)
        aliases
            .iter()
            .find(|a| a.name.eq_ignore_ascii_case(&normalized))
            .map(|a| a.expr.clone())
    }

    fn resolve_outer_alias(&mut self, name: &str) -> Option<ast::Expr> {
        let normalized = normalize_ident(name);
        let resolved = self.outer_query_frames_iter().find_map(|frame| {
            frame
                .aliases
                .iter()
                .find(|alias| alias.name.eq_ignore_ascii_case(&normalized))
                .map(|alias| alias.expr.clone())
        })?;
        self.record_outer_refs_in_expr(&resolved);
        Some(resolved)
    }

    fn record_outer_refs_in_expr(&mut self, expr: &ast::Expr) {
        let _ = walk_expr(expr, &mut |expr| {
            if let ast::Expr::Column { table, column, .. } = expr {
                self.tracking.record_outer_ref(*table, *column);
            }
            Ok(WalkControl::Continue)
        });
    }

    fn resolve_unqualified_column(
        &mut self,
        name: &str,
        scope: &BindScope,
    ) -> Result<Option<ast::Expr>> {
        if let Some((table_id, col_idx, is_rowid_alias)) = scope.find_column_unqualified(name)? {
            self.tracking.record_column(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        for st in &scope.tables {
            if let Some(row_id_expr) =
                parse_row_id(name, st.internal_id, || scope.tables.len() != 1)?
            {
                self.tracking.record_rowid(st.internal_id);
                return Ok(Some(row_id_expr));
            }
        }

        let outer_match = {
            let mut result = None;
            for outer_scope in self.outer_scopes_iter() {
                if let Some(found) = outer_scope.find_column_unqualified(name)? {
                    result = Some(found);
                    break;
                }
            }
            result
        };
        if let Some((table_id, col_idx, is_rowid_alias)) = outer_match {
            self.tracking.record_outer_ref(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        Ok(None)
    }

    fn resolve_qualified_column(
        &mut self,
        table_name: &str,
        col_name: &str,
        scope: &BindScope,
    ) -> Result<Option<ast::Expr>> {
        // Try real columns first. A user-defined column named "oid", "rowid",
        // or "_rowid_" takes priority over the rowid pseudo-column.
        // find_column_qualified returns Err only when the table IS found but
        // the column is NOT — we intercept that case to try rowid fallback.
        match scope.find_column_qualified(table_name, col_name) {
            Ok(Some((table_id, col_idx, is_rowid_alias))) => {
                self.tracking.record_column(table_id, col_idx);
                return Ok(Some(ast::Expr::Column {
                    database: None,
                    table: table_id,
                    column: col_idx,
                    is_rowid_alias,
                }));
            }
            Ok(None) => {
                // Table not found — continue to outer scope / rowid checks
            }
            Err(err) => {
                // Ambiguity is definitive — never fall back to rowid.
                if err.to_string().contains("ambiguous column name") {
                    return Err(err);
                }
                // Table found but column not found — try rowid pseudo-column
                if let Some(st) = scope.find_table_by_identifier(table_name) {
                    if let Some(row_id_expr) = parse_row_id(col_name, st.internal_id, || false)? {
                        self.tracking.record_rowid(st.internal_id);
                        return Ok(Some(row_id_expr));
                    }
                }
                // Not a rowid either — re-raise the original error
                return Err(crate::LimboError::ParseError(format!(
                    "no such column: {table_name}.{col_name}"
                )));
            }
        }

        // Check outer scopes for columns and rowid
        let outer_match: Option<Result<ast::Expr>> = {
            let mut result = None;
            for outer_scope in self.outer_scopes_iter() {
                // Check real columns first, rowid as fallback
                match outer_scope.find_column_qualified(table_name, col_name) {
                    Ok(Some((table_id, col_idx, is_rowid_alias))) => {
                        result = Some(Ok(ast::Expr::Column {
                            database: None,
                            table: table_id,
                            column: col_idx,
                            is_rowid_alias,
                        }));
                        break;
                    }
                    Ok(None) => {
                        // Table not found in this scope, continue to next
                        continue;
                    }
                    Err(_) => {
                        // Table found but column not — try rowid pseudo-column
                    }
                }
                if let Some(st) = outer_scope.find_table_by_identifier(table_name) {
                    if let Some(row_id_expr) = parse_row_id(col_name, st.internal_id, || false)? {
                        result = Some(Ok(row_id_expr));
                        break;
                    }
                }
                // Column name not found in this scope as real column or rowid
                result = Some(Err(crate::LimboError::ParseError(format!(
                    "no such column: {table_name}.{col_name}"
                ))));
                break;
            }
            result
        };
        if let Some(outer_result) = outer_match {
            let resolved = outer_result?;
            match &resolved {
                ast::Expr::Column {
                    table: table_id,
                    column: col_idx,
                    ..
                } => {
                    self.tracking.record_outer_ref(*table_id, *col_idx);
                }
                ast::Expr::RowId {
                    table: table_id, ..
                } => {
                    self.tracking.record_rowid(*table_id);
                }
                _ => {}
            }
            return Ok(Some(resolved));
        }

        Ok(None)
    }

    /// Search scope tables for a column named `col_name` with a struct/union
    /// type. Errors on ambiguity (>1 match). Returns
    /// `(internal_id, col_idx, is_rowid_alias, type_def)` or `None`.
    /// Scope-based counterpart of `find_custom_type_column`.
    fn find_custom_type_column_in_scope<'t>(
        &'t self,
        scope: &BindScope,
        col_name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool, &'t crate::schema::TypeDef)>> {
        let mut result = None;
        let mut match_count = 0usize;
        for st in &scope.tables {
            let ScopeTableSource::Table(table) = &st.source else {
                continue;
            };
            let cols = table.columns();
            if let Some(col_idx) = cols.iter().position(|c| {
                c.name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(col_name))
            }) {
                let col = &cols[col_idx];
                let type_def = self.resolver.schema().get_type_def_unchecked(&col.ty_str);
                let is_struct_or_union = type_def
                    .map(|td| td.is_struct() || td.is_union())
                    .unwrap_or(false);
                if is_struct_or_union {
                    match_count += 1;
                    result = Some((
                        st.internal_id,
                        col_idx,
                        col.is_rowid_alias(),
                        &**type_def.unwrap(),
                    ));
                }
            }
        }
        if match_count > 1 {
            crate::bail_parse_error!(
                "ambiguous column reference: '{}' — multiple tables have a struct/union column with this name",
                col_name
            );
        }
        Ok(result)
    }

    fn field_access_resolution(
        type_def: &crate::schema::TypeDef,
        field_name: &str,
    ) -> Option<ast::FieldAccessResolution> {
        if let Some((field_index, _)) = type_def.find_struct_field(field_name) {
            Some(ast::FieldAccessResolution::StructField { field_index })
        } else if let Some((tag_index, _)) = type_def.find_union_variant(field_name) {
            Some(ast::FieldAccessResolution::UnionVariant { tag_index })
        } else {
            None
        }
    }

    fn make_field_access_expr(
        table_id: TableInternalId,
        col_idx: usize,
        is_rowid_alias: bool,
        field_name: &str,
        type_def: &crate::schema::TypeDef,
    ) -> Result<ast::Expr> {
        let Some(resolved) = Self::field_access_resolution(type_def, field_name) else {
            if type_def.is_struct() {
                crate::bail_parse_error!(
                    "no such field '{}' in struct type '{}'",
                    field_name,
                    type_def.name
                );
            }
            if type_def.is_union() {
                crate::bail_parse_error!(
                    "no such variant '{}' in union type '{}'",
                    field_name,
                    type_def.name
                );
            }
            crate::bail_parse_error!("type '{}' is not a struct or union type", type_def.name);
        };

        Ok(ast::Expr::FieldAccess {
            base: Box::new(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }),
            field: ast::Name::from_bytes(field_name.as_bytes()),
            resolved: Some(resolved),
        })
    }

    /// Try to resolve `col.mid.leaf` as 2-level deep field access
    /// (e.g. `data.telegram.chat_id`). Scope-based counterpart of
    /// `try_resolve_nested_field_access`.
    fn try_resolve_nested_field_access_in_scope(
        &mut self,
        scope: &BindScope,
        col_name: &str,
        mid_name: &str,
        leaf_name: &str,
    ) -> Result<Option<ast::Expr>> {
        let Some((table_id, col_idx, is_rowid_alias, td)) =
            self.find_custom_type_column_in_scope(scope, col_name)?
        else {
            return Ok(None);
        };

        // Case A: UNION column — mid_name is a variant tag.
        // Case B: STRUCT column — mid_name is a struct field.
        let Some(mid_resolution) = Self::field_access_resolution(td, mid_name) else {
            return Ok(None);
        };
        let inner_type_name = td
            .find_union_variant(mid_name)
            .map(|(_, variant)| variant.type_name.as_str())
            .or_else(|| {
                td.find_struct_field(mid_name)
                    .map(|(_, field)| field.type_name.as_str())
            })
            .expect("resolved field access has a matching type definition entry");
        let Some(inner_type) = self
            .resolver
            .schema()
            .get_type_def_unchecked(inner_type_name)
        else {
            return Ok(None);
        };
        let Some(leaf_resolution) = Self::field_access_resolution(inner_type, leaf_name) else {
            return Ok(None);
        };

        let nested_expr = ast::Expr::FieldAccess {
            base: Box::new(ast::Expr::FieldAccess {
                base: Box::new(ast::Expr::Column {
                    database: None,
                    table: table_id,
                    column: col_idx,
                    is_rowid_alias,
                }),
                field: ast::Name::from_bytes(mid_name.as_bytes()),
                resolved: Some(mid_resolution),
            }),
            field: ast::Name::from_bytes(leaf_name.as_bytes()),
            resolved: Some(leaf_resolution),
        };
        self.tracking.record_column(table_id, col_idx);
        Ok(Some(nested_expr))
    }

    fn custom_type_expr_definition(
        &self,
        expr: &ast::Expr,
        scope: &BindScope,
    ) -> Option<Arc<crate::schema::TypeDef>> {
        let type_name = match expr {
            ast::Expr::Column { table, column, .. } => {
                let scope_table = scope
                    .tables
                    .iter()
                    .find(|scope_table| scope_table.internal_id == *table)
                    .or_else(|| {
                        self.all_outer_scopes_iter().find_map(|outer_scope| {
                            outer_scope
                                .tables
                                .iter()
                                .find(|scope_table| scope_table.internal_id == *table)
                        })
                    })?;
                let ScopeTableSource::Table(table) = &scope_table.source else {
                    return None;
                };
                table.columns().get(*column)?.ty_str.clone()
            }
            ast::Expr::Variable(variable) => variable.col_type.as_ref()?.to_string(),
            ast::Expr::FieldAccess { base, field, .. } => {
                let parent = self.custom_type_expr_definition(base, scope)?;
                parent
                    .find_struct_field(field.as_str())
                    .map(|(_, field)| field.type_name.clone())
                    .or_else(|| {
                        parent
                            .find_union_variant(field.as_str())
                            .map(|(_, variant)| variant.type_name.clone())
                    })?
            }
            ast::Expr::BoundCustomTypeFunction { resolution, .. } => match resolution {
                ast::CustomTypeFunctionResolution::UnionValue { result_type, .. }
                | ast::CustomTypeFunctionResolution::UnionExtract { result_type, .. }
                | ast::CustomTypeFunctionResolution::StructExtract { result_type, .. } => {
                    result_type.clone()
                }
                ast::CustomTypeFunctionResolution::UnionTag { .. } => return None,
            },
            ast::Expr::FunctionCall { name, args, .. } => {
                let function_name = normalize_ident(name.as_str());
                match function_name.as_str() {
                    "union_extract" if args.len() == 2 => {
                        let ast::Expr::Literal(ast::Literal::String(tag_name)) = args[1].as_ref()
                        else {
                            return None;
                        };
                        let union = self.custom_type_expr_definition(&args[0], scope)?;
                        union
                            .find_union_variant(tag_name.trim_matches('\''))?
                            .1
                            .type_name
                            .clone()
                    }
                    "struct_extract" if args.len() == 2 => {
                        let ast::Expr::Literal(ast::Literal::String(field_name)) = args[1].as_ref()
                        else {
                            return None;
                        };
                        let structure = self.custom_type_expr_definition(&args[0], scope)?;
                        structure
                            .find_struct_field(field_name.trim_matches('\''))?
                            .1
                            .type_name
                            .clone()
                    }
                    _ => return None,
                }
            }
            _ => return None,
        };
        self.resolver
            .schema()
            .get_type_def_unchecked(&type_name)
            .cloned()
    }

    fn bind_custom_type_function_calls(
        &self,
        expr: &mut ast::Expr,
        scope: &BindScope,
        expected_type: Option<&crate::schema::TypeDef>,
    ) -> Result<()> {
        walk_expr_mut(expr, &mut |expr| {
            let ast::Expr::FunctionCall {
                name,
                args,
                order_by,
                within_group,
                filter_over,
                ..
            } = expr
            else {
                return Ok(WalkControl::Continue);
            };
            let function_name = normalize_ident(name.as_str());
            if matches!(
                function_name.as_str(),
                "union_value" | "union_tag" | "union_extract" | "struct_extract"
            ) {
                if !order_by.is_empty() || !within_group.is_empty() {
                    crate::bail_parse_error!(
                        "ORDER BY is not allowed for scalar function {}()",
                        function_name
                    );
                }
                if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
                    crate::bail_parse_error!(
                        "{}() may not be used as an aggregate or window function",
                        function_name
                    );
                }
            }
            let (resolution, children_already_bound) = match function_name.as_str() {
                "union_value" => {
                    let ast::Expr::Literal(ast::Literal::String(tag_name)) = args[0].as_ref()
                    else {
                        unreachable!("union_value literal argument was validated earlier")
                    };
                    let tag_name = tag_name.trim_matches('\'');
                    let union = expected_type
                        .filter(|type_def| type_def.is_union())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "union_value() can only be used in INSERT/UPDATE targeting a union-typed column"
                                    .to_string(),
                            )
                        })?;
                    let (tag_index, variant) =
                        union.find_union_variant(tag_name).ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "unknown variant '{}' in union type '{}'",
                                tag_name, union.name
                            ))
                        })?;
                    let value_type = self
                        .resolver
                        .schema()
                        .get_type_def_unchecked(&variant.type_name);
                    self.bind_custom_type_function_calls(
                        &mut args[1],
                        scope,
                        value_type.map(AsRef::as_ref),
                    )?;
                    (
                        ast::CustomTypeFunctionResolution::UnionValue {
                            tag_index,
                            result_type: union.name.clone(),
                        },
                        true,
                    )
                }
                "union_tag" => {
                    let union = self
                        .custom_type_expr_definition(&args[0], scope)
                        .filter(|type_def| type_def.is_union())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "union_tag() argument must have a known union type".to_string(),
                            )
                        })?;
                    let tag_names = Arc::clone(
                        &union
                            .union_def()
                            .expect("union type must have a union definition")
                            .tag_names,
                    );
                    (
                        ast::CustomTypeFunctionResolution::UnionTag { tag_names },
                        false,
                    )
                }
                "union_extract" => {
                    let ast::Expr::Literal(ast::Literal::String(tag_name)) = args[1].as_ref()
                    else {
                        unreachable!("union_extract literal argument was validated earlier")
                    };
                    let tag_name = tag_name.trim_matches('\'');
                    let union = self
                        .custom_type_expr_definition(&args[0], scope)
                        .filter(|type_def| type_def.is_union())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "union_extract() first argument must have a known union type"
                                    .to_string(),
                            )
                        })?;
                    let (tag_index, variant) =
                        union.find_union_variant(tag_name).ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "unknown variant '{}' in union type '{}'",
                                tag_name, union.name
                            ))
                        })?;
                    (
                        ast::CustomTypeFunctionResolution::UnionExtract {
                            tag_index,
                            result_type: variant.type_name.clone(),
                        },
                        false,
                    )
                }
                "struct_extract" => {
                    let ast::Expr::Literal(ast::Literal::String(field_name)) = args[1].as_ref()
                    else {
                        unreachable!("struct_extract literal argument was validated earlier")
                    };
                    let field_name = field_name.trim_matches('\'');
                    let structure = self
                        .custom_type_expr_definition(&args[0], scope)
                        .filter(|type_def| type_def.is_struct())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "struct_extract() first argument must have a known struct type"
                                    .to_string(),
                            )
                        })?;
                    let (field_index, field) =
                        structure.find_struct_field(field_name).ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "unknown field '{}' in struct type '{}'",
                                field_name, structure.name
                            ))
                        })?;
                    (
                        ast::CustomTypeFunctionResolution::StructExtract {
                            field_index,
                            result_type: field.type_name.clone(),
                        },
                        false,
                    )
                }
                _ => return Ok(WalkControl::Continue),
            };

            let call = Box::new(take_expr(expr));
            *expr = ast::Expr::BoundCustomTypeFunction { call, resolution };
            Ok(if children_already_bound {
                WalkControl::SkipChildren
            } else {
                WalkControl::Continue
            })
        })?;
        Ok(())
    }

    fn bind_identifier(&mut self, expr: &mut ast::Expr, scope: &BindScope) -> Result<()> {
        if self.bind_trigger_column(expr)? {
            return Ok(());
        }

        match expr {
            ast::Expr::Id(id) => {
                let resolved = match self.phase() {
                    BindPhase::NoAliases => self.resolve_unqualified_column(id.as_str(), scope)?,
                    BindPhase::TableFirst => self
                        .resolve_unqualified_column(id.as_str(), scope)?
                        .or_else(|| self.resolve_alias(id.as_str()))
                        .or_else(|| self.resolve_outer_alias(id.as_str())),
                    BindPhase::AliasFirst => {
                        if let Some(alias) = self.resolve_alias(id.as_str()) {
                            // Even though the alias matched, check if the name is
                            // ambiguous as a table column. SQLite errors on
                            // ORDER BY value when multiple tables have 'value'.
                            scope.find_column_unqualified(id.as_str())?;
                            Some(alias)
                        } else {
                            self.resolve_unqualified_column(id.as_str(), scope)?
                                .or_else(|| self.resolve_outer_alias(id.as_str()))
                        }
                    }
                };

                if let Some(resolved) = resolved {
                    *expr = resolved;
                    return Ok(());
                }

                // SQLite DQS misfeature: double-quoted identifiers fall back
                // to string literals only when DQS is enabled.
                if id.quoted_with('"') && self.resolver.dqs_dml.is_enabled() {
                    *expr = ast::Expr::Literal(ast::Literal::String(id.as_literal()));
                } else if self.allow_unbound {
                    // Leave as-is (e.g. EXCLUDED pseudo-table refs in UPSERT)
                } else {
                    crate::bail_parse_error!("no such column: {}", id.as_str());
                }
            }
            ast::Expr::Qualified(tbl, col) => {
                if let Some(resolved) =
                    self.resolve_qualified_column(tbl.as_str(), col.as_str(), scope)?
                {
                    *expr = resolved;
                } else if self.allow_unbound {
                    // Leave as-is (e.g. EXCLUDED.col in UPSERT)
                } else {
                    // Check whether the table itself exists to give a better error.
                    // Also check CTEs and outer scopes — a CTE name that isn't
                    // in FROM still counts as a "known table" for error messages.
                    let tbl_normalized = normalize_ident(tbl.as_str());
                    let table_exists = scope.find_table_by_identifier(tbl.as_str()).is_some()
                        || self.ctes.contains_key(&tbl_normalized)
                        || self
                            .all_outer_scopes_iter()
                            .any(|s| s.find_table_by_identifier(tbl.as_str()).is_some());
                    if table_exists {
                        crate::bail_parse_error!(
                            "no such column: {}.{}",
                            tbl.as_str(),
                            col.as_str()
                        );
                    }
                    // Dot-notation fallback for struct/union field access:
                    // for `a.b`, if no table `a` exists anywhere, try
                    // a=column, b=struct field (table references win).
                    let field_name = normalize_ident(col.as_str());
                    if let Some((table_id, col_idx, is_rowid_alias, td)) =
                        self.find_custom_type_column_in_scope(scope, &tbl_normalized)?
                    {
                        *expr = Self::make_field_access_expr(
                            table_id,
                            col_idx,
                            is_rowid_alias,
                            &field_name,
                            td,
                        )?;
                        self.tracking.record_column(table_id, col_idx);
                        return Ok(());
                    }
                    crate::bail_parse_error!("no such table: {}", tbl_normalized);
                }
            }
            ast::Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                let qname = ast::QualifiedName {
                    db_name: Some(db_name.clone()),
                    name: tbl_name.clone(),
                    alias: None,
                };
                // In trigger context, cross-database DoublyQualified references
                // (e.g. aux.ref_t.v) should not be resolved at compile time.
                // SQLite defers this to runtime with "no such column".
                // We check the db name directly because resolve_database_id
                // would error with "trigger cannot reference objects in database X".
                if let Some(ref ctx) = self.resolver.trigger_context {
                    let db_name_normalized = normalize_ident(db_name.as_str());
                    let trigger_db_name = if ctx.database_id() == crate::MAIN_DB_ID {
                        "main".to_string()
                    } else {
                        self.resolver
                            .get_database_name_by_index(ctx.database_id())
                            .unwrap_or_else(|| "main".to_string())
                            .to_lowercase()
                    };
                    if !db_name_normalized.eq_ignore_ascii_case(&trigger_db_name) {
                        // Cross-database ref in trigger — error at runtime with
                        // "no such column" matching SQLite behavior.
                        crate::bail_parse_error!(
                            "no such column: {}.{}.{}",
                            db_name.as_str(),
                            tbl_name.as_str(),
                            col_name.as_str()
                        );
                    }
                }
                if self.allow_unbound {
                    return Ok(());
                }

                // `a.b.c` resolution order (DuckDB-style precedence, mirrors
                // bind_and_rewrite_expr):
                //   1. a=database, b=table,  c=column
                //   2. a=table,    b=column, c=struct/union field
                //   3. a=column,   b=field,  c=sub-field
                let db_resolution = self.resolver.resolve_database_id(&qname);
                if let Ok(database_id) = db_resolution {
                    // The interpretation only holds if the named database
                    // actually contains the table (mirrors bind_and_rewrite:
                    // `temp.t1.y` must not resolve through a main-schema t1
                    // that happens to be in the FROM clause).
                    let table_in_db = self.resolver.with_schema(database_id, |schema| {
                        schema
                            .get_table(&normalize_ident(tbl_name.as_str()))
                            .is_some()
                    });
                    if table_in_db {
                        if let Some(resolved) = self.resolve_qualified_column(
                            tbl_name.as_str(),
                            col_name.as_str(),
                            scope,
                        )? {
                            match resolved {
                                ast::Expr::Column {
                                    table,
                                    column,
                                    is_rowid_alias,
                                    ..
                                } => {
                                    *expr = ast::Expr::Column {
                                        database: Some(database_id),
                                        table,
                                        column,
                                        is_rowid_alias,
                                    };
                                }
                                other => *expr = other,
                            }
                            return Ok(());
                        }
                    }
                }

                // db.table.column failed — try table.column.field for
                // struct/union access.
                let normalized_tbl_name = normalize_ident(db_name.as_str());
                let normalized_col = normalize_ident(tbl_name.as_str());
                let field_name = normalize_ident(col_name.as_str());
                if let Some(st) = scope.find_table_by_identifier(&normalized_tbl_name) {
                    if let ScopeTableSource::Table(table) = &st.source {
                        let cols = table.columns();
                        if let Some(col_idx) = cols.iter().position(|c| {
                            c.name
                                .as_ref()
                                .is_some_and(|n| n.eq_ignore_ascii_case(&normalized_col))
                        }) {
                            let col = &cols[col_idx];
                            let type_def =
                                self.resolver.schema().get_type_def_unchecked(&col.ty_str);
                            let is_struct_or_union = type_def
                                .map(|td| td.is_struct() || td.is_union())
                                .unwrap_or(false);
                            if is_struct_or_union {
                                let internal_id = st.internal_id;
                                let is_rowid_alias = col.is_rowid_alias();
                                *expr = Self::make_field_access_expr(
                                    internal_id,
                                    col_idx,
                                    is_rowid_alias,
                                    &field_name,
                                    type_def.unwrap(),
                                )?;
                                self.tracking.record_column(internal_id, col_idx);
                                return Ok(());
                            } else {
                                // Column exists but is not a struct/union type
                                return Err(crate::LimboError::ParseError(format!(
                                    "column '{normalized_col}' is not a STRUCT or UNION type; \
                                     cannot access field '{field_name}'"
                                )));
                            }
                        }
                    }
                }

                // Fallback (3): column.field.subfield for nested struct/union
                // access (e.g. data.telegram.chat_id).
                if let Some(nested_expr) = self.try_resolve_nested_field_access_in_scope(
                    scope,
                    &normalized_tbl_name,
                    &normalized_col,
                    &field_name,
                )? {
                    *expr = nested_expr;
                    return Ok(());
                }

                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    db_name.as_str(),
                    tbl_name.as_str(),
                    col_name.as_str()
                );
            }
            _ => unreachable!("bind_identifier only handles identifier nodes"),
        }

        Ok(())
    }

    fn bind_subquery_expr(
        &mut self,
        select: &mut ast::Select,
        scope: &BindScope,
    ) -> Result<BoundSelect> {
        #[expect(clippy::arc_with_non_send_sync)]
        self.append_outer_query_scope(Arc::new(scope.clone()), Arc::clone(&self.aliases));
        let result = self.bind_select(select);
        self.pop_outer_query_scope();
        result
    }

    /// Populate `shared_subqueries` with scalar subqueries that appear in both
    /// GROUP BY and the SELECT list (compared on the raw, pre-bind AST).
    fn collect_shared_subqueries(
        &mut self,
        columns: &[ast::ResultColumn],
        group_by: Option<&ast::GroupBy>,
    ) {
        self.shared_subqueries.clear();
        let Some(group_by) = group_by else {
            return;
        };
        let mut in_group_by: Vec<ast::Expr> = Vec::new();
        for expr in &group_by.exprs {
            let _ = walk_expr(expr, &mut |e: &ast::Expr| {
                if matches!(e, ast::Expr::Subquery(_)) {
                    in_group_by.push(e.clone());
                }
                Ok(WalkControl::Continue)
            });
        }
        if in_group_by.is_empty() {
            return;
        }
        for col in columns {
            let ast::ResultColumn::Expr(expr, _) = col else {
                continue;
            };
            let _ = walk_expr(expr.as_ref(), &mut |e: &ast::Expr| {
                if matches!(e, ast::Expr::Subquery(_)) && in_group_by.contains(e) {
                    self.shared_subqueries.push((e.clone(), None));
                }
                Ok(WalkControl::Continue)
            });
        }
    }

    fn bind_trigger_column(&self, expr: &mut ast::Expr) -> Result<bool> {
        let Some(bindings) = &self.trigger_columns else {
            return Ok(false);
        };
        let (namespace, column) = match expr {
            ast::Expr::Qualified(namespace, column)
            | ast::Expr::DoublyQualified(_, namespace, column) => (
                normalize_ident(namespace.as_str()),
                normalize_ident(column.as_str()),
            ),
            _ => return Ok(false),
        };

        let registers = if namespace.eq_ignore_ascii_case("new") {
            bindings.new_registers.as_deref().ok_or_else(|| {
                crate::LimboError::ParseError(
                    "NEW references are only valid in INSERT and UPDATE triggers".to_string(),
                )
            })?
        } else if namespace.eq_ignore_ascii_case("old") {
            bindings.old_registers.as_deref().ok_or_else(|| {
                crate::LimboError::ParseError(
                    "OLD references are only valid in UPDATE and DELETE triggers".to_string(),
                )
            })?
        } else {
            return Ok(false);
        };

        let register = if let Some((index, column_definition)) = bindings.table.get_column(&column)
        {
            if column_definition.is_rowid_alias() {
                registers.last().copied()
            } else {
                registers.get(index).copied()
            }
        } else if super::planner::ROWID_STRS
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column))
        {
            registers.last().copied()
        } else {
            None
        };

        let Some(register) = register else {
            crate::bail_parse_error!("no such column in {}: {}", namespace.to_uppercase(), column);
        };
        *expr = ast::Expr::Register(register);
        Ok(true)
    }

    /// Bind an expression, resolving column references against the given scope.
    fn bind_expr(&mut self, expr: &mut ast::Expr, scope: &BindScope) -> Result<()> {
        self.bind_expr_with_expected_type(expr, scope, None)
    }

    fn bind_expr_with_expected_type(
        &mut self,
        expr: &mut ast::Expr,
        scope: &BindScope,
        expected_type: Option<&crate::schema::TypeDef>,
    ) -> Result<()> {
        walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
            match expr {
                ast::Expr::Between { .. } => {
                    // Keep BETWEEN's tested expression on the left side of
                    // both comparisons so SQLite collation precedence holds.
                    rewrite_between_node(expr);
                }
                ast::Expr::Id(_)
                | ast::Expr::Qualified(_, _)
                | ast::Expr::DoublyQualified(_, _, _) => {
                    self.bind_identifier(expr, scope)?;
                }
                ast::Expr::Exists(_) => {
                    let subquery_id = self.id_gen.next_table_id();
                    let ast::Expr::Exists(mut select) =
                        std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: None,
                        not_in: false,
                        query_type: ast::SubqueryType::Exists { result_reg: 0 },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                ast::Expr::Subquery(_) => {
                    // Scalar-subquery CSE: an occurrence shared between GROUP BY
                    // and the SELECT list reuses the first occurrence's id so
                    // both point at a single evaluation.
                    let shared_slot = self
                        .shared_subqueries
                        .iter()
                        .position(|(raw, _)| raw == expr);
                    if let Some(slot) = shared_slot {
                        if let (_, Some(existing_id)) = self.shared_subqueries[slot] {
                            let num_regs = self
                                .subquery_bindings
                                .get(&existing_id)
                                .map(|b| b.inner_bound.result_columns.len())
                                .unwrap_or(0);
                            *expr = ast::Expr::SubqueryResult {
                                subquery_id: existing_id,
                                lhs: None,
                                not_in: false,
                                query_type: ast::SubqueryType::RowValue {
                                    result_reg_start: 0,
                                    num_regs,
                                },
                            };
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    let subquery_id = self.id_gen.next_table_id();
                    if let Some(slot) = shared_slot {
                        self.shared_subqueries[slot].1 = Some(subquery_id);
                    }
                    let ast::Expr::Subquery(mut select) =
                        std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    let num_result_cols = inner_bound.result_columns.len();
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: None,
                        not_in: false,
                        query_type: ast::SubqueryType::RowValue {
                            result_reg_start: 0,
                            num_regs: num_result_cols,
                        },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                ast::Expr::InSelect { .. } => {
                    let subquery_id = self.id_gen.next_table_id();
                    let ast::Expr::InSelect {
                        lhs,
                        not,
                        rhs: mut select,
                    } = std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                    else {
                        unreachable!();
                    };
                    // Bind lhs first against the current scope
                    // (already handled by walker for non-subquery children,
                    // but InSelect lhs needs explicit binding since we took ownership)
                    let mut lhs = lhs;
                    self.bind_expr(&mut lhs, scope)?;
                    let inner_bound = self.bind_subquery_expr(&mut select, scope)?;
                    self.subquery_bindings.insert(
                        subquery_id,
                        BoundSubquery {
                            select,
                            inner_bound,
                        },
                    );
                    *expr = ast::Expr::SubqueryResult {
                        subquery_id,
                        lhs: Some(lhs),
                        not_in: not,
                        query_type: ast::SubqueryType::In {
                            cursor_id: 0,
                            affinity_str: Arc::new(String::new()),
                        },
                    };
                    return Ok(WalkControl::SkipChildren);
                }
                // Validate struct/union/array function calls at bind time
                // (arity and literal-argument checks), mirroring
                // bind_and_rewrite_expr.
                ast::Expr::FunctionCall { name, args, .. } => {
                    super::expr::validate_custom_type_function_call(
                        name.as_str(),
                        args,
                        self.resolver,
                    )?;
                }
                ast::Expr::FunctionCallStar { .. } => {
                    self.expand_star_function(expr, scope);
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        self.bind_custom_type_function_calls(expr, scope, expected_type)?;
        Ok(())
    }

    /// Expand `f(*)` for functions that need star expansion (json_object,
    /// jsonb_object) into alternating column-name / column-reference
    /// arguments over the scope's tables. Leaves the call untouched when the
    /// scope has no tables so translation can report the error.
    fn expand_star_function(&mut self, expr: &mut ast::Expr, scope: &BindScope) {
        let ast::Expr::FunctionCallStar { name, filter_over } = expr else {
            return;
        };
        let Ok(Some(func)) = Func::resolve_function(name.as_str(), 0) else {
            return;
        };
        if !func.needs_star_expansion() || scope.tables.is_empty() {
            return;
        }
        let mut args: Vec<Box<ast::Expr>> = Vec::new();
        for st in &scope.tables {
            for col_ref in st.table.columns() {
                if col_ref.is_hidden {
                    continue;
                }
                // Column name as string literal
                let quoted = format!("'{}'", col_ref.name);
                args.push(Box::new(ast::Expr::Literal(ast::Literal::String(quoted))));
                // Column reference
                args.push(Box::new(ast::Expr::Column {
                    database: None,
                    table: st.internal_id,
                    column: col_ref.idx,
                    is_rowid_alias: col_ref.is_rowid_alias,
                }));
                self.tracking.record_column(st.internal_id, col_ref.idx);
            }
        }
        *expr = ast::Expr::FunctionCall {
            name: name.clone(),
            distinctness: None,
            args,
            filter_over: filter_over.clone(),
            order_by: vec![],
            within_group: vec![],
        };
    }

    fn bind_from(&mut self, from: &mut ast::FromClause) -> Result<BindScope> {
        use super::plan::JoinType as PlanJoinType;

        let mut tables: Vec<ScopeTable> = Vec::new();
        let mut right_join_swapped = false;

        tables.push(self.resolve_select_table(&mut from.select, None, 0)?);
        for join in &mut from.joins {
            // Build a temporary scope from tables accumulated so far, so that
            // table function arguments can reference previously-joined tables.
            let lateral_scope = BindScope {
                tables: tables.clone(),
                right_join_swapped: false,
            };
            let mut st =
                self.resolve_select_table(&mut join.table, Some(&lateral_scope), tables.len())?;

            // SQLite allows duplicate table names/aliases in FROM clauses.
            // Ambiguity is detected later during column resolution.

            let (is_outer, is_full_outer, is_right, is_cross, is_natural) = match &join.operator {
                ast::JoinOperator::TypedJoin(Some(jt)) => {
                    let is_left = jt.contains(ast::JoinType::LEFT);
                    let is_right = jt.contains(ast::JoinType::RIGHT);
                    let is_outer = jt.contains(ast::JoinType::OUTER) || is_left;
                    let is_full = (is_left && is_right) || (is_outer && !is_left && !is_right);
                    let is_cross = jt.contains(ast::JoinType::CROSS);
                    let is_natural = jt.contains(ast::JoinType::NATURAL);
                    (
                        is_outer && !is_full,
                        is_full,
                        is_right && !is_left && !is_full,
                        is_cross,
                        is_natural,
                    )
                }
                _ => (false, false, false, false, false),
            };

            // NATURAL JOIN: find common columns and rewrite constraint to USING
            if is_natural {
                if join.constraint.is_some() {
                    crate::bail_parse_error!("a NATURAL join may not have an ON or USING clause");
                }
                // SQLite doesn't use HIDDEN columns for NATURAL joins:
                // https://www3.sqlite.org/src/info/ab09ef427181130b
                // The USING list uses the left table's column name spelling,
                // matching parse_join. No common columns = cross join.
                let right_table: &dyn BindTable = st.table.as_ref();
                let mut common_cols: Vec<ast::Name> = Vec::new();
                for right_col in right_table.columns() {
                    if right_col.is_hidden {
                        continue;
                    }
                    let mut found: Option<String> = None;
                    for left_st in &tables {
                        let left_table: &dyn BindTable = left_st.table.as_ref();
                        for left_col in left_table.columns() {
                            if left_col.is_hidden {
                                continue;
                            }
                            if left_col.name.eq_ignore_ascii_case(right_col.name) {
                                found = Some(left_col.name.to_string());
                                break;
                            }
                        }
                        if found.is_some() {
                            break;
                        }
                    }
                    if let Some(left_name) = found {
                        common_cols.push(ast::Name::exact(left_name));
                    }
                }
                if !common_cols.is_empty() {
                    join.constraint = Some(JoinConstraint::Using(common_cols));
                }
            }

            // Determine USING columns from (possibly rewritten) constraint
            let using_cols = match &join.constraint {
                Some(JoinConstraint::Using(cols)) => cols.to_vec(),
                _ => vec![],
            };

            // RIGHT JOIN: rewrite as LEFT JOIN by swapping tables.
            // Push the right table first, then swap it to the front so it
            // becomes the driving table. The originally-left table gets the
            // LeftOuter join info.
            if is_right {
                if tables.len() > 1 {
                    crate::bail_parse_error!(
                        "RIGHT JOIN following another join is not yet supported. \
                         Try rewriting as LEFT JOIN or using a subquery."
                    );
                }
                // Push right table, then swap so it's first
                tables.push(st);
                let last = tables.len() - 1;
                tables.swap(0, last);
                // The originally-left table (now at last position) gets the outer flag
                tables[last].join_info = Some(JoinInfo {
                    join_type: PlanJoinType::LeftOuter,
                    using: using_cols.clone(),
                    no_reorder: false,
                });
                right_join_swapped = true;
            } else {
                let plan_join_type = if is_full_outer {
                    PlanJoinType::FullOuter
                } else if is_outer {
                    PlanJoinType::LeftOuter
                } else {
                    PlanJoinType::Inner
                };
                st.join_info = Some(JoinInfo {
                    join_type: plan_join_type,
                    using: using_cols,
                    no_reorder: is_cross,
                });
                tables.push(st);
            }
        }

        let mut scope = BindScope {
            tables,
            right_join_swapped,
        };
        self.bind_index_method_patterns(&mut scope)?;
        self.bind_index_expressions(&mut scope);

        // Bind ON expressions against the complete scope
        for join in &mut from.joins {
            match &mut join.constraint {
                Some(JoinConstraint::On(expr)) => {
                    self.bind_expr(expr, &scope)?;
                }
                // USING column usage is marked by fold_join_constraints when
                // the equality predicates are synthesized, matching parse_join.
                Some(JoinConstraint::Using(_)) | None => {}
            }
        }

        // Second pass: re-bind table function args that may have been left
        // unresolved due to forward references (e.g. FROM func(s.col), s).
        // Now that all tables are in scope, resolve any remaining Expr::Qualified/Id.
        if let ast::SelectTable::TableCall(_, args, _) = from.select.as_mut() {
            for arg in args.iter_mut() {
                self.bind_expr(arg, &scope)?;
            }
        }
        for join in &mut from.joins {
            if let ast::SelectTable::TableCall(_, args, _) = join.table.as_mut() {
                for arg in args.iter_mut() {
                    self.bind_expr(arg, &scope)?;
                }
            }
        }

        Ok(scope)
    }

    /// Resolve every custom index-method pattern against the table reference
    /// it may optimize. The optimizer receives only this bound form.
    fn bind_index_method_patterns(&mut self, scope: &mut BindScope) -> Result<()> {
        for scope_table in &mut scope.tables {
            let ScopeTableSource::Table(table) = &scope_table.source else {
                continue;
            };
            let indexes = self
                .resolver
                .with_schema(scope_table.database_id, |schema| {
                    schema.indexes.get(table.get_name()).cloned()
                });
            let Some(indexes) = indexes else {
                continue;
            };

            let mut bound_patterns = Vec::new();
            for index in indexes {
                let Some(index_method) = &index.index_method else {
                    continue;
                };
                if index.is_backing_btree_index() {
                    continue;
                }
                let raw_patterns = index_method.definition().patterns.to_vec();
                for (pattern_idx, raw_pattern) in raw_patterns.iter().enumerate() {
                    bound_patterns.push(self.bind_index_method_pattern(
                        raw_pattern,
                        scope_table,
                        index.name.clone(),
                        pattern_idx,
                    )?);
                }
            }
            scope_table.bound_index_method_patterns = bound_patterns;
        }
        Ok(())
    }

    fn bind_index_expressions(&self, scope: &mut BindScope) {
        for scope_table in &mut scope.tables {
            let ScopeTableSource::Table(table) = &scope_table.source else {
                continue;
            };
            scope_table.bound_index_expressions = bind_table_index_expressions(
                self.resolver,
                scope_table.database_id,
                table.get_name(),
                scope_table.internal_id,
            );
        }
    }

    fn bind_index_method_pattern(
        &mut self,
        raw_pattern: &ast::Select,
        target: &ScopeTable,
        index_name: String,
        pattern_idx: usize,
    ) -> Result<super::plan::BoundIndexMethodPattern> {
        let mut pattern = raw_pattern.clone();
        if pattern.with.is_some() || !pattern.body.compounds.is_empty() {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' must be a single SELECT"
            )));
        }

        let ast::OneSelect::Select {
            columns,
            from: Some(ast::FromClause { select, joins }),
            distinctness: None,
            where_clause,
            group_by: None,
            window_clause,
        } = &mut pattern.body.select
        else {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' has an unsupported SELECT body"
            )));
        };
        if !joins.is_empty() || !window_clause.is_empty() {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' cannot contain joins or windows"
            )));
        }
        let ast::SelectTable::Table(pattern_table_name, _, _) = select.as_ref() else {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' must read one table"
            )));
        };
        let ScopeTableSource::Table(target_table) = &target.source else {
            unreachable!("index method patterns only belong to schema tables")
        };
        let target_table_name = target_table.get_name();
        if !pattern_table_name
            .name
            .as_str()
            .eq_ignore_ascii_case(target_table_name)
        {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' reads '{}', expected '{target_table_name}'",
                pattern_table_name.name.as_str()
            )));
        }

        let mut pattern_table = target.clone();
        pattern_table.identifier = normalize_ident(pattern_table_name.name.as_str());
        pattern_table.bound_index_method_patterns.clear();
        let pattern_scope = BindScope {
            tables: vec![pattern_table],
            right_join_swapped: false,
        };

        let mut binder = BindContext::new(self.resolver, &mut *self.id_gen);
        let aliases = Arc::new(binder.extract_bound_columns(columns, &pattern_scope, &[])?);
        binder.set_aliases(Arc::clone(&aliases));
        binder.with_phase(BindPhase::NoAliases, |binder| {
            binder.bind_select_list(columns, &pattern_scope)
        })?;
        if let Some(where_clause) = where_clause {
            binder.with_phase(BindPhase::AliasFirst, |binder| {
                binder.bind_expr(where_clause, &pattern_scope)
            })?;
        }
        binder.with_phase(BindPhase::AliasFirst, |binder| {
            for order_by in &mut pattern.order_by {
                binder.bind_expr(&mut order_by.expr, &pattern_scope)?;
            }
            Ok(())
        })?;
        if let Some(limit) = pattern.limit.as_mut() {
            let empty_scope = BindScope::empty();
            binder.bind_expr(&mut limit.expr, &empty_scope)?;
            if let Some(offset) = limit.offset.as_mut() {
                binder.bind_expr(offset, &empty_scope)?;
            }
        }
        if !binder.subquery_bindings.is_empty() || !binder.derived_bindings.is_empty() {
            return Err(crate::LimboError::InternalError(format!(
                "index method pattern {pattern_idx} for '{index_name}' cannot contain subqueries"
            )));
        }
        drop(binder);

        let ast::OneSelect::Select {
            columns,
            where_clause,
            ..
        } = pattern.body.select
        else {
            unreachable!("index method pattern shape was validated above")
        };
        Ok(super::plan::BoundIndexMethodPattern {
            index_name,
            pattern_idx,
            columns,
            where_clause,
            order_by: pattern.order_by,
            limit: pattern.limit,
        })
    }

    // ── UPDATE binding ──────────────────────────────────────────────────

    /// Bind an UPDATE statement, resolving all name references in-place.
    pub fn bind_update(
        &mut self,
        update: &mut ast::Update,
        database_id: usize,
        table: Arc<Table>,
    ) -> Result<BoundUpdate> {
        let or_conflict = update.or_conflict.take();
        self.with_query(|ctx| {
            // 1. Bind CTEs from WITH clause
            if let Some(with) = &mut update.with {
                ctx.bind_cte(with)?;
            }

            // 2. Build scope with target table
            let target_scope = ctx.build_table_scope(
                &update.tbl_name.name,
                update.tbl_name.alias.as_ref(),
                update.indexed.clone(),
                database_id,
            )?;

            // 3. Bind the UPDATE ... FROM clause (if present). Its JOIN ON
            //    constraints bind against the FROM tables only — they cannot
            //    reference the target table.
            let from_scope = match update.from.as_mut() {
                Some(from) => Some(ctx.bind_from(from)?),
                None => None,
            };

            // 4. Merge target + FROM tables into the read scope used by SET
            //    and WHERE expressions.
            let mut read_scope = target_scope.clone();
            if let Some(fs) = &from_scope {
                read_scope.tables.extend(fs.tables.iter().cloned());
                read_scope.right_join_swapped = fs.right_join_swapped;
            }

            // 5. Bind SET expressions (column-name → index mapping stays in
            //    the planner via collect_update_set_clauses).
            ctx.with_phase(BindPhase::NoAliases, |ctx| {
                for set in update.sets.iter_mut() {
                    bind_update_set(ctx, set, &read_scope, &table)?;
                }
                Ok(())
            })?;

            // 6. Bind WHERE clause against the merged scope
            if let Some(where_expr) = &mut update.where_clause {
                ctx.with_phase(BindPhase::NoAliases, |ctx| {
                    ctx.bind_expr(where_expr, &read_scope)
                })?;
            }

            // 7. Bind RETURNING. SQLite resolves RETURNING columns for an
            //    aliased UPDATE target through the base table name, not the
            //    alias, and FROM tables are not visible.
            let mut returning_scope = target_scope.clone();
            returning_scope.tables[0].identifier = normalize_ident(update.tbl_name.name.as_str());
            ctx.bind_returning(&mut update.returning, &returning_scope)?;

            // 8. Bind ORDER BY
            for sort_col in &mut update.order_by {
                ctx.bind_expr(&mut sort_col.expr, &read_scope)?;
            }

            // 9. Bind LIMIT/OFFSET
            if let Some(limit) = update.limit.as_mut() {
                let empty = BindScope::empty();
                ctx.bind_expr(&mut limit.expr, &empty)?;
                if let Some(offset) = limit.offset.as_mut() {
                    ctx.bind_expr(offset, &empty)?;
                }
            }

            // 10. Extract CTE definitions in definition order (critical for
            //     referenced_cte_indices correctness).
            let cte_definitions: Vec<(String, CteEntry)> = if let Some(with) = &update.with {
                let mut ctes = std::mem::take(&mut ctx.ctes);
                with.ctes
                    .iter()
                    .filter_map(|cte| {
                        let name = normalize_ident(cte.tbl_name.as_str());
                        ctes.remove(&name).map(|entry| (name, entry))
                    })
                    .collect()
            } else {
                vec![]
            };

            Ok(BoundUpdate {
                target_scope,
                from_scope,
                tracking: std::mem::take(&mut ctx.tracking),
                subquery_bindings: std::mem::take(&mut ctx.subquery_bindings),
                derived_bindings: std::mem::take(&mut ctx.derived_bindings),
                cte_definitions,
                database_id,
                table,
                or_conflict,
            })
        })
    }

    /// Build a single-table scope for DML statements (UPDATE, DELETE).
    /// `database_id` specifies which attached database to search (0 = main).
    ///
    /// DML targets are always schema tables — a WITH-clause CTE never shadows
    /// the target (matching SQLite, where `WITH t AS (...) DELETE FROM t`
    /// modifies the real table t).
    fn build_table_scope(
        &mut self,
        table_name: &ast::Name,
        alias: Option<&ast::Name>,
        indexed: Option<ast::Indexed>,
        database_id: usize,
    ) -> Result<BindScope> {
        let normalized = normalize_ident(table_name.as_str());
        let identifier = alias
            .map(|a| normalize_ident(a.as_str()))
            .unwrap_or_else(|| normalized.clone());

        // Schema lookup (uses the specified database for attached DB support)
        let schema_table = self
            .resolver
            .with_schema(database_id, |s| s.get_table(&normalized))
            .ok_or_else(|| crate::LimboError::ParseError(format!("no such table: {normalized}")))?;

        let mut scope = BindScope {
            tables: vec![ScopeTable {
                identifier,
                internal_id: self.id_gen.next_table_id(),
                source: ScopeTableSource::Table(schema_table.clone()),
                table: schema_table,
                join_info: None,
                database_id,
                indexed,
                bound_index_method_patterns: Vec::new(),
                bound_index_expressions: Vec::new(),
            }],
            right_join_swapped: false,
        };
        self.bind_index_method_patterns(&mut scope)?;
        self.bind_index_expressions(&mut scope);
        Ok(scope)
    }

    /// Bind a RETURNING clause: expand stars, bind expressions, return bound columns.
    fn bind_returning(
        &mut self,
        returning: &mut Vec<ast::ResultColumn>,
        scope: &BindScope,
    ) -> Result<Vec<BoundColumn>> {
        if returning.is_empty() {
            return Ok(vec![]);
        }

        // Expand Star/TableStar in RETURNING
        self.expand_stars(returning, scope)?;

        let mut result = Vec::with_capacity(returning.len());
        for rc in returning.iter_mut() {
            match rc {
                ast::ResultColumn::Expr(expr, alias) => {
                    self.bind_expr(expr, scope)?;
                    let name = alias
                        .as_ref()
                        .map(|a| a.name().as_str().to_string())
                        .unwrap_or_else(|| Self::infer_column_name(expr));
                    result.push(BoundColumn {
                        name,
                        expr: expr.as_ref().clone(),
                        is_explicit_alias: alias.is_some(),
                    });
                }
                ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {
                    unreachable!("Star/TableStar should be expanded before binding RETURNING")
                }
            }
        }

        Ok(result)
    }

    // ── DELETE binding ──────────────────────────────────────────────────

    /// Bind a DELETE statement, resolving all name references in-place.
    pub fn bind_delete(
        &mut self,
        tbl_name: &ast::QualifiedName,
        indexed: Option<ast::Indexed>,
        where_clause: &mut Option<Box<ast::Expr>>,
        returning: &mut Vec<ast::ResultColumn>,
        with: &mut Option<ast::With>,
        database_id: usize,
        table: Arc<Table>,
    ) -> Result<BoundDelete> {
        self.with_query(|ctx| {
            // 1. Bind CTEs from WITH clause
            if let Some(with) = with.as_mut() {
                ctx.bind_cte(with)?;
            }

            // 2. Build scope with target table
            let scope = ctx.build_table_scope(
                &tbl_name.name,
                tbl_name.alias.as_ref(),
                indexed,
                database_id,
            )?;

            // 3. Bind WHERE clause
            if let Some(where_expr) = where_clause.as_mut() {
                ctx.with_phase(BindPhase::NoAliases, |ctx| {
                    ctx.bind_expr(where_expr, &scope)
                })?;
            }

            // 4. Bind RETURNING (expand stars, bind exprs)
            ctx.bind_returning(returning, &scope)?;

            // 5. Extract CTE definitions in definition order
            let cte_definitions: Vec<(String, CteEntry)> = if let Some(with) = with.as_ref() {
                let mut ctes = std::mem::take(&mut ctx.ctes);
                with.ctes
                    .iter()
                    .filter_map(|cte| {
                        let name = normalize_ident(cte.tbl_name.as_str());
                        ctes.remove(&name).map(|entry| (name, entry))
                    })
                    .collect()
            } else {
                vec![]
            };

            Ok(BoundDelete {
                scope,
                tracking: std::mem::take(&mut ctx.tracking),
                subquery_bindings: std::mem::take(&mut ctx.subquery_bindings),
                cte_definitions,
                database_id,
                table,
            })
        })
    }

    // ── Trigger WHEN binding ────────────────────────────────────────────

    /// Bind NEW/OLD references and subqueries in a trigger WHEN clause.
    /// Top-level identifiers are either resolved here or rejected. DQS string
    /// fallback is also completed here so emission never binds raw SQL names.
    pub fn bind_trigger_when(
        &mut self,
        expr: &mut ast::Expr,
        table: Arc<BTreeTable>,
        new_registers: Option<&[usize]>,
        old_registers: Option<&[usize]>,
    ) -> Result<HashMap<ast::TableInternalId, BoundSubquery>> {
        let saved_trigger_columns = self.trigger_columns.take();
        self.trigger_columns = Some(TriggerColumnBindings {
            table,
            new_registers: new_registers.map(<[usize]>::to_vec),
            old_registers: old_registers.map(<[usize]>::to_vec),
        });

        let result = self.with_query(|ctx| {
            let scope = BindScope::empty();
            ctx.with_phase(BindPhase::NoAliases, |ctx| {
                walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
                    match e {
                        ast::Expr::Exists(_)
                        | ast::Expr::Subquery(_)
                        | ast::Expr::InSelect { .. } => {
                            ctx.bind_expr(e, &scope)?;
                            Ok(WalkControl::SkipChildren)
                        }
                        ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                            if !ctx.bind_trigger_column(e)? {
                                let (namespace, column) = match e {
                                    ast::Expr::Qualified(namespace, column)
                                    | ast::Expr::DoublyQualified(_, namespace, column) => {
                                        (namespace.as_str(), column.as_str())
                                    }
                                    _ => unreachable!(),
                                };
                                crate::bail_parse_error!(
                                    "no such column: {}.{}",
                                    namespace,
                                    column
                                );
                            }
                            Ok(WalkControl::Continue)
                        }
                        ast::Expr::Id(name) => {
                            let identifier = normalize_ident(name.as_str());
                            let trigger_table = &ctx
                                .trigger_columns
                                .as_ref()
                                .expect("trigger columns must be set")
                                .table;
                            if trigger_table.get_column(&identifier).is_some()
                                || super::planner::ROWID_STRS
                                    .iter()
                                    .any(|name| name.eq_ignore_ascii_case(&identifier))
                            {
                                crate::bail_parse_error!("no such column: {}", identifier);
                            }
                            if name.quoted_with('"') && ctx.resolver.dqs_dml.is_enabled() {
                                *e = ast::Expr::Literal(ast::Literal::String(name.as_literal()));
                                Ok(WalkControl::Continue)
                            } else {
                                crate::bail_parse_error!("no such column: {}", identifier);
                            }
                        }
                        _ => Ok(WalkControl::Continue),
                    }
                })
            })?;
            Ok(std::mem::take(&mut ctx.subquery_bindings))
        });

        self.trigger_columns = saved_trigger_columns;
        result
    }

    // ── INSERT binding ──────────────────────────────────────────────────

    /// Bind an INSERT statement's RETURNING clause (with its WITH-clause CTEs
    /// for subquery resolution). The caller supplies the internal id it
    /// already allocated for the target table so the bound column references
    /// line up with its `TableReferences`.
    #[allow(clippy::type_complexity)]
    fn bind_insert_returning(
        &mut self,
        tbl_name: &ast::Name,
        target_internal_id: TableInternalId,
        returning: &mut Vec<ast::ResultColumn>,
        with: &mut Option<ast::With>,
        database_id: usize,
    ) -> Result<(
        Vec<(String, CteEntry)>,
        HashMap<ast::TableInternalId, BoundSubquery>,
    )> {
        self.with_query(|ctx| {
            // 1. Bind CTEs from the WITH clause
            if let Some(with) = with.as_mut() {
                ctx.bind_cte(with)?;
            }

            // 2. Build the target scope with the caller-provided id.
            // RETURNING always resolves through the schema table name.
            let normalized = normalize_ident(tbl_name.as_str());
            let schema_table = ctx
                .resolver
                .with_schema(database_id, |s| s.get_table(&normalized))
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such table: {normalized}"))
                })?;
            let scope = BindScope {
                tables: vec![ScopeTable {
                    identifier: normalized,
                    internal_id: target_internal_id,
                    source: ScopeTableSource::Table(schema_table.clone()),
                    table: schema_table,
                    join_info: None,
                    database_id,
                    indexed: None,
                    bound_index_method_patterns: Vec::new(),
                    bound_index_expressions: Vec::new(),
                }],
                right_join_swapped: false,
            };

            // 3. Bind RETURNING (expand stars, bind exprs)
            ctx.bind_returning(returning, &scope)?;

            // 4. Extract CTE definitions in definition order
            let cte_definitions: Vec<(String, CteEntry)> = if let Some(with) = with.as_ref() {
                let mut ctes = std::mem::take(&mut ctx.ctes);
                with.ctes
                    .iter()
                    .filter_map(|cte| {
                        let name = normalize_ident(cte.tbl_name.as_str());
                        ctes.remove(&name).map(|entry| (name, entry))
                    })
                    .collect()
            } else {
                vec![]
            };

            Ok((cte_definitions, std::mem::take(&mut ctx.subquery_bindings)))
        })
    }

    /// Infer a column name from an expression (for RETURNING without alias).
    fn infer_column_name(expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Column {
                database: _,
                table: _,
                column: _,
                ..
            } => {
                // After binding, we can't easily recover the original name.
                // The caller should use the alias or fall back to the original text.
                String::new()
            }
            ast::Expr::Id(name) => name.as_str().to_string(),
            ast::Expr::Qualified(_, name) => name.as_str().to_string(),
            _ => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{BTreeTable, Schema};
    use crate::{DatabaseCatalog, RwLock, SymbolTable};
    use turso_parser::ast::{Cmd, Stmt};
    use turso_parser::parser::Parser;

    #[derive(Default)]
    struct TestIdGenerator {
        next: usize,
    }

    impl IdGenerator for TestIdGenerator {
        fn next_table_id(&mut self) -> TableInternalId {
            let id = self.next;
            self.next += 1;
            id.into()
        }

        fn next_cte_id(&mut self) -> usize {
            let id = self.next;
            self.next += 1;
            id
        }
    }

    fn parse_select(sql: &str) -> ast::Select {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser
            .next_cmd()
            .expect("SQL should parse")
            .expect("SQL should contain a statement");
        match cmd {
            Cmd::Stmt(Stmt::Select(select)) => select,
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    fn with_bind_context<T>(
        table_ddls: &[&str],
        f: impl FnOnce(&mut BindContext<'_, TestIdGenerator>) -> T,
    ) -> T {
        let mut schema = Schema::new();
        for (idx, ddl) in table_ddls.iter().enumerate() {
            schema
                .add_btree_table(Arc::new(
                    BTreeTable::from_sql(ddl, (idx + 2) as i64).expect("table DDL should parse"),
                ))
                .expect("table should be added to schema");
        }

        with_schema_bind_context(&schema, false, f)
    }

    fn with_schema_bind_context<T>(
        schema: &Schema,
        enable_custom_types: bool,
        f: impl FnOnce(&mut BindContext<'_, TestIdGenerator>) -> T,
    ) -> T {
        let database_schemas = RwLock::new(HashMap::default());
        let temp_database = RwLock::new(None);
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let symbol_table = SymbolTable::new();
        let resolver = Resolver::new(
            &schema,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &symbol_table,
            enable_custom_types,
            crate::translate::emitter::DoubleQuotedDml::Enabled,
            crate::sync::Arc::new(crate::dialect::SqliteDialect),
        );
        let mut id_gen = TestIdGenerator::default();
        let mut ctx = BindContext::new(&resolver, &mut id_gen);
        f(&mut ctx)
    }

    #[test]
    fn nested_custom_field_access_is_fully_bound() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE telegram_msg AS STRUCT(chat_id INT, text TEXT)")
            .unwrap();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram telegram_msg, slack TEXT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE msgs(id INT, data platform) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let mut select = parse_select("SELECT data.telegram.chat_id FROM msgs");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::FieldAccess {
                base,
                resolved: Some(ast::FieldAccessResolution::StructField { field_index: 0 }),
                ..
            } = select_expr(&select, 0)
            else {
                panic!("expected bound struct field access");
            };
            let ast::Expr::FieldAccess {
                base,
                resolved: Some(ast::FieldAccessResolution::UnionVariant { tag_index: 0 }),
                ..
            } = base.as_ref()
            else {
                panic!("expected bound union variant access");
            };
            assert_column_expr(base, 0, 1);
        });
    }

    #[test]
    fn custom_type_extraction_functions_are_fully_bound() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE telegram_msg AS STRUCT(chat_id INT, text TEXT)")
            .unwrap();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram telegram_msg, slack TEXT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE msgs(id INT, data platform) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let mut select = parse_select(
                "SELECT union_tag(data), \
                 struct_extract(union_extract(data, 'telegram'), 'chat_id') FROM msgs",
            );
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::BoundCustomTypeFunction {
                resolution: ast::CustomTypeFunctionResolution::UnionTag { tag_names },
                ..
            } = select_expr(&select, 0)
            else {
                panic!("expected bound union_tag call");
            };
            assert_eq!(tag_names.as_ref(), ["telegram", "slack"]);

            let ast::Expr::BoundCustomTypeFunction {
                call,
                resolution: ast::CustomTypeFunctionResolution::StructExtract { field_index: 0, .. },
            } = select_expr(&select, 1)
            else {
                panic!("expected bound struct_extract call");
            };
            let ast::Expr::FunctionCall { args, .. } = call.as_ref() else {
                panic!("expected wrapped struct_extract call");
            };
            assert!(matches!(
                args[0].as_ref(),
                ast::Expr::BoundCustomTypeFunction {
                    resolution: ast::CustomTypeFunctionResolution::UnionExtract {
                        tag_index: 0,
                        ..
                    },
                    ..
                }
            ));
        });
    }

    #[test]
    fn invalid_union_extract_fails_during_binding() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram TEXT, slack TEXT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE msgs(id INT, data platform) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let error = bind_select_error(ctx, "SELECT union_extract(data, 'discord') FROM msgs")
                .to_string();
            assert!(
                error.contains("unknown variant 'discord' in union type 'platform'"),
                "unexpected error: {error}"
            );
        });
    }

    #[test]
    fn invalid_custom_field_access_fails_during_binding() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE point AS STRUCT(x INT, y INT)")
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE points(id INT, pos point) STRICT", 2).unwrap(),
            ))
            .unwrap();

        with_schema_bind_context(&schema, true, |ctx| {
            let error = bind_select_error(ctx, "SELECT pos.z FROM points").to_string();
            assert!(
                error.contains("no such field 'z' in struct type 'point'"),
                "unexpected error: {error}"
            );
        });
    }

    fn select_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { columns, .. } => match &columns[idx] {
                ast::ResultColumn::Expr(expr, _) => expr,
                other => panic!("expected expression result column, got {other:?}"),
            },
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn where_expr(select: &ast::Select) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { where_clause, .. } => where_clause
                .as_deref()
                .expect("expected WHERE clause on bound select"),
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn group_by_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { group_by, .. } => {
                &group_by.as_ref().expect("expected GROUP BY clause").exprs[idx]
            }
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn having_expr(select: &ast::Select) -> &ast::Expr {
        match &select.body.select {
            ast::OneSelect::Select { group_by, .. } => group_by
                .as_ref()
                .and_then(|group_by| group_by.having.as_deref())
                .expect("expected HAVING clause"),
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    fn order_by_expr(select: &ast::Select, idx: usize) -> &ast::Expr {
        &select.order_by[idx].expr
    }

    fn exists_subquery_id(select: &ast::Select) -> TableInternalId {
        match where_expr(select) {
            ast::Expr::SubqueryResult { subquery_id, .. } => *subquery_id,
            other => panic!("expected SubqueryResult in WHERE, got {other:?}"),
        }
    }

    fn subquery_id_from_expr(expr: &ast::Expr) -> TableInternalId {
        match expr {
            ast::Expr::SubqueryResult { subquery_id, .. } => *subquery_id,
            other => panic!("expected SubqueryResult expression, got {other:?}"),
        }
    }

    fn assert_column_expr(expr: &ast::Expr, table: usize, column: usize) {
        assert_eq!(
            expr,
            &ast::Expr::Column {
                database: None,
                table: TableInternalId::from(table),
                column,
                is_rowid_alias: false,
            }
        );
    }

    fn bind_select_error(
        ctx: &mut BindContext<'_, TestIdGenerator>,
        sql: &str,
    ) -> crate::LimboError {
        let mut select = parse_select(sql);
        match ctx.bind_select(&mut select) {
            Ok(_) => panic!("expected bind failure for SQL: {sql}"),
            Err(err) => err,
        }
    }

    #[test]
    fn bind_select_returns_main_scope_and_tracking() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT b FROM t WHERE a = 1 ORDER BY b");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "t");
            assert_eq!(
                bound.main_scope.tables[0].internal_id,
                TableInternalId::from(0usize)
            );
            assert_eq!(bound.tracking.columns_used.len(), 2);
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0)));
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 1)));
        });
    }

    #[test]
    fn bind_select_keeps_subquery_tracking_out_of_outer_tracking() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.b = a)");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(
                bound.tracking.columns_used,
                vec![(TableInternalId::from(0usize), 0)]
            );
            assert!(bound.tracking.outer_refs_used.is_empty());
        });
    }

    #[test]
    fn bound_select_into_table_references_populates_joined_tables() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT b FROM t WHERE a = 1");
            let bound = ctx.bind_select(&mut select).unwrap();
            let mut all_refs = bound
                .into_table_references_with_outer_refs(
                    &mut HashMap::default(),
                    &mut HashMap::default(),
                    Vec::new(),
                )
                .unwrap();

            assert_eq!(all_refs.len(), 1);
            let table_references = all_refs.remove(0);
            assert_eq!(table_references.joined_tables().len(), 1);
            let table = &table_references.joined_tables()[0];
            assert_eq!(table.identifier, "t");
            assert_eq!(table.internal_id, TableInternalId::from(0usize));
            assert_eq!(table.table.get_name(), "t");
            assert!(table.col_used_mask.get(0));
            assert!(table.col_used_mask.get(1));
            assert!(table_references.outer_query_refs().is_empty());
        });
    }

    #[test]
    fn bind_cte_uses_bound_select_result_columns() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select =
                parse_select("WITH cte(col_x, col_y) AS (SELECT x, y FROM t) SELECT * FROM cte");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("cte").expect("cte should exist");
            assert_eq!(cte.resolved_columns, vec!["col_x", "col_y"]);
        });
    }

    #[test]
    fn bind_cte_allocates_cte_id_and_stores_inner_bound() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select =
                parse_select("WITH c AS (SELECT a, b FROM t WHERE a > 1) SELECT * FROM c");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("c").expect("cte should exist");
            // cte_id was allocated
            assert_eq!(cte.cte_id, 0);
            // inner_bound is populated
            assert!(cte.inner_bound.is_some());
            let inner = cte.inner_bound.as_ref().unwrap();
            assert_eq!(inner.main_scope.tables.len(), 1);
            assert_eq!(inner.main_scope.tables[0].identifier, "t");
            // resolved columns inferred from SELECT list
            assert_eq!(cte.resolved_columns, vec!["a", "b"]);
        });
    }

    #[test]
    fn bind_cte_tracks_referenced_cte_indices() {
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut select =
                parse_select("WITH a AS (SELECT x FROM t), b AS (SELECT * FROM a) SELECT * FROM b");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let a = ctx.get_cte("a").expect("cte a should exist");
            assert!(a.referenced_cte_indices.is_empty());

            let b = ctx.get_cte("b").expect("cte b should exist");
            assert_eq!(b.referenced_cte_indices.as_slice(), &[0]);
        });
    }

    #[test]
    fn bind_cte_materialize_hint() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select =
                parse_select("WITH c AS MATERIALIZED (SELECT a FROM t) SELECT * FROM c");
            let with = select.with.as_mut().expect("expected WITH clause");
            ctx.bind_cte(with).unwrap();

            let cte = ctx.get_cte("c").expect("cte should exist");
            assert!(cte.materialize_hint);
        });
    }

    #[test]
    fn select_list_uses_no_aliases_phase() {
        with_bind_context(&["CREATE TABLE t(x, a)"], |ctx| {
            let mut select = parse_select("SELECT a AS x, x FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_column_expr(select_expr(&select, 0), 0, 1);
            assert_column_expr(select_expr(&select, 1), 0, 0);
            assert_eq!(bound.result_columns[0].name, "x");
            assert_eq!(bound.result_columns[1].name, "x");
        });
    }

    #[test]
    fn where_clause_prefers_table_column_over_alias() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a AS b FROM t WHERE b = 1");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(&select) else {
                panic!("expected bound WHERE binary expression");
            };
            assert_column_expr(lhs, 0, 1);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
        });
    }

    #[test]
    fn where_clause_falls_back_to_alias_when_no_table_column_matches() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS x FROM t WHERE x = 3");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(&select) else {
                panic!("expected bound WHERE binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("3".into()))
            );
        });
    }

    #[test]
    fn group_by_prefers_source_column_over_alias() {
        // In GROUP BY, real columns take precedence over SELECT aliases
        // (matches SQLite): `b` resolves to column t.b, not alias (a + 1).
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t GROUP BY b");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(group_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn having_prefers_alias_expression_over_table_column() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t GROUP BY c HAVING b > 10");
            ctx.bind_select(&mut select).unwrap();

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&select) else {
                panic!("expected bound HAVING binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
        });
    }

    #[test]
    fn order_by_prefers_alias_expression_over_table_column() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1 AS b FROM t ORDER BY b");
            ctx.bind_select(&mut select).unwrap();

            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
        });
    }

    #[test]
    fn order_by_falls_back_to_main_scope_column_when_alias_is_missing() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a AS renamed FROM t ORDER BY b");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_column_expr(order_by_expr(&select, 0), 0, 1);
            assert_eq!(bound.tracking.columns_used.len(), 2);
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0)));
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 1)));
        });
    }

    #[test]
    fn correlated_grouped_subquery_binds_inner_aliases_and_outer_references() {
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select(
                "SELECT t.a \
                 FROM t \
                 WHERE EXISTS (\
                    SELECT u.c + 2 AS a \
                    FROM u \
                    WHERE u.b = t.a \
                    GROUP BY a \
                    HAVING a > t.a \
                    ORDER BY a\
                 )",
            );
            let bound = ctx.bind_select(&mut select).unwrap();
            let sq_id = exists_subquery_id(&select);
            let subquery = &bound.subquery_bindings[&sq_id].select;

            assert_eq!(
                bound.tracking.columns_used,
                vec![(TableInternalId::from(0usize), 0)]
            );
            assert!(bound.tracking.outer_refs_used.is_empty());

            // t=0, subquery_id=1, u=2
            assert_eq!(
                select_expr(subquery, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(2usize),
                        column: 1,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("2".into())).into_boxed(),
                )
            );

            let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = where_expr(subquery) else {
                panic!("expected bound inner WHERE binary expression");
            };
            assert_column_expr(lhs, 2, 0);
            assert_column_expr(rhs, 0, 0);

            assert_eq!(group_by_expr(subquery, 0), select_expr(subquery, 0));

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(subquery) else {
                panic!("expected bound inner HAVING binary expression");
            };
            assert_eq!(lhs.as_ref(), select_expr(subquery, 0));
            assert_column_expr(rhs, 0, 0);

            assert_eq!(order_by_expr(subquery, 0), select_expr(subquery, 0));
        });
    }

    #[test]
    fn derived_table_columns_flow_into_outer_alias_binding() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select(
                "SELECT sq.x AS y \
                 FROM (SELECT t.a + 1 AS x FROM t) AS sq \
                 WHERE y > 2 \
                 ORDER BY y",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "sq");

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = where_expr(&select) else {
                panic!("expected bound outer WHERE binary expression");
            };
            assert_eq!(
                lhs.as_ref(),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(1usize),
                    column: 0,
                    is_rowid_alias: false,
                }
            );
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("2".into()))
            );

            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(1usize),
                    column: 0,
                    is_rowid_alias: false,
                }
            );
        });
    }

    #[test]
    fn cte_query_combines_cte_scope_group_by_having_and_order_by() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select(
                "WITH cte AS (SELECT a, b FROM t) \
                 SELECT a + 1 AS b \
                 FROM cte \
                 GROUP BY b \
                 HAVING b > 2 \
                 ORDER BY b",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.main_scope.tables.len(), 1);
            assert_eq!(bound.main_scope.tables[0].identifier, "cte");
            assert!(matches!(
                bound.main_scope.tables[0].source,
                ScopeTableSource::Cte { .. }
            ));

            // cte_id=0, t (inside CTE body)=1, cte (outer FROM)=2
            let alias_expr = ast::Expr::Binary(
                ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(2usize),
                    column: 0,
                    is_rowid_alias: false,
                }
                .into_boxed(),
                ast::Operator::Add,
                ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
            );

            assert_eq!(select_expr(&select, 0), &alias_expr);
            // GROUP BY prefers real columns over aliases (matches SQLite):
            // `b` resolves to column cte.b, not the alias expression (a + 1).
            assert_eq!(
                group_by_expr(&select, 0),
                &ast::Expr::Column {
                    database: None,
                    table: TableInternalId::from(2usize),
                    column: 1,
                    is_rowid_alias: false,
                }
            );

            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&select) else {
                panic!("expected bound HAVING binary expression");
            };
            assert_eq!(lhs.as_ref(), &alias_expr);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("2".into()))
            );

            assert_eq!(order_by_expr(&select, 0), &alias_expr);
        });
    }

    #[test]
    fn table_alias_hides_base_name_and_qualified_alias_resolves() {
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut good = parse_select("SELECT u.x FROM t AS u");
            ctx.bind_select(&mut good).unwrap();
            assert_column_expr(select_expr(&good, 0), 0, 0);

            let err = bind_select_error(ctx, "SELECT t.x FROM t AS u").to_string();
            assert!(
                err.contains("no such table: t") || err.contains("no such column: t.x"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn correlated_subquery_group_by_does_not_capture_outer_column_without_inner_match() {
        with_bind_context(&["CREATE TABLE t1(a, b)", "CREATE TABLE t2(x, y)"], |ctx| {
            let err = bind_select_error(
                ctx,
                "SELECT a FROM t1 WHERE EXISTS (SELECT x FROM t2 GROUP BY a)",
            )
            .to_string();
            assert!(err.contains("no such column: a"), "unexpected error: {err}");
        });
    }

    #[test]
    fn correlated_subquery_group_by_prefers_inner_column_when_present() {
        with_bind_context(&["CREATE TABLE t1(a, b)", "CREATE TABLE t3(a, x)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t1 WHERE EXISTS (SELECT x FROM t3 GROUP BY a)");
            let bound = ctx.bind_select(&mut select).unwrap();

            let sq_id = exists_subquery_id(&select);
            let subquery = &bound.subquery_bindings[&sq_id].select;
            // t1=0, subquery_id=1, t3=2
            assert_column_expr(group_by_expr(subquery, 0), 2, 0);
        });
    }

    #[test]
    fn duplicate_aliases_are_allowed_in_order_by() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select = parse_select("SELECT x AS a, y AS a FROM t ORDER BY a");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(order_by_expr(&select, 0), 0, 0);
        });
    }

    #[test]
    fn sqlite_compat_where_and_order_by_precedence_cases() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut where_select = parse_select("SELECT -a AS b, a, t.b FROM t WHERE b > 15");
            ctx.bind_select(&mut where_select).unwrap();
            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = where_expr(&where_select)
            else {
                panic!("expected bound WHERE binary expression");
            };
            assert_column_expr(lhs, 0, 1);
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("15".into()))
            );

            let mut order_select = parse_select("SELECT -a AS b, a, t.b FROM t ORDER BY b");
            ctx.bind_select(&mut order_select).unwrap();
            assert_eq!(
                order_by_expr(&order_select, 0),
                &ast::Expr::Unary(
                    ast::UnaryOperator::Negative,
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(1usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                )
            );
        });
    }

    #[test]
    fn sqlite_compat_group_by_prefers_source_column_over_alias() {
        // In GROUP BY, real columns take precedence over SELECT aliases
        // (matches SQLite): `b` resolves to column t.b, not alias (-a).
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT -a AS b, COUNT(*) FROM t GROUP BY b ORDER BY 1");
            ctx.bind_select(&mut select).unwrap();

            assert_column_expr(group_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn order_by_subquery_can_see_select_alias_and_prefer_source_column() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            // (SELECT x) is inlined to the alias expression -a since x is only an alias
            let mut alias_visible = parse_select("SELECT a, -a AS x FROM t ORDER BY (SELECT x)");
            ctx.bind_select(&mut alias_visible).unwrap();
            assert_eq!(
                order_by_expr(&alias_visible, 0),
                &ast::Expr::Unary(
                    ast::UnaryOperator::Negative,
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                )
            );

            let mut source_preferred =
                parse_select("SELECT -a AS b, a, t.b FROM t ORDER BY (SELECT b)");
            let bound = ctx.bind_select(&mut source_preferred).unwrap();
            let sq_id = subquery_id_from_expr(order_by_expr(&source_preferred, 0));
            let order_subquery = &bound.subquery_bindings[&sq_id].select;
            assert_eq!(
                select_expr(order_subquery, 0),
                select_expr(&source_preferred, 2)
            );
        });
    }

    #[test]
    fn subqueries_in_having_and_where_can_see_outer_aliases() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut having_select = parse_select(
                "SELECT a % 2 AS g, SUM(b) AS s FROM t GROUP BY g HAVING (SELECT s) > 15 ORDER BY g",
            );
            ctx.bind_select(&mut having_select).unwrap();
            let ast::Expr::Binary(lhs, ast::Operator::Greater, rhs) = having_expr(&having_select)
            else {
                panic!("expected bound HAVING binary expression");
            };
            // The trivial subquery `(SELECT s)` is inlined to the aggregate
            // alias expression, matching SQLite semantics (the HAVING clause
            // reads the outer aggregate value, not a nested query).
            assert_eq!(lhs.as_ref(), select_expr(&having_select, 1));
            assert_eq!(
                rhs.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("15".into()))
            );

            let mut nested_where = parse_select(
                "SELECT -a AS x, a \
                 FROM t \
                 WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT x WHERE x < 0))",
            );
            let bound = ctx.bind_select(&mut nested_where).unwrap();
            let outer_id = exists_subquery_id(&nested_where);
            let first_exists = &bound.subquery_bindings[&outer_id].select;
            let inner_id = exists_subquery_id(first_exists);
            let second_exists = &bound.subquery_bindings[&outer_id]
                .inner_bound
                .subquery_bindings[&inner_id]
                .select;
            assert_eq!(select_expr(second_exists, 0), select_expr(&nested_where, 0));
        });
    }

    fn window_clause(select: &ast::Select) -> &[ast::WindowDef] {
        match &select.body.select {
            ast::OneSelect::Select { window_clause, .. } => window_clause,
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn window_partition_by_and_order_by_are_bound() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select =
                parse_select("SELECT a FROM t WINDOW w AS (PARTITION BY b ORDER BY c)");
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_eq!(defs.len(), 1);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 1);
            assert_column_expr(&defs[0].window.order_by[0].expr, 0, 2);
        });
    }

    #[test]
    fn window_binds_qualified_column_refs() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select =
                parse_select("SELECT x FROM t WINDOW w AS (PARTITION BY t.y ORDER BY t.x)");
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 1);
            assert_column_expr(&defs[0].window.order_by[0].expr, 0, 0);
        });
    }

    #[test]
    fn window_does_not_resolve_aliases() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a AS z FROM t WINDOW w AS (PARTITION BY z)")
                .to_string();
            assert!(err.contains("no such column: z"), "unexpected error: {err}");
        });
    }

    #[test]
    fn order_by_column_number_replaces_with_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a, b FROM t ORDER BY 2");
            ctx.bind_select(&mut select).unwrap();

            // ORDER BY 2 should resolve to column b (index 1)
            assert_column_expr(order_by_expr(&select, 0), 0, 1);
        });
    }

    #[test]
    fn group_by_column_number_replaces_with_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a, b FROM t GROUP BY 1");
            ctx.bind_select(&mut select).unwrap();

            // GROUP BY 1 should resolve to column a (index 0)
            assert_column_expr(group_by_expr(&select, 0), 0, 0);
        });
    }

    #[test]
    fn column_number_zero_is_invalid() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a FROM t ORDER BY 0").to_string();
            assert!(
                err.contains("1st ORDER BY term out of range - should be between 1 and 1"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn column_number_out_of_range_is_invalid() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let err = bind_select_error(ctx, "SELECT a FROM t ORDER BY 5").to_string();
            assert!(
                err.contains("1st ORDER BY term out of range - should be between 1 and 1"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn float_literal_in_order_by_is_not_treated_as_column_number() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t ORDER BY 1.5");
            ctx.bind_select(&mut select).unwrap();

            // 1.5 should remain as a numeric literal, not replaced
            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Literal(ast::Literal::Numeric("1.5".into()))
            );
        });
    }

    #[test]
    fn order_by_column_number_with_complex_result_expr() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT a + 1, b FROM t ORDER BY 1");
            ctx.bind_select(&mut select).unwrap();

            // ORDER BY 1 should expand to the expression `a + 1` (already bound)
            assert_eq!(
                order_by_expr(&select, 0),
                &ast::Expr::Binary(
                    ast::Expr::Column {
                        database: None,
                        table: TableInternalId::from(0usize),
                        column: 0,
                        is_rowid_alias: false,
                    }
                    .into_boxed(),
                    ast::Operator::Add,
                    ast::Expr::Literal(ast::Literal::Numeric("1".into())).into_boxed(),
                )
            );
        });
    }

    #[test]
    fn limit_clause_is_bound() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT 10");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
        });
    }

    #[test]
    fn limit_with_offset_is_bound() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT 10 OFFSET 5");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("10".into()))
            );
            let offset = limit.offset.as_ref().expect("expected OFFSET");
            assert_eq!(
                offset.as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("5".into()))
            );
        });
    }

    #[test]
    fn limit_double_quoted_string_becomes_literal() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t LIMIT \"1\"");
            ctx.bind_select(&mut select).unwrap();

            let limit = select.limit.as_ref().expect("expected LIMIT clause");
            assert_eq!(
                limit.expr.as_ref(),
                &ast::Expr::Literal(ast::Literal::String("'1'".into()))
            );
        });
    }

    #[test]
    #[cfg(feature = "json")]
    fn function_call_star_expands_to_column_pairs() {
        with_bind_context(&["CREATE TABLE t(x, y)"], |ctx| {
            let mut select = parse_select("SELECT json_object(*) FROM t");
            ctx.bind_select(&mut select).unwrap();

            match select_expr(&select, 0) {
                ast::Expr::FunctionCall { name, args, .. } => {
                    assert_eq!(name.as_str(), "json_object");
                    // 2 columns × 2 (name + ref) = 4 args
                    assert_eq!(args.len(), 4);
                    assert_eq!(
                        args[0].as_ref(),
                        &ast::Expr::Literal(ast::Literal::String("'x'".into()))
                    );
                    assert_column_expr(&args[1], 0, 0);
                    assert_eq!(
                        args[2].as_ref(),
                        &ast::Expr::Literal(ast::Literal::String("'y'".into()))
                    );
                    assert_column_expr(&args[3], 0, 1);
                }
                other => panic!("expected FunctionCall, got {other:?}"),
            }
        });
    }

    #[test]
    fn function_call_star_without_expansion_stays_unchanged() {
        with_bind_context(&["CREATE TABLE t(a)"], |ctx| {
            let mut select = parse_select("SELECT count(*) FROM t");
            ctx.bind_select(&mut select).unwrap();

            // count(*) should remain as FunctionCallStar (not expanded)
            assert!(matches!(
                select_expr(&select, 0),
                ast::Expr::FunctionCallStar { .. }
            ));
        });
    }

    #[expect(clippy::vec_box)]
    fn values_exprs(select: &ast::Select) -> &[Vec<Box<ast::Expr>>] {
        match &select.body.select {
            ast::OneSelect::Values(rows) => rows,
            other => panic!("expected VALUES, got {other:?}"),
        }
    }

    #[test]
    fn values_double_quoted_identifier_becomes_string_literal() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("VALUES (\"hello\")");
            ctx.bind_select(&mut select).unwrap();

            let rows = values_exprs(&select);
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0].as_ref(),
                &ast::Expr::Literal(ast::Literal::String("'hello'".into()))
            );
        });
    }

    #[test]
    fn values_numeric_literals_are_untouched() {
        with_bind_context(&[], |ctx| {
            let mut select = parse_select("VALUES (1, 2, 3)");
            ctx.bind_select(&mut select).unwrap();

            let rows = values_exprs(&select);
            assert_eq!(rows[0].len(), 3);
            assert_eq!(
                rows[0][0].as_ref(),
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
        });
    }

    #[test]
    fn values_unquoted_identifier_errors() {
        with_bind_context(&[], |ctx| {
            let err = bind_select_error(ctx, "VALUES (x)").to_string();
            assert!(err.contains("no such column: x"), "unexpected error: {err}");
        });
    }

    #[test]
    fn multiple_window_defs_are_all_bound() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select(
                "SELECT a FROM t WINDOW w1 AS (PARTITION BY a), w2 AS (ORDER BY b, c)",
            );
            ctx.bind_select(&mut select).unwrap();

            let defs = window_clause(&select);
            assert_eq!(defs.len(), 2);
            assert_column_expr(&defs[0].window.partition_by[0], 0, 0);
            assert_eq!(defs[1].window.order_by.len(), 2);
            assert_column_expr(&defs[1].window.order_by[0].expr, 0, 1);
            assert_column_expr(&defs[1].window.order_by[1].expr, 0, 2);
        });
    }

    // ── expand_stars tests ──────────────────────────────────────────────

    fn select_columns(select: &ast::Select) -> &[ast::ResultColumn] {
        match &select.body.select {
            ast::OneSelect::Select { columns, .. } => columns,
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn expand_star_single_table() {
        with_bind_context(&["CREATE TABLE t(a, b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            // The star stays unexpanded in the AST (the planner's select_star
            // expands it); its columns are visible in the bound output.
            assert!(matches!(
                select_columns(&select)[0],
                ast::ResultColumn::Star
            ));
            assert_eq!(bound.result_columns.len(), 3);
            assert_column_expr(&bound.result_columns[0].expr, 0, 0);
            assert_column_expr(&bound.result_columns[1].expr, 0, 1);
            assert_column_expr(&bound.result_columns[2].expr, 0, 2);
        });
    }

    #[test]
    fn expand_star_multiple_tables() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(x, y)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t, u");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.result_columns.len(), 4);
            // t.a, t.b, u.x, u.y
            assert_column_expr(&bound.result_columns[0].expr, 0, 0);
            assert_column_expr(&bound.result_columns[1].expr, 0, 1);
            assert_column_expr(&bound.result_columns[2].expr, 1, 0);
            assert_column_expr(&bound.result_columns[3].expr, 1, 1);
        });
    }

    #[test]
    fn expand_table_star() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(x, y)"], |ctx| {
            let mut select = parse_select("SELECT u.* FROM t, u");
            let bound = ctx.bind_select(&mut select).unwrap();

            assert_eq!(bound.result_columns.len(), 2);
            // u.x, u.y
            assert_column_expr(&bound.result_columns[0].expr, 1, 0);
            assert_column_expr(&bound.result_columns[1].expr, 1, 1);
        });
    }

    #[test]
    fn expand_star_with_join_using_dedup() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t JOIN u USING(b)");
            let bound = ctx.bind_select(&mut select).unwrap();

            // t.a, t.b, u.c — u.b is deduped by USING
            assert_eq!(bound.result_columns.len(), 3);
            assert_column_expr(&bound.result_columns[0].expr, 0, 0);
            assert_column_expr(&bound.result_columns[1].expr, 0, 1);
            assert_column_expr(&bound.result_columns[2].expr, 1, 1);
        });
    }

    #[test]
    fn expand_star_mixed_with_explicit_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)"], |ctx| {
            let mut select = parse_select("SELECT 1, *, a FROM t");
            let bound = ctx.bind_select(&mut select).unwrap();

            // literal 1, t.a, t.b, t.a
            assert_eq!(bound.result_columns.len(), 4);
            assert_eq!(
                &bound.result_columns[0].expr,
                &ast::Expr::Literal(ast::Literal::Numeric("1".into()))
            );
            assert_column_expr(&bound.result_columns[1].expr, 0, 0);
            assert_column_expr(&bound.result_columns[2].expr, 0, 1);
            assert_column_expr(&bound.result_columns[3].expr, 0, 0);
        });
    }

    #[test]
    fn expand_star_no_tables_errors() {
        with_bind_context(&[], |ctx| {
            let select = parse_select("SELECT *");
            let cols = select_columns(&select);
            // No FROM → no tables in scope → star expands to nothing
            assert_eq!(cols.len(), 1); // still Star before binding
                                       // Binding should succeed but star expands to zero columns
                                       // Actually the parser requires FROM for star, let's just test
                                       // the expand_stars produces empty
            let scope = BindScope::empty();
            let mut columns = vec![ast::ResultColumn::Star];
            ctx.expand_stars(&mut columns, &scope).unwrap();
            assert_eq!(columns.len(), 0);
        });
    }

    // ── NATURAL JOIN tests ──────────────────────────────────────────────

    fn join_constraint(select: &ast::Select) -> &Option<ast::JoinConstraint> {
        match &select.body.select {
            ast::OneSelect::Select { from, .. } => {
                let from = from.as_ref().expect("expected FROM clause");
                &from.joins[0].constraint
            }
            other => panic!("expected SELECT core, got {other:?}"),
        }
    }

    #[test]
    fn natural_join_rewrites_to_using_with_common_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            ctx.bind_select(&mut select).unwrap();

            match join_constraint(&select) {
                Some(JoinConstraint::Using(cols)) => {
                    assert_eq!(cols.len(), 1);
                    assert_eq!(cols[0].as_str(), "b");
                }
                other => panic!("expected USING constraint, got {other:?}"),
            }
        });
    }

    #[test]
    fn natural_join_multiple_common_columns() {
        with_bind_context(
            &["CREATE TABLE t(a, b, c)", "CREATE TABLE u(b, c, d)"],
            |ctx| {
                let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
                ctx.bind_select(&mut select).unwrap();

                match join_constraint(&select) {
                    Some(JoinConstraint::Using(cols)) => {
                        assert_eq!(cols.len(), 2);
                        let names: Vec<&str> = cols.iter().map(|c| c.as_str()).collect();
                        assert!(names.contains(&"b"));
                        assert!(names.contains(&"c"));
                    }
                    other => panic!("expected USING constraint, got {other:?}"),
                }
            },
        );
    }

    #[test]
    fn natural_join_no_common_columns_is_cross_join() {
        // Matches SQLite: a NATURAL JOIN with no common columns degrades to a
        // cross join instead of erroring.
        with_bind_context(&["CREATE TABLE t(a)", "CREATE TABLE u(b)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            let bound = ctx.bind_select(&mut select).unwrap();
            assert_eq!(bound.result_columns.len(), 2); // t.a, u.b — no dedup, no constraint
        });
    }

    #[test]
    fn natural_join_with_on_clause_errors() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let err =
                bind_select_error(ctx, "SELECT * FROM t NATURAL JOIN u ON t.b = u.b").to_string();
            assert!(
                err.contains("a NATURAL join may not have an ON or USING clause"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn natural_join_star_deduplicates_common_columns() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT * FROM t NATURAL JOIN u");
            let bound = ctx.bind_select(&mut select).unwrap();

            // t.a, t.b, u.c — u.b is deduped by USING(b)
            assert_eq!(bound.result_columns.len(), 3);
            assert_column_expr(&bound.result_columns[0].expr, 0, 0); // t.a
            assert_column_expr(&bound.result_columns[1].expr, 0, 1); // t.b
            assert_column_expr(&bound.result_columns[2].expr, 1, 1); // u.c
        });
    }

    #[test]
    fn natural_join_rewrites_constraint_to_using() {
        with_bind_context(&["CREATE TABLE t(a, b)", "CREATE TABLE u(b, c)"], |ctx| {
            let mut select = parse_select("SELECT a FROM t NATURAL JOIN u");
            let bound = ctx.bind_select(&mut select).unwrap();

            // SELECT-list usage is tracked by the binder.
            assert!(bound
                .tracking
                .columns_used
                .contains(&(TableInternalId::from(0usize), 0))); // t.a from SELECT

            // The NATURAL constraint is rewritten to USING(b); the equality
            // predicate synthesis (and its column-usage marking) happens later
            // in fold_join_constraints, mirroring parse_join.
            let ast::OneSelect::Select { from, .. } = &select.body.select else {
                panic!("expected SELECT core");
            };
            let from = from.as_ref().expect("expected FROM clause");
            match &from.joins[0].constraint {
                Some(ast::JoinConstraint::Using(cols)) => {
                    assert_eq!(cols.len(), 1);
                    assert!(cols[0].as_str().eq_ignore_ascii_case("b"));
                }
                other => panic!("expected USING constraint, got {other:?}"),
            }
        });
    }

    #[test]
    fn bind_cte_multi_reference_produces_separate_scope_tables() {
        // When the same CTE is referenced twice (e.g. FROM cte t1 JOIN cte t2),
        // into_table_references must produce two JoinedTables — not fail because
        // the CTE was consumed by the first reference.
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut select =
                parse_select("WITH c AS (SELECT x FROM t) SELECT t1.x, t2.x FROM c t1, c t2");
            let bound = ctx.bind_select(&mut select).unwrap();

            // The scope should have two tables (c t1 and c t2) with distinct internal_ids.
            assert_eq!(bound.main_scope.tables.len(), 2);
            assert_ne!(
                bound.main_scope.tables[0].internal_id,
                bound.main_scope.tables[1].internal_id
            );
            assert_eq!(bound.main_scope.tables[0].identifier, "t1");
            assert_eq!(bound.main_scope.tables[1].identifier, "t2");
        });
    }

    #[test]
    fn bind_cte_definitions_preserve_definition_order() {
        // referenced_cte_indices are offsets into the cte_definitions vec.
        // If cte_definitions were collected in arbitrary HashMap iteration order,
        // the indices would point to the wrong CTEs, causing infinite recursion
        // during planning.
        with_bind_context(&["CREATE TABLE t(x)"], |ctx| {
            let mut select = parse_select(
                "WITH a AS (SELECT x FROM t), \
                      b AS (SELECT x FROM a), \
                      c AS (SELECT x FROM a) \
                 SELECT * FROM c",
            );
            let bound = ctx.bind_select(&mut select).unwrap();

            // Validate that referenced_cte_indices actually point to the right CTEs.
            // b references a, and c references a. Regardless of iteration order,
            // the index stored must resolve to "a" in the cte_definitions vec.
            for (name, entry) in &bound.cte_definitions {
                if name == "b" || name == "c" {
                    assert_eq!(
                        entry.referenced_cte_indices.len(),
                        1,
                        "CTE '{name}' should reference exactly one sibling"
                    );
                    let ref_idx = entry.referenced_cte_indices[0];
                    assert_eq!(
                        bound.cte_definitions[ref_idx].0, "a",
                        "CTE '{name}' references index {ref_idx} which should be 'a', \
                         but found '{}' — cte_definitions is not in definition order",
                        bound.cte_definitions[ref_idx].0
                    );
                }
            }
        });
    }
}
