use crate::sync::Arc;
use crate::HashMap;
use crate::LimboError;

use crate::ext::VTabImpl;
use crate::function::{Deterministic, Func, MathFunc, ReturnType, ScalarFunc};
use crate::namespace::is_reserved_object_name;
use crate::namespace::{NamespaceId, NamespaceRef};
use crate::schema::{
    create_table, translate_ident_to_string_literal, type_registry_key, BTreeCharacteristics,
    BTreeTable, ColDef, Column, GeneratedType, Schema, SchemaObjectType, Table, Trigger, Type,
    TypeDef, SQLITE_SEQUENCE_TABLE_NAME, TURSO_NAMESPACES_TABLE_NAME, TURSO_TYPES_TABLE_NAME,
};
use crate::stats::STATS_TABLE;
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::catalog::CatalogContext;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::{
    emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns, prepare_cdc_if_necessary,
    OperationMode, Resolver,
};
use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::fkeys::emit_fk_drop_table_check;
use crate::translate::plan::{compound_column_affinity, Plan, QueryDestination};
use crate::translate::planner::ROWID_STRS;
use crate::translate::select::{emit_select_plan, prepare_select_plan};
use crate::translate::sequence::bind_sequence_names;
use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::util::{
    column_declared_type, dequote, escape_sql_string_literal, is_builtin_strict_type,
    normalize_ident, quote_identifier, walk_expr_with_subqueries, walk_select_expressions,
    walk_trigger_cmd_expressions, PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX,
};
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{
    to_u32, {CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral},
};
use crate::{bail_parse_error, turso_assert, turso_assert_eq, CaptureDataChangesExt, Result};
use crate::{Connection, Value, MAIN_DB_ID};

use turso_ext::VTabKind;
use turso_parser::ast;
use turso_parser::ast::ColumnDefinition;

/// Validate a CHECK constraint expression at CREATE TABLE / ALTER TABLE ADD COLUMN time.
/// Rejects non-existent columns, non-existent functions, aggregates, window functions,
/// bind parameters, and subqueries.
pub(crate) fn validate_check_expr(
    expr: &ast::Expr,
    table_name: &str,
    column_names: &[&str],
    resolver: &CatalogContext,
) -> Result<()> {
    let normalized_table = normalize_ident(table_name);
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                let n = normalize_ident(name.as_str());
                if !column_names.iter().any(|c| normalize_ident(c) == n)
                    && !ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&n))
                {
                    bail_parse_error!("no such column: {}", name.as_str());
                }
            }
            ast::Expr::Qualified(tbl, col) => {
                if normalize_ident(tbl.as_str()) != normalized_table {
                    bail_parse_error!("no such column: {}.{}", tbl.as_str(), col.as_str());
                }
                let cn = normalize_ident(col.as_str());
                if !column_names.iter().any(|c| normalize_ident(c) == cn)
                    && !ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&cn))
                {
                    bail_parse_error!("no such column: {}", col.as_str());
                }
            }
            ast::Expr::DoublyQualified(db, tbl, col) => {
                bail_parse_error!(
                    "no such column: {}.{}.{}",
                    db.as_str(),
                    tbl.as_str(),
                    col.as_str()
                );
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                order_by,
                ..
            } => {
                super::expr::validate_function_tail(
                    name.as_str(),
                    args.len(),
                    filter_over,
                    !order_by.is_empty(),
                    resolver,
                )?;
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), args.len())? {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                    }
                    if matches!(func, Func::Window(..)) {
                        bail_parse_error!("misuse of window function {}()", name.as_str());
                    }
                } else {
                    bail_parse_error!("no such function: {}", name.as_str());
                }
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), 0)? {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                    }
                    if matches!(func, Func::Window(..)) {
                        bail_parse_error!("misuse of window function {}()", name.as_str());
                    }
                } else {
                    bail_parse_error!("no such function: {}", name.as_str());
                }
            }
            ast::Expr::Variable(_) => {
                bail_parse_error!("parameters prohibited in CHECK constraints");
            }
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries prohibited in CHECK constraints");
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

pub(crate) fn validate_default_expr(expr: &ast::Expr, col: &ColumnDefinition) -> Result<()> {
    let expr = translate_ident_to_string_literal(expr).unwrap_or_else(|| Box::new(expr.clone()));
    walk_expr(&expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Column { .. }
            | ast::Expr::RowId { .. }
            | ast::Expr::Name(_)
            | ast::Expr::Qualified(_, _)
            | ast::Expr::DoublyQualified(_, _, _)
            | ast::Expr::Variable(_)
            | ast::Expr::Raise(_, _)
            | ast::Expr::Exists(_)
            | ast::Expr::InSelect { .. }
            | ast::Expr::InTable { .. }
            | ast::Expr::Subquery(_)
            | ast::Expr::SubqueryResult { .. }
            | ast::Expr::Id(_) => {
                bail_parse_error!(
                    "default value of column [{}] is not constant",
                    col.col_name.as_str()
                );
            }
            ast::Expr::FunctionCall { filter_over, .. }
            | ast::Expr::FunctionCallStar { filter_over, .. }
                if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() =>
            {
                bail_parse_error!(
                    "default value of column [{}] is not constant",
                    col.col_name.as_str()
                );
            }
            _ => Ok(WalkControl::Continue),
        }
    })?;
    Ok(())
}

/// Resolved type of an expression node for strict type checking of CHECK constraints.
/// In STRICT tables, every comparison operand must have a determinable, compatible type.
/// If a type cannot be determined (e.g. function calls), the user must use an explicit CAST.
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
            (a, b) if a == b => true,
            (a, b) if a.is_numeric() && b.is_numeric() => true,
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

/// Resolve the type of an expression node in a CHECK constraint.
/// Returns an error if the type cannot be determined — the user must use CAST.
fn resolve_check_expr_type(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &CatalogContext,
) -> Result<CheckExprType> {
    use ast::{Literal, Operator, UnaryOperator};
    match expr {
        ast::Expr::Id(name) | ast::Expr::Name(name) => {
            let n = normalize_ident(name.as_str());
            // rowid/oid/_rowid_ are INTEGER
            if ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&n)) {
                return Ok(CheckExprType::Integer);
            }
            for col in columns {
                if normalize_ident(col.col_name.as_str()) == n {
                    return resolve_column_type(col, resolver);
                }
            }
            bail_parse_error!("no such column: {}", name.as_str());
        }
        ast::Expr::Qualified(_tbl, col) => {
            let cn = normalize_ident(col.as_str());
            if ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&cn)) {
                return Ok(CheckExprType::Integer);
            }
            for c in columns {
                if normalize_ident(c.col_name.as_str()) == cn {
                    return resolve_column_type(c, resolver);
                }
            }
            bail_parse_error!("no such column: {}", col.as_str());
        }
        ast::Expr::Literal(lit) => match lit {
            Literal::Numeric(s) => {
                if s.contains('.') || s.contains('e') || s.contains('E') {
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
            Literal::Keyword(s) => {
                bail_parse_error!(
                    "cannot determine type of '{}' in CHECK constraint; use CAST",
                    s
                );
            }
        },
        ast::Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                resolve_check_expr_type(&exprs[0], columns, resolver)
            } else {
                bail_parse_error!(
                    "cannot determine type of expression in CHECK constraint; use CAST"
                );
            }
        }
        ast::Expr::Cast { type_name, .. } => {
            if let Some(ref tn) = type_name {
                resolve_type_name(&dequote(&tn.name), resolver)
            } else {
                bail_parse_error!(
                    "cannot determine type of CAST in CHECK constraint; use CAST with explicit type"
                );
            }
        }
        ast::Expr::Unary(op, inner) => match op {
            UnaryOperator::Negative | UnaryOperator::Positive => {
                let inner_ty = resolve_check_expr_type(inner, columns, resolver)?;
                if !inner_ty.is_numeric() && inner_ty != CheckExprType::Null {
                    bail_parse_error!(
                        "unary minus/plus requires a numeric type, got {}",
                        inner_ty.display_name()
                    );
                }
                Ok(inner_ty)
            }
            UnaryOperator::BitwiseNot => Ok(CheckExprType::Integer),
            UnaryOperator::Not => Ok(CheckExprType::Integer),
        },
        ast::Expr::Binary(lhs, op, rhs) => {
            match op {
                // Arithmetic: both must be numeric, result follows promotion rules
                Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                    let lty = resolve_check_expr_type(lhs, columns, resolver)?;
                    let rty = resolve_check_expr_type(rhs, columns, resolver)?;
                    if lty == CheckExprType::Null || rty == CheckExprType::Null {
                        return Ok(CheckExprType::Null);
                    }
                    if !lty.is_numeric() || !rty.is_numeric() {
                        bail_parse_error!(
                            "arithmetic requires numeric types, got {} and {}",
                            lty.display_name(),
                            rty.display_name()
                        );
                    }
                    if lty == CheckExprType::Real || rty == CheckExprType::Real {
                        Ok(CheckExprType::Real)
                    } else {
                        Ok(CheckExprType::Integer)
                    }
                }
                Operator::Modulus => Ok(CheckExprType::Integer),
                Operator::Concat => Ok(CheckExprType::Text),
                Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::LeftShift
                | Operator::RightShift => Ok(CheckExprType::Integer),
                // Logical: recurse to find nested comparisons
                Operator::And | Operator::Or => {
                    // The result of AND/OR is boolean (integer), but we need to
                    // recurse to validate any comparisons inside.
                    validate_check_types_in_expr(lhs, columns, resolver)?;
                    validate_check_types_in_expr(rhs, columns, resolver)?;
                    Ok(CheckExprType::Integer)
                }
                // Comparison operators: validate type compatibility and return Integer (boolean)
                Operator::Equals
                | Operator::NotEquals
                | Operator::Less
                | Operator::LessEquals
                | Operator::Greater
                | Operator::GreaterEquals => {
                    let lty = resolve_check_expr_type(lhs, columns, resolver)?;
                    let rty = resolve_check_expr_type(rhs, columns, resolver)?;
                    if !lty.is_compatible_with(&rty) {
                        bail_parse_error!(
                            "type mismatch in CHECK constraint: cannot compare {} with {}",
                            lty.display_name(),
                            rty.display_name()
                        );
                    }
                    Ok(CheckExprType::Integer)
                }
                // IS/IS NOT are NULL-checking operators, skip type validation
                Operator::Is | Operator::IsNot => Ok(CheckExprType::Integer),
                _ => {
                    bail_parse_error!(
                        "cannot determine type of expression in CHECK constraint; use CAST"
                    );
                }
            }
        }
        ast::Expr::NotNull(_) | ast::Expr::IsNull(_) => Ok(CheckExprType::Integer),
        ast::Expr::FunctionCall { name, args, .. } => {
            if let Some(func) = resolver.resolve_function(name.as_str(), args.len())? {
                resolve_func_return_type(&func, name.as_str(), args, columns, resolver)
            } else {
                bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            }
        }
        ast::Expr::FunctionCallStar { name, .. } => {
            if let Some(func) = resolver.resolve_function(name.as_str(), 0)? {
                resolve_func_return_type(&func, name.as_str(), &[], columns, resolver)
            } else {
                bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            }
        }
        _ => {
            bail_parse_error!("cannot determine type of expression in CHECK constraint; use CAST");
        }
    }
}

/// Resolve the return type of a function for CHECK constraint type checking.
fn resolve_func_return_type(
    func: &Func,
    name: &str,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &CatalogContext,
) -> Result<CheckExprType> {
    if let Func::Agg(_) = func {
        bail_parse_error!("misuse of aggregate function {}()", name);
    }
    match func.return_type() {
        Some(ReturnType::Integer) => return Ok(CheckExprType::Integer),
        Some(ReturnType::Real) => return Ok(CheckExprType::Real),
        Some(ReturnType::Text) => return Ok(CheckExprType::Text),
        Some(ReturnType::Blob) => return Ok(CheckExprType::Blob),
        Some(ReturnType::Custom(type_name)) => {
            reject_value_unlike_stored_columns(name, type_name, resolver)?;
            return resolve_type_name(type_name, resolver);
        }
        Some(ReturnType::Array) => return Ok(CheckExprType::Any),
        None => {}
    }
    match func {
        Func::Scalar(sf) => resolve_scalar_func_return_type(sf, args, columns, resolver),
        // ceil, floor and trunc give an integer for an integer and a real for
        // a real. A text argument is first converted to a number, which may
        // be either. SQLite's other math functions return a real.
        Func::Math(MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc) => {
            match args.first() {
                Some(arg) => Ok(match resolve_check_expr_type(arg, columns, resolver)? {
                    ty @ (CheckExprType::Integer | CheckExprType::Real | CheckExprType::Null) => ty,
                    _ => CheckExprType::Any,
                }),
                None => Ok(CheckExprType::Any),
            }
        }
        Func::Math(_) => Ok(CheckExprType::Real),
        Func::External(_) => {
            bail_parse_error!(
                "cannot determine return type of function {}() in CHECK constraint; \
                 wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                name,
                name
            );
        }
        _ => Ok(CheckExprType::Any),
    }
}

/// A function that returns a value of a custom type returns it as a column
/// read gives it, after DECODE, but a CHECK constraint sees the columns as they
/// are stored. For a type whose DECODE changes the value, such as `uuid`
/// stored as a blob and read as text, comparing the function's value with a
/// column would never find them equal, so the CHECK constraint is refused.
fn reject_value_unlike_stored_columns(
    name: &str,
    type_name: &str,
    resolver: &CatalogContext,
) -> Result<()> {
    if let Some(resolved) = resolver.schema().resolve_type_unchecked(type_name)? {
        if !resolved.is_domain() && !resolved.reads_stored_values_unchanged() {
            bail_parse_error!(
                "cannot compare {}() in a CHECK constraint: it returns {} values in a \
                 different form than they are stored",
                name,
                type_name
            );
        }
    }
    Ok(())
}

/// Resolve the return type of a scalar function that declares none, because
/// its type depends on its arguments.
fn resolve_scalar_func_return_type(
    func: &ScalarFunc,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &CatalogContext,
) -> Result<CheckExprType> {
    match func {
        // substr returns a blob for a blob and text for a number or text.
        ScalarFunc::Substr | ScalarFunc::Substring => {
            let Some(arg) = args.first() else {
                return Ok(CheckExprType::Any);
            };
            Ok(match resolve_check_expr_type(arg, columns, resolver)? {
                ty @ (CheckExprType::Blob | CheckExprType::Any | CheckExprType::Null) => ty,
                // The stored value of a custom type may be a blob.
                CheckExprType::CustomType(_) => CheckExprType::Any,
                CheckExprType::Integer | CheckExprType::Real | CheckExprType::Text => {
                    CheckExprType::Text
                }
            })
        }

        // The type of the first argument: likely(x) is x.
        ScalarFunc::Abs
        | ScalarFunc::Nullif
        | ScalarFunc::Likely
        | ScalarFunc::Unlikely
        | ScalarFunc::Likelihood => {
            if let Some(arg) = args.first() {
                resolve_check_expr_type(arg, columns, resolver)
            } else {
                Ok(CheckExprType::Any)
            }
        }

        ScalarFunc::Coalesce | ScalarFunc::IfNull => {
            for arg in args {
                let ty = resolve_check_expr_type(arg, columns, resolver)?;
                if ty != CheckExprType::Null {
                    return Ok(ty);
                }
            }
            Ok(CheckExprType::Null)
        }

        // SQLite's round() always returns a real.
        ScalarFunc::Round => Ok(CheckExprType::Real),

        ScalarFunc::Min | ScalarFunc::Max => {
            if let Some(first) = args.first() {
                resolve_check_expr_type(first, columns, resolver)
            } else {
                Ok(CheckExprType::Any)
            }
        }

        ScalarFunc::Iif => {
            // iif(cond, then_val, else_val) — return type of then_val
            if args.len() >= 2 {
                resolve_check_expr_type(&args[1], columns, resolver)
            } else {
                Ok(CheckExprType::Any)
            }
        }

        _ => Ok(CheckExprType::Any),
    }
}

/// Resolve a column's type from its definition.
fn resolve_column_type(
    col: &ast::ColumnDefinition,
    resolver: &CatalogContext,
) -> Result<CheckExprType> {
    if let Some(ref col_type) = col.col_type {
        resolve_type_name(&column_declared_type(&col_type.name), resolver)
    } else {
        // No type specified — in STRICT tables this would be caught elsewhere,
        // but treat as ANY for CHECK validation purposes.
        Ok(CheckExprType::Any)
    }
}

/// Resolve a type name string to a CheckExprType.
fn resolve_type_name(type_name: &str, resolver: &CatalogContext) -> Result<CheckExprType> {
    let name_bytes = type_name.as_bytes();
    let result = turso_macros::match_ignore_ascii_case!(match name_bytes {
        b"INT" | b"INTEGER" => Some(CheckExprType::Integer),
        b"REAL" | b"FLOAT" | b"DOUBLE" => Some(CheckExprType::Real),
        b"TEXT" => Some(CheckExprType::Text),
        b"BLOB" => Some(CheckExprType::Blob),
        b"ANY" => Some(CheckExprType::Any),
        _ => None,
    });
    if let Some(ty) = result {
        return Ok(ty);
    }
    // Check if it's a known custom type
    if let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) {
        // Domains are transparent wrappers — resolve to the base primitive type
        // so CHECK constraint type checking compares primitives, not domain names.
        if resolved.is_domain() || resolved.reads_stored_values_unchanged() {
            return resolve_type_name(&resolved.primitive, resolver);
        }
        return Ok(CheckExprType::CustomType(type_name.to_lowercase()));
    }
    bail_parse_error!("unknown type '{}' in CHECK constraint", type_name);
}

/// Walk a CHECK expression and validate that all comparisons have compatible types.
/// Only called for STRICT tables.
fn validate_check_types_in_expr(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &CatalogContext,
) -> Result<()> {
    use ast::Operator;
    match expr {
        ast::Expr::Binary(lhs, op, rhs) => {
            match op {
                Operator::Equals
                | Operator::NotEquals
                | Operator::Less
                | Operator::LessEquals
                | Operator::Greater
                | Operator::GreaterEquals => {
                    let lty = resolve_check_expr_type(lhs, columns, resolver)?;
                    let rty = resolve_check_expr_type(rhs, columns, resolver)?;
                    if !lty.is_compatible_with(&rty) {
                        bail_parse_error!(
                            "type mismatch in CHECK constraint: cannot compare {} with {}",
                            lty.display_name(),
                            rty.display_name()
                        );
                    }
                }
                Operator::And | Operator::Or => {
                    validate_check_types_in_expr(lhs, columns, resolver)?;
                    validate_check_types_in_expr(rhs, columns, resolver)?;
                }
                // Arithmetic, concat, bitwise — recurse to find nested comparisons
                _ => {
                    validate_check_types_in_expr(lhs, columns, resolver)?;
                    validate_check_types_in_expr(rhs, columns, resolver)?;
                }
            }
        }
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            let lty = resolve_check_expr_type(lhs, columns, resolver)?;
            let sty = resolve_check_expr_type(start, columns, resolver)?;
            let ety = resolve_check_expr_type(end, columns, resolver)?;
            if !lty.is_compatible_with(&sty) {
                bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lty.display_name(),
                    sty.display_name()
                );
            }
            if !lty.is_compatible_with(&ety) {
                bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lty.display_name(),
                    ety.display_name()
                );
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            let lty = resolve_check_expr_type(lhs, columns, resolver)?;
            for item in rhs {
                let ity = resolve_check_expr_type(item, columns, resolver)?;
                if !lty.is_compatible_with(&ity) {
                    bail_parse_error!(
                        "type mismatch in CHECK IN list: cannot compare {} with {}",
                        lty.display_name(),
                        ity.display_name()
                    );
                }
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            for e in exprs {
                validate_check_types_in_expr(e, columns, resolver)?;
            }
        }
        ast::Expr::Unary(_, inner) => {
            validate_check_types_in_expr(inner, columns, resolver)?;
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(op) = base {
                validate_check_types_in_expr(op, columns, resolver)?;
            }
            for (when_expr, then_expr) in when_then_pairs {
                validate_check_types_in_expr(when_expr, columns, resolver)?;
                validate_check_types_in_expr(then_expr, columns, resolver)?;
            }
            if let Some(else_e) = else_expr {
                validate_check_types_in_expr(else_e, columns, resolver)?;
            }
        }
        ast::Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_check_types_in_expr(arg, columns, resolver)?;
            }
        }
        // Leaf nodes and other expressions: no nested comparisons to validate
        _ => {}
    }
    Ok(())
}

fn validate(
    body: &ast::CreateTableBody,
    table_name: &str,
    resolver: &CatalogContext,
    is_nested: bool,
) -> Result<()> {
    if let ast::CreateTableBody::ColumnsAndConstraints {
        options,
        columns,
        constraints,
    } = &body
    {
        if columns.len() > crate::types::MAX_COLUMN {
            return Err(LimboError::ParseError(format!(
                "too many columns on {table_name}"
            )));
        }
        let column_names: Vec<&str> = columns.iter().map(|c| c.col_name.as_str()).collect();
        for i in 0..columns.len() {
            let col_i = &columns[i];
            for constraint in &col_i.constraints {
                match &constraint.constraint {
                    ast::ColumnConstraint::Check { expr, .. } => {
                        validate_check_expr(expr, table_name, &column_names, resolver)?;
                    }
                    ast::ColumnConstraint::Default(expr) => validate_default_expr(expr, col_i)?,
                    ast::ColumnConstraint::Collate { collation_name } => {
                        let collation = resolver.resolve_collation(collation_name.as_str())?;
                        if collation.is_custom() {
                            bail_parse_error!(
                                "custom collations are not supported in schema definitions"
                            );
                        }
                    }
                    _ => {}
                }
            }
            for j in &columns[(i + 1)..] {
                if col_i
                    .col_name
                    .as_str()
                    .eq_ignore_ascii_case(j.col_name.as_str())
                {
                    bail_parse_error!("duplicate column name: {}", j.col_name.as_str());
                }
            }
        }
        for constraint in constraints {
            if let ast::TableConstraint::Check { ref expr, .. } = constraint.constraint {
                validate_check_expr(expr, table_name, &column_names, resolver)?;
            }
        }

        let is_strict = options.contains_strict();

        for c in columns {
            if let Some(ref col_type) = c.col_type {
                let type_name = &column_declared_type(&col_type.name);
                let is_builtin = is_builtin_strict_type(&col_type.name);

                // Array columns require STRICT tables because the encode/decode
                // pipeline is only emitted for STRICT tables.
                if col_type.is_array() && !is_strict {
                    bail_parse_error!(
                        "array type columns require STRICT tables: {}.{}",
                        table_name,
                        c.col_name
                    );
                }
                if col_type.is_array() {
                    resolver.schema().check_array_element_type(type_name)?;
                }

                // Domain types require STRICT tables because domain constraints
                // (CHECK, NOT NULL, DEFAULT) are only enforced on STRICT tables.
                if !is_builtin && !is_strict {
                    let type_def = resolver.schema().get_type_def_unchecked(type_name);
                    if let Some(td) = type_def {
                        if td.is_domain {
                            bail_parse_error!(
                                "domain type columns require STRICT tables: {}.{}",
                                table_name,
                                c.col_name
                            );
                        }
                    }
                }

                if !is_builtin && is_strict {
                    let type_def = resolver.schema().get_type_def_unchecked(type_name);
                    {
                        match type_def {
                            None => {
                                bail_parse_error!(
                                    "unknown datatype for {}.{}: \"{}\"",
                                    table_name,
                                    c.col_name,
                                    type_name
                                );
                            }
                            Some(td) if td.user_params().next().is_some() => {
                                // Parametric type: verify the column provides the right
                                // number of user parameters (excluding `value`).
                                let provided = match &col_type.size {
                                    Some(ast::TypeSize::TypeSize(_, _)) => 2,
                                    Some(ast::TypeSize::MaxSize(_)) => 1,
                                    None => 0,
                                };
                                let expected = td.user_params().count();
                                if provided != expected {
                                    bail_parse_error!(
                                        "type \"{}\" requires {} parameter(s), got {}",
                                        type_name,
                                        expected,
                                        provided
                                    );
                                }
                            }
                            Some(_) => {}
                        }
                    }
                }
            }
        }

        // In STRICT tables, validate that CHECK constraint comparisons have
        // compatible types. This catches type mismatches at CREATE TABLE time
        // rather than producing wrong results at INSERT/UPDATE time.
        if is_strict {
            let col_refs: Vec<&ast::ColumnDefinition> = columns.iter().collect();
            for col in columns {
                for constraint in &col.constraints {
                    if let ast::ColumnConstraint::Check { expr, .. } = &constraint.constraint {
                        validate_check_types_in_expr(expr, &col_refs, resolver)?;
                    }
                }
            }
            for constraint in constraints {
                if let ast::TableConstraint::Check { ref expr, .. } = constraint.constraint {
                    validate_check_types_in_expr(expr, &col_refs, resolver)?;
                }
            }
        }

        // Rules for new tables only: VACUUM creates the tables the database
        // already has as nested statements, and those may break these rules.
        if !is_nested {
            let mut table =
                create_table(table_name, body, 0, resolver.dialect.supports_namespaces())?;
            if !table.has_rowid {
                if table.has_autoincrement {
                    bail_parse_error!("AUTOINCREMENT is not allowed on WITHOUT ROWID tables");
                }
                if table.primary_key_columns.is_empty() {
                    bail_parse_error!("PRIMARY KEY missing on table {}", table_name);
                }
                if table.unique_sets.iter().any(|us| !us.is_primary_key) {
                    bail_parse_error!(
                        "secondary UNIQUE constraints on WITHOUT ROWID tables are not supported"
                    );
                }
            } else if table.has_autoincrement {
                // SQLite allows AUTOINCREMENT only on a key declared exactly as
                // INTEGER PRIMARY KEY: quotes around INTEGER are allowed, a size
                // such as INTEGER(8) and an inline DESC are not. This is checked
                // here rather than in `create_table` so that databases where older
                // Turso versions stored such tables still open.
                let (pk_name, pk_order) = &table.primary_key_columns[0];
                let is_integer_primary_key = table.get_column(pk_name).is_some_and(|(_, col)| {
                    col.ty_str.eq_ignore_ascii_case("INTEGER") && col.ty_params.is_empty()
                }) && !(table.primary_key_is_inline
                    && *pk_order == ast::SortOrder::Desc);
                if !is_integer_primary_key {
                    bail_parse_error!("AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY");
                }
            }
            table.resolve_custom_types(resolver.schema());
            table.check_collates_are_allowed()?;
        }
    }
    Ok(())
}

/// Schema information derived from a CTAS SELECT.
struct CtasInfo {
    plan: Plan,
    schema_sql: String,
}

/// Pre-plan the SELECT to derive the schema for a CTAS table.
/// Returns the plan (for reuse in emission) along with the complete schema SQL and column definitions.
fn derive_ctas_schema(
    select: ast::Select,
    table_name_in_sql: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<(CtasInfo, Vec<ColumnDefinition>)> {
    let plan = prepare_select_plan(
        select,
        resolver,
        program,
        &[],
        QueryDestination::ResultRows,
        connection,
    )?;

    // For compound selects, use the leftmost select's columns for naming (matching SQLite).
    // The planner guarantees `left` is always non-empty in a CompoundSelect.
    let (result_columns, table_refs) = match &plan {
        Plan::Select(sp) => (&sp.result_columns, &sp.table_references),
        Plan::CompoundSelect { left, .. } => {
            (&left[0].0.result_columns, &left[0].0.table_references)
        }
        _ => bail_parse_error!("unexpected plan type for CTAS"),
    };
    // SQLite derives a compound output affinity from all arms, not only the leftmost arm.
    let compound_arms = match &plan {
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut arms = Vec::with_capacity(left.len() + 1);
            arms.extend(left.iter().map(|(select, _)| select));
            arms.push(right_most);
            Some(arms)
        }
        _ => None,
    };

    // Collect names first, then deduplicate using SQLite's :N suffix convention.
    let mut names: Vec<String> = result_columns
        .iter()
        .map(|col| col.name_or_expr_for_ctas(table_refs))
        .collect();

    let mut seen: HashMap<String, usize> = HashMap::default();
    for name in &mut names {
        let lower = name.to_lowercase();
        let count = seen.entry(lower).or_insert(0);
        if *count > 0 {
            *name = format!("{name}:{count}");
        }
        *count += 1;
    }

    let mut sql_parts = Vec::with_capacity(result_columns.len());
    let mut col_defs = Vec::with_capacity(result_columns.len());

    for (column_index, (col, name)) in result_columns.iter().zip(names).enumerate() {
        // Names come from the leftmost arm, but compound affinity can weaken its type.
        let ty = compound_arms
            .as_ref()
            .map(|arms| compound_column_affinity(arms, column_index).short_type_name())
            .unwrap_or_else(|| col.declared_type(table_refs));

        let quoted = quote_identifier(&name);
        if ty.is_empty() {
            sql_parts.push(quoted);
        } else {
            sql_parts.push(format!("{quoted} {ty}"));
        }

        let col_type = if ty.is_empty() {
            None
        } else {
            Some(ast::Type {
                name: ty.to_string(),
                size: None,
                array_dimensions: 0,
            })
        };
        col_defs.push(ColumnDefinition {
            col_name: ast::Name::exact(name),
            col_type,
            constraints: vec![],
        });
    }

    let info = CtasInfo {
        plan,
        schema_sql: format!("CREATE TABLE {table_name_in_sql}({})", sql_parts.join(",")),
    };
    Ok((info, col_defs))
}

/// Emit bytecode to populate a newly-created CTAS table from the SELECT.
/// Uses a coroutine to run the SELECT and insert each result row.
/// Takes an already-prepared plan (from `derive_ctas_schema`) to avoid double planning.
#[allow(clippy::too_many_arguments)]
fn emit_ctas_insert(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut plan: Plan,
    body: &ast::CreateTableBody,
    table_root_reg: usize,
    col_count: usize,
    database_id: usize,
    table_name: &str,
    connection: &Arc<Connection>,
) -> Result<()> {
    let opts = ProgramBuilderOpts {
        num_cursors: 2,
        approx_num_insns: 20,
        approx_num_labels: 3,
    };
    program.extend(&opts);

    // Set up coroutine for the SELECT
    let yield_reg = program.alloc_register();
    let jump_on_definition_label = program.allocate_label();
    let start_offset_label = program.allocate_label();
    let halt_label = program.allocate_label();

    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: jump_on_definition_label,
        start_offset: start_offset_label,
    });
    program.preassign_label_to_next_insn(start_offset_label);

    // Switch the plan's destination to coroutine yield mode.
    let dest = plan.select_query_destination_mut().ok_or_else(|| {
        crate::LimboError::InternalError("CTAS plan must be a SELECT or CompoundSelect".into())
    })?;
    *dest = QueryDestination::CoroutineYield {
        yield_reg,
        coroutine_implementation_start: halt_label,
    };

    let num_result_cols =
        program.nested(|program| emit_select_plan(plan, resolver, program, connection))?;

    if num_result_cols != col_count {
        bail_parse_error!(
            "CTAS internal error: expected {} columns from SELECT but got {}",
            col_count,
            num_result_cols
        );
    }

    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(jump_on_definition_label);

    // Open the new table for writing using the root page from CreateBtree.
    let ctas_btree = Arc::new(create_table(
        table_name,
        body,
        0,
        resolver.catalog.dialect.supports_namespaces(),
    )?);
    // SQLite applies the derived CTAS affinity in MakeRecord. This keeps each
    // stored value consistent with the declared type of the CTAS column.
    let affinity_str = ctas_btree
        .columns()
        .iter()
        .map(|column| column.affinity().aff_mask())
        .collect();
    let new_table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ctas_btree));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: new_table_cursor_id,
        root_page: RegisterOrLiteral::Register(table_root_reg),
        db: database_id,
    });

    // Main insert loop: yield from coroutine, make record, insert
    let loop_start = program.allocate_label();
    let loop_end = program.allocate_label();

    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::Yield {
        yield_reg,
        end_offset: loop_end,
        subtype_clear_start_reg: 0,
        subtype_clear_count: 0,
    });

    let result_start_reg = program.reg_result_cols_start.ok_or_else(|| {
        crate::LimboError::InternalError(
            "CTAS internal error: result column start register not set".into(),
        )
    })?;
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(result_start_reg),
        count: to_u32(col_count),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: new_table_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    program.emit_insn(Insn::Insert {
        cursor: new_table_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: table_name.to_string(),
    });

    program.emit_insn(Insn::Goto {
        target_pc: loop_start,
    });

    program.preassign_label_to_next_insn(loop_end);
    program.preassign_label_to_next_insn(halt_label);

    program.result_columns.clear();

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn translate_create_table(
    tbl_name: ast::QualifiedName,
    resolver: &Resolver,
    temporary: bool,
    if_not_exists: bool,
    body: ast::CreateTableBody,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    input: &str,
) -> Result<()> {
    // For CTAS, extract the SELECT, determine column info, and convert to a
    // regular ColumnsAndConstraints body + separate SELECT for data insertion.
    let (mut body, ctas_info) = match body {
        ast::CreateTableBody::AsSelect(select) => {
            let (info, col_defs) = derive_ctas_schema(
                select,
                &resolver
                    .name_in_sql(
                        resolver.resolve_namespace_for_create(&tbl_name)?,
                        &tbl_name.name,
                    )
                    .as_ident(),
                resolver,
                program,
                connection,
            )?;
            let body = ast::CreateTableBody::ColumnsAndConstraints {
                columns: col_defs,
                constraints: vec![],
                options: ast::TableOptions::empty(),
            };
            (body, Some(info))
        }
        other => (other, None),
    };

    let namespace = if temporary {
        NamespaceRef::root_of(crate::TEMP_DB_ID)
    } else {
        resolver.resolve_namespace_for_create(&tbl_name)?
    };
    let database_id = namespace.database_id;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    let normalized_tbl_name = normalize_ident(tbl_name.name.as_str());
    let mangled_tbl_name = resolver.mangle_name(namespace, &normalized_tbl_name);
    let tbl_name_in_sql = ast::QualifiedName {
        namespace: None,
        name: resolver.name_in_sql(namespace, &tbl_name.name),
        alias: None,
    };

    // Gate array column types behind the experimental custom types flag.
    if !connection.experimental_custom_types_enabled() {
        if let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = &body {
            for col in columns {
                if col.col_type.as_ref().is_some_and(|t| t.is_array()) {
                    bail_parse_error!(
                        "Array column types require --experimental-custom-types flag"
                    );
                }
            }
        }
    }

    if !connection.experimental_without_rowid_enabled() {
        if let ast::CreateTableBody::ColumnsAndConstraints { options, .. } = &body {
            if options.contains_without_rowid() {
                bail_parse_error!(
                    "WITHOUT ROWID tables are an experimental feature. Enable with --experimental-without-rowid flag"
                );
            }
        }
    }

    let opts = ProgramBuilderOpts::new(1, 30, 1);
    program.extend(&opts);

    if !connection.is_mvcc_bootstrap_connection()
        && is_reserved_object_name(
            connection.dialect().supports_namespaces(),
            &normalized_tbl_name,
        )
        && !connection.is_nested_stmt()
    {
        bail_parse_error!(
            "Object name reserved for internal use: {}",
            tbl_name.name.as_str()
        );
    }

    // Check for name conflicts with existing schema objects
    if let Some(object_type) = resolver.with_schema(database_id, |s| {
        s.get_object_type(namespace.id, &normalized_tbl_name)
    }) {
        match object_type {
            // IF NOT EXISTS suppresses errors for table/view conflicts
            SchemaObjectType::Table | SchemaObjectType::View if if_not_exists => {
                return Ok(());
            }
            _ => {
                // SQLite echoes the new table's name token as written
                // (`table "t" already exists`), except when the name clashes
                // with an index, which gets its own message shape.
                let token = crate::util::identifier_token_for_error(&tbl_name.name);
                match object_type {
                    SchemaObjectType::Table => {
                        bail_parse_error!("table {} already exists", token)
                    }
                    SchemaObjectType::View => {
                        bail_parse_error!("view {} already exists", token)
                    }
                    SchemaObjectType::Index => bail_parse_error!(
                        "there is already an index named {}",
                        tbl_name.name.as_str()
                    ),
                }
            }
        }
    }

    // Like SQLite, the body is only checked when the table is really created,
    // so IF NOT EXISTS on an existing table succeeds whatever the body says.
    validate(
        &body,
        &normalized_tbl_name,
        &resolver.catalog,
        connection.is_nested_stmt(),
    )?;
    let bound_sequence_names = resolver.catalog.dialect.binds_names_at_create()
        && bind_default_sequence_names(&mut body, resolver)?;

    let mut has_autoincrement = false;
    if let ast::CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        ..
    } = &body
    {
        for col in columns {
            for constraint in &col.constraints {
                if let ast::ColumnConstraint::PrimaryKey { auto_increment, .. } =
                    constraint.constraint
                {
                    if auto_increment {
                        has_autoincrement = true;
                        break;
                    }
                }
            }
            if has_autoincrement {
                break;
            }
        }
        if !has_autoincrement {
            for constraint in constraints {
                if let ast::TableConstraint::PrimaryKey { auto_increment, .. } =
                    constraint.constraint
                {
                    if auto_increment {
                        has_autoincrement = true;
                        break;
                    }
                }
            }
        }
    }

    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        database_id,
        Some(SQLITE_TABLEID),
    )?;

    let create_btree_label = program.allocate_label();
    let database_format_reg = program.alloc_register();
    program.emit_insn(Insn::ReadCookie {
        db: database_id,
        dest: database_format_reg,
        cookie: Cookie::DatabaseFormat,
    });
    program.emit_insn(Insn::If {
        reg: database_format_reg,
        target_pc: create_btree_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::DatabaseFormat,
        value: 4,
        p5: 0,
    });
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::DatabaseTextEncoding,
        value: 1,
        p5: 0,
    });
    program.preassign_label_to_next_insn(create_btree_label);

    let created_sequence_table = if has_autoincrement
        && resolver.with_schema(database_id, |s| {
            s.get_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME)
                .is_none()
        }) {
        let schema_master_table = resolver
            .schema()
            .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
            .unwrap();
        let sqlite_schema_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeTable(schema_master_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id,
            root_page: 1i64.into(),
            db: database_id,
        });
        let seq_table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: database_id,
            root: seq_table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });

        let seq_sql = "CREATE TABLE sqlite_sequence(name,seq)";
        emit_schema_entry(
            program,
            resolver,
            sqlite_schema_cursor_id,
            cdc_table.as_ref().map(|x| x.0),
            SchemaEntryType::Table,
            SQLITE_SEQUENCE_TABLE_NAME,
            SQLITE_SEQUENCE_TABLE_NAME,
            seq_table_root_reg,
            Some(seq_sql.to_string()),
        )?;
        true
    } else {
        false
    };

    // For CTAS, use the pre-built SQL string; for regular CREATE TABLE, let
    // the schema dialect format the SQL to store (the SQLite dialect renders
    // canonical text from the AST, a frontend dialect preserves its own
    // input text).
    let sql = if let Some(ref info) = ctas_info {
        info.schema_sql.clone()
    } else if bound_sequence_names {
        connection
            .dialect()
            .format_rewritten_table_sql(&ast::Stmt::CreateTable {
                temporary,
                if_not_exists,
                tbl_name: tbl_name_in_sql,
                body: body.clone(),
            })?
    } else {
        connection
            .dialect()
            .format_table_sql(input, &tbl_name_in_sql, &body)?
    };

    let parse_schema_label = program.allocate_label();

    let table_root_reg = program.alloc_register();
    let btree_flags = match &body {
        ast::CreateTableBody::ColumnsAndConstraints { options, .. }
            if options.contains_without_rowid() =>
        {
            CreateBTreeFlags::new_index()
        }
        _ => CreateBTreeFlags::new_table(),
    };
    program.emit_insn(Insn::CreateBtree {
        db: database_id,
        root: table_root_reg,
        flags: btree_flags,
    });

    // Create an automatic index B-tree if needed
    //
    // NOTE: we are deviating from SQLite bytecode here. For some reason, SQLite first creates a placeholder entry
    // for the table in sqlite_schema, then writes the index to sqlite_schema, then UPDATEs the table placeholder entry
    // in sqlite_schema with actual data.
    //
    // What we do instead is:
    // 1. Create the table B-tree
    // 2. Create the index B-tree
    // 3. Add the table entry to sqlite_schema
    // 4. Add the index entry to sqlite_schema
    //
    // I.e. we skip the weird song and dance with the placeholder entry. Unclear why sqlite does this.
    // The sqlite code has this comment:
    //
    // "This just creates a place-holder record in the sqlite_schema table.
    // The record created does not contain anything yet.  It will be replaced
    // by the real entry in code generated at sqlite3EndTable()."
    //
    // References:
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1355
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L2856-L2871
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1334C5-L1336C65

    let index_regs = collect_autoindexes(&body, program, &mangled_tbl_name, resolver)?;
    if let Some(index_regs) = index_regs.as_ref() {
        for index_reg in index_regs.iter() {
            program.emit_insn(Insn::CreateBtree {
                db: database_id,
                root: *index_reg,
                flags: CreateBTreeFlags::new_index(),
            });
        }
    }

    let table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        database_id,
        Some(SQLITE_TABLEID),
    )?;

    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.as_ref().map(|x| x.0),
        SchemaEntryType::Table,
        &mangled_tbl_name,
        &mangled_tbl_name,
        table_root_reg,
        Some(sql),
    )?;

    if let Some(index_regs) = index_regs {
        for (idx, index_reg) in index_regs.into_iter().enumerate() {
            let index_name = format!(
                "{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{}_{}",
                mangled_tbl_name,
                idx + 1
            );
            emit_schema_entry(
                program,
                resolver,
                sqlite_schema_cursor_id,
                None,
                SchemaEntryType::Index,
                &index_name,
                &mangled_tbl_name,
                index_reg,
                None,
            )?;
        }
    }

    // Create the backing table for the hidden AUTOINCREMENT sequence. The
    // `__turso_internal_autoincrement_` prefix identifies the sequence object;
    // `sequence_backing_table_name` then maps it to the physical
    // `__turso_internal_seq_<sequence-name>` table.
    // Skip if it already exists (e.g. VACUUM INTO copies all tables first).
    if has_autoincrement {
        let autoinc_seq_name = crate::schema::autoincrement_sequence_name(&mangled_tbl_name);
        let backing_table_name =
            crate::translate::sequence::sequence_backing_table_name(&autoinc_seq_name);
        let already_exists = resolver.with_schema(database_id, |s| {
            s.get_btree_table(NamespaceId::ROOT, &backing_table_name)
                .is_some()
        });
        if !already_exists {
            crate::translate::sequence::emit_sequence_backing_table(
                program,
                resolver,
                database_id,
                sqlite_schema_cursor_id,
                &autoinc_seq_name,
                1,        // start
                1,        // increment
                1,        // min
                i64::MAX, // max
                false,    // cycle
            )?;
        }
    }

    program.preassign_label_to_next_insn(parse_schema_label);
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });

    let escaped_tbl_name = escape_sql_string_literal(&mangled_tbl_name);
    let mut parse_schema_where_clause = String::with_capacity(
        "tbl_name = '' AND type != 'trigger'".len()
            + escaped_tbl_name.len()
            + if created_sequence_table {
                " OR tbl_name = 'sqlite_sequence'".len()
            } else {
                0
            },
    );
    parse_schema_where_clause.push_str("tbl_name = '");
    parse_schema_where_clause.push_str(&escaped_tbl_name);
    parse_schema_where_clause.push_str("' AND type != 'trigger'");
    if created_sequence_table {
        parse_schema_where_clause.push_str(" OR tbl_name = 'sqlite_sequence'");
    }

    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(parse_schema_where_clause),
        trigger_target_database_id: None,
    });

    // For CTAS, emit bytecode to populate the new table from the SELECT
    if let Some(info) = ctas_info {
        let col_count = match &body {
            ast::CreateTableBody::ColumnsAndConstraints { columns, .. } => columns.len(),
            _ => unreachable!("CTAS body was converted to ColumnsAndConstraints above"),
        };
        emit_ctas_insert(
            program,
            resolver,
            info.plan,
            &body,
            table_root_reg,
            col_count,
            database_id,
            &normalized_tbl_name,
            connection,
        )?;
    }

    Ok(())
}

/// Binds the sequences named in the column defaults of `body` to the
/// schemas they resolve to now. See [`bind_sequence_names`].
fn bind_default_sequence_names(
    body: &mut ast::CreateTableBody,
    resolver: &Resolver,
) -> Result<bool> {
    let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
        return Ok(false);
    };
    let mut bound = false;
    for column in columns {
        for constraint in &mut column.constraints {
            if let ast::ColumnConstraint::Default(expr) = &mut constraint.constraint {
                bound |= bind_sequence_names(expr, resolver)?;
            }
        }
    }
    Ok(bound)
}

/// Translates `CREATE SCHEMA name`: a new namespace of the main
/// database, recorded as a row of the namespaces table, under the next
/// free id. Only a dialect that supports namespaces has them.
pub fn translate_create_schema(
    name: &ast::Name,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !resolver.catalog.dialect.supports_namespaces() {
        bail_parse_error!("CREATE SCHEMA is not supported");
    }
    let normalized_name = normalize_ident(name.as_str());
    if resolver
        .catalog
        .namespace_database_id(&normalized_name)
        .is_some()
    {
        bail_parse_error!("schema {normalized_name} already exists");
    }
    let id = resolver.with_schema(MAIN_DB_ID, |s| s.next_namespace_id());
    let schema_version = resolver.with_schema(MAIN_DB_ID, |s| s.schema_version);
    program.begin_write_on_database(MAIN_DB_ID, schema_version)?;

    let cursor_id = open_namespaces_table_for_writing(program, resolver)?;
    emit_namespace_row(program, cursor_id, id, &normalized_name);
    program.emit_insn(Insn::AddNamespace {
        db: MAIN_DB_ID,
        namespace: id,
        name: normalized_name,
    });
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });
    Ok(())
}

/// Translates `DROP SCHEMA name`: removes the namespace's row from the
/// namespaces table and forgets its name. The objects stored under its id
/// stay until they are dropped, so the caller drops them first.
pub fn translate_drop_schema(
    name: &ast::Name,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !resolver.catalog.dialect.supports_namespaces() {
        bail_parse_error!("DROP SCHEMA is not supported");
    }
    let normalized_name = normalize_ident(name.as_str());
    let Some(id) = resolver.with_schema(MAIN_DB_ID, |s| s.namespace_named(&normalized_name)) else {
        bail_parse_error!("no such schema: {normalized_name}");
    };
    let schema_version = resolver.with_schema(MAIN_DB_ID, |s| s.schema_version);
    program.begin_write_on_database(MAIN_DB_ID, schema_version)?;

    let cursor_id = open_namespaces_table_for_writing(program, resolver)?;
    let id_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        dest: id_reg,
        value: i64::from(id.as_u32()),
    });
    let missing_row_label = program.allocate_label();
    let delete_done_label = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: cursor_id,
        rowid_reg: id_reg,
        target_pc: missing_row_label,
    });
    program.emit_insn(Insn::Delete {
        cursor_id,
        table_name: TURSO_NAMESPACES_TABLE_NAME.to_string(),
        is_part_of_update: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: delete_done_label,
    });
    program.preassign_label_to_next_insn(missing_row_label);
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: crate::error::SQLITE_ERROR,
        description: format!(
            "{TURSO_NAMESPACES_TABLE_NAME} has no row for namespace {normalized_name}"
        ),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(delete_done_label);

    program.emit_insn(Insn::DropNamespace {
        db: MAIN_DB_ID,
        namespace: id,
    });
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });
    Ok(())
}

/// Opens the namespaces table of the main database for writing. A database
/// that has never created or dropped a namespace has no namespaces table
/// yet, so it is created first with a row for each namespace the database
/// has, which are its default namespaces.
fn open_namespaces_table_for_writing(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
) -> Result<usize> {
    let existing = resolver.with_schema(MAIN_DB_ID, |s| {
        s.get_btree_table(NamespaceId::ROOT, TURSO_NAMESPACES_TABLE_NAME)
    });
    let created = existing.is_none();
    let (table, root_page) = match existing {
        Some(table) => {
            let root_page = RegisterOrLiteral::Literal(table.root_page);
            (table, root_page)
        }
        None => {
            let root_reg = program.alloc_register();
            program.emit_insn(Insn::CreateBtree {
                db: MAIN_DB_ID,
                root: root_reg,
                flags: CreateBTreeFlags::new_table(),
            });
            let create_sql = format!(
                "CREATE TABLE {TURSO_NAMESPACES_TABLE_NAME}(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
            );
            let table = Arc::new(BTreeTable::from_sql(&create_sql, 0)?);
            let schema_table = resolver.with_schema(MAIN_DB_ID, |s| {
                s.get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
                    .expect("every database has sqlite_schema")
            });
            let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: schema_cursor_id,
                root_page: 1i64.into(),
                db: MAIN_DB_ID,
            });
            emit_schema_entry(
                program,
                resolver,
                schema_cursor_id,
                None,
                SchemaEntryType::Table,
                TURSO_NAMESPACES_TABLE_NAME,
                TURSO_NAMESPACES_TABLE_NAME,
                root_reg,
                Some(create_sql),
            )?;
            program.emit_insn(Insn::ParseSchema {
                db: MAIN_DB_ID,
                where_clause: Some(format!(
                    "tbl_name = '{TURSO_NAMESPACES_TABLE_NAME}' AND type != 'trigger'"
                )),
                trigger_target_database_id: None,
            });
            (table, RegisterOrLiteral::Register(root_reg))
        }
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page,
        db: MAIN_DB_ID,
    });
    if created {
        let namespaces = resolver.with_schema(MAIN_DB_ID, |s| {
            s.namespaces()
                .map(|(id, name)| (id, name.to_string()))
                .collect::<Vec<_>>()
        });
        for (id, name) in namespaces {
            emit_namespace_row(program, cursor_id, id, &name);
        }
    }
    Ok(cursor_id)
}

/// Inserts the row of namespace `id` called `name` into the namespaces
/// table open at `cursor_id`, failing if the table has a row with that id.
fn emit_namespace_row(program: &mut ProgramBuilder, cursor_id: usize, id: NamespaceId, name: &str) {
    let id_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        dest: id_reg,
        value: i64::from(id.as_u32()),
    });
    let insert_label = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: cursor_id,
        rowid_reg: id_reg,
        target_pc: insert_label,
    });
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: crate::error::SQLITE_CONSTRAINT_PRIMARYKEY,
        description: format!("UNIQUE constraint failed: {TURSO_NAMESPACES_TABLE_NAME}.id"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(insert_label);
    // The id is the rowid, so the record keeps NULL for it, as SQLite does
    // for an INTEGER PRIMARY KEY column.
    let record_start_reg = program.alloc_registers(2);
    program.emit_insn(Insn::Null {
        dest: record_start_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: record_start_reg + 1,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: id_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: TURSO_NAMESPACES_TABLE_NAME.to_string(),
    });
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaEntryType {
    Table,
    Index,
    View,
    Trigger,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
            SchemaEntryType::View => "view",
            SchemaEntryType::Trigger => "trigger",
        }
    }
}
pub const SQLITE_TABLEID: &str = "sqlite_schema";

#[allow(clippy::too_many_arguments)]
pub fn emit_schema_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    sqlite_schema_cursor_id: usize,
    cdc_table_cursor_id: Option<usize>,
    entry_type: SchemaEntryType,
    name: &str,
    tbl_name: &str,
    root_page_reg: usize,
    sql: Option<String>,
) -> Result<()> {
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let type_reg = program.emit_string8_new_reg(entry_type.as_str().to_string());
    program.emit_string8_new_reg(name.to_string());
    program.emit_string8_new_reg(tbl_name.to_string());

    let table_root_reg = program.alloc_register();
    if root_page_reg == 0 {
        program.emit_insn(Insn::Integer {
            dest: table_root_reg,
            value: 0, // virtual tables in sqlite always have rootpage=0
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: root_page_reg,
            dst_reg: table_root_reg,
            extra_amount: 0,
        });
    }

    let sql_reg = program.alloc_register();
    if let Some(sql) = sql {
        program.emit_string8(sql, sql_reg);
    } else {
        program.emit_null(sql_reg, None);
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(type_reg),
        count: to_u32(5),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: tbl_name.to_string(),
    });

    if let Some(cdc_table_cursor_id) = cdc_table_cursor_id {
        let after_record_reg = if program.capture_data_changes_info().has_after() {
            Some(record_reg)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::INSERT,
            cdc_table_cursor_id,
            rowid_reg,
            None,
            after_record_reg,
            None,
            SQLITE_TABLEID,
        )?;
        emit_cdc_autocommit_commit(program, resolver, Some(cdc_table_cursor_id))?;
    }
    Ok(())
}

/// Check if an automatic PRIMARY KEY index is required for the table.
/// If so, create a register for the index root page and return it.
///
/// An automatic PRIMARY KEY index is not required if:
/// - The table has no PRIMARY KEY
/// - The table has a single-column PRIMARY KEY whose typename is _exactly_ "INTEGER" e.g. not "INT".
///   In this case, the PRIMARY KEY column becomes an alias for the rowid.
///
/// Otherwise, an automatic PRIMARY KEY index is required.
fn collect_autoindexes(
    body: &ast::CreateTableBody,
    program: &mut ProgramBuilder,
    tbl_name: &str,
    resolver: &Resolver,
) -> Result<Option<Vec<usize>>> {
    let table = create_table(
        tbl_name,
        body,
        0,
        resolver.catalog.dialect.supports_namespaces(),
    )?;

    let mut regs: Vec<usize> = Vec::new();

    // include UNIQUE singles, include PK single only if not rowid alias
    for us in table.unique_sets.iter().filter(|us| us.columns.len() == 1) {
        if us.is_primary_key && !table.has_rowid {
            continue;
        }
        let col_name = &us.columns.first().unwrap().name;
        let Some((_pos, col)) = table.get_column(col_name) else {
            bail_parse_error!("Column {col_name} not found in table {}", table.name);
        };

        let needs_index = if us.is_primary_key {
            !(col.primary_key() && col.is_rowid_alias())
        } else {
            // UNIQUE single needs an index
            true
        };

        if needs_index {
            regs.push(program.alloc_register());
        }
    }

    for _us in table.unique_sets.iter().filter(|us| us.columns.len() > 1) {
        if !table.has_rowid && _us.is_primary_key {
            continue;
        }
        regs.push(program.alloc_register());
    }
    if regs.is_empty() {
        Ok(None)
    } else {
        Ok(Some(regs))
    }
}

fn create_vtable_body_to_str(vtab: &ast::CreateVirtualTable, module: Arc<VTabImpl>) -> String {
    let args = vtab
        .args
        .iter()
        .map(|arg| arg.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    let if_not_exists = if vtab.if_not_exists {
        "IF NOT EXISTS "
    } else {
        ""
    };
    let module_args = vtab
        .args
        .iter()
        .map(|a| Value::from_text(a.to_string()))
        .collect::<Vec<_>>();
    let schema = module
        .implementation
        .create_schema(module_args)
        .unwrap_or_default();
    let vtab_args = if let Some(first_paren) = schema.find('(') {
        let closing_paren = schema.rfind(')').unwrap_or_default();
        &schema[first_paren..=closing_paren]
    } else {
        "()"
    };
    format!(
        "CREATE VIRTUAL TABLE {} {} USING {}{}\n /*{}{}*/",
        vtab.tbl_name.name.as_ident(),
        if_not_exists,
        vtab.module_name.as_ident(),
        if args.is_empty() {
            String::new()
        } else {
            format!("({args})")
        },
        vtab.tbl_name.name.as_ident(),
        vtab_args
    )
}

pub fn translate_create_virtual_table(
    vtab: ast::CreateVirtualTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    if connection.mvcc_enabled() {
        bail_parse_error!("Virtual tables are not supported in MVCC mode");
    }
    let ast::CreateVirtualTable {
        if_not_exists,
        tbl_name,
        module_name,
        args,
    } = &vtab;

    let table_name = tbl_name.name.as_str().to_string();
    let module_name_str = module_name.as_str().to_string();
    let args_vec = args.clone();
    let Some(vtab_module) = resolver
        .catalog
        .symbol_table
        .vtab_modules
        .get(&module_name_str)
    else {
        bail_parse_error!("no such module: {}", module_name_str);
    };
    if !vtab_module.module_kind.eq(&VTabKind::VirtualTable) {
        bail_parse_error!("module {} is not a virtual table", module_name_str);
    };
    if resolver
        .schema()
        .get_table(NamespaceId::ROOT, &table_name)
        .is_some()
    {
        if *if_not_exists {
            return Ok(());
        }
        bail_parse_error!(
            "table {} already exists",
            crate::util::identifier_token_for_error(&tbl_name.name)
        );
    }

    let opts = ProgramBuilderOpts::new(2, 40, 2);
    program.extend(&opts);
    let module_name_reg = program.emit_string8_new_reg(module_name_str.clone());
    let table_name_reg = program.emit_string8_new_reg(table_name.clone());
    let args_reg = if !args_vec.is_empty() {
        let args_start = program.alloc_registers(args_vec.len());

        // Emit string8 instructions for each arg
        for (i, arg) in args_vec.iter().enumerate() {
            program.emit_string8(arg.clone(), args_start + i);
        }
        let args_record_reg = program.alloc_register();

        // VCreate expects an array of args as a record
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(args_start),
            count: to_u32(args_vec.len()),
            dest_reg: to_u32(args_record_reg),
            index_name: None,
            affinity_str: None,
        });
        Some(args_record_reg)
    } else {
        None
    };

    program.emit_insn(Insn::VCreate {
        module_name: module_name_reg,
        table_name: table_name_reg,
        args_reg,
    });
    let table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: crate::MAIN_DB_ID,
    });

    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        crate::MAIN_DB_ID,
        Some(SQLITE_TABLEID),
    )?;
    let sql = create_vtable_body_to_str(&vtab, vtab_module.clone());
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Table,
        tbl_name.name.as_str(),
        tbl_name.name.as_str(),
        0, // virtual tables dont have a root page
        Some(sql),
    )?;

    program.emit_insn(Insn::SetCookie {
        db: crate::MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: resolver.schema().schema_version as i32 + 1,
        p5: 0,
    });
    let escaped_table_name = escape_sql_string_literal(&table_name);
    let parse_schema_where_clause =
        format!("tbl_name = '{escaped_table_name}' AND type != 'trigger'");
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
        trigger_target_database_id: None,
    });

    Ok(())
}

/// Validates whether a DROP TABLE operation is allowed on the given table name.
fn validate_drop_table(
    resolver: &CatalogContext,
    namespace: NamespaceRef,
    tbl_name: &str,
    connection: &Arc<Connection>,
) -> Result<()> {
    if !connection.is_nested_stmt()
        && crate::schema::is_system_table(tbl_name)
        // special case, allow dropping `sqlite_stat1`
        && !tbl_name.eq_ignore_ascii_case(STATS_TABLE)
    {
        bail_parse_error!("Cannot drop system table {}", tbl_name);
    }
    // Check if this is a materialized view - if so, refuse to drop it with DROP TABLE
    if resolver.with_schema(namespace.database_id, |schema| {
        schema.is_materialized_view(namespace.id, tbl_name)
    }) {
        bail_parse_error!(
            "Cannot DROP TABLE on materialized view {tbl_name}. Use DROP VIEW instead.",
        );
    }
    let dependent_views = resolver.with_schema(namespace.database_id, |schema| {
        schema.get_dependent_materialized_views(namespace.id, tbl_name)
    });
    if !dependent_views.is_empty() {
        bail_parse_error!(
            "cannot drop table \"{tbl_name}\": it has dependent materialized view(s): {}",
            dependent_views.join(", ")
        );
    }
    Ok(())
}

pub fn translate_drop_table(
    tbl_name: ast::QualifiedName,
    resolver: &mut Resolver,
    if_exists: bool,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let name = tbl_name.name.as_str();
    let opts = ProgramBuilderOpts::new(4, 40, 4);
    program.extend(&opts);

    let Some(namespace) = resolver.find_existing_table(&tbl_name)? else {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!(
            "no such table: {}",
            crate::util::table_name_for_error(&tbl_name)
        );
    };
    let database_id = namespace.database_id;
    let mangled_name = resolver.mangle_name(namespace, &normalize_ident(name));

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let Some(table) = resolver.with_schema(database_id, |s| s.get_table(namespace.id, name)) else {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!(
            "no such table: {}",
            crate::util::table_name_for_error(&tbl_name)
        );
    };
    validate_drop_table(&resolver.catalog, namespace, name, connection)?;
    // Check if foreign keys are enabled and if this table is referenced by foreign keys
    // Fire FK actions (CASCADE, SET NULL, SET DEFAULT) or check for violations (RESTRICT, NO ACTION)
    if connection.foreign_keys_enabled()
        && resolver.with_schema(database_id, |s| {
            s.any_resolved_fks_referencing(namespace.id, name)
        })
    {
        emit_fk_drop_table_check(program, resolver, name, connection, namespace)?;
    }
    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        database_id,
        Some(SQLITE_TABLEID),
    )?;

    let null_reg = program.alloc_register(); //  r1
    program.emit_null(null_reg, None);
    let table_name_and_root_page_register = program.alloc_register(); //  r2, this register is special because it's first used to track table name and then moved root page
    let table_reg = program.emit_string8_new_reg(mangled_name.clone()); //  r3
    program.mark_last_insn_constant();
    let _table_type = program.emit_string8_new_reg("trigger".to_string()); //  r4
    program.mark_last_insn_constant();
    let row_id_reg = program.alloc_register(); //  r5

    let schema_table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .unwrap();
    let sqlite_schema_cursor_id_0 = program.alloc_cursor_id(
        //  cursor 0
        CursorType::BTreeTable(schema_table.clone()),
    );
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id_0,
        root_page: 1i64.into(),
        db: database_id,
    });

    //  1. Remove all entries from the schema table related to the table we are dropping (including triggers)
    //  loop to beginning of schema table
    let end_metadata_label = program.allocate_label();
    let metadata_loop = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_empty: end_metadata_label,
    });
    program.preassign_label_to_next_insn(metadata_loop);

    // start loop on schema table
    program.emit_column_or_rowid(
        sqlite_schema_cursor_id_0,
        2,
        table_name_and_root_page_register,
    );
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: table_name_and_root_page_register,
        rhs: table_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::NOCASE,
    });
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id_0,
        dest: row_id_reg,
    });
    if let Some((cdc_cursor_id, _)) = cdc_table {
        let table_type = program.emit_string8_new_reg("table".to_string()); // r4
        program.mark_last_insn_constant();

        let skip_cdc_label = program.allocate_label();

        let entry_type_reg = program.alloc_register();
        program.emit_column_or_rowid(sqlite_schema_cursor_id_0, 0, entry_type_reg);
        program.emit_insn(Insn::Ne {
            lhs: entry_type_reg,
            rhs: table_type,
            target_pc: skip_cdc_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        let before_record_reg = if program.capture_data_changes_info().has_before() {
            Some(emit_cdc_full_record(
                program,
                resolver,
                schema_table.columns(),
                sqlite_schema_cursor_id_0,
                row_id_reg,
            )?)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::DELETE,
            cdc_cursor_id,
            row_id_reg,
            before_record_reg,
            None,
            None,
            SQLITE_TABLEID,
        )?;
        program.preassign_label_to_next_insn(skip_cdc_label);
    }
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id_0,
        table_name: SQLITE_TABLEID.to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_next: metadata_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_metadata_label);
    // end of loop on schema table
    if let Some((cdc_cursor_id, _)) = cdc_table {
        emit_cdc_autocommit_commit(program, resolver, Some(cdc_cursor_id))?;
    }

    // SQLite removes temp triggers targeting the dropped table.
    // Enumerate the temp schema triggers at translate time (which is
    // safe because a concurrent DDL would bump the schema cookie and
    // force a reprepare) and emit per-trigger bytecode to delete only
    // the rows that should be removed. Filtering in the bytecode by
    // `tbl_name` alone is not enough: two triggers with the same
    // unqualified table name can live in the temp schema but point to
    // different databases (e.g. one on `main.t`, one on `temp.t` when a
    // shadow table exists). We must key on trigger name.
    if database_id != crate::TEMP_DB_ID && resolver.has_temp_database() {
        // A temp schema trigger targets the dropped db iff:
        //   - it explicitly qualifies with the dropped db, or
        //   - it is unqualified AND dropping from main AND temp has no
        //     shadow table of the same name (in which case the
        //     unqualified reference resolves to main).
        let temp_has_shadow = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_table(NamespaceId::ROOT, tbl_name.name.as_str())
                .is_some()
        });
        let trigger_names_to_drop: Vec<String> = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_triggers_for_table(NamespaceId::ROOT, tbl_name.name.as_str())
                .filter(|trigger| match trigger.target_database_id {
                    Some(db_id) => db_id == database_id,
                    None => !temp_has_shadow && database_id == crate::MAIN_DB_ID,
                })
                .map(|trigger| trigger.name.clone())
                .collect()
        });

        if !trigger_names_to_drop.is_empty() {
            let temp_schema_cookie = resolver.with_schema(crate::TEMP_DB_ID, |s| s.schema_version);
            program.begin_write_on_database(crate::TEMP_DB_ID, temp_schema_cookie)?;
            let temp_schema_table = resolver.with_schema(crate::TEMP_DB_ID, |s| {
                s.get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
            });
            if let Some(temp_schema_table) = temp_schema_table {
                let temp_cursor =
                    program.alloc_cursor_id(CursorType::BTreeTable(temp_schema_table));
                program.emit_insn(Insn::OpenWrite {
                    cursor_id: temp_cursor,
                    root_page: 1i64.into(),
                    db: crate::TEMP_DB_ID,
                });
                // Hoist the literal trigger names + `"trigger"` type
                // string into constant registers before the loop.
                let trigger_type_reg = program.emit_string8_new_reg("trigger".to_string());
                program.mark_last_insn_constant();
                let name_regs: Vec<usize> = trigger_names_to_drop
                    .iter()
                    .map(|name| {
                        let reg = program.emit_string8_new_reg(name.clone());
                        program.mark_last_insn_constant();
                        reg
                    })
                    .collect();

                let temp_end_label = program.allocate_label();
                let temp_loop_label = program.allocate_label();
                program.emit_insn(Insn::Rewind {
                    cursor_id: temp_cursor,
                    pc_if_empty: temp_end_label,
                });
                program.preassign_label_to_next_insn(temp_loop_label);
                let temp_next_label = program.allocate_label();
                let temp_delete_label = program.allocate_label();

                // Skip non-trigger rows (column 0 = type).
                let temp_type_reg = program.alloc_register();
                program.emit_column_or_rowid(temp_cursor, 0, temp_type_reg);
                program.emit_insn(Insn::Ne {
                    lhs: temp_type_reg,
                    rhs: trigger_type_reg,
                    target_pc: temp_next_label,
                    flags: CmpInsFlags::default(),
                    collation: CollationSeq::for_internal_values(),
                });

                // Cascade-check name (column 1) against each trigger we
                // want to drop. First match jumps to the delete label.
                let temp_name_reg = program.alloc_register();
                program.emit_column_or_rowid(temp_cursor, 1, temp_name_reg);
                for name_reg in &name_regs {
                    program.emit_insn(Insn::Eq {
                        lhs: temp_name_reg,
                        rhs: *name_reg,
                        target_pc: temp_delete_label,
                        flags: CmpInsFlags::default(),
                        collation: CollationSeq::for_internal_values(),
                    });
                }
                // No name matched — skip the delete.
                program.emit_insn(Insn::Goto {
                    target_pc: temp_next_label,
                });
                program.preassign_label_to_next_insn(temp_delete_label);
                program.emit_insn(Insn::Delete {
                    cursor_id: temp_cursor,
                    table_name: SQLITE_TABLEID.to_string(),
                    is_part_of_update: false,
                });
                program.preassign_label_to_next_insn(temp_next_label);
                program.emit_insn(Insn::Next {
                    cursor_id: temp_cursor,
                    pc_if_next: temp_loop_label,
                    fullscan: false,
                    is_index: false,
                });
                program.preassign_label_to_next_insn(temp_end_label);
            }
        }
    }

    //  2. Destroy the indices within a loop
    let indices: Vec<_> = resolver.with_schema(database_id, |s| {
        s.get_indices(namespace.id, tbl_name.name.as_str())
            .chain(s.get_unowned_backing_btrees(namespace.id, tbl_name.name.as_str()))
            .cloned()
            .collect()
    });
    for index in &indices {
        if index.index_method.is_some() && !index.is_backing_btree_index() {
            // Index methods without backing btree need special destroy handling
            let cursor_id = program.alloc_cursor_index(None, index)?;
            program.emit_insn(Insn::IndexMethodDestroy {
                db: database_id,
                cursor_id,
            });
        } else {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: index.root_page,
                former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
                is_temp: 0,
            });
        }

        //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
        //  Requires support via https://github.com/tursodatabase/turso/pull/768

        //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
        //  Requires support via https://github.com/tursodatabase/turso/pull/768
    }

    //  3. Destroy the table structure
    match table.as_ref() {
        Table::BTree(table) => {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: table.root_page,
                former_root_reg: table_name_and_root_page_register,
                is_temp: 0,
            });
        }
        Table::Virtual(vtab) => {
            if !vtab.is_droppable {
                return Err(crate::LimboError::ParseError(format!(
                    "table {} may not be dropped",
                    vtab.name
                )));
            }
            program.emit_insn(Insn::VDestroy {
                table_name: vtab.name.clone(),
                db: database_id,
            });
        }
        Table::FromClauseSubquery(..) => panic!("FromClauseSubquery can't be dropped"),
        Table::RecursiveCteInput(..) => panic!("recursive CTE inputs cannot be dropped"),
    };

    let schema_data_register = program.alloc_register();
    let schema_row_id_register = program.alloc_register();
    program.emit_null(schema_data_register, Some(schema_row_id_register));

    // All of the following processing needs to be done only if the table is not a virtual table
    if table.btree().is_some() {
        // 4. Open an ephemeral table, and read over the entry from the schema table whose root page was moved in the destroy operation

        // cursor id 1
        let sqlite_schema_cursor_id_1 =
            program.alloc_cursor_id(CursorType::BTreeTable(schema_table.clone()));
        let columns = crate::alloc::try_vec![Column::new(
            Some("rowid".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef::default(),
        )]?;
        let simple_table_rc = Arc::new(BTreeTable::new(
            0, // root_page, not relevant for ephemeral table definition
            "ephemeral_scratch".to_string(),
            crate::alloc::vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ));
        // cursor id 2
        let ephemeral_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(simple_table_rc));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: ephemeral_cursor_id,
            is_table: true,
        });
        let if_not_label = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: table_name_and_root_page_register,
            target_pc: if_not_label,
            jump_if_null: true, //  jump anyway
        });
        program.emit_insn(Insn::OpenRead {
            cursor_id: sqlite_schema_cursor_id_1,
            root_page: 1i64,
            db: database_id,
        });

        let schema_column_0_register = program.alloc_register();
        let schema_column_1_register = program.alloc_register();
        let schema_column_2_register = program.alloc_register();
        let moved_to_root_page_register = program.alloc_register(); //  the register that will contain the root page number the last root page is moved to
        let schema_column_4_register = program.alloc_register();
        let prev_root_page_register = program.alloc_register(); //  the register that will contain the root page number that the last root page was on before VACUUM
        let _r14 = program.alloc_register(); //  Unsure why this register is allocated but putting it in here to make comparison with SQLite easier
        let new_record_register = program.alloc_register();

        //  Loop to copy over row id's from the schema table for rows that have the same root page as the one that was moved
        let copy_schema_to_temp_table_loop_end_label = program.allocate_label();
        let copy_schema_to_temp_table_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: sqlite_schema_cursor_id_1,
            pc_if_empty: copy_schema_to_temp_table_loop_end_label,
        });
        program.preassign_label_to_next_insn(copy_schema_to_temp_table_loop);
        // start loop on schema table
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 3, prev_root_page_register);
        // The label and Insn::Ne are used to skip over any rows in the schema table that don't have the root page that was moved
        let next_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: prev_root_page_register,
            rhs: table_name_and_root_page_register,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        program.emit_insn(Insn::RowId {
            cursor_id: sqlite_schema_cursor_id_1,
            dest: schema_row_id_register,
        });
        program.emit_insn(Insn::Insert {
            cursor: ephemeral_cursor_id,
            key_reg: schema_row_id_register,
            record_reg: schema_data_register,
            flag: InsertFlags::new(),
            table_name: "scratch_table".to_string(),
        });

        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: sqlite_schema_cursor_id_1,
            pc_if_next: copy_schema_to_temp_table_loop,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(copy_schema_to_temp_table_loop_end_label);
        // End loop to copy over row id's from the schema table for rows that have the same root page as the one that was moved

        program.preassign_label_to_next_insn(if_not_label);

        // 5. Open a write cursor to the schema table and re-insert the records placed in the ephemeral table but insert the correct root page now
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id_1,
            root_page: 1i64.into(),
            db: database_id,
        });

        // Loop to copy over row id's from the ephemeral table and then re-insert into the schema table with the correct root page
        let copy_temp_table_to_schema_loop_end_label = program.allocate_label();
        let copy_temp_table_to_schema_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: ephemeral_cursor_id,
            pc_if_empty: copy_temp_table_to_schema_loop_end_label,
        });
        program.preassign_label_to_next_insn(copy_temp_table_to_schema_loop);
        //  start loop on schema table
        program.emit_insn(Insn::RowId {
            cursor_id: ephemeral_cursor_id,
            dest: schema_row_id_register,
        });
        //  the next_label and Insn::NotExists are used to skip patching any rows in the schema table that don't have the row id that was written to the ephemeral table
        let next_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: sqlite_schema_cursor_id_1,
            rowid_reg: schema_row_id_register,
            target_pc: next_label,
        });
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 0, schema_column_0_register);
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 1, schema_column_1_register);
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 2, schema_column_2_register);
        let root_page = table.get_root_page()?;
        program.emit_insn(Insn::Integer {
            value: root_page,
            dest: moved_to_root_page_register,
        });
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 4, schema_column_4_register);
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(schema_column_0_register),
            count: to_u32(5),
            dest_reg: to_u32(new_record_register),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: sqlite_schema_cursor_id_1,
            table_name: SQLITE_TABLEID.to_string(),
            is_part_of_update: false,
        });
        program.emit_insn(Insn::Insert {
            cursor: sqlite_schema_cursor_id_1,
            key_reg: schema_row_id_register,
            record_reg: new_record_register,
            flag: InsertFlags::new(),
            table_name: SQLITE_TABLEID.to_string(),
        });

        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: ephemeral_cursor_id,
            pc_if_next: copy_temp_table_to_schema_loop,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(copy_temp_table_to_schema_loop_end_label);
        // End loop to copy over row id's from the ephemeral table and then re-insert into the schema table with the correct root page
    }

    // If the dropped table had AUTOINCREMENT, clear its `sqlite_sequence` row.
    // Look up the table in the TARGET database's schema (not the main one).
    // `resolver.schema()` returns the MAIN schema, so for `DROP TABLE aux.t`
    // it would either (a) skip the cleanup entirely when main lacks
    // `sqlite_sequence`, leaving a stale high-water-mark in `aux.sqlite_sequence`
    // that the next AUTOINCREMENT INSERT into a same-named table would
    // resume from, or (b) — worse — open the cursor against the attached
    // database using main's root page, corrupting whatever lived there.
    // The matching `OpenWrite` below passes `db: database_id`, so the root
    // page must come from the same schema.
    if let Some(seq_table) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME)
    }) {
        let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));
        let seq_table_name_reg = program.alloc_register();
        let dropped_table_name_reg = program.emit_string8_new_reg(mangled_name.clone());
        program.mark_last_insn_constant();

        program.emit_insn(Insn::OpenWrite {
            cursor_id: seq_cursor_id,
            root_page: seq_table.root_page.into(),
            db: database_id,
        });

        let end_loop_label = program.allocate_label();
        let loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: seq_cursor_id,
            pc_if_empty: end_loop_label,
        });

        program.preassign_label_to_next_insn(loop_start_label);

        program.emit_column_or_rowid(seq_cursor_id, 0, seq_table_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: seq_table_name_reg,
            rhs: dropped_table_name_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default().jump_if_null(),
            collation: CollationSeq::NOCASE,
        });

        program.emit_insn(Insn::Delete {
            cursor_id: seq_cursor_id,
            table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });

        program.preassign_label_to_next_insn(continue_loop_label);
        program.emit_insn(Insn::Next {
            cursor_id: seq_cursor_id,
            pc_if_next: loop_start_label,
            fullscan: false,
            is_index: false,
        });

        program.preassign_label_to_next_insn(end_loop_label);
    }

    // Clean up turso_cdc_version entry for the dropped table (if version table exists)
    if let Some(version_table) = resolver
        .schema()
        .get_table(NamespaceId::ROOT, crate::cdc::TURSO_CDC_VERSION_TABLE_NAME)
        .and_then(|t| t.btree())
    {
        let version_index_name = format!(
            "{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{}_1",
            crate::cdc::TURSO_CDC_VERSION_TABLE_NAME
        );
        let version_index = resolver
            .schema()
            .get_index(
                NamespaceId::ROOT,
                crate::cdc::TURSO_CDC_VERSION_TABLE_NAME,
                &version_index_name,
            )
            .cloned();
        let ver_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(version_table.clone()));
        let ver_index_cursor_id = version_index
            .as_ref()
            .map(|index| program.alloc_cursor_index(None, index))
            .transpose()?;
        let ver_table_name_reg = program.alloc_register();
        let dropped_name_reg = program.emit_string8_new_reg(mangled_name.clone());
        program.mark_last_insn_constant();

        program.emit_insn(Insn::OpenWrite {
            cursor_id: ver_cursor_id,
            root_page: version_table.root_page.into(),
            db: crate::MAIN_DB_ID,
        });
        if let (Some(index), Some(cursor_id)) = (&version_index, ver_index_cursor_id) {
            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: index.root_page.into(),
                db: crate::MAIN_DB_ID,
            });
        }

        let end_ver_loop_label = program.allocate_label();
        let ver_loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: ver_cursor_id,
            pc_if_empty: end_ver_loop_label,
        });

        program.preassign_label_to_next_insn(ver_loop_start_label);

        program.emit_column_or_rowid(ver_cursor_id, 0, ver_table_name_reg);

        let continue_ver_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: ver_table_name_reg,
            rhs: dropped_name_reg,
            target_pc: continue_ver_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });

        if let (Some(index), Some(cursor_id)) = (&version_index, ver_index_cursor_id) {
            turso_assert_eq!(index.columns.len(), 1);
            turso_assert!(index.has_rowid);
            turso_assert!(index.where_clause.is_none());
            turso_assert!(index.columns[0].expr.is_none());

            let index_key_reg = program.alloc_registers(2);
            program.emit_column_or_rowid(
                ver_cursor_id,
                index.columns[0].pos_in_table,
                index_key_reg,
            );
            program.emit_insn(Insn::RowId {
                cursor_id: ver_cursor_id,
                dest: index_key_reg + 1,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: index_key_reg,
                num_regs: 2,
                cursor_id,
                raise_error_if_no_matching_entry: true,
            });
        }

        program.emit_insn(Insn::Delete {
            cursor_id: ver_cursor_id,
            table_name: crate::cdc::TURSO_CDC_VERSION_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });

        program.preassign_label_to_next_insn(continue_ver_label);
        program.emit_insn(Insn::Next {
            cursor_id: ver_cursor_id,
            pc_if_next: ver_loop_start_label,
            fullscan: false,
            is_index: false,
        });

        program.preassign_label_to_next_insn(end_ver_loop_label);
    }

    // Drop the in-memory structures for the table
    program.emit_insn(Insn::DropTable {
        db: database_id,
        namespace: namespace.id,
        _p2: 0,
        _p3: 0,
        table_name: name.to_string(),
    });

    // If the dropped table owned an implicit AUTOINCREMENT sequence, tear
    // down its backing table too: destroy the B-tree root, remove the
    // sqlite_schema entry, and drop the in-memory `Sequence`. Without this
    // the `__turso_internal_seq_<…>` backing table outlives its parent,
    // and a subsequent `CREATE TABLE <name>` with the same name and
    // AUTOINCREMENT under MVCC would resume nextval at the old watermark
    // (and the orphan table bloats the DB indefinitely in WAL mode).
    // Schema-cookie bump is shared with the table-drop below; the helper
    // never touches it. No-op when has_autoincrement is false or the
    // sequence/backing-table is somehow already missing — same idempotency
    // contract as DROP SEQUENCE IF EXISTS.
    if table.btree().is_some_and(|bt| bt.has_autoincrement) {
        let seq_name = crate::schema::autoincrement_sequence_name(&mangled_name);
        crate::translate::sequence::emit_drop_sequence_cleanup(
            program,
            resolver,
            NamespaceRef::root_of(database_id),
            &seq_name,
        )?;
    }

    let current_schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: current_schema_version as i32 + 1,
        p5: 0,
    });

    Ok(())
}

/// Validate an encode or decode expression for safety.
/// Rejects subqueries, aggregates, and window functions.
fn validate_type_expr(expr: &ast::Expr, kind: &str, resolver: &CatalogContext) -> Result<()> {
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries prohibited in {kind} expressions");
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("window functions prohibited in {kind} expressions");
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), args.len())? {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!(
                            "aggregate functions prohibited in {kind} expressions: {}",
                            name.as_str()
                        );
                    }
                    // Reject known non-deterministic built-in functions.
                    // External functions are excluded from this check since
                    // they default to non-deterministic but may actually be
                    // deterministic (e.g. uuid_blob).
                    if !matches!(func, Func::External(_)) && !func.is_deterministic() {
                        bail_parse_error!(
                            "non-deterministic functions prohibited in {kind} expressions: {}",
                            name.as_str()
                        );
                    }
                }
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                bail_parse_error!(
                    "aggregate functions prohibited in {kind} expressions: {}",
                    name.as_str()
                );
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Shared persistence logic for CREATE TYPE / CREATE DOMAIN.
/// Persists the type SQL into __turso_internal_types and registers it in memory.
fn persist_type_definition(
    normalized_name: String,
    sql: String,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    // Ensure sqlite_turso_types table exists (lazy creation)
    let types_table: Arc<BTreeTable>;
    let types_root_page: RegisterOrLiteral<i64>;
    let existing_types_table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, TURSO_TYPES_TABLE_NAME);
    let creating_types_table = existing_types_table.is_none();

    if let Some(existing) = existing_types_table {
        types_table = existing.clone();
        types_root_page = RegisterOrLiteral::Literal(existing.root_page);
    } else {
        // Create the sqlite_turso_types btree
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: MAIN_DB_ID,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let create_sql =
            format!("CREATE TABLE {TURSO_TYPES_TABLE_NAME}(name TEXT PRIMARY KEY, sql TEXT)");
        types_table = Arc::new(BTreeTable::from_sql(&create_sql, 0)?);
        types_root_page = RegisterOrLiteral::Register(table_root_reg);

        // Register it in sqlite_schema so it persists
        let schema_table = resolver
            .schema()
            .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
            .unwrap();
        let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: schema_cursor_id,
            root_page: 1i64.into(),
            db: MAIN_DB_ID,
        });
        emit_schema_entry(
            program,
            resolver,
            schema_cursor_id,
            None,
            SchemaEntryType::Table,
            TURSO_TYPES_TABLE_NAME,
            TURSO_TYPES_TABLE_NAME,
            table_root_reg,
            Some(create_sql),
        )?;

        // `SetCookie` is what marks the transaction as having changed the schema, so that a
        // rollback restores the connection schema (see `Pager::rollback`). It has to precede
        // `ParseSchema`: if a later instruction fails - `AddType` re-parses the type SQL and can -
        // the schema would otherwise keep the types table while the rollback removed its page.
        emit_schema_cookie(resolver, program);

        // Parse schema to register the new table in-memory
        program.emit_insn(Insn::ParseSchema {
            db: schema_cursor_id,
            where_clause: Some(format!(
                "tbl_name = '{TURSO_TYPES_TABLE_NAME}' AND type != 'trigger'"
            )),
            trigger_target_database_id: None,
        });
    }

    // Open sqlite_turso_types for writing
    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_root_page,
        db: MAIN_DB_ID,
    });

    // Insert (name, sql) record
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: types_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    let name_reg = program.emit_string8_new_reg(normalized_name);
    program.emit_string8_new_reg(sql.clone());
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(name_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: types_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
    });

    // Add the type to the in-memory registry
    program.emit_insn(Insn::AddType {
        db: MAIN_DB_ID,
        sql,
    });

    if !creating_types_table {
        // Only when this path emitted no cookie above. `AddType` registers the type in the
        // connection schema but allocates nothing, so its failure needs no restore and a cookie
        // emitted before it only leaves the connection and the database out of step: inside an
        // explicit transaction, the statement rollback restores neither and `COMMIT` fails with
        // "Database schema changed".
        emit_schema_cookie(resolver, program);
    }

    Ok(())
}

/// Marks the transaction as having changed the schema, so that a rollback restores the connection
/// schema, and bumps the schema cookie of the main database.
fn emit_schema_cookie(resolver: &Resolver, program: &mut ProgramBuilder) {
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });
}

pub fn translate_create_type(
    type_name: &str,
    body: &ast::CreateTypeBody,
    if_not_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let normalized_name = normalize_ident(type_name);

    // Reject names that shadow SQLite base types
    let is_base_type = turso_macros::match_ignore_ascii_case!(match normalized_name.as_bytes() {
        b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" | b"ANY" => true,
        _ => false,
    });
    if is_base_type {
        bail_parse_error!("cannot create type \"{normalized_name}\": name is a built-in type");
    }

    // Check if type already exists
    if resolver
        .schema()
        .get_type_def_unchecked(&normalized_name)
        .is_some()
    {
        if if_not_exists {
            return Ok(());
        }
        bail_parse_error!("type {normalized_name} already exists");
    }

    // Validate encode/coerce/decode expressions for safety (only for custom types)
    if let ast::CreateTypeBody::CustomType {
        ref params,
        ref encode,
        ref coerce,
        ref decode,
        ..
    } = body
    {
        if let Some(ref encode) = encode {
            validate_type_expr(encode, "ENCODE", &resolver.catalog)?;
        }
        if let Some(ref coerce) = coerce {
            validate_type_expr(coerce, "COERCE", &resolver.catalog)?;
        }
        if let Some(ref decode) = decode {
            validate_type_expr(decode, "DECODE", &resolver.catalog)?;
        }
        crate::schema::check_type_param_use(params, encode, decode)?;
    }

    // Build canonical SQL (without IF NOT EXISTS) for persistence
    let sql = build_create_type_sql(&normalized_name, body);

    persist_type_definition(normalized_name, sql, resolver, program)
}

/// Build canonical CREATE TYPE SQL from a normalized name and parsed body.
fn build_create_type_sql(name: &str, body: &ast::CreateTypeBody) -> String {
    use crate::util::quote_identifier as quote_ident;
    fn quote_string_literal(s: &str) -> String {
        s.replace('\'', "''")
    }

    match body {
        ast::CreateTypeBody::CustomType {
            params,
            base,
            encode,
            coerce,
            decode,
            default,
            operators,
        } => {
            let mut sql = if params.is_empty() {
                format!(
                    "CREATE TYPE {} BASE {}",
                    quote_ident(name),
                    quote_ident(base)
                )
            } else {
                let param_strs: Vec<String> = params
                    .iter()
                    .map(|p| match &p.ty {
                        Some(ty) => format!("{} {}", quote_ident(&p.name), ty),
                        None => quote_ident(&p.name),
                    })
                    .collect();
                format!(
                    "CREATE TYPE {}({}) BASE {}",
                    quote_ident(name),
                    param_strs.join(", "),
                    quote_ident(base)
                )
            };
            if let Some(ref encode) = encode {
                sql.push_str(&format!(" ENCODE {encode}"));
            }
            if let Some(ref coerce) = coerce {
                sql.push_str(&format!(" COERCE {coerce}"));
            }
            if let Some(ref decode) = decode {
                sql.push_str(&format!(" DECODE {decode}"));
            }
            if let Some(ref default) = default {
                sql.push_str(&format!(" DEFAULT {default}"));
            }
            for op in operators {
                match &op.func_name {
                    Some(func_name) => sql.push_str(&format!(
                        " OPERATOR '{}' {}",
                        quote_string_literal(&op.op),
                        quote_ident(func_name)
                    )),
                    None => sql.push_str(&format!(" OPERATOR '{}'", quote_string_literal(&op.op))),
                }
            }
            sql
        }
        ast::CreateTypeBody::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "{} {}",
                        quote_ident(f.name.as_str()),
                        quote_ident(&f.field_type.name)
                    )
                })
                .collect();
            format!(
                "CREATE TYPE {} AS STRUCT({})",
                quote_ident(name),
                field_strs.join(", ")
            )
        }
        ast::CreateTypeBody::Union(fields) => {
            let variant_strs: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "{} {}",
                        quote_ident(f.name.as_str()),
                        quote_ident(&f.field_type.name)
                    )
                })
                .collect();
            format!(
                "CREATE TYPE {} AS UNION({})",
                quote_ident(name),
                variant_strs.join(", ")
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn translate_create_domain(
    domain_name: &str,
    base_type: &str,
    not_null: bool,
    constraints: &[ast::DomainConstraint],
    default: Option<Box<ast::Expr>>,
    if_not_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let normalized_name = normalize_ident(domain_name);

    // Reject names that shadow SQLite base types
    let is_base_type = turso_macros::match_ignore_ascii_case!(match normalized_name.as_bytes() {
        b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" | b"ANY" => true,
        _ => false,
    });
    if is_base_type {
        bail_parse_error!("cannot create domain \"{normalized_name}\": name is a built-in type");
    }

    // Check if type/domain already exists
    if resolver
        .schema()
        .get_type_def_unchecked(&normalized_name)
        .is_some()
    {
        if if_not_exists {
            return Ok(());
        }
        bail_parse_error!("type {normalized_name} already exists");
    }

    // Validate base type exists — must be a primitive or a registered type
    let base_normalized = normalize_ident(base_type);
    let is_primitive = turso_macros::match_ignore_ascii_case!(match base_normalized.as_bytes() {
        b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" => true,
        _ => false,
    });
    if !is_primitive
        && resolver
            .schema()
            .get_type_def_unchecked(&base_normalized)
            .is_none()
    {
        bail_parse_error!("base type \"{base_type}\" does not exist");
    }

    // Validate no cycles — check if base type chain is acyclic
    if !is_primitive {
        resolver
            .schema()
            .resolve_base_type_chain(&base_normalized)?;
    }

    // Validate CHECK and DEFAULT expressions (reject subqueries, aggregates, etc.)
    for c in constraints {
        validate_type_expr(&c.check, "domain CHECK", &resolver.catalog)?;
    }
    if let Some(ref def) = default {
        validate_type_expr(def, "domain DEFAULT", &resolver.catalog)?;
    }

    // Build the CREATE DOMAIN SQL for persistence
    let sql = {
        let mut s = format!("CREATE DOMAIN {normalized_name} AS {base_type}");
        if let Some(ref def) = default {
            s.push_str(&format!(" DEFAULT {def}"));
        }
        if not_null {
            s.push_str(" NOT NULL");
        }
        for c in constraints {
            if let Some(ref name) = c.name {
                s.push_str(&format!(" CONSTRAINT {name}"));
            }
            s.push_str(&format!(" CHECK ({})", c.check));
        }
        s
    };

    persist_type_definition(normalized_name, sql, resolver, program)
}

pub fn translate_drop_type(
    type_name: &str,
    if_exists: bool,
    is_domain_drop: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let normalized_name = normalize_ident(type_name);
    let kind = if is_domain_drop { "domain" } else { "type" };

    // Check if type exists
    let type_def = resolver.schema().get_type_def_unchecked(&normalized_name);
    if type_def.is_none() {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("no such {kind}: {normalized_name}");
    }

    let type_def = type_def.unwrap();

    // Validate that DROP TYPE targets a type and DROP DOMAIN targets a domain
    let target_is_domain = type_def.is_domain;
    if is_domain_drop && !target_is_domain {
        bail_parse_error!("{normalized_name} is a type, not a domain. Use DROP TYPE instead");
    }
    if !is_domain_drop && target_is_domain {
        bail_parse_error!("{normalized_name} is a domain, not a type. Use DROP DOMAIN instead");
    }

    // Check if built-in type
    if type_def.is_builtin {
        bail_parse_error!("cannot drop built-in type: {normalized_name}");
    }

    // Check if anything in any database uses this type
    let registry_key = type_registry_key(type_name);
    let mut database_ids = resolver.attached_database_ids_in_search_order()?;
    database_ids.set(MAIN_DB_ID)?;
    if resolver.has_temp_database() {
        database_ids.set(crate::TEMP_DB_ID)?;
    }
    for database_id in database_ids {
        let type_use =
            resolver.with_schema(database_id, |schema| find_type_use(schema, &registry_key))?;
        if let Some(type_use) = type_use {
            bail_parse_error!("cannot drop type {normalized_name}: used by {type_use}");
        }
    }

    // Check if any other type/domain depends on this type
    for (name, td) in resolver.schema().type_registry.iter() {
        if type_registry_key(name) != registry_key && type_def_uses_type(td, &registry_key)? {
            bail_parse_error!(
                "cannot drop type {}: type {} depends on it",
                normalized_name,
                name
            );
        }
    }

    // Open cursor to sqlite_turso_types table
    let types_table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, TURSO_TYPES_TABLE_NAME)
        .ok_or_else(|| crate::LimboError::ParseError(format!("no such type: {normalized_name}")))?;
    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_table.root_page.into(),
        db: MAIN_DB_ID,
    });

    // Search for matching row: name=type_name (col 0)
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        dest: name_reg,
        value: normalized_name.clone(),
    });

    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: types_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    // Read name (col 0)
    let col0_reg = program.alloc_register();
    program.emit_column_or_rowid(types_cursor_id, 0, col0_reg);

    let skip_delete_label = program.allocate_label();

    // Check name=type_name
    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });

    // Delete matching row
    program.emit_insn(Insn::Delete {
        cursor_id: types_cursor_id,
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(skip_delete_label);

    program.emit_insn(Insn::Next {
        cursor_id: types_cursor_id,
        pc_if_next: loop_start_label,
        fullscan: false,
        is_index: false,
    });

    program.preassign_label_to_next_insn(end_loop_label);

    // Remove from in-memory schema
    program.emit_insn(Insn::DropType {
        db: MAIN_DB_ID,
        type_name: normalized_name,
    });

    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });

    Ok(())
}

/// Describes the first object in `schema` that uses the type, such as a column
/// of that type or a view that casts to it.
fn find_type_use(schema: &Schema, registry_key: &str) -> Result<Option<String>> {
    for namespace in schema.namespace_ids() {
        for table in schema.tables(namespace) {
            if let Some(type_use) = find_type_use_in_table(schema, namespace, table, registry_key)?
            {
                return Ok(Some(type_use));
            }
        }
        for index in schema.indexes(namespace) {
            let index_exprs = index
                .columns
                .iter()
                .filter_map(|column| column.expr.as_deref())
                .chain(index.where_clause.as_deref());
            for expr in index_exprs {
                if expr_casts_to_type(expr, registry_key)? {
                    return Ok(Some(format!("index {}", index.name)));
                }
            }
        }
        for (name, view) in schema.views(namespace) {
            if select_casts_to_type(&view.select_stmt, registry_key)? {
                return Ok(Some(format!("view {name}")));
            }
        }
        for trigger in schema.triggers(namespace) {
            if trigger_casts_to_type(trigger, registry_key)? {
                return Ok(Some(format!("trigger {}", trigger.name)));
            }
        }
    }
    Ok(None)
}

fn find_type_use_in_table(
    schema: &Schema,
    namespace: NamespaceId,
    table: &Table,
    registry_key: &str,
) -> Result<Option<String>> {
    let table_name = table.get_name();
    for column in table.columns() {
        let column_name = column.name.as_deref().unwrap_or("?");
        if type_registry_key(&column.ty_str) == registry_key {
            return Ok(Some(format!("column {column_name} in table {table_name}")));
        }
        if let Some(default) = &column.default {
            if expr_casts_to_type(default, registry_key)? {
                return Ok(Some(format!(
                    "the default of column {column_name} in table {table_name}"
                )));
            }
        }
        if let GeneratedType::Virtual { expr, .. } = column.generated_type() {
            if expr_casts_to_type(expr, registry_key)? {
                return Ok(Some(format!(
                    "generated column {column_name} in table {table_name}"
                )));
            }
        }
    }
    if let Table::BTree(btree) = table {
        for check in &btree.check_constraints {
            if expr_casts_to_type(&check.expr, registry_key)? {
                return Ok(Some(format!("a CHECK constraint in table {table_name}")));
            }
        }
    }
    if let Some(view) = schema.get_materialized_view(namespace, table_name) {
        if select_casts_to_type(&view.lock().select_stmt, registry_key)? {
            return Ok(Some(format!("materialized view {table_name}")));
        }
    }
    Ok(None)
}

fn type_def_uses_type(type_def: &TypeDef, registry_key: &str) -> Result<bool> {
    if type_registry_key(type_def.base()) == registry_key {
        return Ok(true);
    }
    let struct_field_types = type_def
        .struct_def()
        .into_iter()
        .flat_map(|def| def.fields.iter().map(|field| &field.type_name));
    let union_variant_types = type_def
        .union_def()
        .into_iter()
        .flat_map(|def| def.variants.iter().map(|variant| &variant.type_name));
    if struct_field_types
        .chain(union_variant_types)
        .any(|name| type_registry_key(name) == registry_key)
    {
        return Ok(true);
    }
    let exprs = [
        type_def.encode(),
        type_def.coerce(),
        type_def.decode(),
        type_def.default_expr(),
    ];
    let domain_checks = type_def.domain_checks.iter().map(|c| c.check.as_ref());
    for expr in exprs.into_iter().flatten().chain(domain_checks) {
        if expr_casts_to_type(expr, registry_key)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn select_casts_to_type(select: &ast::Select, registry_key: &str) -> Result<bool> {
    let mut found = false;
    walk_select_expressions(select, &mut |expr| {
        found |= is_cast_to_type(expr, registry_key);
        Ok(WalkControl::Continue)
    })?;
    Ok(found)
}

fn trigger_casts_to_type(trigger: &Trigger, registry_key: &str) -> Result<bool> {
    let mut found = match &trigger.when_clause {
        Some(when_clause) => expr_casts_to_type(when_clause, registry_key)?,
        None => false,
    };
    for command in &trigger.commands {
        walk_trigger_cmd_expressions(command, &mut |expr| {
            found |= is_cast_to_type(expr, registry_key);
            Ok(WalkControl::Continue)
        })?;
    }
    Ok(found)
}

fn expr_casts_to_type(expr: &ast::Expr, registry_key: &str) -> Result<bool> {
    let mut found = false;
    walk_expr_with_subqueries(expr, &mut |expr| {
        found |= is_cast_to_type(expr, registry_key);
        Ok(WalkControl::Continue)
    })?;
    Ok(found)
}

fn is_cast_to_type(expr: &ast::Expr, registry_key: &str) -> bool {
    matches!(
        expr,
        ast::Expr::Cast { type_name: Some(type_name), .. }
            if type_registry_key(&type_name.name) == registry_key
    )
}
