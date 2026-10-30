//! The SQLite dialect's type rule for CHECK constraints: in a STRICT table,
//! every comparison must have operands whose types can be determined and
//! are compatible, or CREATE TABLE fails. Comparing values of different
//! types gives results that are rarely what the user meant, such as an
//! integer always sorting before any text.

use crate::function::{Func, MathFunc, ScalarFunc};
use crate::translate::catalog::CatalogContext;
use crate::translate::planner::ROWID_STRS;
use crate::util::{column_declared_type, dequote, normalize_ident};
use crate::{bail_parse_error, Result};
use turso_parser::ast;

/// Reject a CHECK constraint of a STRICT table that compares values whose
/// types differ or cannot be determined.
pub(crate) fn validate_check_constraint_types(
    columns: &[ast::ColumnDefinition],
    constraints: &[ast::NamedTableConstraint],
    options: &ast::TableOptions,
    resolver: &CatalogContext,
) -> Result<()> {
    if !options.contains_strict() {
        return Ok(());
    }
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
    Ok(())
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

/// Resolve the return type of a built-in function for CHECK constraint type checking.
fn resolve_func_return_type(
    func: &Func,
    name: &str,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &CatalogContext,
) -> Result<CheckExprType> {
    match func {
        Func::Scalar(sf) => resolve_scalar_func_return_type(sf, args, columns, resolver),
        Func::Math(mf) => resolve_math_func_return_type(mf),
        #[cfg(feature = "json")]
        Func::Json(jf) => resolve_json_func_return_type(jf),
        Func::Agg(_) => bail_parse_error!("misuse of aggregate function {}()", name),
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

/// Resolve the return type of a scalar function.
fn resolve_scalar_func_return_type(
    func: &ScalarFunc,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &CatalogContext,
) -> Result<CheckExprType> {
    match func {
        // Functions that always return INTEGER
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
        | ScalarFunc::ValidateIpAddr
        | ScalarFunc::GetByte
        | ScalarFunc::UnixEpoch => Ok(CheckExprType::Integer),

        // Functions that always return TEXT
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

        // Functions that always return REAL
        ScalarFunc::Round | ScalarFunc::JulianDay => Ok(CheckExprType::Real),

        // Functions that always return BLOB
        ScalarFunc::RandomBlob | ScalarFunc::ZeroBlob | ScalarFunc::Unhex | ScalarFunc::SetByte => {
            Ok(CheckExprType::Blob)
        }

        // Functions whose return type depends on arguments
        ScalarFunc::Abs | ScalarFunc::Nullif => {
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

        // Internal/custom type functions
        ScalarFunc::TestUintEncode
        | ScalarFunc::TestUintDecode
        | ScalarFunc::NumericEncode
        | ScalarFunc::NumericFit
        | ScalarFunc::NumericDecode
        | ScalarFunc::NumericAdd
        | ScalarFunc::NumericSub
        | ScalarFunc::NumericMul
        | ScalarFunc::NumericDiv
        | ScalarFunc::NumericMod
        | ScalarFunc::NumericNeg => Ok(CheckExprType::Blob),

        // Remaining functions — treat as ANY
        _ => Ok(CheckExprType::Any),
    }
}

/// Resolve the return type of a math function.
fn resolve_math_func_return_type(func: &MathFunc) -> Result<CheckExprType> {
    match func {
        // Floor/ceil/trunc return INTEGER for integer inputs, but always produce numeric results
        MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc => {
            Ok(CheckExprType::Integer)
        }
        // All other math functions return REAL
        _ => Ok(CheckExprType::Real),
    }
}

/// Resolve the return type of a JSON function.
#[cfg(feature = "json")]
fn resolve_json_func_return_type(func: &crate::function::JsonFunc) -> Result<CheckExprType> {
    use crate::function::JsonFunc;
    match func {
        // Functions that return TEXT (JSON text)
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
        | JsonFunc::JsonType => Ok(CheckExprType::Text),

        // Functions that return BLOB (JSONB binary)
        JsonFunc::Jsonb
        | JsonFunc::JsonbArray
        | JsonFunc::JsonbObject
        | JsonFunc::JsonbPatch
        | JsonFunc::JsonbRemove
        | JsonFunc::JsonbReplace
        | JsonFunc::JsonbInsert
        | JsonFunc::JsonbSet => Ok(CheckExprType::Blob),

        // Functions that return INTEGER
        JsonFunc::JsonArrayLength | JsonFunc::JsonErrorPosition | JsonFunc::JsonValid => {
            Ok(CheckExprType::Integer)
        }

        // Extract functions can return any type
        JsonFunc::JsonExtract
        | JsonFunc::JsonbExtract
        | JsonFunc::JsonArrowExtract
        | JsonFunc::JsonArrowShiftExtract => Ok(CheckExprType::Any),
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
