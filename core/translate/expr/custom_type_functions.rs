//! SQLite's functions called on a value they would misread, such as `sum` on
//! an interval.

use super::*;
use crate::schema::Schema;
use crate::translate::expr::expr_type::{type_of, ExprType, TypeEnv};

/// Refuses a call of one of SQLite's functions that reads values, such as
/// `sum(iv)` or `abs(iv)`, on a type whose values it would misread, such as
/// interval. A function that only counts values or compares them in their
/// type's order, such as count or the aggregate min, takes a value of any
/// type. See [Func::reads_argument_values].
pub(crate) fn check_function_reads(expr: &ast::Expr, env: &TypeEnv) -> Result<()> {
    let (name, args): (&str, Vec<&ast::Expr>) = match expr {
        ast::Expr::FunctionCall { name, args, .. } => {
            (name.as_str(), args.iter().map(AsRef::as_ref).collect())
        }
        // `x LIKE y ESCAPE z` is the call like(y, x, z), as in SQLite's
        // parser (parse.y, `expr likeop expr`).
        ast::Expr::Like {
            lhs,
            op,
            rhs,
            escape,
            ..
        } => {
            let mut args = vec![rhs.as_ref(), lhs.as_ref()];
            args.extend(escape.as_deref());
            (like_function_name(*op), args)
        }
        _ => return Ok(()),
    };
    // Translation reports a function that does not exist.
    let Some(func) = env.resolve_function(name, args.len())? else {
        return Ok(());
    };
    if !func.reads_argument_values() {
        return Ok(());
    }
    let arg_types: Vec<ExprType> = args.iter().map(|arg| type_of(arg, env)).collect();
    for arg_type in &arg_types {
        if !sqlite_functions_read(arg_type, env.schema())? {
            let arg_types: Vec<String> = arg_types.iter().map(ToString::to_string).collect();
            return Err(LimboError::NotSupported(format!(
                "function {func}({})",
                arg_types.join(", ")
            )));
        }
    }
    Ok(())
}

fn like_function_name(op: ast::LikeOperator) -> &'static str {
    match op {
        ast::LikeOperator::Like => "like",
        ast::LikeOperator::Glob => "glob",
        ast::LikeOperator::Regexp => "regexp",
        ast::LikeOperator::Match => "match",
    }
}

/// Whether SQLite's functions may read a value of type `expr_type`.
pub(super) fn sqlite_functions_read(expr_type: &ExprType, schema: &Schema) -> Result<bool> {
    Ok(match expr_type {
        ExprType::Custom { type_def, .. } => schema
            .resolve_type_unchecked(&type_def.name)?
            .ok_or_else(|| {
                LimboError::InternalError(format!("type {} is not registered", type_def.name))
            })?
            .sqlite_functions_read_values(),
        // An array holds no value of a type whose values SQLite's functions
        // would misread: CREATE TABLE and CAST refuse an array of such a type.
        ExprType::Array { .. } => true,
        // An expression with a subquery whose type is not known yet is
        // checked again once the subquery is planned.
        ExprType::Builtin(_)
        | ExprType::Untyped
        | ExprType::Null
        | ExprType::Unknown
        | ExprType::NotKnownYet
        | ExprType::RowValue => true,
    })
}
