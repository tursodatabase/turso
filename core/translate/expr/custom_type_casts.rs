use super::*;
use crate::translate::expr::expr_type::{same_type_def, type_of, BuiltinType, ExprType, TypeEnv};

/// Refuses a CAST to a custom type with arguments it does not take, such as
/// `CAST(x AS date(3))`, and a CAST that SQLite would do on a value it would
/// misread, such as `CAST(iv AS integer)` on an interval. A value of such a
/// type casts only to its own type and to text. Only text, NULL or a value
/// whose type is not known before the statement runs casts to such a type;
/// the cast reads that value as text and the type's ENCODE checks the text.
pub(crate) fn check_custom_cast(expr: &ast::Expr, env: &TypeEnv) -> Result<()> {
    let ast::Expr::Cast {
        expr: source,
        type_name,
    } = expr
    else {
        return Ok(());
    };
    let source_type = type_of(source, env);
    let target_type = type_of(expr, env);
    if let Some(type_name) = type_name {
        check_type_modifier(&target_type, type_name)?;
    }
    let allowed = match (
        sqlite_functions_read(&source_type, env.schema())?,
        sqlite_functions_read(&target_type, env.schema())?,
    ) {
        (true, true) => true,
        (true, false) => matches!(
            source_type,
            ExprType::Untyped
                | ExprType::Null
                | ExprType::NotKnownYet
                | ExprType::Builtin(BuiltinType::Text | BuiltinType::Any)
        ),
        (false, true) => matches!(target_type, ExprType::Builtin(BuiltinType::Text)),
        (false, false) => match (&source_type, &target_type) {
            (ExprType::Custom { type_def: from, .. }, ExprType::Custom { type_def: to, .. }) => {
                same_type_def(from, to)
            }
            _ => unreachable!("sqlite_functions_read is false only for a custom type"),
        },
    };
    if !allowed {
        return Err(LimboError::NotSupported(format!(
            "cast from {source_type} to {target_type}"
        )));
    }
    Ok(())
}

fn check_type_modifier(target_type: &ExprType, type_name: &ast::Type) -> Result<()> {
    let target_element = match target_type {
        ExprType::Array { element, .. } => element.as_ref(),
        scalar => scalar,
    };
    match target_element {
        ExprType::Custom { type_def, .. } => type_def.check_type_modifier(type_name),
        // A plain SQLite type takes and ignores arguments, as in SQLite.
        ExprType::Builtin(_) => Ok(()),
        ExprType::Array { .. }
        | ExprType::Untyped
        | ExprType::Null
        | ExprType::Unknown
        | ExprType::NotKnownYet
        | ExprType::RowValue => {
            unreachable!("cast_type gives a custom or plain type, or an array of one")
        }
    }
}
