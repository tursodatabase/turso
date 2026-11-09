//! The statements that name a function: DROP, ALTER and COMMENT ON FUNCTION,
//! PROCEDURE, ROUTINE and AGGREGATE, and CALL.
//!
//! The only functions in tursopg are the built-in functions of PostgreSQL,
//! which are in pg_catalog. PostgreSQL has no built-in procedures.

use super::expr::{self, ExpressionKind};
use super::function::Attributes;
use super::types::{find_type, type_name_text, TypeRef};
use super::{
    check_schema, error, permission_denied, qualified_name, strings, RoutineCatalog, RoutineObject,
};
use crate::builtins::{self, Function, FunctionKind};
use crate::sqlstate::{
    SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST, SQLSTATE_INVALID_PARAMETER_VALUE,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT, SQLSTATE_WRONG_OBJECT_TYPE,
};
use crate::translator::not_supported;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self as pb, ObjectWithArgs};

/// SQLSTATE 42725: a name finds more than one function.
const SQLSTATE_AMBIGUOUS_FUNCTION: &str = "42725";
/// SQLSTATE 42723: a function with the name and argument types exists.
const SQLSTATE_DUPLICATE_FUNCTION: &str = "42723";

pub(super) fn check_drop_functions(
    kind: RoutineObject,
    objects: &[pb::Node],
    missing_ok: bool,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    // PostgreSQL's RemoveObjects() finds each object and checks that the
    // current role owns it, and then tries to drop them all.
    let mut found = Vec::new();
    for object in objects {
        let Some(Node::ObjectWithArgs(object)) = &object.node else {
            continue;
        };
        let Some(function) = find_function(kind, object, missing_ok, catalog)? else {
            continue;
        };
        if kind == RoutineObject::Function && function.kind == FunctionKind::Aggregate {
            return Err(error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("\"{}\" is an aggregate function", written_name(object)),
            ));
        }
        check_owner(kind, object, catalog)?;
        found.push(function);
    }
    match found.first() {
        Some(function) => Err(error(
            SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
            format!(
                "cannot drop function {} because it is required by the database system",
                function.signature()
            ),
        )),
        None => Ok(()),
    }
}

pub(super) fn check_alter_function(
    stmt: &pb::AlterFunctionStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Some(kind) = super::routine_object(stmt.objtype()) else {
        return Err(not_supported("ALTER FUNCTION"));
    };
    let Some(object) = &stmt.func else {
        return Err(not_supported("ALTER FUNCTION"));
    };
    let function = find_existing_function(kind, object, catalog)?;
    check_owner(kind, object, catalog)?;
    if function.kind == FunctionKind::Aggregate {
        return Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!("\"{}\" is an aggregate function", written_name(object)),
        ));
    }
    let attributes = Attributes::collect(&stmt.actions, false)?;
    attributes.check_values(catalog)?;
    if attributes.rows.is_some() && !function.returns_set {
        return Err(error(
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "ROWS is not applicable when function does not return a set",
        ));
    }
    Err(not_supported(format!("ALTER {}", kind.keyword())))
}

pub(super) fn check_rename_function(
    kind: RoutineObject,
    stmt: &pb::RenameStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Some(Node::ObjectWithArgs(object)) = stmt.object.as_deref().and_then(|o| o.node.as_ref())
    else {
        return Err(not_supported(format!("ALTER {}", kind.keyword())));
    };
    let function = find_existing_function(kind, object, catalog)?;
    check_owner(kind, object, catalog)?;
    let new_name_exists = builtins::functions(&stmt.newname)
        .iter()
        .any(|other| other.parameter_types().eq(function.parameter_types()));
    if new_name_exists {
        let types: Vec<String> = function
            .parameter_types()
            .map(builtins::type_display_name)
            .collect();
        return Err(error(
            SQLSTATE_DUPLICATE_FUNCTION,
            format!(
                "function {}({}) already exists in schema \"pg_catalog\"",
                stmt.newname,
                types.join(", ")
            ),
        ));
    }
    Err(not_supported(format!("ALTER {}", kind.keyword())))
}

pub(super) fn check_alter_function_owner(
    kind: RoutineObject,
    object: &ObjectWithArgs,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    find_existing_function(kind, object, catalog)?;
    check_owner(kind, object, catalog)?;
    Err(not_supported(format!("ALTER {}", kind.keyword())))
}

pub(super) fn check_alter_schema(
    kind: RoutineObject,
    stmt: &pb::AlterObjectSchemaStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Some(Node::ObjectWithArgs(object)) = stmt.object.as_deref().and_then(|o| o.node.as_ref())
    else {
        return Err(not_supported(format!("ALTER {}", kind.keyword())));
    };
    if find_function(kind, object, stmt.missing_ok, catalog)?.is_none() {
        return Ok(());
    }
    check_schema(&stmt.newschema, catalog)?;
    check_owner(kind, object, catalog)?;
    Err(not_supported(format!("ALTER {}", kind.keyword())))
}

pub(super) fn check_alter_depends(
    kind: RoutineObject,
    stmt: &pb::AlterObjectDependsStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Some(Node::ObjectWithArgs(object)) = stmt.object.as_deref().and_then(|o| o.node.as_ref())
    else {
        return Err(not_supported(format!("ALTER {}", kind.keyword())));
    };
    find_existing_function(kind, object, catalog)?;
    check_owner(kind, object, catalog)?;
    let extension = stmt
        .extname
        .as_ref()
        .map(|name| name.sval.as_str())
        .unwrap_or_default();
    if !catalog.extension_exists(extension) {
        return Err(error(
            SQLSTATE_UNDEFINED_OBJECT,
            format!("extension \"{extension}\" does not exist"),
        ));
    }
    Err(not_supported(format!("ALTER {}", kind.keyword())))
}

/// COMMENT ON FUNCTION does nothing in tursopg, which does not keep
/// comments.
pub(super) fn check_comment_on_function(
    kind: RoutineObject,
    object: &ObjectWithArgs,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    find_existing_function(kind, object, catalog)?;
    check_owner(kind, object, catalog)
}

/// CALL fails in tursopg, which has no procedures. Like PostgreSQL's
/// transformCallStmt(), it checks the arguments first.
pub(super) fn check_call(
    stmt: &pb::CallStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Some(call) = &stmt.funccall else {
        return Err(not_supported("CALL"));
    };
    let arguments: Vec<&pb::Node> = call.args.iter().map(expr::named_argument_value).collect();
    expr::check_expressions(&arguments, ExpressionKind::CallArgument, catalog)?;
    let names = strings(&call.funcname);
    let (schema, name) = qualified_name(&names)?;
    if let Some(schema) = schema {
        check_schema(schema, catalog)?;
    }
    let types: Vec<String> = arguments
        .iter()
        .map(|argument| {
            expr::expression_type(argument)
                .unwrap_or("unknown")
                .to_string()
        })
        .collect();
    let argument_names: Vec<String> = call
        .args
        .iter()
        .filter_map(|argument| match &argument.node {
            Some(Node::NamedArgExpr(named)) => Some(named.name.clone()),
            _ => None,
        })
        .collect();
    let signature = call_signature(&names.join("."), &types, &argument_names);
    let is_function = matches!(schema, None | Some("pg_catalog"))
        && builtins::matching_functions(name, &types, &argument_names)
            .next()
            .is_some();
    if is_function {
        return Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!("{signature} is not a procedure"),
        ));
    }
    Err(error(
        SQLSTATE_UNDEFINED_FUNCTION,
        format!("procedure {signature} does not exist"),
    ))
}

/// A call as PostgreSQL's func_signature_string() writes it, such as
/// `p(integer, b => unknown)`.
fn call_signature(name: &str, types: &[String], argument_names: &[String]) -> String {
    let positional = types.len() - argument_names.len();
    let arguments: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(index, ty)| {
            let ty = builtins::type_display_name(ty);
            match index.checked_sub(positional) {
                Some(named) => format!("{} => {ty}", argument_names[named]),
                None => ty,
            }
        })
        .collect();
    format!("{name}({})", arguments.join(", "))
}

/// The function that `object` names, which must exist.
fn find_existing_function(
    kind: RoutineObject,
    object: &ObjectWithArgs,
    catalog: &dyn RoutineCatalog,
) -> Result<&'static Function, ParseError> {
    Ok(find_function(kind, object, false, catalog)?.expect("a missing function is an error"))
}

/// Finds the function that `object` names, as PostgreSQL's
/// LookupFuncWithArgs() does: by its name and the exact types of its input
/// parameters. None when the function does not exist and `missing_ok` is
/// true.
pub(super) fn find_function(
    kind: RoutineObject,
    object: &ObjectWithArgs,
    missing_ok: bool,
    catalog: &dyn RoutineCatalog,
) -> Result<Option<&'static Function>, ParseError> {
    let mut types = Vec::new();
    for argument in &object.objargs {
        let Some(Node::TypeName(type_name)) = &argument.node else {
            continue;
        };
        match find_type(type_name, catalog) {
            Ok(Some(ty)) => types.push(ty),
            Ok(None) | Err(_) if missing_ok => return Ok(None),
            Ok(None) => {
                return Err(error(
                    SQLSTATE_UNDEFINED_OBJECT,
                    format!("type \"{}\" does not exist", type_name_text(type_name)),
                ))
            }
            Err(error) => return Err(error),
        }
    }
    let names = strings(&object.objname);
    let (schema, name) = qualified_name(&names)?;
    if let Some(schema) = schema {
        match check_schema(schema, catalog) {
            Ok(()) => {}
            Err(_) if missing_ok => return Ok(None),
            Err(error) => return Err(error),
        }
    }
    let candidates = match schema {
        None | Some("pg_catalog") => builtins::functions(name),
        Some(_) => &[],
    };
    let mut found = None;
    for function in candidates {
        if object.args_unspecified {
            // Without an argument list, PostgreSQL looks only at the
            // objects of the kind that the statement names.
            if kind == RoutineObject::Procedure {
                continue;
            }
        } else if !function
            .parameter_types()
            .eq(types.iter().map(|ty| ty.builtin_name().unwrap_or("")))
        {
            continue;
        }
        if found.is_some() {
            return Err(ambiguous_function(kind, &names));
        }
        found = Some(function);
    }
    let Some(function) = found else {
        if missing_ok {
            return Ok(None);
        }
        return Err(missing_function(kind, object, &names, &types));
    };
    let signature = || {
        let types: Vec<String> = types.iter().map(TypeRef::display_name).collect();
        format!("{}({})", names.join("."), types.join(", "))
    };
    match kind {
        RoutineObject::Procedure => Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!("{} is not a procedure", signature()),
        )),
        RoutineObject::Aggregate if function.kind != FunctionKind::Aggregate => Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!("function {} is not an aggregate", signature()),
        )),
        _ => Ok(Some(function)),
    }
}

fn missing_function(
    kind: RoutineObject,
    object: &ObjectWithArgs,
    names: &[&str],
    types: &[TypeRef],
) -> ParseError {
    let name = names.join(".");
    let types: Vec<String> = types.iter().map(TypeRef::display_name).collect();
    let signature = format!("{name}({})", types.join(", "));
    let message = match kind {
        _ if object.args_unspecified => match kind {
            RoutineObject::Procedure => format!("could not find a procedure named \"{name}\""),
            RoutineObject::Aggregate => format!("could not find an aggregate named \"{name}\""),
            _ => format!("could not find a function named \"{name}\""),
        },
        RoutineObject::Procedure => format!("procedure {signature} does not exist"),
        RoutineObject::Aggregate if types.is_empty() => {
            format!("aggregate {name}(*) does not exist")
        }
        RoutineObject::Aggregate => format!("aggregate {signature} does not exist"),
        _ => format!("function {signature} does not exist"),
    };
    error(SQLSTATE_UNDEFINED_FUNCTION, message)
}

fn ambiguous_function(kind: RoutineObject, names: &[&str]) -> ParseError {
    error(
        SQLSTATE_AMBIGUOUS_FUNCTION,
        format!("{} name \"{}\" is not unique", kind.word(), names.join(".")),
    )
}

/// The built-in functions belong to the bootstrap superuser, so only a
/// superuser owns them.
fn check_owner(
    kind: RoutineObject,
    object: &ObjectWithArgs,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    if catalog.is_superuser() {
        return Ok(());
    }
    Err(permission_denied(format!(
        "must be owner of {} {}",
        kind.word(),
        written_name(object)
    )))
}

/// The name of a function as the statement writes it, such as
/// `pg_catalog.lower`.
fn written_name(object: &ObjectWithArgs) -> String {
    strings(&object.objname).join(".")
}
