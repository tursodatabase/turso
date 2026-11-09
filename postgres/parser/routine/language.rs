//! DO, and CREATE, ALTER, DROP and COMMENT ON LANGUAGE and TRANSFORM.
//!
//! The languages of tursopg are the languages of a new PostgreSQL database:
//! sql, plpgsql, c and internal. tursopg has no transforms.

use super::lookup::find_function;
use super::types::{find_type, type_name_text, TypeRef, UserKind};
use super::{error, permission_denied, strings, RoutineCatalog, RoutineObject};
use crate::builtins::{self, FunctionKind, Volatility};
use crate::sqlstate::{
    SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST, SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_SYNTAX_ERROR,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT, SQLSTATE_WRONG_OBJECT_TYPE,
};
use crate::translator::not_supported;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self as pb, ObjectWithArgs};

/// SQLSTATE 42710: an object with the name exists.
const SQLSTATE_DUPLICATE_OBJECT: &str = "42710";

const LANGUAGES: &[&str] = &["sql", "plpgsql", "c", "internal"];

/// Whether only a superuser can use the language.
fn is_untrusted(language: &str) -> bool {
    matches!(language, "c" | "internal")
}

/// Checks DO as PostgreSQL's ExecuteDoStmt() does, and compiles a PL/pgSQL
/// block. PostgreSQL then runs the block, which tursopg cannot do.
pub(super) fn check_do(
    sql: &str,
    stmt: &pb::DoStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let mut code = None;
    let mut language = None;
    for option in &stmt.args {
        let Some(Node::DefElem(option)) = &option.node else {
            continue;
        };
        let slot = match option.defname.as_str() {
            "as" => &mut code,
            "language" => &mut language,
            _ => continue,
        };
        if slot.is_some() {
            return Err(error(
                SQLSTATE_SYNTAX_ERROR,
                "conflicting or redundant options",
            ));
        }
        *slot = Some(option);
    }
    if code.is_none() {
        return Err(error(SQLSTATE_SYNTAX_ERROR, "no inline code specified"));
    }
    let language = language
        .and_then(|option| match option.arg.as_deref()?.node.as_ref()? {
            Node::String(name) => Some(name.sval.as_str()),
            _ => None,
        })
        .unwrap_or("plpgsql");
    check_language_exists(language)?;
    if is_untrusted(language) && !catalog.is_superuser() {
        return Err(permission_denied(format!(
            "permission denied for language {language}"
        )));
    }
    if language != "plpgsql" {
        return Err(error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("language \"{language}\" does not support inline code execution"),
        ));
    }
    super::function::check_plpgsql(sql)?;
    Err(not_supported("DO"))
}

/// Checks CREATE LANGUAGE ... HANDLER as PostgreSQL's
/// CreateProceduralLanguage() does.
pub(super) fn check_create_language(
    stmt: &pb::CreatePLangStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    if !catalog.is_superuser() {
        return Err(permission_denied(
            "must be superuser to create custom procedural language",
        ));
    }
    let handler = find_function_by_types(&stmt.plhandler, &[])?;
    if handler.result_type != "language_handler" {
        return Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!(
                "function {} must return type language_handler",
                strings(&stmt.plhandler).join(".")
            ),
        ));
    }
    if !stmt.plinline.is_empty() {
        find_function_by_types(&stmt.plinline, &["internal"])?;
    }
    if !stmt.plvalidator.is_empty() {
        find_function_by_types(&stmt.plvalidator, &["oid"])?;
    }
    if LANGUAGES.contains(&stmt.plname.as_str()) && !stmt.replace {
        return Err(error(
            SQLSTATE_DUPLICATE_OBJECT,
            format!("language \"{}\" already exists", stmt.plname),
        ));
    }
    Err(not_supported("CREATE LANGUAGE"))
}

/// The function with the name `names` and the parameter types `types`, as
/// PostgreSQL's LookupFuncName() finds it.
fn find_function_by_types(
    names: &[pb::Node],
    types: &[&str],
) -> Result<&'static builtins::Function, ParseError> {
    let names = strings(names);
    let function = match names.as_slice() {
        [name] | ["pg_catalog", name] => builtins::functions(name)
            .iter()
            .find(|function| function.parameter_types().eq(types.iter().copied())),
        _ => None,
    };
    function.ok_or_else(|| {
        let types: Vec<String> = types
            .iter()
            .map(|ty| builtins::type_display_name(ty))
            .collect();
        error(
            SQLSTATE_UNDEFINED_FUNCTION,
            format!(
                "function {}({}) does not exist",
                names.join("."),
                types.join(", ")
            ),
        )
    })
}

pub(super) fn check_drop_language(
    objects: &[pb::Node],
    missing_ok: bool,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let mut found = Vec::new();
    for object in objects {
        let Some(Node::String(name)) = &object.node else {
            continue;
        };
        let name = name.sval.as_str();
        if !LANGUAGES.contains(&name) {
            if missing_ok {
                continue;
            }
            return Err(missing_language(name));
        }
        check_language_owner(name, catalog)?;
        found.push(name);
    }
    match found.first() {
        Some(&"plpgsql") => Err(error(
            SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
            "cannot drop language plpgsql because extension plpgsql requires it",
        )),
        Some(name) => Err(error(
            SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
            format!("cannot drop language {name} because it is required by the database system"),
        )),
        None => Ok(()),
    }
}

/// COMMENT ON LANGUAGE does nothing in tursopg, which does not keep
/// comments.
pub(super) fn check_comment_on_language(
    object: &Node,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Node::String(name) = object else {
        return Err(not_supported("COMMENT ON LANGUAGE"));
    };
    check_language_exists(&name.sval)?;
    check_language_owner(&name.sval, catalog)
}

pub(super) fn check_rename_language(
    stmt: &pb::RenameStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Some(Node::String(name)) = stmt.object.as_deref().and_then(|o| o.node.as_ref()) else {
        return Err(not_supported("ALTER LANGUAGE"));
    };
    check_language_exists(&name.sval)?;
    check_language_owner(&name.sval, catalog)?;
    if LANGUAGES.contains(&stmt.newname.as_str()) {
        return Err(error(
            SQLSTATE_DUPLICATE_OBJECT,
            format!("language \"{}\" already exists", stmt.newname),
        ));
    }
    Err(not_supported("ALTER LANGUAGE"))
}

pub(super) fn check_alter_language_owner(
    object: &Node,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Node::String(name) = object else {
        return Err(not_supported("ALTER LANGUAGE"));
    };
    check_language_exists(&name.sval)?;
    check_language_owner(&name.sval, catalog)?;
    Err(not_supported("ALTER LANGUAGE"))
}

fn check_language_exists(name: &str) -> Result<(), ParseError> {
    if LANGUAGES.contains(&name) {
        Ok(())
    } else {
        Err(missing_language(name))
    }
}

/// The languages belong to the bootstrap superuser.
fn check_language_owner(name: &str, catalog: &dyn RoutineCatalog) -> Result<(), ParseError> {
    if catalog.is_superuser() {
        Ok(())
    } else {
        Err(permission_denied(format!(
            "must be owner of language {name}"
        )))
    }
}

fn missing_language(name: &str) -> ParseError {
    error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("language \"{name}\" does not exist"),
    )
}

/// Checks CREATE TRANSFORM in the order of PostgreSQL's CreateTransform().
pub(super) fn check_create_transform(
    stmt: &pb::CreateTransformStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let type_name = stmt
        .type_name
        .as_ref()
        .ok_or_else(|| not_supported("CREATE TRANSFORM"))?;
    let Some(ty) = find_type(type_name, catalog)? else {
        return Err(missing_type(type_name));
    };
    if ty.is_pseudo() {
        return Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!("data type {} is a pseudo-type", type_name_text(type_name)),
        ));
    }
    if matches!(
        ty,
        TypeRef::User {
            kind: UserKind::Domain,
            ..
        }
    ) {
        return Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!("data type {} is a domain", type_name_text(type_name)),
        ));
    }
    if !catalog.is_superuser() {
        return Err(permission_denied(format!(
            "must be owner of type {}",
            ty.display_name()
        )));
    }
    check_language_exists(&stmt.lang)?;
    if let Some(from_sql) = &stmt.fromsql {
        let function = transform_function(from_sql, catalog)?;
        if function.result_type != "internal" {
            return Err(invalid_definition(
                "return data type of FROM SQL function must be internal",
            ));
        }
        check_transform_function(function)?;
    }
    if let Some(to_sql) = &stmt.tosql {
        let function = transform_function(to_sql, catalog)?;
        if Some(function.result_type) != ty.builtin_name() {
            return Err(invalid_definition(
                "return data type of TO SQL function must be the transform data type",
            ));
        }
        check_transform_function(function)?;
    }
    Err(not_supported("CREATE TRANSFORM"))
}

fn transform_function(
    object: &ObjectWithArgs,
    catalog: &dyn RoutineCatalog,
) -> Result<&'static builtins::Function, ParseError> {
    Ok(
        find_function(RoutineObject::Function, object, false, catalog)?
            .expect("a missing function is an error"),
    )
}

/// The checks of PostgreSQL's check_transform_function().
fn check_transform_function(function: &builtins::Function) -> Result<(), ParseError> {
    if function.volatility == Volatility::Volatile {
        return Err(invalid_definition(
            "transform function must not be volatile",
        ));
    }
    if function.kind != FunctionKind::Function {
        return Err(invalid_definition(
            "transform function must be a normal function",
        ));
    }
    if function.returns_set {
        return Err(invalid_definition(
            "transform function must not return a set",
        ));
    }
    if function.parameters.len() != 1 {
        return Err(invalid_definition(
            "transform function must take one argument",
        ));
    }
    if function.parameters[0].type_name != "internal" {
        return Err(invalid_definition(
            "first argument of transform function must be type internal",
        ));
    }
    Ok(())
}

pub(super) fn check_drop_transform(
    objects: &[pb::Node],
    missing_ok: bool,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    for object in objects {
        if let Some(object) = &object.node {
            check_transform_reference(object, missing_ok, catalog)?;
        }
    }
    Ok(())
}

/// The error for a transform that a statement names, as in DROP TRANSFORM
/// FOR integer LANGUAGE sql. tursopg has no transforms. Ok when
/// `missing_ok`.
pub(super) fn check_transform_reference(
    object: &Node,
    missing_ok: bool,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Node::List(list) = object else {
        return Ok(());
    };
    let mut items = list.items.iter().filter_map(|item| item.node.as_ref());
    let (Some(Node::TypeName(type_name)), Some(Node::String(language))) =
        (items.next(), items.next())
    else {
        return Ok(());
    };
    let ty = match find_type(type_name, catalog) {
        Ok(Some(ty)) => ty,
        _ if missing_ok => return Ok(()),
        Ok(None) => return Err(missing_type(type_name)),
        Err(error) => return Err(error),
    };
    if !LANGUAGES.contains(&language.sval.as_str()) {
        if missing_ok {
            return Ok(());
        }
        return Err(missing_language(&language.sval));
    }
    if missing_ok {
        return Ok(());
    }
    Err(missing_transform(&ty, &language.sval))
}

pub(super) fn missing_transform(ty: &TypeRef, language: &str) -> ParseError {
    error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!(
            "transform for type {} language \"{language}\" does not exist",
            ty.display_name()
        ),
    )
}

fn missing_type(type_name: &pb::TypeName) -> ParseError {
    error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("type \"{}\" does not exist", type_name_text(type_name)),
    )
}

fn invalid_definition(message: &str) -> ParseError {
    error(super::function::SQLSTATE_INVALID_OBJECT_DEFINITION, message)
}
