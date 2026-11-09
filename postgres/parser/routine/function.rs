//! CREATE FUNCTION and CREATE PROCEDURE.
//!
//! The checks of PostgreSQL come from CreateFunction(), ProcedureCreate()
//! and the validator function of the language, in that order. A definition
//! that passes them gets the error for the first feature that tursopg does
//! not support. The last one is "CREATE FUNCTION is not supported": tursopg
//! does not support user-defined functions yet.

use super::body::{self, Signature};
use super::expr::{self, ExpressionKind};
use super::types::{find_type, type_name_text, TypeRef, UserKind};
use super::{
    creation_schema, error, not_supported_as_postgres, permission_denied, qualified_name, strings,
    RoutineCatalog,
};
use crate::builtins::{self, Coercion, FunctionKind};
use crate::sqlstate::{
    SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_FEATURE_NOT_SUPPORTED,
    SQLSTATE_INVALID_FUNCTION_DEFINITION, SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_SYNTAX_ERROR,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT, SQLSTATE_WRONG_OBJECT_TYPE,
};
use crate::translator::not_supported;
use crate::{quote_identifier, ParseError};
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{
    self as pb, CreateFunctionStmt, DefElem, FunctionParameter, FunctionParameterMode, Token,
    VariableSetKind,
};

/// SQLSTATE 42723: a function with the name and argument types exists.
const SQLSTATE_DUPLICATE_FUNCTION: &str = "42723";
/// SQLSTATE 42P17: an object definition that PostgreSQL rejects.
pub(super) const SQLSTATE_INVALID_OBJECT_DEFINITION: &str = "42P17";
/// SQLSTATE 54023: a function with more arguments than PostgreSQL permits.
const SQLSTATE_TOO_MANY_ARGUMENTS: &str = "54023";
/// SQLSTATE 58P01: a file that the server cannot find.
const SQLSTATE_UNDEFINED_FILE: &str = "58P01";
/// The maximum number of input parameters of a PostgreSQL function.
const FUNC_MAX_ARGS: usize = 100;

pub(super) fn check_create_function(
    sql: &str,
    stmt: &CreateFunctionStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let definition = check_definition(sql, stmt, catalog)?;
    Err(unsupported_feature(sql, stmt, &definition))
}

/// A language of PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Language {
    Sql,
    Plpgsql,
    C,
    Internal,
}

impl Language {
    fn find(name: &str) -> Option<Self> {
        Some(match name {
            "sql" => Self::Sql,
            "plpgsql" => Self::Plpgsql,
            "c" => Self::C,
            "internal" => Self::Internal,
            _ => return None,
        })
    }
}

/// The kind of object that a parameter list is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParameterOwner {
    Function,
    Procedure,
    Aggregate,
}

/// A parameter of a function definition with its type.
pub(super) struct Parameter<'a> {
    pub name: &'a str,
    pub mode: FunctionParameterMode,
    pub ty: TypeRef,
    pub type_name: &'a pb::TypeName,
    pub default: Option<&'a pb::Node>,
}

impl Parameter<'_> {
    pub(super) fn is_input(&self) -> bool {
        !matches!(
            self.mode,
            FunctionParameterMode::FuncParamOut | FunctionParameterMode::FuncParamTable
        )
    }

    pub(super) fn is_output(&self) -> bool {
        !matches!(
            self.mode,
            FunctionParameterMode::FuncParamIn | FunctionParameterMode::FuncParamVariadic
        )
    }
}

/// The result of a function.
pub(super) struct ResultType {
    /// `void` for a procedure without output parameters, and `record` for
    /// one with them.
    pub ty: TypeRef,
    pub returns_set: bool,
}

/// What the checks of PostgreSQL found out about a definition.
struct Definition<'a> {
    attributes: Attributes<'a>,
    language: Language,
    parameters: Vec<Parameter<'a>>,
    result: ResultType,
}

fn check_definition<'a>(
    sql: &str,
    stmt: &'a CreateFunctionStmt,
    catalog: &dyn RoutineCatalog,
) -> Result<Definition<'a>, ParseError> {
    let names = strings(&stmt.funcname);
    let (schema, name) = qualified_name(&names)?;
    let schema = creation_schema(schema, catalog)?;
    let attributes = Attributes::collect(&stmt.options, stmt.is_procedure)?;
    attributes.check_values(catalog)?;
    let language = find_language(stmt, &attributes)?;
    if let Some(transform) = attributes.transform {
        check_transforms(transform, &attributes, catalog)?;
    }
    let owner = if stmt.is_procedure {
        ParameterOwner::Procedure
    } else {
        ParameterOwner::Function
    };
    let (parameters, required_result) = check_parameters(&stmt.parameters, owner, catalog)?;
    let result = check_result_type(stmt, required_result, language, catalog)?;
    let signature = Signature {
        function_name: name,
        parameters: &parameters,
        result: &result,
        is_procedure: stmt.is_procedure,
    };
    check_as_clause(stmt, &attributes, language, &signature, catalog)?;
    if attributes.rows.is_some() && !result.returns_set {
        return Err(error(
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "ROWS is not applicable when function does not return a set",
        ));
    }
    check_polymorphic_signature(&parameters, &result)?;
    if schema == "pg_catalog" {
        check_builtin_with_same_signature(stmt, name, &attributes, &parameters, &result)?;
    }
    validate(sql, stmt, &attributes, language, &signature, catalog)?;
    Ok(Definition {
        attributes,
        language,
        parameters,
        result,
    })
}

/// The options of CREATE FUNCTION and ALTER FUNCTION, such as LANGUAGE and
/// STABLE.
#[derive(Default)]
pub(super) struct Attributes<'a> {
    /// The AS clause, which gives the body as a string.
    body: Option<&'a DefElem>,
    language: Option<&'a DefElem>,
    transform: Option<&'a DefElem>,
    volatility: Option<&'a DefElem>,
    cost: Option<&'a DefElem>,
    pub(super) rows: Option<&'a DefElem>,
    support: Option<&'a DefElem>,
    parallel: Option<&'a DefElem>,
    /// The options that tursopg does not support, in the order of the
    /// statement.
    unsupported: Vec<&'a DefElem>,
}

impl<'a> Attributes<'a> {
    /// Sorts the options as PostgreSQL's compute_function_attributes()
    /// does. An option that is given twice, except SET, and an option that
    /// a procedure cannot have are errors.
    pub(super) fn collect(options: &'a [pb::Node], is_procedure: bool) -> Result<Self, ParseError> {
        let mut attributes = Self::default();
        let mut given: Vec<&str> = Vec::new();
        for option in options {
            let Some(Node::DefElem(option)) = &option.node else {
                continue;
            };
            let name = option.defname.as_str();
            let only_for_functions = matches!(
                name,
                "volatility" | "strict" | "leakproof" | "cost" | "rows" | "support" | "parallel"
            );
            if is_procedure && only_for_functions {
                return Err(invalid_function(
                    "invalid attribute in procedure definition",
                ));
            }
            if name != "set" {
                if given.contains(&name) {
                    return Err(error(
                        SQLSTATE_SYNTAX_ERROR,
                        "conflicting or redundant options",
                    ));
                }
                given.push(name);
            }
            if is_procedure && name == "window" {
                return Err(invalid_function(
                    "invalid attribute in procedure definition",
                ));
            }
            match name {
                "as" => attributes.body = Some(option),
                "language" => attributes.language = Some(option),
                "volatility" => attributes.volatility = Some(option),
                _ => {
                    match name {
                        "transform" => attributes.transform = Some(option),
                        "cost" => attributes.cost = Some(option),
                        "rows" => attributes.rows = Some(option),
                        "support" => attributes.support = Some(option),
                        "parallel" => attributes.parallel = Some(option),
                        _ => {}
                    }
                    attributes.unsupported.push(option);
                }
            }
        }
        Ok(attributes)
    }

    /// Checks the values of COST, ROWS, SUPPORT and PARALLEL, in the order
    /// of PostgreSQL.
    pub(super) fn check_values(&self, catalog: &dyn RoutineCatalog) -> Result<(), ParseError> {
        if self.cost.is_some_and(|cost| numeric_arg(cost) <= 0.0) {
            return Err(error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "COST must be positive",
            ));
        }
        if self.rows.is_some_and(|rows| numeric_arg(rows) <= 0.0) {
            return Err(error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "ROWS must be positive",
            ));
        }
        if let Some(support) = self.support {
            check_support_function(support, catalog)?;
        }
        if let Some(parallel) = self.parallel {
            if !matches!(string_arg(parallel), Some("safe" | "restricted" | "unsafe")) {
                return Err(error(
                    SQLSTATE_SYNTAX_ERROR,
                    "parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE",
                ));
            }
        }
        Ok(())
    }
}

/// Checks SUPPORT as PostgreSQL's interpret_func_support() does: a support
/// function takes one argument of type internal and returns internal.
fn check_support_function(
    support: &DefElem,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let names = match node_arg(support) {
        Some(Node::List(list)) => strings(&list.items),
        Some(Node::TypeName(type_name)) => strings(&type_name.names),
        Some(Node::String(name)) => vec![name.sval.as_str()],
        _ => Vec::new(),
    };
    let name = names.join(".");
    let function = match names.as_slice() {
        [name] | ["pg_catalog", name] => builtins::functions(name)
            .iter()
            .find(|function| function.parameter_types().eq(["internal"])),
        _ => None,
    };
    let Some(function) = function else {
        return Err(error(
            SQLSTATE_UNDEFINED_FUNCTION,
            format!("function {name}(internal) does not exist"),
        ));
    };
    if function.result_type != "internal" {
        return Err(error(
            SQLSTATE_INVALID_OBJECT_DEFINITION,
            format!("support function {name} must return type internal"),
        ));
    }
    if !catalog.is_superuser() {
        return Err(permission_denied(
            "must be superuser to specify a support function",
        ));
    }
    Ok(())
}

/// The language of the function. The language of a RETURN or BEGIN ATOMIC
/// body is SQL. A body in a string needs LANGUAGE.
fn find_language(
    stmt: &CreateFunctionStmt,
    attributes: &Attributes,
) -> Result<Language, ParseError> {
    let name = match attributes.language.and_then(string_arg) {
        Some(name) => name,
        None if stmt.sql_body.is_some() => "sql",
        None => return Err(invalid_function("no language specified")),
    };
    Language::find(name).ok_or_else(|| {
        error(
            SQLSTATE_UNDEFINED_OBJECT,
            format!("language \"{name}\" does not exist"),
        )
    })
}

/// TRANSFORM FOR TYPE names transforms, and tursopg has none.
fn check_transforms(
    transform: &DefElem,
    attributes: &Attributes,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let Some(Node::List(types)) = node_arg(transform) else {
        return Ok(());
    };
    let language = attributes.language.and_then(string_arg).unwrap_or("sql");
    for type_name in &types.items {
        let Some(Node::TypeName(type_name)) = &type_name.node else {
            continue;
        };
        let Some(ty) = find_type(type_name, catalog)? else {
            return Err(error(
                SQLSTATE_UNDEFINED_OBJECT,
                format!("type \"{}\" does not exist", type_name_text(type_name)),
            ));
        };
        return Err(super::language::missing_transform(
            &element_type(&ty),
            language,
        ));
    }
    Ok(())
}

/// The element type of an array type, or the type.
fn element_type(ty: &TypeRef) -> TypeRef {
    match ty {
        TypeRef::Builtin(builtin) if builtin.is_array() => builtin
            .name
            .strip_prefix('_')
            .and_then(builtins::find_type)
            .map(TypeRef::Builtin)
            .unwrap_or_else(|| ty.clone()),
        _ => ty.clone(),
    }
}

/// Checks the parameters as PostgreSQL's interpret_function_parameter_list()
/// does. Gives the parameters with their types, and the result type that
/// the output parameters require.
pub(super) fn check_parameters<'a>(
    nodes: &'a [pb::Node],
    owner: ParameterOwner,
    catalog: &dyn RoutineCatalog,
) -> Result<(Vec<Parameter<'a>>, Option<TypeRef>), ParseError> {
    let is_procedure = owner == ParameterOwner::Procedure;
    let what = match owner {
        ParameterOwner::Function => "functions",
        ParameterOwner::Procedure => "procedures",
        ParameterOwner::Aggregate => "aggregates",
    };
    let mut parameters: Vec<Parameter> = Vec::new();
    let mut required_result = None;
    let mut outputs = 0;
    let mut variadic = false;
    let mut have_defaults = false;
    for parameter in nodes {
        let Some(Node::FunctionParameter(parameter)) = &parameter.node else {
            continue;
        };
        let parameter = check_parameter(parameter, what, catalog)?;
        if parameter.is_input() && variadic {
            return Err(invalid_function(
                "VARIADIC parameter must be the last input parameter",
            ));
        }
        if parameter.is_output() {
            if is_procedure {
                if variadic {
                    return Err(invalid_function(
                        "VARIADIC parameter must be the last parameter",
                    ));
                }
                required_result = builtins::find_type("record").map(TypeRef::Builtin);
            } else if outputs == 0 {
                required_result = Some(parameter.ty.clone());
            }
            outputs += 1;
        }
        if parameter.mode == FunctionParameterMode::FuncParamVariadic {
            variadic = true;
            let ty = &parameter.ty;
            if !(ty.is_array() || ty.is("any") || ty.is("anyarray") || ty.is("anycompatiblearray"))
            {
                return Err(invalid_function("VARIADIC parameter must be an array"));
            }
        }
        if !parameter.name.is_empty() {
            let is_input_only = matches!(
                parameter.mode,
                FunctionParameterMode::FuncParamIn | FunctionParameterMode::FuncParamVariadic
            );
            let is_output_only = matches!(
                parameter.mode,
                FunctionParameterMode::FuncParamOut | FunctionParameterMode::FuncParamTable
            );
            let conflicts = parameters.iter().any(|previous| {
                let previous_input_only = matches!(
                    previous.mode,
                    FunctionParameterMode::FuncParamIn | FunctionParameterMode::FuncParamVariadic
                );
                let previous_output_only = matches!(
                    previous.mode,
                    FunctionParameterMode::FuncParamOut | FunctionParameterMode::FuncParamTable
                );
                // An input-only parameter can have the name of an
                // output-only one.
                let separate = (is_input_only && previous_output_only)
                    || (previous_input_only && is_output_only);
                !separate && previous.name == parameter.name
            });
            if conflicts {
                return Err(invalid_function(format!(
                    "parameter name \"{}\" used more than once",
                    parameter.name
                )));
            }
        }
        match parameter.default {
            Some(default) => {
                if !parameter.is_input() {
                    return Err(invalid_function(
                        "only input parameters can have default values",
                    ));
                }
                check_default(default, &parameter.ty, catalog)?;
                have_defaults = true;
            }
            None => {
                if parameter.is_input() && have_defaults {
                    return Err(invalid_function(
                        "input parameters after one with a default value must also have defaults",
                    ));
                }
                if is_procedure && have_defaults {
                    return Err(invalid_function(
                        "procedure OUT parameters cannot appear after one with a default value",
                    ));
                }
            }
        }
        parameters.push(parameter);
    }
    if outputs > 1 {
        required_result = builtins::find_type("record").map(TypeRef::Builtin);
    }
    Ok((parameters, required_result))
}

fn check_parameter<'a>(
    parameter: &'a FunctionParameter,
    what: &str,
    catalog: &dyn RoutineCatalog,
) -> Result<Parameter<'a>, ParseError> {
    let type_name = parameter
        .arg_type
        .as_ref()
        .ok_or_else(|| invalid_function("a parameter without a type"))?;
    let Some(ty) = find_type(type_name, catalog)? else {
        return Err(error(
            SQLSTATE_UNDEFINED_OBJECT,
            format!("type {} does not exist", type_name_text(type_name)),
        ));
    };
    if type_name.setof {
        return Err(invalid_function(format!(
            "{what} cannot accept set arguments"
        )));
    }
    let mode = match parameter.mode() {
        FunctionParameterMode::FuncParamDefault | FunctionParameterMode::Undefined => {
            FunctionParameterMode::FuncParamIn
        }
        mode => mode,
    };
    Ok(Parameter {
        name: &parameter.name,
        mode,
        ty,
        type_name,
        default: parameter.defexpr.as_deref(),
    })
}

/// Checks the DEFAULT of a parameter as PostgreSQL does: the expression,
/// then its conversion to the type of the parameter.
fn check_default(
    default: &pb::Node,
    ty: &TypeRef,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    expr::check_expressions(&[default], ExpressionKind::Default, catalog)?;
    let Some(target) = ty.builtin_name() else {
        return Ok(());
    };
    let Some(source) = expr::expression_type(default) else {
        return Ok(());
    };
    if source == "unknown" {
        return body::check_literal(default, target);
    }
    if !builtins::can_coerce(source, target, Coercion::Assignment) {
        return Err(error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!(
                "argument of DEFAULT must be type {}, not type {}",
                ty.display_name(),
                builtins::type_display_name(source)
            ),
        ));
    }
    Ok(())
}

/// The result type, as PostgreSQL's compute_return_type() finds it, and
/// the checks against the output parameters.
fn check_result_type(
    stmt: &CreateFunctionStmt,
    required: Option<TypeRef>,
    language: Language,
    catalog: &dyn RoutineCatalog,
) -> Result<ResultType, ParseError> {
    if stmt.is_procedure {
        let name = if required.is_some() { "record" } else { "void" };
        return Ok(ResultType {
            ty: TypeRef::Builtin(builtins::find_type(name).expect("record and void are built in")),
            returns_set: false,
        });
    }
    let Some(return_type) = &stmt.return_type else {
        return match required {
            Some(ty) => Ok(ResultType {
                ty,
                returns_set: false,
            }),
            None => Err(invalid_function("function result type must be specified")),
        };
    };
    let ty = match find_type(return_type, catalog)? {
        Some(ty) => ty,
        // PostgreSQL makes a shell type for the result of a C function.
        None if matches!(language, Language::C | Language::Internal) => TypeRef::User {
            name: type_name_text(return_type),
            kind: UserKind::Other,
        },
        None => {
            return Err(error(
                SQLSTATE_UNDEFINED_OBJECT,
                format!("type \"{}\" does not exist", type_name_text(return_type)),
            ))
        }
    };
    if let Some(required) = required {
        if !ty.same_as(&required) {
            return Err(invalid_function(format!(
                "function result type must be {} because of OUT parameters",
                required.display_name()
            )));
        }
    }
    Ok(ResultType {
        ty,
        returns_set: return_type.setof,
    })
}

/// Checks the body as PostgreSQL's interpret_AS_clause() does. A RETURN or
/// BEGIN ATOMIC body is analyzed here, whatever check_function_bodies is.
fn check_as_clause(
    stmt: &CreateFunctionStmt,
    attributes: &Attributes,
    language: Language,
    signature: &Signature,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    match (&stmt.sql_body, attributes.body) {
        (None, None) => return Err(invalid_function("no function body specified")),
        (Some(_), Some(_)) => return Err(invalid_function("duplicate function body specified")),
        (Some(_), None) if language != Language::Sql => {
            return Err(invalid_function(
                "inline SQL function body only valid for language SQL",
            ))
        }
        _ => {}
    }
    if let Some(sql_body) = stmt.sql_body.as_deref() {
        if signature
            .parameters
            .iter()
            .any(|parameter| parameter.is_input() && parameter.ty.is_polymorphic())
        {
            return Err(invalid_function(
                "SQL function with unquoted function body cannot have polymorphic arguments",
            ));
        }
        return body::analyze_sql_standard_body(sql_body, signature, catalog);
    }
    if language != Language::C && as_items(attributes).len() != 1 {
        let name = attributes.language.and_then(string_arg).unwrap_or("sql");
        return Err(invalid_function(format!(
            "only one AS item needed for language \"{name}\""
        )));
    }
    Ok(())
}

/// The strings of the AS clause.
fn as_items<'a>(attributes: &Attributes<'a>) -> Vec<&'a str> {
    match attributes.body.and_then(node_arg) {
        Some(Node::List(list)) => strings(&list.items),
        Some(Node::String(text)) => vec![text.sval.as_str()],
        _ => Vec::new(),
    }
}

/// The checks of PostgreSQL's ProcedureCreate() on the types: the number
/// of arguments, and a polymorphic or internal result that the arguments
/// cannot determine.
fn check_polymorphic_signature(
    parameters: &[Parameter],
    result: &ResultType,
) -> Result<(), ParseError> {
    let inputs: Vec<&TypeRef> = parameters
        .iter()
        .filter(|parameter| parameter.is_input())
        .map(|parameter| &parameter.ty)
        .collect();
    if inputs.len() > FUNC_MAX_ARGS {
        return Err(error(
            SQLSTATE_TOO_MANY_ARGUMENTS,
            format!("functions cannot have more than {FUNC_MAX_ARGS} arguments"),
        ));
    }
    let outputs = parameters
        .iter()
        .filter(|parameter| {
            !matches!(
                parameter.mode,
                FunctionParameterMode::FuncParamIn | FunctionParameterMode::FuncParamVariadic
            )
        })
        .map(|parameter| &parameter.ty);
    for result in std::iter::once(&result.ty).chain(outputs) {
        if polymorphic_result_is_unknown(result, &inputs) {
            return Err(invalid_function("cannot determine result data type"));
        }
        if result.is("internal") && !inputs.iter().any(|input| input.is("internal")) {
            return Err(invalid_function("unsafe use of pseudo-type \"internal\""));
        }
    }
    Ok(())
}

/// Whether no input type determines a polymorphic result type, as
/// PostgreSQL's check_valid_polymorphic_signature() decides it.
fn polymorphic_result_is_unknown(result: &TypeRef, inputs: &[&TypeRef]) -> bool {
    const FAMILY_1: &[&str] = &[
        "anyelement",
        "anyarray",
        "anynonarray",
        "anyenum",
        "anyrange",
        "anymultirange",
    ];
    const FAMILY_2: &[&str] = &[
        "anycompatible",
        "anycompatiblearray",
        "anycompatiblenonarray",
        "anycompatiblerange",
        "anycompatiblemultirange",
    ];
    let Some(name) = result.builtin_name() else {
        return false;
    };
    let has_input = |names: &[&str]| {
        inputs.iter().any(|input| {
            input
                .builtin_name()
                .is_some_and(|input| names.contains(&input))
        })
    };
    match name {
        "anyrange" | "anymultirange" => !has_input(&["anyrange", "anymultirange"]),
        "anycompatiblerange" | "anycompatiblemultirange" => {
            !has_input(&["anycompatiblerange", "anycompatiblemultirange"])
        }
        name if FAMILY_1.contains(&name) => !has_input(FAMILY_1),
        name if FAMILY_2.contains(&name) => !has_input(FAMILY_2),
        _ => false,
    }
}

/// A function of pg_catalog with the same name and input types: PostgreSQL
/// replaces it only with OR REPLACE, and only when the kind and the result
/// type stay the same.
fn check_builtin_with_same_signature(
    stmt: &CreateFunctionStmt,
    name: &str,
    attributes: &Attributes,
    parameters: &[Parameter],
    result: &ResultType,
) -> Result<(), ParseError> {
    let inputs: Vec<&str> = parameters
        .iter()
        .filter(|parameter| parameter.is_input())
        .map(|parameter| parameter.ty.builtin_name().unwrap_or(""))
        .collect();
    let Some(existing) = builtins::functions(name)
        .iter()
        .find(|function| function.parameter_types().eq(inputs.iter().copied()))
    else {
        return Ok(());
    };
    if !stmt.replace {
        return Err(error(
            SQLSTATE_DUPLICATE_FUNCTION,
            format!("function \"{name}\" already exists with same argument types"),
        ));
    }
    let is_window = attributes
        .unsupported
        .iter()
        .any(|option| option.defname == "window" && bool_arg(option));
    let kind_changes = stmt.is_procedure
        || match existing.kind {
            FunctionKind::Function => is_window,
            FunctionKind::Window => !is_window,
            FunctionKind::Aggregate => true,
        };
    if kind_changes {
        return Err(error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            "cannot change routine kind",
        ));
    }
    if result.ty.builtin_name() != Some(existing.result_type)
        || result.returns_set != existing.returns_set
    {
        return Err(invalid_function(
            "cannot change return type of existing function",
        ));
    }
    Ok(())
}

/// The checks of the validator function of the language.
fn validate(
    sql: &str,
    stmt: &CreateFunctionStmt,
    attributes: &Attributes,
    language: Language,
    signature: &Signature,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let inputs = signature
        .parameters
        .iter()
        .filter(|parameter| parameter.is_input());
    let result = &signature.result.ty;
    match language {
        Language::Sql => {
            if result.is_pseudo()
                && !result.is("record")
                && !result.is("void")
                && !result.is_polymorphic()
            {
                return Err(invalid_function(format!(
                    "SQL functions cannot return type {}",
                    result.display_name()
                )));
            }
            for parameter in inputs {
                if parameter.ty.is_pseudo() && !parameter.ty.is_polymorphic() {
                    return Err(invalid_function(format!(
                        "SQL functions cannot have arguments of type {}",
                        parameter.ty.display_name()
                    )));
                }
            }
            if !catalog.check_function_bodies() {
                return Ok(());
            }
            match stmt.sql_body.as_deref() {
                Some(sql_body) => {
                    body::check_sql_standard_body_result(sql_body, signature, catalog)
                }
                None => body::check_sql_body(as_items(attributes)[0], signature, catalog),
            }
        }
        Language::Plpgsql => {
            if result.is_pseudo()
                && !["trigger", "event_trigger", "record", "void"]
                    .iter()
                    .any(|name| result.is(name))
                && !result.is_polymorphic()
            {
                return Err(not_supported_as_postgres(format!(
                    "PL/pgSQL functions cannot return type {}",
                    result.display_name()
                )));
            }
            for parameter in signature.parameters {
                let ty = &parameter.ty;
                if ty.is_pseudo() && !ty.is("record") && !ty.is_polymorphic() {
                    return Err(not_supported_as_postgres(format!(
                        "PL/pgSQL functions cannot accept type {}",
                        ty.display_name()
                    )));
                }
            }
            if !catalog.check_function_bodies() {
                return Ok(());
            }
            let has_inputs = signature.parameters.iter().any(Parameter::is_input);
            if has_inputs && result.is("trigger") {
                return Err(invalid_function(
                    "trigger functions cannot have declared arguments",
                ));
            }
            if has_inputs && result.is("event_trigger") {
                return Err(invalid_function(
                    "event trigger functions cannot have declared arguments",
                ));
            }
            check_plpgsql(sql)
        }
        Language::Internal => {
            let name = match as_items(attributes).first() {
                Some(name) if !name.is_empty() => name.to_string(),
                _ => signature.function_name.to_string(),
            };
            if builtins::is_internal_c_function(&name) {
                Ok(())
            } else {
                Err(error(
                    SQLSTATE_UNDEFINED_FUNCTION,
                    format!("there is no built-in function named \"{name}\""),
                ))
            }
        }
        // tursopg has none of the libraries that a C function can be in.
        Language::C => {
            let file = as_items(attributes).first().copied().unwrap_or_default();
            Err(error(
                SQLSTATE_UNDEFINED_FILE,
                format!("could not access file \"{file}\": No such file or directory"),
            ))
        }
    }
}

/// Parses a PL/pgSQL function or DO block as PostgreSQL's plpgsql_compile()
/// does. `sql` is the whole statement.
pub(super) fn check_plpgsql(sql: &str) -> Result<(), ParseError> {
    match pg_query::parse_plpgsql(sql) {
        Ok(_) => Ok(()),
        Err(pg_query::Error::Parse(message)) => {
            Err(error(plpgsql_error_sqlstate(&message), message))
        }
        Err(other) => Err(error(SQLSTATE_SYNTAX_ERROR, other.to_string())),
    }
}

/// The SQLSTATE of an error of the PL/pgSQL parser. Most of them are syntax
/// errors.
fn plpgsql_error_sqlstate(message: &str) -> &'static str {
    let is_variable_error =
        |suffix: &str| message.starts_with("variable \"") && message.ends_with(suffix);
    if is_variable_error("\" is declared CONSTANT") {
        "22005"
    } else if is_variable_error("\" does not exist") {
        SQLSTATE_UNDEFINED_OBJECT
    } else if is_variable_error("since it's declared NOT NULL") {
        "22004"
    } else if is_variable_error("\" must be of type cursor or refcursor")
        || message.starts_with("collations are not supported by type ")
        || message.starts_with("loop variable of loop over rows must be")
        || message == "cursor variable must be a simple variable"
        || message == "cannot use RETURN NEXT in a non-SETOF function"
        || message == "RETURN NEXT cannot have a parameter in function with OUT parameters"
        || message == "cannot use RETURN QUERY in a non-SETOF function"
    {
        SQLSTATE_DATATYPE_MISMATCH
    } else if message == "FETCH statement cannot return multiple rows" {
        SQLSTATE_FEATURE_NOT_SUPPORTED
    } else if message == "too many INTO variables specified" {
        "54000"
    } else {
        SQLSTATE_SYNTAX_ERROR
    }
}

/// The error for the first feature of a valid definition that tursopg does
/// not support.
fn unsupported_feature(
    sql: &str,
    stmt: &CreateFunctionStmt,
    definition: &Definition,
) -> ParseError {
    if stmt.is_procedure {
        return not_supported("CREATE PROCEDURE");
    }
    if definition.language != Language::Sql {
        let name = definition
            .attributes
            .language
            .and_then(string_arg)
            .unwrap_or_default();
        return not_supported(format!("LANGUAGE {}", quote_identifier(name)));
    }
    if let Some(option) = definition.attributes.unsupported.first() {
        return option_error(option);
    }
    match definition.attributes.volatility.and_then(string_arg) {
        Some("stable") => {}
        Some("immutable") => return not_supported("an IMMUTABLE function"),
        _ => return not_supported("a VOLATILE function (a function without STABLE or IMMUTABLE)"),
    }
    if let Some(error) = unsupported_signature(stmt, &definition.parameters, &definition.result) {
        return error;
    }
    unsupported_body(sql, stmt, &definition.attributes)
}

/// The error for an option of CREATE FUNCTION that tursopg does not
/// support.
fn option_error(option: &DefElem) -> ParseError {
    let clause = match option.defname.as_str() {
        "window" => "WINDOW",
        "transform" => "TRANSFORM",
        "leakproof" if bool_arg(option) => "LEAKPROOF",
        "leakproof" => "NOT LEAKPROOF",
        "security" if bool_arg(option) => "SECURITY DEFINER",
        "security" => "SECURITY INVOKER",
        "strict" if bool_arg(option) => "STRICT / RETURNS NULL ON NULL INPUT",
        "strict" => "CALLED ON NULL INPUT",
        "parallel" => "PARALLEL",
        "cost" => "COST",
        "rows" => "ROWS",
        "support" => "SUPPORT",
        "set" => match node_arg(option) {
            Some(Node::VariableSetStmt(set)) => match set.kind() {
                VariableSetKind::VarSetCurrent => "SET ... FROM CURRENT",
                VariableSetKind::VarReset | VariableSetKind::VarResetAll => "RESET",
                _ => "SET",
            },
            _ => "SET",
        },
        other => return not_supported(format!("CREATE FUNCTION ... {}", other.to_uppercase())),
    };
    not_supported(format!("CREATE FUNCTION ... {clause}"))
}

/// The error for the first feature of the parameters and the result that
/// tursopg does not support. Only a result of one value and parameters
/// without a mode, name or default are supported, and each type must be a
/// base type.
fn unsupported_signature(
    stmt: &CreateFunctionStmt,
    parameters: &[Parameter],
    result: &ResultType,
) -> Option<ParseError> {
    if parameters
        .iter()
        .any(|parameter| parameter.mode == FunctionParameterMode::FuncParamTable)
    {
        return Some(not_supported("RETURNS TABLE"));
    }
    if stmt.return_type.as_ref().is_some_and(|ty| ty.setof) {
        return Some(not_supported("RETURNS SETOF"));
    }
    for parameter in parameters {
        let mode = match parameter.mode {
            FunctionParameterMode::FuncParamOut => Some("an OUT parameter"),
            FunctionParameterMode::FuncParamInout => Some("an INOUT parameter"),
            FunctionParameterMode::FuncParamVariadic => Some("a VARIADIC parameter"),
            _ => None,
        };
        if let Some(mode) = mode {
            return Some(not_supported(mode));
        }
        if let Some(error) =
            unsupported_type(parameter.type_name, &parameter.ty, "a function parameter")
        {
            return Some(error);
        }
        if !parameter.name.is_empty() {
            return Some(not_supported("a named function parameter"));
        }
        if parameter.default.is_some() {
            return Some(not_supported("DEFAULT on a function parameter"));
        }
    }
    let return_type = stmt.return_type.as_ref()?;
    if result.ty.is("record") {
        return Some(not_supported("RETURNS record"));
    }
    if result.ty.is("void") {
        return Some(not_supported("RETURNS void"));
    }
    unsupported_type(return_type, &result.ty, "a function result")
}

/// The error for a type in a signature that is not a base type that
/// PostgreSQL has built in, such as integer or text. `what` names the
/// value of the type.
fn unsupported_type(type_name: &pb::TypeName, ty: &TypeRef, what: &str) -> Option<ParseError> {
    if type_name.pct_type {
        return Some(not_supported(format!("{what} type written with %TYPE")));
    }
    let written = type_name_text(type_name);
    Some(match ty {
        _ if ty.is_array() => not_supported(format!("{what} of an array type")),
        TypeRef::Builtin(builtin) => match builtin.kind {
            'b' => return None,
            'p' if builtin.is_polymorphic() => {
                not_supported(format!("{what} of polymorphic type {}", builtin.name))
            }
            'p' => not_supported(format!("{what} of pseudo-type {}", builtin.name)),
            'r' | 'm' => not_supported(format!("{what} of range type {}", builtin.name)),
            _ => not_supported(format!("{what} of composite type {written}")),
        },
        TypeRef::User { kind, .. } => match kind {
            UserKind::Row => not_supported(format!("{what} of composite type {written}")),
            _ => not_supported(format!("{what} of user-defined type {written}")),
        },
    })
}

/// The error for the first feature of the body that tursopg does not
/// support. Only a dollar-quoted string with one SELECT statement is
/// supported.
fn unsupported_body(sql: &str, stmt: &CreateFunctionStmt, attributes: &Attributes) -> ParseError {
    if let Some(sql_body) = stmt.sql_body.as_deref() {
        return match &sql_body.node {
            Some(Node::ReturnStmt(_)) => not_supported("CREATE FUNCTION ... RETURN"),
            _ => not_supported("CREATE FUNCTION ... BEGIN ATOMIC"),
        };
    }
    let Some(body) = attributes.body else {
        return not_supported("CREATE FUNCTION");
    };
    if !string_after_is_dollar_quoted(sql, body.location) {
        return not_supported("a function body that is not dollar-quoted");
    }
    // With check_function_bodies off, the body can be any string.
    let Ok(parsed) = pg_query::parse(as_items(attributes)[0]) else {
        return not_supported("CREATE FUNCTION");
    };
    let statements: Vec<&Node> = parsed
        .protobuf
        .stmts
        .iter()
        .filter_map(|stmt| stmt.stmt.as_ref()?.node.as_ref())
        .collect();
    for statement in &statements {
        if body::is_ddl(statement) {
            return not_supported("a DDL statement in a function body");
        }
        if let Some(command) = body::data_changing_command(statement) {
            return not_supported_as_postgres(format!(
                "{command} is not allowed in a non-volatile function"
            ));
        }
    }
    match statements.as_slice() {
        [Node::SelectStmt(_)] => not_supported("CREATE FUNCTION"),
        [_, _, ..] => not_supported("a function body with more than one statement"),
        _ => not_supported("a function body that is not a SELECT statement"),
    }
}

/// Whether the string constant that follows the keyword at byte offset
/// `location` of `sql` is dollar-quoted, as `$$...$$` is.
fn string_after_is_dollar_quoted(sql: &str, location: i32) -> bool {
    let Some(rest) = usize::try_from(location)
        .ok()
        .and_then(|location| sql.get(location..))
    else {
        return false;
    };
    let Ok(scanned) = pg_query::scan(rest) else {
        return false;
    };
    let is_comment =
        |token: i32| token == Token::SqlComment as i32 || token == Token::CComment as i32;
    scanned
        .tokens
        .iter()
        .skip(1)
        .find(|token| !is_comment(token.token))
        .and_then(|token| usize::try_from(token.start).ok())
        .is_some_and(|start| rest[start..].starts_with('$'))
}

fn node_arg(option: &DefElem) -> Option<&Node> {
    option.arg.as_deref()?.node.as_ref()
}

fn string_arg(option: &DefElem) -> Option<&str> {
    match node_arg(option)? {
        Node::String(value) => Some(value.sval.as_str()),
        _ => None,
    }
}

fn bool_arg(option: &DefElem) -> bool {
    matches!(node_arg(option), Some(Node::Boolean(value)) if value.boolval)
}

/// The value of COST or ROWS.
fn numeric_arg(option: &DefElem) -> f64 {
    match node_arg(option) {
        Some(Node::Integer(value)) => f64::from(value.ival),
        Some(Node::Float(value)) => value.fval.parse().unwrap_or(0.0),
        Some(Node::String(value)) => value.sval.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// An error that PostgreSQL gives for an invalid function definition.
pub(super) fn invalid_function(message: impl Into<String>) -> ParseError {
    error(SQLSTATE_INVALID_FUNCTION_DEFINITION, message)
}
