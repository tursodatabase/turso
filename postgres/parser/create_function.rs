//! CREATE FUNCTION and CREATE PROCEDURE, which tursopg does not support yet.
//!
//! Every statement is rejected. A statement that PostgreSQL also rejects gets
//! PostgreSQL's error. Otherwise the error names the first feature that
//! tursopg does not support, so that each feature can be enabled on its own.

use crate::cmdtag::CommandTag;
use crate::names::{extract_key_columns, is_default_schema, object_name};
use crate::sqlstate::{
    SQLSTATE_INVALID_FUNCTION_DEFINITION, SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_SYNTAX_ERROR,
    SQLSTATE_UNDEFINED_OBJECT,
};
use crate::{FunctionTypeUse, ParseError};
use pg_query::protobuf::{
    node::Node, CreateFunctionStmt, DefElem, FunctionParameter, FunctionParameterMode, TypeName,
    VariableSetKind,
};
use pg_query::NodeEnum;
use std::collections::HashMap;

/// The error for `stmt`, a CREATE FUNCTION or CREATE PROCEDURE statement of
/// `sql`.
pub(crate) fn reject_create_function(sql: &str, stmt: &CreateFunctionStmt) -> ParseError {
    match check_create_function(sql, stmt) {
        Err(error) => error,
        Ok(()) => ParseError::not_supported("CREATE FUNCTION"),
    }
}

/// Raises the errors of PostgreSQL in the order PostgreSQL raises them, then
/// the error for the first feature that tursopg does not support.
fn check_create_function(sql: &str, stmt: &CreateFunctionStmt) -> Result<(), ParseError> {
    if stmt.is_procedure {
        return Err(ParseError::not_supported("CREATE PROCEDURE"));
    }
    object_name(&stmt.funcname, is_default_schema, "function")?;
    let options = function_options(&stmt.options)?;
    let language = function_language(&options, stmt)?;
    let is_sql = language == "sql";
    let parameters = function_parameters(stmt)?;
    let parameter_types = parameters
        .iter()
        .map(|parameter| check_parameter_type(parameter, is_sql))
        .collect::<Result<Vec<_>, _>>()?;
    let result_type = check_result_type(stmt, &parameters, &parameter_types, is_sql)?;
    let returns_set = stmt.return_type.as_ref().is_some_and(|t| t.setof);
    if options.contains_key("rows") && !returns_set {
        return Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_INVALID_PARAMETER_VALUE,
            message: "ROWS is not applicable when function does not return a set".to_string(),
        });
    }
    let body = function_body(&options, stmt, is_sql)?;
    let body_statements = match body {
        Some(body) if is_sql => Some(parse_body(body)?),
        _ => None,
    };

    // PostgreSQL accepts the statement from here on.
    if !is_sql {
        return Err(ParseError::not_supported(format!("LANGUAGE {language}")));
    }
    match stmt.sql_body.as_ref().and_then(|body| body.node.as_ref()) {
        Some(Node::ReturnStmt(_)) => {
            return Err(ParseError::not_supported("a RETURN function body"))
        }
        Some(_) => return Err(ParseError::not_supported("a BEGIN ATOMIC function body")),
        None => {}
    }
    if options.contains_key("transform") {
        return Err(ParseError::not_supported("TRANSFORM in CREATE FUNCTION"));
    }
    if options.contains_key("window") {
        return Err(ParseError::not_supported("WINDOW in CREATE FUNCTION"));
    }
    if let Some(as_item) = options.get("as") {
        if !is_dollar_quoted_after(sql, as_item.location)? {
            return Err(ParseError::not_supported(
                "a function body that is not dollar-quoted",
            ));
        }
    }
    if let Some(statements) = &body_statements {
        check_body_statements(statements)?;
    }
    for (parameter, class) in parameters.iter().zip(&parameter_types) {
        check_parameter_features(parameter, class)?;
    }
    if let Some(class) = &result_type {
        check_result_features(stmt, class)?;
    }
    check_option_features(&stmt.options)?;
    if stmt.replace {
        return Err(ParseError::not_supported("CREATE OR REPLACE FUNCTION"));
    }
    Ok(())
}

/// The options of CREATE FUNCTION by name, such as `language` or
/// `volatility`. SET options are left out: there can be several.
fn function_options(
    options: &[pg_query::protobuf::Node],
) -> Result<HashMap<&str, &DefElem>, ParseError> {
    let mut by_name = HashMap::new();
    for def in def_elems(options)? {
        if def.defname == "set" {
            continue;
        }
        if by_name.insert(def.defname.as_str(), def).is_some() {
            return Err(ParseError::Sqlstate {
                sqlstate: SQLSTATE_SYNTAX_ERROR,
                message: "conflicting or redundant options".to_string(),
            });
        }
    }
    for (name, clause) in [("cost", "COST"), ("rows", "ROWS")] {
        if let Some(def) = by_name.get(name) {
            if numeric_value(def)? <= 0.0 {
                return Err(ParseError::Sqlstate {
                    sqlstate: SQLSTATE_INVALID_PARAMETER_VALUE,
                    message: format!("{clause} must be positive"),
                });
            }
        }
    }
    Ok(by_name)
}

fn def_elems(options: &[pg_query::protobuf::Node]) -> Result<Vec<&DefElem>, ParseError> {
    options
        .iter()
        .map(|option| match &option.node {
            Some(Node::DefElem(def)) => Ok(def.as_ref()),
            _ => Err(ParseError::ParseError(
                "expected a function option".to_string(),
            )),
        })
        .collect()
}

fn numeric_value(def: &DefElem) -> Result<f64, ParseError> {
    match def.arg.as_ref().and_then(|arg| arg.node.as_ref()) {
        Some(Node::Integer(integer)) => Ok(integer.ival as f64),
        Some(Node::Float(float)) => float
            .fval
            .parse()
            .map_err(|_| ParseError::ParseError(format!("invalid number: {}", float.fval))),
        _ => Err(ParseError::ParseError(format!(
            "{} requires a numeric value",
            def.defname.to_uppercase()
        ))),
    }
}

/// The function's language. Without a LANGUAGE clause, a function whose body
/// is written in SQL (RETURN or BEGIN ATOMIC) is in language `sql`.
fn function_language(
    options: &HashMap<&str, &DefElem>,
    stmt: &CreateFunctionStmt,
) -> Result<String, ParseError> {
    let Some(def) = options.get("language") else {
        if stmt.sql_body.is_some() {
            return Ok("sql".to_string());
        }
        return Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_INVALID_FUNCTION_DEFINITION,
            message: "no language specified".to_string(),
        });
    };
    let language = string_value(def)?;
    if !matches!(language, "sql" | "plpgsql" | "c" | "internal") {
        return Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_UNDEFINED_OBJECT,
            message: format!("language \"{language}\" does not exist"),
        });
    }
    Ok(language.to_string())
}

fn string_value(def: &DefElem) -> Result<&str, ParseError> {
    match def.arg.as_ref().and_then(|arg| arg.node.as_ref()) {
        Some(Node::String(string)) => Ok(&string.sval),
        _ => Err(ParseError::ParseError(format!(
            "{} requires a name",
            def.defname.to_uppercase()
        ))),
    }
}

fn function_parameters(stmt: &CreateFunctionStmt) -> Result<Vec<&FunctionParameter>, ParseError> {
    stmt.parameters
        .iter()
        .map(|node| match &node.node {
            Some(Node::FunctionParameter(parameter)) => Ok(parameter.as_ref()),
            _ => Err(ParseError::ParseError(
                "expected a function parameter".to_string(),
            )),
        })
        .collect()
}

/// The class of a parameter's type, or PostgreSQL's error for a type that a
/// parameter cannot have.
fn check_parameter_type(
    parameter: &FunctionParameter,
    is_sql: bool,
) -> Result<TypeClass, ParseError> {
    let type_name = parameter
        .arg_type
        .as_ref()
        .ok_or_else(|| ParseError::ParseError("function parameter without a type".to_string()))?;
    match classify_type(type_name)? {
        TypeClass::Pseudo(name) if is_sql => Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_INVALID_FUNCTION_DEFINITION,
            message: format!("SQL functions cannot have arguments of type {name}"),
        }),
        TypeClass::NotBuiltIn { schema, name } => Err(ParseError::UnresolvedFunctionType {
            schema,
            name,
            full_name: type_name_text(type_name)?,
            used_as: FunctionTypeUse::Parameter,
        }),
        class => Ok(class),
    }
}

/// The class of the result type, or PostgreSQL's error for a function
/// without a result type or with a type that a result cannot have. A
/// function with OUT parameters can leave out its result type.
fn check_result_type(
    stmt: &CreateFunctionStmt,
    parameters: &[&FunctionParameter],
    parameter_types: &[TypeClass],
    is_sql: bool,
) -> Result<Option<TypeClass>, ParseError> {
    let invalid = |message: String| ParseError::Sqlstate {
        sqlstate: SQLSTATE_INVALID_FUNCTION_DEFINITION,
        message,
    };
    let Some(type_name) = &stmt.return_type else {
        if parameters
            .iter()
            .any(|parameter| is_output(parameter.mode()))
        {
            return Ok(None);
        }
        return Err(invalid(
            "function result type must be specified".to_string(),
        ));
    };
    match classify_type(type_name)? {
        // PostgreSQL finds the result type from the types of the arguments.
        // Which polymorphic parameter types determine which result types is
        // left to the rejection of polymorphic parameters.
        TypeClass::Polymorphic(_)
            if !parameter_types
                .iter()
                .any(|class| matches!(class, TypeClass::Polymorphic(_))) =>
        {
            Err(invalid("cannot determine result data type".to_string()))
        }
        // SQL functions cannot have internal parameters.
        TypeClass::Pseudo(name) if name == "internal" => Err(invalid(
            "unsafe use of pseudo-type \"internal\"".to_string(),
        )),
        TypeClass::Pseudo(name) if is_sql && !matches!(name.as_str(), "record" | "void") => {
            Err(invalid(format!("SQL functions cannot return type {name}")))
        }
        TypeClass::NotBuiltIn { schema, name } => Err(ParseError::UnresolvedFunctionType {
            schema,
            name,
            full_name: type_name_text(type_name)?,
            used_as: if type_name.setof {
                FunctionTypeUse::SetofResult
            } else {
                FunctionTypeUse::Result
            },
        }),
        class => Ok(Some(class)),
    }
}

fn is_output(mode: FunctionParameterMode) -> bool {
    matches!(
        mode,
        FunctionParameterMode::FuncParamOut
            | FunctionParameterMode::FuncParamInout
            | FunctionParameterMode::FuncParamTable
    )
}

/// The text of the function body, or PostgreSQL's error for a function
/// without a body or with two.
fn function_body<'a>(
    options: &HashMap<&str, &'a DefElem>,
    stmt: &CreateFunctionStmt,
    is_sql: bool,
) -> Result<Option<&'a str>, ParseError> {
    let invalid = |message: &str| ParseError::Sqlstate {
        sqlstate: SQLSTATE_INVALID_FUNCTION_DEFINITION,
        message: message.to_string(),
    };
    let Some(as_item) = options.get("as") else {
        if stmt.sql_body.is_none() {
            return Err(invalid("no function body specified"));
        }
        return Ok(None);
    };
    if stmt.sql_body.is_some() {
        return Err(invalid("duplicate function body specified"));
    }
    let Some(Node::List(items)) = as_item.arg.as_ref().and_then(|arg| arg.node.as_ref()) else {
        return Err(ParseError::ParseError(
            "AS requires a function body".to_string(),
        ));
    };
    let body = match items.items.as_slice() {
        [item] => item,
        [item, _] if !is_sql => item,
        _ => return Err(invalid("only one AS item needed for language \"sql\"")),
    };
    match &body.node {
        Some(Node::String(body)) => Ok(Some(&body.sval)),
        _ => Err(ParseError::ParseError(
            "AS requires a function body".to_string(),
        )),
    }
}

fn parse_body(body: &str) -> Result<pg_query::ParseResult, ParseError> {
    pg_query::parse(body).map_err(|error| ParseError::Sqlstate {
        sqlstate: SQLSTATE_SYNTAX_ERROR,
        message: match error {
            pg_query::Error::Parse(message) => message,
            error => error.to_string(),
        },
    })
}

/// Whether the token after the AS keyword at `as_location` of `sql` is a
/// dollar-quoted string.
fn is_dollar_quoted_after(sql: &str, as_location: i32) -> Result<bool, ParseError> {
    let scan = pg_query::scan(sql).map_err(|e| ParseError::ParseError(e.to_string()))?;
    let body = scan
        .tokens
        .iter()
        .find(|token| token.start > as_location)
        .ok_or_else(|| ParseError::ParseError("AS requires a function body".to_string()))?;
    Ok(sql[body.start as usize..].starts_with('$'))
}

/// Rejects a body that is not a single query.
fn check_body_statements(statements: &pg_query::ParseResult) -> Result<(), ParseError> {
    let [statement] = statements.protobuf.stmts.as_slice() else {
        if statements.protobuf.stmts.is_empty() {
            return Ok(());
        }
        return Err(ParseError::not_supported(
            "a function body with several statements",
        ));
    };
    match statement.stmt.as_ref().and_then(|stmt| stmt.node.as_ref()) {
        Some(NodeEnum::SelectStmt(select)) if select.into_clause.is_none() => Ok(()),
        Some(NodeEnum::SelectStmt(_)) => {
            Err(ParseError::not_supported("SELECT INTO in a function body"))
        }
        Some(
            NodeEnum::InsertStmt(_)
            | NodeEnum::UpdateStmt(_)
            | NodeEnum::DeleteStmt(_)
            | NodeEnum::MergeStmt(_),
        ) => Err(ParseError::not_supported(
            "a data-modifying statement in a function body",
        )),
        _ => Err(ParseError::not_supported(format!(
            "{} in a function body",
            match CommandTag::of(statements) {
                CommandTag::Unknown => "a utility statement",
                tag => tag.name(),
            }
        ))),
    }
}

fn check_parameter_features(
    parameter: &FunctionParameter,
    class: &TypeClass,
) -> Result<(), ParseError> {
    match parameter.mode() {
        FunctionParameterMode::FuncParamOut => {
            return Err(ParseError::not_supported("an OUT parameter"))
        }
        FunctionParameterMode::FuncParamInout => {
            return Err(ParseError::not_supported("an INOUT parameter"))
        }
        FunctionParameterMode::FuncParamVariadic => {
            return Err(ParseError::not_supported("a VARIADIC parameter"))
        }
        FunctionParameterMode::FuncParamTable => {
            return Err(ParseError::not_supported("RETURNS TABLE"))
        }
        FunctionParameterMode::FuncParamIn
        | FunctionParameterMode::FuncParamDefault
        | FunctionParameterMode::Undefined => {}
    }
    if parameter.defexpr.is_some() {
        return Err(ParseError::not_supported(
            "a function parameter with a DEFAULT",
        ));
    }
    if !parameter.name.is_empty() {
        return Err(ParseError::not_supported("a named function parameter"));
    }
    match class {
        TypeClass::Base => Ok(()),
        TypeClass::Polymorphic(name) => Err(ParseError::not_supported(format!(
            "a function parameter of polymorphic type {name}"
        ))),
        TypeClass::Array(name) => Err(ParseError::not_supported(format!(
            "a function parameter of array type {name}"
        ))),
        TypeClass::ColumnType => Err(ParseError::not_supported(
            "%TYPE in a function parameter type",
        )),
        TypeClass::Pseudo(_) | TypeClass::NotBuiltIn { .. } => {
            unreachable!("{class:?} is rejected with the error of PostgreSQL")
        }
    }
}

fn check_result_features(stmt: &CreateFunctionStmt, class: &TypeClass) -> Result<(), ParseError> {
    if stmt.return_type.as_ref().is_some_and(|t| t.setof) {
        return Err(ParseError::not_supported("RETURNS SETOF"));
    }
    match class {
        TypeClass::Base => Ok(()),
        TypeClass::Pseudo(name) => Err(ParseError::not_supported(format!("RETURNS {name}"))),
        TypeClass::Array(name) => Err(ParseError::not_supported(format!(
            "a function result of array type {name}"
        ))),
        TypeClass::ColumnType => Err(ParseError::not_supported("%TYPE in a function result type")),
        // A polymorphic result needs a polymorphic parameter, which is
        // rejected first.
        TypeClass::Polymorphic(_) | TypeClass::NotBuiltIn { .. } => {
            unreachable!("{class:?} is rejected before the function's result")
        }
    }
}

/// Rejects the first option other than LANGUAGE and AS, in the order of the
/// statement.
fn check_option_features(options: &[pg_query::protobuf::Node]) -> Result<(), ParseError> {
    for def in def_elems(options)? {
        let clause = match def.defname.as_str() {
            "language" | "as" => continue,
            "volatility" => string_value(def)?.to_uppercase(),
            "strict" if bool_value(def) => "STRICT".to_string(),
            "strict" => "CALLED ON NULL INPUT".to_string(),
            "security" if bool_value(def) => "SECURITY DEFINER".to_string(),
            "security" => "SECURITY INVOKER".to_string(),
            "leakproof" if bool_value(def) => "LEAKPROOF".to_string(),
            "leakproof" => "NOT LEAKPROOF".to_string(),
            "parallel" => format!("PARALLEL {}", string_value(def)?.to_uppercase()),
            "cost" => "COST".to_string(),
            "rows" => unreachable!("ROWS needs SETOF, which is rejected first"),
            "support" => "SUPPORT".to_string(),
            "set" => match def.arg.as_ref().and_then(|arg| arg.node.as_ref()) {
                Some(Node::VariableSetStmt(set))
                    if set.kind() == VariableSetKind::VarSetCurrent =>
                {
                    "SET FROM CURRENT".to_string()
                }
                _ => "SET".to_string(),
            },
            other => unreachable!("option {other} is rejected before the other options"),
        };
        return Err(ParseError::not_supported(format!(
            "{clause} in CREATE FUNCTION"
        )));
    }
    Ok(())
}

fn bool_value(def: &DefElem) -> bool {
    matches!(
        def.arg.as_ref().and_then(|arg| arg.node.as_ref()),
        Some(Node::Boolean(boolean)) if boolean.boolval
    )
}

/// What a type name of a parameter or result names.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TypeClass {
    /// A built-in type that holds data, such as `int4` or `text`.
    Base,
    /// A built-in type that stands for the types of the arguments, such as
    /// `anyelement`.
    Polymorphic(String),
    /// A built-in type that is not polymorphic and holds no data of its own,
    /// such as `record`, `void` or `trigger`, as PostgreSQL writes it in
    /// errors.
    Pseudo(String),
    /// An array of a built-in type, such as `int4[]`.
    Array(String),
    /// `table.column%TYPE`.
    ColumnType,
    /// A relation, a custom type such as a domain or an enum, or no type at
    /// all. Only a caller with a connection can tell which.
    NotBuiltIn {
        schema: Option<String>,
        name: String,
    },
}

fn classify_type(type_name: &TypeName) -> Result<TypeClass, ParseError> {
    if type_name.pct_type {
        return Ok(TypeClass::ColumnType);
    }
    let names = extract_key_columns(&type_name.names)?;
    let (schema, name) = match names.as_slice() {
        [name] => (None, name),
        [schema, name] => (Some(schema), name),
        _ => return Err(ParseError::not_supported("a cross-database reference")),
    };
    let is_built_in_schema = schema.is_none_or(|schema| schema == "pg_catalog");
    let class = if !is_built_in_schema {
        None
    } else if POLYMORPHIC_TYPES.contains(&name.as_str()) {
        Some(TypeClass::Polymorphic(name.clone()))
    } else if name == "any" {
        // "any" is a keyword, so PostgreSQL quotes it.
        Some(TypeClass::Pseudo("\"any\"".to_string()))
    } else if PSEUDO_TYPES.contains(&name.as_str()) {
        Some(TypeClass::Pseudo(name.clone()))
    } else if BASE_TYPES.contains(&name.as_str()) {
        Some(TypeClass::Base)
    } else {
        None
    };
    let not_built_in = TypeClass::NotBuiltIn {
        schema: schema.cloned(),
        name: name.clone(),
    };
    if type_name.array_bounds.is_empty() {
        return Ok(class.unwrap_or(not_built_in));
    }
    // PostgreSQL has arrays of base types and of some pseudo-types, such as
    // record[], but not of polymorphic types.
    Ok(match class {
        Some(TypeClass::Base) => TypeClass::Array(type_name_text(type_name)?),
        Some(TypeClass::Pseudo(name)) => {
            TypeClass::Pseudo(name + &"[]".repeat(type_name.array_bounds.len()))
        }
        _ => not_built_in,
    })
}

/// The type name as PostgreSQL writes it in an error about a type that does
/// not exist, such as `public.nosuch[]`.
fn type_name_text(type_name: &TypeName) -> Result<String, ParseError> {
    let mut text = extract_key_columns(&type_name.names)?.join(".");
    for _ in &type_name.array_bounds {
        text.push_str("[]");
    }
    Ok(text)
}

// FIXME: the three lists that follow are written by hand, and they must
// follow the PostgreSQL version of pg_query. They are also one of three
// places that say what a type name is. The FIXME in
// postgres/frontend/pg_type.rs gives the plan for one type lookup.

/// The types that stand for the types of a function's arguments.
const POLYMORPHIC_TYPES: &[&str] = &[
    "anyelement",
    "anyarray",
    "anynonarray",
    "anyenum",
    "anyrange",
    "anymultirange",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "anycompatiblemultirange",
];

/// The pseudo-types that are not polymorphic.
const PSEUDO_TYPES: &[&str] = &[
    "any",
    "cstring",
    "internal",
    "language_handler",
    "fdw_handler",
    "table_am_handler",
    "index_am_handler",
    "tsm_handler",
    "record",
    "trigger",
    "event_trigger",
    "pg_ddl_command",
    "void",
    "unknown",
];

/// The built-in base, range and multirange types, by the names pg_query
/// gives them: `int` is `int4` and `double precision` is `float8`, for
/// example.
const BASE_TYPES: &[&str] = &[
    "bool",
    "int2",
    "int4",
    "int8",
    "float4",
    "float8",
    "numeric",
    "money",
    "text",
    "varchar",
    "bpchar",
    "char",
    "name",
    "bytea",
    "uuid",
    "date",
    "time",
    "timetz",
    "timestamp",
    "timestamptz",
    "interval",
    "json",
    "jsonb",
    "xml",
    "inet",
    "cidr",
    "macaddr",
    "macaddr8",
    "bit",
    "varbit",
    "oid",
    "regclass",
    "regtype",
    "regproc",
    "regprocedure",
    "regnamespace",
    "regrole",
    "point",
    "line",
    "lseg",
    "box",
    "path",
    "polygon",
    "circle",
    "tsvector",
    "tsquery",
    "gtsvector",
    "jsonpath",
    "refcursor",
    "aclitem",
    "int2vector",
    "oidvector",
    "tid",
    "xid",
    "xid8",
    "cid",
    "pg_lsn",
    "pg_snapshot",
    "txid_snapshot",
    "regoper",
    "regoperator",
    "regconfig",
    "regdictionary",
    "regcollation",
    "regdatabase",
    "pg_node_tree",
    "pg_ndistinct",
    "pg_dependencies",
    "pg_mcv_list",
    "pg_brin_bloom_summary",
    "pg_brin_minmax_multi_summary",
    "int4range",
    "int8range",
    "numrange",
    "daterange",
    "tsrange",
    "tstzrange",
    "int4multirange",
    "int8multirange",
    "nummultirange",
    "datemultirange",
    "tsmultirange",
    "tstzmultirange",
];
