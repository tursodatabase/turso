//! CREATE FUNCTION, CREATE PROCEDURE and DROP FUNCTION.
//!
//! tursopg supports SQL functions whose parameters and result have base
//! types and whose body is one query in a dollar-quoted string. A statement
//! that PostgreSQL rejects gets PostgreSQL's error. Any other statement that
//! tursopg does not support gets an error that names the first feature it
//! does not support, so that each feature can be enabled on its own.

use crate::cmdtag::CommandTag;
use crate::names::{extract_key_columns, schema_qualifier};
use crate::sqlstate::{
    SQLSTATE_INVALID_FUNCTION_DEFINITION, SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_SYNTAX_ERROR,
    SQLSTATE_UNDEFINED_OBJECT,
};
use crate::{FunctionTypeUse, ParseError};
use pg_query::protobuf::{
    node::Node, CreateFunctionStmt, DefElem, DropStmt, FunctionParameter, FunctionParameterMode,
    ObjectWithArgs, TypeName, VariableSetKind,
};
use pg_query::NodeEnum;
use std::collections::HashMap;
use turso_parser::ast;

/// The statement that `stmt`, a CREATE FUNCTION statement, translates to.
/// The text of the statement is not known here, so a body that is not
/// dollar-quoted is accepted: [`check_create_function`] rejects it.
pub(crate) fn translate_create_function(
    stmt: &CreateFunctionStmt,
) -> Result<ast::Stmt, ParseError> {
    let function = checked_function(None, stmt)?;
    Ok(ast::Stmt::CreateFunction(ast::CreateFunction {
        or_replace: stmt.replace,
        name: function.name,
        parameter_types: function.parameter_types,
        return_type: function.return_type,
        body: function.body,
    }))
}

/// Rejects `stmt`, a CREATE FUNCTION or CREATE PROCEDURE statement of `sql`,
/// when PostgreSQL or tursopg does not support it.
pub(crate) fn check_create_function(
    sql: &str,
    stmt: &CreateFunctionStmt,
) -> Result<(), ParseError> {
    checked_function(Some(sql), stmt).map(|_| ())
}

/// The statement that `drop`, a DROP FUNCTION statement, translates to.
pub(crate) fn translate_drop_function(drop: &DropStmt) -> Result<ast::Stmt, ParseError> {
    if drop.behavior == pg_query::protobuf::DropBehavior::DropCascade as i32 {
        return Err(ParseError::not_supported("DROP FUNCTION ... CASCADE"));
    }
    let [object] = drop.objects.as_slice() else {
        return Err(ParseError::not_supported(
            "DROP FUNCTION of several objects",
        ));
    };
    let Some(Node::ObjectWithArgs(function)) = &object.node else {
        return Err(ParseError::ParseError(
            "DROP FUNCTION: expected a function name".to_string(),
        ));
    };
    Ok(ast::Stmt::DropFunction(ast::DropFunction {
        if_exists: drop.missing_ok,
        name: function_name(&function.objname)?,
        parameter_types: dropped_parameter_types(function)?,
    }))
}

/// The parameter types that DROP FUNCTION names the function with, or None
/// when it names the function without its parameters.
fn dropped_parameter_types(function: &ObjectWithArgs) -> Result<Option<Vec<String>>, ParseError> {
    if function.args_unspecified {
        return Ok(None);
    }
    function
        .objargs
        .iter()
        .map(|argument| match &argument.node {
            Some(Node::TypeName(type_name)) => signature_type_name(type_name),
            _ => Err(ParseError::ParseError(
                "DROP FUNCTION: expected a parameter type".to_string(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// The name of `type_name` in the signature of a function: the name
/// PostgreSQL stores for a built-in type, such as `int4` for `integer`,
/// with `[]` for each array dimension. Type modifiers do not take part in a
/// signature.
fn signature_type_name(type_name: &TypeName) -> Result<String, ParseError> {
    let names = extract_key_columns(&type_name.names)?;
    let name = match names.as_slice() {
        [name] => name.clone(),
        [schema, name] if schema == "pg_catalog" => name.clone(),
        _ => names.join("."),
    };
    Ok(name + &"[]".repeat(type_name.array_bounds.len()))
}

/// The schema and name of a function. A function is not stored under the
/// names of PostgreSQL's own schemas.
fn function_name(names: &[pg_query::protobuf::Node]) -> Result<ast::QualifiedName, ParseError> {
    let names = extract_key_columns(names)?;
    let (schema, name) = match names.as_slice() {
        [name] => (None, name),
        [schema, name] => (Some(schema), name),
        [] => return Err(ParseError::ParseError("missing function name".to_string())),
        _ => return Err(ParseError::not_supported("a cross-database reference")),
    };
    // A call names a function of a schema as `schema.name`.
    if name.contains('.') || schema.is_some_and(|schema| schema.contains('.')) {
        return Err(ParseError::not_supported(
            "a function or schema name with a dot",
        ));
    }
    let name = ast::Name::exact(name.clone());
    Ok(match schema {
        Some(schema) if matches!(schema.as_str(), "pg_catalog" | "information_schema") => {
            return Err(ParseError::not_supported(format!(
                "a function in schema {schema}"
            )))
        }
        Some(schema) => ast::QualifiedName::fullname(
            ast::Name::exact(schema_qualifier(schema)?.to_string()),
            name,
        ),
        None => ast::QualifiedName::single(name),
    })
}

/// What CREATE FUNCTION defines, once PostgreSQL and tursopg accept it.
struct CheckedFunction {
    name: ast::QualifiedName,
    parameter_types: Vec<String>,
    return_type: String,
    body: String,
}

/// Raises the errors of PostgreSQL in the order PostgreSQL raises them, then
/// the error for the first feature that tursopg does not support. `sql` is
/// the text of the statement, when it is known.
fn checked_function(
    sql: Option<&str>,
    stmt: &CreateFunctionStmt,
) -> Result<CheckedFunction, ParseError> {
    if stmt.is_procedure {
        return Err(ParseError::not_supported("CREATE PROCEDURE"));
    }
    let name = function_name(&stmt.funcname)?;
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
    if let (Some(sql), Some(as_item)) = (sql, options.get("as")) {
        if !is_dollar_quoted_after(sql, as_item.location)? {
            return Err(ParseError::not_supported(
                "a function body that is not dollar-quoted",
            ));
        }
    }
    let body_statements = body_statements.expect("an SQL function without a RETURN has a body");
    check_body_statements(&body_statements)?;
    for (parameter, class) in parameters.iter().zip(&parameter_types) {
        check_parameter_features(parameter, class)?;
    }
    let result_type = result_type.expect("a function without OUT parameters has a result type");
    check_result_features(stmt, &result_type)?;
    check_option_features(&stmt.options)?;
    check_body_query(&body_statements, stmt)?;
    let parameter_types = parameters
        .iter()
        .map(|parameter| {
            signature_type_name(
                parameter
                    .arg_type
                    .as_ref()
                    .expect("the type of each parameter was checked"),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let return_type = signature_type_name(
        stmt.return_type
            .as_ref()
            .expect("the result type was checked"),
    )?;
    Ok(CheckedFunction {
        name,
        parameter_types,
        return_type,
        body: body.expect("the body was parsed").to_string(),
    })
}

/// Rejects a body whose final statement cannot give the function's result,
/// as PostgreSQL does when it validates the function. The frontend checks
/// the rest of what PostgreSQL validates, which needs the query compiled.
fn check_body_query(
    statements: &pg_query::ParseResult,
    stmt: &CreateFunctionStmt,
) -> Result<(), ParseError> {
    let result_type = || {
        stmt.return_type
            .as_ref()
            .map(display_type_name)
            .transpose()
            .map(Option::unwrap_or_default)
    };
    let mismatch = || -> Result<(), ParseError> {
        Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_INVALID_FUNCTION_DEFINITION,
            message: return_type_mismatch_message(&result_type()?),
        })
    };
    // A function's final statement must be SELECT or have RETURNING, which
    // PostgreSQL says in the DETAIL of the error.
    let Some(NodeEnum::SelectStmt(select)) = statements
        .protobuf
        .stmts
        .first()
        .and_then(|statement| statement.stmt.as_ref())
        .and_then(|statement| statement.node.as_ref())
    else {
        return mismatch();
    };
    // The final statement must return exactly one column. The frontend
    // counts the columns of a query that compiles. A SELECT without columns
    // does not compile in tursopg.
    let is_plain_select = select.larg.is_none() && select.values_lists.is_empty();
    if is_plain_select && select.target_list.is_empty() {
        return mismatch();
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

/// Rejects a body that is not a single query. An empty body is left to
/// [`check_body_query`], which gives it PostgreSQL's error.
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

/// PostgreSQL's error for a body whose result is not what the function
/// returns. `return_type` is the type as PostgreSQL writes it in errors.
pub fn return_type_mismatch_message(return_type: &str) -> String {
    format!("return type mismatch in function declared to return {return_type}")
}

/// The type name as PostgreSQL writes it in most errors, such as `integer`
/// for `int4`.
fn display_type_name(type_name: &TypeName) -> Result<String, ParseError> {
    Ok(display_signature_type_name(&signature_type_name(
        type_name,
    )?))
}

/// The name that PostgreSQL writes in most errors for the type that a
/// signature names `name`, such as `integer` for `int4`.
pub fn display_signature_type_name(name: &str) -> String {
    let element = name.trim_end_matches("[]");
    let display = match element {
        "bool" => "boolean",
        "int2" => "smallint",
        "int4" => "integer",
        "int8" => "bigint",
        "float4" => "real",
        "float8" => "double precision",
        "varchar" => "character varying",
        "bpchar" => "character",
        "char" => "\"char\"",
        "time" => "time without time zone",
        "timetz" => "time with time zone",
        "timestamp" => "timestamp without time zone",
        "timestamptz" => "timestamp with time zone",
        "varbit" => "bit varying",
        // PostgreSQL shows a name with an uppercase letter in quotes.
        other if other.bytes().any(|byte| byte.is_ascii_uppercase()) => {
            return format!("\"{other}\"{}", &name[element.len()..]);
        }
        other => other,
    };
    format!("{display}{}", &name[element.len()..])
}

/// Whether a signature names a built-in base type `name`.
pub fn is_base_type_name(name: &str) -> bool {
    BASE_TYPES.contains(&name)
}

/// Whether a signature names a built-in pseudo-type `name`, such as
/// `anyelement` or `record`.
pub fn is_pseudo_type_name(name: &str) -> bool {
    POLYMORPHIC_TYPES.contains(&name) || PSEUDO_TYPES.contains(&name)
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

#[cfg(test)]
mod tests {
    use crate::translator::PostgreSQLTranslator;
    use turso_parser::ast;

    fn translate(sql: &str) -> ast::Stmt {
        let parsed = crate::parse(sql).unwrap();
        PostgreSQLTranslator::new().translate(&parsed).unwrap()
    }

    #[test]
    fn create_function_keeps_the_types_as_postgres_stores_them() {
        let ast::Stmt::CreateFunction(function) = translate(
            "CREATE OR REPLACE FUNCTION app.pad(varchar(10), int, double precision) \
             RETURNS timestamp with time zone LANGUAGE sql AS $$ SELECT now() $$",
        ) else {
            panic!("expected CREATE FUNCTION");
        };
        assert!(function.or_replace);
        assert_eq!(function.name.namespace.unwrap().as_str(), "app");
        assert_eq!(function.name.name.as_str(), "pad");
        assert_eq!(function.parameter_types, ["varchar", "int4", "float8"]);
        assert_eq!(function.return_type, "timestamptz");
        assert_eq!(function.body, " SELECT now() ");
    }

    #[test]
    fn drop_function_tells_no_parameters_from_no_parameter_list() {
        let ast::Stmt::DropFunction(without_list) = translate("DROP FUNCTION one") else {
            panic!("expected DROP FUNCTION");
        };
        assert_eq!(without_list.parameter_types, None);
        assert!(!without_list.if_exists);

        let ast::Stmt::DropFunction(without_parameters) =
            translate("DROP FUNCTION IF EXISTS app.one()")
        else {
            panic!("expected DROP FUNCTION");
        };
        assert_eq!(without_parameters.parameter_types, Some(vec![]));
        assert!(without_parameters.if_exists);
        assert_eq!(without_parameters.name.namespace.unwrap().as_str(), "app");

        let ast::Stmt::DropFunction(with_parameters) =
            translate("DROP FUNCTION one(integer, text[], app.item)")
        else {
            panic!("expected DROP FUNCTION");
        };
        assert_eq!(
            with_parameters.parameter_types,
            Some(vec![
                "int4".to_string(),
                "text[]".to_string(),
                "app.item".to_string()
            ])
        );
    }

    #[test]
    fn a_function_statement_formats_as_sql() {
        assert_eq!(
            translate("CREATE FUNCTION greet(text) RETURNS text LANGUAGE sql AS $$SELECT 'hi'$$")
                .to_string(),
            "CREATE FUNCTION greet (text) RETURNS text AS 'SELECT ''hi'''"
        );
        assert_eq!(
            translate("DROP FUNCTION IF EXISTS greet(text)").to_string(),
            "DROP FUNCTION IF EXISTS greet (text)"
        );
    }
}
