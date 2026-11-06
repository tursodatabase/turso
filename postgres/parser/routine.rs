//! Errors for the statements that create, change, drop or call functions,
//! procedures and triggers.
//!
//! tursopg cannot run these statements, so each of them gets an error. The
//! error names the first feature of the statement that tursopg does not
//! support. A CREATE FUNCTION statement that uses only the features of a
//! STABLE SQL function with base types gets the last error,
//! "CREATE FUNCTION is not supported".

use crate::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_FUNCTION_DEFINITION, SQLSTATE_SYNTAX_ERROR,
    SQLSTATE_UNDEFINED_OBJECT,
};
use crate::translator::not_supported;
use crate::{quote_identifier, ParseError};
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{
    CreateFunctionStmt, CreateTrigStmt, DefElem, FunctionParameter, FunctionParameterMode,
    LockClauseStrength, ObjectType, Token, TypeName, VariableSetKind,
};
use pg_query::ParseResult;

/// What a type name in a function signature names in the catalog, when the
/// name is not the name of a built-in type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogType {
    /// The row type of a table, view or sequence, which is a composite type.
    RowType,
    /// A type made with CREATE TYPE or CREATE DOMAIN.
    UserDefined,
    /// No type has the name.
    Missing,
}

/// The error for the first statement of `parse_result` when the statement
/// creates, changes, drops or calls a function, procedure or trigger. None
/// for every other statement. `sql` is the text that was parsed into
/// `parse_result`. `find_type` looks up a type name that is not built in,
/// with its schema when the name has one.
pub fn routine_error(
    sql: &str,
    parse_result: &ParseResult,
    find_type: impl Fn(Option<&str>, &str) -> CatalogType,
) -> Option<ParseError> {
    Some(match crate::first_statement(parse_result)? {
        Node::CreateFunctionStmt(stmt) => create_function_error(sql, stmt, &find_type),
        Node::CreateTrigStmt(stmt) => create_trigger_error(stmt),
        Node::AlterFunctionStmt(stmt) => alter_error(stmt.objtype())?,
        Node::RenameStmt(stmt) => alter_error(stmt.rename_type())?,
        Node::AlterOwnerStmt(stmt) => alter_error(stmt.object_type())?,
        Node::AlterObjectSchemaStmt(stmt) => alter_error(stmt.object_type())?,
        Node::AlterObjectDependsStmt(stmt) => alter_error(stmt.object_type())?,
        Node::DropStmt(stmt) => not_supported(format!(
            "DROP {}",
            routine_object_keyword(stmt.remove_type())?
        )),
        Node::CreateEventTrigStmt(_) => not_supported("CREATE EVENT TRIGGER"),
        Node::AlterEventTrigStmt(_) => not_supported("ALTER EVENT TRIGGER"),
        Node::CreateTransformStmt(_) => not_supported("CREATE TRANSFORM"),
        Node::CreatePlangStmt(_) => not_supported("CREATE LANGUAGE"),
        Node::DefineStmt(stmt) if stmt.kind() == ObjectType::ObjectAggregate => {
            not_supported("CREATE AGGREGATE")
        }
        Node::DoStmt(_) => not_supported("DO"),
        Node::CallStmt(_) => not_supported("CALL"),
        _ => return None,
    })
}

fn alter_error(object: ObjectType) -> Option<ParseError> {
    Some(not_supported(format!(
        "ALTER {}",
        routine_object_keyword(object)?
    )))
}

/// The SQL keyword of an object kind that every statement of this module is
/// about, such as `FUNCTION`.
fn routine_object_keyword(object: ObjectType) -> Option<&'static str> {
    Some(match object {
        ObjectType::ObjectFunction => "FUNCTION",
        ObjectType::ObjectProcedure => "PROCEDURE",
        ObjectType::ObjectRoutine => "ROUTINE",
        ObjectType::ObjectAggregate => "AGGREGATE",
        ObjectType::ObjectTrigger => "TRIGGER",
        ObjectType::ObjectEventTrigger => "EVENT TRIGGER",
        ObjectType::ObjectTransform => "TRANSFORM",
        ObjectType::ObjectLanguage => "LANGUAGE",
        _ => return None,
    })
}

fn create_function_error(
    sql: &str,
    stmt: &CreateFunctionStmt,
    find_type: &dyn Fn(Option<&str>, &str) -> CatalogType,
) -> ParseError {
    match check_create_function(sql, stmt, find_type) {
        Err(error) => error,
        Ok(()) => not_supported("CREATE FUNCTION"),
    }
}

/// Checks a CREATE FUNCTION or CREATE PROCEDURE statement. The error is for
/// the first feature that tursopg does not support, or for a definition
/// that PostgreSQL rejects too. Ok for a STABLE SQL function with
/// parameters and a result of base types and a dollar-quoted body of one
/// SELECT statement.
fn check_create_function(
    sql: &str,
    stmt: &CreateFunctionStmt,
    find_type: &dyn Fn(Option<&str>, &str) -> CatalogType,
) -> Result<(), ParseError> {
    if stmt.is_procedure {
        return Err(not_supported("CREATE PROCEDURE"));
    }
    let options = FunctionOptions::new(&stmt.options)?;
    check_language(stmt, &options)?;
    if let Some(option) = options.other.first() {
        return Err(option_error(option));
    }
    check_volatility(options.volatility)?;
    check_signature(stmt, find_type)?;
    check_body(sql, stmt, &options)
}

/// The options of a CREATE FUNCTION statement, such as LANGUAGE and STABLE.
struct FunctionOptions<'a> {
    /// The AS clause, which gives the body as a string.
    body: Option<&'a DefElem>,
    language: Option<&'a DefElem>,
    volatility: Option<&'a DefElem>,
    /// The options that tursopg does not support, in the order of the
    /// statement.
    other: Vec<&'a DefElem>,
}

impl<'a> FunctionOptions<'a> {
    /// Sorts `options`. Like PostgreSQL's compute_function_attributes(), it
    /// rejects an option that is given twice, except SET.
    fn new(options: &'a [pg_query::protobuf::Node]) -> Result<Self, ParseError> {
        let mut sorted = Self {
            body: None,
            language: None,
            volatility: None,
            other: Vec::new(),
        };
        let mut names: Vec<&str> = Vec::new();
        for option in options {
            let Some(Node::DefElem(option)) = &option.node else {
                continue;
            };
            let name = option.defname.as_str();
            if name != "set" {
                if names.contains(&name) {
                    return Err(ParseError::Sqlstate {
                        sqlstate: SQLSTATE_SYNTAX_ERROR,
                        message: "conflicting or redundant options".to_string(),
                    });
                }
                names.push(name);
            }
            match name {
                "as" => sorted.body = Some(option),
                "language" => sorted.language = Some(option),
                "volatility" => sorted.volatility = Some(option),
                _ => sorted.other.push(option),
            }
        }
        Ok(sorted)
    }
}

/// Rejects a function in a language other than SQL. The language of a
/// RETURN or BEGIN ATOMIC body is SQL. For a body in a string, PostgreSQL
/// requires LANGUAGE.
fn check_language(stmt: &CreateFunctionStmt, options: &FunctionOptions) -> Result<(), ParseError> {
    let has_sql_body = stmt.sql_body.is_some();
    match options.language.and_then(string_arg) {
        Some("sql") => Ok(()),
        Some(_) if has_sql_body => Err(invalid_function(
            "inline SQL function body only valid for language SQL",
        )),
        Some(language) => Err(not_supported(format!(
            "LANGUAGE {}",
            quote_identifier(language)
        ))),
        None if has_sql_body => Ok(()),
        None => Err(invalid_function("no language specified")),
    }
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

/// Rejects a function that is not STABLE. A function is VOLATILE unless it
/// is declared STABLE or IMMUTABLE.
fn check_volatility(volatility: Option<&DefElem>) -> Result<(), ParseError> {
    match volatility.and_then(string_arg) {
        Some("stable") => Ok(()),
        Some("immutable") => Err(not_supported("an IMMUTABLE function")),
        _ => Err(not_supported(
            "a VOLATILE function (a function without STABLE or IMMUTABLE)",
        )),
    }
}

/// Checks the parameters and the result of a function. Only a result of
/// one value and parameters without a mode, name or default are supported,
/// and each type must be a base type.
fn check_signature(
    stmt: &CreateFunctionStmt,
    find_type: &dyn Fn(Option<&str>, &str) -> CatalogType,
) -> Result<(), ParseError> {
    let parameters: Vec<&FunctionParameter> = stmt
        .parameters
        .iter()
        .filter_map(|parameter| match &parameter.node {
            Some(Node::FunctionParameter(parameter)) => Some(parameter.as_ref()),
            _ => None,
        })
        .collect();
    // The columns of RETURNS TABLE are parameters with the mode TABLE.
    if parameters
        .iter()
        .any(|parameter| parameter.mode() == FunctionParameterMode::FuncParamTable)
    {
        return Err(not_supported("RETURNS TABLE"));
    }
    let has_out_parameter = parameters.iter().any(|parameter| {
        matches!(
            parameter.mode(),
            FunctionParameterMode::FuncParamOut | FunctionParameterMode::FuncParamInout
        )
    });
    match &stmt.return_type {
        Some(return_type) if return_type.setof => return Err(not_supported("RETURNS SETOF")),
        Some(_) => {}
        // The OUT parameters give the result, and check_parameter() rejects
        // them.
        None if has_out_parameter => {}
        None => return Err(invalid_function("function result type must be specified")),
    }
    for parameter in parameters {
        check_parameter(parameter, find_type)?;
    }
    if let Some(return_type) = &stmt.return_type {
        check_type(return_type, TypeRole::Result, find_type)?;
    }
    Ok(())
}

fn check_parameter(
    parameter: &FunctionParameter,
    find_type: &dyn Fn(Option<&str>, &str) -> CatalogType,
) -> Result<(), ParseError> {
    match parameter.mode() {
        FunctionParameterMode::FuncParamOut => return Err(not_supported("an OUT parameter")),
        FunctionParameterMode::FuncParamInout => return Err(not_supported("an INOUT parameter")),
        FunctionParameterMode::FuncParamVariadic => {
            return Err(not_supported("a VARIADIC parameter"))
        }
        _ => {}
    }
    if let Some(arg_type) = &parameter.arg_type {
        check_type(arg_type, TypeRole::Parameter, find_type)?;
    }
    if !parameter.name.is_empty() {
        return Err(not_supported("a named function parameter"));
    }
    if parameter.defexpr.is_some() {
        return Err(not_supported("DEFAULT on a function parameter"));
    }
    Ok(())
}

/// Whether a type in a function signature is the type of a parameter or of
/// the result.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TypeRole {
    Parameter,
    Result,
}

impl TypeRole {
    /// The words for a value of this role in an error.
    fn noun(self) -> &'static str {
        match self {
            Self::Parameter => "a function parameter",
            Self::Result => "a function result",
        }
    }
}

/// Checks that a type in a function signature is a base type: a scalar
/// type that PostgreSQL has built in, such as integer or text.
fn check_type(
    type_name: &TypeName,
    role: TypeRole,
    find_type: &dyn Fn(Option<&str>, &str) -> CatalogType,
) -> Result<(), ParseError> {
    let what = role.noun();
    if type_name.pct_type {
        return Err(not_supported(format!("{what} type written with %TYPE")));
    }
    if type_name.setof {
        return Err(invalid_function("functions cannot accept set arguments"));
    }
    let names: Vec<&str> = type_name
        .names
        .iter()
        .filter_map(|name| match &name.node {
            Some(Node::String(name)) => Some(name.sval.as_str()),
            _ => None,
        })
        .collect();
    let (schema, name) = match names.as_slice() {
        [name] => (None, *name),
        [schema, name] => (Some(*schema), *name),
        _ => return Err(not_supported("a cross-database reference")),
    };
    if !type_name.array_bounds.is_empty() {
        return Err(not_supported(format!("{what} of an array type")));
    }
    if matches!(schema, None | Some("pg_catalog")) {
        if POLYMORPHIC_TYPES.contains(&name) {
            return Err(not_supported(format!("{what} of polymorphic type {name}")));
        }
        if PSEUDO_TYPES.contains(&name) {
            // PostgreSQL's fmgr_sql_validator() accepts a pseudo-type only
            // as the result, and only void and record.
            return Err(match (role, name) {
                (TypeRole::Result, "record") => not_supported("RETURNS record"),
                (TypeRole::Result, "void") => not_supported("RETURNS void"),
                (TypeRole::Result, _) => invalid_function(format!(
                    "SQL functions cannot return type {}",
                    quote_identifier(name)
                )),
                (TypeRole::Parameter, _) => invalid_function(format!(
                    "SQL functions cannot have arguments of type {}",
                    quote_identifier(name)
                )),
            });
        }
        if RANGE_TYPES.contains(&name) {
            return Err(not_supported(format!("{what} of range type {name}")));
        }
        if BASE_TYPES.contains(&name) {
            return Ok(());
        }
    }
    let written = match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    };
    Err(match find_type(schema, name) {
        CatalogType::RowType => not_supported(format!("{what} of composite type {written}")),
        CatalogType::UserDefined => not_supported(format!("{what} of user-defined type {written}")),
        CatalogType::Missing => ParseError::Sqlstate {
            sqlstate: SQLSTATE_UNDEFINED_OBJECT,
            message: format!("type {written} does not exist"),
        },
    })
}

/// The types of PostgreSQL that take the type of the argument a function
/// is called with.
const POLYMORPHIC_TYPES: &[&str] = &[
    "anyarray",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblemultirange",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "anyelement",
    "anyenum",
    "anymultirange",
    "anynonarray",
    "anyrange",
];

/// The pseudo-types of PostgreSQL that are not polymorphic.
const PSEUDO_TYPES: &[&str] = &[
    "_record",
    "any",
    "cstring",
    "event_trigger",
    "fdw_handler",
    "index_am_handler",
    "internal",
    "language_handler",
    "pg_ddl_command",
    "record",
    "table_am_handler",
    "trigger",
    "tsm_handler",
    "unknown",
    "void",
];

/// The range and multirange types that PostgreSQL has built in.
const RANGE_TYPES: &[&str] = &[
    "datemultirange",
    "daterange",
    "int4multirange",
    "int4range",
    "int8multirange",
    "int8range",
    "nummultirange",
    "numrange",
    "tsmultirange",
    "tsrange",
    "tstzmultirange",
    "tstzrange",
];

/// The base types that PostgreSQL has built in, by the names that the
/// parser gives them: `integer` is `int4`.
const BASE_TYPES: &[&str] = &[
    "aclitem",
    "bit",
    "bool",
    "box",
    "bpchar",
    "bytea",
    "char",
    "cid",
    "cidr",
    "circle",
    "date",
    "float4",
    "float8",
    "gtsvector",
    "inet",
    "int2",
    "int4",
    "int8",
    "interval",
    "json",
    "jsonb",
    "jsonpath",
    "line",
    "lseg",
    "macaddr",
    "macaddr8",
    "money",
    "name",
    "numeric",
    "oid",
    "path",
    "pg_brin_bloom_summary",
    "pg_brin_minmax_multi_summary",
    "pg_dependencies",
    "pg_lsn",
    "pg_mcv_list",
    "pg_ndistinct",
    "pg_node_tree",
    "pg_snapshot",
    "point",
    "polygon",
    "refcursor",
    "regclass",
    "regcollation",
    "regconfig",
    "regdictionary",
    "regnamespace",
    "regoper",
    "regoperator",
    "regproc",
    "regprocedure",
    "regrole",
    "regtype",
    "text",
    "tid",
    "time",
    "timestamp",
    "timestamptz",
    "timetz",
    "tsquery",
    "tsvector",
    "txid_snapshot",
    "uuid",
    "varbit",
    "varchar",
    "xid",
    "xid8",
    "xml",
];

/// Checks the body of a SQL function. Only a dollar-quoted string with one
/// SELECT statement is supported.
fn check_body(
    sql: &str,
    stmt: &CreateFunctionStmt,
    options: &FunctionOptions,
) -> Result<(), ParseError> {
    if let Some(sql_body) = stmt.sql_body.as_deref() {
        if options.body.is_some() {
            return Err(invalid_function("duplicate function body specified"));
        }
        return Err(match &sql_body.node {
            Some(Node::ReturnStmt(_)) => not_supported("CREATE FUNCTION ... RETURN"),
            _ => not_supported("CREATE FUNCTION ... BEGIN ATOMIC"),
        });
    }
    let Some(body) = options.body else {
        return Err(invalid_function("no function body specified"));
    };
    let items = match node_arg(body) {
        Some(Node::List(list)) => list.items.as_slice(),
        _ => &[],
    };
    let [item] = items else {
        return Err(invalid_function(
            "only one AS item needed for language \"sql\"",
        ));
    };
    let Some(Node::String(text)) = &item.node else {
        return Err(invalid_function("no function body specified"));
    };
    if !string_after_is_dollar_quoted(sql, body.location) {
        return Err(not_supported("a function body that is not dollar-quoted"));
    }
    check_body_statements(&text.sval)
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

/// Checks the statements of a SQL function body when the function is
/// created. PostgreSQL rejects a statement that changes data in a STABLE
/// function only when the function runs.
fn check_body_statements(body: &str) -> Result<(), ParseError> {
    let parsed = pg_query::parse(body).map_err(|error| ParseError::Sqlstate {
        sqlstate: SQLSTATE_SYNTAX_ERROR,
        message: match error {
            pg_query::Error::Parse(message) => message,
            error => error.to_string(),
        },
    })?;
    let statements: Vec<&Node> = parsed
        .protobuf
        .stmts
        .iter()
        .filter_map(|stmt| stmt.stmt.as_ref()?.node.as_ref())
        .collect();
    for statement in &statements {
        if is_ddl(statement) {
            return Err(not_supported("a DDL statement in a function body"));
        }
        if let Some(command) = data_changing_command(statement) {
            return Err(ParseError::Sqlstate {
                sqlstate: SQLSTATE_FEATURE_NOT_SUPPORTED,
                message: format!("{command} is not allowed in a non-volatile function"),
            });
        }
    }
    match statements.as_slice() {
        [Node::SelectStmt(_)] => Ok(()),
        [] | [_] => Err(invalid_function(
            "a function body must end with a SELECT statement",
        )),
        _ => Err(not_supported(
            "a function body with more than one statement",
        )),
    }
}

/// Whether a statement defines, changes or drops an object or a privilege.
/// SELECT INTO creates a table.
fn is_ddl(statement: &Node) -> bool {
    match statement {
        Node::SelectStmt(select) => select.into_clause.is_some(),
        Node::InsertStmt(_)
        | Node::UpdateStmt(_)
        | Node::DeleteStmt(_)
        | Node::MergeStmt(_)
        | Node::TruncateStmt(_)
        | Node::TransactionStmt(_)
        | Node::VariableSetStmt(_)
        | Node::VariableShowStmt(_)
        | Node::ExplainStmt(_)
        | Node::CopyStmt(_)
        | Node::LockStmt(_)
        | Node::NotifyStmt(_)
        | Node::ListenStmt(_)
        | Node::UnlistenStmt(_)
        | Node::CheckPointStmt(_)
        | Node::VacuumStmt(_)
        | Node::DiscardStmt(_)
        | Node::PrepareStmt(_)
        | Node::ExecuteStmt(_)
        | Node::DeallocateStmt(_)
        | Node::DeclareCursorStmt(_)
        | Node::FetchStmt(_)
        | Node::ClosePortalStmt(_)
        | Node::DoStmt(_)
        | Node::CallStmt(_)
        | Node::LoadStmt(_)
        | Node::ClusterStmt(_)
        | Node::ReindexStmt(_)
        | Node::ConstraintsSetStmt(_) => false,
        _ => true,
    }
}

/// The command name of a statement that changes data, as PostgreSQL names
/// it in its errors. A SELECT changes data with a data-modifying WITH query
/// or locks rows with FOR UPDATE or FOR SHARE.
fn data_changing_command(statement: &Node) -> Option<&'static str> {
    match statement {
        Node::InsertStmt(_) => Some("INSERT"),
        Node::UpdateStmt(_) => Some("UPDATE"),
        Node::DeleteStmt(_) => Some("DELETE"),
        Node::MergeStmt(_) => Some("MERGE"),
        Node::TruncateStmt(_) => Some("TRUNCATE TABLE"),
        Node::SelectStmt(select) => {
            let has_modifying_query = select
                .with_clause
                .iter()
                .flat_map(|with| &with.ctes)
                .filter_map(|cte| match &cte.node {
                    Some(Node::CommonTableExpr(cte)) => cte.ctequery.as_deref()?.node.as_ref(),
                    _ => None,
                })
                .any(|query| data_changing_command(query).is_some());
            if has_modifying_query {
                return Some("SELECT");
            }
            select
                .locking_clause
                .iter()
                .find_map(|clause| match &clause.node {
                    Some(Node::LockingClause(clause)) => Some(match clause.strength() {
                        LockClauseStrength::LcsForkeyshare => "SELECT FOR KEY SHARE",
                        LockClauseStrength::LcsForshare => "SELECT FOR SHARE",
                        LockClauseStrength::LcsFornokeyupdate => "SELECT FOR NO KEY UPDATE",
                        _ => "SELECT FOR UPDATE",
                    }),
                    _ => None,
                })
        }
        _ => None,
    }
}

/// The error for CREATE TRIGGER. A feature that tursopg does not plan to
/// support comes first, then statement-level triggers.
fn create_trigger_error(stmt: &CreateTrigStmt) -> ParseError {
    // The flags of PostgreSQL's trigger.h in `timing` and `events`.
    const TRIGGER_TYPE_TRUNCATE: i32 = 1 << 5;
    const TRIGGER_TYPE_INSTEAD: i32 = 1 << 6;

    let clause = if stmt.isconstraint {
        return not_supported("CREATE CONSTRAINT TRIGGER");
    } else if stmt.replace {
        return not_supported("CREATE OR REPLACE TRIGGER");
    } else if stmt.timing & TRIGGER_TYPE_INSTEAD != 0 {
        "INSTEAD OF"
    } else if stmt.events & TRIGGER_TYPE_TRUNCATE != 0 {
        "TRUNCATE"
    } else if !stmt.columns.is_empty() {
        "UPDATE OF"
    } else if stmt.when_clause.is_some() {
        "WHEN"
    } else if !stmt.transition_rels.is_empty() {
        "REFERENCING"
    } else if !stmt.args.is_empty() {
        return not_supported("an argument to a trigger function");
    } else if !stmt.row {
        return not_supported("a statement-level trigger (a trigger without FOR EACH ROW)");
    } else {
        return not_supported("CREATE TRIGGER");
    };
    not_supported(format!("CREATE TRIGGER ... {clause}"))
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

/// An error that PostgreSQL also gives for the function definition.
fn invalid_function(message: impl Into<String>) -> ParseError {
    ParseError::Sqlstate {
        sqlstate: SQLSTATE_INVALID_FUNCTION_DEFINITION,
        message: message.into(),
    }
}
