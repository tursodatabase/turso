//! The parts of a `CREATE FUNCTION` statement that the frontend reads.
//!
//! The engine stores the statement text as written. This module parses the
//! text again when the frontend needs the function: for the catalog, to
//! inline a call, or to compile a trigger body.

use crate::translator::extract_type_name_from_typename;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self, FunctionParameterMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgFunctionParam {
    pub name: Option<String>,
    /// The type as PostgreSQL spells it, such as `int4`, `numeric` or `text`.
    pub type_name: String,
}

/// A function that `CREATE FUNCTION` declares.
#[derive(Debug, Clone)]
pub struct PgFunction {
    pub schema: Option<String>,
    pub name: String,
    pub or_replace: bool,
    /// The input parameters: `IN`, `INOUT`, `VARIADIC` and parameters with
    /// no mode.
    pub params: Vec<PgFunctionParam>,
    pub return_type: Option<String>,
    pub returns_set: bool,
    /// The language in lowercase: `sql`, `plpgsql`, ...
    pub language: String,
    /// The string of the `AS` clause.
    pub body: Option<String>,
    /// A body in the SQL standard form: `BEGIN ATOMIC ... END` or `RETURN expr`.
    pub sql_body: Option<Box<protobuf::Node>>,
    pub strict: bool,
    pub volatility: Volatility,
    pub security_definer: bool,
}

impl PgFunction {
    /// Parse the text of a `CREATE FUNCTION` statement.
    pub fn parse(sql: &str) -> Result<PgFunction, ParseError> {
        let parse_result = crate::parse(sql)?;
        let node = parse_result
            .protobuf
            .nodes()
            .into_iter()
            .next()
            .ok_or_else(|| ParseError::ParseError("No statements found".to_string()))?;
        match node.0 {
            pg_query::NodeRef::CreateFunctionStmt(stmt) => PgFunction::from_stmt(stmt),
            _ => Err(ParseError::ParseError(
                "expected a CREATE FUNCTION statement".to_string(),
            )),
        }
    }

    pub fn from_stmt(stmt: &protobuf::CreateFunctionStmt) -> Result<PgFunction, ParseError> {
        if stmt.is_procedure {
            return Err(ParseError::ParseError(
                "CREATE PROCEDURE is not supported".to_string(),
            ));
        }
        let names: Vec<&str> = stmt
            .funcname
            .iter()
            .filter_map(|node| match &node.node {
                Some(Node::String(s)) => Some(s.sval.as_str()),
                _ => None,
            })
            .collect();
        let (schema, name) = match names.as_slice() {
            [name] => (None, (*name).to_string()),
            [schema, name] => (Some((*schema).to_string()), (*name).to_string()),
            _ => {
                return Err(ParseError::ParseError(
                    "CREATE FUNCTION missing function name".to_string(),
                ))
            }
        };

        let mut params = Vec::new();
        for node in &stmt.parameters {
            let Some(Node::FunctionParameter(param)) = &node.node else {
                continue;
            };
            let mode = FunctionParameterMode::try_from(param.mode)
                .unwrap_or(FunctionParameterMode::Undefined);
            if matches!(
                mode,
                FunctionParameterMode::FuncParamOut | FunctionParameterMode::FuncParamTable
            ) {
                continue;
            }
            let type_name = match &param.arg_type {
                Some(type_name) => extract_type_name_from_typename(type_name)?,
                None => String::new(),
            };
            params.push(PgFunctionParam {
                name: (!param.name.is_empty()).then(|| param.name.clone()),
                type_name,
            });
        }

        let (return_type, returns_set) = match &stmt.return_type {
            Some(type_name) => (
                Some(extract_type_name_from_typename(type_name)?),
                type_name.setof,
            ),
            None => (None, false),
        };

        let mut language = None;
        let mut body = None;
        let mut strict = false;
        let mut volatility = Volatility::Volatile;
        let mut security_definer = false;
        for node in &stmt.options {
            let Some(Node::DefElem(option)) = &node.node else {
                continue;
            };
            match option.defname.as_str() {
                "language" => language = def_elem_string(option).map(|s| s.to_ascii_lowercase()),
                "as" => body = def_elem_first_string(option),
                "strict" => strict = def_elem_bool(option),
                "volatility" => {
                    volatility = match def_elem_string(option).as_deref() {
                        Some("immutable") => Volatility::Immutable,
                        Some("stable") => Volatility::Stable,
                        _ => Volatility::Volatile,
                    }
                }
                "security" => security_definer = def_elem_bool(option),
                _ => {}
            }
        }
        let sql_body = stmt.sql_body.clone();
        let language = match language {
            Some(language) => language,
            None if sql_body.is_some() => "sql".to_string(),
            None => return Err(ParseError::ParseError("no language specified".to_string())),
        };

        Ok(PgFunction {
            schema,
            name,
            or_replace: stmt.replace,
            params,
            return_type,
            returns_set,
            language,
            body,
            sql_body,
            strict,
            volatility,
            security_definer,
        })
    }

    pub fn is_trigger(&self) -> bool {
        self.return_type
            .as_deref()
            .is_some_and(|return_type| return_type.eq_ignore_ascii_case("trigger"))
    }
}

fn def_elem_string(option: &protobuf::DefElem) -> Option<String> {
    match option.arg.as_deref().and_then(|arg| arg.node.as_ref()) {
        Some(Node::String(s)) => Some(s.sval.clone()),
        _ => None,
    }
}

/// The first string of a list option, such as the body of `AS 'body'`.
fn def_elem_first_string(option: &protobuf::DefElem) -> Option<String> {
    match option.arg.as_deref().and_then(|arg| arg.node.as_ref()) {
        Some(Node::List(list)) => list.items.iter().find_map(|item| match &item.node {
            Some(Node::String(s)) => Some(s.sval.clone()),
            _ => None,
        }),
        Some(Node::String(s)) => Some(s.sval.clone()),
        _ => None,
    }
}

fn def_elem_bool(option: &protobuf::DefElem) -> bool {
    match option.arg.as_deref().and_then(|arg| arg.node.as_ref()) {
        Some(Node::Boolean(b)) => b.boolval,
        Some(Node::Integer(i)) => i.ival != 0,
        _ => false,
    }
}

/// The name of a type as PostgreSQL writes it in messages: `integer` for
/// `int4`, `character varying` for `varchar`, and so on.
pub fn display_type_name(type_name: &str) -> String {
    let (base, suffix) = match type_name.find("[]") {
        Some(index) => type_name.split_at(index),
        None => (type_name, ""),
    };
    let base = match base.to_ascii_lowercase().as_str() {
        "int4" | "int" => "integer".to_string(),
        "int8" => "bigint".to_string(),
        "int2" => "smallint".to_string(),
        "bool" => "boolean".to_string(),
        "float8" => "double precision".to_string(),
        "float4" => "real".to_string(),
        "varchar" => "character varying".to_string(),
        "bpchar" => "character".to_string(),
        "timestamp" => "timestamp without time zone".to_string(),
        "timestamptz" => "timestamp with time zone".to_string(),
        "time" => "time without time zone".to_string(),
        "timetz" => "time with time zone".to_string(),
        other => other.to_string(),
    };
    format!("{base}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_sql_function() {
        let function = PgFunction::parse(
            "CREATE OR REPLACE FUNCTION public.add_tax(price numeric, OUT total numeric) \
             RETURNS numeric LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT price * 1.2'",
        )
        .unwrap();
        assert_eq!(function.schema.as_deref(), Some("public"));
        assert_eq!(function.name, "add_tax");
        assert!(function.or_replace);
        assert_eq!(
            function.params,
            vec![PgFunctionParam {
                name: Some("price".to_string()),
                type_name: "numeric".to_string(),
            }]
        );
        assert_eq!(function.return_type.as_deref(), Some("numeric"));
        assert!(!function.returns_set);
        assert_eq!(function.language, "sql");
        assert_eq!(function.body.as_deref(), Some("SELECT price * 1.2"));
        assert!(function.sql_body.is_none());
        assert!(function.strict);
        assert_eq!(function.volatility, Volatility::Immutable);
        assert!(!function.security_definer);
        assert!(!function.is_trigger());
    }

    #[test]
    fn parses_a_trigger_function() {
        let function = PgFunction::parse(
            "CREATE FUNCTION handle_new_user() RETURNS trigger \
             LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' \
             AS $$ BEGIN RETURN NEW; END $$",
        )
        .unwrap();
        assert!(function.params.is_empty());
        assert!(function.is_trigger());
        assert_eq!(function.language, "plpgsql");
        assert!(function.security_definer);
        assert_eq!(function.volatility, Volatility::Volatile);
        assert!(!function.strict);
    }

    #[test]
    fn a_sql_standard_body_implies_the_sql_language() {
        let function = PgFunction::parse("CREATE FUNCTION two() RETURNS int RETURN 1 + 1").unwrap();
        assert_eq!(function.language, "sql");
        assert!(function.body.is_none());
        assert!(function.sql_body.is_some());
    }

    #[test]
    fn rejects_a_procedure() {
        let err = PgFunction::parse("CREATE PROCEDURE p() LANGUAGE SQL AS 'SELECT 1'").unwrap_err();
        assert!(err
            .to_string()
            .contains("CREATE PROCEDURE is not supported"));
    }

    #[test]
    fn display_type_names_match_postgresql_messages() {
        assert_eq!(display_type_name("int4"), "integer");
        assert_eq!(display_type_name("int8[]"), "bigint[]");
        assert_eq!(display_type_name("varchar"), "character varying");
        assert_eq!(display_type_name("numeric"), "numeric");
        assert_eq!(display_type_name("my_type"), "my_type");
    }
}
