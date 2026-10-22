use crate::ParseError;
use pg_query::protobuf::{
    self, node::Node, CreateFunctionStmt, DefElem, FunctionParameterMode, ResTarget, ScanToken,
    SelectStmt, SetOperation, Token, TypeName,
};
use pg_query::ParseResult;
use serde_json::Value;

const SUPPORTED_TYPES: &[&str] = &[
    "bool",
    "int2",
    "int4",
    "int8",
    "float4",
    "float8",
    "numeric",
    "text",
    "varchar",
    "bpchar",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "uuid",
    "json",
    "jsonb",
    "bytea",
];

#[derive(Debug)]
pub struct CreateFunction {
    pub replace: bool,
    pub schema: Option<String>,
    pub function: SqlFunction,
}

#[derive(Debug, Clone)]
pub struct SqlFunction {
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub return_type: SqlType,
    body: Vec<BodyPart>,
}

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: Option<String>,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlType {
    name: String,
    array_dimensions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BodyPart {
    Text(String),
    Parameter(usize),
}

impl CreateFunction {
    pub fn parse(sql: &str) -> Result<Self, ParseError> {
        let parse_result = crate::parse(sql)?;
        let Some(Node::CreateFunctionStmt(stmt)) = first_statement(&parse_result) else {
            return Err(error("expected a CREATE FUNCTION statement"));
        };
        if stmt.is_procedure {
            return Err(error("CREATE PROCEDURE is not supported"));
        }
        let (schema, name) = qualified_name(&stmt.funcname)?;
        let body = sql_body(stmt)?;
        let parameters = parameters(stmt)?;
        let return_type = return_type(stmt)?;
        let body = body_template(&body, &name, &parameters)?;
        Ok(Self {
            replace: stmt.replace,
            schema,
            function: SqlFunction {
                name,
                parameters,
                return_type,
                body,
            },
        })
    }
}

fn qualified_name(names: &[protobuf::Node]) -> Result<(Option<String>, String), ParseError> {
    match strings(names).as_slice() {
        [name] => Ok((None, name.clone())),
        [schema, name] => Ok((Some(schema.clone()), name.clone())),
        names => Err(error(format!(
            "improper qualified name (too many dotted names): {}",
            names.join(".")
        ))),
    }
}

fn sql_body(stmt: &CreateFunctionStmt) -> Result<String, ParseError> {
    if stmt.sql_body.is_some() {
        return Err(error(
            "function bodies written with RETURN or BEGIN ATOMIC are not supported yet",
        ));
    }
    let mut language = None;
    let mut body = None;
    let mut unsupported_option = None;
    for option in &stmt.options {
        let Some(Node::DefElem(option)) = &option.node else {
            unreachable!("CREATE FUNCTION options are DefElem nodes");
        };
        match option.defname.as_str() {
            "language" => language = def_elem_string(option),
            "as" => body = Some(option),
            "volatility" | "leakproof" | "parallel" | "cost" | "rows" => {}
            "strict" | "security" if !def_elem_bool(option) => {}
            _ => unsupported_option = unsupported_option.or(Some(option)),
        }
    }
    let Some(language) = language else {
        return Err(error("no language specified"));
    };
    if !language.eq_ignore_ascii_case("sql") {
        return Err(error(format!(
            "language \"{language}\" is not supported: only SQL functions can be inlined"
        )));
    }
    if let Some(option) = unsupported_option {
        return Err(unsupported_option_error(option));
    }
    let Some(body) = body else {
        return Err(error("no function body specified"));
    };
    let Some(Node::List(items)) = body.arg.as_deref().and_then(|arg| arg.node.as_ref()) else {
        unreachable!("the AS option of CREATE FUNCTION is a list");
    };
    match strings(&items.items).as_slice() {
        [body] => Ok(body.clone()),
        _ => Err(error("only one AS item needed for language \"sql\"")),
    }
}

fn unsupported_option_error(option: &DefElem) -> ParseError {
    match option.defname.as_str() {
        "strict" => error("STRICT SQL functions are not supported yet"),
        "security" => error("SECURITY DEFINER functions cannot be inlined"),
        "set" => error("SQL functions with a SET clause cannot be inlined"),
        name => error(format!(
            "CREATE FUNCTION option {} is not supported",
            name.to_uppercase()
        )),
    }
}

fn parameters(stmt: &CreateFunctionStmt) -> Result<Vec<Parameter>, ParseError> {
    let mut parameters: Vec<Parameter> = Vec::with_capacity(stmt.parameters.len());
    for parameter in &stmt.parameters {
        let Some(Node::FunctionParameter(parameter)) = &parameter.node else {
            unreachable!("CREATE FUNCTION parameters are FunctionParameter nodes");
        };
        match parameter.mode() {
            FunctionParameterMode::FuncParamIn | FunctionParameterMode::FuncParamDefault => {}
            FunctionParameterMode::FuncParamTable => return Err(set_returning_error()),
            _ => {
                return Err(error(
                    "OUT, INOUT, and VARIADIC parameters are not supported yet",
                ))
            }
        }
        if parameter.defexpr.is_some() {
            return Err(error("parameter defaults are not supported yet"));
        }
        let name = (!parameter.name.is_empty()).then(|| parameter.name.clone());
        if let Some(name) = &name {
            if parameters.iter().any(|p| p.name.as_ref() == Some(name)) {
                return Err(error(format!(
                    "parameter name \"{name}\" used more than once"
                )));
            }
        }
        let arg_type = parameter
            .arg_type
            .as_ref()
            .expect("CREATE FUNCTION parameters have a type");
        parameters.push(Parameter {
            name,
            sql_type: SqlType::from_type_name(arg_type)?,
        });
    }
    Ok(parameters)
}

fn return_type(stmt: &CreateFunctionStmt) -> Result<SqlType, ParseError> {
    let Some(return_type) = &stmt.return_type else {
        return Err(error("function result type must be specified"));
    };
    if return_type.setof {
        return Err(set_returning_error());
    }
    if strings(&return_type.names) == ["void"] {
        return Err(error("SQL functions that return void cannot be inlined"));
    }
    SqlType::from_type_name(return_type)
}

fn set_returning_error() -> ParseError {
    error("SQL functions that return SETOF or TABLE are not supported yet")
}

impl SqlType {
    fn from_type_name(type_name: &TypeName) -> Result<Self, ParseError> {
        let names = strings(&type_name.names);
        let name = match names.as_slice() {
            [name] => name,
            [schema, name] if schema == "pg_catalog" => name,
            _ => return Err(unsupported_type_error(&names.join("."))),
        };
        if type_name.pct_type || !SUPPORTED_TYPES.contains(&name.as_str()) {
            return Err(unsupported_type_error(name));
        }
        Ok(Self {
            name: name.clone(),
            array_dimensions: type_name.array_bounds.len(),
        })
    }
}

fn unsupported_type_error(name: &str) -> ParseError {
    error(format!(
        "type \"{name}\" is not supported in SQL functions yet"
    ))
}

fn body_template(
    body: &str,
    function: &str,
    parameters: &[Parameter],
) -> Result<Vec<BodyPart>, ParseError> {
    let parse_result = crate::parse(body)?;
    let target = single_select_target(&parse_result, function)?;
    let expression = target
        .val
        .as_deref()
        .expect("SELECT targets have an expression");
    let expression = serde_json::to_value(expression).expect("parse trees serialize to JSON");
    if contains_node(&expression, "SubLink") {
        return Err(cannot_inline_error(
            function,
            "its body contains a subquery",
        ));
    }
    let tokens = Tokens::scan(body)?;
    let mut references = Vec::new();
    collect_parameter_references(&expression, function, parameters, &tokens, &mut references)?;
    references.sort_by_key(|reference| reference.start);
    let mut parts = Vec::with_capacity(2 * references.len() + 1);
    let mut cursor = byte_offset(target.location);
    for reference in references {
        push_text(&mut parts, &body[cursor..reference.start]);
        parts.push(BodyPart::Parameter(reference.index));
        cursor = reference.end;
    }
    push_text(&mut parts, &body[cursor..tokens.expression_end(target)]);
    Ok(parts)
}

fn single_select_target<'a>(
    parse_result: &'a ParseResult,
    function: &str,
) -> Result<&'a ResTarget, ParseError> {
    let select = match (
        parse_result.protobuf.stmts.len(),
        first_statement(parse_result),
    ) {
        (1, Some(Node::SelectStmt(select))) => select,
        _ => {
            return Err(cannot_inline_error(
                function,
                "its body must be a single SELECT",
            ))
        }
    };
    if !is_plain_select(select) {
        return Err(cannot_inline_error(
            function,
            "its body must be a SELECT of one expression without FROM or any other clause",
        ));
    }
    match select.target_list.as_slice() {
        [target] => match &target.node {
            Some(Node::ResTarget(target)) => Ok(target),
            _ => unreachable!("SELECT targets are ResTarget nodes"),
        },
        _ => Err(error(format!(
            "SQL function \"{function}\" must return exactly one column"
        ))),
    }
}

fn is_plain_select(select: &SelectStmt) -> bool {
    select.op() == SetOperation::SetopNone
        && select.distinct_clause.is_empty()
        && select.into_clause.is_none()
        && select.from_clause.is_empty()
        && select.where_clause.is_none()
        && select.group_clause.is_empty()
        && select.having_clause.is_none()
        && select.window_clause.is_empty()
        && select.values_lists.is_empty()
        && select.sort_clause.is_empty()
        && select.limit_offset.is_none()
        && select.limit_count.is_none()
        && select.locking_clause.is_empty()
        && select.with_clause.is_none()
}

struct ParameterReference {
    start: usize,
    end: usize,
    index: usize,
}

fn collect_parameter_references(
    value: &Value,
    function: &str,
    parameters: &[Parameter],
    tokens: &Tokens,
    references: &mut Vec<ParameterReference>,
) -> Result<(), ParseError> {
    match value {
        Value::Object(fields) => {
            if let Some(column) = fields.get("ColumnRef") {
                references.push(named_parameter_reference(
                    column, function, parameters, tokens,
                )?);
            } else if let Some(parameter) = fields.get("ParamRef") {
                references.push(positional_parameter_reference(
                    parameter, parameters, tokens,
                )?);
            } else {
                for child in fields.values() {
                    collect_parameter_references(child, function, parameters, tokens, references)?;
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_parameter_references(item, function, parameters, tokens, references)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn named_parameter_reference(
    column: &Value,
    function: &str,
    parameters: &[Parameter],
    tokens: &Tokens,
) -> Result<ParameterReference, ParseError> {
    let fields: Vec<Option<&str>> = column["fields"]
        .as_array()
        .expect("ColumnRef has fields")
        .iter()
        .map(|field| field.pointer("/node/String/sval").and_then(Value::as_str))
        .collect();
    let parameter_index = |name: &str| {
        parameters
            .iter()
            .position(|parameter| parameter.name.as_deref() == Some(name))
    };
    let (index, token_count) = match fields.as_slice() {
        [Some(name)] => match parameter_index(name) {
            Some(index) => (index, 1),
            None => return Err(error(format!("column \"{name}\" does not exist"))),
        },
        [Some(qualifier), Some(name)] if *qualifier == function => match parameter_index(name) {
            Some(index) => (index, 3),
            None => return Err(error(format!("column {qualifier}.{name} does not exist"))),
        },
        [Some(qualifier), Some(_)] => {
            return Err(error(format!(
                "missing FROM-clause entry for table \"{qualifier}\""
            )))
        }
        [None] => return Err(error("SELECT * with no tables specified is not valid")),
        _ => {
            return Err(cannot_inline_error(
                function,
                "its body refers to a column of a table",
            ))
        }
    };
    let (start, end) = tokens.span(json_location(column), token_count);
    Ok(ParameterReference { start, end, index })
}

fn positional_parameter_reference(
    parameter: &Value,
    parameters: &[Parameter],
    tokens: &Tokens,
) -> Result<ParameterReference, ParseError> {
    let number = parameter["number"].as_i64().expect("ParamRef has a number");
    let index = usize::try_from(number)
        .ok()
        .and_then(|number| number.checked_sub(1))
        .filter(|index| *index < parameters.len())
        .ok_or_else(|| error(format!("there is no parameter ${number}")))?;
    let (start, end) = tokens.span(json_location(parameter), 1);
    Ok(ParameterReference { start, end, index })
}

fn push_text(parts: &mut Vec<BodyPart>, text: &str) {
    if !text.is_empty() {
        parts.push(BodyPart::Text(text.to_string()));
    }
}

impl SqlFunction {
    pub fn instantiate(&self, arguments: &[String]) -> String {
        assert_eq!(
            arguments.len(),
            self.parameters.len(),
            "a call of {} passes one argument for each parameter",
            self.name
        );
        let mut body = String::new();
        for part in &self.body {
            match part {
                BodyPart::Text(text) => body.push_str(text),
                BodyPart::Parameter(index) => body.push_str(&format!(
                    "(CAST(({}) AS {}))",
                    arguments[*index],
                    self.parameters[*index].sql_type.to_sql()
                )),
            }
        }
        format!("(CAST(({body}) AS {}))", self.return_type.to_sql())
    }
}

impl SqlType {
    fn to_sql(&self) -> String {
        format!(
            "pg_catalog.{}{}",
            self.name,
            "[]".repeat(self.array_dimensions)
        )
    }
}

struct Tokens(Vec<ScanToken>);

impl Tokens {
    fn scan(sql: &str) -> Result<Self, ParseError> {
        let scan = pg_query::scan(sql).map_err(|e| error(e.to_string()))?;
        Ok(Self(
            scan.tokens
                .into_iter()
                .filter(|token| !matches!(token.token(), Token::SqlComment | Token::CComment))
                .collect(),
        ))
    }

    fn expression_end(&self, target: &ResTarget) -> usize {
        let last = self
            .0
            .iter()
            .rposition(|token| token.token() != Token::Ascii59)
            .expect("a SELECT has tokens");
        let last_of_expression = match (target.name.is_empty(), self.0[last - 1].token()) {
            (true, _) => last,
            (false, Token::As) => last - 2,
            (false, _) => last - 1,
        };
        byte_offset(self.0[last_of_expression].end)
    }

    fn span(&self, start: usize, token_count: usize) -> (usize, usize) {
        let first = self.index_at(start);
        (start, byte_offset(self.0[first + token_count - 1].end))
    }

    fn index_at(&self, start: usize) -> usize {
        self.0
            .binary_search_by_key(&start, |token| byte_offset(token.start))
            .expect("parse tree locations are token starts")
    }
}

fn first_statement(parse_result: &ParseResult) -> Option<&Node> {
    parse_result
        .protobuf
        .stmts
        .first()?
        .stmt
        .as_ref()?
        .node
        .as_ref()
}

fn contains_node(value: &Value, kind: &str) -> bool {
    match value {
        Value::Object(fields) => {
            fields.contains_key(kind) || fields.values().any(|child| contains_node(child, kind))
        }
        Value::Array(items) => items.iter().any(|item| contains_node(item, kind)),
        _ => false,
    }
}

fn json_location(node: &Value) -> usize {
    let location = node["location"].as_i64().expect("node has a location");
    location.try_into().expect("node location is known")
}

fn byte_offset(offset: i32) -> usize {
    usize::try_from(offset).expect("location is known")
}

fn strings(nodes: &[protobuf::Node]) -> Vec<String> {
    nodes
        .iter()
        .filter_map(|node| match &node.node {
            Some(Node::String(s)) => Some(s.sval.clone()),
            _ => None,
        })
        .collect()
}

fn def_elem_string(option: &DefElem) -> Option<String> {
    match option.arg.as_deref().and_then(|arg| arg.node.as_ref()) {
        Some(Node::String(s)) => Some(s.sval.clone()),
        _ => None,
    }
}

fn def_elem_bool(option: &DefElem) -> bool {
    matches!(
        option.arg.as_deref().and_then(|arg| arg.node.as_ref()),
        Some(Node::Boolean(b)) if b.boolval
    )
}

fn cannot_inline_error(function: &str, reason: &str) -> ParseError {
    error(format!(
        "SQL function \"{function}\" cannot be inlined: {reason}"
    ))
}

fn error(message: impl Into<String>) -> ParseError {
    ParseError::ParseError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> CreateFunction {
        CreateFunction::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn arguments(arguments: &[&str]) -> Vec<String> {
        arguments
            .iter()
            .map(|argument| argument.to_string())
            .collect()
    }

    #[test]
    fn instantiate_casts_arguments_and_result() {
        let create = parse(
            "CREATE FUNCTION add_one(a integer) RETURNS integer LANGUAGE sql AS 'SELECT a + 1'",
        );
        assert!(!create.replace);
        assert_eq!(create.schema, None);
        let function = create.function;
        assert_eq!(function.name, "add_one");
        assert_eq!(function.parameters.len(), 1);
        assert_eq!(function.parameters[0].name.as_deref(), Some("a"));
        assert_eq!(
            function.instantiate(&arguments(&["x"])),
            "(CAST(((CAST((x) AS pg_catalog.int4)) + 1) AS pg_catalog.int4))"
        );
    }

    #[test]
    fn instantiate_replaces_positional_and_qualified_parameters() {
        let create = parse(
            "CREATE OR REPLACE FUNCTION public.f(a int, int8) RETURNS bigint LANGUAGE sql \
             AS $$ SELECT $2 * f.a - a $$",
        );
        assert!(create.replace);
        assert_eq!(create.schema.as_deref(), Some("public"));
        assert_eq!(
            create.function.instantiate(&arguments(&["x", "y"])),
            "(CAST(((CAST((y) AS pg_catalog.int8)) * (CAST((x) AS pg_catalog.int4)) \
             - (CAST((x) AS pg_catalog.int4))) AS pg_catalog.int8))"
        );
    }

    #[test]
    fn instantiate_leaves_out_alias_semicolon_and_comments_around_the_body() {
        let function = parse(
            "CREATE FUNCTION f(a text) RETURNS text LANGUAGE sql \
             AS $$ /* start */ SELECT lower(a) -- one\n || 'x' AS result; -- end\n $$",
        )
        .function;
        assert_eq!(
            function.instantiate(&arguments(&["y"])),
            "(CAST((lower((CAST((y) AS pg_catalog.text))) -- one\n || 'x') AS pg_catalog.text))"
        );
        let function =
            parse("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1 one'").function;
        assert_eq!(function.instantiate(&[]), "(CAST((1) AS pg_catalog.int4))");
    }

    #[test]
    fn instantiate_ignores_type_modifiers_and_keeps_array_dimensions() {
        let function = parse(
            "CREATE FUNCTION f(a varchar(10), b numeric(5, 2)[]) RETURNS double precision \
             LANGUAGE sql IMMUTABLE PARALLEL SAFE AS 'SELECT b[1] + length(a)'",
        )
        .function;
        assert_eq!(
            function.instantiate(&arguments(&["p", "q"])),
            "(CAST(((CAST((q) AS pg_catalog.numeric[]))[1] \
             + length((CAST((p) AS pg_catalog.varchar)))) AS pg_catalog.float8))"
        );
    }

    #[test]
    fn definitions_that_cannot_be_inlined_are_rejected() {
        for (sql, message) in [
            (
                "CREATE PROCEDURE p() LANGUAGE sql AS 'SELECT 1'",
                "CREATE PROCEDURE is not supported",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN 1; END'",
                "language \"plpgsql\" is not supported",
            ),
            (
                "CREATE FUNCTION f() RETURNS int AS 'SELECT 1'",
                "no language specified",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql RETURN 1",
                "RETURN or BEGIN ATOMIC are not supported yet",
            ),
            (
                "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS 'UPDATE t SET x = a'",
                "its body must be a single SELECT",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1; SELECT 2'",
                "its body must be a single SELECT",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS ''",
                "its body must be a single SELECT",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT x FROM t'",
                "without FROM or any other clause",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1 UNION SELECT 2'",
                "without FROM or any other clause",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT (SELECT 1)'",
                "its body contains a subquery",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1, 2'",
                "must return exactly one column",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT nosuch'",
                "column \"nosuch\" does not exist",
            ),
            (
                "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS 'SELECT t.a'",
                "missing FROM-clause entry for table \"t\"",
            ),
            (
                "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS 'SELECT $2'",
                "there is no parameter $2",
            ),
            (
                "CREATE FUNCTION f() RETURNS SETOF int LANGUAGE sql AS 'SELECT 1'",
                "SETOF or TABLE are not supported yet",
            ),
            (
                "CREATE FUNCTION f() RETURNS TABLE (x int) LANGUAGE sql AS 'SELECT 1'",
                "SETOF or TABLE are not supported yet",
            ),
            (
                "CREATE FUNCTION f(OUT x int) LANGUAGE sql AS 'SELECT 1'",
                "OUT, INOUT, and VARIADIC parameters are not supported yet",
            ),
            (
                "CREATE FUNCTION f(VARIADIC x int[]) RETURNS int LANGUAGE sql AS 'SELECT 1'",
                "OUT, INOUT, and VARIADIC parameters are not supported yet",
            ),
            (
                "CREATE FUNCTION f(a int DEFAULT 1) RETURNS int LANGUAGE sql AS 'SELECT a'",
                "parameter defaults are not supported yet",
            ),
            (
                "CREATE FUNCTION f(a int, a int) RETURNS int LANGUAGE sql AS 'SELECT 1'",
                "parameter name \"a\" used more than once",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql STRICT AS 'SELECT 1'",
                "STRICT SQL functions are not supported yet",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'",
                "SECURITY DEFINER functions cannot be inlined",
            ),
            (
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET search_path = public \
                 AS 'SELECT 1'",
                "SET clause cannot be inlined",
            ),
            (
                "CREATE FUNCTION f() RETURNS void LANGUAGE sql AS 'SELECT 1'",
                "return void cannot be inlined",
            ),
            (
                "CREATE FUNCTION f() RETURNS record LANGUAGE sql AS 'SELECT 1'",
                "type \"record\" is not supported in SQL functions yet",
            ),
            (
                "CREATE FUNCTION f(a public.mood) RETURNS int LANGUAGE sql AS 'SELECT 1'",
                "type \"public.mood\" is not supported in SQL functions yet",
            ),
            (
                "CREATE FUNCTION a.b.c() RETURNS int LANGUAGE sql AS 'SELECT 1'",
                "improper qualified name",
            ),
        ] {
            let Err(ParseError::ParseError(e)) = CreateFunction::parse(sql) else {
                panic!("{sql} must be rejected");
            };
            assert!(e.contains(message), "{sql}: expected {message}, got {e}");
        }
    }
}
