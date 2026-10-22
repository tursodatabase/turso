use crate::ParseError;
use pg_query::protobuf::{
    self, node::Node, CoercionForm, CreateFunctionStmt, DefElem, FunctionParameterMode,
    KeywordKind, ResTarget, ScanToken, SelectStmt, SetOperation, Token, TypeName,
};
use pg_query::ParseResult;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const EXPRESSION_PREFIX: &str = "SELECT ";
const MAX_INLINING_DEPTH: usize = 64;
const MAX_INLINED_SQL_LENGTH: usize = 16 * 1024 * 1024;

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

pub type FunctionLookup = Result<Option<Arc<SqlFunction>>, ParseError>;

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
    body: Body,
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

#[derive(Debug, Clone)]
struct Body {
    parts: Vec<BodyPart>,
    calls_functions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BodyPart {
    Text(String),
    Parameter(usize),
}

pub fn is_create_function(parse_result: &ParseResult) -> bool {
    matches!(
        first_statement(parse_result),
        Some(Node::CreateFunctionStmt(_))
    )
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

fn body_template(body: &str, function: &str, parameters: &[Parameter]) -> Result<Body, ParseError> {
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
    Ok(Body {
        parts,
        calls_functions: contains_node(&expression, "FuncCall"),
    })
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

pub fn inline_calls(
    sql: &str,
    parse_result: &ParseResult,
    lookup: &mut dyn FnMut(&str) -> FunctionLookup,
) -> Result<Option<String>, ParseError> {
    let Some(statement) = first_statement(parse_result) else {
        return Ok(None);
    };
    let mut inliner = Inliner {
        lookup,
        functions: HashMap::new(),
        calling: Vec::new(),
    };
    if !inliner.calls_known_function(sql)? {
        return Ok(None);
    }
    let calls = inliner.resolve_calls(statement, None)?;
    let Some(call) = calls.first() else {
        return Ok(None);
    };
    if !matches!(
        statement,
        Node::SelectStmt(_)
            | Node::InsertStmt(_)
            | Node::UpdateStmt(_)
            | Node::DeleteStmt(_)
            | Node::ExplainStmt(_)
    ) {
        return Err(error(format!(
            "SQL function \"{}\" can be called only in SELECT, INSERT, UPDATE, DELETE, and EXPLAIN statements",
            call.function.name
        )));
    }
    inliner.splice(sql, &calls).map(Some)
}

struct Inliner<'a> {
    lookup: &'a mut dyn FnMut(&str) -> FunctionLookup,
    functions: HashMap<String, Option<Arc<SqlFunction>>>,
    calling: Vec<String>,
}

struct ResolvedCall {
    location: usize,
    function: Arc<SqlFunction>,
    is_select_item: bool,
}

impl Inliner<'_> {
    fn calls_known_function(&mut self, sql: &str) -> Result<bool, ParseError> {
        for name in Tokens::scan(sql)?.called_names(sql) {
            if self.function_named(&name)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn resolve_calls(
        &mut self,
        statement: &Node,
        wrapper_location: Option<usize>,
    ) -> Result<Vec<ResolvedCall>, ParseError> {
        let statement = serde_json::to_value(statement).expect("parse trees serialize to JSON");
        let mut sites = CallSites::default();
        sites.collect(&statement);
        let mut calls = Vec::new();
        for site in &sites.calls {
            let Some(function) = self.function(site)? else {
                continue;
            };
            if let Some(syntax) = site.unsupported_syntax {
                return Err(error(format!(
                    "{syntax} is not supported in calls of SQL function \"{}\"",
                    function.name
                )));
            }
            if sites.from_clause_calls.contains(&site.location) {
                return Err(error(format!(
                    "SQL function \"{}\" cannot be called in FROM yet",
                    function.name
                )));
            }
            calls.push(ResolvedCall {
                location: site.location,
                is_select_item: sites.select_items.contains(&site.location)
                    && Some(site.location) != wrapper_location,
                function,
            });
        }
        calls.sort_by_key(|call| call.location);
        Ok(calls)
    }

    fn function(&mut self, site: &CallSite) -> Result<Option<Arc<SqlFunction>>, ParseError> {
        let name = match site.names.as_slice() {
            [name] => name,
            [schema, name] if schema == "public" => name,
            _ => return Ok(None),
        };
        if !site.is_explicit_call {
            return Ok(None);
        }
        let function = self.function_named(name)?;
        Ok(function.filter(|function| function.parameters.len() == site.argument_count))
    }

    fn function_named(&mut self, name: &str) -> FunctionLookup {
        if let Some(function) = self.functions.get(name) {
            return Ok(function.clone());
        }
        let function = (self.lookup)(name)?;
        self.functions.insert(name.to_string(), function.clone());
        Ok(function)
    }

    fn splice(&mut self, sql: &str, calls: &[ResolvedCall]) -> Result<String, ParseError> {
        let tokens = Tokens::scan(sql)?;
        let mut inlined = String::with_capacity(sql.len());
        let mut cursor = 0;
        for call in calls {
            let span = tokens.call_span(call.location);
            if span.start < cursor {
                continue;
            }
            inlined.push_str(&sql[cursor..span.start]);
            let arguments = span
                .arguments
                .iter()
                .map(|&(start, end)| &sql[start..end])
                .collect();
            inlined.push_str(&self.expand(&call.function, arguments)?);
            if call.is_select_item {
                inlined.push_str(" AS ");
                inlined.push_str(&crate::quote_identifier(&call.function.name));
            }
            cursor = span.end;
        }
        inlined.push_str(&sql[cursor..]);
        if inlined.len() > MAX_INLINED_SQL_LENGTH {
            return Err(error(format!(
                "inlining SQL functions makes the statement longer than {MAX_INLINED_SQL_LENGTH} bytes"
            )));
        }
        Ok(inlined)
    }

    fn expand(
        &mut self,
        function: &SqlFunction,
        arguments: Vec<&str>,
    ) -> Result<String, ParseError> {
        if self.calling.contains(&function.name) {
            return Err(error(format!(
                "cannot inline recursive SQL function \"{}\"",
                function.name
            )));
        }
        if self.calling.len() == MAX_INLINING_DEPTH {
            return Err(error(format!(
                "SQL functions are nested more than {MAX_INLINING_DEPTH} levels deep"
            )));
        }
        let arguments = arguments
            .into_iter()
            .map(|argument| self.rewrite_expression(argument))
            .collect::<Result<Vec<_>, _>>()?;
        for (index, argument) in arguments.iter().enumerate() {
            if function.parameter_uses(index) > 1 && !can_be_repeated(argument)? {
                return Err(error(format!(
                    "cannot inline SQL function \"{}\": argument {} is used more than once, so it cannot contain function calls or subqueries",
                    function.name,
                    index + 1
                )));
            }
        }
        let body = function.instantiate(&arguments);
        if !function.body.calls_functions {
            return Ok(body);
        }
        self.calling.push(function.name.clone());
        let body = self.rewrite_expression(&body);
        self.calling.pop();
        body
    }

    fn rewrite_expression(&mut self, expression: &str) -> Result<String, ParseError> {
        if !self.calls_known_function(expression)? {
            return Ok(expression.to_string());
        }
        let select = format!("{EXPRESSION_PREFIX}{expression}");
        let parse_result = crate::parse(&select)?;
        let statement = first_statement(&parse_result).expect("a SELECT is a statement");
        let calls = self.resolve_calls(statement, Some(EXPRESSION_PREFIX.len()))?;
        if calls.is_empty() {
            return Ok(expression.to_string());
        }
        let select = self.splice(&select, &calls)?;
        Ok(select[EXPRESSION_PREFIX.len()..].to_string())
    }
}

impl SqlFunction {
    fn instantiate(&self, arguments: &[String]) -> String {
        assert_eq!(
            arguments.len(),
            self.parameters.len(),
            "a call of {} passes one argument for each parameter",
            self.name
        );
        let mut body = String::new();
        for part in &self.body.parts {
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

    fn parameter_uses(&self, index: usize) -> usize {
        self.body
            .parts
            .iter()
            .filter(|part| **part == BodyPart::Parameter(index))
            .count()
    }

    pub fn call_with_null_arguments(&self) -> String {
        let arguments: Vec<String> = self
            .parameters
            .iter()
            .map(|parameter| format!("CAST(NULL AS {})", parameter.sql_type.to_sql()))
            .collect();
        format!(
            "{}({})",
            crate::quote_identifier(&self.name),
            arguments.join(", ")
        )
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

fn can_be_repeated(expression: &str) -> Result<bool, ParseError> {
    let parse_result = crate::parse(&format!("{EXPRESSION_PREFIX}{expression}"))?;
    let statement = serde_json::to_value(first_statement(&parse_result))
        .expect("parse trees serialize to JSON");
    Ok(!contains_node(&statement, "FuncCall") && !contains_node(&statement, "SubLink"))
}

#[derive(Default)]
struct CallSites {
    calls: Vec<CallSite>,
    from_clause_calls: HashSet<usize>,
    select_items: HashSet<usize>,
}

struct CallSite {
    names: Vec<String>,
    location: usize,
    argument_count: usize,
    is_explicit_call: bool,
    unsupported_syntax: Option<&'static str>,
}

impl CallSites {
    fn collect(&mut self, value: &Value) {
        match value {
            Value::Object(fields) => {
                if let Some(call) = fields.get("FuncCall") {
                    self.calls.push(CallSite::new(call));
                }
                if let Some(range_function) = fields.get("RangeFunction") {
                    let functions = range_function["functions"]
                        .as_array()
                        .expect("RangeFunction has functions");
                    self.from_clause_calls.extend(
                        functions
                            .iter()
                            .filter_map(|function| {
                                function.pointer("/node/List/items/0/node/FuncCall")
                            })
                            .map(json_location),
                    );
                }
                if let Some(target) = fields.get("ResTarget") {
                    if target["name"] == ""
                        && target.pointer("/val/node/FuncCall/location")
                            == Some(&target["location"])
                    {
                        self.select_items.insert(json_location(target));
                    }
                }
                for child in fields.values() {
                    self.collect(child);
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.collect(item);
                }
            }
            _ => {}
        }
    }
}

impl CallSite {
    fn new(call: &Value) -> Self {
        let arguments = call["args"].as_array().expect("FuncCall has arguments");
        let unsupported_syntax = if call["agg_star"] == true {
            Some("*")
        } else if call["agg_distinct"] == true {
            Some("DISTINCT")
        } else if call["agg_within_group"] == true {
            Some("WITHIN GROUP")
        } else if call["agg_order"]
            .as_array()
            .is_some_and(|order| !order.is_empty())
        {
            Some("ORDER BY")
        } else if !call["agg_filter"].is_null() {
            Some("FILTER")
        } else if !call["over"].is_null() {
            Some("OVER")
        } else if call["func_variadic"] == true {
            Some("VARIADIC")
        } else if arguments
            .iter()
            .any(|argument| argument.pointer("/node/NamedArgExpr").is_some())
        {
            Some("Named argument notation")
        } else {
            None
        };
        Self {
            names: call["funcname"]
                .as_array()
                .expect("FuncCall has a name")
                .iter()
                .filter_map(|name| name.pointer("/node/String/sval").and_then(Value::as_str))
                .map(str::to_string)
                .collect(),
            location: json_location(call),
            argument_count: arguments.len(),
            is_explicit_call: call["funcformat"] == CoercionForm::CoerceExplicitCall as i32,
            unsupported_syntax,
        }
    }
}

struct CallSpan {
    start: usize,
    end: usize,
    arguments: Vec<(usize, usize)>,
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

    fn call_span(&self, start: usize) -> CallSpan {
        let open = (self.index_at(start)..self.0.len())
            .find(|&index| self.0[index].token() == Token::Ascii40)
            .expect("a call has an opening parenthesis");
        let mut depth = 0usize;
        let mut arguments = Vec::new();
        let mut argument_start = None;
        for index in open + 1..self.0.len() {
            let token = self.0[index].token();
            if depth == 0 && matches!(token, Token::Ascii41 | Token::Ascii44) {
                if let Some(argument_start) = argument_start.take() {
                    arguments.push((argument_start, byte_offset(self.0[index - 1].end)));
                }
                if token == Token::Ascii41 {
                    return CallSpan {
                        start,
                        end: byte_offset(self.0[index].end),
                        arguments,
                    };
                }
                continue;
            }
            match token {
                Token::Ascii40 | Token::Ascii91 => depth += 1,
                Token::Ascii41 | Token::Ascii93 => depth -= 1,
                _ => {}
            }
            argument_start.get_or_insert(byte_offset(self.0[index].start));
        }
        unreachable!("a call has a closing parenthesis")
    }

    fn called_names(&self, sql: &str) -> Vec<String> {
        self.0
            .windows(2)
            .filter(|pair| pair[1].token() == Token::Ascii40)
            .filter(|pair| {
                pair[0].token() == Token::Ident || pair[0].keyword_kind() != KeywordKind::NoKeyword
            })
            .map(|pair| {
                let name = &sql[byte_offset(pair[0].start)..byte_offset(pair[0].end)];
                match name
                    .strip_prefix('"')
                    .and_then(|name| name.strip_suffix('"'))
                {
                    Some(quoted) => quoted.replace("\"\"", "\""),
                    None => name.to_ascii_lowercase(),
                }
            })
            .collect()
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

    const ADD_ONE: &str =
        "CREATE FUNCTION add_one(a integer) RETURNS integer LANGUAGE sql AS 'SELECT a + 1'";

    fn inline(sql: &str, definitions: &[&str]) -> Result<Option<String>, ParseError> {
        let functions: Vec<SqlFunction> = definitions
            .iter()
            .map(|definition| parse(definition).function)
            .collect();
        let parse_result = crate::parse(sql)?;
        inline_calls(sql, &parse_result, &mut |name| {
            Ok(functions
                .iter()
                .find(|function| function.name == name)
                .cloned()
                .map(Arc::new))
        })
    }

    fn inlined(sql: &str, definitions: &[&str]) -> String {
        inline(sql, definitions)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .unwrap_or_else(|| panic!("{sql} has no calls to inline"))
    }

    fn inline_error(sql: &str, definitions: &[&str]) -> String {
        match inline(sql, definitions) {
            Err(ParseError::ParseError(e)) => e,
            Ok(result) => panic!("{sql} must be rejected, got {result:?}"),
        }
    }

    fn add_one(argument: &str) -> String {
        format!("(CAST(((CAST(({argument}) AS pg_catalog.int4)) + 1) AS pg_catalog.int4))")
    }

    #[test]
    fn statements_without_calls_of_sql_functions_are_not_changed() {
        assert_eq!(
            inline(
                "SELECT lower('A'), add_one(1, 2), other.add_one(1)",
                &[ADD_ONE]
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn select_item_call_is_replaced_and_keeps_the_function_name() {
        assert_eq!(
            inlined("SELECT add_one(41)", &[ADD_ONE]),
            format!("SELECT {} AS add_one", add_one("41"))
        );
        assert_eq!(
            inlined("SELECT add_one(41) AS answer", &[ADD_ONE]),
            format!("SELECT {} AS answer", add_one("41"))
        );
    }

    #[test]
    fn calls_are_replaced_in_every_clause() {
        assert_eq!(
            inlined(
                "SELECT x FROM t WHERE public.add_one(x) > 2 ORDER BY add_one(x)",
                &[ADD_ONE]
            ),
            format!(
                "SELECT x FROM t WHERE {} > 2 ORDER BY {}",
                add_one("x"),
                add_one("x")
            )
        );
    }

    #[test]
    fn calls_are_replaced_in_data_changes() {
        assert_eq!(
            inlined("INSERT INTO t VALUES (add_one(1))", &[ADD_ONE]),
            format!("INSERT INTO t VALUES ({})", add_one("1"))
        );
        assert_eq!(
            inlined(
                "UPDATE t SET x = add_one(x) RETURNING add_one(x)",
                &[ADD_ONE]
            ),
            format!(
                "UPDATE t SET x = {} RETURNING {} AS add_one",
                add_one("x"),
                add_one("x")
            )
        );
        assert_eq!(
            inlined("DELETE FROM t WHERE add_one($1) = x", &[ADD_ONE]),
            format!("DELETE FROM t WHERE {} = x", add_one("$1"))
        );
    }

    #[test]
    fn calls_in_arguments_and_bodies_are_replaced() {
        assert_eq!(
            inlined("SELECT add_one(add_one(1))", &[ADD_ONE]),
            format!("SELECT {} AS add_one", add_one(&add_one("1")))
        );
        let add_two = "CREATE FUNCTION add_two(a integer) RETURNS integer LANGUAGE sql \
                       AS 'SELECT add_one(add_one(a))'";
        let argument = "(CAST((1) AS pg_catalog.int4))";
        assert_eq!(
            inlined("SELECT add_two(1) AS v", &[ADD_ONE, add_two]),
            format!(
                "SELECT (CAST(({}) AS pg_catalog.int4)) AS v",
                add_one(&add_one(argument))
            )
        );
    }

    #[test]
    fn calls_are_found_in_any_letter_case_and_with_keyword_names() {
        let first = "CREATE FUNCTION first(a integer) RETURNS integer LANGUAGE sql AS 'SELECT a'";
        assert_eq!(
            inlined("SELECT ADD_ONE(1), First(2) AS f", &[ADD_ONE, first]),
            format!(
                "SELECT {} AS add_one, (CAST(((CAST((2) AS pg_catalog.int4))) AS pg_catalog.int4)) AS f",
                add_one("1")
            )
        );
    }

    #[test]
    fn quoted_function_names_are_quoted_in_the_alias() {
        let five = "CREATE FUNCTION \"Five\"() RETURNS integer LANGUAGE sql AS 'SELECT 5'";
        assert_eq!(
            inlined("SELECT \"Five\"()", &[five]),
            "SELECT (CAST((5) AS pg_catalog.int4)) AS \"Five\""
        );
    }

    #[test]
    fn repeated_arguments_cannot_contain_calls_or_subqueries() {
        let square =
            "CREATE FUNCTION square(a float8) RETURNS float8 LANGUAGE sql AS 'SELECT a * a'";
        assert!(inline("SELECT square(x + 1) FROM t", &[square]).is_ok());
        for sql in ["SELECT square(random())", "SELECT square((SELECT 2))"] {
            let e = inline_error(sql, &[square]);
            assert!(
                e.contains("argument 1 is used more than once"),
                "{sql}: {e}"
            );
        }
    }

    #[test]
    fn calls_that_cannot_be_inlined_are_rejected() {
        let f = "CREATE FUNCTION f(a integer) RETURNS integer LANGUAGE sql AS 'SELECT g(a)'";
        let g = "CREATE FUNCTION g(a integer) RETURNS integer LANGUAGE sql AS 'SELECT f(a)'";
        for (sql, definitions, message) in [
            (
                "SELECT f(1)",
                &[f, g][..],
                "cannot inline recursive SQL function \"f\"",
            ),
            (
                "SELECT * FROM add_one(1)",
                &[ADD_ONE],
                "cannot be called in FROM yet",
            ),
            (
                "SELECT add_one(DISTINCT x) FROM t",
                &[ADD_ONE],
                "DISTINCT is not supported",
            ),
            (
                "SELECT add_one(x) OVER () FROM t",
                &[ADD_ONE],
                "OVER is not supported",
            ),
            (
                "SELECT add_one(a => 1)",
                &[ADD_ONE],
                "Named argument notation is not supported",
            ),
            (
                "CREATE VIEW v AS SELECT add_one(1)",
                &[ADD_ONE],
                "can be called only in SELECT",
            ),
            (
                "CREATE TABLE t (x integer DEFAULT add_one(1))",
                &[ADD_ONE],
                "can be called only in SELECT",
            ),
        ] {
            let e = inline_error(sql, definitions);
            assert!(e.contains(message), "{sql}: expected {message}, got {e}");
        }
    }

    #[test]
    fn deeply_nested_functions_are_rejected() {
        let definitions: Vec<String> = (0..=MAX_INLINING_DEPTH)
            .map(|level| match level {
                0 => "CREATE FUNCTION f0() RETURNS integer LANGUAGE sql AS 'SELECT 1'".to_string(),
                level => format!(
                    "CREATE FUNCTION f{level}() RETURNS integer LANGUAGE sql AS 'SELECT f{}()'",
                    level - 1
                ),
            })
            .collect();
        let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
        assert!(inline(
            &format!("SELECT f{}()", MAX_INLINING_DEPTH - 1),
            &definitions
        )
        .is_ok());
        let e = inline_error(&format!("SELECT f{MAX_INLINING_DEPTH}()"), &definitions);
        assert!(e.contains("nested more than 64 levels deep"), "{e}");
    }
}
