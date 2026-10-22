use crate::translator::map_schema_name;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{CreateFunctionStmt, DropStmt, FunctionParameterMode};
use pg_query::NodeRef;
use turso_parser::ast;

const SUPPORTED_LANGUAGES: [&str; 2] = ["sql", "plpgsql"];

pub(crate) fn translate_create_function(
    function: &CreateFunctionStmt,
) -> Result<ast::Stmt, ParseError> {
    let language = function_language(function)?;
    if !SUPPORTED_LANGUAGES.contains(&language.as_str()) {
        return Err(ParseError::ParseError(format!(
            "language \"{language}\" does not exist"
        )));
    }
    let definition = NodeRef::CreateFunctionStmt(function)
        .deparse()
        .map_err(|e| ParseError::ParseError(e.to_string()))?;
    Ok(ast::Stmt::CreateFunction {
        or_replace: function.replace,
        func_name: function_name(&function.funcname)?,
        arg_count: input_parameter_count(&function.parameters),
        definition,
    })
}

pub(crate) fn translate_drop_functions(drop: &DropStmt) -> Result<Vec<ast::Stmt>, ParseError> {
    drop.objects
        .iter()
        .map(|object| {
            let Some(Node::ObjectWithArgs(function)) = &object.node else {
                return Err(ParseError::ParseError(
                    "DROP FUNCTION: unexpected object format".into(),
                ));
            };
            let arg_count =
                (!function.args_unspecified).then(|| input_parameter_count(&function.objfuncargs));
            Ok(ast::Stmt::DropFunction {
                if_exists: drop.missing_ok,
                func_name: function_name(&function.objname)?,
                arg_count,
            })
        })
        .collect()
}

pub fn function_language(function: &CreateFunctionStmt) -> Result<String, ParseError> {
    let language = function
        .options
        .iter()
        .find_map(|option| match &option.node {
            Some(Node::DefElem(def)) if def.defname == "language" => match def.arg.as_deref() {
                Some(pg_query::protobuf::Node {
                    node: Some(Node::String(language)),
                }) => Some(language.sval.to_ascii_lowercase()),
                _ => None,
            },
            _ => None,
        });
    match language {
        Some(language) => Ok(language),
        None if function.sql_body.is_some() => Ok("sql".to_string()),
        None => Err(ParseError::ParseError("no language specified".into())),
    }
}

pub fn input_parameter_count(parameters: &[pg_query::protobuf::Node]) -> usize {
    parameters
        .iter()
        .filter(|parameter| match &parameter.node {
            Some(Node::FunctionParameter(parameter)) => !matches!(
                parameter.mode(),
                FunctionParameterMode::FuncParamOut | FunctionParameterMode::FuncParamTable
            ),
            _ => false,
        })
        .count()
}

/// The CREATE FUNCTION statement of `parse_result` when it names its
/// function without a schema.
pub fn unqualified_create_function(
    parse_result: &pg_query::ParseResult,
) -> Option<&CreateFunctionStmt> {
    let node = parse_result
        .protobuf
        .stmts
        .first()?
        .stmt
        .as_deref()?
        .node
        .as_ref()?;
    match node {
        Node::CreateFunctionStmt(function) if function.funcname.len() == 1 => Some(function),
        _ => None,
    }
}

/// The SQL text of `function` with its function name in `schema`.
pub fn qualify_create_function_name(
    function: &CreateFunctionStmt,
    schema: &str,
) -> Result<String, ParseError> {
    let mut function = function.clone();
    function.funcname.insert(
        0,
        pg_query::protobuf::Node {
            node: Some(Node::String(pg_query::protobuf::String {
                sval: schema.to_string(),
            })),
        },
    );
    NodeRef::CreateFunctionStmt(&function)
        .deparse()
        .map_err(|e| ParseError::ParseError(e.to_string()))
}

fn function_name(names: &[pg_query::protobuf::Node]) -> Result<ast::QualifiedName, ParseError> {
    let names: Vec<&str> = names
        .iter()
        .filter_map(|name| match &name.node {
            Some(Node::String(name)) => Some(name.sval.as_str()),
            _ => None,
        })
        .collect();
    match names.as_slice() {
        [name] => Ok(ast::QualifiedName::single(ast::Name::from_string(*name))),
        [schema, name] => Ok(ast::QualifiedName::fullname(
            ast::Name::from_string(map_schema_name(schema)),
            ast::Name::from_string(*name),
        )),
        _ => Err(ParseError::ParseError(format!(
            "improper qualified name (too many dotted names): {}",
            names.join(".")
        ))),
    }
}
