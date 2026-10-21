use crate::user_function::{UserFunction, Volatility};
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{FunctionParameter, SelectStmt, SetOperation, TypeName};
use turso_core::dialect::FunctionCatalog;
use turso_core::schema::FunctionDef;
use turso_core::{walk_expr_mut, Func, LimboError, Result, WalkControl};
use turso_parser::ast;
use turso_pg_parser::translator::PostgreSQLTranslator;

pub(crate) fn inline_sql_function(
    function: &FunctionDef,
    user_function: &UserFunction,
    args: &[Box<ast::Expr>],
    functions: &dyn FunctionCatalog,
) -> Result<ast::Expr> {
    let cannot_execute = |reason: &str| {
        LimboError::ParseError(format!(
            "cannot execute function {}(): {reason}",
            function.name
        ))
    };
    if user_function.returns_set() {
        return Err(cannot_execute(
            "functions that return a set are not supported yet",
        ));
    }
    let Some(return_type) = user_function.return_type() else {
        return Err(cannot_execute(
            "functions with OUT parameters are not supported yet",
        ));
    };
    match type_name(return_type) {
        "trigger" => {
            return Err(LimboError::ParseError(
                "trigger functions can only be called as triggers".to_string(),
            ))
        }
        "void" | "record" => {
            return Err(cannot_execute(&format!(
                "functions that return {} are not supported yet",
                type_name(return_type)
            )))
        }
        name if name.starts_with("any") => {
            return Err(cannot_execute(
                "polymorphic functions are not supported yet",
            ))
        }
        _ => {}
    }
    let Some(body) = body_expression(user_function)? else {
        return Err(cannot_execute(
            "only a body of one expression without FROM is supported",
        ));
    };
    let mut expr = translate_with_cast(body, return_type)?;

    let parameter_names: Vec<&str> = user_function
        .input_parameters()
        .map(|parameter| parameter.name.as_str())
        .collect();
    let mut uses = vec![0usize; parameter_names.len()];
    let mut body_contains_subquery = false;
    let mut body_contains_aggregate = false;
    walk_expr_mut(&mut expr, &mut |e| {
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                body_contains_subquery = true;
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                body_contains_aggregate |= filter_over.over_clause.is_some()
                    || matches!(
                        Func::resolve_function(name.as_str(), args.len()),
                        Ok(Some(Func::Agg(_) | Func::Window(_)))
                    );
            }
            ast::Expr::FunctionCallStar { .. } => body_contains_aggregate = true,
            _ => {}
        }
        if let Some(index) = parameter_index(e, &function.name, &parameter_names)? {
            uses[index] += 1;
        }
        Ok(WalkControl::Continue)
    })?;
    if body_contains_subquery {
        return Err(cannot_execute(
            "a body with a subquery is not supported yet",
        ));
    }
    if body_contains_aggregate {
        return Err(cannot_execute(
            "a body with an aggregate or window function is not supported yet",
        ));
    }
    if expression_volatility(&expr, functions) > user_function.volatility() {
        return Err(cannot_execute(&format!(
            "the body is more volatile than the declaration {}",
            user_function.volatility().keyword()
        )));
    }

    let strict = user_function.is_strict();
    for (index, arg) in args.iter().enumerate() {
        let uses = uses[index] + usize::from(strict);
        if uses > 1
            && (contains_subquery(arg)
                || expression_volatility(arg, functions) == Volatility::Volatile)
        {
            return Err(cannot_execute(&format!(
                "argument {} is volatile and the body uses it more than one time",
                index + 1
            )));
        }
    }

    let arguments = user_function
        .input_parameters()
        .zip(args)
        .map(|(parameter, arg)| converted_argument(parameter, arg))
        .collect::<Result<Vec<_>>>()?;
    walk_expr_mut(&mut expr, &mut |e| {
        let Some(index) = parameter_index(e, &function.name, &parameter_names)? else {
            return Ok(WalkControl::Continue);
        };
        *e = arguments[index].clone();
        Ok(WalkControl::SkipChildren)
    })?;

    if !strict || args.is_empty() {
        return Ok(expr);
    }
    let any_argument_is_null = args
        .iter()
        .map(|arg| ast::Expr::IsNull(arg.clone()))
        .reduce(|a, b| ast::Expr::Binary(Box::new(a), ast::Operator::Or, Box::new(b)))
        .expect("a function with arguments has at least one null test");
    Ok(ast::Expr::Case {
        base: None,
        when_then_pairs: vec![(
            Box::new(any_argument_is_null),
            Box::new(ast::Expr::Literal(ast::Literal::Null)),
        )],
        else_expr: Some(Box::new(expr)),
    })
}

fn body_expression(user_function: &UserFunction) -> Result<Option<pg_query::protobuf::Node>> {
    let body_statement = match user_function.statement.sql_body.as_deref() {
        Some(body) => single_statement(body),
        None => {
            let Some(text) = user_function.body_text() else {
                return Ok(None);
            };
            let parsed =
                turso_pg_parser::parse(&text).map_err(|e| LimboError::ParseError(e.to_string()))?;
            match parsed.protobuf.stmts.as_slice() {
                [statement] => statement.stmt.as_deref().cloned(),
                _ => None,
            }
        }
    };
    let Some(body_statement) = body_statement else {
        return Ok(None);
    };
    Ok(match body_statement.node {
        Some(Node::ReturnStmt(statement)) => statement.returnval.map(|value| *value),
        Some(Node::SelectStmt(select)) => simple_select_target(&select),
        _ => None,
    })
}

fn single_statement(body: &pg_query::protobuf::Node) -> Option<pg_query::protobuf::Node> {
    match &body.node {
        Some(Node::List(list)) => match list.items.as_slice() {
            [item] => single_statement(item),
            _ => None,
        },
        _ => Some(body.clone()),
    }
}

fn simple_select_target(select: &SelectStmt) -> Option<pg_query::protobuf::Node> {
    let simple = select.op() == SetOperation::SetopNone
        && select.from_clause.is_empty()
        && select.where_clause.is_none()
        && select.group_clause.is_empty()
        && select.having_clause.is_none()
        && select.window_clause.is_empty()
        && select.distinct_clause.is_empty()
        && select.sort_clause.is_empty()
        && select.limit_offset.is_none()
        && select.limit_count.is_none()
        && select.with_clause.is_none()
        && select.values_lists.is_empty()
        && select.into_clause.is_none()
        && select.locking_clause.is_empty();
    match select.target_list.as_slice() {
        [target] if simple => match &target.node {
            Some(Node::ResTarget(target)) => target.val.as_deref().cloned(),
            _ => None,
        },
        _ => None,
    }
}

fn translate_with_cast(
    body: pg_query::protobuf::Node,
    return_type: &TypeName,
) -> Result<ast::Expr> {
    let mut parsed =
        turso_pg_parser::parse("SELECT NULL").expect("a constant SELECT is valid PostgreSQL");
    let Some(Node::SelectStmt(select)) = parsed.protobuf.stmts[0]
        .stmt
        .as_mut()
        .and_then(|stmt| stmt.node.as_mut())
    else {
        unreachable!("SELECT NULL parses to a SELECT statement");
    };
    let Some(Node::ResTarget(target)) = select.target_list[0].node.as_mut() else {
        unreachable!("SELECT NULL has one target");
    };
    target.val = Some(Box::new(pg_query::protobuf::Node {
        node: Some(Node::TypeCast(Box::new(pg_query::protobuf::TypeCast {
            arg: Some(Box::new(body)),
            type_name: Some(return_type.clone()),
            location: -1,
        }))),
    }));
    let parsed = pg_query::ParseResult::new(parsed.protobuf, String::new());
    let statement = PostgreSQLTranslator::new()
        .translate(&parsed)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let ast::Stmt::Select(select) = statement else {
        unreachable!("a SELECT translates to a SELECT");
    };
    let ast::OneSelect::Select { mut columns, .. } = select.body.select else {
        unreachable!("a SELECT with one target translates to one SELECT");
    };
    let Some(ast::ResultColumn::Expr(expr, _)) = columns.pop() else {
        unreachable!("a SELECT with one target translates to one result column");
    };
    Ok(*expr)
}

/// The argument converted to the type of its parameter, as PostgreSQL
/// converts it before the call. As in PostgreSQL, the type modifiers of the
/// parameter, such as the length of `varchar(3)`, do not apply.
fn converted_argument(parameter: &FunctionParameter, arg: &ast::Expr) -> Result<ast::Expr> {
    let argument = ast::Expr::Parenthesized(vec![Box::new(arg.clone())]);
    let Some(parameter_type) = parameter.arg_type.as_ref() else {
        return Ok(argument);
    };
    if type_name(parameter_type).starts_with("any") {
        return Ok(argument);
    }
    let mut parameter_type = parameter_type.clone();
    parameter_type.typmods.clear();
    parameter_type.typemod = -1;
    let mut converted = translate_with_cast(argument_placeholder(), &parameter_type)?;
    walk_expr_mut(&mut converted, &mut |e| {
        if matches!(e, ast::Expr::Id(name) if name.as_str() == ARGUMENT_PLACEHOLDER) {
            *e = argument.clone();
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(converted)
}

const ARGUMENT_PLACEHOLDER: &str = "__argument";

fn argument_placeholder() -> pg_query::protobuf::Node {
    pg_query::protobuf::Node {
        node: Some(Node::ColumnRef(pg_query::protobuf::ColumnRef {
            fields: vec![pg_query::protobuf::Node {
                node: Some(Node::String(pg_query::protobuf::String {
                    sval: ARGUMENT_PLACEHOLDER.to_string(),
                })),
            }],
            location: -1,
        })),
    }
}

fn parameter_index(
    expr: &ast::Expr,
    function_name: &str,
    parameter_names: &[&str],
) -> Result<Option<usize>> {
    let index = match expr {
        ast::Expr::Id(name) => {
            let Some(index) = parameter_names.iter().position(|p| *p == name.as_str()) else {
                return Err(LimboError::ParseError(format!(
                    "column \"{}\" does not exist",
                    name.as_str()
                )));
            };
            index
        }
        ast::Expr::Qualified(qualifier, name) if qualifier.as_str() == function_name => {
            let Some(index) = parameter_names.iter().position(|p| *p == name.as_str()) else {
                return Err(LimboError::ParseError(format!(
                    "column {}.{} does not exist",
                    qualifier.as_str(),
                    name.as_str()
                )));
            };
            index
        }
        ast::Expr::Qualified(qualifier, name) => {
            return Err(LimboError::ParseError(format!(
                "missing FROM-clause entry for table \"{}\" in reference to {}",
                qualifier.as_str(),
                name.as_str()
            )))
        }
        ast::Expr::Variable(variable) => {
            let index = variable.index.get() as usize - 1;
            if index >= parameter_names.len() {
                return Err(LimboError::ParseError(format!(
                    "there is no parameter ${}",
                    variable.index
                )));
            }
            index
        }
        _ => return Ok(None),
    };
    Ok(Some(index))
}

fn contains_subquery(expr: &ast::Expr) -> bool {
    let mut found = false;
    let mut expr = expr.clone();
    walk_expr_mut(&mut expr, &mut |e| {
        found |= matches!(
            e,
            ast::Expr::Subquery(_)
                | ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::SubqueryResult { .. }
        );
        Ok(WalkControl::Continue)
    })
    .expect("the walk callback does not fail");
    found
}

fn expression_volatility(expr: &ast::Expr, functions: &dyn FunctionCatalog) -> Volatility {
    let mut volatility = Volatility::Immutable;
    let mut expr = expr.clone();
    walk_expr_mut(&mut expr, &mut |e| {
        let found = match e {
            ast::Expr::FunctionCall { name, args, .. } => {
                function_volatility(name.as_str(), args.len(), functions)
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                function_volatility(name.as_str(), 0, functions)
            }
            ast::Expr::Literal(
                ast::Literal::CurrentDate
                | ast::Literal::CurrentTime
                | ast::Literal::CurrentTimestamp,
            ) => Volatility::Stable,
            ast::Expr::Raise(..) => Volatility::Volatile,
            _ => Volatility::Immutable,
        };
        volatility = volatility.max(found);
        Ok(WalkControl::Continue)
    })
    .expect("the walk callback does not fail");
    volatility
}

fn function_volatility(
    name: &str,
    arg_count: usize,
    functions: &dyn FunctionCatalog,
) -> Volatility {
    if let Some(function) = functions.find_function(name, arg_count) {
        return UserFunction::parse(&function)
            .map_or(Volatility::Volatile, |function| function.volatility());
    }
    let bare_name = name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase();
    match bare_name.as_str() {
        "random" | "randomblob" | "random_normal" | "gen_random_uuid" | "uuid_generate_v1"
        | "uuid_generate_v4" | "uuid4" | "uuid7" | "nextval" | "setval" | "currval" | "lastval"
        | "clock_timestamp" | "timeofday" | "txid_current" | "set_config" | "pg_notify"
        | "changes" | "total_changes" | "last_insert_rowid" => Volatility::Volatile,
        "now"
        | "transaction_timestamp"
        | "statement_timestamp"
        | "current_setting"
        | "current_schema"
        | "current_schemas"
        | "current_database"
        | "current_user"
        | "session_user"
        | "version"
        | "to_char"
        | "to_date"
        | "to_timestamp"
        | "age"
        | "date"
        | "time"
        | "datetime"
        | "julianday"
        | "strftime"
        | "unixepoch"
        | "timediff" => Volatility::Stable,
        _ => Volatility::Immutable,
    }
}

fn type_name(type_name: &TypeName) -> &str {
    type_name
        .names
        .iter()
        .filter_map(|name| match &name.node {
            Some(Node::String(name)) => Some(name.sval.as_str()),
            _ => None,
        })
        .next_back()
        .unwrap_or_default()
}
