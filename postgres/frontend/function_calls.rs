//! Calls of the functions that users define.
//!
//! tursopg runs a call of an SQL function as PostgreSQL does when it inlines
//! the function (`inline_function` in `clauses.c`): the expression of the
//! body takes the place of the call, with the arguments in the place of the
//! parameters. The engine asks for this expression before it translates a
//! statement, also when it prepares the statement again, so a call always
//! uses the function as it is then. The names in the body are not bound
//! before: the statement that calls the function binds them, with its own
//! search path, as in PostgreSQL.
//!
//! PostgreSQL runs a function that it does not inline as a query of its
//! own. tursopg cannot do that yet and rejects a call of such a function.

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::{walk_expr, walk_expr_mut, Connection, Func, LimboError, WalkControl, MAIN_DB_ID};
use turso_parser::ast;
use turso_parser::parser::MAX_EXPR_DEPTH;
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_SCHEMA_NAME, SQLSTATE_STATEMENT_TOO_COMPLEX,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_WRONG_OBJECT_TYPE,
};
use turso_pg_parser::{inlined_function, InlinedFunction};

use crate::error::sqlstate_error;
use crate::sql_functions::{
    function_catalog, is_built_in_function, search_path_namespaces, FunctionCatalog, SqlFunction,
};

/// [`turso_core::Dialect::expands_function_calls`] for the PostgreSQL
/// frontend.
pub(crate) fn has_functions(schema: &Schema) -> bool {
    function_catalog(schema).is_some_and(|functions| functions.iter().next().is_some())
}

/// [`turso_core::Dialect::expand_function_call`] for the PostgreSQL
/// frontend.
pub(crate) fn expand_function_call(
    dialect: &dyn turso_core::Dialect,
    schema: &Schema,
    connection: &Connection,
    call: &ast::Expr,
) -> turso_core::Result<Option<ast::Expr>> {
    let Some(functions) = function_catalog(schema) else {
        return Ok(None);
    };
    Expansion {
        built_in_functions: BuiltInFunctions {
            dialect,
            connection,
        },
        connection,
        functions,
        active: Vec::new(),
    }
    .expand(call)
}

/// Fails when one of `called_functions`, the functions that the statement
/// `parsed` calls, is a function of a schema that does not exist, with
/// PostgreSQL's errors, or is a function that a user defined, called where
/// tursopg cannot expand the call: in a FROM clause, or in a statement that
/// stores the call, such as CREATE VIEW or a column default.
pub(crate) fn check_function_calls(
    connection: &Connection,
    called_functions: &[turso_pg_parser::CalledFunction],
    parsed: &turso_pg_parser::ParseResult,
) -> turso_core::Result<()> {
    let schema = connection.current_schema();
    let functions = function_catalog(&schema);
    let is_user_function = |called: &turso_pg_parser::CalledFunction| {
        functions.is_some_and(|functions| {
            functions_named(
                connection,
                functions,
                called.schema.as_deref(),
                &called.name,
            )
            .next()
            .is_some()
        })
    };
    for called in called_functions {
        let Some(schema_name) = &called.schema else {
            continue;
        };
        // PostgreSQL names the types of the arguments, which tursopg does
        // not know.
        let no_such_function = || {
            sqlstate_error(
                SQLSTATE_UNDEFINED_FUNCTION,
                format!("function {schema_name}.{} does not exist", called.name),
            )
        };
        if schema_name == "pg_catalog" {
            // The translator removes `pg_catalog.` from the name, so the
            // expansion would find a function of the search path.
            let without_schema = turso_pg_parser::CalledFunction {
                schema: None,
                ..called.clone()
            };
            if is_user_function(&without_schema) && !is_built_in_function(connection, &called.name)
            {
                return Err(no_such_function());
            }
            continue;
        }
        // The translator knows the auth functions.
        if schema_name == "auth" {
            continue;
        }
        if schema_namespace(connection, schema_name).is_none() {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                format!("schema \"{schema_name}\" does not exist"),
            ));
        }
        if !is_user_function(called) {
            return Err(no_such_function());
        }
    }
    if functions.is_none_or(|functions| functions.iter().next().is_none()) {
        return Ok(());
    }
    let unsupported_call = |called: &turso_pg_parser::CalledFunction, place: &str| {
        not_supported(format!(
            "a call of the user-defined function \"{}\" in {place}",
            called.name
        ))
    };
    if let Some(called) = called_functions
        .iter()
        .find(|called| called.in_from && is_user_function(called))
    {
        return Err(unsupported_call(called, "FROM"));
    }
    if let Some(statement) = turso_pg_parser::statement_without_function_expansion(parsed) {
        if let Some(called) = called_functions
            .iter()
            .find(|called| is_user_function(called))
        {
            return Err(unsupported_call(called, statement));
        }
    }
    Ok(())
}

struct Expansion<'a> {
    built_in_functions: BuiltInFunctions<'a>,
    connection: &'a Connection,
    functions: &'a FunctionCatalog,
    /// The oids of the functions whose bodies are being expanded, outermost
    /// first.
    active: Vec<i64>,
}

impl Expansion<'_> {
    /// The expression that `call` stands for, or None when `call` does not
    /// call a function that a user defined.
    fn expand(&mut self, call: &ast::Expr) -> turso_core::Result<Option<ast::Expr>> {
        let ast::Expr::FunctionCall { name, args, .. } = call else {
            return Ok(None);
        };
        let name = name.as_str();
        let Some(function) = self.called_function(name)? else {
            return Ok(None);
        };
        reject_aggregate_syntax(name, call)?;
        if args.len() != function.parameter_types.len() {
            // PostgreSQL names the types of the arguments, which tursopg
            // does not know.
            return Err(sqlstate_error(
                SQLSTATE_UNDEFINED_FUNCTION,
                format!(
                    "function {name} with {} argument{} does not exist",
                    args.len(),
                    if args.len() == 1 { "" } else { "s" }
                ),
            ));
        }
        if self.active.contains(&function.oid) {
            return Err(not_supported(format!(
                "a call of function \"{name}\", which calls itself,"
            )));
        }
        // The expansion of a body in a body uses the stack.
        if self.active.len() == MAX_EXPR_DEPTH {
            return Err(stack_depth_limit_exceeded());
        }
        let inlined = inlined_function(
            &function.parameter_types,
            &function.return_type,
            &function.body,
        )
        .map_err(|error| LimboError::ParseError(error.to_string()))?
        .filter(|inlined| !self.has_subquery_or_aggregate(&inlined.result))
        .ok_or_else(|| {
            not_supported(format!(
                "a call of function \"{name}\", whose body is not a plain SELECT of one \
                 expression,"
            ))
        })?;

        let (mut expression, use_counts) = with_arguments(inlined, args)?;
        // PostgreSQL does not inline a function that would compute a
        // volatile or an expensive argument more than once. tursopg cannot
        // tell such an argument yet.
        if use_counts.iter().any(|use_count| *use_count > 1) {
            return Err(not_supported(format!(
                "a call of function \"{name}\", whose body uses a parameter more than once,"
            )));
        }
        self.active.push(function.oid);
        let expanded = self.expand_calls_in(&mut expression);
        self.active.pop();
        expanded?;
        // The engine walks an expression with recursion. Its parser does
        // not make an expression with more levels than this.
        if height(&expression) > MAX_EXPR_DEPTH {
            return Err(stack_depth_limit_exceeded());
        }
        Ok(Some(expression))
    }

    /// Expands the calls in `expression`, the arguments of a call before the
    /// call.
    fn expand_calls_in(&mut self, expression: &mut ast::Expr) -> turso_core::Result<()> {
        walk_expr_mut(
            expression,
            &mut |expression: &mut ast::Expr| -> turso_core::Result<WalkControl> {
                let ast::Expr::FunctionCall { args, .. } = expression else {
                    return Ok(WalkControl::Continue);
                };
                for argument in args.iter_mut() {
                    self.expand_calls_in(argument)?;
                }
                if let Some(expansion) = self.expand(expression)? {
                    *expression = expansion;
                }
                Ok(WalkControl::SkipChildren)
            },
        )?;
        Ok(())
    }

    /// The function that a call names `name`: `schema.function`, or
    /// `function` for a function of the search path. PostgreSQL chooses
    /// between functions of one name by the types of the arguments, which
    /// tursopg does not know, so a call of a name that several functions
    /// have is rejected. A built-in function is one of them when the call
    /// names no schema, because PostgreSQL looks in pg_catalog first.
    /// CREATE FUNCTION refuses the name of a built-in function, but a later
    /// version of tursopg or an extension can add a built-in function with
    /// the name of a stored function.
    fn called_function(&self, name: &str) -> turso_core::Result<Option<&SqlFunction>> {
        let (schema_name, function_name) = match name.split_once('.') {
            Some((schema_name, function_name)) => (Some(schema_name), function_name),
            None => (None, name),
        };
        let mut named =
            functions_named(self.connection, self.functions, schema_name, function_name);
        let Some(function) = named.next() else {
            return Ok(None);
        };
        if named.next().is_some()
            || (schema_name.is_none() && is_built_in_function(self.connection, function_name))
        {
            return Err(not_supported(format!(
                "a call of function \"{name}\", a name that several functions have,"
            )));
        }
        Ok(Some(function))
    }

    /// Whether `expression` has a subquery or a call of an aggregate or
    /// window function, which PostgreSQL does not inline.
    fn has_subquery_or_aggregate(&self, expression: &ast::Expr) -> bool {
        let mut found = false;
        let _ = walk_expr(expression, &mut |expression| {
            found |= match expression {
                ast::Expr::Subquery(_)
                | ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::SubqueryResult { .. } => true,
                call => self.built_in_functions.is_aggregate_call(call),
            };
            Ok(WalkControl::Continue)
        });
        found
    }
}

/// The built-in functions of a connection.
#[derive(Clone, Copy)]
struct BuiltInFunctions<'a> {
    dialect: &'a dyn turso_core::Dialect,
    connection: &'a Connection,
}

/// What the engine knows of a built-in function.
struct BuiltInFunction {
    /// An aggregate or a window function.
    is_aggregate: bool,
}

impl BuiltInFunctions<'_> {
    /// The function that a call of `name` with `argument_count` arguments
    /// runs, found as the engine finds it: the dialect has the functions of
    /// the engine, and the connection has the functions that the frontend
    /// or an extension registered. The number of arguments is necessary:
    /// `max(a)` is an aggregate, and `max(a, b)`, which GREATEST becomes, is
    /// not.
    fn get(&self, name: &str, argument_count: usize) -> Option<BuiltInFunction> {
        if let Ok(Some(function)) = self.dialect.resolve_function(name, argument_count) {
            return Some(BuiltInFunction {
                is_aggregate: matches!(function, Func::Agg(_) | Func::Window(_)),
            });
        }
        self.connection
            .get_syms_functions()
            .into_iter()
            .find(|(function_name, ..)| function_name.eq_ignore_ascii_case(name))
            .map(|(_, is_aggregate, ..)| BuiltInFunction { is_aggregate })
    }

    /// Whether `expression` is a call of an aggregate or a window function.
    fn is_aggregate_call(&self, expression: &ast::Expr) -> bool {
        match expression {
            ast::Expr::FunctionCallStar { .. } => true,
            ast::Expr::FunctionCall {
                name,
                args,
                distinctness,
                order_by,
                within_group,
                filter_over,
            } => {
                distinctness.is_some()
                    || !order_by.is_empty()
                    || !within_group.is_empty()
                    || filter_over.filter_clause.is_some()
                    || filter_over.over_clause.is_some()
                    || self
                        .get(name.as_str(), args.len())
                        .is_some_and(|function| function.is_aggregate)
            }
            _ => false,
        }
    }
}

/// PostgreSQL's errors for a call of a function that is not an aggregate
/// with the syntax of an aggregate call, in the order PostgreSQL checks.
fn reject_aggregate_syntax(name: &str, call: &ast::Expr) -> turso_core::Result<()> {
    let ast::Expr::FunctionCall {
        distinctness,
        order_by,
        within_group,
        filter_over,
        ..
    } = call
    else {
        return Ok(());
    };
    let clause = if distinctness.is_some() {
        "DISTINCT"
    } else if !within_group.is_empty() {
        "WITHIN GROUP"
    } else if !order_by.is_empty() {
        "ORDER BY"
    } else if filter_over.filter_clause.is_some() {
        "FILTER"
    } else if filter_over.over_clause.is_some() {
        return Err(sqlstate_error(
            SQLSTATE_WRONG_OBJECT_TYPE,
            format!(
                "OVER specified, but {name} is not a window function nor an aggregate function"
            ),
        ));
    } else {
        return Ok(());
    };
    Err(sqlstate_error(
        SQLSTATE_WRONG_OBJECT_TYPE,
        format!("{clause} specified, but {name} is not an aggregate function"),
    ))
}

/// The expression of `inlined` with `arguments` in the place of the
/// parameters, and how many times the expression uses each parameter.
fn with_arguments(
    inlined: InlinedFunction,
    arguments: &[Box<ast::Expr>],
) -> turso_core::Result<(ast::Expr, Vec<usize>)> {
    let mut use_counts = vec![0; arguments.len()];
    let mut expression = inlined.result;
    walk_expr_mut(
        &mut expression,
        &mut |expression: &mut ast::Expr| -> turso_core::Result<WalkControl> {
            let ast::Expr::Variable(parameter) = expression else {
                return Ok(WalkControl::Continue);
            };
            let position = parameter.index.get() as usize - 1;
            // CREATE FUNCTION refuses a body that uses a parameter that the
            // function does not have.
            let (Some(argument), Some(cast)) =
                (arguments.get(position), inlined.parameters.get(position))
            else {
                return Err(LimboError::Corrupt(format!(
                    "a stored function uses parameter ${}, which it does not have",
                    parameter.index
                )));
            };
            use_counts[position] += 1;
            *expression = cast_argument(cast.clone(), argument);
            // A variable in the argument is a variable of the statement.
            Ok(WalkControl::SkipChildren)
        },
    )?;
    Ok((expression, use_counts))
}

/// `cast`, the cast of a parameter to its type, with `argument` in the
/// place of the parameter.
fn cast_argument(mut cast: ast::Expr, argument: &ast::Expr) -> ast::Expr {
    let _ = walk_expr_mut(
        &mut cast,
        &mut |expression: &mut ast::Expr| -> turso_core::Result<WalkControl> {
            if !matches!(expression, ast::Expr::Variable(_)) {
                return Ok(WalkControl::Continue);
            }
            *expression = ast::Expr::Parenthesized(vec![Box::new(argument.clone())]);
            Ok(WalkControl::SkipChildren)
        },
    );
    cast
}

/// The functions of users that a call names `name`: the functions of schema
/// `schema_name`, or of the schemas of the search path when the call names
/// no schema.
fn functions_named<'a>(
    connection: &Connection,
    functions: &'a FunctionCatalog,
    schema_name: Option<&str>,
    name: &str,
) -> impl Iterator<Item = &'a SqlFunction> + use<'a> {
    let namespaces: Vec<NamespaceId> = match schema_name {
        Some(schema_name) => schema_namespace(connection, schema_name)
            .into_iter()
            .collect(),
        None => search_path_namespaces(connection),
    };
    let name = name.to_string();
    namespaces
        .into_iter()
        .flat_map(move |namespace| functions.named(namespace, &name))
}

/// The namespace of schema `schema_name` of the main database.
pub(crate) fn schema_namespace(connection: &Connection, schema_name: &str) -> Option<NamespaceId> {
    connection
        .qualifier(&schema_name.to_lowercase())
        .filter(|qualifier| qualifier.database_id() == MAIN_DB_ID && !qualifier.is_database())
        .map(|qualifier| qualifier.target.id)
}

/// The number of levels of `expression`: 1 for an expression that has no
/// expression in it.
fn height(expression: &ast::Expr) -> usize {
    let mut is_the_expression = true;
    let mut tallest_part = 0;
    let _ = walk_expr(expression, &mut |part: &ast::Expr| {
        if std::mem::take(&mut is_the_expression) {
            return Ok(WalkControl::Continue);
        }
        tallest_part = tallest_part.max(height(part));
        Ok(WalkControl::SkipChildren)
    });
    1 + tallest_part
}

/// PostgreSQL's error for a statement that needs more stack than
/// `max_stack_depth` permits.
fn stack_depth_limit_exceeded() -> LimboError {
    sqlstate_error(SQLSTATE_STATEMENT_TOO_COMPLEX, "stack depth limit exceeded")
}

fn not_supported(what: String) -> LimboError {
    sqlstate_error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        format!("{what} is not supported"),
    )
}
