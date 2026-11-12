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
use turso_core::{
    expr_is_deeper_than, walk_expr, walk_expr_mut, Connection, Deterministic, Func, LimboError,
    WalkControl,
};
use turso_parser::ast;
use turso_parser::parser::MAX_EXPR_DEPTH;
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_SCHEMA_NAME, SQLSTATE_STATEMENT_TOO_COMPLEX,
    SQLSTATE_UNDEFINED_COLUMN, SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_PARAMETER,
    SQLSTATE_UNDEFINED_TABLE, SQLSTATE_WRONG_OBJECT_TYPE,
};
use turso_pg_parser::{inlined_function, InlinedFunction};

use crate::error::sqlstate_error;
use crate::sql_functions::{
    function_catalog, is_built_in_function, schema_namespace, search_path_namespaces,
    FunctionCatalog, SqlFunction,
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
    let ast::Expr::FunctionCall { name, .. } = call else {
        return Ok(None);
    };
    Expansion {
        built_in_functions: BuiltInFunctions {
            dialect,
            connection,
        },
        connection,
        functions,
        statement_call_name: name.as_str(),
        active: Vec::new(),
        added_expressions: 0,
    }
    .expand(call, 1)
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
    check_schemas_of_calls(connection, functions, called_functions)?;
    if !has_functions(&schema) {
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
        .find(|called| called.in_from && is_user_function(connection, functions, called))
    {
        return Err(unsupported_call(called, "FROM"));
    }
    if let Some(statement) = turso_pg_parser::statement_without_function_expansion(parsed) {
        if let Some(called) = called_functions
            .iter()
            .find(|called| is_user_function(connection, functions, called))
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
    /// The name of the function that the statement calls.
    statement_call_name: &'a str,
    /// The oids of the functions whose bodies are being expanded, outermost
    /// first.
    active: Vec<i64>,
    /// How many expressions the bodies added to the statement.
    added_expressions: usize,
}

impl<'a> Expansion<'a> {
    /// The expression that `call` stands for, or None when `call` does not
    /// call a function that a user defined. `call` is at level `level` of
    /// the expression that takes the place of the call of the statement.
    fn expand(&mut self, call: &ast::Expr, level: usize) -> turso_core::Result<Option<ast::Expr>> {
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
        .map_err(body_error)?
        .filter(|inlined| !self.has_subquery_or_aggregate(&inlined.result))
        .ok_or_else(|| {
            not_supported(format!(
                "a call of function \"{name}\", whose body is not a plain SELECT of one \
                 expression,"
            ))
        })?;
        // CREATE FUNCTION makes these checks when it checks the body, which
        // it does not do when check_function_bodies is off.
        check_schemas_of_calls(
            self.connection,
            Some(self.functions),
            &inlined.called_functions,
        )?;
        check_body_has_no_column(&inlined.result)?;

        let use_counts = parameter_use_counts(&inlined.result, args.len())?;
        // PostgreSQL does not inline a function that would compute a
        // volatile argument or a subquery more than once.
        let has_volatile_copies = args.iter().zip(&use_counts).any(|(argument, use_count)| {
            *use_count > 1 && has_subquery_or_volatile_call(self.built_in_functions, argument)
        });
        if has_volatile_copies {
            return Err(not_supported(format!(
                "a call of function \"{name}\" with a volatile argument or a subquery that the \
                 body uses more than once"
            )));
        }
        // The expression has no place for the argument of a parameter that
        // the body does not use, so the engine does not see this argument.
        // PostgreSQL reads each argument before it inlines the function: an
        // aggregate in the argument makes the statement group its rows, and
        // a column or a function that does not exist is an error.
        let has_unread_argument = args
            .iter()
            .zip(&use_counts)
            .any(|(argument, use_count)| *use_count == 0 && !is_constant_or_variable(argument));
        if has_unread_argument {
            return Err(not_supported(format!(
                "a call of function \"{name}\" with an argument that is not a constant, for a \
                 parameter that the body does not use,"
            )));
        }
        self.add_expressions(size(&inlined.result))?;
        let mut expression = with_arguments(inlined, args);
        self.active.push(function.oid);
        let expanded = self.expand_calls_in(&mut expression, level);
        self.active.pop();
        expanded?;
        // The engine walks an expression with recursion. Its parser does
        // not make an expression with more levels than this.
        if expr_is_deeper_than(&expression, MAX_EXPR_DEPTH) {
            return Err(stack_depth_limit_exceeded());
        }
        // PostgreSQL computes an argument that is a constant before it
        // inlines the function. It also computes an argument before the
        // call when it does not inline the function, which it does for an
        // expensive argument that the body uses more than once. tursopg
        // does not know the cost that PostgreSQL gives to an argument,
        // because the cost depends on the types. Thus an argument that
        // fails can be an error in PostgreSQL also when the body does not
        // compute its parameter. tursopg computes the argument where the
        // expression has it.
        let has_argument_that_can_be_skipped =
            args.iter().zip(&use_counts).any(|(argument, use_count)| {
                let can_be_computed_before_the_call =
                    *use_count > 1 || is_constant(self.built_in_functions, argument);
                *use_count > 0
                    && can_be_computed_before_the_call
                    && !cannot_fail(argument)
                    && !always_computes(&expression, argument)
            });
        if has_argument_that_can_be_skipped {
            return Err(not_supported(format!(
                "a call of function \"{name}\" with an argument that can fail, for a parameter \
                 that the body does not always compute,"
            )));
        }
        Ok(Some(expression))
    }

    /// Counts `count` expressions that a body adds to the statement. Fails
    /// when the bodies then add more than [`MAX_ADDED_EXPRESSIONS`].
    fn add_expressions(&mut self, count: usize) -> turso_core::Result<()> {
        self.added_expressions += count;
        if self.added_expressions > MAX_ADDED_EXPRESSIONS {
            return Err(sqlstate_error(
                SQLSTATE_STATEMENT_TOO_COMPLEX,
                format!(
                    "the expression that takes the place of the call of function \"{}\" is too \
                     large",
                    self.statement_call_name
                ),
            ));
        }
        Ok(())
    }

    /// Expands the calls in `expression`, the arguments of a call before the
    /// call. `expression` is at level `level` of the expression that takes
    /// the place of the call of the statement.
    ///
    /// The expansion of a body in a body uses the stack, and so does the
    /// walk of an expression. A level above the limit thus fails here, on
    /// the way down, and not only when the complete expression is known.
    fn expand_calls_in(
        &mut self,
        expression: &mut ast::Expr,
        level: usize,
    ) -> turso_core::Result<()> {
        if level > MAX_EXPR_DEPTH {
            return Err(stack_depth_limit_exceeded());
        }
        let mut is_expression = true;
        walk_expr_mut(
            expression,
            &mut |part: &mut ast::Expr| -> turso_core::Result<WalkControl> {
                if std::mem::take(&mut is_expression) {
                    return Ok(WalkControl::Continue);
                }
                self.expand_calls_in(part, level + 1)?;
                Ok(WalkControl::SkipChildren)
            },
        )?;
        if let Some(expansion) = self.expand(expression, level)? {
            *expression = expansion;
        }
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
    fn called_function(&self, name: &str) -> turso_core::Result<Option<&'a SqlFunction>> {
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
    is_deterministic: bool,
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
                is_deterministic: function.is_deterministic(),
            });
        }
        self.connection
            .get_syms_functions()
            .into_iter()
            .find(|(function_name, ..)| function_name.eq_ignore_ascii_case(name))
            .map(|(_, is_aggregate, _, is_deterministic)| BuiltInFunction {
                is_aggregate,
                is_deterministic,
            })
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

impl BuiltInFunctions<'_> {
    /// Whether two calls of the function that a call of `name` with
    /// `argument_count` arguments runs can give different results for the
    /// same arguments in one statement. The engine's flag for a function
    /// that is not deterministic says this, as `pg_proc.provolatile` shows.
    /// An aggregate or a window function is not volatile: PostgreSQL
    /// computes it one time where a query uses it. A function that tursopg
    /// does not have is volatile.
    fn is_volatile(&self, name: &str, argument_count: usize) -> bool {
        self.get(name, argument_count)
            .is_none_or(|function| !function.is_aggregate && !function.is_deterministic)
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

/// How many times PostgreSQL has each of the `parameter_count` parameters
/// of a function in its body `body`. Fails with PostgreSQL's error when the
/// body has a parameter that the function does not have.
fn parameter_use_counts(
    body: &ast::Expr,
    parameter_count: usize,
) -> turso_core::Result<Vec<usize>> {
    let mut use_counts = vec![0; parameter_count];
    add_parameter_uses(body, 1, &mut use_counts)?;
    Ok(use_counts)
}

/// Adds to `use_counts` the parameters of `expression`, which PostgreSQL has
/// `copies` times.
///
/// The parser of PostgreSQL writes `a BETWEEN b AND c` as
/// `a >= b AND a <= c`, and it can write `a IN (b, c)` as `a = b OR a = c`.
/// Thus PostgreSQL has these left sides more than once, where the expression
/// of tursopg has them one time.
fn add_parameter_uses(
    expression: &ast::Expr,
    copies: usize,
    use_counts: &mut [usize],
) -> turso_core::Result<()> {
    walk_expr(
        expression,
        &mut |part: &ast::Expr| -> turso_core::Result<WalkControl> {
            match part {
                ast::Expr::Variable(parameter) => {
                    // CREATE FUNCTION refuses a body that uses a parameter
                    // that the function does not have, unless
                    // check_function_bodies is off.
                    let position = parameter.index.get() as usize - 1;
                    let Some(use_count) = use_counts.get_mut(position) else {
                        return Err(sqlstate_error(
                            SQLSTATE_UNDEFINED_PARAMETER,
                            format!("there is no parameter ${}", parameter.index),
                        ));
                    };
                    *use_count = use_count.saturating_add(copies);
                    Ok(WalkControl::Continue)
                }
                ast::Expr::Between {
                    lhs, start, end, ..
                } => {
                    add_parameter_uses(lhs, copies.saturating_mul(2), use_counts)?;
                    add_parameter_uses(start, copies, use_counts)?;
                    add_parameter_uses(end, copies, use_counts)?;
                    Ok(WalkControl::SkipChildren)
                }
                ast::Expr::InList { lhs, rhs, .. } => {
                    let left_copies = copies.saturating_mul(in_list_left_copies(lhs, rhs));
                    add_parameter_uses(lhs, left_copies, use_counts)?;
                    for item in rhs {
                        add_parameter_uses(item, copies, use_counts)?;
                    }
                    Ok(WalkControl::SkipChildren)
                }
                _ => Ok(WalkControl::Continue),
            }
        },
    )?;
    Ok(())
}

/// How many times PostgreSQL has `left`, the left side of `IN`, when no item
/// of the list `items` has a column, as in the body of a function.
///
/// PostgreSQL makes one array of the items and compares `left` with this
/// array. It makes no array of rows: it compares a row with each item on its
/// own.
fn in_list_left_copies(left: &ast::Expr, items: &[Box<ast::Expr>]) -> usize {
    match left {
        ast::Expr::Parenthesized(columns) if columns.len() > 1 => items.len(),
        _ => 1,
    }
}

/// The expression of `inlined` with `arguments` in the place of the
/// parameters. Each parameter of the expression is a parameter of the
/// function, as [`parameter_use_counts`] makes sure.
fn with_arguments(inlined: InlinedFunction, arguments: &[Box<ast::Expr>]) -> ast::Expr {
    let mut expression = inlined.result;
    let _ = walk_expr_mut(
        &mut expression,
        &mut |expression: &mut ast::Expr| -> turso_core::Result<WalkControl> {
            let ast::Expr::Variable(parameter) = expression else {
                return Ok(WalkControl::Continue);
            };
            let position = parameter.index.get() as usize - 1;
            *expression = cast_argument(inlined.parameters[position].clone(), &arguments[position]);
            // A variable in the argument is a variable of the statement.
            Ok(WalkControl::SkipChildren)
        },
    );
    expression
}

/// Whether `argument` is a literal or a variable of the statement, also with
/// a sign, a cast or parentheses. The statement gives the same result and
/// the same errors with or without such an argument.
fn is_constant_or_variable(argument: &ast::Expr) -> bool {
    match argument {
        ast::Expr::Literal(_) | ast::Expr::Variable(_) => true,
        ast::Expr::Parenthesized(parts) => {
            matches!(parts.as_slice(), [part] if is_constant_or_variable(part))
        }
        ast::Expr::Unary(_, operand) => is_constant_or_variable(operand),
        ast::Expr::Cast { expr, .. } => is_constant_or_variable(expr),
        _ => false,
    }
}

/// Whether PostgreSQL computes `argument` when it plans the statement:
/// `argument` has no column, no variable of the statement, no subquery, and
/// no function or literal that gives a new result at each use.
fn is_constant(built_in_functions: BuiltInFunctions, argument: &ast::Expr) -> bool {
    !has_column_or_variable(argument)
        && !has_subquery_or_volatile_call(built_in_functions, argument)
}

/// Whether `expression` has a column or a variable of the statement. A
/// variable of the statement can be a parameter of a function, when
/// `CREATE FUNCTION` checks the body.
fn has_column_or_variable(expression: &ast::Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expression, &mut |part| {
        found |= is_column(part) || matches!(part, ast::Expr::Variable(_));
        Ok(WalkControl::Continue)
    });
    found
}

fn is_column(expression: &ast::Expr) -> bool {
    matches!(
        expression,
        ast::Expr::Id(_)
            | ast::Expr::Name(_)
            | ast::Expr::Qualified(..)
            | ast::Expr::DoublyQualified(..)
            | ast::Expr::Column { .. }
            | ast::Expr::RowId { .. }
    )
}

/// Whether `argument` has a subquery, or a function or a literal that can
/// give a different result at each use.
///
/// PostgreSQL has three classes of functions: IMMUTABLE, STABLE (one result
/// in a statement, such as `now()` and `CURRENT_DATE`) and VOLATILE. tursopg
/// only knows if a function is deterministic. A function that is not
/// deterministic is volatile here, also when PostgreSQL calls it STABLE,
/// because tursopg computes it again at each call.
fn has_subquery_or_volatile_call(
    built_in_functions: BuiltInFunctions,
    argument: &ast::Expr,
) -> bool {
    let mut found = false;
    let _ = walk_expr(argument, &mut |expression| {
        found |= match expression {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => true,
            ast::Expr::Literal(literal) => is_computed_at_each_use(literal),
            ast::Expr::FunctionCall { name, args, .. } => {
                built_in_functions.is_volatile(name.as_str(), args.len())
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                built_in_functions.is_volatile(name.as_str(), 0)
            }
            _ => false,
        };
        Ok(WalkControl::Continue)
    });
    found
}

/// Whether tursopg computes `literal` again at each use, so that two copies
/// of it can be different.
fn is_computed_at_each_use(literal: &ast::Literal) -> bool {
    match literal {
        ast::Literal::CurrentDate | ast::Literal::CurrentTime | ast::Literal::CurrentTimestamp => {
            true
        }
        ast::Literal::Numeric(_)
        | ast::Literal::String(_)
        | ast::Literal::Blob(_)
        | ast::Literal::Keyword(_)
        | ast::Literal::Null
        | ast::Literal::True
        | ast::Literal::False => false,
    }
}

/// Whether `argument` cannot fail when it is computed: it is a column, a
/// literal or a variable of the statement, also with a sign or parentheses.
/// An operator, a function and a cast can fail.
fn cannot_fail(argument: &ast::Expr) -> bool {
    match argument {
        ast::Expr::Literal(_) | ast::Expr::Variable(_) => true,
        // The translator writes `true` and `false` as a cast of 1 and 0.
        ast::Expr::Cast {
            expr,
            type_name: Some(type_name),
        } if type_name.name == "boolean" => {
            matches!(expr.as_ref(), ast::Expr::Literal(ast::Literal::Numeric(digit)) if digit == "0" || digit == "1")
        }
        ast::Expr::Parenthesized(parts) => parts.iter().all(|part| cannot_fail(part)),
        ast::Expr::Unary(ast::UnaryOperator::Negative | ast::UnaryOperator::Positive, operand) => {
            matches!(
                operand.as_ref(),
                ast::Expr::Literal(ast::Literal::Numeric(_))
            )
        }
        column => is_column(column),
    }
}

/// Whether the engine computes `argument` each time it computes
/// `expression`, which has copies of `argument`. The engine does not always
/// compute a branch of `CASE`, the right side of `AND` or `OR`, an item of a
/// list, or an argument of a function after the first one.
fn always_computes(expression: &ast::Expr, argument: &ast::Expr) -> bool {
    if expression == argument {
        return true;
    }
    match expression {
        ast::Expr::Parenthesized(parts) => parts
            .first()
            .is_some_and(|part| always_computes(part, argument)),
        ast::Expr::Unary(_, part)
        | ast::Expr::IsNull(part)
        | ast::Expr::NotNull(part)
        | ast::Expr::Cast { expr: part, .. } => always_computes(part, argument),
        ast::Expr::Binary(left, ast::Operator::And | ast::Operator::Or, _) => {
            always_computes(left, argument)
        }
        ast::Expr::Binary(left, _, right) => {
            always_computes(left, argument) || always_computes(right, argument)
        }
        ast::Expr::Between { lhs, .. }
        | ast::Expr::Like { lhs, .. }
        | ast::Expr::InList { lhs, .. } => always_computes(lhs, argument),
        ast::Expr::Case {
            base: Some(base), ..
        } => always_computes(base, argument),
        ast::Expr::Case {
            base: None,
            when_then_pairs,
            ..
        } => when_then_pairs
            .first()
            .is_some_and(|(condition, _)| always_computes(condition, argument)),
        ast::Expr::FunctionCall { args, .. } => args
            .first()
            .is_some_and(|first| always_computes(first, argument)),
        _ => false,
    }
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

/// Fails with PostgreSQL's errors when the body `expression` names a column.
/// A body that tursopg can call has no FROM clause, so it has no column.
/// The name must not get a column of the statement that calls the function.
fn check_body_has_no_column(expression: &ast::Expr) -> turso_core::Result<()> {
    walk_expr(expression, &mut |part: &ast::Expr| match part {
        ast::Expr::Id(column) | ast::Expr::Name(column) => Err(sqlstate_error(
            SQLSTATE_UNDEFINED_COLUMN,
            format!("column \"{}\" does not exist", column.as_str()),
        )),
        ast::Expr::Qualified(table, _) | ast::Expr::DoublyQualified(_, table, _) => {
            Err(sqlstate_error(
                SQLSTATE_UNDEFINED_TABLE,
                format!("missing FROM-clause entry for table \"{}\"", table.as_str()),
            ))
        }
        _ => Ok(WalkControl::Continue),
    })?;
    Ok(())
}

/// The engine error for an error in the body of a stored function.
fn body_error(error: turso_pg_parser::ParseError) -> LimboError {
    match error {
        turso_pg_parser::ParseError::Sqlstate { sqlstate, message } => {
            sqlstate_error(sqlstate, message)
        }
        other => LimboError::ParseError(other.to_string()),
    }
}

/// Fails with PostgreSQL's errors when one of `called_functions` names a
/// schema that does not exist, or a schema that does not have the function.
fn check_schemas_of_calls(
    connection: &Connection,
    functions: Option<&FunctionCatalog>,
    called_functions: &[turso_pg_parser::CalledFunction],
) -> turso_core::Result<()> {
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
            if is_user_function(connection, functions, &without_schema)
                && !is_built_in_function(connection, &called.name)
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
        if !is_user_function(connection, functions, called) {
            return Err(no_such_function());
        }
    }
    Ok(())
}

/// Whether `called` names a function that a user defined.
fn is_user_function(
    connection: &Connection,
    functions: Option<&FunctionCatalog>,
    called: &turso_pg_parser::CalledFunction,
) -> bool {
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

/// How many expressions the bodies of functions can add to the statement
/// for one call of the statement.
///
/// A body can call a function several times, and the body of that function
/// can do the same. The expression of a short chain of such functions has a
/// size that no statement text has: 7 calls in each of 7 bodies make 823543
/// calls.
const MAX_ADDED_EXPRESSIONS: usize = 10_000;

/// The number of expressions in `expression`, with `expression`.
fn size(expression: &ast::Expr) -> usize {
    let mut size = 0;
    let _ = walk_expr(expression, &mut |_| {
        size += 1;
        Ok(WalkControl::Continue)
    });
    size
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
