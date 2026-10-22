//! Inline a call to a SQL function into the statement that calls it.
//!
//! PostgreSQL inlines a SQL function whose body is one expression. Turso
//! does the same, and it is the only way Turso executes a function that
//! `CREATE FUNCTION` created: a call to any other function gives an error.

use turso_core::schema::Schema;
use turso_core::{walk_expr, walk_expr_mut, LimboError, Result, WalkControl};
use turso_parser::ast::{self, Expr};
use turso_pg_parser::function::{PgFunction, Volatility};
use turso_pg_parser::translator::cast_type_for_pg_type;

/// The expression that computes `name(args)`, or `Ok(None)` when no stored
/// function has that name.
pub(crate) fn expand_function_call(
    schema: &Schema,
    name: &str,
    args: &[Expr],
) -> Result<Option<Expr>> {
    let Some(stored) = schema.get_user_function(name, args.len()) else {
        if schema.user_functions_named(name).is_empty() {
            return Ok(None);
        }
        return Err(parse_error(format!(
            "function {name} with {} arguments does not exist",
            args.len()
        )));
    };
    let function = PgFunction::parse(&stored.sql).map_err(|e| parse_error(e.to_string()))?;
    if function.is_trigger() {
        return Err(parse_error(
            "trigger functions can only be called as triggers".to_string(),
        ));
    }
    if function.language != "sql" {
        return Err(parse_error(format!(
            "function {name} cannot be executed: only SQL functions with one expression are supported"
        )));
    }
    if function.returns_set {
        return Err(parse_error(format!(
            "function {name} cannot be executed: functions that return a set are not supported"
        )));
    }
    let Some(mut body) = function
        .single_expression()
        .map_err(|e| parse_error(e.to_string()))?
    else {
        return Err(parse_error(format!(
            "function {name} cannot be executed: only a body of one expression without FROM is supported"
        )));
    };
    if contains_subquery(&body) {
        return Err(parse_error(format!(
            "function {name} cannot be executed: a subquery in the body is not supported"
        )));
    }
    if expr_volatility(&body) > function.volatility {
        return Err(parse_error(format!(
            "function {name} cannot be executed: its body is more volatile than its declaration"
        )));
    }

    let mut uses = vec![0usize; args.len()];
    substitute_parameters(&mut body, &function, args, &mut uses)?;
    for (index, param) in function.params.iter().enumerate() {
        let occurrences = uses[index] + usize::from(function.strict);
        let argument = &args[index];
        if occurrences > 1
            && (contains_subquery(argument) || expr_volatility(argument) == Volatility::Volatile)
        {
            return Err(parse_error(format!(
                "function {name} cannot be executed: parameter {} is used more than once with a volatile argument",
                param.name.as_deref().unwrap_or("$1")
            )));
        }
    }

    let mut result = cast_to_return_type(body, &function);
    if function.strict && !args.is_empty() {
        result = Expr::Case {
            base: None,
            when_then_pairs: args
                .iter()
                .map(|argument| {
                    (
                        Box::new(Expr::IsNull(Box::new(argument.clone()))),
                        Box::new(Expr::Literal(ast::Literal::Null)),
                    )
                })
                .collect(),
            else_expr: Some(Box::new(result)),
        };
    }
    Ok(Some(result))
}

/// Replace each parameter in `body` with its argument and count the uses of
/// each parameter. A parameter is a bare name, `function.name`, or `$n`.
fn substitute_parameters(
    body: &mut Expr,
    function: &PgFunction,
    args: &[Expr],
    uses: &mut [usize],
) -> Result<()> {
    let parameter_index = |parameter: &str| {
        function
            .params
            .iter()
            .position(|param| param.name.as_deref() == Some(parameter))
    };
    walk_expr_mut(body, &mut |expr| {
        let index = match expr {
            Expr::Id(id) => parameter_index(id.as_str())
                .ok_or_else(|| parse_error(format!("column \"{}\" does not exist", id.as_str())))?,
            Expr::Qualified(table, column) => {
                if !table.as_str().eq_ignore_ascii_case(&function.name) {
                    return Err(parse_error(format!(
                        "missing FROM-clause entry for table \"{}\"",
                        table.as_str()
                    )));
                }
                parameter_index(column.as_str()).ok_or_else(|| {
                    parse_error(format!(
                        "column {}.{} does not exist",
                        table.as_str(),
                        column.as_str()
                    ))
                })?
            }
            Expr::Variable(variable) if variable.name.is_none() => {
                let number = variable.index.get() as usize;
                if number > args.len() {
                    return Err(parse_error(format!("there is no parameter ${number}")));
                }
                number - 1
            }
            _ => return Ok(WalkControl::Continue),
        };
        *expr = args[index].clone();
        uses[index] += 1;
        Ok(WalkControl::SkipChildren)
    })?;
    Ok(())
}

fn cast_to_return_type(body: Expr, function: &PgFunction) -> Expr {
    let Some(return_type) = function.return_type.as_deref() else {
        return body;
    };
    if return_type.eq_ignore_ascii_case("void") {
        return body;
    }
    let base = return_type.trim_end_matches("[]");
    let array_dimensions = ((return_type.len() - base.len()) / 2) as u32;
    Expr::Cast {
        expr: Box::new(body),
        type_name: Some(cast_type_for_pg_type(base, array_dimensions)),
    }
}

fn contains_subquery(expr: &Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |expr| {
        if matches!(
            expr,
            Expr::Subquery(_) | Expr::Exists(_) | Expr::InSelect { .. }
        ) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found
}

/// How much the value of an expression can change: the most volatile function
/// it calls decides.
fn expr_volatility(expr: &Expr) -> Volatility {
    let mut volatility = Volatility::Immutable;
    let _ = walk_expr(expr, &mut |expr| {
        let call = match expr {
            Expr::FunctionCall { name, .. } | Expr::FunctionCallStar { name, .. } => {
                function_volatility(name.as_str())
            }
            Expr::Literal(
                ast::Literal::CurrentDate
                | ast::Literal::CurrentTime
                | ast::Literal::CurrentTimestamp,
            ) => Volatility::Stable,
            _ => Volatility::Immutable,
        };
        volatility = volatility.max(call);
        Ok(WalkControl::Continue)
    });
    volatility
}

const VOLATILE_FUNCTIONS: &[&str] = &[
    "random",
    "randomblob",
    "gen_random_uuid",
    "uuid4",
    "uuid4_str",
    "uuid7",
    "uuid7_str",
    "nextval",
    "currval",
    "setval",
    "set_config",
    "changes",
    "total_changes",
    "last_insert_rowid",
    "clock_timestamp",
    "statement_timestamp",
    "load_extension",
];

const STABLE_FUNCTIONS: &[&str] = &[
    "now",
    "transaction_timestamp",
    "current_setting",
    "current_schema",
    "current_database",
    "version",
    "date",
    "time",
    "datetime",
    "julianday",
    "unixepoch",
    "strftime",
    "timediff",
    "pg_backend_pid",
    "pg_get_constraintdef",
    "pg_get_indexdef",
    "pg_is_in_recovery",
];

fn function_volatility(name: &str) -> Volatility {
    let name = name.to_ascii_lowercase();
    if VOLATILE_FUNCTIONS.contains(&name.as_str()) {
        Volatility::Volatile
    } else if STABLE_FUNCTIONS.contains(&name.as_str()) {
        Volatility::Stable
    } else {
        Volatility::Immutable
    }
}

fn parse_error(message: String) -> LimboError {
    LimboError::ParseError(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::schema::UserFunction;

    fn schema_with(functions: &[(&str, usize, &str)]) -> Schema {
        let mut schema = Schema::new();
        for (name, arg_count, sql) in functions {
            schema.add_user_function(UserFunction {
                name: name.to_string(),
                arg_count: *arg_count,
                sql: sql.to_string(),
            });
        }
        schema
    }

    fn expand(schema: &Schema, call: &str) -> Result<Option<String>> {
        let parsed = turso_pg_parser::parse(&format!("SELECT {call}")).unwrap();
        let stmt = turso_pg_parser::translator::PostgreSQLTranslator::new()
            .translate(&parsed)
            .unwrap();
        let ast::Stmt::Select(select) = stmt else {
            unreachable!("SELECT");
        };
        let ast::OneSelect::Select { columns, .. } = select.body.select else {
            unreachable!("SELECT");
        };
        let Some(ast::ResultColumn::Expr(expr, _)) = columns.into_iter().next() else {
            unreachable!("one column");
        };
        let Expr::FunctionCall { name, args, .. } = *expr else {
            unreachable!("a function call");
        };
        let args: Vec<Expr> = args.into_iter().map(|arg| *arg).collect();
        Ok(expand_function_call(schema, name.as_str(), &args)?.map(|expr| expr.to_string()))
    }

    #[test]
    fn inlines_a_body_of_one_expression() {
        let schema = schema_with(&[(
            "add_tax",
            1,
            "CREATE FUNCTION add_tax(price numeric) RETURNS numeric LANGUAGE sql AS 'SELECT price * 1.2'",
        )]);
        assert_eq!(
            expand(&schema, "add_tax(10)").unwrap().as_deref(),
            Some("CAST (10 * 1.2 AS REAL)")
        );
        assert_eq!(expand(&schema, "abs(10)").unwrap(), None);
    }

    #[test]
    fn strict_functions_test_each_argument_for_null() {
        let schema = schema_with(&[(
            "add",
            2,
            "CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql STRICT AS 'SELECT a + b'",
        )]);
        assert_eq!(
            expand(&schema, "add(x, 2)").unwrap().as_deref(),
            Some("CASE WHEN x ISNULL THEN NULL WHEN 2 ISNULL THEN NULL ELSE CAST (x + 2 AS INTEGER) END")
        );
    }

    #[test]
    fn parameters_by_number_and_by_qualified_name() {
        let schema = schema_with(&[
            (
                "dbl",
                1,
                "CREATE FUNCTION dbl(int) RETURNS int LANGUAGE sql AS 'SELECT $1 * 2'",
            ),
            (
                "neg",
                1,
                "CREATE FUNCTION neg(a int) RETURNS int RETURN -neg.a",
            ),
        ]);
        assert_eq!(
            expand(&schema, "dbl(21)").unwrap().as_deref(),
            Some("CAST (21 * 2 AS INTEGER)")
        );
        assert_eq!(
            expand(&schema, "neg(21)").unwrap().as_deref(),
            Some("CAST (- 21 AS INTEGER)")
        );
    }

    #[test]
    fn a_volatile_argument_cannot_be_used_twice() {
        let schema = schema_with(&[
            (
                "sq",
                1,
                "CREATE FUNCTION sq(a int) RETURNS int LANGUAGE sql AS 'SELECT a * a'",
            ),
            (
                "same",
                1,
                "CREATE FUNCTION same(a int) RETURNS int LANGUAGE sql STRICT AS 'SELECT a'",
            ),
        ]);
        assert_eq!(
            expand(&schema, "sq(3)").unwrap().as_deref(),
            Some("CAST (3 * 3 AS INTEGER)")
        );
        let err = expand(&schema, "sq(random())").unwrap_err();
        assert!(err.to_string().contains("used more than once"), "{err}");
        let err = expand(&schema, "same(random())").unwrap_err();
        assert!(err.to_string().contains("used more than once"), "{err}");
        let err = expand(&schema, "sq((SELECT 1))").unwrap_err();
        assert!(err.to_string().contains("used more than once"), "{err}");
    }

    #[test]
    fn a_body_more_volatile_than_the_declaration_is_an_error() {
        let schema = schema_with(&[(
            "r",
            0,
            "CREATE FUNCTION r() RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT random()'",
        )]);
        let err = expand(&schema, "r()").unwrap_err();
        assert!(err.to_string().contains("more volatile"), "{err}");
    }

    #[test]
    fn functions_turso_cannot_execute_are_errors() {
        let schema = schema_with(&[
            (
                "cnt",
                0,
                "CREATE FUNCTION cnt() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM pg_class'",
            ),
            (
                "trg",
                0,
                "CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'",
            ),
            (
                "unknown_column",
                1,
                "CREATE FUNCTION unknown_column(a int) RETURNS int LANGUAGE sql AS 'SELECT b'",
            ),
        ]);
        let err = expand(&schema, "cnt()").unwrap_err();
        assert!(err.to_string().contains("without FROM"), "{err}");
        let err = expand(&schema, "trg()").unwrap_err();
        assert!(
            err.to_string().contains("only be called as triggers"),
            "{err}"
        );
        let err = expand(&schema, "unknown_column(1)").unwrap_err();
        assert!(
            err.to_string().contains("column \"b\" does not exist"),
            "{err}"
        );
        let err = expand(&schema, "cnt(1)").unwrap_err();
        assert!(
            err.to_string().contains("with 1 arguments does not exist"),
            "{err}"
        );
    }
}
