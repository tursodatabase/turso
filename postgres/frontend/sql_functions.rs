//! Functions that users define with `CREATE FUNCTION ... LANGUAGE sql`.
//!
//! [`crate::function_catalog`] stores them. CREATE FUNCTION and DROP
//! FUNCTION are checked here, with what PostgreSQL checks, and then run as
//! the engine's statements of the same name, which the frontend's catalog
//! stores.
//!
//! The engine never calls such a function. The body is one SELECT of one
//! expression, and the engine asks the dialect, through
//! [`turso_core::Dialect::expand_function_call`], for the expression that a
//! call stands for: the body's expression with the arguments in place of
//! the parameters. PostgreSQL does the same with simple SQL functions. It
//! means that `user_id = auth.uid()` can use an index on `user_id`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use turso_core::ast;
use turso_core::dialect::resolve_builtin_function;
use turso_core::namespace::{NamespaceId, NamespaceRef, Qualifier};
use turso_core::schema::Schema;
use turso_core::security::roles::RoleId;
use turso_core::{
    walk_expr_mut, walk_expr_with_subqueries, Connection, Deterministic, Dialect, Func,
    FunctionScope, LimboError, WalkControl, MAIN_DB_ID,
};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DUPLICATE_FUNCTION, SQLSTATE_FEATURE_NOT_SUPPORTED,
    SQLSTATE_INVALID_FUNCTION_DEFINITION, SQLSTATE_INVALID_SCHEMA_NAME, SQLSTATE_UNDEFINED_COLUMN,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_PARAMETER,
};
use turso_pg_parser::translator::{
    parse_sql_function_definition, split_function_name, FunctionPlannerHints, FunctionVolatility,
    PgCreateFunctionStmt, PgDropFunctionStmt, PgFunctionTarget, SqlFunctionDefinition,
    SqlFunctionParameter,
};

use crate::error::sqlstate_error;
use crate::schema_state;

/// A function that a user defined in SQL.
#[derive(Debug)]
pub(crate) struct SqlFunction {
    pub(crate) oid: i64,
    pub(crate) name: String,
    pub(crate) namespace: NamespaceId,
    pub(crate) owner: RoleId,
    /// The number of parameters.
    pub(crate) argument_count: usize,
    /// The text of the CREATE FUNCTION statement.
    sql: String,
    definition: OnceLock<turso_core::Result<Definition>>,
}

/// What the stored statement of a function defines, read once from its
/// text.
#[derive(Debug, Clone)]
pub(crate) struct Definition {
    pub(crate) parameters: Vec<SqlFunctionParameter>,
    pub(crate) return_type: String,
    pub(crate) volatility: FunctionVolatility,
    /// The body as written.
    pub(crate) source: String,
    /// The expression of the body, with the variable `$n` wherever the body
    /// reads parameter n, by number or by name.
    body: ast::Expr,
    /// How many times the body reads each parameter.
    parameter_uses: Vec<usize>,
    pub(crate) hints: FunctionPlannerHints,
}

/// [`turso_core::Dialect::expand_function_call`] for the PostgreSQL
/// frontend.
pub(crate) fn expand_call(
    scope: &FunctionScope<'_>,
    name: &str,
    args: &[Box<ast::Expr>],
) -> turso_core::Result<Option<ast::Expr>> {
    let Some(function) = find_function(scope, name, args.len())? else {
        // The engine matches the names of its functions without regard to
        // case, and PostgreSQL matches them exactly. No function of tursopg
        // itself has an uppercase letter in its name, so a name with one
        // that no user defined names no function.
        let (schema_name, function_name) = split_function_name(name);
        if schema_name.is_some() && function_name.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(sqlstate_error(
                SQLSTATE_UNDEFINED_FUNCTION,
                format!("no such function: {name}"),
                None,
            ));
        }
        return Ok(None);
    };
    check_does_not_call_itself(scope, function)?;
    function
        .definition()?
        .expand(&function.name, args, &|name, argument_count| {
            returns_the_same_value_in_one_statement(scope, name, argument_count, 0)
        })
        .map(Some)
}

/// The function that a call of `name`, as the translated statement names
/// it, with `argument_count` arguments calls, or None when no function that
/// a user defined has that name.
fn find_function<'a>(
    scope: &'a FunctionScope<'_>,
    name: &str,
    argument_count: usize,
) -> turso_core::Result<Option<&'a Arc<SqlFunction>>> {
    let (schema_name, function_name) = split_function_name(name);
    let namespaces = match schema_name {
        Some(schema_name) => {
            match schema_namespace(schema_name, scope.qualifiers.get(schema_name)) {
                Ok(namespace) => vec![namespace],
                // tursopg has the functions of the auth schema built in.
                Err(_) if schema_name == crate::auth_functions::SCHEMA => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        None => search_namespaces(scope.search_order.iter().copied()),
    };
    let Some(state) = schema_state::schema_state(scope.schema) else {
        return Ok(None);
    };
    Ok(namespaces.into_iter().find_map(|namespace| {
        state
            .functions
            .get(namespace, function_name, argument_count)
    }))
}

/// The namespace of schema `name`, given `qualifier`, which is what looking
/// the name up found. The name of a database, like main, is not the name of
/// a schema.
fn schema_namespace(name: &str, qualifier: Option<Qualifier>) -> turso_core::Result<NamespaceId> {
    match qualifier {
        Some(qualifier) if !qualifier.is_database() && qualifier.database_id() == MAIN_DB_ID => {
            Ok(qualifier.target.id)
        }
        _ => Err(sqlstate_error(
            SQLSTATE_INVALID_SCHEMA_NAME,
            format!("schema \"{name}\" does not exist"),
            None,
        )),
    }
}

/// The schemas that a function name without a schema is looked up in, first
/// to last, given the namespaces of the search path.
fn search_namespaces(search_order: impl IntoIterator<Item = NamespaceRef>) -> Vec<NamespaceId> {
    search_order
        .into_iter()
        .filter(|namespace| namespace.database_id == MAIN_DB_ID && !namespace.id.is_root())
        .map(|namespace| namespace.id)
        .collect()
}

/// How deeply functions may call each other, a call in the body of a
/// function that a call reaches being one level deeper.
const MAX_CALL_DEPTH: usize = 32;

/// How many subqueries the bodies of the functions that a call reaches, one
/// calling the next, may have together. The engine compiles each subquery
/// inside the compilation of the statement, which has subqueries of its
/// own, so subqueries nested too deeply would run out of stack.
const MAX_SUBQUERIES_IN_CALLED_BODIES: usize = 8;

/// Fails when a call of `function` would expand into a call of a function
/// that is being expanded already, through the calls in the bodies of the
/// functions that it reaches, subqueries included, or when those calls nest
/// too deeply. PostgreSQL defines a function that calls itself and fails
/// when it is called. The engine stops an expansion that nests too deeply,
/// but it compiles a subquery of an expansion on its own, and would expand
/// a call that reaches itself through one until it ran out of stack.
fn check_does_not_call_itself(
    scope: &FunctionScope<'_>,
    function: &Arc<SqlFunction>,
) -> turso_core::Result<()> {
    let mut calls = CallChecker {
        scope,
        called: &function.name,
        calling: vec![function.oid],
        checked: HashMap::new(),
    };
    calls.check(function, 0)
}

/// Follows the calls that a call of function `called` reaches.
struct CallChecker<'a> {
    scope: &'a FunctionScope<'a>,
    called: &'a str,
    /// The oids of the functions whose bodies lead to the function being
    /// checked, which is last.
    calling: Vec<i64>,
    /// For each function whose calls are known not to lead back to a
    /// function of `calling`, how deep it was and how many subqueries the
    /// bodies before it had when it was checked.
    checked: HashMap<i64, (usize, usize)>,
}

impl CallChecker<'_> {
    /// Checks the calls in the body of `function`, after bodies that have
    /// `subqueries_before` subqueries.
    fn check(
        &mut self,
        function: &SqlFunction,
        subqueries_before: usize,
    ) -> turso_core::Result<()> {
        let body = BodyCalls::of(self.scope, function)?;
        let subqueries = subqueries_before + body.subqueries;
        if self.calling.len() > MAX_CALL_DEPTH {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "function {} calls functions more than {MAX_CALL_DEPTH} levels deep, \
                     which is not supported",
                    self.called
                ),
                None,
            ));
        }
        if subqueries > MAX_SUBQUERIES_IN_CALLED_BODIES {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "function {} calls functions with more than \
                     {MAX_SUBQUERIES_IN_CALLED_BODIES} subqueries in their bodies, \
                     which is not supported",
                    self.called
                ),
                None,
            ));
        }
        for called in body.functions {
            if self.calling.contains(&called.oid) {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "function {} calls itself, which is not supported",
                        called.name
                    ),
                    None,
                ));
            }
            let depth = self.calling.len() + 1;
            if self
                .checked
                .get(&called.oid)
                .is_some_and(|&(checked_depth, checked_subqueries)| {
                    depth <= checked_depth && subqueries <= checked_subqueries
                })
            {
                continue;
            }
            self.calling.push(called.oid);
            self.check(&called, subqueries)?;
            self.calling.pop();
            self.checked.insert(called.oid, (depth, subqueries));
        }
        Ok(())
    }
}

/// What the body of a function calls.
struct BodyCalls {
    /// The functions that users defined that it calls, subqueries included.
    functions: Vec<Arc<SqlFunction>>,
    /// How many subqueries it has, at least as many as are nested in it.
    subqueries: usize,
}

impl BodyCalls {
    fn of(scope: &FunctionScope<'_>, function: &SqlFunction) -> turso_core::Result<Self> {
        let mut calls = Self {
            functions: Vec::new(),
            subqueries: 0,
        };
        walk_expr_with_subqueries(&function.definition()?.body, &mut |expr| {
            match expr {
                ast::Expr::FunctionCall { name, args, .. } => {
                    // A call of a function of a schema that does not exist
                    // fails when it is expanded.
                    if let Ok(Some(function)) = find_function(scope, name.as_str(), args.len()) {
                        calls.functions.push(Arc::clone(function));
                    }
                }
                ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                    calls.subqueries += 1;
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(calls)
    }
}

/// Whether two calls of function `name` with the same arguments in one
/// statement are known to return the same value: a built-in function that
/// always does, or a function that a user defined whose definition says it
/// is not VOLATILE or whose body calls only such functions. `depth` is the
/// number of function bodies around the call.
fn returns_the_same_value_in_one_statement(
    scope: &FunctionScope<'_>,
    name: &str,
    argument_count: usize,
    depth: usize,
) -> bool {
    /// Functions that call each other in a circle would be followed forever.
    const MAX_DEPTH: usize = 32;

    if let Ok(Some(function)) = resolve_builtin_function(name, argument_count) {
        return function.is_deterministic();
    }
    let Ok(Some(function)) = find_function(scope, name, argument_count) else {
        return false;
    };
    let Ok(definition) = function.definition() else {
        return false;
    };
    definition.volatility != FunctionVolatility::Volatile
        || (depth < MAX_DEPTH
            && gives_the_same_value_twice(&definition.body, &|name, argument_count| {
                returns_the_same_value_in_one_statement(scope, name, argument_count, depth + 1)
            }))
}

/// Checks a CREATE FUNCTION statement as PostgreSQL does, and returns the
/// engine's statement that stores the function in the schema that it goes
/// in.
pub(crate) fn create_statement(
    conn: &Arc<Connection>,
    stmt: &PgCreateFunctionStmt,
) -> turso_core::Result<ast::Stmt> {
    let (schema_name, namespace) = creation_namespace(conn, stmt.schema.as_deref())?;
    let definition = Definition::new(stmt.definition.clone())?;
    check_name_is_not_built_in(conn, &stmt.name, definition.parameters.len())?;
    definition.check_body_compiles(conn, &schema_name, &stmt.name)?;

    let argument_count = definition.parameters.len();
    let schema = conn.current_schema();
    let existing = schema_state::schema_state(&schema)
        .and_then(|state| state.functions.get(namespace, &stmt.name, argument_count));
    if let Some(existing) = existing {
        let existing_definition = existing.definition()?;
        let same_argument_types = existing_definition
            .parameters
            .iter()
            .map(|parameter| &parameter.type_name)
            .eq(definition.parameters.iter().map(|p| &p.type_name));
        if !same_argument_types {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "function {} already exists with other argument types; \
                     two functions of a schema with the same name and number of arguments \
                     are not supported",
                    stmt.name
                ),
                None,
            ));
        }
        if !stmt.or_replace {
            return Err(sqlstate_error(
                SQLSTATE_DUPLICATE_FUNCTION,
                format!(
                    "function \"{}\" already exists with same argument types",
                    stmt.name
                ),
                None,
            ));
        }
        if existing_definition.return_type != definition.return_type {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_FUNCTION_DEFINITION,
                "cannot change return type of existing function",
                None,
            ));
        }
    }
    Ok(ast::Stmt::CreateFunction {
        or_replace: stmt.or_replace,
        name: qualified_name(&schema_name, &stmt.name),
        argument_count,
        sql: stmt.sql.clone(),
    })
}

/// The name of function `name` of schema `schema`, as the engine's
/// statements name it.
fn qualified_name(schema: &str, name: &str) -> ast::QualifiedName {
    ast::QualifiedName::fullname(
        ast::Name::exact(schema.to_string()),
        ast::Name::exact(name.to_string()),
    )
}

/// The namespace of the schema of function `name`, as the engine's
/// statements name it: the schema that the name has, or else the first
/// schema of the search path.
pub(crate) fn function_namespace(
    conn: &Connection,
    name: &ast::QualifiedName,
) -> turso_core::Result<NamespaceId> {
    let schema = name.namespace.as_ref().map(|schema| schema.as_str());
    creation_namespace(conn, schema).map(|(_, namespace)| namespace)
}

/// The name of the schema that CREATE FUNCTION makes a function in: the
/// one the statement names, or else the first schema of the search path.
pub(crate) fn creation_schema_name(
    conn: &Connection,
    schema: Option<&str>,
) -> turso_core::Result<String> {
    creation_namespace(conn, schema).map(|(name, _)| name)
}

fn creation_namespace(
    conn: &Connection,
    schema: Option<&str>,
) -> turso_core::Result<(String, NamespaceId)> {
    let qualifier = match schema {
        Some(schema) => {
            let namespace = schema_namespace(schema, conn.qualifier(schema))?;
            return Ok((schema.to_string(), namespace));
        }
        None => conn.search_order().into_iter().next().ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                "no schema has been selected to create in",
                None,
            )
        })?,
    };
    let name = qualifier.name.clone();
    let namespace = schema_namespace(&name, Some(qualifier))?;
    Ok((name, namespace))
}

/// Fails when a call of function `name` with `argument_count` arguments
/// would call a function that tursopg has, which a call without a schema
/// finds first. PostgreSQL would call the function a user defined where it
/// has no such function itself, as for `hex()`, and the `::jsonb` cast of
/// tursopg calls a function of its own.
fn check_name_is_not_built_in(
    conn: &Connection,
    name: &str,
    argument_count: usize,
) -> turso_core::Result<()> {
    let is_built_in = !matches!(
        crate::catalog::PostgresDialect.resolve_function(name, argument_count),
        Ok(None)
    ) || conn
        .syms
        .read()
        .resolve_function(name, argument_count)
        .is_some();
    if is_built_in {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "a function with the name and number of parameters of the built-in \
                 function {name} is not supported"
            ),
            None,
        ));
    }
    Ok(())
}

/// Looks up the functions of a DROP FUNCTION statement as PostgreSQL does,
/// and returns the engine's statement that deletes them, or None when IF
/// EXISTS skips every one.
pub(crate) fn drop_statement(
    conn: &Arc<Connection>,
    stmt: &PgDropFunctionStmt,
) -> turso_core::Result<Option<ast::Stmt>> {
    let schema = conn.current_schema();
    let mut functions = Vec::new();
    for target in &stmt.functions {
        match find_named_function(conn, &schema, target, stmt.if_exists)? {
            Some(function) => {
                let schema_name = schema
                    .namespace_name(function.namespace)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "function {} is in a schema without a name",
                            function.name
                        ))
                    })?;
                let argument_types = function
                    .definition()?
                    .parameters
                    .iter()
                    .map(|parameter| parameter.type_name.clone())
                    .collect();
                functions.push(ast::FunctionSignature {
                    name: qualified_name(&schema_name, &function.name),
                    argument_types: Some(argument_types),
                });
            }
            None if stmt.if_exists => {}
            None => return Err(undefined_function(target)),
        }
    }
    Ok((!functions.is_empty()).then_some(ast::Stmt::DropFunction {
        if_exists: stmt.if_exists,
        functions,
    }))
}

/// The error for `target`, a function that does not exist.
pub(crate) fn undefined_function(target: &PgFunctionTarget) -> LimboError {
    let message = match &target.argument_types {
        Some(types) => format!(
            "function {}({}) does not exist",
            target.name,
            types.join(", ")
        ),
        None => format!("could not find a function named \"{}\"", target.name),
    };
    sqlstate_error(SQLSTATE_UNDEFINED_FUNCTION, message, None)
}

/// The function that `target` names, which DROP FUNCTION or COMMENT ON
/// FUNCTION finds as PostgreSQL does, or None when there is none.
/// `if_exists` makes a schema that does not exist find no function.
pub(crate) fn find_named_function(
    conn: &Connection,
    schema: &Schema,
    target: &PgFunctionTarget,
    if_exists: bool,
) -> turso_core::Result<Option<Arc<SqlFunction>>> {
    let Some(state) = schema_state::schema_state(schema) else {
        return Ok(None);
    };
    let namespaces = match &target.schema {
        Some(name) => match schema_namespace(name, conn.qualifier(name)) {
            Ok(namespace) => vec![namespace],
            // PostgreSQL does not report a missing schema for DROP FUNCTION
            // IF EXISTS.
            Err(_) if if_exists => return Ok(None),
            Err(error) => return Err(error),
        },
        None => search_namespaces(
            conn.search_order()
                .into_iter()
                .map(|qualifier| qualifier.target),
        ),
    };
    for namespace in namespaces {
        let mut functions = state
            .functions
            .iter()
            .filter(|function| function.namespace == namespace && function.name == target.name)
            .filter(|function| {
                target
                    .argument_types
                    .as_ref()
                    .is_none_or(|types| function.argument_count == types.len())
            });
        let Some(function) = functions.next() else {
            continue;
        };
        if functions.next().is_some() {
            return Err(sqlstate_error(
                "42725",
                format!("function name \"{}\" is not unique", target.name),
                None,
            ));
        }
        return Ok(Some(Arc::clone(function)));
    }
    Ok(None)
}

pub(crate) fn schema_has_functions(conn: &Connection, namespace: NamespaceId) -> bool {
    let schema = conn.current_schema();
    schema_state::schema_state(&schema)
        .is_some_and(|state| state.functions.in_namespace(namespace).next().is_some())
}

impl SqlFunction {
    /// The function with `oid`, whose CREATE FUNCTION statement is `sql`.
    pub(crate) fn new(
        oid: i64,
        name: String,
        namespace: NamespaceId,
        owner: RoleId,
        argument_count: usize,
        sql: String,
    ) -> Self {
        Self {
            oid,
            name,
            namespace,
            owner,
            argument_count,
            sql,
            definition: OnceLock::new(),
        }
    }

    /// What the function's stored statement defines.
    pub(crate) fn definition(&self) -> turso_core::Result<&Definition> {
        self.definition
            .get_or_init(|| {
                let parsed = parse_sql_function_definition(&self.sql).map_err(|error| {
                    LimboError::Corrupt(format!(
                        "the stored definition of function {} cannot be read: {error}",
                        self.name
                    ))
                })?;
                Definition::new(parsed)
            })
            .as_ref()
            .map_err(Clone::clone)
    }
}

impl Definition {
    /// Checks the definition that a CREATE FUNCTION statement gives and
    /// numbers the parameters that the body reads by name.
    fn new(parsed: SqlFunctionDefinition) -> turso_core::Result<Self> {
        let SqlFunctionDefinition {
            parameters,
            return_type,
            volatility,
            source,
            mut body,
            hints,
        } = parsed;
        let mut parameter_uses = vec![0; parameters.len()];
        let mut has_subquery = false;
        walk_expr_mut(&mut body, &mut |expr| {
            match expr {
                ast::Expr::Id(name) => {
                    let Some(position) = parameters
                        .iter()
                        .position(|parameter| parameter.name.as_deref() == Some(name.as_str()))
                    else {
                        return Err(sqlstate_error(
                            SQLSTATE_UNDEFINED_COLUMN,
                            format!("column \"{}\" does not exist", name.as_str()),
                            None,
                        ));
                    };
                    parameter_uses[position] += 1;
                    *expr = parameter_variable(position);
                }
                ast::Expr::Variable(variable) => {
                    let number = variable.index.get() as usize;
                    if number > parameters.len() {
                        return Err(sqlstate_error(
                            SQLSTATE_UNDEFINED_PARAMETER,
                            format!("there is no parameter ${number}"),
                            None,
                        ));
                    }
                    parameter_uses[number - 1] += 1;
                }
                ast::Expr::Qualified(..) | ast::Expr::DoublyQualified(..) => {
                    return Err(unsupported(
                        "a name with a qualifier outside a subquery in a function body",
                    ));
                }
                ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                    has_subquery = true;
                }
                ast::Expr::FunctionCallStar { .. } => {
                    return Err(unsupported("an aggregate function in a function body"));
                }
                ast::Expr::FunctionCall {
                    name,
                    args,
                    filter_over,
                    ..
                } => {
                    let is_aggregate_or_window = filter_over.over_clause.is_some()
                        || matches!(
                            resolve_builtin_function(name.as_str(), args.len()),
                            Ok(Some(Func::Agg(_) | Func::Window(_)))
                        );
                    if is_aggregate_or_window {
                        return Err(unsupported(
                            "an aggregate or window function in a function body",
                        ));
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        // The arguments of a call are written by the caller and can name the
        // caller's tables. Put inside a subquery of the body, such a name
        // could mean a table of the subquery instead.
        if has_subquery && !parameters.is_empty() {
            return Err(unsupported(
                "a subquery in the body of a function with parameters",
            ));
        }
        Ok(Self {
            parameters,
            return_type,
            volatility,
            source,
            body,
            parameter_uses,
            hints,
        })
    }

    /// The expression that a call of the function, named `name`, with
    /// `args` stands for. `is_repeatable(function, argument_count)` says
    /// whether a function that an argument calls returns the same value
    /// every time in one statement.
    fn expand(
        &self,
        name: &str,
        args: &[Box<ast::Expr>],
        is_repeatable: &dyn Fn(&str, usize) -> bool,
    ) -> turso_core::Result<ast::Expr> {
        assert_eq!(
            args.len(),
            self.parameters.len(),
            "function {name} is looked up by its number of arguments"
        );
        // The expansion computes an argument once for each time the body
        // reads its parameter, where PostgreSQL computes every argument
        // once. That is the same value unless the argument can give a
        // different value each time.
        for (position, arg) in args.iter().enumerate() {
            if self.parameter_uses[position] > 1 && !gives_the_same_value_twice(arg, is_repeatable)
            {
                return Err(unsupported(format!(
                    "function {name} reads parameter {} more than once, and an argument \
                     for it that can give a different value each time it is computed",
                    position + 1
                )));
            }
        }
        let mut expansion = self.body.clone();
        walk_expr_mut(&mut expansion, &mut |expr| {
            let ast::Expr::Variable(variable) = expr else {
                return Ok(WalkControl::Continue);
            };
            let position = variable.index.get() as usize - 1;
            let mut value = self.parameters[position].value.clone();
            walk_expr_mut(&mut value, &mut |expr| {
                if matches!(expr, ast::Expr::Variable(_)) {
                    *expr = (*args[position]).clone();
                    // A variable inside the argument is a parameter of the
                    // calling statement, not of the function.
                    return Ok(WalkControl::SkipChildren);
                }
                Ok(WalkControl::Continue)
            })?;
            *expr = value;
            Ok(WalkControl::SkipChildren)
        })?;
        Ok(expansion)
    }

    /// Compiles the body with NULL for every argument, so that CREATE
    /// FUNCTION reports a body that names a function or table that does not
    /// exist, as PostgreSQL does. `schema_name` and `name` name the function
    /// that is being defined.
    fn check_body_compiles(
        &self,
        conn: &Arc<Connection>,
        schema_name: &str,
        name: &str,
    ) -> turso_core::Result<()> {
        self.check_does_not_call_itself(conn, schema_name, name)?;
        let null_args: Vec<_> = self
            .parameters
            .iter()
            .map(|_| Box::new(ast::Expr::Literal(ast::Literal::Null)))
            .collect();
        let select = ast::Stmt::Select(ast::Select {
            with: None,
            body: ast::SelectBody {
                select: ast::OneSelect::Select {
                    distinctness: None,
                    columns: vec![ast::ResultColumn::Expr(
                        Box::new(self.expand(name, &null_args, &|_, _| true)?),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        });
        let input = select.to_string();
        conn.prepare_translated_stmt(select, &input)?;
        Ok(())
    }

    /// A call is expanded into the body, so a body that calls its own
    /// function would expand forever.
    fn check_does_not_call_itself(
        &self,
        conn: &Connection,
        schema_name: &str,
        name: &str,
    ) -> turso_core::Result<()> {
        let first_schema_of_search_path = conn
            .search_order()
            .into_iter()
            .next()
            .map(|namespace| namespace.name);
        let mut body = self.body.clone();
        walk_expr_mut(&mut body, &mut |expr| {
            if let ast::Expr::FunctionCall {
                name: called, args, ..
            } = expr
            {
                let calls_itself = args.len() == self.parameters.len()
                    && match split_function_name(called.as_str()) {
                        (Some(schema), called) => schema == schema_name && called == name,
                        (None, called) => {
                            called == name
                                && first_schema_of_search_path.as_deref() == Some(schema_name)
                        }
                    };
                if calls_itself {
                    return Err(unsupported("a function that calls itself"));
                }
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(())
    }
}

fn parameter_variable(position: usize) -> ast::Expr {
    let number = u32::try_from(position + 1).expect("a function has few parameters");
    ast::Expr::Variable(ast::Variable::indexed(
        std::num::NonZeroU32::new(number).expect("parameter numbers start at one"),
    ))
}

/// Whether computing `expr` twice in one statement gives the same value
/// both times. A subquery is not looked into, and counts as an expression
/// that might not.
fn gives_the_same_value_twice(
    expr: &ast::Expr,
    is_repeatable: &dyn Fn(&str, usize) -> bool,
) -> bool {
    let mut expr = expr.clone();
    let mut same = true;
    let walked = walk_expr_mut(&mut expr, &mut |expr| {
        match expr {
            ast::Expr::Subquery(_)
            | ast::Expr::Exists(_)
            | ast::Expr::InSelect { .. }
            | ast::Expr::FunctionCallStar { .. } => same = false,
            ast::Expr::FunctionCall { name, args, .. } => {
                same &= is_repeatable(name.as_str(), args.len());
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    walked.is_ok() && same
}

fn unsupported(what: impl std::fmt::Display) -> LimboError {
    sqlstate_error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        format!("{what} is not supported"),
        None,
    )
}
