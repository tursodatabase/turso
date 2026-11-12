//! Functions that users define with `CREATE FUNCTION ... LANGUAGE sql`.
//!
//! A function is stored as one row of the `__turso_internal_functions` table
//! of the main database: its name, schema, owner and the text of the CREATE
//! FUNCTION statement, as `sqlite_schema` stores the text of CREATE TABLE.
//! The rows are part of the frontend's schema state,
//! [`crate::schema_state`], so the engine loads them with the schema, a
//! CREATE FUNCTION that is rolled back leaves no function behind, and other
//! connections see a function once the transaction that made it commits.
//!
//! The engine never calls such a function. The body is one SELECT of one
//! expression, and the engine asks the dialect, through
//! [`turso_core::Dialect::expand_function_call`], for the expression that a
//! call stands for: the body's expression with the arguments in place of
//! the parameters. PostgreSQL does the same with simple SQL functions. It
//! means that `user_id = auth.uid()` can use an index on `user_id`.
//!
//! A function has an oid from the same counter as relations, so no function
//! and relation share one.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use turso_core::dialect::engine::resolve_builtin_function;
use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::security::roles::RoleId;
use turso_core::{
    walk_expr_mut, Connection, Deterministic, Func, FunctionScope, LimboError, Numeric, Value,
    WalkControl, MAIN_DB_ID,
};
use turso_parser::ast;
use turso_pg_parser::sqlstate::{
    SQLSTATE_DUPLICATE_FUNCTION, SQLSTATE_FEATURE_NOT_SUPPORTED,
    SQLSTATE_INVALID_FUNCTION_DEFINITION, SQLSTATE_INVALID_SCHEMA_NAME, SQLSTATE_UNDEFINED_COLUMN,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_PARAMETER,
};
use turso_pg_parser::translator::{
    parse_sql_function_definition, split_function_name, FunctionPlannerHints, FunctionVolatility,
    PgCreateFunctionStmt, PgDropFunctionStmt, PgDropFunctionTarget, SqlFunctionDefinition,
    SqlFunctionParameter,
};

use crate::classes::{self, CLASSES_TABLE_NAME, FIRST_NORMAL_OBJECT_ID};
use crate::error::sqlstate_error;
use crate::schema_state;

pub(crate) const FUNCTIONS_TABLE_NAME: &str = "__turso_internal_functions";

/// `sql` is the text of the CREATE FUNCTION statement. `pronargs` is the
/// number of parameters: a schema has one function of a name for each
/// number of parameters.
const CREATE_FUNCTIONS_TABLE_SQL: &str = "CREATE TABLE __turso_internal_functions (\
    oid INTEGER PRIMARY KEY, \
    proname TEXT NOT NULL, \
    pronamespace INTEGER NOT NULL, \
    pronargs INTEGER NOT NULL, \
    proowner INTEGER NOT NULL, \
    sql TEXT NOT NULL, \
    UNIQUE (pronamespace, proname, pronargs))";

/// The columns that [`FunctionCatalog::new`] reads from the functions table.
const SELECT_COLUMNS: &str = "oid, proname, pronamespace, pronargs, proowner, sql";

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

/// The functions of a database.
#[derive(Debug, Clone, Default)]
pub(crate) struct FunctionCatalog {
    by_oid: BTreeMap<i64, Arc<SqlFunction>>,
    oids_by_name: HashMap<(NamespaceId, String, usize), i64>,
}

/// A step of a statement that changes the function catalog.
pub(crate) enum CatalogCommand {
    /// Engine SQL that writes the functions table.
    Write(String),
    /// Read the functions table into the connection's in-memory catalog.
    Reload,
}

/// Creates the functions table if the database has none: a new database, or
/// one that an earlier tursopg made. A read-only open cannot create it, and
/// such a database has no functions.
pub(crate) fn ensure_functions_table(
    db: &Arc<turso_core::Database>,
    read_only: bool,
) -> crate::Result<()> {
    let conn = db.connect(turso_core::ConnectOpts::new(
        turso_core::SessionRole::Superuser,
    ))?;
    if read_only || has_table(&conn.current_schema()) {
        return Ok(());
    }
    classes::in_write_transaction(&conn, create_functions_table)?;
    conn.reload_dialect_schema().map_err(Into::into)
}

/// Another process may have created the table first, so it is checked again
/// inside the write transaction.
fn create_functions_table(conn: &Arc<Connection>) -> crate::Result<()> {
    if has_table(&conn.current_schema()) {
        return Ok(());
    }
    classes::run_internal(conn, CREATE_FUNCTIONS_TABLE_SQL)
}

pub(crate) fn has_table(schema: &Schema) -> bool {
    schema
        .get_btree_table(NamespaceId::ROOT, FUNCTIONS_TABLE_NAME)
        .is_some()
}

/// The statement that reads the functions into the schema state, None for a
/// database without the functions table.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    has_table(schema).then(select_sql)
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
            ));
        }
        return Ok(None);
    };
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
    let namespaces: Vec<NamespaceId> = match schema_name {
        Some(schema_name) => {
            // The name of a database, like main, is not the name of a schema.
            let namespace = scope
                .qualifiers
                .get(schema_name)
                .filter(|namespace| !namespace.is_database());
            let Some(namespace) = namespace else {
                if schema_name == crate::auth_functions::SCHEMA {
                    return Ok(None);
                }
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_SCHEMA_NAME,
                    format!("schema \"{schema_name}\" does not exist"),
                ));
            };
            vec![namespace.target]
        }
        None => scope.search_order.to_vec(),
    }
    .into_iter()
    .filter(|namespace| namespace.database_id == MAIN_DB_ID)
    .map(|namespace| namespace.id)
    .collect();
    let Some(state) = schema_state::schema_state(scope.schema) else {
        return Ok(None);
    };
    Ok(namespaces.into_iter().find_map(|namespace| {
        state
            .functions
            .get(namespace, function_name, argument_count)
    }))
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

/// The steps that run a CREATE FUNCTION statement.
pub(crate) fn create_commands(
    conn: &Arc<Connection>,
    stmt: &PgCreateFunctionStmt,
) -> turso_core::Result<Vec<CatalogCommand>> {
    let (schema_name, namespace) = creation_namespace(conn, stmt.schema.as_deref())?;
    let definition = Definition::new(stmt.definition.clone())?;
    definition.check_body_compiles(conn, &schema_name, &stmt.name)?;

    let schema = conn.current_schema();
    let state = schema_state::schema_state(&schema).ok_or_else(|| {
        LimboError::ParseError("the database has no function catalog".to_string())
    })?;
    let argument_count = definition.parameters.len();
    let sql = sql_text(&stmt.sql);
    let Some(existing) = state.functions.get(namespace, &stmt.name, argument_count) else {
        let name = sql_text(&stmt.name);
        let owner = conn.current_role().get();
        return Ok(vec![
            CatalogCommand::Write(format!(
                "UPDATE sqlite_sequence SET seq = seq + 1 WHERE name = '{CLASSES_TABLE_NAME}'"
            )),
            CatalogCommand::Write(format!(
                "INSERT INTO {FUNCTIONS_TABLE_NAME} \
                 (oid, proname, pronamespace, pronargs, proowner, sql) \
                 SELECT seq, {name}, {}, {argument_count}, {owner}, {sql} \
                 FROM sqlite_sequence WHERE name = '{CLASSES_TABLE_NAME}'",
                namespace.as_u32()
            )),
            CatalogCommand::Reload,
        ]);
    };

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
        ));
    }
    if !stmt.or_replace {
        return Err(sqlstate_error(
            SQLSTATE_DUPLICATE_FUNCTION,
            format!(
                "function \"{}\" already exists with same argument types",
                stmt.name
            ),
        ));
    }
    if existing_definition.return_type != definition.return_type {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_FUNCTION_DEFINITION,
            "cannot change return type of existing function",
        ));
    }
    Ok(vec![
        CatalogCommand::Write(format!(
            "UPDATE {FUNCTIONS_TABLE_NAME} SET sql = {sql} WHERE oid = {}",
            existing.oid
        )),
        CatalogCommand::Reload,
    ])
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
    let namespace = match schema {
        Some(schema) => conn.qualifier(schema).ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                format!("schema \"{schema}\" does not exist"),
            )
        })?,
        None => conn.search_order().into_iter().next().ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                "no schema has been selected to create in",
            )
        })?,
    };
    if namespace.is_database() || namespace.database_id() != MAIN_DB_ID {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_SCHEMA_NAME,
            format!("schema \"{}\" does not exist", namespace.name),
        ));
    }
    Ok((namespace.name, namespace.target.id))
}

/// The steps that run a DROP FUNCTION statement.
pub(crate) fn drop_commands(
    conn: &Arc<Connection>,
    stmt: &PgDropFunctionStmt,
) -> turso_core::Result<Vec<CatalogCommand>> {
    let schema = conn.current_schema();
    let mut commands = Vec::new();
    for target in &stmt.functions {
        match function_to_drop(conn, &schema, target)? {
            Some(function) => commands.push(CatalogCommand::Write(format!(
                "DELETE FROM {FUNCTIONS_TABLE_NAME} WHERE oid = {}",
                function.oid
            ))),
            None if stmt.if_exists => {}
            None => {
                return Err(sqlstate_error(
                    SQLSTATE_UNDEFINED_FUNCTION,
                    match &target.argument_types {
                        Some(types) => format!(
                            "function {}({}) does not exist",
                            target.name,
                            types.join(", ")
                        ),
                        None => format!("could not find a function named \"{}\"", target.name),
                    },
                ));
            }
        }
    }
    if !commands.is_empty() {
        commands.push(CatalogCommand::Reload);
    }
    Ok(commands)
}

fn function_to_drop(
    conn: &Connection,
    schema: &Schema,
    target: &PgDropFunctionTarget,
) -> turso_core::Result<Option<Arc<SqlFunction>>> {
    let Some(state) = schema_state::schema_state(schema) else {
        return Ok(None);
    };
    let namespaces: Vec<NamespaceId> = match &target.schema {
        // PostgreSQL does not report a missing schema for DROP FUNCTION IF
        // EXISTS, so a missing schema is a missing function.
        Some(name) => conn.qualifier(name).into_iter().collect(),
        None => conn.search_order(),
    }
    .into_iter()
    .filter(|namespace| namespace.database_id() == MAIN_DB_ID)
    .map(|namespace| namespace.target.id)
    .collect();
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
            ));
        }
        return Ok(Some(Arc::clone(function)));
    }
    Ok(None)
}

/// The steps that remove the functions of schema `namespace`, for DROP
/// SCHEMA ... CASCADE. No steps when the schema has no functions.
pub(crate) fn drop_schema_commands(
    conn: &Connection,
    namespace: NamespaceId,
) -> Vec<CatalogCommand> {
    if !schema_has_functions(conn, namespace) {
        return Vec::new();
    }
    vec![
        CatalogCommand::Write(format!(
            "DELETE FROM {FUNCTIONS_TABLE_NAME} WHERE pronamespace = {}",
            namespace.as_u32()
        )),
        CatalogCommand::Reload,
    ]
}

pub(crate) fn schema_has_functions(conn: &Connection, namespace: NamespaceId) -> bool {
    let schema = conn.current_schema();
    schema_state::schema_state(&schema).is_some_and(|state| {
        state
            .functions
            .iter()
            .any(|function| function.namespace == namespace)
    })
}

/// The SQL that reads the functions table for [`replace_catalog`].
pub(crate) fn select_sql() -> String {
    format!("SELECT {SELECT_COLUMNS} FROM {FUNCTIONS_TABLE_NAME}")
}

/// Makes the rows read with [`select_sql`] the function catalog of the
/// connection's schema. The connection is in the transaction that wrote the
/// rows, so a rollback puts the catalog back with the rest of the schema.
pub(crate) fn replace_catalog(conn: &Connection, rows: &[Vec<Value>]) -> turso_core::Result<()> {
    let functions = FunctionCatalog::new(rows)?;
    conn.with_schema_mut(|schema| {
        schema_state::update(schema, |state| {
            state.functions = Arc::new(functions);
            Ok(())
        })
    })?
}

impl FunctionCatalog {
    /// The catalog of the rows read from the functions table. Each row is
    /// `(oid, proname, pronamespace, pronargs, proowner, sql)`.
    pub(crate) fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            let function = function_from_row(row)?;
            let key = (
                function.namespace,
                function.name.clone(),
                function.argument_count,
            );
            if catalog.by_oid.contains_key(&function.oid) || catalog.oids_by_name.contains_key(&key)
            {
                return Err(LimboError::Corrupt(format!(
                    "{FUNCTIONS_TABLE_NAME} has a duplicate function: {function:?}"
                )));
            }
            catalog.oids_by_name.insert(key, function.oid);
            catalog.by_oid.insert(function.oid, Arc::new(function));
        }
        Ok(catalog)
    }

    /// The function `name` of schema `namespace` with `argument_count`
    /// parameters.
    pub(crate) fn get(
        &self,
        namespace: NamespaceId,
        name: &str,
        argument_count: usize,
    ) -> Option<&Arc<SqlFunction>> {
        self.oids_by_name
            .get(&(namespace, name.to_string(), argument_count))
            .and_then(|oid| self.by_oid.get(oid))
    }

    /// The functions in the order of their oids.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Arc<SqlFunction>> {
        self.by_oid.values()
    }
}

fn function_from_row(row: &[Value]) -> turso_core::Result<SqlFunction> {
    let corrupt = || {
        LimboError::Corrupt(format!(
            "{FUNCTIONS_TABLE_NAME} has an invalid row: {row:?}"
        ))
    };
    let [Value::Numeric(Numeric::Integer(oid)), Value::Text(name), Value::Numeric(Numeric::Integer(namespace)), Value::Numeric(Numeric::Integer(argument_count)), Value::Numeric(Numeric::Integer(owner)), Value::Text(sql)] =
        row
    else {
        return Err(corrupt());
    };
    if *oid < FIRST_NORMAL_OBJECT_ID || *owner < 0 {
        return Err(corrupt());
    }
    Ok(SqlFunction {
        oid: *oid,
        name: name.as_str().to_string(),
        namespace: NamespaceId::new(u32::try_from(*namespace).map_err(|_| corrupt())?),
        owner: RoleId::from_stored(*owner),
        argument_count: usize::try_from(*argument_count).map_err(|_| corrupt())?,
        sql: sql.as_str().to_string(),
        definition: OnceLock::new(),
    })
}

impl SqlFunction {
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
    )
}

/// `text` as an SQL string literal.
fn sql_text(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}
