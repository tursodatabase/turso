//! The functions that users define with `CREATE FUNCTION`.
//!
//! A function has two stored rows, written in one transaction:
//!
//! - A row of `sqlite_schema` with the type [`FUNCTION_SCHEMA_ROW_TYPE`],
//!   which holds the `CREATE FUNCTION` statement. The engine skips rows of
//!   a type it does not know.
//! - A row of the relation catalog (see [`crate::classes`]) with the kind
//!   [`FUNCTION_RELKIND`], which gives the function its oid and its owner.
//!
//! Both rows are named after the function's signature, such as
//! `add_one(int4)`, because functions with different parameter types can
//! have the same name. The name starts with [`FUNCTION_ROW_NAME_PREFIX`],
//! so that no relation can have it: the statements that drop or rename a
//! table change each row of `sqlite_schema` with the name of the table.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use turso_core::namespace::{mangle_name, NamespaceId};
use turso_core::schema::{RelationKind, Schema};
use turso_core::security::roles::RoleId;
use turso_core::{
    emit_dialect_schema_row_delete, emit_dialect_schema_row_insert,
    open_schema_table_for_dialect_rows, CatalogChange, Connection, Insn, LimboError, Numeric,
    ProgramBuilder, Value, MAIN_DB_ID,
};
use turso_parser::ast;
use turso_pg_parser::sqlstate::{
    SQLSTATE_AMBIGUOUS_FUNCTION, SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
    SQLSTATE_DUPLICATE_FUNCTION, SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INSUFFICIENT_PRIVILEGE,
    SQLSTATE_INVALID_FUNCTION_DEFINITION, SQLSTATE_INVALID_SCHEMA_NAME,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT,
};
use turso_pg_parser::{display_signature_type_name, is_base_type_name, is_pseudo_type_name};

use crate::catalog::pg_base_type_oid;
use crate::classes::{
    cannot_have_the_name_of_a_function, class_catalog, corrupt_row, emit_class_row_insert_of_kind,
    emit_function_class_row_removal, open_class_table_for_writing, read_back_integer,
    FIRST_NORMAL_OBJECT_ID,
};
use crate::error::sqlstate_error;
use crate::session::{check_schema_exists, relation_lookup_namespaces};

/// The `type` of the `sqlite_schema` row that stores a function. The
/// version is part of the type, so that a later format gets a type of its
/// own.
pub(crate) const FUNCTION_SCHEMA_ROW_TYPE: &str = "pg_function_v1";

/// The `relkind` of the class row of a function. PostgreSQL keeps functions
/// in `pg_proc`, not in `pg_class`, so no `pg_class.relkind` letter stands
/// for one.
pub(crate) const FUNCTION_RELKIND: &str = "function";

/// What the name of the two stored rows of a function starts with, before
/// the signature. The engine keeps names with this prefix for itself, so a
/// relation of the user, whose rows are in the same tables, cannot have
/// such a name.
const FUNCTION_ROW_NAME_PREFIX: &str = "__turso_internal_function_";

/// A function that a user defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlFunction {
    pub(crate) oid: i64,
    pub(crate) namespace: NamespaceId,
    pub(crate) name: String,
    /// The name PostgreSQL stores for the type of each parameter, such as
    /// `int4`.
    pub(crate) parameter_types: Vec<String>,
    pub(crate) return_type: String,
    /// The body as the user wrote it, which is `pg_proc.prosrc`.
    pub(crate) body: String,
    pub(crate) owner: RoleId,
}

impl SqlFunction {
    fn signature(&self) -> String {
        signature(&self.name, &self.parameter_types)
    }

    /// The name of the function's two stored rows.
    fn stored_name(&self) -> String {
        format!("{FUNCTION_ROW_NAME_PREFIX}{}", self.signature())
    }
}

/// The function `name` with the parameters `parameter_types` as its stored
/// rows name it, such as `add_one(int4)`.
fn signature(name: &str, parameter_types: &[String]) -> String {
    format!("{name}({})", parameter_types.join(","))
}

/// The function as PostgreSQL writes it in errors, such as
/// `add_one(integer)`.
fn display_signature(name: &str, parameter_types: &[String]) -> String {
    let parameter_types: Vec<_> = parameter_types
        .iter()
        .map(|parameter_type| display_signature_type_name(parameter_type))
        .collect();
    format!("{name}({})", parameter_types.join(", "))
}

/// The functions of a database.
#[derive(Debug, Clone, Default)]
pub(crate) struct FunctionCatalog {
    by_oid: BTreeMap<i64, SqlFunction>,
}

impl FunctionCatalog {
    /// Returns the functions of `class_rows`, each `(oid, relname,
    /// relnamespace, relkind, relowner, ...)`, with the statements of
    /// `sql_by_name`, which has the `sqlite_schema` rows of the functions by
    /// name.
    pub(crate) fn new(
        class_rows: &[&[Value]],
        sql_by_name: &HashMap<String, String>,
    ) -> turso_core::Result<Self> {
        let mut catalog = Self::default();
        for row in class_rows {
            let function = function_from_rows(row, sql_by_name)?;
            if catalog.by_oid.contains_key(&function.oid)
                || catalog
                    .get(
                        function.namespace,
                        &function.name,
                        &function.parameter_types,
                    )
                    .is_some()
            {
                return Err(LimboError::Corrupt(format!(
                    "the catalog has a duplicate function: {function:?}"
                )));
            }
            catalog.by_oid.insert(function.oid, function);
        }
        Ok(catalog)
    }

    /// The functions in the order of their oids.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &SqlFunction> {
        self.by_oid.values()
    }

    fn in_namespace(&self, namespace: NamespaceId) -> impl Iterator<Item = &SqlFunction> {
        self.iter()
            .filter(move |function| function.namespace == namespace)
    }

    /// The functions named `name` of `namespace`, which differ in their
    /// parameter types.
    pub(crate) fn named<'a>(
        &'a self,
        namespace: NamespaceId,
        name: &str,
    ) -> impl Iterator<Item = &'a SqlFunction> + use<'a> {
        let name = name.to_string();
        self.iter()
            .filter(move |function| function.namespace == namespace && function.name == name)
    }

    fn get(
        &self,
        namespace: NamespaceId,
        name: &str,
        parameter_types: &[String],
    ) -> Option<&SqlFunction> {
        self.named(namespace, name)
            .find(|function| function.parameter_types == parameter_types)
    }

    /// Applies what a statement wrote to the stored rows. `values` are what
    /// the emitted instruction read back: the oid and the owner of a created
    /// function.
    pub(crate) fn apply(
        &mut self,
        change: &FunctionChange,
        values: &[Value],
    ) -> turso_core::Result<()> {
        match change {
            FunctionChange::Create(function) => {
                let [oid, owner] = values else {
                    return Err(LimboError::InternalError(
                        "a created function carries its oid and owner".to_string(),
                    ));
                };
                let oid = read_back_integer(oid)?;
                let function = SqlFunction {
                    oid,
                    owner: RoleId::from_stored(read_back_integer(owner)?),
                    ..function.clone()
                };
                assert!(
                    self.by_oid.insert(oid, function).is_none(),
                    "oid {oid} is already in use"
                );
            }
            FunctionChange::Replace(function) => {
                let replaced = self.by_oid.insert(function.oid, function.clone());
                assert!(replaced.is_some(), "a replaced function exists");
            }
            FunctionChange::Drop { oid } => {
                let dropped = self.by_oid.remove(oid);
                assert!(dropped.is_some(), "a dropped function exists");
            }
        }
        Ok(())
    }
}

/// What a statement did to a function, which the emitted bytecode tells the
/// in-memory catalog once the rows are written.
#[derive(Debug)]
pub(crate) enum FunctionChange {
    /// The oid and the owner come from the stored class row.
    Create(SqlFunction),
    /// `CREATE OR REPLACE` of a function that exists keeps its oid and
    /// owner.
    Replace(SqlFunction),
    Drop {
        oid: i64,
    },
}

fn function_from_rows(
    class_row: &[Value],
    sql_by_name: &HashMap<String, String>,
) -> turso_core::Result<SqlFunction> {
    let [Value::Numeric(Numeric::Integer(oid)), Value::Text(stored_name), Value::Numeric(Numeric::Integer(namespace)), _, Value::Numeric(Numeric::Integer(owner)), ..] =
        class_row
    else {
        return Err(corrupt_row(class_row));
    };
    if *oid < FIRST_NORMAL_OBJECT_ID || u32::try_from(*namespace).is_err() || *owner < 0 {
        return Err(corrupt_row(class_row));
    }
    let namespace = NamespaceId::new(*namespace as u32);
    let stored_signature = stored_name
        .as_str()
        .strip_prefix(FUNCTION_ROW_NAME_PREFIX)
        .ok_or_else(|| corrupt_row(class_row))?;
    let sql = sql_by_name
        .get(&mangle_name(namespace, stored_name.as_str()))
        .ok_or_else(|| {
            LimboError::Corrupt(format!(
                "function {stored_signature} has no {FUNCTION_SCHEMA_ROW_TYPE} row in \
                 sqlite_schema"
            ))
        })?;
    let definition = parse_stored_function(sql)?;
    let function = SqlFunction {
        oid: *oid,
        namespace,
        name: definition.name.name.as_str().to_string(),
        parameter_types: definition.parameter_types,
        return_type: definition.return_type,
        body: definition.body,
        owner: RoleId::from_stored(*owner),
    };
    if function.signature() != stored_signature {
        return Err(LimboError::Corrupt(format!(
            "function {stored_signature} is stored with the statement of {}",
            function.signature()
        )));
    }
    Ok(function)
}

/// The definition in `sql`, the `CREATE FUNCTION` statement of a stored
/// function.
fn parse_stored_function(sql: &str) -> turso_core::Result<ast::CreateFunction> {
    let corrupt =
        |reason: String| LimboError::Corrupt(format!("invalid stored function {sql:?}: {reason}"));
    let parsed = turso_pg_parser::parse(sql).map_err(|error| corrupt(error.to_string()))?;
    let translator = turso_pg_parser::translator::PostgreSQLTranslator::new();
    match translator.translate(&parsed) {
        Ok(ast::Stmt::CreateFunction(definition)) => Ok(definition),
        Ok(_) => Err(corrupt("it is not CREATE FUNCTION".to_string())),
        Err(error) => Err(corrupt(error.to_string())),
    }
}

/// A `DROP FUNCTION` statement for each function of schema `schema_name`.
pub(crate) fn drops_of_schema_functions(
    connection: &Connection,
    schema_name: &str,
) -> Vec<ast::DropFunction> {
    let Some(namespace) = connection.qualifier(schema_name) else {
        return Vec::new();
    };
    let schema = connection.current_schema();
    let Some(functions) = function_catalog(&schema) else {
        return Vec::new();
    };
    functions
        .in_namespace(namespace.target.id)
        .map(|function| ast::DropFunction {
            if_exists: false,
            name: ast::QualifiedName::fullname(
                ast::Name::exact(schema_name.to_string()),
                ast::Name::exact(function.name.clone()),
            ),
            parameter_types: Some(function.parameter_types.clone()),
        })
        .collect()
}

/// The functions that the engine loaded with the relation catalog, or None
/// when the catalog is not loaded.
pub(crate) fn function_catalog(schema: &Schema) -> Option<&FunctionCatalog> {
    class_catalog(schema).map(|catalog| &catalog.functions)
}

/// Whether the function of `create` exists: its schema has a function with
/// its name and its parameter types.
pub(crate) fn created_function_exists(
    connection: &Arc<Connection>,
    create: &ast::CreateFunction,
) -> bool {
    let Ok(Some(namespace)) = function_namespace(connection, &create.name) else {
        return false;
    };
    let schema = connection.current_schema();
    function_catalog(&schema).is_some_and(|functions| {
        functions
            .get(
                namespace,
                create.name.name.as_str(),
                &create.parameter_types,
            )
            .is_some()
    })
}

/// [`turso_core::Dialect::translate_create_function`] for the PostgreSQL
/// frontend. With PostgreSQL's errors: a schema that does not exist (3F000),
/// a function with the same parameter types (42723), `OR REPLACE` by a role
/// that does not own the function (42501) or with another result type
/// (42P13).
pub(crate) fn translate_create_function(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    create: &ast::CreateFunction,
    input: &str,
) -> turso_core::Result<()> {
    let namespace = function_namespace(connection, &create.name)?.ok_or_else(|| {
        sqlstate_error(
            SQLSTATE_INVALID_SCHEMA_NAME,
            "no schema has been selected to create in",
        )
    })?;
    let name = create.name.name.as_str();
    let functions = function_catalog(schema).ok_or_else(no_catalog)?;
    check_types_are_supported(create)?;
    // PostgreSQL matches the name of a function exactly, and each built-in
    // function or table that a statement can use has a name in lowercase.
    // Thus a quoted name with an uppercase letter, such as "Abs", is not
    // the name of one. The only built-in names with an uppercase letter are
    // the trigger functions `RI_FKey_*` of PostgreSQL, which no statement
    // can call.
    let can_be_a_built_in_name = !name.bytes().any(|byte| byte.is_ascii_uppercase());
    // PostgreSQL looks in pg_catalog first and chooses between functions of
    // one name by the types of the arguments. tursopg does not know these
    // types. With a user-defined function of the same name, it could not
    // tell which function a call names. For a built-in function that only
    // PostgreSQL has, tursopg would call the function of the user where
    // PostgreSQL calls the built-in function.
    if can_be_a_built_in_name && is_built_in_function(connection, name) {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "a function with the name of the built-in function \"{name}\" is not supported"
            ),
        ));
    }
    // The frontend has built-in functions with the name of a schema, such
    // as `auth.uid`. A call with this schema finds the built-in function.
    let name_in_schema = function_schema_name(connection, &create.name)
        .map(|schema_name| format!("{schema_name}.{name}"));
    if let Some(name_in_schema) = name_in_schema.filter(|name_in_schema| {
        can_be_a_built_in_name && is_built_in_function(connection, name_in_schema)
    }) {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "a function with the name of the built-in function \"{name_in_schema}\" is not \
                 supported"
            ),
        ));
    }
    // A call in FROM finds a built-in table function, such as json_tree, by
    // its name alone, in every schema.
    if can_be_a_built_in_name && is_built_in_table(schema, connection, name) {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "a function with the name of the built-in table or table function \"{name}\" \
                 is not supported"
            ),
        ));
    }
    // PostgreSQL lets a table and a function have the same name. tursopg
    // does not yet. The class of a relation has its name with its case, so
    // names that differ in case are two names, as in PostgreSQL.
    if class_catalog(schema)
        .and_then(|classes| classes.get_by_name(namespace, name))
        .is_some_and(|class| cannot_have_the_name_of_a_function(class.relkind))
    {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "a function with the name of relation \"{name}\" of the same schema is not \
                 supported"
            ),
        ));
    }

    let function = SqlFunction {
        oid: 0,
        namespace,
        name: name.to_string(),
        parameter_types: create.parameter_types.clone(),
        return_type: create.return_type.clone(),
        body: create.body.clone(),
        owner: connection.current_role(),
    };
    let stored_name = function.stored_name();
    let schema_cursor_id = open_schema_table_for_dialect_rows(program, schema)?;
    let change = match functions.get(namespace, name, &create.parameter_types) {
        Some(_) if !create.or_replace => {
            return Err(sqlstate_error(
                SQLSTATE_DUPLICATE_FUNCTION,
                format!("function \"{name}\" already exists with same argument types"),
            ));
        }
        Some(existing) => {
            check_ownership(connection, existing)?;
            if existing.return_type != create.return_type {
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_FUNCTION_DEFINITION,
                    "cannot change return type of existing function",
                ));
            }
            emit_dialect_schema_row_delete(
                program,
                schema_cursor_id,
                FUNCTION_SCHEMA_ROW_TYPE,
                &mangle_name(namespace, &stored_name),
            );
            (
                FunctionChange::Replace(SqlFunction {
                    oid: existing.oid,
                    owner: existing.owner,
                    ..function
                }),
                vec![],
            )
        }
        None => {
            let cursors =
                open_class_table_for_writing(program, schema, true)?.ok_or_else(no_catalog)?;
            let row = emit_class_row_insert_of_kind(
                program,
                &cursors,
                namespace,
                &stored_name,
                FUNCTION_RELKIND,
                function.owner,
            );
            (
                FunctionChange::Create(function),
                vec![row.oid_reg, row.owner_reg],
            )
        }
    };
    emit_dialect_schema_row_insert(
        program,
        schema_cursor_id,
        FUNCTION_SCHEMA_ROW_TYPE,
        &mangle_name(namespace, &stored_name),
        input,
    );
    emit_function_change(program, change.0, change.1);
    Ok(())
}

/// [`turso_core::Dialect::translate_drop_function`] for the PostgreSQL
/// frontend. With PostgreSQL's errors: a function that does not exist
/// (42883), a name that several functions have (42725), a role that does
/// not own the function (42501). Nothing checks whether another object uses
/// the function.
pub(crate) fn translate_drop_function(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    drop: &ast::DropFunction,
) -> turso_core::Result<()> {
    let functions = function_catalog(schema).ok_or_else(no_catalog)?;
    let found = find_function_to_drop(connection, functions, drop)?;
    if let Some(built_in) = built_in_function_signature(connection, drop) {
        check_built_in_function_is_not_dropped(drop, &built_in, found.is_some())?;
    }
    let Some(function) = found else {
        if drop.if_exists {
            return Ok(());
        }
        // PostgreSQL names the function as the statement does, with its
        // schema when the statement has one.
        let name = match &drop.name.namespace {
            Some(schema_name) => format!("{}.{}", schema_name.as_str(), drop.name.name.as_str()),
            None => drop.name.name.as_str().to_string(),
        };
        let function = match &drop.parameter_types {
            Some(parameter_types) => display_signature(&name, parameter_types),
            None => name,
        };
        return Err(sqlstate_error(
            SQLSTATE_UNDEFINED_FUNCTION,
            match &drop.parameter_types {
                Some(_) => format!("function {function} does not exist"),
                None => format!("could not find a function named \"{function}\""),
            },
        ));
    };
    check_ownership(connection, function)?;

    let stored_name = function.stored_name();
    let schema_cursor_id = open_schema_table_for_dialect_rows(program, schema)?;
    emit_dialect_schema_row_delete(
        program,
        schema_cursor_id,
        FUNCTION_SCHEMA_ROW_TYPE,
        &mangle_name(function.namespace, &stored_name),
    );
    let cursors = open_class_table_for_writing(program, schema, false)?.ok_or_else(no_catalog)?;
    emit_function_class_row_removal(program, &cursors, function.namespace, &stored_name);
    emit_function_change(program, FunctionChange::Drop { oid: function.oid }, vec![]);
    Ok(())
}

/// Fails for a statement that names the built-in function `built_in`.
/// PostgreSQL looks in pg_catalog before the schemas of the search path, and
/// it does not drop a function of pg_catalog. `has_user_function` says if a
/// schema of the search path also has a function that the statement names.
fn check_built_in_function_is_not_dropped(
    drop: &ast::DropFunction,
    built_in: &str,
    has_user_function: bool,
) -> turso_core::Result<()> {
    let name = drop.name.name.as_str();
    Err(match (&drop.parameter_types, has_user_function) {
        (_, false) => sqlstate_error(
            SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
            format!(
                "cannot drop function {built_in} because it is required by the database system"
            ),
        ),
        // PostgreSQL finds the built-in function when it has these parameter
        // types, and the function of the user when it does not. tursopg
        // does not know the parameter types of its built-in functions.
        (Some(_), true) => sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "DROP FUNCTION without a schema for \"{name}\", a name that a built-in function \
                 also has, is not supported"
            ),
        ),
        (None, true) => function_name_is_not_unique(name),
    })
}

/// The built-in function of tursopg that `drop` names, as PostgreSQL shows
/// it. tursopg does not know the parameter types of its built-in functions.
/// With a parameter list, the function is one with the name and the number
/// of parameters of the statement. Without one, it is one with the name.
fn built_in_function_signature(
    connection: &Connection,
    drop: &ast::DropFunction,
) -> Option<String> {
    let name = drop.name.name.as_str();
    // tursopg finds its built-in functions without regard to case, and each
    // of them has a name in lowercase.
    if drop.name.namespace.is_some() || name.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return None;
    }
    let argument_counts = built_in_function_argument_counts(connection, name);
    match &drop.parameter_types {
        Some(parameter_types) => {
            let is_built_in = matches!(
                turso_core::Func::resolve_function(name, parameter_types.len()),
                Ok(Some(_))
            ) || argument_counts
                .iter()
                .any(|count| *count < 0 || *count as usize == parameter_types.len());
            is_built_in.then(|| display_signature(name, parameter_types))
        }
        // PostgreSQL shows the parameter types, which tursopg only knows
        // for a function without parameters.
        None => match argument_counts.as_slice() {
            [] => None,
            [0] => Some(format!("{name}()")),
            _ => Some(name.to_string()),
        },
    }
}

/// The numbers of arguments that tursopg has a built-in function `name`
/// for: a function of the engine, or a function that the frontend or an
/// extension registered. A negative number is a function for each number of
/// arguments.
fn built_in_function_argument_counts(connection: &Connection, name: &str) -> Vec<i32> {
    let of_the_engine = turso_core::Func::builtin_function_list()
        .into_iter()
        .filter(|function| function.name.eq_ignore_ascii_case(name))
        .map(|function| function.narg);
    let registered = connection
        .get_syms_functions()
        .into_iter()
        .filter(|(function_name, ..)| function_name.eq_ignore_ascii_case(name))
        .map(|(_, _, argument_count, _)| argument_count);
    let mut argument_counts: Vec<i32> = of_the_engine.chain(registered).collect();
    argument_counts.sort_unstable();
    argument_counts.dedup();
    argument_counts
}

/// The function that `drop` names. A statement with parameter types names
/// the function of the named schema, or of the first schema of the search
/// path that has it. A statement without parameter types names the only
/// function of the name in the named schema, or in the search path.
fn find_function_to_drop<'a>(
    connection: &Arc<Connection>,
    functions: &'a FunctionCatalog,
    drop: &ast::DropFunction,
) -> turso_core::Result<Option<&'a SqlFunction>> {
    let name = drop.name.name.as_str();
    let namespaces = match &drop.name.namespace {
        Some(_) => match function_namespace(connection, &drop.name) {
            Ok(namespace) => namespace.into_iter().collect(),
            // PostgreSQL skips a function of a schema that does not exist.
            Err(_) if drop.if_exists => vec![],
            Err(error) => return Err(error),
        },
        None => search_path_namespaces(connection),
    };
    let Some(parameter_types) = &drop.parameter_types else {
        // The name must be the name of one function in all of the schemas
        // that the statement looks in. A function of an earlier schema
        // hides the functions with its parameter types of the later ones.
        let mut visible: Vec<&SqlFunction> = Vec::new();
        for function in namespaces
            .into_iter()
            .flat_map(|namespace| functions.named(namespace, name))
        {
            let is_hidden = visible
                .iter()
                .any(|earlier| earlier.parameter_types == function.parameter_types);
            if !is_hidden {
                visible.push(function);
            }
        }
        return match visible.as_slice() {
            [] => Ok(None),
            [function] => Ok(Some(function)),
            _ => Err(function_name_is_not_unique(name)),
        };
    };
    for parameter_type in parameter_types {
        if let Err(error) = check_type_exists(connection, parameter_type) {
            // PostgreSQL only warns when the statement has IF EXISTS.
            if drop.if_exists {
                return Ok(None);
            }
            return Err(error);
        }
    }
    Ok(namespaces
        .into_iter()
        .find_map(|namespace| functions.get(namespace, name, parameter_types)))
}

/// Fails with PostgreSQL's error when `type_name`, as a signature names a
/// type, names no type: a built-in type, or a type of the named schema or
/// of the search path.
fn check_type_exists(connection: &Arc<Connection>, type_name: &str) -> turso_core::Result<()> {
    let element = type_name.trim_end_matches("[]");
    let (schema_name, name) = match element.split_once('.') {
        Some((schema_name, name)) => (Some(schema_name), name),
        None => (None, element),
    };
    if let Some(schema_name) = schema_name {
        check_schema_exists(connection, schema_name)?;
    }
    if (schema_name.is_none()
        && (is_base_type_name(name)
            || is_pseudo_type_name(name)
            || is_catalog_table(connection, name)))
        || has_type_with_exact_name(connection, schema_name, name)
    {
        return Ok(());
    }
    Err(sqlstate_error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("type \"{type_name}\" does not exist"),
    ))
}

/// Whether tursopg has a catalog table `name`, such as `pg_class`.
/// PostgreSQL has a row type for each of its catalog tables.
fn is_catalog_table(connection: &Connection, name: &str) -> bool {
    name.starts_with("pg_") && connection.get_syms_vtab_mods().contains(name)
}

/// Whether the schema `schema_name`, or a schema of the search path, has a
/// type that is not built in with exactly the name `name`. PostgreSQL finds
/// a type by its exact name: `"Mood"` is not `mood`. tursopg stores the name
/// of a custom type in lowercase, and it knows the name of a table as its
/// statement has it.
fn has_type_with_exact_name(
    connection: &Arc<Connection>,
    schema_name: Option<&str>,
    name: &str,
) -> bool {
    let is_lowercase = !name.bytes().any(|byte| byte.is_ascii_uppercase());
    relation_lookup_namespaces(connection, schema_name)
        .iter()
        .any(|namespace| {
            connection.with_schema(namespace.database_id(), |schema| {
                let is_custom_type = is_lowercase && schema.get_type_def_unchecked(name).is_some();
                let is_row_type = match schema.relation_kind(namespace.target.id, name) {
                    None | Some(RelationKind::Index) => false,
                    Some(RelationKind::Table) => schema
                        .get_btree_table(namespace.target.id, name)
                        .is_none_or(|table| table.declared_name == name),
                    Some(_) => true,
                };
                is_custom_type || is_row_type
            })
        })
}

fn function_name_is_not_unique(name: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_AMBIGUOUS_FUNCTION,
        format!("function name \"{name}\" is not unique"),
    )
}

/// The namespace of the schema that `name` puts a function in: the schema
/// it names, or the first schema of the search path, None when no schema of
/// the search path exists.
fn function_namespace(
    connection: &Arc<Connection>,
    name: &ast::QualifiedName,
) -> turso_core::Result<Option<NamespaceId>> {
    let Some(schema_name) = &name.namespace else {
        return Ok(search_path_namespaces(connection).into_iter().next());
    };
    let schema_name = schema_name.as_str();
    connection
        .qualifier(&schema_name.to_lowercase())
        .filter(|qualifier| qualifier.database_id() == MAIN_DB_ID && !qualifier.is_database())
        .map(|qualifier| Some(qualifier.target.id))
        .ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                format!("schema \"{schema_name}\" does not exist"),
            )
        })
}

/// The name of the schema that `name` puts a function in: the schema it
/// names, or the first schema of the search path, None when no schema of the
/// search path exists.
fn function_schema_name(connection: &Connection, name: &ast::QualifiedName) -> Option<String> {
    match &name.namespace {
        Some(schema_name) => Some(schema_name.as_str().to_lowercase()),
        None => connection
            .search_order()
            .into_iter()
            .find(|qualifier| qualifier.database_id() == MAIN_DB_ID && !qualifier.is_database())
            .map(|qualifier| qualifier.name),
    }
}

/// The namespaces of the schemas of the search path that exist, in order.
pub(crate) fn search_path_namespaces(connection: &Connection) -> Vec<NamespaceId> {
    connection
        .search_order()
        .into_iter()
        .filter(|qualifier| qualifier.database_id() == MAIN_DB_ID && !qualifier.is_database())
        .map(|qualifier| qualifier.target.id)
        .collect()
}

/// Whether a call of `name` without a schema can name a built-in function:
/// a function of the engine, a function that the frontend or an extension
/// registered, or a function of PostgreSQL. The last group has the
/// functions that tursopg does not have, and the functions that the
/// translator replaces with another expression, such as `int4(x)`.
pub(crate) fn is_built_in_function(connection: &Connection, name: &str) -> bool {
    is_engine_function(name)
        || connection
            .get_syms_functions()
            .iter()
            .any(|(function_name, ..)| function_name.eq_ignore_ascii_case(name))
        || is_postgres_built_in_function(name)
}

/// Whether the engine has a function `name`, for some number of arguments.
fn is_engine_function(name: &str) -> bool {
    // The list has one name for each function and each number of arguments.
    let list = turso_core::Func::builtin_function_list();
    let is_listed = list
        .iter()
        .any(|function| function.name.eq_ignore_ascii_case(name));
    // The lookup of a call also finds the other names of a function, such
    // as `array_overlaps` for `array_overlap`. An error means that the
    // engine has the function, but not for this number of arguments. The
    // lookup can depend on the number of arguments, so each number that a
    // listed function accepts is tried.
    let most_arguments = list
        .iter()
        .map(|function| function.narg)
        .max()
        .map_or(0, |count| count.max(0) as usize);
    let is_found_by_a_call = (0..=most_arguments).any(|argument_count| {
        !matches!(
            turso_core::Func::resolve_function(name, argument_count),
            Ok(None)
        )
    });
    is_listed || is_found_by_a_call
}

/// Whether tursopg has a built-in table or table function `name`: a table
/// of the engine's own namespace, or a table module that the frontend or an
/// extension registered.
fn is_built_in_table(schema: &Schema, connection: &Connection, name: &str) -> bool {
    schema.get_table(NamespaceId::ROOT, name).is_some()
        || connection
            .get_syms_vtab_mods()
            .iter()
            .any(|module| module.eq_ignore_ascii_case(name))
}

/// Whether PostgreSQL 18 has a function `name` in pg_catalog.
///
/// `pg_catalog_function_names.list` has the names, one for each line. For a
/// new PostgreSQL version, write the result of this query to the file:
///
/// ```sql
/// SELECT proname FROM (
///     SELECT DISTINCT proname::text AS proname FROM pg_proc
///     WHERE pronamespace = 'pg_catalog'::regnamespace
/// ) AS names ORDER BY proname COLLATE "C";
/// ```
fn is_postgres_built_in_function(name: &str) -> bool {
    static NAMES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
        include_str!("pg_catalog_function_names.list")
            .lines()
            .collect()
    });
    NAMES.contains(name)
}

/// tursopg has fewer base types than PostgreSQL. A function can only have
/// the ones that `pg_type` shows.
fn check_types_are_supported(create: &ast::CreateFunction) -> turso_core::Result<()> {
    let unsupported = |used_as: &str, type_name: &str| {
        sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "a function {used_as} of type {} is not supported",
                display_signature_type_name(type_name)
            ),
        )
    };
    for parameter_type in &create.parameter_types {
        if pg_base_type_oid(parameter_type).is_none() {
            return Err(unsupported("parameter", parameter_type));
        }
    }
    if pg_base_type_oid(&create.return_type).is_none() {
        return Err(unsupported("result", &create.return_type));
    }
    Ok(())
}

/// Only the owner of a function and a superuser may replace or drop it.
fn check_ownership(connection: &Connection, function: &SqlFunction) -> turso_core::Result<()> {
    if connection.current_role_is_superuser() || function.owner == connection.current_role() {
        return Ok(());
    }
    Err(sqlstate_error(
        SQLSTATE_INSUFFICIENT_PRIVILEGE,
        format!("must be owner of function {}", function.name),
    ))
}

fn emit_function_change(
    program: &mut ProgramBuilder,
    change: FunctionChange,
    value_regs: Vec<usize>,
) {
    let change: Arc<dyn Any + Send + Sync> = Arc::new(change);
    program.emit_insn(Insn::CatalogChange {
        db: MAIN_DB_ID,
        change: Box::new(CatalogChange::DialectObject(change)),
        value_regs,
    });
}

fn no_catalog() -> LimboError {
    LimboError::ParseError("the database has no relation catalog".to_string())
}
