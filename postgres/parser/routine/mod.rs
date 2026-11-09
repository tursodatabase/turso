//! The statements about functions, procedures and triggers, and about the
//! objects that only they use: aggregates, event triggers, languages and
//! transforms.
//!
//! tursopg cannot run these statements. For each statement it does the
//! checks of PostgreSQL first, in the order of PostgreSQL, and gives the
//! error of the first check that fails. A statement that passes these checks
//! is a statement that PostgreSQL accepts. It gets SQLSTATE 0A000 and a
//! message that names the first feature that tursopg does not support.
//!
//! tursopg has no user-defined functions or triggers. Some statements do
//! nothing in PostgreSQL when the object is not there, for example DROP
//! FUNCTION IF EXISTS. They do nothing in tursopg too.

mod aggregate;
mod body;
pub(crate) mod expr;
mod function;
mod language;
mod lookup;
mod trigger;
mod types;

use crate::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INSUFFICIENT_PRIVILEGE, SQLSTATE_INVALID_SCHEMA_NAME,
    SQLSTATE_SYNTAX_ERROR,
};
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self as pb, ObjectType};
use pg_query::ParseResult;

/// The objects of the database that the checks look up.
pub trait RoutineCatalog {
    /// Whether the current role is a superuser.
    fn is_superuser(&self) -> bool;
    /// The value of the check_function_bodies setting.
    fn check_function_bodies(&self) -> bool;
    /// The first schema of search_path that exists. CREATE puts an object
    /// without a schema in it.
    fn creation_schema(&self) -> Option<String>;
    fn schema_exists(&self, name: &str) -> bool;
    fn role_exists(&self, name: &str) -> bool;
    fn extension_exists(&self, name: &str) -> bool;
    /// The relation with the name `name` in `schema`, or in the schemas of
    /// search_path when `schema` is None.
    fn relation(&self, schema: Option<&str>, name: &str) -> Option<Relation>;
    /// The type that CREATE TYPE or CREATE DOMAIN made with the name `name`.
    fn user_type(&self, schema: Option<&str>, name: &str) -> Option<UserType>;
    /// Prepares the statement `sql` without running it. Gives the number of
    /// its result columns.
    fn prepare(&self, sql: &str) -> Result<usize, ParseError>;
}

/// A table, view, sequence or index.
#[derive(Debug, Clone)]
pub struct Relation {
    pub kind: RelationKind,
    /// The name of each column, with the type that it was declared with,
    /// such as `integer` or `numeric(10, 2)`.
    pub columns: Vec<(String, String)>,
    /// Whether the session checks a foreign key of the table: a foreign key
    /// on the table, or one that references the table.
    pub foreign_keys_checked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationKind {
    Table,
    View,
    MaterializedView,
    Sequence,
    Index,
}

/// A type that CREATE TYPE or CREATE DOMAIN made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserType {
    Domain,
    Enum,
    Other,
}

/// Checks the first statement of `parse_result` when it is about functions,
/// procedures, triggers, languages or extensions. `sql` is the text that was
/// parsed into `parse_result`.
///
/// None for any other statement. Ok for a statement that does nothing, and
/// the error for a statement that fails.
pub fn check_routine_statement(
    sql: &str,
    parse_result: &ParseResult,
    catalog: &dyn RoutineCatalog,
) -> Option<Result<(), ParseError>> {
    let statement = crate::first_statement(parse_result)?;
    Some(match statement {
        Node::CreateFunctionStmt(stmt) => function::check_create_function(sql, stmt, catalog),
        Node::DefineStmt(stmt) if stmt.kind() == ObjectType::ObjectAggregate => {
            aggregate::check_create_aggregate(stmt, catalog)
        }
        Node::AlterFunctionStmt(stmt) => lookup::check_alter_function(stmt, catalog),
        Node::CallStmt(stmt) => lookup::check_call(stmt, catalog),
        Node::CreateTrigStmt(stmt) => trigger::check_create_trigger(stmt, catalog),
        Node::CreateEventTrigStmt(stmt) => trigger::check_create_event_trigger(stmt, catalog),
        Node::AlterEventTrigStmt(stmt) => Err(trigger::missing_event_trigger(&stmt.trigname)),
        Node::AlterTableStmt(stmt) => trigger::check_alter_table_triggers(stmt, catalog)?,
        Node::DoStmt(stmt) => language::check_do(sql, stmt, catalog),
        Node::CreatePlangStmt(stmt) => language::check_create_language(stmt, catalog),
        Node::CreateExtensionStmt(stmt) => language::check_create_extension(stmt, catalog),
        Node::CreateTransformStmt(stmt) => language::check_create_transform(stmt, catalog),
        Node::DropStmt(stmt) => check_drop(stmt, catalog)?,
        Node::CommentStmt(stmt) => check_comment(stmt, catalog)?,
        Node::RenameStmt(stmt) => check_rename(stmt, catalog)?,
        Node::AlterOwnerStmt(stmt) => check_alter_owner(stmt, catalog)?,
        Node::AlterObjectSchemaStmt(stmt) => {
            let object = routine_object(stmt.object_type())?;
            lookup::check_alter_schema(object, stmt, catalog)
        }
        Node::AlterObjectDependsStmt(stmt) => check_alter_depends(stmt, catalog)?,
        _ => return None,
    })
}

/// A kind of function that a statement names: FUNCTION, PROCEDURE, ROUTINE
/// or AGGREGATE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutineObject {
    Function,
    Procedure,
    Routine,
    Aggregate,
}

impl RoutineObject {
    /// The word for the object in an error, such as `function`.
    fn word(self) -> &'static str {
        match self {
            Self::Function => "function",
            Self::Procedure => "procedure",
            Self::Routine => "routine",
            Self::Aggregate => "aggregate",
        }
    }

    /// The SQL keyword of the object, such as `FUNCTION`.
    fn keyword(self) -> &'static str {
        match self {
            Self::Function => "FUNCTION",
            Self::Procedure => "PROCEDURE",
            Self::Routine => "ROUTINE",
            Self::Aggregate => "AGGREGATE",
        }
    }
}

fn routine_object(object: ObjectType) -> Option<RoutineObject> {
    Some(match object {
        ObjectType::ObjectFunction => RoutineObject::Function,
        ObjectType::ObjectProcedure => RoutineObject::Procedure,
        ObjectType::ObjectRoutine => RoutineObject::Routine,
        ObjectType::ObjectAggregate => RoutineObject::Aggregate,
        _ => return None,
    })
}

fn check_drop(stmt: &pb::DropStmt, catalog: &dyn RoutineCatalog) -> Option<Result<(), ParseError>> {
    let missing_ok = stmt.missing_ok;
    let objects = &stmt.objects;
    Some(match stmt.remove_type() {
        ObjectType::ObjectTrigger => trigger::check_drop_trigger(objects, missing_ok, catalog),
        ObjectType::ObjectEventTrigger => trigger::check_drop_event_trigger(objects, missing_ok),
        ObjectType::ObjectLanguage => language::check_drop_language(objects, missing_ok, catalog),
        ObjectType::ObjectTransform => language::check_drop_transform(objects, missing_ok, catalog),
        object => {
            let object = routine_object(object)?;
            lookup::check_drop_functions(object, objects, missing_ok, catalog)
        }
    })
}

/// COMMENT ON does nothing in tursopg, which does not keep comments. It
/// fails when the object does not exist.
fn check_comment(
    stmt: &pb::CommentStmt,
    catalog: &dyn RoutineCatalog,
) -> Option<Result<(), ParseError>> {
    let object = stmt.object.as_deref()?.node.as_ref()?;
    Some(match stmt.objtype() {
        ObjectType::ObjectTrigger => trigger::check_trigger_reference(object, catalog),
        ObjectType::ObjectEventTrigger => Err(trigger::missing_event_trigger(string_node(object)?)),
        ObjectType::ObjectLanguage => language::check_comment_on_language(object, catalog),
        ObjectType::ObjectTransform => language::check_transform_reference(object, false, catalog),
        objtype => {
            let routine = routine_object(objtype)?;
            let Node::ObjectWithArgs(function) = object else {
                return None;
            };
            lookup::check_comment_on_function(routine, function, catalog)
        }
    })
}

fn check_rename(
    stmt: &pb::RenameStmt,
    catalog: &dyn RoutineCatalog,
) -> Option<Result<(), ParseError>> {
    Some(match stmt.rename_type() {
        ObjectType::ObjectTrigger => {
            trigger::check_alter_trigger(stmt.relation.as_ref()?, catalog, &stmt.subname)
        }
        ObjectType::ObjectEventTrigger => Err(trigger::missing_event_trigger(string_node(
            stmt.object.as_deref()?.node.as_ref()?,
        )?)),
        ObjectType::ObjectLanguage => language::check_rename_language(stmt, catalog),
        object => {
            let object = routine_object(object)?;
            lookup::check_rename_function(object, stmt, catalog)
        }
    })
}

fn check_alter_owner(
    stmt: &pb::AlterOwnerStmt,
    catalog: &dyn RoutineCatalog,
) -> Option<Result<(), ParseError>> {
    let object = stmt.object.as_deref()?.node.as_ref()?;
    let kind = match stmt.object_type() {
        ObjectType::ObjectEventTrigger => None,
        ObjectType::ObjectLanguage => None,
        object => Some(routine_object(object)?),
    };
    // PostgreSQL's ExecAlterOwnerStmt() finds the new owner first.
    if let Err(error) = check_role(stmt.newowner.as_ref(), catalog) {
        return Some(Err(error));
    }
    Some(match (stmt.object_type(), kind) {
        (ObjectType::ObjectEventTrigger, _) => {
            Err(trigger::missing_event_trigger(string_node(object)?))
        }
        (ObjectType::ObjectLanguage, _) => language::check_alter_language_owner(object, catalog),
        (_, Some(kind)) => {
            let Node::ObjectWithArgs(function) = object else {
                return None;
            };
            lookup::check_alter_function_owner(kind, function, catalog)
        }
        _ => return None,
    })
}

fn check_alter_depends(
    stmt: &pb::AlterObjectDependsStmt,
    catalog: &dyn RoutineCatalog,
) -> Option<Result<(), ParseError>> {
    match stmt.object_type() {
        ObjectType::ObjectTrigger => {
            let trigger = match stmt.object.as_deref()?.node.as_ref()? {
                Node::List(list) => list
                    .items
                    .last()
                    .and_then(|item| string_node(item.node.as_ref()?))?,
                _ => return None,
            };
            Some(trigger::check_alter_trigger(
                stmt.relation.as_ref()?,
                catalog,
                trigger,
            ))
        }
        object => {
            let object = routine_object(object)?;
            Some(lookup::check_alter_depends(object, stmt, catalog))
        }
    }
}

/// Checks that the role of OWNER TO exists.
fn check_role(role: Option<&pb::RoleSpec>, catalog: &dyn RoutineCatalog) -> Result<(), ParseError> {
    let Some(role) = role else {
        return Ok(());
    };
    if role.roletype() != pb::RoleSpecType::RolespecCstring || catalog.role_exists(&role.rolename) {
        return Ok(());
    }
    Err(ParseError::Sqlstate {
        sqlstate: crate::sqlstate::SQLSTATE_UNDEFINED_OBJECT,
        message: format!("role \"{}\" does not exist", role.rolename),
    })
}

/// Prepares `sql`, a statement that PostgreSQL parses, to find the errors
/// that PostgreSQL gives for it. Gives the number of result columns, or None
/// when tursopg cannot prepare the statement: SQLSTATE 0A000, or 42601 for
/// SQL that tursopg does not translate. PostgreSQL can accept such a
/// statement.
fn prepare(catalog: &dyn RoutineCatalog, sql: &str) -> Result<Option<usize>, ParseError> {
    match catalog.prepare(sql) {
        Ok(columns) => Ok(Some(columns)),
        Err(ParseError::Sqlstate { sqlstate, .. })
            if sqlstate == SQLSTATE_FEATURE_NOT_SUPPORTED || sqlstate == SQLSTATE_SYNTAX_ERROR =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// The strings of a list of String nodes, such as the parts of a qualified
/// name.
fn strings(nodes: &[pb::Node]) -> Vec<&str> {
    nodes
        .iter()
        .filter_map(|node| string_node(node.node.as_ref()?))
        .collect()
}

fn string_node(node: &Node) -> Option<&str> {
    match node {
        Node::String(value) => Some(value.sval.as_str()),
        _ => None,
    }
}

/// A name with an optional schema, from the parts of a qualified name, as
/// PostgreSQL's DeconstructQualifiedName() reads them.
fn qualified_name<'a>(names: &[&'a str]) -> Result<(Option<&'a str>, &'a str), ParseError> {
    match names {
        [name] => Ok((None, name)),
        [schema, name] => Ok((Some(schema), name)),
        [_, _, _] => Err(not_supported_as_postgres(format!(
            "cross-database references are not implemented: {}",
            names.join(".")
        ))),
        _ => Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_SYNTAX_ERROR,
            message: format!(
                "improper qualified name (too many dotted names): {}",
                names.join(".")
            ),
        }),
    }
}

/// The schema that CREATE puts an object in, as PostgreSQL's
/// QualifiedNameGetCreationNamespace() finds it, and the check that the
/// current role may create objects in it.
fn creation_schema(
    schema: Option<&str>,
    catalog: &dyn RoutineCatalog,
) -> Result<String, ParseError> {
    let schema = match schema {
        Some(schema) => {
            check_schema(schema, catalog)?;
            schema.to_string()
        }
        None => catalog
            .creation_schema()
            .ok_or_else(|| ParseError::Sqlstate {
                sqlstate: SQLSTATE_INVALID_SCHEMA_NAME,
                message: "no schema has been selected to create in".to_string(),
            })?,
    };
    // tursopg gives the privilege to create objects only to superusers.
    if !catalog.is_superuser() {
        return Err(permission_denied(format!(
            "permission denied for schema {schema}"
        )));
    }
    Ok(schema)
}

/// Checks that a schema that a name names exists.
fn check_schema(schema: &str, catalog: &dyn RoutineCatalog) -> Result<(), ParseError> {
    if schema == "pg_catalog" || catalog.schema_exists(schema) {
        Ok(())
    } else {
        Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_INVALID_SCHEMA_NAME,
            message: format!("schema \"{schema}\" does not exist"),
        })
    }
}

fn permission_denied(message: impl Into<String>) -> ParseError {
    ParseError::Sqlstate {
        sqlstate: SQLSTATE_INSUFFICIENT_PRIVILEGE,
        message: message.into(),
    }
}

/// An error with SQLSTATE 0A000 that PostgreSQL gives too, with the message
/// of PostgreSQL.
fn not_supported_as_postgres(message: impl Into<String>) -> ParseError {
    ParseError::Sqlstate {
        sqlstate: SQLSTATE_FEATURE_NOT_SUPPORTED,
        message: message.into(),
    }
}

fn error(sqlstate: &'static str, message: impl Into<String>) -> ParseError {
    ParseError::Sqlstate {
        sqlstate,
        message: message.into(),
    }
}
