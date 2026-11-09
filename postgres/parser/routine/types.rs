//! The types that the type names of a statement name, found as PostgreSQL's
//! LookupTypeName() finds them.

use super::{check_schema, error, qualified_name, strings, RoutineCatalog, UserType};
use crate::builtins::{self, Type};
use crate::sqlstate::{SQLSTATE_SYNTAX_ERROR, SQLSTATE_UNDEFINED_COLUMN, SQLSTATE_UNDEFINED_TABLE};
use crate::ParseError;
use pg_query::protobuf::TypeName;

/// A type that a type name names.
#[derive(Debug, Clone)]
pub(super) enum TypeRef {
    /// A type of pg_catalog.
    Builtin(&'static Type),
    /// The row type of a table, view or sequence, or a type that CREATE TYPE
    /// or CREATE DOMAIN made.
    User { name: String, kind: UserKind },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UserKind {
    /// The row type of a relation, which is a composite type.
    Row,
    Domain,
    Enum,
    Other,
    /// An array of a type that the user made.
    Array,
}

impl TypeRef {
    /// The name of the type in pg_type for a type of pg_catalog, such as
    /// `int4`.
    pub(super) fn builtin_name(&self) -> Option<&'static str> {
        match self {
            Self::Builtin(ty) => Some(ty.name),
            Self::User { .. } => None,
        }
    }

    /// The name of the type in an error, as PostgreSQL's format_type_be()
    /// gives it.
    pub(super) fn display_name(&self) -> String {
        match self {
            Self::Builtin(ty) => ty.display_name.to_string(),
            Self::User { name, .. } => name.clone(),
        }
    }

    pub(super) fn is_pseudo(&self) -> bool {
        matches!(self, Self::Builtin(ty) if ty.is_pseudo())
    }

    pub(super) fn is_polymorphic(&self) -> bool {
        matches!(self, Self::Builtin(ty) if ty.is_polymorphic())
    }

    pub(super) fn is(&self, name: &str) -> bool {
        self.builtin_name() == Some(name)
    }

    /// Whether the type is the same type as `other`.
    pub(super) fn same_as(&self, other: &TypeRef) -> bool {
        match (self, other) {
            (Self::Builtin(a), Self::Builtin(b)) => a.name == b.name,
            (Self::User { name: a, .. }, Self::User { name: b, .. }) => a == b,
            _ => false,
        }
    }

    /// Whether the type is a composite type: the row type of a relation, or
    /// a composite type of pg_catalog.
    pub(super) fn is_composite(&self) -> bool {
        match self {
            Self::Builtin(ty) => ty.kind == 'c',
            Self::User { kind, .. } => *kind == UserKind::Row,
        }
    }

    pub(super) fn is_array(&self) -> bool {
        match self {
            Self::Builtin(ty) => ty.is_array(),
            Self::User { kind, .. } => *kind == UserKind::Array,
        }
    }
}

/// The type that `type_name` names, or None when no type has the name.
/// The errors are for a missing schema and for a %TYPE reference to a
/// relation or column that does not exist.
pub(super) fn find_type(
    type_name: &TypeName,
    catalog: &dyn RoutineCatalog,
) -> Result<Option<TypeRef>, ParseError> {
    let names = strings(&type_name.names);
    if type_name.pct_type {
        return column_type(type_name, &names, catalog).map(Some);
    }
    let (schema, name) = qualified_name(&names)?;
    if let Some(schema) = schema {
        check_schema(schema, catalog)?;
    }
    let is_array = !type_name.array_bounds.is_empty();
    if matches!(schema, None | Some("pg_catalog")) {
        if let Some(ty) = builtins::find_type(name) {
            if !is_array {
                return Ok(Some(TypeRef::Builtin(ty)));
            }
            return Ok(builtins::find_type(&format!("_{name}")).map(TypeRef::Builtin));
        }
        if schema.is_some() {
            return Ok(None);
        }
    }
    let kind = if catalog.relation(schema, name).is_some() {
        UserKind::Row
    } else {
        match catalog.user_type(schema, name) {
            Some(UserType::Domain) => UserKind::Domain,
            Some(UserType::Enum) => UserKind::Enum,
            Some(UserType::Other) => UserKind::Other,
            None => return Ok(None),
        }
    };
    Ok(Some(if is_array {
        TypeRef::User {
            name: format!("{name}[]"),
            kind: UserKind::Array,
        }
    } else {
        TypeRef::User {
            name: name.to_string(),
            kind,
        }
    }))
}

/// The type that a %TYPE reference such as `emp.salary%TYPE` names: the
/// type of the column.
fn column_type(
    type_name: &TypeName,
    names: &[&str],
    catalog: &dyn RoutineCatalog,
) -> Result<TypeRef, ParseError> {
    let (relation_names, column) = match names {
        [] | [_] => {
            return Err(error(
                SQLSTATE_SYNTAX_ERROR,
                format!(
                    "improper %TYPE reference (too few dotted names): {}",
                    type_name_text(type_name)
                ),
            ))
        }
        [relation @ .., column] => (relation, *column),
    };
    let (schema, relation_name) = qualified_name(relation_names)?;
    if let Some(schema) = schema {
        check_schema(schema, catalog)?;
    }
    let relation = catalog.relation(schema, relation_name).ok_or_else(|| {
        error(
            SQLSTATE_UNDEFINED_TABLE,
            format!("relation \"{}\" does not exist", relation_names.join(".")),
        )
    })?;
    let declared = relation
        .columns
        .iter()
        .find(|(name, _)| name == column)
        .map(|(_, declared)| declared.as_str())
        .ok_or_else(|| {
            error(
                SQLSTATE_UNDEFINED_COLUMN,
                format!("column \"{column}\" of relation \"{relation_name}\" does not exist"),
            )
        })?;
    Ok(declared_type(declared).unwrap_or_else(|| TypeRef::User {
        name: declared.to_string(),
        kind: UserKind::Other,
    }))
}

/// The built-in type of a column declared with the type `declared`, such as
/// `integer` or `varchar(20)`.
pub(super) fn declared_type(declared: &str) -> Option<TypeRef> {
    let parsed = pg_query::parse(&format!("SELECT NULL::{declared}")).ok()?;
    let Some(pg_query::NodeEnum::SelectStmt(select)) = crate::first_statement(&parsed) else {
        return None;
    };
    let target = select.target_list.first()?.node.as_ref()?;
    let pg_query::NodeEnum::ResTarget(target) = target else {
        return None;
    };
    let pg_query::NodeEnum::TypeCast(cast) = target.val.as_deref()?.node.as_ref()? else {
        return None;
    };
    builtin_type(cast.type_name.as_ref()?)
}

/// The type of pg_catalog that `type_name` names, without a look at the
/// types that the user made.
pub(super) fn builtin_type(type_name: &TypeName) -> Option<TypeRef> {
    let names = strings(&type_name.names);
    let name = match names.as_slice() {
        [name] | ["pg_catalog", name] => *name,
        _ => return None,
    };
    let name = if type_name.array_bounds.is_empty() {
        name.to_string()
    } else {
        format!("_{name}")
    };
    builtins::find_type(&name).map(TypeRef::Builtin)
}

/// A type name as PostgreSQL's TypeNameToString() writes it, such as
/// `missing_type[]`.
pub(super) fn type_name_text(type_name: &TypeName) -> String {
    let mut text = strings(&type_name.names).join(".");
    if type_name.pct_type {
        text.push_str("%TYPE");
    }
    if !type_name.array_bounds.is_empty() {
        text.push_str("[]");
    }
    text
}
