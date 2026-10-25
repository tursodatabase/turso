//! The type information `Statement::get_column_type_info` reports for each result
//! column. It is worked out while the statement is compiled, when the tables,
//! registers and types the result columns refer to are known.

use turso_parser::ast;

use super::emitter::Resolver;
use super::expr::expr_type::{type_of, BuiltinType, ExprType, TypeEnv};
use super::plan::{ResultSetColumn, TableReferences};
use crate::schema::Schema;
use crate::statement::{ColumnTypeInfo, ColumnTypeKind};

pub(crate) fn result_column_types(
    result_columns: &[ResultSetColumn],
    table_references: &TableReferences,
    resolver: &Resolver,
) -> Vec<Option<ColumnTypeInfo>> {
    let env = TypeEnv::new(Some(table_references), resolver);
    result_columns
        .iter()
        .map(|column| match &column.expr {
            ast::Expr::Column { table, column, .. } => {
                table_column_type(*table, *column, table_references, resolver.schema())
            }
            expr => expression_type(&type_of(expr, &env), resolver.schema()),
        })
        .collect()
}

/// A table column reports the type name it was declared with.
fn table_column_type(
    table: ast::TableInternalId,
    column: usize,
    table_references: &TableReferences,
    schema: &Schema,
) -> Option<ColumnTypeInfo> {
    let (_, table) = table_references.find_table_by_internal_id(table)?;
    let column = table.get_column_at(column)?;
    let resolved = schema
        .resolve_type(&column.ty_str, table.is_strict())
        .ok()
        .flatten();
    // STRUCT and UNION are tagged on `TypeDefKind`, DOMAIN on `TypeDef.is_domain`,
    // and anything else registered through CREATE TYPE is a Custom. A name that
    // isn't in the type registry is a Builtin.
    let (base_type, kind) = match resolved {
        Some(resolved) => {
            let leaf = resolved.leaf();
            let kind = if leaf.is_struct() {
                ColumnTypeKind::Struct
            } else if leaf.is_union() {
                ColumnTypeKind::Union
            } else if leaf.is_domain {
                ColumnTypeKind::Domain
            } else {
                ColumnTypeKind::Custom
            };
            (Some(resolved.primitive.to_uppercase()), kind)
        }
        None => (None, ColumnTypeKind::Builtin),
    };
    Some(ColumnTypeInfo {
        declared_name: column.ty_str.clone(),
        array_dimensions: column.array_dimensions(),
        base_type,
        kind,
    })
}

/// Any other expression reports the SQLite primitive behind its type: INTEGER,
/// REAL, NUMERIC or TEXT, or nothing when the value may be anything.
fn expression_type(ty: &ExprType, schema: &Schema) -> Option<ColumnTypeInfo> {
    let (element, array_dimensions) = match ty {
        ExprType::Array {
            element,
            dimensions,
        } => (element.as_ref(), *dimensions),
        scalar => (scalar, 0),
    };
    let declared_name = match element {
        ExprType::Builtin(builtin) => builtin_primitive(*builtin)?,
        ExprType::Untyped => "TEXT",
        ExprType::Custom { type_def, .. } => {
            let resolved = schema.resolve_type_unchecked(&type_def.name).ok()??;
            primitive_of_base_type(&resolved.primitive)?
        }
        ExprType::Null | ExprType::Unknown | ExprType::Array { .. } => return None,
    };
    Some(ColumnTypeInfo {
        declared_name: declared_name.to_string(),
        array_dimensions,
        base_type: None,
        kind: ColumnTypeKind::Builtin,
    })
}

fn builtin_primitive(ty: BuiltinType) -> Option<&'static str> {
    match ty {
        BuiltinType::Integer => Some("INTEGER"),
        BuiltinType::Real => Some("REAL"),
        BuiltinType::Numeric => Some("NUMERIC"),
        BuiltinType::Text => Some("TEXT"),
        BuiltinType::Blob | BuiltinType::Any => None,
    }
}

/// A custom type's base type as one of the primitives above. `blob` and `any` give
/// nothing, like a built-in blob does.
fn primitive_of_base_type(base_type: &str) -> Option<&'static str> {
    match base_type.to_ascii_lowercase().as_str() {
        "integer" => Some("INTEGER"),
        "real" => Some("REAL"),
        "numeric" => Some("NUMERIC"),
        "text" => Some("TEXT"),
        _ => None,
    }
}
