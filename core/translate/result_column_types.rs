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
    let (base_type, kind) = custom_type_info(&column.ty_str, table.is_strict(), schema);
    Some(ColumnTypeInfo {
        declared_name: column.ty_str.clone(),
        array_dimensions: column.array_dimensions(),
        base_type,
        kind,
    })
}

/// Any other expression reports the SQLite primitive behind its type: INTEGER,
/// REAL, NUMERIC or TEXT, or nothing when the value may be anything. An
/// expression of a custom type reports it the way a column of that type does.
fn expression_type(ty: &ExprType, schema: &Schema) -> Option<ColumnTypeInfo> {
    let (element, array_dimensions) = match ty {
        ExprType::Array {
            element,
            dimensions,
        } => (element.as_ref(), *dimensions),
        scalar => (scalar, 0),
    };
    let declared_name = match element {
        ExprType::Builtin(builtin) => builtin_primitive(*builtin)?.to_string(),
        ExprType::Untyped => "TEXT".to_string(),
        ExprType::Custom { type_def, .. } => {
            let (base_type, kind) = custom_type_info(&type_def.name, true, schema);
            return Some(ColumnTypeInfo {
                declared_name: type_def.name.clone(),
                array_dimensions,
                base_type,
                kind,
            });
        }
        ExprType::Null | ExprType::Unknown | ExprType::Array { .. } => return None,
    };
    Some(ColumnTypeInfo {
        declared_name,
        array_dimensions,
        base_type: None,
        kind: ColumnTypeKind::Builtin,
    })
}

/// The stored primitive and kind of a type name. STRUCT and UNION are tagged on
/// `TypeDefKind`, DOMAIN on `TypeDef.is_domain`, and anything else registered
/// through CREATE TYPE is a Custom. A name that isn't in the type registry is a
/// Builtin.
fn custom_type_info(
    type_name: &str,
    is_strict: bool,
    schema: &Schema,
) -> (Option<String>, ColumnTypeKind) {
    let Some(resolved) = schema.resolve_type(type_name, is_strict).ok().flatten() else {
        return (None, ColumnTypeKind::Builtin);
    };
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

fn builtin_primitive(ty: BuiltinType) -> Option<&'static str> {
    match ty {
        BuiltinType::Integer | BuiltinType::Boolean => Some("INTEGER"),
        BuiltinType::Real | BuiltinType::Decimal => Some("REAL"),
        BuiltinType::Numeric => Some("NUMERIC"),
        BuiltinType::Text => Some("TEXT"),
        BuiltinType::Blob | BuiltinType::Any => None,
    }
}
