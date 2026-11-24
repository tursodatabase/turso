//! The type information `Statement::get_column_type_info` reports for each result
//! column. It is worked out while the statement is compiled, when the tables,
//! registers and types the result columns refer to are known.

use crate::ast;

use super::emitter::Resolver;
use super::expr::expr_type::{type_of, BuiltinType, ExprType, TypeEnv};
use super::plan::{Plan, ResultSetColumn, TableReferences};
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

/// The types of the rows a SELECT, compound SELECT or recursive CTE returns.
/// A compound SELECT takes each column's type from its leftmost arm. Where that
/// arm returns a NULL literal, the column takes its type from the first later
/// arm that has one, so that `SELECT NULL UNION ALL SELECT ARRAY[1]` is an
/// array column.
pub(crate) fn plan_result_column_types(
    plan: &Plan,
    resolver: &Resolver,
) -> Vec<Option<ColumnTypeInfo>> {
    match plan {
        Plan::Select(select) => {
            result_column_types(&select.result_columns, &select.table_references, resolver)
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut arms = left
                .iter()
                .map(|(select, _)| select)
                .chain(std::iter::once(right_most.as_ref()));
            let leftmost = arms.next().expect("a compound SELECT has a leftmost arm");
            let mut types = result_column_types(
                &leftmost.result_columns,
                &leftmost.table_references,
                resolver,
            );
            let untyped_null: Vec<usize> = leftmost
                .result_columns
                .iter()
                .enumerate()
                .filter(|(_, column)| matches!(column.expr, ast::Expr::Literal(ast::Literal::Null)))
                .map(|(i, _)| i)
                .collect();
            // FIXME(pg-catalog): this hand-codes one case of PostgreSQL's UNION
            // type resolution (an untyped NULL takes the type of the other
            // arms). An expression tree that carries PostgreSQL types would give
            // the compound column its type, from every arm.
            if untyped_null.is_empty() {
                return types;
            }
            for arm in arms {
                let arm_types =
                    result_column_types(&arm.result_columns, &arm.table_references, resolver);
                for &i in &untyped_null {
                    if types[i].is_none() {
                        types[i].clone_from(&arm_types[i]);
                    }
                }
            }
            types
        }
        Plan::RecursiveCte(recursive_cte) => {
            plan_result_column_types(&recursive_cte.initial_query, resolver)
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("plan_result_column_types called on a non-SELECT plan")
        }
    }
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
/// REAL, NUMERIC or TEXT, or nothing when the value may be anything. An array
/// of values that may be anything reports BLOB or ANY elements. An expression
/// of a custom type reports it the way a column of that type does.
fn expression_type(ty: &ExprType, schema: &Schema) -> Option<ColumnTypeInfo> {
    let (element, array_dimensions) = match ty {
        ExprType::Array {
            element,
            dimensions,
        } => (element.as_ref(), *dimensions),
        scalar => (scalar, 0),
    };
    let declared_name = match element {
        ExprType::Builtin(builtin) => match builtin_primitive(*builtin) {
            Some(primitive) => primitive.to_string(),
            // An array is reported as one whatever its elements are, because
            // the rows the statement returns convert it to text by this type.
            None if array_dimensions > 0 => builtin.sqlite_name().to_string(),
            None => return None,
        },
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
        ExprType::Null
        | ExprType::Unknown
        | ExprType::NotKnownYet
        | ExprType::RowValue
        | ExprType::Array { .. } => {
            if array_dimensions == 0 {
                return None;
            }
            "ANY".to_string()
        }
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
        BuiltinType::Integer
        | BuiltinType::Real
        | BuiltinType::Decimal
        | BuiltinType::Numeric
        | BuiltinType::Text => Some(ty.sqlite_name()),
        BuiltinType::Blob | BuiltinType::Any => None,
    }
}
