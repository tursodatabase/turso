//! The type information `Statement::get_column_type_info` reports for each result
//! column. It is worked out while the statement is compiled, when the tables,
//! registers and types the result columns refer to are known. Also the declared
//! type `Statement::get_column_decltype` reports.

use turso_parser::ast;

use super::emitter::Resolver;
use super::expr::expr_type::{type_of, BuiltinType, ExprType, TypeEnv};
use super::plan::{Plan, RecursiveCtePlan, ResultSetColumn, SelectPlan, TableReferences};
use crate::schema::{Schema, Table};
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
        BuiltinType::Integer => Some("INTEGER"),
        BuiltinType::Real | BuiltinType::Decimal => Some("REAL"),
        BuiltinType::Numeric => Some("NUMERIC"),
        BuiltinType::Text => Some("TEXT"),
        BuiltinType::Blob | BuiltinType::Any => None,
    }
}

/// The type name `sqlite3_column_decltype` reports for a result column: the
/// declared type of the table column it reads, also when the column is read
/// through a FROM-clause subquery, a view or a CTE, and nothing for any other
/// expression. Like SQLite, a compound SELECT reports the types of its
/// right-most SELECT, a rowid reports INTEGER, and the self-reference of a
/// recursive CTE reports the types of the CTE's initial SELECT.
pub(crate) fn declared_type<'a>(
    expr: &ast::Expr,
    table_references: &'a TableReferences,
) -> Option<&'a str> {
    declared_type_inside(expr, table_references, None)
}

fn declared_type_inside<'a>(
    expr: &ast::Expr,
    table_references: &'a TableReferences,
    recursive_cte: Option<&'a RecursiveCtePlan>,
) -> Option<&'a str> {
    let (table, column) = match expr {
        ast::Expr::RowId { .. } => return Some("INTEGER"),
        ast::Expr::Column { table, column, .. } => (*table, *column),
        _ => return None,
    };
    let (_, table_ref) = table_references.find_table_by_internal_id(table)?;
    let (plan, recursive_cte) = match table_ref {
        Table::FromClauseSubquery(subquery) => match subquery.plan.as_ref() {
            Plan::RecursiveCte(cte) => (cte.recursive_query.as_ref(), Some(cte.as_ref())),
            plan => (plan, recursive_cte),
        },
        Table::RecursiveCteInput(_) => {
            let cte = recursive_cte.filter(|cte| cte.input_table_id == table)?;
            (cte.initial_query.as_ref(), None)
        }
        Table::BTree(_) | Table::Virtual(_) => {
            let ty = table_ref.get_column_at(column)?.ty_str.as_str();
            return (!ty.is_empty()).then_some(ty);
        }
    };
    let select = right_most_select(plan)?;
    let result_column = select.result_columns.get(column)?;
    declared_type_inside(&result_column.expr, &select.table_references, recursive_cte)
}

fn right_most_select(plan: &Plan) -> Option<&SelectPlan> {
    match plan {
        Plan::Select(select) => Some(select),
        Plan::CompoundSelect { right_most, .. } => Some(right_most),
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => None,
    }
}
