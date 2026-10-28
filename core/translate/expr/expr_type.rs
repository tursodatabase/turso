//! The type of an expression.
//!
//! [`type_of`] is the one place that works out what type an expression has. Code that
//! needs to know whether a value is an array, which custom type it has, or what a
//! wire protocol should report for it asks here instead of matching on expression
//! shapes itself.

use turso_parser::ast;

use super::custom_type_operators::find_operator_call;
use super::get_expr_affinity;
use crate::function::{AggFunc, Func, ScalarFunc, WindowFunc};
use crate::schema::{BTreeTable, Column, Schema, Table, TypeDef};
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::plan::{Plan, SelectPlan, TableReferences};
use crate::type_ordering::TypeOrdering;
use crate::vdbe::affinity::Affinity;
use crate::{Result, SymbolTable};

/// Type parameters are kept as written, in the same form as `Column::ty_params`.
#[allow(clippy::vec_box)]
#[derive(Debug, Clone)]
pub(crate) enum ExprType {
    /// A type created with CREATE TYPE or CREATE DOMAIN. `params` are the type's
    /// arguments as written, such as `10, 2` in `numeric(10, 2)`.
    Custom {
        type_def: Arc<TypeDef>,
        params: Vec<Box<ast::Expr>>,
    },
    /// An array. `element` is never itself an array: `integer[][]` is an array of
    /// integers with two dimensions. Build one with [`ExprType::array`].
    Array {
        element: Box<ExprType>,
        dimensions: u32,
    },
    Builtin(BuiltinType),
    /// A string literal. Its type comes from where it is used.
    ///
    /// Parameters (`?`) should be untyped too, but are `Builtin(Any)` for now.
    Untyped,
    Null,
    /// A value whose type nothing declares: the result of a custom type's own
    /// operator function, or of an operator on an array.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuiltinType {
    Integer,
    Real,
    /// A decimal number written in the statement, such as `0.5` or `1e3`. SQLite
    /// stores it as a REAL, but like a PostgreSQL numeric literal it is exact to
    /// a custom type's operators: `numeric_column * 0.1` computes with 0.1, not
    /// with the REAL nearest to it.
    Decimal,
    /// A number that may be an integer or a real.
    Numeric,
    Text,
    Blob,
    /// Any value: a column without a declared type, or a SQLite function that may
    /// return any kind of value.
    Any,
}

impl ExprType {
    pub(crate) fn array(element: ExprType, dimensions: u32) -> Self {
        match element {
            _ if dimensions == 0 => element,
            Self::Array {
                element,
                dimensions: inner,
            } => Self::Array {
                element,
                dimensions: inner + dimensions,
            },
            element => Self::Array {
                element: Box::new(element),
                dimensions,
            },
        }
    }

    pub(crate) fn is_array(&self) -> bool {
        matches!(self, Self::Array { .. })
    }

    /// Whether this is an array whose element type nothing declares, such as
    /// `ARRAY[x]` over a column without a type, or `ARRAY[]`.
    pub(crate) fn is_array_of_unknown_elements(&self) -> bool {
        matches!(self, Self::Array { element, .. } if element.is_unknown_element())
    }

    /// Whether nothing declares the type of this array element.
    fn is_unknown_element(&self) -> bool {
        matches!(
            self,
            Self::Builtin(BuiltinType::Any) | Self::Null | Self::Unknown
        )
    }

    pub(crate) fn array_dimensions(&self) -> u32 {
        match self {
            Self::Array { dimensions, .. } => *dimensions,
            _ => 0,
        }
    }
}

/// Type names are unique within a schema, so two definitions with the same name
/// are the same type.
pub(crate) fn same_type_def(a: &TypeDef, b: &TypeDef) -> bool {
    a.name.eq_ignore_ascii_case(&b.name)
}

/// What an expression's column references and registers refer to.
pub(crate) enum TypeEnv<'a> {
    /// An expression in a statement being translated. The resolver holds the
    /// table being written (`SELF_TABLE`) and what is known about registers
    /// and subquery results, so every caller sees the same types.
    Statement {
        tables: Option<&'a TableReferences>,
        resolver: &'a Resolver<'a>,
    },
    /// An expression over the columns of one table, such as an index
    /// expression read from the schema. Its columns are `SELF_TABLE` columns.
    Table {
        table: &'a BTreeTable,
        schema: &'a Schema,
        syms: &'a SymbolTable,
    },
}

impl<'a> TypeEnv<'a> {
    pub(crate) fn new(tables: Option<&'a TableReferences>, resolver: &'a Resolver<'a>) -> Self {
        Self::Statement { tables, resolver }
    }

    pub(crate) fn over_table(
        table: &'a BTreeTable,
        schema: &'a Schema,
        syms: &'a SymbolTable,
    ) -> Self {
        Self::Table {
            table,
            schema,
            syms,
        }
    }

    pub(crate) fn schema(&self) -> &Schema {
        match self {
            Self::Statement { resolver, .. } => resolver.schema(),
            Self::Table { schema, .. } => schema,
        }
    }

    /// The function a custom type's operator calls.
    pub(crate) fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        match self {
            Self::Statement { resolver, .. } => resolver.resolve_function(name, arg_count),
            // The schema is read without the connection's dialect. Both
            // dialects resolve built-in functions the same way.
            Self::Table { syms, .. } => {
                match crate::dialect::sqlite::resolve_builtin_function(name, arg_count)? {
                    Some(func) => Ok(Some(func)),
                    None => Ok(syms.resolve_function(name, arg_count).map(Func::External)),
                }
            }
        }
    }

    fn tables(&self) -> Option<&TableReferences> {
        match self {
            Self::Statement { tables, .. } => *tables,
            Self::Table { .. } => None,
        }
    }

    fn resolver(&self) -> Option<&Resolver<'a>> {
        match self {
            Self::Statement { resolver, .. } => Some(resolver),
            Self::Table { .. } => None,
        }
    }

    fn self_table_column_type(&self, column: usize) -> Option<ExprType> {
        match self {
            Self::Statement { resolver, .. } => {
                let table = resolver.self_table()?;
                Some(column_type(
                    table.columns().get(column)?,
                    table.is_strict,
                    resolver.schema(),
                ))
            }
            Self::Table { table, schema, .. } => Some(column_type(
                table.columns().get(column)?,
                table.is_strict,
                schema,
            )),
        }
    }
}

pub(crate) fn type_of(expr: &ast::Expr, env: &TypeEnv) -> ExprType {
    let expr = without_wrappers(expr);
    match expr {
        ast::Expr::Literal(literal) => literal_type(literal),
        ast::Expr::Column { table, column, .. } => {
            column_reference_type(*table, *column, env).unwrap_or_else(|| affinity_type(expr, env))
        }
        ast::Expr::RowId { .. } => ExprType::Builtin(BuiltinType::Integer),
        ast::Expr::Register { .. } | ast::Expr::Variable(_) => {
            match column_custom_type(expr, env) {
                Some(ty) => ExprType::Custom {
                    type_def: ty.type_def,
                    params: ty.params,
                },
                None => register_column_type(expr, env).unwrap_or_else(|| affinity_type(expr, env)),
            }
        }
        ast::Expr::Cast {
            type_name: Some(type_name),
            ..
        } => cast_type(type_name, env.schema()),
        ast::Expr::Unary(op, inner) => match op {
            ast::UnaryOperator::Not => ExprType::Builtin(BuiltinType::Integer),
            ast::UnaryOperator::BitwiseNot => {
                custom_operator_type(expr, &[type_of(inner, env)], env)
                    .unwrap_or(ExprType::Builtin(BuiltinType::Integer))
            }
            ast::UnaryOperator::Negative => {
                let inner_type = type_of(inner, env);
                custom_operator_type(expr, core::slice::from_ref(&inner_type), env)
                    .unwrap_or_else(|| negated_type(inner_type, env.schema()))
            }
            ast::UnaryOperator::Positive => {
                unreachable!("without_wrappers looks through unary +")
            }
        },
        ast::Expr::Binary(lhs, op, rhs) => binary_type(lhs, op, rhs, expr, env),
        ast::Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => common_type(
            when_then_pairs
                .iter()
                .map(|(_, then)| then.as_ref())
                .chain(else_expr.as_deref()),
            env,
        ),
        ast::Expr::FunctionCall { name, args, .. } => function_type(name, args, env),
        ast::Expr::SubqueryResult {
            subquery_id,
            query_type:
                ast::SubqueryType::RowValue {
                    result_reg_start,
                    num_regs: 1,
                },
            ..
        } => {
            let Some(resolver) = env.resolver() else {
                return ExprType::Unknown;
            };
            let subqueries = resolver.row_value_subqueries.borrow();
            match subqueries
                .get(subquery_id)
                .and_then(|subquery| subquery.column(*result_reg_start))
            {
                Some(column) => column.expr_type.clone(),
                None => ExprType::Unknown,
            }
        }
        // FULL JOIN ... USING (d): the value of `d` from whichever side has one.
        ast::Expr::MergedColumn(columns) => common_type(columns.iter().map(|c| c.as_ref()), env),
        ast::Expr::FieldAccess {
            base,
            resolved: Some(resolution),
            ..
        } => field_type(&type_of(base, env), resolution, env.schema()),
        // Expressions whose value is never a custom-type value. SQLite's affinity
        // gives their built-in type.
        ast::Expr::Between { .. }
        | ast::Expr::InList { .. }
        | ast::Expr::InSelect { .. }
        | ast::Expr::InTable { .. }
        | ast::Expr::IsNull(_)
        | ast::Expr::NotNull(_)
        | ast::Expr::Like { .. }
        | ast::Expr::Exists(_)
        | ast::Expr::SubqueryResult {
            query_type: ast::SubqueryType::Exists { .. } | ast::SubqueryType::In { .. },
            ..
        }
        | ast::Expr::FunctionCallStar { .. }
        | ast::Expr::Raise(..) => affinity_type(expr, env),
        // Not bound or planned yet: names, subqueries, `DEFAULT`, and field
        // access on an unbound base.
        ast::Expr::Id(_)
        | ast::Expr::Name(_)
        | ast::Expr::Qualified(..)
        | ast::Expr::DoublyQualified(..)
        | ast::Expr::Subquery(_)
        | ast::Expr::Default
        | ast::Expr::FieldAccess { resolved: None, .. }
        | ast::Expr::Cast {
            type_name: None, ..
        } => ExprType::Unknown,
        // A row value such as `(a, b)` or `(SELECT x, y ...)` is not one value.
        ast::Expr::Parenthesized(_)
        | ast::Expr::SubqueryResult {
            query_type: ast::SubqueryType::RowValue { .. },
            ..
        } => ExprType::Unknown,
        ast::Expr::Collate(..) => unreachable!("without_wrappers looks through COLLATE"),
        ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
    }
}

/// The custom type of a column reference, for code that must know whether a register
/// holds a custom type's decoded or stored form.
///
/// Only a column read has a known form today: operators read columns decoded and
/// the sorter reads them stored, while a CAST to a custom type gives the stored form
/// and other expressions aren't tracked. A register or parameter that holds a
/// decoded column value counts as a column read. Once registers always hold the stored form,
/// `type_of` is enough and this goes away.
pub(crate) fn column_custom_type(expr: &ast::Expr, env: &TypeEnv) -> Option<ColumnCustomType> {
    match without_wrappers(expr) {
        ast::Expr::Column { table, column, .. } => {
            match base_table_column_type(*table, *column, env)? {
                ExprType::Custom { type_def, params } => {
                    Some(ColumnCustomType { type_def, params })
                }
                _ => None,
            }
        }
        ast::Expr::Register { custom_type, .. } => Some(ColumnCustomType::from_type_name(
            custom_type.as_deref()?,
            env.schema(),
        )),
        ast::Expr::Variable(variable) => Some(ColumnCustomType::from_type_name(
            variable.custom_type.as_deref()?,
            env.schema(),
        )),
        _ => None,
    }
}

/// The type of a register that stands for a column (see
/// `Resolver::register_columns`), when it doesn't say it holds a decoded
/// custom-type value. Such a register holds a custom type's stored form, so
/// its type is the type the custom type is stored as.
fn register_column_type(expr: &ast::Expr, env: &TypeEnv) -> Option<ExprType> {
    let ast::Expr::Register { register, .. } = expr else {
        return None;
    };
    let column = env.resolver()?.register_columns.get(register)?;
    Some(sqlite_operand_type(
        column.column_type.clone(),
        env.schema(),
    ))
}

/// The expression inside parentheses, COLLATE and unary `+`, which give their
/// operand's value unchanged: `(d)`, `d COLLATE NOCASE` and `+d` are all `d`.
pub(crate) fn without_wrappers(mut expr: &ast::Expr) -> &ast::Expr {
    loop {
        expr = match expr {
            ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => &exprs[0],
            ast::Expr::Collate(inner, _)
            | ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => inner,
            _ => return expr,
        };
    }
}

/// The custom type of a column of a table, for a register that holds its
/// decoded value.
pub(crate) fn column_custom_type_of(
    column: &Column,
    is_strict: bool,
    schema: &Schema,
) -> Option<ColumnCustomType> {
    match column_type(column, is_strict, schema) {
        ExprType::Custom { type_def, params } => Some(ColumnCustomType { type_def, params }),
        _ => None,
    }
}

/// The custom type of each column, written the way a CAST names it, e.g.
/// `numeric(10, 2)`, for registers or parameters that hold the decoded values.
#[allow(clippy::vec_box)]
pub(crate) fn column_custom_type_names(
    columns: &[Column],
    is_strict: bool,
    schema: &Schema,
) -> Vec<Option<Box<ast::Type>>> {
    columns
        .iter()
        .map(|column| {
            column_custom_type_of(column, is_strict, schema).map(|ty| Box::new(ty.type_name()))
        })
        .collect()
}

#[allow(clippy::vec_box)]
#[derive(Clone)]
pub(crate) struct ColumnCustomType {
    pub(crate) type_def: Arc<TypeDef>,
    pub(crate) params: Vec<Box<ast::Expr>>,
}

impl ColumnCustomType {
    /// The type written the way a CAST names it, e.g. `numeric(10, 2)`.
    pub(crate) fn type_name(&self) -> ast::Type {
        let size = match self.params.as_slice() {
            [] => None,
            [max] => Some(ast::TypeSize::MaxSize(max.clone())),
            [precision, scale] => Some(ast::TypeSize::TypeSize(precision.clone(), scale.clone())),
            _ => unreachable!("a column type has at most two parameters"),
        };
        ast::Type {
            name: self.type_def.name.clone(),
            size,
            array_dimensions: 0,
        }
    }

    /// The inverse of [Self::type_name]. The name was written from a column's
    /// type, so the type exists.
    fn from_type_name(type_name: &ast::Type, schema: &Schema) -> Self {
        match custom_cast_target(&type_name.name, type_name, schema) {
            Some(ExprType::Custom { type_def, params }) => Self { type_def, params },
            _ => panic!("custom type {} must exist", type_name.name),
        }
    }
}

fn literal_type(literal: &ast::Literal) -> ExprType {
    match literal {
        ast::Literal::Null => ExprType::Null,
        ast::Literal::String(_) => ExprType::Untyped,
        ast::Literal::Blob(_) => ExprType::Builtin(BuiltinType::Blob),
        _ => {
            match crate::translate::alter::literal_default_value(literal).map(|v| v.value_type()) {
                Ok(crate::types::ValueType::Integer) => ExprType::Builtin(BuiltinType::Integer),
                Ok(crate::types::ValueType::Float) => ExprType::Builtin(BuiltinType::Decimal),
                Ok(crate::types::ValueType::Text) => ExprType::Builtin(BuiltinType::Text),
                _ => ExprType::Builtin(BuiltinType::Any),
            }
        }
    }
}

/// None when the column can't be found, for example a reference into a table that
/// isn't among `env`'s tables.
fn column_reference_type(
    table: ast::TableInternalId,
    column: usize,
    env: &TypeEnv,
) -> Option<ExprType> {
    match env
        .tables()
        .and_then(|tables| tables.find_table_by_internal_id(table))
    {
        Some((_, Table::FromClauseSubquery(subquery))) => Some(query_column_type(
            &subquery.plan,
            column,
            env.resolver()
                .expect("only a statement's expressions refer to FROM clause subqueries"),
        )),
        Some((_, Table::RecursiveCteInput(input))) => input.column_types.get(column).cloned(),
        _ => base_table_column_type(table, column, env),
    }
}

/// The type of column `column` of a query's result. In a compound SELECT it is
/// [compound_column_type], and in a recursive CTE the type the first SELECT
/// gives it, as in PostgreSQL.
pub(crate) fn query_column_type(plan: &Plan, column: usize, resolver: &Resolver) -> ExprType {
    match plan {
        Plan::Select(select) => select_column_type(select, column, resolver),
        Plan::CompoundSelect {
            left, right_most, ..
        } => compound_column_type(
            left.iter()
                .map(|(select, _)| select)
                .chain(core::iter::once(right_most.as_ref())),
            column,
            resolver,
        ),
        Plan::RecursiveCte(plan) => query_column_type(&plan.initial_query, column, resolver),
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE and UPDATE plans have no result columns")
        }
    }
}

/// The type of column `column` of a compound SELECT whose SELECTs are `arms`:
/// the common type of the column in each SELECT. The compound removes
/// duplicates with the order of this type, and an outer query sees this type.
pub(crate) fn compound_column_type<'p>(
    arms: impl IntoIterator<Item = &'p SelectPlan>,
    column: usize,
    resolver: &Resolver,
) -> ExprType {
    common_of_types(
        arms.into_iter()
            .map(|select| select_column_type(select, column, resolver)),
        resolver.schema(),
    )
}

fn select_column_type(select: &SelectPlan, column: usize, resolver: &Resolver) -> ExprType {
    match select.result_columns.get(column) {
        Some(result_column) => type_of(
            &result_column.expr,
            &TypeEnv::new(Some(&select.table_references), resolver),
        ),
        None => ExprType::Unknown,
    }
}

/// The type of a column of a table or of the table being written. Derived
/// tables and recursive CTEs are not included: their columns hold what a SELECT
/// computed, which may be a custom type's stored or decoded form.
fn base_table_column_type(
    table: ast::TableInternalId,
    column: usize,
    env: &TypeEnv,
) -> Option<ExprType> {
    if let Some((_, table)) = env
        .tables()
        .and_then(|tables| tables.find_table_by_internal_id(table))
    {
        if matches!(
            table,
            Table::FromClauseSubquery(_) | Table::RecursiveCteInput(_)
        ) {
            return None;
        }
        return Some(column_type(
            table.get_column_at(column)?,
            table.is_strict(),
            env.schema(),
        ));
    }
    if table.is_self_table() {
        return env.self_table_column_type(column);
    }
    None
}

/// The type of a struct field or union variant, as declared in CREATE TYPE.
fn field_type(
    base: &ExprType,
    resolution: &ast::FieldAccessResolution,
    schema: &Schema,
) -> ExprType {
    let ExprType::Custom { type_def, .. } = base else {
        return ExprType::Unknown;
    };
    let type_name = match resolution {
        ast::FieldAccessResolution::StructField { field_index } => type_def
            .struct_def()
            .and_then(|def| def.fields.get(*field_index))
            .map(|field| &field.type_name),
        ast::FieldAccessResolution::UnionVariant { tag_index } => type_def
            .union_def()
            .and_then(|def| def.variants.iter().find(|v| v.tag_index == *tag_index))
            .map(|variant| &variant.type_name),
    };
    match type_name {
        Some(name) => match schema.get_type_def(name, true) {
            Some(type_def) => ExprType::Custom {
                type_def: Arc::clone(type_def),
                params: Vec::new(),
            },
            None => builtin_type_of_affinity(Affinity::affinity(name)),
        },
        None => ExprType::Unknown,
    }
}

/// The order values of this type compare in, if the type has one: an array,
/// or a custom type whose `OPERATOR '<'` names a function. A domain has the
/// order of the type it is based on.
pub(crate) fn type_ordering(ty: &ExprType, schema: &Schema) -> Option<TypeOrdering> {
    match ty {
        ExprType::Custom { type_def, .. } => schema.type_ordering(type_def),
        ExprType::Array { element, .. } => {
            let element_is_text = matches!(
                sqlite_operand_type(element.as_ref().clone(), schema),
                ExprType::Builtin(BuiltinType::Text) | ExprType::Untyped
            );
            Some(if element_is_text {
                TypeOrdering::TextArray
            } else {
                TypeOrdering::Array
            })
        }
        ExprType::Builtin(_) | ExprType::Untyped | ExprType::Null | ExprType::Unknown => None,
    }
}

/// Custom types only apply to columns of STRICT tables.
pub(crate) fn column_type(column: &Column, is_strict: bool, schema: &Schema) -> ExprType {
    let scalar = match schema.get_type_def(&column.ty_str, is_strict) {
        Some(type_def) => ExprType::Custom {
            type_def: Arc::clone(type_def),
            params: column.ty_params.clone(),
        },
        // A BLOB column of a STRICT table holds only blobs. In other tables it
        // may hold any value, as may an ANY column.
        None if is_strict && column.ty_str.eq_ignore_ascii_case("blob") => {
            ExprType::Builtin(BuiltinType::Blob)
        }
        // An array column stores blobs, so its affinity says nothing about its
        // elements; the declared element type does.
        None if column.is_array() => builtin_type_of_affinity(Affinity::affinity(&column.ty_str)),
        None => builtin_type_of_affinity(column.affinity()),
    };
    ExprType::array(scalar, column.array_dimensions())
}

fn cast_type(type_name: &ast::Type, schema: &Schema) -> ExprType {
    let name = crate::util::dequote(&type_name.name);
    let scalar = match custom_cast_target(&name, type_name, schema) {
        Some(custom) => custom,
        None => match Affinity::of_written_type(&name) {
            // A CAST to BLOB always gives a blob.
            Affinity::Blob => ExprType::Builtin(BuiltinType::Blob),
            affinity => builtin_type_of_affinity(affinity),
        },
    };
    ExprType::array(scalar, type_name.array_dimensions)
}

/// A CAST to a custom type with some but not all of its arguments, such as
/// `CAST(x AS numeric(10))` for `numeric(precision, scale)`, is a plain SQLite
/// CAST. Without arguments it converts to the type without limits.
fn custom_cast_target(name: &str, type_name: &ast::Type, schema: &Schema) -> Option<ExprType> {
    let type_def = schema.get_type_def_unchecked(name)?;
    let params: Vec<Box<ast::Expr>> = match &type_name.size {
        Some(ast::TypeSize::MaxSize(e)) => vec![e.clone()],
        Some(ast::TypeSize::TypeSize(e1, e2)) => vec![e1.clone(), e2.clone()],
        None => Vec::new(),
    };
    let needed_params = type_def.user_params().count();
    if !type_def.is_domain && !params.is_empty() && params.len() != needed_params {
        return None;
    }
    Some(ExprType::Custom {
        type_def: Arc::clone(type_def),
        params,
    })
}

/// The type a function returns. Unlike a CAST target, a custom type needs no
/// arguments: `numeric_add` returns a numeric of any precision and scale.
fn call_result_type(type_name: &ast::Type, schema: &Schema) -> ExprType {
    match schema.get_type_def_unchecked(&type_name.name) {
        Some(type_def) => ExprType::Custom {
            type_def: Arc::clone(type_def),
            params: Vec::new(),
        },
        None => cast_type(type_name, schema),
    }
}

fn binary_type(
    lhs: &ast::Expr,
    op: &ast::Operator,
    rhs: &ast::Expr,
    expr: &ast::Expr,
    env: &TypeEnv,
) -> ExprType {
    use ast::Operator;
    match op {
        Operator::Add
        | Operator::Subtract
        | Operator::Multiply
        | Operator::Divide
        | Operator::Modulus => {
            let types = [type_of(lhs, env), type_of(rhs, env)];
            custom_operator_type(expr, &types, env).unwrap_or_else(|| {
                let [lhs_type, rhs_type] = types;
                arithmetic_type(
                    &sqlite_operand_type(lhs_type, env.schema()),
                    &sqlite_operand_type(rhs_type, env.schema()),
                )
            })
        }
        Operator::BitwiseAnd | Operator::BitwiseOr | Operator::LeftShift | Operator::RightShift => {
            custom_operator_type(expr, &[type_of(lhs, env), type_of(rhs, env)], env)
                .unwrap_or(ExprType::Builtin(BuiltinType::Integer))
        }
        Operator::BitwiseNot
        | Operator::Equals
        | Operator::NotEquals
        | Operator::Less
        | Operator::LessEquals
        | Operator::Greater
        | Operator::GreaterEquals
        | Operator::Is
        | Operator::IsNot
        | Operator::And
        | Operator::Or
        | Operator::ArrayContains
        | Operator::ArrayOverlap => ExprType::Builtin(BuiltinType::Integer),
        // `||` joins arrays when either side is an array.
        Operator::Concat => {
            let lhs = type_of(lhs, env);
            if lhs.is_array() {
                return lhs;
            }
            let rhs = type_of(rhs, env);
            if rhs.is_array() {
                return rhs;
            }
            ExprType::Builtin(BuiltinType::Text)
        }
        Operator::ArrowRight | Operator::ArrowRightShift => affinity_type(expr, env),
    }
}

/// The type of the operator expression `expr` when a custom type's function runs
/// it: what the function returns. `None` means SQLite's operator runs. An
/// operator that does not exist, such as `date * 2`, has no type: emitting it
/// fails. `types` are the operands' types.
fn custom_operator_type(expr: &ast::Expr, types: &[ExprType], env: &TypeEnv) -> Option<ExprType> {
    match find_operator_call(expr, types, env) {
        Ok(Some(call)) => Some(match &call.result {
            Some(result) => call_result_type(result, env.schema()),
            None => ExprType::Unknown,
        }),
        Ok(None) => None,
        Err(_) => Some(ExprType::Unknown),
    }
}

/// The type an operand has for SQLite's own arithmetic. A custom value that no
/// function of its type runs goes to SQLite's operator as a value of the type it
/// is stored as: `smallint * 2` is an integer. A decimal written in the
/// statement is a REAL to SQLite.
pub(super) fn sqlite_operand_type(ty: ExprType, schema: &Schema) -> ExprType {
    match &ty {
        ExprType::Custom { type_def, .. } => stored_type(type_def, schema),
        ExprType::Builtin(BuiltinType::Decimal) => ExprType::Builtin(BuiltinType::Real),
        _ => ty,
    }
}

/// The type of `-x` and `abs(x)`. Negating a decimal written in the statement
/// gives a decimal, as `-0.5` is one.
fn negated_type(ty: ExprType, schema: &Schema) -> ExprType {
    match ty {
        ExprType::Builtin(BuiltinType::Decimal) => ty,
        ty => arithmetic_type(
            &ExprType::Builtin(BuiltinType::Integer),
            &sqlite_operand_type(ty, schema),
        ),
    }
}

/// The built-in type a custom type's values are stored as: its base type, after
/// following domains and custom types built on other custom types.
pub(crate) fn stored_type(type_def: &TypeDef, schema: &Schema) -> ExprType {
    let Ok(Some(resolved)) = schema.resolve_type_unchecked(&type_def.name) else {
        return ExprType::Builtin(BuiltinType::Any);
    };
    ExprType::Builtin(match resolved.primitive.to_ascii_lowercase().as_str() {
        "integer" => BuiltinType::Integer,
        "real" => BuiltinType::Real,
        "numeric" => BuiltinType::Numeric,
        "text" => BuiltinType::Text,
        "blob" => BuiltinType::Blob,
        _ => BuiltinType::Any,
    })
}

/// The result of SQLite's arithmetic: `INTEGER + INTEGER` is an integer, an
/// integer or real mixed with a real is a real, and any other mix of built-in
/// values is some number. With an array or an unknown value on either side the
/// result is unknown.
fn arithmetic_type(lhs: &ExprType, rhs: &ExprType) -> ExprType {
    use BuiltinType::{Integer, Numeric, Real};
    let is_custom_or_decimal = |ty: &ExprType| {
        matches!(
            ty,
            ExprType::Custom { .. } | ExprType::Builtin(BuiltinType::Decimal)
        )
    };
    assert!(
        !is_custom_or_decimal(lhs) && !is_custom_or_decimal(rhs),
        "operands of SQLite arithmetic must be typed as SQLite sees them first"
    );
    let is_array_or_unknown =
        |ty: &ExprType| matches!(ty, ExprType::Array { .. } | ExprType::Unknown);
    if is_array_or_unknown(lhs) || is_array_or_unknown(rhs) {
        return ExprType::Unknown;
    }
    ExprType::Builtin(match (lhs, rhs) {
        (ExprType::Builtin(Integer), ExprType::Builtin(Integer)) => Integer,
        (ExprType::Builtin(Integer | Real), ExprType::Builtin(Integer | Real)) => Real,
        _ => Numeric,
    })
}

fn function_type(name: &ast::Name, args: &[Box<ast::Expr>], env: &TypeEnv) -> ExprType {
    let Ok(Some(func)) = Func::resolve_function(name.as_str(), args.len()) else {
        return ExprType::Builtin(BuiltinType::Any);
    };
    match func {
        Func::Scalar(ScalarFunc::Array) => {
            ExprType::array(common_type(args.iter().map(|arg| arg.as_ref()), env), 1)
        }
        Func::Scalar(ScalarFunc::StringToArray) => {
            ExprType::array(ExprType::Builtin(BuiltinType::Text), 1)
        }
        // The other functions that build an array give an array of the type of
        // their array argument: array_prepend(element, array) and
        // array_cat(array1, array2) take it second when the first is not one.
        Func::Scalar(f) if f.returns_array_blob() => args
            .iter()
            .map(|arg| type_of(arg, env))
            .find(ExprType::is_array)
            .unwrap_or_else(|| ExprType::array(ExprType::Builtin(BuiltinType::Any), 1)),
        Func::Agg(AggFunc::ArrayAgg) => ExprType::array(
            args.first()
                .map_or(ExprType::Builtin(BuiltinType::Any), |arg| type_of(arg, env)),
            1,
        ),
        Func::Scalar(
            ScalarFunc::Coalesce | ScalarFunc::IfNull | ScalarFunc::Max | ScalarFunc::Min,
        )
        | Func::Agg(AggFunc::Max | AggFunc::Min) => {
            common_type(args.iter().map(|arg| arg.as_ref()), env)
        }
        Func::Scalar(ScalarFunc::Iif) => common_type(args.iter().skip(1).map(|a| a.as_ref()), env),
        // A value of another row: its own argument's type, or for lag and
        // lead the common type of the argument and the default (the third
        // argument).
        Func::Window(WindowFunc::Lag | WindowFunc::Lead) => common_type(
            args.iter()
                .enumerate()
                .filter(|(i, _)| *i != 1)
                .map(|(_, arg)| arg.as_ref()),
            env,
        ),
        Func::Window(WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue) => args
            .first()
            .map_or(ExprType::Builtin(BuiltinType::Any), |arg| type_of(arg, env)),
        // A call with the wrong number of arguments is reported when it is translated.
        Func::Scalar(
            ScalarFunc::Nullif
            | ScalarFunc::Likely
            | ScalarFunc::Unlikely
            | ScalarFunc::Likelihood
            | ScalarFunc::Abs
            | ScalarFunc::ArrayElement,
        ) if args.is_empty() => ExprType::Builtin(BuiltinType::Any),
        // nullif returns its first argument or NULL; likely, unlikely and
        // likelihood return their first argument unchanged.
        Func::Scalar(
            ScalarFunc::Nullif | ScalarFunc::Likely | ScalarFunc::Unlikely | ScalarFunc::Likelihood,
        ) => type_of(&args[0], env),
        Func::Scalar(ScalarFunc::Abs) => negated_type(type_of(&args[0], env), env.schema()),
        Func::Scalar(ScalarFunc::ArrayElement) => match type_of(&args[0], env) {
            ExprType::Array {
                element,
                dimensions,
            } => ExprType::array(*element, dimensions - 1),
            _ => ExprType::Builtin(BuiltinType::Any),
        },
        _ => ExprType::Builtin(BuiltinType::Any),
    }
}

/// The type of a value that comes from one of several expressions, such as the
/// branches of a CASE or the arguments of COALESCE, under SQLite's rules. The
/// PostgreSQL frontend will choose its own common type through the dialect.
///
/// NULLs don't count. An array wins over anything else, as main has always decoded
/// such a value as an array. Two arrays give an array of the common type of
/// their elements, where elements of unknown type take the other array's
/// element type. Numbers widen as in arithmetic, except that a decimal
/// written in the statement mixed with an integer stays a decimal: both are
/// exact, as in PostgreSQL. String literals count
/// as text. The same custom type with different parameters, such as numeric(10,2)
/// and numeric(5,1), is that type without parameters. A type that computes with
/// exact numbers, such as numeric, mixed with an integer or a decimal is that
/// type without parameters. A custom type mixed with any other type counts as
/// the type it is stored as. Any other mix is `Any`.
fn common_type<'e>(exprs: impl Iterator<Item = &'e ast::Expr>, env: &TypeEnv) -> ExprType {
    common_of_types(exprs.map(|expr| type_of(expr, env)), env.schema())
}

fn common_of_types(types: impl Iterator<Item = ExprType>, schema: &Schema) -> ExprType {
    let mut common: Option<ExprType> = None;
    for ty in types {
        common = Some(match (common, ty) {
            (None, ty) | (Some(ExprType::Null), ty) => ty,
            (Some(common), ExprType::Null) => common,
            (
                Some(ExprType::Array {
                    element: a,
                    dimensions: a_dimensions,
                }),
                ExprType::Array {
                    element: b,
                    dimensions: b_dimensions,
                },
            ) => ExprType::array(
                common_element_type(*a, *b, schema),
                a_dimensions.max(b_dimensions),
            ),
            (Some(common @ ExprType::Array { .. }), _) => common,
            (_, ty @ ExprType::Array { .. }) => ty,
            (Some(common), ty) => common_scalar_type(common, ty, schema),
        });
    }
    common.unwrap_or(ExprType::Null)
}

fn common_element_type(a: ExprType, b: ExprType, schema: &Schema) -> ExprType {
    match (a, b) {
        (a, b) if a.is_unknown_element() => b,
        (a, b) if b.is_unknown_element() => a,
        (a, b) => common_scalar_type(a, b, schema),
    }
}

fn common_scalar_type(a: ExprType, b: ExprType, schema: &Schema) -> ExprType {
    use BuiltinType::{Decimal, Integer, Numeric, Real, Text};
    match (a, b) {
        (ExprType::Unknown, _) | (_, ExprType::Unknown) => ExprType::Unknown,
        (ExprType::Untyped, ExprType::Untyped) => ExprType::Untyped,
        (
            ExprType::Untyped | ExprType::Builtin(Text),
            ExprType::Untyped | ExprType::Builtin(Text),
        ) => ExprType::Builtin(Text),
        (ExprType::Builtin(a), ExprType::Builtin(b)) if a == b => ExprType::Builtin(a),
        (ExprType::Builtin(Decimal), ExprType::Builtin(Integer))
        | (ExprType::Builtin(Integer), ExprType::Builtin(Decimal)) => ExprType::Builtin(Decimal),
        (
            a @ ExprType::Builtin(Integer | Real | Decimal | Numeric),
            b @ ExprType::Builtin(Integer | Real | Decimal | Numeric),
        ) => arithmetic_type(
            &sqlite_operand_type(a, schema),
            &sqlite_operand_type(b, schema),
        ),
        (
            ExprType::Custom {
                type_def,
                params: a_params,
            },
            ExprType::Custom {
                type_def: b_type_def,
                params: b_params,
            },
        ) if same_type_def(&type_def, &b_type_def) => ExprType::Custom {
            type_def,
            params: if a_params == b_params {
                a_params
            } else {
                Vec::new()
            },
        },
        // A string literal is a value of the custom type it is mixed with, as in
        // PostgreSQL: coalesce(date_column, '2024-01-01') is a date.
        (custom @ ExprType::Custom { .. }, ExprType::Untyped)
        | (ExprType::Untyped, custom @ ExprType::Custom { .. }) => custom,
        // An integer or a decimal mixed with a type that computes with exact
        // numbers is a value of that type without parameters, as PostgreSQL
        // converts integer to numeric: coalesce(numeric_column, 0) is a numeric.
        (ExprType::Custom { type_def, .. }, ExprType::Builtin(Integer | Decimal))
        | (ExprType::Builtin(Integer | Decimal), ExprType::Custom { type_def, .. })
            if type_def.computes_with_exact_numbers() =>
        {
            ExprType::Custom {
                type_def,
                params: Vec::new(),
            }
        }
        // Mixed with anything else, including another custom type, a custom value
        // is a value of the type it is stored as: coalesce(smallint_column, 0) and
        // coalesce(smallint_column, bigint_column) are integers.
        (ExprType::Custom { type_def, .. }, other) | (other, ExprType::Custom { type_def, .. }) => {
            common_scalar_type(stored_type(&type_def, schema), other, schema)
        }
        _ => ExprType::Builtin(BuiltinType::Any),
    }
}

/// For expressions whose type is only known through their SQLite affinity: a column
/// that could not be found, a register, a subquery result.
fn affinity_type(expr: &ast::Expr, env: &TypeEnv) -> ExprType {
    builtin_type_of_affinity(get_expr_affinity(expr, env.tables(), env.resolver()))
}

fn builtin_type_of_affinity(affinity: Affinity) -> ExprType {
    ExprType::Builtin(match affinity {
        Affinity::Integer => BuiltinType::Integer,
        Affinity::Real => BuiltinType::Real,
        Affinity::Numeric => BuiltinType::Numeric,
        Affinity::Text => BuiltinType::Text,
        Affinity::Blob | Affinity::None => BuiltinType::Any,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::emitter::DoubleQuotedDml;
    use crate::{DatabaseCatalog, RwLock, SymbolTable};
    use rustc_hash::FxHashMap as HashMap;
    use turso_parser::ast::{Cmd, OneSelect, ResultColumn, Stmt};
    use turso_parser::parser::Parser;

    impl PartialEq for ExprType {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (
                    Self::Custom {
                        type_def: a,
                        params: a_params,
                    },
                    Self::Custom {
                        type_def: b,
                        params: b_params,
                    },
                ) => same_type_def(a, b) && a_params == b_params,
                (
                    Self::Array {
                        element: a,
                        dimensions: a_dims,
                    },
                    Self::Array {
                        element: b,
                        dimensions: b_dims,
                    },
                ) => a == b && a_dims == b_dims,
                (Self::Builtin(a), Self::Builtin(b)) => a == b,
                (Self::Untyped, Self::Untyped)
                | (Self::Null, Self::Null)
                | (Self::Unknown, Self::Unknown) => true,
                _ => false,
            }
        }
    }

    #[test]
    fn literals_and_casts() {
        check(&[
            ("1", builtin(BuiltinType::Integer)),
            ("1.5", builtin(BuiltinType::Decimal)),
            ("-1.5", builtin(BuiltinType::Decimal)),
            ("abs(-1.5)", builtin(BuiltinType::Decimal)),
            ("'a'", ExprType::Untyped),
            ("x'00'", builtin(BuiltinType::Blob)),
            ("NULL", ExprType::Null),
            ("CAST(1 AS TEXT)", builtin(BuiltinType::Text)),
            ("CAST(1 AS BLOB)", builtin(BuiltinType::Blob)),
            ("CAST(1 AS cents)", custom("cents", &[])),
            ("CAST(1 AS numeric(10, 2))", custom("numeric", &["10", "2"])),
            // numeric needs a precision and a scale, so without them this is
            // SQLite's NUMERIC.
            ("CAST(1 AS numeric)", builtin(BuiltinType::Numeric)),
            (
                "CAST(1 AS cents[])",
                ExprType::array(custom("cents", &[]), 1),
            ),
        ]);
    }

    #[test]
    fn values_from_several_branches() {
        check(&[
            (
                "CASE WHEN 1 THEN 1 ELSE 2.5 END",
                builtin(BuiltinType::Decimal),
            ),
            (
                "CASE WHEN 1 THEN CAST(1 AS REAL) ELSE 2.5 END",
                builtin(BuiltinType::Real),
            ),
            ("CASE WHEN 1 THEN 'a' END", ExprType::Untyped),
            (
                "coalesce(NULL, 'a', CAST(1 AS TEXT))",
                builtin(BuiltinType::Text),
            ),
            ("coalesce(1, 'a')", builtin(BuiltinType::Any)),
            (
                "coalesce(CAST(1 AS cents), CAST(2 AS cents))",
                custom("cents", &[]),
            ),
            (
                "coalesce(CAST(1 AS numeric(10, 2)), CAST(2 AS numeric(10, 2)))",
                custom("numeric", &["10", "2"]),
            ),
            (
                "coalesce(CAST(1 AS numeric(10, 2)), CAST(2 AS numeric(5, 1)))",
                custom("numeric", &[]),
            ),
            // A custom value and a plain one have the custom type's stored type.
            (
                "coalesce(CAST(1 AS cents), 1)",
                builtin(BuiltinType::Integer),
            ),
            // Except that numeric computes with integers and decimals.
            (
                "coalesce(CAST(1 AS numeric(10, 2)), 0)",
                custom("numeric", &[]),
            ),
            (
                "coalesce(CAST(1 AS numeric(10, 2)), 0.5)",
                custom("numeric", &[]),
            ),
            (
                "coalesce(CAST(1 AS numeric(10, 2)), CAST(0.5 AS REAL))",
                builtin(BuiltinType::Any),
            ),
            // Two different custom types have their common stored type.
            (
                "coalesce(CAST(1 AS cents), CAST(2 AS smallint))",
                builtin(BuiltinType::Integer),
            ),
            ("iif(1, NULL, 2)", builtin(BuiltinType::Integer)),
        ]);
    }

    #[test]
    fn arithmetic() {
        check(&[
            ("1 + 2", builtin(BuiltinType::Integer)),
            ("1 + 2.5", builtin(BuiltinType::Real)),
            ("0.5 + 2.5", builtin(BuiltinType::Real)),
            ("1 + 'a'", builtin(BuiltinType::Numeric)),
            ("-CAST(1 AS REAL)", builtin(BuiltinType::Real)),
            // cents declares no `+`, so the statement fails with "operator does not exist".
            ("CAST(1 AS cents) + 1", ExprType::Unknown),
            // Nor any other operator.
            ("-CAST(1 AS cents)", ExprType::Unknown),
            ("~CAST(1 AS cents)", ExprType::Unknown),
            ("CAST(1 AS cents) & 1", ExprType::Unknown),
            ("-CAST(1 AS numeric(10, 2))", custom("numeric", &[])),
            ("-CAST(1 AS smallint)", builtin(BuiltinType::Integer)),
            ("CAST(1 AS smallint) << 1", builtin(BuiltinType::Integer)),
            ("abs(CAST(1 AS cents))", builtin(BuiltinType::Integer)),
            ("1 || 2", builtin(BuiltinType::Text)),
        ]);
    }

    #[test]
    fn arrays_never_nest() {
        check(&[
            (
                "array_agg(CAST(1 AS INTEGER[]))",
                ExprType::array(builtin(BuiltinType::Integer), 2),
            ),
            (
                "array_element(CAST(1 AS INTEGER[][]), 1)",
                ExprType::array(builtin(BuiltinType::Integer), 1),
            ),
            (
                "array_element(CAST(1 AS INTEGER[]), 1)",
                builtin(BuiltinType::Integer),
            ),
            (
                "CAST(1 AS INTEGER[]) || 2",
                ExprType::array(builtin(BuiltinType::Integer), 1),
            ),
        ]);
    }

    fn check(cases: &[(&str, ExprType)]) {
        let schema = test_schema();
        let syms = SymbolTable::new();
        let database_schemas = RwLock::new(HashMap::default());
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let namespaces = crate::Qualifiers::default();
        let temp_database = RwLock::new(None);
        let resolver = Resolver::new(
            &schema,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &namespaces,
            &syms,
            true,
            DoubleQuotedDml::Enabled,
            Arc::new(crate::dialect::SqliteDialect),
            None,
        );
        let env = TypeEnv::new(None, &resolver);
        let mismatches: Vec<String> = cases
            .iter()
            .filter_map(|(sql, expected)| {
                let actual = type_of(&parse_expr(sql), &env);
                (actual != *expected)
                    .then(|| format!("  {sql}: expected {expected:?}, got {actual:?}"))
            })
            .collect();
        assert!(
            mismatches.is_empty(),
            "wrong types:\n{}",
            mismatches.join("\n")
        );
    }

    fn parse_expr(sql: &str) -> ast::Expr {
        let select_sql = format!("SELECT {sql}");
        let mut parser = Parser::new(select_sql.as_bytes());
        let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next_cmd() else {
            panic!("not a SELECT: {sql}");
        };
        let OneSelect::Select { mut columns, .. } = select.body.select else {
            panic!("not a plain SELECT: {sql}");
        };
        let ResultColumn::Expr(expr, _) = columns.remove(0) else {
            panic!("not an expression: {sql}");
        };
        *expr
    }

    fn builtin(ty: BuiltinType) -> ExprType {
        ExprType::Builtin(ty)
    }

    /// A custom type from the test schema, with its parameters as written.
    fn custom(name: &str, params: &[&str]) -> ExprType {
        let schema = test_schema();
        ExprType::Custom {
            type_def: Arc::clone(schema.get_type_def_unchecked(name).unwrap()),
            params: params.iter().map(|p| Box::new(parse_expr(p))).collect(),
        }
    }

    /// The built-in types plus `cents`.
    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql(
                "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
            )
            .unwrap();
        schema
    }
}
