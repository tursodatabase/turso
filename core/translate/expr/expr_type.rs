//! The type of an expression.
//!
//! [`type_of`] is the one place that works out what type an expression has. Code that
//! needs to know whether a value is an array, which custom type it has, or what a
//! wire protocol should report for it asks here instead of matching on expression
//! shapes itself.

use turso_parser::ast;

use super::custom_types::operator_to_str;
use super::get_expr_affinity;
use crate::function::{AggFunc, Func, ScalarFunc};
use crate::schema::{Column, Schema, TypeDef};
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::plan::TableReferences;
use crate::vdbe::affinity::Affinity;

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
    /// The result of an operator or function applied to a custom type or an array.
    /// Which function runs, and so the result type, is not decided here.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuiltinType {
    Integer,
    Real,
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
///
/// The resolver holds the table being written (`SELF_TABLE`) and what is known
/// about registers and subquery results, so every caller sees the same types.
pub(crate) struct TypeEnv<'a> {
    tables: Option<&'a TableReferences>,
    resolver: &'a Resolver<'a>,
}

impl<'a> TypeEnv<'a> {
    pub(crate) fn new(tables: Option<&'a TableReferences>, resolver: &'a Resolver<'a>) -> Self {
        Self { tables, resolver }
    }

    fn schema(&self) -> &Schema {
        self.resolver.schema()
    }
}

pub(crate) fn type_of(expr: &ast::Expr, env: &TypeEnv) -> ExprType {
    match expr {
        ast::Expr::Literal(literal) => literal_type(literal),
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => type_of(&exprs[0], env),
        ast::Expr::Collate(inner, _) => type_of(inner, env),
        ast::Expr::Column { table, column, .. } => {
            column_reference_type(*table, *column, env).unwrap_or_else(|| affinity_type(expr, env))
        }
        ast::Expr::RowId { .. } => ExprType::Builtin(BuiltinType::Integer),
        ast::Expr::Cast {
            type_name: Some(type_name),
            ..
        } => cast_type(type_name, env.schema()),
        ast::Expr::Unary(op, inner) => match op {
            ast::UnaryOperator::Not | ast::UnaryOperator::BitwiseNot => {
                ExprType::Builtin(BuiltinType::Integer)
            }
            ast::UnaryOperator::Negative => arithmetic_type(
                &ExprType::Builtin(BuiltinType::Integer),
                &sqlite_operand_type(type_of(inner, env), None, env.schema()),
            ),
            ast::UnaryOperator::Positive => type_of(inner, env),
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
        ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
        _ => affinity_type(expr, env),
    }
}

/// The custom type of a column reference, for code that must know whether a register
/// holds a custom type's decoded or stored form.
///
/// Only a column read has a known form today: operators read columns decoded and
/// the sorter reads them stored, while a CAST to a custom type gives the stored form
/// and other expressions aren't tracked. Once registers always hold the stored form,
/// `type_of` is enough and this goes away.
pub(crate) fn column_custom_type(expr: &ast::Expr, env: &TypeEnv) -> Option<ColumnCustomType> {
    let ast::Expr::Column { table, column, .. } = expr else {
        return None;
    };
    match column_reference_type(*table, *column, env)? {
        ExprType::Custom { type_def, params } => Some(ColumnCustomType { type_def, params }),
        _ => None,
    }
}

#[allow(clippy::vec_box)]
pub(crate) struct ColumnCustomType {
    pub(crate) type_def: Arc<TypeDef>,
    pub(crate) params: Vec<Box<ast::Expr>>,
}

fn literal_type(literal: &ast::Literal) -> ExprType {
    match literal {
        ast::Literal::Null => ExprType::Null,
        ast::Literal::String(_) => ExprType::Untyped,
        ast::Literal::Blob(_) => ExprType::Builtin(BuiltinType::Blob),
        _ => {
            match crate::translate::alter::literal_default_value(literal).map(|v| v.value_type()) {
                Ok(crate::types::ValueType::Integer) => ExprType::Builtin(BuiltinType::Integer),
                Ok(crate::types::ValueType::Float) => ExprType::Builtin(BuiltinType::Real),
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
    if let Some((_, table)) = env
        .tables
        .and_then(|tables| tables.find_table_by_internal_id(table))
    {
        return Some(column_type(
            table.get_column_at(column)?,
            table.is_strict(),
            env.schema(),
        ));
    }
    if table.is_self_table() {
        let table = env.resolver.self_table()?;
        return Some(column_type(
            table.columns().get(column)?,
            table.is_strict,
            env.schema(),
        ));
    }
    None
}

/// Custom types only apply to columns of STRICT tables.
pub(crate) fn column_type(column: &Column, is_strict: bool, schema: &Schema) -> ExprType {
    let scalar = match schema.get_type_def(&column.ty_str, is_strict) {
        Some(type_def) => ExprType::Custom {
            type_def: Arc::clone(type_def),
            params: column.ty_params.clone(),
        },
        None => builtin_type_of_affinity(column.affinity()),
    };
    ExprType::array(scalar, column.array_dimensions())
}

fn cast_type(type_name: &ast::Type, schema: &Schema) -> ExprType {
    let name = crate::util::dequote(&type_name.name);
    let scalar = match custom_cast_target(&name, type_name, schema) {
        Some(custom) => custom,
        None => builtin_type_of_affinity(Affinity::of_written_type(&name)),
    };
    ExprType::array(scalar, type_name.array_dimensions)
}

/// A CAST to a custom type that needs arguments but has none, such as
/// `CAST(x AS numeric)` for `numeric(precision, scale)`, is a plain SQLite CAST.
fn custom_cast_target(name: &str, type_name: &ast::Type, schema: &Schema) -> Option<ExprType> {
    let type_def = schema.get_type_def_unchecked(name)?;
    let params: Vec<Box<ast::Expr>> = match &type_name.size {
        Some(ast::TypeSize::MaxSize(e)) => vec![e.clone()],
        Some(ast::TypeSize::TypeSize(e1, e2)) => vec![e1.clone(), e2.clone()],
        None => Vec::new(),
    };
    let needed_params = type_def.user_params().count();
    if !type_def.is_domain && needed_params != 0 && params.len() != needed_params {
        return None;
    }
    Some(ExprType::Custom {
        type_def: Arc::clone(type_def),
        params,
    })
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
            let symbol = operator_to_str(op);
            arithmetic_type(
                &sqlite_operand_type(type_of(lhs, env), symbol, env.schema()),
                &sqlite_operand_type(type_of(rhs, env), symbol, env.schema()),
            )
        }
        Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseNot
        | Operator::LeftShift
        | Operator::RightShift
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

/// The type an operand has for SQLite's own arithmetic. A custom type that has a
/// function for the operator stays as it is: the function decides the result. Any
/// other custom value goes to SQLite's operator as a value of the type it is
/// stored as, so `smallint * 2` is an integer.
fn sqlite_operand_type(ty: ExprType, operator: Option<&str>, schema: &Schema) -> ExprType {
    let ExprType::Custom { type_def, .. } = &ty else {
        return ty;
    };
    let has_function = operator.is_some_and(|operator| {
        type_def
            .operators()
            .iter()
            .any(|o| o.op == operator && o.func_name.is_some())
    });
    if has_function {
        return ty;
    }
    stored_type(type_def, schema)
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

/// `INTEGER + INTEGER` is an integer, an integer or real mixed with a real is a
/// real, and any other mix of built-in values is some number. With a custom type
/// or an array on either side the result depends on which function runs.
fn arithmetic_type(lhs: &ExprType, rhs: &ExprType) -> ExprType {
    use BuiltinType::{Integer, Numeric, Real};
    let is_custom_or_array = |ty: &ExprType| {
        matches!(
            ty,
            ExprType::Custom { .. } | ExprType::Array { .. } | ExprType::Unknown
        )
    };
    if is_custom_or_array(lhs) || is_custom_or_array(rhs) {
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
        Func::Scalar(f) if f.returns_array_blob() => {
            ExprType::array(ExprType::Builtin(BuiltinType::Any), 1)
        }
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
        // A call with the wrong number of arguments is reported when it is translated.
        Func::Scalar(ScalarFunc::Nullif | ScalarFunc::Abs | ScalarFunc::ArrayElement)
            if args.is_empty() =>
        {
            ExprType::Builtin(BuiltinType::Any)
        }
        Func::Scalar(ScalarFunc::Nullif) => type_of(&args[0], env),
        Func::Scalar(ScalarFunc::Abs) => arithmetic_type(
            &ExprType::Builtin(BuiltinType::Integer),
            &sqlite_operand_type(type_of(&args[0], env), None, env.schema()),
        ),
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
/// such a value as an array. Numbers widen as in arithmetic. String literals count
/// as text. The same custom type with different parameters, such as numeric(10,2)
/// and numeric(5,1), is that type without parameters. A custom type mixed with any
/// other type counts as the type it is stored as. Any other mix is `Any`.
fn common_type<'e>(exprs: impl Iterator<Item = &'e ast::Expr>, env: &TypeEnv) -> ExprType {
    common_type_of(exprs.map(|expr| type_of(expr, env)), env.schema())
}

/// [`common_type`] for types that are already worked out, such as the types of
/// one result column in each SELECT of a compound SELECT.
pub(crate) fn common_type_of(types: impl Iterator<Item = ExprType>, schema: &Schema) -> ExprType {
    let mut common: Option<ExprType> = None;
    for ty in types {
        common = Some(match (common, ty) {
            (None, ty) | (Some(ExprType::Null), ty) => ty,
            (Some(common), ExprType::Null) => common,
            (Some(common @ ExprType::Array { .. }), _) => common,
            (_, ty @ ExprType::Array { .. }) => ty,
            (Some(common), ty) => common_scalar_type(common, ty, schema),
        });
    }
    common.unwrap_or(ExprType::Null)
}

fn common_scalar_type(a: ExprType, b: ExprType, schema: &Schema) -> ExprType {
    use BuiltinType::{Integer, Numeric, Real, Text};
    match (a, b) {
        (ExprType::Unknown, _) | (_, ExprType::Unknown) => ExprType::Unknown,
        (ExprType::Untyped, ExprType::Untyped) => ExprType::Untyped,
        (
            ExprType::Untyped | ExprType::Builtin(Text),
            ExprType::Untyped | ExprType::Builtin(Text),
        ) => ExprType::Builtin(Text),
        (ExprType::Builtin(a), ExprType::Builtin(b)) if a == b => ExprType::Builtin(a),
        (
            a @ ExprType::Builtin(Integer | Real | Numeric),
            b @ ExprType::Builtin(Integer | Real | Numeric),
        ) => arithmetic_type(&a, &b),
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
    builtin_type_of_affinity(get_expr_affinity(expr, env.tables, Some(env.resolver)))
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
            ("1.5", builtin(BuiltinType::Real)),
            ("'a'", ExprType::Untyped),
            ("x'00'", builtin(BuiltinType::Blob)),
            ("NULL", ExprType::Null),
            ("CAST(1 AS TEXT)", builtin(BuiltinType::Text)),
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
            ("1 + 'a'", builtin(BuiltinType::Numeric)),
            ("-CAST(1 AS REAL)", builtin(BuiltinType::Real)),
            // cents declares no `+`, so SQLite's operator runs on its stored value.
            ("CAST(1 AS cents) + 1", builtin(BuiltinType::Integer)),
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
