//! The types of the PostgreSQL frontend.
//!
//! This started as a literal copy of the SQLite types in
//! `sqlite/frontend/types.rs`. The translator writes types with the names of
//! SQLite types and of Turso's custom types, and core used to read those
//! names by SQLite's rules whatever the dialect. It is a copy and not shared
//! code so that the PostgreSQL types can change without touching SQLite's.
//! What differs from SQLite so far: a CAST of a boolean to text.

use std::sync::{Arc, LazyLock};
use turso_core::ast::{self, TypeHandle, TypePlace, TypeResolver};
use turso_core::dialect::types::{handle_type, type_handle, CastContext, DialectType, TypeEnv};
use turso_core::{Affinity, Numeric, Result, Value};

/// Gives the translator the type of each type name it writes.
pub struct PostgresTypes;

impl TypeResolver for PostgresTypes {
    fn resolve(&self, type_name: &str, place: TypePlace) -> TypeHandle {
        if is_boolean_type(type_name) {
            boolean_handle()
        } else if type_name.eq_ignore_ascii_case("text") {
            text_handle()
        } else {
            handle_of(affinity_of_type_name(type_name, place))
        }
    }
}

fn boolean_handle() -> TypeHandle {
    static BOOLEAN: LazyLock<TypeHandle> = LazyLock::new(|| {
        type_handle(Arc::new(PostgresType {
            affinity: affinity_of_type_name("boolean", TypePlace::Column),
            name: TypeName::Boolean,
        }))
    });
    BOOLEAN.clone()
}

fn text_handle() -> TypeHandle {
    static TEXT: LazyLock<TypeHandle> = LazyLock::new(|| {
        type_handle(Arc::new(PostgresType {
            affinity: affinity_of_type_name("text", TypePlace::Column),
            name: TypeName::Text,
        }))
    });
    TEXT.clone()
}

/// The type of a column or of a CAST, and boolean for an expression that
/// always gives a boolean, such as a comparison. The type of any other
/// expression, such as a function call, is not worked out.
pub(crate) fn type_of(expr: &ast::Expr, env: &dyn TypeEnv) -> Option<Arc<dyn DialectType>> {
    match expr {
        ast::Expr::Column { table, column, .. } => env.column_type(*table, *column),
        ast::Expr::Cast {
            type_name: Some(type_name),
            ..
        } if type_name.array_dimensions == 0 => type_name
            .resolved
            .as_ref()
            .map(|handle| Arc::clone(handle_type(handle))),
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => type_of(&exprs[0], env),
        _ => {
            let column_is_boolean = |table, column| {
                env.column_type(table, column)
                    .is_some_and(|ty| is_boolean(&*ty))
            };
            match boolean_result_type(expr, &column_is_boolean) {
                BooleanResultType::Boolean => Some(Arc::clone(handle_type(&boolean_handle()))),
                BooleanResultType::Null | BooleanResultType::Other => None,
            }
        }
    }
}

pub(crate) enum BooleanResultType {
    Boolean,
    Null,
    Other,
}

// FIXME(pg-catalog): This guesses from the shape of the expression whether an
// expression is boolean, because Turso has no typed expression tree. With
// better binding and resolved expression support, read the type from the
// resolved expression instead. Inferring it from expression shapes here
// is fragile: new expressions and rewrites can lose the boolean type.
pub(crate) fn boolean_result_type(
    expr: &ast::Expr,
    column_is_boolean: &dyn Fn(ast::TableInternalId, usize) -> bool,
) -> BooleanResultType {
    use ast::{Expr, Literal, Operator, UnaryOperator};

    match expr {
        Expr::Literal(Literal::Null) => BooleanResultType::Null,
        Expr::Cast {
            type_name: Some(ty),
            ..
        } if ty.array_dimensions == 0 && is_boolean_type(&ty.name) => BooleanResultType::Boolean,
        Expr::Column { table, column, .. } => {
            if column_is_boolean(*table, *column) {
                BooleanResultType::Boolean
            } else {
                BooleanResultType::Other
            }
        }
        Expr::Binary(
            _,
            Operator::And
            | Operator::Or
            | Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals
            | Operator::Is
            | Operator::IsNot,
            _,
        )
        | Expr::Unary(UnaryOperator::Not, _)
        | Expr::IsNull(_)
        | Expr::NotNull(_)
        | Expr::Like { .. }
        | Expr::Between { .. }
        | Expr::InList { .. }
        | Expr::InSelect { .. }
        | Expr::InTable { .. }
        | Expr::Exists(_) => BooleanResultType::Boolean,
        Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            boolean_result_type(&exprs[0], column_is_boolean)
        }
        Expr::Collate(expr, _) => boolean_result_type(expr, column_is_boolean),
        Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            let mut result = BooleanResultType::Null;
            for expr in when_then_pairs
                .iter()
                .map(|(_, then)| then.as_ref())
                .chain(else_expr.as_deref())
            {
                match boolean_result_type(expr, column_is_boolean) {
                    BooleanResultType::Boolean => result = BooleanResultType::Boolean,
                    BooleanResultType::Null => {}
                    BooleanResultType::Other => return BooleanResultType::Other,
                }
            }
            result
        }
        _ => BooleanResultType::Other,
    }
}

pub(crate) fn is_boolean_type(name: &str) -> bool {
    name.eq_ignore_ascii_case("boolean") || name.eq_ignore_ascii_case("bool")
}

#[derive(Debug)]
struct PostgresType {
    affinity: Affinity,
    name: TypeName,
}

/// The types that the frontend tells apart from an affinity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeName {
    Boolean,
    Text,
    Other,
}

impl DialectType for PostgresType {
    fn cast(
        &self,
        value: &Value,
        from: Option<&dyn DialectType>,
        _context: CastContext,
    ) -> Result<Value> {
        if self.name == TypeName::Text && from.is_some_and(is_boolean) {
            return Ok(boolean_as_text(value));
        }
        Ok(value.exec_cast(self.affinity)?)
    }

    fn comparison_affinity(&self) -> Affinity {
        self.affinity
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl std::fmt::Display for PostgresType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.affinity)
    }
}

fn is_boolean(ty: &dyn DialectType) -> bool {
    ty.as_any()
        .downcast_ref::<PostgresType>()
        .is_some_and(|ty| ty.name == TypeName::Boolean)
}

/// A boolean is stored as the integer 1 or 0.
fn boolean_as_text(value: &Value) -> Value {
    match value {
        Value::Numeric(Numeric::Integer(0)) => Value::build_text("false"),
        _ => Value::build_text("true"),
    }
}

/// The one handle of each affinity, so that reading a type allocates nothing.
fn handle_of(affinity: Affinity) -> TypeHandle {
    static HANDLES: LazyLock<[TypeHandle; 6]> = LazyLock::new(|| {
        [
            Affinity::Blob,
            Affinity::Text,
            Affinity::Numeric,
            Affinity::Integer,
            Affinity::Real,
            Affinity::None,
        ]
        .map(|affinity| {
            type_handle(Arc::new(PostgresType {
                affinity,
                name: TypeName::Other,
            }))
        })
    });
    HANDLES[affinity as usize - 1].clone()
}

fn affinity_of_type_name(type_name: &str, place: TypePlace) -> Affinity {
    let declared_type = match place {
        TypePlace::Column => column_declared_type(type_name),
        TypePlace::Cast => dequote(type_name),
    };
    // A type name that is empty once its quotes are removed, such as `""`,
    // has NUMERIC affinity. A column declared with no type has none to parse.
    if declared_type.is_empty() {
        Affinity::Numeric
    } else {
        affinity_of_declared_type(&declared_type)
    }
}

/// 3.1. Determination Of Column Affinity
/// For tables not declared as STRICT, the affinity of a column is determined by the declared type of the column, according to the following rules in the order shown:
///
/// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
///
/// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
///
/// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
///
/// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
///
/// Otherwise, the affinity is NUMERIC.
///
/// Note that the order of the rules for determining column affinity is important. A column whose declared type is "CHARINT" will match both rules 1 and 2 but the first rule takes precedence and so the column affinity will be INTEGER.
fn affinity_of_declared_type(datatype: &str) -> Affinity {
    const INT: u32 = u32::from_be_bytes([0, b'I', b'N', b'T']);
    const CHAR: u32 = u32::from_be_bytes(*b"CHAR");
    const CLOB: u32 = u32::from_be_bytes(*b"CLOB");
    const TEXT: u32 = u32::from_be_bytes(*b"TEXT");
    const BLOB: u32 = u32::from_be_bytes(*b"BLOB");
    const REAL: u32 = u32::from_be_bytes(*b"REAL");
    const FLOA: u32 = u32::from_be_bytes(*b"FLOA");
    const DOUB: u32 = u32::from_be_bytes(*b"DOUB");

    // One pass finds all the keywords of the rules, as SQLite does: `window`
    // holds the last four bytes in upper case.
    let mut window: u32 = 0;
    let (mut has_text_keyword, mut has_blob, mut has_real_keyword) = (false, false, false);
    for &byte in datatype.as_bytes() {
        window = (window << 8) | u32::from(byte.to_ascii_uppercase());
        // Rule 1: INT -> INTEGER affinity
        if window & 0x00FF_FFFF == INT {
            return Affinity::Integer;
        }
        match window {
            CHAR | CLOB | TEXT => has_text_keyword = true,
            BLOB => has_blob = true,
            REAL | FLOA | DOUB => has_real_keyword = true,
            _ => {}
        }
    }

    // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
    if has_text_keyword {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
    if has_blob || datatype.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL/FLOA/DOUB -> REAL affinity
    if has_real_keyword {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC affinity
    Affinity::Numeric
}

/// Returns a column's declared type from its type name the way SQLite's
/// `sqlite3AddColumn` does: a name that starts with a quote and has no other
/// quote characters before its last character loses its first and last
/// characters, so `[x] INT` becomes `x] IN`. Other names go through [dequote].
fn column_declared_type(type_name: &str) -> String {
    match strip_type_name_quotes(type_name) {
        Some(stripped) => stripped.to_string(),
        None => dequote(type_name),
    }
}

fn strip_type_name_quotes(type_name: &str) -> Option<&str> {
    let is_quote = |b: &u8| matches!(b, b'"' | b'\'' | b'[' | b'`');
    let bytes = type_name.as_bytes();
    if bytes.len() >= 3 && is_quote(&bytes[0]) && !bytes[1..bytes.len() - 1].iter().any(is_quote) {
        let mut chars = type_name.chars();
        chars.next();
        chars.next_back();
        return Some(chars.as_str());
    }
    None
}

/// Removes the quotes around the start of `source` the way SQLite's
/// `sqlite3Dequote` does: when it starts with a quote character, only the
/// first quoted token survives, so `"x" < +5` becomes just `x`.
fn dequote(source: &str) -> String {
    let bytes = source.as_bytes();
    let close = match bytes.first() {
        Some(&c @ (b'"' | b'\'' | b'`')) => c,
        Some(b'[') => b']',
        _ => return source.to_string(),
    };
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == close {
            // A doubled quote is an escaped quote, not the end.
            if close != b']' && bytes.get(i + 1) == Some(&close) {
                i += 2;
                continue;
            }
            break;
        }
        i += 1;
    }
    let inner = &source[1..i.min(source.len())];
    if close == b']' {
        inner.to_string()
    } else {
        let quote = (close as char).to_string();
        inner.replace(&format!("{quote}{quote}"), &quote)
    }
}

/// A translator that gives each type it writes its PostgreSQL type.
pub(crate) fn translator() -> turso_pg_parser::translator::PostgreSQLTranslator {
    turso_pg_parser::translator::PostgreSQLTranslator::new().with_types(Arc::new(PostgresTypes))
}

/// A parser of the SQL that the frontend stores and reads back, which gives
/// each type it reads its PostgreSQL type.
pub(crate) fn stored_sql_parser(sql: &[u8]) -> turso_parser::parser::Parser<'_> {
    turso_parser::parser::Parser::with_types(sql, &PostgresTypes)
}
