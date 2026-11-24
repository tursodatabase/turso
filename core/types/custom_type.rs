//! Types other than the storage types of SQLite (INTEGER, REAL, TEXT, BLOB).
//!
//! [`Type`] says what a type takes, stores and reads back. A type is written
//! in Rust, as the built-in types are (see `builtin.rs`), or in SQL with
//! CREATE TYPE, which is [`UserType`]. The schema keeps a
//! [`TypeDef`](crate::schema::TypeDef) for each registered type.

use super::StorageClass;
use crate::ast;
use crate::schema::{OperatorSignature, RangeOffsets};
use crate::type_ordering::TypeOrdering;
use crate::{Result, Value};
use std::fmt::Debug;

/// A Rust function that converts `value`, which is never NULL. `params` are
/// the values written after the type name, such as 10 and 2 in
/// `numeric(10, 2)`, with NULL for one that is left out. Only a COERCE gets
/// them. An ENCODE and a DECODE get none.
pub type ConversionFn = fn(value: &Value, params: &[Value]) -> Result<Value>;

/// How a type converts a value when it stores the value or reads it back.
#[derive(Debug, Clone)]
pub enum Conversion {
    /// The value stays as it is. `ENCODE value` and `DECODE value` say this in
    /// CREATE TYPE.
    Unchanged,
    /// A Rust function converts the value. NULL is not converted.
    Function(ConversionFn),
    /// A SQL expression gives the converted value. It reads the value as
    /// `value`. The translator compiles it into each statement that uses the
    /// type.
    Expr(Box<ast::Expr>),
}

impl Conversion {
    /// Whether the converted value is always the value itself.
    pub fn keeps_value(&self) -> bool {
        match self {
            Conversion::Unchanged => true,
            Conversion::Function(_) => false,
            Conversion::Expr(expr) => {
                matches!(&**expr, ast::Expr::Id(name) if name.as_str().eq_ignore_ascii_case("value"))
            }
        }
    }

    /// The SQL expression, when the conversion is written in SQL.
    pub fn expr(&self) -> Option<&ast::Expr> {
        match self {
            Conversion::Expr(expr) => Some(expr),
            Conversion::Unchanged | Conversion::Function(_) => None,
        }
    }
}

/// The conversion as the type listing of `PRAGMA list_types` shows it: the
/// SQL expression, or `built-in` for a Rust function, which has no SQL text.
impl std::fmt::Display for Conversion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Conversion::Unchanged => f.write_str("value"),
            Conversion::Function(_) => f.write_str("built-in"),
            Conversion::Expr(expr) => write!(f, "{expr}"),
        }
    }
}

/// What a type takes, stores and reads back.
///
/// A column of the type stores a value in three steps. ENCODE turns the value
/// that was written into the form the type stores. The stored form is then
/// converted to the BASE as a STRICT column of the BASE converts it. COERCE
/// checks or adjusts the stored form with the parameters of the column, such
/// as the 10 and 2 of `numeric(10, 2)`. Reading a column runs DECODE on the
/// stored form.
///
/// The schema reads a `Type` once, when the type is registered.
pub trait Type: Debug + Send + Sync {
    /// The name of the type, in lowercase.
    fn name(&self) -> &str;

    /// Other names of the type, such as `bool` for `boolean`.
    fn aliases(&self) -> &[&str] {
        &[]
    }

    /// The parameters of the type, in the order CREATE TYPE writes them:
    /// `value` with the type of the values the type takes, then the
    /// parameters a column gives after the type name.
    fn params(&self) -> Vec<ast::TypeParam>;

    /// The SQLite type that holds the stored form: integer, real, text or
    /// blob.
    fn base(&self) -> &str;

    /// ENCODE: turns a value that is written into the stored form.
    fn encode(&self) -> Option<Conversion> {
        None
    }

    /// COERCE: checks or adjusts the stored form with the parameters of the
    /// column.
    fn coerce(&self) -> Option<Conversion> {
        None
    }

    /// DECODE: turns the stored form into the value that is read.
    fn decode(&self) -> Option<Conversion> {
        None
    }

    /// The value of a column of the type that has no DEFAULT of its own.
    fn default(&self) -> Option<Box<ast::Expr>> {
        None
    }

    /// The operators that CREATE TYPE declares with OPERATOR.
    fn operators(&self) -> Vec<ast::TypeOperator> {
        Vec::new()
    }

    /// Whether values of the type take a COLLATE.
    fn accepts_collate(&self) -> bool {
        false
    }

    /// The offsets a RANGE frame over values of the type may have.
    fn range_offsets(&self) -> RangeOffsets {
        RangeOffsets::None
    }

    /// Whether the stored form sorts in the same order as the values.
    fn stored_form_sorts_in_value_order(&self) -> bool {
        false
    }

    /// How values of the type compare, when the stored forms do not compare
    /// in the order of the values.
    fn ordering(&self) -> Option<TypeOrdering> {
        None
    }

    /// The kind of value DECODE gives. A type whose DECODE is a Rust function
    /// has to say it. Without one, a built-in type reads as the kind it
    /// stores, and the kind of a [`UserType`] is worked out from its DECODE.
    fn reads_as(&self) -> Option<StorageClass> {
        None
    }

    /// How operators such as `+` and `-x` run on values of the type.
    fn operator_signatures(&self) -> Vec<OperatorSignature> {
        Vec::new()
    }
}

/// A type that a CREATE TYPE statement defines. Its ENCODE, COERCE and DECODE
/// are the SQL expressions of the statement.
#[derive(Debug, Clone)]
pub struct UserType {
    pub name: String,
    pub params: Vec<ast::TypeParam>,
    pub base: String,
    pub encode: Option<Box<ast::Expr>>,
    pub coerce: Option<Box<ast::Expr>>,
    pub decode: Option<Box<ast::Expr>>,
    pub operators: Vec<ast::TypeOperator>,
    pub default: Option<Box<ast::Expr>>,
}

impl Type for UserType {
    fn name(&self) -> &str {
        &self.name
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        self.params.clone()
    }

    fn base(&self) -> &str {
        &self.base
    }

    fn encode(&self) -> Option<Conversion> {
        self.encode.clone().map(Conversion::Expr)
    }

    fn coerce(&self) -> Option<Conversion> {
        self.coerce.clone().map(Conversion::Expr)
    }

    fn decode(&self) -> Option<Conversion> {
        self.decode.clone().map(Conversion::Expr)
    }

    fn default(&self) -> Option<Box<ast::Expr>> {
        self.default.clone()
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        self.operators.clone()
    }

    /// A user type takes a COLLATE when its values are text.
    fn accepts_collate(&self) -> bool {
        self.base.eq_ignore_ascii_case("text")
    }
}
