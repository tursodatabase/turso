use std::{fmt::Display, hash::Hash, ops::Deref};

use serde::{Deserialize, Serialize};
use turso_core::{numeric::Numeric, types};
use turso_sqlite3_parser::ast;

use crate::model::query::predicate::Predicate;

pub(crate) struct Name(pub(crate) String);

impl Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ContextColumn<'a> {
    pub table_name: &'a str,
    pub column: &'a Column,
}

pub trait TableContext {
    fn columns<'a>(&'a self) -> impl Iterator<Item = ContextColumn<'a>>;
    fn rows(&self) -> &Vec<Vec<SimValue>>;
}

impl TableContext for Table {
    fn columns<'a>(&'a self) -> impl Iterator<Item = ContextColumn<'a>> {
        self.columns.iter().map(|col| ContextColumn {
            column: col,
            table_name: &self.name,
        })
    }

    fn rows(&self) -> &Vec<Vec<SimValue>> {
        &self.rows
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) rows: Vec<Vec<SimValue>>,
}

impl Table {
    pub fn anonymous(rows: Vec<Vec<SimValue>>) -> Self {
        Self {
            rows,
            name: "".to_string(),
            columns: vec![],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) primary: bool,
    pub(crate) unique: bool,
}

// Uniquely defined by name in this case
impl Hash for Column {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Column {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ColumnType {
    Integer,
    Float,
    Text,
    Blob,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => write!(f, "INTEGER"),
            Self::Float => write!(f, "REAL"),
            Self::Text => write!(f, "TEXT"),
            Self::Blob => write!(f, "BLOB"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinedTable {
    /// table name
    pub table: String,
    /// `JOIN` type
    pub join_type: JoinType,
    /// `ON` clause
    pub on: Predicate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl TableContext for JoinTable {
    fn columns<'a>(&'a self) -> impl Iterator<Item = ContextColumn<'a>> {
        self.tables.iter().flat_map(|table| table.columns())
    }

    fn rows(&self) -> &Vec<Vec<SimValue>> {
        &self.rows
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinTable {
    pub tables: Vec<Table>,
    pub rows: Vec<Vec<SimValue>>,
}

fn float_to_string<S>(float: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("{float}"))
}

fn string_to_float<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub(crate) struct SimValue(pub turso_core::Value);

fn to_sqlite_blob(bytes: &[u8]) -> String {
    format!(
        "X'{}'",
        bytes
            .iter()
            .fold(String::new(), |acc, b| acc + &format!("{b:02X}"))
    )
}

impl Display for SimValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            types::Value::Null => write!(f, "NULL"),
            types::Value::Integer(i) => write!(f, "{i}"),
            types::Value::Float(fl) => write!(f, "{fl}"),
            value @ types::Value::Text(..) => write!(f, "'{value}'"),
            types::Value::Blob(b) => write!(f, "{}", to_sqlite_blob(b)),
        }
    }
}

impl SimValue {
    pub const FALSE: Self = SimValue(types::Value::Integer(0));
    pub const TRUE: Self = SimValue(types::Value::Integer(1));

    pub fn as_bool(&self) -> bool {
        Numeric::from(&self.0).try_into_bool().unwrap_or_default()
    }

    // TODO: support more predicates
    /// Returns a Result of a Binary Operation
    ///
    /// TODO: forget collations for now
    /// TODO: have the [ast::Operator::Equals], [ast::Operator::NotEquals], [ast::Operator::Greater],
    /// [ast::Operator::GreaterEquals], [ast::Operator::Less], [ast::Operator::LessEquals] function to be extracted
    /// into its functions in turso_core so that it can be used here
    pub fn binary_compare(&self, other: &Self, operator: ast::Operator) -> SimValue {
        match operator {
            ast::Operator::Add => self.0.exec_add(&other.0).into(),
            ast::Operator::And => self.0.exec_and(&other.0).into(),
            ast::Operator::ArrowRight => todo!(),
            ast::Operator::ArrowRightShift => todo!(),
            ast::Operator::BitwiseAnd => self.0.exec_bit_and(&other.0).into(),
            ast::Operator::BitwiseOr => self.0.exec_bit_or(&other.0).into(),
            ast::Operator::BitwiseNot => todo!(), // TODO: Do not see any function usage of this operator in Core
            ast::Operator::Concat => self.0.exec_concat(&other.0).into(),
            ast::Operator::Equals => (self == other).into(),
            ast::Operator::Divide => self.0.exec_divide(&other.0).into(),
            ast::Operator::Greater => (self > other).into(),
            ast::Operator::GreaterEquals => (self >= other).into(),
            // TODO: Test these implementations
            ast::Operator::Is => match (&self.0, &other.0) {
                (types::Value::Null, types::Value::Null) => true.into(),
                (types::Value::Null, _) => false.into(),
                (_, types::Value::Null) => false.into(),
                _ => self.binary_compare(other, ast::Operator::Equals),
            },
            ast::Operator::IsNot => self
                .binary_compare(other, ast::Operator::Is)
                .unary_exec(ast::UnaryOperator::Not),
            ast::Operator::LeftShift => self.0.exec_shift_left(&other.0).into(),
            ast::Operator::Less => (self < other).into(),
            ast::Operator::LessEquals => (self <= other).into(),
            ast::Operator::Modulus => self.0.exec_remainder(&other.0).into(),
            ast::Operator::Multiply => self.0.exec_multiply(&other.0).into(),
            ast::Operator::NotEquals => (self != other).into(),
            ast::Operator::Or => self.0.exec_or(&other.0).into(),
            ast::Operator::RightShift => self.0.exec_shift_right(&other.0).into(),
            ast::Operator::Subtract => self.0.exec_subtract(&other.0).into(),
        }
    }

    // TODO: support more operators. Copy the implementation for exec_glob
    pub fn like_compare(&self, other: &Self, operator: ast::LikeOperator) -> bool {
        match operator {
            ast::LikeOperator::Glob => todo!(),
            ast::LikeOperator::Like => {
                // TODO: support ESCAPE `expr` option in AST
                // TODO: regex cache
                types::Value::exec_like(
                    None,
                    other.0.to_string().as_str(),
                    self.0.to_string().as_str(),
                )
            }
            ast::LikeOperator::Match => todo!(),
            ast::LikeOperator::Regexp => todo!(),
        }
    }

    pub fn unary_exec(&self, operator: ast::UnaryOperator) -> SimValue {
        let new_value = match operator {
            ast::UnaryOperator::BitwiseNot => self.0.exec_bit_not(),
            ast::UnaryOperator::Negative => {
                SimValue(types::Value::Integer(0))
                    .binary_compare(self, ast::Operator::Subtract)
                    .0
            }
            ast::UnaryOperator::Not => self.0.exec_boolean_not(),
            ast::UnaryOperator::Positive => self.0.clone(),
        };
        Self(new_value)
    }
}

impl From<ast::Literal> for SimValue {
    fn from(value: ast::Literal) -> Self {
        Self::from(&value)
    }
}

/// Converts a SQL string literal with already-escaped single quotes to a regular string by:
/// - Removing the enclosing single quotes
/// - Converting sequences of 2N single quotes ('''''') to N single quotes (''')
///
/// Assumes:
/// - The input starts and ends with a single quote
/// - The input contains a valid amount of single quotes inside the enclosing quotes;
///   i.e. any ' is escaped as a double ''
fn unescape_singlequotes(input: &str) -> String {
    assert!(
        input.starts_with('\'') && input.ends_with('\''),
        "Input string must be wrapped in single quotes"
    );
    // Skip first and last characters (the enclosing quotes)
    let inner = &input[1..input.len() - 1];

    let mut result = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\'' {
            // Count consecutive single quotes
            let mut quote_count = 1;
            while chars.peek() == Some(&'\'') {
                quote_count += 1;
                chars.next();
            }
            assert!(
                quote_count % 2 == 0,
                "Expected even number of quotes, got {quote_count} in string {input}"
            );
            // For every pair of quotes, output one quote
            for _ in 0..(quote_count / 2) {
                result.push('\'');
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// Escapes a string by doubling contained single quotes and then wrapping it in single quotes.
fn escape_singlequotes(input: &str) -> String {
    let mut result = String::with_capacity(input.len() + 2);
    result.push('\'');
    result.push_str(&input.replace("'", "''"));
    result.push('\'');
    result
}

impl From<&ast::Literal> for SimValue {
    fn from(value: &ast::Literal) -> Self {
        let new_value = match value {
            ast::Literal::Null => types::Value::Null,
            ast::Literal::Numeric(number) => Numeric::from(number).into(),
            ast::Literal::String(string) => types::Value::build_text(unescape_singlequotes(string)),
            ast::Literal::Blob(blob) => types::Value::Blob(
                blob.as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        // We assume that sqlite3-parser has already validated that
                        // the input is valid hex string, thus unwrap is safe.
                        let hex_byte = std::str::from_utf8(pair).unwrap();
                        u8::from_str_radix(hex_byte, 16).unwrap()
                    })
                    .collect(),
            ),
            ast::Literal::Keyword(keyword) => match keyword.to_uppercase().as_str() {
                "TRUE" => types::Value::Integer(1),
                "FALSE" => types::Value::Integer(0),
                "NULL" => types::Value::Null,
                _ => unimplemented!("Unsupported keyword literal: {}", keyword),
            },
            lit => unimplemented!("{:?}", lit),
        };
        Self(new_value)
    }
}

impl From<SimValue> for ast::Literal {
    fn from(value: SimValue) -> Self {
        Self::from(&value)
    }
}

impl From<&SimValue> for ast::Literal {
    fn from(value: &SimValue) -> Self {
        match &value.0 {
            types::Value::Null => Self::Null,
            types::Value::Integer(i) => Self::Numeric(i.to_string()),
            types::Value::Float(f) => Self::Numeric(f.to_string()),
            text @ types::Value::Text(..) => Self::String(escape_singlequotes(&text.to_string())),
            types::Value::Blob(blob) => Self::Blob(hex::encode(blob)),
        }
    }
}

impl From<bool> for SimValue {
    fn from(value: bool) -> Self {
        if value {
            SimValue::TRUE
        } else {
            SimValue::FALSE
        }
    }
}

impl From<SimValue> for turso_core::types::Value {
    fn from(value: SimValue) -> Self {
        value.0
    }
}

impl From<&SimValue> for turso_core::types::Value {
    fn from(value: &SimValue) -> Self {
        value.0.clone()
    }
}

impl From<turso_core::types::Value> for SimValue {
    fn from(value: turso_core::types::Value) -> Self {
        Self(value)
    }
}

impl From<&turso_core::types::Value> for SimValue {
    fn from(value: &turso_core::types::Value) -> Self {
        Self(value.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::model::table::{escape_singlequotes, unescape_singlequotes};

    #[test]
    fn test_unescape_singlequotes() {
        assert_eq!(unescape_singlequotes("'hello'"), "hello");
        assert_eq!(unescape_singlequotes("'O''Reilly'"), "O'Reilly");
        assert_eq!(
            unescape_singlequotes("'multiple''single''quotes'"),
            "multiple'single'quotes"
        );
        assert_eq!(unescape_singlequotes("'test''''test'"), "test''test");
        assert_eq!(unescape_singlequotes("'many''''''quotes'"), "many'''quotes");
    }

    #[test]
    fn test_escape_singlequotes() {
        assert_eq!(escape_singlequotes("hello"), "'hello'");
        assert_eq!(escape_singlequotes("O'Reilly"), "'O''Reilly'");
        assert_eq!(
            escape_singlequotes("multiple'single'quotes"),
            "'multiple''single''quotes'"
        );
        assert_eq!(escape_singlequotes("test''test"), "'test''''test'");
        assert_eq!(escape_singlequotes("many'''quotes"), "'many''''''quotes'");
    }
}
