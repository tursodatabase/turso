use std::{fmt::Display, ops::Deref};

use limbo_core::numeric::{nonnan::NonNan, Numeric};
use limbo_sqlite3_parser::ast;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

pub(crate) struct Name(pub(crate) String);

impl Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Table {
    pub(crate) rows: Vec<Vec<Value>>,
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) primary: bool,
    pub(crate) unique: bool,
}

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

fn float_to_string<S>(float: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("{}", float))
}

fn string_to_float<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Value {
    Null,
    Integer(i64),
    // we use custom serialization to preserve float precision
    #[serde(
        serialize_with = "float_to_string",
        deserialize_with = "string_to_float"
    )]
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(std::cmp::Ordering::Equal),
            (Self::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Self::Null) => Some(std::cmp::Ordering::Greater),
            (Self::Integer(i1), Self::Integer(i2)) => i1.partial_cmp(i2),
            (Self::Float(f1), Self::Float(f2)) => f1.partial_cmp(f2),
            (Self::Text(t1), Self::Text(t2)) => t1.partial_cmp(t2),
            (Self::Blob(b1), Self::Blob(b2)) => b1.partial_cmp(b2),
            // todo: add type coercions here
            _ => None,
        }
    }
}

fn to_sqlite_blob(bytes: &[u8]) -> String {
    format!(
        "X'{}'",
        bytes
            .iter()
            .fold(String::new(), |acc, b| acc + &format!("{:02X}", b))
    )
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::Text(t) => write!(f, "'{}'", t),
            Self::Blob(b) => write!(f, "{}", to_sqlite_blob(b)),
        }
    }
}

impl Value {
    pub const FALSE: Self = Value::Integer(0);
    pub const TRUE: Self = Value::Integer(1);

    pub fn into_bool(&self) -> bool {
        match Numeric::from(self).try_into_bool() {
            None => false, // Value::Null
            Some(v) => v,
        }
    }

    // TODO: support more predicates
    /// Returns a Value::TRUE or VALUE::FALSE
    pub fn binary_compare(&self, other: &Self, operator: ast::Operator) -> Value {
        match operator {
            ast::Operator::Add => todo!(),
            ast::Operator::And => self.into_bool() && other.into_bool(),
            ast::Operator::ArrowRight => todo!(),
            ast::Operator::ArrowRightShift => todo!(),
            ast::Operator::BitwiseAnd => todo!(),
            ast::Operator::BitwiseOr => todo!(),
            ast::Operator::BitwiseNot => todo!(),
            ast::Operator::Concat => todo!(),
            ast::Operator::Equals => self == other,
            ast::Operator::Divide => todo!(),
            ast::Operator::Greater => self > other,
            ast::Operator::GreaterEquals => self >= other,
            ast::Operator::Is => todo!(),
            ast::Operator::IsNot => todo!(),
            ast::Operator::LeftShift => todo!(),
            ast::Operator::Less => self < other,
            ast::Operator::LessEquals => self <= other,
            ast::Operator::Modulus => todo!(),
            ast::Operator::Multiply => todo!(),
            ast::Operator::NotEquals => self != other,
            ast::Operator::Or => self.into_bool() || other.into_bool(),
            ast::Operator::RightShift => todo!(),
            ast::Operator::Subtract => todo!(),
        }
        .into()
    }

    // TODO: support more operators. Copy the implementation for exec_glob
    pub fn like_compare(&self, other: &Self, operator: ast::LikeOperator) -> bool {
        match operator {
            ast::LikeOperator::Glob => todo!(),
            ast::LikeOperator::Like => {
                // TODO: support ESCAPE `expr` option in AST
                exec_like(self.to_string().as_str(), other.to_string().as_str())
            }
            ast::LikeOperator::Match => todo!(),
            ast::LikeOperator::Regexp => todo!(),
        }
    }
}

/// This function is a duplication of the exec_like function in core/vdbe/mod.rs at commit 9b9d5f9b4c9920e066ef1237c80878f4c3968524
/// Any updates to the original function should be reflected here, otherwise the test will be incorrect.
fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(ch);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
}

fn exec_like(pattern: &str, text: &str) -> bool {
    let re = construct_like_regex(pattern);
    re.is_match(text)
}

impl From<&ast::Literal> for Value {
    fn from(value: &ast::Literal) -> Self {
        match value {
            ast::Literal::Null => Self::Null,
            ast::Literal::Numeric(number) => Numeric::from(number).into(),
            ast::Literal::String(string) => Self::Text(string.clone()),
            ast::Literal::Blob(blob) => Self::Blob(
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
            lit => unimplemented!("{:?}", lit),
        }
    }
}

impl From<ast::Literal> for Value {
    fn from(value: ast::Literal) -> Self {
        match value {
            ast::Literal::Null => Self::Null,
            ast::Literal::Numeric(number) => Numeric::from(number).into(),
            ast::Literal::String(string) => Self::Text(string),
            ast::Literal::Blob(blob) => Self::Blob(
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
            lit => unimplemented!("{:?}", lit),
        }
    }
}

impl From<Value> for ast::Literal {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Integer(i) => Self::Numeric(i.to_string()),
            Value::Float(f) => Self::Numeric(f.to_string()),
            Value::Text(string) => Self::String(string),
            Value::Blob(blob) => Self::Blob(hex::encode(blob)),
        }
    }
}

impl From<&Value> for ast::Literal {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Integer(i) => Self::Numeric(i.to_string()),
            Value::Float(f) => Self::Numeric(f.to_string()),
            Value::Text(string) => Self::String(string.clone()),
            Value::Blob(blob) => Self::Blob(hex::encode(blob)),
        }
    }
}

impl From<Numeric> for Value {
    fn from(value: Numeric) -> Self {
        match value {
            Numeric::Null => Value::Null,
            Numeric::Integer(i) => Value::Integer(i),
            Numeric::Float(f) => Value::Float(f.into()),
        }
    }
}

// Copied from numeric in Core
impl From<Value> for Numeric {
    fn from(value: Value) -> Self {
        Self::from(&value)
    }
}

impl From<&Value> for Numeric {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Integer(v) => Self::Integer(*v),
            Value::Float(v) => match NonNan::new(*v) {
                Some(v) => Self::Float(v),
                None => Self::Null,
            },
            Value::Text(text) => Numeric::from(text.as_str()),
            Value::Blob(blob) => {
                let text = String::from_utf8_lossy(blob.as_slice());
                Numeric::from(&text)
            }
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        value.then_some(Value::TRUE).unwrap_or(Value::FALSE)
    }
}
