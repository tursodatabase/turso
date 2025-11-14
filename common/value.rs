use either::Either;
#[cfg(feature = "serde")]
use serde::Deserialize;
use std::{
    borrow::{Borrow, Cow},
    fmt::{Debug, Display},
    ops::Deref,
};

use crate::numeric::format_float;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueType {
    Null,
    Integer,
    Float,
    Text,
    Blob,
    Error,
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Null => "NULL",
            Self::Integer => "INT",
            Self::Float => "REAL",
            Self::Blob => "BLOB",
            Self::Text => "TEXT",
            Self::Error => "ERROR",
        };
        write!(f, "{value}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TextSubtype {
    Text,
    #[cfg(feature = "json")]
    Json,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Text {
    pub value: Cow<'static, str>,
    pub subtype: TextSubtype,
}

impl Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Text {
    pub fn new(value: impl Into<Cow<'static, str>>) -> Self {
        Self {
            value: value.into(),
            subtype: TextSubtype::Text,
        }
    }
    #[cfg(feature = "json")]
    pub fn json(value: String) -> Self {
        Self {
            value: value.into(),
            subtype: TextSubtype::Json,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TextRef<'a> {
    pub value: &'a str,
    pub subtype: TextSubtype,
}

impl<'a> TextRef<'a> {
    pub fn new(value: &'a str, subtype: TextSubtype) -> Self {
        Self { value, subtype }
    }

    #[inline]
    pub fn as_str(&self) -> &'a str {
        self.value
    }
}

impl<'a> Borrow<str> for TextRef<'a> {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<'a> Deref for TextRef<'a> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

pub trait Extendable<T> {
    fn do_extend(&mut self, other: &T);
}

impl<T: AnyText> Extendable<T> for Text {
    #[inline(always)]
    fn do_extend(&mut self, other: &T) {
        let value = self.value.to_mut();
        value.clear();
        value.push_str(other.as_ref());
        self.subtype = other.subtype();
    }
}

impl<T: AnyBlob> Extendable<T> for Vec<u8> {
    #[inline(always)]
    fn do_extend(&mut self, other: &T) {
        self.clear();
        self.extend_from_slice(other.as_slice());
    }
}

pub trait AnyText: AsRef<str> {
    fn subtype(&self) -> TextSubtype;
}

impl AnyText for Text {
    fn subtype(&self) -> TextSubtype {
        self.subtype
    }
}

impl AnyText for &str {
    fn subtype(&self) -> TextSubtype {
        TextSubtype::Text
    }
}

pub trait AnyBlob {
    fn as_slice(&self) -> &[u8];
}

impl AnyBlob for Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AnyBlob for &[u8] {
    fn as_slice(&self) -> &[u8] {
        self
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Text {
            value: value.to_owned().into(),
            subtype: TextSubtype::Text,
        }
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Text {
            value: Cow::from(value),
            subtype: TextSubtype::Text,
        }
    }
}

impl From<Text> for String {
    fn from(value: Text) -> Self {
        value.value.into_owned()
    }
}

#[cfg(feature = "serde")]
fn float_to_string<S>(float: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("{float}"))
}

#[cfg(feature = "serde")]
fn string_to_float<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match crate::numeric::str_to_f64(s) {
        Some(result) => Ok(result.into()),
        None => Err(serde::de::Error::custom("")),
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    Null,
    Integer(i64),
    // we use custom serialization to preserve float precision
    #[cfg_attr(
        feature = "serde",
        serde(
            serialize_with = "float_to_string",
            deserialize_with = "string_to_float"
        )
    )]
    Float(f64),
    Text(Text),
    Blob(Vec<u8>),
}

/// Please use Display trait for all limbo output so we have single origin of truth
/// When you need value as string:
/// ---GOOD---
/// format!("{}", value);
/// ---BAD---
/// match value {
///   Value::Integer(i) => *i.as_str(),
///   Value::Float(f) => *f.as_str(),
///   ....
/// }
impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, ""),
            Self::Integer(i) => {
                write!(f, "{i}")
            }
            Self::Float(fl) => f.write_str(&format_float(*fl)),
            Self::Text(s) => {
                write!(f, "{}", s.as_str())
            }
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        let (left, right) = (self.as_value_ref(), other.as_value_ref());
        left.eq(&right)
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let (left, right) = (self.as_value_ref(), other.as_value_ref());
        left.cmp(&right)
    }
}

impl std::ops::Add<Value> for Value {
    type Output = Value;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::ops::Add<f64> for Value {
    type Output = Value;

    fn add(mut self, rhs: f64) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::ops::Add<i64> for Value {
    type Output = Value;

    fn add(mut self, rhs: i64) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::ops::AddAssign for Value {
    fn add_assign(mut self: &mut Self, rhs: Self) {
        match (&mut self, rhs) {
            (Self::Integer(int_left), Self::Integer(int_right)) => *int_left += int_right,
            (Self::Integer(int_left), Self::Float(float_right)) => {
                *self = Self::Float(*int_left as f64 + float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                *self = Self::Float(*float_left + int_right as f64)
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                *float_left += float_right;
            }
            (Self::Text(string_left), Self::Text(string_right)) => {
                string_left.value.to_mut().push_str(&string_right.value);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Text(string_left), Self::Integer(int_right)) => {
                let string_right = int_right.to_string();
                string_left.value.to_mut().push_str(&string_right);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Integer(int_left), Self::Text(string_right)) => {
                let string_left = int_left.to_string();
                *self = Self::build_text(string_left + string_right.as_str());
            }
            (Self::Text(string_left), Self::Float(float_right)) => {
                let string_right = Self::Float(float_right).to_string();
                string_left.value.to_mut().push_str(&string_right);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Float(float_left), Self::Text(string_right)) => {
                let string_left = Self::Float(*float_left).to_string();
                *self = Self::build_text(string_left + string_right.as_str());
            }
            (_, Self::Null) => {}
            (Self::Null, rhs) => *self = rhs,
            _ => *self = Self::Float(0.0),
        }
    }
}

impl std::ops::AddAssign<i64> for Value {
    fn add_assign(&mut self, rhs: i64) {
        match self {
            Self::Integer(int_left) => *int_left += rhs,
            Self::Float(float_left) => *float_left += rhs as f64,
            _ => unreachable!(),
        }
    }
}

impl std::ops::AddAssign<f64> for Value {
    fn add_assign(&mut self, rhs: f64) {
        match self {
            Self::Integer(int_left) => *self = Self::Float(*int_left as f64 + rhs),
            Self::Float(float_left) => *float_left += rhs,
            _ => unreachable!(),
        }
    }
}

impl std::ops::Div<Value> for Value {
    type Output = Value;

    fn div(self, rhs: Value) -> Self::Output {
        match (self, rhs) {
            (Self::Integer(int_left), Self::Integer(int_right)) => {
                Self::Integer(int_left / int_right)
            }
            (Self::Integer(int_left), Self::Float(float_right)) => {
                Self::Float(int_left as f64 / float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                Self::Float(float_left / int_right as f64)
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                Self::Float(float_left / float_right)
            }
            _ => Self::Float(0.0),
        }
    }
}

impl std::ops::DivAssign<Value> for Value {
    fn div_assign(&mut self, rhs: Value) {
        *self = self.clone() / rhs;
    }
}

impl Value {
    pub fn as_ref<'a>(&'a self) -> ValueRef<'a> {
        match self {
            Value::Null => ValueRef::Null,
            Value::Integer(v) => ValueRef::Integer(*v),
            Value::Float(v) => ValueRef::Float(*v),
            Value::Text(v) => ValueRef::Text(TextRef {
                value: &v.value,
                subtype: v.subtype,
            }),
            Value::Blob(v) => ValueRef::Blob(v.as_slice()),
        }
    }

    // A helper function that makes building a text Value easier.
    pub fn build_text(text: impl Into<Cow<'static, str>>) -> Self {
        Self::Text(Text::new(text))
    }

    pub fn to_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(blob) => Some(blob),
            _ => None,
        }
    }

    pub fn from_blob(data: Vec<u8>) -> Self {
        Value::Blob(data)
    }

    pub fn to_text(&self) -> Option<&str> {
        match self {
            Value::Text(t) => Some(t.as_str()),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> &Vec<u8> {
        match self {
            Value::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }

    pub fn as_blob_mut(&mut self) -> &mut Vec<u8> {
        match self {
            Value::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }
    pub fn as_float(&self) -> f64 {
        match self {
            Value::Float(f) => *f,
            Value::Integer(i) => *i as f64,
            _ => panic!("as_float must be called only for Value::Float or Value::Integer"),
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_uint(&self) -> u64 {
        match self {
            Value::Integer(i) => (*i).cast_unsigned(),
            _ => 0,
        }
    }

    pub fn from_text(text: impl Into<Cow<'static, str>>) -> Self {
        Value::Text(Text::new(text))
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Integer(_) => ValueType::Integer,
            Value::Float(_) => ValueType::Float,
            Value::Text(_) => ValueType::Text,
            Value::Blob(_) => ValueType::Blob,
        }
    }

    /// Cast Value to String, if Value is NULL returns None
    pub fn cast_text(&self) -> Option<String> {
        Some(match self {
            Value::Null => return None,
            v => v.to_string(),
        })
    }
}

#[derive(Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(TextRef<'a>),
    Blob(&'a [u8]),
}

impl<'a> ValueRef<'a> {
    pub fn to_blob(&self) -> Option<&'a [u8]> {
        match self {
            Self::Blob(blob) => Some(*blob),
            _ => None,
        }
    }

    pub fn to_text(&self) -> Option<&'a str> {
        match self {
            Self::Text(t) => Some(t.as_str()),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> &'a [u8] {
        match self {
            Self::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }

    pub fn as_float(&self) -> f64 {
        match self {
            Self::Float(f) => *f,
            Self::Integer(i) => *i as f64,
            _ => panic!("as_float must be called only for Value::Float or Value::Integer"),
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_uint(&self) -> u64 {
        match self {
            Self::Integer(i) => (*i).cast_unsigned(),
            _ => 0,
        }
    }

    pub fn to_owned(&self) -> Value {
        match self {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(*i),
            ValueRef::Float(f) => Value::Float(*f),
            ValueRef::Text(text) => Value::Text(Text {
                value: text.value.to_string().into(),
                subtype: text.subtype,
            }),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Integer(_) => ValueType::Integer,
            Self::Float(_) => ValueType::Float,
            Self::Text(_) => ValueType::Text,
            Self::Blob(_) => ValueType::Blob,
        }
    }
}

impl Display for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Float(fl) => write!(f, "{fl:?}"),
            Self::Text(s) => write!(f, "{}", s.as_str()),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

impl Debug for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueRef::Null => write!(f, "Null"),
            ValueRef::Integer(i) => f.debug_tuple("Integer").field(i).finish(),
            ValueRef::Float(float) => f.debug_tuple("Float").field(float).finish(),
            ValueRef::Text(text_ref) => {
                // truncate string to at most 256 chars
                let text = text_ref.as_str();
                let max_len = text.len().min(256);
                f.debug_struct("Text")
                    .field("data", &&text[0..max_len])
                    // Indicates to the developer debugging that the data is truncated for printing
                    .field("truncated", &(text.len() > max_len))
                    .finish()
            }
            ValueRef::Blob(blob) => {
                // truncate blob_slice to at most 32 bytes
                let max_len = blob.len().min(32);
                f.debug_struct("Blob")
                    .field("data", &&blob[0..max_len])
                    // Indicates to the developer debugging that the data is truncated for printing
                    .field("truncated", &(blob.len() > max_len))
                    .finish()
            }
        }
    }
}

impl<'a> PartialEq<ValueRef<'a>> for ValueRef<'a> {
    fn eq(&self, other: &ValueRef<'a>) -> bool {
        match (self, other) {
            (Self::Integer(int_left), Self::Integer(int_right)) => int_left == int_right,
            (Self::Integer(int), Self::Float(float)) | (Self::Float(float), Self::Integer(int)) => {
                sqlite_int_float_compare(*int, *float).is_eq()
            }
            (Self::Float(float_left), Self::Float(float_right)) => float_left == float_right,
            (Self::Integer(_) | Self::Float(_), Self::Text(_) | Self::Blob(_)) => false,
            (Self::Text(_) | Self::Blob(_), Self::Integer(_) | Self::Float(_)) => false,
            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.as_bytes() == text_right.value.as_bytes()
            }
            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.eq(blob_right),
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl<'a> PartialEq<Value> for ValueRef<'a> {
    fn eq(&self, other: &Value) -> bool {
        let other = other.as_value_ref();
        self.eq(&other)
    }
}

impl<'a> Eq for ValueRef<'a> {}

#[expect(clippy::non_canonical_partial_ord_impl)]
impl<'a> PartialOrd<ValueRef<'a>> for ValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(int_left), Self::Integer(int_right)) => int_left.partial_cmp(int_right),
            (Self::Integer(int_left), Self::Float(float_right)) => {
                (*int_left as f64).partial_cmp(float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                float_left.partial_cmp(&(*int_right as f64))
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                float_left.partial_cmp(float_right)
            }
            // Numeric vs Text/Blob
            (Self::Integer(_) | Self::Float(_), Self::Text(_) | Self::Blob(_)) => {
                Some(std::cmp::Ordering::Less)
            }
            (Self::Text(_) | Self::Blob(_), Self::Integer(_) | Self::Float(_)) => {
                Some(std::cmp::Ordering::Greater)
            }

            (Self::Text(text_left), Self::Text(text_right)) => text_left
                .value
                .as_bytes()
                .partial_cmp(text_right.value.as_bytes()),
            // Text vs Blob
            (Self::Text(_), Self::Blob(_)) => Some(std::cmp::Ordering::Less),
            (Self::Blob(_), Self::Text(_)) => Some(std::cmp::Ordering::Greater),

            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.partial_cmp(blob_right),
            (Self::Null, Self::Null) => Some(std::cmp::Ordering::Equal),
            (Self::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Self::Null) => Some(std::cmp::Ordering::Greater),
        }
    }
}

impl<'a> Ord for ValueRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub fn sqlite_int_float_compare(int_val: i64, float_val: f64) -> std::cmp::Ordering {
    if float_val.is_nan() {
        return std::cmp::Ordering::Greater;
    }

    if float_val < -9223372036854775808.0 {
        return std::cmp::Ordering::Greater;
    }
    if float_val >= 9223372036854775808.0 {
        return std::cmp::Ordering::Less;
    }

    let float_as_int = float_val as i64;
    match int_val.cmp(&float_as_int) {
        std::cmp::Ordering::Equal => {
            let int_as_float = int_val as f64;
            int_as_float
                .partial_cmp(&float_val)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        other => other,
    }
}

pub trait AsValueRef {
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a>;
}

impl<'b> AsValueRef for ValueRef<'b> {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        *self
    }
}

impl AsValueRef for Value {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        self.as_ref()
    }
}

impl AsValueRef for &mut Value {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        self.as_ref()
    }
}

impl<V1, V2> AsValueRef for Either<V1, V2>
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        match self {
            Either::Left(left) => left.as_value_ref(),
            Either::Right(right) => right.as_value_ref(),
        }
    }
}

impl<V: AsValueRef> AsValueRef for &V {
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        (*self).as_value_ref()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FromSqlError {
    #[error("Null value")]
    NullValue,
    #[error("invalid column type")]
    InvalidColumnType,
    #[error("Invalid blob size, expected {0}")]
    InvalidBlobSize(usize),
}

/// Convert a `Value` into the implementors type.
pub trait FromValue: Sealed {
    fn from_sql(val: Value) -> Result<Self, FromSqlError>
    where
        Self: Sized;
}

impl FromValue for Value {
    fn from_sql(val: Value) -> Result<Self, FromSqlError> {
        Ok(val)
    }
}
impl Sealed for Value {}

macro_rules! impl_int_from_value {
    ($ty:ty, $cast:expr) => {
        impl FromValue for $ty {
            fn from_sql(val: Value) -> Result<Self, FromSqlError> {
                match val {
                    Value::Null => Err(FromSqlError::NullValue),
                    Value::Integer(i) => Ok($cast(i)),
                    _ => unreachable!("invalid value type"),
                }
            }
        }

        impl Sealed for $ty {}
    };
}

impl_int_from_value!(i32, |i| i as i32);
impl_int_from_value!(u32, |i| i as u32);
impl_int_from_value!(i64, |i| i);
impl_int_from_value!(u64, |i| i as u64);

impl FromValue for f64 {
    fn from_sql(val: Value) -> Result<Self, FromSqlError> {
        match val {
            Value::Null => Err(FromSqlError::NullValue),
            Value::Float(f) => Ok(f),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl Sealed for f64 {}

impl FromValue for Vec<u8> {
    fn from_sql(val: Value) -> Result<Self, FromSqlError> {
        match val {
            Value::Null => Err(FromSqlError::NullValue),
            Value::Blob(blob) => Ok(blob),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl Sealed for Vec<u8> {}

impl<const N: usize> FromValue for [u8; N] {
    fn from_sql(val: Value) -> Result<Self, FromSqlError> {
        match val {
            Value::Null => Err(FromSqlError::NullValue),
            Value::Blob(blob) => blob
                .try_into()
                .map_err(|_| FromSqlError::InvalidBlobSize(N)),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl<const N: usize> Sealed for [u8; N] {}

impl FromValue for String {
    fn from_sql(val: Value) -> Result<Self, FromSqlError> {
        match val {
            Value::Null => Err(FromSqlError::NullValue),
            Value::Text(s) => Ok(s.to_string()),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl Sealed for String {}

impl FromValue for bool {
    fn from_sql(val: Value) -> Result<Self, FromSqlError> {
        match val {
            Value::Null => Err(FromSqlError::NullValue),
            Value::Integer(i) => match i {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(FromSqlError::InvalidColumnType),
            },
            _ => unreachable!("invalid value type"),
        }
    }
}
impl Sealed for bool {}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_sql(val: Value) -> Result<Self, FromSqlError> {
        match val {
            Value::Null => Ok(None),
            _ => T::from_sql(val).map(Some),
        }
    }
}
impl<T> Sealed for Option<T> {}

mod sealed {
    pub trait Sealed {}
}
use sealed::Sealed;
