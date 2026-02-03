use branches::{mark_unlikely, unlikely};
use either::Either;
#[cfg(feature = "serde")]
use serde::Deserialize;
use turso_ext::{AggCtx, FinalizeFunction, StepFunction};
use turso_parser::ast::SortOrder;

use crate::error::LimboError;
use crate::ext::{ExtValue, ExtValueType};
#[allow(unused_imports)]
use crate::turso_soft_unreachable;
use crate::index_method::IndexMethodCursor;
use crate::numeric::format_float;
use crate::pseudo::PseudoCursor;
use crate::schema::Index;
use crate::storage::btree::CursorTrait;
use crate::storage::sqlite3_ondisk::{read_integer, read_value, read_varint, write_varint};
use crate::translate::collate::CollationSeq;
use crate::translate::plan::IterationDirection;
use crate::vdbe::sorter::Sorter;
use crate::vdbe::Register;
use crate::vtab::VirtualTableCursor;
use crate::{Completion, CompletionError, Result, IO};
use std::borrow::{Borrow, Cow};
use std::cell::Cell;
use std::fmt::{Debug, Display};
use std::iter::{FusedIterator, Peekable};
use std::ops::Deref;
use std::task::Waker;

/// SQLite by default uses 2000 as maximum numbers in a row.
/// It controlld by the constant called SQLITE_MAX_COLUMN
/// But the hard limit of number of columns is 32,767 columns i16::MAX
/// const MAX_COLUMN: usize = 2000;

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
        let other_str = other.as_ref();
        match &mut self.value {
            Cow::Owned(s) => {
                let needed = other_str.len();
                if s.capacity() >= needed {
                    // SAFETY: capacity >= needed, source is valid UTF-8
                    debug_assert!(
                        s.as_ptr().wrapping_add(s.len()) <= other_str.as_ptr()
                            || other_str.as_ptr().wrapping_add(other_str.len()) <= s.as_ptr(),
                        "source and destination ranges must not overlap"
                    );
                    unsafe {
                        std::ptr::copy_nonoverlapping(other_str.as_ptr(), s.as_mut_ptr(), needed);
                        s.as_mut_vec().set_len(needed);
                    }
                } else {
                    *s = other_str.to_owned();
                }
            }
            Cow::Borrowed(_) => {
                self.value = Cow::Owned(other_str.to_owned());
            }
        }
        self.subtype = other.subtype();
    }
}

impl<T: AnyBlob> Extendable<T> for Vec<u8> {
    #[inline(always)]
    fn do_extend(&mut self, other: &T) {
        let other_slice = other.as_slice();
        let needed = other_slice.len();
        if self.capacity() >= needed {
            // SAFETY: capacity >= needed
            debug_assert!(
                self.as_ptr().wrapping_add(self.len()) <= other_slice.as_ptr()
                    || other_slice.as_ptr().wrapping_add(other_slice.len()) <= self.as_ptr(),
                "source and destination ranges must not overlap"
            );
            unsafe {
                std::ptr::copy_nonoverlapping(other_slice.as_ptr(), self.as_mut_ptr(), needed);
                self.set_len(needed);
            }
        } else {
            self.clear();
            self.extend_from_slice(other_slice);
        }
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

#[derive(Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(TextRef<'a>),
    Blob(&'a [u8]),
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
    pub fn serialize_serial(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => {}
            Value::Integer(i) => {
                let serial_type = SerialType::from(self);
                match serial_type.kind() {
                    SerialTypeKind::I8 => out.extend_from_slice(&(*i as i8).to_be_bytes()),
                    SerialTypeKind::I16 => out.extend_from_slice(&(*i as i16).to_be_bytes()),
                    SerialTypeKind::I24 => out.extend_from_slice(&(*i as i32).to_be_bytes()[1..]), // remove most significant byte
                    SerialTypeKind::I32 => out.extend_from_slice(&(*i as i32).to_be_bytes()),
                    SerialTypeKind::I48 => out.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                    SerialTypeKind::I64 => out.extend_from_slice(&i.to_be_bytes()),
                    _ => unreachable!(),
                }
            }
            Value::Float(f) => out.extend_from_slice(&f.to_be_bytes()),
            Value::Text(t) => out.extend_from_slice(t.value.as_bytes()),
            Value::Blob(b) => out.extend_from_slice(b),
        };
    }

    /// Cast Value to String, if Value is NULL returns None
    pub fn cast_text(&self) -> Option<String> {
        Some(match self {
            Value::Null => return None,
            v => v.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalAggState {
    pub state: *mut AggCtx,
    pub argc: usize,
    pub step_fn: StepFunction,
    pub finalize_fn: FinalizeFunction,
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

impl Value {
    pub fn to_ffi(&self) -> ExtValue {
        match self {
            Self::Null => ExtValue::null(),
            Self::Integer(i) => ExtValue::from_integer(*i),
            Self::Float(fl) => ExtValue::from_float(*fl),
            Self::Text(text) => ExtValue::from_text(text.as_str().to_string()),
            Self::Blob(blob) => ExtValue::from_blob(blob.to_vec()),
        }
    }

    pub fn from_ffi(v: ExtValue) -> Result<Self> {
        let res = match v.value_type() {
            ExtValueType::Null => Ok(Value::Null),
            ExtValueType::Integer => {
                let Some(int) = v.to_integer() else {
                    return Ok(Value::Null);
                };
                Ok(Value::Integer(int))
            }
            ExtValueType::Float => {
                let Some(float) = v.to_float() else {
                    return Ok(Value::Null);
                };
                Ok(Value::Float(float))
            }
            ExtValueType::Text => {
                let Some(text) = v.to_text() else {
                    return Ok(Value::Null);
                };
                #[cfg(feature = "json")]
                if v.is_json() {
                    return Ok(Value::Text(Text::json(text.to_string())));
                }
                Ok(Value::build_text(text.to_string()))
            }
            ExtValueType::Blob => {
                let Some(blob) = v.to_blob() else {
                    return Ok(Value::Null);
                };
                Ok(Value::Blob(blob))
            }
            ExtValueType::Error => {
                let Some(err) = v.to_error_details() else {
                    return Ok(Value::Null);
                };
                match err {
                    (_, Some(msg)) => Err(LimboError::ExtensionError(msg)),
                    (code, None) => Err(LimboError::ExtensionError(code.to_string())),
                }
            }
        };
        unsafe { v.__free_internal_type() };
        res
    }
}

/// Convert a `Value` into the implementors type.
pub trait FromValue: Sealed {
    fn from_sql(val: Value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for Value {
    fn from_sql(val: Value) -> Result<Self> {
        Ok(val)
    }
}
impl Sealed for crate::Value {}

macro_rules! impl_int_from_value {
    ($ty:ty, $cast:expr) => {
        impl FromValue for $ty {
            fn from_sql(val: Value) -> Result<Self> {
                match val {
                    Value::Null => Err(LimboError::NullValue),
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
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Float(f) => Ok(f),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl Sealed for f64 {}

impl FromValue for Vec<u8> {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Blob(blob) => Ok(blob),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl Sealed for Vec<u8> {}

impl<const N: usize> FromValue for [u8; N] {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Blob(blob) => blob.try_into().map_err(|_| LimboError::InvalidBlobSize(N)),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl<const N: usize> Sealed for [u8; N] {}

impl FromValue for String {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Text(s) => Ok(s.to_string()),
            _ => unreachable!("invalid value type"),
        }
    }
}
impl Sealed for String {}

impl FromValue for bool {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Integer(i) => match i {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(LimboError::InvalidColumnType),
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
    fn from_sql(val: Value) -> Result<Self> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct SumAggState {
    pub r_err: f64,   // Error term for Kahan-Babushka-Neumaier summation
    pub approx: bool, // True if any non-integer value was input to the sum
    pub ovrfl: bool,  // Integer overflow seen
}
impl Default for SumAggState {
    fn default() -> Self {
        Self {
            r_err: 0.0,
            approx: false,
            ovrfl: false,
        }
    }
}

/// Aggregate context for accumulating values during GROUP BY.
/// Built-in aggregates use a flat payload representation for efficiency and
/// to share code between register-based and hash-based aggregation (future enhancement).
#[derive(Debug, Clone, PartialEq)]
pub enum AggContext {
    /// Built-in aggregates store state as a flat Vec<Value> payload.
    /// The layout depends on the aggregate function (see init_agg_payload).
    Builtin(Vec<Value>),
    /// External (extension) aggregates need FFI state that can't be serialized.
    External(ExternalAggState),
}

impl AggContext {
    pub fn compute_external(&self) -> Result<Value> {
        if let Self::External(ext_state) = self {
            let final_value = unsafe { (ext_state.finalize_fn)(ext_state.state) };
            Value::from_ffi(final_value)
        } else {
            panic!("AggContext::compute_external() expected External, found {self:?}");
        }
    }

    /// Get a mutable reference to the builtin payload
    pub fn payload_mut(&mut self) -> &mut [Value] {
        match self {
            Self::Builtin(payload) => payload,
            Self::External(_) => panic!("payload_mut() called on External aggregate"),
        }
    }

    /// Get an immutable reference to the builtin payload
    pub fn payload(&self) -> &[Value] {
        match self {
            Self::Builtin(payload) => payload,
            Self::External(_) => panic!("payload() called on External aggregate"),
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

impl PartialOrd<AggContext> for AggContext {
    fn partial_cmp(&self, other: &AggContext) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Builtin(a), Self::Builtin(b)) => {
                // Compare by first element (the accumulator) if present
                match (a.first(), b.first()) {
                    (Some(a), Some(b)) => a.partial_cmp(b),
                    _ => None,
                }
            }
            _ => None,
        }
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

impl TryFrom<ValueRef<'_>> for i64 {
    type Error = LimboError;

    fn try_from(value: ValueRef<'_>) -> Result<Self, Self::Error> {
        match value {
            ValueRef::Integer(i) => Ok(i),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl TryFrom<ValueRef<'_>> for String {
    type Error = LimboError;

    #[inline]
    fn try_from(value: ValueRef<'_>) -> Result<Self, Self::Error> {
        Ok(<&str>::try_from(value)?.to_string())
    }
}

impl<'a> TryFrom<ValueRef<'a>> for &'a str {
    type Error = LimboError;

    #[inline]
    fn try_from(value: ValueRef<'a>) -> Result<Self, Self::Error> {
        match value {
            ValueRef::Text(s) => Ok(s.as_str()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

/// This struct serves the purpose of not allocating multiple vectors of bytes if not needed.
/// A value in a record that has already been serialized can stay serialized and what this struct offsers
/// is easy acces to each value which point to the payload.
/// The name might be contradictory as it is immutable in the sense that you cannot modify the values without modifying the payload.
pub struct ImmutableRecord {
    // We have to be super careful with this buffer since we make values point to the payload we need to take care reallocations
    // happen in a controlled manner. If we realocate with values that should be correct, they will now point to undefined data.
    // We don't use pin here because it would make it imposible to reuse the buffer if we need to push a new record in the same struct.
    //
    // payload is the Vec<u8> but in order to use Register which holds ImmutableRecord as a Value - we store Vec<u8> as Value::Blob
    payload: Value,
}

// SAFETY: all ImmutableRecord instances are intended to be used in a single thread
// by a single connection.
unsafe impl Send for ImmutableRecord {}
unsafe impl Sync for ImmutableRecord {}

impl Clone for ImmutableRecord {
    fn clone(&self) -> Self {
        Self {
            payload: self.payload.clone(),
        }
    }
}

impl PartialEq for ImmutableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.payload == other.payload // Only compare payload, ignore cursor state
    }
}

impl Eq for ImmutableRecord {}

impl PartialOrd for ImmutableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImmutableRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.payload.cmp(&other.payload) // Only compare payload, ignore cursor state
    }
}

impl std::fmt::Debug for ImmutableRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.payload {
            Value::Blob(bytes) => {
                let preview = if bytes.len() > 20 {
                    format!("{:?} ... ({} bytes total)", &bytes[..20], bytes.len())
                } else {
                    format!("{bytes:?}")
                };
                write!(f, "ImmutableRecord {{ payload: {preview} }}")
            }
            Value::Text(s) => {
                let string = s.as_str();
                let preview = if string.len() > 20 {
                    format!("{:?} ... ({} chars total)", &string[..20], string.len())
                } else {
                    format!("{string:?}")
                };
                write!(f, "ImmutableRecord {{ payload: {preview} }}")
            }
            other => write!(f, "ImmutableRecord {{ payload: {other:?} }}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Record {
    values: Vec<Value>,
}

impl Record {
    // pub fn get<'a, T: FromValue<'a> + 'a>(&'a self, idx: usize) -> Result<T> {
    //     let value = &self.values[idx];
    //     T::from_value(value)
    // }

    pub fn count(&self) -> usize {
        self.values.len()
    }

    pub fn last_value(&self) -> Option<&Value> {
        self.values.last()
    }

    pub fn get_values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn get_value(&self, idx: usize) -> &Value {
        &self.values[idx]
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}
struct AppendWriter<'a> {
    buf: &'a mut Vec<u8>,
    pos: usize,
    buf_capacity_start: usize,
    buf_ptr_start: *const u8,
}

impl<'a> AppendWriter<'a> {
    pub fn new(buf: &'a mut Vec<u8>, pos: usize) -> Self {
        let buf_ptr_start = buf.as_ptr();
        let buf_capacity_start = buf.capacity();
        Self {
            buf,
            pos,
            buf_capacity_start,
            buf_ptr_start,
        }
    }

    #[inline]
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.buf[self.pos..self.pos + slice.len()].copy_from_slice(slice);
        self.pos += slice.len();
    }

    fn assert_finish_capacity(&self) {
        // let's make sure we didn't reallocate anywhere else
        assert_eq!(self.buf_capacity_start, self.buf.capacity());
        assert_eq!(self.buf_ptr_start, self.buf.as_ptr());
    }
}

impl ImmutableRecord {
    pub fn new(payload_capacity: usize) -> Self {
        Self {
            payload: Value::Blob(Vec::with_capacity(payload_capacity)),
        }
    }

    pub fn from_bin_record(payload: Vec<u8>) -> Self {
        Self {
            payload: Value::Blob(payload),
        }
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values(&self) -> Result<Vec<ValueRef<'_>>> {
        let iter = self.iter()?;
        let mut values = Vec::with_capacity(iter.size_hint().0);
        for value in iter {
            values.push(value?);
        }
        Ok(values)
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values_range(&self, range: std::ops::Range<usize>) -> Result<Vec<ValueRef<'_>>> {
        let mut iter = self.iter()?;
        let mut values = Vec::with_capacity(range.end - range.start);
        // advance to start
        if let Some(value) = iter.nth(range.start) {
            values.push(value?);
        } else {
            return Ok(values);
        }
        // collect rest
        for _ in range.start + 1..range.end {
            if let Some(value) = iter.next() {
                values.push(value?);
            } else {
                break;
            }
        }
        Ok(values)
    }

    // Idx values must be sorted ascending
    pub fn get_two_values(&self, idx1: usize, idx2: usize) -> Result<(ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = self.iter()?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1); // idx2 - idx1 - 1 because we already advanced to idx1
        match (val1, val2) {
            (Some(v1), Some(v2)) => Ok((v1?, v2?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    // Idx values must be sorted ascending
    pub fn get_three_values(
        &self,
        idx1: usize,
        idx2: usize,
        idx3: usize,
    ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = self.iter()?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1); // idx2 - idx1 - 1 because we already advanced to idx1
        let val3 = iter.nth(idx3 - idx2 - 1); // idx3 - idx2 - 1 because we already advanced to idx2
        match (val1, val2, val3) {
            (Some(v1), Some(v2), Some(v3)) => Ok((v1?, v2?, v3?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    // Idx values must be sorted ascending
    pub fn get_four_values(
        &self,
        idx1: usize,
        idx2: usize,
        idx3: usize,
        idx4: usize,
    ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = self.iter()?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1); // idx2 - idx1 - 1 because we already advanced to idx1
        let val3 = iter.nth(idx3 - idx2 - 1); // idx3 - idx2 - 1 because we already advanced to idx2
        let val4 = iter.nth(idx4 - idx3 - 1); // idx4 - idx3 - 1 because we already advanced to idx3
        match (val1, val2, val3, val4) {
            (Some(v1), Some(v2), Some(v3), Some(v4)) => Ok((v1?, v2?, v3?, v4?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values_owned(&self) -> Result<Vec<Value>> {
        let iter = self.iter().expect("Failed to create payload iterator");
        let mut values = Vec::with_capacity(iter.size_hint().0);
        for value in iter {
            values.push(value?.to_owned());
        }
        Ok(values)
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values_owned_range(&self, range: std::ops::Range<usize>) -> Result<Vec<Value>> {
        let mut iter = self.iter().expect("Failed to create payload iterator");
        let mut values = Vec::with_capacity(range.end - range.start);
        // advance to start
        if let Some(value) = iter.nth(range.start) {
            values.push(value?.to_owned());
        } else {
            return Ok(values);
        }
        // collect rest
        for _ in range.start + 1..range.end {
            if let Some(value) = iter.next() {
                values.push(value?.to_owned());
            } else {
                break;
            }
        }
        Ok(values)
    }

    pub fn from_registers<'a, I: Iterator<Item = &'a Register> + Clone>(
        // we need to accept both &[Register] and &[&Register] values - that's why non-trivial signature
        //
        // std::slice::Iter under the hood just stores pointer and length of slice and also implements a Clone which just copy those meta-values
        // (without copying the data itself)
        registers: impl IntoIterator<Item = &'a Register, IntoIter = I>,
        len: usize,
    ) -> Self {
        Self::from_values(registers.into_iter().map(|x| x.get_value()), len)
    }

    pub fn from_values<'a>(
        values: impl IntoIterator<Item = impl AsValueRef + 'a> + Clone,
        len: usize,
    ) -> Self {
        let mut serials = Vec::with_capacity(len);
        let mut size_header = 0;
        let mut size_values = 0;

        let mut serial_type_buf = [0; 9];
        // write serial types
        for value in values.clone() {
            let serial_type = SerialType::from(value.as_value_ref());
            let n = write_varint(&mut serial_type_buf[0..], serial_type.into());
            serials.push((serial_type_buf, n));

            let value_size = serial_type.size();

            size_header += n;
            size_values += value_size;
        }

        let header_size = Record::calc_header_size(size_header);

        // 1. write header size
        let mut buf = Vec::new();
        buf.reserve_exact(header_size + size_values);
        assert_eq!(buf.capacity(), header_size + size_values);
        let n = write_varint(&mut serial_type_buf, header_size as u64);

        buf.resize(buf.capacity(), 0);
        let mut writer = AppendWriter::new(&mut buf, 0);
        writer.extend_from_slice(&serial_type_buf[..n]);

        // 2. Write serial
        for (value, n) in serials {
            writer.extend_from_slice(&value[..n]);
        }

        // write content
        for value in values {
            let value = value.as_value_ref();
            match value {
                ValueRef::Null => {}
                ValueRef::Integer(i) => {
                    let serial_type = SerialType::from(value);
                    match serial_type.kind() {
                        SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 => {}
                        SerialTypeKind::I8 => writer.extend_from_slice(&(i as i8).to_be_bytes()),
                        SerialTypeKind::I16 => writer.extend_from_slice(&(i as i16).to_be_bytes()),
                        SerialTypeKind::I24 => {
                            writer.extend_from_slice(&(i as i32).to_be_bytes()[1..])
                        } // remove most significant byte
                        SerialTypeKind::I32 => writer.extend_from_slice(&(i as i32).to_be_bytes()),
                        SerialTypeKind::I48 => writer.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialTypeKind::I64 => writer.extend_from_slice(&i.to_be_bytes()),
                        other => panic!("Serial type is not an integer: {other:?}"),
                    }
                }
                ValueRef::Float(f) => writer.extend_from_slice(&f.to_be_bytes()),
                ValueRef::Text(t) => {
                    writer.extend_from_slice(t.value.as_bytes());
                }
                ValueRef::Blob(b) => {
                    writer.extend_from_slice(b);
                }
            };
        }

        writer.assert_finish_capacity();
        Self {
            payload: Value::Blob(buf),
        }
    }

    #[inline]
    pub fn into_payload(self) -> Vec<u8> {
        match self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    #[inline]
    pub fn as_blob(&self) -> &Vec<u8> {
        match &self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    #[inline]
    pub fn as_blob_mut(&mut self) -> &mut Vec<u8> {
        match &mut self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    #[inline]
    pub fn as_blob_value(&self) -> &Value {
        &self.payload
    }

    #[inline]
    pub fn start_serialization(&mut self, payload: &[u8]) {
        self.as_blob_mut().extend_from_slice(payload);
    }

    #[inline]
    pub fn invalidate(&mut self) {
        self.as_blob_mut().clear();
    }

    #[inline]
    pub fn is_invalidated(&self) -> bool {
        self.as_blob().is_empty()
    }

    #[inline]
    pub fn get_payload(&self) -> &[u8] {
        self.as_blob()
    }

    #[inline(always)]
    pub fn iter(&self) -> Result<ValueIterator<'_>, LimboError> {
        ValueIterator::new(self.get_payload())
    }

    #[inline]
    /// Returns true if the record contains any NULL values.
    /// This is an optimization that only examines the header (serial types)
    /// without deserializing the data section.
    pub fn contains_null(&self) -> Result<bool> {
        let payload = self.get_payload();
        let (header_size, header_varint_len) = read_varint(payload)?;
        let header_size = header_size as usize;

        if header_size > payload.len() || header_varint_len > payload.len() {
            return Err(LimboError::Corrupt(
                "Payload too small for indicated header size".into(),
            ));
        }

        let mut header = &payload[header_varint_len..header_size];

        while !header.is_empty() {
            let (serial_type, bytes_read) = read_varint(header)?;
            if serial_type == 0 {
                return Ok(true);
            }
            header = &header[bytes_read..];
        }

        Ok(false)
    }

    #[inline]
    pub fn last_value(&self) -> Option<Result<ValueRef<'_>>> {
        if unlikely(self.is_invalidated()) {
            return Some(Err(LimboError::InternalError(
                "Record is invalidated".into(),
            )));
        }
        let iter = match self.iter() {
            Ok(it) => it,
            Err(e) => return Some(Err(e)),
        };
        iter.last()
    }

    #[inline]
    pub fn first_value(&self) -> Result<ValueRef<'_>> {
        if unlikely(self.is_invalidated()) {
            turso_soft_unreachable!("Record is invalidated");
            return Err(LimboError::InternalError("Record is invalidated".into()));
        }
        match self.iter()?.next() {
            Some(v) => v,
            None => Err(LimboError::InternalError("Record has no columns".into())),
        }
    }

    #[inline]
    pub fn get_value(&self, idx: usize) -> Result<ValueRef<'_>> {
        if unlikely(self.is_invalidated()) {
            turso_soft_unreachable!("Record is invalidated");
            return Err(LimboError::InternalError("Record is invalidated".into()));
        }
        let mut iter = self.iter()?;
        iter.nth(idx)
            .transpose()?
            .ok_or_else(|| LimboError::InternalError("Index out of bounds".into()))
    }

    #[inline]
    pub fn get_value_opt(&self, idx: usize) -> Option<ValueRef<'_>> {
        let mut iter = match self.iter() {
            Ok(it) => it,
            Err(_) => {
                mark_unlikely();
                return None;
            }
        };
        match iter.nth(idx) {
            Some(Ok(v)) => Some(v),
            _ => {
                mark_unlikely();
                None
            }
        }
    }

    pub fn column_count(&self) -> usize {
        self.iter().map(|it| it.count()).unwrap_or_default()
    }
}

/// A zero-allocation iterator over SQLite record payload data.
///
/// This iterator provides efficient, lazy parsing of SQLite records without
/// any heap allocation. It processes record data on-the-fly, returning `ValueRef`
/// instances that borrow directly from the underlying payload.
///
/// # Memory Layout
///
/// SQLite records follow this binary format:
/// ```text
/// [header_size: varint][serial_type1: varint][serial_type2: varint]...
/// [data1][data2][data3]...
/// ```
///
/// - **header_size**: Total bytes in the header section (including this varint)
/// - **serial_typeN**: Encodes the type and size of column N's data
/// - **dataN**: The actual data for column N (length determined by serial_typeN)
pub struct ValueIterator<'a> {
    /// Reference to header section up to data offset
    header_section: Cell<&'a [u8]>,
    /// Reference to data section only
    data_section: Cell<&'a [u8]>,
}

impl<'a> ValueIterator<'a> {
    /// Creates a new payload iterator from a raw payload slice.
    ///
    /// # Arguments
    ///
    /// * `payload` - The serialized SQLite record payload
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` if the header can be parsed, or an error if the
    /// payload is malformed.
    #[inline(always)]
    pub fn new(payload: &'a [u8]) -> Result<Self> {
        let (header_size, header_varint_len) = read_varint(payload)?;
        let header_size = header_size as usize;

        if header_size > payload.len() || header_varint_len > payload.len() {
            return Err(LimboError::Corrupt(
                "Payload too small for indicated header size".into(),
            ));
        }

        Ok(Self {
            header_section: Cell::new(&payload[header_varint_len..header_size]),
            data_section: Cell::new(&payload[header_size..]),
        })
    }

    /// Returns `true` if the payload is empty or the record has no columns.
    pub fn is_empty(&self) -> bool {
        self.header_section.get().is_empty()
    }

    /// Returns a reference to the current header section.
    #[inline(always)]
    pub fn header_section_ref(&self) -> &'a [u8] {
        self.header_section.get()
    }

    /// Returns a reference to the current data section.
    #[inline(always)]
    pub fn data_section_ref(&self) -> &'a [u8] {
        self.data_section.get()
    }

    /// Sets the header section to a new slice.
    #[inline(always)]
    pub fn set_header_section(&self, header: &'a [u8]) {
        self.header_section.set(header);
    }

    /// Sets the data section to a new slice.
    #[inline(always)]
    pub fn set_data_section(&self, data: &'a [u8]) {
        self.data_section.set(data);
    }
}

impl<'a> Iterator for ValueIterator<'a> {
    type Item = Result<ValueRef<'a>, LimboError>;

    #[inline(always)]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        let mut count = 0;
        let mut header = self.header_section.get();
        while !header.is_empty() {
            match read_varint(header) {
                Ok((_, bytes_read)) => {
                    count += 1;
                    header = &header[bytes_read..];
                }
                Err(_) => break,
            }
        }
        count
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut count = 0;
        let mut header = self.header_section.get();
        while !header.is_empty() {
            match read_varint(header) {
                Ok((_, bytes_read)) => {
                    count += 1;
                    header = &header[bytes_read..];
                }
                Err(_) => break,
            }
        }
        (count, Some(count))
    }

    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        for item in self {
            acc = f(acc, item);
        }
        acc
    }

    /// Returns the nth element of the iterator.
    #[inline(always)]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut header = self.header_section.get();
        let mut data = self.data_section.get();

        let mut data_sum = 0;
        for _ in 0..n {
            if unlikely(header.is_empty()) {
                return None;
            }

            let (serial_type, bytes_read) = match read_varint(header) {
                Ok(v) => v,
                Err(e) => {
                    mark_unlikely();
                    return Some(Err(e));
                }
            };
            header = &header[bytes_read..];

            data_sum += match get_serial_type_size(serial_type) {
                Ok(size) => size,
                Err(e) => {
                    mark_unlikely();
                    return Some(Err(e));
                }
            };
        }

        if unlikely(data_sum > data.len()) {
            return Some(Err(LimboError::Corrupt(
                "Data section too small for indicated serial type size".into(),
            )));
        }
        data = &data[data_sum..];

        // Update iterator state
        self.header_section.set(header);
        self.data_section.set(data);

        // Return the nth value
        self.next()
    }

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let header = self.header_section.get();
        if unlikely(header.is_empty()) {
            return None;
        }

        // Read next serial type
        let (serial_type, bytes_read) = match read_varint(header) {
            Ok(v) => v,
            Err(e) => {
                mark_unlikely();
                return Some(Err(e));
            }
        };

        // Update header section to remove the consumed serial type
        self.header_section.set(&header[bytes_read..]);

        let data_section = self.data_section.get();

        match crate::storage::sqlite3_ondisk::read_value_serial_type(data_section, serial_type) {
            Ok((value, n)) => {
                self.data_section.set(&data_section[n..]);
                Some(Ok(value))
            }
            Err(e) => {
                mark_unlikely();
                Some(Err(e))
            }
        }
    }
}

// Optimization: indicate that once the iterator is exhausted, it will always return None.
impl<'a> FusedIterator for ValueIterator<'a> {}

impl<'a> Clone for ValueIterator<'a> {
    fn clone(&self) -> Self {
        Self {
            header_section: Cell::new(self.header_section.get()),
            data_section: Cell::new(self.data_section.get()),
        }
    }
}

impl<'a> ValueRef<'a> {
    pub fn to_ffi(&self) -> ExtValue {
        match self {
            Self::Null => ExtValue::null(),
            Self::Integer(i) => ExtValue::from_integer(*i),
            Self::Float(fl) => ExtValue::from_float(*fl),
            Self::Text(text) => ExtValue::from_text(text.as_str().to_string()),
            Self::Blob(blob) => ExtValue::from_blob(blob.to_vec()),
        }
    }

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
                Some(sqlite_int_float_compare(*int_left, *float_right))
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                Some(sqlite_int_float_compare(*int_right, *float_left).reverse())
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

fn sqlite_int_float_compare(int_val: i64, float_val: f64) -> std::cmp::Ordering {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyInfo {
    pub sort_order: SortOrder,
    pub collation: CollationSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Metadata about an index, used for handling and comparing index keys.
///
/// This struct provides information about the sorting order of columns,
/// whether the index includes a row ID, and the total number of columns
/// in the index.
pub struct IndexInfo {
    /// Specifies the sorting order (ascending or descending) for each column in the index.
    pub key_info: Vec<KeyInfo>,
    /// Indicates whether the index includes a row ID column.
    pub has_rowid: bool,
    /// The total number of columns in the index, including the row ID column if present.
    pub num_cols: usize,
}

impl Default for IndexInfo {
    fn default() -> Self {
        Self {
            key_info: vec![],
            has_rowid: true,
            num_cols: 1,
        }
    }
}

impl IndexInfo {
    pub fn new_from_index(index: &Index) -> Self {
        Self {
            key_info: {
                let mut key_info: Vec<KeyInfo> = index
                    .columns
                    .iter()
                    .map(|c| KeyInfo {
                        sort_order: c.order,
                        collation: c.collation.unwrap_or_default(),
                    })
                    .collect();
                if index.has_rowid {
                    key_info.push(KeyInfo {
                        sort_order: SortOrder::Asc,
                        collation: CollationSeq::Binary,
                    });
                }
                key_info
            },
            has_rowid: index.has_rowid,
            num_cols: index.columns.len() + (index.has_rowid as usize),
        }
    }
}

pub fn compare_immutable<V1, V2, E1, E2, I1, I2>(
    l: I1,
    r: I2,
    column_info: &[KeyInfo],
) -> std::cmp::Ordering
where
    V1: AsValueRef,
    V2: AsValueRef,
    E1: ExactSizeIterator<Item = V1>,
    E2: ExactSizeIterator<Item = V2>,
    I1: IntoIterator<IntoIter = E1, Item = E1::Item>,
    I2: IntoIterator<IntoIter = E2, Item = E2::Item>,
{
    let (l, r): (E1, E2) = (l.into_iter(), r.into_iter());
    assert!(
        l.len() >= column_info.len(),
        "{} < {}",
        l.len(),
        column_info.len()
    );
    assert!(
        r.len() >= column_info.len(),
        "{} < {}",
        r.len(),
        column_info.len()
    );
    let (l, r) = (l.take(column_info.len()), r.take(column_info.len()));
    for (i, (l, r)) in l.zip(r).enumerate() {
        let column_order = column_info[i].sort_order;
        let collation = column_info[i].collation;
        let cmp = compare_immutable_single(l, r, collation);
        if !cmp.is_eq() {
            return match column_order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            };
        }
    }
    std::cmp::Ordering::Equal
}

pub fn compare_immutable_iter<V, E1, E2>(
    mut l: E1,
    mut r: E2,
    column_info: &[KeyInfo],
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    E1: Iterator<Item = Result<V>>,
    E2: Iterator<Item = Result<V>>,
{
    for col_info in column_info.iter() {
        let l = match l.next() {
            Some(v) => v,
            None => break,
        };
        let r = match r.next() {
            Some(v) => v,
            None => break,
        };
        let column_order = col_info.sort_order;
        let collation = col_info.collation;
        let cmp = compare_immutable_single(l?, r?, collation);
        if !cmp.is_eq() {
            return match column_order {
                SortOrder::Asc => Ok(cmp),
                SortOrder::Desc => Ok(cmp.reverse()),
            };
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

pub fn compare_immutable_single<V1, V2>(l: V1, r: V2, collation: CollationSeq) -> std::cmp::Ordering
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    let l = l.as_value_ref();
    let r = r.as_value_ref();
    match (l, r) {
        (ValueRef::Text(left), ValueRef::Text(right)) => collation.compare_strings(&left, &right),
        _ => l.partial_cmp(&r).unwrap(),
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RecordCompare {
    Int,
    String,
    Generic,
}

impl RecordCompare {
    pub fn compare<V, E, I>(
        &self,
        serialized: &ImmutableRecord,
        unpacked: I,
        index_info: &IndexInfo,
        skip: usize,
        tie_breaker: std::cmp::Ordering,
    ) -> Result<std::cmp::Ordering>
    where
        V: AsValueRef,
        E: ExactSizeIterator<Item = V>,
        I: IntoIterator<IntoIter = E, Item = E::Item>,
    {
        let unpacked = unpacked.into_iter();
        match self {
            RecordCompare::Int => {
                compare_records_int(serialized, unpacked, index_info, tie_breaker)
            }
            RecordCompare::String => {
                compare_records_string(serialized, unpacked, index_info, tie_breaker)
            }
            RecordCompare::Generic => {
                compare_records_generic(serialized, unpacked, index_info, skip, tie_breaker)
            }
        }
    }
}

pub fn find_compare<I, E, V>(unpacked: I, index_info: &IndexInfo) -> RecordCompare
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = Peekable<E>, Item = V>,
{
    let mut unpacked = unpacked.into_iter();
    if unpacked.len() != 0 && index_info.num_cols <= 13 {
        let val = unpacked.peek().unwrap();
        match val.as_value_ref() {
            ValueRef::Integer(_) => RecordCompare::Int,
            ValueRef::Text(_) if index_info.key_info[0].collation == CollationSeq::Binary => {
                RecordCompare::String
            }
            _ => RecordCompare::Generic,
        }
    } else {
        RecordCompare::Generic
    }
}

pub fn get_tie_breaker_from_seek_op(seek_op: SeekOp) -> std::cmp::Ordering {
    match seek_op {
        // exact‐match “key == X” opcodes
        SeekOp::GE { eq_only: true } | SeekOp::LE { eq_only: true } => std::cmp::Ordering::Equal,

        // forward search – want the *first* ≥ / > key
        SeekOp::GE { eq_only: false } => std::cmp::Ordering::Greater,
        SeekOp::GT => std::cmp::Ordering::Less,

        // backward search – want the *last* ≤ / < key
        SeekOp::LE { eq_only: false } => std::cmp::Ordering::Less,
        SeekOp::LT => std::cmp::Ordering::Greater,
    }
}

/// Optimized integer-first record comparison function.
///
/// This function is an optimized version of `compare_records_generic()` for the
/// common case where:
/// - (a) The first field of the unpacked record is an integer
/// - (b) The serialized record's first field is also an integer
/// - (c) The header size varint fits in a single byte and is ≤ 63 bytes
///
/// The 63-byte header limit prevents buffer overreads and ensures safe direct
/// memory access patterns. This optimization avoids generic parsing overhead
/// by directly extracting and comparing integer values using known layouts.
///
/// # Fast Path Conditions
///
/// The function uses the optimized path when ALL of these conditions are met:
/// - Payload is at least 2 bytes (header size + first serial type)
/// - First serial type indicates integer (`1-6`, `8`, or `9`)
/// - First unpacked field is a `RefValue::Integer`
///
/// If any condition fails, it falls back to `compare_records_generic()`.
///
/// # Arguments
///
/// * `serialized` - The left-hand side record in serialized format
/// * `unpacked` - The right-hand side record as an array of parsed values
/// * `index_info` - Contains sort order information for each field
/// * `collations` - Array of collation sequences (unused for integers)
/// * `tie_breaker` - Result to return when all compared fields are equal
///
/// /// # Comparison Logic
///
/// The function follows optimized integer comparison semantics:
///
/// 1. **Type validation**: Ensures both sides are integers, otherwise falls back
/// 2. **Direct extraction**: Reads integer value using specialized decoder
/// 3. **Native comparison**: Uses Rust's built-in `i64::cmp()` for speed
/// 4. **Sort order**: Applies ascending/descending order to comparison result
/// 5. **Remaining fields**: If first field is equal and more fields exist,
///    delegates to `compare_records_generic()` with `skip=1`
fn compare_records_int<V, I>(
    serialized: &ImmutableRecord,
    unpacked: I,
    index_info: &IndexInfo,
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    I: ExactSizeIterator<Item = V>,
{
    let payload = serialized.get_payload();
    if payload.len() < 2 {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let (header_size, offset_1st_serialtype) = read_varint(payload)?;
    let header_size = header_size as usize;

    if payload.len() < header_size {
        return Err(LimboError::Corrupt(format!(
            "Record payload too short: claimed header size {} but payload only {} bytes",
            header_size,
            payload.len()
        )));
    }

    let (first_serial_type, _) = read_varint(&payload[offset_1st_serialtype..])?;

    let serialtype_is_integer = matches!(first_serial_type, 1..=6 | 8 | 9);
    if !serialtype_is_integer {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let data_start = header_size;

    let lhs_int = read_integer(&payload[data_start..], first_serial_type as u8)?;
    let mut unpacked = unpacked.peekable();
    // Do not consume iterator here
    let ValueRef::Integer(rhs_int) = unpacked.peek().unwrap().as_value_ref() else {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    };
    let comparison = match index_info.key_info[0].sort_order {
        SortOrder::Asc => lhs_int.cmp(&rhs_int),
        SortOrder::Desc => lhs_int.cmp(&rhs_int).reverse(),
    };
    match comparison {
        std::cmp::Ordering::Equal => {
            // First fields equal, compare remaining fields if any
            if unpacked.len() > 1 {
                return compare_records_generic(serialized, unpacked, index_info, 1, tie_breaker);
            }
            Ok(tie_breaker)
        }
        other => Ok(other),
    }
}

/// This function is an optimized version of `compare_records_generic()` for the
/// common case where:
/// - (a) The first field of the unpacked record is a string
/// - (b) The serialized record's first field is also a string
/// - (c) The header size varint fits in a single byte (most records)
///
/// This optimization avoids the overhead of generic field parsing by directly
/// accessing the first string field using known offsets, then falling back to
/// the generic comparison for remaining fields if needed.
///
/// # Fast Path Conditions
///
/// The function uses the optimized path when ALL of these conditions are met:
/// - Payload is at least 2 bytes (header size + first serial type)
/// - Header size fits in single byte (`payload[0] < 0x80`)
/// - First serial type indicates string (`>= 13` and odd number)
/// - First unpacked field is a `RefValue::Text`
///
/// If any condition fails, it falls back to `compare_records_generic()`.
///
/// # Arguments
///
/// * `serialized` - The left-hand side record in serialized format
/// * `unpacked` - The right-hand side record as an array of parsed values
/// * `index_info` - Contains sort order information for each field
/// * `collations` - Array of collation sequences for string comparisons
/// * `tie_breaker` - Result to return when all compared fields are equal
///
/// # Comparison Logic
///
/// The function follows SQLite's string comparison semantics:
///
/// 1. **Type checking**: Ensures both sides are strings, otherwise falls back
/// 2. **String comparison**: Uses collation if provided, binary otherwise
/// 3. **Sort order**: Applies ascending/descending order to comparison result
/// 4. **Length comparison**: If strings are equal, compares lengths
/// 5. **Remaining fields**: If first field is equal and more fields exist,
///    delegates to `compare_records_generic()` with `skip=1`
fn compare_records_string<V, I>(
    serialized: &ImmutableRecord,
    unpacked: I,
    index_info: &IndexInfo,
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    I: ExactSizeIterator<Item = V>,
{
    let payload = serialized.get_payload();
    if payload.len() < 2 {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let (header_size, offset_1st_serialtype) = read_varint(payload)?;
    let header_size = header_size as usize;

    if payload.len() < header_size {
        return Err(LimboError::Corrupt(format!(
            "Record payload too short: claimed header size {} but payload only {} bytes",
            header_size,
            payload.len()
        )));
    }

    let (first_serial_type, _) = read_varint(&payload[offset_1st_serialtype..])?;

    let serialtype_is_string = first_serial_type >= 13 && (first_serial_type & 1) == 1;
    if !serialtype_is_string {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let mut unpacked = unpacked.peekable();

    let ValueRef::Text(rhs_text) = unpacked.peek().unwrap().as_value_ref() else {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    };

    let string_len = (first_serial_type as usize - 13) / 2;
    let data_start = header_size;

    debug_assert!(data_start + string_len <= payload.len());

    let serial_type = SerialType::try_from(first_serial_type)?;
    let (lhs_value, _) = read_value(&payload[data_start..], serial_type)?;

    let ValueRef::Text(lhs_text) = lhs_value else {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    };

    let collation = index_info.key_info[0].collation;
    let comparison = collation.compare_strings(&lhs_text, &rhs_text);

    let final_comparison = match index_info.key_info[0].sort_order {
        SortOrder::Asc => comparison,
        SortOrder::Desc => comparison.reverse(),
    };

    match final_comparison {
        std::cmp::Ordering::Equal => {
            let len_cmp = lhs_text.len().cmp(&rhs_text.len());
            if len_cmp != std::cmp::Ordering::Equal {
                let adjusted = match index_info.key_info[0].sort_order {
                    SortOrder::Asc => len_cmp,
                    SortOrder::Desc => len_cmp.reverse(),
                };
                return Ok(adjusted);
            }

            if unpacked.len() > 1 {
                return compare_records_generic(serialized, unpacked, index_info, 1, tie_breaker);
            }
            Ok(tie_breaker)
        }
        other => Ok(other),
    }
}

/// Compare two table rows or index records.
///
/// This function compares a serialized record (`serialized`) with an unpacked
/// record (`unpacked`) and returns a comparison result. It returns `Less`, `Equal`,
/// or `Greater` if the serialized record is less than, equal to, or greater than
/// the unpacked record.
///
/// The `serialized` record must be a blob created by the record serialization
/// process (equivalent to SQLite's OP_MakeRecord opcode). The `unpacked` record
/// must be a parsed key array of `RefValue` objects.
///
/// # Arguments
///
/// * `serialized` - The left-hand side record in serialized format
/// * `unpacked` - The right-hand side record as an array of parsed values
/// * `index_info` - Contains sort order information for each field
/// * `collations` - Array of collation sequences for string comparisons
/// * `skip` - Number of initial fields to skip (assumes caller verified equality)
/// * `tie_breaker` - Result to return when all compared fields are equal
///
/// # Skipping Fields
///
/// If `skip` is non-zero, it is assumed that the caller has already determined
/// that the first `skip` fields of the records are equal. This function will
/// begin comparing at field index `skip`, skipping over the header and data
/// portions of the already-verified fields.
///
/// # Field Count Differences
///
/// The serialized and unpacked records do not have to contain the same number
/// of fields. If all fields that appear in both records are equal, then
/// `tie_breaker` is returned.
pub fn compare_records_generic<V, I>(
    serialized: &ImmutableRecord,
    unpacked: I,
    index_info: &IndexInfo,
    skip: usize,
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    I: ExactSizeIterator<Item = V>,
{
    let payload = serialized.get_payload();
    if payload.is_empty() {
        return Ok(std::cmp::Ordering::Less);
    }

    let (header_size, mut header_pos) = read_varint(payload)?;
    let header_end = header_size as usize;
    debug_assert!(header_end <= payload.len());

    let mut data_pos = header_size as usize;

    // Skip over `skip` number of fields
    for _ in 0..skip {
        if header_pos >= header_end {
            break;
        }

        let (serial_type_raw, bytes_read) = read_varint(&payload[header_pos..])?;
        header_pos += bytes_read;

        let serial_type = SerialType::try_from(serial_type_raw)?;
        if !matches!(
            serial_type.kind(),
            SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 | SerialTypeKind::Null
        ) {
            data_pos += serial_type.size();
        }
    }

    let mut field_idx = skip;
    let field_limit = unpacked.len().min(index_info.key_info.len());

    // assumes that that the `unpacked' iterator was not skipped outside this function call`
    for rhs_value in unpacked.skip(skip) {
        let rhs_value = &rhs_value.as_value_ref();
        if field_idx >= field_limit || header_pos >= header_end {
            break;
        }
        let (serial_type_raw, bytes_read) = read_varint(&payload[header_pos..])?;
        header_pos += bytes_read;

        let serial_type = SerialType::try_from(serial_type_raw)?;

        let lhs_value = match serial_type.kind() {
            SerialTypeKind::ConstInt0 => ValueRef::Integer(0),
            SerialTypeKind::ConstInt1 => ValueRef::Integer(1),
            SerialTypeKind::Null => ValueRef::Null,
            _ => {
                let (value, field_size) = read_value(&payload[data_pos..], serial_type)?;
                data_pos += field_size;
                value
            }
        };

        let comparison = match (&lhs_value, rhs_value) {
            (ValueRef::Text(lhs_text), ValueRef::Text(rhs_text)) => index_info.key_info[field_idx]
                .collation
                .compare_strings(lhs_text, rhs_text),

            (ValueRef::Integer(lhs_int), ValueRef::Float(rhs_float)) => {
                sqlite_int_float_compare(*lhs_int, *rhs_float)
            }

            (ValueRef::Float(lhs_float), ValueRef::Integer(rhs_int)) => {
                sqlite_int_float_compare(*rhs_int, *lhs_float).reverse()
            }

            _ => lhs_value.partial_cmp(rhs_value).unwrap(),
        };

        let final_comparison = match index_info.key_info[field_idx].sort_order {
            SortOrder::Asc => comparison,
            SortOrder::Desc => comparison.reverse(),
        };

        if final_comparison != std::cmp::Ordering::Equal {
            return Ok(final_comparison);
        }

        field_idx += 1;
    }

    Ok(tie_breaker)
}

const I8_LOW: i64 = -128;
const I8_HIGH: i64 = 127;
const I16_LOW: i64 = -32768;
const I16_HIGH: i64 = 32767;
const I24_LOW: i64 = -8388608;
const I24_HIGH: i64 = 8388607;
const I32_LOW: i64 = -2147483648;
const I32_HIGH: i64 = 2147483647;
const I48_LOW: i64 = -140737488355328;
const I48_HIGH: i64 = 140737488355327;

/// Sqlite Serial Types
/// https://www.sqlite.org/fileformat.html#record_format
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SerialType(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerialTypeKind {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    ConstInt0,
    ConstInt1,
    Text,
    Blob,
}

impl SerialType {
    #[inline(always)]
    pub fn u64_is_valid_serial_type(n: u64) -> bool {
        n != 10 && n != 11
    }

    const NULL: Self = Self(0);
    const I8: Self = Self(1);
    const I16: Self = Self(2);
    const I24: Self = Self(3);
    const I32: Self = Self(4);
    const I48: Self = Self(5);
    const I64: Self = Self(6);
    const F64: Self = Self(7);
    const CONST_INT0: Self = Self(8);
    const CONST_INT1: Self = Self(9);

    pub fn null() -> Self {
        Self::NULL
    }

    pub fn i8() -> Self {
        Self::I8
    }

    pub fn i16() -> Self {
        Self::I16
    }

    pub fn i24() -> Self {
        Self::I24
    }

    pub fn i32() -> Self {
        Self::I32
    }

    pub fn i48() -> Self {
        Self::I48
    }

    pub fn i64() -> Self {
        Self::I64
    }

    pub fn f64() -> Self {
        Self::F64
    }

    pub fn const_int0() -> Self {
        Self::CONST_INT0
    }

    pub fn const_int1() -> Self {
        Self::CONST_INT1
    }

    pub fn blob(size: u64) -> Self {
        Self(12 + size * 2)
    }

    pub fn text(size: u64) -> Self {
        Self(13 + size * 2)
    }

    #[inline(always)]
    pub fn kind(&self) -> SerialTypeKind {
        match self.0 {
            0 => SerialTypeKind::Null,
            1 => SerialTypeKind::I8,
            2 => SerialTypeKind::I16,
            3 => SerialTypeKind::I24,
            4 => SerialTypeKind::I32,
            5 => SerialTypeKind::I48,
            6 => SerialTypeKind::I64,
            7 => SerialTypeKind::F64,
            8 => SerialTypeKind::ConstInt0,
            9 => SerialTypeKind::ConstInt1,
            n if n >= 12 => match n % 2 {
                0 => SerialTypeKind::Blob,
                1 => SerialTypeKind::Text,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn size(&self) -> usize {
        match self.kind() {
            SerialTypeKind::Null => 0,
            SerialTypeKind::I8 => 1,
            SerialTypeKind::I16 => 2,
            SerialTypeKind::I24 => 3,
            SerialTypeKind::I32 => 4,
            SerialTypeKind::I48 => 6,
            SerialTypeKind::I64 => 8,
            SerialTypeKind::F64 => 8,
            SerialTypeKind::ConstInt0 => 0,
            SerialTypeKind::ConstInt1 => 0,
            SerialTypeKind::Text => (self.0 as usize - 13) / 2,
            SerialTypeKind::Blob => (self.0 as usize - 12) / 2,
        }
    }
}

#[inline(always)]
pub fn get_serial_type_size(serial: u64) -> Result<usize> {
    match serial {
        0 | 8 | 9 => Ok(0),
        1 => Ok(1),
        2 => Ok(2),
        3 => Ok(3),
        4 => Ok(4),
        5 => Ok(6),
        6 | 7 => Ok(8),
        n if n >= 12 => match n % 2 {
            0 => Ok(((n - 12) / 2) as usize), // Blob
            1 => Ok(((n - 13) / 2) as usize), // Text
            _ => unreachable!(),
        },
        _ => Err(LimboError::Corrupt(format!(
            "Invalid serial type: {serial}"
        ))),
    }
}

impl<T: AsValueRef> From<T> for SerialType {
    fn from(value: T) -> Self {
        let value = value.as_value_ref();
        match value {
            ValueRef::Null => SerialType::null(),
            ValueRef::Integer(i) => match i {
                0 => SerialType::const_int0(),
                1 => SerialType::const_int1(),
                i if (I8_LOW..=I8_HIGH).contains(&i) => SerialType::i8(),
                i if (I16_LOW..=I16_HIGH).contains(&i) => SerialType::i16(),
                i if (I24_LOW..=I24_HIGH).contains(&i) => SerialType::i24(),
                i if (I32_LOW..=I32_HIGH).contains(&i) => SerialType::i32(),
                i if (I48_LOW..=I48_HIGH).contains(&i) => SerialType::i48(),
                _ => SerialType::i64(),
            },
            ValueRef::Float(_) => SerialType::f64(),
            ValueRef::Text(t) => SerialType::text(t.value.len() as u64),
            ValueRef::Blob(b) => SerialType::blob(b.len() as u64),
        }
    }
}

impl From<SerialType> for u64 {
    fn from(serial_type: SerialType) -> Self {
        serial_type.0
    }
}

impl TryFrom<u64> for SerialType {
    type Error = LimboError;

    #[inline(always)]
    fn try_from(uint: u64) -> Result<Self> {
        if unlikely(uint == 10 || uint == 11) {
            return Err(LimboError::Corrupt(format!("Invalid serial type: {uint}")));
        }
        Ok(SerialType(uint))
    }
}

impl Record {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Calculates the total size needed for a SQLite record header.
    ///
    /// The record header consists of:
    /// 1. A varint encoding the total header size (self-referentially, e.g. a 100 byte header literally has the number '100' in the header suffix)
    /// 2. A sequence of varints encoding the serial types
    ///
    /// For small headers (<=126 bytes), we only need 1 byte to encode the header size, because 127 fits in 7 bits (varint uses 7 bits for the value and 1 continuation bit)
    /// For larger headers, we need to account for the variable length of the header size varint.
    pub fn calc_header_size(sizeof_serial_types: usize) -> usize {
        if sizeof_serial_types < i8::MAX as usize {
            return sizeof_serial_types + 1;
        }

        let mut header_size = sizeof_serial_types;
        // For larger headers, calculate how many bytes we need for the header size varint
        let mut temp_buf = [0u8; 9];
        let mut prev_header_size;

        loop {
            prev_header_size = header_size;
            let varint_len = write_varint(&mut temp_buf, header_size as u64);
            header_size = sizeof_serial_types + varint_len;

            if header_size == prev_header_size {
                break;
            }
        }

        header_size
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let initial_i = buf.len();

        // write serial types
        for value in &self.values {
            let serial_type = SerialType::from(value);
            buf.resize(buf.len() + 9, 0); // Ensure space for varint (1-9 bytes in length)
            let len = buf.len();
            let n = write_varint(&mut buf[len - 9..], serial_type.into());
            buf.truncate(buf.len() - 9 + n); // Remove unused bytes
        }

        let mut header_size = buf.len() - initial_i;
        // write content
        for value in &self.values {
            match value {
                Value::Null => {}
                Value::Integer(i) => {
                    let serial_type = SerialType::from(value);
                    match serial_type.kind() {
                        SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 => {}
                        SerialTypeKind::I8 => buf.extend_from_slice(&(*i as i8).to_be_bytes()),
                        SerialTypeKind::I16 => buf.extend_from_slice(&(*i as i16).to_be_bytes()),
                        SerialTypeKind::I24 => {
                            buf.extend_from_slice(&(*i as i32).to_be_bytes()[1..])
                        } // remove most significant byte
                        SerialTypeKind::I32 => buf.extend_from_slice(&(*i as i32).to_be_bytes()),
                        SerialTypeKind::I48 => buf.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialTypeKind::I64 => buf.extend_from_slice(&i.to_be_bytes()),
                        _ => unreachable!(),
                    }
                }
                Value::Float(f) => buf.extend_from_slice(&f.to_be_bytes()),
                Value::Text(t) => buf.extend_from_slice(t.value.as_bytes()),
                Value::Blob(b) => buf.extend_from_slice(b),
            };
        }

        let mut header_bytes_buf: Vec<u8> = Vec::new();
        header_size = Record::calc_header_size(header_size);
        header_bytes_buf.extend(std::iter::repeat_n(0, 9));
        let n = write_varint(header_bytes_buf.as_mut_slice(), header_size as u64);
        header_bytes_buf.truncate(n);
        buf.splice(initial_i..initial_i, header_bytes_buf.iter().cloned());
    }
}

pub enum Cursor {
    BTree(Box<dyn CursorTrait>),
    IndexMethod(Box<dyn IndexMethodCursor>),
    Pseudo(Box<PseudoCursor>),
    Sorter(Box<Sorter>),
    Virtual(VirtualTableCursor),
    MaterializedView(Box<crate::incremental::cursor::MaterializedViewCursor>),
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BTree(..) => f.debug_tuple("BTree").finish(),
            Self::IndexMethod(..) => f.debug_tuple("IndexMethod").finish(),
            Self::Pseudo(..) => f.debug_tuple("Pseudo").finish(),
            Self::Sorter(..) => f.debug_tuple("Sorter").finish(),
            Self::Virtual(..) => f.debug_tuple("Virtual").finish(),
            Self::MaterializedView(..) => f.debug_tuple("MaterializedView").finish(),
        }
    }
}

impl Cursor {
    pub fn new_btree(cursor: Box<dyn CursorTrait>) -> Self {
        Self::BTree(cursor)
    }

    pub fn new_pseudo(cursor: PseudoCursor) -> Self {
        Self::Pseudo(Box::new(cursor))
    }

    pub fn new_sorter(cursor: Sorter) -> Self {
        Self::Sorter(Box::new(cursor))
    }

    pub fn new_materialized_view(
        cursor: crate::incremental::cursor::MaterializedViewCursor,
    ) -> Self {
        Self::MaterializedView(Box::new(cursor))
    }

    pub fn as_btree_mut(&mut self) -> &mut dyn CursorTrait {
        match self {
            Self::BTree(cursor) => cursor.as_mut(),
            _ => panic!("Cursor is not a btree"),
        }
    }

    pub fn as_pseudo_mut(&mut self) -> &mut PseudoCursor {
        match self {
            Self::Pseudo(cursor) => cursor,
            _ => panic!("Cursor is not a pseudo cursor"),
        }
    }

    pub fn as_sorter_mut(&mut self) -> &mut Sorter {
        match self {
            Self::Sorter(cursor) => cursor,
            _ => panic!("Cursor is not a sorter cursor"),
        }
    }

    pub fn as_virtual_mut(&mut self) -> &mut VirtualTableCursor {
        match self {
            Self::Virtual(cursor) => cursor,
            _ => panic!("Cursor is not a virtual cursor"),
        }
    }

    pub fn as_materialized_view_mut(
        &mut self,
    ) -> &mut crate::incremental::cursor::MaterializedViewCursor {
        match self {
            Self::MaterializedView(cursor) => cursor,
            _ => panic!("Cursor is not a materialized view cursor"),
        }
    }

    pub fn as_index_method_mut(&mut self) -> &mut dyn IndexMethodCursor {
        match self {
            Self::IndexMethod(cursor) => cursor.as_mut(),
            _ => panic!("Cursor is not an IndexMethod cursor"),
        }
    }
}

#[derive(Debug)]
#[must_use]
pub enum IOCompletions {
    Single(Completion),
}

impl IOCompletions {
    /// Wais for the Completions to complete
    pub fn wait<I: ?Sized + IO>(self, io: &I) -> Result<()> {
        match self {
            IOCompletions::Single(c) => io.wait_for_completion(c),
        }
    }

    pub fn finished(&self) -> bool {
        match self {
            IOCompletions::Single(c) => c.finished(),
        }
    }

    /// Send abort signal to completions
    pub fn abort(&self) {
        match self {
            IOCompletions::Single(c) => c.abort(),
        }
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        match self {
            IOCompletions::Single(c) => c.get_error(),
        }
    }

    pub fn set_waker(&self, waker: Option<&Waker>) {
        if let Some(waker) = waker {
            match self {
                IOCompletions::Single(c) => c.set_waker(waker),
            }
        }
    }
}

#[derive(Debug)]
#[must_use]
pub enum IOResult<T> {
    Done(T),
    IO(IOCompletions),
}

impl<T> IOResult<T> {
    #[inline]
    pub fn is_io(&self) -> bool {
        matches!(self, IOResult::IO(..))
    }

    #[inline]
    pub fn map<U>(self, func: impl FnOnce(T) -> U) -> IOResult<U> {
        match self {
            IOResult::Done(t) => IOResult::Done(func(t)),
            IOResult::IO(io) => IOResult::IO(io),
        }
    }
}

/// Evaluate a Result<IOResult<T>>, if IO return IO.
#[macro_export]
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
            Err(err) => return Err(err),
        }
    };
}

#[macro_export]
macro_rules! return_and_restore_if_io {
    ($field:expr, $saved_state:expr, $e:expr) => {
        match $e {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => {
                let _ = std::mem::replace($field, $saved_state);
                return Ok(IOResult::IO(io));
            }
            Err(e) => {
                let _ = std::mem::replace($field, $saved_state);
                return Err(e);
            }
        }
    };
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SeekResult {
    /// Record matching the [SeekOp] found in the B-tree and cursor was positioned to point onto that record
    Found,
    /// Record matching the [SeekOp] doesn't exists in the B-tree
    NotFound,
    /// This result can happen only if eq_only for [SeekOp] is false
    /// In this case Seek can position cursor to the leaf page boundaries (before the start, after the end)
    /// (e.g. if leaf page holds rows with keys from range [1..10], key 10 is absent and [SeekOp] is >= 10)
    ///
    /// turso-db has this extra [SeekResult] in order to make [BTreeCursor::seek] method to position cursor at
    /// the leaf of potential insertion, but also communicate to caller the fact that current cursor position
    /// doesn't hold a matching entry
    /// (necessary for Seek{XX} VM op-codes, so these op-codes will try to advance cursor in order to move it to matching entry)
    TryAdvance,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// The match condition of a table/index seek.
pub enum SeekOp {
    /// If eq_only is true, this means in practice:
    /// We are iterating forwards, but we are really looking for an exact match on the seek key.
    GE {
        eq_only: bool,
    },
    GT,
    /// If eq_only is true, this means in practice:
    /// We are iterating backwards, but we are really looking for an exact match on the seek key.
    LE {
        eq_only: bool,
    },
    LT,
}

impl SeekOp {
    /// A given seek op implies an iteration direction.
    ///
    /// For example, a seek with SeekOp::GT implies:
    /// Find the first table/index key that compares greater than the seek key
    /// -> used in forwards iteration.
    ///
    /// A seek with SeekOp::LE implies:
    /// Find the last table/index key that compares less than or equal to the seek key
    /// -> used in backwards iteration.
    #[inline(always)]
    pub fn iteration_direction(&self) -> IterationDirection {
        match self {
            SeekOp::GE { .. } | SeekOp::GT => IterationDirection::Forwards,
            SeekOp::LE { .. } | SeekOp::LT => IterationDirection::Backwards,
        }
    }

    pub fn eq_only(&self) -> bool {
        match self {
            SeekOp::GE { eq_only } | SeekOp::LE { eq_only } => *eq_only,
            _ => false,
        }
    }

    pub fn reverse(&self) -> Self {
        match self {
            SeekOp::GE { eq_only } => SeekOp::LE { eq_only: *eq_only },
            SeekOp::GT => SeekOp::LT,
            SeekOp::LE { eq_only } => SeekOp::GE { eq_only: *eq_only },
            SeekOp::LT => SeekOp::GT,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SeekKey<'a> {
    TableRowId(i64),
    IndexKey(&'a ImmutableRecord),
}

#[derive(Debug)]
pub enum DatabaseChangeType {
    Delete,
    Update { bin_record: Vec<u8> },
    Insert { bin_record: Vec<u8> },
}

#[derive(Debug)]
pub struct DatabaseChange {
    pub change_id: i64,
    pub change_time: u64,
    pub change: DatabaseChangeType,
    pub table_name: String,
    pub id: i64,
}

#[derive(Debug)]
pub struct WalFrameInfo {
    pub page_no: u32,
    pub db_size: u32,
}

#[derive(Debug, PartialEq)]
pub struct WalState {
    pub checkpoint_seq_no: u32,
    pub max_frame: u64,
}

impl WalFrameInfo {
    pub fn is_commit_frame(&self) -> bool {
        self.db_size > 0
    }
    pub fn from_frame_header(frame: &[u8]) -> Self {
        let page_no = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let db_size = u32::from_be_bytes(frame[4..8].try_into().unwrap());
        Self { page_no, db_size }
    }
    pub fn put_to_frame_header(&self, frame: &mut [u8]) {
        frame[0..4].copy_from_slice(&self.page_no.to_be_bytes());
        frame[4..8].copy_from_slice(&self.db_size.to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;

    #[test]
    fn test_value_iterator_simple() {
        let mut buf = Vec::new();
        let record = Record::new(vec![Value::Integer(42), Value::Text(Text::new("hello"))]);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();
        assert!(!iter.is_empty());
        assert_eq!(iter.clone().count(), 2);

        let mut iter = ValueIterator::new(&buf).unwrap();

        let val = iter.next().unwrap().unwrap();
        assert_eq!(val, ValueRef::Integer(42));

        let val = iter.next().unwrap().unwrap();
        assert_eq!(
            val,
            ValueRef::Text(TextRef::new("hello", TextSubtype::Text))
        );

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_value_iterator_nulls() {
        let mut buf = Vec::new();
        let record = Record::new(vec![Value::Null, Value::Null, Value::Null]);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();

        for val in iter {
            assert_eq!(val.unwrap(), ValueRef::Null);
        }
    }

    #[test]
    fn test_value_iterator_mixed_types() {
        let mut buf = Vec::new();
        let record = Record::new(vec![
            Value::Null,
            Value::Integer(100),
            Value::Float(std::f64::consts::PI),
            Value::Text(Text::new("test")),
            Value::Blob(vec![1, 2, 3]),
            Value::Integer(0),
            Value::Integer(1),
        ]);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();
        let values: Vec<_> = iter.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(values[0], ValueRef::Null);
        assert_eq!(values[1], ValueRef::Integer(100));
        assert_eq!(values[2], ValueRef::Float(std::f64::consts::PI));
        assert_eq!(
            values[3],
            ValueRef::Text(TextRef::new("test", TextSubtype::Text))
        );
        assert_eq!(values[4], ValueRef::Blob(&[1, 2, 3]));
        assert_eq!(values[5], ValueRef::Integer(0));
        assert_eq!(values[6], ValueRef::Integer(1));
    }

    #[test]
    fn test_value_iterator_large_record() {
        let mut buf = Vec::new();
        let values: Vec<Value> = (0..20).map(|i| Value::Integer(i as i64)).collect();
        let record = Record::new(values);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();
        assert_eq!(iter.count(), 20);

        let iter = ValueIterator::new(&buf).unwrap();
        for (i, val) in iter.enumerate() {
            assert_eq!(val.unwrap(), ValueRef::Integer(i as i64));
        }
    }

    #[test]
    fn test_value_iterator_zero_allocation() {
        let mut buf = Vec::new();
        let values: Vec<Value> = (0..5).map(|i| Value::Integer(i as i64)).collect();
        let record = Record::new(values);
        record.serialize(&mut buf);

        let mut iter = ValueIterator::new(&buf).unwrap();
        let _ = iter.next();
        let _ = iter.next();
    }

    pub fn compare_immutable_for_testing(
        l: &[ValueRef],
        r: &[ValueRef],
        index_key_info: &[KeyInfo],
        tie_breaker: std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        let min_len = l.len().min(r.len());

        for i in 0..min_len {
            let column_order = index_key_info[i].sort_order;
            let collation = index_key_info[i].collation;

            let cmp = match (&l[i], &r[i]) {
                (ValueRef::Text(left), ValueRef::Text(right)) => {
                    collation.compare_strings(left, right)
                }
                _ => l[i].partial_cmp(&r[i]).unwrap_or(std::cmp::Ordering::Equal),
            };

            if cmp != std::cmp::Ordering::Equal {
                return match column_order {
                    SortOrder::Asc => cmp,
                    SortOrder::Desc => cmp.reverse(),
                };
            }
        }

        tie_breaker
    }

    fn create_record(values: Vec<Value>) -> ImmutableRecord {
        let registers: Vec<Register> = values.into_iter().map(Register::Value).collect();
        ImmutableRecord::from_registers(&registers, registers.len())
    }

    fn create_index_info(
        num_cols: usize,
        sort_orders: Vec<SortOrder>,
        collations: Vec<CollationSeq>,
    ) -> IndexInfo {
        IndexInfo {
            key_info: sort_orders
                .into_iter()
                .zip(collations)
                .map(|(sort_order, collation)| KeyInfo {
                    sort_order,
                    collation,
                })
                .collect(),
            has_rowid: false,
            num_cols,
        }
    }

    fn assert_compare_matches_full_comparison(
        serialized_values: Vec<Value>,
        unpacked_values: Vec<ValueRef>,
        index_info: &IndexInfo,
        test_name: &str,
    ) {
        let serialized = create_record(serialized_values.clone());

        let serialized_ref_values: Vec<ValueRef> =
            serialized_values.iter().map(Value::as_ref).collect();

        let tie_breaker = std::cmp::Ordering::Equal;

        let gold_result = compare_immutable_for_testing(
            &serialized_ref_values,
            &unpacked_values,
            &index_info.key_info,
            tie_breaker,
        );

        let comparer = find_compare(unpacked_values.iter().peekable(), index_info);
        let optimized_result = comparer
            .compare(&serialized, &unpacked_values, index_info, 0, tie_breaker)
            .unwrap();

        assert_eq!(
            gold_result, optimized_result,
            "Test '{test_name}' failed: Full Comparison: {gold_result:?}, Optimized: {optimized_result:?}, Strategy: {comparer:?}"
        );

        let generic_result = compare_records_generic(
            &serialized,
            unpacked_values.iter(),
            index_info,
            0,
            tie_breaker,
        )
        .unwrap();
        assert_eq!(
            gold_result, generic_result,
            "Test '{test_name}' failed with generic: Full Comparison: {gold_result:?}, Generic: {generic_result:?}\n LHS: {serialized_values:?}\n RHS: {unpacked_values:?}"
        );
    }

    #[test]
    fn test_calc_header_size() {
        // Test 1-byte header size (serial type sizes 0 to 126)
        const MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER: usize = 0;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1
        );
        const BITS_7_MAX: usize = (1 << 7) - 1; // varints use 7 bits for the value and 1 continuation bit
        const MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER: usize = BITS_7_MAX - 1;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1
        );

        // Test 2-byte header size (serial type sizes 127 to 16381)
        const MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER: usize =
            MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 2
        );
        const BITS_14_MAX: usize = (1 << 14) - 1;
        const MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER: usize = BITS_14_MAX - 2;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 2
        );

        // Test 3-byte header size (serial type sizes 16382 to 2097148)
        const MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER: usize =
            MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 1;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 3
        );
        const BITS_21_MAX: usize = (1 << 21) - 1;
        const MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER: usize = BITS_21_MAX - 3;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 3
        );

        // Test 4-byte header size (serial type sizes 2097149 to 268435451)
        const MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER: usize =
            MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 1;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER + 4
        );
        const BITS_28_MAX: usize = (1 << 28) - 1;
        const MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER: usize = BITS_28_MAX - 4;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER + 4
        );
    }

    #[test]
    fn test_integer_fast_path() {
        let index_info = create_index_info(
            2,
            vec![SortOrder::Asc, SortOrder::Asc],
            vec![CollationSeq::Binary; 2],
        );

        let test_cases = vec![
            (
                vec![Value::Integer(42)],
                vec![ValueRef::Integer(42)],
                "equal_integers",
            ),
            (
                vec![Value::Integer(10)],
                vec![ValueRef::Integer(20)],
                "less_than_integers",
            ),
            (
                vec![Value::Integer(30)],
                vec![ValueRef::Integer(20)],
                "greater_than_integers",
            ),
            (
                vec![Value::Integer(0)],
                vec![ValueRef::Integer(0)],
                "zero_integers",
            ),
            (
                vec![Value::Integer(-5)],
                vec![ValueRef::Integer(-5)],
                "negative_integers",
            ),
            (
                vec![Value::Integer(i64::MAX)],
                vec![ValueRef::Integer(i64::MAX)],
                "max_integers",
            ),
            (
                vec![Value::Integer(i64::MIN)],
                vec![ValueRef::Integer(i64::MIN)],
                "min_integers",
            ),
            (
                vec![Value::Integer(42), Value::Text(Text::new("hello"))],
                vec![
                    ValueRef::Integer(42),
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                ],
                "integer_text_equal",
            ),
            (
                vec![Value::Integer(42), Value::Text(Text::new("hello"))],
                vec![
                    ValueRef::Integer(42),
                    ValueRef::Text(TextRef::new("world", TextSubtype::Text)),
                ],
                "integer_equal_text_different",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            println!(
                "Testing integer fast path `{test_name}`\nLHS: {serialized_values:?}\nRHS: {unpacked_values:?}"
            );
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_string_fast_path() {
        let index_info = create_index_info(
            2,
            vec![SortOrder::Asc, SortOrder::Asc],
            vec![CollationSeq::Binary; 2],
        );

        let test_cases = vec![
            (
                vec![Value::Text(Text::new("hello"))],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "equal_strings",
            ),
            (
                vec![Value::Text(Text::new("abc"))],
                vec![ValueRef::Text(TextRef::new("def", TextSubtype::Text))],
                "less_than_strings",
            ),
            (
                vec![Value::Text(Text::new("xyz"))],
                vec![ValueRef::Text(TextRef::new("abc", TextSubtype::Text))],
                "greater_than_strings",
            ),
            (
                vec![Value::Text(Text::new(""))],
                vec![ValueRef::Text(TextRef::new("", TextSubtype::Text))],
                "empty_strings",
            ),
            (
                vec![Value::Text(Text::new("a"))],
                vec![ValueRef::Text(TextRef::new("aa", TextSubtype::Text))],
                "prefix_strings",
            ),
            // Multi-field with string first
            (
                vec![Value::Text(Text::new("hello")), Value::Integer(42)],
                vec![
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                    ValueRef::Integer(42),
                ],
                "string_integer_equal",
            ),
            (
                vec![Value::Text(Text::new("hello")), Value::Integer(42)],
                vec![
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                    ValueRef::Integer(99),
                ],
                "string_equal_integer_different",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_type_precedence() {
        let index_info = create_index_info(1, vec![SortOrder::Asc], vec![CollationSeq::Binary]);

        // Test SQLite type precedence: NULL < Numbers < Text < Blob
        let test_cases = vec![
            // NULL vs others
            (
                vec![Value::Null],
                vec![ValueRef::Integer(42)],
                "null_vs_integer",
            ),
            (
                vec![Value::Null],
                vec![ValueRef::Float(64.4)],
                "null_vs_float",
            ),
            (
                vec![Value::Null],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "null_vs_text",
            ),
            (
                vec![Value::Null],
                vec![ValueRef::Blob(b"blob")],
                "null_vs_blob",
            ),
            // Numbers vs Text/Blob
            (
                vec![Value::Integer(42)],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "integer_vs_text",
            ),
            (
                vec![Value::Float(64.4)],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "float_vs_text",
            ),
            (
                vec![Value::Integer(42)],
                vec![ValueRef::Blob(b"blob")],
                "integer_vs_blob",
            ),
            (
                vec![Value::Float(64.4)],
                vec![ValueRef::Blob(b"blob")],
                "float_vs_blob",
            ),
            // Text vs Blob
            (
                vec![Value::Text(Text::new("hello"))],
                vec![ValueRef::Blob(b"blob")],
                "text_vs_blob",
            ),
            // Integer vs Float (affinity conversion)
            (
                vec![Value::Integer(42)],
                vec![ValueRef::Float(42.0)],
                "integer_vs_equal_float",
            ),
            (
                vec![Value::Integer(42)],
                vec![ValueRef::Float(42.5)],
                "integer_vs_different_float",
            ),
            (
                vec![Value::Float(42.5)],
                vec![ValueRef::Integer(42)],
                "float_vs_integer",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_sort_order_desc() {
        let index_info = create_index_info(
            2,
            vec![SortOrder::Desc, SortOrder::Asc],
            vec![CollationSeq::Binary; 2],
        );

        let test_cases = vec![
            // DESC order should reverse first field comparison
            (
                vec![Value::Integer(10)],
                vec![ValueRef::Integer(20)],
                "desc_integer_reversed",
            ),
            (
                vec![Value::Text(Text::new("abc"))],
                vec![ValueRef::Text(TextRef::new("def", TextSubtype::Text))],
                "desc_string_reversed",
            ),
            // Mixed sort orders
            (
                vec![Value::Integer(10), Value::Text(Text::new("hello"))],
                vec![
                    ValueRef::Integer(20),
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                ],
                "desc_first_asc_second",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_edge_cases() {
        let index_info =
            create_index_info(15, vec![SortOrder::Asc; 15], vec![CollationSeq::Binary; 15]);

        let test_cases = vec![
            (
                vec![Value::Integer(42)],
                vec![
                    ValueRef::Integer(42),
                    ValueRef::Text(TextRef::new("extra", TextSubtype::Text)),
                ],
                "fewer_serialized_fields",
            ),
            (
                vec![Value::Integer(42), Value::Text(Text::new("extra"))],
                vec![ValueRef::Integer(42)],
                "fewer_unpacked_fields",
            ),
            (vec![], vec![], "both_empty"),
            (vec![], vec![ValueRef::Integer(42)], "empty_serialized"),
            (
                (0..15).map(Value::Integer).collect(),
                (0..15).map(ValueRef::Integer).collect(),
                "large_field_count",
            ),
            (
                vec![Value::Blob(vec![1, 2, 3])],
                vec![ValueRef::Blob(&[1, 2, 3])],
                "blob_first_field",
            ),
            (
                vec![Value::Text(Text::new("hello")), Value::Integer(5)],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "equal_text_prefix_but_more_serialized_fields",
            ),
            (
                vec![Value::Text(Text::new("same")), Value::Integer(5)],
                vec![
                    ValueRef::Text(TextRef::new("same", TextSubtype::Text)),
                    ValueRef::Integer(5),
                ],
                "equal_text_then_equal_int",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_skip_parameter() {
        let index_info = create_index_info(
            3,
            vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc],
            vec![CollationSeq::Binary; 3],
        );

        let serialized = create_record(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let unpacked = [
            ValueRef::Integer(1),
            ValueRef::Integer(99),
            ValueRef::Integer(3),
        ];

        let tie_breaker = std::cmp::Ordering::Equal;
        let result_skip_0 =
            compare_records_generic(&serialized, unpacked.iter(), &index_info, 0, tie_breaker)
                .unwrap();
        let result_skip_1 =
            compare_records_generic(&serialized, unpacked.iter(), &index_info, 1, tie_breaker)
                .unwrap();

        assert_eq!(result_skip_0, std::cmp::Ordering::Less);

        assert_eq!(result_skip_1, std::cmp::Ordering::Less);
    }

    #[test]
    fn test_strategy_selection() {
        let collations_small = vec![CollationSeq::Binary; 3];
        let collations_large = vec![CollationSeq::Binary; 15];
        let index_info_small = create_index_info(
            3,
            vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc],
            collations_small,
        );
        let index_info_large = create_index_info(15, vec![SortOrder::Asc; 15], collations_large);

        let int_values = [
            ValueRef::Integer(42),
            ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
        ];
        assert!(matches!(
            find_compare(int_values.iter().peekable(), &index_info_small),
            RecordCompare::Int
        ));

        let string_values = [
            ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
            ValueRef::Integer(42),
        ];
        assert!(matches!(
            find_compare(string_values.iter().peekable(), &index_info_small),
            RecordCompare::String
        ));

        let large_values: Vec<ValueRef> = (0..15).map(ValueRef::Integer).collect();
        assert!(matches!(
            find_compare(large_values.iter().peekable(), &index_info_large),
            RecordCompare::Generic
        ));

        let blob_values = [ValueRef::Blob(&[1, 2, 3])];
        assert!(matches!(
            find_compare(blob_values.iter().peekable(), &index_info_small),
            RecordCompare::Generic
        ));
    }

    #[test]
    fn test_serialize_null() {
        let record = Record::new(vec![Value::Null]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::null()));
        // Check that the buffer is empty after the header
        assert_eq!(buf.len(), header_length);
    }

    #[test]
    fn test_serialize_integers() {
        let record = Record::new(vec![
            Value::Integer(0),                 // Should use ConstInt0
            Value::Integer(1),                 // Should use ConstInt1
            Value::Integer(42),                // Should use SERIAL_TYPE_I8
            Value::Integer(1000),              // Should use SERIAL_TYPE_I16
            Value::Integer(1_000_000),         // Should use SERIAL_TYPE_I24
            Value::Integer(1_000_000_000),     // Should use SERIAL_TYPE_I32
            Value::Integer(1_000_000_000_000), // Should use SERIAL_TYPE_I48
            Value::Integer(i64::MAX),          // Should use SERIAL_TYPE_I64
        ]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8); // Header should be larger than number of values

        // Check that correct serial types were chosen
        assert_eq!(header[1] as u64, u64::from(SerialType::const_int0())); // 8
        assert_eq!(header[2] as u64, u64::from(SerialType::const_int1())); // 9
        assert_eq!(header[3] as u64, u64::from(SerialType::i8())); // 1
        assert_eq!(header[4] as u64, u64::from(SerialType::i16())); // 2
        assert_eq!(header[5] as u64, u64::from(SerialType::i24())); // 3
        assert_eq!(header[6] as u64, u64::from(SerialType::i32())); // 4
        assert_eq!(header[7] as u64, u64::from(SerialType::i48())); // 5
        assert_eq!(header[8] as u64, u64::from(SerialType::i64())); // 6

        // test that the bytes after the header can be interpreted as the correct values
        let mut cur_offset = header_length;

        // Value::Integer(0) - ConstInt0: NO PAYLOAD BYTES
        // Value::Integer(1) - ConstInt1: NO PAYLOAD BYTES

        // Value::Integer(42) - I8: 1 byte
        let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
        cur_offset += size_of::<i8>();

        // Value::Integer(1000) - I16: 2 bytes
        let i16_bytes = &buf[cur_offset..cur_offset + size_of::<i16>()];
        cur_offset += size_of::<i16>();

        // Value::Integer(1_000_000) - I24: 3 bytes
        let i24_bytes = &buf[cur_offset..cur_offset + 3];
        cur_offset += 3;

        // Value::Integer(1_000_000_000) - I32: 4 bytes
        let i32_bytes = &buf[cur_offset..cur_offset + size_of::<i32>()];
        cur_offset += size_of::<i32>();

        // Value::Integer(1_000_000_000_000) - I48: 6 bytes
        let i48_bytes = &buf[cur_offset..cur_offset + 6];
        cur_offset += 6;

        // Value::Integer(i64::MAX) - I64: 8 bytes
        let i64_bytes = &buf[cur_offset..cur_offset + size_of::<i64>()];

        // Verify the payload values
        let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
        let val_int16 = i16::from_be_bytes(i16_bytes.try_into().unwrap());

        let mut i24_with_padding = vec![0];
        i24_with_padding.extend(i24_bytes);
        let val_int24 = i32::from_be_bytes(i24_with_padding.try_into().unwrap());

        let val_int32 = i32::from_be_bytes(i32_bytes.try_into().unwrap());

        let mut i48_with_padding = vec![0, 0];
        i48_with_padding.extend(i48_bytes);
        let val_int48 = i64::from_be_bytes(i48_with_padding.try_into().unwrap());

        let val_int64 = i64::from_be_bytes(i64_bytes.try_into().unwrap());

        assert_eq!(val_int8, 42);
        assert_eq!(val_int16, 1000);
        assert_eq!(val_int24, 1_000_000);
        assert_eq!(val_int32, 1_000_000_000);
        assert_eq!(val_int48, 1_000_000_000_000);
        assert_eq!(val_int64, i64::MAX);

        //Size of buffer = header + payload bytes
        // ConstInt0 and ConstInt1 contribute 0 bytes to payload
        assert_eq!(
            buf.len(),
            header_length  // 9 bytes (header size + 8 serial types)
                + size_of::<i8>()        // I8: 1 byte
                + size_of::<i16>()        // I16: 2 bytes
                + (size_of::<i32>() - 1)        // I24: 3 bytes
                + size_of::<i32>()        // I32: 4 bytes
                + (size_of::<i64>() - 2)        // I48: 6 bytes
                + size_of::<i64>() // I64: 8 bytes
        );
    }

    #[test]
    fn test_serialize_const_integers() {
        let record = Record::new(vec![Value::Integer(0), Value::Integer(1)]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        // [header_size, serial_type_0, serial_type_1] + no payload bytes
        let expected_header_size = 3; // 1 byte for header size + 2 bytes for serial types

        assert_eq!(buf.len(), expected_header_size);

        // Check header size
        assert_eq!(buf[0], expected_header_size as u8);

        assert_eq!(buf[1] as u64, u64::from(SerialType::const_int0())); // Should be 8
        assert_eq!(buf[2] as u64, u64::from(SerialType::const_int1())); // Should be 9

        assert_eq!(buf[1], 8); // ConstInt0 serial type
        assert_eq!(buf[2], 9); // ConstInt1 serial type
    }

    #[test]
    fn test_serialize_single_const_int0() {
        let record = Record::new(vec![Value::Integer(0)]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        // Expected: [header_size=2, serial_type=8]
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 2); // Header size
        assert_eq!(buf[1], 8); // ConstInt0 serial type
    }

    #[test]
    fn test_serialize_float() {
        #[warn(clippy::approx_constant)]
        let record = Record::new(vec![Value::Float(3.15555)]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for FLOAT
        assert_eq!(header[1] as u64, u64::from(SerialType::f64()));
        // Check that the bytes after the header can be interpreted as the float
        let float_bytes = &buf[header_length..header_length + size_of::<f64>()];
        let float = f64::from_be_bytes(float_bytes.try_into().unwrap());
        assert_eq!(float, 3.15555);
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + size_of::<f64>());
    }

    #[test]
    fn test_serialize_text() {
        let text = "hello";
        let record = Record::new(vec![Value::Text(Text::new(text))]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for TEXT, which is (len * 2 + 13)
        assert_eq!(header[1], (5 * 2 + 13) as u8);
        // Check the actual text bytes
        assert_eq!(&buf[2..7], b"hello");
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + text.len());
    }

    #[test]
    fn test_serialize_blob() {
        let blob = vec![1, 2, 3, 4, 5];
        let record = Record::new(vec![Value::Blob(blob.clone())]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for BLOB, which is (len * 2 + 12)
        assert_eq!(header[1], (5 * 2 + 12) as u8);
        // Check the actual blob bytes
        assert_eq!(&buf[2..7], &[1, 2, 3, 4, 5]);
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + blob.len());
    }

    #[test]
    fn test_serialize_mixed_types() {
        let text = "test";
        let record = Record::new(vec![
            Value::Null,
            Value::Integer(42),
            Value::Float(3.15),
            Value::Text(Text::new(text)),
        ]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::null()));
        // Third byte should be serial type for I8
        assert_eq!(header[2] as u64, u64::from(SerialType::i8()));
        // Fourth byte should be serial type for F64
        assert_eq!(header[3] as u64, u64::from(SerialType::f64()));
        // Fifth byte should be serial type for TEXT, which is (len * 2 + 13)
        assert_eq!(header[4] as u64, (4 * 2 + 13) as u64);

        // Check that the bytes after the header can be interpreted as the correct values
        let mut cur_offset = header_length;
        let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
        cur_offset += size_of::<i8>();
        let f64_bytes = &buf[cur_offset..cur_offset + size_of::<f64>()];
        cur_offset += size_of::<f64>();
        let text_bytes = &buf[cur_offset..cur_offset + text.len()];

        let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
        let val_float = f64::from_be_bytes(f64_bytes.try_into().unwrap());
        let val_text = String::from_utf8(text_bytes.to_vec()).unwrap();

        assert_eq!(val_int8, 42);
        assert_eq!(val_float, 3.15);
        assert_eq!(val_text, "test");

        // Check that buffer length is correct
        assert_eq!(
            buf.len(),
            header_length + size_of::<i8>() + size_of::<f64>() + text.len()
        );
    }

    #[test]
    fn test_column_count_matches_values_written() {
        // Test with different numbers of values
        for num_values in 1..=10 {
            let values: Vec<Value> = (0..num_values).map(|i| Value::Integer(i as i64)).collect();

            let record = ImmutableRecord::from_values(&values, values.len());
            let cnt = record.column_count();
            assert_eq!(
                cnt, num_values,
                "column_count should be {num_values}, not {cnt}"
            );
        }
    }
}
