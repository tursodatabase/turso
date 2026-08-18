//! The core SQL value representation: `Value`, `ValueRef`, `Text`, blobs,
//! and aggregate accumulator state.

use branches::{mark_unlikely, unlikely};
use either::Either;
use turso_ext::{
    AggCtx, ContextDestructor, FinalizeFunction, StepFunction, Value as ExtValue, ValueDestructor,
    ValueType as ExtValueType,
};

use crate::alloc::*;
use crate::numeric::format_float;
use crate::numeric::nonnan::NonNan;
use crate::numeric::Numeric;
use turso_core_common::{LimboError, Result};
use turso_macros::turso_debug_assert;

use std::borrow::{Borrow, Cow};
use std::fmt::{Debug, Display};
use std::ops::Deref;

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
    fn do_extend(&mut self, other: &T) -> Result<()>;
}

/// Copies non-overlapping bytes while keeping common small lengths visible to the optimizer.
///
/// # Safety
///
/// `src` and `dst` must be valid for `len` bytes and must not overlap.
#[inline(always)]
unsafe fn copy_nonoverlapping_inline(src: *const u8, dst: *mut u8, len: usize) {
    // Record decoding frequently reuses registers for short values. Fixed-size
    // copies compile inline instead of calling the platform memcpy routine.
    unsafe {
        match len {
            0 => {}
            1 => std::ptr::copy_nonoverlapping(src, dst, 1),
            2 => std::ptr::copy_nonoverlapping(src, dst, 2),
            3 => std::ptr::copy_nonoverlapping(src, dst, 3),
            4 => std::ptr::copy_nonoverlapping(src, dst, 4),
            5 => std::ptr::copy_nonoverlapping(src, dst, 5),
            6 => std::ptr::copy_nonoverlapping(src, dst, 6),
            7 => std::ptr::copy_nonoverlapping(src, dst, 7),
            8 => std::ptr::copy_nonoverlapping(src, dst, 8),
            9 => std::ptr::copy_nonoverlapping(src, dst, 9),
            10 => std::ptr::copy_nonoverlapping(src, dst, 10),
            11 => std::ptr::copy_nonoverlapping(src, dst, 11),
            12 => std::ptr::copy_nonoverlapping(src, dst, 12),
            13 => std::ptr::copy_nonoverlapping(src, dst, 13),
            14 => std::ptr::copy_nonoverlapping(src, dst, 14),
            15 => std::ptr::copy_nonoverlapping(src, dst, 15),
            16 => std::ptr::copy_nonoverlapping(src, dst, 16),
            _ => std::ptr::copy_nonoverlapping(src, dst, len),
        }
    }
}

impl<T: AnyText> Extendable<T> for Text {
    #[inline(always)]
    fn do_extend(&mut self, other: &T) -> Result<()> {
        let other_str = other.as_ref();
        match &mut self.value {
            Cow::Owned(s) => {
                let needed = other_str.len();
                if s.capacity() >= needed {
                    // SAFETY: capacity >= needed, source is valid UTF-8
                    turso_debug_assert!(
                        s.as_ptr().wrapping_add(s.len()) <= other_str.as_ptr()
                            || other_str.as_ptr().wrapping_add(other_str.len()) <= s.as_ptr(),
                        "source and destination ranges must not overlap"
                    );
                    unsafe {
                        copy_nonoverlapping_inline(other_str.as_ptr(), s.as_mut_ptr(), needed);
                        s.as_mut_vec().set_len(needed);
                    }
                } else {
                    other_str.clone_into(s);
                }
            }
            Cow::Borrowed(_) => {
                self.value = Cow::Owned(other_str.to_owned());
            }
        }
        self.subtype = other.subtype();
        Ok(())
    }
}

impl<T: AnyBlob> Extendable<T> for ValueBlob {
    #[inline(always)]
    fn do_extend(&mut self, other: &T) -> Result<()> {
        let other_slice = other.as_slice();
        let needed = other_slice.len();
        if self.capacity() >= needed {
            // SAFETY: capacity >= needed
            turso_debug_assert!(
                self.as_ptr().wrapping_add(self.len()) <= other_slice.as_ptr()
                    || other_slice.as_ptr().wrapping_add(other_slice.len()) <= self.as_ptr(),
                "source and destination ranges must not overlap"
            );
            unsafe {
                copy_nonoverlapping_inline(other_slice.as_ptr(), self.as_mut_ptr(), needed);
                self.set_len(needed);
            }
        } else {
            // Reserve before mutation so an allocation failure leaves the old value intact.
            self.try_reserve(needed - self.len())?;
            self.clear();
            self.extend_from_slice(other_slice);
        }
        Ok(())
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

impl AnyBlob for ValueBlob {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(nightly)]
impl AnyBlob for std::vec::Vec<u8> {
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

// Note: Struct and union values are serialized directly in VDBE instructions
// (MakeArray for structs, op_union_pack for unions) using the SQLite record format for structs
// and [tag_name_len: 1 byte][tag_name: N bytes][record] for unions.
// No intermediate StructValue/UnionValue types are needed — blobs are
// constructed from registers and extracted directly into registers.

/// Owned bytes stored by [`Value::Blob`].
///
/// Stable builds use `std::vec::Vec`; allocator-enabled nightly builds retain
/// [`TursoAllocator`] in the vector type.
pub type ValueBlob = crate::alloc::Vec<u8>;

#[inline]
pub fn value_blob_from_slice(
    bytes: &[u8],
) -> std::result::Result<ValueBlob, crate::alloc::TryReserveError> {
    bytes.try_to_vec()
}

#[cfg(feature = "serde")]
mod value_blob_serde {
    use super::ValueBlob;
    use crate::alloc::{TursoAllocExt, TursoTryWithCapacityExt, TursoVecExt};
    use serde::de::{Error as _, SeqAccess, Visitor};
    use serde::{Deserializer, Serialize as _, Serializer};
    use std::fmt;

    pub fn serialize<S>(value: &ValueBlob, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_slice().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ValueBlob, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueBlobVisitor;

        impl<'de> Visitor<'de> for ValueBlobVisitor {
            type Value = ValueBlob;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a sequence of bytes")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut value = match sequence.size_hint() {
                    Some(capacity) => {
                        <ValueBlob as TursoTryWithCapacityExt>::try_with_capacity_ext(capacity)
                            .map_err(A::Error::custom)?
                    }
                    None => <ValueBlob as TursoAllocExt>::new(),
                };
                while let Some(byte) = sequence.next_element()? {
                    value.try_push(byte).map_err(A::Error::custom)?;
                }
                Ok(value)
            }
        }

        deserializer.deserialize_seq(ValueBlobVisitor)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    Null,
    Numeric(Numeric),
    Text(Text),
    Blob(#[cfg_attr(feature = "serde", serde(with = "value_blob_serde"))] ValueBlob),
}

impl TryClone for Value {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        match self {
            Self::Null => Ok(Self::Null),
            Self::Numeric(numeric) => Ok(Self::Numeric(*numeric)),
            Self::Text(text) => {
                let mut value = String::new();
                value.try_reserve(text.as_str().len())?;
                value.push_str(text.as_str());
                Ok(Self::Text(Text {
                    value: Cow::Owned(value),
                    subtype: text.subtype,
                }))
            }
            Self::Blob(blob) => Self::from_slice(blob),
        }
    }

    /// Fallibly copies `source` into `self`, reusing the existing Text/Blob
    /// allocation when the variants match, so hot per-row copies are
    /// allocation-free once buffers have grown to the row size. On allocation
    /// failure `self` is left valid but unspecified (an empty Text/Blob).
    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::CloneFrom)]
    fn try_clone_from(&mut self, source: &Self) -> Result<(), Self::Error> {
        match (self, source) {
            (Self::Text(dst), Self::Text(src)) => {
                let src_str = src.as_str();
                match &mut dst.value {
                    Cow::Owned(s) => {
                        s.clear();
                        s.try_reserve(src_str.len())?;
                        s.push_str(src_str);
                    }
                    borrowed => {
                        let mut s = String::new();
                        s.try_reserve(src_str.len())?;
                        s.push_str(src_str);
                        *borrowed = Cow::Owned(s);
                    }
                }
                dst.subtype = src.subtype;
            }
            (Self::Blob(dst), Self::Blob(src)) => {
                dst.clear();
                dst.try_extend(src.iter().copied())?;
            }
            (dst, src) => {
                *dst = src.try_clone()?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Numeric(Numeric),
    Text(TextRef<'a>),
    Blob(&'a [u8]),
}

impl Debug for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueRef::Null => write!(f, "Null"),
            ValueRef::Numeric(Numeric::Integer(i)) => f.debug_tuple("Integer").field(i).finish(),
            ValueRef::Numeric(Numeric::Float(float)) => {
                let fval: f64 = (*float).into();
                f.debug_tuple("Float").field(&fval).finish()
            }
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
    fn as_value_ref(&'_ self) -> ValueRef<'_>;
}

impl<'b> AsValueRef for ValueRef<'b> {
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        *self
    }
}

impl AsValueRef for Value {
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        self.as_ref()
    }
}

impl AsValueRef for &mut Value {
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        self.as_ref()
    }
}

impl<V1, V2> AsValueRef for Either<V1, V2>
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        match self {
            Either::Left(left) => left.as_value_ref(),
            Either::Right(right) => right.as_value_ref(),
        }
    }
}

impl<V: AsValueRef> AsValueRef for &V {
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        (*self).as_value_ref()
    }
}

impl Value {
    pub const fn from_f64(f: f64) -> Self {
        match NonNan::new(f) {
            Some(nn) => Self::Numeric(Numeric::Float(nn)),
            None => Self::Null,
        }
    }

    pub const fn from_i64(i: i64) -> Self {
        Self::Numeric(Numeric::Integer(i))
    }

    pub fn as_ref(&'_ self) -> ValueRef<'_> {
        match self {
            Value::Null => ValueRef::Null,
            Value::Numeric(n) => ValueRef::Numeric(*n),
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

    pub const fn from_blob(data: ValueBlob) -> Self {
        Value::Blob(data)
    }

    #[inline]
    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::FromSlice)]
    pub fn from_slice(data: &[u8]) -> std::result::Result<Self, TryReserveError> {
        Ok(Value::Blob(value_blob_from_slice(data)?))
    }

    pub fn to_text(&self) -> Option<&str> {
        match self {
            Value::Text(t) => Some(t.as_str()),
            _ => None,
        }
    }

    pub const fn as_blob(&self) -> &ValueBlob {
        match self {
            Value::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }

    pub const fn as_blob_mut(&mut self) -> &mut ValueBlob {
        match self {
            Value::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }
    pub fn as_float(&self) -> f64 {
        match self {
            Value::Numeric(Numeric::Float(f)) => f64::from(*f),
            Value::Numeric(Numeric::Integer(i)) => *i as f64,
            _ => panic!("as_float must be called only for Value::Numeric"),
        }
    }

    pub fn to_float_or_zero(&self) -> f64 {
        match self {
            Value::Numeric(Numeric::Float(f)) => f64::from(*f),
            Value::Numeric(Numeric::Integer(i)) => *i as f64,
            _ => 0.0,
        }
    }

    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Value::Numeric(Numeric::Integer(i)) => Some(*i),
            _ => None,
        }
    }

    pub const fn as_uint(&self) -> u64 {
        match self {
            Value::Numeric(Numeric::Integer(i)) => (*i).cast_unsigned(),
            _ => 0,
        }
    }

    pub fn from_text(text: impl Into<Cow<'static, str>>) -> Self {
        Value::Text(Text::new(text))
    }

    pub const fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Numeric(Numeric::Integer(_)) => ValueType::Integer,
            Value::Numeric(Numeric::Float(_)) => ValueType::Float,
            Value::Text(_) => ValueType::Text,
            Value::Blob(_) => ValueType::Blob,
        }
    }
    pub fn serialize_serial(&self, out: &mut std::vec::Vec<u8>) {
        match self {
            Value::Null => {}
            Value::Numeric(Numeric::Integer(i)) => {
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
            Value::Numeric(Numeric::Float(f)) => {
                let fval: f64 = (*f).into();
                out.extend_from_slice(&fval.to_be_bytes());
            }
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
    pub context: usize,
    pub state: *mut AggCtx,
    pub argc: usize,
    pub step_fn: StepFunction,
    pub finalize_fn: FinalizeFunction,
    pub aggregate_destructor: Option<ContextDestructor>,
    pub value_destructor: Option<ValueDestructor>,
}

/// Please use Display trait for all limbo output so we have single origin of truth
/// When you need value as string:
/// ---GOOD---
/// format!("{}", value);
/// ---BAD---
/// match value {
///   Value::Numeric(Numeric::Integer(i)) => i.to_string(),
///   Value::Numeric(Numeric::Float(f)) => f64::from(*f).to_string(),
///   ....
/// }
impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, ""),
            Self::Numeric(Numeric::Integer(i)) => write!(f, "{i}"),
            Self::Numeric(Numeric::Float(fl)) => f.write_str(&format_float(f64::from(*fl))),
            Self::Text(s) => write!(f, "{}", s.as_str()),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

impl Value {
    pub fn to_ffi(&self) -> ExtValue {
        match self {
            Self::Null => ExtValue::null(),
            Self::Numeric(Numeric::Integer(i)) => ExtValue::from_integer(*i),
            Self::Numeric(Numeric::Float(fl)) => ExtValue::from_float(f64::from(*fl)),
            Self::Text(text) => ExtValue::from_text(text.as_str().to_string()),
            Self::Blob(blob) => ExtValue::from_blob(blob.to_vec()),
        }
    }

    pub fn from_ffi_ref(v: &ExtValue) -> Result<Self> {
        match v.value_type() {
            ExtValueType::Null => Ok(Value::Null),
            ExtValueType::Integer => {
                let Some(int) = v.to_integer() else {
                    return Ok(Value::Null);
                };
                Ok(Value::from_i64(int))
            }
            ExtValueType::Float => {
                let Some(float) = v.to_float() else {
                    return Ok(Value::Null);
                };
                Ok(Value::from_f64(float))
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
                Ok(Value::from_slice(&blob)?)
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
        }
    }

    pub fn from_ffi(v: ExtValue) -> Result<Self> {
        let res = Self::from_ffi_ref(&v);
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
    ($ty:ty) => {
        impl FromValue for $ty {
            fn from_sql(val: Value) -> Result<Self> {
                match val {
                    Value::Null => Err(LimboError::NullValue),
                    Value::Numeric(Numeric::Integer(i)) => {
                        <$ty>::try_from(i).map_err(|_| LimboError::IntegerOverflow)
                    }
                    _ => Err(LimboError::InvalidColumnType),
                }
            }
        }

        impl Sealed for $ty {}
    };
}

impl_int_from_value!(i32);
impl_int_from_value!(u32);
impl_int_from_value!(i64);
impl_int_from_value!(u64);

impl FromValue for f64 {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Numeric(Numeric::Float(f)) => Ok(f64::from(f)),
            Value::Numeric(Numeric::Integer(i)) => Ok(i as f64),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl Sealed for f64 {}

impl FromValue for ValueBlob {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Blob(blob) => Ok(blob),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl Sealed for ValueBlob {}

impl<const N: usize> FromValue for [u8; N] {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Blob(blob) => blob
                .as_slice()
                .try_into()
                .map_err(|_| LimboError::InvalidBlobSize(N)),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl<const N: usize> Sealed for [u8; N] {}

impl FromValue for String {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Text(s) => Ok(s.to_string()),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl Sealed for String {}

impl FromValue for bool {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Numeric(Numeric::Integer(i)) => match i {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(LimboError::InvalidColumnType),
            },
            _ => Err(LimboError::InvalidColumnType),
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

impl TryClone for AggContext {
    type Error = TryReserveError;

    /// Fallible clone: the builtin payload's Vec and each contained Text/Blob
    /// go through fallible reservation. External state holds only FFI
    /// pointers and copies without allocating.
    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::CloneFrom)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        match self {
            Self::Builtin(payload) => {
                let mut values = Vec::try_with_capacity_ext(payload.len())?;
                for value in payload {
                    let mut copy = Value::Null;
                    copy.try_clone_from(value)?;
                    values.push(copy);
                }
                Ok(Self::Builtin(values))
            }
            Self::External(_) => Ok(self.clone()),
        }
    }
}

impl AggContext {
    pub fn compute_external(&self) -> Result<Value> {
        if let Self::External(ext_state) = self {
            let mut final_value =
                unsafe { (ext_state.finalize_fn)(ext_state.context, ext_state.state) };
            let value = Value::from_ffi_ref(&final_value);
            if let Some(value_destructor) = ext_state.value_destructor {
                unsafe { value_destructor(&mut final_value) };
            } else {
                unsafe { final_value.__free_internal_type() };
            }
            if let Some(aggregate_destructor) = ext_state.aggregate_destructor {
                unsafe { aggregate_destructor(ext_state.state as usize) };
            }
            value
        } else {
            panic!("AggContext::compute_external() expected External, found {self:?}");
        }
    }

    /// Get a mutable reference to the builtin payload as a slice
    pub fn payload_mut(&mut self) -> &mut [Value] {
        match self {
            Self::Builtin(payload) => payload,
            Self::External(_) => panic!("payload_mut() called on External aggregate"),
        }
    }

    /// Get a mutable reference to the builtin payload Vec (for aggregates that
    /// grow the payload, e.g. array_agg).
    pub fn payload_vec_mut(&mut self) -> &mut Vec<Value> {
        match self {
            Self::Builtin(payload) => payload,
            Self::External(_) => panic!("payload_vec_mut() called on External aggregate"),
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
        match (&mut self, &rhs) {
            (Self::Numeric(_), Self::Numeric(_)) => {
                let sum = (|| {
                    let lhs_num = Numeric::from_value(&self)?;
                    let rhs_num = Numeric::from_value(&rhs)?;
                    lhs_num.checked_add(rhs_num)
                })();
                *self = sum.into();
            }
            (Self::Text(string_left), Self::Text(string_right)) => {
                string_left.value.to_mut().push_str(&string_right.value);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Text(string_left), Self::Numeric(Numeric::Integer(int_right))) => {
                let string_right = int_right.to_string();
                string_left.value.to_mut().push_str(&string_right);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Numeric(Numeric::Integer(int_left)), Self::Text(string_right)) => {
                let string_left = int_left.to_string();
                *self = Self::build_text(string_left + string_right.as_str());
            }
            (Self::Text(string_left), Self::Numeric(Numeric::Float(_))) => {
                let string_right = rhs.to_string();
                string_left.value.to_mut().push_str(&string_right);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Numeric(Numeric::Float(_)), Self::Text(string_right)) => {
                let string_left = self.to_string();
                *self = Self::build_text(string_left + string_right.as_str());
            }
            (_, Self::Null) => {}
            (Self::Null, _) => *self = rhs,
            _ => *self = Self::from_f64(0.0),
        }
    }
}

impl std::ops::AddAssign<i64> for Value {
    fn add_assign(&mut self, rhs: i64) {
        let sum = (|| {
            let lhs_num = Numeric::from_value(&self)?;
            let rhs_num = Numeric::Integer(rhs);
            lhs_num.checked_add(rhs_num)
        })();
        *self = sum.into();
    }
}

impl std::ops::AddAssign<f64> for Value {
    fn add_assign(&mut self, rhs: f64) {
        let sum = (|| {
            let lhs_num = Numeric::from_value(&self)?;
            let rhs_num = NonNan::new(rhs).map(Numeric::Float)?;
            lhs_num.checked_add(rhs_num)
        })();

        *self = sum.into();
    }
}

impl std::ops::Div<Value> for Value {
    type Output = Value;

    fn div(self, rhs: Value) -> Self::Output {
        let div = (|| {
            let lhs_num = Numeric::from_value(self)?;
            let rhs_num = Numeric::from_value(rhs)?;
            lhs_num.checked_div(rhs_num)
        })();
        div.into()
    }
}

impl std::ops::DivAssign<Value> for Value {
    fn div_assign(&mut self, rhs: Value) {
        *self = self.clone() / rhs;
    }
}

impl TryFrom<ValueRef<'_>> for Value {
    type Error = TryReserveError;

    fn try_from(value: ValueRef<'_>) -> std::result::Result<Self, Self::Error> {
        value.to_owned()
    }
}

impl TryFrom<ValueRef<'_>> for i64 {
    type Error = LimboError;

    fn try_from(value: ValueRef<'_>) -> Result<Self, Self::Error> {
        match value {
            ValueRef::Numeric(Numeric::Integer(i)) => Ok(i),
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

impl<'a> ValueRef<'a> {
    pub fn from_f64(f: f64) -> Self {
        match NonNan::new(f) {
            Some(nn) => Self::Numeric(Numeric::Float(nn)),
            None => Self::Null,
        }
    }

    pub fn from_i64(i: i64) -> Self {
        Self::Numeric(Numeric::Integer(i))
    }

    pub fn to_ffi(&self) -> ExtValue {
        match self {
            Self::Null => ExtValue::null(),
            Self::Numeric(Numeric::Integer(i)) => ExtValue::from_integer(*i),
            Self::Numeric(Numeric::Float(fl)) => ExtValue::from_float(f64::from(*fl)),
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
            Self::Numeric(Numeric::Float(f)) => f64::from(*f),
            Self::Numeric(Numeric::Integer(i)) => *i as f64,
            _ => panic!("as_float must be called only for ValueRef::Numeric"),
        }
    }

    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Self::Numeric(Numeric::Integer(i)) => Some(*i),
            _ => None,
        }
    }

    pub const fn as_uint(&self) -> u64 {
        match self {
            Self::Numeric(Numeric::Integer(i)) => (*i).cast_unsigned(),
            _ => 0,
        }
    }

    #[inline]
    pub fn to_owned(&self) -> std::result::Result<Value, TryReserveError> {
        Ok(match self {
            ValueRef::Null => Value::Null,
            ValueRef::Numeric(n) => Value::from(*n),
            ValueRef::Text(text) => Value::Text(Text {
                value: text.value.to_string().into(),
                subtype: text.subtype,
            }),
            ValueRef::Blob(b) => return Value::from_slice(b),
        })
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Numeric(Numeric::Integer(_)) => ValueType::Integer,
            Self::Numeric(Numeric::Float(_)) => ValueType::Float,
            Self::Text(_) => ValueType::Text,
            Self::Blob(_) => ValueType::Blob,
        }
    }
}

impl Display for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Numeric(Numeric::Integer(i)) => write!(f, "{i}"),
            Self::Numeric(Numeric::Float(fl)) => {
                let fval: f64 = (*fl).into();
                write!(f, "{fval:?}")
            }
            Self::Text(s) => write!(f, "{}", s.as_str()),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

impl<'a> PartialEq<ValueRef<'a>> for ValueRef<'a> {
    fn eq(&self, other: &ValueRef<'a>) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Numeric(a), Self::Numeric(b)) => a == b,
            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.as_bytes() == text_right.value.as_bytes()
            }
            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.eq(blob_right),
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

impl<'a> PartialOrd<ValueRef<'a>> for ValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ValueRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => std::cmp::Ordering::Equal,
            (Self::Null, _) => std::cmp::Ordering::Less,
            (_, Self::Null) => std::cmp::Ordering::Greater,

            (Self::Numeric(a), Self::Numeric(b)) => a.cmp(b),

            // Numeric < Text < Blob
            (Self::Numeric(_), _) => std::cmp::Ordering::Less,
            (_, Self::Numeric(_)) => std::cmp::Ordering::Greater,

            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.as_bytes().cmp(text_right.value.as_bytes())
            }
            (Self::Text(_), Self::Blob(_)) => std::cmp::Ordering::Less,
            (Self::Blob(_), Self::Text(_)) => std::cmp::Ordering::Greater,

            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.cmp(blob_right),
        }
    }
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

    pub const fn null() -> Self {
        Self::NULL
    }

    pub const fn i8() -> Self {
        Self::I8
    }

    pub const fn i16() -> Self {
        Self::I16
    }

    pub const fn i24() -> Self {
        Self::I24
    }

    pub const fn i32() -> Self {
        Self::I32
    }

    pub const fn i48() -> Self {
        Self::I48
    }

    pub const fn i64() -> Self {
        Self::I64
    }

    pub const fn f64() -> Self {
        Self::F64
    }

    pub const fn const_int0() -> Self {
        Self::CONST_INT0
    }

    pub const fn const_int1() -> Self {
        Self::CONST_INT1
    }

    pub const fn blob(size: u64) -> Self {
        Self(12 + size * 2)
    }

    pub const fn text(size: u64) -> Self {
        Self(13 + size * 2)
    }

    #[inline(always)]
    pub const fn kind(&self) -> SerialTypeKind {
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
                _ => {
                    mark_unlikely();
                    unreachable!();
                }
            },
            _ => {
                mark_unlikely();
                unreachable!();
            }
        }
    }

    pub const fn size(&self) -> usize {
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
            _ => {
                mark_unlikely();
                unreachable!();
            }
        },
        _ => {
            mark_unlikely();
            Err(LimboError::Corrupt(format!(
                "Invalid serial type: {serial}"
            )))
        }
    }
}

impl<T: AsValueRef> From<T> for SerialType {
    fn from(value: T) -> Self {
        let value = value.as_value_ref();
        match value {
            ValueRef::Null => SerialType::null(),
            ValueRef::Numeric(Numeric::Integer(i)) => match i {
                0 => SerialType::const_int0(),
                1 => SerialType::const_int1(),
                i if (I8_LOW..=I8_HIGH).contains(&i) => SerialType::i8(),
                i if (I16_LOW..=I16_HIGH).contains(&i) => SerialType::i16(),
                i if (I24_LOW..=I24_HIGH).contains(&i) => SerialType::i24(),
                i if (I32_LOW..=I32_HIGH).contains(&i) => SerialType::i32(),
                i if (I48_LOW..=I48_HIGH).contains(&i) => SerialType::i48(),
                _ => SerialType::i64(),
            },
            ValueRef::Numeric(Numeric::Float(_)) => SerialType::f64(),
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
