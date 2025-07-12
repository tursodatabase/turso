#[cfg(feature = "serde")]
use serde::Deserialize;
use turso_ext::{AggCtx, FinalizeFunction, StepFunction};
use turso_sqlite3_parser::ast::SortOrder;

use crate::error::LimboError;
use crate::ext::{ExtValue, ExtValueType};
use crate::pseudo::PseudoCursor;
use crate::schema::Index;
use crate::storage::btree::BTreeCursor;
use crate::storage::sqlite3_ondisk::{read_integer, read_value, read_varint, write_varint};
use crate::translate::collate::CollationSeq;
use crate::translate::plan::IterationDirection;
use crate::vdbe::sorter::Sorter;
use crate::vdbe::Register;
use crate::vtab::VirtualTableCursor;
use crate::Result;
use std::fmt::{Debug, Display};

const MAX_REAL_SIZE: u8 = 15;

/// SQLite by default uses 2000 as maximum numbers in a row.
/// It controlld by the constant called SQLITE_MAX_COLUMN
/// But the hard limit of number of columns is 32,767 columns i16::MAX
const MAX_COLUMN: usize = 2000;

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

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Text {
    pub value: Vec<u8>,
    pub subtype: TextSubtype,
}

impl Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextRef {
    pub value: RawSlice,
    pub subtype: TextSubtype,
}

impl Text {
    pub fn new(value: &str) -> Self {
        Self {
            value: value.as_bytes().to_vec(),
            subtype: TextSubtype::Text,
        }
    }

    #[cfg(feature = "json")]
    pub fn json(value: String) -> Self {
        Self {
            value: value.into_bytes(),
            subtype: TextSubtype::Json,
        }
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.value.as_ref()) }
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
            value: value.as_bytes().to_vec(),
            subtype: TextSubtype::Text,
        }
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Text {
            value: value.into_bytes(),
            subtype: TextSubtype::Text,
        }
    }
}

impl Display for TextRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TextRef {
    pub fn create_from(value: &[u8], subtype: TextSubtype) -> Self {
        let value = RawSlice::create_from(value);
        Self { value, subtype }
    }
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.value.to_slice()) }
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
        Some(result) => Ok(match result {
            crate::numeric::StrToF64::Fractional(non_nan) => non_nan.into(),
            crate::numeric::StrToF64::Decimal(non_nan) => non_nan.into(),
        }),
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

#[derive(Debug, Clone, PartialEq)]
pub struct RawSlice {
    data: *const u8,
    len: usize,
}

#[derive(PartialEq, Clone)]
pub enum RefValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(TextRef),
    Blob(RawSlice),
}

impl Debug for RefValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefValue::Null => write!(f, "Null"),
            RefValue::Integer(i) => f.debug_tuple("Integer").field(i).finish(),
            RefValue::Float(float) => f.debug_tuple("Float").field(float).finish(),
            RefValue::Text(text_ref) => {
                // truncate string to at most 256 chars
                let text = text_ref.as_str();
                let max_len = text.len().min(256);
                f.debug_struct("Text")
                    .field("data", &&text[0..max_len])
                    // Indicates to the developer debugging that the data is truncated for printing
                    .field("truncated", &(text.len() > max_len))
                    .finish()
            }
            RefValue::Blob(raw_slice) => {
                // truncate blob_slice to at most 32 bytes
                let blob = raw_slice.to_slice();
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

impl Value {
    // A helper function that makes building a text Value easier.
    pub fn build_text(text: impl AsRef<str>) -> Self {
        Self::Text(Text::new(text.as_ref()))
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

    pub fn from_text(text: &str) -> Self {
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
            Value::Text(t) => out.extend_from_slice(&t.value),
            Value::Blob(b) => out.extend_from_slice(b),
        };
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalAggState {
    pub state: *mut AggCtx,
    pub argc: usize,
    pub step_fn: StepFunction,
    pub finalize_fn: FinalizeFunction,
    pub finalized_value: Option<Value>,
}

impl ExternalAggState {
    pub fn cache_final_value(&mut self, value: Value) -> &Value {
        self.finalized_value = Some(value);
        self.finalized_value.as_ref().unwrap()
    }
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
            Self::Float(fl) => {
                let fl = *fl;
                if fl == f64::INFINITY {
                    return write!(f, "Inf");
                }
                if fl == f64::NEG_INFINITY {
                    return write!(f, "-Inf");
                }
                if fl.is_nan() {
                    return write!(f, "");
                }
                // handle negative 0
                if fl == -0.0 {
                    return write!(f, "{:.1}", fl.abs());
                }

                // handle scientific notation without trailing zeros
                if (fl.abs() < 1e-4 || fl.abs() >= 1e15) && fl != 0.0 {
                    let sci_notation = format!("{fl:.14e}");
                    let parts: Vec<&str> = sci_notation.split('e').collect();

                    if parts.len() == 2 {
                        let mantissa = parts[0];
                        let exponent = parts[1];

                        let decimal_parts: Vec<&str> = mantissa.split('.').collect();
                        if decimal_parts.len() == 2 {
                            let whole = decimal_parts[0];
                            // 1.{this part}
                            let mut fraction = String::from(decimal_parts[1]);

                            //removing trailing 0 from fraction
                            while fraction.ends_with('0') {
                                fraction.pop();
                            }

                            let trimmed_mantissa = if fraction.is_empty() {
                                whole.to_string()
                            } else {
                                format!("{whole}.{fraction}")
                            };
                            let (prefix, exponent) =
                                if let Some(stripped_exponent) = exponent.strip_prefix('-') {
                                    ("-0", &stripped_exponent[1..])
                                } else {
                                    ("+", exponent)
                                };
                            return write!(f, "{trimmed_mantissa}e{prefix}{exponent}");
                        }
                    }

                    // fallback
                    return write!(f, "{sci_notation}");
                }

                // handle floating point max size is 15.
                // If left > right && right + left > 15 go to sci notation
                // If right > left && right + left > 15 truncate left so right + left == 15
                let rounded = fl.round();
                if (fl - rounded).abs() < 1e-14 {
                    // if we very close to integer trim decimal part to 1 digit
                    if rounded == rounded as i64 as f64 {
                        return write!(f, "{fl:.1}");
                    }
                }

                let fl_str = format!("{fl}");
                let splitted = fl_str.split('.').collect::<Vec<&str>>();
                // fallback
                if splitted.len() != 2 {
                    return write!(f, "{fl:.14e}");
                }

                let first_part = if fl < 0.0 {
                    // remove -
                    &splitted[0][1..]
                } else {
                    splitted[0]
                };

                let second = splitted[1];

                // We want more precision for smaller numbers. in SQLite case we want 15 non zero digits in 0 < number < 1
                // leading zeroes added to max real size. But if float < 1e-4 we go to scientific notation
                let leading_zeros = second.chars().take_while(|c| c == &'0').count();
                let reminder = if first_part != "0" {
                    MAX_REAL_SIZE as isize - first_part.len() as isize
                } else {
                    MAX_REAL_SIZE as isize + leading_zeros as isize
                };
                // float that have integer part > 15 converted to sci notation
                if reminder < 0 {
                    return write!(f, "{fl:.14e}");
                }
                // trim decimal part to reminder or self len so total digits is 15;
                let mut fl = format!("{:.*}", second.len().min(reminder as usize), fl);
                // if decimal part ends with 0 we trim it
                while fl.ends_with('0') {
                    fl.pop();
                }
                write!(f, "{fl}")
            }
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
                Ok(Value::build_text(text))
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

#[derive(Debug, Clone, PartialEq)]
pub enum AggContext {
    Avg(Value, Value), // acc and count
    Sum(Value),
    Count(Value),
    Max(Option<Value>),
    Min(Option<Value>),
    GroupConcat(Value),
    External(ExternalAggState),
}

const NULL: Value = Value::Null;

impl AggContext {
    pub fn compute_external(&mut self) -> Result<()> {
        if let Self::External(ext_state) = self {
            if ext_state.finalized_value.is_none() {
                let final_value = unsafe { (ext_state.finalize_fn)(ext_state.state) };
                ext_state.cache_final_value(Value::from_ffi(final_value)?);
            }
        }
        Ok(())
    }

    pub fn final_value(&self) -> &Value {
        match self {
            Self::Avg(acc, _count) => acc,
            Self::Sum(acc) => acc,
            Self::Count(count) => count,
            Self::Max(max) => max.as_ref().unwrap_or(&NULL),
            Self::Min(min) => min.as_ref().unwrap_or(&NULL),
            Self::GroupConcat(s) => s,
            Self::External(ext_state) => ext_state.finalized_value.as_ref().unwrap_or(&NULL),
        }
    }
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Self::Integer(int_left), Self::Integer(int_right)) => int_left == int_right,
            (Self::Integer(int_left), Self::Float(float_right)) => {
                (*int_left as f64) == (*float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                float_left == (&(*int_right as f64))
            }
            (Self::Float(float_left), Self::Float(float_right)) => float_left == float_right,
            (Self::Integer(_) | Self::Float(_), Self::Text(_) | Self::Blob(_)) => false,
            (Self::Text(_) | Self::Blob(_), Self::Integer(_) | Self::Float(_)) => false,
            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.eq(&text_right.value)
            }
            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.eq(blob_right),
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd<Value> for Value {
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

            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.partial_cmp(&text_right.value)
            }
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

impl PartialOrd<AggContext> for AggContext {
    fn partial_cmp(&self, other: &AggContext) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Avg(a, _), Self::Avg(b, _)) => a.partial_cmp(b),
            (Self::Sum(a), Self::Sum(b)) => a.partial_cmp(b),
            (Self::Count(a), Self::Count(b)) => a.partial_cmp(b),
            (Self::Max(a), Self::Max(b)) => a.partial_cmp(b),
            (Self::Min(a), Self::Min(b)) => a.partial_cmp(b),
            (Self::GroupConcat(a), Self::GroupConcat(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::ops::Add<Value> for Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Integer(int_left), Self::Integer(int_right)) => {
                Self::Integer(int_left + int_right)
            }
            (Self::Integer(int_left), Self::Float(float_right)) => {
                Self::Float(int_left as f64 + float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                Self::Float(float_left + int_right as f64)
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                Self::Float(float_left + float_right)
            }
            (Self::Text(string_left), Self::Text(string_right)) => {
                Self::build_text(&(string_left.as_str().to_string() + string_right.as_str()))
            }
            (Self::Text(string_left), Self::Integer(int_right)) => {
                Self::build_text(&(string_left.as_str().to_string() + &int_right.to_string()))
            }
            (Self::Integer(int_left), Self::Text(string_right)) => {
                Self::build_text(&(int_left.to_string() + string_right.as_str()))
            }
            (Self::Text(string_left), Self::Float(float_right)) => {
                let string_right = Self::Float(float_right).to_string();
                Self::build_text(&(string_left.as_str().to_string() + &string_right))
            }
            (Self::Float(float_left), Self::Text(string_right)) => {
                let string_left = Self::Float(float_left).to_string();
                Self::build_text(&(string_left + string_right.as_str()))
            }
            (lhs, Self::Null) => lhs,
            (Self::Null, rhs) => rhs,
            _ => Self::Float(0.0),
        }
    }
}

impl std::ops::Add<f64> for Value {
    type Output = Value;

    fn add(self, rhs: f64) -> Self::Output {
        match self {
            Self::Integer(int_left) => Self::Float(int_left as f64 + rhs),
            Self::Float(float_left) => Self::Float(float_left + rhs),
            _ => unreachable!(),
        }
    }
}

impl std::ops::Add<i64> for Value {
    type Output = Value;

    fn add(self, rhs: i64) -> Self::Output {
        match self {
            Self::Integer(int_left) => Self::Integer(int_left + rhs),
            Self::Float(float_left) => Self::Float(float_left + rhs as f64),
            _ => unreachable!(),
        }
    }
}

impl std::ops::AddAssign for Value {
    fn add_assign(&mut self, rhs: Self) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<i64> for Value {
    fn add_assign(&mut self, rhs: i64) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<f64> for Value {
    fn add_assign(&mut self, rhs: f64) {
        *self = self.clone() + rhs;
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

impl<'a> TryFrom<&'a RefValue> for i64 {
    type Error = LimboError;

    fn try_from(value: &'a RefValue) -> Result<Self, Self::Error> {
        match value {
            RefValue::Integer(i) => Ok(*i),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl<'a> TryFrom<&'a RefValue> for String {
    type Error = LimboError;

    fn try_from(value: &'a RefValue) -> Result<Self, Self::Error> {
        match value {
            RefValue::Text(s) => Ok(s.as_str().to_string()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

impl<'a> TryFrom<&'a RefValue> for &'a str {
    type Error = LimboError;

    fn try_from(value: &'a RefValue) -> Result<Self, Self::Error> {
        match value {
            RefValue::Text(s) => Ok(s.as_str()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

/// This struct serves the purpose of not allocating multiple vectors of bytes if not needed.
/// A value in a record that has already been serialized can stay serialized and what this struct offsers
/// is easy acces to each value which point to the payload.
/// The name might be contradictory as it is immutable in the sense that you cannot modify the values without modifying the payload.
#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ImmutableRecord {
    // We have to be super careful with this buffer since we make values point to the payload we need to take care reallocations
    // happen in a controlled manner. If we realocate with values that should be correct, they will now point to undefined data.
    // We don't use pin here because it would make it imposible to reuse the buffer if we need to push a new record in the same struct.
    //
    // payload is the Vec<u8> but in order to use Register which holds ImmutableRecord as a Value - we store Vec<u8> as Value::Blob
    payload: Value,
}

#[derive(PartialEq)]
pub enum ParseRecordState {
    Init,
    Parsing { payload: Vec<u8> },
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

    // TODO: inline the complete record parsing code here.
    // Its probably more efficient.
    pub fn get_values(&self) -> Vec<RefValue> {
        let mut cursor = RecordCursor::new();
        cursor.get_values(self).unwrap_or_default()
    }

    pub fn from_registers<'a, I: Iterator<Item = &'a Register> + Clone>(
        // we need to accept both &[Register] and &[&Register] values - that's why non-trivial signature
        //
        // std::slice::Iter under the hood just stores pointer and length of slice and also implements a Clone which just copy those meta-values
        // (without copying the data itself)
        registers: impl IntoIterator<Item = &'a Register, IntoIter = I>,
        len: usize,
    ) -> Self {
        Self::from_values(registers.into_iter().map(|x| x.get_owned_value()), len)
    }

    pub fn from_values<'a>(
        values: impl IntoIterator<Item = &'a Value> + Clone,
        len: usize,
    ) -> Self {
        let mut ref_values = Vec::with_capacity(len);
        let mut serials = Vec::with_capacity(len);
        let mut size_header = 0;
        let mut size_values = 0;

        let mut serial_type_buf = [0; 9];
        // write serial types
        for value in values.clone() {
            let serial_type = SerialType::from(value);
            let n = write_varint(&mut serial_type_buf[0..], serial_type.into());
            serials.push((serial_type_buf, n));

            let value_size = serial_type.size();

            size_header += n;
            size_values += value_size;
        }

        let mut header_size = size_header;
        const MIN_HEADER_SIZE: usize = 126;
        if header_size <= MIN_HEADER_SIZE {
            // common case
            // This case means the header size can be contained by a single byte, therefore
            // header_size == size of serial types + 1 byte from the header size
            // Since header_size is a varint, and a varint the first bit is used to represent we have more bytes to read,
            // header size here will be 126 == (2^7 - 1)
            header_size += 1;
        } else {
            // Rare case of a really large header
            let mut temp_buf = [0u8; 9];
            let n_varint = write_varint(&mut temp_buf, header_size as u64); // or however you get varint length
            header_size += n_varint;

            // Check if adding the varint bytes changes the varint length
            let new_n_varint = write_varint(&mut temp_buf, header_size as u64);
            if n_varint < new_n_varint {
                header_size += 1;
            }
        }

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
            let start_offset = writer.pos;
            match value {
                Value::Null => {
                    ref_values.push(RefValue::Null);
                }
                Value::Integer(i) => {
                    ref_values.push(RefValue::Integer(*i));
                    let serial_type = SerialType::from(value);
                    match serial_type.kind() {
                        SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 => {}
                        SerialTypeKind::I8 => writer.extend_from_slice(&(*i as i8).to_be_bytes()),
                        SerialTypeKind::I16 => writer.extend_from_slice(&(*i as i16).to_be_bytes()),
                        SerialTypeKind::I24 => {
                            writer.extend_from_slice(&(*i as i32).to_be_bytes()[1..])
                        } // remove most significant byte
                        SerialTypeKind::I32 => writer.extend_from_slice(&(*i as i32).to_be_bytes()),
                        SerialTypeKind::I48 => writer.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialTypeKind::I64 => writer.extend_from_slice(&i.to_be_bytes()),
                        other => panic!("Serial type is not an integer: {other:?}"),
                    }
                }
                Value::Float(f) => {
                    ref_values.push(RefValue::Float(*f));
                    writer.extend_from_slice(&f.to_be_bytes())
                }
                Value::Text(t) => {
                    writer.extend_from_slice(&t.value);
                    let end_offset = writer.pos;
                    let len = end_offset - start_offset;
                    let ptr = unsafe { writer.buf.as_ptr().add(start_offset) };
                    let value = RefValue::Text(TextRef {
                        value: RawSlice::new(ptr, len),
                        subtype: t.subtype,
                    });
                    ref_values.push(value);
                }
                Value::Blob(b) => {
                    writer.extend_from_slice(b);
                    let end_offset = writer.pos;
                    let len = end_offset - start_offset;
                    let ptr = unsafe { writer.buf.as_ptr().add(start_offset) };
                    ref_values.push(RefValue::Blob(RawSlice::new(ptr, len)));
                }
            };
        }

        writer.assert_finish_capacity();
        Self {
            payload: Value::Blob(buf),
        }
    }

    pub fn as_blob(&self) -> &Vec<u8> {
        match &self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    pub fn as_blob_mut(&mut self) -> &mut Vec<u8> {
        match &mut self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    pub fn as_blob_value(&self) -> &Value {
        &self.payload
    }

    pub fn start_serialization(&mut self, payload: &[u8]) {
        self.as_blob_mut().extend_from_slice(payload);
    }

    pub fn invalidate(&mut self) {
        self.as_blob_mut().clear();
    }

    pub fn is_invalidated(&self) -> bool {
        self.as_blob().is_empty()
    }

    pub fn get_payload(&self) -> &[u8] {
        self.as_blob()
    }

    // TODO: its probably better to not instantiate the RecordCurosr. Instead do the deserialization
    // inside the function.
    pub fn last_value(&self, record_cursor: &mut RecordCursor) -> Option<Result<RefValue>> {
        if self.is_invalidated() {
            return Some(Err(LimboError::InternalError(
                "Record is invalidated".into(),
            )));
        }
        record_cursor.parse_full_header(self).unwrap();
        let last_idx = record_cursor.serial_types.len().checked_sub(1)?;
        Some(record_cursor.get_value(self, last_idx))
    }

    pub fn get_value(&self, idx: usize) -> Result<RefValue> {
        let mut cursor = RecordCursor::new();
        cursor.get_value(self, idx)
    }

    pub fn get_value_opt(&self, idx: usize) -> Option<RefValue> {
        if self.is_invalidated() {
            return None;
        }

        let mut cursor = RecordCursor::new();

        match cursor.ensure_parsed_upto(self, idx) {
            Ok(()) => {
                if idx >= cursor.serial_types.len() {
                    return None;
                }

                match cursor.deserialize_column(self, idx) {
                    Ok(value) => Some(value),
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }
}

/// A cursor for lazily parsing SQLite record format data.
///
/// `RecordCursor` provides incremental parsing of SQLite records, which follow the format:
/// `[header_size][serial_type1][serial_type2]...[data1][data2]...`
///
/// Instead of parsing the entire record upfront, this cursor parses only what's needed
/// for the requested operations, improving performance for large records where only
/// a few columns are accessed.
///
/// SQLite records consist of:
/// - **Header size**: Varint indicating total header length
/// - **Serial types**: Variable-length integers describing each field's type and size
/// - **Data section**: The actual field data in the same order as serial types
#[derive(Debug, Default)]
pub struct RecordCursor {
    /// Parsed serial type values for each column.
    /// Serial types encode both the data type and size information.
    pub serial_types: Vec<u64>,
    /// Byte offsets where each column's data begins in the record payload.
    /// Always has one more entry than `serial_types` (the final offset marks the end).
    pub offsets: Vec<usize>,
    /// Total size of the record header in bytes.
    pub header_size: usize,
    /// Current parsing position within the header section.
    pub header_offset: usize,
}

impl RecordCursor {
    pub fn new() -> Self {
        Self {
            serial_types: Vec::new(),
            offsets: Vec::new(),
            header_size: 0,
            header_offset: 0,
        }
    }

    pub fn with_capacity(num_columns: usize) -> Self {
        Self {
            serial_types: Vec::with_capacity(num_columns),
            offsets: Vec::with_capacity(num_columns + 1),
            header_size: 0,
            header_offset: 0,
        }
    }

    pub fn invalidate(&mut self) {
        self.serial_types.clear();
        self.offsets.clear();
        self.header_size = 0;
        self.header_offset = 0;
    }

    pub fn is_invalidated(&self) -> bool {
        self.serial_types.is_empty() && self.offsets.is_empty()
    }

    pub fn parse_full_header(&mut self, record: &ImmutableRecord) -> Result<()> {
        self.ensure_parsed_upto(record, MAX_COLUMN)
    }

    /// Ensures the header is parsed up to (and including) the target column index.
    ///
    /// This is the core lazy parsing method. It only parses as much of the header
    /// as needed to access the requested column, making it efficient for sparse
    /// column access patterns.
    ///
    /// # Arguments
    ///
    /// * `record` - The record containing the data to parse
    /// * `target_idx` - The column index that needs to be accessible (0-based)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Parsing completed successfully
    /// * `Err(LimboError)` - Parsing failed due to corrupt data or I/O error
    ///
    /// # Behavior
    ///
    /// - If `target_idx` is already parsed, returns immediately
    /// - Parses incrementally from the current position to the target
    /// - Handles the initial header size parsing on first call
    /// - Calculates and caches data offsets for each parsed column
    ///
    #[inline(always)]
    pub fn ensure_parsed_upto(
        &mut self,
        record: &ImmutableRecord,
        target_idx: usize,
    ) -> Result<()> {
        let payload = record.get_payload();
        if payload.is_empty() {
            return Ok(());
        }

        // Parse header size and initialize parsing
        if self.serial_types.is_empty() && self.offsets.is_empty() {
            let (header_size, bytes_read) = read_varint(payload)?;
            self.header_size = header_size as usize;
            self.header_offset = bytes_read;
            self.offsets.push(self.header_size); // First column starts after header
        }

        // Parse serial types incrementally
        while self.serial_types.len() <= target_idx
            && self.header_offset < self.header_size
            && self.header_offset < payload.len()
        {
            let (serial_type, read_bytes) = read_varint(&payload[self.header_offset..])?;
            self.serial_types.push(serial_type);
            self.header_offset += read_bytes;

            let serial_type_obj = SerialType::try_from(serial_type)?;
            let data_size = serial_type_obj.size();
            let prev_offset = *self.offsets.last().unwrap();
            self.offsets.push(prev_offset + data_size);
        }

        Ok(())
    }

    /// Deserializes a specific column without additional parsing.
    ///
    /// This method assumes the header has already been parsed up to the target
    /// column index (via `ensure_parsed_upto`). It extracts the actual data
    /// value from the record's data section.
    ///
    /// # Arguments
    ///
    /// * `record` - The record containing the data
    /// * `idx` - The column index to deserialize (0-based)
    ///
    /// # Returns
    ///
    /// * `Ok(RefValue)` - The deserialized value (may reference record data)
    /// * `Err(LimboError)` - Deserialization failed
    ///
    /// # Special Cases
    ///
    /// - Returns `RefValue::Null` for out-of-bounds indices
    pub fn deserialize_column(&self, record: &ImmutableRecord, idx: usize) -> Result<RefValue> {
        if idx >= self.serial_types.len() {
            return Ok(RefValue::Null);
        }

        let serial_type = self.serial_types[idx];
        let serial_type_obj = SerialType::try_from(serial_type)?;

        match serial_type_obj.kind() {
            SerialTypeKind::Null => return Ok(RefValue::Null),
            SerialTypeKind::ConstInt0 => return Ok(RefValue::Integer(0)),
            SerialTypeKind::ConstInt1 => return Ok(RefValue::Integer(1)),
            _ => {} // continue
        }

        if idx + 1 >= self.offsets.len() {
            return Ok(RefValue::Null);
        }

        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        let payload = record.get_payload();

        let slice = &payload[start..end];
        let (value, _) = crate::storage::sqlite3_ondisk::read_value(slice, serial_type_obj)?;
        Ok(value)
    }

    /// Gets the value at the specified column index.
    ///
    /// This is the primary method for accessing record data. It combines
    /// lazy parsing with deserialization in a single call.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to read from
    /// * `idx` - The column index (0-based)
    ///
    /// # Returns
    ///
    /// * `Ok(RefValue)` - The value at the specified index
    /// * `Err(LimboError)` - Access failed due to invalid record or parsing error
    ///
    #[inline(always)]
    pub fn get_value(&mut self, record: &ImmutableRecord, idx: usize) -> Result<RefValue> {
        if record.is_invalidated() {
            return Err(LimboError::InternalError("Record not initialized".into()));
        }

        self.ensure_parsed_upto(record, idx)?;
        self.deserialize_column(record, idx)
    }

    /// Gets the value at the specified column index, returning `None` on any error.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to read from
    /// * `idx` - The column index (0-based)
    ///
    /// # Returns
    ///
    /// * `Some(Ok(RefValue))` - Successfully read value
    /// * `Some(Err(LimboError))` - Parsing succeeded but deserialization failed
    /// * `None` - Record is invalid or index is out of bounds
    ///
    pub fn get_value_opt(
        &mut self,
        record: &ImmutableRecord,
        idx: usize,
    ) -> Option<Result<RefValue>> {
        if record.is_invalidated() {
            return None;
        }

        if let Err(e) = self.ensure_parsed_upto(record, idx) {
            return Some(Err(e));
        }

        Some(self.deserialize_column(record, idx))
    }

    /// Returns the number of columns in the record.
    ///
    /// This method parses the complete header to determine the total
    /// column count. The result is cached for subsequent calls.
    /// # Arguments
    ///
    /// * `record` - The record to count columns in
    ///
    /// # Returns
    ///
    /// The number of columns, or 0 if the record is invalid.
    pub fn count(&mut self, record: &ImmutableRecord) -> usize {
        if record.is_invalidated() {
            return 0;
        }

        let _ = self.parse_full_header(record);
        self.serial_types.len()
    }

    /// Alias for `count()`. Returns the number of columns in the record.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to get length of
    ///
    /// # Returns
    ///
    /// The number of columns, or 0 if the record is invalid.
    pub fn len(&mut self, record: &ImmutableRecord) -> usize {
        self.count(record)
    }

    /// Returns all values in the record as a vector.
    ///
    /// This method parses the complete header and deserializes all columns.
    /// Use this when you need access to most or all columns in the record.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to extract all values from
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<RefValue>)` - All values in column order
    /// * `Err(LimboError)` - Parsing or deserialization failed
    ///
    pub fn get_values(&mut self, record: &ImmutableRecord) -> Result<Vec<RefValue>> {
        if record.is_invalidated() {
            return Ok(Vec::new());
        }

        self.parse_full_header(record)?;
        let mut result = Vec::with_capacity(self.serial_types.len());

        for i in 0..self.serial_types.len() {
            result.push(self.deserialize_column(record, i)?);
        }

        Ok(result)
    }
}

impl RefValue {
    pub fn to_ffi(&self) -> ExtValue {
        match self {
            Self::Null => ExtValue::null(),
            Self::Integer(i) => ExtValue::from_integer(*i),
            Self::Float(fl) => ExtValue::from_float(*fl),
            Self::Text(text) => ExtValue::from_text(
                std::str::from_utf8(text.value.to_slice())
                    .unwrap()
                    .to_string(),
            ),
            Self::Blob(blob) => ExtValue::from_blob(blob.to_slice().to_vec()),
        }
    }

    pub fn to_owned(&self) -> Value {
        match self {
            RefValue::Null => Value::Null,
            RefValue::Integer(i) => Value::Integer(*i),
            RefValue::Float(f) => Value::Float(*f),
            RefValue::Text(text_ref) => Value::Text(Text {
                value: text_ref.value.to_slice().to_vec(),
                subtype: text_ref.subtype,
            }),
            RefValue::Blob(b) => Value::Blob(b.to_slice().to_vec()),
        }
    }
    pub fn to_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(blob) => Some(blob.to_slice()),
            _ => None,
        }
    }
}

impl Display for RefValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Float(fl) => write!(f, "{fl:?}"),
            Self::Text(s) => write!(f, "{}", s.as_str()),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b.to_slice())),
        }
    }
}
impl Eq for RefValue {}

impl Ord for RefValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd<RefValue> for RefValue {
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
                .to_slice()
                .partial_cmp(text_right.value.to_slice()),
            // Text vs Blob
            (Self::Text(_), Self::Blob(_)) => Some(std::cmp::Ordering::Less),
            (Self::Blob(_), Self::Text(_)) => Some(std::cmp::Ordering::Greater),

            (Self::Blob(blob_left), Self::Blob(blob_right)) => {
                blob_left.to_slice().partial_cmp(blob_right.to_slice())
            }
            (Self::Null, Self::Null) => Some(std::cmp::Ordering::Equal),
            (Self::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Self::Null) => Some(std::cmp::Ordering::Greater),
        }
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

/// A bitfield that represents the comparison spec for index keys.
/// Since indexed columns can individually specify ASC/DESC, each key must
/// be compared differently.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct IndexKeySortOrder(pub u64);

impl IndexKeySortOrder {
    pub fn get_sort_order_for_col(&self, column_idx: usize) -> SortOrder {
        assert!(column_idx < 64, "column index out of range: {column_idx}");
        match self.0 & (1 << column_idx) {
            0 => SortOrder::Asc,
            _ => SortOrder::Desc,
        }
    }

    pub fn from_index(index: &Index) -> Self {
        let mut spec = 0;
        for (i, column) in index.columns.iter().enumerate() {
            spec |= ((column.order == SortOrder::Desc) as u64) << i;
        }
        IndexKeySortOrder(spec)
    }

    pub fn from_list(order: &[SortOrder]) -> Self {
        let mut spec = 0;
        for (i, order) in order.iter().enumerate() {
            spec |= ((*order == SortOrder::Desc) as u64) << i;
        }
        IndexKeySortOrder(spec)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
/// Metadata about an index, used for handling and comparing index keys.
///
/// This struct provides information about the sorting order of columns,
/// whether the index includes a row ID, and the total number of columns
/// in the index.
pub struct IndexKeyInfo {
    /// Specifies the sorting order (ascending or descending) for each column in the index.
    pub sort_order: IndexKeySortOrder,
    /// Indicates whether the index includes a row ID column.
    pub has_rowid: bool,
    /// The total number of columns in the index, including the row ID column if present.
    pub num_cols: usize,
}

impl Default for IndexKeyInfo {
    fn default() -> Self {
        Self {
            sort_order: IndexKeySortOrder::default(),
            has_rowid: true,
            num_cols: 1,
        }
    }
}

impl IndexKeyInfo {
    pub fn new_from_index(index: &Index) -> Self {
        Self {
            sort_order: IndexKeySortOrder::from_index(index),
            has_rowid: index.has_rowid,
            num_cols: index.columns.len() + (index.has_rowid as usize),
        }
    }
}

pub fn compare_immutable(
    l: &[RefValue],
    r: &[RefValue],
    index_key_sort_order: IndexKeySortOrder,
    collations: &[CollationSeq],
) -> std::cmp::Ordering {
    assert_eq!(l.len(), r.len());
    for (i, (l, r)) in l.iter().zip(r).enumerate() {
        let column_order = index_key_sort_order.get_sort_order_for_col(i);
        let collation = collations.get(i).copied().unwrap_or_default();
        let cmp = match (l, r) {
            (RefValue::Text(left), RefValue::Text(right)) => {
                collation.compare_strings(left.as_str(), right.as_str())
            }
            _ => l.partial_cmp(r).unwrap(),
        };
        if !cmp.is_eq() {
            return match column_order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            };
        }
    }
    std::cmp::Ordering::Equal
}

#[derive(Debug, Clone, Copy)]
pub enum RecordCompare {
    Int,
    String,
    Generic,
}

impl RecordCompare {
    pub fn compare(
        &self,
        serialized: &ImmutableRecord,
        unpacked: &[RefValue],
        index_info: &IndexKeyInfo,
        collations: &[CollationSeq],
        skip: usize,
        tie_breaker: std::cmp::Ordering,
    ) -> Result<std::cmp::Ordering> {
        match self {
            RecordCompare::Int => {
                compare_records_int(serialized, unpacked, index_info, collations, tie_breaker)
            }
            RecordCompare::String => {
                compare_records_string(serialized, unpacked, index_info, collations, tie_breaker)
            }
            RecordCompare::Generic => compare_records_generic(
                serialized,
                unpacked,
                index_info,
                collations,
                skip,
                tie_breaker,
            ),
        }
    }
}

pub fn find_compare(
    unpacked: &[RefValue],
    index_info: &IndexKeyInfo,
    collations: &[CollationSeq],
) -> RecordCompare {
    if !unpacked.is_empty() && index_info.num_cols <= 13 {
        match &unpacked[0] {
            RefValue::Integer(_) => RecordCompare::Int,
            RefValue::Text(_) if is_binary_collation(collations, 0) => RecordCompare::String,
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
/// - Header size ≤ 63 bytes (`payload[0] <= 63`) - safety constraint
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
fn compare_records_int(
    serialized: &ImmutableRecord,
    unpacked: &[RefValue],
    index_info: &IndexKeyInfo,
    collations: &[CollationSeq],
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering> {
    let payload = serialized.get_payload();
    if payload.len() < 2 || payload[0] > 63 {
        return compare_records_generic(
            serialized,
            unpacked,
            index_info,
            collations,
            0,
            tie_breaker,
        );
    }

    let header_size = payload[0] as usize;
    let first_serial_type = payload[1];

    if !matches!(first_serial_type, 1..=6 | 8 | 9) {
        return compare_records_generic(
            serialized,
            unpacked,
            index_info,
            collations,
            0,
            tie_breaker,
        );
    }

    let data_start = header_size;

    let lhs_int = read_integer(&payload[data_start..], first_serial_type)?;
    let RefValue::Integer(rhs_int) = unpacked[0] else {
        return compare_records_generic(
            serialized,
            unpacked,
            index_info,
            collations,
            0,
            tie_breaker,
        );
    };
    let comparison = match index_info.sort_order.get_sort_order_for_col(0) {
        SortOrder::Asc => lhs_int.cmp(&rhs_int),
        SortOrder::Desc => lhs_int.cmp(&rhs_int).reverse(),
    };
    match comparison {
        std::cmp::Ordering::Equal => {
            // First fields equal, compare remaining fields if any
            if unpacked.len() > 1 {
                return compare_records_generic(
                    serialized,
                    unpacked,
                    index_info,
                    collations,
                    1,
                    tie_breaker,
                );
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
fn compare_records_string(
    serialized: &ImmutableRecord,
    unpacked: &[RefValue],
    index_info: &IndexKeyInfo,
    collations: &[CollationSeq],
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering> {
    let payload = serialized.get_payload();
    if payload.len() < 2 {
        return compare_records_generic(
            serialized,
            unpacked,
            index_info,
            collations,
            0,
            tie_breaker,
        );
    }

    let header_size = payload[0] as usize;
    let first_serial_type = payload[1];

    // Check if serial type is not a string or if its a blob
    if first_serial_type < 13 || (first_serial_type & 1) == 0 {
        return compare_records_generic(
            serialized,
            unpacked,
            index_info,
            collations,
            0,
            tie_breaker,
        );
    }

    let RefValue::Text(rhs_text) = &unpacked[0] else {
        return compare_records_generic(
            serialized,
            unpacked,
            index_info,
            collations,
            0,
            tie_breaker,
        );
    };

    let string_len = (first_serial_type as usize - 13) / 2;
    let data_start = header_size;

    debug_assert!(data_start + string_len <= payload.len());

    let serial_type = SerialType::try_from(first_serial_type as u64)?;
    let (lhs_value, _) = read_value(&payload[data_start..], serial_type)?;

    let RefValue::Text(lhs_text) = lhs_value else {
        return compare_records_generic(
            serialized,
            unpacked,
            index_info,
            collations,
            0,
            tie_breaker,
        );
    };

    let comparison = if let Some(collation) = collations.first() {
        collation.compare_strings(lhs_text.as_str(), rhs_text.as_str())
    } else {
        // No collation case
        lhs_text.value.to_slice().cmp(rhs_text.value.to_slice())
    };

    let final_comparison = match index_info.sort_order.get_sort_order_for_col(0) {
        SortOrder::Asc => comparison,
        SortOrder::Desc => comparison.reverse(),
    };

    match final_comparison {
        std::cmp::Ordering::Equal => {
            let len_cmp = lhs_text.value.len.cmp(&rhs_text.value.len);
            if len_cmp != std::cmp::Ordering::Equal {
                let adjusted = match index_info.sort_order.get_sort_order_for_col(0) {
                    SortOrder::Asc => len_cmp,
                    SortOrder::Desc => len_cmp.reverse(),
                };
                return Ok(adjusted);
            }

            if unpacked.len() > 1 {
                return compare_records_generic(
                    serialized,
                    unpacked,
                    index_info,
                    collations,
                    1,
                    tie_breaker,
                );
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
pub fn compare_records_generic(
    serialized: &ImmutableRecord,
    unpacked: &[RefValue],
    index_info: &IndexKeyInfo,
    collations: &[CollationSeq],
    skip: usize,
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering> {
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
    while field_idx < unpacked.len() && header_pos < header_end {
        let (serial_type_raw, bytes_read) = read_varint(&payload[header_pos..])?;
        header_pos += bytes_read;

        let serial_type = SerialType::try_from(serial_type_raw)?;
        let rhs_value = &unpacked[field_idx];

        let lhs_value = match serial_type.kind() {
            SerialTypeKind::ConstInt0 => RefValue::Integer(0),
            SerialTypeKind::ConstInt1 => RefValue::Integer(1),
            SerialTypeKind::Null => RefValue::Null,
            _ => {
                let (value, field_size) = read_value(&payload[data_pos..], serial_type)?;
                data_pos += field_size;
                value
            }
        };

        let comparison = match (&lhs_value, rhs_value) {
            (RefValue::Text(lhs_text), RefValue::Text(rhs_text)) => {
                if let Some(collation) = collations.get(field_idx) {
                    collation.compare_strings(lhs_text.as_str(), rhs_text.as_str())
                } else {
                    lhs_text.value.to_slice().cmp(rhs_text.value.to_slice())
                }
            }

            (RefValue::Integer(lhs_int), RefValue::Float(rhs_float)) => {
                sqlite_int_float_compare(*lhs_int, *rhs_float)
            }

            (RefValue::Float(lhs_float), RefValue::Integer(rhs_int)) => {
                sqlite_int_float_compare(*rhs_int, *lhs_float).reverse()
            }

            _ => lhs_value.partial_cmp(rhs_value).unwrap(),
        };

        let final_comparison = match index_info.sort_order.get_sort_order_for_col(field_idx) {
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

#[inline(always)]
fn is_binary_collation(collations: &[CollationSeq], col_idx: usize) -> bool {
    collations[col_idx] == CollationSeq::Binary
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

impl From<&Value> for SerialType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => SerialType::null(),
            Value::Integer(i) => match i {
                0 => SerialType::const_int0(),
                1 => SerialType::const_int1(),
                i if *i >= I8_LOW && *i <= I8_HIGH => SerialType::i8(),
                i if *i >= I16_LOW && *i <= I16_HIGH => SerialType::i16(),
                i if *i >= I24_LOW && *i <= I24_HIGH => SerialType::i24(),
                i if *i >= I32_LOW && *i <= I32_HIGH => SerialType::i32(),
                i if *i >= I48_LOW && *i <= I48_HIGH => SerialType::i48(),
                _ => SerialType::i64(),
            },
            Value::Float(_) => SerialType::f64(),
            Value::Text(t) => SerialType::text(t.value.len() as u64),
            Value::Blob(b) => SerialType::blob(b.len() as u64),
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

    fn try_from(uint: u64) -> Result<Self> {
        if uint == 10 || uint == 11 {
            return Err(LimboError::Corrupt(format!("Invalid serial type: {uint}")));
        }
        Ok(SerialType(uint))
    }
}

impl Record {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
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
                Value::Text(t) => buf.extend_from_slice(&t.value),
                Value::Blob(b) => buf.extend_from_slice(b),
            };
        }

        let mut header_bytes_buf: Vec<u8> = Vec::new();
        if header_size <= 126 {
            // common case
            header_size += 1;
        } else {
            todo!("calculate big header size extra bytes");
            // get header varint len
            // header_size += n;
            // if( nVarint<sqlite3VarintLen(nHdr) ) nHdr++;
        }
        assert!(header_size <= 126);
        header_bytes_buf.extend(std::iter::repeat_n(0, 9));
        let n = write_varint(header_bytes_buf.as_mut_slice(), header_size as u64);
        header_bytes_buf.truncate(n);
        buf.splice(initial_i..initial_i, header_bytes_buf.iter().cloned());
    }
}

pub enum Cursor {
    BTree(Box<BTreeCursor>),
    Pseudo(PseudoCursor),
    Sorter(Sorter),
    Virtual(VirtualTableCursor),
}

impl Cursor {
    pub fn new_btree(cursor: BTreeCursor) -> Self {
        Self::BTree(Box::new(cursor))
    }

    pub fn new_pseudo(cursor: PseudoCursor) -> Self {
        Self::Pseudo(cursor)
    }

    pub fn new_sorter(cursor: Sorter) -> Self {
        Self::Sorter(cursor)
    }

    pub fn as_btree_mut(&mut self) -> &mut BTreeCursor {
        match self {
            Self::BTree(cursor) => cursor,
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
}

#[derive(Debug)]
pub enum CursorResult<T> {
    Ok(T),
    IO,
}

#[derive(Debug)]
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

impl RawSlice {
    pub fn create_from(value: &[u8]) -> Self {
        if value.is_empty() {
            RawSlice::new(std::ptr::null(), 0)
        } else {
            let ptr = &value[0] as *const u8;
            RawSlice::new(ptr, value.len())
        }
    }
    pub fn new(data: *const u8, len: usize) -> Self {
        Self { data, len }
    }
    pub fn to_slice(&self) -> &[u8] {
        if self.data.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.data, self.len) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;

    pub fn compare_immutable_for_testing(
        l: &[RefValue],
        r: &[RefValue],
        index_key_sort_order: IndexKeySortOrder,
        collations: &[CollationSeq],
        tie_breaker: std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        let min_len = l.len().min(r.len());

        for i in 0..min_len {
            let column_order = index_key_sort_order.get_sort_order_for_col(i);
            let collation = collations.get(i).copied().unwrap_or_default();

            let cmp = match (&l[i], &r[i]) {
                (RefValue::Text(left), RefValue::Text(right)) => {
                    collation.compare_strings(left.as_str(), right.as_str())
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

    fn create_index_info(num_cols: usize, sort_orders: Vec<SortOrder>) -> IndexKeyInfo {
        IndexKeyInfo {
            sort_order: IndexKeySortOrder::from_list(sort_orders.as_slice()),
            has_rowid: false,
            num_cols,
        }
    }

    fn value_to_ref_value(value: &Value) -> RefValue {
        match value {
            Value::Null => RefValue::Null,
            Value::Integer(i) => RefValue::Integer(*i),
            Value::Float(f) => RefValue::Float(*f),
            Value::Text(text) => RefValue::Text(TextRef {
                value: RawSlice::from_slice(&text.value),
                subtype: text.subtype,
            }),
            Value::Blob(blob) => RefValue::Blob(RawSlice::from_slice(blob)),
        }
    }

    impl TextRef {
        fn from_str(s: &str) -> Self {
            TextRef {
                value: RawSlice::from_slice(s.as_bytes()),
                subtype: crate::types::TextSubtype::Text,
            }
        }
    }

    impl RawSlice {
        fn from_slice(data: &[u8]) -> Self {
            Self {
                data: data.as_ptr(),
                len: data.len(),
            }
        }
    }

    fn assert_compare_matches_full_comparison(
        serialized_values: Vec<Value>,
        unpacked_values: Vec<RefValue>,
        index_info: &IndexKeyInfo,
        collations: &[CollationSeq],
        test_name: &str,
    ) {
        let serialized = create_record(serialized_values.clone());

        let serialized_ref_values: Vec<RefValue> =
            serialized_values.iter().map(value_to_ref_value).collect();

        let tie_breaker = std::cmp::Ordering::Equal;

        let gold_result = compare_immutable_for_testing(
            &serialized_ref_values,
            &unpacked_values,
            index_info.sort_order,
            collations,
            tie_breaker,
        );

        let comparer = find_compare(&unpacked_values, index_info, collations);
        let optimized_result = comparer
            .compare(
                &serialized,
                &unpacked_values,
                index_info,
                collations,
                0,
                tie_breaker,
            )
            .unwrap();

        assert_eq!(
            gold_result, optimized_result,
            "Test '{}' failed: Full Comparison: {:?}, Optimized: {:?}, Strategy: {:?}",
            test_name, gold_result, optimized_result, comparer
        );

        let generic_result = compare_records_generic(
            &serialized,
            &unpacked_values,
            index_info,
            collations,
            0,
            tie_breaker,
        )
        .unwrap();
        assert_eq!(
            gold_result, generic_result,
            "Test '{}' failed with generic: Full Comparison: {:?}, Generic: {:?}",
            test_name, gold_result, generic_result
        );
    }

    #[test]
    fn test_integer_fast_path() {
        let index_info = create_index_info(2, vec![SortOrder::Asc, SortOrder::Asc]);
        let collations = vec![CollationSeq::Binary, CollationSeq::Binary];

        let test_cases = vec![
            (
                vec![Value::Integer(42)],
                vec![RefValue::Integer(42)],
                "equal_integers",
            ),
            (
                vec![Value::Integer(10)],
                vec![RefValue::Integer(20)],
                "less_than_integers",
            ),
            (
                vec![Value::Integer(30)],
                vec![RefValue::Integer(20)],
                "greater_than_integers",
            ),
            (
                vec![Value::Integer(0)],
                vec![RefValue::Integer(0)],
                "zero_integers",
            ),
            (
                vec![Value::Integer(-5)],
                vec![RefValue::Integer(-5)],
                "negative_integers",
            ),
            (
                vec![Value::Integer(i64::MAX)],
                vec![RefValue::Integer(i64::MAX)],
                "max_integers",
            ),
            (
                vec![Value::Integer(i64::MIN)],
                vec![RefValue::Integer(i64::MIN)],
                "min_integers",
            ),
            (
                vec![Value::Integer(42), Value::Text(Text::new("hello"))],
                vec![
                    RefValue::Integer(42),
                    RefValue::Text(TextRef::from_str("hello")),
                ],
                "integer_text_equal",
            ),
            (
                vec![Value::Integer(42), Value::Text(Text::new("hello"))],
                vec![
                    RefValue::Integer(42),
                    RefValue::Text(TextRef::from_str("world")),
                ],
                "integer_equal_text_different",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                &collations,
                test_name,
            );
        }
    }

    #[test]
    fn test_string_fast_path() {
        let index_info = create_index_info(2, vec![SortOrder::Asc, SortOrder::Asc]);
        let collations = vec![CollationSeq::Binary, CollationSeq::Binary];

        let test_cases = vec![
            (
                vec![Value::Text(Text::new("hello"))],
                vec![RefValue::Text(TextRef::from_str("hello"))],
                "equal_strings",
            ),
            (
                vec![Value::Text(Text::new("abc"))],
                vec![RefValue::Text(TextRef::from_str("def"))],
                "less_than_strings",
            ),
            (
                vec![Value::Text(Text::new("xyz"))],
                vec![RefValue::Text(TextRef::from_str("abc"))],
                "greater_than_strings",
            ),
            (
                vec![Value::Text(Text::new(""))],
                vec![RefValue::Text(TextRef::from_str(""))],
                "empty_strings",
            ),
            (
                vec![Value::Text(Text::new("a"))],
                vec![RefValue::Text(TextRef::from_str("aa"))],
                "prefix_strings",
            ),
            // Multi-field with string first
            (
                vec![Value::Text(Text::new("hello")), Value::Integer(42)],
                vec![
                    RefValue::Text(TextRef::from_str("hello")),
                    RefValue::Integer(42),
                ],
                "string_integer_equal",
            ),
            (
                vec![Value::Text(Text::new("hello")), Value::Integer(42)],
                vec![
                    RefValue::Text(TextRef::from_str("hello")),
                    RefValue::Integer(99),
                ],
                "string_equal_integer_different",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                &collations,
                test_name,
            );
        }
    }

    #[test]
    fn test_type_precedence() {
        let index_info = create_index_info(1, vec![SortOrder::Asc]);
        let collations = vec![CollationSeq::Binary];

        // Test SQLite type precedence: NULL < Numbers < Text < Blob
        let test_cases = vec![
            // NULL vs others
            (
                vec![Value::Null],
                vec![RefValue::Integer(42)],
                "null_vs_integer",
            ),
            (
                vec![Value::Null],
                vec![RefValue::Float(64.4)],
                "null_vs_float",
            ),
            (
                vec![Value::Null],
                vec![RefValue::Text(TextRef::from_str("hello"))],
                "null_vs_text",
            ),
            (
                vec![Value::Null],
                vec![RefValue::Blob(RawSlice::from_slice(b"blob"))],
                "null_vs_blob",
            ),
            // Numbers vs Text/Blob
            (
                vec![Value::Integer(42)],
                vec![RefValue::Text(TextRef::from_str("hello"))],
                "integer_vs_text",
            ),
            (
                vec![Value::Float(64.4)],
                vec![RefValue::Text(TextRef::from_str("hello"))],
                "float_vs_text",
            ),
            (
                vec![Value::Integer(42)],
                vec![RefValue::Blob(RawSlice::from_slice(b"blob"))],
                "integer_vs_blob",
            ),
            (
                vec![Value::Float(64.4)],
                vec![RefValue::Blob(RawSlice::from_slice(b"blob"))],
                "float_vs_blob",
            ),
            // Text vs Blob
            (
                vec![Value::Text(Text::new("hello"))],
                vec![RefValue::Blob(RawSlice::from_slice(b"blob"))],
                "text_vs_blob",
            ),
            // Integer vs Float (affinity conversion)
            (
                vec![Value::Integer(42)],
                vec![RefValue::Float(42.0)],
                "integer_vs_equal_float",
            ),
            (
                vec![Value::Integer(42)],
                vec![RefValue::Float(42.5)],
                "integer_vs_different_float",
            ),
            (
                vec![Value::Float(42.5)],
                vec![RefValue::Integer(42)],
                "float_vs_integer",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                &collations,
                test_name,
            );
        }
    }

    #[test]
    fn test_sort_order_desc() {
        let index_info = create_index_info(2, vec![SortOrder::Desc, SortOrder::Asc]);
        let collations = vec![CollationSeq::Binary, CollationSeq::Binary];

        let test_cases = vec![
            // DESC order should reverse first field comparison
            (
                vec![Value::Integer(10)],
                vec![RefValue::Integer(20)],
                "desc_integer_reversed",
            ),
            (
                vec![Value::Text(Text::new("abc"))],
                vec![RefValue::Text(TextRef::from_str("def"))],
                "desc_string_reversed",
            ),
            // Mixed sort orders
            (
                vec![Value::Integer(10), Value::Text(Text::new("hello"))],
                vec![
                    RefValue::Integer(20),
                    RefValue::Text(TextRef::from_str("hello")),
                ],
                "desc_first_asc_second",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                &collations,
                test_name,
            );
        }
    }

    #[test]
    fn test_edge_cases() {
        let index_info = create_index_info(3, vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc]);
        let collations = vec![
            CollationSeq::Binary,
            CollationSeq::Binary,
            CollationSeq::Binary,
        ];

        let test_cases = vec![
            (
                vec![Value::Integer(42)],
                vec![
                    RefValue::Integer(42),
                    RefValue::Text(TextRef::from_str("extra")),
                ],
                "fewer_serialized_fields",
            ),
            (
                vec![Value::Integer(42), Value::Text(Text::new("extra"))],
                vec![RefValue::Integer(42)],
                "fewer_unpacked_fields",
            ),
            (vec![], vec![], "both_empty"),
            (vec![], vec![RefValue::Integer(42)], "empty_serialized"),
            (
                (0..15).map(Value::Integer).collect(),
                (0..15).map(RefValue::Integer).collect(),
                "large_field_count",
            ),
            (
                vec![Value::Blob(vec![1, 2, 3])],
                vec![RefValue::Blob(RawSlice::from_slice(&[1, 2, 3]))],
                "blob_first_field",
            ),
            (
                vec![Value::Text(Text::new("hello")), Value::Integer(5)],
                vec![RefValue::Text(TextRef::from_str("hello"))],
                "equal_text_prefix_but_more_serialized_fields",
            ),
            (
                vec![Value::Text(Text::new("same")), Value::Integer(5)],
                vec![
                    RefValue::Text(TextRef::from_str("same")),
                    RefValue::Integer(5),
                ],
                "equal_text_then_equal_int",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                &collations,
                test_name,
            );
        }
    }

    #[test]
    fn test_skip_parameter() {
        let index_info = create_index_info(3, vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc]);
        let collations = vec![
            CollationSeq::Binary,
            CollationSeq::Binary,
            CollationSeq::Binary,
        ];

        let serialized = create_record(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let unpacked = vec![
            RefValue::Integer(1),
            RefValue::Integer(99),
            RefValue::Integer(3),
        ];

        let tie_breaker = std::cmp::Ordering::Equal;
        let result_skip_0 = compare_records_generic(
            &serialized,
            &unpacked,
            &index_info,
            &collations,
            0,
            tie_breaker,
        )
        .unwrap();
        let result_skip_1 = compare_records_generic(
            &serialized,
            &unpacked,
            &index_info,
            &collations,
            1,
            tie_breaker,
        )
        .unwrap();

        assert_eq!(result_skip_0, std::cmp::Ordering::Less);

        assert_eq!(result_skip_1, std::cmp::Ordering::Less);
    }

    #[test]
    fn test_strategy_selection() {
        let index_info_small =
            create_index_info(3, vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc]);
        let index_info_large = create_index_info(15, vec![SortOrder::Asc; 15]);
        let collations = vec![CollationSeq::Binary, CollationSeq::Binary];

        let int_values = vec![
            RefValue::Integer(42),
            RefValue::Text(TextRef::from_str("hello")),
        ];
        assert!(matches!(
            find_compare(&int_values, &index_info_small, &collations),
            RecordCompare::Int
        ));

        let string_values = vec![
            RefValue::Text(TextRef::from_str("hello")),
            RefValue::Integer(42),
        ];
        assert!(matches!(
            find_compare(&string_values, &index_info_small, &collations),
            RecordCompare::String
        ));

        let large_values: Vec<RefValue> = (0..15).map(RefValue::Integer).collect();
        assert!(matches!(
            find_compare(&large_values, &index_info_large, &collations),
            RecordCompare::Generic
        ));

        let blob_values = vec![RefValue::Blob(RawSlice::from_slice(&[1, 2, 3]))];
        assert!(matches!(
            find_compare(&blob_values, &index_info_small, &collations),
            RecordCompare::Generic
        ));
    }

    #[test]
    fn test_record_parsing() {
        let values = [
            Value::Integer(42),
            Value::Text(Text::new("hello")),
            Value::Float(64.4),
            Value::Null,
            Value::Integer(1000000),
            Value::Blob(vec![1, 2, 3, 4, 5]),
        ];

        let registers: Vec<Register> = values.iter().cloned().map(Register::Value).collect();
        let record = ImmutableRecord::from_registers(&registers, registers.len());

        // Full Parsing
        let mut cursor1 = RecordCursor::new();
        cursor1
            .parse_full_header(&record)
            .expect("Failed to parse full header");

        assert_eq!(
            cursor1.offsets.len(),
            cursor1.serial_types.len() + 1,
            "offsets should be one longer than serial_types"
        );

        for i in 0..values.len() {
            cursor1
                .deserialize_column(&record, i)
                .expect("Failed to deserialize column");
        }

        // Incremental Parsing
        let mut cursor2 = RecordCursor::new();
        cursor2
            .ensure_parsed_upto(&record, 2)
            .expect("Failed to parse up to column 2");

        assert_eq!(
            cursor2.offsets.len(),
            cursor2.serial_types.len() + 1,
            "offsets should be one longer than serial_types"
        );

        cursor2.get_value(&record, 2).expect("Column 2 failed");

        // Access column 0 (already parsed)
        let before = cursor2.serial_types.len();
        cursor2.get_value(&record, 0).expect("Column 0 failed");
        let after = cursor2.serial_types.len();
        assert_eq!(before, after, "Should not parse more");

        // Access column 5 (forces full parse)
        cursor2
            .ensure_parsed_upto(&record, 5)
            .expect("Column 5 parse failed");
        cursor2.get_value(&record, 5).expect("Column 5 failed");

        // Compare both parsing strategies
        for i in 0..values.len() {
            let full = cursor1.get_value(&record, i).expect("full failed");
            let incr = cursor2.get_value(&record, i).expect("incr failed");
            assert_eq!(full, incr, "Mismatch at column {}", i);
        }

        assert_eq!(
            cursor1.serial_types, cursor2.serial_types,
            "serial_types must match"
        );
        assert_eq!(cursor1.offsets, cursor2.offsets, "offsets must match");
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
}
