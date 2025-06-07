use limbo_ext::{AggCtx, FinalizeFunction, StepFunction};
use limbo_sqlite3_parser::ast::SortOrder;

use crate::error::LimboError;
use crate::ext::{ExtValue, ExtValueType};
use crate::pseudo::PseudoCursor;
use crate::schema::Index;
use crate::storage::btree::BTreeCursor;
use crate::storage::sqlite3_ondisk::write_varint;
use crate::translate::collate::CollationSeq;
use crate::translate::plan::IterationDirection;
use crate::vdbe::sorter::Sorter;
use crate::vdbe::Register;
use crate::vtab::VirtualTableCursor;
use crate::Result;
use std::fmt::Display;

const MAX_REAL_SIZE: u8 = 15;

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
        write!(f, "{}", value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TextSubtype {
    Text,
    #[cfg(feature = "json")]
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Text {
    pub value: Vec<u8>,
    pub subtype: TextSubtype,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextRef {
    pub value: RawSlice,
    pub subtype: TextSubtype,
}

impl Text {
    pub fn from_str<S: Into<String>>(value: S) -> Self {
        Self::new(&value.into())
    }

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

    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.value.as_ref()) }
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

impl TextRef {
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.value.to_slice()) }
    }

    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(Text),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawSlice {
    data: *const u8,
    len: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub enum RefValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(TextRef),
    Blob(RawSlice),
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
                write!(f, "{}", i)
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
                    let sci_notation = format!("{:.14e}", fl);
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
                                format!("{}.{}", whole, fraction)
                            };
                            let (prefix, exponent) = if exponent.starts_with('-') {
                                ("-0", &exponent[1..])
                            } else {
                                ("+", exponent)
                            };
                            return write!(f, "{}e{}{}", trimmed_mantissa, prefix, exponent);
                        }
                    }

                    // fallback
                    return write!(f, "{}", sci_notation);
                }

                // handle floating point max size is 15.
                // If left > right && right + left > 15 go to sci notation
                // If right > left && right + left > 15 truncate left so right + left == 15
                let rounded = fl.round();
                if (fl - rounded).abs() < 1e-14 {
                    // if we very close to integer trim decimal part to 1 digit
                    if rounded == rounded as i64 as f64 {
                        return write!(f, "{:.1}", fl);
                    }
                }

                let fl_str = format!("{}", fl);
                let splitted = fl_str.split('.').collect::<Vec<&str>>();
                // fallback
                if splitted.len() != 2 {
                    return write!(f, "{:.14e}", fl);
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
                    return write!(f, "{:.14e}", fl);
                }
                // trim decimal part to reminder or self len so total digits is 15;
                let mut fl = format!("{:.*}", second.len().min(reminder as usize), fl);
                // if decimal part ends with 0 we trim it
                while fl.ends_with('0') {
                    fl.pop();
                }
                write!(f, "{}", fl)
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

    fn ne(&self, other: &Value) -> bool {
        !self.eq(other)
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

pub trait FromValue<'a> {
    fn from_value(value: &'a RefValue) -> Result<Self>
    where
        Self: Sized + 'a;
}

impl<'a> FromValue<'a> for i64 {
    fn from_value(value: &'a RefValue) -> Result<Self> {
        match value {
            RefValue::Integer(i) => Ok(*i),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl<'a> FromValue<'a> for String {
    fn from_value(value: &'a RefValue) -> Result<Self> {
        match value {
            RefValue::Text(s) => Ok(s.as_str().to_string()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

impl<'a> FromValue<'a> for &'a str {
    fn from_value(value: &'a RefValue) -> Result<Self> {
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
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ImmutableRecord {
    // We have to be super careful with this buffer since we make values point to the payload we need to take care reallocations
    // happen in a controlled manner. If we realocate with values that should be correct, they will now point to undefined data.
    // We don't use pin here because it would make it imposible to reuse the buffer if we need to push a new record in the same struct.
    payload: Vec<u8>,
    pub values: Vec<RefValue>,
    recreating: bool,
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
    pub fn new(payload_capacity: usize, value_capacity: usize) -> Self {
        Self {
            payload: Vec::with_capacity(payload_capacity),
            values: Vec::with_capacity(value_capacity),
            recreating: false,
        }
    }

    pub fn get<'a, T: FromValue<'a> + 'a>(&'a self, idx: usize) -> Result<T> {
        let value = self
            .values
            .get(idx)
            .ok_or(LimboError::InternalError("Index out of bounds".into()))?;
        T::from_value(value)
    }

    pub fn count(&self) -> usize {
        self.values.len()
    }

    pub fn last_value(&self) -> Option<&RefValue> {
        self.values.last()
    }

    pub fn get_values(&self) -> &Vec<RefValue> {
        &self.values
    }

    pub fn get_value(&self, idx: usize) -> &RefValue {
        &self.values[idx]
    }

    pub fn get_value_opt(&self, idx: usize) -> Option<&RefValue> {
        self.values.get(idx)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn from_registers(registers: &[Register]) -> Self {
        let mut values = Vec::with_capacity(registers.len());
        let mut serials = Vec::with_capacity(registers.len());
        let mut size_header = 0;
        let mut size_values = 0;

        let mut serial_type_buf = [0; 9];
        // write serial types
        for value in registers {
            let value = value.get_owned_value();
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
            todo!("calculate big header size extra bytes");
            // get header varint len
            // header_size += n;
            // if( nVarint<sqlite3VarintLen(nHdr) ) nHdr++;
        }
        // 1. write header size
        let mut buf = Vec::new();
        buf.reserve_exact(header_size + size_values);
        assert_eq!(buf.capacity(), header_size + size_values);
        assert!(header_size <= 126);
        let n = write_varint(&mut serial_type_buf, header_size as u64);

        buf.resize(buf.capacity(), 0);
        let mut writer = AppendWriter::new(&mut buf, 0);
        writer.extend_from_slice(&serial_type_buf[..n]);

        // 2. Write serial
        for (value, n) in serials {
            writer.extend_from_slice(&value[..n]);
        }

        // write content
        for value in registers {
            let value = value.get_owned_value();
            let start_offset = writer.pos;
            match value {
                Value::Null => {
                    values.push(RefValue::Null);
                }
                Value::Integer(i) => {
                    values.push(RefValue::Integer(*i));
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
                        other => panic!("Serial type is not an integer: {:?}", other),
                    }
                }
                Value::Float(f) => {
                    values.push(RefValue::Float(*f));
                    writer.extend_from_slice(&f.to_be_bytes())
                }
                Value::Text(t) => {
                    writer.extend_from_slice(&t.value);
                    let end_offset = writer.pos;
                    let len = end_offset - start_offset;
                    let ptr = unsafe { writer.buf.as_ptr().add(start_offset) };
                    let value = RefValue::Text(TextRef {
                        value: RawSlice::new(ptr, len),
                        subtype: t.subtype.clone(),
                    });
                    values.push(value);
                }
                Value::Blob(b) => {
                    writer.extend_from_slice(b);
                    let end_offset = writer.pos;
                    let len = end_offset - start_offset;
                    let ptr = unsafe { writer.buf.as_ptr().add(start_offset) };
                    values.push(RefValue::Blob(RawSlice::new(ptr, len)));
                }
            };
        }

        writer.assert_finish_capacity();
        Self {
            payload: buf,
            values,
            recreating: false,
        }
    }

    pub fn start_serialization(&mut self, payload: &[u8]) {
        self.recreating = true;
        self.payload.extend_from_slice(payload);
    }
    pub fn end_serialization(&mut self) {
        assert!(self.recreating);
        self.recreating = false;
    }

    pub fn add_value(&mut self, value: RefValue) {
        assert!(self.recreating);
        self.values.push(value);
    }

    pub fn invalidate(&mut self) {
        self.payload.clear();
        self.values.clear();
    }

    pub fn get_payload(&self) -> &[u8] {
        &self.payload
    }
}

impl Display for ImmutableRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for value in &self.values {
            match value {
                RefValue::Null => write!(f, "NULL")?,
                RefValue::Integer(i) => write!(f, "Integer({})", *i)?,
                RefValue::Float(flo) => write!(f, "Float({})", *flo)?,
                RefValue::Text(text_ref) => write!(f, "Text({})", text_ref.as_str())?,
                RefValue::Blob(raw_slice) => {
                    write!(f, "Blob({})", String::from_utf8_lossy(raw_slice.to_slice()))?
                }
            }
            if value != self.values.last().unwrap() {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

impl Clone for ImmutableRecord {
    fn clone(&self) -> Self {
        let mut new_values = Vec::new();
        let new_payload = self.payload.clone();
        for value in &self.values {
            let value = match value {
                RefValue::Null => RefValue::Null,
                RefValue::Integer(i) => RefValue::Integer(*i),
                RefValue::Float(f) => RefValue::Float(*f),
                RefValue::Text(text_ref) => {
                    // let's update pointer
                    let ptr_start = self.payload.as_ptr() as usize;
                    let ptr_end = text_ref.value.data as usize;
                    let len = ptr_end - ptr_start;
                    let new_ptr = unsafe { new_payload.as_ptr().add(len) };
                    RefValue::Text(TextRef {
                        value: RawSlice::new(new_ptr, text_ref.value.len),
                        subtype: text_ref.subtype.clone(),
                    })
                }
                RefValue::Blob(raw_slice) => {
                    let ptr_start = self.payload.as_ptr() as usize;
                    let ptr_end = raw_slice.data as usize;
                    let len = ptr_end - ptr_start;
                    let new_ptr = unsafe { new_payload.as_ptr().add(len) };
                    RefValue::Blob(RawSlice::new(new_ptr, raw_slice.len))
                }
            };
            new_values.push(value);
        }
        Self {
            payload: new_payload,
            values: new_values,
            recreating: self.recreating,
        }
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
                subtype: text_ref.subtype.clone(),
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
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{:?}", fl),
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

/// A bitfield that represents the comparison spec for index keys.
/// Since indexed columns can individually specify ASC/DESC, each key must
/// be compared differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct IndexKeySortOrder(u64);

impl IndexKeySortOrder {
    pub fn get_sort_order_for_col(&self, column_idx: usize) -> SortOrder {
        assert!(column_idx < 64, "column index out of range: {}", column_idx);
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

    pub fn default() -> Self {
        Self(0)
    }
}

impl Default for IndexKeySortOrder {
    fn default() -> Self {
        Self::default()
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
            return Err(LimboError::Corrupt(format!(
                "Invalid serial type: {}",
                uint
            )));
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
        header_bytes_buf.extend(std::iter::repeat(0).take(9));
        let n = write_varint(header_bytes_buf.as_mut_slice(), header_size as u64);
        header_bytes_buf.truncate(n);
        buf.splice(initial_i..initial_i, header_bytes_buf.iter().cloned());
    }
}

pub enum Cursor {
    BTree(BTreeCursor),
    Pseudo(PseudoCursor),
    Sorter(Sorter),
    Virtual(VirtualTableCursor),
}

impl Cursor {
    pub fn new_btree(cursor: BTreeCursor) -> Self {
        Self::BTree(cursor)
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// The match condition of a table/index seek.
pub enum SeekOp {
    EQ,
    GE,
    GT,
    LE,
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
            SeekOp::EQ | SeekOp::GE | SeekOp::GT => IterationDirection::Forwards,
            SeekOp::LE | SeekOp::LT => IterationDirection::Backwards,
        }
    }

    pub fn reverse(&self) -> Self {
        match self {
            SeekOp::EQ => SeekOp::EQ,
            SeekOp::GE => SeekOp::LE,
            SeekOp::GT => SeekOp::LT,
            SeekOp::LE => SeekOp::GE,
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
                + 0        // ConstInt0: 0 bytes
                + 0        // ConstInt1: 0 bytes  
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
        // First byte should be header size
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
