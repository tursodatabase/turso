use limbo_ext::{AggCtx, FinalizeFunction, StepFunction};

use crate::error::LimboError;
use crate::ext::{ExtValue, ExtValueType};
use crate::pseudo::PseudoCursor;
use crate::storage::btree::BTreeCursor;
use crate::storage::sqlite3_ondisk::write_varint;
use crate::translate::plan::IterationDirection;
use crate::vdbe::sorter::Sorter;
use crate::vdbe::{Register, VTabOpaqueCursor};
use crate::Result;
use std::fmt::Display;

const MAX_REAL_SIZE: u8 = 15;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OwnedValueType {
    Null,
    Integer,
    Float,
    Text,
    Blob,
    Error,
}

impl Display for OwnedValueType {
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
pub struct OwnedValue {
    buffer: Vec<u8>,
    value_type: OwnedValueType,
    text_subtype: Option<TextSubtype>,
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

impl OwnedValue {
    pub fn null() -> Self {
        Self {
            buffer: Vec::new(),
            value_type: OwnedValueType::Null,
            text_subtype: None,
        }
    }

    pub fn integer(value: i64) -> Self {
        let mut buffer = Vec::with_capacity(8);
        buffer.extend_from_slice(&value.to_ne_bytes());
        Self {
            buffer,
            value_type: OwnedValueType::Integer,
            text_subtype: None,
        }
    }

    pub fn float(value: f64) -> Self {
        let mut buffer = Vec::with_capacity(8);
        buffer.extend_from_slice(&value.to_ne_bytes());
        Self {
            buffer,
            value_type: OwnedValueType::Float,
            text_subtype: None,
        }
    }

    pub fn set_text(&mut self, value: &str) {
        let bytes = value.as_bytes();
        self.buffer.clear();
        self.buffer.reserve(bytes.len());
        self.buffer.extend_from_slice(bytes);
        self.value_type = OwnedValueType::Text;
        self.text_subtype = Some(TextSubtype::Text);
    }

    pub fn build_text(text: &str) -> Self {
        let bytes = text.as_bytes();
        Self {
            buffer: bytes.to_vec(),
            value_type: OwnedValueType::Text,
            text_subtype: Some(TextSubtype::Text),
        }
    }

    pub fn from_blob(data: Vec<u8>) -> Self {
        Self {
            buffer: data,
            value_type: OwnedValueType::Blob,
            text_subtype: None,
        }
    }

    pub fn set_blob(&mut self, data: &[u8]) {
        self.buffer.clear();
        self.buffer.reserve(data.len());
        self.buffer.extend_from_slice(data);
        self.value_type = OwnedValueType::Blob;
        self.text_subtype = None;
    }

    // Type conversion methods
    pub fn as_integer(&self) -> Option<i64> {
        match self.value_type {
            OwnedValueType::Integer => {
                if self.buffer.len() >= 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&self.buffer[0..8]);
                    Some(i64::from_ne_bytes(bytes))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self.value_type {
            OwnedValueType::Float => {
                if self.buffer.len() >= 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&self.buffer[0..8]);
                    Some(f64::from_ne_bytes(bytes))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self.value_type {
            OwnedValueType::Text => std::str::from_utf8(&self.buffer).ok(),
            _ => None,
        }
    }

    pub fn to_text(&self) -> Option<&str> {
        self.as_text()
    }

    pub fn to_blob(&self) -> Option<&[u8]> {
        match self.value_type {
            OwnedValueType::Blob => Some(&self.buffer),
            _ => None,
        }
    }

    pub fn value_type(&self) -> OwnedValueType {
        self.value_type
    }

    pub fn convert_to_integer(&mut self, value: i64) {
        self.buffer.clear();
        self.buffer.reserve(8);
        self.buffer.extend_from_slice(&value.to_ne_bytes());
        self.value_type = OwnedValueType::Integer;
        self.text_subtype = None;
    }

    pub fn convert_to_float(&mut self, value: f64) {
        self.buffer.clear();
        self.buffer.reserve(8);
        self.buffer.extend_from_slice(&value.to_ne_bytes());
        self.value_type = OwnedValueType::Float;
        self.text_subtype = None;
    }

    pub fn convert_to_null(&mut self) {
        self.buffer.clear();
        self.value_type = OwnedValueType::Null;
        self.text_subtype = None;
    }

    pub fn serialize_serial(&self, out: &mut Vec<u8>) {
        match self.value_type {
            OwnedValueType::Null => {}
            OwnedValueType::Integer => {
                let i = self.as_integer().unwrap();
                let serial_type = SerialType::from(self);
                match serial_type {
                    SerialType::I8 => out.extend_from_slice(&(i as i8).to_be_bytes()),
                    SerialType::I16 => out.extend_from_slice(&(i as i16).to_be_bytes()),
                    SerialType::I24 => out.extend_from_slice(&(i as i32).to_be_bytes()[1..]),
                    SerialType::I32 => out.extend_from_slice(&(i as i32).to_be_bytes()),
                    SerialType::I48 => out.extend_from_slice(&i.to_be_bytes()[2..]),
                    SerialType::I64 => out.extend_from_slice(&i.to_be_bytes()),
                    _ => unreachable!(),
                }
            }
            OwnedValueType::Float => {
                let f = self.as_float().unwrap();
                out.extend_from_slice(&f.to_be_bytes());
            }
            OwnedValueType::Text => out.extend_from_slice(&self.buffer),
            OwnedValueType::Blob => out.extend_from_slice(&self.buffer),
            OwnedValueType::Error => todo!(),
        }
    }

    pub fn from_text(text: &str) -> Self {
        Self {
            buffer: text.as_bytes().to_vec(),
            value_type: OwnedValueType::Text,
            text_subtype: Some(TextSubtype::Text),
        }
    }

    pub fn blob_buffer_mut(&mut self) -> Option<&mut Vec<u8>> {
        if self.value_type == OwnedValueType::Blob {
            Some(&mut self.buffer)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalAggState {
    pub state: *mut AggCtx,
    pub argc: usize,
    pub step_fn: StepFunction,
    pub finalize_fn: FinalizeFunction,
    pub finalized_value: Option<OwnedValue>,
}

impl ExternalAggState {
    pub fn cache_final_value(&mut self, value: OwnedValue) -> &OwnedValue {
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
///   OwnedValue::Integer(i) => *i.as_str(),
///   OwnedValue::Float(f) => *f.as_str(),
///   ....
/// }
impl Display for OwnedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value_type {
            OwnedValueType::Null => write!(f, ""),
            OwnedValueType::Integer => {
                if let Some(i) = self.as_integer() {
                    write!(f, "{}", i)
                } else {
                    write!(f, "0")
                }
            }
            OwnedValueType::Float => {
                if let Some(fl) = self.as_float() {
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
                } else {
                    write!(f, "0.0")
                }
            }
            OwnedValueType::Text => {
                if let Some(s) = self.as_text() {
                    write!(f, "{}", s)
                } else {
                    write!(f, "")
                }
            }

            OwnedValueType::Blob => {
                write!(f, "{}", String::from_utf8_lossy(&self.buffer))
            }
            OwnedValueType::Error => todo!(),
        }
    }
}

impl OwnedValue {
    pub fn to_ffi(&self) -> ExtValue {
        match self.value_type {
            OwnedValueType::Null => ExtValue::null(),
            OwnedValueType::Integer => ExtValue::from_integer(self.as_integer().unwrap()),
            OwnedValueType::Float => ExtValue::from_float(self.as_float().unwrap()),
            OwnedValueType::Text => ExtValue::from_text(self.as_text().unwrap().to_string()),
            OwnedValueType::Blob => ExtValue::from_blob(self.buffer.clone()),
            OwnedValueType::Error => todo!(),
        }
    }

    pub fn from_ffi(v: ExtValue) -> Result<Self> {
        let res = match v.value_type() {
            ExtValueType::Null => Ok(Self::null()),
            ExtValueType::Integer => {
                let Some(int) = v.to_integer() else {
                    return Ok(Self::null());
                };
                Ok(Self::integer(int))
            }
            ExtValueType::Float => {
                let Some(float) = v.to_float() else {
                    return Ok(Self::null());
                };
                Ok(Self::float(float))
            }
            ExtValueType::Text => {
                let Some(text) = v.to_text() else {
                    return Ok(Self::null());
                };
                #[cfg(feature = "json")]
                if v.is_json() {
                    let mut value = Self::from_text(text);
                    value.text_subtype = Some(TextSubtype::Json);
                    return Ok(value);
                }
                Ok(OwnedValue::build_text(text))
            }
            ExtValueType::Blob => {
                let Some(blob) = v.to_blob() else {
                    return Ok(Self::null());
                };
                Ok(Self::from_blob(blob))
            }
            ExtValueType::Error => {
                let Some(err) = v.to_error_details() else {
                    return Ok(Self::null());
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
    Avg(OwnedValue, OwnedValue), // acc and count
    Sum(OwnedValue),
    Count(OwnedValue),
    Max(Option<OwnedValue>),
    Min(Option<OwnedValue>),
    GroupConcat(OwnedValue),
    External(ExternalAggState),
}

const NULL: OwnedValue = OwnedValue {
    buffer: Vec::new(),
    value_type: OwnedValueType::Null,
    text_subtype: None,
};

impl AggContext {
    pub fn compute_external(&mut self) -> Result<()> {
        if let Self::External(ext_state) = self {
            if ext_state.finalized_value.is_none() {
                let final_value = unsafe { (ext_state.finalize_fn)(ext_state.state) };
                ext_state.cache_final_value(OwnedValue::from_ffi(final_value)?);
            }
        }
        Ok(())
    }

    pub fn final_value(&self) -> OwnedValue {
        match self {
            Self::Avg(acc, _count) => acc.clone(),
            Self::Sum(acc) => acc.clone(),
            Self::Count(count) => count.clone(),
            Self::Max(max) => max.clone().unwrap_or_else(|| OwnedValue::null()),
            Self::Min(min) => min.clone().unwrap_or_else(|| OwnedValue::null()),
            Self::GroupConcat(s) => s.clone(),
            Self::External(ext_state) => ext_state
                .finalized_value
                .clone()
                .unwrap_or_else(|| OwnedValue::null()),
        }
    }
}

impl PartialEq<OwnedValue> for OwnedValue {
    fn eq(&self, other: &OwnedValue) -> bool {
        match (self.value_type, other.value_type) {
            (OwnedValueType::Integer, OwnedValueType::Integer) => {
                self.as_integer() == other.as_integer()
            }
            (OwnedValueType::Integer, OwnedValueType::Float) => {
                (self.as_integer().unwrap() as f64) == (other.as_float().unwrap())
            }
            (OwnedValueType::Float, OwnedValueType::Integer) => {
                self.as_float().unwrap() == other.as_integer().unwrap() as f64
            }
            (OwnedValueType::Float, OwnedValueType::Float) => {
                self.as_float().unwrap() == other.as_float().unwrap()
            }
            (
                OwnedValueType::Integer | OwnedValueType::Float,
                OwnedValueType::Text | OwnedValueType::Blob,
            ) => false,
            (
                OwnedValueType::Text | OwnedValueType::Blob,
                OwnedValueType::Integer | OwnedValueType::Float,
            ) => false,
            (OwnedValueType::Text, OwnedValueType::Text) => self.buffer == other.buffer,
            (OwnedValueType::Blob, OwnedValueType::Blob) => self.buffer == other.buffer,
            (OwnedValueType::Null, OwnedValueType::Null) => true,
            _ => false,
        }
    }

    fn ne(&self, other: &OwnedValue) -> bool {
        !self.eq(other)
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd<OwnedValue> for OwnedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self.value_type, other.value_type) {
            (OwnedValueType::Integer, OwnedValueType::Integer) => self
                .as_integer()
                .unwrap()
                .partial_cmp(&other.as_integer().unwrap()),
            (OwnedValueType::Integer, OwnedValueType::Float) => {
                (self.as_integer().unwrap() as f64).partial_cmp(&other.as_float().unwrap())
            }
            (OwnedValueType::Float, OwnedValueType::Integer) => self
                .as_integer()
                .unwrap()
                .partial_cmp(&(other.as_float().unwrap() as i64)),
            (OwnedValueType::Float, OwnedValueType::Float) => self
                .as_float()
                .unwrap()
                .partial_cmp(&other.as_float().unwrap()),
            (
                OwnedValueType::Integer | OwnedValueType::Float,
                OwnedValueType::Text | OwnedValueType::Blob,
            ) => Some(std::cmp::Ordering::Less),
            (
                OwnedValueType::Text | OwnedValueType::Blob,
                OwnedValueType::Integer | OwnedValueType::Float,
            ) => Some(std::cmp::Ordering::Greater),
            (OwnedValueType::Text, OwnedValueType::Text) => self.buffer.partial_cmp(&other.buffer),
            // Text vs Blob
            (OwnedValueType::Text, OwnedValueType::Blob) => Some(std::cmp::Ordering::Less),
            (OwnedValueType::Blob, OwnedValueType::Text) => Some(std::cmp::Ordering::Greater),

            (OwnedValueType::Blob, OwnedValueType::Blob) => self.buffer.partial_cmp(&other.buffer),
            (OwnedValueType::Null, OwnedValueType::Null) => Some(std::cmp::Ordering::Equal),
            (OwnedValueType::Null, _) => Some(std::cmp::Ordering::Less),
            (_, OwnedValueType::Null) => Some(std::cmp::Ordering::Greater),
            (_, _) => todo!(),
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

impl Eq for OwnedValue {}

impl Ord for OwnedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::ops::Add<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self.value_type, rhs.value_type) {
            (OwnedValueType::Integer, OwnedValueType::Integer) => {
                let lhs = self.as_integer().unwrap();
                let rhs = rhs.as_integer().unwrap();
                OwnedValue::integer(lhs + rhs)
            }
            (OwnedValueType::Integer, OwnedValueType::Float) => {
                let lhs = self.as_integer().unwrap() as f64;
                let rhs = rhs.as_float().unwrap();
                OwnedValue::float(lhs + rhs)
            }
            (OwnedValueType::Float, OwnedValueType::Integer) => {
                let lhs = self.as_float().unwrap();
                let rhs = rhs.as_integer().unwrap() as f64;
                OwnedValue::float(lhs + rhs)
            }
            (OwnedValueType::Float, OwnedValueType::Float) => {
                let lhs = self.as_float().unwrap();
                let rhs = rhs.as_float().unwrap();
                OwnedValue::float(lhs + rhs)
            }
            (OwnedValueType::Text, OwnedValueType::Text) => {
                let lhs = self.as_text().unwrap();
                let rhs = rhs.as_text().unwrap();
                OwnedValue::build_text(&(lhs.to_string() + rhs))
            }
            (OwnedValueType::Text, OwnedValueType::Integer) => {
                let lhs = self.as_text().unwrap();
                let rhs = rhs.as_integer().unwrap().to_string();
                OwnedValue::build_text(&(lhs.to_string() + &rhs))
            }

            (OwnedValueType::Integer, OwnedValueType::Text) => {
                let lhs = self.as_integer().unwrap().to_string();
                let rhs = rhs.as_text().unwrap();
                OwnedValue::build_text(&(lhs + rhs))
            }
            (OwnedValueType::Text, OwnedValueType::Float) => {
                let lhs = self.as_text().unwrap();
                let rhs = rhs.as_float().unwrap();
                let string_right = rhs.to_string();
                OwnedValue::build_text(&(lhs.to_string() + &string_right))
            }
            (OwnedValueType::Float, OwnedValueType::Text) => {
                let lhs = self.as_float().unwrap();
                let string_left = lhs.to_string();
                let rhs = rhs.as_text().unwrap();
                OwnedValue::build_text(&(string_left + rhs))
            }
            (_, OwnedValueType::Null) => self,
            (OwnedValueType::Null, _) => rhs,
            _ => OwnedValue::float(0.0),
        }
    }
}

impl std::ops::Add<f64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: f64) -> Self::Output {
        match self.value_type {
            OwnedValueType::Integer => {
                let int_left = self.as_integer().unwrap();
                OwnedValue::float(int_left as f64 + rhs)
            }
            OwnedValueType::Float => {
                let float_left = self.as_float().unwrap();
                OwnedValue::float(float_left + rhs)
            }
            _ => unreachable!(),
        }
    }
}

impl std::ops::Add<i64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: i64) -> Self::Output {
        match self.value_type {
            OwnedValueType::Integer => {
                let int_left = self.as_integer().unwrap();
                OwnedValue::integer(int_left + rhs)
            }
            OwnedValueType::Float => {
                let float_left = self.as_float().unwrap();
                OwnedValue::float(float_left + rhs as f64)
            }
            _ => unreachable!(),
        }
    }
}

impl std::ops::AddAssign for OwnedValue {
    fn add_assign(&mut self, rhs: Self) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<i64> for OwnedValue {
    fn add_assign(&mut self, rhs: i64) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<f64> for OwnedValue {
    fn add_assign(&mut self, rhs: f64) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::Div<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn div(self, rhs: OwnedValue) -> Self::Output {
        match (self.value_type, rhs.value_type) {
            (OwnedValueType::Integer, OwnedValueType::Integer) => {
                Self::integer(self.as_integer().unwrap() / rhs.as_integer().unwrap())
            }
            (OwnedValueType::Integer, OwnedValueType::Float) => {
                Self::float(self.as_integer().unwrap() as f64 / rhs.as_float().unwrap())
            }
            (OwnedValueType::Float, OwnedValueType::Integer) => {
                Self::float(self.as_float().unwrap() / rhs.as_integer().unwrap() as f64)
            }
            (OwnedValueType::Float, OwnedValueType::Float) => {
                Self::float(self.as_float().unwrap() / self.as_float().unwrap())
            }
            _ => Self::float(0.0),
        }
    }
}

impl std::ops::DivAssign<OwnedValue> for OwnedValue {
    fn div_assign(&mut self, rhs: OwnedValue) {
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
    values: Vec<OwnedValue>,
}

impl Record {
    // pub fn get<'a, T: FromValue<'a> + 'a>(&'a self, idx: usize) -> Result<T> {
    //     let value = &self.values[idx];
    //     T::from_value(value)
    // }

    pub fn count(&self) -> usize {
        self.values.len()
    }

    pub fn last_value(&self) -> Option<&OwnedValue> {
        self.values.last()
    }

    pub fn get_values(&self) -> &Vec<OwnedValue> {
        &self.values
    }

    pub fn get_value(&self, idx: usize) -> &OwnedValue {
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

            let value_size = match serial_type {
                SerialType::Null => 0,
                SerialType::I8 => 1,
                SerialType::I16 => 2,
                SerialType::I24 => 3,
                SerialType::I32 => 4,
                SerialType::I48 => 6,
                SerialType::I64 => 8,
                SerialType::F64 => 8,
                SerialType::Text { content_size } => content_size,
                SerialType::Blob { content_size } => content_size,
            };

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
            match value.value_type {
                OwnedValueType::Null => {
                    values.push(RefValue::Null);
                }
                OwnedValueType::Integer => {
                    let i = value.as_integer().unwrap();
                    values.push(RefValue::Integer(i));
                    let serial_type = SerialType::from(value);
                    match serial_type {
                        SerialType::I8 => writer.extend_from_slice(&(i as i8).to_be_bytes()),
                        SerialType::I16 => writer.extend_from_slice(&(i as i16).to_be_bytes()),
                        SerialType::I24 => writer.extend_from_slice(&(i as i32).to_be_bytes()[1..]), // remove most significant byte
                        SerialType::I32 => writer.extend_from_slice(&(i as i32).to_be_bytes()),
                        SerialType::I48 => writer.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialType::I64 => writer.extend_from_slice(&i.to_be_bytes()),
                        _ => unreachable!(),
                    }
                }
                OwnedValueType::Float => {
                    let f = value.as_float().unwrap();
                    values.push(RefValue::Float(f));
                    writer.extend_from_slice(&f.to_be_bytes())
                }
                OwnedValueType::Text => {
                    writer.extend_from_slice(&value.buffer);
                    let end_offset = writer.pos;
                    let len = end_offset - start_offset;
                    let ptr = unsafe { writer.buf.as_ptr().add(start_offset) };
                    let value = RefValue::Text(TextRef {
                        value: RawSlice::new(ptr, len),
                        subtype: value.text_subtype.clone().unwrap(),
                    });
                    values.push(value);
                }
                OwnedValueType::Blob => {
                    writer.extend_from_slice(&value.buffer.as_slice());
                    let end_offset = writer.pos;
                    let len = end_offset - start_offset;
                    let ptr = unsafe { writer.buf.as_ptr().add(start_offset) };
                    values.push(RefValue::Blob(RawSlice::new(ptr, len)));
                }
                _ => todo!(),
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

    pub fn to_owned(&self) -> OwnedValue {
        match self {
            RefValue::Null => OwnedValue::null(),
            RefValue::Integer(i) => OwnedValue::integer(*i),
            RefValue::Float(f) => OwnedValue::float(*f),
            RefValue::Text(text_ref) => {
                let bytes = text_ref.value.to_slice();
                let mut value = OwnedValue {
                    buffer: bytes.to_vec(),
                    value_type: OwnedValueType::Text,
                    text_subtype: Some(text_ref.subtype.clone()),
                };
                value
            }
            RefValue::Blob(b) => OwnedValue::from_blob(b.to_slice().to_vec()),
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

pub fn compare_immutable(l: &[RefValue], r: &[RefValue]) -> std::cmp::Ordering {
    l.partial_cmp(r).unwrap()
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
enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Text { content_size: usize },
    Blob { content_size: usize },
}

impl From<&OwnedValue> for SerialType {
    fn from(value: &OwnedValue) -> Self {
        match value.value_type {
            OwnedValueType::Null => SerialType::Null,
            OwnedValueType::Integer => {
                let i = value.as_integer().unwrap();
                match i {
                    i if i >= I8_LOW && i <= I8_HIGH => SerialType::I8,
                    i if i >= I16_LOW && i <= I16_HIGH => SerialType::I16,
                    i if i >= I24_LOW && i <= I24_HIGH => SerialType::I24,
                    i if i >= I32_LOW && i <= I32_HIGH => SerialType::I32,
                    i if i >= I48_LOW && i <= I48_HIGH => SerialType::I48,
                    _ => SerialType::I64,
                }
            }
            OwnedValueType::Float => SerialType::F64,
            OwnedValueType::Text => SerialType::Text {
                content_size: value.buffer.len(),
            },
            OwnedValueType::Blob => SerialType::Blob {
                content_size: value.buffer.len(),
            },
            _ => todo!(),
        }
    }
}

impl From<SerialType> for u64 {
    fn from(serial_type: SerialType) -> Self {
        match serial_type {
            SerialType::Null => 0,
            SerialType::I8 => 1,
            SerialType::I16 => 2,
            SerialType::I24 => 3,
            SerialType::I32 => 4,
            SerialType::I48 => 5,
            SerialType::I64 => 6,
            SerialType::F64 => 7,
            SerialType::Text { content_size } => (content_size * 2 + 13) as u64,
            SerialType::Blob { content_size } => (content_size * 2 + 12) as u64,
        }
    }
}

impl Record {
    pub fn new(values: Vec<OwnedValue>) -> Self {
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
            match value.value_type {
                OwnedValueType::Null => {}
                OwnedValueType::Integer => {
                    let i = value.as_integer().unwrap();
                    let serial_type = SerialType::from(value);
                    match serial_type {
                        SerialType::I8 => buf.extend_from_slice(&(i as i8).to_be_bytes()),
                        SerialType::I16 => buf.extend_from_slice(&(i as i16).to_be_bytes()),
                        SerialType::I24 => buf.extend_from_slice(&(i as i32).to_be_bytes()[1..]), // remove most significant byte
                        SerialType::I32 => buf.extend_from_slice(&(i as i32).to_be_bytes()),
                        SerialType::I48 => buf.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialType::I64 => buf.extend_from_slice(&i.to_be_bytes()),
                        _ => unreachable!(),
                    }
                }
                OwnedValueType::Float => {
                    let f = value.as_float().unwrap();
                    buf.extend_from_slice(&f.to_be_bytes())
                }
                OwnedValueType::Text => {
                    buf.extend_from_slice(&value.buffer);
                }
                OwnedValueType::Blob => {
                    buf.extend_from_slice(&value.buffer);
                }
                _ => todo!(),
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
    Virtual(VTabOpaqueCursor),
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

    pub fn as_virtual_mut(&mut self) -> &mut VTabOpaqueCursor {
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
    pub fn iteration_direction(&self) -> IterationDirection {
        match self {
            SeekOp::EQ | SeekOp::GE | SeekOp::GT => IterationDirection::Forwards,
            SeekOp::LE | SeekOp::LT => IterationDirection::Backwards,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SeekKey<'a> {
    TableRowId(u64),
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
        let record = Record::new(vec![OwnedValue::null()]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::Null));
        // Check that the buffer is empty after the header
        assert_eq!(buf.len(), header_length);
    }

    #[test]
    fn test_serialize_integers() {
        let record = Record::new(vec![
            OwnedValue::integer(42),                // Should use SERIAL_TYPE_I8
            OwnedValue::integer(1000),              // Should use SERIAL_TYPE_I16
            OwnedValue::integer(1_000_000),         // Should use SERIAL_TYPE_I24
            OwnedValue::integer(1_000_000_000),     // Should use SERIAL_TYPE_I32
            OwnedValue::integer(1_000_000_000_000), // Should use SERIAL_TYPE_I48
            OwnedValue::integer(i64::MAX),          // Should use SERIAL_TYPE_I64
        ]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8); // Header should be larger than number of values

        // Check that correct serial types were chosen
        assert_eq!(header[1] as u64, u64::from(SerialType::I8));
        assert_eq!(header[2] as u64, u64::from(SerialType::I16));
        assert_eq!(header[3] as u64, u64::from(SerialType::I24));
        assert_eq!(header[4] as u64, u64::from(SerialType::I32));
        assert_eq!(header[5] as u64, u64::from(SerialType::I48));
        assert_eq!(header[6] as u64, u64::from(SerialType::I64));

        // test that the bytes after the header can be interpreted as the correct values
        let mut cur_offset = header_length;
        let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
        cur_offset += size_of::<i8>();
        let i16_bytes = &buf[cur_offset..cur_offset + size_of::<i16>()];
        cur_offset += size_of::<i16>();
        let i24_bytes = &buf[cur_offset..cur_offset + size_of::<i32>() - 1];
        cur_offset += size_of::<i32>() - 1; // i24
        let i32_bytes = &buf[cur_offset..cur_offset + size_of::<i32>()];
        cur_offset += size_of::<i32>();
        let i48_bytes = &buf[cur_offset..cur_offset + size_of::<i64>() - 2];
        cur_offset += size_of::<i64>() - 2; // i48
        let i64_bytes = &buf[cur_offset..cur_offset + size_of::<i64>()];

        let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
        let val_int16 = i16::from_be_bytes(i16_bytes.try_into().unwrap());

        let mut leading_0 = vec![0];
        leading_0.extend(i24_bytes);
        let val_int24 = i32::from_be_bytes(leading_0.try_into().unwrap());

        let val_int32 = i32::from_be_bytes(i32_bytes.try_into().unwrap());

        let mut leading_00 = vec![0, 0];
        leading_00.extend(i48_bytes);
        let val_int48 = i64::from_be_bytes(leading_00.try_into().unwrap());

        let val_int64 = i64::from_be_bytes(i64_bytes.try_into().unwrap());

        assert_eq!(val_int8, 42);
        assert_eq!(val_int16, 1000);
        assert_eq!(val_int24, 1_000_000);
        assert_eq!(val_int32, 1_000_000_000);
        assert_eq!(val_int48, 1_000_000_000_000);
        assert_eq!(val_int64, i64::MAX);

        // assert correct size of buffer: header + values (bytes per value depends on serial type)
        assert_eq!(
            buf.len(),
            header_length
                + size_of::<i8>()
                + size_of::<i16>()
                + (size_of::<i32>() - 1) // i24
                + size_of::<i32>()
                + (size_of::<i64>() - 2) // i48
                + size_of::<f64>()
        );
    }

    #[test]
    fn test_serialize_float() {
        #[warn(clippy::approx_constant)]
        let record = Record::new(vec![OwnedValue::float(3.15555)]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for FLOAT
        assert_eq!(header[1] as u64, u64::from(SerialType::F64));
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
        let record = Record::new(vec![OwnedValue {
            buffer: text.as_bytes().to_vec(),
            value_type: OwnedValueType::Text,
            text_subtype: Some(TextSubtype::Text),
        }]);
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
        let record = Record::new(vec![OwnedValue::from_blob(blob.clone())]);
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
            OwnedValue::null(),
            OwnedValue::integer(42),
            OwnedValue::float(3.15),
            OwnedValue {
                buffer: text.as_bytes().to_vec(),
                value_type: OwnedValueType::Text,
                text_subtype: Some(TextSubtype::Text),
            },
        ]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::Null));
        // Third byte should be serial type for I8
        assert_eq!(header[2] as u64, u64::from(SerialType::I8));
        // Fourth byte should be serial type for F64
        assert_eq!(header[3] as u64, u64::from(SerialType::F64));
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
