use std::fmt::Display;

/// Error type is of type ExtError which can be
/// either a user defined error or an error code
#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ResultCode {
    OK = 0,
    Error = 1,
    InvalidArgs = 2,
    Unknown = 3,
    OoM = 4,
    Corrupt = 5,
    NotFound = 6,
    AlreadyExists = 7,
    PermissionDenied = 8,
    Aborted = 9,
    OutOfRange = 10,
    Unimplemented = 11,
    Internal = 12,
    Unavailable = 13,
    CustomError = 14,
    EOF = 15,
    ReadOnly = 16,
    RowID = 17,
}

impl ResultCode {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::OK)
    }

    pub fn is_error(&self) -> bool {
        !matches!(self, Self::OK | Self::RowID | Self::EOF)
    }

    pub fn has_error_set(&self) -> bool {
        matches!(self, Self::CustomError)
    }
}

impl Display for ResultCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultCode::OK => write!(f, "OK"),
            ResultCode::Error => write!(f, "Error"),
            ResultCode::InvalidArgs => write!(f, "Invalid Argument"),
            ResultCode::Unknown => write!(f, "Unknown"),
            ResultCode::OoM => write!(f, "Out of Memory"),
            ResultCode::Corrupt => write!(f, "Corrupt"),
            ResultCode::NotFound => write!(f, "Not Found"),
            ResultCode::AlreadyExists => write!(f, "Already Exists"),
            ResultCode::PermissionDenied => write!(f, "Permission Denied"),
            ResultCode::Aborted => write!(f, "Aborted"),
            ResultCode::OutOfRange => write!(f, "Out of Range"),
            ResultCode::Unimplemented => write!(f, "Unimplemented"),
            ResultCode::Internal => write!(f, "Internal Error"),
            ResultCode::Unavailable => write!(f, "Unavailable"),
            ResultCode::CustomError => write!(f, "Error "),
            ResultCode::EOF => write!(f, "EOF"),
            ResultCode::ReadOnly => write!(f, "Read Only"),
            ResultCode::RowID => write!(f, "RowID"),
        }
    }
}

#[repr(C)]
#[derive(PartialEq, Debug, Eq, Clone, Copy)]
pub enum ValueType {
    Null,
    Integer,
    Float,
    Text,
    Blob,
    Error,
}

#[repr(C)]
pub struct Value {
    value_type: ValueType,
    value: ValueData,
}

#[repr(C)]
union ValueData {
    int: i64,
    float: f64,
    text: *const TextValue,
    blob: *const Blob,
    error: *const ErrValue,
}

#[repr(C)]
#[derive(PartialEq, Clone, Copy, Debug)]
enum TextSubtype {
    Text,
    Json,
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value_type {
            ValueType::Null => write!(f, "Value {{ Null }}"),
            ValueType::Integer => write!(
                f,
                "Value {{ Integer: {} }}",
                self.to_integer().unwrap_or_default()
            ),
            ValueType::Float => write!(
                f,
                "Value {{ Float: {} }}",
                self.to_float().unwrap_or_default()
            ),
            ValueType::Text => write!(f, "Value {{ Text: {:?} }}", self.to_text()),
            ValueType::Blob => write!(f, "Value {{ Blob: {:?} }}", self.to_blob()),
            ValueType::Error => write!(f, "Value {{ Error }}"),
        }
    }
}

#[repr(C)]
pub struct TextValue {
    _type: TextSubtype,
    text: *const u8,
    len: u32,
}

impl std::fmt::Debug for TextValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TextValue {{ text: {:?}, len: {} }}",
            self.text, self.len
        )
    }
}

impl Default for TextValue {
    fn default() -> Self {
        Self {
            _type: TextSubtype::Text,
            text: std::ptr::null(),
            len: 0,
        }
    }
}

impl TextValue {
    fn new(text: *const u8, len: usize) -> Self {
        Self {
            _type: TextSubtype::Text,
            text,
            len: len as u32,
        }
    }

    fn new_boxed(s: impl Into<Box<str>>, sub: TextSubtype) -> Box<Self> {
        let buffer: Box<str> = s.into();
        let len = buffer.len();
        let strbox = Box::into_raw(buffer);
        Box::new(Self {
            _type: sub,
            text: strbox as *const u8,
            len: len as u32,
        })
    }

    #[cfg(feature = "core_only")]
    fn free(self) {
        if !self.text.is_null() {
            let _ = unsafe { Box::from_raw(self.text as *mut u8) };
        }
    }

    fn as_str(&self) -> &str {
        if self.text.is_null() {
            return "";
        }
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(self.text, self.len as usize))
        }
    }
}

#[repr(C)]
struct ErrValue {
    code: ResultCode,
    message: *mut TextValue,
}

impl ErrValue {
    fn new(code: ResultCode) -> Self {
        Self {
            code,
            message: std::ptr::null_mut(),
        }
    }

    fn new_with_message(code: ResultCode, message: String) -> Self {
        let buffer = message.into_boxed_str();
        let ptr = buffer.as_ptr();
        let len = buffer.len();
        std::mem::forget(buffer);
        let text_value = TextValue::new(ptr, len);
        Self {
            code,
            message: Box::into_raw(Box::new(text_value)),
        }
    }
}

#[repr(C)]
struct Blob {
    data: *const u8,
    size: u64,
}

impl std::fmt::Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blob {{ data: {:?}, size: {} }}", self.data, self.size)
    }
}

impl Blob {
    pub fn new(data: *const u8, size: u64) -> Self {
        Self { data, size }
    }

    pub fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.data, self.size as usize) }
    }
    #[cfg(feature = "core_only")]
    fn free(self) {
        if !self.data.is_null() {
            let _ = unsafe { Box::from_raw(self.data as *mut u8) };
        }
    }
}

impl Value {
    /// Creates a new Value with type Null
    pub fn null() -> Self {
        Self {
            value_type: ValueType::Null,
            value: ValueData { int: 0 },
        }
    }

    /// Returns the value type of the Value
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    /// Returns the float value or casts the relevant value to a float
    pub fn to_float(&self) -> Option<f64> {
        match self.value_type {
            ValueType::Float => Some(unsafe { self.value.float }),
            ValueType::Integer => Some(unsafe { self.value.int } as f64),
            ValueType::Text => {
                let txt = self.to_text().unwrap_or_default();
                txt.parse().ok()
            }
            _ => None,
        }
    }

    /// Returns the text value if the Value is the proper type
    pub fn to_text(&self) -> Option<&str> {
        unsafe {
            if self.value_type == ValueType::Text && !self.value.text.is_null() {
                let txt = &*self.value.text;
                Some(txt.as_str())
            } else {
                None
            }
        }
    }

    pub fn is_json(&self) -> bool {
        unsafe {
            if self.value_type == ValueType::Text && !self.value.text.is_null() {
                let txt = &*self.value.text;
                return txt._type == TextSubtype::Json;
            }
        }
        false
    }

    /// Returns the blob value if the ValueType is Blob
    pub fn to_blob(&self) -> Option<Vec<u8>> {
        match self.value_type {
            ValueType::Blob => {
                if unsafe { self.value.blob.is_null() } {
                    return None;
                }
                let blob = unsafe { &*(self.value.blob) };
                let slice = unsafe { std::slice::from_raw_parts(blob.data, blob.size as usize) };
                Some(slice.to_vec())
            }
            ValueType::Text => {
                let txt = self.to_text().unwrap_or_default();
                Some(txt.as_bytes().to_vec())
            }
            _ => None,
        }
    }

    /// Returns the value as an integer (casts or converts if possible)
    pub fn to_integer(&self) -> Option<i64> {
        match self.value_type() {
            ValueType::Integer => Some(unsafe { self.value.int }),
            ValueType::Float => Some(unsafe { self.value.float } as i64),
            ValueType::Text => self
                .to_text()
                .map(|txt| txt.parse::<i64>().unwrap_or_default()),
            _ => None,
        }
    }

    /// Returns the error code if the value is an error
    pub fn to_error(&self) -> Option<ResultCode> {
        if self.value_type != ValueType::Error {
            return None;
        }
        if unsafe { self.value.error.is_null() } {
            return None;
        }
        let err = unsafe { &*self.value.error };
        Some(err.code)
    }

    /// Returns the error code and optional message if the value is an error
    pub fn to_error_details(&self) -> Option<(ResultCode, Option<String>)> {
        if self.value_type != ValueType::Error || unsafe { self.value.error.is_null() } {
            return None;
        }
        let err_val = unsafe { &*(self.value.error) };
        let code = err_val.code;

        if err_val.message.is_null() {
            Some((code, None))
        } else {
            let txt = unsafe { &*(err_val.message as *const TextValue) };
            let msg = txt.as_str().to_owned();
            Some((code, Some(msg)))
        }
    }

    // Return ValueData as raw bytes
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        unsafe {
            match self.value_type {
                ValueType::Integer => bytes.extend_from_slice(&self.value.int.to_le_bytes()),
                ValueType::Float => bytes.extend_from_slice(&self.value.float.to_le_bytes()),
                ValueType::Text => {
                    let text = self.value.text.as_ref().expect("Invalid text pointer");
                    bytes.extend_from_slice(text.as_str().as_bytes());
                }
                ValueType::Blob => {
                    let blob = self.value.blob.as_ref().expect("Invalid blob pointer");
                    bytes.extend_from_slice(blob.as_bytes());
                }
                ValueType::Error | ValueType::Null => {}
            }
        }

        bytes
    }

    /// Creates a new integer Value from an i64
    pub fn from_integer(i: i64) -> Self {
        Self {
            value_type: ValueType::Integer,
            value: ValueData { int: i },
        }
    }

    /// Creates a new float Value from a f64
    pub fn from_float(value: f64) -> Self {
        Self {
            value_type: ValueType::Float,
            value: ValueData { float: value },
        }
    }

    /// Creates a new text Value from a String
    pub fn from_text(s: impl Into<Box<str>>) -> Self {
        let txt_value = TextValue::new_boxed(s, TextSubtype::Text);
        let ptr = Box::into_raw(txt_value);
        Self {
            value_type: ValueType::Text,
            value: ValueData { text: ptr },
        }
    }

    pub fn from_json<S>(s: S) -> Self
    where
        S: Into<Box<str>> + ExactSizeIterator,
    {
        let txt_value = TextValue::new_boxed(s, TextSubtype::Json);
        let ptr = Box::into_raw(txt_value);
        Self {
            value_type: ValueType::Text,
            value: ValueData { text: ptr },
        }
    }

    /// Creates a new error Value from a ResultCode
    pub fn error(code: ResultCode) -> Self {
        let err_val = ErrValue::new(code);
        Self {
            value_type: ValueType::Error,
            value: ValueData {
                error: Box::into_raw(Box::new(err_val)) as *const ErrValue,
            },
        }
    }

    /// Creates a new error Value from a ResultCode and a message
    pub fn error_with_message(message: String) -> Self {
        let err_value = ErrValue::new_with_message(ResultCode::CustomError, message);
        let err_box = Box::new(err_value);
        Self {
            value_type: ValueType::Error,
            value: ValueData {
                error: Box::into_raw(err_box) as *const ErrValue,
            },
        }
    }

    /// Creates a new blob Value from a Vec<u8>
    pub fn from_blob(value: Vec<u8>) -> Self {
        let len = value.len();
        let boxed_data = value.into_boxed_slice();
        let ptr = Box::into_raw(boxed_data) as *const u8;
        let boxed_blob = Box::new(Blob::new(ptr, len as u64));
        Self {
            value_type: ValueType::Blob,
            value: ValueData {
                blob: Box::into_raw(boxed_blob) as *const Blob,
            },
        }
    }

    /// Extension authors should __not__ use this function, or enable the 'core_only' feature
    ///
    /// # Safety
    /// consumes the value while freeing the underlying memory with null check.
    /// however this does assume that the type was properly constructed with
    /// the appropriate value_type and value.
    #[cfg(feature = "core_only")]
    pub unsafe fn __free_internal_type(self) {
        match self.value_type {
            ValueType::Text => {
                let txt = Box::from_raw(self.value.text as *mut TextValue);
                txt.free();
            }
            ValueType::Blob => {
                let blob = Box::from_raw(self.value.blob as *mut Blob);
                blob.free();
            }
            ValueType::Error => {
                let err_val = Box::from_raw(self.value.error as *mut ErrValue);
                if !err_val.message.is_null() {
                    let _ = Box::from_raw(err_val.message);
                }
            }
            _ => {}
        }
    }
}
