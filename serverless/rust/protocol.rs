//! Low-level wire types for the [SQL over HTTP
//! protocol](https://github.com/tursodatabase/turso/blob/main/serverless/PROTOCOL.md).
//!
//! Most users never need this module: the [`crate::Connection`] API covers
//! normal use. It is public for tooling that needs to speak the protocol
//! directly.

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::{Error, Result, Value};

/// HTTP header carrying the remote encryption key (section 3.1).
pub const ENCRYPTION_KEY_HEADER: &str = "x-turso-encryption-key";

/// A SQL value on the wire (section 8.2).
///
/// Integers are transported as decimal strings because JSON numbers cannot
/// represent the full 64-bit range faithfully. Blobs are base64; the server
/// emits unpadded base64 and accepts both padded and unpadded input.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProtoValue {
    Null,
    Integer { value: String },
    Float { value: Option<f64> },
    Text { value: String },
    Blob { base64: String },
}

pub fn encode_value(value: &Value) -> Result<ProtoValue> {
    Ok(match value {
        Value::Null => ProtoValue::Null,
        Value::Integer(n) => ProtoValue::Integer {
            value: n.to_string(),
        },
        Value::Real(f) => {
            if f.is_nan() {
                // SQLite binds NaN as NULL; JSON cannot carry it.
                ProtoValue::Null
            } else if f.is_infinite() {
                return Err(Error::ToSqlConversionFailure(
                    "infinite float values cannot be sent over the protocol".into(),
                ));
            } else {
                ProtoValue::Float { value: Some(*f) }
            }
        }
        Value::Text(s) => ProtoValue::Text { value: s.clone() },
        Value::Blob(b) => ProtoValue::Blob {
            base64: base64::engine::general_purpose::STANDARD.encode(b),
        },
    })
}

/// Like [`decode_value`], but consumes the protocol value so text is moved
/// rather than cloned. Preferred when the response is owned.
pub fn decode_value_owned(value: ProtoValue) -> Result<Value> {
    Ok(match value {
        ProtoValue::Text { value } => Value::Text(value),
        ProtoValue::Blob { .. } | ProtoValue::Null | ProtoValue::Integer { .. } => {
            decode_value(&value)?
        }
        ProtoValue::Float { value } => Value::Real(value.unwrap_or(f64::NAN)),
    })
}

pub fn decode_value(value: &ProtoValue) -> Result<Value> {
    Ok(match value {
        ProtoValue::Null => Value::Null,
        ProtoValue::Integer { value } => {
            Value::Integer(value.parse::<i64>().map_err(|e| {
                Error::Error(format!("invalid integer value in server response: {e}"))
            })?)
        }
        // A null float encodes a non-finite value; the spec says to decode
        // it as NaN.
        ProtoValue::Float { value } => Value::Real(value.unwrap_or(f64::NAN)),
        ProtoValue::Text { value } => Value::Text(value.clone()),
        ProtoValue::Blob { base64 } => {
            let unpadded = base64.trim_end_matches('=');
            let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(unpadded)
                .map_err(|e| Error::Error(format!("invalid base64 in server response: {e}")))?;
            Value::Blob(bytes)
        }
    })
}

/// A named argument (section 8.1).
#[derive(Serialize, Debug, Clone)]
pub struct NamedArg {
    pub name: String,
    pub value: ProtoValue,
}

/// A statement together with its arguments (section 8.1).
#[derive(Serialize, Debug, Clone)]
pub struct Stmt {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_id: Option<i32>,
    pub args: Vec<ProtoValue>,
    pub named_args: Vec<NamedArg>,
    pub want_rows: bool,
}

impl Stmt {
    pub fn new(sql: impl Into<String>, want_rows: bool) -> Self {
        Self {
            sql: Some(sql.into()),
            sql_id: None,
            args: Vec::new(),
            named_args: Vec::new(),
            want_rows,
        }
    }
}

/// A batch step condition (section 6.2.1).
#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchCond {
    Ok { step: u32 },
    Error { step: u32 },
    Not { cond: Box<BatchCond> },
    And { conds: Vec<BatchCond> },
    Or { conds: Vec<BatchCond> },
    IsAutocommit,
}

/// One step of a batch (section 6.2).
#[derive(Serialize, Debug, Clone)]
pub struct BatchStep {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<BatchCond>,
    pub stmt: Stmt,
}

#[derive(Serialize, Debug, Clone)]
pub struct Batch {
    pub steps: Vec<BatchStep>,
}

/// A request in a pipeline (section 6).
#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    Execute { stmt: Stmt },
    Batch { batch: Batch },
    Sequence { sql: String },
    Describe { sql: String },
    StoreSql { sql_id: i32, sql: String },
    CloseSql { sql_id: i32 },
    GetAutocommit,
    Close,
}

#[derive(Serialize, Debug)]
pub struct PipelineRequest {
    pub baton: Option<String>,
    pub requests: Vec<StreamRequest>,
}

/// An error object (section 9.1).
#[derive(Deserialize, Debug, Clone)]
pub struct ProtoError {
    pub message: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub extended_code: Option<String>,
}

/// A result column (section 8.3).
#[derive(Deserialize, Debug, Clone)]
pub struct ProtoCol {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub decltype: Option<String>,
}

/// A statement result (section 8.4).
#[derive(Deserialize, Debug)]
pub struct StmtResult {
    #[serde(default)]
    pub cols: Vec<ProtoCol>,
    #[serde(default)]
    pub rows: Vec<Vec<ProtoValue>>,
    #[serde(default)]
    pub affected_row_count: u64,
    #[serde(default)]
    pub last_insert_rowid: Option<String>,
    #[serde(default)]
    pub rows_read: Option<u64>,
    #[serde(default)]
    pub rows_written: Option<u64>,
    #[serde(default)]
    pub query_duration_ms: Option<f64>,
}

/// A batch result (section 6.2).
#[derive(Deserialize, Debug)]
pub struct BatchResult {
    pub step_results: Vec<Option<StmtResult>>,
    pub step_errors: Vec<Option<ProtoError>>,
}

/// A describe result (section 6.4).
#[derive(Deserialize, Debug)]
pub struct DescribeResult {
    #[serde(default)]
    pub params: Vec<DescribeParam>,
    #[serde(default)]
    pub cols: Vec<ProtoCol>,
    #[serde(default)]
    pub is_explain: bool,
    #[serde(default)]
    pub is_readonly: bool,
}

#[derive(Deserialize, Debug)]
pub struct DescribeParam {
    #[serde(default)]
    pub name: Option<String>,
}

/// A successful response to one pipeline request (section 5.2).
#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Execute { result: StmtResult },
    Batch { result: BatchResult },
    Sequence,
    Describe { result: DescribeResult },
    StoreSql,
    CloseSql,
    GetAutocommit { is_autocommit: bool },
    Close,
}

/// One entry of a pipeline response's `results` array (section 5.2).
#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: ProtoError },
}

#[derive(Deserialize, Debug)]
pub struct PipelineResponse {
    #[serde(default)]
    pub baton: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub results: Vec<StreamResult>,
}

#[derive(Serialize, Debug)]
pub struct CursorRequest {
    pub baton: Option<String>,
    pub batch: Batch,
}

/// The first line of a cursor response body (section 7.2).
#[derive(Deserialize, Debug)]
pub struct CursorResponse {
    #[serde(default)]
    pub baton: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
}

/// The cursor endpoint emits `last_insert_rowid` as a JSON number, but
/// clients must accept both a number and a decimal string (section 7.2.3).
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum CursorRowid {
    Number(i64),
    String(String),
}

impl CursorRowid {
    pub fn to_i64(&self) -> Result<i64> {
        match self {
            CursorRowid::Number(n) => Ok(*n),
            CursorRowid::String(s) => s
                .parse::<i64>()
                .map_err(|e| Error::Error(format!("invalid rowid in server response: {e}"))),
        }
    }
}

/// A cursor entry (section 7.2). Unknown entry types must be ignored for
/// forward compatibility.
#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CursorEntry {
    StepBegin {
        step: u32,
        #[serde(default)]
        cols: Vec<ProtoCol>,
    },
    Row {
        row: Vec<ProtoValue>,
    },
    StepEnd {
        #[serde(default)]
        affected_row_count: u64,
        #[serde(default)]
        last_insert_rowid: Option<CursorRowid>,
    },
    StepError {
        step: u32,
        error: ProtoError,
    },
    Error {
        error: ProtoError,
    },
    ReplicationIndex {},
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
#[path = "tests/unit/protocol/tests.rs"]
mod tests;
