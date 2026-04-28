//! Shared Hrana pipeline protocol types for the Turso ecosystem.
//!
//! This crate defines the wire-format types used by the hrana v2/v3 pipeline
//! protocol. It is consumed by the serverless driver, sync engine, and the
//! validating proxy.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Pipeline request / response
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PipelineRequest {
    pub baton: Option<String>,
    pub requests: Vec<StreamRequest>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PipelineResponse {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<StreamResult>,
}

// ---------------------------------------------------------------------------
// Stream request
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    #[serde(skip_deserializing)]
    #[default]
    None,
    Execute(ExecuteStreamReq),
    Batch(BatchStreamReq),
    Sequence(SequenceStreamReq),
    Close,
    Describe(DescribeStreamReq),
    StoreSql(StoreSqlStreamReq),
    CloseSql(CloseSqlStreamReq),
    GetAutocommit,
}

impl StreamRequest {
    /// Extract SQL strings from this request.
    pub fn sql_strings(&self) -> Vec<&str> {
        match self {
            StreamRequest::Execute(req) => {
                req.stmt.sql.as_deref().map(|s| vec![s]).unwrap_or_default()
            }
            StreamRequest::Batch(req) => req
                .batch
                .steps
                .iter()
                .filter_map(|s| s.stmt.sql.as_deref())
                .collect(),
            StreamRequest::Sequence(req) => req.sql.as_deref().map(|s| vec![s]).unwrap_or_default(),
            StreamRequest::Describe(req) => req.sql.as_deref().map(|s| vec![s]).unwrap_or_default(),
            StreamRequest::StoreSql(req) => vec![req.sql.as_str()],
            StreamRequest::None
            | StreamRequest::Close
            | StreamRequest::CloseSql(_)
            | StreamRequest::GetAutocommit => vec![],
        }
    }

    pub fn is_close(&self) -> bool {
        matches!(self, StreamRequest::Close)
    }
}

// ---------------------------------------------------------------------------
// Stream result / response
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    #[default]
    None,
    Ok {
        response: StreamResponse,
    },
    Error {
        error: Error,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Execute(ExecuteStreamResp),
    Batch(BatchStreamResp),
    Describe(DescribeStreamResp),
    Close {},
    Sequence {},
    StoreSql {},
    CloseSql {},
    GetAutocommit(GetAutocommitStreamResp),
}

// ---------------------------------------------------------------------------
// Request sub-types
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ExecuteStreamReq {
    pub stmt: Stmt,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BatchStreamReq {
    pub batch: Batch,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SequenceStreamReq {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DescribeStreamReq {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StoreSqlStreamReq {
    pub sql_id: i32,
    pub sql: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CloseSqlStreamReq {
    pub sql_id: i32,
}

// ---------------------------------------------------------------------------
// Response sub-types
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ExecuteStreamResp {
    pub result: StmtResult,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct BatchStreamResp {
    pub result: BatchResult,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct DescribeStreamResp {
    pub result: DescribeResult,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct GetAutocommitStreamResp {
    pub is_autocommit: bool,
}

// ---------------------------------------------------------------------------
// Statement / batch
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Stmt {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    #[serde(default)]
    pub want_rows: Option<bool>,
    #[serde(default, with = "option_u64_as_str")]
    pub replication_index: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Batch {
    pub steps: Vec<BatchStep>,
    #[serde(default, with = "option_u64_as_str")]
    pub replication_index: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BatchStep {
    pub stmt: Stmt,
    #[serde(default)]
    pub condition: Option<BatchCond>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchCond {
    #[serde(skip_deserializing)]
    #[default]
    None,
    Ok {
        step: u32,
    },
    Error {
        step: u32,
    },
    Not {
        cond: Box<BatchCond>,
    },
    And(BatchCondList),
    Or(BatchCondList),
    IsAutocommit {},
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BatchCondList {
    pub conds: Vec<BatchCond>,
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct StmtResult {
    pub cols: Vec<Col>,
    pub rows: Vec<Row>,
    #[serde(default)]
    pub affected_row_count: u64,
    #[serde(default, with = "option_i64_as_str")]
    pub last_insert_rowid: Option<i64>,
    #[serde(default, with = "option_u64_as_str")]
    pub replication_index: Option<u64>,
    #[serde(default)]
    pub rows_read: u64,
    #[serde(default)]
    pub rows_written: u64,
    #[serde(default)]
    pub query_duration_ms: f64,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Col {
    pub name: Option<String>,
    pub decltype: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(transparent)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct BatchResult {
    pub step_results: Vec<Option<StmtResult>>,
    pub step_errors: Vec<Option<Error>>,
    #[serde(default, with = "option_u64_as_str")]
    pub replication_index: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct DescribeResult {
    pub params: Vec<DescribeParam>,
    pub cols: Vec<Col>,
    pub is_explain: bool,
    pub is_readonly: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct DescribeParam {
    pub name: Option<String>,
}

// ---------------------------------------------------------------------------
// Value / error / named arg
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Value {
    #[serde(skip_deserializing)]
    #[default]
    None,
    Null,
    Integer {
        #[serde(with = "i64_as_str")]
        value: i64,
    },
    Float {
        value: f64,
    },
    Text {
        value: String,
    },
    Blob {
        #[serde(with = "bytes_as_base64", rename = "base64")]
        value: Bytes,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NamedArg {
    pub name: String,
    pub value: Value,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Error {
    pub message: String,
    #[serde(default)]
    pub code: String,
}

// ---------------------------------------------------------------------------
// Cursor types (streaming NDJSON)
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CursorRequest {
    pub baton: Option<String>,
    pub batch: CursorBatch,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CursorBatch {
    pub steps: Vec<BatchStep>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CursorResponse {
    pub baton: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum CursorEntry {
    #[serde(rename = "step_begin")]
    StepBegin {
        #[serde(default)]
        step: usize,
        cols: Option<Vec<Col>>,
    },
    #[serde(rename = "step_end")]
    StepEnd {
        #[serde(default)]
        step: usize,
        affected_row_count: Option<u64>,
        #[serde(default, with = "option_i64_as_str")]
        last_insert_rowid: Option<i64>,
    },
    #[serde(rename = "step_error")]
    StepError {
        #[serde(default)]
        step: usize,
        error: Option<Error>,
    },
    #[serde(rename = "row")]
    Row {
        #[serde(default)]
        step: usize,
        row: Option<Vec<Value>>,
    },
    #[serde(rename = "error")]
    Error { error: Option<Error> },
    #[serde(rename = "replication_index")]
    ReplicationIndex { replication_index: Option<u64> },
}

// ---------------------------------------------------------------------------
// Serde helper modules
// ---------------------------------------------------------------------------

pub mod i64_as_str {
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &i64, ser: S) -> Result<S::Ok, S::Error> {
        value.to_string().serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<i64, D::Error> {
        let str_value = <&'de str as de::Deserialize>::deserialize(de)?;
        str_value.parse().map_err(|_| {
            D::Error::invalid_value(
                de::Unexpected::Str(str_value),
                &"decimal integer as a string",
            )
        })
    }
}

pub mod option_i64_as_str {
    use serde::de::{Error, Visitor};
    use serde::{ser, Deserializer, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Option<i64>, ser: S) -> Result<S::Ok, S::Error> {
        value.map(|v| v.to_string()).serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Option<i64>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a string representing a signed integer, or null")
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(V)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Some(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                v.parse().map_err(E::custom).map(Some)
            }
        }

        d.deserialize_option(V)
    }
}

pub mod option_u64_as_str {
    use serde::de::Error;
    use serde::{de::Visitor, ser, Deserializer, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Option<u64>, ser: S) -> Result<S::Ok, S::Error> {
        value.map(|v| v.to_string()).serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Option<u64>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a string representing an integer, or null")
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(V)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Some(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                v.parse().map_err(E::custom).map(Some)
            }
        }

        d.deserialize_option(V)
    }

    #[cfg(test)]
    mod test {
        use serde::Deserialize;

        #[test]
        fn deserialize_ok() {
            #[derive(Deserialize)]
            struct Test {
                #[serde(with = "super")]
                value: Option<u64>,
            }

            let json = r#"{"value": null }"#;
            let val: Test = serde_json::from_str(json).unwrap();
            assert!(val.value.is_none());

            let json = r#"{"value": "124" }"#;
            let val: Test = serde_json::from_str(json).unwrap();
            assert_eq!(val.value.unwrap(), 124);

            let json = r#"{"value": 124 }"#;
            let val: Test = serde_json::from_str(json).unwrap();
            assert_eq!(val.value.unwrap(), 124);
        }
    }
}

pub mod bytes_as_base64 {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
    use bytes::Bytes;
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        STANDARD_NO_PAD.encode(value).serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        let text = <&'de str as de::Deserialize>::deserialize(de)?;
        let text = text.trim_end_matches('=');
        let bytes = STANDARD_NO_PAD.decode(text).map_err(|_| {
            D::Error::invalid_value(de::Unexpected::Str(text), &"binary data encoded as base64")
        })?;
        Ok(Bytes::from(bytes))
    }
}

pub mod bytes_as_base64_pad {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use bytes::Bytes;
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        STANDARD.encode(value).serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        let text = <&'de str as de::Deserialize>::deserialize(de)?;
        let bytes = STANDARD.decode(text).map_err(|_| {
            D::Error::invalid_value(de::Unexpected::Str(text), &"binary data encoded as base64")
        })?;
        Ok(Bytes::from(bytes))
    }
}
