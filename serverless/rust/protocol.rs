//! Low-level wire types for the SQL over HTTP protocol, as specified in
//! `serverless/PROTOCOL.md`.
//!
//! Most users never need this module: the [`crate::Connection`] API covers
//! normal use. It is public for tooling that needs to speak the protocol
//! directly.

use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::{Error, Result, Value};

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
mod tests {
    use super::*;

    #[test]
    fn integer_encodes_as_decimal_string() {
        let encoded = encode_value(&Value::Integer(i64::MAX)).unwrap();
        let json = serde_json::to_value(&encoded).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "integer", "value": "9223372036854775807"})
        );
    }

    #[test]
    fn integer_roundtrip_extremes() {
        for n in [i64::MIN, -1, 0, 1, i64::MAX] {
            let decoded = decode_value(&encode_value(&Value::Integer(n)).unwrap()).unwrap();
            assert_eq!(decoded, Value::Integer(n));
        }
    }

    #[test]
    fn float_encodes_as_json_number() {
        let json = serde_json::to_value(encode_value(&Value::Real(1.5)).unwrap()).unwrap();
        assert_eq!(json, serde_json::json!({"type": "float", "value": 1.5}));
    }

    #[test]
    fn nan_encodes_as_null() {
        let encoded = encode_value(&Value::Real(f64::NAN)).unwrap();
        assert_eq!(encoded, ProtoValue::Null);
    }

    #[test]
    fn infinity_is_rejected() {
        assert!(encode_value(&Value::Real(f64::INFINITY)).is_err());
        assert!(encode_value(&Value::Real(f64::NEG_INFINITY)).is_err());
    }

    #[test]
    fn null_float_decodes_as_nan() {
        let decoded = decode_value(&ProtoValue::Float { value: None }).unwrap();
        match decoded {
            Value::Real(f) => assert!(f.is_nan()),
            other => panic!("expected Real, got {other:?}"),
        }
    }

    #[test]
    fn blob_decodes_unpadded_and_padded_base64() {
        for b64 in ["3q2+7w", "3q2+7w=="] {
            let decoded = decode_value(&ProtoValue::Blob {
                base64: b64.to_string(),
            })
            .unwrap();
            assert_eq!(decoded, Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]));
        }
    }

    #[test]
    fn blob_roundtrip() {
        for blob in [vec![], vec![0u8, 1, 2, 255, 128, 64]] {
            let decoded = decode_value(&encode_value(&Value::Blob(blob.clone())).unwrap()).unwrap();
            assert_eq!(decoded, Value::Blob(blob));
        }
    }

    #[test]
    fn stmt_omits_unset_sql_id() {
        let stmt = Stmt::new("SELECT 1", true);
        let json = serde_json::to_value(&stmt).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "sql": "SELECT 1",
                "args": [],
                "named_args": [],
                "want_rows": true,
            })
        );
    }

    #[test]
    fn request_types_serialize_with_type_tag() {
        let json = serde_json::to_value(StreamRequest::GetAutocommit).unwrap();
        assert_eq!(json, serde_json::json!({"type": "get_autocommit"}));
        let json = serde_json::to_value(StreamRequest::Close).unwrap();
        assert_eq!(json, serde_json::json!({"type": "close"}));
        let json = serde_json::to_value(StreamRequest::StoreSql {
            sql_id: 1,
            sql: "SELECT 1".to_string(),
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "store_sql", "sql_id": 1, "sql": "SELECT 1"})
        );
    }

    #[test]
    fn batch_condition_serialization() {
        let cond = BatchCond::And {
            conds: vec![
                BatchCond::Ok { step: 0 },
                BatchCond::Not {
                    cond: Box::new(BatchCond::Error { step: 1 }),
                },
                BatchCond::IsAutocommit,
            ],
        };
        let json = serde_json::to_value(&cond).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "and",
                "conds": [
                    {"type": "ok", "step": 0},
                    {"type": "not", "cond": {"type": "error", "step": 1}},
                    {"type": "is_autocommit"},
                ],
            })
        );
    }

    #[test]
    fn cursor_entry_parses_spec_examples() {
        let entry: CursorEntry = serde_json::from_str(
            r#"{ "type": "step_begin", "step": 0, "cols": [ { "name": "id", "decltype": "INTEGER" } ] }"#,
        )
        .unwrap();
        assert!(matches!(entry, CursorEntry::StepBegin { step: 0, .. }));

        let entry: CursorEntry = serde_json::from_str(
            r#"{ "type": "row", "row": [ { "type": "integer", "value": "1" } ] }"#,
        )
        .unwrap();
        assert!(matches!(entry, CursorEntry::Row { .. }));

        let entry: CursorEntry = serde_json::from_str(
            r#"{ "type": "step_end", "affected_row_count": 0, "last_insert_rowid": null }"#,
        )
        .unwrap();
        assert!(matches!(
            entry,
            CursorEntry::StepEnd {
                affected_row_count: 0,
                last_insert_rowid: None,
            }
        ));

        let entry: CursorEntry =
            serde_json::from_str(r#"{ "type": "replication_index", "replication_index": null }"#)
                .unwrap();
        assert!(matches!(entry, CursorEntry::ReplicationIndex {}));
    }

    #[test]
    fn cursor_rowid_accepts_number_and_string() {
        let entry: CursorEntry = serde_json::from_str(
            r#"{ "type": "step_end", "affected_row_count": 1, "last_insert_rowid": 42 }"#,
        )
        .unwrap();
        match entry {
            CursorEntry::StepEnd {
                last_insert_rowid: Some(rowid),
                ..
            } => assert_eq!(rowid.to_i64().unwrap(), 42),
            other => panic!("unexpected entry: {other:?}"),
        }

        let entry: CursorEntry = serde_json::from_str(
            r#"{ "type": "step_end", "affected_row_count": 1, "last_insert_rowid": "42" }"#,
        )
        .unwrap();
        match entry {
            CursorEntry::StepEnd {
                last_insert_rowid: Some(rowid),
                ..
            } => assert_eq!(rowid.to_i64().unwrap(), 42),
            other => panic!("unexpected entry: {other:?}"),
        }
    }

    #[test]
    fn unknown_cursor_entry_types_are_ignored() {
        let entry: CursorEntry =
            serde_json::from_str(r#"{ "type": "some_future_entry", "data": 1 }"#).unwrap();
        assert!(matches!(entry, CursorEntry::Unknown));
    }

    #[test]
    fn pipeline_response_parses_spec_example() {
        let response: PipelineResponse = serde_json::from_str(
            r#"{
              "baton": null,
              "base_url": null,
              "results": [
                {
                  "type": "ok",
                  "response": {
                    "type": "execute",
                    "result": {
                      "cols": [
                        { "name": "id", "decltype": "INTEGER" },
                        { "name": "name", "decltype": "TEXT" }
                      ],
                      "rows": [
                        [ { "type": "integer", "value": "1" }, { "type": "text", "value": "Alice" } ]
                      ],
                      "affected_row_count": 0,
                      "last_insert_rowid": null,
                      "rows_read": 1,
                      "rows_written": 0,
                      "query_duration_ms": 0.2
                    }
                  }
                },
                { "type": "ok", "response": { "type": "close" } }
              ]
            }"#,
        )
        .unwrap();
        assert!(response.baton.is_none());
        assert_eq!(response.results.len(), 2);
        match &response.results[0] {
            StreamResult::Ok {
                response: StreamResponse::Execute { result },
            } => {
                assert_eq!(result.cols.len(), 2);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.affected_row_count, 0);
                assert!(result.last_insert_rowid.is_none());
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert!(matches!(
            response.results[1],
            StreamResult::Ok {
                response: StreamResponse::Close
            }
        ));
    }

    #[test]
    fn error_result_parses_with_extended_code() {
        let result: StreamResult = serde_json::from_str(
            r#"{
              "type": "error",
              "error": {
                "message": "UNIQUE constraint failed: users.email",
                "code": "SQLITE_CONSTRAINT",
                "extended_code": "SQLITE_CONSTRAINT_UNIQUE"
              }
            }"#,
        )
        .unwrap();
        match result {
            StreamResult::Error { error } => {
                assert_eq!(error.code.as_deref(), Some("SQLITE_CONSTRAINT"));
                assert_eq!(
                    error.extended_code.as_deref(),
                    Some("SQLITE_CONSTRAINT_UNIQUE")
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
