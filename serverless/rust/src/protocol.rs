use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize};
use crate::Value;

/// Deserialize `last_insert_rowid` which may be a string, integer, or null.
fn deserialize_rowid<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s)),
        Some(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(other) => Ok(Some(other.to_string())),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtoValue {
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base64: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtoColumn {
    pub name: String,
    #[serde(default)]
    pub decltype: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamedArg {
    pub name: String,
    pub value: ProtoValue,
}

#[derive(Debug, Serialize)]
pub struct StmtBody {
    pub sql: String,
    pub args: Vec<ProtoValue>,
    pub named_args: Vec<NamedArg>,
    pub want_rows: bool,
}

#[derive(Debug, Serialize)]
pub struct BatchStep {
    pub stmt: StmtBody,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<StepCondition>,
}

#[derive(Debug, Serialize)]
pub struct StepCondition {
    #[serde(rename = "type")]
    pub typ: String,
    pub step: usize,
}

// --- Cursor (streaming) ---

#[derive(Debug, Serialize)]
pub struct CursorRequest {
    pub baton: Option<String>,
    pub batch: CursorBatch,
}

#[derive(Debug, Serialize)]
pub struct CursorBatch {
    pub steps: Vec<BatchStep>,
}

#[derive(Debug, Deserialize)]
pub struct CursorResponse {
    pub baton: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum CursorEntry {
    #[serde(rename = "step_begin")]
    StepBegin {
        #[allow(dead_code)]
        #[serde(default)]
        step: usize,
        cols: Option<Vec<ProtoColumn>>,
    },
    #[serde(rename = "step_end")]
    StepEnd {
        #[allow(dead_code)]
        #[serde(default)]
        step: usize,
        affected_row_count: Option<u64>,
        #[serde(default, deserialize_with = "deserialize_rowid")]
        last_insert_rowid: Option<String>,
    },
    #[serde(rename = "step_error")]
    StepError {
        #[allow(dead_code)]
        #[serde(default)]
        step: usize,
        error: Option<ProtoError>,
    },
    #[serde(rename = "row")]
    Row {
        #[allow(dead_code)]
        #[serde(default)]
        step: usize,
        row: Option<Vec<ProtoValue>>,
    },
    #[serde(rename = "error")]
    Error { error: Option<ProtoError> },
    #[serde(rename = "replication_index")]
    ReplicationIndex {
        #[allow(dead_code)]
        replication_index: Option<u64>,
    },
}

#[derive(Debug, Deserialize)]
pub struct ProtoError {
    pub message: String,
    #[allow(dead_code)]
    pub code: Option<String>,
}

// --- Pipeline (JSON request/response) ---

#[derive(Debug, Serialize)]
pub struct PipelineRequest {
    pub baton: Option<String>,
    pub requests: Vec<PipelineRequestEntry>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum PipelineRequestEntry {
    #[serde(rename = "sequence")]
    Sequence { sql: String },
    #[serde(rename = "close")]
    Close,
    #[serde(rename = "describe")]
    Describe { sql: String },
}

#[derive(Debug, Deserialize)]
pub struct PipelineResponse {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<PipelineResult>,
}

#[derive(Debug, Deserialize)]
pub struct PipelineResult {
    #[serde(rename = "type")]
    pub typ: String,
    pub response: Option<PipelineResultResponse>,
    pub error: Option<ProtoError>,
}

#[derive(Debug, Deserialize)]
pub struct PipelineResultResponse {
    #[serde(rename = "type")]
    pub typ: String,
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct DescribeResult {
    pub params: Vec<DescribeParam>,
    pub cols: Vec<ProtoColumn>,
    pub is_explain: bool,
    pub is_readonly: bool,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct DescribeParam {
    pub name: Option<String>,
}

// --- Encoding/Decoding ---

pub fn encode_value(value: &Value) -> ProtoValue {
    match value {
        Value::Null => ProtoValue {
            typ: "null".to_string(),
            value: None,
            base64: None,
        },
        Value::Integer(n) => ProtoValue {
            typ: "integer".to_string(),
            value: Some(serde_json::Value::String(n.to_string())),
            base64: None,
        },
        Value::Real(n) => ProtoValue {
            typ: "float".to_string(),
            value: Some(serde_json::Value::Number(
                serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
            )),
            base64: None,
        },
        Value::Text(s) => ProtoValue {
            typ: "text".to_string(),
            value: Some(serde_json::Value::String(s.clone())),
            base64: None,
        },
        Value::Blob(b) => ProtoValue {
            typ: "blob".to_string(),
            value: None,
            base64: Some(base64::engine::general_purpose::STANDARD.encode(b)),
        },
    }
}

pub fn decode_value(pv: &ProtoValue) -> Value {
    match pv.typ.as_str() {
        "null" => Value::Null,
        "integer" => {
            let s = match &pv.value {
                Some(serde_json::Value::String(s)) => s.as_str(),
                Some(serde_json::Value::Number(n)) => {
                    return Value::Integer(n.as_i64().unwrap_or(0))
                }
                _ => return Value::Null,
            };
            Value::Integer(s.parse::<i64>().unwrap_or(0))
        }
        "float" => {
            let n = match &pv.value {
                Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                Some(serde_json::Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
                _ => 0.0,
            };
            Value::Real(n)
        }
        "text" => {
            let s = match &pv.value {
                Some(serde_json::Value::String(s)) => s.clone(),
                _ => String::new(),
            };
            Value::Text(s)
        }
        "blob" => {
            if let Some(b64) = &pv.base64 {
                // Server may return base64 with or without padding
                match base64::engine::general_purpose::STANDARD_NO_PAD.decode(b64)
                    .or_else(|_| base64::engine::general_purpose::STANDARD.decode(b64))
                {
                    Ok(bytes) => Value::Blob(bytes),
                    Err(_) => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_null() {
        let v = Value::Null;
        let encoded = encode_value(&v);
        assert_eq!(encoded.typ, "null");
        let decoded = decode_value(&encoded);
        assert_eq!(decoded, Value::Null);
    }

    #[test]
    fn encode_decode_integer() {
        let v = Value::Integer(42);
        let encoded = encode_value(&v);
        assert_eq!(encoded.typ, "integer");
        assert_eq!(
            encoded.value,
            Some(serde_json::Value::String("42".to_string()))
        );
        let decoded = decode_value(&encoded);
        assert_eq!(decoded, Value::Integer(42));
    }

    #[test]
    fn encode_decode_negative_integer() {
        let v = Value::Integer(-9999);
        let decoded = decode_value(&encode_value(&v));
        assert_eq!(decoded, Value::Integer(-9999));
    }

    #[test]
    fn encode_decode_max_integer() {
        let v = Value::Integer(i64::MAX);
        let decoded = decode_value(&encode_value(&v));
        assert_eq!(decoded, Value::Integer(i64::MAX));
    }

    #[test]
    fn encode_decode_min_integer() {
        let v = Value::Integer(i64::MIN);
        let decoded = decode_value(&encode_value(&v));
        assert_eq!(decoded, Value::Integer(i64::MIN));
    }

    #[test]
    fn encode_decode_float() {
        let v = Value::Real(3.125);
        let encoded = encode_value(&v);
        assert_eq!(encoded.typ, "float");
        let decoded = decode_value(&encoded);
        assert_eq!(decoded, Value::Real(3.125));
    }

    #[test]
    fn encode_decode_text() {
        let v = Value::Text("hello world".to_string());
        let encoded = encode_value(&v);
        assert_eq!(encoded.typ, "text");
        let decoded = decode_value(&encoded);
        assert_eq!(decoded, Value::Text("hello world".to_string()));
    }

    #[test]
    fn encode_decode_unicode_text() {
        let v = Value::Text("žluťoučký kůň".to_string());
        let decoded = decode_value(&encode_value(&v));
        assert_eq!(decoded, Value::Text("žluťoučký kůň".to_string()));
    }

    #[test]
    fn encode_decode_blob() {
        let blob: Vec<u8> = vec![0, 1, 2, 255, 128, 64];
        let v = Value::Blob(blob.clone());
        let encoded = encode_value(&v);
        assert_eq!(encoded.typ, "blob");
        assert!(encoded.base64.is_some());
        let decoded = decode_value(&encoded);
        assert_eq!(decoded, Value::Blob(blob));
    }

    #[test]
    fn encode_decode_empty_blob() {
        let v = Value::Blob(vec![]);
        let decoded = decode_value(&encode_value(&v));
        assert_eq!(decoded, Value::Blob(vec![]));
    }

    #[test]
    fn decode_integer_from_number() {
        let pv = ProtoValue {
            typ: "integer".to_string(),
            value: Some(serde_json::Value::Number(serde_json::Number::from(42))),
            base64: None,
        };
        assert_eq!(decode_value(&pv), Value::Integer(42));
    }

    #[test]
    fn json_roundtrip_cursor_request() {
        let req = CursorRequest {
            baton: None,
            batch: CursorBatch {
                steps: vec![BatchStep {
                    stmt: StmtBody {
                        sql: "SELECT 1".to_string(),
                        args: vec![],
                        named_args: vec![],
                        want_rows: true,
                    },
                    condition: None,
                }],
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("SELECT 1"));
        assert!(json.contains("want_rows"));
    }
}
