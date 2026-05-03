// Re-export shared protocol types
pub use turso_remote_protocol::Stmt as StmtBody;
pub use turso_remote_protocol::{
    CursorBatch, CursorEntry, CursorRequest, CursorResponse, ExecuteStreamReq, NamedArg,
    PipelineRequest, PipelineResponse, SequenceStreamReq, StreamRequest, StreamResponse,
    StreamResult, Value as ProtoValue,
};

use crate::Value;

// --- Encoding/Decoding ---

pub fn encode_value(value: &Value) -> ProtoValue {
    match value {
        Value::Null => ProtoValue::Null,
        Value::Integer(n) => ProtoValue::Integer { value: *n },
        Value::Real(n) => ProtoValue::Float { value: *n },
        Value::Text(s) => ProtoValue::Text { value: s.clone() },
        Value::Blob(b) => ProtoValue::Blob {
            value: bytes::Bytes::from(b.clone()),
        },
    }
}

pub fn decode_value(pv: &ProtoValue) -> Value {
    match pv {
        ProtoValue::Null => Value::Null,
        ProtoValue::Integer { value } => Value::Integer(*value),
        ProtoValue::Float { value } => Value::Real(*value),
        ProtoValue::Text { value } => Value::Text(value.clone()),
        ProtoValue::Blob { value } => Value::Blob(value.to_vec()),
        ProtoValue::None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_remote_protocol::BatchStep;

    #[test]
    fn encode_decode_null() {
        let v = Value::Null;
        let encoded = encode_value(&v);
        let decoded = decode_value(&encoded);
        assert_eq!(decoded, Value::Null);
    }

    #[test]
    fn encode_decode_integer() {
        let v = Value::Integer(42);
        let encoded = encode_value(&v);
        assert!(matches!(encoded, ProtoValue::Integer { value: 42 }));
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
        assert!(matches!(encoded, ProtoValue::Float { value } if value == 3.125));
        let decoded = decode_value(&encoded);
        assert_eq!(decoded, Value::Real(3.125));
    }

    #[test]
    fn encode_decode_text() {
        let v = Value::Text("hello world".to_string());
        let encoded = encode_value(&v);
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
        assert!(matches!(encoded, ProtoValue::Blob { .. }));
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
    fn json_roundtrip_values() {
        // Verify that encoding to JSON and back works correctly via serde
        let values = vec![
            ProtoValue::Null,
            ProtoValue::Integer { value: 42 },
            ProtoValue::Float { value: 3.14 },
            ProtoValue::Text {
                value: "hello".to_string(),
            },
        ];
        for v in &values {
            let json = serde_json::to_string(v).unwrap();
            let parsed: ProtoValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, &parsed);
        }
    }

    #[test]
    fn json_roundtrip_cursor_request() {
        let req = CursorRequest {
            baton: None,
            batch: CursorBatch {
                steps: vec![BatchStep {
                    stmt: turso_remote_protocol::Stmt {
                        sql: Some("SELECT 1".to_string()),
                        sql_id: None,
                        args: vec![],
                        named_args: vec![],
                        want_rows: Some(true),
                        replication_index: None,
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
