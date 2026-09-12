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
