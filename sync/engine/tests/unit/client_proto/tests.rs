use super::{LogicalOp, LogicalOpType, LogicalSchemaAction, LogicalSchemaKind, LogicalTxnData};
use bytes::Bytes;
use prost::Message;

#[test]
fn logical_txn_data_round_trips_schema_and_row_ops() {
    let txn = LogicalTxnData {
        end_offset: 128,
        commit_ts: 42,
        origin_client_id: "client-a".to_string(),
        ops: vec![
            LogicalOp {
                op_type: LogicalOpType::Schema as i32,
                table_name: String::new(),
                rowid: 0,
                record: Bytes::new(),
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)".to_string(),
                user_version: None,
                application_id: None,
                schema_action: Some(LogicalSchemaAction::Create as i32),
                schema_kind: Some(LogicalSchemaKind::Table as i32),
                schema_name: "items".to_string(),
                stable_table_id: 2,
            },
            LogicalOp {
                op_type: LogicalOpType::UpsertRow as i32,
                table_name: "items".to_string(),
                rowid: 1,
                record: Bytes::from_static(b"record"),
                sql: String::new(),
                user_version: None,
                application_id: None,
                schema_action: None,
                schema_kind: None,
                schema_name: String::new(),
                stable_table_id: 2,
            },
        ],
    };

    let decoded = LogicalTxnData::decode(txn.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.end_offset, 128);
    assert_eq!(decoded.commit_ts, 42);
    assert_eq!(decoded.origin_client_id, "client-a");
    assert_eq!(decoded.ops.len(), 2);
    assert_eq!(
        LogicalOpType::try_from(decoded.ops[0].op_type).unwrap(),
        LogicalOpType::Schema
    );
    assert_eq!(
        LogicalSchemaAction::try_from(decoded.ops[0].schema_action.unwrap()).unwrap(),
        LogicalSchemaAction::Create
    );
    assert_eq!(
        LogicalSchemaKind::try_from(decoded.ops[0].schema_kind.unwrap()).unwrap(),
        LogicalSchemaKind::Table
    );
    assert_eq!(decoded.ops[1].record.as_ref(), b"record");
}
