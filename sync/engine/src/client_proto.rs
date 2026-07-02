use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
/// Logical operation kind decoded from the server's MVCC log.
pub enum LogicalOpType {
    Unspecified = 0,
    /// Insert or replace one row by rowid.
    UpsertRow = 1,
    /// Delete one row by rowid.
    DeleteRow = 2,
    /// Replay a schema create/drop/refresh/alter operation.
    Schema = 3,
    /// Replay database-header fields such as `user_version` and `application_id`.
    UpdateHeader = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
/// Schema action represented by a logical schema operation.
pub enum LogicalSchemaAction {
    Unspecified = 0,
    /// Create the schema object if it does not already exist.
    Create = 1,
    /// Drop the schema object.
    Drop = 2,
    /// Replace the client-side schema object definition with the supplied SQL.
    Refresh = 3,
    /// Replay an ALTER statement exactly as supplied by the server.
    Alter = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
/// Type of schema object affected by a logical schema operation.
pub enum LogicalSchemaKind {
    Unspecified = 0,
    /// A table entry in `sqlite_schema`.
    Table = 1,
    /// An index entry in `sqlite_schema`.
    Index = 2,
    /// A trigger entry in `sqlite_schema`.
    Trigger = 3,
    /// A view entry in `sqlite_schema`.
    View = 4,
}

#[derive(prost::Message, Clone, PartialEq, Eq)]
/// One logical operation decoded from a portable MVCC logical-log frame.
///
/// Only the fields required by `op_type` are populated. Row operations use
/// `table_name`, `rowid`, and optionally `record`; schema operations use the
/// schema fields; header updates use `user_version` and/or `application_id`.
pub struct LogicalOp {
    /// Encoded [`LogicalOpType`].
    #[prost(enumeration = "LogicalOpType", tag = "1")]
    pub op_type: i32,
    /// User table affected by row operations.
    #[prost(string, tag = "2")]
    pub table_name: String,
    /// Rowid affected by row operations.
    #[prost(sint64, tag = "3")]
    pub rowid: i64,
    /// SQLite record bytes for upsert operations.
    #[prost(bytes, tag = "4")]
    pub record: Bytes,
    /// SQL used for schema create, refresh, or alter operations.
    #[prost(string, tag = "5")]
    pub sql: String,
    /// New `PRAGMA user_version`, when the operation updates it.
    #[prost(optional, uint32, tag = "6")]
    pub user_version: Option<u32>,
    /// New `PRAGMA application_id`, when the operation updates it.
    #[prost(optional, uint32, tag = "7")]
    pub application_id: Option<u32>,
    /// Encoded [`LogicalSchemaAction`] for schema operations.
    #[prost(optional, enumeration = "LogicalSchemaAction", tag = "8")]
    pub schema_action: Option<i32>,
    /// Encoded [`LogicalSchemaKind`] for schema operations.
    #[prost(optional, enumeration = "LogicalSchemaKind", tag = "9")]
    pub schema_kind: Option<i32>,
    /// Schema object name for schema operations.
    #[prost(string, tag = "10")]
    pub schema_name: String,
    /// Stable table/object identifier carried by portable MVCC logical changes.
    ///
    /// Row operations may omit `table_name` when this is set. The sync engine
    /// maintains the stable-id-to-name map from schema operations, and still
    /// accepts named row operations for compatibility with older logical paths.
    #[prost(uint64, tag = "11")]
    pub stable_table_id: u64,
}

#[derive(prost::Message, Clone, PartialEq, Eq)]
/// One committed MVCC transaction decoded from a portable MVCC logical-log frame.
pub struct LogicalTxnData {
    /// Logical-log byte offset immediately after this transaction.
    #[prost(uint64, tag = "1")]
    pub end_offset: u64,
    /// MVCC commit timestamp used as the replay change time.
    ///
    /// TODO: confirm the long-term replay-time semantics before relying on
    /// this as user-visible change time.
    #[prost(uint64, tag = "2")]
    pub commit_ts: u64,
    /// Logical operations in commit order.
    #[prost(message, repeated, tag = "3")]
    pub ops: Vec<LogicalOp>,
    /// Client that originated this transaction, when known.
    #[prost(string, tag = "4")]
    pub origin_client_id: String,
}

#[cfg(test)]
mod tests {
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
}
