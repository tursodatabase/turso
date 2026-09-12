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
#[path = "../tests/unit/client_proto/tests.rs"]
mod tests;
