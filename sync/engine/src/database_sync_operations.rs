use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::{Bytes, BytesMut};
use prost::Message;
use roaring::RoaringBitmap;
use turso_core::{
    io::FileSyncType,
    storage::sqlite3_ondisk::read_varint as read_sqlite_varint,
    types::{Text, WalFrameInfo},
    Buffer, Completion, LimboError, OpenFlags, Value,
};

use crate::{
    client_proto::{
        LogicalOp, LogicalOpType, LogicalSchemaAction, LogicalSchemaKind, LogicalTxnData,
    },
    database_replay_generator::DatabaseReplayGenerator,
    database_sync_engine::{DataStats, DatabaseSyncEngineOpts},
    database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
    database_tape::{
        run_stmt_expect_one_row, run_stmt_ignore_rows, run_stmt_once, DatabaseChangesIteratorMode,
        DatabaseChangesIteratorOpts, DatabaseReplaySession, DatabaseReplaySessionOpts,
        DatabaseTape, DatabaseWalSession,
    },
    errors::Error,
    io_operations::IoOperations,
    server_proto::{
        self, Batch, BatchCond, BatchStep, BatchStreamReq, PageData, PageUpdatesEncodingReq,
        PullUpdatesApplyMode, PullUpdatesProtocol, PullUpdatesReqProtoBody,
        PullUpdatesRespProtoBody, PullUpdatesStreamKind, Stmt, StmtResult, StreamRequest,
    },
    types::{
        parse_bin_record, Coro, DatabasePullRevision, DatabaseRowMutation,
        DatabaseRowTransformResult, DatabaseSchemaKind, DatabaseSchemaReplay,
        DatabaseStatementReplay, DatabaseSyncEngineProtocolVersion, DatabaseTapeOperation,
        DatabaseTapeRowChange, DatabaseTapeRowChangeType, DbSyncInfo, DbSyncStatus,
        PartialBootstrapStrategy, PartialSyncOpts, RemotePullProtocol, SyncEngineIoResult,
    },
    wal_session::WalSession,
    Result,
};

pub const WAL_HEADER: usize = 32;
pub const WAL_FRAME_HEADER: usize = 24;
pub const PAGE_SIZE: usize = 4096;
pub const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER + PAGE_SIZE;
const MVCC_LOG_MAGIC: u32 = 0x4C4D4C32;
const MVCC_LOG_VERSION: u8 = 3;
const MVCC_LOG_HEADER_SIZE: usize = 56;
const MVCC_TX_FRAME_MAGIC: u32 = 0x5854564D;
const MVCC_TX_EXT_FRAME_MAGIC: u32 = 0x5845564D;
const MVCC_TX_END_MAGIC: u32 = 0x4554564D;
const MVCC_TX_HEADER_SIZE: usize = 24;
const MVCC_TX_EXT_HEADER_SIZE: usize = 40;
const MVCC_TX_TRAILER_SIZE: usize = 8;
const MVCC_TX_FLAG_HAS_EXTENSION_BLOCK: u32 = 1 << 0;
const MVCC_EXTENSION_RECORD_HEADER_SIZE: usize = 8;
const MVCC_EXTENSION_TYPE_PORTABLE_CHANGES: u16 = 1;
const MVCC_LOG_HEADER_SALT_START: usize = 8;
const MVCC_LOG_HEADER_SALT_END: usize = 16;
const MVCC_LOG_HEADER_RESERVED_START: usize = 16;
const MVCC_LOG_HEADER_CRC_START: usize = 52;
const MVCC_OP_UPSERT_TABLE: u8 = 0;
const MVCC_OP_DELETE_TABLE: u8 = 1;
const MVCC_OP_UPSERT_INDEX: u8 = 2;
const MVCC_OP_DELETE_INDEX: u8 = 3;
const MVCC_OP_UPDATE_HEADER: u8 = 4;
const MVCC_OP_FLAG_PORTABLE_EXTENSION: u8 = 1 << 1;
const MVCC_SQLITE_SCHEMA_TABLE_ID: i64 = -1;
const MVCC_DELETE_EXT_IDENTITY_RECORD_FIELD: u64 = 1;
const MVCC_DELETE_EXT_PK_RECORD_FIELD: u64 = 2;
const PORTABLE_TXN_META_CLIENT_KEY: &str = "client";
const SQLITE_INTERNAL_PREFIX: &str = "sqlite_";
const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";
const TURSO_INTERNAL_PREFIX: &str = "__turso_internal_";
const TURSO_CDC_TABLE_NAME: &str = "turso_cdc";
const TURSO_CDC_VERSION_TABLE_NAME: &str = "turso_cdc_version";
const CDC_COMMIT_CHANGE_TYPE: i64 = 2;

#[derive(prost::Message, Clone, PartialEq, Eq)]
struct PortableLogicalTxn {
    #[prost(uint64, tag = "1")]
    end_offset: u64,
    #[prost(uint64, tag = "2")]
    commit_ts: u64,
    #[prost(bytes = "bytes", repeated, tag = "12")]
    string_table: Vec<Bytes>,
    #[prost(message, repeated, tag = "13")]
    object_map: Vec<PortableObjectMap>,
    #[prost(message, repeated, tag = "14")]
    meta: Vec<PortableMeta>,
}

#[derive(prost::Message, Clone, PartialEq, Eq)]
struct PortableObjectMap {
    #[prost(sint64, tag = "1")]
    mv_table_id: i64,
    #[prost(uint64, tag = "2")]
    name_ref: u64,
}

#[derive(prost::Message, Clone, PartialEq, Eq)]
struct PortableMeta {
    #[prost(uint64, tag = "1")]
    key_ref: u64,
    #[prost(uint64, tag = "2")]
    value_ref: u64,
}

fn pull_updates_stream_kind(header: &PullUpdatesRespProtoBody) -> Result<PullUpdatesStreamKind> {
    PullUpdatesStreamKind::try_from(header.stream_kind).map_err(|_| {
        Error::DatabaseSyncEngineError(format!(
            "unknown pull-updates stream kind: {}",
            header.stream_kind
        ))
    })
}

fn pull_updates_apply_mode(header: &PullUpdatesRespProtoBody) -> Result<PullUpdatesApplyMode> {
    PullUpdatesApplyMode::try_from(header.apply_mode).map_err(|_| {
        Error::DatabaseSyncEngineError(format!(
            "unknown pull-updates apply mode: {}",
            header.apply_mode
        ))
    })
}

/// Detects the remote's sync protocol from the `protocol` field of a
/// pull-updates response header.
///
/// Servers deploy before SDK releases, so a response without the field (or
/// with an unknown future value) comes from a page-protocol server by
/// definition — MVCC databases only exist behind servers that advertise it.
pub(crate) fn detect_remote_pull_protocol(header: &PullUpdatesRespProtoBody) -> RemotePullProtocol {
    match PullUpdatesProtocol::try_from(header.protocol) {
        Ok(PullUpdatesProtocol::MvccLogical) => RemotePullProtocol::MvccLogical,
        Ok(PullUpdatesProtocol::Pages | PullUpdatesProtocol::Unspecified) | Err(_) => {
            RemotePullProtocol::Pages
        }
    }
}

fn ensure_page_stream(header: &PullUpdatesRespProtoBody, context: &str) -> Result<()> {
    let stream_kind = pull_updates_stream_kind(header)?;
    let _apply_mode = pull_updates_apply_mode(header)?;
    match stream_kind {
        PullUpdatesStreamKind::Pages => Ok(()),
        PullUpdatesStreamKind::MvccLogicalLog => Err(Error::DatabaseSyncEngineError(format!(
            "{context} does not support raw MVCC logical-log pull-updates streams yet"
        ))),
    }
}

fn ensure_incremental_page_stream(header: &PullUpdatesRespProtoBody, context: &str) -> Result<()> {
    ensure_page_stream(header, context)?;
    match pull_updates_apply_mode(header)? {
        PullUpdatesApplyMode::Incremental => Ok(()),
        PullUpdatesApplyMode::ReplaceBase => Err(Error::DatabaseSyncEngineError(format!(
            "{context} does not support replace-base page streams yet"
        ))),
    }
}

fn logical_op_to_tape_operations(
    op: LogicalOp,
    commit_ts: u64,
) -> Result<Vec<DatabaseTapeOperation>> {
    let op_type = LogicalOpType::try_from(op.op_type).map_err(|_| {
        Error::DatabaseSyncEngineError(format!("unknown logical op type: {}", op.op_type))
    })?;
    match op_type {
        LogicalOpType::Unspecified => Err(Error::DatabaseSyncEngineError(
            "logical op type must not be unspecified".to_string(),
        )),
        LogicalOpType::UpsertRow => {
            if op.table_name.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical upsert_row must include table_name".to_string(),
                ));
            }
            if op.record.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical upsert_row must include record bytes".to_string(),
                ));
            }
            Ok(vec![DatabaseTapeOperation::RowChange(
                DatabaseTapeRowChange {
                    change_id: 0,
                    change_time: commit_ts,
                    change: DatabaseTapeRowChangeType::Insert {
                        after: parse_bin_record(&op.record)?,
                    },
                    table_name: op.table_name,
                    id: op.rowid,
                },
            )])
        }
        LogicalOpType::DeleteRow => {
            if op.table_name.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical delete_row must include table_name".to_string(),
                ));
            }
            Ok(vec![DatabaseTapeOperation::RowChange(
                DatabaseTapeRowChange {
                    change_id: 0,
                    change_time: commit_ts,
                    change: DatabaseTapeRowChangeType::Delete {
                        before: turso_core::alloc::vec![],
                        key: if op.record.is_empty() {
                            None
                        } else {
                            Some(parse_bin_record(&op.record)?)
                        },
                    },
                    table_name: op.table_name,
                    id: op.rowid,
                },
            )])
        }
        LogicalOpType::Schema => {
            if op.schema_name.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical schema op must include schema_name".to_string(),
                ));
            }
            if !is_logically_replayable_table(&op.schema_name) {
                return Ok(Vec::new());
            }
            let schema_action =
                LogicalSchemaAction::try_from(op.schema_action.ok_or_else(|| {
                    Error::DatabaseSyncEngineError(
                        "logical schema op must include schema_action".to_string(),
                    )
                })?)
                .map_err(|_| {
                    Error::DatabaseSyncEngineError(format!(
                        "unknown logical schema action: {:?}",
                        op.schema_action
                    ))
                })?;
            let schema_kind = logical_schema_kind(op.schema_kind.ok_or_else(|| {
                Error::DatabaseSyncEngineError(
                    "logical schema op must include schema_kind".to_string(),
                )
            })?)?;
            let operation = match schema_action {
                LogicalSchemaAction::Unspecified => {
                    return Err(Error::DatabaseSyncEngineError(
                        "logical schema action must not be unspecified".to_string(),
                    ));
                }
                LogicalSchemaAction::Create => {
                    if op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema create must include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Create {
                        sql: op.sql,
                    })
                }
                LogicalSchemaAction::Drop => {
                    if !op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema drop must not include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Drop {
                        kind: schema_kind,
                        name: op.schema_name,
                    })
                }
                LogicalSchemaAction::Refresh => {
                    if op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema refresh must include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Refresh {
                        kind: schema_kind,
                        name: op.schema_name,
                        sql: op.sql,
                    })
                }
                LogicalSchemaAction::Alter => {
                    if op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema alter must include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Alter { sql: op.sql })
                }
            };
            Ok(vec![operation])
        }
        LogicalOpType::UpdateHeader => {
            let mut operations = Vec::with_capacity(2);
            if let Some(user_version) = op.user_version {
                operations.push(DatabaseTapeOperation::StmtReplay(DatabaseStatementReplay {
                    sql: format!("PRAGMA user_version = {user_version}"),
                    values: Vec::new(),
                }));
            }
            if let Some(application_id) = op.application_id {
                operations.push(DatabaseTapeOperation::StmtReplay(DatabaseStatementReplay {
                    sql: format!("PRAGMA application_id = {application_id}"),
                    values: Vec::new(),
                }));
            }
            if operations.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical update_header must include at least one field".to_string(),
                ));
            }
            Ok(operations)
        }
    }
}

pub(crate) fn is_logically_replayable_table(name: &str) -> bool {
    !name.starts_with(SQLITE_INTERNAL_PREFIX)
        && !name.starts_with(TURSO_INTERNAL_PREFIX)
        && name != TURSO_SYNC_TABLE_NAME
        && name != TURSO_CDC_TABLE_NAME
        && name != TURSO_CDC_VERSION_TABLE_NAME
}

/// SQL form of [`is_logically_replayable_table`] over a `table_name` column.
/// GLOB is used instead of LIKE because `_` is a wildcard for LIKE but a plain
/// character for GLOB, and every prefix here contains underscores.
fn logically_replayable_table_filter() -> String {
    format!(
        "table_name NOT GLOB '{SQLITE_INTERNAL_PREFIX}*' \
         AND table_name NOT GLOB '{TURSO_INTERNAL_PREFIX}*' \
         AND table_name NOT IN ('{TURSO_SYNC_TABLE_NAME}', '{TURSO_CDC_TABLE_NAME}', '{TURSO_CDC_VERSION_TABLE_NAME}')"
    )
}

fn sqlite_schema_change_name(change: &DatabaseTapeRowChange) -> Result<Option<String>> {
    if change.table_name != SQLITE_SCHEMA_TABLE {
        return Ok(None);
    }
    let values = match &change.change {
        DatabaseTapeRowChangeType::Delete { before, .. } => before,
        DatabaseTapeRowChangeType::Insert { after } => after,
        DatabaseTapeRowChangeType::Update { after, .. } => after,
    };
    match values.get(1) {
        Some(Value::Text(name)) => Ok(Some(name.as_str().to_string())),
        Some(Value::Null) | None => Ok(None),
        Some(other) => Err(Error::DatabaseSyncEngineError(format!(
            "sqlite_schema.name must be text while filtering push changes, got {other:?}"
        ))),
    }
}

fn sqlite_schema_change_updates_sql(change: &DatabaseTapeRowChange) -> Result<bool> {
    if change.table_name != SQLITE_SCHEMA_TABLE {
        return Ok(true);
    }
    let DatabaseTapeRowChangeType::Update { updates, .. } = &change.change else {
        return Ok(true);
    };
    let Some(updates) = updates else {
        return Err(Error::DatabaseSyncEngineError(
            "sqlite_schema update must include update flags while filtering changes".to_string(),
        ));
    };
    if updates.len() != 10 {
        return Err(Error::DatabaseSyncEngineError(format!(
            "sqlite_schema update must contain 5 update flags and 5 values, got {} values",
            updates.len()
        )));
    }
    match updates.get(4) {
        Some(Value::Numeric(turso_core::Numeric::Integer(0))) => Ok(false),
        Some(Value::Numeric(turso_core::Numeric::Integer(1))) => Ok(true),
        Some(other) => Err(Error::DatabaseSyncEngineError(format!(
            "sqlite_schema.sql update flag must be integer 0 or 1 while filtering changes, got {other:?}"
        ))),
        None => unreachable!("sqlite_schema update length was validated"),
    }
}

fn should_push_change(
    change: &DatabaseTapeRowChange,
    opts: &DatabaseSyncEngineOpts,
) -> Result<bool> {
    if change.table_name == TURSO_SYNC_TABLE_NAME {
        return Ok(false);
    }
    if opts.tables_ignore.iter().any(|x| &change.table_name == x) {
        return Ok(false);
    }
    if change.table_name == SQLITE_SCHEMA_TABLE {
        if !sqlite_schema_change_updates_sql(change)? {
            return Ok(false);
        }
        return Ok(sqlite_schema_change_name(change)?
            .as_deref()
            .is_some_and(is_logically_replayable_table));
    }
    Ok(is_logically_replayable_table(&change.table_name))
}

pub(crate) fn should_replay_local_change(change: &DatabaseTapeRowChange) -> Result<bool> {
    if change.table_name == TURSO_SYNC_TABLE_NAME {
        return Ok(false);
    }
    if change.table_name == SQLITE_SCHEMA_TABLE {
        if !sqlite_schema_change_updates_sql(change)? {
            return Ok(false);
        }
        return Ok(sqlite_schema_change_name(change)?
            .as_deref()
            .is_some_and(is_logically_replayable_table));
    }
    Ok(is_logically_replayable_table(&change.table_name))
}

fn logical_schema_kind(kind: i32) -> Result<DatabaseSchemaKind> {
    let kind = LogicalSchemaKind::try_from(kind).map_err(|_| {
        Error::DatabaseSyncEngineError(format!("unknown logical schema kind: {kind}"))
    })?;
    Ok(match kind {
        LogicalSchemaKind::Unspecified => {
            return Err(Error::DatabaseSyncEngineError(
                "logical schema kind must not be unspecified".to_string(),
            ));
        }
        LogicalSchemaKind::Table => DatabaseSchemaKind::Table,
        LogicalSchemaKind::Index => DatabaseSchemaKind::Index,
        LogicalSchemaKind::Trigger => DatabaseSchemaKind::Trigger,
        LogicalSchemaKind::View => DatabaseSchemaKind::View,
    })
}

#[derive(Clone, Default)]
struct LogicalReplayTableMap {
    names_by_stable_id: BTreeMap<u64, String>,
}

impl LogicalReplayTableMap {
    fn from_persisted(names_by_stable_id: BTreeMap<u64, String>) -> Self {
        Self { names_by_stable_id }
    }

    fn into_persisted(self) -> BTreeMap<u64, String> {
        self.names_by_stable_id
    }

    fn resolve_row_table_name(&self, op: &LogicalOp) -> Result<String> {
        if !op.table_name.is_empty() {
            return Ok(op.table_name.clone());
        }
        if op.stable_table_id == 0 {
            return Err(Error::DatabaseSyncEngineError(
                "logical row op must include table_name or stable_table_id".to_string(),
            ));
        }
        self.names_by_stable_id
            .get(&op.stable_table_id)
            .cloned()
            .ok_or_else(|| {
                Error::DatabaseSyncEngineError(format!(
                    "logical row op references unknown stable_table_id {}",
                    op.stable_table_id
                ))
            })
    }

    fn observe_schema_op(
        &mut self,
        stable_table_id: u64,
        schema_name: &str,
        schema_kind: DatabaseSchemaKind,
        schema_action: LogicalSchemaAction,
    ) {
        if stable_table_id == 0 || schema_kind != DatabaseSchemaKind::Table {
            return;
        }
        match schema_action {
            LogicalSchemaAction::Unspecified => {}
            LogicalSchemaAction::Create
            | LogicalSchemaAction::Refresh
            | LogicalSchemaAction::Alter => {
                self.names_by_stable_id
                    .insert(stable_table_id, schema_name.to_string());
            }
            LogicalSchemaAction::Drop => {
                self.names_by_stable_id.remove(&stable_table_id);
            }
        }
    }
}

pub fn logical_txn_to_tape_operations(txn: &LogicalTxnData) -> Result<Vec<DatabaseTapeOperation>> {
    let mut table_map = LogicalReplayTableMap::default();
    logical_txn_to_tape_operations_with_table_map(txn, &mut table_map)
}

fn logical_txn_to_tape_operations_with_table_map(
    txn: &LogicalTxnData,
    table_map: &mut LogicalReplayTableMap,
) -> Result<Vec<DatabaseTapeOperation>> {
    let mut operations = Vec::new();
    for op in txn.ops.iter().cloned() {
        let op_type = LogicalOpType::try_from(op.op_type).map_err(|_| {
            Error::DatabaseSyncEngineError(format!("unknown logical op type: {}", op.op_type))
        })?;
        match op_type {
            LogicalOpType::Unspecified => {
                return Err(Error::DatabaseSyncEngineError(
                    "logical op type must not be unspecified".to_string(),
                ));
            }
            LogicalOpType::UpsertRow | LogicalOpType::DeleteRow => {
                let table_name = table_map.resolve_row_table_name(&op)?;
                if !is_logically_replayable_table(&table_name) {
                    continue;
                }
                let mut op = op;
                op.table_name = table_name;
                operations.extend(logical_op_to_tape_operations(op, txn.commit_ts)?);
            }
            LogicalOpType::Schema => {
                if !is_logically_replayable_table(&op.schema_name) {
                    continue;
                }
                let schema_action =
                    LogicalSchemaAction::try_from(op.schema_action.ok_or_else(|| {
                        Error::DatabaseSyncEngineError(
                            "logical schema op must include schema_action".to_string(),
                        )
                    })?)
                    .map_err(|_| {
                        Error::DatabaseSyncEngineError(format!(
                            "unknown logical schema action: {:?}",
                            op.schema_action
                        ))
                    })?;
                let schema_kind = logical_schema_kind(op.schema_kind.ok_or_else(|| {
                    Error::DatabaseSyncEngineError(
                        "logical schema op must include schema_kind".to_string(),
                    )
                })?)?;
                table_map.observe_schema_op(
                    op.stable_table_id,
                    &op.schema_name,
                    schema_kind,
                    schema_action,
                );
                operations.extend(logical_op_to_tape_operations(op, txn.commit_ts)?);
            }
            LogicalOpType::UpdateHeader => {
                operations.extend(logical_op_to_tape_operations(op, txn.commit_ts)?);
            }
        }
    }
    Ok(operations)
}

#[derive(Debug, Default)]
pub struct LogicalReplayApplyStats {
    pub touched_rows: std::collections::BTreeSet<(String, i64)>,
}

fn logical_operations_touched_rows(
    operations: &[DatabaseTapeOperation],
) -> std::collections::BTreeSet<(String, i64)> {
    operations
        .iter()
        .filter_map(|operation| match operation {
            DatabaseTapeOperation::RowChange(change)
                if is_logically_replayable_table(&change.table_name) =>
            {
                Some((change.table_name.clone(), change.id))
            }
            _ => None,
        })
        .collect()
}

fn logical_txn_acknowledges_client(txn: &LogicalTxnData, client_id: &str) -> Result<bool> {
    if txn.origin_client_id == client_id {
        return Ok(true);
    }

    for op in &txn.ops {
        if LogicalOpType::try_from(op.op_type) != Ok(LogicalOpType::UpsertRow) {
            continue;
        }
        if op.table_name != TURSO_SYNC_TABLE_NAME {
            continue;
        }
        let values = parse_bin_record(&op.record)?;
        if values
            .first()
            .and_then(|value| value.to_text())
            .is_some_and(|value| value == client_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn read_proto_file_chunk<Ctx>(
    coro: &Coro<Ctx>,
    file: &Arc<dyn turso_core::File>,
    size: u64,
    file_offset: &mut u64,
    bytes: &mut BytesMut,
) -> Result<()> {
    if *file_offset >= size {
        if bytes.is_empty() {
            return Ok(());
        }
        return Err(Error::DatabaseSyncEngineError(
            "unexpected end of protobuf message in file".to_string(),
        ));
    }

    let to_read = (size - *file_offset).min(64 * 1024) as usize;
    let buffer = Arc::new(Buffer::new_temporary(to_read));
    let read_len = Arc::new(Mutex::new(Ok(0usize)));
    let c = Completion::new_read(buffer.clone(), {
        let read_len = read_len.clone();
        move |result| {
            *read_len.lock().unwrap() = result
                .map(|(_, len)| len as usize)
                .map_err(|err| Error::IoError(err.to_string()));
            None
        }
    });
    let c = file.pread(*file_offset, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    let read_len = read_len.lock().unwrap().clone()?;
    if read_len == 0 {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected EOF while reading protobuf messages from file".to_string(),
        ));
    }
    *file_offset += read_len as u64;
    bytes.extend_from_slice(&buffer.as_slice()[..read_len]);
    Ok(())
}

async fn replay_logical_transactions_from_file<Ctx>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns_file: &Arc<dyn turso_core::File>,
    excluded_client_id: Option<&str>,
    table_names_by_stable_id: &mut BTreeMap<u64, String>,
) -> Result<LogicalReplayApplyStats> {
    let size = txns_file.size()?;
    let mut file_offset = 0u64;
    let mut bytes = BytesMut::new();
    let mut stats = LogicalReplayApplyStats::default();
    let mut table_map =
        LogicalReplayTableMap::from_persisted(std::mem::take(table_names_by_stable_id));
    loop {
        while let Some(txn) = take_proto_message_from_bytes::<LogicalTxnData>(&mut bytes)? {
            let mut decoded_table_map = table_map.clone();
            let operations =
                logical_txn_to_tape_operations_with_table_map(&txn, &mut decoded_table_map)?;
            if let Some(excluded_client_id) = excluded_client_id {
                if logical_txn_acknowledges_client(&txn, excluded_client_id)? {
                    continue;
                }
            }
            table_map = decoded_table_map;
            for operation in operations {
                for row in logical_operations_touched_rows(std::slice::from_ref(&operation)) {
                    stats.touched_rows.insert(row);
                }
                replay.replay(coro, operation).await?;
            }
        }
        if file_offset >= size {
            if bytes.is_empty() {
                break;
            }
            return Err(Error::DatabaseSyncEngineError(
                "unexpected end of protobuf message in file".to_string(),
            ));
        }
        read_proto_file_chunk(coro, txns_file, size, &mut file_offset, &mut bytes).await?;
    }
    *table_names_by_stable_id = table_map.into_persisted();
    Ok(stats)
}

pub async fn apply_logical_transactions_file_without_commit_excluding_client_txns_with_table_map_and_stats<
    Ctx,
>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns_file: &Arc<dyn turso_core::File>,
    excluded_client_id: &str,
    table_names_by_stable_id: &mut BTreeMap<u64, String>,
) -> Result<LogicalReplayApplyStats> {
    replay_logical_transactions_from_file(
        coro,
        replay,
        txns_file,
        Some(excluded_client_id),
        table_names_by_stable_id,
    )
    .await
}

pub struct MutexSlot<T: Clone> {
    pub value: T,
    pub slot: Arc<Mutex<Option<T>>>,
}

impl<T: Clone> Drop for MutexSlot<T> {
    fn drop(&mut self) {
        self.slot.lock().unwrap().replace(self.value.clone());
    }
}

pub(crate) fn acquire_slot<T: Clone>(slot: &Arc<Mutex<Option<T>>>) -> Result<MutexSlot<T>> {
    let Some(value) = slot.lock().unwrap().take() else {
        return Err(Error::DatabaseSyncEngineError(
            "changes file already acquired by another operation".to_string(),
        ));
    };
    Ok(MutexSlot {
        value,
        slot: slot.clone(),
    })
}

pub struct SyncEngineIoStats<IO: SyncEngineIo> {
    pub io: Arc<IO>,
    pub network_stats: Arc<DataStats>,
}

impl<IO: SyncEngineIo> SyncEngineIoStats<IO> {
    pub fn new(io: Arc<IO>) -> Self {
        Self {
            io,
            network_stats: Arc::new(DataStats::new()),
        }
    }
}

impl<IO: SyncEngineIo> Clone for SyncEngineIoStats<IO> {
    fn clone(&self) -> Self {
        Self {
            io: self.io.clone(),
            network_stats: self.network_stats.clone(),
        }
    }
}

impl<IO: SyncEngineIo> Deref for SyncEngineIoStats<IO> {
    type Target = IO;

    fn deref(&self) -> &Self::Target {
        &self.io
    }
}

enum WalHttpPullResult<C: DataCompletion<u8>> {
    Frames(C),
    NeedCheckpoint(DbSyncStatus),
}

pub enum WalPushResult {
    Ok { baton: Option<String> },
    NeedCheckpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullUpdatesV1Result {
    Pages { replace_base: bool },
    Logical { txns: usize, ops: usize },
}

pub fn connect_untracked(tape: &DatabaseTape) -> Result<Arc<turso_core::Connection>> {
    let conn = tape.connect_untracked()?;
    assert_eq!(
        conn.wal_auto_actions(),
        turso_core::WalAutoActions::empty(),
        "tape must be configured to have all auto-WAL actions disabled"
    );
    Ok(conn)
}

/// HTTP header key for the encryption key, for the encrypted Turso Cloud databases
pub const ENCRYPTION_KEY_HEADER: &str = "x-turso-encryption-key";

pub struct SyncOperationCtx<'a, IO: SyncEngineIo, Ctx> {
    pub coro: &'a Coro<Ctx>,
    pub io: &'a SyncEngineIoStats<IO>,
    // optional remote url set in the saved configuration section of metadata file
    pub remote_url: Option<String>,
    // optional remote encryption key for the encrypted Turso Cloud databases, base64 encoded
    pub remote_encryption_key: Option<String>,
}

impl<'a, IO: SyncEngineIo, Ctx> SyncOperationCtx<'a, IO, Ctx> {
    /// Create a sync operation context.
    /// `remote_encryption_key` should be base64-encoded if provided.
    pub fn new(
        coro: &'a Coro<Ctx>,
        io: &'a SyncEngineIoStats<IO>,
        remote_url: Option<String>,
        remote_encryption_key: Option<&str>,
    ) -> Self {
        Self {
            coro,
            io,
            remote_url: remote_url.map(|x| x.to_string()),
            remote_encryption_key: remote_encryption_key.map(|k| k.to_string()),
        }
    }
    pub fn http(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> Result<IO::DataCompletionBytes> {
        let encryption_header = self
            .remote_encryption_key
            .as_ref()
            .map(|key| (ENCRYPTION_KEY_HEADER, key.as_str()));

        let all_headers: Vec<_> = headers.iter().copied().chain(encryption_header).collect();

        self.io
            .http(self.remote_url.as_deref(), method, path, body, &all_headers)
    }
}

async fn truncate_file<Ctx>(coro: &Coro<Ctx>, file: &Arc<dyn turso_core::File>) -> Result<()> {
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert_eq!(rc, 0);
    });
    let c = file.truncate(0, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

async fn append_file_bytes<Ctx>(
    coro: &Coro<Ctx>,
    file: &Arc<dyn turso_core::File>,
    offset: &mut u64,
    data: &[u8],
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let buffer = Arc::new(Buffer::new(data.to_vec()));
    let len = data.len();
    let c = Completion::new_write(move |result| {
        let Ok(size) = result else {
            return;
        };
        assert_eq!(size as usize, len);
    });
    let c = file.pwrite(*offset, buffer, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    *offset += len as u64;
    Ok(())
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
    let bytes = buf.get(offset..offset + 4).ok_or_else(|| {
        Error::DatabaseSyncEngineError("truncated MVCC logical-log frame".to_string())
    })?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u16_le(buf: &[u8], offset: usize) -> Result<u16> {
    let bytes = buf.get(offset..offset + 2).ok_or_else(|| {
        Error::DatabaseSyncEngineError("truncated MVCC logical-log frame".to_string())
    })?;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
    let bytes = buf.get(offset..offset + 8).ok_or_else(|| {
        Error::DatabaseSyncEngineError("truncated MVCC logical-log frame".to_string())
    })?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn find_mvcc_extension_payload(
    extension_block: &[u8],
    extension_record_count: u32,
    wanted_type: u16,
) -> Result<Vec<u8>> {
    let mut offset = 0usize;
    let mut payload = Vec::new();
    for _ in 0..extension_record_count {
        let header_end = offset
            .checked_add(MVCC_EXTENSION_RECORD_HEADER_SIZE)
            .ok_or_else(|| {
                Error::DatabaseSyncEngineError(
                    "MVCC logical-log extension record offset overflow".to_string(),
                )
            })?;
        if header_end > extension_block.len() {
            return Err(Error::DatabaseSyncEngineError(
                "MVCC logical-log extension record header is truncated".to_string(),
            ));
        }
        let extension_type = read_u16_le(extension_block, offset)?;
        let extension_flags = read_u16_le(extension_block, offset + 2)?;
        if extension_flags != 0 {
            return Err(Error::DatabaseSyncEngineError(format!(
                "unsupported MVCC logical-log extension flags for type {extension_type}: {extension_flags:#x}"
            )));
        }
        let extension_len = read_u32_le(extension_block, offset + 4)? as usize;
        let payload_start = header_end;
        let payload_end = payload_start.checked_add(extension_len).ok_or_else(|| {
            Error::DatabaseSyncEngineError(
                "MVCC logical-log extension record payload offset overflow".to_string(),
            )
        })?;
        if payload_end > extension_block.len() {
            return Err(Error::DatabaseSyncEngineError(
                "MVCC logical-log extension record payload is truncated".to_string(),
            ));
        }
        if extension_type == wanted_type {
            payload.extend_from_slice(&extension_block[payload_start..payload_end]);
        }
        offset = payload_end;
    }
    if offset != extension_block.len() {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical-log extension block has trailing bytes".to_string(),
        ));
    }
    Ok(payload)
}

fn validate_mvcc_log_header(buf: &[u8]) -> Result<u32> {
    if buf.len() < MVCC_LOG_HEADER_SIZE {
        return Err(Error::DatabaseSyncEngineError(
            "truncated MVCC logical-log header".to_string(),
        ));
    }
    if read_u32_le(buf, 0)? != MVCC_LOG_MAGIC {
        return Err(Error::DatabaseSyncEngineError(
            "invalid MVCC logical-log header magic".to_string(),
        ));
    }
    if buf[4] != MVCC_LOG_VERSION {
        return Err(Error::DatabaseSyncEngineError(format!(
            "unsupported MVCC logical-log version {}",
            buf[4]
        )));
    }
    if buf[5] & 0b1111_1110 != 0 {
        return Err(Error::DatabaseSyncEngineError(
            "invalid MVCC logical-log header flags".to_string(),
        ));
    }
    let hdr_len = u16::from_le_bytes([buf[6], buf[7]]) as usize;
    if hdr_len != MVCC_LOG_HEADER_SIZE {
        return Err(Error::DatabaseSyncEngineError(format!(
            "invalid MVCC logical-log header length {hdr_len}"
        )));
    }
    let stored_crc = read_u32_le(buf, MVCC_LOG_HEADER_CRC_START)?;
    let mut crc_buf = [0u8; MVCC_LOG_HEADER_SIZE];
    crc_buf.copy_from_slice(&buf[..MVCC_LOG_HEADER_SIZE]);
    crc_buf[MVCC_LOG_HEADER_CRC_START..MVCC_LOG_HEADER_SIZE].fill(0);
    if crc32c::crc32c(&crc_buf) != stored_crc {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical-log header checksum mismatch".to_string(),
        ));
    }
    if buf[MVCC_LOG_HEADER_RESERVED_START..MVCC_LOG_HEADER_CRC_START]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical-log header reserved bytes must be zero".to_string(),
        ));
    }
    let salt = u64::from_le_bytes(
        buf[MVCC_LOG_HEADER_SALT_START..MVCC_LOG_HEADER_SALT_END]
            .try_into()
            .expect("fixed-size salt slice"),
    );
    Ok(crc32c::crc32c(&salt.to_le_bytes()))
}

fn decode_mvcc_log_crc_seed(seed: &[u8]) -> Result<u32> {
    if seed.len() != std::mem::size_of::<u32>() {
        return Err(Error::DatabaseSyncEngineError(format!(
            "invalid MVCC logical-log CRC seed length {}",
            seed.len()
        )));
    }
    Ok(u32::from_le_bytes(
        seed.try_into().expect("validated CRC seed length"),
    ))
}

fn read_mvcc_proto_varint(buf: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    while *cursor < buf.len() {
        let byte = buf[*cursor];
        *cursor += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(Error::DatabaseSyncEngineError(
                "MVCC logical-log varint overflows u64".to_string(),
            ));
        }
    }
    Err(Error::DatabaseSyncEngineError(
        "truncated MVCC logical-log varint".to_string(),
    ))
}

fn read_mvcc_sqlite_varint(buf: &[u8], cursor: &mut usize) -> Result<u64> {
    let (value, len) = read_sqlite_varint(&buf[*cursor..]).map_err(|err| {
        Error::DatabaseSyncEngineError(format!("invalid MVCC logical-log SQLite varint: {err}"))
    })?;
    *cursor = cursor.checked_add(len).ok_or_else(|| {
        Error::DatabaseSyncEngineError("MVCC logical-log SQLite varint offset overflow".to_string())
    })?;
    Ok(value)
}

fn skip_mvcc_proto_field(buf: &[u8], cursor: &mut usize, wire_type: u64) -> Result<()> {
    match wire_type {
        0 => {
            let _ = read_mvcc_proto_varint(buf, cursor)?;
        }
        2 => {
            let len = usize::try_from(read_mvcc_proto_varint(buf, cursor)?).map_err(|_| {
                Error::DatabaseSyncEngineError(
                    "MVCC logical-log protobuf length overflows usize".to_string(),
                )
            })?;
            let end = cursor.checked_add(len).ok_or_else(|| {
                Error::DatabaseSyncEngineError(
                    "MVCC logical-log protobuf length overflow".to_string(),
                )
            })?;
            if end > buf.len() {
                return Err(Error::DatabaseSyncEngineError(
                    "truncated MVCC logical-log protobuf field".to_string(),
                ));
            }
            *cursor = end;
        }
        other => {
            return Err(Error::DatabaseSyncEngineError(format!(
                "unsupported MVCC logical-log protobuf wire type {other}"
            )));
        }
    }
    Ok(())
}

fn decode_mvcc_delete_record(extension: &[u8], record_field: u64) -> Result<Vec<u8>> {
    let mut cursor = 0usize;
    let mut record = Vec::new();
    while cursor < extension.len() {
        let key = read_mvcc_proto_varint(extension, &mut cursor)?;
        let field = key >> 3;
        let wire_type = key & 7;
        if field == record_field && wire_type == 2 {
            let len =
                usize::try_from(read_mvcc_proto_varint(extension, &mut cursor)?).map_err(|_| {
                    Error::DatabaseSyncEngineError(
                        "MVCC delete record length overflows usize".to_string(),
                    )
                })?;
            let end = cursor.checked_add(len).ok_or_else(|| {
                Error::DatabaseSyncEngineError("MVCC delete record length overflow".to_string())
            })?;
            if end > extension.len() {
                return Err(Error::DatabaseSyncEngineError(
                    "truncated MVCC delete record".to_string(),
                ));
            }
            record = extension[cursor..end].to_vec();
            cursor = end;
        } else {
            skip_mvcc_proto_field(extension, &mut cursor, wire_type)?;
        }
    }
    Ok(record)
}

fn portable_string(strings: &[String], idx: u64, context: &str) -> Result<String> {
    let idx = usize::try_from(idx).map_err(|_| {
        Error::DatabaseSyncEngineError(format!("{context} string ref overflows usize"))
    })?;
    strings.get(idx).cloned().ok_or_else(|| {
        Error::DatabaseSyncEngineError(format!("{context} references missing string {idx}"))
    })
}

fn portable_txn_strings(txn: &PortableLogicalTxn) -> Result<Vec<String>> {
    txn.string_table
        .iter()
        .map(|bytes| {
            String::from_utf8(bytes.to_vec()).map_err(|err| {
                Error::DatabaseSyncEngineError(format!(
                    "portable MVCC logical-log string is not UTF-8: {err}"
                ))
            })
        })
        .collect()
}

fn portable_object_names(txn: &PortableLogicalTxn) -> Result<BTreeMap<i64, String>> {
    let strings = portable_txn_strings(txn)?;
    let mut names = BTreeMap::new();
    for object in &txn.object_map {
        names.insert(
            object.mv_table_id,
            portable_string(&strings, object.name_ref, "portable object map")?,
        );
    }
    Ok(names)
}

fn portable_origin_client_id(txn: &PortableLogicalTxn) -> Result<String> {
    let strings = portable_txn_strings(txn)?;
    for meta in &txn.meta {
        let key = portable_string(&strings, meta.key_ref, "portable metadata key")?;
        if key == PORTABLE_TXN_META_CLIENT_KEY {
            return portable_string(&strings, meta.value_ref, "portable metadata value");
        }
    }
    Ok(String::new())
}

#[derive(Clone)]
struct DecodedSchemaRow {
    row_type: String,
    name: String,
    sql: String,
}

#[derive(Default)]
struct SchemaRowDelta {
    old: Option<DecodedSchemaRow>,
    new: Option<DecodedSchemaRow>,
}

fn schema_text_value(value: &Value, field: &str) -> Result<String> {
    match value {
        Value::Text(text) => Ok(text.as_str().to_string()),
        Value::Null => Ok(String::new()),
        other => Err(Error::DatabaseSyncEngineError(format!(
            "sqlite_schema.{field} must be text, got {other:?}"
        ))),
    }
}

fn schema_integer_value(value: &Value, field: &str) -> Result<i64> {
    match value.as_int() {
        Some(value) => Ok(value),
        None => Err(Error::DatabaseSyncEngineError(format!(
            "sqlite_schema.{field} must be integer, got {value:?}"
        ))),
    }
}

fn decode_schema_row(record: &[u8]) -> Result<DecodedSchemaRow> {
    let values = parse_bin_record(record)?;
    if values.len() < 5 {
        return Err(Error::DatabaseSyncEngineError(format!(
            "sqlite_schema record must have at least 5 columns, got {}",
            values.len()
        )));
    }
    Ok(DecodedSchemaRow {
        row_type: schema_text_value(&values[0], "type")?,
        name: schema_text_value(&values[1], "name")?,
        sql: {
            let _rootpage = schema_integer_value(&values[3], "rootpage")?;
            schema_text_value(&values[4], "sql")?
        },
    })
}

fn logical_schema_kind_from_row(row: &DecodedSchemaRow) -> Result<LogicalSchemaKind> {
    if row.row_type.eq_ignore_ascii_case("table") {
        Ok(LogicalSchemaKind::Table)
    } else if row.row_type.eq_ignore_ascii_case("index") {
        Ok(LogicalSchemaKind::Index)
    } else if row.row_type.eq_ignore_ascii_case("trigger") {
        Ok(LogicalSchemaKind::Trigger)
    } else if row.row_type.eq_ignore_ascii_case("view") {
        Ok(LogicalSchemaKind::View)
    } else {
        Err(Error::DatabaseSyncEngineError(format!(
            "unsupported sqlite_schema object type {}",
            row.row_type
        )))
    }
}

fn schema_logical_op(row: &DecodedSchemaRow, action: LogicalSchemaAction) -> Result<LogicalOp> {
    Ok(LogicalOp {
        op_type: LogicalOpType::Schema as i32,
        table_name: String::new(),
        rowid: 0,
        record: Bytes::new(),
        sql: if action == LogicalSchemaAction::Drop {
            String::new()
        } else {
            row.sql.clone()
        },
        user_version: None,
        application_id: None,
        schema_action: Some(action as i32),
        schema_kind: Some(logical_schema_kind_from_row(row)? as i32),
        schema_name: row.name.clone(),
        stable_table_id: 0,
    })
}

fn append_schema_ops(
    deltas: BTreeMap<i64, SchemaRowDelta>,
    ops: &mut Vec<LogicalOp>,
) -> Result<()> {
    for delta in deltas.into_values() {
        match (delta.old, delta.new) {
            (Some(old), Some(new)) => {
                if is_logically_replayable_table(&old.name) {
                    ops.push(schema_logical_op(&new, LogicalSchemaAction::Refresh)?);
                }
            }
            (None, Some(new)) => {
                if is_logically_replayable_table(&new.name) {
                    ops.push(schema_logical_op(&new, LogicalSchemaAction::Create)?);
                }
            }
            (Some(old), None) => {
                if is_logically_replayable_table(&old.name) {
                    ops.push(schema_logical_op(&old, LogicalSchemaAction::Drop)?);
                }
            }
            (None, None) => {}
        }
    }
    Ok(())
}

fn decode_update_header_op(payload: &[u8]) -> Result<LogicalOp> {
    if payload.len() < 72 || &payload[..16] != b"SQLite format 3\0" {
        return Err(Error::DatabaseSyncEngineError(
            "invalid MVCC UPDATE_HEADER payload".to_string(),
        ));
    }
    Ok(LogicalOp {
        op_type: LogicalOpType::UpdateHeader as i32,
        table_name: String::new(),
        rowid: 0,
        record: Bytes::new(),
        sql: String::new(),
        user_version: Some(u32::from_be_bytes(payload[60..64].try_into().unwrap())),
        application_id: Some(u32::from_be_bytes(payload[68..72].try_into().unwrap())),
        schema_action: None,
        schema_kind: None,
        schema_name: String::new(),
        stable_table_id: 0,
    })
}

fn decode_recovery_ops_to_logical_txn(
    portable_txn: PortableLogicalTxn,
    recovery_payload: &[u8],
    op_count: u32,
) -> Result<LogicalTxnData> {
    let object_names = portable_object_names(&portable_txn)?;
    let origin_client_id = portable_origin_client_id(&portable_txn)?;
    let mut cursor = 0usize;
    let mut schema_deltas = BTreeMap::<i64, SchemaRowDelta>::new();
    let mut row_ops = Vec::new();
    let mut header_ops = Vec::new();

    for _ in 0..op_count {
        if recovery_payload.len().saturating_sub(cursor) < 6 {
            return Err(Error::DatabaseSyncEngineError(
                "truncated MVCC logical-log recovery op".to_string(),
            ));
        }
        let tag = recovery_payload[cursor];
        let flags = recovery_payload[cursor + 1];
        let table_id =
            i32::from_le_bytes(recovery_payload[cursor + 2..cursor + 6].try_into().unwrap()) as i64;
        cursor += 6;
        let payload_len = usize::try_from(read_mvcc_sqlite_varint(recovery_payload, &mut cursor)?)
            .map_err(|_| {
                Error::DatabaseSyncEngineError(
                    "MVCC logical-log op payload length overflows usize".to_string(),
                )
            })?;
        let payload_end = cursor.checked_add(payload_len).ok_or_else(|| {
            Error::DatabaseSyncEngineError("MVCC logical-log op payload overflow".to_string())
        })?;
        if payload_end > recovery_payload.len() {
            return Err(Error::DatabaseSyncEngineError(
                "truncated MVCC logical-log op payload".to_string(),
            ));
        }
        let payload = &recovery_payload[cursor..payload_end];
        cursor = payload_end;
        let portable_extension = if flags & MVCC_OP_FLAG_PORTABLE_EXTENSION == 0 {
            &[][..]
        } else {
            let extension_len =
                usize::try_from(read_mvcc_sqlite_varint(recovery_payload, &mut cursor)?).map_err(
                    |_| {
                        Error::DatabaseSyncEngineError(
                            "MVCC logical-log op extension length overflows usize".to_string(),
                        )
                    },
                )?;
            let extension_end = cursor.checked_add(extension_len).ok_or_else(|| {
                Error::DatabaseSyncEngineError(
                    "MVCC logical-log op extension length overflow".to_string(),
                )
            })?;
            if extension_end > recovery_payload.len() {
                return Err(Error::DatabaseSyncEngineError(
                    "truncated MVCC logical-log op extension".to_string(),
                ));
            }
            let extension = &recovery_payload[cursor..extension_end];
            cursor = extension_end;
            extension
        };

        match tag {
            MVCC_OP_UPSERT_TABLE => {
                let mut payload_cursor = 0usize;
                let rowid = read_mvcc_sqlite_varint(payload, &mut payload_cursor)? as i64;
                let record = &payload[payload_cursor..];
                if table_id == MVCC_SQLITE_SCHEMA_TABLE_ID {
                    schema_deltas.entry(rowid).or_default().new = Some(decode_schema_row(record)?);
                } else if let Some(table_name) = object_names.get(&table_id) {
                    row_ops.push(LogicalOp {
                        op_type: LogicalOpType::UpsertRow as i32,
                        table_name: table_name.clone(),
                        rowid,
                        record: Bytes::copy_from_slice(record),
                        sql: String::new(),
                        user_version: None,
                        application_id: None,
                        schema_action: None,
                        schema_kind: None,
                        schema_name: String::new(),
                        stable_table_id: 0,
                    });
                }
            }
            MVCC_OP_DELETE_TABLE => {
                let mut payload_cursor = 0usize;
                let rowid = read_mvcc_sqlite_varint(payload, &mut payload_cursor)? as i64;
                if table_id == MVCC_SQLITE_SCHEMA_TABLE_ID {
                    let identity_record = decode_mvcc_delete_record(
                        portable_extension,
                        MVCC_DELETE_EXT_IDENTITY_RECORD_FIELD,
                    )?;
                    if identity_record.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "MVCC sqlite_schema delete is missing portable identity record"
                                .to_string(),
                        ));
                    }
                    schema_deltas.entry(rowid).or_default().old =
                        Some(decode_schema_row(&identity_record)?);
                } else if let Some(table_name) = object_names.get(&table_id) {
                    let primary_key_record = decode_mvcc_delete_record(
                        portable_extension,
                        MVCC_DELETE_EXT_PK_RECORD_FIELD,
                    )?;
                    row_ops.push(LogicalOp {
                        op_type: LogicalOpType::DeleteRow as i32,
                        table_name: table_name.clone(),
                        rowid,
                        record: Bytes::from(primary_key_record),
                        sql: String::new(),
                        user_version: None,
                        application_id: None,
                        schema_action: None,
                        schema_kind: None,
                        schema_name: String::new(),
                        stable_table_id: 0,
                    });
                }
            }
            MVCC_OP_UPSERT_INDEX | MVCC_OP_DELETE_INDEX => {}
            MVCC_OP_UPDATE_HEADER => {
                header_ops.push(decode_update_header_op(payload)?);
            }
            other => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "unknown MVCC logical-log recovery op tag {other}"
                )));
            }
        }
    }
    if cursor != recovery_payload.len() {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical-log recovery payload has trailing bytes".to_string(),
        ));
    }

    let mut ops = header_ops;
    append_schema_ops(schema_deltas, &mut ops)?;
    // Every row delete in a transaction is applied before every row upsert.
    //
    // MVCC coalesces a transaction to one final version per rowid, and a row
    // whose primary key changed arrives as a delete of its old key followed by
    // an upsert of its new image. Applying those pairs row by row breaks as soon
    // as two rows exchange keys inside one transaction: the second row's delete
    // targets the key the first row's upsert just took, so it removes the row
    // that was just written and the replica silently ends up one row short.
    //
    // Draining the deletes first applies the transaction as a set difference,
    // which is also what lets both upserts land without tripping the unique
    // index on an intermediate state — the remote needed a temporary key to make
    // the same swap statement by statement. A rowid appears at most once per
    // coalesced transaction, so no upsert can depend on a delete of its own row
    // running later.
    let (row_deletes, row_upserts): (Vec<_>, Vec<_>) = row_ops
        .into_iter()
        .partition(|op| op.op_type == LogicalOpType::DeleteRow as i32);
    ops.extend(row_deletes);
    ops.extend(row_upserts);

    Ok(LogicalTxnData {
        end_offset: portable_txn.end_offset,
        commit_ts: portable_txn.commit_ts,
        ops,
        origin_client_id,
    })
}

fn take_proto_message_from_bytes<T: prost::Message + Default>(
    bytes: &mut BytesMut,
) -> Result<Option<T>> {
    let Some((message_length, prefix_length)) = read_varint(bytes)? else {
        return Ok(None);
    };
    if message_length + prefix_length > bytes.len() {
        return Ok(None);
    }
    let message = T::decode_length_delimited(&**bytes).map_err(|e| {
        Error::DatabaseSyncEngineError(format!(
            "unable to deserialize protobuf message from file: {e}"
        ))
    })?;
    let _ = bytes.split_to(message_length + prefix_length);
    Ok(Some(message))
}

async fn append_decoded_mvcc_frame_to_txns_file<Ctx>(
    coro: &Coro<Ctx>,
    txns_file: &Arc<dyn turso_core::File>,
    txns_offset: &mut u64,
    portable_payload: &[u8],
    recovery_payload: &[u8],
    op_count: u32,
) -> Result<(usize, usize)> {
    let mut payload_bytes = BytesMut::from(portable_payload);
    let mut txns = 0usize;
    let mut ops = 0usize;
    while let Some(portable_txn) =
        take_proto_message_from_bytes::<PortableLogicalTxn>(&mut payload_bytes)?
    {
        let txn = decode_recovery_ops_to_logical_txn(portable_txn, recovery_payload, op_count)?;
        if txn.ops.is_empty() {
            continue;
        }
        ops += txn.ops.len();
        txns += 1;
        append_file_bytes(
            coro,
            txns_file,
            txns_offset,
            &txn.encode_length_delimited_to_vec(),
        )
        .await?;
    }
    if !payload_bytes.is_empty() {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical-log portable payload has trailing partial protobuf message".to_string(),
        ));
    }
    Ok((txns, ops))
}

pub async fn decode_raw_mvcc_logical_log_to_file<Ctx>(
    coro: &Coro<Ctx>,
    txns_file: &Arc<dyn turso_core::File>,
    body: &[u8],
    header: &PullUpdatesRespProtoBody,
) -> Result<(usize, usize)> {
    let Some(metadata) = header.mvcc_log.as_ref() else {
        if body.is_empty() {
            truncate_file(coro, txns_file).await?;
            return Ok((0, 0));
        }
        return Err(Error::DatabaseSyncEngineError(
            "raw MVCC logical-log stream is missing metadata".to_string(),
        ));
    };
    if metadata.format != "lml3" {
        return Err(Error::DatabaseSyncEngineError(format!(
            "unsupported MVCC logical-log format {}",
            metadata.format
        )));
    }
    if metadata.ranges.is_empty() {
        return Err(Error::DatabaseSyncEngineError(
            "raw MVCC logical-log stream has no ranges".to_string(),
        ));
    }

    truncate_file(coro, txns_file).await?;
    let mut body_offset = 0usize;
    let mut txns_offset = 0u64;
    let mut txns = 0usize;
    let mut ops = 0usize;
    let mut running_crc: Option<u32> = None;
    let mut previous_range_boundary: Option<(u64, u64)> = None;

    for range in &metadata.ranges {
        if previous_range_boundary != Some((range.generation, range.start_offset)) {
            running_crc = None;
        }
        let range_len = usize::try_from(
            range
                .end_offset
                .checked_sub(range.start_offset)
                .ok_or_else(|| {
                    Error::DatabaseSyncEngineError(format!(
                        "invalid MVCC logical-log range {}..{}",
                        range.start_offset, range.end_offset
                    ))
                })?,
        )
        .map_err(|_| {
            Error::DatabaseSyncEngineError(
                "MVCC logical-log range length overflows usize".to_string(),
            )
        })?;
        let range_end = body_offset.checked_add(range_len).ok_or_else(|| {
            Error::DatabaseSyncEngineError("MVCC logical-log range length overflow".to_string())
        })?;
        let range_body = body.get(body_offset..range_end).ok_or_else(|| {
            Error::DatabaseSyncEngineError(format!(
                "raw MVCC logical-log body is shorter than advertised range {}..{}",
                range.start_offset, range.end_offset
            ))
        })?;
        body_offset = range_end;

        let mut pos = 0usize;
        if range.starts_with_header {
            running_crc = Some(validate_mvcc_log_header(range_body)?);
            pos = MVCC_LOG_HEADER_SIZE;
        } else if let Some(seed) = range.crc_seed.as_deref() {
            running_crc = Some(decode_mvcc_log_crc_seed(seed)?);
        } else if running_crc.is_none() {
            return Err(Error::DatabaseSyncEngineError(format!(
                "raw MVCC logical-log range {}..{} is missing CRC seed",
                range.start_offset, range.end_offset
            )));
        }

        while pos < range_body.len() {
            let frame_start = pos;
            if range_body.len() - pos < MVCC_TX_HEADER_SIZE + MVCC_TX_TRAILER_SIZE {
                return Err(Error::DatabaseSyncEngineError(
                    "truncated MVCC logical-log transaction frame".to_string(),
                ));
            }
            let frame_magic = read_u32_le(range_body, pos)?;
            let has_extension_header = frame_magic == MVCC_TX_EXT_FRAME_MAGIC;
            if frame_magic != MVCC_TX_FRAME_MAGIC && !has_extension_header {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "invalid MVCC logical-log transaction frame magic at range offset {frame_start}"
                )));
            }
            let header_size = if has_extension_header {
                if range_body.len() - pos < MVCC_TX_EXT_HEADER_SIZE + MVCC_TX_TRAILER_SIZE {
                    return Err(Error::DatabaseSyncEngineError(
                        "truncated MVCC logical-log transaction frame".to_string(),
                    ));
                }
                MVCC_TX_EXT_HEADER_SIZE
            } else {
                MVCC_TX_HEADER_SIZE
            };
            let payload_size =
                usize::try_from(read_u64_le(range_body, pos + 4)?).map_err(|_| {
                    Error::DatabaseSyncEngineError(
                        "MVCC logical-log recovery payload size overflows usize".to_string(),
                    )
                })?;
            let op_count = read_u32_le(range_body, pos + 12)?;
            let (extension_size, extension_record_count) = if has_extension_header {
                let extension_size =
                    usize::try_from(read_u64_le(range_body, pos + 24)?).map_err(|_| {
                        Error::DatabaseSyncEngineError(
                            "MVCC logical-log extension size overflows usize".to_string(),
                        )
                    })?;
                let extension_record_count = read_u32_le(range_body, pos + 32)?;
                let frame_flags = read_u32_le(range_body, pos + 36)?;
                if frame_flags & !MVCC_TX_FLAG_HAS_EXTENSION_BLOCK != 0 {
                    return Err(Error::DatabaseSyncEngineError(format!(
                        "unsupported MVCC logical-log transaction flags {frame_flags:#x}"
                    )));
                }
                if extension_size == 0 && extension_record_count != 0 {
                    return Err(Error::DatabaseSyncEngineError(
                        "MVCC logical-log frame has extension record count without extension block"
                            .to_string(),
                    ));
                }
                if extension_size > 0 && frame_flags & MVCC_TX_FLAG_HAS_EXTENSION_BLOCK == 0 {
                    return Err(Error::DatabaseSyncEngineError(
                        "MVCC logical-log frame has extension block without extension flag"
                            .to_string(),
                    ));
                }
                (extension_size, extension_record_count)
            } else {
                (0, 0)
            };

            let extension_start = pos.checked_add(header_size).ok_or_else(|| {
                Error::DatabaseSyncEngineError("MVCC logical-log frame offset overflow".to_string())
            })?;
            let recovery_start = extension_start.checked_add(extension_size).ok_or_else(|| {
                Error::DatabaseSyncEngineError("MVCC logical-log frame offset overflow".to_string())
            })?;
            let trailer_start = recovery_start.checked_add(payload_size).ok_or_else(|| {
                Error::DatabaseSyncEngineError("MVCC logical-log frame offset overflow".to_string())
            })?;
            let frame_end = trailer_start
                .checked_add(MVCC_TX_TRAILER_SIZE)
                .ok_or_else(|| {
                    Error::DatabaseSyncEngineError(
                        "MVCC logical-log frame offset overflow".to_string(),
                    )
                })?;
            if frame_end > range_body.len() {
                return Err(Error::DatabaseSyncEngineError(
                    "truncated MVCC logical-log transaction frame".to_string(),
                ));
            }
            if read_u32_le(range_body, trailer_start + 4)? != MVCC_TX_END_MAGIC {
                return Err(Error::DatabaseSyncEngineError(
                    "invalid MVCC logical-log transaction trailer magic".to_string(),
                ));
            }
            if let Some(prev_crc) = running_crc {
                let expected_crc =
                    crc32c::crc32c_append(prev_crc, &range_body[frame_start..trailer_start]);
                let stored_crc = read_u32_le(range_body, trailer_start)?;
                if expected_crc != stored_crc {
                    return Err(Error::DatabaseSyncEngineError(format!(
                        "MVCC logical-log transaction checksum mismatch at range offset {frame_start}"
                    )));
                }
                running_crc = Some(stored_crc);
            }

            if extension_size == 0 {
                pos = frame_end;
                continue;
            }
            let extension_block = &range_body[extension_start..recovery_start];
            let recovery_payload = &range_body[recovery_start..trailer_start];
            let portable_payload = find_mvcc_extension_payload(
                extension_block,
                extension_record_count,
                MVCC_EXTENSION_TYPE_PORTABLE_CHANGES,
            )?;
            if portable_payload.is_empty() {
                pos = frame_end;
                continue;
            }
            let (frame_txns, frame_ops) = append_decoded_mvcc_frame_to_txns_file(
                coro,
                txns_file,
                &mut txns_offset,
                &portable_payload,
                recovery_payload,
                op_count,
            )
            .await?;
            txns += frame_txns;
            ops += frame_ops;
            pos = frame_end;
        }
        previous_range_boundary = Some((range.generation, range.end_offset));
    }

    if body_offset != body.len() {
        return Err(Error::DatabaseSyncEngineError(
            "raw MVCC logical-log body has trailing bytes after advertised ranges".to_string(),
        ));
    }
    sync_file(coro, txns_file).await?;
    Ok((txns, ops))
}

/// Bootstrap multiple DB files from latest generation from remote
pub async fn db_bootstrap<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    db: Arc<dyn turso_core::File>,
) -> Result<DbSyncInfo> {
    tracing::info!("db_bootstrap");
    let start_time = std::time::Instant::now();
    let db_info = db_info_http(ctx).await?;
    tracing::info!("db_bootstrap: fetched db_info={db_info:?}");
    let content = db_bootstrap_http(ctx, db_info.current_generation).await?;
    let mut pos = 0;
    loop {
        while let Some(chunk) = content.poll_data()? {
            ctx.io.network_stats.read(chunk.data().len());
            let chunk = chunk.data();
            let content_len = chunk.len();

            // todo(sivukhin): optimize allocations here
            #[allow(clippy::arc_with_non_send_sync)]
            let buffer = Arc::new(Buffer::new_temporary(chunk.len()));
            buffer.as_mut_slice().copy_from_slice(chunk);
            let c = Completion::new_write(move |result| {
                // todo(sivukhin): we need to error out in case of partial read
                let Ok(size) = result else {
                    return;
                };
                assert!(size as usize == content_len);
            });
            let c = db.pwrite(pos, buffer.clone(), c)?;
            while !c.succeeded() {
                ctx.coro.yield_(SyncEngineIoResult::IO).await?;
            }
            pos += content_len as u64;
        }
        if content.is_done()? {
            break;
        }
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }

    // sync file in the end
    sync_file(ctx.coro, &db).await?;

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::info!("db_bootstrap: finished: bytes={pos}, elapsed={:?}", elapsed);

    Ok(db_info)
}

pub async fn wal_apply_from_file<Ctx>(
    coro: &Coro<Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    session: &mut DatabaseWalSession,
) -> Result<u32> {
    let size = frames_file.size()?;
    assert!(size % WAL_FRAME_SIZE as u64 == 0);
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    tracing::info!("wal_apply_from_file: size={}", size);
    let mut db_size = 0;
    for offset in (0..size).step_by(WAL_FRAME_SIZE) {
        let c = Completion::new_read(buffer.clone(), move |result| {
            let Ok((_, size)) = result else {
                return None;
            };
            // todo(sivukhin): we need to error out in case of partial read
            assert!(size as usize == WAL_FRAME_SIZE);
            None
        });
        let c = frames_file.pread(offset, c)?;
        while !c.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        let info = WalFrameInfo::from_frame_header(buffer.as_slice());
        tracing::debug!("got frame: {:?}", info);
        db_size = info.db_size;
        session.append_page(info.page_no, &buffer.as_slice()[WAL_FRAME_HEADER..])?;
    }
    assert!(db_size > 0);
    Ok(db_size)
}

pub async fn wal_pull_to_file<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &Option<DatabasePullRevision>,
    wal_pull_batch_size: u64,
    long_poll_timeout: Option<std::time::Duration>,
) -> Result<DatabasePullRevision> {
    // truncate file before pulling new data
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = frames_file.truncate(0, c)?;
    while !c.succeeded() {
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }
    match revision {
        Some(DatabasePullRevision::Legacy {
            generation,
            synced_frame_no,
        }) => {
            let start_frame = synced_frame_no.unwrap_or(0) + 1;
            wal_pull_to_file_legacy(
                ctx,
                frames_file,
                *generation,
                start_frame,
                wal_pull_batch_size,
            )
            .await
        }
        Some(DatabasePullRevision::V1 { revision }) => {
            wal_pull_to_file_v1(ctx, frames_file, revision, long_poll_timeout).await
        }
        None => wal_pull_to_file_v1(ctx, frames_file, "", long_poll_timeout).await,
    }
}

/// Pulls V1 updates as either page frames or raw MVCC logical-log bytes.
///
/// Existing WAL sync calls this with `logical_updates=false`, which keeps the
/// request and scratch-file format page-based. Logical callers opt in
/// explicitly and receive decoded `LogicalTxnData` messages in `frames_file`.
pub async fn pull_updates_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &str,
    long_poll_timeout: Option<std::time::Duration>,
    logical_updates: bool,
) -> Result<(
    DatabasePullRevision,
    PullUpdatesV1Result,
    RemotePullProtocol,
)> {
    tracing::info!(
        "pull_updates_v1: remote_url={:?} revision={} logical_updates={} long_poll_timeout_ms={}",
        ctx.remote_url,
        revision,
        logical_updates,
        long_poll_timeout.map(|x| x.as_millis()).unwrap_or(0)
    );
    if logical_updates && ctx.remote_encryption_key.is_some() {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical pull is not supported with encrypted remote databases".to_string(),
        ));
    }
    let mut bytes = BytesMut::new();

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        stream_kind: if logical_updates {
            PullUpdatesStreamKind::MvccLogicalLog as i32
        } else {
            PullUpdatesStreamKind::Pages as i32
        },
        server_revision: String::new(),
        client_revision: revision.to_string(),
        long_poll_timeout_ms: long_poll_timeout.map(|x| x.as_millis() as u32).unwrap_or(0),
        server_pages_selector: BytesMut::new().into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!(
        "pull_updates_v1: got header: remote_url={:?} server_revision={} stream_kind={} apply_mode={} db_size={} raw_encoding={:?} zstd_encoding={:?} mvcc_log={:?}",
        ctx.remote_url,
        header.server_revision,
        header.stream_kind,
        header.apply_mode,
        header.db_size,
        header.raw_encoding,
        header.zstd_encoding,
        header.mvcc_log
    );

    let next_revision = DatabasePullRevision::V1 {
        revision: header.server_revision.clone(),
    };
    let remote_protocol = detect_remote_pull_protocol(&header);
    let apply_mode = pull_updates_apply_mode(&header)?;
    match pull_updates_stream_kind(&header)? {
        PullUpdatesStreamKind::Pages => {
            let replace_base = matches!(apply_mode, PullUpdatesApplyMode::ReplaceBase);
            truncate_file(ctx.coro, frames_file).await?;

            let mut offset = 0;
            #[allow(clippy::arc_with_non_send_sync)]
            let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));

            let mut page_data_opt = wait_proto_message::<Ctx, PageData>(
                ctx.coro,
                &completion,
                &ctx.io.network_stats,
                &mut bytes,
            )
            .await?;
            while let Some(page_data) = page_data_opt.take() {
                let page_id = page_data.page_id;
                tracing::debug!("received page {}", page_id);
                let page = decode_page(&header, page_data)?;
                if page.len() != PAGE_SIZE {
                    return Err(Error::DatabaseSyncEngineError(format!(
                        "page has unexpected size: {} != {}",
                        page.len(),
                        PAGE_SIZE
                    )));
                }
                buffer.as_mut_slice()[WAL_FRAME_HEADER..].copy_from_slice(&page);
                page_data_opt =
                    wait_proto_message(ctx.coro, &completion, &ctx.io.network_stats, &mut bytes)
                        .await?;
                let mut frame_info = WalFrameInfo {
                    db_size: 0,
                    page_no: page_id as u32 + 1,
                };
                if page_data_opt.is_none() {
                    frame_info.db_size = header.db_size as u32;
                }
                tracing::debug!("page_data_opt: {}", page_data_opt.is_some());
                frame_info.put_to_frame_header(buffer.as_mut_slice());

                let c = Completion::new_write(move |result| {
                    let Ok(size) = result else {
                        return;
                    };
                    assert!(size as usize == WAL_FRAME_SIZE);
                });

                let c = frames_file.pwrite(offset, buffer.clone(), c)?;
                while !c.succeeded() {
                    ctx.coro.yield_(SyncEngineIoResult::IO).await?;
                }
                offset += WAL_FRAME_SIZE as u64;
            }

            sync_file(ctx.coro, frames_file).await?;

            tracing::info!(
                "pull_updates_v1: completed page stream: remote_url={:?} next_revision={:?} replace_base={}",
                ctx.remote_url,
                next_revision,
                replace_base
            );
            Ok((
                next_revision,
                PullUpdatesV1Result::Pages { replace_base },
                remote_protocol,
            ))
        }
        PullUpdatesStreamKind::MvccLogicalLog => {
            if matches!(apply_mode, PullUpdatesApplyMode::ReplaceBase) {
                return Err(Error::DatabaseSyncEngineError(
                    "server returned replace_base apply mode with raw MVCC logical-log stream"
                        .to_string(),
                ));
            }
            if !logical_updates {
                return Err(Error::DatabaseSyncEngineError(
                    "server returned a raw MVCC logical-log stream but logical_updates was not requested"
                        .to_string(),
                ));
            }
            let mut body = bytes.split().to_vec();
            body.extend_from_slice(
                &wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?,
            );
            let (txns, ops) =
                decode_raw_mvcc_logical_log_to_file(ctx.coro, frames_file, &body, &header).await?;
            tracing::info!(
                "pull_updates_v1: completed raw MVCC logical-log stream: remote_url={:?} next_revision={:?} txns={} ops={}",
                ctx.remote_url,
                next_revision,
                txns,
                ops
            );
            Ok((
                next_revision,
                PullUpdatesV1Result::Logical { txns, ops },
                remote_protocol,
            ))
        }
    }
}

/// Pull updates from remote to the separate file
pub async fn wal_pull_to_file_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &str,
    long_poll_timeout: Option<std::time::Duration>,
) -> Result<DatabasePullRevision> {
    tracing::info!("wal_pull: revision={revision}");
    let mut bytes = BytesMut::new();

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        server_revision: String::new(),
        client_revision: revision.to_string(),
        long_poll_timeout_ms: long_poll_timeout.map(|x| x.as_millis() as u32).unwrap_or(0),
        server_pages_selector: BytesMut::new().into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!("wal_pull_to_file: got header={:?}", header);
    ensure_incremental_page_stream(&header, "wal_pull_to_file_v1")?;

    let mut offset = 0;
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));

    let mut page_data_opt = wait_proto_message::<Ctx, PageData>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?;
    while let Some(page_data) = page_data_opt.take() {
        let page_id = page_data.page_id;
        tracing::debug!("received page {}", page_id);
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        buffer.as_mut_slice()[WAL_FRAME_HEADER..].copy_from_slice(&page);
        page_data_opt =
            wait_proto_message(ctx.coro, &completion, &ctx.io.network_stats, &mut bytes).await?;
        let mut frame_info = WalFrameInfo {
            db_size: 0,
            page_no: page_id as u32 + 1,
        };
        if page_data_opt.is_none() {
            frame_info.db_size = header.db_size as u32;
        }
        tracing::debug!("page_data_opt: {}", page_data_opt.is_some());
        frame_info.put_to_frame_header(buffer.as_mut_slice());

        let c = Completion::new_write(move |result| {
            // todo(sivukhin): we need to error out in case of partial read
            let Ok(size) = result else {
                return;
            };
            assert!(size as usize == WAL_FRAME_SIZE);
        });

        let c = frames_file.pwrite(offset, buffer.clone(), c)?;
        while !c.succeeded() {
            ctx.coro.yield_(SyncEngineIoResult::IO).await?;
        }
        offset += WAL_FRAME_SIZE as u64;
    }

    sync_file(ctx.coro, frames_file).await?;

    Ok(DatabasePullRevision::V1 {
        revision: header.server_revision,
    })
}

#[derive(Debug)]
pub struct PulledPage {
    pub page_id: u64,
    pub page: Vec<u8>,
}

#[derive(Debug)]
pub struct PulledPages {
    pub db_pages: u64,
    pub pages: Vec<PulledPage>,
}

/// Pull pages from remote, pages slice must be non-empty
pub async fn pull_pages_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    server_revision: &str,
    pages: &[u32],
) -> Result<PulledPages> {
    tracing::info!("pull_pages_v1: revision={server_revision}, pages={pages:?}");

    assert!(!pages.is_empty(), "pages must be non-empty");

    let mut bytes = BytesMut::new();

    let mut bitmap = RoaringBitmap::new();
    bitmap.extend(pages);

    let mut bitmap_bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
        Error::DatabaseSyncEngineError(format!("unable to serialize pull page request: {e}"))
    })?;

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        server_revision: server_revision.to_string(),
        client_revision: String::new(),
        long_poll_timeout_ms: 0,
        server_pages_selector: bitmap_bytes.into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!("pull_pages_v1: got header={:?}", header);
    ensure_page_stream(&header, "pull_pages_v1")?;

    let mut pages = Vec::with_capacity(pages.len());

    let mut page_data_opt = wait_proto_message::<Ctx, PageData>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?;
    while let Some(page_data) = page_data_opt.take() {
        let page_id = page_data.page_id;
        tracing::debug!("received page {}", page_id);
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        pages.push(PulledPage { page_id, page });
        page_data_opt =
            wait_proto_message(ctx.coro, &completion, &ctx.io.network_stats, &mut bytes).await?;
        tracing::debug!("page_data_opt: {}", page_data_opt.is_some());
    }

    Ok(PulledPages {
        db_pages: header.db_size,
        pages,
    })
}

/// Pull updates from remote to the separate file
pub async fn wal_pull_to_file_legacy<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    mut generation: u64,
    mut start_frame: u64,
    wal_pull_batch_size: u64,
) -> Result<DatabasePullRevision> {
    tracing::info!(
        "wal_pull: generation={generation}, start_frame={start_frame}, wal_pull_batch_size={wal_pull_batch_size}"
    );

    // todo(sivukhin): optimize allocation by using buffer pool in the DatabaseSyncOperations
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    let mut buffer_len = 0;
    let mut last_offset = 0;
    let mut committed_len = 0;
    let revision = loop {
        let end_frame = start_frame + wal_pull_batch_size;
        let result = wal_pull_http(ctx, generation, start_frame, end_frame).await?;
        let data = match result {
            WalHttpPullResult::NeedCheckpoint(status) => {
                assert!(status.status == "checkpoint_needed");
                tracing::info!("wal_pull: need checkpoint: status={status:?}");
                if status.generation == generation && status.max_frame_no < start_frame {
                    tracing::info!("wal_pull: end of history: status={:?}", status);
                    break DatabasePullRevision::Legacy {
                        generation: status.generation,
                        synced_frame_no: Some(status.max_frame_no),
                    };
                }
                generation += 1;
                start_frame = 1;
                continue;
            }
            WalHttpPullResult::Frames(content) => content,
        };
        loop {
            while let Some(chunk) = data.poll_data()? {
                ctx.io.network_stats.read(chunk.data().len());
                let mut chunk = chunk.data();

                while !chunk.is_empty() {
                    let to_fill = (WAL_FRAME_SIZE - buffer_len).min(chunk.len());
                    buffer.as_mut_slice()[buffer_len..buffer_len + to_fill]
                        .copy_from_slice(&chunk[0..to_fill]);
                    buffer_len += to_fill;
                    chunk = &chunk[to_fill..];

                    if buffer_len < WAL_FRAME_SIZE {
                        continue;
                    }
                    let c = Completion::new_write(move |result| {
                        // todo(sivukhin): we need to error out in case of partial read
                        let Ok(size) = result else {
                            return;
                        };
                        assert!(size as usize == WAL_FRAME_SIZE);
                    });
                    let c = frames_file.pwrite(last_offset, buffer.clone(), c)?;
                    while !c.succeeded() {
                        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
                    }

                    last_offset += WAL_FRAME_SIZE as u64;
                    buffer_len = 0;
                    start_frame += 1;

                    let info = WalFrameInfo::from_frame_header(buffer.as_slice());
                    if info.is_commit_frame() {
                        committed_len = last_offset;
                    }
                }
            }
            if data.is_done()? {
                break;
            }
            ctx.coro.yield_(SyncEngineIoResult::IO).await?;
        }
        if start_frame < end_frame {
            // chunk which was sent from the server has ended early - so there is nothing left on server-side for pull
            break DatabasePullRevision::Legacy {
                generation,
                synced_frame_no: Some(start_frame - 1),
            };
        }
        if buffer_len != 0 {
            return Err(Error::DatabaseSyncEngineError(format!(
                "wal_pull: response has unexpected trailing data: buffer_len={buffer_len}"
            )));
        }
    };

    tracing::info!(
        "wal_pull: generation={generation}, frame={start_frame}, last_offset={last_offset}, commited_len={committed_len}"
    );
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = frames_file.truncate(committed_len, c)?;
    while !c.succeeded() {
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }

    sync_file(ctx.coro, frames_file).await?;

    Ok(revision)
}

/// Push frame range [start_frame..end_frame) to the remote
/// Returns baton for WAL remote-session in case of success
/// Returns [Error::DatabaseSyncEngineConflict] in case of frame conflict at remote side
///
/// Guarantees:
/// 1. If there is a single client which calls wal_push, then this operation is idempotent for fixed generation
///    and can be called multiple times with same frame range
pub async fn wal_push<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    wal_session: &mut WalSession,
    baton: Option<String>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
) -> Result<WalPushResult> {
    assert!(wal_session.in_txn());
    tracing::info!("wal_push: baton={baton:?}, generation={generation}, start_frame={start_frame}, end_frame={end_frame}");

    if start_frame == end_frame {
        return Ok(WalPushResult::Ok { baton: None });
    }

    let mut frames_data = Vec::with_capacity((end_frame - start_frame) as usize * WAL_FRAME_SIZE);
    let mut buffer = [0u8; WAL_FRAME_SIZE];
    for frame_no in start_frame..end_frame {
        let frame_info = wal_session.read_at(frame_no, &mut buffer)?;
        tracing::trace!(
            "wal_push: collect frame {} ({:?}) for push",
            frame_no,
            frame_info
        );
        frames_data.extend_from_slice(&buffer);
    }

    let status = wal_push_http(ctx, None, generation, start_frame, end_frame, frames_data).await?;
    if status.status == "ok" {
        Ok(WalPushResult::Ok {
            baton: status.baton,
        })
    } else if status.status == "checkpoint_needed" {
        Ok(WalPushResult::NeedCheckpoint)
    } else if status.status == "conflict" {
        Err(Error::DatabaseSyncEngineConflict(format!(
            "wal_push conflict: {status:?}"
        )))
    } else {
        Err(Error::DatabaseSyncEngineError(format!(
            "wal_push unexpected status: {status:?}"
        )))
    }
}

pub const TURSO_SYNC_TABLE_NAME: &str = "turso_sync_last_change_id";
pub const TURSO_SYNC_CREATE_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS turso_sync_last_change_id (client_id TEXT PRIMARY KEY, pull_gen INTEGER, change_id INTEGER)";
pub const TURSO_SYNC_INSERT_LAST_CHANGE_ID: &str =
    "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, ?, ?)";
pub const TURSO_SYNC_UPSERT_LAST_CHANGE_ID: &str =
    "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, ?, ?) ON CONFLICT(client_id) DO UPDATE SET pull_gen=excluded.pull_gen, change_id=excluded.change_id";
pub const TURSO_SYNC_UPDATE_LAST_CHANGE_ID: &str =
    "UPDATE turso_sync_last_change_id SET pull_gen = ?, change_id = ? WHERE client_id = ?";
const TURSO_SYNC_SELECT_LAST_CHANGE_ID: &str =
    "SELECT pull_gen, change_id FROM turso_sync_last_change_id WHERE client_id = ?";

fn convert_to_args(
    values: impl IntoIterator<Item = turso_core::Value>,
) -> Vec<server_proto::Value> {
    values
        .into_iter()
        .map(|value| match value {
            Value::Null => server_proto::Value::Null,
            Value::Numeric(turso_core::Numeric::Integer(value)) => {
                server_proto::Value::Integer { value }
            }
            Value::Numeric(turso_core::Numeric::Float(value)) => server_proto::Value::Float {
                value: f64::from(value),
            },
            Value::Text(value) => server_proto::Value::Text {
                value: value.as_str().to_string(),
            },
            Value::Blob(value) => server_proto::Value::Blob {
                value: Bytes::copy_from_slice(&value),
            },
        })
        .collect()
}

/// Lists user-created tables (i.e. anything beyond sqlite/turso internals).
/// Used to decide whether local data exists that a replace-base apply would
/// need to preserve.
pub async fn list_user_tables<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'turso_%' AND name NOT LIKE '\\_\\_turso\\_%' ESCAPE '\\'",
    )?;
    let mut names = Vec::new();
    while let Some(row) = run_stmt_once(coro, &mut stmt).await? {
        let name = row
            .get_value(0)
            .to_text()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?
            .to_string();
        names.push(name);
    }
    Ok(names)
}

pub async fn has_table<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?")?;
    stmt.bind_at(
        1.try_into().unwrap(),
        Value::Text(Text::new(table_name.to_string())),
    )?;

    let count = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0]
            .as_int()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?,
        _ => panic!("expected single row"),
    };
    Ok(count > 0)
}

/// Counts local changes above `change_id` which the next push will send to the remote.
/// Rows the push loop drops - COMMIT markers and changes to internal tables like
/// `turso_sync_last_change_id` - must not be counted, otherwise callers see pending
/// work which no push can ever clear.
///
/// COMMIT markers are dropped by `change_type != COMMIT`, which also drops the CDC v2
/// records that store a NULL change type. `sqlite_schema` rows are always counted even
/// though push drops the ones which describe internal objects: telling those apart needs
/// the row payload decoded, so this can overcount DDL on internal tables.
pub async fn count_local_changes<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    opts: &DatabaseSyncEngineOpts,
    change_id: i64,
) -> Result<i64> {
    let ignored_placeholders = vec!["?"; opts.tables_ignore.len()].join(", ");
    let ignored_filter = if opts.tables_ignore.is_empty() {
        String::new()
    } else {
        format!(" AND table_name NOT IN ({ignored_placeholders})")
    };
    let mut stmt = conn.prepare(format!(
        "SELECT COUNT(*) FROM {TURSO_CDC_TABLE_NAME} \
         WHERE change_id > ? \
           AND change_type != {CDC_COMMIT_CHANGE_TYPE} \
           AND (table_name = '{SQLITE_SCHEMA_TABLE}' OR ({})){ignored_filter}",
        logically_replayable_table_filter(),
    ))?;
    stmt.bind_at(1.try_into().unwrap(), Value::from_i64(change_id))?;
    for (i, table) in opts.tables_ignore.iter().enumerate() {
        stmt.bind_at(
            (i + 2).try_into().unwrap(),
            Value::Text(Text::new(table.clone())),
        )?;
    }

    let count = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0]
            .as_int()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?,
        _ => panic!("expected single row"),
    };
    Ok(count)
}

/// Rebuilds the portable logical-replay table identity map from the current
/// local schema. This is used after page-based apply paths, where no logical
/// schema identity operations are decoded but future logical pulls may depend
/// on an existing map.
pub async fn read_logical_replay_table_map<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
) -> Result<BTreeMap<u64, String>> {
    let mut stmt = conn.prepare(
        "SELECT rootpage, name FROM sqlite_schema WHERE type = 'table' AND rootpage != 0",
    )?;
    let mut map = BTreeMap::new();
    while let Some(row) = run_stmt_once(coro, &mut stmt).await? {
        let rootpage = row.get_value(0).as_int().ok_or_else(|| {
            Error::DatabaseSyncEngineError(format!(
                "unexpected sqlite_schema.rootpage type while rebuilding logical table map: {:?}",
                row.get_value(0)
            ))
        })?;
        let name = match row.get_value(1) {
            Value::Text(text) => text.as_str().to_string(),
            other => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "unexpected sqlite_schema.name type while rebuilding logical table map: {other:?}"
                )));
            }
        };
        if rootpage != 0 && is_logically_replayable_table(&name) {
            map.insert(rootpage.unsigned_abs(), name);
        }
    }
    Ok(map)
}

pub async fn max_local_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
) -> Result<Option<i64>> {
    let mut stmt = match conn.prepare("SELECT MAX(change_id) FROM turso_cdc") {
        Ok(stmt) => stmt,
        Err(LimboError::ParseError(err)) if err.contains("no such table") => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    let value = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0].clone(),
        None => return Ok(None),
    };
    match value {
        Value::Null => Ok(None),
        Value::Numeric(turso_core::Numeric::Integer(value)) => Ok(Some(value)),
        other => Err(Error::DatabaseSyncEngineError(format!(
            "unexpected MAX(change_id) column type: {other:?}"
        ))),
    }
}

/// Read the CDC change-id sequence watermark: the first change id that is NOT
/// yet safe for the push loop to consume.
pub async fn read_cdc_sequence_watermark<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    cdc_table: &str,
) -> Result<Option<i64>> {
    let seq_name = turso_core::schema::autoincrement_sequence_name(cdc_table);
    let mut stmt = conn.prepare("SELECT sequence_watermark_experimental(?)")?;
    stmt.bind_at(
        1.try_into().unwrap(),
        turso_core::Value::build_text(seq_name),
    )?;

    let value = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0].clone(),
        None => return Ok(None),
    };
    match value {
        Value::Null => Ok(None),
        Value::Numeric(turso_core::Numeric::Integer(value)) => Ok(Some(value)),
        other => Err(Error::DatabaseSyncEngineError(format!(
            "unexpected sequence_watermark_experimental column type: {other:?}"
        ))),
    }
}

pub async fn update_last_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
    pull_gen: i64,
    change_id: i64,
) -> Result<()> {
    tracing::info!(
        "update_last_change_id(client_id={client_id}): pull_gen={pull_gen}, change_id={change_id}"
    );
    if !has_table(coro, conn, TURSO_SYNC_TABLE_NAME).await? {
        ensure_sync_last_change_id_table(coro, conn, client_id).await?;
        tracing::info!("update_last_change_id(client_id={client_id}): initialized table");
    }
    let mut select_stmt = conn.prepare(TURSO_SYNC_SELECT_LAST_CHANGE_ID)?;
    select_stmt.bind_at(
        1.try_into().unwrap(),
        turso_core::Value::Text(turso_core::types::Text::new(client_id.to_string())),
    )?;
    let row = run_stmt_expect_one_row(coro, &mut select_stmt).await?;
    tracing::info!("update_last_change_id(client_id={client_id}): selected client row if any");

    if row.is_some() {
        let mut update_stmt = conn.prepare(TURSO_SYNC_UPDATE_LAST_CHANGE_ID)?;
        update_stmt.bind_at(1.try_into().unwrap(), turso_core::Value::from_i64(pull_gen))?;
        update_stmt.bind_at(
            2.try_into().unwrap(),
            turso_core::Value::from_i64(change_id),
        )?;
        update_stmt.bind_at(
            3.try_into().unwrap(),
            turso_core::Value::Text(turso_core::types::Text::new(client_id.to_string())),
        )?;
        run_stmt_ignore_rows(coro, &mut update_stmt).await?;
        tracing::info!("update_last_change_id(client_id={client_id}): updated row for the client");
    } else {
        let mut update_stmt = conn.prepare(TURSO_SYNC_INSERT_LAST_CHANGE_ID)?;
        update_stmt.bind_at(
            1.try_into().unwrap(),
            turso_core::Value::Text(turso_core::types::Text::new(client_id.to_string())),
        )?;
        update_stmt.bind_at(2.try_into().unwrap(), turso_core::Value::from_i64(pull_gen))?;
        update_stmt.bind_at(
            3.try_into().unwrap(),
            turso_core::Value::from_i64(change_id),
        )?;
        run_stmt_ignore_rows(coro, &mut update_stmt).await?;
        tracing::info!(
            "update_last_change_id(client_id={client_id}): inserted new row for the client"
        );
    }

    Ok(())
}

/// Ensures the local high-water mark table exists and publishes schema changes.
pub async fn ensure_sync_last_change_id_table<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<()> {
    for attempt in 0..=2 {
        match conn.execute(TURSO_SYNC_CREATE_TABLE) {
            Ok(()) => {
                conn.publish_schema_if_newer();
                return Ok(());
            }
            Err(LimboError::ParseError(err)) if err.contains("already exists") => {
                tracing::debug!(
                    "update_last_change_id(client_id={client_id}): sync table already exists while initializing; refreshing schema"
                );
                conn.publish_schema_if_newer();
                return Ok(());
            }
            Err(LimboError::SchemaUpdated) => {
                tracing::debug!(
                    "update_last_change_id(client_id={client_id}): schema updated while initializing sync table; refreshing schema"
                );
                force_reparse_schema_with_retry(conn)?;
                publish_schema_after_external_restore(
                    conn,
                    "sync high-water mark table initialization",
                )?;
                if attempt == 2 {
                    return Ok(());
                }
                match has_table(coro, conn, TURSO_SYNC_TABLE_NAME).await {
                    Ok(true) => return Ok(()),
                    Ok(false) => continue,
                    Err(Error::TursoError(LimboError::SchemaUpdated)) => continue,
                    Err(error) => return Err(error),
                }
            }
            Err(err) => return Err(err.into()),
        }
    }
    Err(Error::DatabaseSyncEngineError(format!(
        "failed to initialize sync high-water mark table after schema refresh: client_id={client_id}"
    )))
}

fn force_reparse_schema_with_retry(conn: &Arc<turso_core::Connection>) -> Result<()> {
    for attempt in 0..=2 {
        match conn.force_reparse_schema() {
            Ok(()) => return Ok(()),
            Err(LimboError::SchemaUpdated) if attempt < 2 => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn publish_schema_after_external_restore(
    conn: &Arc<turso_core::Connection>,
    context: &str,
) -> Result<()> {
    conn.publish_schema_after_external_restore()
        .map_err(|error| {
            Error::DatabaseSyncEngineError(format!(
                "failed to publish schema after {context}: {error}",
            ))
        })
}

pub async fn read_last_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<(i64, Option<i64>)> {
    tracing::info!("read_last_change_id: client_id={client_id}");

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let mut select_last_change_id_stmt = match conn.prepare(TURSO_SYNC_SELECT_LAST_CHANGE_ID) {
        Ok(stmt) => stmt,
        Err(LimboError::ParseError(..)) => return Ok((0, None)),
        Err(err) => return Err(err.into()),
    };

    select_last_change_id_stmt.bind_at(
        1.try_into().unwrap(),
        Value::Text(Text::new(client_id.to_string())),
    )?;

    match run_stmt_expect_one_row(coro, &mut select_last_change_id_stmt).await? {
        Some(row) => {
            let pull_gen = row[0].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected source pull_gen type".to_string())
            })?;
            let change_id = row[1].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected source change_id type".to_string())
            })?;
            Ok((pull_gen, Some(change_id)))
        }
        None => {
            tracing::info!("read_last_change_id: client_id={client_id}, turso_sync_last_change_id client id is not found");
            Ok((0, None))
        }
    }
}

pub async fn fetch_last_change_id<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    source_conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<(i64, Option<i64>)> {
    tracing::info!("fetch_last_change_id: client_id={client_id}");

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let (source_pull_gen, _) = read_last_change_id(ctx.coro, source_conn, client_id).await?;
    tracing::info!(
        "fetch_last_change_id: client_id={client_id}, source_pull_gen={source_pull_gen}"
    );

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let init_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: vec![
            // read pull_gen, change_id values for current client if they were set before
            StreamRequest::Batch(BatchStreamReq {
                batch: Batch {
                    steps: vec![BatchStep {
                        stmt: Stmt {
                            sql: Some(TURSO_SYNC_SELECT_LAST_CHANGE_ID.to_string()),
                            sql_id: None,
                            args: vec![server_proto::Value::Text {
                                value: client_id.to_string(),
                            }],
                            named_args: Vec::new(),
                            want_rows: Some(true),
                            replication_index: None,
                        },
                        condition: None,
                    }]
                    .into(),
                    replication_index: None,
                },
            }),
        ]
        .into(),
    };

    let no_ignored_steps = std::collections::HashSet::new();
    let response = match sql_execute_http(ctx, init_hrana_request, &no_ignored_steps).await {
        Ok(response) => response,
        Err(Error::DatabaseSyncEngineError(err)) if err.contains("no such table") => {
            return Ok((source_pull_gen, None));
        }
        Err(err) => return Err(err),
    };
    assert!(response.len() == 1);
    let last_change_id_response = &response[0];
    tracing::debug!("fetch_last_change_id: response={:?}", response);
    assert!(last_change_id_response.rows.len() <= 1);
    if last_change_id_response.rows.is_empty() {
        return Ok((source_pull_gen, None));
    }
    let row = &last_change_id_response.rows[0].values;
    let server_proto::Value::Integer {
        value: target_pull_gen,
    } = row[0]
    else {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected target pull_gen type".to_string(),
        ));
    };
    let server_proto::Value::Integer {
        value: target_change_id,
    } = row[1]
    else {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected target change_id type".to_string(),
        ));
    };
    tracing::debug!(
        "fetch_last_change_id: client_id={client_id}, target_pull_gen={target_pull_gen}, target_change_id={target_change_id}"
    );
    if target_pull_gen > source_pull_gen {
        return Err(Error::DatabaseSyncEngineError(format!("protocol error: target_pull_gen > source_pull_gen: {target_pull_gen} > {source_pull_gen}")));
    }
    let last_change_id = if target_pull_gen == source_pull_gen {
        Some(target_change_id)
    } else {
        Some(0)
    };
    Ok((source_pull_gen, last_change_id))
}

pub async fn push_logical_changes<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    source: &DatabaseTape,
    client_id: &str,
    opts: &DatabaseSyncEngineOpts,
) -> Result<(i64, i64, i64)> {
    tracing::info!("push_logical_changes: client_id={client_id}");
    let source_conn = connect_untracked(source)?;

    let (source_pull_gen, mut last_change_id) =
        fetch_last_change_id(ctx, &source_conn, client_id).await?;
    let replay_floor_change_id = last_change_id.unwrap_or(0);

    tracing::debug!("push_logical_changes: last_change_id={:?}", last_change_id);

    // In MVCC mode, bound the scan by the CDC sequence watermark so snapshot
    // isolation cannot make us skip a change id that a concurrent transaction
    // commits below the current max after we advance the push watermark. WAL mode
    // keeps its unbounded scan (change ids there come from NewRowid, and the WAL
    // push loop does not rely on the sequence watermark).
    let max_change_id_exclusive = if source_conn.mvcc_enabled() {
        let watermark =
            read_cdc_sequence_watermark(ctx.coro, &source_conn, source.cdc_table()).await?;
        tracing::debug!("push_logical_changes: cdc sequence watermark={watermark:?}");
        watermark
    } else {
        None
    };

    let replay_opts = DatabaseReplaySessionOpts {
        use_implicit_rowid: false,
    };

    let generator = DatabaseReplayGenerator::new(source_conn, replay_opts);

    let iterate_opts = DatabaseChangesIteratorOpts {
        first_change_id: last_change_id.map(|x| x + 1),
        mode: DatabaseChangesIteratorMode::Apply,
        ignore_schema_changes: false,
        max_change_id_exclusive,
        ..Default::default()
    };

    let threshold = opts.push_operations_threshold.filter(|t| *t > 0);
    let mut changes = source.iterate_changes(iterate_opts)?;
    let mut batch: Vec<DatabaseTapeRowChange> = Vec::new();
    let mut total_rows_changed: i64 = 0;

    let mut next_operation = changes.next(ctx.coro).await?;
    while let Some(operation) = next_operation.take() {
        next_operation = changes.next(ctx.coro).await?;

        if next_operation.is_none() {
            assert!(
                matches!(operation, DatabaseTapeOperation::Commit),
                "last operation in the changes stream must be COMMIT"
            );
        }

        match operation {
            DatabaseTapeOperation::StmtReplay(_) | DatabaseTapeOperation::SchemaReplay(_) => {
                panic!("changes iterator must not use replay-only operations")
            }
            DatabaseTapeOperation::RowChange(change) => {
                if !should_push_change(&change, opts)? {
                    continue;
                }
                batch.push(change);
            }
            DatabaseTapeOperation::Commit => {
                // push batch if we reach threshold OR if this is last operation
                let must_push =
                    threshold.is_some_and(|t| batch.len() >= t) || next_operation.is_none();
                if !must_push {
                    continue;
                }
                let (rows_changed, next_change_id) = send_push_batch(
                    ctx,
                    &generator,
                    opts,
                    &batch,
                    client_id,
                    source_pull_gen,
                    last_change_id,
                )
                .await?;
                total_rows_changed += rows_changed;
                last_change_id = Some(next_change_id);
                batch.clear();
            }
        }
    }

    assert!(
        batch.is_empty(),
        "batch must be empty in the end so all operations are send to remote"
    );

    tracing::info!(
        "push_logical_changes: rows_changed={total_rows_changed}, last_change_id={:?}",
        last_change_id
    );
    Ok((
        source_pull_gen,
        replay_floor_change_id,
        last_change_id.unwrap_or(0),
    ))
}

/// Build and send a single push batch over HTTP. The caller owns the
/// per-batch slice of changes; transformations (if enabled) run lazily here so
/// the user-defined transform sees one batch's worth of rows at a time —
/// matching the streaming semantics of the outer loop.
///
/// Returns the count of rows that actually produced SQL steps
/// (post-transformation, excluding `Skip`).
async fn send_push_batch<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    generator: &DatabaseReplayGenerator,
    opts: &DatabaseSyncEngineOpts,
    batch_changes: &[DatabaseTapeRowChange],
    client_id: &str,
    source_pull_gen: i64,
    mut last_change_id: Option<i64>,
) -> Result<(i64, i64)> {
    let mut transformed = if opts.use_transform {
        Some(apply_transformation(ctx, batch_changes, generator).await?)
    } else {
        None
    };

    let step = |query, args| BatchStep {
        stmt: Stmt {
            sql: Some(query),
            sql_id: None,
            args,
            named_args: Vec::new(),
            want_rows: Some(false),
            replication_index: None,
        },
        condition: Some(BatchCond::Not {
            cond: Box::new(BatchCond::IsAutocommit {}),
        }),
    };

    let mut add_column_step_indices = std::collections::HashSet::new();
    let mut sql_over_http_requests = vec![
        BatchStep {
            stmt: Stmt {
                sql: Some("BEGIN IMMEDIATE".to_string()),
                sql_id: None,
                args: Vec::new(),
                named_args: Vec::new(),
                want_rows: Some(false),
                replication_index: None,
            },
            condition: None,
        },
        step(TURSO_SYNC_CREATE_TABLE.to_string(), Vec::new()),
    ];

    let mut rows_changed: i64 = 0;
    for (i, change) in batch_changes.iter().enumerate() {
        let change_id = change.change_id;
        let transform_result = if let Some(transformed) = transformed.as_mut() {
            std::mem::replace(&mut transformed[i], DatabaseRowTransformResult::Skip)
        } else {
            DatabaseRowTransformResult::Keep
        };
        if let DatabaseRowTransformResult::Skip = transform_result {
            continue;
        }
        tracing::debug!(
            "change_id: {}, last_change_id: {:?}",
            change_id,
            last_change_id
        );
        assert!(
            last_change_id.is_none() || last_change_id.unwrap() < change_id,
            "change id must be strictly increasing: last_change_id={last_change_id:?}, change.change_id={change_id}"
        );
        rows_changed += 1;
        // we give user full control over CDC table - so let's not emit assert here for now
        if last_change_id.is_some() && last_change_id.unwrap() + 1 != change_id {
            tracing::debug!(
                "out of order change sequence: {} -> {}",
                last_change_id.unwrap(),
                change_id
            );
        }
        last_change_id = Some(change_id);
        match transform_result {
            DatabaseRowTransformResult::Skip => panic!("Skip must be handled earlier"),
            DatabaseRowTransformResult::Rewrite(replay) => {
                sql_over_http_requests.push(step(replay.sql, convert_to_args(replay.values)))
            }
            DatabaseRowTransformResult::Keep => {
                let mut replay_info = generator.replay_info(ctx.coro, change).await?;
                // for now we try to support DDL statements which "extends" the schema (CREATE INDEX, CREATE TABLE, ALTER TABLE ADD COLUMN) and they have `IF NOT EXISTS` semantic
                // as ALTER TABLE has no such syntax - we ignore error for such statements from remote for now
                let is_alter_add_column =
                    replay_info.is_ddl_replay && is_alter_table_add_column(&replay_info.query);
                if replay_info.is_ddl_replay {
                    // the remote may already have this schema object because another
                    // client pushed its own version of the DDL first - replay CREATE
                    // statements with IF NOT EXISTS so the push stays idempotent.
                    //
                    // note that execute_ddl_idempotent() cannot be used here: it
                    // decides what to execute by introspecting the schema through a
                    // live local connection (table_columns_info(),
                    // schema_object_exists()), while this path only assembles a
                    // static batch of SQL that the remote executes in a single
                    // /v2/pipeline round trip - there is no way to inspect the
                    // remote schema before choosing what to send, so rewriting the
                    // SQL text (or ignoring specific step errors, as done for ALTER
                    // TABLE ADD COLUMN above) is the only available lever
                    if let Some(rewritten) = rewrite_create_ddl_as_if_not_exists(&replay_info.query)
                    {
                        replay_info.query = rewritten;
                    }
                }
                match &change.change {
                    DatabaseTapeRowChangeType::Delete { before, key } => {
                        let values = generator.replay_delete_values(
                            &replay_info,
                            change.id,
                            before.clone(),
                            key.clone(),
                        )?;
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)))
                    }
                    DatabaseTapeRowChangeType::Insert { after } => {
                        if generator.upsert_needs_null_safe_predelete(&replay_info, after) {
                            // ON CONFLICT cannot resolve a key with a NULL
                            // component, so remove the remote row by its
                            // NULL-safe identity before inserting the new image.
                            let delete_info = generator
                                .delete_query(ctx.coro, &change.table_name, false)
                                .await?;
                            let delete_values = generator.replay_delete_values(
                                &delete_info,
                                change.id,
                                after.clone(),
                                None,
                            )?;
                            sql_over_http_requests.push(step(
                                delete_info.query.clone(),
                                convert_to_args(delete_values),
                            ));
                        }
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after.clone(),
                            None,
                            None,
                        );
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                    DatabaseTapeRowChangeType::Update {
                        before,
                        after,
                        updates: Some(updates),
                    } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after.clone(),
                            Some(updates.clone()),
                            Some(before.clone()),
                        );
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                    DatabaseTapeRowChangeType::Update {
                        after,
                        updates: None,
                        ..
                    } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after.clone(),
                            None,
                            None,
                        );
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                }
                if is_alter_add_column {
                    add_column_step_indices.insert(sql_over_http_requests.len() - 1);
                }
            }
        }
    }

    if rows_changed > 0 {
        tracing::info!("prepare update stmt for turso_sync_last_change_id table with client_id={} and last_change_id={:?}", client_id, last_change_id);
        // update turso_sync_last_change_id table with new value before commit
        let next_change_id = last_change_id.unwrap_or(0);
        tracing::info!("push_logical_changes: client_id={client_id}, set pull_gen={source_pull_gen}, change_id={next_change_id}, rows_changed={rows_changed}");
        sql_over_http_requests.push(step(
            TURSO_SYNC_UPSERT_LAST_CHANGE_ID.to_string(),
            vec![
                server_proto::Value::Text {
                    value: client_id.to_string(),
                },
                server_proto::Value::Integer {
                    value: source_pull_gen,
                },
                server_proto::Value::Integer {
                    value: next_change_id,
                },
            ],
        ));
    }
    sql_over_http_requests.push(step("COMMIT".to_string(), Vec::new()));

    tracing::debug!("hrana request: {:?}", sql_over_http_requests);
    let replay_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: vec![StreamRequest::Batch(BatchStreamReq {
            batch: Batch {
                steps: sql_over_http_requests.into(),
                replication_index: None,
            },
        })]
        .into(),
    };

    let _ = sql_execute_http(ctx, replay_hrana_request, &add_column_step_indices).await?;
    Ok((rows_changed, last_change_id.unwrap_or(0)))
}

pub async fn apply_transformation<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    changes: &[DatabaseTapeRowChange],
    generator: &DatabaseReplayGenerator,
) -> Result<Vec<DatabaseRowTransformResult>> {
    let mut transformed = Vec::new();
    let mut mutations = Vec::new();
    // break changes at DDL boundaries and apply transformation callback only of DML operations
    // e.g. for sequence like that:
    // 1. INSERT INTO t1
    // 2. INSERT INTO t2
    // 3. CREATE TABLE t3
    // 4. INSERT INTO t3
    // 5. CREATE TABLE t4
    // 6. CREATE TABLE t5
    // We will invoke transform callback for operations [1, 2] and then for operation [4]

    let flush_mutations = async |mutations: &mut Vec<DatabaseRowMutation>,
                                 transformed: &mut Vec<DatabaseRowTransformResult>|
           -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let mutations_cnt = mutations.len();
        let completion = ctx.io.transform(std::mem::take(mutations))?;
        let transformed_part = wait_all_results(ctx.coro, &completion, None).await?;
        if transformed_part.len() != mutations_cnt {
            return Err(Error::DatabaseSyncEngineError(format!(
                "unexpected result from custom transformation: mismatch in shapes: {} != {}",
                transformed_part.len(),
                mutations_cnt,
            )));
        }
        tracing::info!("apply_transformation: got {:?}", transformed_part);
        transformed.extend(transformed_part);
        Ok(())
    };

    for change in changes {
        let replay_info = generator.replay_info(ctx.coro, change).await?;
        if !replay_info.is_ddl_replay {
            mutations.push(generator.create_mutation(&replay_info, change)?);
        } else {
            flush_mutations(&mut mutations, &mut transformed).await?;
            transformed.push(DatabaseRowTransformResult::Keep);
        }
    }
    flush_mutations(&mut mutations, &mut transformed).await?;

    if transformed.len() != changes.len() {
        return Err(Error::DatabaseSyncEngineError(format!(
            "unexpected result from apply_transformation: mismatch in shapes: {} != {}",
            transformed.len(),
            changes.len(),
        )));
    }
    tracing::info!("apply_transformation: got {:?}", transformed);
    Ok(transformed)
}

pub async fn read_wal_salt<Ctx>(
    coro: &Coro<Ctx>,
    wal: &Arc<dyn turso_core::File>,
) -> Result<Option<Vec<u32>>> {
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_HEADER));
    let c = Completion::new_read(buffer.clone(), |result| {
        let Ok((buffer, len)) = result else {
            return None;
        };
        if (len as usize) < WAL_HEADER {
            buffer.as_mut_slice().fill(0);
        }
        None
    });
    let c = wal.pread(0, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    if buffer.as_mut_slice() == [0u8; WAL_HEADER] {
        return Ok(None);
    }
    let salt1 = u32::from_be_bytes(buffer.as_slice()[16..20].try_into().unwrap());
    let salt2 = u32::from_be_bytes(buffer.as_slice()[20..24].try_into().unwrap());
    Ok(Some(vec![salt1, salt2]))
}

pub async fn checkpoint_wal_file<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
) -> Result<()> {
    let mut checkpoint_stmt = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)")?;
    loop {
        match checkpoint_stmt.step()? {
            turso_core::StepResult::IO | turso_core::StepResult::Yield => {
                // todo(sivukhin): introduce Yield result in the sync engine
                coro.yield_(SyncEngineIoResult::IO).await?
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::Row => continue,
            r => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "unexepcted checkpoint result: {r:?}"
                )))
            }
        }
    }
    Ok(())
}

pub async fn bootstrap_db_file<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
    protocol: DatabaseSyncEngineProtocolVersion,
    partial_sync: Option<PartialSyncOpts>,
    pull_bytes_threshold: Option<usize>,
) -> Result<(DatabasePullRevision, RemotePullProtocol)> {
    match protocol {
        DatabaseSyncEngineProtocolVersion::Legacy => {
            if partial_sync.is_some() {
                return Err(Error::DatabaseSyncEngineError(
                    "can't bootstrap prefix of database with legacy protocol".to_string(),
                ));
            }
            // The legacy wire protocol has no MVCC variant; it is pages by definition.
            let revision = bootstrap_db_file_legacy(ctx, io, main_db_path).await?;
            Ok((revision, RemotePullProtocol::Pages))
        }
        DatabaseSyncEngineProtocolVersion::V1 => {
            bootstrap_db_file_v1(ctx, io, main_db_path, partial_sync, pull_bytes_threshold).await
        }
    }
}

/// Serialise a contiguous `[start, end)` page-id range into a RoaringBitmap
/// blob suitable for `PullUpdatesReqProtoBody::server_pages_selector`.
fn page_range_bitmap(start_inclusive: u32, end_exclusive: u32) -> Result<Vec<u8>> {
    let mut bitmap = RoaringBitmap::new();
    if start_inclusive < end_exclusive {
        bitmap.insert_range(start_inclusive..end_exclusive);
    }
    let mut bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap.serialize_into(&mut bytes).map_err(|e| {
        Error::DatabaseSyncEngineError(format!("unable to serialize bootstrap request: {e}"))
    })?;
    Ok(bytes)
}

/// Send a single `/pull-updates` request and stream every returned page into
/// `file`. Returns the response header for the caller to inspect (db_size,
/// server_revision). When `truncate_on_first_response` is set, the file is
/// resized to `header.db_size * PAGE_SIZE` between reading the header and the
/// first page — used by the first chunk of a chunked bootstrap.
#[allow(clippy::too_many_arguments)]
async fn pull_chunk_into_file<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    file: &Arc<dyn turso_core::File>,
    server_revision: &str,
    server_pages_selector: Vec<u8>,
    server_query_selector: String,
    truncate_on_first_response: bool,
) -> Result<PullUpdatesRespProtoBody> {
    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        server_revision: server_revision.to_string(),
        client_revision: String::new(),
        long_poll_timeout_ms: 0,
        server_pages_selector: server_pages_selector.into(),
        server_query_selector,
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let mut bytes = BytesMut::new();
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!("bootstrap_db_file: got header={:?}", header);
    ensure_page_stream(&header, "pull_chunk_into_file")?;
    if truncate_on_first_response {
        let c = Completion::new_trunc(move |result| {
            let Ok(rc) = result else {
                return;
            };
            assert!(rc as usize == 0);
        });
        let c = file.truncate(header.db_size * PAGE_SIZE as u64, c)?;
        while !c.succeeded() {
            ctx.coro.yield_(SyncEngineIoResult::IO).await?;
        }
    }
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(PAGE_SIZE));
    while let Some(page_data) = wait_proto_message::<Ctx, PageData>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    {
        tracing::debug!(
            "bootstrap_db_file: received page page_id={}",
            page_data.page_id
        );
        let offset = page_data.page_id * PAGE_SIZE as u64;
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        buffer.as_mut_slice().copy_from_slice(&page);
        let c = Completion::new_write(move |result| {
            // todo(sivukhin): we need to error out in case of partial read
            let Ok(size) = result else {
                return;
            };
            assert!(size as usize == PAGE_SIZE);
        });
        let c = file.pwrite(offset, buffer.clone(), c)?;
        while !c.succeeded() {
            ctx.coro.yield_(SyncEngineIoResult::IO).await?;
        }
    }
    Ok(header)
}

pub async fn bootstrap_db_file_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
    partial_sync: Option<PartialSyncOpts>,
    pull_bytes_threshold: Option<usize>,
) -> Result<(DatabasePullRevision, RemotePullProtocol)> {
    if let Some(PartialSyncOpts {
        bootstrap_strategy: None,
        ..
    }) = partial_sync
    {
        return Err(Error::DatabaseSyncEngineError(
            "partial sync bootstrap strategy must be set for initialization".to_string(),
        ));
    }
    // Predetermined last page id from the partial-sync prefix strategy (if any).
    let prefix_bootstrap_last_page_id: Option<u32> = if let Some(PartialSyncOpts {
        bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix { length }),
        ..
    }) = &partial_sync
    {
        Some((*length / PAGE_SIZE) as u32)
    } else {
        None
    };
    // Server-side query selector (can't be chunked locally — server picks pages).
    let server_query_selector: String = if let Some(PartialSyncOpts {
        bootstrap_strategy: Some(PartialBootstrapStrategy::Query { query }),
        ..
    }) = &partial_sync
    {
        query.clone()
    } else {
        String::new()
    };
    let has_query = !server_query_selector.is_empty();

    // Convert the byte threshold into a page-count chunk size. Chunking only
    // applies when the page set is statically known on the client (i.e. no
    // query selector).
    let chunk_pages: Option<u32> = if has_query {
        None
    } else {
        pull_bytes_threshold
            .filter(|t| *t > 0)
            .map(|t| (t.div_ceil(PAGE_SIZE)).max(1) as u32)
    };

    let file = io.open_file(main_db_path, OpenFlags::Create, false)?;

    // First request: covers either [0..min(N, prefix)) when chunking, [0..L)
    // for a non-chunked prefix bootstrap, or an empty bitmap (server returns
    // the full DB) for the bare full bootstrap.
    let first_selector = if let Some(n) = chunk_pages {
        let upper = prefix_bootstrap_last_page_id.unwrap_or(u32::MAX);
        page_range_bitmap(0, std::cmp::min(n, upper))?
    } else if let Some(l) = prefix_bootstrap_last_page_id {
        page_range_bitmap(0, l)?
    } else {
        Vec::new()
    };
    let header =
        pull_chunk_into_file(ctx, &file, "", first_selector, server_query_selector, true).await?;

    // Subsequent chunks (if any). Pin every chunk to the same `server_revision`
    // so the page set stays consistent across HTTP round-trips, and never go
    // past the predetermined upper bound (prefix length or db_size).
    if let Some(n) = chunk_pages {
        let last_page_id: u64 = match prefix_bootstrap_last_page_id {
            Some(l) => std::cmp::min(l as u64, header.db_size),
            None => header.db_size,
        };
        let mut start = n as u64;
        while start < last_page_id {
            let end = std::cmp::min(start + n as u64, last_page_id);
            let selector = page_range_bitmap(start as u32, end as u32)?;
            pull_chunk_into_file(
                ctx,
                &file,
                &header.server_revision,
                selector,
                String::new(),
                false,
            )
            .await?;
            start = end;
        }
    }

    // Persist the bootstrapped DB file before the caller records this revision
    // as durable in the metadata; otherwise a crash could leave the DB file
    // with unflushed pages while the metadata claims a completed bootstrap.
    sync_file(ctx.coro, &file).await?;

    let remote_protocol = detect_remote_pull_protocol(&header);
    Ok((
        DatabasePullRevision::V1 {
            revision: header.server_revision,
        },
        remote_protocol,
    ))
}

fn decode_page(header: &PullUpdatesRespProtoBody, page_data: PageData) -> Result<Vec<u8>> {
    if header.raw_encoding.is_some() && header.zstd_encoding.is_some() {
        return Err(Error::DatabaseSyncEngineError(
            "both of raw_encoding and zstd_encoding are set".to_string(),
        ));
    }
    if header.raw_encoding.is_none() && header.zstd_encoding.is_none() {
        return Err(Error::DatabaseSyncEngineError(
            "none from raw_encoding and zstd_encoding are set".to_string(),
        ));
    }

    if header.raw_encoding.is_some() {
        return Ok(page_data.encoded_page.to_vec());
    }
    Err(Error::DatabaseSyncEngineError(
        "zstd encoding is not supported".to_string(),
    ))
}

pub async fn bootstrap_db_file_legacy<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
) -> Result<DatabasePullRevision> {
    tracing::info!("bootstrap_db_file(path={})", main_db_path);

    let start_time = std::time::Instant::now();
    // cleanup all files left from previous attempt to bootstrap
    // we shouldn't write any WAL files - but let's truncate them too for safety
    if let Some(file) = io.try_open(main_db_path)? {
        io.truncate(ctx.coro, file, 0).await?;
    }
    if let Some(file) = io.try_open(&format!("{main_db_path}-wal"))? {
        io.truncate(ctx.coro, file.clone(), 0).await?;
        // Make the WAL reset durable so a stale WAL can't reappear next to the
        // freshly bootstrapped DB after a crash and get replayed onto it.
        sync_file(ctx.coro, &file).await?;
    }

    let file = io.create(main_db_path)?;
    let db_info = db_bootstrap(ctx, file).await?;

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::info!(
        "bootstrap_db_files(path={}): finished: elapsed={:?}",
        main_db_path,
        elapsed
    );

    Ok(DatabasePullRevision::Legacy {
        generation: db_info.current_generation,
        synced_frame_no: None,
    })
}

/// fsync the given file, yielding on the coro until the sync completion finishes.
pub async fn sync_file<Ctx>(coro: &Coro<Ctx>, file: &Arc<dyn turso_core::File>) -> Result<()> {
    let c = Completion::new_sync(|_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = file.sync(c, FileSyncType::Fsync)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

pub async fn reset_wal_file<Ctx>(
    coro: &Coro<Ctx>,
    wal: Arc<dyn turso_core::File>,
    frames_count: u64,
) -> Result<()> {
    let wal_size = if frames_count == 0 {
        // let's truncate WAL file completely in order for this operation to safely execute on empty WAL in case of initial bootstrap phase
        0
    } else {
        WAL_HEADER as u64 + WAL_FRAME_SIZE as u64 * frames_count
    };
    tracing::debug!("reset db wal to the size of {} frames", frames_count);
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = wal.truncate(wal_size, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    // Persist the truncation: without fsync a crash could leave stale revert
    // frames on disk while the metadata already records the new revert
    // watermark, which would corrupt the next rollback.
    sync_file(coro, &wal).await?;
    Ok(())
}

async fn sql_execute_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    request: server_proto::PipelineReqBody,
    ignored_step_indices: &std::collections::HashSet<usize>,
) -> Result<Vec<StmtResult>> {
    let body = serde_json::to_vec(&request)?;

    ctx.io.network_stats.write(body.len());
    let completion = ctx.http(
        "POST",
        "/v2/pipeline",
        Some(body),
        &[("content-type", "application/json")],
    )?;

    wait_ok_status(ctx.coro, &completion, "sql_execute_http").await?;

    let response = wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
    let response: server_proto::PipelineRespBody = serde_json::from_slice(&response)?;
    tracing::debug!("hrana response: {:?}", response);
    let mut results = Vec::new();
    for result in response.results {
        match result {
            server_proto::StreamResult::Error { error } => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "failed to execute sql: {error:?}"
                )))
            }
            server_proto::StreamResult::None => {
                return Err(Error::DatabaseSyncEngineError(
                    "unexpected None result".to_string(),
                ));
            }
            server_proto::StreamResult::Ok { response } => match response {
                server_proto::StreamResponse::Execute(execute) => {
                    results.push(execute.result);
                }
                server_proto::StreamResponse::Batch(batch) => {
                    for (i, error) in batch.result.step_errors.iter().enumerate() {
                        if let Some(error) = error {
                            if ignored_step_indices.contains(&i) {
                                tracing::info!("ignoring step error at index {i}: {error:?}");
                            } else {
                                return Err(Error::DatabaseSyncEngineError(format!(
                                    "failed to execute sql: {error:?}"
                                )));
                            }
                        }
                    }
                    for result in batch.result.step_results.into_iter().flatten() {
                        results.push(result);
                    }
                }
            },
        }
    }
    Ok(results)
}

fn is_alter_table_add_column(sql: &str) -> bool {
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let Some(Ok(ast)) = parser.next() else {
        return false;
    };
    matches!(
        ast,
        turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::AlterTable(
            turso_parser::ast::AlterTable {
                body: turso_parser::ast::AlterTableBody::AddColumn(_),
                ..
            }
        ))
    )
}

/// Rewrite a CREATE statement with `IF NOT EXISTS` so it can be replayed on a
/// remote which may already have the object (e.g. another client pushed its
/// own version of the DDL first). Returns None for non-CREATE statements.
fn rewrite_create_ddl_as_if_not_exists(sql: &str) -> Option<String> {
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let Some(Ok(mut ast)) = parser.next() else {
        return None;
    };
    let turso_parser::ast::Cmd::Stmt(stmt) = &mut ast else {
        return None;
    };
    match stmt {
        turso_parser::ast::Stmt::CreateTable { if_not_exists, .. }
        | turso_parser::ast::Stmt::CreateIndex { if_not_exists, .. }
        | turso_parser::ast::Stmt::CreateTrigger { if_not_exists, .. }
        | turso_parser::ast::Stmt::CreateMaterializedView { if_not_exists, .. }
        | turso_parser::ast::Stmt::CreateView { if_not_exists, .. } => {
            *if_not_exists = true;
            Some(ast.to_string())
        }
        _ => None,
    }
}

async fn wal_pull_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
) -> Result<WalHttpPullResult<IO::DataCompletionBytes>> {
    let completion = ctx.http(
        "GET",
        &format!("/sync/{generation}/{start_frame}/{end_frame}"),
        None,
        &[],
    )?;
    let status = wait_status(ctx.coro, &completion).await?;
    if status == http::StatusCode::BAD_REQUEST {
        let status_body =
            wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
        let status: DbSyncStatus = serde_json::from_slice(&status_body)?;
        if status.status == "checkpoint_needed" {
            return Ok(WalHttpPullResult::NeedCheckpoint(status));
        } else {
            let error = format!("wal_pull: unexpected sync status: {status:?}");
            return Err(Error::DatabaseSyncEngineError(error));
        }
    }
    if status != http::StatusCode::OK {
        let error = format!("wal_pull: unexpected status code: {status}");
        return Err(Error::DatabaseSyncEngineError(error));
    }
    Ok(WalHttpPullResult::Frames(completion))
}

async fn wal_push_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    baton: Option<String>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
    frames: Vec<u8>,
) -> Result<DbSyncStatus> {
    let baton = baton
        .map(|baton| format!("/{baton}"))
        .unwrap_or("".to_string());

    ctx.io.network_stats.write(frames.len());
    let completion = ctx.http(
        "POST",
        &format!("/sync/{generation}/{start_frame}/{end_frame}{baton}"),
        Some(frames),
        &[],
    )?;
    wait_ok_status(ctx.coro, &completion, "wal_push").await?;
    let status_body = wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_info_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
) -> Result<DbSyncInfo> {
    let completion = ctx.http("GET", "/info", None, &[])?;
    wait_ok_status(ctx.coro, &completion, "db_info").await?;
    let status_body = wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_bootstrap_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    generation: u64,
) -> Result<IO::DataCompletionBytes> {
    let completion = ctx.http("GET", &format!("/export/{generation}"), None, &[])?;
    wait_ok_status(ctx.coro, &completion, "db_bootstrap").await?;
    Ok(completion)
}

pub async fn wait_ok_status<Ctx>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<u8>,
    operation: &'static str,
) -> Result<()> {
    let status = wait_status(coro, completion).await?;
    if status == http::StatusCode::OK {
        return Ok(());
    }
    let body = wait_all_results(coro, completion, None).await?;
    match std::str::from_utf8(body.as_slice()) {
        Ok(body) => Err(Error::DatabaseSyncEngineError(format!(
            "{operation}: unexpected http response: status={status}, body={body}"
        ))),
        Err(_) => Err(Error::DatabaseSyncEngineError(format!(
            "{operation}: unexpected http response: status={status}"
        ))),
    }
}

pub async fn wait_status<Ctx, T>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<T>,
) -> Result<u16> {
    while completion.status()?.is_none() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(completion.status()?.unwrap())
}

#[inline(always)]
pub fn read_varint(buf: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut v: u64 = 0;
    for i in 0..9 {
        match buf.get(i) {
            Some(c) => {
                v |= ((c & 0x7f) as u64) << (i * 7);
                if (c & 0x80) == 0 {
                    return Ok(Some((v as usize, i + 1)));
                }
            }
            None => return Ok(None),
        }
    }
    Err(Error::DatabaseSyncEngineError(format!(
        "invalid variant byte: {:?}",
        &buf[0..=8]
    )))
}

pub async fn wait_proto_message<Ctx, T: prost::Message + Default>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<u8>,
    network_stats: &DataStats,
    bytes: &mut BytesMut,
) -> Result<Option<T>> {
    let start_time = std::time::Instant::now();
    while completion.status()?.is_none() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    let status = completion.status()?.expect("status must be set");
    if status != 200 {
        let body = wait_all_results(coro, completion, Some(network_stats)).await?;
        return match std::str::from_utf8(body.as_slice()) {
            Ok(body) => Err(Error::DatabaseSyncEngineError(format!(
                "remote server returned an error: status={status}, body={body}"
            ))),
            Err(_) => Err(Error::DatabaseSyncEngineError(format!(
                "remote server returned an error: status={status}"
            ))),
        };
    }
    loop {
        let length = read_varint(bytes)?;
        let not_enough_bytes = match length {
            None => true,
            Some((message_length, prefix_length)) => message_length + prefix_length > bytes.len(),
        };
        if not_enough_bytes {
            if let Some(poll) = completion.poll_data()? {
                network_stats.read(poll.data().len());
                bytes.extend_from_slice(poll.data());
            } else if !completion.is_done()? {
                coro.yield_(SyncEngineIoResult::IO).await?;
            } else if bytes.is_empty() {
                return Ok(None);
            } else {
                return Err(Error::DatabaseSyncEngineError(
                    "unexpected end of protobuf message".to_string(),
                ));
            }
            continue;
        }
        let (message_length, prefix_length) = length.unwrap();
        let message = T::decode_length_delimited(&**bytes).map_err(|e| {
            Error::DatabaseSyncEngineError(format!("unable to deserialize protobuf message: {e}"))
        })?;
        let _ = bytes.split_to(message_length + prefix_length);
        tracing::trace!(
            "wait_proto_message: elapsed={:?}",
            std::time::Instant::now().duration_since(start_time)
        );
        return Ok(Some(message));
    }
}

pub async fn wait_all_results<Ctx, T: Clone>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<T>,
    stats: Option<&DataStats>,
) -> Result<Vec<T>> {
    let mut results = Vec::new();
    loop {
        while let Some(poll) = completion.poll_data()? {
            stats.inspect(|s| s.read(poll.data().len()));
            results.extend_from_slice(poll.data());
        }
        if completion.is_done()? {
            break;
        }
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(results)
}

#[cfg(test)]
#[path = "../tests/unit/database_sync_operations/tests.rs"]
mod tests;
