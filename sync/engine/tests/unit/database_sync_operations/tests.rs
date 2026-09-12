use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use turso_core::SqliteDialect;

use bytes::{Bytes, BytesMut};
use prost::Message;
use tempfile::NamedTempFile;

use crate::{
    client_proto::{
        LogicalOp, LogicalOpType, LogicalSchemaAction, LogicalSchemaKind, LogicalTxnData,
    },
    database_sync_engine::DataStats,
    database_sync_engine::DatabaseSyncEngineOpts,
    database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
    database_sync_operations::{
        apply_logical_transactions_file_without_commit_excluding_client_txns_with_table_map_and_stats,
        count_local_changes, detect_remote_pull_protocol, ensure_incremental_page_stream,
        ensure_page_stream, is_logically_replayable_table, logical_txn_to_tape_operations,
        logically_replayable_table_filter, pull_pages_v1, pull_updates_v1, should_push_change,
        should_replay_local_change, update_last_change_id, wait_proto_message, wal_pull_to_file_v1,
        PullUpdatesV1Result, SyncEngineIoStats, SyncOperationCtx,
    },
    database_tape::{
        run_stmt_expect_one_row, run_stmt_once, DatabaseReplaySessionOpts, DatabaseTape,
    },
    server_proto,
    server_proto::{
        PageData, PageSetRawEncodingProto, PullUpdatesApplyMode, PullUpdatesReqProtoBody,
        PullUpdatesRespProtoBody, PullUpdatesStreamKind,
    },
    types::{
        parse_bin_record, Coro, DatabasePullRevision, DatabaseRowMutation,
        DatabaseRowTransformResult, DatabaseSchemaReplay, DatabaseTapeOperation,
        DatabaseTapeRowChange, DatabaseTapeRowChangeType,
    },
    Result,
};

struct TestPollResult(Vec<u8>);

impl DataPollResult<u8> for TestPollResult {
    fn data(&self) -> &[u8] {
        &self.0
    }
}

struct TestCompletion {
    data: RefCell<Bytes>,
    chunk: usize,
}

unsafe impl Sync for TestCompletion {}

impl DataCompletion<u8> for TestCompletion {
    type DataPollResult = TestPollResult;
    fn status(&self) -> crate::Result<Option<u16>> {
        Ok(Some(200))
    }

    fn poll_data(&self) -> crate::Result<Option<Self::DataPollResult>> {
        let mut data = self.data.borrow_mut();
        let len = data.len();
        let chunk = data.split_to(len.min(self.chunk));
        if chunk.is_empty() {
            Ok(None)
        } else {
            Ok(Some(TestPollResult(chunk.to_vec())))
        }
    }

    fn is_done(&self) -> crate::Result<bool> {
        Ok(self.data.borrow().is_empty())
    }
}

struct TestTransformPollResult(Vec<DatabaseRowTransformResult>);

impl DataPollResult<DatabaseRowTransformResult> for TestTransformPollResult {
    fn data(&self) -> &[DatabaseRowTransformResult] {
        &self.0
    }
}

struct TestTransformCompletion;

impl DataCompletion<DatabaseRowTransformResult> for TestTransformCompletion {
    type DataPollResult = TestTransformPollResult;

    fn status(&self) -> crate::Result<Option<u16>> {
        Ok(Some(200))
    }

    fn poll_data(&self) -> crate::Result<Option<Self::DataPollResult>> {
        Ok(None)
    }

    fn is_done(&self) -> crate::Result<bool> {
        Ok(true)
    }
}

#[derive(Default)]
struct TestHttpIo {
    response: Vec<u8>,
    chunk: usize,
    request: Mutex<Option<(String, String, Vec<u8>)>>,
    headers: Mutex<Vec<(String, String)>>,
}

impl SyncEngineIo for TestHttpIo {
    type DataCompletionBytes = TestCompletion;
    type DataCompletionTransform = TestTransformCompletion;

    fn full_read(&self, _path: &str) -> Result<Self::DataCompletionBytes> {
        panic!("full_read is not used in this test")
    }

    fn full_write(&self, _path: &str, _content: Vec<u8>) -> Result<Self::DataCompletionBytes> {
        panic!("full_write is not used in this test")
    }

    fn transform(
        &self,
        _mutations: Vec<DatabaseRowMutation>,
    ) -> Result<Self::DataCompletionTransform> {
        panic!("transform is not used in this test")
    }

    fn http(
        &self,
        _url: Option<&str>,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> Result<Self::DataCompletionBytes> {
        self.request.lock().unwrap().replace((
            method.to_string(),
            path.to_string(),
            body.unwrap_or_default(),
        ));
        *self.headers.lock().unwrap() = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect();
        Ok(TestCompletion {
            data: RefCell::new(self.response.clone().into()),
            chunk: self.chunk,
        })
    }

    fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

    fn step_io_callbacks(&self) {}
}

#[test]
pub fn wait_proto_message_test() {
    let mut data = Vec::new();
    for i in 0..1024 {
        let page = PageData {
            page_id: i as u64,
            encoded_page: vec![0u8; 16 * 1024].into(),
        };
        data.extend_from_slice(&page.encode_length_delimited_to_vec());
    }
    let completion = TestCompletion {
        data: RefCell::new(data.into()),
        chunk: 128,
    };
    let mut gen = genawaiter::sync::Gen::new({
        |coro| async move {
            let coro: Coro<()> = coro.into();
            let mut bytes = BytesMut::new();
            let mut count = 0;
            let network_stats = DataStats::new();
            while wait_proto_message::<(), PageData>(&coro, &completion, &network_stats, &mut bytes)
                .await?
                .is_some()
            {
                assert!(bytes.capacity() <= 16 * 1024 + 1024);
                count += 1;
            }
            assert_eq!(count, 1024);
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
        }
    }
}

#[test]
fn test_remote_encryption_key_header_constant() {
    use super::ENCRYPTION_KEY_HEADER;
    assert_eq!(ENCRYPTION_KEY_HEADER, "x-turso-encryption-key");
}

/// `reset_wal_file` resets the revert WAL and then records the new revert
/// watermark durably in the metadata. If the truncation isn't fsynced, a
/// crash could leave stale revert frames on disk, so the function must issue
/// an fsync after truncating.
#[test]
fn test_reset_wal_file_fsyncs_truncation() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use tempfile::NamedTempFile;
    use turso_core::{io::FileSyncType, Buffer, Completion, File, OpenFlags, IO};

    use crate::database_sync_operations::reset_wal_file;

    struct CountingFile {
        inner: Arc<dyn File>,
        syncs: Arc<AtomicUsize>,
        truncates: Arc<AtomicUsize>,
    }

    impl File for CountingFile {
        fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> turso_core::Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
            self.inner.pread(pos, c)
        }
        fn pwrite(
            &self,
            pos: u64,
            buffer: Arc<Buffer>,
            c: Completion,
        ) -> turso_core::Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }
        fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            self.inner.sync(c, sync_type)
        }
        fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
            self.truncates.fetch_add(1, Ordering::SeqCst);
            self.inner.truncate(len, c)
        }
        fn size(&self) -> turso_core::Result<u64> {
            self.inner.size()
        }
    }

    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let inner = io.open_file(path, OpenFlags::Create, false).unwrap();

    // make the WAL non-empty so the truncation actually has frames to drop
    let buffer = Arc::new(Buffer::new_temporary(4096));
    let c = inner
        .pwrite(0, buffer.clone(), Completion::new_write(|_| {}))
        .unwrap();
    while !c.succeeded() {
        io.step().unwrap();
    }
    assert!(inner.size().unwrap() > 0);

    let syncs = Arc::new(AtomicUsize::new(0));
    let truncates = Arc::new(AtomicUsize::new(0));
    let counting: Arc<dyn File> = Arc::new(CountingFile {
        inner: inner.clone(),
        syncs: syncs.clone(),
        truncates: truncates.clone(),
    });

    let mut gen = genawaiter::sync::Gen::new({
        let counting = counting.clone();
        |coro| async move {
            let coro: Coro<()> = coro.into();
            reset_wal_file(&coro, counting, 0).await
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }

    assert_eq!(truncates.load(Ordering::SeqCst), 1, "wal must be truncated");
    assert!(
        syncs.load(Ordering::SeqCst) >= 1,
        "reset_wal_file must fsync the truncation"
    );
    assert_eq!(counting.size().unwrap(), 0, "wal must be truncated to zero");
}

fn record(values: &[turso_core::Value]) -> Bytes {
    Bytes::from(
        turso_core::types::ImmutableRecord::from_values(values, values.len())
            .unwrap()
            .into_payload(),
    )
}

fn encoded_logical_txns(txns: &[LogicalTxnData]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for txn in txns {
        bytes.extend_from_slice(&txn.encode_length_delimited_to_vec());
    }
    bytes
}

fn logical_schema_op(
    action: LogicalSchemaAction,
    kind: LogicalSchemaKind,
    name: &str,
    sql: Option<&str>,
    stable_table_id: u64,
) -> LogicalOp {
    LogicalOp {
        op_type: LogicalOpType::Schema as i32,
        table_name: String::new(),
        rowid: 0,
        record: Bytes::new(),
        sql: sql.unwrap_or_default().to_string(),
        user_version: None,
        application_id: None,
        schema_action: Some(action as i32),
        schema_kind: Some(kind as i32),
        schema_name: name.to_string(),
        stable_table_id,
    }
}

fn logical_upsert_op(table_name: &str, stable_table_id: u64, rowid: i64, value: &str) -> LogicalOp {
    LogicalOp {
        op_type: LogicalOpType::UpsertRow as i32,
        table_name: table_name.to_string(),
        rowid,
        record: record(&[turso_core::Value::Text(turso_core::types::Text::new(
            value.to_string(),
        ))]),
        sql: String::new(),
        user_version: None,
        application_id: None,
        schema_action: None,
        schema_kind: None,
        schema_name: String::new(),
        stable_table_id,
    }
}

#[test]
fn max_local_change_id_reads_cdc_high_water_mark() {
    let temp_file = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(
        io.clone(),
        temp_file.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            assert_eq!(
                super::max_local_change_id(&coro, &conn).await.unwrap(),
                None
            );
            conn.execute("CREATE TABLE t(x)").unwrap();
            conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
            let max_change_id = super::max_local_change_id(&coro, &conn).await.unwrap();
            assert!(max_change_id.is_some_and(|change_id| change_id > 0));
        }
    });
    while let genawaiter::GeneratorState::Yielded(..) = gen.resume_with(Ok(())) {
        io.step().unwrap()
    }
}

fn write_test_varint(value: u64, out: &mut Vec<u8>) {
    turso_core::storage::sqlite3_ondisk::write_varint_to_vec(value, out);
}

fn test_schema_record(op: &LogicalOp) -> Bytes {
    let kind = LogicalSchemaKind::try_from(op.schema_kind.unwrap()).unwrap();
    let row_type = match kind {
        LogicalSchemaKind::Unspecified => panic!("test schema kind must be specified"),
        LogicalSchemaKind::Table => "table",
        LogicalSchemaKind::Index => "index",
        LogicalSchemaKind::Trigger => "trigger",
        LogicalSchemaKind::View => "view",
    };
    let rootpage = if op.stable_table_id == 0 {
        2
    } else {
        op.stable_table_id as i64
    };
    record(&[
        turso_core::Value::build_text(row_type),
        turso_core::Value::Text(turso_core::types::Text::new(op.schema_name.clone())),
        turso_core::Value::Text(turso_core::types::Text::new(op.schema_name.clone())),
        turso_core::Value::from_i64(rootpage),
        turso_core::Value::Text(turso_core::types::Text::new(op.sql.clone())),
    ])
}

fn append_test_table_upsert(
    recovery_payload: &mut Vec<u8>,
    table_id: i64,
    rowid: i64,
    record: &[u8],
) {
    let mut payload = Vec::new();
    write_test_varint(rowid as u64, &mut payload);
    payload.extend_from_slice(record);
    recovery_payload.push(super::MVCC_OP_UPSERT_TABLE);
    recovery_payload.push(0);
    recovery_payload.extend_from_slice(&(table_id as i32).to_le_bytes());
    write_test_varint(payload.len() as u64, recovery_payload);
    recovery_payload.extend_from_slice(&payload);
}

fn append_test_table_delete(
    recovery_payload: &mut Vec<u8>,
    table_id: i64,
    rowid: i64,
    primary_key_record: &[u8],
) {
    let mut payload = Vec::new();
    write_test_varint(rowid as u64, &mut payload);

    let mut extension = Vec::new();
    write_test_varint(
        (super::MVCC_DELETE_EXT_PK_RECORD_FIELD << 3) | 2,
        &mut extension,
    );
    write_test_varint(primary_key_record.len() as u64, &mut extension);
    extension.extend_from_slice(primary_key_record);

    recovery_payload.push(super::MVCC_OP_DELETE_TABLE);
    recovery_payload.push(super::MVCC_OP_FLAG_PORTABLE_EXTENSION);
    recovery_payload.extend_from_slice(&(table_id as i32).to_le_bytes());
    write_test_varint(payload.len() as u64, recovery_payload);
    recovery_payload.extend_from_slice(&payload);
    write_test_varint(extension.len() as u64, recovery_payload);
    recovery_payload.extend_from_slice(&extension);
}

fn raw_mvcc_log_frame_from_payloads(
    commit_ts: u64,
    portable_payload: &[u8],
    recovery_payload: &[u8],
    op_count: u32,
) -> Vec<u8> {
    let mut extension_block = Vec::new();
    extension_block.extend_from_slice(&super::MVCC_EXTENSION_TYPE_PORTABLE_CHANGES.to_le_bytes());
    extension_block.extend_from_slice(&0u16.to_le_bytes());
    extension_block.extend_from_slice(&(portable_payload.len() as u32).to_le_bytes());
    extension_block.extend_from_slice(portable_payload);

    let mut frame = Vec::new();
    frame.extend_from_slice(&super::MVCC_TX_EXT_FRAME_MAGIC.to_le_bytes());
    frame.extend_from_slice(&(recovery_payload.len() as u64).to_le_bytes());
    frame.extend_from_slice(&op_count.to_le_bytes());
    frame.extend_from_slice(&commit_ts.to_le_bytes());
    frame.extend_from_slice(&(extension_block.len() as u64).to_le_bytes());
    frame.extend_from_slice(&1u32.to_le_bytes());
    frame.extend_from_slice(&super::MVCC_TX_FLAG_HAS_EXTENSION_BLOCK.to_le_bytes());
    frame.extend_from_slice(&extension_block);
    frame.extend_from_slice(recovery_payload);
    frame.extend_from_slice(&0u32.to_le_bytes());
    frame.extend_from_slice(&super::MVCC_TX_END_MAGIC.to_le_bytes());
    frame
}

fn raw_mvcc_log_header(salt: u64) -> Vec<u8> {
    let mut header = vec![0u8; super::MVCC_LOG_HEADER_SIZE];
    header[0..4].copy_from_slice(&super::MVCC_LOG_MAGIC.to_le_bytes());
    header[4] = super::MVCC_LOG_VERSION;
    header[6..8].copy_from_slice(&(super::MVCC_LOG_HEADER_SIZE as u16).to_le_bytes());
    header[8..16].copy_from_slice(&salt.to_le_bytes());
    let crc = crc32c::crc32c(&header);
    header[super::MVCC_LOG_HEADER_CRC_START..super::MVCC_LOG_HEADER_SIZE]
        .copy_from_slice(&crc.to_le_bytes());
    header
}

fn raw_mvcc_log_frame_with_crc(
    commit_ts: u64,
    portable_payload: &[u8],
    recovery_payload: &[u8],
    op_count: u32,
    previous_crc: u32,
) -> (Vec<u8>, u32) {
    let mut frame =
        raw_mvcc_log_frame_from_payloads(commit_ts, portable_payload, recovery_payload, op_count);
    let trailer_start = frame.len() - super::MVCC_TX_TRAILER_SIZE;
    let crc = crc32c::crc32c_append(previous_crc, &frame[..trailer_start]);
    frame[trailer_start..trailer_start + 4].copy_from_slice(&crc.to_le_bytes());
    (frame, crc)
}

fn decode_raw_mvcc_log_for_test(
    header: PullUpdatesRespProtoBody,
    body: Vec<u8>,
) -> Result<Vec<LogicalTxnData>> {
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path().to_owned();
    let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = core_io
        .open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false)
        .unwrap();
    let mut gen = genawaiter::sync::Gen::new({
        let file = file.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            super::decode_raw_mvcc_logical_log_to_file(&coro, &file, &body, &header).await?;
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => {
                result?;
                break;
            }
        }
    }

    let mut bytes = BytesMut::from(std::fs::read(path).unwrap().as_slice());
    let mut txns = Vec::new();
    while let Some(txn) = super::take_proto_message_from_bytes::<LogicalTxnData>(&mut bytes)? {
        txns.push(txn);
    }
    Ok(txns)
}

fn read_logical_txns_from_path(path: &std::path::Path) -> Result<Vec<LogicalTxnData>> {
    let mut bytes = BytesMut::from(std::fs::read(path).unwrap().as_slice());
    let mut txns = Vec::new();
    while let Some(txn) = super::take_proto_message_from_bytes::<LogicalTxnData>(&mut bytes)? {
        txns.push(txn);
    }
    Ok(txns)
}

#[test]
fn raw_mvcc_log_decoder_decodes_portable_schema_and_row_ops() {
    let table_id = -42;
    let schema = logical_schema_op(
        LogicalSchemaAction::Create,
        LogicalSchemaKind::Table,
        "t",
        Some("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)"),
        0,
    );
    let schema_record = test_schema_record(&schema);
    let row_record = record(&[
        turso_core::Value::from_i64(1),
        turso_core::Value::build_text("one"),
    ]);
    let primary_key_record = record(&[turso_core::Value::from_i64(1)]);
    let portable_txn = super::PortableLogicalTxn {
        end_offset: 104,
        commit_ts: 77,
        string_table: vec![
            Bytes::from_static(b"t"),
            Bytes::from_static(super::PORTABLE_TXN_META_CLIENT_KEY.as_bytes()),
            Bytes::from_static(b"client-a"),
        ],
        object_map: vec![super::PortableObjectMap {
            mv_table_id: table_id,
            name_ref: 0,
        }],
        meta: vec![super::PortableMeta {
            key_ref: 1,
            value_ref: 2,
        }],
    };
    let portable_payload = portable_txn.encode_length_delimited_to_vec();
    let mut recovery_payload = Vec::new();
    append_test_table_upsert(
        &mut recovery_payload,
        super::MVCC_SQLITE_SCHEMA_TABLE_ID,
        1,
        &schema_record,
    );
    append_test_table_upsert(&mut recovery_payload, table_id, 1, &row_record);
    append_test_table_delete(&mut recovery_payload, table_id, 1, &primary_key_record);

    let salt = 0x0123_4567_89ab_cdefu64;
    let log_header = raw_mvcc_log_header(salt);
    let initial_crc = crc32c::crc32c(&salt.to_le_bytes());
    let (frame, _) =
        raw_mvcc_log_frame_with_crc(77, &portable_payload, &recovery_payload, 3, initial_crc);
    let end_offset = (log_header.len() + frame.len()) as u64;
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: format!("g1:o{end_offset}"),
        db_size: 0,
        raw_encoding: None,
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: Some(server_proto::MvccLogicalLogMetadataProto {
            format: "lml3".to_string(),
            checkpoint_transition: false,
            ranges: vec![server_proto::MvccLogicalLogRangeProto {
                generation: 1,
                start_offset: 0,
                end_offset,
                starts_with_header: true,
                crc_seed: None,
            }],
        }),
    };
    let mut body = log_header;
    body.extend_from_slice(&frame);

    let txns = decode_raw_mvcc_log_for_test(header, body).unwrap();
    assert_eq!(txns.len(), 1);
    assert_eq!(txns[0].end_offset, 104);
    assert_eq!(txns[0].commit_ts, 77);
    assert_eq!(txns[0].origin_client_id, "client-a");
    assert_eq!(txns[0].ops.len(), 3);
    assert_eq!(txns[0].ops[0].op_type, LogicalOpType::Schema as i32);
    assert_eq!(txns[0].ops[0].schema_name, "t");
    assert_eq!(txns[0].ops[0].stable_table_id, 0);
    // Row deletes are drained before row upserts, so the frame's
    // upsert-then-delete order is inverted here on purpose.
    assert_eq!(txns[0].ops[1].op_type, LogicalOpType::DeleteRow as i32);
    assert_eq!(txns[0].ops[1].table_name, "t");
    assert_eq!(txns[0].ops[1].rowid, 1);
    assert_eq!(txns[0].ops[1].record, primary_key_record);
    assert_eq!(txns[0].ops[2].op_type, LogicalOpType::UpsertRow as i32);
    assert_eq!(txns[0].ops[2].table_name, "t");
    assert_eq!(txns[0].ops[2].rowid, 1);
    assert_eq!(txns[0].ops[2].record, row_record);
}

/// A transaction that swaps the primary keys of two rows arrives as
/// interleaved (delete old key, upsert new image) pairs. Replaying it in that
/// order lets the second row's delete remove the row the first row's upsert
/// just wrote, so the decoder has to group all deletes ahead of all upserts.
#[test]
fn raw_mvcc_log_decoder_orders_row_deletes_before_upserts() {
    let table_id = -42;
    let text = turso_core::Value::build_text;
    let first_new_image = record(&[text("b"), text("2"), text("left")]);
    let second_new_image = record(&[text("a"), text("1"), text("right")]);
    let first_old_key = record(&[text("a"), text("1")]);
    let second_old_key = record(&[text("b"), text("2")]);

    let portable_txn = super::PortableLogicalTxn {
        end_offset: 104,
        commit_ts: 77,
        string_table: vec![Bytes::from_static(b"t")],
        object_map: vec![super::PortableObjectMap {
            mv_table_id: table_id,
            name_ref: 0,
        }],
        meta: vec![],
    };
    let portable_payload = portable_txn.encode_length_delimited_to_vec();

    // Exactly what the MVCC writer emits for
    //   BEGIN;
    //     UPDATE t SET x='tmp' WHERE x='a' AND y='1';
    //     UPDATE t SET x='a', y='1' WHERE x='b' AND y='2';
    //     UPDATE t SET x='b', y='2' WHERE x='tmp';
    //   COMMIT;
    // on `CREATE TABLE t(x, y, z, PRIMARY KEY (x, y))`: one delete+upsert
    // pair per rowid, coalesced to the transaction's final image.
    let mut recovery_payload = Vec::new();
    append_test_table_delete(&mut recovery_payload, table_id, 1, &first_old_key);
    append_test_table_upsert(&mut recovery_payload, table_id, 1, &first_new_image);
    append_test_table_delete(&mut recovery_payload, table_id, 2, &second_old_key);
    append_test_table_upsert(&mut recovery_payload, table_id, 2, &second_new_image);

    let salt = 0x0123_4567_89ab_cdefu64;
    let log_header = raw_mvcc_log_header(salt);
    let initial_crc = crc32c::crc32c(&salt.to_le_bytes());
    let (frame, _) =
        raw_mvcc_log_frame_with_crc(77, &portable_payload, &recovery_payload, 4, initial_crc);
    let end_offset = (log_header.len() + frame.len()) as u64;
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: format!("g1:o{end_offset}"),
        db_size: 0,
        raw_encoding: None,
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: Some(server_proto::MvccLogicalLogMetadataProto {
            format: "lml3".to_string(),
            checkpoint_transition: false,
            ranges: vec![server_proto::MvccLogicalLogRangeProto {
                generation: 1,
                start_offset: 0,
                end_offset,
                starts_with_header: true,
                crc_seed: None,
            }],
        }),
    };
    let mut body = log_header;
    body.extend_from_slice(&frame);

    let txns = decode_raw_mvcc_log_for_test(header, body).unwrap();
    assert_eq!(txns.len(), 1);
    let ops = &txns[0].ops;
    assert_eq!(ops.len(), 4);
    let kinds = ops.iter().map(|op| op.op_type).collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            LogicalOpType::DeleteRow as i32,
            LogicalOpType::DeleteRow as i32,
            LogicalOpType::UpsertRow as i32,
            LogicalOpType::UpsertRow as i32,
        ],
        "row deletes must precede row upserts"
    );
    // Order within each group is the frame's order.
    assert_eq!(ops[0].record, first_old_key);
    assert_eq!(ops[1].record, second_old_key);
    assert_eq!(ops[2].record, first_new_image);
    assert_eq!(ops[3].record, second_new_image);
}

#[test]
fn pull_updates_v1_decodes_raw_mvcc_log_stream() {
    let table_id = -42;
    let record = record(&[turso_core::Value::build_text("logical")]);
    let expected_txn = LogicalTxnData {
        end_offset: 104,
        commit_ts: 77,
        origin_client_id: String::new(),
        ops: vec![LogicalOp {
            op_type: LogicalOpType::UpsertRow as i32,
            table_name: "t".to_string(),
            rowid: 7,
            record: record.clone(),
            sql: String::new(),
            user_version: None,
            application_id: None,
            schema_action: None,
            schema_kind: None,
            schema_name: String::new(),
            stable_table_id: 0,
        }],
    };
    let portable_txn = super::PortableLogicalTxn {
        end_offset: expected_txn.end_offset,
        commit_ts: expected_txn.commit_ts,
        string_table: vec![Bytes::from_static(b"t")],
        object_map: vec![super::PortableObjectMap {
            mv_table_id: table_id,
            name_ref: 0,
        }],
        meta: Vec::new(),
    };
    let portable_payload = portable_txn.encode_length_delimited_to_vec();
    let mut recovery_payload = Vec::new();
    append_test_table_upsert(&mut recovery_payload, table_id, 7, &record);
    let crc_seed = crc32c::crc32c(&1234u64.to_le_bytes());
    let (raw_frame, _) =
        raw_mvcc_log_frame_with_crc(77, &portable_payload, &recovery_payload, 1, crc_seed);
    let range_start = super::MVCC_LOG_HEADER_SIZE as u64;
    let range_end = range_start + raw_frame.len() as u64;
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: format!("g1:o{range_end}"),
        db_size: 0,
        raw_encoding: None,
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: Some(server_proto::MvccLogicalLogMetadataProto {
            format: "lml3".to_string(),
            checkpoint_transition: false,
            ranges: vec![server_proto::MvccLogicalLogRangeProto {
                generation: 1,
                start_offset: range_start,
                end_offset: range_end,
                starts_with_header: false,
                crc_seed: Some(crc_seed.to_le_bytes().to_vec()),
            }],
        }),
    };
    let mut response = Vec::new();
    response.extend_from_slice(&header.encode_length_delimited_to_vec());
    response.extend_from_slice(&raw_frame);

    let io = Arc::new(TestHttpIo {
        response,
        chunk: 7,
        request: Mutex::new(None),
        headers: Mutex::new(Vec::new()),
    });
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path().to_owned();
    let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = core_io
        .open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false)
        .unwrap();

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let file = file.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let stats = SyncEngineIoStats::new(io.clone());
            let ctx =
                SyncOperationCtx::new(&coro, &stats, Some("https://example.com".to_string()), None);
            let (revision, result, _) = pull_updates_v1(&ctx, &file, "g1:o56", None, true).await?;
            let DatabasePullRevision::V1 { revision } = revision else {
                panic!("expected V1 revision");
            };
            assert_eq!(revision, format!("g1:o{range_end}"));
            assert_eq!(result, PullUpdatesV1Result::Logical { txns: 1, ops: 1 });
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }

    assert_eq!(
        read_logical_txns_from_path(&path).unwrap(),
        vec![expected_txn]
    );
    let request = io.request.lock().unwrap().clone().unwrap();
    let req = PullUpdatesReqProtoBody::decode(request.2.as_slice()).unwrap();
    assert_eq!(
        req.stream_kind,
        PullUpdatesStreamKind::MvccLogicalLog as i32
    );
}

#[test]
fn pull_updates_v1_accepts_page_stream_when_logical_pull_is_requested() {
    let page = vec![7u8; super::PAGE_SIZE];
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: "g1:o45".to_string(),
        db_size: 1,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: None,
    };
    let page_data = PageData {
        page_id: 0,
        encoded_page: page.clone().into(),
    };
    let mut response = Vec::new();
    response.extend_from_slice(&header.encode_length_delimited_to_vec());
    response.extend_from_slice(&page_data.encode_length_delimited_to_vec());

    let io = Arc::new(TestHttpIo {
        response,
        chunk: 11,
        request: Mutex::new(None),
        headers: Mutex::new(Vec::new()),
    });
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path().to_owned();
    let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = core_io
        .open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false)
        .unwrap();

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let file = file.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let stats = SyncEngineIoStats::new(io.clone());
            let ctx =
                SyncOperationCtx::new(&coro, &stats, Some("https://example.com".to_string()), None);
            let (revision, result, _) = pull_updates_v1(&ctx, &file, "g1:o40", None, true).await?;
            let DatabasePullRevision::V1 { revision } = revision else {
                panic!("expected V1 revision");
            };
            assert_eq!(revision, "g1:o45");
            assert_eq!(
                result,
                PullUpdatesV1Result::Pages {
                    replace_base: false
                }
            );
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }

    let bytes = std::fs::read(path).unwrap();
    assert_eq!(bytes.len(), super::WAL_FRAME_SIZE);
    let info =
        turso_core::types::WalFrameInfo::from_frame_header(&bytes[..super::WAL_FRAME_HEADER]);
    assert_eq!(info.page_no, 1);
    assert_eq!(info.db_size, 1);
    assert_eq!(&bytes[super::WAL_FRAME_HEADER..], page.as_slice());
    let request = io.request.lock().unwrap().clone().unwrap();
    let req = PullUpdatesReqProtoBody::decode(request.2.as_slice()).unwrap();
    assert_eq!(
        req.stream_kind,
        PullUpdatesStreamKind::MvccLogicalLog as i32
    );
}

#[test]
fn pull_updates_v1_preserves_replace_base_page_fallback() {
    let page = vec![9u8; super::PAGE_SIZE];
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: "g1:o80".to_string(),
        db_size: 1,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        apply_mode: PullUpdatesApplyMode::ReplaceBase as i32,
        mvcc_log: None,
    };
    let page_data = PageData {
        page_id: 0,
        encoded_page: page.clone().into(),
    };
    let mut response = Vec::new();
    response.extend_from_slice(&header.encode_length_delimited_to_vec());
    response.extend_from_slice(&page_data.encode_length_delimited_to_vec());

    let io = Arc::new(TestHttpIo {
        response,
        chunk: 13,
        request: Mutex::new(None),
        headers: Mutex::new(Vec::new()),
    });
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path().to_owned();
    let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = core_io
        .open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false)
        .unwrap();

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let file = file.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let stats = SyncEngineIoStats::new(io.clone());
            let ctx =
                SyncOperationCtx::new(&coro, &stats, Some("https://example.com".to_string()), None);
            let (revision, result, _) = pull_updates_v1(&ctx, &file, "g1:o40", None, true).await?;
            let DatabasePullRevision::V1 { revision } = revision else {
                panic!("expected V1 revision");
            };
            assert_eq!(revision, "g1:o80");
            assert_eq!(result, PullUpdatesV1Result::Pages { replace_base: true });
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }

    let bytes = std::fs::read(path).unwrap();
    assert_eq!(bytes.len(), super::WAL_FRAME_SIZE);
    let info =
        turso_core::types::WalFrameInfo::from_frame_header(&bytes[..super::WAL_FRAME_HEADER]);
    assert_eq!(info.page_no, 1);
    assert_eq!(info.db_size, 1);
    assert_eq!(&bytes[super::WAL_FRAME_HEADER..], page.as_slice());
}

#[test]
fn wal_pull_to_file_v1_rejects_replace_base_page_stream() {
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: "g1:o80".to_string(),
        db_size: 1,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        apply_mode: PullUpdatesApplyMode::ReplaceBase as i32,
        mvcc_log: None,
    };
    let response = header.encode_length_delimited_to_vec();

    let io = Arc::new(TestHttpIo {
        response,
        chunk: 13,
        request: Mutex::new(None),
        headers: Mutex::new(Vec::new()),
    });
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path().to_owned();
    let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = core_io
        .open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false)
        .unwrap();

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let file = file.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let stats = SyncEngineIoStats::new(io.clone());
            let ctx =
                SyncOperationCtx::new(&coro, &stats, Some("https://example.com".to_string()), None);
            let err = wal_pull_to_file_v1(&ctx, &file, "g1:o40", None)
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("replace-base page streams"),
                "unexpected error: {err:?}"
            );
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }

    assert_eq!(file.size().unwrap(), 0);
    let request = io.request.lock().unwrap().clone().unwrap();
    let req = PullUpdatesReqProtoBody::decode(request.2.as_slice()).unwrap();
    assert_eq!(req.stream_kind, PullUpdatesStreamKind::Pages as i32);
}

#[test]
fn pull_pages_v1_accepts_replace_base_page_stream_for_revision_pinned_reads() {
    let page = vec![11u8; super::PAGE_SIZE];
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: "g1:o80".to_string(),
        db_size: 3,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        apply_mode: PullUpdatesApplyMode::ReplaceBase as i32,
        mvcc_log: None,
    };
    let page_data = PageData {
        page_id: 2,
        encoded_page: page.clone().into(),
    };
    let mut response = Vec::new();
    response.extend_from_slice(&header.encode_length_delimited_to_vec());
    response.extend_from_slice(&page_data.encode_length_delimited_to_vec());

    let io = Arc::new(TestHttpIo {
        response,
        chunk: 13,
        request: Mutex::new(None),
        headers: Mutex::new(Vec::new()),
    });

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let stats = SyncEngineIoStats::new(io.clone());
            let ctx =
                SyncOperationCtx::new(&coro, &stats, Some("https://example.com".to_string()), None);
            let loaded = pull_pages_v1(&ctx, "g1:o40", &[2]).await?;
            assert_eq!(loaded.db_pages, 3);
            assert_eq!(loaded.pages.len(), 1);
            assert_eq!(loaded.pages[0].page_id, 2);
            assert_eq!(loaded.pages[0].page, page);
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }

    let request = io.request.lock().unwrap().clone().unwrap();
    let req = PullUpdatesReqProtoBody::decode(request.2.as_slice()).unwrap();
    assert_eq!(request.0, "POST");
    assert_eq!(request.1, "/pull-updates");
    assert_eq!(req.stream_kind, PullUpdatesStreamKind::Pages as i32);
    assert_eq!(req.server_revision, "g1:o40");
    assert_eq!(req.client_revision, "");
}

#[test]
fn pull_updates_v1_rejects_remote_encryption_for_logical_pull() {
    let io = Arc::new(TestHttpIo {
        response: Vec::new(),
        chunk: 32,
        request: Mutex::new(None),
        headers: Mutex::new(Vec::new()),
    });
    let temp = NamedTempFile::new().unwrap();
    let path = temp.path().to_owned();
    let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = core_io
        .open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false)
        .unwrap();

    let mut gen = genawaiter::sync::Gen::new({
        let io = io.clone();
        let file = file.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let stats = SyncEngineIoStats::new(io.clone());
            let ctx = SyncOperationCtx::new(
                &coro,
                &stats,
                Some("https://example.com".to_string()),
                Some("dGVzdC1lbmNyeXB0aW9uLWtleQ=="),
            );
            let err = pull_updates_v1(&ctx, &file, "g1:o40", None, true)
                .await
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("not supported with encrypted remote databases"),
                "unexpected error: {err:?}"
            );
            Result::Ok(())
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => {}
            genawaiter::GeneratorState::Complete(result) => {
                result.unwrap();
                break;
            }
        }
    }

    assert!(io.request.lock().unwrap().is_none());
    assert!(io.headers.lock().unwrap().is_empty());
}

#[test]
fn raw_mvcc_log_decoder_requires_crc_seed_for_mid_log_range() {
    let portable_txn = super::PortableLogicalTxn {
        end_offset: 104,
        commit_ts: 77,
        string_table: Vec::new(),
        object_map: Vec::new(),
        meta: Vec::new(),
    };
    let portable_payload = portable_txn.encode_length_delimited_to_vec();
    let crc_seed = crc32c::crc32c(&1234u64.to_le_bytes());
    let (frame, _) = raw_mvcc_log_frame_with_crc(77, &portable_payload, &[], 0, crc_seed);
    let range_start = super::MVCC_LOG_HEADER_SIZE as u64;
    let range_end = range_start + frame.len() as u64;
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: format!("g1:o{range_end}"),
        db_size: 0,
        raw_encoding: None,
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: Some(server_proto::MvccLogicalLogMetadataProto {
            format: "lml3".to_string(),
            checkpoint_transition: false,
            ranges: vec![server_proto::MvccLogicalLogRangeProto {
                generation: 1,
                start_offset: range_start,
                end_offset: range_end,
                starts_with_header: false,
                crc_seed: None,
            }],
        }),
    };

    let err = decode_raw_mvcc_log_for_test(header, frame).unwrap_err();
    assert!(
        err.to_string().contains("missing CRC seed"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn raw_mvcc_log_decoder_validates_header_and_frame_crc() {
    let portable_txn = super::PortableLogicalTxn {
        end_offset: 104,
        commit_ts: 77,
        string_table: Vec::new(),
        object_map: Vec::new(),
        meta: Vec::new(),
    };
    let portable_payload = portable_txn.encode_length_delimited_to_vec();
    let salt = 0x1020_3040_5060_7080u64;
    let mut log_header = raw_mvcc_log_header(salt);
    let initial_crc = crc32c::crc32c(&salt.to_le_bytes());
    let (frame, _) = raw_mvcc_log_frame_with_crc(77, &portable_payload, &[], 0, initial_crc);
    log_header[super::MVCC_LOG_HEADER_CRC_START] ^= 0x01;
    let end_offset = (log_header.len() + frame.len()) as u64;
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: format!("g1:o{end_offset}"),
        db_size: 0,
        raw_encoding: None,
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: Some(server_proto::MvccLogicalLogMetadataProto {
            format: "lml3".to_string(),
            checkpoint_transition: false,
            ranges: vec![server_proto::MvccLogicalLogRangeProto {
                generation: 1,
                start_offset: 0,
                end_offset,
                starts_with_header: true,
                crc_seed: None,
            }],
        }),
    };
    let mut body = log_header;
    body.extend_from_slice(&frame);

    let err = decode_raw_mvcc_log_for_test(header, body).unwrap_err();
    assert!(
        err.to_string().contains("header checksum mismatch"),
        "unexpected error: {err:?}"
    );

    let log_header = raw_mvcc_log_header(salt);
    let initial_crc = crc32c::crc32c(&salt.to_le_bytes());
    let (mut frame, _) = raw_mvcc_log_frame_with_crc(77, &portable_payload, &[], 0, initial_crc);
    frame[super::MVCC_TX_EXT_HEADER_SIZE + super::MVCC_EXTENSION_RECORD_HEADER_SIZE] ^= 0x01;
    let end_offset = (log_header.len() + frame.len()) as u64;
    let header = PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: format!("g1:o{end_offset}"),
        db_size: 0,
        raw_encoding: None,
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: Some(server_proto::MvccLogicalLogMetadataProto {
            format: "lml3".to_string(),
            checkpoint_transition: false,
            ranges: vec![server_proto::MvccLogicalLogRangeProto {
                generation: 1,
                start_offset: 0,
                end_offset,
                starts_with_header: true,
                crc_seed: None,
            }],
        }),
    };
    let mut body = log_header;
    body.extend_from_slice(&frame);

    let err = decode_raw_mvcc_log_for_test(header, body).unwrap_err();
    assert!(
        err.to_string().contains("transaction checksum mismatch"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn logical_txn_acknowledges_client_from_origin_or_sync_metadata_row() {
    let origin_txn = LogicalTxnData {
        end_offset: 1,
        commit_ts: 1,
        origin_client_id: "client-a".to_string(),
        ops: Vec::new(),
    };
    assert!(super::logical_txn_acknowledges_client(&origin_txn, "client-a").unwrap());
    assert!(!super::logical_txn_acknowledges_client(&origin_txn, "client-b").unwrap());

    let metadata_txn = LogicalTxnData {
        end_offset: 2,
        commit_ts: 2,
        origin_client_id: String::new(),
        ops: vec![LogicalOp {
            op_type: LogicalOpType::UpsertRow as i32,
            table_name: super::TURSO_SYNC_TABLE_NAME.to_string(),
            rowid: 1,
            record: record(&[
                turso_core::Value::Text(turso_core::types::Text::new("client-b".to_string())),
                turso_core::Value::from_i64(42),
            ]),
            sql: String::new(),
            user_version: None,
            application_id: None,
            schema_action: None,
            schema_kind: None,
            schema_name: String::new(),
            stable_table_id: 0,
        }],
    };
    assert!(super::logical_txn_acknowledges_client(&metadata_txn, "client-b").unwrap());
    assert!(!super::logical_txn_acknowledges_client(&metadata_txn, "client-a").unwrap());
}

#[test]
fn file_backed_logical_replay_skips_self_origin_and_keeps_table_map() {
    let db_temp = NamedTempFile::new().unwrap();
    let txns_temp = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let txns = vec![
        LogicalTxnData {
            end_offset: 1,
            commit_ts: 1,
            origin_client_id: "client-a".to_string(),
            ops: vec![logical_schema_op(
                LogicalSchemaAction::Create,
                LogicalSchemaKind::Table,
                "local_only",
                Some("CREATE TABLE local_only(x TEXT)"),
                99,
            )],
        },
        LogicalTxnData {
            end_offset: 2,
            commit_ts: 2,
            origin_client_id: "remote".to_string(),
            ops: vec![logical_schema_op(
                LogicalSchemaAction::Create,
                LogicalSchemaKind::Table,
                "items",
                Some("CREATE TABLE items(x TEXT)"),
                7,
            )],
        },
        LogicalTxnData {
            end_offset: 3,
            commit_ts: 3,
            origin_client_id: "client-a".to_string(),
            ops: vec![logical_upsert_op("items", 0, 1, "local")],
        },
        LogicalTxnData {
            end_offset: 4,
            commit_ts: 4,
            origin_client_id: "remote".to_string(),
            ops: vec![logical_upsert_op("", 7, 2, "remote")],
        },
        LogicalTxnData {
            end_offset: 5,
            commit_ts: 5,
            origin_client_id: "client-a".to_string(),
            ops: vec![logical_upsert_op("", 7, 2, "local-overwrite")],
        },
    ];
    std::fs::write(txns_temp.path(), encoded_logical_txns(&txns)).unwrap();

    let db = turso_core::Database::open_file(
        io.clone(),
        db_temp.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let db = Arc::new(DatabaseTape::new(db));
    let txns_file = io
        .open_file(
            txns_temp.path().to_str().unwrap(),
            turso_core::OpenFlags::None,
            false,
        )
        .unwrap();

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        let txns_file = txns_file.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let opts = DatabaseReplaySessionOpts {
                use_implicit_rowid: true,
            };
            let mut replay = db.start_replay_session(&coro, opts).await.unwrap();
            let mut table_names_by_stable_id = BTreeMap::new();
            let stats =
                apply_logical_transactions_file_without_commit_excluding_client_txns_with_table_map_and_stats(
                    &coro,
                    &mut replay,
                    &txns_file,
                    "client-a",
                    &mut table_names_by_stable_id,
                )
                .await
                .unwrap();
            replay
                .replay(&coro, DatabaseTapeOperation::Commit)
                .await
                .unwrap();

            let conn = db.connect(&coro).await.unwrap();
            let mut stmt = conn.prepare("SELECT rowid, x FROM items").unwrap();
            let mut rows = Vec::new();
            while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            (stats.touched_rows, table_names_by_stable_id, rows)
        }
    });
    let (touched_rows, table_names_by_stable_id, rows) = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };

    assert_eq!(table_names_by_stable_id.get(&7).unwrap(), "items");
    assert!(!table_names_by_stable_id.contains_key(&99));
    assert!(touched_rows.contains(&("items".to_string(), 2)));
    assert!(!touched_rows.contains(&("items".to_string(), 1)));
    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::from_i64(2),
            turso_core::Value::Text(turso_core::types::Text::new("remote".to_string())),
        ]]
    );
}

#[test]
fn logical_txn_to_tape_operations_maps_schema_and_stable_table_rows() {
    let txn = LogicalTxnData {
        end_offset: 128,
        commit_ts: 77,
        origin_client_id: String::new(),
        ops: vec![
            logical_schema_op(
                LogicalSchemaAction::Create,
                LogicalSchemaKind::Table,
                "items",
                Some("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)"),
                9,
            ),
            LogicalOp {
                op_type: LogicalOpType::UpsertRow as i32,
                table_name: String::new(),
                rowid: 1,
                record: record(&[
                    turso_core::Value::from_i64(1),
                    turso_core::Value::build_text("alpha"),
                ]),
                sql: String::new(),
                user_version: None,
                application_id: None,
                schema_action: None,
                schema_kind: None,
                schema_name: String::new(),
                stable_table_id: 9,
            },
            LogicalOp {
                op_type: LogicalOpType::DeleteRow as i32,
                table_name: String::new(),
                rowid: 99,
                record: record(&[turso_core::Value::from_i64(1)]),
                sql: String::new(),
                user_version: None,
                application_id: None,
                schema_action: None,
                schema_kind: None,
                schema_name: String::new(),
                stable_table_id: 9,
            },
        ],
    };

    let operations = logical_txn_to_tape_operations(&txn).unwrap();
    assert_eq!(operations.len(), 3);
    assert!(matches!(
        &operations[0],
        DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Create { sql })
            if sql.contains("CREATE TABLE items")
    ));
    match &operations[1] {
        DatabaseTapeOperation::RowChange(change) => {
            assert_eq!(change.table_name, "items");
            assert_eq!(change.id, 1);
            assert_eq!(change.change_time, 77);
            assert!(matches!(
                change.change,
                DatabaseTapeRowChangeType::Insert { .. }
            ));
        }
        other => panic!("expected row change, got {other:?}"),
    }
    match &operations[2] {
        DatabaseTapeOperation::RowChange(change) => {
            assert_eq!(change.table_name, "items");
            assert_eq!(change.id, 99);
            assert!(matches!(
                &change.change,
                DatabaseTapeRowChangeType::Delete {
                    before,
                    key: Some(key)
                } if before.is_empty() && *key == vec![turso_core::Value::from_i64(1)]
            ));
        }
        other => panic!("expected row change, got {other:?}"),
    }
}

#[test]
fn logical_txn_to_tape_operations_filters_internal_tables() {
    assert!(!is_logically_replayable_table("turso_sync_last_change_id"));
    assert!(!is_logically_replayable_table("turso_cdc"));
    assert!(!is_logically_replayable_table("sqlite_sequence"));
    assert!(!is_logically_replayable_table("__turso_internal_mvcc_meta"));

    let txn = LogicalTxnData {
        end_offset: 128,
        commit_ts: 77,
        origin_client_id: String::new(),
        ops: vec![logical_schema_op(
            LogicalSchemaAction::Create,
            LogicalSchemaKind::Table,
            "turso_sync_last_change_id",
            Some("CREATE TABLE turso_sync_last_change_id(client_id TEXT PRIMARY KEY)"),
            1,
        )],
    };

    let operations = logical_txn_to_tape_operations(&txn).unwrap();
    assert!(operations.is_empty());
}

#[test]
fn push_change_filter_skips_internal_sqlite_schema_objects() {
    let opts = push_test_opts();
    let internal_schema_change = DatabaseTapeRowChange {
        change_id: 1,
        change_time: 77,
        table_name: "sqlite_schema".to_string(),
        id: 1,
        change: DatabaseTapeRowChangeType::Insert {
            after: parse_bin_record(record(&[
                turso_core::Value::build_text("table"),
                turso_core::Value::build_text("__turso_internal_mvcc_meta"),
                turso_core::Value::build_text("__turso_internal_mvcc_meta"),
                turso_core::Value::from_i64(42),
                turso_core::Value::build_text(
                    "CREATE TABLE __turso_internal_mvcc_meta(k TEXT, v INTEGER)",
                ),
            ]))
            .unwrap(),
        },
    };
    let user_schema_change = DatabaseTapeRowChange {
        change_id: 2,
        change_time: 77,
        table_name: "sqlite_schema".to_string(),
        id: 2,
        change: DatabaseTapeRowChangeType::Insert {
            after: parse_bin_record(record(&[
                turso_core::Value::build_text("table"),
                turso_core::Value::build_text("items"),
                turso_core::Value::build_text("items"),
                turso_core::Value::from_i64(43),
                turso_core::Value::build_text("CREATE TABLE items(id INTEGER PRIMARY KEY)"),
            ]))
            .unwrap(),
        },
    };

    assert!(!should_push_change(&internal_schema_change, &opts).unwrap());
    assert!(should_push_change(&user_schema_change, &opts).unwrap());
    assert!(!should_replay_local_change(&internal_schema_change).unwrap());
    assert!(should_replay_local_change(&user_schema_change).unwrap());

    let rootpage_only_schema_update = DatabaseTapeRowChange {
        change_id: 3,
        change_time: 77,
        table_name: "sqlite_schema".to_string(),
        id: 2,
        change: DatabaseTapeRowChangeType::Update {
            before: Vec::new(),
            after: parse_bin_record(record(&[
                turso_core::Value::build_text("table"),
                turso_core::Value::build_text("items"),
                turso_core::Value::build_text("items"),
                turso_core::Value::from_i64(44),
                turso_core::Value::build_text("CREATE TABLE items(id INTEGER PRIMARY KEY)"),
            ]))
            .unwrap(),
            updates: Some(
                parse_bin_record(record(&[
                    turso_core::Value::from_i64(0),
                    turso_core::Value::from_i64(0),
                    turso_core::Value::from_i64(0),
                    turso_core::Value::from_i64(1),
                    turso_core::Value::from_i64(0),
                    turso_core::Value::Null,
                    turso_core::Value::Null,
                    turso_core::Value::Null,
                    turso_core::Value::from_i64(44),
                    turso_core::Value::Null,
                ]))
                .unwrap(),
            ),
        },
    };

    assert!(!should_push_change(&rootpage_only_schema_update, &opts).unwrap());
    assert!(!should_replay_local_change(&rootpage_only_schema_update).unwrap());
}

#[test]
fn count_local_changes_counts_only_changes_which_push_will_send() {
    let db_temp = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(
        io.clone(),
        db_temp.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let opts = push_test_opts();
            let conn = db.connect(&coro).await.unwrap();

            conn.execute("CREATE TABLE t(x)").unwrap();
            conn.execute("INSERT INTO t VALUES (1)").unwrap();
            conn.execute("INSERT INTO t VALUES (2), (3)").unwrap();
            let after_writes = count_local_changes(&coro, &conn, &opts, 0).await.unwrap();

            update_last_change_id(&coro, &conn, "client-a", 1, 0)
                .await
                .unwrap();
            let after_first_high_water_mark =
                count_local_changes(&coro, &conn, &opts, 0).await.unwrap();

            update_last_change_id(&coro, &conn, "client-a", 2, 3)
                .await
                .unwrap();
            let after_second_high_water_mark =
                count_local_changes(&coro, &conn, &opts, 0).await.unwrap();

            let ignore_t = DatabaseSyncEngineOpts {
                tables_ignore: vec!["t".to_string()],
                ..push_test_opts()
            };
            let with_ignored_table = count_local_changes(&coro, &conn, &ignore_t, 0)
                .await
                .unwrap();

            let mut stmt = conn.prepare("SELECT COUNT(*) FROM turso_cdc").unwrap();
            let cdc_rows = run_stmt_expect_one_row(&coro, &mut stmt)
                .await
                .unwrap()
                .unwrap()[0]
                .as_int()
                .unwrap();
            (
                after_writes,
                after_first_high_water_mark,
                after_second_high_water_mark,
                with_ignored_table,
                cdc_rows,
            )
        }
    });
    let (
        after_writes,
        after_first_high_water_mark,
        after_second_high_water_mark,
        with_ignored_table,
        cdc_rows,
    ) = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };

    assert_eq!(after_writes, 4);
    assert_eq!(after_second_high_water_mark, after_first_high_water_mark);
    assert_eq!(
        with_ignored_table,
        after_second_high_water_mark - 3,
        "the three rows written to t must drop out"
    );
    assert!(cdc_rows > after_second_high_water_mark);
}

#[test]
fn sql_table_name_filter_agrees_with_is_logically_replayable_table() {
    let names = [
        "t",
        "items",
        "turso_data",
        "sqlite_schema",
        "sqlite_sequence",
        "turso_sync_last_change_id",
        "turso_cdc",
        "turso_cdc_version",
        "__turso_internal_mvcc_meta",
        "sqliteXschema",
        "__tursoXinternalXmvcc",
    ];

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db =
        turso_core::Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
    let db = Arc::new(DatabaseTape::new(db));

    let mut gen = genawaiter::sync::Gen::new({
        let db = db.clone();
        move |coro| async move {
            let coro: Coro<()> = coro.into();
            let conn = db.connect(&coro).await.unwrap();
            let mut stmt = conn
                .prepare(format!(
                    "SELECT {} FROM (SELECT ? AS table_name)",
                    logically_replayable_table_filter()
                ))
                .unwrap();
            let mut matched = Vec::new();
            for name in names {
                stmt.reset().unwrap();
                stmt.bind_at(1.try_into().unwrap(), turso_core::Value::build_text(name))
                    .unwrap();
                let row = run_stmt_expect_one_row(&coro, &mut stmt).await.unwrap();
                matched.push(row.unwrap()[0].as_int().unwrap() == 1);
            }
            matched
        }
    });
    let matched = loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    };

    let expected = names
        .iter()
        .map(|name| is_logically_replayable_table(name))
        .collect::<Vec<_>>();
    assert_eq!(matched, expected);
}

fn push_test_opts() -> DatabaseSyncEngineOpts {
    DatabaseSyncEngineOpts {
        remote_url: None,
        client_name: "test-client".to_string(),
        tables_ignore: Vec::new(),
        use_transform: false,
        wal_pull_batch_size: 0,
        long_poll_timeout: None,
        protocol_version_hint: crate::types::DatabaseSyncEngineProtocolVersion::V1,
        bootstrap_if_empty: false,
        reserved_bytes: 0,
        db_opts: turso_core::DatabaseOpts::default(),
        partial_sync_opts: None,
        remote_encryption_key: None,
        push_operations_threshold: None,
        pull_bytes_threshold: None,
        logical_mvcc_pull: Some(true),
    }
}

fn page_header(stream_kind: i32, apply_mode: i32) -> PullUpdatesRespProtoBody {
    PullUpdatesRespProtoBody {
        protocol: 0,
        server_revision: "rev".to_string(),
        db_size: 1,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind,
        apply_mode,
        mvcc_log: None,
    }
}

#[test]
fn ensure_page_stream_accepts_default_page_header() {
    ensure_page_stream(
        &page_header(
            PullUpdatesStreamKind::Pages as i32,
            PullUpdatesApplyMode::Incremental as i32,
        ),
        "test",
    )
    .unwrap();
}

#[test]
fn ensure_incremental_page_stream_rejects_replace_base() {
    ensure_page_stream(
        &page_header(
            PullUpdatesStreamKind::Pages as i32,
            PullUpdatesApplyMode::ReplaceBase as i32,
        ),
        "test",
    )
    .unwrap();
    let err = ensure_incremental_page_stream(
        &page_header(
            PullUpdatesStreamKind::Pages as i32,
            PullUpdatesApplyMode::ReplaceBase as i32,
        ),
        "test",
    )
    .unwrap_err();
    assert!(err.to_string().contains("replace-base page streams"));
}

#[test]
fn ensure_page_stream_rejects_logical_log_header() {
    let err = ensure_page_stream(
        &page_header(
            PullUpdatesStreamKind::MvccLogicalLog as i32,
            PullUpdatesApplyMode::Incremental as i32,
        ),
        "test",
    )
    .unwrap_err();
    assert!(err
        .to_string()
        .contains("does not support raw MVCC logical-log"));
}

#[test]
fn ensure_page_stream_rejects_unknown_enums() {
    let err = ensure_page_stream(
        &page_header(99, PullUpdatesApplyMode::Incremental as i32),
        "test",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown pull-updates stream kind"));

    let err = ensure_page_stream(
        &page_header(PullUpdatesStreamKind::Pages as i32, 99),
        "test",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown pull-updates apply mode"));
}

/// Pushed DDL is replayed verbatim on the remote, where the same object may
/// already exist because another client pushed its own version of the DDL
/// first. CREATE statements must be rewritten with IF NOT EXISTS so the
/// remote replay stays idempotent instead of failing with "already exists".
#[test]
pub fn test_pushed_create_ddl_is_rewritten_as_if_not_exists() {
    let rewritten = super::rewrite_create_ddl_as_if_not_exists("CREATE TABLE q (x, y, z)").unwrap();
    assert!(
        rewritten.contains("IF NOT EXISTS"),
        "unexpected rewrite: {rewritten}"
    );
    let rewritten =
        super::rewrite_create_ddl_as_if_not_exists("CREATE INDEX q_x ON q (x)").unwrap();
    assert!(
        rewritten.contains("IF NOT EXISTS"),
        "unexpected rewrite: {rewritten}"
    );
}

#[test]
pub fn test_non_create_ddl_is_not_rewritten() {
    assert!(super::rewrite_create_ddl_as_if_not_exists("ALTER TABLE q ADD COLUMN w").is_none());
    assert!(super::rewrite_create_ddl_as_if_not_exists("DROP TABLE q").is_none());
    assert!(super::rewrite_create_ddl_as_if_not_exists("not valid sql").is_none());
}

#[test]
fn detect_remote_pull_protocol_reads_the_explicit_field_only() {
    use crate::server_proto::PullUpdatesProtocol;
    use crate::types::RemotePullProtocol;

    let header = |protocol: i32| PullUpdatesRespProtoBody {
        server_revision: "g1:o0".to_string(),
        db_size: 0,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::Pages as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: None,
        protocol,
    };

    assert_eq!(
        detect_remote_pull_protocol(&header(PullUpdatesProtocol::MvccLogical as i32)),
        RemotePullProtocol::MvccLogical
    );
    assert_eq!(
        detect_remote_pull_protocol(&header(PullUpdatesProtocol::Pages as i32)),
        RemotePullProtocol::Pages
    );
    // Servers deploy before SDK releases: a missing field (old server)
    // or an unknown future value means a page-protocol server.
    assert_eq!(
        detect_remote_pull_protocol(&header(PullUpdatesProtocol::Unspecified as i32)),
        RemotePullProtocol::Pages
    );
    assert_eq!(
        detect_remote_pull_protocol(&header(99)),
        RemotePullProtocol::Pages
    );
}
