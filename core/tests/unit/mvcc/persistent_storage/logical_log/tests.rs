use crate::types::IOResult;
use crate::util::IOExt as _;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Once;

use quickcheck_macros::quickcheck;
use rand::{random_range, rng, Rng};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

use crate::io::MemoryIO;
use crate::sync::Arc;
use crate::{
    mvcc::database::{
        tests::{commit_tx, generate_simple_string_row, MvccTestDbNoConn},
        MVTableId, Row, RowID, RowKey, SortableIndexKey,
    },
    schema::Table,
    storage::sqlite3_ondisk::{
        read_varint, read_varint_partial, varint_len, write_varint, DatabaseHeader,
    },
    types::{ImmutableRecord, ImmutableRecordRef, IndexInfo, Text},
    Buffer, Completion, SharedBufferData, Value, ValueRef,
};

use super::{
    build_encrypted_chunk_aad, derive_initial_crc, encrypted_chunk_blob_size,
    encrypted_chunk_plaintext_len, encrypted_payload_blob_size, encrypted_payload_chunk_count,
    HeaderReadResult, LogHeader, LogSerializer, LogTxFrameInfo, LogicalLog, ParseResult, ParsedOp,
    StreamingLogicalLogReader, ENCRYPTED_CHUNK_AAD_SIZE, ENCRYPTED_PAYLOAD_CHUNK_SIZE, END_MAGIC,
    EXT_FRAME_MAGIC, FRAME_MAGIC, LOG_HDR_CRC_START, LOG_HDR_RESERVED_START, LOG_HDR_SIZE,
    LOG_VERSION, LOG_VERSION_V2, TX_EXT_HEADER_SIZE, TX_HEADER_SIZE, TX_HEADER_SIZE_V2,
    TX_TRAILER_SIZE,
};
#[cfg(feature = "conn_raw_api")]
use super::{EXTENSION_RECORD_HEADER_SIZE, EXTENSION_TYPE_PORTABLE_CHANGES, OP_UPSERT_TABLE};
use crate::OpenFlags;
use crate::{turso_assert, turso_assert_less_than};
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();
    });
}

fn write_single_table_tx(
    io: &Arc<dyn crate::IO>,
    file_name: &str,
    commit_ts: u64,
) -> (Arc<dyn crate::File>, usize) {
    let file = io.open_file(file_name, OpenFlags::Create, false).unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let mut tx =
        crate::mvcc::database::LogRecord::new(commit_ts, crate::alloc::DynAllocator::default())
            .unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "foo");
    let version = crate::mvcc::database::RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: row.clone(),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    tx.push_row_version_for_test(&version);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let rowid_len = varint_len(1);
    let payload_len = rowid_len + row.payload().len();
    let payload_len_len = varint_len(payload_len as u64);
    let op_size = 6 + payload_len_len + payload_len;
    (file, op_size)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExpectedTableOp {
    Upsert {
        rowid: i64,
        payload: crate::ValueBlob,
        commit_ts: u64,
        btree_resident: bool,
    },
    Delete {
        rowid: i64,
        commit_ts: u64,
        btree_resident: bool,
    },
}

fn read_table_ops(file: Arc<dyn crate::File>, io: &Arc<dyn crate::IO>) -> Vec<ExpectedTableOp> {
    let mut reader = StreamingLogicalLogReader::new(file, None);
    reader.read_header(io).unwrap();
    let mut ops = Vec::new();
    while let Some(frame) = reader.next_frame_blocking(io).unwrap() {
        for op in frame {
            match op {
                ParsedOp::UpsertTable {
                    rowid,
                    record_bytes,
                    commit_ts,
                    btree_resident,
                    ..
                } => {
                    ops.push(ExpectedTableOp::Upsert {
                        rowid: rowid.row_id.to_int_or_panic(),
                        payload: record_bytes,
                        commit_ts,
                        btree_resident,
                    });
                }
                ParsedOp::DeleteTable {
                    rowid,
                    commit_ts,
                    btree_resident,
                    ..
                } => {
                    ops.push(ExpectedTableOp::Delete {
                        rowid: rowid.row_id.to_int_or_panic(),
                        commit_ts,
                        btree_resident,
                    });
                }
                other => panic!("unexpected op: {other:?}"),
            }
        }
    }
    ops
}

fn read_file_range_bytes(
    file: &Arc<dyn crate::File>,
    io: &Arc<dyn crate::IO>,
    pos: u64,
    len: usize,
) -> Vec<u8> {
    let buf = Arc::new(Buffer::new_temporary(len));
    let c = file
        .pread(pos, Completion::new_read(buf.clone(), |_| None))
        .unwrap();
    io.wait_for_completion(c).unwrap();
    buf.as_slice().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn append_single_table_op_tx(
    log: &mut LogicalLog,
    io: &Arc<dyn crate::IO>,
    table_id: crate::mvcc::database::MVTableId,
    rowid: i64,
    commit_ts: u64,
    is_delete: bool,
    btree_resident: bool,
    payload_text: &str,
) {
    let row = generate_simple_string_row(table_id, rowid, payload_text);
    let row_version = crate::mvcc::database::RowVersion {
        id: commit_ts,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
        )),
        end: crate::mvcc::database::PackedTs::pack(if is_delete {
            Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
        } else {
            None
        }),
        row,
        btree_resident,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let tx = crate::mvcc::database::LogRecord::for_test(commit_ts, &[row_version], None);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();
}

fn decode_streaming_varint(bytes: &[u8]) -> crate::Result<Option<(u64, [u8; 9], usize)>> {
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("logical_log_varint_decode_tmp", OpenFlags::Create, false)
        .unwrap();
    let mut reader = StreamingLogicalLogReader::new(file, None);
    reader.buffer.write().extend_from_slice(bytes);
    io.block(|| reader.consume_varint_bytes())
}

/// A test `File` that DEFERS every pread and SHORT-READS at most `max_read`
/// bytes per call, so the streaming reader actually yields mid-frame (and is
/// re-entered) and a single op spans many reads. Owns the full log bytes;
/// reads complete only via an explicit `step()`, letting the test drive the
/// reader's state machine one IO at a time and observe peak buffer usage.
/// `MemoryIO` completes preads synchronously and fully, so it cannot exercise
/// the mid-frame yield/resume paths — this can.
struct SlowReadFile {
    data: Vec<u8>,
    max_read: usize,
    pending: std::sync::Mutex<std::collections::VecDeque<(u64, Completion)>>,
}

impl SlowReadFile {
    fn new(data: Vec<u8>, max_read: usize) -> Self {
        Self {
            data,
            max_read,
            pending: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Complete the oldest pending pread with a short read. Returns false if
    /// nothing is pending (a stall — the reader expected more IO).
    fn step(&self) -> bool {
        let Some((pos, c)) = self.pending.lock().unwrap().pop_front() else {
            return false;
        };
        let pos = pos as usize;
        let read = c.as_read();
        let cap = read.buf().len();
        let avail = self.data.len().saturating_sub(pos);
        let n = cap.min(self.max_read).min(avail);
        if n > 0 {
            read.buf().as_mut_slice()[..n].copy_from_slice(&self.data[pos..pos + n]);
        }
        c.complete(n as i32);
        true
    }
}

impl crate::File for SlowReadFile {
    fn lock_file(&self, _exclusive: bool) -> crate::Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> crate::Result<()> {
        Ok(())
    }
    fn pread(&self, pos: u64, c: Completion) -> crate::Result<Completion> {
        self.pending.lock().unwrap().push_back((pos, c.clone()));
        Ok(c)
    }
    fn pwrite(&self, _pos: u64, _buffer: Arc<Buffer>, _c: Completion) -> crate::Result<Completion> {
        unimplemented!("SlowReadFile is read-only")
    }
    fn sync(
        &self,
        _c: Completion,
        _sync_type: crate::io::FileSyncType,
    ) -> crate::Result<Completion> {
        unimplemented!("SlowReadFile is read-only")
    }
    fn size(&self) -> crate::Result<u64> {
        Ok(self.data.len() as u64)
    }
    fn truncate(&self, _len: u64, _c: Completion) -> crate::Result<Completion> {
        unimplemented!("SlowReadFile is read-only")
    }
}

/// Read an entire (synchronous) file into a `Vec`.
fn read_file_to_vec(file: &Arc<dyn crate::File>) -> Vec<u8> {
    let size = file.size().unwrap() as usize;
    let out = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = out.clone();
    let buf = Arc::new(Buffer::new_temporary(size));
    let c = Completion::new_read(buf, move |res| {
        if let Ok((b, n)) = res {
            sink.lock()
                .unwrap()
                .extend_from_slice(&b.as_slice()[..n as usize]);
        }
        None
    });
    let _completion = file.pread(0, c).unwrap();
    let bytes = out.lock().unwrap().clone();
    assert_eq!(bytes.len(), size, "expected a synchronous full read");
    bytes
}

fn table_row_version(
    table_id: MVTableId,
    rowid: i64,
    commit_ts: u64,
    data: &str,
) -> crate::mvcc::database::RowVersion {
    crate::mvcc::database::RowVersion {
        id: commit_ts,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: generate_simple_string_row(table_id, rowid, data),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

/// Recover all ops through `SlowReadFile`, driving the reader one deferred IO
/// at a time. Returns the recovered ops and the peak `buffer` length observed
/// across the whole recovery.
fn recover_with_forced_yields(data: Vec<u8>, max_read: usize) -> (Vec<ParsedOp>, usize) {
    recover_with_forced_yields_inner(data, max_read, None)
}

fn recover_with_forced_yields_inner(
    data: Vec<u8>,
    max_read: usize,
    encryption: Option<(crate::storage::encryption::EncryptionContext, usize)>,
) -> (Vec<ParsedOp>, usize) {
    let slow = Arc::new(SlowReadFile::new(data, max_read));
    let file: Arc<dyn crate::File> = slow.clone();
    let mut reader = match encryption {
        Some((ctx, chunk_size)) => {
            StreamingLogicalLogReader::new_with_payload_chunk_size(file, Some(ctx), chunk_size)
        }
        None => StreamingLogicalLogReader::new(file, None),
    };
    let mut peak = 0usize;

    // Header (read in one shot; max_read must exceed LOG_HDR_SIZE).
    loop {
        match reader.try_read_header_nonblock().unwrap() {
            IOResult::Done(HeaderReadResult::Valid(_)) => break,
            IOResult::Done(other) => panic!("unexpected header result: {other:?}"),
            IOResult::IO(_) => assert!(slow.step(), "stalled reading header"),
        }
        peak = peak.max(reader.buffer.read().len());
    }

    // Frames.
    let mut ops = Vec::new();
    loop {
        let result = reader.next_frame().unwrap();
        peak = peak.max(reader.buffer.read().len());
        match result {
            IOResult::Done(Some(frame_ops)) => ops.extend(frame_ops),
            IOResult::Done(None) => break,
            IOResult::IO(_) => {
                assert!(slow.step(), "stalled reading frame");
                peak = peak.max(reader.buffer.read().len());
            }
        }
    }
    (ops, peak)
}

/// What this test checks: streaming recovery is correctly re-entrant when IO
/// yields at every read boundary, and the read buffer compacts per-op so a
/// large multi-op frame is not forced wholesale into memory.
/// Why this matters: recovery runs on a cooperative event loop, so a frame
/// must resume identically across yields; and a huge transaction must not
/// blow up memory during replay (the buffer is bounded to ~one op, not the
/// whole frame).
#[test]
fn test_logical_log_streaming_recovery_forced_yields_bounded_memory() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("logical_log_forced_yields", OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let table_id: MVTableId = (-100).into();

    // A small frame, then one large frame with many sizable ops, then a
    // multi-op small frame. The large frame is what would dominate memory if
    // the whole frame had to stay buffered for re-parse.
    let big_payload = "x".repeat(3000);
    let frames: Vec<Vec<crate::mvcc::database::RowVersion>> = vec![
        vec![table_row_version(table_id, 1, 10, "first")],
        (0..40)
            .map(|i| table_row_version(table_id, 100 + i as i64, 20, &big_payload))
            .collect(),
        vec![
            table_row_version(table_id, 2, 30, "a"),
            table_row_version(table_id, 3, 30, "b"),
        ],
    ];
    for (idx, rows) in frames.iter().enumerate() {
        let commit_ts = (idx as u64 + 1) * 10;
        let tx = crate::mvcc::database::LogRecord::for_test(commit_ts, rows, None);
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    // Expected ops via the straightforward synchronous (full-read) path.
    let mut expected_reader = StreamingLogicalLogReader::new(file.clone(), None);
    expected_reader.read_header(&io).unwrap();
    let mut expected = Vec::new();
    while let Some(frame) = expected_reader.next_frame_blocking(&io).unwrap() {
        expected.extend(frame);
    }
    assert_eq!(expected.len(), 1 + 40 + 2);

    // Recover the same bytes with deferred, short (64-byte) reads so every
    // `try_consume_*` yields and a single op spans dozens of reads.
    let bytes = read_file_to_vec(&file);
    let (recovered, peak) = recover_with_forced_yields(bytes, 64);

    assert_eq!(
        recovered, expected,
        "forced-yield recovery must match the synchronous path exactly"
    );

    // The large frame is ~40 * ~3KB ≈ 120KB. With per-op checkpointing the
    // buffer holds at most ~one op plus a read chunk; the whole-frame rewind
    // model would keep the entire frame resident. Assert a bound far below
    // the frame size.
    assert!(
        peak < 16 * 1024,
        "peak buffer {peak} bytes should be bounded to ~one op, not the whole frame"
    );
}

/// What this test checks: encrypted recovery is correctly re-entrant when IO
/// yields at every read boundary. The encrypted payload is parsed wholesale
/// (rewind-and-rebuild from the payload start), so this exercises that the
/// post-header checkpoint + payload-start resume decrypts identically across
/// yields. (Memory is intentionally not bounded for encrypted frames — the
/// plaintext is accumulated contiguously by design — so only correctness is
/// asserted here.)
/// Why this matters: the refactor changed the encrypted resume point from the
/// frame start to the payload start; this guards that change under yields,
/// which `MemoryIO`-based tests cannot reach.
#[test]
fn test_encrypted_log_recovery_forced_yields() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();
    const TEST_CHUNK_SIZE: usize = 2 * 1024;

    // Two transactions; the first has a multi-op payload large enough to span
    // several encrypted chunks, the second is small.
    let big = "z".repeat(2500);
    let tx0 = crate::mvcc::database::LogRecord::for_test(
        100,
        &[
            make_test_row_version((-2).into(), 1, &big, 100),
            make_test_row_version((-2).into(), 2, &big, 100),
            make_test_row_version((-2).into(), 3, "small", 100),
        ],
        None,
    );
    let tx1 = crate::mvcc::database::LogRecord::for_test(
        200,
        &[make_test_row_version((-2).into(), 4, "tail", 200)],
        None,
    );
    let file = write_encrypted_txs_with_chunk_size_for_test(
        &io,
        "enc-forced-yields.db-log",
        &enc_ctx,
        TEST_CHUNK_SIZE,
        vec![tx0, tx1],
    );

    let expected: Vec<ParsedOp> = parse_all_encrypted_tx_ops_with_chunk_size_for_test(
        file.clone(),
        &io,
        &enc_ctx,
        TEST_CHUNK_SIZE,
    )
    .unwrap()
    .into_iter()
    .flatten()
    .collect();
    assert_eq!(expected.len(), 4);

    let bytes = read_file_to_vec(&file);
    let (recovered, _peak) =
        recover_with_forced_yields_inner(bytes, 64, Some((enc_ctx.clone(), TEST_CHUNK_SIZE)));

    assert_eq!(
        recovered, expected,
        "encrypted forced-yield recovery must match the synchronous path exactly"
    );
}

/// What this test checks: A committed transaction written to the logical log is replayed correctly after restart.
/// Why this matters: This is the baseline durability/recovery guarantee for MVCC commits.
#[test]
fn test_logical_log_read() {
    init_tracing();
    // Load a transaction
    // let's not drop db as we don't want files to be removed
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        let pager = conn.pager.load().clone();
        let mvcc_store = db.get_mvcc_store();
        let table_id: MVTableId = (-100).into();
        let tx_id = mvcc_store.begin_tx(pager).unwrap();
        // insert table id -2 into sqlite_schema table (table_id -1)
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")),  // type
                Value::Text(Text::new("test")),   // name
                Value::Text(Text::new("test")),   // tbl_name
                Value::from_i64(table_id.into()), // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
            ],
            5,
        )
        .unwrap();
        mvcc_store
            .insert(
                tx_id,
                Row::new_table_row(
                    RowID::new((-1).into(), RowKey::Int(1000)),
                    data.as_blob(),
                    5,
                )
                .unwrap(),
            )
            .unwrap();
        // now insert a row into table -2
        let row = generate_simple_string_row(table_id, 1, "foo");
        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store, &conn, tx_id).unwrap();
    }

    // Restart the database to trigger recovery
    db.restart();

    // Now try to read it back - recovery happens automatically during bootstrap
    let conn = db.connect();
    let pager = conn.pager.load().clone();
    let mvcc_store = db.get_mvcc_store();
    let tx = mvcc_store.begin_tx(pager).unwrap();
    let row = mvcc_store
        .read(tx, &RowID::new((-100).into(), RowKey::Int(1)))
        .unwrap()
        .unwrap();
    let record = ImmutableRecordRef::from_bin_record(row.payload());
    let foo = record.iter().unwrap().next().unwrap().unwrap();
    let ValueRef::Text(foo) = foo else {
        unreachable!()
    };
    assert_eq!(foo.as_str(), "foo");
}

/// What this test checks: A long sequence of committed frames is replayed in order without dropping or reordering transactions.
/// Why this matters: Recovery must preserve commit order to maintain MVCC visibility semantics.
#[test]
fn test_logical_log_read_multiple_transactions() {
    init_tracing();
    let table_id: MVTableId = (-100).into();
    let values = (0..100)
        .map(|i| {
            (
                RowID::new(table_id, RowKey::Int(i as i64)),
                format!("foo_{i}"),
            )
        })
        .collect::<Vec<(RowID, String)>>();
    // let's not drop db as we don't want files to be removed
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        let pager = conn.pager.load().clone();
        let mvcc_store = db.get_mvcc_store();

        let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
        // insert table id -2 into sqlite_schema table (table_id -1)
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")),  // type
                Value::Text(Text::new("test")),   // name
                Value::Text(Text::new("test")),   // tbl_name
                Value::from_i64(table_id.into()), // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
            ],
            5,
        )
        .unwrap();
        mvcc_store
            .insert(
                tx_id,
                Row::new_table_row(
                    RowID::new((-1).into(), RowKey::Int(1000)),
                    data.as_blob(),
                    5,
                )
                .unwrap(),
            )
            .unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        // now insert a row into table -2
        // generate insert per transaction
        for (rowid, value) in &values {
            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
            let row =
                generate_simple_string_row(rowid.table_id, rowid.row_id.to_int_or_panic(), value);
            mvcc_store.insert(tx_id, row).unwrap();
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        }
    }

    // Restart the database to trigger recovery
    db.restart();

    // Now try to read it back - recovery happens automatically during bootstrap
    let conn = db.connect();
    let pager = conn.pager.load().clone();
    let mvcc_store = db.get_mvcc_store();
    for (rowid, value) in &values {
        let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
        let row = mvcc_store.read(tx, rowid).unwrap().unwrap();
        let record = ImmutableRecordRef::from_bin_record(row.payload());
        let foo = record.iter().unwrap().next().unwrap().unwrap();
        let ValueRef::Text(foo) = foo else {
            unreachable!()
        };
        assert_eq!(foo.as_str(), value.as_str());
    }
}

/// What this test checks: Randomized insert/delete workloads round-trip through write + restart replay with matching final contents.
/// Why this matters: Fuzz-style coverage catches edge combinations that hand-written examples miss.
#[test]
fn test_logical_log_read_fuzz() {
    init_tracing();
    let table_id: MVTableId = (-100).into();
    let seed = rng().random();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let num_transactions = rng.next_u64() % 128;
    let mut txns = vec![];
    let mut present_rowids = BTreeSet::new();
    let mut non_present_rowids = BTreeSet::new();
    for _ in 0..num_transactions {
        let num_operations = rng.next_u64() % 8;
        let mut ops = vec![];
        for _ in 0..num_operations {
            let op_type = rng.next_u64() % 2;
            match op_type {
                0 => {
                    // Generate a positive rowid that fits in i64
                    let row_id = (rng.next_u64() % (i64::MAX as u64)) as i64;
                    let rowid = RowID::new(table_id, RowKey::Int(row_id));
                    let row = generate_simple_string_row(
                        rowid.table_id,
                        rowid.row_id.to_int_or_panic(),
                        &format!("row_{row_id}"),
                    );
                    ops.push((true, Some(row), rowid.clone()));
                    present_rowids.insert(rowid.clone());
                    non_present_rowids.remove(&rowid);
                    tracing::debug!("insert {rowid:?}");
                }
                1 => {
                    if present_rowids.is_empty() {
                        continue;
                    }
                    let row_id_pos = rng.next_u64() as usize % present_rowids.len();
                    let row_id = present_rowids.iter().nth(row_id_pos).unwrap().clone();
                    ops.push((false, None, row_id.clone()));
                    present_rowids.remove(&row_id);
                    non_present_rowids.insert(row_id.clone());
                    tracing::debug!("removed {row_id:?}");
                }
                _ => unreachable!(),
            }
        }
        txns.push(ops);
    }
    // let's not drop db as we don't want files to be removed
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let pager = {
        let conn = db.connect();
        let pager = conn.pager.load().clone();
        let mvcc_store = db.get_mvcc_store();

        // insert table id -2 into sqlite_schema table (table_id -1)
        let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")),  // type
                Value::Text(Text::new("test")),   // name
                Value::Text(Text::new("test")),   // tbl_name
                Value::from_i64(table_id.into()), // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
            ],
            5,
        )
        .unwrap();
        mvcc_store
            .insert(
                tx_id,
                Row::new_table_row(
                    RowID::new((-1).into(), RowKey::Int(1000)),
                    data.as_blob(),
                    5,
                )
                .unwrap(),
            )
            .unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

        // insert rows
        for ops in &txns {
            let tx_id = mvcc_store.begin_tx(pager.clone()).unwrap();
            for (is_insert, maybe_row, rowid) in ops {
                if *is_insert {
                    mvcc_store
                        .insert(tx_id, maybe_row.as_ref().unwrap().clone())
                        .unwrap();
                } else {
                    mvcc_store.delete(tx_id, rowid.clone()).unwrap();
                }
            }
            commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        }

        conn.close().unwrap();
        pager
    };

    db.restart();

    // connect after restart should recover log.
    let _conn = db.connect();
    let mvcc_store = db.get_mvcc_store();

    // Check rowids that weren't deleted
    let tx = mvcc_store.begin_tx(pager.clone()).unwrap();
    for present_rowid in present_rowids {
        let row = mvcc_store.read(tx, &present_rowid).unwrap().unwrap();
        let record = ImmutableRecordRef::from_bin_record(row.payload());
        let foo = record.iter().unwrap().next().unwrap().unwrap();
        let ValueRef::Text(foo) = foo else {
            unreachable!()
        };

        assert_eq!(
            foo.as_str(),
            format!("row_{}", present_rowid.row_id.to_int_or_panic())
        );
    }

    // Check rowids that were deleted
    let tx = mvcc_store.begin_tx(pager).unwrap();
    for present_rowid in non_present_rowids {
        let row = mvcc_store.read(tx, &present_rowid).unwrap();
        assert!(
            row.is_none(),
            "row {present_rowid:?} should have been removed"
        );
    }
}

/// What this test checks: Recovery rebuilds both table rows and index rows from logical-log operations.
/// Why this matters: Table/index divergence after restart would break query correctness.
#[test]
fn test_logical_log_read_table_and_index_rows() {
    init_tracing();
    // Test that both table rows and index rows can be read back after recovery
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();

        // Create a table with an index
        conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_data ON test(data)").unwrap();

        // Checkpoint to ensure the index has a root_page mapping
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Insert some data - this will create both table rows and index rows in the logical log
        // Don't checkpoint after inserts so they remain in the logical log for recovery testing
        conn.execute("INSERT INTO test(id, data) VALUES (1, 'foo')")
            .unwrap();
        conn.execute("INSERT INTO test(id, data) VALUES (2, 'bar')")
            .unwrap();
        conn.execute("INSERT INTO test(id, data) VALUES (3, 'baz')")
            .unwrap();
    }

    // Restart the database to trigger recovery
    db.restart();

    // Now verify that both table rows and index rows can be read back
    let conn = db.connect();
    let pager = conn.pager.load().clone();
    let mvcc_store = db.get_mvcc_store();
    let schema = conn.schema.read();
    let table = schema.get_table("test").expect("table test should exist");
    let Table::BTree(table) = table.as_ref() else {
        panic!("table test should be btree");
    };
    let table_id = mvcc_store.get_table_id_from_root_page(table.root_page);

    // Get the index from schema
    let index = schema
        .get_index("test", "idx_data")
        .expect("Index should exist");
    // Use get_table_id_from_root_page to get the correct index_id (handles both checkpointed and non-checkpointed)
    let index_id = mvcc_store.get_table_id_from_root_page(index.root_page);
    let index_info = Arc::new(IndexInfo::new_from_index(index).unwrap());

    // Verify table rows can be read
    let tx = mvcc_store.begin_tx(pager).unwrap();
    for (row_id, expected_data) in [(1, "foo"), (2, "bar"), (3, "baz")] {
        let row = mvcc_store
            .read(tx, &RowID::new(table_id, RowKey::Int(row_id)))
            .unwrap()
            .expect("Table row should exist");
        let record = ImmutableRecordRef::from_bin_record(row.payload());
        let values = record.get_values().unwrap();
        let data_value = values.get(1).expect("Should have data column");
        let ValueRef::Text(data_text) = data_value else {
            panic!("Data column should be text");
        };
        assert_eq!(data_text.as_str(), expected_data);
    }

    // Verify index rows can be read
    // Note: Index rows are written to the logical log, but we need to construct the correct key format
    // The index key format is (indexed_column_value, table_rowid)
    for (row_id, data_value) in [(1, "foo"), (2, "bar"), (3, "baz")] {
        // Create the index key: (data_value, rowid)
        // The index on data column stores (data_value, table_rowid) as the key
        let key_record = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(data_value.to_string())),
                Value::from_i64(row_id),
            ],
            2,
        )
        .unwrap();
        let sortable_key = SortableIndexKey::new_from_payload_in(
            &key_record,
            index_info.clone(),
            crate::alloc::TursoAllocator,
        )
        .unwrap();
        let index_rowid = RowID::new(index_id, RowKey::Record(Arc::new(sortable_key)));

        // Use read_from_table_or_index to read the index row
        // This verifies that index rows were properly serialized and deserialized from the logical log
        let index_row_opt = mvcc_store
            .read_from_table_or_index(tx, &index_rowid, Some(index_id))
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to read index row for ({}, {}): {:?}. Index ID: {:?}, root_page: {}",
                    data_value, row_id, e, index_id, index.root_page
                )
            });

        let Some(index_row) = index_row_opt else {
            panic!(
                "Index row for ({data_value}, {row_id}) not found after recovery. Index rows should be in the logical log."
            );
        };
        // Verify the index row contains the correct data
        let RowKey::Record(sortable_key) = index_row.id.row_id else {
            panic!("Index row should have a record row_id");
        };
        let record = sortable_key.key.clone();
        let values = record.get_values().unwrap();
        assert_eq!(
            values.len(),
            2,
            "Index row should have 2 columns (data, rowid)"
        );
        let ValueRef::Text(index_data) = values[0] else {
            panic!("First index column should be text");
        };
        assert_eq!(index_data.as_str(), data_value, "Index data should match");
        let ValueRef::Numeric(crate::numeric::Numeric::Integer(index_rowid_val)) = values[1] else {
            panic!("Second index column should be integer (rowid)");
        };
        assert_eq!(index_rowid_val, row_id, "Index rowid should match");
    }
}

/// What this test checks: If the last frame is torn, recovery keeps the valid prefix and ignores only the incomplete tail.
/// Why this matters: Crashes commonly leave partial EOF writes; we need safe prefix recovery instead of full failure.
#[test]
fn test_logical_log_torn_tail_stops_cleanly() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("test.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let row = generate_simple_string_row((-2).into(), 1, "foo");
    let rowid_len = varint_len(1);
    let payload_len = rowid_len + row.payload().len();
    let payload_len_len = varint_len(payload_len as u64);
    let op_size = 6 + payload_len_len + payload_len;
    let frame_size = TX_HEADER_SIZE + op_size + TX_TRAILER_SIZE;

    let mut tx1 =
        crate::mvcc::database::LogRecord::new(10, crate::alloc::DynAllocator::default()).unwrap();
    tx1.push_row_version_for_test(&crate::mvcc::database::RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(10),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: row.clone(),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    });
    let c = log.log_tx(tx1).unwrap();
    io.wait_for_completion(c).unwrap();

    let mut tx2 =
        crate::mvcc::database::LogRecord::new(20, crate::alloc::DynAllocator::default()).unwrap();
    tx2.push_row_version_for_test(&crate::mvcc::database::RowVersion {
        id: 2,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(20),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    });
    let c = log.log_tx(tx2).unwrap();
    io.wait_for_completion(c).unwrap();

    let file_size = file.size().unwrap() as usize;
    let last_frame_start = LOG_HDR_SIZE + frame_size;

    // Truncate the file at every offset within the last frame.
    for cut in (last_frame_start..file_size).rev() {
        let c = file
            .truncate(cut as u64, Completion::new_trunc(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
        reader.read_header(&io).unwrap();
        let mut seen = 0;
        loop {
            match reader.next_frame_blocking(&io) {
                Ok(Some(frame)) => {
                    for op in frame {
                        match op {
                            ParsedOp::UpsertTable { .. } => seen += 1,
                            other => panic!("unexpected op: {other:?}"),
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => panic!("unexpected error: {err:?}"),
            }
        }
        assert_eq!(seen, 1, "should apply only the first transaction");
    }
}

/// What this test checks: With many frames, a torn tail still preserves all earlier complete frames.
/// Why this matters: Durable commits before the crash boundary must survive regardless of tail damage.
#[test]
fn test_logical_log_torn_tail_multiple_frames_stops_cleanly() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "logical_log_torn_tail_multi_frame",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 1, false, false, "a");
    append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 2, false, false, "b");
    let after_tx2 = log.offset as usize;
    append_single_table_op_tx(&mut log, &io, (-2).into(), 3, 3, false, false, "c");
    let after_tx3 = log.offset as usize;

    let partial_tail_len = (after_tx3 - after_tx2) / 2;
    let trunc_offset = (after_tx2 + partial_tail_len) as u64;
    let c = file
        .truncate(trunc_offset, Completion::new_trunc(|_| {}))
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let read_back = read_table_ops(file.clone(), &io);
    assert_eq!(read_back.len(), 2);
    assert_eq!(
        read_back[0],
        ExpectedTableOp::Upsert {
            rowid: 1,
            payload: crate::types::value_blob_from_slice(
                generate_simple_string_row((-2).into(), 1, "a").payload(),
            )
            .expect(crate::alloc::ALLOC_ERR_MSG),
            commit_ts: 1,
            btree_resident: false,
        }
    );
    assert_eq!(
        read_back[1],
        ExpectedTableOp::Upsert {
            rowid: 2,
            payload: crate::types::value_blob_from_slice(
                generate_simple_string_row((-2).into(), 2, "b").payload(),
            )
            .expect(crate::alloc::ALLOC_ERR_MSG),
            commit_ts: 2,
            btree_resident: false,
        }
    );
}

/// What this test checks: The parser accepts the full valid negative table-id range, including i32::MIN.
/// Why this matters: Edge ID handling must be stable to avoid replay panics/corruption on valid inputs.
#[test]
fn test_logical_log_read_i32_min_table_id() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("logical_log_i32_min_table_id", OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);
    let table_id = crate::mvcc::database::MVTableId::from(i32::MIN as i64);

    append_single_table_op_tx(&mut log, &io, table_id, 7, 11, false, false, "min");

    let mut reader = StreamingLogicalLogReader::new(file, None);
    reader.read_header(&io).unwrap();
    let frame = reader
        .next_frame_blocking(&io)
        .unwrap()
        .expect("expected one frame");
    assert_eq!(frame.len(), 1);
    match &frame[0] {
        ParsedOp::UpsertTable { rowid, .. } => {
            assert_eq!(rowid.table_id, table_id);
            assert_eq!(rowid.row_id.to_int_or_panic(), 7);
        }
        other => panic!("unexpected op: {other:?}"),
    }
}

/// What this test checks: Rowid varint encoding/decoding is consistent for negative i64-style
/// values, and the deferred-offset write path (log_tx_deferred_offset) does not advance the
/// writer offset until advance_offset_after_success is called, after which all frames are
/// readable with a valid CRC chain.
/// Why this matters: Rowid decoding mismatches would replay to the wrong keys.
///   The MVCC commit path uses deferred writes so an aborted commit can be silently overwritten;
///   the offset must not advance before confirmation.
#[test]
fn test_logical_log_rowid_negative_varint_roundtrip() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "logical_log_negative_rowid_roundtrip",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    append_single_table_op_tx(&mut log, &io, (-2).into(), -1, 1, false, false, "neg");
    append_single_table_op_tx(&mut log, &io, (-2).into(), -1, 2, true, false, "neg");
    let offset_after_frame2 = log.offset;

    // Frame 3: deferred path — offset must not advance until confirmed.
    let row3 = generate_simple_string_row((-2).into(), 3, "deferred");
    let tx3 = crate::mvcc::database::LogRecord::for_test(
        3,
        &[crate::mvcc::database::RowVersion {
            id: 3,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(3),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row: row3,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }],
        None,
    );
    let (c, bytes_written) = log.log_tx_deferred_offset(tx3, None).unwrap();
    io.wait_for_completion(c).unwrap();

    assert_eq!(
        log.offset, offset_after_frame2,
        "deferred write must not advance offset before advance_offset_after_success"
    );
    log.advance_offset_after_success(bytes_written);
    assert_eq!(
        log.offset,
        offset_after_frame2 + bytes_written,
        "offset must advance by exactly bytes_written after confirmation"
    );

    let read_back = read_table_ops(file, &io);
    assert_eq!(read_back.len(), 3);
    match &read_back[0] {
        ExpectedTableOp::Upsert { rowid, .. } => assert_eq!(*rowid, -1),
        other => panic!("unexpected op: {other:?}"),
    }
    match &read_back[1] {
        ExpectedTableOp::Delete { rowid, .. } => assert_eq!(*rowid, -1),
        other => panic!("unexpected op: {other:?}"),
    }
    match &read_back[2] {
        ExpectedTableOp::Upsert { rowid, .. } => assert_eq!(*rowid, 3),
        other => panic!("unexpected op: {other:?}"),
    }
}

#[test]
fn test_on_serialization_complete_gets_shared_write_bytes() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "serialization-callback-shared.db-log",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);
    let captured = RefCell::new(Vec::<(SharedBufferData, LogTxFrameInfo)>::new());
    let callback = |bytes: SharedBufferData, info: LogTxFrameInfo| {
        captured.borrow_mut().push((bytes, info));
        Ok(())
    };

    let tx1 = crate::mvcc::database::LogRecord::for_test(
        1,
        &[crate::mvcc::database::RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(1),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row: generate_simple_string_row((-2).into(), 1, "first"),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }],
        None,
    );
    let (c, first_len) = log.log_tx_deferred_offset(tx1, Some(&callback)).unwrap();
    io.wait_for_completion(c).unwrap();
    log.advance_offset_after_success(first_len);

    let tx2 = crate::mvcc::database::LogRecord::for_test(
        2,
        &[crate::mvcc::database::RowVersion {
            id: 2,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(2),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row: generate_simple_string_row((-2).into(), 2, "second"),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }],
        None,
    );
    let (c, second_len) = log.log_tx_deferred_offset(tx2, Some(&callback)).unwrap();
    io.wait_for_completion(c).unwrap();
    log.advance_offset_after_success(second_len);

    let captured = captured.borrow();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].0.len(), first_len as usize);
    assert_eq!(captured[1].0.len(), second_len as usize);
    assert!(matches!(&captured[0].0, SharedBufferData::Full(_)));
    assert!(matches!(&captured[1].0, SharedBufferData::View(_)));

    let first_on_disk = read_file_range_bytes(&file, &io, 0, first_len as usize);
    let second_on_disk = read_file_range_bytes(&file, &io, first_len, second_len as usize);
    assert_eq!(captured[0].0.as_slice(), first_on_disk.as_slice());
    assert_eq!(captured[1].0.as_slice(), second_on_disk.as_slice());
    assert_eq!(
        captured[0].1.end_crc32c,
        u32::from_le_bytes(
            captured[0].0.as_slice()
                [captured[0].0.len() - TX_TRAILER_SIZE..captured[0].0.len() - TX_TRAILER_SIZE + 4]
                .try_into()
                .unwrap()
        )
    );
    assert_eq!(
        captured[1].1.end_crc32c,
        u32::from_le_bytes(
            captured[1].0.as_slice()
                [captured[1].0.len() - TX_TRAILER_SIZE..captured[1].0.len() - TX_TRAILER_SIZE + 4]
                .try_into()
                .unwrap()
        )
    );

    // Frame chain state: frame 1 starts at offset 0 seeded from the salt,
    // frame 2 starts where frame 1 ended and chains from its end CRC.
    assert_eq!(captured[0].1.logical_start_offset, 0);
    assert_eq!(captured[1].1.logical_start_offset, first_len);
    let salt = log
        .header
        .as_ref()
        .expect("header created on first write")
        .salt;
    assert_eq!(captured[0].1.start_crc32c, derive_initial_crc(salt));
    assert_eq!(captured[1].1.start_crc32c, captured[0].1.end_crc32c);
    assert_eq!(log.running_crc, captured[1].1.end_crc32c);
}

/// What this test checks: A payload bit flip in a fully present tail frame is ignored as invalid tail.
/// Why this matters: Availability-focused recovery keeps the valid prefix even when newest tail bytes are bad.
#[test]
fn test_logical_log_corruption_detected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("corrupt.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let mut tx =
        crate::mvcc::database::LogRecord::new(123, crate::alloc::DynAllocator::default()).unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "foo");
    let version = crate::mvcc::database::RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(123),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    tx.push_row_version_for_test(&version);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    // Flip one byte in the op data (varint payload_len).
    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    // After read_header, reader.offset = LOG_HDR_SIZE.
    // Skip frame header (TX_HEADER_SIZE) + fixed op prefix (tag+flags+table_id = 6 bytes).
    let offset = reader.offset + TX_HEADER_SIZE + 6; // first byte of varint payload_len
    let buf = Arc::new(Buffer::new(vec![0xFF]));
    let c = file
        .pwrite(offset as u64, buf, Completion::new_write(|_| {}))
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let res = reader.next_frame_blocking(&io);
    assert!(res.unwrap().is_none());
}

/// What this test checks: Malformed payload-length varint in newest frame is treated as invalid tail.
/// Why this matters: Recovery must preserve already-validated commits instead of failing hard.
#[test]
fn test_logical_log_payload_len_varint_corrupt_tail_keeps_prefix() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "payload-len-varint-corrupt.db-log",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 1, false, false, "first");
    let frame2_start = log.offset;
    append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 2, false, false, "second");

    // Corrupt frame-2 payload_len varint into an invalid 9-byte varint sequence.
    let payload_len_offset = frame2_start + (TX_HEADER_SIZE + 6) as u64;
    let mut bad_varint = vec![0x80; 8];
    bad_varint.push(0x00);
    let c = file
        .pwrite(
            payload_len_offset,
            Arc::new(Buffer::new(bad_varint)),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let read_back = read_table_ops(file, &io);
    assert_eq!(read_back.len(), 1);
    assert_eq!(
        read_back[0],
        ExpectedTableOp::Upsert {
            rowid: 1,
            payload: crate::types::value_blob_from_slice(
                generate_simple_string_row((-2).into(), 1, "first").payload(),
            )
            .expect(crate::alloc::ALLOC_ERR_MSG),
            commit_ts: 1,
            btree_resident: false,
        }
    );
}

/// What this test checks: Frames with invalid trailer end-magic are treated as invalid tail.
/// Why this matters: End-magic damage in newest bytes should not fail startup.
#[test]
fn test_logical_log_end_magic_corruption() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, op_size) = write_single_table_tx(&io, "end-magic.db-log", 100);
    let trailer_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + op_size;
    // TX trailer layout: [crc32c(4)][END_MAGIC(4)]; END_MAGIC is at offset +4.
    let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
    let c = file
        .pwrite(
            (trailer_offset + 4) as u64,
            bad,
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let res = reader.next_frame_blocking(&io);
    assert!(res.unwrap().is_none());
}

/// What this test checks: Header payload-size mismatch in the newest frame is treated as invalid tail.
/// Why this matters: Prefix-preserving recovery should not hard-fail on newest damaged frame.
#[test]
fn test_logical_log_payload_size_corruption() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, op_size) = write_single_table_tx(&io, "payload-size.db-log", 101);
    // TX header layout: [FRAME_MAGIC(4)][payload_size(8)][op_count(4)][commit_ts(8)]
    // payload_size is at byte 4 of the frame (right after FRAME_MAGIC).
    let bad_payload_size = (op_size as u64 + 1).to_le_bytes().to_vec();
    let bad = Arc::new(Buffer::new(bad_payload_size));
    let c = file
        .pwrite(
            (LOG_HDR_SIZE + 4) as u64,
            bad,
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let res = reader.next_frame_blocking(&io);
    assert!(res.unwrap().is_none());
}

/// What this test checks: Invalid frame-magic at newest frame boundary is treated as invalid tail.
/// Why this matters: Recovery should stop at last valid frame instead of failing startup.
#[test]
fn test_logical_log_frame_magic_corruption() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, _) = write_single_table_tx(&io, "frame-magic.db-log", 103);

    // TX header layout: [FRAME_MAGIC(4)][payload_size(8)][op_count(4)][commit_ts(8)]
    // FRAME_MAGIC is at offset +0 from frame start.
    let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
    let c = file
        .pwrite(LOG_HDR_SIZE as u64, bad, Completion::new_write(|_| {}))
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let res = reader.next_frame_blocking(&io);
    assert!(res.unwrap().is_none());
}

/// What this test checks: Corrupting only the stored CRC field turns newest frame into invalid tail.
/// Why this matters: Prefix must remain replayable under tail checksum damage.
#[test]
fn test_logical_log_crc_field_corruption() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, op_size) = write_single_table_tx(&io, "crc-field.db-log", 104);
    let trailer_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + op_size;
    // TX trailer layout: [crc32c(4)][END_MAGIC(4)]; crc32c is at offset +0.
    let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
    let c = file
        .pwrite(trailer_offset as u64, bad, Completion::new_write(|_| {}))
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let res = reader.next_frame_blocking(&io);
    assert!(res.unwrap().is_none());
}

/// What this test checks: A corrupted newest frame is dropped while older valid frames still replay.
/// Why this matters: Prefix-preserving behavior is required for SQLite-style availability recovery.
#[test]
fn test_logical_log_corrupt_tail_keeps_valid_prefix() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "corrupt-tail-prefix.db-log",
            crate::OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "a");
    let after_first = log.offset as usize;
    append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "b");
    let after_second = log.offset as usize;
    let second_frame_len = after_second - after_first;

    // TX trailer layout: [crc32c(4)][END_MAGIC(4)]; crc32c is at trailer offset +0.
    let second_trailer_crc_offset = after_first + second_frame_len - TX_TRAILER_SIZE;
    let c = file
        .pwrite(
            second_trailer_crc_offset as u64,
            Arc::new(Buffer::new(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let ops = read_table_ops(file, &io);
    assert_eq!(
        ops,
        vec![ExpectedTableOp::Upsert {
            rowid: 1,
            payload: crate::types::value_blob_from_slice(
                generate_simple_string_row((-2).into(), 1, "a").payload(),
            )
            .expect(crate::alloc::ALLOC_ERR_MSG),
            commit_ts: 10,
            btree_resident: false,
        }]
    );
}

/// What this test checks: Corrupted file-header bytes are detected before replay starts.
/// Why this matters: Header trust is foundational for offsets and version checks.
#[test]
fn test_logical_log_header_corruption_detected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("header-corrupt.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);
    let tx = crate::mvcc::database::LogRecord::for_test(77, &[], None);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    // Corrupt magic bytes in the file header.
    let bad = Arc::new(Buffer::new(0u32.to_le_bytes().to_vec()));
    let c = file.pwrite(0, bad, Completion::new_write(|_| {})).unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    let res = reader.read_header(&io);
    assert!(res.is_err());
}

/// What this test checks: Unknown/invalid header flag bits are rejected.
/// Why this matters: Fail-closed flag handling prevents old readers from misinterpreting new format states.
#[test]
fn test_logical_log_header_flags_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, _) = write_single_table_tx(&io, "header-flags.db-log", 105);

    // Header flags byte at offset 5 must not have reserved bits set.
    let c = file
        .pwrite(
            5,
            Arc::new(Buffer::new(vec![0b0000_0010])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    let res = reader.read_header(&io);
    assert!(res.is_err());
}

/// What this test checks: v2 headers must use the fixed 56-byte length and a known version byte.
/// Why this matters: Accepting larger lengths can misalign frame parsing and drop valid commits.
///   Unknown versions must not be silently misread.
#[test]
fn test_logical_log_header_non_default_len_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, _) = write_single_table_tx(&io, "header-len.db-log", 106);

    let header_buf = Arc::new(Buffer::new_temporary(LOG_HDR_SIZE));
    let c = file
        .pread(0, Completion::new_read(header_buf.clone(), |_| None))
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let original_header_bytes = header_buf.as_slice()[..LOG_HDR_SIZE].to_vec();

    // Test 1: non-default header length (LOG_HDR_SIZE + 1) with valid CRC is rejected.
    let mut header_bytes = original_header_bytes.clone();
    header_bytes[6..8].copy_from_slice(&(LOG_HDR_SIZE as u16 + 1).to_le_bytes());
    header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].fill(0);
    let new_crc = crc32c::crc32c(&header_bytes);
    header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&new_crc.to_le_bytes());

    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(header_bytes)),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    let res = reader.read_header(&io);
    assert!(res.is_err());

    // Test 2: unknown version byte (99) with valid CRC is rejected as Invalid.
    let mut header_bytes = original_header_bytes;
    header_bytes[4] = 99; // unknown version
    header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].fill(0);
    let new_crc = crc32c::crc32c(&header_bytes);
    header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&new_crc.to_le_bytes());

    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(header_bytes)),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file, None);
    let result = reader.try_read_header(&io).unwrap();
    assert!(
        matches!(result, HeaderReadResult::Invalid),
        "unknown version header must be rejected as Invalid, got {result:?}"
    );
}

/// What this test checks: Non-zero reserved bytes in the file header are rejected for this format version.
/// Why this matters: Reserved-region discipline preserves forward-compatibility and corruption detection.
#[test]
fn test_logical_log_header_reserved_bytes_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, _) = write_single_table_tx(&io, "header-reserved.db-log", 106);

    // Read existing header bytes so we can corrupt reserved and recompute CRC.
    let header_buf = Arc::new(Buffer::new_temporary(LOG_HDR_SIZE));
    let c = file
        .pread(0, Completion::new_read(header_buf.clone(), |_| None))
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let mut header_bytes = header_buf.as_slice()[..LOG_HDR_SIZE].to_vec();

    // Corrupt reserved region (bytes 16-51). Reserved region starts at offset 16 (after salt at 8-15).
    header_bytes[LOG_HDR_RESERVED_START] = 1;

    // Recompute CRC with CRC field zeroed, then fill in the new CRC.
    header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].fill(0);
    let new_crc = crc32c::crc32c(&header_bytes);
    header_bytes[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&new_crc.to_le_bytes());

    // Write the corrupted header back.
    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(header_bytes)),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    let res = reader.read_header(&io);
    assert!(res.is_err());
}

/// What this test checks: Unknown op reserved-flag bits in newest frame are treated as invalid tail.
/// Why this matters: Prefix frames must remain usable after tail damage.
#[test]
fn test_logical_log_op_reserved_flags_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, _) = write_single_table_tx(&io, "op-flags.db-log", 108);

    // First op flags byte at frame offset: TX header + tag byte.
    let c = file
        .pwrite(
            (LOG_HDR_SIZE + TX_HEADER_SIZE + 1) as u64,
            Arc::new(Buffer::new(vec![0b0000_0010])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let res = reader.next_frame_blocking(&io);
    assert!(res.unwrap().is_none());
}

/// What this test checks: Non-negative table_id in newest frame is treated as invalid tail.
/// Why this matters: Bad tail metadata should not make the entire log unreadable.
#[test]
fn test_logical_log_non_negative_table_id_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let (file, _) = write_single_table_tx(&io, "table-id-sign.db-log", 109);

    // First op table_id starts after tag+flags.
    let c = file
        .pwrite(
            (LOG_HDR_SIZE + TX_HEADER_SIZE + 2) as u64,
            Arc::new(Buffer::new(1i32.to_le_bytes().to_vec())),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let res = reader.next_frame_blocking(&io);
    assert!(res.unwrap().is_none());
}

/// What this test checks: Zero-operation frames are silently skipped by the reader, and a
/// LogRecord carrying a DatabaseHeader round-trips as UpdateHeader with all fields intact.
/// Why this matters: Edge-case frame shapes must remain parseable to keep format handling robust.
///   UPDATE_HEADER is a distinct op type with its own fixed-size payload, zero-flags constraint,
///   zero-table_id constraint, and magic validation — none of which the table/index op tests cover.
#[test]
fn test_logical_log_empty_transaction_frame() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("empty-tx.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    // Frame 1: empty tx (no ops). The reader must skip it silently (ops.is_empty() → continue).
    let tx = crate::mvcc::database::LogRecord::for_test(200, &[], None);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    // Frame 2: header-only tx. DatabaseHeader::default() has the SQLite magic that passes
    // the reader's magic validation check.
    let commit_ts = 201u64;
    let db_header = DatabaseHeader::default();
    let header_tx = crate::mvcc::database::LogRecord::for_test(commit_ts, &[], Some(db_header));
    let c = log.log_tx(header_tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();

    // The reader skips the empty frame and returns the UpdateHeader from frame 2.
    let frame = reader
        .next_frame_blocking(&io)
        .unwrap()
        .expect("expected UpdateHeader frame after empty tx");
    assert_eq!(frame.len(), 1);
    match &frame[0] {
        ParsedOp::UpdateHeader {
            header: recovered,
            commit_ts: recovered_ts,
        } => {
            assert_eq!(*recovered_ts, commit_ts);
            assert_eq!(recovered.magic, db_header.magic);
        }
        other => panic!("expected UpdateHeader, got {other:?}"),
    }

    // Nothing left after frame 2.
    assert!(reader.next_frame_blocking(&io).unwrap().is_none());
}

/// What this test checks: Every single-bit flip in a full frame is either detected or safely rejected.
/// Why this matters: This gives strong confidence that integrity checks catch realistic media faults.
#[test]
fn test_logical_log_bitflip_integrity_exhaustive_single_frame() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("bitflip.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);
    let mut tx =
        crate::mvcc::database::LogRecord::new(300, crate::alloc::DynAllocator::default()).unwrap();
    tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(300),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: generate_simple_string_row((-2).into(), 42, "flip"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    });
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let size = file.size().unwrap() as usize;
    let mut original = vec![0u8; size];
    let read_buf = Arc::new(Buffer::new_temporary(size));
    let c = file
        .pread(0, Completion::new_read(read_buf.clone(), |_| None))
        .unwrap();
    io.wait_for_completion(c).unwrap();
    original.copy_from_slice(&read_buf.as_slice()[..size]);

    for (i, original_byte) in original.iter().enumerate().take(size).skip(LOG_HDR_SIZE) {
        for bit in 0..8u8 {
            let mutated = original_byte ^ (1 << bit);
            let c = file
                .pwrite(
                    i as u64,
                    Arc::new(Buffer::new(vec![mutated])),
                    Completion::new_write(|_| {}),
                )
                .unwrap();
            io.wait_for_completion(c).unwrap();

            let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
            reader.read_header(&io).unwrap();
            let res = reader.next_frame_blocking(&io);
            match res {
                Err(_) | Ok(None) => {}
                Ok(Some(frame)) => {
                    panic!("bit flip at offset={i}, bit={bit} produced valid frame: {frame:?}")
                }
            }

            let c = file
                .pwrite(
                    i as u64,
                    Arc::new(Buffer::new(vec![*original_byte])),
                    Completion::new_write(|_| {}),
                )
                .unwrap();
            io.wait_for_completion(c).unwrap();
        }
    }
}

/// What this test checks: Random table upsert/delete sequences round-trip through serialize + parse.
/// Why this matters: Randomized coverage validates invariants across many payload/order combinations.
#[test]
fn test_logical_log_roundtrip_random_table_ops() {
    init_tracing();
    let seed = 0xA11CE55u64;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("roundtrip-rand.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let mut expected = Vec::new();
    for tx_i in 0..128u64 {
        let mut tx = crate::mvcc::database::LogRecord::new(
            1_000 + tx_i,
            crate::alloc::DynAllocator::default(),
        )
        .unwrap();
        let op_count = (rng.next_u64() % 4) as usize;
        for _ in 0..op_count {
            let rowid = (rng.next_u64() % 64) as i64 + 1;
            let btree_resident = (rng.next_u32() & 1) == 1;
            let is_delete = (rng.next_u32() & 1) == 1;
            if is_delete {
                tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
                    id: 0,
                    begin: crate::mvcc::database::PackedTs::pack(None),
                    end: crate::mvcc::database::PackedTs::pack(Some(
                        crate::mvcc::database::TxTimestampOrID::Timestamp(tx.tx_timestamp),
                    )),
                    row: Row::new_table_row(RowID::new((-2).into(), RowKey::Int(rowid)), &[], 0)
                        .unwrap(),
                    btree_resident,
                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                });
                expected.push(ExpectedTableOp::Delete {
                    rowid,
                    commit_ts: tx.tx_timestamp,
                    btree_resident,
                });
            } else {
                let payload = format!("r-{tx_i}-{rowid}");
                let row = generate_simple_string_row((-2).into(), rowid, &payload);
                tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
                    id: 0,
                    begin: crate::mvcc::database::PackedTs::pack(Some(
                        crate::mvcc::database::TxTimestampOrID::Timestamp(tx.tx_timestamp),
                    )),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row: row.clone(),
                    btree_resident,
                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                });
                expected.push(ExpectedTableOp::Upsert {
                    rowid,
                    payload: crate::types::value_blob_from_slice(row.payload())
                        .expect(crate::alloc::ALLOC_ERR_MSG),
                    commit_ts: tx.tx_timestamp,
                    btree_resident,
                });
            }
        }
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    // Large-payload frame: 30 rows × 200 bytes ≈ 6 KB — well above the 4096-byte internal
    // read-chunk boundary. This verifies the reader stitches together multiple pread results
    // correctly when a single frame spans chunk boundaries.
    let large_commit_ts = 1_000 + 128u64;
    let large_text: String = "x".repeat(200);
    let mut large_tx = crate::mvcc::database::LogRecord::new(
        large_commit_ts,
        crate::alloc::DynAllocator::default(),
    )
    .unwrap();
    for rowid in 1..=30i64 {
        let row = generate_simple_string_row((-3).into(), rowid, &large_text);
        expected.push(ExpectedTableOp::Upsert {
            rowid,
            payload: crate::types::value_blob_from_slice(row.payload())
                .expect(crate::alloc::ALLOC_ERR_MSG),
            commit_ts: large_commit_ts,
            btree_resident: false,
        });
        large_tx.push_row_version_for_test(&crate::mvcc::database::RowVersion {
            id: rowid as u64,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(large_commit_ts),
            )),
            end: crate::mvcc::database::PackedTs::pack(None),
            row,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        });
    }
    let c = log.log_tx(large_tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let got = read_table_ops(file.clone(), &io);
    assert_eq!(got, expected);
}

/// What this property checks: For arbitrary event sequences, write/read round-trip preserves operation intent.
#[quickcheck]
fn prop_logical_log_roundtrip_sequence(events: Vec<(bool, i64, bool)>) -> bool {
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = match io.open_file(
        "logical_log_prop_roundtrip_sequence",
        OpenFlags::Create,
        false,
    ) {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);
    let mut expected = Vec::new();

    for (idx, (is_delete, rowid, btree_resident)) in events.into_iter().take(64).enumerate() {
        let commit_ts = (idx + 1) as u64;
        let payload_text = format!("v{idx}");
        let row = generate_simple_string_row((-2).into(), rowid, &payload_text);
        let row_version = crate::mvcc::database::RowVersion {
            id: commit_ts,
            begin: crate::mvcc::database::PackedTs::pack(Some(
                crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
            )),
            end: crate::mvcc::database::PackedTs::pack(if is_delete {
                Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
            } else {
                None
            }),
            row: row.clone(),
            btree_resident,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        expected.push(if is_delete {
            ExpectedTableOp::Delete {
                rowid,
                commit_ts,
                btree_resident,
            }
        } else {
            ExpectedTableOp::Upsert {
                rowid,
                payload: crate::types::value_blob_from_slice(row.payload())
                    .expect(crate::alloc::ALLOC_ERR_MSG),
                commit_ts,
                btree_resident,
            }
        });
        let tx = crate::mvcc::database::LogRecord::for_test(commit_ts, &[row_version], None);
        let Ok(c) = log.log_tx(tx) else {
            return false;
        };
        if io.wait_for_completion(c).is_err() {
            return false;
        }
    }

    if expected.is_empty() {
        return file.size().expect("file.size() failed") == 0;
    }

    read_table_ops(file, &io) == expected
}

/// What this property checks: Streaming varint decode returns the original value for encoded inputs.
#[quickcheck]
fn prop_streaming_varint_roundtrip(value: u64) -> bool {
    let mut encoded = [0u8; 9];
    let len = write_varint(&mut encoded, value);
    if len == 0 || len > 9 {
        return false;
    }
    let encoded = &encoded[..len];

    let parsed_streaming = match decode_streaming_varint(encoded) {
        Ok(Some(v)) => v,
        _ => return false,
    };
    let parsed_read = match read_varint(encoded) {
        Ok(v) => v,
        Err(_) => return false,
    };

    parsed_streaming.0 == value
        && parsed_streaming.2 == len
        && parsed_streaming.1[..len] == encoded[..]
        && parsed_read.0 == value
        && parsed_read.1 == len
}

/// What this property checks: The streaming varint decoder agrees with the reference decoder on the same bytes.
#[quickcheck]
fn prop_streaming_varint_matches_read_varint(bytes: Vec<u8>) -> bool {
    let bytes = if bytes.len() > 16 {
        &bytes[..16]
    } else {
        bytes.as_slice()
    };
    let streaming = decode_streaming_varint(bytes);
    let plain = read_varint(bytes);

    match (streaming, plain) {
        (Ok(Some((v1, b1, l1))), Ok((v2, l2))) => v1 == v2 && l1 == l2 && b1[..l1] == bytes[..l1],
        (Ok(None), Err(_)) => true, // truncated varint in streaming path
        (Err(_), Err(_)) => true,   // malformed varint in both paths
        _ => false,
    }
}

/// What this test checks: The btree_resident flag survives write/read round-trip unchanged,
/// and the on-disk frame header has the correct binary layout (FRAME_MAGIC at [0..4],
/// payload_size as u64 at [4..12]).
/// Why this matters: This flag affects tombstone and checkpoint behavior after recovery.
///   The frame layout check is baseline confirmation that the serialized format is self-consistent.
#[test]
fn test_logical_log_btree_resident_roundtrip() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("btree.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let mut tx =
        crate::mvcc::database::LogRecord::new(55, crate::alloc::DynAllocator::default()).unwrap();
    let mut row = generate_simple_string_row((-2).into(), 1, "foo");
    row.id.table_id = (-2).into();
    let version = crate::mvcc::database::RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(55),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row,
        btree_resident: true,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    tx.push_row_version_for_test(&version);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    // Verify the on-disk frame header binary layout.
    let frame_hdr_buf = Arc::new(Buffer::new_temporary(TX_HEADER_SIZE));
    let c = file
        .pread(
            LOG_HDR_SIZE as u64,
            Completion::new_read(frame_hdr_buf.clone(), |_| None),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let frame_hdr = frame_hdr_buf.as_slice()[..TX_HEADER_SIZE].to_vec();
    assert_eq!(
        u32::from_le_bytes(frame_hdr[0..4].try_into().unwrap()),
        FRAME_MAGIC,
        "FRAME_MAGIC at bytes [0..4]"
    );
    assert!(
        u64::from_le_bytes(frame_hdr[4..12].try_into().unwrap()) > 0,
        "payload_size at bytes [4..12] must be non-zero for a non-empty op"
    );

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let frame = reader
        .next_frame_blocking(&io)
        .unwrap()
        .expect("expected one frame");
    assert_eq!(frame.len(), 1);
    match &frame[0] {
        ParsedOp::UpsertTable { btree_resident, .. } => {
            assert!(*btree_resident);
        }
        other => panic!("unexpected op: {other:?}"),
    }
}

/// What this test checks: Header rewrites remain durable and parseable across truncate/reopen cycles.
/// Why this matters: Recovery depends on header validity even when the log body is empty.
#[test]
fn test_logical_log_header_persistence() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("header.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let mut tx =
        crate::mvcc::database::LogRecord::new(10, crate::alloc::DynAllocator::default()).unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "foo");
    let version = crate::mvcc::database::RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(10),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    tx.push_row_version_for_test(&version);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let c = file
        .truncate(LOG_HDR_SIZE as u64, Completion::new_trunc(|_| {}))
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(&io).unwrap();
    let header = reader.header().unwrap();
    assert_eq!(header.version, LOG_VERSION_V2);
    // Verify the on-disk CRC matches a fresh computation over the header bytes
    let encoded = header.encode();
    let mut check_buf = [0u8; LOG_HDR_SIZE];
    check_buf.copy_from_slice(&encoded);
    check_buf[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&[0; 4]);
    let expected_crc = crc32c::crc32c(&check_buf);
    assert_eq!(header.hdr_crc32c, expected_crc);
}

/// What this test checks: Header encode/decode with CRC validation round-trips cleanly, including salt.
/// Why this matters: Header integrity verification must be deterministic across writes/restarts.
#[test]
fn test_logical_log_header_crc_roundtrip() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let header = LogHeader::new(&io);
    assert_eq!(header.version, LOG_VERSION_V2);
    assert_ne!(header.salt, 0, "salt should be non-zero from IO RNG");
    let bytes = header.encode();
    // Verify CRC: zero out the CRC field and recompute
    let mut check_buf = bytes;
    check_buf[LOG_HDR_CRC_START..LOG_HDR_SIZE].copy_from_slice(&[0; 4]);
    let expected_crc = crc32c::crc32c(&check_buf);
    let decoded = LogHeader::decode(&bytes).unwrap();
    assert_eq!(decoded.version, header.version);
    assert_eq!(decoded.salt, header.salt);
    assert_eq!(decoded.hdr_crc32c, expected_crc);
}

/// What this test checks: try_read_header classifies malformed headers as Invalid (recoverable path) instead of hard-failing immediately.
/// Why this matters: Bootstrap logic needs this distinction to decide between body-scan fallback and fatal errors.
#[test]
fn test_try_read_header_reports_invalid_not_corrupt() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "try-read-header-invalid.db-log",
            crate::OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 11, false, false, "foo");
    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(vec![0])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file, None);
    let result = reader.try_read_header(&io).unwrap();
    assert!(matches!(result, HeaderReadResult::Invalid));
}

/// What this test checks: Truncation regenerates the salt and old frames can't validate with the new salt.
/// Why this matters: Salt rotation on truncation ensures stale data from a previous log epoch
/// cannot accidentally validate against the new CRC chain.
#[test]
fn test_truncation_regenerates_salt() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("salt-regen.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    // Write a frame and capture the salt
    append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "a");
    let salt_before = log.header.as_ref().unwrap().salt;

    // Truncate to 0 (simulates checkpoint truncation); header with new salt
    // will be written together with the next frame. u64::MAX boundary => all
    // frames are considered checkpointed, so it truncates unconditionally.
    let (c, outcome) = log.truncate(u64::MAX).unwrap();
    assert_eq!(
        outcome,
        crate::mvcc::persistent_storage::LogicalLogTruncateOutcome::Truncated
    );
    io.wait_for_completion(c).unwrap();

    let salt_after = log.header.as_ref().unwrap().salt;
    assert_ne!(salt_before, salt_after, "salt must change on truncation");
    assert_eq!(log.offset, 0, "offset must be 0 after truncation");

    // Write a new frame — this also writes the header with the new salt
    append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "b");

    // Reader should see only the new frame (old data was truncated)
    let mut reader = StreamingLogicalLogReader::new(file, None);
    assert!(matches!(
        reader.try_read_header(&io).unwrap(),
        HeaderReadResult::Valid(_)
    ));
    let header = reader.header().unwrap();
    assert_eq!(header.salt, salt_after);

    match io.block(|| reader.parse_next_transaction()) {
        Ok(ParseResult::Frame(frame)) => {
            let ops = frame.ops;
            assert!(!ops.is_empty(), "expected at least one op");
        }
        Ok(ParseResult::Eof) => panic!("expected ops, got EOF"),
        Ok(ParseResult::InvalidFrame) => panic!("expected ops, got InvalidFrame"),
        Err(e) => panic!("expected ops, got error: {e:?}"),
    }
    assert!(matches!(
        io.block(|| reader.parse_next_transaction()),
        Ok(ParseResult::Eof)
    ));
}

/// Passive truncate must report Retained (and leave salt/offset alone) when commits
/// remain above the checkpoint boundary.
#[test]
fn test_truncate_retained_when_uncheckpointed_frames_remain() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("truncate-retained.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file, io.clone(), None);

    append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "a");
    append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "b");
    let salt_before = log.header.as_ref().unwrap().salt;
    let offset_before = log.offset;

    // Boundary below max_appended_commit_ts (20) => retain the live tail.
    let (c, outcome) = log.truncate(10).unwrap();
    assert_eq!(
        outcome,
        crate::mvcc::persistent_storage::LogicalLogTruncateOutcome::Retained
    );
    io.wait_for_completion(c).unwrap();
    assert_eq!(log.header.as_ref().unwrap().salt, salt_before);
    assert_eq!(log.offset, offset_before);
}

/// What this test checks: Corrupting frame 1 in a multi-frame log invalidates frame 2 even
/// though frame 2's bytes are intact, because the CRC chain is broken.
/// Why this matters: Chained CRC guarantees prefix integrity — any corruption stops the entire
/// suffix from validating, not just the corrupted frame.
#[test]
fn test_crc_chain_invalidates_suffix_on_corruption() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("crc-chain.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    // Write 3 frames
    append_single_table_op_tx(&mut log, &io, (-2).into(), 1, 10, false, false, "aaa");
    let after_first = log.offset as usize;
    append_single_table_op_tx(&mut log, &io, (-2).into(), 2, 20, false, false, "bbb");
    append_single_table_op_tx(&mut log, &io, (-2).into(), 3, 30, false, false, "ccc");

    // Without corruption, all 3 frames should read back
    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    assert!(matches!(
        reader.try_read_header(&io).unwrap(),
        HeaderReadResult::Valid(_)
    ));
    let mut count = 0;
    while let Ok(ParseResult::Frame(_)) = io.block(|| reader.parse_next_transaction()) {
        count += 1;
    }
    assert_eq!(count, 3);

    // Corrupt one byte in frame 1's payload (not the CRC field itself)
    let corrupt_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + 1; // inside frame 1 payload
    let c = file
        .pwrite(
            corrupt_offset as u64,
            Arc::new(Buffer::new(vec![0xFF])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    // Now frame 1 should fail CRC, and frames 2+3 should NOT be returned
    // (chained CRC means the reader stops at the first invalid frame)
    let mut reader = StreamingLogicalLogReader::new(file, None);
    assert!(matches!(
        reader.try_read_header(&io).unwrap(),
        HeaderReadResult::Valid(_)
    ));
    // Frame 1 is corrupted — CRC mismatch on structurally complete frame
    match io.block(|| reader.parse_next_transaction()) {
        Ok(ParseResult::InvalidFrame) => {}
        other => panic!("expected InvalidFrame after corrupted frame 1, got {other:?}"),
    }
    // Verify we didn't somehow get frame 2 or 3
    let valid_offset = reader.last_valid_offset();
    assert!(
        valid_offset <= after_first,
        "valid offset {valid_offset} should be <= first frame end {after_first}",
    );
}

/// What this test checks: A structurally valid tx frame from one log cannot be spliced
/// into another log and pass CRC validation, because the two logs have different salts
/// and therefore different CRC chains.
/// Why this matters: Salt-seeded chained CRC prevents cross-log frame replay attacks —
/// an adversary cannot copy frames between logs to forge commit history.
#[test]
fn test_splice_frame_from_different_log_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());

    // --- Log A: write one frame ---
    let file_a = io
        .open_file("splice-a.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log_a = LogicalLog::new(file_a.clone(), io.clone(), None);
    append_single_table_op_tx(&mut log_a, &io, (-2).into(), 1, 10, false, false, "aaa");
    let log_a_end = log_a.offset as usize;

    // --- Log B: write one frame (different salt → different CRC chain) ---
    let file_b = io
        .open_file("splice-b.db-log", crate::OpenFlags::Create, false)
        .unwrap();
    let mut log_b = LogicalLog::new(file_b.clone(), io.clone(), None);
    append_single_table_op_tx(&mut log_b, &io, (-2).into(), 2, 20, false, false, "bbb");
    let log_b_end = log_b.offset as usize;

    // Verify the two logs have different salts
    let salt_a = log_a.header.as_ref().unwrap().salt;
    let salt_b = log_b.header.as_ref().unwrap().salt;
    assert_ne!(
        salt_a, salt_b,
        "two independent logs should have different salts"
    );

    // Read raw frame bytes from log B (everything after the header)
    let frame_b_len = log_b_end - LOG_HDR_SIZE;
    let read_buf = Arc::new(Buffer::new_temporary(frame_b_len));
    let c = file_b
        .pread(
            LOG_HDR_SIZE as u64,
            Completion::new_read(read_buf.clone(), |_| None),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let frame_b_bytes: Vec<u8> = read_buf.as_slice()[..frame_b_len].to_vec();

    // Splice log B's frame onto the end of log A
    let c = file_a
        .pwrite(
            log_a_end as u64,
            Arc::new(Buffer::new(frame_b_bytes)),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    // Read log A — should get 1 valid frame (A's own), then reject the spliced frame
    let mut reader = StreamingLogicalLogReader::new(file_a, None);
    assert!(matches!(
        reader.try_read_header(&io).unwrap(),
        HeaderReadResult::Valid(_)
    ));

    // Frame 1 from log A should validate fine
    match io.block(|| reader.parse_next_transaction()) {
        Ok(ParseResult::Frame(frame)) => assert!(!frame.ops.is_empty()),
        other => panic!("expected log A's frame to parse, got {other:?}"),
    }

    // The spliced frame from log B should fail CRC validation
    match io.block(|| reader.parse_next_transaction()) {
        Ok(ParseResult::InvalidFrame) => {}
        other => {
            panic!("spliced frame from a different log should NOT validate, got {other:?}")
        }
    }
}

fn test_enc_ctx() -> crate::storage::encryption::EncryptionContext {
    use crate::storage::encryption::{CipherMode, EncryptionKey};
    let key = EncryptionKey::Key128([0x42u8; 16]);
    crate::storage::encryption::EncryptionContext::new(CipherMode::Aes128Gcm, &key, 4096).unwrap()
}

fn wrong_key_enc_ctx() -> crate::storage::encryption::EncryptionContext {
    use crate::storage::encryption::{CipherMode, EncryptionKey};
    let key = EncryptionKey::Key128([0xFFu8; 16]);
    crate::storage::encryption::EncryptionContext::new(CipherMode::Aes128Gcm, &key, 4096).unwrap()
}

fn make_test_row_version(
    table_id: MVTableId,
    rowid: i64,
    value: &str,
    commit_ts: u64,
) -> crate::mvcc::database::RowVersion {
    let row = generate_simple_string_row(table_id, rowid, value);
    crate::mvcc::database::RowVersion {
        id: rowid as u64,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

fn make_test_index_row_version(
    table_id: MVTableId,
    rowid: i64,
    value: &str,
    commit_ts: u64,
) -> crate::mvcc::database::RowVersion {
    let key_record = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new(value.to_string())),
            Value::from_i64(rowid),
        ],
        2,
    )
    .unwrap();
    let sortable_key = SortableIndexKey::new_from_payload_in(
        &key_record,
        test_index_info(),
        crate::alloc::TursoAllocator,
    )
    .unwrap();
    let row_id = RowID::new(table_id, RowKey::Record(Arc::new(sortable_key)));
    let row = Row::new_index_row(row_id, 2);
    crate::mvcc::database::RowVersion {
        id: rowid as u64,
        begin: crate::mvcc::database::PackedTs::pack(Some(
            crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts),
        )),
        end: crate::mvcc::database::PackedTs::pack(None),
        row,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

fn test_index_info() -> Arc<IndexInfo> {
    Arc::new(
        IndexInfo::new(
            [
                crate::types::KeyInfo {
                    sort_order: turso_parser::ast::SortOrder::Asc,
                    collation: crate::translate::collate::CollationSeq::Binary,
                    nulls_order: None,
                },
                crate::types::KeyInfo {
                    sort_order: turso_parser::ast::SortOrder::Asc,
                    collation: crate::translate::collate::CollationSeq::Binary,
                    nulls_order: None,
                },
            ],
            true,
            2,
            false,
        )
        .unwrap(),
    )
}

fn make_test_raw_table_row_version(
    table_id: MVTableId,
    rowid: i64,
    record_bytes: Vec<u8>,
    commit_ts: u64,
    is_delete: bool,
) -> crate::mvcc::database::RowVersion {
    let row =
        Row::new_table_row(RowID::new(table_id, RowKey::Int(rowid)), &record_bytes, 1).unwrap();
    crate::mvcc::database::RowVersion {
        id: rowid as u64,
        begin: crate::mvcc::database::PackedTs::pack(if is_delete {
            None
        } else {
            Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
        }),
        end: crate::mvcc::database::PackedTs::pack(if is_delete {
            Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
        } else {
            None
        }),
        row,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

fn make_test_raw_index_row_version(
    table_id: MVTableId,
    rowid: i64,
    payload_bytes: Vec<u8>,
    commit_ts: u64,
    is_delete: bool,
) -> crate::mvcc::database::RowVersion {
    let sortable_key = SortableIndexKey::new_from_payload_in(
        &payload_bytes,
        test_index_info(),
        crate::alloc::TursoAllocator,
    )
    .unwrap();
    let row_id = RowID::new(table_id, RowKey::Record(Arc::new(sortable_key)));
    let row = Row::new_index_row(row_id, 2);
    crate::mvcc::database::RowVersion {
        id: rowid as u64,
        begin: crate::mvcc::database::PackedTs::pack(if is_delete {
            None
        } else {
            Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
        }),
        end: crate::mvcc::database::PackedTs::pack(if is_delete {
            Some(crate::mvcc::database::TxTimestampOrID::Timestamp(commit_ts))
        } else {
            None
        }),
        row,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

fn single_upsert_table_op_size_for_text_len(rowid: i64, text_len: usize) -> usize {
    let mut encoded = Vec::new();
    let value = "x".repeat(text_len);
    let row_version = make_test_row_version((-2).into(), rowid, &value, 100);
    LogSerializer::new(&mut encoded)
        .serialize_op_entry(&row_version, None)
        .unwrap();
    encoded.len()
}

fn try_text_len_for_single_upsert_table_op_size(
    rowid: i64,
    target_op_size: usize,
) -> Option<usize> {
    (0..=target_op_size).find(|&text_len| {
        single_upsert_table_op_size_for_text_len(rowid, text_len) == target_op_size
    })
}

fn text_len_for_single_upsert_table_op_size(target_op_size: usize) -> usize {
    if let Some(text_len) = try_text_len_for_single_upsert_table_op_size(1, target_op_size) {
        return text_len;
    }
    panic!("could not find text length for op size {target_op_size}");
}

fn try_record_bytes_len_for_upsert_table_op_size(
    rowid: i64,
    target_op_size: usize,
) -> Option<usize> {
    let rowid_len = varint_len(rowid as u64);
    for payload_len_varint_len in 1..=9usize {
        let record_bytes_len =
            target_op_size.checked_sub(6 + payload_len_varint_len + rowid_len)?;
        let payload_len = rowid_len + record_bytes_len;
        if varint_len(payload_len as u64) == payload_len_varint_len {
            return Some(record_bytes_len);
        }
    }
    None
}

fn read_file_bytes(file: Arc<dyn crate::File>, io: &Arc<dyn crate::IO>) -> Vec<u8> {
    let file_size = file.size().unwrap() as usize;
    if file_size == 0 {
        return Vec::new();
    }
    let mut reader = StreamingLogicalLogReader::new(file, None);
    io.block(|| reader.read_exact_at(0, file_size)).unwrap()
}

fn overwrite_file_bytes(file: Arc<dyn crate::File>, io: &Arc<dyn crate::IO>, bytes: &[u8]) {
    let c = file.truncate(0, Completion::new_trunc(|_| {})).unwrap();
    io.wait_for_completion(c).unwrap();
    if bytes.is_empty() {
        return;
    }
    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(bytes.to_vec())),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
}

fn open_test_file(io: &Arc<dyn crate::IO>, file_name: &str) -> Arc<dyn crate::File> {
    io.open_file(file_name, OpenFlags::Create, false).unwrap()
}

fn append_encrypted_tx(
    log: &mut LogicalLog,
    io: &Arc<dyn crate::IO>,
    tx: crate::mvcc::database::LogRecord,
) {
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();
}

fn write_first_encrypted_tx(
    file: Arc<dyn crate::File>,
    io: &Arc<dyn crate::IO>,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
    tx: crate::mvcc::database::LogRecord,
) {
    assert_eq!(
        file.size().unwrap(),
        0,
        "write_first_encrypted_tx only supports writing the first frame to a fresh file"
    );
    let mut log = LogicalLog::new(file, io.clone(), Some(enc_ctx.clone()));
    append_encrypted_tx(&mut log, io, tx);
}

fn write_first_encrypted_tx_with_chunk_size_for_test(
    file: Arc<dyn crate::File>,
    io: &Arc<dyn crate::IO>,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
    encrypted_payload_chunk_size: usize,
    tx: crate::mvcc::database::LogRecord,
) {
    assert_eq!(
        file.size().unwrap(),
        0,
        "write_first_encrypted_tx_with_chunk_size_for_test only supports writing the first frame to a fresh file"
    );
    let mut log = LogicalLog::new_with_payload_chunk_size(
        file,
        io.clone(),
        Some(enc_ctx.clone()),
        encrypted_payload_chunk_size,
    );
    append_encrypted_tx(&mut log, io, tx);
}

fn write_single_encrypted_tx(
    io: &Arc<dyn crate::IO>,
    file_name: &str,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
    tx: crate::mvcc::database::LogRecord,
) -> Arc<dyn crate::File> {
    let file = open_test_file(io, file_name);
    write_first_encrypted_tx(file.clone(), io, enc_ctx, tx);
    file
}

fn write_single_encrypted_tx_with_chunk_size_for_test(
    io: &Arc<dyn crate::IO>,
    file_name: &str,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
    encrypted_payload_chunk_size: usize,
    tx: crate::mvcc::database::LogRecord,
) -> Arc<dyn crate::File> {
    let file = open_test_file(io, file_name);
    write_first_encrypted_tx_with_chunk_size_for_test(
        file.clone(),
        io,
        enc_ctx,
        encrypted_payload_chunk_size,
        tx,
    );
    file
}

fn write_encrypted_txs_with_chunk_size_for_test(
    io: &Arc<dyn crate::IO>,
    file_name: &str,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
    encrypted_payload_chunk_size: usize,
    txs: Vec<crate::mvcc::database::LogRecord>,
) -> Arc<dyn crate::File> {
    let file = open_test_file(io, file_name);
    let mut log = LogicalLog::new_with_payload_chunk_size(
        file.clone(),
        io.clone(),
        Some(enc_ctx.clone()),
        encrypted_payload_chunk_size,
    );
    for tx in txs {
        append_encrypted_tx(&mut log, io, tx);
    }
    file
}

fn parse_only_encrypted_tx_ops(
    file: Arc<dyn crate::File>,
    io: &Arc<dyn crate::IO>,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
) -> Vec<ParsedOp> {
    let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
    reader.read_header(io).unwrap();
    let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
        ParseResult::Frame(frame) => frame.ops,
        other => panic!("expected Ops, got {other:?}"),
    };
    assert!(matches!(
        io.block(|| reader.parse_next_transaction()).unwrap(),
        ParseResult::Eof
    ));
    ops
}

fn parse_only_encrypted_tx_ops_with_chunk_size_for_test(
    file: Arc<dyn crate::File>,
    io: &Arc<dyn crate::IO>,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
    encrypted_payload_chunk_size: usize,
) -> Vec<ParsedOp> {
    let mut reader = StreamingLogicalLogReader::new_with_payload_chunk_size(
        file,
        Some(enc_ctx.clone()),
        encrypted_payload_chunk_size,
    );
    reader.read_header(io).unwrap();
    let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
        ParseResult::Frame(frame) => frame.ops,
        other => panic!("expected Ops, got {other:?}"),
    };
    assert!(matches!(
        io.block(|| reader.parse_next_transaction()).unwrap(),
        ParseResult::Eof
    ));
    ops
}

fn parse_all_encrypted_tx_ops_with_chunk_size_for_test(
    file: Arc<dyn crate::File>,
    io: &Arc<dyn crate::IO>,
    enc_ctx: &crate::storage::encryption::EncryptionContext,
    encrypted_payload_chunk_size: usize,
) -> std::result::Result<Vec<Vec<ParsedOp>>, String> {
    let mut reader = StreamingLogicalLogReader::new_with_payload_chunk_size(
        file,
        Some(enc_ctx.clone()),
        encrypted_payload_chunk_size,
    );
    reader
        .read_header(io)
        .map_err(|e| format!("failed to read fuzz log header: {e}"))?;
    let mut frames = Vec::new();
    let mut tx_index = 0usize;
    loop {
        match io
            .block(|| reader.parse_next_transaction())
            .map_err(|e| format!("failed to parse fuzz frame {tx_index}: {e}"))?
        {
            ParseResult::Frame(frame) => frames.push(frame.ops),
            ParseResult::Eof => break,
            ParseResult::InvalidFrame => {
                return Err(format!("invalid fuzz frame at tx_index={tx_index}"));
            }
        }
        tx_index += 1;
    }
    Ok(frames)
}

fn assert_upsert_table_op(
    op: &ParsedOp,
    expected_table_id: MVTableId,
    expected_rowid: i64,
    expected_record_bytes: &[u8],
    expected_commit_ts: u64,
) {
    match op {
        ParsedOp::UpsertTable {
            table_id,
            rowid,
            record_bytes,
            commit_ts,
            btree_resident,
        } => {
            assert_eq!(*table_id, expected_table_id);
            assert_eq!(rowid.row_id, RowKey::Int(expected_rowid));
            assert_eq!(record_bytes, expected_record_bytes);
            assert_eq!(*commit_ts, expected_commit_ts);
            assert!(!btree_resident);
        }
        other => panic!("expected UpsertTable, got {other:?}"),
    }
}

fn assert_upsert_index_op(
    op: &ParsedOp,
    expected_table_id: MVTableId,
    expected_payload: &[u8],
    expected_commit_ts: u64,
) {
    match op {
        ParsedOp::UpsertIndex {
            table_id,
            payload,
            commit_ts,
            btree_resident,
        } => {
            assert_eq!(*table_id, expected_table_id);
            assert_eq!(payload, expected_payload);
            assert_eq!(*commit_ts, expected_commit_ts);
            assert!(!btree_resident);
        }
        other => panic!("expected UpsertIndex, got {other:?}"),
    }
}

fn assert_update_header_op(
    op: &ParsedOp,
    expected_header: &DatabaseHeader,
    expected_commit_ts: u64,
) {
    match op {
        ParsedOp::UpdateHeader { header, commit_ts } => {
            assert_eq!(*commit_ts, expected_commit_ts);
            assert_eq!(
                bytemuck::bytes_of(header),
                bytemuck::bytes_of(expected_header)
            );
        }
        other => panic!("expected UpdateHeader, got {other:?}"),
    }
}

// Generate one record-bytes length from buckets that bias heavily toward
// chunk boundaries, while still mixing in smaller values.
fn encrypted_carry_fuzz_record_bytes_len(
    rng: &mut ChaCha8Rng,
    rowid: i64,
    chunk_size: usize,
) -> usize {
    // Sometimes force the whole serialized upsert op to land exactly on a chunk multiple.
    if rng.random_range(0..4) == 0 {
        let exact_op_size = rng.random_range(1..=3) * chunk_size;
        if let Some(record_bytes_len) =
            try_record_bytes_len_for_upsert_table_op_size(rowid, exact_op_size)
        {
            return record_bytes_len;
        }
    }

    let jitter = rng.random_range(0..=16) as isize - 8;
    let base = match rng.random_range(0..15) {
        0 => 1usize,
        1 => 16usize,
        2 => chunk_size,
        3 => chunk_size + 1,
        4 => chunk_size - 1,
        5 => 2 * chunk_size,
        6 => 2 * chunk_size + 1,
        7 => 2 * chunk_size - 1,
        8 => 3 * chunk_size,
        9 => 3 * chunk_size + 1,
        10 => chunk_size / 2,
        11 => chunk_size + chunk_size / 2,
        12 => 2 * chunk_size + chunk_size / 2,
        13 => random_range(1..=16usize) + random_range(0..=chunk_size),
        14 => random_range(1..=chunk_size),
        _ => rng.random_range(1..=3) * chunk_size,
    } as isize;
    (base + jitter).max(1) as usize
}

fn expected_upsert_table_fuzz_op(
    row_version: &crate::mvcc::database::RowVersion,
    rowid: i64,
    commit_ts: u64,
) -> ParsedOp {
    ParsedOp::UpsertTable {
        table_id: (-2).into(),
        rowid: RowID::new((-2).into(), RowKey::Int(rowid)),
        record_bytes: crate::types::value_blob_from_slice(row_version.row.payload())
            .expect(crate::alloc::ALLOC_ERR_MSG),
        commit_ts,
        btree_resident: false,
    }
}

fn assert_forced_upsert_carry_prefix_layout(
    short_filler: &crate::mvcc::database::RowVersion,
    short_upsert: &crate::mvcc::database::RowVersion,
    long_upsert: &crate::mvcc::database::RowVersion,
    chunk_size: usize,
) {
    let mut filler_buf = Vec::new();
    LogSerializer::new(&mut filler_buf)
        .serialize_op_entry(short_filler, None)
        .unwrap();
    let mut short_upsert_buf = Vec::new();
    LogSerializer::new(&mut short_upsert_buf)
        .serialize_op_entry(short_upsert, None)
        .unwrap();
    let mut long_upsert_buf = Vec::new();
    LogSerializer::new(&mut long_upsert_buf)
        .serialize_op_entry(long_upsert, None)
        .unwrap();

    turso_assert_less_than!(
        filler_buf.len(),
        chunk_size,
        "forced short-carry filler upsert must fit before the first chunk boundary"
    );
    let short_split_offset = chunk_size - filler_buf.len();
    turso_assert!(
        short_split_offset > 0 && short_split_offset < short_upsert_buf.len(),
        "forced short carry must end the first chunk inside the short upsert"
    );
    turso_assert_less_than!(
        short_upsert_buf.len(),
        StreamingLogicalLogReader::MAX_SERIALIZED_OP_PREFIX_LEN,
        "forced short carry upsert must remain below MAX_SERIALIZED_OP_PREFIX_LEN"
    );

    let long_start_offset = (filler_buf.len() + short_upsert_buf.len()) % chunk_size;
    turso_assert!(
        long_start_offset > 0,
        "forced long carry upsert must begin inside a chunk, not on a chunk boundary"
    );
    turso_assert!(
        long_upsert_buf.len() > 2 * chunk_size,
        "forced long carry upsert must span more than two chunk widths"
    );
}

fn append_forced_upsert_carry_prefix(
    rng: &mut ChaCha8Rng,
    chunk_size: usize,
    commit_ts: u64,
    row_versions: &mut Vec<crate::mvcc::database::RowVersion>,
    expected_ops: &mut Vec<ParsedOp>,
) {
    // Every forced case starts with:
    // 1. an upsert filler that lands the chunk boundary inside the next upsert
    // 2. a short carried upsert whose total size is below MAX_SERIALIZED_OP_PREFIX_LEN
    // 3. a long carried upsert that spans more than two later chunks
    let short_rowid = 0i64;
    let short_record_bytes = vec![0x11];
    let short_upsert = make_test_raw_table_row_version(
        (-2).into(),
        short_rowid,
        short_record_bytes,
        commit_ts,
        false,
    );
    let mut short_upsert_buf = Vec::new();
    LogSerializer::new(&mut short_upsert_buf)
        .serialize_op_entry(&short_upsert, None)
        .unwrap();
    turso_assert_less_than!(
        short_upsert_buf.len(),
        StreamingLogicalLogReader::MAX_SERIALIZED_OP_PREFIX_LEN,
        "forced short carry upsert must remain below MAX_SERIALIZED_OP_PREFIX_LEN"
    );

    let split_offset = rng.random_range(1..short_upsert_buf.len());
    let filler_op_size = chunk_size - split_offset;
    let filler_record_bytes_len = try_record_bytes_len_for_upsert_table_op_size(1, filler_op_size)
        .expect("forced filler upsert size must map to a valid record_bytes length");
    let short_filler = make_test_raw_table_row_version(
        (-2).into(),
        1,
        vec![0x22; filler_record_bytes_len],
        commit_ts,
        false,
    );
    let long_upsert = make_test_raw_table_row_version(
        (-2).into(),
        2,
        vec![0x5A; 2 * chunk_size + rng.random_range(64..=256)],
        commit_ts,
        false,
    );
    assert_forced_upsert_carry_prefix_layout(
        &short_filler,
        &short_upsert,
        &long_upsert,
        chunk_size,
    );

    expected_ops.push(expected_upsert_table_fuzz_op(&short_filler, 1, commit_ts));
    row_versions.push(short_filler);

    expected_ops.push(expected_upsert_table_fuzz_op(
        &short_upsert,
        short_rowid,
        commit_ts,
    ));
    row_versions.push(short_upsert);

    expected_ops.push(expected_upsert_table_fuzz_op(&long_upsert, 2, commit_ts));
    row_versions.push(long_upsert);
}

fn generate_random_encrypted_carry_fuzz_upsert(
    rng: &mut ChaCha8Rng,
    rowid: i64,
    chunk_size: usize,
    commit_ts: u64,
) -> (crate::mvcc::database::RowVersion, ParsedOp) {
    // first generate a random payload size
    let record_bytes_len = encrypted_carry_fuzz_record_bytes_len(rng, rowid, chunk_size);
    let row_version = make_test_raw_table_row_version(
        (-2).into(),
        rowid,
        vec![(rowid as u8).wrapping_add(1); record_bytes_len],
        commit_ts,
        false,
    );
    let expected = expected_upsert_table_fuzz_op(&row_version, rowid, commit_ts);
    (row_version, expected)
}

/// given a seed, generate fuzz plan with all kinds of random payload sizes.
fn generate_encrypted_carry_fuzz_case(
    case_seed: u64,
    chunk_size: usize,
    include_forced_prefix: bool,
) -> (Vec<crate::mvcc::database::LogRecord>, Vec<Vec<ParsedOp>>) {
    let mut rng = ChaCha8Rng::seed_from_u64(case_seed);
    let tx_count = rng.random_range(1..=3);
    let mut txs = Vec::with_capacity(tx_count);
    let mut expected_frames = Vec::with_capacity(tx_count);

    for tx_index in 0..tx_count {
        let commit_ts = 1_000 + (rng.next_u64() % 1_000_000) + tx_index as u64;
        let op_count = rng.random_range(1..=20);

        let mut row_versions = Vec::with_capacity(op_count);
        let mut expected_ops = Vec::with_capacity(op_count);
        // When requested, the first tx begins with two deliberate upsert carry scenarios:
        // - a short carried upsert that ends the first chunk inside a sub-15-byte op
        // - a long carried upsert that starts mid-chunk and spans more than two later chunks
        if tx_index == 0 && include_forced_prefix {
            append_forced_upsert_carry_prefix(
                &mut rng,
                chunk_size,
                commit_ts,
                &mut row_versions,
                &mut expected_ops,
            );
        }

        while row_versions.len() < op_count {
            let rowid = (row_versions.len() + 1) as i64;
            let (row_version, expected_op) =
                generate_random_encrypted_carry_fuzz_upsert(&mut rng, rowid, chunk_size, commit_ts);
            row_versions.push(row_version);
            expected_ops.push(expected_op);
        }

        txs.push(crate::mvcc::database::LogRecord::for_test(
            commit_ts,
            &row_versions,
            None,
        ));
        expected_frames.push(expected_ops);
    }

    (txs, expected_frames)
}

// Returns the byte ranges of each encrypted chunk within a frame's payload blob,
// where every chunk occupies plaintext_len + tag_size + nonce_size bytes on disk.
fn encrypted_chunk_ranges(
    payload_size: usize,
    tag_size: usize,
    nonce_size: usize,
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut offset = 0usize;
    for chunk_index in 0..encrypted_payload_chunk_count(payload_size, ENCRYPTED_PAYLOAD_CHUNK_SIZE)
    {
        let plaintext_len =
            encrypted_chunk_plaintext_len(payload_size, chunk_index, ENCRYPTED_PAYLOAD_CHUNK_SIZE)
                .unwrap();
        let chunk_len = encrypted_chunk_blob_size(plaintext_len, tag_size, nonce_size).unwrap();
        ranges.push(offset..offset + chunk_len);
        offset += chunk_len;
    }
    ranges
}

fn assert_single_frame_invalid(
    file: Arc<dyn crate::File>,
    io: &Arc<dyn crate::IO>,
    enc_ctx: crate::storage::encryption::EncryptionContext,
) {
    let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
    reader.read_header(io).unwrap();
    match io.block(|| reader.parse_next_transaction()).unwrap() {
        ParseResult::InvalidFrame => {}
        other => panic!("expected InvalidFrame, got {other:?}"),
    }
}

/// Write an encrypted frame, verify the on-disk layout invariant
/// (`plaintext + per-chunk tag/nonce metadata`), then read back and
/// verify roundtrip correctness with multiple ops.
#[test]
fn test_encrypted_log_roundtrip_and_layout() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = open_test_file(&io, "enc-roundtrip.db-log");
    let table_id: MVTableId = (-2).into();
    let enc_ctx = test_enc_ctx();
    let tag_size = enc_ctx.tag_size();
    let nonce_size = enc_ctx.nonce_size();
    let expected_hello_record_bytes = generate_simple_string_row(table_id, 1, "hello")
        .payload()
        .to_vec();
    let expected_world_record_bytes = generate_simple_string_row(table_id, 2, "world")
        .payload()
        .to_vec();

    // Write one encrypted frame with 2 ops.
    let tx = crate::mvcc::database::LogRecord::for_test(
        100,
        &[
            make_test_row_version(table_id, 1, "hello", 100),
            make_test_row_version(table_id, 2, "world", 100),
        ],
        None,
    );
    write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);

    // ── Layout invariant check ──
    // Read the raw TX header to extract payload_size.
    let frame_hdr_buf = Arc::new(Buffer::new_temporary(TX_HEADER_SIZE));
    let frame_hdr_out = Arc::new(crate::sync::RwLock::new(Vec::new()));
    let out = frame_hdr_out.clone();
    let c = Completion::new_read(
        frame_hdr_buf,
        Box::new(
            move |res: std::result::Result<(Arc<Buffer>, i32), crate::CompletionError>| {
                let Ok((buf, n)) = res else { return None };
                out.write().extend_from_slice(&buf.as_slice()[..n as usize]);
                None
            },
        ),
    );
    let c = file.pread(LOG_HDR_SIZE as u64, c).unwrap();
    io.wait_for_completion(c).unwrap();

    let frame_hdr = frame_hdr_out.read();
    assert_eq!(frame_hdr.len(), TX_HEADER_SIZE);
    let payload_size = u64::from_le_bytes(frame_hdr[4..12].try_into().unwrap()) as usize;

    let file_size = file.size().unwrap() as usize;
    let encrypted_blob_size = file_size - LOG_HDR_SIZE - TX_HEADER_SIZE - TX_TRAILER_SIZE;
    let expected_blob_size = encrypted_payload_blob_size(
        payload_size,
        ENCRYPTED_PAYLOAD_CHUNK_SIZE,
        tag_size,
        nonce_size,
    )
    .unwrap();
    assert_eq!(
        encrypted_blob_size, expected_blob_size,
        "on-disk blob size ({encrypted_blob_size}) != expected chunked encrypted size({expected_blob_size})"
    );

    // ── Roundtrip read ──
    let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
    reader.read_header(&io).unwrap();

    let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
        ParseResult::Frame(frame) => frame.ops,
        other => panic!("expected Ops, got {other:?}"),
    };
    assert_eq!(ops.len(), 2);
    assert_upsert_table_op(&ops[0], table_id, 1, &expected_hello_record_bytes, 100);
    assert_upsert_table_op(&ops[1], table_id, 2, &expected_world_record_bytes, 100);

    assert!(matches!(
        io.block(|| reader.parse_next_transaction()).unwrap(),
        ParseResult::Eof
    ));
}

/// What this test checks: Test-only chunk-size overrides affect both encrypted writing and
/// streaming recovery, so fuzz tests can exercise smaller chunk boundaries without changing
/// the production format constant.
#[test]
fn test_encrypted_log_roundtrip_with_test_chunk_size_override() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();
    const TEST_CHUNK_SIZE: usize = 2 * 1024;

    let target_op_size = TEST_CHUNK_SIZE + 257;
    let text_len = text_len_for_single_upsert_table_op_size(target_op_size);
    let value = "t".repeat(text_len);
    let row_version = make_test_row_version((-2).into(), 1, &value, 100);
    let expected_record_bytes = row_version.row.payload().to_vec();
    let tx = crate::mvcc::database::LogRecord::for_test(100, &[row_version], None);

    let file = write_single_encrypted_tx_with_chunk_size_for_test(
        &io,
        "enc-roundtrip-test-chunk-size.db-log",
        &enc_ctx,
        TEST_CHUNK_SIZE,
        tx,
    );

    assert_eq!(
        encrypted_payload_chunk_count(target_op_size, TEST_CHUNK_SIZE),
        2,
        "test payload should span exactly two test-sized chunks"
    );
    let expected_blob_size = encrypted_payload_blob_size(
        target_op_size,
        TEST_CHUNK_SIZE,
        enc_ctx.tag_size(),
        enc_ctx.nonce_size(),
    )
    .unwrap();
    assert_eq!(
        file.size().unwrap() as usize,
        LOG_HDR_SIZE + TX_HEADER_SIZE + expected_blob_size + TX_TRAILER_SIZE
    );

    let ops =
        parse_only_encrypted_tx_ops_with_chunk_size_for_test(file, &io, &enc_ctx, TEST_CHUNK_SIZE);
    assert_eq!(ops.len(), 1);
    assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_record_bytes, 100);
}

/// Random fuzzer to test encrypted chunking logic, especially carry.
/// We create a plan from a seed, then generate ops, write to encrypted log file and read it back
#[test]
fn test_encrypted_log_carry_fuzz() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();
    const TEST_CHUNK_SIZE: usize = 2 * 1024;

    let seed = std::env::var("TURSO_ENCRYPTED_CARRY_FUZZ_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| rng().random::<u64>());
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let case_count = rng.random_range(1..=8);
    let forced_case_index = rng.random_range(0..case_count);
    eprintln!(
        "encrypted carry fuzz root_seed={seed} case_count={case_count} forced_case_index={forced_case_index} test_chunk_size={TEST_CHUNK_SIZE}"
    );

    for case_index in 0..case_count {
        let case_seed = rng.next_u64();
        let include_forced_prefix = case_index == forced_case_index;
        let (txs, expected_frames) =
            generate_encrypted_carry_fuzz_case(case_seed, TEST_CHUNK_SIZE, include_forced_prefix);

        let file = write_encrypted_txs_with_chunk_size_for_test(
            &io,
            &format!("enc-carry-fuzz-{seed}-{case_index}.db-log"),
            &enc_ctx,
            TEST_CHUNK_SIZE,
            txs,
        );
        let actual_frames = parse_all_encrypted_tx_ops_with_chunk_size_for_test(
            file,
            &io,
            &enc_ctx,
            TEST_CHUNK_SIZE,
        )
        .unwrap_or_else(|err| {
            panic!(
                "encrypted carry fuzz failed while parsing frames: root_seed={seed} case_index={case_index} forced_case_index={forced_case_index} include_forced_prefix={include_forced_prefix} case_seed={case_seed} err={err}"
            )
        });

        assert_eq!(
            actual_frames, expected_frames,
            "encrypted carry fuzz failed: root_seed={seed} case_index={case_index} forced_case_index={forced_case_index} include_forced_prefix={include_forced_prefix} case_seed={case_seed}"
        );
    }
}

#[test]
fn test_encrypted_log_format_assumptions_are_pinned() {
    assert_eq!(LOG_VERSION_V2, 2);
    assert_eq!(LOG_VERSION, 3);
    assert_eq!(LOG_HDR_SIZE, 56);
    assert_eq!(ENCRYPTED_PAYLOAD_CHUNK_SIZE, 32 * 1024);
    assert_eq!(ENCRYPTED_CHUNK_AAD_SIZE, 32);
    assert_eq!(FRAME_MAGIC, 0x5854_564D);
    assert_eq!(EXT_FRAME_MAGIC, 0x5845_564D);
    assert_eq!(END_MAGIC, 0x4554_564D);
    assert_eq!(TX_HEADER_SIZE_V2, 24);
    assert_eq!(TX_HEADER_SIZE, 24);
    assert_eq!(TX_EXT_HEADER_SIZE, 40);
    assert_eq!(TX_TRAILER_SIZE, 8);
}

#[test]
fn test_non_portable_first_write_uses_lml2_header_and_v2_frame() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "non-portable-first-write-lml2.db-log",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let tx = crate::mvcc::database::LogRecord::for_test(
        10,
        &[make_test_row_version((-2).into(), 1, "visible", 10)],
        None,
    );
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let frame = read_file_bytes(file, &io);
    let header = LogHeader::decode(&frame[..LOG_HDR_SIZE]).unwrap();
    assert_eq!(header.version, LOG_VERSION_V2);
    assert_eq!(
        u32::from_le_bytes(frame[LOG_HDR_SIZE..LOG_HDR_SIZE + 4].try_into().unwrap()),
        FRAME_MAGIC
    );
}

#[test]
fn test_non_portable_appends_keep_lml2_header_and_v2_frames() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("non-portable-appends-lml2.db-log", OpenFlags::Create, false)
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    for (commit_ts, rowid) in [(10, 1), (20, 2)] {
        let tx = crate::mvcc::database::LogRecord::for_test(
            commit_ts,
            &[make_test_row_version(
                (-2).into(),
                rowid,
                "visible",
                commit_ts,
            )],
            None,
        );
        let c = log.log_tx(tx).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    let frame = read_file_bytes(file, &io);
    let header = LogHeader::decode(&frame[..LOG_HDR_SIZE]).unwrap();
    assert_eq!(header.version, LOG_VERSION_V2);
    assert_eq!(
        u32::from_le_bytes(frame[LOG_HDR_SIZE..LOG_HDR_SIZE + 4].try_into().unwrap()),
        FRAME_MAGIC
    );

    let first_payload_size = u64::from_le_bytes(
        frame[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let second_frame_start = LOG_HDR_SIZE + TX_HEADER_SIZE + first_payload_size + TX_TRAILER_SIZE;
    assert_eq!(
        u32::from_le_bytes(
            frame[second_frame_start..second_frame_start + 4]
                .try_into()
                .unwrap()
        ),
        FRAME_MAGIC
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_portable_changes_upgrade_non_empty_lml2_log_to_lml3() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "portable-after-lml2-upgrade.db-log",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let tx = crate::mvcc::database::LogRecord::for_test(
        10,
        &[make_test_row_version((-2).into(), 1, "visible", 10)],
        None,
    );
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let mut portable_tx = crate::mvcc::database::LogRecord::for_test(
        20,
        &[make_test_row_version((-2).into(), 2, "visible", 20)],
        None,
    );
    portable_tx.portable_changes_enabled = true;
    portable_tx.portable_changes = crate::alloc::vec![0x1a, 0x00];

    let c = log
        .upgrade_header_for_log_tx(&portable_tx)
        .unwrap()
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let c = log.log_tx(portable_tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let frame = read_file_bytes(file, &io);
    let header = LogHeader::decode(&frame[..LOG_HDR_SIZE]).unwrap();
    assert_eq!(header.version, LOG_VERSION);

    let first_payload_size = u64::from_le_bytes(
        frame[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let second_frame_start = LOG_HDR_SIZE + TX_HEADER_SIZE + first_payload_size + TX_TRAILER_SIZE;
    assert_eq!(
        u32::from_le_bytes(
            frame[second_frame_start..second_frame_start + 4]
                .try_into()
                .unwrap()
        ),
        EXT_FRAME_MAGIC
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_next_portable_change_frame_returns_empty_and_nonempty_lml3_frames() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "sync-frame-empty-and-nonempty.db-log",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let mut empty_sync_tx = crate::mvcc::database::LogRecord::for_test(
        10,
        &[make_test_row_version((-2).into(), 1, "internal", 10)],
        None,
    );
    empty_sync_tx.portable_changes_enabled = true;
    let c = log.log_tx(empty_sync_tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let encoded_empty_logical_op = crate::alloc::vec![0x1a, 0x00];
    let mut sync_tx = crate::mvcc::database::LogRecord::for_test(
        20,
        &[make_test_row_version((-2).into(), 2, "visible", 20)],
        None,
    );
    sync_tx.portable_changes = encoded_empty_logical_op;
    let c = log.log_tx(sync_tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let mut reader = StreamingLogicalLogReader::new(file, None);
    reader.read_header(&io).unwrap();
    assert_eq!(reader.header().unwrap().version, LOG_VERSION);
    // A portable-enabled writer marks an empty change set explicitly: the
    // frame carries an extension record whose payload has the frame cursor
    // and commit timestamp but no object map, so readers decode it into no
    // logical ops instead of having to guess whether a plain frame predates
    // portable changes.
    let first = io
        .block(|| reader.next_portable_change_frame())
        .unwrap()
        .unwrap();
    assert_eq!(first.commit_ts, 10);
    assert_eq!(first.extension_record_count, 1);
    assert!(!first.payload.is_empty());
    assert_eq!(first.end_offset, reader.last_valid_offset() as u64);

    let second = io
        .block(|| reader.next_portable_change_frame())
        .unwrap()
        .unwrap();
    assert_eq!(second.commit_ts, 20);
    assert_eq!(second.extension_record_count, 1);
    assert!(!second.payload.is_empty());
    assert_eq!(second.end_offset, reader.last_valid_offset() as u64);

    assert!(io
        .block(|| reader.next_portable_change_frame())
        .unwrap()
        .is_none());
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_portable_extension_block_precedes_recovery_payload() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(
            "portable-extension-before-payload.db-log",
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let mut log = LogicalLog::new(file.clone(), io.clone(), None);

    let portable_metadata = crate::alloc::vec![0x1a, 0x00];
    let mut tx = crate::mvcc::database::LogRecord::for_test(
        20,
        &[make_test_row_version((-2).into(), 2, "visible", 20)],
        None,
    );
    tx.portable_changes = portable_metadata.clone();
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let frame = read_file_bytes(file, &io);
    let tx_header_start = LOG_HDR_SIZE;
    let body_start = LOG_HDR_SIZE + TX_EXT_HEADER_SIZE;
    assert_eq!(
        u32::from_le_bytes(
            frame[tx_header_start..tx_header_start + 4]
                .try_into()
                .unwrap()
        ),
        EXT_FRAME_MAGIC
    );
    let extension_size = u64::from_le_bytes(
        frame[tx_header_start + 24..tx_header_start + 32]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(extension_size >= EXTENSION_RECORD_HEADER_SIZE);

    let extension_type = u16::from_le_bytes(frame[body_start..body_start + 2].try_into().unwrap());
    assert_eq!(extension_type, EXTENSION_TYPE_PORTABLE_CHANGES);
    let extension_payload_len = u32::from_le_bytes(
        frame[body_start + 4..body_start + EXTENSION_RECORD_HEADER_SIZE]
            .try_into()
            .unwrap(),
    ) as usize;
    let extension_payload = &frame[body_start + EXTENSION_RECORD_HEADER_SIZE
        ..body_start + EXTENSION_RECORD_HEADER_SIZE + extension_payload_len];
    assert!(extension_payload.ends_with(&portable_metadata));

    let recovery_start = body_start + extension_size;
    assert_eq!(frame[recovery_start], OP_UPSERT_TABLE);
}

#[test]
fn test_next_portable_change_frame_does_not_advance_lml2_logs() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("sync-frame-lml2.db-log", OpenFlags::Create, false)
        .unwrap();

    let mut header = LogHeader::new(&io);
    header.version = LOG_VERSION_V2;
    let buffer = Arc::new(Buffer::new(header.encode().to_vec()));
    let c = Completion::new_write(|_| {});
    io.wait_for_completion(file.pwrite(0, buffer, c).unwrap())
        .unwrap();

    let mut reader = StreamingLogicalLogReader::new(file, None);
    reader.read_header(&io).unwrap();
    assert_eq!(reader.last_valid_offset(), LOG_HDR_SIZE);
    assert!(io
        .block(|| reader.next_portable_change_frame())
        .unwrap()
        .is_none());
    assert_eq!(reader.last_valid_offset(), LOG_HDR_SIZE);
}

#[test]
fn test_encrypted_chunk_aad_layout_is_pinned() {
    let non_last_aad = build_encrypted_chunk_aad(
        0x0102_0304_0506_0708,
        None,
        0x2122_2324,
        0x3132_3334_3536_3738,
        0x4142_4344,
    );
    assert_eq!(
        non_last_aad,
        [
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // salt
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, // payload_size omitted for non-final chunk
            0x24, 0x23, 0x22, 0x21, // op_count
            0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, // commit_ts
            0x44, 0x43, 0x42, 0x41, // chunk_index
        ]
    );

    let last_aad = build_encrypted_chunk_aad(
        0x0102_0304_0506_0708,
        Some(0x1112_1314_1516_1718),
        0x2122_2324,
        0x3132_3334_3536_3738,
        0x4142_4344,
    );

    assert_eq!(
        last_aad,
        [
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // salt
            0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // payload_size (final chunk only)
            0x24, 0x23, 0x22, 0x21, // op_count
            0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, // commit_ts
            0x44, 0x43, 0x42, 0x41, // chunk_index
        ]
    );
}

#[test]
fn test_encrypted_log_aes128_chunk_layout_assumptions_are_pinned() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();

    assert_eq!(
        enc_ctx.cipher_mode(),
        crate::storage::encryption::CipherMode::Aes128Gcm
    );
    assert_eq!(enc_ctx.tag_size(), 16);
    assert_eq!(enc_ctx.nonce_size(), 12);

    for (payload_size, expected_chunk_ranges, expected_file_size) in [
        (
            32_767usize,
            std::iter::once(0..32_795).collect::<Vec<_>>(),
            32_883usize,
        ),
        (
            32_768usize,
            std::iter::once(0..32_796).collect::<Vec<_>>(),
            32_884usize,
        ),
        (32_769usize, vec![0..32_796, 32_796..32_825], 32_913usize),
        (65_536usize, vec![0..32_796, 32_796..65_592], 65_680usize),
        (
            65_537usize,
            vec![0..32_796, 32_796..65_592, 65_592..65_621],
            65_709usize,
        ),
    ] {
        let text_len = text_len_for_single_upsert_table_op_size(payload_size);
        let value = "p".repeat(text_len);
        let tx = crate::mvcc::database::LogRecord::for_test(
            100,
            &[make_test_row_version((-2).into(), 1, &value, 100)],
            None,
        );
        let file = write_single_encrypted_tx(
            &io,
            &format!("enc-layout-pinned-{payload_size}.db-log"),
            &enc_ctx,
            tx,
        );

        let frame_bytes = read_file_bytes(file.clone(), &io);
        let actual_payload_size = u64::from_le_bytes(
            frame_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(actual_payload_size, payload_size);
        assert_eq!(file.size().unwrap() as usize, expected_file_size);
        assert_eq!(
            encrypted_chunk_ranges(payload_size, 16, 12),
            expected_chunk_ranges
        );
    }
}

// Verifies the final chunk authenticates payload_size: tampering the TX header's
// payload_size field must still reject the encrypted frame.
#[test]
fn test_encrypted_log_payload_size_tamper_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("enc-payload-size-tamper.db-log", OpenFlags::Create, false)
        .unwrap();
    let enc_ctx = test_enc_ctx();
    let table_id: MVTableId = (-2).into();
    let text_len = text_len_for_single_upsert_table_op_size(2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257);
    let value = "s".repeat(text_len);

    let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
    let tx = crate::mvcc::database::LogRecord::for_test(
        444,
        &[make_test_row_version(table_id, 1, &value, 444)],
        None,
    );
    append_encrypted_tx(&mut log, &io, tx);

    let frame_bytes = read_file_bytes(file.clone(), &io);
    let payload_size = u64::from_le_bytes(
        frame_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    );
    let bad_payload_size = Arc::new(Buffer::new((payload_size + 1).to_le_bytes().to_vec()));
    let c = Completion::new_write(|_| {});
    io.wait_for_completion(
        file.pwrite((LOG_HDR_SIZE + 4) as u64, bad_payload_size, c)
            .unwrap(),
    )
    .unwrap();

    let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
    reader.read_header(&io).unwrap();
    match io.block(|| reader.parse_next_transaction()).unwrap() {
        ParseResult::InvalidFrame => {}
        other => panic!("expected InvalidFrame after payload_size tamper, got {other:?}"),
    }
}

#[test]
fn test_encrypted_log_chunk_layout_boundaries() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();
    let tag_size = enc_ctx.tag_size();
    let nonce_size = enc_ctx.nonce_size();

    for target_op_size in [
        ENCRYPTED_PAYLOAD_CHUNK_SIZE - 1,
        ENCRYPTED_PAYLOAD_CHUNK_SIZE,
        ENCRYPTED_PAYLOAD_CHUNK_SIZE + 1,
        2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE,
        2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 1,
    ] {
        let text_len = text_len_for_single_upsert_table_op_size(target_op_size);
        let value = "x".repeat(text_len);
        let row_version = make_test_row_version((-2).into(), 1, &value, 100);
        let expected_record_bytes = row_version.row.payload().to_vec();
        let tx = crate::mvcc::database::LogRecord::for_test(100, &[row_version], None);
        let file = write_single_encrypted_tx(
            &io,
            &format!("enc-layout-{target_op_size}.db-log"),
            &enc_ctx,
            tx,
        );

        let frame_hdr = read_file_bytes(file.clone(), &io);
        let payload_size = u64::from_le_bytes(
            frame_hdr[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(payload_size, target_op_size);

        let file_size = file.size().unwrap() as usize;
        let encrypted_blob_size = file_size - LOG_HDR_SIZE - TX_HEADER_SIZE - TX_TRAILER_SIZE;
        let expected_blob_size = encrypted_payload_blob_size(
            payload_size,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            tag_size,
            nonce_size,
        )
        .unwrap();
        assert_eq!(encrypted_blob_size, expected_blob_size);

        let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
        assert_eq!(ops.len(), 1);
        assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_record_bytes, 100);
    }
}

#[test]
fn test_encrypted_log_single_op_crosses_chunk_boundary() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();
    let target_op_size = ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257;
    let text_len = text_len_for_single_upsert_table_op_size(target_op_size);
    let value = "x".repeat(text_len);
    let row_version = make_test_row_version((-2).into(), 1, &value, 100);
    let expected_record_bytes = row_version.row.payload().to_vec();

    let tx = crate::mvcc::database::LogRecord::for_test(100, &[row_version], None);
    let file = write_single_encrypted_tx(&io, "enc-cross-boundary.db-log", &enc_ctx, tx);
    let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
    assert_eq!(ops.len(), 1);
    assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_record_bytes, 100);
}

// Verifies the reader can reconstruct a payload_len varint that is split across
// two encrypted chunks, without changing either row payload.
#[test]
fn test_encrypted_log_varint_crosses_chunk_boundary() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("enc-varint-boundary.db-log", OpenFlags::Create, false)
        .unwrap();
    let enc_ctx = test_enc_ctx();
    // Keep the first op 7 bytes short of a full chunk so the second op begins with:
    // 6-byte op prelude (tag + flags + table_id) and then 1 byte of payload_len varint.
    // That places the chunk boundary immediately after the first varint byte.
    let filler_len = text_len_for_single_upsert_table_op_size(ENCRYPTED_PAYLOAD_CHUNK_SIZE - 7);
    let filler_value = "a".repeat(filler_len);
    let second_value = "b".repeat(200);
    let filler = make_test_row_version((-2).into(), 1, &filler_value, 100);
    let second = make_test_row_version((-2).into(), 2, &second_value, 100);
    let expected_filler_record_bytes = filler.row.payload().to_vec();
    let expected_second_record_bytes = second.row.payload().to_vec();

    let mut filler_buf = Vec::new();
    LogSerializer::new(&mut filler_buf)
        .serialize_op_entry(&filler, None)
        .unwrap();
    assert_eq!(filler_buf.len(), ENCRYPTED_PAYLOAD_CHUNK_SIZE - 7);

    let mut second_buf = Vec::new();
    LogSerializer::new(&mut second_buf)
        .serialize_op_entry(&second, None)
        .unwrap();
    // Table ops begin with a fixed 6-byte prelude:
    // 1 byte op tag + 1 byte flags + 4 bytes table_id.
    // The payload_len varint begins immediately after that prefix.
    let (_, varint_bytes) = read_varint_partial(&second_buf[6..]).unwrap().unwrap();
    assert!(
        varint_bytes >= 2,
        "second op payload_len must use a multi-byte varint so the chunk boundary can split it"
    );
    // filler_buf.len() consumes the prefix of the chunk, then the second op contributes:
    // 6 bytes of fixed prelude + exactly 1 byte of payload_len varint before the boundary.
    // That forces the remaining varint bytes into the next encrypted chunk.
    assert_eq!(
        filler_buf.len() + 6 + 1,
        ENCRYPTED_PAYLOAD_CHUNK_SIZE,
        "chunk boundary should fall after the first payload_len varint byte"
    );

    let tx = crate::mvcc::database::LogRecord::for_test(100, &[filler, second], None);
    write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);
    let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
    assert_eq!(ops.len(), 2);
    assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_filler_record_bytes, 100);
    assert_upsert_table_op(&ops[1], (-2).into(), 2, &expected_second_record_bytes, 100);
}

// Verifies a transaction header update still round-trips when the OP_UPDATE_HEADER
// entry itself is split across an encrypted chunk boundary.
#[test]
fn test_encrypted_log_header_op_crosses_chunk_boundary() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("enc-header-boundary.db-log", OpenFlags::Create, false)
        .unwrap();
    let enc_ctx = test_enc_ctx();
    let mut header_buf = Vec::new();
    let mut header = DatabaseHeader::default();
    header.database_size = 123.into();
    header.schema_cookie = 456.into();
    LogSerializer::new(&mut header_buf)
        .serialize_header_entry(&header)
        .unwrap();

    let filler_payload_size = ENCRYPTED_PAYLOAD_CHUNK_SIZE - (header_buf.len() - 1);
    let filler_len = text_len_for_single_upsert_table_op_size(filler_payload_size);
    let filler_value = "h".repeat(filler_len);
    let filler = make_test_row_version((-2).into(), 1, &filler_value, 100);
    let expected_filler_record_bytes = filler.row.payload().to_vec();

    let mut filler_buf = Vec::new();
    LogSerializer::new(&mut filler_buf)
        .serialize_op_entry(&filler, None)
        .unwrap();
    assert_eq!(filler_buf.len(), filler_payload_size);
    assert_eq!(
        filler_buf.len() + header_buf.len() - 1,
        ENCRYPTED_PAYLOAD_CHUNK_SIZE,
        "chunk boundary should split the header op after its first byte"
    );

    let tx = crate::mvcc::database::LogRecord::for_test(100, &[filler], Some(header));
    write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);
    let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
    assert_eq!(ops.len(), 2);
    assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_filler_record_bytes, 100);
    assert_update_header_op(&ops[1], &header, 100);
}

// Verifies the chunked reader can walk a long sequence of table upserts whose
// boundaries land both between ops and in the middle of serialized row payloads.
#[test]
fn test_encrypted_log_many_ops_cross_chunk_boundaries() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("enc-many-ops.db-log", OpenFlags::Create, false)
        .unwrap();
    let enc_ctx = test_enc_ctx();
    let table_id: MVTableId = (-2).into();

    let row_versions = (0..96)
        .map(|rowid| {
            let value = format!("row-{rowid}-{}", "x".repeat(900));
            make_test_row_version(table_id, rowid + 1, &value, 200)
        })
        .collect::<Vec<_>>();
    let expected_record_bytes = row_versions
        .iter()
        .map(|row_version| row_version.row.payload().to_vec())
        .collect::<Vec<_>>();
    let tx = crate::mvcc::database::LogRecord::for_test(200, &row_versions, None);
    write_first_encrypted_tx(file.clone(), &io, &enc_ctx, tx);
    let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
    assert_eq!(ops.len(), 96);
    for (idx, op) in ops.iter().enumerate() {
        assert_upsert_table_op(
            op,
            table_id,
            (idx + 1) as i64,
            &expected_record_bytes[idx],
            200,
        );
    }
}

// Verifies a large index-key payload is chunked, decrypted, and parsed back as an
// UpsertIndex op without changing the serialized key bytes.
#[test]
fn test_encrypted_log_upsert_index_crosses_chunk_boundary() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();
    let index_id: MVTableId = (-3).into();
    let value = "i".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 2);
    let row_version = make_test_index_row_version(index_id, 42, &value, 250);
    let expected_payload = row_version.row.payload().to_vec();

    let tx = crate::mvcc::database::LogRecord::for_test(250, &[row_version], None);
    let file = write_single_encrypted_tx(&io, "enc-index-boundary.db-log", &enc_ctx, tx);

    let frame_bytes = read_file_bytes(file.clone(), &io);
    let payload_size = u64::from_le_bytes(
        frame_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(
        payload_size > ENCRYPTED_PAYLOAD_CHUNK_SIZE,
        "index payload should span multiple encrypted chunks"
    );

    let ops = parse_only_encrypted_tx_ops(file, &io, &enc_ctx);
    assert_eq!(ops.len(), 1);
    assert_upsert_index_op(&ops[0], index_id, &expected_payload, 250);
}

// Verifies CRC chaining across multiple encrypted frames while still preserving the
// exact row payload bytes in every successfully parsed frame.
#[test]
fn test_encrypted_log_multiple_frames_crc_chain() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("enc-multi.db-log", OpenFlags::Create, false)
        .unwrap();
    let table_id: MVTableId = (-2).into();
    let enc_ctx = test_enc_ctx();
    let expected_record_bytes = (0..5u64)
        .map(|i| generate_simple_string_row(table_id, i as i64, &format!("val_{i}")))
        .map(|row| row.payload().to_vec())
        .collect::<Vec<_>>();

    let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
    for i in 0..5u64 {
        let tx = crate::mvcc::database::LogRecord::for_test(
            100 + i,
            &[make_test_row_version(
                table_id,
                i as i64,
                &format!("val_{i}"),
                100 + i,
            )],
            None,
        );
        append_encrypted_tx(&mut log, &io, tx);
    }

    let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
    reader.read_header(&io).unwrap();

    for i in 0..5u64 {
        let ops = match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::Frame(frame) => frame.ops,
            other => panic!("frame {i}: expected Ops, got {other:?}"),
        };
        assert_eq!(ops.len(), 1, "frame {i}");
        assert_upsert_table_op(
            &ops[0],
            table_id,
            i as i64,
            &expected_record_bytes[i as usize],
            100 + i,
        );
    }

    assert!(matches!(
        io.block(|| reader.parse_next_transaction()).unwrap(),
        ParseResult::Eof
    ));
}

/// AEAD integrity: wrong key and tampered ciphertext must both be rejected.
#[test]
fn test_encrypted_log_integrity_rejection() {
    init_tracing();
    let table_id: MVTableId = (-2).into();
    let enc_ctx = test_enc_ctx();

    // ── Wrong key ──
    {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-wrongkey.db-log", OpenFlags::Create, false)
            .unwrap();

        let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
        let tx = crate::mvcc::database::LogRecord::for_test(
            100,
            &[make_test_row_version(table_id, 1, "secret", 100)],
            None,
        );
        append_encrypted_tx(&mut log, &io, tx);

        let mut reader = StreamingLogicalLogReader::new(file, Some(wrong_key_enc_ctx()));
        reader.read_header(&io).unwrap();

        match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::InvalidFrame => {}
            other => panic!("expected InvalidFrame with wrong key, got {other:?}"),
        }
    }

    // ── Tampered TX header (commit_ts) ──
    // commit_ts is part of the AAD, so flipping a byte in it causes AEAD
    // decryption to fail even though the ciphertext itself is untouched.
    {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-hdr-tamper.db-log", OpenFlags::Create, false)
            .unwrap();

        let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
        let tx = crate::mvcc::database::LogRecord::for_test(
            100,
            &[make_test_row_version(table_id, 1, "hdr_tamper", 100)],
            None,
        );
        append_encrypted_tx(&mut log, &io, tx);

        // Flip a byte in the commit_ts field (TX header offset 16..24, file offset = LOG_HDR + 16).
        let corrupt_offset = (LOG_HDR_SIZE + 16) as u64;
        let byte_buf = Arc::new(Buffer::new(vec![0xFF]));
        let c = Completion::new_write(move |_| {});
        io.wait_for_completion(file.pwrite(corrupt_offset, byte_buf, c).unwrap())
            .unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
        reader.read_header(&io).unwrap();

        match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::InvalidFrame => {}
            other => panic!("expected InvalidFrame after TX header tamper, got {other:?}"),
        }
    }

    // ── Tampered ciphertext ──
    {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("enc-tamper.db-log", OpenFlags::Create, false)
            .unwrap();

        let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
        let tx = crate::mvcc::database::LogRecord::for_test(
            100,
            &[make_test_row_version(table_id, 1, "tamper_me", 100)],
            None,
        );
        append_encrypted_tx(&mut log, &io, tx);

        // Tamper a 16-byte window in the ciphertext (after log header
        // + TX header). A single-byte overwrite with 0xFF can be a no-op
        // when the cipher output at that offset already equals 0xFF
        // (~1/256 per run); tampering a 16-byte run with a fixed
        // alternating pattern makes the no-op probability 1/256^16,
        // which is effectively never.
        let corrupt_offset = (LOG_HDR_SIZE + TX_HEADER_SIZE + 1) as u64;
        let pattern: Vec<u8> = (0..16)
            .map(|i| if i & 1 == 0 { 0x00 } else { 0xFF })
            .collect();
        let byte_buf = Arc::new(Buffer::new(pattern));
        let c = Completion::new_write(move |_| {});
        io.wait_for_completion(file.pwrite(corrupt_offset, byte_buf, c).unwrap())
            .unwrap();

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
        reader.read_header(&io).unwrap();

        match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::InvalidFrame => {}
            other => panic!("expected InvalidFrame after ciphertext tamper, got {other:?}"),
        }
    }
}

// Verifies a torn final frame is ignored while the last fully written prefix frame
// still decrypts to the exact bytes that were committed before the tear.
#[test]
fn test_encrypted_log_torn_tail_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = open_test_file(&io, "enc-torn.db-log");
    let table_id: MVTableId = (-2).into();
    let enc_ctx = test_enc_ctx();
    let first_row_version = make_test_row_version(table_id, 0, "data", 100);
    let expected_first_record_bytes = first_row_version.row.payload().to_vec();

    // Write 2 frames.
    let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
    let first_tx = crate::mvcc::database::LogRecord::for_test(100, &[first_row_version], None);
    append_encrypted_tx(&mut log, &io, first_tx);
    let second_tx = crate::mvcc::database::LogRecord::for_test(
        101,
        &[make_test_row_version(table_id, 1, "data", 101)],
        None,
    );
    append_encrypted_tx(&mut log, &io, second_tx);

    // Truncate mid-way through the second frame.
    let file_size = file.size().unwrap();
    let truncate_at = file_size - 5; // remove last 5 bytes
    let c = Completion::new_trunc(|_| {});
    io.wait_for_completion(file.truncate(truncate_at, c).unwrap())
        .unwrap();

    let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx));
    reader.read_header(&io).unwrap();

    // First frame should parse fine.
    match io.block(|| reader.parse_next_transaction()).unwrap() {
        ParseResult::Frame(frame) => {
            let ops = frame.ops;
            assert_eq!(ops.len(), 1);
            assert_upsert_table_op(&ops[0], (-2).into(), 0, &expected_first_record_bytes, 100);
        }
        other => panic!("expected Ops for frame 1, got {other:?}"),
    }

    // Second frame is torn — should be EOF.
    match io.block(|| reader.parse_next_transaction()).unwrap() {
        ParseResult::Eof => {}
        other => panic!("expected Eof for torn frame 2, got {other:?}"),
    }
}

// Verifies chunk-level tampering is rejected: any corruption, reorder, drop, or
// duplicate in the encrypted chunk stream must fail closed instead of replaying data.
#[test]
fn test_encrypted_log_chunk_integrity_rejection() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let enc_ctx = test_enc_ctx();
    let text_len = text_len_for_single_upsert_table_op_size(2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257);
    let value = "z".repeat(text_len);

    let base_file = open_test_file(&io, "enc-chunk-integrity-base.db-log");
    let row_version = make_test_row_version((-2).into(), 1, &value, 333);
    let expected_record_bytes = row_version.row.payload().to_vec();
    let tx = crate::mvcc::database::LogRecord::for_test(333, &[row_version], None);
    write_first_encrypted_tx(base_file.clone(), &io, &enc_ctx, tx);
    let base_ops = parse_only_encrypted_tx_ops(base_file.clone(), &io, &enc_ctx);
    assert_eq!(base_ops.len(), 1);
    assert_upsert_table_op(&base_ops[0], (-2).into(), 1, &expected_record_bytes, 333);

    let base_bytes = read_file_bytes(base_file, &io);
    let payload_size = u64::from_le_bytes(
        base_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let chunk_ranges =
        encrypted_chunk_ranges(payload_size, enc_ctx.tag_size(), enc_ctx.nonce_size());
    assert!(
        chunk_ranges.len() >= 3,
        "expected at least 3 encrypted chunks for corruption coverage"
    );
    let frame_payload_start = LOG_HDR_SIZE + TX_HEADER_SIZE;
    let full_chunk_plaintext_len =
        encrypted_chunk_plaintext_len(payload_size, 1, ENCRYPTED_PAYLOAD_CHUNK_SIZE).unwrap();

    let mut cases: Vec<(&str, Vec<u8>, bool)> = Vec::new();

    // Corrupt ciphertext in chunk 2.
    {
        let mut bytes = base_bytes.clone();
        let offset = frame_payload_start + chunk_ranges[1].start + 1;
        bytes[offset] ^= 0xFF;
        cases.push(("ciphertext", bytes, false));
    }

    // Corrupt tag in chunk 2.
    {
        let mut bytes = base_bytes.clone();
        let offset = frame_payload_start + chunk_ranges[1].start + full_chunk_plaintext_len + 1;
        bytes[offset] ^= 0xFF;
        cases.push(("tag", bytes, false));
    }

    // Corrupt nonce in chunk 2.
    {
        let mut bytes = base_bytes.clone();
        let offset = frame_payload_start
            + chunk_ranges[1].start
            + full_chunk_plaintext_len
            + enc_ctx.tag_size();
        bytes[offset] ^= 0xFF;
        cases.push(("nonce", bytes, false));
    }

    // Reorder the first two full-size chunks.
    {
        let mut bytes = base_bytes.clone();
        let first = chunk_ranges[0].clone();
        let second = chunk_ranges[1].clone();
        let first_bytes =
            bytes[frame_payload_start + first.start..frame_payload_start + first.end].to_vec();
        let second_bytes =
            bytes[frame_payload_start + second.start..frame_payload_start + second.end].to_vec();
        bytes[frame_payload_start + first.start..frame_payload_start + first.end]
            .copy_from_slice(&second_bytes);
        bytes[frame_payload_start + second.start..frame_payload_start + second.end]
            .copy_from_slice(&first_bytes);
        cases.push(("reorder", bytes, false));
    }

    // Drop the middle chunk entirely.
    {
        let mut bytes = base_bytes.clone();
        let second = chunk_ranges[1].clone();
        bytes.drain(frame_payload_start + second.start..frame_payload_start + second.end);
        cases.push(("drop", bytes, true));
    }

    // Duplicate chunk 1 over chunk 2.
    {
        let mut bytes = base_bytes;
        let first = chunk_ranges[0].clone();
        let second = chunk_ranges[1].clone();
        let first_bytes =
            bytes[frame_payload_start + first.start..frame_payload_start + first.end].to_vec();
        bytes[frame_payload_start + second.start..frame_payload_start + second.end]
            .copy_from_slice(&first_bytes);
        cases.push(("duplicate", bytes, false));
    }

    for (label, bytes, allow_eof) in cases {
        let file = io
            .open_file(
                &format!("enc-chunk-integrity-{label}.db-log"),
                OpenFlags::Create,
                false,
            )
            .unwrap();
        overwrite_file_bytes(file.clone(), &io, &bytes);
        if allow_eof {
            let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
            reader.read_header(&io).unwrap();
            match io.block(|| reader.parse_next_transaction()).unwrap() {
                ParseResult::InvalidFrame | ParseResult::Eof => {}
                other => panic!("expected rejection for {label}, got {other:?}"),
            }
        } else {
            assert_single_frame_invalid(file, &io, enc_ctx.clone());
        }
    }
}

// Verifies a torn multi-chunk tail is ignored without losing the last fully written
// prefix frame that appears before the truncation point.
#[test]
fn test_encrypted_log_chunk_torn_tail_rejected() {
    init_tracing();
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("enc-chunk-torn-tail.db-log", OpenFlags::Create, false)
        .unwrap();
    let enc_ctx = test_enc_ctx();
    let table_id: MVTableId = (-2).into();
    let text_len = text_len_for_single_upsert_table_op_size(2 * ENCRYPTED_PAYLOAD_CHUNK_SIZE + 257);
    let value = "q".repeat(text_len);

    let mut log = LogicalLog::new(file.clone(), io.clone(), Some(enc_ctx.clone()));
    let first_row_version = make_test_row_version(table_id, 1, "prefix", 500);
    let expected_prefix_record_bytes = first_row_version.row.payload().to_vec();
    let first_tx = crate::mvcc::database::LogRecord::for_test(500, &[first_row_version], None);
    let c = log.log_tx(first_tx).unwrap();
    io.wait_for_completion(c).unwrap();
    let second_frame_start = log.offset as usize;

    let second_tx = crate::mvcc::database::LogRecord::for_test(
        600,
        &[make_test_row_version(table_id, 2, &value, 600)],
        None,
    );
    let c = log.log_tx(second_tx).unwrap();
    io.wait_for_completion(c).unwrap();

    let base_bytes = read_file_bytes(file.clone(), &io);
    let second_payload_size = u64::from_le_bytes(
        base_bytes[second_frame_start + 4..second_frame_start + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let chunk_ranges = encrypted_chunk_ranges(
        second_payload_size,
        enc_ctx.tag_size(),
        enc_ctx.nonce_size(),
    );
    assert!(chunk_ranges.len() >= 3);
    let second_payload_start = second_frame_start + TX_HEADER_SIZE;
    let second_chunk_plaintext_len =
        encrypted_chunk_plaintext_len(second_payload_size, 1, ENCRYPTED_PAYLOAD_CHUNK_SIZE)
            .unwrap();
    let second_chunk = chunk_ranges[1].clone();
    let last_chunk = chunk_ranges.last().unwrap().clone();
    let second_frame_end = base_bytes.len();

    let cuts = [
        second_payload_start + second_chunk.start + 17,
        second_payload_start + second_chunk.start + second_chunk_plaintext_len + enc_ctx.tag_size(),
        second_payload_start + second_chunk.end,
        second_frame_end - TX_TRAILER_SIZE + 3,
    ];

    for (idx, cut) in cuts.into_iter().enumerate() {
        let file = io
            .open_file(
                &format!("enc-chunk-torn-tail-{idx}.db-log"),
                OpenFlags::Create,
                false,
            )
            .unwrap();
        overwrite_file_bytes(file.clone(), &io, &base_bytes[..cut]);

        let mut reader = StreamingLogicalLogReader::new(file, Some(enc_ctx.clone()));
        reader.read_header(&io).unwrap();
        match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::Frame(frame) => {
                let ops = frame.ops;
                assert_eq!(ops.len(), 1);
                assert_upsert_table_op(&ops[0], (-2).into(), 1, &expected_prefix_record_bytes, 500);
            }
            other => panic!("expected prefix frame to survive, got {other:?}"),
        }
        match io.block(|| reader.parse_next_transaction()).unwrap() {
            ParseResult::Eof => {}
            other => panic!("expected Eof for torn multi-chunk frame, got {other:?}"),
        }
    }

    // Keep the last chunk variable used so the compiler notices if the range math changes.
    assert!(last_chunk.end > last_chunk.start);
}
