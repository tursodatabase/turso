use rustc_hash::FxHashSet as HashSet;

use super::*;
use crate::io::PlatformIO;
use crate::mvcc::clock::MvccClock;
use crate::mvcc::cursor::{CursorYieldPoint, MvccCursorType};
use crate::mvcc::persistent_storage::logical_log::{
    ENCRYPTED_PAYLOAD_CHUNK_SIZE, FRAME_MAGIC, LOG_HDR_SIZE,
};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::state_machine::{StateTransition, TransitionResult};
use crate::storage::sqlite3_ondisk::{
    checksum_wal, read_varint, write_varint, DatabaseHeader, WalHeader, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Mutex;
use crate::sync::RwLock;
use crate::{Buffer, Completion, DatabaseOpts, EncryptionKey, LimboError, OpenFlags};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const TX_HEADER_SIZE: usize = 24;
const TX_TRAILER_SIZE: usize = 8;

pub(crate) struct MvccTestDbNoConn {
    pub(crate) db: Option<Arc<Database>>,
    path: Option<String>,
    opts: DatabaseOpts,
    enc_opts: Option<crate::EncryptionOpts>,
    // Stored mainly to not drop the temp dir before the test is done.
    _temp_dir: Option<tempfile::TempDir>,
}
pub(crate) struct MvccTestDb {
    pub(crate) mvcc_store: Arc<MvStore<MvccClock>>,
    pub(crate) db: Arc<Database>,
    pub(crate) conn: Arc<Connection>,
}

#[derive(Debug)]
struct FixedYieldInjector {
    remaining: Mutex<HashSet<YieldPoint>>,
}

impl FixedYieldInjector {
    fn new(points: impl IntoIterator<Item = YieldPoint>) -> Arc<Self> {
        Arc::new(Self {
            remaining: Mutex::new(points.into_iter().collect()),
        })
    }
}

impl YieldInjector for FixedYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        self.remaining.lock().remove(&point)
    }
}

impl MvccTestDb {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();
        // Enable MVCC via PRAGMA
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let mvcc_store = db.get_mv_store().clone().unwrap();
        Self {
            mvcc_store,
            db,
            conn,
        }
    }
}

#[test]
fn mvcc_active_read_tx_blocks_vacuum_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let tx_id = db.mvcc_store.begin_tx(pager).unwrap();

    assert!(matches!(
        db.mvcc_store.try_begin_vacuum_gate(),
        Err(LimboError::Busy)
    ));

    db.mvcc_store.remove_tx(tx_id);
    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_active_write_tx_blocks_vacuum_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let tx_id = db
        .mvcc_store
        .begin_exclusive_tx(pager.clone(), None)
        .unwrap();

    assert!(matches!(
        db.mvcc_store.try_begin_vacuum_gate(),
        Err(LimboError::Busy)
    ));

    db.mvcc_store
        .rollback_tx(tx_id, pager, &db.conn, crate::MAIN_DB_ID);
    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_vacuum_gate_blocks_new_read_and_write_tx() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();

    db.mvcc_store.try_begin_vacuum_gate().unwrap();

    assert!(matches!(
        db.mvcc_store.begin_tx(pager.clone()),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        db.mvcc_store.begin_exclusive_tx(pager, None),
        Err(LimboError::Busy)
    ));

    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_reset_after_vacuum_installs_header_and_rootpages() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    db.conn.demote_to_mvcc_connection();
    db.conn.reparse_schema().unwrap();
    let schema = db.conn.schema.read().clone();
    db.conn.promote_to_regular_connection();
    let table_root = match schema.tables.get("t").expect("table t").as_ref() {
        Table::BTree(btree) => btree.root_page,
        _ => panic!("expected btree table"),
    };
    let index_root = schema
        .indexes
        .get("t")
        .and_then(|indexes| indexes.front())
        .map(|index| index.root_page)
        .expect("index idx_t_v");

    let mut header = DatabaseHeader::default();
    header.schema_cookie = 77.into();

    db.mvcc_store
        .global_header
        .write()
        .replace(DatabaseHeader::default());
    db.mvcc_store
        .insert_table_id_to_rootpage(MVTableId::from(-999_i64), Some(999));

    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.reset_after_vacuum(header, schema.as_ref());
    db.mvcc_store.release_vacuum_gate();

    assert_eq!(
        db.mvcc_store
            .with_header(|header| header.schema_cookie.get(), None)
            .unwrap(),
        77
    );
    assert_eq!(
        *db.mvcc_store
            .table_id_to_rootpage
            .get(&SQLITE_SCHEMA_MVCC_TABLE_ID)
            .expect("sqlite_schema mapping")
            .value(),
        Some(1)
    );
    assert_eq!(
        *db.mvcc_store
            .table_id_to_rootpage
            .get(&MVTableId::from(-(table_root)))
            .expect("table root mapping")
            .value(),
        Some(table_root as u64)
    );
    assert_eq!(
        *db.mvcc_store
            .table_id_to_rootpage
            .get(&MVTableId::from(-(index_root)))
            .expect("index root mapping")
            .value(),
        Some(index_root as u64)
    );
    assert!(
        db.mvcc_store
            .table_id_to_rootpage
            .get(&MVTableId::from(-999_i64))
            .is_none(),
        "stale root-page entries must be cleared"
    );
}

#[test]
fn mvcc_reset_after_vacuum_clears_checkpointed_empty_version_buckets() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();

    db.conn
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    db.conn
        .execute("UPDATE t SET v = 'z' WHERE id = 1")
        .unwrap();
    db.conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Normal MVCC checkpoint GC removes versions but can leave empty map
    // buckets behind; VACUUM reset must not preserve those stale keys.
    let checkpointed_row_ids = db
        .mvcc_store
        .rows
        .iter()
        .filter(|entry| entry.value().read().is_empty())
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    let checkpointed_index_ids = db
        .mvcc_store
        .index_rows
        .iter()
        .filter(|entry| {
            entry
                .value()
                .iter()
                .all(|row_entry| row_entry.value().read().is_empty())
        })
        .map(|entry| *entry.key())
        .collect::<Vec<_>>();
    assert!(
        !checkpointed_row_ids.is_empty(),
        "checkpoint GC should leave empty table row buckets before VACUUM reset"
    );
    assert!(
        !checkpointed_index_ids.is_empty(),
        "checkpoint GC should leave empty index buckets before VACUUM reset"
    );

    db.conn.demote_to_mvcc_connection();
    db.conn.reparse_schema().unwrap();
    let schema = db.conn.schema.read().clone();
    db.conn.promote_to_regular_connection();

    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store
        .reset_after_vacuum(DatabaseHeader::default(), schema.as_ref());
    db.mvcc_store.release_vacuum_gate();

    for row_id in checkpointed_row_ids {
        assert!(
            db.mvcc_store.rows.get(&row_id).is_none(),
            "checkpointed empty table row buckets must be cleared across VACUUM reset"
        );
    }
    for index_id in checkpointed_index_ids {
        assert!(
            db.mvcc_store.index_rows.get(&index_id).is_none(),
            "checkpointed empty index buckets must be cleared across VACUUM reset"
        );
    }
}

impl MvccTestDbNoConn {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let opts = DatabaseOpts::new();
        let db = Database::open_file_with_flags(io, ":memory:", OpenFlags::default(), opts, None)
            .unwrap();
        // Enable MVCC via PRAGMA
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: None,
            opts,
            enc_opts: None,
            _temp_dir: None,
        }
    }

    /// Opens a database with a file
    pub fn new_with_random_db() -> Self {
        Self::new_with_random_db_with_opts(DatabaseOpts::new())
    }

    /// Opens a database with a file and the requested options.
    pub fn new_with_random_db_with_opts(opts: DatabaseOpts) -> Self {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("test_{}", rand::random::<u64>()));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let io = Arc::new(PlatformIO::new().unwrap());
        println!("path: {}", path.as_os_str().to_str().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
        )
        .unwrap();
        // Enable MVCC via PRAGMA
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: None,
            _temp_dir: Some(temp_dir),
        }
    }

    /// Opens a file-backed encrypted database with the given hex key.
    pub fn new_encrypted(hex_key: &str) -> Self {
        let opts = DatabaseOpts::new().with_encryption(true);
        let enc_opts = crate::EncryptionOpts {
            cipher: "aes256gcm".to_string(),
            hexkey: hex_key.to_string(),
        };
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            Some(enc_opts.clone()),
        )
        .unwrap();
        let encryption_key = EncryptionKey::from_hex_string(hex_key).unwrap();
        let conn = db.connect_with_encryption(Some(encryption_key)).unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: Some(enc_opts),
            _temp_dir: Some(temp_dir),
        }
    }

    /// Restarts the database, make sure there is no connection to the database open before calling this!
    pub fn restart(&mut self) {
        self.restart_result().unwrap();
    }

    /// Creates a file-backed MVCC test database, randomly picking a cipher
    /// when `encrypted` is true.
    pub fn new_maybe_encrypted(encrypted: bool) -> Self {
        if !encrypted {
            return Self::new_with_random_db();
        }
        const KEY128: &str = "b1bbfda4f589dc9daaf004fe21111e00";
        const KEY256: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
        let ciphers: &[(&str, &str)] = &[
            ("aes128gcm", KEY128),
            ("aes256gcm", KEY256),
            ("aegis128l", KEY128),
            ("aegis128x2", KEY128),
            ("aegis128x4", KEY128),
            ("aegis256", KEY256),
            ("aegis256x2", KEY256),
            ("aegis256x4", KEY256),
        ];
        let (cipher, hexkey) = ciphers[rand::random_range(0..ciphers.len())];
        Self::new_encrypted_with_cipher(hexkey, cipher)
    }

    fn new_encrypted_with_cipher(hex_key: &str, cipher: &str) -> Self {
        let opts = DatabaseOpts::new().with_encryption(true);
        let enc_opts = crate::EncryptionOpts {
            cipher: cipher.to_string(),
            hexkey: hex_key.to_string(),
        };
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            Some(enc_opts.clone()),
        )
        .unwrap();
        let encryption_key = EncryptionKey::from_hex_string(hex_key).unwrap();
        let conn = db.connect_with_encryption(Some(encryption_key)).unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: Some(enc_opts),
            _temp_dir: Some(temp_dir),
        }
    }

    /// Like `restart`, but returns the error instead of panicking.
    /// Useful for testing wrong-key scenarios.
    pub fn restart_result(&mut self) -> crate::Result<()> {
        // First let's clear any entries in database manager in order to force restart.
        // If not, we will load the same database instance again.
        {
            let mut manager = DATABASE_MANAGER.lock();
            manager.clear();
        }
        // Now open again.
        let io = Arc::new(PlatformIO::new().unwrap());
        let path = self.path.as_ref().unwrap();
        let db = Database::open_file_with_flags(
            io,
            path,
            OpenFlags::default(),
            self.opts,
            self.enc_opts.clone(),
        )?;
        self.db.replace(db);
        Ok(())
    }

    /// Asumes there is a database open
    pub fn get_db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }

    pub fn connect(&self) -> Arc<Connection> {
        let enc_key = self
            .enc_opts
            .as_ref()
            .map(|e| EncryptionKey::from_hex_string(&e.hexkey).unwrap());
        self.get_db().connect_with_encryption(enc_key).unwrap()
    }

    pub fn get_mvcc_store(&self) -> Arc<MvStore<MvccClock>> {
        self.get_db().get_mv_store().clone().unwrap()
    }
}

pub(crate) fn generate_simple_string_row(table_id: MVTableId, id: i64, data: &str) -> Row {
    let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1);
    Row::new_table_row(
        RowID::new(table_id, RowKey::Int(id)),
        record.as_blob().to_vec(),
        1,
    )
}

pub(crate) fn generate_simple_string_record(data: &str) -> ImmutableRecord {
    ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1)
}

fn advance_checkpoint_until_wal_has_commit_frame(
    mvcc_store: Arc<MvStore<MvccClock>>,
    conn: &Arc<Connection>,
) {
    let pager = conn.pager.load().clone();
    let initial_wal_max_frame = pager
        .wal
        .as_ref()
        .expect("mvcc mode requires wal")
        .get_max_frame_in_wal();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store,
        conn.clone(),
        true,
        conn.get_sync_mode(),
    );

    for _ in 0..10_000 {
        if pager
            .wal
            .as_ref()
            .expect("mvcc mode requires wal")
            .get_max_frame_in_wal()
            > initial_wal_max_frame
        {
            return;
        }

        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finalized before WAL had committed frames")
            }
        }
    }

    panic!("checkpoint did not produce committed WAL frame in bounded steps");
}

fn overwrite_log_header_byte(path: &str, offset: u64, value: u8) {
    let log_path = std::path::Path::new(path).with_extension("db-log");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(log_path)
        .unwrap();
    use std::io::{Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[value]).unwrap();
    file.sync_all().unwrap();
}

fn overwrite_file_with_junk(path: &std::path::Path, size: usize, byte: u8) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    let payload = vec![byte; size];
    use std::io::Write;
    file.write_all(&payload).unwrap();
    file.sync_all().unwrap();
}

fn wal_path_for_db(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{path}-wal"))
}

fn force_close_for_artifact_tamper(db: &mut MvccTestDbNoConn) {
    db.db.take();
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
}

fn read_db_page_size(path: &str) -> usize {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut header = [0u8; 100];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut header).unwrap();
    let raw = u16::from_be_bytes([header[16], header[17]]);
    if raw == 1 {
        65536
    } else {
        raw as usize
    }
}

fn page_file_offset(page_no: u32, page_size: usize) -> u64 {
    (page_no as u64 - 1) * page_size as u64
}

fn page_header_offset(page_no: u32) -> usize {
    if page_no == 1 {
        100
    } else {
        0
    }
}

fn read_db_page(path: &str, page_no: u32, page_size: usize) -> Vec<u8> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut page = vec![0u8; page_size];
    file.seek(SeekFrom::Start(page_file_offset(page_no, page_size)))
        .unwrap();
    file.read_exact(&mut page).unwrap();
    page
}

fn write_db_page(path: &str, page_no: u32, page_size: usize, page: &[u8]) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(page_file_offset(page_no, page_size)))
        .unwrap();
    file.write_all(page).unwrap();
    file.sync_all().unwrap();
}

#[derive(Debug, Clone, Copy)]
struct TableLeafCellLoc {
    cell_offset: usize,
    payload_varint_len: usize,
    payload_len: usize,
    payload_offset: usize,
}

fn table_leaf_cell_locs(page: &[u8], page_no: u32) -> Vec<TableLeafCellLoc> {
    let hdr_off = page_header_offset(page_no);
    assert_eq!(page[hdr_off], 0x0D, "expected table-leaf page type");
    let cell_count = u16::from_be_bytes([page[hdr_off + 3], page[hdr_off + 4]]) as usize;
    let ptr_base = hdr_off + 8;
    let mut locs = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let ptr_off = ptr_base + i * 2;
        let cell_ptr = u16::from_be_bytes([page[ptr_off], page[ptr_off + 1]]) as usize;
        let (payload_len_u64, payload_varint_len) = read_varint(&page[cell_ptr..]).unwrap();
        let payload_len = payload_len_u64 as usize;
        let (_, rowid_varint_len) = read_varint(&page[cell_ptr + payload_varint_len..]).unwrap();
        let payload_offset = cell_ptr + payload_varint_len + rowid_varint_len;
        locs.push(TableLeafCellLoc {
            cell_offset: cell_ptr,
            payload_varint_len,
            payload_len,
            payload_offset,
        });
    }
    locs
}

fn table_leaf_first_cell_loc(page: &[u8], page_no: u32) -> TableLeafCellLoc {
    let locs = table_leaf_cell_locs(page, page_no);
    let cell_count = locs.len();
    assert!(
        cell_count > 0,
        "expected at least one cell in metadata page"
    );
    locs[0]
}

fn rewrite_table_leaf_cell_payload(page: &mut [u8], loc: TableLeafCellLoc, new_payload: &[u8]) {
    assert!(
        new_payload.len() <= loc.payload_len,
        "new payload {} exceeds existing payload {}",
        new_payload.len(),
        loc.payload_len
    );
    let mut varint_buf = [0u8; 9];
    let n = write_varint(&mut varint_buf, new_payload.len() as u64);
    assert_eq!(
        n, loc.payload_varint_len,
        "payload varint length changed; in-place rewrite is unsafe"
    );
    page[loc.cell_offset..loc.cell_offset + n].copy_from_slice(&varint_buf[..n]);
    page[loc.payload_offset..loc.payload_offset + new_payload.len()].copy_from_slice(new_payload);
    if new_payload.len() < loc.payload_len {
        page[loc.payload_offset + new_payload.len()..loc.payload_offset + loc.payload_len].fill(0);
    }
}

fn tamper_table_leaf_value_serial_type(page: &mut [u8], page_no: u32, new_serial_type: u8) -> bool {
    let loc = table_leaf_first_cell_loc(page, page_no);
    let payload = &mut page[loc.payload_offset..loc.payload_offset + loc.payload_len];

    let (header_size, hs_len) = read_varint(payload).unwrap();
    let header_size = header_size as usize;
    if header_size < hs_len + 2 || header_size > payload.len() {
        return false;
    }

    let mut idx = hs_len;
    let (_serial_type0, n0) = read_varint(&payload[idx..header_size]).unwrap();
    idx += n0;
    if idx >= header_size {
        return false;
    }
    payload[idx] = new_serial_type;
    true
}

fn wipe_table_leaf_cells(page: &mut [u8], page_no: u32) -> bool {
    let hdr_off = page_header_offset(page_no);
    if page.len() <= hdr_off + 8 || page[hdr_off] != 0x0D {
        return false;
    }
    let page_size = page.len();
    page[hdr_off + 3..hdr_off + 5].copy_from_slice(&0u16.to_be_bytes()); // number of cells
    page[hdr_off + 5..hdr_off + 7].copy_from_slice(&(page_size as u16).to_be_bytes()); // cell content area start
    page[hdr_off + 7] = 0; // fragmented free bytes
    true
}

fn metadata_root_page(conn: &Arc<Connection>) -> u32 {
    let rows = get_rows(
        conn,
        "SELECT rootpage FROM sqlite_schema
         WHERE type = 'table' AND name = '__turso_internal_mvcc_meta'",
    );
    assert_eq!(rows.len(), 1, "expected exactly one metadata table row");
    rows[0][0].as_int().unwrap() as u32
}

fn tamper_db_metadata_row_value(db_path: &str, metadata_root_page: u32, new_value: i64) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    let loc = table_leaf_first_cell_loc(&page, metadata_root_page);
    let payload = &page[loc.payload_offset..loc.payload_offset + loc.payload_len];
    let record = ImmutableRecord::from_bin_record(payload.to_vec());
    let key = record
        .get_value_opt(0)
        .expect("metadata key column missing");
    let ValueRef::Text(key) = key else {
        panic!("metadata key must be text");
    };
    let new_record = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new(key.as_str().to_string())),
            Value::from_i64(new_value),
        ],
        2,
    );
    rewrite_table_leaf_cell_payload(&mut page, loc, new_record.as_blob());
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_db_metadata_row_value_by_key(
    db_path: &str,
    metadata_root_page: u32,
    target_key: &str,
    new_value: i64,
) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    let mut updated = false;
    for loc in table_leaf_cell_locs(&page, metadata_root_page) {
        let payload = &page[loc.payload_offset..loc.payload_offset + loc.payload_len];
        let record = ImmutableRecord::from_bin_record(payload.to_vec());
        let key = record
            .get_value_opt(0)
            .expect("metadata key column missing");
        let ValueRef::Text(key) = key else {
            panic!("metadata key must be text");
        };
        if key.as_str() != target_key {
            continue;
        }
        let new_record = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(target_key.to_string())),
                Value::from_i64(new_value),
            ],
            2,
        );
        rewrite_table_leaf_cell_payload(&mut page, loc, new_record.as_blob());
        updated = true;
    }
    assert!(updated, "expected metadata key {target_key} to exist");
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_db_metadata_value_serial_type(
    db_path: &str,
    metadata_root_page: u32,
    new_serial_type: u8,
) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    assert!(
        tamper_table_leaf_value_serial_type(&mut page, metadata_root_page, new_serial_type),
        "expected metadata serial-type tamper to succeed"
    );
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_db_metadata_row_key(db_path: &str, metadata_root_page: u32, new_key: &str) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    let loc = table_leaf_first_cell_loc(&page, metadata_root_page);
    let payload = &page[loc.payload_offset..loc.payload_offset + loc.payload_len];
    let record = ImmutableRecord::from_bin_record(payload.to_vec());
    let value = record
        .get_value_opt(1)
        .expect("metadata value column missing");
    let ValueRef::Numeric(Numeric::Integer(value)) = value else {
        panic!("metadata value must be integer");
    };
    let new_record = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new(new_key.to_string())),
            Value::from_i64(value),
        ],
        2,
    );
    rewrite_table_leaf_cell_payload(&mut page, loc, new_record.as_blob());
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_wal_metadata_value_serial_type(
    wal_path: &std::path::Path,
    metadata_root_page: u32,
    new_serial_type: u8,
) -> bool {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path)
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    if bytes.len() < WAL_HEADER_SIZE {
        return false;
    }

    let header = WalHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        file_format: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        page_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        checkpoint_seq: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        salt_1: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        salt_2: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        checksum_1: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
        checksum_2: u32::from_be_bytes(bytes[28..32].try_into().unwrap()),
    };
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size as usize;
    let mut frame_offset = WAL_HEADER_SIZE;
    let mut prev_checksums = (header.checksum_1, header.checksum_2);
    let mut mutated = false;

    while frame_offset + frame_size <= bytes.len() {
        let frame = &mut bytes[frame_offset..frame_offset + frame_size];
        let page_no = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        if page_no == metadata_root_page {
            let page_image = &mut frame
                [WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize];
            let hdr_off = page_header_offset(metadata_root_page);
            if page_image.len() > hdr_off + 5 && page_image[hdr_off] == 0x0D {
                let cell_count =
                    u16::from_be_bytes([page_image[hdr_off + 3], page_image[hdr_off + 4]]);
                if cell_count > 0
                    && tamper_table_leaf_value_serial_type(
                        page_image,
                        metadata_root_page,
                        new_serial_type,
                    )
                {
                    mutated = true;
                }
            }
        }

        let header_checksum =
            checksum_wal(&frame[0..8], &header, prev_checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize],
            &header,
            header_checksum,
            use_native_endian,
        );
        frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
        frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
        prev_checksums = final_checksum;
        frame_offset += frame_size;
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    mutated
}

fn tamper_wal_metadata_page_empty(wal_path: &std::path::Path, metadata_root_page: u32) -> bool {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path)
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    if bytes.len() < WAL_HEADER_SIZE {
        return false;
    }

    let header = WalHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        file_format: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        page_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        checkpoint_seq: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        salt_1: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        salt_2: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        checksum_1: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
        checksum_2: u32::from_be_bytes(bytes[28..32].try_into().unwrap()),
    };
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size as usize;
    let mut frame_offset = WAL_HEADER_SIZE;
    let mut prev_checksums = (header.checksum_1, header.checksum_2);
    let mut mutated = false;

    while frame_offset + frame_size <= bytes.len() {
        let frame = &mut bytes[frame_offset..frame_offset + frame_size];
        let page_no = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        if page_no == metadata_root_page {
            let page_image = &mut frame
                [WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize];
            if wipe_table_leaf_cells(page_image, metadata_root_page) {
                mutated = true;
            }
        }

        let header_checksum =
            checksum_wal(&frame[0..8], &header, prev_checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize],
            &header,
            header_checksum,
            use_native_endian,
        );
        frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
        frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
        prev_checksums = final_checksum;
        frame_offset += frame_size;
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    mutated
}

fn rewrite_wal_frames_as_non_commit(path: &std::path::Path) {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() >= WAL_HEADER_SIZE);

    let header = WalHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        file_format: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        page_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        checkpoint_seq: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        salt_1: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        salt_2: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        checksum_1: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
        checksum_2: u32::from_be_bytes(bytes[28..32].try_into().unwrap()),
    };
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size as usize;
    let mut frame_offset = WAL_HEADER_SIZE;
    let mut prev_checksums = (header.checksum_1, header.checksum_2);

    while frame_offset + frame_size <= bytes.len() {
        let frame = &mut bytes[frame_offset..frame_offset + frame_size];
        frame[4..8].copy_from_slice(&0u32.to_be_bytes());
        let header_checksum =
            checksum_wal(&frame[0..8], &header, prev_checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize],
            &header,
            header_checksum,
            use_native_endian,
        );
        frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
        frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
        prev_checksums = final_checksum;
        frame_offset += frame_size;
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_recovery_clock_monotonicity() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let max_commit_ts = {
        let conn = db.connect();
        conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        conn.execute("INSERT INTO test(id, data) VALUES (1, 'foo')")
            .unwrap();
        let mvcc_store = db.get_mvcc_store();
        mvcc_store.last_committed_tx_ts.load(Ordering::SeqCst)
    };

    db.restart();
    let conn = db.connect();
    let pager = conn.pager.load().clone();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(pager).unwrap();
    let tx_entry = mvcc_store
        .txs
        .get(&tx_id)
        .expect("transaction should exist");
    let tx = tx_entry.value();
    assert!(
        tx.begin_ts > max_commit_ts,
        "expected begin_ts {} to be > max_commit_ts {}",
        tx.begin_ts,
        max_commit_ts
    );
}

/// What this test checks: Recovery stops cleanly at a torn/incomplete tail and keeps all previously validated frames.
/// Why this matters: Crashes can leave partial writes at EOF; we need durable-prefix recovery, not all-or-nothing failure.
#[test]
fn test_recover_logical_log_short_file_ignored() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let file = mvcc_store.get_logical_log_file();

    let c = file.truncate(1, Completion::new_write(|_| {})).unwrap();
    conn.db.io.wait_for_completion(c).unwrap();

    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(vec![0xAB])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    conn.db.io.wait_for_completion(c).unwrap();
    assert_eq!(file.size().unwrap(), 1);

    let recovered = mvcc_store.maybe_recover_logical_log(conn).unwrap();
    assert!(!recovered);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_journal_mode_switch_from_mvcc_to_wal_without_log_frames() {
    let db = MvccTestDb::new();
    let rows = get_rows(&db.conn, "PRAGMA journal_mode = 'wal'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string().to_lowercase(), "wal");
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[turso_macros::test(encryption)]
fn test_recovery_checkpoint_then_more_writes() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

/// What this test checks: MVCC restart handles sqlite_schema rows with rootpage=0 (triggers).
/// Why this matters: Trigger definitions are stored without btrees and should not break recovery.
#[test]
fn test_restart_with_trigger_rootpage_zero() {
    let mut db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new());
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, action TEXT)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER trg_del AFTER DELETE ON t1 \
             BEGIN INSERT INTO audit VALUES (NULL, 'deleted'); END;",
        )
        .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'x')").unwrap();
        conn.close().unwrap();
    }

    db.restart();

    {
        let conn = db.connect();
        conn.execute("DELETE FROM t1 WHERE id = 1").unwrap();
        let rows = get_rows(&conn, "SELECT action FROM audit ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_string(), "deleted");
    }
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[turso_macros::test(encryption)]
fn test_btree_resident_recovery_then_checkpoint_delete_stays_deleted() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'gone')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Delete a B-tree resident row and crash/restart before checkpoint.
    {
        let conn = db.connect();
        conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        // Recovery tombstone must hide stale B-tree row before checkpoint.
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);

        // After checkpoint + GC, row must stay deleted (B-tree delete persisted).
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);

        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].to_string(), "ok");
    }
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_recovery_overwrites_torn_tail_on_next_append() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    }

    // Corrupt only the tail of the latest frame.
    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let file = mvcc_store.get_logical_log_file();
        let size = file.size().unwrap();
        assert!(size > 1);
        let c = file
            .truncate(size - 1, Completion::new_trunc(|_| {}))
            .unwrap();
        conn.db.io.wait_for_completion(c).unwrap();
    }

    // First restart: recovery should stop at torn tail and reset log write offset.
    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    // Second restart: row 3 must be recoverable, proving it was appended at last_valid_offset.
    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
        assert_eq!(rows[0][1].to_string(), "a");
        assert_eq!(rows[1][0].as_int().unwrap(), 3);
        assert_eq!(rows[1][1].to_string(), "c");
    }
}

/// What this test checks: First-time MVCC bootstrap repairs a torn short `.db-log` header before metadata writes commit.
/// Why this matters: Otherwise a crash after metadata WAL commit can leave an unrecoverable startup state.
#[test]
#[ignore = "Needs a dedicated bootstrap harness that can create header=MVCC + missing metadata + torn short log atomically"]
fn test_bootstrap_repairs_torn_short_log_before_metadata_init() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join(format!("bootstrap_torn_{}", rand::random::<u64>()));
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, &db_path_str).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.close().unwrap();
    }

    let log_path = std::path::Path::new(&db_path_str).with_extension("db-log");
    overwrite_file_with_junk(&log_path, LOG_HDR_SIZE / 2, 0xAB);

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    {
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, &db_path_str).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
    }

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, &db_path_str).unwrap();
    let conn = db.connect().unwrap();
    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0][0].as_int().unwrap(), 0);

    let log_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        log_len >= LOG_HDR_SIZE as u64,
        "expected bootstrap to rewrite durable logical-log header"
    );
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_completes_interrupted_checkpoint_with_committed_wal() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);

        let pager = conn.pager.load().clone();
        assert!(
            pager
                .wal
                .as_ref()
                .expect("wal must exist")
                .get_max_frame_in_wal()
                > 0
        );
        let log_file = db.get_mvcc_store().get_logical_log_file();
        assert!(log_file.size().unwrap() > LOG_HDR_SIZE as u64);
    }

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");

    let log_size = db.get_mvcc_store().get_logical_log_file().size().unwrap();
    assert!(
        log_size >= LOG_HDR_SIZE as u64,
        "logical log must be at least {LOG_HDR_SIZE} bytes after interrupted-checkpoint reconciliation"
    );
    let wal_path = wal_path_for_db(&db_path);
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_len, 0);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_checkpoint_truncates_wal_last() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
    );

    let mut saw_truncate_log_state_with_wal = false;
    let mut finished = false;
    for _ in 0..50_000 {
        let state = checkpoint_sm.state_for_test();

        if state == CheckpointState::TruncateLogicalLog {
            let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
            assert!(wal_len > 0, "WAL must still exist before log truncation");
            saw_truncate_log_state_with_wal = true;
        }

        if state == CheckpointState::TruncateWal {
            assert!(
                saw_truncate_log_state_with_wal,
                "must truncate logical log before truncating WAL"
            );
            assert_eq!(
                mvcc_store.get_logical_log_file().size().unwrap(),
                0,
                "logical log should be truncated to 0"
            );
        }

        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                finished = true;
                break;
            }
        }
    }

    assert!(finished, "checkpoint state machine did not finish");
    assert!(saw_truncate_log_state_with_wal);

    let final_wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(final_wal_len, 0);
    assert_eq!(
        mvcc_store.get_logical_log_file().size().unwrap(),
        0,
        "logical log should be truncated to 0 after checkpoint"
    );
}

/// What this test checks: Checkpoint accepts sqlite_schema index-row updates for already-checkpointed indexes
/// (e.g. column rename), without requiring create/destroy special writes.
/// Why this matters: RENAME COLUMN on indexed tables rewrites sqlite_schema index SQL text while preserving rootpage.
/// Treating that as an impossible state crashes checkpoint.
#[test]
fn test_checkpoint_allows_index_schema_update_after_rename_column() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_a ON t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Rewrites sqlite_schema entry for the existing index while keeping positive rootpage.
    conn.execute("ALTER TABLE t RENAME COLUMN a TO c").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT c, b FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 2);
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_rejects_committed_wal_without_log_file() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }

    let log_path = std::path::Path::new(&db_path).with_extension("db-log");
    std::fs::remove_file(&log_path).unwrap();

    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path) {
        Ok(db) => match db.connect() {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_rejects_torn_log_header_with_committed_wal() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'y')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    overwrite_log_header_byte(&db_path, 0, 0x00);

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }

    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path) {
        Ok(db) => match db.connect() {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert!(
        wal_len > 0,
        "failed bootstrap must not truncate WAL before header validation"
    );
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_rejects_corrupt_log_header_without_wal() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    }

    overwrite_log_header_byte(&db_path, 0, 0x00);

    {
        let wal_path = wal_path_for_db(&db_path);
        let _ = std::fs::remove_file(&wal_path);
        overwrite_file_with_junk(&wal_path, 0, 0x00);
    }

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }

    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path) {
        Ok(db) => match db.connect() {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_handles_committed_wal_when_log_truncated() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store.clone(), &conn);

        let log_file = mvcc_store.get_logical_log_file();
        let c = log_file
            .truncate(LOG_HDR_SIZE as u64, Completion::new_trunc(|_| {}))
            .unwrap();
        conn.db.io.wait_for_completion(c).unwrap();
    }

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");

    let log_size = db.get_mvcc_store().get_logical_log_file().size().unwrap();
    assert_eq!(log_size, LOG_HDR_SIZE as u64);
    let wal_path = wal_path_for_db(&db_path);
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_len, 0);
}

/// What this test checks: WAL frames without a commit marker are treated as non-committed tail and ignored.
/// Why this matters: Recovery must preserve availability by discarding invalid WAL tail bytes instead of failing startup.
#[test]
fn test_bootstrap_ignores_wal_frames_without_commit_marker() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    rewrite_wal_frames_as_non_commit(&wal_path);
    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    let db2 = Database::open_file(io, &db_path).expect("open should succeed");
    let conn2 = db2.connect().expect("connect should succeed");
    let rows = get_rows(&conn2, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "x");
}

/// What this test checks: Recovery after checkpoint (empty log) seeds new tx timestamps above durable metadata boundary.
/// Why this matters: Timestamp rewind below checkpointed boundary would break MVCC ordering.
#[test]
fn test_empty_log_recovery_loads_checkpoint_watermark() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let persistent_tx_ts_max = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(
            mvcc_store.get_logical_log_file().size().unwrap(),
            0,
            "logical log should be truncated to 0 after checkpoint"
        );
        let meta = get_rows(
            &conn,
            "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
        );
        assert_eq!(meta.len(), 1);
        meta[0][0].as_int().unwrap() as u64
    };

    db.restart();
    let conn = db.connect();
    let pager = conn.pager.load().clone();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(pager).unwrap();
    let tx_entry = mvcc_store
        .txs
        .get(&tx_id)
        .expect("transaction should exist");
    assert!(
        tx_entry.value().begin_ts > persistent_tx_ts_max,
        "expected begin_ts {} > persistent_tx_ts_max {}",
        tx_entry.value().begin_ts,
        persistent_tx_ts_max
    );
}

/// TDD recovery/checkpoint matrix for metadata-table source of truth.
///
/// Proposed semantics under test:
/// - Source of truth for replay boundary is internal SQLite table
///   `turso_internal_mvcc_meta` with `persistent_tx_ts_max`.
/// - Logical-log header carries no replay timestamps.
/// - On startup with committed WAL frames, recovery must reconcile WAL first, then read metadata.
///
/// Enumerated cases:
/// 1. No committed WAL + no logical-log frames + metadata row present.
/// 2. No committed WAL + logical-log frames + metadata row present -> replay `ts > persistent_tx_ts_max`.
/// 3. No committed WAL + logical-log frames + metadata row missing/corrupt -> fail closed.
/// 4. Committed WAL + metadata row present -> reconcile WAL first, then replay above metadata boundary.
/// 5. Committed WAL + metadata row missing -> fail closed.
/// 6. Committed WAL + metadata row malformed/corrupt -> fail closed.
/// 7. Metadata table exists but has duplicate rows/invalid key shape -> fail closed.
/// 8. User tampered metadata row downward -> detect and fail closed.
/// 9. User deleted metadata row -> detect and fail closed.
/// 10. Checkpoint pager commit atomically upserts metadata row in same WAL txn.
/// 11. Auto-checkpoint failure after pager commit keeps COMMIT result stable and recoverable.
/// 12. Replay gate correctness: never apply `commit_ts <= persistent_tx_ts_max`.
#[test]
fn test_meta_recovery_case_1_no_wal_no_log_metadata_present_clean_boot() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let log_path = std::path::Path::new(&db_path).with_extension("db-log");

    {
        let conn = db.connect();
        let rows = get_rows(
            &conn,
            "SELECT k, v FROM __turso_internal_mvcc_meta ORDER BY rowid",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_string(), "persistent_tx_ts_max");
        assert_eq!(rows[0][1].as_int().unwrap(), 0);
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT k, v FROM __turso_internal_mvcc_meta ORDER BY rowid",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "persistent_tx_ts_max");
    assert_eq!(rows[0][1].as_int().unwrap(), 0);

    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        wal_len, 0,
        "expected no committed WAL tail after clean boot"
    );
    let log_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        log_len, LOG_HDR_SIZE as u64,
        "expected logical log to be {LOG_HDR_SIZE} bytes (bootstrap header) on clean boot"
    );
}

/// What this test checks: With no committed WAL and metadata present, replay includes only frames above `persistent_tx_ts_max`.
/// Why this matters: This is the core idempotency contract for logical-log replay.
#[turso_macros::test(encryption)]
fn test_meta_recovery_case_2_no_wal_replay_above_metadata_boundary() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let meta = get_rows(
            &conn,
            "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
        );
        assert_eq!(meta.len(), 1);
        let boundary = meta[0][0].as_int().unwrap();
        assert!(
            boundary >= 2,
            "expected metadata boundary >= 2 after checkpoint, got {boundary}"
        );

        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

/// What this test checks: Header-only commits are durably replayed from the logical log and
/// then persisted into the database header by checkpoint.
/// Why this matters: PRAGMA header mutations (for example user_version) must survive restart
/// both before and after log truncation, including implicit autocommit statement transactions.
#[turso_macros::test(encryption)]
fn test_header_only_mutation_is_replayed_and_checkpointed() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);

    {
        let conn = db.connect();
        conn.execute("PRAGMA user_version = 42").unwrap();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 42);
    }

    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_int().unwrap(),
            42,
            "header mutation should recover from logical log before checkpoint",
        );
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_int().unwrap(),
            42,
            "header mutation should persist in DB header after checkpoint truncates logical log",
        );
    }
}

/// What this test checks: Header PRAGMAs in MVCC require an exclusive transaction and reject
/// BEGIN CONCURRENT writes.
/// Why this matters: Header updates have no row-level conflict keys, so they must not run under
/// optimistic concurrent write mode.
#[test]
fn test_mvcc_header_updates_require_exclusive_transaction() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let err = conn.execute("PRAGMA user_version = 42").unwrap_err();
    assert!(
        err.to_string().contains("exclusive transaction"),
        "expected exclusive-transaction error, got: {err:?}"
    );
    conn.execute("ROLLBACK").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("PRAGMA user_version = 7").unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "PRAGMA user_version");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 7);
}

/// What this test checks: Header PRAGMAs in MVCC succeed in autocommit mode, where the VM
/// opens an implicit single-statement write transaction.
/// Why this matters: The exclusive-transaction gate must block BEGIN CONCURRENT, but not reject
/// valid autocommit writes that are internally upgraded to exclusive write mode.
#[test]
fn test_mvcc_header_updates_allow_autocommit_statement_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("PRAGMA user_version = 19").unwrap();

    let rows = get_rows(&conn, "PRAGMA user_version");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 19);
}

/// What this test checks: Missing/corrupt metadata with logical-log frames and no WAL causes fail-closed startup.
/// Why this matters: Without metadata boundary recovery cannot choose replay/discard safely.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_3_no_wal_log_frames_without_valid_metadata_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        metadata_root_page(&conn)
    };
    force_close_for_artifact_tamper(&mut db);
    tamper_db_metadata_row_value(&db_path, metadata_root_page, -1);
    let wal_path = wal_path_for_db(&db_path);
    let _ = std::fs::remove_file(&wal_path);
    overwrite_file_with_junk(&wal_path, 0, 0);

    {
        // Ensure cold open after artifact tamper.
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path) {
        Ok(db2) => match db2.connect() {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(
                matches!(err, LimboError::Corrupt(_)),
                "unexpected connect error: {err:?}"
            ),
        },
        Err(err) => assert!(
            matches!(err, LimboError::Corrupt(_)),
            "unexpected open error: {err:?}"
        ),
    }
}

/// What this test checks: Recovery reconciles committed WAL first, then applies logical-log replay boundary from metadata.
/// Why this matters: Ordering prevents double-apply and loss when WAL and logical log both exist.
#[test]
fn test_meta_recovery_case_4_committed_wal_reconcile_before_metadata_boundary_replay() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert!(
        meta[0][0].as_int().unwrap() >= 2,
        "expected replay boundary to advance after committed-WAL reconciliation",
    );

    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_len, 0, "reconciliation must truncate WAL at the end");
}

/// What this test checks: Committed WAL with missing metadata row fails closed.
/// Why this matters: Recovery cannot infer authoritative replay boundary from WAL bytes alone.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_5_committed_wal_missing_metadata_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        let root_page = metadata_root_page(&conn);
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
        root_page
    };
    force_close_for_artifact_tamper(&mut db);
    let mutated = tamper_wal_metadata_page_empty(&wal_path, metadata_root_page);
    assert!(
        mutated,
        "expected metadata WAL frame to be mutated into missing-row shape"
    );

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path) {
        Ok(db2) => match db2.connect() {
            Ok(_) => panic!("expected connect to fail closed"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: Committed WAL with malformed metadata row fails closed.
/// Why this matters: Corrupt internal metadata must never be interpreted best-effort.
#[test]
fn test_meta_recovery_case_6_committed_wal_corrupt_metadata_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        let root_page = metadata_root_page(&conn);
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
        root_page
    };
    force_close_for_artifact_tamper(&mut db);
    let mutated = tamper_wal_metadata_value_serial_type(&wal_path, metadata_root_page, 0);
    assert!(
        mutated,
        "expected at least one metadata WAL frame to be mutated"
    );

    {
        // Ensure cold open after artifact tamper.
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    if Database::open_file(io, &db_path).is_ok_and(|db2| db2.connect().is_ok()) {
        panic!("expected connect to fail closed")
    }
}

/// What this test checks: Invalid metadata-table shape (duplicates or bad key domain) fails closed.
/// Why this matters: Schema/shape corruption in internal state must not be silently tolerated.
#[test]
fn test_meta_recovery_case_7_metadata_table_shape_violation_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        metadata_root_page(&conn)
    };
    force_close_for_artifact_tamper(&mut db);
    tamper_db_metadata_value_serial_type(&db_path, metadata_root_page, 0);
    let wal_path = wal_path_for_db(&db_path);
    let _ = std::fs::remove_file(&wal_path);
    overwrite_file_with_junk(&wal_path, 0, 0);

    {
        // Ensure cold open after artifact tamper.
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    if Database::open_file(io, &db_path).is_ok_and(|db2| db2.connect().is_ok()) {
        panic!("expected connect to fail closed")
    }
}

/// What this test checks: Deletion of metadata row is detected and rejected.
/// Why this matters: Missing boundary metadata makes replay decision ambiguous.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_9_metadata_row_deleted_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        metadata_root_page(&conn)
    };
    force_close_for_artifact_tamper(&mut db);
    tamper_db_metadata_row_key(&db_path, metadata_root_page, "persistent_tx_ts_may");
    let wal_path = wal_path_for_db(&db_path);
    let _ = std::fs::remove_file(&wal_path);
    overwrite_file_with_junk(&wal_path, 0, 0);

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path) {
        Ok(db2) => match db2.connect() {
            Ok(_) => panic!("expected connect to fail closed"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: Checkpoint pager commit writes data pages and metadata row in the same WAL transaction.
/// Why this matters: This is the atomicity mechanism replacing logical-log header checkpoint timestamps.
#[test]
fn test_meta_checkpoint_case_10_metadata_upsert_is_atomic_with_pager_commit() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        let committed_ts = mvcc_store.last_committed_tx_ts.load(Ordering::SeqCst);
        assert!(committed_ts > 0);

        let pager = conn.pager.load().clone();
        let mut checkpoint_sm = CheckpointStateMachine::new(
            pager.clone(),
            mvcc_store,
            conn.clone(),
            true,
            conn.get_sync_mode(),
        );

        for _ in 0..50_000 {
            if checkpoint_sm.state_for_test() == CheckpointState::CheckpointWal {
                break;
            }
            match checkpoint_sm.step(&()).unwrap() {
                TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
                TransitionResult::Continue => {}
                TransitionResult::Done(_) => {
                    panic!("checkpoint finished before expected stop window")
                }
            }
        }
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert!(
        meta[0][0].as_int().unwrap() >= 1,
        "expected metadata boundary to persist with pager commit"
    );
}

/// What this test checks: Auto-checkpoint post-commit failure does not invalidate committed transaction visibility on restart.
/// Why this matters: Commit contract must remain stable even when checkpoint cleanup fails mid-flight.
#[test]
fn test_meta_checkpoint_case_11_auto_checkpoint_failure_after_commit_remains_recoverable() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let ts1 = mvcc_store.last_committed_tx_ts.load(Ordering::SeqCst);
    assert!(ts1 > 0, "expected committed timestamp for first insert");

    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
    );
    let mut reached_truncate = false;
    for _ in 0..50_000 {
        if checkpoint_sm.state_for_test() == CheckpointState::TruncateLogicalLog {
            reached_truncate = true;
            break; // Simulate checkpoint aborting before log truncation
        }
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before reaching truncate state")
            }
        }
    }
    assert!(
        reached_truncate,
        "expected to reach TruncateLogicalLog state"
    );

    // Pager commit already succeeded before log truncation.
    // Same-process retries must advance from this durable boundary.
    let durable_boundary = mvcc_store.durable_txid_max.load(Ordering::SeqCst);
    assert!(
        durable_boundary >= ts1,
        "expected in-memory durable checkpoint boundary to advance after pager commit: boundary={durable_boundary} ts1={ts1}"
    );

    let sync_mode = conn.get_sync_mode();
    let checkpoint_sm2 = CheckpointStateMachine::new(pager, mvcc_store, conn, true, sync_mode);
    let (old_boundary, _) = checkpoint_sm2.checkpoint_bounds_for_test();
    assert!(
        old_boundary.unwrap_or_default() >= ts1,
        "expected retry checkpoint to start from durable boundary: old={old_boundary:?} ts1={ts1}"
    );
}

/// What this test checks: Replay gate uses metadata boundary and never applies frames at or below it.
/// Why this matters: This enforces exactly-once effects at the DB-file apply boundary.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_12_replay_gate_skips_at_or_below_metadata_boundary() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let boundary = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let root_page = metadata_root_page(&conn);
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        let ts3 = db
            .get_mvcc_store()
            .last_committed_tx_ts
            .load(Ordering::SeqCst);
        drop(conn);
        force_close_for_artifact_tamper(&mut db);
        tamper_db_metadata_row_value_by_key(
            &db_path,
            root_page,
            MVCC_META_KEY_PERSISTENT_TX_TS_MAX,
            ts3 as i64,
        );
        ts3
    };

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0][0].as_int().unwrap() as u64, boundary);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_memory_keeps_builtin_table_valued_functions() {
    let db = MvccTestDb::new();
    let rows = get_rows(&db.conn, "SELECT value FROM generate_series(1,3)");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_insert_read() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_read_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db.mvcc_store.read(
        tx,
        &RowID {
            table_id: (-2).into(),
            row_id: RowKey::Int(1),
        },
    );
    assert!(row.unwrap().is_none());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.mvcc_store
        .delete(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    assert!(!db
        .mvcc_store
        .delete(
            tx,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1)
            },
        )
        .unwrap());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_commit() {
    let db = MvccTestDb::new();
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    let tx1_updated_row = generate_simple_string_row((-2).into(), 1, "World");
    db.mvcc_store.update(tx1, tx1_updated_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_updated_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx2).unwrap();
    assert_eq!(tx1_updated_row, row);
    db.mvcc_store.drop_unused_row_versions();
}

/// What this test checks: Rollback/savepoint behavior restores exactly the intended state when statements or transactions fail.
/// Why this matters: Partial rollback mistakes leave data in impossible intermediate states.
#[test]
fn test_rollback() {
    let db = MvccTestDb::new();
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1.clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row1, row2);
    let row3 = generate_simple_string_row((-2).into(), 1, "World");
    db.mvcc_store.update(tx1, row3.clone()).unwrap();
    let row4 = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row3, row4);
    db.mvcc_store.rollback_tx(
        tx1,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );
    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row5 = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row5, None);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_write() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    let conn2 = db.db.connect().unwrap();
    // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(!db.mvcc_store.update(tx2, tx2_row).unwrap());

    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1).unwrap();

    // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row2, None);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_read_deleted() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 deletes row with ID 1, but does not commit.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    assert!(db
        .mvcc_store
        .delete(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1)
            },
        )
        .unwrap());

    // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_fuzzy_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "First");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 reads the row with ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T3 updates the row and commits.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Second");
    db.mvcc_store.update(tx3, tx3_row).unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // T2 still reads the same version of the row as before.
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T2 tries to update the row, but fails because T3 has already committed an update to the row,
    // so T2 trying to write would violate snapshot isolation if it succeeded.
    let tx2_newrow = generate_simple_string_row((-2).into(), 1, "Third");
    let update_result = db.mvcc_store.update(tx2, tx2_newrow);
    assert!(matches!(update_result, Err(LimboError::WriteWriteConflict)));
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_lost_update() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 attempts to update row ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());

    // T3 also attempts to update row ID 1 within an active transaction.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Hello, world!");
    assert!(matches!(
        db.mvcc_store.update(tx3, tx3_row),
        Err(LimboError::WriteWriteConflict)
    ));
    // hack: in the actual tursodb database we rollback the mvcc tx ourselves, so manually roll it back here
    db.mvcc_store
        .rollback_tx(tx3, conn3.pager.load().clone(), &conn3, crate::MAIN_DB_ID);

    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    assert!(matches!(
        commit_tx(db.mvcc_store.clone(), &conn3, tx3),
        Err(LimboError::TxTerminated)
    ));

    let conn4 = db.db.connect().unwrap();
    let tx4 = db.mvcc_store.begin_tx(conn4.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx4,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx2_row, row);
}

// Test for the visibility to check if a new transaction can see old committed values.
// This test checks for the typo present in the paper, explained in https://github.com/penberg/mvcc-rs/issues/15
#[test]
fn test_committed_visibility() {
    let db = MvccTestDb::new();

    // let's add $10 to my account since I like money
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "10");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // but I like more money, so let me try adding $10 more
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "20");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row, tx2_row);

    // can I check how much money I have?
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

// Test to check if a older transaction can see (un)committed future rows
#[test]
fn test_future_row() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx2, tx2_row).unwrap();

    // transaction in progress, so tx1 shouldn't be able to see the value
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row, None);

    // lets commit the transaction and check if tx1 can see it
    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row, None);
}

use crate::mvcc::cursor::MvccLazyCursor;
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::types::Text;
use crate::Value;
use crate::{Database, StepResult};
use crate::{MemoryIO, Statement};
use crate::{ValueRef, DATABASE_MANAGER};

// Simple atomic clock implementation for testing

fn setup_test_db() -> (MvccTestDb, u64, MVTableId, i64) {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE mvcc_lazy_gap_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'mvcc_lazy_gap_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let btree_root_page = root_page.abs();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let test_rows = [
        (5, "row5"),
        (10, "row10"),
        (15, "row15"),
        (20, "row20"),
        (30, "row30"),
    ];

    for (row_id, data) in test_rows.iter() {
        let id = RowID::new(table_id, RowKey::Int(*row_id));
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1);
        let row = Row::new_table_row(id, record.as_blob().to_vec(), 1);
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id, table_id, btree_root_page)
}

fn setup_lazy_db(initial_keys: &[i64]) -> (MvccTestDb, u64, MVTableId, i64) {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE mvcc_lazy_basic_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'mvcc_lazy_basic_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let btree_root_page = root_page.abs();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    for i in initial_keys {
        let id = RowID::new(table_id, RowKey::Int(*i));
        let data = format!("row{i}");
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data))], 1);
        let row = Row::new_table_row(id, record.as_blob().to_vec(), 1);
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id, table_id, btree_root_page)
}

#[test]
fn test_mvcc_cursor_next_yields_with_injected_yield() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE cursor_yield_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'cursor_yield_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.conn.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::NextStart.point()
    ])));

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            root_page.abs(),
            1,
        )),
    )
    .unwrap();

    let saw_yield = matches!(
        cursor.next().unwrap(),
        IOResult::IO(io) if io.is_explicit_yield()
    );
    db.mvcc_store
        .rollback_tx(tx_id, db.conn.pager.load().clone(), db.conn.as_ref(), 0);

    assert!(
        saw_yield,
        "MVCC cursor should inject an explicit yield on the first next() transition",
    );
}

pub(crate) fn commit_tx(
    mv_store: Arc<MvStore<MvccClock>>,
    conn: &Arc<Connection>,
    tx_id: u64,
) -> Result<()> {
    let mut sm = mv_store.commit_tx(tx_id, conn).unwrap();
    // TODO: sync IO hack
    loop {
        let res = sm.step(&mv_store)?;
        match res {
            IOResult::IO(io) => {
                io.wait(conn.db.io.as_ref())?;
            }
            IOResult::Done(_) => break,
        }
    }
    assert!(sm.is_finalized());
    Ok(())
}

pub(crate) fn commit_tx_no_conn(
    db: &MvccTestDbNoConn,
    tx_id: u64,
    conn: &Arc<Connection>,
) -> Result<(), LimboError> {
    let mv_store = db.get_mvcc_store();
    let mut sm = mv_store.commit_tx(tx_id, conn).unwrap();
    // TODO: sync IO hack
    loop {
        let res = sm.step(&mv_store)?;
        match res {
            IOResult::IO(io) => {
                io.wait(conn.db.io.as_ref())?;
            }
            IOResult::Done(_) => break,
        }
    }
    assert!(sm.is_finalized());
    Ok(())
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_lazy_scan_cursor_basic() {
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 3, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Check first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_lazy_scan_cursor_with_gaps() {
    let (db, tx_id, table_id, btree_root_page) = setup_test_db();

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Check first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 5);

    // Test moving forward and checking IDs
    let expected_ids = [5, 10, 15, 20, 30];
    let mut index = 0;

    let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
        unreachable!();
    };
    let rowid = rowid.unwrap();
    assert_eq!(rowid, expected_ids[index]);

    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        index += 1;
        if index < expected_ids.len() {
            let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
                unreachable!();
            };
            let rowid = rowid.unwrap();
            assert_eq!(rowid, expected_ids[index]);
        }
    }

    // Should have found all 5 rows
    assert_eq!(index, expected_ids.len() - 1);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_basic() {
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 3, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    let _ = cursor.next().unwrap();

    // Check first row
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_with_empty_table() {
    let db = MvccTestDb::new();
    {
        // FIXME: force page 1 initialization
        let pager = db.conn.pager.load().clone();
        let tx_id = db.mvcc_store.begin_tx(pager).unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();
    }
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let table_id = -1; // Empty table

    // Test LazyScanCursor with empty table
    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();
    assert!(cursor.is_empty());
    let rowid = cursor.rowid().unwrap();
    assert!(matches!(rowid, IOResult::Done(None)));
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_modification_during_scan() {
    let _ = tracing_subscriber::fmt::try_init();
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Read first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    let first_row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(first_row.id.row_id.to_int_or_panic(), 1);

    // Insert a new row with ID between existing rows
    let new_row_id = RowID::new(table_id, RowKey::Int(3));
    let new_row = generate_simple_string_record("new_row");

    let _ = cursor
        .insert(&BTreeKey::TableRowId((
            new_row_id.row_id.to_int_or_panic(),
            Some(&new_row),
        )))
        .unwrap();

    let mut read_rowids = vec![];
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        read_rowids.push(
            cursor
                .read_mvcc_current_row()
                .unwrap()
                .unwrap()
                .id
                .row_id
                .to_int_or_panic(),
        );
    }
    assert_eq!(read_rowids, vec![2, 3, 4, 5]);
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/* States described in the Hekaton paper *for serializability*:

Table 1: Case analysis of action to take when version V’s
Begin field contains the ID of transaction TB
------------------------------------------------------------------------------------------------------
TB’s state   | TB’s end timestamp | Action to take when transaction T checks visibility of version V.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TB=T and V’s end timestamp equals infinity.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s begin timestamp will be TS ut V is not yet committed. Use TS
                                  | as V’s begin time when testing visibility. If the test is true,
                                  | allow T to speculatively read V. Committed TS V’s begin timestamp
                                  | will be TS and V is committed. Use TS as V’s begin time to test
                                  | visibility.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s begin timestamp will be TS and V is committed. Use TS as V’s
                                  | begin time to test visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | Ignore V; it’s a garbage version.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s Begin field. TB has terminated so it must have finalized
or not found |                    | the timestamp.
------------------------------------------------------------------------------------------------------

Table 2: Case analysis of action to take when V's End field
contains a transaction ID TE.
------------------------------------------------------------------------------------------------------
TE’s state   | TE’s end timestamp | Action to take when transaction T checks visibility of a version V
             |                    | as of read time RT.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TE is not T.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s end timestamp will be TS provided that TE commits. If TS > RT,
                                  | V is visible to T. If TS < RT, T speculatively ignores V.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s end timestamp will be TS and V is committed. Use TS as V’s end
                                  | timestamp when testing visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | V is visible.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s End field. TE has terminated so it must have finalized
or not found |                    | the timestamp.
*/

fn new_tx(tx_id: TxID, begin_ts: u64, state: TransactionState) -> Transaction {
    let state = state.into();
    Transaction {
        state,
        tx_id,
        begin_ts,
        write_set: SkipSet::new(),
        read_set: SkipSet::new(),
        header: RwLock::new(DatabaseHeader::default()),
        header_dirty: AtomicBool::new(false),
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
        commit_dep_counter: AtomicU64::new(0),
        abort_now: AtomicBool::new(false),
        commit_dep_set: Mutex::new(HashSet::default()),
    }
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_snapshot_isolation_tx_visible1() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (2, new_tx(2, 2, TransactionState::Committed(5))),
        (3, new_tx(3, 3, TransactionState::Aborted)),
        (5, new_tx(5, 5, TransactionState::Preparing(8))),
        (6, new_tx(6, 6, TransactionState::Committed(10))),
        (7, new_tx(7, 7, TransactionState::Active)),
        // tx 8 with Preparing(3): current_tx (begin_ts=4) can speculatively read
        (8, new_tx(8, 1, TransactionState::Preparing(3))),
    ]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let current_tx = new_tx(4, 4, TransactionState::Preparing(7));

    let rv_visible = |begin: Option<TxTimestampOrID>, end: Option<TxTimestampOrID>| {
        let row_version = RowVersion {
            id: 0, // Dummy ID for visibility tests
            begin,
            end,
            row: generate_simple_string_row((-2).into(), 1, "testme"),
            btree_resident: false,
        };
        tracing::debug!("Testing visibility of {row_version:?}");
        row_version.is_visible_to(&current_tx, &txs, &finalized_tx_states)
    };

    // begin visible:   transaction committed with ts < current_tx.begin_ts
    // end visible:     inf
    assert!(rv_visible(Some(TxTimestampOrID::TxID(1)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(2)), None));

    // begin invisible: transaction aborted
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(3)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(1))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction committed with ts < current_tx.begin_ts
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(2))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction aborted, delete never happened (Table 2)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(3))
    ));

    // begin invisible: transaction preparing with end_ts(8) > begin_ts(4)
    // Speculative read condition (begin_ts >= end_ts) is false: 4 >= 8 is false
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(5)), None));

    // begin VISIBLE via speculative read: tx 8 is Preparing(3), begin_ts(4) >= end_ts(3)
    // Hekaton Table 1: speculatively read and register commit dependency
    assert!(rv_visible(Some(TxTimestampOrID::TxID(8)), None));
    // Verify dependency was registered via register-and-report protocol
    assert_eq!(
        current_tx.commit_dep_counter.load(Ordering::Acquire),
        1,
        "speculative read should register a commit dependency"
    );

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible: transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible:   transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction preparing with TS(8) > RT(4) (Table 2)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(5))
    ));

    // begin invisible: timestamp > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(6)),
        Some(TxTimestampOrID::TxID(6))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     some active transaction will eventually overwrite this version,
    //                  but that hasn't happened
    //                  (this is the https://avi.im/blag/2023/hekaton-paper-typo/ case, I believe!)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(7))
    ));

    assert!(!rv_visible(None, None));
}

#[test]
fn test_visibility_uses_finalized_state_for_removed_committed_tx() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::new();
    let finalized_tx_states: SkipMap<TxID, TransactionState> =
        SkipMap::from_iter([(42, TransactionState::Committed(5))]);
    let reader = new_tx(7, 10, TransactionState::Active);

    let inserted_row = RowVersion {
        id: 1,
        begin: Some(TxTimestampOrID::TxID(42)),
        end: None,
        row: generate_simple_string_row((-2).into(), 1, "x"),
        btree_resident: false,
    };
    assert!(
        inserted_row.is_visible_to(&reader, &txs, &finalized_tx_states),
        "stale begin=TxID should resolve via finalized committed state"
    );

    let deleted_row = RowVersion {
        id: 2,
        begin: Some(TxTimestampOrID::Timestamp(1)),
        end: Some(TxTimestampOrID::TxID(42)),
        row: generate_simple_string_row((-2).into(), 2, "y"),
        btree_resident: false,
    };
    assert!(
        !deleted_row.is_visible_to(&reader, &txs, &finalized_tx_states),
        "stale end=TxID should resolve via finalized committed state"
    );
}

#[test]
fn test_read_only_commit_does_not_cache_finalized_state() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    // Establish a clean baseline after schema/setup writes.
    mvcc_store.drop_unused_row_versions();
    let baseline = mvcc_store.finalized_tx_states.len();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let _ = get_rows(&conn, "SELECT 1");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        mvcc_store.finalized_tx_states.len(),
        baseline,
        "read-only commit should not add finalized tx cache entries"
    );
}

#[test]
fn test_drop_unused_row_versions_prunes_unreferenced_finalized_tx_states() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    // Establish a clean baseline after schema/setup writes.
    mvcc_store.drop_unused_row_versions();
    let baseline = mvcc_store.finalized_tx_states.len();

    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    let after_write = mvcc_store.finalized_tx_states.len();
    assert!(
        after_write > baseline,
        "write commit should add at least one finalized tx cache entry"
    );

    mvcc_store.drop_unused_row_versions();

    assert_eq!(
        mvcc_store.finalized_tx_states.len(),
        baseline,
        "GC scan should prune finalized tx cache entries with no remaining TxID references"
    );
}

/// Test Hekaton register-and-report: speculative read increments CommitDepCounter
/// and adds to CommitDepSet.
#[test]
fn test_commit_dependency_speculative_read() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    // Reader with begin_ts=10 > end_ts=5 → speculative read → dependency
    let reader = new_tx(2, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: Some(TxTimestampOrID::TxID(1)),
        end: None,
        row: generate_simple_string_row((-2).into(), 1, "test"),
        btree_resident: false,
    };

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);

    // Speculative read: begin_ts(10) >= end_ts(5) → visible, dependency registered
    assert!(rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // Verify tx 1's CommitDepSet contains reader's tx_id
    let dep_set = txs.get(&1).unwrap();
    assert_eq!(
        *dep_set.value().commit_dep_set.lock(),
        HashSet::from_iter([2])
    );
}

/// Test cascade abort: when depended-on tx aborts, it sets AbortNow on dependents
/// and decrements their CommitDepCounter.
#[test]
fn test_commit_dependency_cascade_abort() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: Some(TxTimestampOrID::TxID(1)),
        end: None,
        row: generate_simple_string_row((-2).into(), 1, "test"),
        btree_resident: false,
    };

    // Speculative read registers dependency
    assert!(rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);
    assert!(!reader.abort_now.load(Ordering::Acquire));

    // Simulate tx 1 aborting and cascading to dependents
    let tx1 = txs.get(&1).unwrap();
    let tx1 = tx1.value();
    tx1.state.store(TransactionState::Aborted);

    // Add reader to txs so cascade can find it
    txs.insert(2, reader);

    for dep_tx_id in tx1.commit_dep_set.lock().drain() {
        if let Some(dep_tx_entry) = txs.get(&dep_tx_id) {
            let dep_tx = dep_tx_entry.value();
            dep_tx.abort_now.store(true, Ordering::Release);
            dep_tx.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
        }
    }

    let reader = txs.get(&2).unwrap();
    let reader = reader.value();
    assert!(reader.abort_now.load(Ordering::Acquire));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
}

/// Test that registering a dependency on an already-committed tx is a no-op.
#[test]
fn test_commit_dependency_already_committed() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Committed(5)))]);

    let reader = new_tx(2, 10, TransactionState::Active);

    register_commit_dependency(&txs, &reader, 1);

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
    assert!(!reader.abort_now.load(Ordering::Acquire));
}

/// Test that registering a dependency on an already-aborted tx sets AbortNow.
#[test]
fn test_commit_dependency_already_aborted() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Aborted))]);

    let reader = new_tx(2, 10, TransactionState::Active);

    register_commit_dependency(&txs, &reader, 1);

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
    assert!(reader.abort_now.load(Ordering::Acquire));
}

/// Test speculative ignore in is_end_visible registers dependency.
#[test]
fn test_commit_dependency_speculative_ignore() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (3, new_tx(3, 3, TransactionState::Preparing(5))),
    ]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    // Reader with begin_ts=10 > end_ts=5: will speculatively ignore (treat as deleted)
    let reader = new_tx(4, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: Some(TxTimestampOrID::Timestamp(2)),
        end: Some(TxTimestampOrID::TxID(3)),
        row: generate_simple_string_row((-2).into(), 1, "test"),
        btree_resident: false,
    };

    // is_end_visible: Preparing(5), begin_ts(10) < 5 = false → deletion visible
    // is_begin_visible: Timestamp(2), 10 >= 2 = true
    // Combined: true && false = false (row not visible because it was deleted)
    assert!(!rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        1,
        "speculative ignore should register a commit dependency"
    );
}

/// Test that multiple speculative reads from the same preparing tx only
/// register one commit dependency (dedup).
#[test]
fn test_commit_dependency_multiple_reads_dedup() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);

    let make_rv = |row_id: i64| RowVersion {
        id: row_id as u64,
        begin: Some(TxTimestampOrID::TxID(1)),
        end: None,
        row: generate_simple_string_row((-2).into(), row_id, "test"),
        btree_resident: false,
    };

    // Read 3 rows from the same preparing tx — dependency is deduplicated
    assert!(make_rv(1).is_visible_to(&reader, &txs, &finalized_tx_states));
    assert!(make_rv(2).is_visible_to(&reader, &txs, &finalized_tx_states));
    assert!(make_rv(3).is_visible_to(&reader, &txs, &finalized_tx_states));

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // tx 1's CommitDepSet has 1 entry for reader (deduplicated)
    let dep_set = txs.get(&1).unwrap();
    assert_eq!(dep_set.value().commit_dep_set.lock().len(), 1);
}

/// Hekaton §2.7 cascade abort with real connections and threads.
///
/// A Preparing writer is speculatively read by a reader on another thread.
/// When the writer aborts, the reader's COMMIT must fail with
/// CommitDependencyAborted (cascade abort via AbortNow).
///
/// Sequence:
///   1. Writer: BEGIN CONCURRENT → UPDATE (real SQL)
///   2. Writer state manually set to Preparing(end_ts)
///   3. Reader thread: BEGIN → SELECT (speculative read → dependency) → INSERT
///   4. Main thread: rollback writer → cascade abort via AbortNow
///   5. Reader COMMIT → CommitDependencyAborted
#[test]
fn test_commit_dep_threaded_abort_cascades() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: real SQL operations
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    // Simulate mid-commit: transition writer to Preparing.
    // end_ts comes from the global clock so the reader's begin_ts will be >=
    // end_ts, satisfying the speculative read condition (Hekaton Table 1).
    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    // Reader signals after speculative read so main thread can abort writer.
    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // SELECT triggers speculative read: reader.begin_ts >= writer.end_ts
        // → Hekaton Table 1: visible, register commit dependency
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        // Write so COMMIT exercises the full commit state machine path.
        reader_conn
            .execute("INSERT INTO t VALUES (2, 'reader_data')")
            .unwrap();

        // Signal: speculative read done, dependency registered
        signal_tx.send(()).unwrap();

        // COMMIT blocks in WaitForDependencies until the writer resolves.
        // Writer will abort → AbortNow set → CommitDependencyAborted.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close(); // cleanup (rolls back if still active)
        (rows, commit_result)
    });

    // Wait for reader to complete speculative read
    signal_rx.recv().unwrap();

    // Abort writer → cascade: sets AbortNow on reader, decrements counter
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    let (rows, commit_result) = reader_handle.join().unwrap();

    // Reader saw the writer's modified value via speculative read
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].to_text().unwrap(),
        "modified",
        "reader should have speculatively read the Preparing writer's value"
    );

    // Reader's COMMIT must fail: depended-on writer aborted
    assert!(
        matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
        "expected CommitDependencyAborted, got: {commit_result:?}",
    );

    // Verify database consistency
    {
        let conn = db.connect();

        // Only the initial value should remain
        let mut stmt = conn.prepare("SELECT value FROM t WHERE id = 1").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_text().unwrap(), "initial");

        // Reader's INSERT must not be visible (cascade-aborted)
        let mut stmt = conn.prepare("SELECT * FROM t WHERE id = 2").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert!(
            rows.is_empty(),
            "reader's write should not be visible after cascade abort"
        );
    }
}

/// Hekaton §2.7: multiple readers depending on the same Preparing writer
/// all cascade-abort when the writer aborts.
#[test]
fn test_commit_dep_threaded_multiple_dependents_abort() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let num_readers = 4;
    // Barrier: all readers + main thread synchronize after speculative reads
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_readers + 1));

    let mut handles = Vec::new();
    for i in 0..num_readers {
        let db_arc = db.get_db();
        let barrier_clone = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            conn.execute("BEGIN CONCURRENT").unwrap();

            // Speculative read from Preparing writer
            let mut stmt = conn.prepare("SELECT value FROM t WHERE id = 1").unwrap();
            let rows = stmt.run_collect_rows().unwrap();

            // Each reader writes to a unique row (no conflicts)
            conn.execute(format!("INSERT INTO t VALUES ({}, 'reader_{i}')", i + 10,))
                .unwrap();

            // Signal: all readers done with speculative reads
            barrier_clone.wait();

            let commit_result = conn.execute("COMMIT");
            let _ = conn.close();
            (rows, commit_result)
        }));
    }

    // Wait for all readers to complete speculative reads
    barrier.wait();

    // Abort writer → cascade to ALL readers
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    for handle in handles {
        let (rows, commit_result) = handle.join().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_text().unwrap(), "modified");
        assert!(
            matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
            "expected CommitDependencyAborted, got: {commit_result:?}",
        );
    }

    // All reader writes should be invisible — only the initial row remains
    {
        let conn = db.connect();
        let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
    }
}

/// Hekaton §2.7 happy path: when a Preparing writer commits, the dependent
/// reader's CommitDepCounter is decremented and the reader can proceed.
#[test]
fn test_commit_dep_threaded_commit_resolves() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: UPDATE via real connection, then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'committed' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        reader_conn
            .execute("INSERT INTO t VALUES (2, 'reader_data')")
            .unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT blocks in WaitForDependencies. Writer will commit →
        // counter decremented → reader proceeds.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        (rows, commit_result)
    });

    signal_rx.recv().unwrap();

    // Complete the writer's commit manually (postprocessing):
    // 1. Convert TxID → Timestamp in row versions
    // 2. Set state to Committed
    // 3. Drain CommitDepSet, decrement dependents' counters
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();

        // Convert TxID→Timestamp in row versions (Hekaton §3.3 postprocessing)
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.begin = Some(TxTimestampOrID::Timestamp(end_ts));
                }
                if rv.end == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.end = Some(TxTimestampOrID::Timestamp(end_ts));
                }
            }
        }

        // Committed state + notify dependents
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let (rows, commit_result) = reader_handle.join().unwrap();

    // Reader speculatively read the writer's value
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_text().unwrap(), "committed");

    // Reader's COMMIT succeeds: dependency resolved by writer's commit
    assert!(
        commit_result.is_ok(),
        "expected reader COMMIT to succeed, got: {commit_result:?}",
    );

    // Both writes are visible
    {
        let conn = db.connect();
        let mut stmt = conn.prepare("SELECT value FROM t ORDER BY id").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].to_text().unwrap(), "committed");
        assert_eq!(rows[1][0].to_text().unwrap(), "reader_data");
    }
}

/// Regression: the write_set.is_empty() fast path used to commit read-only
/// transactions without checking commit dependencies. A read-only tx that
/// speculatively read from a Preparing writer must still honour AbortNow.
#[test]
fn test_commit_dep_threaded_readonly_abort_cascades() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // Read-only: no writes, only SELECT → triggers speculative read
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT on a read-only tx hits the write_set.is_empty() fast path.
        // It must still check commit dependencies.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        (rows, commit_result)
    });

    signal_rx.recv().unwrap();

    // Abort writer → cascade to read-only reader
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    let (rows, commit_result) = reader_handle.join().unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_text().unwrap(), "modified");

    // Read-only tx must still fail when its dependency aborts
    assert!(
        matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
        "read-only tx should fail with CommitDependencyAborted, got: {commit_result:?}",
    );
}

/// Test that register_commit_dependency increments counter before pushing to
/// dep_set, preventing underflow. If counter is incremented after push+unlock,
/// a concurrent drain could fetch_sub(1) on a zero counter, wrapping to MAX.
#[test]
fn test_commit_dependency_counter_no_underflow() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let reader = new_tx(2, 10, TransactionState::Active);

    // Register dependency: counter should go 0 → 1
    register_commit_dependency(&txs, &reader, 1);
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // Simulate drain (as in CommitEnd): fetch_sub should go 1 → 0, not wrap
    reader.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        0,
        "counter should be exactly 0, not u64::MAX (underflow)"
    );
}

/// Test that registering a dependency on a Terminated (aborted+removed from map)
/// transaction correctly sets AbortNow. Before the fix, rollback_tx removed the
/// tx from txs, so register_commit_dependency saw None and assumed "committed."
#[test]
fn test_commit_dependency_terminated_tx_sets_abort() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Terminated))]);

    let reader = new_tx(2, 10, TransactionState::Active);
    register_commit_dependency(&txs, &reader, 1);

    // Terminated means the tx aborted — must set abort_now
    assert!(
        reader.abort_now.load(Ordering::Acquire),
        "dependency on Terminated tx should set abort_now"
    );
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        0,
        "no counter increment for aborted/terminated dependency"
    );
}

/// Test that when tx is NOT in the map (removed), register_commit_dependency
/// treats it as committed (no abort_now, no counter increment). This is correct
/// only for committed transactions. Aborted transactions should NOT be removed
/// from the map (Issue #3 fix ensures this).
#[test]
fn test_commit_dependency_missing_tx_assumes_committed() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);
    register_commit_dependency(&txs, &reader, 99);

    assert!(
        !reader.abort_now.load(Ordering::Acquire),
        "missing tx (committed+removed) should not set abort_now"
    );
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
}

/// Test that read-only transactions with resolved dependencies do NOT advance
/// last_committed_tx_ts. A read-only tx going through WaitForDependencies →
/// CommitEnd would update last_committed_tx_ts, causing spurious Busy errors
/// from acquire_exclusive_tx.
#[test]
fn test_commit_dep_readonly_does_not_advance_timestamp() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();
    let ts_before = mvcc_store.last_committed_tx_ts.load(Ordering::Acquire);

    // Writer: UPDATE then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let mvcc_clone = mvcc_store.clone();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // Read-only: SELECT only → speculative read registers dependency
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let _rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT: read-only with dependency → WaitForDependencies
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    // Complete writer's commit manually (resolve dependency)
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.begin = Some(TxTimestampOrID::Timestamp(end_ts));
                }
                if rv.end == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.end = Some(TxTimestampOrID::Timestamp(end_ts));
                }
            }
        }
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let commit_result = reader_handle.join().unwrap();
    assert!(
        commit_result.is_ok(),
        "read-only tx with resolved dependency should commit: {commit_result:?}",
    );

    let ts_after = mvcc_clone.last_committed_tx_ts.load(Ordering::Acquire);
    assert_eq!(
        ts_before, ts_after,
        "read-only tx should NOT advance last_committed_tx_ts (was {ts_before}, now {ts_after})"
    );
}

/// Test that a new transaction can still acquire the exclusive lock after a
/// read-only dependent tx commits. Before the fix, the read-only tx would
/// advance last_committed_tx_ts via CommitEnd, making acquire_exclusive_tx
/// return Busy for transactions that started before the read.
#[test]
fn test_commit_dep_readonly_does_not_cause_spurious_busy() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: UPDATE then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    // Start a non-CONCURRENT tx that will try to get exclusive lock later.
    // Its begin_ts is assigned now, before the read-only tx commits.
    let exclusive_conn = db.connect();
    exclusive_conn.execute("BEGIN CONCURRENT").unwrap();
    let exclusive_tx_id = exclusive_conn.get_mv_tx_id().unwrap();

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let _rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    // Resolve the writer's commit (unblocks reader's WaitForDependencies)
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.begin = Some(TxTimestampOrID::Timestamp(end_ts));
                }
                if rv.end == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.end = Some(TxTimestampOrID::Timestamp(end_ts));
                }
            }
        }
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let commit_result = reader_handle.join().unwrap();
    assert!(commit_result.is_ok());

    // Now try to acquire exclusive lock for the tx that started before the
    // read-only dependent committed. Should succeed because the read-only tx
    // did not advance last_committed_tx_ts.
    let acquire_result = mvcc_store.acquire_exclusive_tx(&exclusive_tx_id);
    assert!(
        acquire_result.is_ok(),
        "acquire_exclusive_tx should not return Busy after a read-only dependent committed: {acquire_result:?}",
    );
    mvcc_store.release_exclusive_tx(&exclusive_tx_id);
}

/// Insert a synthetic table and a single row via the MVCC store, then commit.
/// Used by restart / recovery tests to seed durable state before a restart cycle.
fn write_synthetic_row(db: &MvccTestDbNoConn, value: &str) {
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let max_root_page = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
    )[0][0]
        .as_int()
        .unwrap();
    let next_schema_rowid = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rowid), 0) + 1 FROM sqlite_schema",
    )[0][0]
        .as_int()
        .unwrap();
    let synthetic_root = -(max_root_page + 100);
    let synthetic_table_id = MVTableId::new(synthetic_root);
    let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    let data = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new("table")),
            Value::Text(Text::new("test")),
            Value::Text(Text::new("test")),
            Value::from_i64(synthetic_root),
            Value::Text(Text::new(
                "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
            )),
        ],
        5,
    );
    mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(
                RowID::new((-1).into(), RowKey::Int(next_schema_rowid)),
                data.as_blob().to_vec(),
                5,
            ),
        )
        .unwrap();
    let row = generate_simple_string_row(synthetic_table_id, 1, value);
    mvcc_store.insert(tx_id, row).unwrap();
    commit_tx(mvcc_store, &conn, tx_id).unwrap();
    conn.close().unwrap();
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    write_synthetic_row(&db, "foo");
    db.restart();

    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let max_root_page = get_rows(
            &conn,
            "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
        )[0][0]
            .as_int()
            .unwrap();
        let synthetic_table_id = MVTableId::new(-(max_root_page + 100));
        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = generate_simple_string_row(synthetic_table_id, 2, "bar");

        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = mvcc_store
            .read(tx_id, &RowID::new(synthetic_table_id, RowKey::Int(2)))
            .unwrap()
            .unwrap();
        let record = get_record_value(&row);
        match record.get_value(0).unwrap() {
            ValueRef::Text(text) => {
                assert_eq!(text.as_str(), "bar");
            }
            _ => panic!("Expected Text value"),
        }
        conn.close().unwrap();
    }
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_connection_sees_other_connection_changes() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0
        .execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    let conn1 = db.connect();
    conn1
        .execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    conn0
        .execute("INSERT INTO test_table (id, text) VALUES (965, 'text_877')")
        .unwrap();
    let mut stmt = conn1.query("SELECT * FROM test_table").unwrap().unwrap();
    stmt.run_with_row_callback(|row| {
        let text = row.get_value(1).to_text().unwrap();
        assert_eq!(text, "text_877");
        Ok(())
    })
    .unwrap();
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete_with_conn() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0.execute("CREATE TABLE test(t)").unwrap();

    let mut inserts = vec![1, 2, 3, 4, 5, 6, 7];

    for t in &inserts {
        conn0
            .execute(format!("INSERT INTO test(t) VALUES ({t})"))
            .unwrap();
    }

    conn0.execute("DELETE FROM test WHERE t = 5").unwrap();
    inserts.remove(4);

    let mut stmt = conn0.prepare("SELECT * FROM test").unwrap();
    let mut pos = 0;
    stmt.run_with_row_callback(|row| {
        let t = row.get_value(0).as_int().unwrap();
        assert_eq!(t, inserts[pos]);
        pos += 1;
        Ok(())
    })
    .unwrap();
}

fn get_record_value(row: &Row) -> ImmutableRecord {
    let mut record = ImmutableRecord::new(1024);
    record.start_serialization(row.payload());
    record
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_interactive_transaction() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // do some transaction
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE test (x)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();

    // expect other transaction to see the changes
    let rows = get_rows(&conn, "SELECT * FROM test");
    assert_eq!(
        rows,
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
    );
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_commit_without_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    // do not start interactive transaction
    conn.execute("CREATE TABLE test (x)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)").unwrap();

    // expect error on trying to commit a non-existent interactive transaction
    let err = conn.execute("COMMIT").unwrap_err();
    if let LimboError::TxError(e) = err {
        assert_eq!(e, "cannot commit - no transaction is active");
    } else {
        panic!("Expected TxError");
    }
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        rows.push(values);
        Ok(())
    })
    .unwrap();
    rows
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
#[ignore]
fn test_concurrent_writes() {
    struct ConnectionState {
        conn: Arc<Connection>,
        inserts: Vec<i64>,
        current_statement: Option<Statement>,
    }
    let db = MvccTestDbNoConn::new_with_random_db();
    let mut connections = Vec::new();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE test (x)").unwrap();
        conn.close().unwrap();
    }
    let num_connections = 20;
    let num_inserts_per_connection = 10000;
    for i in 0..num_connections {
        let conn = db.connect();
        let mut inserts = ((num_inserts_per_connection * i)
            ..(num_inserts_per_connection * (i + 1)))
            .collect::<Vec<i64>>();
        inserts.reverse();
        connections.push(ConnectionState {
            conn,
            inserts,
            current_statement: None,
        });
    }

    loop {
        let mut all_finished = true;
        for conn in &mut connections {
            if !conn.inserts.is_empty() || conn.current_statement.is_some() {
                all_finished = false;
                break;
            }
        }
        for (conn_id, conn) in connections.iter_mut().enumerate() {
            // println!("connection {conn_id} inserts: {:?}", conn.inserts);
            if conn.current_statement.is_none() && !conn.inserts.is_empty() {
                let write = conn.inserts.pop().unwrap();
                println!("inserting row {write} from connection {conn_id}");
                conn.current_statement = Some(
                    conn.conn
                        .prepare(format!("INSERT INTO test (x) VALUES ({write})"))
                        .unwrap(),
                );
            }
            if conn.current_statement.is_none() {
                continue;
            }
            println!("connection step {conn_id}");
            let stmt = conn.current_statement.as_mut().unwrap();
            match stmt.step().unwrap() {
                // These you be only possible cases in write concurrency.
                // No rows because insert doesn't return
                // No interrupt because insert doesn't interrupt
                // No busy because insert in mvcc should be multi concurrent write
                StepResult::Done => {
                    println!("connection {conn_id} done");
                    conn.current_statement = None;
                }
                StepResult::IO => {
                    // let's skip doing I/O here, we want to perform io only after all the statements are stepped
                }
                StepResult::Busy => {
                    println!("connection {conn_id} busy");
                    // stmt.reprepare().unwrap();
                    unreachable!();
                }
                _ => {
                    unreachable!()
                }
            }
        }
        db.get_db().io.step().unwrap();

        if all_finished {
            println!("all finished");
            break;
        }
    }

    // Now let's find out if we wrote everything we intended to write.
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT * FROM test ORDER BY x ASC");
    assert_eq!(
        rows.len() as i64,
        num_connections * num_inserts_per_connection
    );
    for (row_id, row) in rows.iter().enumerate() {
        assert_eq!(row[0].as_int().unwrap(), row_id as i64);
    }
    conn.close().unwrap();
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn transaction_display() {
    let state = AtomicTransactionState::from(TransactionState::Preparing(20250915));
    let tx_id = 42;
    let begin_ts = 20250914;

    let write_set = SkipSet::new();
    write_set.insert(RowID::new((-2).into(), RowKey::Int(11)));
    write_set.insert(RowID::new((-2).into(), RowKey::Int(13)));

    let read_set = SkipSet::new();
    read_set.insert(RowID::new((-2).into(), RowKey::Int(17)));
    read_set.insert(RowID::new((-2).into(), RowKey::Int(19)));

    let tx = Transaction {
        state,
        tx_id,
        begin_ts,
        write_set,
        read_set,
        header: RwLock::new(DatabaseHeader::default()),
        header_dirty: AtomicBool::new(false),
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
        commit_dep_counter: AtomicU64::new(0),
        abort_now: AtomicBool::new(false),
        commit_dep_set: Mutex::new(HashSet::default()),
    };

    let expected = "{ state: Preparing(20250915), id: 42, begin_ts: 20250914, write_set: [RowID { table_id: MVTableId(-2), row_id: Int(11) }, RowID { table_id: MVTableId(-2), row_id: Int(13) }], read_set: [RowID { table_id: MVTableId(-2), row_id: Int(17) }, RowID { table_id: MVTableId(-2), row_id: Int(19) }] }";
    let output = format!("{tx}");
    assert_eq!(output, expected);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_should_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    assert!(!mv_store.storage.should_checkpoint());
    mv_store.set_checkpoint_threshold(0);
    assert!(mv_store.storage.should_checkpoint());
}

/// What this test checks: After restart recovery, checkpoint-threshold checks use the recovered log offset.
/// Why this matters: Shadow-offset drift can suppress auto-checkpoint despite a large recovered log tail.
#[test]
fn test_should_checkpoint_after_recovery_uses_recovered_offset() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
    }

    db.restart();
    let _conn = db.connect();
    let mv_store = db.get_mvcc_store();

    // We used to assert on the concrete logical-log offset here, but MVCC durable storage
    // is now abstracted behind a trait object (to allow injecting custom implementations).
    // Validate behavior instead: after recovery, the recovered offset should be reflected
    // in should_checkpoint() when the threshold is set very low.
    mv_store.set_checkpoint_threshold(1);
    assert!(
        mv_store.storage.should_checkpoint(),
        "expected should_checkpoint() to reflect the recovered logical-log offset"
    );
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_insert_with_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    // force checkpoint on every transaction
    mv_store.set_checkpoint_threshold(0);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    let value = row.first().unwrap();
    match value {
        Value::Numeric(crate::numeric::Numeric::Integer(i)) => assert_eq!(*i, 1),
        _ => unreachable!(),
    }
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_auto_checkpoint_busy_is_ignored() {
    let db = MvccTestDb::new();
    db.mvcc_store.set_checkpoint_threshold(0);

    // Keep a second transaction open to hold the checkpoint read lock.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row).unwrap();

    // Regression: auto-checkpoint returning Busy used to bubble up and cause
    // statement abort/rollback after the tx was removed.
    // Commit should succeed even if the auto-checkpoint is busy.
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // Cleanup: release the read lock held by tx2.
    db.mvcc_store.rollback_tx(
        tx2,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_read_tx_lifecycle() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SELECT * FROM t").unwrap();

    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().expect("wal should be enabled");
    assert!(wal.holds_read_lock());

    conn.execute("COMMIT").unwrap();
    assert!(!wal.holds_read_lock());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_conn_drop_releases_read_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(x)").unwrap();

    let pager = conn.pager.load();
    pager.begin_read_tx().unwrap();
    let wal = pager.wal.as_ref().expect("wal should be enabled").clone();
    assert!(wal.holds_read_lock());

    drop(conn);
    assert!(!wal.holds_read_lock());
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_select_empty_table() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    // force checkpoint on every transaction
    mv_store.set_checkpoint_threshold(0);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(x integer primary key)")
        .unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t where x > 100");
    assert!(rows.is_empty());
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Value::from_i64(1)]);
    assert_eq!(rows[1], vec![Value::from_i64(2)]);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc_2() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (3)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t");
    dbg!(&rows);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Value::from_i64(1)]);
    assert_eq!(rows[1], vec![Value::from_i64(2)]);
    assert_eq!(rows[2], vec![Value::from_i64(3)]);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (3)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    dbg!(&rows);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Value::from_i64(3)]);
    assert_eq!(rows[1], vec![Value::from_i64(2)]);
    assert_eq!(rows[2], vec![Value::from_i64(1)]);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor_with_delete() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("INSERT INTO t VALUES (4)").unwrap();
        conn.execute("INSERT INTO t VALUES (5)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    conn.execute("DELETE FROM t WHERE x = 2").unwrap();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    dbg!(&rows);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec![Value::from_i64(5)]);
    assert_eq!(rows[1], vec![Value::from_i64(4)]);
    assert_eq!(rows[2], vec![Value::from_i64(3)]);
    assert_eq!(rows[3], vec![Value::from_i64(1)]);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[turso_macros::test(encryption)]
#[ignore] // FIXME: This fails constantly on main and is really annoying, disabling for now :]
fn test_cursor_with_btree_and_mvcc_fuzz() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    let mut rows_in_db = sorted_vec::SortedVec::new();
    let mut seen = HashSet::default();
    let (mut rng, _seed) = rng_from_time_or_env();
    println!("seed: {_seed}");

    let mut maybe_conn = Some(db.connect());
    {
        maybe_conn
            .as_mut()
            .unwrap()
            .execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
    }

    #[repr(u8)]
    #[derive(Debug)]
    enum Op {
        Insert = 0,
        Delete = 1,
        SelectForward = 2,
        SelectBackward = 3,
        SeekForward = 4,
        SeekBackward = 5,
        Checkpoint = 6,
    }

    impl From<u8> for Op {
        fn from(value: u8) -> Self {
            match value {
                0 => Op::Insert,
                1 => Op::Delete,
                2 => Op::SelectForward,
                3 => Op::SelectBackward,
                4 => Op::SeekForward,
                5 => Op::SeekBackward,
                6 => Op::Checkpoint,
                _ => unreachable!(),
            }
        }
    }

    for i in 0..10000 {
        let conn = maybe_conn.as_mut().unwrap();
        let op = rng.random_range(0..=Op::Checkpoint as usize);
        let op = Op::from(op as u8);
        println!("tick: {i} op: {op:?} ");
        match op {
            Op::Insert => {
                let value = loop {
                    let value = rng.random_range(0..10000);
                    if !seen.contains(&value) {
                        seen.insert(value);
                        break value;
                    }
                };
                let query = format!("INSERT INTO t VALUES ({value})");
                println!("inserting: {query}");
                conn.execute(query.as_str()).unwrap();
                rows_in_db.push(value);
            }
            Op::Delete => {
                if rows_in_db.is_empty() {
                    continue;
                }
                let index = rng.random_range(0..rows_in_db.len());
                let value = rows_in_db[index];
                let query = format!("DELETE FROM t WHERE x = {value}");
                println!("deleting: {query}");
                conn.execute(query.as_str()).unwrap();
                rows_in_db.remove_index(index);
                seen.remove(&value);
            }
            Op::SelectForward => {
                let rows = get_rows(conn, "SELECT * FROM t order by x asc");
                assert_eq!(
                    rows.len(),
                    rows_in_db.len(),
                    "expected {} rows, got {}",
                    rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(rows_in_db.iter()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SelectBackward => {
                let rows = get_rows(conn, "SELECT * FROM t order by x desc");
                assert_eq!(
                    rows.len(),
                    rows_in_db.len(),
                    "expected {} rows, got {}",
                    rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(rows_in_db.iter().rev()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SeekForward => {
                let value = rng.random_range(0..10000);
                let rows = get_rows(
                    conn,
                    format!("SELECT * FROM t where x > {value} order by x asc").as_str(),
                );
                let filtered_rows_in_db = rows_in_db
                    .iter()
                    .filter(|&id| *id > value)
                    .cloned()
                    .collect::<Vec<i64>>();

                assert_eq!(
                    rows.len(),
                    filtered_rows_in_db.len(),
                    "expected {} rows, got {}",
                    filtered_rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(filtered_rows_in_db.iter()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SeekBackward => {
                let value = rng.random_range(0..10000);
                let rows = get_rows(
                    conn,
                    format!("SELECT * FROM t where x > {value} order by x desc").as_str(),
                );
                let filtered_rows_in_db = rows_in_db
                    .iter()
                    .filter(|&id| *id > value)
                    .cloned()
                    .collect::<Vec<i64>>();

                assert_eq!(
                    rows.len(),
                    filtered_rows_in_db.len(),
                    "expected {} rows, got {}",
                    filtered_rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(filtered_rows_in_db.iter().rev()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::Checkpoint => {
                // This forces things to move to the BTree file (.db)
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
                // This forces MVCC to be cleared
                db.restart();
                maybe_conn = Some(db.connect());
            }
        }
    }
}

pub fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    );
    let rng = ChaCha8Rng::seed_from_u64(seed as u64);
    (rng, seed as u64)
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_cursor_with_btree_and_mvcc_insert_after_checkpoint_repeated_key() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    let res = conn.execute("INSERT INTO t VALUES (2)");
    assert!(res.is_err(), "Expected error because key 2 already exists");
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_cursor_with_btree_and_mvcc_seek_after_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Seek to the second row.
    let res = get_rows(&conn, "SELECT * FROM t WHERE x = 2");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0][0].as_int().unwrap(), 2);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_cursor_with_btree_and_mvcc_delete_after_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    assert_eq!(rows.len(), 0);
}

/// Core MVCC read/write semantics for AUTOINCREMENT with rowid update.
#[test]
#[ignore = "AUTOINCREMENT not yet supported in MVCC mode"]
fn test_skips_updated_rowid() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();

    // we insert with default values
    conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM sqlite_sequence");
    dbg!(&rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);

    // we update the rowid to +1
    conn.execute("UPDATE t SET a = a + 1").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM sqlite_sequence");
    dbg!(&rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);

    // we insert with default values again
    conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM sqlite_sequence");
    dbg!(&rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 3);
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_mvcc_integrity_check() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY)")
        .unwrap();

    // we insert with default values
    conn.execute("INSERT INTO t values(1)").unwrap();

    let ensure_integrity = || {
        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].cast_text().unwrap(), "ok");
    };

    ensure_integrity();

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    ensure_integrity();
}

/// Test that integrity_check passes after DROP TABLE but before checkpoint.
/// Issue #4975: After checkpointing a table and then dropping it, integrity_check
/// would fail because the dropped table's btree pages still exist but aren't
/// tracked by the schema. The fix is to track dropped root pages until checkpoint.
#[test]
fn test_integrity_check_after_drop_table_before_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();

    // Now drop table. Before the fix, this would make integrity_check fail because
    // we dropped the table before checkpointing, meaning integrity_check would find
    // pages not being used since we didn't provide root page of table t for checks.
    conn.execute("DROP TABLE t").unwrap();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that integrity_check passes after DROP INDEX but before checkpoint.
/// Issue #4975: After checkpointing an index and then dropping it, integrity_check
/// would fail because the dropped index's btree pages still exist but aren't
/// tracked by the schema. The fix is to track dropped root pages until checkpoint.
#[test]
fn test_integrity_check_after_drop_index_before_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();

    // Now drop index. Before the fix, this would make integrity_check fail because
    // we dropped the index before checkpointing, meaning integrity_check would find
    // pages not being used since we didn't provide root page of index idx_t_data for checks.
    conn.execute("DROP INDEX idx_t_data").unwrap();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// What this test checks: Rollback/savepoint behavior restores exactly the intended state when statements or transactions fail.
/// Why this matters: Partial rollback mistakes leave data in impossible intermediate states.
#[test]
fn test_rollback_with_index() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE)")
        .unwrap();

    // we insert with default values
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t values (1, 1)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // This query will try to use index to find the row, if we rollback correctly it shouldn't panic
    let rows = get_rows(&conn, "SELECT * FROM t where b = 1");
    assert_eq!(rows.len(), 0);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}
/// 1. BEGIN CONCURRENT (start interactive transaction)
/// 2. UPDATE modifies col_a's index, then fails constraint check on col_b
/// 3. The partial index changes are NOT rolled back (this is the bug!)
/// 4. COMMIT succeeds, persisting the inconsistent state
/// 5. Later UPDATE on same row fails: "IdxDelete: no matching index entry found"
///    because table row has old value but index has new value
#[test]
fn test_update_multiple_unique_columns_partial_rollback() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create table with multiple unique columns (like blue_sun_77 in the bug)
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            col_a TEXT UNIQUE,
            col_b REAL UNIQUE
        )",
    )
    .unwrap();

    // Insert two rows - one to update, one to cause conflict
    conn.execute("INSERT INTO t VALUES (1, 'original_a', 1.0)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'other_a', 2.0)")
        .unwrap();

    // Start an INTERACTIVE transaction - this is KEY to reproducing the bug!
    // In auto-commit mode, the entire transaction is rolled back on error.
    // In interactive mode, only the statement should be rolled back.
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try to UPDATE row 1 with:
    // - col_a = 'new_a' (index modification happens first)
    // - col_b = 2.0 (should FAIL - conflicts with row 2)
    //
    // The UPDATE bytecode does:
    // 1. Delete old index entry for col_a ('original_a', 1)
    // 2. Insert new index entry for col_a ('new_a', 1)
    // 3. Delete old index entry for col_b (1.0, 1)
    // 4. Check constraint for col_b (2.0) - FAIL with Halt err_code=1555!
    //
    // BUG: Without proper statement rollback, steps 1-3 are committed!
    let result = conn.execute("UPDATE t SET col_a = 'new_a', col_b = 2.0 WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on col_b"
    );

    // COMMIT the transaction - this is what the stress test does after the error!
    // In the buggy case, this commits the partial index changes from the failed UPDATE.
    conn.execute("COMMIT").unwrap();

    // Now in a NEW transaction, try to UPDATE the same row.
    // If the previous statement's partial changes were committed:
    // - Table row still has col_a = 'original_a' (UPDATE didn't complete)
    // - But index for col_a now has 'new_a' instead of 'original_a'!
    // - This UPDATE reads 'original_a' from table, tries to delete that index entry
    // - CRASH: "IdxDelete: no matching index entry found for key ['original_a', 1]"
    conn.execute("UPDATE t SET col_a = 'updated_a', col_b = 3.0 WHERE id = 1")
        .unwrap();

    // Verify the update worked
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].cast_text().unwrap(), "updated_a");

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

// ─── GC helpers ───────────────────────────────────────────────────────────

fn make_rv(begin: Option<TxTimestampOrID>, end: Option<TxTimestampOrID>) -> RowVersion {
    RowVersion {
        id: 0,
        begin,
        end,
        row: generate_simple_string_row((-2).into(), 1, "gc_test"),
        btree_resident: false,
    }
}

fn ts(v: u64) -> Option<TxTimestampOrID> {
    Some(TxTimestampOrID::Timestamp(v))
}

fn txid(v: u64) -> Option<TxTimestampOrID> {
    Some(TxTimestampOrID::TxID(v))
}

// ─── GC unit tests ───────────────────────────────────────────────────────

#[test]
/// Rolled-back transactions leave versions with begin=None, end=None. These are
/// invisible to every transaction and must be removed unconditionally by Rule 1.
fn test_gc_rule1_aborted_garbage_removed() {
    let mut versions = vec![make_rv(None, None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, u64::MAX, 0);
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Rule 1 removes only aborted garbage, leaving live and superseded versions intact.
fn test_gc_rule1_aborted_among_live_versions() {
    let mut versions = vec![
        make_rv(ts(5), None),  // current
        make_rv(None, None),   // aborted
        make_rv(ts(3), ts(5)), // superseded
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 2, 0);
    // Only aborted removed; superseded has e=5 > lwm=2 so retained
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 2);
    assert!(versions
        .iter()
        .all(|rv| rv.begin.is_some() || rv.end.is_some()));
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// A superseded version whose end timestamp is at or below the low-water mark is
/// invisible to all active readers. When a committed current version exists to
/// take over B-tree invalidation, the superseded version is safely removable.
fn test_gc_rule2_superseded_below_lwm_with_current() {
    // Superseded version (end=Timestamp(3)) below LWM=10, and there's a current version.
    let mut versions = vec![
        make_rv(ts(3), ts(5)), // superseded, e=5 <= lwm=10
        make_rv(ts(5), None),  // current
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 0);
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 1);
    assert!(versions[0].end.is_none()); // only current remains
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// A superseded version whose end timestamp exceeds the LWM may still be visible
/// to an active reader. It must be retained regardless of other conditions.
fn test_gc_rule2_superseded_above_lwm_retained() {
    // Superseded version (end=Timestamp(15)) above LWM=10 — must be retained.
    let mut versions = vec![make_rv(ts(3), ts(15)), make_rv(ts(15), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 0);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// When a row was deleted but the deletion hasn't been checkpointed to the B-tree
/// yet (e > ckpt_max), the tombstone is the only thing hiding the stale B-tree
/// row. Removing it would resurrect a deleted row. Must be retained.
fn test_gc_rule2_tombstone_guard_uncheckpointed() {
    // Tombstone: end is set, no current version, and e > ckpt_max.
    // Must be retained to prevent row resurrection via dual cursor.
    let mut versions = vec![
        make_rv(ts(3), ts(5)), // tombstone (sole version, no current)
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 2);
    // e=5 > ckpt_max=2, no current → tombstone guard retains it
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Once the deletion has been checkpointed (e <= ckpt_max), the B-tree no longer
/// contains the row, so the tombstone is safe to remove.
fn test_gc_rule2_tombstone_guard_checkpointed() {
    // Tombstone with e <= ckpt_max — deletion is checkpointed, safe to remove.
    let mut versions = vec![make_rv(ts(3), ts(5))];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 5);
    // e=5 <= ckpt_max=5, e=5 <= lwm=10 → removable
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// A current version that's been checkpointed to B-tree, with no other versions in
/// the chain and no active reader needing it, is redundant. The dual cursor will
/// fall through to the B-tree which has identical data. Safe to remove.
fn test_gc_rule3_checkpointed_sole_survivor_removed() {
    // Single current version with b <= ckpt_max and b < lwm.
    let mut versions = vec![make_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 5);
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// A current version not yet checkpointed (b > ckpt_max) cannot be removed —
/// the B-tree doesn't have the data, so fallthrough would return stale results.
fn test_gc_rule3_not_checkpointed_retained() {
    // Single current version with b > ckpt_max — B-tree doesn't have it yet.
    let mut versions = vec![make_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 3);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// A current version whose begin timestamp equals the LWM might still be needed
/// by the oldest active reader. Rule 3 requires strict b < lwm, so it's retained.
fn test_gc_rule3_visible_to_active_tx_retained() {
    // Single current version with b >= lwm — some active tx might need it.
    let mut versions = vec![make_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 5, 10);
    // b=5 is NOT < lwm=5 (strict <), so retained
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// A current version cannot be removed before checkpoint has persisted it.
fn test_gc_rule3_current_retained_before_first_checkpoint() {
    let mut versions = vec![make_rv(ts(1), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 0);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Once checkpoint has persisted a sole current version, it becomes GC-eligible.
fn test_gc_rule3_current_collected_after_checkpoint() {
    let mut versions = vec![make_rv(ts(1), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 5);
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 0);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Rule 3 requires the current version to be the sole remaining version in the
/// chain. When a superseded version is removed first by Rule 2, Rule 3 can then
/// fire on the remaining sole survivor — both rules compose correctly.
fn test_gc_rule3_not_sole_survivor() {
    // Rule 3 only fires when exactly one version remains after rules 1 & 2.
    let mut versions = vec![make_rv(ts(3), ts(5)), make_rv(ts(5), None)];
    // Both b <= ckpt_max and b < lwm, but there are 2 versions.
    // Rule 2 removes the superseded one (has_current=true), then rule 3 fires
    // on the remaining sole survivor.
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 5);
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Versions referencing an active transaction (begin=TxID) represent uncommitted
/// inserts. They don't match any removal rule and must always be retained.
fn test_gc_txid_refs_retained() {
    // Versions with TxID (uncommitted) references are never collected.
    let mut versions = vec![make_rv(txid(99), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, u64::MAX, u64::MAX);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Versions with end=TxID represent an uncommitted deletion. Rule 2 only matches
/// end=Timestamp, so these are never collected until the deleting tx resolves.
fn test_gc_txid_end_retained() {
    // end=TxID means the deletion is uncommitted; rule 2 only matches Timestamp.
    let mut versions = vec![make_rv(ts(3), txid(50))];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, u64::MAX, u64::MAX);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// A pending insert (begin=TxID) must NOT count as a "committed current version"
/// for the tombstone guard. If it rolled back, the tombstone would be the only
/// thing hiding the stale B-tree row, and removing it would resurrect deleted data.
fn test_gc_rule2_pending_insert_does_not_disable_tombstone_guard() {
    // A pending insert (begin=TxID, end=None) coexists with a tombstone.
    // has_current must NOT count the pending insert — if it rolls back,
    // the tombstone is the only thing hiding the B-tree row.
    let mut versions = vec![
        make_rv(ts(3), ts(5)), // tombstone: deletion at e=5, not checkpointed (ckpt_max=2)
        make_rv(txid(99), None), // pending insert (uncommitted)
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 2);
    // Tombstone must be retained: e=5 > ckpt_max=2, and pending insert doesn't count.
    // Only nothing changes (pending insert is not aborted garbage either).
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// When a committed current version exists (begin=Timestamp, end=None), it takes
/// over B-tree invalidation from the superseded version. The tombstone guard is
/// no longer needed, so the superseded version can be safely removed.
fn test_gc_rule2_committed_current_disables_tombstone_guard() {
    // A committed current version (begin=Timestamp, end=None) means the row
    // has a live successor — the tombstone can safely be removed.
    let mut versions = vec![
        make_rv(ts(3), ts(5)), // superseded, e=5 <= lwm=10
        make_rv(ts(5), None),  // committed current
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 2);
    // Superseded removed (has_current=true for committed version), current remains.
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 1);
    assert!(versions[0].end.is_none());
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// B-tree tombstones (begin=None, end=e) represent rows that existed in the B-tree
/// before MVCC was enabled and were then deleted. Before checkpoint writes the
/// deletion, the tombstone hides the stale B-tree row. After checkpoint, it's safe
/// to remove. Tests the full lifecycle: retained → checkpointed → collected.
fn test_gc_rule2_btree_tombstone_lifecycle() {
    // B-tree tombstone: begin=None, end=Timestamp(e) where e > 0.
    // Represents a row deleted in MVCC that existed in B-tree before MVCC.
    // Before checkpoint (ckpt_max < e): tombstone must be retained.
    let mut versions = vec![make_rv(None, ts(5))];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, u64::MAX, 3);
    assert_eq!(dropped, 0, "tombstone retained: e=5 > ckpt_max=3");
    assert_eq!(versions.len(), 1);

    // After checkpoint (ckpt_max >= e): tombstone is collected.
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, u64::MAX, 5);
    assert_eq!(dropped, 1, "tombstone collected: e=5 <= ckpt_max=5");
    assert_eq!(versions.len(), 0);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Rule 3 must never fire when superseded versions remain in the chain — removing
/// the current version would leave orphaned superseded versions that "poison" the
/// dual cursor, making it hide the B-tree row without providing a replacement.
fn test_gc_rule3_not_firing_with_unremovable_superseded() {
    // Two versions: superseded with e > lwm (can't remove), and current.
    // Rule 2 can't remove the superseded one, so 2 versions remain.
    // Rule 3 requires sole-survivor, so it must NOT fire.
    let mut versions = vec![
        make_rv(ts(3), ts(15)), // e=15 > lwm=10 — retained
        make_rv(ts(15), None),  // current
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 20);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// GC on an empty version chain is a no-op. Verifies no panics or off-by-one errors.
fn test_gc_noop_on_empty() {
    let mut versions: Vec<RowVersion> = vec![];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 5);
    assert_eq!(dropped, 0);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// All three rules fire together: aborted garbage (Rule 1), two superseded versions
/// below LWM with a committed current (Rule 2), and the sole surviving current
/// version below LWM and checkpointed (Rule 3). The chain is fully reclaimed.
fn test_gc_combined_rules() {
    // Mix of all cases: aborted, superseded below LWM, current checkpointed,
    // and one above LWM that must be retained.
    let mut versions = vec![
        make_rv(None, None),   // aborted → rule 1
        make_rv(ts(1), ts(3)), // superseded, e=3 <= lwm=10 → rule 2 (has_current=true)
        make_rv(ts(3), ts(5)), // superseded, e=5 <= lwm=10 → rule 2
        make_rv(ts(5), None),  // current, b=5 <= ckpt_max=5, b < lwm=10 → rule 3
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(&mut versions, 10, 5);
    assert_eq!(dropped, 4);
    assert!(versions.is_empty());
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// End-to-end at the MvStore level: insert a row, commit, and run GC. Without a
/// checkpoint the version is not yet in the B-tree, so Rule 3 doesn't fire and
/// the version survives. Verifies the full insert→commit→GC pipeline.
fn test_gc_integration_insert_commit_gc() {
    let db = MvccTestDb::new();

    // Insert and commit a row.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "gc_test");
    db.mvcc_store.insert(tx1, row).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // Row should be in the MvStore.
    assert!(!db.mvcc_store.rows.is_empty());

    // No active transactions → LWM = u64::MAX.
    // ckpt_max = 0 (no checkpoint yet), so rule 3 won't fire (b > ckpt_max).
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 0);
    assert!(!db.mvcc_store.rows.is_empty());
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Rolling back a transaction leaves aborted garbage (begin=None, end=None).
/// GC reclaims the versions. The SkipMap entry stays (lazy removal to avoid
/// TOCTOU with concurrent writers) but the version vec is empty.
fn test_gc_integration_rollback_creates_aborted_garbage() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "will_rollback");
    db.mvcc_store.insert(tx1, row).unwrap();
    db.mvcc_store.rollback_tx(
        tx1,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );

    // Rollback should leave aborted garbage (begin=None, end=None).
    let entry = db
        .mvcc_store
        .rows
        .get(&RowID::new((-2).into(), RowKey::Int(1)));
    assert!(entry.is_some());
    {
        let versions = entry.as_ref().unwrap().value().read();
        assert_eq!(versions.len(), 1);
        assert!(versions[0].begin.is_none());
        assert!(versions[0].end.is_none());
    }

    // GC should clean up the version. The SkipMap entry stays (lazy removal
    // in background GC avoids TOCTOU), but the version vec should be empty.
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 1);
    let entry = db
        .mvcc_store
        .rows
        .get(&RowID::new((-2).into(), RowKey::Int(1)));
    assert!(entry.is_some(), "SkipMap entry stays (lazy removal)");
    assert!(
        entry.unwrap().value().read().is_empty(),
        "but versions should be empty"
    );
}

/// The low-water mark (LWM) is the minimum begin_ts of all active readers. GC
/// must not remove any version that an active reader might still need. This test
/// opens a reader, writes a new version that supersedes the reader's snapshot,
/// and runs GC — the old version must survive. After the reader closes, GC runs
/// again and reclaims it. This is the core safety property of LWM-based GC.
#[test]
fn test_gc_active_reader_pins_lwm() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1 inserts a row and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "version_1");
    db.mvcc_store.insert(tx1, row_v1.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 begins a read transaction — pins LWM at T2's begin_ts.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_begin_ts = db.mvcc_store.txs.get(&tx2).unwrap().value().begin_ts;

    // T3 updates the row and commits, creating a superseded version.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row_v2 = generate_simple_string_row(table_id, 1, "version_2");
    db.mvcc_store.update(tx3, row_v2).unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // LWM should be T2's begin_ts (the active reader).
    let lwm = db.mvcc_store.compute_lwm();
    assert_eq!(
        lwm, tx2_begin_ts,
        "LWM should equal the active reader's begin_ts"
    );

    // GC should NOT remove the superseded version (its end_ts > lwm).
    let row_id = RowID::new(table_id, RowKey::Int(1));
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(
        dropped, 0,
        "GC should not remove versions visible to active reader"
    );
    {
        let entry = db.mvcc_store.rows.get(&row_id).unwrap();
        let versions = entry.value().read();
        assert_eq!(versions.len(), 2, "both versions should be retained");
    }

    // T2 still sees the old version.
    let read_row = db.mvcc_store.read(tx2, &row_id).unwrap().unwrap();
    assert_eq!(
        read_row, row_v1,
        "active reader should still see the old version"
    );

    // Close the reader transaction.
    db.mvcc_store.remove_tx(tx2);

    // LWM should now be u64::MAX.
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // GC should now remove the superseded version.
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(
        dropped, 1,
        "superseded version should be reclaimed after reader closes"
    );
    {
        let entry = db.mvcc_store.rows.get(&row_id).unwrap();
        let versions = entry.value().read();
        assert_eq!(versions.len(), 1, "only current version should remain");
    }
}

/// Index rows live in a separate SkipMap from table rows and go through their own
/// GC path (gc_index_row_versions). This SQL-level test creates an indexed table,
/// checkpoints, updates the row (creating superseded index versions), checkpoints
/// again, and verifies the index still returns correct results. Catches regressions
/// where index GC removes versions that the dual cursor still needs.
#[test]
fn test_gc_e2e_index_rows_collected_after_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();

    // Checkpoint flushes to B-tree and triggers GC on both table and index rows.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After GC, reads should still work via B-tree fallthrough.
    let rows = get_rows(&conn, "SELECT val FROM t ORDER BY val");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].to_string(), "alpha");
    assert_eq!(rows[1][0].to_string(), "beta");

    // Index scan should also work.
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'alpha'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    // Update a row — creates new index versions.
    conn.execute("UPDATE t SET val = 'gamma' WHERE id = 1")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Old index entry ('alpha') should be gone, new entry ('gamma') visible.
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'alpha'");
    assert_eq!(rows.len(), 0);
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'gamma'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

// ─── GC quickcheck property tests ────────────────────────────────────────

/// Represents a version chain entry for quickcheck.
#[derive(Debug, Clone)]
struct ArbitraryVersionChain {
    versions: Vec<RowVersion>,
    lwm: u64,
    ckpt_max: u64,
}

/// Generates RowVersions matching realistic MVCC states.
/// Only produces valid (begin, end) combinations that can actually occur.
fn arbitrary_row_version(g: &mut Gen) -> RowVersion {
    // Weight toward realistic states:
    // 32% current (Timestamp, None), 24% superseded (Timestamp, Timestamp),
    // 8% aborted (None, None), 8% pending insert (TxID, None),
    // 8% pending delete (Timestamp, TxID), 20% B-tree tombstone (None, Timestamp)
    let kind = u8::arbitrary(g) % 25;
    let (begin, end) = match kind {
        0..=7 => {
            // Current committed version
            let b = u64::arbitrary(g) % 20 + 1;
            (Some(TxTimestampOrID::Timestamp(b)), None)
        }
        8..=13 => {
            // Superseded version
            let b = u64::arbitrary(g) % 15 + 1;
            let e = b + u64::arbitrary(g) % 10 + 1;
            (
                Some(TxTimestampOrID::Timestamp(b)),
                Some(TxTimestampOrID::Timestamp(e)),
            )
        }
        14..=15 => {
            // Aborted garbage
            (None, None)
        }
        16..=17 => {
            // Pending insert
            let t = u64::arbitrary(g) % 20 + 1;
            (Some(TxTimestampOrID::TxID(t)), None)
        }
        18..=19 => {
            // Pending delete
            let b = u64::arbitrary(g) % 15 + 1;
            let t = u64::arbitrary(g) % 20 + 1;
            (
                Some(TxTimestampOrID::Timestamp(b)),
                Some(TxTimestampOrID::TxID(t)),
            )
        }
        20..=24 => {
            // B-tree tombstone (begin=None, end=e) — row existed before MVCC, then deleted
            let e = u64::arbitrary(g) % 20 + 1;
            (None, Some(TxTimestampOrID::Timestamp(e)))
        }
        _ => unreachable!(),
    };

    RowVersion {
        id: 0,
        begin,
        end,
        row: generate_simple_string_row((-2).into(), 1, "qc"),
        btree_resident: bool::arbitrary(g),
    }
}

impl Arbitrary for ArbitraryVersionChain {
    fn arbitrary(g: &mut Gen) -> Self {
        // 1..8 versions (no empty chains — they trivially pass all properties)
        let len = usize::arbitrary(g) % 8 + 1;
        let versions: Vec<RowVersion> = (0..len).map(|_| arbitrary_row_version(g)).collect();
        // Include boundary values with ~20% probability each.
        let lwm = match u8::arbitrary(g) % 5 {
            0 => 0,
            1 => u64::MAX, // blocking checkpoint case
            _ => u64::arbitrary(g) % 30,
        };
        let ckpt_max = match u8::arbitrary(g) % 5 {
            0 => 0,        // no checkpoint has run
            1 => u64::MAX, // everything checkpointed
            _ => u64::arbitrary(g) % 30,
        };
        Self {
            versions,
            lwm,
            ckpt_max,
        }
    }
}

/// GC only removes versions — it never synthesizes new ones. For any input chain,
/// the output must be a subset (same length or shorter).
#[quickcheck]
fn prop_gc_never_increases_version_count(chain: ArbitraryVersionChain) -> bool {
    let before = chain.versions.len();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
    versions.len() <= before
}

/// Running GC twice with the same LWM and ckpt_max must produce the same result
/// as running it once. A non-idempotent GC would indicate that GC output triggers
/// further removals on re-evaluation, which means the first pass missed something.
/// Compares actual version content (begin/end), not just chain length.
#[quickcheck]
fn prop_gc_is_idempotent(chain: ArbitraryVersionChain) -> bool {
    let mut v1 = chain.versions.clone();
    MvStore::<MvccClock>::gc_version_chain(&mut v1, chain.lwm, chain.ckpt_max);
    let snapshot = v1.clone();
    MvStore::<MvccClock>::gc_version_chain(&mut v1, chain.lwm, chain.ckpt_max);
    // Compare content, not just length — a swap bug would pass a length-only check.
    v1.len() == snapshot.len()
        && v1
            .iter()
            .zip(snapshot.iter())
            .all(|(a, b)| a.begin == b.begin && a.end == b.end)
}

/// Aborted garbage (begin=None, end=None) is invisible to every transaction and
/// has no B-tree implications. GC must remove all of it unconditionally (Rule 1).
/// No aborted garbage should survive a GC pass, regardless of LWM or ckpt_max.
#[quickcheck]
fn prop_gc_removes_all_aborted_garbage(chain: ArbitraryVersionChain) -> bool {
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
    versions
        .iter()
        .all(|rv| !matches!((&rv.begin, &rv.end), (None, None)))
}

/// Uncommitted inserts (begin=TxID, end=None) belong to an in-flight transaction.
/// GC cannot know whether it will commit or abort, so it must never touch them.
/// Verifies all such versions survive GC regardless of other chain contents.
#[quickcheck]
fn prop_gc_retains_txid_begins(chain: ArbitraryVersionChain) -> bool {
    let txid_begins_before: usize = chain
        .versions
        .iter()
        .filter(|rv| matches!(&rv.begin, Some(TxTimestampOrID::TxID(_))) && rv.end.is_none())
        .count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
    let txid_begins_after: usize = versions
        .iter()
        .filter(|rv| matches!(&rv.begin, Some(TxTimestampOrID::TxID(_))) && rv.end.is_none())
        .count();
    // Active uncommitted versions (begin=TxID, end=None) are never aborted garbage
    // and don't match rule 2 or 3, so they should be retained.
    txid_begins_after == txid_begins_before
}

/// Uncommitted deletions (end=TxID) represent a pending delete by an in-flight
/// transaction. Rule 2 only matches end=Timestamp, so these must be retained.
/// Verifies GC never removes versions with TxID end markers.
#[quickcheck]
fn prop_gc_retains_txid_ends(chain: ArbitraryVersionChain) -> bool {
    // Versions with end=TxID and non-None begin are not matched by any removal
    // rule (rule 1 requires (None,None), rule 2 requires end=Timestamp).
    let filter =
        |rv: &&RowVersion| matches!(&rv.end, Some(TxTimestampOrID::TxID(_))) && rv.begin.is_some();
    let txid_ends_before: usize = chain.versions.iter().filter(filter).count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
    let txid_ends_after: usize = versions.iter().filter(filter).count();
    txid_ends_after == txid_ends_before
}

/// Current versions (begin=Timestamp(b), end=None) are not removable before they
/// are checkpointed. Forces ckpt_max=0 and verifies all committed current versions
/// survive.
#[quickcheck]
fn prop_gc_current_versions_protected_before_checkpoint(chain: ArbitraryVersionChain) -> bool {
    let current_before: usize = chain
        .versions
        .iter()
        .filter(|rv| {
            matches!(
                (&rv.begin, &rv.end),
                (Some(TxTimestampOrID::Timestamp(_)), None)
            )
        })
        .count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(&mut versions, chain.lwm, 0);
    let current_after: usize = versions
        .iter()
        .filter(|rv| {
            matches!(
                (&rv.begin, &rv.end),
                (Some(TxTimestampOrID::Timestamp(_)), None)
            )
        })
        .count();
    current_after == current_before
}

/// When a row has been deleted but the deletion isn't checkpointed yet, the
/// tombstone (superseded version with end > ckpt_max) is the only thing preventing
/// the dual cursor from reading a stale B-tree row. If GC empties such a chain,
/// the deleted row reappears. Verifies GC never empties a chain that has
/// uncheckpointed tombstones and no committed current version to take over.
#[quickcheck]
fn prop_gc_tombstone_guard_preserves_btree_safety(chain: ArbitraryVersionChain) -> bool {
    // If a chain has only superseded versions (no committed current) and at
    // least one has e > ckpt_max, GC must not empty the chain — removing all
    // versions would let the dual cursor fall through to a stale B-tree row.
    let mut versions = chain.versions.clone();
    MvStore::<MvccClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);

    // Check: if pre-GC chain had no committed current version AND had a
    // superseded version with e > ckpt_max, post-GC chain must not be empty.
    let had_committed_current = chain
        .versions
        .iter()
        .any(|rv| rv.end.is_none() && matches!(&rv.begin, Some(TxTimestampOrID::Timestamp(_))));
    let had_uncheckpointed_tombstone = chain
        .versions
        .iter()
        .any(|rv| matches!(&rv.end, Some(TxTimestampOrID::Timestamp(e)) if *e > chain.ckpt_max));
    // Only non-garbage versions matter (aborted garbage is always removed first)
    let had_non_garbage = chain
        .versions
        .iter()
        .any(|rv| !matches!((&rv.begin, &rv.end), (None, None)));

    if !had_committed_current && had_uncheckpointed_tombstone && had_non_garbage {
        !versions.is_empty()
    } else {
        true // no constraint in this case
    }
}

/// Superseded versions without a committed current version are dangerous — their
/// presence tells the dual cursor "this row was modified" but there's no current
/// version to serve reads. GC must only leave such orphans when they're justifiably
/// retained: still visible to a reader (e > lwm), guarding an uncheckpointed
/// deletion (e > ckpt_max).
#[quickcheck]
fn prop_gc_no_orphaned_superseded_versions(chain: ArbitraryVersionChain) -> bool {
    // After GC, if a chain has superseded versions without a committed current
    // version, each superseded version must be justifiably retained:
    // - e > lwm (Rule 2 didn't fire — still visible to some reader)
    // - e > ckpt_max (tombstone guard — deletion not yet in B-tree)
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);

    let has_committed_current = versions
        .iter()
        .any(|rv| rv.end.is_none() && matches!(&rv.begin, Some(TxTimestampOrID::Timestamp(_))));
    let has_superseded = versions.iter().any(|rv| {
        matches!(
            (&rv.begin, &rv.end),
            (
                Some(TxTimestampOrID::Timestamp(_)),
                Some(TxTimestampOrID::Timestamp(_))
            )
        )
    });

    if has_superseded && !has_committed_current {
        versions
            .iter()
            .filter(|rv| {
                matches!(
                    (&rv.begin, &rv.end),
                    (
                        Some(TxTimestampOrID::Timestamp(_)),
                        Some(TxTimestampOrID::Timestamp(_))
                    )
                )
            })
            .all(|rv| {
                if let Some(TxTimestampOrID::Timestamp(e)) = &rv.end {
                    *e > chain.lwm || *e > chain.ckpt_max
                } else {
                    false
                }
            })
    } else {
        true
    }
}

/// Test that a transaction cannot see uncommitted changes from another transaction.
/// This verifies snapshot isolation.
#[test]
fn test_mvcc_snapshot_isolation() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let conn1 = db.connect();
    conn1
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    // Start tx1 and read initial values
    conn1.execute("BEGIN CONCURRENT").unwrap();
    let rows1 = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(rows1[0][0].to_string(), "200");

    // Start tx2 and modify the same row
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("UPDATE t SET value = 999 WHERE id = 2")
        .unwrap();
    conn2.execute("COMMIT").unwrap();

    // Tx1 should still see the old value (snapshot isolation)
    let rows1_again = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(
        rows1_again[0][0].to_string(),
        "200",
        "Tx1 should not see tx2's committed changes"
    );

    conn1.execute("COMMIT").unwrap();

    // After tx1 commits, new reads should see tx2's changes
    let rows_after = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(rows_after[0][0].to_string(), "999");
}
/// Similar test but with the constraint error happening on the third unique column.
/// This tests that ALL previous index modifications are rolled back.
/// Uses interactive transaction (BEGIN CONCURRENT) to reproduce the bug.
#[test]
fn test_update_three_unique_columns_partial_rollback() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create table with three unique columns
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            col_a TEXT UNIQUE,
            col_b REAL UNIQUE,
            col_c INTEGER UNIQUE
        )",
    )
    .unwrap();

    // Insert two rows
    conn.execute("INSERT INTO t VALUES (1, 'a1', 1.0, 100)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'a2', 2.0, 200)")
        .unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try to UPDATE row 1 with:
    // - col_a = 'new_a' (index modified)
    // - col_b = 3.0 (index modified)
    // - col_c = 200 (FAIL - conflicts with row 2)
    // BUG: col_a and col_b index changes are NOT rolled back!
    let result =
        conn.execute("UPDATE t SET col_a = 'new_a', col_b = 3.0, col_c = 200 WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on col_c"
    );

    // COMMIT - in buggy case, this commits partial index changes
    conn.execute("COMMIT").unwrap();

    // Now try to UPDATE the same row - this should work but may crash
    // if col_a or col_b index entries are inconsistent
    conn.execute("UPDATE t SET col_a = 'updated_a', col_b = 5.0, col_c = 500 WHERE id = 1")
        .unwrap();

    // Verify index lookups work
    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_a = 'updated_a'");
    assert_eq!(rows.len(), 1);

    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_b = 5.0");
    assert_eq!(rows.len(), 1);

    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_c = 500");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that simulates the exact sequence from the stress test bug:
/// Multiple interactive transactions updating the same row, with constraint errors.
///
/// From the log:
/// - tx 248: UPDATE row with pk=1.37, sets unique_col='sweet_wind_280' -> COMMIT
/// - tx 1149: BEGIN, UPDATE same row (modifies unique_col index, fails on other_unique), COMMIT
///   BUG: partial index changes from failed UPDATE are committed!
/// - tx 1324: UPDATE same row -> CRASH "IdxDelete: no matching index entry found"
#[test]
fn test_sequential_updates_with_constraint_errors() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(
            pk REAL PRIMARY KEY,
            unique_col TEXT UNIQUE,
            other_unique REAL UNIQUE
        )",
    )
    .unwrap();

    // Insert initial rows (simulating the stress test setup)
    conn.execute("INSERT INTO t VALUES (1.37, 'sweet_wind_280', 9.05)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2.13, 'other_value', 2.13)")
        .unwrap();

    // First successful update (like tx 248 in the bug)
    conn.execute("UPDATE t SET unique_col = 'cold_grass_813', other_unique = 3.90 WHERE pk = 1.37")
        .unwrap();

    // Verify the update
    let rows = get_rows(&conn, "SELECT unique_col FROM t WHERE pk = 1.37");
    assert_eq!(rows[0][0].cast_text().unwrap(), "cold_grass_813");

    // Like tx 1149: Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try an update that will fail on other_unique (conflicts with row 2)
    // The UPDATE will:
    // 1. Delete old index entry for unique_col ('cold_grass_813')
    // 2. Insert new index entry for unique_col ('new_value')
    // 3. Delete old index entry for other_unique (3.90)
    // 4. Check constraint for other_unique (2.13) -> FAIL!
    // BUG: Steps 1-3 are NOT rolled back!
    let result =
        conn.execute("UPDATE t SET unique_col = 'new_value', other_unique = 2.13 WHERE pk = 1.37");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT the transaction (like the stress test does after the error)
    // BUG: This commits the partial index changes!
    conn.execute("COMMIT").unwrap();

    // Like tx 1324: Try another update on the same row
    // If partial changes were committed:
    // - Table row has unique_col = 'cold_grass_813'
    // - But unique_col index has 'new_value' (not 'cold_grass_813')!
    // - This UPDATE reads 'cold_grass_813' from table, tries to delete that index entry
    // - CRASH: "IdxDelete: no matching index entry found"
    conn.execute("UPDATE t SET unique_col = 'fresh_sun_348', other_unique = 5.0 WHERE pk = 1.37")
        .unwrap();

    // Verify final state
    let rows = get_rows(
        &conn,
        "SELECT unique_col, other_unique FROM t WHERE pk = 1.37",
    );
    assert_eq!(rows[0][0].cast_text().unwrap(), "fresh_sun_348");

    // Verify index lookups work
    let rows = get_rows(&conn, "SELECT * FROM t WHERE unique_col = 'fresh_sun_348'");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that multiple successful statements in an interactive transaction
/// have their changes preserved when a subsequent statement fails.
/// This tests the statement-level savepoint functionality.
#[test]
fn test_savepoint_multiple_statements_last_fails() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Insert row 1 - success
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    // Statement 2: Insert row 2 - success
    conn.execute("INSERT INTO t VALUES (2)").unwrap();

    // Statement 3: Insert row 1 again - fails with PK violation
    let result = conn.execute("INSERT INTO t VALUES (1)");
    assert!(result.is_err(), "Expected primary key violation");

    // COMMIT - should preserve statements 1 and 2
    conn.execute("COMMIT").unwrap();

    // Verify rows 1 and 2 exist
    let rows = get_rows(&conn, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that when the same row is modified by multiple statements,
/// and the second modification fails, the first modification is preserved.
#[test]
fn test_savepoint_same_row_multiple_statements() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, other_unique INTEGER UNIQUE)")
        .unwrap();

    // Insert initial row and a row to cause conflict
    conn.execute("INSERT INTO t VALUES (1, 100, 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200, 2)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Update row 1's value to 150 - success
    conn.execute("UPDATE t SET v = 150 WHERE id = 1").unwrap();

    // Statement 2: Try to update row 1 with conflicting other_unique - fails
    let result = conn.execute("UPDATE t SET v = 175, other_unique = 2 WHERE id = 1");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT - should preserve statement 1's change (v = 150)
    conn.execute("COMMIT").unwrap();

    // Verify row 1 has v = 150 (from statement 1), not 175 (from failed statement 2)
    let rows = get_rows(&conn, "SELECT v, other_unique FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 150);
    assert_eq!(rows[0][1].as_int().unwrap(), 1); // other_unique unchanged

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that index operations are properly tracked per-statement.
/// When a statement fails after partially modifying indexes,
/// only that statement's index changes are rolled back.
#[test]
fn test_savepoint_index_multiple_statements() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            value INTEGER UNIQUE
        )",
    )
    .unwrap();

    // Insert rows
    conn.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b', 20)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Successfully change name for row 1
    conn.execute("UPDATE t SET name = 'c' WHERE id = 1")
        .unwrap();

    // Statement 2: Try to change name to 'b' (conflict with row 2) - fails
    let result = conn.execute("UPDATE t SET name = 'b' WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on name"
    );

    // COMMIT
    conn.execute("COMMIT").unwrap();

    // Verify row 1 has name 'c' from statement 1
    let rows = get_rows(&conn, "SELECT name FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].cast_text().unwrap(), "c");

    // Verify index lookups work correctly
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'c'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    // 'a' should no longer be in the index
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'a'");
    assert_eq!(rows.len(), 0);

    // 'b' should still point to row 2
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'b'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test INSERT followed by DELETE of same row, then another statement fails.
/// The insert+delete should be preserved (row shouldn't exist).
#[test]
fn test_savepoint_insert_delete_then_fail() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Insert row 1
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    // Statement 2: Delete row 1
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    // Statement 3: Try to insert with conflicting unique value - fails
    let result = conn.execute("INSERT INTO t VALUES (3, 200)");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT
    conn.execute("COMMIT").unwrap();

    // Verify row 1 does not exist (was deleted in statement 2)
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows.len(), 0);

    // Row 2 should still exist
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 2");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_delete_row_is_hidden_from_desc_unique_index_scan() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (42, 46)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 42").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY val DESC");
    assert_eq!(rows, Vec::<Vec<Value>>::new());

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_delete_row_is_skipped_by_desc_explicit_index_scan() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_val ON t(val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY val DESC");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 10);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_delete_btree_resident_row_is_skipped_by_desc_unique_index_scan() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();

    let conn = db.connect();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY val DESC");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 10);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test DELETE all B-tree rows and re-insert with same IDs in MVCC.
/// Verifies tombstones correctly shadow B-tree and new rows are visible.
///
/// This test was initially failing with "UNIQUE constraint failed: t.id"
/// Fixed by implementing dual-peek in the exists() method to check MVCC tombstones.
#[test]
fn test_mvcc_dual_cursor_delete_all_btree_reinsert() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'old1')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'old2')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();

    let conn = db.connect();
    // Delete all B-tree rows
    conn.execute("DELETE FROM t WHERE id IN (1, 2)").unwrap();
    // Re-insert with new values
    conn.execute("INSERT INTO t VALUES (1, 'new1')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'new2')").unwrap();

    // Should see new values, not old B-tree values
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1].to_string(), "new1");
    assert_eq!(rows[1][1].to_string(), "new2");
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_checkpoint_root_page_mismatch_with_index() {
    // Strategy:
    // 1. Create table1 with index, insert many rows to allocate many pages (e.g., pages 2-30)
    // 2. Create table2 with index (will get negative IDs like -35, -36)
    // 3. Insert into table2
    // 4. Checkpoint - table2 will be allocated to pages 32, 33 (after table1's pages)
    // 5. But schema update will do abs(-35) = 35, abs(-36) = 36 (WRONG!)
    // 6. Query table2 using index - will look for page 36 but data is in page 33

    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

    // Create MULTIPLE tables to consume enough page numbers
    // so that test_table's allocated pages diverge from abs(negative_id)
    for table_num in 1..=30 {
        conn.execute(format!(
            "CREATE TABLE tbl{table_num} (id INTEGER PRIMARY KEY, data TEXT)",
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE INDEX idx{table_num} ON tbl{table_num}(data)",
        ))
        .unwrap();

        // Insert data to force page allocation
        for i in 0..10 {
            let data = format!("data_{table_num}_{i}");
            conn.execute(format!("INSERT INTO tbl{table_num} VALUES ({i}, '{data}')",))
                .unwrap();
        }
    }

    println!("Created 30 tables with indexes and data");

    // Create test_table with UNIQUE index (auto-created for the key)
    conn.execute("CREATE TABLE test_table (key TEXT PRIMARY KEY, value TEXT)")
        .unwrap();

    // Check test_table's root pages (should be negative)
    let rows = get_rows(
        &conn,
        "SELECT name, rootpage FROM sqlite_schema WHERE tbl_name = 'test_table' ORDER BY name",
    );
    let table_root: i64 = rows
        .iter()
        .find(|r| r[0].to_string() == "test_table")
        .unwrap()[1]
        .to_string()
        .parse()
        .unwrap();
    let index_root: i64 = rows
        .iter()
        .find(|r| r[0].to_string().contains("autoindex"))
        .unwrap()[1]
        .to_string()
        .parse()
        .unwrap();
    assert!(
        table_root < 0,
        "test_table should have negative root before checkpoint"
    );
    assert!(
        index_root < 0,
        "test_table index should have negative root before checkpoint"
    );

    // Insert a row into test_table
    conn.execute("INSERT INTO test_table (key, value) VALUES ('test_key', 'test_value')")
        .unwrap();

    // Verify row exists before checkpoint
    let rows = get_rows(&conn, "SELECT value FROM test_table WHERE key = 'test_key'");
    assert_eq!(rows.len(), 1, "Row should exist before checkpoint");
    assert_eq!(rows[0][0].to_string(), "test_value");

    println!("Inserted row into test_table, verified it exists");

    // Run checkpoint
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    println!("Checkpoint complete");

    // Now try to query using the index - this is where the bug manifests
    // The query will use root_page from schema (e.g., abs(index_root) if bug exists)
    // But data is actually in the correct allocated page
    let rows = get_rows(&conn, "SELECT value FROM test_table WHERE key = 'test_key'");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "test_value", "Value should match");

    println!("Test passed - row found correctly after checkpoint");
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_checkpoint_drop_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')",))
            .unwrap();
    }
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After a DROP TABLE frees pages and a CREATE INDEX reuses one of those
/// freed pages as its new root, a subsequent checkpoint must not use the
/// stale table cursor (which lacks index_info) when writing index rows.
#[test]
fn test_checkpoint_drop_table_then_create_index_page_reuse() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES(1,'x')").unwrap();
    conn.execute("INSERT INTO b VALUES(1,'y')").unwrap();
    // First checkpoint writes both tables to the B-tree.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // DROP TABLE a frees its root page; CREATE INDEX may reuse it.
    conn.execute("DROP TABLE a").unwrap();
    conn.execute("CREATE INDEX new_b_v ON b(v)").unwrap();
    // Second checkpoint must handle the page reuse without panicking.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT * FROM b");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "1");
    assert_eq!(rows[0][1].to_string(), "y");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that inserting a duplicate primary key fails when the existing row
/// was committed before this transaction started (and thus is visible).
#[test]
fn test_mvcc_same_primary_key() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let conn2 = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (666)").unwrap();
    conn.execute("COMMIT").unwrap();

    // conn2 starts AFTER conn1 committed, so conn2 can see the committed row.
    // INSERT should fail with UNIQUE constraint because the row is visible.
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("INSERT INTO t VALUES (666)")
        .expect_err("duplicate key - visible committed row");
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_mvcc_same_primary_key_concurrent() {
    // Pure optimistic concurrency: both transactions can INSERT the same rowid,
    // but only one can commit (first-committer-wins based on end_ts comparison).
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let conn2 = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (666)").unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    // With pure optimistic CC, INSERT succeeds - conflict detected at commit time
    conn2.execute("INSERT INTO t VALUES (666)").unwrap();

    // First transaction commits successfully (gets lower end_ts)
    conn.execute("COMMIT").unwrap();

    // Second transaction fails at commit time (first-committer-wins)
    conn2
        .execute("COMMIT")
        .expect_err("duplicate key - first committer wins");
}

// ─── End-to-end GC + dual cursor tests ───────────────────────────────────

/// After checkpoint + GC, checkpointed current versions are removed from
/// the SkipMap. Readers must still see the data via B-tree fallthrough.
#[test]
fn test_gc_e2e_checkpointed_row_readable_after_gc() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'world')").unwrap();

    // Checkpoint flushes to B-tree and triggers GC.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After GC, the SkipMap entries should be cleared (sole-survivor rule 3),
    // and reads fall through to B-tree.
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "hello");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "world");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After deleting a B-tree row and checkpointing, the tombstone is removed
/// by GC. The deleted row must stay invisible (B-tree no longer has it).
#[turso_macros::test(encryption)]
fn test_gc_e2e_deleted_row_stays_hidden_after_gc() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'delete_me')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Restart so rows are only in B-tree.
    db.restart();
    let conn = db.connect();

    // Delete row 2 in MVCC (creates tombstone over B-tree row).
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    // Checkpoint writes the deletion to B-tree and GC removes the tombstone.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Row 2 must remain invisible.
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "keep");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After updating a B-tree row and checkpointing, GC removes old versions.
/// The updated value must be visible (from B-tree after GC).
#[turso_macros::test(encryption)]
fn test_gc_e2e_updated_row_correct_after_gc() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'original')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();
    let conn = db.connect();

    // Update in MVCC.
    conn.execute("UPDATE t SET val = 'updated' WHERE id = 1")
        .unwrap();

    // Checkpoint + GC.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Must see updated value.
    let rows = get_rows(&conn, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "updated");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Multiple checkpoints with interleaved writes. Each checkpoint triggers GC.
/// Verifies cumulative correctness across GC cycles.
#[test]
fn test_gc_e2e_multiple_checkpoint_gc_cycles() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    for i in 1..=5 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Delete rows 2, 4 and update row 3.
    conn.execute("DELETE FROM t WHERE id IN (2, 4)").unwrap();
    conn.execute("UPDATE t SET val = 30 WHERE id = 3").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Insert row 6, delete row 1.
    conn.execute("INSERT INTO t VALUES (6, 6)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 3);
    assert_eq!(rows[0][1].as_int().unwrap(), 30);
    assert_eq!(rows[1][0].as_int().unwrap(), 5);
    assert_eq!(rows[1][1].as_int().unwrap(), 5);
    assert_eq!(rows[2][0].as_int().unwrap(), 6);
    assert_eq!(rows[2][1].as_int().unwrap(), 6);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_mvcc_unique_constraint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id UNIQUE)").unwrap();
    let conn2 = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (666)").unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t VALUES (666)").unwrap();

    conn.execute("COMMIT").unwrap();
    // conn2 should see conflict with conn1's row where first conneciton changed `begin` to a Timestamp that is < than conn2's end_ts
    conn2
        .execute("COMMIT")
        .expect_err("duplicate unique - first committer wins");
}

/// Regression test for MVCC concurrent commit yield-spin deadlock.
///
/// When the VDBE encounters a yield completion (pager_commit_lock contention),
/// it must return StepResult::IO to yield control. Previously, it checked
/// `finished()` which is always true for yield completions, causing an infinite
/// spin inside a single step() call — deadlocking cooperative schedulers.
///
/// We simulate lock contention by pre-acquiring pager_commit_lock before
/// calling COMMIT, then verify step() returns IO instead of hanging.
#[test]
fn test_concurrent_commit_yield_spin() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    // Pre-acquire the pager_commit_lock to simulate another connection
    // holding it mid-commit.
    let mv_store = db.get_mvcc_store();
    let lock = &mv_store.commit_coordinator.pager_commit_lock;
    assert!(lock.write(), "should acquire lock");

    // Prepare COMMIT — step() should yield (return IO), not spin forever.
    let mut stmt = conn.prepare("COMMIT").unwrap();
    let mut returned_io = false;
    for _ in 0..100 {
        match stmt.step().unwrap() {
            crate::StepResult::IO => {
                returned_io = true;
                break;
            }
            crate::StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        returned_io,
        "step() should return IO when pager_commit_lock is contended"
    );

    // Release the lock and let the commit finish
    lock.unlock();
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Done => break,
            crate::StepResult::IO => {}
            _ => {}
        }
    }

    // Verify the insert is visible
    let rows = get_rows(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
}

fn abandon_commit_after_first_io(conn: &Arc<Connection>, mv_store: &Arc<MvStore<MvccClock>>) {
    let lock = &mv_store.commit_coordinator.pager_commit_lock;
    assert!(lock.write(), "should acquire commit lock");

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), crate::StepResult::IO),
        "COMMIT should yield while the commit lock is held",
    );

    drop(stmt);
    lock.unlock();
    conn.close().unwrap();
}

#[test]
fn test_abandoned_commit_rolls_back_insert_with_injected_yield() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new());
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), crate::StepResult::IO),
        "MVCC commit should yield before completion",
    );

    drop(stmt);
    conn.close().unwrap();

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id FROM t WHERE id = 1");
    assert!(
        rows.is_empty(),
        "row from abandoned INSERT commit remained visible: {rows:?}",
    );
    observer.close().unwrap();
}

/// if a txn made some inserts, then aborted (or abandoned due to some IO issue), then those
/// inserted rows should not be visible
#[test]
fn test_abandoned_commit_rolls_back_insert() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();

    let mv_store = db.get_mvcc_store();
    abandon_commit_after_first_io(&conn, &mv_store);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id FROM t WHERE id = 1");
    assert!(
        rows.is_empty(),
        "row from abandoned INSERT commit remained visible: {rows:?}",
    );
    observer.close().unwrap();
}

/// if a txn deleted some existing rows, but then aborted (or abandoned due to some IO issue), then
/// those rows should not become deleted
#[test]
fn test_abandoned_commit_rolls_back_delete() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    let mv_store = db.get_mvcc_store();
    abandon_commit_after_first_io(&conn, &mv_store);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id, v FROM t WHERE id = 1");
    assert_eq!(
        rows,
        vec![vec![
            Value::Numeric(Numeric::Integer(1)),
            Value::Text(Text::new("seed".to_string())),
        ]],
        "row disappeared after abandoned DELETE commit: {rows:?}",
    );
    observer.close().unwrap();
}

/// ALTER TABLE RENAME TO on a table with a CREATE INDEX panics on the next
/// session open. Reproduces the issue with 3 separate sessions (DB restarts).
#[test]
fn test_alter_table_rename_with_index_panics_on_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // Session 1: Create indexed table + checkpoint
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("CREATE TABLE old_name(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_val ON old_name(val)")
            .unwrap();
        conn.execute("INSERT INTO old_name VALUES (1, 'a')")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 2: Rename table
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("ALTER TABLE old_name RENAME TO new_name")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 3: PANIC
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let rows = get_rows(&conn, "SELECT * FROM new_name");
        assert_eq!(rows.len(), 1);
    }
}

/// Same as above but with a UNIQUE constraint (autoindex).
#[test]
fn test_alter_table_rename_with_unique_constraint_panics_on_restart() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // Session 1
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("CREATE TABLE old_name(id INTEGER PRIMARY KEY, val TEXT UNIQUE)")
            .unwrap();
        conn.execute("INSERT INTO old_name VALUES (1, 'a')")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 2
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("ALTER TABLE old_name RENAME TO new_name")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 3: PANIC
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let rows = get_rows(&conn, "SELECT * FROM new_name");
        assert_eq!(rows.len(), 1);
    }
}

/// Reproducer: DROP TABLE ghost data after restart without explicit checkpoint.
/// Session 1: create + insert + checkpoint. Session 2: drop. Session 3: reopen.
#[test]
fn test_close_persists_drop_table() {
    // Session 1: create table, insert data, checkpoint to DB file
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE todrop(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO todrop VALUES (1, 'data')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();

    // Session 2: drop table (no explicit checkpoint, rely on close)
    let conn = db.connect();
    conn.execute("DROP TABLE todrop").unwrap();
    conn.close().unwrap();

    // Session 3: reopen — table must be gone
    db.restart();
    let conn = db.connect();

    // The table must not exist — CREATE should succeed
    let create_result = conn.execute("CREATE TABLE todrop(id INTEGER PRIMARY KEY, newval TEXT)");
    assert!(
        create_result.is_ok(),
        "CREATE TABLE should succeed after DROP, but got: {:?}",
        create_result.unwrap_err()
    );

    // No ghost data from old table
    let rows = get_rows(&conn, "SELECT * FROM todrop");
    assert!(rows.is_empty(), "New table should be empty, got {rows:?}");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Reproducer: DROP INDEX ghost pages after restart without explicit checkpoint.
/// Session 1: create table + index + insert + checkpoint. Session 2: drop index. Session 3: reopen.
#[test]
fn test_close_persists_drop_index() {
    // Session 1: create table/index, insert data, checkpoint to DB file
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE tdropidx(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_tdropidx_val ON tdropidx(val)")
        .unwrap();
    conn.execute("INSERT INTO tdropidx VALUES (1, 'data')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();

    // Session 2: drop index (no explicit checkpoint, rely on close)
    let conn = db.connect();
    conn.execute("DROP INDEX idx_tdropidx_val").unwrap();
    conn.close().unwrap();

    // Session 3: reopen - index must be gone and integrity check must pass
    db.restart();
    let conn = db.connect();

    let recreate_index = conn.execute("CREATE INDEX idx_tdropidx_val ON tdropidx(val)");
    assert!(
        recreate_index.is_ok(),
        "CREATE INDEX should succeed after DROP INDEX, but got: {:?}",
        recreate_index.unwrap_err()
    );

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_partial_commit_visibility_bug() {
    use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use crate::sync::Arc;
    use std::collections::HashMap;
    use std::thread;
    use std::time::Duration;
    for _ in 0..10 {
        // Setup: Create a table with batch_id and row_num columns
        let db = Arc::new(MvccTestDbNoConn::new_with_random_db());
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE consistency_test (batch_id INTEGER, row_num INTEGER)")
                .unwrap();
        }

        const ROWS_PER_BATCH: i64 = 50; // Large enough to increase race window
        const NUM_BATCHES: u64 = 100;
        const NUM_READER_THREADS: usize = 4;

        let writer_done = Arc::new(AtomicBool::new(false));
        let violation_detected = Arc::new(AtomicBool::new(false));
        let current_batch = Arc::new(AtomicU64::new(0));

        // Writer thread: Insert batches of rows
        let writer_handle = {
            let db = db.clone();
            let writer_done = writer_done.clone();
            let current_batch = current_batch.clone();
            thread::spawn(move || {
                let conn = db.connect();

                for batch_id in 0..NUM_BATCHES {
                    // Start a transaction
                    conn.execute("BEGIN CONCURRENT").unwrap();

                    // Insert ROWS_PER_BATCH rows with the same batch_id
                    // This simulates a multi-row operation like a bank transfer
                    for row_num in 0..ROWS_PER_BATCH {
                        conn.execute(format!(
                            "INSERT INTO consistency_test VALUES ({batch_id}, {row_num})",
                        ))
                        .unwrap();
                    }

                    // Update current batch before committing to allow readers to check
                    current_batch.store(batch_id, Ordering::Release);

                    // Commit the transaction
                    // BUG LOCATION: During commit, the loop at mod.rs:912-984 updates
                    // row timestamps one-by-one while state remains Preparing.
                    // Concurrent readers can see partial updates.
                    conn.execute("COMMIT").unwrap();

                    // Small delay to allow readers to observe the race window
                    thread::sleep(Duration::from_micros(100));
                }

                writer_done.store(true, Ordering::Release);
            })
        };

        // Reader threads: Continuously read and verify batch consistency
        let mut reader_handles = Vec::new();
        for reader_id in 0..NUM_READER_THREADS {
            let db = db.clone();
            let writer_done = writer_done.clone();
            let violation_detected = violation_detected.clone();
            let current_batch = current_batch.clone();

            let handle = thread::spawn(move || {
                let conn = db.connect();
                let mut iteration = 0u64;

                loop {
                    iteration += 1;

                    // Start a new transaction to get a fresh snapshot
                    // Snapshot isolation: This snapshot should see a consistent state
                    conn.execute("BEGIN CONCURRENT").unwrap();

                    // Read all rows grouped by batch_id
                    let rows = get_rows(
                        &conn,
                        "SELECT batch_id, row_num FROM consistency_test ORDER BY batch_id, row_num",
                    );

                    // Group rows by batch_id
                    let mut batches: HashMap<i64, Vec<i64>> = HashMap::new();
                    for row in rows {
                        let batch_id = row[0].as_int().unwrap();
                        let row_num = row[1].as_int().unwrap();
                        batches.entry(batch_id).or_default().push(row_num);
                    }

                    // Check consistency: Each batch must have EITHER all rows OR no rows
                    for (batch_id, row_nums) in &batches {
                        let count = row_nums.len() as i64;

                        // CRITICAL ASSERTION: Snapshot isolation guarantees atomic visibility
                        // A batch is either fully committed (all 50 rows) or not yet committed (0 rows)
                        //
                        // If we see a partial batch (e.g., 23 rows), it means:
                        // 1. The commit loop updated timestamps for rows 0-22 (visible)
                        // 2. Transaction still in Preparing state
                        // 3. Rows 23-49 still have TxID (invisible to us)
                        // 4. We started our snapshot DURING the commit loop
                        //
                        // This is a SNAPSHOT ISOLATION VIOLATION.
                        if count != 0 && count != ROWS_PER_BATCH {
                            eprintln!(
                                "[Reader {reader_id}] VIOLATION DETECTED at iteration {iteration}!",
                            );
                            eprintln!(
                                "  Batch {batch_id} has {count} rows (expected {ROWS_PER_BATCH} or 0)",
                            );
                            eprintln!("  Visible row_nums: {row_nums:?}");
                            eprintln!();
                            eprintln!("  EXPLANATION:");
                            eprintln!(
                                "  - This reader started a snapshot during batch {batch_id}'s commit",
                            );
                            eprintln!(
                                "  - The commit loop (mod.rs:912-984) was updating timestamps"
                            );
                            eprintln!("  - Transaction state was still Preparing(ts)");
                            eprintln!("  - Rows with updated Timestamps became visible");
                            eprintln!("  - Rows with TxID timestamps remained invisible");
                            eprintln!("  - Result: Partial batch visibility (atomicity violation)");
                            eprintln!();
                            eprintln!("  RACE TIMELINE:");
                            eprintln!("  1. Writer: state = Preparing(end_ts)");
                            eprintln!("  2. Writer: Update row 0's timestamp");
                            eprintln!("  3. Writer: Update row 1's timestamp");
                            eprintln!("  ...");
                            eprintln!("  N. Reader: BEGIN (snapshot)");
                            eprintln!(
                                "  N+1. Reader: Read rows 0-{} (visible via Timestamp)",
                                count - 1
                            );
                            eprintln!(
                                "  N+2. Reader: Read rows {}-{} (invisible, still TxID)",
                                count,
                                ROWS_PER_BATCH - 1
                            );
                            eprintln!("  N+3. Writer: Continue updating remaining timestamps...");

                            violation_detected.store(true, Ordering::Release);

                            // Continue to accumulate more evidence
                        }
                    }

                    conn.execute("COMMIT").unwrap();

                    // Exit if writer is done and we've checked a few more times
                    if writer_done.load(Ordering::Acquire) {
                        let final_batch = current_batch.load(Ordering::Acquire);
                        if iteration > final_batch + 10 {
                            break;
                        }
                    }

                    // Small delay to vary timing
                    thread::sleep(Duration::from_micros(50));
                }

                eprintln!("[Reader {reader_id}] Completed {iteration} iterations");
            });

            reader_handles.push(handle);
        }

        // Wait for writer to complete
        writer_handle.join().unwrap();

        // Wait for readers to complete
        for handle in reader_handles {
            handle.join().unwrap();
        }

        // ASSERTION: No violations should be detected
        // With the current bug, this will FAIL because readers observe partial commits
        assert!(
            !violation_detected.load(Ordering::Acquire),
            "Partial commit visibility detected! Transaction atomicity violated.\n\
         \n\
         ROOT CAUSE: Commit loop (mod.rs:912-984) updates row timestamps non-atomically\n\
         while transaction state remains Preparing. Concurrent readers see inconsistent\n\
         snapshots with partial transaction visibility.\n\
         \n\
         FIX REQUIRED: Make timestamp updates atomic, or change visibility logic to\n\
         always dereference transaction state instead of reading row timestamps directly."
        );
    }
}

/// Two concurrent transactions delete the same B-tree-resident row that has a
/// UNIQUE index. Both DELETEs succeed at execute time because tombstones
/// (begin: None) are invisible to is_visible_to(), so both transactions
/// create independent tombstones. However, commit-time validation in
/// check_version_conflicts detects the other transaction's tombstone as a
/// write lock (via its end: TxID field) and rejects the second committer
/// with WriteWriteConflict.
#[test]
fn test_double_delete_btree_resident_row_with_unique_index() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER, uniq TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 10, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES(2, 20, 'b')").unwrap();

    // Checkpoint so rows are only in B-tree, not in MVCC store
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    // Two transactions both try to delete the same B-tree-resident row
    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1 deletes row 1 — creates tombstone (begin: None, end: TxID(T1))
    conn1.execute("DELETE FROM t WHERE id = 1").unwrap();

    // T2 deletes the same row — creates a second tombstone at execute time
    // (is_visible_to still returns false for tombstones, so operation-time
    // conflict detection is bypassed — that's a separate issue)
    conn2.execute("DELETE FROM t WHERE id = 1").unwrap();

    // T1 commits first — stamps its tombstones with Timestamp
    conn1.execute("COMMIT").unwrap();

    // T2's commit should fail: check_version_conflicts now detects T1's
    // committed tombstone (end: Timestamp >= T2.begin_ts)
    assert!(
        conn2.execute("COMMIT").is_err(),
        "T2's COMMIT should fail with WriteWriteConflict when T1 already \
         committed a tombstone for the same row"
    );
    drop(conn1);
    drop(conn2);

    // Checkpoint: only T1's delete should have gone through
    let conn = db.connect();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        &rows[0][0].to_string(),
        "ok",
        "Index corruption after concurrent double-delete of B-tree-resident row"
    );
}

/// AUTOINCREMENT is not supported in MVCC mode due to sqlite_sequence
/// corruption with concurrent transactions. Verify that CREATE TABLE
/// with AUTOINCREMENT and INSERT into AUTOINCREMENT tables are blocked.
#[test]
fn test_autoincrement_blocked_in_mvcc() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // CREATE TABLE with AUTOINCREMENT should fail in MVCC mode
    let result = conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)");
    assert!(
        result.is_err(),
        "CREATE TABLE with AUTOINCREMENT should fail in MVCC mode"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("AUTOINCREMENT is not supported in MVCC mode"),
        "unexpected error: {err}"
    );

    // Regular tables without AUTOINCREMENT should still work
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
}

/// If a table with AUTOINCREMENT was created before MVCC was enabled,
/// INSERT into that table should still be blocked in MVCC mode.
#[test]
fn test_autoincrement_insert_blocked_for_preexisting_table() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join(format!("test_{}", rand::random::<u64>()));
    let path_str = path.to_str().unwrap();
    let io = Arc::new(PlatformIO::new().unwrap());

    // Phase 1: Open in WAL mode, create AUTOINCREMENT table
    {
        let db = crate::Database::open_file_with_flags(
            io.clone(),
            path_str,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t(b) VALUES ('before_mvcc')")
            .unwrap();
        conn.close().unwrap();
    }

    // Clear the database manager to force a fresh open
    {
        let mut manager = crate::DATABASE_MANAGER.lock();
        manager.clear();
    }

    // Phase 2: Reopen in MVCC mode — INSERT should be blocked
    {
        let db = crate::Database::open_file_with_flags(
            io,
            path_str,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();

        let result = conn.execute("INSERT INTO t(b) VALUES ('in_mvcc')");
        assert!(
            result.is_err(),
            "INSERT into AUTOINCREMENT table should fail in MVCC mode"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("AUTOINCREMENT is not supported in MVCC mode"),
            "unexpected error: {err}"
        );
    }
}

/// Two concurrent MVCC transactions inserting into an AUTOINCREMENT table must
/// both succeed. Before the fix, the second transaction would fail with a
/// WriteWriteConflict on the sqlite_sequence metadata table.
#[test]
#[ignore = "AUTOINCREMENT not yet supported in MVCC mode"]
fn test_concurrent_autoincrement_inserts() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();

    conn1
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // Tx1: begin and insert
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("INSERT INTO t(b) VALUES ('from_tx1')")
        .unwrap();

    // Tx2: begin and insert (while tx1 is still open)
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("INSERT INTO t(b) VALUES ('from_tx2')")
        .unwrap();

    // Both commits must succeed
    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Verify both rows are present with distinct, increasing rowids
    let rows = get_rows(&conn1, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 2, "both inserts should be visible");
    let rowid1 = rows[0][0].as_int().unwrap();
    let rowid2 = rows[1][0].as_int().unwrap();
    assert!(rowid1 < rowid2, "rowids must be strictly increasing");
    assert_eq!(rows[0][1].to_string(), "from_tx1");
    assert_eq!(rows[1][1].to_string(), "from_tx2");
}

/// After concurrent autoincrement inserts and a checkpoint, sqlite_sequence
/// must reflect the true maximum rowid.
#[test]
#[ignore = "AUTOINCREMENT not yet supported in MVCC mode"]
fn test_autoincrement_sqlite_sequence_after_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();

    conn1
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // Insert several rows from separate transactions
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1.execute("INSERT INTO t(b) VALUES ('row1')").unwrap();
    conn1.execute("COMMIT").unwrap();

    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t(b) VALUES ('row2')").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Force a checkpoint to flush autoincrement entries
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After checkpoint, sqlite_sequence should have the correct max (2)
    let rows = get_rows(&conn1, "SELECT seq FROM sqlite_sequence WHERE name = 't'");
    assert_eq!(rows.len(), 1, "sqlite_sequence should have entry for 't'");
    let seq = rows[0][0].as_int().unwrap();
    assert_eq!(seq, 2, "sqlite_sequence should reflect the max rowid");

    // A subsequent insert should get rowid 3
    conn1.execute("INSERT INTO t(b) VALUES ('row3')").unwrap();
    let rows = get_rows(&conn1, "SELECT MAX(a) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 3);
}

/// Three concurrent transactions all inserting into the same AUTOINCREMENT table
/// must all succeed and produce unique, increasing rowids.
#[test]
#[ignore = "AUTOINCREMENT not yet supported in MVCC mode"]
fn test_three_concurrent_autoincrement_inserts() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    let conn1 = db.connect();
    let conn2 = db.connect();
    let conn3 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1.execute("INSERT INTO t(b) VALUES ('tx1')").unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t(b) VALUES ('tx2')").unwrap();

    conn3.execute("BEGIN CONCURRENT").unwrap();
    conn3.execute("INSERT INTO t(b) VALUES ('tx3')").unwrap();

    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();
    conn3.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 3, "all three inserts should be visible");
    let ids: Vec<i64> = rows.iter().map(|r| r[0].as_int().unwrap()).collect();
    assert!(
        ids[0] < ids[1] && ids[1] < ids[2],
        "rowids must be strictly increasing: {ids:?}"
    );
}

/// Deterministic reproduction of the sqlite_sequence pollution bug.
///
/// Two concurrent transactions insert into an AUTOINCREMENT table.
/// Tx1 gets data rowid 1, tx2 gets data rowid 2. Tx1 commits first,
/// so its sqlite_sequence row (name='t', seq=1) gets sqlite_sequence
/// rowid 1. Tx2 commits second, so its sqlite_sequence row (name='t',
/// seq=2) gets sqlite_sequence rowid 2.
///
/// After DELETE + checkpoint + restart, the table is empty (btree max = 0).
/// init_autoincrement scans sqlite_sequence by rowid order, finds the FIRST
/// match at sqlite_sequence rowid 1 with seq=1, and uses that.
/// New rowid = max(1, 0) + 1 = 2, which REUSES the previously-used rowid 2.
///
/// This violates AUTOINCREMENT's contract that rowids must never decrease.
#[test]
#[ignore = "AUTOINCREMENT not yet supported in MVCC mode"]
fn test_autoincrement_no_reuse_after_delete_and_restart() {
    let _ = tracing_subscriber::fmt().try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();

    conn1
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // Two concurrent transactions: tx1 commits first, tx2 commits second.
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("INSERT INTO t(b) VALUES ('from_tx1')")
        .unwrap();

    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("INSERT INTO t(b) VALUES ('from_tx2')")
        .unwrap();

    // Commit tx1 first: its sqlite_sequence row gets the lower rowid
    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Verify: data rowids are 1 and 2
    let rows = get_rows(&conn1, "SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    let max_data_rowid = rows[1][0].as_int().unwrap();
    assert_eq!(max_data_rowid, 2);

    // sqlite_sequence should have duplicate rows (the bug):
    // rowid=1: name=t, seq=1  (from tx1, committed first)
    // rowid=2: name=t, seq=2  (from tx2, committed second)
    let seq_rows = get_rows(
        &conn1,
        "SELECT rowid, seq FROM sqlite_sequence WHERE name = 't' ORDER BY rowid",
    );
    // If there's only 1 row with the correct max, the fix is applied.
    // If there are 2 rows, the bug is present and init_autoincrement will
    // pick the wrong one after restart.
    let seq_count = seq_rows.len();

    // Delete all data rows so btree max becomes 0 after restart
    conn1.execute("DELETE FROM t").unwrap();

    // Checkpoint to flush everything to disk
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Drop connections and restart
    drop(conn1);
    drop(conn2);
    db.restart();

    let conn = db.connect();

    // Verify table is empty
    let rows = get_rows(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 0);

    // Insert after restart. The new rowid MUST be > max_data_rowid (2).
    conn.execute("INSERT INTO t(b) VALUES ('after_restart')")
        .unwrap();
    let rows = get_rows(&conn, "SELECT a FROM t");
    let new_rowid = rows[0][0].as_int().unwrap();

    if seq_count > 1 {
        // Bug present: sqlite_sequence has duplicate rows.
        // init_autoincrement picked the first match (seq=1), so new rowid = 2,
        // which reuses a previously-issued rowid.
        eprintln!(
            "sqlite_sequence had {seq_count} rows for 't'. \
             After restart, new rowid = {new_rowid} (previous max was {max_data_rowid})"
        );
    }

    assert!(
        new_rowid > max_data_rowid,
        "AUTOINCREMENT rowid reuse! Previous max was {max_data_rowid}, \
         but new rowid after delete+restart is {new_rowid}. \
         sqlite_sequence had {seq_count} duplicate rows; \
         init_autoincrement picked the stale one (seq=1 instead of seq=2)."
    );
}

/// Same bug as `test_elle_lost_update_exclusive_concurrent` but with simplified SQLs.
/// For this bug to happen, we need deferred conflict detection done at
/// `check_version_conflicts`.
/// Requires UPSERT (INSERT ... ON CONFLICT DO UPDATE) — to hit `insert_btree_resident`
/// and then `check_version_conflicts`.
/// (Note: plain UPDATE eagerly detects conflicts via `delete_from_table_or_index`)
///
/// We need three txns for this bug to happen:
/// Initial state: some row btree resident
/// tx2: starts
/// tx1: upserts the row, commits
/// tx3: upserts the same row, which sets end=TxID(T3) on tx1's version (speculative delete)
/// tx2: upserts the same row, should get WriteWriteConflict at commit time because tx1 committed previously
#[test]
fn test_speculative_delete_hides_committed_version_sql() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (key TEXT PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('k1', 'a')").unwrap();
        conn.close().unwrap();
    }
    // lets do this so that row becomes b tree resident
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
    }

    let upsert = |val: &str| {
        format!(
            "INSERT INTO t VALUES ('k1', '{val}') \
             ON CONFLICT(key) DO UPDATE SET val = excluded.val"
        )
    };

    // T2: begin early so T1's future commit is invisible under SI.
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: auto-commit UPSERT → insert_btree_resident, commits.
    let conn1 = db.connect();
    conn1.execute(upsert("b")).unwrap();
    conn1.close().unwrap();

    // T3: UPSERT → sets end=TxID(T3) on T1's version.
    let conn3 = db.connect();
    conn3.execute("BEGIN CONCURRENT").unwrap();
    conn3.execute(upsert("d")).unwrap();

    // T2: UPSERT → insert_btree_resident (T1 invisible, T3 invisible).
    conn2.execute(upsert("c")).unwrap();

    // T2: COMMIT → must detect conflict with T1.
    let result = conn2.execute("COMMIT");
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}."
    );
}

/// Regression test for Elle bug (https://github.com/tursodatabase/turso/actions/runs/22855976911/job/66296309873?pr=5819#logs)
/// Previously, `check_version_conflicts` skipped any version with `end.is_some()`,
/// so a speculative delete by T3 (setting end=TxID(T3)) hid T1's committed version
/// from T2's conflict check, allowing a lost update.
///
/// The SQL statements resemble the ones in Elle. For simplified variant, check
/// `test_speculative_delete_hides_committed_version_sql` test
///
/// 1. Row for key "k8" exists in B-tree (B-tree-resident, no MVCC version)
/// 2. T2 starts via BEGIN CONCURRENT
/// 3. T1 does auto-commit UPSERT on "k8" → insert_btree_resident, commits
/// 4. T3 starts via BEGIN CONCURRENT, does UPSERT on "k8" → update path sets
///    end=TxID(T3) on T1's committed version
/// 5. T2 does UPSERT on "k8" → insert_btree_resident (T1's version invisible, T3's invisible)
/// 6. T2 COMMIT → check_version_conflicts SKIPS T1's version because end.is_some()
///    → no WriteWriteConflict detected → lost update!
/// 7. T3 eventually aborts, restoring T1's version, but T2 already committed.
#[test]
fn test_elle_lost_update_exclusive_concurrent() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    // Setup: create the elle-style table and seed initial data into the B-tree
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE elle_lists (key TEXT PRIMARY KEY, vals TEXT DEFAULT '')")
            .unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("INSERT INTO elle_lists (key, vals) VALUES ('k8', '100')")
            .unwrap();
        conn.close().unwrap();
    }
    // Restart: data is only in B-tree, MVCC store is empty.
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
    }

    // T2: start concurrent transaction (begin_ts established early)
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: auto-commit UPSERT on k8 → insert_btree_resident, commits immediately
    let conn1 = db.connect();
    conn1
        .execute(
            "INSERT INTO elle_lists (key, vals) VALUES ('k8', '200') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE WHEN vals = '' THEN '200' ELSE vals || ',' || '200' END",
        )
        .unwrap();
    conn1.close().unwrap();

    // T3: start concurrent transaction (begin_ts > T1.end_ts, so T1's version IS visible to T3)
    let conn3 = db.connect();
    conn3.execute("BEGIN CONCURRENT").unwrap();

    // T3: UPSERT on k8 → update path: deletes T1's version (sets end=TxID(T3))
    // and creates T3's own version. This speculatively hides T1's version.
    conn3
        .execute(
            "INSERT INTO elle_lists (key, vals) VALUES ('k8', '400') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE WHEN vals = '' THEN '400' ELSE vals || ',' || '400' END",
        )
        .unwrap();

    // T2: UPSERT on k8 → insert_btree_resident (T1's version invisible under SI,
    // T3's version invisible as Active)
    conn2
        .execute(
            "INSERT INTO elle_lists (key, vals) VALUES ('k8', '300') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE WHEN vals = '' THEN '300' ELSE vals || ',' || '300' END",
        )
        .unwrap();

    // T2: COMMIT → should detect write-write conflict with T1's committed version.
    // BUG: T1's version has end=TxID(T3), and the old code skips versions with
    // end.is_some() → conflict missed → lost update.
    let commit_result = conn2.execute("COMMIT");
    assert!(
        matches!(&commit_result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {commit_result:?}. \
         T1's committed version was hidden by T3's speculative delete (end=TxID), \
         causing check_version_conflicts to skip it."
    );
}

/// Regression test: speculative delete by an active transaction must not hide a
/// committed version from commit-time conflict checks.
///
/// Previously, `check_version_conflicts` skipped any version with `end.is_some()`,
/// including versions where `end` was `TxID` of an active (uncommitted) transaction.
/// This allowed T2 to commit without detecting the write-write conflict with T1.
///
/// Minimal reproduction using the MvStore API directly (no SQL, no restart, no UPSERT).
/// Note: T2 begins after T1 commits, so T2 *can* see T1 under SI. We call
/// `insert_btree_resident` directly to simulate the UPSERT code path where the
/// cursor doesn't go through normal read visibility.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   T3: begin, update row 1 → sets end=TxID(T3) on T1's version
///   T2: insert_btree_resident row 1 (via API, bypassing read visibility)
///   T2: commit → must detect conflict with T1
#[test]
fn test_speculative_delete_hides_committed_version() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();

    // T3: begin, update row 1 → delete sets end=TxID(T3) on T1's version.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row_v3 = generate_simple_string_row(table_id, 1, "v3");
    assert!(db.mvcc_store.update(tx3, row_v3).unwrap());

    // T2: insert_btree_resident for the same row (called directly via API to
    // simulate the UPSERT code path that bypasses eager conflict detection).
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must fail with WriteWriteConflict.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         T3's speculative delete (end=TxID) on T1's version must not hide it from conflict checks."
    );
}

/// Verify that a committed pure delete (tombstone) is detected as a conflict.
///
/// Scenario: Td deletes a row and commits. Between Td's Commit and CommitEnd
/// (when TxID→Timestamp conversion happens), the tombstone still has
/// end=TxID(Td). T2 does insert_btree_resident for the same row and tries to
/// commit. The tombstone's begin=None, end=TxID(Td) should be caught by the
/// B-tree tombstone check in check_version_conflicts.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   Td: delete row 1, commit
///   T2: insert_btree_resident row 1
///   T2: commit → must detect conflict with Td's tombstone
#[test]
fn test_committed_delete_tombstone_conflict() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();

    // Td: delete row 1 and commit.
    let conn_d = db.db.connect().unwrap();
    let tx_d = db.mvcc_store.begin_tx(conn_d.pager.load().clone()).unwrap();
    assert!(db
        .mvcc_store
        .delete(tx_d, RowID::new(table_id, RowKey::Int(1)))
        .unwrap());
    commit_tx(db.mvcc_store.clone(), &conn_d, tx_d).unwrap();

    // T2: insert_btree_resident for the same row.
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must detect conflict with Td.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         Td's committed delete (tombstone) must be detected as a conflict."
    );
}

/// Verify that when a transaction (Td) updates a row and commits, another
/// transaction (T2) that also writes to the same row detects the conflict —
/// even though T1's version has end=TxID(Td) with Td committed.
///
/// This tests the `Committed(_) => continue` branch in `check_version_conflicts`:
/// skipping T1's version is safe because Td's NEW version (begin=TxID(Td)) catches
/// the conflict.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   Td: update row 1 (sets end=TxID(Td) on T1's version, creates new version), commit
///   T2: insert_btree_resident row 1
///   T2: commit → must detect conflict with Td's new version
#[test]
fn test_committed_update_version_conflict() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();

    // Td: update row 1 and commit.
    let conn_d = db.db.connect().unwrap();
    let tx_d = db.mvcc_store.begin_tx(conn_d.pager.load().clone()).unwrap();
    let row_vd = generate_simple_string_row(table_id, 1, "vd");
    assert!(db.mvcc_store.update(tx_d, row_vd).unwrap());
    commit_tx(db.mvcc_store.clone(), &conn_d, tx_d).unwrap();

    // T2: insert_btree_resident for the same row.
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must detect conflict with Td.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         Td's committed update must be detected via Td's new version."
    );
}

/// Encrypted MVCC: write rows, restart with same key, verify recovery replays them.
/// Then swap to a wrong key and verify that restart fails.
#[test]
fn test_mvcc_encrypted_log_recovery_and_wrong_key() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    write_synthetic_row(&db, "encrypted_value");

    // --- Verify the raw log file is encrypted (no plaintext leakage) ---
    {
        let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
        let log_bytes = std::fs::read(&log_path).expect("MVCC log file should exist");
        assert!(
            log_bytes.len() > 56,
            "MVCC log should contain data beyond the header"
        );
        let plaintext = b"encrypted_value";
        assert!(
            !log_bytes.windows(plaintext.len()).any(|w| w == plaintext),
            "MVCC log must not contain plaintext data when encryption is enabled"
        );
    }

    // --- Restart with correct key: recovery should replay the encrypted log ---
    db.restart();
    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let max_root_page = get_rows(
            &conn,
            "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
        )[0][0]
            .as_int()
            .unwrap();
        let synthetic_table_id = MVTableId::new(-(max_root_page + 100));
        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = mvcc_store
            .read(tx_id, &RowID::new(synthetic_table_id, RowKey::Int(1)))
            .unwrap()
            .unwrap();
        let record = get_record_value(&row);
        match record.get_value(0).unwrap() {
            ValueRef::Text(text) => assert_eq!(text.as_str(), "encrypted_value"),
            other => panic!("Expected Text, got {other:?}"),
        }
        conn.close().unwrap();
    }

    // --- Restart with wrong key: should fail ---
    let wrong_key = "ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    db.enc_opts = Some(crate::EncryptionOpts {
        cipher: "aes256gcm".to_string(),
        hexkey: wrong_key.to_string(),
    });
    assert!(
        db.restart_result().is_err(),
        "Expected error when reopening encrypted MVCC DB with wrong key"
    );
}

/// Enabling MVCC on a file-backed database must still bootstrap durable MVCC
/// metadata even if encryption has only been opted-in and no key/cipher exists yet.
#[test]
fn test_mvcc_late_encryption_setup_keeps_metadata_bootstrapped() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new().with_encryption(true);
    let db = Database::open_file_with_flags(
        io,
        path.as_os_str().to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Reproduce the deferred-key flow: encryption is enabled as a feature, but
    // the session has not configured any key/cipher when MVCC bootstrap runs.
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

    let metadata_root = metadata_root_page(&conn);
    assert!(
        metadata_root > 0,
        "metadata table must be present after enabling MVCC on a file-backed db",
    );

    let meta = get_rows(
        &conn,
        "SELECT k, v FROM __turso_internal_mvcc_meta ORDER BY rowid",
    );
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0][0].to_string(), "persistent_tx_ts_max");
    assert_eq!(meta[0][1].as_int().unwrap(), 0);
}

/// Reopening an encrypted MVCC database without any key material must fail before
/// logical-log recovery, even if there is an outstanding MVCC log tail on disk.
#[test]
fn test_mvcc_encrypted_restart_without_key_fails_before_recovery() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'secret')").unwrap();
        conn.close().unwrap();
    }

    let log_bytes = std::fs::read(&log_path).expect("db-log should exist after MVCC writes");
    assert!(
        log_bytes.len() > LOG_HDR_SIZE,
        "db-log should contain at least one frame before restart"
    );

    db.enc_opts = None;
    assert!(
        matches!(db.restart_result(), Err(LimboError::NotADB)),
        "reopening an encrypted MVCC database without a key must fail during db open, before recovery",
    );
}

#[test]
fn test_encrypted_recovery_large_payload_multi_chunk() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let large_value = "x".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 3);
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute(format!("INSERT INTO t VALUES (1, '{large_value}')"))
            .unwrap();
    }

    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
    assert!(log_path.exists(), "db-log should exist before restart");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    db.restart();
    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT id, length(v), substr(v, 1, 16), substr(v, length(v) - 15, 16) FROM t",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), large_value.len() as i64);
    assert_eq!(rows[0][2].to_string(), "xxxxxxxxxxxxxxxx");
    assert_eq!(rows[0][3].to_string(), "xxxxxxxxxxxxxxxx");
}

#[test]
fn test_encrypted_recovery_corrupted_later_chunk_keeps_checkpointed_prefix() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let large_value = "z".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 3);
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'survives')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute(format!("INSERT INTO t VALUES (2, '{large_value}')"))
            .unwrap();
    }

    let mut log_bytes = std::fs::read(&log_path).expect("db-log should exist");
    let payload_size = u64::from_le_bytes(
        log_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let chunk_count = payload_size.div_ceil(ENCRYPTED_PAYLOAD_CHUNK_SIZE);
    assert!(
        chunk_count >= 3,
        "expected multi-chunk encrypted recovery tail"
    );

    let enc_ctx = crate::storage::encryption::EncryptionContext::new(
        crate::storage::encryption::CipherMode::Aes256Gcm,
        &EncryptionKey::from_hex_string(hex_key).unwrap(),
        4096,
    )
    .unwrap();
    let first_chunk_on_disk_size =
        ENCRYPTED_PAYLOAD_CHUNK_SIZE + enc_ctx.tag_size() + enc_ctx.nonce_size();
    let corrupt_offset = LOG_HDR_SIZE + TX_HEADER_SIZE + first_chunk_on_disk_size + 1;
    log_bytes[corrupt_offset] ^= 0xFF;
    std::fs::write(&log_path, &log_bytes).unwrap();

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "survives");
}

/// Read the raw db-log file and verify every TX frame payload can be decrypted.
/// Panics if the file is missing, has no frames, or any payload fails to decrypt.
fn assert_log_payloads_decrypt(
    log_path: &std::path::Path,
    hex_key: &str,
    cipher: crate::storage::encryption::CipherMode,
) {
    use crate::storage::encryption::EncryptionContext;

    let log_bytes = std::fs::read(log_path).expect("db-log file should exist");
    assert!(
        log_bytes.len() > LOG_HDR_SIZE,
        "db-log should contain data beyond the header"
    );

    let key = EncryptionKey::from_hex_string(hex_key).unwrap();
    let enc_ctx = EncryptionContext::new(cipher, &key, 4096).unwrap();
    let nonce_size = enc_ctx.nonce_size();
    let tag_size = enc_ctx.tag_size();

    // Parse salt from log header (bytes 8..16, little-endian u64)
    let salt = u64::from_le_bytes(log_bytes[8..16].try_into().unwrap());

    let mut offset = LOG_HDR_SIZE;
    let mut frame_count = 0;

    while offset + TX_HEADER_SIZE + TX_TRAILER_SIZE <= log_bytes.len() {
        // TX Header: frame_magic(4) | payload_size(8) | op_count(4) | commit_ts(8)
        let frame_magic = u32::from_le_bytes(log_bytes[offset..offset + 4].try_into().unwrap());
        if frame_magic != FRAME_MAGIC {
            break; // not a valid frame
        }
        let payload_size =
            u64::from_le_bytes(log_bytes[offset + 4..offset + 12].try_into().unwrap()) as usize;
        let op_count = u32::from_le_bytes(log_bytes[offset + 12..offset + 16].try_into().unwrap());
        let commit_ts = u64::from_le_bytes(log_bytes[offset + 16..offset + 24].try_into().unwrap());

        let mut payload_offset = offset + TX_HEADER_SIZE;
        let chunk_count = if payload_size == 0 {
            0
        } else {
            payload_size.div_ceil(ENCRYPTED_PAYLOAD_CHUNK_SIZE)
        };

        let mut frame_complete = true;
        for chunk_index in 0..chunk_count {
            let chunk_plaintext_len = (payload_size - chunk_index * ENCRYPTED_PAYLOAD_CHUNK_SIZE)
                .min(ENCRYPTED_PAYLOAD_CHUNK_SIZE);
            let chunk_on_disk_size = chunk_plaintext_len + tag_size + nonce_size;
            if payload_offset + chunk_on_disk_size + TX_TRAILER_SIZE > log_bytes.len() {
                frame_complete = false;
                break;
            }

            let blob = &log_bytes[payload_offset..payload_offset + chunk_on_disk_size];
            let ciphertext = &blob[..chunk_plaintext_len + tag_size];
            let nonce = &blob[chunk_plaintext_len + tag_size..];

            let mut aad = [0u8; 32];
            aad[..8].copy_from_slice(&salt.to_le_bytes());
            if chunk_index + 1 == chunk_count {
                aad[8..16].copy_from_slice(&(payload_size as u64).to_le_bytes());
            }
            aad[16..20].copy_from_slice(&op_count.to_le_bytes());
            aad[20..28].copy_from_slice(&commit_ts.to_le_bytes());
            aad[28..32].copy_from_slice(&(chunk_index as u32).to_le_bytes());

            enc_ctx
                .decrypt_chunk(ciphertext, nonce, &aad)
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to decrypt frame {frame_count} chunk {chunk_index} at offset {offset}: {e}"
                    )
                });

            payload_offset += chunk_on_disk_size;
        }
        if !frame_complete {
            break;
        }

        frame_count += 1;
        offset = payload_offset + TX_TRAILER_SIZE; // skip trailer
    }

    assert!(
        frame_count > 0,
        "db-log should contain at least one TX frame"
    );
}

/// Encrypted version of test_recovery_checkpoint_then_more_writes.
/// Checkpoint some rows, write more without checkpointing, restart, verify all rows survive.
#[test]
fn test_encrypted_recovery_checkpoint_then_more_writes() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
    assert!(log_path.exists(), "db-log file should exist before restart");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

/// Write, restart, write more, restart again, verify all data accumulates correctly.
#[test]
fn test_encrypted_recovery_multiple_restart_cycles() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    // Cycle 1: create table + insert
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    }

    assert!(log_path.exists(), "db-log file should exist after cycle 1");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );
    db.restart();

    // Cycle 2: insert more rows
    {
        let conn = db.connect();
        conn.execute("INSERT INTO t VALUES (2, 'second')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'third')").unwrap();
    }

    db.restart();

    // Verify all rows survived two restart cycles
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1].to_string(), "first");
    assert_eq!(rows[1][1].to_string(), "second");
    assert_eq!(rows[2][1].to_string(), "third");
}

/// Corrupt ciphertext bytes in the encrypted log payload. Recovery should treat the
/// corrupted frame as a torn tail and stop cleanly without losing earlier valid frames.
#[test]
fn test_encrypted_recovery_corrupted_ciphertext() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    // Write two transactions: checkpoint the first, leave the second only in the log.
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'survives')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'corrupted')")
            .unwrap();
    }

    assert!(
        log_path.exists(),
        "db-log file should exist before corruption"
    );
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    // Corrupt the payload of the second (non-checkpointed) frame in the log file.
    // The log header is 56 bytes, then the TX header is 24 bytes. Flip a byte
    // in the encrypted payload area right after that.
    {
        let mut log_bytes = std::fs::read(&log_path).expect("log file should exist");
        assert!(
            log_bytes.len() > 56 + 24 + 1,
            "log should have data beyond header + tx header"
        );
        // Flip a byte in the encrypted payload region
        let corrupt_offset = 56 + 24 + 1;
        log_bytes[corrupt_offset] ^= 0xFF;
        std::fs::write(&log_path, &log_bytes).unwrap();
    }

    // Restart: recovery should discard the corrupted frame but the checkpointed
    // row (id=1) must survive because it's already in the DB file.
    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "survives");
}

/// Reproducer for a bug where log replay after checkpoint-restart-checkpoint-restart
/// panics with "table id that does not exist in the table_id_to_rootpage map".
///
/// The scenario from the simulator:
/// 1. Create many tables, insert data, checkpoint (tables get positive root pages)
/// 2. Restart (recovery rebuilds table_id_to_rootpage from btree schema)
/// 3. Create more tables + insert into old and new tables
/// 4. Checkpoint (all tables now have positive root pages, log is truncated)
/// 5. Insert more data into all tables (un-checkpointed, written to log with
///    table IDs assigned in this server incarnation)
/// 6. Restart → bootstrap rebuilds map from btree root pages, then log replay
///    sees row inserts for table IDs that may not match the bootstrap mapping
#[test]
fn test_recovery_many_tables_checkpoint_restart_checkpoint_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let num_initial_tables = 50;
    let num_extra_tables = 30;

    // Step 1: Create many tables, insert data, checkpoint
    {
        let conn = db.connect();
        for i in 0..num_initial_tables {
            conn.execute(format!("CREATE TABLE t{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO t{i} VALUES (1, 'init')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    // Step 2: Restart (simulates server redeploy)
    db.restart();

    // Step 3: Create more tables + insert into old tables, then checkpoint
    {
        let conn = db.connect();
        // Create new tables (these get new negative table IDs)
        for i in 0..num_extra_tables {
            conn.execute(format!(
                "CREATE TABLE extra{i}(id INTEGER PRIMARY KEY, v TEXT)"
            ))
            .unwrap();
            conn.execute(format!("INSERT INTO extra{i} VALUES (1, 'extra')"))
                .unwrap();
        }
        // Insert into the original tables
        for i in 0..num_initial_tables {
            conn.execute(format!("INSERT INTO t{i} VALUES (2, 'after_restart')"))
                .unwrap();
        }
        // Step 4: Checkpoint - all tables get positive root pages, log truncated
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Step 5: More writes after checkpoint (un-checkpointed, in the log)
        for i in 0..num_initial_tables {
            conn.execute(format!("INSERT INTO t{i} VALUES (3, 'post_ckpt2')"))
                .unwrap();
        }
        for i in 0..num_extra_tables {
            conn.execute(format!(
                "INSERT INTO extra{i} VALUES (2, 'extra_post_ckpt')"
            ))
            .unwrap();
        }
        conn.close().unwrap();
    }

    // Step 6: Restart again - log replay should not panic
    db.restart();

    // Verify data integrity
    {
        let conn = db.connect();
        for i in 0..num_initial_tables {
            let rows = get_rows(&conn, &format!("SELECT id, v FROM t{i} ORDER BY id"));
            assert_eq!(
                rows.len(),
                3,
                "table t{i} should have 3 rows, got {}",
                rows.len()
            );
        }
        for i in 0..num_extra_tables {
            let rows = get_rows(&conn, &format!("SELECT id, v FROM extra{i} ORDER BY id"));
            assert_eq!(
                rows.len(),
                2,
                "table extra{i} should have 2 rows, got {}",
                rows.len()
            );
        }
    }
}

/// Variant that does 3 restart cycles with tables created across each incarnation.
/// This stresses the table_id_to_rootpage mapping more aggressively.
#[test]
fn test_recovery_three_restarts_with_table_creation() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    // Incarnation 1: create tables, checkpoint
    {
        let conn = db.connect();
        for i in 0..20 {
            conn.execute(format!("CREATE TABLE a{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO a{i} VALUES (1, 'a')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    db.restart();

    // Incarnation 2: create more tables, insert into old, checkpoint, then more writes
    {
        let conn = db.connect();
        for i in 0..20 {
            conn.execute(format!("CREATE TABLE b{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (1, 'b')"))
                .unwrap();
        }
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (2, 'a2')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        // Un-checkpointed writes
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (3, 'a3')"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (2, 'b2')"))
                .unwrap();
        }
        conn.close().unwrap();
    }

    db.restart();

    // Incarnation 3: create even more tables, insert everywhere, checkpoint, more writes
    {
        let conn = db.connect();
        for i in 0..20 {
            conn.execute(format!("CREATE TABLE c{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO c{i} VALUES (1, 'c')"))
                .unwrap();
        }
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (4, 'a4')"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (3, 'b3')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        // Un-checkpointed writes to all tables
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (5, 'a5')"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (4, 'b4')"))
                .unwrap();
            conn.execute(format!("INSERT INTO c{i} VALUES (2, 'c2')"))
                .unwrap();
        }
        conn.close().unwrap();
    }

    // Final restart - should not panic during log replay
    db.restart();

    {
        let conn = db.connect();
        for i in 0..20 {
            let rows = get_rows(&conn, &format!("SELECT id FROM a{i} ORDER BY id"));
            assert_eq!(rows.len(), 5, "table a{i} should have 5 rows");
            let rows = get_rows(&conn, &format!("SELECT id FROM b{i} ORDER BY id"));
            assert_eq!(rows.len(), 4, "table b{i} should have 4 rows");
            let rows = get_rows(&conn, &format!("SELECT id FROM c{i} ORDER BY id"));
            assert_eq!(rows.len(), 2, "table c{i} should have 2 rows");
        }
    }
}

fn create_wide_table_like_schema(conn: &Arc<Connection>) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS core(
            id INTEGER PRIMARY KEY,
            row_number INTEGER NOT NULL,
            sheet_id INTEGER NOT NULL,
            created_by TEXT,
            updated_by TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            col_1 TEXT,
            col_2 TEXT,
            col_3 TEXT,
            col_4 TEXT,
            col_5 TEXT,
            col_6 TEXT,
            col_7 TEXT,
            col_8 TEXT
        )",
    )
    .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_sheet_row ON core(sheet_id, row_number)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_created ON core(created_at)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_updated ON core(updated_at, sheet_id)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_created_by ON core(created_by, sheet_id)")
        .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metadata(
            sheet_id INTEGER PRIMARY KEY,
            next_row_number INTEGER NOT NULL DEFAULT 1,
            row_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS audit_log(
            id INTEGER PRIMARY KEY,
            sheet_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            row_id INTEGER,
            row_number INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            details TEXT
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trigger_gate(
            id INTEGER PRIMARY KEY,
            sheet_id INTEGER NOT NULL,
            trigger_type TEXT NOT NULL,
            payload TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO metadata(sheet_id, next_row_number, row_count, updated_at)
         VALUES (1, 1, 0, datetime('now'))",
    )
    .unwrap();
}

fn drop_wide_table_like_schema(conn: &Arc<Connection>) {
    conn.execute("DROP TABLE IF EXISTS trigger_gate").unwrap();
    conn.execute("DROP TABLE IF EXISTS audit_log").unwrap();
    conn.execute("DROP TABLE IF EXISTS metadata").unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_sheet_row")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_created")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_updated")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_created_by")
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS core").unwrap();
}

fn insert_wide_table_like_batch(conn: &Arc<Connection>, start_row_number: i64, rows: usize) {
    conn.execute("BEGIN").unwrap();

    for offset in 0..rows {
        let row_number = start_row_number + offset as i64;
        conn.execute(format!(
            "INSERT INTO core(
                row_number, sheet_id, created_by, updated_by,
                created_at, updated_at,
                col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8
             ) VALUES (
                {row_number}, 1, 'seed', 'seed',
                datetime('now'), datetime('now'),
                hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)),
                hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8))
             )",
        ))
        .unwrap();

        conn.execute(format!(
            "INSERT INTO audit_log(sheet_id, action, row_number, details, created_at)
             VALUES (1, 'INSERT', {row_number}, 'wide table repro', datetime('now'))",
        ))
        .unwrap();
    }

    conn.execute(format!(
        "UPDATE metadata
         SET next_row_number = next_row_number + {rows},
             row_count = row_count + {rows},
             updated_at = datetime('now')
         WHERE sheet_id = 1",
    ))
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, payload, created_at)
         VALUES (1, 'ROW_INSERT', '{\"count\": 1}', datetime('now'))",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, payload, created_at)
         VALUES (1, 'RECALC', '{\"sheet_id\": 1}', datetime('now'))",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, payload, created_at)
         VALUES (1, 'WEBHOOK', '{\"event\": \"rows_added\"}', datetime('now'))",
    )
    .unwrap();

    conn.execute("COMMIT").unwrap();
}

/// Reproducer for an MVCC crash-restart bug in checkpointing.
///
/// Sequence:
/// 1. Create a wide-table style schema and write one row.
/// 2. Simulate an abrupt process death (no clean connection close).
/// 3. Restart, drop the old schema, recreate it, write one new row.
/// 4. Checkpoint.
///
/// Checkpoint should retire the dropped table before creating the replacement table,
/// even when sqlite_schema rowids are reused across a crash + restart cycle.
#[test]
fn test_checkpoint_recovers_after_crash_restart_drop_recreate_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        create_wide_table_like_schema(&conn);
        insert_wide_table_like_batch(&conn, 1, 1);
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    drop_wide_table_like_schema(&conn);
    create_wide_table_like_schema(&conn);
    insert_wide_table_like_batch(&conn, 1, 1);
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(
        &conn,
        "SELECT row_number, sheet_id, created_by FROM core ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);
    assert_eq!(rows[0][2].to_string(), "seed");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    conn.close().unwrap();
    db.restart();

    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT row_number, sheet_id, created_by FROM core ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);
    assert_eq!(rows[0][2].to_string(), "seed");
}

/// Reproducer for the original index-side panic:
/// "Index struct for index_id ... must exist when checkpointing index rows".
///
/// Sequence:
/// 1. Create and checkpoint a table with one row.
/// 2. Create an index on that existing table.
/// 3. Simulate an abrupt process death before the index is checkpointed.
/// 4. Restart, drop and recreate the index, insert one more row.
/// 5. Checkpoint.
///
/// Checkpoint should retire the dropped index before processing recovered index rows,
/// even when sqlite_schema reuses the same rowid for the recreated index.
#[test]
fn test_checkpoint_recovers_after_crash_restart_drop_recreate_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, payload TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'seed_1', hex(randomblob(16)))")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_t_v").unwrap();
    conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'post_2', hex(randomblob(16)))")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "post_2");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    conn.close().unwrap();
    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "post_2");
}

/// Reproducer for recovery of a dropped checkpointed index.
///
/// Sequence:
/// 1. Create and checkpoint a table plus index.
/// 2. Drop the checkpointed index.
/// 3. Simulate an abrupt process death before checkpoint.
/// 4. Restart and checkpoint.
///
/// Recovery must preserve the deleted sqlite_schema record so checkpoint can
/// retire the dropped index without losing its object identity.
#[test]
fn test_checkpoint_recovers_after_restart_drop_checkpointed_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'seed_1')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("DROP INDEX idx_t_v").unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");

    let rows = get_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = 'idx_t_v'",
    );
    assert_eq!(rows.len(), 0);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_drop_recreate_indexed_table_many_inserts_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    for round in 0..2 {
        {
            let conn = db.connect();
            let mv_store = db.get_mvcc_store();
            mv_store.set_checkpoint_threshold(4096);

            if round > 0 {
                conn.execute("DROP TABLE IF EXISTS t").unwrap();
            }

            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER)")
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();
            conn.execute("CREATE INDEX idx_c ON t(c)").unwrap();

            for i in 0..1000 {
                conn.execute(format!("INSERT INTO t VALUES({i}, 'a_{i}', 'b_{i}', {i})"))
                    .unwrap();
            }

            conn.close().unwrap();
        }

        db.restart();

        {
            let conn = db.connect();
            let rows = get_rows(&conn, "SELECT count(*) FROM t");
            assert_eq!(
                rows[0][0].as_int().unwrap(),
                1000,
                "round {round}: expected 1000 rows"
            );
            conn.close().unwrap();
        }
    }
}

/// What this test checks: CREATE TYPE (which writes to __turso_internal_types,
/// not sqlite_schema) is visible to a second connection under MVCC.
/// Why this matters: The commit phase must detect schema changes even when no
/// rows are written to sqlite_schema. Without the fix, did_commit_schema_change
/// stayed false and the second connection never reloaded the schema.
#[test]
fn test_create_type_visible_to_second_connection_under_mvcc() {
    let db =
        MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_custom_types(true));

    // conn1: define a custom type
    let conn1 = db.connect();
    conn1
        .execute("CREATE TYPE my_uint(value any) BASE text ENCODE my_uint_enc(value) DECODE my_uint_dec(value)")
        .unwrap();
    conn1.close().unwrap();

    // conn2: the type should be visible without reopening the database
    let conn2 = db.connect();
    let rows = get_rows(
        &conn2,
        "SELECT name FROM sqlite_turso_types WHERE name LIKE 'my_uint%'",
    );
    assert_eq!(rows.len(), 1, "CREATE TYPE should be visible to conn2");
    assert_eq!(rows[0][0].to_string(), "my_uint(value any)");
    conn2.close().unwrap();
}

#[test]
fn test_integrity_check_ignores_dropped_root_that_is_live_after_recovery() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    db.restart();

    let conn = db.connect();

    let rows = get_rows(
        &conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    );
    let root_page = rows[0][0].as_int().unwrap();
    assert!(root_page > 0);

    conn.with_schema_mut(|schema| {
        schema.dropped_root_pages.insert(root_page);
    });

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}
/// Snapshot stability under all of: nested-savepoint rollbacks, checkpoints,
/// CREATE/DROP INDEX, and concurrent committed writers.
///
/// One reader holds a long BEGIN CONCURRENT and repeatedly samples
/// `SELECT count(*) FROM t`; *every sample within the tx must be equal*.
/// If any pair differs, MVCC snapshot isolation is violated — the case the
/// original analysis points at (`gc_version_chain` Rule 3 reaping `V_old`
/// after a savepoint-thread's rollback restores `end=None`, while the
/// reader's snapshot still depends on it).
///
/// Disruptor threads (each toggleable via env):
///   REPRO_SP=1     — runs the nested-savepoint driver (BEGIN CONCURRENT;
///                    SAVEPOINT × depth with INSERTs and DELETEs of
///                    pre-existing rows; ROLLBACK TO sp_<rb>; RELEASE; COMMIT)
///   REPRO_CKPT=1   — cycles PRAGMA wal_checkpoint(PASSIVE/FULL/RESTART/TRUNCATE)
///   REPRO_DDL=1    — CREATE/DROP INDEX cycle
///   REPRO_WRITER=1 — committed INSERT/DELETE in BEGIN CONCURRENT/COMMIT
///   (defaults: SP, CKPT, WRITER on; DDL off because it currently hangs.)
///
/// Other knobs:
///   REPRO_DURATION_SECS=N  total wall-clock cap (default 30)
///   REPRO_READER_OPS=N     count samples per reader transaction (default 8)
#[test]
fn test_snapshot_stability_full() {
    use crate::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    // Honor RUST_LOG when set; ignored if a subscriber is already installed.
    // NOTE: deliberately NOT using `with_test_writer()` — that routes through
    // libtest's stdout capture, which serializes events behind a mutex and is
    // slow enough under heavy concurrency to suppress the very race we're
    // trying to log.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();

    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE INDEX idx_v ON t(v)").unwrap();
        // Pre-existing rows so V_old candidates exist before any tx starts.
        for i in 0..500 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v_{i}', NULL)"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mismatch = Arc::new(AtomicBool::new(false));
    let reader_iters = Arc::new(AtomicU64::new(0));
    let reader_samples = Arc::new(AtomicU64::new(0));
    let sp_iters = Arc::new(AtomicU64::new(0));
    let writer_iters = Arc::new(AtomicU64::new(0));
    let ckpt_iters = Arc::new(AtomicU64::new(0));
    let ddl_iters = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(10_000_000));

    let mismatch_first = Arc::new(AtomicI64::new(0));
    let mismatch_second = Arc::new(AtomicI64::new(0));
    let mismatch_idx_a = Arc::new(AtomicU64::new(0));
    let mismatch_idx_b = Arc::new(AtomicU64::new(0));

    let duration = Duration::from_secs(
        std::env::var("REPRO_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5),
    );
    let reader_ops: usize = std::env::var("REPRO_READER_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    let enable_sp = std::env::var("REPRO_SP").map(|s| s != "0").unwrap_or(true);
    let enable_writer = std::env::var("REPRO_WRITER")
        .map(|s| s != "0")
        .unwrap_or(true);
    let enable_ckpt = std::env::var("REPRO_CKPT")
        .map(|s| s != "0")
        .unwrap_or(true);
    let enable_ddl = std::env::var("REPRO_DDL")
        .map(|s| s != "0")
        .unwrap_or(false);

    // --- Reader: snapshot-stability assertion ---
    let reader = {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let reader_iters = reader_iters.clone();
        let reader_samples = reader_samples.clone();
        let mismatch_first = mismatch_first.clone();
        let mismatch_second = mismatch_second.clone();
        let mismatch_idx_a = mismatch_idx_a.clone();
        let mismatch_idx_b = mismatch_idx_b.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT").is_err() {
                    std::thread::yield_now();
                    continue;
                }
                let mut samples: Vec<i64> = Vec::with_capacity(reader_ops);
                for _ in 0..reader_ops {
                    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
                    let rows = stmt.run_collect_rows().unwrap();
                    let c = rows[0][0].as_int().unwrap();
                    samples.push(c);
                    reader_samples.fetch_add(1, Ordering::Relaxed);
                }
                // All samples within one snapshot must be equal.
                if let Some((i, &c)) = samples.iter().enumerate().find(|(_, &c)| c != samples[0]) {
                    mismatch_first.store(samples[0], Ordering::Relaxed);
                    mismatch_second.store(c, Ordering::Relaxed);
                    mismatch_idx_a.store(0, Ordering::Relaxed);
                    mismatch_idx_b.store(i as u64, Ordering::Relaxed);
                    mismatch.store(true, Ordering::Relaxed);
                    let _ = conn.execute("ROLLBACK");
                    return;
                }
                let _ = conn.execute("COMMIT");
                reader_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    // --- Savepoint-rollback driver: tombstones pre-existing rows inside a
    //     savepoint, then rolls back, restoring V_old.end = None. ---
    let sp_thread = enable_sp.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let sp_iters = sp_iters.clone();
        let next_id = next_id.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let mut rng = ChaCha8Rng::seed_from_u64(0xCAFEF00D);
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT").is_err() {
                    std::thread::yield_now();
                    continue;
                }
                let depth = 2 + (rng.random::<u8>() % 3) as usize;
                let mut sps = Vec::with_capacity(depth);
                let mut aborted = false;
                'sp: for i in 0..depth {
                    let name = format!("sp_{i}_{}", rng.random::<u32>() % 100_000);
                    if conn.execute(&format!("SAVEPOINT {name}")).is_err() {
                        aborted = true;
                        break 'sp;
                    }
                    sps.push(name);
                    let muts = 1 + (rng.random::<u8>() % 4) as u64;
                    for _ in 0..muts {
                        let op = rng.random::<u8>() % 3;
                        let sql = if op == 0 {
                            // Tombstone a pre-existing baseline row inside SP.
                            let target = (rng.random::<u32>() % 500) as i64;
                            format!("DELETE FROM t WHERE id = {target}")
                        } else {
                            let id = next_id.fetch_add(1, Ordering::Relaxed) as i64;
                            format!("INSERT INTO t VALUES ({id}, 'sp_{id}', NULL)")
                        };
                        match conn.execute(&sql) {
                            Ok(_) => {}
                            Err(LimboError::Constraint(_)) => {}
                            Err(LimboError::WriteWriteConflict)
                            | Err(LimboError::Busy)
                            | Err(LimboError::TxTerminated) => {
                                aborted = true;
                                break 'sp;
                            }
                            Err(e) => panic!("sp mutation failed: {e:?}"),
                        }
                    }
                }
                if aborted {
                    let _ = conn.execute("ROLLBACK");
                    continue;
                }
                let rb = (rng.random::<u8>() as usize) % depth;
                let target = sps[rb].clone();
                let _ = conn.execute(&format!("ROLLBACK TO {target}"));
                let _ = conn.execute(&format!("RELEASE {target}"));
                let _ = conn.execute("COMMIT");
                sp_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    // --- Independent committed writer: drives ckpt_max + GC. ---
    let writer_thread = enable_writer.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let writer_iters = writer_iters.clone();
        let next_id = next_id.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let mut rng = ChaCha8Rng::seed_from_u64(0xDEADBEEF);
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT").is_err() {
                    std::thread::yield_now();
                    continue;
                }
                let id = next_id.fetch_add(1, Ordering::Relaxed) as i64;
                let sql = if rng.random::<u8>() & 3 == 0 {
                    let target = (rng.random::<u32>() % 500) as i64;
                    format!("DELETE FROM t WHERE id = {target}")
                } else {
                    format!("INSERT INTO t VALUES ({id}, 'w_{id}', NULL)")
                };
                if conn.execute(&sql).is_err() {
                    let _ = conn.execute("ROLLBACK");
                    continue;
                }
                if conn.execute("COMMIT").is_ok() {
                    writer_iters.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    });

    // --- Checkpoint thread: drives drop_unused_row_versions. ---
    let ckpt_thread = enable_ckpt.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let ckpt_iters = ckpt_iters.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let modes = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"];
            let mut idx = 0usize;
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                let _ = conn.execute(&format!(
                    "PRAGMA wal_checkpoint({})",
                    modes[idx % modes.len()]
                ));
                idx = idx.wrapping_add(1);
                ckpt_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    // --- DDL thread: CREATE INDEX / DROP INDEX cycle (default OFF). ---
    let ddl_thread = enable_ddl.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let ddl_iters = ddl_iters.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                let name = format!("idx_dyn_{}", i % 4);
                let _ = conn.execute(&format!("CREATE INDEX {name} ON t(v)"));
                let _ = conn.execute(&format!("DROP INDEX {name}"));
                i = i.wrapping_add(1);
                ddl_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    let started = Instant::now();
    while started.elapsed() < duration && !mismatch.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(50));
    }
    stop.store(true, Ordering::Relaxed);

    reader.join().unwrap();
    if let Some(h) = sp_thread {
        h.join().unwrap();
    }
    if let Some(h) = writer_thread {
        h.join().unwrap();
    }
    if let Some(h) = ckpt_thread {
        h.join().unwrap();
    }
    if let Some(h) = ddl_thread {
        h.join().unwrap();
    }

    let r = reader_iters.load(Ordering::Relaxed);
    let rs = reader_samples.load(Ordering::Relaxed);
    let s = sp_iters.load(Ordering::Relaxed);
    let w = writer_iters.load(Ordering::Relaxed);
    let c = ckpt_iters.load(Ordering::Relaxed);
    let d = ddl_iters.load(Ordering::Relaxed);
    eprintln!(
        "reader_iters={r} reader_samples={rs} sp_iters={s} writer_iters={w} ckpt_iters={c} ddl_iters={d} elapsed={:?}",
        started.elapsed()
    );

    if mismatch.load(Ordering::Relaxed) {
        let a = mismatch_first.load(Ordering::Relaxed);
        let b = mismatch_second.load(Ordering::Relaxed);
        let ia = mismatch_idx_a.load(Ordering::Relaxed);
        let ib = mismatch_idx_b.load(Ordering::Relaxed);
        panic!(
            "snapshot count drifted within a single BEGIN CONCURRENT: \
             samples[{ia}]={a} samples[{ib}]={b} \
             (reader_iters={r}, sp_iters={s}, writer_iters={w}, ckpt_iters={c}, ddl_iters={d})"
        );
    }
    assert!(rs > 0, "reader made no progress");
}

#[test]
fn test_read_lock_leak_deferred_then_concurrent() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0
        .execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn0.execute("INSERT INTO t1 VALUES(1, 'v1')").unwrap();
    conn0.close().unwrap();

    let conn1 = db.connect();
    conn1.execute("BEGIN DEFERRED").unwrap();
    // BEGIN CONCURRENT after BEGIN DEFERRED should error but not leak state
    let result = conn1.execute("BEGIN CONCURRENT");
    assert!(result.is_err());

    // After the error, SELECT should work without panicking
    let rows = get_rows(&conn1, "SELECT * FROM t1");
    assert_eq!(rows.len(), 1);
}
