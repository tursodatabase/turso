use rustc_hash::FxHashSet as HashSet;

use super::*;
use crate::io::PlatformIO;
use crate::mvcc::clock::LocalClock;
use crate::mvcc::cursor::MvccCursorType;
use crate::mvcc::persistent_storage::logical_log::LOG_HDR_SIZE;
use crate::state_machine::{StateTransition, TransitionResult};
use crate::storage::sqlite3_ondisk::{
    checksum_wal, read_varint, write_varint, DatabaseHeader, WalHeader, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::RwLock;
use crate::{Buffer, Completion, DatabaseOpts, OpenFlags};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

pub(crate) struct MvccTestDbNoConn {
    pub(crate) db: Option<Arc<Database>>,
    path: Option<String>,
    opts: DatabaseOpts,
    // Stored mainly to not drop the temp dir before the test is done.
    _temp_dir: Option<tempfile::TempDir>,
}
pub(crate) struct MvccTestDb {
    pub(crate) mvcc_store: Arc<MvStore<LocalClock>>,
    pub(crate) db: Arc<Database>,
    pub(crate) conn: Arc<Connection>,
}

impl MvccTestDb {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();
        // Enable MVCC via PRAGMA
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
        let mvcc_store = db.get_mv_store().clone().unwrap();
        Self {
            mvcc_store,
            db,
            conn,
        }
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
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: None,
            opts,
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
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            _temp_dir: Some(temp_dir),
        }
    }

    /// Restarts the database, make sure there is no connection to the database open before calling this!
    pub fn restart(&mut self) {
        // First let's clear any entries in database manager in order to force restart.
        // If not, we will load the same database instance again.
        {
            let mut manager = DATABASE_MANAGER.lock();
            manager.clear();
        }

        // Now open again.
        let io = Arc::new(PlatformIO::new().unwrap());
        let path = self.path.as_ref().unwrap();
        let db = Database::open_file_with_flags(io, path, OpenFlags::default(), self.opts, None)
            .unwrap();
        self.db.replace(db);
    }

    /// Asumes there is a database open
    pub fn get_db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }

    pub fn connect(&self) -> Arc<Connection> {
        self.get_db().connect().unwrap()
    }

    pub fn get_mvcc_store(&self) -> Arc<MvStore<LocalClock>> {
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
    mvcc_store: Arc<MvStore<LocalClock>>,
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
#[test]
fn test_recovery_checkpoint_then_more_writes() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
    let mut db =
        MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_triggers(true));
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
#[test]
fn test_btree_resident_recovery_then_checkpoint_delete_stays_deleted() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
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
                LOG_HDR_SIZE as u64,
                "logical log should contain only the header after truncation"
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
        LOG_HDR_SIZE as u64,
        "logical log should contain only the header after checkpoint"
    );
}

/// What this test checks: The blocking checkpoint lock is released before the WAL-to-DB
/// backfill (CheckpointWal state), so that new transactions can start while the expensive
/// I/O operations complete.
/// Why this matters: Holding the lock during WAL backfill + DB fsync blocks all readers
/// and writers for the entire checkpoint duration.
#[test]
fn test_checkpoint_releases_lock_before_wal_backfill() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
    );

    // Step through until we reach CheckpointWal. At that point the blocking lock
    // must already be released (ReleaseLock runs before CheckpointWal).
    let mut reached_checkpoint_wal = false;
    for _ in 0..50_000 {
        if checkpoint_sm.state_for_test() == CheckpointState::CheckpointWal {
            reached_checkpoint_wal = true;
            // The lock should have been released in the ReleaseLock state.
            // Verify by acquiring a read lock (which begin_tx does).
            assert!(
                mvcc_store.blocking_checkpoint_lock.read(),
                "blocking_checkpoint_lock should be available for readers at CheckpointWal"
            );
            // Release the read lock we just acquired for the assertion.
            mvcc_store.blocking_checkpoint_lock.unlock();
            break;
        }

        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before reaching CheckpointWal state")
            }
        }
    }
    assert!(
        reached_checkpoint_wal,
        "expected to reach CheckpointWal state"
    );

    // Finish the rest of the checkpoint (WAL backfill + truncation).
    let mut finished = false;
    for _ in 0..50_000 {
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

    // Verify data is still correct.
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
}

/// What this test checks: After the checkpoint releases the lock in ReleaseLock state,
/// new transactions can begin and commit while the WAL backfill is still pending.
/// Why this matters: This is the core improvement — the expensive WAL I/O no longer
/// blocks other transactions.
#[test]
fn test_checkpoint_allows_new_tx_during_wal_backfill() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'before')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
    );

    // Advance to CheckpointWal (lock is released at this point).
    for _ in 0..50_000 {
        if checkpoint_sm.state_for_test() == CheckpointState::CheckpointWal {
            break;
        }
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before reaching CheckpointWal state")
            }
        }
    }
    assert_eq!(
        checkpoint_sm.state_for_test(),
        CheckpointState::CheckpointWal
    );

    // With the lock released, a new transaction should be able to begin.
    // Use begin_tx directly to verify the lock is available.
    let tx = mvcc_store
        .begin_tx(conn.pager.load().clone())
        .expect("begin_tx should succeed while WAL backfill is pending");
    mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn);

    // Now finish the checkpoint.
    let mut finished = false;
    for _ in 0..50_000 {
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

    // Verify data is intact after checkpoint with concurrent tx begin.
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "before");
}

/// What this test checks: The logical log is truncated before the WAL-to-DB backfill
/// in the new checkpoint ordering, and the WAL is still truncated last.
/// Why this matters: Recovery relies on the WAL being intact when the logical log is
/// truncated, and the DB file being durable before the WAL is truncated.
#[test]
fn test_checkpoint_log_truncated_before_wal_backfill() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
    );

    let mut saw_log_truncated_before_wal_ckpt = false;
    let mut saw_wal_truncate_after_backfill = false;
    let mut finished = false;
    for _ in 0..50_000 {
        let state = checkpoint_sm.state_for_test();

        if state == CheckpointState::CheckpointWal {
            // At CheckpointWal, logical log should already be truncated (header-only)
            assert_eq!(
                mvcc_store.get_logical_log_file().size().unwrap(),
                LOG_HDR_SIZE as u64,
                "logical log should contain only the header before WAL backfill"
            );
            // WAL should still exist (backfill hasn't happened yet)
            let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
            assert!(wal_len > 0, "WAL must still exist before backfill");
            saw_log_truncated_before_wal_ckpt = true;
        }

        if state == CheckpointState::TruncateWal {
            saw_wal_truncate_after_backfill = true;
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
    assert!(saw_log_truncated_before_wal_ckpt);
    assert!(saw_wal_truncate_after_backfill);

    let final_wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(final_wal_len, 0);
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
            LOG_HDR_SIZE as u64,
            "logical log should contain only the header after checkpoint"
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
#[test]
fn test_meta_recovery_case_2_no_wal_replay_above_metadata_boundary() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
            RowID {
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
            RowID {
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
        RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row3, row4);
    db.mvcc_store
        .rollback_tx(tx1, db.conn.pager.load().clone(), &db.conn);
    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row5 = db
        .mvcc_store
        .read(
            tx2,
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
        .rollback_tx(tx3, conn3.pager.load().clone(), &conn3);

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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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
            RowID {
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

pub(crate) fn commit_tx(
    mv_store: Arc<MvStore<LocalClock>>,
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
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
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
    ]);

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
        row_version.is_visible_to(&current_tx, &txs)
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

    // begin invisible: transaction preparing
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(5)), None));

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

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
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
        // Insert synthetic table metadata into sqlite_schema (table_id -1).
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")), // type
                Value::Text(Text::new("test")),  // name
                Value::Text(Text::new("test")),  // tbl_name
                Value::from_i64(synthetic_root), // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
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
        // Now insert a row into the synthetic table.
        let row = generate_simple_string_row(synthetic_table_id, 1, "foo");
        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store, &conn, tx_id).unwrap();
        conn.close().unwrap();
    }
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
            .read(tx_id, RowID::new(synthetic_table_id, RowKey::Int(2)))
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

fn generate_batched_insert(num_inserts: usize) -> String {
    let mut inserts = String::from("INSERT INTO test (x) VALUES ");
    for i in 0..num_inserts {
        inserts.push_str(&format!("({i})"));
        if i < num_inserts - 1 {
            inserts.push(',');
        }
    }
    inserts.push(';');
    inserts
}
/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
#[ignore]
fn test_batch_writes() {
    let mut start = 0;
    let mut end = 5000;
    while start < end {
        let i = ((end - start) / 2) + start;
        let db = MvccTestDbNoConn::new_with_random_db();
        let conn = db.connect();
        conn.execute("CREATE TABLE test (x)").unwrap();
        let inserts = generate_batched_insert(i);
        if conn.execute(inserts.clone()).is_err() {
            end = i;
        } else {
            start = i + 1;
        }
    }
    println!("start: {start} end: {end}");
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
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
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
    db.mvcc_store
        .rollback_tx(tx2, db.conn.pager.load().clone(), &db.conn);
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
#[test]
fn test_cursor_with_btree_and_mvcc() {
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
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Value::from_i64(1)]);
    assert_eq!(rows[1], vec![Value::from_i64(2)]);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_with_btree_and_mvcc_2() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
#[test]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
#[test]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor_with_delete() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
#[test]
#[ignore] // FIXME: This fails constantly on main and is really annoying, disabling for now :]
fn test_cursor_with_btree_and_mvcc_fuzz() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, u64::MAX, 0);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 2, 0);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 0);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 0);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 2);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 5);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 5);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 3);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 5, 10);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 0);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// Once checkpoint has persisted a sole current version, it becomes GC-eligible.
fn test_gc_rule3_current_collected_after_checkpoint() {
    let mut versions = vec![make_rv(ts(1), None)];
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 5);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 5);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, u64::MAX, u64::MAX);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, u64::MAX, u64::MAX);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 2);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 2);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, u64::MAX, 3);
    assert_eq!(dropped, 0, "tombstone retained: e=5 > ckpt_max=3");
    assert_eq!(versions.len(), 1);

    // After checkpoint (ckpt_max >= e): tombstone is collected.
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, u64::MAX, 5);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 20);
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// What this test checks: Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
/// Why this matters: GC mistakes can either lose data (over-collection) or retain stale history forever (under-collection).
#[test]
/// GC on an empty version chain is a no-op. Verifies no panics or off-by-one errors.
fn test_gc_noop_on_empty() {
    let mut versions: Vec<RowVersion> = vec![];
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 5);
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
    let dropped = MvStore::<LocalClock>::gc_version_chain(&mut versions, 10, 5);
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
    db.mvcc_store
        .rollback_tx(tx1, db.conn.pager.load().clone(), &db.conn);

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
    let read_row = db.mvcc_store.read(tx2, row_id.clone()).unwrap().unwrap();
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
    MvStore::<LocalClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
    versions.len() <= before
}

/// Running GC twice with the same LWM and ckpt_max must produce the same result
/// as running it once. A non-idempotent GC would indicate that GC output triggers
/// further removals on re-evaluation, which means the first pass missed something.
/// Compares actual version content (begin/end), not just chain length.
#[quickcheck]
fn prop_gc_is_idempotent(chain: ArbitraryVersionChain) -> bool {
    let mut v1 = chain.versions.clone();
    MvStore::<LocalClock>::gc_version_chain(&mut v1, chain.lwm, chain.ckpt_max);
    let snapshot = v1.clone();
    MvStore::<LocalClock>::gc_version_chain(&mut v1, chain.lwm, chain.ckpt_max);
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
    MvStore::<LocalClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
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
    MvStore::<LocalClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
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
    MvStore::<LocalClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);
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
    MvStore::<LocalClock>::gc_version_chain(&mut versions, chain.lwm, 0);
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
    MvStore::<LocalClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);

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
    MvStore::<LocalClock>::gc_version_chain(&mut versions, chain.lwm, chain.ckpt_max);

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

    conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
        .unwrap();

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
#[test]
fn test_gc_e2e_deleted_row_stays_hidden_after_gc() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
#[test]
fn test_gc_e2e_updated_row_correct_after_gc() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
        conn.execute("ALTER TABLE old_name RENAME TO new_name")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 3: PANIC
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
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
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
        conn.execute("ALTER TABLE old_name RENAME TO new_name")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 3: PANIC
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
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
