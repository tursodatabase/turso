//! SQLite on-disk file format.
//!
//! SQLite stores data in a single database file, which is divided into fixed-size
//! pages:
//!
//! ```text
//! +----------+----------+----------+-----------------------------+----------+
//! |          |          |          |                             |          |
//! |  Page 1  |  Page 2  |  Page 3  |           ...               |  Page N  |
//! |          |          |          |                             |          |
//! +----------+----------+----------+-----------------------------+----------+
//! ```
//!
//! The first page is special because it contains a 100 byte header at the beginning.
//!
//! Each page consists of a page header and N cells, which contain the records.
//!
//! ```text
//! +-----------------+----------------+---------------------+----------------+
//! |                 |                |                     |                |
//! |   Page header   |  Cell pointer  |     Unallocated     |  Cell content  |
//! | (8 or 12 bytes) |     array      |        space        |      area      |
//! |                 |                |                     |                |
//! +-----------------+----------------+---------------------+----------------+
//! ```
//!
//! The write-ahead log (WAL) is a separate file that contains the physical
//! log of changes to a database file. The file starts with a WAL header and
//! is followed by a sequence of WAL frames, which are database pages with
//! additional metadata.
//!
//! ```text
//! +-----------------+-----------------+-----------------+-----------------+
//! |                 |                 |                 |                 |
//! |    WAL header   |    WAL frame 1  |    WAL frame 2  |    WAL frame N  |
//! |                 |                 |                 |                 |
//! +-----------------+-----------------+-----------------+-----------------+
//! ```
//!
//! For more information, see the SQLite file format specification:
//!
//! https://www.sqlite.org/fileformat.html

use crate::error::LimboError;
use crate::fast_lock::SpinLock;
use crate::io::{Buffer, Completion, ReadCompletion, SyncCompletion, WriteCompletion};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::pager::Pager;
use crate::types::{ImmutableRecord, RawSlice, RefValue, TextRef, TextSubtype};
use crate::{File, Result};
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use tracing::trace;

use super::pager::PageRef;

/// The size of the database header in bytes.
pub const DATABASE_HEADER_SIZE: usize = 100;
// DEFAULT_CACHE_SIZE negative values mean that we store the amount of pages a XKiB of memory can hold.
// We can calculate "real" cache size by diving by page size.
const DEFAULT_CACHE_SIZE: i32 = -2000;
// Minimum number of pages that cache can hold.
pub const MIN_PAGE_CACHE_SIZE: usize = 10;

/// The database header.
/// The first 100 bytes of the database file comprise the database file header.
/// The database file header is divided into fields as shown by the table below.
/// All multibyte fields in the database file header are stored with the most significant byte first (big-endian).
#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    /// The header string: "SQLite format 3\0"
    magic: [u8; 16],

    /// The database page size in bytes. Must be a power of two between 512 and 32768 inclusive,
    /// or the value 1 representing a page size of 65536.
    pub page_size: u16,

    /// File format write version. 1 for legacy; 2 for WAL.
    write_version: u8,

    /// File format read version. 1 for legacy; 2 for WAL.
    read_version: u8,

    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    /// SQLite has the ability to set aside a small number of extra bytes at the end of every page for use by extensions.
    /// These extra bytes are used, for example, by the SQLite Encryption Extension to store a nonce and/or
    /// cryptographic checksum associated with each page.
    pub reserved_space: u8,

    /// Maximum embedded payload fraction. Must be 64.
    max_embed_frac: u8,

    /// Minimum embedded payload fraction. Must be 32.
    min_embed_frac: u8,

    /// Leaf payload fraction. Must be 32.
    min_leaf_frac: u8,

    /// File change counter, incremented when database is modified.
    change_counter: u32,

    /// Size of the database file in pages. The "in-header database size".
    pub database_size: u32,

    /// Page number of the first freelist trunk page.
    pub freelist_trunk_page: u32,

    /// Total number of freelist pages.
    pub freelist_pages: u32,

    /// The schema cookie. Incremented when the database schema changes.
    schema_cookie: u32,

    /// The schema format number. Supported formats are 1, 2, 3, and 4.
    schema_format: u32,

    /// Default page cache size.
    pub default_page_cache_size: i32,

    /// The page number of the largest root b-tree page when in auto-vacuum or
    /// incremental-vacuum modes, or zero otherwise.
    vacuum_mode_largest_root_page: u32,

    /// The database text encoding. 1=UTF-8, 2=UTF-16le, 3=UTF-16be.
    text_encoding: u32,

    /// The "user version" as read and set by the user_version pragma.
    pub user_version: u32,

    /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    incremental_vacuum_enabled: u32,

    /// The "Application ID" set by PRAGMA application_id.
    application_id: u32,

    /// Reserved for expansion. Must be zero.
    reserved_for_expansion: [u8; 20],

    /// The version-valid-for number.
    version_valid_for: u32,

    /// SQLITE_VERSION_NUMBER
    pub version_number: u32,
}

pub const WAL_HEADER_SIZE: usize = 32;
pub const WAL_FRAME_HEADER_SIZE: usize = 24;
// magic is a single number represented as WAL_MAGIC_LE but the big endian
// counterpart is just the same number with LSB set to 1.
pub const WAL_MAGIC_LE: u32 = 0x377f0682;
pub const WAL_MAGIC_BE: u32 = 0x377f0683;

/// The Write-Ahead Log (WAL) header.
/// The first 32 bytes of a WAL file comprise the WAL header.
/// The WAL header is divided into the following fields stored in big-endian order.
#[derive(Debug, Default, Clone, Copy)]
#[repr(C)] // This helps with encoding because rust does not respect the order in structs, so in
           // this case we want to keep the order
pub struct WalHeader {
    /// Magic number. 0x377f0682 or 0x377f0683
    /// If the LSB is 0, checksums are native byte order, else checksums are serialized
    pub magic: u32,

    /// WAL format version. Currently 3007000
    pub file_format: u32,

    /// Database page size in bytes. Power of two between 512 and 32768 inclusive
    pub page_size: u32,

    /// Checkpoint sequence number. Increases with each checkpoint
    pub checkpoint_seq: u32,

    /// Random value used for the first salt in checksum calculations
    pub salt_1: u32,

    /// Random value used for the second salt in checksum calculations
    pub salt_2: u32,

    /// First checksum value in the wal-header
    pub checksum_1: u32,

    /// Second checksum value in the wal-header
    pub checksum_2: u32,
}

/// Immediately following the wal-header are zero or more frames.
/// Each frame consists of a 24-byte frame-header followed by <page-size> bytes of page data.
/// The frame-header is six big-endian 32-bit unsigned integer values, as follows:
#[allow(dead_code)]
#[derive(Debug, Default, Copy, Clone)]
pub struct WalFrameHeader {
    /// Page number
    page_number: u32,

    /// For commit records, the size of the database file in pages after the commit.
    /// For all other records, zero.
    db_size: u32,

    /// Salt-1 copied from the WAL header
    salt_1: u32,

    /// Salt-2 copied from the WAL header
    salt_2: u32,

    /// Checksum-1: Cumulative checksum up through and including this page
    checksum_1: u32,

    /// Checksum-2: Second half of the cumulative checksum
    checksum_2: u32,
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self {
            magic: *b"SQLite format 3\0",
            page_size: 4096,
            write_version: 2,
            read_version: 2,
            reserved_space: 0,
            max_embed_frac: 64,
            min_embed_frac: 32,
            min_leaf_frac: 32,
            change_counter: 1,
            database_size: 1,
            freelist_trunk_page: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format: 4, // latest format, new sqlite3 databases use this format
            default_page_cache_size: 500, // pages
            vacuum_mode_largest_root_page: 0,
            text_encoding: 1, // utf-8
            user_version: 0,
            incremental_vacuum_enabled: 0,
            application_id: 0,
            reserved_for_expansion: [0; 20],
            version_valid_for: 3047000,
            version_number: 3047000,
        }
    }
}

pub fn begin_read_database_header(
    db_file: Arc<dyn DatabaseStorage>,
) -> Result<Arc<SpinLock<DatabaseHeader>>> {
    let drop_fn = Rc::new(|_buf| {});
    #[allow(clippy::arc_with_non_send_sync)]
    let buf = Arc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let result = Arc::new(SpinLock::new(DatabaseHeader::default()));
    let header = result.clone();
    let complete = Box::new(move |buf: Arc<RefCell<Buffer>>| {
        let header = header.clone();
        finish_read_database_header(buf, header).unwrap();
    });
    let c = Completion::Read(ReadCompletion::new(buf, complete));
    db_file.read_page(1, c)?;
    Ok(result)
}

fn finish_read_database_header(
    buf: Arc<RefCell<Buffer>>,
    header: Arc<SpinLock<DatabaseHeader>>,
) -> Result<()> {
    let buf = buf.borrow();
    let buf = buf.as_slice();
    let mut header = header.lock();
    header.magic.copy_from_slice(&buf[0..16]);
    header.page_size = u16::from_be_bytes([buf[16], buf[17]]);
    header.write_version = buf[18];
    header.read_version = buf[19];
    header.reserved_space = buf[20];
    header.max_embed_frac = buf[21];
    header.min_embed_frac = buf[22];
    header.min_leaf_frac = buf[23];
    header.change_counter = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);
    header.database_size = u32::from_be_bytes([buf[28], buf[29], buf[30], buf[31]]);
    header.freelist_trunk_page = u32::from_be_bytes([buf[32], buf[33], buf[34], buf[35]]);
    header.freelist_pages = u32::from_be_bytes([buf[36], buf[37], buf[38], buf[39]]);
    header.schema_cookie = u32::from_be_bytes([buf[40], buf[41], buf[42], buf[43]]);
    header.schema_format = u32::from_be_bytes([buf[44], buf[45], buf[46], buf[47]]);
    header.default_page_cache_size = i32::from_be_bytes([buf[48], buf[49], buf[50], buf[51]]);
    if header.default_page_cache_size == 0 {
        header.default_page_cache_size = DEFAULT_CACHE_SIZE;
    }
    header.vacuum_mode_largest_root_page = u32::from_be_bytes([buf[52], buf[53], buf[54], buf[55]]);
    header.text_encoding = u32::from_be_bytes([buf[56], buf[57], buf[58], buf[59]]);
    header.user_version = u32::from_be_bytes([buf[60], buf[61], buf[62], buf[63]]);
    header.incremental_vacuum_enabled = u32::from_be_bytes([buf[64], buf[65], buf[66], buf[67]]);
    header.application_id = u32::from_be_bytes([buf[68], buf[69], buf[70], buf[71]]);
    header.reserved_for_expansion.copy_from_slice(&buf[72..92]);
    header.version_valid_for = u32::from_be_bytes([buf[92], buf[93], buf[94], buf[95]]);
    header.version_number = u32::from_be_bytes([buf[96], buf[97], buf[98], buf[99]]);
    Ok(())
}

pub fn begin_write_database_header(header: &DatabaseHeader, pager: &Pager) -> Result<()> {
    let page_source = pager.db_file.clone();
    let header = Rc::new(header.clone());

    let drop_fn = Rc::new(|_buf| {});
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer_to_copy = Arc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let buffer_to_copy_in_cb = buffer_to_copy.clone();

    let read_complete = Box::new(move |buffer: Arc<RefCell<Buffer>>| {
        let buffer = buffer.borrow().clone();
        let buffer = Rc::new(RefCell::new(buffer));
        let mut buf_mut = buffer.borrow_mut();
        write_header_to_buf(buf_mut.as_mut_slice(), &header);
        let mut dest_buf = buffer_to_copy_in_cb.borrow_mut();
        dest_buf.as_mut_slice().copy_from_slice(buf_mut.as_slice());
    });

    let drop_fn = Rc::new(|_buf| {});
    #[allow(clippy::arc_with_non_send_sync)]
    let buf = Arc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let c = Completion::Read(ReadCompletion::new(buf, read_complete));
    page_source.read_page(1, c)?;
    // run get header block
    pager.io.run_once()?;

    let buffer_to_copy_in_cb = buffer_to_copy.clone();
    let write_complete = Box::new(move |bytes_written: i32| {
        let buf_len = buffer_to_copy_in_cb.borrow().len();
        if bytes_written < buf_len as i32 {
            tracing::error!("wrote({bytes_written}) less than expected({buf_len})");
        }
        // finish_read_database_header(buf, header).unwrap();
    });

    let c = Completion::Write(WriteCompletion::new(write_complete));
    page_source.write_page(1, buffer_to_copy, c)?;

    Ok(())
}

pub fn write_header_to_buf(buf: &mut [u8], header: &DatabaseHeader) {
    buf[0..16].copy_from_slice(&header.magic);
    buf[16..18].copy_from_slice(&header.page_size.to_be_bytes());
    buf[18] = header.write_version;
    buf[19] = header.read_version;
    buf[20] = header.reserved_space;
    buf[21] = header.max_embed_frac;
    buf[22] = header.min_embed_frac;
    buf[23] = header.min_leaf_frac;
    buf[24..28].copy_from_slice(&header.change_counter.to_be_bytes());
    buf[28..32].copy_from_slice(&header.database_size.to_be_bytes());
    buf[32..36].copy_from_slice(&header.freelist_trunk_page.to_be_bytes());
    buf[36..40].copy_from_slice(&header.freelist_pages.to_be_bytes());
    buf[40..44].copy_from_slice(&header.schema_cookie.to_be_bytes());
    buf[44..48].copy_from_slice(&header.schema_format.to_be_bytes());
    buf[48..52].copy_from_slice(&header.default_page_cache_size.to_be_bytes());

    buf[52..56].copy_from_slice(&header.vacuum_mode_largest_root_page.to_be_bytes());
    buf[56..60].copy_from_slice(&header.text_encoding.to_be_bytes());
    buf[60..64].copy_from_slice(&header.user_version.to_be_bytes());
    buf[64..68].copy_from_slice(&header.incremental_vacuum_enabled.to_be_bytes());

    buf[68..72].copy_from_slice(&header.application_id.to_be_bytes());
    buf[72..92].copy_from_slice(&header.reserved_for_expansion);
    buf[92..96].copy_from_slice(&header.version_valid_for.to_be_bytes());
    buf[96..100].copy_from_slice(&header.version_number.to_be_bytes());
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl TryFrom<u8> for PageType {
    type Error = LimboError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::IndexInterior),
            5 => Ok(Self::TableInterior),
            10 => Ok(Self::IndexLeaf),
            13 => Ok(Self::TableLeaf),
            _ => Err(LimboError::Corrupt(format!("Invalid page type: {}", value))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OverflowCell {
    pub index: usize,
    pub payload: Pin<Vec<u8>>,
}

#[derive(Debug)]
pub struct PageContent {
    pub offset: usize,
    pub buffer: Arc<RefCell<Buffer>>,
    pub overflow_cells: Vec<OverflowCell>,
}

impl Clone for PageContent {
    fn clone(&self) -> Self {
        #[allow(clippy::arc_with_non_send_sync)]
        Self {
            offset: self.offset,
            buffer: Arc::new(RefCell::new((*self.buffer.borrow()).clone())),
            overflow_cells: self.overflow_cells.clone(),
        }
    }
}

impl PageContent {
    pub fn new(offset: usize, buffer: Arc<RefCell<Buffer>>) -> Self {
        Self {
            offset,
            buffer,
            overflow_cells: Vec::new(),
        }
    }

    pub fn page_type(&self) -> PageType {
        self.read_u8(0).try_into().unwrap()
    }

    pub fn maybe_page_type(&self) -> Option<PageType> {
        match self.read_u8(0).try_into() {
            Ok(v) => Some(v),
            Err(_) => None, // this could be an overflow page
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_ptr(&self) -> &mut [u8] {
        unsafe {
            // unsafe trick to borrow twice
            let buf_pointer = &self.buffer.as_ptr();
            let buf = (*buf_pointer).as_mut().unwrap().as_mut_slice();
            buf
        }
    }

    pub fn read_u8(&self, pos: usize) -> u8 {
        let buf = self.as_ptr();
        buf[self.offset + pos]
    }

    pub fn read_u16(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_be_bytes([buf[self.offset + pos], buf[self.offset + pos + 1]])
    }

    pub fn read_u16_no_offset(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_be_bytes([buf[pos], buf[pos + 1]])
    }

    pub fn read_u32_no_offset(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
    }

    pub fn read_u32(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        read_u32(buf, self.offset + pos)
    }

    pub fn write_u8(&self, pos: usize, value: u8) {
        tracing::trace!("write_u8(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos] = value;
    }

    pub fn write_u16(&self, pos: usize, value: u16) {
        tracing::trace!("write_u16(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    pub fn write_u16_no_offset(&self, pos: usize, value: u16) {
        tracing::trace!("write_u16(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[pos..pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    pub fn write_u32(&self, pos: usize, value: u32) {
        tracing::trace!("write_u32(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 4].copy_from_slice(&value.to_be_bytes());
    }

    /// The second field of the b-tree page header is the offset of the first freeblock, or zero if there are no freeblocks on the page.
    /// A freeblock is a structure used to identify unallocated space within a b-tree page.
    /// Freeblocks are organized as a chain.
    ///
    /// To be clear, freeblocks do not mean the regular unallocated free space to the left of the cell content area pointer, but instead
    /// blocks of at least 4 bytes WITHIN the cell content area that are not in use due to e.g. deletions.
    pub fn first_freeblock(&self) -> u16 {
        self.read_u16(1)
    }

    /// The number of cells on the page.
    pub fn cell_count(&self) -> usize {
        self.read_u16(3) as usize
    }

    /// The size of the cell pointer array in bytes.
    /// 2 bytes per cell pointer
    pub fn cell_pointer_array_size(&self) -> usize {
        const CELL_POINTER_SIZE_BYTES: usize = 2;
        self.cell_count() * CELL_POINTER_SIZE_BYTES
    }

    /// The start of the unallocated region.
    /// Effectively: the offset after the page header + the cell pointer array.
    pub fn unallocated_region_start(&self) -> usize {
        let (cell_ptr_array_start, cell_ptr_array_size) = self.cell_pointer_array_offset_and_size();
        cell_ptr_array_start + cell_ptr_array_size
    }

    pub fn unallocated_region_size(&self) -> usize {
        self.cell_content_area() as usize - self.unallocated_region_start()
    }

    /// The start of the cell content area.
    /// SQLite strives to place cells as far toward the end of the b-tree page as it can,
    /// in order to leave space for future growth of the cell pointer array.
    /// = the cell content area pointer moves leftward as cells are added to the page
    pub fn cell_content_area(&self) -> u16 {
        self.read_u16(5)
    }

    /// The size of the page header in bytes.
    /// 8 bytes for leaf pages, 12 bytes for interior pages (due to storing rightmost child pointer)
    pub fn header_size(&self) -> usize {
        match self.page_type() {
            PageType::IndexInterior => 12,
            PageType::TableInterior => 12,
            PageType::IndexLeaf => 8,
            PageType::TableLeaf => 8,
        }
    }

    /// The total number of bytes in all fragments is stored in the fifth field of the b-tree page header.
    /// Fragments are isolated groups of 1, 2, or 3 unused bytes within the cell content area.
    pub fn num_frag_free_bytes(&self) -> u8 {
        self.read_u8(7)
    }

    pub fn rightmost_pointer(&self) -> Option<u32> {
        match self.page_type() {
            PageType::IndexInterior => Some(self.read_u32(8)),
            PageType::TableInterior => Some(self.read_u32(8)),
            PageType::IndexLeaf => None,
            PageType::TableLeaf => None,
        }
    }

    pub fn rightmost_pointer_raw(&self) -> Option<*mut u8> {
        match self.page_type() {
            PageType::IndexInterior | PageType::TableInterior => {
                Some(unsafe { self.as_ptr().as_mut_ptr().add(self.offset + 8) })
            }
            PageType::IndexLeaf => None,
            PageType::TableLeaf => None,
        }
    }

    pub fn cell_get(
        &self,
        idx: usize,
        payload_overflow_threshold_max: usize,
        payload_overflow_threshold_min: usize,
        usable_size: usize,
    ) -> Result<BTreeCell> {
        tracing::trace!("cell_get(idx={})", idx);
        let buf = self.as_ptr();

        let ncells = self.cell_count();
        // the page header is 12 bytes for interior pages, 8 bytes for leaf pages
        // this is because the 4 last bytes in the interior page's header are used for the rightmost pointer.
        let cell_pointer_array_start = self.header_size();
        assert!(idx < ncells, "cell_get: idx out of bounds");
        let cell_pointer = cell_pointer_array_start + (idx * 2);
        let cell_pointer = self.read_u16(cell_pointer) as usize;

        // SAFETY: this buffer is valid as long as the page is alive. We could store the page in the cell and do some lifetime magic
        // but that is extra memory for no reason at all. Just be careful like in the old times :).
        let static_buf: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(buf) };
        read_btree_cell(
            static_buf,
            &self.page_type(),
            cell_pointer,
            payload_overflow_threshold_max,
            payload_overflow_threshold_min,
            usable_size,
        )
    }
    /// The cell pointer array of a b-tree page immediately follows the b-tree page header.
    /// Let K be the number of cells on the btree.
    /// The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    /// The cell pointers are arranged in key order with:
    /// - left-most cell (the cell with the smallest key) first and
    /// - the right-most cell (the cell with the largest key) last.
    pub fn cell_pointer_array_offset_and_size(&self) -> (usize, usize) {
        let header_size = self.header_size();
        (self.offset + header_size, self.cell_pointer_array_size())
    }

    /// Get region of a cell's payload
    pub fn cell_get_raw_region(
        &self,
        idx: usize,
        payload_overflow_threshold_max: usize,
        payload_overflow_threshold_min: usize,
        usable_size: usize,
    ) -> (usize, usize) {
        let buf = self.as_ptr();
        let ncells = self.cell_count();
        let (cell_pointer_array_start, _) = self.cell_pointer_array_offset_and_size();
        assert!(idx < ncells, "cell_get: idx out of bounds");
        let cell_pointer = cell_pointer_array_start + (idx * 2); // pointers are 2 bytes each
        let cell_pointer = self.read_u16_no_offset(cell_pointer) as usize;
        let start = cell_pointer;
        let len = match self.page_type() {
            PageType::IndexInterior => {
                let (len_payload, n_payload) = read_varint(&buf[cell_pointer + 4..]).unwrap();
                let (overflows, to_read) = payload_overflows(
                    len_payload as usize,
                    payload_overflow_threshold_max,
                    payload_overflow_threshold_min,
                    usable_size,
                );
                if overflows {
                    4 + to_read + n_payload
                } else {
                    4 + len_payload as usize + n_payload
                }
            }
            PageType::TableInterior => {
                let (_, n_rowid) = read_varint(&buf[cell_pointer + 4..]).unwrap();
                4 + n_rowid
            }
            PageType::IndexLeaf => {
                let (len_payload, n_payload) = read_varint(&buf[cell_pointer..]).unwrap();
                let (overflows, to_read) = payload_overflows(
                    len_payload as usize,
                    payload_overflow_threshold_max,
                    payload_overflow_threshold_min,
                    usable_size,
                );
                if overflows {
                    to_read + n_payload
                } else {
                    len_payload as usize + n_payload
                }
            }
            PageType::TableLeaf => {
                let (len_payload, n_payload) = read_varint(&buf[cell_pointer..]).unwrap();
                let (_, n_rowid) = read_varint(&buf[cell_pointer + n_payload..]).unwrap();
                let (overflows, to_read) = payload_overflows(
                    len_payload as usize,
                    payload_overflow_threshold_max,
                    payload_overflow_threshold_min,
                    usable_size,
                );
                if overflows {
                    to_read + n_payload + n_rowid
                } else {
                    len_payload as usize + n_payload + n_rowid
                }
            }
        };
        (start, len)
    }

    pub fn is_leaf(&self) -> bool {
        match self.page_type() {
            PageType::IndexInterior => false,
            PageType::TableInterior => false,
            PageType::IndexLeaf => true,
            PageType::TableLeaf => true,
        }
    }

    pub fn write_database_header(&self, header: &DatabaseHeader) {
        let buf = self.as_ptr();
        write_header_to_buf(buf, header);
    }

    pub fn debug_print_freelist(&self, usable_space: u16) {
        let mut pc = self.first_freeblock() as usize;
        let mut block_num = 0;
        println!("---- Free List Blocks ----");
        println!("first freeblock pointer: {}", pc);
        println!("cell content area: {}", self.cell_content_area());
        println!("fragmented bytes: {}", self.num_frag_free_bytes());

        while pc != 0 && pc <= usable_space as usize {
            let next = self.read_u16_no_offset(pc);
            let size = self.read_u16_no_offset(pc + 2);

            println!(
                "block {}: position={}, size={}, next={}",
                block_num, pc, size, next
            );
            pc = next as usize;
            block_num += 1;
        }
        println!("--------------");
    }
}

pub fn begin_read_page(
    db_file: Arc<dyn DatabaseStorage>,
    buffer_pool: Rc<BufferPool>,
    page: PageRef,
    page_idx: usize,
) -> Result<()> {
    trace!("begin_read_btree_page(page_idx = {})", page_idx);
    let buf = buffer_pool.get();
    let drop_fn = Rc::new(move |buf| {
        let buffer_pool = buffer_pool.clone();
        buffer_pool.put(buf);
    });
    #[allow(clippy::arc_with_non_send_sync)]
    let buf = Arc::new(RefCell::new(Buffer::new(buf, drop_fn)));
    let complete = Box::new(move |buf: Arc<RefCell<Buffer>>| {
        let page = page.clone();
        if finish_read_page(page_idx, buf, page.clone()).is_err() {
            page.set_error();
        }
    });
    let c = Completion::Read(ReadCompletion::new(buf, complete));
    db_file.read_page(page_idx, c)?;
    Ok(())
}

fn finish_read_page(
    page_idx: usize,
    buffer_ref: Arc<RefCell<Buffer>>,
    page: PageRef,
) -> Result<()> {
    trace!("finish_read_btree_page(page_idx = {})", page_idx);
    let pos = if page_idx == 1 {
        DATABASE_HEADER_SIZE
    } else {
        0
    };
    let inner = PageContent::new(pos, buffer_ref.clone());
    {
        page.get().contents.replace(inner);
        page.set_uptodate();
        page.clear_locked();
        page.set_loaded();
    }
    Ok(())
}

pub fn begin_write_btree_page(
    pager: &Pager,
    page: &PageRef,
    write_counter: Rc<RefCell<usize>>,
) -> Result<()> {
    trace!("begin_write_btree_page(page={})", page.get().id);
    let page_source = &pager.db_file;
    let page_finish = page.clone();

    let page_id = page.get().id;
    trace!("begin_write_btree_page(page_id={})", page_id);
    let buffer = {
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        contents.buffer.clone()
    };

    *write_counter.borrow_mut() += 1;
    let write_complete = {
        let buf_copy = buffer.clone();
        Box::new(move |bytes_written: i32| {
            trace!("finish_write_btree_page");
            let buf_copy = buf_copy.clone();
            let buf_len = buf_copy.borrow().len();
            *write_counter.borrow_mut() -= 1;

            page_finish.clear_dirty();
            if bytes_written < buf_len as i32 {
                tracing::error!("wrote({bytes_written}) less than expected({buf_len})");
            }
        })
    };
    let c = Completion::Write(WriteCompletion::new(write_complete));
    page_source.write_page(page_id, buffer.clone(), c)?;
    Ok(())
}

pub fn begin_sync(db_file: Arc<dyn DatabaseStorage>, syncing: Rc<RefCell<bool>>) -> Result<()> {
    assert!(!*syncing.borrow());
    *syncing.borrow_mut() = true;
    let completion = Completion::Sync(SyncCompletion {
        complete: Box::new(move |_| {
            *syncing.borrow_mut() = false;
        }),
    });
    db_file.sync(completion)?;
    Ok(())
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
pub enum BTreeCell {
    TableInteriorCell(TableInteriorCell),
    TableLeafCell(TableLeafCell),
    IndexInteriorCell(IndexInteriorCell),
    IndexLeafCell(IndexLeafCell),
}

#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    pub _left_child_page: u32,
    pub _rowid: u64,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub _rowid: u64,
    /// Payload of cell, if it overflows it won't include overflowed payload.
    pub _payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    pub left_child_page: u32,
    pub payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    pub payload: &'static [u8],
    pub first_overflow_page: Option<u32>,
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
}

/// read_btree_cell contructs a BTreeCell which is basically a wrapper around pointer to the payload of a cell.
/// buffer input "page" is static because we want the cell to point to the data in the page in case it has any payload.
pub fn read_btree_cell(
    page: &'static [u8],
    page_type: &PageType,
    pos: usize,
    max_local: usize,
    min_local: usize,
    usable_size: usize,
) -> Result<BTreeCell> {
    match page_type {
        PageType::IndexInterior => {
            let mut pos = pos;
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize);
            Ok(BTreeCell::IndexInteriorCell(IndexInteriorCell {
                left_child_page,
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
        PageType::TableInterior => {
            let mut pos = pos;
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (rowid, _) = read_varint(&page[pos..])?;
            Ok(BTreeCell::TableInteriorCell(TableInteriorCell {
                _left_child_page: left_child_page,
                _rowid: rowid,
            }))
        }
        PageType::IndexLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize);
            Ok(BTreeCell::IndexLeafCell(IndexLeafCell {
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
        PageType::TableLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;
            let (rowid, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize);
            Ok(BTreeCell::TableLeafCell(TableLeafCell {
                _rowid: rowid,
                _payload: payload,
                first_overflow_page,
                payload_size,
            }))
        }
    }
}

/// read_payload takes in the unread bytearray with the payload size
/// and returns the payload on the page, and optionally the first overflow page number.
#[allow(clippy::readonly_write_lock)]
fn read_payload(unread: &'static [u8], payload_size: usize) -> (&'static [u8], Option<u32>) {
    let cell_len = unread.len();
    // We will let overflow be constructed back if needed or requested.
    if payload_size <= cell_len {
        // fit within 1 page
        (&unread[..payload_size], None)
    } else {
        // overflow
        let first_overflow_page = u32::from_be_bytes([
            unread[cell_len - 4],
            unread[cell_len - 3],
            unread[cell_len - 2],
            unread[cell_len - 1],
        ]);
        (&unread[..cell_len - 4], Some(first_overflow_page))
    }
}

pub type SerialType = u64;

pub const SERIAL_TYPE_NULL: SerialType = 0;
pub const SERIAL_TYPE_INT8: SerialType = 1;
pub const SERIAL_TYPE_BEINT16: SerialType = 2;
pub const SERIAL_TYPE_BEINT24: SerialType = 3;
pub const SERIAL_TYPE_BEINT32: SerialType = 4;
pub const SERIAL_TYPE_BEINT48: SerialType = 5;
pub const SERIAL_TYPE_BEINT64: SerialType = 6;
pub const SERIAL_TYPE_BEFLOAT64: SerialType = 7;
pub const SERIAL_TYPE_CONSTINT0: SerialType = 8;
pub const SERIAL_TYPE_CONSTINT1: SerialType = 9;

pub trait SerialTypeExt {
    fn is_null(self) -> bool;
    fn is_int8(self) -> bool;
    fn is_beint16(self) -> bool;
    fn is_beint24(self) -> bool;
    fn is_beint32(self) -> bool;
    fn is_beint48(self) -> bool;
    fn is_beint64(self) -> bool;
    fn is_befloat64(self) -> bool;
    fn is_constint0(self) -> bool;
    fn is_constint1(self) -> bool;
    fn is_blob(self) -> bool;
    fn is_string(self) -> bool;
    fn blob_size(self) -> usize;
    fn string_size(self) -> usize;
    fn is_valid(self) -> bool;
}

impl SerialTypeExt for u64 {
    fn is_null(self) -> bool {
        self == SERIAL_TYPE_NULL
    }

    fn is_int8(self) -> bool {
        self == SERIAL_TYPE_INT8
    }

    fn is_beint16(self) -> bool {
        self == SERIAL_TYPE_BEINT16
    }

    fn is_beint24(self) -> bool {
        self == SERIAL_TYPE_BEINT24
    }

    fn is_beint32(self) -> bool {
        self == SERIAL_TYPE_BEINT32
    }

    fn is_beint48(self) -> bool {
        self == SERIAL_TYPE_BEINT48
    }

    fn is_beint64(self) -> bool {
        self == SERIAL_TYPE_BEINT64
    }

    fn is_befloat64(self) -> bool {
        self == SERIAL_TYPE_BEFLOAT64
    }

    fn is_constint0(self) -> bool {
        self == SERIAL_TYPE_CONSTINT0
    }

    fn is_constint1(self) -> bool {
        self == SERIAL_TYPE_CONSTINT1
    }

    fn is_blob(self) -> bool {
        self >= 12 && self % 2 == 0
    }

    fn is_string(self) -> bool {
        self >= 13 && self % 2 == 1
    }

    fn blob_size(self) -> usize {
        debug_assert!(self.is_blob());
        ((self - 12) / 2) as usize
    }

    fn string_size(self) -> usize {
        debug_assert!(self.is_string());
        ((self - 13) / 2) as usize
    }

    fn is_valid(self) -> bool {
        self <= 9 || self.is_blob() || self.is_string()
    }
}

pub fn validate_serial_type(value: u64) -> Result<SerialType> {
    if value.is_valid() {
        Ok(value)
    } else {
        crate::bail_corrupt_error!("Invalid serial type: {}", value)
    }
}

struct SmallVec<T> {
    pub data: [std::mem::MaybeUninit<T>; 64],
    pub len: usize,
    pub extra_data: Option<Vec<T>>,
}

impl<T: Default + Copy> SmallVec<T> {
    pub fn new() -> Self {
        Self {
            data: unsafe { std::mem::MaybeUninit::uninit().assume_init() },
            len: 0,
            extra_data: None,
        }
    }

    pub fn push(&mut self, value: T) {
        if self.len < self.data.len() {
            self.data[self.len] = MaybeUninit::new(value);
            self.len += 1;
        } else {
            if self.extra_data.is_none() {
                self.extra_data = Some(Vec::new());
            }
            self.extra_data.as_mut().unwrap().push(value);
            self.len += 1;
        }
    }
}

pub fn read_record(payload: &[u8], reuse_immutable: &mut ImmutableRecord) -> Result<()> {
    // Let's clear previous use
    reuse_immutable.invalidate();
    // Copy payload to ImmutableRecord in order to make RefValue that point to this new buffer.
    // By reusing this immutable record we make it less allocation expensive.
    reuse_immutable.start_serialization(payload);

    let mut pos = 0;
    let (header_size, nr) = read_varint(payload)?;
    assert!((header_size as usize) >= nr);
    let mut header_size = (header_size as usize) - nr;
    pos += nr;

    let mut serial_types = SmallVec::new();
    while header_size > 0 {
        let (serial_type, nr) = read_varint(&reuse_immutable.get_payload()[pos..])?;
        let serial_type = validate_serial_type(serial_type)?;
        serial_types.push(serial_type);
        pos += nr;
        assert!(header_size >= nr);
        header_size -= nr;
    }

    for &serial_type in &serial_types.data[..serial_types.len.min(serial_types.data.len())] {
        let (value, n) = read_value(&reuse_immutable.get_payload()[pos..], unsafe {
            *serial_type.as_ptr()
        })?;
        pos += n;
        reuse_immutable.add_value(value);
    }
    if let Some(extra) = serial_types.extra_data.as_ref() {
        for serial_type in extra {
            let (value, n) = read_value(&reuse_immutable.get_payload()[pos..], *serial_type)?;
            pos += n;
            reuse_immutable.add_value(value);
        }
    }

    Ok(())
}

/// Reads a value that might reference the buffer it is reading from. Be sure to store RefValue with the buffer
/// always.
#[inline(always)]
pub fn read_value(buf: &[u8], serial_type: SerialType) -> Result<(RefValue, usize)> {
    if serial_type.is_null() {
        return Ok((RefValue::Null, 0));
    }

    if serial_type.is_int8() {
        if buf.is_empty() {
            crate::bail_corrupt_error!("Invalid UInt8 value");
        }
        let val = buf[0] as i8;
        return Ok((RefValue::Integer(val as i64), 1));
    }

    if serial_type.is_beint16() {
        if buf.len() < 2 {
            crate::bail_corrupt_error!("Invalid BEInt16 value");
        }
        return Ok((
            RefValue::Integer(i16::from_be_bytes([buf[0], buf[1]]) as i64),
            2,
        ));
    }

    if serial_type.is_beint24() {
        if buf.len() < 3 {
            crate::bail_corrupt_error!("Invalid BEInt24 value");
        }
        let sign_extension = if buf[0] <= 127 { 0 } else { 255 };
        return Ok((
            RefValue::Integer(i32::from_be_bytes([sign_extension, buf[0], buf[1], buf[2]]) as i64),
            3,
        ));
    }

    if serial_type.is_beint32() {
        if buf.len() < 4 {
            crate::bail_corrupt_error!("Invalid BEInt32 value");
        }
        return Ok((
            RefValue::Integer(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64),
            4,
        ));
    }

    if serial_type.is_beint48() {
        if buf.len() < 6 {
            crate::bail_corrupt_error!("Invalid BEInt48 value");
        }
        let sign_extension = if buf[0] <= 127 { 0 } else { 255 };
        return Ok((
            RefValue::Integer(i64::from_be_bytes([
                sign_extension,
                sign_extension,
                buf[0],
                buf[1],
                buf[2],
                buf[3],
                buf[4],
                buf[5],
            ])),
            6,
        ));
    }

    if serial_type.is_beint64() {
        if buf.len() < 8 {
            crate::bail_corrupt_error!("Invalid BEInt64 value");
        }
        return Ok((
            RefValue::Integer(i64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ])),
            8,
        ));
    }

    if serial_type.is_befloat64() {
        if buf.len() < 8 {
            crate::bail_corrupt_error!("Invalid BEFloat64 value");
        }
        return Ok((
            RefValue::Float(f64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ])),
            8,
        ));
    }

    if serial_type.is_constint0() {
        return Ok((RefValue::Integer(0), 0));
    }

    if serial_type.is_constint1() {
        return Ok((RefValue::Integer(1), 0));
    }

    if serial_type.is_blob() {
        let n = serial_type.blob_size();
        if buf.len() < n {
            crate::bail_corrupt_error!("Invalid Blob value");
        }
        if n == 0 {
            return Ok((RefValue::Blob(RawSlice::new(std::ptr::null(), 0)), 0));
        }
        let ptr = &buf[0] as *const u8;
        let slice = RawSlice::new(ptr, n);
        return Ok((RefValue::Blob(slice), n));
    }

    if serial_type.is_string() {
        let n = serial_type.string_size();
        if buf.len() < n {
            crate::bail_corrupt_error!(
                "Invalid String value, length {} < expected length {}",
                buf.len(),
                n
            );
        }
        let slice = if n == 0 {
            RawSlice::new(std::ptr::null(), 0)
        } else {
            let ptr = &buf[0] as *const u8;
            RawSlice::new(ptr, n)
        };
        return Ok((
            RefValue::Text(TextRef {
                value: slice,
                subtype: TextSubtype::Text,
            }),
            n,
        ));
    }

    // This should never happen if validate_serial_type is used correctly
    crate::bail_corrupt_error!("Invalid serial type: {}", serial_type)
}

#[inline(always)]
pub fn read_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut v: u64 = 0;
    for i in 0..8 {
        match buf.get(i) {
            Some(c) => {
                v = (v << 7) + (c & 0x7f) as u64;
                if (c & 0x80) == 0 {
                    return Ok((v, i + 1));
                }
            }
            None => {
                crate::bail_corrupt_error!("Invalid varint");
            }
        }
    }
    v = (v << 8) + buf[8] as u64;
    Ok((v, 9))
}

pub fn write_varint(buf: &mut [u8], value: u64) -> usize {
    if value <= 0x7f {
        buf[0] = (value & 0x7f) as u8;
        return 1;
    }

    if value <= 0x3fff {
        buf[0] = (((value >> 7) & 0x7f) | 0x80) as u8;
        buf[1] = (value & 0x7f) as u8;
        return 2;
    }

    let mut value = value;
    if (value & ((0xff000000_u64) << 32)) > 0 {
        buf[8] = value as u8;
        value >>= 8;
        for i in (0..8).rev() {
            buf[i] = ((value & 0x7f) | 0x80) as u8;
            value >>= 7;
        }
        return 9;
    }

    let mut encoded: [u8; 10] = [0; 10];
    let mut bytes = value;
    let mut n = 0;
    while bytes != 0 {
        let v = 0x80 | (bytes & 0x7f);
        encoded[n] = v as u8;
        bytes >>= 7;
        n += 1;
    }
    encoded[0] &= 0x7f;
    for i in 0..n {
        buf[i] = encoded[n - 1 - i];
    }
    n
}

pub fn write_varint_to_vec(value: u64, payload: &mut Vec<u8>) {
    let mut varint = [0u8; 9];
    let n = write_varint(&mut varint, value);
    payload.extend_from_slice(&varint[0..n]);
}

pub fn begin_read_wal_header(io: &Arc<dyn File>) -> Result<Arc<SpinLock<WalHeader>>> {
    let drop_fn = Rc::new(|_buf| {});
    #[allow(clippy::arc_with_non_send_sync)]
    let buf = Arc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let result = Arc::new(SpinLock::new(WalHeader::default()));
    let header = result.clone();
    let complete = Box::new(move |buf: Arc<RefCell<Buffer>>| {
        let header = header.clone();
        finish_read_wal_header(buf, header).unwrap();
    });
    let c = Completion::Read(ReadCompletion::new(buf, complete));
    io.pread(0, c)?;
    Ok(result)
}

fn finish_read_wal_header(
    buf: Arc<RefCell<Buffer>>,
    header: Arc<SpinLock<WalHeader>>,
) -> Result<()> {
    let buf = buf.borrow();
    let buf = buf.as_slice();
    let mut header = header.lock();
    header.magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    header.file_format = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    header.page_size = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    header.checkpoint_seq = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    header.salt_1 = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
    header.salt_2 = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
    header.checksum_1 = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);
    header.checksum_2 = u32::from_be_bytes([buf[28], buf[29], buf[30], buf[31]]);
    Ok(())
}

pub fn begin_read_wal_frame(
    io: &Arc<dyn File>,
    offset: usize,
    buffer_pool: Rc<BufferPool>,
    page: PageRef,
) -> Result<()> {
    trace!(
        "begin_read_wal_frame(offset={}, page={})",
        offset,
        page.get().id
    );
    let buf = buffer_pool.get();
    let drop_fn = Rc::new(move |buf| {
        let buffer_pool = buffer_pool.clone();
        buffer_pool.put(buf);
    });
    #[allow(clippy::arc_with_non_send_sync)]
    let buf = Arc::new(RefCell::new(Buffer::new(buf, drop_fn)));
    let frame = page.clone();
    let complete = Box::new(move |buf: Arc<RefCell<Buffer>>| {
        let frame = frame.clone();
        finish_read_page(page.get().id, buf, frame).unwrap();
    });
    let c = Completion::Read(ReadCompletion::new(buf, complete));
    io.pread(offset, c)?;
    Ok(())
}

pub fn begin_write_wal_frame(
    io: &Arc<dyn File>,
    offset: usize,
    page: &PageRef,
    db_size: u32,
    write_counter: Rc<RefCell<usize>>,
    wal_header: &WalHeader,
    checksums: (u32, u32),
) -> Result<(u32, u32)> {
    let page_finish = page.clone();
    let page_id = page.get().id;
    trace!("begin_write_wal_frame(offset={}, page={})", offset, page_id);

    let mut header = WalFrameHeader {
        page_number: page_id as u32,
        db_size,
        salt_1: wal_header.salt_1,
        salt_2: wal_header.salt_2,
        checksum_1: 0,
        checksum_2: 0,
    };
    let (buffer, checksums) = {
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        let drop_fn = Rc::new(|_buf| {});

        let mut buffer = Buffer::allocate(
            contents.buffer.borrow().len() + WAL_FRAME_HEADER_SIZE,
            drop_fn,
        );
        let buf = buffer.as_mut_slice();
        buf[0..4].copy_from_slice(&header.page_number.to_be_bytes());
        buf[4..8].copy_from_slice(&header.db_size.to_be_bytes());
        buf[8..12].copy_from_slice(&header.salt_1.to_be_bytes());
        buf[12..16].copy_from_slice(&header.salt_2.to_be_bytes());

        let contents_buf = contents.as_ptr();
        let content_len = contents_buf.len();
        buf[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + content_len]
            .copy_from_slice(contents_buf);
        if content_len < 4096 {
            buf[WAL_FRAME_HEADER_SIZE + content_len..WAL_FRAME_HEADER_SIZE + 4096].fill(0);
        }

        let expects_be = wal_header.magic & 1;
        let use_native_endian = cfg!(target_endian = "big") as u32 == expects_be;
        let header_checksum = checksum_wal(&buf[0..8], wal_header, checksums, use_native_endian); // Only 8 bytes
        let final_checksum = checksum_wal(
            &buf[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + 4096],
            wal_header,
            header_checksum,
            use_native_endian,
        );
        header.checksum_1 = final_checksum.0;
        header.checksum_2 = final_checksum.1;

        buf[16..20].copy_from_slice(&header.checksum_1.to_be_bytes());
        buf[20..24].copy_from_slice(&header.checksum_2.to_be_bytes());

        #[allow(clippy::arc_with_non_send_sync)]
        (Arc::new(RefCell::new(buffer)), final_checksum)
    };

    *write_counter.borrow_mut() += 1;
    let write_complete = {
        let buf_copy = buffer.clone();
        Box::new(move |bytes_written: i32| {
            let buf_copy = buf_copy.clone();
            let buf_len = buf_copy.borrow().len();
            *write_counter.borrow_mut() -= 1;

            page_finish.clear_dirty();
            if bytes_written < buf_len as i32 {
                tracing::error!("wrote({bytes_written}) less than expected({buf_len})");
            }
        })
    };
    let c = Completion::Write(WriteCompletion::new(write_complete));
    io.pwrite(offset, buffer.clone(), c)?;
    trace!("Frame written and synced at offset={offset}");
    Ok(checksums)
}

pub fn begin_write_wal_header(io: &Arc<dyn File>, header: &WalHeader) -> Result<()> {
    let buffer = {
        let drop_fn = Rc::new(|_buf| {});

        let mut buffer = Buffer::allocate(512, drop_fn);
        let buf = buffer.as_mut_slice();

        buf[0..4].copy_from_slice(&header.magic.to_be_bytes());
        buf[4..8].copy_from_slice(&header.file_format.to_be_bytes());
        buf[8..12].copy_from_slice(&header.page_size.to_be_bytes());
        buf[12..16].copy_from_slice(&header.checkpoint_seq.to_be_bytes());
        buf[16..20].copy_from_slice(&header.salt_1.to_be_bytes());
        buf[20..24].copy_from_slice(&header.salt_2.to_be_bytes());
        buf[24..28].copy_from_slice(&header.checksum_1.to_be_bytes());
        buf[28..32].copy_from_slice(&header.checksum_2.to_be_bytes());

        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(RefCell::new(buffer))
    };

    let write_complete = {
        Box::new(move |bytes_written: i32| {
            if bytes_written < WAL_HEADER_SIZE as i32 {
                tracing::error!(
                    "wal header wrote({bytes_written}) less than expected({WAL_HEADER_SIZE})"
                );
            }
        })
    };
    let c = Completion::Write(WriteCompletion::new(write_complete));
    io.pwrite(0, buffer.clone(), c)?;
    Ok(())
}

/// Checks if payload will overflow a cell based on the maximum allowed size.
/// It will return the min size that will be stored in that case,
/// including overflow pointer
/// see e.g. https://github.com/sqlite/sqlite/blob/9591d3fe93936533c8c3b0dc4d025ac999539e11/src/dbstat.c#L371
pub fn payload_overflows(
    payload_size: usize,
    payload_overflow_threshold_max: usize,
    payload_overflow_threshold_min: usize,
    usable_size: usize,
) -> (bool, usize) {
    if payload_size <= payload_overflow_threshold_max {
        return (false, 0);
    }

    let mut space_left = payload_overflow_threshold_min
        + (payload_size - payload_overflow_threshold_min) % (usable_size - 4);
    if space_left > payload_overflow_threshold_max {
        space_left = payload_overflow_threshold_min;
    }
    (true, space_left + 4)
}

/// The checksum is computed by interpreting the input as an even number of unsigned 32-bit integers: x(0) through x(N).
/// The 32-bit integers are big-endian if the magic number in the first 4 bytes of the WAL header is 0x377f0683
/// and the integers are little-endian if the magic number is 0x377f0682.
/// The checksum values are always stored in the frame header in a big-endian format regardless of which byte order is used to compute the checksum.
///
/// The checksum algorithm only works for content which is a multiple of 8 bytes in length.
/// In other words, if the inputs are x(0) through x(N) then N must be odd.
/// The checksum algorithm is as follows:
///
/// s0 = s1 = 0
/// for i from 0 to n-1 step 2:
///    s0 += x(i) + s1;
///    s1 += x(i+1) + s0;
/// endfor
///
/// The outputs s0 and s1 are both weighted checksums using Fibonacci weights in reverse order.
/// (The largest Fibonacci weight occurs on the first element of the sequence being summed.)
/// The s1 value spans all 32-bit integer terms of the sequence whereas s0 omits the final term.
pub fn checksum_wal(
    buf: &[u8],
    _wal_header: &WalHeader,
    input: (u32, u32),
    native_endian: bool, // Sqlite interprets big endian as "native"
) -> (u32, u32) {
    assert_eq!(buf.len() % 8, 0, "buffer must be a multiple of 8");
    let mut s0: u32 = input.0;
    let mut s1: u32 = input.1;
    let mut i = 0;
    if native_endian {
        while i < buf.len() {
            let v0 = u32::from_ne_bytes(buf[i..i + 4].try_into().unwrap());
            let v1 = u32::from_ne_bytes(buf[i + 4..i + 8].try_into().unwrap());
            s0 = s0.wrapping_add(v0.wrapping_add(s1));
            s1 = s1.wrapping_add(v1.wrapping_add(s0));
            i += 8;
        }
    } else {
        while i < buf.len() {
            let v0 = u32::from_ne_bytes(buf[i..i + 4].try_into().unwrap()).swap_bytes();
            let v1 = u32::from_ne_bytes(buf[i + 4..i + 8].try_into().unwrap()).swap_bytes();
            s0 = s0.wrapping_add(v0.wrapping_add(s1));
            s1 = s1.wrapping_add(v1.wrapping_add(s0));
            i += 8;
        }
    }
    (s0, s1)
}

impl WalHeader {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::mem::transmute::<&WalHeader, &[u8; size_of::<WalHeader>()]>(self) }
    }
}

pub fn read_u32(buf: &[u8], pos: usize) -> u32 {
    u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
}

#[cfg(test)]
mod tests {
    use crate::OwnedValue;

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(&[], SERIAL_TYPE_NULL, OwnedValue::Null)]
    #[case(&[255], SERIAL_TYPE_INT8, OwnedValue::Integer(-1))]
    #[case(&[0x12, 0x34], SERIAL_TYPE_BEINT16, OwnedValue::Integer(0x1234))]
    #[case(&[0xFE], SERIAL_TYPE_INT8, OwnedValue::Integer(-2))]
    #[case(&[0x12, 0x34, 0x56], SERIAL_TYPE_BEINT24, OwnedValue::Integer(0x123456))]
    #[case(&[0x12, 0x34, 0x56, 0x78], SERIAL_TYPE_BEINT32, OwnedValue::Integer(0x12345678))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC], SERIAL_TYPE_BEINT48, OwnedValue::Integer(0x123456789ABC))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF], SERIAL_TYPE_BEINT64, OwnedValue::Integer(0x123456789ABCDEFF))]
    #[case(&[0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18], SERIAL_TYPE_BEFLOAT64, OwnedValue::Float(std::f64::consts::PI))]
    #[case(&[1, 2], SERIAL_TYPE_CONSTINT0, OwnedValue::Integer(0))]
    #[case(&[65, 66], SERIAL_TYPE_CONSTINT1, OwnedValue::Integer(1))]
    #[case(&[1, 2, 3], 18, OwnedValue::Blob(vec![1, 2, 3].into()))]
    #[case(&[], 12, OwnedValue::Blob(vec![].into()))] // empty blob
    #[case(&[65, 66, 67], 19, OwnedValue::build_text("ABC"))]
    #[case(&[0x80], SERIAL_TYPE_INT8, OwnedValue::Integer(-128))]
    #[case(&[0x80, 0], SERIAL_TYPE_BEINT16, OwnedValue::Integer(-32768))]
    #[case(&[0x80, 0, 0], SERIAL_TYPE_BEINT24, OwnedValue::Integer(-8388608))]
    #[case(&[0x80, 0, 0, 0], SERIAL_TYPE_BEINT32, OwnedValue::Integer(-2147483648))]
    #[case(&[0x80, 0, 0, 0, 0, 0], SERIAL_TYPE_BEINT48, OwnedValue::Integer(-140737488355328))]
    #[case(&[0x80, 0, 0, 0, 0, 0, 0, 0], SERIAL_TYPE_BEINT64, OwnedValue::Integer(-9223372036854775808))]
    #[case(&[0x7f], SERIAL_TYPE_INT8, OwnedValue::Integer(127))]
    #[case(&[0x7f, 0xff], SERIAL_TYPE_BEINT16, OwnedValue::Integer(32767))]
    #[case(&[0x7f, 0xff, 0xff], SERIAL_TYPE_BEINT24, OwnedValue::Integer(8388607))]
    #[case(&[0x7f, 0xff, 0xff, 0xff], SERIAL_TYPE_BEINT32, OwnedValue::Integer(2147483647))]
    #[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff], SERIAL_TYPE_BEINT48, OwnedValue::Integer(140737488355327))]
    #[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], SERIAL_TYPE_BEINT64, OwnedValue::Integer(9223372036854775807))]
    fn test_read_value(
        #[case] buf: &[u8],
        #[case] serial_type: SerialType,
        #[case] expected: OwnedValue,
    ) {
        let result = read_value(buf, serial_type).unwrap();
        assert_eq!(result.0.to_owned(), expected);
    }

    #[test]
    fn test_serial_type_helpers() {
        assert!(SERIAL_TYPE_NULL.is_null());
        assert!(SERIAL_TYPE_INT8.is_int8());
        assert!(SERIAL_TYPE_BEINT16.is_beint16());
        assert!(SERIAL_TYPE_BEINT24.is_beint24());
        assert!(SERIAL_TYPE_BEINT32.is_beint32());
        assert!(SERIAL_TYPE_BEINT48.is_beint48());
        assert!(SERIAL_TYPE_BEINT64.is_beint64());
        assert!(SERIAL_TYPE_BEFLOAT64.is_befloat64());
        assert!(SERIAL_TYPE_CONSTINT0.is_constint0());
        assert!(SERIAL_TYPE_CONSTINT1.is_constint1());

        assert!(12u64.is_blob());
        assert!(14u64.is_blob());
        assert!(13u64.is_string());
        assert!(15u64.is_string());

        assert_eq!(12u64.blob_size(), 0);
        assert_eq!(14u64.blob_size(), 1);
        assert_eq!(16u64.blob_size(), 2);

        assert_eq!(13u64.string_size(), 0);
        assert_eq!(15u64.string_size(), 1);
        assert_eq!(17u64.string_size(), 2);
    }

    #[rstest]
    #[case(0, SERIAL_TYPE_NULL)]
    #[case(1, SERIAL_TYPE_INT8)]
    #[case(2, SERIAL_TYPE_BEINT16)]
    #[case(3, SERIAL_TYPE_BEINT24)]
    #[case(4, SERIAL_TYPE_BEINT32)]
    #[case(5, SERIAL_TYPE_BEINT48)]
    #[case(6, SERIAL_TYPE_BEINT64)]
    #[case(7, SERIAL_TYPE_BEFLOAT64)]
    #[case(8, SERIAL_TYPE_CONSTINT0)]
    #[case(9, SERIAL_TYPE_CONSTINT1)]
    #[case(12, 12)] // Blob(0)
    #[case(13, 13)] // String(0)
    #[case(14, 14)] // Blob(1)
    #[case(15, 15)] // String(1)
    fn test_validate_serial_type(#[case] input: u64, #[case] expected: SerialType) {
        let result = validate_serial_type(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_invalid_serial_type() {
        let result = validate_serial_type(10);
        assert!(result.is_err());
    }
}
