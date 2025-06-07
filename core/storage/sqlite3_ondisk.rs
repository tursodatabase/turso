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

#![allow(clippy::arc_with_non_send_sync)]

use crate::error::LimboError;
use crate::fast_lock::SpinLock;
use crate::io::{Buffer, Complete, Completion, ReadCompletion, SyncCompletion, WriteCompletion};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::pager::Pager;
use crate::types::{
    ImmutableRecord, RawSlice, RefValue, SerialType, SerialTypeKind, TextRef, TextSubtype,
};
use crate::{File, Result, WalFileShared};
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::trace;

use super::pager::PageRef;
use super::wal::LimboRwLock;

/// The size of the database header in bytes.
pub const DATABASE_HEADER_SIZE: usize = 100;
// DEFAULT_CACHE_SIZE negative values mean that we store the amount of pages a XKiB of memory can hold.
// We can calculate "real" cache size by diving by page size.
const DEFAULT_CACHE_SIZE: i32 = -2000;

// Minimum number of pages that cache can hold.
pub const MIN_PAGE_CACHE_SIZE: usize = 10;

/// The minimum page size in bytes.
const MIN_PAGE_SIZE: u32 = 512;

/// The maximum page size in bytes.
const MAX_PAGE_SIZE: u32 = 65536;

/// The default page size in bytes.
const DEFAULT_PAGE_SIZE: u16 = 4096;

pub const DATABASE_HEADER_PAGE_ID: usize = 1;

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
    page_size: u16,

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
    pub schema_cookie: u32,

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
    pub user_version: i32,

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

    /// Database page size in bytes. Power of two between 512 and 65536 inclusive
    pub page_size: u32,

    /// Checkpoint sequence number. Increases with each checkpoint
    pub checkpoint_seq: u32,

    /// Random value used for the first salt in checksum calculations
    /// TODO: Incremented with each checkpoint
    pub salt_1: u32,

    /// Random value used for the second salt in checksum calculations.
    /// TODO: A different random value for each checkpoint
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
            page_size: DEFAULT_PAGE_SIZE,
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
            default_page_cache_size: DEFAULT_CACHE_SIZE,
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

impl DatabaseHeader {
    pub fn update_page_size(&mut self, size: u32) {
        if !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size) || (size & (size - 1) != 0) {
            return;
        }

        self.page_size = if size == MAX_PAGE_SIZE {
            1u16
        } else {
            size as u16
        };
    }

    pub fn get_page_size(&self) -> u32 {
        if self.page_size == 1 {
            MAX_PAGE_SIZE
        } else {
            self.page_size as u32
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
    #[allow(clippy::arc_with_non_send_sync)]
    db_file.read_page(DATABASE_HEADER_PAGE_ID, Arc::new(c))?;
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
    header.user_version = i32::from_be_bytes([buf[60], buf[61], buf[62], buf[63]]);
    header.incremental_vacuum_enabled = u32::from_be_bytes([buf[64], buf[65], buf[66], buf[67]]);
    header.application_id = u32::from_be_bytes([buf[68], buf[69], buf[70], buf[71]]);
    header.reserved_for_expansion.copy_from_slice(&buf[72..92]);
    header.version_valid_for = u32::from_be_bytes([buf[92], buf[93], buf[94], buf[95]]);
    header.version_number = u32::from_be_bytes([buf[96], buf[97], buf[98], buf[99]]);
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

    /// Read the rowid of a table interior cell.
    #[inline(always)]
    pub fn cell_table_interior_read_rowid(&self, idx: usize) -> Result<i64> {
        debug_assert!(self.page_type() == PageType::TableInterior);
        let buf = self.as_ptr();
        const INTERIOR_PAGE_HEADER_SIZE_BYTES: usize = 12;
        let cell_pointer_array_start = INTERIOR_PAGE_HEADER_SIZE_BYTES;
        let cell_pointer = cell_pointer_array_start + (idx * 2);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        const LEFT_CHILD_PAGE_SIZE_BYTES: usize = 4;
        let (rowid, _) = read_varint(&buf[cell_pointer + LEFT_CHILD_PAGE_SIZE_BYTES..])?;
        Ok(rowid as i64)
    }

    /// Read the left child page of a table interior cell.
    #[inline(always)]
    pub fn cell_table_interior_read_left_child_page(&self, idx: usize) -> Result<u32> {
        debug_assert!(self.page_type() == PageType::TableInterior);
        let buf = self.as_ptr();
        const INTERIOR_PAGE_HEADER_SIZE_BYTES: usize = 12;
        let cell_pointer_array_start = INTERIOR_PAGE_HEADER_SIZE_BYTES;
        let cell_pointer = cell_pointer_array_start + (idx * 2);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        Ok(u32::from_be_bytes([
            buf[cell_pointer],
            buf[cell_pointer + 1],
            buf[cell_pointer + 2],
            buf[cell_pointer + 3],
        ]))
    }

    /// Read the rowid of a table leaf cell.
    #[inline(always)]
    pub fn cell_table_leaf_read_rowid(&self, idx: usize) -> Result<i64> {
        debug_assert!(self.page_type() == PageType::TableLeaf);
        let buf = self.as_ptr();
        const LEAF_PAGE_HEADER_SIZE_BYTES: usize = 8;
        let cell_pointer_array_start = LEAF_PAGE_HEADER_SIZE_BYTES;
        let cell_pointer = cell_pointer_array_start + (idx * 2);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        let mut pos = cell_pointer;
        let (_, nr) = read_varint(&buf[pos..])?;
        pos += nr;
        let (rowid, _) = read_varint(&buf[pos..])?;
        Ok(rowid as i64)
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
    db_file.read_page(page_idx, Arc::new(c))?;
    Ok(())
}

pub fn finish_read_page(
    page_idx: usize,
    buffer_ref: Arc<RefCell<Buffer>>,
    page: PageRef,
) -> Result<()> {
    trace!("finish_read_btree_page(page_idx = {})", page_idx);
    let pos = if page_idx == DATABASE_HEADER_PAGE_ID {
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
    page_source.write_page(page_id, buffer.clone(), Arc::new(c))?;
    Ok(())
}

pub fn begin_sync(db_file: Arc<dyn DatabaseStorage>, syncing: Rc<RefCell<bool>>) -> Result<()> {
    assert!(!*syncing.borrow());
    *syncing.borrow_mut() = true;
    let completion = Completion::Sync(SyncCompletion {
        complete: Box::new(move |_| {
            *syncing.borrow_mut() = false;
        }),
        is_completed: Cell::new(false),
    });
    #[allow(clippy::arc_with_non_send_sync)]
    db_file.sync(Arc::new(completion))?;
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
    pub _rowid: i64,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub _rowid: i64,
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
                _rowid: rowid as i64,
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
                _rowid: rowid as i64,
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

#[inline(always)]
pub fn validate_serial_type(value: u64) -> Result<()> {
    if !SerialType::u64_is_valid_serial_type(value) {
        crate::bail_corrupt_error!("Invalid serial type: {}", value);
    }
    Ok(())
}

pub struct SmallVec<T, const N: usize = 64> {
    /// Stack allocated data
    pub data: [std::mem::MaybeUninit<T>; N],
    /// Length of the vector, accounting for both stack and heap allocated data
    pub len: usize,
    /// Extra data on heap
    pub extra_data: Option<Vec<T>>,
}

impl<T: Default + Copy, const N: usize> SmallVec<T, N> {
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

    fn get_from_heap(&self, index: usize) -> T {
        assert!(self.extra_data.is_some());
        assert!(index >= self.data.len());
        let extra_data_index = index - self.data.len();
        let extra_data = self.extra_data.as_ref().unwrap();
        assert!(extra_data_index < extra_data.len());
        extra_data[extra_data_index]
    }

    pub fn get(&self, index: usize) -> Option<T> {
        if index >= self.len {
            return None;
        }
        let data_is_on_stack = index < self.data.len();
        if data_is_on_stack {
            // SAFETY: We know this index is initialized we checked for index < self.len earlier above.
            unsafe { Some(self.data[index].assume_init()) }
        } else {
            Some(self.get_from_heap(index))
        }
    }
}

impl<T: Default + Copy, const N: usize> SmallVec<T, N> {
    pub fn iter(&self) -> SmallVecIter<'_, T, N> {
        SmallVecIter { vec: self, pos: 0 }
    }
}

pub struct SmallVecIter<'a, T, const N: usize> {
    vec: &'a SmallVec<T, N>,
    pos: usize,
}

impl<'a, T: Default + Copy, const N: usize> Iterator for SmallVecIter<'a, T, N> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.vec.get(self.pos).map(|item| {
            self.pos += 1;
            item
        })
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

    let mut serial_types = SmallVec::<u64, 64>::new();
    while header_size > 0 {
        let (serial_type, nr) = read_varint(&reuse_immutable.get_payload()[pos..])?;
        validate_serial_type(serial_type)?;
        serial_types.push(serial_type);
        pos += nr;
        assert!(header_size >= nr);
        header_size -= nr;
    }

    for &serial_type in &serial_types.data[..serial_types.len.min(serial_types.data.len())] {
        let (value, n) = read_value(&reuse_immutable.get_payload()[pos..], unsafe {
            serial_type.assume_init().try_into()?
        })?;
        pos += n;
        reuse_immutable.add_value(value);
    }
    if let Some(extra) = serial_types.extra_data.as_ref() {
        for serial_type in extra {
            let (value, n) = read_value(
                &reuse_immutable.get_payload()[pos..],
                (*serial_type).try_into()?,
            )?;
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
    match serial_type.kind() {
        SerialTypeKind::Null => Ok((RefValue::Null, 0)),
        SerialTypeKind::I8 => {
            if buf.is_empty() {
                crate::bail_corrupt_error!("Invalid UInt8 value");
            }
            let val = buf[0] as i8;
            Ok((RefValue::Integer(val as i64), 1))
        }
        SerialTypeKind::I16 => {
            if buf.len() < 2 {
                crate::bail_corrupt_error!("Invalid BEInt16 value");
            }
            Ok((
                RefValue::Integer(i16::from_be_bytes([buf[0], buf[1]]) as i64),
                2,
            ))
        }
        SerialTypeKind::I24 => {
            if buf.len() < 3 {
                crate::bail_corrupt_error!("Invalid BEInt24 value");
            }
            let sign_extension = if buf[0] <= 127 { 0 } else { 255 };
            Ok((
                RefValue::Integer(
                    i32::from_be_bytes([sign_extension, buf[0], buf[1], buf[2]]) as i64
                ),
                3,
            ))
        }
        SerialTypeKind::I32 => {
            if buf.len() < 4 {
                crate::bail_corrupt_error!("Invalid BEInt32 value");
            }
            Ok((
                RefValue::Integer(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64),
                4,
            ))
        }
        SerialTypeKind::I48 => {
            if buf.len() < 6 {
                crate::bail_corrupt_error!("Invalid BEInt48 value");
            }
            let sign_extension = if buf[0] <= 127 { 0 } else { 255 };
            Ok((
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
            ))
        }
        SerialTypeKind::I64 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid BEInt64 value");
            }
            Ok((
                RefValue::Integer(i64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ])),
                8,
            ))
        }
        SerialTypeKind::F64 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid BEFloat64 value");
            }
            Ok((
                RefValue::Float(f64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ])),
                8,
            ))
        }
        SerialTypeKind::ConstInt0 => Ok((RefValue::Integer(0), 0)),
        SerialTypeKind::ConstInt1 => Ok((RefValue::Integer(1), 0)),
        SerialTypeKind::Blob => {
            let content_size = serial_type.size();
            if buf.len() < content_size {
                crate::bail_corrupt_error!("Invalid Blob value");
            }
            if content_size == 0 {
                Ok((RefValue::Blob(RawSlice::new(std::ptr::null(), 0)), 0))
            } else {
                let ptr = &buf[0] as *const u8;
                let slice = RawSlice::new(ptr, content_size);
                Ok((RefValue::Blob(slice), content_size))
            }
        }
        SerialTypeKind::Text => {
            let content_size = serial_type.size();
            if buf.len() < content_size {
                crate::bail_corrupt_error!(
                    "Invalid String value, length {} < expected length {}",
                    buf.len(),
                    content_size
                );
            }
            let slice = if content_size == 0 {
                RawSlice::new(std::ptr::null(), 0)
            } else {
                let ptr = &buf[0] as *const u8;
                RawSlice::new(ptr, content_size)
            };
            Ok((
                RefValue::Text(TextRef {
                    value: slice,
                    subtype: TextSubtype::Text,
                }),
                content_size,
            ))
        }
    }
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

/// We need to read the WAL file on open to reconstruct the WAL frame cache.
pub fn read_entire_wal_dumb(file: &Arc<dyn File>) -> Result<Arc<UnsafeCell<WalFileShared>>> {
    let drop_fn = Rc::new(|_buf| {});
    let size = file.size()?;
    #[allow(clippy::arc_with_non_send_sync)]
    let buf_for_pread = Arc::new(RefCell::new(Buffer::allocate(size as usize, drop_fn)));
    let header = Arc::new(SpinLock::new(WalHeader::default()));
    #[allow(clippy::arc_with_non_send_sync)]
    let wal_file_shared_ret = Arc::new(UnsafeCell::new(WalFileShared {
        wal_header: header.clone(),
        min_frame: AtomicU64::new(0),
        max_frame: AtomicU64::new(0),
        nbackfills: AtomicU64::new(0),
        frame_cache: Arc::new(SpinLock::new(HashMap::new())),
        pages_in_frames: Arc::new(SpinLock::new(Vec::new())),
        last_checksum: (0, 0),
        file: file.clone(),
        read_locks: [
            LimboRwLock::new(),
            LimboRwLock::new(),
            LimboRwLock::new(),
            LimboRwLock::new(),
            LimboRwLock::new(),
        ],
        write_lock: LimboRwLock::new(),
        loaded: AtomicBool::new(false),
    }));
    let wal_file_shared_for_completion = wal_file_shared_ret.clone();

    let complete: Box<Complete> = Box::new(move |buf: Arc<RefCell<Buffer>>| {
        let buf = buf.borrow();
        let buf_slice = buf.as_slice();
        let mut header_locked = header.lock();
        // Read header
        header_locked.magic =
            u32::from_be_bytes([buf_slice[0], buf_slice[1], buf_slice[2], buf_slice[3]]);
        header_locked.file_format =
            u32::from_be_bytes([buf_slice[4], buf_slice[5], buf_slice[6], buf_slice[7]]);
        header_locked.page_size =
            u32::from_be_bytes([buf_slice[8], buf_slice[9], buf_slice[10], buf_slice[11]]);
        header_locked.checkpoint_seq =
            u32::from_be_bytes([buf_slice[12], buf_slice[13], buf_slice[14], buf_slice[15]]);
        header_locked.salt_1 =
            u32::from_be_bytes([buf_slice[16], buf_slice[17], buf_slice[18], buf_slice[19]]);
        header_locked.salt_2 =
            u32::from_be_bytes([buf_slice[20], buf_slice[21], buf_slice[22], buf_slice[23]]);
        header_locked.checksum_1 =
            u32::from_be_bytes([buf_slice[24], buf_slice[25], buf_slice[26], buf_slice[27]]);
        header_locked.checksum_2 =
            u32::from_be_bytes([buf_slice[28], buf_slice[29], buf_slice[30], buf_slice[31]]);

        // Read frames into frame_cache and pages_in_frames
        if buf_slice.len() < WAL_HEADER_SIZE {
            panic!("WAL file too small for header");
        }

        let use_native_endian_checksum =
            cfg!(target_endian = "big") == ((header_locked.magic & 1) != 0);

        let calculated_header_checksum = checksum_wal(
            &buf_slice[0..24],
            &*header_locked,
            (0, 0),
            use_native_endian_checksum,
        );

        if calculated_header_checksum != (header_locked.checksum_1, header_locked.checksum_2) {
            panic!(
                "WAL header checksum mismatch. Expected ({}, {}), Got ({}, {})",
                header_locked.checksum_1,
                header_locked.checksum_2,
                calculated_header_checksum.0,
                calculated_header_checksum.1
            );
        }

        let mut cumulative_checksum = (header_locked.checksum_1, header_locked.checksum_2);
        let page_size_u32 = header_locked.page_size;

        if page_size_u32 < MIN_PAGE_SIZE
            || page_size_u32 > MAX_PAGE_SIZE
            || page_size_u32.count_ones() != 1
        {
            panic!("Invalid page size in WAL header: {}", page_size_u32);
        }
        let page_size = page_size_u32 as usize;

        let mut current_offset = WAL_HEADER_SIZE;
        let mut frame_idx = 1_u64;

        let wfs_data = unsafe { &mut *wal_file_shared_for_completion.get() };

        while current_offset + WAL_FRAME_HEADER_SIZE + page_size <= buf_slice.len() {
            let frame_header_slice =
                &buf_slice[current_offset..current_offset + WAL_FRAME_HEADER_SIZE];
            let page_data_slice = &buf_slice[current_offset + WAL_FRAME_HEADER_SIZE
                ..current_offset + WAL_FRAME_HEADER_SIZE + page_size];

            let frame_h_page_number =
                u32::from_be_bytes(frame_header_slice[0..4].try_into().unwrap());
            let _frame_h_db_size = u32::from_be_bytes(frame_header_slice[4..8].try_into().unwrap());
            let frame_h_salt_1 = u32::from_be_bytes(frame_header_slice[8..12].try_into().unwrap());
            let frame_h_salt_2 = u32::from_be_bytes(frame_header_slice[12..16].try_into().unwrap());
            let frame_h_checksum_1 =
                u32::from_be_bytes(frame_header_slice[16..20].try_into().unwrap());
            let frame_h_checksum_2 =
                u32::from_be_bytes(frame_header_slice[20..24].try_into().unwrap());

            // It contains more frames with mismatched SALT values, which means they're leftovers from previous checkpoints
            if frame_h_salt_1 != header_locked.salt_1 || frame_h_salt_2 != header_locked.salt_2 {
                tracing::trace!(
                    "WAL frame salt mismatch: expected ({}, {}), got ({}, {}), ignoring frame",
                    header_locked.salt_1,
                    header_locked.salt_2,
                    frame_h_salt_1,
                    frame_h_salt_2
                );
                break;
            }

            let checksum_after_fh_meta = checksum_wal(
                &frame_header_slice[0..8],
                &*header_locked,
                cumulative_checksum,
                use_native_endian_checksum,
            );
            let calculated_frame_checksum = checksum_wal(
                page_data_slice,
                &*header_locked,
                checksum_after_fh_meta,
                use_native_endian_checksum,
            );

            if calculated_frame_checksum != (frame_h_checksum_1, frame_h_checksum_2) {
                panic!(
                    "WAL frame checksum mismatch. Expected ({}, {}), Got ({}, {})",
                    frame_h_checksum_1,
                    frame_h_checksum_2,
                    calculated_frame_checksum.0,
                    calculated_frame_checksum.1
                );
            }

            cumulative_checksum = calculated_frame_checksum;

            wfs_data
                .frame_cache
                .lock()
                .entry(frame_h_page_number as u64)
                .or_default()
                .push(frame_idx);
            wfs_data
                .pages_in_frames
                .lock()
                .push(frame_h_page_number as u64);

            frame_idx += 1;
            current_offset += WAL_FRAME_HEADER_SIZE + page_size;
        }

        wfs_data
            .max_frame
            .store(frame_idx.saturating_sub(1), Ordering::SeqCst);
        wfs_data.last_checksum = cumulative_checksum;
        wfs_data.loaded.store(true, Ordering::SeqCst);
    });
    let c = Completion::Read(ReadCompletion::new(buf_for_pread, complete));
    file.pread(0, Arc::new(c))?;

    Ok(wal_file_shared_ret)
}

pub fn begin_read_wal_frame(
    io: &Arc<dyn File>,
    offset: usize,
    buffer_pool: Rc<BufferPool>,
    complete: Box<dyn Fn(Arc<RefCell<Buffer>>) -> ()>,
) -> Result<Arc<Completion>> {
    trace!("begin_read_wal_frame(offset={})", offset);
    let buf = buffer_pool.get();
    let drop_fn = Rc::new(move |buf| {
        let buffer_pool = buffer_pool.clone();
        buffer_pool.put(buf);
    });
    let buf = Arc::new(RefCell::new(Buffer::new(buf, drop_fn)));
    #[allow(clippy::arc_with_non_send_sync)]
    let c = Arc::new(Completion::Read(ReadCompletion::new(buf, complete)));
    io.pread(offset, c.clone())?;
    Ok(c)
}

pub fn begin_write_wal_frame(
    io: &Arc<dyn File>,
    offset: usize,
    page: &PageRef,
    page_size: u16,
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
        if content_len < page_size as usize {
            buf[WAL_FRAME_HEADER_SIZE + content_len..WAL_FRAME_HEADER_SIZE + page_size as usize]
                .fill(0);
        }

        let expects_be = wal_header.magic & 1;
        let use_native_endian = cfg!(target_endian = "big") as u32 == expects_be;
        let header_checksum = checksum_wal(&buf[0..8], wal_header, checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &buf[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + page_size as usize],
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
    #[allow(clippy::arc_with_non_send_sync)]
    let c = Arc::new(Completion::Write(WriteCompletion::new(write_complete)));
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
    #[allow(clippy::arc_with_non_send_sync)]
    let c = Arc::new(Completion::Write(WriteCompletion::new(write_complete)));
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
    use crate::Value;

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(&[], SerialType::null(), Value::Null)]
    #[case(&[255], SerialType::i8(), Value::Integer(-1))]
    #[case(&[0x12, 0x34], SerialType::i16(), Value::Integer(0x1234))]
    #[case(&[0xFE], SerialType::i8(), Value::Integer(-2))]
    #[case(&[0x12, 0x34, 0x56], SerialType::i24(), Value::Integer(0x123456))]
    #[case(&[0x12, 0x34, 0x56, 0x78], SerialType::i32(), Value::Integer(0x12345678))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC], SerialType::i48(), Value::Integer(0x123456789ABC))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF], SerialType::i64(), Value::Integer(0x123456789ABCDEFF))]
    #[case(&[0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18], SerialType::f64(), Value::Float(std::f64::consts::PI))]
    #[case(&[1, 2], SerialType::const_int0(), Value::Integer(0))]
    #[case(&[65, 66], SerialType::const_int1(), Value::Integer(1))]
    #[case(&[1, 2, 3], SerialType::blob(3), Value::Blob(vec![1, 2, 3].into()))]
    #[case(&[], SerialType::blob(0), Value::Blob(vec![].into()))] // empty blob
    #[case(&[65, 66, 67], SerialType::text(3), Value::build_text("ABC"))]
    #[case(&[0x80], SerialType::i8(), Value::Integer(-128))]
    #[case(&[0x80, 0], SerialType::i16(), Value::Integer(-32768))]
    #[case(&[0x80, 0, 0], SerialType::i24(), Value::Integer(-8388608))]
    #[case(&[0x80, 0, 0, 0], SerialType::i32(), Value::Integer(-2147483648))]
    #[case(&[0x80, 0, 0, 0, 0, 0], SerialType::i48(), Value::Integer(-140737488355328))]
    #[case(&[0x80, 0, 0, 0, 0, 0, 0, 0], SerialType::i64(), Value::Integer(-9223372036854775808))]
    #[case(&[0x7f], SerialType::i8(), Value::Integer(127))]
    #[case(&[0x7f, 0xff], SerialType::i16(), Value::Integer(32767))]
    #[case(&[0x7f, 0xff, 0xff], SerialType::i24(), Value::Integer(8388607))]
    #[case(&[0x7f, 0xff, 0xff, 0xff], SerialType::i32(), Value::Integer(2147483647))]
    #[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff], SerialType::i48(), Value::Integer(140737488355327))]
    #[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], SerialType::i64(), Value::Integer(9223372036854775807))]
    fn test_read_value(
        #[case] buf: &[u8],
        #[case] serial_type: SerialType,
        #[case] expected: Value,
    ) {
        let result = read_value(buf, serial_type).unwrap();
        assert_eq!(result.0.to_owned(), expected);
    }

    #[test]
    fn test_serial_type_helpers() {
        assert_eq!(
            TryInto::<SerialType>::try_into(12u64).unwrap(),
            SerialType::blob(0)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(14u64).unwrap(),
            SerialType::blob(1)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(13u64).unwrap(),
            SerialType::text(0)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(15u64).unwrap(),
            SerialType::text(1)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(16u64).unwrap(),
            SerialType::blob(2)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(17u64).unwrap(),
            SerialType::text(2)
        );
    }

    #[rstest]
    #[case(0, SerialType::null())]
    #[case(1, SerialType::i8())]
    #[case(2, SerialType::i16())]
    #[case(3, SerialType::i24())]
    #[case(4, SerialType::i32())]
    #[case(5, SerialType::i48())]
    #[case(6, SerialType::i64())]
    #[case(7, SerialType::f64())]
    #[case(8, SerialType::const_int0())]
    #[case(9, SerialType::const_int1())]
    #[case(12, SerialType::blob(0))]
    #[case(13, SerialType::text(0))]
    #[case(14, SerialType::blob(1))]
    #[case(15, SerialType::text(1))]
    fn test_parse_serial_type(#[case] input: u64, #[case] expected: SerialType) {
        let result = SerialType::try_from(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_validate_serial_type() {
        for i in 0..=9 {
            let result = validate_serial_type(i);
            assert!(result.is_ok());
        }
        for i in 10..=11 {
            let result = validate_serial_type(i);
            assert!(result.is_err());
        }
        for i in 12..=1000 {
            let result = validate_serial_type(i);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_smallvec_iter() {
        let mut small_vec = SmallVec::<i32, 4>::new();
        (0..8).for_each(|i| small_vec.push(i));

        let mut iter = small_vec.iter();
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(3));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(5));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), Some(7));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_smallvec_get() {
        let mut small_vec = SmallVec::<i32, 4>::new();
        (0..8).for_each(|i| small_vec.push(i));

        (0..8).for_each(|i| {
            assert_eq!(small_vec.get(i), Some(i as i32));
        });

        assert_eq!(small_vec.get(8), None);
    }
}
