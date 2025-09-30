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

use bytemuck::{Pod, Zeroable};
use pack1::{I32BE, U16BE, U32BE};
use tracing::{instrument, Level};

use super::pager::PageRef;
use super::wal::TursoRwLock;
use crate::error::LimboError;
use crate::fast_lock::SpinLock;
use crate::io::{Buffer, Completion, ReadComplete};
use crate::storage::btree::offset::{
    BTREE_CELL_CONTENT_AREA, BTREE_CELL_COUNT, BTREE_FIRST_FREEBLOCK, BTREE_FRAGMENTED_BYTES_COUNT,
    BTREE_PAGE_TYPE, BTREE_RIGHTMOST_PTR,
};
use crate::storage::btree::{payload_overflow_threshold_max, payload_overflow_threshold_min};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::{DatabaseStorage, EncryptionOrChecksum};
use crate::storage::pager::Pager;
use crate::storage::wal::READMARK_NOT_USED;
use crate::types::{RawSlice, RefValue, SerialType, SerialTypeKind, TextRef, TextSubtype};
use crate::{
    bail_corrupt_error, turso_assert, CompletionError, File, IOContext, Result, WalFileShared,
};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// The minimum size of a cell in bytes.
pub const MINIMUM_CELL_SIZE: usize = 4;

pub const CELL_PTR_SIZE_BYTES: usize = 2;
pub const INTERIOR_PAGE_HEADER_SIZE_BYTES: usize = 12;
pub const LEAF_PAGE_HEADER_SIZE_BYTES: usize = 8;
pub const LEFT_CHILD_PTR_SIZE_BYTES: usize = 4;

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy, Debug)]
#[repr(transparent)]
/// Read/Write file format version.
pub struct PageSize(U16BE);

impl PageSize {
    pub const MIN: u32 = 512;
    pub const MAX: u32 = 65536;
    pub const DEFAULT: u16 = 4096;

    /// Interpret a user-provided u32 as either a valid page size or None.
    pub const fn new(size: u32) -> Option<Self> {
        if size < PageSize::MIN || size > PageSize::MAX {
            return None;
        }

        // Page size must be a power of two.
        if size.count_ones() != 1 {
            return None;
        }

        if size == PageSize::MAX {
            // Internally, the value 1 represents 65536, since the on-disk value of the page size in the DB header is 2 bytes.
            return Some(Self(U16BE::new(1)));
        }

        Some(Self(U16BE::new(size as u16)))
    }

    /// Interpret a u16 on disk (DB file header) as either a valid page size or
    /// return a corrupt error.
    pub fn new_from_header_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self(U16BE::new(1))),
            n => {
                let Some(size) = Self::new(n as u32) else {
                    bail_corrupt_error!("invalid page size in database header: {n}");
                };

                Ok(size)
            }
        }
    }

    pub const fn get(self) -> u32 {
        match self.0.get() {
            1 => Self::MAX,
            v => v as u32,
        }
    }

    /// Get the raw u16 value stored internally
    pub const fn get_raw(self) -> u16 {
        self.0.get()
    }
}

impl Default for PageSize {
    fn default() -> Self {
        Self(U16BE::new(Self::DEFAULT))
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy, Debug)]
#[repr(transparent)]
/// Read/Write file format version.
pub struct CacheSize(I32BE);

impl CacheSize {
    // The negative value means that we store the amount of pages a XKiB of memory can hold.
    // We can calculate "real" cache size by diving by page size.
    pub const DEFAULT: i32 = -2000;

    // Minimum number of pages that cache can hold.
    pub const MIN: i64 = 10;

    // SQLite uses this value as threshold for maximum cache size
    pub const MAX_SAFE: i64 = 2147450880;

    pub const fn new(size: i32) -> Self {
        match size {
            Self::DEFAULT => Self(I32BE::new(0)),
            v => Self(I32BE::new(v)),
        }
    }

    pub const fn get(self) -> i32 {
        match self.0.get() {
            0 => Self::DEFAULT,
            v => v,
        }
    }
}

impl Default for CacheSize {
    fn default() -> Self {
        Self(I32BE::new(Self::DEFAULT))
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy)]
#[repr(transparent)]
/// Read/Write file format version.
pub struct Version(u8);

impl Version {
    #![allow(non_upper_case_globals)]
    const Legacy: Self = Self(1);
    const Wal: Self = Self(2);
}

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Legacy => f.write_str("Version::Legacy"),
            Self::Wal => f.write_str("Version::Wal"),
            Self(v) => write!(f, "Version::Invalid({v})"),
        }
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy)]
#[repr(transparent)]
/// Text encoding.
pub struct TextEncoding(U32BE);

impl TextEncoding {
    #![allow(non_upper_case_globals)]
    pub const Utf8: Self = Self(U32BE::new(1));
    pub const Utf16Le: Self = Self(U32BE::new(2));
    pub const Utf16Be: Self = Self(U32BE::new(3));
}

impl std::fmt::Display for TextEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Utf8 => f.write_str("UTF-8"),
            Self::Utf16Le => f.write_str("UTF-16le"),
            Self::Utf16Be => f.write_str("UTF-16be"),
            Self(v) => write!(f, "TextEncoding::Invalid({})", v.get()),
        }
    }
}

impl std::fmt::Debug for TextEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Utf8 => f.write_str("TextEncoding::Utf8"),
            Self::Utf16Le => f.write_str("TextEncoding::Utf16Le"),
            Self::Utf16Be => f.write_str("TextEncoding::Utf16Be"),
            Self(v) => write!(f, "TextEncoding::Invalid({})", v.get()),
        }
    }
}

impl Default for TextEncoding {
    fn default() -> Self {
        Self::Utf8
    }
}

#[derive(Pod, Zeroable, Clone, Copy, Debug)]
#[repr(C, packed)]
/// Database Header Format
pub struct DatabaseHeader {
    /// b"SQLite format 3\0"
    pub magic: [u8; 16],
    /// Page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    pub page_size: PageSize,
    /// File format write version. 1 for legacy; 2 for WAL.
    pub write_version: Version,
    /// File format read version. 1 for legacy; 2 for WAL.
    pub read_version: Version,
    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    pub reserved_space: u8,
    /// Maximum embedded payload fraction. Must be 64.
    pub max_embed_frac: u8,
    /// Minimum embedded payload fraction. Must be 32.
    pub min_embed_frac: u8,
    /// Leaf payload fraction. Must be 32.
    pub leaf_frac: u8,
    /// File change counter.
    pub change_counter: U32BE,
    /// Size of the database file in pages. The "in-header database size".
    pub database_size: U32BE,
    /// Page number of the first freelist trunk page.
    pub freelist_trunk_page: U32BE,
    /// Total number of freelist pages.
    pub freelist_pages: U32BE,
    /// The schema cookie.
    pub schema_cookie: U32BE,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    pub schema_format: U32BE,
    /// Default page cache size.
    pub default_page_cache_size: CacheSize,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    pub vacuum_mode_largest_root_page: U32BE,
    /// Text encoding.
    pub text_encoding: TextEncoding,
    /// The "user version" as read and set by the user_version pragma.
    pub user_version: I32BE,
    /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    pub incremental_vacuum_enabled: U32BE,
    /// The "Application ID" set by PRAGMA application_id.
    pub application_id: I32BE,
    /// Reserved for expansion. Must be zero.
    _padding: [u8; 20],
    /// The version-valid-for number.
    pub version_valid_for: U32BE,
    /// SQLITE_VERSION_NUMBER
    pub version_number: U32BE,
}

impl DatabaseHeader {
    pub const PAGE_ID: usize = 1;
    pub const SIZE: usize = size_of::<Self>();

    const _CHECK: () = {
        assert!(Self::SIZE == 100);
    };

    pub fn usable_space(self) -> usize {
        (self.page_size.get() as usize) - (self.reserved_space as usize)
    }
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self {
            magic: *b"SQLite format 3\0",
            page_size: Default::default(),
            write_version: Version::Wal,
            read_version: Version::Wal,
            reserved_space: 0,
            max_embed_frac: 64,
            min_embed_frac: 32,
            leaf_frac: 32,
            change_counter: U32BE::new(1),
            database_size: U32BE::new(0),
            freelist_trunk_page: U32BE::new(0),
            freelist_pages: U32BE::new(0),
            schema_cookie: U32BE::new(0),
            schema_format: U32BE::new(4), // latest format, new sqlite3 databases use this format
            default_page_cache_size: Default::default(),
            vacuum_mode_largest_root_page: U32BE::new(0),
            text_encoding: TextEncoding::Utf8,
            user_version: I32BE::new(0),
            incremental_vacuum_enabled: U32BE::new(0),
            application_id: I32BE::new(0),
            _padding: [0; 20],
            version_valid_for: U32BE::new(3047000),
            version_number: U32BE::new(3047000),
        }
    }
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
    pub(crate) page_number: u32,

    /// For commit records, the size of the database file in pages after the commit.
    /// For all other records, zero.
    pub(crate) db_size: u32,

    /// Salt-1 copied from the WAL header
    pub(crate) salt_1: u32,

    /// Salt-2 copied from the WAL header
    pub(crate) salt_2: u32,

    /// Checksum-1: Cumulative checksum up through and including this page
    pub(crate) checksum_1: u32,

    /// Checksum-2: Second half of the cumulative checksum
    pub(crate) checksum_2: u32,
}

impl WalFrameHeader {
    pub fn is_commit_frame(&self) -> bool {
        self.db_size > 0
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl PageType {
    pub fn is_table(&self) -> bool {
        match self {
            PageType::IndexInterior | PageType::IndexLeaf => false,
            PageType::TableInterior | PageType::TableLeaf => true,
        }
    }
}

impl TryFrom<u8> for PageType {
    type Error = LimboError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::IndexInterior),
            5 => Ok(Self::TableInterior),
            10 => Ok(Self::IndexLeaf),
            13 => Ok(Self::TableLeaf),
            _ => Err(LimboError::Corrupt(format!("Invalid page type: {value}"))),
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
    /// the position where page content starts. it's 100 for page 1(database file header is 100 bytes),
    /// 0 for all other pages.
    pub offset: usize,
    pub buffer: Arc<Buffer>,
    pub overflow_cells: Vec<OverflowCell>,
}

impl PageContent {
    pub fn new(offset: usize, buffer: Arc<Buffer>) -> Self {
        Self {
            offset,
            buffer,
            overflow_cells: Vec::new(),
        }
    }

    pub fn page_type(&self) -> PageType {
        self.read_u8(BTREE_PAGE_TYPE).try_into().unwrap()
    }

    pub fn maybe_page_type(&self) -> Option<PageType> {
        self.read_u8(0).try_into().ok() // this could be an overflow page
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_ptr(&self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }

    /// Read a u8 from the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn read_u8(&self, pos: usize) -> u8 {
        let buf = self.as_ptr();
        buf[self.offset + pos]
    }

    /// Read a u16 from the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn read_u16(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_be_bytes([buf[self.offset + pos], buf[self.offset + pos + 1]])
    }

    /// Read a u32 from the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn read_u32(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        read_u32(buf, self.offset + pos)
    }

    /// Write a u8 to the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn write_u8(&self, pos: usize, value: u8) {
        tracing::trace!("write_u8(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos] = value;
    }

    /// Write a u16 to the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn write_u16(&self, pos: usize, value: u16) {
        tracing::trace!("write_u16(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    /// Write a u32 to the page content at the given offset, taking account the possible db header on page 1 (self.offset).
    /// Do not make this method public.
    fn write_u32(&self, pos: usize, value: u32) {
        tracing::trace!("write_u32(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 4].copy_from_slice(&value.to_be_bytes());
    }

    /// Read a u16 from the page content at the given absolute offset, i.e. NOT taking account the possible db header on page 1 (self.offset).
    /// This is useful when you want to read a location that you read from another location on the page, or when writing a field of an overflow
    /// or freelist page, for example.
    pub fn read_u16_no_offset(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_be_bytes([buf[pos], buf[pos + 1]])
    }

    /// Read a u32 from the page content at the given absolute offset, i.e. NOT taking account the possible db header on page 1 (self.offset).
    /// This is useful when you want to read a location that you read from another location on the page, or when writing a field of an overflow
    /// or freelist page, for example.
    pub fn read_u32_no_offset(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
    }

    /// Write a u16 to the page content at the given absolute offset, i.e. NOT taking account the possible db header on page 1 (self.offset).
    /// This is useful when you want to write a location that you read from another location on the page, or when writing a field of an overflow
    /// or freelist page, for example.
    pub fn write_u16_no_offset(&self, pos: usize, value: u16) {
        tracing::trace!("write_u16(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[pos..pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    /// Write a u32 to the page content at the given absolute offset, i.e. NOT taking account the possible db header on page 1 (self.offset).
    /// This is useful when you want to write a location that you read from another location on the page, or when writing a field of an overflow
    /// or freelist page, for example.
    pub fn write_u32_no_offset(&self, pos: usize, value: u32) {
        tracing::trace!("write_u32(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[pos..pos + 4].copy_from_slice(&value.to_be_bytes());
    }

    /// Assign a new page type to this page.
    pub fn write_page_type(&self, value: u8) {
        self.write_u8(BTREE_PAGE_TYPE, value);
    }

    /// Assign a new rightmost pointer to this page.
    pub fn write_rightmost_ptr(&self, value: u32) {
        self.write_u32(BTREE_RIGHTMOST_PTR, value);
    }

    /// Write the location (byte offset) of the first freeblock on this page, or zero if there are no freeblocks on the page.
    pub fn write_first_freeblock(&self, value: u16) {
        self.write_u16(BTREE_FIRST_FREEBLOCK, value);
    }

    /// Write a freeblock to the page content at the given absolute offset.
    /// Parameters:
    /// - offset: the absolute offset of the freeblock
    /// - size: the size of the freeblock
    /// - next_block: the absolute offset of the next freeblock, or None if this is the last freeblock
    pub fn write_freeblock(&self, offset: u16, size: u16, next_block: Option<u16>) {
        self.write_freeblock_next_ptr(offset, next_block.unwrap_or(0));
        self.write_freeblock_size(offset, size);
    }

    /// Write the new size of a freeblock.
    /// Parameters:
    /// - offset: the absolute offset of the freeblock
    /// - size: the new size of the freeblock
    pub fn write_freeblock_size(&self, offset: u16, size: u16) {
        self.write_u16_no_offset(offset as usize + 2, size);
    }

    /// Write the absolute offset of the next freeblock.
    /// Parameters:
    /// - offset: the absolute offset of the current freeblock
    /// - next_block: the absolute offset of the next freeblock
    pub fn write_freeblock_next_ptr(&self, offset: u16, next_block: u16) {
        self.write_u16_no_offset(offset as usize, next_block);
    }

    /// Read a freeblock from the page content at the given absolute offset.
    /// Returns (absolute offset of next freeblock, size of the current freeblock)
    pub fn read_freeblock(&self, offset: u16) -> (u16, u16) {
        (
            self.read_u16_no_offset(offset as usize),
            self.read_u16_no_offset(offset as usize + 2),
        )
    }

    /// Write the number of cells on this page.
    pub fn write_cell_count(&self, value: u16) {
        self.write_u16(BTREE_CELL_COUNT, value);
    }

    /// Write the beginning of the cell content area on this page.
    pub fn write_cell_content_area(&self, value: usize) {
        debug_assert!(value <= PageSize::MAX as usize);
        let value = value as u16; // deliberate cast to u16 because 0 is interpreted as 65536
        self.write_u16(BTREE_CELL_CONTENT_AREA, value);
    }

    /// Write the number of fragmented bytes on this page.
    pub fn write_fragmented_bytes_count(&self, value: u8) {
        self.write_u8(BTREE_FRAGMENTED_BYTES_COUNT, value);
    }

    /// The offset of the first freeblock, or zero if there are no freeblocks on the page.
    pub fn first_freeblock(&self) -> u16 {
        self.read_u16(BTREE_FIRST_FREEBLOCK)
    }

    /// The number of cells on the page.
    pub fn cell_count(&self) -> usize {
        self.read_u16(BTREE_CELL_COUNT) as usize
    }

    /// The size of the cell pointer array in bytes.
    /// 2 bytes per cell pointer
    pub fn cell_pointer_array_size(&self) -> usize {
        self.cell_count() * CELL_PTR_SIZE_BYTES
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
    pub fn cell_content_area(&self) -> u32 {
        let offset = self.read_u16(BTREE_CELL_CONTENT_AREA);
        if offset == 0 {
            PageSize::MAX
        } else {
            offset as u32
        }
    }

    /// The size of the page header in bytes.
    /// 8 bytes for leaf pages, 12 bytes for interior pages (due to storing rightmost child pointer)
    pub fn header_size(&self) -> usize {
        let is_interior = self.read_u8(BTREE_PAGE_TYPE) <= PageType::TableInterior as u8;
        (!is_interior as usize) * LEAF_PAGE_HEADER_SIZE_BYTES
            + (is_interior as usize) * INTERIOR_PAGE_HEADER_SIZE_BYTES
    }

    /// The total number of bytes in all fragments
    pub fn num_frag_free_bytes(&self) -> u8 {
        self.read_u8(BTREE_FRAGMENTED_BYTES_COUNT)
    }

    pub fn rightmost_pointer(&self) -> Option<u32> {
        match self.page_type() {
            PageType::IndexInterior => Some(self.read_u32(BTREE_RIGHTMOST_PTR)),
            PageType::TableInterior => Some(self.read_u32(BTREE_RIGHTMOST_PTR)),
            PageType::IndexLeaf => None,
            PageType::TableLeaf => None,
        }
    }

    pub fn rightmost_pointer_raw(&self) -> Option<*mut u8> {
        match self.page_type() {
            PageType::IndexInterior | PageType::TableInterior => Some(unsafe {
                self.as_ptr()
                    .as_mut_ptr()
                    .add(self.offset + BTREE_RIGHTMOST_PTR)
            }),
            PageType::IndexLeaf => None,
            PageType::TableLeaf => None,
        }
    }

    pub fn cell_get(&self, idx: usize, usable_size: usize) -> Result<BTreeCell> {
        tracing::trace!("cell_get(idx={})", idx);
        let buf = self.as_ptr();

        let ncells = self.cell_count();
        assert!(
            idx < ncells,
            "cell_get: idx out of bounds: idx={idx}, ncells={ncells}"
        );
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_pointer = self.read_u16(cell_pointer) as usize;

        // SAFETY: this buffer is valid as long as the page is alive. We could store the page in the cell and do some lifetime magic
        // but that is extra memory for no reason at all. Just be careful like in the old times :).
        let static_buf: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(buf) };
        read_btree_cell(static_buf, self, cell_pointer, usable_size)
    }

    /// Read the rowid of a table interior cell.
    #[inline(always)]
    pub fn cell_table_interior_read_rowid(&self, idx: usize) -> Result<i64> {
        debug_assert!(self.page_type() == PageType::TableInterior);
        let buf = self.as_ptr();
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        const LEFT_CHILD_PAGE_SIZE_BYTES: usize = 4;
        let (rowid, _) = read_varint(&buf[cell_pointer + LEFT_CHILD_PAGE_SIZE_BYTES..])?;
        Ok(rowid as i64)
    }

    /// Read the left child page of a table interior cell or an index interior cell.
    #[inline(always)]
    pub fn cell_interior_read_left_child_page(&self, idx: usize) -> u32 {
        debug_assert!(
            self.page_type() == PageType::TableInterior
                || self.page_type() == PageType::IndexInterior
        );
        let buf = self.as_ptr();
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        u32::from_be_bytes([
            buf[cell_pointer],
            buf[cell_pointer + 1],
            buf[cell_pointer + 2],
            buf[cell_pointer + 3],
        ])
    }

    /// Read the rowid of a table leaf cell.
    #[inline(always)]
    pub fn cell_table_leaf_read_rowid(&self, idx: usize) -> Result<i64> {
        debug_assert!(self.page_type() == PageType::TableLeaf);
        let buf = self.as_ptr();
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
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
        (
            self.cell_pointer_array_offset(),
            self.cell_pointer_array_size(),
        )
    }

    pub fn cell_pointer_array_offset(&self) -> usize {
        self.offset + self.header_size()
    }

    /// Get the start offset of a cell's payload, not taking into account the 100-byte offset that is present on page 1.
    pub fn cell_get_raw_start_offset(&self, idx: usize) -> usize {
        let cell_pointer_array_start = self.cell_pointer_array_offset();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        self.read_u16_no_offset(cell_pointer) as usize
    }

    /// Get region(start end length) of a cell's payload
    /// FIXME: make all usages of [cell_get_raw_region] to use the _faster version in cases where the method is called
    /// repeatedly, since page_type, max_local, min_local are the same for all cells on the page. Also consider whether
    /// max_local and min_local should be static properties of the page.
    pub fn cell_get_raw_region(&self, idx: usize, usable_size: usize) -> (usize, usize) {
        let page_type = self.page_type();
        let max_local = payload_overflow_threshold_max(page_type, usable_size);
        let min_local = payload_overflow_threshold_min(page_type, usable_size);
        let cell_count = self.cell_count();
        self._cell_get_raw_region_faster(
            idx,
            usable_size,
            cell_count,
            max_local,
            min_local,
            page_type,
        )
    }

    /// Get region(start end length) of a cell's payload
    pub fn _cell_get_raw_region_faster(
        &self,
        idx: usize,
        usable_size: usize,
        cell_count: usize,
        max_local: usize,
        min_local: usize,
        page_type: PageType,
    ) -> (usize, usize) {
        let buf = self.as_ptr();
        assert!(idx < cell_count, "cell_get: idx out of bounds");
        let start = self.cell_get_raw_start_offset(idx);
        let len = match page_type {
            PageType::IndexInterior => {
                let (len_payload, n_payload) = read_varint(&buf[start + 4..]).unwrap();
                let (overflows, to_read) =
                    payload_overflows(len_payload as usize, max_local, min_local, usable_size);
                if overflows {
                    4 + to_read + n_payload
                } else {
                    4 + len_payload as usize + n_payload
                }
            }
            PageType::TableInterior => {
                let (_, n_rowid) = read_varint(&buf[start + 4..]).unwrap();
                4 + n_rowid
            }
            PageType::IndexLeaf => {
                let (len_payload, n_payload) = read_varint(&buf[start..]).unwrap();
                let (overflows, to_read) =
                    payload_overflows(len_payload as usize, max_local, min_local, usable_size);
                if overflows {
                    to_read + n_payload
                } else {
                    let mut size = len_payload as usize + n_payload;
                    if size < MINIMUM_CELL_SIZE {
                        size = MINIMUM_CELL_SIZE;
                    }
                    size
                }
            }
            PageType::TableLeaf => {
                let (len_payload, n_payload) = read_varint(&buf[start..]).unwrap();
                let (_, n_rowid) = read_varint(&buf[start + n_payload..]).unwrap();
                let (overflows, to_read) =
                    payload_overflows(len_payload as usize, max_local, min_local, usable_size);
                if overflows {
                    to_read + n_payload + n_rowid
                } else {
                    let mut size = len_payload as usize + n_payload + n_rowid;
                    if size < MINIMUM_CELL_SIZE {
                        size = MINIMUM_CELL_SIZE;
                    }
                    size
                }
            }
        };
        (start, len)
    }

    pub fn is_leaf(&self) -> bool {
        self.read_u8(BTREE_PAGE_TYPE) > PageType::TableInterior as u8
    }

    pub fn write_database_header(&self, header: &DatabaseHeader) {
        let buf = self.as_ptr();
        buf[0..DatabaseHeader::SIZE].copy_from_slice(bytemuck::bytes_of(header));
    }

    pub fn debug_print_freelist(&self, usable_space: usize) {
        let mut pc = self.first_freeblock() as usize;
        let mut block_num = 0;
        println!("---- Free List Blocks ----");
        println!("first freeblock pointer: {pc}");
        println!("cell content area: {}", self.cell_content_area());
        println!("fragmented bytes: {}", self.num_frag_free_bytes());

        while pc != 0 && pc <= usable_space {
            let next = self.read_u16_no_offset(pc);
            let size = self.read_u16_no_offset(pc + 2);

            println!("block {block_num}: position={pc}, size={size}, next={next}");
            pc = next as usize;
            block_num += 1;
        }
        println!("--------------");
    }
}

/// Send read request for DB page read to the IO
/// if allow_empty_read is set, than empty read will be raise error for the page, but will not panic
#[instrument(skip_all, level = Level::DEBUG)]
pub fn begin_read_page(
    db_file: Arc<dyn DatabaseStorage>,
    buffer_pool: Arc<BufferPool>,
    page: PageRef,
    page_idx: usize,
    allow_empty_read: bool,
    io_ctx: &IOContext,
) -> Result<Completion> {
    tracing::trace!("begin_read_btree_page(page_idx = {})", page_idx);
    let buf = buffer_pool.get_page();
    #[allow(clippy::arc_with_non_send_sync)]
    let buf = Arc::new(buf);
    let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
        let Ok((mut buf, bytes_read)) = res else {
            page.clear_locked();
            return;
        };
        let buf_len = buf.len();
        turso_assert!(
            (allow_empty_read && bytes_read == 0) || bytes_read == buf_len as i32,
            "read({bytes_read}) != expected({buf_len})"
        );
        let page = page.clone();
        if bytes_read == 0 {
            buf = Arc::new(Buffer::new_temporary(0));
        }
        finish_read_page(page_idx, buf, page.clone());
    });
    let c = Completion::new_read(buf, complete);
    db_file.read_page(page_idx, io_ctx, c)
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn finish_read_page(page_idx: usize, buffer_ref: Arc<Buffer>, page: PageRef) {
    tracing::trace!("finish_read_page(page_idx = {page_idx})");
    let pos = if page_idx == DatabaseHeader::PAGE_ID {
        DatabaseHeader::SIZE
    } else {
        0
    };
    let inner = PageContent::new(pos, buffer_ref.clone());
    {
        page.get().contents.replace(inner);
        page.clear_locked();
        page.set_loaded();
        // we set the wal tag only when reading page from log, or in allocate_page,
        // we clear it here for safety in case page is being re-loaded.
        page.clear_wal_tag();
    }
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn begin_write_btree_page(pager: &Pager, page: &PageRef) -> Result<Completion> {
    tracing::trace!("begin_write_btree_page(page={})", page.get().id);
    let page_source = &pager.db_file;
    let page_finish = page.clone();

    let page_id = page.get().id;
    tracing::trace!("begin_write_btree_page(page_id={})", page_id);
    let buffer = {
        let contents = page.get_contents();
        contents.buffer.clone()
    };

    let write_complete = {
        let buf_copy = buffer.clone();
        Box::new(move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                return;
            };
            tracing::trace!("finish_write_btree_page");
            let buf_copy = buf_copy.clone();
            let buf_len = buf_copy.len();

            page_finish.clear_dirty();
            turso_assert!(
                bytes_written == buf_len as i32,
                "wrote({bytes_written}) != expected({buf_len})"
            );
        })
    };
    let c = Completion::new_write(write_complete);
    let io_ctx = pager.io_ctx.read();
    page_source.write_page(page_id, buffer.clone(), &io_ctx, c)
}

#[instrument(skip_all, level = Level::DEBUG)]
/// Write a batch of pages to the database file.
///
/// we have a batch of pages to write, lets say the following:
/// (they are already sorted by id thanks to BTreeMap)
/// [1,2,3,6,7,9,10,11,12]
//
/// we want to collect this into runs of:
/// [1,2,3], [6,7], [9,10,11,12]
/// and submit each run as a `writev` call,
/// for 3 total syscalls instead of 9.
pub fn write_pages_vectored(
    pager: &Pager,
    batch: BTreeMap<usize, Arc<Buffer>>,
    done_flag: Arc<AtomicBool>,
) -> Result<Vec<Completion>> {
    if batch.is_empty() {
        done_flag.store(true, Ordering::SeqCst);
        return Ok(Vec::new());
    }

    let page_sz = pager.get_page_size_unchecked().get() as usize;
    // Count expected number of runs to create the atomic counter we need to track each batch
    let mut run_count = 0;
    let mut prev_id = None;
    for &id in batch.keys() {
        if let Some(prev) = prev_id {
            if id != prev + 1 {
                run_count += 1;
            }
        } else {
            run_count = 1; // First run
        }
        prev_id = Some(id);
    }

    // Create the atomic counters
    let runs_left = Arc::new(AtomicUsize::new(run_count));

    const EST_BUFF_CAPACITY: usize = 32;
    let mut run_bufs = Vec::with_capacity(EST_BUFF_CAPACITY);
    let mut run_start_id: Option<usize> = None;
    let mut completions = Vec::new();

    let mut iter = batch.iter().peekable();
    while let Some((id, buffer)) = iter.next() {
        if run_start_id.is_none() {
            run_start_id = Some(*id);
        }
        run_bufs.push(buffer.clone());

        if iter.peek().is_none_or(|(next_id, _)| **next_id != id + 1) {
            let start_id = run_start_id.expect("should have a start id");
            let runs_left_cl = runs_left.clone();
            let done_cl = done_flag.clone();
            let total_sz = (page_sz * run_bufs.len()) as i32;

            let cmp = Completion::new_write_linked(move |res| {
                let Ok(res) = res else { return };
                turso_assert!(total_sz == res, "failed to write expected size");
                if runs_left_cl.fetch_sub(1, Ordering::AcqRel) == 1 {
                    done_cl.store(true, Ordering::Release);
                }
            });

            let io_ctx = pager.io_ctx.read();
            match pager.db_file.write_pages(
                start_id,
                page_sz,
                std::mem::replace(&mut run_bufs, Vec::with_capacity(EST_BUFF_CAPACITY)),
                &io_ctx,
                cmp,
            ) {
                Ok(c) => completions.push(c),
                Err(e) => {
                    if runs_left.fetch_sub(1, Ordering::AcqRel) == 1 {
                        done_flag.store(true, Ordering::Release);
                    }
                    pager.io.cancel(&completions)?;
                    pager.io.drain()?;
                    return Err(e);
                }
            }
            run_start_id = None;
        }
    }

    tracing::debug!("write_pages_vectored: total runs={run_count}");
    Ok(completions)
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn begin_sync(
    db_file: Arc<dyn DatabaseStorage>,
    syncing: Arc<AtomicBool>,
) -> Result<Completion> {
    assert!(!syncing.load(Ordering::SeqCst));
    syncing.store(true, Ordering::SeqCst);
    let completion = Completion::new_sync(move |_| {
        syncing.store(false, Ordering::SeqCst);
    });
    #[allow(clippy::arc_with_non_send_sync)]
    db_file.sync(completion)
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
    pub left_child_page: u32,
    pub rowid: i64,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub rowid: i64,
    /// Payload of cell, if it overflows it won't include overflowed payload.
    pub payload: &'static [u8],
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
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

/// read_btree_cell contructs a BTreeCell which is basically a wrapper around pointer to the payload of a cell.
/// buffer input "page" is static because we want the cell to point to the data in the page in case it has any payload.
pub fn read_btree_cell(
    page: &'static [u8],
    page_content: &PageContent,
    pos: usize,
    usable_size: usize,
) -> Result<BTreeCell> {
    let page_type = page_content.page_type();
    let max_local = payload_overflow_threshold_max(page_type, usable_size);
    let min_local = payload_overflow_threshold_min(page_type, usable_size);
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
                left_child_page,
                rowid: rowid as i64,
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
                rowid: rowid as i64,
                payload,
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
#[allow(dead_code)]
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

impl<T: Default + Copy, const N: usize> Iterator for SmallVecIter<'_, T, N> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.vec.get(self.pos)?;
        self.pos += 1;
        Some(next)
    }
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

            Ok((
                RefValue::Text(TextRef::create_from(
                    &buf[..content_size],
                    TextSubtype::Text,
                )),
                content_size,
            ))
        }
    }
}

#[inline(always)]
pub fn read_integer(buf: &[u8], serial_type: u8) -> Result<i64> {
    match serial_type {
        1 => {
            if buf.is_empty() {
                crate::bail_corrupt_error!("Invalid 1-byte int");
            }
            Ok(buf[0] as i8 as i64)
        }
        2 => {
            if buf.len() < 2 {
                crate::bail_corrupt_error!("Invalid 2-byte int");
            }
            Ok(i16::from_be_bytes([buf[0], buf[1]]) as i64)
        }
        3 => {
            if buf.len() < 3 {
                crate::bail_corrupt_error!("Invalid 3-byte int");
            }
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            Ok(i32::from_be_bytes([sign_extension, buf[0], buf[1], buf[2]]) as i64)
        }
        4 => {
            if buf.len() < 4 {
                crate::bail_corrupt_error!("Invalid 4-byte int");
            }
            Ok(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64)
        }
        5 => {
            if buf.len() < 6 {
                crate::bail_corrupt_error!("Invalid 6-byte int");
            }
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            Ok(i64::from_be_bytes([
                sign_extension,
                sign_extension,
                buf[0],
                buf[1],
                buf[2],
                buf[3],
                buf[4],
                buf[5],
            ]))
        }
        6 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid 8-byte int");
            }
            Ok(i64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]))
        }
        8 => Ok(0),
        9 => Ok(1),
        _ => crate::bail_corrupt_error!("Invalid serial type for integer"),
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
    match buf.get(8) {
        Some(&c) => {
            // Values requiring 9 bytes must have non-zero in the top 8 bits (value >= 1<<56).
            // Since the final value is `(v<<8) + c`, the top 8 bits (v >> 48) must not be 0.
            // If those are zero, this should be treated as corrupt.
            // Perf? the comparison + branching happens only in parsing 9-byte varint which is rare.
            if (v >> 48) == 0 {
                bail_corrupt_error!("Invalid varint");
            }
            v = (v << 8) + c as u64;
            Ok((v, 9))
        }
        None => {
            bail_corrupt_error!("Invalid varint");
        }
    }
}

pub fn varint_len(value: u64) -> usize {
    if value <= 0x7f {
        return 1;
    }
    if value <= 0x3fff {
        return 2;
    }
    if (value & ((0xff000000_u64) << 32)) > 0 {
        return 9;
    }

    let mut bytes = value;
    let mut n = 0;
    while bytes != 0 {
        bytes >>= 7;
        n += 1;
    }
    n
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

/// Stream through frames in chunks, building frame_cache incrementally
/// Track last valid commit frame for consistency
pub fn build_shared_wal(
    file: &Arc<dyn File>,
    io: &Arc<dyn crate::IO>,
) -> Result<Arc<RwLock<WalFileShared>>> {
    let size = file.size()?;

    let header = Arc::new(SpinLock::new(WalHeader::default()));
    let read_locks = std::array::from_fn(|_| TursoRwLock::new());
    for (i, l) in read_locks.iter().enumerate() {
        l.write();
        l.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
        l.unlock();
    }

    let wal_file_shared = Arc::new(RwLock::new(WalFileShared {
        enabled: AtomicBool::new(true),
        wal_header: header.clone(),
        min_frame: AtomicU64::new(0),
        max_frame: AtomicU64::new(0),
        nbackfills: AtomicU64::new(0),
        frame_cache: Arc::new(SpinLock::new(HashMap::new())),
        last_checksum: (0, 0),
        file: Some(file.clone()),
        read_locks,
        write_lock: TursoRwLock::new(),
        loaded: AtomicBool::new(false),
        checkpoint_lock: TursoRwLock::new(),
        initialized: AtomicBool::new(false),
        epoch: AtomicU32::new(0),
    }));

    if size < WAL_HEADER_SIZE as u64 {
        wal_file_shared.write().loaded.store(true, Ordering::SeqCst);
        return Ok(wal_file_shared);
    }

    let reader = Arc::new(StreamingWalReader::new(
        file.clone(),
        wal_file_shared.clone(),
        header.clone(),
        size,
    ));

    let h = reader.clone().read_header()?;
    io.wait_for_completion(h)?;

    loop {
        if reader.done.load(Ordering::Acquire) {
            break;
        }
        let offset = reader.off_atomic.load(Ordering::Acquire);
        if offset >= size {
            reader.finalize_loading();
            break;
        }

        let (_read_size, c) = reader.clone().submit_one_chunk(offset)?;
        io.wait_for_completion(c)?;

        let new_off = reader.off_atomic.load(Ordering::Acquire);
        if new_off <= offset {
            reader.finalize_loading();
            break;
        }
    }

    Ok(wal_file_shared)
}

pub(super) struct StreamingWalReader {
    file: Arc<dyn File>,
    wal_shared: Arc<RwLock<WalFileShared>>,
    header: Arc<SpinLock<WalHeader>>,
    file_size: u64,
    state: RwLock<StreamingState>,
    off_atomic: AtomicU64,
    page_atomic: AtomicU64,
    pub(super) done: AtomicBool,
}

/// Mutable state for streaming reader
struct StreamingState {
    frame_idx: u64,
    cumulative_checksum: (u32, u32),
    last_valid_frame: u64,
    pending_frames: HashMap<u64, Vec<u64>>,
    page_size: usize,
    use_native_endian: bool,
    header_valid: bool,
}

impl StreamingWalReader {
    fn new(
        file: Arc<dyn File>,
        wal_shared: Arc<RwLock<WalFileShared>>,
        header: Arc<SpinLock<WalHeader>>,
        file_size: u64,
    ) -> Self {
        Self {
            file,
            wal_shared,
            header,
            file_size,
            off_atomic: AtomicU64::new(0),
            page_atomic: AtomicU64::new(0),
            done: AtomicBool::new(false),
            state: RwLock::new(StreamingState {
                frame_idx: 1,
                cumulative_checksum: (0, 0),
                last_valid_frame: 0,
                pending_frames: HashMap::new(),
                page_size: 0,
                use_native_endian: false,
                header_valid: false,
            }),
        }
    }

    fn read_header(self: Arc<Self>) -> crate::Result<Completion> {
        let header_buf = Arc::new(Buffer::new_temporary(WAL_HEADER_SIZE));
        let reader = self.clone();
        let completion: Box<ReadComplete> = Box::new(move |res| {
            let _reader = reader.clone();
            _reader.handle_header_read(res);
        });
        let c = Completion::new_read(header_buf, completion);
        self.file.pread(0, c)
    }

    fn submit_one_chunk(self: Arc<Self>, offset: u64) -> crate::Result<(usize, Completion)> {
        let page_size = self.page_atomic.load(Ordering::Acquire) as usize;
        if page_size == 0 {
            return Err(crate::LimboError::InternalError(
                "page size not initialized".into(),
            ));
        }
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        if frame_size == 0 {
            return Err(crate::LimboError::InternalError(
                "invalid frame size".into(),
            ));
        }
        const BASE: usize = 16 * 1024 * 1024;
        let aligned = (BASE / frame_size) * frame_size;
        let read_size = aligned
            .max(frame_size)
            .min((self.file_size - offset) as usize);
        if read_size == 0 {
            // end-of-file; let caller finalize
            return Ok((0, Completion::new_dummy()));
        }

        let buf = Arc::new(Buffer::new_temporary(read_size));
        let me = self.clone();
        let completion: Box<ReadComplete> = Box::new(move |res| {
            tracing::debug!("WAL chunk read complete");
            let reader = me.clone();
            reader.handle_chunk_read(res);
        });
        let c = Completion::new_read(buf, completion);
        let guard = self.file.pread(offset, c)?;
        Ok((read_size, guard))
    }

    fn handle_header_read(self: Arc<Self>, res: Result<(Arc<Buffer>, i32), CompletionError>) {
        let Ok((buf, bytes_read)) = res else {
            self.finalize_loading();
            return;
        };
        if bytes_read != WAL_HEADER_SIZE as i32 {
            self.finalize_loading();
            return;
        }

        let (page_sz, c1, c2, use_native, ok) = {
            let mut h = self.header.lock();
            let s = buf.as_slice();
            h.magic = u32::from_be_bytes(s[0..4].try_into().unwrap());
            h.file_format = u32::from_be_bytes(s[4..8].try_into().unwrap());
            h.page_size = u32::from_be_bytes(s[8..12].try_into().unwrap());
            h.checkpoint_seq = u32::from_be_bytes(s[12..16].try_into().unwrap());
            h.salt_1 = u32::from_be_bytes(s[16..20].try_into().unwrap());
            h.salt_2 = u32::from_be_bytes(s[20..24].try_into().unwrap());
            h.checksum_1 = u32::from_be_bytes(s[24..28].try_into().unwrap());
            h.checksum_2 = u32::from_be_bytes(s[28..32].try_into().unwrap());
            tracing::debug!("WAL header: {:?}", *h);

            let use_native = cfg!(target_endian = "big") == ((h.magic & 1) != 0);
            let calc = checksum_wal(&s[0..24], &h, (0, 0), use_native);
            (
                h.page_size,
                h.checksum_1,
                h.checksum_2,
                use_native,
                calc == (h.checksum_1, h.checksum_2),
            )
        };
        if PageSize::new(page_sz).is_none() || !ok {
            self.finalize_loading();
            return;
        }
        {
            let mut st = self.state.write();
            st.page_size = page_sz as usize;
            st.use_native_endian = use_native;
            st.cumulative_checksum = (c1, c2);
            st.header_valid = true;
        }
        self.off_atomic
            .store(WAL_HEADER_SIZE as u64, Ordering::Release);
        self.page_atomic.store(page_sz as u64, Ordering::Release);
    }

    fn handle_chunk_read(self: Arc<Self>, res: Result<(Arc<Buffer>, i32), CompletionError>) {
        let Ok((buf, bytes_read)) = res else {
            self.finalize_loading();
            return;
        };
        let buf_slice = &buf.as_slice()[..bytes_read as usize];
        // Snapshot salts/endianness once to avoid per-frame header locks
        let (header_copy, use_native) = {
            let st = self.state.read();
            let h = self.header.lock();
            (*h, st.use_native_endian)
        };

        let consumed = self.process_frames(buf_slice, &header_copy, use_native);
        self.off_atomic.fetch_add(consumed as u64, Ordering::AcqRel);
        // If we didn’t consume the full chunk, we hit a stop condition
        if consumed < buf_slice.len() || self.off_atomic.load(Ordering::Acquire) >= self.file_size {
            self.finalize_loading();
        }
    }

    // Processes frames from a buffer, returns bytes processed
    fn process_frames(&self, buf: &[u8], header: &WalHeader, use_native: bool) -> usize {
        let mut st = self.state.write();
        let page_size = st.page_size;
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let mut pos = 0;

        while pos + frame_size <= buf.len() {
            let fh = &buf[pos..pos + WAL_FRAME_HEADER_SIZE];
            let page = &buf[pos + WAL_FRAME_HEADER_SIZE..pos + frame_size];

            let page_number = u32::from_be_bytes(fh[0..4].try_into().unwrap());
            let db_size = u32::from_be_bytes(fh[4..8].try_into().unwrap());
            let s1 = u32::from_be_bytes(fh[8..12].try_into().unwrap());
            let s2 = u32::from_be_bytes(fh[12..16].try_into().unwrap());
            let c1 = u32::from_be_bytes(fh[16..20].try_into().unwrap());
            let c2 = u32::from_be_bytes(fh[20..24].try_into().unwrap());

            if page_number == 0 {
                break;
            }
            if s1 != header.salt_1 || s2 != header.salt_2 {
                break;
            }

            let seed = checksum_wal(&fh[0..8], header, st.cumulative_checksum, use_native);
            let calc = checksum_wal(page, header, seed, use_native);
            if calc != (c1, c2) {
                break;
            }

            st.cumulative_checksum = calc;
            let frame_idx = st.frame_idx;
            st.pending_frames
                .entry(page_number as u64)
                .or_default()
                .push(frame_idx);

            if db_size > 0 {
                st.last_valid_frame = st.frame_idx;
                self.flush_pending_frames(&mut st);
            }
            st.frame_idx += 1;
            pos += frame_size;
        }
        pos
    }

    fn flush_pending_frames(&self, state: &mut StreamingState) {
        if state.pending_frames.is_empty() {
            return;
        }
        let wfs = self.wal_shared.read();
        {
            let mut frame_cache = wfs.frame_cache.lock();
            for (page, mut frames) in state.pending_frames.drain() {
                // Only include frames up to last valid commit
                frames.retain(|&f| f <= state.last_valid_frame);
                if !frames.is_empty() {
                    frame_cache.entry(page).or_default().extend(frames);
                }
            }
        }
        wfs.max_frame
            .store(state.last_valid_frame, Ordering::Release);
    }

    /// Finalizes the loading process
    fn finalize_loading(&self) {
        let mut wfs = self.wal_shared.write();
        let st = self.state.read();

        let max_frame = st.last_valid_frame;
        if max_frame > 0 {
            let mut frame_cache = wfs.frame_cache.lock();
            for frames in frame_cache.values_mut() {
                frames.retain(|&f| f <= max_frame);
            }
            frame_cache.retain(|_, frames| !frames.is_empty());
        }

        wfs.max_frame.store(max_frame, Ordering::SeqCst);
        wfs.last_checksum = st.cumulative_checksum;
        if st.header_valid {
            wfs.initialized.store(true, Ordering::SeqCst);
        }
        wfs.nbackfills.store(0, Ordering::SeqCst);
        wfs.loaded.store(true, Ordering::SeqCst);

        self.done.store(true, Ordering::Release);
        tracing::info!(
            "WAL loading complete: {} frames processed, last commit at frame {}",
            st.frame_idx - 1,
            max_frame
        );
    }
}

pub fn begin_read_wal_frame_raw(
    buffer_pool: &Arc<BufferPool>,
    io: &Arc<dyn File>,
    offset: u64,
    complete: Box<ReadComplete>,
) -> Result<Completion> {
    tracing::trace!("begin_read_wal_frame_raw(offset={})", offset);
    let buf = Arc::new(buffer_pool.get_wal_frame());
    let c = Completion::new_read(buf, complete);
    let c = io.pread(offset, c)?;
    Ok(c)
}

pub fn begin_read_wal_frame(
    io: &Arc<dyn File>,
    offset: u64,
    buffer_pool: Arc<BufferPool>,
    complete: Box<ReadComplete>,
    page_idx: usize,
    io_ctx: &IOContext,
) -> Result<Completion> {
    tracing::trace!(
        "begin_read_wal_frame(offset={}, page_idx={})",
        offset,
        page_idx
    );
    let buf = buffer_pool.get_page();
    let buf = Arc::new(buf);

    match io_ctx.encryption_or_checksum() {
        EncryptionOrChecksum::Encryption(ctx) => {
            let encryption_ctx = ctx.clone();
            let original_complete = complete;

            let decrypt_complete =
                Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                    let Ok((encrypted_buf, bytes_read)) = res else {
                        original_complete(res);
                        return;
                    };
                    assert!(
                        bytes_read > 0,
                        "Expected to read some data on success for page_idx={page_idx}"
                    );
                    match encryption_ctx.decrypt_page(encrypted_buf.as_slice(), page_idx) {
                        Ok(decrypted_data) => {
                            encrypted_buf
                                .as_mut_slice()
                                .copy_from_slice(&decrypted_data);
                            original_complete(Ok((encrypted_buf, bytes_read)));
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to decrypt WAL frame data for page_idx={page_idx}: {e}"
                            );
                            original_complete(Err(CompletionError::DecryptionError { page_idx }));
                        }
                    }
                });

            let new_completion = Completion::new_read(buf, decrypt_complete);
            io.pread(offset, new_completion)
        }
        EncryptionOrChecksum::Checksum(ctx) => {
            let checksum_ctx = ctx.clone();
            let original_c = complete;
            let verify_complete =
                Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                    let Ok((buf, bytes_read)) = res else {
                        original_c(res);
                        return;
                    };
                    if bytes_read <= 0 {
                        tracing::trace!("Read page {page_idx} with {} bytes", bytes_read);
                        original_c(Ok((buf, bytes_read)));
                        return;
                    }

                    match checksum_ctx.verify_checksum(buf.as_mut_slice(), page_idx) {
                        Ok(_) => {
                            original_c(Ok((buf, bytes_read)));
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to verify checksum for page_id={page_idx}: {e}"
                            );
                            original_c(Err(e))
                        }
                    }
                });
            let c = Completion::new_read(buf, verify_complete);
            io.pread(offset, c)
        }
        EncryptionOrChecksum::None => {
            let c = Completion::new_read(buf, complete);
            io.pread(offset, c)
        }
    }
}

pub fn parse_wal_frame_header(frame: &[u8]) -> (WalFrameHeader, &[u8]) {
    let page_number = u32::from_be_bytes(frame[0..4].try_into().unwrap());
    let db_size = u32::from_be_bytes(frame[4..8].try_into().unwrap());
    let salt_1 = u32::from_be_bytes(frame[8..12].try_into().unwrap());
    let salt_2 = u32::from_be_bytes(frame[12..16].try_into().unwrap());
    let checksum_1 = u32::from_be_bytes(frame[16..20].try_into().unwrap());
    let checksum_2 = u32::from_be_bytes(frame[20..24].try_into().unwrap());
    let header = WalFrameHeader {
        page_number,
        db_size,
        salt_1,
        salt_2,
        checksum_1,
        checksum_2,
    };
    let page = &frame[WAL_FRAME_HEADER_SIZE..];
    (header, page)
}

pub fn prepare_wal_frame(
    buffer_pool: &Arc<BufferPool>,
    wal_header: &WalHeader,
    prev_checksums: (u32, u32),
    page_size: u32,
    page_number: u32,
    db_size: u32,
    page: &[u8],
) -> ((u32, u32), Arc<Buffer>) {
    tracing::trace!(page_number);

    let buffer = buffer_pool.get_wal_frame();
    let frame = buffer.as_mut_slice();
    frame[WAL_FRAME_HEADER_SIZE..].copy_from_slice(page);

    frame[0..4].copy_from_slice(&page_number.to_be_bytes());
    frame[4..8].copy_from_slice(&db_size.to_be_bytes());
    frame[8..12].copy_from_slice(&wal_header.salt_1.to_be_bytes());
    frame[12..16].copy_from_slice(&wal_header.salt_2.to_be_bytes());

    let expects_be = wal_header.magic & 1;
    let use_native_endian = cfg!(target_endian = "big") as u32 == expects_be;
    let header_checksum = checksum_wal(&frame[0..8], wal_header, prev_checksums, use_native_endian);
    let final_checksum = checksum_wal(
        &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + page_size as usize],
        wal_header,
        header_checksum,
        use_native_endian,
    );
    frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
    frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());

    (final_checksum, Arc::new(buffer))
}

pub fn begin_write_wal_header(io: &Arc<dyn File>, header: &WalHeader) -> Result<Completion> {
    tracing::trace!("begin_write_wal_header");
    let buffer = {
        let buffer = Buffer::new_temporary(WAL_HEADER_SIZE);
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
        Arc::new(buffer)
    };

    let cloned = buffer.clone();
    let write_complete = move |res: Result<i32, CompletionError>| {
        let Ok(bytes_written) = res else {
            return;
        };
        // make sure to reference buffer so it's alive for async IO
        let _buf = cloned.clone();
        turso_assert!(
            bytes_written == WAL_HEADER_SIZE as i32,
            "wal header wrote({bytes_written}) != expected({WAL_HEADER_SIZE})"
        );
    };
    #[allow(clippy::arc_with_non_send_sync)]
    let c = Completion::new_write(write_complete);
    let c = io.pwrite(0, buffer.clone(), c.clone())?;
    Ok(c)
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
    #[case(&[1, 2, 3], SerialType::blob(3), Value::Blob(vec![1, 2, 3]))]
    #[case(&[], SerialType::blob(0), Value::Blob(vec![]))] // empty blob
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

        (0..8usize).for_each(|i| {
            assert_eq!(small_vec.get(i), Some(i as i32));
        });

        assert_eq!(small_vec.get(8), None);
    }

    #[rstest]
    #[case(&[])] // empty buffer
    #[case(&[0x80])] // truncated 1-byte with continuation
    #[case(&[0x80, 0x80])] // truncated 2-byte
    #[case(&[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80])] // 9-byte truncated to 8
    #[case(&[0x80; 9])] // bits set without end
    fn test_read_varint_malformed_inputs(#[case] buf: &[u8]) {
        assert!(read_varint(buf).is_err());
    }
}
