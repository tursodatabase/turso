//! Differential test over hand-written B-tree files.
//!
//! The generator below writes raw B-tree pages (interior/leaf cells, free
//! blocks, fragmented bytes, overflow chains, shuffled physical placement)
//! straight into a database file. The layouts are valid per the SQLite file
//! format but unusual: stale divider keys, pages far from how a real writer
//! would balance them, gaps filled with 0xFF. The test then compares Turso
//! against SQLite on the same file: integrity_check, full-content reads,
//! range scans in both directions, point lookups, and finally an identical
//! stream of writes applied to identical copies of the file. Every attempt
//! picks its own page size, so layouts are exercised at each size SQLite
//! supports (except 65536, see PAGE_SIZES).
//!
//! The corrupt variants (`test_btree_corrupt_*`) reuse the generator but
//! flip a small fraction of fields while serializing pages: off-by-one
//! lengths, pointers to wrong or nonexistent pages, garbage bytes. A corrupt
//! file has no "right answer", so those tests only demand that turso answers
//! every query with either rows or an error: no panic, no crash, no hang.
//!
//! Env knobs: `BTREE_TEST_SEED` picks the random seed; `BTREE_TEST_TARGET_SIZE`
//! sets an approximate generated-database size (e.g. `512KB`, `500MB`, `2GB`,
//! or a raw byte count), converted internally to a blob-length scale. Writing
//! stays memory-bounded at any size; reads that pull whole blobs do not.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    panic::AssertUnwindSafe,
    path::Path,
    rc::Rc,
    sync::{mpsc, Arc, Mutex, Once, OnceLock},
    thread::{self, ThreadId},
    time::Duration,
};

use rand::{seq::SliceRandom, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Buffer, Completion, File, OpenFlags, PlatformIO, IO};
use zerocopy::big_endian::{U16, U32, U64};

use crate::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};

/// Page sizes the generator picks from. 65536 is excluded: the format
/// special-cases it (a 0 in two-byte header fields means 65536) and the
/// generator's two-byte offset arithmetic does not model that.
const PAGE_SIZES: &[usize] = &[512, 1024, 2048, 4096, 8192, 16384, 32768];
/// Largest rowid the generator hands out (rowids are signed in SQL).
const MAX_ROWID: u64 = i64::MAX as u64;
/// Target size of the buffer the writer accumulates before flushing to disk,
/// so large files cost a handful of big writes instead of one syscall per page
/// while memory stays bounded (rounded down to whole pages at write time).
const FLUSH_CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// Largest table-leaf payload stored fully in-page: usable - 35. The whole
/// page is usable: the generator assumes no reserved space.
fn leaf_max_local(page_size: usize) -> usize {
    page_size - 35
}

/// Smallest in-page part of an overflowing payload: (usable-12)*32/255 - 23.
fn leaf_min_local(page_size: usize) -> usize {
    (page_size - 12) * 32 / 255 - 23
}

#[derive(Debug, Eq, PartialEq)]
pub enum BTreePageType {
    Interior,
    Leaf,
}

#[derive(Debug)]
struct BTreeFreeBlock {
    offset: u16,
    size: u16,
}

/// An overflow chain, described rather than materialized: the blob bytes
/// [blob_offset .. blob_offset + byte_len) come from `payload_byte(seed, ..)`,
/// and the chain occupies `overflow_page_count` pages. Keeping this a plain
/// descriptor (instead of a linked list of page nodes) is what bounds the
/// generator's working set: the tree never holds the blob bytes, and there is
/// no long node chain to recurse over or drop.
#[derive(Debug)]
pub struct OverflowRun {
    seed: u64,
    blob_offset: usize,
    byte_len: usize,
}

impl OverflowRun {
    fn page_count(&self, page_size: usize) -> u32 {
        self.byte_len.div_ceil(page_size - 4) as u32
    }
}

#[derive(Debug)]
pub struct BTreeLeafCell {
    size: usize,
    rowid: u64,
    on_page_data: Vec<u8>,
    overflow: Option<OverflowRun>,
}
#[derive(Debug)]
pub struct BTreeInteriorCell {
    left_child_pointer: Rc<BTreeTablePageData>,
    rowid: u64,
}

#[derive(Debug)]
pub enum BTreeCell {
    Interior(BTreeInteriorCell),
    Leaf(BTreeLeafCell),
}

impl BTreeCell {
    fn size(&self) -> u16 {
        match self {
            BTreeCell::Interior(cell) => 4 + length_varint(cell.rowid) as u16,
            BTreeCell::Leaf(cell) => {
                (length_varint(cell.size as u64)
                    + length_varint(cell.rowid)
                    + cell.on_page_data.len()
                    + cell.overflow.as_ref().map(|_| 4).unwrap_or(0)) as u16
            }
        }
    }
}

#[derive(Debug)]
pub struct BTreeTablePageData {
    page_type: BTreePageType,
    cell_content_area: u16,
    cell_right_pointer: Option<Rc<BTreeTablePageData>>,
    fragmented_free_bytes: u8,
    cells: Vec<(u16, BTreeCell)>,
    free_blocks: Vec<BTreeFreeBlock>,
}

/// Collect the table pages (interior + leaf) in a stable pre-order. Recursion
/// depth equals tree depth (bounded), never the overflow chain length.
fn list_table_pages(root: &Rc<BTreeTablePageData>, pages: &mut Vec<Rc<BTreeTablePageData>>) {
    pages.push(root.clone());
    for (_, cell) in &root.cells {
        if let BTreeCell::Interior(cell) = cell {
            list_table_pages(&cell.left_child_pointer, pages);
        }
    }
    if let Some(right) = &root.cell_right_pointer {
        list_table_pages(right, pages);
    }
}

fn collect_rowids(root: &Rc<BTreeTablePageData>, rowids: &mut Vec<u64>) {
    for (_, cell) in &root.cells {
        match cell {
            BTreeCell::Interior(cell) => collect_rowids(&cell.left_child_pointer, rowids),
            BTreeCell::Leaf(cell) => rowids.push(cell.rowid),
        }
    }
    if let Some(right) = &root.cell_right_pointer {
        collect_rowids(right, rowids);
    }
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

    let mut encoded: [u8; 9] = [0; 9];
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

pub fn length_varint(value: u64) -> usize {
    let mut buf = [0u8; 10];
    write_varint(&mut buf, value)
}

fn write_u64_column(header: &mut Vec<u8>, data: &mut Vec<u8>, value: u64) {
    let mut buf = [0u8; 10];
    let buf_len = write_varint(&mut buf, 6u64);
    header.extend_from_slice(&buf[0..buf_len]);
    data.extend_from_slice(&U64::new(value).to_bytes());
}

/// Deterministic pseudo-random blob byte at a given index, derived purely from
/// a per-cell seed. Lets a payload of any size be regenerated one slice at a
/// time (at write) without ever holding the whole blob in memory (splitmix64).
fn payload_byte(seed: u64, index: usize) -> u8 {
    let mut x = seed ^ (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (x ^ (x >> 31)) as u8
}

fn fill_payload(seed: u64, start: usize, dst: &mut [u8]) {
    for (i, byte) in dst.iter_mut().enumerate() {
        *byte = payload_byte(seed, start + i);
    }
}

/// Everything in a table-leaf record except the blob bytes: the header-length
/// varint, the serial-type header, and the (fixed-size) rowid column. The blob
/// bytes follow this prefix and are generated lazily via `payload_byte`.
fn record_prefix(rowid: u64, blob_len: usize, key_as_null: bool) -> Vec<u8> {
    let mut header = Vec::new();
    let mut key_data = Vec::new();
    if key_as_null {
        // SQLite itself stores NULL for an INTEGER PRIMARY KEY column and
        // substitutes the rowid on read. Storing the integer instead (the
        // else-branch) is a layout SQLite tolerates but never produces.
        header.push(0);
    } else {
        write_u64_column(&mut header, &mut key_data, rowid);
    }
    let mut buf = [0u8; 10];
    let buf_len = write_varint(&mut buf, (blob_len * 2 + 12) as u64);
    header.extend_from_slice(&buf[0..buf_len]);

    let header_len = header.len() + 1;
    assert!(header_len <= 127);
    let buf_len = write_varint(&mut buf, header_len as u64);
    let mut prefix = buf[0..buf_len].to_vec();
    prefix.extend_from_slice(&header);
    prefix.extend_from_slice(&key_data);
    prefix
}

/// How many payload bytes stay on the leaf page; the rest go to overflow
/// pages. Mirrors the SQLite table-leaf formula.
fn leaf_local_size(payload_size: usize, page_size: usize) -> usize {
    let max_local = leaf_max_local(page_size);
    if payload_size <= max_local {
        return payload_size;
    }
    let min_local = leaf_min_local(page_size);
    let surplus = min_local + (payload_size - min_local) % (page_size - 4);
    if surplus <= max_local {
        surplus
    } else {
        min_local
    }
}

/// Build a leaf cell for `rowid` carrying a blob of `blob_len` bytes derived
/// from `blob_seed`. The local (in-page) portion is materialized (bounded by
/// the page size); the overflow portion is a chain of pages that only remember
/// which blob slice they cover, so the blob bytes never live in memory.
fn build_leaf_cell(
    rowid: u64,
    blob_len: usize,
    blob_seed: u64,
    key_as_null: bool,
    page_size: usize,
) -> BTreeLeafCell {
    let prefix = record_prefix(rowid, blob_len, key_as_null);
    let total_size = prefix.len() + blob_len;
    let local_size = leaf_local_size(total_size, page_size);

    // Local bytes = record prefix followed by the first blob bytes.
    let local_blob = local_size - prefix.len();
    let mut on_page_data = prefix;
    let start = on_page_data.len();
    on_page_data.resize(local_size, 0);
    fill_payload(blob_seed, 0, &mut on_page_data[start..]);

    // The rest of the blob (if any) becomes an overflow descriptor, expanded to
    // pages only at write time.
    let overflow = (local_size < total_size).then(|| OverflowRun {
        seed: blob_seed,
        blob_offset: local_blob,
        byte_len: blob_len - local_blob,
    });

    BTreeLeafCell {
        size: total_size,
        rowid,
        on_page_data,
        overflow,
    }
}

/// Rough size of a representative database at scale 1.0. Used only to turn a
/// requested target size into a blob-length scale. The mapping is approximate:
/// a run spans several depths, fanouts and page sizes, so individual attempts
/// land above and below the target, and the largest (depth-3) attempts track it
/// most closely.
const REFERENCE_DB_BYTES: f64 = 1024.0 * 1024.0;

/// Blob-length multiplier for this run. Set `BTREE_TEST_TARGET_SIZE` to an
/// approximate database size (`512KB`, `500MB`, `2GB`, `2GiB`, or a raw byte
/// count) and it is converted to a scale internally; unset means scale 1.0.
/// Generation stays memory-bounded at any size, but queries that read whole
/// blobs use more memory the larger the target.
fn size_scale() -> f64 {
    match env_target_bytes() {
        Some(target) => (target as f64 / REFERENCE_DB_BYTES).max(f64::MIN_POSITIVE),
        None => 1.0,
    }
}

/// Parse `BTREE_TEST_TARGET_SIZE`. Accepts a raw byte count or a number with a
/// K/M/G/T suffix (optionally `B`/`iB`); all suffixes are 1024-based.
fn env_target_bytes() -> Option<u64> {
    let raw = std::env::var("BTREE_TEST_TARGET_SIZE").ok()?;
    let raw = raw.trim();
    let digits_end = raw
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(raw.len());
    let (num, suffix) = raw.split_at(digits_end);
    let num: f64 = num.parse().ok()?;
    let mult: f64 = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((num * mult) as u64)
}

/// Blob length classes: small (most rows), medium (fills pages with just a few
/// cells, still mostly in-page) and large (straddles the in-page threshold, so
/// most of these records spill into overflow chains). `scale` stretches every
/// class so the generated file size can be dialed up (see `size_scale`).
fn random_blob_len(rng: &mut ChaCha8Rng, page_size: usize, scale: f64) -> usize {
    let max_local = leaf_max_local(page_size);
    let class = rng.next_u32() % 100;
    let base = if class < 60 {
        rng.next_u32() as usize % 128
    } else if class < 85 {
        128 + rng.next_u32() as usize % (max_local - 128)
    } else {
        max_local.saturating_sub(page_size / 8) + rng.next_u32() as usize % (page_size * 3)
    };
    (base as f64 * scale) as usize
}

/// Materialized random blob, for the SQL write phase where the bytes go into a
/// statement rather than a generated page. Bounded by the query, not the file,
/// so it is never scaled up.
fn random_payload(rng: &mut ChaCha8Rng, page_size: usize) -> Vec<u8> {
    let len = random_blob_len(rng, page_size, 1.0);
    let mut payload = vec![0u8; len];
    rng.fill_bytes(&mut payload);
    payload
}

/// Per-field corruption chances, in permille (checked once per field while
/// serializing pages). Zero everywhere means the generated file stays valid.
#[derive(Clone, Copy, Debug, Default)]
struct CorruptionProfile {
    /// +-1 on length-like fields: cell counts, content area, cell and
    /// freeblock offsets, size varints, the header page count.
    off_by_one_permille: u32,
    /// Page pointers (child, right, overflow) replaced with 0, page 1, a
    /// page past EOF, a random u32, or a wrong-but-existing page (which can
    /// create cycles). In-page offsets replaced with random values.
    bad_pointer_permille: u32,
    /// Random bytes over the fragmentation counter or a random page region.
    garbage_permille: u32,
}

impl CorruptionProfile {
    const NONE: Self = Self {
        off_by_one_permille: 0,
        bad_pointer_permille: 0,
        garbage_permille: 0,
    };

    fn enabled(&self) -> bool {
        self.off_by_one_permille + self.bad_pointer_permille + self.garbage_permille > 0
    }
}

struct BTreeGenerator<'a> {
    rng: &'a mut ChaCha8Rng,
    page_size: usize,
    max_interior_keys: usize,
    max_leaf_keys: usize,
    /// Blob-length multiplier; see `size_scale`.
    blob_scale: f64,
    profile: CorruptionProfile,
    /// Human-readable description of every corruption applied.
    corruptions: Vec<String>,
}

impl BTreeGenerator<'_> {
    fn chance(&mut self, permille: u32) -> bool {
        permille > 0 && self.rng.next_u32() % 1000 < permille
    }

    /// +-1 on a two-byte field (wrapping, so 0 becomes 65535).
    fn corrupt_u16_off_by_one(&mut self, page_no: u32, what: &str, value: u16) -> u16 {
        if !self.chance(self.profile.off_by_one_permille) {
            return value;
        }
        let corrupted = if self.rng.next_u32() % 2 == 0 {
            value.wrapping_add(1)
        } else {
            value.wrapping_sub(1)
        };
        self.corruptions
            .push(format!("page {page_no}: {what} {value} -> {corrupted}"));
        corrupted
    }

    /// Replace an in-page offset with a random value (may point into the
    /// header, into another cell, or past the end of the page).
    fn corrupt_u16_offset(&mut self, page_no: u32, what: &str, value: u16) -> u16 {
        if !self.chance(self.profile.bad_pointer_permille) {
            return value;
        }
        let corrupted = (self.rng.next_u32() % (self.page_size as u32 * 2)) as u16;
        self.corruptions
            .push(format!("page {page_no}: {what} {value} -> {corrupted}"));
        corrupted
    }

    /// Replace a page pointer with one that must not be followed blindly.
    fn corrupt_page_pointer(&mut self, page_no: u32, what: &str, value: u32, max_page: u32) -> u32 {
        if !self.chance(self.profile.bad_pointer_permille) {
            return value;
        }
        let corrupted = match self.rng.next_u32() % 5 {
            0 => 0,
            1 => 1,
            2 => max_page + 1 + self.rng.next_u32() % 100,
            3 => self.rng.next_u32(),
            // wrong-but-existing page: cross-links and possible cycles
            _ => 2 + self.rng.next_u32() % max_page.saturating_sub(1).max(1),
        };
        self.corruptions
            .push(format!("page {page_no}: {what} {value} -> {corrupted}"));
        corrupted
    }

    /// +-1 on a varint-encoded value, only when the re-encoded varint keeps
    /// its length so the rest of the cell stays in place.
    fn corrupt_varint_off_by_one(&mut self, page_no: u32, what: &str, value: u64) -> u64 {
        if !self.chance(self.profile.off_by_one_permille) {
            return value;
        }
        let corrupted = if self.rng.next_u32() % 2 == 0 {
            value.wrapping_add(1)
        } else {
            value.wrapping_sub(1)
        };
        if length_varint(corrupted) != length_varint(value) {
            return value;
        }
        self.corruptions
            .push(format!("page {page_no}: {what} {value} -> {corrupted}"));
        corrupted
    }

    fn corrupt_u8_garbage(&mut self, page_no: u32, what: &str, value: u8) -> u8 {
        if !self.chance(self.profile.garbage_permille) {
            return value;
        }
        let corrupted = (self.rng.next_u32() & 0xff) as u8;
        self.corruptions
            .push(format!("page {page_no}: {what} {value} -> {corrupted}"));
        corrupted
    }

    /// Splat 8..=64 random bytes over a random region of the page.
    fn corrupt_garbage_region(&mut self, page_no: u32, data: &mut [u8]) {
        if !self.chance(self.profile.garbage_permille) {
            return;
        }
        let len = 8 + self.rng.next_u32() as usize % 57;
        let start = self.rng.next_u32() as usize % (data.len() - len);
        for byte in &mut data[start..start + len] {
            *byte = (self.rng.next_u32() & 0xff) as u8;
        }
        self.corruptions.push(format!(
            "page {page_no}: garbage over bytes {start}..{}",
            start + len
        ));
    }

    /// Serialize one page of an overflow chain. `page_no` is this page's number,
    /// `next_page` the following page in the chain (0 if last), and the blob
    /// slice is regenerated straight into the buffer from the run's seed.
    fn create_overflow_page(
        &mut self,
        page_no: u32,
        max_page: u32,
        next_page: u32,
        run: &OverflowRun,
        chunk_index: u32,
    ) -> Vec<u8> {
        let mut data = vec![255u8; self.page_size];
        let next_page =
            self.corrupt_page_pointer(page_no, "overflow next pointer", next_page, max_page);
        data[0..4].copy_from_slice(&U32::new(next_page).to_bytes());
        let start = chunk_index as usize * (self.page_size - 4);
        let len = (run.byte_len - start).min(self.page_size - 4);
        fill_payload(run.seed, run.blob_offset + start, &mut data[4..4 + len]);
        self.corrupt_garbage_region(page_no, &mut data);
        data
    }

    /// Serialize a table page. `overflow_first` gives, per leaf cell that has an
    /// overflow chain (in cell order), the page number where its chain starts;
    /// `run_cursor` walks that slice as cells are visited.
    fn create_btree_page(
        &mut self,
        page_no: u32,
        max_page: u32,
        page: &BTreeTablePageData,
        page_numbers: &HashMap<*const BTreeTablePageData, u32>,
        overflow_first: &[u32],
        run_cursor: &mut usize,
    ) -> Vec<u8> {
        let mut data = vec![255u8; self.page_size];

        data[0] = match page.page_type {
            BTreePageType::Interior => 0x05,
            BTreePageType::Leaf => 0x0d,
        };
        let first_free_block = page.free_blocks.first().map(|x| x.offset).unwrap_or(0);
        let first_free_block =
            self.corrupt_u16_off_by_one(page_no, "first freeblock offset", first_free_block);
        let first_free_block =
            self.corrupt_u16_offset(page_no, "first freeblock offset", first_free_block);
        data[1..3].copy_from_slice(&U16::new(first_free_block).to_bytes());
        let cell_count =
            self.corrupt_u16_off_by_one(page_no, "cell count", page.cells.len() as u16);
        data[3..5].copy_from_slice(&U16::new(cell_count).to_bytes());
        let content_area =
            self.corrupt_u16_off_by_one(page_no, "content area", page.cell_content_area);
        data[5..7].copy_from_slice(&U16::new(content_area).to_bytes());
        data[7] = self.corrupt_u8_garbage(page_no, "fragmented bytes", page.fragmented_free_bytes);
        let mut offset = 8;
        if page.page_type == BTreePageType::Interior {
            let cell_right_pointer = page.cell_right_pointer.as_ref().unwrap();
            let cell_right_pointer = Rc::as_ptr(cell_right_pointer);
            let cell_right_pointer = *page_numbers.get(&cell_right_pointer).unwrap();
            let cell_right_pointer =
                self.corrupt_page_pointer(page_no, "right pointer", cell_right_pointer, max_page);
            data[8..12].copy_from_slice(&U32::new(cell_right_pointer).to_bytes());
            offset = 12;
        }

        for (i, (pointer, _)) in page.cells.iter().enumerate() {
            let pointer = self.corrupt_u16_off_by_one(page_no, "cell pointer", *pointer);
            let pointer = self.corrupt_u16_offset(page_no, "cell pointer", pointer);
            data[offset + 2 * i..offset + 2 * (i + 1)]
                .copy_from_slice(&U16::new(pointer).to_bytes());
        }

        for i in 0..page.free_blocks.len() {
            let offset = page.free_blocks[i].offset as usize;
            let next = page.free_blocks.get(i + 1).map(|x| x.offset).unwrap_or(0);
            let next = self.corrupt_u16_off_by_one(page_no, "freeblock next offset", next);
            let next = self.corrupt_u16_offset(page_no, "freeblock next offset", next);
            data[offset..offset + 2].copy_from_slice(&U16::new(next).to_bytes());
            let size =
                self.corrupt_u16_off_by_one(page_no, "freeblock size", page.free_blocks[i].size);
            data[offset + 2..offset + 4].copy_from_slice(&U16::new(size).to_bytes());
        }

        for (pointer, cell) in page.cells.iter() {
            let mut p = *pointer as usize;
            match cell {
                BTreeCell::Interior(cell) => {
                    let left_child_pointer = Rc::as_ptr(&cell.left_child_pointer);
                    let left_child_pointer = *page_numbers.get(&left_child_pointer).unwrap();
                    let left_child_pointer = self.corrupt_page_pointer(
                        page_no,
                        "child pointer",
                        left_child_pointer,
                        max_page,
                    );
                    data[p..p + 4].copy_from_slice(&U32::new(left_child_pointer).to_bytes());
                    p += 4;
                    let rowid =
                        self.corrupt_varint_off_by_one(page_no, "interior rowid", cell.rowid);
                    _ = write_varint(&mut data[p..], rowid);
                }
                BTreeCell::Leaf(cell) => {
                    let size =
                        self.corrupt_varint_off_by_one(page_no, "payload size", cell.size as u64);
                    p += write_varint(&mut data[p..], size);
                    let rowid = self.corrupt_varint_off_by_one(page_no, "leaf rowid", cell.rowid);
                    p += write_varint(&mut data[p..], rowid);
                    data[p..p + cell.on_page_data.len()].copy_from_slice(&cell.on_page_data);
                    p += cell.on_page_data.len();
                    if cell.overflow.is_some() {
                        let first = overflow_first[*run_cursor];
                        *run_cursor += 1;
                        let first =
                            self.corrupt_page_pointer(page_no, "overflow pointer", first, max_page);
                        data[p..p + 4].copy_from_slice(&U32::new(first).to_bytes());
                    }
                }
            }
        }

        self.corrupt_garbage_region(page_no, &mut data);
        data
    }

    fn generate_btree(&mut self, depth: usize, mut l: u64, r: u64) -> Rc<BTreeTablePageData> {
        let mut cells = vec![];
        let cells_max_limit = if depth == 0 {
            self.max_leaf_keys
        } else {
            self.max_interior_keys
        };
        let cells_limit = self.rng.next_u32() as usize % cells_max_limit + 1;

        let mut rowids = HashSet::new();
        for _ in 0..cells_limit {
            let rowid = l + self.rng.next_u64() % (r - l + 1);
            if rowids.contains(&rowid) {
                continue;
            }
            rowids.insert(rowid);
        }

        let mut rowids = rowids.into_iter().collect::<Vec<_>>();
        rowids.sort();

        let header_offset = if depth == 0 { 8 } else { 12 };
        let mut it = 0;
        let mut cells_size = header_offset;
        while cells.len() < cells_limit && it < rowids.len() {
            let rowid = rowids[it];
            it += 1;

            let cell = if depth == 0 {
                let blob_len = random_blob_len(self.rng, self.page_size, self.blob_scale);
                let key_as_null = self.rng.next_u32() % 2 == 0;
                let blob_seed = self.rng.next_u64();
                BTreeCell::Leaf(build_leaf_cell(
                    rowid,
                    blob_len,
                    blob_seed,
                    key_as_null,
                    self.page_size,
                ))
            } else {
                BTreeCell::Interior(BTreeInteriorCell {
                    left_child_pointer: self.generate_btree(depth - 1, l, rowid),
                    rowid,
                })
            };
            if cells_size + 2 + cell.size() > self.page_size as u16 {
                break;
            }
            cells_size += 2 + cell.size();
            cells.push((rowid, cell));
            if depth > 0 {
                l = rowid + 1;
            }
        }

        cells.shuffle(&mut self.rng);

        let mut cells_with_offset = Vec::new();
        let mut fragmentation_budget = self.page_size as u16 - cells_size;
        let mut pointer_offset = header_offset;
        let mut content_offset = self.page_size as u16;
        let mut fragmented_free_bytes = 0;
        let mut free_blocks = vec![];

        for (rowid, cell) in cells {
            let mut fragmentation = ((self.rng.next_u32() % 4) as u16).min(fragmentation_budget);
            if fragmented_free_bytes + fragmentation > 60 {
                fragmentation = 0;
            }
            let mut free_block_size = 0;
            if fragmentation == 0 && fragmentation_budget >= 4 {
                free_block_size = 4 + self.rng.next_u32() as u16 % (fragmentation_budget - 3);
            }

            let cell_size = cell.size() + fragmentation.max(free_block_size);
            assert!(pointer_offset + 2 + cell_size <= content_offset);

            pointer_offset += 2;
            content_offset -= cell_size;
            fragmented_free_bytes += fragmentation;
            fragmentation_budget -= fragmentation.max(free_block_size);
            if free_block_size > 0 {
                free_blocks.push(BTreeFreeBlock {
                    offset: content_offset + cell.size(),
                    size: free_block_size,
                });
            }
            cells_with_offset.push((rowid, content_offset, cell));
        }

        cells_with_offset.sort_by_key(|(rowid, ..)| *rowid);
        let cells = cells_with_offset
            .into_iter()
            .map(|(_, offset, cell)| (offset, cell))
            .collect::<Vec<_>>();

        free_blocks.sort_by_key(|x| x.offset);

        if depth == 0 {
            Rc::new(BTreeTablePageData {
                page_type: BTreePageType::Leaf,
                cell_content_area: content_offset,
                cell_right_pointer: None,
                fragmented_free_bytes: fragmented_free_bytes as u8,
                cells,
                free_blocks,
            })
        } else {
            Rc::new(BTreeTablePageData {
                page_type: BTreePageType::Interior,
                cell_content_area: content_offset,
                cell_right_pointer: if l <= r {
                    Some(self.generate_btree(depth - 1, l, r))
                } else {
                    None
                },
                fragmented_free_bytes: fragmented_free_bytes as u8,
                cells,
                free_blocks,
            })
        }
    }

    fn write_btree(&mut self, path: &Path, root: &Rc<BTreeTablePageData>, start_page: u32) {
        // Only the table pages (interior + leaf) are materialized as nodes;
        // there are few of them (bounded by depth and fanout). Overflow pages
        // exist only as descriptors and are expanded to bytes at write time.
        let mut table_pages = Vec::new();
        list_table_pages(root, &mut table_pages);
        table_pages[1..].shuffle(&mut self.rng);
        let n_table = table_pages.len();
        let mut page_numbers = HashMap::default();
        for (page, page_no) in table_pages.iter().zip(start_page..) {
            page_numbers.insert(Rc::as_ptr(page), page_no);
        }

        // Lay overflow chains out in contiguous page-number blocks that follow
        // the table pages, in the order leaf cells are visited. `overflow_first`
        // records each chain's starting page (for the leaf cell's pointer);
        // `runs` records where to write the chain pages themselves.
        let mut next_overflow = start_page + n_table as u32;
        let mut overflow_first = Vec::new();
        let mut runs: Vec<(u32, &OverflowRun)> = Vec::new();
        for page in &table_pages {
            for (_, cell) in &page.cells {
                if let BTreeCell::Leaf(cell) = cell {
                    if let Some(run) = &cell.overflow {
                        overflow_first.push(next_overflow);
                        runs.push((next_overflow, run));
                        next_overflow += run.page_count(self.page_size);
                    }
                }
            }
        }
        // Highest page number in use == total page count (numbers are 1-based
        // and contiguous: page 1, then table pages, then overflow pages).
        let total_pages = next_overflow - 1;
        let max_page = total_pages;

        let io = PlatformIO::new().unwrap();
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::None, true)
            .unwrap();
        assert_eq!(file.size().unwrap(), (self.page_size * 2) as u64);

        // Pages go to sequential offsets from `start_page` (page 2 onward), so
        // a single chunk writer streams table pages then overflow pages in one
        // increasing run. Page 1 (header + schema) was written by the sqlite
        // bootstrap and is left untouched, except for the page-count field.
        let base_offset = (start_page as usize - 1) * self.page_size;
        let mut writer = ChunkWriter::new(&io, file.as_ref(), self.page_size, base_offset);

        // Table pages, in page-number order (index i -> page start_page + i).
        let mut run_cursor = 0usize;
        for (i, page) in table_pages.iter().enumerate() {
            let bytes = self.create_btree_page(
                start_page + i as u32,
                max_page,
                page,
                &page_numbers,
                &overflow_first,
                &mut run_cursor,
            );
            writer.push(&bytes);
        }
        // Overflow chains, in the same order they were numbered above.
        for (first, run) in &runs {
            let count = run.page_count(self.page_size);
            for j in 0..count {
                let page_no = first + j;
                let next_page = if j + 1 < count { page_no + 1 } else { 0 };
                let bytes = self.create_overflow_page(page_no, max_page, next_page, run, j);
                writer.push(&bytes);
            }
        }
        writer.finish();

        let mut size = total_pages;
        // The corrupt tests need at least one corruption even for tiny trees
        // where no per-field check fired; fall back to the header page count.
        if self.chance(self.profile.off_by_one_permille)
            || (self.profile.enabled() && self.corruptions.is_empty())
        {
            let corrupted = if self.rng.next_u32() % 2 == 0 {
                size + 1
            } else {
                size.saturating_sub(1)
            };
            self.corruptions
                .push(format!("header: page count {size} -> {corrupted}"));
            size = corrupted;
        }
        let size_bytes = U32::new(size).to_bytes();
        write_at(&io, file.as_ref(), 28, &size_bytes);
    }
}

/// Accumulates serialized pages and flushes them to disk in bounded chunks
/// (~`FLUSH_CHUNK_BYTES`), so a large file costs a handful of big sequential
/// writes while the writer's memory stays capped regardless of file size.
struct ChunkWriter<'a, F: File + ?Sized> {
    io: &'a PlatformIO,
    file: &'a F,
    page_size: usize,
    base_offset: usize,
    pages_per_chunk: usize,
    chunk: Vec<u8>,
    /// Index (0-based, relative to base_offset) of the first page in `chunk`.
    chunk_first_page: usize,
    /// Total pages pushed so far.
    pushed: usize,
}

impl<'a, F: File + ?Sized> ChunkWriter<'a, F> {
    fn new(io: &'a PlatformIO, file: &'a F, page_size: usize, base_offset: usize) -> Self {
        let pages_per_chunk = (FLUSH_CHUNK_BYTES / page_size).max(1);
        Self {
            io,
            file,
            page_size,
            base_offset,
            pages_per_chunk,
            chunk: Vec::with_capacity(pages_per_chunk * page_size),
            chunk_first_page: 0,
            pushed: 0,
        }
    }

    fn push(&mut self, page: &[u8]) {
        self.chunk.extend_from_slice(page);
        self.pushed += 1;
        if self.chunk.len() >= self.pages_per_chunk * self.page_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.chunk.is_empty() {
            return;
        }
        let offset = self.base_offset + self.chunk_first_page * self.page_size;
        write_at(self.io, self.file, offset, &self.chunk);
        self.chunk_first_page = self.pushed;
        self.chunk.clear();
    }

    fn finish(&mut self) {
        self.flush();
    }
}

fn write_at<F: File + ?Sized>(io: &impl IO, file: &F, offset: usize, data: &[u8]) {
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new(data.to_vec()));
    let _buf = buffer.clone();
    let completion = Completion::new_write(move |_| {
        // reference the buffer to keep alive for async io
        let _buf = _buf.clone();
    });
    let result = file.pwrite(offset as u64, buffer, completion).unwrap();
    while !result.succeeded() {
        io.step().unwrap();
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

fn value_brief(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Blob(blob) => format!(
            "blob(len={}, prefix={:02x?})",
            blob.len(),
            &blob[..blob.len().min(8)]
        ),
        other => format!("{other:?}"),
    }
}

fn assert_rows_eq(
    sqlite_rows: &[Vec<rusqlite::types::Value>],
    turso_rows: &[Vec<rusqlite::types::Value>],
    query: &str,
    ctx: &str,
) {
    if sqlite_rows == turso_rows {
        return;
    }
    let mismatch = sqlite_rows
        .iter()
        .zip(turso_rows.iter())
        .position(|(sqlite_row, turso_row)| sqlite_row != turso_row)
        .unwrap_or(sqlite_rows.len().min(turso_rows.len()));
    let brief = |rows: &[Vec<rusqlite::types::Value>]| -> String {
        rows.get(mismatch).map_or("<no row>".to_string(), |row| {
            row.iter().map(value_brief).collect::<Vec<_>>().join(", ")
        })
    };
    panic!(
        "{ctx}: `{query}` diverged: sqlite returned {} rows, turso returned {} rows, \
         first difference at row {mismatch}:\n  sqlite: {}\n  turso:  {}",
        sqlite_rows.len(),
        turso_rows.len(),
        brief(sqlite_rows),
        brief(turso_rows),
    );
}

fn compare_query(
    sqlite_conn: &rusqlite::Connection,
    turso_conn: &Arc<turso_core::Connection>,
    query: &str,
    ctx: &str,
) {
    let sqlite_rows = sqlite_exec_rows(sqlite_conn, query);
    let turso_rows = limbo_exec_rows(turso_conn, query);
    assert_rows_eq(&sqlite_rows, &turso_rows, query, ctx);
}

/// One turso row converted to the rusqlite value representation, so the two
/// engines can be compared with a single owned row in flight at a time.
fn turso_row_values(row: &turso_core::Row) -> Vec<rusqlite::types::Value> {
    row.get_values()
        .map(|x| match x {
            turso_core::Value::Null => rusqlite::types::Value::Null,
            turso_core::Value::Numeric(turso_core::Numeric::Integer(x)) => {
                rusqlite::types::Value::Integer(*x)
            }
            turso_core::Value::Numeric(turso_core::Numeric::Float(x)) => {
                rusqlite::types::Value::Real(f64::from(*x))
            }
            turso_core::Value::Text(x) => rusqlite::types::Value::Text(x.as_str().to_string()),
            turso_core::Value::Blob(x) => rusqlite::types::Value::Blob(x.to_vec()),
        })
        .collect()
}

fn sqlite_row_values(row: &rusqlite::Row) -> Vec<rusqlite::types::Value> {
    let mut values = Vec::new();
    for i in 0.. {
        match row.get::<_, rusqlite::types::Value>(i) {
            Ok(value) => values.push(value),
            Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
            Err(err) => panic!("unexpected rusqlite error: {err}"),
        }
    }
    values
}

/// Compare a query's full result row-by-row in lockstep, without collecting
/// either side into a `Vec`. Only one row from each engine is held at a time,
/// so a full-table `SELECT k, b` scan stays bounded by the largest single row
/// instead of the whole table. Use this for the big content scans; the small
/// aggregate/point queries can keep using `compare_query`.
fn compare_query_streaming(
    sqlite_conn: &rusqlite::Connection,
    turso_conn: &Arc<turso_core::Connection>,
    query: &str,
    ctx: &str,
) {
    let mut sqlite_stmt = sqlite_conn.prepare(query).unwrap();
    let mut sqlite_rows = sqlite_stmt.query([]).unwrap();
    let mut row_index = 0usize;

    let mut turso_stmt = turso_conn.prepare(query).unwrap();
    turso_stmt
        .run_with_row_callback(|turso_row| {
            let turso_values = turso_row_values(turso_row);
            match sqlite_rows.next().unwrap() {
                Some(sqlite_row) => {
                    let sqlite_values = sqlite_row_values(sqlite_row);
                    if sqlite_values != turso_values {
                        panic!(
                            "{ctx}: `{query}` diverged at row {row_index}:\n  sqlite: {}\n  turso:  {}",
                            sqlite_values.iter().map(value_brief).collect::<Vec<_>>().join(", "),
                            turso_values.iter().map(value_brief).collect::<Vec<_>>().join(", "),
                        );
                    }
                }
                None => panic!(
                    "{ctx}: `{query}` diverged: turso produced row {row_index} but sqlite ended:\n  turso: {}",
                    turso_values.iter().map(value_brief).collect::<Vec<_>>().join(", "),
                ),
            }
            row_index += 1;
            Ok(())
        })
        .unwrap_or_else(|e| panic!("{ctx}: `{query}` failed on turso: {e}"));

    if let Some(sqlite_row) = sqlite_rows.next().unwrap() {
        let sqlite_values = sqlite_row_values(sqlite_row);
        panic!(
            "{ctx}: `{query}` diverged: sqlite produced row {row_index} but turso ended:\n  sqlite: {}",
            sqlite_values.iter().map(value_brief).collect::<Vec<_>>().join(", "),
        );
    }
}

/// Half the ranges are uniform over the whole keyspace (mostly wide), half
/// hug existing rowids so scans start and stop at exact keys and near-misses.
fn random_range(rng: &mut ChaCha8Rng, known: &[u64]) -> (u64, u64) {
    if known.is_empty() || rng.next_u32() % 2 == 0 {
        let mut l = rng.next_u64() % MAX_ROWID;
        let mut r = rng.next_u64() % MAX_ROWID;
        if l > r {
            (l, r) = (r, l);
        }
        (l, r)
    } else {
        let i = rng.next_u32() as usize % known.len();
        let j = (i + rng.next_u32() as usize % 16).min(known.len() - 1);
        (
            known[i].saturating_sub(1),
            known[j].saturating_add(1).min(MAX_ROWID),
        )
    }
}

/// Window over a few adjacent known rowids, so DELETE/UPDATE ranges touch a
/// handful of rows instead of the whole table.
fn known_window(rng: &mut ChaCha8Rng, known: &[u64]) -> (u64, u64) {
    let i = rng.next_u32() as usize % known.len();
    let j = (i + rng.next_u32() as usize % 8).min(known.len() - 1);
    (known[i], known[j])
}

/// `known` stays sorted and only grows; deletes may turn later windows into
/// no-ops, which is fine.
fn random_write_stmt(rng: &mut ChaCha8Rng, known: &mut Vec<u64>, page_size: usize) -> String {
    match rng.next_u32() % 4 {
        0 | 1 => {
            let k = if rng.next_u32() % 2 == 0 {
                known[rng.next_u32() as usize % known.len()]
            } else {
                rng.next_u64() % MAX_ROWID
            };
            let payload = random_payload(rng, page_size);
            if let Err(pos) = known.binary_search(&k) {
                known.insert(pos, k);
            }
            format!(
                "INSERT OR REPLACE INTO test VALUES ({k}, X'{}')",
                to_hex(&payload)
            )
        }
        2 => {
            let (a, b) = known_window(rng, known);
            format!("DELETE FROM test WHERE k BETWEEN {a} AND {b}")
        }
        _ => {
            let (a, b) = known_window(rng, known);
            let payload = random_payload(rng, page_size);
            format!(
                "UPDATE test SET b = X'{}' WHERE k BETWEEN {a} AND {b}",
                to_hex(&payload)
            )
        }
    }
}

fn run_attempt(
    rng: &mut ChaCha8Rng,
    seed: u64,
    depth: usize,
    attempt: usize,
    opts: turso_core::DatabaseOpts,
    flags: OpenFlags,
) {
    let page_size = PAGE_SIZES[rng.next_u32() as usize % PAGE_SIZES.len()];
    let ctx = format!("seed={seed} depth={depth} attempt={attempt} page_size={page_size}");
    let temp_dir = tempfile::TempDir::new().unwrap();
    let turso_path = temp_dir.path().join("btree-turso.db");
    let sqlite_path = temp_dir.path().join("btree-sqlite.db");

    // A real SQLite writer lays down the header and schema; closing the
    // connection checkpoints the WAL, leaving exactly two pages on disk.
    // The page size must be set before the first write to the fresh file.
    {
        let conn = rusqlite::Connection::open(&turso_path).unwrap();
        conn.pragma_update(None, "page_size", page_size as i64)
            .unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
        conn.execute("create table test (k INTEGER PRIMARY KEY, b BLOB)", [])
            .unwrap();
    }

    let max_interior_keys = 2 + rng.next_u32() as usize % 5;
    let mut generator = BTreeGenerator {
        rng,
        page_size,
        max_interior_keys,
        max_leaf_keys: 4096,
        blob_scale: size_scale(),
        profile: CorruptionProfile::NONE,
        corruptions: Vec::new(),
    };
    let root = generator.generate_btree(depth, 0, MAX_ROWID);
    generator.write_btree(&turso_path, &root, 2);

    let mut known = Vec::new();
    collect_rowids(&root, &mut known);
    known.sort_unstable();
    log::info!(
        "{ctx}: fanout={max_interior_keys} rows={} file={} bytes",
        known.len(),
        std::fs::metadata(&turso_path).unwrap().len(),
    );

    // Each engine gets its own identical copy of the generated file, so the
    // write phase can diverge only through engine behavior.
    std::fs::copy(&turso_path, &sqlite_path).unwrap();

    // Open turso only after the raw file is fully written.
    let db = TempDatabase::builder()
        .with_db_path(&turso_path)
        .with_opts(opts)
        .with_flags(flags)
        .build();
    let turso_conn = db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open(&sqlite_path).unwrap();

    // The generated file must look like a valid database to both engines.
    compare_query(&sqlite_conn, &turso_conn, "PRAGMA integrity_check", &ctx);

    // Read phase: full contents (walks every overflow chain), range scans in
    // both directions, and point lookups on hits and misses. The full-content
    // scan is compared streaming, so a large target size does not materialize
    // the whole table on the harness side.
    compare_query_streaming(
        &sqlite_conn,
        &turso_conn,
        "SELECT k, b FROM test ORDER BY k",
        &ctx,
    );
    for _ in 0..8 {
        let (l, r) = random_range(rng, &known);
        let query = format!("SELECT SUM(LENGTH(b)) FROM test WHERE k >= {l} AND k <= {r}");
        compare_query(&sqlite_conn, &turso_conn, &query, &ctx);
    }
    for _ in 0..3 {
        let (l, r) = random_range(rng, &known);
        for order in ["ASC", "DESC"] {
            let query = format!(
                "SELECT k, b FROM test WHERE k >= {l} AND k <= {r} ORDER BY k {order} LIMIT 32"
            );
            compare_query(&sqlite_conn, &turso_conn, &query, &ctx);
        }
    }
    for _ in 0..4 {
        let k = if rng.next_u32() % 2 == 0 && !known.is_empty() {
            known[rng.next_u32() as usize % known.len()]
        } else {
            rng.next_u64() % MAX_ROWID
        };
        let query = format!("SELECT b FROM test WHERE k = {k}");
        compare_query(&sqlite_conn, &turso_conn, &query, &ctx);
    }

    // Write phase: the same statement stream runs on both engines, churning
    // the weird pages (free-block reuse, defragmentation, overflow chains
    // created and freed, balancing around stale divider keys).
    for i in 0..24 {
        let stmt = random_write_stmt(rng, &mut known, page_size);
        log::debug!("{ctx}: write {i}: {}", &stmt[..stmt.len().min(100)]);
        let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &stmt);
        let turso_rows = limbo_exec_rows(&turso_conn, &stmt);
        assert_rows_eq(&sqlite_rows, &turso_rows, &stmt, &ctx);
        if i % 4 == 3 {
            compare_query(
                &sqlite_conn,
                &turso_conn,
                "SELECT COUNT(*), SUM(LENGTH(b)) FROM test",
                &ctx,
            );
        }
    }

    // After the writes both databases must hold identical content and each
    // must still pass its own integrity check.
    compare_query_streaming(
        &sqlite_conn,
        &turso_conn,
        "SELECT k, b FROM test ORDER BY k",
        &ctx,
    );
    for _ in 0..4 {
        let (l, r) = random_range(rng, &known);
        let query = format!("SELECT SUM(LENGTH(b)) FROM test WHERE k >= {l} AND k <= {r}");
        compare_query(&sqlite_conn, &turso_conn, &query, &ctx);
    }
    let ok = vec![vec![rusqlite::types::Value::Text("ok".to_string())]];
    let sqlite_check = sqlite_exec_rows(&sqlite_conn, "PRAGMA integrity_check");
    assert_eq!(
        sqlite_check, ok,
        "{ctx}: sqlite integrity_check after writes"
    );
    let turso_check = limbo_exec_rows(&turso_conn, "PRAGMA integrity_check");
    assert_eq!(turso_check, ok, "{ctx}: turso integrity_check after writes");
}

/// Name given to every worker thread that drives a corrupt attempt, so the
/// panic hook can tell our intentional panics apart from everyone else's.
const CORRUPT_THREAD: &str = "btree-corrupt-worker";
/// A single corrupt attempt must finish within this budget; overrunning it is
/// treated as a hang (a bug), not as a slow machine.
const CORRUPT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20);

/// Thread-local scratch where the panic hook stashes the source location of
/// the panic, keyed by thread, so the worker can read it back after
/// `catch_unwind` (the unwind payload carries the message but not the location).
fn panic_locations() -> &'static Mutex<HashMap<ThreadId, String>> {
    static MAP: OnceLock<Mutex<HashMap<ThreadId, String>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Install (once) a panic hook that records the location of panics happening
/// on our worker threads and swallows their output, while leaving panics from
/// every other thread to the previous hook. Without this, each caught panic
/// would dump a message and backtrace, drowning the final summary.
fn install_corrupt_panic_hook() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let current = thread::current();
            if current.name() == Some(CORRUPT_THREAD) {
                let location = info
                    .location()
                    .map(|l| format!("{}:{}", l.file(), l.line()))
                    .unwrap_or_else(|| "unknown".to_string());
                panic_locations()
                    .lock()
                    .unwrap()
                    .insert(current.id(), location);
            } else {
                previous(info);
            }
        }));
    });
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// What happened when we threw queries at one corrupt file.
enum AttemptOutcome {
    /// turso answered every query with rows or a clean error.
    Clean,
    /// turso panicked; a bug.
    Panicked { location: String, message: String },
    /// turso did not finish within the budget; a hang, also a bug.
    Hung,
}

/// Build one corrupt file and run a fixed battery of read queries against it.
/// Returns the number of queries turso rejected with an error (expected and
/// fine). Panics propagate to the caller's `catch_unwind`.
fn corrupt_attempt_body(
    seed: u64,
    depth: usize,
    attempt: usize,
    profile: CorruptionProfile,
    opts: turso_core::DatabaseOpts,
    flags: OpenFlags,
) {
    // Derive an independent, reproducible rng for this attempt.
    let attempt_seed = seed
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add((depth as u64) << 32)
        .wrapping_add(attempt as u64);
    let mut rng = ChaCha8Rng::seed_from_u64(attempt_seed);

    let page_size = PAGE_SIZES[rng.next_u32() as usize % PAGE_SIZES.len()];
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().join("btree-corrupt.db");

    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "page_size", page_size as i64)
            .unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
        conn.execute("create table test (k INTEGER PRIMARY KEY, b BLOB)", [])
            .unwrap();
    }

    let max_interior_keys = 2 + rng.next_u32() as usize % 5;
    let mut generator = BTreeGenerator {
        rng: &mut rng,
        page_size,
        max_interior_keys,
        max_leaf_keys: 4096,
        blob_scale: size_scale(),
        profile,
        corruptions: Vec::new(),
    };
    let root = generator.generate_btree(depth, 0, MAX_ROWID);
    generator.write_btree(&db_path, &root, 2);

    let mut known = Vec::new();
    collect_rowids(&root, &mut known);
    known.sort_unstable();

    let db = TempDatabase::builder()
        .with_db_path(&db_path)
        .with_opts(opts)
        .with_flags(flags)
        .build();
    let conn = db.connect_limbo();

    let mut queries = vec![
        "PRAGMA integrity_check".to_string(),
        "SELECT k, b FROM test ORDER BY k".to_string(),
    ];
    for _ in 0..6 {
        let (l, r) = random_range(&mut rng, &known);
        queries.push(format!(
            "SELECT SUM(LENGTH(b)) FROM test WHERE k >= {l} AND k <= {r}"
        ));
    }
    for _ in 0..2 {
        let (l, r) = random_range(&mut rng, &known);
        for order in ["ASC", "DESC"] {
            queries.push(format!(
                "SELECT k, b FROM test WHERE k >= {l} AND k <= {r} ORDER BY k {order} LIMIT 32"
            ));
        }
    }
    for _ in 0..4 {
        let k = if rng.next_u32() % 2 == 0 && !known.is_empty() {
            known[rng.next_u32() as usize % known.len()]
        } else {
            rng.next_u64() % MAX_ROWID
        };
        queries.push(format!("SELECT b FROM test WHERE k = {k}"));
    }

    // A clean error is fine; only a panic (unwinds out of here) or a hang
    // (caught by the caller's timeout) counts as a bug. Rows are streamed and
    // discarded so a large target size cannot exhaust the worker's memory.
    for query in &queries {
        let _ = run_query_discard(&conn, query);
    }
}

/// Drive a query on turso and discard its rows without collecting them, so a
/// huge result set does not blow up memory. Returns a clean SQL error as `Err`;
/// panics and hangs propagate to the caller (catch_unwind / timeout).
fn run_query_discard(
    conn: &Arc<turso_core::Connection>,
    query: &str,
) -> Result<(), turso_core::LimboError> {
    let mut stmt = conn.prepare(query)?;
    stmt.run_with_row_callback(|_row| Ok(()))
}

/// Run one corrupt attempt on a worker thread bounded by a timeout, so a hang
/// becomes an observable outcome instead of stalling the suite.
fn run_corrupt_attempt(
    seed: u64,
    depth: usize,
    attempt: usize,
    profile: CorruptionProfile,
    opts: turso_core::DatabaseOpts,
    flags: OpenFlags,
) -> AttemptOutcome {
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name(CORRUPT_THREAD.to_string())
        .spawn(move || {
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                corrupt_attempt_body(seed, depth, attempt, profile, opts, flags)
            }));
            let outcome = match result {
                Ok(()) => AttemptOutcome::Clean,
                Err(payload) => {
                    let location = panic_locations()
                        .lock()
                        .unwrap()
                        .remove(&thread::current().id())
                        .unwrap_or_else(|| "unknown".to_string());
                    AttemptOutcome::Panicked {
                        location,
                        message: panic_message(&*payload),
                    }
                }
            };
            let _ = tx.send(outcome);
        })
        .expect("spawn corrupt worker");

    // If the worker hangs we abandon it (std threads can't be killed); it dies
    // with the process. The test is going to fail and exit anyway.
    match rx.recv_timeout(CORRUPT_ATTEMPT_TIMEOUT) {
        Ok(outcome) => outcome,
        Err(_) => AttemptOutcome::Hung,
    }
}

/// A distinct bug turso exhibited on corrupt input, plus one example of the
/// attempt that triggered it so it can be reproduced.
struct CorruptBug {
    location: String,
    message: String,
    example: String,
}

fn run_corruption_test(profile_name: &str, profile: CorruptionProfile) {
    install_corrupt_panic_hook();
    let seed = std::env::var("BTREE_TEST_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);

    // Distinct bugs keyed by panic location (or "hang"); first trigger wins.
    let mut bugs: BTreeMap<String, CorruptBug> = BTreeMap::new();
    let (mut attempts, mut clean) = (0usize, 0usize);
    for depth in 0..4 {
        for attempt in 0..4 {
            attempts += 1;
            let example = format!(
                "BTREE_TEST_SEED={seed} profile={profile_name} depth={depth} attempt={attempt}"
            );
            match run_corrupt_attempt(
                seed,
                depth,
                attempt,
                profile,
                default_corrupt_opts(),
                OpenFlags::default(),
            ) {
                AttemptOutcome::Clean => clean += 1,
                AttemptOutcome::Panicked { location, message } => {
                    bugs.entry(location.clone()).or_insert(CorruptBug {
                        location,
                        message,
                        example,
                    });
                }
                AttemptOutcome::Hung => {
                    bugs.entry("hang".to_string()).or_insert(CorruptBug {
                        location: "hang".to_string(),
                        message: format!("no result within {CORRUPT_ATTEMPT_TIMEOUT:?}"),
                        example,
                    });
                }
            }
        }
    }

    if bugs.is_empty() {
        return;
    }

    let mut report = format!(
        "turso mishandled corrupt input in profile '{profile_name}': {}/{attempts} attempts clean, \
         {} distinct crash/hang site(s):\n",
        clean,
        bugs.len()
    );
    for bug in bugs.values() {
        report.push_str(&format!(
            "  - {} :: {}\n      reproduce with: {}\n",
            bug.location, bug.message, bug.example
        ));
    }
    panic!("{report}");
}

/// Options matching what the `#[turso_macros::test]` harness builds, but usable
/// off the macro (the corrupt attempts run on plain worker threads).
fn default_corrupt_opts() -> turso_core::DatabaseOpts {
    turso_core::DatabaseOpts::new()
        .with_index_method(true)
        .with_encryption(true)
        .with_attach(true)
        .with_generated_columns(true)
}

// TODO: currently fails with MVCC
#[turso_macros::test]
fn test_btree_valid(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    // Deterministic by default; set BTREE_TEST_SEED to explore other trees.
    let seed = std::env::var("BTREE_TEST_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    for depth in 0..4 {
        for attempt in 0..6 {
            run_attempt(
                &mut rng,
                seed,
                depth,
                attempt,
                tmp_db.db_opts,
                tmp_db.db_flags,
            );
        }
    }
}

// The corrupt variants run each attempt on a bounded worker thread (see
// `run_corrupt_attempt`), so they are plain `#[test]`s rather than
// `#[turso_macros::test]` and build their own databases.
#[test]
fn test_btree_corrupt_off_by_one() {
    run_corruption_test(
        "off_by_one",
        CorruptionProfile {
            off_by_one_permille: 10,
            ..CorruptionProfile::NONE
        },
    );
}

#[test]
fn test_btree_corrupt_pointers() {
    run_corruption_test(
        "pointers",
        CorruptionProfile {
            bad_pointer_permille: 10,
            ..CorruptionProfile::NONE
        },
    );
}

#[test]
fn test_btree_corrupt_garbage() {
    run_corruption_test(
        "garbage",
        CorruptionProfile {
            garbage_permille: 25,
            ..CorruptionProfile::NONE
        },
    );
}

#[test]
fn test_btree_corrupt_mixed() {
    run_corruption_test(
        "mixed",
        CorruptionProfile {
            off_by_one_permille: 5,
            bad_pointer_permille: 5,
            garbage_permille: 10,
        },
    );
}
