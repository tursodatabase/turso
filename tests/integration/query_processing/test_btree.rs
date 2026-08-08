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

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    rc::Rc,
    sync::Arc,
};

use rand::{seq::SliceRandom, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Buffer, Completion, File, OpenFlags, PlatformIO, IO};
use zerocopy::big_endian::{U16, U32, U64};

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase};

/// Page sizes the generator picks from. 65536 is excluded: the format
/// special-cases it (a 0 in two-byte header fields means 65536) and the
/// generator's two-byte offset arithmetic does not model that.
const PAGE_SIZES: &[usize] = &[512, 1024, 2048, 4096, 8192, 16384, 32768];
/// Largest rowid the generator hands out (rowids are signed in SQL).
const MAX_ROWID: u64 = i64::MAX as u64;

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

#[derive(Debug)]
pub struct BTreeLeafCell {
    size: usize,
    rowid: u64,
    on_page_data: Vec<u8>,
    overflow_page: Option<Rc<BTreePageData>>,
}
#[derive(Debug)]
pub struct BTreeInteriorCell {
    left_child_pointer: Rc<BTreePageData>,
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
                    + cell.overflow_page.as_ref().map(|_| 4).unwrap_or(0)) as u16
            }
        }
    }
}

#[derive(Debug)]
pub struct BTreeOverflowPageData {
    next: Option<Rc<BTreePageData>>,
    payload: Vec<u8>,
}

#[derive(Debug)]
pub struct BTreeTablePageData {
    page_type: BTreePageType,
    cell_content_area: u16,
    cell_right_pointer: Option<Rc<BTreePageData>>,
    fragmented_free_bytes: u8,
    cells: Vec<(u16, BTreeCell)>,
    free_blocks: Vec<BTreeFreeBlock>,
}

#[derive(Debug)]
pub enum BTreePageData {
    Table(BTreeTablePageData),
    Overflow(BTreeOverflowPageData),
}

pub fn list_pages(root: &Rc<BTreePageData>, pages: &mut Vec<Rc<BTreePageData>>) {
    pages.push(root.clone());
    match root.as_ref() {
        BTreePageData::Table(root) => {
            for (_, cell) in &root.cells {
                match cell {
                    BTreeCell::Interior(cell) => list_pages(&cell.left_child_pointer, pages),
                    BTreeCell::Leaf(cell) => {
                        let Some(overflow_page) = &cell.overflow_page else {
                            continue;
                        };
                        list_pages(overflow_page, pages);
                    }
                }
            }
            if let Some(right) = &root.cell_right_pointer {
                list_pages(right, pages);
            }
        }
        BTreePageData::Overflow(root) => {
            if let Some(next) = &root.next {
                list_pages(next, pages);
            }
        }
    }
}

fn collect_rowids(root: &Rc<BTreePageData>, rowids: &mut Vec<u64>) {
    if let BTreePageData::Table(page) = root.as_ref() {
        for (_, cell) in &page.cells {
            match cell {
                BTreeCell::Interior(cell) => collect_rowids(&cell.left_child_pointer, rowids),
                BTreeCell::Leaf(cell) => rowids.push(cell.rowid),
            }
        }
        if let Some(right) = &page.cell_right_pointer {
            collect_rowids(right, rowids);
        }
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

fn write_blob_column(header: &mut Vec<u8>, data: &mut Vec<u8>, value: &[u8]) {
    let mut buf = [0u8; 10];
    let buf_len = write_varint(&mut buf, (value.len() * 2 + 12) as u64);
    header.extend_from_slice(&buf[0..buf_len]);
    data.extend_from_slice(value);
}

fn create_simple_record(value: u64, payload: &[u8], key_as_null: bool) -> Vec<u8> {
    let mut header = Vec::new();
    let mut data = Vec::new();
    if key_as_null {
        // SQLite itself stores NULL for an INTEGER PRIMARY KEY column and
        // substitutes the rowid on read. Storing the integer instead (the
        // else-branch) is a layout SQLite tolerates but never produces.
        header.push(0);
    } else {
        write_u64_column(&mut header, &mut data, value);
    }
    write_blob_column(&mut header, &mut data, payload);
    let header_len = header.len() + 1;
    assert!(header_len <= 127);
    let mut buf = [0u8; 10];
    let buf_len = write_varint(&mut buf, header_len as u64);
    let mut result = buf[0..buf_len].to_vec();
    result.extend_from_slice(&header);
    result.extend_from_slice(&data);
    result
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

fn build_overflow_chain(payload: &[u8], page_size: usize) -> Rc<BTreePageData> {
    let mut next = None;
    for chunk in payload.chunks(page_size - 4).rev() {
        next = Some(Rc::new(BTreePageData::Overflow(BTreeOverflowPageData {
            next,
            payload: chunk.to_vec(),
        })));
    }
    next.expect("overflow chain needs at least one page")
}

/// Payload sizes come in three classes: small (most rows), medium (fills
/// pages with just a few cells, still mostly in-page) and large (straddles
/// the in-page threshold, so most of these records need overflow chains).
fn random_payload(rng: &mut ChaCha8Rng, page_size: usize) -> Vec<u8> {
    let max_local = leaf_max_local(page_size);
    let class = rng.next_u32() % 100;
    let len = if class < 60 {
        rng.next_u32() as usize % 128
    } else if class < 85 {
        128 + rng.next_u32() as usize % (max_local - 128)
    } else {
        max_local.saturating_sub(page_size / 8) + rng.next_u32() as usize % (page_size * 3)
    };
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

    pub fn create_page(
        &mut self,
        page_no: u32,
        max_page: u32,
        page: &BTreePageData,
        page_numbers: &HashMap<*const BTreePageData, u32>,
    ) -> Vec<u8> {
        match page {
            BTreePageData::Table(page) => {
                self.create_btree_page(page_no, max_page, page, page_numbers)
            }
            BTreePageData::Overflow(page) => {
                self.create_overflow_page(page_no, max_page, page, page_numbers)
            }
        }
    }
    pub fn create_overflow_page(
        &mut self,
        page_no: u32,
        max_page: u32,
        page: &BTreeOverflowPageData,
        page_numbers: &HashMap<*const BTreePageData, u32>,
    ) -> Vec<u8> {
        let mut data = vec![255u8; self.page_size];
        let first_4bytes = if let Some(next) = &page.next {
            *page_numbers.get(&Rc::as_ptr(next)).unwrap()
        } else {
            0
        };
        let first_4bytes =
            self.corrupt_page_pointer(page_no, "overflow next pointer", first_4bytes, max_page);
        data[0..4].copy_from_slice(&U32::new(first_4bytes).to_bytes());
        data[4..4 + page.payload.len()].copy_from_slice(&page.payload);
        self.corrupt_garbage_region(page_no, &mut data);
        data
    }
    pub fn create_btree_page(
        &mut self,
        page_no: u32,
        max_page: u32,
        page: &BTreeTablePageData,
        page_numbers: &HashMap<*const BTreePageData, u32>,
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
                    if let Some(overflow_page) = &cell.overflow_page {
                        let overflow_page = Rc::as_ptr(overflow_page);
                        let overflow_page = *page_numbers.get(&overflow_page).unwrap();
                        let overflow_page = self.corrupt_page_pointer(
                            page_no,
                            "overflow pointer",
                            overflow_page,
                            max_page,
                        );
                        data[p..p + 4].copy_from_slice(&U32::new(overflow_page).to_bytes());
                    }
                }
            }
        }

        self.corrupt_garbage_region(page_no, &mut data);
        data
    }

    fn generate_btree(&mut self, depth: usize, mut l: u64, r: u64) -> Rc<BTreePageData> {
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
                let payload = random_payload(self.rng, self.page_size);
                let key_as_null = self.rng.next_u32() % 2 == 0;
                let record = create_simple_record(rowid, &payload, key_as_null);
                let total_size = record.len();
                let local_size = leaf_local_size(total_size, self.page_size);
                let (on_page_data, overflow_page) = if local_size < total_size {
                    let chain = build_overflow_chain(&record[local_size..], self.page_size);
                    (record[..local_size].to_vec(), Some(chain))
                } else {
                    (record, None)
                };
                BTreeCell::Leaf(BTreeLeafCell {
                    size: total_size,
                    rowid,
                    on_page_data,
                    overflow_page,
                })
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
            Rc::new(BTreePageData::Table(BTreeTablePageData {
                page_type: BTreePageType::Leaf,
                cell_content_area: content_offset,
                cell_right_pointer: None,
                fragmented_free_bytes: fragmented_free_bytes as u8,
                cells,
                free_blocks,
            }))
        } else {
            Rc::new(BTreePageData::Table(BTreeTablePageData {
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
            }))
        }
    }

    fn write_btree(&mut self, path: &Path, root: &Rc<BTreePageData>, start_page: u32) {
        let mut pages = Vec::new();
        list_pages(root, &mut pages);
        pages[1..].shuffle(&mut self.rng);
        let mut page_numbers = HashMap::default();
        for (page, page_no) in pages.iter().zip(start_page..) {
            page_numbers.insert(Rc::as_ptr(page), page_no);
        }

        let io = PlatformIO::new().unwrap();
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::None, true)
            .unwrap();

        assert_eq!(file.size().unwrap(), (self.page_size * 2) as u64);
        let max_page = (1 + pages.len()) as u32;
        for (i, page) in pages.iter().enumerate() {
            let page = self.create_page(start_page + i as u32, max_page, page, &page_numbers);
            write_at(&io, file.as_ref(), self.page_size * (i + 1), &page);
        }
        let mut size = (1 + pages.len()) as u32;
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
    // both directions, and point lookups on hits and misses.
    compare_query(
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
    compare_query(
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

/// Corrupt-mode attempt: generate a tree, corrupt it while serializing, then
/// throw read queries at turso. Every query may return rows or an error; the
/// only failure is a panic, crash or hang inside turso.
fn run_corrupt_attempt(
    rng: &mut ChaCha8Rng,
    seed: u64,
    depth: usize,
    attempt: usize,
    profile: CorruptionProfile,
    opts: turso_core::DatabaseOpts,
    flags: OpenFlags,
) {
    let page_size = PAGE_SIZES[rng.next_u32() as usize % PAGE_SIZES.len()];
    let ctx = format!("seed={seed} depth={depth} attempt={attempt} page_size={page_size}");
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
        rng,
        page_size,
        max_interior_keys,
        max_leaf_keys: 4096,
        profile,
        corruptions: Vec::new(),
    };
    let root = generator.generate_btree(depth, 0, MAX_ROWID);
    generator.write_btree(&db_path, &root, 2);
    let corruptions = std::mem::take(&mut generator.corruptions);

    let mut known = Vec::new();
    collect_rowids(&root, &mut known);
    known.sort_unstable();

    for corruption in &corruptions {
        log::debug!("{ctx}: corrupted {corruption}");
    }

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
        let (l, r) = random_range(rng, &known);
        queries.push(format!(
            "SELECT SUM(LENGTH(b)) FROM test WHERE k >= {l} AND k <= {r}"
        ));
    }
    for _ in 0..2 {
        let (l, r) = random_range(rng, &known);
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

    let (mut ok_count, mut err_count) = (0, 0);
    for query in &queries {
        match limbo_exec_rows_fallible(&db, &conn, query) {
            Ok(_) => ok_count += 1,
            Err(err) => {
                err_count += 1;
                log::debug!("{ctx}: `{query}` -> {err}");
            }
        }
    }
    log::info!(
        "{ctx}: {} corruptions, {ok_count} queries ok, {err_count} queries failed",
        corruptions.len()
    );
}

fn run_corruption_test(tmp_db: TempDatabase, profile: CorruptionProfile) {
    let _ = env_logger::try_init();
    let seed = std::env::var("BTREE_TEST_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    for depth in 0..4 {
        for attempt in 0..4 {
            run_corrupt_attempt(
                &mut rng,
                seed,
                depth,
                attempt,
                profile,
                tmp_db.db_opts,
                tmp_db.db_flags,
            );
        }
    }
}

// TODO: currently fails with MVCC
#[turso_macros::test]
fn test_btree(tmp_db: TempDatabase) {
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

#[turso_macros::test]
fn test_btree_corrupt_off_by_one(tmp_db: TempDatabase) {
    run_corruption_test(
        tmp_db,
        CorruptionProfile {
            off_by_one_permille: 10,
            ..CorruptionProfile::NONE
        },
    );
}

#[turso_macros::test]
fn test_btree_corrupt_pointers(tmp_db: TempDatabase) {
    run_corruption_test(
        tmp_db,
        CorruptionProfile {
            bad_pointer_permille: 10,
            ..CorruptionProfile::NONE
        },
    );
}

#[turso_macros::test]
fn test_btree_corrupt_garbage(tmp_db: TempDatabase) {
    run_corruption_test(
        tmp_db,
        CorruptionProfile {
            garbage_permille: 25,
            ..CorruptionProfile::NONE
        },
    );
}

#[turso_macros::test]
fn test_btree_corrupt_mixed(tmp_db: TempDatabase) {
    run_corruption_test(
        tmp_db,
        CorruptionProfile {
            off_by_one_permille: 5,
            bad_pointer_permille: 5,
            garbage_permille: 10,
        },
    );
}
