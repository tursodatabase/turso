use std::{
    collections::{HashMap, HashSet},
    i64,
    os::unix::fs::FileExt,
    path::Path,
    rc::Rc,
};

use rand::{seq::SliceRandom, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use zerocopy::big_endian::{U16, U32, U64};

use crate::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};

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
    #[allow(dead_code)]
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
                        list_pages(&overflow_page, pages);
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

pub fn length_varint(value: u64) -> usize {
    let mut buf = [0u8; 10];
    write_varint(&mut buf, value)
}

fn write_u64_column(header: &mut Vec<u8>, data: &mut Vec<u8>, value: u64) {
    let mut buf = [0u8; 10];
    let buf_len = write_varint(&mut buf, 6 as u64);
    header.extend_from_slice(&buf[0..buf_len]);
    data.extend_from_slice(&U64::new(value).to_bytes());
}

fn write_blob_column(header: &mut Vec<u8>, data: &mut Vec<u8>, value: &[u8]) {
    let mut buf = [0u8; 10];
    let buf_len = write_varint(&mut buf, (value.len() * 2 + 12) as u64);
    header.extend_from_slice(&buf[0..buf_len]);
    data.extend_from_slice(value);
}

fn create_simple_record(value: u64, payload: &[u8]) -> Vec<u8> {
    let mut header = Vec::new();
    let mut data = Vec::new();
    write_u64_column(&mut header, &mut data, value);
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

struct BTreeGenerator<'a> {
    rng: &'a mut ChaCha8Rng,
    max_interior_keys: usize,
    max_leaf_keys: usize,
    max_payload_size: usize,
}

impl<'a> BTreeGenerator<'a> {
    pub fn create_page(
        &self,
        page: &BTreePageData,
        page_numbers: &HashMap<*const BTreePageData, u32>,
    ) -> Vec<u8> {
        match page {
            BTreePageData::Table(page) => self.create_btree_page(page, page_numbers),
            BTreePageData::Overflow(page) => self.create_overflow_page(page, page_numbers),
        }
    }
    pub fn create_overflow_page(
        &self,
        page: &BTreeOverflowPageData,
        page_numbers: &HashMap<*const BTreePageData, u32>,
    ) -> Vec<u8> {
        let mut data = [255u8; 4096];
        let first_4bytes = if let Some(next) = &page.next {
            *page_numbers.get(&Rc::as_ptr(&next)).unwrap()
        } else {
            0
        };
        data[0..4].copy_from_slice(&U32::new(first_4bytes).to_bytes());
        data[4..4 + page.payload.len()].copy_from_slice(&page.payload);
        data.to_vec()
    }
    pub fn create_btree_page(
        &self,
        page: &BTreeTablePageData,
        page_numbers: &HashMap<*const BTreePageData, u32>,
    ) -> Vec<u8> {
        let mut data = [255u8; 4096];

        data[0] = match page.page_type {
            BTreePageType::Interior => 0x05,
            BTreePageType::Leaf => 0x0d,
        };
        data[1..3].copy_from_slice(
            &U16::new(page.free_blocks.get(0).map(|x| x.offset).unwrap_or(0)).to_bytes(),
        );
        data[3..5].copy_from_slice(&U16::new(page.cells.len() as u16).to_bytes());
        data[5..7].copy_from_slice(&U16::new(page.cell_content_area).to_bytes());
        data[7] = page.fragmented_free_bytes;
        let mut offset = 8;
        if page.page_type == BTreePageType::Interior {
            let cell_right_pointer = page.cell_right_pointer.as_ref().unwrap();
            let cell_right_pointer = Rc::as_ptr(&cell_right_pointer);
            let cell_right_pointer = page_numbers.get(&cell_right_pointer).unwrap();
            data[8..12].copy_from_slice(&U32::new(*cell_right_pointer).to_bytes());
            offset = 12;
        }

        for (i, (pointer, _)) in page.cells.iter().enumerate() {
            data[offset + 2 * i..offset + 2 * (i + 1)]
                .copy_from_slice(&U16::new(*pointer).to_bytes());
        }

        for i in 0..page.free_blocks.len() {
            let offset = page.free_blocks[i].offset as usize;
            data[offset + 0..offset + 2].copy_from_slice(
                &U16::new(page.free_blocks.get(i + 1).map(|x| x.offset).unwrap_or(0)).to_bytes(),
            );
            data[offset + 2..offset + 4]
                .copy_from_slice(&U16::new(page.free_blocks[i].size).to_bytes());
        }

        for (pointer, cell) in page.cells.iter() {
            let mut p = *pointer as usize;
            match cell {
                BTreeCell::Interior(cell) => {
                    let left_child_pointer = Rc::as_ptr(&cell.left_child_pointer);
                    let left_child_pointer = page_numbers.get(&left_child_pointer).unwrap();
                    data[p..p + 4].copy_from_slice(&U32::new(*left_child_pointer).to_bytes());
                    p += 4;
                    _ = write_varint(&mut data[p..], cell.rowid);
                }
                BTreeCell::Leaf(cell) => {
                    p += write_varint(&mut data[p..], cell.size as u64);
                    p += write_varint(&mut data[p..], cell.rowid as u64);
                    data[p..p + cell.on_page_data.len()].copy_from_slice(&cell.on_page_data);
                    p += cell.on_page_data.len();
                    if let Some(overflow_page) = &cell.overflow_page {
                        let overflow_page = Rc::as_ptr(&overflow_page);
                        let overflow_page = page_numbers.get(&overflow_page).unwrap();
                        data[p..p + 4].copy_from_slice(&U32::new(*overflow_page).to_bytes());
                    }
                }
            }
        }

        data.into()
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
                let length = self.rng.next_u32() as usize % self.max_payload_size;
                let record = create_simple_record(rowid, &vec![1u8; length]);
                BTreeCell::Leaf(BTreeLeafCell {
                    size: record.len(),
                    rowid,
                    on_page_data: record,
                    overflow_page: None,
                })
            } else {
                BTreeCell::Interior(BTreeInteriorCell {
                    left_child_pointer: self.generate_btree(depth - 1, l, rowid),
                    rowid,
                })
            };
            if cells_size + 2 + cell.size() > 4096 {
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
        let mut fragmentation_budget = 4096 - cells_size;
        let mut pointer_offset = header_offset;
        let mut content_offset = 4096;
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
        list_pages(&root, &mut pages);
        pages[1..].shuffle(&mut self.rng);
        let mut page_numbers = HashMap::new();
        for (page, page_no) in pages.iter().zip(start_page..) {
            page_numbers.insert(Rc::as_ptr(&page), page_no);
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();

        assert_eq!(file.metadata().unwrap().len(), 4096 * 2);
        for (i, page) in pages.iter().enumerate() {
            let page = self.create_page(page, &page_numbers);
            file.write_at(&page, 4096 * (i + 1) as u64).unwrap();
        }
        let size = 1 + pages.len();
        let size_bytes = U32::new(size as u32).to_bytes();
        file.write_at(&size_bytes, 28).unwrap();
    }
}

#[test]
fn test_btree() {
    let _ = env_logger::try_init();
    let mut rng = ChaCha8Rng::seed_from_u64(0);
    for depth in 0..4 {
        for attempt in 0..16 {
            let db = TempDatabase::new_with_rusqlite(
                "create table test (k INTEGER PRIMARY KEY, b BLOB);",
                false,
            );
            log::info!(
                "depth: {}, attempt: {}, path: {:?}",
                depth,
                attempt,
                db.path
            );

            let mut generator = BTreeGenerator {
                rng: &mut rng,
                max_interior_keys: 3,
                max_leaf_keys: 4096,
                max_payload_size: 100,
            };
            let root = generator.generate_btree(depth, 0, i64::MAX as u64);
            generator.write_btree(&db.path, &root, 2);

            for _ in 0..16 {
                let mut l = rng.next_u64() % (i64::MAX as u64);
                let mut r = rng.next_u64() % (i64::MAX as u64);
                if l > r {
                    (l, r) = (r, l);
                }

                let query = format!("SELECT SUM(LENGTH(b)) FROM test WHERE k >= {l} AND k <= {r}");
                let sqlite_sum = {
                    let conn = rusqlite::Connection::open(&db.path).unwrap();
                    sqlite_exec_rows(&conn, &query)
                };
                let limbo_sum = {
                    let conn = db.connect_limbo();
                    limbo_exec_rows(&db, &conn, &query)
                };
                assert_eq!(
                    limbo_sum, sqlite_sum,
                    "query={}, limbo={:?}, sqlite={:?}",
                    query, limbo_sum, sqlite_sum
                );
            }
        }
    }
}
