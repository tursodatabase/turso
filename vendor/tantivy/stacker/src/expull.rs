use std::mem;

use common::serialize_vint_u32;

use crate::fastcpy::fast_short_slice_copy;
use crate::{Addr, MemoryArena};

const FIRST_BLOCK_NUM: u32 = 2;

/// An exponential unrolled link.
///
/// The use case is as follows. Tantivy's indexer conceptually acts like a
/// `HashMap<Term, Vec<u32>>`. As we come across a given term in document
/// `D`, we lookup the term in the map and append the document id to its vector.
///
/// The vector is then only read when it is serialized.
///
/// The `ExpUnrolledLinkedList` offers a more efficient solution to this
/// problem.
///
/// It combines the idea of the unrolled linked list and tries to address the
/// problem of selecting an adequate block size using a strategy similar to
/// that of the `Vec` amortized resize strategy.
///
/// Data is stored in a linked list of blocks. The first block has a size of `8`
/// and each block has a length of twice that of the previous block up to
/// `MAX_BLOCK_LEN = 1<<15`.
///
/// This strategy is a good trade off to handle numerous very rare terms
/// and avoid wasting half of the memory for very frequent terms.
#[derive(Debug, Clone, Copy)]
pub struct ExpUnrolledLinkedList {
    // u16, since the max size of each block is (1<<next_cap_pow_2)
    // Limited to 15, so we don't overflow remaining_cap.
    remaining_cap: u16,
    // Tracks the number of blocks allocated: block_num - FIRST_BLOCK_NUM
    block_num: u32,
    head: Addr,
    tail: Addr,
}

impl Default for ExpUnrolledLinkedList {
    fn default() -> Self {
        Self {
            // 0 to trigger an initial allocation. Init with MemoryArena would be better.
            remaining_cap: 0,
            block_num: FIRST_BLOCK_NUM,
            head: Addr::null_pointer(),
            tail: Addr::null_pointer(),
        }
    }
}

pub struct ExpUnrolledLinkedListWriter<'a> {
    eull: &'a mut ExpUnrolledLinkedList,
    arena: &'a mut MemoryArena,
}

#[inline]
fn ensure_capacity<'a>(
    eull: &'a mut ExpUnrolledLinkedList,
    arena: &'a mut MemoryArena,
    allocate: u32,
) {
    let new_block_addr: Addr = arena.allocate_space(allocate as usize + mem::size_of::<Addr>());
    // Check first write
    if eull.head.is_null() {
        eull.head = new_block_addr;
    } else {
        arena.write_at(eull.tail, new_block_addr);
    }

    eull.tail = new_block_addr;
    eull.remaining_cap = allocate as u16;
}

impl ExpUnrolledLinkedListWriter<'_> {
    #[inline]
    pub fn write_u32_vint(&mut self, val: u32) {
        let mut buf = [0u8; 8];
        let data = serialize_vint_u32(val, &mut buf);
        self.extend_from_slice(data);
    }

    #[inline]
    pub fn extend_from_slice(&mut self, mut buf: &[u8]) {
        while !buf.is_empty() {
            let add_len: usize;
            {
                if self.eull.remaining_cap == 0 {
                    // Double the next cap
                    self.eull.increment_num_blocks();
                    let block_size = get_block_size(self.eull.block_num);
                    ensure_capacity(self.eull, self.arena, block_size as u32);
                }

                let output_buf = self
                    .arena
                    .slice_mut(self.eull.tail, self.eull.remaining_cap as usize);
                add_len = buf.len().min(output_buf.len());
                let output_buf = &mut output_buf[..add_len];
                let buf = &buf[..add_len];

                fast_short_slice_copy(buf, output_buf);
            }
            self.eull.remaining_cap -= add_len as u16;
            self.eull.tail = self.eull.tail.offset(add_len as u32);
            buf = &buf[add_len..];
        }
    }
}

// The block size is 2^block_num, but max 2^15 = 32KB
// Initial size is 8 bytes (2^3), for the first block => block_num == 2
// Block size caps at 32KB (2^15) regardless of how high block_num goes
#[inline]
fn get_block_size(block_num: u32) -> u16 {
    // Cap at 15 to prevent block sizes > 32KB
    // block_num can now be much larger than 15, but block size maxes out
    let exp: u32 = block_num.min(15u32);
    (1u32 << exp) as u16
}

impl ExpUnrolledLinkedList {
    #[inline(always)]
    pub fn increment_num_blocks(&mut self) {
        // Add overflow check as a safety measure
        // With u32, we can handle up to ~4 billion blocks before overflow
        // At 32KB per block (max size), that's 128 TB of data
        self.block_num = self
            .block_num
            .checked_add(1)
            .expect("ExpUnrolledLinkedList block count overflow - exceeded 4 billion blocks");
    }

    #[inline]
    pub fn writer<'a>(&'a mut self, arena: &'a mut MemoryArena) -> ExpUnrolledLinkedListWriter<'a> {
        ExpUnrolledLinkedListWriter { eull: self, arena }
    }

    pub fn read_to_end(&self, arena: &MemoryArena, output: &mut Vec<u8>) {
        let mut addr = self.head;
        if addr.is_null() {
            return;
        }

        // Calculate last block length with bounds checking to prevent underflow
        let block_size = get_block_size(self.block_num) as usize;
        let last_block_len = block_size.saturating_sub(self.remaining_cap as usize);

        // Safety check: if remaining_cap > block_size, the metadata is corrupted
        assert!(
            self.remaining_cap as usize <= block_size,
            "ExpUnrolledLinkedList metadata corruption detected: remaining_cap ({}) > block_size \
             ({}). This indicates a serious bug, please report! (block_num={}, head={:?}, \
             tail={:?})",
            self.remaining_cap,
            block_size,
            self.block_num,
            self.head,
            self.tail
        );

        // Full Blocks (iterate through all blocks except the last one)
        // Note: Blocks are numbered starting from FIRST_BLOCK_NUM+1 (=3) after first allocation
        for block_num in FIRST_BLOCK_NUM + 1..self.block_num {
            let cap = get_block_size(block_num) as usize;
            let data = arena.slice(addr, cap);
            output.extend_from_slice(data);
            addr = arena.read(addr.offset(cap as u32));
        }
        // Last Block
        let data = arena.slice(addr, last_block_len);
        output.extend_from_slice(data);
    }
}

#[cfg(test)]
mod tests {
    use common::{read_u32_vint, write_u32_vint};

    use super::*;

    #[test]
    fn test_eull_empty() {
        let arena = MemoryArena::default();
        let stack = ExpUnrolledLinkedList::default();
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[] as &[u8]);
        }
    }

    #[test]
    fn test_eull1() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        stack.writer(&mut arena).extend_from_slice(&[1u8]);
        stack.writer(&mut arena).extend_from_slice(&[2u8]);
        stack.writer(&mut arena).extend_from_slice(&[3u8, 4u8]);
        stack.writer(&mut arena).extend_from_slice(&[5u8]);
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[1u8, 2u8, 3u8, 4u8, 5u8]);
        }
    }

    #[test]
    fn test_eull_vint1() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        stack.writer(&mut arena).extend_from_slice(&[1u8]);
        stack.writer(&mut arena).extend_from_slice(&[2u8]);
        stack.writer(&mut arena).extend_from_slice(&[3u8, 4u8]);
        stack.writer(&mut arena).extend_from_slice(&[5u8]);
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[1u8, 2u8, 3u8, 4u8, 5u8]);
        }
    }

    #[test]
    fn test_eull_first_write_extends_cap() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        stack
            .writer(&mut arena)
            .extend_from_slice(&[1u8, 2, 3, 4, 5, 6, 7, 8, 9]);
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[1u8, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
    }

    #[test]
    fn test_eull_long() {
        let mut arena = MemoryArena::default();
        let mut eull = ExpUnrolledLinkedList::default();
        let data: Vec<u32> = (0..100).collect();
        for &el in &data {
            eull.writer(&mut arena).write_u32_vint(el);
        }
        let mut buffer = Vec::new();
        eull.read_to_end(&arena, &mut buffer);
        let mut result = vec![];
        let mut remaining = &buffer[..];
        while !remaining.is_empty() {
            result.push(read_u32_vint(&mut remaining));
        }
        assert_eq!(&result[..], &data[..]);
    }

    #[test]
    fn test_eull_limit() {
        let mut eull = ExpUnrolledLinkedList::default();
        for _ in 0..100 {
            eull.increment_num_blocks();
        }
        assert_eq!(get_block_size(eull.block_num), 1 << 15);
    }

    #[test]
    fn test_eull_interlaced() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        let mut stack2 = ExpUnrolledLinkedList::default();

        let mut vec1: Vec<u8> = vec![];
        let mut vec2: Vec<u8> = vec![];

        for i in 0..9 {
            stack.writer(&mut arena).write_u32_vint(i);
            assert!(write_u32_vint(i, &mut vec1).is_ok());
            if i % 2 == 0 {
                stack2.writer(&mut arena).write_u32_vint(i);
                assert!(write_u32_vint(i, &mut vec2).is_ok());
            }
        }
        let mut res1 = vec![];
        let mut res2 = vec![];
        stack.read_to_end(&arena, &mut res1);
        stack2.read_to_end(&arena, &mut res2);
        assert_eq!(&vec1[..], &res1[..]);
        assert_eq!(&vec2[..], &res2[..]);
    }

    // Tests for u32 block_num fix (issue with large arrays)

    #[test]
    fn test_block_num_exceeds_u16_max() {
        // Test that we can handle more than 65,535 blocks (old u16 limit)
        let mut eull = ExpUnrolledLinkedList::default();

        // Simulate allocating 70,000 blocks (exceeds u16::MAX of 65,535)
        for _ in 0..70_000 {
            eull.increment_num_blocks();
        }

        // Verify block_num is correct
        assert_eq!(eull.block_num, FIRST_BLOCK_NUM + 70_000);

        // Verify we can still get block size (should be capped at 32KB)
        let block_size = get_block_size(eull.block_num);
        assert_eq!(block_size, 1 << 15); // 32KB max
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn test_large_dataset_simulation() {
        // Simulate the scenario: large arrays requiring many blocks
        // We write enough data to require thousands of blocks
        let mut arena = MemoryArena::default();
        let mut eull = ExpUnrolledLinkedList::default();

        // Write 100 MB of data (this will require ~3,200 blocks at 32KB each)
        // This is enough to validate the system works with large datasets
        // but not so much that the test is slow
        let bytes_per_write = 10_000;
        let num_writes = 10_000; // 10k * 10k = 100 MB

        let data: Vec<u8> = (0..bytes_per_write).map(|i| (i % 256) as u8).collect();
        for _ in 0..num_writes {
            eull.writer(&mut arena).extend_from_slice(&data);
        }

        // Verify we allocated many blocks (should be in the thousands)
        assert!(
            eull.block_num > 1000,
            "block_num ({}) should be > 1000 for this much data",
            eull.block_num
        );

        // Verify we can read back correctly
        let mut buffer = Vec::new();
        eull.read_to_end(&arena, &mut buffer);
        assert_eq!(buffer.len(), bytes_per_write * num_writes);

        // Verify data integrity on a sample
        for i in 0..bytes_per_write {
            assert_eq!(buffer[i], (i % 256) as u8);
        }
    }

    #[test]
    fn test_get_block_size_with_large_block_num() {
        // Test that get_block_size handles large u32 values correctly

        // Small block numbers (under 15)
        assert_eq!(get_block_size(2), 4); // 2^2 = 4
        assert_eq!(get_block_size(3), 8); // 2^3 = 8
        assert_eq!(get_block_size(10), 1024); // 2^10 = 1KB

        // At the cap (15)
        assert_eq!(get_block_size(15), 32768); // 2^15 = 32KB

        // Beyond the cap (should stay at 32KB)
        assert_eq!(get_block_size(16), 32768);
        assert_eq!(get_block_size(100), 32768);
        assert_eq!(get_block_size(65_536), 32768); // Old u16::MAX + 1
        assert_eq!(get_block_size(100_000), 32768);
        assert_eq!(get_block_size(1_000_000), 32768);
    }

    #[test]
    fn test_increment_blocks_near_u16_boundary() {
        // Test incrementing around the old u16::MAX boundary
        let mut eull = ExpUnrolledLinkedList::default();

        // Set to just before old limit
        for _ in 0..65_533 {
            eull.increment_num_blocks();
        }
        assert_eq!(eull.block_num, FIRST_BLOCK_NUM + 65_533);

        // Cross the old u16::MAX boundary (this would have overflowed before)
        eull.increment_num_blocks(); // 65,534
        eull.increment_num_blocks(); // 65,535 (old max)
        eull.increment_num_blocks(); // 65,536 (would overflow u16)
        eull.increment_num_blocks(); // 65,537

        // Verify we're past the old limit
        assert_eq!(eull.block_num, FIRST_BLOCK_NUM + 65_537);
    }

    #[test]
    fn test_write_and_read_with_many_blocks() {
        // Test that write/read works correctly with many blocks
        let mut arena = MemoryArena::default();
        let mut eull = ExpUnrolledLinkedList::default();

        // Write data that will span many blocks
        let test_data: Vec<u8> = (0..50_000).map(|i| (i % 256) as u8).collect();
        eull.writer(&mut arena).extend_from_slice(&test_data);

        // Read it back
        let mut buffer = Vec::new();
        eull.read_to_end(&arena, &mut buffer);

        // Verify data integrity
        assert_eq!(buffer.len(), test_data.len());
        assert_eq!(&buffer[..], &test_data[..]);
    }

    #[test]
    fn test_multiple_eull_with_large_block_counts() {
        // Test multiple ExpUnrolledLinkedLists with high block counts
        // (simulates parallel columnar writes)
        let mut arena = MemoryArena::default();
        let mut eull1 = ExpUnrolledLinkedList::default();
        let mut eull2 = ExpUnrolledLinkedList::default();

        // Write different data to each
        for i in 0..10_000u32 {
            eull1.writer(&mut arena).write_u32_vint(i);
            eull2.writer(&mut arena).write_u32_vint(i * 2);
        }

        // Read back and verify
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        eull1.read_to_end(&arena, &mut buf1);
        eull2.read_to_end(&arena, &mut buf2);

        // Deserialize and check
        let mut cursor1 = &buf1[..];
        let mut cursor2 = &buf2[..];
        for i in 0..10_000u32 {
            assert_eq!(read_u32_vint(&mut cursor1), i);
            assert_eq!(read_u32_vint(&mut cursor2), i * 2);
        }
    }

    #[test]
    fn test_block_size_stays_capped() {
        // Verify that even with massive block numbers, size stays at 32KB
        let mut eull = ExpUnrolledLinkedList::default();

        // Increment to a very large number
        for _ in 0..200_000 {
            eull.increment_num_blocks();
        }

        let block_size = get_block_size(eull.block_num);
        assert_eq!(block_size, 32768, "Block size should be capped at 32KB");
    }

    #[test]
    #[should_panic(expected = "ExpUnrolledLinkedList block count overflow")]
    fn test_increment_overflow_protection() {
        // Test that we panic gracefully if we somehow hit u32::MAX
        // This is extremely unlikely in practice (would require 128TB of data)
        let mut eull = ExpUnrolledLinkedList {
            block_num: u32::MAX,
            ..Default::default()
        };

        // This should panic with our custom error message
        eull.increment_num_blocks();
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use std::iter;

    use test::Bencher;

    use super::super::MemoryArena;
    use super::ExpUnrolledLinkedList;

    const NUM_STACK: usize = 10_000;
    const STACK_SIZE: u32 = 1000;

    #[bench]
    fn bench_push_vec(bench: &mut Bencher) {
        bench.iter(|| {
            let mut vecs = Vec::with_capacity(100);
            for _ in 0..NUM_STACK {
                vecs.push(Vec::new());
            }
            for s in 0..NUM_STACK {
                for i in 0u32..STACK_SIZE {
                    let t = s * 392017 % NUM_STACK;
                    vecs[t].push(i);
                }
            }
        });
    }

    #[bench]
    fn bench_push_stack(bench: &mut Bencher) {
        bench.iter(|| {
            let mut arena = MemoryArena::default();
            let mut stacks: Vec<ExpUnrolledLinkedList> =
                iter::repeat_with(ExpUnrolledLinkedList::default)
                    .take(NUM_STACK)
                    .collect();
            for s in 0..NUM_STACK {
                for i in 0u32..STACK_SIZE {
                    let t = s * 392017 % NUM_STACK;
                    stacks[t]
                        .writer(&mut arena)
                        .extend_from_slice(&i.to_ne_bytes());
                }
            }
        });
    }
}
