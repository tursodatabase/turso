//! Pages are appended to the database file and a pointer log records where they
//! went. The log lives in the database file too, so one fsync covers a commit.
//!
//! See `docs/internals/pointer-log/design.md`.

use crate::turso_assert;
use rustc_hash::FxHashMap;

/// "TPL1" — identifies a commit block.
pub const COMMIT_BLOCK_MAGIC: u32 = 0x5450_4c31;
pub const COMMIT_BLOCK_FORMAT: u32 = 1;

const BLOCK_HEADER_SIZE: usize = 40;
const BLOCK_ENTRY_SIZE: usize = 12;
const BLOCK_CRC_SIZE: usize = 4;

/// One page version: logical page `page_no` now lives in physical `slot`.
///
/// `body_crc` is what lets a commit need only one fsync. A crash part way
/// through that fsync can leave the block on disk and the body missing, so
/// recovery checks every body the block names before it trusts the block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointerEntry {
    pub page_no: u32,
    pub slot: u32,
    pub body_crc: u32,
}

/// The record a transaction appends to publish its pages.
///
/// A block occupies the first slot of the contiguous run its transaction
/// claimed, and `slot_count` covers that whole run. That is what lets recovery
/// walk from one block to the next without an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBlock {
    pub generation: u32,
    pub block_id: u64,
    pub slot_count: u32,
    pub db_size: u32,
    pub prev_block: u32,
    pub entries: Vec<PointerEntry>,
}

impl CommitBlock {
    /// A non-zero database size marks the end of a transaction, matching the
    /// WAL's commit-frame convention. Blocks before it in the same transaction
    /// hold zero.
    pub fn is_commit(&self) -> bool {
        self.db_size != 0
    }

    pub fn capacity(page_size: usize) -> usize {
        page_size
            .saturating_sub(BLOCK_HEADER_SIZE + BLOCK_CRC_SIZE)
            .checked_div(BLOCK_ENTRY_SIZE)
            .unwrap_or(0)
    }

    pub fn encode(&self, out: &mut [u8]) {
        turso_assert!(
            self.entries.len() <= Self::capacity(out.len()),
            "commit block has more entries than the page holds",
            {
                "entries": self.entries.len(),
                "capacity": Self::capacity(out.len()),
                "page_size": out.len()
            }
        );
        out.fill(0);
        out[0..4].copy_from_slice(&COMMIT_BLOCK_MAGIC.to_le_bytes());
        out[4..8].copy_from_slice(&COMMIT_BLOCK_FORMAT.to_le_bytes());
        out[8..12].copy_from_slice(&self.generation.to_le_bytes());
        out[12..20].copy_from_slice(&self.block_id.to_le_bytes());
        out[20..24].copy_from_slice(&self.slot_count.to_le_bytes());
        out[24..28].copy_from_slice(&self.db_size.to_le_bytes());
        out[28..32].copy_from_slice(&self.prev_block.to_le_bytes());
        out[32..36].copy_from_slice(&(self.entries.len() as u32).to_le_bytes());

        let mut pos = BLOCK_HEADER_SIZE;
        for entry in &self.entries {
            out[pos..pos + 4].copy_from_slice(&entry.page_no.to_le_bytes());
            out[pos + 4..pos + 8].copy_from_slice(&entry.slot.to_le_bytes());
            out[pos + 8..pos + 12].copy_from_slice(&entry.body_crc.to_le_bytes());
            pos += BLOCK_ENTRY_SIZE;
        }

        let crc_at = out.len() - BLOCK_CRC_SIZE;
        let crc = crc32c::crc32c(&out[..crc_at]);
        out[crc_at..].copy_from_slice(&crc.to_le_bytes());
    }

    /// `None` when the slot does not hold a readable block. The recovery walk
    /// reads unwritten and half-written slots as a matter of course, so this
    /// is an ordinary outcome and not an error.
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < BLOCK_HEADER_SIZE + BLOCK_CRC_SIZE {
            return None;
        }
        if read_u32(buf, 0) != COMMIT_BLOCK_MAGIC || read_u32(buf, 4) != COMMIT_BLOCK_FORMAT {
            return None;
        }
        let crc_at = buf.len() - BLOCK_CRC_SIZE;
        if crc32c::crc32c(&buf[..crc_at]) != read_u32(buf, crc_at) {
            return None;
        }
        let entry_count = read_u32(buf, 32) as usize;
        if entry_count > Self::capacity(buf.len()) {
            return None;
        }
        let mut entries = Vec::with_capacity(entry_count);
        let mut pos = BLOCK_HEADER_SIZE;
        for _ in 0..entry_count {
            entries.push(PointerEntry {
                page_no: read_u32(buf, pos),
                slot: read_u32(buf, pos + 4),
                body_crc: read_u32(buf, pos + 8),
            });
            pos += BLOCK_ENTRY_SIZE;
        }
        Some(Self {
            generation: read_u32(buf, 8),
            block_id: read_u64(buf, 12),
            slot_count: read_u32(buf, 20),
            db_size: read_u32(buf, 24),
            prev_block: read_u32(buf, 28),
            entries,
        })
    }
}

/// Where each logical page currently lives.
///
/// A page keeps every version a live reader can still read, newest last, so a
/// reader holding an older block still resolves to the version it saw.
#[derive(Debug, Default)]
pub struct PageMap {
    versions: FxHashMap<u32, Vec<(u64, u32)>>,
}

impl PageMap {
    /// The slot holding `page_no` as of `at_block`, or `None` when the page has
    /// not been rewritten since the last compaction and so still sits in its
    /// canonical slot.
    pub fn slot_of(&self, page_no: u32, at_block: u64) -> Option<u32> {
        let versions = self.versions.get(&page_no)?;
        versions
            .iter()
            .rev()
            .find(|(block_id, _)| *block_id <= at_block)
            .map(|(_, slot)| *slot)
    }

    pub fn insert(&mut self, page_no: u32, block_id: u64, slot: u32) {
        self.versions
            .entry(page_no)
            .or_default()
            .push((block_id, slot));
    }

    /// Slots no reader at or after `floor` can read, because a newer
    /// version of the same page is already visible to all of them.
    pub fn dead_slots(&self, floor: u64) -> Vec<u32> {
        let mut dead = Vec::new();
        for versions in self.versions.values() {
            for window in versions.windows(2) {
                let (_, old_slot) = window[0];
                let (newer_block, _) = window[1];
                if newer_block <= floor {
                    dead.push(old_slot);
                }
            }
        }
        dead
    }

    /// Drop the versions `dead_slots` reported, so the map keeps only what a
    /// live reader can still read.
    pub fn retain_live(&mut self, floor: u64) {
        for versions in self.versions.values_mut() {
            let keep_from = versions
                .iter()
                .rposition(|(block_id, _)| *block_id <= floor)
                .unwrap_or(0);
            versions.drain(..keep_from);
        }
    }
}

/// Hands out contiguous runs of slots past the end of the canonical region.
///
/// Runs are never broken up and dead slots are never handed back one at a time.
/// Compaction is what reclaims space. Keeping the runs whole is what lets
/// recovery step from one commit block to the next.
#[derive(Debug)]
pub struct TailAllocator {
    next_slot: u32,
}

impl TailAllocator {
    pub fn new(tail_base: u32) -> Self {
        Self {
            next_slot: tail_base,
        }
    }

    pub fn claim(&mut self, slot_count: u32) -> Option<u32> {
        let first = self.next_slot;
        self.next_slot = self.next_slot.checked_add(slot_count)?;
        Some(first)
    }

    pub fn next_slot(&self) -> u32 {
        self.next_slot
    }

    /// Called by compaction once the tail is folded back into the canonical
    /// region and the file is about to be truncated.
    pub fn reset(&mut self, tail_base: u32) {
        self.next_slot = tail_base;
    }
}

#[derive(Debug, Default)]
pub struct Recovered {
    pub map: PageMap,
    pub db_size: u32,
    pub last_block_id: u64,
    pub last_commit_block_slot: u32,
    pub next_free_slot: u32,
}

/// What the walk needs next from the caller.
#[derive(Debug, PartialEq, Eq)]
pub enum WalkStep {
    /// Read this slot and hand the bytes back with [`RecoveryWalk::feed`].
    NeedSlot(u32),
    Done,
}

#[derive(Debug)]
enum WalkState {
    ReadBlock {
        slot: u32,
    },
    VerifyBody {
        block: CommitBlock,
        slot: u32,
        idx: usize,
    },
    Done,
}

/// Replays the commit blocks in the tail and rebuilds the page map.
///
/// The caller drives it: [`step`](Self::step) names a slot to read and
/// [`feed`](Self::feed) supplies the bytes. Pulling the reads out this way keeps
/// the replay logic free of I/O, so a test can drive it from memory and the
/// pager can drive it from the file.
#[derive(Debug)]
pub struct RecoveryWalk {
    generation: u32,
    state: WalkState,
    expected_block_id: Option<u64>,
    staged: Vec<PointerEntry>,
    result: Recovered,
}

impl RecoveryWalk {
    pub fn new(generation: u32, anchor_slot: u32) -> Self {
        Self {
            generation,
            state: WalkState::ReadBlock { slot: anchor_slot },
            expected_block_id: None,
            staged: Vec::new(),
            result: Recovered {
                next_free_slot: anchor_slot,
                ..Default::default()
            },
        }
    }

    pub fn step(&self) -> WalkStep {
        match &self.state {
            WalkState::ReadBlock { slot } => WalkStep::NeedSlot(*slot),
            WalkState::VerifyBody { block, idx, .. } => {
                WalkStep::NeedSlot(block.entries[*idx].slot)
            }
            WalkState::Done => WalkStep::Done,
        }
    }

    /// `None` means the slot is past the end of the file, which ends the walk.
    pub fn feed(&mut self, bytes: Option<&[u8]>) {
        let Some(bytes) = bytes else {
            self.state = WalkState::Done;
            return;
        };
        match std::mem::replace(&mut self.state, WalkState::Done) {
            WalkState::ReadBlock { slot } => self.on_block(slot, bytes),
            WalkState::VerifyBody { block, slot, idx } => self.on_body(block, slot, idx, bytes),
            WalkState::Done => {}
        }
    }

    pub fn finish(self) -> Recovered {
        self.result
    }

    fn on_block(&mut self, slot: u32, bytes: &[u8]) {
        let Some(block) = CommitBlock::decode(bytes) else {
            return;
        };
        if block.generation != self.generation {
            return;
        }
        if let Some(expected) = self.expected_block_id {
            if block.block_id != expected {
                return;
            }
        }
        if block.slot_count == 0 {
            return;
        }
        if block.entries.is_empty() {
            self.apply(block, slot);
            return;
        }
        self.state = WalkState::VerifyBody {
            block,
            slot,
            idx: 0,
        };
    }

    fn on_body(&mut self, block: CommitBlock, slot: u32, idx: usize, bytes: &[u8]) {
        if crc32c::crc32c(bytes) != block.entries[idx].body_crc {
            return;
        }
        if idx + 1 < block.entries.len() {
            self.state = WalkState::VerifyBody {
                block,
                slot,
                idx: idx + 1,
            };
            return;
        }
        self.apply(block, slot);
    }

    fn apply(&mut self, block: CommitBlock, slot: u32) {
        self.staged.extend_from_slice(&block.entries);
        if block.is_commit() {
            for entry in self.staged.drain(..) {
                self.result
                    .map
                    .insert(entry.page_no, block.block_id, entry.slot);
            }
            self.result.db_size = block.db_size;
            self.result.last_block_id = block.block_id;
            self.result.last_commit_block_slot = slot;
        }
        self.result.next_free_slot = slot + block.slot_count;
        self.expected_block_id = Some(block.block_id + 1);
        self.state = WalkState::ReadBlock {
            slot: slot + block.slot_count,
        };
    }
}

fn read_u32(buf: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
}

fn read_u64(buf: &[u8], pos: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[pos..pos + 8]);
    u64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE: usize = 4096;

    /// Slots that are `None` are past the end of the file. Slots that hold
    /// zeroes were claimed but their write never got to disk, which is what a
    /// crash part way through a sync leaves behind.
    struct FakeFile {
        slots: Vec<Option<Vec<u8>>>,
    }

    impl FakeFile {
        fn new() -> Self {
            Self { slots: Vec::new() }
        }

        fn write(&mut self, slot: u32, bytes: Vec<u8>) {
            let slot = slot as usize;
            while self.slots.len() <= slot {
                self.slots.push(None);
            }
            self.slots[slot] = Some(bytes);
        }

        fn claim_without_writing(&mut self, slot: u32) {
            self.write(slot, vec![0u8; PAGE_SIZE]);
        }

        fn read(&self, slot: u32) -> Option<&[u8]> {
            self.slots.get(slot as usize)?.as_deref()
        }
    }

    fn body(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE]
    }

    fn encoded(block: &CommitBlock) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        block.encode(&mut buf);
        buf
    }

    fn walk(file: &FakeFile, generation: u32, anchor: u32) -> Recovered {
        let mut walk = RecoveryWalk::new(generation, anchor);
        loop {
            match walk.step() {
                WalkStep::NeedSlot(slot) => {
                    let bytes = file.read(slot);
                    walk.feed(bytes);
                }
                WalkStep::Done => return walk.finish(),
            }
        }
    }

    /// One transaction: a commit block at `first_slot` and one body per page.
    /// Returns the writes it makes, so a test can choose which ones survive.
    fn transaction(
        generation: u32,
        block_id: u64,
        first_slot: u32,
        db_size: u32,
        pages: &[(u32, u8)],
    ) -> Vec<(u32, Vec<u8>)> {
        let mut writes = Vec::new();
        let mut entries = Vec::new();
        for (idx, (page_no, fill)) in pages.iter().enumerate() {
            let slot = first_slot + 1 + idx as u32;
            let bytes = body(*fill);
            entries.push(PointerEntry {
                page_no: *page_no,
                slot,
                body_crc: crc32c::crc32c(&bytes),
            });
            writes.push((slot, bytes));
        }
        let block = CommitBlock {
            generation,
            block_id,
            slot_count: pages.len() as u32 + 1,
            db_size,
            prev_block: 0,
            entries,
        };
        writes.push((first_slot, encoded(&block)));
        writes
    }

    #[test]
    fn commit_block_survives_a_round_trip() {
        let block = CommitBlock {
            generation: 7,
            block_id: 42,
            slot_count: 3,
            db_size: 100,
            prev_block: 11,
            entries: vec![
                PointerEntry {
                    page_no: 1,
                    slot: 200,
                    body_crc: 0xdead_beef,
                },
                PointerEntry {
                    page_no: 9,
                    slot: 201,
                    body_crc: 0x0bad_cafe,
                },
            ],
        };
        assert_eq!(CommitBlock::decode(&encoded(&block)), Some(block));
    }

    #[test]
    fn a_zeroed_slot_is_not_a_block() {
        assert_eq!(CommitBlock::decode(&vec![0u8; PAGE_SIZE]), None);
    }

    #[test]
    fn one_flipped_bit_makes_a_block_unreadable() {
        let block = CommitBlock {
            generation: 1,
            block_id: 1,
            slot_count: 2,
            db_size: 4,
            prev_block: 0,
            entries: vec![PointerEntry {
                page_no: 3,
                slot: 5,
                body_crc: 7,
            }],
        };
        for bit in [0usize, 100, 1000, PAGE_SIZE * 8 - 1] {
            let mut buf = encoded(&block);
            buf[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(CommitBlock::decode(&buf), None, "bit {bit} went unnoticed");
        }
    }

    #[test]
    fn a_page_resolves_to_the_newest_version_the_reader_may_see() {
        let mut map = PageMap::default();
        map.insert(5, 10, 100);
        map.insert(5, 20, 200);
        map.insert(5, 30, 300);

        assert_eq!(map.slot_of(5, 9), None);
        assert_eq!(map.slot_of(5, 10), Some(100));
        assert_eq!(map.slot_of(5, 25), Some(200));
        assert_eq!(map.slot_of(5, 99), Some(300));
        assert_eq!(map.slot_of(6, 99), None);
    }

    #[test]
    fn a_slot_stays_alive_while_a_reader_can_still_read_it() {
        let mut map = PageMap::default();
        map.insert(5, 10, 100);
        map.insert(5, 20, 200);
        map.insert(5, 30, 300);

        assert_eq!(map.dead_slots(9), Vec::<u32>::new());
        assert_eq!(map.dead_slots(20), vec![100]);
        assert_eq!(map.dead_slots(30), vec![100, 200]);

        map.retain_live(30);
        assert_eq!(map.slot_of(5, 30), Some(300));
        assert_eq!(map.dead_slots(30), Vec::<u32>::new());
    }

    #[test]
    fn runs_are_handed_out_one_after_another() {
        let mut alloc = TailAllocator::new(64);
        assert_eq!(alloc.claim(3), Some(64));
        assert_eq!(alloc.claim(2), Some(67));
        assert_eq!(alloc.next_slot(), 69);
        alloc.reset(70);
        assert_eq!(alloc.claim(1), Some(70));
    }

    #[test]
    fn recovery_walks_a_chain_of_committed_transactions() {
        let mut file = FakeFile::new();
        for (slot, bytes) in transaction(1, 1, 10, 4, &[(1, 0xa1), (2, 0xa2)]) {
            file.write(slot, bytes);
        }
        for (slot, bytes) in transaction(1, 2, 13, 5, &[(2, 0xb2), (5, 0xb5)]) {
            file.write(slot, bytes);
        }

        let recovered = walk(&file, 1, 10);
        assert_eq!(recovered.db_size, 5);
        assert_eq!(recovered.last_block_id, 2);
        assert_eq!(recovered.last_commit_block_slot, 13);
        assert_eq!(recovered.next_free_slot, 16);
        assert_eq!(recovered.map.slot_of(1, 2), Some(11));
        assert_eq!(recovered.map.slot_of(2, 2), Some(14));
        assert_eq!(recovered.map.slot_of(5, 2), Some(15));
        assert_eq!(recovered.map.slot_of(2, 1), Some(12));
    }

    #[test]
    fn a_block_from_an_older_generation_is_rejected() {
        let mut file = FakeFile::new();
        for (slot, bytes) in transaction(1, 1, 10, 4, &[(1, 0xa1)]) {
            file.write(slot, bytes);
        }
        for (slot, bytes) in transaction(9, 2, 12, 5, &[(2, 0xb2)]) {
            file.write(slot, bytes);
        }

        let recovered = walk(&file, 1, 10);
        assert_eq!(recovered.db_size, 4);
        assert_eq!(recovered.last_block_id, 1);
        assert_eq!(recovered.map.slot_of(2, 99), None);
    }

    #[test]
    fn a_gap_in_the_block_ids_stops_the_walk() {
        let mut file = FakeFile::new();
        for (slot, bytes) in transaction(1, 1, 10, 4, &[(1, 0xa1)]) {
            file.write(slot, bytes);
        }
        for (slot, bytes) in transaction(1, 7, 12, 5, &[(2, 0xb2)]) {
            file.write(slot, bytes);
        }

        let recovered = walk(&file, 1, 10);
        assert_eq!(recovered.last_block_id, 1);
        assert_eq!(recovered.map.slot_of(2, 99), None);
    }

    #[test]
    fn a_transaction_whose_body_never_got_to_disk_is_discarded() {
        let mut file = FakeFile::new();
        for (slot, bytes) in transaction(1, 1, 10, 4, &[(1, 0xa1)]) {
            file.write(slot, bytes);
        }
        let second = transaction(1, 2, 12, 5, &[(2, 0xb2), (3, 0xb3)]);
        for (slot, bytes) in &second {
            if *slot == 13 {
                file.claim_without_writing(*slot);
            } else {
                file.write(*slot, bytes.clone());
            }
        }

        let recovered = walk(&file, 1, 10);
        assert_eq!(
            recovered.db_size, 4,
            "the block was on disk but a body was not"
        );
        assert_eq!(recovered.map.slot_of(2, 99), None);
        assert_eq!(recovered.map.slot_of(3, 99), None);
    }

    #[test]
    fn a_transaction_spanning_two_blocks_is_all_or_nothing() {
        let mut file = FakeFile::new();
        let first_body = body(0xc1);
        let second_body = body(0xc2);
        let staged = CommitBlock {
            generation: 1,
            block_id: 1,
            slot_count: 2,
            db_size: 0,
            prev_block: 0,
            entries: vec![PointerEntry {
                page_no: 1,
                slot: 11,
                body_crc: crc32c::crc32c(&first_body),
            }],
        };
        let commit = CommitBlock {
            generation: 1,
            block_id: 2,
            slot_count: 2,
            db_size: 9,
            prev_block: 10,
            entries: vec![PointerEntry {
                page_no: 2,
                slot: 13,
                body_crc: crc32c::crc32c(&second_body),
            }],
        };
        file.write(10, encoded(&staged));
        file.write(11, first_body);

        let without_commit = walk(&file, 1, 10);
        assert_eq!(without_commit.db_size, 0);
        assert_eq!(without_commit.map.slot_of(1, 99), None);

        file.write(12, encoded(&commit));
        file.write(13, second_body);

        let with_commit = walk(&file, 1, 10);
        assert_eq!(with_commit.db_size, 9);
        assert_eq!(with_commit.map.slot_of(1, 99), Some(11));
        assert_eq!(with_commit.map.slot_of(2, 99), Some(13));
    }

    /// A sync that is cut short leaves some of its writes on disk and loses the
    /// rest, in no particular order. For every one of those outcomes recovery
    /// must show the whole transaction or none of it.
    #[test]
    fn a_commit_is_all_or_nothing_for_every_way_a_sync_can_be_cut_short() {
        let committed = transaction(1, 1, 10, 4, &[(1, 0xa1), (2, 0xa2)]);
        let pending = transaction(1, 2, 13, 7, &[(2, 0xb2), (3, 0xb3), (4, 0xb4)]);
        let all_durable = (1u32 << pending.len()) - 1;

        for subset in 0..=all_durable {
            let mut file = FakeFile::new();
            for (slot, bytes) in &committed {
                file.write(*slot, bytes.clone());
            }
            for (idx, (slot, bytes)) in pending.iter().enumerate() {
                if subset & (1 << idx) != 0 {
                    file.write(*slot, bytes.clone());
                } else {
                    file.claim_without_writing(*slot);
                }
            }

            let recovered = walk(&file, 1, 10);
            if subset == all_durable {
                assert_eq!(recovered.db_size, 7, "subset {subset:b} lost a full commit");
                assert_eq!(recovered.map.slot_of(2, 99), Some(14));
                assert_eq!(recovered.map.slot_of(3, 99), Some(15));
                assert_eq!(recovered.map.slot_of(4, 99), Some(16));
            } else {
                assert_eq!(
                    recovered.db_size, 4,
                    "subset {subset:b} showed a transaction that never committed"
                );
                assert_eq!(recovered.map.slot_of(2, 99), Some(12));
                assert_eq!(recovered.map.slot_of(3, 99), None);
                assert_eq!(recovered.map.slot_of(4, 99), None);
            }
        }
    }
}
