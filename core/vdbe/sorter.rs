use crate::types::IOResultOr;
use crate::{turso_assert, turso_assert_eq};
use turso_parser::ast::SortOrder;

use crate::sync::RwLock;
use crate::sync::{atomic, Arc};
use bumpalo::Bump;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::ops::Range;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::alloc::vec;
use crate::alloc::*;
use crate::io::TempFile;
use crate::types::{cmp_in_column, cmp_with_sort, IOCompletions, ValueIterator};
use crate::{
    error::LimboError,
    io::{Buffer, Completion, CompletionGroup, File, IO},
    storage::sqlite3_ondisk::{read_varint, varint_len, write_varint},
    translate::collate::CollationSeq,
    types::{IOResult, ImmutableRecord, KeyInfo, RecordBuf, ValueRef},
    Result,
};
use crate::{io_yield_one, return_if_io, CompletionError};

/// A custom comparison function for sorting custom type columns.
/// Takes two value references and returns an Ordering.
/// Used when a custom type defines a `<` operator for correct sort behavior.
pub type SortComparator = Arc<dyn Fn(&ValueRef, &ValueRef) -> Result<Ordering> + Send + Sync>;

/// Number of leading sort-key columns encoded in a normalized key.
const NORM_COLUMNS: usize = 2;

/// Bit position of the 3-bit class rank in a normalized column field.
const NORM_CLASS_SHIFT: u32 = 61;

/// Order-preserving encoding of the leading sort-key columns, one 64-bit
/// field per column, column 0 in the high bits.
type NormKey = u128;

/// Encodes the first [NORM_COLUMNS] sort-key columns of a record.
///
/// Each column field is 3 class-rank bits followed by a 61-bit payload. Class
/// ranks follow the SQL type ordering (NULL < numeric < text < blob), with NULL
/// remapped above blob when the effective NULLS placement requires it. A field
/// is bit-inverted for DESC so a plain integer comparison applies the sort
/// direction.
///
/// Invariant per column: a smaller field means the column orders first. Equal
/// fields mean nothing on their own unless the column is *settled* for the
/// record, in which case an equal field proves an equal column value. The
/// returned count is the number of leading columns settled for this record,
/// capped at the key length; see [cmp_normalized] for how two records use it.
fn normalized_key(
    values: &[ValueRef<'_>],
    key_info: &[KeyInfo],
    comparators: &[Option<SortComparator>],
) -> (NormKey, u8) {
    let mut key: NormKey = 0;
    let mut settled = 0u8;
    let mut leading_settled = true;
    for col in 0..NORM_COLUMNS {
        let field = match (values.get(col), key_info.get(col)) {
            (Some(value), Some(info)) => {
                let custom = comparators.get(col).is_some_and(|c| c.is_some());
                let (field, decisive) = normalized_column(value, info, custom);
                if leading_settled && decisive {
                    settled += 1;
                } else {
                    leading_settled = false;
                }
                field
            }
            _ => 0,
        };
        key = (key << 64) | field as NormKey;
    }
    (key, settled)
}

/// Encodes one column value; see [normalized_key]. The flag is true when
/// equal fields prove equal values.
fn normalized_column(value: &ValueRef<'_>, key: &KeyInfo, custom_comparator: bool) -> (u64, bool) {
    use crate::numeric::Numeric;
    if custom_comparator {
        // Custom ordering: the normalized key cannot mirror it.
        return (0, false);
    }
    let (norm, decisive) = match value {
        ValueRef::Null => {
            // Rank NULL above blobs when it must sort after non-NULL values in
            // the pre-inversion key space. With `nulls_order` unset the natural
            // rank is correct for ASC, and the DESC bit inversion below moves
            // NULL last, matching the reversed comparison.
            let rank_high = match (key.nulls_order, key.sort_order) {
                (None, _) => false,
                (Some(turso_parser::ast::NullsOrder::First), SortOrder::Asc) => false,
                (Some(turso_parser::ast::NullsOrder::First), SortOrder::Desc) => true,
                (Some(turso_parser::ast::NullsOrder::Last), SortOrder::Asc) => true,
                (Some(turso_parser::ast::NullsOrder::Last), SortOrder::Desc) => false,
            };
            (
                if rank_high {
                    4u64 << NORM_CLASS_SHIFT
                } else {
                    0
                },
                true,
            )
        }
        ValueRef::Numeric(n) => {
            // Map both integers and floats through the monotone f64 encoding so
            // cross-type comparisons stay ordered (rounding to nearest is
            // monotone). Equal-after-truncation pairs fall back to the exact
            // comparison; `decisive` only covers integers whose f64 form is
            // exact and injective after dropping the low 3 mantissa bits.
            let (f, exact) = match n {
                Numeric::Integer(i) => (*i as f64, i.unsigned_abs() < (1u64 << 49)),
                Numeric::Float(f) => (f64::from(*f), false),
            };
            let f = if f == 0.0 { 0.0 } else { f }; // collapse -0.0 with +0.0
            let bits = f.to_bits();
            let monotone = if bits >> 63 == 1 {
                !bits
            } else {
                bits | (1u64 << 63)
            };
            ((1u64 << NORM_CLASS_SHIFT) | (monotone >> 3), exact)
        }
        ValueRef::Text(t) => {
            if !matches!(key.collation, CollationSeq::Unset | CollationSeq::Binary) {
                // Non-binary collation: constant key, always full comparison.
                (2u64 << NORM_CLASS_SHIFT, false)
            } else {
                let bytes = t.value.as_bytes();
                (normalized_prefix(2, bytes), bytes.len() <= 7)
            }
        }
        ValueRef::Blob(b) => (normalized_prefix(3, b), b.len() <= 7),
    };
    (
        if key.sort_order == SortOrder::Desc {
            !norm
        } else {
            norm
        },
        decisive,
    )
}

/// Packs the first 7 bytes (big-endian) and a length capped at 8 into the
/// 61-bit payload. With equal prefixes the capped length orders a string
/// against its zero-padded extension correctly, and ties beyond the prefix
/// (both lengths >= 8) fall back to the full comparison.
fn normalized_prefix(class: u64, bytes: &[u8]) -> u64 {
    let mut prefix = [0u8; 8];
    let n = bytes.len().min(7);
    prefix[..n].copy_from_slice(&bytes[..n]);
    let p56 = u64::from_be_bytes(prefix) >> 8;
    (class << NORM_CLASS_SHIFT) | (p56 << 5) | (bytes.len().min(8) as u64)
}

/// Orders two records by their normalized keys when that is enough.
///
/// Returns the ordering when the first differing column field is preceded
/// only by columns settled on both sides, or when every key column is
/// settled and the keys are equal. Otherwise returns the index of the first
/// column the caller still has to compare with the full comparison.
#[inline]
fn cmp_normalized(
    a: NormKey,
    a_settled: u8,
    b: NormKey,
    b_settled: u8,
    key_len: usize,
) -> std::result::Result<Ordering, usize> {
    let settled = a_settled.min(b_settled) as usize;
    if a == b {
        if settled >= key_len {
            Ok(Ordering::Equal)
        } else {
            Err(settled)
        }
    } else {
        let first_diff = if (a >> 64) != (b >> 64) { 0 } else { 1 };
        if first_diff <= settled {
            Ok(a.cmp(&b))
        } else {
            Err(settled)
        }
    }
}

/// Compares sort-key columns starting at `from`, honoring custom comparators,
/// collations, sort direction and NULLS placement.
fn cmp_key_columns(
    a: &[ValueRef<'_>],
    b: &[ValueRef<'_>],
    key_info: &[KeyInfo],
    comparators: &[Option<SortComparator>],
    from: usize,
) -> Ordering {
    for i in from..a.len().min(b.len()).min(key_info.len()) {
        let (a_val, b_val, info) = (&a[i], &b[i], &key_info[i]);
        let cmp = if let Some(Some(comparator)) = comparators.get(i) {
            let base = comparator(a_val, b_val).expect("Memory allocation failed here");
            cmp_with_sort(base, a_val, b_val, info)
        } else {
            cmp_in_column(a_val, b_val, info)
        };
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

#[derive(Debug, Clone, Copy)]
enum SortState {
    Start,
    Flush,
    InitHeap,
    Next,
}

#[derive(Debug, Clone, Copy)]
enum InsertState {
    Start,
    Insert,
}

#[derive(Debug, Clone, Copy)]
enum InitChunkHeapState {
    Start,
    PushChunk,
}

pub struct Sorter {
    /// Arena allocator for records - provides fast bump allocation and bulk deallocation.
    /// All record data (payload bytes, key_values) is stored here for in-memory sorting.
    arena: Bump,
    /// Pointers to records allocated in the arena. Sorting moves only 8-byte pointers,
    /// which prevents high memmove costs during sorting.
    /// SAFETY: These pointers are valid as long as the arena hasn't been reset.
    records: Vec<NonNull<ArenaSortableRecord>>,
    /// The current record.
    current: Option<ImmutableRecord>,
    /// The number of values in the key.
    key_len: usize,
    /// The key info.
    pub index_key_info: Rc<Vec<KeyInfo>>,
    /// Per-column custom comparators for custom type ordering.
    /// When present, used instead of standard ValueRef comparison for that column.
    comparators: Rc<Vec<Option<SortComparator>>>,
    /// Sorted chunks stored on disk.
    chunks: Vec<SortedChunk>,
    /// Min-heap over the head record of every chunk that still has one.
    merge_heap: Vec<MergeEntry>,
    /// The maximum size of the in-memory buffer in bytes before the records are flushed to a chunk file.
    max_buffer_size: usize,
    /// The current size of the in-memory buffer in bytes.
    current_buffer_size: usize,
    /// The minimum size of a chunk read buffer in bytes. The actual buffer size can be larger if the largest
    /// record in the buffer is larger than this value.
    min_chunk_read_buffer_size: usize,
    /// The maximum record payload size in the in-memory buffer.
    max_payload_size_in_buffer: usize,
    /// The IO object.
    io: Arc<dyn IO>,
    /// The temporary file for chunks.
    temp_file: Option<TempFile>,
    /// Offset where the next chunk will be placed in the `temp_file`
    next_chunk_offset: usize,
    /// State machine for [Sorter::sort]
    sort_state: SortState,
    /// State machine for [Sorter::insert]
    insert_state: InsertState,
    /// State machine for [Sorter::init_chunk_heap]
    init_chunk_heap_state: InitChunkHeapState,
    /// Pending IO completion along with the chunk index that needs to be retried after IO completes.
    pending_completion: Option<(Completion, usize)>,
    /// Temp storage mode (memory vs file) for spilled data
    temp_store: crate::TempStore,
}

impl Sorter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        order: &[SortOrder],
        collations: Vec<CollationSeq>,
        nulls_orders: Vec<Option<turso_parser::ast::NullsOrder>>,
        comparators: Vec<Option<SortComparator>>,
        max_buffer_size_bytes: usize,
        min_chunk_read_buffer_size_bytes: usize,
        io: Arc<dyn IO>,
        temp_store: crate::TempStore,
    ) -> Result<Self> {
        turso_assert_eq!(order.len(), collations.len());
        let index_key_info = order
            .iter()
            .zip(collations)
            .zip(nulls_orders)
            .map(|((order, collation), nulls)| KeyInfo {
                sort_order: *order,
                collation,
                nulls_order: nulls,
            })
            .try_collect()?;
        let this = Self {
            arena: Bump::new(),
            records: vec![],
            current: None,
            key_len: order.len(),
            index_key_info: Rc::new(index_key_info),
            comparators: Rc::new(comparators),
            chunks: vec![],
            merge_heap: vec![],
            max_buffer_size: max_buffer_size_bytes,
            current_buffer_size: 0,
            min_chunk_read_buffer_size: min_chunk_read_buffer_size_bytes,
            max_payload_size_in_buffer: 0,
            io,
            temp_file: None,
            next_chunk_offset: 0,
            sort_state: SortState::Start,
            insert_state: InsertState::Start,
            init_chunk_heap_state: InitChunkHeapState::Start,
            pending_completion: None,
            temp_store,
        };
        Ok(this)
    }

    pub const fn is_empty(&self) -> bool {
        self.records.is_empty() && self.chunks.is_empty()
    }

    pub const fn has_more(&self) -> bool {
        self.current.is_some()
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) -> IOResultOr<()> {
        loop {
            match self.sort_state {
                SortState::Start => {
                    if self.chunks.is_empty() {
                        // Sort ascending then reverse - we pop from end so this gives ascending output.
                        // NOTE: We can't just sort descending because stable sort preserves insertion
                        // order for equal elements, and descending sort doesn't reverse equal elements.
                        // SAFETY: All pointers in records are valid (arena hasn't been reset).
                        self.records
                            .sort_by(|a, b| unsafe { a.as_ref().cmp(b.as_ref()) });
                        self.records.reverse();
                        self.sort_state = SortState::Next;
                    } else {
                        self.sort_state = SortState::Flush;
                    }
                }
                SortState::Flush => {
                    self.sort_state = SortState::InitHeap;
                    if let Some(c) = self.flush()? {
                        io_yield_one!(c);
                    }
                }
                SortState::InitHeap => {
                    // Check for write errors before proceeding
                    if self.chunks.iter().any(|chunk| {
                        matches!(*chunk.io_state.read(), SortedChunkIOState::WriteError)
                    }) {
                        return Err(CompletionError::IOError(
                            std::io::ErrorKind::WriteZero,
                            "sorter write",
                        )
                        .into());
                    }
                    turso_assert!(
                        !self.chunks.iter().any(|chunk| {
                            matches!(*chunk.io_state.read(), SortedChunkIOState::WaitingForWrite)
                        }),
                        "chunks should been written"
                    );
                    return_if_io!(self.init_chunk_heap());
                    self.sort_state = SortState::Next;
                }
                SortState::Next => {
                    return_if_io!(self.next());
                    self.sort_state = SortState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> IOResultOr<()> {
        if self.chunks.is_empty() {
            match self.records.pop() {
                Some(ptr) => {
                    // SAFETY: ptr is valid - arena hasn't been reset yet.
                    let arena_record = unsafe { ptr.as_ref() };
                    let payload = arena_record.payload();

                    match &mut self.current {
                        Some(record) => {
                            record.invalidate();
                            record.start_serialization(payload)?;
                        }
                        None => {
                            self.current = Some(arena_record.to_immutable_record()?);
                        }
                    }
                    if self.records.is_empty() {
                        self.arena.reset();
                    }
                }
                None => self.current = None,
            }
            Ok(IOResult::Done(()))
        } else {
            self.next_from_chunks()
        }
    }

    pub const fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    /// Moves the current record out of the sorter, seeding the next
    /// [Sorter::next] call with `spent`'s allocation. This lets SorterData
    /// transfer records to a register with no allocation or payload copy.
    pub fn take_current(&mut self, spent: RecordBuf) -> Option<ImmutableRecord> {
        let current = self.current.take();
        if current.is_some() {
            self.current = Some(ImmutableRecord::from_buf(spent));
        }
        current
    }

    pub fn insert(&mut self, record: &ImmutableRecord) -> IOResultOr<()> {
        let payload_size = record.get_payload().len();
        loop {
            match self.insert_state {
                InsertState::Start => {
                    self.insert_state = InsertState::Insert;
                    if self.current_buffer_size + payload_size > self.max_buffer_size {
                        if let Some(c) = self.flush()? {
                            if !c.succeeded() {
                                io_yield_one!(c);
                            }
                        }
                        // Check for write errors immediately after flush completes
                        if self.chunks.iter().any(|chunk| {
                            matches!(*chunk.io_state.read(), SortedChunkIOState::WriteError)
                        }) {
                            return Err(CompletionError::IOError(
                                std::io::ErrorKind::WriteZero,
                                "sorter write",
                            )
                            .into());
                        }
                    }
                }
                InsertState::Insert => {
                    let sortable_record = ArenaSortableRecord::new(
                        &self.arena,
                        record,
                        self.key_len,
                        &self.index_key_info,
                        &self.comparators,
                    )?;
                    let record_ref = self.arena.try_alloc(sortable_record)?;
                    // SAFETY: try_alloc returns a valid, aligned, non-null pointer.
                    self.records.try_push(NonNull::from(record_ref))?;
                    self.current_buffer_size += payload_size;
                    self.max_payload_size_in_buffer =
                        self.max_payload_size_in_buffer.max(payload_size);
                    self.insert_state = InsertState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    fn init_chunk_heap(&mut self) -> IOResultOr<()> {
        match self.init_chunk_heap_state {
            InitChunkHeapState::Start => {
                let mut group = CompletionGroup::new(|_| {});
                for chunk in self.chunks.iter_mut() {
                    if let Err(e) = chunk.read(Some(&mut group)) {
                        tracing::error!("Failed to read chunk: {e}");
                        group.cancel();
                        self.io.drain_completions(group.completions())?;
                        return Err(e.into());
                    }
                }
                self.init_chunk_heap_state = InitChunkHeapState::PushChunk;
                let completion = group.build();
                io_yield_one!(completion);
            }
            InitChunkHeapState::PushChunk => {
                self.merge_heap.try_reserve(self.chunks.len())?;
                for chunk_idx in 0..self.chunks.len() {
                    // Every chunk's first read filled its buffer, or read the
                    // whole chunk, so its first record is complete.
                    match self.advance_chunk(chunk_idx)? {
                        ChunkNextResult::Done(true) => {
                            let entry = self.chunks[chunk_idx].merge_entry(chunk_idx);
                            self.heap_push(entry)?;
                        }
                        ChunkNextResult::Done(false) => {}
                        ChunkNextResult::IO(_) => {
                            turso_assert!(false, "chunk needs a second read for its first record");
                        }
                    }
                }
                self.init_chunk_heap_state = InitChunkHeapState::Start;
                Ok(IOResult::Done(()))
            }
        }
    }

    /// Serves the next record from the chunk heap.
    ///
    /// The heap holds the head record of every chunk that has one. After a
    /// head is taken, its chunk is advanced; if that needs a read, the chunk
    /// leaves the heap and `pending_completion` remembers it, so the next
    /// call waits for the read and puts the chunk back before choosing the
    /// next smallest head.
    fn next_from_chunks(&mut self) -> IOResultOr<()> {
        while let Some((completion, chunk_idx)) = self.pending_completion.take() {
            if !completion.finished() {
                self.pending_completion = Some((completion.clone(), chunk_idx));
                return Ok(IOResult::IO(IOCompletions(completion)));
            }
            match self.advance_chunk(chunk_idx)? {
                ChunkNextResult::Done(true) => {
                    let entry = self.chunks[chunk_idx].merge_entry(chunk_idx);
                    self.heap_push(entry)?;
                }
                ChunkNextResult::Done(false) => {}
                ChunkNextResult::IO(c) => self.pending_completion = Some((c, chunk_idx)),
            }
        }

        let Some(top) = self.merge_heap.first().copied() else {
            self.current = None;
            return Ok(IOResult::Done(()));
        };
        let chunk_idx = top.chunk_idx as usize;
        self.copy_head_to_current(chunk_idx)?;
        match self.advance_chunk(chunk_idx)? {
            ChunkNextResult::Done(true) => {
                let entry = self.chunks[chunk_idx].merge_entry(chunk_idx);
                self.heap_replace_top(entry);
            }
            ChunkNextResult::Done(false) => {
                self.heap_pop();
            }
            ChunkNextResult::IO(c) => {
                self.heap_pop();
                self.pending_completion = Some((c, chunk_idx));
            }
        }
        Ok(IOResult::Done(()))
    }

    fn copy_head_to_current(&mut self, chunk_idx: usize) -> Result<()> {
        let chunk = &self.chunks[chunk_idx];
        let buffer = chunk.buffer.read();
        let payload = &buffer[chunk.head.clone().expect("chunk in heap has a head")];
        match &mut self.current {
            Some(record) => {
                record.invalidate();
                record.start_serialization(payload)?;
            }
            None => {
                let mut record = ImmutableRecord::new(payload.len())?;
                record.start_serialization(payload)?;
                self.current = Some(record);
            }
        }
        Ok(())
    }

    fn advance_chunk(&mut self, chunk_idx: usize) -> Result<ChunkNextResult> {
        self.chunks[chunk_idx].advance(self.key_len, &self.index_key_info, &self.comparators)
    }

    /// Orders two heap entries; equal keys keep the older chunk first so
    /// records that compare equal come out in insertion order.
    #[inline]
    fn merge_less(&self, a: &MergeEntry, b: &MergeEntry) -> bool {
        let ord = match cmp_normalized(
            a.norm_key,
            a.norm_settled,
            b.norm_key,
            b.norm_settled,
            self.key_len,
        ) {
            Ok(ord) => ord,
            Err(from) => cmp_key_columns(
                &self.chunks[a.chunk_idx as usize].head_keys,
                &self.chunks[b.chunk_idx as usize].head_keys,
                &self.index_key_info,
                &self.comparators,
                from,
            ),
        };
        match ord {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => a.chunk_idx < b.chunk_idx,
        }
    }

    fn heap_push(&mut self, entry: MergeEntry) -> Result<()> {
        self.merge_heap.try_push(entry)?;
        let mut i = self.merge_heap.len() - 1;
        while i > 0 {
            let parent = (i - 1) / 2;
            if !self.merge_less(&self.merge_heap[i], &self.merge_heap[parent]) {
                break;
            }
            self.merge_heap.swap(i, parent);
            i = parent;
        }
        Ok(())
    }

    fn heap_pop(&mut self) -> Option<MergeEntry> {
        let last = self.merge_heap.pop()?;
        if self.merge_heap.is_empty() {
            return Some(last);
        }
        let top = std::mem::replace(&mut self.merge_heap[0], last);
        self.sift_down(0);
        Some(top)
    }

    fn heap_replace_top(&mut self, entry: MergeEntry) {
        self.merge_heap[0] = entry;
        self.sift_down(0);
    }

    fn sift_down(&mut self, mut i: usize) {
        let len = self.merge_heap.len();
        loop {
            let left = 2 * i + 1;
            if left >= len {
                break;
            }
            let right = left + 1;
            let mut smallest = left;
            if right < len && self.merge_less(&self.merge_heap[right], &self.merge_heap[left]) {
                smallest = right;
            }
            if !self.merge_less(&self.merge_heap[smallest], &self.merge_heap[i]) {
                break;
            }
            self.merge_heap.swap(i, smallest);
            i = smallest;
        }
    }

    fn flush(&mut self) -> Result<Option<Completion>> {
        if self.records.is_empty() {
            // Dummy completion to not complicate logic handling
            return Ok(None);
        }

        // SAFETY: All pointers are valid (arena not reset).
        self.records
            .sort_by(|a, b| unsafe { a.as_ref().cmp(b.as_ref()) });

        let chunk_file = match &self.temp_file {
            Some(temp_file) => temp_file.file.clone(),
            None => {
                let temp_file = TempFile::with_temp_store(&self.io, self.temp_store)?;
                let chunk_file = temp_file.file.clone();
                self.temp_file = Some(temp_file);
                chunk_file
            }
        };

        // Make sure the chunk buffer size can fit the largest record and its size varint.
        let chunk_buffer_size = self
            .min_chunk_read_buffer_size
            .max(self.max_payload_size_in_buffer + 9);

        let mut chunk_size = 0;
        // Pre-compute varint lengths for record sizes to determine the total buffer size.
        // SAFETY: All pointers are valid because they are allocated in the arena,
        // and the arena hasn't been reset.
        let mut record_size_lengths = Vec::try_with_capacity_ext(self.records.len())?;
        for ptr in self.records.iter() {
            let record_size = unsafe { ptr.as_ref().payload().len() };
            let size_len = varint_len(record_size as u64);
            // Enough space was preallocated to `push` instead of `try_push`
            record_size_lengths.push(size_len);
            chunk_size += size_len + record_size;
        }

        let mut chunk = SortedChunk::new(
            chunk_file,
            self.next_chunk_offset,
            chunk_buffer_size,
            self.key_len,
        )?;
        let c = chunk.write(&self.records, record_size_lengths, chunk_size)?;
        self.chunks.try_push(chunk)?;

        self.records.clear();
        self.arena.reset();

        self.current_buffer_size = 0;
        self.max_payload_size_in_buffer = 0;
        // increase offset start for next chunk
        self.next_chunk_offset += chunk_size;

        Ok(Some(c))
    }
}

/// The head record of one chunk, as held in the merge heap. Ties on the
/// normalized key are broken by comparing the chunks' decoded head keys.
#[derive(Clone, Copy)]
struct MergeEntry {
    norm_key: NormKey,
    norm_settled: u8,
    chunk_idx: u32,
}

/// A sorted chunk is a sorted run of records written to the temp file when
/// the in-memory buffer fills up. During the merge phase each chunk reads
/// its run back through a fixed-size buffer and exposes one record at a
/// time, the head, as a byte range into that buffer.
///
/// The buffer holds `buffer_len` valid bytes. Bytes before `parse_pos` are
/// consumed; the head is the last consumed record and stays valid until the
/// chunk is advanced. When the front half of the buffer is consumed the
/// unparsed tail is moved to the front and a read is started into the free
/// space, so the disk read overlaps with merging the records already in
/// memory. A read only ever appends after `buffer_len`, so the head's bytes
/// are never touched while a read is in flight.
struct SortedChunk {
    /// The file containing the chunk data.
    file: Arc<dyn File>,
    /// Byte offset where this chunk starts in the file.
    start_offset: u64,
    /// Total size of this chunk in bytes (set during write, used to detect EOF during read).
    chunk_size: usize,
    /// Fixed-size buffer for reading data from disk. The capacity (`buffer.len()`) is
    /// constant; use `buffer_len` for the amount of valid data.
    buffer: Arc<RwLock<Vec<u8>>>,
    /// Amount of valid data in `buffer`, from index 0 to buffer_len.
    buffer_len: Arc<atomic::AtomicUsize>,
    /// Offset in `buffer` of the first byte not yet parsed.
    parse_pos: usize,
    /// Byte range in `buffer` of the head record.
    head: Option<Range<usize>>,
    /// Sort-key values of the head record, pointing into `buffer`.
    head_keys: Vec<ValueRef<'static>>,
    /// Normalized key of the head record; see [normalized_key].
    head_norm_key: NormKey,
    /// Number of leading key columns settled by `head_norm_key`.
    head_norm_settled: u8,
    /// The read in flight, or finished but not yet acknowledged by `advance`.
    pending_read: Option<Completion>,
    /// State of the chunk write.
    io_state: Arc<RwLock<SortedChunkIOState>>,
    /// Cumulative bytes read from disk. When this equals `chunk_size`, we've read everything.
    total_bytes_read: Arc<atomic::AtomicUsize>,
}

enum ChunkNextResult {
    /// True when the chunk now has a head record, false when it is exhausted.
    Done(bool),
    IO(Completion),
}

impl SortedChunk {
    fn new(
        file: Arc<dyn File>,
        start_offset: usize,
        buffer_size: usize,
        key_len: usize,
    ) -> Result<Self> {
        Ok(Self {
            file,
            start_offset: start_offset as u64,
            chunk_size: 0,
            buffer: Arc::new(RwLock::new(try_vec![0; buffer_size]?)),
            buffer_len: Arc::new(atomic::AtomicUsize::new(0)),
            parse_pos: 0,
            head: None,
            head_keys: Vec::try_with_capacity_ext(key_len)?,
            head_norm_key: 0,
            head_norm_settled: 0,
            pending_read: None,
            io_state: Arc::new(RwLock::new(SortedChunkIOState::None)),
            total_bytes_read: Arc::new(atomic::AtomicUsize::new(0)),
        })
    }

    fn buffer_len(&self) -> usize {
        self.buffer_len.load(atomic::Ordering::SeqCst)
    }

    fn set_buffer_len(&self, len: usize) {
        self.buffer_len.store(len, atomic::Ordering::SeqCst);
    }

    fn bytes_read(&self) -> usize {
        self.total_bytes_read.load(atomic::Ordering::SeqCst)
    }

    const fn merge_entry(&self, chunk_idx: usize) -> MergeEntry {
        MergeEntry {
            norm_key: self.head_norm_key,
            norm_settled: self.head_norm_settled,
            chunk_idx: chunk_idx as u32,
        }
    }

    /// Drops the current head and makes the next record of the chunk the
    /// head. Returns `IO` when the record is not in memory yet; the caller
    /// waits for the completion and calls `advance` again.
    fn advance(
        &mut self,
        key_len: usize,
        key_info: &[KeyInfo],
        comparators: &[Option<SortComparator>],
    ) -> Result<ChunkNextResult> {
        self.head = None;
        loop {
            if let Some(read) = &self.pending_read {
                if let Some(err) = read.get_error() {
                    return Err(err.into());
                }
                if !read.finished() {
                    return Ok(ChunkNextResult::IO(read.clone()));
                }
                self.pending_read = None;
            }

            let capacity = self.buffer.read().len();
            if self.parse_pos >= capacity / 2 {
                self.compact();
            }

            let buffer_len = self.buffer_len();
            let at_eof = self.bytes_read() == self.chunk_size;
            let parsed = {
                let buffer = self.buffer.read();
                Self::parse_record(&buffer[self.parse_pos..buffer_len], at_eof)?
            };
            match parsed {
                Some((size_len, record_len)) => {
                    let start = self.parse_pos + size_len;
                    self.head = Some(start..start + record_len);
                    self.parse_pos = start + record_len;
                    self.decode_head_keys(key_len, key_info, comparators)?;
                    if !at_eof && self.buffer_len() <= capacity / 2 {
                        self.read(None)?;
                    }
                    return Ok(ChunkNextResult::Done(true));
                }
                None if at_eof => {
                    turso_assert!(
                        self.parse_pos == buffer_len,
                        "sorter chunk ends with an incomplete record"
                    );
                    return Ok(ChunkNextResult::Done(false));
                }
                None => {
                    self.compact();
                    let read = self.read(None)?;
                    turso_assert!(
                        read.is_some(),
                        "sorter chunk read buffer is too small for a record"
                    );
                }
            }
        }
    }

    /// Returns the size varint length and payload length of the record at
    /// the start of `bytes`, or None when the record is not complete yet.
    fn parse_record(bytes: &[u8], at_eof: bool) -> Result<Option<(usize, usize)>> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let (record_len, size_len) = match read_varint(bytes) {
            Ok(parsed) => parsed,
            Err(LimboError::Corrupt(_)) if !at_eof && bytes.len() < 9 => return Ok(None),
            Err(e) => return Err(e),
        };
        let record_len = record_len as usize;
        if record_len > bytes.len() - size_len {
            if at_eof {
                crate::bail_corrupt_error!("Incomplete record in sorter chunk");
            }
            return Ok(None);
        }
        Ok(Some((size_len, record_len)))
    }

    fn decode_head_keys(
        &mut self,
        key_len: usize,
        key_info: &[KeyInfo],
        comparators: &[Option<SortComparator>],
    ) -> Result<()> {
        let buffer = self.buffer.read();
        let head = &buffer[self.head.clone().expect("head was just parsed")];
        let mut values = ValueIterator::new(head)?;
        self.head_keys.clear();
        for _ in 0..key_len {
            let value = match values.next() {
                Some(Ok(value)) => value,
                Some(Err(e)) => return Err(e),
                None => crate::bail_corrupt_error!("Not enough columns in record"),
            };
            // SAFETY: the value points into `buffer`, whose allocation never
            // moves, and the head's bytes stay untouched until the next
            // `advance` call clears `head_keys`.
            let value: ValueRef<'static> = unsafe { std::mem::transmute(value) };
            self.head_keys
                .push_within_capacity(value)
                .expect("head key vector was preallocated");
        }
        let (norm_key, norm_settled) = normalized_key(&self.head_keys, key_info, comparators);
        self.head_norm_key = norm_key;
        self.head_norm_settled = norm_settled;
        Ok(())
    }

    /// Moves the unparsed tail of the buffer to the front. Only valid while
    /// no read is in flight, since a read appends at `buffer_len`.
    fn compact(&mut self) {
        turso_assert!(
            self.pending_read.is_none(),
            "sorter chunk compacted while a read is in flight"
        );
        let buffer_len = self.buffer_len();
        let mut buffer = self.buffer.write();
        buffer.copy_within(self.parse_pos..buffer_len, 0);
        self.set_buffer_len(buffer_len - self.parse_pos);
        self.parse_pos = 0;
    }

    /// Issues an async read that appends more of the chunk file to the
    /// buffer, up to `min(free_buffer_space, remaining_chunk_bytes)` bytes.
    /// Returns `None` if there's no room in the buffer or no data left to
    /// read. The read is added to `group`, when given, before it is submitted,
    /// and is remembered in `pending_read` until `advance` sees it finish.
    fn read(&mut self, group: Option<&mut CompletionGroup>) -> Result<Option<Completion>> {
        let free_buffer_space = self.buffer.read().len() - self.buffer_len();
        let remaining_chunk_bytes = self.chunk_size - self.bytes_read();
        let read_buffer_size = free_buffer_space.min(remaining_chunk_bytes);

        if read_buffer_size == 0 {
            return Ok(None);
        }

        let read_buffer = Buffer::new_temporary(read_buffer_size);
        let read_buffer_ref = Arc::new(read_buffer);

        let stored_buffer_copy = self.buffer.clone();
        let stored_buffer_len_copy = self.buffer_len.clone();
        let total_bytes_read_copy = self.total_bytes_read.clone();
        let read_complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                return None;
            };
            let read_buf = buf.as_slice();

            let bytes_read = bytes_read as usize;
            let mut stored_buf_ref = stored_buffer_copy.write();
            let stored_buf = stored_buf_ref.as_mut_slice();
            let mut stored_buf_len = stored_buffer_len_copy.load(atomic::Ordering::SeqCst);

            stored_buf[stored_buf_len..stored_buf_len + bytes_read]
                .copy_from_slice(&read_buf[..bytes_read]);
            stored_buf_len += bytes_read;

            stored_buffer_len_copy.store(stored_buf_len, atomic::Ordering::SeqCst);
            total_bytes_read_copy.fetch_add(bytes_read, atomic::Ordering::SeqCst);
            None
        });

        let c = Completion::new_read(read_buffer_ref, read_complete);
        if let Some(group) = group {
            group.add(&c);
        }
        let c = self
            .file
            .pread(self.start_offset + self.bytes_read() as u64, c)?;
        self.pending_read = Some(c.clone());
        Ok(Some(c))
    }

    fn write(
        &mut self,
        records: &[NonNull<ArenaSortableRecord>],
        record_size_lengths: Vec<usize>,
        chunk_size: usize,
    ) -> Result<Completion> {
        turso_assert_eq!(*self.io_state.read(), SortedChunkIOState::None);
        *self.io_state.write() = SortedChunkIOState::WaitingForWrite;
        self.chunk_size = chunk_size;

        let buffer = Buffer::new_temporary(self.chunk_size);

        let mut buf_pos = 0;
        let buf = buffer.as_mut_slice();
        for (ptr, size_len) in records.iter().zip(record_size_lengths) {
            // SAFETY: All pointers are valid (arena not reset).
            let payload = unsafe { ptr.as_ref().payload() };
            // Write the record size varint.
            write_varint(&mut buf[buf_pos..buf_pos + size_len], payload.len() as u64);
            buf_pos += size_len;
            // Write the record payload.
            buf[buf_pos..buf_pos + payload.len()].copy_from_slice(payload);
            buf_pos += payload.len();
        }

        let buffer_ref = Arc::new(buffer);

        let buffer_ref_copy = buffer_ref.clone();
        let chunk_io_state_copy = self.io_state.clone();
        let write_complete = Box::new(move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                *chunk_io_state_copy.write() = SortedChunkIOState::WriteError;
                return;
            };
            let buf_len = buffer_ref_copy.len();
            if bytes_written < buf_len as i32 {
                tracing::error!("wrote({bytes_written}) less than expected({buf_len})");
                *chunk_io_state_copy.write() = SortedChunkIOState::WriteError;
            } else {
                *chunk_io_state_copy.write() = SortedChunkIOState::WriteComplete;
            }
        });

        let c = Completion::new_write(write_complete);
        let c = self.file.pwrite(self.start_offset, buffer_ref, c)?;
        Ok(c)
    }
}

/// Record for in-memory sorting. All data lives in the arena, so no Drop is needed.
struct ArenaSortableRecord {
    /// Payload bytes in arena. Using NonNull avoids lifetime issues with
    /// self-referential struct (key_values points into this payload).
    payload: NonNull<[u8]>,
    /// Pre-computed key values in arena. Points into `payload`.
    key_values: NonNull<[ValueRef<'static>]>,
    /// Shared KeyInfo owned by Sorter. Avoids Rc refcount overhead that would
    /// leak when arena.reset() skips Drop.
    index_key_info: NonNull<[KeyInfo]>,
    /// Shared comparators owned by Sorter. Same safety model as index_key_info.
    comparators: NonNull<[Option<SortComparator>]>,
    /// Encoding of the leading key columns; see [normalized_key].
    norm_key: NormKey,
    /// Number of leading key columns settled by `norm_key`.
    norm_settled: u8,
}

impl ArenaSortableRecord {
    fn new(
        arena: &Bump,
        record: &ImmutableRecord,
        key_len: usize,
        index_key_info: &[KeyInfo],
        comparators: &[Option<SortComparator>],
    ) -> Result<Self> {
        let payload = arena.try_alloc_slice_copy(record.get_payload())?;

        let mut payload_iter = ValueIterator::new(payload)?;

        let mut key_values = bumpalo::collections::Vec::new_in(arena);
        key_values.try_reserve(key_len)?;
        for _ in 0..key_len {
            let value = match payload_iter.next() {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => crate::bail_corrupt_error!("Not enough columns in record"),
            };
            // SAFETY: value borrows from payload which is in the arena and outlives this struct.
            let value: ValueRef<'static> = unsafe { std::mem::transmute(value) };
            key_values.push(value);
        }

        let key_values = key_values.into_bump_slice();
        let (norm_key, norm_settled) = normalized_key(key_values, index_key_info, comparators);
        Ok(Self {
            payload: NonNull::from(payload),
            key_values: NonNull::from(key_values),
            index_key_info: NonNull::from(index_key_info),
            comparators: NonNull::from(comparators),
            norm_key,
            norm_settled,
        })
    }

    #[inline]
    const fn key_values(&self) -> &[ValueRef<'static>] {
        // SAFETY: valid from construction, arena not reset
        unsafe { self.key_values.as_ref() }
    }

    #[inline]
    const fn payload(&self) -> &[u8] {
        // SAFETY: valid from construction, arena not reset
        unsafe { self.payload.as_ref() }
    }

    /// Create an ImmutableRecord by copying payload bytes out of the arena.
    fn to_immutable_record(&self) -> Result<ImmutableRecord> {
        let payload = self.payload();
        let mut record = ImmutableRecord::new(payload.len())?;
        record.start_serialization(payload)?;
        Ok(record)
    }
}

impl ArenaSortableRecord {
    /// Full key comparison from column `from` on; only reached when the
    /// normalized keys cannot decide.
    fn full_cmp(&self, other: &Self, from: usize) -> Ordering {
        // SAFETY: index_key_info and comparators point to Sorter-owned data that outlives all records.
        let index_key_info = unsafe { self.index_key_info.as_ref() };
        let comparators = unsafe { self.comparators.as_ref() };
        cmp_key_columns(
            self.key_values(),
            other.key_values(),
            index_key_info,
            comparators,
            from,
        )
    }
}

impl Ord for ArenaSortableRecord {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match cmp_normalized(
            self.norm_key,
            self.norm_settled,
            other.norm_key,
            other.norm_settled,
            self.key_values().len(),
        ) {
            Ok(ord) => ord,
            Err(from) => self.full_cmp(other, from),
        }
    }
}

impl PartialOrd for ArenaSortableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ArenaSortableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ArenaSortableRecord {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum SortedChunkIOState {
    WaitingForWrite,
    WriteComplete,
    WriteError,
    None,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;
    use crate::types::{ImmutableRecord, Value, ValueRef, ValueType};
    use crate::util::IOExt;
    use crate::PlatformIO;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    fn get_seed() -> u64 {
        std::env::var("SEED").map_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            |v| {
                v.parse()
                    .expect("Failed to parse SEED environment variable as u64")
            },
        ) as u64
    }

    #[test]
    fn fuzz_normalized_key_invariant() {
        use crate::types::AsValueRef;
        use turso_parser::ast::NullsOrder;
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Values chosen to stress every branch: type boundaries, f64 exactness
        // limits, shared prefixes, embedded NULs, lengths straddling 7 bytes.
        let gen_value = |rng: &mut ChaCha8Rng| -> Value {
            match rng.next_u64() % 10 {
                0 => Value::Null,
                1 => Value::from_i64(rng.next_u64() as i64),
                2 => Value::from_i64((rng.next_u64() % 100) as i64 - 50),
                3 => {
                    // Integers around the 2^49..2^53 exactness boundaries.
                    let base = 1i64 << (48 + rng.next_u64() % 8);
                    Value::from_i64(base + (rng.next_u64() % 5) as i64 - 2)
                }
                4 => {
                    let numerator = rng.next_u64() as f64;
                    let denominator = (rng.next_u64() as f64).abs().max(1.0);
                    Value::from_f64(numerator / denominator)
                }
                5 => Value::from_f64(if rng.next_u64() % 2 == 0 { 0.0 } else { -0.0 }),
                6..=8 => {
                    let alphabet = [b'a', b'b', b'\0'];
                    let len = (rng.next_u64() % 10) as usize;
                    let s: String = (0..len)
                        .map(|_| alphabet[(rng.next_u64() % 3) as usize] as char)
                        .collect();
                    Value::build_text(s)
                }
                _ => {
                    let len = (rng.next_u64() % 10) as usize;
                    let mut blob = try_vec![0u8; len].unwrap();
                    rng.fill_bytes(&mut blob);
                    Value::Blob(blob)
                }
            }
        };

        let gen_key = |rng: &mut ChaCha8Rng| KeyInfo {
            sort_order: if rng.next_u64() % 2 == 0 {
                SortOrder::Asc
            } else {
                SortOrder::Desc
            },
            collation: CollationSeq::Binary,
            nulls_order: match rng.next_u64() % 3 {
                0 => None,
                1 => Some(NullsOrder::First),
                _ => Some(NullsOrder::Last),
            },
        };

        for _ in 0..200_000 {
            // Half the iterations use a two-column key so both encoded
            // columns and the settled-column logic are covered.
            let ncols = 1 + (rng.next_u64() % 2) as usize;
            // Fixed-size arrays sliced to `ncols`: the crate's `Vec` alias is
            // allocator-parameterized under the nightly cfg and has no
            // `FromIterator`, so `collect()` into it would not compile.
            let va = [gen_value(&mut rng), gen_value(&mut rng)];
            let vb = [gen_value(&mut rng), gen_value(&mut rng)];
            let keys = [gen_key(&mut rng), gen_key(&mut rng)];
            let comparators: [Option<SortComparator>; 2] = [None, None];
            let ra = [va[0].as_value_ref(), va[1].as_value_ref()];
            let rb = [vb[0].as_value_ref(), vb[1].as_value_ref()];

            let (norm_a, settled_a) =
                normalized_key(&ra[..ncols], &keys[..ncols], &comparators[..ncols]);
            let (norm_b, settled_b) =
                normalized_key(&rb[..ncols], &keys[..ncols], &comparators[..ncols]);
            let reference =
                cmp_key_columns(&ra[..ncols], &rb[..ncols], &keys[..ncols], &comparators, 0);

            match cmp_normalized(norm_a, settled_a, norm_b, settled_b, ncols) {
                Ok(ord) => {
                    // A decided order must match the reference exactly: it may
                    // never contradict it, and it may never separate keys the
                    // reference deems equal (that would split GROUP BY groups).
                    assert_eq!(
                        ord, reference,
                        "normalized order must match reference: {va:?} vs {vb:?} keys {keys:?}"
                    );
                }
                Err(from) => {
                    // Every column the caller is told to skip must really be equal.
                    for col in 0..from {
                        assert_eq!(
                            cmp_in_column(&ra[col], &rb[col], &keys[col]),
                            Ordering::Equal,
                            "skipped column {col} must be equal: {va:?} vs {vb:?} keys {keys:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn fuzz_external_sort() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let io = Arc::new(PlatformIO::new().unwrap());

        let attempts = 8;
        for _ in 0..attempts {
            let mut sorter = Sorter::new(
                &[SortOrder::Asc],
                try_vec![CollationSeq::Binary].unwrap(),
                try_vec![None].unwrap(),
                try_vec![None].unwrap(),
                256,
                64,
                io.clone(),
                crate::TempStore::Default,
            )
            .unwrap();

            let num_records = 1000 + rng.next_u64() % 2000;
            let num_records = num_records as i64;

            let num_values = 1 + rng.next_u64() % 4;
            let value_types = generate_value_types(&mut rng, num_values as usize);

            let mut initial_records = Vec::with_capacity(num_records as usize);
            for i in (0..num_records).rev() {
                let mut values = try_vec![Value::from_i64(i)].unwrap();
                values.append(&mut generate_values(&mut rng, &value_types));
                let record = ImmutableRecord::from_values(&values, values.len()).unwrap();

                io.block(|| sorter.insert(&record))
                    .expect("Failed to insert the record");
                initial_records.push(record);
            }

            io.block(|| sorter.sort())
                .expect("Failed to sort the records");

            assert!(!sorter.is_empty());
            assert!(!sorter.chunks.is_empty());

            for i in 0..num_records {
                assert!(sorter.has_more());
                let record = sorter.record().unwrap();
                assert_eq!(record.get_values().unwrap()[0], ValueRef::from_i64(i));
                // Check that the record remained unchanged after sorting.
                assert_eq!(record, &initial_records[(num_records - i - 1) as usize]);

                io.block(|| sorter.next())
                    .expect("Failed to get the next record");
            }
            assert!(!sorter.has_more());
        }
    }

    fn generate_value_types<R: RngCore>(rng: &mut R, num_values: usize) -> Vec<ValueType> {
        let mut value_types = <Vec<ValueType> as TursoVecExt<ValueType>>::with_capacity(num_values);

        for _ in 0..num_values {
            let value_type: ValueType = match rng.next_u64() % 4 {
                0 => ValueType::Integer,
                1 => ValueType::Float,
                2 => ValueType::Blob,
                3 => ValueType::Null,
                _ => unreachable!(),
            };
            value_types.push(value_type);
        }

        value_types
    }

    fn generate_values<R: RngCore>(rng: &mut R, value_types: &[ValueType]) -> Vec<Value> {
        let mut values = <Vec<Value> as TursoVecExt<Value>>::with_capacity(value_types.len());
        for value_type in value_types {
            let value = match value_type {
                ValueType::Integer => Value::from_i64(rng.next_u64() as i64),
                ValueType::Float => {
                    let numerator = rng.next_u64() as f64;
                    let denominator = rng.next_u64() as f64;
                    Value::from_f64(numerator / denominator)
                }
                ValueType::Blob => {
                    let mut blob = <Vec<u8> as TursoVecExt<u8>>::with_capacity(
                        (rng.next_u64() % 2047 + 1) as usize,
                    );
                    rng.fill_bytes(&mut blob);
                    Value::Blob(blob)
                }
                ValueType::Null => Value::Null,
                _ => unreachable!(),
            };
            values.push(value);
        }
        values
    }

    fn assert_secondary_key_sort(
        second_order: SortOrder,
        second_nulls: Option<turso_parser::ast::NullsOrder>,
        seconds: &[Value],
        expected: &[ValueRef],
    ) {
        let io = Arc::new(PlatformIO::new().unwrap());
        let mut sorter = Sorter::new(
            &[SortOrder::Asc, second_order],
            try_vec![CollationSeq::Binary, CollationSeq::Binary].unwrap(),
            try_vec![None, second_nulls].unwrap(),
            try_vec![None, None].unwrap(),
            1 << 20,
            64,
            io.clone(),
            crate::TempStore::Default,
        )
        .unwrap();

        for second in seconds {
            let values = try_vec![Value::from_i64(1), second.clone()].unwrap();
            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
            io.block(|| sorter.insert(&record))
                .expect("Failed to insert the record");
        }

        io.block(|| sorter.sort())
            .expect("Failed to sort the records");
        assert!(sorter.chunks.is_empty());

        let mut idx = 0;
        while sorter.has_more() {
            {
                let record = sorter.record().unwrap();
                let vals = record.get_values().unwrap();
                assert_eq!(vals[0], ValueRef::from_i64(1));
                assert_eq!(vals[1], expected[idx]);
            }
            idx += 1;
            io.block(|| sorter.next())
                .expect("Failed to get the next record");
        }
        assert_eq!(idx, expected.len());
    }

    #[test]
    fn spilled_sort_orders_secondary_key_across_chunks() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let mut sorter = Sorter::new(
            &[SortOrder::Asc, SortOrder::Asc],
            try_vec![CollationSeq::Binary, CollationSeq::Binary].unwrap(),
            try_vec![None, None].unwrap(),
            try_vec![None, None].unwrap(),
            // Tiny buffer so the sorter spills to multiple chunk files.
            256,
            64,
            io.clone(),
            crate::TempStore::Default,
        )
        .unwrap();

        let n = 200;
        // Equal first key, ascending second key on insert.
        for x in 0..n {
            let values = try_vec![Value::from_i64(1), Value::from_i64(x)].unwrap();
            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
            io.block(|| sorter.insert(&record))
                .expect("Failed to insert the record");
        }

        io.block(|| sorter.sort())
            .expect("Failed to sort the records");
        assert!(
            !sorter.chunks.is_empty(),
            "test requires the sorter to have spilled to chunks"
        );

        let mut idx = 0;
        while sorter.has_more() {
            {
                let record = sorter.record().unwrap();
                let vals = record.get_values().unwrap();
                assert_eq!(vals[0], ValueRef::from_i64(1));
                assert_eq!(
                    vals[1],
                    ValueRef::from_i64(idx),
                    "secondary key out of order at position {idx}"
                );
            }
            idx += 1;
            io.block(|| sorter.next())
                .expect("Failed to get the next record");
        }
        assert_eq!(idx, n);
    }

    #[test]
    fn spilled_sort_places_nulls_last_across_chunks() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let mut sorter = Sorter::new(
            &[SortOrder::Asc, SortOrder::Asc],
            try_vec![CollationSeq::Binary, CollationSeq::Binary].unwrap(),
            try_vec![None, Some(turso_parser::ast::NullsOrder::Last)].unwrap(),
            try_vec![None, None].unwrap(),
            256,
            64,
            io.clone(),
            crate::TempStore::Default,
        )
        .unwrap();

        let n = 200;
        for x in 0..n {
            let second = if x % 2 == 0 {
                Value::Null
            } else {
                Value::from_i64(x)
            };
            let values = try_vec![Value::from_i64(1), second].unwrap();
            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
            io.block(|| sorter.insert(&record)).unwrap();
        }

        io.block(|| sorter.sort()).unwrap();
        assert!(
            !sorter.chunks.is_empty(),
            "test requires the sorter to have spilled to chunks"
        );

        let mut idx = 0;
        let mut prev = None;
        while sorter.has_more() {
            {
                let record = sorter.record().unwrap();
                let vals = record.get_values().unwrap();
                assert_eq!(vals[0], ValueRef::from_i64(1));
                match vals[1] {
                    ValueRef::Null => {
                        assert!(idx >= n / 2, "NULL emitted before all non-NULL values");
                    }
                    v => {
                        assert!(idx < n / 2, "non-NULL value {v:?} emitted after NULL block");
                        let current = match v {
                            ValueRef::Numeric(crate::numeric::Numeric::Integer(i)) => i,
                            other => panic!("unexpected value {other:?}"),
                        };
                        if let Some(prev) = prev {
                            assert!(current > prev);
                        }
                        prev = Some(current);
                    }
                }
            }
            idx += 1;
            io.block(|| sorter.next()).unwrap();
        }
        assert_eq!(idx, n);
    }

    #[test]
    fn in_memory_sort_applies_desc_on_secondary_key() {
        let seconds = try_vec![
            Value::from_i64(10),
            Value::from_i64(40),
            Value::from_i64(20),
            Value::from_i64(30)
        ]
        .unwrap();
        assert_secondary_key_sort(
            SortOrder::Desc,
            None,
            &seconds,
            &[
                ValueRef::from_i64(40),
                ValueRef::from_i64(30),
                ValueRef::from_i64(20),
                ValueRef::from_i64(10),
            ],
        );
    }

    #[test]
    fn in_memory_sort_places_nulls_last_on_desc_secondary_key() {
        let seconds = try_vec![
            Value::Null,
            Value::from_i64(10),
            Value::Null,
            Value::from_i64(20)
        ]
        .unwrap();
        assert_secondary_key_sort(
            SortOrder::Desc,
            Some(turso_parser::ast::NullsOrder::Last),
            &seconds,
            &[
                ValueRef::from_i64(20),
                ValueRef::from_i64(10),
                ValueRef::Null,
                ValueRef::Null,
            ],
        );
    }
}
