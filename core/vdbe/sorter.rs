use crate::types::IOResultOr;
use crate::{turso_assert, turso_assert_eq};
use branches::mark_unlikely;
use turso_parser::ast::SortOrder;

use crate::sync::RwLock;
use crate::sync::{atomic, Arc};
use bumpalo::Bump;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd, Reverse};
use std::ptr::NonNull;
use std::rc::Rc;

use crate::alloc::vec;
use crate::alloc::*;
use crate::io::TempFile;
use crate::types::{cmp_in_column, cmp_with_sort, IOCompletions, ValueIterator};
use crate::vdbe::adaptive_sort::{adaptive_sort, cache_at, AdaptiveSortItem, CACHE_LEN};
use crate::vdbe::sort_key::{conditioned_key_estimate, encode_sort_key, keys_are_byte_orderable};
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

/// Bit position of the 3-bit class rank in a normalized key.
const NORM_CLASS_SHIFT: u32 = 61;

/// Order-preserving 64-bit prefix of the first sort-key column.
///
/// Layout: 3-bit class rank | 61-bit payload. Class ranks follow the SQL type
/// ordering (NULL < numeric < text < blob), with NULL remapped above blob when
/// the effective NULLS placement requires it. The whole key is bit-inverted for
/// DESC so a plain `u64` comparison applies the sort direction.
///
/// Invariant: `norm < other_norm` implies the full key comparison orders this
/// record first, so the sort comparator only falls back to the full (collation
/// and comparator aware) comparison when two normalized keys are equal. The
/// returned `decisive` flag is true when equal normalized keys additionally
/// prove the full keys are equal, letting the comparator skip the fallback.
fn normalized_first_key(
    values: &[ValueRef<'_>],
    key_info: &[KeyInfo],
    comparators: &[Option<SortComparator>],
) -> (u64, bool) {
    use crate::numeric::Numeric;
    let (Some(value), Some(key)) = (values.first(), key_info.first()) else {
        return (0, false);
    };
    if comparators.first().is_some_and(|c| c.is_some()) {
        // Custom ordering: the normalized key cannot mirror it.
        return (0, false);
    }
    let (norm, mut decisive) = match value {
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
    decisive &= values.len() == 1 && key_info.len() == 1;
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
    /// Pointers to records allocated in the arena, each with the sort tag of
    /// its record next to it (see [SortItem]). Sorting moves only these
    /// 16-byte items, which prevents high memmove costs during sorting.
    /// SAFETY: These pointers are valid as long as the arena hasn't been reset.
    records: Vec<SortItem>,
    /// True when every key column compares by bytes, so a batch can be sorted
    /// with conditioned keys and the adaptive sort. Otherwise every batch is
    /// sorted with the value comparison.
    byte_orderable: bool,
    /// Sort path to use for every batch instead of the one chosen from a
    /// sample; only set by tests and benchmarks.
    sort_path_override: Option<SortPath>,
    /// Reused while building the sortable record of each inserted record and
    /// while conditioning keys.
    scratch: KeyScratch,
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
    /// The heap of records consumed from the chunks and their corresponding chunk index.
    /// The chunk index breaks ties so equal keys keep their insertion order across chunks.
    chunk_heap: BinaryHeap<Reverse<(Box<BoxedSortableRecord>, usize)>>,
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
        let index_key_info: Vec<KeyInfo> = order
            .iter()
            .zip(collations)
            .zip(nulls_orders)
            .map(|((order, collation), nulls)| KeyInfo {
                sort_order: *order,
                collation,
                nulls_order: nulls,
            })
            .try_collect()?;
        let byte_orderable = keys_are_byte_orderable(&index_key_info, &comparators);
        let this = Self {
            arena: Bump::new(),
            records: vec![],
            byte_orderable,
            sort_path_override: None,
            scratch: KeyScratch {
                values: TursoAllocExt::new(),
                bytes: TursoAllocExt::new(),
            },
            current: None,
            key_len: order.len(),
            index_key_info: Rc::new(index_key_info),
            comparators: Rc::new(comparators),
            chunks: vec![],
            chunk_heap: TursoAllocExt::new(),
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
                        self.sort_records()?;
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
                Some(item) => {
                    // SAFETY: the record pointer is valid - arena hasn't been reset yet.
                    let arena_record = unsafe { item.record.as_ref() };
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
        } else {
            // Serve from sorted chunk files
            match return_if_io!(self.next_from_chunk_heap()) {
                Some(boxed_record) => {
                    if let Some(ref error) = boxed_record.deserialization_error {
                        return Err(error.clone().into());
                    }
                    let payload = boxed_record.record.get_payload();
                    match &mut self.current {
                        Some(record) => {
                            record.invalidate();
                            record.start_serialization(payload)?;
                        }
                        None => {
                            self.current = Some(boxed_record.record);
                        }
                    }
                }
                None => self.current = None,
            }
        }
        Ok(IOResult::Done(()))
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
                    let (sortable_record, norm_key) = ArenaSortableRecord::new(
                        &self.arena,
                        record,
                        self.key_len,
                        &self.index_key_info,
                        &self.comparators,
                        &mut self.scratch,
                    )?;
                    // The conditioned key is only built when the adaptive sort is
                    // chosen for the batch, but it is counted now so a spill happens
                    // at the same memory bound either way.
                    let key_size = if self.byte_orderable {
                        conditioned_key_estimate(sortable_record.key_values())
                    } else {
                        0
                    };
                    let record_ref = self.arena.try_alloc(sortable_record)?;
                    // SAFETY: try_alloc returns a valid, aligned, non-null pointer.
                    self.records.try_push(SortItem {
                        record: NonNull::from(record_ref),
                        tag: norm_key,
                    })?;
                    self.current_buffer_size += payload_size + key_size;
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
                // Make sure all chunks read at least one record into their buffer.
                turso_assert!(
                    !self.chunks.iter().any(|chunk| matches!(
                        *chunk.io_state.read(),
                        SortedChunkIOState::WaitingForRead
                    )),
                    "chunks should have been read"
                );
                self.chunk_heap.try_reserve(self.chunks.len())?;
                // TODO: blocking will be unnecessary here with IO completions
                let mut group = CompletionGroup::new(|_| {});
                for chunk_idx in 0..self.chunks.len() {
                    self.push_to_chunk_heap(chunk_idx, Some(&mut group))?;
                }
                self.init_chunk_heap_state = InitChunkHeapState::Start;
                let completion = group.build();
                if completion.finished() {
                    Ok(IOResult::Done(()))
                } else {
                    io_yield_one!(completion);
                }
            }
        }
    }

    /// Returns the next record from the chunk heap in sorted order.
    ///
    /// The heap contains at most one record per chunk. When we pop a record, we try to refill
    /// from that chunk. If IO is needed, we store it in `pending_completion` and wait for it
    /// on the next call before popping again - this ensures all non-exhausted chunks have
    /// a record in the heap before we decide which is smallest.
    fn next_from_chunk_heap(&mut self) -> IOResultOr<Option<Box<BoxedSortableRecord>>> {
        // If there is a pending IO, we must wait for it before popping from the heap,
        // otherwise we might return records out of order.
        while let Some((completion, chunk_idx)) = self.pending_completion.take() {
            if !completion.succeeded() {
                // IO not complete - put it back and yield
                self.pending_completion = Some((completion.clone(), chunk_idx));
                return Ok(IOResult::IO(IOCompletions(completion)));
            }
            // IO completed - push result to heap and retry
            if let Some(c) = self.push_to_chunk_heap(chunk_idx, None)? {
                self.pending_completion = Some((c, chunk_idx));
            }
        }

        // No pending IO - safe to pop from heap
        if let Some(Reverse((next_record, chunk_idx))) = self.chunk_heap.pop() {
            if let Some(c) = self.push_to_chunk_heap(chunk_idx, None)? {
                self.pending_completion = Some((c, chunk_idx));
            }
            return Ok(IOResult::Done(Some(next_record)));
        }

        // Heap empty and no pending IO - sorter exhausted
        Ok(IOResult::Done(None))
    }

    /// Pushes the next record of chunk `chunk_idx` onto the heap. If the
    /// chunk needs a read first, the read is added to `group` when given,
    /// and its completion is returned.
    fn push_to_chunk_heap(
        &mut self,
        chunk_idx: usize,
        group: Option<&mut CompletionGroup>,
    ) -> Result<Option<Completion>> {
        let chunk = &mut self.chunks[chunk_idx];

        match chunk.next(group)? {
            ChunkNextResult::Done(Some(record)) => {
                self.chunk_heap.try_push(Reverse((
                    Box::new(BoxedSortableRecord::new(
                        record,
                        self.key_len,
                        self.index_key_info.clone(),
                        self.comparators.clone(),
                    )?),
                    chunk_idx,
                )))?;
                Ok(None)
            }
            ChunkNextResult::Done(None) => Ok(None),
            ChunkNextResult::IO(io) => Ok(Some(io)),
        }
    }

    fn flush(&mut self) -> Result<Option<Completion>> {
        if self.records.is_empty() {
            // Dummy completion to not complicate logic handling
            return Ok(None);
        }

        self.sort_records()?;

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
        for item in self.records.iter() {
            let record_size = unsafe { item.record.as_ref().payload().len() };
            let size_len = varint_len(record_size as u64);
            // Enough space was preallocated to `push` instead of `try_push`
            record_size_lengths.push(size_len);
            chunk_size += size_len + record_size;
        }

        let mut chunk = SortedChunk::new(chunk_file, self.next_chunk_offset, chunk_buffer_size)?;
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

    /// Orders the in-memory records ascending, keeping insertion order for
    /// equal keys.
    fn sort_records(&mut self) -> Result<()> {
        match self.choose_sort_path() {
            SortPath::Comparison => {
                self.records.sort_by(compare_items);
                Ok(())
            }
            SortPath::Adaptive => {
                self.condition_keys()?;
                adaptive_sort(self.records.as_mut_slice())
            }
        }
    }

    /// Picks the sort for the batch from a sample of its records.
    ///
    /// The comparison sort settles most pairs on the normalized keys in the
    /// item array and finishes ordered input in one pass, so it wins when the
    /// input is already in order or when the normalized keys rarely tie. The
    /// adaptive sort pays for key conditioning up front and wins when many
    /// pairs share a normalized key but differ later: long shared prefixes,
    /// or a first column with few distinct values ahead of more columns.
    /// Equal keys do not count as such ties, because the comparison sort
    /// settles them as fast as the adaptive sort does.
    fn choose_sort_path(&self) -> SortPath {
        if let Some(path) = self.sort_path_override {
            return path;
        }
        let len = self.records.len();
        if !self.byte_orderable || len < MIN_ADAPTIVE_RECORDS {
            return SortPath::Comparison;
        }
        let mut sample: [SortItem; SORT_PATH_SAMPLE] = std::array::from_fn(|i| {
            self.records[(i as u64 * len as u64 / SORT_PATH_SAMPLE as u64) as usize]
        });
        if sample
            .windows(2)
            .all(|pair| compare_items(&pair[0], &pair[1]) != Ordering::Greater)
        {
            return SortPath::Comparison;
        }
        sample.sort_unstable_by_key(|item| item.tag);
        let ties = sample
            .windows(2)
            .filter(|pair| {
                pair[0].tag == pair[1].tag && compare_items(&pair[0], &pair[1]) != Ordering::Equal
            })
            .count();
        if ties * SORT_PATH_TIE_DIVISOR >= SORT_PATH_SAMPLE {
            SortPath::Adaptive
        } else {
            SortPath::Comparison
        }
    }

    /// Builds the conditioned key of every record in the batch and turns the
    /// items into adaptive sort items.
    fn condition_keys(&mut self) -> Result<()> {
        let Self {
            records,
            arena,
            scratch,
            index_key_info,
            ..
        } = self;
        for item in records.iter_mut() {
            // SAFETY: the record lives in the arena, and this loop holds the
            // only reference to it.
            let record = unsafe { item.record.as_mut() };
            scratch.bytes.clear();
            encode_sort_key(
                record.key_values(),
                index_key_info.as_slice(),
                &mut scratch.bytes,
            )?;
            let key: &[u8] = arena.try_alloc_slice_copy(&scratch.bytes)?;
            record.key = NonNull::from(key);
            item.tag = adaptive_tag(key);
        }
        Ok(())
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn force_sort_path(&mut self, path: SortPath) {
        turso_assert!(
            self.byte_orderable || path == SortPath::Comparison,
            "keys that need the value comparison cannot take the adaptive sort"
        );
        self.sort_path_override = Some(path);
    }
}

/// The in-memory sorts the sorter can run on a batch of records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortPath {
    /// A stable merge sort over the normalized keys, with the value
    /// comparison for the pairs the normalized keys cannot settle.
    Comparison,
    /// Key conditioning followed by [adaptive_sort].
    Adaptive,
}

/// Batches smaller than this always take the comparison sort: the adaptive
/// sort's key conditioning and scratch buffers do not pay off for them.
const MIN_ADAPTIVE_RECORDS: usize = 256;
/// Records looked at when picking the sort path.
const SORT_PATH_SAMPLE: usize = 256;
/// The adaptive sort is chosen when at least one in this many neighbors of
/// the sorted sample share a normalized key but differ.
const SORT_PATH_TIE_DIVISOR: usize = 8;

/// Comparison sort order: the normalized keys in the item array first, the
/// records only for ties the normalized keys cannot settle.
fn compare_items(a: &SortItem, b: &SortItem) -> Ordering {
    match a.tag.cmp(&b.tag) {
        Ordering::Equal => {
            // SAFETY: the records live in the arena, which is not reset while items exist.
            let (a, b) = unsafe { (a.record.as_ref(), b.record.as_ref()) };
            if a.norm_decisive && b.norm_decisive {
                Ordering::Equal
            } else {
                a.full_cmp(b)
            }
        }
        order => order,
    }
}

/// One entry of the sorter's item array: the record pointer and a tag that
/// keeps the sort from following the pointer. Until a batch is sorted the
/// tag is the normalized first key (see [normalized_first_key]). When the
/// adaptive sort is chosen for the batch, [Sorter::condition_keys] turns the
/// tag into the key length and the key substring cache (see [adaptive_tag]).
#[derive(Clone, Copy)]
struct SortItem {
    record: NonNull<ArenaSortableRecord>,
    tag: u64,
}

/// Packs the key length above the two cached key bytes.
fn adaptive_tag(key: &[u8]) -> u64 {
    turso_assert!(
        (key.len() as u64) < (1u64 << 48),
        "conditioned sort keys are shorter than 2^48 bytes"
    );
    let cache = cache_at(key, 0);
    ((key.len() as u64) << 16) | ((cache[0] as u64) << 8) | cache[1] as u64
}

impl AdaptiveSortItem for SortItem {
    #[inline]
    fn key(&self) -> &[u8] {
        // SAFETY: the record lives in the arena, which is not reset while items exist.
        unsafe { self.record.as_ref() }.key()
    }

    #[inline]
    fn key_len(&self) -> usize {
        (self.tag >> 16) as usize
    }

    #[inline]
    fn cache(&self) -> [u8; CACHE_LEN] {
        [(self.tag >> 8) as u8, self.tag as u8]
    }

    #[inline]
    fn set_cache(&mut self, cache: [u8; CACHE_LEN]) {
        self.tag = (self.tag & !0xFFFF) | ((cache[0] as u64) << 8) | cache[1] as u64;
    }
}

/// Buffers reused for every inserted record (its key values) and for every
/// conditioned key.
struct KeyScratch {
    values: Vec<ValueRef<'static>>,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
enum NextState {
    Start,
    Finish,
}

/// A sorted chunk represents a portion of sorted data that has been written to disk
/// during external merge sort. When the in-memory buffer fills up, records are sorted
/// and flushed to a chunk file. During the merge phase, chunks are read back and merged
/// using a heap to produce the final sorted output.
///
/// # Buffer management
///
/// The chunk uses a fixed-size read buffer (`buffer`) to read data from disk. The buffer
/// has two relevant sizes:
/// - `buffer.len()` (capacity): The total allocated size of the buffer (fixed at creation)
/// - `buffer_len`: The amount of valid data currently in the buffer (0 to capacity)
///
/// The difference `buffer.len() - buffer_len` is the free space available for reading
/// more data from disk.
///
/// # Reading progress
///
/// - `chunk_size`: Total bytes of this chunk on disk (set when chunk is written)
/// - `total_bytes_read`: Cumulative bytes read from disk so far (0 to chunk_size)
///
/// The difference `chunk_size - total_bytes_read` is the remaining data on disk that
/// hasn't been read yet. When `total_bytes_read == chunk_size`, we've read all data.
///
/// # Record parsing
///
/// Data flows: disk -> buffer -> records -> caller
///
/// 1. `read()` fills `buffer` from disk, updates `total_bytes_read`
/// 2. `next()` parses records from `buffer` into `records` vec, updates `buffer_len`
/// 3. `next()` returns records one at a time from `records`
///
/// Incomplete records at the end of the buffer are kept (buffer compacted) until
/// more data is read to complete them.
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
    /// Amount of valid (unparsed) data in `buffer`, from index 0 to buffer_len.
    /// This is separate from buffer.len() because we reuse the same allocation.
    buffer_len: Arc<atomic::AtomicUsize>,
    /// Records parsed from the buffer, waiting to be returned by `next()`.
    /// Stored in reverse order so we can efficiently pop from the end.
    records: Vec<ImmutableRecord>,
    /// Current async IO state (None, WaitingForRead, ReadComplete, ReadEOF, etc).
    io_state: Arc<RwLock<SortedChunkIOState>>,
    /// Cumulative bytes read from disk. When this equals `chunk_size`, we've read everything.
    total_bytes_read: Arc<atomic::AtomicUsize>,
    /// State machine for the `next()` method.
    next_state: NextState,
}

enum ChunkNextResult {
    Done(Option<ImmutableRecord>),
    IO(Completion),
}

impl SortedChunk {
    fn new(file: Arc<dyn File>, start_offset: usize, buffer_size: usize) -> Result<Self> {
        Ok(Self {
            file,
            start_offset: start_offset as u64,
            chunk_size: 0,
            buffer: Arc::new(RwLock::new(try_vec![0; buffer_size]?)),
            buffer_len: Arc::new(atomic::AtomicUsize::new(0)),
            records: vec![],
            io_state: Arc::new(RwLock::new(SortedChunkIOState::None)),
            total_bytes_read: Arc::new(atomic::AtomicUsize::new(0)),
            next_state: NextState::Start,
        })
    }

    fn buffer_len(&self) -> usize {
        self.buffer_len.load(atomic::Ordering::SeqCst)
    }

    fn set_buffer_len(&self, len: usize) {
        self.buffer_len.store(len, atomic::Ordering::SeqCst);
    }

    /// Returns the next record from this chunk, or None if exhausted.
    ///
    /// May return `ChunkNextResult::IO` if async IO is needed, in which case
    /// the caller should wait for the completion and call `next()` again.
    ///
    /// Internally manages a two-phase state machine:
    /// - `Start`: Parse records from buffer, issue prefetch read if needed
    /// - `Finish`: Return the next parsed record
    fn next(&mut self, mut group: Option<&mut CompletionGroup>) -> Result<ChunkNextResult> {
        loop {
            match self.next_state {
                NextState::Start => {
                    let mut buffer_len = self.buffer_len();
                    if self.records.is_empty() && buffer_len == 0 {
                        return Ok(ChunkNextResult::Done(None));
                    }

                    if self.records.is_empty() {
                        let mut buffer_ref = self.buffer.write();
                        let buffer = buffer_ref.as_mut_slice();
                        let mut buffer_offset = 0;
                        while buffer_offset < buffer_len {
                            // Extract records from the buffer until we run out of the buffer or we hit an incomplete record.
                            let (record_size, bytes_read) =
                                match read_varint(&buffer[buffer_offset..buffer_len]) {
                                    Ok((record_size, bytes_read)) => {
                                        (record_size as usize, bytes_read)
                                    }
                                    Err(LimboError::Corrupt(_))
                                        if *self.io_state.read() != SortedChunkIOState::ReadEOF =>
                                    {
                                        // Failed to decode a partial varint.
                                        break;
                                    }
                                    Err(e) => {
                                        return Err(e);
                                    }
                                };
                            if record_size > buffer_len - (buffer_offset + bytes_read) {
                                if *self.io_state.read() == SortedChunkIOState::ReadEOF {
                                    crate::bail_corrupt_error!("Incomplete record");
                                }
                                break;
                            }
                            buffer_offset += bytes_read;

                            let mut record = ImmutableRecord::new(record_size)?;
                            record.start_serialization(
                                &buffer[buffer_offset..buffer_offset + record_size],
                            )?;
                            buffer_offset += record_size;

                            self.records.try_push(record)?;
                        }
                        if buffer_offset < buffer_len {
                            buffer.copy_within(buffer_offset..buffer_len, 0);
                            buffer_len -= buffer_offset;
                        } else {
                            buffer_len = 0;
                        }
                        self.set_buffer_len(buffer_len);

                        self.records.reverse();
                    }

                    self.next_state = NextState::Finish;
                    // Prefetch: if down to last record, try to read more data into the buffer.
                    if self.records.len() == 1
                        && *self.io_state.read() != SortedChunkIOState::ReadEOF
                    {
                        if let Some(c) = self.read(group.as_deref_mut())? {
                            if !c.succeeded() {
                                return Ok(ChunkNextResult::IO(c));
                            }
                        }
                    }
                }
                NextState::Finish => {
                    self.next_state = NextState::Start;
                    return Ok(ChunkNextResult::Done(self.records.pop()));
                }
            }
        }
    }

    /// Issues an async read to fill the buffer with more data from the chunk file.
    ///
    /// Reads up to `min(free_buffer_space, remaining_chunk_bytes)` bytes. Returns `None`
    /// if there's no room in the buffer or no data left to read (no IO issued).
    ///
    /// On completion, appends data to `buffer` and updates `buffer_len` and `total_bytes_read`.
    /// Reads more of the chunk file into the buffer. The read is added to
    /// `group`, when given, before it is submitted.
    fn read(&mut self, group: Option<&mut CompletionGroup>) -> Result<Option<Completion>> {
        let free_buffer_space = self.buffer.read().len() - self.buffer_len();
        let remaining_chunk_bytes =
            self.chunk_size - self.total_bytes_read.load(atomic::Ordering::SeqCst);
        let read_buffer_size = free_buffer_space.min(remaining_chunk_bytes);

        // If there's no room in the buffer or nothing left to read, skip the read.
        if read_buffer_size == 0 {
            if remaining_chunk_bytes == 0 {
                // No more data in the chunk file.
                *self.io_state.write() = SortedChunkIOState::ReadEOF;
            }
            return Ok(None);
        }

        *self.io_state.write() = SortedChunkIOState::WaitingForRead;

        let read_buffer = Buffer::new_temporary(read_buffer_size);
        let read_buffer_ref = Arc::new(read_buffer);

        let chunk_io_state_copy = self.io_state.clone();
        let stored_buffer_copy = self.buffer.clone();
        let stored_buffer_len_copy = self.buffer_len.clone();
        let total_bytes_read_copy = self.total_bytes_read.clone();
        let read_complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                return None;
            };
            let read_buf = buf.as_slice();

            let bytes_read = bytes_read as usize;
            if bytes_read == 0 {
                *chunk_io_state_copy.write() = SortedChunkIOState::ReadEOF;
                return None;
            }
            *chunk_io_state_copy.write() = SortedChunkIOState::ReadComplete;

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
        let c = self.file.pread(
            self.start_offset + self.total_bytes_read.load(atomic::Ordering::SeqCst) as u64,
            c,
        )?;
        Ok(Some(c))
    }

    fn write(
        &mut self,
        records: &[SortItem],
        record_size_lengths: Vec<usize>,
        chunk_size: usize,
    ) -> Result<Completion> {
        turso_assert_eq!(*self.io_state.read(), SortedChunkIOState::None);
        *self.io_state.write() = SortedChunkIOState::WaitingForWrite;
        self.chunk_size = chunk_size;

        let buffer = Buffer::new_temporary(self.chunk_size);

        let mut buf_pos = 0;
        let buf = buffer.as_mut_slice();
        for (item, size_len) in records.iter().zip(record_size_lengths) {
            // SAFETY: All pointers are valid (arena not reset).
            let payload = unsafe { item.record.as_ref().payload() };
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
    /// Conditioned sort key in arena; see [crate::vdbe::sort_key]. Empty
    /// until the batch is sorted with the adaptive sort.
    key: NonNull<[u8]>,
    /// Pre-computed key values in arena. Points into `payload`.
    key_values: NonNull<[ValueRef<'static>]>,
    /// Shared KeyInfo owned by Sorter. Avoids Rc refcount overhead that would
    /// leak when arena.reset() skips Drop.
    index_key_info: NonNull<[KeyInfo]>,
    /// Shared comparators owned by Sorter. Same safety model as index_key_info.
    comparators: NonNull<[Option<SortComparator>]>,
    /// True when equal normalized keys prove the full keys are equal.
    norm_decisive: bool,
}

impl ArenaSortableRecord {
    /// Copies the record into the arena and parses its key values. Returns
    /// the record and its normalized first key; see [normalized_first_key].
    fn new(
        arena: &Bump,
        record: &ImmutableRecord,
        key_len: usize,
        index_key_info: &[KeyInfo],
        comparators: &[Option<SortComparator>],
        scratch: &mut KeyScratch,
    ) -> Result<(Self, u64)> {
        let payload = arena.try_alloc_slice_copy(record.get_payload())?;

        let mut payload_iter = ValueIterator::new(payload)?;

        scratch.values.clear();
        scratch.values.try_reserve(key_len)?;
        for _ in 0..key_len {
            let value = match payload_iter.next() {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => crate::bail_corrupt_error!("Not enough columns in record"),
            };
            // SAFETY: value borrows from payload which is in the arena and outlives this struct.
            let value: ValueRef<'static> = unsafe { std::mem::transmute(value) };
            // Reserved for key_len values above, so this push cannot grow the vector.
            scratch.values.push(value);
        }

        let key_values: &[ValueRef<'static>] = arena.try_alloc_slice_copy(&scratch.values)?;
        scratch.values.clear();
        let (norm_key, norm_decisive) =
            normalized_first_key(key_values, index_key_info, comparators);
        let record = Self {
            payload: NonNull::from(payload),
            key: NonNull::from(&[] as &[u8]),
            key_values: NonNull::from(key_values),
            index_key_info: NonNull::from(index_key_info),
            comparators: NonNull::from(comparators),
            norm_decisive,
        };
        Ok((record, norm_key))
    }

    #[inline]
    const fn key(&self) -> &[u8] {
        // SAFETY: valid from construction, arena not reset
        unsafe { self.key.as_ref() }
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
    /// Full key comparison; only reached when the normalized keys tie.
    fn full_cmp(&self, other: &Self) -> Ordering {
        let self_values = self.key_values();
        let other_values = other.key_values();
        // SAFETY: index_key_info and comparators point to Sorter-owned data that outlives all records.
        let index_key_info = unsafe { self.index_key_info.as_ref() };
        let comparators = unsafe { self.comparators.as_ref() };

        for (i, ((&self_val, &other_val), key_info)) in self_values
            .iter()
            .zip(other_values.iter())
            .zip(index_key_info.iter())
            .enumerate()
        {
            let cmp = if let Some(Some(comparator)) = comparators.get(i) {
                let base =
                    comparator(&self_val, &other_val).expect("Memory allocation failed here");
                cmp_with_sort(base, &self_val, &other_val, key_info)
            } else {
                cmp_in_column(&self_val, &other_val, key_info)
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        Ordering::Equal
    }
}

/// Heap-allocated record for external merge sort. Used when records are read
/// back from chunk files. Normal Drop semantics apply.
struct BoxedSortableRecord {
    record: ImmutableRecord,
    key_values: Vec<ValueRef<'static>>,
    index_key_info: Rc<Vec<KeyInfo>>,
    comparators: Rc<Vec<Option<SortComparator>>>,
    deserialization_error: Option<LimboError>,
    /// Order-preserving prefix of the first key column; see [normalized_first_key].
    norm_key: u64,
    /// True when equal `norm_key`s prove the full keys are equal.
    norm_decisive: bool,
}

impl BoxedSortableRecord {
    fn new(
        record: ImmutableRecord,
        key_len: usize,
        index_key_info: Rc<Vec<KeyInfo>>,
        comparators: Rc<Vec<Option<SortComparator>>>,
    ) -> Result<Self> {
        let mut value_iterator = record.iter()?;
        let mut key_values = Vec::try_with_capacity_ext(key_len)?;
        let mut deserialization_error = None;

        for _ in 0..key_len {
            match value_iterator.next() {
                Some(Ok(value)) => {
                    // SAFETY: value points into record which lives as long as this struct
                    let value: ValueRef<'static> = unsafe { std::mem::transmute(value) };
                    key_values
                        .push_within_capacity(value)
                        .expect("sort key vector was preallocated");
                }
                Some(Err(err)) => {
                    mark_unlikely();
                    deserialization_error = Some(err);
                    break;
                }
                None => {
                    mark_unlikely();
                    deserialization_error = Some(LimboError::Corrupt(
                        "Not enough columns in record".to_string(),
                    ));
                    break;
                }
            }
        }

        let (norm_key, norm_decisive) =
            normalized_first_key(&key_values, &index_key_info, &comparators);
        Ok(Self {
            record,
            key_values,
            index_key_info,
            comparators,
            deserialization_error,
            norm_key,
            norm_decisive,
        })
    }
}

impl BoxedSortableRecord {
    /// Full key comparison; only reached when the normalized keys tie.
    fn full_cmp(&self, other: &Self) -> Ordering {
        for (i, ((&self_val, &other_val), key_info)) in self
            .key_values
            .iter()
            .zip(other.key_values.iter())
            .zip(self.index_key_info.iter())
            .enumerate()
        {
            let cmp = if let Some(Some(comparator)) = self.comparators.get(i) {
                comparator(&self_val, &other_val).expect("Memory allocation failed here")
            } else {
                match (self_val, other_val) {
                    (ValueRef::Text(left), ValueRef::Text(right)) => {
                        key_info.collation.compare_strings(&left, &right)
                    }
                    _ => self_val.partial_cmp(&other_val).unwrap_or(Ordering::Equal),
                }
            };
            let cmp = cmp_with_sort(cmp, &self_val, &other_val, key_info);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

impl Ord for BoxedSortableRecord {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        if self.deserialization_error.is_some() || other.deserialization_error.is_some() {
            return Ordering::Equal;
        }
        match self.norm_key.cmp(&other.norm_key) {
            Ordering::Equal if self.norm_decisive && other.norm_decisive => Ordering::Equal,
            Ordering::Equal => self.full_cmp(other),
            ord => ord,
        }
    }
}

impl PartialOrd for BoxedSortableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for BoxedSortableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for BoxedSortableRecord {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum SortedChunkIOState {
    WaitingForRead,
    ReadComplete,
    WaitingForWrite,
    WriteComplete,
    WriteError,
    ReadEOF,
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
            // Half the iterations use a two-column key so the multi-column path
            // is covered: the normalized key still encodes only the first
            // column, and `decisive` must never be set (else equal first
            // columns would wrongly short-circuit past the second column).
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

            let (norm_a, dec_a) =
                normalized_first_key(&ra[..ncols], &keys[..ncols], &comparators[..ncols]);
            let (norm_b, dec_b) =
                normalized_first_key(&rb[..ncols], &keys[..ncols], &comparators[..ncols]);
            // The normalized key only ever reflects the first column.
            let a = &ra[0];
            let b = &rb[0];
            let key = &keys[0];
            let reference = cmp_in_column(a, b, key);

            if ncols > 1 {
                assert!(
                    !dec_a && !dec_b,
                    "multi-column keys must never be decisive: {va:?} vs {vb:?} keys {keys:?}"
                );
            }
            match norm_a.cmp(&norm_b) {
                Ordering::Equal => {
                    if dec_a && dec_b {
                        assert_eq!(
                            reference,
                            Ordering::Equal,
                            "decisive equal norms must mean equal keys: {va:?} vs {vb:?} keys {keys:?}"
                        );
                    }
                }
                ord => {
                    // A strict normalized order must match the first-column
                    // reference exactly: it may never contradict the reference,
                    // and it may never separate keys the reference deems equal
                    // (that would split GROUP BY groups or skip a later column).
                    assert_eq!(
                        ord, reference,
                        "strict norm order must match reference: {va:?} vs {vb:?} keys {keys:?}"
                    );
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

    fn random_sort_value(rng: &mut ChaCha8Rng) -> Value {
        match rng.next_u64() % 9 {
            0 => Value::Null,
            1 => Value::from_i64(rng.next_u64() as i64),
            2 => Value::from_i64((rng.next_u64() % 8) as i64 - 4),
            3 => Value::from_f64((rng.next_u64() % 16) as f64 / 4.0 - 2.0),
            4 => Value::from_f64(
                [0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY, 9.3e18, -9.3e18]
                    [(rng.next_u64() % 6) as usize],
            ),
            5..=7 => {
                let alphabet = [b'a', b'b', b'A', b'B', b' ', b'\0'];
                let len = (rng.next_u64() % 8) as usize;
                let bytes: std::vec::Vec<u8> = (0..len)
                    .map(|_| alphabet[(rng.next_u64() % alphabet.len() as u64) as usize])
                    .collect();
                Value::build_text(String::from_utf8(bytes).unwrap())
            }
            _ => {
                let len = (rng.next_u64() % 8) as usize;
                let mut blob = try_vec![0u8; len].unwrap();
                for byte in blob.iter_mut() {
                    *byte = [0x00, 0x01, 0xFF][(rng.next_u64() % 3) as usize];
                }
                Value::Blob(blob)
            }
        }
    }

    /// The sorter must produce exactly the order of a stable sort by the
    /// value comparison, whether the records stay in memory or spill to
    /// chunk files, for every collation and NULL placement the conditioned
    /// keys cover.
    #[test]
    fn fuzz_sorter_matches_stable_value_comparison() {
        use crate::types::{compare_immutable, AsValueRef};
        use turso_parser::ast::NullsOrder;
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let io = Arc::new(PlatformIO::new().unwrap());

        for round in 0..48 {
            let ncols = 1 + (rng.next_u64() % 3) as usize;
            let mut key_info = try_vec![].unwrap();
            let mut orders = try_vec![].unwrap();
            let mut collations = try_vec![].unwrap();
            let mut nulls_orders = try_vec![].unwrap();
            let mut comparators: Vec<Option<SortComparator>> = try_vec![].unwrap();
            for _ in 0..ncols {
                let key = KeyInfo {
                    sort_order: if rng.next_u64() % 2 == 0 {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    },
                    collation: match rng.next_u64() % 3 {
                        0 => CollationSeq::Binary,
                        1 => CollationSeq::NoCase,
                        _ => CollationSeq::Rtrim,
                    },
                    nulls_order: match rng.next_u64() % 3 {
                        0 => None,
                        1 => Some(NullsOrder::First),
                        _ => Some(NullsOrder::Last),
                    },
                };
                key_info.try_push(key).unwrap();
                orders.try_push(key.sort_order).unwrap();
                collations.try_push(key.collation).unwrap();
                nulls_orders.try_push(key.nulls_order).unwrap();
                comparators.try_push(None).unwrap();
            }
            let max_buffer_size = if round % 2 == 0 { 512 } else { 1 << 24 };
            let mut sorter = Sorter::new(
                &orders,
                collations,
                nulls_orders,
                comparators,
                max_buffer_size,
                64,
                io.clone(),
                crate::TempStore::Default,
            )
            .unwrap();
            assert!(sorter.byte_orderable);
            match round % 3 {
                0 => {}
                1 => sorter.force_sort_path(SortPath::Comparison),
                _ => sorter.force_sort_path(SortPath::Adaptive),
            }

            let num_records = (rng.next_u64() % 1500) as usize;
            let mut rows: std::vec::Vec<std::vec::Vec<Value>> = std::vec::Vec::new();
            for i in 0..num_records {
                let mut values: std::vec::Vec<Value> =
                    (0..ncols).map(|_| random_sort_value(&mut rng)).collect();
                values.push(Value::from_i64(i as i64));
                let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
                io.block(|| sorter.insert(&record)).unwrap();
                rows.push(values);
            }

            let mut expected: std::vec::Vec<usize> = (0..num_records).collect();
            expected.sort_by(|a, b| {
                compare_immutable(
                    rows[*a][..ncols].iter().map(AsValueRef::as_value_ref),
                    rows[*b][..ncols].iter().map(AsValueRef::as_value_ref),
                    &key_info,
                )
            });

            io.block(|| sorter.sort()).unwrap();
            if max_buffer_size == 512 && num_records >= 128 {
                assert!(
                    !sorter.chunks.is_empty(),
                    "128 records do not fit in 512 bytes"
                );
            } else if max_buffer_size != 512 {
                assert!(sorter.chunks.is_empty());
            }
            let mut got = std::vec::Vec::new();
            while sorter.has_more() {
                let values = sorter.record().unwrap().get_values().unwrap();
                match values[ncols] {
                    ValueRef::Numeric(crate::numeric::Numeric::Integer(i)) => got.push(i as usize),
                    other => panic!("unexpected row id {other:?}"),
                }
                io.block(|| sorter.next()).unwrap();
            }
            assert_eq!(
                got, expected,
                "seed {seed} round {round}: keys {key_info:?}"
            );
        }
    }

    #[test]
    fn in_memory_sort_applies_nocase_and_rtrim_collations() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let mut sorter = Sorter::new(
            &[SortOrder::Asc, SortOrder::Desc],
            try_vec![CollationSeq::NoCase, CollationSeq::Rtrim].unwrap(),
            try_vec![None, None].unwrap(),
            try_vec![None, None].unwrap(),
            1 << 20,
            64,
            io.clone(),
            crate::TempStore::Default,
        )
        .unwrap();
        assert!(sorter.byte_orderable);
        sorter.force_sort_path(SortPath::Adaptive);
        let rows = [("b", "x  "), ("A", "y"), ("a", "x"), ("B", "z ")];
        for (i, (first, second)) in rows.iter().enumerate() {
            let values = try_vec![
                Value::build_text(first.to_string()),
                Value::build_text(second.to_string()),
                Value::from_i64(i as i64)
            ]
            .unwrap();
            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
            io.block(|| sorter.insert(&record)).unwrap();
        }
        io.block(|| sorter.sort()).unwrap();
        let mut got = std::vec::Vec::new();
        while sorter.has_more() {
            let values = sorter.record().unwrap().get_values().unwrap();
            got.push(values[2].to_owned().unwrap());
            io.block(|| sorter.next()).unwrap();
        }
        assert_eq!(
            got,
            [
                Value::from_i64(1),
                Value::from_i64(2),
                Value::from_i64(3),
                Value::from_i64(0)
            ]
        );
    }

    #[test]
    fn custom_comparator_sorts_by_value_comparison() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let by_magnitude: SortComparator = Arc::new(|a: &ValueRef, b: &ValueRef| {
            let magnitude = |value: &ValueRef| match value {
                ValueRef::Numeric(crate::numeric::Numeric::Integer(i)) => i.abs(),
                _ => 0,
            };
            Ok(magnitude(a).cmp(&magnitude(b)))
        });
        let mut sorter = Sorter::new(
            &[SortOrder::Asc],
            try_vec![CollationSeq::Binary].unwrap(),
            try_vec![None].unwrap(),
            try_vec![Some(by_magnitude)].unwrap(),
            1 << 20,
            64,
            io.clone(),
            crate::TempStore::Default,
        )
        .unwrap();
        assert!(!sorter.byte_orderable);
        for value in [-5, 3, -1, 4, -3] {
            let values = try_vec![Value::from_i64(value)].unwrap();
            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
            io.block(|| sorter.insert(&record)).unwrap();
        }
        io.block(|| sorter.sort()).unwrap();
        let mut got = std::vec::Vec::new();
        while sorter.has_more() {
            got.push(
                sorter.record().unwrap().get_values().unwrap()[0]
                    .to_owned()
                    .unwrap(),
            );
            io.block(|| sorter.next()).unwrap();
        }
        assert_eq!(
            got,
            [
                Value::from_i64(-1),
                Value::from_i64(3),
                Value::from_i64(-3),
                Value::from_i64(4),
                Value::from_i64(-5)
            ]
        );
    }

    fn sorter_with_records(
        orders: &[SortOrder],
        rows: impl Iterator<Item = Vec<Value>>,
        comparators: Vec<Option<SortComparator>>,
    ) -> Sorter {
        let io = Arc::new(PlatformIO::new().unwrap());
        let columns = orders.len();
        let mut sorter = Sorter::new(
            orders,
            try_vec![CollationSeq::Binary; columns].unwrap(),
            try_vec![None; columns].unwrap(),
            comparators,
            1 << 24,
            64,
            io.clone(),
            crate::TempStore::Default,
        )
        .unwrap();
        for values in rows {
            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
            io.block(|| sorter.insert(&record)).unwrap();
        }
        sorter
    }

    #[test]
    fn sort_path_follows_the_sample() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let count = 4 * MIN_ADAPTIVE_RECORDS;
        let no_comparators =
            |columns: usize| -> Vec<Option<SortComparator>> { try_vec![None; columns].unwrap() };
        let prefixed = |i: u64| {
            try_vec![Value::build_text(format!(
                "https://www.example.com/items/{i:08}"
            ))]
            .unwrap()
        };

        let random_ints = sorter_with_records(
            &[SortOrder::Asc],
            (0..count).map(|_| try_vec![Value::from_i64(rng.next_u64() as i64)].unwrap()),
            no_comparators(1),
        );
        assert_eq!(random_ints.choose_sort_path(), SortPath::Comparison);

        let ordered_prefixed = sorter_with_records(
            &[SortOrder::Asc],
            (0..count as u64).map(prefixed),
            no_comparators(1),
        );
        assert_eq!(ordered_prefixed.choose_sort_path(), SortPath::Comparison);

        let shuffled_prefixed = sorter_with_records(
            &[SortOrder::Asc],
            (0..count).map(|_| prefixed(rng.next_u64() % 100_000)),
            no_comparators(1),
        );
        assert_eq!(shuffled_prefixed.choose_sort_path(), SortPath::Adaptive);

        let few_then_many = sorter_with_records(
            &[SortOrder::Asc, SortOrder::Asc],
            (0..count).map(|_| {
                try_vec![
                    Value::from_i64((rng.next_u64() % 16) as i64),
                    Value::from_i64(rng.next_u64() as i64)
                ]
                .unwrap()
            }),
            no_comparators(2),
        );
        assert_eq!(few_then_many.choose_sort_path(), SortPath::Adaptive);

        let small_batch = sorter_with_records(
            &[SortOrder::Asc],
            (0..MIN_ADAPTIVE_RECORDS - 1).map(|_| prefixed(rng.next_u64() % 100_000)),
            no_comparators(1),
        );
        assert_eq!(small_batch.choose_sort_path(), SortPath::Comparison);

        let cities = ["Amsterdam", "Berlin", "Copenhagen", "Dublin"];
        let few_distinct_text = sorter_with_records(
            &[SortOrder::Asc],
            (0..count).map(|_| {
                try_vec![Value::build_text(
                    cities[(rng.next_u64() % 4) as usize].to_string()
                )]
                .unwrap()
            }),
            no_comparators(1),
        );
        assert_eq!(few_distinct_text.choose_sort_path(), SortPath::Comparison);

        let custom: SortComparator = Arc::new(|a: &ValueRef, b: &ValueRef| Ok(a.cmp(b)));
        let custom_comparator = sorter_with_records(
            &[SortOrder::Asc],
            (0..count).map(|_| prefixed(rng.next_u64() % 100_000)),
            try_vec![Some(custom)].unwrap(),
        );
        assert_eq!(custom_comparator.choose_sort_path(), SortPath::Comparison);
    }
}
