#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::array;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use strum::EnumString;
use tracing::{instrument, Level};

use parking_lot::RwLock;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::{cell::Cell, fmt, sync::Arc};

use super::buffer_pool::BufferPool;
use super::pager::{PageRef, Pager};
use super::sqlite3_ondisk::{self, checksum_wal, WalHeader, WAL_MAGIC_BE, WAL_MAGIC_LE};
use crate::fast_lock::SpinLock;
use crate::io::{clock, File, IO};
use crate::storage::database::EncryptionOrChecksum;
use crate::storage::sqlite3_ondisk::{
    begin_read_wal_frame, begin_read_wal_frame_raw, finish_read_page, prepare_wal_frame,
    write_pages_vectored, PageSize, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use crate::types::{IOCompletions, IOResult};
use crate::{
    bail_corrupt_error, io_yield_many, io_yield_one, return_if_io, turso_assert, Buffer,
    Completion, CompletionError, IOContext, LimboError, Result,
};

#[derive(Debug, Clone, Default)]
pub struct CheckpointResult {
    /// number of frames in WAL that could have been backfilled
    pub num_attempted: u64,
    /// number of frames moved successfully from WAL to db file after checkpoint
    pub num_backfilled: u64,
    pub max_frame: u64,
    /// In the case of everything backfilled, we need to hold the locks until the db
    /// file is truncated.
    maybe_guard: Option<CheckpointLocks>,
    pub db_truncate_sent: bool,
    pub db_sync_sent: bool,
}

impl Drop for CheckpointResult {
    fn drop(&mut self) {
        let _ = self.maybe_guard.take();
    }
}

impl CheckpointResult {
    pub fn new(n_frames: u64, n_ckpt: u64, max_frame: u64) -> Self {
        Self {
            num_attempted: n_frames,
            num_backfilled: n_ckpt,
            max_frame,
            maybe_guard: None,
            db_sync_sent: false,
            db_truncate_sent: false,
        }
    }

    pub const fn everything_backfilled(&self) -> bool {
        self.num_attempted == self.num_backfilled
    }
    pub fn release_guard(&mut self) {
        let _ = self.maybe_guard.take();
    }
}

#[derive(Debug, Copy, Clone, EnumString)]
#[strum(ascii_case_insensitive)]
pub enum CheckpointMode {
    /// Checkpoint as many frames as possible without waiting for any database readers or writers to finish, then sync the database file if all frames in the log were checkpointed.
    /// Passive never blocks readers or writers, only ensures (like all modes do) that there are no other checkpointers.
    ///
    /// Optional upper_bound_inclusive parameter can be set in order to checkpoint frames with number no larger than the parameter
    Passive { upper_bound_inclusive: Option<u64> },
    /// This mode blocks until there is no database writer and all readers are reading from the most recent database snapshot. It then checkpoints all frames in the log file and syncs the database file. This mode blocks new database writers while it is pending, but new database readers are allowed to continue unimpeded.
    Full,
    /// This mode works the same way as `Full` with the addition that after checkpointing the log file it blocks (calls the busy-handler callback) until all readers are reading from the database file only. This ensures that the next writer will restart the log file from the beginning. Like `Full`, this mode blocks new database writer attempts while it is pending, but does not impede readers.
    Restart,
    /// This mode works the same way as `Restart` with the addition that it also truncates the log file to zero bytes just prior to a successful return.
    ///
    /// Extra parameter can be set in order to perform conditional TRUNCATE: database will be checkpointed and truncated only if max_frames equals to the parameter value
    /// this behaviour used by sync-engine which consolidate WAL before checkpoint and needs to be sure that no frames will be missed
    Truncate { upper_bound_inclusive: Option<u64> },
}

impl CheckpointMode {
    fn should_restart_log(&self) -> bool {
        matches!(
            self,
            CheckpointMode::Truncate { .. } | CheckpointMode::Restart
        )
    }
    /// All modes other than Passive require a complete backfilling of all available frames
    /// from `shared.nbackfills + 1 -> shared.max_frame`
    fn require_all_backfilled(&self) -> bool {
        !matches!(self, CheckpointMode::Passive { .. })
    }
}

#[repr(transparent)]
#[derive(Debug, Default)]
/// A 64-bit read-write lock with embedded 32-bit value storage.
/// Using a single Atomic allows the reader count and lock state are updated
/// atomically together while sitting in a single cpu cache line.
///
/// # Memory Layout:
/// ```ignore
/// [63:32] Value bits    - 32 bits for stored value
/// [31:1]  Reader count  - 31 bits for reader count
/// [0]     Writer bit    - 1 bit indicating exclusive write lock
/// ```
///
/// # Synchronization Guarantees:
/// - Acquire semantics on lock acquisition ensure visibility of all writes
///   made by the previous lock holder
/// - Release semantics on unlock ensure all writes made while holding the
///   lock are visible to the next acquirer
/// - The embedded value can be atomically read without holding any lock
pub struct TursoRwLock(AtomicU64);

pub const READMARK_NOT_USED: u32 = 0xffffffff;
const NO_LOCK_HELD: usize = usize::MAX;

impl TursoRwLock {
    /// Bit 0: Writer flag
    const WRITER: u64 = 0b1;

    /// Reader increment value (bit 1)
    const READER_INC: u64 = 0b10;

    /// Reader count starts at bit 1
    const READER_SHIFT: u32 = 1;

    /// Mask for 31 reader bits [31:1]
    const READER_COUNT_MASK: u64 = 0x7fff_ffffu64 << Self::READER_SHIFT;

    /// Value starts at bit 32
    const VALUE_SHIFT: u32 = 32;

    /// Mask for 32 value bits [63:32]
    const VALUE_MASK: u64 = 0xffff_ffffu64 << Self::VALUE_SHIFT;

    #[inline]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    const fn has_writer(val: u64) -> bool {
        val & Self::WRITER != 0
    }

    const fn has_readers(val: u64) -> bool {
        val & Self::READER_COUNT_MASK != 0
    }

    #[inline]
    /// Try to acquire a shared read lock.
    pub fn read(&self) -> bool {
        let cur = self.0.load(Ordering::Acquire);
        // If a writer is present we cannot proceed.
        if Self::has_writer(cur) {
            return false;
        }
        // 2 billion readers is a high enough number where we will skip the branch
        // and assume that we are not overflowing :)
        let desired = cur.wrapping_add(Self::READER_INC);
        // for success, Acquire establishes happens-before relationship with the previous Release from unlock
        // for failure we only care about reading it for the next iteration so we can use Relaxed.
        self.0
            .compare_exchange_weak(cur, desired, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Try to take an exclusive lock. Succeeds if no readers and no writer.
    #[inline]
    pub fn write(&self) -> bool {
        let cur = self.0.load(Ordering::Acquire);
        // exclusive lock, so require no readers and no writer
        if Self::has_writer(cur) || Self::has_readers(cur) {
            return false;
        }
        let desired = cur | Self::WRITER;
        self.0
            .compare_exchange(cur, desired, Ordering::Acquire, Ordering::SeqCst)
            .is_ok()
    }

    #[inline]
    /// Unlock whatever lock is currently held.
    /// For write lock: clear writer bit
    /// For read lock: decrement reader count
    pub fn unlock(&self) {
        let cur = self.0.load(Ordering::Acquire);
        if (cur & Self::WRITER) != 0 {
            // Clear writer bit, preserve everything else (including value)
            // Release ordering ensures all our writes are visible to next acquirer
            let cur = self.0.fetch_and(!Self::WRITER, Ordering::Release);
            turso_assert!(!Self::has_readers(cur), "write lock was held with readers");
        } else {
            turso_assert!(
                Self::has_readers(cur),
                "unlock called with no readers or writers"
            );
            self.0.fetch_sub(Self::READER_INC, Ordering::Release);
        }
    }

    #[inline]
    /// Read the embedded 32-bit value atomically regardless of slot occupancy.
    pub fn get_value(&self) -> u32 {
        (self.0.load(Ordering::Acquire) >> Self::VALUE_SHIFT) as u32
    }

    #[inline]
    /// Set the embedded value while holding the write lock.
    pub fn set_value_exclusive(&self, v: u32) {
        // Must be called only while WRITER bit is set
        let cur = self.0.load(Ordering::SeqCst);
        turso_assert!(Self::has_writer(cur), "must hold exclusive lock");
        let desired = (cur & !Self::VALUE_MASK) | ((v as u64) << Self::VALUE_SHIFT);
        self.0.store(desired, Ordering::SeqCst);
    }
}

/// Write-ahead log (WAL).
pub trait Wal: Debug {
    /// Begin a read transaction.
    /// Returns whether the database state has changed since the last read transaction.
    fn begin_read_tx(&mut self) -> Result<bool>;

    /// Begin a write transaction.
    fn begin_write_tx(&mut self) -> Result<()>;

    /// End a read transaction.
    fn end_read_tx(&self);

    /// End a write transaction.
    fn end_write_tx(&self);

    /// Find the latest frame containing a page.
    ///
    /// optional frame_watermark parameter can be passed to force WAL to find frame not larger than watermark value
    /// caller must guarantee, that frame_watermark must be greater than last checkpointed frame, otherwise method will panic
    fn find_frame(&self, page_id: u64, frame_watermark: Option<u64>) -> Result<Option<u64>>;

    /// Read a frame from the WAL.
    fn read_frame(
        &self,
        frame_id: u64,
        page: PageRef,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Completion>;

    /// Read a raw frame (header included) from the WAL.
    fn read_frame_raw(&self, frame_id: u64, frame: &mut [u8]) -> Result<Completion>;

    /// Write a raw frame (header included) from the WAL.
    /// Note, that turso-db will use page_no and size_after fields from the header, but will overwrite checksum with proper value
    fn write_frame_raw(
        &mut self,
        buffer_pool: Arc<BufferPool>,
        frame_id: u64,
        page_id: u64,
        db_size: u64,
        page: &[u8],
    ) -> Result<()>;

    /// Prepare WAL header for the future append
    /// Most of the time this method will return Ok(None)
    fn prepare_wal_start(&mut self, page_sz: PageSize) -> Result<Option<Completion>>;

    fn prepare_wal_finish(&mut self) -> Result<Completion>;

    /// Write a bunch of frames to the WAL.
    /// db_size is the database size in pages after the transaction finishes.
    /// db_size is set  -> last frame written in transaction
    /// db_size is none -> non-last frame written in transaction
    fn append_frames_vectored(
        &mut self,
        pages: Vec<PageRef>,
        page_sz: PageSize,
        db_size_on_commit: Option<u32>,
    ) -> Result<Completion>;

    /// Complete append of frames by updating shared wal state. Before this
    /// all changes were stored locally.
    fn finish_append_frames_commit(&mut self) -> Result<()>;

    fn should_checkpoint(&self) -> bool;
    fn checkpoint(
        &mut self,
        pager: &Pager,
        mode: CheckpointMode,
    ) -> Result<IOResult<CheckpointResult>>;
    fn sync(&mut self) -> Result<Completion>;
    fn is_syncing(&self) -> bool;
    fn get_max_frame_in_wal(&self) -> u64;
    fn get_checkpoint_seq(&self) -> u32;
    fn get_max_frame(&self) -> u64;
    fn get_min_frame(&self) -> u64;
    fn rollback(&mut self) -> Result<()>;

    /// Return unique set of pages changed **after** frame_watermark position and until current WAL session max_frame_no
    fn changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>>;

    fn set_io_context(&mut self, ctx: IOContext);

    /// Update the max frame to the current shared max frame.
    /// Currently this is only used for MVCC as it takes care of write conflicts on its own.
    /// This should't be used with regular WAL mode.
    fn update_max_frame(&mut self);

    #[cfg(debug_assertions)]
    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Debug, Clone)]
pub enum CheckpointState {
    Start,
    Processing,
    Finalize,
    Truncate {
        checkpoint_result: Option<CheckpointResult>,
        truncate_sent: bool,
        sync_sent: bool,
    },
}

/// IOV_MAX is 1024 on most systems, lets use 512 to be safe
pub const CKPT_BATCH_PAGES: usize = 512;

/// TODO: *ALL* of these need to be tuned for perf. It is tricky
/// trying to figure out the ideal numbers here to work together concurrently
const MIN_AVG_RUN_FOR_FLUSH: f32 = 32.0;
const MIN_BATCH_LEN_FOR_FLUSH: usize = 512;
const MAX_INFLIGHT_WRITES: usize = 64;
pub const MAX_INFLIGHT_READS: usize = 512;
pub const IOV_MAX: usize = 1024;

type PageId = usize;
struct InflightRead {
    completion: Completion,
    page_id: PageId,
    /// Buffer slot to contain the page content from the WAL read.
    buf: Arc<SpinLock<Option<Arc<Buffer>>>>,
}

/// WriteBatch is a collection of pages that are being checkpointed together. It is used to
/// aggregate contiguous pages into a single write operation to the database file.
#[derive(Default)]
struct WriteBatch {
    /// BTreeMap for sorting during insertion, helps create more efficient `writev` operations.
    items: BTreeMap<PageId, Arc<Buffer>>,
    /// total number of `runs`, each representing a contiguous group of `PageId`s
    run_count: usize,
}

impl WriteBatch {
    fn new() -> Self {
        Self {
            items: BTreeMap::new(),
            run_count: 0,
        }
    }

    #[inline]
    /// Add a pageId + Buffer to the batch of Writes to be submitted.
    fn insert(&mut self, page_id: PageId, buf: Arc<Buffer>) {
        if let std::collections::btree_map::Entry::Occupied(mut entry) = self.items.entry(page_id) {
            entry.insert(buf);
            return;
        }
        let left = page_id
            .checked_sub(1)
            .is_some_and(|p| self.items.contains_key(&p));
        let right = page_id
            .checked_add(1)
            .is_some_and(|p| self.items.contains_key(&p));
        match (left, right) {
            (false, false) => {
                // new singleton run
                self.run_count += 1;
            }
            (true, false) | (false, true) => {
                // extends an existing run, run_count unchanged
            }
            (true, true) => {
                // merges two runs into one
                turso_assert!(self.run_count >= 2, "should have at least two runs here");
                self.run_count -= 1;
            }
        }
        self.items.insert(page_id, buf);
    }

    #[inline]
    fn len(&self) -> usize {
        self.items.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    #[inline]
    fn is_full(&self) -> bool {
        self.items.len() >= CKPT_BATCH_PAGES
    }

    #[inline]
    fn avg_run_len(&self) -> f32 {
        if self.run_count == 0 {
            0.0
        } else {
            self.items.len() as f32 / self.run_count as f32
        }
    }

    #[inline]
    fn take(&mut self) -> BTreeMap<PageId, Arc<Buffer>> {
        self.run_count = 0;
        std::mem::take(&mut self.items)
    }

    #[inline]
    fn clear(&mut self) {
        self.items.clear();
        self.run_count = 0;
    }
}

impl std::ops::Deref for WriteBatch {
    type Target = BTreeMap<PageId, Arc<Buffer>>;
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
impl std::ops::DerefMut for WriteBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.items
    }
}

/// Information and structures for processing a checkpoint operation.
struct OngoingCheckpoint {
    /// Used for benchmarking/debugging a checkpoint operation.
    time: clock::Instant,
    /// minimum frame number to be backfilled by this checkpoint operation.
    min_frame: u64,
    /// maximum safe frame number that will be backfilled by this checkpoint operation.
    max_frame: u64,
    /// cursor used to iterate through all the pages that might have a frame in the safe range
    current_page: u64,
    /// State of the checkpoint
    state: CheckpointState,
    /// Batch repreesnts a collection of pages to be backfilled to the DB file.
    pending_writes: WriteBatch,
    /// Read operations currently ongoing.
    inflight_reads: Vec<InflightRead>,
    /// Array of atomic counters representing write operations that are currently in flight.
    inflight_writes: Vec<Arc<AtomicBool>>,
    /// List of all page_id + frame_id combinations to be backfilled
    pages_to_checkpoint: Vec<(u64, u64)>,
}

impl OngoingCheckpoint {
    fn reset(&mut self) {
        self.min_frame = 0;
        self.max_frame = 0;
        self.current_page = 0;
        self.pages_to_checkpoint.clear();
        self.pending_writes.clear();
        self.inflight_reads.clear();
        self.inflight_writes.clear();
        self.state = CheckpointState::Start;
    }

    #[inline]
    /// Whether or not new reads should be issued during checkpoint processing.
    fn should_issue_reads(&self) -> bool {
        (self.current_page as usize) < self.pages_to_checkpoint.len()
            && !self.pending_writes.is_full()
            && self.inflight_reads.len() < MAX_INFLIGHT_READS
    }

    #[inline]
    /// Whether the backfilling/IO process is entirely completed during checkpoint processing.
    fn complete(&self) -> bool {
        (self.current_page as usize) >= self.pages_to_checkpoint.len()
            && self.inflight_reads.is_empty()
            && self.pending_writes.is_empty()
            && self.inflight_writes.is_empty()
    }

    #[inline]
    /// Whether we should flush an exisitng batch of writes and begin concurrently aggregating a new one.
    fn should_flush_batch(&self) -> bool {
        self.pending_writes.is_full()
            || (self.pending_writes.len() >= MIN_BATCH_LEN_FOR_FLUSH
                && self.pending_writes.avg_run_len() >= MIN_AVG_RUN_FOR_FLUSH)
            || ((self.current_page as usize) >= self.pages_to_checkpoint.len()
                && self.inflight_reads.is_empty()
                && !self.pending_writes.is_empty())
    }

    #[inline]
    /// Remove any completed write operations from `inflight_writes`,
    /// returns whether any progress was made.
    fn process_inflight_writes(&mut self) -> bool {
        let before_len = self.inflight_writes.len();
        self.inflight_writes
            .retain(|done| !done.load(Ordering::Acquire));
        before_len > self.inflight_writes.len()
    }

    #[inline]
    /// Remove any completed read operations from `inflight_reads`
    /// returns whether any progress was made.
    fn process_pending_reads(&mut self) -> bool {
        let mut moved = false;
        // retain only those still pending
        self.inflight_reads.retain(|slot| {
            if slot.completion.is_completed() {
                if let Some(buf) = slot.buf.lock().take() {
                    // read is done, take the buffer and add it to the batch to write
                    self.pending_writes.insert(slot.page_id, buf);
                    moved = true;
                }
                false
            } else {
                true
            }
        });
        moved
    }

    #[inline]
    /// Add an `inflight write` and return the Atomic counter used to
    /// track it's completion.
    fn add_write(&mut self) -> Arc<AtomicBool> {
        let new = Arc::new(AtomicBool::new(false));
        self.inflight_writes.push(new.clone());
        new
    }
}

impl fmt::Debug for OngoingCheckpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OngoingCheckpoint")
            .field("state", &self.state)
            .field("min_frame", &self.min_frame)
            .field("max_frame", &self.max_frame)
            .field("current_page", &self.current_page)
            .finish()
    }
}

pub struct WalFile {
    io: Arc<dyn IO>,
    buffer_pool: Arc<BufferPool>,

    syncing: Arc<AtomicBool>,

    shared: Arc<RwLock<WalFileShared>>,
    ongoing_checkpoint: OngoingCheckpoint,
    checkpoint_threshold: usize,
    // min and max frames for this connection
    /// This is the index to the read_lock in WalFileShared that we are holding. This lock contains
    /// the max frame for this connection.
    max_frame_read_lock_index: AtomicUsize,
    /// Max frame allowed to lookup range=(minframe..max_frame)
    max_frame: AtomicU64,
    /// Start of range to look for frames range=(minframe..max_frame)
    min_frame: AtomicU64,
    /// Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    last_checksum: (u32, u32),
    checkpoint_seq: AtomicU32,

    /// Count of possible pages to checkpoint, and number of backfilled
    prev_checkpoint: CheckpointResult,

    /// Private copy of WalHeader
    pub header: WalHeader,

    /// Manages locks needed for checkpointing
    checkpoint_guard: Option<CheckpointLocks>,

    io_ctx: RwLock<IOContext>,
}

impl fmt::Debug for WalFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFile")
            .field("syncing", &self.syncing.load(Ordering::SeqCst))
            .field("page_size", &self.page_size())
            .field("shared", &self.shared)
            .field("ongoing_checkpoint", &self.ongoing_checkpoint)
            .field("checkpoint_threshold", &self.checkpoint_threshold)
            .field("max_frame_read_lock_index", &self.max_frame_read_lock_index)
            .field("max_frame", &self.max_frame)
            .field("min_frame", &self.min_frame)
            // Excluding other fields
            .finish()
    }
}

/*
* sqlite3/src/wal.c
*
** nBackfill is the number of frames in the WAL that have been written
** back into the database. (We call the act of moving content from WAL to
** database "backfilling".)  The nBackfill number is never greater than
** WalIndexHdr.mxFrame.  nBackfill can only be increased by threads
** holding the WAL_CKPT_LOCK lock (which includes a recovery thread).
** However, a WAL_WRITE_LOCK thread can move the value of nBackfill from
** mxFrame back to zero when the WAL is reset.
**
** nBackfillAttempted is the largest value of nBackfill that a checkpoint
** has attempted to achieve.  Normally nBackfill==nBackfillAtempted, however
** the nBackfillAttempted is set before any backfilling is done and the
** nBackfill is only set after all backfilling completes.  So if a checkpoint
** crashes, nBackfillAttempted might be larger than nBackfill.  The
** WalIndexHdr.mxFrame must never be less than nBackfillAttempted.
**
** The aLock[] field is a set of bytes used for locking.  These bytes should
** never be read or written.
**
** There is one entry in aReadMark[] for each reader lock.  If a reader
** holds read-lock K, then the value in aReadMark[K] is no greater than
** the mxFrame for that reader.  The value READMARK_NOT_USED (0xffffffff)
** for any aReadMark[] means that entry is unused.  aReadMark[0] is
** a special case; its value is never used and it exists as a place-holder
** to avoid having to offset aReadMark[] indexes by one.  Readers holding
** WAL_READ_LOCK(0) always ignore the entire WAL and read all content
** directly from the database.
**
** The value of aReadMark[K] may only be changed by a thread that
** is holding an exclusive lock on WAL_READ_LOCK(K).  Thus, the value of
** aReadMark[K] cannot changed while there is a reader is using that mark
** since the reader will be holding a shared lock on WAL_READ_LOCK(K).
**
** The checkpointer may only transfer frames from WAL to database where
** the frame numbers are less than or equal to every aReadMark[] that is
** in use (that is, every aReadMark[j] for which there is a corresponding
** WAL_READ_LOCK(j)).  New readers (usually) pick the aReadMark[] with the
** largest value and will increase an unused aReadMark[] to mxFrame if there
** is not already an aReadMark[] equal to mxFrame.  The exception to the
** previous sentence is when nBackfill equals mxFrame (meaning that everything
** in the WAL has been backfilled into the database) then new readers
** will choose aReadMark[0] which has value 0 and hence such reader will
** get all their all content directly from the database file and ignore
** the WAL.
**
** Writers normally append new frames to the end of the WAL.  However,
** if nBackfill equals mxFrame (meaning that all WAL content has been
** written back into the database) and if no readers are using the WAL
** (in other words, if there are no WAL_READ_LOCK(i) where i>0) then
** the writer will first "reset" the WAL back to the beginning and start
** writing new content beginning at frame 1.
*/

// TODO(pere): lock only important parts + pin WalFileShared
/// WalFileShared is the part of a WAL that will be shared between threads. A wal has information
/// that needs to be communicated between threads so this struct does the job.
pub struct WalFileShared {
    pub enabled: AtomicBool,
    pub wal_header: Arc<SpinLock<WalHeader>>,
    pub min_frame: AtomicU64,
    pub max_frame: AtomicU64,
    pub nbackfills: AtomicU64,
    // Frame cache maps a Page to all the frames it has stored in WAL in ascending order.
    // This is to easily find the frame it must checkpoint each connection if a checkpoint is
    // necessary.
    // One difference between SQLite and limbo is that we will never support multi process, meaning
    // we don't need WAL's index file. So we can do stuff like this without shared memory.
    // TODO: this will need refactoring because this is incredible memory inefficient.
    pub frame_cache: Arc<SpinLock<HashMap<u64, Vec<u64>>>>,
    pub last_checksum: (u32, u32), // Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    pub file: Option<Arc<dyn File>>,
    /// Read locks advertise the maximum WAL frame a reader may access.
    /// Slot 0 is special, when it is held (shared) the reader bypasses the WAL and uses the main DB file.
    /// When checkpointing, we must acquire the exclusive read lock 0 to ensure that no readers read
    /// from a partially checkpointed db file.
    /// Slots 1‑4 carry a frame‑number in value and may be shared by many readers. Slot 1 is the
    /// default read lock and is to contain the max_frame in WAL.
    pub read_locks: [TursoRwLock; 5],
    /// There is only one write allowed in WAL mode. This lock takes care of ensuring there is only
    /// one used.
    pub write_lock: TursoRwLock,

    /// Serialises checkpointer threads, only one checkpoint can be in flight at any time. Blocking and exclusive only
    pub checkpoint_lock: TursoRwLock,
    pub loaded: AtomicBool,
    pub initialized: AtomicBool,
    /// Increments on each checkpoint, used to prevent stale cached pages being used for
    /// backfilling.
    pub epoch: AtomicU32,
}

impl fmt::Debug for WalFileShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFileShared")
            .field("enabled", &self.enabled.load(Ordering::SeqCst))
            .field("wal_header", &self.wal_header)
            .field("min_frame", &self.min_frame)
            .field("max_frame", &self.max_frame)
            .field("nbackfills", &self.nbackfills)
            .field("frame_cache", &self.frame_cache)
            .field("last_checksum", &self.last_checksum)
            // Excluding `file`, `read_locks`, and `write_lock`
            .finish()
    }
}

#[derive(Clone, Debug)]
/// To manage and ensure that no locks are leaked during checkpointing in
/// the case of errors. It is held by the WalFile while checkpoint is ongoing
/// then transferred to the CheckpointResult if necessary.
enum CheckpointLocks {
    Writer { ptr: Arc<RwLock<WalFileShared>> },
    Read0 { ptr: Arc<RwLock<WalFileShared>> },
}

/// Database checkpointers takes the following locks, in order:
/// The exclusive CHECKPOINTER lock.
/// The exclusive WRITER lock (FULL, RESTART and TRUNCATE only).
/// Exclusive lock on read-mark slots 1-N. These are immediately released after being taken.
/// Exclusive lock on read-mark 0.
/// Exclusive lock on read-mark slots 1-N again. These are immediately released after being taken (RESTART and TRUNCATE only).
/// All of the above use blocking locks.
impl CheckpointLocks {
    fn new(ptr: Arc<RwLock<WalFileShared>>, mode: CheckpointMode) -> Result<Self> {
        let ptr_clone = ptr.clone();
        {
            let shared = ptr.write();
            if !shared.checkpoint_lock.write() {
                tracing::trace!("CheckpointGuard::new: checkpoint lock failed, returning Busy");
                return Err(LimboError::Busy);
            }
            match mode {
                CheckpointMode::Passive { .. } => {
                    if !shared.read_locks[0].write() {
                        shared.checkpoint_lock.unlock();
                        tracing::trace!("CheckpointGuard: read0 lock failed, returning Busy");
                        return Err(LimboError::Busy);
                    }
                }
                CheckpointMode::Full => {
                    if !shared.read_locks[0].write() {
                        shared.checkpoint_lock.unlock();
                        tracing::trace!("CheckpointGuard: read0 lock failed (Full), Busy");
                        return Err(LimboError::Busy);
                    }
                    if !shared.write_lock.write() {
                        shared.read_locks[0].unlock();
                        shared.checkpoint_lock.unlock();
                        tracing::trace!("CheckpointGuard: write lock failed (Full), Busy");
                        return Err(LimboError::Busy);
                    }
                }
                CheckpointMode::Restart | CheckpointMode::Truncate { .. } => {
                    if !shared.read_locks[0].write() {
                        shared.checkpoint_lock.unlock();
                        tracing::trace!("CheckpointGuard: read0 lock failed, returning Busy");
                        return Err(LimboError::Busy);
                    }
                    if !shared.write_lock.write() {
                        shared.checkpoint_lock.unlock();
                        shared.read_locks[0].unlock();
                        tracing::trace!("CheckpointGuard: write lock failed, returning Busy");
                        return Err(LimboError::Busy);
                    }
                }
            }
        }

        match mode {
            CheckpointMode::Passive { .. } => Ok(Self::Read0 { ptr: ptr_clone }),
            CheckpointMode::Full | CheckpointMode::Restart | CheckpointMode::Truncate { .. } => {
                Ok(Self::Writer { ptr: ptr_clone })
            }
        }
    }
}

impl Drop for CheckpointLocks {
    fn drop(&mut self) {
        match self {
            CheckpointLocks::Writer { ptr: shared } => {
                let guard = shared.write();
                guard.write_lock.unlock();
                guard.read_locks[0].unlock();
                guard.checkpoint_lock.unlock();
            }
            CheckpointLocks::Read0 { ptr: shared } => {
                let guard = shared.write();
                guard.read_locks[0].unlock();
                guard.checkpoint_lock.unlock();
            }
        }
    }
}

impl Wal for WalFile {
    /// Begin a read transaction. The caller must ensure that there is not already
    /// an ongoing read transaction.
    /// Returns whether the database state has changed since the last read transaction.
    /// sqlite/src/wal.c 3023
    /// assert(pWal->readLock < 0); /* Not currently locked */
    #[instrument(skip_all, level = Level::DEBUG)]
    fn begin_read_tx(&mut self) -> Result<bool> {
        turso_assert!(
            self.max_frame_read_lock_index
                .load(Ordering::Acquire)
                .eq(&NO_LOCK_HELD),
            "cannot start a new read tx without ending an existing one, lock_value={}, expected={}",
            self.max_frame_read_lock_index.load(Ordering::Acquire),
            NO_LOCK_HELD
        );
        let (shared_max, nbackfills, last_checksum, checkpoint_seq) = {
            let shared = self.get_shared();
            let mx = shared.max_frame.load(Ordering::Acquire);
            let nb = shared.nbackfills.load(Ordering::Acquire);
            let ck = shared.last_checksum;
            let checkpoint_seq = shared.wal_header.lock().checkpoint_seq;
            (mx, nb, ck, checkpoint_seq)
        };
        let db_changed = shared_max != self.max_frame.load(Ordering::Acquire)
            || last_checksum != self.last_checksum
            || checkpoint_seq != self.checkpoint_seq.load(Ordering::Acquire);

        // WAL is already fully back‑filled into the main DB image
        // (mxFrame == nBackfill). Readers can therefore ignore the
        // WAL and fetch pages directly from the DB file.  We do this
        // by taking read‑lock 0, and capturing the latest state.
        if shared_max == nbackfills {
            tracing::debug!("begin_read_tx: WAL is already fully back‑filled into the main DB image, shared_max={}, nbackfills={}", shared_max, nbackfills);
            let lock_0_idx = 0;
            if !self.get_shared().read_locks[lock_0_idx].read() {
                tracing::debug!("begin_read_tx: read lock 0 is already held, returning Busy");
                return Err(LimboError::Busy);
            }
            // we need to keep self.max_frame set to the appropriate
            // max frame in the wal at the time this transaction starts.
            self.max_frame.store(shared_max, Ordering::Release);
            self.max_frame_read_lock_index
                .store(lock_0_idx, Ordering::Release);
            self.min_frame.store(nbackfills + 1, Ordering::Release);
            self.last_checksum = last_checksum;
            return Ok(db_changed);
        }

        // If we get this far, it means that the reader will want to use
        // the WAL to get at content from recent commits.  The job now is
        // to select one of the aReadMark[] entries that is closest to
        // but not exceeding pWal->hdr.mxFrame and lock that entry.
        // Find largest mark <= mx among slots 1..N
        let mut best_idx: i64 = -1;
        let mut best_mark: u32 = 0;
        for (idx, lock) in self.get_shared().read_locks.iter().enumerate().skip(1) {
            let m = lock.get_value();
            if m != READMARK_NOT_USED && m <= shared_max as u32 && m > best_mark {
                best_mark = m;
                best_idx = idx as i64;
            }
        }

        // If none found or lagging, try to claim/update a slot
        if best_idx == -1 || (best_mark as u64) < shared_max {
            for (idx, lock) in self
                .get_shared_mut()
                .read_locks
                .iter_mut()
                .enumerate()
                .skip(1)
            {
                if !lock.write() {
                    continue; // busy slot
                }
                // claim or bump this slot
                lock.set_value_exclusive(shared_max as u32);
                best_idx = idx as i64;
                best_mark = shared_max as u32;
                lock.unlock();
                break;
            }
        }

        if best_idx == -1 || best_mark != shared_max as u32 {
            // If we cannot find a valid slot or the highest readmark has a stale max frame, we must return busy;
            // otherwise we would not see some committed changes.
            return Err(LimboError::Busy);
        }

        // Now take a shared read on that slot, and if we are successful,
        // grab another snapshot of the shared state.
        let (mx2, nb2, cksm2, ckpt_seq2) = {
            let shared = self.get_shared();
            if !shared.read_locks[best_idx as usize].read() {
                // TODO: we should retry here instead of always returning Busy
                return Err(LimboError::Busy);
            }
            let checkpoint_seq = shared.wal_header.lock().checkpoint_seq;
            (
                shared.max_frame.load(Ordering::Acquire),
                shared.nbackfills.load(Ordering::Acquire),
                shared.last_checksum,
                checkpoint_seq,
            )
        };

        // sqlite/src/wal.c 3225
        // Now that the read-lock has been obtained, check that neither the
        // value in the aReadMark[] array or the contents of the wal-index
        // header have changed.
        //
        // It is necessary to check that the wal-index header did not change
        // between the time it was read and when the shared-lock was obtained
        // on WAL_READ_LOCK(mxI) was obtained to account for the possibility
        // that the log file may have been wrapped by a writer, or that frames
        // that occur later in the log than pWal->hdr.mxFrame may have been
        // copied into the database by a checkpointer. If either of these things
        // happened, then reading the database with the current value of
        // pWal->hdr.mxFrame risks reading a corrupted snapshot. So, retry
        // instead.
        //
        // Before checking that the live wal-index header has not changed
        // since it was read, set Wal.minFrame to the first frame in the wal
        // file that has not yet been checkpointed. This client will not need
        // to read any frames earlier than minFrame from the wal file - they
        // can be safely read directly from the database file.
        if mx2 != shared_max
            || nb2 != nbackfills
            || cksm2 != last_checksum
            || ckpt_seq2 != checkpoint_seq
        {
            return Err(LimboError::Busy);
        }
        self.min_frame.store(nb2 + 1, Ordering::Release);
        self.max_frame.store(best_mark as u64, Ordering::Release);
        self.max_frame_read_lock_index
            .store(best_idx as usize, Ordering::Release);
        tracing::debug!(
            "begin_read_tx(min={}, max={}, slot={}, max_frame_in_wal={})",
            self.min_frame.load(Ordering::Acquire),
            self.max_frame.load(Ordering::Acquire),
            best_idx,
            shared_max
        );
        Ok(db_changed)
    }

    /// End a read transaction.
    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    fn end_read_tx(&self) {
        let slot = self.max_frame_read_lock_index.load(Ordering::Acquire);
        if slot != NO_LOCK_HELD {
            self.get_shared_mut().read_locks[slot].unlock();
            self.max_frame_read_lock_index
                .store(NO_LOCK_HELD, Ordering::Release);
            tracing::debug!("end_read_tx(slot={slot})");
        } else {
            tracing::debug!("end_read_tx(slot=no_lock)");
        }
    }

    /// Begin a write transaction
    #[instrument(skip_all, level = Level::DEBUG)]
    fn begin_write_tx(&mut self) -> Result<()> {
        let shared = self.get_shared_mut();
        // sqlite/src/wal.c 3702
        // Cannot start a write transaction without first holding a read
        // transaction.
        // assert(pWal->readLock >= 0);
        // assert(pWal->writeLock == 0 && pWal->iReCksum == 0);
        turso_assert!(
            self.max_frame_read_lock_index.load(Ordering::Acquire) != NO_LOCK_HELD,
            "must have a read transaction to begin a write transaction"
        );
        if !shared.write_lock.write() {
            return Err(LimboError::Busy);
        }
        let (shared_max, nbackfills, last_checksum) = (
            shared.max_frame.load(Ordering::Acquire),
            shared.nbackfills.load(Ordering::Acquire),
            shared.last_checksum,
        );
        if self.max_frame.load(Ordering::Acquire) == shared_max {
            // Snapshot still valid; adopt counters
            drop(shared);
            self.last_checksum = last_checksum;
            self.min_frame.store(nbackfills + 1, Ordering::Release);
            return Ok(());
        }

        // Snapshot is stale, give up and let caller retry from scratch
        tracing::debug!("unable to upgrade transaction from read to write: snapshot is stale, give up and let caller retry from scratch, self.max_frame={}, shared_max={}", self.max_frame.load(Ordering::Acquire), shared_max);
        shared.write_lock.unlock();
        Err(LimboError::Busy)
    }

    /// End a write transaction
    #[instrument(skip_all, level = Level::DEBUG)]
    fn end_write_tx(&self) {
        tracing::debug!("end_write_txn");
        self.get_shared().write_lock.unlock();
    }

    /// Find the latest frame containing a page.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn find_frame(&self, page_id: u64, frame_watermark: Option<u64>) -> Result<Option<u64>> {
        #[cfg(not(feature = "conn_raw_api"))]
        turso_assert!(
            frame_watermark.is_none(),
            "unexpected use of frame_watermark optional argument"
        );

        turso_assert!(
            frame_watermark.unwrap_or(0) <= self.max_frame.load(Ordering::Acquire),
            "frame_watermark must be <= than current WAL max_frame value"
        );

        // we can guarantee correctness of the method, only if frame_watermark is strictly after the current checkpointed prefix
        //
        // if it's not, than pages from WAL range [frame_watermark..nBackfill] are already in the DB file,
        // and in case if page first occurrence in WAL was after frame_watermark - we will be unable to read proper previous version of the page
        turso_assert!(
            frame_watermark.is_none() || frame_watermark.unwrap() >= self.get_shared().nbackfills.load(Ordering::Acquire),
            "frame_watermark must be >= than current WAL backfill amount: frame_watermark={:?}, nBackfill={}", frame_watermark, self.get_shared().nbackfills.load(Ordering::Acquire)
        );

        // if we are holding read_lock 0 and didn't write anything to the WAL, skip and read right from db file.
        //
        // note, that max_frame_read_lock_index is set to 0 only when shared_max_frame == nbackfill in which case
        // min_frame is set to nbackfill + 1 and max_frame is set to shared_max_frame
        //
        // by default, SQLite tries to restart log file in this case - but for now let's keep it simple in the turso-db
        if self.max_frame_read_lock_index.load(Ordering::Acquire) == 0
            && self.max_frame.load(Ordering::Acquire) < self.min_frame.load(Ordering::Acquire)
        {
            tracing::debug!(
                "find_frame(page_id={}, frame_watermark={:?}): max_frame is 0 - read from DB file",
                page_id,
                frame_watermark,
            );
            return Ok(None);
        }
        let shared = self.get_shared();
        let frames = shared.frame_cache.lock();
        let range = frame_watermark.map(|x| 0..=x).unwrap_or(
            self.min_frame.load(Ordering::Acquire)..=self.max_frame.load(Ordering::Acquire),
        );
        tracing::debug!(
            "find_frame(page_id={}, frame_watermark={:?}): min_frame={}, max_frame={}",
            page_id,
            frame_watermark,
            self.min_frame.load(Ordering::Acquire),
            self.max_frame.load(Ordering::Acquire)
        );
        if let Some(list) = frames.get(&page_id) {
            if let Some(f) = list.iter().rfind(|&&f| range.contains(&f)) {
                tracing::debug!(
                    "find_frame(page_id={}, frame_watermark={:?}): found frame={}",
                    page_id,
                    frame_watermark,
                    *f
                );
                return Ok(Some(*f));
            }
        }
        Ok(None)
    }

    /// Read a frame from the WAL.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_frame(
        &self,
        frame_id: u64,
        page: PageRef,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Completion> {
        tracing::debug!(
            "read_frame(page_idx = {}, frame_id = {})",
            page.get().id,
            frame_id
        );
        let offset = self.frame_offset(frame_id);
        page.set_locked();
        let frame = page.clone();
        let page_idx = page.get().id;
        let shared_file = self.shared.clone();
        let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                page.clear_locked();
                page.clear_wal_tag();
                return;
            };
            let buf_len = buf.len();
            turso_assert!(
                bytes_read == buf_len as i32,
                "read({bytes_read}) less than expected({buf_len}): frame_id={frame_id}"
            );
            let cloned = frame.clone();
            finish_read_page(page.get().id, buf, cloned);
            let epoch = shared_file.read().epoch.load(Ordering::Acquire);
            frame.set_wal_tag(frame_id, epoch);
        });
        let file = {
            let shared = self.get_shared();
            assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
            // important not to hold shared lock beyond this point to avoid deadlock scenario where:
            // thread 1: takes readlock here, passes reference to shared.file to begin_read_wal_frame
            // thread 2: tries to acquire write lock elsewhere
            // thread 1: tries to re-acquire read lock in the completion (see 'complete' above)
            //
            // this causes a deadlock due to the locking policy in parking_lot:
            // from https://docs.rs/parking_lot/latest/parking_lot/type.RwLock.html:
            // "This lock uses a task-fair locking policy which avoids both reader and writer starvation.
            // This means that readers trying to acquire the lock will block even if the lock is unlocked
            // when there are writers waiting to acquire the lock.
            // Because of this, attempts to recursively acquire a read lock within a single thread may result in a deadlock."
            shared.file.as_ref().unwrap().clone()
        };
        begin_read_wal_frame(
            &file,
            offset + WAL_FRAME_HEADER_SIZE as u64,
            buffer_pool,
            complete,
            page_idx,
            &self.io_ctx.read(),
        )
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_frame_raw(&self, frame_id: u64, frame: &mut [u8]) -> Result<Completion> {
        tracing::debug!("read_frame({})", frame_id);
        let offset = self.frame_offset(frame_id);
        let (frame_ptr, frame_len) = (frame.as_mut_ptr(), frame.len());

        let encryption_ctx = {
            let io_ctx = self.io_ctx.read();
            io_ctx.encryption_context().cloned()
        };
        let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                return;
            };
            let buf_len = buf.len();
            turso_assert!(
                bytes_read == buf_len as i32,
                "read({bytes_read}) != expected({buf_len})"
            );
            let buf_ptr = buf.as_ptr();
            let frame_ref: &mut [u8] =
                unsafe { std::slice::from_raw_parts_mut(frame_ptr, frame_len) };

            // Copy the just-read WAL frame into the destination buffer
            unsafe {
                std::ptr::copy_nonoverlapping(buf_ptr, frame_ptr, frame_len);
            }

            // Now parse the header from the freshly-copied data
            let (header, raw_page) = sqlite3_ondisk::parse_wal_frame_header(frame_ref);

            if let Some(ctx) = encryption_ctx.clone() {
                match ctx.decrypt_page(raw_page, header.page_number as usize) {
                    Ok(decrypted_data) => {
                        turso_assert!(
                            (frame_len - WAL_FRAME_HEADER_SIZE) == decrypted_data.len(),
                            "frame_len - header_size({}) != expected({})",
                            frame_len - WAL_FRAME_HEADER_SIZE,
                            decrypted_data.len()
                        );
                        frame_ref[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&decrypted_data);
                    }
                    Err(_) => {
                        tracing::error!("Failed to decrypt page data for frame_id={frame_id}");
                    }
                }
            }
        });
        let shared = self.get_shared();
        assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
        let file = shared.file.as_ref().unwrap();
        let c = begin_read_wal_frame_raw(&self.buffer_pool, file, offset, complete)?;
        Ok(c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn write_frame_raw(
        &mut self,
        buffer_pool: Arc<BufferPool>,
        frame_id: u64,
        page_id: u64,
        db_size: u64,
        page: &[u8],
    ) -> Result<()> {
        let Some(page_size) = PageSize::new(page.len() as u32) else {
            bail_corrupt_error!("invalid page size: {}", page.len());
        };
        self.ensure_header_if_needed(page_size)?;
        tracing::debug!("write_raw_frame({})", frame_id);
        // if page_size wasn't initialized before - we will initialize it during that raw write
        if self.page_size() != 0 && page.len() != self.page_size() as usize {
            return Err(LimboError::InvalidArgument(format!(
                "unexpected page size in frame: got={}, expected={}",
                page.len(),
                self.page_size(),
            )));
        }
        if frame_id > self.max_frame.load(Ordering::Acquire) + 1 {
            // attempt to write frame out of sequential order - error out
            return Err(LimboError::InvalidArgument(format!(
                "frame_id is beyond next frame in the WAL: frame_id={}, max_frame={}",
                frame_id,
                self.max_frame.load(Ordering::Acquire)
            )));
        }
        if frame_id <= self.max_frame.load(Ordering::Acquire) {
            // just validate if page content from the frame matches frame in the WAL
            let offset = self.frame_offset(frame_id);
            let conflict = Arc::new(Cell::new(false));
            let (page_ptr, page_len) = (page.as_ptr(), page.len());
            let complete = Box::new({
                let conflict = conflict.clone();
                move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                    let Ok((buf, bytes_read)) = res else {
                        return;
                    };
                    let buf_len = buf.len();
                    turso_assert!(
                        bytes_read == buf_len as i32,
                        "read({bytes_read}) != expected({buf_len})"
                    );
                    let page = unsafe { std::slice::from_raw_parts(page_ptr, page_len) };
                    if buf.as_slice() != page {
                        conflict.set(true);
                    }
                }
            });
            let shared = self.get_shared();
            assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
            let file = shared.file.as_ref().unwrap();
            let c = begin_read_wal_frame(
                file,
                offset + WAL_FRAME_HEADER_SIZE as u64,
                buffer_pool,
                complete,
                page_id as usize,
                &self.io_ctx.read(),
            )?;
            self.io.wait_for_completion(c)?;
            return if conflict.get() {
                Err(LimboError::Conflict(format!(
                    "frame content differs from the WAL: frame_id={frame_id}"
                )))
            } else {
                Ok(())
            };
        }

        // perform actual write
        let offset = self.frame_offset(frame_id);
        let (header, file) = {
            let shared = self.get_shared();
            let header = shared.wal_header.clone();
            assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
            let file = shared.file.as_ref().unwrap().clone();
            (header, file)
        };
        let header = header.lock();
        let checksums = self.last_checksum;
        let (checksums, frame_bytes) = prepare_wal_frame(
            &self.buffer_pool,
            &header,
            checksums,
            header.page_size,
            page_id as u32,
            db_size as u32,
            page,
        );
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(offset, frame_bytes, c)?;
        self.io.wait_for_completion(c)?;
        self.complete_append_frame(page_id, frame_id, checksums);
        if db_size > 0 {
            self.finish_append_frames_commit()?;
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn should_checkpoint(&self) -> bool {
        let shared = self.get_shared();
        let frame_id = shared.max_frame.load(Ordering::Acquire) as usize;
        let nbackfills = shared.nbackfills.load(Ordering::Acquire) as usize;
        frame_id > self.checkpoint_threshold + nbackfills
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn checkpoint(
        &mut self,
        pager: &Pager,
        mode: CheckpointMode,
    ) -> Result<IOResult<CheckpointResult>> {
        self.checkpoint_inner(pager, mode).inspect_err(|_| {
            let _ = self.checkpoint_guard.take();
            self.ongoing_checkpoint.state = CheckpointState::Start;
        })
    }

    #[instrument(err, skip_all, level = Level::DEBUG)]
    fn sync(&mut self) -> Result<Completion> {
        tracing::debug!("wal_sync");
        let syncing = self.syncing.clone();
        let completion = Completion::new_sync(move |_| {
            tracing::debug!("wal_sync finish");
            syncing.store(false, Ordering::SeqCst);
        });
        let shared = self.get_shared();
        self.syncing.store(true, Ordering::SeqCst);
        assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
        let file = shared.file.as_ref().unwrap();
        let c = file.sync(completion)?;
        Ok(c)
    }

    // Currently used for assertion purposes
    fn is_syncing(&self) -> bool {
        self.syncing.load(Ordering::SeqCst)
    }

    fn get_max_frame_in_wal(&self) -> u64 {
        self.get_shared().max_frame.load(Ordering::Acquire)
    }

    fn get_checkpoint_seq(&self) -> u32 {
        self.get_shared().wal_header.lock().checkpoint_seq
    }

    fn get_max_frame(&self) -> u64 {
        self.max_frame.load(Ordering::Acquire)
    }

    fn get_min_frame(&self) -> u64 {
        self.min_frame.load(Ordering::Acquire)
    }

    #[instrument(err, skip_all, level = Level::DEBUG)]
    fn rollback(&mut self) -> Result<()> {
        let (max_frame, last_checksum) = {
            let shared = self.get_shared();
            let max_frame = shared.max_frame.load(Ordering::Acquire);
            let mut frame_cache = shared.frame_cache.lock();
            frame_cache.retain(|_page_id, frames| {
                // keep frames <= max_frame
                while frames.last().is_some_and(|&f| f > max_frame) {
                    frames.pop();
                }
                !frames.is_empty()
            });
            (max_frame, shared.last_checksum)
        };
        self.last_checksum = last_checksum;
        self.max_frame.store(max_frame, Ordering::Release);
        self.reset_internal_states();
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn finish_append_frames_commit(&mut self) -> Result<()> {
        let mut shared = self.get_shared_mut();
        shared
            .max_frame
            .store(self.max_frame.load(Ordering::Acquire), Ordering::Release);
        tracing::trace!(max_frame = self.max_frame.load(Ordering::Acquire), ?self.last_checksum);
        shared.last_checksum = self.last_checksum;
        Ok(())
    }

    fn changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        let frame_count = self.get_max_frame();
        let page_size = self.page_size();
        let mut frame = vec![0u8; page_size as usize + WAL_FRAME_HEADER_SIZE];
        let mut seen = HashSet::new();
        turso_assert!(
            frame_count >= frame_watermark,
            "frame_count must be not less than frame_watermark: {} vs {}",
            frame_count,
            frame_watermark
        );
        let mut pages = Vec::with_capacity((frame_count - frame_watermark) as usize);
        for frame_no in frame_watermark + 1..=frame_count {
            let c = self.read_frame_raw(frame_no, &mut frame)?;
            self.io.wait_for_completion(c)?;
            let (header, _) = sqlite3_ondisk::parse_wal_frame_header(&frame);
            if seen.insert(header.page_number) {
                pages.push(header.page_number);
            }
        }
        Ok(pages)
    }

    fn prepare_wal_start(&mut self, page_size: PageSize) -> Result<Option<Completion>> {
        if self.get_shared().is_initialized()? {
            return Ok(None);
        }
        tracing::debug!("ensure_header_if_needed");
        self.last_checksum = {
            let mut shared = self.get_shared_mut();
            let checksum = {
                let mut hdr = shared.wal_header.lock();
                hdr.magic = if cfg!(target_endian = "big") {
                    WAL_MAGIC_BE
                } else {
                    WAL_MAGIC_LE
                };
                if hdr.page_size == 0 {
                    hdr.page_size = page_size.get();
                }
                if hdr.salt_1 == 0 && hdr.salt_2 == 0 {
                    hdr.salt_1 = self.io.generate_random_number() as u32;
                    hdr.salt_2 = self.io.generate_random_number() as u32;
                }

                // recompute header checksum
                let prefix = &hdr.as_bytes()[..WAL_HEADER_SIZE - 8];
                let use_native = (hdr.magic & 1) != 0;
                let (c1, c2) = checksum_wal(prefix, &hdr, (0, 0), use_native);
                hdr.checksum_1 = c1;
                hdr.checksum_2 = c2;
                (c1, c2)
            };
            shared.last_checksum = checksum;
            checksum
        };

        self.max_frame.store(0, Ordering::Release);
        let shared = self.get_shared();
        assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
        let file = shared.file.as_ref().unwrap();
        let c = sqlite3_ondisk::begin_write_wal_header(file, &shared.wal_header.lock())?;
        Ok(Some(c))
    }

    fn prepare_wal_finish(&mut self) -> Result<Completion> {
        let shared = self.get_shared();
        assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
        let file = shared.file.as_ref().unwrap();
        let shared = self.shared.clone();
        let c = file.sync(Completion::new_sync(move |_| {
            shared.read().initialized.store(true, Ordering::Release);
        }))?;
        Ok(c)
    }

    /// Use pwritev to append many frames to the log at once
    fn append_frames_vectored(
        &mut self,
        pages: Vec<PageRef>,
        page_sz: PageSize,
        db_size_on_commit: Option<u32>,
    ) -> Result<Completion> {
        turso_assert!(
            pages.len() <= IOV_MAX,
            "we limit number of iovecs to IOV_MAX"
        );
        turso_assert!(
            self.get_shared().is_initialized()?,
            "WAL must be prepared with prepare_wal_start/prepare_wal_finish method"
        );

        let (header, shared_page_size, epoch) = {
            let shared = self.get_shared();
            let hdr_guard = shared.wal_header.lock();
            let header: WalHeader = *hdr_guard;
            let shared_page_size = header.page_size;
            let epoch = shared.epoch.load(Ordering::Acquire);
            (header, shared_page_size, epoch)
        };
        turso_assert!(
            shared_page_size == page_sz.get(),
            "page size mismatch, tried to change page size after WAL header was already initialized: shared.page_size={shared_page_size}, page_size={}",
            page_sz.get()
        );

        // Prepare write buffers and bookkeeping
        let mut iovecs: Vec<Arc<Buffer>> = Vec::with_capacity(pages.len());
        let mut page_frame_and_checksum: Vec<(PageRef, u64, (u32, u32))> =
            Vec::with_capacity(pages.len());

        // Rolling checksum input to each frame build
        let mut rolling_checksum: (u32, u32) = self.last_checksum;

        let mut next_frame_id = self.max_frame.load(Ordering::Acquire) + 1;
        // Build every frame in order, updating the rolling checksum
        for (idx, page) in pages.iter().enumerate() {
            let page_id = page.get().id;
            let plain = page.get_contents().as_ptr();

            let data_to_write: std::borrow::Cow<[u8]> = {
                let io_ctx = self.io_ctx.read();
                match &io_ctx.encryption_or_checksum() {
                    EncryptionOrChecksum::Encryption(ctx) => {
                        Cow::Owned(ctx.encrypt_page(plain, page_id)?)
                    }
                    EncryptionOrChecksum::Checksum(ctx) => {
                        ctx.add_checksum_to_page(plain, page_id)?;
                        Cow::Borrowed(plain)
                    }
                    EncryptionOrChecksum::None => Cow::Borrowed(plain),
                }
            };

            let frame_db_size = if idx + 1 == pages.len() {
                // if it's the final frame we are appending, and the caller included a db_size for the
                // commit frame, then we ensure to set it in the header.
                db_size_on_commit.unwrap_or(0)
            } else {
                0
            };
            let (new_checksum, frame_bytes) = prepare_wal_frame(
                &self.buffer_pool,
                &header,
                rolling_checksum,
                shared_page_size,
                page_id as u32,
                frame_db_size,
                &data_to_write,
            );
            iovecs.push(frame_bytes);

            // (page, assigned_frame_id, cumulative_checksum_at_this_frame)
            page_frame_and_checksum.push((page.clone(), next_frame_id, new_checksum));

            // Advance for the next frame
            rolling_checksum = new_checksum;
            next_frame_id += 1;
        }

        let first_frame_id = self.max_frame.load(Ordering::Acquire) + 1;
        let start_off = self.frame_offset(first_frame_id);

        // pre-advance in-memory WAL state
        for (page, fid, csum) in &page_frame_and_checksum {
            self.complete_append_frame(page.get().id as u64, *fid, *csum);
        }

        // single completion for the whole batch
        let total_len: i32 = iovecs.iter().map(|b| b.len() as i32).sum();
        let page_frame_for_cb = page_frame_and_checksum.clone();
        let cmp = move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                return;
            };
            turso_assert!(
                bytes_written == total_len,
                "pwritev wrote {bytes_written} bytes, expected {total_len}"
            );

            for (page, fid, _csum) in &page_frame_for_cb {
                page.clear_dirty();
                page.set_wal_tag(*fid, epoch);
            }
        };

        let c = Completion::new_write_linked(cmp);

        let shared = self.get_shared();
        assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
        let file = shared.file.as_ref().unwrap();
        let c = file.pwritev(start_off, iovecs, c)?;
        Ok(c)
    }

    #[cfg(debug_assertions)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn set_io_context(&mut self, ctx: IOContext) {
        *self.io_ctx.write() = ctx;
    }

    fn update_max_frame(&mut self) {
        let new_max_frame = self.get_shared().max_frame.load(Ordering::Acquire);
        self.max_frame.store(new_max_frame, Ordering::Release);
    }
}

impl WalFile {
    pub fn new(
        io: Arc<dyn IO>,
        shared: Arc<RwLock<WalFileShared>>,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let (header, last_checksum, max_frame) = {
            let shared_guard = shared.read();
            let header = *shared_guard.wal_header.lock();
            (
                header,
                shared_guard.last_checksum,
                shared_guard.max_frame.load(Ordering::Acquire),
            )
        };
        let now = io.now();
        Self {
            io,
            // default to max frame in WAL, so that when we read schema we can read from WAL too if it's there.
            max_frame: AtomicU64::new(max_frame),
            shared,
            ongoing_checkpoint: OngoingCheckpoint {
                time: now,
                pending_writes: WriteBatch::new(),
                inflight_writes: Vec::new(),
                state: CheckpointState::Start,
                min_frame: 0,
                max_frame: 0,
                current_page: 0,
                pages_to_checkpoint: Vec::new(),
                inflight_reads: Vec::with_capacity(MAX_INFLIGHT_READS),
            },
            checkpoint_threshold: 1000,
            buffer_pool,
            checkpoint_seq: AtomicU32::new(0),
            syncing: Arc::new(AtomicBool::new(false)),
            min_frame: AtomicU64::new(0),
            max_frame_read_lock_index: AtomicUsize::new(NO_LOCK_HELD),
            last_checksum,
            prev_checkpoint: CheckpointResult::default(),
            checkpoint_guard: None,
            header,
            io_ctx: RwLock::new(IOContext::default()),
        }
    }

    fn page_size(&self) -> u32 {
        self.get_shared().wal_header.lock().page_size
    }

    fn frame_offset(&self, frame_id: u64) -> u64 {
        assert!(frame_id > 0, "Frame ID must be 1-based");
        let page_offset = (frame_id - 1) * (self.page_size() + WAL_FRAME_HEADER_SIZE as u32) as u64;
        WAL_HEADER_SIZE as u64 + page_offset
    }

    fn get_shared_mut(&self) -> parking_lot::RwLockWriteGuard<'_, WalFileShared> {
        // WASM in browser main thread doesn't have a way to "park" a thread
        // so, we spin way here instead of calling blocking lock
        #[cfg(target_family = "wasm")]
        {
            loop {
                let Some(lock) = self.shared.try_write() else {
                    std::hint::spin_loop();
                    continue;
                };
                return lock;
            }
        }
        #[cfg(not(target_family = "wasm"))]
        {
            self.shared.write()
        }
    }

    fn get_shared(&self) -> parking_lot::RwLockReadGuard<'_, WalFileShared> {
        // WASM in browser main thread doesn't have a way to "park" a thread
        // so, we spin way here instead of calling blocking lock
        #[cfg(target_family = "wasm")]
        {
            loop {
                let Some(lock) = self.shared.try_read() else {
                    std::hint::spin_loop();
                    continue;
                };
                return lock;
            }
        }
        #[cfg(not(target_family = "wasm"))]
        {
            self.shared.read()
        }
    }

    fn complete_append_frame(&mut self, page_id: u64, frame_id: u64, checksums: (u32, u32)) {
        self.last_checksum = checksums;
        self.max_frame.store(frame_id, Ordering::Release);
        let shared = self.get_shared();
        {
            let mut frame_cache = shared.frame_cache.lock();
            match frame_cache.get_mut(&page_id) {
                Some(frames) => {
                    frames.push(frame_id);
                }
                None => {
                    frame_cache.insert(page_id, vec![frame_id]);
                }
            }
        }
    }

    fn reset_internal_states(&mut self) {
        self.max_frame_read_lock_index
            .store(NO_LOCK_HELD, Ordering::Release);
        self.ongoing_checkpoint.reset();
        self.syncing.store(false, Ordering::SeqCst);
    }

    /// the WAL file has been truncated and we are writing the first
    /// frame since then. We need to ensure that the header is initialized.
    fn ensure_header_if_needed(&mut self, page_size: PageSize) -> Result<()> {
        let Some(c) = self.prepare_wal_start(page_size)? else {
            return Ok(());
        };
        self.io.wait_for_completion(c)?;
        let c = self.prepare_wal_finish()?;
        self.io.wait_for_completion(c)?;
        Ok(())
    }

    fn checkpoint_inner(
        &mut self,
        pager: &Pager,
        mode: CheckpointMode,
    ) -> Result<IOResult<CheckpointResult>> {
        loop {
            let state = &mut self.ongoing_checkpoint.state;
            tracing::debug!(?state);
            match state {
                // Acquire the relevant exclusive locks and checkpoint_lock
                // so no other checkpointer can run. fsync WAL if there are unapplied frames.
                // Decide the largest frame we are allowed to back‑fill.
                CheckpointState::Start => {
                    let (max_frame, nbackfills) = {
                        let shared = self.get_shared();
                        let max_frame = shared.max_frame.load(Ordering::Acquire);
                        let n_backfills = shared.nbackfills.load(Ordering::Acquire);
                        (max_frame, n_backfills)
                    };
                    let needs_backfill = max_frame > nbackfills;
                    if !needs_backfill && !mode.should_restart_log() {
                        // there are no frames to copy over and we don't need to reset
                        // the log so we can return early success.
                        return Ok(IOResult::Done(CheckpointResult {
                            num_attempted: self.prev_checkpoint.num_attempted,
                            num_backfilled: self.prev_checkpoint.num_backfilled,
                            max_frame: nbackfills,
                            maybe_guard: None,
                            db_sync_sent: false,
                            db_truncate_sent: false,
                        }));
                    }
                    // acquire the appropriate exclusive locks depending on the checkpoint mode
                    self.acquire_proper_checkpoint_guard(mode)?;
                    let mut max_frame = self.determine_max_safe_checkpoint_frame();

                    if let CheckpointMode::Truncate {
                        upper_bound_inclusive: Some(upper_bound),
                    } = mode
                    {
                        if max_frame > upper_bound {
                            tracing::info!("abort checkpoint because latest frame in WAL is greater than upper_bound in TRUNCATE mode: {max_frame} != {upper_bound}");
                            return Err(LimboError::Busy);
                        }
                    }
                    if let CheckpointMode::Passive {
                        upper_bound_inclusive: Some(upper_bound),
                    } = mode
                    {
                        max_frame = max_frame.min(upper_bound);
                    }

                    self.ongoing_checkpoint.max_frame = max_frame;
                    self.ongoing_checkpoint.min_frame = nbackfills + 1;
                    let to_checkpoint = {
                        let shared = self.get_shared();
                        let frame_cache = shared.frame_cache.lock();
                        let mut list = Vec::with_capacity(
                            self.ongoing_checkpoint
                                .max_frame
                                .checked_sub(nbackfills)
                                .unwrap_or_default() as usize,
                        );
                        for (&page_id, frames) in frame_cache.iter() {
                            // for each page in the frame cache, grab the last (latest) frame for
                            // that page that falls in the range of our safe min..max frame
                            if let Some(&frame) = frames.iter().rev().find(|&&f| {
                                f >= self.ongoing_checkpoint.min_frame
                                    && f <= self.ongoing_checkpoint.max_frame
                            }) {
                                list.push((page_id, frame));
                            }
                        }
                        // sort by frame_id for read locality
                        list.sort_unstable_by(|a, b| (a.1, a.0).cmp(&(b.1, b.0)));
                        list
                    };
                    self.ongoing_checkpoint.pages_to_checkpoint = to_checkpoint;
                    self.ongoing_checkpoint.current_page = 0;
                    self.ongoing_checkpoint.inflight_writes.clear();
                    self.ongoing_checkpoint.inflight_reads.clear();
                    self.ongoing_checkpoint.state = CheckpointState::Processing;
                    self.ongoing_checkpoint.time = self.io.now();
                    tracing::trace!(
                        "checkpoint_start(min_frame={}, max_frame={})",
                        self.ongoing_checkpoint.min_frame,
                        self.ongoing_checkpoint.max_frame,
                    );
                }
                // For locality, reading is ordered by frame ID, and writing ordered by page ID.
                // the more consecutive page ID's that we submit together, the fewer overall
                // write/writev syscalls made. All I/O during checkpointing is now in a single step
                // to prevent serialization, and we try to issue reads and flush batches concurrently
                // if at all possible, at the cost of some batching potential.
                CheckpointState::Processing => {
                    // Gather I/O completions, estimate with MAX_INFLIGHT_WRITES to prevent realloc
                    let mut completions = Vec::with_capacity(MAX_INFLIGHT_WRITES);

                    // Check and clean any completed writes from pending flush
                    if self.ongoing_checkpoint.process_inflight_writes() {
                        tracing::trace!("Completed a write batch");
                    }
                    // Process completed reads into current batch
                    if self.ongoing_checkpoint.process_pending_reads() {
                        tracing::trace!("Drained reads into batch");
                    }
                    let epoch = self.get_shared().epoch.load(Ordering::Acquire);
                    // Issue reads until we hit limits
                    'inner: while self.ongoing_checkpoint.should_issue_reads() {
                        let (page_id, target_frame) = self.ongoing_checkpoint.pages_to_checkpoint
                            [self.ongoing_checkpoint.current_page as usize];
                        'fast_path: {
                            if let Some(cached_page) = pager.cache_get_for_checkpoint(
                                page_id as usize,
                                target_frame,
                                epoch,
                            )? {
                                let contents = cached_page.get_contents().buffer.clone();
                                // to avoid TOCTOU issues with using cached pages, we snapshot the contents and pay the memcpy
                                // instead of risking the page changing out from under us.
                                let buffer = Arc::new(self.buffer_pool.get_page());
                                buffer.as_mut_slice()[..contents.len()]
                                    .copy_from_slice(contents.as_slice());
                                if !cached_page.is_valid_for_checkpoint(target_frame, epoch) {
                                    // check again, atomically, if the page is still valid after we
                                    // copied a snapshot of it, if not: fallthrough to reading
                                    // from disk
                                    break 'fast_path;
                                }
                                // TODO: remove this eventually to actually benefit from the
                                // performance.. for now we assert that the cached page has the
                                // exact contents as one read from the WAL.
                                #[cfg(debug_assertions)]
                                {
                                    let mut raw = vec![
                                        0u8;
                                        self.page_size() as usize
                                            + WAL_FRAME_HEADER_SIZE
                                    ];
                                    self.io.wait_for_completion(
                                        self.read_frame_raw(target_frame, &mut raw)?,
                                    )?;
                                    let (_, wal_page) =
                                        sqlite3_ondisk::parse_wal_frame_header(&raw);
                                    let cached = buffer.as_slice();
                                    turso_assert!(wal_page == cached, "cached page content differs from WAL read for page_id={page_id}, frame_id={target_frame}");
                                }
                                self.ongoing_checkpoint
                                    .pending_writes
                                    .insert(page_id as usize, buffer);

                                // signify that a cached page was used, so it can be unpinned
                                self.ongoing_checkpoint.pages_to_checkpoint
                                    [self.ongoing_checkpoint.current_page as usize] =
                                    (page_id, target_frame);
                                self.ongoing_checkpoint.current_page += 1;
                                continue 'inner;
                            }
                        }
                        // Issue read if page wasn't found in the page cache or doesnt meet
                        // the frame requirements
                        let inflight =
                            self.issue_wal_read_into_buffer(page_id as usize, target_frame)?;
                        completions.push(inflight.completion.clone());
                        self.ongoing_checkpoint.inflight_reads.push(inflight);
                        self.ongoing_checkpoint.current_page += 1;
                    }

                    // Start a write if batch is ready and we're not at write limit
                    if self.ongoing_checkpoint.inflight_writes.len() < MAX_INFLIGHT_WRITES
                        && self.ongoing_checkpoint.should_flush_batch()
                    {
                        let batch_map = self.ongoing_checkpoint.pending_writes.take();
                        if !batch_map.is_empty() {
                            let done_flag = self.ongoing_checkpoint.add_write();
                            completions.extend(write_pages_vectored(pager, batch_map, done_flag)?);
                        }
                    }

                    if !completions.is_empty() {
                        io_yield_many!(completions);
                    } else if self.ongoing_checkpoint.complete() {
                        self.ongoing_checkpoint.state = CheckpointState::Finalize;
                    }
                }
                // All eligible frames copied to the db file
                // Update nBackfills
                // In Restart or Truncate mode, we need to restart the log over and possibly truncate the file
                // Release all locks and return the current num of wal frames and the amount we backfilled
                CheckpointState::Finalize => {
                    turso_assert!(
                        self.ongoing_checkpoint.complete(),
                        "checkpoint pending flush must have finished"
                    );
                    let checkpoint_result = {
                        let shared = self.get_shared();
                        let current_mx = shared.max_frame.load(Ordering::Acquire);
                        let nbackfills = shared.nbackfills.load(Ordering::Acquire);
                        // Record two num pages fields to return as checkpoint result to caller.
                        // Ref: pnLog, pnCkpt on https://www.sqlite.org/c3ref/wal_checkpoint_v2.html

                        // the total # of frames we could have possibly backfilled
                        let frames_possible = current_mx.saturating_sub(nbackfills);

                        // the total # of frames we actually backfilled
                        let checkpoint_max_frame = self.ongoing_checkpoint.max_frame;
                        let frames_checkpointed = checkpoint_max_frame
                            .saturating_sub(self.ongoing_checkpoint.min_frame - 1);

                        if matches!(mode, CheckpointMode::Truncate { .. }) {
                            // sqlite always returns zeros for truncate mode
                            CheckpointResult::default()
                        } else if frames_checkpointed == 0
                            && matches!(mode, CheckpointMode::Restart)
                        // if we restarted the log but didn't backfill pages we still have to
                        // return the last checkpoint result.
                        {
                            self.prev_checkpoint.clone()
                        } else {
                            // otherwise return the normal result of the total # of possible frames
                            // we could have backfilled, and the number we actually did.
                            CheckpointResult::new(
                                frames_possible,
                                frames_checkpointed,
                                checkpoint_max_frame,
                            )
                        }
                    };

                    // store the max frame we were able to successfully checkpoint.
                    // NOTE: we don't have a .shm file yet, so it's safe to update nbackfills here
                    // before we sync, because if we crash and then recover, we will checkpoint the entire db anyway.
                    self.get_shared()
                        .nbackfills
                        .store(self.ongoing_checkpoint.max_frame, Ordering::Release);

                    if mode.require_all_backfilled() && !checkpoint_result.everything_backfilled() {
                        return Err(LimboError::Busy);
                    }
                    if mode.should_restart_log() {
                        self.restart_log(mode)?;
                    }
                    self.ongoing_checkpoint.state = CheckpointState::Truncate {
                        checkpoint_result: Some(checkpoint_result),
                        truncate_sent: false,
                        sync_sent: false,
                    };
                }
                CheckpointState::Truncate { .. } => {
                    if matches!(mode, CheckpointMode::Truncate { .. }) {
                        return_if_io!(self.truncate_log());
                    }
                    if mode.should_restart_log() {
                        Self::unlock_after_restart(&self.shared, None);
                    }
                    let mut checkpoint_result = {
                        let CheckpointState::Truncate {
                            checkpoint_result, ..
                        } = &mut self.ongoing_checkpoint.state
                        else {
                            panic!("unxpected state");
                        };
                        checkpoint_result.take().unwrap()
                    };
                    // increment wal epoch to ensure no stale pages are used for backfilling
                    self.get_shared().epoch.fetch_add(1, Ordering::Release);

                    // store a copy of the checkpoint result to return in the future if pragma
                    // wal_checkpoint is called and we haven't backfilled again since.
                    self.prev_checkpoint = checkpoint_result.clone();

                    // we cannot truncate the db file here because we are currently inside a
                    // mut borrow of pager.wal, and accessing the header will attempt a borrow
                    // during 'read_page', so the caller will use the result to determine if:
                    // a. the max frame == num wal frames (everything backfilled)
                    // b. the physical db file size differs from the expected pages * page_size
                    // and truncate + sync the db file if necessary.
                    if checkpoint_result.everything_backfilled()
                        && checkpoint_result.num_backfilled > 0
                    {
                        checkpoint_result.maybe_guard = self.checkpoint_guard.take();
                    } else {
                        let _ = self.checkpoint_guard.take();
                    }
                    self.ongoing_checkpoint.inflight_writes.clear();
                    self.ongoing_checkpoint.pending_writes.clear();
                    self.ongoing_checkpoint.pages_to_checkpoint.clear();
                    self.ongoing_checkpoint.current_page = 0;
                    tracing::debug!(
                        "total time spent checkpointing: {:?}",
                        self.io
                            .now()
                            .to_system_time()
                            .duration_since(self.ongoing_checkpoint.time.to_system_time())
                            .expect("time")
                            .as_millis()
                    );
                    self.ongoing_checkpoint.state = CheckpointState::Start;
                    return Ok(IOResult::Done(checkpoint_result));
                }
            }
        }
    }

    /// Coordinate what the maximum safe frame is for us to backfill when checkpointing.
    /// We can never backfill a frame with a higher number than any reader's read mark,
    /// because we might overwrite content the reader is reading from the database file.
    ///
    /// A checkpoint must never overwrite a page in the main DB file if some
    /// active reader might still need to read that page from the WAL.  
    /// Concretely: the checkpoint may only copy frames `<= aReadMark[k]` for
    /// every in-use reader slot `k > 0`.
    ///
    /// `read_locks[0]` is special: readers holding slot 0 ignore the WAL entirely
    /// (they read only the DB file). Its value is a placeholder and does not
    /// constrain `mxSafeFrame`.
    ///
    /// For each slot 1..N:
    /// - If we can acquire the write lock (slot is free):
    ///   - Slot 1: Set to mxSafeFrame (allowing new readers to see up to this point)
    ///   - Slots 2+: Set to READMARK_NOT_USED (freeing the slot)
    /// - If we cannot acquire the lock (SQLITE_BUSY):
    ///   - Lower mxSafeFrame to that reader's mark
    ///   - In PASSIVE mode: Already have no busy handler, continue scanning
    ///   - In FULL/RESTART/TRUNCATE: Disable busy handler for remaining slots
    ///
    /// Locking behavior:
    /// - PASSIVE: Never waits, no busy handler (xBusy==NULL)
    /// - FULL/RESTART/TRUNCATE: May wait via busy handler, but after first BUSY,
    ///   switches to non-blocking for remaining slots
    ///
    /// We never modify slot values while a reader holds that slot's lock.
    /// TOOD: implement proper BUSY handling behavior
    fn determine_max_safe_checkpoint_frame(&self) -> u64 {
        let mut shared = self.get_shared_mut();
        let shared_max = shared.max_frame.load(Ordering::Acquire);
        let mut max_safe_frame = shared_max;

        for (read_lock_idx, read_lock) in shared.read_locks.iter_mut().enumerate().skip(1) {
            let this_mark = read_lock.get_value();
            if this_mark < max_safe_frame as u32 {
                let busy = !read_lock.write();
                if !busy {
                    let val = if read_lock_idx == 1 {
                        // store the max_frame for the default read slot 1
                        max_safe_frame as u32
                    } else {
                        READMARK_NOT_USED
                    };
                    read_lock.set_value_exclusive(val);
                    read_lock.unlock();
                } else {
                    max_safe_frame = this_mark as u64;
                }
            }
        }
        max_safe_frame
    }

    /// Called once the entire WAL has been back‑filled in RESTART or TRUNCATE mode.
    /// Must be invoked while writer and checkpoint locks are still held.
    fn restart_log(&mut self, mode: CheckpointMode) -> Result<()> {
        turso_assert!(
            mode.should_restart_log(),
            "CheckpointMode must be Restart or Truncate"
        );
        turso_assert!(
            matches!(self.checkpoint_guard, Some(CheckpointLocks::Writer { .. })),
            "We must hold writer and checkpoint locks to restart the log, found: {:?}",
            self.checkpoint_guard
        );
        tracing::debug!("restart_log(mode={mode:?})");
        {
            // Block all readers
            let mut shared = self.get_shared_mut();
            for idx in 1..shared.read_locks.len() {
                let lock = &mut shared.read_locks[idx];
                if !lock.write() {
                    // release everything we got so far
                    for j in 1..idx {
                        shared.read_locks[j].unlock();
                    }
                    // Reader is active, cannot proceed
                    return Err(LimboError::Busy);
                }
                // after the log is reset, we must set all secondary marks to READMARK_NOT_USED so the next reader selects a fresh slot
                lock.set_value_exclusive(READMARK_NOT_USED);
            }
        }

        // reinitialize in‑memory state
        self.get_shared_mut().restart_wal_header(&self.io, mode);
        let cksm = self.get_shared().last_checksum;
        self.last_checksum = cksm;
        self.max_frame.store(0, Ordering::Release);
        self.min_frame.store(0, Ordering::Release);
        self.checkpoint_seq.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn truncate_log(&mut self) -> Result<IOResult<()>> {
        let file = {
            let shared = self.get_shared();
            assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
            shared.initialized.store(false, Ordering::Release);
            shared.file.as_ref().unwrap().clone()
        };

        let CheckpointState::Truncate {
            sync_sent,
            truncate_sent,
            ..
        } = &mut self.ongoing_checkpoint.state
        else {
            panic!("unxpected state");
        };
        if !*truncate_sent {
            // For TRUNCATE mode: shrink the WAL file to 0 B

            let c = Completion::new_trunc({
                let shared = self.shared.clone();
                move |result| {
                    if let Err(err) = result {
                        Self::unlock_after_restart(
                            &shared,
                            Some(&LimboError::InternalError(err.to_string())),
                        );
                    } else {
                        tracing::trace!("WAL file truncated to 0 B");
                    }
                }
            });
            let c = file
                .truncate(0, c)
                .inspect_err(|e| Self::unlock_after_restart(&self.shared, Some(e)))?;
            *truncate_sent = true;
            io_yield_one!(c);
        } else if !*sync_sent {
            let shared = self.shared.clone();
            let c = file
                .sync(Completion::new_sync(move |result| {
                    if let Err(err) = result {
                        Self::unlock_after_restart(
                            &shared,
                            Some(&LimboError::InternalError(err.to_string())),
                        );
                    } else {
                        tracing::trace!("WAL file synced after reset/truncation");
                    }
                }))
                .inspect_err(|e| Self::unlock_after_restart(&self.shared, Some(e)))?;
            *sync_sent = true;
            io_yield_one!(c);
        }
        Ok(IOResult::Done(()))
    }

    // unlock shared read locks taken by RESTART/TRUNCATE checkpoint modes
    fn unlock_after_restart(shared: &Arc<RwLock<WalFileShared>>, e: Option<&LimboError>) {
        // release all read locks we just acquired, the caller will take care of the others
        let shared = shared.write();
        for idx in 1..shared.read_locks.len() {
            shared.read_locks[idx].unlock();
        }
        if let Some(e) = e {
            tracing::error!(
                "Failed to restart WAL header: {:?}, releasing read locks",
                e
            );
        }
    }

    fn acquire_proper_checkpoint_guard(&mut self, mode: CheckpointMode) -> Result<()> {
        let needs_new_guard = !matches!(
            (&self.checkpoint_guard, mode),
            (
                Some(CheckpointLocks::Read0 { .. }),
                CheckpointMode::Passive { .. },
            ) | (
                Some(CheckpointLocks::Writer { .. }),
                CheckpointMode::Restart | CheckpointMode::Truncate { .. },
            ),
        );
        if needs_new_guard {
            // Drop any existing guard
            if self.checkpoint_guard.is_some() {
                let _ = self.checkpoint_guard.take();
            }
            let guard = CheckpointLocks::new(self.shared.clone(), mode)?;
            self.checkpoint_guard = Some(guard);
        }
        Ok(())
    }

    fn issue_wal_read_into_buffer(&self, page_id: usize, frame_id: u64) -> Result<InflightRead> {
        let offset = self.frame_offset(frame_id);
        let buf_slot = Arc::new(SpinLock::new(None));

        let complete = {
            let buf_slot = buf_slot.clone();
            Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((buf, read)) = res else {
                    return;
                };
                let buf_len = buf.len();
                turso_assert!(
                    read == buf_len as i32,
                    "read({read}) != expected({buf_len}): frame_id={frame_id}"
                );
                *buf_slot.lock() = Some(buf);
            })
        };
        // schedule read of the page payload
        let shared = self.get_shared();
        assert!(shared.enabled.load(Ordering::SeqCst), "WAL must be enabled");
        let file = shared.file.as_ref().unwrap();
        let c = begin_read_wal_frame(
            file,
            offset + WAL_FRAME_HEADER_SIZE as u64,
            self.buffer_pool.clone(),
            complete,
            page_id,
            &self.io_ctx.read(),
        )?;

        Ok(InflightRead {
            completion: c,
            page_id,
            buf: buf_slot,
        })
    }
}

impl WalFileShared {
    pub fn open_shared_if_exists(
        io: &Arc<dyn IO>,
        path: &str,
    ) -> Result<Arc<RwLock<WalFileShared>>> {
        let file = io.open_file(path, crate::io::OpenFlags::Create, false)?;
        if file.size()? == 0 {
            return WalFileShared::new_noop();
        }
        let wal_file_shared = sqlite3_ondisk::build_shared_wal(&file, io)?;
        turso_assert!(
            wal_file_shared
                .try_read()
                .is_some_and(|wfs| wfs.loaded.load(Ordering::Acquire)),
            "Unable to read WAL shared state"
        );
        Ok(wal_file_shared)
    }

    pub fn is_initialized(&self) -> Result<bool> {
        Ok(self.initialized.load(Ordering::Acquire))
    }

    pub fn new_noop() -> Result<Arc<RwLock<WalFileShared>>> {
        let wal_header = WalHeader {
            magic: 0,
            file_format: 0,
            page_size: 0,
            checkpoint_seq: 0,
            salt_1: 0,
            salt_2: 0,
            checksum_1: 0,
            checksum_2: 0,
        };
        let read_locks = array::from_fn(|_| TursoRwLock::new());
        for (i, lock) in read_locks.iter().enumerate() {
            lock.write();
            lock.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
            lock.unlock();
        }
        let shared = WalFileShared {
            enabled: AtomicBool::new(false),
            wal_header: Arc::new(SpinLock::new(wal_header)),
            min_frame: AtomicU64::new(0),
            max_frame: AtomicU64::new(0),
            nbackfills: AtomicU64::new(0),
            frame_cache: Arc::new(SpinLock::new(HashMap::new())),
            last_checksum: (0, 0),
            file: None,
            read_locks,
            write_lock: TursoRwLock::new(),
            checkpoint_lock: TursoRwLock::new(),
            loaded: AtomicBool::new(true),
            initialized: AtomicBool::new(false),
            epoch: AtomicU32::new(0),
        };
        Ok(Arc::new(RwLock::new(shared)))
    }

    pub fn new_shared(file: Arc<dyn File>) -> Result<Arc<RwLock<WalFileShared>>> {
        let magic = if cfg!(target_endian = "big") {
            WAL_MAGIC_BE
        } else {
            WAL_MAGIC_LE
        };
        let wal_header = WalHeader {
            magic,
            file_format: 3007000,
            page_size: 0, // Signifies WAL header that is not persistent on disk yet.
            checkpoint_seq: 0, // TODO implement sequence number
            salt_1: 0,
            salt_2: 0,
            checksum_1: 0,
            checksum_2: 0,
        };
        let read_locks = array::from_fn(|_| TursoRwLock::new());
        // slot zero is always zero as it signifies that reads can be done from the db file
        // directly, and slot 1 is the default read mark containing the max frame. in this case
        // our max frame is zero so both slots 0 and 1 begin at 0
        for (i, lock) in read_locks.iter().enumerate() {
            lock.write();
            lock.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
            lock.unlock();
        }
        let shared = WalFileShared {
            enabled: AtomicBool::new(true),
            wal_header: Arc::new(SpinLock::new(wal_header)),
            min_frame: AtomicU64::new(0),
            max_frame: AtomicU64::new(0),
            nbackfills: AtomicU64::new(0),
            frame_cache: Arc::new(SpinLock::new(HashMap::new())),
            last_checksum: (0, 0),
            file: Some(file),
            read_locks,
            write_lock: TursoRwLock::new(),
            checkpoint_lock: TursoRwLock::new(),
            loaded: AtomicBool::new(true),
            initialized: AtomicBool::new(false),
            epoch: AtomicU32::new(0),
        };
        Ok(Arc::new(RwLock::new(shared)))
    }

    pub fn create(&mut self, file: Arc<dyn File>) -> Result<()> {
        if self.enabled.load(Ordering::SeqCst) {
            return Err(LimboError::InternalError("WAL already enabled".to_string()));
        }

        let magic = if cfg!(target_endian = "big") {
            WAL_MAGIC_BE
        } else {
            WAL_MAGIC_LE
        };

        *self.wal_header.lock() = WalHeader {
            magic,
            file_format: 3007000,
            page_size: 0, // Signifies WAL header that is not persistent on disk yet.
            checkpoint_seq: 0,
            salt_1: 0,
            salt_2: 0,
            checksum_1: 0,
            checksum_2: 0,
        };

        self.file = Some(file);
        self.enabled.store(true, Ordering::SeqCst);
        self.initialized.store(false, Ordering::SeqCst);

        Ok(())
    }

    pub fn page_size(&self) -> u32 {
        self.wal_header.lock().page_size
    }

    /// Called after a successful RESTART/TRUNCATE mode checkpoint
    /// when all frames are back‑filled.
    ///
    /// sqlite3/src/wal.c
    /// The following is guaranteed when this function is called:
    ///
    ///   a) the WRITER lock is held,
    ///   b) the entire log file has been checkpointed, and
    ///   c) any existing readers are reading exclusively from the database
    ///      file - there are no readers that may attempt to read a frame from
    ///      the log file.
    ///
    /// This function updates the shared-memory structures so that the next
    /// client to write to the database (which may be this one) does so by
    /// writing frames into the start of the log file.
    fn restart_wal_header(&mut self, io: &Arc<dyn IO>, mode: CheckpointMode) {
        turso_assert!(
            matches!(
                mode,
                CheckpointMode::Restart | CheckpointMode::Truncate { .. }
            ),
            "CheckpointMode must be Restart or Truncate"
        );
        {
            let mut hdr = self.wal_header.lock();
            hdr.checkpoint_seq = hdr.checkpoint_seq.wrapping_add(1);
            // keep hdr.magic, hdr.file_format, hdr.page_size as-is
            hdr.salt_1 = hdr.salt_1.wrapping_add(1);
            hdr.salt_2 = io.generate_random_number() as u32;

            self.max_frame.store(0, Ordering::Release);
            self.nbackfills.store(0, Ordering::Release);
            self.last_checksum = (hdr.checksum_1, hdr.checksum_2);
        }

        self.frame_cache.lock().clear();
        // read-marks
        self.read_locks[0].set_value_exclusive(0);
        self.read_locks[1].set_value_exclusive(0);
        for lock in &self.read_locks[2..] {
            lock.set_value_exclusive(READMARK_NOT_USED);
        }
    }
}

#[cfg(test)]
pub mod test {
    use crate::{
        storage::{
            sqlite3_ondisk::{self, WAL_HEADER_SIZE},
            wal::READMARK_NOT_USED,
        },
        types::IOResult,
        util::IOExt,
        CheckpointMode, CheckpointResult, Completion, Connection, Database, LimboError, PlatformIO,
        StepResult, Wal, WalFile, WalFileShared, IO,
    };
    use parking_lot::RwLock;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::{
        cell::Cell,
        rc::Rc,
        sync::{atomic::Ordering, Arc},
    };
    #[allow(clippy::arc_with_non_send_sync)]
    pub(crate) fn get_database() -> (Arc<Database>, std::path::PathBuf) {
        let mut path = tempfile::tempdir().unwrap().keep();
        let dbpath = path.clone();
        path.push("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
        }
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), path.to_str().unwrap(), false, false).unwrap();
        // db + tmp directory
        (db, dbpath)
    }
    #[test]
    fn test_truncate_file() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();
        conn.execute("create table test (id integer primary key, value text)")
            .unwrap();
        let _ = conn.execute("insert into test (value) values ('test1'), ('test2'), ('test3')");
        let wal = db.shared_wal.write();
        let wal_file = wal.file.as_ref().unwrap().clone();
        let done = Rc::new(Cell::new(false));
        let _done = done.clone();
        let _ = wal_file.truncate(
            WAL_HEADER_SIZE as u64,
            Completion::new_trunc(move |_| {
                let done = _done.clone();
                done.set(true);
            }),
        );
        assert!(wal_file.size().unwrap() == WAL_HEADER_SIZE as u64);
        assert!(done.get());
    }

    #[test]
    fn test_wal_truncate_checkpoint() {
        let (db, path) = get_database();
        let mut walpath = path.clone().into_os_string().into_string().unwrap();
        walpath.push_str("/test.db-wal");
        let walpath = std::path::PathBuf::from(walpath);

        let conn = db.connect().unwrap();
        conn.execute("create table test (id integer primary key, value text)")
            .unwrap();
        for _i in 0..25 {
            let _ = conn.execute("insert into test (value) values (randomblob(1024)), (randomblob(1024)), (randomblob(1024))");
        }
        let pager = conn.pager.write();
        let _ = pager.cacheflush();
        let mut wal = pager.wal.as_ref().unwrap().borrow_mut();

        let stat = std::fs::metadata(&walpath).unwrap();
        let meta_before = std::fs::metadata(&walpath).unwrap();
        let bytes_before = meta_before.len();
        run_checkpoint_until_done(
            &mut *wal,
            &pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        drop(wal);

        assert_eq!(pager.wal_state().unwrap().max_frame, 0);

        tracing::info!("wal filepath: {walpath:?}, size: {}", stat.len());
        let meta_after = std::fs::metadata(&walpath).unwrap();
        let bytes_after = meta_after.len();
        assert_ne!(
            bytes_before, bytes_after,
            "WAL file should not have been empty before checkpoint"
        );
        assert_eq!(
            bytes_after, 0,
            "WAL file should be truncated to 0 bytes, but is {bytes_after} bytes",
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    fn bulk_inserts(conn: &Arc<Connection>, n_txns: usize, rows_per_txn: usize) {
        for _ in 0..n_txns {
            conn.execute("begin transaction").unwrap();
            for i in 0..rows_per_txn {
                conn.execute(format!("insert into test(value) values ('v{i}')"))
                    .unwrap();
            }
            conn.execute("commit").unwrap();
        }
    }

    fn count_test_table(conn: &Arc<Connection>) -> i64 {
        let mut stmt = conn.prepare("select count(*) from test").unwrap();
        loop {
            match stmt.step() {
                Ok(StepResult::Row) => {
                    break;
                }
                Ok(StepResult::IO) => {
                    stmt.run_once().unwrap();
                }
                _ => {
                    panic!("Failed to step through the statement");
                }
            }
        }
        let count: i64 = stmt.row().unwrap().get(0).unwrap();
        count
    }

    fn run_checkpoint_until_done(
        wal: &mut dyn Wal,
        pager: &crate::Pager,
        mode: CheckpointMode,
    ) -> CheckpointResult {
        pager.io.block(|| wal.checkpoint(pager, mode)).unwrap()
    }

    fn wal_header_snapshot(shared: &Arc<RwLock<WalFileShared>>) -> (u32, u32, u32, u32) {
        // (checkpoint_seq, salt1, salt2, page_size)
        let shared_guard = shared.read();
        let hdr = shared_guard.wal_header.lock();
        (hdr.checkpoint_seq, hdr.salt_1, hdr.salt_2, hdr.page_size)
    }

    #[test]
    fn restart_checkpoint_reset_wal_state_handling() {
        let (db, path) = get_database();

        let walpath = {
            let mut p = path.clone().into_os_string().into_string().unwrap();
            p.push_str("/test.db-wal");
            std::path::PathBuf::from(p)
        };

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 20, 3);
        let completions = conn.pager.write().cacheflush().unwrap();
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Snapshot header & counters before the RESTART checkpoint.
        let wal_shared = db.shared_wal.clone();
        let (seq_before, salt1_before, salt2_before, _ps_before) = wal_header_snapshot(&wal_shared);
        let (mx_before, backfill_before) = {
            let s = wal_shared.read();
            (
                s.max_frame.load(Ordering::SeqCst),
                s.nbackfills.load(Ordering::SeqCst),
            )
        };
        assert!(mx_before > 0);
        assert_eq!(backfill_before, 0);

        let meta_before = std::fs::metadata(&walpath).unwrap();
        #[cfg(unix)]
        let size_before = meta_before.blocks();
        #[cfg(not(unix))]
        let size_before = meta_before.len();
        // Run a RESTART checkpoint, should backfill everything and reset WAL counters,
        // but NOT truncate the file.
        {
            let pager = conn.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let res = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Restart);
            assert_eq!(res.num_attempted, mx_before);
            assert_eq!(res.num_backfilled, mx_before);
        }

        // Validate post‑RESTART header & counters.
        let (seq_after, salt1_after, salt2_after, _ps_after) = wal_header_snapshot(&wal_shared);
        assert_eq!(
            seq_after,
            seq_before.wrapping_add(1),
            "checkpoint_seq must increment on RESTART"
        );
        assert_eq!(
            salt1_after,
            salt1_before.wrapping_add(1),
            "salt_1 is incremented"
        );
        assert_ne!(salt2_after, salt2_before, "salt_2 is randomized");

        let (mx_after, backfill_after) = {
            let s = wal_shared.read();
            (
                s.max_frame.load(Ordering::SeqCst),
                s.nbackfills.load(Ordering::SeqCst),
            )
        };
        assert_eq!(mx_after, 0, "mxFrame reset to 0 after RESTART");
        assert_eq!(backfill_after, 0, "nBackfill reset to 0 after RESTART");

        // File size should be unchanged for RESTART (no truncate).
        let meta_after = std::fs::metadata(&walpath).unwrap();
        #[cfg(unix)]
        let size_after = meta_after.blocks();
        #[cfg(not(unix))]
        let size_after = meta_after.len();
        assert_eq!(
            size_before, size_after,
            "RESTART must not change WAL file size"
        );

        // Next write should start a new sequence at frame 1.
        conn.execute("insert into test(value) values ('post_restart')")
            .unwrap();
        conn.pager
            .write()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .finish_append_frames_commit()
            .unwrap();
        let new_max = wal_shared.read().max_frame.load(Ordering::SeqCst);
        assert_eq!(new_max, 1, "first append after RESTART starts at frame 1");

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_wal_passive_partial_then_complete() {
        let (db, _tmp) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn1.clone(), 15, 2);
        let completions = conn1.pager.write().cacheflush().unwrap();
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Force a read transaction that will freeze a lower read mark
        let readmark = {
            let pager = conn2.pager.write();
            let mut wal2 = pager.wal.as_ref().unwrap().borrow_mut();
            wal2.begin_read_tx().unwrap();
            wal2.get_max_frame()
        };

        // generate more frames that the reader will not see.
        bulk_inserts(&conn1.clone(), 15, 2);
        let completions = conn1.pager.write().cacheflush().unwrap();
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Run passive checkpoint, expect partial
        let (res1, max_before) = {
            let pager = conn1.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let res = run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
            let maxf = db.shared_wal.read().max_frame.load(Ordering::SeqCst);
            (res, maxf)
        };
        assert_eq!(res1.num_attempted, max_before);
        assert!(
            res1.num_backfilled < res1.num_attempted,
            "Partial backfill expected, {} : {}",
            res1.num_backfilled,
            res1.num_attempted
        );
        assert_eq!(
            res1.num_backfilled, readmark,
            "Checkpointed frames should match read mark"
        );
        // Release reader
        {
            let pager = conn2.pager.write();
            let wal2 = pager.wal.as_ref().unwrap().borrow_mut();
            wal2.end_read_tx();
        }

        // Second passive checkpoint should finish
        let pager = conn1.pager.read();
        let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
        let res2 = run_checkpoint_until_done(
            &mut *wal,
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert_eq!(
            res2.num_backfilled, res2.num_attempted,
            "Second checkpoint completes remaining frames"
        );
    }

    #[test]
    fn test_wal_restart_blocks_readers() {
        let (db, _) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        // Start a read transaction
        conn2
            .pager
            .write()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .begin_read_tx()
            .unwrap();

        // checkpoint should succeed here because the wal is fully checkpointed (empty)
        // so the reader is using readmark0 to read directly from the db file.
        let p = conn1.pager.read();
        let mut w = p.wal.as_ref().unwrap().borrow_mut();
        loop {
            match w.checkpoint(&p, CheckpointMode::Restart) {
                Ok(IOResult::IO(io)) => {
                    io.wait(db.io.as_ref()).unwrap();
                }
                e => {
                    assert!(
                        matches!(e, Err(LimboError::Busy)),
                        "reader is holding readmark0 we should return Busy"
                    );
                    break;
                }
            }
        }
        drop(w);
        conn2.pager.write().end_read_tx().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        for i in 0..10 {
            conn1
                .execute(format!("insert into test(value) values ('value{i}')"))
                .unwrap();
        }
        // now that we have some frames to checkpoint, try again
        conn2.pager.write().begin_read_tx().unwrap();
        let p = conn1.pager.read();
        let mut w = p.wal.as_ref().unwrap().borrow_mut();
        loop {
            match w.checkpoint(&p, CheckpointMode::Restart) {
                Ok(IOResult::IO(io)) => {
                    io.wait(db.io.as_ref()).unwrap();
                }
                Ok(IOResult::Done(_)) => {
                    panic!("Checkpoint should not have succeeded");
                }
                Err(e) => {
                    assert!(
                        matches!(e, LimboError::Busy),
                        "should return busy if we have readers"
                    );
                    break;
                }
            }
        }
    }

    #[test]
    fn test_wal_read_marks_after_restart() {
        let (db, _path) = get_database();
        let wal_shared = db.shared_wal.clone();

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 5);
        // Checkpoint with restart
        {
            let pager = conn.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let result = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Restart);
            assert!(result.everything_backfilled());
        }

        // Verify read marks after restart
        let read_marks_after: Vec<_> = {
            let s = wal_shared.read();
            (0..5).map(|i| s.read_locks[i].get_value()).collect()
        };

        assert_eq!(read_marks_after[0], 0, "Slot 0 should remain 0");
        assert_eq!(
            read_marks_after[1], 0,
            "Slot 1 (default reader) should be reset to 0"
        );
        for (i, item) in read_marks_after.iter().take(5).skip(2).enumerate() {
            assert_eq!(
                *item, READMARK_NOT_USED,
                "Slot {i} should be READMARK_NOT_USED after restart",
            );
        }
    }

    #[test]
    fn test_wal_concurrent_readers_during_checkpoint() {
        let (db, _path) = get_database();
        let conn_writer = db.connect().unwrap();

        conn_writer
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn_writer, 5, 10);

        // Start multiple readers at different points
        let conn_r1 = db.connect().unwrap();
        let conn_r2 = db.connect().unwrap();

        // R1 starts reading
        let r1_max_frame = {
            let pager = conn_r1.pager.write();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_read_tx().unwrap();
            wal.get_max_frame()
        };
        bulk_inserts(&conn_writer, 5, 10);

        // R2 starts reading, sees more frames than R1
        let r2_max_frame = {
            let pager = conn_r2.pager.write();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_read_tx().unwrap();
            wal.get_max_frame()
        };

        // try passive checkpoint, should only checkpoint up to R1's position
        let checkpoint_result = {
            let pager = conn_writer.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            )
        };

        assert!(
            checkpoint_result.num_backfilled < checkpoint_result.num_attempted,
            "Should not checkpoint all frames when readers are active"
        );
        assert_eq!(
            checkpoint_result.num_backfilled, r1_max_frame,
            "Should have checkpointed up to R1's max frame"
        );

        // Verify R2 still sees its frames
        assert_eq!(
            conn_r2
                .pager
                .read()
                .wal
                .as_ref()
                .unwrap()
                .borrow()
                .get_max_frame(),
            r2_max_frame,
            "Reader should maintain its snapshot"
        );
    }

    #[test]
    fn test_wal_checkpoint_updates_read_marks() {
        let (db, _path) = get_database();
        let wal_shared = db.shared_wal.clone();

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 5);

        // get max frame before checkpoint
        let max_frame_before = wal_shared.read().max_frame.load(Ordering::SeqCst);

        {
            let pager = conn.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let _result = run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
        }

        // check that read mark 1 (default reader) was updated to max_frame
        let read_mark_1 = wal_shared.read().read_locks[1].get_value();

        assert_eq!(
            read_mark_1 as u64, max_frame_before,
            "Read mark 1 should be updated to max frame during checkpoint"
        );
    }

    #[test]
    fn test_wal_writer_blocks_restart_checkpoint() {
        let (db, _path) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn1, 5, 5);

        // start a write transaction
        {
            let pager = conn2.pager.write();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let _ = wal.begin_read_tx().unwrap();
            wal.begin_write_tx().unwrap();
        }

        // should fail because writer lock is held
        let result = {
            let pager = conn1.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.checkpoint(&pager, CheckpointMode::Restart)
        };

        assert!(
            matches!(result, Err(LimboError::Busy)),
            "Restart checkpoint should fail when write lock is held"
        );

        conn2
            .pager
            .read()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .end_read_tx();
        // release write lock
        conn2
            .pager
            .read()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .end_write_tx();

        // now restart should succeed
        let result = {
            let pager = conn1.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Restart)
        };

        assert!(result.everything_backfilled());
    }

    #[test]
    #[should_panic(expected = "must have a read transaction to begin a write transaction")]
    fn test_wal_read_transaction_required_before_write() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();

        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();

        // Attempt to start a write transaction without a read transaction
        let pager = conn.pager.read();
        let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
        let _ = wal.begin_write_tx();
    }

    fn check_read_lock_slot(conn: &Arc<Connection>, expected_slot: usize) -> bool {
        let pager = conn.pager.read();
        let wal = pager.wal.as_ref().unwrap().borrow();
        #[cfg(debug_assertions)]
        {
            let wal_any = wal.as_any();
            if let Some(wal_file) = wal_any.downcast_ref::<WalFile>() {
                return wal_file.max_frame_read_lock_index.load(Ordering::Acquire) == expected_slot;
            }
        }

        false
    }

    #[test]
    fn test_wal_multiple_readers_at_different_frames() {
        let (db, _path) = get_database();
        let conn_writer = db.connect().unwrap();

        conn_writer
            .execute("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();

        fn start_reader(conn: &Arc<Connection>) -> (u64, crate::Statement) {
            conn.execute("BEGIN").unwrap();
            let mut stmt = conn.prepare("SELECT * FROM test").unwrap();
            stmt.step().unwrap();
            let frame = conn
                .pager
                .read()
                .wal
                .as_ref()
                .unwrap()
                .borrow()
                .get_max_frame();
            (frame, stmt)
        }

        bulk_inserts(&conn_writer, 3, 5);

        let conn1 = &db.connect().unwrap();
        let (r1_frame, _stmt) = start_reader(conn1); // reader 1

        bulk_inserts(&conn_writer, 3, 5);

        let conn_r2 = db.connect().unwrap();
        let (r2_frame, _stmt2) = start_reader(&conn_r2); // reader 2

        bulk_inserts(&conn_writer, 3, 5);

        let conn_r3 = db.connect().unwrap();
        let (r3_frame, _stmt3) = start_reader(&conn_r3); // reader 3

        assert!(r1_frame < r2_frame && r2_frame < r3_frame);

        // passive checkpoint #1
        let result1 = {
            let pager = conn_writer.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            )
        };
        assert_eq!(result1.num_backfilled, r1_frame);

        // finish reader‑1
        conn1.execute("COMMIT").unwrap();

        // passive checkpoint #2
        let result2 = {
            let pager = conn_writer.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            )
        };
        assert_eq!(result1.num_backfilled + result2.num_backfilled, r2_frame);

        // verify visible rows
        let mut stmt = conn_r2.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
        while !matches!(stmt.step().unwrap(), StepResult::Row) {
            stmt.run_once().unwrap();
        }
        let r2_cnt: i64 = stmt.row().unwrap().get(0).unwrap();

        let mut stmt2 = conn_r3.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
        while !matches!(stmt2.step().unwrap(), StepResult::Row) {
            stmt2.run_once().unwrap();
        }
        let r3_cnt: i64 = stmt2.row().unwrap().get(0).unwrap();
        assert_eq!(r2_cnt, 30);
        assert_eq!(r3_cnt, 45);
    }

    #[test]
    fn test_checkpoint_truncate_reset_handling() {
        let (db, path) = get_database();
        let conn = db.connect().unwrap();

        let walpath = {
            let mut p = path.clone().into_os_string().into_string().unwrap();
            p.push_str("/test.db-wal");
            std::path::PathBuf::from(p)
        };

        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 10);

        // Get size before checkpoint
        let size_before = std::fs::metadata(&walpath).unwrap().len();
        assert!(size_before > 0, "WAL file should have content");

        // Do a TRUNCATE checkpoint
        {
            let pager = conn.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
            );
        }

        // Check file size after truncate
        let size_after = std::fs::metadata(&walpath).unwrap().len();
        assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

        // Verify we can still write to the database
        conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
            .unwrap();

        // Check WAL has new content
        let new_size = std::fs::metadata(&walpath).unwrap().len();
        assert!(new_size >= 32, "WAL file too small");
        let hdr = read_wal_header(&walpath);
        let expected_magic = if cfg!(target_endian = "big") {
            sqlite3_ondisk::WAL_MAGIC_BE
        } else {
            sqlite3_ondisk::WAL_MAGIC_LE
        };
        assert!(
            hdr.magic == expected_magic,
            "bad WAL magic: {:#X}, expected: {:#X}",
            hdr.magic,
            sqlite3_ondisk::WAL_MAGIC_BE
        );
        assert_eq!(hdr.file_format, 3007000);
        assert_eq!(hdr.page_size, 4096, "invalid page size");
        assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_wal_checkpoint_truncate_db_file_contains_data() {
        let (db, path) = get_database();
        let conn = db.connect().unwrap();

        let walpath = {
            let mut p = path.clone().into_os_string().into_string().unwrap();
            p.push_str("/test.db-wal");
            std::path::PathBuf::from(p)
        };

        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 100);

        // Get size before checkpoint
        let size_before = std::fs::metadata(&walpath).unwrap().len();
        assert!(size_before > 0, "WAL file should have content");

        // Do a TRUNCATE checkpoint
        {
            let pager = conn.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
            );
        }

        // Check file size after truncate
        let size_after = std::fs::metadata(&walpath).unwrap().len();
        assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

        // Verify we can still write to the database
        conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
            .unwrap();

        // Check WAL has new content
        let new_size = std::fs::metadata(&walpath).unwrap().len();
        assert!(new_size >= 32, "WAL file too small");
        let hdr = read_wal_header(&walpath);
        let expected_magic = if cfg!(target_endian = "big") {
            sqlite3_ondisk::WAL_MAGIC_BE
        } else {
            sqlite3_ondisk::WAL_MAGIC_LE
        };
        assert!(
            hdr.magic == expected_magic,
            "bad WAL magic: {:#X}, expected: {:#X}",
            hdr.magic,
            sqlite3_ondisk::WAL_MAGIC_BE
        );
        assert_eq!(hdr.file_format, 3007000);
        assert_eq!(hdr.page_size, 4096, "invalid page size");
        assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
        {
            let pager = conn.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
        }
        // delete the WAL file so we can read right from db and assert
        // that everything was backfilled properly
        std::fs::remove_file(&walpath).unwrap();

        let count = count_test_table(&conn);
        assert_eq!(
            count, 1001,
            "we should have 1001 rows in the table all together"
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    fn read_wal_header(path: &std::path::Path) -> sqlite3_ondisk::WalHeader {
        use std::{fs::File, io::Read};
        let mut hdr = [0u8; 32];
        File::open(path).unwrap().read_exact(&mut hdr).unwrap();
        let be = |i| u32::from_be_bytes(hdr[i..i + 4].try_into().unwrap());
        sqlite3_ondisk::WalHeader {
            magic: be(0x00),
            file_format: be(0x04),
            page_size: be(0x08),
            checkpoint_seq: be(0x0C),
            salt_1: be(0x10),
            salt_2: be(0x14),
            checksum_1: be(0x18),
            checksum_2: be(0x1C),
        }
    }

    #[test]
    fn test_wal_stale_snapshot_in_write_transaction() {
        let (db, _path) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        // Start a read transaction on conn2
        {
            let pager = conn2.pager.write();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_read_tx().unwrap();
        }
        // Make changes using conn1
        bulk_inserts(&conn1, 5, 5);
        // Try to start a write transaction on conn2 with a stale snapshot
        let result = {
            let pager = conn2.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_write_tx()
        };
        // Should get Busy due to stale snapshot
        assert!(matches!(result, Err(LimboError::Busy)));

        // End read transaction and start a fresh one
        {
            let pager = conn2.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.end_read_tx();
            wal.begin_read_tx().unwrap();
        }
        // Now write transaction should work
        let result = {
            let pager = conn2.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_write_tx()
        };
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_wal_readlock0_optimization_behavior() {
        let (db, _path) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn1, 5, 5);
        // Do a full checkpoint to move all data to DB file
        {
            let pager = conn1.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(
                &mut *wal,
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
        }

        // Start a read transaction on conn2
        {
            let pager = conn2.pager.write();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_read_tx().unwrap();
        }
        // should use slot 0, as everything is backfilled
        assert!(check_read_lock_slot(&conn2, 0));
        {
            let pager = conn1.pager.read();
            let wal = pager.wal.as_ref().unwrap().borrow();
            let frame = wal.find_frame(5, None);
            // since we hold readlock0, we should ignore the db file and find_frame should return none
            assert!(frame.is_ok_and(|f| f.is_none()));
        }
        // Try checkpoint, should fail because reader has slot 0
        {
            let pager = conn1.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let result = wal.checkpoint(&pager, CheckpointMode::Restart);

            assert!(
                matches!(result, Err(LimboError::Busy)),
                "RESTART checkpoint should fail when a reader is using slot 0"
            );
        }
        // End the read transaction
        {
            let pager = conn2.pager.read();
            let wal = pager.wal.as_ref().unwrap().borrow();
            wal.end_read_tx();
        }
        {
            let pager = conn1.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let result = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Restart);
            assert!(
                result.everything_backfilled(),
                "RESTART checkpoint should succeed after reader releases slot 0"
            );
        }
    }

    #[test]
    fn test_wal_full_backfills_all() {
        let (db, _tmp) = get_database();
        let conn = db.connect().unwrap();

        // Write some data to put frames in the WAL
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 8, 4);

        // Ensure frames are flushed to the WAL
        let completions = conn.pager.write().cacheflush().unwrap();
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Snapshot the current mxFrame before running FULL
        let wal_shared = db.shared_wal.clone();
        let mx_before = wal_shared.read().max_frame.load(Ordering::SeqCst);
        assert!(mx_before > 0, "expected frames in WAL before FULL");

        // Run FULL checkpoint - must backfill *all* frames up to mx_before
        let result = {
            let pager = conn.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Full)
        };

        assert_eq!(result.num_attempted, mx_before);
        assert_eq!(result.num_backfilled, mx_before);
    }

    #[test]
    fn test_wal_full_waits_for_old_reader_then_succeeds() {
        let (db, _tmp) = get_database();
        let writer = db.connect().unwrap();
        let reader = db.connect().unwrap();

        writer
            .execute("create table test(id integer primary key, value text)")
            .unwrap();

        // First commit some data and flush (reader will snapshot here)
        bulk_inserts(&writer, 2, 3);
        let completions = writer.pager.write().cacheflush().unwrap();
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Start a read transaction pinned at the current snapshot
        {
            let pager = reader.pager.write();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_read_tx().unwrap();
        }
        let r_snapshot = {
            let pager = reader.pager.read();
            let wal = pager.wal.as_ref().unwrap().borrow();
            wal.get_max_frame()
        };

        // Advance WAL beyond the reader's snapshot
        bulk_inserts(&writer, 3, 4);
        let completions = writer.pager.write().cacheflush().unwrap();
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }
        let mx_now = db.shared_wal.read().max_frame.load(Ordering::SeqCst);
        assert!(mx_now > r_snapshot);

        // FULL must return Busy while a reader is stuck behind
        {
            let pager = writer.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            loop {
                match wal.checkpoint(&pager, CheckpointMode::Full) {
                    Ok(IOResult::IO(io)) => {
                        // Drive any pending IO (should quickly become Busy or Done)
                        io.wait(db.io.as_ref()).unwrap();
                    }
                    Err(LimboError::Busy) => {
                        break;
                    }
                    other => panic!("expected Busy from FULL with old reader, got {other:?}"),
                }
            }
        }

        // Release the reader, now full mode should succeed and backfill everything
        {
            let pager = reader.pager.read();
            let wal = pager.wal.as_ref().unwrap().borrow();
            wal.end_read_tx();
        }

        let result = {
            let pager = writer.pager.read();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Full)
        };

        assert_eq!(result.num_attempted, mx_now - r_snapshot);
        assert!(result.everything_backfilled());
    }
}
