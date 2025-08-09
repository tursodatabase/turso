#![allow(clippy::arc_with_non_send_sync)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::array;
use std::cell::UnsafeCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use strum::EnumString;
use tracing::{instrument, Level};

use std::fmt::Formatter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{
    cell::{Cell, RefCell},
    fmt,
    rc::Rc,
    sync::Arc,
};

use crate::fast_lock::SpinLock;
use crate::io::{File, IO};
use crate::result::LimboResult;
use crate::storage::sqlite3_ondisk::{
    begin_read_wal_frame, begin_read_wal_frame_raw, finish_read_page, prepare_wal_frame,
    write_pages_vectored, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use crate::types::IOResult;
use crate::{turso_assert, Buffer, LimboError, Result};
use crate::{Completion, Page};

use self::sqlite3_ondisk::{checksum_wal, PageContent, WAL_MAGIC_BE, WAL_MAGIC_LE};

use super::buffer_pool::BufferPool;
use super::pager::{PageRef, Pager};
use super::sqlite3_ondisk::{self, WalHeader};

#[derive(Debug, Clone, Default)]
pub struct CheckpointResult {
    /// number of frames in WAL
    pub num_wal_frames: u64,
    /// number of frames moved successfully from WAL to db file after checkpoint
    pub num_checkpointed_frames: u64,
    /// In the case of everything backfilled, we need to hold the locks until the db
    /// file is truncated.
    maybe_guard: Option<CheckpointLocks>,
}

impl Drop for CheckpointResult {
    fn drop(&mut self) {
        let _ = self.maybe_guard.take();
    }
}

impl CheckpointResult {
    pub fn new(n_frames: u64, n_ckpt: u64) -> Self {
        Self {
            num_wal_frames: n_frames,
            num_checkpointed_frames: n_ckpt,
            maybe_guard: None,
        }
    }

    pub const fn everything_backfilled(&self) -> bool {
        self.num_wal_frames == self.num_checkpointed_frames
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
    Passive,
    /// This mode blocks until there is no database writer and all readers are reading from the most recent database snapshot. It then checkpoints all frames in the log file and syncs the database file. This mode blocks new database writers while it is pending, but new database readers are allowed to continue unimpeded.
    Full,
    /// This mode works the same way as `Full` with the addition that after checkpointing the log file it blocks (calls the busy-handler callback) until all readers are reading from the database file only. This ensures that the next writer will restart the log file from the beginning. Like `Full`, this mode blocks new database writer attempts while it is pending, but does not impede readers.
    Restart,
    /// This mode works the same way as `Restart` with the addition that it also truncates the log file to zero bytes just prior to a successful return.
    Truncate,
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
            .compare_exchange(cur, desired, Ordering::Acquire, Ordering::Relaxed)
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
        let cur = self.0.load(Ordering::Relaxed);
        turso_assert!(Self::has_writer(cur), "must hold exclusive lock");
        let desired = (cur & !Self::VALUE_MASK) | ((v as u64) << Self::VALUE_SHIFT);
        self.0.store(desired, Ordering::Relaxed);
    }
}

/// Write-ahead log (WAL).
pub trait Wal {
    /// Begin a read transaction.
    fn begin_read_tx(&mut self) -> Result<(LimboResult, bool)>;

    /// Begin a write transaction.
    fn begin_write_tx(&mut self) -> Result<LimboResult>;

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

    /// Write a frame to the WAL.
    /// db_size is the database size in pages after the transaction finishes.
    /// db_size > 0    -> last frame written in transaction
    /// db_size == 0   -> non-last frame written in transaction
    /// write_counter is the counter we use to track when the I/O operation starts and completes
    fn append_frame(
        &mut self,
        page: PageRef,
        db_size: u32,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<Completion>;

    /// Complete append of frames by updating shared wal state. Before this
    /// all changes were stored locally.
    fn finish_append_frames_commit(&mut self) -> Result<()>;

    fn should_checkpoint(&self) -> bool;
    fn checkpoint(
        &mut self,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
        mode: CheckpointMode,
    ) -> Result<IOResult<CheckpointResult>>;
    fn sync(&mut self) -> Result<IOResult<()>>;
    fn get_max_frame_in_wal(&self) -> u64;
    fn get_max_frame(&self) -> u64;
    fn get_min_frame(&self) -> u64;
    fn rollback(&mut self) -> Result<()>;

    /// Return unique set of pages changed **after** frame_watermark position and until current WAL session max_frame_no
    fn changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>>;

    #[cfg(debug_assertions)]
    fn as_any(&self) -> &dyn std::any::Any;
}

// Syncing requires a state machine because we need to schedule a sync and then wait until it is
// finished. If we don't wait there will be undefined behaviour that no one wants to debug.
#[derive(Copy, Clone, Debug)]
enum SyncState {
    NotSyncing,
    Syncing,
}

#[derive(Debug, Copy, Clone)]
pub enum CheckpointState {
    Start,
    ReadFrame,
    WaitReadFrame,
    AccumulatePage,
    FlushBatch,
    WaitFlush,
    Done,
}

/// IOV_MAX is 1024 on most systems, lets use 512 to be safe
pub const CKPT_BATCH_PAGES: usize = 512;
type PageId = usize;

/// Batch is a collection of pages that are being checkpointed together. It is used to
/// aggregate contiguous pages into a single write operation to the database file.
pub(super) struct Batch {
    items: BTreeMap<PageId, Arc<Buffer>>,
}
// TODO(preston): implement the same thing for `readv`
impl Batch {
    fn new() -> Self {
        Self {
            items: BTreeMap::new(),
        }
    }
    fn is_full(&self) -> bool {
        self.items.len() >= CKPT_BATCH_PAGES
    }
    fn add_to_batch(&mut self, scratch: &PageRef, pool: &Arc<BufferPool>) {
        let (id, buf_clone) = unsafe {
            let inner = &*scratch.inner.get();
            let id = inner.id;
            let contents = inner.contents.as_ref().expect("scratch has contents");
            let buf = contents.buffer.clone();
            (id, buf)
        };
        // Insert the new batch item at the correct position
        self.items.insert(id, buf_clone);

        // Re-initialize scratch with a fresh buffer
        let raw = pool.get_page();
        let new_buf = Arc::new(raw);

        unsafe {
            let inner = &mut *scratch.inner.get();
            inner.contents = Some(PageContent::new(0, new_buf));
            // reset flags on scratch so it won't be cleared later with the real page
            inner.flags.store(0, Ordering::Release);
        }
    }
}

impl std::ops::Deref for Batch {
    type Target = BTreeMap<PageId, Arc<Buffer>>;
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
impl std::ops::DerefMut for Batch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.items
    }
}

// Checkpointing is a state machine that has multiple steps. Since there are multiple steps we save
// in flight information of the checkpoint in OngoingCheckpoint. page is just a helper Page to do
// page operations like reading a frame to a page, and writing a page to disk. This page should not
// be placed back in pager page cache or anything, it's just a helper.
// min_frame and max_frame is the range of frames that can be safely transferred from WAL to db
// file.
// current_page is a helper to iterate through all the pages that might have a frame in the safe
// range. This is inefficient for now.
struct OngoingCheckpoint {
    scratch_page: PageRef,
    batch: Batch,
    state: CheckpointState,
    pending_flush: Option<PendingFlush>,
    min_frame: u64,
    max_frame: u64,
    current_page: u64,
}

pub(super) struct PendingFlush {
    // page ids to clear
    pub(super) pages: Vec<usize>,
    // completion flag set by IO callback
    pub(super) done: Arc<AtomicBool>,
}

impl Default for PendingFlush {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingFlush {
    pub fn new() -> Self {
        Self {
            pages: Vec::with_capacity(CKPT_BATCH_PAGES),
            done: Arc::new(AtomicBool::new(false)),
        }
    }
    // clear the dirty flag of all pages in the pending flush batch
    fn clear_dirty(&self, pager: &Pager) {
        for id in &self.pages {
            if let Some(p) = pager.cache_get(*id) {
                p.clear_dirty();
            }
        }
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

#[allow(dead_code)]
pub struct WalFile {
    io: Arc<dyn IO>,
    buffer_pool: Arc<BufferPool>,

    syncing: Rc<Cell<bool>>,
    sync_state: Cell<SyncState>,

    shared: Arc<UnsafeCell<WalFileShared>>,
    ongoing_checkpoint: OngoingCheckpoint,
    checkpoint_threshold: usize,
    // min and max frames for this connection
    /// This is the index to the read_lock in WalFileShared that we are holding. This lock contains
    /// the max frame for this connection.
    max_frame_read_lock_index: Cell<usize>,
    /// Max frame allowed to lookup range=(minframe..max_frame)
    max_frame: u64,
    /// Start of range to look for frames range=(minframe..max_frame)
    min_frame: u64,
    /// Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    last_checksum: (u32, u32),

    /// Count of possible pages to checkpoint, and number of backfilled
    prev_checkpoint: CheckpointResult,

    /// Private copy of WalHeader
    pub header: WalHeader,

    /// Manages locks needed for checkpointing
    checkpoint_guard: Option<CheckpointLocks>,
}

impl fmt::Debug for WalFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFile")
            .field("syncing", &self.syncing.get())
            .field("sync_state", &self.sync_state)
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
#[allow(dead_code)]
pub struct WalFileShared {
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
    // Another memory inefficient array made to just keep track of pages that are in frame_cache.
    pub pages_in_frames: Arc<SpinLock<Vec<u64>>>,
    pub last_checksum: (u32, u32), // Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    pub file: Arc<dyn File>,

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
}

impl fmt::Debug for WalFileShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFileShared")
            .field("wal_header", &self.wal_header)
            .field("min_frame", &self.min_frame)
            .field("max_frame", &self.max_frame)
            .field("nbackfills", &self.nbackfills)
            .field("frame_cache", &self.frame_cache)
            .field("pages_in_frames", &self.pages_in_frames)
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
    Writer { ptr: Arc<UnsafeCell<WalFileShared>> },
    Read0 { ptr: Arc<UnsafeCell<WalFileShared>> },
}

/// Database checkpointers takes the following locks, in order:
/// The exclusive CHECKPOINTER lock.
/// The exclusive WRITER lock (FULL, RESTART and TRUNCATE only).
/// Exclusive lock on read-mark slots 1-N. These are immediately released after being taken.
/// Exclusive lock on read-mark 0.
/// Exclusive lock on read-mark slots 1-N again. These are immediately released after being taken (RESTART and TRUNCATE only).
/// All of the above use blocking locks.
impl CheckpointLocks {
    fn new(ptr: Arc<UnsafeCell<WalFileShared>>, mode: CheckpointMode) -> Result<Self> {
        let shared = &mut unsafe { &mut *ptr.get() };
        if !shared.checkpoint_lock.write() {
            tracing::trace!("CheckpointGuard::new: checkpoint lock failed, returning Busy");
            // we hold the exclusive checkpoint lock no matter which mode for the duration
            return Err(LimboError::Busy);
        }
        match mode {
            // Passive mode is the only mode not requiring a write lock, as it doesn't block
            // readers or writers. It acquires the checkpoint lock to ensure that no other
            // concurrent checkpoint happens, and acquires the exclusive read lock 0
            // to ensure that no readers read from a partially checkpointed db file.
            CheckpointMode::Passive => {
                let read0 = &mut shared.read_locks[0];
                if !read0.write() {
                    shared.checkpoint_lock.unlock();
                    tracing::trace!("CheckpointGuard: read0 lock failed, returning Busy");
                    // for passive and full we need to hold the read0 lock
                    return Err(LimboError::Busy);
                }
                Ok(Self::Read0 { ptr })
            }
            CheckpointMode::Full => {
                // Full blocks writers and holds read0 exclusively (readers may still use >0 slots)
                let read0 = &mut shared.read_locks[0];
                if !read0.write() {
                    shared.checkpoint_lock.unlock();
                    tracing::trace!("CheckpointGuard: read0 lock failed (Full), Busy");
                    return Err(LimboError::Busy);
                }
                if !shared.write_lock.write() {
                    read0.unlock();
                    shared.checkpoint_lock.unlock();
                    tracing::trace!("CheckpointGuard: write lock failed (Full), Busy");
                    return Err(LimboError::Busy);
                }
                Ok(Self::Writer { ptr })
            }
            CheckpointMode::Restart | CheckpointMode::Truncate => {
                // like all modes, we must acquire an exclusive checkpoint lock and lock on read 0
                // to prevent a reader from reading a partially checkpointed db file.
                let read0 = &mut shared.read_locks[0];
                if !read0.write() {
                    shared.checkpoint_lock.unlock();
                    tracing::trace!("CheckpointGuard: read0 lock failed, returning Busy");
                    return Err(LimboError::Busy);
                }
                // if we are resetting the log we must hold the write lock for the duration.
                // ensures no writer can append frames while we reset the log.
                if !shared.write_lock.write() {
                    shared.checkpoint_lock.unlock();
                    read0.unlock();
                    tracing::trace!("CheckpointGuard: write lock failed, returning Busy");
                    return Err(LimboError::Busy);
                }
                Ok(Self::Writer { ptr })
            }
        }
    }
}

impl Drop for CheckpointLocks {
    fn drop(&mut self) {
        match self {
            CheckpointLocks::Writer { ptr: shared } => unsafe {
                (*shared.get()).write_lock.unlock();
                (*shared.get()).read_locks[0].unlock();
                (*shared.get()).checkpoint_lock.unlock();
            },
            CheckpointLocks::Read0 { ptr: shared } => unsafe {
                (*shared.get()).read_locks[0].unlock();
                (*shared.get()).checkpoint_lock.unlock();
            },
        }
    }
}

impl Wal for WalFile {
    /// Begin a read transaction. The caller must ensure that there is not already
    /// an ongoing read transaction.
    /// sqlite/src/wal.c 3023
    /// assert(pWal->readLock < 0); /* Not currently locked */
    #[instrument(skip_all, level = Level::DEBUG)]
    fn begin_read_tx(&mut self) -> Result<(LimboResult, bool)> {
        turso_assert!(
            self.max_frame_read_lock_index.get().eq(&NO_LOCK_HELD),
            "cannot start a new read tx without ending an existing one, lock_value={}, expected={}",
            self.max_frame_read_lock_index.get(),
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
        let db_changed = shared_max != self.max_frame
            || last_checksum != self.last_checksum
            || checkpoint_seq != self.header.checkpoint_seq;

        // WAL is already fully back‑filled into the main DB image
        // (mxFrame == nBackfill). Readers can therefore ignore the
        // WAL and fetch pages directly from the DB file.  We do this
        // by taking read‑lock 0, and capturing the latest state.
        if shared_max == nbackfills {
            let lock_idx = 0;
            if !self.get_shared().read_locks[lock_idx].read() {
                return Ok((LimboResult::Busy, db_changed));
            }
            // we need to keep self.max_frame set to the appropriate
            // max frame in the wal at the time this transaction starts.
            self.max_frame = shared_max;
            self.max_frame_read_lock_index.set(lock_idx);
            self.min_frame = nbackfills + 1;
            self.last_checksum = last_checksum;
            return Ok((LimboResult::Ok, db_changed));
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
            for (idx, lock) in self.get_shared().read_locks.iter_mut().enumerate().skip(1) {
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

        if best_idx == -1 {
            return Ok((LimboResult::Busy, db_changed));
        }

        // Now take a shared read on that slot, and if we are successful,
        // grab another snapshot of the shared state.
        let (mx2, nb2, cksm2, ckpt_seq2) = {
            let shared = self.get_shared();
            if !shared.read_locks[best_idx as usize].read() {
                // TODO: we should retry here instead of always returning Busy
                return Ok((LimboResult::Busy, db_changed));
            }
            (
                shared.max_frame.load(Ordering::Acquire),
                shared.nbackfills.load(Ordering::Acquire),
                shared.last_checksum,
                shared.wal_header.lock().checkpoint_seq,
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
        self.min_frame = nb2 + 1;
        if mx2 != shared_max
            || nb2 != nbackfills
            || cksm2 != last_checksum
            || ckpt_seq2 != checkpoint_seq
        {
            return Err(LimboError::Busy);
        }
        self.max_frame = best_mark as u64;
        self.max_frame_read_lock_index.set(best_idx as usize);
        tracing::debug!(
            "begin_read_tx(min={}, max={}, slot={}, max_frame_in_wal={})",
            self.min_frame,
            self.max_frame,
            best_idx,
            shared_max
        );
        Ok((LimboResult::Ok, db_changed))
    }

    /// End a read transaction.
    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    fn end_read_tx(&self) {
        let slot = self.max_frame_read_lock_index.get();
        if slot != NO_LOCK_HELD {
            let rl = &mut self.get_shared().read_locks[slot];
            rl.unlock();
            self.max_frame_read_lock_index.set(NO_LOCK_HELD);
            tracing::debug!("end_read_tx(slot={slot})");
        } else {
            tracing::debug!("end_read_tx(slot=no_lock)");
        }
    }

    /// Begin a write transaction
    #[instrument(skip_all, level = Level::DEBUG)]
    fn begin_write_tx(&mut self) -> Result<LimboResult> {
        let shared = self.get_shared();
        // sqlite/src/wal.c 3702
        // Cannot start a write transaction without first holding a read
        // transaction.
        // assert(pWal->readLock >= 0);
        // assert(pWal->writeLock == 0 && pWal->iReCksum == 0);
        turso_assert!(
            self.max_frame_read_lock_index.get() != NO_LOCK_HELD,
            "must have a read transaction to begin a write transaction"
        );
        if !shared.write_lock.write() {
            return Ok(LimboResult::Busy);
        }
        let (shared_max, nbackfills, last_checksum) = {
            let shared = self.get_shared();
            (
                shared.max_frame.load(Ordering::Acquire),
                shared.nbackfills.load(Ordering::Acquire),
                shared.last_checksum,
            )
        };
        if self.max_frame == shared_max {
            // Snapshot still valid; adopt counters
            self.last_checksum = last_checksum;
            self.min_frame = nbackfills + 1;
            return Ok(LimboResult::Ok);
        }

        // Snapshot is stale, give up and let caller retry from scratch
        shared.write_lock.unlock();
        Ok(LimboResult::Busy)
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
            frame_watermark.unwrap_or(0) <= self.max_frame,
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

        // if we are holding read_lock 0, skip and read right from db file.
        if self.max_frame_read_lock_index.get() == 0 {
            return Ok(None);
        }
        let shared = self.get_shared();
        let frames = shared.frame_cache.lock();
        let range = frame_watermark
            .map(|x| 0..=x)
            .unwrap_or(self.min_frame..=self.max_frame);
        if let Some(list) = frames.get(&page_id) {
            if let Some(f) = list.iter().rfind(|&&f| range.contains(&f)) {
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
        tracing::debug!("read_frame({})", frame_id);
        let offset = self.frame_offset(frame_id);
        page.set_locked();
        let frame = page.clone();
        let complete = Box::new(move |buf: Arc<Buffer>, bytes_read: i32| {
            let buf_len = buf.len();
            turso_assert!(
                bytes_read == buf_len as i32,
                "read({bytes_read}) less than expected({buf_len}): frame_id={frame_id}"
            );
            let frame = frame.clone();
            finish_read_page(page.get().id, buf, frame).unwrap();
        });
        begin_read_wal_frame(
            &self.get_shared().file,
            offset + WAL_FRAME_HEADER_SIZE,
            buffer_pool,
            complete,
        )
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_frame_raw(&self, frame_id: u64, frame: &mut [u8]) -> Result<Completion> {
        tracing::debug!("read_frame({})", frame_id);
        let offset = self.frame_offset(frame_id);
        let (frame_ptr, frame_len) = (frame.as_mut_ptr(), frame.len());
        let complete = Box::new(move |buf: Arc<Buffer>, bytes_read: i32| {
            let buf_len = buf.len();
            turso_assert!(
                bytes_read == buf_len as i32,
                "read({bytes_read}) != expected({buf_len})"
            );
            let buf_ptr = buf.as_mut_ptr();
            unsafe {
                std::ptr::copy_nonoverlapping(buf_ptr, frame_ptr, frame_len);
            }
        });
        let c =
            begin_read_wal_frame_raw(&self.buffer_pool, &self.get_shared().file, offset, complete)?;
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
        tracing::debug!("write_raw_frame({})", frame_id);
        if page.len() != self.page_size() as usize {
            return Err(LimboError::InvalidArgument(format!(
                "unexpected page size in frame: got={}, expected={}",
                page.len(),
                self.page_size(),
            )));
        }
        if frame_id > self.max_frame + 1 {
            // attempt to write frame out of sequential order - error out
            return Err(LimboError::InvalidArgument(format!(
                "frame_id is beyond next frame in the WAL: frame_id={}, max_frame={}",
                frame_id, self.max_frame
            )));
        }
        if frame_id <= self.max_frame {
            // just validate if page content from the frame matches frame in the WAL
            let offset = self.frame_offset(frame_id);
            let conflict = Arc::new(Cell::new(false));
            let (page_ptr, page_len) = (page.as_ptr(), page.len());
            let complete = Box::new({
                let conflict = conflict.clone();
                move |buf: Arc<Buffer>, bytes_read: i32| {
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
            let c = begin_read_wal_frame(
                &self.get_shared().file,
                offset + WAL_FRAME_HEADER_SIZE,
                buffer_pool,
                complete,
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
        let shared = self.get_shared();
        let header = shared.wal_header.clone();
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
        let c = shared.file.pwrite(offset, frame_bytes, c)?;
        self.io.wait_for_completion(c)?;
        self.complete_append_frame(page_id, frame_id, checksums);
        if db_size > 0 {
            self.finish_append_frames_commit()?;
        }
        Ok(())
    }

    /// Write a frame to the WAL.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn append_frame(
        &mut self,
        page: PageRef,
        db_size: u32,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<Completion> {
        let shared = self.get_shared();
        if shared.max_frame.load(Ordering::Acquire).eq(&0) {
            self.ensure_header_if_needed()?;
        }
        let page_id = page.get().id;
        let frame_id = self.max_frame + 1;
        let offset = self.frame_offset(frame_id);
        tracing::debug!(frame_id, offset, page_id);
        let (c, checksums) = {
            let shared = self.get_shared();
            let header = shared.wal_header.clone();
            let header = header.lock();
            let checksums = self.last_checksum;
            let page_content = page.get_contents();
            let page_buf = page_content.as_ptr();
            let (frame_checksums, frame_bytes) = prepare_wal_frame(
                &self.buffer_pool,
                &header,
                checksums,
                header.page_size,
                page_id as u32,
                db_size,
                page_buf,
            );

            *write_counter.borrow_mut() += 1;
            let c = Completion::new_write({
                let frame_bytes = frame_bytes.clone();
                let write_counter = write_counter.clone();
                move |bytes_written| {
                    let frame_len = frame_bytes.len();
                    turso_assert!(
                        bytes_written == frame_len as i32,
                        "wrote({bytes_written}) != expected({frame_len})"
                    );

                    page.clear_dirty();
                    *write_counter.borrow_mut() -= 1;
                }
            });
            let result = shared.file.pwrite(offset, frame_bytes.clone(), c);
            if let Err(err) = result {
                *write_counter.borrow_mut() -= 1;
                return Err(err);
            }
            (result.unwrap(), frame_checksums)
        };
        self.complete_append_frame(page_id as u64, frame_id, checksums);
        Ok(c)
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
        _write_counter: Rc<RefCell<usize>>,
        mode: CheckpointMode,
    ) -> Result<IOResult<CheckpointResult>> {
        self.checkpoint_inner(pager, _write_counter, mode)
            .inspect_err(|_| {
                let _ = self.checkpoint_guard.take();
                self.ongoing_checkpoint.state = CheckpointState::Start;
            })
    }

    #[instrument(err, skip_all, level = Level::DEBUG)]
    fn sync(&mut self) -> Result<IOResult<()>> {
        match self.sync_state.get() {
            SyncState::NotSyncing => {
                tracing::debug!("wal_sync");
                let syncing = self.syncing.clone();
                self.syncing.set(true);
                let completion = Completion::new_sync(move |_| {
                    tracing::debug!("wal_sync finish");
                    syncing.set(false);
                });
                let shared = self.get_shared();
                let _c = shared.file.sync(completion)?;
                self.sync_state.set(SyncState::Syncing);
                Ok(IOResult::IO)
            }
            SyncState::Syncing => {
                if self.syncing.get() {
                    tracing::debug!("wal_sync is already syncing");
                    Ok(IOResult::IO)
                } else {
                    self.sync_state.set(SyncState::NotSyncing);
                    Ok(IOResult::Done(()))
                }
            }
        }
    }

    fn get_max_frame_in_wal(&self) -> u64 {
        self.get_shared().max_frame.load(Ordering::Acquire)
    }

    fn get_max_frame(&self) -> u64 {
        self.max_frame
    }

    fn get_min_frame(&self) -> u64 {
        self.min_frame
    }

    #[instrument(err, skip_all, level = Level::DEBUG)]
    fn rollback(&mut self) -> Result<()> {
        // TODO(pere): have to remove things from frame_cache because they are no longer valid.
        // TODO(pere): clear page cache in pager.
        {
            // TODO(pere): implement proper hashmap, this sucks :).
            let shared = self.get_shared();
            let max_frame = shared.max_frame.load(Ordering::Acquire);
            tracing::debug!(to_max_frame = max_frame);
            {
                let mut frame_cache = shared.frame_cache.lock();
                for (_, frames) in frame_cache.iter_mut() {
                    let mut last_valid_frame = frames.len();
                    for frame in frames.iter().rev() {
                        if *frame <= max_frame {
                            break;
                        }
                        last_valid_frame -= 1;
                    }
                    frames.truncate(last_valid_frame);
                }
                let mut pages_in_frames = shared.pages_in_frames.lock();
                pages_in_frames.retain(|page| {
                    frame_cache.contains_key(page) && !frame_cache.get(page).unwrap().is_empty()
                });
            }
            self.last_checksum = shared.last_checksum;
        }
        self.reset_internal_states();
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn finish_append_frames_commit(&mut self) -> Result<()> {
        let shared = self.get_shared();
        shared.max_frame.store(self.max_frame, Ordering::Release);
        tracing::trace!(self.max_frame, ?self.last_checksum);
        shared.last_checksum = self.last_checksum;
        Ok(())
    }

    fn changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        let frame_count = self.get_max_frame();
        let page_size = self.page_size();
        let mut frame = vec![0u8; page_size as usize + WAL_FRAME_HEADER_SIZE];
        let mut seen = HashSet::new();
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

    #[cfg(debug_assertions)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl WalFile {
    pub fn new(
        io: Arc<dyn IO>,
        shared: Arc<UnsafeCell<WalFileShared>>,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let checkpoint_page = Arc::new(Page::new(0));
        let buffer = buffer_pool.get_page();
        {
            checkpoint_page.get().contents = Some(PageContent::new(0, Arc::new(buffer)));
        }

        let header = unsafe { shared.get().as_mut().unwrap().wal_header.lock() };
        let last_checksum = unsafe { (*shared.get()).last_checksum };
        Self {
            io,
            // default to max frame in WAL, so that when we read schema we can read from WAL too if it's there.
            max_frame: unsafe { (*shared.get()).max_frame.load(Ordering::Acquire) },
            shared,
            ongoing_checkpoint: OngoingCheckpoint {
                scratch_page: checkpoint_page,
                batch: Batch::new(),
                pending_flush: None,
                state: CheckpointState::Start,
                min_frame: 0,
                max_frame: 0,
                current_page: 0,
            },
            checkpoint_threshold: 1000,
            buffer_pool,
            syncing: Rc::new(Cell::new(false)),
            sync_state: Cell::new(SyncState::NotSyncing),
            min_frame: 0,
            max_frame_read_lock_index: NO_LOCK_HELD.into(),
            last_checksum,
            prev_checkpoint: CheckpointResult::default(),
            checkpoint_guard: None,
            header: *header,
        }
    }

    fn page_size(&self) -> u32 {
        self.get_shared().wal_header.lock().page_size
    }

    fn frame_offset(&self, frame_id: u64) -> usize {
        assert!(frame_id > 0, "Frame ID must be 1-based");
        let page_offset = (frame_id - 1) * (self.page_size() + WAL_FRAME_HEADER_SIZE as u32) as u64;
        let offset = WAL_HEADER_SIZE as u64 + page_offset;
        offset as usize
    }

    #[allow(clippy::mut_from_ref)]
    fn get_shared(&self) -> &mut WalFileShared {
        unsafe { self.shared.get().as_mut().unwrap() }
    }

    fn complete_append_frame(&mut self, page_id: u64, frame_id: u64, checksums: (u32, u32)) {
        self.last_checksum = checksums;
        self.max_frame = frame_id;
        let shared = self.get_shared();
        {
            let mut frame_cache = shared.frame_cache.lock();
            match frame_cache.get_mut(&page_id) {
                Some(frames) => {
                    if frames.is_empty() {
                        let mut pages_in_frames = shared.pages_in_frames.lock();
                        pages_in_frames.push(page_id);
                    }
                    frames.push(frame_id);
                }
                None => {
                    frame_cache.insert(page_id, vec![frame_id]);
                    shared.pages_in_frames.lock().push(page_id);
                }
            }
        }
    }

    fn reset_internal_states(&mut self) {
        self.ongoing_checkpoint.state = CheckpointState::Start;
        self.ongoing_checkpoint.min_frame = 0;
        self.ongoing_checkpoint.max_frame = 0;
        self.ongoing_checkpoint.current_page = 0;
        self.max_frame_read_lock_index.set(NO_LOCK_HELD);
        self.ongoing_checkpoint.batch.clear();
        let _ = self.ongoing_checkpoint.pending_flush.take();
        self.sync_state.set(SyncState::NotSyncing);
        self.syncing.set(false);
    }

    /// the WAL file has been truncated and we are writing the first
    /// frame since then. We need to ensure that the header is initialized.
    fn ensure_header_if_needed(&mut self) -> Result<()> {
        tracing::debug!("ensure_header_if_needed");
        self.last_checksum = {
            let shared = self.get_shared();
            if shared.max_frame.load(Ordering::Acquire) != 0 {
                return Ok(());
            }
            if shared.file.size()? >= WAL_HEADER_SIZE as u64 {
                return Ok(());
            }

            let mut hdr = shared.wal_header.lock();
            if hdr.page_size == 0 {
                hdr.page_size = self.page_size();
            }

            // recompute header checksum
            let prefix = &hdr.as_bytes()[..WAL_HEADER_SIZE - 8];
            let use_native = (hdr.magic & 1) != 0;
            let (c1, c2) = checksum_wal(prefix, &hdr, (0, 0), use_native);
            hdr.checksum_1 = c1;
            hdr.checksum_2 = c2;

            shared.last_checksum = (c1, c2);
            (c1, c2)
        };

        self.max_frame = 0;
        let shared = self.get_shared();
        self.io
            .wait_for_completion(sqlite3_ondisk::begin_write_wal_header(
                &shared.file,
                &shared.wal_header.lock(),
            )?)?;
        self.io
            .wait_for_completion(shared.file.sync(Completion::new_sync(|_| {}))?)?;
        Ok(())
    }

    fn checkpoint_inner(
        &mut self,
        pager: &Pager,
        _write_counter: Rc<RefCell<usize>>,
        mode: CheckpointMode,
    ) -> Result<IOResult<CheckpointResult>> {
        'checkpoint_loop: loop {
            let state = self.ongoing_checkpoint.state;
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
                    if !needs_backfill
                        && matches!(mode, CheckpointMode::Passive | CheckpointMode::Full)
                    {
                        // there are no frames to copy over and we don't need to reset
                        // the log so we can return early success.
                        return Ok(IOResult::Done(self.prev_checkpoint.clone()));
                    }
                    // acquire the appropriate exclusive locks depending on the checkpoint mode
                    self.acquire_proper_checkpoint_guard(mode)?;
                    self.ongoing_checkpoint.max_frame =
                        match self.determine_max_safe_checkpoint_frame(mode) {
                            Err(_) => return Err(LimboError::Busy),
                            Ok(res) => res,
                        };
                    self.ongoing_checkpoint.min_frame = nbackfills + 1;
                    self.ongoing_checkpoint.current_page = 0;
                    self.ongoing_checkpoint.state = CheckpointState::ReadFrame;
                    tracing::trace!(
                        "checkpoint_start(min_frame={}, max_frame={})",
                        self.ongoing_checkpoint.min_frame,
                        self.ongoing_checkpoint.max_frame,
                    );
                }
                // Find the next page that has a frame in the safe interval and schedule a read of that frame.
                CheckpointState::ReadFrame => {
                    let shared = self.get_shared();
                    let min_frame = self.ongoing_checkpoint.min_frame;
                    let max_frame = self.ongoing_checkpoint.max_frame;
                    let pages_in_frames = shared.pages_in_frames.clone();
                    let pages_in_frames = pages_in_frames.lock();

                    let frame_cache = shared.frame_cache.clone();
                    let frame_cache = frame_cache.lock();
                    assert!(self.ongoing_checkpoint.current_page as usize <= pages_in_frames.len());
                    if self.ongoing_checkpoint.current_page as usize == pages_in_frames.len() {
                        if self.ongoing_checkpoint.batch.is_empty() {
                            // no more pages to checkpoint, we are done
                            tracing::info!("checkpoint done, no more pages to checkpoint");
                            self.ongoing_checkpoint.state = CheckpointState::Done;
                        } else {
                            // flush the batch
                            self.ongoing_checkpoint.state = CheckpointState::FlushBatch;
                        }
                        continue 'checkpoint_loop;
                    }
                    let page = pages_in_frames[self.ongoing_checkpoint.current_page as usize];
                    let frames = frame_cache
                        .get(&page)
                        .expect("page must be in frame cache if it's in list");

                    for frame in frames.iter().rev() {
                        if *frame >= min_frame && *frame <= max_frame {
                            tracing::debug!(
                                "checkpoint page(state={:?}, page={}, frame={})",
                                state,
                                page,
                                *frame
                            );
                            self.ongoing_checkpoint.scratch_page.get().id = page as usize;
                            let _ = self.read_frame(
                                *frame,
                                self.ongoing_checkpoint.scratch_page.clone(),
                                self.buffer_pool.clone(),
                            )?;
                            self.ongoing_checkpoint.state = CheckpointState::WaitReadFrame;
                            continue 'checkpoint_loop;
                        }
                    }
                    self.ongoing_checkpoint.current_page += 1;
                }
                CheckpointState::WaitReadFrame => {
                    if self.ongoing_checkpoint.scratch_page.is_locked() {
                        return Ok(IOResult::IO);
                    } else {
                        self.ongoing_checkpoint.state = CheckpointState::AccumulatePage;
                    }
                }
                CheckpointState::AccumulatePage => {
                    // mark before batching
                    self.ongoing_checkpoint.scratch_page.set_dirty();
                    // we read the frame into memory, add it to our batch
                    self.ongoing_checkpoint
                        .batch
                        .add_to_batch(&self.ongoing_checkpoint.scratch_page, &self.buffer_pool);

                    let more_pages = (self.ongoing_checkpoint.current_page as usize)
                        < self
                            .get_shared()
                            .pages_in_frames
                            .lock()
                            .len()
                            .saturating_sub(1)
                        && !self.ongoing_checkpoint.batch.is_full();

                    // if we can read more pages, continue reading and accumulating pages
                    if more_pages {
                        self.ongoing_checkpoint.current_page += 1;
                        self.ongoing_checkpoint.state = CheckpointState::ReadFrame;
                    } else {
                        // if we have enough pages in the batch, flush it
                        self.ongoing_checkpoint.state = CheckpointState::FlushBatch;
                    }
                }
                CheckpointState::FlushBatch => {
                    tracing::trace!("started checkpoint backfilling batch");
                    self.ongoing_checkpoint.pending_flush = Some(write_pages_vectored(
                        pager,
                        std::mem::take(&mut self.ongoing_checkpoint.batch),
                    )?);
                    // batch is queued
                    self.ongoing_checkpoint.batch.clear();
                    self.ongoing_checkpoint.state = CheckpointState::WaitFlush;
                }
                CheckpointState::WaitFlush => {
                    match self.ongoing_checkpoint.pending_flush.as_ref() {
                        Some(pf) if pf.done.load(Ordering::Acquire) => {
                            // flush is done, we can continue
                            tracing::trace!("checkpoint backfilling batch done");
                        }
                        Some(_) => return Ok(IOResult::IO),
                        None => panic!("we should have a pending flush here"),
                    }
                    tracing::debug!("finished checkpoint backfilling batch");
                    let pf = self
                        .ongoing_checkpoint
                        .pending_flush
                        .as_ref()
                        .expect("we should have a pending flush here");
                    pf.clear_dirty(pager);
                    // done with batch
                    let shared = self.get_shared();
                    if (self.ongoing_checkpoint.current_page as usize)
                        < shared.pages_in_frames.lock().len()
                    {
                        self.ongoing_checkpoint.current_page += 1;
                        self.ongoing_checkpoint.state = CheckpointState::ReadFrame;
                    } else {
                        tracing::debug!("WaitFlush transitioning checkpoint to Done");
                        self.ongoing_checkpoint.state = CheckpointState::Done;
                    }
                }
                // All eligible frames copied to the db file
                // Update nBackfills
                // In Restart or Truncate mode, we need to restart the log over and possibly truncate the file
                // Release all locks and return the current num of wal frames and the amount we backfilled
                CheckpointState::Done => {
                    if let Some(pf) = self.ongoing_checkpoint.pending_flush.as_ref() {
                        turso_assert!(
                            pf.done.load(Ordering::Relaxed),
                            "checkpoint pending flush must have finished"
                        );
                    }
                    let mut checkpoint_result = {
                        let shared = self.get_shared();
                        let current_mx = shared.max_frame.load(Ordering::Acquire);
                        let nbackfills = shared.nbackfills.load(Ordering::Acquire);
                        // Record two num pages fields to return as checkpoint result to caller.
                        // Ref: pnLog, pnCkpt on https://www.sqlite.org/c3ref/wal_checkpoint_v2.html

                        // the total # of frames we could have possibly backfilled
                        let frames_possible = current_mx.saturating_sub(nbackfills);

                        // the total # of frames we actually backfilled
                        let frames_checkpointed = self
                            .ongoing_checkpoint
                            .max_frame
                            .saturating_sub(self.ongoing_checkpoint.min_frame - 1);

                        if matches!(mode, CheckpointMode::Truncate) {
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
                            CheckpointResult::new(frames_possible, frames_checkpointed)
                        }
                    };

                    // store the max frame we were able to successfully checkpoint.
                    self.get_shared()
                        .nbackfills
                        .store(self.ongoing_checkpoint.max_frame, Ordering::Release);

                    if matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate) {
                        if checkpoint_result.everything_backfilled() {
                            self.restart_log(mode)?;
                        } else {
                            return Err(LimboError::Busy);
                        }
                    }

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
                        && checkpoint_result.num_checkpointed_frames > 0
                    {
                        checkpoint_result.maybe_guard = self.checkpoint_guard.take();
                    } else {
                        let _ = self.checkpoint_guard.take();
                    }
                    self.ongoing_checkpoint.scratch_page.clear_dirty();
                    self.ongoing_checkpoint.scratch_page.get().id = 0;
                    self.ongoing_checkpoint.scratch_page.get().contents = None;
                    let _ = self.ongoing_checkpoint.pending_flush.take();
                    self.ongoing_checkpoint.batch.clear();
                    self.ongoing_checkpoint.state = CheckpointState::Start;
                    return Ok(IOResult::Done(checkpoint_result));
                }
            }
        }
    }

    /// Coordinate what the maximum safe frame is for us to backfill when checkpointing.
    /// We can never backfill a frame with a higher number than any reader's max frame,
    /// because we might overwrite content the reader is reading from the database file.
    ///
    /// A checkpoint must never overwrite a page in the main DB file if some
    /// active reader might still need to read that page from the WAL.  
    /// Concretely: the checkpoint may only copy frames `<= aReadMark[k]` for
    /// every in‑use reader slot `k > 0`.
    ///
    /// `read_locks[0]` is special: readers holding slot 0 ignore the WAL entirely
    /// (they read only the DB file). Its value is a placeholder and does not
    /// constrain `mxSafeFrame`.
    ///
    /// Slot 1 is the “default” reader slot. If it is free (we can take its
    /// write-lock) we raise it to the global max so new readers see the most
    /// recent snapshot. We do not clear it to `READMARK_NOT_USED` in ordinary
    /// checkpoints (SQLite only clears nonzero slots during a log reset).
    ///
    /// Slots 2..N: If a reader is stuck at an older frame, that frame becomes the
    /// limit. If we can’t atomically bump that slot (write-lock fails), we must
    /// clamp `mxSafeFrame` down to that mark. In PASSIVE mode we stop trying
    /// immediately (we are not allowed to block or spin). In the blocking modes
    /// (FULL/RESTART/TRUNCATE) we can loop and retry, but for now we can
    /// just respect the first busy slot and move on.
    ///
    /// Locking rules:
    /// This routine tries to take an exclusive (write) lock on each slot to
    /// update/clean it. If the try-lock fails:
    /// PASSIVE: do not wait; just lower `mxSafeFrame` and break.
    /// Others: lower `mxSafeFrame` and continue scanning.
    ///
    /// We never modify slot values while a reader holds that slot.
    fn determine_max_safe_checkpoint_frame(&self, mode: CheckpointMode) -> Result<u64> {
        let shared = self.get_shared();
        let shared_max = shared.max_frame.load(Ordering::Acquire);

        // Start optimistic: we want to advance everyone to shared_max
        for (idx, lock) in shared.read_locks.iter_mut().enumerate().skip(1) {
            let mark = lock.get_value();
            if mark == READMARK_NOT_USED || mark >= shared_max as u32 {
                continue;
            }
            // Try to bump this slot to shared_max (requires exclusive on the slot)
            if lock.write() {
                // Slot is free to edit, bump to shared_max (slot 1 keeps a real mark, others can be cleaned)
                let val = if idx == 1 {
                    shared_max as u32
                } else {
                    READMARK_NOT_USED
                };
                lock.set_value_exclusive(val);
                lock.unlock();
            } else {
                // Reader is using this slot.
                return match mode {
                    // clamp down for passive
                    CheckpointMode::Passive => Ok(mark as u64),
                    _ => Err(LimboError::Busy), // all others must wait
                };
            }
        }
        Ok(shared_max)
    }

    /// Called once the entire WAL has been back‑filled in RESTART or TRUNCATE mode.
    /// Must be invoked while writer and checkpoint locks are still held.
    fn restart_log(&mut self, mode: CheckpointMode) -> Result<()> {
        turso_assert!(
            matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate),
            "CheckpointMode must be Restart or Truncate"
        );
        turso_assert!(
            matches!(self.checkpoint_guard, Some(CheckpointLocks::Writer { .. })),
            "We must hold writer and checkpoint locks to restart the log, found: {:?}",
            self.checkpoint_guard
        );
        tracing::info!("restart_log(mode={mode:?})");
        {
            // Block all readers
            let shared = self.get_shared();
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

        let handle_err = |e: &LimboError| {
            // release all read locks we just acquired, the caller will take care of the others
            let shared = self.get_shared();
            for idx in 1..shared.read_locks.len() {
                shared.read_locks[idx].unlock();
            }
            tracing::error!(
                "Failed to restart WAL header: {:?}, releasing read locks",
                e
            );
        };
        // reinitialize in‑memory state
        self.get_shared()
            .restart_wal_header(&self.io, mode)
            .inspect_err(|e| {
                handle_err(e);
            })?;

        // For TRUNCATE mode: shrink the WAL file to 0 B
        if matches!(mode, CheckpointMode::Truncate) {
            let c = Completion::new_trunc(|_| {
                tracing::trace!("WAL file truncated to 0 B");
            });
            let shared = self.get_shared();
            // for now at least, lets do all this IO syncronously
            let c = shared.file.truncate(0, c).inspect_err(handle_err)?;
            self.io.wait_for_completion(c).inspect_err(handle_err)?;
            // fsync after truncation
            self.io
                .wait_for_completion(
                    shared
                        .file
                        .sync(Completion::new_sync(|_| {
                            tracing::trace!("WAL file synced after reset/truncation");
                        }))
                        .inspect_err(handle_err)?,
                )
                .inspect_err(handle_err)?;
        }

        // release read‑locks 1..4
        {
            let shared = self.get_shared();
            for idx in 1..shared.read_locks.len() {
                shared.read_locks[idx].unlock();
            }
        }

        self.last_checksum = self.get_shared().last_checksum;
        self.max_frame = 0;
        self.min_frame = 0;
        Ok(())
    }

    fn acquire_proper_checkpoint_guard(&mut self, mode: CheckpointMode) -> Result<()> {
        let needs_new_guard = !matches!(
            (&self.checkpoint_guard, mode),
            (Some(CheckpointLocks::Read0 { .. }), CheckpointMode::Passive,)
                | (
                    Some(CheckpointLocks::Writer { .. }),
                    CheckpointMode::Restart | CheckpointMode::Truncate,
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
}

impl WalFileShared {
    pub fn open_shared_if_exists(
        io: &Arc<dyn IO>,
        path: &str,
    ) -> Result<Option<Arc<UnsafeCell<WalFileShared>>>> {
        let file = io.open_file(path, crate::io::OpenFlags::Create, false)?;
        if file.size()? > 0 {
            let wal_file_shared = sqlite3_ondisk::read_entire_wal_dumb(&file)?;
            // TODO: Return a completion instead.
            let mut max_loops = 100_000;
            while !unsafe { &*wal_file_shared.get() }
                .loaded
                .load(Ordering::Acquire)
            {
                io.run_once()?;
                max_loops -= 1;
                if max_loops == 0 {
                    panic!("WAL file not loaded");
                }
            }
            Ok(Some(wal_file_shared))
        } else {
            Ok(None)
        }
    }

    pub fn new_shared(
        page_size: u32,
        io: &Arc<dyn IO>,
        file: Arc<dyn File>,
    ) -> Result<Arc<UnsafeCell<WalFileShared>>> {
        let magic = if cfg!(target_endian = "big") {
            WAL_MAGIC_BE
        } else {
            WAL_MAGIC_LE
        };
        let mut wal_header = WalHeader {
            magic,
            file_format: 3007000,
            page_size,
            checkpoint_seq: 0, // TODO implement sequence number
            salt_1: io.generate_random_number() as u32,
            salt_2: io.generate_random_number() as u32,
            checksum_1: 0,
            checksum_2: 0,
        };
        let native = cfg!(target_endian = "big"); // if target_endian is
                                                  // already big then we don't care but if isn't, header hasn't yet been
                                                  // encoded to big endian, therefore we want to swap bytes to compute this
                                                  // checksum.
        let checksums = (0, 0);
        let checksums = checksum_wal(
            &wal_header.as_bytes()[..WAL_HEADER_SIZE - 2 * 4], // first 24 bytes
            &wal_header,
            checksums,
            native, // this is false because we haven't encoded the wal header yet
        );
        wal_header.checksum_1 = checksums.0;
        wal_header.checksum_2 = checksums.1;
        let c = sqlite3_ondisk::begin_write_wal_header(&file, &wal_header)?;
        let header = Arc::new(SpinLock::new(wal_header));
        let checksum = {
            let checksum = header.lock();
            (checksum.checksum_1, checksum.checksum_2)
        };
        io.wait_for_completion(c)?;
        tracing::debug!("new_shared(header={:?})", header);
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
            wal_header: Arc::new(SpinLock::new(wal_header)),
            min_frame: AtomicU64::new(0),
            max_frame: AtomicU64::new(0),
            nbackfills: AtomicU64::new(0),
            frame_cache: Arc::new(SpinLock::new(HashMap::new())),
            last_checksum: checksum,
            file,
            pages_in_frames: Arc::new(SpinLock::new(Vec::new())),
            read_locks,
            write_lock: TursoRwLock::new(),
            checkpoint_lock: TursoRwLock::new(),
            loaded: AtomicBool::new(true),
        };
        Ok(Arc::new(UnsafeCell::new(shared)))
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
    fn restart_wal_header(&mut self, io: &Arc<dyn IO>, mode: CheckpointMode) -> Result<()> {
        turso_assert!(
            matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate),
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
        self.pages_in_frames.lock().clear();

        // read-marks
        self.read_locks[0].set_value_exclusive(0);
        self.read_locks[1].set_value_exclusive(0);
        for lock in &self.read_locks[2..] {
            lock.set_value_exclusive(READMARK_NOT_USED);
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use crate::{
        result::LimboResult,
        storage::{
            sqlite3_ondisk::{self, WAL_HEADER_SIZE},
            wal::READMARK_NOT_USED,
        },
        types::IOResult,
        CheckpointMode, CheckpointResult, Completion, Connection, Database, LimboError, PlatformIO,
        StepResult, Wal, WalFile, WalFileShared, IO,
    };
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::{
        cell::{Cell, RefCell, UnsafeCell},
        rc::Rc,
        sync::{atomic::Ordering, Arc},
    };
    #[allow(clippy::arc_with_non_send_sync)]
    fn get_database() -> (Arc<Database>, std::path::PathBuf) {
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
        let wal = db.maybe_shared_wal.write();
        let wal_file = wal.as_ref().unwrap();
        let file = unsafe { &mut *wal_file.get() };
        let done = Rc::new(Cell::new(false));
        let _done = done.clone();
        let _ = file.file.truncate(
            WAL_HEADER_SIZE,
            Completion::new_trunc(move |_| {
                let done = _done.clone();
                done.set(true);
            }),
        );
        assert!(file.file.size().unwrap() == WAL_HEADER_SIZE as u64);
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
        let pager = conn.pager.borrow_mut();
        let _ = pager.cacheflush();
        let mut wal = pager.wal.as_ref().unwrap().borrow_mut();

        let stat = std::fs::metadata(&walpath).unwrap();
        let meta_before = std::fs::metadata(&walpath).unwrap();
        let bytes_before = meta_before.len();
        run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Truncate);
        drop(wal);

        assert_eq!(pager.wal_frame_count().unwrap(), 0);

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
        let wc = Rc::new(RefCell::new(0usize));
        loop {
            match wal.checkpoint(pager, wc.clone(), mode).unwrap() {
                IOResult::Done(r) => return r,
                IOResult::IO => {
                    pager.io.run_once().unwrap();
                }
            }
        }
    }

    fn wal_header_snapshot(shared: &Arc<UnsafeCell<WalFileShared>>) -> (u32, u32, u32, u32) {
        // (checkpoint_seq, salt1, salt2, page_size)
        unsafe {
            let hdr = (*shared.get()).wal_header.lock();
            (hdr.checkpoint_seq, hdr.salt_1, hdr.salt_2, hdr.page_size)
        }
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
        while let IOResult::IO = conn.pager.borrow_mut().cacheflush().unwrap() {
            conn.run_once().unwrap();
        }

        // Snapshot header & counters before the RESTART checkpoint.
        let wal_shared = db.maybe_shared_wal.read().as_ref().unwrap().clone();
        let (seq_before, salt1_before, salt2_before, _ps_before) = wal_header_snapshot(&wal_shared);
        let (mx_before, backfill_before) = unsafe {
            let s = &*wal_shared.get();
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
            let pager = conn.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let res = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Restart);
            assert_eq!(res.num_wal_frames, mx_before);
            assert_eq!(res.num_checkpointed_frames, mx_before);
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

        let (mx_after, backfill_after) = unsafe {
            let s = &*wal_shared.get();
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
            .borrow_mut()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .finish_append_frames_commit()
            .unwrap();
        let new_max = unsafe { (&*wal_shared.get()).max_frame.load(Ordering::SeqCst) };
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
        while let IOResult::IO = conn1.pager.borrow_mut().cacheflush().unwrap() {
            conn1.run_once().unwrap();
        }

        // Force a read transaction that will freeze a lower read mark
        let readmark = {
            let pager = conn2.pager.borrow_mut();
            let mut wal2 = pager.wal.as_ref().unwrap().borrow_mut();
            assert!(matches!(wal2.begin_read_tx().unwrap().0, LimboResult::Ok));
            wal2.get_max_frame()
        };

        // generate more frames that the reader will not see.
        bulk_inserts(&conn1.clone(), 15, 2);
        while let IOResult::IO = conn1.pager.borrow_mut().cacheflush().unwrap() {
            conn1.run_once().unwrap();
        }

        // Run passive checkpoint, expect partial
        let (res1, max_before) = {
            let pager = conn1.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let res = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive);
            let maxf = unsafe {
                (&*db.maybe_shared_wal.read().as_ref().unwrap().get())
                    .max_frame
                    .load(Ordering::SeqCst)
            };
            (res, maxf)
        };
        assert_eq!(res1.num_wal_frames, max_before);
        assert!(
            res1.num_checkpointed_frames < res1.num_wal_frames,
            "Partial backfill expected, {} : {}",
            res1.num_checkpointed_frames,
            res1.num_wal_frames
        );
        assert_eq!(
            res1.num_checkpointed_frames, readmark,
            "Checkpointed frames should match read mark"
        );
        // Release reader
        {
            let pager = conn2.pager.borrow_mut();
            let wal2 = pager.wal.as_ref().unwrap().borrow_mut();
            wal2.end_read_tx();
        }

        // Second passive checkpoint should finish
        let pager = conn1.pager.borrow();
        let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
        let res2 = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive);
        assert_eq!(
            res2.num_checkpointed_frames, res2.num_wal_frames,
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
            .borrow_mut()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .begin_read_tx()
            .unwrap();

        // checkpoint should succeed here because the wal is fully checkpointed (empty)
        // so the reader is using readmark0 to read directly from the db file.
        let p = conn1.pager.borrow();
        let mut w = p.wal.as_ref().unwrap().borrow_mut();
        loop {
            match w.checkpoint(&p, Rc::new(RefCell::new(0)), CheckpointMode::Restart) {
                Ok(IOResult::IO) => {
                    conn1.run_once().unwrap();
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
        conn2.pager.borrow_mut().end_read_tx().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        for i in 0..10 {
            conn1
                .execute(format!("insert into test(value) values ('value{i}')"))
                .unwrap();
        }
        // now that we have some frames to checkpoint, try again
        conn2.pager.borrow_mut().begin_read_tx().unwrap();
        let p = conn1.pager.borrow();
        let mut w = p.wal.as_ref().unwrap().borrow_mut();
        loop {
            match w.checkpoint(&p, Rc::new(RefCell::new(0)), CheckpointMode::Restart) {
                Ok(IOResult::IO) => {
                    conn1.run_once().unwrap();
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
        let wal_shared = db.maybe_shared_wal.read().as_ref().unwrap().clone();

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 5);
        // Checkpoint with restart
        {
            let pager = conn.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let result = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Restart);
            assert!(result.everything_backfilled());
        }

        // Verify read marks after restart
        let read_marks_after: Vec<_> = unsafe {
            let s = &*wal_shared.get();
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
            let pager = conn_r1.pager.borrow_mut();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            assert!(matches!(wal.begin_read_tx().unwrap().0, LimboResult::Ok));
            wal.get_max_frame()
        };
        bulk_inserts(&conn_writer, 5, 10);

        // R2 starts reading, sees more frames than R1
        let r2_max_frame = {
            let pager = conn_r2.pager.borrow_mut();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            assert!(matches!(wal.begin_read_tx().unwrap().0, LimboResult::Ok));
            wal.get_max_frame()
        };

        // try passive checkpoint, should only checkpoint up to R1's position
        let checkpoint_result = {
            let pager = conn_writer.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive)
        };

        assert!(
            checkpoint_result.num_checkpointed_frames < checkpoint_result.num_wal_frames,
            "Should not checkpoint all frames when readers are active"
        );
        assert_eq!(
            checkpoint_result.num_checkpointed_frames, r1_max_frame,
            "Should have checkpointed up to R1's max frame"
        );

        // Verify R2 still sees its frames
        assert_eq!(
            conn_r2
                .pager
                .borrow()
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
        let wal_shared = db.maybe_shared_wal.read().as_ref().unwrap().clone();

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 5);

        // get max frame before checkpoint
        let max_frame_before = unsafe { (*wal_shared.get()).max_frame.load(Ordering::SeqCst) };

        {
            let pager = conn.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let _result = run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive);
        }

        // check that read mark 1 (default reader) was updated to max_frame
        let read_mark_1 = unsafe { (*wal_shared.get()).read_locks[1].get_value() };

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
            let pager = conn2.pager.borrow_mut();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let _ = wal.begin_read_tx().unwrap();
            let res = wal.begin_write_tx().unwrap();
            assert!(matches!(res, LimboResult::Ok), "result: {res:?}");
        }

        // should fail because writer lock is held
        let result = {
            let pager = conn1.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.checkpoint(&pager, Rc::new(RefCell::new(0)), CheckpointMode::Restart)
        };

        assert!(
            matches!(result, Err(LimboError::Busy)),
            "Restart checkpoint should fail when write lock is held"
        );

        conn2
            .pager
            .borrow()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .end_read_tx();
        // release write lock
        conn2
            .pager
            .borrow()
            .wal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .end_write_tx();

        // now restart should succeed
        let result = {
            let pager = conn1.pager.borrow();
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
        let pager = conn.pager.borrow();
        let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
        let _ = wal.begin_write_tx();
    }

    fn check_read_lock_slot(conn: &Arc<Connection>, expected_slot: usize) -> bool {
        let pager = conn.pager.borrow();
        let wal = pager.wal.as_ref().unwrap().borrow();
        #[cfg(debug_assertions)]
        {
            let wal_any = wal.as_any();
            if let Some(wal_file) = wal_any.downcast_ref::<WalFile>() {
                return wal_file.max_frame_read_lock_index.get() == expected_slot;
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
                .borrow()
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
            let pager = conn_writer.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive)
        };
        assert_eq!(result1.num_checkpointed_frames, r1_frame);

        // finish reader‑1
        conn1.execute("COMMIT").unwrap();

        // passive checkpoint #2
        let result2 = {
            let pager = conn_writer.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive)
        };
        assert_eq!(
            result1.num_checkpointed_frames + result2.num_checkpointed_frames,
            r2_frame
        );

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
            let pager = conn.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Truncate);
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
            let pager = conn.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Truncate);
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
            let pager = conn.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive);
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
            let pager = conn2.pager.borrow_mut();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let (res, _) = wal.begin_read_tx().unwrap();
            assert!(matches!(res, LimboResult::Ok));
        }
        // Make changes using conn1
        bulk_inserts(&conn1, 5, 5);
        // Try to start a write transaction on conn2 with a stale snapshot
        let result = {
            let pager = conn2.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_write_tx()
        };
        // Should get Busy due to stale snapshot
        assert!(matches!(result.unwrap(), LimboResult::Busy));

        // End read transaction and start a fresh one
        {
            let pager = conn2.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.end_read_tx();
            let (res, _) = wal.begin_read_tx().unwrap();
            assert!(matches!(res, LimboResult::Ok));
        }
        // Now write transaction should work
        let result = {
            let pager = conn2.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            wal.begin_write_tx()
        };
        assert!(matches!(result.unwrap(), LimboResult::Ok));
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
            let pager = conn1.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Passive);
        }

        // Start a read transaction on conn2
        {
            let pager = conn2.pager.borrow_mut();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let (res, _) = wal.begin_read_tx().unwrap();
            assert!(matches!(res, LimboResult::Ok));
        }
        // should use slot 0, as everything is backfilled
        assert!(check_read_lock_slot(&conn2, 0));
        {
            let pager = conn1.pager.borrow();
            let wal = pager.wal.as_ref().unwrap().borrow();
            let frame = wal.find_frame(5, None);
            // since we hold readlock0, we should ignore the db file and find_frame should return none
            assert!(frame.is_ok_and(|f| f.is_none()));
        }
        // Try checkpoint, should fail because reader has slot 0
        {
            let pager = conn1.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let result = wal.checkpoint(&pager, Rc::new(RefCell::new(0)), CheckpointMode::Restart);

            assert!(
                matches!(result, Err(LimboError::Busy)),
                "RESTART checkpoint should fail when a reader is using slot 0"
            );
        }
        // End the read transaction
        {
            let pager = conn2.pager.borrow();
            let wal = pager.wal.as_ref().unwrap().borrow();
            wal.end_read_tx();
        }
        {
            let pager = conn1.pager.borrow();
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
        while let IOResult::IO = conn.pager.borrow_mut().cacheflush().unwrap() {
            conn.run_once().unwrap();
        }

        // Snapshot the current mxFrame before running FULL
        let wal_shared = db.maybe_shared_wal.read().as_ref().unwrap().clone();
        let mx_before = unsafe { (&*wal_shared.get()).max_frame.load(Ordering::SeqCst) };
        assert!(mx_before > 0, "expected frames in WAL before FULL");

        // Run FULL checkpoint - must backfill *all* frames up to mx_before
        let result = {
            let pager = conn.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Full)
        };

        assert_eq!(result.num_wal_frames, mx_before);
        assert_eq!(result.num_checkpointed_frames, mx_before);
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
        while let IOResult::IO = writer.pager.borrow_mut().cacheflush().unwrap() {
            writer.run_once().unwrap();
        }

        // Start a read transaction pinned at the current snapshot
        {
            let pager = reader.pager.borrow_mut();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            let (res, _) = wal.begin_read_tx().unwrap();
            assert!(matches!(res, LimboResult::Ok));
        }
        let r_snapshot = {
            let pager = reader.pager.borrow();
            let wal = pager.wal.as_ref().unwrap().borrow();
            wal.get_max_frame()
        };

        // Advance WAL beyond the reader's snapshot
        bulk_inserts(&writer, 3, 4);
        while let IOResult::IO = writer.pager.borrow_mut().cacheflush().unwrap() {
            writer.run_once().unwrap();
        }
        let mx_now = unsafe {
            (&*db.maybe_shared_wal.read().as_ref().unwrap().get())
                .max_frame
                .load(Ordering::SeqCst)
        };
        assert!(mx_now > r_snapshot);

        // FULL must return Busy while a reader is stuck behind
        {
            let pager = writer.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            match wal.checkpoint(&pager, Rc::new(RefCell::new(0)), CheckpointMode::Full) {
                Ok(IOResult::IO) => {
                    // Drive any pending IO (should quickly become Busy or Done)
                    writer.run_once().unwrap();
                    // Call again to see final state
                    match wal.checkpoint(&pager, Rc::new(RefCell::new(0)), CheckpointMode::Full) {
                        Err(LimboError::Busy) => {}
                        other => panic!("expected Busy from FULL with old reader, got {other:?}"),
                    }
                }
                Err(LimboError::Busy) => {}
                other => panic!("expected Busy from FULL with old reader, got {other:?}"),
            }
        }

        // Release the reader, now full mode should succeed and backfill everything
        {
            let pager = reader.pager.borrow();
            let wal = pager.wal.as_ref().unwrap().borrow();
            wal.end_read_tx();
        }

        let result = {
            let pager = writer.pager.borrow();
            let mut wal = pager.wal.as_ref().unwrap().borrow_mut();
            run_checkpoint_until_done(&mut *wal, &pager, CheckpointMode::Full)
        };

        assert_eq!(result.num_wal_frames, mx_now);
        assert_eq!(result.num_checkpointed_frames, mx_now);
    }
}
