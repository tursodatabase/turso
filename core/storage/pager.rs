use crate::storage::btree::PinGuard;
use crate::storage::subjournal::Subjournal;
use crate::storage::{
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    sqlite3_ondisk::{
        self, parse_wal_frame_header, DatabaseHeader, PageContent, PageSize, PageType,
    },
    wal::{CheckpointResult, Wal, IOV_MAX},
};
use crate::types::{IOCompletions, WalState};
use crate::util::IOExt as _;
use crate::{
    io::CompletionGroup, return_if_io, turso_assert, types::WalFrameInfo, Completion, Connection,
    IOResult, LimboError, Result, TransactionState,
};
use crate::{io_yield_one, Buffer, CompletionError, IOContext, OpenFlags, IO};
use arc_swap::ArcSwapOption;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use std::cell::UnsafeCell;
use std::collections::BTreeSet;
use std::sync::atomic::{
    AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::Arc;
use tracing::{instrument, trace, Level};

use super::btree::btree_init_page;
use super::page_cache::{CacheError, CacheResizeResult, PageCache, PageCacheKey, SpillResult};
use super::sqlite3_ondisk::begin_write_btree_page;
use super::wal::CheckpointMode;
use crate::storage::encryption::{CipherMode, EncryptionContext, EncryptionKey};

/// SQLite's default maximum page count
const DEFAULT_MAX_PAGE_COUNT: u32 = 0xfffffffe;
const RESERVED_SPACE_NOT_SET: u16 = u16::MAX;

#[cfg(feature = "test_helper")]
/// Used for testing purposes to change the position of the PENDING BYTE
static PENDING_BYTE: AtomicU32 = AtomicU32::new(0x40000000);

#[cfg(not(feature = "test_helper"))]
/// Byte offset that signifies the start of the ignored page - 1 GB mark
const PENDING_BYTE: u32 = 0x40000000;

#[cfg(not(feature = "omit_autovacuum"))]
use ptrmap::*;

#[derive(Debug, Clone)]
pub struct HeaderRef(PageRef);

impl HeaderRef {
    pub fn from_pager(pager: &Pager) -> Result<IOResult<Self>> {
        loop {
            let state = pager.header_ref_state.read().clone();
            tracing::trace!("HeaderRef::from_pager - {:?}", state);
            match state {
                HeaderRefState::Start => {
                    // If db is not initialized, return the in-memory page
                    if let Some(page1) = pager.init_page_1.load_full() {
                        return Ok(IOResult::Done(Self(page1)));
                    }

                    let (page, c) = pager.read_page(DatabaseHeader::PAGE_ID as i64)?;
                    *pager.header_ref_state.write() = HeaderRefState::CreateHeader { page };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                HeaderRefState::CreateHeader { page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    turso_assert!(
                        page.get().id == DatabaseHeader::PAGE_ID,
                        "incorrect header page id"
                    );
                    *pager.header_ref_state.write() = HeaderRefState::Start;
                    break Ok(IOResult::Done(Self(page)));
                }
            }
        }
    }

    pub fn borrow(&self) -> &DatabaseHeader {
        // TODO: Instead of erasing mutability, implement `get_mut_contents` and return a shared reference.
        let content: &PageContent = self.0.get_contents();
        bytemuck::from_bytes::<DatabaseHeader>(&content.buffer.as_slice()[0..DatabaseHeader::SIZE])
    }
}

#[derive(Debug, Clone)]
pub struct HeaderRefMut(PageRef);

impl HeaderRefMut {
    pub fn from_pager(pager: &Pager) -> Result<IOResult<Self>> {
        loop {
            let state = pager.header_ref_state.read().clone();
            tracing::trace!(?state);
            match state {
                HeaderRefState::Start => {
                    // If db is not initialized, return the in-memory page
                    if let Some(page1) = pager.init_page_1.load_full() {
                        return Ok(IOResult::Done(Self(page1)));
                    }

                    let (page, c) = pager.read_page(DatabaseHeader::PAGE_ID as i64)?;
                    *pager.header_ref_state.write() = HeaderRefState::CreateHeader { page };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                HeaderRefState::CreateHeader { page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    turso_assert!(
                        page.get().id == DatabaseHeader::PAGE_ID,
                        "incorrect header page id"
                    );

                    pager.add_dirty(&page)?;
                    *pager.header_ref_state.write() = HeaderRefState::Start;
                    break Ok(IOResult::Done(Self(page)));
                }
            }
        }
    }

    pub fn borrow_mut(&self) -> &mut DatabaseHeader {
        let content = self.0.get_contents();
        bytemuck::from_bytes_mut::<DatabaseHeader>(
            &mut content.buffer.as_mut_slice()[0..DatabaseHeader::SIZE],
        )
    }

    /// Get a reference to the underlying page
    pub fn page(&self) -> &PageRef {
        &self.0
    }
}

pub struct PageInner {
    pub flags: AtomicUsize,
    pub contents: Option<PageContent>,
    pub id: usize,
    /// If >0, the page is pinned and not eligible for eviction from the page cache.
    /// The reason this is a counter is that multiple nested code paths may signal that
    /// a page must not be evicted from the page cache, so even if an inner code path
    /// requests unpinning via [Page::unpin], the pin count will still be >0 if the outer
    /// code path has not yet requested to unpin the page as well.
    ///
    /// Note that [PageCache::clear] evicts the pages even if pinned, so as long as
    /// we clear the page cache on errors, pins will not 'leak'.
    pub pin_count: AtomicUsize,
    /// The WAL frame number this page was loaded from (0 if loaded from main DB file)
    /// This tracks which version of the page we have in memory
    pub wal_tag: AtomicU64,
}

/// WAL tag not set
pub const TAG_UNSET: u64 = u64::MAX;
/// WAL write in progress, sentinel value set before starting a WAL write
/// so we can detect if page was modified during the write
pub const TAG_WRITE_PENDING: u64 = u64::MAX - 1;

/// Bit layout:
/// epoch: 20
/// frame: 44
const EPOCH_BITS: u32 = 20;
const FRAME_BITS: u32 = 64 - EPOCH_BITS;
const EPOCH_SHIFT: u32 = FRAME_BITS;
const EPOCH_MAX: u32 = (1u32 << EPOCH_BITS) - 1;
const FRAME_MAX: u64 = (1u64 << FRAME_BITS) - 1;

#[inline]
pub fn pack_tag_pair(frame: u64, seq: u32) -> u64 {
    ((seq as u64) << EPOCH_SHIFT) | (frame & FRAME_MAX)
}

#[inline]
pub fn unpack_tag_pair(tag: u64) -> (u64, u32) {
    let epoch = ((tag >> EPOCH_SHIFT) & (EPOCH_MAX as u64)) as u32;
    let frame = tag & FRAME_MAX;
    (frame, epoch)
}

#[derive(Debug)]
pub struct Page {
    pub inner: UnsafeCell<PageInner>,
}

// SAFETY: Page is thread-safe because we use atomic page flags to serialize
// concurrent modifications.
unsafe impl Send for Page {}
unsafe impl Sync for Page {}

// Concurrency control of pages will be handled by the pager, we won't wrap Page with RwLock
// because that is bad bad.
pub type PageRef = Arc<Page>;

/// Page is locked for I/O to prevent concurrent access.
const PAGE_LOCKED: usize = 0b010;
/// Page is dirty. Flush needed.
const PAGE_DIRTY: usize = 0b1000;
/// Page's contents are loaded in memory.
const PAGE_LOADED: usize = 0b10000;
/// Page has been spilled to WAL (can be evicted even though dirty).
const PAGE_SPILLED: usize = 0b100000;

impl Page {
    pub fn new(id: i64) -> Self {
        assert!(id >= 0, "page id should be positive");
        Self {
            inner: UnsafeCell::new(PageInner {
                flags: AtomicUsize::new(0),
                contents: None,
                id: id as usize,
                pin_count: AtomicUsize::new(0),
                wal_tag: AtomicU64::new(TAG_UNSET),
            }),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get(&self) -> &mut PageInner {
        unsafe { &mut *self.inner.get() }
    }

    pub fn get_contents(&self) -> &mut PageContent {
        self.get().contents.as_mut().unwrap()
    }

    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::Acquire);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        tracing::debug!("set_dirty(page={})", self.get().id);
        self.clear_wal_tag();
        // Clear spilled flag since page is being modified again
        self.get().flags.fetch_and(!PAGE_SPILLED, Ordering::Release);
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        tracing::debug!("clear_dirty(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::Release);
        self.clear_wal_tag();
    }

    /// Clear the dirty flag without touching wal_tag.
    /// Used when a WAL frame has been durably written and the tag already encodes it.
    pub fn clear_dirty_keep_wal_tag(&self) {
        tracing::debug!("clear_dirty_keep_wal_tag(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::Release);
    }

    /// Returns true if the page has been spilled to WAL and is safe to evict even while dirty.
    pub fn is_spilled(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_SPILLED != 0
    }

    /// Mark the page as spilled to WAL. Spilled pages remain dirty but may be evicted from cache.
    pub fn set_spilled(&self) {
        tracing::debug!("set_spilled(page={})", self.get().id);
        self.get().flags.fetch_or(PAGE_SPILLED, Ordering::Release);
    }

    /// Clear the spilled flag. This is also done implicitly on set_dirty().
    pub fn clear_spilled(&self) {
        self.get().flags.fetch_and(!PAGE_SPILLED, Ordering::Release);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::Release);
    }

    pub fn clear_loaded(&self) {
        tracing::debug!("clear loaded {}", self.get().id);
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::Release);
    }

    pub fn is_index(&self) -> bool {
        match self.get_contents().page_type() {
            PageType::IndexLeaf | PageType::IndexInterior => true,
            PageType::TableLeaf | PageType::TableInterior => false,
        }
    }

    /// Increment the pin count by 1. A pin count >0 means the page is pinned and not eligible for eviction from the page cache.
    pub fn pin(&self) {
        self.get().pin_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement the pin count by 1. If the count reaches 0, the page is no longer
    /// pinned and is eligible for eviction from the page cache.
    pub fn unpin(&self) {
        let was_pinned = self.try_unpin();

        turso_assert!(
            was_pinned,
            "Attempted to unpin page {} that was not pinned",
            self.get().id
        );
    }

    /// Try to decrement the pin count by 1, but do nothing if it was already 0.
    /// Returns true if the pin count was decremented.
    pub fn try_unpin(&self) -> bool {
        self.get()
            .pin_count
            .fetch_update(Ordering::Release, Ordering::SeqCst, |current| {
                if current == 0 {
                    None
                } else {
                    Some(current - 1)
                }
            })
            .is_ok()
    }

    /// Returns true if the page is pinned and thus not eligible for eviction from the page cache.
    pub fn is_pinned(&self) -> bool {
        self.get().pin_count.load(Ordering::Acquire) > 0
    }

    #[inline]
    /// Set the WAL tag from a (frame, epoch) pair.
    /// If inputs are invalid, stores TAG_UNSET, which will prevent
    /// the cached page from being used during checkpoint.
    pub fn set_wal_tag(&self, frame: u64, epoch: u32) {
        // use only first 20 bits for seq (max: 1048576)
        let e = epoch & EPOCH_MAX;
        self.get()
            .wal_tag
            .store(pack_tag_pair(frame, e), Ordering::Release);
    }

    #[inline]
    /// Load the (frame, seq) pair from the packed tag.
    pub fn wal_tag_pair(&self) -> (u64, u32) {
        unpack_tag_pair(self.get().wal_tag.load(Ordering::Acquire))
    }

    #[inline]
    pub fn clear_wal_tag(&self) {
        self.get().wal_tag.store(TAG_UNSET, Ordering::Release)
    }

    #[inline]
    /// Returns true if the page has a valid WAL tag (i.e., was written to WAL and not modified since).
    /// Returns false if the wal_tag is TAG_UNSET (page was modified since last WAL write).
    pub fn has_wal_tag(&self) -> bool {
        let tag = self.get().wal_tag.load(Ordering::Acquire);
        let result = tag != TAG_UNSET && tag != TAG_WRITE_PENDING;
        tracing::debug!(
            "has_wal_tag(page={}) = {} (tag={:x})",
            self.get().id,
            result,
            tag
        );
        result
    }

    #[inline]
    /// Mark page as having a WAL write in progress.
    /// This is set before starting a spill/cacheflush so we can detect
    /// if the page was modified during the write.
    pub fn set_write_pending(&self) {
        tracing::debug!("set_write_pending(page={})", self.get().id);
        self.get()
            .wal_tag
            .store(TAG_WRITE_PENDING, Ordering::Release);
    }

    #[inline]
    /// Try to set the WAL tag, but only if the page wasn't modified during the write.
    /// Returns true if the tag was set, false if the page was modified (wal_tag became TAG_UNSET).
    pub fn try_set_wal_tag(&self, frame: u64, epoch: u32) -> bool {
        let new_tag = pack_tag_pair(frame, epoch);
        let page_id = self.get().id;
        let current = self.get().wal_tag.load(Ordering::Acquire);
        // Only set if current tag is not TAG_UNSET (meaning page wasn't modified during write)
        // TAG_WRITE_PENDING is fine, it means the write was in progress and page wasn't modified
        if current == TAG_UNSET {
            tracing::debug!(
                "try_set_wal_tag(page={}, frame={}) SKIPPED: wal_tag is TAG_UNSET (page was modified)",
                page_id, frame
            );
            return false;
        }
        tracing::debug!(
            "try_set_wal_tag(page={}, frame={}) SUCCESS: current={:x}",
            page_id,
            frame,
            current
        );
        self.get().wal_tag.store(new_tag, Ordering::Release);
        true
    }

    #[inline]
    pub fn is_valid_for_checkpoint(&self, target_frame: u64, epoch: u32) -> bool {
        let (f, s) = self.wal_tag_pair();
        f == target_frame && s == epoch && !self.is_dirty() && self.is_loaded() && !self.is_locked()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
/// The state of the current pager cache commit.
enum CommitState {
    /// Prepare WAL header for commit if needed
    PrepareWal,
    /// Sync WAL header after prepare
    PrepareWalSync,
    /// Get DB size (mostly from page cache - but in rare cases we can read it from disk)
    GetDbSize,
    /// Appends all frames to the WAL.
    PrepareFrames {
        db_size: u32,
    },
    WaitingForPageRead {
        db_size: u32,
        page_sz: PageSize,
    },
    /// Fsync the on-disk WAL.
    SyncWal,
    /// Wait for WAL sync to complete and finalize the WAL commit.
    /// After this state, the write transaction is durable.
    /// If autocheckpoint is enabled and the autocheckpoint threshold is reached, checkpoint will be attempted.
    WalCommitDone,
    /// Checkpoint the WAL to the database file (if needed).
    /// This is decoupled from commit - checkpoint failure does not affect commit durability.
    AutoCheckpoint,
}

#[derive(Debug, Default)]
struct CheckpointState {
    phase: CheckpointPhase,
    /// The checkpoint result, set after WAL checkpoint completes
    result: Option<CheckpointResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
enum CheckpointPhase {
    #[default]
    NotCheckpointing,
    Checkpoint {
        mode: CheckpointMode,
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
    },
    /// Truncate the database file if everything was backfilled and file is larger than expected.
    TruncateDbFile {
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
    },
    /// Sync the database file after checkpoint (if sync_mode != Off and we backfilled any frames from the WAL).
    SyncDbFile { clear_page_cache: bool },
    /// Finalize: release guard and optionally clear page cache.
    Finalize { clear_page_cache: bool },
}

/// The mode of allocating a btree page.
/// SQLite defines the following:
/// #define BTALLOC_ANY   0           /* Allocate any page */
/// #define BTALLOC_EXACT 1           /* Allocate exact page if possible */
/// #define BTALLOC_LE    2           /* Allocate any page <= the parameter */
pub enum BtreePageAllocMode {
    /// Allocate any btree page
    Any,
    /// Allocate a specific page number, typically used for root page allocation
    Exact(u32),
    /// Allocate a page number less than or equal to the parameter
    Le(u32),
}

/// This will keep track of the state of current cache commit in order to not repeat work
struct CommitInfo {
    completions: Vec<Completion>,
    state: CommitState,
    /// Dirty page IDs to process during PrepareFrames (for async evicted page reads)
    dirty_ids_to_process: Vec<usize>,
    /// Current index into dirty_ids_to_process
    current_dirty_idx: usize,
    /// Pages collected so far during PrepareFrames
    collected_pages: Vec<PageRef>,
    /// Pending read for an evicted page: (page_id, page_ref, read_completion)
    pending_read: Option<(usize, PageRef, Completion)>,
}

impl CommitInfo {
    fn reset(&mut self) {
        self.completions.clear();
        self.state = CommitState::PrepareWal;
        self.dirty_ids_to_process.clear();
        self.current_dirty_idx = 0;
        self.collected_pages.clear();
        self.pending_read = None;
    }
}

/// Track the state of the auto-vacuum mode.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AutoVacuumMode {
    None,
    Full,
    Incremental,
}

impl From<AutoVacuumMode> for u8 {
    fn from(mode: AutoVacuumMode) -> u8 {
        match mode {
            AutoVacuumMode::None => 0,
            AutoVacuumMode::Full => 1,
            AutoVacuumMode::Incremental => 2,
        }
    }
}

impl From<u8> for AutoVacuumMode {
    fn from(value: u8) -> AutoVacuumMode {
        match value {
            0 => AutoVacuumMode::None,
            1 => AutoVacuumMode::Full,
            2 => AutoVacuumMode::Incremental,
            _ => unreachable!("Invalid AutoVacuumMode value: {}", value),
        }
    }
}

#[derive(Debug, Clone)]
#[cfg(not(feature = "omit_autovacuum"))]
enum PtrMapGetState {
    Start,
    Deserialize {
        ptrmap_page: PageRef,
        offset_in_ptrmap_page: usize,
    },
}

#[derive(Debug, Clone)]
#[cfg(not(feature = "omit_autovacuum"))]
enum PtrMapPutState {
    Start,
    Deserialize {
        ptrmap_page: PageRef,
        offset_in_ptrmap_page: usize,
    },
}

#[derive(Debug, Clone)]
enum HeaderRefState {
    Start,
    CreateHeader { page: PageRef },
}

#[cfg(not(feature = "omit_autovacuum"))]
#[derive(Debug, Clone, Copy)]
enum BtreeCreateVacuumFullState {
    Start,
    AllocatePage { root_page_num: u32 },
    PtrMapPut { allocated_page_id: u32 },
}

pub struct Savepoint {
    /// Start offset of this savepoint in the subjournal.
    start_offset: AtomicU64,
    /// Current write offset in the subjournal.
    write_offset: AtomicU64,
    /// Bitmap of page numbers that are dirty in the savepoint.
    page_bitmap: RwLock<RoaringBitmap>,
    /// Database size at the start of the savepoint.
    /// If the database grows during the savepoint and a rollback to the savepoint is performed,
    /// the pages exceeding the database size at the start of the savepoint will be ignored.
    db_size: AtomicU32,
}

impl Savepoint {
    pub fn new(subjournal_offset: u64, db_size: u32) -> Self {
        Self {
            start_offset: AtomicU64::new(subjournal_offset),
            write_offset: AtomicU64::new(subjournal_offset),
            page_bitmap: RwLock::new(RoaringBitmap::new()),
            db_size: AtomicU32::new(db_size),
        }
    }

    pub fn add_dirty_page(&self, page_num: u32) {
        self.page_bitmap.write().insert(page_num);
    }

    pub fn has_dirty_page(&self, page_num: u32) -> bool {
        self.page_bitmap.read().contains(page_num)
    }
}

/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    /// Source of the database pages.
    pub db_file: Arc<dyn DatabaseStorage>,
    /// The write-ahead log (WAL) for the database.
    /// in-memory databases, ephemeral tables and ephemeral indexes do not have a WAL.
    pub(crate) wal: Option<Arc<dyn Wal>>,
    /// A page cache for the database.
    page_cache: Arc<RwLock<PageCache>>,
    /// Buffer pool for temporary data storage.
    pub buffer_pool: Arc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    /// Dirty pages sorted by page number.
    ///
    /// We need dirty pages in page number order when we flush them out to ensure
    /// that the WAL we generate is compatible with SQLite.
    dirty_pages: Arc<RwLock<BTreeSet<usize>>>,
    subjournal: RwLock<Option<Subjournal>>,
    savepoints: Arc<RwLock<Vec<Savepoint>>>,
    commit_info: RwLock<CommitInfo>,
    checkpoint_state: RwLock<CheckpointState>,
    syncing: Arc<AtomicBool>,
    auto_vacuum_mode: AtomicU8,
    /// Mutex for synchronizing database initialization to prevent race conditions
    init_lock: Arc<Mutex<()>>,
    /// The state of the current allocate page operation.
    allocate_page_state: RwLock<AllocatePageState>,
    /// The state of the current allocate page1 operation.
    allocate_page1_state: RwLock<AllocatePage1State>,
    /// Cache page_size and reserved_space at Pager init and reuse for subsequent
    /// `usable_space` calls. TODO: Invalidate reserved_space when we add the functionality
    /// to change it.
    pub(crate) page_size: AtomicU32,
    reserved_space: AtomicU16,
    /// Schema cookie cache.
    ///
    /// Note that schema cookie is 32-bits, but we use 64-bit field so we can
    /// represent case where value is not set.
    schema_cookie: AtomicU64,
    free_page_state: RwLock<FreePageState>,
    /// State machine for async cache spilling.
    spill_state: RwLock<SpillState>,
    /// State machine for async cacheflush operation.
    cacheflush_state: RwLock<CacheFlushState>,
    /// Maximum number of pages allowed in the database. Default is 1073741823 (SQLite default).
    max_page_count: AtomicU32,
    header_ref_state: RwLock<HeaderRefState>,
    #[cfg(not(feature = "omit_autovacuum"))]
    vacuum_state: RwLock<VacuumState>,
    pub(crate) io_ctx: RwLock<IOContext>,
    /// encryption is an opt-in feature. we will enable it only if the flag is passed
    enable_encryption: AtomicBool,
    /// In Memory Page 1 for Empty Dbs
    init_page_1: Arc<ArcSwapOption<Page>>,
}

#[cfg(not(feature = "omit_autovacuum"))]
pub struct VacuumState {
    /// State machine for [Pager::ptrmap_get]
    ptrmap_get_state: PtrMapGetState,
    /// State machine for [Pager::ptrmap_put]
    ptrmap_put_state: PtrMapPutState,
    btree_create_vacuum_full_state: BtreeCreateVacuumFullState,
}

#[derive(Debug, Clone)]
/// The status of the current cache flush.
pub enum PagerCommitResult {
    /// The WAL was written to disk and fsynced.
    WalWritten,
    /// The WAL was written, fsynced, and a checkpoint was performed.
    /// The database file was then also fsynced.
    Checkpointed(CheckpointResult),
    Rollback,
}

#[derive(Debug, Clone)]
enum AllocatePageState {
    Start,
    /// Search the trunk page for an available free list leaf.
    /// If none are found, there are two options:
    /// - If there are no more trunk pages, the freelist is empty, so allocate a new page.
    /// - If there are more trunk pages, use the current first trunk page as the new allocation,
    ///   and set the next trunk page as the database's "first freelist trunk page".
    SearchAvailableFreeListLeaf {
        trunk_page: PageRef,
        current_db_size: u32,
    },
    /// If a freelist leaf is found, reuse it for the page allocation and remove it from the trunk page.
    ReuseFreelistLeaf {
        trunk_page: PageRef,
        leaf_page: PageRef,
        number_of_freelist_leaves: u32,
    },
    /// If a suitable freelist leaf is not found, allocate an entirely new page.
    AllocateNewPage {
        current_db_size: u32,
    },
}

#[derive(Clone)]
enum AllocatePage1State {
    Start,
    Writing { page: PageRef },
    Done,
}

#[derive(Debug, Clone)]
enum FreePageState {
    Start,
    AddToTrunk { page: Arc<Page> },
    NewTrunk { page: Arc<Page> },
}

/// State machine for async cache spilling.
/// Tracks progress of writing dirty pages to WAL or disk.
#[derive(Debug, Default, Clone)]
enum SpillState {
    #[default]
    /// No spill operation in progress
    Idle,
    /// WAL spill in progress, waiting for write completions
    WritingToWal {
        /// Pinned pages being spilled
        pages: Vec<PinGuard>,
        /// Completions to wait for
        completions: Vec<Completion>,
    },
    /// Writing ephemeral tables pages directly to disk
    WritingToDisk {
        /// Pages being spilled
        pages: Vec<PinGuard>,
        /// Completions to wait for
        completions: Vec<Completion>,
    },
}

enum CacheFlushStep {
    /// Yield to caller with pending I/O, resume with given phase
    Yield(CacheFlushState, IOCompletions),
    /// Continue immediately to next phase (no I/O wait)
    Continue(CacheFlushState),
    /// Flush complete, return accumulated completions
    Done(Vec<Completion>),
}

#[derive(Default)]
pub enum CacheFlushState {
    #[default]
    Init,
    WalPrepareStart {
        dirty_ids: Vec<usize>,
        completion: Completion,
    },
    WalPrepareFinish {
        dirty_ids: Vec<usize>,
        completion: Completion,
    },
    Collecting(CollectingState),
    WaitingForRead {
        state: CollectingState,
        page_id: usize,
        page: PageRef,
        completion: Completion,
    },
}

#[derive(Default)]
pub struct CollectingState {
    pub dirty_ids: Vec<usize>,
    pub current_idx: usize,
    pub collected_pages: Vec<PageRef>,
    pub completions: Vec<Completion>,
}

impl Pager {
    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Option<Arc<dyn Wal>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<PageCache>>,
        buffer_pool: Arc<BufferPool>,
        init_lock: Arc<Mutex<()>>,
        init_page_1: Arc<ArcSwapOption<Page>>,
    ) -> Result<Self> {
        let allocate_page1_state = if init_page_1.load().is_some() {
            RwLock::new(AllocatePage1State::Start)
        } else {
            RwLock::new(AllocatePage1State::Done)
        };
        Ok(Self {
            db_file,
            wal,
            page_cache,
            io,
            dirty_pages: Arc::new(RwLock::new(BTreeSet::new())),
            subjournal: RwLock::new(None),
            savepoints: Arc::new(RwLock::new(Vec::new())),
            commit_info: RwLock::new(CommitInfo {
                completions: Vec::new(),
                state: CommitState::PrepareWal,
                dirty_ids_to_process: Vec::new(),
                current_dirty_idx: 0,
                collected_pages: Vec::new(),
                pending_read: None,
            }),
            syncing: Arc::new(AtomicBool::new(false)),
            checkpoint_state: RwLock::new(CheckpointState::default()),
            buffer_pool,
            auto_vacuum_mode: AtomicU8::new(AutoVacuumMode::None.into()),
            init_lock,
            allocate_page1_state,
            page_size: AtomicU32::new(0), // 0 means not set
            reserved_space: AtomicU16::new(RESERVED_SPACE_NOT_SET),
            schema_cookie: AtomicU64::new(Self::SCHEMA_COOKIE_NOT_SET),
            free_page_state: RwLock::new(FreePageState::Start),
            spill_state: RwLock::new(SpillState::Idle),
            cacheflush_state: RwLock::new(CacheFlushState::default()),
            allocate_page_state: RwLock::new(AllocatePageState::Start),
            max_page_count: AtomicU32::new(DEFAULT_MAX_PAGE_COUNT),
            header_ref_state: RwLock::new(HeaderRefState::Start),
            #[cfg(not(feature = "omit_autovacuum"))]
            vacuum_state: RwLock::new(VacuumState {
                ptrmap_get_state: PtrMapGetState::Start,
                ptrmap_put_state: PtrMapPutState::Start,
                btree_create_vacuum_full_state: BtreeCreateVacuumFullState::Start,
            }),
            io_ctx: RwLock::new(IOContext::default()),
            enable_encryption: AtomicBool::new(false),
            init_page_1,
        })
    }

    pub fn init_page_1(&self) -> Arc<ArcSwapOption<Page>> {
        self.init_page_1.clone()
    }

    /// Open the subjournal if not yet open.
    /// The subjournal is a file that is used to store the "before images" of pages for the
    /// current savepoint. If the savepoint is rolled back, the pages can be restored from the subjournal.
    ///
    /// Currently uses MemoryIO, but should eventually be backed by temporary on-disk files.
    pub fn open_subjournal(&self) -> Result<()> {
        if self.subjournal.read().is_some() {
            return Ok(());
        }
        use crate::MemoryIO;

        let db_file_io = Arc::new(MemoryIO::new());
        let file = db_file_io.open_file("subjournal", OpenFlags::Create, false)?;
        let db_file = Subjournal::new(file);
        *self.subjournal.write() = Some(db_file);
        Ok(())
    }

    /// Write page to subjournal if the current savepoint does not currently
    /// contain an an entry for it. In case of a statement-level rollback,
    /// the page image can be restored from the subjournal.
    ///
    /// A buffer of length page_size + 4 bytes is allocated and the page id
    /// is written to the beginning of the buffer. The rest of the buffer is filled with the page contents.
    pub fn subjournal_page_if_required(&self, page: &Page) -> Result<()> {
        if self.subjournal.read().is_none() {
            return Ok(());
        }
        let write_offset = {
            let savepoints = self.savepoints.read();
            let Some(cur_savepoint) = savepoints.last() else {
                return Ok(());
            };
            if cur_savepoint.has_dirty_page(page.get().id as u32) {
                return Ok(());
            }
            cur_savepoint.write_offset.load(Ordering::SeqCst)
        };
        let page_id = page.get().id;
        let page_size = self.page_size.load(Ordering::SeqCst) as usize;
        let buffer = {
            let page_id = page.get().id as u32;
            let contents = page.get_contents();
            let buffer = self.buffer_pool.allocate(page_size + 4);
            let contents_buffer = contents.buffer.as_slice();
            turso_assert!(
                contents_buffer.len() == page_size,
                "contents buffer length should be equal to page size"
            );

            buffer.as_mut_slice()[0..4].copy_from_slice(&page_id.to_be_bytes());
            buffer.as_mut_slice()[4..4 + page_size].copy_from_slice(contents_buffer);

            Arc::new(buffer)
        };

        let savepoints = self.savepoints.clone();

        let write_complete = {
            let buf_copy = buffer.clone();
            Box::new(move |res: Result<i32, CompletionError>| {
                let Ok(bytes_written) = res else {
                    return;
                };
                let buf_copy = buf_copy.clone();
                let buf_len = buf_copy.len();

                turso_assert!(
                    bytes_written == buf_len as i32,
                    "wrote({bytes_written}) != expected({buf_len})"
                );

                let savepoints = savepoints.read();
                let cur_savepoint = savepoints.last().unwrap();
                cur_savepoint.add_dirty_page(page_id as u32);
                cur_savepoint
                    .write_offset
                    .fetch_add(page_size as u64 + 4, Ordering::SeqCst);
            })
        };
        let c = Completion::new_write(write_complete);

        let subjournal = self.subjournal.read();
        let subjournal = subjournal.as_ref().unwrap();

        let c = subjournal.write_page(write_offset, page_size, buffer.clone(), c)?;
        assert!(c.succeeded(), "memory IO should complete immediately");
        Ok(())
    }

    /// try to "acquire" ownership on the subjournal of the connection-scoped pager
    /// if another statement owns the subjournal - return Busy error and let the caller retry attempt later
    pub fn try_use_subjournal(&self) -> Result<()> {
        let subjournal = self.subjournal.read();
        let subjournal = subjournal.as_ref().expect("subjournal must be opened");
        subjournal.try_use()
    }

    /// release ownership of the subjournal
    /// caller must guarantee that [Self::stop_use_subjournal] is called only after successful call to the [Self::try_use_subjournal]
    pub fn stop_use_subjournal(&self) {
        let subjournal = self.subjournal.read();
        let subjournal = subjournal.as_ref().expect("subjournal must be opened");
        subjournal.stop_use()
    }

    /// check if subjournal is in use for some statement
    pub fn subjournal_in_use(&self) -> bool {
        let subjournal = self.subjournal.read();
        let Some(subjournal) = subjournal.as_ref() else {
            return false;
        };
        subjournal.in_use()
    }

    pub fn open_savepoint(&self, db_size: u32) -> Result<()> {
        let subjournal_offset = {
            let subjournal = self.subjournal.read();
            let subjournal = subjournal.as_ref().expect("subjournal must be opened");
            subjournal.size()?
        };
        // Currently as we only have anonymous savepoints opened at the start of a statement,
        // the subjournal offset should always be 0 as we should only have max 1 savepoint
        // opened at any given time.
        turso_assert!(subjournal_offset == 0, "subjournal offset should be 0");
        let savepoint = Savepoint::new(subjournal_offset, db_size);
        let mut savepoints = self.savepoints.write();
        turso_assert!(
            savepoints.is_empty(),
            "savepoints should be empty, but had {} savepoints open",
            savepoints.len()
        );
        savepoints.push(savepoint);
        Ok(())
    }

    /// Release i.e. commit the current savepoint. This basically just means removing it.
    pub fn release_savepoint(&self) -> Result<()> {
        let mut savepoints = self.savepoints.write();
        let Some(savepoint) = savepoints.pop() else {
            return Ok(());
        };
        let subjournal = self.subjournal.read();
        let Some(subjournal) = subjournal.as_ref() else {
            return Ok(());
        };
        let start_offset = savepoint.start_offset.load(Ordering::SeqCst);
        // Same reason as in open_savepoint, the start offset should always be 0 as we should only have max 1 savepoint
        // opened at any given time.
        turso_assert!(start_offset == 0, "start offset should be 0");
        let c = subjournal.truncate(start_offset)?;
        assert!(c.succeeded(), "memory IO should complete immediately");
        Ok(())
    }

    pub fn clear_savepoints(&self) -> Result<()> {
        *self.savepoints.write() = Vec::new();
        let subjournal = self.subjournal.read();
        let Some(subjournal) = subjournal.as_ref() else {
            return Ok(());
        };
        let c = subjournal.truncate(0)?;
        assert!(c.succeeded(), "memory IO should complete immediately");
        Ok(())
    }

    /// Rollback to the newest savepoint. This basically just means reading the subjournal from the start offset
    /// of the savepoint to the end of the subjournal and restoring the page images to the page cache.
    pub fn rollback_to_newest_savepoint(&self) -> Result<bool> {
        let subjournal = self.subjournal.read();
        let Some(subjournal) = subjournal.as_ref() else {
            return Ok(false);
        };
        let mut savepoints = self.savepoints.write();
        let Some(savepoint) = savepoints.pop() else {
            return Ok(false);
        };
        let journal_start_offset = savepoint.start_offset.load(Ordering::SeqCst);

        let mut rollback_bitset = RoaringBitmap::new();

        // Read the subjournal starting from start offset, first reading 4 bytes to get page id, then if rollback_bitset already has the page, skip reading the page
        // and just advance the offset. otherwise read the page and add the page id to the rollback_bitset + put the page image into the page cache
        let mut current_offset = journal_start_offset;
        let page_size = self.page_size.load(Ordering::SeqCst) as u64;
        let journal_end_offset = savepoint.write_offset.load(Ordering::SeqCst);
        let db_size = savepoint.db_size.load(Ordering::SeqCst);

        let mut dirty_pages = self.dirty_pages.write();

        while current_offset < journal_end_offset {
            // Read 4 bytes for page id
            let page_id_buffer = Arc::new(self.buffer_pool.allocate(4));
            let c = subjournal.read_page_number(current_offset, page_id_buffer.clone())?;
            assert!(c.succeeded(), "memory IO should complete immediately");
            let page_id = u32::from_be_bytes(page_id_buffer.as_slice()[0..4].try_into().unwrap());
            current_offset += 4;

            // Check if we've already rolled back this page or if the page is beyond the database size at the start of the savepoint
            let already_rolled_back = rollback_bitset.contains(page_id);
            if already_rolled_back {
                current_offset += page_size;
                continue;
            }
            let page_wont_exist_after_rollback = page_id > db_size;
            if page_wont_exist_after_rollback {
                dirty_pages.remove(&(page_id as usize));
                if let Some(page) = self
                    .page_cache
                    .write()
                    .get(&PageCacheKey::new(page_id as usize))?
                {
                    page.clear_dirty();
                    page.try_unpin();
                }
                current_offset += page_size;
                rollback_bitset.insert(page_id);
                continue;
            }

            // Read the page data
            let page_buffer = Arc::new(self.buffer_pool.allocate(page_size as usize));
            let page = Arc::new(Page::new(page_id as i64));
            let c = subjournal.read_page(
                current_offset,
                page_buffer,
                page.clone(),
                page_size as usize,
            )?;
            assert!(c.succeeded(), "memory IO should complete immediately");
            current_offset += page_size;

            // Add page to rollback bitset
            rollback_bitset.insert(page_id);

            // Put the page image into the page cache
            self.upsert_page_in_cache(page_id as usize, page, false)?;
        }

        let truncate_completion = self
            .subjournal
            .read()
            .as_ref()
            .unwrap()
            .truncate(journal_start_offset)?;
        assert!(
            truncate_completion.succeeded(),
            "memory IO should complete immediately"
        );

        self.page_cache.write().truncate(db_size as usize)?;

        Ok(true)
    }

    #[cfg(feature = "test_helper")]
    pub fn get_pending_byte() -> u32 {
        PENDING_BYTE.load(Ordering::Relaxed)
    }

    #[cfg(feature = "test_helper")]
    /// Used in testing to allow for pending byte pages in smaller dbs
    pub fn set_pending_byte(val: u32) {
        PENDING_BYTE.store(val, Ordering::Relaxed);
    }

    #[cfg(not(feature = "test_helper"))]
    pub const fn get_pending_byte() -> u32 {
        PENDING_BYTE
    }

    /// From SQLITE: https://github.com/sqlite/sqlite/blob/7e38287da43ea3b661da3d8c1f431aa907d648c9/src/btreeInt.h#L608 \
    /// The database page the [PENDING_BYTE] occupies. This page is never used.
    pub fn pending_byte_page_id(&self) -> Option<u32> {
        // PENDING_BYTE_PAGE(pBt)  ((Pgno)((PENDING_BYTE/((pBt)->pageSize))+1))
        let page_size = self.page_size.load(Ordering::SeqCst);
        Self::get_pending_byte()
            .checked_div(page_size)
            .map(|val| val + 1)
    }

    /// Get the maximum page count for this database
    pub fn get_max_page_count(&self) -> u32 {
        self.max_page_count.load(Ordering::SeqCst)
    }

    /// Set the maximum page count for this database
    /// Returns the new maximum page count (may be clamped to current database size)
    pub fn set_max_page_count(&self, new_max: u32) -> crate::Result<IOResult<u32>> {
        // Get current database size
        let current_page_count =
            return_if_io!(self.with_header(|header| header.database_size.get()));

        // Clamp new_max to be at least the current database size
        let clamped_max = std::cmp::max(new_max, current_page_count);
        self.max_page_count.store(clamped_max, Ordering::SeqCst);
        Ok(IOResult::Done(clamped_max))
    }

    pub fn set_wal(&mut self, wal: Arc<dyn Wal>) {
        wal.set_io_context(self.io_ctx.read().clone());
        self.wal = Some(wal);
    }

    pub fn get_auto_vacuum_mode(&self) -> AutoVacuumMode {
        self.auto_vacuum_mode.load(Ordering::SeqCst).into()
    }

    pub fn set_auto_vacuum_mode(&self, mode: AutoVacuumMode) {
        self.auto_vacuum_mode.store(mode.into(), Ordering::SeqCst);
    }

    /// Retrieves the pointer map entry for a given database page.
    /// `target_page_num` (1-indexed) is the page whose entry is sought.
    /// Returns `Ok(None)` if the page is not supposed to have a ptrmap entry (e.g. header, or a ptrmap page itself).
    #[cfg(not(feature = "omit_autovacuum"))]
    pub fn ptrmap_get(&self, target_page_num: u32) -> Result<IOResult<Option<PtrmapEntry>>> {
        loop {
            let ptrmap_get_state = {
                let vacuum_state = self.vacuum_state.read();
                vacuum_state.ptrmap_get_state.clone()
            };
            match ptrmap_get_state {
                PtrMapGetState::Start => {
                    tracing::trace!("ptrmap_get(page_idx = {})", target_page_num);
                    let configured_page_size =
                        return_if_io!(self.with_header(|header| header.page_size)).get() as usize;

                    if target_page_num < FIRST_PTRMAP_PAGE_NO
                        || is_ptrmap_page(target_page_num, configured_page_size)
                    {
                        return Ok(IOResult::Done(None));
                    }

                    let ptrmap_pg_no =
                        get_ptrmap_page_no_for_db_page(target_page_num, configured_page_size);
                    let offset_in_ptrmap_page = get_ptrmap_offset_in_page(
                        target_page_num,
                        ptrmap_pg_no,
                        configured_page_size,
                    )?;
                    tracing::trace!(
                        "ptrmap_get(page_idx = {}) = ptrmap_pg_no = {}",
                        target_page_num,
                        ptrmap_pg_no
                    );

                    let (ptrmap_page, c) = self.read_page(ptrmap_pg_no as i64)?;
                    self.vacuum_state.write().ptrmap_get_state = PtrMapGetState::Deserialize {
                        ptrmap_page,
                        offset_in_ptrmap_page,
                    };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                PtrMapGetState::Deserialize {
                    ptrmap_page,
                    offset_in_ptrmap_page,
                } => {
                    turso_assert!(ptrmap_page.is_loaded(), "ptrmap_page should be loaded");
                    let ptrmap_page_inner = ptrmap_page.get();
                    let ptrmap_pg_no = ptrmap_page_inner.id;

                    let page_content: &PageContent = match ptrmap_page_inner.contents.as_ref() {
                        Some(content) => content,
                        None => {
                            return Err(LimboError::InternalError(format!(
                                "Ptrmap page {ptrmap_pg_no} content not loaded"
                            )));
                        }
                    };

                    let full_buffer_slice: &[u8] = page_content.buffer.as_slice();

                    // Ptrmap pages are not page 1, so their internal offset within their buffer should be 0.
                    // The actual page data starts at page_content.offset within the full_buffer_slice.
                    if ptrmap_pg_no != 1 && page_content.offset != 0 {
                        return Err(LimboError::Corrupt(format!(
                            "Ptrmap page {} has unexpected internal offset {}",
                            ptrmap_pg_no, page_content.offset
                        )));
                    }
                    let ptrmap_page_data_slice: &[u8] = &full_buffer_slice[page_content.offset..];
                    let actual_data_length = ptrmap_page_data_slice.len();

                    // Check if the calculated offset for the entry is within the bounds of the actual page data length.
                    if offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE > actual_data_length {
                        return Err(LimboError::InternalError(format!(
                        "Ptrmap offset {offset_in_ptrmap_page} + entry size {PTRMAP_ENTRY_SIZE} out of bounds for page {ptrmap_pg_no} (actual data len {actual_data_length})"
                    )));
                    }

                    let entry_slice = &ptrmap_page_data_slice
                        [offset_in_ptrmap_page..offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE];
                    self.vacuum_state.write().ptrmap_get_state = PtrMapGetState::Start;
                    break match PtrmapEntry::deserialize(entry_slice) {
                        Some(entry) => Ok(IOResult::Done(Some(entry))),
                        None => Err(LimboError::Corrupt(format!(
                            "Failed to deserialize ptrmap entry for page {target_page_num} from ptrmap page {ptrmap_pg_no}"
                        ))),
                    };
                }
            }
        }
    }

    /// Writes or updates the pointer map entry for a given database page.
    /// `db_page_no_to_update` (1-indexed) is the page whose entry is to be set.
    /// `entry_type` and `parent_page_no` define the new entry.
    #[cfg(not(feature = "omit_autovacuum"))]
    pub fn ptrmap_put(
        &self,
        db_page_no_to_update: u32,
        entry_type: PtrmapType,
        parent_page_no: u32,
    ) -> Result<IOResult<()>> {
        tracing::trace!(
            "ptrmap_put(page_idx = {}, entry_type = {:?}, parent_page_no = {})",
            db_page_no_to_update,
            entry_type,
            parent_page_no
        );
        loop {
            let ptrmap_put_state = {
                let vacuum_state = self.vacuum_state.read();
                vacuum_state.ptrmap_put_state.clone()
            };
            match ptrmap_put_state {
                PtrMapPutState::Start => {
                    let page_size =
                        return_if_io!(self.with_header(|header| header.page_size)).get() as usize;

                    if db_page_no_to_update < FIRST_PTRMAP_PAGE_NO
                        || is_ptrmap_page(db_page_no_to_update, page_size)
                    {
                        return Err(LimboError::InternalError(format!(
                        "Cannot set ptrmap entry for page {db_page_no_to_update}: it's a header/ptrmap page or invalid."
                    )));
                    }

                    let ptrmap_pg_no =
                        get_ptrmap_page_no_for_db_page(db_page_no_to_update, page_size);
                    let offset_in_ptrmap_page =
                        get_ptrmap_offset_in_page(db_page_no_to_update, ptrmap_pg_no, page_size)?;
                    tracing::trace!(
                        "ptrmap_put(page_idx = {}, entry_type = {:?}, parent_page_no = {}) = ptrmap_pg_no = {}, offset_in_ptrmap_page = {}",
                        db_page_no_to_update,
                        entry_type,
                        parent_page_no,
                        ptrmap_pg_no,
                        offset_in_ptrmap_page
                    );

                    let (ptrmap_page, c) = self.read_page(ptrmap_pg_no as i64)?;
                    self.vacuum_state.write().ptrmap_put_state = PtrMapPutState::Deserialize {
                        ptrmap_page,
                        offset_in_ptrmap_page,
                    };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                PtrMapPutState::Deserialize {
                    ptrmap_page,
                    offset_in_ptrmap_page,
                } => {
                    turso_assert!(ptrmap_page.is_loaded(), "page should be loaded");
                    self.add_dirty(&ptrmap_page)?;
                    let ptrmap_page_inner = ptrmap_page.get();
                    let ptrmap_pg_no = ptrmap_page_inner.id;

                    let page_content = match ptrmap_page_inner.contents.as_ref() {
                        Some(content) => content,
                        None => {
                            return Err(LimboError::InternalError(format!(
                                "Ptrmap page {ptrmap_pg_no} content not loaded"
                            )))
                        }
                    };

                    let full_buffer_slice = page_content.buffer.as_mut_slice();

                    if offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE > full_buffer_slice.len() {
                        return Err(LimboError::InternalError(format!(
                        "Ptrmap offset {} + entry size {} out of bounds for page {} (actual data len {})",
                        offset_in_ptrmap_page,
                        PTRMAP_ENTRY_SIZE,
                        ptrmap_pg_no,
                        full_buffer_slice.len()
                    )));
                    }

                    let entry = PtrmapEntry {
                        entry_type,
                        parent_page_no,
                    };
                    entry.serialize(
                        &mut full_buffer_slice
                            [offset_in_ptrmap_page..offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE],
                    )?;

                    turso_assert!(
                        ptrmap_page.get().id == ptrmap_pg_no,
                        "ptrmap page has unexpected number"
                    );
                    self.vacuum_state.write().ptrmap_put_state = PtrMapPutState::Start;
                    break Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// This method is used to allocate a new root page for a btree, both for tables and indexes
    /// FIXME: handle no room in page cache
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn btree_create(&self, flags: &CreateBTreeFlags) -> Result<IOResult<u32>> {
        let page_type = match flags {
            _ if flags.is_table() => PageType::TableLeaf,
            _ if flags.is_index() => PageType::IndexLeaf,
            _ => unreachable!("Invalid flags state"),
        };
        #[cfg(feature = "omit_autovacuum")]
        {
            let page = return_if_io!(self.do_allocate_page(page_type, 0, BtreePageAllocMode::Any));
            Ok(IOResult::Done(page.get().id as u32))
        }

        //  If autovacuum is enabled, we need to allocate a new page number that is greater than the largest root page number
        #[cfg(not(feature = "omit_autovacuum"))]
        {
            let auto_vacuum_mode =
                AutoVacuumMode::from(self.auto_vacuum_mode.load(Ordering::SeqCst));
            match auto_vacuum_mode {
                AutoVacuumMode::None => {
                    let page =
                        return_if_io!(self.do_allocate_page(page_type, 0, BtreePageAllocMode::Any));
                    Ok(IOResult::Done(page.get().id as u32))
                }
                AutoVacuumMode::Full => {
                    loop {
                        let btree_create_vacuum_full_state = {
                            let vacuum_state = self.vacuum_state.read();
                            vacuum_state.btree_create_vacuum_full_state
                        };
                        match btree_create_vacuum_full_state {
                            BtreeCreateVacuumFullState::Start => {
                                let (mut root_page_num, page_size) = return_if_io!(self
                                    .with_header(|header| {
                                        (
                                            header.vacuum_mode_largest_root_page.get(),
                                            header.page_size.get(),
                                        )
                                    }));

                                assert!(root_page_num > 0); //  Largest root page number cannot be 0 because that is set to 1 when creating the database with autovacuum enabled
                                root_page_num += 1;
                                assert!(root_page_num >= FIRST_PTRMAP_PAGE_NO); //  can never be less than 2 because we have already incremented

                                while is_ptrmap_page(root_page_num, page_size as usize) {
                                    root_page_num += 1;
                                }
                                assert!(root_page_num >= 3); //  the very first root page is page 3
                                self.vacuum_state.write().btree_create_vacuum_full_state =
                                    BtreeCreateVacuumFullState::AllocatePage { root_page_num };
                            }
                            BtreeCreateVacuumFullState::AllocatePage { root_page_num } => {
                                //  root_page_num here is the desired root page
                                let page = return_if_io!(self.do_allocate_page(
                                    page_type,
                                    0,
                                    BtreePageAllocMode::Exact(root_page_num),
                                ));
                                let allocated_page_id = page.get().id as u32;

                                return_if_io!(self.with_header_mut(|header| {
                                    if allocated_page_id
                                        > header.vacuum_mode_largest_root_page.get()
                                    {
                                        tracing::debug!(
                                            "Updating largest root page in header from {} to {}",
                                            header.vacuum_mode_largest_root_page.get(),
                                            allocated_page_id
                                        );
                                        header.vacuum_mode_largest_root_page =
                                            allocated_page_id.into();
                                    }
                                }));

                                if allocated_page_id != root_page_num {
                                    //  TODO(Zaid): Handle swapping the allocated page with the desired root page
                                }

                                //  TODO(Zaid): Update the header metadata to reflect the new root page number
                                self.vacuum_state.write().btree_create_vacuum_full_state =
                                    BtreeCreateVacuumFullState::PtrMapPut { allocated_page_id };
                            }
                            BtreeCreateVacuumFullState::PtrMapPut { allocated_page_id } => {
                                //  For now map allocated_page_id since we are not swapping it with root_page_num
                                return_if_io!(self.ptrmap_put(
                                    allocated_page_id,
                                    PtrmapType::RootPage,
                                    0,
                                ));
                                self.vacuum_state.write().btree_create_vacuum_full_state =
                                    BtreeCreateVacuumFullState::Start;
                                return Ok(IOResult::Done(allocated_page_id));
                            }
                        }
                    }
                }
                AutoVacuumMode::Incremental => {
                    unimplemented!()
                }
            }
        }
    }

    /// Allocate a new overflow page.
    /// This is done when a cell overflows and new space is needed.
    // FIXME: handle no room in page cache
    pub fn allocate_overflow_page(&self) -> Result<IOResult<PageRef>> {
        let page = return_if_io!(self.allocate_page());
        tracing::debug!("Pager::allocate_overflow_page(id={})", page.get().id);

        // setup overflow page
        let contents = page.get().contents.as_mut().unwrap();
        let buf = contents.as_ptr();
        buf.fill(0);

        Ok(IOResult::Done(page))
    }

    /// Allocate a new page to the btree via the pager.
    /// This marks the page as dirty and writes the page header.
    // FIXME: handle no room in page cache
    pub fn do_allocate_page(
        &self,
        page_type: PageType,
        offset: usize,
        _alloc_mode: BtreePageAllocMode,
    ) -> Result<IOResult<PageRef>> {
        let page = return_if_io!(self.allocate_page());
        btree_init_page(&page, page_type, offset, self.usable_space());
        tracing::debug!(
            "do_allocate_page(id={}, page_type={:?})",
            page.get().id,
            page.get_contents().page_type()
        );
        Ok(IOResult::Done(page))
    }

    /// The "usable size" of a database page is the page size specified by the 2-byte integer at offset 16
    /// in the header, minus the "reserved" space size recorded in the 1-byte integer at offset 20 in the header.
    /// The usable size of a page might be an odd number. However, the usable size is not allowed to be less than 480.
    /// In other words, if the page size is 512, then the reserved space size cannot exceed 32.
    pub fn usable_space(&self) -> usize {
        let page_size = self.get_page_size().unwrap_or_else(|| {
            let size = self
                .io
                .block(|| self.with_header(|header| header.page_size))
                .unwrap_or_default();
            self.page_size.store(size.get(), Ordering::SeqCst);
            size
        });

        let reserved_space = self.get_reserved_space().unwrap_or_else(|| {
            let space = self
                .io
                .block(|| self.with_header(|header| header.reserved_space))
                .unwrap_or_default();
            self.set_reserved_space(space);
            space
        });

        (page_size.get() as usize) - (reserved_space as usize)
    }

    pub fn db_initialized(&self) -> bool {
        self.init_page_1.load().is_none()
    }

    /// Set the initial page size for the database. Should only be called before the database is initialized
    pub fn set_initial_page_size(&self, size: PageSize) -> Result<()> {
        assert!(!self.db_initialized());
        let IOResult::Done(_) = self.with_header_mut(|header| {
            header.page_size = size;
        })?
        else {
            panic!("DB should not be initialized and should not do any IO");
        };
        self.page_size.store(size.get(), Ordering::SeqCst);
        Ok(())
    }

    /// Get the current page size. Returns None if not set yet.
    pub fn get_page_size(&self) -> Option<PageSize> {
        let value = self.page_size.load(Ordering::SeqCst);
        if value == 0 {
            None
        } else {
            PageSize::new(value)
        }
    }

    /// Get the current page size, panicking if not set.
    pub fn get_page_size_unchecked(&self) -> PageSize {
        let value = self.page_size.load(Ordering::SeqCst);
        assert_ne!(value, 0, "page size not set");
        PageSize::new(value).expect("invalid page size stored")
    }

    /// Set the page size. Used internally when page size is determined.
    pub fn set_page_size(&self, size: PageSize) {
        self.page_size.store(size.get(), Ordering::SeqCst);
    }

    /// Get the current reserved space. Returns None if not set yet.
    pub fn get_reserved_space(&self) -> Option<u8> {
        let value = self.reserved_space.load(Ordering::SeqCst);
        if value == RESERVED_SPACE_NOT_SET {
            None
        } else {
            Some(value as u8)
        }
    }

    /// Set the reserved space. Must fit in u8.
    pub fn set_reserved_space(&self, space: u8) {
        self.reserved_space.store(space as u16, Ordering::SeqCst);
    }

    /// Schema cookie sentinel value that represents value not set.
    const SCHEMA_COOKIE_NOT_SET: u64 = u64::MAX;

    /// Get the cached schema cookie. Returns None if not set yet.
    pub fn get_schema_cookie_cached(&self) -> Option<u32> {
        let value = self.schema_cookie.load(Ordering::SeqCst);
        if value == Self::SCHEMA_COOKIE_NOT_SET {
            None
        } else {
            Some(value as u32)
        }
    }

    /// Set the schema cookie cache.
    pub fn set_schema_cookie(&self, cookie: Option<u32>) {
        match cookie {
            Some(value) => {
                self.schema_cookie.store(value as u64, Ordering::SeqCst);
            }
            None => self
                .schema_cookie
                .store(Self::SCHEMA_COOKIE_NOT_SET, Ordering::SeqCst),
        }
    }

    /// Get the schema cookie, using the cached value if available to avoid reading page 1.
    pub fn get_schema_cookie(&self) -> Result<IOResult<u32>> {
        // Try to use cached value first
        if let Some(cookie) = self.get_schema_cookie_cached() {
            return Ok(IOResult::Done(cookie));
        }
        // If not cached, read from header and cache it
        self.with_header(|header| header.schema_cookie.get())
    }

    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_read_tx(&self) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        let changed = wal.begin_read_tx()?;
        if changed {
            // Someone else changed the database -> assume our page cache is invalid (this is default SQLite behavior, we can probably do better with more granular invalidation)
            self.clear_page_cache(false);
            // Invalidate cached schema cookie to force re-read on next access
            self.set_schema_cookie(None);
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn maybe_allocate_page1(&self) -> Result<IOResult<()>> {
        if !self.db_initialized() {
            if let Some(_lock) = self.init_lock.try_lock() {
                if let IOResult::IO(c) = self.allocate_page1()? {
                    return Ok(IOResult::IO(c));
                } else {
                    return Ok(IOResult::Done(()));
                }
            } else {
                // Give a chance for the allocation to happen elsewhere
                io_yield_one!(Completion::new_yield());
            }
        }
        Ok(IOResult::Done(()))
    }

    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_write_tx(&self) -> Result<IOResult<()>> {
        // TODO(Diego): The only possibly allocate page1 here is because OpenEphemeral needs a write transaction
        // we should have a unique API to begin transactions, something like sqlite3BtreeBeginTrans
        return_if_io!(self.maybe_allocate_page1());
        let Some(wal) = self.wal.as_ref() else {
            return Ok(IOResult::Done(()));
        };
        Ok(IOResult::Done(wal.begin_write_tx()?))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn commit_tx(&self, connection: &Connection) -> Result<IOResult<PagerCommitResult>> {
        if connection.is_nested_stmt() {
            // Parent statement will handle the transaction commit.
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        }
        let Some(wal) = self.wal.as_ref() else {
            // TODO: Unsure what the semantics of "end_tx" is for in-memory databases, ephemeral tables and ephemeral indexes.
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        };
        let (_, schema_did_change) = match connection.get_tx_state() {
            TransactionState::Write { schema_did_change } => (true, schema_did_change),
            _ => (false, false),
        };
        tracing::trace!("commit_tx(schema_did_change={})", schema_did_change);
        let commit_status = return_if_io!(self.commit_dirty_pages(
            connection.is_wal_auto_checkpoint_disabled(),
            connection.get_sync_mode(),
            connection.get_data_sync_retry()
        ));
        wal.end_write_tx();
        wal.end_read_tx();

        if schema_did_change {
            let schema = connection.schema.read().clone();
            connection.db.update_schema_if_newer(schema);
        }
        Ok(IOResult::Done(commit_status))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn rollback_tx(&self, connection: &Connection) {
        if connection.is_nested_stmt() {
            // Parent statement will handle the transaction rollback.
            return;
        }
        let Some(wal) = self.wal.as_ref() else {
            // TODO: Unsure what the semantics of "end_tx" is for in-memory databases, ephemeral tables and ephemeral indexes.
            return;
        };
        let (is_write, schema_did_change) = match connection.get_tx_state() {
            TransactionState::Write { schema_did_change } => (true, schema_did_change),
            _ => (false, false),
        };
        tracing::trace!("rollback_tx(schema_did_change={})", schema_did_change);
        if is_write {
            self.clear_savepoints()
                .expect("in practice, clear_savepoints() should never fail as it uses memory IO");
            wal.end_write_tx();
        }
        wal.end_read_tx();
        self.rollback(schema_did_change, connection, is_write);
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn end_read_tx(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        wal.end_read_tx();
    }

    /// Reads a page from disk (either WAL or DB file) bypassing page-cache
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub fn read_page_no_cache(
        &self,
        page_idx: i64,
        frame_watermark: Option<u64>,
        allow_empty_read: bool,
    ) -> Result<(PageRef, Completion)> {
        assert!(page_idx >= 0);
        tracing::debug!("read_page_no_cache(page_idx = {})", page_idx);
        let page = Arc::new(Page::new(page_idx));
        let io_ctx = self.io_ctx.read();
        let Some(wal) = self.wal.as_ref() else {
            turso_assert!(
                matches!(frame_watermark, Some(0) | None),
                "frame_watermark must be either None or Some(0) because DB has no WAL and read with other watermark is invalid"
            );

            page.set_locked();
            let c = self.begin_read_disk_page(
                page_idx as usize,
                page.clone(),
                allow_empty_read,
                &io_ctx,
            )?;
            return Ok((page, c));
        };

        if let Some(frame_id) = wal.find_frame(page_idx as u64, frame_watermark)? {
            let c = wal.read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            // TODO(pere) should probably first insert to page cache, and if successful,
            // read frame or page
            return Ok((page, c));
        }

        let c =
            self.begin_read_disk_page(page_idx as usize, page.clone(), allow_empty_read, &io_ctx)?;
        Ok((page, c))
    }

    /// Reads a page from the database.
    #[tracing::instrument(skip_all, level = Level::TRACE)]
    pub fn read_page(&self, page_idx: i64) -> Result<(PageRef, Option<Completion>)> {
        assert!(page_idx >= 0, "pages in pager should be positive, negative might indicate unallocated pages from mvcc or any other nasty bug");
        tracing::debug!("read_page(page_idx = {})", page_idx);

        // First check if page is in cache
        {
            let mut page_cache = self.page_cache.write();
            let page_key = PageCacheKey::new(page_idx as usize);
            if let Some(page) = page_cache.get(&page_key)? {
                turso_assert!(
                    page_idx as usize == page.get().id,
                    "attempted to read page {page_idx} but got page {}",
                    page.get().id
                );
                return Ok((page, None));
            }
        }

        tracing::debug!("read_page(page_idx = {page_idx}) = reading page from disk");
        // Page not in cache, read from disk
        let (page, c) = self.read_page_no_cache(page_idx, None, false)?;
        loop {
            match self.cache_insert(page_idx as usize, page.clone())? {
                IOResult::Done(()) => {
                    return Ok((page, Some(c)));
                }
                IOResult::IO(IOCompletions::Single(spill_c)) => {
                    // NOTE: Because `cache_insert` can return completions as *multiple* different states, we cannot
                    // simply create a new CompletionGroup and return it here without inserting the
                    // page into the cache. In order to do this, we would need to make read_page
                    // re-entrant so it continues to call cache_insert and have every caller
                    // propogate the IOResult. For now, we will wait syncronously for spilling IO
                    // on cache insertion on read_page.
                    self.io.wait_for_completion(spill_c)?;
                }
            }
        }
    }

    fn begin_read_disk_page(
        &self,
        page_idx: usize,
        page: PageRef,
        allow_empty_read: bool,
        io_ctx: &IOContext,
    ) -> Result<Completion> {
        sqlite3_ondisk::begin_read_page(
            self.db_file.as_ref(),
            self.buffer_pool.clone(),
            page,
            page_idx,
            allow_empty_read,
            io_ctx,
        )
    }

    /// Insert a page into the cache, with spilling support.
    /// This handles cache full conditions by spilling dirty pages and retrying.
    fn cache_insert(&self, page_idx: usize, page: PageRef) -> Result<IOResult<()>> {
        {
            let mut page_cache = self.page_cache.write();
            let page_key = PageCacheKey::new(page_idx);
            match page_cache.insert(page_key, page.clone()) {
                Ok(_) => return Ok(IOResult::Done(())),
                Err(CacheError::KeyExists) => {
                    unreachable!("Page should not exist in cache after get() miss");
                }
                Err(CacheError::Full) => {
                    // Fall through to spilling
                }
                Err(e) => return Err(e.into()),
            }
        }

        match self.try_spill_dirty_pages()? {
            IOResult::Done(true) => {
                let mut page_cache = self.page_cache.write();
                let page_key = PageCacheKey::new(page_idx);
                match page_cache.insert(page_key, page) {
                    Ok(_) => Ok(IOResult::Done(())),
                    Err(CacheError::KeyExists) => Ok(IOResult::Done(())),
                    Err(e) => Err(e.into()),
                }
            }
            IOResult::Done(false) => Err(LimboError::Busy),
            IOResult::IO(c) => Ok(IOResult::IO(c)),
        }
    }

    // Get a page from the cache, if it exists.
    pub fn cache_get(&self, page_idx: usize) -> Result<Option<PageRef>> {
        tracing::trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx);
        page_cache.get(&page_key)
    }

    /// Get a page from cache only if it matches the target frame
    pub fn cache_get_for_checkpoint(
        &self,
        page_idx: usize,
        target_frame: u64,
        seq: u32,
    ) -> Result<Option<PageRef>> {
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx);
        let page = page_cache.get(&page_key)?.and_then(|page| {
            if page.is_valid_for_checkpoint(target_frame, seq) {
                tracing::debug!(
                    "cache_get_for_checkpoint: page {page_idx} frame {target_frame} is valid",
                );
                Some(page)
            } else {
                tracing::trace!(
                    "cache_get_for_checkpoint: page {} has frame/tag {:?}: (dirty={}), need frame {} and seq {seq}",
                    page_idx,
                    page.wal_tag_pair(),
                    page.is_dirty(),
                    target_frame
                );
                None
            }
        });
        Ok(page)
    }

    /// Changes the size of the page cache.
    pub fn change_page_cache_size(&self, capacity: usize) -> Result<CacheResizeResult> {
        let mut page_cache = self.page_cache.write();
        Ok(page_cache.resize(capacity))
    }

    pub fn add_dirty(&self, page: &Page) -> Result<()> {
        turso_assert!(
            page.is_loaded(),
            "page {} must be loaded in add_dirty() so its contents can be subjournaled",
            page.get().id
        );
        self.subjournal_page_if_required(page)?;
        // TODO: check duplicates?
        let mut dirty_pages = self.dirty_pages.write();
        dirty_pages.insert(page.get().id);
        page.set_dirty();
        Ok(())
    }

    pub fn wal_state(&self) -> Result<WalState> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_state() called on database without WAL".to_string(),
            ));
        };
        Ok(WalState {
            checkpoint_seq_no: wal.get_checkpoint_seq(),
            max_frame: wal.get_max_frame(),
        })
    }

    /// Flush all dirty pages to disk (async/re-entrant).
    /// Unlike commit_dirty_pages, this function does not commit, checkpoint nor sync the WAL/Database.
    #[instrument(skip_all, level = Level::INFO)]
    pub fn cacheflush(&self) -> Result<IOResult<Vec<Completion>>> {
        let wal = self
            .wal
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("cacheflush() called without WAL".into()))?;
        let page_sz = self.get_page_size().unwrap_or_default();

        loop {
            let phase = std::mem::take(&mut *self.cacheflush_state.write());

            match self.cacheflush_step(wal, page_sz, phase)? {
                CacheFlushStep::Yield(next_phase, io) => {
                    *self.cacheflush_state.write() = next_phase;
                    return Ok(IOResult::IO(io));
                }
                CacheFlushStep::Continue(next_phase) => {
                    *self.cacheflush_state.write() = next_phase;
                }
                CacheFlushStep::Done(completions) => {
                    *self.cacheflush_state.write() = CacheFlushState::Init;
                    return Ok(IOResult::Done(completions));
                }
            }
        }
    }

    /// Executes one step of the cache flush state machine.
    #[inline]
    fn cacheflush_step(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        phase: CacheFlushState,
    ) -> Result<CacheFlushStep> {
        match phase {
            CacheFlushState::Init => self.cacheflush_init(wal, page_sz),
            CacheFlushState::WalPrepareStart {
                dirty_ids,
                completion,
            } => self.cacheflush_wal_prepare_start(wal, dirty_ids, completion),
            CacheFlushState::WalPrepareFinish {
                dirty_ids,
                completion,
            } => self.cacheflush_wal_prepare_finish(dirty_ids, completion),
            CacheFlushState::Collecting(state) => self.cacheflush_collect(wal, page_sz, state),
            CacheFlushState::WaitingForRead {
                state,
                page_id,
                page,
                completion,
            } => self.cacheflush_handle_read(wal, page_sz, state, page_id, page, completion),
        }
    }

    /// Init phase: gather dirty page IDs and begin WAL preparation.
    fn cacheflush_init(&self, wal: &Arc<dyn Wal>, page_sz: PageSize) -> Result<CacheFlushStep> {
        let dirty_ids: Vec<usize> = self.dirty_pages.read().iter().copied().collect();

        if dirty_ids.is_empty() {
            return Ok(CacheFlushStep::Done(Vec::new()));
        }

        // Start WAL preparation
        match wal.prepare_wal_start(page_sz)? {
            Some(completion) => Ok(CacheFlushStep::Yield(
                CacheFlushState::WalPrepareStart {
                    dirty_ids,
                    completion: completion.clone(),
                },
                IOCompletions::Single(completion),
            )),
            None => {
                // No async prep needed, go straight to finish
                let completion = wal.prepare_wal_finish()?;
                Ok(CacheFlushStep::Yield(
                    CacheFlushState::WalPrepareFinish {
                        dirty_ids,
                        completion: completion.clone(),
                    },
                    IOCompletions::Single(completion),
                ))
            }
        }
    }

    #[inline]
    /// Wait for WAL prepare_start, then call prepare_finish.
    fn cacheflush_wal_prepare_start(
        &self,
        wal: &Arc<dyn Wal>,
        dirty_ids: Vec<usize>,
        completion: Completion,
    ) -> Result<CacheFlushStep> {
        if !completion.succeeded() {
            return Ok(CacheFlushStep::Yield(
                CacheFlushState::WalPrepareStart {
                    dirty_ids,
                    completion: completion.clone(),
                },
                IOCompletions::Single(completion),
            ));
        }

        let finish_completion = wal.prepare_wal_finish()?;
        Ok(CacheFlushStep::Yield(
            CacheFlushState::WalPrepareFinish {
                dirty_ids,
                completion: finish_completion.clone(),
            },
            IOCompletions::Single(finish_completion),
        ))
    }

    #[inline]
    /// Wait for WAL prepare_finish, then start collecting pages.
    fn cacheflush_wal_prepare_finish(
        &self,
        dirty_ids: Vec<usize>,
        completion: Completion,
    ) -> Result<CacheFlushStep> {
        if !completion.succeeded() {
            return Ok(CacheFlushStep::Yield(
                CacheFlushState::WalPrepareFinish {
                    dirty_ids,
                    completion: completion.clone(),
                },
                IOCompletions::Single(completion),
            ));
        }

        Ok(CacheFlushStep::Continue(CacheFlushState::Collecting(
            CollectingState {
                dirty_ids,
                current_idx: 0,
                collected_pages: Vec::new(),
                completions: Vec::new(),
            },
        )))
    }

    #[inline]
    /// Main collection loop: fetch pages from cache, handle evictions, write batches.
    fn cacheflush_collect(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        mut state: CollectingState,
    ) -> Result<CacheFlushStep> {
        while state.current_idx < state.dirty_ids.len() {
            let page_id = state.dirty_ids[state.current_idx];
            let cache_result = self.page_cache.write().get(&PageCacheKey::new(page_id))?;

            match cache_result {
                Some(page) => {
                    trace!(
                        "cacheflush(page={}, page_type={:?})",
                        page_id,
                        page.get_contents().maybe_page_type()
                    );
                    state.collected_pages.push(page);
                    state.current_idx += 1;
                }
                None => {
                    // Page evicted, need async read from WAL
                    trace!("cacheflush: page {} evicted, reading from WAL", page_id);
                    let (page, completion) =
                        self.read_page_no_cache(page_id as i64, None, false)?;

                    if !completion.succeeded() {
                        return Ok(CacheFlushStep::Yield(
                            CacheFlushState::WaitingForRead {
                                state,
                                page_id,
                                page,
                                completion: completion.clone(),
                            },
                            IOCompletions::Single(completion),
                        ));
                    }

                    // Sync read completed immediately
                    trace!(
                        "cacheflush(page={}, page_type={:?}) [re-read sync]",
                        page_id,
                        page.get_contents().maybe_page_type()
                    );
                    state.collected_pages.push(page);
                    state.current_idx += 1;
                }
            }
            if Self::should_flush_batch(&state) {
                self.flush_page_batch(wal, page_sz, &mut state)?;
            }
        }
        // All pages collected and written
        Ok(CacheFlushStep::Done(state.completions))
    }

    /// Handle completion of async page read for evicted page.
    fn cacheflush_handle_read(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        mut state: CollectingState,
        page_id: usize,
        page: PageRef,
        completion: Completion,
    ) -> Result<CacheFlushStep> {
        if !completion.succeeded() {
            return Ok(CacheFlushStep::Yield(
                CacheFlushState::WaitingForRead {
                    state,
                    page_id,
                    page,
                    completion: completion.clone(),
                },
                IOCompletions::Single(completion),
            ));
        }
        trace!(
            "cacheflush(page={}, page_type={:?}) [re-read complete]",
            page_id,
            page.get_contents().maybe_page_type()
        );
        state.collected_pages.push(page);
        state.current_idx += 1;
        if Self::should_flush_batch(&state) {
            self.flush_page_batch(wal, page_sz, &mut state)?;
        }

        Ok(CacheFlushStep::Continue(CacheFlushState::Collecting(state)))
    }

    #[inline]
    fn should_flush_batch(state: &CollectingState) -> bool {
        let at_capacity = state.collected_pages.len() == IOV_MAX;
        let at_end = state.current_idx >= state.dirty_ids.len();
        !state.collected_pages.is_empty() && (at_capacity || at_end)
    }

    /// Writes accumulated pages to WAL as a single vectored append.
    #[inline]
    fn flush_page_batch(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        state: &mut CollectingState,
    ) -> Result<()> {
        let pages = std::mem::take(&mut state.collected_pages);
        // Mark pages as write-pending to detect concurrent modifications
        for page in &pages {
            page.set_write_pending();
        }
        match wal.append_frames_vectored(pages, page_sz, None) {
            Ok(completion) => {
                state.completions.push(completion);
                Ok(())
            }
            Err(e) => {
                self.io.cancel(&state.completions)?;
                self.io.drain()?;
                Err(e)
            }
        }
    }

    /// Attempt to spill dirty pages from the cache to make room for new pages.
    /// This is called when the cache reaches its spill threshold.
    ///
    /// For databases with a WAL: write only spillable dirty pages to WAL,
    /// then mark them as spilled so they can be evicted even while dirty.
    /// For ephemeral tables: writes pages directly to the temp database file.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn try_spill_dirty_pages(&self) -> Result<IOResult<bool>> {
        let state = self.spill_state.read().clone();
        match state {
            SpillState::Idle => {
                // Check if spilling is needed
                let spill_result = {
                    let cache = self.page_cache.read();
                    cache.check_spill(IOV_MAX)
                };
                match spill_result {
                    SpillResult::NotNeeded | SpillResult::Disabled => {
                        return Ok(IOResult::Done(false));
                    }
                    SpillResult::CacheFull => {
                        tracing::debug!("try_spill_dirty_pages: cache full, no spillable pages");
                        return Ok(IOResult::Done(false));
                    }
                    SpillResult::PagesToSpill(pages) => {
                        if pages.is_empty() {
                            return Ok(IOResult::Done(false));
                        }
                        let page_count = pages.len();
                        tracing::debug!("try_spill_dirty_pages: spilling {} pages", page_count);
                        if self.wal.is_some() {
                            let Some(wal) = self.wal.as_ref() else {
                                unreachable!("wal checked above");
                            };
                            let page_sz = self.get_page_size().unwrap_or_default();

                            // Ensure WAL is initialized. Most of the time this is a no-op.
                            let prepare = wal.prepare_wal_start(page_sz)?;
                            if let Some(c) = prepare {
                                self.io.wait_for_completion(c)?;
                                let c = wal.prepare_wal_finish()?;
                                self.io.wait_for_completion(c)?;
                            }

                            let wal_pages: Vec<PageRef> = pages
                                .iter()
                                .map(|p| {
                                    // Set write_pending on all pages before WAL write so callback can
                                    // detect mid-write modifications.
                                    p.set_write_pending();
                                    p.to_page()
                                })
                                .collect();
                            let c = wal.append_frames_vectored(wal_pages, page_sz, None)?;

                            if c.succeeded() {
                                // Synchronous completion, WAL tags already set by callback.
                                for page in &pages {
                                    if page.has_wal_tag() {
                                        page.set_spilled();
                                    }
                                }
                                *self.spill_state.write() = SpillState::Idle;
                                return Ok(IOResult::Done(true));
                            }
                            *self.spill_state.write() = SpillState::WritingToWal {
                                pages,
                                completions: vec![c.clone()],
                            };
                            io_yield_one!(c);
                        } else {
                            let mut group = CompletionGroup::new(|_| {});
                            // Ephemeral table case: write directly to temp file
                            for page in &pages {
                                page.set_write_pending();
                            }
                            let completions = self.spill_pages_to_disk(&pages)?;
                            if completions.is_empty() {
                                self.finish_ephemeral_spill(&pages);
                                return Ok(IOResult::Done(true));
                            }
                            for completion in &completions {
                                group.add(completion);
                            }
                            *self.spill_state.write() = SpillState::WritingToDisk {
                                pages,
                                completions: completions.clone(),
                            };
                            io_yield_one!(group.build());
                        }
                    }
                }
            }
            SpillState::WritingToWal { pages, completions } => {
                for c in &completions {
                    if !c.succeeded() {
                        io_yield_one!(c.clone());
                    }
                }
                // All I/O complete, pages are now in WAL.
                // Mark spilled pages so they can be evicted while dirty.
                // Only do so if page wasn't modified since write started (each page has valid wal_tag).
                let mut spilled_count = 0;
                for page in &pages {
                    if page.has_wal_tag() {
                        page.set_spilled();
                        spilled_count += 1;
                    } else {
                        // Page was modified during write, it will need to be re-spilled
                        tracing::debug!(
                            "try_spill_dirty_pages: page {} modified during write, not marking as spilled",
                            page.get().id
                        );
                    }
                }
                if spilled_count == 0 && !pages.is_empty() {
                    tracing::warn!(
                        "try_spill_dirty_pages: no pages marked as spilled out of {}, all were modified during write",
                        pages.len()
                    );
                }
                *self.spill_state.write() = SpillState::Idle;
                trace!(
                    "try_spill_dirty_pages: successfully spilled {} / {} pages to WAL",
                    spilled_count,
                    pages.len(),
                );
                return Ok(IOResult::Done(true));
            }
            SpillState::WritingToDisk { pages, completions } => {
                let all_done = completions.iter().all(|c| c.succeeded());
                if !all_done {
                    for c in &completions {
                        if !c.succeeded() {
                            io_yield_one!(c.clone());
                        }
                    }
                }
                // All I/O complete, finish ephemeral spill
                self.finish_ephemeral_spill(&pages);
                *self.spill_state.write() = SpillState::Idle;
                trace!(
                    "try_spill_dirty_pages: successfully spilled {} pages to disk",
                    pages.len()
                );
                return Ok(IOResult::Done(true));
            }
        }
    }

    /// Wait for any in-flight spill writes to finish.
    /// This prevents publishing WAL metadata that references frames that are not yet durable.
    fn wait_for_spill_completions(&self) -> Result<IOResult<()>> {
        loop {
            let state = self.spill_state.read().clone();
            if matches!(state, SpillState::Idle) {
                return Ok(IOResult::Done(()));
            }
            match self.try_spill_dirty_pages()? {
                IOResult::Done(_) => continue,
                IOResult::IO(c) => return Ok(IOResult::IO(c)),
            }
        }
    }

    /// Finish a spill operation for ephemeral tables
    fn finish_ephemeral_spill(&self, pages: &[PinGuard]) {
        for page in pages {
            let tag = page.get().wal_tag.load(Ordering::Acquire);
            // wal tag is set to TAG_UNSET when adding to dirty_pages, meaning that this
            // page was dirtied after the spill started, so we don't clear the dirty flag in that case
            if tag != TAG_UNSET {
                page.clear_dirty();
            }
        }
    }
    /// Write a set of pages directly to the database file (for ephemeral tables without WAL).
    /// This is used by try_spill_dirty_pages for ephemeral tables/indexes.
    fn spill_pages_to_disk(&self, pages: &[PinGuard]) -> Result<Vec<Completion>> {
        let mut completions: Vec<Completion> = Vec::with_capacity(pages.len());
        for page in pages {
            match begin_write_btree_page(self, &page.to_page()) {
                Ok(c) => completions.push(c),
                Err(e) => {
                    self.io.cancel(&completions)?;
                    self.io.drain()?;
                    return Err(e);
                }
            }
        }

        Ok(completions)
    }

    /// Check if the cache needs spilling and attempt to spill if necessary.
    /// This should be called before inserting new pages into the cache.
    pub fn ensure_cache_space(&self) -> Result<IOResult<()>> {
        let needs_spill = {
            let cache = self.page_cache.read();
            cache.needs_spill()
        };

        if needs_spill {
            match self.try_spill_dirty_pages()? {
                IOResult::Done(spilled) => {
                    if spilled {
                        // After spilling, try to evict clean pages to make room in the cache
                        let mut cache = self.page_cache.write();
                        if let Err(e) = cache.make_room_for(1) {
                            // Cache is completely full with unevictable pages
                            tracing::error!(
                                "ensure_cache_space: {e} cache full, could not make room"
                            );
                            return Err(LimboError::CacheError(CacheError::Full));
                        }
                    }
                }
                IOResult::IO(completion) => {
                    return Ok(IOResult::IO(completion));
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Flush all dirty pages to disk.
    /// In the base case, it will write the dirty pages to the WAL and then fsync the WAL.
    /// If the WAL size is over the checkpoint threshold, it will checkpoint the WAL to
    /// the database file and then fsync the database file.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn commit_dirty_pages(
        &self,
        wal_auto_checkpoint_disabled: bool,
        sync_mode: crate::SyncMode,
        data_sync_retry: bool,
    ) -> Result<IOResult<PagerCommitResult>> {
        // Reset commit_info at the start of each commit to ensure clean state.
        // This is necessary because commit_info is shared across connections,
        // and a previous connection's commit may have left stale state.
        {
            let commit_info = self.commit_info.read();
            if commit_info.state == CommitState::PrepareWal {
                // Only reset if we're starting fresh (not resuming from IO yield)
                drop(commit_info);
                self.commit_info.write().reset();
            }
        }

        // Make sure any asynchronous spill writes are finished before we start publishing
        // new frames to readers. Otherwise wal.max_frame may get ahead of what is actually
        // on disk and readers can attempt to read unwritten frames.
        if let IOResult::IO(c) = self.wait_for_spill_completions()? {
            return Ok(IOResult::IO(c));
        }

        match self.commit_dirty_pages_inner(
            wal_auto_checkpoint_disabled,
            sync_mode,
            data_sync_retry,
        ) {
            r @ (Ok(IOResult::Done(..)) | Err(..)) => {
                let mut commit_info = self.commit_info.write();
                commit_info.reset();
                r
            }
            Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn commit_dirty_pages_inner(
        &self,
        wal_auto_checkpoint_disabled: bool,
        sync_mode: crate::SyncMode,
        data_sync_retry: bool,
    ) -> Result<IOResult<PagerCommitResult>> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "commit_dirty_pages() called on database without WAL".to_string(),
            ));
        };

        'outer: loop {
            let state = self.commit_info.read().state;
            trace!(?state);
            match state {
                CommitState::PrepareWal => {
                    let page_sz = self.get_page_size_unchecked();
                    let c = wal.prepare_wal_start(page_sz)?;
                    let Some(c) = c else {
                        self.commit_info.write().state = CommitState::GetDbSize;
                        continue;
                    };
                    self.commit_info.write().state = CommitState::PrepareWalSync;
                    if !c.succeeded() {
                        io_yield_one!(c);
                    }
                }
                CommitState::PrepareWalSync => {
                    let c = wal.prepare_wal_finish()?;
                    self.commit_info.write().state = CommitState::GetDbSize;
                    if !c.succeeded() {
                        io_yield_one!(c);
                    }
                }
                CommitState::GetDbSize => {
                    let db_size = return_if_io!(self.with_header(|header| header.database_size));
                    self.commit_info.write().state = CommitState::PrepareFrames {
                        db_size: db_size.get(),
                    };
                }
                CommitState::PrepareFrames { db_size } => {
                    let page_sz = self.get_page_size_unchecked();
                    // Initialize dirty_ids on first entry, since this state is re-entrant
                    // we need to be sure to clear the dirty_ids_to_process on any success or error
                    // path.
                    {
                        let mut commit_info = self.commit_info.write();
                        if commit_info.dirty_ids_to_process.is_empty()
                            && commit_info.current_dirty_idx == 0
                        {
                            let dirty_pages = self.dirty_pages.read();
                            commit_info.dirty_ids_to_process =
                                dirty_pages.iter().copied().collect();
                            if commit_info.dirty_ids_to_process.is_empty() {
                                return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                            }
                        }
                    }

                    // Collect pages until we need to yield or finish
                    'inner: loop {
                        let (total, page_id) = {
                            let commit_info = self.commit_info.read();
                            let idx = commit_info.current_dirty_idx;
                            let total = commit_info.dirty_ids_to_process.len();
                            if idx >= total {
                                break 'inner;
                            }
                            (total, commit_info.dirty_ids_to_process[idx])
                        };

                        let page_key = PageCacheKey::new(page_id);
                        let cache_result = self.page_cache.write().get(&page_key)?;

                        match cache_result {
                            Some(page) => {
                                let mut commit_info = self.commit_info.write();
                                commit_info.collected_pages.push(page);
                                commit_info.current_dirty_idx += 1;

                                let is_last = commit_info.current_dirty_idx >= total;
                                if commit_info.collected_pages.len() == IOV_MAX || is_last {
                                    self.flush_collected_to_wal(
                                        &mut commit_info,
                                        page_sz,
                                        db_size,
                                        is_last,
                                    )?;
                                }
                            }
                            None => {
                                // Page evicted, transition to waiting state
                                let (page, completion) =
                                    self.read_page_no_cache(page_id as i64, None, false)?;

                                let mut commit_info = self.commit_info.write();
                                commit_info.pending_read =
                                    Some((page_id, page, completion.clone()));
                                commit_info.state =
                                    CommitState::WaitingForPageRead { db_size, page_sz };

                                if !completion.succeeded() {
                                    io_yield_one!(completion);
                                }
                                continue 'outer;
                            }
                        }
                    }

                    // Nothing to append
                    if self.commit_info.read().completions.is_empty() {
                        turso_assert!(
                            self.dirty_pages.read().is_empty(),
                            "dirty pages must be empty if we didnt collect any completions"
                        );
                        return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                    } else {
                        // Skip sync if synchronous mode is OFF, go directly to WalCommitDone
                        let mut commit_info = self.commit_info.write();
                        if sync_mode == crate::SyncMode::Off {
                            commit_info.state = CommitState::WalCommitDone;
                        } else {
                            commit_info.state = CommitState::SyncWal;
                        }
                    }
                }
                CommitState::WaitingForPageRead { db_size, page_sz } => {
                    let mut commit_info = self.commit_info.write();
                    let (page_id, page, completion) = commit_info
                        .pending_read
                        .take()
                        .expect("must have pending_read in WaitingForPageRead state");

                    if !completion.succeeded() {
                        commit_info.pending_read = Some((page_id, page, completion.clone()));
                        io_yield_one!(completion);
                    }
                    // Read complete, add to collected pages
                    commit_info.collected_pages.push(page);
                    commit_info.current_dirty_idx += 1;

                    // Check if batch needed
                    let total = commit_info.dirty_ids_to_process.len();
                    let is_last = commit_info.current_dirty_idx >= total;
                    if commit_info.collected_pages.len() == IOV_MAX || is_last {
                        self.flush_collected_to_wal(&mut commit_info, page_sz, db_size, is_last)?;
                    }
                    // Return to PrepareFrames to continue collecting
                    commit_info.state = CommitState::PrepareFrames { db_size };
                }
                CommitState::SyncWal => {
                    let sync_result = wal.sync();
                    let c = match sync_result {
                        Ok(c) => c,
                        Err(e) if !data_sync_retry => {
                            panic!("fsync error (data_sync_retry=off): {e:?}");
                        }
                        Err(e) => return Err(e),
                    };
                    self.commit_info.write().completions.push(c);
                    self.commit_info.write().state = CommitState::WalCommitDone;
                }
                CommitState::WalCommitDone => {
                    let pending_io = {
                        let commit_info = self.commit_info.read();
                        !commit_info.completions.iter().all(|c| c.succeeded())
                    };
                    if pending_io {
                        let completion = {
                            let mut commit_info = self.commit_info.write();
                            let mut group = CompletionGroup::new(|_| {});
                            for c in commit_info.completions.drain(..) {
                                group.add(&c);
                            }
                            group.build()
                        };
                        io_yield_one!(completion);
                    }
                    // WAL sync is complete - finalize the WAL commit
                    // After this point, the write transaction is durable regardless of checkpoint outcome
                    self.commit_info.write().completions.clear();
                    wal.finish_append_frames_commit()?;
                    self.dirty_pages.write().clear();

                    if wal_auto_checkpoint_disabled || !wal.should_checkpoint() {
                        return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                    }
                    self.commit_info.write().state = CommitState::AutoCheckpoint;
                }
                CommitState::AutoCheckpoint => {
                    // Auto-checkpoint uses Passive mode. Sync is handled by checkpoint() based on sync_mode.
                    let checkpoint_mode = CheckpointMode::Passive {
                        upper_bound_inclusive: None,
                    };
                    // Auto-checkpoint does NOT clear page cache (only explicit checkpoints do)
                    match self.checkpoint(checkpoint_mode, sync_mode, false) {
                        Err(LimboError::Busy) => {
                            // Auto-checkpoint during commit uses Passive mode, which can return
                            // Busy if either:
                            // 1. Another connection is already checkpointing (checkpoint_lock held)
                            // 2. A reader is active on read slot 0 (read_locks[0] held)
                            // In either case, skip the checkpoint and complete the commit. The WAL
                            // frames are already written, so the commit succeeds. Checkpoint will
                            // happen later when conditions allow.
                            tracing::debug!("Auto-checkpoint skipped due to busy (lock conflict)");
                            // WAL commit is already finalized in WalCommitDone, just return success
                            return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                        }
                        Err(e) => {
                            // Checkpoint failed but WAL commit is already finalized (durable).
                            // Return CheckpointFailed to inform caller that commit succeeded
                            // but autocheckpoint failed.
                            tracing::error!("Auto-checkpoint failed: {}", e);
                            return Err(LimboError::CheckpointFailed(e.to_string()));
                        }
                        Ok(IOResult::IO(IOCompletions::Single(c))) => {
                            io_yield_one!(c);
                        }
                        Ok(IOResult::Done(res)) => {
                            return Ok(IOResult::Done(PagerCommitResult::Checkpointed(res)));
                        }
                    }
                }
            }
        }
    }

    /// Writes collected pages to WAL as a single batch.
    /// Clears `collected_pages` and appends completion to `frame_completions`.
    fn flush_collected_to_wal(
        &self,
        commit_info: &mut CommitInfo,
        page_sz: PageSize,
        db_size: u32,
        is_commit_frame: bool,
    ) -> Result<()> {
        let pages = std::mem::take(&mut commit_info.collected_pages);
        if pages.is_empty() {
            return Ok(());
        }

        let commit_flag = if is_commit_frame { Some(db_size) } else { None };
        for page in &pages {
            page.set_write_pending();
        }

        let wal = self.wal.as_ref().unwrap();
        match wal.append_frames_vectored(pages, page_sz, commit_flag) {
            Ok(c) => {
                commit_info.completions.push(c);
                Ok(())
            }
            Err(e) => {
                self.io.cancel(&commit_info.completions)?;
                Err(e)
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        let wal = self.wal.as_ref().unwrap();
        wal.changed_pages_after(frame_watermark)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<Completion> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_get_frame() called on database without WAL".to_string(),
            ));
        };
        wal.read_frame_raw(frame_no, frame)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<WalFrameInfo> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_insert_frame() called on database without WAL".to_string(),
            ));
        };
        let (header, raw_page) = parse_wal_frame_header(frame);

        wal.write_frame_raw(
            self.buffer_pool.clone(),
            frame_no,
            header.page_number as u64,
            header.db_size as u64,
            raw_page,
        )?;
        if let Some(page) = self.cache_get(header.page_number as usize)? {
            let content = page.get_contents();
            content.as_ptr().copy_from_slice(raw_page);
            turso_assert!(
                page.get().id == header.page_number as usize,
                "page has unexpected id"
            );
        }
        if header.page_number == 1 {
            let db_size = self
                .io
                .block(|| self.with_header(|header| header.database_size))?;
            tracing::debug!("truncate page_cache as first page was written: {}", db_size);
            let mut page_cache = self.page_cache.write();
            page_cache.truncate(db_size.get() as usize).map_err(|e| {
                LimboError::InternalError(format!("Failed to truncate page cache: {e:?}"))
            })?;
        }
        if header.is_commit_frame() {
            let mut dirty_pages = self.dirty_pages.write();
            tracing::debug!(
                "wal_callback: commit frame, clearing {} dirty pages",
                dirty_pages.len()
            );
            let mut cache = self.page_cache.write();
            for page_id in dirty_pages.iter() {
                let page_key = PageCacheKey::new(*page_id);
                // Page may have been evicted from cache after spilling to WAL
                if let Some(page) = cache.get(&page_key)? {
                    page.clear_dirty();
                }
            }
            dirty_pages.clear();
        }
        Ok(WalFrameInfo {
            page_no: header.page_number,
            db_size: header.db_size,
        })
    }

    pub fn is_checkpointing(&self) -> bool {
        self.checkpoint_state.read().phase != CheckpointPhase::NotCheckpointing
    }

    fn reset_checkpoint_state(&self) {
        let mut state = self.checkpoint_state.write();
        state.phase = CheckpointPhase::NotCheckpointing;
        state.result = None;
        self.commit_info.write().state = CommitState::PrepareWal;
    }

    /// Clean up after a checkpoint failure. The WAL commit succeeded but checkpoint failed.
    /// This ends the write and read transactions that would normally be ended in commit_tx().
    pub fn finish_commit_after_checkpoint_failure(&self) {
        self.reset_checkpoint_state();
        if let Some(wal) = self.wal.as_ref() {
            wal.abort_checkpoint();
            wal.end_write_tx();
            wal.end_read_tx();
        }
    }

    #[instrument(skip_all, level = Level::DEBUG, name = "pager_checkpoint",)]
    /// Checkpoint the WAL to the database file (if needed).
    /// Args:
    /// - mode: The checkpoint mode to use (PASSIVE, FULL, RESTART, TRUNCATE)
    /// - sync_mode: The fsync mode to use (OFF, NORMAL, FULL)
    /// - clear_page_cache: Whether to clear the page cache after checkpointing
    pub fn checkpoint(
        &self,
        mode: CheckpointMode,
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
    ) -> Result<IOResult<CheckpointResult>> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "checkpoint() called on database without WAL".to_string(),
            ));
        };
        loop {
            // Clone the phase to check what state we're in, but keep result in place
            // This is important because we need to be careful not to e.g. clone and drop the checkpoint result which
            // causes a drop of CheckpointLocks prematurely and results in a panic.
            let phase = self.checkpoint_state.read().phase.clone();
            match phase {
                CheckpointPhase::NotCheckpointing => {
                    self.checkpoint_state.write().phase = CheckpointPhase::Checkpoint {
                        mode,
                        sync_mode,
                        clear_page_cache,
                    };
                }
                CheckpointPhase::Checkpoint {
                    mode,
                    sync_mode,
                    clear_page_cache,
                } => {
                    let res = return_if_io!(wal.checkpoint(self, mode));
                    let mut state = self.checkpoint_state.write();
                    if res.num_backfilled != 0
                        && res.everything_backfilled()
                        && matches!(mode, CheckpointMode::Truncate { .. })
                    {
                        // If we backfilled anything, check if we need to also truncate the database file
                        state.phase = CheckpointPhase::TruncateDbFile {
                            sync_mode,
                            clear_page_cache,
                        };
                    } else if res.num_backfilled == 0 || sync_mode == crate::SyncMode::Off {
                        // Nothing was backfilled so no need to truncate or fsync or anything else
                        state.phase = CheckpointPhase::Finalize { clear_page_cache };
                    } else {
                        state.phase = CheckpointPhase::SyncDbFile { clear_page_cache };
                    }
                    state.result = Some(res);
                }
                CheckpointPhase::TruncateDbFile {
                    sync_mode,
                    clear_page_cache,
                } => {
                    let should_skip_truncate = {
                        let state = self.checkpoint_state.read();
                        let result = state.result.as_ref().expect("result should be set");
                        // Skip if we already sent truncate
                        result.db_truncate_sent
                    };

                    if should_skip_truncate {
                        let mut state = self.checkpoint_state.write();
                        if sync_mode == crate::SyncMode::Off {
                            state.phase = CheckpointPhase::Finalize { clear_page_cache };
                        } else {
                            state.phase = CheckpointPhase::SyncDbFile { clear_page_cache };
                        }
                        continue;
                    }

                    // Truncate the database file unless already at correct size
                    let db_size =
                        return_if_io!(self.with_header(|header| header.database_size)).get();
                    let page_size = self.get_page_size().unwrap_or_default();
                    let expected = (db_size * page_size.get()) as u64;
                    if expected >= self.db_file.size()? {
                        // No truncation needed, move to next phase
                        let mut state = self.checkpoint_state.write();
                        if sync_mode == crate::SyncMode::Off {
                            state.phase = CheckpointPhase::Finalize { clear_page_cache };
                        } else {
                            state.phase = CheckpointPhase::SyncDbFile { clear_page_cache };
                        }
                        continue;
                    }
                    let c = self.db_file.truncate(
                        expected as usize,
                        Completion::new_trunc(move |_| {
                            tracing::trace!(
                                "Database file truncated to expected size: {} bytes",
                                expected
                            );
                        }),
                    )?;
                    self.checkpoint_state
                        .write()
                        .result
                        .as_mut()
                        .expect("result should be set")
                        .db_truncate_sent = true;
                    io_yield_one!(c);
                }
                CheckpointPhase::SyncDbFile { clear_page_cache } => {
                    let need_sync_db_file = {
                        let state = self.checkpoint_state.read();
                        let result = state.result.as_ref().expect("result should be set");
                        !result.db_sync_sent
                    };

                    if !need_sync_db_file {
                        turso_assert!(
                            !self.syncing.load(Ordering::SeqCst),
                            "syncing should be done"
                        );
                        self.checkpoint_state.write().phase =
                            CheckpointPhase::Finalize { clear_page_cache };
                        continue;
                    }

                    let c =
                        sqlite3_ondisk::begin_sync(self.db_file.as_ref(), self.syncing.clone())?;
                    self.checkpoint_state
                        .write()
                        .result
                        .as_mut()
                        .expect("result should be set")
                        .db_sync_sent = true;
                    io_yield_one!(c);
                }
                CheckpointPhase::Finalize { clear_page_cache } => {
                    let mut state = self.checkpoint_state.write();
                    let mut res = state.result.take().expect("result should be set");

                    // Release checkpoint guard
                    res.release_guard();

                    // Clear page cache only if requested (explicit checkpoints do this, auto-checkpoint does not)
                    if clear_page_cache {
                        self.page_cache.write().clear(false).map_err(|e| {
                            LimboError::InternalError(format!("Failed to clear page cache: {e:?}"))
                        })?;
                    }

                    state.phase = CheckpointPhase::NotCheckpointing;
                    return Ok(IOResult::Done(res));
                }
            }
        }
    }

    /// Invalidates entire page cache by removing all dirty and clean pages. Usually used in case
    /// of a rollback or in case we want to invalidate page cache after starting a read transaction
    /// right after new writes happened which would invalidate current page cache.
    pub fn clear_page_cache(&self, clear_dirty: bool) {
        let dirty_pages = self.dirty_pages.write();
        let mut cache = self.page_cache.write();
        for page_id in dirty_pages.iter() {
            let page_key = PageCacheKey::new(*page_id);
            if let Some(page) = cache.get(&page_key).unwrap_or(None) {
                page.clear_dirty();
            }
        }
        cache
            .clear(clear_dirty)
            .expect("Failed to clear page cache");
        if clear_dirty {
            drop(dirty_pages);
            self.dirty_pages.write().clear();
        }
    }

    /// Checkpoint in Truncate mode and delete the WAL file. This method is _only_ to be called
    /// for shutting down the last remaining connection to a database.
    ///
    /// sqlite3.h
    /// Usually, when a database in [WAL mode] is closed or detached from a
    /// database handle, SQLite checks if if there are other connections to the
    /// same database, and if there are no other database connection (if the
    /// connection being closed is the last open connection to the database),
    /// then SQLite performs a [checkpoint] before closing the connection and
    /// deletes the WAL file.
    pub fn checkpoint_shutdown(
        &self,
        wal_auto_checkpoint_disabled: bool,
        sync_mode: crate::SyncMode,
    ) -> Result<()> {
        let mut attempts = 0;
        {
            let Some(wal) = self.wal.as_ref() else {
                return Err(LimboError::InternalError(
                    "checkpoint_shutdown() called on database without WAL".to_string(),
                ));
            };
            // fsync the wal syncronously before beginning checkpoint
            let c = wal.sync()?;
            self.io.wait_for_completion(c)?;
        }
        if !wal_auto_checkpoint_disabled {
            while let Err(LimboError::Busy) = self.blocking_checkpoint(
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
                sync_mode,
            ) {
                if attempts == 3 {
                    // don't return error on `close` if we are unable to checkpoint, we can silently fail
                    tracing::warn!(
                        "Failed to checkpoint WAL on shutdown after 3 attempts, giving up"
                    );
                    return Ok(());
                }
                attempts += 1;
            }
        }
        // TODO: delete the WAL file here after truncate checkpoint, but *only* if we are sure that
        // no other connections have opened since.
        Ok(())
    }

    /// Perform a blocking checkpoint with the specified mode.
    /// This is a convenience wrapper around `checkpoint()` that blocks until completion.
    /// Explicit checkpoints clear the page cache after completion.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn blocking_checkpoint(
        &self,
        mode: CheckpointMode,
        sync_mode: crate::SyncMode,
    ) -> Result<CheckpointResult> {
        self.io.block(|| self.checkpoint(mode, sync_mode, true))
    }

    pub fn freepage_list(&self) -> u32 {
        self.io
            .block(|| HeaderRefMut::from_pager(self))
            .map(|header_ref| header_ref.borrow_mut().freelist_pages.into())
            .unwrap_or(0)
    }
    // Providing a page is optional, if provided it will be used to avoid reading the page from disk.
    // This is implemented in accordance with sqlite freepage2() function.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn free_page(&self, mut page: Option<PageRef>, page_id: usize) -> Result<IOResult<()>> {
        tracing::trace!("free_page(page_id={})", page_id);
        const TRUNK_PAGE_HEADER_SIZE: usize = 8;
        const LEAF_ENTRY_SIZE: usize = 4;
        const RESERVED_SLOTS: usize = 2;

        const TRUNK_PAGE_NEXT_PAGE_OFFSET: usize = 0; // Offset to next trunk page pointer
        const TRUNK_PAGE_LEAF_COUNT_OFFSET: usize = 4; // Offset to leaf count

        let header_ref = self.io.block(|| HeaderRefMut::from_pager(self))?;
        let header = header_ref.borrow_mut();

        let mut state = self.free_page_state.write();
        tracing::debug!(?state);
        loop {
            match &mut *state {
                FreePageState::Start => {
                    if page_id < 2 || page_id > header.database_size.get() as usize {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid page number {page_id} for free operation"
                        )));
                    }

                    let (page, c) = match page.take() {
                        Some(page) => {
                            assert_eq!(
                                page.get().id,
                                page_id,
                                "Pager::free_page: Page id mismatch: expected {} but got {}",
                                page_id,
                                page.get().id
                            );
                            if page.is_loaded() {
                                let page_contents = page.get_contents();
                                page_contents.overflow_cells.clear();
                            }
                            (page, None)
                        }
                        None => self.read_page(page_id as i64)?,
                    };
                    header.freelist_pages = (header.freelist_pages.get() + 1).into();

                    let trunk_page_id = header.freelist_trunk_page.get();

                    // Pin page to prevent eviction while stored in state machine
                    page.pin();

                    if trunk_page_id != 0 {
                        *state = FreePageState::AddToTrunk { page };
                    } else {
                        *state = FreePageState::NewTrunk { page };
                    }
                    if let Some(c) = c {
                        if !c.succeeded() {
                            io_yield_one!(c);
                        }
                    }
                }
                FreePageState::AddToTrunk { page } => {
                    let trunk_page_id = header.freelist_trunk_page.get();
                    let (trunk_page, c) = self.read_page(trunk_page_id as i64)?;
                    if let Some(c) = c {
                        if !c.succeeded() {
                            io_yield_one!(c);
                        }
                    }
                    turso_assert!(trunk_page.is_loaded(), "trunk_page should be loaded");

                    let trunk_page_contents = trunk_page.get_contents();
                    let number_of_leaf_pages =
                        trunk_page_contents.read_u32_no_offset(TRUNK_PAGE_LEAF_COUNT_OFFSET);

                    // Reserve 2 slots for the trunk page header which is 8 bytes or 2*LEAF_ENTRY_SIZE
                    let max_free_list_entries =
                        (header.usable_space() / LEAF_ENTRY_SIZE) - RESERVED_SLOTS;

                    if number_of_leaf_pages < max_free_list_entries as u32 {
                        turso_assert!(
                            trunk_page.get().id == trunk_page_id as usize,
                            "trunk page has unexpected id"
                        );
                        self.add_dirty(&trunk_page)?;

                        trunk_page_contents.write_u32_no_offset(
                            TRUNK_PAGE_LEAF_COUNT_OFFSET,
                            number_of_leaf_pages + 1,
                        );
                        trunk_page_contents.write_u32_no_offset(
                            TRUNK_PAGE_HEADER_SIZE
                                + (number_of_leaf_pages as usize * LEAF_ENTRY_SIZE),
                            page_id as u32,
                        );

                        // Unpin page before finishing - it's added to freelist
                        page.unpin();
                        break;
                    }
                    // page remains pinned as it transitions to NewTrunk state
                    *state = FreePageState::NewTrunk { page: page.clone() };
                }
                FreePageState::NewTrunk { page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    // If we get here, need to make this page a new trunk
                    turso_assert!(page.get().id == page_id, "page has unexpected id");
                    self.add_dirty(page)?;

                    let trunk_page_id = header.freelist_trunk_page.get();

                    let contents = page.get().contents.as_mut().unwrap();
                    // Point to previous trunk
                    contents.write_u32_no_offset(TRUNK_PAGE_NEXT_PAGE_OFFSET, trunk_page_id);
                    // Zero leaf count
                    contents.write_u32_no_offset(TRUNK_PAGE_LEAF_COUNT_OFFSET, 0);
                    // Update page 1 to point to new trunk
                    header.freelist_trunk_page = (page_id as u32).into();
                    // Unpin page before finishing - it's now a trunk page
                    page.unpin();
                    break;
                }
            }
        }
        *state = FreePageState::Start;
        Ok(IOResult::Done(()))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn allocate_page1(&self) -> Result<IOResult<PageRef>> {
        let state = self.allocate_page1_state.read().clone();
        match state {
            AllocatePage1State::Start => {
                assert!(!self.db_initialized());
                tracing::trace!("allocate_page1(Start)");

                let IOResult::Done(mut default_header) = self.with_header(|header| *header)? else {
                    panic!("DB should not be initialized and should not do any IO");
                };

                assert_eq!(default_header.database_size.get(), 0);
                default_header.database_size = 1.into();

                // based on the IOContext set, we will set the reserved space bytes as required by
                // either the encryption or checksum, or None if they are not set.
                let reserved_space_bytes = {
                    let io_ctx = self.io_ctx.read();
                    io_ctx.get_reserved_space_bytes()
                };
                default_header.reserved_space = reserved_space_bytes;
                self.set_reserved_space(reserved_space_bytes);

                if let Some(size) = self.get_page_size() {
                    default_header.page_size = size;
                }

                tracing::debug!(
                    "allocate_page1(Start) page_size = {:?}, reserved_space = {}",
                    default_header.page_size,
                    default_header.reserved_space
                );

                self.buffer_pool
                    .finalize_with_page_size(default_header.page_size.get() as usize)?;
                let page = allocate_new_page(1, &self.buffer_pool, 0);

                let contents = page.get_contents();
                contents.write_database_header(&default_header);

                let page1 = page;
                // Create the sqlite_schema table, for this we just need to create the btree page
                // for the first page of the database which is basically like any other btree page
                // but with a 100 byte offset, so we just init the page so that sqlite understands
                // this is a correct page.
                btree_init_page(
                    &page1,
                    PageType::TableLeaf,
                    DatabaseHeader::SIZE,
                    (default_header.page_size.get() - default_header.reserved_space as u32)
                        as usize,
                );
                let c = begin_write_btree_page(self, &page1)?;

                // Pin page1 to prevent eviction while stored in state machine
                page1.pin();
                *self.allocate_page1_state.write() = AllocatePage1State::Writing { page: page1 };
                io_yield_one!(c);
            }
            AllocatePage1State::Writing { page } => {
                turso_assert!(page.is_loaded(), "page should be loaded");
                tracing::trace!("allocate_page1(Writing done)");
                let page_key = PageCacheKey::new(page.get().id);
                let mut cache = self.page_cache.write();
                cache.insert(page_key, page.clone()).map_err(|e| {
                    LimboError::InternalError(format!("Failed to insert page 1 into cache: {e:?}"))
                })?;
                // After we wrote the header page, we may now set this None, to signify we initialized
                self.init_page_1.store(None);
                page.unpin();
                *self.allocate_page1_state.write() = AllocatePage1State::Done;
                Ok(IOResult::Done(page))
            }
            AllocatePage1State::Done => unreachable!("cannot try to allocate page 1 again"),
        }
    }

    pub fn allocating_page1(&self) -> bool {
        matches!(
            *self.allocate_page1_state.read(),
            AllocatePage1State::Writing { .. }
        )
    }

    /// Tries to reuse a page from the freelist if available.
    /// If not, allocates a new page which increases the database size.
    ///
    /// FIXME: implement sqlite's 'nearby' parameter and use AllocMode.
    ///        SQLite's allocate_page() equivalent has a parameter 'nearby' which is a hint about the page number we want to have for the allocated page.
    ///        We should use this parameter to allocate the page in the same way as SQLite does; instead now we just either take the first available freelist page
    ///        or allocate a new page.
    #[allow(clippy::readonly_write_lock)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn allocate_page(&self) -> Result<IOResult<PageRef>> {
        const FREELIST_TRUNK_OFFSET_NEXT_TRUNK: usize = 0;
        const FREELIST_TRUNK_OFFSET_LEAF_COUNT: usize = 4;
        const FREELIST_TRUNK_OFFSET_FIRST_LEAF: usize = 8;

        // Ensure cache has room before allocating (we may spill dirty pages first)
        return_if_io!(self.ensure_cache_space());

        let header_ref = self.io.block(|| HeaderRefMut::from_pager(self))?;
        let header = header_ref.borrow_mut();

        loop {
            let mut state = self.allocate_page_state.write();
            tracing::debug!("allocate_page(state={:?})", state);
            match &mut *state {
                AllocatePageState::Start => {
                    let old_db_size = header.database_size.get();
                    #[cfg(not(feature = "omit_autovacuum"))]
                    let mut new_db_size = old_db_size;
                    #[cfg(feature = "omit_autovacuum")]
                    let new_db_size = old_db_size;

                    tracing::debug!("allocate_page(database_size={})", new_db_size);
                    #[cfg(not(feature = "omit_autovacuum"))]
                    {
                        //  If the following conditions are met, allocate a pointer map page, add to cache and increment the database size
                        //  - autovacuum is enabled
                        //  - the last page is a pointer map page
                        if matches!(
                            AutoVacuumMode::from(self.auto_vacuum_mode.load(Ordering::SeqCst)),
                            AutoVacuumMode::Full
                        ) && is_ptrmap_page(new_db_size + 1, header.page_size.get() as usize)
                        {
                            // we will allocate a ptrmap page, so increment size
                            new_db_size += 1;
                            let page = allocate_new_page(new_db_size as i64, &self.buffer_pool, 0);
                            self.add_dirty(&page)?;
                            let page_key = PageCacheKey::new(page.get().id as usize);
                            let mut cache = self.page_cache.write();
                            cache.insert(page_key, page)?;
                        }
                    }

                    let first_freelist_trunk_page_id = header.freelist_trunk_page.get();
                    if first_freelist_trunk_page_id == 0 {
                        *state = AllocatePageState::AllocateNewPage {
                            current_db_size: new_db_size,
                        };
                        continue;
                    }
                    let (trunk_page, c) = self.read_page(first_freelist_trunk_page_id as i64)?;
                    // Pin trunk_page to prevent eviction while stored in state machine
                    trunk_page.pin();
                    *state = AllocatePageState::SearchAvailableFreeListLeaf {
                        trunk_page,
                        current_db_size: new_db_size,
                    };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                AllocatePageState::SearchAvailableFreeListLeaf {
                    trunk_page,
                    current_db_size,
                } => {
                    turso_assert!(
                        trunk_page.is_loaded(),
                        "Freelist trunk page {} is not loaded",
                        trunk_page.get().id
                    );
                    let page_contents = trunk_page.get_contents();
                    let next_trunk_page_id =
                        page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_NEXT_TRUNK);
                    let number_of_freelist_leaves =
                        page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_LEAF_COUNT);

                    // There are leaf pointers on this trunk page, so we can reuse one of the pages
                    // for the allocation.
                    if number_of_freelist_leaves != 0 {
                        let page_contents = trunk_page.get_contents();
                        let next_leaf_page_id =
                            page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_FIRST_LEAF);
                        let (leaf_page, c) = self.read_page(next_leaf_page_id as i64)?;

                        turso_assert!(
                            number_of_freelist_leaves > 0,
                            "Freelist trunk page {} has no leaves",
                            trunk_page.get().id
                        );

                        // Pin leaf_page to prevent eviction while stored in state machine
                        // trunk_page is already pinned from previous state
                        leaf_page.pin();

                        *state = AllocatePageState::ReuseFreelistLeaf {
                            trunk_page: trunk_page.clone(),
                            leaf_page,
                            number_of_freelist_leaves,
                        };
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                        continue;
                    }

                    // No freelist leaves on this trunk page.
                    // If the freelist is completely empty, allocate a new page.
                    if next_trunk_page_id == 0 {
                        // Unpin trunk_page since we're transitioning away
                        trunk_page.unpin();
                        *state = AllocatePageState::AllocateNewPage {
                            current_db_size: *current_db_size,
                        };
                        continue;
                    }

                    // Freelist is not empty, so we can reuse the trunk itself as a new page
                    // and update the database's first freelist trunk page to the next trunk page.
                    header.freelist_trunk_page = next_trunk_page_id.into();
                    header.freelist_pages = (header.freelist_pages.get() - 1).into();
                    self.add_dirty(trunk_page)?;
                    // zero out the page
                    turso_assert!(
                        trunk_page.get_contents().overflow_cells.is_empty(),
                        "Freelist leaf page {} has overflow cells",
                        trunk_page.get().id
                    );
                    trunk_page.get_contents().as_ptr().fill(0);
                    let page_key = PageCacheKey::new(trunk_page.get().id);
                    {
                        let page_cache = self.page_cache.read();
                        turso_assert!(
                            page_cache.contains_key(&page_key),
                            "page {} is not in cache",
                            trunk_page.get().id
                        );
                    }
                    // Unpin trunk_page before returning - caller takes ownership
                    trunk_page.unpin();
                    let trunk_page = trunk_page.clone();
                    *state = AllocatePageState::Start;
                    return Ok(IOResult::Done(trunk_page));
                }
                AllocatePageState::ReuseFreelistLeaf {
                    trunk_page,
                    leaf_page,
                    number_of_freelist_leaves,
                } => {
                    turso_assert!(
                        leaf_page.is_loaded(),
                        "Leaf page {} is not loaded",
                        leaf_page.get().id
                    );
                    let page_contents = trunk_page.get_contents();
                    self.add_dirty(leaf_page)?;
                    // zero out the page
                    turso_assert!(
                        leaf_page.get_contents().overflow_cells.is_empty(),
                        "Freelist leaf page {} has overflow cells",
                        leaf_page.get().id
                    );
                    leaf_page.get_contents().as_ptr().fill(0);
                    let page_key = PageCacheKey::new(leaf_page.get().id);
                    {
                        let page_cache = self.page_cache.read();
                        turso_assert!(
                            page_cache.contains_key(&page_key),
                            "page {} is not in cache",
                            leaf_page.get().id
                        );
                    }

                    // Mark trunk page dirty BEFORE modifying it so subjournal captures original content
                    self.add_dirty(trunk_page)?;

                    // Shift left all the other leaf pages in the trunk page and subtract 1 from the leaf count
                    let remaining_leaves_count = (*number_of_freelist_leaves - 1) as usize;
                    {
                        let buf = page_contents.as_ptr();
                        // use copy within the same page
                        const LEAF_PTR_SIZE_BYTES: usize = 4;
                        let offset_remaining_leaves_start =
                            FREELIST_TRUNK_OFFSET_FIRST_LEAF + LEAF_PTR_SIZE_BYTES;
                        let offset_remaining_leaves_end = offset_remaining_leaves_start
                            + remaining_leaves_count * LEAF_PTR_SIZE_BYTES;
                        buf.copy_within(
                            offset_remaining_leaves_start..offset_remaining_leaves_end,
                            FREELIST_TRUNK_OFFSET_FIRST_LEAF,
                        );
                    }
                    // write the new leaf count
                    page_contents.write_u32_no_offset(
                        FREELIST_TRUNK_OFFSET_LEAF_COUNT,
                        remaining_leaves_count as u32,
                    );

                    header.freelist_pages = (header.freelist_pages.get() - 1).into();
                    // Unpin both pages before returning - caller takes ownership of leaf_page
                    trunk_page.unpin();
                    leaf_page.unpin();
                    let leaf_page = leaf_page.clone();
                    *state = AllocatePageState::Start;
                    return Ok(IOResult::Done(leaf_page));
                }
                AllocatePageState::AllocateNewPage { current_db_size } => {
                    let mut new_db_size = *current_db_size + 1;

                    // if new_db_size reaches the pending page, we need to allocate a new one
                    if Some(new_db_size) == self.pending_byte_page_id() {
                        let richard_hipp_special_page =
                            allocate_new_page(new_db_size as i64, &self.buffer_pool, 0);
                        self.add_dirty(&richard_hipp_special_page)?;
                        let page_key = PageCacheKey::new(richard_hipp_special_page.get().id);
                        {
                            let mut cache = self.page_cache.write();
                            cache.insert(page_key, richard_hipp_special_page).unwrap();
                        }
                        // HIPP special page is assumed to zeroed and should never be read or written to by the BTREE
                        new_db_size += 1;
                    }

                    // Check if allocating a new page would exceed the maximum page count
                    let max_page_count = self.get_max_page_count();
                    if new_db_size > max_page_count {
                        return Err(LimboError::DatabaseFull(
                            "database or disk is full".to_string(),
                        ));
                    }

                    // FIXME: should reserve page cache entry before modifying the database
                    let page = allocate_new_page(new_db_size as i64, &self.buffer_pool, 0);
                    {
                        // setup page and add to cache
                        self.add_dirty(&page)?;

                        let page_key = PageCacheKey::new(page.get().id as usize);
                        {
                            // Run in separate block to avoid deadlock on page cache write lock
                            let mut cache = self.page_cache.write();
                            cache.insert(page_key, page.clone())?;
                        }
                        header.database_size = new_db_size.into();
                        *state = AllocatePageState::Start;
                        return Ok(IOResult::Done(page));
                    }
                }
            }
        }
    }

    pub fn upsert_page_in_cache(
        &self,
        id: usize,
        page: PageRef,
        dirty_page_must_exist: bool,
    ) -> Result<(), LimboError> {
        let mut cache = self.page_cache.write();
        let page_key = PageCacheKey::new(id);

        // FIXME: use specific page key for writer instead of max frame, this will make readers not conflict
        if dirty_page_must_exist {
            assert!(page.is_dirty());
        }
        cache.upsert_page(page_key, page.clone()).map_err(|e| {
            LimboError::InternalError(format!(
                "Failed to insert loaded page {id} into cache: {e:?}"
            ))
        })?;
        page.set_loaded();
        page.clear_wal_tag();
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn rollback(&self, schema_did_change: bool, connection: &Connection, is_write: bool) {
        tracing::debug!(schema_did_change);
        self.clear_page_cache(is_write);
        if is_write {
            self.dirty_pages.write().clear();
        } else {
            turso_assert!(
                self.dirty_pages.read().is_empty(),
                "dirty pages should be empty for read txn"
            );
        }
        self.reset_internal_states();
        // Invalidate cached schema cookie since rollback may have restored the database schema cookie
        self.set_schema_cookie(None);
        if schema_did_change {
            *connection.schema.write() = connection.db.clone_schema();
        }
        if is_write {
            if let Some(wal) = self.wal.as_ref() {
                wal.rollback();
            }
        }
    }

    fn reset_internal_states(&self) {
        *self.checkpoint_state.write() = CheckpointState::default();
        self.syncing.store(false, Ordering::SeqCst);
        self.commit_info.write().reset();
        *self.allocate_page_state.write() = AllocatePageState::Start;
        *self.free_page_state.write() = FreePageState::Start;
        *self.spill_state.write() = SpillState::Idle;
        #[cfg(not(feature = "omit_autovacuum"))]
        {
            let mut vacuum_state = self.vacuum_state.write();
            vacuum_state.ptrmap_get_state = PtrMapGetState::Start;
            vacuum_state.ptrmap_put_state = PtrMapPutState::Start;
            vacuum_state.btree_create_vacuum_full_state = BtreeCreateVacuumFullState::Start;
        }

        *self.header_ref_state.write() = HeaderRefState::Start;
    }

    pub fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> Result<IOResult<T>> {
        let header_ref = return_if_io!(HeaderRef::from_pager(self));
        let header = header_ref.borrow();
        // Update cached schema cookie when reading header
        self.set_schema_cookie(Some(header.schema_cookie.get()));
        Ok(IOResult::Done(f(header)))
    }

    pub fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<IOResult<T>> {
        let header_ref = return_if_io!(HeaderRefMut::from_pager(self));
        let header = header_ref.borrow_mut();
        let result = f(header);
        // Update cached schema cookie after modification
        self.set_schema_cookie(Some(header.schema_cookie.get()));
        Ok(IOResult::Done(result))
    }

    pub fn is_encryption_ctx_set(&self) -> bool {
        self.io_ctx.write().encryption_context().is_some()
    }

    pub fn set_encryption_context(
        &self,
        cipher_mode: CipherMode,
        key: &EncryptionKey,
    ) -> Result<()> {
        // we will set the encryption context only if the encryption is opted-in.
        if !self.enable_encryption.load(Ordering::SeqCst) {
            return Err(LimboError::InvalidArgument(
                "encryption is an opt in feature. enable it via passing `--experimental-encryption`"
                    .into(),
            ));
        }

        let page_size = self.get_page_size_unchecked().get() as usize;
        let encryption_ctx = EncryptionContext::new(cipher_mode, key, page_size)?;
        {
            let mut io_ctx = self.io_ctx.write();
            io_ctx.set_encryption(encryption_ctx);
        }
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        wal.set_io_context(self.io_ctx.read().clone());
        // whenever we set the encryption context, lets reset the page cache. The page cache
        // might have been loaded with page 1 to initialise the connection. During initialisation,
        // we only read the header which is unencrypted, but the rest of the page is. If so, lets
        // clear the cache.
        self.clear_page_cache(false);
        Ok(())
    }

    pub fn reset_checksum_context(&self) {
        {
            let mut io_ctx = self.io_ctx.write();
            io_ctx.reset_checksum();
        }
        let Some(wal) = self.wal.as_ref() else { return };
        wal.set_io_context(self.io_ctx.read().clone())
    }

    pub fn set_reserved_space_bytes(&self, value: u8) {
        self.set_reserved_space(value);
    }

    /// Encryption is an opt-in feature. If the flag is passed, then enable the encryption on
    /// pager, which is then used to set it on the IOContext.
    pub fn enable_encryption(&self, enable: bool) {
        self.enable_encryption.store(enable, Ordering::SeqCst);
    }
}

pub fn allocate_new_page(page_id: i64, buffer_pool: &Arc<BufferPool>, offset: usize) -> PageRef {
    let page = Arc::new(Page::new(page_id));
    {
        let buffer = buffer_pool.get_page();
        let buffer = Arc::new(buffer);
        page.set_loaded();
        page.clear_wal_tag();
        page.get().contents = Some(PageContent::new(offset, buffer));
    }
    page
}

pub fn default_page1(cipher: Option<&CipherMode>) -> PageRef {
    // New Database header for empty Database
    let mut default_header = DatabaseHeader::default();

    if let Some(cipher) = cipher {
        // we will set the reserved space bytes as required by either the encryption
        let reserved_space_bytes = cipher.metadata_size() as u8;
        default_header.reserved_space = reserved_space_bytes;
    }

    let page = Arc::new(Page::new(DatabaseHeader::PAGE_ID as i64));

    let contents = PageContent::new(
        0,
        Arc::new(Buffer::new_temporary(
            default_header.page_size.get() as usize
        )),
    );

    contents.write_database_header(&default_header);
    page.set_loaded();
    page.clear_wal_tag();
    page.get().contents.replace(contents);

    btree_init_page(
        &page,
        PageType::TableLeaf,
        DatabaseHeader::SIZE, // offset of 100 bytes
        (default_header.page_size.get() - default_header.reserved_space as u32) as usize,
    );

    page
}

#[derive(Debug)]
pub struct CreateBTreeFlags(pub u8);
impl CreateBTreeFlags {
    pub const TABLE: u8 = 0b0001;
    pub const INDEX: u8 = 0b0010;

    pub fn new_table() -> Self {
        Self(CreateBTreeFlags::TABLE)
    }

    pub fn new_index() -> Self {
        Self(CreateBTreeFlags::INDEX)
    }

    pub fn is_table(&self) -> bool {
        (self.0 & CreateBTreeFlags::TABLE) != 0
    }

    pub fn is_index(&self) -> bool {
        (self.0 & CreateBTreeFlags::INDEX) != 0
    }

    pub fn get_flags(&self) -> u8 {
        self.0
    }
}

/*
** The pointer map is a lookup table that identifies the parent page for
** each child page in the database file.  The parent page is the page that
** contains a pointer to the child.  Every page in the database contains
** 0 or 1 parent pages. Each pointer map entry consists of a single byte 'type'
** and a 4 byte parent page number.
**
** The PTRMAP_XXX identifiers below are the valid types.
**
** The purpose of the pointer map is to facilitate moving pages from one
** position in the file to another as part of autovacuum.  When a page
** is moved, the pointer in its parent must be updated to point to the
** new location.  The pointer map is used to locate the parent page quickly.
**
** PTRMAP_ROOTPAGE: The database page is a root-page. The page-number is not
**                  used in this case.
**
** PTRMAP_FREEPAGE: The database page is an unused (free) page. The page-number
**                  is not used in this case.
**
** PTRMAP_OVERFLOW1: The database page is the first page in a list of
**                   overflow pages. The page number identifies the page that
**                   contains the cell with a pointer to this overflow page.
**
** PTRMAP_OVERFLOW2: The database page is the second or later page in a list of
**                   overflow pages. The page-number identifies the previous
**                   page in the overflow page list.
**
** PTRMAP_BTREE: The database page is a non-root btree page. The page number
**               identifies the parent page in the btree.
*/
#[cfg(not(feature = "omit_autovacuum"))]
pub(crate) mod ptrmap {
    use crate::{storage::sqlite3_ondisk::PageSize, LimboError, Result};

    // Constants
    pub const PTRMAP_ENTRY_SIZE: usize = 5;
    /// Page 1 is the schema page which contains the database header.
    /// Page 2 is the first pointer map page if the database has any pointer map pages.
    pub const FIRST_PTRMAP_PAGE_NO: u32 = 2;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u8)]
    pub enum PtrmapType {
        RootPage = 1,
        FreePage = 2,
        Overflow1 = 3,
        Overflow2 = 4,
        BTreeNode = 5,
    }

    impl PtrmapType {
        pub fn from_u8(value: u8) -> Option<Self> {
            match value {
                1 => Some(PtrmapType::RootPage),
                2 => Some(PtrmapType::FreePage),
                3 => Some(PtrmapType::Overflow1),
                4 => Some(PtrmapType::Overflow2),
                5 => Some(PtrmapType::BTreeNode),
                _ => None,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub struct PtrmapEntry {
        pub entry_type: PtrmapType,
        pub parent_page_no: u32,
    }

    impl PtrmapEntry {
        pub fn serialize(&self, buffer: &mut [u8]) -> Result<()> {
            if buffer.len() < PTRMAP_ENTRY_SIZE {
                return Err(LimboError::InternalError(format!(
                    "Buffer too small to serialize ptrmap entry. Expected at least {} bytes, got {}",
                    PTRMAP_ENTRY_SIZE,
                    buffer.len()
                )));
            }
            buffer[0] = self.entry_type as u8;
            buffer[1..5].copy_from_slice(&self.parent_page_no.to_be_bytes());
            Ok(())
        }

        pub fn deserialize(buffer: &[u8]) -> Option<Self> {
            if buffer.len() < PTRMAP_ENTRY_SIZE {
                return None;
            }
            let entry_type_u8 = buffer[0];
            let parent_bytes_slice = buffer.get(1..5)?;
            let parent_page_no = u32::from_be_bytes(parent_bytes_slice.try_into().ok()?);
            PtrmapType::from_u8(entry_type_u8).map(|entry_type| PtrmapEntry {
                entry_type,
                parent_page_no,
            })
        }
    }

    /// Calculates how many database pages are mapped by a single pointer map page.
    /// This is based on the total page size, as ptrmap pages are filled with entries.
    pub fn entries_per_ptrmap_page(page_size: usize) -> usize {
        assert!(page_size >= PageSize::MIN as usize);
        page_size / PTRMAP_ENTRY_SIZE
    }

    /// Calculates the cycle length of pointer map pages
    /// The cycle length is the number of database pages that are mapped by a single pointer map page.
    pub fn ptrmap_page_cycle_length(page_size: usize) -> usize {
        assert!(page_size >= PageSize::MIN as usize);
        (page_size / PTRMAP_ENTRY_SIZE) + 1
    }

    /// Determines if a given page number `db_page_no` (1-indexed) is a pointer map page in a database with autovacuum enabled
    pub fn is_ptrmap_page(db_page_no: u32, page_size: usize) -> bool {
        //  The first page cannot be a ptrmap page because its for the schema
        if db_page_no == 1 {
            return false;
        }
        if db_page_no == FIRST_PTRMAP_PAGE_NO {
            return true;
        }
        get_ptrmap_page_no_for_db_page(db_page_no, page_size) == db_page_no
    }

    /// Calculates which pointer map page (1-indexed) contains the entry for `db_page_no_to_query` (1-indexed).
    /// `db_page_no_to_query` is the page whose ptrmap entry we are interested in.
    pub fn get_ptrmap_page_no_for_db_page(db_page_no_to_query: u32, page_size: usize) -> u32 {
        let group_size = ptrmap_page_cycle_length(page_size) as u32;
        if group_size == 0 {
            panic!("Page size too small, a ptrmap page cannot map any db pages.");
        }

        let effective_page_index = db_page_no_to_query - FIRST_PTRMAP_PAGE_NO;
        let group_idx = effective_page_index / group_size;

        (group_idx * group_size) + FIRST_PTRMAP_PAGE_NO
    }

    /// Calculates the byte offset of the entry for `db_page_no_to_query` (1-indexed)
    /// within its pointer map page (`ptrmap_page_no`, 1-indexed).
    pub fn get_ptrmap_offset_in_page(
        db_page_no_to_query: u32,
        ptrmap_page_no: u32,
        page_size: usize,
    ) -> Result<usize> {
        // The data pages mapped by `ptrmap_page_no` are:
        // `ptrmap_page_no + 1`, `ptrmap_page_no + 2`, ..., up to `ptrmap_page_no + n_data_pages_per_group`.
        // `db_page_no_to_query` must be one of these.
        // The 0-indexed position of `db_page_no_to_query` within this sequence of data pages is:
        // `db_page_no_to_query - (ptrmap_page_no + 1)`.

        let n_data_pages_per_group = entries_per_ptrmap_page(page_size);
        let first_data_page_mapped = ptrmap_page_no + 1;
        let last_data_page_mapped = ptrmap_page_no + n_data_pages_per_group as u32;

        if db_page_no_to_query < first_data_page_mapped
            || db_page_no_to_query > last_data_page_mapped
        {
            return Err(LimboError::InternalError(format!(
                "Page {db_page_no_to_query} is not mapped by the data page range [{first_data_page_mapped}, {last_data_page_mapped}] of ptrmap page {ptrmap_page_no}"
            )));
        }
        if is_ptrmap_page(db_page_no_to_query, page_size) {
            return Err(LimboError::InternalError(format!(
                "Page {db_page_no_to_query} is a pointer map page and should not have an entry calculated this way."
            )));
        }

        let entry_index_on_page = (db_page_no_to_query - first_data_page_mapped) as usize;
        Ok(entry_index_on_page * PTRMAP_ENTRY_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use crate::storage::page_cache::{PageCache, PageCacheKey};

    use super::Page;

    #[test]
    fn test_shared_cache() {
        // ensure cache can be shared between threads
        let cache = Arc::new(RwLock::new(PageCache::new(10)));

        let thread = {
            let cache = cache.clone();
            std::thread::spawn(move || {
                let mut cache = cache.write();
                let page_key = PageCacheKey::new(1);
                let page = Page::new(1);
                // Set loaded so that we avoid eviction, as we evict the page from cache if it is not locked and not loaded
                page.set_loaded();
                cache.insert(page_key, Arc::new(page)).unwrap();
            })
        };
        let _ = thread.join();
        let mut cache = cache.write();
        let page_key = PageCacheKey::new(1);
        let page = cache.get(&page_key).unwrap();
        assert_eq!(page.unwrap().get().id, 1);
    }
}

#[cfg(test)]
#[cfg(not(feature = "omit_autovacuum"))]
mod ptrmap_tests {
    use std::sync::Arc;

    use super::ptrmap::*;
    use super::*;
    use crate::io::{MemoryIO, OpenFlags, IO};
    use crate::storage::buffer_pool::BufferPool;
    use crate::storage::database::DatabaseFile;
    use crate::storage::page_cache::PageCache;
    use crate::storage::pager::Pager;
    use crate::storage::sqlite3_ondisk::PageSize;
    use crate::storage::wal::{WalFile, WalFileShared};

    pub fn run_until_done<T>(
        mut action: impl FnMut() -> Result<IOResult<T>>,
        pager: &Pager,
    ) -> Result<T> {
        loop {
            match action()? {
                IOResult::Done(res) => {
                    return Ok(res);
                }
                IOResult::IO(io) => io.wait(pager.io.as_ref())?,
            }
        }
    }
    // Helper to create a Pager for testing
    fn test_pager_setup(page_size: u32, initial_db_pages: u32) -> Pager {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(
            io.open_file("test.db", OpenFlags::Create, true).unwrap(),
        ));

        //  Construct interfaces for the pager
        let pages = initial_db_pages + 10;
        let sz = std::cmp::max(std::cmp::min(pages, 64), pages);
        let buffer_pool = BufferPool::begin_init(&io, (sz * page_size) as usize);
        let page_cache = Arc::new(RwLock::new(PageCache::new(sz as usize)));

        let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
            io.clone(),
            WalFileShared::new_shared(
                io.open_file("test.db-wal", OpenFlags::Create, false)
                    .unwrap(),
            )
            .unwrap(),
            buffer_pool.clone(),
        ));

        let pager = Pager::new(
            db_file,
            Some(wal),
            io,
            page_cache,
            buffer_pool,
            Arc::new(Mutex::new(())),
            Default::default(),
        )
        .unwrap();
        run_until_done(|| pager.allocate_page1(), &pager).unwrap();
        {
            let page_cache = pager.page_cache.read();
            println!(
                "Cache Len: {} Cap: {}",
                page_cache.len(),
                page_cache.capacity()
            );
        }
        pager
            .io
            .block(|| {
                pager.with_header_mut(|header| header.vacuum_mode_largest_root_page = 1.into())
            })
            .unwrap();
        pager.set_auto_vacuum_mode(AutoVacuumMode::Full);

        //  Allocate all the pages as btree root pages
        const EXPECTED_FIRST_ROOT_PAGE_ID: u32 = 3; // page1 = 1,  first ptrmap page = 2, root page = 3
        for i in 0..initial_db_pages {
            let res = run_until_done(
                || pager.btree_create(&CreateBTreeFlags::new_table()),
                &pager,
            );
            {
                let page_cache = pager.page_cache.read();
                println!(
                    "i: {} Cache Len: {} Cap: {}",
                    i,
                    page_cache.len(),
                    page_cache.capacity()
                );
            }
            match res {
                Ok(root_page_id) => {
                    assert_eq!(root_page_id, EXPECTED_FIRST_ROOT_PAGE_ID + i);
                }
                Err(e) => {
                    panic!("test_pager_setup: btree_create failed: {e:?}");
                }
            }
        }

        pager
    }

    #[test]
    fn test_ptrmap_page_allocation() {
        let page_size = 4096;
        let initial_db_pages = 10;
        let pager = test_pager_setup(page_size, initial_db_pages);

        // Page 5 should be mapped by ptrmap page 2.
        let db_page_to_update: u32 = 5;
        let expected_ptrmap_pg_no =
            get_ptrmap_page_no_for_db_page(db_page_to_update, page_size as usize);
        assert_eq!(expected_ptrmap_pg_no, FIRST_PTRMAP_PAGE_NO);

        //  Ensure the pointer map page ref is created and loadable via the pager
        let ptrmap_page_ref = pager.read_page(expected_ptrmap_pg_no as i64);
        assert!(ptrmap_page_ref.is_ok());

        //  Ensure that the database header size is correctly reflected
        assert_eq!(
            pager
                .io
                .block(|| pager.with_header(|header| header.database_size))
                .unwrap()
                .get(),
            initial_db_pages + 2
        ); // (1+1) -> (header + ptrmap)

        //  Read the entry from the ptrmap page and verify it
        let entry = pager
            .io
            .block(|| pager.ptrmap_get(db_page_to_update))
            .unwrap()
            .unwrap();
        assert_eq!(entry.entry_type, PtrmapType::RootPage);
        assert_eq!(entry.parent_page_no, 0);
    }

    #[test]
    fn test_is_ptrmap_page_logic() {
        let page_size = PageSize::MIN as usize;
        let n_data_pages = entries_per_ptrmap_page(page_size);
        assert_eq!(n_data_pages, 102); //   512/5 = 102

        assert!(!is_ptrmap_page(1, page_size)); // Header
        assert!(is_ptrmap_page(2, page_size)); // P0
        assert!(!is_ptrmap_page(3, page_size)); // D0_1
        assert!(!is_ptrmap_page(4, page_size)); // D0_2
        assert!(!is_ptrmap_page(5, page_size)); // D0_3
        assert!(is_ptrmap_page(105, page_size)); // P1
        assert!(!is_ptrmap_page(106, page_size)); // D1_1
        assert!(!is_ptrmap_page(107, page_size)); // D1_2
        assert!(!is_ptrmap_page(108, page_size)); // D1_3
        assert!(is_ptrmap_page(208, page_size)); // P2
    }

    #[test]
    fn test_get_ptrmap_page_no() {
        let page_size = PageSize::MIN as usize; // Maps 103 data pages

        // Test pages mapped by P0 (page 2)
        assert_eq!(get_ptrmap_page_no_for_db_page(3, page_size), 2); // D(3) -> P0(2)
        assert_eq!(get_ptrmap_page_no_for_db_page(4, page_size), 2); // D(4) -> P0(2)
        assert_eq!(get_ptrmap_page_no_for_db_page(5, page_size), 2); // D(5) -> P0(2)
        assert_eq!(get_ptrmap_page_no_for_db_page(104, page_size), 2); // D(104) -> P0(2)

        assert_eq!(get_ptrmap_page_no_for_db_page(105, page_size), 105); // Page 105 is a pointer map page.

        // Test pages mapped by P1 (page 6)
        assert_eq!(get_ptrmap_page_no_for_db_page(106, page_size), 105); // D(106) -> P1(105)
        assert_eq!(get_ptrmap_page_no_for_db_page(107, page_size), 105); // D(107) -> P1(105)
        assert_eq!(get_ptrmap_page_no_for_db_page(108, page_size), 105); // D(108) -> P1(105)

        assert_eq!(get_ptrmap_page_no_for_db_page(208, page_size), 208); // Page 208 is a pointer map page.
    }

    #[test]
    fn test_get_ptrmap_offset() {
        let page_size = PageSize::MIN as usize; //  Maps 103 data pages

        assert_eq!(get_ptrmap_offset_in_page(3, 2, page_size).unwrap(), 0);
        assert_eq!(
            get_ptrmap_offset_in_page(4, 2, page_size).unwrap(),
            PTRMAP_ENTRY_SIZE
        );
        assert_eq!(
            get_ptrmap_offset_in_page(5, 2, page_size).unwrap(),
            2 * PTRMAP_ENTRY_SIZE
        );

        //  P1 (page 105) maps D(106)...D(207)
        // D(106) is index 0 on P1. Offset 0.
        // D(107) is index 1 on P1. Offset 5.
        // D(108) is index 2 on P1. Offset 10.
        assert_eq!(get_ptrmap_offset_in_page(106, 105, page_size).unwrap(), 0);
        assert_eq!(
            get_ptrmap_offset_in_page(107, 105, page_size).unwrap(),
            PTRMAP_ENTRY_SIZE
        );
        assert_eq!(
            get_ptrmap_offset_in_page(108, 105, page_size).unwrap(),
            2 * PTRMAP_ENTRY_SIZE
        );
    }
}
