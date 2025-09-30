use crate::storage::wal::IOV_MAX;
use crate::storage::{
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    sqlite3_ondisk::{
        self, parse_wal_frame_header, DatabaseHeader, PageContent, PageSize, PageType,
    },
    wal::{CheckpointResult, Wal},
};
use crate::types::{IOCompletions, WalState};
use crate::util::IOExt as _;
use crate::{io_yield_many, io_yield_one, IOContext};
use crate::{
    return_if_io, turso_assert, types::WalFrameInfo, Completion, Connection, IOResult, LimboError,
    Result, TransactionState,
};
use parking_lot::RwLock;
use std::cell::{RefCell, UnsafeCell};
use std::collections::HashSet;
use std::hash;
use std::rc::Rc;
use std::sync::atomic::{
    AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::{Arc, Mutex};
use tracing::{instrument, trace, Level};

use super::btree::btree_init_page;
use super::page_cache::{CacheError, CacheResizeResult, PageCache, PageCacheKey};
use super::sqlite3_ondisk::begin_write_btree_page;
use super::wal::CheckpointMode;
use crate::storage::encryption::{CipherMode, EncryptionContext, EncryptionKey};

/// SQLite's default maximum page count
const DEFAULT_MAX_PAGE_COUNT: u32 = 0xfffffffe;
const RESERVED_SPACE_NOT_SET: u16 = u16::MAX;

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
                    if !pager.db_state.is_initialized() {
                        return Err(LimboError::Page1NotAlloc);
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
                    if !pager.db_state.is_initialized() {
                        return Err(LimboError::Page1NotAlloc);
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

                    pager.add_dirty(&page);
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
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        tracing::debug!("clear_dirty(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::Release);
        self.clear_wal_tag();
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
    pub fn is_valid_for_checkpoint(&self, target_frame: u64, epoch: u32) -> bool {
        let (f, s) = self.wal_tag_pair();
        f == target_frame && s == epoch && !self.is_dirty() && self.is_loaded() && !self.is_locked()
    }
}

#[derive(Clone, Copy, Debug)]
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
    /// Fsync the on-disk WAL.
    SyncWal,
    /// Checkpoint the WAL to the database file (if needed).
    Checkpoint,
    /// Fsync the database file.
    SyncDbFile,
    Done,
}

#[derive(Clone, Debug, Default)]
enum CheckpointState {
    #[default]
    Checkpoint,
    SyncDbFile {
        res: CheckpointResult,
    },
    CheckpointDone {
        res: CheckpointResult,
    },
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
    result: Option<PagerCommitResult>,
    state: CommitState,
    time: crate::io::clock::Instant,
}

/// Track the state of the auto-vacuum mode.
#[derive(Clone, Copy, Debug)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum DbState {
    Uninitialized = Self::UNINITIALIZED,
    Initializing = Self::INITIALIZING,
    Initialized = Self::INITIALIZED,
}

impl DbState {
    pub(self) const UNINITIALIZED: usize = 0;
    pub(self) const INITIALIZING: usize = 1;
    pub(self) const INITIALIZED: usize = 2;

    #[inline]
    pub fn is_initialized(&self) -> bool {
        matches!(self, DbState::Initialized)
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct AtomicDbState(AtomicUsize);

impl AtomicDbState {
    #[inline]
    pub const fn new(state: DbState) -> Self {
        Self(AtomicUsize::new(state as usize))
    }

    #[inline]
    pub fn set(&self, state: DbState) {
        self.0.store(state as usize, Ordering::Release);
    }

    #[inline]
    pub fn get(&self) -> DbState {
        let v = self.0.load(Ordering::Acquire);
        match v {
            DbState::UNINITIALIZED => DbState::Uninitialized,
            DbState::INITIALIZING => DbState::Initializing,
            DbState::INITIALIZED => DbState::Initialized,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.get().is_initialized()
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

/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    /// Source of the database pages.
    pub db_file: Arc<dyn DatabaseStorage>,
    /// The write-ahead log (WAL) for the database.
    /// in-memory databases, ephemeral tables and ephemeral indexes do not have a WAL.
    pub(crate) wal: Option<Rc<RefCell<dyn Wal>>>,
    /// A page cache for the database.
    page_cache: Arc<RwLock<PageCache>>,
    /// Buffer pool for temporary data storage.
    pub buffer_pool: Arc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    dirty_pages: Arc<RwLock<HashSet<usize, hash::BuildHasherDefault<hash::DefaultHasher>>>>,

    commit_info: RwLock<CommitInfo>,
    checkpoint_state: RwLock<CheckpointState>,
    syncing: Arc<AtomicBool>,
    auto_vacuum_mode: AtomicU8,
    /// 0 -> Database is empty,
    /// 1 -> Database is being initialized,
    /// 2 -> Database is initialized and ready for use.
    pub db_state: Arc<AtomicDbState>,
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
    free_page_state: RwLock<FreePageState>,
    /// Maximum number of pages allowed in the database. Default is 1073741823 (SQLite default).
    max_page_count: AtomicU32,
    header_ref_state: RwLock<HeaderRefState>,
    #[cfg(not(feature = "omit_autovacuum"))]
    vacuum_state: RwLock<VacuumState>,
    pub(crate) io_ctx: RwLock<IOContext>,
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

impl Pager {
    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Option<Rc<RefCell<dyn Wal>>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<PageCache>>,
        buffer_pool: Arc<BufferPool>,
        db_state: Arc<AtomicDbState>,
        init_lock: Arc<Mutex<()>>,
    ) -> Result<Self> {
        let allocate_page1_state = if !db_state.is_initialized() {
            RwLock::new(AllocatePage1State::Start)
        } else {
            RwLock::new(AllocatePage1State::Done)
        };
        let now = io.now();
        Ok(Self {
            db_file,
            wal,
            page_cache,
            io,
            dirty_pages: Arc::new(RwLock::new(HashSet::with_hasher(
                hash::BuildHasherDefault::new(),
            ))),
            commit_info: RwLock::new(CommitInfo {
                result: None,
                completions: Vec::new(),
                state: CommitState::PrepareWal,
                time: now,
            }),
            syncing: Arc::new(AtomicBool::new(false)),
            checkpoint_state: RwLock::new(CheckpointState::Checkpoint),
            buffer_pool,
            auto_vacuum_mode: AtomicU8::new(AutoVacuumMode::None.into()),
            db_state,
            init_lock,
            allocate_page1_state,
            page_size: AtomicU32::new(0), // 0 means not set
            reserved_space: AtomicU16::new(RESERVED_SPACE_NOT_SET),
            free_page_state: RwLock::new(FreePageState::Start),
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
        })
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

    pub fn set_wal(&mut self, wal: Rc<RefCell<dyn Wal>>) {
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
                    self.add_dirty(&ptrmap_page);
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

    /// Set the initial page size for the database. Should only be called before the database is initialized
    pub fn set_initial_page_size(&self, size: PageSize) {
        assert_eq!(self.db_state.get(), DbState::Uninitialized);
        self.page_size.store(size.get(), Ordering::SeqCst);
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

    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_read_tx(&self) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        let changed = wal.borrow_mut().begin_read_tx()?;
        if changed {
            // Someone else changed the database -> assume our page cache is invalid (this is default SQLite behavior, we can probably do better with more granular invalidation)
            self.clear_page_cache();
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn maybe_allocate_page1(&self) -> Result<IOResult<()>> {
        if !self.db_state.is_initialized() {
            if let Ok(_lock) = self.init_lock.try_lock() {
                match (self.db_state.get(), self.allocating_page1()) {
                    // In case of being empty or (allocating and this connection is performing allocation) then allocate the first page
                    (DbState::Uninitialized, false) | (DbState::Initializing, true) => {
                        if let IOResult::IO(c) = self.allocate_page1()? {
                            return Ok(IOResult::IO(c));
                        } else {
                            return Ok(IOResult::Done(()));
                        }
                    }
                    // Give a chance for the allocation to happen elsewhere
                    _ => {}
                }
            } else {
                // Give a chance for the allocation to happen elsewhere
                io_yield_one!(Completion::new_dummy());
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
        Ok(IOResult::Done(wal.borrow_mut().begin_write_tx()?))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn end_tx(
        &self,
        rollback: bool,
        connection: &Connection,
    ) -> Result<IOResult<PagerCommitResult>> {
        if connection.is_nested_stmt.load(Ordering::SeqCst) {
            // Parent statement will handle the transaction rollback.
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        }
        tracing::trace!("end_tx(rollback={})", rollback);
        let Some(wal) = self.wal.as_ref() else {
            // TODO: Unsure what the semantics of "end_tx" is for in-memory databases, ephemeral tables and ephemeral indexes.
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        };
        let (is_write, schema_did_change) = match connection.get_tx_state() {
            TransactionState::Write { schema_did_change } => (true, schema_did_change),
            _ => (false, false),
        };
        tracing::trace!("end_tx(schema_did_change={})", schema_did_change);
        if rollback {
            if is_write {
                wal.borrow().end_write_tx();
            }
            wal.borrow().end_read_tx();
            self.rollback(schema_did_change, connection, is_write)?;
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        }
        let commit_status = return_if_io!(self.commit_dirty_pages(
            connection.is_wal_auto_checkpoint_disabled(),
            connection.get_sync_mode(),
            connection.get_data_sync_retry()
        ));
        wal.borrow().end_write_tx();
        wal.borrow().end_read_tx();

        if schema_did_change {
            let schema = connection.schema.read().clone();
            connection.db.update_schema_if_newer(schema)?;
        }
        Ok(IOResult::Done(commit_status))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn end_read_tx(&self) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        wal.borrow().end_read_tx();
        Ok(())
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
        tracing::trace!("read_page_no_cache(page_idx = {})", page_idx);
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

        if let Some(frame_id) = wal.borrow().find_frame(page_idx as u64, frame_watermark)? {
            let c = wal
                .borrow()
                .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            // TODO(pere) should probably first insert to page cache, and if successful,
            // read frame or page
            return Ok((page, c));
        }

        let c =
            self.begin_read_disk_page(page_idx as usize, page.clone(), allow_empty_read, &io_ctx)?;
        Ok((page, c))
    }

    /// Reads a page from the database.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub fn read_page(&self, page_idx: i64) -> Result<(PageRef, Option<Completion>)> {
        assert!(page_idx >= 0, "pages in pager should be positive, negative might indicate unallocated pages from mvcc or any other nasty bug");
        tracing::trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx as usize);
        if let Some(page) = page_cache.get(&page_key)? {
            tracing::trace!("read_page(page_idx = {}) = cached", page_idx);
            turso_assert!(
                page_idx as usize == page.get().id,
                "attempted to read page {page_idx} but got page {}",
                page.get().id
            );
            return Ok((page.clone(), None));
        }
        let (page, c) = self.read_page_no_cache(page_idx, None, false)?;
        turso_assert!(
            page_idx as usize == page.get().id,
            "attempted to read page {page_idx} but got page {}",
            page.get().id
        );
        self.cache_insert(page_idx as usize, page.clone(), &mut page_cache)?;
        Ok((page, Some(c)))
    }

    fn begin_read_disk_page(
        &self,
        page_idx: usize,
        page: PageRef,
        allow_empty_read: bool,
        io_ctx: &IOContext,
    ) -> Result<Completion> {
        sqlite3_ondisk::begin_read_page(
            self.db_file.clone(),
            self.buffer_pool.clone(),
            page,
            page_idx,
            allow_empty_read,
            io_ctx,
        )
    }

    fn cache_insert(
        &self,
        page_idx: usize,
        page: PageRef,
        page_cache: &mut PageCache,
    ) -> Result<()> {
        let page_key = PageCacheKey::new(page_idx);
        match page_cache.insert(page_key, page.clone()) {
            Ok(_) => {}
            Err(CacheError::KeyExists) => {
                unreachable!("Page should not exist in cache after get() miss")
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
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
                Some(page.clone())
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

    pub fn add_dirty(&self, page: &Page) {
        // TODO: check duplicates?
        let mut dirty_pages = self.dirty_pages.write();
        dirty_pages.insert(page.get().id);
        page.set_dirty();
    }

    pub fn wal_state(&self) -> Result<WalState> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_state() called on database without WAL".to_string(),
            ));
        };
        Ok(WalState {
            checkpoint_seq_no: wal.borrow().get_checkpoint_seq(),
            max_frame: wal.borrow().get_max_frame(),
        })
    }

    /// Flush all dirty pages to disk.
    /// Unlike commit_dirty_pages, this function does not commit, checkpoint now sync the WAL/Database.
    #[instrument(skip_all, level = Level::INFO)]
    pub fn cacheflush(&self) -> Result<Vec<Completion>> {
        let Some(wal) = self.wal.as_ref() else {
            // TODO: when ephemeral table spills to disk, it should cacheflush pages directly to the temporary database file.
            // This handling is not yet implemented, but it should be when spilling is implemented.
            return Err(LimboError::InternalError(
                "cacheflush() called on database without WAL".to_string(),
            ));
        };
        let dirty_pages = self
            .dirty_pages
            .read()
            .iter()
            .copied()
            .collect::<Vec<usize>>();
        let len = dirty_pages.len().min(IOV_MAX);
        let mut completions: Vec<Completion> = Vec::new();
        let mut pages = Vec::with_capacity(len);
        let page_sz = self.get_page_size().unwrap_or_default();

        let prepare = wal.borrow_mut().prepare_wal_start(page_sz)?;
        if let Some(c) = prepare {
            self.io.wait_for_completion(c)?;
            let c = wal.borrow_mut().prepare_wal_finish()?;
            self.io.wait_for_completion(c)?;
        }

        let commit_frame = None; // cacheflush only so we are not setting a commit frame here
        for (idx, page_id) in dirty_pages.iter().enumerate() {
            let page = {
                let mut cache = self.page_cache.write();
                let page_key = PageCacheKey::new(*page_id);
                let page = cache.get(&page_key)?.expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                let page_type = page.get_contents().maybe_page_type();
                trace!("cacheflush(page={}, page_type={:?}", page_id, page_type);
                page
            };
            pages.push(page);
            if pages.len() == IOV_MAX {
                match wal.borrow_mut().append_frames_vectored(
                    std::mem::replace(
                        &mut pages,
                        Vec::with_capacity(std::cmp::min(IOV_MAX, dirty_pages.len() - idx)),
                    ),
                    page_sz,
                    commit_frame,
                ) {
                    Err(e) => {
                        self.io.cancel(&completions)?;
                        self.io.drain()?;
                        return Err(e);
                    }
                    Ok(c) => completions.push(c),
                }
            }
        }
        if !pages.is_empty() {
            match wal
                .borrow_mut()
                .append_frames_vectored(pages, page_sz, commit_frame)
            {
                Ok(c) => completions.push(c),
                Err(e) => {
                    tracing::error!("cacheflush: error appending frames: {e}");
                    self.io.cancel(&completions)?;
                    self.io.drain()?;
                    return Err(e);
                }
            }
        }
        Ok(completions)
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
        match self.commit_dirty_pages_inner(
            wal_auto_checkpoint_disabled,
            sync_mode,
            data_sync_retry,
        ) {
            r @ (Ok(IOResult::Done(..)) | Err(..)) => {
                self.commit_info.write().state = CommitState::PrepareWal;
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

        loop {
            let state = self.commit_info.read().state;
            trace!(?state);
            match state {
                CommitState::PrepareWal => {
                    let page_sz = self.get_page_size_unchecked();
                    let c = wal.borrow_mut().prepare_wal_start(page_sz)?;
                    let Some(c) = c else {
                        self.commit_info.write().state = CommitState::GetDbSize;
                        continue;
                    };
                    self.commit_info.write().state = CommitState::PrepareWalSync;
                    if !c.is_completed() {
                        io_yield_one!(c);
                    }
                }
                CommitState::PrepareWalSync => {
                    let c = wal.borrow_mut().prepare_wal_finish()?;
                    self.commit_info.write().state = CommitState::GetDbSize;
                    if !c.is_completed() {
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
                    let now = self.io.now();
                    self.commit_info.write().time = now;

                    let dirty_ids: Vec<usize> = self.dirty_pages.read().iter().copied().collect();
                    if dirty_ids.is_empty() {
                        return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                    }

                    let mut completions = Vec::new();
                    {
                        let mut commit_info = self.commit_info.write();
                        commit_info.completions.clear();
                    }
                    let page_sz = self.get_page_size_unchecked();
                    let mut pages: Vec<PageRef> = Vec::with_capacity(dirty_ids.len().min(IOV_MAX));
                    let total = dirty_ids.len();
                    let mut cache = self.page_cache.write();
                    for (i, page_id) in dirty_ids.into_iter().enumerate() {
                        let page = {
                            let page_key = PageCacheKey::new(page_id);
                            let page = cache.get(&page_key)?.expect(
                                "dirty list contained a page that cache dropped (page={page_id})",
                            );
                            trace!(
                                "commit_dirty_pages(page={}, page_type={:?})",
                                page_id,
                                page.get_contents().maybe_page_type()
                            );
                            page
                        };
                        pages.push(page);

                        let end_of_chunk = pages.len() == IOV_MAX || i == total - 1;
                        if end_of_chunk {
                            let commit_flag = if i == total - 1 {
                                // Only the commit frame (final) frame carries the db_size
                                Some(db_size)
                            } else {
                                None
                            };
                            let r = wal.borrow_mut().append_frames_vectored(
                                std::mem::take(&mut pages),
                                page_sz,
                                commit_flag,
                            );
                            match r {
                                Ok(c) => completions.push(c),
                                Err(e) => {
                                    self.io.cancel(&completions)?;
                                    return Err(e);
                                }
                            }
                        }
                    }
                    self.dirty_pages.write().clear();
                    // Nothing to append
                    if completions.is_empty() {
                        return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                    } else {
                        // Skip sync if synchronous mode is OFF
                        if sync_mode == crate::SyncMode::Off {
                            if wal_auto_checkpoint_disabled || !wal.borrow().should_checkpoint() {
                                let mut commit_info = self.commit_info.write();
                                commit_info.completions = completions;
                                commit_info.result = Some(PagerCommitResult::WalWritten);
                                commit_info.state = CommitState::Done;
                                continue;
                            }
                            {
                                let mut commit_info = self.commit_info.write();
                                commit_info.completions = completions;
                                commit_info.state = CommitState::Checkpoint;
                            }
                        } else {
                            let mut commit_info = self.commit_info.write();
                            commit_info.completions = completions;
                            commit_info.state = CommitState::SyncWal;
                        }
                    }
                }
                CommitState::SyncWal => {
                    let sync_result = wal.borrow_mut().sync();
                    let c = match sync_result {
                        Ok(c) => c,
                        Err(e) if !data_sync_retry => {
                            panic!("fsync error (data_sync_retry=off): {e:?}");
                        }
                        Err(e) => return Err(e),
                    };
                    self.commit_info.write().completions.push(c);
                    if wal_auto_checkpoint_disabled || !wal.borrow().should_checkpoint() {
                        let mut commit_info = self.commit_info.write();
                        commit_info.result = Some(PagerCommitResult::WalWritten);
                        commit_info.state = CommitState::Done;
                        continue;
                    }
                    self.commit_info.write().state = CommitState::Checkpoint;
                }
                CommitState::Checkpoint => {
                    match self.checkpoint()? {
                        IOResult::IO(cmp) => {
                            let completions = {
                                let mut commit_info = self.commit_info.write();
                                match cmp {
                                    IOCompletions::Single(c) => {
                                        commit_info.completions.push(c);
                                    }
                                    IOCompletions::Many(c) => {
                                        commit_info.completions.extend(c);
                                    }
                                }
                                std::mem::take(&mut commit_info.completions)
                            };
                            // TODO: remove serialization of checkpoint path
                            io_yield_many!(completions);
                        }
                        IOResult::Done(res) => {
                            let mut commit_info = self.commit_info.write();
                            commit_info.result = Some(PagerCommitResult::Checkpointed(res));
                            // Skip sync if synchronous mode is OFF
                            if sync_mode == crate::SyncMode::Off {
                                commit_info.state = CommitState::Done;
                            } else {
                                commit_info.state = CommitState::SyncDbFile;
                            }
                        }
                    }
                }
                CommitState::SyncDbFile => {
                    let sync_result =
                        sqlite3_ondisk::begin_sync(self.db_file.clone(), self.syncing.clone());
                    self.commit_info
                        .write()
                        .completions
                        .push(match sync_result {
                            Ok(c) => c,
                            Err(e) if !data_sync_retry => {
                                panic!("fsync error on database file (data_sync_retry=off): {e:?}");
                            }
                            Err(e) => return Err(e),
                        });
                    self.commit_info.write().state = CommitState::Done;
                }
                CommitState::Done => {
                    tracing::debug!(
                        "total time flushing cache: {} ms",
                        self.io
                            .now()
                            .to_system_time()
                            .duration_since(self.commit_info.read().time.to_system_time())
                            .unwrap()
                            .as_millis()
                    );
                    let (should_finish, result, completions) = {
                        let mut commit_info = self.commit_info.write();
                        if commit_info.completions.iter().all(|c| c.is_completed()) {
                            commit_info.completions.clear();
                            commit_info.state = CommitState::PrepareWal;
                            (true, commit_info.result.take(), Vec::new())
                        } else {
                            (false, None, std::mem::take(&mut commit_info.completions))
                        }
                    };
                    if should_finish {
                        wal.borrow_mut().finish_append_frames_commit()?;
                        return Ok(IOResult::Done(result.expect("commit result should be set")));
                    }
                    io_yield_many!(completions);
                }
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        let wal = self.wal.as_ref().unwrap().borrow();
        wal.changed_pages_after(frame_watermark)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<Completion> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_get_frame() called on database without WAL".to_string(),
            ));
        };
        let wal = wal.borrow();
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

        {
            let mut wal = wal.borrow_mut();
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
            for page_id in self.dirty_pages.read().iter() {
                let page_key = PageCacheKey::new(*page_id);
                let mut cache = self.page_cache.write();
                let page = cache.get(&page_key)?.expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                page.clear_dirty();
            }
            self.dirty_pages.write().clear();
        }
        Ok(WalFrameInfo {
            page_no: header.page_number,
            db_size: header.db_size,
        })
    }

    #[instrument(skip_all, level = Level::DEBUG, name = "pager_checkpoint",)]
    pub fn checkpoint(&self) -> Result<IOResult<CheckpointResult>> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "checkpoint() called on database without WAL".to_string(),
            ));
        };
        loop {
            let state = std::mem::take(&mut *self.checkpoint_state.write());
            trace!(?state);
            match state {
                CheckpointState::Checkpoint => {
                    let res = return_if_io!(wal.borrow_mut().checkpoint(
                        self,
                        CheckpointMode::Passive {
                            upper_bound_inclusive: None
                        }
                    ));
                    *self.checkpoint_state.write() = CheckpointState::SyncDbFile { res };
                }
                CheckpointState::SyncDbFile { res } => {
                    let c = sqlite3_ondisk::begin_sync(self.db_file.clone(), self.syncing.clone())?;
                    *self.checkpoint_state.write() = CheckpointState::CheckpointDone { res };
                    io_yield_one!(c);
                }
                CheckpointState::CheckpointDone { res } => {
                    turso_assert!(
                        !self.syncing.load(Ordering::SeqCst),
                        "syncing should be done"
                    );
                    *self.checkpoint_state.write() = CheckpointState::Checkpoint;
                    return Ok(IOResult::Done(res));
                }
            }
        }
    }

    /// Invalidates entire page cache by removing all dirty and clean pages. Usually used in case
    /// of a rollback or in case we want to invalidate page cache after starting a read transaction
    /// right after new writes happened which would invalidate current page cache.
    pub fn clear_page_cache(&self) {
        let dirty_pages = self.dirty_pages.read();
        let mut cache = self.page_cache.write();
        for page_id in dirty_pages.iter() {
            let page_key = PageCacheKey::new(*page_id);
            if let Some(page) = cache.get(&page_key).unwrap_or(None) {
                page.clear_dirty();
            }
        }
        cache.clear().expect("Failed to clear page cache");
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
    pub fn checkpoint_shutdown(&self, wal_auto_checkpoint_disabled: bool) -> Result<()> {
        let mut attempts = 0;
        {
            let Some(wal) = self.wal.as_ref() else {
                return Err(LimboError::InternalError(
                    "checkpoint_shutdown() called on database without WAL".to_string(),
                ));
            };
            let mut wal = wal.borrow_mut();
            // fsync the wal syncronously before beginning checkpoint
            let c = wal.sync()?;
            self.io.wait_for_completion(c)?;
        }
        if !wal_auto_checkpoint_disabled {
            while let Err(LimboError::Busy) = self.wal_checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            }) {
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

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_checkpoint_start(&self, mode: CheckpointMode) -> Result<IOResult<CheckpointResult>> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_checkpoint() called on database without WAL".to_string(),
            ));
        };

        wal.borrow_mut().checkpoint(self, mode)
    }

    pub fn wal_checkpoint_finish(
        &self,
        checkpoint_result: &mut CheckpointResult,
    ) -> Result<IOResult<()>> {
        'ensure_sync: {
            if checkpoint_result.num_backfilled != 0 {
                if checkpoint_result.everything_backfilled() {
                    let db_size = self
                        .io
                        .block(|| self.with_header(|header| header.database_size))?
                        .get();
                    let page_size = self.get_page_size().unwrap_or_default();
                    let expected = (db_size * page_size.get()) as u64;
                    if expected < self.db_file.size()? {
                        if !checkpoint_result.db_truncate_sent {
                            let c = self.db_file.truncate(
                                expected as usize,
                                Completion::new_trunc(move |_| {
                                    tracing::trace!(
                                        "Database file truncated to expected size: {} bytes",
                                        expected
                                    );
                                }),
                            )?;
                            checkpoint_result.db_truncate_sent = true;
                            io_yield_one!(c);
                        }
                        if !checkpoint_result.db_sync_sent {
                            let c = self.db_file.sync(Completion::new_sync(move |_| {
                                tracing::trace!("Database file syncd after truncation");
                            }))?;
                            checkpoint_result.db_sync_sent = true;
                            io_yield_one!(c);
                        }
                        break 'ensure_sync;
                    }
                }
                if !checkpoint_result.db_sync_sent {
                    // if we backfilled at all, we have to sync the db-file here
                    let c = self.db_file.sync(Completion::new_sync(move |_| {}))?;
                    checkpoint_result.db_sync_sent = true;
                    io_yield_one!(c);
                }
            }
        }
        checkpoint_result.release_guard();
        // TODO: only clear cache of things that are really invalidated
        self.page_cache
            .write()
            .clear()
            .map_err(|e| LimboError::InternalError(format!("Failed to clear page cache: {e:?}")))?;
        Ok(IOResult::Done(()))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_checkpoint(&self, mode: CheckpointMode) -> Result<CheckpointResult> {
        let mut result = self.io.block(|| self.wal_checkpoint_start(mode))?;
        self.io.block(|| self.wal_checkpoint_finish(&mut result))?;
        Ok(result)
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

                    let (page, _c) = match page.take() {
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
                        None => {
                            let (page, c) = self.read_page(page_id as i64)?;
                            (page, Some(c))
                        }
                    };
                    header.freelist_pages = (header.freelist_pages.get() + 1).into();

                    let trunk_page_id = header.freelist_trunk_page.get();

                    if trunk_page_id != 0 {
                        *state = FreePageState::AddToTrunk { page };
                    } else {
                        *state = FreePageState::NewTrunk { page };
                    }
                }
                FreePageState::AddToTrunk { page } => {
                    let trunk_page_id = header.freelist_trunk_page.get();
                    let (trunk_page, c) = self.read_page(trunk_page_id as i64)?;
                    if let Some(c) = c {
                        if !c.is_completed() {
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
                        self.add_dirty(&trunk_page);

                        trunk_page_contents.write_u32_no_offset(
                            TRUNK_PAGE_LEAF_COUNT_OFFSET,
                            number_of_leaf_pages + 1,
                        );
                        trunk_page_contents.write_u32_no_offset(
                            TRUNK_PAGE_HEADER_SIZE
                                + (number_of_leaf_pages as usize * LEAF_ENTRY_SIZE),
                            page_id as u32,
                        );

                        break;
                    }
                    *state = FreePageState::NewTrunk { page: page.clone() };
                }
                FreePageState::NewTrunk { page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    // If we get here, need to make this page a new trunk
                    turso_assert!(page.get().id == page_id, "page has unexpected id");
                    self.add_dirty(page);

                    let trunk_page_id = header.freelist_trunk_page.get();

                    let contents = page.get().contents.as_mut().unwrap();
                    // Point to previous trunk
                    contents.write_u32_no_offset(TRUNK_PAGE_NEXT_PAGE_OFFSET, trunk_page_id);
                    // Zero leaf count
                    contents.write_u32_no_offset(TRUNK_PAGE_LEAF_COUNT_OFFSET, 0);
                    // Update page 1 to point to new trunk
                    header.freelist_trunk_page = (page_id as u32).into();
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
                tracing::trace!("allocate_page1(Start)");
                self.db_state.set(DbState::Initializing);
                let mut default_header = DatabaseHeader::default();

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
                self.db_state.set(DbState::Initialized);
                *self.allocate_page1_state.write() = AllocatePage1State::Done;
                Ok(IOResult::Done(page.clone()))
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
    /// FIXME: handle no room in page cache
    #[allow(clippy::readonly_write_lock)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn allocate_page(&self) -> Result<IOResult<PageRef>> {
        const FREELIST_TRUNK_OFFSET_NEXT_TRUNK: usize = 0;
        const FREELIST_TRUNK_OFFSET_LEAF_COUNT: usize = 4;
        const FREELIST_TRUNK_OFFSET_FIRST_LEAF: usize = 8;

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
                            self.add_dirty(&page);
                            let page_key = PageCacheKey::new(page.get().id as usize);
                            let mut cache = self.page_cache.write();
                            cache.insert(page_key, page.clone())?;
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
                        *state = AllocatePageState::AllocateNewPage {
                            current_db_size: *current_db_size,
                        };
                        continue;
                    }

                    // Freelist is not empty, so we can reuse the trunk itself as a new page
                    // and update the database's first freelist trunk page to the next trunk page.
                    header.freelist_trunk_page = next_trunk_page_id.into();
                    header.freelist_pages = (header.freelist_pages.get() - 1).into();
                    self.add_dirty(trunk_page);
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
                    self.add_dirty(leaf_page);
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
                    self.add_dirty(trunk_page);

                    header.freelist_pages = (header.freelist_pages.get() - 1).into();
                    let leaf_page = leaf_page.clone();
                    *state = AllocatePageState::Start;
                    return Ok(IOResult::Done(leaf_page));
                }
                AllocatePageState::AllocateNewPage { current_db_size } => {
                    let new_db_size = *current_db_size + 1;

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
                        self.add_dirty(&page);

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

    pub fn update_dirty_loaded_page_in_cache(
        &self,
        id: usize,
        page: PageRef,
    ) -> Result<(), LimboError> {
        let mut cache = self.page_cache.write();
        let page_key = PageCacheKey::new(id);

        // FIXME: use specific page key for writer instead of max frame, this will make readers not conflict
        assert!(page.is_dirty());
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
    pub fn rollback(
        &self,
        schema_did_change: bool,
        connection: &Connection,
        is_write: bool,
    ) -> Result<(), LimboError> {
        tracing::debug!(schema_did_change);
        self.clear_page_cache();
        if is_write {
            self.dirty_pages.write().clear();
        } else {
            turso_assert!(
                self.dirty_pages.read().is_empty(),
                "dirty pages should be empty for read txn"
            );
        }
        self.reset_internal_states();
        if schema_did_change {
            *connection.schema.write() = connection.db.clone_schema()?;
        }
        if is_write {
            if let Some(wal) = self.wal.as_ref() {
                wal.borrow_mut().rollback()?;
            }
        }

        Ok(())
    }

    fn reset_internal_states(&self) {
        *self.checkpoint_state.write() = CheckpointState::Checkpoint;
        self.syncing.store(false, Ordering::SeqCst);
        self.commit_info.write().state = CommitState::PrepareWal;
        self.commit_info.write().time = self.io.now();
        *self.allocate_page_state.write() = AllocatePageState::Start;
        *self.free_page_state.write() = FreePageState::Start;
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
        Ok(IOResult::Done(f(header)))
    }

    pub fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<IOResult<T>> {
        let header_ref = return_if_io!(HeaderRefMut::from_pager(self));
        let header = header_ref.borrow_mut();
        Ok(IOResult::Done(f(header)))
    }

    pub fn is_encryption_ctx_set(&self) -> bool {
        self.io_ctx.write().encryption_context().is_some()
    }

    pub fn set_encryption_context(
        &self,
        cipher_mode: CipherMode,
        key: &EncryptionKey,
    ) -> Result<()> {
        let page_size = self.get_page_size_unchecked().get() as usize;
        let encryption_ctx = EncryptionContext::new(cipher_mode, key, page_size)?;
        {
            let mut io_ctx = self.io_ctx.write();
            io_ctx.set_encryption(encryption_ctx);
        }
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        wal.borrow_mut().set_io_context(self.io_ctx.read().clone());
        Ok(())
    }

    pub fn reset_checksum_context(&self) {
        {
            let mut io_ctx = self.io_ctx.write();
            io_ctx.reset_checksum();
        }
        let Some(wal) = self.wal.as_ref() else { return };
        wal.borrow_mut().set_io_context(self.io_ctx.read().clone())
    }

    pub fn set_reserved_space_bytes(&self, value: u8) {
        self.set_reserved_space(value);
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
mod ptrmap {
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
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    use super::ptrmap::*;
    use super::*;
    use crate::io::{MemoryIO, OpenFlags, IO};
    use crate::storage::buffer_pool::BufferPool;
    use crate::storage::database::{DatabaseFile, DatabaseStorage};
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

        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            WalFileShared::new_shared(
                io.open_file("test.db-wal", OpenFlags::Create, false)
                    .unwrap(),
            )
            .unwrap(),
            buffer_pool.clone(),
        )));

        let pager = Pager::new(
            db_file,
            Some(wal),
            io,
            page_cache,
            buffer_pool,
            Arc::new(AtomicDbState::new(DbState::Uninitialized)),
            Arc::new(Mutex::new(())),
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
