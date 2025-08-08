use crate::result::LimboResult;
use crate::storage::{
    btree::BTreePageInner,
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    sqlite3_ondisk::{
        self, parse_wal_frame_header, DatabaseHeader, PageContent, PageSize, PageType,
    },
    wal::{CheckpointResult, Wal},
};
use crate::storage::wal::{CheckpointResult, Wal};
use crate::types::{IOCompletions, IOResult, WalFrameInfo};
use crate::util::IOExt as _;
use crate::{
    return_if_io, turso_assert, types::WalFrameInfo, Completion, Connection, IOResult, LimboError,
    Result, TransactionState,
};
use parking_lot::RwLock;
use std::cell::{Cell, OnceCell, RefCell, UnsafeCell};
use std::collections::HashSet;
use std::hash;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{instrument, trace, Level};

use super::btree::{btree_init_page, BTreePage};
use super::page_cache::{CacheError, CacheResizeResult, DumbLruPageCache, PageCacheKey};
use super::sqlite3_ondisk::begin_write_btree_page;
use super::wal::CheckpointMode;

/// SQLite's default maximum page count
const DEFAULT_MAX_PAGE_COUNT: u32 = 0xfffffffe;

#[cfg(not(feature = "omit_autovacuum"))]
use ptrmap::*;

#[derive(Debug, Clone)]
pub struct HeaderRef(PageRef);

impl HeaderRef {
    pub fn from_pager(pager: &Pager) -> Result<IOResult<Self>> {
        let state = pager.header_ref_state.borrow().clone();
        tracing::trace!(?state);
        match state {
            HeaderRefState::Start => {
                if !pager.db_state.is_initialized() {
                    return Err(LimboError::Page1NotAlloc);
                }

                let (page, c) = pager.read_page(DatabaseHeader::PAGE_ID)?;
                *pager.header_ref_state.borrow_mut() = HeaderRefState::CreateHeader { page };
                Ok(IOResult::IO(IOCompletions::Single(c)))
            }
            HeaderRefState::CreateHeader { page } => {
                turso_assert!(page.is_loaded(), "page should be loaded");
                turso_assert!(
                    page.get().id == DatabaseHeader::PAGE_ID,
                    "incorrect header page id"
                );
                *pager.header_ref_state.borrow_mut() = HeaderRefState::Start;
                Ok(IOResult::Done(Self(page)))
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
        let state = pager.header_ref_state.borrow().clone();
        tracing::trace!(?state);
        match state {
            HeaderRefState::Start => {
                if !pager.db_state.is_initialized() {
                    return Err(LimboError::Page1NotAlloc);
                }

                let (page, c) = pager.read_page(DatabaseHeader::PAGE_ID)?;
                *pager.header_ref_state.borrow_mut() = HeaderRefState::CreateHeader { page };
                Ok(IOResult::IO(IOCompletions::Single(c)))
            }
            HeaderRefState::CreateHeader { page } => {
                turso_assert!(page.is_loaded(), "page should be loaded");
                turso_assert!(
                    page.get().id == DatabaseHeader::PAGE_ID,
                    "incorrect header page id"
                );

                pager.add_dirty(&page);
                *pager.header_ref_state.borrow_mut() = HeaderRefState::Start;
                Ok(IOResult::Done(Self(page)))
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
    pub pin_count: AtomicUsize,
}

#[derive(Debug)]
pub struct Page {
    pub inner: UnsafeCell<PageInner>,
}

// Concurrency control of pages will be handled by the pager, we won't wrap Page with RwLock
// because that is bad bad.
pub type PageRef = Arc<Page>;

/// Page is locked for I/O to prevent concurrent access.
const PAGE_LOCKED: usize = 0b010;
/// Page had an I/O error.
const PAGE_ERROR: usize = 0b100;
/// Page is dirty. Flush needed.
const PAGE_DIRTY: usize = 0b1000;
/// Page's contents are loaded in memory.
const PAGE_LOADED: usize = 0b10000;

impl Page {
    pub fn new(id: usize) -> Self {
        Self {
            inner: UnsafeCell::new(PageInner {
                flags: AtomicUsize::new(0),
                contents: None,
                id,
                pin_count: AtomicUsize::new(0),
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
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn is_error(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_ERROR != 0
    }

    pub fn set_error(&self) {
        self.get().flags.fetch_or(PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn clear_error(&self) {
        self.get().flags.fetch_and(!PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        tracing::debug!("set_dirty(page={})", self.get().id);
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn clear_dirty(&self) {
        tracing::debug!("clear_dirty(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn clear_loaded(&self) {
        tracing::debug!("clear loaded {}", self.get().id);
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn is_index(&self) -> bool {
        match self.get_contents().page_type() {
            PageType::IndexLeaf | PageType::IndexInterior => true,
            PageType::TableLeaf | PageType::TableInterior => false,
        }
    }

    /// Pin the page to prevent it from being evicted from the page cache.
    pub fn pin(&self) {
        self.get().pin_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Unpin the page to allow it to be evicted from the page cache.
    pub fn unpin(&self) {
        let was_pinned = self.try_unpin();

        turso_assert!(
            was_pinned,
            "Attempted to unpin page {} that was not pinned",
            self.get().id
        );
    }

    /// Try to unpin the page if it's pinned, otherwise do nothing.
    /// Returns true if the page was originally pinned.
    pub fn try_unpin(&self) -> bool {
        self.get()
            .pin_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current == 0 {
                    None
                } else {
                    Some(current - 1)
                }
            })
            .is_ok()
    }

    pub fn is_pinned(&self) -> bool {
        self.get().pin_count.load(Ordering::SeqCst) > 0
    }
}

#[derive(Clone, Copy, Debug)]
/// The state of the current pager cache commit.
enum CommitState {
    /// Appends all frames to the WAL.
    Start,
    /// Fsync the on-disk WAL.
    SyncWal,
    /// After Fsync the on-disk WAL.
    AfterSyncWal,
    /// Checkpoint the WAL to the database file (if needed).
    Checkpoint,
    /// Fsync the database file.
    SyncDbFile,
    /// After database file is fsynced.
    AfterSyncDbFile,
}

#[derive(Clone, Debug, Copy)]
enum CheckpointState {
    Checkpoint,
    SyncDbFile,
    WaitSyncDbFile,
    CheckpointDone,
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
    state: CommitState,
    /// Number of writes taking place. When in_flight gets to 0 we can schedule a fsync.
    in_flight_writes: Rc<RefCell<usize>>,
    /// Dirty pages to be flushed.
    dirty_pages: Vec<usize>,
}

/// Track the state of the auto-vacuum mode.
#[derive(Clone, Copy, Debug)]
pub enum AutoVacuumMode {
    None,
    Full,
    Incremental,
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
        self.0.store(state as usize, Ordering::SeqCst);
    }

    #[inline]
    pub fn get(&self) -> DbState {
        let v = self.0.load(Ordering::SeqCst);
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
    page_cache: Arc<RwLock<DumbLruPageCache>>,
    /// Buffer pool for temporary data storage.
    pub buffer_pool: Arc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    dirty_pages: Rc<RefCell<HashSet<usize, hash::BuildHasherDefault<hash::DefaultHasher>>>>,

    commit_info: RefCell<CommitInfo>,
    checkpoint_state: RefCell<CheckpointState>,
    checkpoint_inflight: Rc<RefCell<usize>>,
    syncing: Rc<RefCell<bool>>,
    auto_vacuum_mode: RefCell<AutoVacuumMode>,
    /// 0 -> Database is empty,
    /// 1 -> Database is being initialized,
    /// 2 -> Database is initialized and ready for use.
    pub db_state: Arc<AtomicDbState>,
    /// Mutex for synchronizing database initialization to prevent race conditions
    init_lock: Arc<Mutex<()>>,
    /// The state of the current allocate page operation.
    allocate_page_state: RefCell<AllocatePageState>,
    /// The state of the current allocate page1 operation.
    allocate_page1_state: RefCell<AllocatePage1State>,
    /// Cache page_size and reserved_space at Pager init and reuse for subsequent
    /// `usable_space` calls. TODO: Invalidate reserved_space when we add the functionality
    /// to change it.
    pub(crate) page_size: Cell<Option<u32>>,
    reserved_space: OnceCell<u8>,
    free_page_state: RefCell<FreePageState>,
    /// Maximum number of pages allowed in the database. Default is 1073741823 (SQLite default).
    max_page_count: Cell<u32>,
    #[cfg(not(feature = "omit_autovacuum"))]
    /// State machine for [Pager::ptrmap_get]
    ptrmap_get_state: RefCell<PtrMapGetState>,
    #[cfg(not(feature = "omit_autovacuum"))]
    /// State machine for [Pager::ptrmap_put]
    ptrmap_put_state: RefCell<PtrMapPutState>,
    header_ref_state: RefCell<HeaderRefState>,
    #[cfg(not(feature = "omit_autovacuum"))]
    btree_create_vacuum_full_state: Cell<BtreeCreateVacuumFullState>,
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
    Writing {
        write_counter: Rc<RefCell<usize>>,
        page: BTreePage,
    },
    Done,
}

#[derive(Debug, Clone)]
enum FreePageState {
    Start,
    AddToTrunk {
        page: Arc<Page>,
        trunk_page: Option<Arc<Page>>,
    },
    NewTrunk {
        page: Arc<Page>,
    },
}

impl Pager {
    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Option<Rc<RefCell<dyn Wal>>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<DumbLruPageCache>>,
        buffer_pool: Arc<BufferPool>,
        db_state: Arc<AtomicDbState>,
        init_lock: Arc<Mutex<()>>,
    ) -> Result<Self> {
        let allocate_page1_state = if !db_state.is_initialized() {
            RefCell::new(AllocatePage1State::Start)
        } else {
            RefCell::new(AllocatePage1State::Done)
        };
        Ok(Self {
            db_file,
            wal,
            page_cache,
            io,
            dirty_pages: Rc::new(RefCell::new(HashSet::with_hasher(
                hash::BuildHasherDefault::new(),
            ))),
            commit_info: RefCell::new(CommitInfo {
                state: CommitState::Start,
                in_flight_writes: Rc::new(RefCell::new(0)),
                dirty_pages: Vec::new(),
            }),
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_state: RefCell::new(CheckpointState::Checkpoint),
            checkpoint_inflight: Rc::new(RefCell::new(0)),
            buffer_pool,
            auto_vacuum_mode: RefCell::new(AutoVacuumMode::None),
            db_state,
            init_lock,
            allocate_page1_state,
            page_size: Cell::new(None),
            reserved_space: OnceCell::new(),
            free_page_state: RefCell::new(FreePageState::Start),
            allocate_page_state: RefCell::new(AllocatePageState::Start),
            max_page_count: Cell::new(DEFAULT_MAX_PAGE_COUNT),
            #[cfg(not(feature = "omit_autovacuum"))]
            ptrmap_get_state: RefCell::new(PtrMapGetState::Start),
            #[cfg(not(feature = "omit_autovacuum"))]
            ptrmap_put_state: RefCell::new(PtrMapPutState::Start),
            header_ref_state: RefCell::new(HeaderRefState::Start),
            #[cfg(not(feature = "omit_autovacuum"))]
            btree_create_vacuum_full_state: Cell::new(BtreeCreateVacuumFullState::Start),
        })
    }

    /// Get the maximum page count for this database
    pub fn get_max_page_count(&self) -> u32 {
        self.max_page_count.get()
    }

    /// Set the maximum page count for this database
    /// Returns the new maximum page count (may be clamped to current database size)
    pub fn set_max_page_count(&self, new_max: u32) -> crate::Result<IOResult<u32>> {
        // Get current database size
        let current_page_count =
            return_if_io!(self.with_header(|header| header.database_size.get()));

        // Clamp new_max to be at least the current database size
        let clamped_max = std::cmp::max(new_max, current_page_count);
        self.max_page_count.set(clamped_max);
        Ok(IOResult::Done(clamped_max))
    }

    pub fn set_wal(&mut self, wal: Rc<RefCell<dyn Wal>>) {
        self.wal = Some(wal);
    }

    pub fn get_auto_vacuum_mode(&self) -> AutoVacuumMode {
        *self.auto_vacuum_mode.borrow()
    }

    pub fn set_auto_vacuum_mode(&self, mode: AutoVacuumMode) {
        *self.auto_vacuum_mode.borrow_mut() = mode;
    }

    /// Retrieves the pointer map entry for a given database page.
    /// `target_page_num` (1-indexed) is the page whose entry is sought.
    /// Returns `Ok(None)` if the page is not supposed to have a ptrmap entry (e.g. header, or a ptrmap page itself).
    #[cfg(not(feature = "omit_autovacuum"))]
    pub fn ptrmap_get(&self, target_page_num: u32) -> Result<IOResult<Option<PtrmapEntry>>> {
        let ptrmap_get_state = self.ptrmap_get_state.borrow().clone();
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
                let offset_in_ptrmap_page =
                    get_ptrmap_offset_in_page(target_page_num, ptrmap_pg_no, configured_page_size)?;
                tracing::trace!(
                    "ptrmap_get(page_idx = {}) = ptrmap_pg_no = {}",
                    target_page_num,
                    ptrmap_pg_no
                );

                let (ptrmap_page, _c) = self.read_page(ptrmap_pg_no as usize)?;
                self.ptrmap_get_state.replace(PtrMapGetState::Deserialize {
                    ptrmap_page,
                    offset_in_ptrmap_page,
                });
                Ok(IOResult::IO)
            }
            PtrMapGetState::Deserialize {
                ptrmap_page,
                offset_in_ptrmap_page,
            } => {
                if ptrmap_page.is_locked() {
                    return Ok(IOResult::IO);
                }
                if !ptrmap_page.is_loaded() {
                    return Ok(IOResult::IO);
                }
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
                self.ptrmap_get_state.replace(PtrMapGetState::Start);
                match PtrmapEntry::deserialize(entry_slice) {
                    Some(entry) => Ok(IOResult::Done(Some(entry))),
                    None => Err(LimboError::Corrupt(format!(
                        "Failed to deserialize ptrmap entry for page {target_page_num} from ptrmap page {ptrmap_pg_no}"
                    ))),
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
        let ptrmap_put_state = self.ptrmap_put_state.borrow().clone();
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

                let ptrmap_pg_no = get_ptrmap_page_no_for_db_page(db_page_no_to_update, page_size);
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

                let (ptrmap_page, _c) = self.read_page(ptrmap_pg_no as usize)?;
                self.ptrmap_put_state.replace(PtrMapPutState::Deserialize {
                    ptrmap_page,
                    offset_in_ptrmap_page,
                });
                Ok(IOResult::IO)
            }
            PtrMapPutState::Deserialize {
                ptrmap_page,
                offset_in_ptrmap_page,
            } => {
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
                self.ptrmap_put_state.replace(PtrMapPutState::Start);
                Ok(IOResult::Done(()))
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
            Ok(IOResult::Done(page.get().get().id as u32))
        }

        //  If autovacuum is enabled, we need to allocate a new page number that is greater than the largest root page number
        #[cfg(not(feature = "omit_autovacuum"))]
        {
            let auto_vacuum_mode = self.auto_vacuum_mode.borrow();
            match *auto_vacuum_mode {
                AutoVacuumMode::None => {
                    let page =
                        return_if_io!(self.do_allocate_page(page_type, 0, BtreePageAllocMode::Any));
                    Ok(IOResult::Done(page.get().get().id as u32))
                }
                AutoVacuumMode::Full => {
                    loop {
                        match self.btree_create_vacuum_full_state.get() {
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
                                self.btree_create_vacuum_full_state.set(
                                    BtreeCreateVacuumFullState::AllocatePage { root_page_num },
                                );
                            }
                            BtreeCreateVacuumFullState::AllocatePage { root_page_num } => {
                                //  root_page_num here is the desired root page
                                let page = return_if_io!(self.do_allocate_page(
                                    page_type,
                                    0,
                                    BtreePageAllocMode::Exact(root_page_num),
                                ));
                                let allocated_page_id = page.get().get().id as u32;
                                if allocated_page_id != root_page_num {
                                    //  TODO(Zaid): Handle swapping the allocated page with the desired root page
                                }

                                //  TODO(Zaid): Update the header metadata to reflect the new root page number
                                self.btree_create_vacuum_full_state.set(
                                    BtreeCreateVacuumFullState::PtrMapPut { allocated_page_id },
                                );
                            }
                            BtreeCreateVacuumFullState::PtrMapPut { allocated_page_id } => {
                                //  For now map allocated_page_id since we are not swapping it with root_page_num
                                let res = match self.ptrmap_put(
                                    allocated_page_id,
                                    PtrmapType::RootPage,
                                    0,
                                )? {
                                    IOResult::Done(_) => Ok(IOResult::Done(allocated_page_id)),
                                    IOResult::IO => return Ok(IOResult::IO),
                                };
                                self.btree_create_vacuum_full_state
                                    .set(BtreeCreateVacuumFullState::Start);
                                return res;
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
    ) -> Result<IOResult<BTreePage>> {
        let page = return_if_io!(self.allocate_page());
        let page = Arc::new(BTreePageInner {
            page: RefCell::new(page),
        });
        btree_init_page(&page, page_type, offset, self.usable_space() as u16);
        tracing::debug!(
            "do_allocate_page(id={}, page_type={:?})",
            page.get().get().id,
            page.get().get_contents().page_type()
        );
        Ok(IOResult::Done(page))
    }

    /// The "usable size" of a database page is the page size specified by the 2-byte integer at offset 16
    /// in the header, minus the "reserved" space size recorded in the 1-byte integer at offset 20 in the header.
    /// The usable size of a page might be an odd number. However, the usable size is not allowed to be less than 480.
    /// In other words, if the page size is 512, then the reserved space size cannot exceed 32.
    pub fn usable_space(&self) -> usize {
        let page_size = *self.page_size.get().get_or_insert_with(|| {
            self.io
                .block(|| self.with_header(|header| header.page_size))
                .unwrap_or_default()
                .get()
        });

        let reserved_space = *self.reserved_space.get_or_init(|| {
            self.io
                .block(|| self.with_header(|header| header.reserved_space))
                .unwrap_or_default()
        });

        (page_size as usize) - (reserved_space as usize)
    }

    /// Set the initial page size for the database. Should only be called before the database is initialized
    pub fn set_initial_page_size(&self, size: u32) {
        assert_eq!(self.db_state.get(), DbState::Uninitialized);
        self.page_size.replace(Some(size));
    }

    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_read_tx(&self) -> Result<LimboResult> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(LimboResult::Ok);
        };
        let (result, changed) = wal.borrow_mut().begin_read_tx()?;
        if changed {
            // Someone else changed the database -> assume our page cache is invalid (this is default SQLite behavior, we can probably do better with more granular invalidation)
            self.clear_page_cache();
        }
        Ok(result)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn maybe_allocate_page1(&self) -> Result<IOResult<()>> {
        if !self.db_state.is_initialized() {
            if let Ok(_lock) = self.init_lock.try_lock() {
                match (self.db_state.get(), self.allocating_page1()) {
                    // In case of being empty or (allocating and this connection is performing allocation) then allocate the first page
                    (DbState::Uninitialized, false) | (DbState::Initializing, true) => {
                        match self.allocate_page1()? {
                            IOResult::Done(_) => Ok(IOResult::Done(())),
                            IOResult::IO(io) => Ok(IOResult::IO(io)),
                        }
                    }
                    // Dummy completion to force a return IO
                    _ => Ok(IOResult::IO(IOCompletions::Single(Completion::new_dummy()))),
                }
            } else {
                Ok(IOResult::IO(IOCompletions::Single(Completion::new_dummy())))
            }
        } else {
            Ok(IOResult::Done(()))
        }
    }

    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_write_tx(&self) -> Result<IOResult<LimboResult>> {
        // TODO(Diego): The only possibly allocate page1 here is because OpenEphemeral needs a write transaction
        // we should have a unique API to begin transactions, something like sqlite3BtreeBeginTrans
        return_if_io!(self.maybe_allocate_page1());
        let Some(wal) = self.wal.as_ref() else {
            return Ok(IOResult::Done(LimboResult::Ok));
        };
        Ok(IOResult::Done(wal.borrow_mut().begin_write_tx()?))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn end_tx(
        &self,
        rollback: bool,
        schema_did_change: bool,
        connection: &Connection,
        wal_checkpoint_disabled: bool,
    ) -> Result<IOResult<PagerCommitResult>> {
        tracing::trace!("end_tx(rollback={})", rollback);
        let Some(wal) = self.wal.as_ref() else {
            // TODO: Unsure what the semantics of "end_tx" is for in-memory databases, ephemeral tables and ephemeral indexes.
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        };
        if rollback {
            let is_write = matches!(
                connection.transaction_state.get(),
                TransactionState::Write { .. }
            );
            if is_write {
                wal.borrow().end_write_tx();
            }
            wal.borrow().end_read_tx();
            self.rollback(schema_did_change, connection, is_write)?;
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        }
        let commit_status = self.commit_dirty_pages(wal_checkpoint_disabled)?;
        match commit_status {
            IOResult::IO(io) => Ok(IOResult::IO(io)),
            IOResult::Done(_) => {
                wal.borrow().end_write_tx();
                wal.borrow().end_read_tx();

                if schema_did_change {
                    let schema = connection.schema.borrow().clone();
                    connection._db.update_schema_if_newer(schema)?;
                }
                Ok(commit_status)
            }
        }
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
        page_idx: usize,
        frame_watermark: Option<u64>,
        allow_empty_read: bool,
    ) -> Result<(PageRef, Completion)> {
        tracing::trace!("read_page_no_cache(page_idx = {})", page_idx);
        let page = Arc::new(Page::new(page_idx));
        page.set_locked();

        let Some(wal) = self.wal.as_ref() else {
            turso_assert!(
                matches!(frame_watermark, Some(0) | None),
                "frame_watermark must be either None or Some(0) because DB has no WAL and read with other watermark is invalid"
            );
            let c = self.begin_read_disk_page(page_idx, page.clone(), allow_empty_read)?;
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

        let c = self.begin_read_disk_page(page_idx, page.clone(), allow_empty_read)?;
        Ok((page, c))
    }

    /// Reads a page from the database.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub fn read_page(&self, page_idx: usize) -> Result<(PageRef, Completion)> {
        tracing::trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx);
        if let Some(page) = page_cache.get(&page_key) {
            tracing::trace!("read_page(page_idx = {}) = cached", page_idx);
            // Dummy completion being passed, as we do not need to read from database or wal
            return Ok((page.clone(), Completion::new_dummy()));
        }
        let (page, c) = self.read_page_no_cache(page_idx, None, false)?;
        self.cache_insert(page_idx, page.clone(), &mut page_cache)?;
        Ok((page, c))
    }

    fn begin_read_disk_page(
        &self,
        page_idx: usize,
        page: PageRef,
        allow_empty_read: bool,
    ) -> Result<Completion> {
        sqlite3_ondisk::begin_read_page(
            self.db_file.clone(),
            self.buffer_pool.clone(),
            page,
            page_idx,
            allow_empty_read,
        )
    }

    fn cache_insert(
        &self,
        page_idx: usize,
        page: PageRef,
        page_cache: &mut DumbLruPageCache,
    ) -> Result<()> {
        let page_key = PageCacheKey::new(page_idx);
        match page_cache.insert(page_key, page.clone()) {
            Ok(_) => {}
            Err(CacheError::Full) => return Err(LimboError::CacheFull),
            Err(CacheError::KeyExists) => {
                unreachable!("Page should not exist in cache after get() miss")
            }
            Err(e) => {
                return Err(LimboError::InternalError(format!(
                    "Failed to insert page into cache: {e:?}"
                )))
            }
        }
        Ok(())
    }

    // Get a page from the cache, if it exists.
    pub fn cache_get(&self, page_idx: usize) -> Option<PageRef> {
        tracing::trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx);
        page_cache.get(&page_key)
    }

    /// Changes the size of the page cache.
    pub fn change_page_cache_size(&self, capacity: usize) -> Result<CacheResizeResult> {
        let mut page_cache = self.page_cache.write();
        Ok(page_cache.resize(capacity))
    }

    pub fn add_dirty(&self, page: &Page) {
        // TODO: check duplicates?
        let mut dirty_pages = RefCell::borrow_mut(&self.dirty_pages);
        dirty_pages.insert(page.get().id);
        page.set_dirty();
    }

    pub fn wal_frame_count(&self) -> Result<u64> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_frame_count() called on database without WAL".to_string(),
            ));
        };
        Ok(wal.borrow().get_max_frame())
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
        let mut completions = Vec::with_capacity(self.dirty_pages.borrow().len());
        for page_id in self.dirty_pages.borrow().iter().copied() {
            let page = {
                let mut cache = self.page_cache.write();
                let page_key = PageCacheKey::new(page_id);
                let page = cache.get(&page_key).expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                let page_type = page.get().contents.as_ref().unwrap().maybe_page_type();
                trace!(
                    "commit_dirty_pages(page={}, page_type={:?}",
                    page_id,
                    page_type
                );
                page
            };

            // TODO: invalidade previous completions on error here
            let c = wal.borrow_mut().append_frame(page.clone(), 0)?;
            completions.push(c);
        }
        // No need to have a separate state for clearing dirty the page as the callback does this for us
        Ok(completions)
    }

    /// Flush all dirty pages to disk.
    /// In the base case, it will write the dirty pages to the WAL and then fsync the WAL.
    /// If the WAL size is over the checkpoint threshold, it will checkpoint the WAL to
    /// the database file and then fsync the database file.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn commit_dirty_pages(
        &self,
        wal_checkpoint_disabled: bool,
    ) -> Result<IOResult<PagerCommitResult>> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "commit_dirty_pages() called on database without WAL".to_string(),
            ));
        };
        let mut checkpoint_result = CheckpointResult::default();
        let res = loop {
            let state = self.commit_info.borrow().state;
            trace!(?state);
            match state {
                CommitState::Start => {
                    let db_size = {
                        self.io
                            .block(|| self.with_header(|header| header.database_size))?
                            .get()
                    };
                    let dirty_len = self.dirty_pages.borrow().iter().len();
                    let mut completions = Vec::with_capacity(dirty_len);
                    for (curr_page_idx, page_id) in
                        self.dirty_pages.borrow().iter().copied().enumerate()
                    {
                        let is_last_frame = curr_page_idx == dirty_len - 1;

                        let db_size = if is_last_frame { db_size } else { 0 };

                        let page = {
                            let mut cache = self.page_cache.write();
                            let page_key = PageCacheKey::new(page_id);
                            let page = cache.get(&page_key).unwrap_or_else(|| {
                                panic!(
                                    "we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it. page={page_id}"
                                )
                            });
                            let page_type = page.get().contents.as_ref().unwrap().maybe_page_type();
                            trace!(
                                "commit_dirty_pages(page={}, page_type={:?}",
                                page_id,
                                page_type
                            );
                            page
                        };

                        // TODO: invalidade previous completions on error here
                        let c = wal.borrow_mut().append_frame(page.clone(), db_size)?;
                        completions.push(c);
                    }
                    self.dirty_pages.borrow_mut().clear();
                    // Nothing to append
                    if completions.is_empty() {
                        return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                    } else {
                        self.commit_info.borrow_mut().state = CommitState::SyncWal;
                        return Ok(IOResult::IO(IOCompletions::Many(completions)));
                    }
                }
                CommitState::SyncWal => {
                    self.commit_info.borrow_mut().state = CommitState::AfterSyncWal;
                    let c = wal.borrow_mut().sync()?;
                    return Ok(IOResult::IO(IOCompletions::Single(c)));
                }
                CommitState::AfterSyncWal => {
                    if wal_checkpoint_disabled || !wal.borrow().should_checkpoint() {
                        self.commit_info.borrow_mut().state = CommitState::Start;
                        break PagerCommitResult::WalWritten;
                    }
                    self.commit_info.borrow_mut().state = CommitState::Checkpoint;
                }
                CommitState::Checkpoint => {
                    checkpoint_result = return_if_io!(self.checkpoint());
                    self.commit_info.borrow_mut().state = CommitState::SyncDbFile;
                }
                CommitState::SyncDbFile => {
                    let c = sqlite3_ondisk::begin_sync(self.db_file.clone())?;
                    self.commit_info.borrow_mut().state = CommitState::AfterSyncDbFile;
                    return Ok(IOResult::IO(IOCompletions::Single(c)));
                }
                CommitState::AfterSyncDbFile => {
                    self.commit_info.borrow_mut().state = CommitState::Start;
                    break PagerCommitResult::Checkpointed(checkpoint_result);
                }
            }
        };
        // We should only signal that we finished appending frames after wal sync to avoid inconsistencies when sync fails
        wal.borrow_mut().finish_append_frames_commit()?;
        Ok(IOResult::Done(res))
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
        let mut wal = wal.borrow_mut();
        let (header, raw_page) = parse_wal_frame_header(frame);
        wal.write_frame_raw(
            self.buffer_pool.clone(),
            frame_no,
            header.page_number as u64,
            header.db_size as u64,
            raw_page,
        )?;
        if let Some(page) = self.cache_get(header.page_number as usize) {
            let content = page.get_contents();
            content.as_ptr().copy_from_slice(raw_page);
            turso_assert!(
                page.get().id == header.page_number as usize,
                "page has unexpected id"
            );
            self.add_dirty(&page);
        }
        if header.is_commit_frame() {
            for page_id in self.dirty_pages.borrow().iter() {
                let page_key = PageCacheKey::new(*page_id);
                let mut cache = self.page_cache.write();
                let page = cache.get(&page_key).expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                page.clear_dirty();
            }
            self.dirty_pages.borrow_mut().clear();
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
        let mut checkpoint_result = CheckpointResult::default();
        loop {
            let state = *self.checkpoint_state.borrow();
            trace!(?state);
            match state {
                CheckpointState::Checkpoint => {
                    let in_flight = self.checkpoint_inflight.clone();
                    match wal
                        .borrow_mut()
                        .checkpoint(self, in_flight, CheckpointMode::Passive)?
                    {
                        IOResult::IO => return Ok(IOResult::IO),
                        IOResult::Done(res) => {
                            checkpoint_result = res;
                            self.checkpoint_state.replace(CheckpointState::SyncDbFile);
                        }
                    };
                }
                CheckpointState::SyncDbFile => {
                    let _c =
                        sqlite3_ondisk::begin_sync(self.db_file.clone(), self.syncing.clone())?;
                    self.checkpoint_state
                        .replace(CheckpointState::WaitSyncDbFile);
                }
                CheckpointState::WaitSyncDbFile => {
                    if *self.syncing.borrow() {
                        return Ok(IOResult::IO);
                    } else {
                        self.checkpoint_state
                            .replace(CheckpointState::CheckpointDone);
                    }
                }
                CheckpointState::CheckpointDone => {
                    return if *self.checkpoint_inflight.borrow() > 0 {
                        Ok(IOResult::IO)
                    } else {
                        self.checkpoint_state.replace(CheckpointState::Checkpoint);
                        Ok(IOResult::Done(checkpoint_result))
                    };
                }
            }
        }
    }

    /// Invalidates entire page cache by removing all dirty and clean pages. Usually used in case
    /// of a rollback or in case we want to invalidate page cache after starting a read transaction
    /// right after new writes happened which would invalidate current page cache.
    pub fn clear_page_cache(&self) {
        self.dirty_pages.borrow_mut().clear();
        self.page_cache.write().unset_dirty_all_pages();
        self.page_cache
            .write()
            .clear()
            .expect("Failed to clear page cache");
    }

    pub fn checkpoint_shutdown(&self, wal_checkpoint_disabled: bool) -> Result<()> {
        let mut _attempts = 0;
        {
            let Some(wal) = self.wal.as_ref() else {
                return Err(LimboError::InternalError(
                    "checkpoint_shutdown() called on database without WAL".to_string(),
                ));
            };
            let mut wal = wal.borrow_mut();
            // fsync the wal syncronously before beginning checkpoint
            let c = wal.sync()?;
            // TODO: for now forget about timeouts as they fail regularly in SIM
            // need to think of a better way to do this
            self.io.wait_for_completion(c)?;
        }
        self.wal_checkpoint(wal_checkpoint_disabled, CheckpointMode::Passive)?;
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_checkpoint(
        &self,
        wal_checkpoint_disabled: bool,
        mode: CheckpointMode,
    ) -> Result<CheckpointResult> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(LimboError::InternalError(
                "wal_checkpoint() called on database without WAL".to_string(),
            ));
        };
        if wal_checkpoint_disabled {
            return Ok(CheckpointResult::default());
        }

        let write_counter = Rc::new(RefCell::new(0));
        let mut checkpoint_result = self.io.block(|| {
            wal.borrow_mut()
                .checkpoint(self, write_counter.clone(), mode)
        })?;

        if checkpoint_result.everything_backfilled()
            && checkpoint_result.num_checkpointed_frames != 0
        {
            let db_size = self
                .io
                .block(|| self.with_header(|header| header.database_size))?
                .get();
            let page_size = self.page_size.get().unwrap_or(PageSize::DEFAULT as u32);
            let expected = (db_size * page_size) as u64;
            if expected < self.db_file.size()? {
                self.io.wait_for_completion(self.db_file.truncate(
                    expected as usize,
                    Completion::new_trunc(move |_| {
                        tracing::trace!(
                            "Database file truncated to expected size: {} bytes",
                            expected
                        );
                    }),
                )?)?;
                self.io
                    .wait_for_completion(self.db_file.sync(Completion::new_sync(move |_| {
                        tracing::trace!("Database file syncd after truncation");
                    }))?)?;
            }
            checkpoint_result.release_guard();
        }

        // TODO: only clear cache of things that are really invalidated
        self.page_cache
            .write()
            .clear()
            .map_err(|e| LimboError::InternalError(format!("Failed to clear page cache: {e:?}")))?;
        Ok(checkpoint_result)
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

        let mut state = self.free_page_state.borrow_mut();
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
                            let (page, c) = self.read_page(page_id)?;
                            (page, Some(c))
                        }
                    };
                    header.freelist_pages = (header.freelist_pages.get() + 1).into();

                    let trunk_page_id = header.freelist_trunk_page.get();

                    if trunk_page_id != 0 {
                        *state = FreePageState::AddToTrunk {
                            page,
                            trunk_page: None,
                        };
                    } else {
                        *state = FreePageState::NewTrunk { page };
                    }
                }
                FreePageState::AddToTrunk { page, trunk_page } => {
                    let trunk_page_id = header.freelist_trunk_page.get();
                    if trunk_page.is_none() {
                        // Add as leaf to current trunk
                        let (page, _c) = self.read_page(trunk_page_id as usize)?;
                        trunk_page.replace(page);
                        return Ok(IOResult::IO);
                    }
                    let trunk_page = trunk_page.as_ref().unwrap();
                    if trunk_page.is_locked() || !trunk_page.is_loaded() {
                        return Ok(IOResult::IO);
                    }

                    let trunk_page_contents = trunk_page.get().contents.as_ref().unwrap();
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
                        self.add_dirty(trunk_page);

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
                    if page.is_locked() || !page.is_loaded() {
                        return Ok(IOResult::IO);
                    }
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
        let state = self.allocate_page1_state.borrow().clone();
        match state {
            AllocatePage1State::Start => {
                tracing::trace!("allocate_page1(Start)");
                self.db_state.set(DbState::Initializing);
                let mut default_header = DatabaseHeader::default();

                assert_eq!(default_header.database_size.get(), 0);
                default_header.database_size = 1.into();

                if let Some(size) = self.page_size.get() {
                    default_header.page_size = PageSize::new(size).expect("page size");
                }
                self.buffer_pool
                    .finalize_with_page_size(default_header.page_size.get() as usize)?;
                let page = allocate_new_page(1, &self.buffer_pool, 0);

                let contents = page.get_contents();
                contents.write_database_header(&default_header);

                let page1 = Arc::new(BTreePageInner {
                    page: RefCell::new(page),
                });
                // Create the sqlite_schema table, for this we just need to create the btree page
                // for the first page of the database which is basically like any other btree page
                // but with a 100 byte offset, so we just init the page so that sqlite understands
                // this is a correct page.
                btree_init_page(
                    &page1,
                    PageType::TableLeaf,
                    DatabaseHeader::SIZE,
                    (default_header.page_size.get() - default_header.reserved_space as u32) as u16,
                );
                let write_counter = Rc::new(RefCell::new(0));
                let _c = begin_write_btree_page(self, &page1.get(), write_counter.clone())?;

                self.allocate_page1_state
                    .replace(AllocatePage1State::Writing {
                        write_counter,
                        page: page1,
                    });
                Ok(IOResult::IO)
            }
            AllocatePage1State::Writing {
                write_counter,
                page,
            } => {
                tracing::trace!("allocate_page1(Writing)");
                if *write_counter.borrow() > 0 {
                    return Ok(IOResult::IO);
                }
                tracing::trace!("allocate_page1(Writing done)");
                let page1_ref = page.get();
                let page_key = PageCacheKey::new(page1_ref.get().id);
                let mut cache = self.page_cache.write();
                cache.insert(page_key, page1_ref.clone()).map_err(|e| {
                    LimboError::InternalError(format!("Failed to insert page 1 into cache: {e:?}"))
                })?;
                self.db_state.set(DbState::Initialized);
                self.allocate_page1_state.replace(AllocatePage1State::Done);
                Ok(IOResult::Done(page1_ref.clone()))
            }
            AllocatePage1State::Done => unreachable!("cannot try to allocate page 1 again"),
        }
    }

    pub fn allocating_page1(&self) -> bool {
        matches!(
            *self.allocate_page1_state.borrow(),
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
            let mut state = self.allocate_page_state.borrow_mut();
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
                        if matches!(*self.auto_vacuum_mode.borrow(), AutoVacuumMode::Full)
                            && is_ptrmap_page(new_db_size + 1, header.page_size.get() as usize)
                        {
                            // we will allocate a ptrmap page, so increment size
                            new_db_size += 1;
                            let page =
                                allocate_new_page(new_db_size as usize, &self.buffer_pool, 0);
                            self.add_dirty(&page);
                            let page_key = PageCacheKey::new(page.get().id);
                            let mut cache = self.page_cache.write();
                            match cache.insert(page_key, page.clone()) {
                                Ok(_) => (),
                                Err(CacheError::Full) => return Err(LimboError::CacheFull),
                                Err(_) => {
                                    return Err(LimboError::InternalError(
                                        "Unknown error inserting page to cache".into(),
                                    ))
                                }
                            }
                        }
                    }

                    let first_freelist_trunk_page_id = header.freelist_trunk_page.get();
                    if first_freelist_trunk_page_id == 0 {
                        *state = AllocatePageState::AllocateNewPage {
                            current_db_size: new_db_size,
                        };
                        continue;
                    }
                    let (trunk_page, _c) = self.read_page(first_freelist_trunk_page_id as usize)?;
                    *state = AllocatePageState::SearchAvailableFreeListLeaf {
                        trunk_page,
                        current_db_size: new_db_size,
                    };
                    return Ok(IOResult::IO);
                }
                AllocatePageState::SearchAvailableFreeListLeaf {
                    trunk_page,
                    current_db_size,
                } => {
                    if trunk_page.is_locked() {
                        return Ok(IOResult::IO);
                    }
                    turso_assert!(
                        trunk_page.is_loaded(),
                        "Freelist trunk page {} is not loaded",
                        trunk_page.get().id
                    );
                    let page_contents = trunk_page.get().contents.as_ref().unwrap();
                    let next_trunk_page_id =
                        page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_NEXT_TRUNK);
                    let number_of_freelist_leaves =
                        page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_LEAF_COUNT);

                    // There are leaf pointers on this trunk page, so we can reuse one of the pages
                    // for the allocation.
                    if number_of_freelist_leaves != 0 {
                        *state = AllocatePageState::ReuseFreelistLeaf {
                            trunk_page: trunk_page.clone(),
                            number_of_freelist_leaves,
                        };
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
                    header.freelist_pages = (header.freelist_pages.get() + 1).into();
                    self.add_dirty(trunk_page);
                    // zero out the page
                    turso_assert!(
                        trunk_page.get_contents().overflow_cells.is_empty(),
                        "Freelist leaf page {} has overflow cells",
                        trunk_page.get().id
                    );
                    trunk_page.get().contents.as_ref().unwrap().as_ptr().fill(0);
                    let page_key = PageCacheKey::new(trunk_page.get().id);
                    {
                        let mut page_cache = self.page_cache.write();
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
                    number_of_freelist_leaves,
                } => {
                    turso_assert!(
                        trunk_page.is_loaded(),
                        "Freelist trunk page {} is not loaded",
                        trunk_page.get().id
                    );
                    turso_assert!(
                        *number_of_freelist_leaves > 0,
                        "Freelist trunk page {} has no leaves",
                        trunk_page.get().id
                    );
                    let page_contents = trunk_page.get().contents.as_ref().unwrap();
                    let next_leaf_page_id =
                        page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_FIRST_LEAF);
                    let (leaf_page, _c) = self.read_page(next_leaf_page_id as usize)?;
                    if leaf_page.is_locked() {
                        return Ok(IOResult::IO);
                    }
                    self.add_dirty(&leaf_page);
                    // zero out the page
                    turso_assert!(
                        leaf_page.get_contents().overflow_cells.is_empty(),
                        "Freelist leaf page {} has overflow cells",
                        leaf_page.get().id
                    );
                    leaf_page.get().contents.as_ref().unwrap().as_ptr().fill(0);
                    let page_key = PageCacheKey::new(leaf_page.get().id);
                    {
                        let mut page_cache = self.page_cache.write();
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
                    let page = allocate_new_page(new_db_size as usize, &self.buffer_pool, 0);
                    {
                        // setup page and add to cache
                        self.add_dirty(&page);

                        let page_key = PageCacheKey::new(page.get().id);
                        {
                            // Run in separate block to avoid deadlock on page cache write lock
                            let mut cache = self.page_cache.write();
                            match cache.insert(page_key, page.clone()) {
                                Err(CacheError::Full) => return Err(LimboError::CacheFull),
                                Err(_) => {
                                    return Err(LimboError::InternalError(
                                        "Unknown error inserting page to cache".into(),
                                    ))
                                }
                                Ok(_) => {}
                            };
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
        cache
            .insert_ignore_existing(page_key, page.clone())
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "Failed to insert loaded page {id} into cache: {e:?}"
                ))
            })?;
        page.set_loaded();
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
        if is_write {
            self.dirty_pages.borrow_mut().clear();
        } else {
            turso_assert!(
                self.dirty_pages.borrow().is_empty(),
                "dirty pages should be empty for read txn"
            );
        }
        let mut cache = self.page_cache.write();

        self.reset_internal_states();

        cache.unset_dirty_all_pages();
        cache.clear().expect("failed to clear page cache");
        if schema_did_change {
            connection.schema.replace(connection._db.clone_schema()?);
        }
        if is_write {
            if let Some(wal) = self.wal.as_ref() {
                wal.borrow_mut().rollback()?;
            }
        }

        Ok(())
    }

    fn reset_internal_states(&self) {
        self.checkpoint_state.replace(CheckpointState::Checkpoint);
        self.checkpoint_inflight.replace(0);
        self.syncing.replace(false);
        self.commit_info.replace(CommitInfo {
            state: CommitState::Start,
            in_flight_writes: Rc::new(RefCell::new(0)),
            dirty_pages: Vec::new(),
        });
        self.allocate_page_state.replace(AllocatePageState::Start);
    }

    pub fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> Result<IOResult<T>> {
        let IOResult::Done(header_ref) = HeaderRef::from_pager(self)? else {
            return Ok(IOResult::IO);
        };
        let header = header_ref.borrow();
        Ok(IOResult::Done(f(header)))
    }

    pub fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<IOResult<T>> {
        let IOResult::Done(header_ref) = HeaderRefMut::from_pager(self)? else {
            return Ok(IOResult::IO);
        };
        let header = header_ref.borrow_mut();
        Ok(IOResult::Done(f(header)))
    }
}

pub fn allocate_new_page(page_id: usize, buffer_pool: &Arc<BufferPool>, offset: usize) -> PageRef {
    let page = Arc::new(Page::new(page_id));
    {
        let buffer = buffer_pool.get_page();
        let buffer = Arc::new(buffer);
        page.set_loaded();
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

    use crate::storage::page_cache::{DumbLruPageCache, PageCacheKey};

    use super::Page;

    #[test]
    fn test_shared_cache() {
        // ensure cache can be shared between threads
        let cache = Arc::new(RwLock::new(DumbLruPageCache::new(10)));

        let thread = {
            let cache = cache.clone();
            std::thread::spawn(move || {
                let mut cache = cache.write();
                let page_key = PageCacheKey::new(1);
                cache.insert(page_key, Arc::new(Page::new(1))).unwrap();
            })
        };
        let _ = thread.join();
        let mut cache = cache.write();
        let page_key = PageCacheKey::new(1);
        let page = cache.get(&page_key);
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
    use crate::storage::page_cache::DumbLruPageCache;
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
                IOResult::IO => pager.io.run_once().unwrap(),
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
        let page_cache = Arc::new(RwLock::new(DumbLruPageCache::new(sz as usize)));

        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            WalFileShared::new_shared(
                page_size,
                &io,
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
        let ptrmap_page_ref = pager.read_page(expected_ptrmap_pg_no as usize);
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
