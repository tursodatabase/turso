use crate::result::LimboResult;
use crate::storage::btree::BTreePageInner;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::header_accessor;
use crate::storage::sqlite3_ondisk::{
    self, parse_wal_frame_header, DatabaseHeader, PageContent, PageType,
};
use crate::storage::wal::{CheckpointResult, Wal};
use crate::types::{Cursor, IOResult, WalInsertInfo};
use crate::util::IOExt as _;
use crate::{return_if_io, Completion};
use crate::{turso_assert, Buffer, Connection, LimboError, Result};
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
use super::sqlite3_ondisk::{begin_write_btree_page, DATABASE_HEADER_SIZE};
use super::wal::CheckpointMode;

#[cfg(not(feature = "omit_autovacuum"))]
use {crate::io::Buffer as IoBuffer, ptrmap::*};

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

/// Page is up-to-date.
const PAGE_UPTODATE: usize = 0b001;
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

    pub fn is_uptodate(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_UPTODATE != 0
    }

    pub fn set_uptodate(&self) {
        self.get().flags.fetch_or(PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn clear_uptodate(&self) {
        self.get().flags.fetch_and(!PAGE_UPTODATE, Ordering::SeqCst);
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
/// The state of the current pager cache flush.
enum CacheFlushState {
    /// Idle.
    Start,
    /// Append a single frame to the WAL.
    AppendFrame { current_page_to_append_idx: usize },
    /// Wait for append frame to complete.
    WaitAppendFrame { current_page_to_append_idx: usize },
}

#[derive(Clone, Copy, Debug)]
/// The state of the current pager cache commit.
enum CommitState {
    /// Idle.
    Start,
    /// Append a single frame to the WAL.
    AppendFrame { current_page_to_append_idx: usize },
    /// Wait for append frame to complete.
    /// If the current page is the last page to append, sync wal and clear dirty pages and cache.
    WaitAppendFrame { current_page_to_append_idx: usize },
    /// Fsync the on-disk WAL.
    SyncWal,
    /// Checkpoint the WAL to the database file (if needed).
    Checkpoint,
    /// Fsync the database file.
    SyncDbFile,
    /// Waiting for the database file to be fsynced.
    WaitSyncDbFile,
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

/// This will keep track of the state of current cache flush in order to not repeat work
struct FlushInfo {
    state: CacheFlushState,
    /// Number of writes taking place.
    in_flight_writes: Rc<RefCell<usize>>,
    /// Dirty pages to be flushed.
    dirty_pages: Vec<usize>,
}

/// Track the state of the auto-vacuum mode.
#[derive(Clone, Copy, Debug, PartialEq)]
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

/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    /// Source of the database pages.
    pub db_file: Arc<dyn DatabaseStorage>,
    /// The write-ahead log (WAL) for the database.
    pub(crate) wal: Rc<RefCell<dyn Wal>>,
    /// A page cache for the database.
    page_cache: Arc<RwLock<DumbLruPageCache>>,
    /// Buffer pool for temporary data storage.
    pub buffer_pool: Arc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    /// Indicates whether the database is running as transient or persistent
    #[cfg_attr(feature = "omit_autovacuum", allow(dead_code))]
    database_mode: DatabaseMode,

    dirty_pages: Rc<RefCell<HashSet<usize, hash::BuildHasherDefault<hash::DefaultHasher>>>>,

    commit_info: RefCell<CommitInfo>,
    flush_info: RefCell<FlushInfo>,
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
    page_size: Cell<Option<u32>>,
    reserved_space: OnceCell<u8>,
    free_page_state: RefCell<FreePageState>,
}

#[derive(Debug, Copy, Clone)]
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Rc<RefCell<dyn Wal>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<DumbLruPageCache>>,
        buffer_pool: Arc<BufferPool>,
        db_state: Arc<AtomicDbState>,
        init_lock: Arc<Mutex<()>>,
        database_mode: DatabaseMode,
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
            flush_info: RefCell::new(FlushInfo {
                state: CacheFlushState::Start,
                in_flight_writes: Rc::new(RefCell::new(0)),
                dirty_pages: Vec::new(),
            }),
            free_page_state: RefCell::new(FreePageState::Start),
            allocate_page_state: RefCell::new(AllocatePageState::Start),
        })
    }

    pub fn set_wal(&mut self, wal: Rc<RefCell<dyn Wal>>) {
        self.wal = wal;
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
        tracing::trace!("ptrmap_get(page_idx = {})", target_page_num);
        let configured_page_size = match header_accessor::get_page_size_async(self)? {
            IOResult::Done(size) => size as usize,
            IOResult::IO => return Ok(IOResult::IO),
        };

        if target_page_num < FIRST_PTRMAP_PAGE_NO
            || is_ptrmap_page(target_page_num, configured_page_size)
        {
            return Ok(IOResult::Done(None));
        }

        let ptrmap_pg_no = get_ptrmap_page_no_for_db_page(target_page_num, configured_page_size);
        let offset_in_ptrmap_page =
            get_ptrmap_offset_in_page(target_page_num, ptrmap_pg_no, configured_page_size)?;
        tracing::trace!(
            "ptrmap_get(page_idx = {}) = ptrmap_pg_no = {}",
            target_page_num,
            ptrmap_pg_no
        );

        let ptrmap_page = self.read_page(ptrmap_pg_no as usize)?;
        if ptrmap_page.is_locked() {
            return Ok(IOResult::IO);
        }
        if !ptrmap_page.is_loaded() {
            return Ok(IOResult::IO);
        }
        let ptrmap_page_inner = ptrmap_page.get();

        let page_content: &PageContent = match ptrmap_page_inner.contents.as_ref() {
            Some(content) => content,
            None => {
                return Err(LimboError::InternalError(format!(
                    "Ptrmap page {ptrmap_pg_no} content not loaded"
                )))
            }
        };

        let page_buffer_guard: std::cell::Ref<IoBuffer> = page_content.buffer.borrow();
        let full_buffer_slice: &[u8] = page_buffer_guard.as_slice();

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
        match PtrmapEntry::deserialize(entry_slice) {
            Some(entry) => Ok(IOResult::Done(Some(entry))),
            None => Err(LimboError::Corrupt(format!(
                "Failed to deserialize ptrmap entry for page {target_page_num} from ptrmap page {ptrmap_pg_no}"
            ))),
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

        let page_size = match header_accessor::get_page_size_async(self)? {
            IOResult::Done(size) => size as usize,
            IOResult::IO => return Ok(IOResult::IO),
        };

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

        let ptrmap_page = self.read_page(ptrmap_pg_no as usize)?;
        if ptrmap_page.is_locked() {
            return Ok(IOResult::IO);
        }
        if !ptrmap_page.is_loaded() {
            return Ok(IOResult::IO);
        }
        let ptrmap_page_inner = ptrmap_page.get();

        let page_content = match ptrmap_page_inner.contents.as_ref() {
            Some(content) => content,
            None => {
                return Err(LimboError::InternalError(format!(
                    "Ptrmap page {ptrmap_pg_no} content not loaded"
                )))
            }
        };

        let mut page_buffer_guard = page_content.buffer.borrow_mut();
        let full_buffer_slice = page_buffer_guard.as_mut_slice();

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
            ptrmap_page.get().id == ptrmap_pg_no as usize,
            "ptrmap page has unexpected number"
        );
        self.add_dirty(&ptrmap_page);
        Ok(IOResult::Done(()))
    }

    /// This method is used to allocate a new root page for a btree, both for tables and indexes
    /// FIXME: handle no room in page cache
    #[instrument(skip_all, level = Level::DEBUG)]
    #[cfg_attr(feature = "omit_autovacuum", allow(unused_variables))]
    pub fn btree_create(
        &self,
        flags: &CreateBTreeFlags,
        cursors: &mut [Option<Cursor>],
    ) -> Result<IOResult<u32>> {
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
                    let mut root_page_num =
                        match header_accessor::get_vacuum_mode_largest_root_page_async(self)? {
                            IOResult::Done(value) => value,
                            IOResult::IO => return Ok(IOResult::IO),
                        };
                    assert!(root_page_num > 0); //  Largest root page number cannot be 0 because that is set to 1 when creating the database with autovacuum enabled
                    root_page_num += 1;
                    assert!(root_page_num >= FIRST_PTRMAP_PAGE_NO); //  can never be less than 2 because we have already incremented

                    let page_size = match header_accessor::get_page_size_async(self)? {
                        IOResult::Done(size) => size as usize,
                        IOResult::IO => return Ok(IOResult::IO),
                    };

                    while is_ptrmap_page(root_page_num, page_size) {
                        root_page_num += 1;
                    }
                    assert!(root_page_num >= 3); //  the very first root page is page 3

                    //  root_page_num here is the desired root page
                    let page = return_if_io!(self.do_allocate_page(
                        page_type,
                        0,
                        BtreePageAllocMode::Exact(root_page_num),
                    ));
                    let allocated_page_id = page.get().get().id as u32;
                    drop(page);

                    //  We want to be allocated root_page_num but we got allocated allocated_page_id. We need to swap these two pages so that
                    //  root pages remain clustered at the beginning of the file
                    if allocated_page_id != root_page_num {
                        //  save all the btree cursors so that all page references are freed
                        for cursor in cursors {
                            if let Some(cursor) = cursor {
                                use crate::storage::btree::SaveCursorsMode;

                                let Cursor::BTree(c) = cursor else {
                                    continue;
                                };
                                c.save_context_external_invalidation(
                                    SaveCursorsMode::SaveAllCursors,
                                )?;
                            }
                        }

                        let ptrmap_entry = match self.ptrmap_get(root_page_num)? {
                            CursorResult::Ok(result) => match result {
                                Some(entry) => entry,
                                None => panic!("something went wrong"),
                            },
                            CursorResult::IO => return Ok(CursorResult::IO),
                        };

                        //  These conditions shouldn't be possible because:
                        //  1. If the root_page_num is a RootPage then it should have already been allocated and vacuum_mode_largest_root_page should have been updated appropriately. Something is broken in the header
                        //  2. If the root_page_num is a FreePage, then it should have been allocated by `allocate_page` (this doesn't currently hold since that function doesn't look at the free list)
                        assert_ne!(ptrmap_entry.entry_type, PtrmapType::RootPage);
                        assert_ne!(ptrmap_entry.entry_type, PtrmapType::FreePage);

                        //  Relocate the data at root_page to allocated_page_id
                        let root_page = self.read_page(root_page_num as usize)?;
                        self.relocate_page(
                            root_page,
                            allocated_page_id,
                            ptrmap_entry,
                            header_accessor::get_database_size(&self)?,
                        )?;

                        //  Fetch the data at root_page_num
                        let root_page = self.read_page(root_page_num as usize)?;
                        btree_init_page(
                            &Arc::new(BTreePageInner {
                                page: RefCell::new(root_page),
                            }),
                            page_type,
                            0,
                            self.usable_space() as u16,
                        );
                    }

                    //  Update the header metadata to reflect the new root page number and map the new root page in pointer pages
                    header_accessor::set_vacuum_mode_largest_root_page(self, root_page_num)?;
                    match self.ptrmap_put(root_page_num, PtrmapType::RootPage, 0)? {
                        IOResult::Done(_) => Ok(IOResult::Done(root_page_num as u32)),
                        IOResult::IO => Ok(IOResult::IO),
                    }
                }
                AutoVacuumMode::Incremental => {
                    unimplemented!()
                }
            }
        }
    }

    /// This method is responsible for relocating source_page to location dest_page_num. This will also update all the relevant pointers
    /// in the pointer map pages and parent page after the move
    /// If the database is running in temporary mode, then the two pages are swapped by using a temporary page at the end of the file
    /// If the database is running in persistent mode, then the source page is remapped to the destination page and the latter is dropped
    #[cfg(not(feature = "omit_autovacuum"))]
    fn relocate_page(
        &self,
        source_page: Arc<Page>,
        dest_page_num: u32,
        source_page_ptr_map_entry: PtrmapEntry,
        database_size: u32,
    ) -> Result<()> {
        //  The source page can be one of the following pages
        assert!(
            source_page_ptr_map_entry.entry_type == PtrmapType::RootPage
                || source_page_ptr_map_entry.entry_type == PtrmapType::BTreeNode
                || source_page_ptr_map_entry.entry_type == PtrmapType::Overflow1
                || source_page_ptr_map_entry.entry_type == PtrmapType::Overflow2
        );
        assert!(source_page.get().id >= 3); //  No root page in autovacuum can have a root page less than 3

        let orig_source_page_num = {
            let mut cache = self.page_cache.write();
            let dest_key = PageCacheKey::new(dest_page_num as usize);
            let mut temp_page_opt: Option<u32> = None;

            //  Check if the dest_page_num is already present in the page cache
            if let Some(dest_page) = cache.peek(&dest_key, false) {
                //  The destination page already exists in the page cache
                //  Since we invalidated all cursors earlier, this page should no longer be referenced anywhere apart from here and the cache itself
                let ref_count = Arc::strong_count(&dest_page);
                assert!(
                    ref_count <= 2,
                    "Page {} is referenced outside of cache and relocate_page. Found {} ref counts",
                    dest_page_num,
                    ref_count
                );

                //  TODO(Zaid) This is a hack currently until the rollback journal and subjournal is implemented
                //  See this function from SQLite - https://github.com/sqlite/sqlite/blob/master/src/pager.c#L7141-L7270
                if dest_page.is_dirty() {
                    dest_page.clear_dirty();
                    self.dirty_pages
                        .borrow_mut()
                        .remove(&(dest_page_num as usize));
                }

                if matches!(self.database_mode, DatabaseMode::Memory) {
                    //  this is a temporary database, so do not discard the page if we need to rollback later
                    let temp_page_num = database_size + 1;
                    cache
                        .move_page(dest_page_num as usize, temp_page_num as usize)
                        .map_err(|e| LimboError::InternalError(format!("{:?}", e)))?;
                    temp_page_opt = Some(temp_page_num);
                } else {
                    //  this is a persistent database, so drop the entry from the cache
                    drop(dest_page);
                    cache
                        .delete(dest_key)
                        .map_err(|e| LimboError::InternalError(format!("{:?}", e)))?;
                }
            }

            let orig_source_page_num = source_page.get().id;
            cache
                .move_page(orig_source_page_num, dest_page_num as usize)
                .map_err(|e| LimboError::InternalError(format!("{:?}", e)))?;

            //  For the temporary database, associate the temporary page with the original source page number in the cache
            if matches!(self.database_mode, DatabaseMode::Memory) {
                if let Some(temp_page_num) = temp_page_opt {
                    cache
                        .move_page(temp_page_num as usize, orig_source_page_num)
                        .map_err(|e| LimboError::InternalError(format!("{:?}", e)))?;
                }
            }
            orig_source_page_num
        };

        //  Set the page number on the source page as the new page number and mark it dirty
        source_page.get().id = dest_page_num as usize;
        source_page.set_dirty();
        self.add_dirty(source_page.get().id);

        //  Update the pointer map pages for the source_page since the page id has changed
        match source_page_ptr_map_entry.entry_type {
            PtrmapType::RootPage => self.set_child_ptrmaps(source_page)?,
            PtrmapType::BTreeNode => self.set_child_ptrmaps(source_page)?,
            PtrmapType::Overflow1 => self.update_overflow_pointer(source_page)?,
            PtrmapType::Overflow2 => self.update_overflow_pointer(source_page)?,
            PtrmapType::FreePage => unreachable!(), //  this is safe since we assert above that the page cannot be a free page
        };

        //  For non-root pages update the pointers on the parent page and also update the pointer map of the source page
        //  to point to the destination page
        if source_page_ptr_map_entry.entry_type != PtrmapType::RootPage {
            let parent_page = self.read_page(source_page_ptr_map_entry.parent_page_no as usize)?;
            self.modify_parent_page_pointers(
                parent_page,
                orig_source_page_num,
                dest_page_num as usize,
                source_page_ptr_map_entry.entry_type,
            )?;
            self.ptrmap_put(
                dest_page_num,
                source_page_ptr_map_entry.entry_type,
                source_page_ptr_map_entry.parent_page_no,
            )?;
        }
        Ok(())
    }

    /// Update all the child pointer maps of this page to point to the new page number of this page
    /// Called after pages have been relocated
    #[cfg(not(feature = "omit_autovacuum"))]
    fn set_child_ptrmaps(&self, page: Arc<Page>) -> Result<CursorResult<()>> {
        let contents = page.get_contents();

        //  Iterate through all the children cells and update their pointer map entries
        for cell_idx in 0..contents.cell_count() {
            use crate::storage::{
                btree::{payload_overflow_threshold_max, payload_overflow_threshold_min},
                sqlite3_ondisk::BTreeCell,
            };

            let cell = contents.cell_get(
                cell_idx,
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;

            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    self.ptrmap_put(
                        table_interior_cell._left_child_page,
                        PtrmapType::BTreeNode,
                        page.get().id as u32,
                    )?;
                }
                BTreeCell::TableLeafCell(table_leaf_cell) => {
                    if let Some(overflow_page) = table_leaf_cell.first_overflow_page {
                        self.ptrmap_put(
                            overflow_page,
                            PtrmapType::Overflow1,
                            page.get().id as u32,
                        )?;
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    self.ptrmap_put(
                        index_interior_cell.left_child_page,
                        PtrmapType::BTreeNode,
                        page.get().id as u32,
                    )?;
                }
                BTreeCell::IndexLeafCell(index_leaf_cell) => {
                    if let Some(overflow_page) = index_leaf_cell.first_overflow_page {
                        self.ptrmap_put(
                            overflow_page,
                            PtrmapType::Overflow1,
                            page.get().id as u32,
                        )?;
                    }
                }
            }
        }

        if !contents.is_leaf() {
            if let Some(right_child) = contents.rightmost_pointer() {
                self.ptrmap_put(right_child, PtrmapType::BTreeNode, page.get().id as u32)?;
            }
        }
        Ok(CursorResult::Ok(()))
    }

    #[cfg(not(feature = "omit_autovacuum"))]
    /// The page is an overflow page so check if this page has a chained overflow pointer and update
    /// the pointer map entry over that chained overflow page
    fn update_overflow_pointer(&self, page: Arc<Page>) -> Result<CursorResult<()>> {
        let contents = page.get_contents();
        let overflow_ptr = contents.read_u32_no_offset(0);
        if overflow_ptr == 0 {
            return Ok(CursorResult::Ok(()));
        }
        self.ptrmap_put(overflow_ptr, PtrmapType::Overflow2, page.get().id as u32)
    }

    #[cfg(not(feature = "omit_autovacuum"))]
    /// This function takes a [Page] and finds the cell with a reference to prev_child_page_num and changes that to new_child_page_num
    /// Also accepts a [PtrmapType] parameter for some optimizations
    ///
    /// [PtrmapType::BTreeNode] -> the parent_page is a [BTreePage]. The pointer points to a child of this page
    /// [PtrmapType::Overflow1] -> the parent_page is a [BTreePage]. The pointer points to an overflow page pointed to by one of the cells of this page
    /// [PtrmapType::Overflow2] -> the parent_page is an overflow page. The pointer points at the next overflow page in the chain
    fn modify_parent_page_pointers(
        &self,
        parent_page: Arc<Page>,
        prev_child_page_num: usize,
        new_child_page_num: usize,
        ptrmap_type: PtrmapType,
    ) -> Result<()> {
        use crate::storage::btree::payload_overflow_threshold_max;
        use crate::storage::btree::payload_overflow_threshold_min;
        use crate::storage::sqlite3_ondisk::BTreeCell;

        let contents = parent_page.get_contents();

        //  This page is an overflow page which is not at the start of the overflow chain
        //  This means that the first 4 bytes of the parent page must contain the pointer to this page
        if ptrmap_type == PtrmapType::Overflow2 {
            let overflow_ptr = contents.read_u32_no_offset(0);
            assert_eq!(overflow_ptr, prev_child_page_num as u32);
            contents.write_u32(0, new_child_page_num as u32);
            parent_page.set_dirty();
            self.add_dirty(parent_page.get().id);
            return Ok(());
        }

        //  Iterate over all cells on this page and find the cell which points to the page number to modify
        for cell_idx in 0..contents.cell_count() {
            let cell = contents.cell_get(
                cell_idx,
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;
            let (cell_start, cell_len) = contents.cell_get_raw_region(
                cell_idx,
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            );

            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    if ptrmap_type == PtrmapType::BTreeNode
                        && table_interior_cell._left_child_page == prev_child_page_num as u32
                    {
                        contents.write_u32(cell_start, new_child_page_num as u32);
                        parent_page.set_dirty();
                        self.add_dirty(parent_page.get().id);
                        return Ok(());
                    }
                }
                BTreeCell::TableLeafCell(table_leaf_cell) => {
                    if ptrmap_type == PtrmapType::Overflow1 {
                        if let Some(overflow_page) = table_leaf_cell.first_overflow_page {
                            if overflow_page == prev_child_page_num as u32 {
                                contents.write_u32(
                                    cell_start + cell_len - 4,
                                    new_child_page_num as u32,
                                );
                                parent_page.set_dirty();
                                self.add_dirty(parent_page.get().id);
                                return Ok(());
                            }
                        }
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    if ptrmap_type == PtrmapType::Overflow1 {
                        if let Some(overflow_page) = index_interior_cell.first_overflow_page {
                            if overflow_page == prev_child_page_num as u32 {
                                contents.write_u32(
                                    cell_start + cell_len - 4,
                                    new_child_page_num as u32,
                                );
                                parent_page.set_dirty();
                                self.add_dirty(parent_page.get().id);
                                return Ok(());
                            }
                        }
                    } else if ptrmap_type == PtrmapType::BTreeNode
                        && index_interior_cell.left_child_page == prev_child_page_num as u32
                    {
                        contents.write_u32(cell_start, new_child_page_num as u32);
                        parent_page.set_dirty();
                        self.add_dirty(parent_page.get().id);
                        return Ok(());
                    }
                }
                BTreeCell::IndexLeafCell(index_leaf_cell) => {
                    if ptrmap_type == PtrmapType::Overflow1 {
                        if let Some(overflow_page) = index_leaf_cell.first_overflow_page {
                            if overflow_page == prev_child_page_num as u32 {
                                contents.write_u32(
                                    cell_start + cell_len - 4,
                                    new_child_page_num as u32,
                                );
                                parent_page.set_dirty();
                                self.add_dirty(parent_page.get().id);
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        //  If we go through all the cells and don't find the page number, then we need to update the rightmost pointer of this page
        //  The rightmost pointer has to exist in the page at this point and it must equal the page number we are looking for
        assert!(ptrmap_type == PtrmapType::BTreeNode);
        assert!(contents.rightmost_pointer_raw().is_some());
        let rightmost_ptr = contents.rightmost_pointer_raw().unwrap();
        unsafe {
            let slice = std::slice::from_raw_parts_mut(rightmost_ptr, 4);
            let current = u32::from_be_bytes(slice.try_into().unwrap());
            assert_eq!(current, prev_child_page_num as u32);
            slice.copy_from_slice(&(new_child_page_num as u32).to_be_bytes());
        }
        parent_page.set_dirty();
        self.add_dirty(parent_page.get().id);

        Ok(())
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
        let page_size = *self
            .page_size
            .get()
            .get_or_insert_with(|| header_accessor::get_page_size(self).unwrap());

        let reserved_space = *self
            .reserved_space
            .get_or_init(|| header_accessor::get_reserved_space(self).unwrap());

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
        let (result, changed) = self.wal.borrow_mut().begin_read_tx()?;
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
                            IOResult::IO => Ok(IOResult::IO),
                        }
                    }
                    _ => Ok(IOResult::IO),
                }
            } else {
                Ok(IOResult::IO)
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
        match self.maybe_allocate_page1()? {
            IOResult::Done(_) => {}
            IOResult::IO => return Ok(IOResult::IO),
        }
        Ok(IOResult::Done(self.wal.borrow_mut().begin_write_tx()?))
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
        if rollback {
            self.wal.borrow().end_write_tx();
            self.wal.borrow().end_read_tx();
            return Ok(IOResult::Done(PagerCommitResult::Rollback));
        }
        let commit_status = self.commit_dirty_pages(wal_checkpoint_disabled)?;
        match commit_status {
            IOResult::IO => Ok(IOResult::IO),
            IOResult::Done(_) => {
                self.wal.borrow().end_write_tx();
                self.wal.borrow().end_read_tx();

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
        self.wal.borrow().end_read_tx();
        Ok(())
    }

    /// Reads a page from the database.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub fn read_page(&self, page_idx: usize) -> Result<PageRef, LimboError> {
        tracing::trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx);
        if let Some(page) = page_cache.get(&page_key) {
            tracing::trace!("read_page(page_idx = {}) = cached", page_idx);
            return Ok(page.clone());
        }
        let page = Arc::new(Page::new(page_idx));
        page.set_locked();

        if let Some(frame_id) = self.wal.borrow().find_frame(page_idx as u64)? {
            self.wal
                .borrow()
                .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            {
                page.set_uptodate();
            }
            // TODO(pere) should probably first insert to page cache, and if successful,
            // read frame or page
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
            return Ok(page);
        }

        sqlite3_ondisk::begin_read_page(
            self.db_file.clone(),
            self.buffer_pool.clone(),
            page.clone(),
            page_idx,
        )?;
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
        Ok(page)
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
        Ok(self.wal.borrow().get_max_frame_in_wal())
    }

    /// Flush all dirty pages to disk.
    /// Unlike commit_dirty_pages, this function does not commit, checkpoint now sync the WAL/Database.
    #[instrument(skip_all, level = Level::INFO)]
    pub fn cacheflush(&self) -> Result<IOResult<()>> {
        let state = self.flush_info.borrow().state;
        trace!(?state);
        match state {
            CacheFlushState::Start => {
                let dirty_pages = self
                    .dirty_pages
                    .borrow()
                    .iter()
                    .copied()
                    .collect::<Vec<usize>>();
                let mut flush_info = self.flush_info.borrow_mut();
                if dirty_pages.is_empty() {
                    Ok(IOResult::Done(()))
                } else {
                    flush_info.dirty_pages = dirty_pages;
                    flush_info.state = CacheFlushState::AppendFrame {
                        current_page_to_append_idx: 0,
                    };
                    Ok(IOResult::IO)
                }
            }
            CacheFlushState::AppendFrame {
                current_page_to_append_idx,
            } => {
                let page_id = self.flush_info.borrow().dirty_pages[current_page_to_append_idx];
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

                self.wal.borrow_mut().append_frame(
                    page.clone(),
                    0,
                    self.flush_info.borrow().in_flight_writes.clone(),
                )?;
                self.flush_info.borrow_mut().state = CacheFlushState::WaitAppendFrame {
                    current_page_to_append_idx,
                };
                return Ok(IOResult::IO);
            }
            CacheFlushState::WaitAppendFrame {
                current_page_to_append_idx,
            } => {
                let in_flight = self.flush_info.borrow().in_flight_writes.clone();
                if *in_flight.borrow() > 0 {
                    return Ok(IOResult::IO);
                }

                // Clear dirty now
                let page_id = self.flush_info.borrow().dirty_pages[current_page_to_append_idx];
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
                page.clear_dirty();
                // Continue with next page
                let is_last_page =
                    current_page_to_append_idx == self.flush_info.borrow().dirty_pages.len() - 1;
                if is_last_page {
                    self.dirty_pages.borrow_mut().clear();
                    self.flush_info.borrow_mut().state = CacheFlushState::Start;
                    Ok(IOResult::Done(()))
                } else {
                    self.flush_info.borrow_mut().state = CacheFlushState::AppendFrame {
                        current_page_to_append_idx: current_page_to_append_idx + 1,
                    };
                    Ok(IOResult::IO)
                }
            }
        }
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
        let mut checkpoint_result = CheckpointResult::default();
        let res = loop {
            let state = self.commit_info.borrow().state;
            trace!(?state);
            match state {
                CommitState::Start => {
                    let dirty_pages = self
                        .dirty_pages
                        .borrow()
                        .iter()
                        .copied()
                        .collect::<Vec<usize>>();
                    let mut commit_info = self.commit_info.borrow_mut();
                    if dirty_pages.is_empty() {
                        return Ok(IOResult::Done(PagerCommitResult::WalWritten));
                    } else {
                        commit_info.dirty_pages = dirty_pages;
                        commit_info.state = CommitState::AppendFrame {
                            current_page_to_append_idx: 0,
                        };
                    }
                }
                CommitState::AppendFrame {
                    current_page_to_append_idx,
                } => {
                    let page_id = self.commit_info.borrow().dirty_pages[current_page_to_append_idx];
                    let is_last_frame = current_page_to_append_idx
                        == self.commit_info.borrow().dirty_pages.len() - 1;
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

                    let db_size = {
                        let db_size = header_accessor::get_database_size(self)?;
                        if is_last_frame {
                            db_size
                        } else {
                            0
                        }
                    };
                    self.wal.borrow_mut().append_frame(
                        page.clone(),
                        db_size,
                        self.commit_info.borrow().in_flight_writes.clone(),
                    )?;
                    self.commit_info.borrow_mut().state = CommitState::WaitAppendFrame {
                        current_page_to_append_idx,
                    };
                }
                CommitState::WaitAppendFrame {
                    current_page_to_append_idx,
                } => {
                    let in_flight = self.commit_info.borrow().in_flight_writes.clone();
                    if *in_flight.borrow() > 0 {
                        return Ok(IOResult::IO);
                    }
                    // First clear dirty
                    let page_id = self.commit_info.borrow().dirty_pages[current_page_to_append_idx];
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
                    page.clear_dirty();

                    // Now advance to next page if there are more
                    let is_last_frame = current_page_to_append_idx
                        == self.commit_info.borrow().dirty_pages.len() - 1;
                    if is_last_frame {
                        // Let's clear the page cache now
                        {
                            let mut cache = self.page_cache.write();
                            cache.clear().unwrap();
                        }
                        self.dirty_pages.borrow_mut().clear();
                        self.commit_info.borrow_mut().state = CommitState::SyncWal;
                    } else {
                        self.commit_info.borrow_mut().state = CommitState::AppendFrame {
                            current_page_to_append_idx: current_page_to_append_idx + 1,
                        }
                    }
                }
                CommitState::SyncWal => {
                    return_if_io!(self.wal.borrow_mut().sync());

                    if wal_checkpoint_disabled || !self.wal.borrow().should_checkpoint() {
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
                    sqlite3_ondisk::begin_sync(self.db_file.clone(), self.syncing.clone())?;
                    self.commit_info.borrow_mut().state = CommitState::WaitSyncDbFile;
                }
                CommitState::WaitSyncDbFile => {
                    if *self.syncing.borrow() {
                        return Ok(IOResult::IO);
                    } else {
                        self.commit_info.borrow_mut().state = CommitState::Start;
                        break PagerCommitResult::Checkpointed(checkpoint_result);
                    }
                }
            }
        };
        // We should only signal that we finished appenind frames after wal sync to avoid inconsistencies when sync fails
        self.wal.borrow_mut().finish_append_frames_commit()?;
        Ok(IOResult::Done(res))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_get_frame(&self, frame_no: u32, frame: &mut [u8]) -> Result<Arc<Completion>> {
        let wal = self.wal.borrow();
        wal.read_frame_raw(frame_no.into(), frame)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_insert_frame(&self, frame_no: u32, frame: &[u8]) -> Result<WalInsertInfo> {
        let mut wal = self.wal.borrow_mut();
        let (header, raw_page) = parse_wal_frame_header(frame);
        wal.write_frame_raw(
            self.buffer_pool.clone(),
            frame_no as u64,
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
        Ok(WalInsertInfo {
            page_no: header.page_number as usize,
            is_commit: header.is_commit_frame(),
        })
    }

    #[instrument(skip_all, level = Level::DEBUG, name = "pager_checkpoint",)]
    pub fn checkpoint(&self) -> Result<IOResult<CheckpointResult>> {
        let mut checkpoint_result = CheckpointResult::default();
        loop {
            let state = *self.checkpoint_state.borrow();
            trace!(?state);
            match state {
                CheckpointState::Checkpoint => {
                    let in_flight = self.checkpoint_inflight.clone();
                    match self.wal.borrow_mut().checkpoint(
                        self,
                        in_flight,
                        CheckpointMode::Passive,
                    )? {
                        IOResult::IO => return Ok(IOResult::IO),
                        IOResult::Done(res) => {
                            checkpoint_result = res;
                            self.checkpoint_state.replace(CheckpointState::SyncDbFile);
                        }
                    };
                }
                CheckpointState::SyncDbFile => {
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
            let mut wal = self.wal.borrow_mut();
            // fsync the wal syncronously before beginning checkpoint
            while let Ok(IOResult::IO) = wal.sync() {
                // TODO: for now forget about timeouts as they fail regularly in SIM
                // need to think of a better way to do this

                // if attempts >= 1000 {
                //     return Err(LimboError::InternalError(
                //         "Failed to fsync WAL before final checkpoint, fd likely closed".into(),
                //     ));
                // }
                self.io.run_once()?;
                _attempts += 1;
            }
        }
        self.wal_checkpoint(wal_checkpoint_disabled)?;
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_checkpoint(&self, wal_checkpoint_disabled: bool) -> Result<CheckpointResult> {
        if wal_checkpoint_disabled {
            return Ok(CheckpointResult {
                num_wal_frames: 0,
                num_checkpointed_frames: 0,
            });
        }

        let checkpoint_result = self.io.block(|| {
            self.wal
                .borrow_mut()
                .checkpoint(self, Rc::new(RefCell::new(0)), CheckpointMode::Passive)
                .map_err(|err| panic!("error while clearing cache {err}"))
        })?;

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
    pub fn free_page(&self, page: Option<PageRef>, page_id: usize) -> Result<IOResult<()>> {
        tracing::trace!("free_page(page_id={})", page_id);
        const TRUNK_PAGE_HEADER_SIZE: usize = 8;
        const LEAF_ENTRY_SIZE: usize = 4;
        const RESERVED_SLOTS: usize = 2;

        const TRUNK_PAGE_NEXT_PAGE_OFFSET: usize = 0; // Offset to next trunk page pointer
        const TRUNK_PAGE_LEAF_COUNT_OFFSET: usize = 4; // Offset to leaf count

        let mut state = self.free_page_state.borrow_mut();
        tracing::debug!(?state);
        loop {
            match &mut *state {
                FreePageState::Start => {
                    if page_id < 2 || page_id > header_accessor::get_database_size(self)? as usize {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid page number {page_id} for free operation"
                        )));
                    }

                    let page = match page.clone() {
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
                            page
                        }
                        None => self.read_page(page_id)?,
                    };
                    header_accessor::set_freelist_pages(
                        self,
                        header_accessor::get_freelist_pages(self)? + 1,
                    )?;

                    let trunk_page_id = header_accessor::get_freelist_trunk_page(self)?;

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
                    let trunk_page_id = header_accessor::get_freelist_trunk_page(self)?;
                    if trunk_page.is_none() {
                        // Add as leaf to current trunk
                        trunk_page.replace(self.read_page(trunk_page_id as usize)?);
                    }
                    let trunk_page = trunk_page.as_ref().unwrap();
                    if trunk_page.is_locked() || !trunk_page.is_loaded() {
                        return Ok(IOResult::IO);
                    }

                    let trunk_page_contents = trunk_page.get().contents.as_ref().unwrap();
                    let number_of_leaf_pages =
                        trunk_page_contents.read_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET);

                    // Reserve 2 slots for the trunk page header which is 8 bytes or 2*LEAF_ENTRY_SIZE
                    let max_free_list_entries =
                        (self.usable_space() / LEAF_ENTRY_SIZE) - RESERVED_SLOTS;

                    if number_of_leaf_pages < max_free_list_entries as u32 {
                        turso_assert!(
                            trunk_page.get().id == trunk_page_id as usize,
                            "trunk page has unexpected id"
                        );
                        self.add_dirty(trunk_page);

                        trunk_page_contents
                            .write_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET, number_of_leaf_pages + 1);
                        trunk_page_contents.write_u32(
                            TRUNK_PAGE_HEADER_SIZE
                                + (number_of_leaf_pages as usize * LEAF_ENTRY_SIZE),
                            page_id as u32,
                        );
                        page.clear_uptodate();

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

                    let trunk_page_id = header_accessor::get_freelist_trunk_page(self)?;

                    let contents = page.get().contents.as_mut().unwrap();
                    // Point to previous trunk
                    contents.write_u32(TRUNK_PAGE_NEXT_PAGE_OFFSET, trunk_page_id);
                    // Zero leaf count
                    contents.write_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET, 0);
                    // Update page 1 to point to new trunk
                    header_accessor::set_freelist_trunk_page(self, page_id as u32)?;
                    // Clear flags
                    page.clear_uptodate();
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
                default_header.database_size += 1;
                if let Some(size) = self.page_size.get() {
                    default_header.update_page_size(size);
                }
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
                    DATABASE_HEADER_SIZE,
                    (default_header.get_page_size() - default_header.reserved_space as u32) as u16,
                );
                let write_counter = Rc::new(RefCell::new(0));
                begin_write_btree_page(self, &page1.get(), write_counter.clone())?;

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

        loop {
            let mut state = self.allocate_page_state.borrow_mut();
            tracing::debug!("allocate_page(state={:?})", state);
            match &mut *state {
                AllocatePageState::Start => {
                    let old_db_size = header_accessor::get_database_size(self)?;
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
                            && is_ptrmap_page(
                                new_db_size + 1,
                                header_accessor::get_page_size(self)? as usize,
                            )
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

                    let first_freelist_trunk_page_id =
                        header_accessor::get_freelist_trunk_page(self)?;
                    if first_freelist_trunk_page_id == 0 {
                        *state = AllocatePageState::AllocateNewPage {
                            current_db_size: new_db_size,
                        };
                        continue;
                    }
                    let trunk_page = self.read_page(first_freelist_trunk_page_id as usize)?;
                    *state = AllocatePageState::SearchAvailableFreeListLeaf {
                        trunk_page,
                        current_db_size: new_db_size,
                    };
                    continue;
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
                        page_contents.read_u32(FREELIST_TRUNK_OFFSET_NEXT_TRUNK);
                    let number_of_freelist_leaves =
                        page_contents.read_u32(FREELIST_TRUNK_OFFSET_LEAF_COUNT);

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
                    header_accessor::set_freelist_trunk_page(self, next_trunk_page_id)?;
                    header_accessor::set_freelist_pages(
                        self,
                        header_accessor::get_freelist_pages(self)? - 1,
                    )?;
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
                        page_contents.read_u32(FREELIST_TRUNK_OFFSET_FIRST_LEAF);
                    let leaf_page = self.read_page(next_leaf_page_id as usize)?;
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
                    page_contents.write_u32(
                        FREELIST_TRUNK_OFFSET_LEAF_COUNT,
                        remaining_leaves_count as u32,
                    );
                    self.add_dirty(trunk_page);

                    header_accessor::set_freelist_pages(
                        self,
                        header_accessor::get_freelist_pages(self)? - 1,
                    )?;

                    *state = AllocatePageState::Start;
                    return Ok(IOResult::Done(leaf_page));
                }
                AllocatePageState::AllocateNewPage { current_db_size } => {
                    let new_db_size = *current_db_size + 1;
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
                        header_accessor::set_database_size(self, new_db_size)?;
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

    pub fn usable_size(&self) -> usize {
        let page_size = header_accessor::get_page_size(self).unwrap_or_default() as u32;
        let reserved_space = header_accessor::get_reserved_space(self).unwrap_or_default() as u32;
        (page_size - reserved_space) as usize
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn rollback(
        &self,
        schema_did_change: bool,
        connection: &Connection,
    ) -> Result<(), LimboError> {
        tracing::debug!(schema_did_change);
        self.dirty_pages.borrow_mut().clear();
        let mut cache = self.page_cache.write();

        self.reset_internal_states();

        cache.unset_dirty_all_pages();
        cache.clear().expect("failed to clear page cache");
        if schema_did_change {
            connection.schema.replace(connection._db.clone_schema()?);
        }
        self.wal.borrow_mut().rollback()?;

        Ok(())
    }

    fn reset_internal_states(&self) {
        self.checkpoint_state.replace(CheckpointState::Checkpoint);
        self.checkpoint_inflight.replace(0);
        self.syncing.replace(false);
        self.flush_info.replace(FlushInfo {
            state: CacheFlushState::Start,
            in_flight_writes: Rc::new(RefCell::new(0)),
            dirty_pages: Vec::new(),
        });
        self.commit_info.replace(CommitInfo {
            state: CommitState::Start,
            in_flight_writes: Rc::new(RefCell::new(0)),
            dirty_pages: Vec::new(),
        });
        self.allocate_page_state.replace(AllocatePageState::Start);
    }
}

pub fn allocate_new_page(page_id: usize, buffer_pool: &Arc<BufferPool>, offset: usize) -> PageRef {
    let page = Arc::new(Page::new(page_id));
    {
        let buffer = buffer_pool.get();
        let bp = buffer_pool.clone();
        let drop_fn = Rc::new(move |buf| {
            bp.put(buf);
        });
        let buffer = Arc::new(RefCell::new(Buffer::new(buffer, drop_fn)));
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
    use crate::{storage::sqlite3_ondisk::MIN_PAGE_SIZE, LimboError, Result};

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
        assert!(page_size >= MIN_PAGE_SIZE as usize);
        page_size / PTRMAP_ENTRY_SIZE
    }

    /// Calculates the cycle length of pointer map pages
    /// The cycle length is the number of database pages that are mapped by a single pointer map page.
    pub fn ptrmap_page_cycle_length(page_size: usize) -> usize {
        assert!(page_size >= MIN_PAGE_SIZE as usize);
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
    use crate::storage::btree::{insert_into_cell, offset};
    use crate::storage::buffer_pool::BufferPool;
    use crate::storage::database::{DatabaseFile, DatabaseStorage};
    use crate::storage::page_cache::DumbLruPageCache;
    use crate::storage::pager::Pager;
    use crate::storage::sqlite3_ondisk::MIN_PAGE_SIZE;
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
    fn test_pager_setup(page_size: u32, initial_db_pages: u32, mode: DatabaseMode) -> Pager {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(
            io.open_file("test.db", OpenFlags::Create, true).unwrap(),
        ));

        //  Construct interfaces for the pager
        let buffer_pool = Arc::new(BufferPool::new(Some(page_size as usize)));
        let page_cache = Arc::new(RwLock::new(DumbLruPageCache::new(
            (initial_db_pages + 10) as usize,
        )));

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
            wal,
            io,
            page_cache,
            buffer_pool,
            Arc::new(AtomicDbState::new(DbState::Uninitialized)),
            Arc::new(Mutex::new(())),
            mode,
        )
        .unwrap();
        run_until_done(|| pager.allocate_page1(), &pager).unwrap();
        header_accessor::set_vacuum_mode_largest_root_page(&pager, 1).unwrap();
        pager.set_auto_vacuum_mode(AutoVacuumMode::Full);

        //  Allocate all the pages as btree root pages
        const EXPECTED_FIRST_ROOT_PAGE_ID: u32 = 3; // page1 = 1,  first ptrmap page = 2, root page = 3
        for i in 0..initial_db_pages {
            match run_until_done(
                || pager.btree_create(&CreateBTreeFlags::new_table(), &mut Vec::new()),
                &pager,
            ) {
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
        let pager = test_pager_setup(page_size, initial_db_pages, DatabaseMode::Memory);

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
            header_accessor::get_database_size(&pager).unwrap(),
            initial_db_pages + 2
        ); // (1+1) -> (header + ptrmap)

        //  Read the entry from the ptrmap page and verify it
        let entry = pager.ptrmap_get(db_page_to_update).unwrap();
        assert!(matches!(entry, IOResult::Done(Some(_))));
        let IOResult::Done(Some(entry)) = entry else {
            panic!("entry is not Some");
        };
        assert_eq!(entry.entry_type, PtrmapType::RootPage);
        assert_eq!(entry.parent_page_no, 0);
    }

    #[test]
    fn test_is_ptrmap_page_logic() {
        let page_size = MIN_PAGE_SIZE as usize;
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
        let page_size = MIN_PAGE_SIZE as usize; // Maps 103 data pages

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
        let page_size = MIN_PAGE_SIZE as usize; //  Maps 103 data pages

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

    #[test]
    fn test_root_page_relocation_persistent() {
        run_relocation_scenario(DatabaseMode::File);
    }

    #[test]
    fn test_root_page_relocation_tempdb() {
        run_relocation_scenario(DatabaseMode::Memory);
    }

    /// Common body used by both variants above.
    fn run_relocation_scenario(db_mode: DatabaseMode) {
        use crate::storage::header_accessor;
        use ptrmap::PtrmapType;

        //  Create a single root page which will be allocated to page 3
        const PAGE_SIZE: u32 = 4096;
        const INITIAL_ROOTS: u32 = 1;
        let pager = test_pager_setup(PAGE_SIZE, INITIAL_ROOTS, db_mode);

        //  Check that the root page was mapped properly
        let root_page = pager.read_page(3).unwrap();
        let root_page_ptrmap_entry =
            run_until_done(|| pager.ptrmap_get(root_page.get().id as u32), &pager)
                .unwrap()
                .unwrap();
        assert_eq!(root_page_ptrmap_entry.entry_type, PtrmapType::RootPage);

        //  Make the root page an interior node and add to children with proper mappings
        let btree_root = Arc::new(BTreePageInner {
            page: RefCell::new(root_page),
        });
        btree_init_page(
            &btree_root,
            PageType::TableInterior,
            0,
            pager.usable_space() as u16,
        );

        //  allocate two leaf children
        let page_4 = pager.allocate_page().unwrap();
        let page_5 = pager.allocate_page().unwrap();
        let btree_page_4 = Arc::new(BTreePageInner {
            page: RefCell::new(Arc::clone(&page_4)),
        });
        btree_init_page(
            &btree_page_4,
            PageType::TableLeaf,
            0,
            pager.usable_space() as u16,
        );
        let btree_page_5 = Arc::new(BTreePageInner {
            page: RefCell::new(Arc::clone(&page_5)),
        });
        btree_init_page(
            &btree_page_5,
            PageType::TableLeaf,
            0,
            pager.usable_space() as u16,
        );

        //  configure the root page to point to the two children
        let root_page = btree_root.get();
        let contents = root_page.get().contents.as_mut().unwrap();
        let cell_bytes: [u8; 5] = [
            (page_4.get().id >> 24) as u8,
            (page_4.get().id >> 16) as u8,
            (page_4.get().id >> 8) as u8,
            (page_4.get().id) as u8,
            0x01,
        ];
        insert_into_cell(contents, &cell_bytes, 0, pager.usable_space() as u16).unwrap();
        contents.write_u32(offset::BTREE_RIGHTMOST_PTR, page_5.get().id as u32);

        root_page.set_dirty();
        pager.add_dirty(3);

        //  create the pointer map entries for the two children
        pager
            .ptrmap_put(page_4.get().id as u32, PtrmapType::BTreeNode, 3)
            .unwrap();
        pager
            .ptrmap_put(page_5.get().id as u32, PtrmapType::BTreeNode, 3)
            .unwrap();

        //  TODO(Zaid) This is a hack to mark all pages as clean before relocation as part of autovacuum
        //  In reality, dirty pages should become part of the subjournal so they can be rolled back
        //  See - https://github.com/sqlite/sqlite/blob/master/src/pager.c#L7141-L7270
        loop {
            match pager.cacheflush(true).unwrap() {
                PagerCacheflushStatus::Done(_) => break,
                PagerCacheflushStatus::IO => pager.io.run_once().unwrap(),
            }
        }

        //  Create a new table which will be allocated root page 4 forcing a relocation from 4 to 6
        let new_root = run_until_done(
            || pager.btree_create(&CreateBTreeFlags::new_table(), &mut []),
            &pager,
        )
        .unwrap();
        assert_eq!(new_root, 4);
        assert_eq!(
            header_accessor::get_vacuum_mode_largest_root_page(&pager).unwrap(),
            new_root
        );

        //  Check pointer map entries for new root
        let ptrmap_entry_new_root = run_until_done(|| pager.ptrmap_get(new_root), &pager)
            .unwrap()
            .unwrap();
        assert_eq!(ptrmap_entry_new_root.entry_type, PtrmapType::RootPage);
        assert_eq!(ptrmap_entry_new_root.parent_page_no, 0);

        //  Check that the new root is a leaf
        let new_root_page = pager.read_page(new_root as usize).unwrap();
        assert!(
            matches!(
                new_root_page.get_contents().page_type(),
                PageType::TableLeaf | PageType::IndexLeaf
            ),
            "page 4 is not a leaf after relocation"
        );

        //  Check that the parent page correctly re-mapped to the new child page number
        let updated_root_page = pager.read_page(3).unwrap();
        let updated_contents = updated_root_page.get_contents();

        let left_child_page = updated_contents
            .cell_table_interior_read_left_child_page(0)
            .unwrap();
        assert_eq!(
            left_child_page, 6,
            "Interior cell should now point to relocated page 6"
        );

        let rightmost_page = updated_contents.rightmost_pointer().unwrap();
        assert_eq!(
            rightmost_page, 5,
            "Rightmost pointer should still point to page 5"
        );

        let relocated_page_ptrmap_entry = run_until_done(|| pager.ptrmap_get(6), &pager)
            .unwrap()
            .unwrap();
        assert_eq!(
            relocated_page_ptrmap_entry.entry_type,
            PtrmapType::BTreeNode
        );
        assert_eq!(
            relocated_page_ptrmap_entry.parent_page_no, 3,
            "Relocated page should still have parent page 3"
        );
    }

    /// Extended relocation scenario where the left child of the root is itself an interior
    /// node with two leaf children. Relocating this interior node forces `set_child_ptrmaps` to
    /// update the pointer-map entries of its children so that they now reference the relocated
    /// parent. The test asserts that this update took place successfully.
    fn run_relocation_grandchild_scenario(db_mode: DatabaseMode) {
        use crate::storage::header_accessor;
        use ptrmap::PtrmapType;

        //  Create a single root page which will be allocated to page 3
        const PAGE_SIZE: u32 = 4096;
        let pager = test_pager_setup(PAGE_SIZE, 1, db_mode);

        // Root page (3) should exist already and be mapped as a root page
        let root_page = pager.read_page(3).unwrap();
        let root_ptrmap_entry =
            run_until_done(|| pager.ptrmap_get(root_page.get().id as u32), &pager)
                .unwrap()
                .unwrap();
        assert_eq!(root_ptrmap_entry.entry_type, PtrmapType::RootPage);

        // ------------------------------------------------------------------
        // Construct the following b-tree shape (only page numbers shown):
        //            3 (root – interior)
        //           / \
        //          4   5 (leaves)
        //         / \
        //        6   7 (leaves)
        // ------------------------------------------------------------------

        // Turn page 3 into an interior node
        let btree_root = Arc::new(BTreePageInner {
            page: RefCell::new(root_page),
        });
        btree_init_page(
            &btree_root,
            PageType::TableInterior,
            0,
            pager.usable_space() as u16,
        );

        // Allocate pages 4 & 5 (children of root)
        let page_4 = pager.allocate_page().unwrap(); // will become interior
        let page_5 = pager.allocate_page().unwrap(); // leaf child of root

        // Allocate pages 6 & 7 (children of page 4)
        let page_6 = pager.allocate_page().unwrap(); // leaf child of page 4
        let page_7 = pager.allocate_page().unwrap(); // leaf child of page 4

        // Initialise structural page types
        let btree_page_4 = Arc::new(BTreePageInner {
            page: RefCell::new(Arc::clone(&page_4)),
        });
        btree_init_page(
            &btree_page_4,
            PageType::TableInterior,
            0,
            pager.usable_space() as u16,
        );
        let btree_page_5 = Arc::new(BTreePageInner {
            page: RefCell::new(Arc::clone(&page_5)),
        });
        btree_init_page(
            &btree_page_5,
            PageType::TableLeaf,
            0,
            pager.usable_space() as u16,
        );
        let btree_page_6 = Arc::new(BTreePageInner {
            page: RefCell::new(Arc::clone(&page_6)),
        });
        btree_init_page(
            &btree_page_6,
            PageType::TableLeaf,
            0,
            pager.usable_space() as u16,
        );
        let btree_page_7 = Arc::new(BTreePageInner {
            page: RefCell::new(Arc::clone(&page_7)),
        });
        btree_init_page(
            &btree_page_7,
            PageType::TableLeaf,
            0,
            pager.usable_space() as u16,
        );

        // ------------------------------------------------------------------
        // Wire up pointers on page 4 (interior) -> children 6 & 7
        // ------------------------------------------------------------------
        {
            let page4_ref = btree_page_4.get();
            let page4_contents = page4_ref.get_contents();
            // Interior cell pointing to left child (page 6)
            let cell_bytes_4: [u8; 5] = [
                (page_6.get().id >> 24) as u8,
                (page_6.get().id >> 16) as u8,
                (page_6.get().id >> 8) as u8,
                (page_6.get().id) as u8,
                0x01,
            ];
            insert_into_cell(
                page4_contents,
                &cell_bytes_4,
                0,
                pager.usable_space() as u16,
            )
            .unwrap();
            page4_contents.write_u32(offset::BTREE_RIGHTMOST_PTR, page_7.get().id as u32);
            page_4.set_dirty();
            pager.add_dirty(page_4.get().id);
        }

        // ------------------------------------------------------------------
        // Wire up pointers on root page 3 -> children 4 & 5
        // ------------------------------------------------------------------
        {
            let root_page_ref = btree_root.get();
            let root_contents = root_page_ref.get_contents();
            let cell_bytes_root: [u8; 5] = [
                (page_4.get().id >> 24) as u8,
                (page_4.get().id >> 16) as u8,
                (page_4.get().id >> 8) as u8,
                (page_4.get().id) as u8,
                0x01,
            ];
            insert_into_cell(
                root_contents,
                &cell_bytes_root,
                0,
                pager.usable_space() as u16,
            )
            .unwrap();
            root_contents.write_u32(offset::BTREE_RIGHTMOST_PTR, page_5.get().id as u32);
            root_page_ref.set_dirty();
            pager.add_dirty(3);
        }

        // ------------------------------------------------------------------
        // Create pointer-map entries for all pages
        // ------------------------------------------------------------------
        pager
            .ptrmap_put(page_4.get().id as u32, PtrmapType::BTreeNode, 3)
            .unwrap();
        pager
            .ptrmap_put(page_5.get().id as u32, PtrmapType::BTreeNode, 3)
            .unwrap();
        pager
            .ptrmap_put(
                page_6.get().id as u32,
                PtrmapType::BTreeNode,
                page_4.get().id as u32,
            )
            .unwrap();
        pager
            .ptrmap_put(
                page_7.get().id as u32,
                PtrmapType::BTreeNode,
                page_4.get().id as u32,
            )
            .unwrap();

        // Flush all dirty pages so they are clean prior to relocation
        loop {
            match pager.cacheflush(true).unwrap() {
                PagerCacheflushStatus::Done(_) => break,
                PagerCacheflushStatus::IO => pager.io.run_once().unwrap(),
            }
        }

        // ------------------------------------------------------------------
        // Trigger relocation by creating a new table (root page id 4)
        // ------------------------------------------------------------------
        let new_root_id = run_until_done(
            || pager.btree_create(&CreateBTreeFlags::new_table(), &mut []),
            &pager,
        )
        .unwrap();
        assert_eq!(new_root_id, 4);
        assert_eq!(
            header_accessor::get_vacuum_mode_largest_root_page(&pager).unwrap(),
            new_root_id
        );

        // New root page 4 should be a leaf now
        let new_root_page = pager.read_page(new_root_id as usize).unwrap();
        assert!(matches!(
            new_root_page.get_contents().page_type(),
            PageType::TableLeaf | PageType::IndexLeaf
        ));

        // Root page 3 should now point to the relocated page (formerly 4)
        let updated_root_page = pager.read_page(3).unwrap();
        let updated_contents = updated_root_page.get_contents();
        let relocated_page_id = updated_contents
            .cell_table_interior_read_left_child_page(0)
            .unwrap() as usize;
        assert_ne!(relocated_page_id, 4, "Page 4 should have been relocated");

        // Pointer-map entry for relocated page should reference parent 3
        let relocated_entry = run_until_done(|| pager.ptrmap_get(relocated_page_id as u32), &pager)
            .unwrap()
            .unwrap();
        assert_eq!(relocated_entry.entry_type, PtrmapType::BTreeNode);
        assert_eq!(relocated_entry.parent_page_no, 3);

        // Children (pages 6 & 7) should now reference the relocated parent
        for child_id in [page_6.get().id, page_7.get().id] {
            let entry = run_until_done(|| pager.ptrmap_get(child_id as u32), &pager)
                .unwrap()
                .unwrap();
            assert_eq!(entry.entry_type, PtrmapType::BTreeNode);
            assert_eq!(
                entry.parent_page_no, relocated_page_id as u32,
                "Child page {} should now reference relocated parent {}",
                child_id, relocated_page_id
            );
        }
    }

    #[test]
    fn test_grandchild_page_relocation_persistent() {
        run_relocation_grandchild_scenario(DatabaseMode::File);
    }

    #[test]
    fn test_grandchild_page_relocation_tempdb() {
        run_relocation_grandchild_scenario(DatabaseMode::Memory);
    }
    // NEW_CODE_END
}
