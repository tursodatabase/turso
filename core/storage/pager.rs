use crate::result::LimboResult;
use crate::storage::btree::BTreePageInner;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::header_accessor;
use crate::storage::sqlite3_ondisk::{self, DatabaseHeader, PageContent, PageType};
use crate::storage::wal::{CheckpointResult, Wal, WalFsyncStatus};
use crate::types::CursorResult;
use crate::{Buffer, LimboError, Result};
use crate::{Completion, WalFile};
use parking_lot::RwLock;
use std::cell::{OnceCell, RefCell, UnsafeCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{trace, Level};

use super::btree::{btree_init_page, BTreePage};
use super::page_cache::{CacheError, CacheResizeResult, DumbLruPageCache, PageCacheKey};
use super::sqlite3_ondisk::{begin_write_btree_page, DATABASE_HEADER_SIZE};
use super::wal::{CheckpointMode, CheckpointStatus};

#[cfg(not(feature = "omit_autovacuum"))]
use {crate::io::Buffer as IoBuffer, ptrmap::*};

pub struct PageInner {
    pub flags: AtomicUsize,
    pub contents: Option<PageContent>,
    pub id: usize,
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
}

#[derive(Clone, Copy, Debug)]
/// The state of the current pager cache flush.
enum FlushState {
    /// Idle.
    Start,
    /// Waiting for all in-flight writes to the on-disk WAL to complete.
    WaitAppendFrames,
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
pub enum BtreePageAllocMode {
    /// Allocate any btree page
    Any,
    /// Allocate a specific page number, typically used for root page allocation
    Exact(u32),
    /// Allocate a page number less than or equal to the parameter
    Le(u32),
}

/// This will keep track of the state of current cache flush in order to not repeat work
struct FlushInfo {
    state: FlushState,
    /// Number of writes taking place. When in_flight gets to 0 we can schedule a fsync.
    in_flight_writes: Rc<RefCell<usize>>,
}

/// Track the state of the auto-vacuum mode.
#[derive(Clone, Copy, Debug)]
pub enum AutoVacuumMode {
    None,
    Full,
    Incremental,
}

pub const DB_STATE_UNITIALIZED: usize = 0;
pub const DB_STATE_INITIALIZING: usize = 1;
pub const DB_STATE_INITIALIZED: usize = 2;
/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    /// Source of the database pages.
    pub db_file: Arc<dyn DatabaseStorage>,
    /// The write-ahead log (WAL) for the database.
    wal: Rc<RefCell<dyn Wal>>,
    /// A page cache for the database.
    page_cache: Arc<RwLock<DumbLruPageCache>>,
    /// Buffer pool for temporary data storage.
    pub buffer_pool: Rc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    dirty_pages: Rc<RefCell<HashSet<usize>>>,

    flush_info: RefCell<FlushInfo>,
    checkpoint_state: RefCell<CheckpointState>,
    checkpoint_inflight: Rc<RefCell<usize>>,
    syncing: Rc<RefCell<bool>>,
    auto_vacuum_mode: RefCell<AutoVacuumMode>,
    /// 0 -> Database is empty,
    /// 1 -> Database is being initialized,
    /// 2 -> Database is initialized and ready for use.
    pub is_empty: Arc<AtomicUsize>,
    /// Mutex for synchronizing database initialization to prevent race conditions
    init_lock: Arc<Mutex<()>>,
    allocate_page1_state: RefCell<AllocatePage1State>,
    /// Cache page_size and reserved_space at Pager init and reuse for subsequent
    /// `usable_space` calls. TODO: Invalidate reserved_space when we add the functionality
    /// to change it.
    page_size: OnceCell<u16>,
    reserved_space: OnceCell<u8>,
}

#[derive(Debug, Copy, Clone)]
/// The status of the current cache flush.
/// A Done state means that the WAL was committed to disk and fsynced,
/// plus potentially checkpointed to the DB (and the DB then fsynced).
pub enum PagerCacheflushStatus {
    Done(PagerCacheflushResult),
    IO,
}

#[derive(Debug, Copy, Clone)]
pub enum PagerCacheflushResult {
    /// The WAL was written to disk and fsynced.
    WalWritten,
    /// The WAL was written, fsynced, and a checkpoint was performed.
    /// The database file was then also fsynced.
    Checkpointed(CheckpointResult),
    Rollback,
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

impl Pager {
    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Rc<RefCell<dyn Wal>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<DumbLruPageCache>>,
        buffer_pool: Rc<BufferPool>,
        is_empty: Arc<AtomicUsize>,
        init_lock: Arc<Mutex<()>>,
    ) -> Result<Self> {
        let allocate_page1_state = if is_empty.load(Ordering::SeqCst) < DB_STATE_INITIALIZED {
            RefCell::new(AllocatePage1State::Start)
        } else {
            RefCell::new(AllocatePage1State::Done)
        };
        Ok(Self {
            db_file,
            wal,
            page_cache,
            io,
            dirty_pages: Rc::new(RefCell::new(HashSet::new())),
            flush_info: RefCell::new(FlushInfo {
                state: FlushState::Start,
                in_flight_writes: Rc::new(RefCell::new(0)),
            }),
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_state: RefCell::new(CheckpointState::Checkpoint),
            checkpoint_inflight: Rc::new(RefCell::new(0)),
            buffer_pool,
            auto_vacuum_mode: RefCell::new(AutoVacuumMode::None),
            is_empty,
            init_lock,
            allocate_page1_state,
            page_size: OnceCell::new(),
            reserved_space: OnceCell::new(),
        })
    }

    pub fn set_wal(&mut self, wal: Rc<RefCell<WalFile>>) {
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
    pub fn ptrmap_get(&self, target_page_num: u32) -> Result<CursorResult<Option<PtrmapEntry>>> {
        tracing::trace!("ptrmap_get(page_idx = {})", target_page_num);
        let configured_page_size = header_accessor::get_page_size(self)? as usize;

        if target_page_num < FIRST_PTRMAP_PAGE_NO
            || is_ptrmap_page(target_page_num, configured_page_size)
        {
            return Ok(CursorResult::Ok(None));
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
            return Ok(CursorResult::IO);
        }
        if !ptrmap_page.is_loaded() {
            return Ok(CursorResult::IO);
        }
        let ptrmap_page_inner = ptrmap_page.get();

        let page_content: &PageContent = match ptrmap_page_inner.contents.as_ref() {
            Some(content) => content,
            None => {
                return Err(LimboError::InternalError(format!(
                    "Ptrmap page {} content not loaded",
                    ptrmap_pg_no
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
                "Ptrmap offset {} + entry size {} out of bounds for page {} (actual data len {})",
                offset_in_ptrmap_page, PTRMAP_ENTRY_SIZE, ptrmap_pg_no, actual_data_length
            )));
        }

        let entry_slice = &ptrmap_page_data_slice
            [offset_in_ptrmap_page..offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE];
        match PtrmapEntry::deserialize(entry_slice) {
            Some(entry) => Ok(CursorResult::Ok(Some(entry))),
            None => Err(LimboError::Corrupt(format!(
                "Failed to deserialize ptrmap entry for page {} from ptrmap page {}",
                target_page_num, ptrmap_pg_no
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
    ) -> Result<CursorResult<()>> {
        tracing::trace!(
            "ptrmap_put(page_idx = {}, entry_type = {:?}, parent_page_no = {})",
            db_page_no_to_update,
            entry_type,
            parent_page_no
        );

        let page_size = header_accessor::get_page_size(self)? as usize;

        if db_page_no_to_update < FIRST_PTRMAP_PAGE_NO
            || is_ptrmap_page(db_page_no_to_update, page_size)
        {
            return Err(LimboError::InternalError(format!(
                "Cannot set ptrmap entry for page {}: it's a header/ptrmap page or invalid.",
                db_page_no_to_update
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
            return Ok(CursorResult::IO);
        }
        if !ptrmap_page.is_loaded() {
            return Ok(CursorResult::IO);
        }
        let ptrmap_page_inner = ptrmap_page.get();

        let page_content = match ptrmap_page_inner.contents.as_ref() {
            Some(content) => content,
            None => {
                return Err(LimboError::InternalError(format!(
                    "Ptrmap page {} content not loaded",
                    ptrmap_pg_no
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

        ptrmap_page.set_dirty();
        self.add_dirty(ptrmap_pg_no as usize);
        Ok(CursorResult::Ok(()))
    }

    /// This method is used to allocate a new root page for a btree, both for tables and indexes
    /// FIXME: handle no room in page cache
    pub fn btree_create(&self, flags: &CreateBTreeFlags) -> Result<CursorResult<u32>> {
        let page_type = match flags {
            _ if flags.is_table() => PageType::TableLeaf,
            _ if flags.is_index() => PageType::IndexLeaf,
            _ => unreachable!("Invalid flags state"),
        };
        #[cfg(feature = "omit_autovacuum")]
        {
            let page = self.do_allocate_page(page_type, 0, BtreePageAllocMode::Any);
            let page_id = page.get().get().id;
            Ok(CursorResult::Ok(page_id as u32))
        }

        //  If autovacuum is enabled, we need to allocate a new page number that is greater than the largest root page number
        #[cfg(not(feature = "omit_autovacuum"))]
        {
            let auto_vacuum_mode = self.auto_vacuum_mode.borrow();
            match *auto_vacuum_mode {
                AutoVacuumMode::None => {
                    let page = self.do_allocate_page(page_type, 0, BtreePageAllocMode::Any);
                    let page_id = page.get().get().id;
                    Ok(CursorResult::Ok(page_id as u32))
                }
                AutoVacuumMode::Full => {
                    let mut root_page_num =
                        header_accessor::get_vacuum_mode_largest_root_page(self)?;
                    assert!(root_page_num > 0); //  Largest root page number cannot be 0 because that is set to 1 when creating the database with autovacuum enabled
                    root_page_num += 1;
                    assert!(root_page_num >= FIRST_PTRMAP_PAGE_NO); //  can never be less than 2 because we have already incremented

                    while is_ptrmap_page(
                        root_page_num,
                        header_accessor::get_page_size(self)? as usize,
                    ) {
                        root_page_num += 1;
                    }
                    assert!(root_page_num >= 3); //  the very first root page is page 3

                    //  root_page_num here is the desired root page
                    let page = self.do_allocate_page(
                        page_type,
                        0,
                        BtreePageAllocMode::Exact(root_page_num),
                    );
                    let allocated_page_id = page.get().get().id as u32;
                    if allocated_page_id != root_page_num {
                        //  TODO(Zaid): Handle swapping the allocated page with the desired root page
                    }

                    //  TODO(Zaid): Update the header metadata to reflect the new root page number

                    //  For now map allocated_page_id since we are not swapping it with root_page_num
                    match self.ptrmap_put(allocated_page_id, PtrmapType::RootPage, 0)? {
                        CursorResult::Ok(_) => Ok(CursorResult::Ok(allocated_page_id as u32)),
                        CursorResult::IO => Ok(CursorResult::IO),
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
    pub fn allocate_overflow_page(&self) -> PageRef {
        let page = self.allocate_page().unwrap();
        tracing::debug!("Pager::allocate_overflow_page(id={})", page.get().id);

        // setup overflow page
        let contents = page.get().contents.as_mut().unwrap();
        let buf = contents.as_ptr();
        buf.fill(0);

        page
    }

    /// Allocate a new page to the btree via the pager.
    /// This marks the page as dirty and writes the page header.
    // FIXME: handle no room in page cache
    pub fn do_allocate_page(
        &self,
        page_type: PageType,
        offset: usize,
        _alloc_mode: BtreePageAllocMode,
    ) -> BTreePage {
        let page = self.allocate_page().unwrap();
        let page = Arc::new(BTreePageInner {
            page: RefCell::new(page),
        });
        btree_init_page(&page, page_type, offset, self.usable_space() as u16);
        tracing::debug!(
            "do_allocate_page(id={}, page_type={:?})",
            page.get().get().id,
            page.get().get_contents().page_type()
        );
        page
    }

    /// The "usable size" of a database page is the page size specified by the 2-byte integer at offset 16
    /// in the header, minus the "reserved" space size recorded in the 1-byte integer at offset 20 in the header.
    /// The usable size of a page might be an odd number. However, the usable size is not allowed to be less than 480.
    /// In other words, if the page size is 512, then the reserved space size cannot exceed 32.
    pub fn usable_space(&self) -> usize {
        let page_size = *self
            .page_size
            .get_or_init(|| header_accessor::get_page_size(self).unwrap_or_default());

        let reserved_space = *self
            .reserved_space
            .get_or_init(|| header_accessor::get_reserved_space(self).unwrap_or_default());

        (page_size as usize) - (reserved_space as usize)
    }

    #[inline(always)]
    pub fn begin_read_tx(&self) -> Result<CursorResult<LimboResult>> {
        // We allocate the first page lazily in the first transaction
        match self.maybe_allocate_page1()? {
            CursorResult::Ok(_) => {}
            CursorResult::IO => return Ok(CursorResult::IO),
        }
        Ok(CursorResult::Ok(self.wal.borrow_mut().begin_read_tx()?))
    }

    fn maybe_allocate_page1(&self) -> Result<CursorResult<()>> {
        if self.is_empty.load(Ordering::SeqCst) < DB_STATE_INITIALIZED {
            if let Ok(_lock) = self.init_lock.try_lock() {
                match (
                    self.is_empty.load(Ordering::SeqCst),
                    self.allocating_page1(),
                ) {
                    // In case of being empty or (allocating and this connection is performing allocation) then allocate the first page
                    (0, false) | (1, true) => match self.allocate_page1()? {
                        CursorResult::Ok(_) => Ok(CursorResult::Ok(())),
                        CursorResult::IO => Ok(CursorResult::IO),
                    },
                    _ => Ok(CursorResult::IO),
                }
            } else {
                Ok(CursorResult::IO)
            }
        } else {
            Ok(CursorResult::Ok(()))
        }
    }

    #[inline(always)]
    pub fn begin_write_tx(&self) -> Result<CursorResult<LimboResult>> {
        // TODO(Diego): The only possibly allocate page1 here is because OpenEphemeral needs a write transaction
        // we should have a unique API to begin transactions, something like sqlite3BtreeBeginTrans
        match self.maybe_allocate_page1()? {
            CursorResult::Ok(_) => {}
            CursorResult::IO => return Ok(CursorResult::IO),
        }
        Ok(CursorResult::Ok(self.wal.borrow_mut().begin_write_tx()?))
    }

    pub fn end_tx(&self, rollback: bool) -> Result<PagerCacheflushStatus> {
        tracing::trace!("end_tx(rollback={})", rollback);
        if rollback {
            self.wal.borrow().end_write_tx()?;
            self.wal.borrow().end_read_tx()?;
            return Ok(PagerCacheflushStatus::Done(PagerCacheflushResult::Rollback));
        }
        let cacheflush_status = self.cacheflush()?;
        match cacheflush_status {
            PagerCacheflushStatus::IO => Ok(PagerCacheflushStatus::IO),
            PagerCacheflushStatus::Done(_) => {
                self.wal.borrow().end_write_tx()?;
                self.wal.borrow().end_read_tx()?;
                Ok(cacheflush_status)
            }
        }
    }

    pub fn end_read_tx(&self) -> Result<()> {
        self.wal.borrow().end_read_tx()?;
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
                        "Failed to insert page into cache: {:?}",
                        e
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
                    "Failed to insert page into cache: {:?}",
                    e
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

    pub fn add_dirty(&self, page_id: usize) {
        // TODO: check duplicates?
        let mut dirty_pages = RefCell::borrow_mut(&self.dirty_pages);
        dirty_pages.insert(page_id);
    }

    pub fn wal_frame_count(&self) -> Result<u64> {
        Ok(self.wal.borrow().get_max_frame_in_wal())
    }

    /// Flush dirty pages to disk.
    /// In the base case, it will write the dirty pages to the WAL and then fsync the WAL.
    /// If the WAL size is over the checkpoint threshold, it will checkpoint the WAL to
    /// the database file and then fsync the database file.
    pub fn cacheflush(&self) -> Result<PagerCacheflushStatus> {
        let mut checkpoint_result = CheckpointResult::default();
        loop {
            let state = self.flush_info.borrow().state;
            trace!("cacheflush {:?}", state);
            match state {
                FlushState::Start => {
                    let db_size = header_accessor::get_database_size(self)?;
                    for page_id in self.dirty_pages.borrow().iter() {
                        let mut cache = self.page_cache.write();
                        let page_key = PageCacheKey::new(*page_id);
                        let page = cache.get(&page_key).expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                        let page_type = page.get().contents.as_ref().unwrap().maybe_page_type();
                        trace!("cacheflush(page={}, page_type={:?}", page_id, page_type);
                        self.wal.borrow_mut().append_frame(
                            page.clone(),
                            db_size,
                            self.flush_info.borrow().in_flight_writes.clone(),
                        )?;
                        page.clear_dirty();
                    }
                    // This is okay assuming we use shared cache by default.
                    {
                        let mut cache = self.page_cache.write();
                        cache.clear().unwrap();
                    }
                    self.dirty_pages.borrow_mut().clear();
                    self.flush_info.borrow_mut().state = FlushState::WaitAppendFrames;
                    return Ok(PagerCacheflushStatus::IO);
                }
                FlushState::WaitAppendFrames => {
                    let in_flight = *self.flush_info.borrow().in_flight_writes.borrow();
                    if in_flight == 0 {
                        self.flush_info.borrow_mut().state = FlushState::SyncWal;
                        self.wal.borrow_mut().finish_append_frames_commit()?;
                    } else {
                        return Ok(PagerCacheflushStatus::IO);
                    }
                }
                FlushState::SyncWal => {
                    if WalFsyncStatus::IO == self.wal.borrow_mut().sync()? {
                        return Ok(PagerCacheflushStatus::IO);
                    }

                    if !self.wal.borrow().should_checkpoint() {
                        self.flush_info.borrow_mut().state = FlushState::Start;
                        return Ok(PagerCacheflushStatus::Done(
                            PagerCacheflushResult::WalWritten,
                        ));
                    }
                    self.flush_info.borrow_mut().state = FlushState::Checkpoint;
                }
                FlushState::Checkpoint => {
                    match self.checkpoint()? {
                        CheckpointStatus::Done(res) => {
                            checkpoint_result = res;
                            self.flush_info.borrow_mut().state = FlushState::SyncDbFile;
                        }
                        CheckpointStatus::IO => return Ok(PagerCacheflushStatus::IO),
                    };
                }
                FlushState::SyncDbFile => {
                    sqlite3_ondisk::begin_sync(self.db_file.clone(), self.syncing.clone())?;
                    self.flush_info.borrow_mut().state = FlushState::WaitSyncDbFile;
                }
                FlushState::WaitSyncDbFile => {
                    if *self.syncing.borrow() {
                        return Ok(PagerCacheflushStatus::IO);
                    } else {
                        self.flush_info.borrow_mut().state = FlushState::Start;
                        break;
                    }
                }
            }
        }
        Ok(PagerCacheflushStatus::Done(
            PagerCacheflushResult::Checkpointed(checkpoint_result),
        ))
    }

    pub fn wal_get_frame(
        &self,
        frame_no: u32,
        p_frame: *mut u8,
        frame_len: u32,
    ) -> Result<Arc<Completion>> {
        let wal = self.wal.borrow();
        wal.read_frame_raw(
            frame_no.into(),
            self.buffer_pool.clone(),
            p_frame,
            frame_len,
        )
    }

    pub fn checkpoint(&self) -> Result<CheckpointStatus> {
        let mut checkpoint_result = CheckpointResult::default();
        loop {
            let state = *self.checkpoint_state.borrow();
            trace!("pager_checkpoint(state={:?})", state);
            match state {
                CheckpointState::Checkpoint => {
                    let in_flight = self.checkpoint_inflight.clone();
                    match self.wal.borrow_mut().checkpoint(
                        self,
                        in_flight,
                        CheckpointMode::Passive,
                    )? {
                        CheckpointStatus::IO => return Ok(CheckpointStatus::IO),
                        CheckpointStatus::Done(res) => {
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
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.checkpoint_state
                            .replace(CheckpointState::CheckpointDone);
                    }
                }
                CheckpointState::CheckpointDone => {
                    return if *self.checkpoint_inflight.borrow() > 0 {
                        Ok(CheckpointStatus::IO)
                    } else {
                        self.checkpoint_state.replace(CheckpointState::Checkpoint);
                        Ok(CheckpointStatus::Done(checkpoint_result))
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

    pub fn checkpoint_shutdown(&self) -> Result<()> {
        let mut attempts = 0;
        {
            let mut wal = self.wal.borrow_mut();
            // fsync the wal syncronously before beginning checkpoint
            while let Ok(WalFsyncStatus::IO) = wal.sync() {
                if attempts >= 10 {
                    return Err(LimboError::InternalError(
                        "Failed to fsync WAL before final checkpoint, fd likely closed".into(),
                    ));
                }
                self.io.run_once()?;
                attempts += 1;
            }
        }
        self.wal_checkpoint()?;
        Ok(())
    }

    pub fn wal_checkpoint(&self) -> Result<CheckpointResult> {
        let checkpoint_result: CheckpointResult;
        loop {
            match self.wal.borrow_mut().checkpoint(
                self,
                Rc::new(RefCell::new(0)),
                CheckpointMode::Passive,
            ) {
                Ok(CheckpointStatus::IO) => {
                    let _ = self.io.run_once();
                }
                Ok(CheckpointStatus::Done(res)) => {
                    checkpoint_result = res;
                    break;
                }
                Err(err) => panic!("error while clearing cache {}", err),
            }
        }
        // TODO: only clear cache of things that are really invalidated
        self.page_cache.write().clear().map_err(|e| {
            LimboError::InternalError(format!("Failed to clear page cache: {:?}", e))
        })?;
        Ok(checkpoint_result)
    }

    // Providing a page is optional, if provided it will be used to avoid reading the page from disk.
    // This is implemented in accordance with sqlite freepage2() function.
    pub fn free_page(&self, page: Option<PageRef>, page_id: usize) -> Result<()> {
        tracing::trace!("free_page(page_id={})", page_id);
        const TRUNK_PAGE_HEADER_SIZE: usize = 8;
        const LEAF_ENTRY_SIZE: usize = 4;
        const RESERVED_SLOTS: usize = 2;

        const TRUNK_PAGE_NEXT_PAGE_OFFSET: usize = 0; // Offset to next trunk page pointer
        const TRUNK_PAGE_LEAF_COUNT_OFFSET: usize = 4; // Offset to leaf count

        if page_id < 2 || page_id > header_accessor::get_database_size(self)? as usize {
            return Err(LimboError::Corrupt(format!(
                "Invalid page number {} for free operation",
                page_id
            )));
        }

        let page = match page {
            Some(page) => {
                assert_eq!(page.get().id, page_id, "Page id mismatch");
                page
            }
            None => self.read_page(page_id)?,
        };

        header_accessor::set_freelist_pages(self, header_accessor::get_freelist_pages(self)? + 1)?;

        let trunk_page_id = header_accessor::get_freelist_trunk_page(self)?;

        if trunk_page_id != 0 {
            // Add as leaf to current trunk
            let trunk_page = self.read_page(trunk_page_id as usize)?;
            let trunk_page_contents = trunk_page.get().contents.as_ref().unwrap();
            let number_of_leaf_pages = trunk_page_contents.read_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET);

            // Reserve 2 slots for the trunk page header which is 8 bytes or 2*LEAF_ENTRY_SIZE
            let max_free_list_entries = (self.usable_space() / LEAF_ENTRY_SIZE) - RESERVED_SLOTS;

            if number_of_leaf_pages < max_free_list_entries as u32 {
                trunk_page.set_dirty();
                self.add_dirty(trunk_page_id as usize);

                trunk_page_contents
                    .write_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET, number_of_leaf_pages + 1);
                trunk_page_contents.write_u32(
                    TRUNK_PAGE_HEADER_SIZE + (number_of_leaf_pages as usize * LEAF_ENTRY_SIZE),
                    page_id as u32,
                );
                page.clear_uptodate();
                page.clear_loaded();

                return Ok(());
            }
        }

        // If we get here, need to make this page a new trunk
        page.set_dirty();
        self.add_dirty(page_id);

        let contents = page.get().contents.as_mut().unwrap();
        // Point to previous trunk
        contents.write_u32(TRUNK_PAGE_NEXT_PAGE_OFFSET, trunk_page_id);
        // Zero leaf count
        contents.write_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET, 0);
        // Update page 1 to point to new trunk
        header_accessor::set_freelist_trunk_page(self, page_id as u32)?;
        // Clear flags
        page.clear_uptodate();
        page.clear_loaded();
        Ok(())
    }

    pub fn allocate_page1(&self) -> Result<CursorResult<PageRef>> {
        let state = self.allocate_page1_state.borrow().clone();
        match state {
            AllocatePage1State::Start => {
                tracing::trace!("allocate_page1(Start)");
                self.is_empty.store(DB_STATE_INITIALIZING, Ordering::SeqCst);
                let mut default_header = DatabaseHeader::default();
                default_header.database_size += 1;
                let page = allocate_page(1, &self.buffer_pool, 0);

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
                Ok(CursorResult::IO)
            }
            AllocatePage1State::Writing {
                write_counter,
                page,
            } => {
                tracing::trace!("allocate_page1(Writing)");
                if *write_counter.borrow() > 0 {
                    return Ok(CursorResult::IO);
                }
                tracing::trace!("allocate_page1(Writing done)");
                let page1_ref = page.get();
                let page_key = PageCacheKey::new(page1_ref.get().id);
                let mut cache = self.page_cache.write();
                cache.insert(page_key, page1_ref.clone()).map_err(|e| {
                    LimboError::InternalError(format!(
                        "Failed to insert page 1 into cache: {:?}",
                        e
                    ))
                })?;
                self.is_empty.store(DB_STATE_INITIALIZED, Ordering::SeqCst);
                self.allocate_page1_state.replace(AllocatePage1State::Done);
                Ok(CursorResult::Ok(page1_ref.clone()))
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

    /*
        Gets a new page that increasing the size of the page or uses a free page.
        Currently free list pages are not yet supported.
    */
    // FIXME: handle no room in page cache
    #[allow(clippy::readonly_write_lock)]
    pub fn allocate_page(&self) -> Result<PageRef> {
        let old_db_size = header_accessor::get_database_size(self)?;
        #[allow(unused_mut)]
        let mut new_db_size = old_db_size + 1;

        tracing::debug!("allocate_page(database_size={})", new_db_size);

        #[cfg(not(feature = "omit_autovacuum"))]
        {
            //  If the following conditions are met, allocate a pointer map page, add to cache and increment the database size
            //  - autovacuum is enabled
            //  - the last page is a pointer map page
            if matches!(*self.auto_vacuum_mode.borrow(), AutoVacuumMode::Full)
                && is_ptrmap_page(new_db_size, header_accessor::get_page_size(self)? as usize)
            {
                let page = allocate_page(new_db_size as usize, &self.buffer_pool, 0);
                page.set_dirty();
                self.add_dirty(page.get().id);

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
                // we allocated a ptrmap page, so the next data page will be at new_db_size + 1
                new_db_size += 1;
            }
        }

        header_accessor::set_database_size(self, new_db_size)?;

        // FIXME: should reserve page cache entry before modifying the database
        let page = allocate_page(new_db_size as usize, &self.buffer_pool, 0);
        {
            // setup page and add to cache
            page.set_dirty();
            self.add_dirty(page.get().id);

            let page_key = PageCacheKey::new(page.get().id);
            let mut cache = self.page_cache.write();
            match cache.insert(page_key, page.clone()) {
                Err(CacheError::Full) => Err(LimboError::CacheFull),
                Err(_) => Err(LimboError::InternalError(
                    "Unknown error inserting page to cache".into(),
                )),
                Ok(_) => Ok(page),
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
                    "Failed to insert loaded page {} into cache: {:?}",
                    id, e
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

    pub fn rollback(&self) -> Result<(), LimboError> {
        self.dirty_pages.borrow_mut().clear();
        let mut cache = self.page_cache.write();
        cache.unset_dirty_all_pages();
        cache.clear().expect("failed to clear page cache");
        self.wal.borrow_mut().rollback()?;

        Ok(())
    }
}

pub fn allocate_page(page_id: usize, buffer_pool: &Rc<BufferPool>, offset: usize) -> PageRef {
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
                "Page {} is not mapped by the data page range [{}, {}] of ptrmap page {}",
                db_page_no_to_query, first_data_page_mapped, last_data_page_mapped, ptrmap_page_no
            )));
        }
        if is_ptrmap_page(db_page_no_to_query, page_size) {
            return Err(LimboError::InternalError(format!(
                "Page {} is a pointer map page and should not have an entry calculated this way.",
                db_page_no_to_query
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
    use crate::storage::sqlite3_ondisk::MIN_PAGE_SIZE;
    use crate::storage::wal::{WalFile, WalFileShared};

    pub fn run_until_done<T>(
        mut action: impl FnMut() -> Result<CursorResult<T>>,
        pager: &Pager,
    ) -> Result<T> {
        loop {
            match action()? {
                CursorResult::Ok(res) => {
                    return Ok(res);
                }
                CursorResult::IO => pager.io.run_once().unwrap(),
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
        let buffer_pool = Rc::new(BufferPool::new(Some(page_size as usize)));
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
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(())),
        )
        .unwrap();
        run_until_done(|| pager.allocate_page1(), &pager).unwrap();
        header_accessor::set_vacuum_mode_largest_root_page(&pager, 1).unwrap();
        pager.set_auto_vacuum_mode(AutoVacuumMode::Full);

        //  Allocate all the pages as btree root pages
        for _ in 0..initial_db_pages {
            match pager.btree_create(&CreateBTreeFlags::new_table()) {
                Ok(CursorResult::Ok(_root_page_id)) => (),
                Ok(CursorResult::IO) => {
                    panic!("test_pager_setup: btree_create returned CursorResult::IO unexpectedly");
                }
                Err(e) => {
                    panic!("test_pager_setup: btree_create failed: {:?}", e);
                }
            }
        }

        return pager;
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
            header_accessor::get_database_size(&pager).unwrap(),
            initial_db_pages + 2
        ); // (1+1) -> (header + ptrmap)

        //  Read the entry from the ptrmap page and verify it
        let entry = pager.ptrmap_get(db_page_to_update).unwrap();
        assert!(matches!(entry, CursorResult::Ok(Some(_))));
        let CursorResult::Ok(Some(entry)) = entry else {
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
            1 * PTRMAP_ENTRY_SIZE
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
            1 * PTRMAP_ENTRY_SIZE
        );
        assert_eq!(
            get_ptrmap_offset_in_page(108, 105, page_size).unwrap(),
            2 * PTRMAP_ENTRY_SIZE
        );
    }
}
