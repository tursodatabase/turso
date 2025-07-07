use crate::result::LimboResult;
use crate::storage::btree::BTreePageInner;

use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::header_accessor;
use crate::storage::wal::{CheckpointResult, Wal, WalFsyncStatus};
use crate::types::CursorResult;
use crate::{Buffer, Connection, LimboError, Result};
use crate::{Completion, WalFile};
use parking_lot::RwLock;
use std::cell::{OnceCell, RefCell, UnsafeCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{instrument, trace, Level};

use super::btree::BTreePage;
use super::page_cache::{CacheError, CacheResizeResult, DumbLruPageCache, PageCacheKey};
use super::sqlite3_ondisk::{
    self, begin_write_btree_page, DatabaseHeader, PageContent, PageType, DATABASE_HEADER_SIZE,
};
use super::wal::{CheckpointMode, CheckpointStatus};

#[cfg(not(feature = "omit_autovacuum"))]
use crate::storage::btree::ptrmap::*;

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
    pub buffer_pool: Arc<BufferPool>,
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
        buffer_pool: Arc<BufferPool>,
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
    #[instrument(skip_all, level = Level::INFO)]
    pub fn begin_read_tx(&self) -> Result<CursorResult<LimboResult>> {
        // We allocate the first page lazily in the first transaction
        match self.maybe_allocate_page1()? {
            CursorResult::Ok(_) => {}
            CursorResult::IO => return Ok(CursorResult::IO),
        }
        Ok(CursorResult::Ok(self.wal.borrow_mut().begin_read_tx()?))
    }

    #[instrument(skip_all, level = Level::INFO)]
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
    #[instrument(skip_all, level = Level::INFO)]
    pub fn begin_write_tx(&self) -> Result<CursorResult<LimboResult>> {
        // TODO(Diego): The only possibly allocate page1 here is because OpenEphemeral needs a write transaction
        // we should have a unique API to begin transactions, something like sqlite3BtreeBeginTrans
        match self.maybe_allocate_page1()? {
            CursorResult::Ok(_) => {}
            CursorResult::IO => return Ok(CursorResult::IO),
        }
        Ok(CursorResult::Ok(self.wal.borrow_mut().begin_write_tx()?))
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn end_tx(
        &self,
        rollback: bool,
        schema_did_change: bool,
        connection: &Connection,
        wal_checkpoint_disabled: bool,
    ) -> Result<PagerCacheflushStatus> {
        tracing::trace!("end_tx(rollback={})", rollback);
        if rollback {
            self.wal.borrow().end_write_tx()?;
            self.wal.borrow().end_read_tx()?;
            return Ok(PagerCacheflushStatus::Done(PagerCacheflushResult::Rollback));
        }
        let cacheflush_status = self.cacheflush(wal_checkpoint_disabled)?;
        match cacheflush_status {
            PagerCacheflushStatus::IO => Ok(PagerCacheflushStatus::IO),
            PagerCacheflushStatus::Done(_) => {
                let maybe_schema_pair = if schema_did_change {
                    let schema = connection.schema.borrow().clone();
                    // Lock first before writing to the database schema in case someone tries to read the schema before it's updated
                    let db_schema = connection._db.schema.write();
                    Some((schema, db_schema))
                } else {
                    None
                };
                self.wal.borrow().end_write_tx()?;
                self.wal.borrow().end_read_tx()?;
                if let Some((schema, mut db_schema)) = maybe_schema_pair {
                    *db_schema = schema;
                }
                Ok(cacheflush_status)
            }
        }
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn end_read_tx(&self) -> Result<()> {
        self.wal.borrow().end_read_tx()?;
        Ok(())
    }

    /// Reads a page from the database.
    #[tracing::instrument(skip_all, level = Level::INFO)]
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
    #[instrument(skip_all, level = Level::INFO)]
    pub fn cacheflush(&self, wal_checkpoint_disabled: bool) -> Result<PagerCacheflushStatus> {
        let mut checkpoint_result = CheckpointResult::default();
        let res = loop {
            let state = self.flush_info.borrow().state;
            trace!(?state);
            match state {
                FlushState::Start => {
                    let db_size = header_accessor::get_database_size(self)?;
                    for (dirty_page_idx, page_id) in self.dirty_pages.borrow().iter().enumerate() {
                        let is_last_frame = dirty_page_idx == self.dirty_pages.borrow().len() - 1;
                        let mut cache = self.page_cache.write();
                        let page_key = PageCacheKey::new(*page_id);
                        let page = cache.get(&page_key).expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                        let page_type = page.get().contents.as_ref().unwrap().maybe_page_type();
                        trace!("cacheflush(page={}, page_type={:?}", page_id, page_type);
                        let db_size = if is_last_frame { db_size } else { 0 };
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
                    } else {
                        return Ok(PagerCacheflushStatus::IO);
                    }
                }
                FlushState::SyncWal => {
                    if WalFsyncStatus::IO == self.wal.borrow_mut().sync()? {
                        return Ok(PagerCacheflushStatus::IO);
                    }

                    if wal_checkpoint_disabled || !self.wal.borrow().should_checkpoint() {
                        self.flush_info.borrow_mut().state = FlushState::Start;
                        break PagerCacheflushResult::WalWritten;
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
                        break PagerCacheflushResult::Checkpointed(checkpoint_result);
                    }
                }
            }
        };
        // We should only signal that we finished appenind frames after wal sync to avoid inconsistencies when sync fails
        self.wal.borrow_mut().finish_append_frames_commit()?;
        Ok(PagerCacheflushStatus::Done(res))
    }

    #[instrument(skip_all, level = Level::INFO)]
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

    #[instrument(skip_all, level = Level::INFO, target = "pager_checkpoint",)]
    pub fn checkpoint(&self) -> Result<CheckpointStatus> {
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

    pub fn checkpoint_shutdown(&self, wal_checkpoint_disabled: bool) -> Result<()> {
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
        self.wal_checkpoint(wal_checkpoint_disabled)?;
        Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn wal_checkpoint(&self, wal_checkpoint_disabled: bool) -> Result<CheckpointResult> {
        if wal_checkpoint_disabled {
            return Ok(CheckpointResult {
                num_wal_frames: 0,
                num_checkpointed_frames: 0,
            });
        }
        let checkpoint_result: CheckpointResult;
        loop {
            match self.wal.borrow_mut().checkpoint(
                self,
                Rc::new(RefCell::new(0)),
                CheckpointMode::Passive,
            ) {
                Ok(CheckpointStatus::IO) => {
                    self.io.run_once()?;
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
    #[instrument(skip_all, level = Level::INFO)]
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

    #[instrument(skip_all, level = Level::INFO)]
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

                let page1 = BTreePageInner::new(page);
                // Create the sqlite_schema table, for this we just need to create the btree page
                // for the first page of the database which is basically like any other btree page
                // but with a 100 byte offset, so we just init the page so that sqlite understands
                // this is a correct page.
                page1.init(
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
    #[instrument(skip_all, level = Level::INFO)]
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

    #[instrument(skip_all, level = Level::INFO)]
    pub fn rollback(
        &self,
        schema_did_change: bool,
        connection: &Connection,
    ) -> Result<(), LimboError> {
        tracing::debug!(schema_did_change);
        self.dirty_pages.borrow_mut().clear();
        let mut cache = self.page_cache.write();
        cache.unset_dirty_all_pages();
        cache.clear().expect("failed to clear page cache");
        if schema_did_change {
            let prev_schema = connection._db.schema.read().clone();
            connection.schema.replace(prev_schema);
        }
        self.wal.borrow_mut().rollback()?;

        Ok(())
    }
}

pub fn allocate_page(page_id: usize, buffer_pool: &Arc<BufferPool>, offset: usize) -> PageRef {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use crate::{
        storage::page_cache::{DumbLruPageCache, PageCacheKey},
        Page,
    };

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
