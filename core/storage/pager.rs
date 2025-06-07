use crate::fast_lock::SpinLock;
use crate::result::LimboResult;
use crate::storage::btree::BTreePageInner;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::sqlite3_ondisk::{
    self, DatabaseHeader, PageContent, PageType, DATABASE_HEADER_PAGE_ID,
};
use crate::storage::wal::{CheckpointResult, Wal, WalFsyncStatus};
use crate::Completion;
use crate::{Buffer, LimboError, Result};
use parking_lot::RwLock;
use std::cell::{RefCell, UnsafeCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::trace;

use super::btree::BTreePage;
use super::page_cache::{CacheError, CacheResizeResult, DumbLruPageCache, PageCacheKey};
use super::wal::{CheckpointMode, CheckpointStatus};

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
    buffer_pool: Rc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    dirty_pages: Rc<RefCell<HashSet<usize>>>,
    pub db_header: Arc<SpinLock<DatabaseHeader>>,

    flush_info: RefCell<FlushInfo>,
    checkpoint_state: RefCell<CheckpointState>,
    checkpoint_inflight: Rc<RefCell<usize>>,
    syncing: Rc<RefCell<bool>>,
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
}

impl Pager {
    /// Begins opening a database by reading the database header.
    pub fn begin_open(db_file: Arc<dyn DatabaseStorage>) -> Result<Arc<SpinLock<DatabaseHeader>>> {
        sqlite3_ondisk::begin_read_database_header(db_file)
    }

    /// Completes opening a database by initializing the Pager with the database header.
    pub fn finish_open(
        db_header_ref: Arc<SpinLock<DatabaseHeader>>,
        db_file: Arc<dyn DatabaseStorage>,
        wal: Rc<RefCell<dyn Wal>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<DumbLruPageCache>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Result<Self> {
        Ok(Self {
            db_file,
            wal,
            page_cache,
            io,
            dirty_pages: Rc::new(RefCell::new(HashSet::new())),
            db_header: db_header_ref.clone(),
            flush_info: RefCell::new(FlushInfo {
                state: FlushState::Start,
                in_flight_writes: Rc::new(RefCell::new(0)),
            }),
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_state: RefCell::new(CheckpointState::Checkpoint),
            checkpoint_inflight: Rc::new(RefCell::new(0)),
            buffer_pool,
        })
    }

    // FIXME: handle no room in page cache
    pub fn btree_create(&self, flags: &CreateBTreeFlags) -> u32 {
        let page_type = match flags {
            _ if flags.is_table() => PageType::TableLeaf,
            _ if flags.is_index() => PageType::IndexLeaf,
            _ => unreachable!("Invalid flags state"),
        };
        let page = self.do_allocate_page(page_type, 0);
        let id = page.get().get().id;
        id as u32
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
    pub fn do_allocate_page(&self, page_type: PageType, offset: usize) -> BTreePage {
        let page = self.allocate_page().unwrap();
        let page = Arc::new(BTreePageInner {
            page: RefCell::new(page),
        });
        crate::btree_init_page(&page, page_type, offset, self.usable_space() as u16);
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
        let db_header = self.db_header.lock();
        (db_header.get_page_size() - db_header.reserved_space as u32) as usize
    }

    #[inline(always)]
    pub fn begin_read_tx(&self) -> Result<LimboResult> {
        self.wal.borrow_mut().begin_read_tx()
    }

    #[inline(always)]
    pub fn begin_write_tx(&self) -> Result<LimboResult> {
        self.wal.borrow_mut().begin_write_tx()
    }

    pub fn end_tx(&self) -> Result<PagerCacheflushStatus> {
        let cacheflush_status = self.cacheflush()?;
        return match cacheflush_status {
            PagerCacheflushStatus::IO => Ok(PagerCacheflushStatus::IO),
            PagerCacheflushStatus::Done(_) => {
                self.wal.borrow().end_write_tx()?;
                self.wal.borrow().end_read_tx()?;
                Ok(cacheflush_status)
            }
        };
    }

    pub fn end_read_tx(&self) -> Result<()> {
        self.wal.borrow().end_read_tx()?;
        Ok(())
    }

    /// Reads a page from the database.
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

    /// Writes the database header.
    pub fn write_database_header(&self, header: &DatabaseHeader) -> Result<()> {
        let header_page = self.read_page(DATABASE_HEADER_PAGE_ID)?;
        while header_page.is_locked() {
            // FIXME: we should never run io here!
            self.io.run_once()?;
        }
        header_page.set_dirty();
        self.add_dirty(DATABASE_HEADER_PAGE_ID);

        let contents = header_page.get().contents.as_ref().unwrap();
        contents.write_database_header(&header);

        Ok(())
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
                    let db_size = self.db_header.lock().database_size;
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
        return wal.read_frame_raw(
            frame_no.into(),
            self.buffer_pool.clone(),
            p_frame,
            frame_len,
        );
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

    pub fn wal_checkpoint(&self) -> CheckpointResult {
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
        self.page_cache
            .write()
            .clear()
            .expect("Failed to clear page cache");
        checkpoint_result
    }

    // Providing a page is optional, if provided it will be used to avoid reading the page from disk.
    // This is implemented in accordance with sqlite freepage2() function.
    pub fn free_page(&self, page: Option<PageRef>, page_id: usize) -> Result<()> {
        const TRUNK_PAGE_HEADER_SIZE: usize = 8;
        const LEAF_ENTRY_SIZE: usize = 4;
        const RESERVED_SLOTS: usize = 2;

        const TRUNK_PAGE_NEXT_PAGE_OFFSET: usize = 0; // Offset to next trunk page pointer
        const TRUNK_PAGE_LEAF_COUNT_OFFSET: usize = 4; // Offset to leaf count

        if page_id < 2 || page_id > self.db_header.lock().database_size as usize {
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

        self.db_header.lock().freelist_pages += 1;

        let trunk_page_id = self.db_header.lock().freelist_trunk_page;

        if trunk_page_id != 0 {
            // Add as leaf to current trunk
            let trunk_page = self.read_page(trunk_page_id as usize)?;
            let trunk_page_contents = trunk_page.get().contents.as_ref().unwrap();
            let number_of_leaf_pages = trunk_page_contents.read_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET);

            // Reserve 2 slots for the trunk page header which is 8 bytes or 2*LEAF_ENTRY_SIZE
            let max_free_list_entries = (self.usable_size() / LEAF_ENTRY_SIZE) - RESERVED_SLOTS;

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
        self.db_header.lock().freelist_trunk_page = page_id as u32;
        // Clear flags
        page.clear_uptodate();
        page.clear_loaded();
        Ok(())
    }

    /*
        Gets a new page that increasing the size of the page or uses a free page.
        Currently free list pages are not yet supported.
    */
    // FIXME: handle no room in page cache
    #[allow(clippy::readonly_write_lock)]
    pub fn allocate_page(&self) -> Result<PageRef> {
        let header = &self.db_header;
        let mut header = header.lock();
        header.database_size += 1;
        // update database size
        self.write_database_header(&mut header)?;

        // FIXME: should reserve page cache entry before modifying the database
        let page = allocate_page(header.database_size as usize, &self.buffer_pool, 0);
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
        let db_header = self.db_header.lock();
        (db_header.get_page_size() - db_header.reserved_space as u32) as usize
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
