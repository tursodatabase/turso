use std::cell::UnsafeCell;
use std::collections::HashMap;
use tracing::{debug, trace};

use std::fmt::Formatter;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::{cell::RefCell, fmt, rc::Rc, sync::Arc};

use crate::fast_lock::SpinLock;
use crate::io::{File, SyncCompletion, IO};
use crate::result::LimboResult;
use crate::storage::sqlite3_ondisk::{
    begin_read_wal_frame, begin_write_wal_frame, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use crate::{Completion, LimboError, Page, Result};

use self::sqlite3_ondisk::{checksum_wal, PageContent, WAL_MAGIC_BE, WAL_MAGIC_LE};

use super::buffer_pool::BufferPool;
use super::pager::{PageRef, Pager};
use super::sqlite3_ondisk::{self, begin_write_btree_page, WalHeader};

pub const READMARK_NOT_USED: u32 = 0xffffffff;

pub const NO_LOCK: u32 = 0;
pub const SHARED_LOCK: u32 = 1;
pub const WRITE_LOCK: u32 = 2;

#[derive(Debug, Copy, Clone)]
pub struct CheckpointResult {
    /// number of frames in WAL
    pub num_wal_frames: u64,
    /// number of frames moved successfully from WAL to db file after checkpoint
    pub num_checkpointed_frames: u64,
}

impl Default for CheckpointResult {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointResult {
    pub fn new() -> Self {
        Self {
            num_wal_frames: 0,
            num_checkpointed_frames: 0,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum CheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

#[derive(Debug)]
struct LimboRwLock {
    lock: AtomicU32,
    nreads: AtomicU32,
    value: AtomicU32,
}

impl LimboRwLock {
    /// Shared lock. Returns true if it was successful, false if it couldn't lock it
    pub fn read(&mut self) -> bool {
        let lock = self.lock.load(Ordering::SeqCst);
        let ok = match lock {
            NO_LOCK => {
                let res = self.lock.compare_exchange(
                    lock,
                    SHARED_LOCK,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                let ok = res.is_ok();
                if ok {
                    self.nreads.fetch_add(1, Ordering::SeqCst);
                }
                ok
            }
            SHARED_LOCK => {
                self.nreads.fetch_add(1, Ordering::SeqCst);
                true
            }
            WRITE_LOCK => false,
            _ => unreachable!(),
        };
        tracing::trace!("read_lock({})", ok);
        ok
    }

    /// Locks exclusively. Returns true if it was successful, false if it couldn't lock it
    pub fn write(&mut self) -> bool {
        let lock = self.lock.load(Ordering::SeqCst);
        let ok = match lock {
            NO_LOCK => {
                let res = self.lock.compare_exchange(
                    lock,
                    WRITE_LOCK,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                res.is_ok()
            }
            SHARED_LOCK => {
                // no op
                false
            }
            WRITE_LOCK => true,
            _ => unreachable!(),
        };
        tracing::trace!("write_lock({})", ok);
        ok
    }

    /// Unlock the current held lock.
    pub fn unlock(&mut self) {
        let lock = self.lock.load(Ordering::SeqCst);
        tracing::trace!("unlock(lock={})", lock);
        match lock {
            NO_LOCK => {}
            SHARED_LOCK => {
                let prev = self.nreads.fetch_sub(1, Ordering::SeqCst);
                if prev == 1 {
                    let res = self.lock.compare_exchange(
                        lock,
                        NO_LOCK,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                    assert!(res.is_ok());
                }
            }
            WRITE_LOCK => {
                let res =
                    self.lock
                        .compare_exchange(lock, NO_LOCK, Ordering::SeqCst, Ordering::SeqCst);
                assert!(res.is_ok());
            }
            _ => unreachable!(),
        }
    }
}

/// Write-ahead log (WAL).
pub trait Wal {
    /// Begin a read transaction.
    fn begin_read_tx(&mut self) -> Result<LimboResult>;

    /// Begin a write transaction.
    fn begin_write_tx(&mut self) -> Result<LimboResult>;

    /// End a read transaction.
    fn end_read_tx(&self) -> Result<LimboResult>;

    /// End a write transaction.
    fn end_write_tx(&self) -> Result<LimboResult>;

    /// Find the latest frame containing a page.
    fn find_frame(&self, page_id: u64) -> Result<Option<u64>>;

    /// Read a frame from the WAL.
    fn read_frame(&self, frame_id: u64, page: PageRef, buffer_pool: Rc<BufferPool>) -> Result<()>;

    /// Write a frame to the WAL.
    fn append_frame(
        &mut self,
        page: PageRef,
        db_size: u32,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<()>;

    fn should_checkpoint(&self) -> bool;
    fn checkpoint(
        &mut self,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
        mode: CheckpointMode,
    ) -> Result<CheckpointStatus>;
    fn sync(&mut self) -> Result<CheckpointStatus>;
    fn get_max_frame_in_wal(&self) -> u64;
    fn get_max_frame(&self) -> u64;
    fn get_min_frame(&self) -> u64;
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
    WritePage,
    WaitWritePage,
    Done,
}

#[derive(Debug, Copy, Clone)]
pub enum CheckpointStatus {
    Done(CheckpointResult),
    IO,
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
    page: PageRef,
    state: CheckpointState,
    min_frame: u64,
    max_frame: u64,
    current_page: u64,
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
    buffer_pool: Rc<BufferPool>,

    sync_state: RefCell<SyncState>,
    syncing: Rc<RefCell<bool>>,
    page_size: usize,

    shared: Arc<UnsafeCell<WalFileShared>>,
    ongoing_checkpoint: OngoingCheckpoint,
    checkpoint_threshold: usize,
    // min and max frames for this connection
    /// This is the index to the read_lock in WalFileShared that we are holding. This lock contains
    /// the max frame for this connection.
    max_frame_read_lock_index: usize,
    /// Max frame allowed to lookup range=(minframe..max_frame)
    max_frame: u64,
    /// Start of range to look for frames range=(minframe..max_frame)
    min_frame: u64,
}

impl fmt::Debug for WalFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFile")
            .field("sync_state", &self.sync_state)
            .field("syncing", &self.syncing)
            .field("page_size", &self.page_size)
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

// TODO(pere): lock only important parts + pin WalFileShared
/// WalFileShared is the part of a WAL that will be shared between threads. A wal has information
/// that needs to be communicated between threads so this struct does the job.
#[allow(dead_code)]
pub struct WalFileShared {
    wal_header: Arc<SpinLock<WalHeader>>,
    min_frame: AtomicU64,
    max_frame: AtomicU64,
    nbackfills: AtomicU64,
    // Frame cache maps a Page to all the frames it has stored in WAL in ascending order.
    // This is to easily find the frame it must checkpoint each connection if a checkpoint is
    // necessary.
    // One difference between SQLite and limbo is that we will never support multi process, meaning
    // we don't need WAL's index file. So we can do stuff like this without shared memory.
    // TODO: this will need refactoring because this is incredible memory inefficient.
    frame_cache: Arc<SpinLock<HashMap<u64, Vec<u64>>>>,
    // Another memory inefficient array made to just keep track of pages that are in frame_cache.
    pages_in_frames: Arc<SpinLock<Vec<u64>>>,
    last_checksum: (u32, u32), // Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    file: Arc<dyn File>,
    /// read_locks is a list of read locks that can coexist with the max_frame number stored in
    /// value. There is a limited amount because and unbounded amount of connections could be
    /// fatal. Therefore, for now we copy how SQLite behaves with limited amounts of read max
    /// frames that is equal to 5
    read_locks: [LimboRwLock; 5],
    /// There is only one write allowed in WAL mode. This lock takes care of ensuring there is only
    /// one used.
    write_lock: LimboRwLock,
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

impl Wal for WalFile {
    /// Begin a read transaction.
    fn begin_read_tx(&mut self) -> Result<LimboResult> {
        let max_frame_in_wal = self.get_shared().max_frame.load(Ordering::SeqCst);

        let mut max_read_mark = 0;
        let mut max_read_mark_index = -1;
        // Find the largest mark we can find, ignore frames that are impossible to be in range and
        // that are not set
        for (index, lock) in self.get_shared().read_locks.iter().enumerate() {
            let this_mark = lock.value.load(Ordering::SeqCst);
            if this_mark > max_read_mark && this_mark <= max_frame_in_wal as u32 {
                max_read_mark = this_mark;
                max_read_mark_index = index as i64;
            }
        }

        // If we didn't find any mark or we can update, let's update them
        if (max_read_mark as u64) < max_frame_in_wal || max_read_mark_index == -1 {
            for (index, lock) in self.get_shared().read_locks.iter_mut().enumerate() {
                let busy = !lock.write();
                if !busy {
                    // If this was busy then it must mean >1 threads tried to set this read lock
                    lock.value.store(max_frame_in_wal as u32, Ordering::SeqCst);
                    max_read_mark = max_frame_in_wal as u32;
                    max_read_mark_index = index as i64;
                    lock.unlock();
                    break;
                }
            }
        }

        if max_read_mark_index == -1 {
            return Ok(LimboResult::Busy);
        }

        let shared = self.get_shared();
        {
            let lock = &mut shared.read_locks[max_read_mark_index as usize];
            let busy = !lock.read();
            if busy {
                return Ok(LimboResult::Busy);
            }
        }
        self.min_frame = shared.nbackfills.load(Ordering::SeqCst) + 1;
        self.max_frame_read_lock_index = max_read_mark_index as usize;
        self.max_frame = max_read_mark as u64;
        tracing::debug!(
            "begin_read_tx(min_frame={}, max_frame={}, lock={}, max_frame_in_wal={})",
            self.min_frame,
            self.max_frame,
            self.max_frame_read_lock_index,
            max_frame_in_wal
        );
        Ok(LimboResult::Ok)
    }

    /// End a read transaction.
    #[inline(always)]
    fn end_read_tx(&self) -> Result<LimboResult> {
        tracing::debug!("end_read_tx");
        let read_lock = &mut self.get_shared().read_locks[self.max_frame_read_lock_index];
        read_lock.unlock();
        Ok(LimboResult::Ok)
    }

    /// Begin a write transaction
    fn begin_write_tx(&mut self) -> Result<LimboResult> {
        let busy = !self.get_shared().write_lock.write();
        tracing::debug!("begin_write_transaction(busy={})", busy);
        if busy {
            return Ok(LimboResult::Busy);
        }
        Ok(LimboResult::Ok)
    }

    /// End a write transaction
    fn end_write_tx(&self) -> Result<LimboResult> {
        tracing::debug!("end_write_txn");
        self.get_shared().write_lock.unlock();
        Ok(LimboResult::Ok)
    }

    /// Find the latest frame containing a page.
    fn find_frame(&self, page_id: u64) -> Result<Option<u64>> {
        let shared = self.get_shared();
        let frames = shared.frame_cache.lock();
        let frames = frames.get(&page_id);
        if frames.is_none() {
            return Ok(None);
        }
        let frames = frames.unwrap();
        for frame in frames.iter().rev() {
            if *frame <= self.max_frame {
                return Ok(Some(*frame));
            }
        }
        Ok(None)
    }

    /// Read a frame from the WAL.
    fn read_frame(&self, frame_id: u64, page: PageRef, buffer_pool: Rc<BufferPool>) -> Result<()> {
        debug!("read_frame({})", frame_id);
        let offset = self.frame_offset(frame_id);
        page.set_locked();
        begin_read_wal_frame(
            &self.get_shared().file,
            self.page_size,
            offset + WAL_FRAME_HEADER_SIZE,
            buffer_pool,
            page,
        )?;
        Ok(())
    }

    /// Write a frame to the WAL.
    fn append_frame(
        &mut self,
        page: PageRef,
        db_size: u32,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<()> {
        let page_id = page.get().id;
        let shared = self.get_shared();
        let max_frame = shared.max_frame.load(Ordering::SeqCst);
        let frame_id = if max_frame == 0 { 1 } else { max_frame + 1 };
        let offset = self.frame_offset(frame_id);
        tracing::debug!(
            "append_frame(frame={}, offset={}, page_id={})",
            frame_id,
            offset,
            page_id
        );
        let header = shared.wal_header.clone();
        let header = header.lock();
        let checksums = shared.last_checksum;
        let checksums = begin_write_wal_frame(
            &shared.file,
            offset,
            &page,
            self.page_size as u16,
            db_size,
            write_counter,
            &header,
            checksums,
        )?;
        shared.last_checksum = checksums;
        shared.max_frame.store(frame_id, Ordering::SeqCst);
        {
            let mut frame_cache = shared.frame_cache.lock();
            let frames = frame_cache.get_mut(&(page_id as u64));
            match frames {
                Some(frames) => frames.push(frame_id),
                None => {
                    frame_cache.insert(page_id as u64, vec![frame_id]);
                    shared.pages_in_frames.lock().push(page_id as u64);
                }
            }
        }
        Ok(())
    }

    fn should_checkpoint(&self) -> bool {
        let shared = self.get_shared();
        let frame_id = shared.max_frame.load(Ordering::SeqCst) as usize;
        frame_id >= self.checkpoint_threshold
    }

    fn checkpoint(
        &mut self,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
        mode: CheckpointMode,
    ) -> Result<CheckpointStatus> {
        assert!(
            matches!(mode, CheckpointMode::Passive),
            "only passive mode supported for now"
        );
        'checkpoint_loop: loop {
            let state = self.ongoing_checkpoint.state;
            debug!("checkpoint(state={:?})", state);
            match state {
                CheckpointState::Start => {
                    // TODO(pere): check what frames are safe to checkpoint between many readers!
                    self.ongoing_checkpoint.min_frame = self.min_frame;
                    let shared = self.get_shared();
                    let mut max_safe_frame = shared.max_frame.load(Ordering::SeqCst);
                    for (read_lock_idx, read_lock) in shared.read_locks.iter_mut().enumerate() {
                        let this_mark = read_lock.value.load(Ordering::SeqCst);
                        if this_mark < max_safe_frame as u32 {
                            let busy = !read_lock.write();
                            if !busy {
                                let new_mark = if read_lock_idx == 0 {
                                    max_safe_frame as u32
                                } else {
                                    READMARK_NOT_USED
                                };
                                read_lock.value.store(new_mark, Ordering::SeqCst);
                                read_lock.unlock();
                            } else {
                                max_safe_frame = this_mark as u64;
                            }
                        }
                    }
                    self.ongoing_checkpoint.max_frame = max_safe_frame;
                    self.ongoing_checkpoint.current_page = 0;
                    self.ongoing_checkpoint.state = CheckpointState::ReadFrame;
                    trace!(
                        "checkpoint_start(min_frame={}, max_frame={})",
                        self.ongoing_checkpoint.max_frame,
                        self.ongoing_checkpoint.min_frame
                    );
                }
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
                        self.ongoing_checkpoint.state = CheckpointState::Done;
                        continue 'checkpoint_loop;
                    }
                    let page = pages_in_frames[self.ongoing_checkpoint.current_page as usize];
                    let frames = frame_cache
                        .get(&page)
                        .expect("page must be in frame cache if it's in list");

                    for frame in frames.iter().rev() {
                        if *frame >= min_frame && *frame <= max_frame {
                            debug!(
                                "checkpoint page(state={:?}, page={}, frame={})",
                                state, page, *frame
                            );
                            self.ongoing_checkpoint.page.get().id = page as usize;

                            self.read_frame(
                                *frame,
                                self.ongoing_checkpoint.page.clone(),
                                self.buffer_pool.clone(),
                            )?;
                            self.ongoing_checkpoint.state = CheckpointState::WaitReadFrame;
                            self.ongoing_checkpoint.current_page += 1;
                            continue 'checkpoint_loop;
                        }
                    }
                    self.ongoing_checkpoint.current_page += 1;
                }
                CheckpointState::WaitReadFrame => {
                    if self.ongoing_checkpoint.page.is_locked() {
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.ongoing_checkpoint.state = CheckpointState::WritePage;
                    }
                }
                CheckpointState::WritePage => {
                    self.ongoing_checkpoint.page.set_dirty();
                    begin_write_btree_page(
                        pager,
                        &self.ongoing_checkpoint.page,
                        write_counter.clone(),
                    )?;
                    self.ongoing_checkpoint.state = CheckpointState::WaitWritePage;
                }
                CheckpointState::WaitWritePage => {
                    if *write_counter.borrow() > 0 {
                        return Ok(CheckpointStatus::IO);
                    }
                    let shared = self.get_shared();
                    if (self.ongoing_checkpoint.current_page as usize)
                        < shared.pages_in_frames.lock().len()
                    {
                        self.ongoing_checkpoint.state = CheckpointState::ReadFrame;
                    } else {
                        self.ongoing_checkpoint.state = CheckpointState::Done;
                    }
                }
                CheckpointState::Done => {
                    if *write_counter.borrow() > 0 {
                        return Ok(CheckpointStatus::IO);
                    }
                    let shared = self.get_shared();

                    // Record two num pages fields to return as checkpoint result to caller.
                    // Ref: pnLog, pnCkpt on https://www.sqlite.org/c3ref/wal_checkpoint_v2.html
                    let checkpoint_result = CheckpointResult {
                        num_wal_frames: shared.max_frame.load(Ordering::SeqCst),
                        num_checkpointed_frames: self.ongoing_checkpoint.max_frame,
                    };
                    let everything_backfilled = shared.max_frame.load(Ordering::SeqCst)
                        == self.ongoing_checkpoint.max_frame;
                    if everything_backfilled {
                        // Here we know that we backfilled everything, therefore we can safely
                        // reset the wal.
                        shared.frame_cache.lock().clear();
                        shared.pages_in_frames.lock().clear();
                        shared.max_frame.store(0, Ordering::SeqCst);
                        shared.nbackfills.store(0, Ordering::SeqCst);
                        // TODO(pere): truncate wal file here.
                    } else {
                        shared
                            .nbackfills
                            .store(self.ongoing_checkpoint.max_frame, Ordering::SeqCst);
                    }
                    self.ongoing_checkpoint.state = CheckpointState::Start;
                    return Ok(CheckpointStatus::Done(checkpoint_result));
                }
            }
        }
    }

    fn sync(&mut self) -> Result<CheckpointStatus> {
        let state = *self.sync_state.borrow();
        match state {
            SyncState::NotSyncing => {
                let shared = self.get_shared();
                debug!("wal_sync");
                {
                    let syncing = self.syncing.clone();
                    *syncing.borrow_mut() = true;
                    let completion = Completion::Sync(SyncCompletion {
                        complete: Box::new(move |_| {
                            debug!("wal_sync finish");
                            *syncing.borrow_mut() = false;
                        }),
                    });
                    shared.file.sync(completion)?;
                }
                self.sync_state.replace(SyncState::Syncing);
                Ok(CheckpointStatus::IO)
            }
            SyncState::Syncing => {
                if *self.syncing.borrow() {
                    Ok(CheckpointStatus::IO)
                } else {
                    self.sync_state.replace(SyncState::NotSyncing);
                    let checkpoint_result = CheckpointResult {
                        num_wal_frames: self.max_frame,
                        num_checkpointed_frames: self.ongoing_checkpoint.max_frame,
                    };
                    Ok(CheckpointStatus::Done(checkpoint_result))
                }
            }
        }
    }

    fn get_max_frame_in_wal(&self) -> u64 {
        self.get_shared().max_frame.load(Ordering::SeqCst)
    }

    fn get_max_frame(&self) -> u64 {
        self.max_frame
    }

    fn get_min_frame(&self) -> u64 {
        self.min_frame
    }
}

impl WalFile {
    pub fn new(
        io: Arc<dyn IO>,
        page_size: usize,
        shared: Arc<UnsafeCell<WalFileShared>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Self {
        let checkpoint_page = Arc::new(Page::new(0));
        let buffer = buffer_pool.get_page(Some(page_size + WAL_FRAME_HEADER_SIZE));
        let buffer_pool = buffer_pool.clone();
        checkpoint_page.get().contents = Some(PageContent::new(0, buffer));
        Self {
            io,
            shared,
            ongoing_checkpoint: OngoingCheckpoint {
                page: checkpoint_page,
                state: CheckpointState::Start,
                min_frame: 0,
                max_frame: 0,
                current_page: 0,
            },
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_threshold: 1000,
            page_size,
            buffer_pool,
            sync_state: RefCell::new(SyncState::NotSyncing),
            max_frame: 0,
            min_frame: 0,
            max_frame_read_lock_index: 0,
        }
    }

    fn frame_offset(&self, frame_id: u64) -> usize {
        assert!(frame_id > 0, "Frame ID must be 1-based");
        let page_size = self.page_size;
        let page_offset = (frame_id - 1) * (page_size + WAL_FRAME_HEADER_SIZE) as u64;
        let offset = WAL_HEADER_SIZE as u64 + page_offset;
        offset as usize
    }

    #[allow(clippy::mut_from_ref)]
    fn get_shared(&self) -> &mut WalFileShared {
        unsafe { self.shared.get().as_mut().unwrap() }
    }
}

impl WalFileShared {
    pub fn open_shared(
        io: &Arc<dyn IO>,
        path: &str,
        page_size: u16,
    ) -> Result<Arc<UnsafeCell<WalFileShared>>> {
        let file = io.open_file(path, crate::io::OpenFlags::Create, false)?;
        let header = if file.size()? > 0 {
            let wal_header = match sqlite3_ondisk::begin_read_wal_header(&file) {
                Ok(header) => header,
                Err(err) => return Err(LimboError::ParseError(err.to_string())),
            };
            tracing::info!("recover not implemented yet");
            // TODO: Return a completion instead.
            io.run_once()?;
            wal_header
        } else {
            let magic = if cfg!(target_endian = "big") {
                WAL_MAGIC_BE
            } else {
                WAL_MAGIC_LE
            };
            let mut wal_header = WalHeader {
                magic,
                file_format: 3007000,
                page_size: page_size as u32,
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
            sqlite3_ondisk::begin_write_wal_header(&file, &wal_header)?;
            Arc::new(SpinLock::new(wal_header))
        };
        let checksum = {
            let checksum = header.lock();
            (checksum.checksum_1, checksum.checksum_2)
        };
        let shared = WalFileShared {
            wal_header: header,
            min_frame: AtomicU64::new(0),
            max_frame: AtomicU64::new(0),
            nbackfills: AtomicU64::new(0),
            frame_cache: Arc::new(SpinLock::new(HashMap::new())),
            last_checksum: checksum,
            file,
            pages_in_frames: Arc::new(SpinLock::new(Vec::new())),
            read_locks: [
                LimboRwLock {
                    lock: AtomicU32::new(NO_LOCK),
                    nreads: AtomicU32::new(0),
                    value: AtomicU32::new(READMARK_NOT_USED),
                },
                LimboRwLock {
                    lock: AtomicU32::new(NO_LOCK),
                    nreads: AtomicU32::new(0),
                    value: AtomicU32::new(READMARK_NOT_USED),
                },
                LimboRwLock {
                    lock: AtomicU32::new(NO_LOCK),
                    nreads: AtomicU32::new(0),
                    value: AtomicU32::new(READMARK_NOT_USED),
                },
                LimboRwLock {
                    lock: AtomicU32::new(NO_LOCK),
                    nreads: AtomicU32::new(0),
                    value: AtomicU32::new(READMARK_NOT_USED),
                },
                LimboRwLock {
                    lock: AtomicU32::new(NO_LOCK),
                    nreads: AtomicU32::new(0),
                    value: AtomicU32::new(READMARK_NOT_USED),
                },
            ],
            write_lock: LimboRwLock {
                lock: AtomicU32::new(NO_LOCK),
                nreads: AtomicU32::new(0),
                value: AtomicU32::new(READMARK_NOT_USED),
            },
        };
        Ok(Arc::new(UnsafeCell::new(shared)))
    }
}
