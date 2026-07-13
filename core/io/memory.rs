use super::{Buffer, Clock, Completion, File, OpenFlags, IO};
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::FileSyncType;
use crate::sync::{Mutex, RwLock};
use crate::turso_assert;
use crate::Result;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tracing::debug;

pub struct MemoryIO {
    files: Arc<Mutex<HashMap<String, Arc<MemoryFile>>>>,
}

// TODO: page size flag
pub(super) const PAGE_SIZE: usize = 4096;
pub(super) type MemPage = Box<[u8; PAGE_SIZE]>;

struct MemStoreInner {
    pages: BTreeMap<usize, MemPage>,
    size: u64,
}

impl MemoryIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        debug!("Using IO backend 'memory'");
        Self {
            files: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

impl Default for MemoryIO {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MemoryIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for MemoryIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        let mut files = self.files.lock();
        if !files.contains_key(path) && !flags.contains(OpenFlags::Create) {
            return Err(crate::error::CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                "open",
            )
            .into());
        }
        if !files.contains_key(path) {
            files.insert(
                path.to_string(),
                Arc::new(MemoryFile {
                    path: path.to_string(),
                    store: MemStore::new(),
                }),
            );
        }
        Ok(files
            .get(path)
            .ok_or_else(|| {
                crate::LimboError::InternalError("file should exist after insert".to_string())
            })?
            .clone())
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock();
        files.remove(path);
        Ok(())
    }

    fn file_id(&self, path: &str) -> Result<super::FileId> {
        Ok(super::FileId::from_path_hash(path))
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }
}

/// In-memory page-backed storage shared by the synchronous [`MemoryIO`] and
/// the yield-forcing [`super::MemoryYieldIO`] backends used for testing.
pub(super) struct MemStore {
    inner: RwLock<MemStoreInner>,
}

impl MemStore {
    pub(super) fn new() -> Self {
        Self {
            inner: RwLock::new(MemStoreInner {
                pages: BTreeMap::new(),
                size: 0,
            }),
        }
    }

    fn get_or_allocate_page(inner: &mut MemStoreInner, page_no: usize) -> &mut MemPage {
        inner
            .pages
            .entry(page_no)
            .or_insert_with(|| Box::new([0; PAGE_SIZE]))
    }

    fn write_at_inner(inner: &mut MemStoreInner, pos: u64, data: &[u8]) -> usize {
        let buf_len = data.len();
        if buf_len == 0 {
            return 0;
        }
        let mut offset = pos as usize;
        let mut remaining = buf_len;
        let mut buf_offset = 0;
        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);
            let page = Self::get_or_allocate_page(inner, page_no);
            page[page_offset..page_offset + bytes_to_write]
                .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
            offset += bytes_to_write;
            buf_offset += bytes_to_write;
            remaining -= bytes_to_write;
        }
        inner.size = inner.size.max(pos + buf_len as u64);
        buf_len
    }

    pub(super) fn size(&self) -> u64 {
        self.inner.read().size
    }

    /// Read `pos..` into `buf`, zero-filling holes. Returns bytes read.
    pub(super) fn read_into(&self, pos: u64, buf: &Buffer) -> i32 {
        let buf_len = buf.len() as u64;
        if buf_len == 0 {
            return 0;
        }
        let inner = self.inner.read();
        let file_size = inner.size;
        if pos >= file_size {
            return 0;
        }
        let read_len = buf_len.min(file_size - pos);
        let dst = buf.as_mut_slice();
        let mut offset = pos as usize;
        let mut remaining = read_len as usize;
        let mut buf_offset = 0;
        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes_to_read = remaining.min(PAGE_SIZE - page_offset);
            if let Some(page) = inner.pages.get(&page_no) {
                dst[buf_offset..buf_offset + bytes_to_read]
                    .copy_from_slice(&page[page_offset..page_offset + bytes_to_read]);
            } else {
                dst[buf_offset..buf_offset + bytes_to_read].fill(0);
            }
            offset += bytes_to_read;
            buf_offset += bytes_to_read;
            remaining -= bytes_to_read;
        }
        read_len as i32
    }

    /// Write `data` at `pos`, allocating pages as needed. Returns bytes written
    /// and grows the recorded file size if the write extends past it.
    pub(super) fn write_at(&self, pos: u64, data: &[u8]) -> usize {
        Self::write_at_inner(&mut self.inner.write(), pos, data)
    }

    /// Vectored write of `buffers` starting at `pos`. Returns total bytes written.
    pub(super) fn writev(&self, pos: u64, buffers: &[Arc<Buffer>]) -> i32 {
        let mut inner = self.inner.write();
        let mut offset = pos;
        let mut total_written = 0usize;
        for buffer in buffers {
            let written = Self::write_at_inner(&mut inner, offset, buffer.as_slice());
            offset += written as u64;
            total_written += written;
        }
        total_written as i32
    }

    pub(super) fn truncate(&self, len: u64) {
        let mut inner = self.inner.write();
        if len < inner.size {
            inner.pages.retain(|&k, _| k * PAGE_SIZE < len as usize);
        }
        inner.size = len;
    }

    pub(super) fn has_hole(&self, pos: usize, len: usize) -> bool {
        let inner = self.inner.read();
        let start_page = pos / PAGE_SIZE;
        let end_page = ((pos + len.max(1)) - 1) / PAGE_SIZE;
        for page_no in start_page..=end_page {
            if inner.pages.contains_key(&page_no) {
                return false;
            }
        }
        true
    }

    pub(super) fn punch_hole(&self, pos: usize, len: usize) {
        turso_assert!(
            pos % PAGE_SIZE == 0 && len % PAGE_SIZE == 0,
            "hole must be page aligned"
        );
        let mut inner = self.inner.write();
        let start_page = pos / PAGE_SIZE;
        let end_page = ((pos + len.max(1)) - 1) / PAGE_SIZE;
        for page_no in start_page..=end_page {
            inner.pages.remove(&page_no);
        }
    }
}

pub struct MemoryFile {
    path: String,
    store: MemStore,
}

crate::assert::assert_sync!(MemoryFile);

impl File for MemoryFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("pread(path={}): pos={}", self.path, pos);
        let n = self.store.read_into(pos, c.as_read().buf());
        c.complete(n);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwrite(path={}): pos={}, size={}",
            self.path,
            pos,
            buffer.len()
        );
        let n = self.store.write_at(pos, buffer.as_slice());
        c.complete(n as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        tracing::debug!("sync(path={})", self.path);
        // no-op
        c.complete(0);
        Ok(c)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("truncate(path={}): len={}", self.path, len);
        self.store.truncate(len);
        c.complete(0);
        Ok(c)
    }

    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwritev(path={}): pos={}, buffers={:?}",
            self.path,
            pos,
            buffers.iter().map(|x| x.len()).collect::<Vec<_>>()
        );
        let n = self.store.writev(pos, &buffers);
        c.complete(n);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        tracing::debug!("size(path={}): {}", self.path, self.store.size());
        Ok(self.store.size())
    }

    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        Ok(self.store.has_hole(pos, len))
    }

    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.store.punch_hole(pos, len);
        Ok(())
    }
}
