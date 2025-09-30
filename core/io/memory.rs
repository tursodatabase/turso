use super::{Buffer, Clock, Completion, File, OpenFlags, IO};
use crate::Result;

use crate::io::clock::Instant;
use std::{
    cell::{Cell, UnsafeCell},
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use tracing::debug;

pub struct MemoryIO {
    files: Arc<Mutex<HashMap<String, Arc<MemoryFile>>>>,
}

// TODO: page size flag
const PAGE_SIZE: usize = 4096;
type MemPage = Box<[u8; PAGE_SIZE]>;

impl MemoryIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        debug!("Using IO backend 'memory'");
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MemoryIO {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MemoryIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

impl IO for MemoryIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        let mut files = self.files.lock().unwrap();
        if !files.contains_key(path) && !flags.contains(OpenFlags::Create) {
            return Err(
                crate::error::CompletionError::IOError(std::io::ErrorKind::NotFound).into(),
            );
        }
        if !files.contains_key(path) {
            files.insert(
                path.to_string(),
                Arc::new(MemoryFile {
                    path: path.to_string(),
                    pages: BTreeMap::new().into(),
                    size: 0.into(),
                }),
            );
        }
        Ok(files.get(path).unwrap().clone())
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        files.remove(path);
        Ok(())
    }
}

pub struct MemoryFile {
    path: String,
    pages: UnsafeCell<BTreeMap<usize, MemPage>>,
    size: Cell<u64>,
}

unsafe impl Sync for MemoryFile {}

impl File for MemoryFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("pread(path={}): pos={}", self.path, pos);
        let r = c.as_read();
        let buf_len = r.buf().len() as u64;
        if buf_len == 0 {
            c.complete(0);
            return Ok(c);
        }

        let file_size = self.size.get();
        if pos >= file_size {
            c.complete(0);
            return Ok(c);
        }

        let read_len = buf_len.min(file_size - pos);
        {
            let read_buf = r.buf();
            let mut offset = pos as usize;
            let mut remaining = read_len as usize;
            let mut buf_offset = 0;

            while remaining > 0 {
                let page_no = offset / PAGE_SIZE;
                let page_offset = offset % PAGE_SIZE;
                let bytes_to_read = remaining.min(PAGE_SIZE - page_offset);
                if let Some(page) = self.get_page(page_no) {
                    read_buf.as_mut_slice()[buf_offset..buf_offset + bytes_to_read]
                        .copy_from_slice(&page[page_offset..page_offset + bytes_to_read]);
                } else {
                    read_buf.as_mut_slice()[buf_offset..buf_offset + bytes_to_read].fill(0);
                }

                offset += bytes_to_read;
                buf_offset += bytes_to_read;
                remaining -= bytes_to_read;
            }
        }
        c.complete(read_len as i32);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwrite(path={}): pos={}, size={}",
            self.path,
            pos,
            buffer.len()
        );
        let buf_len = buffer.len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(c);
        }

        let mut offset = pos as usize;
        let mut remaining = buf_len;
        let mut buf_offset = 0;
        let data = &buffer.as_slice();

        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);

            {
                let page = self.get_or_allocate_page(page_no as u64);
                page[page_offset..page_offset + bytes_to_write]
                    .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
            }

            offset += bytes_to_write;
            buf_offset += bytes_to_write;
            remaining -= bytes_to_write;
        }

        self.size
            .set(core::cmp::max(pos + buf_len as u64, self.size.get()));

        c.complete(buf_len as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion) -> Result<Completion> {
        tracing::debug!("sync(path={})", self.path);
        // no-op
        c.complete(0);
        Ok(c)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("truncate(path={}): len={}", self.path, len);
        if len < self.size.get() {
            // Truncate pages
            unsafe {
                let pages = &mut *self.pages.get();
                pages.retain(|&k, _| k * PAGE_SIZE < len as usize);
            }
        }
        self.size.set(len);
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
        let mut offset = pos as usize;
        let mut total_written = 0;

        for buffer in buffers {
            let buf_len = buffer.len();
            if buf_len == 0 {
                continue;
            }

            let mut remaining = buf_len;
            let mut buf_offset = 0;
            let data = &buffer.as_slice();

            while remaining > 0 {
                let page_no = offset / PAGE_SIZE;
                let page_offset = offset % PAGE_SIZE;
                let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);

                {
                    let page = self.get_or_allocate_page(page_no as u64);
                    page[page_offset..page_offset + bytes_to_write]
                        .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
                }

                offset += bytes_to_write;
                buf_offset += bytes_to_write;
                remaining -= bytes_to_write;
            }
            total_written += buf_len;
        }
        c.complete(total_written as i32);
        self.size
            .set(core::cmp::max(pos + total_written as u64, self.size.get()));
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        tracing::debug!("size(path={}): {}", self.path, self.size.get());
        Ok(self.size.get())
    }
}

impl MemoryFile {
    #[allow(clippy::mut_from_ref)]
    fn get_or_allocate_page(&self, page_no: u64) -> &mut MemPage {
        unsafe {
            let pages = &mut *self.pages.get();
            pages
                .entry(page_no as usize)
                .or_insert_with(|| Box::new([0; PAGE_SIZE]))
        }
    }

    fn get_page(&self, page_no: usize) -> Option<&MemPage> {
        unsafe { (*self.pages.get()).get(&page_no) }
    }
}
