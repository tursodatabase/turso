#[cfg(target_family = "unix")]
mod mmap_rustix;

#[cfg(target_family = "unix")]
use mmap_rustix::MemoryPages;

#[cfg(not(target_family = "unix"))]
mod btree;

#[cfg(not(target_family = "unix"))]
use btree::MemoryPages;

use super::{Buffer, Completion, File, OpenFlags, IO};
use crate::Result;
use std::{
    cell::{Cell, OnceCell, RefCell, UnsafeCell},
    sync::Arc,
};

use tracing::debug;

#[derive(Default)]
pub struct MemoryIO {}
unsafe impl Send for MemoryIO {}
unsafe impl Sync for MemoryIO {}

// TODO: page size flag
const PAGE_SIZE: usize = 4096;
#[cfg(target_family = "unix")]
pub type MemPage = [u8];

#[cfg(not(target_family = "unix"))]
pub type MemPage = Box<[u8; PAGE_SIZE]>;

const INITIAL_PAGES: OnceCell<usize> = OnceCell::new();
const DEFAULT_INITIAL_PAGES: usize = 16;

impl MemoryIO {
    pub fn new() -> Self {
        Self {}
    }
}

impl MemoryFile {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Result<Arc<Self>> {
        debug!("Using IO backend 'memory'");
        let pages = INITIAL_PAGES;
        let initial_pages = *pages.get_or_init(|| {
            std::env::var("INITIAL_MEM_PAGES").map_or(DEFAULT_INITIAL_PAGES, |str_num| {
                let initial_pages = str_num.parse::<usize>().unwrap_or(DEFAULT_INITIAL_PAGES);
                if initial_pages == 0 {
                    panic!("INITIAL_MEM_PAGES flag cannot be zero")
                }
                initial_pages
            })
        });
        Ok(Arc::new(Self {
            pages: MemoryPages::new(PAGE_SIZE * initial_pages)?.into(),
            size: 0.into(),
        }))
    }

    #[allow(clippy::mut_from_ref)]
    fn get_or_allocate_page(&self, page_no: usize) -> Result<&mut MemPage> {
        #[cfg(target_family = "unix")]
        {
            let start = page_no * PAGE_SIZE;
            let end = start + PAGE_SIZE;
            let len_pages = unsafe {
                let page = &mut *self.pages.get();
                page.len()
            };

            if end > len_pages {
                let cap_pages = len_pages / PAGE_SIZE;
                self.resize(cap_pages * 2)?;
            }

            let page = unsafe {
                let page = &mut *self.pages.get();
                &mut page[start..end]
            };
            Ok(page)
        }
        #[cfg(not(target_family = "unix"))]
        {
            unsafe {
                let pages = &mut *self.pages.get();
                Ok(pages
                    .entry(page_no)
                    .or_insert_with(|| Box::new([0; PAGE_SIZE])))
            }
        }
    }

    fn get_page(&self, page_no: usize) -> Option<&MemPage> {
        #[cfg(target_family = "unix")]
        {
            let start = page_no * PAGE_SIZE;
            let end = start + PAGE_SIZE;
            let pages = unsafe { &mut *self.pages.get() };

            // Make sure that we are accessing into a valid memory page
            assert!(end <= pages.len());

            pages.get(start..end)
        }
        #[cfg(not(target_family = "unix"))]
        unsafe {
            (*self.pages.get()).get(&page_no)
        }
    }

    #[cfg(target_family = "unix")]
    fn resize(&self, capacity: usize) -> Result<()> {
        let pages = unsafe { &mut *self.pages.get() };
        let new_cap = PAGE_SIZE * capacity;
        #[cfg(target_os = "linux")]
        {
            pages.remap(new_cap)?;
        }

        #[cfg(not(target_os = "linux"))]
        {
            let mut new_pages = MemoryPages::new(new_cap)?;
            new_pages[..pages.len()].clone_from_slice(pages);

            *pages = new_pages;
        }

        Ok(())
    }
}

impl IO for MemoryIO {
    fn open_file(&self, _path: &str, _flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        MemoryFile::new().map(|f| f as Arc<dyn File>)
    }

    fn run_once(&self) -> Result<()> {
        // nop
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_current_time(&self) -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

pub struct MemoryFile {
    pages: UnsafeCell<MemoryPages>,
    size: Cell<usize>,
}

unsafe impl Send for MemoryFile {}
unsafe impl Sync for MemoryFile {}

impl File for MemoryFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = c.as_read();
        let buf_len = r.buf().len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(());
        }

        let file_size = self.size.get();
        if pos >= file_size {
            c.complete(0);
            return Ok(());
        }

        let read_len = buf_len.min(file_size - pos);
        {
            let mut read_buf = r.buf_mut();
            let mut offset = pos;
            let mut remaining = read_len;
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
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Arc<RefCell<Buffer>>, c: Completion) -> Result<()> {
        let buf = buffer.borrow();
        let buf_len = buf.len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(());
        }

        let mut offset = pos;
        let mut remaining = buf_len;
        let mut buf_offset = 0;
        let data = &buf.as_slice();

        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);

            {
                let page = self.get_or_allocate_page(page_no)?;
                page[page_offset..page_offset + bytes_to_write]
                    .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
            }

            offset += bytes_to_write;
            buf_offset += bytes_to_write;
            remaining -= bytes_to_write;
        }

        self.size
            .set(core::cmp::max(pos + buf_len, self.size.get()));

        c.complete(buf_len as i32);
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        // no-op
        c.complete(0);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.size.get() as u64)
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        // no-op
        // TODO ideally we could have some flags
        // in Memory File, that when this is dropped
        // if the persist flag is true we could flush the mmap to disk
        // We would also have to store the file name here in this case
    }
}
