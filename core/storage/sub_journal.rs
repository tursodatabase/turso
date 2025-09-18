//! # Subjournal
//! Responsible to manage savepoints
use std::sync::Arc;

use crate::{
    io,
    storage::{pager::allocate_new_page, sqlite3_ondisk::PageContent},
    turso_assert, Buffer, BufferPool, Completion, CompletionError, File, Page, PageRef, Pager,
    Result, IO,
};

pub struct SubJournal {
    file: Arc<dyn File>,
    buffer_pool: Arc<BufferPool>,
}

pub const PAGE_ID_SIZE: usize = size_of::<usize>();

impl SubJournal {
    pub fn new(io: &Arc<dyn IO>, pager: &Pager) -> Result<Self> {
        // We should be able to write on disk
        // but let's keep things simple for now
        let mem_io = io.get_memory_io();
        let page_size = pager.page_size.get().unwrap().0.get();

        let file = mem_io.open_file("", io::OpenFlags::Create, true)?;
        let buffer_pool = Arc::new(BufferPool::new(
            BufferPool::DEFAULT_ARENA_SIZE,
            // Each sub-journal page is the formed by its id + content
            page_size as usize + PAGE_ID_SIZE,
        ));
        Ok(Self { file, buffer_pool })
    }

    pub fn write_page(&self, pager: &Pager, page: &Page) -> Result<()> {
        // todo: use page from our own buffer pool
        let page_size = pager.page_size.get().unwrap().0.get();
        let offset = (pager.n_records.get() * (PAGE_ID_SIZE + page_size as usize) as u32) as u64;
        let c = Completion::new_write(move |_| {});
        let buffer = Arc::new(Buffer::new_temporary(PAGE_ID_SIZE + page_size as usize));
        let page_frame = buffer.as_mut_slice();
        page_frame[0..PAGE_ID_SIZE].copy_from_slice(&page.get().id.to_be_bytes());
        // todo: maybe save just the diff?
        let page_content = page.get().contents.as_ref().unwrap();
        page_frame[PAGE_ID_SIZE..].copy_from_slice(&page_content.as_ptr());
        let _ = self.file.pwrite(offset, buffer, c)?;

        pager.n_records.update(|x| x + 1);
        let mut pager_savepoints = pager.savepoints.borrow_mut();
        assert!(!pager_savepoints.is_empty());
        pager_savepoints.add_page(page.get().id);

        Ok(())
    }

    pub fn read_page(&self, offset: usize) -> Result<(PageRef, Completion)> {
        // We only know page_id after reading the file, so temporaly uses 1 but we gonna change it later
        let page = allocate_new_page(1, &self.buffer_pool, PAGE_ID_SIZE);
        let buf = self.buffer_pool.get_page();
        #[allow(clippy::arc_with_non_send_sync)]
        let buf = Arc::new(buf);
        let completion_page = page.clone();
        let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                completion_page.clear_locked();
                return;
            };
            let buf_len = buf.len();
            turso_assert!(
                bytes_read == buf_len as i32,
                "read({bytes_read}) != expected({buf_len})"
            );
            // Probably is inneficient
            let buf_slice = &buf.as_slice()[8..];
            let buf_without_first_8 = Arc::new(Buffer::new(buf_slice.to_vec()));
            let inner = PageContent::new(PAGE_ID_SIZE, buf_without_first_8);
            let id = usize::from_be_bytes(buf.as_mut_slice()[..PAGE_ID_SIZE].try_into().unwrap());
            {
                completion_page.get().contents.replace(inner);
                completion_page.get().id = id;
                completion_page.clear_locked();
                completion_page.set_loaded();
                // we set the wal tag only when reading page from log, or in allocate_page,
                // we clear it here for safety in case page is being re-loaded.
                completion_page.clear_wal_tag();
            }
        });
        let c = Completion::new_read(buf, complete);
        // we only operate over in-memory file so far
        let result_completion = self.file.pread(offset as u64, c)?;

        Ok((page, result_completion))
    }
}
