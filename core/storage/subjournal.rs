use std::sync::Arc;

use crate::{
    storage::sqlite3_ondisk::finish_read_page, Buffer, Completion, CompletionError, PageRef, Result,
};

#[derive(Clone)]
pub struct Subjournal {
    file: Arc<dyn crate::io::File>,
}

impl Subjournal {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }

    pub fn size(&self) -> Result<u64> {
        self.file.size()
    }

    pub fn write_page(
        &self,
        offset: u64,
        page_size: usize,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        assert!(
            buffer.len() == page_size + 4,
            "buffer length should be page_size + 4 bytes for page id"
        );
        self.file.pwrite(offset, buffer, c)
    }

    pub fn read_page_number(&self, offset: u64, page_id_buffer: Arc<Buffer>) -> Result<Completion> {
        assert!(
            page_id_buffer.len() == 4,
            "page_id_buffer length should be 4 bytes"
        );
        let c = Completion::new_read(
            page_id_buffer,
            move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((_buffer, _bytes_read)) = res else {
                    return;
                };
            },
        );
        let c = self.file.pread(offset, c)?;
        Ok(c)
    }

    pub fn read_page(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        page: PageRef,
        page_size: usize,
    ) -> Result<Completion> {
        assert!(
            buffer.len() == page_size,
            "buffer length should be page_size"
        );
        let c = Completion::new_read(
            buffer,
            move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((buffer, bytes_read)) = res else {
                    return;
                };
                assert!(
                    bytes_read == page_size as i32,
                    "bytes_read should be page_size"
                );
                finish_read_page(page.get().id, buffer, page.clone());
            },
        );
        let c = self.file.pread(offset, c)?;
        Ok(c)
    }

    pub fn truncate(&self, offset: u64) -> Result<Completion> {
        let c = Completion::new_trunc(move |res: Result<i32, CompletionError>| {
            let Ok(_) = res else {
                return;
            };
        });
        self.file.truncate(offset, c)
    }
}
