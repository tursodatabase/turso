use std::sync::Arc;

use turso_core::{
    turso_assert, Buffer, Completion, CompletionError, DatabaseStorage, File, LimboError,
};

use crate::{
    database_sync_operations::{pull_pages_v1, ProtocolIoStats, PAGE_SIZE},
    errors,
    protocol_io::ProtocolIO,
    types::Coro,
};

pub struct LazyDatabaseStorage<P: ProtocolIO> {
    clean_file: Arc<dyn File>,
    dirty_file: Option<Arc<dyn File>>,
    protocol: ProtocolIoStats<P>,
    server_revision: String,
}

impl<P: ProtocolIO> LazyDatabaseStorage<P> {
    pub fn new(
        clean_file: Arc<dyn File>,
        dirty_file: Option<Arc<dyn File>>,
        protocol: ProtocolIoStats<P>,
        server_revision: String,
    ) -> Self {
        Self {
            clean_file,
            dirty_file,
            protocol,
            server_revision,
        }
    }
}

impl<P: ProtocolIO> DatabaseStorage for LazyDatabaseStorage<P> {
    fn read_header(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        assert!(
            !self.clean_file.has_hole(0, PAGE_SIZE)?,
            "first page must be filled"
        );
        self.clean_file.pread(0, c)
    }

    fn read_page(
        &self,
        page_idx: usize,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            io_ctx.encryption_context().is_none(),
            "encryption or checksum are not supported with partial sync"
        );
        assert!(page_idx as i64 >= 0, "page should be positive");
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let Some(pos) = (page_idx as u64 - 1).checked_mul(size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };

        if !self.clean_file.has_hole(pos as usize, size)? {
            let Some(dirty_file) = &self.dirty_file else {
                // no dirty file was set - this means that FS is atomic (e.g. MemoryIO)
                return self.clean_file.pread(pos, c);
            };
            if dirty_file.has_hole(pos as usize, size)? {
                // dirty file has no hole - this means that we cleanly removed the hole when we wrote to the clean file
                return self.clean_file.pread(pos, c);
            }
            let check_buffer = Arc::new(Buffer::new_temporary(size));
            let check_c =
                dirty_file.pread(pos, Completion::new_read(check_buffer.clone(), |_| {}))?;
            turso_assert!(
                check_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );

            let clean_buffer = r.buf_arc();
            let clean_c = self
                .clean_file
                .pread(pos, Completion::new_read(clean_buffer.clone(), |_| {}))?;
            turso_assert!(
                clean_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );

            if check_buffer.as_slice().eq(clean_buffer.as_slice()) {
                // dirty buffer matches clean buffer - this means that clean data is valid
                return self.clean_file.pread(pos, c);
            }
        }
        tracing::info!(
            "PartialDatabaseStorage::read_page(page_idx={}): read page from the remote server",
            page_idx
        );
        let mut generator = genawaiter::sync::Gen::new({
            let protocol = self.protocol.clone();
            let server_revision = self.server_revision.clone();
            let clean_file = self.clean_file.clone();
            let dirty_file = self.dirty_file.clone();
            let c = c.clone();
            |coro| async move {
                let coro = Coro::new((), coro);
                let pages = [(page_idx - 1) as u32];
                let result = pull_pages_v1(&coro, &protocol, &server_revision, &pages).await;
                match result {
                    Ok(page) => {
                        let read = c.as_read();
                        let buf = read.buf_arc();
                        buf.as_mut_slice().copy_from_slice(&page);

                        if let Some(dirty_file) = &dirty_file {
                            let dirty_c = dirty_file.pwrite(
                                pos,
                                buf.clone(),
                                Completion::new_write(|_| {}),
                            )?;
                            turso_assert!(
                                dirty_c.finished(),
                                "LazyDatabaseStorage works only with sync IO"
                            );
                        }

                        let clean_c =
                            clean_file.pwrite(pos, buf.clone(), Completion::new_write(|_| {}))?;
                        turso_assert!(
                            clean_c.finished(),
                            "LazyDatabaseStorage works only with sync IO"
                        );

                        if let Some(dirty_file) = &dirty_file {
                            dirty_file.punch_hole(pos as usize, buf.len())?;
                        }

                        c.complete(buf.len() as i32);
                        Ok::<(), errors::Error>(())
                    }
                    Err(err) => {
                        tracing::error!("failed to fetch path from remote server: {err}");
                        c.error(CompletionError::IOError(std::io::ErrorKind::Other));
                        Err(err)
                    }
                }
            }
        });
        self.protocol
            .add_work(Box::new(move || match generator.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(_) => false,
                genawaiter::GeneratorState::Complete(_) => true,
            }));
        Ok(c)
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: std::sync::Arc<turso_core::Buffer>,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            io_ctx.encryption_context().is_none(),
            "encryption or checksum are not supported with partial sync"
        );

        let buffer_size = buffer.len();
        assert!(page_idx > 0);
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert_eq!(buffer_size & (buffer_size - 1), 0);
        let Some(pos) = (page_idx as u64 - 1).checked_mul(buffer_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };

        // we write to the database only during checkpoint - so we need to punch hole in the dirty file in order to mark this region as valid
        if let Some(dirty_file) = &self.dirty_file {
            dirty_file.punch_hole(pos as usize, buffer_size)?;
        }
        self.clean_file.pwrite(pos, buffer, c)
    }

    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<std::sync::Arc<turso_core::Buffer>>,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            io_ctx.encryption_context().is_none(),
            "encryption or checksum are not supported with partial sync"
        );

        assert!(first_page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert_eq!(page_size & (page_size - 1), 0);

        let Some(pos) = (first_page_idx as u64 - 1).checked_mul(page_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };
        // we write to the database only during checkpoint - so we need to punch hole in the dirty file in order to mark this region as valid
        if let Some(dirty_file) = &self.dirty_file {
            let buffers_size = buffers.iter().map(|b| b.len()).sum();
            dirty_file.punch_hole(pos as usize, buffers_size)?;
        }
        let c = self.clean_file.pwritev(pos, buffers, c)?;
        Ok(c)
    }

    fn sync(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        if let Some(dirty_file) = &self.dirty_file {
            let dirty_c = dirty_file.sync(Completion::new_sync(|_| {}))?;
            turso_assert!(
                dirty_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );
        }

        self.clean_file.sync(c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.clean_file.size()
    }

    fn truncate(
        &self,
        len: usize,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        if let Some(dirty_file) = &self.dirty_file {
            let dirty_c = dirty_file.truncate(len as u64, Completion::new_trunc(|_| {}))?;
            turso_assert!(
                dirty_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );
        }

        self.clean_file.truncate(len as u64, c)
    }
}
