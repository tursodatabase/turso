use std::sync::Arc;

use turso_core::{Completion, CompletionError, DatabaseStorage};

use crate::{
    database_sync_operations::{pull_pages_v1, ProtocolIoStats, PAGE_SIZE},
    errors,
    protocol_io::ProtocolIO,
    types::Coro,
};

pub struct PartialDatabaseStorage<P: ProtocolIO> {
    base: Arc<dyn DatabaseStorage>,
    protocol: ProtocolIoStats<P>,
    server_revision: String,
}

impl<P: ProtocolIO> PartialDatabaseStorage<P> {
    pub fn new(
        base: Arc<dyn DatabaseStorage>,
        protocol: ProtocolIoStats<P>,
        server_revision: String,
    ) -> Self {
        Self {
            base,
            protocol,
            server_revision,
        }
    }
}

impl<P: ProtocolIO> DatabaseStorage for PartialDatabaseStorage<P> {
    fn read_header(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        assert!(
            !self.base.has_hole(0, PAGE_SIZE)?,
            "first page must be filled"
        );
        self.base.read_header(c)
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
        if !self.base.has_hole((page_idx - 1) * PAGE_SIZE, PAGE_SIZE)? {
            return self.base.read_page(page_idx, io_ctx, c);
        }
        tracing::info!(
            "PartialDatabaseStorage::read_page(page_idx={}): read page from the remote server",
            page_idx
        );
        let mut generator = genawaiter::sync::Gen::new({
            let protocol = self.protocol.clone();
            let server_revision = self.server_revision.clone();
            let base = self.base.clone();
            let io_ctx = io_ctx.clone();
            let c = c.clone();
            |coro| async move {
                let coro = Coro::new((), coro);
                let result =
                    pull_pages_v1(&coro, &protocol, &server_revision, &[(page_idx - 1) as u32])
                        .await;
                match result {
                    Ok(page) => {
                        let read = c.as_read();
                        let buf = read.buf_arc();
                        buf.as_mut_slice().copy_from_slice(&page);
                        let write = Completion::new_write(move |result| {
                            let Ok(_) = result else {
                                panic!("unexpected write error: {result:?}");
                            };
                            c.complete(page.len() as i32);
                        });
                        let _ = base.write_page(page_idx, buf.clone(), &io_ctx, write)?;
                        Ok::<(), errors::Error>(())
                    }
                    Err(err) => {
                        tracing::error!("faile to fetch path from remote server: {err}");
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
        self.base.write_page(page_idx, buffer, io_ctx, c)
    }

    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<std::sync::Arc<turso_core::Buffer>>,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        self.base
            .write_pages(first_page_idx, page_size, buffers, io_ctx, c)
    }

    fn sync(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        self.base.sync(c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.base.size()
    }

    fn truncate(
        &self,
        len: usize,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        self.base.truncate(len, c)
    }

    fn has_hole(&self, _pos: usize, _len: usize) -> turso_core::Result<bool> {
        Ok(false)
    }
}
