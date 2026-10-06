use crate::io::{Buffer, Completion, File, IO};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::encode_page_for_database_file;
use crate::sync::Arc;
use crate::{turso_assert, turso_assert_eq, IOContext, Result};

pub(crate) struct AtomicPageWriter {
    file: Arc<dyn File>,
    storage_has_torn_write_protection: bool,
    staging: Arc<BufferPool>,
    page_size: usize,
}

impl AtomicPageWriter {
    pub(crate) const MAX_PAGES_IN_FLIGHT: usize = 1024;

    pub(crate) fn open(
        io: &Arc<dyn IO>,
        path: &str,
        page_size: usize,
        assume_torn_write_protection: bool,
    ) -> Result<Option<Self>> {
        let storage_has_torn_write_protection = io
            .atomic_write_units(path)
            .is_some_and(|units| units.covers(page_size));
        tracing::info!(
            path,
            page_size,
            storage_has_torn_write_protection,
            assume_torn_write_protection,
            "torn-write protection for MVCC checkpoints that skip the WAL"
        );
        if !storage_has_torn_write_protection && !assume_torn_write_protection {
            return Ok(None);
        }
        let staging = BufferPool::begin_init(io, Self::MAX_PAGES_IN_FLIGHT * page_size);
        staging.finalize_with_page_size(page_size)?;
        Ok(Some(Self {
            file: io.open_file_for_direct_io(path)?,
            storage_has_torn_write_protection,
            staging,
            page_size,
        }))
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn write_page(
        &self,
        page_idx: usize,
        page: Arc<Buffer>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> Result<Completion> {
        turso_assert_eq!(page.len(), self.page_size, "page size changed after open");
        let encoded = encode_page_for_database_file(page_idx, page, io_ctx)?;
        let staged = Arc::new(self.staging.get_page());
        turso_assert!(
            staged.as_ptr() as usize % self.page_size == 0,
            "staging page must be aligned for direct I/O"
        );
        staged.as_mut_slice().copy_from_slice(encoded.as_slice());
        let pos = (page_idx as u64 - 1) * self.page_size as u64;
        if self.storage_has_torn_write_protection {
            self.file.pwrite_atomic(pos, staged, c)
        } else {
            self.file.pwrite(pos, staged, c)
        }
    }
}
