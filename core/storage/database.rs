use crate::error::LimboError;
use crate::storage::encryption::EncryptionContext;
use crate::{io::Completion, Buffer, CompletionError, Result};
use std::sync::Arc;
use tracing::{instrument, Level};

#[derive(Clone)]
pub enum EncryptionOrChecksum {
    Encryption(EncryptionContext),
    Checksum,
    None,
}

#[derive(Clone)]
pub struct IOContext {
    encryption_or_checksum: EncryptionOrChecksum,
}

impl IOContext {
    pub fn encryption_context(&self) -> Option<&EncryptionContext> {
        match &self.encryption_or_checksum {
            EncryptionOrChecksum::Encryption(ctx) => Some(ctx),
            _ => None,
        }
    }

    pub fn set_encryption(&mut self, encryption_ctx: EncryptionContext) {
        self.encryption_or_checksum = EncryptionOrChecksum::Encryption(encryption_ctx);
    }
}

impl Default for IOContext {
    fn default() -> Self {
        Self {
            encryption_or_checksum: EncryptionOrChecksum::None,
        }
    }
}

/// DatabaseStorage is an interface a database file that consists of pages.
///
/// The purpose of this trait is to abstract the upper layers of Limbo from
/// the storage medium. A database can either be a file on disk, like in SQLite,
/// or something like a remote page server service.
pub trait DatabaseStorage: Send + Sync {
    fn read_header(&self, c: Completion) -> Result<Completion>;

    fn read_page(&self, page_idx: usize, io_ctx: &IOContext, c: Completion) -> Result<Completion>;
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<Buffer>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> Result<Completion>;
    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<Buffer>>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> Result<Completion>;
    fn sync(&self, c: Completion) -> Result<Completion>;
    fn size(&self) -> Result<u64>;
    fn truncate(&self, len: usize, c: Completion) -> Result<Completion>;
}

#[cfg(feature = "fs")]
pub struct DatabaseFile {
    file: Arc<dyn crate::io::File>,
}

#[cfg(feature = "fs")]
unsafe impl Send for DatabaseFile {}
#[cfg(feature = "fs")]
unsafe impl Sync for DatabaseFile {}

#[cfg(feature = "fs")]
impl DatabaseStorage for DatabaseFile {
    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_header(&self, c: Completion) -> Result<Completion> {
        self.file.pread(0, c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_page(&self, page_idx: usize, io_ctx: &IOContext, c: Completion) -> Result<Completion> {
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let Some(pos) = (page_idx as u64 - 1).checked_mul(size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };

        if let Some(ctx) = io_ctx.encryption_context() {
            let encryption_ctx = ctx.clone();
            let read_buffer = r.buf_arc();
            let original_c = c.clone();

            let decrypt_complete =
                Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                    let Ok((buf, bytes_read)) = res else {
                        return;
                    };
                    assert!(
                        bytes_read > 0,
                        "Expected to read some data on success for page_id={page_idx}"
                    );
                    match encryption_ctx.decrypt_page(buf.as_slice(), page_idx) {
                        Ok(decrypted_data) => {
                            let original_buf = original_c.as_read().buf();
                            original_buf.as_mut_slice().copy_from_slice(&decrypted_data);
                            original_c.complete(bytes_read);
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to decrypt page data for page_id={page_idx}: {e}"
                            );
                            assert!(
                                !original_c.has_error(),
                                "Original completion already has an error"
                            );
                            original_c.error(CompletionError::DecryptionError { page_idx });
                        }
                    }
                });
            let new_completion = Completion::new_read(read_buffer, decrypt_complete);
            self.file.pread(pos, new_completion)
        } else {
            self.file.pread(pos, c)
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<Buffer>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> Result<Completion> {
        let buffer_size = buffer.len();
        assert!(page_idx > 0);
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert_eq!(buffer_size & (buffer_size - 1), 0);
        let Some(pos) = (page_idx as u64 - 1).checked_mul(buffer_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };
        let buffer = {
            if let Some(ctx) = io_ctx.encryption_context() {
                encrypt_buffer(page_idx, buffer, ctx)
            } else {
                buffer
            }
        };
        self.file.pwrite(pos, buffer, c)
    }

    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<Buffer>>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> Result<Completion> {
        assert!(first_page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert_eq!(page_size & (page_size - 1), 0);

        let Some(pos) = (first_page_idx as u64 - 1).checked_mul(page_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };
        let buffers = {
            if let Some(ctx) = io_ctx.encryption_context() {
                buffers
                    .into_iter()
                    .enumerate()
                    .map(|(i, buffer)| encrypt_buffer(first_page_idx + i, buffer, ctx))
                    .collect::<Vec<_>>()
            } else {
                buffers
            }
        };

        let c = self.file.pwritev(pos, buffers, c)?;
        Ok(c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn sync(&self, c: Completion) -> Result<Completion> {
        self.file.sync(c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn size(&self) -> Result<u64> {
        self.file.size()
    }

    #[instrument(skip_all, level = Level::INFO)]
    fn truncate(&self, len: usize, c: Completion) -> Result<Completion> {
        let c = self.file.truncate(len as u64, c)?;
        Ok(c)
    }
}

#[cfg(feature = "fs")]
impl DatabaseFile {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}

fn encrypt_buffer(page_idx: usize, buffer: Arc<Buffer>, ctx: &EncryptionContext) -> Arc<Buffer> {
    let encrypted_data = ctx.encrypt_page(buffer.as_slice(), page_idx).unwrap();
    Arc::new(Buffer::new(encrypted_data.to_vec()))
}
