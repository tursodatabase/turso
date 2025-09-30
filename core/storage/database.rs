use crate::error::LimboError;
use crate::storage::checksum::ChecksumContext;
use crate::storage::encryption::EncryptionContext;
use crate::{io::Completion, Buffer, CompletionError, Result};
use std::sync::Arc;
use tracing::{instrument, Level};

#[derive(Clone)]
pub enum EncryptionOrChecksum {
    Encryption(EncryptionContext),
    Checksum(ChecksumContext),
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

    pub fn get_reserved_space_bytes(&self) -> u8 {
        match &self.encryption_or_checksum {
            EncryptionOrChecksum::Encryption(ctx) => ctx.required_reserved_bytes(),
            EncryptionOrChecksum::Checksum(ctx) => ctx.required_reserved_bytes(),
            EncryptionOrChecksum::None => Default::default(),
        }
    }

    pub fn set_encryption(&mut self, encryption_ctx: EncryptionContext) {
        self.encryption_or_checksum = EncryptionOrChecksum::Encryption(encryption_ctx);
    }

    pub fn encryption_or_checksum(&self) -> &EncryptionOrChecksum {
        &self.encryption_or_checksum
    }

    pub fn reset_checksum(&mut self) {
        self.encryption_or_checksum = EncryptionOrChecksum::None;
    }
}

impl Default for IOContext {
    fn default() -> Self {
        Self {
            encryption_or_checksum: EncryptionOrChecksum::Checksum(ChecksumContext::default()),
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
impl DatabaseStorage for DatabaseFile {
    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_header(&self, c: Completion) -> Result<Completion> {
        self.file.pread(0, c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_page(&self, page_idx: usize, io_ctx: &IOContext, c: Completion) -> Result<Completion> {
        // casting to i64 to check some weird casting that could've happened before. This should be
        // okay since page numbers should be u32
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

        match &io_ctx.encryption_or_checksum {
            EncryptionOrChecksum::Encryption(ctx) => {
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
                let wrapped_completion = Completion::new_read(read_buffer, decrypt_complete);
                self.file.pread(pos, wrapped_completion)
            }
            EncryptionOrChecksum::Checksum(ctx) => {
                let checksum_ctx = ctx.clone();
                let read_buffer = r.buf_arc();
                let original_c = c.clone();

                let verify_complete =
                    Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                        let Ok((buf, bytes_read)) = res else {
                            return;
                        };
                        if bytes_read <= 0 {
                            tracing::trace!("Read page {page_idx} with {} bytes", bytes_read);
                            original_c.complete(bytes_read);
                            return;
                        }
                        match checksum_ctx.verify_checksum(buf.as_mut_slice(), page_idx) {
                            Ok(_) => {
                                original_c.complete(bytes_read);
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to verify checksum for page_id={page_idx}: {e}"
                                );
                                assert!(
                                    !original_c.has_error(),
                                    "Original completion already has an error"
                                );
                                original_c.error(e);
                            }
                        }
                    });

                let wrapped_completion = Completion::new_read(read_buffer, verify_complete);
                self.file.pread(pos, wrapped_completion)
            }
            EncryptionOrChecksum::None => self.file.pread(pos, c),
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
        let buffer = match &io_ctx.encryption_or_checksum {
            EncryptionOrChecksum::Encryption(ctx) => encrypt_buffer(page_idx, buffer, ctx),
            EncryptionOrChecksum::Checksum(ctx) => checksum_buffer(page_idx, buffer, ctx),
            EncryptionOrChecksum::None => buffer,
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
        let buffers = match &io_ctx.encryption_or_checksum() {
            EncryptionOrChecksum::Encryption(ctx) => buffers
                .into_iter()
                .enumerate()
                .map(|(i, buffer)| encrypt_buffer(first_page_idx + i, buffer, ctx))
                .collect::<Vec<_>>(),
            EncryptionOrChecksum::Checksum(ctx) => buffers
                .into_iter()
                .enumerate()
                .map(|(i, buffer)| checksum_buffer(first_page_idx + i, buffer, ctx))
                .collect::<Vec<_>>(),
            EncryptionOrChecksum::None => buffers,
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

fn checksum_buffer(page_idx: usize, buffer: Arc<Buffer>, ctx: &ChecksumContext) -> Arc<Buffer> {
    ctx.add_checksum_to_page(buffer.as_mut_slice(), page_idx)
        .unwrap();
    buffer
}
