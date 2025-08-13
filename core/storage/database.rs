use crate::error::LimboError;
use crate::storage::encryption::{decrypt_page, encrypt_page, EncryptionKey};
use crate::{io::Completion, Buffer, Result};
use std::sync::Arc;
use tracing::{instrument, Level};

/// DatabaseStorage is an interface a database file that consists of pages.
///
/// The purpose of this trait is to abstract the upper layers of Limbo from
/// the storage medium. A database can either be a file on disk, like in SQLite,
/// or something like a remote page server service.
pub trait DatabaseStorage: Send + Sync {
    fn read_page(
        &self,
        page_idx: usize,
        encryption_key: Option<&EncryptionKey>,
        c: Completion,
    ) -> Result<Completion>;
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<Buffer>,
        encryption_key: Option<&EncryptionKey>,
        c: Completion,
    ) -> Result<Completion>;
    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<Buffer>>,
        encryption_key: Option<&EncryptionKey>,
        c: Completion,
    ) -> Result<Completion>;
    fn sync(&self, c: Completion) -> Result<Completion>;
    fn size(&self) -> Result<u64>;
    fn truncate(&self, len: usize, c: Completion) -> Result<Completion>;
    fn copy_to(&self, io: &dyn crate::IO, path: &str) -> Result<()>;
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
    fn read_page(
        &self,
        page_idx: usize,
        encryption_key: Option<&EncryptionKey>,
        c: Completion,
    ) -> Result<Completion> {
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;

        if let Some(key) = encryption_key {
            let key_clone = key.clone();
            let read_buffer = r.buf_arc();
            let original_c = c.clone();

            let decrypt_complete = Box::new(move |buf: Arc<Buffer>, bytes_read: i32| {
                if bytes_read > 0 {
                    match decrypt_page(buf.as_slice(), page_idx, &key_clone) {
                        Ok(decrypted_data) => {
                            let original_buf = original_c.as_read().buf();
                            original_buf.as_mut_slice().copy_from_slice(&decrypted_data);
                            original_c.complete(bytes_read);
                        }
                        Err(_) => {
                            original_c.complete(-1);
                        }
                    }
                } else {
                    original_c.complete(bytes_read);
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
        encryption_key: Option<&EncryptionKey>,
        c: Completion,
    ) -> Result<Completion> {
        let buffer_size = buffer.len();
        assert!(page_idx > 0);
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert_eq!(buffer_size & (buffer_size - 1), 0);
        let pos = (page_idx - 1) * buffer_size;
        let buffer = {
            if let Some(key) = encryption_key {
                encrypt_buffer(page_idx, buffer, key)
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
        encryption_key: Option<&EncryptionKey>,
        c: Completion,
    ) -> Result<Completion> {
        assert!(first_page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert_eq!(page_size & (page_size - 1), 0);

        let pos = (first_page_idx - 1) * page_size;
        let buffers = {
            if let Some(key) = encryption_key {
                buffers
                    .into_iter()
                    .enumerate()
                    .map(|(i, buffer)| encrypt_buffer(first_page_idx + i, buffer, key))
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
        let c = self.file.truncate(len, c)?;
        Ok(c)
    }

    #[instrument(skip_all, level = Level::INFO)]
    fn copy_to(&self, io: &dyn crate::IO, path: &str) -> Result<()> {
        self.file.copy_to(io, path)
    }
}

#[cfg(feature = "fs")]
impl DatabaseFile {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}

fn encrypt_buffer(page_idx: usize, buffer: Arc<Buffer>, key: &EncryptionKey) -> Arc<Buffer> {
    let encrypted_data = encrypt_page(buffer.as_slice(), page_idx, key).unwrap();
    Arc::new(Buffer::new(encrypted_data.to_vec()))
}
