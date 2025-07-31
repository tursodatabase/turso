use crate::error::LimboError;
use crate::{io::Completion, Buffer, Result};
use parking_lot::RwLock;
use std::{cell::RefCell, sync::Arc};
use tracing::{instrument, Level};

/// DatabaseStorage is an interface a database file that consists of pages.
///
/// The purpose of this trait is to abstract the upper layers of Limbo from
/// the storage medium. A database can either be a file on disk, like in SQLite,
/// or something like a remote page server service.
pub trait DatabaseStorage: Send + Sync {
    fn read_page(&self, page_idx: usize, c: Completion) -> Result<Completion>;
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<parking_lot::RwLock<Buffer>>,
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
    fn read_page(&self, page_idx: usize, c: Completion) -> Result<Completion> {
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<parking_lot::RwLock<Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        let buffer_size = buffer.read().len();
        assert!(page_idx > 0);
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert_eq!(buffer_size & (buffer_size - 1), 0);
        let pos = (page_idx - 1) * buffer_size;
        self.file.pwrite(pos, buffer, c)
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
}

#[cfg(feature = "fs")]
impl DatabaseFile {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}

pub struct FileMemoryStorage {
    file: Arc<dyn crate::io::File>,
}

unsafe impl Send for FileMemoryStorage {}
unsafe impl Sync for FileMemoryStorage {}

impl DatabaseStorage for FileMemoryStorage {
    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_page(&self, page_idx: usize, c: Completion) -> Result<Completion> {
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<parking_lot::RwLock<Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        let buffer_size = buffer.read().len();
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert_eq!(buffer_size & (buffer_size - 1), 0);
        let pos = (page_idx - 1) * buffer_size;
        self.file.pwrite(pos, buffer, c)
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
}

impl FileMemoryStorage {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}
