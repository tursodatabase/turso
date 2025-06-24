use crate::error::LimboError;
use crate::io::CompletionType;
use crate::{io::Completion, Buffer, Result};
use assertion::{
    assert_always_eq, assert_always_greater_than, assert_always_greater_than_or_equal_to,
    assert_always_less_than_or_equal_to,
};
use std::{cell::RefCell, sync::Arc};

/// DatabaseStorage is an interface a database file that consists of pages.
///
/// The purpose of this trait is to abstract the upper layers of Limbo from
/// the storage medium. A database can either be a file on disk, like in SQLite,
/// or something like a remote page server service.
pub trait DatabaseStorage: Send + Sync {
    fn read_page(&self, page_idx: usize, c: Completion) -> Result<()>;
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<RefCell<Buffer>>,
        c: Completion,
    ) -> Result<()>;
    fn sync(&self, c: Completion) -> Result<()>;
    fn size(&self) -> Result<u64>;
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
    fn read_page(&self, page_idx: usize, c: Completion) -> Result<()> {
        let r = c.as_read();
        let size = r.buf().len();
        assert_always_greater_than!(
            page_idx,
            0,
            "[DatabaseFile - read_page] page_idx is positive"
        );
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<RefCell<Buffer>>,
        c: Completion,
    ) -> Result<()> {
        let buffer_size = buffer.borrow().len();
        assert_always_greater_than!(
            page_idx,
            0,
            "[DatabaseFile - write_page] page_idx is positive"
        );
        assert_always_greater_than_or_equal_to!(
            buffer_size,
            512,
            "[DatabaseFile - write_page] buffer should be bigger or equal to 512"
        );
        assert_always_less_than_or_equal_to!(
            buffer_size,
            65536,
            "[DatabaseFile - write_page] buffer should be less or equal to 65536"
        );
        assert_always_eq!(
            buffer_size & (buffer_size - 1),
            0,
            "[DatabaseFile - write_page] buffer size should pass check"
        );
        let pos = (page_idx - 1) * buffer_size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let _ = self.file.sync(c)?;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        self.file.size()
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
    fn read_page(&self, page_idx: usize, c: Completion) -> Result<()> {
        let r = match c.completion_type {
            CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert_always_greater_than!(
            page_idx,
            0,
            "[FileMemoryStorage - read_page] page_idx is positive"
        );
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<RefCell<Buffer>>,
        c: Completion,
    ) -> Result<()> {
        let buffer_size = buffer.borrow().len();
        assert_always_greater_than_or_equal_to!(
            buffer_size,
            512,
            "[FileMemoryStorage - write_page] buffer should be bigger or equal to 512"
        );
        assert_always_less_than_or_equal_to!(
            buffer_size,
            65536,
            "[FileMemoryStorage - write_page] buffer should be less or equal to 65536"
        );
        assert_always_eq!(
            buffer_size & (buffer_size - 1),
            0,
            "[FileMemoryStorage - write_page] buffer size should pass check"
        );
        let pos = (page_idx - 1) * buffer_size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let _ = self.file.sync(c)?;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        self.file.size()
    }
}

impl FileMemoryStorage {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}
