use crate::io::FileSyncType;
use crate::storage::checksum::ChecksumContext;
use crate::storage::encryption::EncryptionContext;
use crate::sync::Arc;
use crate::{io::Completion, Buffer, CompletionError, LimboError, Result};
use crate::{
    turso_assert, turso_assert_eq, turso_assert_greater_than, turso_assert_greater_than_or_equal,
    turso_assert_less_than_or_equal,
};
use tracing::{instrument, Level};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCodecHeaderInfo {
    pub page_size: usize,
    pub reserved_space: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCodecLocation {
    Database,
    Wal,
}

/// Transforms page images between their on-disk and in-memory representation.
///
/// Implementations must preserve the input page length. SQLite page size is a
/// file-format invariant; codecs that need per-page metadata must store it in
/// the page's reserved bytes rather than changing the page image length.
pub trait PageCodec: std::fmt::Debug + Send + Sync {
    fn probe_header(&self, _raw_page1_prefix: &[u8]) -> Result<Option<PageCodecHeaderInfo>> {
        Ok(None)
    }

    fn required_reserved_bytes(&self) -> u8;
    fn encode_page(
        &self,
        page: &[u8],
        page_idx: usize,
        location: PageCodecLocation,
    ) -> Result<Vec<u8>>;
    fn decode_page(
        &self,
        page: &[u8],
        page_idx: usize,
        location: PageCodecLocation,
    ) -> Result<Vec<u8>>;
}

#[inline]
pub(crate) fn page_codec_size_mismatch(
    page_idx: usize,
    expected: usize,
    actual: usize,
) -> CompletionError {
    CompletionError::PageCodecSizeMismatch {
        page_idx,
        expected,
        actual,
    }
}

#[inline]
pub(crate) fn validate_page_codec_output_len(
    page_idx: usize,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual != expected {
        Err(page_codec_size_mismatch(page_idx, expected, actual).into())
    } else {
        Ok(())
    }
}

impl PageCodec for EncryptionContext {
    fn required_reserved_bytes(&self) -> u8 {
        self.required_reserved_bytes()
    }

    fn encode_page(
        &self,
        page: &[u8],
        page_idx: usize,
        _location: PageCodecLocation,
    ) -> Result<Vec<u8>> {
        self.encrypt_page(page, page_idx)
    }

    fn decode_page(
        &self,
        page: &[u8],
        page_idx: usize,
        _location: PageCodecLocation,
    ) -> Result<Vec<u8>> {
        self.decrypt_page(page, page_idx)
    }
}

#[derive(Debug, Clone)]
pub enum EncryptionOrChecksum {
    Encryption(EncryptionContext),
    PageCodec(Arc<dyn PageCodec>),
    Checksum(ChecksumContext),
    None,
}

#[derive(Debug, Clone)]
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
            EncryptionOrChecksum::PageCodec(ctx) => ctx.required_reserved_bytes(),
            EncryptionOrChecksum::Checksum(ctx) => ctx.required_reserved_bytes(),
            EncryptionOrChecksum::None => Default::default(),
        }
    }

    pub fn set_encryption(&mut self, encryption_ctx: EncryptionContext) {
        self.encryption_or_checksum = EncryptionOrChecksum::Encryption(encryption_ctx);
    }

    pub fn set_page_codec(&mut self, codec: Arc<dyn PageCodec>) {
        self.encryption_or_checksum = EncryptionOrChecksum::PageCodec(codec);
    }

    pub fn has_page_codec(&self) -> bool {
        matches!(
            self.encryption_or_checksum,
            EncryptionOrChecksum::Encryption(_) | EncryptionOrChecksum::PageCodec(_)
        )
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
        #[cfg(feature = "checksum")]
        let encryption_or_checksum = EncryptionOrChecksum::Checksum(ChecksumContext::default());
        #[cfg(not(feature = "checksum"))]
        let encryption_or_checksum = EncryptionOrChecksum::None;
        Self {
            encryption_or_checksum,
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
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion>;
    fn size(&self) -> Result<u64>;
    fn truncate(&self, len: usize, c: Completion) -> Result<Completion>;
}

#[derive(Clone)]
pub struct DatabaseFile {
    file: Arc<dyn crate::io::File>,
}

impl DatabaseStorage for DatabaseFile {
    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_header(&self, c: Completion) -> Result<Completion> {
        self.file.pread(0, c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_page(&self, page_idx: usize, io_ctx: &IOContext, c: Completion) -> Result<Completion> {
        // casting to i64 to check some weird casting that could've happened before. This should be
        // okay since page numbers should be u32
        turso_assert_greater_than_or_equal!(page_idx as i64, 0);
        let r = c.as_read();
        let size = r.buf().len();
        turso_assert_greater_than!(page_idx, 0);
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
                        let (buf, bytes_read) = match res {
                            Ok((buf, bytes_read)) => (buf, bytes_read),
                            Err(err) => {
                                tracing::error!(err = ?err);
                                original_c.error(err);
                                return original_c.get_error();
                            }
                        };
                        turso_assert_greater_than!(
                            bytes_read, 0,
                            "database: expected to read data on success for encrypted page",
                            { "page_idx": page_idx }
                        );
                        match encryption_ctx.decode_page(
                            buf.as_slice(),
                            page_idx,
                            PageCodecLocation::Database,
                        ) {
                            Ok(decrypted_data) => {
                                let original_buf = original_c.as_read().buf();
                                original_buf.as_mut_slice().copy_from_slice(&decrypted_data);
                                original_c.complete(bytes_read);
                                original_c.get_error()
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to decrypt page data for page_id={page_idx}: {e}"
                                );
                                turso_assert!(
                                    !original_c.failed(),
                                    "Original completion already has an error"
                                );
                                original_c.error(CompletionError::DecryptionError { page_idx });
                                original_c.get_error()
                            }
                        }
                    });
                let wrapped_completion = Completion::new_read(read_buffer, decrypt_complete);
                self.file.pread(pos, wrapped_completion)
            }
            EncryptionOrChecksum::PageCodec(ctx) => {
                let page_codec = ctx.clone();
                let read_buffer = r.buf_arc();
                let original_c = c.clone();
                let decode_complete = Box::new(
                    move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                        let (buf, bytes_read) = match res {
                            Ok((buf, bytes_read)) => (buf, bytes_read),
                            Err(err) => {
                                tracing::error!(err = ?err);
                                original_c.error(err);
                                return original_c.get_error();
                            }
                        };
                        turso_assert_greater_than!(
                            bytes_read,
                            0,
                            "database: expected to read data on success for encoded page",
                            { "page_idx": page_idx }
                        );
                        match page_codec.decode_page(
                            buf.as_slice(),
                            page_idx,
                            PageCodecLocation::Database,
                        ) {
                            Ok(decoded_data) => {
                                let original_buf = original_c.as_read().buf();
                                if decoded_data.len() != original_buf.len() {
                                    let err = page_codec_size_mismatch(
                                        page_idx,
                                        original_buf.len(),
                                        decoded_data.len(),
                                    );
                                    tracing::error!(
                                        "Page codec returned wrong-sized decoded page for page_id={page_idx}: expected {}, got {}",
                                        original_buf.len(),
                                        decoded_data.len()
                                    );
                                    original_c.error(err);
                                    return original_c.get_error();
                                }
                                original_buf.as_mut_slice().copy_from_slice(&decoded_data);
                                original_c.complete(bytes_read);
                                original_c.get_error()
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to decode page data for page_id={page_idx}: {e}"
                                );
                                turso_assert!(
                                    !original_c.failed(),
                                    "Original completion already has an error"
                                );
                                original_c.error(CompletionError::DecryptionError { page_idx });
                                original_c.get_error()
                            }
                        }
                    },
                );
                let wrapped_completion = Completion::new_read(read_buffer, decode_complete);
                self.file.pread(pos, wrapped_completion)
            }
            EncryptionOrChecksum::Checksum(ctx) => {
                let checksum_ctx = ctx.clone();
                let read_buffer = r.buf_arc();
                let original_c = c.clone();

                let verify_complete =
                    Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                        let (buf, bytes_read) = match res {
                            Ok((buf, bytes_read)) => (buf, bytes_read),
                            Err(err) => {
                                original_c.error(err);
                                return original_c.get_error();
                            }
                        };
                        if bytes_read <= 0 {
                            tracing::trace!("Read page {page_idx} with {} bytes", bytes_read);
                            original_c.complete(bytes_read);
                            return original_c.get_error();
                        }
                        match checksum_ctx.verify_checksum(buf.as_mut_slice(), page_idx) {
                            Ok(_) => {
                                original_c.complete(bytes_read);
                                original_c.get_error()
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to verify checksum for page_id={page_idx}: {e}"
                                );
                                turso_assert!(
                                    !original_c.failed(),
                                    "Original completion already has an error"
                                );
                                original_c.error(e);
                                original_c.get_error()
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
        turso_assert_greater_than!(page_idx, 0);
        turso_assert_greater_than_or_equal!(buffer_size, 512);
        turso_assert_less_than_or_equal!(buffer_size, 65536);
        turso_assert_eq!(buffer_size & (buffer_size - 1), 0);
        let Some(pos) = (page_idx as u64 - 1).checked_mul(buffer_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };
        let buffer = match &io_ctx.encryption_or_checksum {
            EncryptionOrChecksum::Encryption(ctx) => {
                encode_buffer(page_idx, buffer, ctx, PageCodecLocation::Database)?
            }
            EncryptionOrChecksum::PageCodec(ctx) => {
                encode_buffer(page_idx, buffer, ctx.as_ref(), PageCodecLocation::Database)?
            }
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
        turso_assert_greater_than!(first_page_idx, 0);
        turso_assert_greater_than_or_equal!(page_size, 512);
        turso_assert_less_than_or_equal!(page_size, 65536);
        turso_assert_eq!(page_size & (page_size - 1), 0);

        let Some(pos) = (first_page_idx as u64 - 1).checked_mul(page_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };
        let buffers = match &io_ctx.encryption_or_checksum() {
            EncryptionOrChecksum::Encryption(ctx) => buffers
                .into_iter()
                .enumerate()
                .map(|(i, buffer)| {
                    encode_buffer(first_page_idx + i, buffer, ctx, PageCodecLocation::Database)
                })
                .collect::<Result<Vec<_>>>()?,
            EncryptionOrChecksum::PageCodec(ctx) => buffers
                .into_iter()
                .enumerate()
                .map(|(i, buffer)| {
                    encode_buffer(
                        first_page_idx + i,
                        buffer,
                        ctx.as_ref(),
                        PageCodecLocation::Database,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
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
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.file.sync(c, sync_type)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn size(&self) -> Result<u64> {
        self.file.size()
    }

    #[instrument(skip_all, level = Level::DEBUG)]
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

fn encode_buffer(
    page_idx: usize,
    buffer: Arc<Buffer>,
    ctx: &dyn PageCodec,
    location: PageCodecLocation,
) -> Result<Arc<Buffer>> {
    let encrypted_data = ctx.encode_page(buffer.as_slice(), page_idx, location)?;
    validate_page_codec_output_len(page_idx, buffer.len(), encrypted_data.len())?;
    Ok(Arc::new(Buffer::new(encrypted_data.to_vec())))
}

fn checksum_buffer(page_idx: usize, buffer: Arc<Buffer>, ctx: &ChecksumContext) -> Arc<Buffer> {
    ctx.add_checksum_to_page(buffer.as_mut_slice(), page_idx)
        .unwrap();
    buffer
}

#[cfg(test)]
mod page_codec_tests {
    use super::*;
    use crate::File;
    use crate::{io::IO, MemoryIO};

    #[derive(Debug)]
    struct ShortPageCodec;

    impl PageCodec for ShortPageCodec {
        fn required_reserved_bytes(&self) -> u8 {
            0
        }

        fn encode_page(
            &self,
            page: &[u8],
            _page_idx: usize,
            _location: PageCodecLocation,
        ) -> Result<Vec<u8>> {
            Ok(page[..page.len() - 1].to_vec())
        }

        fn decode_page(
            &self,
            page: &[u8],
            _page_idx: usize,
            _location: PageCodecLocation,
        ) -> Result<Vec<u8>> {
            Ok(page[..page.len() - 1].to_vec())
        }
    }

    struct MockFile {
        read_result: std::result::Result<i32, CompletionError>,
    }

    impl File for MockFile {
        fn lock_file(&self, _exclusive: bool) -> Result<()> {
            Ok(())
        }

        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }

        fn pread(&self, _pos: u64, c: Completion) -> Result<Completion> {
            match self.read_result {
                Ok(bytes_read) => c.complete(bytes_read),
                Err(err) => c.error(err),
            }
            Ok(c)
        }

        fn pwrite(&self, _pos: u64, _buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            c.complete(0);
            Ok(c)
        }

        fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
            c.complete(0);
            Ok(c)
        }

        fn size(&self) -> Result<u64> {
            Ok(0)
        }

        fn truncate(&self, _len: u64, c: Completion) -> Result<Completion> {
            c.complete(0);
            Ok(c)
        }
    }

    #[test]
    fn page_codec_encode_rejects_wrong_sized_database_page() {
        let buffer = Arc::new(Buffer::new(vec![1, 2, 3, 4]));
        let err = encode_buffer(7, buffer, &ShortPageCodec, PageCodecLocation::Database)
            .expect_err("wrong-sized encoded page must fail");

        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::PageCodecSizeMismatch {
                page_idx: 7,
                expected: 4,
                actual: 3,
            })
        ));
    }

    #[test]
    fn page_codec_read_rejects_wrong_sized_database_page() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Ok(4096),
            }),
        };
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(Arc::new(ShortPageCodec));
        let page_idx = 9usize;
        let original = Completion::new_read(Arc::new(Buffer::new_temporary(4096)), |_res| None);

        let wrapped = db_file
            .read_page(page_idx, &io_ctx, original.clone())
            .unwrap();
        let err = MemoryIO::new()
            .wait_for_completion(wrapped)
            .expect_err("wrong-sized decoded page must fail");

        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::PageCodecSizeMismatch {
                page_idx: 9,
                expected: 4096,
                actual: 4095,
            })
        ));
        assert_eq!(
            original.get_error(),
            Some(CompletionError::PageCodecSizeMismatch {
                page_idx: 9,
                expected: 4096,
                actual: 4095,
            })
        );
    }
}

#[cfg(all(test, feature = "checksum"))]
mod tests {
    use super::*;
    use crate::File;
    use crate::{io::IO, MemoryIO};

    struct MockFile {
        read_result: std::result::Result<i32, CompletionError>,
    }

    impl File for MockFile {
        fn lock_file(&self, _exclusive: bool) -> Result<()> {
            Ok(())
        }

        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }

        fn pread(&self, _pos: u64, c: Completion) -> Result<Completion> {
            match self.read_result {
                Ok(bytes_read) => c.complete(bytes_read),
                Err(err) => c.error(err),
            }
            Ok(c)
        }

        fn pwrite(&self, _pos: u64, _buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            c.complete(0);
            Ok(c)
        }

        fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
            c.complete(0);
            Ok(c)
        }

        fn size(&self) -> Result<u64> {
            Ok(0)
        }

        fn truncate(&self, _len: u64, c: Completion) -> Result<Completion> {
            c.complete(0);
            Ok(c)
        }
    }

    #[test]
    fn checksum_read_wrapper_propagates_callback_errors() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile { read_result: Ok(0) }),
        };
        let io_ctx = IOContext::default();
        let page_idx = 1usize;
        let expected = 4096usize;
        let buf = Arc::new(Buffer::new_temporary(expected));
        let original = Completion::new_read(buf, move |res| {
            let (_, bytes_read) = res.expect("mock read should complete");
            if bytes_read == 0 {
                Some(CompletionError::ShortRead {
                    page_idx,
                    expected,
                    actual: 0,
                })
            } else {
                None
            }
        });

        let wrapped = db_file
            .read_page(page_idx, &io_ctx, original.clone())
            .unwrap();
        let io = MemoryIO::new();
        let err = io
            .wait_for_completion(wrapped)
            .expect_err("wrapped completion must fail");
        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::ShortRead { .. })
        ));
        assert!(matches!(
            original.get_error(),
            Some(CompletionError::ShortRead { .. })
        ));
    }

    #[test]
    fn checksum_read_wrapper_propagates_transport_errors_to_original_completion() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Err(CompletionError::Aborted),
            }),
        };
        let io_ctx = IOContext::default();
        let page_idx = 1usize;
        let buf = Arc::new(Buffer::new_temporary(4096));
        let original = Completion::new_read(buf, |_res| None);

        let wrapped = db_file
            .read_page(page_idx, &io_ctx, original.clone())
            .unwrap();
        let io = MemoryIO::new();
        let err = io
            .wait_for_completion(wrapped)
            .expect_err("wrapped completion must fail");
        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::Aborted)
        ));
        assert_eq!(original.get_error(), Some(CompletionError::Aborted));
    }
}
