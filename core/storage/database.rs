use crate::io::FileSyncType;
use crate::storage::checksum::ChecksumContext;
use crate::storage::encryption::EncryptionContext;
use crate::storage::page_transform::{
    page_codec_completion_error, page_codec_encryption_context, page_codec_from_encryption,
    PageCodec, PageCodecContext, PageLocation, PageTransform,
};
use crate::storage::sqlite3_ondisk::PageSize;
use crate::sync::Arc;
use crate::{io::Completion, Buffer, CompletionError, LimboError, Result};
use crate::{
    turso_assert, turso_assert_eq, turso_assert_greater_than, turso_assert_greater_than_or_equal,
    turso_assert_less_than_or_equal,
};
use tracing::{instrument, Level};

#[derive(Debug, Clone)]
pub struct IOContext {
    page_transform: PageTransform,
}

impl IOContext {
    pub fn encryption_context(&self) -> Option<&EncryptionContext> {
        match &self.page_transform {
            PageTransform::Codec(codec) => page_codec_encryption_context(codec.as_ref()),
            _ => None,
        }
    }

    pub fn get_reserved_space_bytes(&self) -> u8 {
        match &self.page_transform {
            PageTransform::Checksum(ctx) => ctx.required_reserved_bytes(),
            PageTransform::Codec(ctx) => ctx.required_reserved_bytes(),
            PageTransform::None => Default::default(),
        }
    }

    pub fn set_encryption(&mut self, encryption_ctx: EncryptionContext) {
        self.page_transform = PageTransform::Codec(page_codec_from_encryption(encryption_ctx));
    }

    pub(crate) fn reset_page_size_in_encryption_ctx(&mut self, page_size: PageSize) {
        let PageTransform::Codec(codec) = &mut self.page_transform else {
            return;
        };
        let Some(encryption_ctx) = page_codec_encryption_context(codec.as_ref()) else {
            return;
        };
        let mut encryption_ctx = encryption_ctx.clone();
        encryption_ctx.set_page_size(page_size);
        *codec = page_codec_from_encryption(encryption_ctx);
    }

    pub(crate) fn set_page_codec(&mut self, codec: Arc<dyn PageCodec>) {
        self.page_transform = PageTransform::Codec(codec);
    }

    pub(crate) fn has_encryption(&self) -> bool {
        self.encryption_context().is_some()
    }

    pub(crate) fn has_external_page_codec(&self) -> bool {
        matches!(self.page_transform, PageTransform::Codec(_)) && !self.has_encryption()
    }

    pub(crate) fn page_codec_external(&self) -> Option<Arc<dyn PageCodec>> {
        match &self.page_transform {
            PageTransform::Codec(codec) if !self.has_encryption() => Some(codec.clone()),
            _ => None,
        }
    }

    pub(crate) fn page_transform(&self) -> &PageTransform {
        &self.page_transform
    }

    /// Returns whether page I/O uses a codec transform.
    ///
    /// Checksums are maintained separately and do not count as codecs. So encryption
    pub fn has_codec_transform(&self) -> bool {
        matches!(self.page_transform, PageTransform::Codec(_))
    }

    pub fn reset_checksum(&mut self) {
        if matches!(self.page_transform, PageTransform::Checksum(_)) {
            self.page_transform = PageTransform::None;
        }
    }
}

impl Default for IOContext {
    fn default() -> Self {
        #[cfg(feature = "checksum")]
        let page_transform = PageTransform::Checksum(ChecksumContext::default());
        #[cfg(not(feature = "checksum"))]
        let page_transform = PageTransform::None;
        Self { page_transform }
    }
}

/// DatabaseStorage is an interface a database file that consists of pages.
///
/// The purpose of this trait is to abstract the upper layers of Limbo from
/// the storage medium. A database can either be a file on disk, like in SQLite,
/// or something like a remote page server service.
pub trait DatabaseStorage: Send + Sync {
    /// Reads the encoded prefix of page 1 without applying a page transform.
    ///
    /// This is only for bootstrapping the page layout before a complete page
    /// can be read and decoded. Callers that need logical page-1 contents must
    /// use [`DatabaseStorage::read_page`].
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

        match io_ctx.page_transform() {
            PageTransform::Codec(ctx) => {
                let page_codec = ctx.clone();
                // TODO(v): support in-place codec decoding to avoid this extra page buffer.
                let read_buffer = Arc::new(Buffer::new_temporary(r.buf_arc().len()));
                let original_c = c;
                let codec_context =
                    PageCodecContext::from_page_idx(page_idx, PageLocation::Database)?;
                let decode_complete =
                    Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                        let (buf, bytes_read) = match res {
                            Ok((buf, bytes_read)) => (buf, bytes_read),
                            Err(err) => {
                                tracing::error!(err = ?err);
                                original_c.error(err);
                                return original_c.get_error();
                            }
                        };
                        if bytes_read == 0 {
                            original_c.complete(bytes_read);
                            return original_c.get_error();
                        }
                        turso_assert_greater_than!(
                            bytes_read,
                            0,
                            "database: expected positive bytes for page codec page",
                            { "page_idx": page_idx }
                        );
                        let expected = original_c.as_read().buf().len();
                        if bytes_read as usize != expected {
                            original_c.error(CompletionError::ShortRead {
                                page_idx,
                                expected,
                                actual: bytes_read as usize,
                            });
                            return original_c.get_error();
                        }
                        let original_buf = original_c.as_read().buf();
                        match page_codec.decode_page(
                            codec_context,
                            buf.as_slice(),
                            original_buf.as_mut_slice(),
                        ) {
                            Ok(()) => {
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
                                original_c.error(page_codec_completion_error(
                                    page_codec.as_ref(),
                                    page_idx,
                                ));
                                original_c.get_error()
                            }
                        }
                    });
                let wrapped_completion = Completion::new_read(read_buffer, decode_complete);
                self.file.pread(pos, wrapped_completion)
            }
            PageTransform::Checksum(ctx) => {
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
            PageTransform::None => self.file.pread(pos, c),
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
        let buffer = match io_ctx.page_transform() {
            PageTransform::Codec(ctx) => {
                encode_buffer(page_idx, buffer, ctx.as_ref(), PageLocation::Database)?
            }
            PageTransform::Checksum(ctx) => checksum_buffer(page_idx, buffer, ctx),
            PageTransform::None => buffer,
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
        let buffers = match io_ctx.page_transform() {
            PageTransform::Codec(ctx) => buffers
                .into_iter()
                .enumerate()
                .map(|(i, buffer)| {
                    encode_buffer(
                        first_page_idx + i,
                        buffer,
                        ctx.as_ref(),
                        PageLocation::Database,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            PageTransform::Checksum(ctx) => buffers
                .into_iter()
                .enumerate()
                .map(|(i, buffer)| checksum_buffer(first_page_idx + i, buffer, ctx))
                .collect::<Vec<_>>(),
            PageTransform::None => buffers,
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
    location: PageLocation,
) -> Result<Arc<Buffer>> {
    let encoded = Arc::new(Buffer::new_temporary(buffer.len()));
    let context = PageCodecContext::from_page_idx(page_idx, location)?;
    ctx.encode_page(context, buffer.as_slice(), encoded.as_mut_slice())?;
    Ok(encoded)
}

fn checksum_buffer(page_idx: usize, buffer: Arc<Buffer>, ctx: &ChecksumContext) -> Arc<Buffer> {
    ctx.add_checksum_to_page(buffer.as_mut_slice(), page_idx)
        .unwrap();
    buffer
}

#[cfg(test)]
mod page_codec_tests {
    use super::*;
    use crate::storage::page_transform::PageCodecId;
    use crate::File;
    use crate::{io::IO, MemoryIO};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct XorPageCodec(u8);

    impl PageCodec for XorPageCodec {
        fn codec_id(&self) -> PageCodecId {
            let mut id = *b"xor-page-codec--";
            id[15] = self.0;
            PageCodecId::new(id)
        }

        fn required_reserved_bytes(&self) -> u8 {
            0
        }

        fn encode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            let _ = context;
            for (input, output) in input.iter().zip(output) {
                *output = input ^ self.0;
            }
            Ok(())
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            self.encode_page(context, input, output)
        }
    }

    #[derive(Debug)]
    enum FailingPageCodec {
        Encode,
        Decode,
    }

    impl PageCodec for FailingPageCodec {
        fn codec_id(&self) -> PageCodecId {
            let mut id = *b"failing-page-cod";
            id[15] = match self {
                Self::Encode => 1,
                Self::Decode => 2,
            };
            PageCodecId::new(id)
        }

        fn required_reserved_bytes(&self) -> u8 {
            0
        }

        fn encode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            match self {
                Self::Encode => Err(LimboError::InternalError("codec encode failed".into())),
                Self::Decode => {
                    output.copy_from_slice(input);
                    Ok(())
                }
            }
        }

        fn decode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            match self {
                Self::Encode => {
                    output.copy_from_slice(input);
                    Ok(())
                }
                Self::Decode => Err(LimboError::InternalError("codec decode failed".into())),
            }
        }
    }

    struct MockFile {
        read_result: std::result::Result<i32, CompletionError>,
        writes_submitted: Arc<AtomicUsize>,
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
            self.writes_submitted.fetch_add(1, Ordering::Relaxed);
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
    fn page_codec_encodes_into_fixed_size_database_buffer() {
        let buffer = Arc::new(Buffer::new(vec![1, 2, 3, 4]));
        let encoded =
            encode_buffer(7, buffer, &XorPageCodec(0xa5), PageLocation::Database).unwrap();
        assert_eq!(encoded.as_slice(), &[0xa4, 0xa7, 0xa6, 0xa1]);
    }

    #[test]
    fn page_codec_read_decodes_into_original_database_buffer() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Ok(4096),
                writes_submitted: Arc::new(AtomicUsize::new(0)),
            }),
        };
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
        let page_idx = 9usize;
        let original = Completion::new_read(Arc::new(Buffer::new_temporary(4096)), |_res| None);

        let wrapped = db_file
            .read_page(page_idx, &io_ctx, original.clone())
            .unwrap();
        MemoryIO::new().wait_for_completion(wrapped).unwrap();
        assert!(original.succeeded());
        assert!(original
            .as_read()
            .buf()
            .as_slice()
            .iter()
            .all(|byte| *byte == 0xa5));
    }

    #[test]
    fn page_codec_zero_byte_read_reaches_original_completion() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Ok(0),
                writes_submitted: Arc::new(AtomicUsize::new(0)),
            }),
        };
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
        let page_idx = 9usize;
        let bytes_seen = Arc::new(AtomicUsize::new(usize::MAX));
        let bytes_seen_callback = bytes_seen.clone();
        let original = Completion::new_read(Arc::new(Buffer::new_temporary(4096)), move |result| {
            let (_, bytes_read) = result.expect("zero-byte read should reach the callback");
            bytes_seen_callback.store(bytes_read as usize, Ordering::Relaxed);
            None
        });

        let wrapped = db_file
            .read_page(page_idx, &io_ctx, original.clone())
            .unwrap();
        MemoryIO::new().wait_for_completion(wrapped).unwrap();

        assert!(original.succeeded());
        assert_eq!(bytes_seen.load(Ordering::Relaxed), 0);
        assert!(
            original
                .as_read()
                .buf()
                .as_slice()
                .iter()
                .all(|byte| *byte == 0),
            "an absent page must not be decoded"
        );
    }

    #[test]
    fn page_codec_partial_database_read_fails_before_decode() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Ok(128),
                writes_submitted: Arc::new(AtomicUsize::new(0)),
            }),
        };
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
        let original = Completion::new_read(Arc::new(Buffer::new_temporary(512)), |_| None);

        let wrapped = db_file.read_page(1, &io_ctx, original.clone()).unwrap();
        let err = MemoryIO::new().wait_for_completion(wrapped).unwrap_err();

        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::ShortRead {
                page_idx: 1,
                expected: 512,
                actual: 128,
            })
        ));
        assert!(matches!(
            original.get_error(),
            Some(CompletionError::ShortRead {
                page_idx: 1,
                expected: 512,
                actual: 128,
            })
        ));
    }

    #[test]
    fn page_codec_database_write_error_does_not_submit_io() {
        let writes_submitted = Arc::new(AtomicUsize::new(0));
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Ok(0),
                writes_submitted: writes_submitted.clone(),
            }),
        };
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(Arc::new(FailingPageCodec::Encode));

        let err = db_file
            .write_page(
                1,
                Arc::new(Buffer::new_temporary(512)),
                &io_ctx,
                Completion::new_write(|_| {}),
            )
            .unwrap_err();

        assert!(err.to_string().contains("codec encode failed"));
        assert_eq!(
            writes_submitted.load(Ordering::Relaxed),
            0,
            "a codec error must prevent the database write from being submitted"
        );
    }

    #[test]
    fn page_codec_database_read_reports_decode_error() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Ok(512),
                writes_submitted: Arc::new(AtomicUsize::new(0)),
            }),
        };
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(Arc::new(FailingPageCodec::Decode));
        let original = Completion::new_read(Arc::new(Buffer::new_temporary(512)), |_| None);

        let wrapped = db_file.read_page(1, &io_ctx, original.clone()).unwrap();
        let err = MemoryIO::new().wait_for_completion(wrapped).unwrap_err();

        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 1 })
        ));
        assert!(matches!(
            original.get_error(),
            Some(CompletionError::PageCodecError { page_idx: 1 })
        ));
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
