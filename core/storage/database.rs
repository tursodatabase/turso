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

/// A stable, non-secret identifier for a page codec configuration.
///
/// This identifies the transform configuration rather than the codec
/// implementation. Callers must return the same value when reopening or
/// sharing a database with an equivalent codec, and a different value for any
/// codec that could transform pages differently. Do not include key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCodecId([u8; 16]);

impl PageCodecId {
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

/// Converts complete SQLite page images between their on-disk and in-memory
/// representations.
///
/// The engine supplies an output buffer exactly as large as the input page,
/// enforcing SQLite's fixed page-size invariant without codec allocations.
/// Codecs needing per-page metadata must store it in reserved bytes.
///
/// Codecs that support reopening initialized databases must implement
/// [`Self::probe_header`]. The engine cannot safely decode an arbitrary
/// full-page transform before it knows the physical page size.
pub trait PageCodec: Send + Sync {
    /// Returns a stable, non-secret identifier for this codec configuration.
    ///
    /// The identifier is retained by the shared [`crate::Database`] so every
    /// connection and cached open for the same file uses an equivalent codec.
    /// It must change whenever the codec could produce different on-disk bytes.
    fn codec_id(&self) -> PageCodecId;

    /// Reports page layout metadata from the raw prefix of page 1.
    ///
    /// Return `None` only when the codec cannot reopen initialized databases.
    fn probe_header(&self, _raw_page1_prefix: &[u8]) -> Result<Option<PageCodecHeaderInfo>> {
        Ok(None)
    }

    /// Returns the exact number of reserved bytes required in every page.
    fn required_reserved_bytes(&self) -> u8;
    fn encode_page(
        &self,
        page: &[u8],
        output: &mut [u8],
        page_idx: usize,
        location: PageCodecLocation,
    ) -> Result<()>;
    fn decode_page(
        &self,
        page: &[u8],
        output: &mut [u8],
        page_idx: usize,
        location: PageCodecLocation,
    ) -> Result<()>;
}

#[derive(Clone)]
/// Storage-layer operations applied to complete SQLite page images.
///
/// `Checksum` is separate because it only maintains and verifies bytes in the
/// reserved page tail; it does not transform logical SQLite content.
pub enum PageTransform {
    Encryption(EncryptionContext),
    PageCodec(Arc<dyn PageCodec>),
    Checksum(ChecksumContext),
    None,
}

#[deprecated(note = "renamed to PageTransform")]
pub type EncryptionOrChecksum = PageTransform;

impl std::fmt::Debug for PageTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Encryption(_) => "Encryption",
            Self::PageCodec(_) => "PageCodec",
            Self::Checksum(_) => "Checksum",
            Self::None => "None",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone)]
pub struct IOContext {
    page_transform: PageTransform,
}

impl IOContext {
    pub fn encryption_context(&self) -> Option<&EncryptionContext> {
        match &self.page_transform {
            PageTransform::Encryption(ctx) => Some(ctx),
            _ => None,
        }
    }

    pub fn get_reserved_space_bytes(&self) -> u8 {
        match &self.page_transform {
            PageTransform::Encryption(ctx) => ctx.required_reserved_bytes(),
            PageTransform::PageCodec(ctx) => ctx.required_reserved_bytes(),
            PageTransform::Checksum(ctx) => ctx.required_reserved_bytes(),
            PageTransform::None => Default::default(),
        }
    }

    pub fn set_encryption(&mut self, encryption_ctx: EncryptionContext) {
        self.page_transform = PageTransform::Encryption(encryption_ctx);
    }

    pub fn set_page_codec(&mut self, codec: Arc<dyn PageCodec>) {
        self.page_transform = PageTransform::PageCodec(codec);
    }

    pub fn has_encryption(&self) -> bool {
        matches!(self.page_transform, PageTransform::Encryption(_))
    }

    pub fn has_page_codec(&self) -> bool {
        matches!(self.page_transform, PageTransform::PageCodec(_))
    }

    pub fn page_codec(&self) -> Option<Arc<dyn PageCodec>> {
        match &self.page_transform {
            PageTransform::PageCodec(codec) => Some(codec.clone()),
            _ => None,
        }
    }

    pub fn page_transform(&self) -> &PageTransform {
        &self.page_transform
    }

    #[deprecated(note = "renamed to page_transform")]
    #[allow(deprecated)]
    pub fn encryption_or_checksum(&self) -> &EncryptionOrChecksum {
        self.page_transform()
    }

    pub fn reset_checksum(&mut self) {
        self.page_transform = PageTransform::None;
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

        match &io_ctx.page_transform {
            PageTransform::Encryption(ctx) => {
                let encryption_ctx = ctx.clone();
                let read_buffer = Arc::new(Buffer::new_temporary(r.buf_arc().len()));
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
                        // A zero-byte read is the expected probe result for an
                        // empty database; only complete page images are decryptable.
                        if bytes_read == 0 {
                            original_c.complete(bytes_read);
                            return original_c.get_error();
                        }
                        turso_assert_greater_than!(
                            bytes_read,
                            0,
                            "database: expected positive bytes for encrypted page",
                            { "page_idx": page_idx }
                        );
                        if bytes_read as usize != buf.len() {
                            original_c.complete(bytes_read);
                            return original_c.get_error();
                        }
                        let original_buf = original_c.as_read().buf();
                        match encryption_ctx.decrypt_page(buf.as_slice(), page_idx) {
                            Ok(decrypted) => {
                                original_buf.as_mut_slice().copy_from_slice(&decrypted);
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
            PageTransform::PageCodec(ctx) => {
                let page_codec = ctx.clone();
                let read_buffer = Arc::new(Buffer::new_temporary(r.buf_arc().len()));
                let original_c = c;
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
                        // A zero-byte read is the expected probe result for an
                        // empty database; only complete page images are decodable.
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
                        if bytes_read as usize != buf.len() {
                            original_c.complete(bytes_read);
                            return original_c.get_error();
                        }
                        let original_buf = original_c.as_read().buf();
                        match page_codec.decode_page(
                            buf.as_slice(),
                            original_buf.as_mut_slice(),
                            page_idx,
                            PageCodecLocation::Database,
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
                                original_c.error(CompletionError::PageCodecError { page_idx });
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
        let buffer = match &io_ctx.page_transform {
            PageTransform::Encryption(ctx) => encrypt_buffer(page_idx, buffer, ctx)?,
            PageTransform::PageCodec(ctx) => {
                encode_buffer(page_idx, buffer, ctx.as_ref(), PageCodecLocation::Database)?
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
        let buffers = match &io_ctx.page_transform() {
            PageTransform::Encryption(ctx) => buffers
                .into_iter()
                .enumerate()
                .map(|(i, buffer)| encrypt_buffer(first_page_idx + i, buffer, ctx))
                .collect::<Result<Vec<_>>>()?,
            PageTransform::PageCodec(ctx) => buffers
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
    location: PageCodecLocation,
) -> Result<Arc<Buffer>> {
    let encoded = Arc::new(Buffer::new_temporary(buffer.len()));
    ctx.encode_page(
        buffer.as_slice(),
        encoded.as_mut_slice(),
        page_idx,
        location,
    )?;
    Ok(encoded)
}

fn encrypt_buffer(
    page_idx: usize,
    buffer: Arc<Buffer>,
    ctx: &EncryptionContext,
) -> Result<Arc<Buffer>> {
    Ok(Arc::new(Buffer::new(
        ctx.encrypt_page(buffer.as_slice(), page_idx)?,
    )))
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
            page: &[u8],
            output: &mut [u8],
            _page_idx: usize,
            _location: PageCodecLocation,
        ) -> Result<()> {
            for (input, output) in page.iter().zip(output) {
                *output = input ^ self.0;
            }
            Ok(())
        }

        fn decode_page(
            &self,
            page: &[u8],
            output: &mut [u8],
            _page_idx: usize,
            _location: PageCodecLocation,
        ) -> Result<()> {
            self.encode_page(page, output, _page_idx, _location)
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
            page: &[u8],
            output: &mut [u8],
            _page_idx: usize,
            _location: PageCodecLocation,
        ) -> Result<()> {
            match self {
                Self::Encode => Err(LimboError::InternalError("codec encode failed".into())),
                Self::Decode => {
                    output.copy_from_slice(page);
                    Ok(())
                }
            }
        }

        fn decode_page(
            &self,
            page: &[u8],
            output: &mut [u8],
            _page_idx: usize,
            _location: PageCodecLocation,
        ) -> Result<()> {
            match self {
                Self::Encode => {
                    output.copy_from_slice(page);
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
            encode_buffer(7, buffer, &XorPageCodec(0xa5), PageCodecLocation::Database).unwrap();
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
    fn page_codec_empty_database_read_completes_without_decoding() {
        let db_file = DatabaseFile {
            file: Arc::new(MockFile {
                read_result: Ok(0),
                writes_submitted: Arc::new(AtomicUsize::new(0)),
            }),
        };
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(Arc::new(XorPageCodec(0xa5)));
        let original = Completion::new_read(Arc::new(Buffer::new_temporary(4096)), |_| None);

        let wrapped = db_file.read_page(9, &io_ctx, original.clone()).unwrap();
        MemoryIO::new().wait_for_completion(wrapped).unwrap();
        assert!(original.succeeded());
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
