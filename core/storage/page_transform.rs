use std::any::Any;

use crate::storage::checksum::ChecksumContext;
use crate::storage::encryption::EncryptionContext;
use crate::sync::Arc;
use crate::{CompletionError, LimboError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCodecHeaderInfo {
    pub page_size: usize,
    pub reserved_space: u8,
}

impl PageCodecHeaderInfo {
    const SQLITE_BOOTSTRAP_HEADER_LEN: usize = 21;

    /// Reads the SQLite page-layout fields that must remain visible before page
    /// 1 can be decoded. Codecs that transform these bytes must override
    /// [`PageCodec::bootstrap_page_info`].
    pub fn from_visible_sqlite_header(raw_page1_prefix: &[u8]) -> Result<Self> {
        if raw_page1_prefix.len() < Self::SQLITE_BOOTSTRAP_HEADER_LEN {
            return Err(LimboError::InvalidArgument(format!(
                "page codec bootstrap requires at least {} bytes, got {}",
                Self::SQLITE_BOOTSTRAP_HEADER_LEN,
                raw_page1_prefix.len()
            )));
        }
        // SQLite stores the two-byte page size at offsets 16..18 and the
        // per-page reserved-space byte at offset 20.
        let raw_page_size =
            u16::from_be_bytes(raw_page1_prefix[16..18].try_into().expect("two bytes"));
        let page_size = if raw_page_size == 1 {
            65_536
        } else {
            usize::from(raw_page_size)
        };
        Ok(Self {
            page_size,
            reserved_space: raw_page1_prefix[20],
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageLocation {
    Database,
    Wal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCodecContext {
    /// One-based SQLite page number.
    pub page_no: u32,
    pub location: PageLocation,
}

impl PageCodecContext {
    pub const fn new(page_no: u32, location: PageLocation) -> Self {
        Self { page_no, location }
    }

    pub(crate) fn from_page_idx(page_idx: usize, location: PageLocation) -> Result<Self> {
        let page_no = u32::try_from(page_idx).map_err(|_| LimboError::IntegerOverflow)?;
        Ok(Self::new(page_no, location))
    }
}

/// A stable, non-secret identifier for a page codec configuration.
///
/// This identifies the transform configuration rather than the codec
/// implementation. Callers must return the same value when reopening or
/// sharing a database with an equivalent codec, and a different value for any
/// codec that could transform pages differently. Do not include any secrets or confidential
/// info in the PageCodecId.
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
/// Before decoding page 1, the engine uses [`Self::bootstrap_page_info`] to
/// discover the physical page size and reserved space.
pub trait PageCodec: Any + Send + Sync {
    /// Returns a stable, non-secret identifier for this codec configuration.
    ///
    /// The identifier is retained by the shared [`crate::Database`] so every
    /// connection and cached open for the same file uses an equivalent codec.
    /// It must change whenever the codec could produce different on-disk bytes.
    fn codec_id(&self) -> PageCodecId;

    /// Reports page layout metadata from the encoded prefix of page 1.
    ///
    /// The default requires the SQLite page-size and reserved-space fields at
    /// bytes 16..18 and 20 to remain visible. Codecs such as SQLCipher that
    /// transform those fields must recover the values themselves.
    fn bootstrap_page_info(&self, raw_page1_prefix: &[u8]) -> Result<PageCodecHeaderInfo> {
        PageCodecHeaderInfo::from_visible_sqlite_header(raw_page1_prefix)
    }

    /// Returns the exact number of reserved bytes required in every page.
    fn required_reserved_bytes(&self) -> u8;
    fn encode_page(&self, context: PageCodecContext, input: &[u8], output: &mut [u8])
        -> Result<()>;
    fn decode_page(&self, context: PageCodecContext, input: &[u8], output: &mut [u8])
        -> Result<()>;
}

impl dyn PageCodec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    pub(crate) fn is_builtin_encryption(&self) -> bool {
        self.as_any().is::<EncryptionPageCodec>()
    }
}

pub(crate) fn page_codec_from_encryption(context: EncryptionContext) -> Arc<dyn PageCodec> {
    Arc::new(EncryptionPageCodec::new(context))
}

pub(crate) fn page_codec_encryption_context(codec: &dyn PageCodec) -> Option<&EncryptionContext> {
    codec
        .as_any()
        .downcast_ref::<EncryptionPageCodec>()
        .map(|codec| &codec.context)
}

pub(crate) fn page_codec_completion_error(
    codec: &dyn PageCodec,
    page_idx: usize,
) -> CompletionError {
    if codec.is_builtin_encryption() {
        CompletionError::DecryptionError { page_idx }
    } else {
        CompletionError::PageCodecError { page_idx }
    }
}

struct EncryptionPageCodec {
    context: EncryptionContext,
}

impl EncryptionPageCodec {
    fn new(context: EncryptionContext) -> Self {
        Self { context }
    }
}

impl PageCodec for EncryptionPageCodec {
    fn codec_id(&self) -> PageCodecId {
        let mut id = *b"turso-encrypt-v0";
        id[15] = self.context.cipher_mode().cipher_id();
        PageCodecId::new(id)
    }

    fn required_reserved_bytes(&self) -> u8 {
        self.context.required_reserved_bytes()
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        // todo(v): once we have in place encryption, we can avoid allocations here
        let encrypted = self.context.encrypt_page(input, context.page_no as usize)?;
        output.copy_from_slice(&encrypted);
        Ok(())
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        // todo(v): once we have in place encryption, we can avoid allocations here
        let decrypted = self.context.decrypt_page(input, context.page_no as usize)?;
        output.copy_from_slice(&decrypted);
        Ok(())
    }
}

#[derive(Clone)]
/// Storage-layer operations applied to complete SQLite page images.
///
/// `Checksum` is separate because it only maintains and verifies bytes in the
/// reserved page tail; it does not transform logical SQLite content.
pub(crate) enum PageTransform {
    #[cfg_attr(not(feature = "checksum"), allow(dead_code))]
    Checksum(ChecksumContext),
    Codec(Arc<dyn PageCodec>),
    None,
}

impl std::fmt::Debug for PageTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Checksum(_) => "Checksum",
            Self::Codec(_) => "Codec",
            Self::None => "None",
        };
        f.write_str(name)
    }
}
