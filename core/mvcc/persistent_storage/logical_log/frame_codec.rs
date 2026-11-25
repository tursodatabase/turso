//! Native MVCC frame decoding and sizing for sync.
//!
//! Validates log headers, frame layouts, CRCs, and extension records.
//! Decrypts encrypted body chunks with AEAD tag verification and produces
//! plaintext wire parts with a separate checksum chain.

use super::{
    build_encrypted_chunk_aad, derive_initial_crc, encrypted_payload_blob_size,
    encrypted_payload_chunk_count, EncryptionContext, FramePosition, LogHeader,
    ENCRYPTED_PAYLOAD_CHUNK_SIZE, END_MAGIC, EXT_FRAME_MAGIC, FRAME_MAGIC, LOG_HDR_SIZE,
    LOG_VERSION, LOG_VERSION_V2, LOG_VERSION_V4, TX_EXT_HEADER_SIZE, TX_HEADER_SIZE,
    TX_TRAILER_SIZE,
};
use crate::{LimboError, Result};

#[derive(Clone, Debug)]
pub struct ValidatedLogHeader {
    bytes: [u8; LOG_HDR_SIZE],
    header: LogHeader,
}

impl ValidatedLogHeader {
    pub fn version(&self) -> u8 {
        self.header.version
    }

    pub fn initial_crc(&self) -> u32 {
        derive_initial_crc(self.header.salt)
    }

    /// Returns the header bytes of the source log, as they are stored. Do not send
    /// them before the frames that `plaintext_wire_parts` makes: an encrypted
    /// source log has version 4, which sync clients reject. Send `wire_bytes`
    /// instead.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the header of the plaintext stream that `plaintext_wire_parts`
    /// makes from the frames of this log. Version 4 differs from version 3 only
    /// in the AAD of encrypted chunks, so the plaintext stream of a version 4 log
    /// is a version 3 log with the same salt. Other headers are returned unchanged.
    pub fn wire_bytes(&self) -> [u8; LOG_HDR_SIZE] {
        if self.header.version != LOG_VERSION_V4 {
            return self.bytes;
        }
        let mut header = self.header.clone();
        header.version = LOG_VERSION;
        header.encode()
    }
}

pub fn validate_log_header(bytes: &[u8]) -> Result<ValidatedLogHeader> {
    let bytes: &[u8; LOG_HDR_SIZE] = bytes
        .try_into()
        .map_err(|_| corrupt("logical log header must be exactly 56 bytes"))?;
    Ok(ValidatedLogHeader {
        header: LogHeader::decode(bytes)?,
        bytes: *bytes,
    })
}

/// Sizes of a transaction frame in storage and on the wire.
///
/// ```text
/// Plaintext storage / wire:
/// +---------------+------------------+------------------+---------------+
/// | TX header     | extension block  | recovery payload | CRC32C | END  |
/// +---------------+------------------+------------------+---------------+
///
/// Encrypted storage:
/// +---------------+-------------------------------------+---------------+
/// | TX header     | (ciphertext | tag | nonce) x chunks | CRC32C | END  |
/// +---------------+-------------------------------------+---------------+
/// ```
///
/// `header_size` is 24 bytes for compact frames or 40 bytes for extension frames.
/// The optional extension block occupies `extension_size` bytes, followed by
/// `recovery_size()` bytes; together they make up `plaintext_size`.
/// Encryption splits this combined body into chunks, adding a tag and nonce
/// to each chunk. The header and 8-byte trailer stay in plaintext.
/// `stored_size` covers the entire stored frame; `wire_size()` uses the
/// plaintext body without the encryption overhead.
#[derive(Clone, Copy, Debug)]
pub struct FrameLayout {
    header_size: usize,
    plaintext_size: usize,
    stored_size: usize,
    extension_size: usize,
    extension_records: u32,
}

impl FrameLayout {
    pub fn header_size(&self) -> usize {
        self.header_size
    }

    pub fn recovery_size(&self) -> usize {
        self.plaintext_size - self.extension_size
    }

    pub fn extension_size(&self) -> usize {
        self.extension_size
    }

    pub fn extension_records(&self) -> u32 {
        self.extension_records
    }

    pub fn stored_size(&self) -> usize {
        self.stored_size
    }

    pub fn wire_size(&self) -> usize {
        self.header_size + self.plaintext_size + TX_TRAILER_SIZE
    }
}

/// Inspect the frame layout from its header.
/// Returns `Ok(None)` if the header is incomplete; the body need not be present.
pub fn inspect_frame_prefix(
    bytes: &[u8],
    crypto: Option<&EncryptionContext>,
) -> Result<Option<FrameLayout>> {
    inspect_frame_prefix_with_overhead(bytes, crypto.map(|c| (c.tag_size(), c.nonce_size())))
}

/// Inspect frame layout using the source cipher's `(tag_size, nonce_size)`.
/// These extra bytes hold an authentication tag and nonce per encrypted chunk.
/// Plaintext frames have no encryption overhead and use `None`.
pub fn inspect_frame_prefix_with_overhead(
    bytes: &[u8],
    overhead: Option<(usize, usize)>,
) -> Result<Option<FrameLayout>> {
    if bytes.len() < 4 {
        return Ok(None);
    }
    let header_size = match u32_at(bytes, 0) {
        FRAME_MAGIC => TX_HEADER_SIZE,
        EXT_FRAME_MAGIC => TX_EXT_HEADER_SIZE,
        _ => return Err(corrupt("invalid logical log frame magic")),
    };
    if bytes.len() < header_size {
        return Ok(None);
    }
    let recovery_size = usize::try_from(u64_at(bytes, 4))
        .map_err(|_| corrupt("logical log recovery size overflow"))?;
    let (extension_size, extension_records) = if header_size == TX_EXT_HEADER_SIZE {
        let size = usize::try_from(u64_at(bytes, 24))
            .map_err(|_| corrupt("logical log extension size overflow"))?;
        let count = u32_at(bytes, 32);
        let flags = u32_at(bytes, 36);
        if !super::valid_extension_header(size as u64, count, flags) || (size > 0 && count == 0) {
            return Err(corrupt("invalid logical log extension flags or count"));
        }
        (size, count)
    } else {
        (0, 0)
    };
    let plaintext_size = recovery_size
        .checked_add(extension_size)
        .ok_or_else(|| corrupt("logical log plaintext size overflow"))?;
    let stored_body_size = if let Some((tag_size, nonce_size)) = overhead {
        let chunks = encrypted_payload_chunk_count(plaintext_size, ENCRYPTED_PAYLOAD_CHUNK_SIZE);
        if chunks > u32::MAX as usize {
            return Err(corrupt("logical log chunk index overflow"));
        }
        encrypted_payload_blob_size(
            plaintext_size,
            ENCRYPTED_PAYLOAD_CHUNK_SIZE,
            tag_size,
            nonce_size,
        )?
    } else {
        plaintext_size
    };
    let stored_size = header_size
        .checked_add(stored_body_size)
        .and_then(|n| n.checked_add(TX_TRAILER_SIZE))
        .ok_or_else(|| corrupt("logical log stored frame size overflow"))?;
    Ok(Some(FrameLayout {
        header_size,
        plaintext_size,
        stored_size,
        extension_size,
        extension_records,
    }))
}

#[derive(Debug)]
pub struct ValidatedFrame<'a> {
    header: &'a [u8],
    plaintext: &'a [u8],
    extension_size: usize,
    source_end: u64,
    source_crc: u32,
}

impl ValidatedFrame<'_> {
    pub fn extension(&self) -> &[u8] {
        &self.plaintext[..self.extension_size]
    }

    pub fn source_end(&self) -> u64 {
        self.source_end
    }

    pub fn source_crc(&self) -> u32 {
        self.source_crc
    }

    pub fn wire_size(&self) -> usize {
        self.header.len() + self.plaintext.len() + TX_TRAILER_SIZE
    }
}

/// Validate a complete stored frame and write its plaintext body to `output`.
/// Checks framing, the chained CRC, extension records, and encrypted chunk tags.
/// `source_start` is the frame's offset in the source log, used to calculate
/// the returned end offset. `source_crc` is the running CRC of the log before
/// the frame. In a version 4 log, the encrypted chunks authenticate both values,
/// so a frame validates only at the place where the writer appended it.
/// The caller must enforce the selected source range.
pub fn validate_frame_into<'a>(
    log: &ValidatedLogHeader,
    source_start: u64,
    stored_frame: &'a [u8],
    source_crc: u32,
    crypto: Option<&EncryptionContext>,
    output: &'a mut Vec<u8>,
) -> Result<ValidatedFrame<'a>> {
    output.clear();
    // Only an encrypted log has version 4, and its frame sizes include the
    // encryption overhead.
    if log.version() == LOG_VERSION_V4 && crypto.is_none() {
        return Err(corrupt(
            "logical log version 4 is encrypted, but no encryption key is set",
        ));
    }
    let layout = inspect_frame_prefix(stored_frame, crypto)?
        .ok_or_else(|| corrupt("truncated logical log frame header"))?;
    if stored_frame.len() != layout.stored_size {
        return Err(corrupt(
            "logical log frame length does not match its header",
        ));
    }
    if source_start < LOG_HDR_SIZE as u64 {
        return Err(corrupt("logical log frame overlaps file header"));
    }
    if log.version() == LOG_VERSION_V2 && layout.header_size == TX_EXT_HEADER_SIZE {
        return Err(corrupt("extension frame requires logical log version 3"));
    }
    let source_end = source_start
        .checked_add(stored_frame.len() as u64)
        .ok_or_else(|| corrupt("logical log source offset overflow"))?;
    let trailer_start = stored_frame.len() - TX_TRAILER_SIZE;
    let crc = crc32c::crc32c_append(source_crc, &stored_frame[..trailer_start]);
    if crc != u32_at(stored_frame, trailer_start)
        || u32_at(stored_frame, trailer_start + 4) != END_MAGIC
    {
        return Err(corrupt("logical log frame checksum or trailer mismatch"));
    }
    let stored_body = &stored_frame[layout.header_size..trailer_start];
    let position = (log.version() == LOG_VERSION_V4).then_some(FramePosition {
        offset: source_start,
        start_crc: source_crc,
    });
    if let Some(crypto) = crypto {
        let chunks =
            encrypted_payload_chunk_count(layout.plaintext_size, ENCRYPTED_PAYLOAD_CHUNK_SIZE);
        if position.is_some() && chunks == 0 {
            return Err(corrupt(
                "encrypted logical log frame has no payload to authenticate",
            ));
        }
        let mut offset = 0;
        for index in 0..chunks {
            let plaintext_len = (layout.plaintext_size - index * ENCRYPTED_PAYLOAD_CHUNK_SIZE)
                .min(ENCRYPTED_PAYLOAD_CHUNK_SIZE);
            let ciphertext_end = offset + plaintext_len + crypto.tag_size();
            let chunk_end = ciphertext_end + crypto.nonce_size();
            let aad = build_encrypted_chunk_aad(
                log.header.salt,
                (index + 1 == chunks).then_some(layout.plaintext_size as u64),
                u32_at(stored_frame, 12),
                u64_at(stored_frame, 16),
                index as u32,
                position,
            );
            let plaintext = match crypto.decrypt_chunk(
                &stored_body[offset..ciphertext_end],
                &stored_body[ciphertext_end..chunk_end],
                aad.as_bytes(),
            ) {
                Ok(plaintext) => plaintext,
                Err(_) => {
                    output.clear();
                    return Err(corrupt("logical log chunk authentication failed"));
                }
            };
            output.extend_from_slice(&plaintext);
            offset = chunk_end;
        }
    } else {
        output.extend_from_slice(stored_body);
    }
    if let Err(error) = super::find_extension_payload(
        &output[..layout.extension_size],
        layout.extension_records,
        None,
    ) {
        output.clear();
        return Err(error);
    }
    Ok(ValidatedFrame {
        header: &stored_frame[..layout.header_size],
        plaintext: output,
        extension_size: layout.extension_size,
        source_end,
        source_crc: crc,
    })
}

/// A plaintext frame split into a borrowed header/body and an owned trailer.
/// The trailer holds the recomputed CRC and end marker; `next_crc` is the
/// checksum seed for the next transmitted frame.
#[derive(Debug)]
pub struct PlaintextWireParts<'a> {
    pub header: &'a [u8],
    pub body: &'a [u8],
    pub trailer: [u8; TX_TRAILER_SIZE],
    pub next_crc: u32,
}

pub fn plaintext_wire_parts<'a>(
    frame: &'a ValidatedFrame<'_>,
    wire_crc: u32,
) -> PlaintextWireParts<'a> {
    let next_crc = crc32c::crc32c_append(
        crc32c::crc32c_append(wire_crc, frame.header),
        frame.plaintext,
    );
    let mut trailer = [0; TX_TRAILER_SIZE];
    trailer[..4].copy_from_slice(&next_crc.to_le_bytes());
    trailer[4..].copy_from_slice(&END_MAGIC.to_le_bytes());
    PlaintextWireParts {
        header: frame.header,
        body: frame.plaintext,
        trailer,
        next_crc,
    }
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checked frame bounds"),
    )
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked frame bounds"),
    )
}

fn corrupt(message: &str) -> LimboError {
    LimboError::Corrupt(message.into())
}
