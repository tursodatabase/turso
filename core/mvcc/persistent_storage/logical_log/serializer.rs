use super::*;

/// A logical-log fragment that can be flattened into an exact byte iterator.
///
/// `TursoFromIterator::try_extend` uses the iterator's upper bound to reserve
/// before mutation and specializes for the concrete iterator where possible.
pub(super) trait LogBufferWrite {
    fn into_bytes(self) -> impl Iterator<Item = u8>;
}

macro_rules! log_write {
    ($serializer:expr, [$first:expr $(, $rest:expr)* $(,)?], $extension:expr) => {{
        if let Some(extension) = $extension {
            log_write!(
                $serializer,
                [
                    $first,
                    $($rest,)*
                    SqliteVarint(extension.len() as u64),
                    extension,
                ]
            )
        } else {
            log_write!($serializer, [$first $(, $rest)*])
        }
    }};
    ($serializer:expr, [$first:expr $(, $rest:expr)* $(,)?]) => {{
        $serializer.write(
            LogBufferWrite::into_bytes($first)
                $(.chain(LogBufferWrite::into_bytes($rest)))*
        )
    }};
}

pub(crate) struct LogSerializer<'a> {
    buffer: &'a mut Vec<u8>,
}

impl<'a> LogSerializer<'a> {
    #[inline(always)]
    pub(crate) fn new(buffer: &'a mut Vec<u8>) -> Self {
        Self { buffer }
    }

    #[inline(always)]
    fn write(&mut self, bytes: impl Iterator<Item = u8>) -> Result<()> {
        self.buffer.try_extend(bytes)?;
        Ok(())
    }

    #[inline(always)]
    pub(super) fn insert<W: LogBufferWrite>(&mut self, offset: usize, value: W) -> Result<()> {
        if offset > self.buffer.len() {
            return Err(LimboError::InternalError(
                "logical log serializer insert offset exceeds buffer length".to_string(),
            ));
        }

        let bytes = value.into_bytes();
        let encoded_len = exact_iterator_len(&bytes).ok_or_else(log_buffer_len_overflow)?;
        let new_len = self
            .buffer
            .len()
            .checked_add(encoded_len)
            .ok_or_else(log_buffer_len_overflow)?;
        self.buffer.try_extend(bytes)?;
        debug_assert_eq!(
            self.buffer.len(),
            new_len,
            "logical log serializer encoded length mismatch"
        );
        self.buffer[offset..].rotate_right(encoded_len);
        Ok(())
    }

    pub(super) fn insert_portable_extension(
        &mut self,
        offset: usize,
        record: ExtensionRecord<PortableChangePayload<'_>>,
    ) -> Result<()> {
        if offset > self.buffer.len() {
            return Err(LimboError::InternalError(
                "logical log serializer insert offset exceeds buffer length".to_string(),
            ));
        }

        let encoded_len = extension_record_len(&record)?;
        let payload_len = encoded_len - EXTENSION_RECORD_HEADER_SIZE;
        let payload_len = u32::try_from(payload_len).map_err(|_| {
            LimboError::InternalError("Logical log extension record exceeds u32".to_string())
        })?;
        let original_len = self.buffer.len();
        let new_len = original_len
            .checked_add(encoded_len)
            .ok_or_else(log_buffer_len_overflow)?;

        let ExtensionRecord {
            extension_type,
            extension_flags,
            payload,
        } = record;
        let body_len = payload.body_len().ok_or_else(log_buffer_len_overflow)?;
        log_write!(
            self,
            [
                extension_type.to_le_bytes(),
                extension_flags.to_le_bytes(),
                payload_len.to_le_bytes(),
                ProtoVarint(body_len as u64),
                ProtoKey::new(1, PROTO_WIRE_VARINT),
                ProtoVarint(payload.end_offset),
                ProtoKey::new(2, PROTO_WIRE_VARINT),
                ProtoVarint(payload.commit_ts),
                payload.encoded_metadata,
            ]
        )?;
        debug_assert_eq!(
            self.buffer.len(),
            new_len,
            "logical log serializer encoded length mismatch"
        );
        self.buffer[offset..].rotate_right(encoded_len);
        Ok(())
    }

    /// Serialize one operation into the wrapped buffer.
    ///
    /// Layout: tag(1) | flags(1) | table_id(4) | payload_len(varint) | payload.
    #[inline(always)]
    pub(crate) fn serialize_op_entry(
        &mut self,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()> {
        let is_delete = row_version.end().is_some();

        let mut flags = 0u8;
        if row_version.btree_resident {
            flags |= OP_FLAG_BTREE_RESIDENT;
        }
        if portable_extension.is_some_and(|extension| !extension.is_empty()) {
            flags |= OP_FLAG_PORTABLE_EXTENSION;
        }

        let table_id_i64: i64 = row_version.row.id.table_id.into();
        turso_assert!(
            table_id_i64 < 0,
            "table_id_i64 should be negative, but got {table_id_i64}"
        );
        turso_assert!(
            (i32::MIN as i64..=i32::MAX as i64).contains(&table_id_i64),
            "table_id_i64 out of i32 range: {table_id_i64}"
        );
        let table_id = (table_id_i64 as i32).to_le_bytes();
        let portable_extension = portable_extension.filter(|extension| !extension.is_empty());

        match (&row_version.row.id.row_id, is_delete) {
            (&RowKey::Int(rowid), false) => {
                let record = row_version.row.payload();
                let rowid = rowid as u64;
                let payload_len = varint_len(rowid)
                    .checked_add(record.len())
                    .ok_or_else(log_buffer_len_overflow)?;
                log_write!(
                    self,
                    [
                        OP_UPSERT_TABLE,
                        flags,
                        table_id,
                        SqliteVarint(payload_len as u64),
                        SqliteVarint(rowid),
                        record,
                    ],
                    portable_extension
                )?;
            }
            (&RowKey::Int(rowid), true) => {
                let rowid = rowid as u64;
                log_write!(
                    self,
                    [
                        OP_DELETE_TABLE,
                        flags,
                        table_id,
                        SqliteVarint(varint_len(rowid) as u64),
                        SqliteVarint(rowid),
                    ],
                    portable_extension
                )?;
            }
            (RowKey::Record(_), is_delete) => {
                let key = row_version.row.payload();
                log_write!(
                    self,
                    [
                        if is_delete {
                            OP_DELETE_INDEX
                        } else {
                            OP_UPSERT_INDEX
                        },
                        flags,
                        table_id,
                        SqliteVarint(key.len() as u64),
                        key,
                    ],
                    portable_extension
                )?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub(crate) fn serialize_header_entry(&mut self, header: &DatabaseHeader) -> Result<()> {
        // Header operations use tag-only addressing (table_id=0, flags=0).
        log_write!(
            self,
            [
                OP_UPDATE_HEADER,
                0,
                0i32.to_le_bytes(),
                SqliteVarint(DatabaseHeader::SIZE as u64),
                bytemuck::bytes_of(header),
            ]
        )
    }

    #[inline(always)]
    pub(crate) fn serialize_tx_trailer(&mut self, crc: u32) -> Result<()> {
        log_write!(self, [crc.to_le_bytes(), END_MAGIC.to_le_bytes()])
    }

    pub(crate) fn encode_delete_portable_extension(
        identity_record: Option<&[u8]>,
        pk_record: Option<&[u8]>,
        rowid: Option<i64>,
    ) -> Result<Vec<u8>> {
        let mut extension = Vec::new();
        let mut serializer = LogSerializer::new(&mut extension);
        if let Some(identity_record) = identity_record.filter(|record| !record.is_empty()) {
            log_write!(
                serializer,
                [
                    ProtoKey::new(
                        OP_EXT_FIELD_DELETE_IDENTITY_RECORD,
                        PROTO_WIRE_LENGTH_DELIMITED
                    ),
                    ProtoVarint(identity_record.len() as u64),
                    identity_record,
                ]
            )?;
        }
        if let Some(pk_record) = pk_record.filter(|record| !record.is_empty()) {
            log_write!(
                serializer,
                [
                    ProtoKey::new(OP_EXT_FIELD_DELETE_PK_RECORD, PROTO_WIRE_LENGTH_DELIMITED),
                    ProtoVarint(pk_record.len() as u64),
                    pk_record,
                ]
            )?;
        }
        if let Some(rowid) = rowid {
            log_write!(
                serializer,
                [ProtoSint64::new(OP_EXT_FIELD_DELETE_ROWID, rowid)]
            )?;
        }
        Ok(extension)
    }

    pub(crate) fn encrypt_payload_in_place(&mut self, payload: EncryptedPayload<'_>) -> Result<()> {
        let tag_size = payload.enc_ctx.tag_size();
        let nonce_size = payload.enc_ctx.nonce_size();
        let on_disk_size = encrypted_payload_blob_size(
            payload.plaintext_size,
            payload.chunk_size,
            tag_size,
            nonce_size,
        )?;
        let payload_end = payload
            .payload_start
            .checked_add(on_disk_size)
            .ok_or_else(|| {
                LimboError::InternalError("encrypted payload end offset overflow".to_string())
            })?;
        if payload.payload_start > self.buffer.len() {
            return Err(LimboError::InternalError(
                "encrypted payload start exceeds buffer length".to_string(),
            ));
        }
        let additional = payload_end.checked_sub(self.buffer.len()).ok_or_else(|| {
            LimboError::InternalError("encrypted payload shrinks unexpectedly".to_string())
        })?;
        self.buffer.try_reserve(additional)?;
        self.buffer.resize(payload_end, 0);

        let chunk_count = encrypted_payload_chunk_count(payload.plaintext_size, payload.chunk_size);
        let plaintext_size = u64::try_from(payload.plaintext_size).map_err(|_| {
            LimboError::InternalError("encrypted plaintext size exceeds u64".to_string())
        })?;
        let mut encrypted_tail = on_disk_size;

        // Ciphertext chunks are larger than plaintext chunks. Moving and
        // encrypting them back-to-front preserves plaintext not yet consumed.
        for chunk_index in (0..chunk_count).rev() {
            let plaintext_len = encrypted_chunk_plaintext_len(
                payload.plaintext_size,
                chunk_index,
                payload.chunk_size,
            )?;
            let encrypted_len = encrypted_chunk_blob_size(plaintext_len, tag_size, nonce_size)?;
            encrypted_tail -= encrypted_len;

            let plaintext_offset = chunk_index * payload.chunk_size;
            let plaintext_start = payload.payload_start + plaintext_offset;
            let plaintext_end = plaintext_start + plaintext_len;
            let ciphertext_start = payload.payload_start + encrypted_tail;
            if ciphertext_start != plaintext_start {
                self.buffer
                    .copy_within(plaintext_start..plaintext_end, ciphertext_start);
            }

            let aad = build_encrypted_chunk_aad(
                payload.salt,
                (chunk_index + 1 == chunk_count).then_some(plaintext_size),
                payload.op_count,
                payload.commit_ts,
                u32::try_from(chunk_index).map_err(|_| {
                    LimboError::InternalError(
                        "encrypted payload chunk index exceeds u32".to_string(),
                    )
                })?,
            );
            let tag_start = ciphertext_start + plaintext_len;
            let nonce_start = tag_start + tag_size;
            let chunk_end = nonce_start + nonce_size;
            let chunk = &mut self.buffer[ciphertext_start..chunk_end];
            let (ciphertext, tag_and_nonce) = chunk.split_at_mut(plaintext_len);
            let (tag, nonce) = tag_and_nonce.split_at_mut(tag_size);
            payload
                .enc_ctx
                .encrypt_chunk_in_place(ciphertext, &aad, tag, nonce)?;
        }

        turso_assert!(
            encrypted_tail == 0,
            "encrypted payload tail cursor should end at payload start"
        );
        turso_assert!(
            self.buffer.len() - payload.payload_start == on_disk_size,
            "encrypted on-disk payload size mismatch"
        );
        Ok(())
    }
}

struct SqliteVarint(u64);

impl LogBufferWrite for SqliteVarint {
    #[inline(always)]
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        let mut bytes = [0; 9];
        let len = write_varint(&mut bytes, self.0);
        bytes.into_iter().take(len)
    }
}

struct ProtoVarint(u64);

impl LogBufferWrite for ProtoVarint {
    #[inline(always)]
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        let mut value = self.0;
        let mut bytes = [0; 10];
        let mut len = 0;
        loop {
            let byte = value as u8;
            value >>= 7;
            bytes[len] = byte | (u8::from(value != 0) * 0x80);
            len += 1;
            if value == 0 {
                break;
            }
        }
        bytes.into_iter().take(len)
    }
}

const PROTO_WIRE_VARINT: u64 = 0;
const PROTO_WIRE_LENGTH_DELIMITED: u64 = 2;

struct ProtoKey {
    field: u64,
    wire_type: u64,
}

impl ProtoKey {
    fn new(field: u64, wire_type: u64) -> Self {
        Self { field, wire_type }
    }
}

impl LogBufferWrite for ProtoKey {
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        ProtoVarint((self.field << 3) | self.wire_type).into_bytes()
    }
}

struct ProtoSint64 {
    field: u64,
    value: i64,
}

impl ProtoSint64 {
    fn new(field: u64, value: i64) -> Self {
        Self { field, value }
    }

    fn zigzag(&self) -> u64 {
        ((self.value << 1) ^ (self.value >> 63)) as u64
    }
}

impl LogBufferWrite for ProtoSint64 {
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        let zigzag = self.zigzag();
        ProtoKey::new(self.field, PROTO_WIRE_VARINT)
            .into_bytes()
            .chain(ProtoVarint(zigzag).into_bytes())
    }
}

impl LogBufferWrite for u8 {
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        std::iter::once(self)
    }
}

impl<const N: usize> LogBufferWrite for [u8; N] {
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        self.into_iter()
    }
}

impl LogBufferWrite for &[u8] {
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        self.iter().copied()
    }
}

fn exact_iterator_len(iter: &impl Iterator) -> Option<usize> {
    let (lower, upper) = iter.size_hint();
    (upper == Some(lower)).then_some(lower)
}

fn log_buffer_len_overflow() -> LimboError {
    LimboError::InternalError("logical log serialization size overflow".to_string())
}

fn proto_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

/// Context needed to find the stable raw-log cursor encoded in a portable payload.
pub(super) struct PortableEndOffsetCtx {
    pub(super) write_offset: u64,
    pub(super) includes_log_header: bool,
    pub(super) tx_header_size: usize,
    pub(super) recovery_payload_size: usize,
    pub(super) encrypted_payload_chunk_size: usize,
    pub(super) encryption_overhead: Option<(usize, usize)>,
}

#[derive(Clone, Copy)]
pub(super) struct PortableChangePayload<'a> {
    end_offset: u64,
    commit_ts: u64,
    encoded_metadata: &'a [u8],
}

impl<'a> PortableChangePayload<'a> {
    fn new(end_offset: u64, commit_ts: u64, encoded_metadata: &'a [u8]) -> Self {
        Self {
            end_offset,
            commit_ts,
            encoded_metadata,
        }
    }

    /// Iterates until the embedded `end_offset` and the encoded frame size agree.
    ///
    /// The cursor itself is a varint, so crossing a varint-width boundary changes
    /// the frame size that determines the cursor.
    pub(super) fn with_stable_end_offset(
        ctx: PortableEndOffsetCtx,
        commit_ts: u64,
        encoded_metadata: &'a [u8],
    ) -> Result<Self> {
        let frame_end_offset = |portable_payload_len: usize| -> Result<u64> {
            let extension_size = EXTENSION_RECORD_HEADER_SIZE
                .checked_add(portable_payload_len)
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "portable logical extension size overflow".to_string(),
                    )
                })?;
            let plaintext_size = ctx
                .recovery_payload_size
                .checked_add(extension_size)
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "portable logical plaintext size overflow".to_string(),
                    )
                })?;
            let body_size = if let Some((tag_size, nonce_size)) = ctx.encryption_overhead {
                encrypted_payload_blob_size(
                    plaintext_size,
                    ctx.encrypted_payload_chunk_size,
                    tag_size,
                    nonce_size,
                )?
            } else {
                plaintext_size
            };
            let prefix_size = if ctx.includes_log_header {
                LOG_HDR_SIZE
            } else {
                0
            };
            let frame_size = prefix_size
                .checked_add(ctx.tx_header_size)
                .and_then(|size| size.checked_add(body_size))
                .and_then(|size| size.checked_add(TX_TRAILER_SIZE))
                .ok_or_else(|| {
                    LimboError::InternalError("portable logical frame size overflow".to_string())
                })?;
            let frame_size = u64::try_from(frame_size).map_err(|_| {
                LimboError::InternalError("portable logical frame size exceeds u64".to_string())
            })?;
            ctx.write_offset.checked_add(frame_size).ok_or_else(|| {
                LimboError::InternalError("portable logical frame offset overflow".to_string())
            })
        };

        let mut end_offset = frame_end_offset(0)?;
        loop {
            let payload = Self::new(end_offset, commit_ts, encoded_metadata);
            let encoded_len = payload.encoded_len().ok_or_else(log_buffer_len_overflow)?;
            let next_end_offset = frame_end_offset(encoded_len)?;
            if next_end_offset == end_offset {
                return Ok(payload);
            }
            end_offset = next_end_offset;
        }
    }

    fn body_len(&self) -> Option<usize> {
        proto_varint_len((1 << 3) | PROTO_WIRE_VARINT)
            .checked_add(proto_varint_len(self.end_offset))?
            .checked_add(proto_varint_len((2 << 3) | PROTO_WIRE_VARINT))?
            .checked_add(proto_varint_len(self.commit_ts))?
            .checked_add(self.encoded_metadata.len())
    }

    fn encoded_len(&self) -> Option<usize> {
        let body_len = self.body_len()?;
        proto_varint_len(body_len as u64).checked_add(body_len)
    }
}

impl LogBufferWrite for PortableChangePayload<'_> {
    fn into_bytes(self) -> impl Iterator<Item = u8> {
        let body_len = self
            .body_len()
            .expect("portable payload length was checked before writing");
        ProtoVarint(body_len as u64)
            .into_bytes()
            .chain(ProtoKey::new(1, PROTO_WIRE_VARINT).into_bytes())
            .chain(ProtoVarint(self.end_offset).into_bytes())
            .chain(ProtoKey::new(2, PROTO_WIRE_VARINT).into_bytes())
            .chain(ProtoVarint(self.commit_ts).into_bytes())
            .chain(self.encoded_metadata.iter().copied())
    }
}

pub(super) struct ExtensionRecord<P> {
    extension_type: u16,
    extension_flags: u16,
    payload: P,
}

impl<P> ExtensionRecord<P> {
    pub(super) fn new(extension_type: u16, extension_flags: u16, payload: P) -> Self {
        Self {
            extension_type,
            extension_flags,
            payload,
        }
    }
}

pub(super) fn extension_record_len(
    record: &ExtensionRecord<PortableChangePayload<'_>>,
) -> Result<usize> {
    let payload_len = record
        .payload
        .encoded_len()
        .ok_or_else(log_buffer_len_overflow)?;
    let _ = u32::try_from(payload_len).map_err(|_| {
        LimboError::InternalError("Logical log extension record exceeds u32".to_string())
    })?;
    EXTENSION_RECORD_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or_else(log_buffer_len_overflow)
}

pub(crate) struct EncryptedPayload<'a> {
    pub(super) enc_ctx: &'a EncryptionContext,
    pub(super) payload_start: usize,
    pub(super) plaintext_size: usize,
    pub(super) chunk_size: usize,
    pub(super) salt: u64,
    pub(super) op_count: u32,
    pub(super) commit_ts: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_iterator_has_exact_size_hint() {
        let bytes = PortableChangePayload::new(1, 2, &[3]).into_bytes();

        assert_eq!(bytes.size_hint(), (6, Some(6)));
        assert_eq!(bytes.collect::<Vec<_>>(), [5, 8, 1, 16, 2, 3]);
    }

    #[test]
    fn insert_preserves_surrounding_bytes() {
        let mut buffer = vec![1, 4];
        LogSerializer::new(&mut buffer).insert(1, [2, 3]).unwrap();

        assert_eq!(buffer, [1, 2, 3, 4]);
    }

    #[test]
    fn portable_extension_insert_preserves_wire_format() {
        let mut buffer = vec![9, 10];
        let payload = PortableChangePayload::new(1, 2, &[3]);
        let record = ExtensionRecord::new(0x1234, 0x5678, payload);

        LogSerializer::new(&mut buffer)
            .insert_portable_extension(1, record)
            .unwrap();

        assert_eq!(
            buffer,
            [9, 0x34, 0x12, 0x78, 0x56, 6, 0, 0, 0, 5, 8, 1, 16, 2, 3, 10,]
        );
    }
}
