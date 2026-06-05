use super::*;

/// A serialization fragment with an exact encoded length.
///
/// Logical-log encoding is a tight append path, and most fragments know their
/// serialized size before writing. This trait lets the serializer reserve
/// fallibly once for a bundle of fragments, then use the existing infallible
/// byte writes without sprinkling allocation checks through every push.
pub(super) trait LogBufferWrite {
    fn encoded_len(&self) -> Option<usize>;
    fn write_to(self, buffer: &mut Vec<u8>);
}

macro_rules! log_write {
    ($serializer:expr, [$($value:expr),+ $(,)?]) => {
        $serializer.write(($($value,)+))
    };
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
    fn write<W: LogBufferWrite>(&mut self, value: W) -> Result<()> {
        let encoded_len = value.encoded_len().ok_or_else(log_buffer_len_overflow)?;
        self.buffer.try_reserve(encoded_len)?;

        let start = self.buffer.len();
        value.write_to(self.buffer);
        debug_assert_eq!(
            self.buffer.len() - start,
            encoded_len,
            "logical log serializer encoded length mismatch"
        );
        Ok(())
    }

    #[inline(always)]
    pub(super) fn insert<W: LogBufferWrite>(&mut self, offset: usize, value: W) -> Result<()> {
        if offset > self.buffer.len() {
            return Err(LimboError::InternalError(
                "logical log serializer insert offset exceeds buffer length".to_string(),
            ));
        }

        let encoded_len = value.encoded_len().ok_or_else(log_buffer_len_overflow)?;
        let old_len = self.buffer.len();
        let new_len =
            checked_log_buffer_len(old_len, encoded_len).ok_or_else(log_buffer_len_overflow)?;
        self.buffer.try_reserve(encoded_len)?;

        value.write_to(self.buffer);
        debug_assert_eq!(
            self.buffer.len(),
            new_len,
            "logical log serializer encoded length mismatch"
        );
        self.buffer[offset..].rotate_right(encoded_len);
        Ok(())
    }

    /// Serialize one op into the wrapped buffer.
    /// Op layout: tag(1) | flags(1) | table_id(4, le i32) | payload_len(varint) | payload(variable)
    #[inline(always)]
    pub(crate) fn serialize_op_entry(
        &mut self,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()> {
        let is_delete = row_version.end.is_some();

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
        let table_id_i32 = table_id_i64 as i32;
        let portable_extension =
            portable_extension.filter(|portable_extension| !portable_extension.is_empty());
        let portable_extension_write =
            portable_extension.map(|extension| (Varint(extension.len() as u64), extension));

        match (&row_version.row.id.row_id, is_delete) {
            (&RowKey::Int(rowid), false) => {
                let record_bytes = row_version.row.payload();
                let rowid_u64 = rowid as u64;
                let rowid_len = varint_len(rowid_u64);
                let payload_len = checked_log_buffer_len(rowid_len, record_bytes.len())
                    .ok_or_else(log_buffer_len_overflow)?;
                log_write!(
                    self,
                    [
                        OP_UPSERT_TABLE,
                        flags,
                        table_id_i32.to_le_bytes(),
                        Varint(payload_len as u64),
                        Varint(rowid_u64),
                        record_bytes,
                        portable_extension_write,
                    ]
                )?;
            }
            (&RowKey::Int(rowid), true) => {
                let rowid_u64 = rowid as u64;
                let rowid_len = varint_len(rowid_u64);
                log_write!(
                    self,
                    [
                        OP_DELETE_TABLE,
                        flags,
                        table_id_i32.to_le_bytes(),
                        Varint(rowid_len as u64),
                        Varint(rowid_u64),
                        portable_extension_write,
                    ]
                )?;
            }
            (RowKey::Record(_), is_delete) => {
                let key_bytes = row_version.row.payload();
                log_write!(
                    self,
                    [
                        if is_delete {
                            OP_DELETE_INDEX
                        } else {
                            OP_UPSERT_INDEX
                        },
                        flags,
                        table_id_i32.to_le_bytes(),
                        Varint(key_bytes.len() as u64),
                        key_bytes,
                        portable_extension_write,
                    ]
                )?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub(crate) fn serialize_header_entry(&mut self, header: &DatabaseHeader) -> Result<()> {
        // Header op uses tag-only addressing (table_id=0, flags=0) and fixed payload length.
        log_write!(
            self,
            [
                OP_UPDATE_HEADER,
                0,
                0i32.to_le_bytes(),
                Varint(DatabaseHeader::SIZE as u64),
                bytemuck::bytes_of(header),
            ]
        )
    }

    #[inline(always)]
    pub(crate) fn serialize_tx_trailer(&mut self, crc: u32) -> Result<()> {
        log_write!(self, [crc.to_le_bytes(), END_MAGIC.to_le_bytes()])
    }

    #[inline(always)]
    pub(crate) fn encode_delete_portable_extension(
        identity_record: Option<&[u8]>,
        pk_record: Option<&[u8]>,
        rowid: Option<i64>,
    ) -> Result<Vec<u8>> {
        let mut extension = Vec::new();
        let mut serializer = LogSerializer::new(&mut extension);
        let identity_record = identity_record
            .filter(|record| !record.is_empty())
            .map(|record| ProtoBytes::new(OP_EXT_FIELD_DELETE_IDENTITY_RECORD, record));
        let pk_record = pk_record
            .filter(|record| !record.is_empty())
            .map(|record| ProtoBytes::new(OP_EXT_FIELD_DELETE_PK_RECORD, record));
        let rowid = rowid.map(|rowid| ProtoSint64::new(OP_EXT_FIELD_DELETE_ROWID, rowid));
        log_write!(serializer, [identity_record, pk_record, rowid])?;
        Ok(extension)
    }

    #[inline(always)]
    pub(crate) fn encrypt_payload_in_place(&mut self, payload: EncryptedPayload<'_>) -> Result<()> {
        let tag_size = payload.enc_ctx.tag_size();
        let nonce_size = payload.enc_ctx.nonce_size();
        let on_disk_payload_size = encrypted_payload_blob_size(
            payload.plaintext_size,
            payload.chunk_size,
            tag_size,
            nonce_size,
        )?;
        let payload_end = payload
            .payload_start
            .checked_add(on_disk_payload_size)
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
        let plaintext_size_u64 = u64::try_from(payload.plaintext_size).map_err(|_| {
            LimboError::InternalError("encrypted plaintext size exceeds u64".to_string())
        })?;
        let mut encrypted_tail = on_disk_payload_size;
        // Encrypted chunks are larger than plaintext chunks. Walk backward so
        // moving each plaintext chunk into its final slot cannot overwrite
        // plaintext that still needs to be encrypted.
        for chunk_index in (0..chunk_count).rev() {
            let plaintext_chunk_len = encrypted_chunk_plaintext_len(
                payload.plaintext_size,
                chunk_index,
                payload.chunk_size,
            )?;
            let encrypted_chunk_len =
                encrypted_chunk_blob_size(plaintext_chunk_len, tag_size, nonce_size)?;
            encrypted_tail = encrypted_tail
                .checked_sub(encrypted_chunk_len)
                .ok_or_else(|| {
                    LimboError::InternalError("encrypted chunk tail underflow".to_string())
                })?;
            let plaintext_offset =
                chunk_index.checked_mul(payload.chunk_size).ok_or_else(|| {
                    LimboError::InternalError(
                        "encrypted plaintext chunk offset overflow".to_string(),
                    )
                })?;
            let plaintext_start = payload
                .payload_start
                .checked_add(plaintext_offset)
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "encrypted plaintext start offset overflow".to_string(),
                    )
                })?;
            let plaintext_end = plaintext_start
                .checked_add(plaintext_chunk_len)
                .ok_or_else(|| {
                    LimboError::InternalError("encrypted plaintext end offset overflow".to_string())
                })?;
            let ciphertext_start = payload
                .payload_start
                .checked_add(encrypted_tail)
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "encrypted ciphertext start offset overflow".to_string(),
                    )
                })?;
            if ciphertext_start != plaintext_start {
                self.buffer
                    .copy_within(plaintext_start..plaintext_end, ciphertext_start);
            }

            let is_last_chunk = chunk_index + 1 == chunk_count;
            let aad = build_encrypted_chunk_aad(
                payload.salt,
                is_last_chunk.then_some(plaintext_size_u64),
                payload.op_count,
                payload.commit_ts,
                u32::try_from(chunk_index).map_err(|_| {
                    LimboError::InternalError(
                        "encrypted payload chunk index exceeds u32".to_string(),
                    )
                })?,
            );
            let tag_start = ciphertext_start
                .checked_add(plaintext_chunk_len)
                .ok_or_else(|| {
                    LimboError::InternalError("encrypted tag start offset overflow".to_string())
                })?;
            let nonce_start = tag_start.checked_add(tag_size).ok_or_else(|| {
                LimboError::InternalError("encrypted nonce start offset overflow".to_string())
            })?;
            let chunk_end = nonce_start.checked_add(nonce_size).ok_or_else(|| {
                LimboError::InternalError("encrypted chunk end offset overflow".to_string())
            })?;
            let chunk = &mut self.buffer[ciphertext_start..chunk_end];
            // encrypt_chunk_in_place appends auth data into separate tag/nonce
            // outputs, so the final on-disk chunk remains:
            // ciphertext(plaintext_len) | tag(tag_size) | nonce(nonce_size).
            let (plaintext_ciphertext, tag_and_nonce) = chunk.split_at_mut(plaintext_chunk_len);
            let (tag_out, nonce_out) = tag_and_nonce.split_at_mut(tag_size);
            payload.enc_ctx.encrypt_chunk_in_place(
                plaintext_ciphertext,
                &aad,
                tag_out,
                nonce_out,
            )?;
        }
        turso_assert!(
            encrypted_tail == 0,
            "encrypted payload tail cursor should end at the payload start"
        );
        turso_assert!(
            self.buffer.len() - payload.payload_start == on_disk_payload_size,
            "encrypted on-disk payload size mismatch"
        );
        Ok(())
    }
}

struct Varint(u64);

impl LogBufferWrite for Varint {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        Some(varint_len(self.0))
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        write_varint_to_vec(self.0, buffer);
    }
}

struct ProtoVarint(u64);

impl LogBufferWrite for ProtoVarint {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        Some(proto_varint_len(self.0))
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        write_proto_varint(self.0, buffer);
    }
}

const PROTO_WIRE_VARINT: u64 = 0;
const PROTO_WIRE_LENGTH_DELIMITED: u64 = 2;

struct ProtoKey {
    field: u64,
    wire_type: u64,
}

impl ProtoKey {
    #[inline(always)]
    fn new(field: u64, wire_type: u64) -> Self {
        Self { field, wire_type }
    }
}

impl LogBufferWrite for ProtoKey {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        ProtoVarint((self.field << 3) | self.wire_type).encoded_len()
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        ProtoVarint((self.field << 3) | self.wire_type).write_to(buffer);
    }
}

struct ProtoSint64 {
    field: u64,
    value: i64,
}

impl ProtoSint64 {
    #[inline(always)]
    fn new(field: u64, value: i64) -> Self {
        Self { field, value }
    }

    #[inline(always)]
    fn zigzag(&self) -> u64 {
        ((self.value << 1) ^ (self.value >> 63)) as u64
    }
}

impl LogBufferWrite for ProtoSint64 {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        checked_log_buffer_len(
            ProtoKey::new(self.field, PROTO_WIRE_VARINT).encoded_len()?,
            ProtoVarint(self.zigzag()).encoded_len()?,
        )
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        let zigzag = self.zigzag();
        ProtoKey::new(self.field, PROTO_WIRE_VARINT).write_to(buffer);
        ProtoVarint(zigzag).write_to(buffer);
    }
}

struct ProtoBytes<'a> {
    field: u64,
    value: &'a [u8],
}

impl<'a> ProtoBytes<'a> {
    #[inline(always)]
    fn new(field: u64, value: &'a [u8]) -> Self {
        Self { field, value }
    }
}

impl LogBufferWrite for ProtoBytes<'_> {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        let len_prefix = ProtoVarint(self.value.len() as u64).encoded_len()?;
        checked_log_buffer_len(
            ProtoKey::new(self.field, PROTO_WIRE_LENGTH_DELIMITED).encoded_len()?,
            len_prefix,
        )
        .and_then(|len| checked_log_buffer_len(len, self.value.len()))
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        ProtoKey::new(self.field, PROTO_WIRE_LENGTH_DELIMITED).write_to(buffer);
        ProtoVarint(self.value.len() as u64).write_to(buffer);
        buffer.extend_from_slice(self.value);
    }
}

impl LogBufferWrite for u8 {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        Some(1)
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        buffer.push(self);
    }
}

impl<const N: usize> LogBufferWrite for [u8; N] {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        Some(N)
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self);
    }
}

impl LogBufferWrite for &[u8] {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        Some(self.len())
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(self);
    }
}

impl<W: LogBufferWrite> LogBufferWrite for Option<W> {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        match self {
            Some(value) => value.encoded_len(),
            None => Some(0),
        }
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        if let Some(value) = self {
            value.write_to(buffer);
        }
    }
}

#[inline(always)]
fn checked_log_buffer_len(lhs: usize, rhs: usize) -> Option<usize> {
    lhs.checked_add(rhs)
}

#[inline(always)]
fn log_buffer_len_overflow() -> LimboError {
    LimboError::InternalError("logical log serialization size overflow".to_string())
}

macro_rules! impl_log_buffer_write_tuple {
    ($(($ty:ident, $value:ident)),+) => {
        impl<$($ty: LogBufferWrite),+> LogBufferWrite for ($($ty,)+) {
            #[inline(always)]
            fn encoded_len(&self) -> Option<usize> {
                let ($($value,)+) = self;
                let mut len = 0usize;
                $(
                    len = checked_log_buffer_len(len, $value.encoded_len()?)?;
                )+
                Some(len)
            }

            #[inline(always)]
            fn write_to(self, buffer: &mut Vec<u8>) {
                let ($($value,)+) = self;
                $(
                    $value.write_to(buffer);
                )+
            }
        }
    };
}

impl_log_buffer_write_tuple!((A, a));
impl_log_buffer_write_tuple!((A, a), (B, b));
impl_log_buffer_write_tuple!((A, a), (B, b), (C, c));
impl_log_buffer_write_tuple!((A, a), (B, b), (C, c), (D, d));
impl_log_buffer_write_tuple!((A, a), (B, b), (C, c), (D, d), (E, e));
impl_log_buffer_write_tuple!((A, a), (B, b), (C, c), (D, d), (E, e), (F, f));
impl_log_buffer_write_tuple!((A, a), (B, b), (C, c), (D, d), (E, e), (F, f), (G, g));
impl_log_buffer_write_tuple!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (F, f),
    (G, g),
    (H, h)
);
impl_log_buffer_write_tuple!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (F, f),
    (G, g),
    (H, h),
    (I, i)
);
impl_log_buffer_write_tuple!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (F, f),
    (G, g),
    (H, h),
    (I, i),
    (J, j)
);
impl_log_buffer_write_tuple!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (F, f),
    (G, g),
    (H, h),
    (I, i),
    (J, j),
    (K, k)
);
impl_log_buffer_write_tuple!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (F, f),
    (G, g),
    (H, h),
    (I, i),
    (J, j),
    (K, k),
    (L, l)
);
impl_log_buffer_write_tuple!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (F, f),
    (G, g),
    (H, h),
    (I, i),
    (J, j),
    (K, k),
    (L, l),
    (M, m)
);

fn write_proto_varint(mut value: u64, buffer: &mut Vec<u8>) {
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

fn read_proto_varint_from_buf(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    while *offset < bytes.len() {
        let byte = bytes[*offset];
        *offset += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(LimboError::Corrupt("protobuf varint overflows u64".into()));
        }
    }
    Err(LimboError::Corrupt("truncated protobuf varint".into()))
}

fn skip_proto_field(bytes: &[u8], offset: &mut usize, wire_type: u64) -> Result<()> {
    match wire_type {
        0 => {
            let _ = read_proto_varint_from_buf(bytes, offset)?;
        }
        2 => {
            let len = read_proto_varint_from_buf(bytes, offset)?;
            let len = usize::try_from(len)
                .map_err(|_| LimboError::Corrupt("protobuf field length overflows usize".into()))?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| LimboError::Corrupt("protobuf field length overflow".into()))?;
            if end > bytes.len() {
                return Err(LimboError::Corrupt(
                    "protobuf length-delimited field exceeds extension".into(),
                ));
            }
            *offset = end;
        }
        other => {
            return Err(LimboError::Corrupt(format!(
                "unsupported protobuf wire type in op extension: {other}"
            )));
        }
    }
    Ok(())
}

fn read_proto_sint64_from_buf(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let value = read_proto_varint_from_buf(bytes, offset)?;
    Ok(((value >> 1) as i64) ^ (-((value & 1) as i64)))
}

pub(super) fn decode_delete_portable_extension(
    extension: &[u8],
) -> Result<DeletePortableExtension> {
    let mut offset = 0usize;
    let mut decoded = DeletePortableExtension::default();
    while offset < extension.len() {
        let key = read_proto_varint_from_buf(extension, &mut offset)?;
        let field = key >> 3;
        let wire_type = key & 7;
        match (field, wire_type) {
            (OP_EXT_FIELD_DELETE_IDENTITY_RECORD, 2) => {
                let len = read_proto_varint_from_buf(extension, &mut offset)?;
                let len = usize::try_from(len).map_err(|_| {
                    LimboError::Corrupt("delete identity record length overflows usize".into())
                })?;
                let end = offset.checked_add(len).ok_or_else(|| {
                    LimboError::Corrupt("delete identity record length overflow".into())
                })?;
                if end > extension.len() {
                    return Err(LimboError::Corrupt(
                        "delete identity record exceeds op extension".into(),
                    ));
                }
                decoded.identity_record = extension[offset..end].to_vec();
                offset = end;
            }
            (OP_EXT_FIELD_DELETE_PK_RECORD, 2) => {
                let len = read_proto_varint_from_buf(extension, &mut offset)?;
                let len = usize::try_from(len).map_err(|_| {
                    LimboError::Corrupt("delete PK record length overflows usize".into())
                })?;
                let end = offset.checked_add(len).ok_or_else(|| {
                    LimboError::Corrupt("delete PK record length overflow".into())
                })?;
                if end > extension.len() {
                    return Err(LimboError::Corrupt(
                        "delete PK record exceeds op extension".into(),
                    ));
                }
                decoded.pk_record = extension[offset..end].to_vec();
                offset = end;
            }
            (OP_EXT_FIELD_DELETE_ROWID, 0) => {
                let _ = read_proto_sint64_from_buf(extension, &mut offset)?;
            }
            _ => skip_proto_field(extension, &mut offset, wire_type)?,
        }
    }
    Ok(decoded)
}

fn proto_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

/// Wraps commit-built logical op messages in one length-delimited
/// portable MVCC logical transaction payload and iterates until the embedded
/// `end_offset` matches the final frame size.
///
/// `end_offset` is part of the raw-log replay cursor, but its varint width can
/// change the payload length. The fixed-point loop converges after the varint
/// width stops changing.
pub(super) struct PortableEndOffsetCtx {
    pub(super) write_offset: u64,
    pub(super) includes_log_header: bool,
    pub(super) tx_header_size: usize,
    pub(super) recovery_payload_size: usize,
    pub(super) encrypted_payload_chunk_size: usize,
    pub(super) encryption_overhead: Option<(usize, usize)>,
}

pub(super) struct PortableChangePayload<'a> {
    end_offset: u64,
    commit_ts: u64,
    encoded_metadata: &'a [u8],
}

impl<'a> PortableChangePayload<'a> {
    #[inline(always)]
    fn new(end_offset: u64, commit_ts: u64, encoded_metadata: &'a [u8]) -> Self {
        Self {
            end_offset,
            commit_ts,
            encoded_metadata,
        }
    }

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
            let frame_bytes = prefix_size
                .checked_add(ctx.tx_header_size)
                .and_then(|value| value.checked_add(body_size))
                .and_then(|value| value.checked_add(TX_TRAILER_SIZE))
                .ok_or_else(|| {
                    LimboError::InternalError("portable logical frame size overflow".to_string())
                })?;
            ctx.write_offset
                .checked_add(frame_bytes as u64)
                .ok_or_else(|| {
                    LimboError::InternalError("portable logical frame offset overflow".to_string())
                })
        };

        let mut end_offset = frame_end_offset(0)?;
        loop {
            let payload = Self::new(end_offset, commit_ts, encoded_metadata);
            let payload_len = payload.encoded_len().ok_or_else(log_buffer_len_overflow)?;
            let next_end_offset = frame_end_offset(payload_len)?;
            if next_end_offset == end_offset {
                return Ok(payload);
            }
            end_offset = next_end_offset;
        }
    }

    #[inline(always)]
    fn body_len(&self) -> Option<usize> {
        checked_log_buffer_len(
            ProtoKey::new(1, PROTO_WIRE_VARINT).encoded_len()?,
            ProtoVarint(self.end_offset).encoded_len()?,
        )
        .and_then(|len| {
            checked_log_buffer_len(len, ProtoKey::new(2, PROTO_WIRE_VARINT).encoded_len()?)
        })
        .and_then(|len| checked_log_buffer_len(len, ProtoVarint(self.commit_ts).encoded_len()?))
        .and_then(|len| checked_log_buffer_len(len, self.encoded_metadata.len()))
    }
}

impl LogBufferWrite for PortableChangePayload<'_> {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        let body_len = self.body_len()?;
        checked_log_buffer_len(ProtoVarint(body_len as u64).encoded_len()?, body_len)
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        let body_len = self
            .body_len()
            .expect("portable payload body length was checked before writing");
        ProtoVarint(body_len as u64).write_to(buffer);
        // PortableLogicalTxn.end_offset, field 1, varint.
        ProtoKey::new(1, PROTO_WIRE_VARINT).write_to(buffer);
        ProtoVarint(self.end_offset).write_to(buffer);
        // PortableLogicalTxn.commit_ts, field 2, varint.
        ProtoKey::new(2, PROTO_WIRE_VARINT).write_to(buffer);
        ProtoVarint(self.commit_ts).write_to(buffer);
        buffer.extend_from_slice(self.encoded_metadata);
    }
}

pub(super) struct ExtensionRecord<P> {
    extension_type: u16,
    extension_flags: u16,
    payload: P,
}

impl<P> ExtensionRecord<P> {
    #[inline(always)]
    pub(super) fn new(extension_type: u16, extension_flags: u16, payload: P) -> Self {
        Self {
            extension_type,
            extension_flags,
            payload,
        }
    }
}

impl<P: LogBufferWrite> LogBufferWrite for ExtensionRecord<P> {
    #[inline(always)]
    fn encoded_len(&self) -> Option<usize> {
        checked_log_buffer_len(EXTENSION_RECORD_HEADER_SIZE, self.payload.encoded_len()?)
    }

    #[inline(always)]
    fn write_to(self, buffer: &mut Vec<u8>) {
        let payload_len = self
            .payload
            .encoded_len()
            .expect("extension record payload length was checked before writing");
        let payload_len = u32::try_from(payload_len)
            .expect("extension record payload length was checked before writing");
        buffer.extend_from_slice(&self.extension_type.to_le_bytes());
        buffer.extend_from_slice(&self.extension_flags.to_le_bytes());
        buffer.extend_from_slice(&payload_len.to_le_bytes());
        self.payload.write_to(buffer);
    }
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

pub(super) fn extension_record_len<P: LogBufferWrite>(
    record: &ExtensionRecord<P>,
) -> Result<usize> {
    let len = record.encoded_len().ok_or_else(log_buffer_len_overflow)?;
    let _ = u32::try_from(len).map_err(|_| {
        LimboError::InternalError("Logical log extension record exceeds u32".to_string())
    })?;
    Ok(len)
}
