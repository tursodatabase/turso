//! On-disk format v2 for FTS backing storage: the segment registry.
//!
//! One backing B-tree holds every row of an FTS index. Rows are key-only
//! (the whole `(path, chunk_no, bytes)` record is the index key), so the
//! format is strictly append-only: a row is inserted once and deleted once
//! (by merge), never rewritten in place. Row kinds are
//! distinguished by the `path` prefix:
//!
//! ```text
//! ("fts2/control",              0,      control_blob)     rare index-level facts
//! ("fts2/seg/<uuid>",           0,      descriptor_blob)  segment registry entry
//! ("fts2/chunk/<uuid>/<ord>",   n,      chunk_bytes)      segment file content
//! ("fts2/tomb/<uuid>",          doc_id, [])               posting tombstone
//! ```
//!
//! `<uuid>` is Tantivy's 32-char lowercase hex segment id, so two
//! transactions' rows can never collide, and appends by different
//! transactions commute under MVCC. There is no `meta.json` on disk: the
//! visible descriptor rows *are* the meta, and `meta.json` is synthesized
//! per snapshot (see [`synthesize_meta_json`]).
//!
//! The pre-registry FTS implementation stored a whole Tantivy directory
//! keyed by file name (`meta.json`, `<uuid>.term`, ...). None of those
//! names start with `fts2/`, so the open path can tell such a store apart
//! and refuse it with a rebuild hint; it is not read or converted.

use rustc_hash::FxHashMap as HashMap;
use std::collections::BTreeSet;
use tantivy::{index::SegmentId, schema::Schema, Index, IndexMeta, IndexSettings};

use crate::sync::Arc;
use crate::{LimboError, Result};

/// Storage format version stored in the v2 control row.
pub(super) const FTS_STORAGE_FORMAT_V2: u32 = 2;

pub(super) const FTS2_CONTROL_PATH: &str = "fts2/control";
pub(super) const FTS2_SEGMENT_PREFIX: &str = "fts2/seg/";
pub(super) const FTS2_CHUNK_PREFIX: &str = "fts2/chunk/";
pub(super) const FTS2_TOMB_PREFIX: &str = "fts2/tomb/";

/// Every row path starts with this; a stored row without it was written
/// by the pre-registry implementation.
pub(super) const FTS2_PATH_PREFIX: &str = "fts2/";

const FTS2_CONTROL_MAGIC: &[u8; 8] = b"TFTSCTL2";
const FTS2_SEGMENT_MAGIC: &[u8; 8] = b"TFTSSEG2";

/// Delete opstamp used for every synthesized delete meta. Tantivy only uses
/// it to name the `.del` file (`<uuid>.<opstamp>.del`); tombstone rows are
/// the real delete state, so one constant value is enough.
pub(super) const FTS2_TOMBSTONE_DELETE_OPSTAMP: u64 = 1;

pub(super) fn segment_registry_path(segment_id: &SegmentId) -> String {
    format!("{FTS2_SEGMENT_PREFIX}{}", segment_id.uuid_string())
}

pub(super) fn segment_chunk_path(segment_id: &SegmentId, file_ord: u32) -> String {
    format!(
        "{FTS2_CHUNK_PREFIX}{}/{file_ord:04}",
        segment_id.uuid_string()
    )
}

pub(super) fn segment_chunk_prefix(segment_id: &SegmentId) -> String {
    format!("{FTS2_CHUNK_PREFIX}{}/", segment_id.uuid_string())
}

pub(super) fn segment_tombstone_path(segment_id: &SegmentId) -> String {
    format!("{FTS2_TOMB_PREFIX}{}", segment_id.uuid_string())
}

pub(super) fn parse_segment_id(hex: &str) -> Result<SegmentId> {
    SegmentId::from_uuid_string(hex)
        .map_err(|_| LimboError::Corrupt(format!("FTS row carries a malformed segment id: {hex}")))
}

fn fts2_checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
    })
}

fn take<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| LimboError::Corrupt("FTS record offset overflow".into()))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| LimboError::Corrupt("truncated FTS record".into()))?;
    *offset = end;
    Ok(value.try_into().expect("slice length checked"))
}

pub(super) fn append_checksum(mut bytes: Vec<u8>) -> Vec<u8> {
    let checksum = fts2_checksum(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes
}

fn verify_checksum<'a>(bytes: &'a [u8], what: &str) -> Result<&'a [u8]> {
    if bytes.len() < 8 {
        return Err(LimboError::Corrupt(format!("truncated FTS {what} record")));
    }
    let payload_len = bytes.len() - 8;
    let expected = u64::from_le_bytes(bytes[payload_len..].try_into().expect("length checked"));
    if fts2_checksum(&bytes[..payload_len]) != expected {
        return Err(LimboError::Corrupt(format!(
            "FTS {what} record checksum mismatch"
        )));
    }
    Ok(&bytes[..payload_len])
}

/// Hash of the Tantivy schema an index was built with. The schema JSON
/// covers field names, order, types, and tokenizer configuration — exactly
/// what must match between the stored postings and the reader; query-time
/// boosts are not part of it and may change freely.
pub(super) fn schema_content_hash(schema: &Schema) -> u64 {
    let json = serde_json::to_vec(schema).expect("tantivy schema serializes to JSON");
    fts2_checksum(&json)
}

/// Rare index-level facts. Written once when the index is created and
/// never rewritten; its presence marks a registry-format store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FtsControlV2 {
    pub format_version: u32,
    /// Distinguishes drop/recreate lifetimes of the same index name.
    pub index_incarnation: u64,
    /// [`schema_content_hash`] of the schema the index was built with, or 0
    /// for stores written before this field existed.
    pub schema_hash: u64,
}

impl FtsControlV2 {
    pub fn new(index_incarnation: u64, schema_hash: u64) -> Self {
        Self {
            format_version: FTS_STORAGE_FORMAT_V2,
            index_incarnation,
            schema_hash,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 4 + 8 + 8 + 8);
        bytes.extend_from_slice(FTS2_CONTROL_MAGIC);
        bytes.extend_from_slice(&self.format_version.to_le_bytes());
        bytes.extend_from_slice(&self.index_incarnation.to_le_bytes());
        bytes.extend_from_slice(&self.schema_hash.to_le_bytes());
        append_checksum(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload = verify_checksum(bytes, "control")?;
        let mut offset = 0;
        if take::<8>(payload, &mut offset)? != *FTS2_CONTROL_MAGIC {
            return Err(LimboError::Corrupt(
                "unrecognized FTS control record".into(),
            ));
        }
        let format_version = u32::from_le_bytes(take(payload, &mut offset)?);
        if format_version != FTS_STORAGE_FORMAT_V2 {
            return Err(LimboError::Corrupt(format!(
                "unsupported FTS storage format version {format_version}"
            )));
        }
        let index_incarnation = u64::from_le_bytes(take(payload, &mut offset)?);
        // Stores written before the schema hash existed end here; 0 means
        // "unknown" and is never checked against.
        let schema_hash = if offset < payload.len() {
            u64::from_le_bytes(take(payload, &mut offset)?)
        } else {
            0
        };
        if offset != payload.len() {
            return Err(LimboError::Corrupt(
                "FTS control record has trailing payload bytes".into(),
            ));
        }
        Ok(Self {
            format_version,
            index_incarnation,
            schema_hash,
        })
    }
}

/// One file of an immutable segment, as recorded in its descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SegmentFileEntry {
    /// Tantivy file name, e.g. `<uuid>.term`.
    pub name: String,
    pub size: u64,
    pub num_chunks: u32,
}

/// A segment's registry entry. Inserting this row *is* publishing the
/// segment; deleting it (merge) retires the segment. The segment id lives
/// in the row path, not the blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub max_doc: u32,
    pub files: Vec<SegmentFileEntry>,
}

impl SegmentDescriptor {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(FTS2_SEGMENT_MAGIC);
        bytes.extend_from_slice(&self.max_doc.to_le_bytes());
        let file_count = u32::try_from(self.files.len()).map_err(|_| {
            LimboError::InternalError("FTS segment descriptor has too many files".into())
        })?;
        bytes.extend_from_slice(&file_count.to_le_bytes());
        for file in &self.files {
            let name_len = u32::try_from(file.name.len()).map_err(|_| {
                LimboError::InternalError("FTS segment file name is too long".into())
            })?;
            bytes.extend_from_slice(&name_len.to_le_bytes());
            bytes.extend_from_slice(file.name.as_bytes());
            bytes.extend_from_slice(&file.size.to_le_bytes());
            bytes.extend_from_slice(&file.num_chunks.to_le_bytes());
        }
        Ok(append_checksum(bytes))
    }

    pub fn decode(segment_id: SegmentId, bytes: &[u8]) -> Result<Self> {
        let payload = verify_checksum(bytes, "segment descriptor")?;
        let mut offset = 0;
        if take::<8>(payload, &mut offset)? != *FTS2_SEGMENT_MAGIC {
            return Err(LimboError::Corrupt(
                "unrecognized FTS segment descriptor".into(),
            ));
        }
        let max_doc = u32::from_le_bytes(take(payload, &mut offset)?);
        let file_count = u32::from_le_bytes(take(payload, &mut offset)?) as usize;
        let mut files = Vec::with_capacity(file_count.min(64));
        for _ in 0..file_count {
            let name_len = u32::from_le_bytes(take(payload, &mut offset)?) as usize;
            let name_end = offset
                .checked_add(name_len)
                .ok_or_else(|| LimboError::Corrupt("FTS file name offset overflow".into()))?;
            let name_bytes = payload
                .get(offset..name_end)
                .ok_or_else(|| LimboError::Corrupt("truncated FTS segment file name".into()))?;
            offset = name_end;
            let name = std::str::from_utf8(name_bytes)
                .map_err(|_| LimboError::Corrupt("FTS segment file name is not UTF-8".into()))?
                .to_string();
            let size = u64::from_le_bytes(take(payload, &mut offset)?);
            let num_chunks = u32::from_le_bytes(take(payload, &mut offset)?);
            if num_chunks == 0 {
                return Err(LimboError::Corrupt(format!(
                    "FTS segment file {name} has zero chunks"
                )));
            }
            files.push(SegmentFileEntry {
                name,
                size,
                num_chunks,
            });
        }
        if offset != payload.len() {
            return Err(LimboError::Corrupt(
                "FTS segment descriptor has trailing payload bytes".into(),
            ));
        }
        Ok(Self {
            segment_id,
            max_doc,
            files,
        })
    }
}

/// The resident bytes of one immutable segment: file name → contents.
/// Shared across connections keyed by segment id — a segment's bytes never
/// change, so the cache needs no snapshot identity.
#[derive(Debug)]
pub(super) struct SegmentData {
    pub files: HashMap<String, Arc<[u8]>>,
    pub total_bytes: usize,
}

impl SegmentData {
    pub fn new(files: HashMap<String, Arc<[u8]>>) -> Self {
        let total_bytes = files.values().map(|data| data.len()).sum();
        Self { files, total_bytes }
    }
}

/// One segment as seen by a cursor's snapshot: immutable bytes plus the
/// tombstoned doc ids visible at (or created by) this transaction.
#[derive(Debug, Clone)]
pub(super) struct LoadedSegment {
    pub descriptor: SegmentDescriptor,
    pub data: Arc<SegmentData>,
    /// Doc ids whose postings are dead at this snapshot. Ordered so cache
    /// identity comparisons and bitset builds are deterministic.
    pub deleted: BTreeSet<u32>,
}

impl LoadedSegment {
    pub fn new(
        descriptor: SegmentDescriptor,
        data: Arc<SegmentData>,
        deleted: BTreeSet<u32>,
    ) -> Self {
        Self {
            descriptor,
            data,
            deleted,
        }
    }

    pub fn id(&self) -> SegmentId {
        self.descriptor.segment_id
    }

    pub fn live_docs(&self) -> u64 {
        u64::from(self.descriptor.max_doc).saturating_sub(self.deleted.len() as u64)
    }
}

/// Serialize an alive bitset in Tantivy's `.del` format:
/// `[u32 max_value LE][ceil(max_value/64) x u64 words LE]`, bit set = alive.
pub(super) fn alive_bitset_bytes(max_doc: u32, deleted: &BTreeSet<u32>) -> Vec<u8> {
    let words = (max_doc as usize).div_ceil(64);
    let mut bytes = Vec::with_capacity(4 + words * 8);
    bytes.extend_from_slice(&max_doc.to_le_bytes());
    let mut word_buf = vec![u64::MAX; words];
    // Clear bits at or beyond max_doc in the last word so num_alive_docs is
    // exact; every earlier word is fully below max_doc.
    let tail_bits = max_doc % 64;
    if tail_bits != 0 {
        if let Some(last) = word_buf.last_mut() {
            *last = (1u64 << tail_bits) - 1;
        }
    }
    for doc in deleted {
        if *doc < max_doc {
            word_buf[(*doc / 64) as usize] &= !(1u64 << (*doc % 64));
        }
    }
    for word in word_buf {
        bytes.extend_from_slice(&word.to_le_bytes());
    }
    bytes
}

#[cfg(test)]
pub(super) fn alive_bitset(
    max_doc: u32,
    deleted: &BTreeSet<u32>,
) -> tantivy::fastfield::AliveBitSet {
    tantivy::fastfield::AliveBitSet::open(tantivy::directory::OwnedBytes::new(alive_bitset_bytes(
        max_doc, deleted,
    )))
}

/// Build the `meta.json` bytes for a snapshot's visible segment set.
///
/// `meta.json` survives as an interface, not a file: Tantivy's `Index::open`
/// wants to deserialize an `IndexMeta`, so we serialize one built from the
/// registry rows. Segments with tombstones get a delete meta pointing at a
/// synthesized `.del` file the snapshot directory serves from the same
/// tombstone set (see `directory`), which makes every query path — including
/// `TopDocs` — honor tombstones at the `SegmentReader` level.
///
/// `scratch` is any index with no meaning of its own; it only mints
/// `SegmentMeta` values (their serialized form is independent of the index
/// they were minted from). `IndexSettings` must be identical across every
/// transaction or a mixed searcher could not read old segments; we pin the
/// default everywhere.
pub(super) fn synthesize_meta_json(
    scratch: &Index,
    schema: &Schema,
    segments: &[LoadedSegment],
) -> Result<Vec<u8>> {
    let metas = segments
        .iter()
        .map(|segment| {
            let meta = scratch.new_segment_meta(segment.id(), segment.descriptor.max_doc);
            if segment.deleted.is_empty() {
                meta
            } else {
                meta.with_delete_meta(segment.deleted.len() as u32, FTS2_TOMBSTONE_DELETE_OPSTAMP)
            }
        })
        .collect();
    let meta = IndexMeta {
        index_settings: IndexSettings::default(),
        segments: metas,
        schema: schema.clone(),
        opstamp: 0,
        payload: None,
    };
    serde_json::to_vec(&meta)
        .map_err(|e| LimboError::InternalError(format!("FTS meta synthesis failed: {e}")))
}

/// The synthesized `.del` file name for a segment
/// (`<uuid>.<FTS2_TOMBSTONE_DELETE_OPSTAMP>.del`).
pub(super) fn tombstone_del_file_name(segment_id: &SegmentId) -> String {
    format!(
        "{}.{}.del",
        segment_id.uuid_string(),
        FTS2_TOMBSTONE_DELETE_OPSTAMP
    )
}

/// Append Tantivy's per-file footer (`<json {version, crc}> <u32 len>
/// <u32 magic>`) to synthesized file bytes.
///
/// Every file served through a Tantivy index is read via `ManagedDirectory`,
/// which validates and strips this footer. Segment files captured from a
/// build already carry one; only files we synthesize ourselves (the `.del`
/// bytes derived from tombstone rows) need it added.
const FOOTER_MAGIC_NUMBER: u32 = 1337;

pub(super) fn with_tantivy_footer(mut body: Vec<u8>) -> Result<Vec<u8>> {
    let crc = crc32fast::hash(&body);
    let footer = serde_json::json!({ "version": tantivy::version(), "crc": crc });
    let payload = serde_json::to_vec(&footer)
        .map_err(|e| LimboError::InternalError(format!("FTS footer synthesis failed: {e}")))?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| LimboError::InternalError("FTS footer payload is too long".into()))?;
    body.extend_from_slice(&payload);
    body.extend_from_slice(&payload_len.to_le_bytes());
    body.extend_from_slice(&FOOTER_MAGIC_NUMBER.to_le_bytes());
    Ok(body)
}

/// Check the Tantivy per-file footer on an assembled segment file: the stored
/// CRC must match a fresh crc32 of the file body. Chunk rows carry no checksum
/// of their own, so this is the only guard that catches a same-length bit flip
/// in segment bytes before they reach Tantivy's decoders.
pub(super) fn verify_tantivy_footer(name: &str, bytes: &[u8]) -> Result<()> {
    let corrupt =
        |detail: &str| LimboError::Corrupt(format!("FTS segment file {name} has {detail}"));
    let Some(footer_start) = bytes.len().checked_sub(8) else {
        return Err(corrupt("no room for a footer"));
    };
    let magic = u32::from_le_bytes(bytes[footer_start + 4..].try_into().expect("4 bytes"));
    if magic != FOOTER_MAGIC_NUMBER {
        return Err(corrupt("a bad footer magic number"));
    }
    let payload_len = u32::from_le_bytes(
        bytes[footer_start..footer_start + 4]
            .try_into()
            .expect("4 bytes"),
    ) as usize;
    let Some(body_len) = footer_start.checked_sub(payload_len) else {
        return Err(corrupt("a footer longer than the file"));
    };
    let footer: serde_json::Value = serde_json::from_slice(&bytes[body_len..footer_start])
        .map_err(|_| corrupt("an unparseable footer"))?;
    let stored_crc = footer
        .get("crc")
        .and_then(serde_json::Value::as_u64)
        .and_then(|crc| u32::try_from(crc).ok())
        .ok_or_else(|| corrupt("a footer without a crc"))?;
    let actual_crc = crc32fast::hash(&bytes[..body_len]);
    if stored_crc != actual_crc {
        return Err(corrupt("bytes that do not match its footer crc"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_record_round_trips_and_detects_corruption() {
        let control = FtsControlV2::new(0xdead_beef, 0x5c4e_a4a5);
        let bytes = control.encode();
        assert_eq!(FtsControlV2::decode(&bytes).unwrap(), control);

        assert!(FtsControlV2::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut corrupted = bytes;
        corrupted[9] ^= 0xff;
        assert!(FtsControlV2::decode(&corrupted).is_err());
    }

    #[test]
    fn control_record_without_schema_hash_decodes_as_unknown() {
        // A store written before the schema-hash field: magic + version +
        // incarnation only, checksummed.
        let mut payload = Vec::new();
        payload.extend_from_slice(FTS2_CONTROL_MAGIC);
        payload.extend_from_slice(&FTS_STORAGE_FORMAT_V2.to_le_bytes());
        payload.extend_from_slice(&0xdead_beef_u64.to_le_bytes());
        let decoded = FtsControlV2::decode(&append_checksum(payload)).unwrap();
        assert_eq!(decoded.index_incarnation, 0xdead_beef);
        assert_eq!(decoded.schema_hash, 0, "missing field reads as unknown");
    }

    #[test]
    fn segment_descriptor_round_trips() {
        let segment_id = SegmentId::generate_random();
        let descriptor = SegmentDescriptor {
            segment_id,
            max_doc: 42,
            files: vec![
                SegmentFileEntry {
                    name: format!("{}.term", segment_id.uuid_string()),
                    size: 1234,
                    num_chunks: 1,
                },
                SegmentFileEntry {
                    name: format!("{}.store", segment_id.uuid_string()),
                    size: 5 * 1024 * 1024,
                    num_chunks: 10,
                },
            ],
        };
        let bytes = descriptor.encode().unwrap();
        assert_eq!(
            SegmentDescriptor::decode(segment_id, &bytes).unwrap(),
            descriptor
        );

        let mut corrupted = bytes;
        *corrupted.last_mut().unwrap() ^= 0x01;
        assert!(SegmentDescriptor::decode(segment_id, &corrupted).is_err());
    }

    #[test]
    fn alive_bitset_marks_exactly_the_tombstoned_docs() {
        let deleted = BTreeSet::from([0u32, 3, 64, 129]);
        let bitset = alive_bitset(130, &deleted);
        assert_eq!(bitset.num_alive_docs(), 130 - deleted.len());
        for doc in 0..130 {
            assert_eq!(
                bitset.is_deleted(doc),
                deleted.contains(&doc),
                "doc {doc} has the wrong liveness"
            );
        }
    }

    #[test]
    fn alive_bitset_with_no_tombstones_keeps_every_doc() {
        let bitset = alive_bitset(65, &BTreeSet::new());
        assert_eq!(bitset.num_alive_docs(), 65);
        assert!(!bitset.is_deleted(64));
    }
}
