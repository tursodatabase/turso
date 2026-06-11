use crate::types::{ImmutableRecord, Value};
use crate::{LimboError, Numeric, Result};
use std::collections::{HashMap, HashSet};

const TX_FIELD_STRING_TABLE: u64 = 12;
const TX_FIELD_OBJECT_MAP: u64 = 13;
const TX_FIELD_META: u64 = 14;

const OBJECT_FIELD_MV_TABLE_ID: u64 = 1;
const OBJECT_FIELD_NAME_REF: u64 = 2;

const META_FIELD_KEY_REF: u64 = 1;
const META_FIELD_VALUE_REF: u64 = 2;

const SQLITE_INTERNAL_PREFIX: &str = "sqlite_";
const TURSO_INTERNAL_PREFIX: &str = "__turso_internal_";
const TURSO_SYNC_PREFIX: &str = "turso_sync_";
const TURSO_CDC_TABLE_NAME: &str = "turso_cdc";
const TURSO_CDC_VERSION_TABLE_NAME: &str = "turso_cdc_version";

// Minimal protobuf-style encoder for core-owned portable MVCC logical changes.
// Keeping this local avoids making turso_core depend on the sync engine or
// server proto crate. Raw-log consumers can decode this envelope into their own
// replay representation.
fn write_proto_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn write_proto_key(field: u64, wire_type: u64, out: &mut Vec<u8>) {
    write_proto_varint((field << 3) | wire_type, out);
}

fn write_proto_uint64(field: u64, value: u64, out: &mut Vec<u8>) {
    write_proto_key(field, 0, out);
    write_proto_varint(value, out);
}

fn write_proto_sint64(field: u64, value: i64, out: &mut Vec<u8>) {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    write_proto_uint64(field, zigzag, out);
}

fn write_proto_bytes(field: u64, value: &[u8], out: &mut Vec<u8>) {
    write_proto_key(field, 2, out);
    write_proto_varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

fn write_proto_string(field: u64, value: &str, out: &mut Vec<u8>) {
    write_proto_bytes(field, value.as_bytes(), out);
}

pub(crate) fn is_portable_logical_name(name: &str) -> bool {
    !name.starts_with(SQLITE_INTERNAL_PREFIX)
        && !name.starts_with(TURSO_INTERNAL_PREFIX)
        && !name.starts_with(TURSO_SYNC_PREFIX)
        && name != TURSO_CDC_TABLE_NAME
        && name != TURSO_CDC_VERSION_TABLE_NAME
}

/// Decoded subset of a `sqlite_schema` record needed to materialize stable
/// portable object-map entries at MVCC commit time.
#[derive(Clone, Debug)]
pub(crate) struct PortableSchemaRow {
    pub(crate) row_type: String,
    pub(crate) name: String,
    pub(crate) rootpage: i64,
}

pub(crate) fn portable_schema_row_from_record(record_bytes: &[u8]) -> Result<PortableSchemaRow> {
    let values = ImmutableRecord::from_bin_record(record_bytes.to_vec()).get_values_owned()?;
    if values.len() < 5 {
        return Err(LimboError::InternalError(format!(
            "sqlite_schema record must have at least 5 columns, got {}",
            values.len()
        )));
    }
    let text = |value: &Value, field: &str| -> Result<String> {
        match value {
            Value::Text(text) => Ok(text.as_str().to_string()),
            other => Err(LimboError::InternalError(format!(
                "{field} must be text in sqlite_schema record, got {other:?}"
            ))),
        }
    };
    let integer = |value: &Value, field: &str| -> Result<i64> {
        match value {
            Value::Numeric(Numeric::Integer(value)) => Ok(*value),
            other => Err(LimboError::InternalError(format!(
                "{field} must be integer in sqlite_schema record, got {other:?}"
            ))),
        }
    };
    Ok(PortableSchemaRow {
        row_type: text(&values[0], "sqlite_schema.type")?,
        name: text(&values[1], "sqlite_schema.name")?,
        rootpage: integer(&values[3], "sqlite_schema.rootpage")?,
    })
}

pub(crate) fn is_portable_table_schema_row(row: &PortableSchemaRow) -> bool {
    row.rootpage != 0 && row.row_type.eq_ignore_ascii_case("table")
}

#[derive(Default)]
pub(crate) struct PortableLogicalBuilder {
    strings: Vec<String>,
    string_refs: HashMap<String, u64>,
    object_maps: Vec<Vec<u8>>,
    object_map_ids: HashSet<i64>,
    metadata: Vec<Vec<u8>>,
}

pub(crate) struct PortableObjectMapEntry<'a> {
    pub(crate) mv_table_id: i64,
    pub(crate) name: &'a str,
}

impl PortableLogicalBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn intern_string(&mut self, value: &str) -> u64 {
        if let Some(idx) = self.string_refs.get(value) {
            return *idx;
        }
        let idx = self.strings.len() as u64;
        self.strings.push(value.to_string());
        self.string_refs.insert(value.to_string(), idx);
        idx
    }

    pub(crate) fn add_object_map(&mut self, entry: PortableObjectMapEntry<'_>) -> bool {
        if !is_portable_logical_name(entry.name) || !self.object_map_ids.insert(entry.mv_table_id) {
            return false;
        }
        let name_ref = self.intern_string(entry.name);

        let mut object = Vec::new();
        write_proto_sint64(OBJECT_FIELD_MV_TABLE_ID, entry.mv_table_id, &mut object);
        write_proto_uint64(OBJECT_FIELD_NAME_REF, name_ref, &mut object);
        self.object_maps.push(object);
        true
    }

    pub(crate) fn add_metadata(&mut self, key: &str, value: &str) {
        let key_ref = self.intern_string(key);
        let value_ref = self.intern_string(value);
        let mut meta = Vec::new();
        write_proto_uint64(META_FIELD_KEY_REF, key_ref, &mut meta);
        write_proto_uint64(META_FIELD_VALUE_REF, value_ref, &mut meta);
        self.metadata.push(meta);
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        let mut txn_fields = Vec::new();
        for value in &self.strings {
            write_proto_string(TX_FIELD_STRING_TABLE, value, &mut txn_fields);
        }
        for object_map in self.object_maps {
            write_proto_key(TX_FIELD_OBJECT_MAP, 2, &mut txn_fields);
            write_proto_varint(object_map.len() as u64, &mut txn_fields);
            txn_fields.extend_from_slice(&object_map);
        }
        for meta in self.metadata {
            write_proto_key(TX_FIELD_META, 2, &mut txn_fields);
            write_proto_varint(meta.len() as u64, &mut txn_fields);
            txn_fields.extend_from_slice(&meta);
        }
        txn_fields
    }
}
