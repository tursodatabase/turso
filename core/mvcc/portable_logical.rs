use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::types::{ImmutableRecord, Value};
use crate::{LimboError, Numeric, Result};
use std::collections::{HashMap, HashSet};

pub(crate) const LOGICAL_OP_UPSERT_ROW: u64 = 0;
pub(crate) const LOGICAL_OP_DELETE_ROW: u64 = 1;
const LOGICAL_OP_SCHEMA: u64 = 2;
const LOGICAL_OP_UPDATE_HEADER: u64 = 3;

pub(crate) const LOGICAL_SCHEMA_CREATE: u64 = 0;
pub(crate) const LOGICAL_SCHEMA_DROP: u64 = 1;
pub(crate) const LOGICAL_SCHEMA_REFRESH: u64 = 2;

const LOGICAL_SCHEMA_KIND_TABLE: u64 = 0;
const LOGICAL_SCHEMA_KIND_INDEX: u64 = 1;
const LOGICAL_SCHEMA_KIND_TRIGGER: u64 = 2;
const LOGICAL_SCHEMA_KIND_VIEW: u64 = 3;

const TX_FIELD_OP: u64 = 3;
const TX_FIELD_ORIGIN_CLIENT_ID: u64 = 4;
const TX_FIELD_STRING_TABLE: u64 = 12;
const TX_FIELD_OBJECT_MAP: u64 = 13;

const OBJECT_FIELD_MV_TABLE_ID: u64 = 1;
const OBJECT_FIELD_LOGICAL_OBJECT_ID: u64 = 2;
const OBJECT_FIELD_KIND: u64 = 3;
const OBJECT_FIELD_NAME_REF: u64 = 4;
const OBJECT_FIELD_SCHEMA_EPOCH: u64 = 5;
const OBJECT_FIELD_CREATE_SQL_REF: u64 = 6;
const OBJECT_FIELD_ROOTPAGE_DEBUG: u64 = 7;

const OP_FIELD_TYPE: u64 = 1;
const OP_FIELD_ROWID: u64 = 3;
const OP_FIELD_RECORD: u64 = 4;
const OP_FIELD_USER_VERSION: u64 = 6;
const OP_FIELD_APPLICATION_ID: u64 = 7;
const OP_FIELD_SCHEMA_ACTION: u64 = 8;
const OP_FIELD_SCHEMA_KIND: u64 = 9;
const OP_FIELD_STABLE_TABLE_ID: u64 = 11;
const OP_FIELD_MV_TABLE_ID: u64 = 12;
const OP_FIELD_SQL_REF: u64 = 13;
const OP_FIELD_SCHEMA_NAME_REF: u64 = 14;

const SQLITE_INTERNAL_PREFIX: &str = "sqlite_";
const TURSO_INTERNAL_PREFIX: &str = "__turso_internal_";
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

fn append_logical_op(op_body: Vec<u8>, out: &mut Vec<u8>) {
    write_proto_key(TX_FIELD_OP, 2, out);
    write_proto_varint(op_body.len() as u64, out);
    out.extend_from_slice(&op_body);
}

pub(crate) fn is_portable_logical_name(name: &str) -> bool {
    !name.starts_with(SQLITE_INTERNAL_PREFIX)
        && !name.starts_with(TURSO_INTERNAL_PREFIX)
        && name != TURSO_CDC_TABLE_NAME
        && name != TURSO_CDC_VERSION_TABLE_NAME
}

/// Decoded subset of a `sqlite_schema` record needed to materialize stable
/// portable schema operations at MVCC commit time.
#[derive(Clone, Debug)]
pub(crate) struct PortableSchemaRow {
    pub(crate) row_type: String,
    pub(crate) name: String,
    pub(crate) rootpage: i64,
    pub(crate) sql: Option<String>,
}

pub(crate) fn portable_schema_row_from_record(record_bytes: &[u8]) -> Result<PortableSchemaRow> {
    let values = ImmutableRecord::from_bin_record(record_bytes.to_vec()).get_values_owned()?;
    if values.len() < 5 {
        return Err(LimboError::Corrupt(format!(
            "sqlite_schema record must have at least 5 columns, got {}",
            values.len()
        )));
    }
    let text = |value: &Value, field: &str| -> Result<String> {
        match value {
            Value::Text(text) => Ok(text.as_str().to_string()),
            other => Err(LimboError::Corrupt(format!(
                "{field} must be text in sqlite_schema record, got {other:?}"
            ))),
        }
    };
    let optional_text = |value: &Value, field: &str| -> Result<Option<String>> {
        match value {
            Value::Text(text) => Ok(Some(text.as_str().to_string())),
            Value::Null => Ok(None),
            other => Err(LimboError::Corrupt(format!(
                "{field} must be text or null in sqlite_schema record, got {other:?}"
            ))),
        }
    };
    let integer = |value: &Value, field: &str| -> Result<i64> {
        match value {
            Value::Numeric(Numeric::Integer(value)) => Ok(*value),
            other => Err(LimboError::Corrupt(format!(
                "{field} must be integer in sqlite_schema record, got {other:?}"
            ))),
        }
    };
    Ok(PortableSchemaRow {
        row_type: text(&values[0], "sqlite_schema.type")?,
        name: text(&values[1], "sqlite_schema.name")?,
        rootpage: integer(&values[3], "sqlite_schema.rootpage")?,
        sql: optional_text(&values[4], "sqlite_schema.sql")?,
    })
}

pub(crate) fn portable_schema_kind(row_type: &str) -> Option<u64> {
    if row_type.eq_ignore_ascii_case("table") {
        Some(LOGICAL_SCHEMA_KIND_TABLE)
    } else if row_type.eq_ignore_ascii_case("index") {
        Some(LOGICAL_SCHEMA_KIND_INDEX)
    } else if row_type.eq_ignore_ascii_case("trigger") {
        Some(LOGICAL_SCHEMA_KIND_TRIGGER)
    } else if row_type.eq_ignore_ascii_case("view") {
        Some(LOGICAL_SCHEMA_KIND_VIEW)
    } else {
        None
    }
}

pub(crate) fn stable_table_id_from_rootpage(rootpage: i64) -> Option<u64> {
    (rootpage != 0).then_some(rootpage.unsigned_abs())
}

#[derive(Default)]
pub(crate) struct PortableLogicalBuilder {
    strings: Vec<String>,
    string_refs: HashMap<String, u64>,
    object_maps: Vec<Vec<u8>>,
    object_map_ids: HashSet<i64>,
    ops: Vec<u8>,
}

pub(crate) struct PortableObjectMapEntry<'a> {
    pub(crate) mv_table_id: i64,
    pub(crate) kind: u64,
    pub(crate) logical_object_id: u64,
    pub(crate) name: &'a str,
    pub(crate) schema_epoch: u64,
    pub(crate) create_sql: Option<&'a str>,
    pub(crate) rootpage_debug: i64,
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
        let sql_ref = entry.create_sql.map(|sql| self.intern_string(sql));

        let mut object = Vec::new();
        write_proto_sint64(OBJECT_FIELD_MV_TABLE_ID, entry.mv_table_id, &mut object);
        write_proto_uint64(
            OBJECT_FIELD_LOGICAL_OBJECT_ID,
            entry.logical_object_id,
            &mut object,
        );
        write_proto_uint64(OBJECT_FIELD_KIND, entry.kind, &mut object);
        write_proto_uint64(OBJECT_FIELD_NAME_REF, name_ref, &mut object);
        write_proto_uint64(OBJECT_FIELD_SCHEMA_EPOCH, entry.schema_epoch, &mut object);
        if let Some(sql_ref) = sql_ref {
            write_proto_uint64(OBJECT_FIELD_CREATE_SQL_REF, sql_ref, &mut object);
        }
        write_proto_sint64(
            OBJECT_FIELD_ROOTPAGE_DEBUG,
            entry.rootpage_debug,
            &mut object,
        );
        self.object_maps.push(object);
        true
    }

    pub(crate) fn encode_schema_logical_op(
        &mut self,
        action: u64,
        row: &PortableSchemaRow,
        stable_table_id: Option<u64>,
    ) -> bool {
        let Some(kind) = portable_schema_kind(&row.row_type) else {
            return false;
        };
        if !is_portable_logical_name(&row.name) {
            return false;
        }
        let name_ref = self.intern_string(&row.name);
        let sql_ref = if matches!(action, LOGICAL_SCHEMA_CREATE | LOGICAL_SCHEMA_REFRESH) {
            row.sql.as_deref().map(|sql| self.intern_string(sql))
        } else {
            None
        };

        let mut op = Vec::new();
        write_proto_uint64(OP_FIELD_TYPE, LOGICAL_OP_SCHEMA, &mut op);
        write_proto_uint64(OP_FIELD_SCHEMA_ACTION, action, &mut op);
        write_proto_uint64(OP_FIELD_SCHEMA_KIND, kind, &mut op);
        if let Some(stable_table_id) = stable_table_id {
            write_proto_uint64(OP_FIELD_STABLE_TABLE_ID, stable_table_id, &mut op);
        }
        if let Some(sql_ref) = sql_ref {
            write_proto_uint64(OP_FIELD_SQL_REF, sql_ref, &mut op);
        }
        write_proto_uint64(OP_FIELD_SCHEMA_NAME_REF, name_ref, &mut op);
        append_logical_op(op, &mut self.ops);
        true
    }

    pub(crate) fn encode_row_logical_op(
        &mut self,
        op_type: u64,
        mv_table_id: i64,
        rowid: i64,
        record: &[u8],
    ) {
        let mut op = Vec::new();
        write_proto_uint64(OP_FIELD_TYPE, op_type, &mut op);
        write_proto_sint64(OP_FIELD_ROWID, rowid, &mut op);
        if !record.is_empty() {
            write_proto_bytes(OP_FIELD_RECORD, record, &mut op);
        }
        write_proto_sint64(OP_FIELD_MV_TABLE_ID, mv_table_id, &mut op);
        append_logical_op(op, &mut self.ops);
    }

    pub(crate) fn encode_header_logical_op(&mut self, header: &DatabaseHeader) {
        let mut op = Vec::new();
        write_proto_uint64(OP_FIELD_TYPE, LOGICAL_OP_UPDATE_HEADER, &mut op);
        write_proto_uint64(
            OP_FIELD_USER_VERSION,
            header.user_version.get() as u64,
            &mut op,
        );
        write_proto_uint64(
            OP_FIELD_APPLICATION_ID,
            header.application_id.get() as u64,
            &mut op,
        );
        append_logical_op(op, &mut self.ops);
    }

    pub(crate) fn finish(self, origin_client_id: Option<String>) -> Vec<u8> {
        let mut txn_fields = Vec::new();
        if let Some(client_id) = origin_client_id {
            write_proto_string(TX_FIELD_ORIGIN_CLIENT_ID, &client_id, &mut txn_fields);
        }
        for value in &self.strings {
            write_proto_string(TX_FIELD_STRING_TABLE, value, &mut txn_fields);
        }
        for object_map in self.object_maps {
            write_proto_key(TX_FIELD_OBJECT_MAP, 2, &mut txn_fields);
            write_proto_varint(object_map.len() as u64, &mut txn_fields);
            txn_fields.extend_from_slice(&object_map);
        }
        txn_fields.extend_from_slice(&self.ops);
        txn_fields
    }
}
