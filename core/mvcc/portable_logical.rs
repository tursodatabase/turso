use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::types::{ImmutableRecord, Value};
use crate::{LimboError, Numeric, Result};

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

pub(crate) fn prepend_origin_client_id(client_id: &str, encoded_ops: Vec<u8>) -> Vec<u8> {
    let mut txn_fields = Vec::new();
    // PortableLogicalTxn.origin_client_id, field 4.
    write_proto_string(4, client_id, &mut txn_fields);
    txn_fields.extend_from_slice(&encoded_ops);
    txn_fields
}

fn append_logical_op(op_body: Vec<u8>, out: &mut Vec<u8>) {
    write_proto_key(3, 2, out);
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

fn portable_schema_kind(row_type: &str) -> Option<u64> {
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

pub(crate) fn encode_schema_logical_op(
    action: u64,
    row: &PortableSchemaRow,
    stable_table_id: Option<u64>,
    out: &mut Vec<u8>,
) -> bool {
    let Some(kind) = portable_schema_kind(&row.row_type) else {
        return false;
    };
    if !is_portable_logical_name(&row.name) {
        return false;
    }
    let mut op = Vec::new();
    write_proto_uint64(1, LOGICAL_OP_SCHEMA, &mut op);
    if matches!(action, LOGICAL_SCHEMA_CREATE | LOGICAL_SCHEMA_REFRESH) {
        if let Some(sql) = row.sql.as_deref() {
            write_proto_string(5, sql, &mut op);
        }
    }
    write_proto_uint64(8, action, &mut op);
    write_proto_uint64(9, kind, &mut op);
    write_proto_string(10, &row.name, &mut op);
    if let Some(stable_table_id) = stable_table_id {
        write_proto_uint64(11, stable_table_id, &mut op);
    }
    append_logical_op(op, out);
    true
}

pub(crate) fn encode_row_logical_op(
    op_type: u64,
    table_name: &str,
    include_table_name: bool,
    stable_table_id: u64,
    rowid: i64,
    record: &[u8],
    out: &mut Vec<u8>,
) {
    if !is_portable_logical_name(table_name) {
        return;
    }
    let mut op = Vec::new();
    write_proto_uint64(1, op_type, &mut op);
    if include_table_name {
        write_proto_string(2, table_name, &mut op);
    }
    write_proto_sint64(3, rowid, &mut op);
    write_proto_uint64(11, stable_table_id, &mut op);
    if op_type == LOGICAL_OP_UPSERT_ROW {
        write_proto_bytes(4, record, &mut op);
    }
    append_logical_op(op, out);
}

pub(crate) fn encode_header_logical_op(header: &DatabaseHeader, out: &mut Vec<u8>) {
    let mut op = Vec::new();
    write_proto_uint64(1, LOGICAL_OP_UPDATE_HEADER, &mut op);
    write_proto_uint64(6, header.user_version.get() as u64, &mut op);
    write_proto_uint64(7, header.application_id.get() as u64, &mut op);
    append_logical_op(op, out);
}
