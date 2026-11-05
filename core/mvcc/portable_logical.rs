use crate::alloc::{TursoVecExt, Vec};
use crate::cdc::{TURSO_CDC_DEFAULT_TABLE_NAME, TURSO_CDC_VERSION_TABLE_NAME};
use crate::mvcc::persistent_storage::logical_log::{
    log_write, LogSerializer, ProtoKey, ProtoSint64, ProtoVarint, PROTO_WIRE_LENGTH_DELIMITED,
    PROTO_WIRE_VARINT,
};
use crate::schema::{SchemaRecord, SchemaRowType};
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

pub(crate) fn is_portable_logical_name(name: &str) -> bool {
    !name.starts_with(SQLITE_INTERNAL_PREFIX)
        && !name.starts_with(TURSO_INTERNAL_PREFIX)
        && !name.starts_with(TURSO_SYNC_PREFIX)
        && name != TURSO_CDC_DEFAULT_TABLE_NAME
        && name != TURSO_CDC_VERSION_TABLE_NAME
}

pub(crate) fn is_portable_table_schema_row(row: &SchemaRecord) -> bool {
    row.root_page != 0 && row.row_type == SchemaRowType::Table && is_portable_schema_row(row)
}

pub(crate) fn is_portable_schema_row(row: &SchemaRecord) -> bool {
    is_portable_logical_name(&row.name) && row.row_type != SchemaRowType::Other
}

pub(crate) struct PortableLogicalBuilder<'a> {
    strings: Vec<&'a str>,
    string_refs: HashMap<&'a str, u64>,
    object_maps: Vec<Vec<u8>>,
    object_map_ids: HashSet<i64>,
    metadata: Vec<Vec<u8>>,
}

impl Default for PortableLogicalBuilder<'_> {
    fn default() -> Self {
        Self {
            strings: crate::alloc::vec![],
            string_refs: HashMap::default(),
            object_maps: crate::alloc::vec![],
            object_map_ids: HashSet::default(),
            metadata: crate::alloc::vec![],
        }
    }
}

pub(crate) struct PortableObjectMapEntry<'a> {
    pub(crate) mv_table_id: i64,
    pub(crate) name: &'a str,
}

impl<'a> PortableLogicalBuilder<'a> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn intern_string(&mut self, value: &'a str) -> Result<u64> {
        if let Some(idx) = self.string_refs.get(value) {
            return Ok(*idx);
        }
        let idx = self.strings.len() as u64;
        self.strings.try_push(value)?;
        self.string_refs.insert(value, idx);
        Ok(idx)
    }

    pub(crate) fn add_object_map(&mut self, entry: PortableObjectMapEntry<'a>) -> Result<bool> {
        if !is_portable_logical_name(entry.name) || self.object_map_ids.contains(&entry.mv_table_id)
        {
            return Ok(false);
        }
        let mv_table_id = entry.mv_table_id;
        let name_ref = self.intern_string(entry.name)?;

        let mut object = crate::alloc::vec![];
        let mut serializer = LogSerializer::new(&mut object);
        log_write!(
            serializer,
            [
                ProtoSint64::new(OBJECT_FIELD_MV_TABLE_ID, entry.mv_table_id),
                ProtoKey::new(OBJECT_FIELD_NAME_REF, PROTO_WIRE_VARINT),
                ProtoVarint(name_ref),
            ]
        )?;
        self.object_maps.try_push(object)?;
        assert!(
            self.object_map_ids.insert(mv_table_id),
            "portable object map id changed while building its payload"
        );
        Ok(true)
    }

    pub(crate) fn add_metadata(&mut self, key: &'a str, value: &'a str) -> Result<()> {
        let key_ref = self.intern_string(key)?;
        let value_ref = self.intern_string(value)?;
        let mut meta = crate::alloc::vec![];
        let mut serializer = LogSerializer::new(&mut meta);
        log_write!(
            serializer,
            [
                ProtoKey::new(META_FIELD_KEY_REF, PROTO_WIRE_VARINT),
                ProtoVarint(key_ref),
                ProtoKey::new(META_FIELD_VALUE_REF, PROTO_WIRE_VARINT),
                ProtoVarint(value_ref),
            ]
        )?;
        self.metadata.try_push(meta)?;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<u8>> {
        let mut txn_fields = crate::alloc::vec![];
        let mut serializer = LogSerializer::new(&mut txn_fields);
        for value in &self.strings {
            log_write!(
                serializer,
                [
                    ProtoKey::new(TX_FIELD_STRING_TABLE, PROTO_WIRE_LENGTH_DELIMITED),
                    ProtoVarint(value.len() as u64),
                    value.as_bytes(),
                ]
            )?;
        }
        for object_map in self.object_maps {
            log_write!(
                serializer,
                [
                    ProtoKey::new(TX_FIELD_OBJECT_MAP, PROTO_WIRE_LENGTH_DELIMITED),
                    ProtoVarint(object_map.len() as u64),
                    object_map.as_slice(),
                ]
            )?;
        }
        for meta in self.metadata {
            log_write!(
                serializer,
                [
                    ProtoKey::new(TX_FIELD_META, PROTO_WIRE_LENGTH_DELIMITED),
                    ProtoVarint(meta.len() as u64),
                    meta.as_slice(),
                ]
            )?;
        }
        Ok(txn_fields)
    }
}
