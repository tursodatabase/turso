//! SQL names and primary keys for portable logical-log consumers.
use crate::alloc::{ConcurrentAllocator, Vec};
use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    LogRecord, MVTableId, MvStore, RowID, RowKey, RowVersion, TxTimestampOrID,
    SQLITE_SCHEMA_MVCC_TABLE_ID,
};
use crate::mvcc::persistent_storage::logical_log::{
    parse_ops_from_plaintext, LogSerializer, ParsedOp, LOG_RECORD_PREFIX_SIZE,
};
use crate::mvcc::portable_logical::{
    is_portable_logical_name, is_portable_schema_row, is_portable_table_schema_row,
    portable_schema_row_from_record, PortableLogicalBuilder, PortableObjectMapEntry,
};
use crate::namespace::NamespaceId;
use crate::schema::Schema;
use crate::sync::Arc;
use crate::types::{ImmutableRecord, ImmutableRecordRef};
use crate::{turso_assert, Connection, LimboError, Result, Value};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

pub(crate) fn populate_changes<Clock: LogicalClock, A: ConcurrentAllocator>(
    connection: &Connection,
    mvcc_store: &Arc<MvStore<Clock, A>>,
    log_record: &mut LogRecord,
) -> Result<()> {
    if !connection.portable_logical_changes_enabled() {
        return Ok(());
    }
    log_record.portable_changes_enabled = true;

    let mut builder = PortableLogicalBuilder::new();
    let mut metadata: Vec<_> = connection.mvcc_log_meta_snapshot().into_iter().collect();
    metadata.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, value) in &metadata {
        builder.add_metadata(key, value)?;
    }

    // The recovery payload is the single durable operation stream.
    // The portable extension only adds the metadata required to interpret
    // recovery table ids outside this database instance.
    let mut table_refs_by_id = HashMap::default();
    let recovery_payload = &log_record.buf[LOG_RECORD_PREFIX_SIZE..];
    let parsed_ops = parse_ops_from_plaintext(
        recovery_payload,
        recovery_payload.len(),
        log_record.op_count,
        log_record.tx_timestamp,
    )?;

    let mut schema_upserts = HashMap::default();
    let mut schema_deletes = HashMap::default();
    let mut schema_rowids = Vec::new();
    let mut data_table_ids = HashSet::default();
    let mut has_portable_schema_changes = false;
    for op in &parsed_ops {
        match op {
            ParsedOp::UpsertTable {
                table_id,
                rowid,
                record_bytes,
                ..
            } if *table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                let rowid = rowid.row_id.to_int_or_panic();
                let row = portable_schema_row_from_record(record_bytes)?;
                has_portable_schema_changes |= is_portable_schema_row(&row);
                schema_rowids.push(rowid);
                schema_upserts.insert(rowid, row);
            }
            ParsedOp::DeleteTable {
                rowid,
                record_bytes,
                ..
            } if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                if record_bytes.is_empty() {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema DELETE_TABLE missing old record".to_string(),
                    ));
                }
                let schema_rowid = rowid.row_id.to_int_or_panic();
                let row = portable_schema_row_from_record(record_bytes)?;
                has_portable_schema_changes |= is_portable_schema_row(&row);
                schema_rowids.push(schema_rowid);
                schema_deletes.insert(schema_rowid, row);
            }
            ParsedOp::UpsertTable { table_id, .. } => {
                data_table_ids.insert(*table_id);
            }
            ParsedOp::DeleteTable { rowid, .. } => {
                if rowid.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                    data_table_ids.insert(rowid.table_id);
                }
            }
            ParsedOp::UpsertIndex { .. }
            | ParsedOp::DeleteIndex { .. }
            | ParsedOp::UpdateHeader { .. }
            | ParsedOp::BtreeChanges { .. } => {}
        }
    }

    schema_rowids.sort_unstable();
    schema_rowids.dedup();
    for rowid in schema_rowids {
        let old_row = schema_deletes.get(&rowid);
        let new_row = schema_upserts.get(&rowid);
        match (old_row, new_row) {
            (Some(old_row), Some(new_row)) => {
                if is_portable_table_schema_row(old_row) {
                    table_refs_by_id.insert(
                        portable_table_id_from_rootpage(old_row.rootpage),
                        PortableTableRef {
                            name: old_row.name.clone(),
                        },
                    );
                }
                if is_portable_table_schema_row(new_row) {
                    table_refs_by_id.insert(
                        portable_table_id_from_rootpage(new_row.rootpage),
                        PortableTableRef {
                            name: new_row.name.clone(),
                        },
                    );
                }
            }
            (None, Some(new_row)) => {
                if is_portable_table_schema_row(new_row) {
                    table_refs_by_id.insert(
                        portable_table_id_from_rootpage(new_row.rootpage),
                        PortableTableRef {
                            name: new_row.name.clone(),
                        },
                    );
                }
            }
            (Some(old_row), None) => {
                if is_portable_table_schema_row(old_row) {
                    table_refs_by_id.insert(
                        portable_table_id_from_rootpage(old_row.rootpage),
                        PortableTableRef {
                            name: old_row.name.clone(),
                        },
                    );
                }
            }
            (None, None) => {}
        }
    }

    // A data table id parsed back out of the serialized payload is
    // already in its log form: either the schema's negative rootpage
    // placeholder (table not yet checkpointed) or the canonical
    // -(root_page) written by `canonicalize_table_id`. Interpret it
    // directly as a rootpage first. Consulting `table_id_to_rootpage`
    // first is wrong: its keys are the original in-memory counter ids,
    // so a canonical id like -159 can alias an unrelated object whose
    // counter id happened to be -159 (and now has a checkpointed root
    // page), sending resolution to the wrong rootpage. The map is only
    // a fallback for a payload id serialized before a concurrent
    // checkpoint published new roots.
    let rootpage_for_table_id = |table_id: MVTableId| -> i64 {
        let direct = i64::from(table_id);
        if table_name_for_rootpage(connection, direct)
            .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, direct))
            .is_some()
        {
            return direct;
        }
        mvcc_store
            .table_id_to_rootpage
            .get(&table_id)
            .and_then(|entry| entry.value().root_page)
            .map(|rootpage| rootpage as i64)
            .unwrap_or(direct)
    };

    let mut unresolved_data_tables = Vec::new();
    let mut needed_rootpages = HashSet::default();
    for table_id in &data_table_ids {
        if table_refs_by_id.contains_key(table_id) {
            continue;
        }
        let rootpage = rootpage_for_table_id(*table_id);
        if rootpage == 0 {
            continue;
        }
        let root_table_id = portable_table_id_from_rootpage(rootpage);
        if let Some(table_ref) = table_refs_by_id.get(&root_table_id).cloned() {
            table_refs_by_id.insert(*table_id, table_ref);
            continue;
        }
        needed_rootpages.insert(rootpage);
        unresolved_data_tables.push((*table_id, root_table_id));
    }

    if !needed_rootpages.is_empty() {
        for rootpage in needed_rootpages {
            let Some(name) = table_name_for_rootpage(connection, rootpage)
                .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, rootpage))
            else {
                continue;
            };
            let resolved_rootpage = if rootpage < 0 { -rootpage } else { rootpage };
            let resolved_table_id = portable_table_id_from_rootpage(resolved_rootpage);
            let table_ref = PortableTableRef { name };
            table_refs_by_id.insert(resolved_table_id, table_ref);
        }

        for (table_id, root_table_id) in unresolved_data_tables {
            if let Some(table_ref) = table_refs_by_id.get(&root_table_id).cloned() {
                table_refs_by_id.insert(table_id, table_ref);
            }
        }
    }

    for (table_id, table_ref) in &table_refs_by_id {
        if !is_portable_logical_name(&table_ref.name) {
            continue;
        }
        let added = builder.add_object_map(PortableObjectMapEntry {
            mv_table_id: i64::from(*table_id),
            name: &table_ref.name,
        })?;
        turso_assert!(
            added,
            "portable object map unexpectedly rejected a user object"
        );
    }

    for table_id in data_table_ids {
        let Some(table_ref) = table_refs_by_id.get(&table_id) else {
            return Err(LimboError::Corrupt(format!(
                "portable changes cannot resolve user data table id {table_id}"
            )));
        };
        if !is_portable_logical_name(&table_ref.name) {
            continue;
        }
    }

    log_record.portable_changes = builder.finish()?;
    log_record.portable_changes_required = has_portable_schema_changes;
    Ok(())
}

#[cfg(feature = "conn_raw_api")]
fn portable_table_id_from_rootpage(rootpage: i64) -> MVTableId {
    if rootpage > 0 {
        MVTableId::from(-rootpage)
    } else {
        MVTableId::from(rootpage)
    }
}

#[derive(Clone, Debug)]
#[cfg(feature = "conn_raw_api")]
struct PortableTableRef {
    name: String,
}

#[cfg(feature = "conn_raw_api")]
fn rootpage_for_mv_table_id<Clock: LogicalClock, A: ConcurrentAllocator>(
    mvcc_store: &MvStore<Clock, A>,
    table_id: MVTableId,
) -> i64 {
    mvcc_store
        .table_id_to_rootpage
        .get(&table_id)
        .and_then(|entry| entry.value().root_page)
        .map(|rootpage| rootpage as i64)
        .unwrap_or_else(|| i64::from(table_id))
}

#[cfg(feature = "conn_raw_api")]
fn table_name_for_rootpage_in_schema(schema: &Schema, rootpage: i64) -> Option<String> {
    if rootpage == 0 {
        return None;
    }
    if let Some(name) = schema.table_name_for_root_page(rootpage) {
        return Some(name.to_string());
    }
    let alternate_rootpage = -rootpage;
    if alternate_rootpage == 0 {
        return None;
    }
    schema
        .table_name_for_root_page(alternate_rootpage)
        .map(ToString::to_string)
}

#[cfg(feature = "conn_raw_api")]
fn table_name_for_rootpage(connection: &Connection, rootpage: i64) -> Option<String> {
    {
        let schema = connection.schema.read();
        if let Some(name) = table_name_for_rootpage_in_schema(&schema, rootpage) {
            return Some(name);
        }
    }

    let schema = connection.db.schema.lock();
    table_name_for_rootpage_in_schema(&schema, rootpage)
}

#[cfg(feature = "conn_raw_api")]
fn table_name_for_rootpage_in_mvcc_schema<Clock: LogicalClock, A: ConcurrentAllocator>(
    mvcc_store: &MvStore<Clock, A>,
    rootpage: i64,
) -> Option<String> {
    if rootpage == 0 {
        return None;
    }
    let alternate_rootpage = -rootpage;
    for entry in mvcc_store.rows.iter() {
        if entry.key().table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
            continue;
        }
        let row_versions = entry.value().read();
        for row_version in row_versions.iter().rev() {
            if row_version.end().is_some() {
                continue;
            }
            let Ok(row) = portable_schema_row_from_record(row_version.payload(entry.key())) else {
                continue;
            };
            if row.rootpage == rootpage || row.rootpage == alternate_rootpage {
                return Some(row.name);
            }
        }
    }
    None
}

#[cfg(feature = "conn_raw_api")]
fn portable_table_name_for_mv_table_id<Clock: LogicalClock, A: ConcurrentAllocator>(
    connection: &Connection,
    mvcc_store: &MvStore<Clock, A>,
    table_id: MVTableId,
) -> Option<String> {
    // `table_id` is the id as serialized into the log (see
    // `canonicalize_table_id`), so interpret it directly as a rootpage first;
    // `table_id_to_rootpage` is keyed by in-memory counter ids and a canonical
    // -(root_page) id can alias an unrelated object's counter id. The map is
    // only a fallback for a stale id serialized before a concurrent checkpoint
    // published new roots.
    let direct = i64::from(table_id);
    if let Some(name) = table_name_for_rootpage(connection, direct)
        .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, direct))
    {
        return Some(name);
    }
    let rootpage = rootpage_for_mv_table_id(mvcc_store, table_id);
    if rootpage == direct {
        return None;
    }
    table_name_for_rootpage(connection, rootpage)
        .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, rootpage))
}

#[cfg(feature = "conn_raw_api")]
pub(crate) fn delete_extension<Clock: LogicalClock, A: ConcurrentAllocator>(
    connection: &Connection,
    mvcc_store: &MvStore<Clock, A>,
    id: &RowID,
    row_version: &RowVersion,
) -> Result<Option<Vec<u8>>> {
    if !connection.portable_logical_changes_enabled() {
        return Ok(None);
    }
    if !matches!(row_version.end(), Some(TxTimestampOrID::Timestamp(_))) {
        return Ok(None);
    }
    let RowKey::Int(rowid) = id.row_id else {
        return Ok(None);
    };

    if id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
        let extension = LogSerializer::encode_delete_portable_extension(
            Some(row_version.payload(id)),
            None,
            Some(rowid),
        )?;
        return Ok((!extension.is_empty()).then_some(extension));
    }

    let Some(table_name) = portable_table_name_for_mv_table_id(connection, mvcc_store, id.table_id)
    else {
        return Ok(None);
    };
    if !is_portable_logical_name(&table_name) {
        return Ok(None);
    }

    let schema = connection.schema.read();
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, &table_name) else {
        return Ok(None);
    };

    let mut record_values = None;
    let mut pk_values = Vec::with_capacity(table.primary_key_columns.len());
    for (pk_name, _) in &table.primary_key_columns {
        let Some((logical_column, column)) = table.get_column(pk_name) else {
            return Err(LimboError::InternalError(format!(
                "primary key column {pk_name} not found for table {table_name}"
            )));
        };
        if column.is_rowid_alias() {
            pk_values.push(Value::from_i64(rowid));
            continue;
        }
        let values = match &record_values {
            Some(values) => values,
            None => record_values.insert(
                ImmutableRecordRef::from_bin_record(row_version.payload(id)).get_values_owned()?,
            ),
        };
        let physical_column = table.logical_to_physical_column(logical_column);
        let Some(value) = values.get(physical_column).cloned() else {
            return Err(LimboError::Corrupt(format!(
                "DELETE_TABLE record for {table_name} missing primary key column {pk_name}"
            )));
        };
        pk_values.push(value);
    }

    let pk_record = if pk_values.is_empty() {
        crate::alloc::vec![]
    } else {
        ImmutableRecord::from_values(&pk_values, pk_values.len())?.into_payload()
    };
    let extension =
        LogSerializer::encode_delete_portable_extension(None, Some(&pk_record), Some(rowid))?;
    Ok((!extension.is_empty()).then_some(extension))
}
