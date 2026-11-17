use super::Schema;
use crate::alloc::TursoTryWithCapacityExt;
use crate::alloc::{ConcurrentAllocator, TryReserveError};
use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    MVTableId, MvStore, RowID, RowKey, RowVersion, SQLITE_SCHEMA_MVCC_TABLE_ID,
};
use crate::sync::Arc;
use crate::types::{ImmutableRecord, ImmutableRecordRef};
use crate::{Connection, Database, LimboError, Numeric, Result, Value, ValueRef};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

#[derive(Debug, Default)]
pub(crate) struct CatalogCheckpoint {
    rows: HashMap<i64, (MVTableId, RowVersion)>,
}

impl CatalogCheckpoint {
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn rewrite<Clock: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        row_id: &RowID,
        version: &mut RowVersion,
        mvstore: &MvStore<Clock, A>,
        allocations: &std::collections::HashMap<MVTableId, u64>,
    ) -> Result<()> {
        if version.end().is_some() {
            return Ok(());
        }
        let Some(identity) =
            sqlite_schema_btree_identity(row_id.table_id, version).filter(|id| id.root_page < 0)
        else {
            return Ok(());
        };
        let id = MVTableId::new(identity.root_page);
        let root = allocations
            .get(&id)
            .copied()
            .or_else(|| mvstore.current_root_page(&id))
            .expect("catalog B-tree must have an allocated root");
        let record = ImmutableRecordRef::from_bin_record(version.payload(row_id));
        let mut values = record.get_values_owned()?;
        values[3] = Value::from_i64(root as i64);
        let record = ImmutableRecord::from_values(&values, values.len())?;
        version.data = Some(crate::alloc::try_arc_slice_from_slice_in(
            record.get_payload(),
            mvstore.allocator(),
        )?);
        self.rows
            .insert(row_id.row_id.to_int_or_panic(), (id, version.clone()));
        Ok(())
    }
    fn has_unpublished_schema_changes<Clock: LogicalClock, A: ConcurrentAllocator>(
        &self,
        mvstore: &MvStore<Clock, A>,
        database: &Database,
    ) -> bool {
        let schema = database.schema.lock();
        if !schema.dropped_root_pages.is_empty() {
            return true;
        }
        // A negative root page is "unpublished" only if THIS checkpoint materialized the
        // object (has a real root page in `table_id_to_rootpage` not yet written back to
        // the schema). Objects created after our snapshot are deferred to a later
        // checkpoint and have no mapping yet, so their placeholders aren't our work —
        // counting them was the false-positive that panicked the TruncateWal assert.
        let owned_negative = |root_page: i64| -> bool {
            root_page < 0
                && mvstore
                    .table_id_to_rootpage
                    .get(&MVTableId::from(root_page))
                    .and_then(|entry| entry.value().root_page)
                    .is_some()
        };
        schema.all_tables().any(|table| {
            table
                .btree()
                .is_some_and(|btree| owned_negative(btree.root_page))
        }) || schema
            .all_indexes()
            .any(|index| owned_negative(index.root_page))
    }

    pub(crate) fn publish<Clock: LogicalClock, A: ConcurrentAllocator>(
        &mut self,
        mvstore: &MvStore<Clock, A>,
        database: &Database,
        connection: &Connection,
        database_id: usize,
        freed_root_pages: &mut HashSet<i64>,
    ) -> Result<(), TryReserveError> {
        if self.rows.is_empty()
            && !self.has_unpublished_schema_changes(mvstore, database)
            && freed_root_pages.is_empty()
        {
            return Ok(());
        }

        let mut ended_created_roots = HashSet::default();
        // Patch sqlite_schema rows in the MV store to use positive rootpages for the btrees
        // flushed to the physical database.
        for (sqlite_schema_rowid, (_, row_version)) in self.rows.drain() {
            let key = RowID {
                table_id: SQLITE_SCHEMA_MVCC_TABLE_ID,
                row_id: RowKey::Int(sqlite_schema_rowid),
            };
            let sqlite_schema_row = mvstore.rows.get(&key).expect("sqlite_schema row not found");
            let mut row_versions = sqlite_schema_row.value().write();
            // Patch payload in place (same version id), don't append: a duplicate
            // (id, begin, end) would leave a phantom current version after a later
            // DELETE (which ends only the first match), causing spurious write-write
            // conflicts at commit time.
            //
            // Only replace the row payload (positive rootpage). Preserve begin/end so a
            // DROP that committed after our snapshot keeps its end stamp — wholesale
            // replace of the collected clone would resurrect the table (#7956).
            let vid = row_version.id;
            if let Some(existing) = row_versions.iter_mut().find(|rv| rv.id == vid) {
                existing.data = row_version.data;
                if existing.end().is_some() {
                    if let Some(identity) =
                        sqlite_schema_btree_identity(SQLITE_SCHEMA_MVCC_TABLE_ID, existing)
                    {
                        if identity.root_page > 0 {
                            ended_created_roots.insert(identity.root_page);
                        }
                    }
                }
            } else {
                mvstore.insert_version_raw(&mut row_versions, row_version)?;
            }
        }

        if !self.has_unpublished_schema_changes(mvstore, database)
            && ended_created_roots.is_empty()
            && freed_root_pages.is_empty()
        {
            return Ok(());
        }

        // Patch the live db.schema (not local_schema, the checkpoint's private snapshot) so
        // clone_schema() propagates real root pages; negative placeholders would orphan the
        // new btree pages.
        let mut schema_ref = database.schema.lock();
        let schema = Schema::try_make_mut(&mut schema_ref)?;
        #[cfg(feature = "conn_raw_api")]
        let mut moved_root_pages = Vec::new();
        for (_, name, table) in schema.all_tables_mut() {
            #[cfg(not(feature = "conn_raw_api"))]
            let _ = name;
            let table = Arc::get_mut(table).expect("this should be the only reference");
            let Some(btree_table) = table.btree_mut() else {
                continue;
            };
            let btree_table = Arc::make_mut(btree_table);
            if btree_table.root_page < 0 {
                #[cfg(feature = "conn_raw_api")]
                let old_root_page = btree_table.root_page;
                let table_id = MVTableId::from(btree_table.root_page);
                // Only tables this pass materialized; ones created after our snapshot stay
                // negative for a later pass.
                if let Some(root_page) = mvstore
                    .table_id_to_rootpage
                    .get(&table_id)
                    .and_then(|entry| entry.value().root_page)
                {
                    btree_table.root_page = root_page as i64;
                    #[cfg(feature = "conn_raw_api")]
                    moved_root_pages.push((old_root_page, btree_table.root_page, name.to_string()));
                }
            }
        }
        #[cfg(feature = "conn_raw_api")]
        for (old_root_page, new_root_page, name) in moved_root_pages {
            schema.table_names_by_root_page.remove(&old_root_page);
            schema.table_names_by_root_page.insert(new_root_page, name);
        }
        for index in schema.all_indexes_mut() {
            if index.root_page < 0 {
                let table_id = MVTableId::from(index.root_page);
                // Same as tables: skip indexes not materialized by this pass.
                if let Some(root_page) = mvstore
                    .table_id_to_rootpage
                    .get(&table_id)
                    .and_then(|entry| entry.value().root_page)
                {
                    let index = Arc::make_mut(index);
                    index.root_page = root_page as i64;
                }
            }
        }

        // Only forget roots whose btrees this checkpoint actually freed. A concurrent
        // DROP after our snapshot may have added roots we did not destroy (#7957).
        for root in std::mem::take(freed_root_pages) {
            schema.dropped_root_pages.remove(&root);
        }
        // Roots we materialized whose schema versions were already ended are still
        // allocated and must stay accounted for until a later destroy (#7956).
        schema
            .dropped_root_pages
            .extend(std::mem::take(&mut ended_created_roots));
        drop(schema_ref);
        let schema = database.clone_schema();
        if database_id == crate::MAIN_DB_ID {
            *connection.schema.write() = schema;
        } else {
            connection
                .database_schemas()
                .write()
                .insert(database_id, schema);
        }
        connection.bump_prepare_context_generation();
        mvstore.bump_schema_generation();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteSchemaBtreeKind {
    Table,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteSchemaBtreeIdentity {
    kind: SqliteSchemaBtreeKind,
    pub root_page: i64,
}

/// Identity of a sqlite_schema row version that refers to a B-tree-backed object.
/// Schema rewrites that preserve this identity are metadata-only and should not be
/// treated as create/drop lifecycle changes.
#[aristo::intent(
    "Recovery never replays a sqlite_schema delete as an empty payload; it keeps the row's pre-delete record, so every recovered schema row still has a decodable (type, rootpage)",
    id = "mvcc_recovered_schema_record_wellformed",
    verify = "full"
)]
fn sqlite_schema_btree_identity(
    table_id: MVTableId,
    version: &RowVersion,
) -> Option<SqliteSchemaBtreeIdentity> {
    if table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
        return None;
    }

    // Recovery can synthesize payload-less sqlite_schema tombstones when the
    // pre-delete record is no longer available. Those versions do not carry
    // enough information to recover B-tree identity.
    let payload = version
        .data
        .as_deref()
        .expect("sqlite_schema rows are table rows");
    if payload.is_empty() {
        return None;
    }

    let row_data = ImmutableRecordRef::from_bin_record(payload);
    let Ok((col0, col3)) = row_data.get_two_values(0, 3) else {
        return None;
    };

    let kind = match col0 {
        ValueRef::Text(type_str) => match type_str.as_str() {
            "table" => SqliteSchemaBtreeKind::Table,
            "index" => SqliteSchemaBtreeKind::Index,
            _ => return None,
        },
        _ => panic!("sqlite_schema.type column must be TEXT, got {col0:?}"),
    };

    let ValueRef::Numeric(Numeric::Integer(root_page)) = col3 else {
        panic!("sqlite_schema.rootpage column must be INTEGER, got {col3:?}");
    };

    if root_page == 0 {
        return None;
    }

    Some(SqliteSchemaBtreeIdentity { kind, root_page })
}

pub(crate) fn recovered_tombstone<A: ConcurrentAllocator>(
    row_id: &RowID,
    rows: &HashMap<i64, ImmutableRecord>,
    alloc: A,
) -> Result<crate::mvcc::database::Row> {
    let payload = if row_id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
        rows.get(&row_id.row_id.to_int_or_panic())
            .map(|row| row.as_blob().as_slice())
            .unwrap_or(&[])
    } else {
        &[]
    };
    Ok(crate::mvcc::database::Row::new_table_row_in(
        row_id.clone(),
        payload,
        alloc,
    )?)
}

pub(crate) struct CatalogFrame {
    pub(crate) schema_rows: HashMap<i64, ImmutableRecord>,
    pub(crate) type_sqls: std::collections::BTreeMap<i64, String>,
}

pub(crate) fn stage_recovered_catalog<Clock: LogicalClock, A: ConcurrentAllocator>(
    mvstore: &MvStore<Clock, A>,
    connection: &Connection,
    ctx: &crate::mvcc::database::RecoverCtx,
    frame: &[crate::mvcc::persistent_storage::logical_log::ParsedOp],
) -> Result<Option<CatalogFrame>> {
    use crate::mvcc::persistent_storage::logical_log::ParsedOp;
    use std::collections::BTreeMap;
    let mut schema_rows_after: Option<HashMap<i64, ImmutableRecord>> = None;
    for parsed_op in frame {
        match parsed_op {
            ParsedOp::UpsertTable {
                rowid,
                record_bytes,
                ..
            } if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                let schema_rows_after =
                    schema_rows_after.get_or_insert_with(|| ctx.schema_rows.clone());
                let record = ImmutableRecordRef::from_bin_record(record_bytes);
                let column_count = record.column_count();
                if column_count < 5 {
                    return Err(LimboError::Corrupt(format!(
                        "sqlite_schema row must have at least 5 columns, got {column_count}",
                    )));
                }
                crate::with_mv_store_allocation_site!(SchemaRowPayload, {
                    schema_rows_after.insert(
                        rowid.row_id.to_int_or_panic(),
                        ImmutableRecord::from_bin_record(crate::types::value_blob_from_slice(
                            record_bytes,
                        )?),
                    );
                });
            }
            ParsedOp::DeleteTable { rowid, .. }
                if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID =>
            {
                let schema_rows_after =
                    schema_rows_after.get_or_insert_with(|| ctx.schema_rows.clone());
                schema_rows_after.remove(&rowid.row_id.to_int_or_panic());
            }
            _ => {}
        }
    }
    // CREATE TYPE and DROP TYPE write the types table. The rebuilt
    // schema needs those types to give indexes their columns' type
    // orders, so stage the types table the same way.
    let types_table_id = if connection.experimental_custom_types_enabled() {
        schema_rows_after
            .as_ref()
            .unwrap_or(&ctx.schema_rows)
            .values()
            .find_map(|record| types_table_root_page(&record.as_record_ref()))
            .map(|root_page| mvstore.get_table_id_from_root_page(root_page))
    } else {
        None
    };
    let mut type_sqls_after: Option<BTreeMap<i64, String>> = None;
    for parsed_op in frame {
        match parsed_op {
            ParsedOp::UpsertTable {
                rowid,
                record_bytes,
                ..
            } if Some(rowid.table_id) == types_table_id => {
                let sql = Schema::type_sql_from_record(&ImmutableRecordRef::from_bin_record(
                    record_bytes,
                ))?;
                type_sqls_after
                    .get_or_insert_with(|| ctx.type_sqls.clone())
                    .insert(rowid.row_id.to_int_or_panic(), sql);
            }
            ParsedOp::DeleteTable { rowid, .. } if Some(rowid.table_id) == types_table_id => {
                type_sqls_after
                    .get_or_insert_with(|| ctx.type_sqls.clone())
                    .remove(&rowid.row_id.to_int_or_panic());
            }
            _ => {}
        }
    }
    if type_sqls_after.is_some() && schema_rows_after.is_none() {
        schema_rows_after = Some(ctx.schema_rows.clone());
    }
    Ok(schema_rows_after.map(|schema_rows| CatalogFrame {
        schema_rows,
        type_sqls: type_sqls_after.unwrap_or_else(|| ctx.type_sqls.clone()),
    }))
}

pub(crate) fn build_recovered_schema(
    connection: &Arc<Connection>,
    schema_rows: &HashMap<i64, ImmutableRecord>,
    type_sqls: &[String],
    cookie: u32,
    preserved_table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    resolve_root: impl Fn(i64) -> i64,
) -> Result<Arc<Schema>> {
    let mut fresh = connection
        .db
        .dialect()
        .new_schema(connection.db.experimental_custom_types_enabled())?;
    fresh.schema_version = cookie;
    for vtab in preserved_table_valued_functions {
        let name = crate::util::normalize_ident(&vtab.name);
        if !fresh.has_table(crate::namespace::NamespaceId::ROOT, &name) {
            fresh.insert_table(
                crate::namespace::NamespaceId::ROOT,
                name,
                Arc::new(super::Table::Virtual(vtab.clone())),
            );
        }
    }
    let mut from_sql_indexes =
        crate::alloc::Vec::try_with_capacity_ext(10).expect(crate::alloc::ALLOC_ERR_MSG);
    let mut automatic_indices: HashMap<String, crate::alloc::Vec<(String, i64)>> =
        HashMap::default();
    let mut dbsp_state_roots: HashMap<String, i64> = HashMap::default();
    let mut dbsp_state_index_roots: HashMap<String, i64> = HashMap::default();
    let mut materialized_view_info: HashMap<String, crate::schema::StoredMaterializedView> =
        HashMap::default();
    let syms = connection.syms.read();
    let mv_store = connection.db.get_mv_store().clone();

    let mut sorted_rowids: Vec<i64> = schema_rows.keys().copied().collect();
    sorted_rowids.sort_unstable();
    for rowid in &sorted_rowids {
        let record = &schema_rows[rowid];
        let ty = match record.get_value_opt(0) {
            Some(ValueRef::Text(v)) => v.as_str(),
            _ => {
                return Err(LimboError::Corrupt(
                    "sqlite_schema type must be text".to_string(),
                ));
            }
        };
        let name = match record.get_value_opt(1) {
            Some(ValueRef::Text(v)) => v.as_str(),
            _ => {
                return Err(LimboError::Corrupt(
                    "sqlite_schema name must be text".to_string(),
                ));
            }
        };
        let table_name = match record.get_value_opt(2) {
            Some(ValueRef::Text(v)) => v.as_str(),
            _ => {
                return Err(LimboError::Corrupt(
                    "sqlite_schema tbl_name must be text".to_string(),
                ));
            }
        };
        let root_page = match record.get_value_opt(3) {
            Some(ValueRef::Numeric(Numeric::Integer(v))) => v,
            _ => {
                return Err(LimboError::Corrupt(
                    "sqlite_schema root_page must be integer".to_string(),
                ));
            }
        };
        // A negative root is a not-yet-materialized placeholder. If a (passive) checkpoint
        // has since materialized this object, resolve to its real positive root so the
        // rebuilt schema reflects the btree page — otherwise integrity_check skips the
        // negative root and reports its materialized page as orphaned ("never used").
        let root_page = resolve_root(root_page);
        let sql = match record.get_value_opt(4) {
            Some(ValueRef::Text(v)) => Some(v.as_str()),
            _ => None,
        };
        let attached_resolver = |alias: &str| -> Option<usize> {
            connection
                .current_qualifiers()
                .database_id(&crate::util::normalize_ident(alias))
        };
        fresh.handle_schema_row(
            ty,
            name,
            table_name,
            root_page,
            sql,
            &syms,
            &mut from_sql_indexes,
            &mut automatic_indices,
            &mut dbsp_state_roots,
            &mut dbsp_state_index_roots,
            &mut materialized_view_info,
            &attached_resolver,
            connection.dialect().as_ref(),
        )?;
    }
    fresh.populate_types_views_then_indices(
        &syms,
        type_sqls,
        materialized_view_info,
        dbsp_state_roots,
        dbsp_state_index_roots,
        from_sql_indexes,
        automatic_indices,
        mv_store.is_some(),
    )?;

    Ok(Arc::new(fresh))
}

pub(crate) fn types_table_root_page(record: &ImmutableRecordRef) -> Option<i64> {
    let is_types_table = matches!(record.get_value_opt(0), Some(ValueRef::Text(ty)) if ty.as_str() == "table")
        && matches!(record.get_value_opt(1), Some(ValueRef::Text(name)) if name.as_str() == crate::schema::TURSO_TYPES_TABLE_NAME);
    match record.get_value_opt(3) {
        Some(ValueRef::Numeric(Numeric::Integer(root_page))) if is_types_table => Some(root_page),
        _ => None,
    }
}
