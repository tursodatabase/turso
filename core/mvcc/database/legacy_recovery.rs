//! Compatibility replay for logical-log frames without B-tree descriptors.
use super::*;

impl<Clock: LogicalClock, A: ConcurrentAllocator> MvStore<Clock, A> {
    /// Replay a single committed logical-log transaction frame into the MVCC
    /// store. Fully synchronous (the only recovery IO is reading the next frame,
    /// driven by the caller); operates on accumulators borrowed from `ctx`.
    ///
    /// A same-transaction create-then-drop leaves a delete of a sqlite_schema rowid absent from
    /// the merged state; replay treats that as a legal no-op, not corruption.
    #[aristo::intent(
        "Replaying a committed logical-log transaction never aborts with a corruption error",
        id = "mvcc_recovery_total_on_committed_log",
        verify = "full"
    )]
    pub(super) fn recover_legacy_frame(
        &self,
        connection: &Arc<Connection>,
        ctx: &mut RecoverCtx,
        frame: Vec<ParsedOp>,
    ) -> Result<()> {
        let mut max_commit_ts_seen = ctx.max_commit_ts_seen;
        let replay_cutoff_ts = ctx.replay_cutoff_ts;
        let cookie = ctx.cookie;
        let mut schema_rows = std::mem::take(&mut ctx.schema_rows);
        let mut type_sqls = std::mem::take(&mut ctx.type_sqls);
        let mut dropped_root_pages = std::mem::take(&mut ctx.dropped_root_pages);
        let mut current_schema = ctx.current_schema.clone();
        let mut index_infos = std::mem::take(&mut ctx.index_infos);

        let install_schema = |schema: Arc<Schema>| {
            *connection.schema.write() = schema.clone();
            *connection.db.schema.lock() = schema;
        };

        let root_page_for_index = |index_id: MVTableId| -> i64 {
            self.current_root_page(&index_id)
                .map(|value| value as i64)
                .unwrap_or_else(|| i64::from(index_id))
        };

        let find_index_info =
            |schema: &Schema, root_page: i64| -> Result<Option<Arc<IndexInfo>>, TryReserveError> {
                schema
                    .all_indexes()
                    .find(|idx| idx.root_page == root_page)
                    .map(|idx| {
                        IndexInfo::new_from_index_in(idx.as_ref(), self.alloc.clone()).map(Arc::new)
                    })
                    .transpose()
            };

        let schema_has_index_root = |schema: &Schema, root_page: i64| -> bool {
            schema.all_indexes().any(|idx| idx.root_page == root_page)
        };

        let parsed_op_commit_ts = |op: &ParsedOp| match op {
            ParsedOp::UpsertTable { commit_ts, .. }
            | ParsedOp::DeleteTable { commit_ts, .. }
            | ParsedOp::UpsertIndex { commit_ts, .. }
            | ParsedOp::DeleteIndex { commit_ts, .. }
            | ParsedOp::UpdateHeader { commit_ts, .. }
            | ParsedOp::BtreeChanges { commit_ts, .. } => *commit_ts,
        };
        'frame: {
            let frame_commit_ts = parsed_op_commit_ts(
                frame
                    .first()
                    .expect("next_frame should not return an empty frame"),
            );
            max_commit_ts_seen = max_commit_ts_seen.max(frame_commit_ts);
            if frame_commit_ts <= replay_cutoff_ts {
                break 'frame;
            }

            // Work out what sqlite_schema will look like after this transaction,
            // before applying any row/index changes from the transaction.
            //
            // Why this is necessary:
            // - CREATE INDEX can log index-entry inserts in the same frame as the
            //   sqlite_schema insert for the new index. Those index-entry inserts
            //   need the post-frame schema.
            // - DROP INDEX can log DELETE_INDEX entries for b-tree index entries
            //   that existed before the transaction. Those deletes still need the
            //   pre-frame schema, because the final schema no longer has idx.
            // - ALTER TABLE can delete and reinsert the same table's sqlite_schema
            //   row while also logging table/index DML. Installing the schema after
            //   only the delete would create an impossible in-between schema:
            //   sqlite_schema may still contain idx while the table row for t is
            //   temporarily absent.
            //
            // So recovery stages sqlite_schema into `schema_rows_after`, builds a
            // post-frame Schema from that staged map, and keeps the installed
            // `current_schema` unchanged until every op in the frame has replayed.
            // Most transaction frames do not change sqlite_schema, so clone the
            // schema row map only if this frame actually writes sqlite_schema.
            let mut schema_rows_after: Option<HashMap<i64, ImmutableRecord>> = None;
            for parsed_op in &frame {
                match parsed_op {
                    ParsedOp::UpsertTable {
                        rowid,
                        record_bytes,
                        ..
                    } if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                        let schema_rows_after =
                            schema_rows_after.get_or_insert_with(|| schema_rows.clone());
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
                                ImmutableRecord::from_bin_record(
                                    crate::types::value_blob_from_slice(record_bytes)?,
                                ),
                            );
                        });
                    }
                    ParsedOp::DeleteTable { rowid, .. }
                        if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID =>
                    {
                        let schema_rows_after =
                            schema_rows_after.get_or_insert_with(|| schema_rows.clone());
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
                    .unwrap_or(&schema_rows)
                    .values()
                    .find_map(|record| {
                        crate::schema::mvcc::types_table_root_page(&record.as_record_ref())
                    })
                    .map(|root_page| self.get_table_id_from_root_page(root_page))
            } else {
                None
            };
            let mut type_sqls_after: Option<BTreeMap<i64, String>> = None;
            for parsed_op in &frame {
                match parsed_op {
                    ParsedOp::UpsertTable {
                        rowid,
                        record_bytes,
                        ..
                    } if Some(rowid.table_id) == types_table_id => {
                        let sql = Schema::type_sql_from_record(
                            &ImmutableRecordRef::from_bin_record(record_bytes),
                        )?;
                        type_sqls_after
                            .get_or_insert_with(|| type_sqls.clone())
                            .insert(rowid.row_id.to_int_or_panic(), sql);
                    }
                    ParsedOp::DeleteTable { rowid, .. }
                        if Some(rowid.table_id) == types_table_id =>
                    {
                        type_sqls_after
                            .get_or_insert_with(|| type_sqls.clone())
                            .remove(&rowid.row_id.to_int_or_panic());
                    }
                    _ => {}
                }
            }
            if type_sqls_after.is_some() && schema_rows_after.is_none() {
                schema_rows_after = Some(schema_rows.clone());
            }
            // schema_rows_after is Some if the frame changes the schema
            let schema_rows_after = schema_rows_after;

            let schema_after = match schema_rows_after.as_ref() {
                Some(schema_rows_after) => {
                    let type_sqls_after = type_sqls_after.as_ref().unwrap_or(&type_sqls);
                    Some(self.recover_build_schema(
                        connection,
                        schema_rows_after,
                        &type_sqls_after.values().cloned().collect::<Vec<_>>(),
                        cookie,
                        &ctx.preserved_table_valued_functions,
                    )?)
                }
                None => None,
            };

            if schema_rows_after.is_some() {
                // Cached IndexInfo values are tied to a specific schema. Clear
                // before decoding a schema-changing frame so this frame chooses
                // from its own before/after schema pair.
                index_infos.clear();
            }

            {
                let should_skip_index_op = |parsed_op: &ParsedOp| -> bool {
                    let Some(schema_after) = schema_after.as_ref() else {
                        return false;
                    };

                    match parsed_op {
                        ParsedOp::UpsertIndex { table_id, .. } => {
                            let root_page = root_page_for_index(*table_id);
                            !schema_has_index_root(schema_after, root_page)
                        }
                        ParsedOp::DeleteIndex { table_id, .. } => {
                            let root_page = root_page_for_index(*table_id);
                            !schema_has_index_root(&current_schema, root_page)
                        }
                        _ => false,
                    }
                };

                let mut get_index_info = |index_id: MVTableId,
                                          op_kind: IndexOpKind|
                 -> Result<Arc<IndexInfo>> {
                    if let Some(index_info) = index_infos.get(&(index_id, op_kind)) {
                        return Ok(index_info.clone());
                    }

                    let root_page = root_page_for_index(index_id);
                    let before = find_index_info(&current_schema, root_page)?;
                    let after = schema_after
                        .as_ref()
                        .map(|schema| find_index_info(schema, root_page))
                        .transpose()?
                        .flatten();

                    // The logical log tells us whether an index entry is being
                    // inserted or deleted, but it stores only encoded key bytes
                    // plus the index root page. The recovery loop below skips
                    // index ops that cannot affect the final state of this
                    // frame before those bytes are decoded. For the remaining
                    // ops, pick the schema view that owns the entry at the
                    // frame boundary:
                    //
                    // - UPSERT_INDEX writes an entry that survives after the
                    //   transaction. In a schema-changing frame, the index must
                    //   exist in the post-frame schema.
                    // - DELETE_INDEX removes an entry that existed before the
                    //   transaction. In a schema-changing frame, the index must
                    //   exist in the pre-frame schema.
                    // - If the frame does not change schema, `current_schema` is
                    //   both the before and after schema.
                    let index_info = match op_kind {
                        IndexOpKind::Upsert if schema_after.is_some() => after,
                        IndexOpKind::Delete if schema_after.is_some() => before,
                        IndexOpKind::Upsert | IndexOpKind::Delete => before,
                    }
                    .ok_or_else(|| {
                        let expected_schema = match op_kind {
                            IndexOpKind::Upsert if schema_after.is_some() => "post-frame",
                            IndexOpKind::Delete if schema_after.is_some() => "pre-frame",
                            IndexOpKind::Upsert | IndexOpKind::Delete => "current",
                        };
                        LimboError::InternalError(format!(
                            "Index with root page {root_page} not found in {expected_schema} schema while recovering logical log",
                        ))
                    })?;
                    index_infos.insert((index_id, op_kind), index_info.clone());
                    Ok(index_info)
                };

                for parsed_op in frame {
                    // Some index writes are real while the transaction is
                    // running, but have no meaning at either durable boundary.
                    //
                    // Example: UPDATE a row so it writes a new entry into
                    // idx_old, then DROP INDEX idx_old before COMMIT. The
                    // UPSERT_INDEX for idx_old is not part of the database
                    // after the transaction, and the post-frame schema no
                    // longer has CREATE INDEX text for idx_old. Decoding it
                    // against the pre-frame schema would preserve an index
                    // entry for an index that was dropped. Decoding it against
                    // the post-frame schema is impossible. The correct action
                    // is to skip it.
                    //
                    // The opposite case is a DELETE_INDEX for an index created
                    // earlier in the same frame. There was no pre-frame index
                    // entry in the database file, so that delete also has no
                    // durable work to do.
                    if should_skip_index_op(&parsed_op) {
                        continue;
                    }

                    let next_rec = ctx.reader.parsed_op_to_streaming_in(
                        parsed_op,
                        &mut get_index_info,
                        self.alloc.clone(),
                    )?;

                    tracing::trace!("next_rec {next_rec:?}");

                    match next_rec {
                        StreamingResult::UpsertTableRow {
                            row,
                            rowid,
                            commit_ts,
                            btree_resident,
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            let is_schema_row = rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID;
                            if is_schema_row {
                                let record = ImmutableRecordRef::from_bin_record(row.payload());
                                let column_count = record.column_count();
                                if column_count < 5 {
                                    return Err(LimboError::Corrupt(format!(
                                        "sqlite_schema row must have at least 5 columns, got {column_count}",
                                    )));
                                }
                                let Some(ValueRef::Text(row_type)) = record.get_value_opt(0) else {
                                    return Err(LimboError::Corrupt(
                                        "sqlite_schema type must be text".to_string(),
                                    ));
                                };
                                let row_type = row_type.as_str();
                                let val = match record.get_value_opt(3) {
                                    Some(v) => v,
                                    None => {
                                        return Err(LimboError::InternalError(
                                            "Expected at least 5 columns in sqlite_schema"
                                                .to_string(),
                                        ));
                                    }
                                };
                                let ValueRef::Numeric(crate::numeric::Numeric::Integer(root_page)) =
                                    val
                                else {
                                    panic!("Expected integer value for root page, got {val:?}");
                                };
                                let sql = match record.get_value_opt(4) {
                                    Some(ValueRef::Text(v)) => Some(v.as_str()),
                                    _ => None,
                                };
                                let has_btree = match row_type {
                                    "index" => root_page != 0,
                                    "table" => !sql
                                        .is_some_and(crate::dialect::sql_is_create_virtual_table),
                                    _ => false,
                                };
                                if has_btree {
                                    if root_page == 0 {
                                        return Err(LimboError::Corrupt(format!(
                                            "sqlite_schema root_page=0 for btree {row_type}"
                                        )));
                                    }
                                    if root_page < 0 {
                                        let table_id = self.get_table_id_from_root_page(root_page);
                                        if let Some(entry) =
                                            self.table_id_to_rootpage.get(&table_id)
                                        {
                                            if let Some(value) = entry.value().root_page {
                                                panic!(
                                                    "Logical log contains an insertion of a sqlite_schema record that has both a negative root page and a positive root page: {root_page} & {value}"
                                                );
                                            }
                                        }
                                        self.insert_table_id_to_rootpage(table_id, None);
                                    } else {
                                        dropped_root_pages.remove(&root_page);
                                        let table_id = self.get_table_id_from_root_page(root_page);
                                        let Some(entry) = self.table_id_to_rootpage.get(&table_id)
                                        else {
                                            panic!(
                                                "Logical log contains root page reference {root_page} that does not exist in the table_id_to_rootpage map"
                                            );
                                        };
                                        let Some(value) = entry.value().root_page else {
                                            panic!(
                                                "Logical log contains root page reference {root_page} that does not have a root page in the table_id_to_rootpage map"
                                            );
                                        };
                                        turso_assert_eq!(value, root_page as u64, "logical log root page does not match table_id_to_rootpage map", { "root_page": root_page, "map_value": value });
                                    }
                                } else if root_page != 0 {
                                    return Err(LimboError::Corrupt(format!(
                                        "sqlite_schema root_page must be 0 for {row_type}, got {root_page}"
                                    )));
                                }
                                let rowid_int = rowid.row_id.to_int_or_panic();
                                crate::with_mv_store_allocation_site!(SchemaRowPayload, {
                                    schema_rows.insert(
                                        rowid_int,
                                        ImmutableRecord::from_bin_record(
                                            crate::types::value_blob_from_slice(row.payload())?,
                                        ),
                                    );
                                });
                            } else if self.table_id_to_rootpage.get(&rowid.table_id).is_none() {
                                // Data row references a table_id not yet in the map. This can happen
                                // with logs written before the schema-first serialization fix: in a
                                // same-transaction CREATE TABLE + INSERT + DROP TABLE, data rows were
                                // serialized before the schema INSERT that registers the table_id.
                                // The schema INSERT (or DELETE) for this table will follow later in
                                // this transaction frame, so we register the table_id now.
                                self.insert_table_id_to_rootpage(rowid.table_id, None);
                            }

                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                end: crate::mvcc::database::PackedTs::pack(None),
                                data: row.data.clone(),
                                btree_resident,
                                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                            };
                            {
                                let versions =
                                    self.get_or_create_table_row_versions(rowid.clone())?;
                                let mut versions = versions.write();
                                self.insert_version_raw(&mut versions, row_version)?;
                            }
                            let allocator = self.get_rowid_allocator(&rowid.table_id);
                            allocator.insert_row_id_maybe_update(rowid.row_id.to_int_or_panic());
                        }
                        StreamingResult::DeleteTableRow {
                            rowid, commit_ts, ..
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            if self.table_id_to_rootpage.get(&rowid.table_id).is_none() {
                                // See comment in UpsertTableRow: old logs may have data rows
                                // serialized before the schema INSERT that registers the table_id.
                                self.insert_table_id_to_rootpage(rowid.table_id, None);
                            }
                            let tombstone_row = crate::with_mv_store_allocation_site!(
                                RowPayload,
                                if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                    let rowid_int = rowid.row_id.to_int_or_panic();
                                    if let Some(record) = schema_rows.get(&rowid_int) {
                                        // Preserve the pre-delete sqlite_schema record in recovered
                                        // tombstones so checkpoint can still recover B-tree identity.
                                        Row::new_table_row_in(
                                            rowid.clone(),
                                            record.as_blob(),
                                            self.alloc.clone(),
                                        )?
                                    } else {
                                        Row::new_table_row_in(
                                            rowid.clone(),
                                            &[],
                                            self.alloc.clone(),
                                        )?
                                    }
                                } else {
                                    Row::new_table_row_in(rowid.clone(), &[], self.alloc.clone())?
                                }
                            );
                            if let Some(versions) = self.rows.get(&rowid) {
                                // Row exists in memory — try to find the current (non-ended) version
                                // that was committed before this delete, and mark it as ended. If no
                                // such version exists (e.g. it was already GC'd or this is a B-tree
                                // resident row not yet in memory), insert a tombstone instead.
                                let mut versions = versions.value().write();
                                if let Some(existing) = versions.iter_mut().rev().find(|rv| {
                                    rv.end().is_none()
                                        && matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                                }) {
                                    existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                                } else {
                                    let version_id = self.get_version_id();
                                    let row_version = RowVersion {
                                        id: version_id,
                                        begin: crate::mvcc::database::PackedTs::pack(None),
                                        end: crate::mvcc::database::PackedTs::pack(Some(
                                            TxTimestampOrID::Timestamp(commit_ts),
                                        )),
                                        data: tombstone_row.data.clone(),
                                        // The version this delete ends was not replayed, so its
                                        // frame is at or below the durable replay boundary: the
                                        // deleted row is in the DB file. Mark the tombstone
                                        // btree-resident so checkpoint applies the delete even
                                        // though the logged flag predates the row becoming durable.
                                        btree_resident: true,
                                        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                                    };
                                    self.insert_version_raw(&mut versions, row_version)?;
                                }
                            } else {
                                let version_id = self.get_version_id();
                                let row_version = RowVersion {
                                    id: version_id,
                                    begin: crate::mvcc::database::PackedTs::pack(None),
                                    end: crate::mvcc::database::PackedTs::pack(Some(
                                        TxTimestampOrID::Timestamp(commit_ts),
                                    )),
                                    data: tombstone_row.data,
                                    // Same invariant as above: no replayed version means the
                                    // deleted row is already durable in the DB file.
                                    btree_resident: true,
                                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                                };
                                let versions =
                                    self.get_or_create_table_row_versions(rowid.clone())?;
                                let mut versions = versions.write();
                                self.insert_version_raw(&mut versions, row_version)?;
                            }
                            if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                let rowid_int = rowid.row_id.to_int_or_panic();
                                let Some(record) = schema_rows.get(&rowid_int) else {
                                    // this can happen if a row in sqlite_schema was inserted and then
                                    // deleted in the same transaction (ex: a CREATE TABLE followed by a DROP TABLE)
                                    continue;
                                };
                                if record.column_count() < 5 {
                                    return Err(LimboError::Corrupt(format!(
                                        "sqlite_schema row must have at least 5 columns, got {}",
                                        record.column_count()
                                    )));
                                }
                                let (
                                    ValueRef::Text(row_type),
                                    ValueRef::Numeric(Numeric::Integer(root_page)),
                                ) = record.get_two_values(0, 3)?
                                else {
                                    return Err(LimboError::Corrupt(
                                        "sqlite_schema type and root_page must be text and integer"
                                            .to_string(),
                                    ));
                                };
                                let row_type = row_type.as_str();
                                if (row_type == "table" || row_type == "index") && root_page > 0 {
                                    dropped_root_pages.insert(root_page);
                                }
                                schema_rows.remove(&rowid_int);
                            }
                        }
                        StreamingResult::UpsertIndexRow {
                            row,
                            rowid,
                            commit_ts,
                            btree_resident,
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                end: crate::mvcc::database::PackedTs::pack(None),
                                data: row.data.clone(),
                                btree_resident,
                                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                            };
                            let RowKey::Record(sortable_key) = rowid.row_id.clone() else {
                                panic!("Index writes must be to a record");
                            };
                            self.insert_index_version(rowid.table_id, sortable_key, row_version)?;
                        }
                        StreamingResult::DeleteIndexRow {
                            row,
                            rowid,
                            commit_ts,
                            ..
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            let RowKey::Record(sortable_key) = rowid.row_id.clone() else {
                                panic!("Index writes must be to a record");
                            };
                            let sortable_key =
                                self.get_or_create_index_key_arc(rowid.table_id, sortable_key)?;
                            if let Some(index_map) = self.index_rows.get(&rowid.table_id) {
                                if let Some(versions) = index_map.value().get(&sortable_key) {
                                    let mut versions = versions.value().write();
                                    if let Some(existing) = versions.iter_mut().rev().find(|rv| {
                                rv.end().is_none()
                                    && matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                            }) {
                                existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                                continue;
                            }
                                }
                            }
                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(None),
                                end: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                data: row.data.clone(),
                                // The index entry this delete ends was not replayed, so its
                                // frame is at or below the durable replay boundary: the entry
                                // is in the DB file. Mark the tombstone btree-resident so
                                // checkpoint applies the delete even though the logged flag
                                // predates the entry becoming durable (e.g. a checkpoint that
                                // failed after its pager commit).
                                btree_resident: true,
                                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                            };
                            self.insert_index_version(rowid.table_id, sortable_key, row_version)?;
                        }
                        StreamingResult::UpdateHeader { header, commit_ts } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            // Recovery applies only post-boundary header ops; the same value is later
                            // staged to pager page-1 during checkpoint.
                            self.global_header.write().replace(header);
                        }
                        StreamingResult::BtreeChanges { .. } => {
                            unreachable!("B-tree records handled before row replay")
                        }
                        StreamingResult::Eof => {
                            unreachable!("next_frame does not return EOF records");
                        }
                    }
                }
            }

            if schema_rows_after.is_some() {
                // Now that all table and index ops from this transaction have
                // been replayed, publish the frame's final schema. No later index
                // op from this frame can observe a half-applied sqlite_schema.
                let schema_rows_after =
                    schema_rows_after.expect("schema_rows_after must exist when schema changed");
                let schema_after = schema_after
                    .expect("schema_after must exist when frame_changes_schema is true");
                schema_rows = schema_rows_after;
                if let Some(type_sqls_after) = type_sqls_after {
                    type_sqls = type_sqls_after;
                }
                self.restore_btree_snapshot(schema_after.btree_descriptors()?, frame_commit_ts);
                install_schema(schema_after.clone());
                current_schema = schema_after;
                // The frame may have decoded DROP INDEX entries using the
                // pre-frame schema. Do not carry those IndexInfo values into
                // later frames after current_schema has changed.
                index_infos.clear();
            }
        }

        ctx.max_commit_ts_seen = max_commit_ts_seen;
        ctx.schema_rows = schema_rows;
        ctx.type_sqls = type_sqls;
        ctx.dropped_root_pages = dropped_root_pages;
        ctx.current_schema = current_schema;
        ctx.index_infos = index_infos;
        Ok(())
    }
}
