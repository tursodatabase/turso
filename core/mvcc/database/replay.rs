//! Replay of logical-log frames with explicit B-tree lifetimes and descriptors.
use super::*;
use crate::storage::btree_descriptor::BtreeDescriptor;

impl<Clock: LogicalClock, A: ConcurrentAllocator> MvStore<Clock, A> {
    pub(super) fn recover_process_frame(
        &self,
        connection: &Arc<Connection>,
        ctx: &mut RecoverCtx,
        frame: Vec<ParsedOp>,
    ) -> Result<()> {
        let mut batches = frame.iter().filter_map(|op| match op {
            ParsedOp::BtreeChanges { changes, commit_ts } => Some((changes, *commit_ts)),
            _ => None,
        });
        let Some((changes, commit_ts)) = batches.next() else {
            return self.recover_legacy_frame(connection, ctx, frame);
        };
        if batches.next().is_some() {
            return Err(LimboError::Corrupt(
                "duplicate B-tree changes in log frame".into(),
            ));
        }
        ctx.max_commit_ts_seen = ctx.max_commit_ts_seen.max(commit_ts);
        if commit_ts <= ctx.replay_cutoff_ts {
            return Ok(());
        }
        self.recover_btree_changes(changes, commit_ts, &mut ctx.dropped_root_pages)?;
        let catalog = crate::schema::mvcc::stage_recovered_catalog(self, connection, ctx, &frame)?;
        for op in frame {
            let (id, index, deleted) = match &op {
                ParsedOp::UpsertTable { table_id, .. } => (*table_id, false, false),
                ParsedOp::DeleteTable { rowid, .. } => (rowid.table_id, false, true),
                ParsedOp::UpsertIndex { table_id, .. } => (*table_id, true, false),
                ParsedOp::DeleteIndex { table_id, .. } => (*table_id, true, true),
                ParsedOp::UpdateHeader { header, .. } => {
                    self.global_header.write().replace(header.clone());
                    continue;
                }
                ParsedOp::BtreeChanges { .. } => continue,
            };
            let timestamp = if deleted {
                commit_ts.saturating_sub(1)
            } else {
                commit_ts
            };
            let info = if id == SQLITE_SCHEMA_MVCC_TABLE_ID && !index {
                None
            } else {
                let objects = self.btree_objects.read();
                let object = objects.get(&id).ok_or_else(|| {
                    LimboError::Corrupt("row log entry has no B-tree descriptor".into())
                })?;
                let live =
                    object.begin <= timestamp && object.end.is_none_or(|end| end > timestamp);
                if !live {
                    if index {
                        continue;
                    }
                    return Err(LimboError::Corrupt(
                        "table log entry has no live table descriptor".into(),
                    ));
                }
                match (&object.descriptor, index) {
                    (BtreeDescriptor::Table, false) => None,
                    (BtreeDescriptor::Index(info), true) => Some(info.clone()),
                    _ => {
                        return Err(LimboError::Corrupt(
                            "row log entry has the wrong B-tree kind".into(),
                        ))
                    }
                }
            };
            let record = ctx.reader.parsed_op_to_streaming_in(
                op,
                &mut |_, _| {
                    info.clone().ok_or_else(|| {
                        LimboError::Corrupt("index log entry has no comparison descriptor".into())
                    })
                },
                self.alloc.clone(),
            )?;
            match record {
                StreamingResult::UpsertTableRow {
                    row,
                    rowid,
                    btree_resident,
                    ..
                } => {
                    let versions = self.get_or_create_table_row_versions(rowid.clone())?;
                    self.insert_version_raw(
                        &mut versions.write(),
                        self.recovered_version(row.data, commit_ts, false, btree_resident),
                    )?;
                    self.get_rowid_allocator(&rowid.table_id)
                        .insert_row_id_maybe_update(rowid.row_id.to_int_or_panic());
                }
                StreamingResult::DeleteTableRow { rowid, .. } => {
                    let row = crate::schema::mvcc::recovered_tombstone(
                        &rowid,
                        &ctx.schema_rows,
                        self.alloc.clone(),
                    )?;
                    let versions = self.get_or_create_table_row_versions(rowid)?;
                    let mut versions = versions.write();
                    if !end_recovered_version(&mut versions, commit_ts) {
                        self.insert_version_raw(
                            &mut versions,
                            self.recovered_version(row.data, commit_ts, true, true),
                        )?;
                    }
                }
                StreamingResult::UpsertIndexRow {
                    row,
                    rowid,
                    btree_resident,
                    ..
                } => {
                    let RowKey::Record(key) = rowid.row_id else {
                        unreachable!()
                    };
                    self.insert_index_version(
                        rowid.table_id,
                        key,
                        self.recovered_version(row.data, commit_ts, false, btree_resident),
                    )?;
                }
                StreamingResult::DeleteIndexRow { row, rowid, .. } => {
                    let RowKey::Record(key) = rowid.row_id else {
                        unreachable!()
                    };
                    let key = self.get_or_create_index_key_arc(rowid.table_id, key)?;
                    if let Some(index) = self.index_rows.get(&rowid.table_id) {
                        if let Some(versions) = index.value().get(&key) {
                            if end_recovered_version(&mut versions.value().write(), commit_ts) {
                                continue;
                            }
                        }
                    }
                    self.insert_index_version(
                        rowid.table_id,
                        key,
                        self.recovered_version(row.data, commit_ts, true, true),
                    )?;
                }
                _ => unreachable!("only row records reach row replay"),
            }
        }
        if let Some((rows, types)) = catalog {
            let schema = self.recover_build_schema(
                connection,
                &rows,
                &types.values().cloned().collect::<Vec<_>>(),
                ctx.cookie,
                &ctx.preserved_table_valued_functions,
            )?;
            *connection.schema.write() = schema.clone();
            *connection.db.schema.lock() = schema.clone();
            ctx.current_schema = schema;
            ctx.schema_rows = rows;
            ctx.type_sqls = types;
            ctx.index_infos.clear();
        }
        Ok(())
    }

    fn recover_btree_changes(
        &self,
        changes: &[BtreeChange],
        timestamp: u64,
        dropped_roots: &mut HashSet<i64>,
    ) -> Result<()> {
        let mut objects = self.btree_objects.write();
        // Validate the whole batch before changing any storage state.
        let mut seen = HashSet::default();
        for change in changes {
            let id = match change {
                BtreeChange::Create(id, _) | BtreeChange::Drop(id) => id,
            };
            if !seen.insert(*id) {
                return Err(LimboError::Corrupt("duplicate B-tree change".into()));
            }
            match change {
                BtreeChange::Create(id, _) if objects.contains_key(id) => {
                    return Err(LimboError::Corrupt("duplicate B-tree identity".into()))
                }
                BtreeChange::Drop(id)
                    if objects.get(id).is_none_or(|object| object.end.is_some()) =>
                {
                    return Err(LimboError::Corrupt(
                        "dropped B-tree has no live descriptor".into(),
                    ))
                }
                _ => (),
            }
        }
        for change in changes {
            match change {
                BtreeChange::Create(id, descriptor) => {
                    objects.insert(
                        *id,
                        BtreeObject {
                            descriptor: descriptor.clone(),
                            begin: timestamp,
                            end: None,
                        },
                    );
                    self.insert_table_id_to_rootpage(*id, None);
                }
                BtreeChange::Drop(id) => {
                    objects.get_mut(id).unwrap().end = Some(timestamp);
                    if let Some(root) = self.current_root_page(id) {
                        dropped_roots.insert(root as i64);
                    }
                }
            }
        }
        Ok(())
    }

    fn recovered_version(
        &self,
        data: Option<crate::alloc::ArcSlice<u8>>,
        timestamp: u64,
        deleted: bool,
        btree_resident: bool,
    ) -> RowVersion {
        RowVersion {
            id: self.get_version_id(),
            begin: PackedTs::pack((!deleted).then_some(TxTimestampOrID::Timestamp(timestamp))),
            end: PackedTs::pack(deleted.then_some(TxTimestampOrID::Timestamp(timestamp))),
            data,
            btree_resident,
            materialized_at: WalPos::ORIGIN,
        }
    }
}

fn end_recovered_version(versions: &mut [RowVersion], timestamp: u64) -> bool {
    if let Some(version) = versions.iter_mut().rev().find(|version| {
        version.end().is_none() && matches!(version.begin(), Some(TxTimestampOrID::Timestamp(begin)) if begin < timestamp)
    }) {
        version.set_end(Some(TxTimestampOrID::Timestamp(timestamp)));
        true
    } else { false }
}
