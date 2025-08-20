use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use turso_core::OpenFlags;
use uuid::Uuid;

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_sync_operations::{
        connect_untracked, count_local_changes, db_bootstrap, fetch_last_change_id, has_table,
        push_logical_changes, reset_wal_file, update_last_change_id, wait_full_body,
        wal_apply_from_file, wal_pull_to_file, PAGE_SIZE, WAL_FRAME_HEADER, WAL_FRAME_SIZE,
    },
    database_tape::{
        DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts, DatabaseReplaySession,
        DatabaseReplaySessionOpts, DatabaseTape, DatabaseTapeOpts, DatabaseWalSession,
        CDC_PRAGMA_NAME,
    },
    errors::Error,
    io_operations::IoOperations,
    protocol_io::ProtocolIO,
    types::{
        Coro, DatabaseMetadata, DatabaseTapeOperation, DbChangesStatus, DbLocalChangesCount,
        DbSyncStatus,
    },
    wal_session::WalSession,
    Result,
};

#[derive(Debug, Clone)]
pub struct DatabaseSyncEngineOpts {
    pub client_name: String,
    pub tables_ignore_changes: Vec<String>,
    pub wal_pull_batch_size: u64,
}

pub struct DatabaseSyncEngine<P: ProtocolIO> {
    io: Arc<dyn turso_core::IO>,
    protocol: Arc<P>,
    db_file: Arc<dyn turso_core::DatabaseStorage>,
    main_tape: DatabaseTape,
    revert_db_wal_path: String,
    main_db_path: String,
    meta_path: String,
    opts: DatabaseSyncEngineOpts,
    meta: RefCell<Option<DatabaseMetadata>>,
}

async fn update_meta<IO: ProtocolIO>(
    coro: &Coro,
    io: &IO,
    meta_path: &str,
    orig: &mut Option<DatabaseMetadata>,
    update: impl FnOnce(&mut DatabaseMetadata),
) -> Result<()> {
    let mut meta = orig.as_ref().unwrap().clone();
    update(&mut meta);
    tracing::info!("update_meta: {meta:?}");
    let completion = io.full_write(meta_path, meta.dump()?)?;
    // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
    wait_full_body(coro, &completion).await?;
    *orig = Some(meta);
    Ok(())
}

async fn set_meta<IO: ProtocolIO>(
    coro: &Coro,
    io: &IO,
    meta_path: &str,
    orig: &mut Option<DatabaseMetadata>,
    meta: DatabaseMetadata,
) -> Result<()> {
    tracing::info!("set_meta: {meta:?}");
    let completion = io.full_write(meta_path, meta.dump()?)?;
    // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
    wait_full_body(coro, &completion).await?;
    *orig = Some(meta);
    Ok(())
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..28 + 4].try_into().unwrap())
}

impl<C: ProtocolIO> DatabaseSyncEngine<C> {
    /// Creates new instance of SyncEngine and initialize it immediately if no consistent local data exists
    pub async fn new(
        coro: &Coro,
        io: Arc<dyn turso_core::IO>,
        protocol: Arc<C>,
        main_db_path: &str,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        let revert_db_wal_path = format!("{main_db_path}-wal-revert");

        let db_file = io.open_file(main_db_path, turso_core::OpenFlags::Create, false)?;
        let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(db_file));
        let main_db = turso_core::Database::open_with_flags(
            io.clone(),
            main_db_path,
            db_file.clone(),
            OpenFlags::Create,
            false,
            true,
            false,
        )
        .unwrap();
        let tape_opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("full".to_string()),
        };
        let main_tape = DatabaseTape::new_with_opts(main_db, tape_opts);
        tracing::debug!("initialize database tape connection: path={}", main_db_path);
        let mut db = Self {
            io,
            protocol,
            db_file,
            main_tape,
            revert_db_wal_path,
            main_db_path: main_db_path.to_string(),
            meta_path: format!("{main_db_path}-info"),
            opts,
            meta: RefCell::new(None),
        };
        db.init(coro).await?;
        Ok(db)
    }

    fn open_revert_db_conn(&mut self) -> Result<Arc<turso_core::Connection>> {
        let db = turso_core::Database::open_with_flags_bypass_registry(
            self.io.clone(),
            &self.main_db_path,
            &self.revert_db_wal_path,
            self.db_file.clone(),
            OpenFlags::Create,
            false,
            true,
            false,
        )?;
        let conn = db.connect()?;
        conn.wal_auto_checkpoint_disable();
        Ok(conn)
    }

    async fn checkpoint_passive(&mut self, coro: &Coro) -> Result<u64> {
        let watermark = self.meta().revert_since_wal_watermark as u64;
        tracing::debug!(
            "checkpoint(path={:?}): revert_since_wal_watermark={}",
            self.main_db_path,
            watermark
        );
        let main_conn = connect_untracked(&self.main_tape)?;
        let main_wal_state = main_conn.wal_state()?;
        tracing::debug!(
            "checkpoint(path={:?}): main_wal_state={:?}",
            self.main_db_path,
            main_wal_state
        );
        assert!(main_wal_state.checkpoint_seq_no >= self.meta().revert_since_wal_checkpoint_seq);

        if main_wal_state.checkpoint_seq_no > self.meta().revert_since_wal_checkpoint_seq {
            update_meta(
                coro,
                self.protocol.as_ref(),
                &self.meta_path,
                &mut self.meta.borrow_mut(),
                |meta| {
                    meta.revert_since_wal_watermark = 0;
                    meta.revert_since_wal_checkpoint_seq = main_wal_state.checkpoint_seq_no;
                },
            )
            .await?;
            return Ok(0);
        }
        // we do this Passive checkpoint in order to transfer all synced frames to the DB file and make history of revert DB valid
        // if we will not do that we will be in situation where WAL in the revert DB is not valid relative to the DB file
        let result = main_conn.checkpoint(turso_core::CheckpointMode::Passive {
            upper_bound_inclusive: Some(watermark),
        })?;
        tracing::debug!(
            "checkpoint(path={:?}): checkpointed portion of WAL: {:?}",
            self.main_db_path,
            result
        );
        if result.max_frame < watermark {
            return Err(Error::DatabaseSyncEngineError(
                format!("unable to checkpoint synced portion of WAL: result={result:?}, watermark={watermark}"),
            ));
        }
        Ok(watermark)
    }

    pub async fn count_local_changes(&self, coro: &Coro) -> Result<DbLocalChangesCount> {
        let main_conn = connect_untracked(&self.main_tape)?;
        let change_id = self.meta().last_pushed_change_id_hint;
        Ok(DbLocalChangesCount {
            cdc_operations: count_local_changes(coro, &main_conn, change_id).await?,
        })
    }

    pub async fn checkpoint(&mut self, coro: &Coro) -> Result<()> {
        let watermark = self.checkpoint_passive(coro).await?;

        let main_conn = connect_untracked(&self.main_tape)?;
        let revert_conn = self.open_revert_db_conn()?;

        let mut page = [0u8; PAGE_SIZE];
        let db_size = if revert_conn.try_wal_watermark_read_page(1, &mut page, None)? {
            db_size_from_page(&page)
        } else {
            0
        };

        tracing::debug!(
            "checkpoint(path={:?}): revert DB initial size: {}",
            self.main_db_path,
            db_size
        );

        let main_wal_state;
        {
            let mut revert_session = WalSession::new(revert_conn.clone());
            revert_session.begin()?;

            let mut main_session = WalSession::new(main_conn.clone());
            main_session.begin()?;

            main_wal_state = main_conn.wal_state()?;
            tracing::debug!(
                "checkpoint(path={:?}): main DB WAL state: {:?}",
                self.main_db_path,
                main_wal_state
            );

            let mut revert_session = DatabaseWalSession::new(coro, revert_session).await?;

            let main_changed_pages = main_conn.wal_changed_pages_after(watermark)?;
            tracing::debug!(
                "checkpoint(path={:?}): collected {} changed pages",
                self.main_db_path,
                main_changed_pages.len()
            );
            let revert_changed_pages: HashSet<u32> = revert_conn
                .wal_changed_pages_after(0)?
                .into_iter()
                .collect();
            for page_no in main_changed_pages {
                if revert_changed_pages.contains(&page_no) {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it present in revert WAL",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                if page_no > db_size {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it ahead of revert-DB size",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                if !main_conn.try_wal_watermark_read_page(page_no, &mut page, Some(watermark))? {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it was allocated in the wAL portion for revert",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                tracing::debug!(
                    "checkpoint(path={:?}): append page {} (current db_size={})",
                    self.main_db_path,
                    page_no,
                    db_size
                );
                revert_session.append_page(page_no, &page)?;
            }
            revert_session.commit(db_size)?;
            revert_session.wal_session.end(false)?;
        }
        update_meta(
            coro,
            self.protocol.as_ref(),
            &self.meta_path,
            &mut self.meta.borrow_mut(),
            |meta| {
                meta.revert_since_wal_checkpoint_seq = main_wal_state.checkpoint_seq_no;
                meta.revert_since_wal_watermark = main_wal_state.max_frame;
            },
        )
        .await?;

        let result = main_conn.checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: Some(main_wal_state.max_frame),
        })?;
        tracing::debug!(
            "checkpoint(path={:?}): main DB TRUNCATE checkpoint result: {:?}",
            self.main_db_path,
            result
        );

        Ok(())
    }

    pub async fn wait_changes_from_remote(&self, coro: &Coro) -> Result<Option<DbChangesStatus>> {
        let file_path = format!("{}-frames-{}", self.main_db_path, Uuid::new_v4());
        tracing::info!(
            "wait_changes(path={}): file_path={}",
            self.main_db_path,
            file_path
        );
        let file = self.io.create(&file_path)?;

        let synced_generation = self.meta().synced_generation;
        let synced_frame_no = self.meta().synced_frame_no;

        let DbSyncStatus {
            generation,
            max_frame_no,
            ..
        } = wal_pull_to_file(
            coro,
            self.protocol.as_ref(),
            file.clone(),
            synced_generation,
            synced_frame_no.unwrap_or(0) + 1,
            self.opts.wal_pull_batch_size,
        )
        .await?;

        if file.size()? == 0 {
            tracing::info!(
                "wait_changes(path={}): no changes detected, removing changes file {}",
                self.main_db_path,
                file_path
            );
            self.io.remove_file(&file_path)?;
            return Ok(None);
        }

        tracing::debug!(
            "wait_changes_from_remote(path={}): generation: {} -> {}, frame_no: {:?} -> {}",
            self.main_db_path,
            synced_generation,
            generation,
            synced_frame_no,
            max_frame_no
        );

        Ok(Some(DbChangesStatus {
            generation,
            max_frame_no,
            file_path,
        }))
    }

    /// Sync all new changes from remote DB and apply them locally
    /// This method will **not** send local changed to the remote
    /// This method will block writes for the period of pull
    pub async fn apply_changes_from_remote(
        &mut self,
        coro: &Coro,
        remote_changes: DbChangesStatus,
    ) -> Result<()> {
        let pull_result = self.apply_changes_internal(coro, &remote_changes).await;
        let cleanup_result: Result<()> = self
            .io
            .remove_file(&remote_changes.file_path)
            .inspect_err(|e| tracing::error!("failed to cleanup changes file: {e}"))
            .map_err(|e| e.into());
        let Ok(revert_since_wal_watermark) = pull_result else {
            return Err(pull_result.err().unwrap());
        };

        let revert_wal_file = self.io.open_file(
            &self.revert_db_wal_path,
            turso_core::OpenFlags::Create,
            false,
        )?;
        reset_wal_file(coro, revert_wal_file, 0).await?;

        update_meta(
            coro,
            self.protocol.as_ref(),
            &self.meta_path,
            &mut self.meta.borrow_mut(),
            |meta| {
                meta.revert_since_wal_watermark = revert_since_wal_watermark;
                meta.synced_frame_no = Some(remote_changes.max_frame_no);
                meta.synced_generation = remote_changes.generation;
                meta.last_pushed_change_id_hint = 0;
            },
        )
        .await?;

        cleanup_result
    }
    async fn apply_changes_internal(
        &mut self,
        coro: &Coro,
        remote_changes: &DbChangesStatus,
    ) -> Result<u64> {
        tracing::info!(
            "apply_changes(path={}, changes={:?})",
            self.main_db_path,
            remote_changes
        );

        let watermark = self.checkpoint_passive(coro).await?;

        let changes_file = self.io.open_file(
            &remote_changes.file_path,
            turso_core::OpenFlags::empty(),
            false,
        )?;

        let revert_conn = self.open_revert_db_conn()?;
        let main_conn = connect_untracked(&self.main_tape)?;

        let mut revert_session = WalSession::new(revert_conn.clone());
        revert_session.begin()?;

        let mut main_session = WalSession::new(main_conn.clone());
        main_session.begin()?;

        let has_cdc_table = has_table(coro, &main_conn, "turso_cdc").await?;

        // read schema version after initiating WAL session (in order to read it with consistent max_frame_no)
        let main_conn_schema_version = main_conn.read_schema_version()?;

        let mut main_session = DatabaseWalSession::new(coro, main_session).await?;

        // fetch last_change_id from remote
        let (pull_gen, last_change_id) = fetch_last_change_id(
            coro,
            self.protocol.as_ref(),
            &main_conn,
            &self.meta().client_unique_id,
        )
        .await?;

        // collect local changes before doing anything with the main DB
        // it's important to do this after opening WAL session - otherwise we can miss some updates
        let iterate_opts = DatabaseChangesIteratorOpts {
            first_change_id: last_change_id.map(|x| x + 1),
            mode: DatabaseChangesIteratorMode::Apply,
            ignore_schema_changes: false,
            ..Default::default()
        };
        let mut local_changes = Vec::new();
        let mut iterator = self.main_tape.iterate_changes(iterate_opts)?;
        while let Some(operation) = iterator.next(coro).await? {
            match operation {
                DatabaseTapeOperation::RowChange(change) => local_changes.push(change),
                DatabaseTapeOperation::Commit => continue,
            }
        }
        tracing::info!(
            "apply_changes(path={}): collected {} changes",
            self.main_db_path,
            local_changes.len()
        );

        // rollback local changes not checkpointed to the revert-db
        tracing::info!(
            "apply_changes(path={}): rolling back frames after {} watermark, max_frame={}",
            self.main_db_path,
            watermark,
            main_conn.wal_state()?.max_frame
        );
        let local_rollback = main_session.rollback_changes_after(watermark)?;
        let mut frame = [0u8; WAL_FRAME_SIZE];

        tracing::info!(
            "apply_changes(path={}): rolling back {} frames from revert DB",
            self.main_db_path,
            revert_conn.wal_state()?.max_frame
        );
        // rollback local changes by using frames from revert-db
        // it's important to append pages from revert-db after local revert - because pages from revert-db must overwrite rollback from main DB
        let remote_rollback = revert_conn.wal_state()?.max_frame;
        for frame_no in 1..=remote_rollback {
            let info = revert_session.read_at(frame_no, &mut frame)?;
            main_session.append_page(info.page_no, &frame[WAL_FRAME_HEADER..])?;
        }

        // after rollback - WAL state is aligned with remote - let's apply changes from it
        let db_size = wal_apply_from_file(coro, changes_file, &mut main_session).await?;
        tracing::info!(
            "apply_changes(path={}): applied changes from remote: db_size={}",
            self.main_db_path,
            db_size,
        );

        let revert_since_wal_watermark;
        if local_changes.is_empty() && local_rollback == 0 && remote_rollback == 0 && !has_cdc_table
        {
            main_session.commit(db_size)?;
            revert_since_wal_watermark = main_session.frames_count()?;
            main_session.wal_session.end(true)?;
        } else {
            main_session.commit(0)?;

            let current_schema_version = main_conn.read_schema_version()?;
            revert_since_wal_watermark = main_session.frames_count()?;
            let final_schema_version = current_schema_version.max(main_conn_schema_version) + 1;
            main_conn.write_schema_version(final_schema_version)?;
            tracing::info!(
                "apply_changes(path={}): updated schema version to {}",
                self.main_db_path,
                final_schema_version
            );

            update_last_change_id(
                coro,
                &main_conn,
                &self.meta().client_unique_id,
                pull_gen + 1,
                0,
            )
            .await?;

            if has_cdc_table {
                tracing::info!(
                    "apply_changes(path={}): initiate CDC pragma again in order to recreate CDC table",
                    self.main_db_path,
                );
                let _ = main_conn.pragma_update(CDC_PRAGMA_NAME, "'full'")?;
            }

            let mut replay = DatabaseReplaySession {
                conn: main_conn.clone(),
                cached_delete_stmt: HashMap::new(),
                cached_insert_stmt: HashMap::new(),
                cached_update_stmt: HashMap::new(),
                in_txn: true,
                generator: DatabaseReplayGenerator {
                    conn: main_conn.clone(),
                    opts: DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    },
                },
            };
            for change in local_changes {
                let operation = DatabaseTapeOperation::RowChange(change);
                replay.replay(coro, operation).await?;
            }

            main_session.wal_session.end(true)?;
        }

        Ok(revert_since_wal_watermark)
    }

    /// Sync local changes to remote DB
    /// This method will **not** pull remote changes to the local DB
    /// This method will **not** block writes for the period of sync
    pub async fn push_changes_to_remote(&self, coro: &Coro) -> Result<()> {
        tracing::info!("push_changes(path={})", self.main_db_path);

        let (_, change_id) = push_logical_changes(
            coro,
            self.protocol.as_ref(),
            &self.main_tape,
            &self.meta().client_unique_id,
            &self.opts.tables_ignore_changes,
        )
        .await?;

        update_meta(
            coro,
            self.protocol.as_ref(),
            &self.meta_path,
            &mut self.meta.borrow_mut(),
            |m| {
                m.last_pushed_change_id_hint = change_id;
            },
        )
        .await?;

        Ok(())
    }

    /// Create read/write database connection and appropriately configure it before use
    pub async fn connect_rw(&self, coro: &Coro) -> Result<Arc<turso_core::Connection>> {
        let conn = self.main_tape.connect(coro).await?;
        conn.wal_auto_checkpoint_disable();
        Ok(conn)
    }

    /// Sync local changes to remote DB and bring new changes from remote to local
    /// This method will block writes for the period of sync
    pub async fn sync(&mut self, coro: &Coro) -> Result<()> {
        // todo(sivukhin): this is bit suboptimal as both 'push' and 'pull' will call pull_synced_from_remote
        // but for now - keep it simple
        self.push_changes_to_remote(coro).await?;
        if let Some(changes) = self.wait_changes_from_remote(coro).await? {
            self.apply_changes_from_remote(coro, changes).await?;
        }
        Ok(())
    }

    async fn init(&mut self, coro: &Coro) -> Result<()> {
        tracing::info!("init(path={}): opts={:?}", self.main_db_path, self.opts);

        let completion = self.protocol.full_read(&self.meta_path)?;
        let data = wait_full_body(coro, &completion).await?;
        let meta = if data.is_empty() {
            None
        } else {
            Some(DatabaseMetadata::load(&data)?)
        };

        match meta {
            Some(meta) => {
                self.meta.replace(Some(meta));
            }
            None => {
                let meta = self.bootstrap_db_file(coro).await?;
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");
                set_meta(
                    coro,
                    self.protocol.as_ref(),
                    &self.meta_path,
                    &mut self.meta.borrow_mut(),
                    meta,
                )
                .await?;
            }
        };

        let main_exists = self.io.try_open(&self.main_db_path)?.is_some();
        if !main_exists {
            let error = "main DB file doesn't exists, but metadata is".to_string();
            return Err(Error::DatabaseSyncEngineError(error));
        }

        if self.meta().synced_frame_no.is_none() {
            // sync WAL from the remote in case of bootstrap - all subsequent initializations will be fast
            if let Some(changes) = self.wait_changes_from_remote(coro).await? {
                self.apply_changes_from_remote(coro, changes).await?;
            }
        }
        Ok(())
    }

    async fn bootstrap_db_file(&mut self, coro: &Coro) -> Result<DatabaseMetadata> {
        assert!(
            self.meta.borrow().is_none(),
            "bootstrap_db_file must be called only when meta is not set"
        );
        tracing::info!("bootstrap_db_file(path={})", self.main_db_path);

        let start_time = std::time::Instant::now();
        // cleanup all files left from previous attempt to bootstrap
        // we shouldn't write any WAL files - but let's truncate them too for safety
        if let Some(file) = self.io.try_open(&self.main_db_path)? {
            self.io.truncate(coro, file, 0).await?;
        }
        if let Some(file) = self.io.try_open(&format!("{}-wal", self.main_db_path))? {
            self.io.truncate(coro, file, 0).await?;
        }

        let file = self.io.create(&self.main_db_path)?;
        let db_info = db_bootstrap(coro, self.protocol.as_ref(), file).await?;

        let elapsed = std::time::Instant::now().duration_since(start_time);
        tracing::info!(
            "bootstrap_db_files(path={}): finished: elapsed={:?}",
            self.main_db_path,
            elapsed
        );

        Ok(DatabaseMetadata {
            client_unique_id: format!("{}-{}", self.opts.client_name, uuid::Uuid::new_v4()),
            synced_generation: db_info.current_generation,
            synced_frame_no: None,
            revert_since_wal_checkpoint_seq: 0,
            revert_since_wal_watermark: 0,
            last_pushed_change_id_hint: 0,
            last_pushed_pull_gen_hint: 0,
        })
    }

    fn meta(&self) -> std::cell::Ref<'_, DatabaseMetadata> {
        std::cell::Ref::map(self.meta.borrow(), |x| {
            x.as_ref().expect("metadata must be set")
        })
    }
}
