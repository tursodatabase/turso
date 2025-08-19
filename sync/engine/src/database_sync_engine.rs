use std::sync::Arc;

use crate::{
    database_sync_operations::{
        checkpoint_wal_file, connect, connect_untracked, db_bootstrap, push_logical_changes,
        reset_wal_file, transfer_logical_changes, transfer_physical_changes, wait_full_body,
        wal_pull, wal_push, WalPullResult,
    },
    database_tape::DatabaseTape,
    errors::Error,
    io_operations::IoOperations,
    protocol_io::ProtocolIO,
    types::{Coro, DatabaseMetadata},
    wal_session::WalSession,
    Result,
};

#[derive(Debug, Clone)]
pub struct DatabaseSyncEngineOpts {
    pub client_name: String,
    pub wal_pull_batch_size: u64,
}

pub struct DatabaseSyncEngine<P: ProtocolIO> {
    io: Arc<dyn turso_core::IO>,
    protocol: Arc<P>,
    draft_tape: DatabaseTape,
    draft_path: String,
    synced_path: String,
    meta_path: String,
    opts: DatabaseSyncEngineOpts,
    meta: Option<DatabaseMetadata>,
    // we remember information if Synced DB is dirty - which will make Database to reset it in case of any sync attempt
    // this bit is set to false when we properly reset Synced DB
    // this bit is set to true when we transfer changes from Draft to Synced or on initialization
    synced_is_dirty: bool,
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

impl<C: ProtocolIO> DatabaseSyncEngine<C> {
    /// Creates new instance of SyncEngine and initialize it immediately if no consistent local data exists
    pub async fn new(
        coro: &Coro,
        io: Arc<dyn turso_core::IO>,
        protocol: Arc<C>,
        path: &str,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        let draft_path = format!("{path}-draft");
        let draft_tape = io.open_tape(&draft_path, true)?;
        let mut db = Self {
            io,
            protocol,
            draft_tape,
            draft_path,
            synced_path: format!("{path}-synced"),
            meta_path: format!("{path}-info"),
            opts,
            meta: None,
            synced_is_dirty: true,
        };
        db.init(coro).await?;
        Ok(db)
    }

    /// Create database connection and appropriately configure it before use
    pub async fn connect(&self, coro: &Coro) -> Result<Arc<turso_core::Connection>> {
        connect(coro, &self.draft_tape).await
    }

    /// Sync all new changes from remote DB and apply them locally
    /// This method will **not** send local changed to the remote
    /// This method will block writes for the period of pull
    pub async fn pull(&mut self, coro: &Coro) -> Result<()> {
        tracing::info!(
            "pull: draft={}, synced={}",
            self.draft_path,
            self.synced_path
        );

        // reset Synced DB if it wasn't properly cleaned-up on previous "sync-method" attempt
        self.reset_synced_if_dirty(coro).await?;

        loop {
            // update Synced DB with fresh changes from remote
            let pull_result = self.pull_synced_from_remote(coro).await?;

            {
                // we will "replay" Synced WAL to the Draft WAL later without pushing it to the remote
                // so, we pass 'capture: true' as we need to preserve all changes for future push of WAL
                let synced = self.io.open_tape(&self.synced_path, true)?;

                // we will start wal write session for Draft DB in order to hold write lock during transfer of changes
                let mut draft_session = WalSession::new(connect(coro, &self.draft_tape).await?);
                draft_session.begin()?;

                // mark Synced as dirty as we will start transfer of logical changes there and if we will fail in the middle - we will need to cleanup Synced db
                self.synced_is_dirty = true;

                // transfer logical changes to the Synced DB in order to later execute physical "rebase" operation
                let client_id = &self.meta().client_unique_id;
                transfer_logical_changes(coro, &self.draft_tape, &synced, client_id, true).await?;

                // now we are ready to do the rebase: let's transfer physical changes from Synced to Draft
                let synced_wal_watermark = self.meta().synced_wal_match_watermark;
                let synced_sync_watermark = self.meta().synced_frame_no.expect(
                    "synced_frame_no must be set as we call pull_synced_from_remote before that",
                );
                let draft_wal_watermark = self.meta().draft_wal_match_watermark;
                let draft_sync_watermark = transfer_physical_changes(
                    coro,
                    &synced,
                    draft_session,
                    synced_wal_watermark,
                    synced_sync_watermark,
                    draft_wal_watermark,
                )
                .await?;
                update_meta(
                    coro,
                    self.protocol.as_ref(),
                    &self.meta_path,
                    &mut self.meta,
                    |m| {
                        m.draft_wal_match_watermark = draft_sync_watermark;
                        m.synced_wal_match_watermark = synced_sync_watermark;
                    },
                )
                .await?;
            }

            // Synced DB is 100% dirty now - let's reset it
            assert!(self.synced_is_dirty);
            self.reset_synced_if_dirty(coro).await?;

            let WalPullResult::NeedCheckpoint = pull_result else {
                break;
            };
            tracing::info!(
                "ready to checkpoint synced db file at {:?}, generation={}",
                self.synced_path,
                self.meta().synced_generation
            );
            {
                let synced = self.io.open_tape(&self.synced_path, false)?;
                checkpoint_wal_file(coro, &connect_untracked(&synced)?).await?;
                update_meta(
                    coro,
                    self.protocol.as_ref(),
                    &self.meta_path,
                    &mut self.meta,
                    |m| {
                        m.synced_generation += 1;
                        m.synced_frame_no = Some(0);
                        m.synced_wal_match_watermark = 0;
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Sync local changes to remote DB
    /// This method will **not** pull remote changes to the local DB
    /// This method will **not** block writes for the period of sync
    pub async fn push(&mut self, coro: &Coro) -> Result<()> {
        tracing::info!("push: draft={}", self.draft_path);

        let client_id = &self.meta().client_unique_id;
        push_logical_changes(coro, self.protocol.as_ref(), &self.draft_tape, client_id).await?;

        Ok(())
    }

    /// Sync local changes to remote DB and bring new changes from remote to local
    /// This method will block writes for the period of sync
    pub async fn sync(&mut self, coro: &Coro) -> Result<()> {
        // todo(sivukhin): this is bit suboptimal as both 'push' and 'pull' will call pull_synced_from_remote
        // but for now - keep it simple
        self.push(coro).await?;
        self.pull(coro).await?;
        Ok(())
    }

    async fn init(&mut self, coro: &Coro) -> Result<()> {
        tracing::info!(
            "initialize sync engine: draft={}, synced={}, opts={:?}",
            self.draft_path,
            self.synced_path,
            self.opts,
        );

        let completion = self.protocol.full_read(&self.meta_path)?;
        let data = wait_full_body(coro, &completion).await?;
        let meta = if data.is_empty() {
            None
        } else {
            Some(DatabaseMetadata::load(&data)?)
        };

        match meta {
            Some(meta) => {
                self.meta = Some(meta);
            }
            None => {
                let meta = self.bootstrap_db_files(coro).await?;
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");
                set_meta(
                    coro,
                    self.protocol.as_ref(),
                    &self.meta_path,
                    &mut self.meta,
                    meta,
                )
                .await?;
            }
        };

        let draft_exists = self.io.try_open(&self.draft_path)?.is_some();
        let synced_exists = self.io.try_open(&self.synced_path)?.is_some();
        if !draft_exists || !synced_exists {
            let error = "Draft or Synced files doesn't exists, but metadata is".to_string();
            return Err(Error::DatabaseSyncEngineError(error));
        }

        if self.meta().synced_frame_no.is_none() {
            // sync WAL from the remote in case of bootstrap - all subsequent initializations will be fast
            self.pull(coro).await?;
        }
        Ok(())
    }

    async fn pull_synced_from_remote(&mut self, coro: &Coro) -> Result<WalPullResult> {
        tracing::info!(
            "pull_synced_from_remote: draft={:?}, synced={:?}",
            self.draft_path,
            self.synced_path,
        );
        let synced = self.io.open_tape(&self.synced_path, false)?;
        let synced_conn = connect(coro, &synced).await?;
        let mut wal = WalSession::new(synced_conn);

        let generation = self.meta().synced_generation;
        let mut start_frame = self.meta().synced_frame_no.unwrap_or(0) + 1;
        loop {
            let end_frame = start_frame + self.opts.wal_pull_batch_size;
            let update = async |coro, frame_no| {
                update_meta(
                    coro,
                    self.protocol.as_ref(),
                    &self.meta_path,
                    &mut self.meta,
                    |m| m.synced_frame_no = Some(frame_no),
                )
                .await
            };
            match wal_pull(
                coro,
                self.protocol.as_ref(),
                &mut wal,
                generation,
                start_frame,
                end_frame,
                update,
            )
            .await?
            {
                WalPullResult::Done => return Ok(WalPullResult::Done),
                WalPullResult::NeedCheckpoint => return Ok(WalPullResult::NeedCheckpoint),
                WalPullResult::PullMore => {
                    start_frame = end_frame;
                    continue;
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn push_synced_to_remote(&mut self, coro: &Coro) -> Result<()> {
        tracing::info!(
            "push_synced_to_remote: draft={}, synced={}, id={}",
            self.draft_path,
            self.synced_path,
            self.meta().client_unique_id
        );
        let synced = self.io.open_tape(&self.synced_path, false)?;
        let synced_conn = connect(coro, &synced).await?;

        let mut wal = WalSession::new(synced_conn);
        wal.begin()?;

        // todo(sivukhin): push frames in multiple batches
        let generation = self.meta().synced_generation;
        let start_frame = self.meta().synced_frame_no.unwrap_or(0) + 1;
        let end_frame = wal.conn().wal_frame_count()? + 1;
        match wal_push(
            coro,
            self.protocol.as_ref(),
            &mut wal,
            None,
            generation,
            start_frame,
            end_frame,
        )
        .await
        {
            Ok(_) => {
                update_meta(
                    coro,
                    self.protocol.as_ref(),
                    &self.meta_path,
                    &mut self.meta,
                    |m| m.synced_frame_no = Some(end_frame - 1),
                )
                .await?;
                self.synced_is_dirty = false;
                Ok(())
            }
            Err(err) => {
                tracing::info!("push_synced_to_remote: failed: err={err}");
                Err(err)
            }
        }
    }

    async fn bootstrap_db_files(&mut self, coro: &Coro) -> Result<DatabaseMetadata> {
        assert!(
            self.meta.is_none(),
            "bootstrap_db_files must be called only when meta is not set"
        );
        tracing::info!(
            "bootstrap_db_files: draft={}, synced={}",
            self.draft_path,
            self.synced_path,
        );

        let start_time = std::time::Instant::now();
        // cleanup all files left from previous attempt to bootstrap
        // we shouldn't write any WAL files - but let's truncate them too for safety
        if let Some(file) = self.io.try_open(&self.draft_path)? {
            self.io.truncate(coro, file, 0).await?;
        }
        if let Some(file) = self.io.try_open(&self.synced_path)? {
            self.io.truncate(coro, file, 0).await?;
        }
        if let Some(file) = self.io.try_open(&format!("{}-wal", self.draft_path))? {
            self.io.truncate(coro, file, 0).await?;
        }
        if let Some(file) = self.io.try_open(&format!("{}-wal", self.synced_path))? {
            self.io.truncate(coro, file, 0).await?;
        }

        let files = &[
            self.io.create(&self.draft_path)?,
            self.io.create(&self.synced_path)?,
        ];
        let db_info = db_bootstrap(coro, self.protocol.as_ref(), files).await?;

        let elapsed = std::time::Instant::now().duration_since(start_time);
        tracing::info!(
            "bootstrap_db_files: finished draft={:?}, synced={:?}: elapsed={:?}",
            self.draft_path,
            self.synced_path,
            elapsed
        );

        Ok(DatabaseMetadata {
            client_unique_id: format!("{}-{}", self.opts.client_name, uuid::Uuid::new_v4()),
            synced_generation: db_info.current_generation,
            synced_frame_no: None,
            draft_wal_match_watermark: 0,
            synced_wal_match_watermark: 0,
        })
    }

    /// Reset WAL of Synced database which potentially can have some local changes
    async fn reset_synced_if_dirty(&mut self, coro: &Coro) -> Result<()> {
        tracing::info!(
            "reset_synced: synced_path={:?}, synced_is_dirty={}",
            self.synced_path,
            self.synced_is_dirty
        );
        // if we know that Synced DB is not dirty - let's skip this phase completely
        if !self.synced_is_dirty {
            return Ok(());
        }
        if let Some(synced_wal) = self.io.try_open(&format!("{}-wal", self.synced_path))? {
            reset_wal_file(coro, synced_wal, self.meta().synced_frame_no.unwrap_or(0)).await?;
        }
        self.synced_is_dirty = false;
        Ok(())
    }

    fn meta(&self) -> &DatabaseMetadata {
        self.meta.as_ref().expect("metadata must be set")
    }
}
