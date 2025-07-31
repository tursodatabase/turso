use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use crate::{
    database_tape::{
        DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts, DatabaseTape, DatabaseTapeOpts,
    },
    errors::Error,
    filesystem::Filesystem,
    metadata::DatabaseMetadata,
    sync_server::{Stream, SyncServer},
    types::DatabaseTapeOperation,
    wal_session::WalSession,
    Result,
};

pub struct DatabaseInner<S: SyncServer, F: Filesystem> {
    filesystem: F,
    sync_server: S,
    draft_path: PathBuf,
    synced_path: PathBuf,
    meta_path: PathBuf,
    meta: Option<DatabaseMetadata>,
    // we remember information if Synced DB is dirty - which will make Database to reset it in case of any sync attempt
    // this bit is set to false when we properly reset Synced DB
    // this bit is set to true when we transfer changes from Draft to Synced or on initialization
    synced_is_dirty: bool,
}

pub const WAL_HEADER: usize = 32;
pub const WAL_FRAME_HEADER: usize = 24;
const PAGE_SIZE: usize = 4096;
const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER + PAGE_SIZE;

impl<S: SyncServer, F: Filesystem> DatabaseInner<S, F> {
    pub async fn new(filesystem: F, sync_server: S, path: &Path) -> Result<Self> {
        let path_str = path
            .to_str()
            .ok_or_else(|| Error::DatabaseSyncError(format!("invalid path: {path:?}")))?;
        let draft_path = PathBuf::from(format!("{path_str}-draft"));
        let synced_path = PathBuf::from(format!("{path_str}-synced"));
        let meta_path = PathBuf::from(format!("{path_str}-info"));
        let mut db = Self {
            sync_server,
            filesystem,
            draft_path,
            synced_path,
            meta_path,
            meta: None,
            synced_is_dirty: true,
        };
        db.init().await?;
        Ok(db)
    }

    pub async fn connect(&self) -> Result<turso::Connection> {
        let db = self.open_draft().await?;
        db.connect().await
    }

    /// Sync any new changes from remote DB and apply them locally
    /// This method will **not** send local changed to the remote
    /// This method will block writes for the period of sync
    pub async fn pull(&mut self) -> Result<()> {
        tracing::debug!("sync_from_remote");
        self.cleanup_synced().await?;

        self.pull_synced_from_remote().await?;
        // we will "replay" Synced WAL to the Draft WAL later without pushing it to the remote
        // so, we pass 'capture: true' as we need to preserve all changes for future push of WAL
        let _ = self.transfer_draft_to_synced(true).await?;
        assert!(
            self.synced_is_dirty,
            "synced_is_dirty must be set after transfer_draft_to_synced"
        );

        self.transfer_synced_to_draft().await?;

        // Synced DB now has extra WAL frames from [transfer_draft_to_synced] call, so we need to reset them
        self.reset_synced().await?;
        assert!(
            !self.synced_is_dirty,
            "synced_is_dirty must not be set after reset_synced"
        );
        Ok(())
    }

    /// Sync local changes to remote DB
    /// This method will **not** pull remote changes to the local DB
    /// This method will **not** block writes for the period of sync
    pub async fn push(&mut self) -> Result<()> {
        tracing::debug!("sync to remote");
        self.cleanup_synced().await?;

        self.pull_synced_from_remote().await?;

        let change_id = self.transfer_draft_to_synced(false).await?;
        // update transferred_change_id field because after we will start pushing frames - we must be able to resume this operation
        // otherwise, we will encounter conflicts because some frames will be pushed while we will think that they are not
        self.meta = Some(
            self.write_meta(|meta| meta.transferred_change_id = change_id)
                .await?,
        );
        self.push_synced_to_remote(change_id).await?;
        Ok(())
    }

    /// Sync local changes to remote DB and bring new changes from remote to local
    /// This method will block writes for the period of sync
    pub async fn sync(&mut self) -> Result<()> {
        // todo(sivukhin): this is bit suboptimal as both 'push' and 'pull' will call pull_synced_from_remote
        // but for now - keep it simple
        self.push().await?;
        self.pull().await?;
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        tracing::debug!("initialize synced database instance");

        let meta = DatabaseMetadata::read_from(&self.filesystem, &self.meta_path).await?;
        let bootstrapped = match meta {
            Some(meta) => {
                self.meta = Some(meta);
                false
            }
            None => {
                let meta = self.bootstrap_db_files().await?;

                tracing::debug!("write meta after successful bootstrap");
                meta.write_to(&self.filesystem, &self.meta_path).await?;
                self.meta = Some(meta);
                true
            }
        };

        let draft_exists = self.filesystem.exists_file(&self.draft_path).await?;
        let synced_exists = self.filesystem.exists_file(&self.synced_path).await?;
        if !draft_exists || !synced_exists {
            return Err(Error::DatabaseSyncError(
                "Draft or Synced files doesn't exists, but metadata is".to_string(),
            ));
        }

        if bootstrapped {
            // sync WAL from the remote in case of bootstrap - all subsequent initializations will be fast
            self.pull().await?;
        }
        Ok(())
    }

    async fn open_synced(&self, capture: bool) -> Result<DatabaseTape> {
        let clean_path_str = self.synced_path.to_str().unwrap();
        let clean = turso::Builder::new_local(clean_path_str).build().await?;
        let opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some(if capture { "after" } else { "off" }.to_string()),
        };
        tracing::debug!("initialize clean database connection");
        Ok(DatabaseTape::new_with_opts(clean, opts))
    }

    async fn open_draft(&self) -> Result<DatabaseTape> {
        let draft_path_str = self.draft_path.to_str().unwrap();
        let draft = turso::Builder::new_local(draft_path_str).build().await?;
        let opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("after".to_string()),
        };
        tracing::debug!("initialize draft database connection");
        Ok(DatabaseTape::new_with_opts(draft, opts))
    }

    async fn bootstrap_db_files(&mut self) -> Result<DatabaseMetadata> {
        assert!(
            self.meta.is_none(),
            "bootstrap_db_files must be called only when meta is not set"
        );
        if self.filesystem.exists_file(&self.draft_path).await? {
            self.filesystem.remove_file(&self.draft_path).await?;
        }
        if self.filesystem.exists_file(&self.synced_path).await? {
            self.filesystem.remove_file(&self.synced_path).await?;
        }

        let info = self.sync_server.db_info().await?;
        let mut synced_file = self.filesystem.create_file(&self.synced_path).await?;

        let start_time = tokio::time::Instant::now();
        let mut written_bytes = 0;
        tracing::debug!("start bootstrapping Synced file from remote");

        let mut bootstrap = self.sync_server.db_export(info.current_generation).await?;
        while let Some(chunk) = bootstrap.read_chunk().await? {
            self.filesystem.write_file(&mut synced_file, &chunk).await?;
            written_bytes += chunk.len();
        }

        let elapsed = tokio::time::Instant::now().duration_since(start_time);
        tracing::debug!(
            "finish bootstrapping Synced file from remote: written_bytes={}, elapsed={:?}",
            written_bytes,
            elapsed
        );

        self.filesystem
            .copy_file(&self.synced_path, &self.draft_path)
            .await?;
        tracing::debug!("copied Synced file to Draft");

        Ok(DatabaseMetadata {
            synced_generation: info.current_generation,
            synced_frame_no: 0,
            synced_change_id: None,
            transferred_change_id: None,
            draft_wal_match_watermark: 0,
            synced_wal_match_watermark: 0,
        })
    }

    async fn write_meta(&self, update: impl Fn(&mut DatabaseMetadata)) -> Result<DatabaseMetadata> {
        let mut meta = self.meta().clone();
        update(&mut meta);
        // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
        meta.write_to(&self.filesystem, &self.meta_path).await?;
        Ok(meta)
    }

    /// Pull updates from remote to the Synced database
    /// This method will update Synced database WAL frames and [DatabaseMetadata::synced_frame_no] metadata field
    async fn pull_synced_from_remote(&mut self) -> Result<()> {
        tracing::debug!("pull_synced_from_remote");

        let (generation, mut frame_no) = {
            let meta = self.meta();
            (meta.synced_generation, meta.synced_frame_no)
        };

        // open fresh connection to the Clean database in order to initiate WAL session
        let clean = self.open_synced(false).await?;
        let clean_conn = clean.connect().await?;

        let mut wal_session = WalSession::new(clean_conn);
        let mut buffer = Vec::with_capacity(WAL_FRAME_SIZE);
        loop {
            tracing::debug!(
                "pull clean wal portion: generation={}, frame={}",
                generation,
                frame_no + 1
            );
            let pull = self.sync_server.wal_pull(generation, frame_no + 1).await;

            let mut data = match pull {
                Ok(data) => data,
                Err(Error::PullNeedCheckpoint(status))
                    if status.generation == generation && status.max_frame_no == frame_no =>
                {
                    tracing::debug!("end of history reached for database: status={:?}", status);
                    break;
                }
                Err(e @ Error::PullNeedCheckpoint(..)) => {
                    // todo(sivukhin): temporary not supported - will implement soon after TRUNCATE checkpoint will be merged to turso-db
                    return Err(e);
                }
                Err(e) => return Err(e),
            };
            while let Some(mut chunk) = data.read_chunk().await? {
                // chunk is arbitrary - aggregate groups of FRAME_SIZE bytes out from the chunks stream
                while !chunk.is_empty() {
                    let to_fill = WAL_FRAME_SIZE - buffer.len();
                    let prefix = chunk.split_to(to_fill.min(chunk.len()));
                    buffer.extend_from_slice(&prefix);
                    assert!(
                        buffer.capacity() == WAL_FRAME_SIZE,
                        "buffer should not extend its capacity"
                    );
                    if buffer.len() < WAL_FRAME_SIZE {
                        continue;
                    }
                    frame_no += 1;
                    if !wal_session.in_txn() {
                        wal_session.begin()?;
                    }
                    let wal_insert_info = wal_session
                        .conn()
                        .wal_insert_frame(frame_no as u32, &buffer)?;
                    if wal_insert_info.is_commit_frame() {
                        wal_session.end()?;
                        // transaction boundary reached - it's safe to commit progress
                        self.meta = Some(self.write_meta(|m| m.synced_frame_no = frame_no).await?);
                    }
                    buffer.clear();
                }
            }
        }
        Ok(())
    }

    async fn push_synced_to_remote(&mut self, change_id: Option<i64>) -> Result<()> {
        tracing::debug!("push_synced_to_remote");
        match self.do_push_synced_to_remote().await {
            Ok(()) => {
                self.meta = Some(
                    self.write_meta(|meta| {
                        meta.synced_change_id = change_id;
                        meta.transferred_change_id = None;
                    })
                    .await?,
                );
                Ok(())
            }
            Err(err @ Error::PushConflict) => {
                tracing::info!("push_synced_to_remote: conflict detected, rollback local changes");
                // we encountered conflict - which means that other client pushed something to the WAL before us
                // as we were unable to insert any frame to the remote WAL - it's safe to reset our state completely
                self.meta = Some(
                    self.write_meta(|meta| meta.transferred_change_id = None)
                        .await?,
                );
                self.reset_synced().await?;
                Err(err)
            }
            Err(err) => {
                tracing::info!("err: {}", err);
                Err(err)
            }
        }
    }

    async fn do_push_synced_to_remote(&mut self) -> Result<()> {
        tracing::debug!("do_push_synced_to_remote");

        let (generation, frame_no) = {
            let meta = self.meta();
            (meta.synced_generation, meta.synced_frame_no)
        };

        let synced = self.open_synced(false).await?;
        let synced_conn = synced.connect().await?;

        // todo(sivukhin): push frames in multiple batches
        let mut frames = Vec::new();
        let mut frames_cnt = 0;
        {
            let mut wal_session = WalSession::new(synced_conn);
            wal_session.begin()?;

            let synced_frames = wal_session.conn().wal_frame_count()? as usize;

            let mut buffer = [0u8; WAL_FRAME_SIZE];
            for frame_no in (frame_no + 1)..=synced_frames {
                let frame_info = wal_session
                    .conn()
                    .wal_get_frame(frame_no as u32, &mut buffer)?;
                tracing::trace!("collect frame {}/{:?} for push", frame_no, frame_info);
                frames.extend_from_slice(&buffer);
                frames_cnt += 1;
            }
        }

        if frames_cnt == 0 {
            return Ok(());
        }

        tracing::debug!(
            "push frames to remote {}/{}/{}",
            generation,
            frame_no + 1,
            frame_no + frames_cnt + 1
        );
        self.sync_server
            .wal_push(
                None,
                generation,
                frame_no + 1,
                frame_no + frames_cnt + 1,
                frames,
            )
            .await?;
        self.meta = Some(
            self.write_meta(|meta| meta.synced_frame_no = frame_no + frames_cnt)
                .await?,
        );

        Ok(())
    }

    /// Transfers row changes from Draft DB to the Clean DB
    async fn transfer_draft_to_synced(&mut self, capture: bool) -> Result<Option<i64>> {
        tracing::debug!("transfer_draft_to_synced");
        self.synced_is_dirty = true;

        let draft = self.open_draft().await?;
        let synced = self.open_synced(capture).await?;
        let opts = DatabaseChangesIteratorOpts {
            first_change_id: self.meta().synced_change_id.map(|x| x + 1),
            mode: DatabaseChangesIteratorMode::Apply,
            ..Default::default()
        };
        let mut last_change_id = self.meta().synced_change_id;
        let mut session = synced.start_replay_session().await?;
        let mut changes = draft.iterate_changes(opts).await?;
        while let Some(operation) = changes.next().await? {
            if let DatabaseTapeOperation::RowChange(change) = &operation {
                assert!(
                    last_change_id.is_none() || last_change_id.unwrap() < change.change_id,
                    "change id must be strictly increasing: last_change_id={:?}, change.change_id={}",
                    last_change_id, change.change_id
                );
                // we give user full control over CDC table - so let's not emit assert here for now
                if last_change_id.is_some() && last_change_id.unwrap() + 1 != change.change_id {
                    tracing::warn!(
                        "out of order change sequence: {} -> {}",
                        last_change_id.unwrap(),
                        change.change_id
                    );
                }
                last_change_id = Some(change.change_id);
            }
            session.replay(operation).await?;
        }

        Ok(last_change_id)
    }

    async fn cleanup_synced(&mut self) -> Result<()> {
        tracing::debug!("cleanup_synced");

        if let Some(change_id) = self.meta().transferred_change_id {
            tracing::info!("some changes was transferred to the Synced DB but wasn't properly pushed to the remote");
            match self.push_synced_to_remote(Some(change_id)).await {
                // ignore Ok and Error::PushConflict - because in this case we sucessfully finalized previous operation
                Ok(()) | Err(Error::PushConflict) => {}
                Err(err) => return Err(err),
            }
        }
        // if we failed in the middle before - let's reset Synced DB if necessary
        // if everything works without error - we will properly set is_synced_dirty flag and this function will be no-op
        self.reset_synced().await?;
        Ok(())
    }

    async fn transfer_synced_to_draft(&mut self) -> Result<()> {
        tracing::debug!("transfer_synced_to_draft");

        let synced = self.open_synced(false).await?;
        let synced_conn = synced.connect().await?;
        let mut synced_session = WalSession::new(synced_conn.clone());
        synced_session.begin()?;

        let draft_watermark = self.meta().draft_wal_match_watermark as u32;
        let synced_watermark = self.meta().synced_wal_match_watermark as u32;
        let synced_frames = self.meta().synced_frame_no as u32;
        let synced_all_frames = synced_conn.wal_frame_count()? as u32;
        assert!(
            synced_all_frames > synced_watermark,
            "some changes must be in Synced DB for transfer"
        );
        assert!(
            (synced_watermark..=synced_all_frames).contains(&synced_frames),
            "synced_watermark={}, synced_all_frames={}, synced_frames={}",
            synced_watermark,
            synced_all_frames,
            synced_frames
        );

        let draft_frames = {
            let draft = self.open_draft().await?;
            let mut draft_session = draft.start_wal_session().await?;
            let _ = draft_session.rollback_changes_after(draft_watermark)?;
            let mut last_frame_info = None;
            let mut frame = vec![0u8; WAL_FRAME_SIZE];
            let mut draft_frames = draft_session.frames_count()?;
            for synced_frame_no in synced_watermark + 1..=synced_all_frames {
                let frame_info = synced_conn.wal_get_frame(synced_frame_no, &mut frame)?;
                tracing::trace!("append page {} to Draft DB", frame_info.page_no);
                draft_session.append_page(frame_info.page_no, &frame[WAL_FRAME_HEADER..])?;
                if synced_frame_no == synced_frames {
                    draft_frames = draft_session.frames_count()? + 1;
                }
                last_frame_info = Some(frame_info);
            }
            let db_size = last_frame_info.unwrap().db_size;
            tracing::trace!("commit WAL session to Draft DB with db_size={db_size}");
            draft_session.commit(db_size)?;
            assert!(draft_frames != 0);
            draft_frames
        };

        self.meta = Some(
            self.write_meta(|meta| {
                meta.draft_wal_match_watermark = draft_frames;
                meta.synced_wal_match_watermark = synced_frames as u64;
                meta.synced_change_id = None;
            })
            .await?,
        );

        Ok(())
    }

    /// Reset WAL of Synced database which potentially can have some local changes
    async fn reset_synced(&mut self) -> Result<()> {
        tracing::debug!("reset_synced");

        // if we know that Clean DB is not dirty - let's skip this phase completely
        if !self.synced_is_dirty {
            return Ok(());
        }

        let clean_path_str = self.synced_path.to_str().unwrap_or("");
        let clean_wal_path = PathBuf::from(format!("{clean_path_str}-wal"));
        let wal_size = WAL_HEADER + WAL_FRAME_SIZE * self.meta().synced_frame_no;
        tracing::debug!(
            "reset Synced DB WAL to the size of {} frames",
            self.meta().synced_frame_no
        );
        match self.filesystem.open_file(&clean_wal_path).await {
            Ok(clean_wal) => {
                self.filesystem.truncate_file(&clean_wal, wal_size).await?;
            }
            Err(Error::FilesystemError(err)) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }

        self.synced_is_dirty = false;
        Ok(())
    }

    fn meta(&self) -> &DatabaseMetadata {
        self.meta.as_ref().expect("metadata must be set")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::RngCore;
    use tokio::sync::Mutex;
    use turso::Value;

    use crate::{
        database_inner::DatabaseInner,
        errors::Error,
        filesystem::test::TestFilesystem,
        sync_server::test::{convert_rows, TestSyncServer, TestSyncServerOpts},
        test_context::{FaultInjectionStrategy, TestContext},
        tests::{deterministic_runtime, seed_u64},
        Result,
    };

    async fn query_rows(conn: &turso::Connection, sql: &str) -> Result<Vec<Vec<Value>>> {
        let mut rows = conn.query(sql, ()).await?;
        convert_rows(&mut rows).await
    }

    #[test]
    pub fn test_sync_single_db_simple() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let server_path = dir.path().join("server.db");
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));
            let server = TestSyncServer::new(ctx.clone(), &server_path, opts)
                .await
                .unwrap();
            let fs = TestFilesystem::new(ctx.clone());
            let local_path = dir.path().join("local.db");
            let mut db = DatabaseInner::new(fs, server.clone(), &local_path)
                .await
                .unwrap();

            server
                .execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", ())
                .await
                .unwrap();
            server
                .execute("INSERT INTO t VALUES (1)", ())
                .await
                .unwrap();

            // no table in schema before sync from remote (as DB was initialized when remote was empty)
            assert!(matches!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t").await,
                Err(Error::TursoError(turso::Error::SqlExecutionFailure(x))) if x.contains("no such table: t")
            ));

            // 1 rows synced
            db.pull().await.unwrap();
            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![vec![Value::Integer(1)]]
            );

            server
                .execute("INSERT INTO t VALUES (2)", ())
                .await
                .unwrap();

            let conn = db.connect().await.unwrap();
            conn.execute("INSERT INTO t VALUES (3)", ()).await.unwrap();

            // changes are synced from the remote - but remote changes are not propagated locally
            db.push().await.unwrap();
            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
            );

            let server_db = server.db();
            let server_conn = server_db.connect().unwrap();
            assert_eq!(
                convert_rows(&mut server_conn.query("SELECT * FROM t", ()).await.unwrap())
                    .await
                    .unwrap(),
                vec![
                    vec![Value::Integer(1)],
                    vec![Value::Integer(2)],
                    vec![Value::Integer(3)],
                ]
            );

            let conn = db.connect().await.unwrap();
            conn.execute("INSERT INTO t VALUES (4)", ()).await.unwrap();
            db.push().await.unwrap();

            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![
                    vec![Value::Integer(1)],
                    vec![Value::Integer(3)],
                    vec![Value::Integer(4)]
                ]
            );

            assert_eq!(
                convert_rows(&mut server_conn.query("SELECT * FROM t", ()).await.unwrap())
                    .await
                    .unwrap(),
                vec![
                    vec![Value::Integer(1)],
                    vec![Value::Integer(2)],
                    vec![Value::Integer(3)],
                    vec![Value::Integer(4)],
                ]
            );

            db.pull().await.unwrap();
            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![
                    vec![Value::Integer(1)],
                    vec![Value::Integer(2)],
                    vec![Value::Integer(3)],
                    vec![Value::Integer(4)]
                ]
            );

            assert_eq!(
                convert_rows(&mut server_conn.query("SELECT * FROM t", ()).await.unwrap())
                    .await
                    .unwrap(),
                vec![
                    vec![Value::Integer(1)],
                    vec![Value::Integer(2)],
                    vec![Value::Integer(3)],
                    vec![Value::Integer(4)],
                ]
            );
        });
    }

    #[test]
    pub fn test_sync_single_db_full_syncs() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let dir = dir.keep();
            tracing::info!("dir: {:?}", dir);
            let server_path = dir.join("server.db");
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));
            let server = TestSyncServer::new(ctx.clone(), &server_path, opts)
                .await
                .unwrap();
            let fs = TestFilesystem::new(ctx.clone());
            let local_path = dir.join("local.db");
            let mut db = DatabaseInner::new(fs, server.clone(), &local_path)
                .await
                .unwrap();

            server
                .execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", ())
                .await
                .unwrap();
            server
                .execute("INSERT INTO t VALUES (1)", ())
                .await
                .unwrap();

            // no table in schema before sync from remote (as DB was initialized when remote was empty)
            assert!(matches!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t").await,
                Err(Error::TursoError(turso::Error::SqlExecutionFailure(x))) if x.contains("no such table: t")
            ));

            db.sync().await.unwrap();
            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![vec![Value::Integer(1)]]
            );

            let conn = db.connect().await.unwrap();
            conn.execute("INSERT INTO t VALUES (2)", ()).await.unwrap();
            db.sync().await.unwrap();
            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
            );

            let conn = db.connect().await.unwrap();
            conn.execute("INSERT INTO t VALUES (3)", ()).await.unwrap();
            db.sync().await.unwrap();
            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![
                    vec![Value::Integer(1)],
                    vec![Value::Integer(2)],
                    vec![Value::Integer(3)]
                ]
            );
        });
    }

    #[test]
    pub fn test_sync_single_db_one_full_sync() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let dir = dir.keep();
            tracing::info!("dir: {:?}", dir);
            let server_path = dir.join("server.db");
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));
            let server = TestSyncServer::new(ctx.clone(), &server_path, opts)
                .await
                .unwrap();
            server
                .execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", ())
                .await
                .unwrap();

            let fs = TestFilesystem::new(ctx.clone());
            let local_path = dir.join("local.db");
            let mut db = DatabaseInner::new(fs, server.clone(), &local_path)
                .await
                .unwrap();

            let conn = db.connect().await.unwrap();
            conn.execute("INSERT INTO t VALUES (1)", ()).await.unwrap();
            db.sync().await.unwrap();
            assert_eq!(
                query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap(),
                vec![vec![Value::Integer(1)]]
            );
        });
    }

    #[test]
    pub fn test_sync_multiple_dbs_conflict() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let server_path = dir.path().join("server.db");
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));
            let server = TestSyncServer::new(ctx.clone(), &server_path, opts)
                .await
                .unwrap();
            let mut dbs = Vec::new();
            const CLIENTS: usize = 8;
            for i in 0..CLIENTS {
                let db = DatabaseInner::new(
                    TestFilesystem::new(ctx.clone()),
                    server.clone(),
                    &dir.path().join(format!("local-{i}.db")),
                )
                .await
                .unwrap();
                dbs.push(Arc::new(Mutex::new(db)));
            }

            server
                .execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", ())
                .await
                .unwrap();

            for db in &mut dbs {
                let mut db = db.lock().await;
                db.pull().await.unwrap();
            }
            for (i, db) in dbs.iter().enumerate() {
                let db = db.lock().await;
                let conn = db.connect().await.unwrap();
                conn.execute("INSERT INTO t VALUES (?)", (i as i32,))
                    .await
                    .unwrap();
            }

            let try_sync = || async {
                let mut tasks = Vec::new();
                for db in &dbs {
                    let db = db.clone();
                    tasks.push(async move {
                        let mut db = db.lock().await;
                        db.push().await
                    });
                }
                futures::future::join_all(tasks).await
            };
            for attempt in 0..CLIENTS {
                let results = try_sync().await;
                tracing::info!("attempt #{}: {:?}", attempt, results);
                assert!(results.iter().filter(|x| x.is_ok()).count() >= attempt);
            }
        });
    }

    #[test]
    pub fn test_sync_multiple_clients_no_conflicts_synchronized() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let server_path = dir.path().join("server.db");
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));
            let server = TestSyncServer::new(ctx.clone(), &server_path, opts)
                .await
                .unwrap();

            server
                .execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v)", ())
                .await
                .unwrap();

            let sync_lock = Arc::new(tokio::sync::Mutex::new(()));
            let mut clients = Vec::new();
            const CLIENTS: usize = 10;
            let mut expected_rows = Vec::new();
            for i in 0..CLIENTS {
                let mut queries = Vec::new();
                let cnt = ctx.rng().await.next_u32() % CLIENTS as u32 + 1;
                for q in 0..cnt {
                    let key = i * CLIENTS + q as usize;
                    let length = ctx.rng().await.next_u32() % 4096;
                    queries.push(format!(
                        "INSERT INTO t VALUES ({key}, randomblob({length}))",
                    ));
                    expected_rows.push(vec![
                        Value::Integer(key as i64),
                        Value::Integer(length as i64),
                    ]);
                }
                clients.push(tokio::spawn({
                    let dir = dir.path().to_path_buf().clone();
                    let ctx = ctx.clone();
                    let server = server.clone();
                    let sync_lock = sync_lock.clone();
                    async move {
                        let local_path = dir.join(format!("local-{i}.db"));
                        let fs = TestFilesystem::new(ctx.clone());
                        let mut db = DatabaseInner::new(fs, server.clone(), &local_path)
                            .await
                            .unwrap();

                        db.pull().await.unwrap();
                        for query in queries {
                            let conn = db.connect().await.unwrap();
                            conn.execute(&query, ()).await.unwrap();
                        }
                        let guard = sync_lock.lock().await;
                        db.push().await.unwrap();
                        drop(guard);
                    }
                }));
            }
            for client in clients {
                client.await.unwrap();
            }
            let db = server.db();
            let conn = db.connect().unwrap();
            let mut result = conn.query("SELECT k, length(v) FROM t", ()).await.unwrap();
            let rows = convert_rows(&mut result).await.unwrap();
            assert_eq!(rows, expected_rows);
        });
    }

    #[test]
    pub fn test_sync_single_db_sync_from_remote_nothing_single_failure() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let server_path = dir.path().join("server.db");
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));
            let server = TestSyncServer::new(ctx.clone(), &server_path, opts)
                .await
                .unwrap();

            server.execute("CREATE TABLE t(x)", ()).await.unwrap();
            server
                .execute("INSERT INTO t VALUES (1), (2), (3)", ())
                .await
                .unwrap();

            let mut session = ctx.fault_session();
            let mut it = 0;
            while let Some(strategy) = session.next().await {
                let local_path = dir.path().join(format!("local-{it}.db"));
                it += 1;

                let fs = TestFilesystem::new(ctx.clone());
                let mut db = DatabaseInner::new(fs, server.clone(), &local_path)
                    .await
                    .unwrap();

                let has_fault = matches!(strategy, FaultInjectionStrategy::Enabled { .. });

                ctx.switch_mode(strategy).await;
                let result = db.pull().await;
                ctx.switch_mode(FaultInjectionStrategy::Disabled).await;

                if !has_fault {
                    result.unwrap();
                } else {
                    let err = result.err().unwrap();
                    tracing::info!("error after fault injection: {}", err);
                }

                let rows = query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap();
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Integer(1)],
                        vec![Value::Integer(2)],
                        vec![Value::Integer(3)],
                    ]
                );

                db.pull().await.unwrap();

                let rows = query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap();
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Integer(1)],
                        vec![Value::Integer(2)],
                        vec![Value::Integer(3)],
                    ]
                );
            }
        });
    }

    #[test]
    pub fn test_sync_single_db_sync_from_remote_single_failure() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));

            let mut session = ctx.fault_session();
            let mut it = 0;
            while let Some(strategy) = session.next().await {
                let server_path = dir.path().join(format!("server-{it}.db"));
                let server = TestSyncServer::new(ctx.clone(), &server_path, opts.clone())
                    .await
                    .unwrap();

                server.execute("CREATE TABLE t(x)", ()).await.unwrap();

                let local_path = dir.path().join(format!("local-{it}.db"));
                it += 1;

                let fs = TestFilesystem::new(ctx.clone());
                let mut db = DatabaseInner::new(fs, server.clone(), &local_path)
                    .await
                    .unwrap();

                server
                    .execute("INSERT INTO t VALUES (1), (2), (3)", ())
                    .await
                    .unwrap();

                let has_fault = matches!(strategy, FaultInjectionStrategy::Enabled { .. });

                ctx.switch_mode(strategy).await;
                let result = db.pull().await;
                ctx.switch_mode(FaultInjectionStrategy::Disabled).await;

                if !has_fault {
                    result.unwrap();
                } else {
                    let err = result.err().unwrap();
                    tracing::info!("error after fault injection: {}", err);
                }

                let rows = query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap();
                assert!(rows.len() <= 3);

                db.pull().await.unwrap();

                let rows = query_rows(&db.connect().await.unwrap(), "SELECT * FROM t")
                    .await
                    .unwrap();
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Integer(1)],
                        vec![Value::Integer(2)],
                        vec![Value::Integer(3)],
                    ]
                );
            }
        });
    }

    #[test]
    pub fn test_sync_single_db_sync_to_remote_single_failure() {
        deterministic_runtime(async || {
            let dir = tempfile::TempDir::new().unwrap();
            let opts = TestSyncServerOpts { pull_batch_size: 1 };
            let ctx = Arc::new(TestContext::new(seed_u64()));

            let mut session = ctx.fault_session();
            let mut it = 0;
            while let Some(strategy) = session.next().await {
                let server_path = dir.path().join(format!("server-{it}.db"));
                let server = TestSyncServer::new(ctx.clone(), &server_path, opts.clone())
                    .await
                    .unwrap();

                server
                    .execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", ())
                    .await
                    .unwrap();

                server
                    .execute("INSERT INTO t VALUES (1)", ())
                    .await
                    .unwrap();

                let local_path = dir.path().join(format!("local-{it}.db"));
                it += 1;

                let fs = TestFilesystem::new(ctx.clone());
                let mut db = DatabaseInner::new(fs, server.clone(), &local_path)
                    .await
                    .unwrap();

                let conn = db.connect().await.unwrap();
                conn.execute("INSERT INTO t VALUES (2), (3)", ())
                    .await
                    .unwrap();

                let has_fault = matches!(strategy, FaultInjectionStrategy::Enabled { .. });

                ctx.switch_mode(strategy).await;
                let result = db.push().await;
                ctx.switch_mode(FaultInjectionStrategy::Disabled).await;

                if !has_fault {
                    result.unwrap();
                } else {
                    let err = result.err().unwrap();
                    tracing::info!("error after fault injection: {}", err);
                }

                let server_db = server.db();
                let server_conn = server_db.connect().unwrap();
                let rows =
                    convert_rows(&mut server_conn.query("SELECT * FROM t", ()).await.unwrap())
                        .await
                        .unwrap();
                assert!(rows.len() <= 3);

                db.push().await.unwrap();

                let rows =
                    convert_rows(&mut server_conn.query("SELECT * FROM t", ()).await.unwrap())
                        .await
                        .unwrap();
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Integer(1)],
                        vec![Value::Integer(2)],
                        vec![Value::Integer(3)],
                    ]
                );
            }
        });
    }
}
