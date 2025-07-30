use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use bytes::Bytes;
use turso_core::{Buffer, Completion, OpenFlags};

use crate::{
    database_tape::{
        DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts, DatabaseReplaySessionOpts,
        DatabaseTape, DatabaseTapeOpts,
    },
    errors::Error,
    types::{Coro, DatabaseMetadata, DatabaseTapeOperation, ProtocolResume, ProtocolYield},
    wal_session::WalSession,
    Result,
};

pub struct DatabaseInner {
    io: Arc<dyn turso_core::IO>,
    draft_path: PathBuf,
    synced_path: PathBuf,
    meta_path: PathBuf,
    meta: Option<DatabaseMetadata>,
    // we remember information if Synced DB is dirty - which will make Database to reset it in case of any sync attempt
    // this bit is set to false when we properly reset Synced DB
    // this bit is set to true when we transfer changes from Draft to Synced or on initialization
    synced_is_dirty: bool,
}

unsafe impl Send for DatabaseInner {}
unsafe impl Sync for DatabaseInner {}

pub const WAL_HEADER: usize = 32;
pub const WAL_FRAME_HEADER: usize = 24;
const PAGE_SIZE: usize = 4096;
const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER + PAGE_SIZE;

impl DatabaseInner {
    pub async fn new(coro: &Coro, io: Arc<dyn turso_core::IO>, path: &Path) -> Result<Self> {
        let path_str = path
            .to_str()
            .ok_or_else(|| Error::DatabaseSyncError(format!("invalid path: {path:?}")))?;
        let draft_path = PathBuf::from(format!("{path_str}-draft"));
        let synced_path = PathBuf::from(format!("{path_str}-synced"));
        let meta_path = PathBuf::from(format!("{path_str}-info"));
        let mut db = Self {
            io,
            draft_path,
            synced_path,
            meta_path,
            meta: None,
            synced_is_dirty: true,
        };
        db.init(coro).await?;
        Ok(db)
    }

    pub async fn connect(&self, coro: &Coro) -> Result<Arc<turso_core::Connection>> {
        let db = self.open_draft()?;
        db.connect(coro).await
    }

    /// Sync any new changes from remote DB and apply them locally
    /// This method will **not** send local changed to the remote
    /// This method will block writes for the period of sync
    pub async fn pull(&mut self, coro: &Coro) -> Result<()> {
        tracing::debug!("sync_from_remote");
        self.cleanup_synced(coro).await?;

        self.pull_synced_from_remote(coro).await?;
        // we will "replay" Synced WAL to the Draft WAL later without pushing it to the remote
        // so, we pass 'capture: true' as we need to preserve all changes for future push of WAL
        let _ = self.transfer_draft_to_synced(coro, true).await?;
        assert!(
            self.synced_is_dirty,
            "synced_is_dirty must be set after transfer_draft_to_synced"
        );

        self.transfer_synced_to_draft(coro).await?;

        // Synced DB now has extra WAL frames from [transfer_draft_to_synced] call, so we need to reset them
        self.reset_synced(coro).await?;
        assert!(
            !self.synced_is_dirty,
            "synced_is_dirty must not be set after reset_synced"
        );
        Ok(())
    }

    /// Sync local changes to remote DB
    /// This method will **not** pull remote changes to the local DB
    /// This method will **not** block writes for the period of sync
    pub async fn push(&mut self, coro: &Coro) -> Result<()> {
        tracing::debug!("sync to remote");
        self.cleanup_synced(coro).await?;

        self.pull_synced_from_remote(coro).await?;

        let change_id = self.transfer_draft_to_synced(coro, false).await?;
        // update transferred_change_id field because after we will start pushing frames - we must be able to resume this operation
        // otherwise, we will encounter conflicts because some frames will be pushed while we will think that they are not
        self.meta = Some(
            self.write_meta(coro, |meta| meta.transferred_change_id = change_id)
                .await?,
        );
        self.push_synced_to_remote(coro, change_id).await?;
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
        tracing::debug!("initialize synced database instance");

        let yield_op = ProtocolYield::ReadFile {
            path: self.meta_path.clone(),
        };
        let meta = coro.yield_(yield_op).await?.content();
        let meta = meta.as_deref().map(DatabaseMetadata::load).transpose()?;

        match meta {
            Some(meta) => {
                self.meta = Some(meta);
            }
            None => {
                let meta = self.bootstrap_db_files(coro).await?;

                tracing::debug!("write meta after successful bootstrap");
                let yield_op = ProtocolYield::WriteFile {
                    path: self.meta_path.clone(),
                    data: meta.dump()?,
                };
                coro.yield_(yield_op).await?.none();
                self.meta = Some(meta);
            }
        };

        // let yield_op = ProtocolYield::ExistsFile {
        //     path: self.draft_path.clone(),
        // };
        // let draft_exists = coro.yield_(yield_op).await?.exists();
        // let yield_op = ProtocolYield::ExistsFile {
        //     path: self.synced_path.clone(),
        // };
        // let synced_exists = coro.yield_(yield_op).await?.exists();
        // if !draft_exists || !synced_exists {
        //     return Err(Error::DatabaseSyncError(
        //         "Draft or Synced files doesn't exists, but metadata is".to_string(),
        //     ));
        // }

        if self.meta().synced_frame_no.is_none() {
            // sync WAL from the remote in case of bootstrap - all subsequent initializations will be fast
            self.pull(coro).await?;
        }
        Ok(())
    }

    fn open_synced(&self, capture: bool) -> Result<DatabaseTape> {
        let clean_path_str = self.synced_path.to_str().unwrap();
        let io = self.io.clone();
        let clean = turso_core::Database::open_file(io, clean_path_str, false, true).unwrap();
        let opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some(if capture { "after" } else { "off" }.to_string()),
        };
        tracing::debug!("initialize clean database connection");
        Ok(DatabaseTape::new_with_opts(clean, opts))
    }

    fn open_draft(&self) -> Result<DatabaseTape> {
        let draft_path_str = self.draft_path.to_str().unwrap();
        let io = self.io.clone();
        let draft = turso_core::Database::open_file(io, draft_path_str, false, true).unwrap();
        let opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("after".to_string()),
        };
        tracing::debug!("initialize draft database connection");
        Ok(DatabaseTape::new_with_opts(draft, opts))
    }

    async fn bootstrap_db_files(&mut self, coro: &Coro) -> Result<DatabaseMetadata> {
        assert!(
            self.meta.is_none(),
            "bootstrap_db_files must be called only when meta is not set"
        );
        let start_time = std::time::Instant::now();
        let yield_op = ProtocolYield::ExistsFile {
            path: self.draft_path.clone(),
        };
        if coro.yield_(yield_op).await?.exists() {
            let yield_op = ProtocolYield::RemoveFile {
                path: self.draft_path.clone(),
            };
            coro.yield_(yield_op).await?.none();
        }
        let yield_op = ProtocolYield::ExistsFile {
            path: self.synced_path.clone(),
        };
        if coro.yield_(yield_op).await?.exists() {
            let yield_op = ProtocolYield::RemoveFile {
                path: self.synced_path.clone(),
            };
            coro.yield_(yield_op).await?.none();
        }

        let yield_op = ProtocolYield::DbInfo;
        let generation = coro.yield_(yield_op).await?.db_info();

        let draft_path = self.draft_path.to_str().unwrap_or("");
        let draft = self.io.open_file(draft_path, OpenFlags::Create, false)?;

        let synced_path = self.synced_path.to_str().unwrap_or("");
        let synced = self.io.open_file(synced_path, OpenFlags::Create, false)?;

        let mut yield_op = ProtocolYield::DbBootstrap { generation };
        let mut pos = 0;
        loop {
            let Some(content) = coro.yield_(yield_op).await?.content() else {
                break;
            };
            yield_op = ProtocolYield::Continue;
            let content_len = content.len();
            let buffer = Arc::new(RefCell::new(Buffer::allocate(
                content.len(),
                Rc::new(|_| {}),
            )));
            buffer.borrow_mut().as_mut_slice().copy_from_slice(&content);

            let draft_c = Completion::new_write(move |size| {
                assert!(size as usize == content_len);
            });
            let draft_c = draft.pwrite(pos, buffer.clone(), draft_c)?;
            let synced_c = Completion::new_write(move |size| {
                assert!(size as usize == content_len);
            });
            let synced_c = synced.pwrite(pos, buffer.clone(), synced_c)?;
            while !draft_c.is_completed() || !synced_c.is_completed() {
                coro.yield_(ProtocolYield::IO).await?.none();
            }
            pos += content_len;
        }

        let elapsed = std::time::Instant::now().duration_since(start_time);
        tracing::debug!(
            "finish bootstrapping Synced file from remote: elapsed={:?}",
            elapsed
        );

        Ok(DatabaseMetadata {
            synced_generation: generation,
            synced_frame_no: None,
            synced_change_id: None,
            transferred_change_id: None,
            draft_wal_match_watermark: 0,
            synced_wal_match_watermark: 0,
        })
    }

    async fn write_meta(
        &self,
        coro: &Coro,
        update: impl Fn(&mut DatabaseMetadata),
    ) -> Result<DatabaseMetadata> {
        let mut meta = self.meta().clone();
        update(&mut meta);
        // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
        let yield_op = ProtocolYield::WriteFile {
            path: self.meta_path.clone(),
            data: meta.dump()?,
        };
        coro.yield_(yield_op).await?.none();
        Ok(meta)
    }

    /// Pull updates from remote to the Synced database
    /// This method will update Synced database WAL frames and [DatabaseMetadata::synced_frame_no] metadata field
    async fn pull_synced_from_remote(&mut self, coro: &Coro) -> Result<()> {
        tracing::debug!("pull_synced_from_remote");

        let (generation, mut frame_no) = {
            let meta = self.meta();
            (meta.synced_generation, meta.synced_frame_no.unwrap_or(0))
        };

        // open fresh connection to the Clean database in order to initiate WAL session
        let clean = self.open_synced(false)?;
        let clean_conn = clean.connect(coro).await?;

        let mut wal_session = WalSession::new(clean_conn);
        let mut buffer = Vec::with_capacity(WAL_FRAME_SIZE);
        'outer: loop {
            tracing::debug!(
                "pull clean wal portion: generation={}, frame={}",
                generation,
                frame_no + 1
            );

            let mut yield_op = ProtocolYield::WalPull {
                generation,
                start_frame: frame_no + 1,
            };
            loop {
                let yield_result = coro.yield_(yield_op).await;
                yield_op = ProtocolYield::Continue;

                let chunk = match yield_result {
                    Ok(ProtocolResume::DbStatus(status)) => {
                        if status.status == "checkpoint_needed"
                            && status.generation == generation
                            && status.max_frame_no == frame_no
                        {
                            tracing::debug!(
                                "end of history reached for database: status={:?}",
                                status
                            );
                            break 'outer;
                        }
                        // todo(sivukhin): temporary not supported - will implement soon after TRUNCATE checkpoint will be merged to turso-db
                        return Err(Error::PullNeedCheckpoint(status));
                    }
                    Ok(resume) => resume,
                    Err(e) => return Err(e),
                };

                let Some(chunk) = chunk.content() else {
                    break;
                };
                // chunk is arbitrary - aggregate groups of FRAME_SIZE bytes out from the chunks stream
                let mut chunk = Bytes::from(chunk);
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
                        self.meta = Some(
                            self.write_meta(coro, |m| m.synced_frame_no = Some(frame_no))
                                .await?,
                        );
                    }
                    buffer.clear();
                }
            }
        }
        Ok(())
    }

    async fn push_synced_to_remote(&mut self, coro: &Coro, change_id: Option<i64>) -> Result<()> {
        tracing::debug!("push_synced_to_remote");
        match self.do_push_synced_to_remote(coro).await {
            Ok(()) => {
                self.meta = Some(
                    self.write_meta(coro, |meta| {
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
                    self.write_meta(coro, |meta| meta.transferred_change_id = None)
                        .await?,
                );
                self.reset_synced(coro).await?;
                Err(err)
            }
            Err(err) => {
                tracing::info!("err: {}", err);
                Err(err)
            }
        }
    }

    async fn do_push_synced_to_remote(&mut self, coro: &Coro) -> Result<()> {
        tracing::debug!("do_push_synced_to_remote");

        let (generation, frame_no) = {
            let meta = self.meta();
            (meta.synced_generation, meta.synced_frame_no.unwrap_or(0))
        };

        let synced = self.open_synced(false)?;
        let synced_conn = synced.connect(coro).await?;

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
        let synced_frame_no = frame_no + frames_cnt;
        let yield_op = ProtocolYield::WalPush {
            baton: None,
            generation,
            start_frame: frame_no + 1,
            end_frame: synced_frame_no + 1,
            frames,
        };
        coro.yield_(yield_op).await?.none();

        let meta = self
            .write_meta(coro, |meta| meta.synced_frame_no = Some(synced_frame_no))
            .await?;
        self.meta = Some(meta);

        Ok(())
    }

    /// Transfers row changes from Draft DB to the Clean DB
    async fn transfer_draft_to_synced(
        &mut self,
        coro: &Coro,
        capture: bool,
    ) -> Result<Option<i64>> {
        tracing::debug!("transfer_draft_to_synced");
        self.synced_is_dirty = true;

        let draft = self.open_draft()?;
        let synced = self.open_synced(capture)?;
        let mut last_change_id = self.meta().synced_change_id;

        let replay_opts = DatabaseReplaySessionOpts {
            preserve_rowid: false,
        };
        let mut session = synced.start_replay_session(coro, replay_opts).await?;

        let iterate_opts = DatabaseChangesIteratorOpts {
            first_change_id: self.meta().synced_change_id.map(|x| x + 1),
            mode: DatabaseChangesIteratorMode::Apply,
            ..Default::default()
        };
        let mut changes = draft.iterate_changes(coro, iterate_opts).await?;
        while let Some(operation) = changes.next(coro).await? {
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
            session.replay(coro, operation).await?;
        }

        Ok(last_change_id)
    }

    async fn cleanup_synced(&mut self, coro: &Coro) -> Result<()> {
        tracing::debug!("cleanup_synced");

        if let Some(change_id) = self.meta().transferred_change_id {
            tracing::info!("some changes was transferred to the Synced DB but wasn't properly pushed to the remote");
            match self.push_synced_to_remote(coro, Some(change_id)).await {
                // ignore Ok and Error::PushConflict - because in this case we sucessfully finalized previous operation
                Ok(()) | Err(Error::PushConflict) => {}
                Err(err) => return Err(err),
            }
        }
        // if we failed in the middle before - let's reset Synced DB if necessary
        // if everything works without error - we will properly set is_synced_dirty flag and this function will be no-op
        self.reset_synced(coro).await?;
        Ok(())
    }

    async fn transfer_synced_to_draft(&mut self, coro: &Coro) -> Result<()> {
        tracing::debug!("transfer_synced_to_draft");

        let synced = self.open_synced(false)?;
        let synced_conn = synced.connect(coro).await?;
        let mut synced_session = WalSession::new(synced_conn.clone());
        synced_session.begin()?;

        let draft_watermark = self.meta().draft_wal_match_watermark as u32;
        let synced_watermark = self.meta().synced_wal_match_watermark as u32;
        assert!(
            self.meta().synced_frame_no.is_some(),
            "Synced DB WAL must be synced"
        );
        let synced_frames = self.meta().synced_frame_no.unwrap() as u32;
        let synced_all_frames = synced_conn.wal_frame_count()? as u32;
        assert!(
            synced_all_frames > synced_watermark,
            "some changes must be in Synced DB for transfer: {synced_all_frames} > {synced_watermark}",
        );
        assert!(
            (synced_watermark..=synced_all_frames).contains(&synced_frames),
            "synced_watermark={}, synced_all_frames={}, synced_frames={}",
            synced_watermark,
            synced_all_frames,
            synced_frames
        );

        let draft_frames = {
            let draft = self.open_draft()?;
            let mut draft_session = draft.start_wal_session(coro).await?;
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
            self.write_meta(coro, |meta| {
                meta.draft_wal_match_watermark = draft_frames;
                meta.synced_wal_match_watermark = synced_frames as u64;
                meta.synced_change_id = None;
            })
            .await?,
        );

        Ok(())
    }

    /// Reset WAL of Synced database which potentially can have some local changes
    async fn reset_synced(&mut self, coro: &Coro) -> Result<()> {
        tracing::debug!("reset_synced");

        // if we know that Clean DB is not dirty - let's skip this phase completely
        if !self.synced_is_dirty {
            return Ok(());
        }

        let synced_path_str = format!("{}-wal", self.synced_path.to_str().unwrap_or(""));
        let synced = match self.io.open_file(&synced_path_str, OpenFlags::None, false) {
            Ok(file) => file,
            Err(err) => {
                // todo(sivukhin): silently ignore error for now
                tracing::error!("ignoring open_file error: {err}");
                return Ok(());
            }
        };
        let synced_frame_no = self.meta().synced_frame_no.unwrap_or(0);
        let wal_size = if synced_frame_no == 0 {
            0
        } else {
            WAL_HEADER + WAL_FRAME_SIZE * synced_frame_no
        };
        tracing::debug!(
            "reset Synced DB WAL to the size of {} frames",
            synced_frame_no
        );
        let c = Completion::new_trunc(move |rc| {
            assert!(rc as usize == 0);
        });
        let c = synced.truncate(wal_size, c)?;
        while !c.is_completed() {
            coro.yield_(ProtocolYield::IO).await?.none();
        }
        self.synced_is_dirty = false;
        Ok(())
    }

    fn meta(&self) -> &DatabaseMetadata {
        self.meta.as_ref().expect("metadata must be set")
    }
}
