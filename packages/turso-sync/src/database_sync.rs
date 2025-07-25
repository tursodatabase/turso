use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use http_body_util::BodyExt;
use tokio::{
    io::AsyncWriteExt,
    sync::{OwnedRwLockReadGuard, RwLock},
};

use crate::{
    database_tape::{
        DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts, DatabaseTape, DatabaseTapeOpts,
    },
    errors::Error,
    metadata::{DatabasePrimary, DatabaseSyncMetadata},
    sync_server::{create_client, SyncServer, TursoSyncServer, TursoSyncServerOpts},
    Result,
};

pub struct DatabaseSync {
    sync_server: TursoSyncServer,
    draft_path: PathBuf,
    clean_path: PathBuf,
    meta_path: PathBuf,
    meta: Option<DatabaseSyncMetadata>,
    database: Arc<RwLock<DatabaseSyncTandem>>,
}

struct DatabaseSyncTandem {
    draft: Option<DatabaseTape>,
    clean: Option<DatabaseTape>,
    primary: DatabasePrimary,
}

impl DatabaseSyncTandem {
    fn primary(&self) -> &DatabaseTape {
        match self.primary {
            DatabasePrimary::Draft => self.draft.as_ref().unwrap(),
            DatabasePrimary::Clean => self.clean.as_ref().unwrap(),
        }
    }
}

pub struct RowsSync {
    _guard: OwnedRwLockReadGuard<DatabaseSyncTandem>,
    rows: turso::Rows,
}

impl std::ops::Deref for RowsSync {
    type Target = turso::Rows;

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl std::ops::DerefMut for RowsSync {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}

const PAGE_SIZE: usize = 4096;
const WAL_HEADER: usize = 32;
const FRAME_SIZE: usize = 24 + PAGE_SIZE;

async fn exists_file(path: &PathBuf) -> Result<bool> {
    tracing::debug!("check file exists at {:?}", path);
    Ok(tokio::fs::try_exists(&path).await?)
}

async fn remove_file(path: &Path) -> Result<()> {
    tracing::debug!("remove file at {:?}", path);
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

async fn create_file(path: &Path) -> Result<tokio::fs::File> {
    tracing::debug!("create file at {:?}", path);
    Ok(tokio::fs::File::create_new(path).await?)
}

async fn open_file(path: &Path) -> Result<tokio::fs::File> {
    tracing::debug!("open file at {:?}", path);
    Ok(tokio::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(path)
        .await?)
}

async fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    tracing::debug!("copy file from {:?} to {:?}", src, dst);
    tokio::fs::copy(&src, &dst).await?;
    Ok(())
}

async fn truncate_file(file: &tokio::fs::File, size: usize) -> Result<()> {
    tracing::debug!("truncate file to size {}", size);
    file.set_len(size as u64).await?;
    Ok(())
}

async fn write_file(file: &mut tokio::fs::File, buf: &[u8]) -> Result<()> {
    tracing::debug!("write buffer of size {} ot file", buf.len());
    file.write_all(&buf).await?;
    Ok(())
}

async fn read_chunk(stream: &mut hyper::body::Incoming) -> Result<Option<hyper::body::Bytes>> {
    let Some(frame) = stream.frame().await else {
        return Ok(None);
    };
    let frame = frame.map_err(Error::HyperResponse)?;
    let frame = frame
        .into_data()
        .map_err(|_| Error::DatabaseSyncError(format!("failed to read export chunk")))?;
    Ok(Some(frame))
}

struct WalSession<'a> {
    conn: &'a turso::Connection,
    in_txn: bool,
}

impl<'a> WalSession<'a> {
    pub fn new(conn: &'a turso::Connection) -> Self {
        Self {
            conn,
            in_txn: false,
        }
    }
    pub fn begin(&mut self) -> Result<()> {
        assert!(!self.in_txn);
        self.conn.wal_insert_begin()?;
        self.in_txn = true;
        Ok(())
    }
    pub fn end(&mut self) -> Result<()> {
        assert!(self.in_txn);
        self.conn.wal_insert_end()?;
        self.in_txn = false;
        Ok(())
    }
    pub fn in_txn(&self) -> bool {
        self.in_txn
    }
}

impl<'a> Drop for WalSession<'a> {
    fn drop(&mut self) {
        if self.in_txn {
            let _ = self
                .end()
                .inspect_err(|e| tracing::error!("failed to close WAL session: {}", e));
        }
    }
}

impl DatabaseSync {
    pub async fn new(path: &Path, opts: TursoSyncServerOpts) -> Result<Self> {
        let path_str = path.to_str().unwrap();
        let draft_path = PathBuf::try_from(format!("{}-draft", path_str)).unwrap();
        let clean_path = PathBuf::try_from(format!("{}-clean", path_str)).unwrap();
        let meta_path = PathBuf::try_from(format!("{}-info", path_str)).unwrap();
        let meta = DatabaseSyncMetadata::read_from(&meta_path).await?;
        let client = create_client()?;
        let sync_server = TursoSyncServer::new(client, opts)?;
        let mut db = Self {
            sync_server,
            draft_path,
            clean_path,
            meta_path,
            meta,
            database: Arc::new(RwLock::new(DatabaseSyncTandem {
                draft: None,
                clean: None,
                primary: DatabasePrimary::Draft,
            })),
        };
        db.init().await?;
        Ok(db)
    }

    pub async fn execute(&self, sql: &str, params: impl turso::IntoParams) -> Result<u64> {
        let database = self.database.read().await;
        let primary = database.primary();
        let conn = primary.connect().await?;
        let result = conn.execute(sql, params).await?;
        Ok(result)
    }

    pub async fn query(&self, sql: &str, params: impl turso::IntoParams) -> Result<RowsSync> {
        let database = self.database.clone().read_owned().await;
        let primary = database.primary();
        let conn = primary.connect().await?;
        let rows = conn.query(sql, params).await?;
        Ok(RowsSync {
            _guard: database,
            rows,
        })
    }

    pub async fn sync_from_remote(&mut self) -> Result<()> {
        self.pull_clean_from_remote().await?;
        self.transfer_draft_to_clean(true).await?;
        self.transfer_clean_to_draft().await?;
        self.reset_clean().await?;
        Ok(())
    }

    pub async fn sync_to_remote(&mut self) -> Result<()> {
        self.pull_clean_from_remote().await?;
        self.transfer_draft_to_clean(false).await?;
        self.push_clean_to_remote().await?;
        self.transfer_clean_to_draft().await?;
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        tracing::debug!("initialize DatabaseSync instance");
        if self.meta.is_none() {
            self.meta = Some(self.bootstrap().await?);
        }

        let draft_exists = exists_file(&self.draft_path).await?;
        let clean_exists = exists_file(&self.clean_path).await?;
        if !draft_exists || !clean_exists {
            return Err(Error::DatabaseSyncError(format!(
                "draft or clean files doesn't exists, but metadata is"
            )));
        }

        let primary = self.get_meta().primary_db;
        let conn = if primary == DatabasePrimary::Draft {
            self.open_draft().await?
        } else {
            self.open_clean(false).await?
        };
        self.switch_primary(primary, conn).await?;

        // clean db is primary - we need to finish transfer from clean to draft then
        if primary == DatabasePrimary::Clean {
            self.transfer_clean_to_draft().await?;
        }

        self.sync_from_remote().await?;
        Ok(())
    }

    async fn open_clean(&self, capture: bool) -> Result<DatabaseTape> {
        let clean_path_str = self.clean_path.to_str().unwrap();
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

    async fn switch_primary(&mut self, primary: DatabasePrimary, conn: DatabaseTape) -> Result<()> {
        let mut database = self.database.write().await;
        self.meta = Some(self.write_meta(|meta| meta.primary_db = primary).await?);
        match primary {
            DatabasePrimary::Draft => {
                let _ = database.clean.take();
                database.draft = Some(conn);
            }
            DatabasePrimary::Clean => {
                let _ = database.draft.take();
                database.clean = Some(conn);
            }
        }
        Ok(())
    }

    async fn bootstrap(&mut self) -> Result<DatabaseSyncMetadata> {
        if exists_file(&self.draft_path).await? {
            remove_file(&self.draft_path).await?;
        }
        if exists_file(&self.clean_path).await? {
            remove_file(&self.clean_path).await?;
        }

        let info = self.sync_server.db_info().await?;
        let mut clean_file = create_file(&self.clean_path).await?;
        let start_time = std::time::Instant::now();
        tracing::debug!("start bootstrapping clean file from remote");

        let mut bootstrap = self.sync_server.db_export(info.current_generation).await?;
        while let Some(frame) = read_chunk(&mut bootstrap).await? {
            write_file(&mut clean_file, &frame).await?;
        }

        let elapsed = std::time::Instant::now().duration_since(start_time);
        tracing::debug!(
            "finish bootstrapping local files from remote: elapsed={:?}",
            elapsed
        );

        copy_file(&self.clean_path, &self.draft_path).await?;
        tracing::debug!("copied upstream file to local");

        Ok(DatabaseSyncMetadata {
            clean_generation: info.current_generation,
            clean_frame_no: 0,
            primary_db: DatabasePrimary::Draft,
            draft_change_id: None,
        })
    }

    async fn write_meta(
        &self,
        update: impl Fn(&mut DatabaseSyncMetadata) -> (),
    ) -> Result<DatabaseSyncMetadata> {
        let mut meta = self.get_meta().clone();
        update(&mut meta);
        // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
        meta.write_to(&self.meta_path).await?;
        Ok(meta)
    }

    async fn pull_clean_from_remote(&mut self) -> Result<()> {
        tracing::debug!("pull clean from remote");
        let database = self.database.read().await;
        assert!(database.primary == DatabasePrimary::Draft);

        let (generation, mut frame_no) = {
            let meta = self.get_meta();
            (meta.clean_generation, meta.clean_frame_no)
        };

        let clean = self.open_clean(false).await?;
        let clean_conn = clean.connect().await?;

        let mut wal_session = WalSession::new(&clean_conn);
        let mut buffer = Vec::with_capacity(FRAME_SIZE);
        loop {
            let pull = self.sync_server.wal_pull(generation, frame_no + 1).await;
            let mut data = match pull {
                Ok(data) => data,
                Err(Error::PullNeedCheckpoint(status))
                    if status.generation == generation && status.max_frame_no == frame_no =>
                {
                    tracing::debug!("end of history reached for database: status={:?}", status);
                    break;
                }
                Err(e) => return Err(e),
            };
            while let Some(mut chunk) = read_chunk(&mut data).await? {
                while chunk.len() > 0 {
                    let to_fill = FRAME_SIZE - buffer.len();
                    let prefix = chunk.split_to(to_fill.min(chunk.len()));
                    buffer.extend_from_slice(&prefix);
                    if buffer.len() < FRAME_SIZE {
                        continue;
                    }
                    frame_no += 1;
                    if !wal_session.in_txn {
                        wal_session.begin()?;
                    }
                    if clean_conn.wal_insert_frame(frame_no as u32, &buffer)? {
                        wal_session.end()?;
                        self.meta = Some(self.write_meta(|m| m.clean_frame_no = frame_no).await?);
                    }
                    buffer.clear();
                }
            }
        }
        Ok(())
    }

    async fn push_clean_to_remote(&mut self) -> Result<()> {
        tracing::debug!("push_clean_to_remote");
        let database = self.database.read().await;
        assert!(database.primary == DatabasePrimary::Draft);

        let (generation, frame_no) = {
            let meta = self.get_meta();
            (meta.clean_generation, meta.clean_frame_no)
        };

        let clean = self.open_clean(false).await?;
        let clean_conn = clean.connect().await?;

        let mut frames = Vec::new();
        let mut frames_cnt = 0;
        {
            let mut wal_session = WalSession::new(&clean_conn);
            wal_session.begin()?;
            let clean_frames = clean_conn.wal_frame_count()? as usize;

            let mut buffer = [0u8; FRAME_SIZE];
            for frame_no in (frame_no + 1)..=clean_frames {
                clean_conn.wal_get_frame(frame_no as u32, &mut buffer)?;
                frames.extend_from_slice(&buffer);
                frames_cnt += 1;
            }
        }

        if frames_cnt == 0 {
            return Ok(());
        }

        // todo(sivukhin): recover from non fatal errors
        self.sync_server
            .wal_push(generation, frame_no + 1, frame_no + frames_cnt + 1, frames)
            .await?;
        self.meta = Some(
            self.write_meta(|meta| meta.clean_frame_no = frame_no + frames_cnt)
                .await?,
        );

        Ok(())
    }

    async fn transfer_draft_to_clean(&mut self, capture: bool) -> Result<()> {
        tracing::debug!("transfer_draft_to_clean");
        // take read lock in order to prevent switch of the primary
        let database = self.database.read().await;
        assert!(database.primary == DatabasePrimary::Draft);

        let draft = database.draft.as_ref().unwrap();
        let clean = self.open_clean(capture).await?;
        let opts = DatabaseChangesIteratorOpts {
            first_change_id: self.get_meta().draft_change_id,
            mode: DatabaseChangesIteratorMode::Apply,
            ..Default::default()
        };
        let mut session = clean.start_tape_session().await?;
        let mut changes = draft.iterate_changes(opts).await?;
        while let Some(operation) = changes.next().await? {
            session.replay(operation).await?;
        }

        Ok(())
    }

    async fn transfer_clean_to_draft(&mut self) -> Result<()> {
        tracing::debug!("transfer_clean_to_draft");
        // switch requests to clean DB and update metadata, because reading draft while we are transferring data from clean is not allowed
        let clean = self.open_clean(false).await?;
        self.switch_primary(DatabasePrimary::Clean, clean).await?;

        let draft_path_str = self.draft_path.to_str().unwrap_or("");
        let clean_path_str = self.clean_path.to_str().unwrap_or("");
        let draft_wal = PathBuf::try_from(format!("{}-wal", draft_path_str)).unwrap();
        let clean_wal = PathBuf::try_from(format!("{}-wal", clean_path_str)).unwrap();
        let draft_shm = PathBuf::try_from(format!("{}-shm", draft_path_str)).unwrap();
        copy_file(&self.clean_path, &self.draft_path).await?;
        copy_file(&clean_wal, &draft_wal).await?;
        remove_file(&draft_shm).await?;

        // switch requests back to draft DB
        let draft = self.open_draft().await?;
        self.switch_primary(DatabasePrimary::Draft, draft).await?;

        Ok(())
    }

    async fn reset_clean(&mut self) -> Result<()> {
        tracing::debug!("reset_clean");

        let draft = self.open_draft().await?;
        self.switch_primary(DatabasePrimary::Draft, draft).await?;

        // we need to ensure that even in case of failure we will reset clean version
        let clean_path_str = self.clean_path.to_str().unwrap_or("");
        let clean_wal_path = PathBuf::try_from(format!("{}-wal", clean_path_str)).unwrap();
        let wal_size = WAL_HEADER + FRAME_SIZE * self.get_meta().clean_frame_no;
        let clean_wal = open_file(&clean_wal_path).await?;
        truncate_file(&clean_wal, wal_size).await?;

        Ok(())
    }

    fn get_meta(&self) -> &DatabaseSyncMetadata {
        self.meta.as_ref().expect("metadata must be set")
    }
}
