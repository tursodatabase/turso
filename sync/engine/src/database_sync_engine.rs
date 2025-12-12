use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use turso_core::{Buffer, Completion, DatabaseStorage, OpenFlags};

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_sync_engine_io::SyncEngineIo,
    database_sync_lazy_storage::LazyDatabaseStorage,
    database_sync_operations::{
        acquire_slot, apply_transformation, bootstrap_db_file, connect_untracked,
        count_local_changes, has_table, push_logical_changes, read_last_change_id, read_wal_salt,
        reset_wal_file, update_last_change_id, wait_all_results, wal_apply_from_file,
        wal_pull_to_file, SyncEngineIoStats, PAGE_SIZE, WAL_FRAME_HEADER, WAL_FRAME_SIZE,
    },
    database_tape::{
        DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts, DatabaseReplaySession,
        DatabaseReplaySessionOpts, DatabaseTape, DatabaseTapeOpts, DatabaseWalSession,
        CDC_PRAGMA_NAME,
    },
    errors::Error,
    io_operations::IoOperations,
    types::{
        Coro, DatabaseMetadata, DatabasePullRevision, DatabaseRowTransformResult,
        DatabaseSyncEngineProtocolVersion, DatabaseTapeOperation, DbChangesStatus, PartialSyncOpts,
        SyncEngineStats, DATABASE_METADATA_VERSION,
    },
    wal_session::WalSession,
    Result,
};

#[derive(Clone, Debug)]
pub struct DatabaseSyncEngineOpts {
    pub client_name: String,
    pub tables_ignore: Vec<String>,
    pub use_transform: bool,
    pub wal_pull_batch_size: u64,
    pub long_poll_timeout: Option<std::time::Duration>,
    pub protocol_version_hint: DatabaseSyncEngineProtocolVersion,
    pub bootstrap_if_empty: bool,
    pub reserved_bytes: usize,
    pub partial_sync_opts: Option<PartialSyncOpts>,
}

pub struct DataStats {
    pub written_bytes: AtomicUsize,
    pub read_bytes: AtomicUsize,
}

impl Default for DataStats {
    fn default() -> Self {
        Self::new()
    }
}

impl DataStats {
    pub fn new() -> Self {
        Self {
            written_bytes: AtomicUsize::new(0),
            read_bytes: AtomicUsize::new(0),
        }
    }
    pub fn write(&self, size: usize) {
        self.written_bytes.fetch_add(size, Ordering::SeqCst);
    }
    pub fn read(&self, size: usize) {
        self.read_bytes.fetch_add(size, Ordering::SeqCst);
    }
}

pub struct DatabaseSyncEngine<IO: SyncEngineIo> {
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: SyncEngineIoStats<IO>,
    db_file: Arc<dyn turso_core::storage::database::DatabaseStorage>,
    main_tape: DatabaseTape,
    main_db_wal_path: String,
    revert_db_wal_path: String,
    main_db_path: String,
    meta_path: String,
    changes_file: Arc<Mutex<Option<Arc<dyn turso_core::File>>>>,
    opts: DatabaseSyncEngineOpts,
    meta: Mutex<DatabaseMetadata>,
    client_unique_id: String,
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..28 + 4].try_into().unwrap())
}
fn is_memory(main_db_path: &str) -> bool {
    main_db_path == ":memory:"
}
fn create_main_db_wal_path(main_db_path: &str) -> String {
    format!("{main_db_path}-wal")
}
fn create_revert_db_wal_path(main_db_path: &str) -> String {
    format!("{main_db_path}-wal-revert")
}
fn create_meta_path(main_db_path: &str) -> String {
    format!("{main_db_path}-info")
}
fn create_changes_path(main_db_path: &str) -> String {
    format!("{main_db_path}-changes")
}

/// caller has no access to the memory io - so we handle it here implicitly
/// ideally, we should add necessary methods to the turso_core::IO trait - but so far I am struggling with nice interface to do that
/// so, I decided to keep a little bit of mess in sync-engine for a little bit longer
async fn full_read<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: Arc<IO>,
    path: &str,
    is_memory: bool,
) -> Result<Option<Vec<u8>>> {
    if !is_memory {
        let completion = sync_engine_io.full_read(path)?;
        let data = wait_all_results(coro, &completion, None).await?;
        if data.is_empty() {
            return Ok(None);
        } else {
            return Ok(Some(data));
        }
    }
    let Ok(file) = io.open_file(path, OpenFlags::None, false) else {
        return Ok(None);
    };
    let mut content = Vec::new();
    let mut offset = 0;
    let buffer = Arc::new(Buffer::new_temporary(4096));
    let read_len = Arc::new(Mutex::new(0));
    loop {
        let c = Completion::new_read(buffer.clone(), {
            let read_len = read_len.clone();
            move |r| *read_len.lock().unwrap() = r.expect("memory io must not fail").1
        });
        let read = file.pread(offset, c).expect("memory io must not fail");
        assert!(read.finished(), "memory io must complete immediately");
        let read_len = *read_len.lock().unwrap();
        if read_len == 0 {
            break;
        }
        content.extend_from_slice(&buffer.as_slice()[0..read_len as usize]);
        offset += read_len as u64;
    }
    Ok(Some(content))
}

/// caller has no access to the memory io - so we handle it here implicitly
/// ideally, we should add necessary methods to the turso_core::IO trait - but so far I am struggling with nice interface to do that
/// so, I decided to keep a little bit of mess in sync-engine for a little bit longer
async fn full_write<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: Arc<IO>,
    path: &str,
    is_memory: bool,
    content: Vec<u8>,
) -> Result<()> {
    if !is_memory {
        let completion = sync_engine_io.full_write(path, content)?;
        wait_all_results(coro, &completion, None).await?;
        return Ok(());
    }
    let file = io.open_file(path, OpenFlags::Create, false)?;
    let trunc = file
        .truncate(0, Completion::new_trunc(|_| {}))
        .expect("memory io must not fail");
    assert!(trunc.finished(), "memory io must complete immediately");
    let write = file
        .pwrite(
            0,
            Arc::new(Buffer::new(content)),
            Completion::new_write(|_| {}),
        )
        .expect("memory io must nof fail");
    assert!(write.finished(), "memory io must complete immediately");
    Ok(())
}

impl<IO: SyncEngineIo> DatabaseSyncEngine<IO> {
    pub async fn read_db_meta<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
    ) -> Result<Option<DatabaseMetadata>> {
        let path = create_meta_path(main_db_path);
        let meta = full_read(
            coro,
            io,
            sync_engine_io.io.clone(),
            &path,
            is_memory(main_db_path),
        )
        .await?;
        match meta {
            Some(meta) => Ok(Some(DatabaseMetadata::load(&meta)?)),
            None => Ok(None),
        }
    }

    pub async fn bootstrap_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        opts: &DatabaseSyncEngineOpts,
    ) -> Result<DatabaseMetadata> {
        tracing::info!("bootstrap_db(path={}): opts={:?}", main_db_path, opts);

        let meta_path = create_meta_path(main_db_path);
        let meta = full_read(
            coro,
            io.clone(),
            sync_engine_io.io.clone(),
            &meta_path,
            is_memory(main_db_path),
        )
        .await?;
        let meta = match meta {
            Some(meta) => Some(DatabaseMetadata::load(&meta)?),
            None => None,
        };
        let partial_sync_opts = opts.partial_sync_opts.clone();
        let partial = partial_sync_opts.is_some();

        let meta = match meta {
            Some(meta) => meta,
            None if opts.bootstrap_if_empty => {
                let client_unique_id = format!("{}-{}", opts.client_name, uuid::Uuid::new_v4());
                let revision = bootstrap_db_file(
                    coro,
                    &sync_engine_io,
                    &io,
                    main_db_path,
                    opts.protocol_version_hint,
                    partial_sync_opts,
                )
                .await?;
                let meta = DatabaseMetadata {
                    version: DATABASE_METADATA_VERSION.to_string(),
                    client_unique_id,
                    synced_revision: Some(revision.clone()),
                    revert_since_wal_salt: None,
                    revert_since_wal_watermark: 0,
                    last_pushed_change_id_hint: 0,
                    last_pushed_pull_gen_hint: 0,
                    last_pull_unix_time: Some(io.now().secs),
                    last_push_unix_time: None,
                    partial_bootstrap_server_revision: if partial {
                        Some(revision.clone())
                    } else {
                        None
                    },
                };
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");

                full_write(
                    coro,
                    io.clone(),
                    sync_engine_io.io.clone(),
                    &meta_path,
                    is_memory(main_db_path),
                    meta.dump()?,
                )
                .await?;
                // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
                meta
            }
            None => {
                if opts.protocol_version_hint == DatabaseSyncEngineProtocolVersion::Legacy {
                    return Err(Error::DatabaseSyncEngineError(
                        "deferred bootstrap is not supported for legacy protocol".to_string(),
                    ));
                }
                if partial {
                    return Err(Error::DatabaseSyncEngineError(
                        "deferred bootstrap is not supported for partial sync".to_string(),
                    ));
                }
                let client_unique_id = format!("{}-{}", opts.client_name, uuid::Uuid::new_v4());
                let meta = DatabaseMetadata {
                    version: DATABASE_METADATA_VERSION.to_string(),
                    client_unique_id,
                    synced_revision: None,
                    revert_since_wal_salt: None,
                    revert_since_wal_watermark: 0,
                    last_pushed_change_id_hint: 0,
                    last_pushed_pull_gen_hint: 0,
                    last_pull_unix_time: None,
                    last_push_unix_time: None,
                    partial_bootstrap_server_revision: None,
                };
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");
                full_write(
                    coro,
                    io.clone(),
                    sync_engine_io.io.clone(),
                    &meta_path,
                    is_memory(main_db_path),
                    meta.dump()?,
                )
                .await?;
                // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
                meta
            }
        };

        if meta.version != DATABASE_METADATA_VERSION {
            return Err(Error::DatabaseSyncEngineError(format!(
                "unsupported metadata version: {}",
                meta.version
            )));
        }

        tracing::info!("check if main db file exists");

        let main_exists = io.try_open(main_db_path)?.is_some();
        if !main_exists && meta.synced_revision.is_some() {
            let error = "main DB file doesn't exists, but metadata is".to_string();
            return Err(Error::DatabaseSyncEngineError(error));
        }

        Ok(meta)
    }

    pub fn init_db_storage(
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        meta: &DatabaseMetadata,
        main_db_path: &str,
        opts: &DatabaseSyncEngineOpts,
    ) -> Result<Arc<dyn DatabaseStorage>> {
        let partial_bootstrap_opts = &opts.partial_sync_opts;
        let partial = !partial_bootstrap_opts.is_none();
        let db_file = io.open_file(main_db_path, turso_core::OpenFlags::Create, false)?;
        let db_file: Arc<dyn DatabaseStorage> = if partial {
            let Some(partial_bootstrap_server_revision) = &meta.partial_bootstrap_server_revision
            else {
                return Err(Error::DatabaseSyncEngineError(
                    "partial_bootstrap_server_revision must be set in the metadata".to_string(),
                ));
            };
            let DatabasePullRevision::V1 { revision } = &partial_bootstrap_server_revision else {
                return Err(Error::DatabaseSyncEngineError(
                    "partial sync is supported only for V1 protocol".to_string(),
                ));
            };
            tracing::info!("create LazyDatabaseStorage database storage");
            Arc::new(LazyDatabaseStorage::new(
                db_file,
                None, // todo(sivukhin): allocate dirty file for FS IO
                sync_engine_io.clone(),
                revision.to_string(),
                partial_bootstrap_opts
                    .clone()
                    .expect("partial sync opts are set here"),
            ))
        } else {
            Arc::new(turso_core::storage::database::DatabaseFile::new(db_file))
        };

        Ok(db_file)
    }

    pub async fn open_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db: Arc<turso_core::Database>,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        let main_db_path = main_db.path.to_string();
        tracing::info!("open_db(path={}): opts={:?}", main_db_path, opts);

        let meta_path = create_meta_path(&main_db_path);

        let meta = full_read(
            coro,
            io.clone(),
            sync_engine_io.io.clone(),
            &meta_path,
            is_memory(&main_db_path),
        )
        .await?;
        let Some(meta) = meta else {
            return Err(Error::DatabaseSyncEngineError(
                "meta must be initialized before open".to_string(),
            ));
        };
        let meta = DatabaseMetadata::load(&meta)?;

        // DB wasn't synced with remote but will be encrypted on remote - so we must properly set reserved bytes field in advance
        if meta.synced_revision.is_none() && opts.reserved_bytes != 0 {
            let conn = main_db.connect()?;
            conn.set_reserved_bytes(opts.reserved_bytes as u8)?;

            // write transaction forces allocation of root DB page
            conn.execute("BEGIN IMMEDIATE")?;
            conn.execute("COMMIT")?;
        }

        let tape_opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("full".to_string()),
        };
        tracing::info!("initialize database tape connection: path={}", main_db_path);
        let main_db_io = main_db.io.clone();
        let main_db_file = main_db.db_file.clone();
        let main_tape = DatabaseTape::new_with_opts(main_db, tape_opts);

        let changes_path = create_changes_path(&main_db_path);
        let changes_file = main_db_io.open_file(&changes_path, OpenFlags::Create, false)?;

        let db = Self {
            io: main_db_io,
            sync_engine_io,
            db_file: main_db_file,
            main_tape,
            main_db_path: main_db_path.to_string(),
            main_db_wal_path: create_main_db_wal_path(&main_db_path),
            revert_db_wal_path: create_revert_db_wal_path(&main_db_path),
            meta_path: create_meta_path(&main_db_path),
            changes_file: Arc::new(Mutex::new(Some(changes_file))),
            opts,
            meta: Mutex::new(meta.clone()),
            client_unique_id: meta.client_unique_id.clone(),
        };

        let synced_revision = meta.synced_revision.as_ref();
        if let Some(DatabasePullRevision::Legacy {
            synced_frame_no: None,
            ..
        }) = synced_revision
        {
            // sync WAL from the remote in case of bootstrap - all subsequent initializations will be fast
            db.pull_changes_from_remote(coro).await?;
        }

        tracing::info!("sync engine was initialized");
        Ok(db)
    }

    /// Creates new instance of SyncEngine and initialize it immediately if no consistent local data exists
    pub async fn create_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        let meta = Self::bootstrap_db(
            coro,
            io.clone(),
            sync_engine_io.clone(),
            main_db_path,
            &opts,
        )
        .await?;
        let main_db_storage = Self::init_db_storage(
            io.clone(),
            sync_engine_io.clone(),
            &meta,
            main_db_path,
            &opts,
        )?;
        let main_db = turso_core::Database::open_with_flags(
            io.clone(),
            main_db_path,
            main_db_storage,
            OpenFlags::Create,
            turso_core::DatabaseOpts::new(),
            None,
        )?;
        Self::open_db(coro, io, sync_engine_io, main_db, opts).await
    }

    fn open_revert_db_conn(&self) -> Result<Arc<turso_core::Connection>> {
        let db = turso_core::Database::open_with_flags_bypass_registry(
            self.io.clone(),
            &self.main_db_path,
            &self.revert_db_wal_path,
            self.db_file.clone(),
            OpenFlags::Create,
            turso_core::DatabaseOpts::new(),
            None,
        )?;
        let conn = db.connect()?;
        conn.wal_auto_checkpoint_disable();
        Ok(conn)
    }

    async fn checkpoint_passive<Ctx>(&self, coro: &Coro<Ctx>) -> Result<(Option<Vec<u32>>, u64)> {
        let watermark = self.meta().revert_since_wal_watermark;
        tracing::info!(
            "checkpoint(path={:?}): revert_since_wal_watermark={}",
            self.main_db_path,
            watermark
        );
        let main_conn = connect_untracked(&self.main_tape)?;
        let main_wal = self.io.try_open(&self.main_db_wal_path)?;
        let main_wal_salt = if let Some(main_wal) = main_wal {
            read_wal_salt(coro, &main_wal).await?
        } else {
            None
        };

        tracing::info!(
            "checkpoint(path={:?}): main_wal_salt={:?}",
            self.main_db_path,
            main_wal_salt
        );

        let revert_since_wal_salt = self.meta().revert_since_wal_salt.clone();
        if revert_since_wal_salt.is_some() && main_wal_salt != revert_since_wal_salt {
            self.update_meta(coro, |meta| {
                meta.revert_since_wal_watermark = 0;
                meta.revert_since_wal_salt = main_wal_salt.clone();
            })
            .await?;
            return Ok((main_wal_salt, 0));
        }
        // we do this Passive checkpoint in order to transfer all synced frames to the DB file and make history of revert DB valid
        // if we will not do that we will be in situation where WAL in the revert DB is not valid relative to the DB file
        let result = main_conn.checkpoint(turso_core::CheckpointMode::Passive {
            upper_bound_inclusive: Some(watermark),
        })?;
        tracing::info!(
            "checkpoint(path={:?}): checkpointed portion of WAL: {:?}",
            self.main_db_path,
            result
        );
        if result.max_frame < watermark {
            return Err(Error::DatabaseSyncEngineError(
                format!("unable to checkpoint synced portion of WAL: result={result:?}, watermark={watermark}"),
            ));
        }
        Ok((main_wal_salt, watermark))
    }

    pub async fn stats<Ctx>(&self, coro: &Coro<Ctx>) -> Result<SyncEngineStats> {
        let main_conn = connect_untracked(&self.main_tape)?;
        let change_id = self.meta().last_pushed_change_id_hint;
        let last_pull_unix_time = self.meta().last_pull_unix_time;
        let revision = self.meta().synced_revision.clone().map(|x| match x {
            DatabasePullRevision::Legacy {
                generation,
                synced_frame_no,
            } => format!("generation={generation},synced_frame_no={synced_frame_no:?}"),
            DatabasePullRevision::V1 { revision } => revision,
        });
        let last_push_unix_time = self.meta().last_push_unix_time;
        let revert_wal_path = &self.revert_db_wal_path;
        let revert_wal_file = self.io.try_open(revert_wal_path)?;
        let revert_wal_size = revert_wal_file.map(|f| f.size()).transpose()?.unwrap_or(0);
        let main_wal_frames = main_conn.wal_state()?.max_frame;
        let main_wal_size = if main_wal_frames == 0 {
            0
        } else {
            WAL_FRAME_HEADER as u64 + WAL_FRAME_SIZE as u64 * main_wal_frames
        };
        Ok(SyncEngineStats {
            cdc_operations: count_local_changes(coro, &main_conn, change_id).await?,
            main_wal_size,
            revert_wal_size,
            last_pull_unix_time,
            last_push_unix_time,
            revision,
            network_sent_bytes: self
                .sync_engine_io
                .network_stats
                .written_bytes
                .load(Ordering::SeqCst),
            network_received_bytes: self
                .sync_engine_io
                .network_stats
                .read_bytes
                .load(Ordering::SeqCst),
        })
    }

    pub async fn checkpoint<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        let (main_wal_salt, watermark) = self.checkpoint_passive(coro).await?;

        tracing::info!(
            "checkpoint(path={:?}): passive checkpoint is done",
            self.main_db_path
        );
        let main_conn = connect_untracked(&self.main_tape)?;
        let revert_conn = self.open_revert_db_conn()?;

        let mut page = [0u8; PAGE_SIZE];
        let db_size = if revert_conn.try_wal_watermark_read_page(1, &mut page, None)? {
            db_size_from_page(&page)
        } else {
            0
        };

        tracing::info!(
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
            tracing::info!(
                "checkpoint(path={:?}): main DB WAL state: {:?}",
                self.main_db_path,
                main_wal_state
            );

            let mut revert_session = DatabaseWalSession::new(coro, revert_session).await?;

            let main_changed_pages = main_conn.wal_changed_pages_after(watermark)?;
            tracing::info!(
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
                    tracing::info!(
                        "checkpoint(path={:?}): skip page {} as it present in revert WAL",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                if page_no > db_size {
                    tracing::info!(
                        "checkpoint(path={:?}): skip page {} as it ahead of revert-DB size",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                if !main_conn.try_wal_watermark_read_page(page_no, &mut page, Some(watermark))? {
                    tracing::info!(
                        "checkpoint(path={:?}): skip page {} as it was allocated in the wAL portion for revert",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                tracing::info!(
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
        self.update_meta(coro, |meta| {
            meta.revert_since_wal_salt = main_wal_salt;
            meta.revert_since_wal_watermark = main_wal_state.max_frame;
        })
        .await?;

        let result = main_conn.checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: Some(main_wal_state.max_frame),
        })?;
        tracing::info!(
            "checkpoint(path={:?}): main DB TRUNCATE checkpoint result: {:?}",
            self.main_db_path,
            result
        );

        Ok(())
    }

    pub async fn wait_changes_from_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<DbChangesStatus> {
        tracing::info!("wait_changes(path={})", self.main_db_path);

        let file = acquire_slot(&self.changes_file)?;

        let now = self.io.now();
        let revision = self.meta().synced_revision.clone();
        let next_revision = wal_pull_to_file(
            coro,
            &self.sync_engine_io,
            &file.value,
            &revision,
            self.opts.wal_pull_batch_size,
            self.opts.long_poll_timeout,
        )
        .await?;

        if file.value.size()? == 0 {
            tracing::info!(
                "wait_changes(path={}): no changes detected",
                self.main_db_path
            );
            return Ok(DbChangesStatus {
                time: now,
                revision: next_revision,
                file_slot: None,
            });
        }

        tracing::info!(
            "wait_changes_from_remote(path={}): revision: {:?} -> {:?}",
            self.main_db_path,
            revision,
            next_revision
        );

        Ok(DbChangesStatus {
            time: now,
            revision: next_revision,
            file_slot: Some(file),
        })
    }

    /// Sync all new changes from remote DB and apply them locally
    /// This method will **not** send local changed to the remote
    /// This method will block writes for the period of pull
    pub async fn apply_changes_from_remote<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        remote_changes: DbChangesStatus,
    ) -> Result<()> {
        if remote_changes.file_slot.is_none() {
            self.update_meta(coro, |m| {
                m.last_pull_unix_time = Some(remote_changes.time.secs);
            })
            .await?;
            return Ok(());
        }
        assert!(remote_changes.file_slot.is_some(), "file_slot must be set");
        let changes_file = remote_changes.file_slot.as_ref().unwrap().value.clone();
        let pull_result = self.apply_changes_internal(coro, &changes_file).await;
        let Ok(revert_since_wal_watermark) = pull_result else {
            return Err(pull_result.err().unwrap());
        };

        let revert_wal_file = self.io.open_file(
            &self.revert_db_wal_path,
            turso_core::OpenFlags::Create,
            false,
        )?;
        reset_wal_file(coro, revert_wal_file, 0).await?;

        self.update_meta(coro, |m| {
            m.revert_since_wal_watermark = revert_since_wal_watermark;
            m.synced_revision = Some(remote_changes.revision);
            m.last_pushed_change_id_hint = 0;
            m.last_pull_unix_time = Some(remote_changes.time.secs);
        })
        .await?;
        Ok(())
    }
    async fn apply_changes_internal<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        changes_file: &Arc<dyn turso_core::File>,
    ) -> Result<u64> {
        tracing::info!("apply_changes(path={})", self.main_db_path);

        let (_, watermark) = self.checkpoint_passive(coro).await?;

        let revert_conn = self.open_revert_db_conn()?;
        let main_conn = connect_untracked(&self.main_tape)?;

        let mut revert_session = WalSession::new(revert_conn.clone());
        revert_session.begin()?;

        // start of the pull updates apply process
        // during this process we need to be very careful with the state of the WAL as at some points it can be not safe to read data from it
        // the reasons why this can be not safe:
        // 1. we are in the middle of rollback or apply from remote WAL - so DB now is in some weird state and no operations can be made safely
        // 2. after rollback or apply from remote WAL it's unsafe to prepare statements because schema cookie can go "back in time" and we first need to adjust it before executing any statement over DB
        let mut main_session = WalSession::new(main_conn.clone());
        main_session.begin()?;

        // we need to make sure that updates from the session will not be commited accidentally in the middle of the pull process
        // in order to achieve that we mark current session as "nested program" which eliminates possibility that data will be actually commited without our explicit command
        //
        // the reason to not use auto-commit is because it has its own rules which resets the flag in case of statement reset - which we do under the hood sometimes
        // that's why nested executed was chosen instead of auto-commit=false mode
        main_conn.start_nested();

        let had_cdc_table = has_table(coro, &main_conn, "turso_cdc").await?;

        // read current pull generation from local table for the given client
        let (local_pull_gen, _) =
            read_last_change_id(coro, &main_conn, &self.client_unique_id).await?;

        // read schema version after initiating WAL session (in order to read it with consistent max_frame_no)
        // note, that as we initiated WAL session earlier - no changes can be made in between and we will have consistent race-free view of schema version
        let main_conn_schema_version = main_conn.read_schema_version()?;

        let mut main_session = DatabaseWalSession::new(coro, main_session).await?;

        // Phase 1 (start): rollback local changes from the WAL

        // Phase 1.a: rollback local changes not checkpointed to the revert-db
        tracing::info!(
            "apply_changes(path={}): rolling back frames after {} watermark, max_frame={}",
            self.main_db_path,
            watermark,
            main_conn.wal_state()?.max_frame
        );
        let local_rollback = main_session.rollback_changes_after(watermark)?;
        let mut frame = [0u8; WAL_FRAME_SIZE];

        let remote_rollback = revert_conn.wal_state()?.max_frame;
        tracing::info!(
            "apply_changes(path={}): rolling back {} frames from revert DB",
            self.main_db_path,
            remote_rollback
        );
        // Phase 1.b: rollback local changes by using frames from revert-db
        // it's important to append pages from revert-db after local revert - because pages from revert-db must overwrite rollback from main DB
        for frame_no in 1..=remote_rollback {
            let info = revert_session.read_at(frame_no, &mut frame)?;
            main_session.append_page(info.page_no, &frame[WAL_FRAME_HEADER..])?;
        }

        // Phase 2: after revert DB has no local changes in its latest state - so its safe to apply changes from remote
        let db_size = wal_apply_from_file(coro, changes_file, &mut main_session).await?;
        tracing::info!(
            "apply_changes(path={}): applied changes from remote: db_size={}",
            self.main_db_path,
            db_size,
        );

        main_session.commit(0)?;
        // now DB is equivalent to the some remote state (all local changes reverted, all remote changes applied)
        // remember this frame watermark as a checkpoint for revert for pull operations in future
        let revert_since_wal_watermark = main_session.frames_count()?;

        // Phase 3: DB now has sane WAL - but schema cookie can be arbitrary - so we need to bump it (potentially forcing re-prepare for cached statement)
        let current_schema_version = main_conn.read_schema_version()?;
        let final_schema_version = current_schema_version.max(main_conn_schema_version) + 1;
        main_conn.write_schema_version(final_schema_version)?;
        tracing::info!(
            "apply_changes(path={}): updated schema version to {}",
            self.main_db_path,
            final_schema_version
        );

        // Phase 4: as now DB has all data from remote - let's read pull generation and last change id for current client
        // we will use last_change_id in order to replay local changes made strictly after that id locally
        let (remote_pull_gen, remote_last_change_id) =
            read_last_change_id(coro, &main_conn, &self.client_unique_id).await?;

        // we update pull generation and last_change_id at remote on push, but locally its updated on pull
        // so its impossible to have remote pull generation to be greater than local one
        if remote_pull_gen > local_pull_gen {
            return Err(Error::DatabaseSyncEngineError(format!("protocol error: remote_pull_gen > local_pull_gen: {remote_pull_gen} > {local_pull_gen}")));
        }
        let last_change_id = if remote_pull_gen == local_pull_gen {
            // if remote_pull_gen == local_pull gen - this means that remote portion of WAL have overlap with our local changes
            // (because we did one or more push operations since last pull) - so we need to take some suffix of local changes for replay
            remote_last_change_id
        } else {
            // if remove_pull_gen < local_pull_gen - this means that remote portion of WAL have no overlaps with all our local changes and we need to replay all of them
            Some(0)
        };

        // Phase 5: collect local changes
        // note, that collecting chanages from main_conn will yield zero rows as we already rolled back everything from it
        // but since we didn't commited these changes yet - we can just collect changes from another connection
        let iterate_opts = DatabaseChangesIteratorOpts {
            first_change_id: last_change_id.map(|x| x + 1),
            mode: DatabaseChangesIteratorMode::Apply,
            ignore_schema_changes: false,
            ..Default::default()
        };
        let mut local_changes = Vec::new();
        {
            // it's important here that DatabaseTape create fresh connection under the hood
            let mut iterator = self.main_tape.iterate_changes(iterate_opts)?;
            while let Some(operation) = iterator.next(coro).await? {
                match operation {
                    DatabaseTapeOperation::StmtReplay(_) => {
                        panic!("changes iterator must not use StmtReplay option")
                    }
                    DatabaseTapeOperation::RowChange(change) => local_changes.push(change),
                    DatabaseTapeOperation::Commit => continue,
                }
            }
        }
        tracing::info!(
            "apply_changes(path={}): collected {} changes",
            self.main_db_path,
            local_changes.len()
        );

        // Phase 6: replay local changes
        // we can skip this phase if we are sure that we had no local changes before
        if !local_changes.is_empty() || local_rollback != 0 || remote_rollback != 0 || had_cdc_table
        {
            // first, we update last_change id in the local meta table for sync
            update_last_change_id(
                coro,
                &main_conn,
                &self.client_unique_id,
                local_pull_gen + 1,
                0,
            )
            .await
            .inspect_err(|e| tracing::error!("update_last_change_id failed: {e}"))?;

            if had_cdc_table {
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

            let mut transformed = if self.opts.use_transform {
                Some(
                    apply_transformation(
                        coro,
                        &self.sync_engine_io,
                        &local_changes,
                        &replay.generator,
                    )
                    .await?,
                )
            } else {
                None
            };

            assert!(!replay.conn().get_auto_commit());
            // Replay local changes collected on Phase 5
            for (i, change) in local_changes.into_iter().enumerate() {
                let operation = if let Some(transformed) = &mut transformed {
                    match std::mem::replace(&mut transformed[i], DatabaseRowTransformResult::Skip) {
                        DatabaseRowTransformResult::Keep => {
                            DatabaseTapeOperation::RowChange(change)
                        }
                        DatabaseRowTransformResult::Skip => continue,
                        DatabaseRowTransformResult::Rewrite(replay) => {
                            DatabaseTapeOperation::StmtReplay(replay)
                        }
                    }
                } else {
                    DatabaseTapeOperation::RowChange(change)
                };
                replay.replay(coro, operation).await?;
            }
            assert!(!replay.conn().get_auto_commit());
        }

        // Final: now we did all necessary operations as one big transaction and we are ready to commit
        main_conn.end_nested();
        main_session.wal_session.end(true)?;

        Ok(revert_since_wal_watermark)
    }

    /// Sync local changes to remote DB
    /// This method will **not** pull remote changes to the local DB
    /// This method will **not** block writes for the period of sync
    pub async fn push_changes_to_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        tracing::info!("push_changes(path={})", self.main_db_path);

        let (_, change_id) = push_logical_changes(
            coro,
            &self.sync_engine_io,
            &self.main_tape,
            &self.client_unique_id,
            &self.opts,
        )
        .await?;

        self.update_meta(coro, |m| {
            m.last_pushed_change_id_hint = change_id;
            m.last_push_unix_time = Some(self.io.now().secs);
        })
        .await?;

        Ok(())
    }

    /// Create read/write database connection and appropriately configure it before use
    pub async fn connect_rw<Ctx>(&self, coro: &Coro<Ctx>) -> Result<Arc<turso_core::Connection>> {
        let conn = self.main_tape.connect(coro).await?;
        conn.wal_auto_checkpoint_disable();
        Ok(conn)
    }

    /// Sync local changes to remote DB and bring new changes from remote to local
    /// This method will block writes for the period of sync
    pub async fn sync<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        // todo(sivukhin): this is bit suboptimal as both 'push' and 'pull' will call pull_synced_from_remote
        // but for now - keep it simple
        self.push_changes_to_remote(coro).await?;
        self.pull_changes_from_remote(coro).await?;
        Ok(())
    }

    pub async fn pull_changes_from_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        let changes = self.wait_changes_from_remote(coro).await?;
        self.apply_changes_from_remote(coro, changes).await?;
        Ok(())
    }

    fn meta(&self) -> std::sync::MutexGuard<'_, DatabaseMetadata> {
        self.meta.lock().unwrap()
    }

    async fn update_meta<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        update: impl FnOnce(&mut DatabaseMetadata),
    ) -> Result<()> {
        let mut meta = self.meta().clone();
        update(&mut meta);
        tracing::info!("update_meta: {meta:?}");
        full_write(
            coro,
            self.io.clone(),
            self.sync_engine_io.io.clone(),
            &self.meta_path,
            is_memory(&self.main_db_path),
            meta.dump()?,
        )
        .await?;
        // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
        *self.meta.lock().unwrap() = meta;
        Ok(())
    }
}
