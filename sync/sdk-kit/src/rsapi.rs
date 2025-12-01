use std::sync::{Arc, Mutex};

use turso_core::{MemoryIO, IO};
use turso_sdk_kit::rsapi::{str_from_c_str, TursoError, TursoStatusCode};
use turso_sync_engine::{
    database_sync_engine::{self, DatabaseSyncEngine},
    database_sync_engine_io::SyncEngineIo,
    types::PartialBootstrapStrategy,
};

use crate::{
    capi,
    sync_engine_io::{self, SyncEngineIoQueue},
    turso_async_operation::{TursoAsyncOperationResult, TursoDatabaseAsyncOperation},
};

const DEFAULT_WAL_PULL_BATCH_SIZE: u32 = 100;

#[derive(Clone)]
pub struct TursoDatabaseSyncConfig {
    pub path: String,
    pub client_name: String,
    pub wal_pull_batch_size: Option<u32>,
    pub long_poll_timeout_ms: Option<u32>,
    pub bootstrap_if_empty: bool,
    pub reserved_bytes: Option<usize>,
    pub partial_bootstrap_strategy: turso_sync_engine::types::PartialBootstrapStrategy,
    pub db_io: Option<Arc<dyn IO>>,
}

impl TursoDatabaseSyncConfig {
    pub fn from_capi(
        value: capi::c::turso_sync_database_config_t,
    ) -> Result<Self, turso_sdk_kit::rsapi::TursoError> {
        Ok(Self {
            path: str_from_c_str(value.path)?.to_string(),
            client_name: str_from_c_str(value.client_name)?.to_string(),
            wal_pull_batch_size: if value.wal_pull_batch_size == 0 {
                None
            } else {
                Some(value.wal_pull_batch_size as u32)
            },
            long_poll_timeout_ms: if value.long_poll_timeout_ms == 0 {
                None
            } else {
                Some(value.long_poll_timeout_ms as u32)
            },
            bootstrap_if_empty: value.bootstrap_if_empty,
            reserved_bytes: if value.reserved_bytes == 0 {
                None
            } else {
                Some(value.reserved_bytes as usize)
            },
            partial_bootstrap_strategy: if value.partial_bootstrap_strategy_prefix != 0 {
                turso_sync_engine::types::PartialBootstrapStrategy::Prefix {
                    length: value.partial_bootstrap_strategy_prefix as usize,
                }
            } else if !value.partial_bootstrap_strategy_query.is_null() {
                let query = str_from_c_str(value.partial_bootstrap_strategy_query)?;
                turso_sync_engine::types::PartialBootstrapStrategy::Query {
                    query: query.to_string(),
                }
            } else {
                turso_sync_engine::types::PartialBootstrapStrategy::None
            },
            db_io: None,
        })
    }
}

pub struct TursoDatabaseSyncChanges {
    changes: turso_sync_engine::types::DbChangesStatus,
}

impl TursoDatabaseSyncChanges {
    /// TODO
    pub fn to_capi(self: Box<Self>) -> capi::c::turso_sync_changes_t {
        capi::c::turso_sync_changes_t {
            inner: Box::into_raw(self) as *mut std::ffi::c_void,
        }
    }
    /// TODO
    pub unsafe fn box_from_capi(value: capi::c::turso_sync_changes_t) -> Box<Self> {
        Box::from_raw(value.inner as *mut Self)
    }
}

pub struct TursoDatabaseSync<TBytes: AsRef<[u8]> + Send + Sync + 'static> {
    db_config: turso_sdk_kit::rsapi::TursoDatabaseConfig,
    sync_config: TursoDatabaseSyncConfig,
    sync_engine_opts: turso_sync_engine::database_sync_engine::DatabaseSyncEngineOpts,
    db_io: Arc<dyn IO>,
    sync_engine_io_queue: Arc<SyncEngineIoQueue<TBytes>>,
    sync_engine: Arc<Mutex<Option<DatabaseSyncEngine<SyncEngineIoQueue<TBytes>>>>>,
}

impl<TBytes: AsRef<[u8]> + Send + Sync + 'static> TursoDatabaseSync<TBytes> {
    pub fn new(
        db_config: turso_sdk_kit::rsapi::TursoDatabaseConfig,
        sync_config: TursoDatabaseSyncConfig,
    ) -> Result<Arc<Self>, turso_sdk_kit::rsapi::TursoError> {
        let sync_engine_opts = turso_sync_engine::database_sync_engine::DatabaseSyncEngineOpts {
            client_name: sync_config.client_name.clone(),
            tables_ignore: vec![],
            use_transform: false,
            wal_pull_batch_size: sync_config
                .wal_pull_batch_size
                .unwrap_or(DEFAULT_WAL_PULL_BATCH_SIZE) as u64,
            long_poll_timeout: sync_config
                .long_poll_timeout_ms
                .map(|t| std::time::Duration::from_millis(t as u64)),
            protocol_version_hint: turso_sync_engine::types::DatabaseSyncEngineProtocolVersion::V1,
            bootstrap_if_empty: sync_config.bootstrap_if_empty,
            reserved_bytes: sync_config.reserved_bytes.unwrap_or(0),
            partial_bootstrap_strategy: sync_config.partial_bootstrap_strategy.clone(),
        };
        let sync_engine_io_queue = SyncEngineIoQueue::new();
        let db_io: Arc<dyn IO> = if let Some(io) = sync_config.db_io.as_ref() {
            io.clone()
        } else {
            let is_memory = db_config.path == ":memory:";
            if is_memory {
                Arc::new(MemoryIO::new())
            } else {
                #[cfg(target_os = "linux")]
                {
                    if matches!(
                        sync_engine_opts.partial_bootstrap_strategy,
                        PartialBootstrapStrategy::None
                    ) {
                        Arc::new(turso_core::PlatformIO::new().map_err(|e| TursoError {
                            code: TursoStatusCode::Error,
                            message: Some(format!("Failed to create platform IO: {e}")),
                        })?)
                    } else {
                        use turso_sync_engine::sparse_io::SparseLinuxIo;

                        Arc::new(SparseLinuxIo::new().map_err(|e| TursoError {
                            code: TursoStatusCode::Error,
                            message: Some(format!("Failed to create sparse IO: {e}")),
                        })?)
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    Arc::new(turso_core::PlatformIO::new().map_err(|e| TursoError {
                        code: TursoStatusCode::Error,
                        message: Some(format!("Failed to create platform IO: {e}")),
                    })?)
                }
            }
        };
        Ok(Arc::new(Self {
            db_config,
            sync_config,
            sync_engine_opts,
            sync_engine_io_queue,
            db_io,
            sync_engine: Arc::new(Mutex::new(None)),
        }))
    }
    pub fn init(&self) -> Box<TursoDatabaseAsyncOperation> {
        let io = self.db_io.clone();
        let sync_engine_io = self.sync_engine_io_queue.clone();
        let main_db_path = self.sync_config.path.clone();
        let sync_engine_opts = self.sync_engine_opts.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let _ = database_sync_engine::DatabaseSyncEngine::bootstrap_db(
                &coro,
                io,
                sync_engine_io,
                &main_db_path,
                &sync_engine_opts,
            )
            .await?;
            Ok(None)
        }))
    }
    pub fn open(&self) -> Box<TursoDatabaseAsyncOperation> {
        let io = self.db_io.clone();
        let sync_engine_io = self.sync_engine_io_queue.clone();
        let main_db_path = self.sync_config.path.clone();
        let db_config = self.db_config.clone();
        let sync_engine_opts = self.sync_engine_opts.clone();
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let metadata = database_sync_engine::DatabaseSyncEngine::read_db_meta(
                &coro,
                sync_engine_io.clone(),
                &main_db_path,
            )
            .await?;
            let Some(metadata) = metadata else {
                return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                    "metadata not found".to_string(),
                ));
            };
            let db_file = database_sync_engine::DatabaseSyncEngine::init_db_storage(
                io.clone(),
                sync_engine_io.clone(),
                &metadata,
                &main_db_path,
                &sync_engine_opts,
            )?;
            let main_db = turso_sdk_kit::rsapi::TursoDatabase::new(
                turso_sdk_kit::rsapi::TursoDatabaseConfig {
                    db_file: Some(db_file),
                    io: Some(io),
                    ..db_config
                },
            );
            main_db.open().map_err(|e| {
                turso_sync_engine::errors::Error::DatabaseSyncEngineError(format!(
                    "unable to open database file: {e}"
                ))
            })?;
            let main_db_core = main_db.db_core().map_err(|e| {
                turso_sync_engine::errors::Error::DatabaseSyncEngineError(format!(
                    "unable to get core database instance: {e}",
                ))
            })?;
            let sync_engine_opened = database_sync_engine::DatabaseSyncEngine::open_db(
                &coro,
                sync_engine_io,
                main_db_core,
                sync_engine_opts,
            )
            .await?;
            *sync_engine.lock().unwrap() = Some(sync_engine_opened);
            Ok(None)
        }))
    }
    pub fn create(&self) -> Box<TursoDatabaseAsyncOperation> {
        let io = self.db_io.clone();
        let sync_engine_io = self.sync_engine_io_queue.clone();
        let main_db_path = self.sync_config.path.clone();
        let db_config = self.db_config.clone();
        let sync_engine_opts = self.sync_engine_opts.clone();
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let metadata = database_sync_engine::DatabaseSyncEngine::bootstrap_db(
                &coro,
                io.clone(),
                sync_engine_io.clone(),
                &main_db_path,
                &sync_engine_opts,
            )
            .await?;
            let db_file = database_sync_engine::DatabaseSyncEngine::init_db_storage(
                io.clone(),
                sync_engine_io.clone(),
                &metadata,
                &main_db_path,
                &sync_engine_opts,
            )?;
            let main_db = turso_sdk_kit::rsapi::TursoDatabase::new(
                turso_sdk_kit::rsapi::TursoDatabaseConfig {
                    db_file: Some(db_file),
                    io: Some(io),
                    ..db_config
                },
            );
            main_db.open().map_err(|e| {
                turso_sync_engine::errors::Error::DatabaseSyncEngineError(format!(
                    "unable to open database file: {e}"
                ))
            })?;
            let main_db_core = main_db.db_core().map_err(|e| {
                turso_sync_engine::errors::Error::DatabaseSyncEngineError(format!(
                    "unable to get core database instance: {e}",
                ))
            })?;
            let sync_engine_opened = database_sync_engine::DatabaseSyncEngine::open_db(
                &coro,
                sync_engine_io,
                main_db_core,
                sync_engine_opts,
            )
            .await?;
            *sync_engine.lock().unwrap() = Some(sync_engine_opened);
            Ok(None)
        }))
    }

    pub fn connect(&self) -> Box<TursoDatabaseAsyncOperation> {
        let db_config = self.db_config.clone();
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let sync_engine = sync_engine.lock().unwrap();
            let Some(sync_engine) = &*sync_engine else {
                return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                    "sync engine must be initialized".to_string(),
                ));
            };
            let connection = sync_engine.connect_rw(&coro).await?;
            Ok(Some(TursoAsyncOperationResult::Connection {
                connection: turso_sdk_kit::rsapi::TursoConnection::new(&db_config, connection),
            }))
        }))
    }

    pub fn stats(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let sync_engine = sync_engine.lock().unwrap();
            let Some(sync_engine) = &*sync_engine else {
                return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                    "sync engine must be initialized".to_string(),
                ));
            };
            let stats = sync_engine.stats(&coro).await?;
            Ok(Some(TursoAsyncOperationResult::Stats { stats: stats }))
        }))
    }
    pub fn checkpoint(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let sync_engine = sync_engine.lock().unwrap();
            let Some(sync_engine) = &*sync_engine else {
                return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                    "sync engine must be initialized".to_string(),
                ));
            };
            sync_engine.checkpoint(&coro).await?;
            Ok(None)
        }))
    }
    pub fn push_changes(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let sync_engine = sync_engine.lock().unwrap();
            let Some(sync_engine) = &*sync_engine else {
                return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                    "sync engine must be initialized".to_string(),
                ));
            };
            sync_engine.push_changes_to_remote(&coro).await?;
            Ok(None)
        }))
    }
    pub fn wait_changes(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let sync_engine = sync_engine.lock().unwrap();
            let Some(sync_engine) = &*sync_engine else {
                return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                    "sync engine must be initialized".to_string(),
                ));
            };
            let changes = sync_engine.wait_changes_from_remote(&coro).await?;
            Ok(Some(TursoAsyncOperationResult::Changes {
                changes: Box::new(TursoDatabaseSyncChanges { changes }),
            }))
        }))
    }
    pub fn apply_changes(
        &self,
        changes: Box<TursoDatabaseSyncChanges>,
    ) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(async move |coro| {
            let sync_engine = sync_engine.lock().unwrap();
            let Some(sync_engine) = &*sync_engine else {
                return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                    "sync engine must be initialized".to_string(),
                ));
            };
            let changes = changes.changes;
            sync_engine
                .apply_changes_from_remote(&coro, changes)
                .await?;
            Ok(None)
        }))
    }

    pub fn take_io_item(&self) -> Option<Box<sync_engine_io::SyncEngineIoQueueItem<TBytes>>> {
        self.sync_engine_io_queue.pop_front()
    }

    pub fn step_io_callbacks(&self) {
        self.sync_engine_io_queue.step_io_callbacks();
    }

    /// helper method to get C raw container to the TursoDatabaseSync instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self: Arc<Self>) -> capi::c::turso_sync_database_t {
        capi::c::turso_sync_database_t {
            inner: Arc::into_raw(self.clone()) as *mut std::ffi::c_void,
        }
    }

    /// TODO
    pub unsafe fn ref_from_capi<'a>(
        value: capi::c::turso_sync_database_t,
    ) -> Result<&'a Self, TursoError> {
        if value.inner.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&*(value.inner as *const Self))
        }
    }

    /// TODO
    pub unsafe fn arc_from_capi(value: capi::c::turso_sync_database_t) -> Arc<Self> {
        Arc::from_raw(value.inner as *const Self)
    }
}
