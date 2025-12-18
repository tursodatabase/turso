use std::sync::Arc;

use parking_lot::Mutex;
use turso_core::{MemoryIO, IO};
use turso_sdk_kit::rsapi::{str_from_c_str, TursoError, TursoStatusCode};
use turso_sync_engine::{
    database_sync_engine::{self, DatabaseSyncEngine},
    database_sync_engine_io::SyncEngineIo,
    database_sync_operations::SyncEngineIoStats,
};

use crate::{
    capi,
    sync_engine_io::{self, SyncEngineIoQueue},
    turso_async_operation::{TursoAsyncOperationResult, TursoDatabaseAsyncOperation},
};

#[derive(Clone)]
pub struct TursoDatabaseSyncConfig {
    pub path: String,
    pub client_name: String,
    pub long_poll_timeout_ms: Option<u32>,
    pub bootstrap_if_empty: bool,
    pub reserved_bytes: Option<usize>,
    pub partial_sync_opts: Option<turso_sync_engine::types::PartialSyncOpts>,
    pub db_io: Option<Arc<dyn IO>>,
}

pub type PartialSyncOpts = turso_sync_engine::types::PartialSyncOpts;
pub type PartialBootstrapStrategy = turso_sync_engine::types::PartialBootstrapStrategy;

impl TursoDatabaseSyncConfig {
    /// helper method to restore [TursoDatabaseSyncConfig] instance from C representation
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// [capi::c::turso_sync_database_config_t::path] field must be valid C-string pointer
    /// [capi::c::turso_sync_database_config_t::client_name] field must be valid C-string pointer
    /// [capi::c::turso_sync_database_config_t::partial_bootstrap_strategy_query] field must be valid C-string pointer or null
    pub unsafe fn from_capi(
        config: *const capi::c::turso_sync_database_config_t,
    ) -> Result<Self, turso_sdk_kit::rsapi::TursoError> {
        if config.is_null() {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("config pointer must be not null".to_string()),
            });
        }
        let config = *config;
        Ok(Self {
            path: str_from_c_str(config.path)?.to_string(),
            client_name: str_from_c_str(config.client_name)?.to_string(),
            long_poll_timeout_ms: if config.long_poll_timeout_ms == 0 {
                None
            } else {
                Some(config.long_poll_timeout_ms as u32)
            },
            bootstrap_if_empty: config.bootstrap_if_empty,
            reserved_bytes: if config.reserved_bytes == 0 {
                None
            } else {
                Some(config.reserved_bytes as usize)
            },
            partial_sync_opts: if config.partial_bootstrap_strategy_prefix != 0 {
                Some(turso_sync_engine::types::PartialSyncOpts {
                    bootstrap_strategy:
                        turso_sync_engine::types::PartialBootstrapStrategy::Prefix {
                            length: config.partial_bootstrap_strategy_prefix as usize,
                        },
                    segment_size: config.partial_bootstrap_segment_size,
                    prefetch: config.partial_bootstrap_prefetch,
                })
            } else if !config.partial_bootstrap_strategy_query.is_null() {
                let query = str_from_c_str(config.partial_bootstrap_strategy_query)?;
                Some(turso_sync_engine::types::PartialSyncOpts {
                    bootstrap_strategy: turso_sync_engine::types::PartialBootstrapStrategy::Query {
                        query: query.to_string(),
                    },
                    segment_size: config.partial_bootstrap_segment_size,
                    prefetch: config.partial_bootstrap_prefetch,
                })
            } else {
                None
            },
            db_io: None,
        })
    }
}

pub struct TursoDatabaseSyncChanges {
    changes: turso_sync_engine::types::DbChangesStatus,
}

impl TursoDatabaseSyncChanges {
    pub fn empty(&self) -> bool {
        self.changes.file_slot.is_none()
    }
    pub fn to_capi(self: Box<Self>) -> *mut capi::c::turso_sync_changes_t {
        Box::into_raw(self) as *mut capi::c::turso_sync_changes_t
    }
    /// helper method to restore [TursoDatabaseSyncChanges] ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: *mut capi::c::turso_sync_changes_t,
    ) -> Result<&'a Self, TursoError> {
        if value.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&*(value as *const Self))
        }
    }
    /// helper method to restore [TursoDatabaseSyncChanges] instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn box_from_capi(value: *const capi::c::turso_sync_changes_t) -> Box<Self> {
        Box::from_raw(value as *mut Self)
    }
}

pub struct TursoDatabaseSync<TBytes: AsRef<[u8]> + Send + Sync + 'static> {
    db_config: turso_sdk_kit::rsapi::TursoDatabaseConfig,
    sync_config: TursoDatabaseSyncConfig,
    sync_engine_opts: turso_sync_engine::database_sync_engine::DatabaseSyncEngineOpts,
    db_io: Arc<dyn IO>,
    sync_engine_io_queue: SyncEngineIoStats<SyncEngineIoQueue<TBytes>>,
    sync_engine: Arc<Mutex<Option<DatabaseSyncEngine<SyncEngineIoQueue<TBytes>>>>>,
}

impl<TBytes: AsRef<[u8]> + Send + Sync + 'static> TursoDatabaseSync<TBytes> {
    /// create database sync holder struct but do not initialize it yet
    /// this can be useful for some environments, where IO operations must be executed in certain fashion (and open do IO under the hood)
    pub fn new(
        db_config: turso_sdk_kit::rsapi::TursoDatabaseConfig,
        sync_config: TursoDatabaseSyncConfig,
    ) -> Result<Arc<Self>, turso_sdk_kit::rsapi::TursoError> {
        let sync_engine_opts = turso_sync_engine::database_sync_engine::DatabaseSyncEngineOpts {
            client_name: sync_config.client_name.clone(),
            tables_ignore: vec![],
            use_transform: false,
            wal_pull_batch_size: 0,
            long_poll_timeout: sync_config
                .long_poll_timeout_ms
                .map(|t| std::time::Duration::from_millis(t as u64)),
            protocol_version_hint: turso_sync_engine::types::DatabaseSyncEngineProtocolVersion::V1,
            bootstrap_if_empty: sync_config.bootstrap_if_empty,
            reserved_bytes: sync_config.reserved_bytes.unwrap_or(0),
            partial_sync_opts: sync_config.partial_sync_opts.clone(),
        };
        let is_memory = db_config.path == ":memory:";
        let db_io: Arc<dyn IO> = if let Some(io) = sync_config.db_io.as_ref() {
            io.clone()
        } else if is_memory {
            Arc::new(MemoryIO::new())
        } else {
            #[cfg(target_os = "linux")]
            {
                if sync_engine_opts.partial_sync_opts.is_none() {
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
        };
        let sync_engine_io_queue = SyncEngineIoStats::new(SyncEngineIoQueue::new());
        Ok(Arc::new(Self {
            db_config,
            sync_config,
            sync_engine_opts,
            sync_engine_io_queue,
            db_io,
            sync_engine: Arc::new(Mutex::new(None)),
        }))
    }
    /// initialize database on disk and bootstrap if necessary (bootstrap requires network availability)
    pub fn init(&self) -> Box<TursoDatabaseAsyncOperation> {
        let io = self.db_io.clone();
        let sync_engine_io = self.sync_engine_io_queue.clone();
        let main_db_path = self.sync_config.path.clone();
        let sync_engine_opts = self.sync_engine_opts.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let _ = database_sync_engine::DatabaseSyncEngine::bootstrap_db(
                    &coro,
                    io,
                    sync_engine_io,
                    &main_db_path,
                    &sync_engine_opts,
                )
                .await?;
                Ok(None)
            })
        })))
    }
    /// open the database which must be created earlier (e.g. through [Self::init])
    pub fn open(&self) -> Box<TursoDatabaseAsyncOperation> {
        let io = self.db_io.clone();
        let sync_engine_io = self.sync_engine_io_queue.clone();
        let main_db_path = self.sync_config.path.clone();
        let db_config = self.db_config.clone();
        let sync_engine_opts = self.sync_engine_opts.clone();
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let metadata = database_sync_engine::DatabaseSyncEngine::read_db_meta(
                    &coro,
                    io.clone(),
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
                        io: Some(io.clone()),
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
                    io,
                    sync_engine_io,
                    main_db_core,
                    sync_engine_opts,
                )
                .await?;
                *sync_engine.lock() = Some(sync_engine_opened);
                Ok(None)
            })
        })))
    }
    /// initialize and open the database
    pub fn create(&self) -> Box<TursoDatabaseAsyncOperation> {
        let io = self.db_io.clone();
        let sync_engine_io = self.sync_engine_io_queue.clone();
        let main_db_path = self.sync_config.path.clone();
        let db_config = self.db_config.clone();
        let sync_engine_opts = self.sync_engine_opts.clone();
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
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
                        io: Some(io.clone()),
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
                    io,
                    sync_engine_io,
                    main_db_core,
                    sync_engine_opts,
                )
                .await?;
                *sync_engine.lock() = Some(sync_engine_opened);
                Ok(None)
            })
        })))
    }

    /// create tursodb connection for already opened database (with [Self::open] or [Self::create] methods)
    pub fn connect(&self) -> Box<TursoDatabaseAsyncOperation> {
        let db_config = self.db_config.clone();
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let sync_engine = sync_engine.lock_arc();
                let Some(sync_engine) = &*sync_engine else {
                    return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                        "sync engine must be initialized".to_string(),
                    ));
                };
                let connection = sync_engine.connect_rw(&coro).await?;
                Ok(Some(TursoAsyncOperationResult::Connection {
                    connection: turso_sdk_kit::rsapi::TursoConnection::new(&db_config, connection),
                }))
            })
        })))
    }

    /// get stats of synced database
    pub fn stats(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let sync_engine = sync_engine.lock_arc();
                let Some(sync_engine) = &*sync_engine else {
                    return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                        "sync engine must be initialized".to_string(),
                    ));
                };
                let stats = sync_engine.stats(&coro).await?;
                Ok(Some(TursoAsyncOperationResult::Stats { stats }))
            })
        })))
    }
    /// checkpoint WAL of synced database
    pub fn checkpoint(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let sync_engine = sync_engine.lock_arc();
                let Some(sync_engine) = &*sync_engine else {
                    return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                        "sync engine must be initialized".to_string(),
                    ));
                };
                sync_engine.checkpoint(&coro).await?;
                Ok(None)
            })
        })))
    }
    /// push local changes to remote for synced database
    pub fn push_changes(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let sync_engine = sync_engine.lock_arc();
                let Some(sync_engine) = &*sync_engine else {
                    return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                        "sync engine must be initialized".to_string(),
                    ));
                };
                sync_engine.push_changes_to_remote(&coro).await?;
                Ok(None)
            })
        })))
    }
    /// wait changes from remote to apply them later with [Self::apply_changes] methods
    pub fn wait_changes(&self) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let sync_engine = sync_engine.lock_arc();
                let Some(sync_engine) = &*sync_engine else {
                    return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                        "sync engine must be initialized".to_string(),
                    ));
                };
                let changes = sync_engine.wait_changes_from_remote(&coro).await?;
                Ok(Some(TursoAsyncOperationResult::Changes {
                    changes: Box::new(TursoDatabaseSyncChanges { changes }),
                }))
            })
        })))
    }
    /// apply changes from remote locally fetched with [Self::wait_changes] method
    pub fn apply_changes(
        &self,
        changes: Box<TursoDatabaseSyncChanges>,
    ) -> Box<TursoDatabaseAsyncOperation> {
        let sync_engine = self.sync_engine.clone();
        Box::new(TursoDatabaseAsyncOperation::new(Box::new(move |coro| {
            Box::pin(async move {
                let sync_engine = sync_engine.lock_arc();
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
            })
        })))
    }

    /// take sync engine IO item to process
    /// note, that sync engine extends IO operation from tursodatabase with atomic file operations and HTTP
    /// that's why there is another flow to process sync-engine specific IO operations
    pub fn take_io_item(&self) -> Option<Box<sync_engine_io::SyncEngineIoQueueItem<TBytes>>> {
        self.sync_engine_io_queue.pop_front()
    }

    /// run synced database extra callbacks after execution of IO operation on the caller side
    pub fn step_io_callbacks(&self) {
        self.sync_engine_io_queue.step_io_callbacks();
    }

    /// helper method to get C raw container to the TursoDatabaseSync instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self: Arc<Self>) -> *mut capi::c::turso_sync_database_t {
        Arc::into_raw(self.clone()) as *mut capi::c::turso_sync_database_t
    }

    /// helper method to restore [TursoDatabaseSync] ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: *const capi::c::turso_sync_database_t,
    ) -> Result<&'a Self, TursoError> {
        if value.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&*(value as *const Self))
        }
    }

    /// helper method to restore [TursoDatabaseSync] instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn arc_from_capi(value: *const capi::c::turso_sync_database_t) -> Arc<Self> {
        Arc::from_raw(value as *const Self)
    }
}
