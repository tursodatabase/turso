#![allow(clippy::await_holding_lock)]
#![allow(clippy::type_complexity)]

pub mod generator;
pub mod js_protocol_io;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
};

use napi::bindgen_prelude::{AsyncTask, Either5, Null};
use napi_derive::napi;
use turso_node::{DatabaseOpts, IoLoopTask};
use turso_sync_engine::{
    database_sync_engine::{DatabaseSyncEngine, DatabaseSyncEngineOpts},
    database_sync_engine_io::SyncEngineIo,
    database_sync_operations::SyncEngineIoStats,
    types::{
        Coro, DatabaseChangeType, DatabaseSyncEngineProtocolVersion, PartialBootstrapStrategy,
        PartialSyncOpts,
    },
};

use crate::{
    generator::{GeneratorHolder, GeneratorResponse, SyncEngineChanges},
    js_protocol_io::{JsProtocolIo, JsProtocolRequestBytes},
};

#[napi]
pub struct SyncEngine {
    opts: SyncEngineOptsFilled,
    io: Option<Arc<dyn turso_core::IO>>,
    protocol: Option<Arc<JsProtocolIo>>,
    sync_engine: Arc<RwLock<Option<DatabaseSyncEngine<JsProtocolIo>>>>,
    db: Arc<Mutex<turso_node::Database>>,
}

#[napi(string_enum = "lowercase")]
pub enum DatabaseChangeTypeJs {
    Insert,
    Update,
    Delete,
}

#[napi(string_enum = "lowercase")]
pub enum SyncEngineProtocolVersion {
    Legacy,
    V1,
}

fn core_change_type_to_js(value: DatabaseChangeType) -> DatabaseChangeTypeJs {
    match value {
        DatabaseChangeType::Delete => DatabaseChangeTypeJs::Delete,
        DatabaseChangeType::Update => DatabaseChangeTypeJs::Update,
        DatabaseChangeType::Insert => DatabaseChangeTypeJs::Insert,
    }
}
fn js_value_to_core(value: Either5<Null, i64, f64, String, Vec<u8>>) -> turso_core::Value {
    match value {
        Either5::A(_) => turso_core::Value::Null,
        Either5::B(value) => turso_core::Value::Integer(value),
        Either5::C(value) => turso_core::Value::Float(value),
        Either5::D(value) => turso_core::Value::Text(turso_core::types::Text::new(value)),
        Either5::E(value) => turso_core::Value::Blob(value),
    }
}
fn core_value_to_js(value: turso_core::Value) -> Either5<Null, i64, f64, String, Vec<u8>> {
    match value {
        turso_core::Value::Null => Either5::<Null, i64, f64, String, Vec<u8>>::A(Null),
        turso_core::Value::Integer(value) => Either5::<Null, i64, f64, String, Vec<u8>>::B(value),
        turso_core::Value::Float(value) => Either5::<Null, i64, f64, String, Vec<u8>>::C(value),
        turso_core::Value::Text(value) => {
            Either5::<Null, i64, f64, String, Vec<u8>>::D(value.as_str().to_string())
        }
        turso_core::Value::Blob(value) => Either5::<Null, i64, f64, String, Vec<u8>>::E(value),
    }
}
fn core_values_map_to_js(
    value: HashMap<String, turso_core::Value>,
) -> HashMap<String, Either5<Null, i64, f64, String, Vec<u8>>> {
    let mut result = HashMap::new();
    for (key, value) in value {
        result.insert(key, core_value_to_js(value));
    }
    result
}

#[napi(object)]
pub struct DatabaseRowMutationJs {
    pub change_time: i64,
    pub table_name: String,
    pub id: i64,
    pub change_type: DatabaseChangeTypeJs,
    pub before: Option<HashMap<String, Either5<Null, i64, f64, String, Vec<u8>>>>,
    pub after: Option<HashMap<String, Either5<Null, i64, f64, String, Vec<u8>>>>,
    pub updates: Option<HashMap<String, Either5<Null, i64, f64, String, Vec<u8>>>>,
}

#[napi(object)]
#[derive(Debug)]
pub struct DatabaseRowStatementJs {
    pub sql: String,
    pub values: Vec<Either5<Null, i64, f64, String, Vec<u8>>>,
}

#[napi(discriminant = "type")]
#[derive(Debug)]
pub enum DatabaseRowTransformResultJs {
    Keep,
    Skip,
    Rewrite { stmt: DatabaseRowStatementJs },
}

#[napi(discriminant = "type")]
#[derive(Debug)]
pub enum JsPartialBootstrapStrategy {
    Prefix { length: i64 },
    Query { query: String },
}

#[napi(object)]
pub struct JsPartialSyncOpts {
    pub bootstrap_strategy: JsPartialBootstrapStrategy,
    pub segment_size: Option<i64>,
    pub prefetch: Option<bool>,
}

#[napi(object, object_to_js = false)]
pub struct SyncEngineOpts {
    pub path: String,
    pub remote_url: Option<String>,
    pub client_name: Option<String>,
    pub wal_pull_batch_size: Option<u32>,
    pub long_poll_timeout_ms: Option<u32>,
    pub tracing: Option<String>,
    pub tables_ignore: Option<Vec<String>>,
    pub use_transform: bool,
    pub protocol_version: Option<SyncEngineProtocolVersion>,
    pub bootstrap_if_empty: bool,
    pub remote_encryption: Option<String>,
    /// Base64-encoded encryption key for the Turso Cloud database.
    /// Must match the key used when creating the encrypted database.
    pub remote_encryption_key: Option<String>,
    pub partial_sync_opts: Option<JsPartialSyncOpts>,
}

struct SyncEngineOptsFilled {
    pub path: String,
    pub remote_url: Option<String>,
    pub client_name: String,
    pub wal_pull_batch_size: u32,
    pub long_poll_timeout: Option<std::time::Duration>,
    pub tables_ignore: Vec<String>,
    pub use_transform: bool,
    pub protocol_version: DatabaseSyncEngineProtocolVersion,
    pub bootstrap_if_empty: bool,
    pub remote_encryption: Option<CipherMode>,
    pub remote_encryption_key: Option<String>,
    pub partial_sync_opts: Option<PartialSyncOpts>,
}

#[derive(Debug, Clone, Copy)]
enum CipherMode {
    Aes256Gcm = 1,
    Aes128Gcm = 2,
    ChaCha20Poly1305 = 3,
    Aegis256 = 4,
}

impl CipherMode {
    /// Returns the nonce size for this cipher mode.
    pub fn required_nonce_size(&self) -> usize {
        match self {
            CipherMode::Aes256Gcm | CipherMode::Aes128Gcm | CipherMode::ChaCha20Poly1305 => 12,
            CipherMode::Aegis256 => 32,
        }
    }

    /// Returns the tag size for this cipher mode.
    pub fn required_tag_size(&self) -> usize {
        // All supported ciphers use 16-byte tags
        16
    }

    /// Returns the total metadata size (nonce + tag) for this cipher mode.
    fn required_metadata_size(&self) -> usize {
        self.required_nonce_size() + self.required_tag_size()
    }
}

#[napi]
impl SyncEngine {
    #[napi(constructor)]
    pub fn new(opts: SyncEngineOpts) -> napi::Result<Self> {
        let is_memory = opts.path == ":memory:";
        let io: Arc<dyn turso_core::IO> = if is_memory {
            Arc::new(turso_core::MemoryIO::new())
        } else {
            #[cfg(all(target_os = "linux", not(feature = "browser")))]
            {
                if opts.partial_sync_opts.is_none() {
                    Arc::new(turso_core::PlatformIO::new().map_err(|e| {
                        napi::Error::new(
                            napi::Status::GenericFailure,
                            format!("Failed to create platform IO: {e}"),
                        )
                    })?)
                } else {
                    use turso_sync_engine::sparse_io::SparseLinuxIo;

                    Arc::new(SparseLinuxIo::new().map_err(|e| {
                        napi::Error::new(
                            napi::Status::GenericFailure,
                            format!("Failed to create sparse IO: {e}"),
                        )
                    })?)
                }
            }
            #[cfg(all(not(target_os = "linux"), not(feature = "browser")))]
            {
                Arc::new(turso_core::PlatformIO::new().map_err(|e| {
                    napi::Error::new(
                        napi::Status::GenericFailure,
                        format!("Failed to create platform IO: {e}"),
                    )
                })?)
            }
            #[cfg(feature = "browser")]
            {
                turso_node::browser::opfs()
            }
        };
        #[allow(clippy::arc_with_non_send_sync)]
        let db = Arc::new(Mutex::new(turso_node::Database::new_with_io(
            opts.path.clone(),
            io.clone(),
            Some(DatabaseOpts {
                file_must_exist: None,
                readonly: None,
                timeout: None,
                tracing: opts.tracing.clone(),
            }),
        )?));
        let opts_filled = SyncEngineOptsFilled {
            path: opts.path,
            remote_url: opts.remote_url,
            client_name: opts
                .client_name
                .unwrap_or_else(|| "turso-sync-js".to_string()),
            wal_pull_batch_size: opts.wal_pull_batch_size.unwrap_or(100),
            long_poll_timeout: opts
                .long_poll_timeout_ms
                .map(|x| std::time::Duration::from_millis(x as u64)),
            tables_ignore: opts.tables_ignore.unwrap_or_default(),
            use_transform: opts.use_transform,
            protocol_version: match opts.protocol_version {
                Some(SyncEngineProtocolVersion::Legacy) | None => {
                    DatabaseSyncEngineProtocolVersion::Legacy
                }
                _ => DatabaseSyncEngineProtocolVersion::V1,
            },
            bootstrap_if_empty: opts.bootstrap_if_empty,
            remote_encryption: match opts.remote_encryption.as_deref() {
                Some("aes256gcm") => Some(CipherMode::Aes256Gcm),
                Some("aes128gcm") => Some(CipherMode::Aes128Gcm),
                Some("chacha20poly1305") => Some(CipherMode::ChaCha20Poly1305),
                Some("aegis256") => Some(CipherMode::Aegis256),
                None => None,
                _ => {
                    return Err(napi::Error::new(
                        napi::Status::GenericFailure,
                        "unsupported remote cipher",
                    ))
                }
            },
            partial_sync_opts: match opts.partial_sync_opts {
                Some(partial_sync_opts) => match partial_sync_opts.bootstrap_strategy {
                    JsPartialBootstrapStrategy::Prefix { length } => Some(PartialSyncOpts {
                        bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix {
                            length: length as usize,
                        }),
                        segment_size: partial_sync_opts.segment_size.unwrap_or(0) as usize,
                        prefetch: partial_sync_opts.prefetch.unwrap_or(false),
                    }),
                    JsPartialBootstrapStrategy::Query { query } => Some(PartialSyncOpts {
                        bootstrap_strategy: Some(PartialBootstrapStrategy::Query { query }),
                        segment_size: partial_sync_opts.segment_size.unwrap_or(0) as usize,
                        prefetch: partial_sync_opts.prefetch.unwrap_or(false),
                    }),
                },
                None => None,
            },
            remote_encryption_key: opts.remote_encryption_key.clone(),
        };
        Ok(SyncEngine {
            opts: opts_filled,
            #[allow(clippy::arc_with_non_send_sync)]
            sync_engine: Arc::new(RwLock::new(None)),
            io: Some(io),
            protocol: Some(Arc::new(JsProtocolIo::default())),
            #[allow(clippy::arc_with_non_send_sync)]
            db,
        })
    }

    #[napi]
    pub fn connect(&mut self) -> napi::Result<GeneratorHolder> {
        let opts = DatabaseSyncEngineOpts {
            client_name: self.opts.client_name.clone(),
            remote_url: self.opts.remote_url.clone(),
            wal_pull_batch_size: self.opts.wal_pull_batch_size as u64,
            long_poll_timeout: self.opts.long_poll_timeout,
            tables_ignore: self.opts.tables_ignore.clone(),
            use_transform: self.opts.use_transform,
            protocol_version_hint: self.opts.protocol_version,
            bootstrap_if_empty: self.opts.bootstrap_if_empty,
            reserved_bytes: self
                .opts
                .remote_encryption
                .map(|x| x.required_metadata_size())
                .unwrap_or(0),
            partial_sync_opts: self.opts.partial_sync_opts.clone(),
            remote_encryption_key: self.opts.remote_encryption_key.clone(),
        };

        let io = self.io()?;
        let protocol = self.protocol()?;
        let sync_engine = self.sync_engine.clone();
        let db = self.db.clone();
        let path = self.opts.path.clone();
        let generator = genawaiter::sync::Gen::new(|coro| async move {
            let coro = Coro::new((), coro);
            let initialized = DatabaseSyncEngine::create_db(
                &coro,
                io.clone(),
                SyncEngineIoStats::new(protocol),
                &path,
                opts,
            )
            .await?;
            let connection = initialized.connect_rw(&coro).await?;

            db.lock().unwrap().set_connected(connection).map_err(|e| {
                turso_sync_engine::errors::Error::DatabaseSyncEngineError(format!(
                    "failed to connect sync engine: {e}"
                ))
            })?;
            *sync_engine.write().unwrap() = Some(initialized);

            Ok(())
        });
        Ok(GeneratorHolder {
            #[allow(clippy::arc_with_non_send_sync)]
            generator: Arc::new(Mutex::new(generator)),
            response: Arc::new(Mutex::new(None)),
        })
    }

    #[napi]
    pub fn io_loop_sync(&self) -> napi::Result<()> {
        self.io()?.step().map_err(|e| {
            napi::Error::new(napi::Status::GenericFailure, format!("IO error: {e}"))
        })?;
        Ok(())
    }

    /// Runs the I/O loop asynchronously, returning a Promise.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn io_loop_async(&self) -> napi::Result<AsyncTask<IoLoopTask>> {
        let io = self.io()?;
        Ok(AsyncTask::new(IoLoopTask { io }))
    }

    #[napi]
    pub fn protocol_io(&self) -> napi::Result<Option<JsProtocolRequestBytes>> {
        Ok(self.protocol()?.take_request())
    }

    #[napi]
    pub fn protocol_io_step(&self) -> napi::Result<()> {
        self.protocol()?.step_io_callbacks();
        Ok(())
    }

    #[napi]
    pub fn push(&self) -> GeneratorHolder {
        self.run(async move |coro, guard| {
            let sync_engine = try_read(guard)?;
            let sync_engine = try_unwrap(&sync_engine)?;
            sync_engine.push_changes_to_remote(coro).await?;
            Ok(None)
        })
    }

    #[napi]
    pub fn stats(&self) -> GeneratorHolder {
        self.run(async move |coro, guard| {
            let sync_engine = try_read(guard)?;
            let sync_engine = try_unwrap(&sync_engine)?;
            let stats = sync_engine.stats(coro).await?;
            Ok(Some(GeneratorResponse::SyncEngineStats {
                cdc_operations: stats.cdc_operations,
                main_wal_size: stats.main_wal_size as i64,
                revert_wal_size: stats.revert_wal_size as i64,
                last_pull_unix_time: stats.last_pull_unix_time,
                last_push_unix_time: stats.last_push_unix_time,
                revision: stats.revision,
                network_sent_bytes: stats.network_sent_bytes as i64,
                network_received_bytes: stats.network_received_bytes as i64,
            }))
        })
    }

    #[napi]
    pub fn wait(&self) -> GeneratorHolder {
        self.run(async move |coro, guard| {
            let sync_engine = try_read(guard)?;
            let sync_engine = try_unwrap(&sync_engine)?;
            Ok(Some(GeneratorResponse::SyncEngineChanges {
                changes: SyncEngineChanges {
                    status: Box::new(Some(sync_engine.wait_changes_from_remote(coro).await?)),
                },
            }))
        })
    }

    #[napi]
    pub fn apply(&self, changes: &mut SyncEngineChanges) -> GeneratorHolder {
        let status = changes.status.take().unwrap();
        self.run(async move |coro, guard| {
            let sync_engine = try_read(guard)?;
            let sync_engine = try_unwrap(&sync_engine)?;
            sync_engine.apply_changes_from_remote(coro, status).await?;
            Ok(None)
        })
    }

    #[napi]
    pub fn checkpoint(&self) -> GeneratorHolder {
        self.run(async move |coro, guard| {
            let sync_engine = try_read(guard)?;
            let sync_engine = try_unwrap(&sync_engine)?;
            sync_engine.checkpoint(coro).await?;
            Ok(None)
        })
    }

    #[napi]
    pub fn db(&self) -> napi::Result<turso_node::Database> {
        Ok(self.db.lock().unwrap().clone())
    }

    #[napi]
    pub fn close(&mut self) {
        let _ = self.sync_engine.write().unwrap().take();
        let _ = self.db.lock().unwrap().close();
        let _ = self.io.take();
        let _ = self.protocol.take();
    }

    fn io(&self) -> napi::Result<Arc<dyn turso_core::IO>> {
        if self.io.is_none() {
            return Err(napi::Error::new(
                napi::Status::GenericFailure,
                "sync engine was closed",
            ));
        }
        Ok(self.io.as_ref().unwrap().clone())
    }
    fn protocol(&self) -> napi::Result<Arc<JsProtocolIo>> {
        if self.protocol.is_none() {
            return Err(napi::Error::new(
                napi::Status::GenericFailure,
                "sync engine was closed",
            ));
        }
        Ok(self.protocol.as_ref().unwrap().clone())
    }

    fn run(
        &self,
        f: impl AsyncFnOnce(
                &Coro<()>,
                &Arc<RwLock<Option<DatabaseSyncEngine<JsProtocolIo>>>>,
            ) -> turso_sync_engine::Result<Option<GeneratorResponse>>
            + 'static,
    ) -> GeneratorHolder {
        let response = Arc::new(Mutex::new(None));
        let sync_engine = self.sync_engine.clone();
        #[allow(clippy::await_holding_lock)]
        let generator = genawaiter::sync::Gen::new({
            let response = response.clone();
            |coro| async move {
                let coro = Coro::new((), coro);
                *response.lock().unwrap() = f(&coro, &sync_engine).await?;
                Ok(())
            }
        });
        GeneratorHolder {
            generator: Arc::new(Mutex::new(generator)),
            response,
        }
    }
}

fn try_read(
    sync_engine: &RwLock<Option<DatabaseSyncEngine<JsProtocolIo>>>,
) -> turso_sync_engine::Result<RwLockReadGuard<'_, Option<DatabaseSyncEngine<JsProtocolIo>>>> {
    let Ok(sync_engine) = sync_engine.try_read() else {
        let nasty_error = "sync_engine is busy".to_string();
        return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
            nasty_error,
        ));
    };
    Ok(sync_engine)
}

fn try_unwrap<'a>(
    sync_engine: &'a RwLockReadGuard<'_, Option<DatabaseSyncEngine<JsProtocolIo>>>,
) -> turso_sync_engine::Result<&'a DatabaseSyncEngine<JsProtocolIo>> {
    let Some(sync_engine) = sync_engine.as_ref() else {
        let error = "sync_engine must be initialized".to_string();
        return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
            error,
        ));
    };
    Ok(sync_engine)
}
