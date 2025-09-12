#![allow(clippy::await_holding_lock)]
#![allow(clippy::type_complexity)]

pub mod generator;
pub mod js_protocol_io;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use napi::bindgen_prelude::{AsyncTask, Either5, Null};
use napi_derive::napi;
use tracing_subscriber::{filter::LevelFilter, fmt::format::FmtSpan};
use turso_node::IoLoopTask;
use turso_sync_engine::{
    database_sync_engine::{DatabaseSyncEngine, DatabaseSyncEngineOpts},
    types::{Coro, DatabaseChangeType, DatabaseSyncEngineProtocolVersion},
};

use crate::{
    generator::{GeneratorHolder, GeneratorResponse},
    js_protocol_io::{JsProtocolIo, JsProtocolRequestBytes},
};

#[napi(object)]
pub struct DatabaseOpts {
    pub path: String,
}

#[napi]
pub struct SyncEngine {
    path: String,
    client_name: String,
    wal_pull_batch_size: u32,
    protocol_version: DatabaseSyncEngineProtocolVersion,
    tables_ignore: Vec<String>,
    use_transform: bool,
    io: Option<Arc<dyn turso_core::IO>>,
    protocol: Option<Arc<JsProtocolIo>>,
    sync_engine: Arc<RwLock<Option<DatabaseSyncEngine<JsProtocolIo>>>>,
    opened: Arc<Mutex<Option<turso_node::Database>>>,
}

#[napi]
pub enum DatabaseChangeTypeJs {
    Insert,
    Update,
    Delete,
}

#[napi]
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
        Either5::D(value) => turso_core::Value::Text(turso_core::types::Text::new(&value)),
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

#[napi(object, object_to_js = false)]
pub struct SyncEngineOpts {
    pub path: String,
    pub client_name: Option<String>,
    pub wal_pull_batch_size: Option<u32>,
    pub tracing: Option<String>,
    pub tables_ignore: Option<Vec<String>>,
    pub use_transform: bool,
    pub protocol_version: Option<SyncEngineProtocolVersion>,
}

static TRACING_INIT: OnceLock<()> = OnceLock::new();
pub fn init_tracing(level_filter: LevelFilter) {
    TRACING_INIT.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::ACTIVE)
            .with_max_level(level_filter)
            .init();
    });
}

#[napi]
impl SyncEngine {
    #[napi(constructor)]
    pub fn new(opts: SyncEngineOpts) -> napi::Result<Self> {
        // helpful for local debugging
        match opts.tracing.as_deref() {
            Some("info") => init_tracing(LevelFilter::INFO),
            Some("debug") => init_tracing(LevelFilter::DEBUG),
            Some("trace") => init_tracing(LevelFilter::TRACE),
            _ => {}
        }
        let is_memory = opts.path == ":memory:";
        let io: Arc<dyn turso_core::IO> = if is_memory {
            Arc::new(turso_core::MemoryIO::new())
        } else {
            #[cfg(not(feature = "browser"))]
            {
                Arc::new(turso_core::PlatformIO::new().map_err(|e| {
                    napi::Error::new(
                        napi::Status::GenericFailure,
                        format!("Failed to create IO: {e}"),
                    )
                })?)
            }
            #[cfg(feature = "browser")]
            {
                Arc::new(turso_node::browser::Opfs::new()?)
            }
        };
        Ok(SyncEngine {
            path: opts.path,
            client_name: opts.client_name.unwrap_or("turso-sync-js".to_string()),
            wal_pull_batch_size: opts.wal_pull_batch_size.unwrap_or(100),
            tables_ignore: opts.tables_ignore.unwrap_or_default(),
            use_transform: opts.use_transform,
            #[allow(clippy::arc_with_non_send_sync)]
            sync_engine: Arc::new(RwLock::new(None)),
            io: Some(io),
            protocol: Some(Arc::new(JsProtocolIo::default())),
            #[allow(clippy::arc_with_non_send_sync)]
            opened: Arc::new(Mutex::new(None)),
            protocol_version: match opts.protocol_version {
                Some(SyncEngineProtocolVersion::Legacy) | None => {
                    DatabaseSyncEngineProtocolVersion::Legacy
                }
                _ => DatabaseSyncEngineProtocolVersion::V1,
            },
        })
    }

    #[napi]
    pub fn init(&mut self) -> napi::Result<GeneratorHolder> {
        let opts = DatabaseSyncEngineOpts {
            client_name: self.client_name.clone(),
            wal_pull_batch_size: self.wal_pull_batch_size as u64,
            tables_ignore: self.tables_ignore.clone(),
            use_transform: self.use_transform,
            protocol_version_hint: self.protocol_version,
        };

        let io = self.io()?;
        let protocol = self.protocol()?;
        let sync_engine = self.sync_engine.clone();
        let opened = self.opened.clone();
        let path = self.path.clone();
        let generator = genawaiter::sync::Gen::new(|coro| async move {
            let coro = Coro::new((), coro);
            let initialized =
                DatabaseSyncEngine::new(&coro, io.clone(), protocol, &path, opts).await?;
            let connection = initialized.connect_rw(&coro).await?;
            let db = turso_node::Database::create(None, io.clone(), connection, path);

            *sync_engine.write().unwrap() = Some(initialized);
            *opened.lock().unwrap() = Some(db);
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
    pub fn sync(&self) -> GeneratorHolder {
        self.run(async move |coro, sync_engine| {
            let mut sync_engine = try_write(sync_engine)?;
            let sync_engine = try_unwrap_mut(&mut sync_engine)?;
            sync_engine.sync(coro).await?;
            Ok(None)
        })
    }

    #[napi]
    pub fn push(&self) -> GeneratorHolder {
        self.run(async move |coro, sync_engine| {
            let sync_engine = try_read(sync_engine)?;
            let sync_engine = try_unwrap(&sync_engine)?;
            sync_engine.push_changes_to_remote(coro).await?;
            Ok(None)
        })
    }

    #[napi]
    pub fn stats(&self) -> GeneratorHolder {
        self.run(async move |coro, sync_engine| {
            let sync_engine = try_read(sync_engine)?;
            let sync_engine = try_unwrap(&sync_engine)?;
            let changes = sync_engine.stats(coro).await?;
            Ok(Some(GeneratorResponse::SyncEngineStats {
                operations: changes.cdc_operations,
                main_wal: changes.main_wal_size as i64,
                revert_wal: changes.revert_wal_size as i64,
                last_pull_unix_time: changes.last_pull_unix_time,
                last_push_unix_time: changes.last_push_unix_time,
            }))
        })
    }

    #[napi]
    pub fn pull(&self) -> GeneratorHolder {
        self.run(async move |coro, sync_engine| {
            let mut sync_engine = try_write(sync_engine)?;
            let sync_engine = try_unwrap_mut(&mut sync_engine)?;
            sync_engine.pull_changes_from_remote(coro).await?;
            Ok(None)
        })
    }

    #[napi]
    pub fn checkpoint(&self) -> GeneratorHolder {
        self.run(async move |coro, sync_engine| {
            let mut sync_engine = try_write(sync_engine)?;
            let sync_engine = try_unwrap_mut(&mut sync_engine)?;
            sync_engine.checkpoint(coro).await?;
            Ok(None)
        })
    }

    #[napi]
    pub fn open(&self) -> napi::Result<turso_node::Database> {
        let opened = self.opened.lock().unwrap();
        let Some(opened) = opened.as_ref() else {
            return Err(napi::Error::new(
                napi::Status::GenericFailure,
                "sync_engine must be initialized".to_string(),
            ));
        };
        Ok(opened.clone())
    }

    #[napi]
    pub fn close(&mut self) {
        let _ = self.sync_engine.write().unwrap().take();
        let _ = self.opened.lock().unwrap().take().unwrap();
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

fn try_write(
    sync_engine: &RwLock<Option<DatabaseSyncEngine<JsProtocolIo>>>,
) -> turso_sync_engine::Result<RwLockWriteGuard<'_, Option<DatabaseSyncEngine<JsProtocolIo>>>> {
    let Ok(sync_engine) = sync_engine.try_write() else {
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

fn try_unwrap_mut<'a>(
    sync_engine: &'a mut RwLockWriteGuard<'_, Option<DatabaseSyncEngine<JsProtocolIo>>>,
) -> turso_sync_engine::Result<&'a mut DatabaseSyncEngine<JsProtocolIo>> {
    let Some(sync_engine) = sync_engine.as_mut() else {
        let error = "sync_engine must be initialized".to_string();
        return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
            error,
        ));
    };
    Ok(sync_engine)
}
