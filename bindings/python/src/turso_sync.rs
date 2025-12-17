use std::sync::Arc;

use pyo3::{
    pyclass, pyfunction, pymethods,
    types::{PyBytes, PyList, PyTuple},
    PyResult, Python,
};
use turso_sdk_kit::rsapi::{TursoDatabaseConfig, TursoStatusCode};
use turso_sync_sdk_kit::{
    rsapi::{
        self, PartialBootstrapStrategy, PartialSyncOpts, TursoDatabaseSync,
        TursoDatabaseSyncChanges,
    },
    sync_engine_io::SyncEngineIoQueueItem,
    turso_async_operation::{TursoAsyncOperationResult, TursoDatabaseAsyncOperation},
};

use crate::turso::{
    turso_error_to_py_err, Error, Misuse, PyTursoConnection, PyTursoDatabaseConfig,
};

#[pyclass]
#[derive(Clone)]
pub struct PyTursoPartialSyncOpts {
    // prefix bootstrap strategy which will enable partial sync which lazily pull necessary pages on demand and bootstrap db with pages from first N bytes of the db
    pub bootstrap_strategy_prefix: Option<usize>,
    // query bootstrap strategy which will enable partial sync which lazily pull necessary pages on demand and bootstrap db with pages touched by the server with given SQL query
    pub bootstrap_strategy_query: Option<String>,
    pub segment_size: Option<usize>,
    pub prefetch: Option<bool>,
}

#[pymethods]
impl PyTursoPartialSyncOpts {
    #[new]
    #[pyo3(signature = (
        bootstrap_strategy_prefix=None,
        bootstrap_strategy_query=None,
        segment_size=None,
        prefetch=None,
    ))]
    fn new(
        bootstrap_strategy_prefix: Option<usize>,
        bootstrap_strategy_query: Option<String>,
        segment_size: Option<usize>,
        prefetch: Option<bool>,
    ) -> Self {
        Self {
            bootstrap_strategy_prefix,
            bootstrap_strategy_query,
            segment_size,
            prefetch,
        }
    }
}

#[pyclass]
pub struct PyTursoSyncDatabaseConfig {
    // path to the main database file (auxilary files like metadata, WAL, revert, changes will derive names from this path)
    pub path: String,
    // arbitrary client name which will be used as a prefix for unique client id
    pub client_name: String,
    // long poll timeout for pull method (if set, server will hold connection for the given timeout until new changes will appear)
    pub long_poll_timeout_ms: Option<u32>,
    // bootstrap db if empty; if set - client will be able to connect to fresh db only when network is online
    pub bootstrap_if_empty: bool,
    // reserved bytes which must be set for the database - necessary if remote encryption is set for the db in cloud
    pub reserved_bytes: Option<usize>,
    pub partial_sync_opts: Option<PyTursoPartialSyncOpts>,
}

#[pymethods]
impl PyTursoSyncDatabaseConfig {
    #[new]
    #[pyo3(signature = (
        path,
        client_name,
        long_poll_timeout_ms=None,
        bootstrap_if_empty=true,
        reserved_bytes=None,
        partial_sync_opts=None,
    ))]
    fn new(
        path: String,
        client_name: String,
        long_poll_timeout_ms: Option<u32>,
        bootstrap_if_empty: bool,
        reserved_bytes: Option<usize>,
        partial_sync_opts: Option<&PyTursoPartialSyncOpts>,
    ) -> Self {
        Self {
            path,
            client_name,
            long_poll_timeout_ms,
            bootstrap_if_empty,
            reserved_bytes,
            partial_sync_opts: partial_sync_opts.cloned(),
        }
    }
}

/// Creates database sync holder but do not open it
#[pyfunction]
pub fn py_turso_sync_new(
    db_config: &PyTursoDatabaseConfig,
    sync_config: &PyTursoSyncDatabaseConfig,
) -> PyResult<PyTursoSyncDatabase> {
    let db_config = TursoDatabaseConfig {
        path: db_config.path.clone(),
        experimental_features: db_config.experimental_features.clone(),
        async_io: db_config.async_io,
        io: None,
        db_file: None,
    };
    let sync_config = rsapi::TursoDatabaseSyncConfig {
        path: sync_config.path.clone(),
        client_name: sync_config.client_name.clone(),
        bootstrap_if_empty: sync_config.bootstrap_if_empty,
        long_poll_timeout_ms: sync_config.long_poll_timeout_ms,
        reserved_bytes: sync_config.reserved_bytes,
        partial_sync_opts: match &sync_config.partial_sync_opts {
            Some(config) => {
                if let Some(length) = config.bootstrap_strategy_prefix {
                    Some(PartialSyncOpts {
                        bootstrap_strategy: PartialBootstrapStrategy::Prefix { length },
                        segment_size: config.segment_size.unwrap_or(0),
                        prefetch: config.prefetch.unwrap_or(false),
                    })
                } else {
                    config
                        .bootstrap_strategy_query
                        .as_ref()
                        .map(|query| PartialSyncOpts {
                            bootstrap_strategy: PartialBootstrapStrategy::Query {
                                query: query.clone(),
                            },
                            segment_size: config.segment_size.unwrap_or(0),
                            prefetch: config.prefetch.unwrap_or(false),
                        })
                }
            }
            None => None,
        },
        db_io: None,
    };
    let database =
        TursoDatabaseSync::<Vec<u8>>::new(db_config, sync_config).map_err(turso_error_to_py_err)?;
    Ok(PyTursoSyncDatabase { database })
}

#[pyclass]
pub struct PyTursoSyncDatabase {
    database: Arc<rsapi::TursoDatabaseSync<Vec<u8>>>,
}

#[pyclass]
pub struct PyTursoAsyncOperation {
    operation: Box<TursoDatabaseAsyncOperation>,
}

#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)] // Add necessary traits for your use case
pub enum PyTursoAsyncOperationResultKind {
    /// async operation has no return value ("void" operation)
    /// Note, that Python bindings have "No" as value because it's impossible to use "None" language keyword here
    No = 0,
    /// async operation returned [PyTursoConnection] instance
    Connection = 1,
    /// async operation returned [PyTursoSyncDatabaseStats]
    Stats = 2,
    /// async operation returned [PyTursoSyncDatabaseChanges]
    Changes = 3,
}

#[pyclass]
pub struct PyTursoSyncDatabaseStats {
    #[pyo3(get)]
    pub cdc_operations: i64,
    #[pyo3(get)]
    pub main_wal_size: u64,
    #[pyo3(get)]
    pub revert_wal_size: i64,
    #[pyo3(get)]
    pub last_pull_unix_time: Option<i64>,
    #[pyo3(get)]
    pub last_push_unix_time: Option<i64>,
    #[pyo3(get)]
    pub revision: Option<String>,
    #[pyo3(get)]
    pub network_sent_bytes: i64,
    #[pyo3(get)]
    pub network_received_bytes: i64,
}

#[pymethods]
impl PyTursoSyncDatabaseStats {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "PyTursoSyncDatabaseStats(
    cdc_operations={}, 
    main_wal_size={}, 
    revert_wal_size={}, 
    last_pull_unix_time={}, 
    last_push_unix_time={}, 
    revision={}, 
    network_sent_bytes={}, 
    network_received_bytes={}
)",
            self.cdc_operations,
            self.main_wal_size,
            self.revert_wal_size,
            self.last_pull_unix_time
                .map(|x| x.to_string())
                .unwrap_or("None".to_string()),
            self.last_push_unix_time
                .map(|x| x.to_string())
                .unwrap_or("None".to_string()),
            self.revision
                .as_ref()
                .map(|x| format!("\"{x}\""))
                .unwrap_or("None".to_string()),
            self.network_sent_bytes,
            self.network_received_bytes,
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

/// changes container fetched from remote; must be passed to the [PyTursoSyncDatabase::apply_changes] method
#[pyclass]
pub struct PyTursoSyncDatabaseChanges {
    changes: Option<Box<TursoDatabaseSyncChanges>>,
}

#[pymethods]
impl PyTursoSyncDatabaseChanges {
    /// check if some changes were fetched from remote
    pub fn empty(&self) -> PyResult<bool> {
        let Some(changes) = &self.changes else {
            return Err(Misuse::new_err("changes were already applied".to_string()));
        };
        Ok(changes.empty())
    }
}

#[pyclass]
pub struct PyTursoAsyncOperationResult {
    #[pyo3(get)]
    pub kind: PyTursoAsyncOperationResultKind,
    #[pyo3(get)]
    pub connection: Option<pyo3::Py<PyTursoConnection>>,
    #[pyo3(get)]
    pub changes: Option<pyo3::Py<PyTursoSyncDatabaseChanges>>,
    #[pyo3(get)]
    pub stats: Option<pyo3::Py<PyTursoSyncDatabaseStats>>,
}

#[pymethods]
impl PyTursoAsyncOperation {
    /// Resume async operation execution
    /// If returns Ok(None) - operation is not finished yet and must be resumed after one iteration of sync engine IO
    /// If returns Ok(Some(...)) - operation is finished and result must be processed accordingly
    pub fn resume(&self, py: Python) -> PyResult<Option<PyTursoAsyncOperationResult>> {
        let result = self.operation.resume().map_err(turso_error_to_py_err)?;
        if result == TursoStatusCode::Io {
            Ok(None)
        } else if result == TursoStatusCode::Done {
            let result = self.operation.take_result();
            match result {
                Ok(TursoAsyncOperationResult::Changes { changes }) => {
                    Ok(Some(PyTursoAsyncOperationResult {
                        kind: PyTursoAsyncOperationResultKind::Changes,
                        changes: Some(pyo3::Py::new(
                            py,
                            PyTursoSyncDatabaseChanges {
                                changes: Some(changes),
                            },
                        )?),
                        connection: None,
                        stats: None,
                    }))
                }
                Ok(TursoAsyncOperationResult::Connection { connection }) => {
                    Ok(Some(PyTursoAsyncOperationResult {
                        kind: PyTursoAsyncOperationResultKind::Connection,
                        changes: None,
                        connection: Some(pyo3::Py::new(py, PyTursoConnection { connection })?),
                        stats: None,
                    }))
                }
                Ok(TursoAsyncOperationResult::Stats { stats }) => {
                    Ok(Some(PyTursoAsyncOperationResult {
                        kind: PyTursoAsyncOperationResultKind::Stats,
                        changes: None,
                        connection: None,
                        stats: Some(pyo3::Py::new(
                            py,
                            PyTursoSyncDatabaseStats {
                                cdc_operations: stats.cdc_operations,
                                main_wal_size: stats.main_wal_size,
                                revert_wal_size: stats.revert_wal_size as i64,
                                last_pull_unix_time: stats.last_pull_unix_time,
                                last_push_unix_time: stats.last_push_unix_time,
                                revision: stats.revision,
                                network_sent_bytes: stats.network_sent_bytes as i64,
                                network_received_bytes: stats.network_received_bytes as i64,
                            },
                        )?),
                    }))
                }
                Err(..) => Ok(Some(PyTursoAsyncOperationResult {
                    kind: PyTursoAsyncOperationResultKind::No,
                    changes: None,
                    connection: None,
                    stats: None,
                })),
            }
        } else {
            Err(Error::new_err("unexpected resume status".to_string()))
        }
    }
}

#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)] // Add necessary traits for your use case
pub enum PyTursoSyncIoItemRequestKind {
    /// HTTP IO operation
    Http = 0,
    /// Atomic read IO operation; in case of not found error - sync engine expects empty response (not error) from the caller
    FullRead = 1,
    /// Atomic write IO operation
    FullWrite = 2,
}

#[pyclass]
pub struct PyTursoSyncIoItemHttpRequest {
    /// HTTP method (e.g. POST / GET)
    #[pyo3(get)]
    pub method: String,
    /// HTTP path (url is controlled outside of the sync engine)
    #[pyo3(get)]
    pub path: String,
    /// optional body of the request
    #[pyo3(get)]
    pub body: Option<pyo3::Py<PyBytes>>,
    /// Headers as list of tuples: list[(str, str)]
    #[pyo3(get)]
    pub headers: pyo3::Py<PyList>,
}

#[pyclass]
pub struct PyTursoSyncIoItemFullReadRequest {
    /// path of the file to read
    #[pyo3(get)]
    pub path: String,
}

#[pyclass]
pub struct PyTursoSyncIoItemFullWriteRequest {
    /// path of the file to write
    #[pyo3(get)]
    pub path: String,
    /// content of the file to write
    #[pyo3(get)]
    pub content: pyo3::Py<PyBytes>,
}

#[pyclass]
pub struct PyTursoSyncIoItemRequest {
    #[pyo3(get)]
    pub kind: PyTursoSyncIoItemRequestKind,
    #[pyo3(get)]
    pub http: Option<pyo3::Py<PyTursoSyncIoItemHttpRequest>>,
    #[pyo3(get)]
    pub full_read: Option<pyo3::Py<PyTursoSyncIoItemFullReadRequest>>,
    #[pyo3(get)]
    pub full_write: Option<pyo3::Py<PyTursoSyncIoItemFullWriteRequest>>,
}

#[pyclass]
pub struct PyTursoSyncIoItem {
    item: Box<SyncEngineIoQueueItem<Vec<u8>>>,
}

#[pymethods]
impl PyTursoSyncIoItem {
    /// Get IO request representation from the sync engine IO queue item
    pub fn request(&self, py: pyo3::Python) -> PyResult<PyTursoSyncIoItemRequest> {
        match self.item.get_request() {
            turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::Http {
                method,
                path,
                body,
                headers,
            } => Ok(PyTursoSyncIoItemRequest {
                kind: PyTursoSyncIoItemRequestKind::Http,
                full_read: None,
                full_write: None,
                http: Some(pyo3::Py::new(
                    py,
                    PyTursoSyncIoItemHttpRequest {
                        method: method.clone(),
                        path: path.clone(),
                        body: body
                            .as_ref()
                            .map(|body| PyBytes::new(py, body.as_ref()).unbind()),
                        headers: {
                            let mut tuples = Vec::new();
                            for (key, value) in headers {
                                tuples
                                    .push(PyTuple::new(py, [key.clone(), value.clone()])?.unbind());
                            }
                            PyList::new(py, tuples.into_iter())?.unbind()
                        },
                    },
                )?),
            }),
            turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullRead { path } => {
                Ok(PyTursoSyncIoItemRequest {
                    kind: PyTursoSyncIoItemRequestKind::FullRead,
                    full_read: Some(pyo3::Py::new(
                        py,
                        PyTursoSyncIoItemFullReadRequest { path: path.clone() },
                    )?),
                    full_write: None,
                    http: None,
                })
            }
            turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullWrite {
                path,
                content,
            } => Ok(PyTursoSyncIoItemRequest {
                kind: PyTursoSyncIoItemRequestKind::FullWrite,
                full_read: None,
                full_write: Some(pyo3::Py::new(
                    py,
                    PyTursoSyncIoItemFullWriteRequest {
                        path: path.clone(),
                        content: PyBytes::new(py, content.as_ref()).unbind(),
                    },
                )?),
                http: None,
            }),
        }
    }
    /// set error as the final completion result of the IO queue item
    pub fn poison(&self, error: String) {
        self.item.get_completion().poison(error);
    }
    /// set IO completion as finished successfully ([Self::done] and [Self::poison] are mutually exclusive)
    pub fn done(&self) {
        self.item.get_completion().done();
    }
    /// push bytes to the IO completion
    pub fn push_buffer(&self, buffer: &[u8]) {
        self.item.get_completion().push_buffer(buffer.to_vec());
    }
    /// set HTTP status to the IO completion
    pub fn status(&self, status: u32) {
        self.item.get_completion().status(status);
    }
}

#[pymethods]
impl PyTursoSyncDatabase {
    /// Prepare synced database for use (bootstrap if needed, setup necessary database parameters for first access)
    /// AsyncOperation returns No
    pub fn init(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.init(),
        }
    }
    /// Open prepared synced database, fail if no properly setup database exists
    /// AsyncOperation returns No
    pub fn open(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.open(),
        }
    }
    /// Prepare synced database and open it
    /// AsyncOperation returns No
    pub fn create(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.create(),
        }
    }
    /// Create [PyTursoConnection] connection
    /// synced database must be opened before that operation (with either turso_database_sync_create or turso_database_sync_open)
    /// AsyncOperation returns Connection
    pub fn connect(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.connect(),
        }
    }
    /// Collect stats about synced database
    /// AsyncOperation returns Stats
    pub fn stats(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.stats(),
        }
    }
    /// Checkpoint WAL of the synced database
    /// AsyncOperation returns No
    pub fn checkpoint(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.checkpoint(),
        }
    }
    /// Push local changes to remote
    /// AsyncOperation returns No
    pub fn push_changes(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.push_changes(),
        }
    }
    /// Wait for remote changes
    /// AsyncOperation returns Changes
    pub fn wait_changes(&self) -> PyTursoAsyncOperation {
        PyTursoAsyncOperation {
            operation: self.database.wait_changes(),
        }
    }
    /// Apply remote changes locally
    /// AsyncOperation returns No
    pub fn apply_changes(
        &self,
        changes: &mut PyTursoSyncDatabaseChanges,
    ) -> PyResult<PyTursoAsyncOperation> {
        let Some(changes) = changes.changes.take() else {
            return Err(Misuse::new_err(
                "changes were already applied before".to_string(),
            ));
        };
        Ok(PyTursoAsyncOperation {
            operation: self.database.apply_changes(changes),
        })
    }
    /// Run extra database callbacks after IO execution
    pub fn step_io_callbacks(&self) {
        self.database.step_io_callbacks();
    }
    /// Try to take IO request from the sync engine IO queue
    pub fn take_io_item(&self) -> Option<PyTursoSyncIoItem> {
        self.database
            .take_io_item()
            .map(|t| PyTursoSyncIoItem { item: t })
    }
}
