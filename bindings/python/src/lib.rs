pub mod turso;
pub mod turso_sync;

// TODO: audit thread-safety of wrapped types and Python::attach() usage before removing gil_used
#[pyo3::pymodule(gil_used = true)]
mod _turso {
    #[allow(non_upper_case_globals)]
    #[pymodule_export]
    const __version__: &str = turso_sdk_kit::rsapi::TursoDatabase::version();

    // database exports
    #[pymodule_export]
    use crate::turso::{
        py_turso_database_open, py_turso_setup, PyTursoConnection, PyTursoDatabase,
        PyTursoDatabaseConfig, PyTursoEncryptionConfig, PyTursoExecutionResult, PyTursoLog,
        PyTursoSetupConfig, PyTursoStatement, PyTursoStatusCode,
    };

    // exception exports
    #[pymodule_export]
    use crate::turso::{
        Busy, Constraint, Corrupt, DatabaseFull, Error, Interrupt, Misuse, NotAdb, Readonly,
    };

    // sync exports
    #[pymodule_export]
    use crate::turso_sync::{
        py_turso_sync_new, PyRemoteEncryptionCipher, PyTursoAsyncOperation,
        PyTursoAsyncOperationResultKind, PyTursoPartialSyncOpts, PyTursoSyncDatabase,
        PyTursoSyncDatabaseChanges, PyTursoSyncDatabaseConfig, PyTursoSyncDatabaseStats,
        PyTursoSyncIoItem, PyTursoSyncIoItemRequestKind,
    };
}
