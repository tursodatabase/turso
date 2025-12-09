use pyo3::{
    pymodule,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult,
};
use turso_sdk_kit::rsapi::TursoDatabase;

use crate::{
    turso::{
        py_turso_database_open, py_turso_setup, Busy, Constraint, Corrupt, DatabaseFull, Interrupt,
        Misuse, NotAdb, PyTursoConnection, PyTursoDatabase, PyTursoDatabaseConfig,
        PyTursoExecutionResult, PyTursoLog, PyTursoSetupConfig, PyTursoStatement,
        PyTursoStatusCode, Readonly,
    },
    turso_sync::{
        py_turso_sync_new, PyTursoAsyncOperation, PyTursoAsyncOperationResultKind,
        PyTursoPartialSyncOpts, PyTursoSyncDatabase, PyTursoSyncDatabaseChanges,
        PyTursoSyncDatabaseConfig, PyTursoSyncDatabaseStats, PyTursoSyncIoItem,
        PyTursoSyncIoItemRequestKind,
    },
};

pub mod turso;
pub mod turso_sync;

#[pymodule]
fn _turso(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("__version__", TursoDatabase::version())?;
    // database exports
    m.add_function(wrap_pyfunction!(py_turso_setup, m)?)?;
    m.add_function(wrap_pyfunction!(py_turso_database_open, m)?)?;
    m.add_class::<PyTursoStatusCode>()?;
    m.add_class::<PyTursoExecutionResult>()?;
    m.add_class::<PyTursoLog>()?;
    m.add_class::<PyTursoSetupConfig>()?;
    m.add_class::<PyTursoDatabaseConfig>()?;
    m.add_class::<PyTursoDatabase>()?;
    m.add_class::<PyTursoConnection>()?;
    m.add_class::<PyTursoStatement>()?;

    m.add("Busy", m.py().get_type::<Busy>())?;
    m.add("Interrupt", m.py().get_type::<Interrupt>())?;
    m.add("Error", m.py().get_type::<crate::turso::Error>())?;
    m.add("Misuse", m.py().get_type::<Misuse>())?;
    m.add("Constraint", m.py().get_type::<Constraint>())?;
    m.add("Readonly", m.py().get_type::<Readonly>())?;
    m.add("DatabaseFull", m.py().get_type::<DatabaseFull>())?;
    m.add("NotAdb", m.py().get_type::<NotAdb>())?;
    m.add("Corrupt", m.py().get_type::<Corrupt>())?;

    // sync exports
    m.add_function(wrap_pyfunction!(py_turso_sync_new, m)?)?;
    m.add_class::<PyTursoSyncDatabase>()?;
    m.add_class::<PyTursoSyncDatabaseConfig>()?;
    m.add_class::<PyTursoSyncDatabaseChanges>()?;
    m.add_class::<PyTursoSyncIoItem>()?;
    m.add_class::<PyTursoSyncDatabaseStats>()?;
    m.add_class::<PyTursoSyncIoItemRequestKind>()?;
    m.add_class::<PyTursoAsyncOperation>()?;
    m.add_class::<PyTursoAsyncOperationResultKind>()?;
    m.add_class::<PyTursoPartialSyncOpts>()?;
    Ok(())
}
