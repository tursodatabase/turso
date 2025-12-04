use pyo3::{
    prelude::*,
    types::{PyBytes, PyTuple},
};
use std::sync::Arc;
use turso_sdk_kit::rsapi::{self, TursoError, TursoStatusCode, Value, ValueRef};

use pyo3::create_exception;
use pyo3::exceptions::PyException;

// support equality for status codes
#[pyclass(eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)] // Add necessary traits for your use case
pub enum PyTursoStatusCode {
    Ok = 0,
    Done = 1,
    Row = 2,
    Io = 3,
}
create_exception!(turso, Busy, PyException, "database is busy");
create_exception!(turso, Interrupt, PyException, "interrupted");
create_exception!(turso, Error, PyException, "generic error");
create_exception!(turso, Misuse, PyException, "API misuse");
create_exception!(turso, Constraint, PyException, "constraint error");
create_exception!(turso, Readonly, PyException, "database is readonly");
create_exception!(turso, DatabaseFull, PyException, "database is full");
create_exception!(turso, NotAdb, PyException, "not a database`");
create_exception!(turso, Corrupt, PyException, "database corrupted");

pub(crate) fn turso_error_to_py_err(err: TursoError) -> PyErr {
    match err.code {
        rsapi::TursoStatusCode::Busy => Busy::new_err(err.message),
        rsapi::TursoStatusCode::Interrupt => Interrupt::new_err(err.message),
        rsapi::TursoStatusCode::Error => Error::new_err(err.message),
        rsapi::TursoStatusCode::Misuse => Misuse::new_err(err.message),
        rsapi::TursoStatusCode::Constraint => Constraint::new_err(err.message),
        rsapi::TursoStatusCode::Readonly => Readonly::new_err(err.message),
        rsapi::TursoStatusCode::DatabaseFull => DatabaseFull::new_err(err.message),
        rsapi::TursoStatusCode::NotAdb => NotAdb::new_err(err.message),
        rsapi::TursoStatusCode::Corrupt => Corrupt::new_err(err.message),
        _ => Error::new_err("unexpected status from the sdk-kit".to_string()),
    }
}

fn turso_status_to_py(status: TursoStatusCode) -> PyTursoStatusCode {
    match status {
        TursoStatusCode::Ok => PyTursoStatusCode::Ok,
        TursoStatusCode::Done => PyTursoStatusCode::Done,
        TursoStatusCode::Row => PyTursoStatusCode::Row,
        TursoStatusCode::Io => PyTursoStatusCode::Io,
        _ => panic!("unexpected status code: {status:?}"),
    }
}

#[pyclass]
pub struct PyTursoExecutionResult {
    #[pyo3(get)]
    pub status: PyTursoStatusCode,
    #[pyo3(get)]
    pub rows_changed: u64,
}

#[pyclass]
pub struct PyTursoLog {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub target: String,
    #[pyo3(get)]
    pub file: String,
    #[pyo3(get)]
    pub timestamp: u64,
    #[pyo3(get)]
    pub line: usize,
    #[pyo3(get)]
    pub level: String,
}

#[pyclass]
pub struct PyTursoSetupConfig {
    pub logger: Option<Py<PyAny>>,
    pub log_level: Option<String>,
}

#[pymethods]
impl PyTursoSetupConfig {
    #[new]
    #[pyo3(signature = (logger, log_level))]
    fn new(logger: Option<Py<PyAny>>, log_level: Option<String>) -> Self {
        Self { logger, log_level }
    }
}

#[pyclass]
pub struct PyTursoDatabaseConfig {
    pub path: String,

    /// comma-separated list of experimental features to enable
    /// this field is intentionally just a string in order to make enablement of experimental features as flexible as possible
    pub experimental_features: Option<String>,

    /// if true, library methods will return Io status code and delegate Io loop to the caller
    /// if false, library will spin IO itself in case of Io status code and never return it to the caller
    pub async_io: bool,
}

#[pymethods]
impl PyTursoDatabaseConfig {
    #[new]
    #[pyo3(signature = (path, experimental_features=None, async_io=false))]
    fn new(path: String, experimental_features: Option<String>, async_io: bool) -> Self {
        Self {
            path,
            experimental_features,
            async_io,
        }
    }
}

#[pyclass]
pub struct PyTursoDatabase {
    database: Arc<rsapi::TursoDatabase>,
}

/// Setup logging for the turso globally
/// Only first invocation has effect - all subsequent updates will be ignored
#[pyfunction]
pub fn py_turso_setup(py: Python, config: &PyTursoSetupConfig) -> PyResult<()> {
    rsapi::turso_setup(rsapi::TursoSetupConfig {
        logger: if let Some(logger) = &config.logger {
            let logger = logger.clone_ref(py);
            Some(Box::new(move |log| {
                Python::attach(|py| {
                    let py_log = PyTursoLog {
                        message: log.message.to_string(),
                        target: log.target.to_string(),
                        file: log.file.to_string(),
                        timestamp: log.timestamp,
                        line: log.line,
                        level: log.level.to_string(),
                    };
                    logger.call1(py, (py_log,)).unwrap();
                })
            }))
        } else {
            None
        },
        log_level: config.log_level.clone(),
    })
    .map_err(turso_error_to_py_err)?;
    Ok(())
}

/// Open the database
#[pyfunction]
pub fn py_turso_database_open(config: &PyTursoDatabaseConfig) -> PyResult<PyTursoDatabase> {
    let database = rsapi::TursoDatabase::new(rsapi::TursoDatabaseConfig {
        path: config.path.clone(),
        experimental_features: config.experimental_features.clone(),
        async_io: config.async_io,
        io: None,
        db_file: None,
    });
    database.open().map_err(turso_error_to_py_err)?;
    Ok(PyTursoDatabase { database })
}

#[pymethods]
impl PyTursoDatabase {
    pub fn connect(&self) -> PyResult<PyTursoConnection> {
        Ok(PyTursoConnection {
            connection: self.database.connect().map_err(turso_error_to_py_err)?,
        })
    }
}

#[pyclass]
pub struct PyTursoConnection {
    pub(crate) connection: Arc<rsapi::TursoConnection>,
}

#[pymethods]
impl PyTursoConnection {
    /// prepare single statement from the string
    pub fn prepare_single(&self, sql: &str) -> PyResult<PyTursoStatement> {
        Ok(PyTursoStatement {
            statement: self
                .connection
                .prepare_single(sql)
                .map_err(turso_error_to_py_err)?,
        })
    }
    /// prepare first statement from the string which can have multiple statements separated by semicolon
    /// returns None if string has no statements
    /// returns Some with prepared statement and position in the string right after the prepared statement end
    pub fn prepare_first(&self, sql: &str) -> PyResult<Option<(PyTursoStatement, usize)>> {
        match self
            .connection
            .prepare_first(sql)
            .map_err(turso_error_to_py_err)?
        {
            Some((statement, tail_idx)) => Ok(Some((PyTursoStatement { statement }, tail_idx))),
            None => Ok(None),
        }
    }
    /// Get the auto_commmit mode for the connection
    pub fn get_auto_commit(&self) -> PyResult<bool> {
        Ok(self.connection.get_auto_commit())
    }
    /// Close the connection
    /// (caller must ensure that no operations over connection or derived statements will happen after the call)
    pub fn close(&self) -> PyResult<()> {
        self.connection.close().map_err(turso_error_to_py_err)
    }
}

#[pyclass]
pub struct PyTursoStatement {
    statement: Box<rsapi::TursoStatement>,
}

#[pymethods]
impl PyTursoStatement {
    /// binds positional parameters to the statement
    pub fn bind(&mut self, parameters: Bound<PyTuple>) -> PyResult<()> {
        let len = parameters.len();
        for i in 0..len {
            let parameter = parameters.get_item(i)?;
            self.statement
                .bind_positional(i + 1, py_to_db_value(parameter)?)
                .map_err(turso_error_to_py_err)?;
        }
        Ok(())
    }

    /// step one iteration of the statement execution
    /// Returns [PyTursoStatusCode::Done] when execution is finished
    /// Returns [PyTursoStatusCode::Row] when execution generated a row which can be consumed with [Self::row] method
    /// Returns [PyTursoStatusCode::Io] when async_io is set and execution needs IO in order to make progress
    ///
    /// The caller must always either use [Self::step] or [Self::execute] methods for single statement - but never mix them together
    pub fn step(&mut self) -> PyResult<PyTursoStatusCode> {
        Ok(turso_status_to_py(
            self.statement.step().map_err(turso_error_to_py_err)?,
        ))
    }

    /// execute statement and ignore all rows generated by it
    /// Returns [PyTursoStatusCode::Done] when execution is finished
    /// Returns [PyTursoStatusCode::Io] when async_io is set and execution needs IO in order to make progress
    ///
    /// Note, that execute never returns Row status code
    ///
    /// The caller must always either use [Self::step] or [Self::execute] methods for single statement - but never mix them together
    pub fn execute(&mut self) -> PyResult<PyTursoExecutionResult> {
        let result = self.statement.execute().map_err(turso_error_to_py_err)?;
        Ok(PyTursoExecutionResult {
            status: turso_status_to_py(result.status),
            rows_changed: result.rows_changed,
        })
    }
    /// Run one iteration of IO backend
    pub fn run_io(&self) -> PyResult<()> {
        self.statement.run_io().map_err(turso_error_to_py_err)?;
        Ok(())
    }
    /// Get column names of the statement
    pub fn columns(&self, py: Python) -> PyResult<Py<PyTuple>> {
        let columns_count = self.statement.column_count();
        let mut columns = Vec::with_capacity(columns_count);
        for i in 0..columns_count {
            columns.push(
                self.statement
                    .column_name(i)
                    .map_err(turso_error_to_py_err)?
                    .to_string(),
            );
        }
        Ok(PyTuple::new(py, columns.into_iter())?.unbind())
    }
    /// Get tuple with current row values
    /// This method is only valid to call after [Self::step] returned [PyTursoStatusCode::Row] status code
    pub fn row(&self, py: Python) -> PyResult<Py<PyTuple>> {
        let columns_count = self.statement.column_count();
        let mut py_values = Vec::with_capacity(columns_count);
        for i in 0..columns_count {
            py_values.push(db_value_to_py(
                py,
                self.statement.row_value(i).map_err(turso_error_to_py_err)?,
            )?);
        }
        Ok(PyTuple::new(py, &py_values)?.into_pyobject(py)?.into())
    }
    /// Finalize statement execution
    /// This method must be called when statement is no longer need
    /// It will perform necessary cleanup and run any unfinished statement operations to completion
    /// (for example, in `INSERT INTO ... RETURNING ...` query, finalize is essential as it will make sure that all inserts will be completed, even if only few first rows were consumed by the caller)
    ///
    /// Note, that if statement wasn't started (no step / execute methods was called) - finalize will not execute the statement
    pub fn finalize(&mut self) -> PyResult<PyTursoStatusCode> {
        Ok(turso_status_to_py(
            self.statement.finalize().map_err(turso_error_to_py_err)?,
        ))
    }
    /// Reset the statement by clearing bindings and reclaiming memory of the program from previous run
    /// This will also abort last operation if any was unfinished (but if transaction was opened before this statement - its state will be untouched, reset will only affect operation within current statement)
    pub fn reset(&mut self) -> PyResult<()> {
        self.statement.reset().map_err(turso_error_to_py_err)?;
        Ok(())
    }
}

fn db_value_to_py(py: Python, value: rsapi::ValueRef) -> PyResult<Py<PyAny>> {
    match value {
        ValueRef::Null => Ok(py.None()),
        ValueRef::Integer(i) => Ok(i.into_pyobject(py)?.into()),
        ValueRef::Float(f) => Ok(f.into_pyobject(py)?.into()),
        ValueRef::Text(s) => Ok(s.as_str().into_pyobject(py)?.into()),
        ValueRef::Blob(b) => Ok(PyBytes::new(py, b).into()),
    }
}

/// Converts a Python object to a Turso Value
fn py_to_db_value(obj: Bound<PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        Ok(Value::Null)
    } else if let Ok(integer) = obj.extract::<i64>() {
        Ok(Value::Integer(integer))
    } else if let Ok(float) = obj.extract::<f64>() {
        Ok(Value::Float(float))
    } else if let Ok(string) = obj.extract::<String>() {
        Ok(Value::Text(string.into()))
    } else if let Ok(bytes) = obj.cast::<PyBytes>() {
        Ok(Value::Blob(bytes.as_bytes().to_vec()))
    } else {
        Err(Error::new_err(
            "unexpected parameter value, only None, numbers, strings and bytes are supported"
                .to_string(),
        ))
    }
}
