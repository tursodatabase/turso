use anyhow::Result;
use errors::*;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::sync::Arc;
use turso_sdk_kit::rsapi::{
    self, TursoConnection, TursoDatabase, TursoDatabaseConfig, TursoStatement, Value, ValueRef,
};

mod errors;

#[pyclass]
#[derive(Clone, Debug)]
struct Description {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    type_code: String,
    #[pyo3(get)]
    display_size: Option<String>,
    #[pyo3(get)]
    internal_size: Option<String>,
    #[pyo3(get)]
    precision: Option<String>,
    #[pyo3(get)]
    scale: Option<String>,
    #[pyo3(get)]
    null_ok: Option<String>,
}

// Helper function to convert TursoError to PyErr
fn turso_error_to_pyerr(err: rsapi::TursoError) -> PyErr {
    let message = err.message.unwrap_or_else(|| "Unknown error".to_string());
    match err.code {
        rsapi::TursoStatusCode::Misuse => ProgrammingError::new_err(message),
        rsapi::TursoStatusCode::Constraint => IntegrityError::new_err(message),
        rsapi::TursoStatusCode::Corrupt => DatabaseError::new_err(message),
        rsapi::TursoStatusCode::NotAdb => DatabaseError::new_err(message),
        rsapi::TursoStatusCode::DatabaseFull => OperationalError::new_err(message),
        rsapi::TursoStatusCode::Readonly => OperationalError::new_err(message),
        rsapi::TursoStatusCode::Busy => OperationalError::new_err(message),
        rsapi::TursoStatusCode::Interrupt => OperationalError::new_err(message),
        rsapi::TursoStatusCode::Io => OperationalError::new_err(message),
        _ => DatabaseError::new_err(message),
    }
}

#[pyclass(unsendable)]
pub struct Cursor {
    /// This read/write attribute specifies the number of rows to fetch at a time with `.fetchmany()`.
    /// It defaults to `1`, meaning it fetches a single row at a time.
    #[pyo3(get)]
    arraysize: i64,

    conn: Arc<rsapi::TursoConnection>,

    /// The `.description` attribute is a read-only sequence of 7-item, each describing a column in the result set:
    ///
    /// - `name`: The column's name (always present).
    /// - `type_code`: The data type code (always present).
    /// - `display_size`: Column's display size (optional).
    /// - `internal_size`: Column's internal size (optional).
    /// - `precision`: Numeric precision (optional).
    /// - `scale`: Numeric scale (optional).
    /// - `null_ok`: Indicates if null values are allowed (optional).
    ///
    /// The `name` and `type_code` fields are mandatory; others default to `None` if not applicable.
    ///
    /// This attribute is `None` for operations that do not return rows or if no `.execute*()` method has been invoked.
    #[pyo3(get)]
    description: Option<Description>,

    /// Read-only attribute that provides the number of modified rows for `INSERT`, `UPDATE`, `DELETE`,
    /// and `REPLACE` statements; it is `-1` for other statements, including CTE queries.
    /// It is only updated by the `execute()` and `executemany()` methods after the statement has run to completion.
    /// This means any resulting rows must be fetched for `rowcount` to be updated.
    #[pyo3(get)]
    rowcount: i64,

    stmt: Option<Box<rsapi::TursoStatement>>,
}

fn exec_stmt(conn: &TursoConnection, sql: impl AsRef<str>) -> Result<()> {
    let mut begin_stmt = conn.prepare_single(sql).map_err(turso_error_to_pyerr)?;
    begin_stmt.execute().map_err(turso_error_to_pyerr)?;
    Ok(())
}

#[allow(unused_variables, clippy::arc_with_non_send_sync)]
#[pymethods]
impl Cursor {
    /// SQLite3 DB-API2 implementation for execute(...) methods returns same reference to the Cursor
    /// (see https://github.com/python/cpython/blob/bc9e63dd9d2931771415cca1b0ed774471d523c0/Modules/_sqlite/cursor.c#L820)
    ///
    /// so, we accept PyRefMut and return the same reference in the end
    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        sql: &str,
        parameters: Option<Bound<'py, PyTuple>>,
    ) -> Result<PyRefMut<'py, Self>> {
        let stmt_is_dml = stmt_is_dml(sql);
        let stmt_is_ddl = stmt_is_ddl(sql);
        let stmt_is_tx = stmt_is_tx(sql);
        let conn = slf.conn.clone();

        let mut stmt = conn.prepare_single(sql).map_err(turso_error_to_pyerr)?;

        if let Some(params) = parameters {
            for (i, elem) in params.iter().enumerate() {
                let value = py_to_db_value(&elem)?;
                stmt.bind_positional(i + 1, value)
                    .map_err(turso_error_to_pyerr)?;
            }
        }

        if stmt_is_dml && conn.get_auto_commit() {
            exec_stmt(&conn, "BEGIN")?;
        }

        // For DDL and DML statements,
        // we need to execute the statement immediately
        if stmt_is_ddl || stmt_is_dml || stmt_is_tx {
            stmt.execute().map_err(turso_error_to_pyerr)?;
        }

        slf.stmt = Some(stmt);
        Ok(slf)
    }

    pub fn fetchone(&mut self, py: Python) -> Result<Option<Py<PyAny>>> {
        let Some(stmt) = &mut self.stmt else {
            return Err(
                PyErr::new::<ProgrammingError, _>("No statement prepared for execution").into(),
            );
        };
        let mut py_row = None;
        loop {
            match stmt.step().map_err(turso_error_to_pyerr)? {
                rsapi::TursoStatusCode::Done => break,
                rsapi::TursoStatusCode::Row => {
                    py_row = Some(row_to_py(py, stmt)?);
                    break;
                }
                _ => {
                    return Err(PyErr::new::<OperationalError, _>(
                        "unexpected step status".to_string(),
                    )
                    .into())
                }
            }
        }
        stmt.finalize().map_err(turso_error_to_pyerr)?;
        Ok(py_row)
    }

    pub fn fetchall(&mut self, py: Python) -> Result<Vec<Py<PyAny>>> {
        let mut results = Vec::new();
        let Some(stmt) = &mut self.stmt else {
            return Err(
                PyErr::new::<ProgrammingError, _>("No statement prepared for execution").into(),
            );
        };
        loop {
            match stmt.step().map_err(turso_error_to_pyerr)? {
                rsapi::TursoStatusCode::Done => return Ok(results),
                rsapi::TursoStatusCode::Row => {
                    let py_row = row_to_py(py, stmt)?;
                    results.push(py_row);
                }
                _ => {
                    return Err(PyErr::new::<OperationalError, _>(
                        "unexpected step status".to_string(),
                    )
                    .into())
                }
            }
        }
    }

    pub fn close(&self) -> PyResult<()> {
        // self.conn.close()?;

        Ok(())
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn executemany(&self, sql: &str, parameters: Option<Py<PyList>>) -> PyResult<()> {
        Err(PyErr::new::<NotSupportedError, _>(
            "executemany() is not supported in this version",
        ))
    }

    #[pyo3(signature = (size=None))]
    pub fn fetchmany(&self, size: Option<i64>) -> PyResult<Option<Vec<Py<PyAny>>>> {
        Err(PyErr::new::<NotSupportedError, _>(
            "fetchmany() is not supported in this version",
        ))
    }
}

fn stmt_is_dml(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE")
}

fn stmt_is_ddl(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("CREATE") || sql.starts_with("ALTER") || sql.starts_with("DROP")
}

fn stmt_is_tx(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("BEGIN") || sql.starts_with("COMMIT") || sql.starts_with("ROLLBACK")
}
#[pyclass(unsendable)]
#[derive(Clone)]
pub struct Connection {
    conn: Arc<rsapi::TursoConnection>,
}

#[pymethods]
impl Connection {
    pub fn cursor(&self) -> Result<Cursor> {
        Ok(Cursor {
            arraysize: 1,
            conn: self.conn.clone(),
            description: None,
            rowcount: -1,
            stmt: None,
        })
    }

    pub fn close(&self) -> PyResult<()> {
        // self.conn.close().map_err(turso_error_to_pyerr)?;
        Ok(())
    }

    pub fn commit(&self) -> PyResult<()> {
        if !self.conn.get_auto_commit() {
            exec_stmt(&self.conn, "COMMIT")?;
            exec_stmt(&self.conn, "BEGIN")?;
        }
        Ok(())
    }

    pub fn rollback(&self) -> PyResult<()> {
        if !self.conn.get_auto_commit() {
            exec_stmt(&self.conn, "ROLLBACK")?;
            exec_stmt(&self.conn, "BEGIN")?;
        }
        Ok(())
    }

    fn __enter__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.close()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if Arc::strong_count(&self.conn) == 1 {
            // self.conn
            //     .close()
            //     .expect("Failed to drop (close) connection");
        }
    }
}

#[allow(clippy::arc_with_non_send_sync)]
#[pyfunction(signature = (path))]
pub fn connect(path: &str) -> Result<Connection> {
    let db = TursoDatabase::create(TursoDatabaseConfig {
        path: path.to_string(),
        experimental_features: None,
        io: None,
        async_io: false,
    });
    db.open().map_err(turso_error_to_pyerr)?;
    let conn = db.connect().map_err(turso_error_to_pyerr)?;
    Ok(Connection { conn })
}

fn row_to_py(py: Python, stmt: &TursoStatement) -> Result<Py<PyAny>> {
    let mut py_values = Vec::new();
    for i in 0..stmt.column_count() {
        let value = stmt.row_value(i).map_err(turso_error_to_pyerr)?;
        match value {
            ValueRef::Null => py_values.push(py.None()),
            ValueRef::Integer(i) => py_values.push(i.into_pyobject(py)?.into()),
            ValueRef::Float(f) => py_values.push(f.into_pyobject(py)?.into()),
            ValueRef::Text(s) => py_values.push(s.as_str().into_pyobject(py)?.into()),
            ValueRef::Blob(b) => py_values.push(PyBytes::new(py, b).into()),
        }
    }
    Ok(PyTuple::new(py, &py_values)
        .unwrap()
        .into_pyobject(py)?
        .into())
}

/// Converts a Python object to a Turso Value
fn py_to_db_value(obj: &Bound<PyAny>) -> Result<Value> {
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
        return Err(PyErr::new::<ProgrammingError, _>(format!(
            "Unsupported Python type: {}",
            obj.get_type().name()?
        ))
        .into());
    }
}

#[pymodule]
fn _turso(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("Warning", m.py().get_type::<Warning>())?;
    m.add("Error", m.py().get_type::<Error>())?;
    m.add("InterfaceError", m.py().get_type::<InterfaceError>())?;
    m.add("DatabaseError", m.py().get_type::<DatabaseError>())?;
    m.add("DataError", m.py().get_type::<DataError>())?;
    m.add("OperationalError", m.py().get_type::<OperationalError>())?;
    m.add("IntegrityError", m.py().get_type::<IntegrityError>())?;
    m.add("InternalError", m.py().get_type::<InternalError>())?;
    m.add("ProgrammingError", m.py().get_type::<ProgrammingError>())?;
    m.add("NotSupportedError", m.py().get_type::<NotSupportedError>())?;
    Ok(())
}
