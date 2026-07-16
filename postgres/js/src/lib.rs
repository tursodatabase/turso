//! PGlite-compatible Node bindings for the Turso Postgres frontend.
//!
//! This is a thin native layer over `turso_pg`: it opens databases with the
//! PostgreSQL schema dialect, prepares and steps statements, and reports
//! result-column metadata as PostgreSQL type OIDs. Everything user-facing
//! (the PGlite class, OID-driven value conversion, SQLSTATE error mapping)
//! lives in the JavaScript layer on top; see `postgres/js/pglite.js` and
//! `postgres/PGLITE.md`.

use napi::bindgen_prelude::*;
use napi::Env;
use napi_derive::napi;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Weak};

/// Step result constants shared with the JS layer.
const STEP_ROW: u32 = 1;
const STEP_DONE: u32 = 2;
const STEP_IO: u32 = 3;

type StatementHandle = Arc<RefCell<Option<turso_core::Statement>>>;

fn generic_error(message: impl std::fmt::Display) -> napi::Error {
    Error::new(Status::GenericFailure, message.to_string())
}

/// A database opened with the PostgreSQL schema dialect.
#[napi]
pub struct PgDatabase {
    path: String,
    io: Arc<dyn turso_core::IO>,
    conn: Option<turso_pg::PgConnection>,
    /// Weak refs to every statement handle created by this database, so
    /// `close()` can finalize them and break the statement -> connection ->
    /// database reference chain (same pattern as `bindings/javascript`).
    stmts: Mutex<Vec<Weak<RefCell<Option<turso_core::Statement>>>>>,
}

#[napi(object)]
#[derive(Clone, Default)]
pub struct PgDatabaseOpts {
    pub readonly: Option<bool>,
    pub file_must_exist: Option<bool>,
    pub timeout: Option<u32>,
    pub default_query_timeout: Option<u32>,
}

#[napi(object)]
pub struct PgColumn {
    pub name: String,
    pub data_type_id: u32,
}

#[napi]
impl PgDatabase {
    /// Opens the database synchronously. The JS layer calls this off the
    /// constructor's synchronous path so open errors reject `waitReady`.
    #[napi(constructor)]
    pub fn new(path: String, opts: Option<PgDatabaseOpts>) -> napi::Result<Self> {
        let opts = opts.unwrap_or_default();
        // Same engine feature set that pgmicro enables for the PG dialect.
        let db_opts = turso_core::DatabaseOpts::new()
            .with_views(true)
            .with_custom_types(true)
            .with_encryption(true)
            .with_index_method(true)
            .with_autovacuum(true)
            .with_attach(true)
            .with_generated_columns(true);

        let mut flags = turso_core::OpenFlags::default();
        if opts.readonly == Some(true) {
            flags.set(turso_core::OpenFlags::ReadOnly, true);
            flags.set(turso_core::OpenFlags::Create, false);
        }
        if opts.file_must_exist == Some(true) {
            flags.set(turso_core::OpenFlags::Create, false);
        }

        let (io, db) =
            turso_pg::open_database(&path, None, flags, db_opts).map_err(generic_error)?;
        let conn = db.connect().map_err(generic_error)?;
        if let Some(timeout) = opts.timeout {
            conn.set_busy_timeout(std::time::Duration::from_millis(timeout as u64));
        }
        if let Some(timeout) = opts.default_query_timeout {
            if timeout > 0 {
                conn.set_query_timeout(std::time::Duration::from_millis(timeout as u64));
            }
        }
        let conn = turso_pg::PgConnection::new(conn);
        Ok(Self {
            path,
            io,
            conn: Some(conn),
            stmts: Mutex::new(Vec::new()),
        })
    }

    fn conn(&self) -> napi::Result<&turso_pg::PgConnection> {
        self.conn
            .as_ref()
            .ok_or_else(|| generic_error("database is closed"))
    }

    #[napi(getter)]
    pub fn path(&self) -> String {
        self.path.clone()
    }

    #[napi(getter)]
    pub fn open(&self) -> bool {
        self.conn.is_some()
    }

    /// Split a SQL string into individual statements (PG-aware: respects
    /// strings, comments, and dollar-quoting).
    #[napi]
    pub fn split_statements(&self, sql: String) -> napi::Result<Vec<String>> {
        turso_pg::split_statements(&sql).map_err(generic_error)
    }

    #[napi]
    pub fn prepare(&self, sql: String) -> napi::Result<PgStatement> {
        let stmt = self.conn()?.prepare(&sql).map_err(generic_error)?;
        #[allow(clippy::arc_with_non_send_sync)]
        let stmt: StatementHandle = Arc::new(RefCell::new(Some(stmt)));
        self.stmts.lock().unwrap().push(Arc::downgrade(&stmt));
        Ok(PgStatement { stmt })
    }

    /// Run one iteration of the I/O loop; the JS layer calls this while a
    /// statement step reports that I/O is pending.
    #[napi]
    pub fn io_step(&self) -> napi::Result<()> {
        self.io.step().map_err(generic_error)
    }

    /// Whether the connection has an open transaction (inverse of autocommit).
    #[napi]
    pub fn in_transaction(&self) -> napi::Result<bool> {
        Ok(!self.conn()?.inner().get_auto_commit())
    }

    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        let mut stmts = self.stmts.lock().unwrap();
        for weak in stmts.drain(..) {
            if let Some(stmt) = weak.upgrade() {
                *stmt.borrow_mut() = None;
            }
        }
        drop(stmts);
        if let Some(conn) = self.conn.take() {
            conn.close().map_err(generic_error)?;
        }
        Ok(())
    }
}

/// A prepared statement.
#[napi]
pub struct PgStatement {
    stmt: StatementHandle,
}

#[napi]
impl PgStatement {
    fn with_stmt<T>(
        &self,
        f: impl FnOnce(&mut turso_core::Statement) -> napi::Result<T>,
    ) -> napi::Result<T> {
        let mut guard = self.stmt.borrow_mut();
        let stmt = guard
            .as_mut()
            .ok_or_else(|| generic_error("statement has been finalized"))?;
        f(stmt)
    }

    #[napi(getter)]
    pub fn num_columns(&self) -> napi::Result<u32> {
        self.with_stmt(|stmt| Ok(stmt.num_columns() as u32))
    }

    /// Result-column metadata with PostgreSQL type OIDs.
    #[napi]
    pub fn columns(&self) -> napi::Result<Vec<PgColumn>> {
        self.with_stmt(|stmt| {
            Ok((0..stmt.num_columns())
                .map(|i| PgColumn {
                    name: stmt.get_column_name(i).into_owned(),
                    data_type_id: pg_oid_for_column(stmt, i),
                })
                .collect())
        })
    }

    /// Bind a parameter by PostgreSQL `$N` name (e.g. `"$1"`). The bytecode
    /// compiler may assign internal indices in a different order than the $N
    /// numbering, so the lookup goes through `parameter_index`; `fallback` is
    /// the 1-based position used when the statement has anonymous parameters.
    #[napi]
    pub fn bind_named(&self, name: String, fallback: u32, value: Unknown) -> napi::Result<()> {
        let turso_value = js_to_value(value)?;
        self.with_stmt(|stmt| {
            let idx = stmt.parameter_index(&name).unwrap_or_else(|| {
                NonZeroUsize::new(fallback as usize).expect("fallback index must be non-zero")
            });
            stmt.bind_at(idx, turso_value)
                .map_err(|e| generic_error(e.to_string()))
        })
    }

    /// Step the statement: 1 = row available, 2 = done, 3 = I/O needed.
    #[napi]
    pub fn step_sync(&self) -> napi::Result<u32> {
        self.with_stmt(|stmt| match stmt.step() {
            Ok(turso_core::StepResult::Row) => Ok(STEP_ROW),
            Ok(turso_core::StepResult::IO | turso_core::StepResult::Yield) => Ok(STEP_IO),
            Ok(turso_core::StepResult::Done) => Ok(STEP_DONE),
            Ok(turso_core::StepResult::Interrupt) => Err(generic_error("statement interrupted")),
            Ok(turso_core::StepResult::Busy) => Err(generic_error("database is locked")),
            Err(e) => Err(generic_error(e)),
        })
    }

    /// The current row as an array of raw values. Integers always come back
    /// as BigInt so 64-bit values never lose precision; the JS layer decides
    /// number vs bigint per PGlite's int8 rules.
    #[napi]
    pub fn row<'env>(&self, env: &'env Env) -> napi::Result<Unknown<'env>> {
        let guard = self.stmt.borrow();
        let stmt = guard
            .as_ref()
            .ok_or_else(|| generic_error("statement has been finalized"))?;
        let row = stmt
            .row()
            .ok_or_else(|| generic_error("no row data available"))?;
        let mut array = env.create_array(row.len() as u32)?;
        for (idx, value) in row.get_values().enumerate() {
            let js_value = match value {
                turso_core::Value::Null => ToNapiValue::into_unknown(Null, env)?,
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => {
                    ToNapiValue::into_unknown(BigInt::from(*i), env)?
                }
                turso_core::Value::Numeric(turso_core::Numeric::Float(f)) => {
                    ToNapiValue::into_unknown(f64::from(*f), env)?
                }
                turso_core::Value::Text(s) => ToNapiValue::into_unknown(s.as_str(), env)?,
                turso_core::Value::Blob(b) => {
                    ToNapiValue::into_unknown(Buffer::from(b.as_slice()), env)?
                }
            };
            array.set(idx as u32, js_value)?;
        }
        array.coerce_to_object().map(|o| o.to_unknown())
    }

    /// Number of rows changed by this statement.
    #[napi]
    pub fn n_change(&self) -> napi::Result<i64> {
        self.with_stmt(|stmt| Ok(stmt.n_change()))
    }

    #[napi]
    pub fn reset(&self) -> napi::Result<()> {
        self.with_stmt(|stmt| stmt.reset().map_err(generic_error))
    }

    #[napi]
    pub fn finalize(&mut self) -> napi::Result<()> {
        *self.stmt.borrow_mut() = None;
        Ok(())
    }
}

/// Convert a JS value to a turso value for binding (same conversion as
/// `bindings/javascript`).
fn js_to_value(value: Unknown) -> napi::Result<turso_core::Value> {
    let value_type = value.get_type()?;
    Ok(match value_type {
        ValueType::Null | ValueType::Undefined => turso_core::Value::Null,
        ValueType::Number => {
            let n: f64 = unsafe { value.cast()? };
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                turso_core::Value::from_i64(n as i64)
            } else {
                turso_core::Value::from_f64(n)
            }
        }
        ValueType::BigInt => {
            let bigint_str = value.coerce_to_string()?.into_utf8()?.as_str()?.to_owned();
            let bigint_value = bigint_str.parse::<i64>().map_err(|e| {
                Error::new(
                    Status::NumberExpected,
                    format!("failed to parse BigInt: {e}"),
                )
            })?;
            turso_core::Value::from_i64(bigint_value)
        }
        ValueType::String => {
            let s = value.coerce_to_string()?.into_utf8()?;
            turso_core::Value::Text(s.as_str()?.to_owned().into())
        }
        ValueType::Boolean => {
            let b: bool = unsafe { value.cast()? };
            turso_core::Value::from_i64(if b { 1 } else { 0 })
        }
        ValueType::Object => {
            let obj = value.coerce_to_object()?;
            if obj.is_buffer()? || obj.is_typedarray()? {
                let length = obj.get_named_property::<u32>("length")?;
                let mut bytes = Vec::with_capacity(length as usize);
                for i in 0..length {
                    let byte = obj.get_element::<u32>(i)?;
                    bytes.push(byte as u8);
                }
                turso_core::Value::Blob(bytes)
            } else {
                let s = value.coerce_to_string()?.into_utf8()?;
                turso_core::Value::Text(s.as_str()?.to_owned().into())
            }
        }
        _ => {
            let s = value.coerce_to_string()?.into_utf8()?;
            turso_core::Value::Text(s.as_str()?.to_owned().into())
        }
    })
}

// PostgreSQL type OIDs. Mirrors the pgwire `Type` constants used by
// `turso_pg_server::sqlite_type_to_pg_type`; the two mappings must agree so
// the in-process API and the wire server report identical column types.
const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_INT8: u32 = 20;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_TEXT: u32 = 25;
const OID_JSON: u32 = 114;
const OID_FLOAT8: u32 = 701;
const OID_MACADDR8: u32 = 774;
const OID_MACADDR: u32 = 829;
const OID_INET: u32 = 869;
const OID_CIDR: u32 = 650;
const OID_VARCHAR: u32 = 1043;
const OID_DATE: u32 = 1082;
const OID_TIME: u32 = 1083;
const OID_TIMESTAMP: u32 = 1114;
const OID_TIMESTAMPTZ: u32 = 1184;
const OID_NUMERIC: u32 = 1700;
const OID_UUID: u32 = 2950;
const OID_JSONB: u32 = 3802;

/// Map a declared column type name to a PostgreSQL type OID.
///
/// Same table as `turso_pg_server::sqlite_type_to_pg_type`, except this
/// mapping is exact where the wire server's is coarse: SMALLINT/INT2 report
/// INT2 and NUMERIC/DECIMAL report NUMERIC instead of being collapsed into
/// INT4/FLOAT8, because PGlite clients dispatch value parsing on the OID.
fn type_name_to_oid(type_str: &str) -> u32 {
    let upper = type_str.to_uppercase();
    match upper.as_str() {
        "INTEGER" | "INT" | "INT4" | "SERIAL" => OID_INT4,
        "SMALLINT" | "INT2" | "SMALLSERIAL" => OID_INT2,
        "BIGINT" | "INT8" | "BIGSERIAL" => OID_INT8,
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => OID_FLOAT8,
        "NUMERIC" | "DECIMAL" => OID_NUMERIC,
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" | "NAME" => OID_TEXT,
        "BLOB" | "BYTEA" => OID_BYTEA,
        "BOOLEAN" | "BOOL" => OID_BOOL,
        "UUID" => OID_UUID,
        "JSON" => OID_JSON,
        "JSONB" => OID_JSONB,
        "DATE" => OID_DATE,
        "TIME" | "TIMETZ" => OID_TIME,
        "TIMESTAMP" => OID_TIMESTAMP,
        "TIMESTAMPTZ" => OID_TIMESTAMPTZ,
        "INET" => OID_INET,
        "CIDR" => OID_CIDR,
        "MACADDR" => OID_MACADDR,
        "MACADDR8" => OID_MACADDR8,
        _ => {
            if upper.starts_with("VARCHAR") || upper.starts_with("CHAR") {
                OID_VARCHAR
            } else if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
                OID_NUMERIC
            } else {
                OID_TEXT
            }
        }
    }
}

/// Map a scalar type OID to its array counterpart. Keep in sync with
/// `turso_pg_server::scalar_pg_type_to_array_type`.
fn scalar_oid_to_array_oid(oid: u32) -> u32 {
    match oid {
        OID_BOOL => 1000,
        OID_BYTEA => 1001,
        OID_INT2 => 1005,
        OID_INT4 => 1007,
        OID_TEXT => 1009,
        OID_INT8 => 1016,
        OID_FLOAT8 => 1022,
        OID_UUID => 2951,
        OID_JSON => 199,
        OID_JSONB => 3807,
        OID_DATE => 1182,
        OID_TIME => 1183,
        OID_TIMESTAMP => 1115,
        OID_TIMESTAMPTZ => 1185,
        OID_INET => 1041,
        OID_CIDR => 651,
        OID_MACADDR => 1040,
        OID_MACADDR8 => 775,
        OID_NUMERIC => 1231,
        OID_VARCHAR => 1015,
        _ => 1009, // TEXT_ARRAY
    }
}

/// Decide the PG type OID for a result column. Mirrors
/// `turso_pg_server::resolve_pg_type_for_column`.
fn pg_oid_for_column(stmt: &turso_core::Statement, idx: usize) -> u32 {
    use turso_core::ColumnTypeKind;

    let Some(info) = stmt.get_column_type_info(idx).ok().flatten() else {
        return OID_TEXT;
    };
    let base = match info.kind {
        // STRUCT and UNION columns surface as JSONB, matching the wire server.
        ColumnTypeKind::Struct | ColumnTypeKind::Union => OID_JSONB,
        _ => {
            let mapped = type_name_to_oid(&info.declared_name);
            if mapped == OID_TEXT {
                info.base_type
                    .as_deref()
                    .map(type_name_to_oid)
                    .unwrap_or(OID_TEXT)
            } else {
                mapped
            }
        }
    };
    if info.array_dimensions > 0 {
        scalar_oid_to_array_oid(base)
    } else {
        base
    }
}
