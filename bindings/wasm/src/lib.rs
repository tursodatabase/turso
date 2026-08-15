//! Turso for the browser, built with wasm-pack.
//!
//! Files live in the Origin Private File System through the VFS in [`vfs`].
//! Because OPFS hands out synchronous access handles only inside a Web Worker,
//! and only after an async setup step, callers must `await preopen(path)` from
//! `js/vfs.js` before constructing a [`Database`].

mod vfs;

use std::sync::Arc;

use turso_core::{
    Connection, Database as CoreDatabase, Numeric, OpenFlags, OpenOptions, SqliteDialect,
    StepResult, Value,
};
use wasm_bindgen::prelude::*;

use crate::vfs::{MemoryIOForWasm, OpfsIO};

/// Routes panics to `console.error` instead of an opaque `unreachable`.
#[wasm_bindgen(start)]
pub fn start() {
    std::panic::set_hook(Box::new(|info| {
        web_error(&info.to_string());
    }));
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn web_error(message: &str);
}

fn js_err(error: impl std::fmt::Display) -> JsError {
    JsError::new(&error.to_string())
}

/// An open database.
#[wasm_bindgen]
pub struct Database {
    conn: Arc<Connection>,
}

#[wasm_bindgen]
impl Database {
    /// Opens `path` in OPFS. `await preopen(path)` first, or this fails
    /// because the underlying handle will not exist yet.
    #[wasm_bindgen(constructor)]
    pub fn new(path: &str) -> Result<Database, JsError> {
        Self::open(Arc::new(OpfsIO), path)
    }

    /// Opens a database that lives only in memory, for tests and scratch work.
    #[wasm_bindgen(js_name = inMemory)]
    pub fn in_memory() -> Result<Database, JsError> {
        Self::open(Arc::new(MemoryIOForWasm::new()), ":memory:")
    }

    fn open(io: Arc<dyn turso_core::IO>, path: &str) -> Result<Database, JsError> {
        let options = OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::Create);
        let db = CoreDatabase::open(io, path, options).map_err(js_err)?;
        let conn = db.connect().map_err(js_err)?;
        Ok(Database { conn })
    }

    /// Runs `sql`, discarding any rows, and reports how many rows changed.
    pub fn exec(&self, sql: &str) -> Result<i64, JsError> {
        let before = self.conn.total_changes();
        let mut stmt = self.conn.prepare(sql).map_err(js_err)?;
        loop {
            match stmt.step().map_err(js_err)? {
                StepResult::Done => break,
                StepResult::Row | StepResult::Yield => continue,
                // A browser cannot block, and the docs allow treating Sleep
                // like IO: drive the event loop and step again.
                StepResult::IO | StepResult::Sleep { .. } => {
                    stmt.get_pager().io.step().map_err(js_err)?;
                }
                StepResult::Interrupt => return Err(JsError::new("statement was interrupted")),
                StepResult::Busy => return Err(JsError::new("database is busy")),
            }
        }
        Ok(self.conn.total_changes() - before)
    }

    /// Runs `sql` and returns its rows as an array of objects, one key per
    /// column.
    pub fn query(&self, sql: &str) -> Result<js_sys::Array, JsError> {
        let mut stmt = self.conn.prepare(sql).map_err(js_err)?;
        let columns: Vec<String> = (0..stmt.num_columns())
            .map(|i| stmt.get_column_name(i).into_owned())
            .collect();

        let rows = js_sys::Array::new();
        loop {
            match stmt.step().map_err(js_err)? {
                StepResult::Done => break,
                StepResult::Yield => continue,
                // A browser cannot block, and the docs allow treating Sleep
                // like IO: drive the event loop and step again.
                StepResult::IO | StepResult::Sleep { .. } => {
                    stmt.get_pager().io.step().map_err(js_err)?;
                }
                StepResult::Interrupt => return Err(JsError::new("statement was interrupted")),
                StepResult::Busy => return Err(JsError::new("database is busy")),
                StepResult::Row => {
                    let Some(row) = stmt.row() else { continue };
                    let object = js_sys::Object::new();
                    for (index, name) in columns.iter().enumerate() {
                        let value = to_js(row.get_value(index));
                        js_sys::Reflect::set(&object, &JsValue::from_str(name), &value)
                            .map_err(|_| JsError::new("could not build the row object"))?;
                    }
                    rows.push(&object);
                }
            }
        }
        Ok(rows)
    }

    /// The rowid of the last successful insert on this connection.
    #[wasm_bindgen(js_name = lastInsertRowId)]
    pub fn last_insert_row_id(&self) -> i64 {
        self.conn.last_insert_rowid()
    }
}

/// Maps a Turso value onto its closest JS counterpart. Integers wider than
/// `Number` can hold become `BigInt` so they survive the trip.
fn to_js(value: &Value) -> JsValue {
    match value {
        Value::Null => JsValue::NULL,
        Value::Numeric(Numeric::Integer(i)) => {
            const SAFE: i64 = 9_007_199_254_740_991;
            if *i <= SAFE && *i >= -SAFE {
                JsValue::from_f64(*i as f64)
            } else {
                JsValue::from(*i)
            }
        }
        Value::Numeric(Numeric::Float(f)) => JsValue::from_f64(f64::from(*f)),
        Value::Text(text) => JsValue::from_str(text.as_str()),
        Value::Blob(blob) => js_sys::Uint8Array::from(blob.as_slice()).into(),
    }
}
