#[cfg(all(feature = "web", feature = "nodejs"))]
compile_error!("Features 'web' and 'nodejs' cannot be enabled at the same time");

use js_sys::{Array, Object};
use std::cell::RefCell;
use std::sync::Arc;
use turso_core::{Clock, Instant, OpenFlags, Result};
use wasm_bindgen::prelude::*;

#[allow(dead_code)]
#[wasm_bindgen]
pub struct Database {
    db: Arc<turso_core::Database>,
    conn: Arc<turso_core::Connection>,
}

#[allow(clippy::arc_with_non_send_sync)]
#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new(path: &str) -> Database {
        let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO { vfs: VFS::new() });
        let file = io.open_file(path, OpenFlags::Create, false).unwrap();
        let db_file = Arc::new(DatabaseFile::new(file));
        let db = turso_core::Database::open(io, path, db_file, false, false).unwrap();
        let conn = db.connect().unwrap();
        Database { db, conn }
    }

    #[wasm_bindgen]
    pub fn exec(&self, _sql: &str) {
        self.conn.execute(_sql).unwrap();
    }

    #[wasm_bindgen]
    pub fn prepare(&self, _sql: &str) -> Statement {
        let stmt = self.conn.prepare(_sql).unwrap();
        Statement::new(RefCell::new(stmt), false)
    }
}

#[wasm_bindgen]
pub struct RowIterator {
    inner: RefCell<turso_core::Statement>,
}

#[wasm_bindgen]
impl RowIterator {
    fn new(inner: RefCell<turso_core::Statement>) -> Self {
        Self { inner }
    }

    #[wasm_bindgen]
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> JsValue {
        let mut stmt = self.inner.borrow_mut();
        match stmt.step() {
            Ok(turso_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                let row_array = Array::new();
                for value in row.get_values() {
                    let value = to_js_value(value);
                    row_array.push(&value);
                }
                JsValue::from(row_array)
            }
            Ok(turso_core::StepResult::IO) => JsValue::UNDEFINED,
            Ok(turso_core::StepResult::Done) | Ok(turso_core::StepResult::Interrupt) => {
                JsValue::UNDEFINED
            }

            Ok(turso_core::StepResult::Busy) => JsValue::UNDEFINED,
            Err(e) => panic!("Error: {e:?}"),
        }
    }
}

#[wasm_bindgen]
pub struct Statement {
    inner: RefCell<turso_core::Statement>,
    raw: bool,
}

#[wasm_bindgen]
impl Statement {
    fn new(inner: RefCell<turso_core::Statement>, raw: bool) -> Self {
        Self { inner, raw }
    }

    #[wasm_bindgen]
    pub fn raw(mut self, toggle: Option<bool>) -> Self {
        self.raw = toggle.unwrap_or(true);
        self
    }

    pub fn get(&self) -> JsValue {
        let mut stmt = self.inner.borrow_mut();
        match stmt.step() {
            Ok(turso_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                let row_array = js_sys::Array::new();
                for value in row.get_values() {
                    let value = to_js_value(value);
                    row_array.push(&value);
                }
                JsValue::from(row_array)
            }

            Ok(turso_core::StepResult::IO)
            | Ok(turso_core::StepResult::Done)
            | Ok(turso_core::StepResult::Interrupt)
            | Ok(turso_core::StepResult::Busy) => JsValue::UNDEFINED,
            Err(e) => panic!("Error: {e:?}"),
        }
    }

    pub fn all(&self) -> js_sys::Array {
        let array = js_sys::Array::new();
        loop {
            let mut stmt = self.inner.borrow_mut();
            match stmt.step() {
                Ok(turso_core::StepResult::Row) => {
                    let row = stmt.row().unwrap();
                    let row_array = js_sys::Array::new();
                    for value in row.get_values() {
                        let value = to_js_value(value);
                        row_array.push(&value);
                    }
                    array.push(&row_array);
                }
                Ok(turso_core::StepResult::IO) => {}
                Ok(turso_core::StepResult::Interrupt) => break,
                Ok(turso_core::StepResult::Done) => break,
                Ok(turso_core::StepResult::Busy) => break,
                Err(e) => panic!("Error: {e:?}"),
            }
        }
        array
    }

    #[wasm_bindgen]
    pub fn iterate(self) -> JsValue {
        let iterator = RowIterator::new(self.inner);
        let iterator_obj = Object::new();

        // Define the next method that will be called by JavaScript
        let next_fn = js_sys::Function::new_with_args(
            "",
            "const value = this.iterator.next();
             const done = value === undefined;
             return {
                value,
                done
             };",
        );

        js_sys::Reflect::set(&iterator_obj, &JsValue::from_str("next"), &next_fn).unwrap();

        js_sys::Reflect::set(
            &iterator_obj,
            &JsValue::from_str("iterator"),
            &JsValue::from(iterator),
        )
        .unwrap();

        let symbol_iterator = js_sys::Function::new_no_args("return this;");
        js_sys::Reflect::set(&iterator_obj, &js_sys::Symbol::iterator(), &symbol_iterator).unwrap();

        JsValue::from(iterator_obj)
    }
}

fn to_js_value(value: &turso_core::Value) -> JsValue {
    match value {
        turso_core::Value::Null => JsValue::null(),
        turso_core::Value::Integer(i) => {
            let i = *i;
            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                JsValue::from(i as i32)
            } else {
                JsValue::from(i)
            }
        }
        turso_core::Value::Float(f) => JsValue::from(*f),
        turso_core::Value::Text(t) => JsValue::from_str(t.as_str()),
        turso_core::Value::Blob(b) => js_sys::Uint8Array::from(b.as_slice()).into(),
    }
}

pub struct File {
    vfs: VFS,
    fd: i32,
}

unsafe impl Send for File {}
unsafe impl Sync for File {}

#[allow(dead_code)]
impl File {
    fn new(vfs: VFS, fd: i32) -> Self {
        Self { vfs, fd }
    }
}

impl turso_core::File for File {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        // TODO
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        // TODO
        Ok(())
    }

    fn pread(
        &self,
        pos: usize,
        c: Arc<turso_core::Completion>,
    ) -> Result<Arc<turso_core::Completion>> {
        let r = match c.completion_type {
            turso_core::CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        };
        let nr = {
            let mut buf = r.buf_mut();
            let buf: &mut [u8] = buf.as_mut_slice();
            self.vfs.pread(self.fd, buf, pos)
        };
        r.complete(nr);
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<std::cell::RefCell<turso_core::Buffer>>,
        c: Arc<turso_core::Completion>,
    ) -> Result<Arc<turso_core::Completion>> {
        let w = match c.completion_type {
            turso_core::CompletionType::Write(ref w) => w,
            _ => unreachable!(),
        };
        let buf = buffer.borrow();
        let buf: &[u8] = buf.as_slice();
        self.vfs.pwrite(self.fd, buf, pos);
        w.complete(buf.len() as i32);
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(c)
    }

    fn sync(&self, c: Arc<turso_core::Completion>) -> Result<Arc<turso_core::Completion>> {
        self.vfs.sync(self.fd);
        c.complete(0);
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self.vfs.size(self.fd))
    }

    fn truncate(&self, len: u64, c: turso_core::Completion) -> Result<Arc<turso_core::Completion>> {
        self.vfs.truncate(self.fd, len as usize);
        c.complete(0);
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(Arc::new(c))
    }
}

pub struct PlatformIO {
    vfs: VFS,
}
unsafe impl Send for PlatformIO {}
unsafe impl Sync for PlatformIO {}

impl Clock for PlatformIO {
    fn now(&self) -> Instant {
        let date = Date::new();
        let ms_since_epoch = date.getTime();

        Instant {
            secs: (ms_since_epoch / 1000.0) as i64,
            micros: ((ms_since_epoch % 1000.0) * 1000.0) as u32,
        }
    }
}

impl turso_core::IO for PlatformIO {
    fn open_file(
        &self,
        path: &str,
        _flags: OpenFlags,
        _direct: bool,
    ) -> Result<Arc<dyn turso_core::File>> {
        let fd = self.vfs.open(path, "a+");
        Ok(Arc::new(File {
            vfs: VFS::new(),
            fd,
        }))
    }

    fn wait_for_completion(&self, c: Arc<turso_core::Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_memory_io(&self) -> Arc<turso_core::MemoryIO> {
        Arc::new(turso_core::MemoryIO::new())
    }
}

#[wasm_bindgen]
extern "C" {
    type Date;

    #[wasm_bindgen(constructor)]
    fn new() -> Date;

    #[wasm_bindgen(method, getter)]
    fn toISOString(this: &Date) -> String;

    #[wasm_bindgen(method)]
    fn getTime(this: &Date) -> f64;
}

pub struct DatabaseFile {
    file: Arc<dyn turso_core::File>,
}

unsafe impl Send for DatabaseFile {}
unsafe impl Sync for DatabaseFile {}

impl DatabaseFile {
    pub fn new(file: Arc<dyn turso_core::File>) -> Self {
        Self { file }
    }
}

impl turso_core::DatabaseStorage for DatabaseFile {
    fn read_page(&self, page_idx: usize, c: turso_core::Completion) -> Result<()> {
        let r = match c.completion_type {
            turso_core::CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(turso_core::LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c.into())?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<std::cell::RefCell<turso_core::Buffer>>,
        c: turso_core::Completion,
    ) -> Result<()> {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c.into())?;
        Ok(())
    }

    fn sync(&self, c: turso_core::Completion) -> Result<()> {
        let _ = self.file.sync(c.into())?;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        self.file.size()
    }

    fn truncate(&self, len: u64, c: turso_core::Completion) -> Result<()> {
        self.file.truncate(len, c);
        Ok(())
    }
}

#[cfg(all(feature = "web", not(feature = "nodejs")))]
#[wasm_bindgen(module = "/web/src/web-vfs.js")]
extern "C" {
    type VFS;
    #[wasm_bindgen(constructor)]
    fn new() -> VFS;

    #[wasm_bindgen(method)]
    fn open(this: &VFS, path: &str, flags: &str) -> i32;

    #[wasm_bindgen(method)]
    fn close(this: &VFS, fd: i32) -> bool;

    #[wasm_bindgen(method)]
    fn pwrite(this: &VFS, fd: i32, buffer: &[u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn pread(this: &VFS, fd: i32, buffer: &mut [u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn size(this: &VFS, fd: i32) -> u64;

    #[wasm_bindgen(method)]
    fn truncate(this: &VFS, fd: i32, len: usize);

    #[wasm_bindgen(method)]
    fn sync(this: &VFS, fd: i32);
}

#[cfg(all(feature = "nodejs", not(feature = "web")))]
#[wasm_bindgen(module = "/node/src/vfs.cjs")]
extern "C" {
    type VFS;
    #[wasm_bindgen(constructor)]
    fn new() -> VFS;

    #[wasm_bindgen(method)]
    fn open(this: &VFS, path: &str, flags: &str) -> i32;

    #[wasm_bindgen(method)]
    fn close(this: &VFS, fd: i32) -> bool;

    #[wasm_bindgen(method)]
    fn pwrite(this: &VFS, fd: i32, buffer: &[u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn pread(this: &VFS, fd: i32, buffer: &mut [u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn size(this: &VFS, fd: i32) -> u64;

    #[wasm_bindgen(method)]
    fn truncate(this: &VFS, fd: i32, len: usize);

    #[wasm_bindgen(method)]
    fn sync(this: &VFS, fd: i32);
}

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}
