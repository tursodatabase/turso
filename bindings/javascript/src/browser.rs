use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use turso_core::{storage::database::DatabaseFile, Clock, File, Instant, IO};

use crate::{init_tracing, is_memory, Database, DatabaseOpts};

pub struct NoopTask;

impl Task for NoopTask {
    type Output = ();
    type JsValue = ();
    fn compute(&mut self) -> Result<Self::Output> {
        Ok(())
    }
    fn resolve(&mut self, _: Env, _: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

#[napi]
/// turso-db in the the browser requires explicit thread pool initialization
/// so, we just put no-op task on the thread pool and force emnapi to allocate web worker
pub fn init_thread_pool() -> napi::Result<AsyncTask<NoopTask>> {
    Ok(AsyncTask::new(NoopTask))
}

pub struct ConnectTask {
    path: String,
    is_memory: bool,
    io: Arc<dyn turso_core::IO>,
}

pub struct ConnectResult {
    db: Arc<turso_core::Database>,
    conn: Arc<turso_core::Connection>,
}

unsafe impl Send for ConnectResult {}

impl Task for ConnectTask {
    type Output = ConnectResult;
    type JsValue = Database;

    fn compute(&mut self) -> Result<Self::Output> {
        let file = self
            .io
            .open_file(&self.path, turso_core::OpenFlags::Create, false)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to open file: {e}")))?;

        let db_file = Arc::new(DatabaseFile::new(file));
        let db = turso_core::Database::open(self.io.clone(), &self.path, db_file, false, true)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to open database: {e}"),
                )
            })?;

        let conn = db
            .connect()
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to connect: {e}")))?;

        Ok(ConnectResult { db, conn })
    }

    fn resolve(&mut self, _: Env, result: Self::Output) -> Result<Self::JsValue> {
        Ok(Database::create(
            Some(result.db),
            self.io.clone(),
            result.conn,
            self.is_memory,
        ))
    }
}

#[napi]
// we offload connect to the web-worker because:
// 1. browser main-thread do not support Atomic.wait operations
// 2. turso-db use blocking IO [io.wait_for_completion(c)] in few places during initialization path
//
// so, we offload connect to the worker thread
pub fn connect(path: String, opts: Option<DatabaseOpts>) -> Result<AsyncTask<ConnectTask>> {
    if let Some(opts) = opts {
        init_tracing(opts.tracing);
    }
    let task = if is_memory(&path) {
        ConnectTask {
            io: Arc::new(turso_core::MemoryIO::new()),
            is_memory: true,
            path,
        }
    } else {
        let io = Arc::new(Opfs::new()?);
        ConnectTask {
            io,
            is_memory: false,
            path,
        }
    };
    Ok(AsyncTask::new(task))
}
#[napi]
#[derive(Clone)]
pub struct Opfs;

#[napi]
#[derive(Clone)]
struct OpfsFile {
    handle: i32,
}

#[napi]
impl Opfs {
    #[napi(constructor)]
    pub fn new() -> napi::Result<Self> {
        Ok(Self)
    }
}

impl Clock for Opfs {
    fn now(&self) -> Instant {
        Instant { secs: 0, micros: 0 } // TODO
    }
}

#[link(wasm_import_module = "env")]
extern "C" {
    fn lookup_file(path: *const u8, path_len: usize) -> i32;
    fn read(handle: i32, buffer: *mut u8, buffer_len: usize, offset: i32) -> i32;
    fn write(handle: i32, buffer: *const u8, buffer_len: usize, offset: i32) -> i32;
    fn sync(handle: i32) -> i32;
    fn truncate(handle: i32, length: usize) -> i32;
    fn size(handle: i32) -> i32;
    fn is_web_worker() -> bool;
}

fn is_web_worker_safe() -> bool {
    unsafe { is_web_worker() }
}

impl IO for Opfs {
    fn open_file(
        &self,
        path: &str,
        _: turso_core::OpenFlags,
        _: bool,
    ) -> turso_core::Result<std::sync::Arc<dyn turso_core::File>> {
        tracing::info!("open_file: {}", path);
        let result = unsafe { lookup_file(path.as_ptr(), path.len()) };
        if result >= 0 {
            Ok(Arc::new(OpfsFile { handle: result }))
        } else if result == -404 {
            Err(turso_core::LimboError::InternalError(
                "files must be created in advance for OPFS IO".to_string(),
            ))
        } else {
            Err(turso_core::LimboError::InternalError(format!(
                "unexpected file lookup error: {result}"
            )))
        }
    }

    fn remove_file(&self, _: &str) -> turso_core::Result<()> {
        Ok(())
    }
}

impl File for OpfsFile {
    fn lock_file(&self, _: bool) -> turso_core::Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        Ok(())
    }

    fn pread(
        &self,
        pos: u64,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            is_web_worker_safe(),
            "opfs must be used only from web worker for now"
        );
        tracing::debug!("pread({}): pos={}", self.handle, pos);
        let handle = self.handle;
        let read_c = c.as_read();
        let buffer = read_c.buf_arc();
        let buffer = buffer.as_mut_slice();
        let result = unsafe { read(handle, buffer.as_mut_ptr(), buffer.len(), pos as i32) };
        c.complete(result as i32);
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            is_web_worker_safe(),
            "opfs must be used only from web worker for now"
        );
        tracing::debug!("pwrite({}): pos={}", self.handle, pos);
        let handle = self.handle;
        let buffer = buffer.as_slice();
        let result = unsafe { write(handle, buffer.as_ptr(), buffer.len(), pos as i32) };
        c.complete(result as i32);
        Ok(c)
    }

    fn sync(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        assert!(
            is_web_worker_safe(),
            "opfs must be used only from web worker for now"
        );
        tracing::debug!("sync({})", self.handle);
        let handle = self.handle;
        let result = unsafe { sync(handle) };
        c.complete(result as i32);
        Ok(c)
    }

    fn truncate(
        &self,
        len: u64,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            is_web_worker_safe(),
            "opfs must be used only from web worker for now"
        );
        tracing::debug!("truncate({}): len={}", self.handle, len);
        let handle = self.handle;
        let result = unsafe { truncate(handle, len as usize) };
        c.complete(result as i32);
        Ok(c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        assert!(
            is_web_worker_safe(),
            "size can be called only from web worker context"
        );
        tracing::debug!("size({})", self.handle);
        let handle = self.handle;
        let result = unsafe { size(handle) };
        Ok(result as u64)
    }
}
