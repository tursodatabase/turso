use std::{cell::RefCell, collections::HashMap, sync::Arc};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use turso_core::{Clock, Completion, File, MonotonicInstant, WallClockInstant, IO};

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

#[napi]
#[derive(Clone)]
pub struct Opfs {
    inner: Arc<OpfsInner>,
}

pub struct OpfsInner {
    completion_no: RefCell<u32>,
    completions: RefCell<HashMap<u32, Completion>>,
}

thread_local! {
    static OPFS: Arc<Opfs> = Arc::new(Opfs::default());
}

#[napi]
#[derive(Clone)]
struct OpfsFile {
    handle: i32,
    opfs: Opfs,
}

unsafe impl Send for Opfs {}
unsafe impl Sync for Opfs {}

#[napi]
pub fn complete_opfs(completion_no: u32, result: i32) {
    OPFS.with(|opfs| opfs.complete(completion_no, result))
}

pub fn opfs() -> Arc<Opfs> {
    OPFS.with(|opfs| opfs.clone())
}

impl Opfs {
    pub fn complete(&self, completion_no: u32, result: i32) {
        let completion = {
            let mut completions = self.inner.completions.borrow_mut();
            completions.remove(&completion_no).unwrap()
        };
        completion.complete(result);
    }

    fn register_completion(&self, c: Completion) -> u32 {
        let inner = &self.inner;
        *inner.completion_no.borrow_mut() += 1;
        let completion_no = *inner.completion_no.borrow();
        tracing::debug!(
            "register completion: {} {:?}",
            completion_no,
            Arc::as_ptr(inner)
        );
        inner.completions.borrow_mut().insert(completion_no, c);
        completion_no
    }
}

impl Clock for Opfs {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        MonotonicInstant::now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        WallClockInstant::now()
    }
}

impl Default for Opfs {
    fn default() -> Self {
        Self {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Arc::new(OpfsInner {
                completion_no: RefCell::new(0),
                completions: RefCell::new(HashMap::new()),
            }),
        }
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

    fn write_async(handle: i32, buffer: *const u8, buffer_len: usize, offset: i32, c: u32);
    fn sync_async(handle: i32, c: u32);
    fn read_async(handle: i32, buffer: *mut u8, buffer_len: usize, offset: i32, c: u32);
    fn truncate_async(handle: i32, length: usize, c: u32);
    // fn size_async(handle: i32) -> i32;

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
            Ok(Arc::new(OpfsFile {
                handle: result,
                opfs: Opfs {
                    inner: self.inner.clone(),
                },
            }))
        } else if result == -404 {
            Err(turso_core::LimboError::InternalError(format!(
                "unexpected path {path}: files must be created in advance for OPFS IO"
            )))
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
        let web_worker = is_web_worker_safe();
        tracing::debug!(
            "pread({}, is_web_worker={}): pos={}",
            self.handle,
            web_worker,
            pos
        );
        let handle = self.handle;
        let read_c = c.as_read();
        let buffer = read_c.buf_arc();
        let buffer = buffer.as_mut_slice();
        if web_worker {
            let result = unsafe { read(handle, buffer.as_mut_ptr(), buffer.len(), pos as i32) };
            c.complete(result as i32);
        } else {
            let completion_no = self.opfs.register_completion(c.clone());
            unsafe {
                read_async(
                    handle,
                    buffer.as_mut_ptr(),
                    buffer.len(),
                    pos as i32,
                    completion_no,
                )
            };
        }
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let web_worker = is_web_worker_safe();
        tracing::debug!(
            "pwrite({}, is_web_worker={}): pos={}",
            self.handle,
            web_worker,
            pos
        );
        let handle = self.handle;
        let buffer = buffer.as_slice();
        if web_worker {
            let result = unsafe { write(handle, buffer.as_ptr(), buffer.len(), pos as i32) };
            c.complete(result as i32);
        } else {
            let completion_no = self.opfs.register_completion(c.clone());
            unsafe {
                write_async(
                    handle,
                    buffer.as_ptr(),
                    buffer.len(),
                    pos as i32,
                    completion_no,
                )
            };
        }
        Ok(c)
    }

    fn sync(
        &self,
        c: turso_core::Completion,
        _sync_type: turso_core::io::FileSyncType,
    ) -> turso_core::Result<turso_core::Completion> {
        let web_worker = is_web_worker_safe();
        tracing::debug!("sync({}, is_web_worker={})", self.handle, web_worker);
        let handle = self.handle;
        if web_worker {
            let result = unsafe { sync(handle) };
            c.complete(result as i32);
        } else {
            let completion_no = self.opfs.register_completion(c.clone());
            unsafe { sync_async(handle, completion_no) };
        }
        Ok(c)
    }

    fn truncate(
        &self,
        len: u64,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let web_worker = is_web_worker_safe();
        tracing::debug!(
            "truncate({}, is_web_worker={}): len={}",
            self.handle,
            web_worker,
            len
        );
        let handle = self.handle;
        if web_worker {
            let result = unsafe { truncate(handle, len as usize) };
            c.complete(result as i32);
        } else {
            let completion_no = self.opfs.register_completion(c.clone());
            unsafe { truncate_async(handle, len as usize, completion_no) };
        }
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
