use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use napi::{bindgen_prelude::*, AsyncWorkPromise};
use napi_derive::napi;
use turso_core::{Clock, Completion, File, Instant, IO};

#[napi(object)]
#[derive(Clone)]
pub struct OpfsFileMeta {
    pub handle: i32,
}

#[napi(object)]
pub struct OpfsRegistry {
    pub path: String,
    pub files: HashMap<String, OpfsFileMeta>,
}

#[napi]
pub struct Opfs {
    env: Env,
    registry: OpfsRegistry,
}

unsafe impl Send for Opfs {}
unsafe impl Sync for Opfs {}

#[napi]
struct OpfsFile {
    env: Env,
    meta: OpfsFileMeta,
}

unsafe impl Send for OpfsFile {}
unsafe impl Sync for OpfsFile {}

#[napi]
impl Opfs {
    #[napi(constructor)]
    pub fn new(env: Env, registry: OpfsRegistry) -> napi::Result<Self> {
        Ok(Self { env, registry })
    }
}

impl Clock for Opfs {
    fn now(&self) -> Instant {
        Instant { secs: 0, micros: 0 }
    }
}

#[link(wasm_import_module = "env")]
extern "C" {
    fn read(handle: i32, buffer: *mut u8, buffer_len: usize, offset: i32) -> i32;
    fn write(handle: i32, buffer: *const u8, buffer_len: usize, offset: i32) -> i32;
    fn sync(handle: i32) -> i32;
    fn truncate(handle: i32, length: usize) -> i32;
    fn size(handle: i32) -> i32;
}

impl IO for Opfs {
    fn open_file(
        &self,
        path: &str,
        _: turso_core::OpenFlags,
        _: bool,
    ) -> turso_core::Result<std::sync::Arc<dyn turso_core::File>> {
        if let Some(meta) = self.registry.files.get(path) {
            Ok(Arc::new(OpfsFile {
                env: self.env.clone(),
                meta: meta.clone(),
            }))
        } else {
            Err(turso_core::LimboError::InternalError(
                "files must be created in advance for OPFS IO".to_string(),
            ))
        }
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        Ok(())
    }
}

impl File for OpfsFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
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
        tracing::debug!("pread: {}", pos);
        let handle = self.meta.handle;
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
        tracing::debug!("pwrite: {}", pos);
        let handle = self.meta.handle;
        let buffer = buffer.as_slice();
        let result = unsafe { write(handle, buffer.as_ptr(), buffer.len(), pos as i32) };
        c.complete(result as i32);
        Ok(c)
    }

    fn sync(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        tracing::debug!("sync");
        let handle = self.meta.handle;
        let result = unsafe { sync(handle) };
        c.complete(result as i32);
        Ok(c)
    }

    fn truncate(
        &self,
        len: u64,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let handle = self.meta.handle;
        let result = unsafe { truncate(handle, len as usize) };
        c.complete(result as i32);
        Ok(c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        tracing::debug!("size");
        let handle = self.meta.handle;
        let result = unsafe { size(handle) };
        return Ok(result as u64);
    }
}
