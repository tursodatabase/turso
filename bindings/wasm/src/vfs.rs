//! Storage for the browser.
//!
//! Turso's [`IO`] and [`File`] traits are synchronous, and so are OPFS sync
//! access handles, so the two line up directly. The one mismatch is opening a
//! file: getting a sync access handle is async, while [`IO::open_file`] is not.
//! The JS side therefore opens handles ahead of time (see `preopen` in
//! `js/vfs.js`) and keeps them in a registry; opening here is just a lookup.
//!
//! Rust holds nothing but an integer handle. The real `FileSystemSyncAccessHandle`
//! objects live in JS, which keeps these types `Send + Sync` without any unsafe
//! impls, and keeps `JsValue` (which is neither) out of the structs entirely.

use turso_core::io::FileSyncType;
use turso_core::{
    Buffer, Clock, Completion, File, LimboError, MemoryIO, MonotonicInstant, OpenFlags, Result,
    WallClockInstant, IO,
};

use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(module = "/js/vfs.js")]
extern "C" {
    #[wasm_bindgen(js_name = vfsOpen)]
    fn vfs_open(path: &str, create: bool) -> i32;

    #[wasm_bindgen(js_name = vfsRead)]
    fn vfs_read(fd: i32, offset: f64, buffer: &mut [u8]) -> i32;

    #[wasm_bindgen(js_name = vfsWrite)]
    fn vfs_write(fd: i32, offset: f64, buffer: &[u8]) -> i32;

    #[wasm_bindgen(js_name = vfsSync)]
    fn vfs_sync(fd: i32) -> i32;

    #[wasm_bindgen(js_name = vfsSize)]
    fn vfs_size(fd: i32) -> f64;

    #[wasm_bindgen(js_name = vfsTruncate)]
    fn vfs_truncate(fd: i32, len: f64) -> i32;

    #[wasm_bindgen(js_name = vfsRemove)]
    fn vfs_remove(path: &str) -> i32;

    #[wasm_bindgen(js_name = preopen, catch)]
    async fn js_preopen(path: &str) -> std::result::Result<JsValue, JsValue>;

    #[wasm_bindgen(js_name = closeAll)]
    fn js_close_all();
}

/// Opens the OPFS handles for `path` and its write-ahead log.
///
/// Await this before constructing a `Database`. It is re-exported through the
/// wasm module on purpose: wasm-bindgen copies `js/vfs.js` into
/// `pkg/snippets/`, so importing that file directly would create a second
/// registry that Rust never sees, and every open would fail.
#[wasm_bindgen]
pub async fn preopen(path: String) -> std::result::Result<(), JsValue> {
    js_preopen(&path).await.map(|_| ())
}

/// Releases every open handle. Call it before the page goes away, or the next
/// load cannot reopen the files.
#[wasm_bindgen(js_name = closeAll)]
pub fn close_all() {
    js_close_all();
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = Date, js_name = now)]
    fn date_now() -> f64;

    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    fn performance_now() -> f64;
}

/// Monotonic time from the browser. `std::time::Instant` panics on
/// wasm32-unknown-unknown, so nothing here may go through `DefaultClock`.
fn monotonic_now() -> MonotonicInstant {
    // performance.now() is milliseconds, with a fractional part.
    MonotonicInstant::from_nanos((performance_now() * 1_000_000.0) as u128)
}

fn wall_clock_now() -> WallClockInstant {
    let millis = date_now();
    WallClockInstant {
        secs: (millis / 1000.0) as i64,
        micros: ((millis % 1000.0) * 1000.0) as u32,
    }
}

/// Core's [`MemoryIO`] with the clock replaced.
///
/// `MemoryIO` reads the clock through `DefaultClock`, which panics on wasm, so
/// its storage is reused but its timekeeping is not.
pub struct MemoryIOForWasm(MemoryIO);

impl MemoryIOForWasm {
    pub fn new() -> Self {
        Self(MemoryIO::new())
    }
}

impl IO for MemoryIOForWasm {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.0.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.0.remove_file(path)
    }

    fn step(&self) -> Result<()> {
        self.0.step()
    }
}

impl Clock for MemoryIOForWasm {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        monotonic_now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        wall_clock_now()
    }
}

fn io_err(what: &str) -> LimboError {
    LimboError::InternalError(format!("opfs: {what} failed"))
}

/// An [`IO`] backed by the Origin Private File System.
pub struct OpfsIO;

impl IO for OpfsIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        let fd = vfs_open(path, flags.contains(OpenFlags::Create));
        if fd < 0 {
            return Err(LimboError::InternalError(format!(
                "opfs: no open handle for {path}. Call preopen(path) before opening the database."
            )));
        }
        Ok(Arc::new(OpfsFile { fd }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        if vfs_remove(path) < 0 {
            return Err(io_err("remove_file"));
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        // Every operation completed before its call returned, so there is
        // nothing pending to drive here.
        Ok(())
    }
}

impl Clock for OpfsIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        monotonic_now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        wall_clock_now()
    }
}

/// A file held open by the JS side; `fd` indexes its registry.
struct OpfsFile {
    fd: i32,
}

impl File for OpfsFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        // A sync access handle is already exclusive for as long as it is open,
        // so acquiring one is the lock.
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let read = {
            let r = c.as_read();
            let buf = r.buf();
            vfs_read(self.fd, pos as f64, buf.as_mut_slice())
        };
        if read < 0 {
            return Err(io_err("pread"));
        }
        c.complete(read);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        let written = vfs_write(self.fd, pos as f64, buffer.as_slice());
        if written < 0 {
            return Err(io_err("pwrite"));
        }
        c.complete(written);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        if vfs_sync(self.fd) < 0 {
            return Err(io_err("sync"));
        }
        c.complete(0);
        Ok(c)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        if vfs_truncate(self.fd, len as f64) < 0 {
            return Err(io_err("truncate"));
        }
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let size = vfs_size(self.fd);
        if size < 0.0 {
            return Err(io_err("size"));
        }
        Ok(size as u64)
    }
}
