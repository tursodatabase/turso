use super::MemoryIO;
use crate::{Clock, Completion, CompletionType, File, Instant, LimboError, OpenFlags, Result, IO};
use parking_lot::Mutex;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tracing::{debug, trace};

type CompletionCallback = Box<dyn Fn() -> Result<()>>;
// TODO: Arc + Mutex here for Send + Sync functionality
// can maybe see a way for only submitting IO through
type CallbackQueue = Arc<Mutex<Vec<CompletionCallback>>>;

pub struct GenericIO {
    callbacks: CallbackQueue,
}

impl GenericIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'generic'");
        Ok(Self {
            callbacks: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

unsafe impl Send for GenericIO {}
unsafe impl Sync for GenericIO {}

impl IO for GenericIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;
        Ok(Arc::new(GenericFile {
            file: Arc::new(Mutex::new(file)),
            callbacks: self.callbacks.clone(),
        }))
    }

    fn wait_for_completion(&self, c: Arc<Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        let mut callbacks = self.callbacks.lock();
        if callbacks.is_empty() {
            return Ok(());
        }
        trace!("run_once() waits for events");
        let events = callbacks.drain(0..);
        for callback in events {
            callback()?;
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }
}

impl Clock for GenericIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

pub struct GenericFile {
    file: Arc<Mutex<std::fs::File>>,
    callbacks: CallbackQueue,
}

unsafe impl Send for GenericFile {}
unsafe impl Sync for GenericFile {}

impl File for GenericFile {
    // Since we let the OS handle the locking, file locking is not supported on the generic IO implementation
    // No-op implementation allows compilation but provides no actual file locking.
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Arc<Completion> {
        let c = Arc::new(c);
        let file = self.file.clone();
        let clone_c = c.clone();
        let callback = Box::new(move || -> Result<()> {
            let mut file = file.lock();
            file.seek(std::io::SeekFrom::Start(pos as u64))?;
            {
                let r = match clone_c.completion_type {
                    CompletionType::Read(ref r) => r,
                    _ => unreachable!(),
                };
                let mut buf = r.buf_mut();
                let buf = buf.as_mut_slice();
                file.read_exact(buf)?;
            }
            clone_c.complete(0);
            Ok(())
        });
        self.callbacks.lock().push(callback);
        c
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<crate::Buffer>>,
        c: Completion,
    ) -> Arc<Completion> {
        let c = Arc::new(c);
        let file = self.file.clone();
        let clone_c = c.clone();
        let callback = Box::new(move || -> Result<()> {
            let mut file = file.lock();
            file.seek(std::io::SeekFrom::Start(pos as u64))?;
            let buf = buffer.borrow();
            let buf = buf.as_slice();
            file.write_all(buf)?;
            clone_c.complete(buf.len() as i32);
            Ok(())
        });
        self.callbacks.lock().push(callback);
        c
    }

    fn sync(&self, c: Completion) -> Arc<Completion> {
        let c = Arc::new(c);
        let file = self.file.clone();
        let clone_c = c.clone();
        let callback = Box::new(move || -> Result<()> {
            let file = file.lock();
            file.sync_all().map_err(|err| LimboError::IOError(err))?;
            clone_c.complete(0);
            Ok(())
        });
        self.callbacks.lock().push(callback);
        c
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.lock();
        Ok(file.metadata()?.len())
    }
}

impl Drop for GenericFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}
