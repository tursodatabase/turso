use super::MemoryIO;
use crate::{Clock, Completion, File, Instant, LimboError, OpenFlags, Result, IO};
use parking_lot::Mutex;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tracing::{debug, trace};

type CompletionCallback = Box<dyn Fn() -> Result<()>>;
// TODO: Arc + Mutex here for Send + Sync functionality
// can maybe see a way for only submitting IO through
type CallbackQueue = Arc<Mutex<Vec<CompletionCallback>>>;

pub struct WindowsIO {
    callbacks: CallbackQueue,
}

impl WindowsIO {
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {
            callbacks: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

unsafe impl Send for WindowsIO {}
unsafe impl Sync for WindowsIO {}

impl IO for WindowsIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path)?;
        Ok(Arc::new(WindowsFile {
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
        while let Some(callback) = callbacks.pop() {
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

impl Clock for WindowsIO {
    fn now(&self) -> Instant {
        let now = chrono::Local::now();
        Instant {
            secs: now.timestamp(),
            micros: now.timestamp_subsec_micros(),
        }
    }
}

pub struct WindowsFile {
    file: Arc<Mutex<std::fs::File>>,
    callbacks: CallbackQueue,
}

unsafe impl Send for WindowsFile {}
unsafe impl Sync for WindowsFile {}

impl File for WindowsFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        unimplemented!()
    }

    fn unlock_file(&self) -> Result<()> {
        unimplemented!()
    }

    fn pread(&self, pos: usize, c: Completion) -> Arc<Completion> {
        let c = Arc::new(c);
        let file = self.file.clone();
        let clone_c = c.clone();
        let callback = Box::new(move || -> Result<()> {
            let mut file = file.lock();
            file.seek(std::io::SeekFrom::Start(pos as u64))?;
            let nr = {
                let r = clone_c.as_read();
                let mut buf = r.buf_mut();
                let buf = buf.as_mut_slice();
                file.read_exact(buf)?;
                buf.len() as i32
            };
            clone_c.complete(nr);
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
            clone_c.complete(buffer.borrow().len() as i32);
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
