use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

use rand::{Rng as _, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tracing::{instrument, Level};
use turso_core::{CompletionType, File, Result};

use crate::runner::memory::io::{CallbackQueue, Fd, Operation};

pub struct MemorySimFile {
    pub callbacks: CallbackQueue,
    pub fd: Arc<Fd>,
    pub buffer: RefCell<Vec<u8>>,
    // TODO: add fault map later here
    pub closed: Cell<bool>,

    /// Number of `pread` function calls (both success and failures).
    pub nr_pread_calls: Cell<usize>,
    /// Number of `pwrite` function calls (both success and failures).
    pub nr_pwrite_calls: Cell<usize>,
    /// Number of `sync` function calls (both success and failures).
    pub nr_sync_calls: Cell<usize>,

    pub rng: RefCell<ChaCha8Rng>,

    pub latency_probability: usize,
}

unsafe impl Send for MemorySimFile {}
unsafe impl Sync for MemorySimFile {}

impl MemorySimFile {
    pub fn new(callbacks: CallbackQueue, fd: Fd, seed: u64, latency_probability: usize) -> Self {
        Self {
            callbacks,
            fd: Arc::new(fd),
            buffer: RefCell::new(Vec::new()),
            closed: Cell::new(false),
            nr_pread_calls: Cell::new(0),
            nr_pwrite_calls: Cell::new(0),
            nr_sync_calls: Cell::new(0),
            rng: RefCell::new(ChaCha8Rng::seed_from_u64(seed)),
            latency_probability,
        }
    }

    pub fn stats_table(&self) -> String {
        let sum_calls =
            self.nr_pread_calls.get() + self.nr_pwrite_calls.get() + self.nr_sync_calls.get();
        let stats_table = [
            "op        calls   ".to_string(),
            "--------- --------".to_string(),
            format!("pread     {:8}", self.nr_pread_calls.get()),
            format!("pwrite    {:8}", self.nr_pwrite_calls.get()),
            format!("sync      {:8}", self.nr_sync_calls.get()),
            "--------- -------- --------".to_string(),
            format!("total     {sum_calls:8}"),
        ];

        stats_table.join("\n")
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn generate_latency_duration(&self) -> Option<std::time::Duration> {
        let mut rng = self.rng.borrow_mut();
        // Chance to introduce some latency
        rng.gen_bool(self.latency_probability as f64 / 100.0)
            .then(|| std::time::Duration::from_millis(rng.gen_range(20..50)))
    }
}

impl File for MemorySimFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(
        &self,
        pos: usize,
        mut c: turso_core::Completion,
    ) -> Result<Arc<turso_core::Completion>> {
        self.nr_pread_calls.set(self.nr_pread_calls.get() + 1);
        if let Some(latency) = self.generate_latency_duration() {
            let CompletionType::Read(read_completion) = &mut c.completion_type else {
                unreachable!();
            };
            let before = self.rng.borrow_mut().gen_bool(0.5);
            let dummy_complete = Box::new(|_, _| {});
            let prev_complete = std::mem::replace(&mut read_completion.complete, dummy_complete);
            let new_complete = move |res, bytes_read| {
                if before {
                    std::thread::sleep(latency);
                }
                (prev_complete)(res, bytes_read);
                if !before {
                    std::thread::sleep(latency);
                }
            };
            read_completion.complete = Box::new(new_complete);
        };
        let c = Arc::new(c);
        let op = Operation::Read {
            fd: self.fd.clone(),
            completion: c.clone(),
            offset: pos,
        };
        self.callbacks.lock().push(op);
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<turso_core::Buffer>>,
        mut c: turso_core::Completion,
    ) -> Result<Arc<turso_core::Completion>> {
        self.nr_pwrite_calls.set(self.nr_pwrite_calls.get() + 1);
        if let Some(latency) = self.generate_latency_duration() {
            let CompletionType::Write(write_completion) = &mut c.completion_type else {
                unreachable!();
            };
            let before = self.rng.borrow_mut().gen_bool(0.5);
            let dummy_complete = Box::new(|_| {});
            let prev_complete = std::mem::replace(&mut write_completion.complete, dummy_complete);
            let new_complete = move |res| {
                if before {
                    std::thread::sleep(latency);
                }
                (prev_complete)(res);
                if !before {
                    std::thread::sleep(latency);
                }
            };
            write_completion.complete = Box::new(new_complete);
        };
        let c = Arc::new(c);
        let op = Operation::Write {
            fd: self.fd.clone(),
            buffer,
            completion: c.clone(),
            offset: pos,
        };
        self.callbacks.lock().push(op);
        Ok(c)
    }

    fn sync(&self, mut c: turso_core::Completion) -> Result<Arc<turso_core::Completion>> {
        self.nr_sync_calls.set(self.nr_sync_calls.get() + 1);
        if let Some(latency) = self.generate_latency_duration() {
            let CompletionType::Sync(sync_completion) = &mut c.completion_type else {
                unreachable!();
            };
            let before = self.rng.borrow_mut().gen_bool(0.5);
            let dummy_complete = Box::new(|_| {});
            let prev_complete = std::mem::replace(&mut sync_completion.complete, dummy_complete);
            let new_complete = move |res| {
                if before {
                    std::thread::sleep(latency);
                }
                (prev_complete)(res);
                if !before {
                    std::thread::sleep(latency);
                }
            };
            sync_completion.complete = Box::new(new_complete);
        };
        let c = Arc::new(c);
        let op = Operation::Sync {
            completion: c.clone(),
        };
        self.callbacks.lock().push(op);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        // TODO: size operation should also be scheduled. But this requires a change in how we
        // Use this function internally in Turso
        Ok(self.buffer.borrow().len() as u64)
    }
}
