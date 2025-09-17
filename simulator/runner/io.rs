use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, Instant, OpenFlags, PlatformIO, Result, IO};

use crate::{
    profiles::io::IOProfile,
    runner::{clock::SimulatorClock, file::SimulatorFile, SimIO},
};

pub(crate) struct SimulatorIO {
    pub(crate) inner: Box<dyn IO>,
    pub(crate) fault: Cell<bool>,
    pub(crate) files: RefCell<Vec<Arc<SimulatorFile>>>,
    pub(crate) rng: RefCell<ChaCha8Rng>,
    pub(crate) page_size: usize,
    seed: u64,
    clock: Arc<SimulatorClock>,
    io_profile: IOProfile,
}

unsafe impl Send for SimulatorIO {}
unsafe impl Sync for SimulatorIO {}

impl SimulatorIO {
    pub(crate) fn new(seed: u64, page_size: usize, io_profile: IOProfile) -> Result<Self> {
        let inner = Box::new(PlatformIO::new()?);
        let fault = Cell::new(false);
        let files = RefCell::new(Vec::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let clock = SimulatorClock::new(
            ChaCha8Rng::seed_from_u64(seed),
            io_profile.latency.min_tick,
            io_profile.latency.max_tick,
        );

        Ok(Self {
            inner,
            fault,
            files,
            rng,
            page_size,
            seed,
            clock: Arc::new(clock),
            io_profile,
        })
    }
}

impl SimIO for SimulatorIO {
    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
        for file in self.files.borrow().iter() {
            file.inject_fault(fault);
        }
    }

    fn print_stats(&self) {
        for file in self.files.borrow().iter() {
            tracing::info!(
                "\n===========================\n\nPath: {}\n{}",
                file.path,
                file.stats_table()
            );
        }
    }

    fn syncing(&self) -> bool {
        let files = self.files.borrow();
        // TODO: currently assuming we only have 1 file that is syncing
        files
            .iter()
            .any(|file| file.sync_completion.borrow().is_some())
    }

    fn close_files(&self) {
        self.files.borrow_mut().clear()
    }
}

impl Clock for SimulatorIO {
    fn now(&self) -> Instant {
        self.clock.now().into()
    }
}

impl IO for SimulatorIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        _direct: bool,
    ) -> Result<Arc<dyn turso_core::File>> {
        let inner = self.inner.open_file(path, flags, false)?;
        let file = Arc::new(SimulatorFile::new(
            path,
            inner,
            self.page_size,
            self.seed,
            self.io_profile.clone(),
            self.clock.clone(),
        ));
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.files.borrow_mut().retain(|x| x.path != path);
        Ok(())
    }

    fn step(&self) -> Result<()> {
        let now = self.now();
        for file in self.files.borrow().iter() {
            file.run_queued_io(now)?;
        }
        self.inner.step()?;
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().next_u64() as i64
    }
}
