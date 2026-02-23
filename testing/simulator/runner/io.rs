use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Clock, IO, MonotonicInstant, OpenFlags, PlatformIO, Result, WallClockInstant};

use crate::runner::{SimIO, cli::IoBackend, clock::SimulatorClock, file::SimulatorFile};

pub(crate) struct SimulatorIO {
    pub(crate) inner: Box<dyn IO>,
    pub(crate) fault: Cell<bool>,
    pub(crate) files: RefCell<Vec<Arc<SimulatorFile>>>,
    pub(crate) rng: RefCell<ChaCha8Rng>,
    pub(crate) page_size: usize,
    seed: u64,
    latency_probability: u8,
    clock: Arc<SimulatorClock>,
}

unsafe impl Send for SimulatorIO {}
unsafe impl Sync for SimulatorIO {}

impl SimulatorIO {
    pub(crate) fn new(
        seed: u64,
        page_size: usize,
        latency_probability: u8,
        min_tick: u64,
        max_tick: u64,
        io_backend: IoBackend,
    ) -> Result<Self> {
        let inner: Box<dyn turso_core::IO> = match io_backend {
            IoBackend::Default => Box::new(PlatformIO::new()?),
            #[cfg(target_os = "linux")]
            IoBackend::IoUring => Box::new(turso_core::UringIO::new()?),
            IoBackend::Memory => {
                panic!("Memory IO has its own impl, is not supported in SimulatorIO");
            }
        };
        let fault = Cell::new(false);
        let files = RefCell::new(Vec::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let clock = SimulatorClock::new(ChaCha8Rng::seed_from_u64(seed), min_tick, max_tick);

        Ok(Self {
            inner,
            fault,
            files,
            rng,
            page_size,
            seed,
            latency_probability,
            clock: Arc::new(clock),
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

    fn inject_fault_selective(&self, faults: &[(&str, bool)]) {
        for file in self.files.borrow().iter() {
            for (stem, fault) in faults {
                if file.path.contains(stem) {
                    file.inject_fault(*fault);
                    break;
                }
            }
        }
    }

    fn print_stats(&self) {
        for file in self.files.borrow().iter() {
            if file.path.contains("ephemeral") {
                // Files created for ephemeral tables just add noise to the simulator output and aren't by default very interesting to debug
                continue;
            }
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

    fn persist_files(&self) -> anyhow::Result<()> {
        // Files are persisted automatically
        Ok(())
    }
}

impl Clock for SimulatorIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        MonotonicInstant::now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
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
        let file = Arc::new(SimulatorFile {
            path: path.to_string(),
            inner,
            fault: Cell::new(false),
            nr_pread_faults: Cell::new(0),
            nr_pwrite_faults: Cell::new(0),
            nr_sync_faults: Cell::new(0),
            nr_pread_calls: Cell::new(0),
            nr_pwrite_calls: Cell::new(0),
            nr_sync_calls: Cell::new(0),
            page_size: self.page_size,
            rng: RefCell::new(ChaCha8Rng::seed_from_u64(self.seed)),
            latency_probability: self.latency_probability,
            sync_completion: RefCell::new(None),
            queued_io: RefCell::new(Vec::new()),
            clock: self.clock.clone(),
        });
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.files.borrow_mut().retain(|x| x.path != path);
        Ok(())
    }

    fn step(&self) -> Result<()> {
        let now = self.current_time_wall_clock();
        for file in self.files.borrow().iter() {
            file.run_queued_io(now)?;
        }
        self.inner.step()?;
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().random()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.rng.borrow_mut().fill_bytes(dest);
    }
}
