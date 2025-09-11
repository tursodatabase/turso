use std::{
    cell::{Cell, RefCell},
    fmt::Debug,
    sync::Arc,
};

use rand::Rng as _;
use rand_chacha::ChaCha8Rng;
use tracing::{Level, instrument};
use turso_core::{File, Result};

use crate::profiles::io::ShortWriteProfile;
use crate::runner::{FAULT_ERROR_MSG, clock::SimulatorClock};
pub(crate) struct SimulatorFile {
    pub path: String,
    pub(crate) inner: Arc<dyn File>,
    pub(crate) fault: Cell<bool>,

    /// Number of `pread` function calls (both success and failures).
    pub(crate) nr_pread_calls: Cell<usize>,

    /// Number of `pread` function calls with injected fault.
    pub(crate) nr_pread_faults: Cell<usize>,

    /// Number of `pwrite` function calls (both success and failures).
    pub(crate) nr_pwrite_calls: Cell<usize>,

    /// Number of `pwrite` function calls with injected fault.
    pub(crate) nr_pwrite_faults: Cell<usize>,

    /// Number of `sync` function calls (both success and failures).
    pub(crate) nr_sync_calls: Cell<usize>,

    /// Number of `sync` function calls with injected fault.
    pub(crate) nr_sync_faults: Cell<usize>,

    pub(crate) page_size: usize,

    pub(crate) rng: RefCell<ChaCha8Rng>,

    pub latency_probability: usize,

    pub sync_completion: RefCell<Option<turso_core::Completion>>,
    pub queued_io: RefCell<Vec<DelayedIo>>,
    pub clock: Arc<SimulatorClock>,
    pub short_write_profile: ShortWriteProfile,
}

type IoOperation = Box<dyn FnOnce(&SimulatorFile) -> Result<turso_core::Completion>>;

pub struct DelayedIo {
    pub time: turso_core::Instant,
    pub op: IoOperation,
}

impl Debug for DelayedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelayedIo")
            .field("time", &self.time)
            .finish()
    }
}

unsafe impl Send for SimulatorFile {}
unsafe impl Sync for SimulatorFile {}

impl SimulatorFile {
    pub(crate) fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }

    /// Check if this file should be subject to short write faults
    fn should_apply_short_write(&self) -> bool {
        if !self.short_write_profile.enable {
            return false;
        }

        if self.short_write_profile.wal_only && !self.path.ends_with("-wal") {
            return false;
        }

        let mut rng = self.rng.borrow_mut();
        rng.random_range(0..100) < self.short_write_profile.probability
    }

    /// Calculate short write length for a given buffer size
    fn calculate_short_write_length(&self, full_length: usize) -> usize {
        let mut rng = self.rng.borrow_mut();
        let bytes_to_subtract =
            rng.random_range(1..=self.short_write_profile.max_bytes_short.min(full_length));
        let short_length = full_length.saturating_sub(bytes_to_subtract);
        short_length.max(self.short_write_profile.min_bytes)
    }

    pub(crate) fn stats_table(&self) -> String {
        let sum_calls =
            self.nr_pread_calls.get() + self.nr_pwrite_calls.get() + self.nr_sync_calls.get();
        let sum_faults = self.nr_pread_faults.get() + self.nr_pwrite_faults.get();
        let stats_table = [
            "op           calls   faults".to_string(),
            "--------- -------- --------".to_string(),
            format!(
                "pread     {:8} {:8}",
                self.nr_pread_calls.get(),
                self.nr_pread_faults.get()
            ),
            format!(
                "pwrite    {:8} {:8}",
                self.nr_pwrite_calls.get(),
                self.nr_pwrite_faults.get()
            ),
            format!(
                "sync      {:8} {:8}",
                self.nr_sync_calls.get(),
                0 // No fault counter for sync
            ),
            "--------- -------- --------".to_string(),
            format!("total     {sum_calls:8} {sum_faults:8}"),
        ];

        stats_table.join("\n")
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn generate_latency_duration(&self) -> Option<turso_core::Instant> {
        let mut rng = self.rng.borrow_mut();
        // Chance to introduce some latency
        rng.random_bool(self.latency_probability as f64 / 100.0)
            .then(|| {
                let now = self.clock.now();
                let sum = now + std::time::Duration::from_millis(rng.random_range(5..20));
                sum.into()
            })
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn run_queued_io(&self, now: turso_core::Instant) -> Result<()> {
        let mut queued_io = self.queued_io.borrow_mut();
        for io in queued_io.extract_if(.., |item| item.time <= now) {
            let _c = (io.op)(self)?;
        }
        Ok(())
    }
}

impl File for SimulatorFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: turso_core::Completion) -> Result<turso_core::Completion> {
        self.nr_pread_calls.set(self.nr_pread_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pread fault");
            self.nr_pread_faults.set(self.nr_pread_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(move |file: &SimulatorFile| file.inner.pread(pos, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            Ok(c)
        } else {
            self.inner.pread(pos, c)
        }
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: turso_core::Completion,
    ) -> Result<turso_core::Completion> {
        self.nr_pwrite_calls.set(self.nr_pwrite_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pwrite fault");
            self.nr_pwrite_faults.set(self.nr_pwrite_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }

        if self.should_apply_short_write() {
            let full_length = buffer.len();
            let short_length = self.calculate_short_write_length(full_length);

            if short_length < full_length {
                tracing::debug!(
                    "Injecting short write: {} bytes instead of {} bytes",
                    short_length,
                    full_length
                );

                let short_buffer = turso_core::Buffer::new_temporary(short_length);
                short_buffer
                    .as_mut_slice()
                    .copy_from_slice(&buffer.as_slice()[..short_length]);

                let short_buffer_arc = Arc::new(short_buffer);

                if let Some(latency) = self.generate_latency_duration() {
                    let cloned_c = c.clone();
                    let op = Box::new(move |file: &SimulatorFile| {
                        let result = file.inner.pwrite(pos, short_buffer_arc, cloned_c);
                        if let Ok(completion) = &result {
                            completion.complete(short_length as i32);
                        }
                        result
                    });
                    self.queued_io
                        .borrow_mut()
                        .push(DelayedIo { time: latency, op });
                    Ok(c)
                } else {
                    // Simulate short write by completing directly without calling underlying I/O
                    c.complete(short_length as i32);
                    Ok(c)
                }
            } else if let Some(latency) = self.generate_latency_duration() {
                let cloned_c = c.clone();
                let op =
                    Box::new(move |file: &SimulatorFile| file.inner.pwrite(pos, buffer, cloned_c));
                self.queued_io
                    .borrow_mut()
                    .push(DelayedIo { time: latency, op });
                Ok(c)
            } else {
                self.inner.pwrite(pos, buffer, c)
            }
        } else if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(move |file: &SimulatorFile| file.inner.pwrite(pos, buffer, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            Ok(c)
        } else {
            self.inner.pwrite(pos, buffer, c)
        }
    }

    fn sync(&self, c: turso_core::Completion) -> Result<turso_core::Completion> {
        self.nr_sync_calls.set(self.nr_sync_calls.get() + 1);
        if self.fault.get() {
            // TODO: Enable this when https://github.com/tursodatabase/turso/issues/2091 is fixed.
            tracing::debug!(
                "ignoring sync fault because it causes false positives with current simulator design"
            );
            self.fault.set(false);
        }
        let c = if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(|file: &SimulatorFile| -> Result<_> {
                let c = file.inner.sync(cloned_c)?;
                *file.sync_completion.borrow_mut() = Some(c.clone());
                Ok(c)
            });
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            c
        } else {
            let c = self.inner.sync(c)?;
            *self.sync_completion.borrow_mut() = Some(c.clone());
            c
        };
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<turso_core::Buffer>>,
        c: turso_core::Completion,
    ) -> Result<turso_core::Completion> {
        self.nr_pwrite_calls.set(self.nr_pwrite_calls.get() + 1);
        if self.fault.get() {
            tracing::debug!("pwritev fault");
            self.nr_pwrite_faults.set(self.nr_pwrite_faults.get() + 1);
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }

        if self.should_apply_short_write() {
            let total_length: usize = buffers.iter().map(|b| b.len()).sum();
            let short_length = self.calculate_short_write_length(total_length);

            if short_length < total_length {
                tracing::debug!(
                    "Injecting short vectored write: {} bytes instead of {} bytes",
                    short_length,
                    total_length
                );

                let mut remaining = short_length;
                let mut short_buffers = Vec::new();

                for buffer in buffers.iter() {
                    if remaining == 0 {
                        break;
                    }

                    let take_len = buffer.len().min(remaining);
                    let short_buffer = turso_core::Buffer::new_temporary(take_len);
                    short_buffer
                        .as_mut_slice()
                        .copy_from_slice(&buffer.as_slice()[..take_len]);
                    short_buffers.push(Arc::new(short_buffer));
                    remaining -= take_len;
                }

                if let Some(latency) = self.generate_latency_duration() {
                    let cloned_c = c.clone();
                    let op = Box::new(move |file: &SimulatorFile| {
                        let result = file.inner.pwritev(pos, short_buffers, cloned_c);
                        if let Ok(completion) = &result {
                            completion.complete(short_length as i32);
                        }
                        result
                    });
                    self.queued_io
                        .borrow_mut()
                        .push(DelayedIo { time: latency, op });
                    Ok(c)
                } else {
                    // Simulate short write by completing directly without calling underlying I/O
                    c.complete(short_length as i32);
                    Ok(c)
                }
            } else if let Some(latency) = self.generate_latency_duration() {
                let cloned_c = c.clone();
                let op = Box::new(move |file: &SimulatorFile| {
                    file.inner.pwritev(pos, buffers, cloned_c)
                });
                self.queued_io
                    .borrow_mut()
                    .push(DelayedIo { time: latency, op });
                Ok(c)
            } else {
                let c = self.inner.pwritev(pos, buffers, c)?;
                Ok(c)
            }
        } else if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op =
                Box::new(move |file: &SimulatorFile| file.inner.pwritev(pos, buffers, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            Ok(c)
        } else {
            let c = self.inner.pwritev(pos, buffers, c)?;
            Ok(c)
        }
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: turso_core::Completion) -> Result<turso_core::Completion> {
        if self.fault.get() {
            return Err(turso_core::LimboError::InternalError(
                FAULT_ERROR_MSG.into(),
            ));
        }
        let c = if let Some(latency) = self.generate_latency_duration() {
            let cloned_c = c.clone();
            let op = Box::new(move |file: &SimulatorFile| file.inner.truncate(len, cloned_c));
            self.queued_io
                .borrow_mut()
                .push(DelayedIo { time: latency, op });
            c
        } else {
            self.inner.truncate(len, c)?
        };
        Ok(c)
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {
        self.inner.unlock_file().expect("Failed to unlock file");
    }
}

struct Latency {}
