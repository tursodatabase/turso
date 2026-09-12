use crate::sync::{atomic::AtomicU64, RwLock};
use std::sync::atomic::Ordering;

pub(crate) type ProgressHandlerCallback = Box<dyn Fn() -> bool + Send + Sync>;

/// Connection-scoped progress callback state.
///
/// This models SQLite's `sqlite3_progress_handler()` contract for step-time
/// execution:
/// - one handler per connection
/// - the callback runs approximately every `N` virtual machine instructions
/// - a non-zero callback result interrupts the running operation
#[derive(Default)]
pub(crate) struct ProgressHandler {
    callback: RwLock<Option<ProgressHandlerCallback>>,
    ops: AtomicU64,
}

impl std::fmt::Debug for ProgressHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressHandler")
            .field("enabled", &self.is_enabled())
            .field("ops", &self.ops())
            .finish()
    }
}

impl ProgressHandler {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Install or clear the progress handler.
    ///
    /// SQLite disables the handler when `N < 1` or the callback is null, so we
    /// do the same here by clearing both the callback and opcode interval.
    pub(crate) fn set(&self, ops: u64, callback: Option<ProgressHandlerCallback>) {
        if ops == 0 || callback.is_none() {
            *self.callback.write() = None;
            self.ops.store(0, Ordering::SeqCst);
            return;
        }
        *self.callback.write() = callback;
        self.ops.store(ops, Ordering::SeqCst);
    }

    pub(crate) fn ops(&self) -> u64 {
        self.ops.load(Ordering::SeqCst)
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.ops() != 0
    }

    /// Returns true when the callback requests interruption for the VM steps
    /// executed in `(prev_steps, vm_steps]`.
    ///
    /// The cadence is approximate by design, matching SQLite's documentation:
    /// the callback is consulted only when the VM-step count crosses a
    /// configured multiple of `ops`. Callers that consult this once per step
    /// pass `prev_steps = vm_steps - 1`; callers that batch several steps per
    /// consultation pass the count from the previous consultation.
    pub(crate) fn should_interrupt(&self, prev_steps: u64, vm_steps: u64) -> bool {
        let ops = self.ops();
        if ops == 0 || prev_steps / ops == vm_steps / ops {
            return false;
        }
        let callback = self.callback.read();
        match callback.as_ref() {
            Some(callback) => callback(),
            None => false,
        }
    }
}

#[cfg(test)]
#[path = "tests/unit/progress/tests.rs"]
mod tests;
