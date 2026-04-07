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

    /// Returns true when the callback requests interruption at this VM step.
    ///
    /// The cadence is approximate by design, matching SQLite's documentation:
    /// the callback is consulted only when the current VM-step count crosses a
    /// configured multiple of `ops`.
    pub(crate) fn should_interrupt(&self, vm_steps: u64) -> bool {
        let ops = self.ops();
        if ops == 0 || vm_steps % ops != 0 {
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
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn disabled_handler_never_interrupts() {
        let handler = ProgressHandler::new();
        assert!(!handler.should_interrupt(1));
        assert!(!handler.is_enabled());
    }

    #[test]
    fn handler_runs_only_on_configured_interval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = ProgressHandler::new();
        let callback_calls = Arc::clone(&calls);
        handler.set(
            3,
            Some(Box::new(move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                false
            })),
        );

        assert!(!handler.should_interrupt(1));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!handler.should_interrupt(2));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!handler.should_interrupt(3));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!handler.should_interrupt(4));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!handler.should_interrupt(6));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn handler_can_request_interrupt() {
        let handler = ProgressHandler::new();
        handler.set(2, Some(Box::new(|| true)));

        assert!(!handler.should_interrupt(1));
        assert!(handler.should_interrupt(2));
    }

    #[test]
    fn disabling_clears_handler() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = ProgressHandler::new();
        let callback_calls = Arc::clone(&calls);
        handler.set(
            1,
            Some(Box::new(move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                true
            })),
        );
        assert!(handler.should_interrupt(1));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        handler.set(0, None);
        assert!(!handler.should_interrupt(2));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
