use std::{
    fmt,
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

use crate::rsapi::TursoError;

pub mod capi;
pub mod rsapi;

#[macro_export]
macro_rules! assert_send {
    ($($ty:ty),+ $(,)?) => {
        const _: fn() = || {
            fn check<T: Send>() {}
            $( check::<$ty>(); )+
        };
    };
}

#[macro_export]
macro_rules! assert_sync {
    ($($ty:ty),+ $(,)?) => {
        const _: fn() = || {
            fn check<T: Sync>() {}
            $( check::<$ty>(); )+
        };
    };
}

#[derive(Clone, Default)]
pub enum IoBackend {
    #[default]
    Default,
    /// In-memory backend
    Memory,
    /// Generic syscall backend
    Syscall,
    /// IO uring (supported only on Linux)
    IoUring,
    /// Experimental iocp (supported only on windows)
    IOCP,
    Other(String),
}

impl fmt::Display for IoBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => write!(f, "memory"),
            Self::Syscall => write!(f, "syscall"),
            Self::IoUring => write!(f, "io_uring"),
            Self::IOCP => write!(f, "experimental_win_iocp"),
            Self::Other(other) => write!(f, "{other}"),
            Self::Default => write!(f, "default"),
        }
    }
}

impl<T: AsRef<str>> From<T> for IoBackend {
    fn from(vfs: T) -> Self {
        match vfs.as_ref() {
            "memory" => IoBackend::Memory,
            "syscall" => IoBackend::Syscall,
            "io_uring" => IoBackend::IoUring,
            "experimental_win_iocp" => IoBackend::IOCP,
            vfs => IoBackend::Other(vfs.to_string()),
        }
    }
}

/// simple helper which return MISUSE error in case of concurrent access to some operation
struct ConcurrentGuard {
    in_use: AtomicU32,
    /// Statements that were dropped while another call held the guard. Dropping a
    /// stepped statement resets it, and the reset can roll back the transaction of the
    /// connection, so the holder of the guard drops them when it releases the guard.
    deferred_drops: Mutex<Vec<turso_core::Statement>>,
}

struct ConcurrentGuardToken<'a> {
    guard: &'a ConcurrentGuard,
}

impl ConcurrentGuard {
    pub fn new() -> Self {
        Self {
            in_use: AtomicU32::new(0),
            deferred_drops: Mutex::new(Vec::new()),
        }
    }
    pub fn try_use(&self) -> Result<ConcurrentGuardToken<'_>, TursoError> {
        if self
            .in_use
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(TursoError::Misuse("concurrent use forbidden".to_string()));
        };
        Ok(ConcurrentGuardToken { guard: self })
    }
    /// Drop the statement now if the guard is free, or else when the call that holds
    /// the guard releases it. This does not wait, because the holder can need something
    /// that the dropping thread holds, for example the Python GIL.
    pub fn drop_statement(&self, statement: turso_core::Statement) {
        self.deferred_drops.lock().unwrap().push(statement);
        // If the guard is free, its token drops the statement. If another call holds
        // the guard, that call sees the statement when it releases the guard.
        let _ = self.try_use();
    }
}

impl<'a> Drop for ConcurrentGuardToken<'a> {
    fn drop(&mut self) {
        let deferred = std::mem::take(&mut *self.guard.deferred_drops.lock().unwrap());
        drop(deferred);
        let before = self.guard.in_use.swap(0, Ordering::SeqCst);
        assert!(
            before == 1,
            "invalid db state: guard wasn't in use while token is active"
        );
        // A statement added after the take above is dropped by the next holder of the
        // guard. Take the guard here, in case no other call takes it.
        if !self.guard.deferred_drops.lock().unwrap().is_empty() {
            let _ = self.guard.try_use();
        }
    }
}
