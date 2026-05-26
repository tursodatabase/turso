use std::sync::atomic::{AtomicU32, Ordering};

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

assert_send!(ConcurrentGuard);
assert_sync!(ConcurrentGuard);

/// simple helper which return MISUSE error in case of concurrent access to some operation
struct ConcurrentGuard {
    in_use: AtomicU32,
}

struct ConcurrentGuardToken<'a> {
    guard: &'a ConcurrentGuard,
}

impl ConcurrentGuard {
    pub fn new() -> Self {
        Self {
            in_use: AtomicU32::new(0),
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
}

impl<'a> Drop for ConcurrentGuardToken<'a> {
    fn drop(&mut self) {
        let before = self.guard.in_use.swap(0, Ordering::SeqCst);
        assert!(
            before == 1,
            "invalid db state: guard wasn't in use while token is active"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_guard_allows_single_use() {
        let guard = ConcurrentGuard::new();
        assert!(guard.try_use().is_ok());
    }

    #[test]
    fn concurrent_guard_rejects_double_use() {
        let guard = ConcurrentGuard::new();
        let _token = guard.try_use().unwrap();
        assert!(guard.try_use().is_err());
    }

    #[test]
    fn concurrent_guard_allows_reuse_after_drop() {
        let guard = ConcurrentGuard::new();
        {
            let token = guard.try_use().unwrap();
            drop(token);
        }
        assert!(guard.try_use().is_ok());
    }
}
