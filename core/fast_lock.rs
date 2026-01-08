use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug)]
pub struct SpinLock<T: ?Sized> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

pub struct SpinLockGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.value.get() }
    }
}

unsafe impl<T: ?Sized + Send> Send for SpinLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    #[inline(always)]
    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        if self
            .locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            SpinLockGuard { lock: self }
        } else {
            self.lock_slow()
        }
    }

    #[inline(never)]
    #[cold]
    pub fn lock_slow(&self) -> SpinLockGuard<'_, T> {
        while self
            .locked
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        SpinLockGuard { lock: self }
    }

    pub fn into_inner(self) -> UnsafeCell<T> {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::SpinLock;

    #[test]
    fn test_fast_lock_multiple_thread_sum() {
        let lock = Arc::new(SpinLock::new(0));
        let mut threads = vec![];
        const NTHREADS: usize = 1000;
        for _ in 0..NTHREADS {
            let lock = lock.clone();
            threads.push(std::thread::spawn(move || {
                let mut guard = lock.lock();
                *guard += 1;
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(*lock.lock(), NTHREADS);
    }
}
