use crate::sync::atomic::{AtomicBool, Ordering};
use crate::thread::spin_loop;
use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
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

    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        while self.locked.swap(true, Ordering::Acquire) {
            spin_loop();
        }
        SpinLockGuard { lock: self }
    }

    pub fn into_inner(self) -> UnsafeCell<T> {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::Arc;

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
