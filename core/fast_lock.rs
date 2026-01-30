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
        self.lock.locked.store(false, Ordering::SeqCst);
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

#[cfg(all(shuttle, test))]
mod shuttle_tests {
    use super::*;
    use crate::sync::*;
    use crate::thread;

    /// Test basic mutual exclusion with counter increment
    #[test]
    fn shuttle_spinlock_counter() {
        shuttle::check_random(
            || {
                let lock = Arc::new(SpinLock::new(0));
                let mut threads = vec![];
                const NTHREADS: usize = 3;
                for _ in 0..NTHREADS {
                    let lock = lock.clone();
                    threads.push(thread::spawn(move || {
                        let mut guard = lock.lock();
                        *guard += 1;
                    }));
                }
                for thread in threads {
                    thread.join().unwrap();
                }
                assert_eq!(*lock.lock(), NTHREADS);
            },
            1000,
        );
    }

    /// Test that lock provides mutual exclusion - no two threads hold lock simultaneously
    #[test]
    fn shuttle_spinlock_mutual_exclusion() {
        shuttle::check_random(
            || {
                let lock = Arc::new(SpinLock::new(()));
                let in_critical_section = Arc::new(AtomicBool::new(false));

                let mut threads = vec![];
                for _ in 0..3 {
                    let lock = lock.clone();
                    let in_cs = in_critical_section.clone();
                    threads.push(thread::spawn(move || {
                        let _guard = lock.lock();
                        // If another thread is in critical section, this is a bug
                        assert!(
                            !in_cs.swap(true, Ordering::SeqCst),
                            "Two threads in critical section!"
                        );
                        // Simulate some work
                        thread::yield_now();
                        in_cs.store(false, Ordering::SeqCst);
                    }));
                }
                for thread in threads {
                    thread.join().unwrap();
                }
            },
            1000,
        );
    }

    /// Test multiple lock/unlock cycles per thread
    #[test]
    fn shuttle_spinlock_multiple_cycles() {
        shuttle::check_random(
            || {
                let lock = Arc::new(SpinLock::new(0i32));

                let mut threads = vec![];
                for _ in 0..2 {
                    let lock = lock.clone();
                    threads.push(thread::spawn(move || {
                        for _ in 0..3 {
                            let mut guard = lock.lock();
                            *guard += 1;
                            // Guard dropped here, releasing lock
                        }
                    }));
                }
                for thread in threads {
                    thread.join().unwrap();
                }
                // 2 threads * 3 iterations = 6
                assert_eq!(*lock.lock(), 6);
            },
            1000,
        );
    }

    /// Test that guard properly releases lock on drop
    #[test]
    fn shuttle_spinlock_guard_drop() {
        shuttle::check_random(
            || {
                let lock = Arc::new(SpinLock::new(0));

                let lock1 = lock.clone();
                let t1 = thread::spawn(move || {
                    {
                        let mut guard = lock1.lock();
                        *guard = 1;
                        // guard dropped here
                    }
                    // After drop, another thread should be able to acquire
                });

                let lock2 = lock.clone();
                let t2 = thread::spawn(move || {
                    let mut guard = lock2.lock();
                    *guard = 2;
                });

                t1.join().unwrap();
                t2.join().unwrap();

                // Value should be 1 or 2 depending on order, but lock should be acquirable
                let val = *lock.lock();
                assert!(val == 1 || val == 2);
            },
            1000,
        );
    }

    /// Test read-modify-write pattern under contention
    #[test]
    fn shuttle_spinlock_read_modify_write() {
        shuttle::check_random(
            || {
                let lock = Arc::new(SpinLock::new(vec![0i32; 3]));

                let mut threads = vec![];
                for i in 0..3 {
                    let lock = lock.clone();
                    threads.push(thread::spawn(move || {
                        let mut guard = lock.lock();
                        // Read current value, modify, write back
                        guard[i] += 1;
                    }));
                }
                for thread in threads {
                    thread.join().unwrap();
                }
                let guard = lock.lock();
                assert_eq!(*guard, vec![1, 1, 1]);
            },
            1000,
        );
    }

    /// Test lock acquisition order doesn't cause starvation (probabilistic)
    #[test]
    fn shuttle_spinlock_no_starvation() {
        shuttle::check_random(
            || {
                let lock = Arc::new(SpinLock::new(Vec::<usize>::new()));

                let mut threads = vec![];
                for id in 0..3 {
                    let lock = lock.clone();
                    threads.push(thread::spawn(move || {
                        let mut guard = lock.lock();
                        guard.push(id);
                    }));
                }
                for thread in threads {
                    thread.join().unwrap();
                }
                // All threads should have acquired the lock exactly once
                let guard = lock.lock();
                assert_eq!(guard.len(), 3);
                let mut sorted = guard.clone();
                sorted.sort();
                assert_eq!(sorted, vec![0, 1, 2]);
            },
            1000,
        );
    }

    /// Test nested-style access pattern (reacquire after release)
    #[test]
    fn shuttle_spinlock_reacquire() {
        shuttle::check_random(
            || {
                let lock = Arc::new(SpinLock::new(0));

                let lock1 = lock.clone();
                let t1 = thread::spawn(move || {
                    {
                        let mut guard = lock1.lock();
                        *guard += 1;
                    }
                    // Release and reacquire
                    {
                        let mut guard = lock1.lock();
                        *guard += 1;
                    }
                });

                let lock2 = lock.clone();
                let t2 = thread::spawn(move || {
                    let mut guard = lock2.lock();
                    *guard += 10;
                });

                t1.join().unwrap();
                t2.join().unwrap();

                // Should be 12 (1 + 1 + 10)
                assert_eq!(*lock.lock(), 12);
            },
            1000,
        );
    }
}
