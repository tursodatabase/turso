use super::{Backoff, LOCK_BIT, LockGuard, NodeVersion, StdThread};
use core::marker::PhantomData;
use core::ptr as StdPtr;
use core::sync::atomic::Ordering;

const MAX_SPINS: u32 = 16;

impl NodeVersion {
    /// Acquire the lock with bounded spinning before yielding.
    #[must_use = "releasing a lock without using the guard is a logic error"]
    pub fn lock_bounded(&self) -> LockGuard<'_> {
        let value: u32 = self.value.load(Ordering::Relaxed);

        if (value & LOCK_BIT) == 0 {
            let locked: u32 = value | LOCK_BIT;

            if self
                .value
                .compare_exchange_weak(value, locked, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return LockGuard {
                    version: StdPtr::from_ref(self),
                    locked_value: locked,
                    _lifetime: PhantomData,
                    _marker: PhantomData,
                };
            }
        }

        self.lock_bounded_contended()
    }

    #[cold]
    #[inline(never)]
    fn lock_bounded_contended(&self) -> LockGuard<'_> {
        let mut backoff = Backoff::new();
        let mut spin_count: u32 = 0;
        let mut value: u32 = self.value.load(Ordering::Relaxed);

        loop {
            if (value & LOCK_BIT) == 0 {
                let locked: u32 = value | LOCK_BIT;

                match self.value.compare_exchange_weak(
                    value,
                    locked,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        return LockGuard {
                            version: StdPtr::from_ref(self),
                            locked_value: locked,
                            _lifetime: PhantomData,
                            _marker: PhantomData,
                        };
                    }

                    Err(v) => {
                        value = v;
                        continue;
                    }
                }
            }

            spin_count += 1;

            if spin_count < MAX_SPINS {
                backoff.spin();
            } else {
                StdThread::yield_now();
                spin_count = 0;
                backoff = Backoff::new();
            }

            value = self.value.load(Ordering::Relaxed);
        }
    }
}
