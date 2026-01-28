#[cfg(shuttle)]
pub(crate) use shuttle_adapter::*;

#[cfg(not(shuttle))]
pub(crate) use std_adapter::*;

#[cfg(shuttle)]
mod shuttle_adapter {
    pub use shuttle::sync::atomic;
    pub use shuttle::sync::{Arc, Weak};
    pub use std::sync::{LazyLock, OnceLock};

    use std::fmt::{self, Debug};
    use std::ops::{Deref, DerefMut};

    pub struct Mutex<T: ?Sized>(shuttle::sync::Mutex<T>);

    impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let mut d = f.debug_struct("Mutex");
            match self.try_lock() {
                Some(guard) => d.field("data", &&*guard),
                None => d.field("data", &format_args!("<locked>")),
            };
            d.finish()
        }
    }

    impl<T> Mutex<T> {
        pub fn new(val: T) -> Self {
            Self(shuttle::sync::Mutex::new(val))
        }
    }

    impl<T: ?Sized> Mutex<T> {
        #[allow(dead_code)]
        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.lock().unwrap())
        }

        pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
            self.0.try_lock().ok().map(MutexGuard)
        }

        /// Lock the mutex through an Arc, returning an owned guard that can be stored
        pub fn lock_arc(self: &Arc<Self>) -> ArcMutexGuard<T>
        where
            T: 'static,
        {
            // We need to lock the mutex and keep the Arc alive
            // Safety: We hold the Arc which keeps the Mutex alive
            let arc = Arc::clone(self);
            let guard = arc.0.lock().unwrap();
            // Transmute the guard to have 'static lifetime since Arc keeps it alive
            let guard: shuttle::sync::MutexGuard<'static, T> =
                unsafe { std::mem::transmute(guard) };
            ArcMutexGuard { _arc: arc, guard }
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    pub struct MutexGuard<'a, T: ?Sized>(shuttle::sync::MutexGuard<'a, T>);

    impl<T: ?Sized> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    /// An owned mutex guard that holds an Arc to the Mutex.
    /// This allows the guard to be stored across async yield points.
    pub struct ArcMutexGuard<T: ?Sized + 'static> {
        _arc: Arc<Mutex<T>>,
        guard: shuttle::sync::MutexGuard<'static, T>,
    }

    impl<T: ?Sized + 'static> Deref for ArcMutexGuard<T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.guard
        }
    }

    impl<T: ?Sized + 'static> DerefMut for ArcMutexGuard<T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.guard
        }
    }

    pub struct RwLock<T: ?Sized>(shuttle::sync::RwLock<T>);

    impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let mut d = f.debug_struct("RwLock");
            match self.try_read() {
                Some(guard) => d.field("data", &&*guard),
                None => d.field("data", &format_args!("<locked>")),
            };
            d.finish()
        }
    }

    impl<T> RwLock<T> {
        pub fn new(val: T) -> Self {
            Self(shuttle::sync::RwLock::new(val))
        }

        pub fn into_inner(self) -> T {
            self.0.into_inner().unwrap()
        }
    }

    impl<T: ?Sized> RwLock<T> {
        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            RwLockReadGuard(self.0.read().unwrap())
        }

        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            RwLockWriteGuard(self.0.write().unwrap())
        }

        pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
            self.0.try_read().ok().map(RwLockReadGuard)
        }

        pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
            self.0.try_write().ok().map(RwLockWriteGuard)
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap()
        }
    }

    impl<T: Default> Default for RwLock<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    pub struct RwLockReadGuard<'a, T: ?Sized>(shuttle::sync::RwLockReadGuard<'a, T>);

    impl<T: Debug> Debug for RwLockReadGuard<'_, T> {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            Debug::fmt(&self.0, f)
        }
    }

    impl<'a, T: fmt::Display + ?Sized + 'a> fmt::Display for RwLockReadGuard<'a, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            (**self).fmt(f)
        }
    }

    impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    pub struct RwLockWriteGuard<'a, T: ?Sized>(shuttle::sync::RwLockWriteGuard<'a, T>);

    impl<T: Debug> Debug for RwLockWriteGuard<'_, T> {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            Debug::fmt(&self.0, f)
        }
    }

    impl<'a, T: fmt::Display + ?Sized + 'a> fmt::Display for RwLockWriteGuard<'a, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            (**self).fmt(f)
        }
    }

    impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }
}

#[cfg(not(shuttle))]
mod std_adapter {
    pub use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
    pub use std::sync::{atomic, Arc, LazyLock, OnceLock, Weak};

    /// Type alias for ArcMutexGuard that hides the RawMutex type parameter
    pub type ArcMutexGuard<T> = parking_lot::ArcMutexGuard<parking_lot::RawMutex, T>;
}
