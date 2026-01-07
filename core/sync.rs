#[cfg(loom)]
pub(crate) use loom_adapter::*;

#[cfg(not(loom))]
pub(crate) use std_adapter::*;

#[cfg(loom)]
mod loom_adapter {
    pub use loom::sync::atomic;
    // Use std::sync::Arc/Weak since loom doesn't support Arc::downgrade/Weak
    pub use std::sync::{Arc, Weak};

    use std::ops::{Deref, DerefMut};

    #[derive(Debug)]
    pub struct Mutex<T>(loom::sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub fn new(val: T) -> Self {
            Self(loom::sync::Mutex::new(val))
        }

        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.lock().unwrap())
        }

        pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
            self.0.try_lock().ok().map(MutexGuard)
        }

        pub fn into_inner(self) -> T {
            self.0.into_inner().unwrap()
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap()
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    pub struct MutexGuard<'a, T>(loom::sync::MutexGuard<'a, T>);

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    #[derive(Debug)]
    pub struct RwLock<T>(loom::sync::RwLock<T>);

    impl<T> RwLock<T> {
        pub fn new(val: T) -> Self {
            Self(loom::sync::RwLock::new(val))
        }

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

        pub fn into_inner(self) -> T {
            self.0.into_inner().unwrap()
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

    pub struct RwLockReadGuard<'a, T>(loom::sync::RwLockReadGuard<'a, T>);

    impl<T> Deref for RwLockReadGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    pub struct RwLockWriteGuard<'a, T>(loom::sync::RwLockWriteGuard<'a, T>);

    impl<T> Deref for RwLockWriteGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T> DerefMut for RwLockWriteGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }
}

#[cfg(not(loom))]
mod std_adapter {
    pub use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
    pub use std::sync::{atomic, Arc, LazyLock, OnceLock, Weak};
}
