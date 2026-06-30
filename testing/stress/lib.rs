use std::fmt::{Display, Formatter};

#[cfg(shuttle)]
pub mod sync {
    pub use super::async_barrier::AsyncBarrier;
    pub use super::async_mutex::{AsyncMutex, AsyncMutexGuard};
    pub use shuttle::sync::atomic;
    pub use shuttle::sync::{Arc, Mutex, Weak};
    pub use std::sync::Mutex as StdMutex;
}

#[cfg(not(shuttle))]
pub mod sync {
    pub use super::async_barrier::AsyncBarrier;
    pub use super::async_mutex::{AsyncMutex, AsyncMutexGuard};
    pub use std::sync::{atomic, Arc, Mutex, Mutex as StdMutex, Weak};
}

#[cfg(shuttle)]
mod async_mutex {
    use shuttle::sync::Mutex;
    pub use shuttle::sync::MutexGuard as AsyncMutexGuard;

    pub struct AsyncMutex<T: ?Sized> {
        inner: Mutex<T>,
    }

    impl<T> AsyncMutex<T> {
        pub fn new(value: T) -> Self {
            Self {
                inner: Mutex::new(value),
            }
        }
    }

    impl<T: ?Sized> AsyncMutex<T> {
        pub async fn lock(&self) -> AsyncMutexGuard<'_, T> {
            self.inner.lock().unwrap()
        }
    }
}

#[cfg(not(shuttle))]
mod async_mutex {
    use tokio::sync::Mutex;
    pub use tokio::sync::MutexGuard as AsyncMutexGuard;

    pub struct AsyncMutex<T: ?Sized> {
        inner: Mutex<T>,
    }

    impl<T> AsyncMutex<T> {
        pub fn new(value: T) -> Self {
            Self {
                inner: Mutex::new(value),
            }
        }
    }

    impl<T: ?Sized> AsyncMutex<T> {
        pub async fn lock(&self) -> AsyncMutexGuard<'_, T> {
            self.inner.lock().await
        }
    }
}

#[cfg(shuttle)]
mod async_barrier {
    use shuttle::sync::Barrier;

    pub struct AsyncBarrier {
        inner: Barrier,
    }

    impl AsyncBarrier {
        pub fn new(n: usize) -> Self {
            Self {
                inner: Barrier::new(n),
            }
        }

        pub async fn wait(&self) {
            self.inner.wait();
        }
    }
}

#[cfg(not(shuttle))]
mod async_barrier {
    use tokio::sync::Barrier;

    pub struct AsyncBarrier {
        inner: Barrier,
    }

    impl AsyncBarrier {
        pub fn new(n: usize) -> Self {
            Self {
                inner: Barrier::new(n),
            }
        }

        pub async fn wait(&self) {
            self.inner.wait().await;
        }
    }
}

#[cfg(shuttle)]
pub mod thread {
    pub use shuttle::hint::spin_loop;
    pub use shuttle::thread::{
        current, panicking, park, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use shuttle::thread_local;
}

#[cfg(not(shuttle))]
pub mod thread {
    pub use std::hint::spin_loop;
    pub use std::thread::{
        current, panicking, park, scope, spawn, Builder, JoinHandle, Scope, ScopedJoinHandle,
        Thread, ThreadId,
    };
    pub use std::thread_local;
    pub use tokio::task::yield_now;
    pub use tokio::time::sleep;
}

#[cfg(shuttle)]
pub mod future {
    pub use shuttle::future::{spawn_local as spawn, yield_now, JoinHandle};
}

#[cfg(not(shuttle))]
pub mod future {
    pub use tokio::task::yield_now;
    pub use tokio::{spawn, task::JoinHandle};
}

#[cfg(shuttle)]
pub fn shuttle_config() -> shuttle::Config {
    let mut config = shuttle::Config::default();
    config.stack_size *= 10;
    config.max_steps = shuttle::MaxSteps::FailAfter(10_000_000);
    config
}

#[derive(Debug, Clone)]
pub struct ThreadId(String);

impl ThreadId {
    pub fn new(thread_id: usize) -> Self {
        Self(format!("Thread {thread_id}"))
    }
}

impl Display for ThreadId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
