#[cfg(shuttle)]
pub mod sync {
    pub use shuttle::sync::atomic;
    pub use shuttle::sync::{Arc, Weak};
    pub use std::sync::{LazyLock, OnceLock};
}

#[cfg(not(shuttle))]
pub mod sync {
    pub use std::sync::{atomic, Arc, LazyLock, OnceLock, Weak};
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
        current, panicking, park, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use std::thread_local;
}

#[cfg(shuttle)]
pub mod future {
    pub use shuttle::future::{spawn, JoinHandle};
}

#[cfg(not(shuttle))]
pub mod future {
    pub use tokio::{spawn, task::JoinHandle};
}

#[cfg(shuttle)]
pub fn shuttle_config() -> shuttle::Config {
    let mut config = shuttle::Config::default();
    config.stack_size *= 10;
    config.max_steps = shuttle::MaxSteps::FailAfter(10_000_000);
    config
}
