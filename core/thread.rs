#[cfg(shuttle)]
pub(crate) use shuttle_adapter::*;

#[cfg(not(shuttle))]
pub(crate) use std_adapter::*;

#[expect(unused_imports)]
#[cfg(shuttle)]
mod shuttle_adapter {
    pub use shuttle::hint::spin_loop;
    pub use shuttle::thread::{
        current, panicking, park, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use shuttle::thread_local;
}

#[expect(unused_imports)]
#[cfg(not(shuttle))]
mod std_adapter {
    pub use std::hint::spin_loop;
    pub use std::thread::{
        current, panicking, park, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use std::thread_local;
}
