//! Blocking/async helpers over `IOResult`-returning state machines.

use crate::{IOResult, IO};
use turso_core_common::Result;

use std::future::Future;

#[macro_export]
macro_rules! io_yield_one {
    ($c:expr) => {
        return Ok(IOResult::IO(IOCompletions($c)));
    };
}

pub trait IOExt {
    fn block<T>(&self, f: impl FnMut() -> Result<IOResult<T>>) -> Result<T>;
    fn wait<T, F>(&self, f: F) -> impl Future<Output = Result<T>> + Send
    where
        F: FnMut() -> Result<IOResult<T>> + Send,
        T: Send;
}

impl<I: ?Sized + IO> IOExt for I {
    fn block<T>(&self, mut f: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
        Ok(loop {
            match f()? {
                IOResult::Done(v) => break v,
                IOResult::IO(io) => io.wait(self)?,
            }
        })
    }

    async fn wait<T, F>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> Result<IOResult<T>> + Send,
        T: Send,
    {
        Ok(loop {
            match f()? {
                IOResult::Done(v) => break v,
                IOResult::IO(io) => io.wait_async(self).await?,
            }
        })
    }
}
