//! `IOResult` and `IOCompletions`: the return-value plumbing every
//! re-entrant state machine in the engine uses to signal "I need I/O".

use crate::{Completion, IO};
use turso_core_common::{CompletionError, Result};

use std::future::Future;
use std::task::{Poll, Waker};

#[derive(Debug)]
#[must_use]
pub struct IOCompletions(pub Completion);

pub struct IOCompletionAsync<'a, I: ?Sized + IO> {
    io: &'a I,
    completion: Completion,
}

impl<'a, I: ?Sized + IO> Future for IOCompletionAsync<'a, I> {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let completion = std::pin::pin!(&mut self.as_mut().completion);
        match completion.poll(cx) {
            Poll::Pending => {
                self.io.step()?;
                Poll::Pending
            }
            res => res,
        }
    }
}

impl IOCompletions {
    /// Wais for the Completions to complete
    pub fn wait<I: ?Sized + IO>(self, io: &I) -> Result<()> {
        io.wait_for_completion(self.0)
    }

    /// Waits for Completion to complete and `steps` IO. Ideally the user should do the stepping,
    /// but we do not have yet a good api for this
    pub async fn wait_async<I: ?Sized + IO>(self, io: &I) -> Result<()> {
        IOCompletionAsync {
            io,
            completion: self.0,
        }
        .await
    }

    pub fn finished(&self) -> bool {
        self.0.finished()
    }

    /// Returns true if this is an explicit yield — a signal to return control
    /// to the cooperative scheduler so other fibers can make progress.
    pub fn is_explicit_yield(&self) -> bool {
        self.0.is_explicit_yield()
    }

    /// Send abort signal to completions
    pub fn abort(&self) {
        self.0.abort()
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        self.0.get_error()
    }

    pub fn set_waker(&self, waker: Option<&Waker>) {
        if let Some(waker) = waker {
            self.0.set_waker(waker)
        }
    }
}

#[derive(Debug)]
#[must_use]
pub enum IOResult<T> {
    Done(T),
    IO(IOCompletions),
}

impl<T> IOResult<T> {
    #[inline]
    pub fn is_io(&self) -> bool {
        matches!(self, IOResult::IO(..))
    }

    #[inline]
    pub fn io(self) -> Option<IOCompletions> {
        match self {
            IOResult::Done(_) => None,
            IOResult::IO(io) => Some(io),
        }
    }

    #[inline]
    pub fn map<U>(self, func: impl FnOnce(T) -> U) -> IOResult<U> {
        match self {
            IOResult::Done(t) => IOResult::Done(func(t)),
            IOResult::IO(io) => IOResult::IO(io),
        }
    }
}

/// Evaluate a Result<IOResult<T>>, if IO return IO.
#[macro_export]
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
            Err(err) => {
                branches::mark_unlikely();
                return Err(err);
            }
        }
    };
}

#[macro_export]
macro_rules! return_and_restore_if_io {
    ($field:expr, $saved_state:expr, $e:expr) => {
        match $e {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => {
                let _ = std::mem::replace($field, $saved_state);
                return Ok(IOResult::IO(io));
            }
            Err(e) => {
                let _ = std::mem::replace($field, $saved_state);
                return Err(e);
            }
        }
    };
}
