use crate::{turso_assert, turso_assert_eq};
use core::fmt::{self, Debug};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    task::{Poll, Waker},
};

use crate::sync::Mutex;

use crate::{Buffer, CompletionError};

/// Callback for read completions. Returns `Some(error)` if the callback detects an error
/// (e.g., short read), which will be stored in the completion and propagated to VDBE.
pub type ReadComplete =
    dyn Fn(Result<(Arc<Buffer>, i32), CompletionError>) -> Option<CompletionError> + Send + Sync;
pub type WriteComplete = dyn Fn(Result<i32, CompletionError>) + Send + Sync;
pub type SyncComplete = dyn Fn(Result<i32, CompletionError>) + Send + Sync;
pub type TruncateComplete = dyn Fn(Result<i32, CompletionError>) + Send + Sync;

#[must_use]
#[derive(Debug, Clone)]
pub struct Completion {
    /// Optional completion state. If None, it means we are Yield in order to not allocate anything
    pub(super) inner: Option<Arc<CompletionInner>>,
}

impl Future for Completion {
    type Output = Result<(), crate::LimboError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.set_waker(cx.waker());
        if self.finished() {
            self.wake();
            let res = self
                .get_error()
                .map_or(Ok(()), |err| Err(crate::LimboError::CompletionError(err)));
            return Poll::Ready(res);
        }
        Poll::Pending
    }
}

#[derive(Debug, Default)]
struct ContextInner {
    waker: Option<Waker>,
    // TODO: add abort signal
}

#[derive(Debug, Clone)]
pub struct Context {
    inner: Arc<Mutex<ContextInner>>,
}

impl ContextInner {
    pub fn new() -> Self {
        Self { waker: None }
    }

    pub fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub fn set_waker(&mut self, waker: &Waker) {
        if let Some(curr_waker) = self.waker.as_mut() {
            // only call and change waker if it would awake a different task
            if !curr_waker.will_wake(waker) {
                let prev_waker = std::mem::replace(curr_waker, waker.clone());
                prev_waker.wake();
            }
        } else {
            self.waker = Some(waker.clone());
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ContextInner::new())),
        }
    }

    pub fn wake(&self) {
        self.inner.lock().wake();
    }

    pub fn set_waker(&self, waker: &Waker) {
        self.inner.lock().set_waker(waker);
    }
}

pub(super) struct CompletionInner {
    completion_type: CompletionType,
    /// None means we completed successfully
    // Thread safe with OnceLock
    pub(super) result: crate::sync::OnceLock<Option<CompletionError>>,
    context: Context,
    /// The group this completion belongs to, if any. `CompletionGroup::add`
    /// sets it before the completion is submitted, so by the time the
    /// completion finishes and its callback counts it into the group, the
    /// link is already there.
    parent: OnceLock<Arc<GroupCompletionInner>>,
    /// Keeps the write buffer alive for async I/O backends (io_uring, VFS)
    /// where pwrite returns before the kernel has consumed the buffer.
    write_buffer: OnceLock<Arc<Buffer>>,
}

impl fmt::Debug for CompletionInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionInner")
            .field("completion_type", &self.completion_type)
            .field("parent", &self.parent.get().is_some())
            .finish()
    }
}

/// Waits for a set of child completions. The group finishes when every
/// child has finished and `build` has been called.
///
/// The group counts how many things are still outstanding. It starts at 1:
/// a token held by the builder, released by `build`. Each `add` counts one
/// more. That token keeps the group from finishing while children are still
/// being added, even if the ones added so far finish right away.
pub struct CompletionGroup {
    completions: Vec<Completion>,
    /// The group's own completion, handed out by `build`.
    completion: Completion,
    inner: Arc<GroupCompletionInner>,
}

impl fmt::Debug for CompletionGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionGroup")
            .field("children", &self.completions.len())
            .field(
                "outstanding",
                &self.inner.outstanding.load(Ordering::SeqCst),
            )
            .finish()
    }
}

impl CompletionGroup {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        let inner = Arc::new(GroupCompletionInner {
            outstanding: AtomicUsize::new(1),
            complete: Box::new(callback),
            result: OnceLock::new(),
            self_completion: OnceLock::new(),
        });
        let completion = Completion::new(CompletionType::Group(GroupCompletion {
            inner: inner.clone(),
        }));
        let _ = inner.self_completion.set(completion.clone());
        Self {
            completions: Vec::new(),
            completion,
            inner,
        }
    }

    /// Add a child. Call this before the child is submitted to the IO
    /// backend. The child's own callback is what counts it into the group,
    /// so the link has to be there before the child can finish.
    pub fn add(&mut self, c: &Completion) {
        self.completions.push(c.clone());
        self.inner.outstanding.fetch_add(1, Ordering::SeqCst);
        turso_assert!(
            c.get_inner().parent.set(self.inner.clone()).is_ok(),
            "completion can only be linked once"
        );
        turso_assert!(
            !c.finished(),
            "completion was added to a group after it finished"
        );
    }

    /// The children added so far. Used by error paths that need to
    /// wait on the kernel side via `IO::drain_completions` after
    /// cancelling the group.
    pub fn completions(&self) -> &[Completion] {
        &self.completions
    }

    pub fn len(&self) -> usize {
        self.completions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.completions.is_empty()
    }

    pub fn cancel(&self) {
        for c in &self.completions {
            c.abort();
        }
    }

    /// Release the builder's token. The group finishes now if every child
    /// has already finished, or later when the last one does.
    pub fn build(self) -> Completion {
        self.inner.one_done(None);
        self.completion
    }
}

pub struct GroupCompletion {
    inner: Arc<GroupCompletionInner>,
}

impl fmt::Debug for GroupCompletion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupCompletion")
            .field(
                "outstanding",
                &self.inner.outstanding.load(Ordering::SeqCst),
            )
            .finish()
    }
}

struct GroupCompletionInner {
    /// Number of completions that need to finish
    outstanding: AtomicUsize,
    /// Callback to invoke when all completions finish
    complete: Box<dyn Fn(Result<i32, CompletionError>) + Send + Sync>,
    /// Cached result after all completions finish
    result: OnceLock<Option<CompletionError>>,
    /// Reference to the group's own Completion for notifying parents
    self_completion: OnceLock<Completion>,
}

impl GroupCompletion {
    pub fn callback(&self, result: Result<i32, CompletionError>) {
        turso_assert_eq!(
            self.inner.outstanding.load(Ordering::SeqCst),
            0,
            "callback called before all completions finished"
        );
        (self.inner.complete)(result);
    }
}

impl GroupCompletionInner {
    /// One outstanding count is done: a child finished with `err`, or
    /// `build` released the builder's token. Fires the group's callback
    /// when this was the last one.
    fn one_done(&self, err: Option<CompletionError>) {
        if let Some(err) = err {
            // Keep the first error.
            let _ = self.result.set(Some(err));
        }
        let prev = self.outstanding.fetch_sub(1, Ordering::SeqCst);
        turso_assert!(prev > 0, "completion group counted below zero");
        let group_completion = self
            .self_completion
            .get()
            .expect("group completion is set in CompletionGroup::new");
        if prev > 1 {
            // Progress wake so the waiter keeps driving io.step.
            group_completion.wake();
            return;
        }
        // Set result to Some(None) on success so succeeded() returns true.
        let _ = self.result.set(None);
        let result = self.result.get().and_then(|e| *e);
        // This runs Completion::callback on the group's own completion,
        // which in turn counts the group into its parent, if it has one.
        group_completion.callback(result.map_or(Ok(0), Err));
    }
}

impl Debug for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(..) => f.debug_tuple("Read").finish(),
            Self::Write(..) => f.debug_tuple("Write").finish(),
            Self::Sync(..) => f.debug_tuple("Sync").finish(),
            Self::Truncate(..) => f.debug_tuple("Truncate").finish(),
            Self::Group(..) => f.debug_tuple("Group").finish(),
            Self::Yield => f.debug_tuple("Yield").finish(),
        }
    }
}

pub enum CompletionType {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
    Truncate(TruncateCompletion),
    Group(GroupCompletion),
    Yield,
}

impl CompletionInner {
    fn new(completion_type: CompletionType) -> Self {
        Self {
            completion_type,
            result: OnceLock::new(),
            context: Context::new(),
            parent: OnceLock::new(),
            write_buffer: OnceLock::new(),
        }
    }
}

impl Completion {
    pub fn new(completion_type: CompletionType) -> Self {
        Self {
            inner: Some(Arc::new(CompletionInner::new(completion_type))),
        }
    }

    pub(super) fn get_inner(&self) -> &Arc<CompletionInner> {
        self.inner
            .as_ref()
            .expect("completion inner should be initialized")
    }

    /// Stores a write buffer reference in the completion to keep it alive
    /// until the I/O completes. Required for async backends (io_uring, VFS)
    /// where pwrite returns before the kernel has consumed the buffer.
    pub fn keep_write_buffer_alive(&self, buf: Arc<Buffer>) {
        self.get_inner()
            .write_buffer
            .set(buf)
            .expect("write buffer should only be set once");
    }

    pub fn new_write<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: Fn(Result<(Arc<Buffer>, i32), CompletionError>) -> Option<CompletionError>
            + Send
            + Sync
            + 'static,
    {
        Self::new(CompletionType::Read(ReadCompletion::new(
            buf,
            Box::new(complete),
        )))
    }
    pub fn new_sync<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Sync(SyncCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_trunc<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Truncate(TruncateCompletion::new(Box::new(
            complete,
        ))))
    }

    /// Create a yield completion. These are completed by default allowing to yield control without
    /// allocating memory.
    pub fn new_yield() -> Self {
        Self { inner: None }
    }

    pub fn wake(&self) {
        if let Some(inner) = &self.inner {
            inner.context.wake();
        }
    }

    /// Fire a "progress wake" without consuming the completion's result —
    /// wakes the waker on this completion *and* on its parent group, if any.
    /// Used by IO backends to nudge the future when an operation made progress
    /// but isn't fully done (e.g. an io_uring writev that completed only the
    /// first chunk and was resubmitted internally). Without this, a poll
    /// whose drained CQEs are all intermediate-chunk completions returns
    /// without waking anything, and since `step()` is the only thing that
    /// drains the CQ, the resubmitted chunks pile up and the task deadlocks.
    pub fn wake_progress(&self) {
        if let Some(inner) = &self.inner {
            if let Some(group) = inner.parent.get() {
                if let Some(group_completion) = group.self_completion.get() {
                    group_completion.wake();
                }
            }
            inner.context.wake();
        }
    }

    pub fn set_waker(&self, waker: &Waker) {
        if self.finished() || self.inner.is_none() {
            waker.wake_by_ref();
        } else {
            self.get_inner().context.set_waker(waker);
        }
    }

    pub fn succeeded(&self) -> bool {
        match &self.inner {
            Some(inner) => match &inner.completion_type {
                CompletionType::Group(g) => {
                    g.inner.outstanding.load(Ordering::SeqCst) == 0
                        && g.inner.result.get().is_some_and(|e| e.is_none())
                }
                _ => inner.result.get().is_some_and(|e| e.is_none()),
            },
            None => true,
        }
    }

    pub fn failed(&self) -> bool {
        match &self.inner {
            Some(inner) => inner.result.get().is_some_and(|val| val.is_some()),
            None => false,
        }
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        match &self.inner {
            Some(inner) => {
                match &inner.completion_type {
                    CompletionType::Group(g) => {
                        // For groups, check the group's cached result field
                        // (set when the last completion finishes)
                        g.inner.result.get().and_then(|res| *res)
                    }
                    _ => inner.result.get().and_then(|res| *res),
                }
            }
            None => None,
        }
    }

    /// Checks if the Completion completed or errored
    pub fn finished(&self) -> bool {
        match &self.inner {
            Some(inner) => match &inner.completion_type {
                CompletionType::Group(g) => g.inner.outstanding.load(Ordering::SeqCst) == 0,
                _ => inner.result.get().is_some(),
            },
            None => true,
        }
    }

    /// Returns true if this completion is an explicit yield — a signal to
    /// return control to the cooperative scheduler so other connections can make
    /// progress. Unlike real I/O completions that happen to be finished,
    /// yield completions must not be treated as "ready to continue immediately"
    /// because the yielding operation is waiting on external state (e.g. a lock
    /// held by another fiber) that can only change when other fibers are stepped.
    pub fn is_explicit_yield(&self) -> bool {
        self.inner.is_none()
    }

    pub fn complete(&self, result: i32) {
        let result = Ok(result);
        self.callback(result);
    }

    pub fn error(&self, err: CompletionError) {
        let result = Err(err);
        self.callback(result);
    }

    pub fn abort(&self) {
        self.error(CompletionError::Aborted);
    }

    fn callback(&self, result: Result<i32, CompletionError>) {
        let inner = self.get_inner();
        let mut first = false;
        inner.result.get_or_init(|| {
            first = true;
            // Run the type-specific callback. For ReadCompletion, this returns
            // an optional error detected by the callback (e.g., short read).
            let callback_error = match &inner.completion_type {
                CompletionType::Read(r) => r.callback(result),
                CompletionType::Write(w) => {
                    w.callback(result);
                    None
                }
                CompletionType::Sync(s) => {
                    s.callback(result);
                    None
                }
                CompletionType::Truncate(t) => {
                    t.callback(result);
                    None
                }
                CompletionType::Group(g) => {
                    g.callback(result);
                    None
                }
                CompletionType::Yield => None,
            };

            // Use callback error if present, otherwise use the original IO error
            callback_error.or_else(|| result.err())
        });
        // Only the call that finished this completion counts it into its
        // group.
        if first {
            if let Some(group) = inner.parent.get() {
                group.one_done(inner.result.get().and_then(|e| *e));
            }
        }
        inner.context.wake();
    }

    /// only call this method if you are sure that the completion is
    /// a ReadCompletion, panics otherwise
    pub fn as_read(&self) -> &ReadCompletion {
        let inner = self.get_inner();
        match inner.completion_type {
            CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        }
    }
}

pub struct ReadCompletion {
    pub buf: Arc<Buffer>,
    pub complete: Box<ReadComplete>,
}

impl ReadCompletion {
    pub fn new(buf: Arc<Buffer>, complete: Box<ReadComplete>) -> Self {
        Self { buf, complete }
    }

    pub fn buf(&self) -> &Buffer {
        &self.buf
    }

    pub fn callback(&self, bytes_read: Result<i32, CompletionError>) -> Option<CompletionError> {
        (self.complete)(bytes_read.map(|b| (self.buf.clone(), b)))
    }

    pub fn buf_arc(&self) -> Arc<Buffer> {
        self.buf.clone()
    }
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, bytes_written: Result<i32, CompletionError>) {
        (self.complete)(bytes_written);
    }
}

pub struct SyncCompletion {
    pub complete: Box<SyncComplete>,
}

impl SyncCompletion {
    pub fn new(complete: Box<SyncComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        (self.complete)(res);
    }
}

pub struct TruncateCompletion {
    pub complete: Box<TruncateComplete>,
}

impl TruncateCompletion {
    pub fn new(complete: Box<TruncateComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        (self.complete)(res);
    }
}

#[cfg(test)]
#[path = "../tests/unit/io/completions/tests.rs"]
mod tests;
