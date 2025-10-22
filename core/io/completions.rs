use core::fmt::{self, Debug};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    task::Waker,
};

use parking_lot::Mutex;

use crate::{Buffer, CompletionError};

pub type ReadComplete = dyn Fn(Result<(Arc<Buffer>, i32), CompletionError>);
pub type WriteComplete = dyn Fn(Result<i32, CompletionError>);
pub type SyncComplete = dyn Fn(Result<i32, CompletionError>);
pub type TruncateComplete = dyn Fn(Result<i32, CompletionError>);

#[must_use]
#[derive(Debug, Clone)]
pub struct Completion {
    /// Optional completion state. If None, it means we are Yield in order to not allocate anything
    pub(super) inner: Option<Arc<CompletionInner>>,
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
    pub(super) result: std::sync::OnceLock<Option<CompletionError>>,
    needs_link: bool,
    context: Context,
    /// Optional parent group this completion belongs to
    parent: OnceLock<Arc<GroupCompletionInner>>,
}

impl fmt::Debug for CompletionInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionInner")
            .field("completion_type", &self.completion_type)
            .field("needs_link", &self.needs_link)
            .field("parent", &self.parent.get().is_some())
            .finish()
    }
}

pub struct CompletionGroup {
    completions: Vec<Completion>,
    callback: Box<dyn Fn(Result<i32, CompletionError>) + Send + Sync>,
}

impl CompletionGroup {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self {
            completions: Vec::new(),
            callback: Box::new(callback),
        }
    }

    pub fn add(&mut self, completion: &Completion) {
        self.completions.push(completion.clone());
    }

    pub fn cancel(&self) {
        for c in &self.completions {
            c.abort();
        }
    }

    pub fn build(self) -> Completion {
        let total = self.completions.len();
        if total == 0 {
            (self.callback)(Ok(0));
            return Completion::new_yield();
        }
        let group_completion = GroupCompletion::new(self.callback, total);
        let group = Completion::new(CompletionType::Group(group_completion));

        // Store the group completion reference for later callback
        if let CompletionType::Group(ref g) = group.get_inner().completion_type {
            let _ = g.inner.self_completion.set(group.clone());
        }

        for mut c in self.completions {
            // If the completion has not completed, link it to the group.
            if !c.finished() {
                c.link_internal(&group);
                continue;
            }
            let group_inner = match &group.get_inner().completion_type {
                CompletionType::Group(g) => &g.inner,
                _ => unreachable!(),
            };
            // Return early if there was an error.
            if let Some(err) = c.get_error() {
                let _ = group_inner.result.set(Some(err));
                group_inner.outstanding.store(0, Ordering::SeqCst);
                (group_inner.complete)(Err(err));
                return group;
            }
            // Mark the successful completion as done.
            group_inner.outstanding.fetch_sub(1, Ordering::SeqCst);
        }

        let group_inner = match &group.get_inner().completion_type {
            CompletionType::Group(g) => &g.inner,
            _ => unreachable!(),
        };
        if group_inner.outstanding.load(Ordering::SeqCst) == 0 {
            (group_inner.complete)(Ok(0));
        }
        group
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
    pub fn new<F>(complete: F, outstanding: usize) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(GroupCompletionInner {
                outstanding: AtomicUsize::new(outstanding),
                complete: Box::new(complete),
                result: OnceLock::new(),
                self_completion: OnceLock::new(),
            }),
        }
    }

    pub fn callback(&self, result: Result<i32, CompletionError>) {
        assert_eq!(
            self.inner.outstanding.load(Ordering::SeqCst),
            0,
            "callback called before all completions finished"
        );
        (self.inner.complete)(result);
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
    fn new(completion_type: CompletionType, needs_link: bool) -> Self {
        Self {
            completion_type,
            result: OnceLock::new(),
            needs_link,
            context: Context::new(),
            parent: OnceLock::new(),
        }
    }
}

impl Completion {
    pub fn new(completion_type: CompletionType) -> Self {
        Self {
            inner: Some(Arc::new(CompletionInner::new(completion_type, false))),
        }
    }

    pub fn new_linked(completion_type: CompletionType) -> Self {
        Self {
            inner: Some(Arc::new(CompletionInner::new(completion_type, true))),
        }
    }

    pub(super) fn get_inner(&self) -> &Arc<CompletionInner> {
        self.inner.as_ref().unwrap()
    }

    pub fn needs_link(&self) -> bool {
        self.get_inner().needs_link
    }

    pub fn new_write_linked<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new_linked(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_write<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: Fn(Result<(Arc<Buffer>, i32), CompletionError>) + 'static,
    {
        Self::new(CompletionType::Read(ReadCompletion::new(
            buf,
            Box::new(complete),
        )))
    }
    pub fn new_sync<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
    {
        Self::new(CompletionType::Sync(SyncCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_trunc<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + 'static,
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
        self.get_inner().context.wake();
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
                        && g.inner.result.get().is_none_or(|e| e.is_none())
                }
                _ => inner.result.get().is_some(),
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
        inner.result.get_or_init(|| {
            match &inner.completion_type {
                CompletionType::Read(r) => r.callback(result),
                CompletionType::Write(w) => w.callback(result),
                CompletionType::Sync(s) => s.callback(result), // fix
                CompletionType::Truncate(t) => t.callback(result),
                CompletionType::Group(g) => g.callback(result),
                CompletionType::Yield => {}
            };

            if let Some(group) = inner.parent.get() {
                // Capture first error in group
                if let Err(err) = result {
                    let _ = group.result.set(Some(err));
                }
                let prev = group.outstanding.fetch_sub(1, Ordering::SeqCst);

                // If this was the last completion in the group, trigger the group's callback
                // which will recursively call this same callback() method to notify parents
                if prev == 1 {
                    if let Some(group_completion) = group.self_completion.get() {
                        let group_result = group.result.get().and_then(|e| *e);
                        group_completion.callback(group_result.map_or(Ok(0), Err));
                    }
                }
            }

            result.err()
        });
        // call the waker regardless
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

    /// Link this completion to a group completion (internal use only)
    fn link_internal(&mut self, group: &Completion) {
        let group_inner = match &group.get_inner().completion_type {
            CompletionType::Group(g) => &g.inner,
            _ => panic!("link_internal() requires a group completion"),
        };

        // Set the parent (can only be set once)
        if self.get_inner().parent.set(group_inner.clone()).is_err() {
            panic!("completion can only be linked once");
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

    pub fn callback(&self, bytes_read: Result<i32, CompletionError>) {
        (self.complete)(bytes_read.map(|b| (self.buf.clone(), b)));
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
mod tests {
    use crate::CompletionError;

    use super::*;

    #[test]
    fn test_completion_group_empty() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_clone = callback_called.clone();

        let group = CompletionGroup::new(move |_| {
            callback_called_clone.store(true, Ordering::SeqCst);
        });
        let group = group.build();
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());

        // Verify the callback was actually called
        assert!(
            callback_called.load(Ordering::SeqCst),
            "callback should be called for empty group"
        );
    }

    #[test]
    fn test_completion_group_single_completion() {
        let mut group = CompletionGroup::new(|_| {});
        let c = Completion::new_write(|_| {});
        group.add(&c);
        let group = group.build();

        assert!(!group.finished());
        assert!(!group.succeeded());

        c.complete(0);

        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_multiple_completions() {
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        let c3 = Completion::new_write(|_| {});
        group.add(&c1);
        group.add(&c2);
        group.add(&c3);
        let group = group.build();

        assert!(!group.succeeded());
        assert!(!group.finished());

        c1.complete(0);
        assert!(!group.succeeded());
        assert!(!group.finished());

        c2.complete(0);
        assert!(!group.succeeded());
        assert!(!group.finished());

        c3.complete(0);
        assert!(group.succeeded());
        assert!(group.finished());
    }

    #[test]
    fn test_completion_group_with_error() {
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        group.add(&c1);
        group.add(&c2);
        let group = group.build();

        c1.complete(0);
        c2.error(CompletionError::Aborted);

        assert!(group.finished());
        assert!(!group.succeeded());
        assert_eq!(group.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_completion_group_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let mut group = CompletionGroup::new(move |_| {
            called_clone.store(true, Ordering::SeqCst);
        });

        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        group.add(&c1);
        group.add(&c2);
        let group = group.build();

        assert!(!called.load(Ordering::SeqCst));

        c1.complete(0);
        assert!(!called.load(Ordering::SeqCst));

        c2.complete(0);
        assert!(called.load(Ordering::SeqCst));
        assert!(group.finished());
        assert!(group.succeeded());
    }

    #[test]
    fn test_completion_group_some_already_completed() {
        // Test some completions added to group, then finish before build()
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        let c3 = Completion::new_write(|_| {});

        // Add all to group while pending
        group.add(&c1);
        group.add(&c2);
        group.add(&c3);

        // Complete c1 and c2 AFTER adding but BEFORE build()
        c1.complete(0);
        c2.complete(0);

        let group = group.build();

        // c1 and c2 finished before build(), so outstanding should account for them
        // Only c3 should be pending
        assert!(!group.finished());
        assert!(!group.succeeded());

        // Complete c3
        c3.complete(0);

        // Now the group should be finished
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_all_already_completed() {
        // Test when all completions are already finished before build()
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});

        // Complete both before adding to group
        c1.complete(0);
        c2.complete(0);

        group.add(&c1);
        group.add(&c2);

        let group = group.build();

        // All completions were already complete, so group should be finished immediately
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_mixed_finished_and_pending() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let mut group = CompletionGroup::new(move |_| {
            called_clone.store(true, Ordering::SeqCst);
        });

        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        let c3 = Completion::new_write(|_| {});
        let c4 = Completion::new_write(|_| {});

        // Complete c1 and c3 before adding to group
        c1.complete(0);
        c3.complete(0);

        group.add(&c1);
        group.add(&c2);
        group.add(&c3);
        group.add(&c4);

        let group = group.build();

        // Only c2 and c4 should be pending
        assert!(!group.finished());
        assert!(!called.load(Ordering::SeqCst));

        c2.complete(0);
        assert!(!group.finished());
        assert!(!called.load(Ordering::SeqCst));

        c4.complete(0);
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_completion_group_already_completed_with_error() {
        // Test when a completion finishes with error before build()
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});

        // Complete c1 with error before adding to group
        c1.error(CompletionError::Aborted);

        group.add(&c1);
        group.add(&c2);

        let group = group.build();

        // Group should immediately fail with the error
        assert!(group.finished());
        assert!(!group.succeeded());
        assert_eq!(group.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_completion_group_tracks_all_completions() {
        // This test verifies the fix for the bug where CompletionGroup::add()
        // would skip successfully-finished completions. This caused problems
        // when code used drain() to move completions into a group, because
        // finished completions would be removed from the source but not tracked
        // by the group, effectively losing them.
        use std::sync::atomic::{AtomicUsize, Ordering};

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();

        // Simulate the pattern: create multiple completions, complete some,
        // then add ALL of them to a group (like drain() would do)
        let mut completions = Vec::new();

        // Create 4 completions
        for _ in 0..4 {
            completions.push(Completion::new_write(|_| {}));
        }

        // Complete 2 of them before adding to group (simulate async completion)
        completions[0].complete(0);
        completions[2].complete(0);

        // Now create a group and add ALL completions (like drain() would do)
        let mut group = CompletionGroup::new(move |_| {
            callback_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Add all completions to the group
        for c in &completions {
            group.add(c);
        }

        let group = group.build();

        // The group should track all 4 completions:
        // - c[0] and c[2] are already finished
        // - c[1] and c[3] are still pending
        // So the group should not be finished yet
        assert!(!group.finished());
        assert_eq!(callback_count.load(Ordering::SeqCst), 0);

        // Complete the first pending completion
        completions[1].complete(0);
        assert!(!group.finished());
        assert_eq!(callback_count.load(Ordering::SeqCst), 0);

        // Complete the last pending completion - now group should finish
        completions[3].complete(0);
        assert!(group.finished());
        assert!(group.succeeded());
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);

        // Verify no errors
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_with_all_finished_successfully() {
        // Edge case: all completions are already successfully finished
        // when added to the group. The group should complete immediately.
        use std::sync::atomic::{AtomicBool, Ordering};

        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_clone = callback_called.clone();

        let mut completions = Vec::new();

        // Create and immediately complete 3 completions
        for _ in 0..3 {
            let c = Completion::new_write(|_| {});
            c.complete(0);
            completions.push(c);
        }

        // Add all already-completed completions to group
        let mut group = CompletionGroup::new(move |_| {
            callback_called_clone.store(true, Ordering::SeqCst);
        });

        for c in &completions {
            group.add(c);
        }

        let group = group.build();

        // Group should be immediately finished since all completions were done
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(callback_called.load(Ordering::SeqCst));
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_nested() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Track callbacks at different levels
        let parent_called = Arc::new(AtomicUsize::new(0));
        let child1_called = Arc::new(AtomicUsize::new(0));
        let child2_called = Arc::new(AtomicUsize::new(0));

        // Create child group 1 with 2 completions
        let child1_called_clone = child1_called.clone();
        let mut child_group1 = CompletionGroup::new(move |_| {
            child1_called_clone.fetch_add(1, Ordering::SeqCst);
        });
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        child_group1.add(&c1);
        child_group1.add(&c2);
        let child_group1 = child_group1.build();

        // Create child group 2 with 2 completions
        let child2_called_clone = child2_called.clone();
        let mut child_group2 = CompletionGroup::new(move |_| {
            child2_called_clone.fetch_add(1, Ordering::SeqCst);
        });
        let c3 = Completion::new_write(|_| {});
        let c4 = Completion::new_write(|_| {});
        child_group2.add(&c3);
        child_group2.add(&c4);
        let child_group2 = child_group2.build();

        // Create parent group containing both child groups
        let parent_called_clone = parent_called.clone();
        let mut parent_group = CompletionGroup::new(move |_| {
            parent_called_clone.fetch_add(1, Ordering::SeqCst);
        });
        parent_group.add(&child_group1);
        parent_group.add(&child_group2);
        let parent_group = parent_group.build();

        // Initially nothing should be finished
        assert!(!parent_group.finished());
        assert!(!child_group1.finished());
        assert!(!child_group2.finished());
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);
        assert_eq!(child1_called.load(Ordering::SeqCst), 0);
        assert_eq!(child2_called.load(Ordering::SeqCst), 0);

        // Complete first completion in child group 1
        c1.complete(0);
        assert!(!child_group1.finished());
        assert!(!parent_group.finished());
        assert_eq!(child1_called.load(Ordering::SeqCst), 0);
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);

        // Complete second completion in child group 1 - should finish child group 1
        c2.complete(0);
        assert!(child_group1.finished());
        assert!(child_group1.succeeded());
        assert_eq!(child1_called.load(Ordering::SeqCst), 1);

        // Parent should not be finished yet because child group 2 is still pending
        assert!(!parent_group.finished());
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);

        // Complete first completion in child group 2
        c3.complete(0);
        assert!(!child_group2.finished());
        assert!(!parent_group.finished());
        assert_eq!(child2_called.load(Ordering::SeqCst), 0);
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);

        // Complete second completion in child group 2 - should finish everything
        c4.complete(0);
        assert!(child_group2.finished());
        assert!(child_group2.succeeded());
        assert_eq!(child2_called.load(Ordering::SeqCst), 1);

        // Parent should now be finished
        assert!(parent_group.finished());
        assert!(parent_group.succeeded());
        assert_eq!(parent_called.load(Ordering::SeqCst), 1);
        assert!(parent_group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_nested_with_error() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let parent_called = Arc::new(AtomicBool::new(false));
        let child_called = Arc::new(AtomicBool::new(false));

        // Create child group with 2 completions
        let child_called_clone = child_called.clone();
        let mut child_group = CompletionGroup::new(move |_| {
            child_called_clone.store(true, Ordering::SeqCst);
        });
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        child_group.add(&c1);
        child_group.add(&c2);
        let child_group = child_group.build();

        // Create parent group containing child group and another completion
        let parent_called_clone = parent_called.clone();
        let mut parent_group = CompletionGroup::new(move |_| {
            parent_called_clone.store(true, Ordering::SeqCst);
        });
        let c3 = Completion::new_write(|_| {});
        parent_group.add(&child_group);
        parent_group.add(&c3);
        let parent_group = parent_group.build();

        // Complete child group with success
        c1.complete(0);
        c2.complete(0);
        assert!(child_group.finished());
        assert!(child_group.succeeded());
        assert!(child_called.load(Ordering::SeqCst));

        // Parent still pending
        assert!(!parent_group.finished());
        assert!(!parent_called.load(Ordering::SeqCst));

        // Complete c3 with error
        c3.error(CompletionError::Aborted);

        // Parent should finish with error
        assert!(parent_group.finished());
        assert!(!parent_group.succeeded());
        assert_eq!(parent_group.get_error(), Some(CompletionError::Aborted));
        assert!(parent_called.load(Ordering::SeqCst));
    }
}
