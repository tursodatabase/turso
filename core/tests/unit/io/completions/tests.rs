use crate::CompletionError;

use super::*;

#[test]
fn group_finishes_when_child_completes_on_another_thread_during_build() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    for i in 0..20_000 {
        let child = Completion::new_write(|_| {});
        let go = Arc::new(AtomicBool::new(false));
        let (c2, go2) = (child.clone(), go.clone());
        let t = thread::spawn(move || {
            while !go2.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            c2.complete(0);
        });
        let mut group = CompletionGroup::new(|_| {});
        group.add(&child);
        go.store(true, Ordering::Release);
        let g = group.build();
        t.join().unwrap();
        assert!(child.finished());
        assert!(
            g.finished(),
            "iteration {i}: child finished but the group did not"
        );
    }
}

#[test]
fn group_callback_fires_once_when_children_finished_before_build() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let c1 = Completion::new_write(|_| {});
    let c2 = Completion::new_write(|_| {});
    let calls2 = calls.clone();
    let mut group = CompletionGroup::new(move |_| {
        calls2.fetch_add(1, Ordering::SeqCst);
    });
    group.add(&c1);
    group.add(&c2);
    c1.complete(0);
    c2.complete(0);
    let g = group.build();
    assert!(g.finished());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
#[should_panic(expected = "added to a group after it finished")]
fn adding_a_finished_completion_panics() {
    let c = Completion::new_write(|_| {});
    c.complete(0);
    let mut group = CompletionGroup::new(|_| {});
    group.add(&c);
}

#[test]
fn test_completion_group_empty() {
    use crate::sync::atomic::{AtomicBool, Ordering};

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
    use crate::sync::atomic::{AtomicBool, Ordering};
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

    group.add(&c1);
    group.add(&c2);

    // Complete both before build()
    c1.complete(0);
    c2.complete(0);

    let group = group.build();

    // All completions were already complete, so group should be finished immediately
    assert!(group.finished());
    assert!(group.succeeded());
    assert!(group.get_error().is_none());
}

#[test]
fn test_completion_group_mixed_finished_and_pending() {
    use crate::sync::atomic::{AtomicBool, Ordering};
    let called = Arc::new(AtomicBool::new(false));
    let called_clone = called.clone();

    let mut group = CompletionGroup::new(move |_| {
        called_clone.store(true, Ordering::SeqCst);
    });

    let c1 = Completion::new_write(|_| {});
    let c2 = Completion::new_write(|_| {});
    let c3 = Completion::new_write(|_| {});
    let c4 = Completion::new_write(|_| {});

    group.add(&c1);
    group.add(&c2);
    group.add(&c3);
    group.add(&c4);

    // Complete c1 and c3 before build()
    c1.complete(0);
    c3.complete(0);

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

    group.add(&c1);
    group.add(&c2);

    // Fail c1 before build()
    c1.error(CompletionError::Aborted);

    let group = group.build();

    // The group waits for c2 even though c1 already failed: a waiter
    // must not carry on while a child's IO is still in flight.
    assert!(!group.finished());

    c2.complete(0);
    assert!(group.finished());
    assert!(!group.succeeded());
    assert_eq!(group.get_error(), Some(CompletionError::Aborted));
}

#[test]
fn test_completion_group_nested() {
    use crate::sync::atomic::{AtomicUsize, Ordering};

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
    use crate::sync::atomic::{AtomicBool, Ordering};

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

// Tests for individual completion success/failure status

#[test]
fn test_write_completion_pending_status() {
    let c = Completion::new_write(|_| {});

    // Pending completion should not be finished, succeeded, or failed
    assert!(!c.finished());
    assert!(!c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_write_completion_success() {
    let c = Completion::new_write(|_| {});

    c.complete(42);

    assert!(c.finished());
    assert!(c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_write_completion_failure() {
    let c = Completion::new_write(|_| {});

    c.error(CompletionError::Aborted);

    assert!(c.finished());
    assert!(!c.succeeded());
    assert!(c.failed());
    assert_eq!(c.get_error(), Some(CompletionError::Aborted));
}

#[test]
fn test_read_completion_pending_status() {
    let buf = Arc::new(crate::Buffer::new_temporary(4096));
    let c = Completion::new_read(buf, |_| None);

    assert!(!c.finished());
    assert!(!c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_read_completion_success() {
    let buf = Arc::new(crate::Buffer::new_temporary(4096));
    let c = Completion::new_read(buf, |_| None);

    c.complete(1024);

    assert!(c.finished());
    assert!(c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_read_completion_failure() {
    let buf = Arc::new(crate::Buffer::new_temporary(4096));
    let c = Completion::new_read(buf, |_| None);

    c.error(CompletionError::Aborted);

    assert!(c.finished());
    assert!(!c.succeeded());
    assert!(c.failed());
    assert_eq!(c.get_error(), Some(CompletionError::Aborted));
}

#[test]
fn test_sync_completion_pending_status() {
    let c = Completion::new_sync(|_| {});

    assert!(!c.finished());
    assert!(!c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_sync_completion_success() {
    let c = Completion::new_sync(|_| {});

    c.complete(0);

    assert!(c.finished());
    assert!(c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_sync_completion_failure() {
    let c = Completion::new_sync(|_| {});

    c.error(CompletionError::Aborted);

    assert!(c.finished());
    assert!(!c.succeeded());
    assert!(c.failed());
    assert_eq!(c.get_error(), Some(CompletionError::Aborted));
}

#[test]
fn test_truncate_completion_pending_status() {
    let c = Completion::new_trunc(|_| {});

    assert!(!c.finished());
    assert!(!c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_truncate_completion_success() {
    let c = Completion::new_trunc(|_| {});

    c.complete(0);

    assert!(c.finished());
    assert!(c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_truncate_completion_failure() {
    let c = Completion::new_trunc(|_| {});

    c.error(CompletionError::Aborted);

    assert!(c.finished());
    assert!(!c.succeeded());
    assert!(c.failed());
    assert_eq!(c.get_error(), Some(CompletionError::Aborted));
}

#[test]
fn test_yield_completion_status() {
    let c = Completion::new_yield();

    // Yield completions are always considered finished and succeeded
    assert!(c.finished());
    assert!(c.succeeded());
    assert!(!c.failed());
    assert!(c.get_error().is_none());
}

#[test]
fn test_completion_abort() {
    let c = Completion::new_write(|_| {});

    c.abort();

    assert!(c.finished());
    assert!(!c.succeeded());
    assert!(c.failed());
    assert_eq!(c.get_error(), Some(CompletionError::Aborted));
}

#[test]
fn test_completion_callback_receives_success_result() {
    use crate::sync::atomic::{AtomicI32, Ordering};

    let result_value = Arc::new(AtomicI32::new(-1));
    let result_value_clone = result_value.clone();

    let c = Completion::new_write(move |res| {
        if let Ok(val) = res {
            result_value_clone.store(val, Ordering::SeqCst);
        }
    });

    c.complete(42);

    assert_eq!(result_value.load(Ordering::SeqCst), 42);
    assert!(c.succeeded());
}

#[test]
fn test_completion_callback_receives_error_result() {
    use crate::sync::atomic::{AtomicBool, Ordering};

    let got_error = Arc::new(AtomicBool::new(false));
    let got_error_clone = got_error.clone();

    let c = Completion::new_write(move |res| {
        if res.is_err() {
            got_error_clone.store(true, Ordering::SeqCst);
        }
    });

    c.error(CompletionError::Aborted);

    assert!(got_error.load(Ordering::SeqCst));
    assert!(c.failed());
}

#[test]
fn test_completion_idempotent_complete() {
    // Completing a completion multiple times should only trigger the callback once
    use crate::sync::atomic::{AtomicUsize, Ordering};

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let c = Completion::new_write(move |_| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    });

    c.complete(1);
    c.complete(2);
    c.complete(3);

    // Callback should only be called once
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert!(c.succeeded());
}

#[test]
fn test_completion_idempotent_error() {
    // Erroring a completion multiple times should only trigger the callback once
    use crate::sync::atomic::{AtomicUsize, Ordering};

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let c = Completion::new_write(move |_| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    });

    c.error(CompletionError::Aborted);
    c.error(CompletionError::Aborted);
    c.complete(0); // Try completing after error

    // Callback should only be called once
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert!(c.failed());
}
