use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[test]
fn disabled_handler_never_interrupts() {
    let handler = ProgressHandler::new();
    assert!(!handler.should_interrupt(0, 1));
    assert!(!handler.is_enabled());
}

#[test]
fn handler_runs_only_on_configured_interval() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = ProgressHandler::new();
    let callback_calls = Arc::clone(&calls);
    handler.set(
        3,
        Some(Box::new(move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            false
        })),
    );

    assert!(!handler.should_interrupt(0, 1));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!handler.should_interrupt(1, 2));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!handler.should_interrupt(2, 3));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!handler.should_interrupt(3, 4));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!handler.should_interrupt(5, 6));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn handler_can_request_interrupt() {
    let handler = ProgressHandler::new();
    handler.set(2, Some(Box::new(|| true)));

    assert!(!handler.should_interrupt(0, 1));
    assert!(handler.should_interrupt(1, 2));
}

#[test]
fn disabling_clears_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = ProgressHandler::new();
    let callback_calls = Arc::clone(&calls);
    handler.set(
        1,
        Some(Box::new(move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            true
        })),
    );
    assert!(handler.should_interrupt(0, 1));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    handler.set(0, None);
    assert!(!handler.should_interrupt(1, 2));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
