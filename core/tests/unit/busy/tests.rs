use super::*;

fn test_instant() -> MonotonicInstant {
    MonotonicInstant::now()
}

#[test]
fn test_busy_handler_timeout_basic() {
    let handler = BusyHandler::Timeout(Duration::from_millis(100));
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // First invocation should return true (retry)
    assert!(state.invoke(&handler, now));
    // Timeout should be set to 1ms from now
    assert_eq!(state.timeout(), now + Duration::from_millis(1));
}

#[test]
fn test_busy_handler_timeout_exhausted() {
    let handler = BusyHandler::Timeout(Duration::from_millis(0));
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // Zero timeout should return false immediately
    assert!(!state.invoke(&handler, now));
}

#[test]
fn test_busy_handler_custom_callback() {
    // Callback that retries 3 times then gives up
    let callback: BusyHandlerCallback = Box::new(|count| if count < 3 { 1 } else { 0 });
    let handler = BusyHandler::Custom { callback };
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // First 3 invocations should retry
    assert!(state.invoke(&handler, now));
    assert!(state.invoke(&handler, now));
    assert!(state.invoke(&handler, now));
    // 4th invocation should return false
    assert!(!state.invoke(&handler, now));
}

#[test]
fn test_busy_handler_none_returns_false_immediately() {
    let handler = BusyHandler::None;
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // None handler should always return false (don't retry)
    assert!(!state.invoke(&handler, now));
    // Even on subsequent invocations
    assert!(!state.invoke(&handler, now));
}

#[test]
fn test_custom_callback_receives_correct_count() {
    use std::sync::{Arc, Mutex};

    // Track the counts passed to callback (using Arc+Mutex for Send+Sync)
    let counts = Arc::new(Mutex::new(Vec::new()));
    let counts_clone = counts.clone();

    let callback: BusyHandlerCallback = Box::new(move |count| {
        counts_clone.lock().unwrap().push(count);
        if count < 5 {
            1
        } else {
            0
        }
    });

    let handler = BusyHandler::Custom { callback };
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // Invoke 6 times
    for _ in 0..6 {
        state.invoke(&handler, now);
    }

    // Verify counts were 0, 1, 2, 3, 4, 5
    assert_eq!(*counts.lock().unwrap(), vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn test_custom_callback_always_retry() {
    // Callback that always retries
    let callback: BusyHandlerCallback = Box::new(|_| 1);
    let handler = BusyHandler::Custom { callback };
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // Should always return true
    for _ in 0..100 {
        assert!(state.invoke(&handler, now));
    }
}

#[test]
fn test_custom_callback_never_retry() {
    // Callback that never retries
    let callback: BusyHandlerCallback = Box::new(|_| 0);
    let handler = BusyHandler::Custom { callback };
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // First invocation should return false
    assert!(!state.invoke(&handler, now));
}

#[test]
fn test_custom_callback_sets_timeout() {
    let callback: BusyHandlerCallback = Box::new(|_| 1);
    let handler = BusyHandler::Custom { callback };
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    assert!(state.invoke(&handler, now));
    // Custom callback sets 1ms timeout
    assert_eq!(state.timeout(), now + Duration::from_millis(1));
}

#[test]
fn test_timeout_delay_schedule() {
    let handler = BusyHandler::Timeout(Duration::from_secs(10)); // Long timeout
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // Expected delays per iteration: 1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100ms
    // The timeout is set to `now + delay` each time, so we check individual delays
    let expected_delays_ms: [u64; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];

    for (i, expected_ms) in expected_delays_ms.iter().enumerate() {
        assert!(state.invoke(&handler, now), "iteration {i} should retry");
        let timeout = state.timeout();
        assert_eq!(
            timeout,
            now + Duration::from_millis(*expected_ms),
            "iteration {i} should have delay of {expected_ms}ms"
        );
    }
}

#[test]
fn test_timeout_caps_at_max_duration() {
    // 5ms timeout - should only allow a few iterations
    let handler = BusyHandler::Timeout(Duration::from_millis(5));
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // First iteration: 1ms delay (total: 1ms)
    assert!(state.invoke(&handler, now));
    // Second iteration: 2ms delay (total: 3ms)
    assert!(state.invoke(&handler, now));
    // Third iteration: would be 5ms but only 2ms left (total would be 8ms > 5ms)
    // So delay is capped to 2ms
    assert!(state.invoke(&handler, now));
    // Fourth iteration: no time left
    assert!(!state.invoke(&handler, now));
}

#[test]
fn test_state_reset() {
    let handler = BusyHandler::Timeout(Duration::from_millis(100));
    let now = test_instant();
    let mut state = BusyHandlerState::new(now);

    // Invoke a few times
    state.invoke(&handler, now);
    state.invoke(&handler, now);
    state.invoke(&handler, now);

    // Reset
    let later = MonotonicInstant::now();
    state.reset(later);

    // Should be back to initial state
    assert_eq!(state.timeout(), later);
    assert!(state.invoke(&handler, later));
    // First delay after reset should be 1ms
    assert_eq!(state.timeout(), later + Duration::from_millis(1));
}

#[test]
fn test_get_delay_when_timeout_passed() {
    let now = MonotonicInstant::now();
    let state = BusyHandlerState::new(now);

    // Timeout is at `now`, so any time >= now should return zero delay
    assert_eq!(state.get_delay(now), Duration::ZERO);

    // A later time should also return zero
    std::thread::sleep(Duration::from_micros(10));
    let later = MonotonicInstant::now();
    assert_eq!(state.get_delay(later), Duration::ZERO);
}

#[test]
fn test_get_delay_calculates_remaining_time() {
    let now = MonotonicInstant::now();
    let mut state = BusyHandlerState::new(now);

    let handler = BusyHandler::Timeout(Duration::from_millis(100));
    state.invoke(&handler, now); // Sets timeout to now + 1ms

    // Check delay from `now` - should be 1ms
    let delay = state.get_delay(now);
    assert_eq!(delay, Duration::from_millis(1));
}
