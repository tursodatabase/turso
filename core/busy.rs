use crate::MonotonicInstant;
use std::time::Duration;

/// Type alias for busy handler callback function.
///
/// The callback receives:
/// - `count`: The number of times the busy handler has been invoked for the same locking event
///
/// Returns:
/// - `0` to stop retrying and return SQLITE_BUSY to the application.
/// - Non-zero to retry the database access.
///
/// # Safety Notes (per SQLite spec)
/// - The callback MUST NOT modify the database connection that invoked it.
/// - The callback MUST NOT close the connection or any prepared statement.
/// - The callback is NOT reentrant.
pub type BusyHandlerCallback = Box<dyn Fn(i32) -> i32 + Send + Sync>;

#[derive(Default)]
/// Represents the busy handler configuration for a connection.
pub enum BusyHandler {
    #[default]
    /// No busy handler: return SQLITE_BUSY immediately on lock contention.
    None,
    /// Default timeout-based handler (implements sqliteDefaultBusyCallback)
    /// The duration is the maximum total time to wait before giving up
    Timeout(Duration),
    /// Custom user-defined callback handler
    Custom { callback: BusyHandlerCallback },
}

impl std::fmt::Debug for BusyHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BusyHandler::None => write!(f, "BusyHandler::None"),
            BusyHandler::Timeout(d) => write!(f, "BusyHandler::Timeout({d:?}"),
            BusyHandler::Custom { .. } => write!(f, "BusyHandler::Custom"),
        }
    }
}

/// Tracks the state of busy handler invocations for a statement.
///
/// This implements a yield-based busy handling mechanism that integrates with
/// the async event loop. Instead of blocking with `thread::sleep`, the statement
/// yields back to the caller with `StepResult::IO` and a timeout. When `step()`
/// is called again after the timeout has passed, it retries the operation.
///
/// Uses increasing delays. After 12 iterations, continues with 100ms delays until max duration is reached.
#[derive(Debug)]
pub struct BusyHandlerState {
    /// Number of times the busy handler has been invoked for this locking event
    invocation_count: i32,
    /// For timeout-based handlers: the next timeout instant to wait until
    timeout: MonotonicInstant,
    /// For timeout-based handlers: the current iteration index into DELAYS
    iteration: usize,
}

impl BusyHandlerState {
    /// Delay schedule for timeout-based busy handler (sqliteDefaultBusyCallback)
    const DELAYS: [Duration; 12] = [
        Duration::from_millis(1),
        Duration::from_millis(2),
        Duration::from_millis(5),
        Duration::from_millis(10),
        Duration::from_millis(15),
        Duration::from_millis(20),
        Duration::from_millis(25),
        Duration::from_millis(25),
        Duration::from_millis(25),
        Duration::from_millis(50),
        Duration::from_millis(50),
        Duration::from_millis(100),
    ];

    /// Cumulative totals for each iteration (for calculating remaining time)
    const TOTALS: [Duration; 12] = [
        Duration::from_millis(0),
        Duration::from_millis(1),
        Duration::from_millis(3),
        Duration::from_millis(8),
        Duration::from_millis(18),
        Duration::from_millis(33),
        Duration::from_millis(53),
        Duration::from_millis(78),
        Duration::from_millis(103),
        Duration::from_millis(128),
        Duration::from_millis(178),
        Duration::from_millis(228),
    ];

    /// Create a new busy handler state
    pub fn new(now: MonotonicInstant) -> Self {
        Self {
            invocation_count: 0,
            timeout: now,
            iteration: 0,
        }
    }

    /// Reset the state for a new locking event
    pub fn reset(&mut self, now: MonotonicInstant) {
        self.invocation_count = 0;
        self.timeout = now;
        self.iteration = 0;
    }

    /// Get the current timeout instant
    pub fn timeout(&self) -> MonotonicInstant {
        self.timeout
    }

    /// Invoke the busy handler and determine whether to retry.
    ///
    /// Returns `true` if the operation should be retried, `false` if SQLITE_BUSY
    /// should be returned to the application.
    ///
    /// For timeout-based handlers, this also updates the internal timeout instant.
    /// For custom handlers, this invokes the callback and respects its return value.
    pub fn invoke(&mut self, handler: &BusyHandler, now: MonotonicInstant) -> bool {
        match handler {
            BusyHandler::None => {
                // No handler: return BUSY immediately
                false
            }
            BusyHandler::Timeout(max_duration) => self.invoke_timeout_handler(*max_duration, now),
            BusyHandler::Custom { callback } => {
                let result = callback(self.invocation_count);
                self.invocation_count += 1;
                if result != 0 {
                    // Retry with a small delay
                    self.timeout = now + Duration::from_millis(1);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Implements sqliteDefaultBusyCallback logic for timeout-based handling.
    ///
    /// This uses an exponentially increasing delay schedule, capped at 100ms per iteration.
    fn invoke_timeout_handler(&mut self, max_duration: Duration, now: MonotonicInstant) -> bool {
        let idx = self.iteration.min(11);
        let mut delay = Self::DELAYS[idx];
        let mut prior = Self::TOTALS[idx];

        // After 12 iterations, each additional iteration adds 100ms
        if self.iteration >= 12 {
            prior += delay * (self.iteration as u32 - 11);
        }

        // Check if we've exceeded or would exceed the max duration
        if prior + delay > max_duration {
            delay = max_duration.saturating_sub(prior);
            if delay.is_zero() {
                return false;
            }
        }

        self.iteration = self.iteration.saturating_add(1);
        self.invocation_count += 1;
        self.timeout = now + delay;
        true
    }

    /// Get the delay duration that should be waited before the next retry.
    ///
    /// This returns the duration between `now` and the timeout instant.
    /// Returns `Duration::ZERO` if the timeout has already passed.
    pub fn get_delay(&self, now: MonotonicInstant) -> Duration {
        if now >= self.timeout {
            Duration::ZERO
        } else {
            self.timeout.duration_since(now)
        }
    }
}

#[cfg(test)]
mod tests {
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
}
