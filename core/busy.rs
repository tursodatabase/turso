use crate::Instant;
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
    timeout: Instant,
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
    pub fn new(now: Instant) -> Self {
        Self {
            invocation_count: 0,
            timeout: now,
            iteration: 0,
        }
    }

    /// Reset the state for a new locking event
    pub fn reset(&mut self, now: Instant) {
        self.invocation_count = 0;
        self.timeout = now;
        self.iteration = 0;
    }

    /// Get the current timeout instant
    pub fn timeout(&self) -> Instant {
        self.timeout
    }

    /// Invoke the busy handler and determine whether to retry.
    ///
    /// Returns `true` if the operation should be retried, `false` if SQLITE_BUSY
    /// should be returned to the application.
    ///
    /// For timeout-based handlers, this also updates the internal timeout instant.
    /// For custom handlers, this invokes the callback and respects its return value.
    pub fn invoke(&mut self, handler: &BusyHandler, now: Instant) -> bool {
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
    fn invoke_timeout_handler(&mut self, max_duration: Duration, now: Instant) -> bool {
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
    pub fn get_delay(&self, now: Instant) -> Duration {
        if now.secs > self.timeout.secs
            || (now.secs == self.timeout.secs && now.micros >= self.timeout.micros)
        {
            Duration::ZERO
        } else {
            let secs_diff = (self.timeout.secs - now.secs) as u64;
            let micros_diff = if self.timeout.micros >= now.micros {
                self.timeout.micros - now.micros
            } else {
                // Borrow from seconds
                return Duration::from_secs(secs_diff.saturating_sub(1))
                    + Duration::from_micros((1_000_000 + self.timeout.micros - now.micros) as u64);
            };
            Duration::from_secs(secs_diff) + Duration::from_micros(micros_diff as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_instant() -> Instant {
        Instant { secs: 0, micros: 0 }
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
}
