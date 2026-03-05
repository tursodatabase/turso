use std::time::Duration;

/// Sleep for a specified duration, yielding the async task.
///
/// Uses `futures_timer::Delay` under normal operation for proper async
/// integration. Under Shuttle deterministic testing, falls back to
/// `shuttle::thread::sleep` since Shuttle doesn't model async timers.
pub(crate) async fn busy_sleep(delay: Duration) {
    #[cfg(not(shuttle))]
    futures_timer::Delay::new(delay).await;
    #[cfg(shuttle)]
    shuttle::thread::sleep(delay);
}
