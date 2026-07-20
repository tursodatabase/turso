use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};

/// Execute the closure `f` with panic safety.
///
/// This is a placeholder for proper GVL release using
/// `rb_thread_call_without_gvl2` from Ruby's C API via `rb-sys`.
/// The GVL release will be implemented in a follow-up once the
/// extension compiles and runs.
///
/// For now, this provides:
/// 1. Panic catching - Rust panics are caught and returned as errors
/// 2. Interruption support via the `cancelled` flag
#[allow(dead_code)]
pub fn without_gvl<F, R>(cancelled: &AtomicBool, f: F) -> Result<R, String>
where
    F: FnOnce() -> R,
{
    if cancelled.load(Ordering::Acquire) {
        return Err("operation was cancelled".to_string());
    }

    match panic::catch_unwind(panic::AssertUnwindSafe(f)) {
        Ok(val) => Ok(val),
        Err(e) => {
            let msg = if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "panic in native code".to_string()
            };
            Err(msg)
        }
    }
}
