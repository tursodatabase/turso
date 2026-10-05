macro_rules! trace_stack {
    () => {
        let _stack = $crate::stack::trace_scope(concat!("line:", line!()));
    };
    ($label:expr $(,)?) => {
        let _stack = $crate::stack::trace_scope($label);
    };
    ($label:expr, $detail:expr $(,)?) => {
        let _stack = $crate::stack::trace_scope_with_detail($label, $detail);
    };
}
pub(crate) use trace_stack;

const DEBUG_BUILD_MINIMUM_STACK_SIZE: usize = 1024 * 1024;
const DEBUG_BUILD_STACK_ALLOCATION_SIZE: usize = 8 * 1024 * 1024;

pub(crate) fn configure_stack_growth() {
    if cfg!(debug_assertions) {
        recursive::set_minimum_stack_size(DEBUG_BUILD_MINIMUM_STACK_SIZE);
        recursive::set_stack_allocation_size(DEBUG_BUILD_STACK_ALLOCATION_SIZE);
    }
}

#[cfg(feature = "stacker")]
pub(crate) struct TraceGuard {
    label: &'static str,
    detail: Option<&'static str>,
}

#[cfg(feature = "stacker")]
impl TraceGuard {
    fn emit(&self, phase: &'static str) {
        if std::env::var_os("TURSO_TRACE_STACK").is_none() {
            return;
        }

        tracing::debug!(
            target: "turso_stack",
            label = self.label,
            detail = self.detail,
            phase,
            remaining_stack = stacker::remaining_stack(),
            "stacker remaining stack"
        );
    }
}

#[cfg(feature = "stacker")]
impl Drop for TraceGuard {
    fn drop(&mut self) {
        self.emit("exit");
    }
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_scope(label: &'static str) -> TraceGuard {
    let guard = TraceGuard {
        label,
        detail: None,
    };
    guard.emit("enter");
    guard
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_scope_with_detail(label: &'static str, detail: &'static str) -> TraceGuard {
    let guard = TraceGuard {
        label,
        detail: Some(detail),
    };
    guard.emit("enter");
    guard
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_remaining(label: &'static str) {
    if std::env::var_os("TURSO_TRACE_STACK").is_none() {
        return;
    }

    tracing::debug!(
        target: "turso_stack",
        label,
        phase = "sample",
        remaining_stack = stacker::remaining_stack(),
        "stacker remaining stack"
    );
}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_scope(_label: &'static str) {}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_scope_with_detail(_label: &'static str, _detail: &'static str) {}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_remaining(_label: &'static str) {}
