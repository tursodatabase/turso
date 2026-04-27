#[cfg(feature = "stacker")]
use std::borrow::Cow;

#[cfg(feature = "stacker")]
pub(crate) struct TraceGuard {
    label: &'static str,
    detail: Option<Cow<'static, str>>,
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
            detail = self.detail.as_deref(),
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
        detail: Some(Cow::Borrowed(detail)),
    };
    guard.emit("enter");
    guard
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_scope_with_dynamic_detail(
    label: &'static str,
    detail: impl FnOnce() -> String,
) -> TraceGuard {
    let guard = TraceGuard {
        label,
        detail: Some(Cow::Owned(detail())),
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
pub(crate) fn trace_scope_with_dynamic_detail(
    _label: &'static str,
    _detail: impl FnOnce() -> String,
) {
}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_remaining(_label: &'static str) {}
