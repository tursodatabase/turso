//! Per-function stack profiler.
//!
//! Wrap functions with `stack_probe!("name")` (RAII). When the profiler is
//! enabled at runtime via [`set_enabled`], each probe records the stack
//! pointer at entry and the deepest sp observed via nested child probes.
//! [`drain`] returns the resulting call tree.
//!
//! Cost when **disabled**: one relaxed atomic load + an `Option::None` return.
//! Cost when **enabled**: a thread-local borrow + small Vec push/pop per probe.
//!
//! Limitations:
//! - We sample sp only at probed call boundaries. The "depth" attributed to
//!   a frame is the difference between its own sp at entry and the lowest sp
//!   any of its (probed) descendants reported. Unprobed leaf work is invisible.
//! - Drop the deepest activation BEFORE calling [`drain`]; reading a
//!   half-built tree returns `None`.

use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);

#[inline]
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
    if !on {
        PROFILER.with(|p| p.borrow_mut().reset());
    }
}

/// Reset the profiler state regardless of enable bit. Call before a
/// measurement window if you want a clean tree.
pub fn reset() {
    PROFILER.with(|p| p.borrow_mut().reset());
}

#[derive(Clone, Debug)]
pub struct ProfFrame {
    pub name: Cow<'static, str>,
    pub sp_enter: usize,
    pub min_sp: usize,
    pub children: Vec<ProfFrame>,
    pub call_count: usize,
}

impl ProfFrame {
    pub fn depth_bytes(&self) -> usize {
        self.sp_enter.saturating_sub(self.min_sp)
    }

    /// Merge another frame with the same name into this one. Used to
    /// aggregate per-call siblings (e.g. 1000 op_add invocations under
    /// vdbe_step → one row showing max depth × 1000 calls).
    fn merge_in(&mut self, other: ProfFrame) {
        debug_assert_eq!(self.name, other.name);
        self.call_count += other.call_count.max(1);
        if other.min_sp < self.min_sp {
            self.min_sp = other.min_sp;
        }
        // Keep the original sp_enter as representative.
        for other_child in other.children {
            if let Some(existing) = self
                .children
                .iter_mut()
                .find(|c| c.name == other_child.name)
            {
                existing.merge_in(other_child);
            } else {
                self.children.push(other_child);
            }
        }
    }
}

#[derive(Default)]
struct Profiler {
    stack: Vec<ProfFrame>,
    completed_roots: Vec<ProfFrame>,
}

impl Profiler {
    fn enter(&mut self, name: Cow<'static, str>, sp: usize) {
        // Update parent's deepest sp (we are below them).
        if let Some(top) = self.stack.last_mut() {
            if sp < top.min_sp {
                top.min_sp = sp;
            }
        }
        self.stack.push(ProfFrame {
            name,
            sp_enter: sp,
            min_sp: sp,
            children: Vec::new(),
            call_count: 1,
        });
    }

    fn exit(&mut self) {
        let Some(frame) = self.stack.pop() else {
            return;
        };
        if let Some(parent) = self.stack.last_mut() {
            if frame.min_sp < parent.min_sp {
                parent.min_sp = frame.min_sp;
            }
            // Aggregate by name so loops (e.g. vdbe dispatch) don't produce
            // thousands of sibling nodes.
            if let Some(sibling) = parent.children.iter_mut().find(|c| c.name == frame.name) {
                sibling.merge_in(frame);
            } else {
                parent.children.push(frame);
            }
        } else {
            // Outermost probe just finished — record this tree alongside any
            // earlier completed top-level trees.
            if let Some(prev) = self
                .completed_roots
                .iter_mut()
                .find(|r| r.name == frame.name)
            {
                prev.merge_in(frame);
            } else {
                self.completed_roots.push(frame);
            }
        }
    }

    fn reset(&mut self) {
        self.stack.clear();
        self.completed_roots.clear();
    }

    fn take_all(&mut self) -> Vec<ProfFrame> {
        std::mem::take(&mut self.completed_roots)
    }
}

thread_local! {
    static PROFILER: RefCell<Profiler> = RefCell::new(Profiler::default());
}

pub struct StackProbe {
    _private: (),
}

impl Drop for StackProbe {
    #[inline]
    fn drop(&mut self) {
        PROFILER.with(|p| p.borrow_mut().exit());
    }
}

/// Internal entry point used by the `stack_probe!` macro. The macro captures
/// `sp` at the call site so the probe doesn't perturb the measured frame.
#[doc(hidden)]
#[inline]
pub fn __enter_probe(name: &'static str, sp: usize) -> Option<StackProbe> {
    if !is_enabled() {
        return None;
    }
    PROFILER.with(|p| p.borrow_mut().enter(Cow::Borrowed(name), sp));
    Some(StackProbe { _private: () })
}

#[doc(hidden)]
#[inline]
pub fn __enter_probe_owned(name: String, sp: usize) -> Option<StackProbe> {
    if !is_enabled() {
        return None;
    }
    PROFILER.with(|p| p.borrow_mut().enter(Cow::Owned(name), sp));
    Some(StackProbe { _private: () })
}

/// Insert a probe with a `&'static str` name. Lives for the rest of the
/// enclosing scope.
#[macro_export]
macro_rules! stack_probe {
    ($name:expr) => {
        let __stack_probe_guard = {
            let __probe_local: u32 = 0;
            let __sp = std::hint::black_box(&__probe_local) as *const u32 as usize;
            $crate::stack_profile::__enter_probe($name, __sp)
        };
    };
}

/// Insert a probe with a runtime-constructed name. Allocates a `String` per
/// call when profiling is enabled; nil cost when disabled.
#[macro_export]
macro_rules! stack_probe_dyn {
    ($name:expr) => {
        let __stack_probe_guard = if $crate::stack_profile::is_enabled() {
            let __probe_local: u32 = 0;
            let __sp = std::hint::black_box(&__probe_local) as *const u32 as usize;
            $crate::stack_profile::__enter_probe_owned($name.to_string(), __sp)
        } else {
            None
        };
    };
}

/// Take all completed call trees in entry order. A single user-level query
/// can produce multiple trees if it triggers nested top-level `translate()`
/// calls (e.g. a PRAGMA whose result is rendered via a follow-up SELECT).
pub fn drain_all() -> Vec<ProfFrame> {
    PROFILER.with(|p| p.borrow_mut().take_all())
}

/// Convenience: take just the last completed tree, if any.
pub fn drain() -> Option<ProfFrame> {
    drain_all().pop()
}

/// Pretty-print the call tree.
///
/// `total` = bytes the deepest probed descendant reached below this frame.
/// `self`  = bytes consumed between this frame's entry and its deepest probed
///           child's entry — i.e. the contribution of this function's own body
///           and the call that follows it. The high `self` rows are the
///           functions to investigate first.
///
/// Rows whose `self` exceeds [`HIGHLIGHT_SELF_BYTES`] are printed in bold red
/// when stdout is a TTY (or unconditionally if `TURSO_STACK_FORCE_COLOR=1`).
pub fn format_tree(root: &ProfFrame) -> String {
    const NAME_W: usize = 50;
    let mut out = String::new();
    out.push_str(&format!(
        "stack profile (peak: {})\n  {:<NAME_W$} {:>10} {:>10} {:>8}\n",
        fmt_bytes(root.depth_bytes()),
        "frame",
        "total",
        "self",
        "calls",
    ));
    out.push_str(&format!("  {}\n", "-".repeat(NAME_W + 31)));
    fmt_root(root, &mut out);
    out
}

/// Rows with `self` ≥ this threshold are highlighted in the formatter.
pub const HIGHLIGHT_SELF_BYTES: usize = 4 * 1024;

/// Format bytes as B / KiB / MiB with one decimal place once we reach KiB.
fn fmt_bytes(b: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * 1024;
    if b < KIB {
        format!("{b} B")
    } else if b < MIB {
        format!("{:.1} KiB", b as f64 / KIB as f64)
    } else {
        format!("{:.2} MiB", b as f64 / MIB as f64)
    }
}

fn use_color() -> bool {
    use std::sync::OnceLock;
    static CACHE: OnceLock<bool> = OnceLock::new();
    *CACHE.get_or_init(|| {
        if std::env::var_os("TURSO_STACK_FORCE_COLOR").is_some() {
            return true;
        }
        if std::env::var_os("NO_COLOR").is_some() {
            return false;
        }
        // Stdout-is-tty heuristic without pulling a crate in.
        unsafe { libc_isatty(1) }
    })
}

#[cfg(unix)]
unsafe fn libc_isatty(fd: i32) -> bool {
    extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    isatty(fd) != 0
}

#[cfg(not(unix))]
unsafe fn libc_isatty(_fd: i32) -> bool {
    false
}

fn self_bytes(node: &ProfFrame) -> usize {
    let max_child = node
        .children
        .iter()
        .map(|c| c.depth_bytes())
        .max()
        .unwrap_or(0);
    node.depth_bytes().saturating_sub(max_child)
}

fn fmt_row(out: &mut String, name: &str, total: usize, self_b: usize, calls: usize) {
    const NAME_W: usize = 50;
    let calls_str = if calls > 1 {
        format!("× {calls}")
    } else {
        String::new()
    };
    let body = format!(
        "{:<NAME_W$} {:>10} {:>10} {:>8}",
        name,
        fmt_bytes(total),
        fmt_bytes(self_b),
        calls_str
    );
    if self_b >= HIGHLIGHT_SELF_BYTES && use_color() {
        // Bold red — stands out without being garish. Reset BEFORE the newline
        // so the colored region doesn't span line boundaries.
        out.push_str("\x1b[1;31m");
        out.push_str(&body);
        out.push_str("\x1b[0m\n");
    } else if self_b >= HIGHLIGHT_SELF_BYTES {
        // No-color fallback: annotate with `*` so piped output still shows hot rows.
        out.push_str(&body);
        out.push_str("  *\n");
    } else {
        out.push_str(&body);
        out.push('\n');
    }
}

fn fmt_root(node: &ProfFrame, out: &mut String) {
    fmt_row(
        out,
        &format!("  {}", node.name),
        node.depth_bytes(),
        self_bytes(node),
        node.call_count,
    );
    // Sort children by depth descending so the heavy hitters surface first.
    let mut children: Vec<&ProfFrame> = node.children.iter().collect();
    children.sort_by_key(|c| std::cmp::Reverse(c.depth_bytes()));
    let n = children.len();
    for (i, c) in children.iter().enumerate() {
        fmt_child(c, out, "  ", i + 1 == n);
    }
}

fn fmt_child(node: &ProfFrame, out: &mut String, parent_prefix: &str, is_last: bool) {
    let connector = if is_last { "└─ " } else { "├─ " };
    let name = format!("{parent_prefix}{connector}{}", node.name);
    fmt_row(
        out,
        &name,
        node.depth_bytes(),
        self_bytes(node),
        node.call_count,
    );
    let child_prefix = format!(
        "{parent_prefix}{}",
        if is_last { "   " } else { "│  " }
    );
    let mut children: Vec<&ProfFrame> = node.children.iter().collect();
    children.sort_by_key(|c| std::cmp::Reverse(c.depth_bytes()));
    let n = children.len();
    for (i, c) in children.iter().enumerate() {
        fmt_child(c, out, &child_prefix, i + 1 == n);
    }
}
