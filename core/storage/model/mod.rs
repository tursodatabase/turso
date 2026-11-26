//! Model storage: an in-memory implementation of the cursor contract
//! (`tests/integration/cursor_contract/SPEC.md`). It is the spec in code, an oracle for SQL
//! results, and a checker that panics with `unsupported: U<n>: ...` on an Undefined sequence.
//!
//! It exists only in debug builds. With `TURSO_MODEL_EPHEMERAL=1`, new connections open their
//! ephemeral trees (`OpenEphemeral`, `OpenAutoindex`, `OpenDup`) on it.

mod cursor;
pub mod spec;

pub use cursor::ModelCursor;

use crate::sync::OnceLock;

/// The default of the per-connection flag that opens ephemeral trees on the model.
pub fn ephemeral_default() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("TURSO_MODEL_EPHEMERAL").is_ok_and(|v| v == "1"))
}

/// Debug aid: `TURSO_MODEL_PEER_RESTORE=1` turns U9 off. A cursor keeps its position by key
/// after another cursor writes, as SQLite does.
pub(crate) fn peer_restore() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("TURSO_MODEL_PEER_RESTORE").is_ok_and(|v| v == "1"))
}

/// Counters that show how much of the model a run used. A process that opened a model tree
/// prints them to stderr when it exits.
mod stats {
    use crate::sync::{LazyLock, Mutex};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TABLES: AtomicU64 = AtomicU64::new(0);
    static INDEXES: AtomicU64 = AtomicU64::new(0);
    static DUPS: AtomicU64 = AtomicU64::new(0);
    static STEPS: LazyLock<Mutex<BTreeMap<&'static str, u64>>> =
        LazyLock::new(|| Mutex::new(BTreeMap::new()));

    pub(super) fn tree_opened(index: bool) {
        report_at_exit();
        let counter = if index { &INDEXES } else { &TABLES };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn dup_opened() {
        DUPS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn step(name: &'static str) {
        *STEPS.lock().entry(name).or_insert(0) += 1;
    }

    #[cfg_attr(not(unix), allow(dead_code))]
    fn report() -> String {
        let steps: Vec<String> = STEPS
            .lock()
            .iter()
            .map(|(name, n)| format!("{name}={n}"))
            .collect();
        format!(
            "model storage: {} table trees, {} index trees, {} OpenDup cursors; steps: {}",
            TABLES.load(Ordering::Relaxed),
            INDEXES.load(Ordering::Relaxed),
            DUPS.load(Ordering::Relaxed),
            steps.join(" "),
        )
    }

    #[cfg(unix)]
    fn report_at_exit() {
        extern "C" fn print_report() {
            eprintln!("{}", report());
        }
        static ONCE: std::sync::Once = std::sync::Once::new();
        // SAFETY: `print_report` is a plain function that lives as long as the process.
        ONCE.call_once(|| unsafe {
            libc::atexit(print_report);
        });
    }

    #[cfg(not(unix))]
    fn report_at_exit() {}
}
