//! Deterministic regression repros for the non-blocking MVCC checkpoint.
//!
//! These drive the in-process (fiber-scheduled, deterministic) whopper binary at
//! pinned seeds under MVCC chaos with frequent Passive auto-checkpoints (the
//! `Whopper::open` MVCC path forces a low `mvcc_checkpoint_threshold` so the
//! off-lock checkpoint runs constantly). Each pinned seed exposes a checkpoint
//! correctness bug that the integrity_check / property layer catches.
//!
//! They are `#[ignore]`d because the underlying bug is not yet fixed (Task 9:
//! fully off-lock write phase + deferral-aware invariants). They are the acceptance
//! criteria for that fix — run with `cargo test -p turso_whopper --test
//! mvcc_checkpoint_regression -- --ignored` and remove `#[ignore]` once green.
//!
//! Determinism: seeds reproduce identically on re-run (verified). The binary path
//! is provided by Cargo via `CARGO_BIN_EXE_turso_whopper`.

use std::process::Command;

/// Run the whopper binary at `seed` in MVCC chaos mode and return Ok(()) on a
/// clean exit, or Err(stderr-tail) on failure.
fn run_whopper_seed(seed: u64) -> Result<(), String> {
    let bin = env!("CARGO_BIN_EXE_turso_whopper");
    let output = Command::new(bin)
        .env("SEED", seed.to_string())
        .args([
            "--mode",
            "chaos",
            "--enable-mvcc",
            "--max-connections",
            "4",
            "--max-steps",
            "8000",
        ])
        .output()
        .expect("failed to spawn turso_whopper");
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Surface the property failure / panic line, skipping the busy-spam.
        let signal = stderr
            .lines()
            .chain(stdout.lines())
            .find(|l| {
                let l = l.to_lowercase();
                (l.contains("missing from index")
                    || l.contains("wrong # of entries")
                    || l.contains("panicked")
                    || l.contains("property failed"))
                    && !l.contains("auto-checkpoint failed")
            })
            .unwrap_or("(no recognizable failure line)");
        Err(format!("seed {seed} exited {:?}: {signal}", output.status))
    }
}

/// Index inconsistency: after an off-lock Passive checkpoint, a row is present in
/// the table btree but its (auto)index entry is missing — `integrity_check` reports
/// "row N missing from index" / "wrong # of entries in index".
/// Seeds 1 and 32 reproduce this deterministically.
#[ignore = "reproduces non-blocking-checkpoint index inconsistency (Task 9); un-ignore when fixed"]
#[test]
fn mvcc_checkpoint_index_inconsistency_seed_1() {
    run_whopper_seed(1).expect("checkpoint must not orphan index entries");
}

#[ignore = "reproduces non-blocking-checkpoint index inconsistency (Task 9); un-ignore when fixed"]
#[test]
fn mvcc_checkpoint_index_inconsistency_seed_32() {
    run_whopper_seed(32).expect("checkpoint must not orphan index entries");
}

/// Concurrent CREATE after the checkpoint's snapshot trips the over-strict
/// `has_pending_root_publication()` assert in `TruncateWal`. Seeds 28 and 60
/// reproduce this. (Also covered by the surgical unit test
/// `test_passive_checkpoint_tolerates_concurrent_create_after_snapshot` in
/// core/mvcc/database/tests.rs.)
// Fixed: `has_unpublished_schema_changes` is now deferral-aware. These pass.
#[test]
fn mvcc_checkpoint_concurrent_create_assert_seed_28() {
    run_whopper_seed(28).expect("checkpoint must tolerate a CREATE committed after its snapshot");
}

#[test]
fn mvcc_checkpoint_concurrent_create_assert_seed_60() {
    run_whopper_seed(60).expect("checkpoint must tolerate a CREATE committed after its snapshot");
}
