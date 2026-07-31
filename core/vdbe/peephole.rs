//! Peephole optimizer for finished bytecode programs.
//!
//! [`optimize_program`] runs on every prepared program right after label
//! resolution ([`super::builder::ProgramBuilder::resolve_labels`]), so every
//! branch operand is already a concrete [`BranchOffset::Offset`]. It applies a
//! fixed list of rewrite rules, marks instructions dead, and then compacts the
//! instruction list once, remapping every branch operand through the shared
//! [`Insn::for_each_branch_offset`] visitor.
//!
//! The rules, in order:
//! 1. Jump threading: retarget every branch operand that lands on a `Goto` to
//!    that `Goto`'s final destination. A `Goto` whose final destination is a
//!    plain `Halt` becomes a copy of that `Halt`.
//! 2. Branch-over-Goto inversion: `If x -> +2; Goto L` becomes `IfNot x -> L`.
//! 3. Copy merge: adjacent `Copy`s over contiguous register ranges become one
//!    `Copy` with a larger `extra_amount`.
//! 4. Affinity fold: a standalone `Affinity` directly before a `MakeRecord`
//!    over the same register range moves its affinity string onto the
//!    `MakeRecord`.
//! 5. Delete jumps to the next instruction.
//! 6. Delete instructions that no path from instruction 0 can reach.
//!
//! Set `TURSO_PEEPHOLE=0` in the environment to disable the pass (read once
//! per process). Set `TURSO_PEEPHOLE_STATS=1` to print cumulative rule-hit
//! counters when the process exits (unix only).
//!
//! The pass never runs for `EXPLAIN QUERY PLAN` programs: those consist of
//! `Insn::Explain` rows that carry raw instruction indices which compaction
//! would invalidate, and they are never executed for performance.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::turso_debug_assert;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::insn::Insn;
use crate::vdbe::{BranchOffset, InsnReference};

/// Process-wide rule-hit counters.
///
/// These use `std::sync::atomic` directly (not `crate::sync`) because they are
/// diagnostic-only and must be const-initializable in a static, which the
/// shuttle atomics are not.
#[derive(Debug)]
pub struct PeepholeCounters {
    /// Programs the pass ran on.
    pub programs: AtomicU64,
    /// Branch operands retargeted through one or more `Goto`s (rule 1).
    pub jumps_threaded: AtomicU64,
    /// `Goto`s replaced by a copy of the `Halt` they jump to (rule 1).
    pub gotos_replaced_with_halt: AtomicU64,
    /// Conditional branches inverted over a following `Goto` (rule 2).
    pub branches_inverted: AtomicU64,
    /// `Copy` instructions merged into a preceding `Copy` (rule 3).
    pub copies_merged: AtomicU64,
    /// `Affinity` instructions folded into a following `MakeRecord` (rule 4).
    pub affinities_folded: AtomicU64,
    /// Jumps to the next instruction deleted (rule 5).
    pub jumps_to_next_deleted: AtomicU64,
    /// Unreachable instructions deleted (rule 6).
    pub unreachable_deleted: AtomicU64,
    /// Comparison jumps to the next instruction kept because deleting them
    /// would also delete their affinity conversion of the operand registers
    /// (rule 5 safety skip).
    pub cmp_deletes_skipped: AtomicU64,
    /// Affinity/MakeRecord pairs left alone because an instruction between
    /// them could observe the register range, or one of the instructions is a
    /// jump target (rule 4 safety skip).
    pub affinity_folds_skipped: AtomicU64,
}

pub static COUNTERS: PeepholeCounters = PeepholeCounters {
    programs: AtomicU64::new(0),
    jumps_threaded: AtomicU64::new(0),
    gotos_replaced_with_halt: AtomicU64::new(0),
    branches_inverted: AtomicU64::new(0),
    copies_merged: AtomicU64::new(0),
    affinities_folded: AtomicU64::new(0),
    jumps_to_next_deleted: AtomicU64::new(0),
    unreachable_deleted: AtomicU64::new(0),
    cmp_deletes_skipped: AtomicU64::new(0),
    affinity_folds_skipped: AtomicU64::new(0),
};

/// Per-run counters, folded into [`COUNTERS`] once per program.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PassStats {
    pub jumps_threaded: u64,
    pub gotos_replaced_with_halt: u64,
    pub branches_inverted: u64,
    pub copies_merged: u64,
    pub affinities_folded: u64,
    pub jumps_to_next_deleted: u64,
    pub unreachable_deleted: u64,
    pub cmp_deletes_skipped: u64,
    pub affinity_folds_skipped: u64,
}

impl PassStats {
    fn total_rewrites(&self) -> u64 {
        self.jumps_threaded
            + self.gotos_replaced_with_halt
            + self.branches_inverted
            + self.copies_merged
            + self.affinities_folded
            + self.jumps_to_next_deleted
            + self.unreachable_deleted
    }

    fn add_to_global(&self) {
        // Most programs leave most counters at zero; skip those RMWs.
        fn add(counter: &AtomicU64, value: u64) {
            if value != 0 {
                counter.fetch_add(value, Ordering::Relaxed);
            }
        }
        let c = &COUNTERS;
        c.programs.fetch_add(1, Ordering::Relaxed);
        add(&c.jumps_threaded, self.jumps_threaded);
        add(&c.gotos_replaced_with_halt, self.gotos_replaced_with_halt);
        add(&c.branches_inverted, self.branches_inverted);
        add(&c.copies_merged, self.copies_merged);
        add(&c.affinities_folded, self.affinities_folded);
        add(&c.jumps_to_next_deleted, self.jumps_to_next_deleted);
        add(&c.unreachable_deleted, self.unreachable_deleted);
        add(&c.cmp_deletes_skipped, self.cmp_deletes_skipped);
        add(&c.affinity_folds_skipped, self.affinity_folds_skipped);
    }
}

/// Whether the pass runs at all. `TURSO_PEEPHOLE=0` (or `false`/`off`)
/// disables it; read once per process.
static ENABLED_FROM_ENV: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| match std::env::var("TURSO_PEEPHOLE") {
        Ok(v) => !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off")),
        Err(_) => true,
    });

#[cfg(test)]
thread_local! {
    /// Test-only override so one process can prepare the same statement with
    /// the pass on and off and compare the bytecode.
    static ENABLED_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn enabled() -> bool {
    #[cfg(test)]
    if let Some(v) = ENABLED_OVERRIDE.with(|c| c.get()) {
        return v;
    }
    *ENABLED_FROM_ENV
}

/// Force the pass on or off for statements prepared on this thread;
/// `None` restores the environment default.
#[cfg(test)]
pub(crate) fn set_enabled_for_current_thread(enabled: Option<bool>) {
    ENABLED_OVERRIDE.with(|c| c.set(enabled));
}

/// Print cumulative counters when the process exits, if
/// `TURSO_PEEPHOLE_STATS=1` is set. Unix only (uses libc::atexit).
fn maybe_register_stats_dump() {
    #[cfg(unix)]
    {
        static STATS_ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
            std::env::var("TURSO_PEEPHOLE_STATS").is_ok_and(|v| v == "1")
        });
        static REGISTER: std::sync::Once = std::sync::Once::new();
        if *STATS_ENABLED {
            REGISTER.call_once(|| unsafe {
                libc::atexit(dump_stats_at_exit);
            });
        }
    }
}

#[cfg(unix)]
extern "C" fn dump_stats_at_exit() {
    let c = &COUNTERS;
    eprintln!(
        "peephole stats: programs={} jumps_threaded={} gotos_replaced_with_halt={} \
         branches_inverted={} copies_merged={} affinities_folded={} jumps_to_next_deleted={} \
         unreachable_deleted={} cmp_deletes_skipped={} affinity_folds_skipped={}",
        c.programs.load(Ordering::Relaxed),
        c.jumps_threaded.load(Ordering::Relaxed),
        c.gotos_replaced_with_halt.load(Ordering::Relaxed),
        c.branches_inverted.load(Ordering::Relaxed),
        c.copies_merged.load(Ordering::Relaxed),
        c.affinities_folded.load(Ordering::Relaxed),
        c.jumps_to_next_deleted.load(Ordering::Relaxed),
        c.unreachable_deleted.load(Ordering::Relaxed),
        c.cmp_deletes_skipped.load(Ordering::Relaxed),
        c.affinity_folds_skipped.load(Ordering::Relaxed),
    );
}

/// Run the peephole pass on a finished, label-resolved instruction list.
///
/// `comments` maps instruction indices to EXPLAIN comments; entries move with
/// their instruction and entries for deleted instructions are dropped.
/// Reusable per-thread buffers. The pass runs on every prepare, often on
/// programs a handful of instructions long, where fresh allocations would
/// dominate its cost.
#[derive(Default)]
struct Scratch {
    /// Which instructions are still alive.
    live: Vec<bool>,
    /// Which instructions some branch operand points at.
    targets: Vec<bool>,
    /// Rule 1's "was a Goto" set, reused as the sweep's "reached" set.
    aux: Vec<bool>,
    /// Rule 1's chain resolution state per instruction.
    state: Vec<u8>,
    /// Rule 1's final chain destinations, reused as compaction's old-to-new map.
    u32_a: Vec<u32>,
    /// The sweep's next-live-instruction table.
    u32_b: Vec<u32>,
    /// Rule 1's current chain of Gotos.
    chain: Vec<u32>,
    /// The sweep's worklist.
    worklist: Vec<u32>,
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::default());
}

pub(crate) fn optimize_program(
    insns: &mut Vec<(Insn, usize)>,
    comments: &mut Vec<(InsnReference, &'static str)>,
) {
    let n = insns.len();
    if n == 0 {
        return;
    }
    maybe_register_stats_dump();
    SCRATCH.with(|cell| match cell.try_borrow_mut() {
        Ok(mut scratch) => run_pass(insns, comments, &mut scratch),
        // Defensive: if a prepare ever nests inside another prepare on this
        // thread, fall back to fresh buffers instead of sharing.
        Err(_) => run_pass(insns, comments, &mut Scratch::default()),
    });
}

fn run_pass(
    insns: &mut Vec<(Insn, usize)>,
    comments: &mut Vec<(InsnReference, &'static str)>,
    scratch: &mut Scratch,
) {
    let n = insns.len();
    let mut stats = PassStats::default();
    scratch.live.clear();
    scratch.live.resize(n, true);

    // Threading also fills `scratch.targets` (which instructions some branch
    // operand points at) in its operand walk: collected after retargeting and
    // before anything is marked dead, so every operand (including those of
    // instructions that later die) is included. The only rule that changes a
    // target afterwards is the inversion, and the destination it retargets to
    // is the dead Goto's destination, which that Goto's own operand already
    // put in the set. Staleness is therefore always conservative: it can only
    // block a rewrite.
    let present = thread_jumps(insns, scratch, &mut stats);

    let (live, targets) = (&mut scratch.live, &mut scratch.targets);
    if present.any_goto {
        // The inversion needs a Goto right after the conditional.
        invert_branches_over_gotos(insns, live, targets, &mut stats);
    }
    if present.any_copy {
        merge_adjacent_copies(insns, live, targets, &mut stats);
    }
    if present.any_affinity {
        fold_affinity_into_make_record(insns, live, targets, &mut stats);
    }
    delete_jumps_to_next(insns, live, targets, &mut stats);
    let dead_before_sweep = stats.branches_inverted
        + stats.copies_merged
        + stats.affinities_folded
        + stats.jumps_to_next_deleted
        > 0;
    sweep_unreachable(insns, scratch, dead_before_sweep, &mut stats);
    if dead_before_sweep || stats.unreachable_deleted > 0 {
        compact(insns, comments, scratch);
    }

    #[cfg(debug_assertions)]
    verify_all_branches_resolved(insns);

    if stats.total_rewrites() > 0 {
        tracing::debug!(
            insns_before = n,
            insns_after = insns.len(),
            ?stats,
            "peephole pass rewrote program"
        );
    }
    stats.add_to_global();
}

/// Return the branch target of a resolved operand, or `None` when the operand
/// is (unexpectedly) still a label or placeholder or out of bounds. Rules skip
/// such operands rather than corrupt them; the final debug verification
/// flags them.
fn resolved_target(off: &BranchOffset, n: usize) -> Option<u32> {
    match off {
        BranchOffset::Offset(t) if (*t as usize) < n => Some(*t),
        _ => None,
    }
}

/// Rule 1: jump threading.
///
/// For every branch operand pointing at a `Goto`, retarget it to the end of
/// the `Goto` chain. Chains are resolved once per `Goto` (linear total time)
/// with a cycle guard: any chain that reaches a `Goto` already on the current
/// chain (including a `Goto` to itself) is left alone.
///
/// Afterwards, any `Goto` that (now) points directly at a plain `Halt` is
/// replaced by a copy of that `Halt`: jumping to a `Halt` and executing it is
/// identical to executing it in place, since `Goto` has no other effect.
/// Which opcodes the program contains at all, so `run_pass` can skip rules
/// that cannot fire. Collected during rule 1's scans, which touch every
/// instruction anyway.
struct RulePresence {
    any_goto: bool,
    any_copy: bool,
    any_affinity: bool,
}

fn thread_jumps(
    insns: &mut [(Insn, usize)],
    scratch: &mut Scratch,
    stats: &mut PassStats,
) -> RulePresence {
    let n = insns.len();
    // was_goto[i]: instruction i was a Goto when chains were resolved. Kept
    // separately from a live `matches!` check so that replacing a Goto with a
    // Halt below does not stop later operands from threading through it.
    let was_goto = &mut scratch.aux;
    was_goto.clear();
    was_goto.resize(n, false);

    // For each Goto index: the ultimate non-Goto destination of its chain, or
    // its own index when the chain hits a cycle or an unresolved operand
    // (meaning: leave every operand that points here unchanged).
    const UNVISITED: u8 = 0;
    const IN_PROGRESS: u8 = 1;
    const DONE: u8 = 2;
    let final_target = &mut scratch.u32_a;
    final_target.clear();
    final_target.extend(0..n as u32);
    let state = &mut scratch.state;
    state.clear();
    state.resize(n, UNVISITED);
    let chain = &mut scratch.chain;
    let mut present = RulePresence {
        any_goto: false,
        any_copy: false,
        any_affinity: false,
    };
    for i in 0..n {
        match &insns[i].0 {
            Insn::Copy { .. } => {
                present.any_copy = true;
                continue;
            }
            Insn::Affinity { .. } => {
                present.any_affinity = true;
                continue;
            }
            Insn::Goto { .. } => {}
            _ => continue,
        }
        was_goto[i] = true;
        present.any_goto = true;
        if state[i] != UNVISITED {
            continue;
        }
        chain.clear();
        let mut cur = i as u32;
        let final_dst = loop {
            // Invariant: `cur` is an unvisited Goto.
            state[cur as usize] = IN_PROGRESS;
            chain.push(cur);
            let Insn::Goto { target_pc } = &insns[cur as usize].0 else {
                unreachable!("checked above");
            };
            let Some(t) = resolved_target(target_pc, n) else {
                break None;
            };
            if !matches!(insns[t as usize].0, Insn::Goto { .. }) {
                break Some(t);
            }
            match state[t as usize] {
                // A previously resolved chain: reuse its result. When that
                // chain was left alone (cycle), final_target[t] == t, which
                // correctly threads this chain up to the cycle entrance.
                DONE => break Some(final_target[t as usize]),
                IN_PROGRESS => break None, // cycle through the current chain
                _ => cur = t,
            }
        };
        for &g in chain.iter() {
            state[g as usize] = DONE;
            if let Some(d) = final_dst {
                final_target[g as usize] = d;
            }
        }
    }
    // Retarget every operand that lands on a Goto with a known final
    // destination, and record the (post-rewrite) target set for the rules
    // below in the same walk. Retargeting includes the Gotos' own operands,
    // so every Goto in a chain ends up pointing directly at the chain's
    // destination — which is why a Goto whose destination is a plain Halt can
    // become a copy of that Halt in the same walk: its own operand was just
    // rewritten, and operands of later instructions thread through
    // `was_goto`/`final_target` rather than re-inspecting the replaced
    // instruction.
    let targets = &mut scratch.targets;
    targets.clear();
    targets.resize(n, false);
    let threading = present.any_goto;
    for i in 0..n {
        if was_goto[i] {
            // A Goto has exactly one operand. Thread it, and if the final
            // destination is a plain Halt, become a copy of that Halt: the
            // operand disappears with the replacement, so it must not be
            // recorded as a target (that would keep an otherwise-unreachable
            // Halt alive). The chain destination is never itself a Goto
            // (cycles keep final_target[i] == i and are skipped by the
            // d != i check), so a Halt seen here is a real Halt, not a copy
            // this loop created.
            let Insn::Goto { target_pc } = &mut insns[i].0 else {
                unreachable!("was_goto is only set on Gotos and nothing replaced this one yet");
            };
            let Some(t) = resolved_target(target_pc, n) else {
                continue;
            };
            let d = if threading && was_goto[t as usize] {
                let d = final_target[t as usize];
                if d != t {
                    *target_pc = BranchOffset::Offset(d);
                    stats.jumps_threaded += 1;
                }
                d
            } else {
                t
            };
            if d as usize != i && matches!(insns[d as usize].0, Insn::Halt { .. }) {
                let halt = insns[d as usize].0.clone();
                insns[i].0 = halt;
                stats.gotos_replaced_with_halt += 1;
            } else {
                targets[d as usize] = true;
            }
            continue;
        }
        insns[i].0.for_each_branch_offset(|off| {
            let Some(t) = resolved_target(off, n) else {
                return;
            };
            if threading && was_goto[t as usize] {
                let d = final_target[t as usize];
                if d != t {
                    *off = BranchOffset::Offset(d);
                    stats.jumps_threaded += 1;
                    targets[d as usize] = true;
                    return;
                }
            }
            targets[t as usize] = true;
        });
    }
    present
}

/// The conditional opcodes rule 2 knows how to invert, paired up.
/// `NotExists`/`Found`/`NotFound`/`DecrJumpZero` and friends are deliberately
/// not invertible: their "jump" and "fall through" sides have different side
/// effects (cursor positioning, register decrements).
fn inverted_condition(insn: &Insn, new_target: BranchOffset) -> Option<Insn> {
    // For If/IfNot, `jump_if_null` decides whether a NULL register jumps; the
    // inverse must make the opposite decision for NULL, so the flag flips.
    //
    // For the comparisons, `CmpInsFlags::JUMP_IF_NULL` flips for the same
    // reason. `NULL_EQ` stays: it only affects Eq/Ne, and with it set, Eq
    // jumps iff `lhs == rhs` and Ne jumps iff `lhs != rhs` even when operands
    // are NULL (op_comparison consults NULL_EQ before JUMP_IF_NULL), so Eq
    // and Ne remain exact complements. Affinity and collation stay unchanged:
    // both sides of the comparison run the exact same conversion code, so an
    // inverted opcode applies the same register conversions the original did.
    match insn {
        Insn::If {
            reg,
            target_pc: _,
            jump_if_null,
        } => Some(Insn::IfNot {
            reg: *reg,
            target_pc: new_target,
            jump_if_null: !*jump_if_null,
        }),
        Insn::IfNot {
            reg,
            target_pc: _,
            jump_if_null,
        } => Some(Insn::If {
            reg: *reg,
            target_pc: new_target,
            jump_if_null: !*jump_if_null,
        }),
        Insn::IsNull { reg, target_pc: _ } => Some(Insn::NotNull {
            reg: *reg,
            target_pc: new_target,
        }),
        Insn::NotNull { reg, target_pc: _ } => Some(Insn::IsNull {
            reg: *reg,
            target_pc: new_target,
        }),
        Insn::Eq {
            lhs,
            rhs,
            target_pc: _,
            flags,
            collation,
        } => Some(Insn::Ne {
            lhs: *lhs,
            rhs: *rhs,
            target_pc: new_target,
            flags: flags.with_jump_if_null_flipped(),
            collation: *collation,
        }),
        Insn::Ne {
            lhs,
            rhs,
            target_pc: _,
            flags,
            collation,
        } => Some(Insn::Eq {
            lhs: *lhs,
            rhs: *rhs,
            target_pc: new_target,
            flags: flags.with_jump_if_null_flipped(),
            collation: *collation,
        }),
        Insn::Lt {
            lhs,
            rhs,
            target_pc: _,
            flags,
            collation,
        } => Some(Insn::Ge {
            lhs: *lhs,
            rhs: *rhs,
            target_pc: new_target,
            flags: flags.with_jump_if_null_flipped(),
            collation: *collation,
        }),
        Insn::Ge {
            lhs,
            rhs,
            target_pc: _,
            flags,
            collation,
        } => Some(Insn::Lt {
            lhs: *lhs,
            rhs: *rhs,
            target_pc: new_target,
            flags: flags.with_jump_if_null_flipped(),
            collation: *collation,
        }),
        Insn::Le {
            lhs,
            rhs,
            target_pc: _,
            flags,
            collation,
        } => Some(Insn::Gt {
            lhs: *lhs,
            rhs: *rhs,
            target_pc: new_target,
            flags: flags.with_jump_if_null_flipped(),
            collation: *collation,
        }),
        Insn::Gt {
            lhs,
            rhs,
            target_pc: _,
            flags,
            collation,
        } => Some(Insn::Le {
            lhs: *lhs,
            rhs: *rhs,
            target_pc: new_target,
            flags: flags.with_jump_if_null_flipped(),
            collation: *collation,
        }),
        _ => None,
    }
}

/// The single branch target of an invertible conditional.
fn invertible_condition_target(insn: &Insn) -> Option<&BranchOffset> {
    match insn {
        Insn::If { target_pc, .. }
        | Insn::IfNot { target_pc, .. }
        | Insn::IsNull { target_pc, .. }
        | Insn::NotNull { target_pc, .. }
        | Insn::Eq { target_pc, .. }
        | Insn::Ne { target_pc, .. }
        | Insn::Lt { target_pc, .. }
        | Insn::Le { target_pc, .. }
        | Insn::Gt { target_pc, .. }
        | Insn::Ge { target_pc, .. } => Some(target_pc),
        _ => None,
    }
}

/// Rule 2: invert a conditional branch that only skips over a `Goto`.
///
/// `cond -> +2; Goto L` becomes `inverted-cond -> L` and the `Goto` dies.
/// The `Goto` must not be a jump target (anything that jumps to it must keep
/// finding it), which also excludes a `Goto` to itself.
fn invert_branches_over_gotos(
    insns: &mut [(Insn, usize)],
    live: &mut [bool],
    targets: &mut [bool],
    stats: &mut PassStats,
) {
    let n = insns.len();
    if n < 3 {
        return;
    }
    for i in 0..n - 2 {
        if !live[i] || !live[i + 1] || targets[i + 1] {
            continue;
        }
        let Some(cond_target) = invertible_condition_target(&insns[i].0) else {
            continue;
        };
        if resolved_target(cond_target, n) != Some(i as u32 + 2) {
            continue;
        }
        let Insn::Goto { target_pc } = &insns[i + 1].0 else {
            continue;
        };
        let Some(goto_dst) = resolved_target(target_pc, n) else {
            continue;
        };
        let Some(inverted) = inverted_condition(&insns[i].0, BranchOffset::Offset(goto_dst)) else {
            continue;
        };
        insns[i].0 = inverted;
        live[i + 1] = false;
        stats.branches_inverted += 1;
    }
}

/// Rule 3: merge adjacent `Copy`s over contiguous register ranges.
///
/// `op_copy` copies element by element from the lowest register up, so running
/// one merged `Copy { extra_amount: a + b + 1 }` performs exactly the same
/// register operations in exactly the same order as the two originals, even
/// when source and destination ranges overlap. A merged-away `Copy` must not
/// be a jump target: whoever jumps there expects the tail of the run only.
fn merge_adjacent_copies(
    insns: &mut [(Insn, usize)],
    live: &mut [bool],
    targets: &mut [bool],
    stats: &mut PassStats,
) {
    let n = insns.len();
    let mut i = 0;
    while i < n {
        if !live[i] {
            i += 1;
            continue;
        }
        let Insn::Copy {
            src_reg,
            dst_reg,
            extra_amount,
        } = insns[i].0
        else {
            i += 1;
            continue;
        };
        let mut merged_extra = extra_amount;
        let mut j = i + 1;
        while j < n && live[j] && !targets[j] {
            let Insn::Copy {
                src_reg: next_src,
                dst_reg: next_dst,
                extra_amount: next_extra,
            } = insns[j].0
            else {
                break;
            };
            if next_src != src_reg + merged_extra + 1 || next_dst != dst_reg + merged_extra + 1 {
                break;
            }
            merged_extra += next_extra + 1;
            live[j] = false;
            stats.copies_merged += 1;
            j += 1;
        }
        if merged_extra != extra_amount {
            let Insn::Copy { extra_amount, .. } = &mut insns[i].0 else {
                unreachable!("checked above");
            };
            *extra_amount = merged_extra;
        }
        i = j;
    }
}

/// Registers an instruction reads or writes, for the rule 4 gap check.
/// Returns `None` for any opcode we have not proven safe: rule 4 then skips.
/// Every whitelisted opcode has no branch operands (it cannot jump away
/// between the `Affinity` and the `MakeRecord`) and cannot suspend.
fn known_register_footprint(insn: &Insn) -> Option<std::ops::RangeInclusive<usize>> {
    match insn {
        // Writes rowid_reg; prev_largest_reg is documented unused but treated
        // as read to stay conservative. The two are always adjacent-or-equal
        // in emitted code, but compute the enclosing range to be safe.
        Insn::NewRowid {
            rowid_reg,
            prev_largest_reg,
            ..
        } => Some(*rowid_reg.min(prev_largest_reg)..=*rowid_reg.max(prev_largest_reg)),
        Insn::Integer { dest, .. }
        | Insn::Real { dest, .. }
        | Insn::String8 { dest, .. }
        | Insn::Blob { dest, .. } => Some(*dest..=*dest),
        Insn::Null { dest, dest_end } => Some(*dest..=dest_end.unwrap_or(*dest)),
        _ => None,
    }
}

/// Rule 4: fold `Affinity` into a directly following `MakeRecord`.
///
/// `op_make_record` with `affinity_str` applies affinities to the source
/// registers in place with the exact same `apply_affinity_char` loop that
/// `op_affinity` uses, so folding preserves both the built record and the
/// register mutations. The fold only moves the *time* of the conversion past
/// the instructions in between, so those instructions (at most one) must not
/// read or write the register range, must not be able to jump away, and
/// nothing may jump into the window past the `Affinity`.
///
/// This fires rarely in practice, and the skips are all correct: UPDATE's
/// main-record path already sets `MakeRecord.affinity_str` (its preceding
/// standalone `Affinity` applies the same conversions twice, which this rule
/// must not touch since folding needs an empty `affinity_str`), index records
/// cover one more register than their `Affinity` (the appended rowid must not
/// be converted), and INSERT applies affinities several instructions before
/// its `MakeRecord` with constraint checks in between.
fn fold_affinity_into_make_record(
    insns: &mut [(Insn, usize)],
    live: &mut [bool],
    targets: &mut [bool],
    stats: &mut PassStats,
) {
    fn next_live(live: &[bool], from: usize) -> Option<usize> {
        (from..live.len()).find(|&j| live[j])
    }
    let n = insns.len();
    for i in 0..n {
        if !live[i] {
            continue;
        }
        let Insn::Affinity {
            start_reg, count, ..
        } = &insns[i].0
        else {
            continue;
        };
        let (aff_start, aff_count) = (*start_reg, count.get());

        // Locate the shape: MakeRecord directly after the Affinity, or after
        // exactly one instruction in between.
        let Some(j) = next_live(live, i + 1) else {
            continue;
        };
        let (mr, gap) = if matches!(insns[j].0, Insn::MakeRecord { .. }) {
            (j, None)
        } else {
            let Some(k) = next_live(live, j + 1) else {
                continue;
            };
            if !matches!(insns[k].0, Insn::MakeRecord { .. }) {
                continue;
            }
            (k, Some(j))
        };
        let Insn::MakeRecord {
            start_reg: mr_start,
            count: mr_count,
            affinity_str,
            ..
        } = &insns[mr].0
        else {
            unreachable!("checked above");
        };
        if *mr_start as usize != aff_start
            || *mr_count as usize != aff_count
            || affinity_str.is_some()
        {
            continue;
        }

        // The shape matches; now the safety checks. Deleting a jump target
        // would leave branches pointing at a removed instruction, and a
        // branch that jumps straight to the MakeRecord skips the Affinity
        // today but would suddenly get the conversions after the fold.
        if targets[i] || targets[mr] {
            stats.affinity_folds_skipped += 1;
            continue;
        }
        // The instruction in between must not read or write the register
        // range (it would observe values before/after conversion), must not
        // be able to branch away (the whitelist only contains straight-line
        // opcodes), and must not be a jump target.
        if let Some(gap) = gap {
            let safe = match known_register_footprint(&insns[gap].0) {
                Some(range) if !targets[gap] => {
                    *range.end() < aff_start || *range.start() >= aff_start + aff_count
                }
                _ => false,
            };
            if !safe {
                stats.affinity_folds_skipped += 1;
                continue;
            }
        }

        let Insn::Affinity { affinities, .. } = &mut insns[i].0 else {
            unreachable!("checked above");
        };
        let affinities = std::mem::take(affinities);
        let Insn::MakeRecord { affinity_str, .. } = &mut insns[mr].0 else {
            unreachable!("checked above");
        };
        *affinity_str = Some(affinities);
        live[i] = false;
        stats.affinities_folded += 1;
    }
}

/// Rule 5: delete branches whose only destination is the next instruction.
///
/// Jumping to `i + 1` and falling through to `i + 1` are the same thing, so
/// the branch only matters through its side effects:
/// - `Goto`, `If`, `IfNot`, `IsNull`, `NotNull` have none (`op_if`/`op_if_not`
///   only read the register; `op_is_null`/`op_not_null` only inspect it).
/// - `Eq`/`Ne`/`Lt`/`Le`/`Gt`/`Ge` write affinity-converted operands back to
///   the registers unless their affinity is BLOB (none), and a custom
///   collation lookup can fail at runtime; such instances are kept.
///
/// The deleted instruction must not be a jump target, since branches pointing
/// at a deleted instruction cannot be remapped.
fn delete_jumps_to_next(
    insns: &mut [(Insn, usize)],
    live: &mut [bool],
    targets: &mut [bool],
    stats: &mut PassStats,
) {
    let n = insns.len();
    for i in 0..n {
        if !live[i] || targets[i] {
            continue;
        }
        let next = i as u32 + 1;
        let deletable = match &insns[i].0 {
            Insn::Goto { target_pc } => resolved_target(target_pc, n) == Some(next),
            Insn::If { target_pc, .. }
            | Insn::IfNot { target_pc, .. }
            | Insn::IsNull { target_pc, .. }
            | Insn::NotNull { target_pc, .. } => resolved_target(target_pc, n) == Some(next),
            Insn::Eq {
                target_pc,
                flags,
                collation,
                ..
            }
            | Insn::Ne {
                target_pc,
                flags,
                collation,
                ..
            }
            | Insn::Lt {
                target_pc,
                flags,
                collation,
                ..
            }
            | Insn::Le {
                target_pc,
                flags,
                collation,
                ..
            }
            | Insn::Gt {
                target_pc,
                flags,
                collation,
                ..
            }
            | Insn::Ge {
                target_pc,
                flags,
                collation,
                ..
            } => {
                if resolved_target(target_pc, n) != Some(next) {
                    false
                } else if flags.get_affinity() == Affinity::Blob
                    && !collation.is_some_and(|c| c.is_custom())
                {
                    true
                } else {
                    stats.cmp_deletes_skipped += 1;
                    false
                }
            }
            _ => false,
        };
        if deletable {
            live[i] = false;
            stats.jumps_to_next_deleted += 1;
        }
    }
}

/// Rule 6: delete instructions no path from instruction 0 reaches.
///
/// Worklist reachability over the live instructions. Successors of a live
/// instruction:
/// - `Goto`, `Jump`, `Halt`: their branch targets only (none for `Halt`).
/// - everything else: every branch target plus the next live instruction.
///   That covers `Gosub` and `Yield`, whose "return points" (index + 1) are
///   entered dynamically through an address stored in a register, and stays
///   conservative for opcodes like `Init` that never actually fall through.
///
/// A cheaper single forward scan (reachable = jump target or fall-through)
/// was tried here and rejected: window-function programs carry whole dead
/// regions with internal loops, and any scheme that trusts the static target
/// set keeps a dead cycle alive because its members point at each other.
/// Only real reachability from instruction 0 removes them.
fn sweep_unreachable(
    insns: &mut [(Insn, usize)],
    scratch: &mut Scratch,
    any_dead: bool,
    stats: &mut PassStats,
) {
    let n = insns.len();
    let live = &mut scratch.live;
    // next_live[i] = smallest live index strictly greater than i. When
    // nothing is dead yet that is just i + 1, so skip building the table.
    let next_live = &mut scratch.u32_b;
    next_live.clear();
    if any_dead {
        next_live.resize(n, u32::MAX);
        let mut nl = u32::MAX;
        for i in (0..n).rev() {
            next_live[i] = nl;
            if live[i] {
                nl = i as u32;
            }
        }
    }

    let reached = &mut scratch.aux;
    reached.clear();
    reached.resize(n, false);
    let worklist = &mut scratch.worklist;
    worklist.clear();
    // Instruction 0 may itself have been marked dead by an earlier rule; the
    // walk still starts there because its fall-through successor is the next
    // live instruction, which is where execution starts after compaction.
    worklist.push(0);
    while let Some(i) = worklist.pop() {
        let i = i as usize;
        if reached[i] {
            continue;
        }
        reached[i] = true;
        let falls_through = !matches!(
            insns[i].0,
            Insn::Goto { .. } | Insn::Jump { .. } | Insn::Halt { .. }
        );
        insns[i].0.for_each_branch_offset(|off| {
            if let Some(t) = resolved_target(off, n) {
                if !reached[t as usize] {
                    worklist.push(t);
                }
            }
        });
        if falls_through {
            let next = if any_dead {
                next_live[i]
            } else if i + 1 < n {
                i as u32 + 1
            } else {
                u32::MAX
            };
            if next != u32::MAX && !reached[next as usize] {
                worklist.push(next);
            }
        }
    }

    for i in 0..n {
        if live[i] && !reached[i] {
            live[i] = false;
            stats.unreachable_deleted += 1;
        }
    }
}

/// Drop dead instructions and remap every branch operand of the survivors.
/// EXPLAIN comments move with their instruction; comments on dead
/// instructions are dropped.
fn compact(
    insns: &mut Vec<(Insn, usize)>,
    comments: &mut Vec<(InsnReference, &'static str)>,
    scratch: &mut Scratch,
) {
    let n = insns.len();
    let live = &scratch.live;
    let old_to_new = &mut scratch.u32_a;
    old_to_new.clear();
    old_to_new.resize(n, u32::MAX);
    let mut new_index = 0u32;
    for i in 0..n {
        if live[i] {
            old_to_new[i] = new_index;
            new_index += 1;
        }
    }
    if new_index as usize == n {
        return;
    }

    let mut i = 0;
    insns.retain(|_| {
        let keep = live[i];
        i += 1;
        keep
    });
    for (insn, _) in insns.iter_mut() {
        insn.for_each_branch_offset(|off| {
            let Some(t) = resolved_target(off, n) else {
                return;
            };
            let new_t = old_to_new[t as usize];
            // A live branch pointing at a deleted instruction means one of
            // the rules above violated its precondition.
            turso_debug_assert!(
                new_t != u32::MAX,
                "live branch operand points at a deleted instruction"
            );
            if new_t != u32::MAX {
                *off = BranchOffset::Offset(new_t);
            }
        });
    }
    comments.retain_mut(|(offset, _)| {
        let new_offset = old_to_new[*offset as usize];
        if new_offset == u32::MAX {
            false
        } else {
            *offset = new_offset;
            true
        }
    });
}

/// Debug check: after the pass every branch operand must be a resolved,
/// in-bounds offset.
#[cfg(debug_assertions)]
fn verify_all_branches_resolved(insns: &mut [(Insn, usize)]) {
    let n = insns.len();
    for (insn, _) in insns.iter_mut() {
        insn.for_each_branch_offset(|off| {
            turso_debug_assert!(
                matches!(off, BranchOffset::Offset(t) if (*t as usize) < n),
                "peephole pass left an unresolved or out-of-bounds branch operand"
            );
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;
    use crate::vdbe::insn::CmpInsFlags;
    use std::num::NonZeroUsize;

    fn off(t: u32) -> BranchOffset {
        BranchOffset::Offset(t)
    }

    fn goto(t: u32) -> Insn {
        Insn::Goto { target_pc: off(t) }
    }

    fn halt() -> Insn {
        Insn::Halt {
            err_code: 0,
            description: String::new(),
            on_error: None,
            description_reg: None,
        }
    }

    fn halt_err(err_code: usize, description: &str) -> Insn {
        Insn::Halt {
            err_code,
            description: description.to_string(),
            on_error: None,
            description_reg: None,
        }
    }

    fn integer(value: i64, dest: usize) -> Insn {
        Insn::Integer { value, dest }
    }

    fn if_insn(reg: usize, t: u32, jump_if_null: bool) -> Insn {
        Insn::If {
            reg,
            target_pc: off(t),
            jump_if_null,
        }
    }

    fn not_null(reg: usize, t: u32) -> Insn {
        Insn::NotNull {
            reg,
            target_pc: off(t),
        }
    }

    fn is_null(reg: usize, t: u32) -> Insn {
        Insn::IsNull {
            reg,
            target_pc: off(t),
        }
    }

    fn eq(lhs: usize, rhs: usize, t: u32, flags: CmpInsFlags) -> Insn {
        Insn::Eq {
            lhs,
            rhs,
            target_pc: off(t),
            flags,
            collation: None,
        }
    }

    fn copy(src_reg: usize, dst_reg: usize, extra_amount: usize) -> Insn {
        Insn::Copy {
            src_reg,
            dst_reg,
            extra_amount,
        }
    }

    fn affinity(start_reg: usize, count: usize, affinities: &str) -> Insn {
        Insn::Affinity {
            start_reg,
            count: NonZeroUsize::new(count).unwrap(),
            affinities: affinities.to_string(),
        }
    }

    fn make_record(start_reg: u32, count: u32, dest_reg: u32) -> Insn {
        Insn::MakeRecord {
            start_reg,
            count,
            dest_reg,
            index_name: None,
            affinity_str: None,
        }
    }

    fn prog(insns: Vec<Insn>) -> Vec<(Insn, usize)> {
        insns
            .into_iter()
            .enumerate()
            .map(|(original_index, insn)| (insn, original_index))
            .collect()
    }

    fn assert_program(actual: &[(Insn, usize)], expected: &[Insn]) {
        let actual_fmt: Vec<String> = actual.iter().map(|(insn, _)| format!("{insn:?}")).collect();
        let expected_fmt: Vec<String> = expected.iter().map(|insn| format!("{insn:?}")).collect();
        assert_eq!(actual_fmt, expected_fmt);
    }

    type PassOutput = (Vec<(Insn, usize)>, Vec<(InsnReference, &'static str)>);

    fn run_whole_pass(insns: Vec<Insn>) -> PassOutput {
        let mut insns = prog(insns);
        let mut comments = Vec::new();
        optimize_program(&mut insns, &mut comments);
        (insns, comments)
    }

    // ---------------- rule 1: jump threading ----------------

    #[test]
    fn threading_retargets_through_a_goto_chain() {
        let mut insns = prog(vec![
            if_insn(0, 2, false), // 0: points at a Goto
            halt(),               // 1
            goto(3),              // 2: -> another Goto
            goto(4),              // 3: -> Integer
            integer(7, 0),        // 4
        ]);
        let mut stats = PassStats::default();
        thread_jumps(&mut insns, &mut Scratch::default(), &mut stats);
        assert_program(
            &insns,
            &[
                if_insn(0, 4, false),
                halt(),
                goto(4),
                goto(4),
                integer(7, 0),
            ],
        );
        // Retargeted: the If operand and the first Goto's own operand. The
        // second Goto already pointed at the final destination.
        assert_eq!(stats.jumps_threaded, 2);
    }

    #[test]
    fn threading_leaves_a_goto_to_itself_alone() {
        let mut insns = prog(vec![if_insn(0, 1, false), goto(1), halt()]);
        let mut stats = PassStats::default();
        thread_jumps(&mut insns, &mut Scratch::default(), &mut stats);
        assert_program(&insns, &[if_insn(0, 1, false), goto(1), halt()]);
        assert_eq!(stats.jumps_threaded, 0);
    }

    #[test]
    fn threading_leaves_a_two_goto_cycle_alone() {
        let mut insns = prog(vec![if_insn(0, 1, false), goto(2), goto(1), halt()]);
        let mut stats = PassStats::default();
        thread_jumps(&mut insns, &mut Scratch::default(), &mut stats);
        assert_program(&insns, &[if_insn(0, 1, false), goto(2), goto(1), halt()]);
    }

    #[test]
    fn goto_to_halt_becomes_that_halt() {
        let mut insns = prog(vec![
            goto(2),                    // 0: jumps straight at a Halt
            integer(1, 0),              // 1
            halt_err(19, "constraint"), // 2
        ]);
        let mut stats = PassStats::default();
        thread_jumps(&mut insns, &mut Scratch::default(), &mut stats);
        assert_program(
            &insns,
            &[
                halt_err(19, "constraint"), // clone carries all fields
                integer(1, 0),
                halt_err(19, "constraint"),
            ],
        );
        assert_eq!(stats.gotos_replaced_with_halt, 1);
    }

    #[test]
    fn conditional_branch_to_halt_is_not_replaced() {
        let mut insns = prog(vec![if_insn(0, 2, false), integer(1, 0), halt()]);
        let mut stats = PassStats::default();
        thread_jumps(&mut insns, &mut Scratch::default(), &mut stats);
        assert_program(&insns, &[if_insn(0, 2, false), integer(1, 0), halt()]);
        assert_eq!(stats.gotos_replaced_with_halt, 0);
    }

    /// Populate `targets` the way `run_pass` does before the rules run.
    fn collect_targets_for_test(insns: &mut [(Insn, usize)]) -> Vec<bool> {
        let n = insns.len();
        let mut targets = vec![false; n];
        for (insn, _) in insns.iter_mut() {
            insn.for_each_branch_offset(|off| {
                if let Some(t) = resolved_target(off, n) {
                    targets[t as usize] = true;
                }
            });
        }
        targets
    }

    // ---------------- rule 2: branch-over-goto inversion ----------------

    fn run_inversion(insns: &mut [(Insn, usize)]) -> (Vec<bool>, PassStats) {
        let n = insns.len();
        let mut live = vec![true; n];
        let mut targets = collect_targets_for_test(insns);
        let mut stats = PassStats::default();
        invert_branches_over_gotos(insns, &mut live, &mut targets, &mut stats);
        (live, stats)
    }

    #[test]
    fn if_over_goto_becomes_ifnot_with_flipped_null_flag() {
        let mut insns = prog(vec![
            if_insn(0, 2, false), // 0: skips over the Goto
            goto(4),              // 1: the loop exit
            integer(1, 0),        // 2
            halt(),               // 3
            halt(),               // 4
        ]);
        let (live, stats) = run_inversion(&mut insns);
        assert_program(
            &insns,
            &[
                Insn::IfNot {
                    reg: 0,
                    target_pc: off(4),
                    jump_if_null: true,
                },
                goto(4), // marked dead, removed later by compaction
                integer(1, 0),
                halt(),
                halt(),
            ],
        );
        assert!(!live[1]);
        assert_eq!(stats.branches_inverted, 1);
    }

    #[test]
    fn not_null_over_goto_becomes_is_null() {
        let mut insns = prog(vec![
            not_null(3, 2), // the INSERT explicit-rowid shape
            goto(4),
            integer(1, 0),
            halt(),
            halt(),
        ]);
        let (live, stats) = run_inversion(&mut insns);
        assert_program(
            &insns,
            &[is_null(3, 4), goto(4), integer(1, 0), halt(), halt()],
        );
        assert!(!live[1]);
        assert_eq!(stats.branches_inverted, 1);
    }

    #[test]
    fn eq_over_goto_becomes_ne_and_flips_jump_if_null() {
        let flags = CmpInsFlags::default().jump_if_null();
        let mut insns = prog(vec![
            eq(0, 1, 2, flags),
            goto(4),
            integer(1, 0),
            halt(),
            halt(),
        ]);
        let (_, stats) = run_inversion(&mut insns);
        let Insn::Ne {
            target_pc, flags, ..
        } = &insns[0].0
        else {
            panic!("Eq must invert to Ne, got {:?}", insns[0].0);
        };
        assert_eq!(*target_pc, off(4));
        // Original jumped on NULL; the inverse must not.
        assert!(!flags.has_jump_if_null());
        assert_eq!(stats.branches_inverted, 1);
    }

    #[test]
    fn eq_inversion_keeps_null_eq_and_sets_jump_if_null() {
        let flags = CmpInsFlags::default().null_eq();
        let mut insns = prog(vec![
            eq(0, 1, 2, flags),
            goto(4),
            integer(1, 0),
            halt(),
            halt(),
        ]);
        run_inversion(&mut insns);
        let Insn::Ne { flags, .. } = &insns[0].0 else {
            panic!("Eq must invert to Ne, got {:?}", insns[0].0);
        };
        assert!(flags.has_nulleq());
        assert!(flags.has_jump_if_null());
    }

    #[test]
    fn inversion_blocked_when_goto_is_a_jump_target() {
        let mut insns = prog(vec![
            if_insn(0, 2, false), // would invert...
            goto(5),              // ...but something jumps here
            integer(1, 0),
            not_null(1, 1), // jumps to the Goto
            halt(),
            halt(),
        ]);
        let (live, stats) = run_inversion(&mut insns);
        assert!(live[1]);
        assert_eq!(stats.branches_inverted, 0);
        assert!(matches!(insns[0].0, Insn::If { .. }));
    }

    #[test]
    fn non_invertible_conditional_is_left_alone() {
        let mut insns = prog(vec![
            Insn::Found {
                cursor_id: 0,
                target_pc: off(2),
                record_reg: 0,
                num_regs: 1,
            },
            goto(4),
            integer(1, 0),
            halt(),
            halt(),
        ]);
        let (live, stats) = run_inversion(&mut insns);
        assert!(live[1]);
        assert_eq!(stats.branches_inverted, 0);
    }

    // ---------------- rule 3: copy merge ----------------

    fn run_copy_merge(insns: &mut [(Insn, usize)]) -> (Vec<bool>, PassStats) {
        let n = insns.len();
        let mut live = vec![true; n];
        let mut targets = collect_targets_for_test(insns);
        let mut stats = PassStats::default();
        merge_adjacent_copies(insns, &mut live, &mut targets, &mut stats);
        (live, stats)
    }

    #[test]
    fn adjacent_parallel_copies_merge_into_a_range_copy() {
        let mut insns = prog(vec![copy(1, 10, 0), copy(2, 11, 0), copy(3, 12, 0), halt()]);
        let (live, stats) = run_copy_merge(&mut insns);
        let Insn::Copy { extra_amount, .. } = insns[0].0 else {
            panic!("first Copy must survive");
        };
        assert_eq!(extra_amount, 2);
        assert!(live[0] && !live[1] && !live[2]);
        assert_eq!(stats.copies_merged, 2);
    }

    #[test]
    fn copies_with_ranges_merge_too() {
        // extra_amount on the first copy already spans two registers.
        let mut insns = prog(vec![copy(1, 10, 1), copy(3, 12, 0), halt()]);
        let (live, stats) = run_copy_merge(&mut insns);
        let Insn::Copy { extra_amount, .. } = insns[0].0 else {
            panic!("first Copy must survive");
        };
        assert_eq!(extra_amount, 2);
        assert!(!live[1]);
        assert_eq!(stats.copies_merged, 1);
    }

    #[test]
    fn copy_merge_blocked_when_second_copy_is_a_jump_target() {
        let mut insns = prog(vec![
            copy(1, 10, 0),
            copy(2, 11, 0), // jump target: must stay addressable
            goto(1),
            halt(),
        ]);
        let (live, stats) = run_copy_merge(&mut insns);
        let Insn::Copy { extra_amount, .. } = insns[0].0 else {
            panic!();
        };
        assert_eq!(extra_amount, 0);
        assert!(live[1]);
        assert_eq!(stats.copies_merged, 0);
    }

    #[test]
    fn non_contiguous_copies_do_not_merge() {
        let mut insns = prog(vec![copy(1, 10, 0), copy(3, 11, 0), halt()]);
        let (live, stats) = run_copy_merge(&mut insns);
        assert!(live[1]);
        assert_eq!(stats.copies_merged, 0);
        let Insn::Copy { extra_amount, .. } = insns[0].0 else {
            panic!();
        };
        assert_eq!(extra_amount, 0);
    }

    // ---------------- rule 4: affinity fold ----------------

    fn run_affinity_fold(insns: &mut [(Insn, usize)]) -> (Vec<bool>, PassStats) {
        let n = insns.len();
        let mut live = vec![true; n];
        let mut targets = collect_targets_for_test(insns);
        let mut stats = PassStats::default();
        fold_affinity_into_make_record(insns, &mut live, &mut targets, &mut stats);
        (live, stats)
    }

    #[test]
    fn affinity_folds_into_adjacent_make_record() {
        let mut insns = prog(vec![affinity(5, 2, "BC"), make_record(5, 2, 9), halt()]);
        let (live, stats) = run_affinity_fold(&mut insns);
        assert!(!live[0]);
        let Insn::MakeRecord { affinity_str, .. } = &insns[1].0 else {
            panic!();
        };
        assert_eq!(affinity_str.as_deref(), Some("BC"));
        assert_eq!(stats.affinities_folded, 1);
    }

    #[test]
    fn affinity_folds_across_new_rowid_outside_the_range() {
        let mut insns = prog(vec![
            affinity(5, 2, "DD"),
            Insn::NewRowid {
                cursor: 0,
                rowid_reg: 3,
                prev_largest_reg: 0,
            },
            make_record(5, 2, 9),
            halt(),
        ]);
        let (live, stats) = run_affinity_fold(&mut insns);
        assert!(!live[0]);
        let Insn::MakeRecord { affinity_str, .. } = &insns[2].0 else {
            panic!();
        };
        assert_eq!(affinity_str.as_deref(), Some("DD"));
        assert_eq!(stats.affinities_folded, 1);
    }

    #[test]
    fn affinity_fold_blocked_when_gap_insn_writes_into_the_range() {
        let mut insns = prog(vec![
            affinity(5, 2, "DD"),
            integer(0, 6), // overwrites a register the affinity already converted
            make_record(5, 2, 9),
            halt(),
        ]);
        let (live, stats) = run_affinity_fold(&mut insns);
        assert!(live[0]);
        assert_eq!(stats.affinities_folded, 0);
        assert_eq!(stats.affinity_folds_skipped, 1);
    }

    #[test]
    fn affinity_fold_blocked_when_make_record_is_a_jump_target() {
        let mut insns = prog(vec![
            affinity(5, 2, "DD"),
            make_record(5, 2, 9), // jumped to directly: skips the Affinity today
            goto(1),
            halt(),
        ]);
        let (live, stats) = run_affinity_fold(&mut insns);
        assert!(live[0]);
        assert_eq!(stats.affinities_folded, 0);
        assert_eq!(stats.affinity_folds_skipped, 1);
    }

    #[test]
    fn affinity_fold_blocked_on_range_mismatch() {
        let mut insns = prog(vec![affinity(5, 2, "DD"), make_record(5, 3, 9), halt()]);
        let (live, stats) = run_affinity_fold(&mut insns);
        assert!(live[0]);
        assert_eq!(stats.affinities_folded, 0);
    }

    #[test]
    fn affinity_fold_blocked_when_make_record_already_has_affinities() {
        let mut insns = prog(vec![
            affinity(5, 2, "DD"),
            Insn::MakeRecord {
                start_reg: 5,
                count: 2,
                dest_reg: 9,
                index_name: None,
                affinity_str: Some("BB".to_string()),
            },
            halt(),
        ]);
        let (live, _) = run_affinity_fold(&mut insns);
        assert!(live[0]);
    }

    // ---------------- rule 5: delete jump-to-next ----------------

    fn run_delete_jump_to_next(insns: &mut [(Insn, usize)]) -> (Vec<bool>, PassStats) {
        let n = insns.len();
        let mut live = vec![true; n];
        let mut targets = collect_targets_for_test(insns);
        let mut stats = PassStats::default();
        delete_jumps_to_next(insns, &mut live, &mut targets, &mut stats);
        (live, stats)
    }

    #[test]
    fn goto_to_next_instruction_is_deleted() {
        let mut insns = prog(vec![integer(1, 0), goto(2), halt()]);
        let (live, stats) = run_delete_jump_to_next(&mut insns);
        assert!(!live[1]);
        assert_eq!(stats.jumps_to_next_deleted, 1);
    }

    #[test]
    fn pure_conditional_to_next_instruction_is_deleted() {
        let mut insns = prog(vec![integer(1, 0), is_null(0, 2), halt()]);
        let (live, stats) = run_delete_jump_to_next(&mut insns);
        assert!(!live[1]);
        assert_eq!(stats.jumps_to_next_deleted, 1);
    }

    #[test]
    fn comparison_without_affinity_to_next_instruction_is_deleted() {
        let mut insns = prog(vec![
            integer(1, 0),
            eq(0, 1, 2, CmpInsFlags::default()),
            halt(),
        ]);
        let (live, stats) = run_delete_jump_to_next(&mut insns);
        assert!(!live[1]);
        assert_eq!(stats.jumps_to_next_deleted, 1);
        assert_eq!(stats.cmp_deletes_skipped, 0);
    }

    #[test]
    fn comparison_with_affinity_to_next_is_kept_for_its_register_writes() {
        // Eq with TEXT affinity converts its operand registers in place;
        // deleting it would skip those conversions.
        let flags = CmpInsFlags::default().with_affinity(crate::vdbe::affinity::Affinity::Text);
        let mut insns = prog(vec![eq(0, 1, 1, flags), halt()]);
        let (live, stats) = run_delete_jump_to_next(&mut insns);
        assert!(live[0]);
        assert_eq!(stats.jumps_to_next_deleted, 0);
        assert_eq!(stats.cmp_deletes_skipped, 1);
    }

    #[test]
    fn comparison_with_custom_collation_to_next_is_kept() {
        let mut insns = prog(vec![
            Insn::Eq {
                lhs: 0,
                rhs: 1,
                target_pc: off(1),
                flags: CmpInsFlags::default(),
                collation: Some(CollationSeq::custom("mycoll")),
            },
            halt(),
        ]);
        let (live, stats) = run_delete_jump_to_next(&mut insns);
        assert!(live[0]);
        assert_eq!(stats.cmp_deletes_skipped, 1);
    }

    #[test]
    fn goto_to_next_that_is_a_jump_target_is_kept() {
        let mut insns = prog(vec![
            integer(1, 0),
            goto(2), // goto-to-next, but the If below jumps here
            if_insn(0, 1, false),
            halt(),
        ]);
        let (live, stats) = run_delete_jump_to_next(&mut insns);
        assert!(live[1]);
        assert_eq!(stats.jumps_to_next_deleted, 0);
    }

    #[test]
    fn decr_jump_zero_to_next_is_kept_for_its_decrement() {
        let mut insns = prog(vec![
            Insn::DecrJumpZero {
                reg: 0,
                target_pc: off(1),
            },
            halt(),
        ]);
        let (live, _) = run_delete_jump_to_next(&mut insns);
        assert!(live[0]);
    }

    // ---------------- rule 6: unreachable sweep ----------------

    fn run_sweep(insns: &mut [(Insn, usize)]) -> (Vec<bool>, PassStats) {
        let n = insns.len();
        let mut scratch = Scratch::default();
        scratch.live.resize(n, true);
        scratch.targets = collect_targets_for_test(insns);
        let mut stats = PassStats::default();
        sweep_unreachable(insns, &mut scratch, false, &mut stats);
        (scratch.live, stats)
    }

    #[test]
    fn code_after_a_goto_with_no_way_in_is_swept() {
        let mut insns = prog(vec![
            goto(3),       // 0
            integer(1, 0), // 1: the abort-subroutine shape nothing calls
            Insn::Return {
                return_reg: 0,
                can_fallthrough: false,
            }, // 2
            halt(),        // 3
        ]);
        let (live, stats) = run_sweep(&mut insns);
        assert!(live[0] && !live[1] && !live[2] && live[3]);
        assert_eq!(stats.unreachable_deleted, 2);
    }

    #[test]
    fn gosub_keeps_its_return_point_alive() {
        let mut insns = prog(vec![
            Insn::Gosub {
                target_pc: off(3),
                return_reg: 0,
            }, // 0: calls the subroutine
            integer(1, 0), // 1: the dynamic return point
            halt(),        // 2
            Insn::Return {
                return_reg: 0,
                can_fallthrough: false,
            }, // 3: subroutine body
        ]);
        let (live, stats) = run_sweep(&mut insns);
        assert!(live.iter().all(|l| *l), "all live, got {live:?}");
        assert_eq!(stats.unreachable_deleted, 0);
    }

    #[test]
    fn yield_keeps_its_resume_point_alive() {
        let mut insns = prog(vec![
            Insn::Yield {
                yield_reg: 0,
                end_offset: off(2),
                subtype_clear_start_reg: 0,
                subtype_clear_count: 0,
            }, // 0
            integer(1, 0), // 1: resumed dynamically after the coroutine yields back
            halt(),        // 2
        ]);
        let (live, _) = run_sweep(&mut insns);
        assert!(live.iter().all(|l| *l), "all live, got {live:?}");
    }

    #[test]
    fn jump_successors_are_its_three_targets_only() {
        let mut insns = prog(vec![
            Insn::Jump {
                target_pc_lt: off(2),
                target_pc_eq: off(3),
                target_pc_gt: off(4),
            }, // 0
            integer(1, 0), // 1: not a successor of Jump
            halt(),        // 2
            halt(),        // 3
            halt(),        // 4
        ]);
        let (live, _) = run_sweep(&mut insns);
        assert!(live[0] && !live[1] && live[2] && live[3] && live[4]);
    }

    // ---------------- compaction ----------------

    #[test]
    fn compaction_remaps_branches_and_moves_comments() {
        let insns = vec![
            goto(2),       // 0: jump-to-next after deleting 1? No: points at 2
            integer(1, 0), // 1: unreachable
            halt(),        // 2
        ];
        let mut insns = prog(insns);
        let mut comments: Vec<(InsnReference, &'static str)> =
            vec![(0, "jump"), (1, "dead"), (2, "stop")];
        let mut scratch = Scratch {
            live: vec![true, false, true],
            ..Scratch::default()
        };
        compact(&mut insns, &mut comments, &mut scratch);
        assert_program(&insns, &[goto(1), halt()]);
        assert_eq!(comments, vec![(0, "jump"), (1, "stop")]);
    }

    // ---------------- whole pass ----------------

    /// Not a correctness test: measures the pass by itself, back to back with
    /// the clone cost, so the numbers hold up even on machines whose speed
    /// drifts between runs. Run with:
    /// `cargo test --release -p turso_core --lib time_the_pass -- --ignored --nocapture`
    #[test]
    #[ignore = "manual timing harness"]
    fn time_the_pass_manually() {
        use std::time::Instant;
        let build = || {
            vec![
                Insn::Init { target_pc: off(4) },
                integer(1, 0),
                goto(3),
                Insn::ResultRow {
                    start_reg: 0,
                    count: 1,
                },
                halt(),
                Insn::Transaction {
                    db: 0,
                    tx_mode: crate::translate::emitter::TransactionMode::Read,
                    schema_cookie: 0,
                },
                goto(1),
            ]
        };
        let build_noop = || {
            vec![
                Insn::Init { target_pc: off(3) },
                Insn::ResultRow {
                    start_reg: 0,
                    count: 1,
                },
                halt(),
                Insn::Transaction {
                    db: 0,
                    tx_mode: crate::translate::emitter::TransactionMode::Read,
                    schema_cookie: 0,
                },
                goto(1),
            ]
        };
        let iters = 1_000_000u32;
        let mut sink = 0usize;
        let time = |f: &dyn Fn() -> Vec<Insn>, run: bool, sink: &mut usize| {
            let t0 = Instant::now();
            for _ in 0..iters {
                let mut p = prog(f());
                if run {
                    let mut c = Vec::new();
                    optimize_program(&mut p, &mut c);
                }
                *sink += p.len();
            }
            t0.elapsed() / iters
        };
        let clone_rw = time(&build, false, &mut sink);
        let full_rw = time(&build, true, &mut sink);
        let clone_no = time(&build_noop, false, &mut sink);
        let full_no = time(&build_noop, true, &mut sink);
        eprintln!(
            "rewriting: pass {:?}/iter; no-op: pass {:?}/iter (sink {sink})",
            full_rw - clone_rw,
            full_no - clone_no,
        );
    }

    #[test]
    fn single_row_insert_epilogue_collapses() {
        // The shape a single-row INSERT ends with: payload, `Goto +1; Goto +1;
        // Halt`, then the transaction prologue that jumps back to the payload.
        let (insns, _) = run_whole_pass(vec![
            Insn::Init { target_pc: off(5) }, // 0
            integer(1, 0),                    // 1: stands in for the Insert
            goto(3),                          // 2: goto to next
            goto(4),                          // 3: goto to next
            halt(),                           // 4
            Insn::Transaction {
                db: 0,
                tx_mode: crate::translate::emitter::TransactionMode::Write,
                schema_cookie: 0,
            }, // 5
            goto(1),                          // 6: back edge to the payload
        ]);
        // Both `Goto +1`s thread to the Halt and become Halts themselves; the
        // second one and the original Halt then have no way in and get swept.
        assert_program(
            &insns,
            &[
                Insn::Init { target_pc: off(3) },
                integer(1, 0),
                halt(),
                Insn::Transaction {
                    db: 0,
                    tx_mode: crate::translate::emitter::TransactionMode::Write,
                    schema_cookie: 0,
                },
                goto(1),
            ],
        );
    }

    #[test]
    fn whole_pass_threads_conditional_through_exit_trampoline() {
        // An If that exits the statement through a `Goto -> Halt` trampoline:
        // the If must end up jumping straight at the Halt and the trampoline
        // must be swept.
        let (insns, _) = run_whole_pass(vec![
            Insn::Init { target_pc: off(5) }, // 0
            if_insn(0, 4, false),             // 1: exit early via the trampoline
            integer(1, 0),                    // 2
            halt(),                           // 3
            goto(3),                          // 4: exit trampoline
            Insn::Transaction {
                db: 0,
                tx_mode: crate::translate::emitter::TransactionMode::Read,
                schema_cookie: 0,
            }, // 5
            goto(1),                          // 6
        ]);
        assert_program(
            &insns,
            &[
                Insn::Init { target_pc: off(4) },
                if_insn(0, 3, false),
                integer(1, 0),
                halt(),
                Insn::Transaction {
                    db: 0,
                    tx_mode: crate::translate::emitter::TransactionMode::Read,
                    schema_cookie: 0,
                },
                goto(1),
            ],
        );
    }

    #[test]
    fn whole_pass_leaves_an_already_tight_program_alone() {
        let build = || {
            vec![
                Insn::Init { target_pc: off(3) },
                Insn::ResultRow {
                    start_reg: 0,
                    count: 1,
                },
                halt(),
                Insn::Transaction {
                    db: 0,
                    tx_mode: crate::translate::emitter::TransactionMode::Read,
                    schema_cookie: 0,
                },
                goto(1),
            ]
        };
        let (insns, _) = run_whole_pass(build());
        assert_program(&insns, &build());
    }
}
