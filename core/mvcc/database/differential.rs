//! Differential-testing accessors for `MvStore`.
//!
//! Only compiled under `--features aristo-instr`. Exposes
//! owned snapshots of private `MvStore` internals (`txs`,
//! `finalized_tx_states`, the version-id / tx-id counters, a
//! `commit_ts` lookup, and the recovered `sqlite_schema` row versions
//! in `rows`) for use by the Aretta Books MVCC conformance harness at
//! `verification/db/flavors/turso/mvcc-conformance/` in the
//! companion `aretta-books` repo.
//!
//! Wrapper types (`TxnSnapshot`, `FinalStateSnapshot`,
//! `TxnStateSnapshot`) shadow the private `Transaction` /
//! `TransactionState` types so the surface stays
//! source-compatible across upstream refactors without leaking
//! private structure. Live as a child module of `database` so
//! Rust visibility rules give us access to the private fields.
//!
//! NEVER use these accessors in production. They take locks, allocate
//! per call, and are intentionally lossy w.r.t. the Hekaton commit-
//! dep machinery — the conformance harness only cares about the
//! Lean-projected fields.

use crate::alloc::ConcurrentAllocator;
use crate::mvcc::database::{
    RowID, RowKey, RowVersions, SQLITE_SCHEMA_MVCC_TABLE_ID, Transaction, TransactionState, TxID,
};
use crate::skiplist::comparator::BasicComparator;
use crate::skiplist::SkipMap;
use crate::sync::atomic::Ordering;

/// Owned snapshot of a `TransactionState`. Mirrors the private enum.
/// `Preparing` and `Terminated` are included for completeness — the
/// MVP scenario never produces them, but multi-conn / multi-op work
/// will.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnStateSnapshot {
    Active,
    Preparing(u64),
    Aborted,
    Terminated,
    Committed(u64),
}

impl From<TransactionState> for TxnStateSnapshot {
    fn from(s: TransactionState) -> Self {
        match s {
            TransactionState::Active => Self::Active,
            TransactionState::Preparing(ts) => Self::Preparing(ts),
            TransactionState::Aborted => Self::Aborted,
            TransactionState::Terminated => Self::Terminated,
            TransactionState::Committed(ts) => Self::Committed(ts),
        }
    }
}

/// Owned snapshot of a live `Transaction` (only the projection-
/// relevant subset: state, begin_ts, write_set). The Hekaton commit-
/// dependency state (`commit_dep_counter`, `commit_dep_set`,
/// `abort_now`) is intentionally elided — the Lean projection does
/// not model it.
#[derive(Clone, Debug)]
pub struct TxnSnapshot {
    pub state: TxnStateSnapshot,
    pub begin_ts: u64,
    /// Sorted ascending by `RowID` for stable cross-side comparison;
    /// see `Projection.lean::projectTxn` / the Rust `projection`
    /// module for the matching canonicalization on the Lean side.
    pub write_set: Vec<RowID>,
}

/// Owned snapshot of a finalized state. Same shape as
/// `TxnStateSnapshot`; kept as a type alias so the public surface
/// documents which `MvStore` field each accessor reads.
pub type FinalStateSnapshot = TxnStateSnapshot;

/// One recovered `sqlite_schema` row version, projected for the #6005
/// decodability coordinate (ACCESSORS.md row 12) — the harness's window
/// onto the schema row versions held in `MvStore::rows` AFTER
/// `maybe_recover_logical_log` (read PRE-checkpoint, before any GC).
/// `payload_empty` is the EXACT discriminator the #6005 fix keys on in
/// `sqlite_schema_btree_identity` (`if version.row.payload().is_empty()
/// { return None }`); `ended` records whether the version is a tombstone
/// (delete) at read time. Lets the DT judge #6005 via the decodability
/// model coordinate instead of an out-of-band checkpoint panic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveredSchemaRecord {
    pub rowid: i64,
    pub payload_empty: bool,
    pub ended: bool,
}

/// Project a live `Transaction` into a `TxnSnapshot` — used by the
/// macro-generated `MvStore::inspect_txs` accessor (ACCESSORS.md row 1).
/// Reads the atomic state slot with `Acquire`, mirrors `TransactionState`
/// via `TxnStateSnapshot::from`, locks the write-set Mutex to copy
/// `RowID`s out, and sorts the resulting list. The per-element sort is
/// part of the projection contract — the Lean side performs the same
/// canonicalization. The outer (by-key) sort is the consumer's
/// responsibility post-migration; `aristo::instrument::Inspect` does
/// not pre-sort.
impl<A: ConcurrentAllocator> From<&Transaction<A>> for TxnSnapshot {
    fn from(tx: &Transaction<A>) -> Self {
        // `AtomicTransactionState::state` is `pub(crate)`; child-module
        // visibility lets us read it directly without consuming the
        // atomic via `From`.
        let encoded = tx.state.state.load(Ordering::Acquire);
        let state = TxnStateSnapshot::from(TransactionState::decode(encoded));
        let begin_ts = tx.begin_ts;
        let mut write_set: Vec<RowID> = tx
            .write_set
            .lock()
            .entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect();
        write_set.sort();
        TxnSnapshot {
            state,
            begin_ts,
            write_set,
        }
    }
}

/// Project a borrowed `TransactionState` into a `FinalStateSnapshot`
/// (= `TxnStateSnapshot`) — used by the macro-generated
/// `MvStore::inspect_finalized` accessor (ACCESSORS.md row 2). Trivial
/// Copy-deref into the existing `From<TransactionState>` impl.
impl From<&TransactionState> for FinalStateSnapshot {
    fn from(s: &TransactionState) -> Self {
        TxnStateSnapshot::from(*s)
    }
}

/// Projector for the macro-generated `MvStore::inspect_txs` accessor.
/// Walks the live `txs` `SkipMap` and projects each live `Transaction`
/// into an owned `TxnSnapshot` via the `From<&Transaction>` impl above.
/// The outer (by-key) order follows `SkipMap`'s sorted iteration; the
/// consumer is responsible for any further canonicalization.
pub(super) fn project_txs<A: ConcurrentAllocator>(
    m: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
) -> Vec<(TxID, TxnSnapshot)> {
    m.iter()
        .map(|e| (*e.key(), TxnSnapshot::from(e.value())))
        .collect()
}

/// Projector for the macro-generated `MvStore::inspect_finalized`
/// accessor. Walks the `finalized_tx_states` `SkipMap` and projects each
/// `TransactionState` into an owned `FinalStateSnapshot` via the
/// `From<&TransactionState>` impl above.
pub(super) fn project_finalized<A: ConcurrentAllocator>(
    m: &SkipMap<TxID, TransactionState, BasicComparator, A>,
) -> Vec<(TxID, FinalStateSnapshot)> {
    m.iter()
        .map(|e| (*e.key(), FinalStateSnapshot::from(e.value())))
        .collect()
}

/// Projector for the macro-generated `MvStore::inspect_recovered_schema_records`
/// accessor (ACCESSORS.md row 12) — referenced by the `#[inspect(..., with = ...)]`
/// tag on `MvStore::rows`. Walks every resident key, keeps only the `sqlite_schema`
/// MVCC table (`table_id == SQLITE_SCHEMA_MVCC_TABLE_ID`, `-1`) with an integer
/// `RowKey`, and emits one `RecoveredSchemaRecord` per `RowVersion` (per-version
/// read lock taken transiently). Read PRE-checkpoint / pre-GC so an empty-payload
/// tombstone synthesized by `maybe_recover_logical_log` is still observable — the
/// #6005 decodability coordinate (`records.all(|r| !r.payload_empty)`).
pub(super) fn project_recovered_schema_records<A: ConcurrentAllocator>(
    rows: &SkipMap<RowID, RowVersions<A>, BasicComparator, A>,
) -> Vec<RecoveredSchemaRecord> {
    let mut records = Vec::new();
    for entry in rows.iter() {
        let id = entry.key();
        if id.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
            continue;
        }
        let rowid = match &id.row_id {
            RowKey::Int(rowid) => *rowid,
            RowKey::Record(_) => continue,
        };
        for version in entry.value().read().iter() {
            records.push(RecoveredSchemaRecord {
                rowid,
                payload_empty: version.row.payload().is_empty(),
                ended: version.end().is_some(),
            });
        }
    }
    records
}
