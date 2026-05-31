//! ReaderHoldsSnapshot — a chaotic workload that opens `BEGIN CONCURRENT`,
//! does an initial SELECT (which loads the connection's MVCC read_view), then
//! sits on that snapshot for N more steps (re-running SELECT each step to
//! occupy the slot without dropping the txn), and finally COMMITs. The point
//! is to keep the read_view alive across many checkpoint cycles so the
//! GC-vs-live-reader and page-free-vs-live-reader contracts are exercised —
//! today the only thing keeping a snapshot alive is short-lived auto-commit
//! reads inside the regular workloads, which finish well before the next
//! checkpoint sweeps the version chain.
//!
//! Exposed as a `ChaoticWorkloadProfile` because the existing chaotic
//! plumbing already handles "drive one fiber through a multi-step sequence,
//! resuming from the previous op's result" — exactly what's needed here
//! without inventing a parallel mechanism.

use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::operations::{OpResult, Operation, TxMode};

/// Per-fiber state machine that drives one reader-hold transaction end to end.
struct ReaderHoldsSnapshot {
    /// Total SELECTs to issue inside the held tx after the initial one. Each
    /// re-read keeps the read_view alive AND exercises read-on-active-snapshot
    /// while the rest of the cohort writes + checkpoints.
    hold_reads: u32,
    issued_reads: u32,
    table_name: Option<String>,
    key: Option<String>,
    state: HoldState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldState {
    /// About to issue `BEGIN CONCURRENT`.
    Begin,
    /// About to issue the first SELECT (loads read_view).
    InitialRead,
    /// Re-issuing SELECT to extend the hold without ending the txn.
    HoldRead,
    /// About to issue `COMMIT`.
    Commit,
    /// Fully done; `next()` returns None and the chaotic slot frees.
    Done,
}

impl ChaoticWorkload for ReaderHoldsSnapshot {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        // If the PREVIOUS op errored, the engine has auto-rolled back the
        // txn (per the lib.rs error-handling) and `Commit` would fail with
        // "cannot commit - no transaction is active". Bail to Done so the
        // chaotic slot frees and the fiber's regular workloads resume.
        if matches!(&result, Some(Err(_))) {
            self.state = HoldState::Done;
            return None;
        }
        // Both the table-name pick and the key pick happen at Begin time so
        // the rest of the state machine is straight-line; `next()` is called
        // serially per fiber so there's no interleaving with itself.
        match self.state {
            HoldState::Begin => {
                self.state = HoldState::InitialRead;
                Some(Operation::Begin {
                    mode: TxMode::Concurrent,
                })
            }
            HoldState::InitialRead => {
                // No simple_table picked at construction (the profile has no
                // sim_state access); use a key from the universe of likely
                // names. The SELECT may return zero rows — fine; the
                // important effect is loading the read_view.
                let table = self.table_name.clone().unwrap_or_else(|| "table_0".into());
                let key = self.key.clone().unwrap_or_else(|| "key_0".into());
                self.state = if self.hold_reads == 0 {
                    HoldState::Commit
                } else {
                    HoldState::HoldRead
                };
                Some(Operation::SimpleSelect {
                    table_name: table,
                    key,
                })
            }
            HoldState::HoldRead => {
                self.issued_reads += 1;
                let table = self.table_name.clone().unwrap_or_else(|| "table_0".into());
                let key = self.key.clone().unwrap_or_else(|| "key_0".into());
                if self.issued_reads >= self.hold_reads {
                    self.state = HoldState::Commit;
                }
                Some(Operation::SimpleSelect {
                    table_name: table,
                    key,
                })
            }
            HoldState::Commit => {
                self.state = HoldState::Done;
                Some(Operation::Commit)
            }
            HoldState::Done => None,
        }
    }
}

/// Factory: each fiber that wins the chaotic roll gets its own
/// `ReaderHoldsSnapshot`. `hold_reads` is drawn per-fiber so the cohort has
/// readers with different snapshot lifetimes overlapping the checkpoint
/// schedule.
pub struct ReaderHoldProfile {
    /// Lower/upper bounds (inclusive) for the random `hold_reads` per fiber.
    pub min_reads: u32,
    pub max_reads: u32,
}

impl ReaderHoldProfile {
    pub fn new(min_reads: u32, max_reads: u32) -> Self {
        Self {
            min_reads,
            max_reads: max_reads.max(min_reads),
        }
    }
}

impl ChaoticWorkloadProfile for ReaderHoldProfile {
    fn generate(
        &self,
        mut rng: ChaCha8Rng,
        _fiber_id: usize,
    ) -> Box<dyn ChaoticWorkload> {
        let hold_reads = if self.min_reads == self.max_reads {
            self.min_reads
        } else {
            rng.random_range(self.min_reads..=self.max_reads)
        };
        Box::new(ReaderHoldsSnapshot {
            hold_reads,
            issued_reads: 0,
            // The profile has no access to sim_state, so we pick a plausible
            // initial-schema table/key. If `table_0` doesn't exist (the
            // initial-schema RNG happened to pick fewer tables), the SELECT
            // returns 0 rows and the read_view still gets established.
            table_name: Some("table_0".to_string()),
            key: Some("key_0".to_string()),
            state: HoldState::Begin,
        })
    }
}
