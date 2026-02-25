//! Chaotic Elle workloads for the concurrent simulator.
//!
//! These workloads generate multi-operation transactions that create rich
//! read-write dependency structures for Elle to analyze. Unlike the coordinated
//! hermitage workloads, chaotic Elle workloads have no barriers or coordination —
//! each is a simple state machine that walks through a pre-planned sequence of
//! Elle operations (reads and appends on the `elle_lists` table).
//!
//! The `ElleHistoryRecorder` property handles recording automatically, so these
//! workloads just emit `Operation::ElleRead` / `Operation::ElleAppend` /
//! `Operation::Begin` / `Operation::Commit` / `Operation::Rollback`.
//!
//! Seven transaction templates create diverse dependency patterns:
//!
//! | Template             | Pattern                        | Targets           |
//! |----------------------|--------------------------------|-------------------|
//! | ReadThenWriteElsewhere | Read X, Append Y             | G-single, G2      |
//! | MultiKeyReader       | Read 2-4 distinct keys         | snapshot inconsistency |
//! | ReadModifyWrite      | Read X, Append X               | G0, G2            |
//! | Aborter              | Append 1-3 keys, Rollback      | G1a               |
//! | LongReader           | Read X, middle ops, re-read X  | snapshot stability |
//! | BlindMultiWriter     | Append 2-3 keys, no reads      | G0                |
//! | MixedReadWrite       | 2-5 random reads/appends       | general deps      |

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::elle::{ELLE_LIST_APPEND_KEY_COUNT, ELLE_RW_REGISTER_KEY_COUNT, elle_key_name};
use crate::operations::{OpResult, Operation};

/// A chaotic workload instance (one per fiber, drives a single transaction).
///
/// Called with `next(None)` to get the first operation, then `next(Some(result))`
/// after each operation completes. Returns `None` when the workload is done.
pub trait ChaoticWorkload {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation>;
}

/// Factory that produces chaotic workload instances.
pub trait ChaoticWorkloadProfile: Send + Sync {
    fn generate(&self, rng: ChaCha8Rng, fiber_id: usize) -> Box<dyn ChaoticWorkload>;
}

/// Which Elle model this workload targets.
#[derive(Debug, Clone, Copy)]
pub enum ElleModelKind {
    ListAppend,
    RwRegister,
}

/// A single planned operation in a chaotic Elle transaction.
#[derive(Debug, Clone)]
enum PlannedOp {
    Begin,
    Read { key: String },
    Write { key: String, value: i64 },
    Commit,
    Rollback,
}

/// A chaotic Elle workload instance that walks through a pre-built operation sequence.
struct ChaoticElleWorkload {
    table_name: String,
    model: ElleModelKind,
    enable_mvcc: bool,
    ops: Vec<PlannedOp>,
    index: usize,
}

impl ChaoticElleWorkload {
    fn new(
        table_name: String,
        model: ElleModelKind,
        enable_mvcc: bool,
        ops: Vec<PlannedOp>,
    ) -> Self {
        Self {
            table_name,
            model,
            enable_mvcc,
            ops,
            index: 0,
        }
    }

    fn to_operation(&self, planned: &PlannedOp) -> Operation {
        match planned {
            PlannedOp::Begin => Operation::Begin {
                mode: if self.enable_mvcc {
                    "BEGIN CONCURRENT".into()
                } else {
                    "BEGIN".into()
                },
            },
            PlannedOp::Read { key } => match self.model {
                ElleModelKind::ListAppend => Operation::ElleRead {
                    table_name: self.table_name.clone(),
                    key: key.clone(),
                },
                ElleModelKind::RwRegister => Operation::ElleRwRead {
                    table_name: self.table_name.clone(),
                    key: key.clone(),
                },
            },
            PlannedOp::Write { key, value } => match self.model {
                ElleModelKind::ListAppend => Operation::ElleAppend {
                    table_name: self.table_name.clone(),
                    key: key.clone(),
                    value: *value,
                },
                ElleModelKind::RwRegister => Operation::ElleRwWrite {
                    table_name: self.table_name.clone(),
                    key: key.clone(),
                    value: *value,
                },
            },
            PlannedOp::Commit => Operation::Commit,
            PlannedOp::Rollback => Operation::Rollback,
        }
    }
}

impl ChaoticWorkload for ChaoticElleWorkload {
    fn next(&mut self, result: Option<OpResult>) -> Option<Operation> {
        // On error, bail out — simulator auto-rollback handles cleanup,
        // ElleHistoryRecorder emits :fail for the pending transaction.
        if let Some(Err(e)) = &result {
            tracing::trace!("operation error: {}", e);
            return None;
        }

        if self.index >= self.ops.len() {
            return None;
        }

        let planned = &self.ops[self.index];
        self.index += 1;
        Some(self.to_operation(planned))
    }
}

/// Factory that produces chaotic Elle workload instances.
///
/// Each call to `generate` randomly picks one of 7 transaction templates
/// and builds a `ChaoticElleWorkload` with a pre-planned op sequence.
pub struct ChaoticElleProfile {
    table_name: String,
    model: ElleModelKind,
    value_counter: Arc<AtomicI64>,
    enable_mvcc: bool,
}

impl ChaoticElleProfile {
    pub fn new(
        table_name: String,
        model: ElleModelKind,
        value_counter: Arc<AtomicI64>,
        enable_mvcc: bool,
    ) -> Self {
        Self {
            table_name,
            model,
            value_counter,
            enable_mvcc,
        }
    }

    /// Allocate a globally unique append value.
    fn next_value(&self) -> i64 {
        self.value_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Key count for this model.
    fn key_count(&self) -> usize {
        match self.model {
            ElleModelKind::ListAppend => ELLE_LIST_APPEND_KEY_COUNT,
            ElleModelKind::RwRegister => ELLE_RW_REGISTER_KEY_COUNT,
        }
    }

    /// Pick a random Elle key.
    fn random_key(&self, rng: &mut ChaCha8Rng) -> String {
        elle_key_name(rng.random_range(0..self.key_count()))
    }

    /// Pick N distinct random Elle keys.
    fn distinct_keys(&self, rng: &mut ChaCha8Rng, n: usize) -> Vec<String> {
        debug_assert!(
            n <= self.key_count(),
            "requested {n} distinct keys but only {} exist",
            self.key_count()
        );
        let mut keys = Vec::with_capacity(n);
        while keys.len() < n {
            let k = self.random_key(rng);
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        keys
    }

    /// Template: Read X, then Append Y (different key).
    /// Creates rw-dependency chains (G-single, G2).
    fn build_read_then_write_elsewhere(&self, rng: &mut ChaCha8Rng) -> Vec<PlannedOp> {
        let keys = self.distinct_keys(rng, 2);
        vec![
            PlannedOp::Begin,
            PlannedOp::Read {
                key: keys[0].clone(),
            },
            PlannedOp::Write {
                key: keys[1].clone(),
                value: self.next_value(),
            },
            PlannedOp::Commit,
        ]
    }

    /// Template: Read 2-4 distinct keys.
    /// Detects snapshot inconsistency.
    fn build_multi_key_reader(&self, rng: &mut ChaCha8Rng) -> Vec<PlannedOp> {
        let n = rng.random_range(2..=4);
        let keys = self.distinct_keys(rng, n);
        let mut ops = vec![PlannedOp::Begin];
        for key in keys {
            ops.push(PlannedOp::Read { key });
        }
        ops.push(PlannedOp::Commit);
        ops
    }

    /// Template: Read X, then Append X (same key).
    /// Creates lost-update potential (G0, G2).
    fn build_read_modify_write(&self, rng: &mut ChaCha8Rng) -> Vec<PlannedOp> {
        let key = self.random_key(rng);
        vec![
            PlannedOp::Begin,
            PlannedOp::Read { key: key.clone() },
            PlannedOp::Write {
                key,
                value: self.next_value(),
            },
            PlannedOp::Commit,
        ]
    }

    /// Template: Append 1-3 keys, then Rollback.
    /// Creates G1a (aborted reads) detection opportunity.
    fn build_aborter(&self, rng: &mut ChaCha8Rng) -> Vec<PlannedOp> {
        let n = rng.random_range(1..=3);
        let mut ops = vec![PlannedOp::Begin];
        for _ in 0..n {
            ops.push(PlannedOp::Write {
                key: self.random_key(rng),
                value: self.next_value(),
            });
        }
        ops.push(PlannedOp::Rollback);
        ops
    }

    /// Template: Read X, do middle ops, re-read X.
    /// Tests snapshot stability within a transaction.
    fn build_long_reader(&self, rng: &mut ChaCha8Rng) -> Vec<PlannedOp> {
        let key = self.random_key(rng);
        let mut ops = vec![PlannedOp::Begin, PlannedOp::Read { key: key.clone() }];

        // 1-3 middle operations on keys other than the re-read key
        let middle_count = rng.random_range(1..=3);
        for _ in 0..middle_count {
            let mut other_key = self.random_key(rng);
            while other_key == key {
                other_key = self.random_key(rng);
            }
            if rng.random_bool(0.5) {
                ops.push(PlannedOp::Read { key: other_key });
            } else {
                ops.push(PlannedOp::Write {
                    key: other_key,
                    value: self.next_value(),
                });
            }
        }

        // Re-read the same key
        ops.push(PlannedOp::Read { key });
        ops.push(PlannedOp::Commit);
        ops
    }

    /// Template: Append 2-3 keys, no reads.
    /// Creates G0 (dirty write) detection opportunity.
    fn build_blind_multi_writer(&self, rng: &mut ChaCha8Rng) -> Vec<PlannedOp> {
        let n = rng.random_range(2..=3);
        let mut ops = vec![PlannedOp::Begin];
        for _ in 0..n {
            ops.push(PlannedOp::Write {
                key: self.random_key(rng),
                value: self.next_value(),
            });
        }
        ops.push(PlannedOp::Commit);
        ops
    }

    /// Template: 2-5 random reads/appends.
    /// General dependency creation.
    fn build_mixed_read_write(&self, rng: &mut ChaCha8Rng) -> Vec<PlannedOp> {
        let n = rng.random_range(2..=5);
        let mut ops = vec![PlannedOp::Begin];
        for _ in 0..n {
            let key = self.random_key(rng);
            if rng.random_bool(0.5) {
                ops.push(PlannedOp::Read { key });
            } else {
                ops.push(PlannedOp::Write {
                    key,
                    value: self.next_value(),
                });
            }
        }
        ops.push(PlannedOp::Commit);
        ops
    }
}

impl ChaoticWorkloadProfile for ChaoticElleProfile {
    fn generate(&self, mut rng: ChaCha8Rng, _fiber_id: usize) -> Box<dyn ChaoticWorkload> {
        // For rw-register, weight ReadModifyWrite (read-then-write-same-key) heavily
        // since it creates both rw and wr dependencies on a single key — the "2rw rule"
        // that rw-register needs for cycle detection.
        let ops = match self.model {
            ElleModelKind::RwRegister => match rng.random_range(0..9u32) {
                0..=2 => self.build_read_modify_write(&mut rng), // 3/9
                3 => self.build_read_then_write_elsewhere(&mut rng), // 1/9
                4 => self.build_multi_key_reader(&mut rng),      // 1/9
                5 => self.build_aborter(&mut rng),               // 1/9
                6 => self.build_long_reader(&mut rng),           // 1/9
                7 => self.build_blind_multi_writer(&mut rng),    // 1/9
                _ => self.build_mixed_read_write(&mut rng),      // 1/9
            },
            ElleModelKind::ListAppend => match rng.random_range(0..7u32) {
                0 => self.build_read_then_write_elsewhere(&mut rng),
                1 => self.build_multi_key_reader(&mut rng),
                2 => self.build_read_modify_write(&mut rng),
                3 => self.build_aborter(&mut rng),
                4 => self.build_long_reader(&mut rng),
                5 => self.build_blind_multi_writer(&mut rng),
                _ => self.build_mixed_read_write(&mut rng),
            },
        };

        Box::new(ChaoticElleWorkload::new(
            self.table_name.clone(),
            self.model,
            self.enable_mvcc,
            ops,
        ))
    }
}
