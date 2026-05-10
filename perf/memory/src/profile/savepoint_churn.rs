use super::{Phase, Profile, WorkItem};

/// Long-running write transaction with K savepoints, each touching the
/// same M index keys. Designed to grow per-key MVCC chains so that
/// Step 5 of `MVCC_COMMIT_MEMORY_NEXT.md` (Arc-ifying
/// `SortableIndexKey.key`) can be honestly evaluated — the audit gate
/// that decides whether to ship Step 5 needs an Arc share factor > 2,
/// which only emerges when the same logical key has multiple in-flight
/// versions in a single tx.
///
/// Setup creates `t (id INTEGER PRIMARY KEY, val INTEGER)` and a
/// secondary index on `val`, then seeds `M = SAVEPOINT_ROWS` rows.
/// Each Run iteration opens a tx, then issues `K = batch_size /
/// SAVEPOINT_ROWS` savepoint cycles. Each savepoint UPDATEs every
/// seeded row, which writes a new index entry for the new `val` (and a
/// tombstone for the old one). After K cycles, the tx commits, with all
/// K * M version chain entries flushed to the log.
pub struct SavepointChurn {
    iterations: usize,
    batch_size: usize,
    setup_phase: SetupPhase,
    seeded: usize,
    iter_done: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupPhase {
    NeedsTable,
    NeedsIndex,
    NeedsRows,
    Done,
}

const SAVEPOINT_ROWS: usize = 1000;
const SEED_CHUNK: usize = 1000;

impl SavepointChurn {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            setup_phase: SetupPhase::NeedsTable,
            seeded: 0,
            iter_done: 0,
        }
    }
}

impl Profile for SavepointChurn {
    fn name(&self) -> &str {
        "savepoint-churn"
    }

    fn next_batch(&mut self, _connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        match self.setup_phase {
            SetupPhase::NeedsTable => {
                self.setup_phase = SetupPhase::NeedsIndex;
                return (
                    Phase::Setup,
                    vec![vec![WorkItem {
                        sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)".to_string(),
                        params: vec![],
                    }]],
                );
            }
            SetupPhase::NeedsIndex => {
                self.setup_phase = SetupPhase::NeedsRows;
                return (
                    Phase::Setup,
                    vec![vec![WorkItem {
                        sql: "CREATE INDEX idx_val ON t(val)".to_string(),
                        params: vec![],
                    }]],
                );
            }
            SetupPhase::NeedsRows => {
                if self.seeded >= SAVEPOINT_ROWS {
                    self.setup_phase = SetupPhase::Done;
                } else {
                    let end = (self.seeded + SEED_CHUNK).min(SAVEPOINT_ROWS);
                    let mut items = Vec::with_capacity(end - self.seeded);
                    for j in self.seeded..end {
                        items.push(WorkItem {
                            sql: "INSERT INTO t (id, val) VALUES (?, ?)".to_string(),
                            params: vec![
                                turso::Value::Integer(j as i64),
                                turso::Value::Integer(j as i64),
                            ],
                        });
                    }
                    self.seeded = end;
                    return (Phase::Setup, vec![items]);
                }
            }
            SetupPhase::Done => {}
        }

        if self.iter_done >= self.iterations {
            return (Phase::Done, vec![]);
        }
        self.iter_done += 1;

        let savepoint_count = self.batch_size.div_ceil(SAVEPOINT_ROWS).max(1);
        let mut items = Vec::with_capacity(savepoint_count * 2 + 1);
        for k in 0..savepoint_count {
            let name = format!("sp_{k}");
            items.push(WorkItem {
                sql: format!("SAVEPOINT {name}"),
                params: vec![],
            });
            // Updating `val` rewrites the index entry: old val gets a
            // tombstone in its key's chain, new val gets a fresh chain
            // entry. Repeating across K savepoints stacks K-deep
            // versions on the SAVEPOINT_ROWS keys touched.
            items.push(WorkItem {
                sql: "UPDATE t SET val = val + 1".to_string(),
                params: vec![],
            });
            items.push(WorkItem {
                sql: format!("RELEASE {name}"),
                params: vec![],
            });
        }
        (Phase::Run, vec![items])
    }
}
