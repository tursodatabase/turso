use rand::Rng;

use super::{Phase, Profile, WorkItem};

const SEED_ROWS: usize = 10_000;

/// Update-churn workload: seed a fixed set of rows, then repeatedly UPDATE
/// random rows within that set under (concurrent) short transactions.
///
/// Each UPDATE to an existing row creates a new committed version and
/// supersedes the prior one, so the version store accumulates *superseded*
/// versions — the exact garbage MVCC GC reclaims once they fall below the
/// low-water mark. Because the live key set is bounded at `SEED_ROWS`, a
/// healthy GC keeps memory flat at ~`SEED_ROWS` current versions, while no GC
/// grows the store with every update. This is the workload that exercises the
/// inline-GC path (decoupled from the stop-the-world checkpoint).
pub struct UpdateChurn {
    iterations: usize,
    batch_size: usize,
    current_iteration: usize,
    phase: InternalPhase,
    seed_offset: usize,
}

enum InternalPhase {
    CreateTable,
    Seed,
    Run,
}

impl UpdateChurn {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            current_iteration: 0,
            phase: InternalPhase::CreateTable,
            seed_offset: 0,
        }
    }
}

impl Profile for UpdateChurn {
    fn name(&self) -> &str {
        "update-churn"
    }

    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        match self.phase {
            InternalPhase::CreateTable => {
                self.phase = InternalPhase::Seed;
                (
                    Phase::Setup,
                    vec![vec![WorkItem {
                        sql: "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, data TEXT NOT NULL, value REAL)".to_string(),
                        params: vec![],
                    }]],
                )
            }
            InternalPhase::Seed => {
                let remaining = SEED_ROWS - self.seed_offset;
                let batch = remaining.min(500);
                let mut items = Vec::with_capacity(batch);
                for i in 0..batch {
                    let id = self.seed_offset + i;
                    items.push(WorkItem {
                        sql: "INSERT INTO bench (id, data, value) VALUES (?, ?, ?)".to_string(),
                        params: vec![
                            turso::Value::Integer(id as i64),
                            turso::Value::Text(format!("seed_{id}")),
                            turso::Value::Real(id as f64 * 0.5),
                        ],
                    });
                }
                self.seed_offset += batch;
                if self.seed_offset >= SEED_ROWS {
                    self.phase = InternalPhase::Run;
                }
                (Phase::Setup, vec![items])
            }
            InternalPhase::Run => {
                if self.current_iteration >= self.iterations {
                    return (Phase::Done, vec![]);
                }

                let mut rng = rand::rng();
                let mut batches = Vec::with_capacity(connections);
                // Partition the key space across connections so concurrent
                // writers never collide on the same row (no write-write
                // conflicts), while each still churns its own slice repeatedly.
                let chunk = (SEED_ROWS / connections.max(1)).max(1) as i64;
                for c in 0..connections {
                    let lo = c as i64 * chunk;
                    let hi = (lo + chunk).min(SEED_ROWS as i64);
                    let mut items = Vec::with_capacity(self.batch_size);
                    for _ in 0..self.batch_size {
                        let id = rng.random_range(lo..hi.max(lo + 1));
                        items.push(WorkItem {
                            sql: "UPDATE bench SET data = ?, value = ? WHERE id = ?".to_string(),
                            params: vec![
                                turso::Value::Text(format!(
                                    "upd_{}_{}",
                                    self.current_iteration, id
                                )),
                                turso::Value::Real(id as f64 * 1.1),
                                turso::Value::Integer(id),
                            ],
                        });
                    }
                    batches.push(items);
                }

                self.current_iteration += 1;
                (Phase::Run, batches)
            }
        }
    }
}
