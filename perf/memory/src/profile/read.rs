use rand::Rng;

use super::{Phase, Profile, WorkItem};

const SEED_ROWS: usize = 10_000;

pub struct ReadHeavy {
    iterations: usize,
    batch_size: usize,
    current_iteration: usize,
    phase: InternalPhase,
    seed_offset: usize,
    total_rows: usize,
    insert_id: usize,
}

enum InternalPhase {
    CreateTable,
    Seed,
    Run,
}

impl ReadHeavy {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            current_iteration: 0,
            phase: InternalPhase::CreateTable,
            seed_offset: 0,
            total_rows: SEED_ROWS,
            insert_id: SEED_ROWS,
        }
    }
}

impl Profile for ReadHeavy {
    fn name(&self) -> &str {
        "read-heavy"
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
                for _ in 0..connections {
                    let mut items = Vec::with_capacity(self.batch_size);
                    for _ in 0..self.batch_size {
                        // 90% reads, 10% writes
                        if rng.random_range(0..10) < 9 {
                            let id = rng.random_range(0..self.total_rows as i64);
                            items.push(WorkItem {
                                sql: "SELECT * FROM bench WHERE id = ?".to_string(),
                                params: vec![turso::Value::Integer(id)],
                            });
                        } else {
                            items.push(WorkItem {
                                sql: "INSERT INTO bench (id, data, value) VALUES (?, ?, ?)"
                                    .to_string(),
                                params: vec![
                                    turso::Value::Integer(self.insert_id as i64),
                                    turso::Value::Text(format!("new_{}", self.insert_id)),
                                    turso::Value::Real(self.insert_id as f64 * 1.1),
                                ],
                            });
                            self.insert_id += 1;
                            self.total_rows += 1;
                        }
                    }
                    batches.push(items);
                }

                self.current_iteration += 1;
                (Phase::Run, batches)
            }
        }
    }
}
