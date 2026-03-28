use super::{Phase, Profile, WorkItem};

const SEED_ROWS: usize = 10_000;

pub struct ScanHeavy {
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

impl ScanHeavy {
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

impl Profile for ScanHeavy {
    fn name(&self) -> &str {
        "scan-heavy"
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

                let mut batches = Vec::with_capacity(connections);
                for _ in 0..connections {
                    let mut items = Vec::with_capacity(self.batch_size);
                    for _ in 0..self.batch_size {
                        let pattern = format!("seed_{}", self.current_iteration % SEED_ROWS);
                        items.push(WorkItem {
                            sql: "SELECT * FROM bench WHERE data LIKE ?".to_string(),
                            params: vec![turso::Value::Text(format!("%{pattern}%"))],
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
