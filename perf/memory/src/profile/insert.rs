use super::{Phase, Profile, WorkItem};

pub struct InsertHeavy {
    iterations: usize,
    batch_size: usize,
    current_iteration: usize,
    setup_done: bool,
    row_id: usize,
}

impl InsertHeavy {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            current_iteration: 0,
            setup_done: false,
            row_id: 0,
        }
    }
}

impl Profile for InsertHeavy {
    fn name(&self) -> &str {
        "insert-heavy"
    }

    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        if !self.setup_done {
            self.setup_done = true;
            return (
                Phase::Setup,
                vec![vec![WorkItem {
                    sql: "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, data TEXT NOT NULL, value REAL)".to_string(),
                    params: vec![],
                }]],
            );
        }

        if self.current_iteration >= self.iterations {
            return (Phase::Done, vec![]);
        }

        let mut batches = Vec::with_capacity(connections);
        for _ in 0..connections {
            let mut items = Vec::with_capacity(self.batch_size);
            for _ in 0..self.batch_size {
                items.push(WorkItem {
                    sql: "INSERT INTO bench (id, data, value) VALUES (?, ?, ?)".to_string(),
                    params: vec![
                        turso::Value::Integer(self.row_id as i64),
                        turso::Value::Text(format!("data_{}", self.row_id)),
                        turso::Value::Real((self.row_id as f64) * 1.1),
                    ],
                });
                self.row_id += 1;
            }
            batches.push(items);
        }

        self.current_iteration += 1;
        (Phase::Run, batches)
    }
}
