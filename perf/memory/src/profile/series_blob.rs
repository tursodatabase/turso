use super::{Phase, Profile, WorkItem};

const BLOB_SIZE_BYTES: i64 = 2 * 1024;

pub struct SeriesBlob {
    iterations: usize,
    batch_size: usize,
    current_iteration: usize,
    setup_done: bool,
}

impl SeriesBlob {
    pub fn new(iterations: usize, batch_size: usize) -> Self {
        Self {
            iterations,
            batch_size,
            current_iteration: 0,
            setup_done: false,
        }
    }
}

impl Profile for SeriesBlob {
    fn name(&self) -> &str {
        "series-blob"
    }

    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        if !self.setup_done {
            self.setup_done = true;
            return (
                Phase::Setup,
                vec![vec![WorkItem {
                    sql: "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, data BLOB NOT NULL)".to_string(),
                    params: vec![],
                }]],
            );
        }

        if self.current_iteration >= self.iterations {
            return (Phase::Done, vec![]);
        }

        let mut batches = Vec::with_capacity(connections);
        for _ in 0..connections {
            batches.push(vec![WorkItem {
                sql: "INSERT INTO bench (data) SELECT zeroblob(?) FROM generate_series(1, ?)"
                    .to_string(),
                params: vec![
                    turso::Value::Integer(BLOB_SIZE_BYTES),
                    turso::Value::Integer(self.batch_size as i64),
                ],
            }]);
        }

        self.current_iteration += 1;
        (Phase::Run, batches)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/profile/series_blob/tests.rs"]
mod tests;
