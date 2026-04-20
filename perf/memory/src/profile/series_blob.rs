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
mod tests {
    use super::*;

    #[test]
    fn series_blob_profile_uses_batch_size_as_series_stop() {
        let mut profile = SeriesBlob::new(2, 7);

        let (phase, batches) = profile.next_batch(3);
        assert_eq!(phase, Phase::Setup);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(
            batches[0][0].sql,
            "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, data BLOB NOT NULL)"
        );
        assert!(batches[0][0].params.is_empty());

        let (phase, batches) = profile.next_batch(3);
        assert_eq!(phase, Phase::Run);
        assert_eq!(batches.len(), 3);

        for batch in batches {
            assert_eq!(batch.len(), 1);
            let item = &batch[0];
            assert_eq!(
                item.sql,
                "INSERT INTO bench (data) SELECT zeroblob(?) FROM generate_series(1, ?)"
            );
            assert_eq!(item.params.len(), 2);
            assert_eq!(item.params[0], turso::Value::Integer(BLOB_SIZE_BYTES));
            assert_eq!(item.params[1], turso::Value::Integer(7));
        }
    }

    #[test]
    fn series_blob_profile_stops_after_iterations() {
        let mut profile = SeriesBlob::new(1, 4);

        let (phase, _) = profile.next_batch(1);
        assert_eq!(phase, Phase::Setup);

        let (phase, _) = profile.next_batch(1);
        assert_eq!(phase, Phase::Run);

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Done);
        assert!(batches.is_empty());
    }
}
