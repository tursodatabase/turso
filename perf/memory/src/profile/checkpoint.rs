use super::{Phase, Profile, WorkItem};

pub struct Checkpoint {
    inner: Box<dyn Profile>,
    name: String,
    needs_checkpoint: bool,
}

impl Checkpoint {
    pub fn new(inner: Box<dyn Profile>) -> Self {
        let name = format!("{}+checkpoint", inner.name());
        Self {
            inner,
            name,
            needs_checkpoint: true,
        }
    }
}

impl Profile for Checkpoint {
    fn name(&self) -> &str {
        &self.name
    }

    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        if !self.needs_checkpoint {
            return (Phase::Done, vec![]);
        }

        let (phase, batches) = self.inner.next_batch(connections);
        if phase != Phase::Done {
            return (phase, batches);
        }

        self.needs_checkpoint = false;
        (
            Phase::Checkpoint,
            vec![vec![WorkItem {
                sql: "PRAGMA wal_checkpoint(TRUNCATE)".to_string(),
                params: vec![],
            }]],
        )
    }
}

#[cfg(test)]
#[path = "../../tests/unit/profile/checkpoint/tests.rs"]
mod tests;
