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
mod tests {
    use super::*;

    struct ScriptedProfile {
        name: &'static str,
        steps: std::vec::IntoIter<(Phase, Vec<Vec<WorkItem>>)>,
    }

    impl ScriptedProfile {
        fn new(steps: Vec<(Phase, Vec<Vec<WorkItem>>)>) -> Self {
            Self {
                name: "scripted",
                steps: steps.into_iter(),
            }
        }
    }

    impl Profile for ScriptedProfile {
        fn name(&self) -> &str {
            self.name
        }

        fn next_batch(&mut self, _connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
            self.steps.next().unwrap_or((Phase::Done, vec![]))
        }
    }

    #[test]
    fn checkpoint_profile_runs_after_inner_profile_completes() {
        let setup = WorkItem {
            sql: "CREATE TABLE bench(id INTEGER PRIMARY KEY)".to_string(),
            params: vec![],
        };
        let run = WorkItem {
            sql: "SELECT * FROM bench".to_string(),
            params: vec![],
        };

        let inner = Box::new(ScriptedProfile::new(vec![
            (Phase::Setup, vec![vec![setup]]),
            (Phase::Run, vec![vec![run]]),
            (Phase::Done, vec![]),
        ]));
        let mut profile = Checkpoint::new(inner);

        assert_eq!(profile.name(), "scripted+checkpoint");

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Setup);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(
            batches[0][0].sql,
            "CREATE TABLE bench(id INTEGER PRIMARY KEY)"
        );

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Run);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].sql, "SELECT * FROM bench");

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Checkpoint);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].sql, "PRAGMA wal_checkpoint(TRUNCATE)");
        assert!(batches[0][0].params.is_empty());

        let (phase, batches) = profile.next_batch(1);
        assert_eq!(phase, Phase::Done);
        assert!(batches.is_empty());
    }
}
