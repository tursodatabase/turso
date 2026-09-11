use super::{Phase, Profile, WorkItem};

/// Recursive-CTE workload covering the queue shapes with distinct memory
/// behavior: a constant-width linear queue, a priority queue, a global UNION
/// distinct set, and an infinite producer stopped by an outer LIMIT.
pub struct RecursiveCte {
    iterations: usize,
    output_rows: usize,
    current_iteration: usize,
}

impl RecursiveCte {
    pub fn new(iterations: usize, output_rows: usize) -> Self {
        Self {
            iterations,
            output_rows: output_rows.max(1),
            current_iteration: 0,
        }
    }
}

impl Profile for RecursiveCte {
    fn name(&self) -> &str {
        "recursive-cte"
    }

    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>) {
        if self.current_iteration >= self.iterations {
            return (Phase::Done, vec![]);
        }

        let rows = self.output_rows as i64;
        let mut batches = Vec::with_capacity(connections);
        for _ in 0..connections {
            batches.push(vec![
                WorkItem {
                    sql: "SELECT (WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < ?) SELECT count(*) FROM seq)".to_string(),
                    params: vec![turso::Value::Integer(rows)],
                },
                WorkItem {
                    sql: "SELECT (WITH RECURSIVE tree(x, depth) AS (VALUES(1, 0) UNION ALL SELECT x * 2, depth + 1 FROM tree WHERE depth < 20 UNION ALL SELECT x * 2 + 1, depth + 1 FROM tree WHERE depth < 20 ORDER BY 2, 1 LIMIT ?) SELECT count(*) FROM tree)".to_string(),
                    params: vec![turso::Value::Integer(rows)],
                },
                WorkItem {
                    sql: "SELECT (WITH RECURSIVE cycle(x) AS (VALUES(0) UNION SELECT (x + 1) % ? FROM cycle) SELECT count(*) FROM cycle)".to_string(),
                    params: vec![turso::Value::Integer(rows)],
                },
                WorkItem {
                    sql: "SELECT count(*) FROM (WITH RECURSIVE infinite(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM infinite) SELECT x FROM infinite LIMIT ?)".to_string(),
                    params: vec![turso::Value::Integer(rows)],
                },
            ]);
        }

        self.current_iteration += 1;
        (Phase::Run, batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_cte_profile_emits_all_queue_shapes_per_connection() {
        let mut profile = RecursiveCte::new(1, 128);
        let (phase, batches) = profile.next_batch(3);

        assert_eq!(phase, Phase::Run);
        assert_eq!(batches.len(), 3);
        for batch in batches {
            assert_eq!(batch.len(), 4);
            assert!(batch[0].sql.contains("UNION ALL"));
            assert!(batch[1].sql.contains("ORDER BY 2, 1 LIMIT ?"));
            assert!(batch[2].sql.contains("UNION SELECT"));
            assert!(batch[3].sql.contains("infinite"));
            assert!(
                batch
                    .iter()
                    .all(|item| { item.params == vec![turso::Value::Integer(128)] })
            );
        }

        let (phase, batches) = profile.next_batch(3);
        assert_eq!(phase, Phase::Done);
        assert!(batches.is_empty());
    }

    #[test]
    fn recursive_cte_profile_never_generates_zero_cardinality() {
        let mut profile = RecursiveCte::new(1, 0);
        let (_, batches) = profile.next_batch(1);
        assert!(
            batches[0]
                .iter()
                .all(|item| item.params == vec![turso::Value::Integer(1)])
        );
    }
}
