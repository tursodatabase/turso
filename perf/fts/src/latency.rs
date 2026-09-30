use std::sync::Barrier;
use std::time::Instant;

use anyhow::{Result, ensure};
use memory_benchmark::fts::RunResult;
use tokio::runtime::Runtime;

use crate::Measurement;

pub fn measure<S: Send>(
    sessions: Vec<S>,
    queries: usize,
    execute: impl Fn(&mut S, &Runtime) -> Result<RunResult> + Sync,
) -> Result<Measurement> {
    let connections = sessions.len();
    ensure!(
        connections > 0 && queries >= connections,
        "need at least one query per connection"
    );
    let workers = sessions
        .into_iter()
        .map(|session| {
            Ok((
                session,
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let barrier = Barrier::new(connections + 1);
    std::thread::scope(|scope| {
        let handles: Vec<_> = workers
            .into_iter()
            .enumerate()
            .map(|(index, (mut session, runtime))| {
                let execute = &execute;
                let barrier = &barrier;
                scope.spawn(move || {
                    let count = queries / connections + usize::from(index < queries % connections);
                    let mut latencies = Vec::with_capacity(count);
                    let mut result = RunResult::default();
                    barrier.wait();
                    for _ in 0..count {
                        let start = Instant::now();
                        let completed = execute(&mut session, &runtime)?;
                        latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                        assert_eq!(completed.queries, 1);
                        result.queries += 1;
                        result.rows += completed.rows;
                        result.id_sum += completed.id_sum;
                    }
                    Ok::<_, anyhow::Error>((result, latencies))
                })
            })
            .collect();
        let start = Instant::now();
        barrier.wait();
        let outcomes: Vec<_> = handles.into_iter().map(|handle| handle.join()).collect();
        let seconds = start.elapsed().as_secs_f64();
        let mut measurement = Measurement {
            result: RunResult::default(),
            seconds,
            latencies: Vec::with_capacity(queries),
        };
        for outcome in outcomes {
            let (result, latencies) =
                outcome.map_err(|_| anyhow::anyhow!("latency worker panicked"))??;
            measurement.result.queries += result.queries;
            measurement.result.rows += result.rows;
            measurement.result.id_sum += result.id_sum;
            measurement.latencies.extend(latencies);
        }
        Ok(measurement)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn uses_distinct_threads_and_records_each_query_with_uneven_work() -> Result<()> {
        let counts = Mutex::new([0; 3]);
        let threads = Mutex::new(HashSet::new());
        let start = Barrier::new(3);
        let result = measure(vec![0, 1, 2], 10, |index, _| {
            let first = {
                let mut counts = counts.lock().unwrap();
                counts[*index] += 1;
                counts[*index] == 1
            };
            if first {
                start.wait();
            }
            threads.lock().unwrap().insert(std::thread::current().id());
            Ok(RunResult {
                queries: 1,
                rows: *index + 1,
                id_sum: *index as i64 * 7,
                ..Default::default()
            })
        })?;
        assert_eq!(*counts.lock().unwrap(), [4, 3, 3]);
        assert_eq!(threads.lock().unwrap().len(), 3);
        assert_eq!(result.latencies.len(), 10);
        assert_eq!(result.result.queries, 10);
        assert_eq!(result.result.rows, 19);
        assert_eq!(result.result.id_sum, 63);
        assert!(measure(vec![0, 1], 1, |_, _| unreachable!()).is_err());
        Ok(())
    }
}
