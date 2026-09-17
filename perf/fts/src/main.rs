use anyhow::{Result, ensure};
use clap::{Parser, ValueEnum};
use memory_benchmark::fts::{
    CorpusConfig, Execution, FtsConfig, FtsWorkload, QueryCase, QueryState, RunResult,
};
use memory_benchmark::workload::JournalMode;
use std::io::{Write, stdout};
use std::time::Instant;

mod latency;
#[cfg(all(feature = "flamegraph", any(target_os = "linux", target_os = "macos")))]
mod profiling;
mod sqlite;

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum)]
    benchmark: Benchmark,
    #[arg(long, default_value_t = 10_000)]
    documents: usize,
    #[arg(long, default_value_t = 5)]
    runs: usize,
    #[arg(long, value_enum, default_value = "warm")]
    state: QueryState,
    #[arg(long)]
    queries: Option<usize>,
    #[arg(long, conflicts_with = "queries")]
    seconds: Option<f64>,
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "sqlite-wal,turso-wal,turso-mvcc"
    )]
    targets: Vec<Target>,
    #[arg(long, default_value_t = 1)]
    connections: usize,
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "rare,common,and,or,phrase,ranked"
    )]
    cases: Vec<QueryCase>,
    #[cfg(all(feature = "flamegraph", any(target_os = "linux", target_os = "macos")))]
    #[arg(long)]
    flamegraph: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.runs > 0, "runs must be positive");
    ensure!(args.connections > 0, "connections must be positive");
    eprintln!("bundled SQLite version: {}", rusqlite::version());
    let runtime = match args.benchmark {
        Benchmark::Search => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?,
        Benchmark::Throughput => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(args.connections)
            .max_blocking_threads(args.connections)
            .enable_all()
            .build()?,
    };
    runtime.block_on(run(args))
}

async fn run(mut args: Args) -> Result<()> {
    let (query_limit, min_seconds) = match args.benchmark {
        Benchmark::Search => {
            ensure!(
                args.seconds.is_none(),
                "--seconds requires --benchmark throughput"
            );
            let queries = args
                .queries
                .unwrap_or(if matches!(args.state, QueryState::First) {
                    args.connections
                } else {
                    10_000
                });
            ensure!(
                queries >= args.connections,
                "need at least one query per connection"
            );
            ensure!(
                !matches!(args.state, QueryState::First) || queries == args.connections,
                "first-query latency requires one query per connection"
            );
            (queries, 0.0)
        }
        Benchmark::Throughput => {
            ensure!(
                args.queries.is_none(),
                "--queries requires --benchmark search"
            );
            ensure!(
                matches!(args.state, QueryState::Warm),
                "throughput requires --state warm"
            );
            let seconds = args.seconds.unwrap_or(5.0);
            ensure!(
                seconds.is_finite() && seconds > 0.0,
                "seconds must be finite and positive"
            );
            (0, seconds)
        }
    };
    #[cfg(all(feature = "flamegraph", any(target_os = "linux", target_os = "macos")))]
    let profiled = args.flamegraph.is_some();
    #[cfg(not(all(feature = "flamegraph", any(target_os = "linux", target_os = "macos"))))]
    let profiled = false;
    #[cfg(all(feature = "flamegraph", any(target_os = "linux", target_os = "macos")))]
    if let Some(directory) = &args.flamegraph {
        std::fs::create_dir(directory)?;
    }
    let mut output = stdout().lock();
    writeln!(
        output,
        "benchmark,engine,mode,state,documents,connections,queries,run,query,seconds,rows,id_sum,debug_assertions,requested_queries,min_seconds,p50_ms,p95_ms,p99_ms,profiled"
    )?;
    for repetition in 1..=args.runs {
        for &query in &args.cases {
            for &target in &args.targets {
                #[cfg(all(feature = "flamegraph", any(target_os = "linux", target_os = "macos")))]
                let profile_path = args.flamegraph.as_ref().map(|directory| {
                    directory.join(format!(
                        "{}-{}-{}-c{}-run{}-{}",
                        args.benchmark.to_possible_value().unwrap().get_name(),
                        target.engine(),
                        target.mode(),
                        args.connections,
                        repetition,
                        query.to_possible_value().unwrap().get_name()
                    ))
                });
                let config = FtsConfig {
                    query,
                    state: args.state,
                    documents: args.documents,
                    corpus: CorpusConfig::default(),
                    mode: target.mode(),
                    connections: args.connections,
                    execution: Execution::Queries(1),
                };
                let mut measured = match target {
                    Target::SqliteWal => {
                        let mut workload = sqlite::SqliteWorkload::prepare(config).await?;
                        #[cfg(all(
                            feature = "flamegraph",
                            any(target_os = "linux", target_os = "macos")
                        ))]
                        let profiler = profiling::start(profiled)?;
                        let measured = match args.benchmark {
                            Benchmark::Search => latency::measure(
                                workload.sessions().iter_mut().collect(),
                                query_limit,
                                |session, _| sqlite::query_batch(session, query, 1),
                            )?,
                            Benchmark::Throughput => {
                                measure_throughput(min_seconds, async || workload.run().await)
                                    .await?
                            }
                        };
                        #[cfg(all(
                            feature = "flamegraph",
                            any(target_os = "linux", target_os = "macos")
                        ))]
                        profiling::finish(profiler, profile_path.as_deref())?;
                        measured
                    }
                    Target::TursoWal | Target::TursoMvcc => {
                        let workload = FtsWorkload::prepare(config, &mut ()).await?;
                        #[cfg(all(
                            feature = "flamegraph",
                            any(target_os = "linux", target_os = "macos")
                        ))]
                        let profiler = profiling::start(profiled)?;
                        let measured = match args.benchmark {
                            Benchmark::Search => latency::measure(
                                workload.sessions().to_vec(),
                                query_limit,
                                |session, runtime| {
                                    let result = runtime.block_on(session.query(query))?;
                                    Ok(RunResult {
                                        queries: 1,
                                        rows: result.rows,
                                        id_sum: result.id_sum,
                                        ..Default::default()
                                    })
                                },
                            )?,
                            Benchmark::Throughput => {
                                measure_throughput(min_seconds, async || {
                                    workload.run(&mut ()).await
                                })
                                .await?
                            }
                        };
                        #[cfg(all(
                            feature = "flamegraph",
                            any(target_os = "linux", target_os = "macos")
                        ))]
                        profiling::finish(profiler, profile_path.as_deref())?;
                        workload.finish(&mut ());
                        measured
                    }
                };
                let result = measured.result;
                validate_result(
                    query,
                    args.documents,
                    result.queries,
                    result.rows,
                    result.id_sum,
                )?;
                writeln!(
                    output,
                    "{},{},{},{},{},{},{},{},{},{:.9},{},{},{},{},{},{},{},{},{}",
                    args.benchmark.to_possible_value().unwrap().get_name(),
                    target.engine(),
                    target.mode(),
                    serde_json::to_value(args.state)?.as_str().unwrap(),
                    args.documents,
                    args.connections,
                    result.queries,
                    repetition,
                    serde_json::to_value(query)?.as_str().unwrap(),
                    measured.seconds,
                    result.rows,
                    result.id_sum,
                    cfg!(debug_assertions),
                    query_limit,
                    min_seconds,
                    percentile(&mut measured.latencies, 50),
                    percentile(&mut measured.latencies, 95),
                    percentile(&mut measured.latencies, 99),
                    profiled
                )?;
            }
        }
        args.targets.rotate_left(1);
    }
    Ok(())
}

async fn measure_throughput(
    min_seconds: f64,
    mut batch: impl AsyncFnMut() -> Result<RunResult>,
) -> Result<Measurement> {
    let start = Instant::now();
    let mut result = RunResult::default();
    loop {
        let completed = batch().await?;
        result.queries += completed.queries;
        result.transactions += completed.transactions;
        result.rows += completed.rows;
        result.id_sum += completed.id_sum;
        result.max_active_transactions = result
            .max_active_transactions
            .max(completed.max_active_transactions);
        let seconds = start.elapsed().as_secs_f64();
        if seconds >= min_seconds {
            return Ok(Measurement {
                result,
                seconds,
                latencies: Vec::new(),
            });
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum Benchmark {
    Search,
    Throughput,
}

struct Measurement {
    result: RunResult,
    seconds: f64,
    latencies: Vec<f64>,
}

fn percentile(latencies: &mut [f64], percent: usize) -> String {
    if latencies.is_empty() {
        return String::new();
    }
    latencies.sort_unstable_by(f64::total_cmp);
    let index = (latencies.len() * percent).div_ceil(100) - 1;
    format!("{:.9}", latencies[index])
}

#[derive(Clone, Copy, ValueEnum)]
enum Target {
    SqliteWal,
    TursoWal,
    TursoMvcc,
}

impl Target {
    fn mode(self) -> JournalMode {
        match self {
            Self::SqliteWal | Self::TursoWal => JournalMode::Wal,
            Self::TursoMvcc => JournalMode::Mvcc,
        }
    }

    fn engine(self) -> &'static str {
        match self {
            Self::SqliteWal => "sqlite",
            Self::TursoWal | Self::TursoMvcc => "turso",
        }
    }
}

fn validate_result(
    case: QueryCase,
    documents: usize,
    queries: usize,
    rows: usize,
    sum: i64,
) -> Result<()> {
    let ids = (0..documents).filter(|id| match case {
        QueryCase::Rare => id % 100 == 0,
        QueryCase::Common => true,
        QueryCase::And => id % 6 == 0,
        QueryCase::Or | QueryCase::Ranked => id % 2 == 0 || id % 3 == 0,
        QueryCase::Phrase => id % 200 == 0,
    });
    let expected_rows = ids.clone().count();
    if matches!(case, QueryCase::Ranked) {
        ensure!(
            rows == expected_rows.min(10) * queries,
            "ranked row count mismatch"
        );
    } else {
        ensure!(
            rows == expected_rows * queries,
            "row count mismatch for {case:?}"
        );
        ensure!(
            sum == ids.map(|id| id as i64).sum::<i64>() * queries as i64,
            "ID sum mismatch for {case:?}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_mixing_search_latency_and_throughput_options() -> Result<()> {
        for options in [
            vec![
                "--benchmark",
                "search",
                "--connections",
                "2",
                "--queries",
                "1",
            ],
            vec!["--benchmark", "search", "--seconds", "1"],
            vec!["--benchmark", "throughput", "--queries", "10000"],
            vec!["--benchmark", "throughput", "--state", "first"],
            vec!["--benchmark", "throughput", "--seconds", "NaN"],
            vec!["--benchmark", "throughput", "--seconds", "0"],
        ] {
            let args = Args::try_parse_from(std::iter::once("fts-benchmark").chain(options))?;
            assert!(run(args).await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn throughput_runs_for_the_requested_duration_without_latency_samples() -> Result<()> {
        for minimum in [0.001, 0.005] {
            let mut batches = 0;
            let measured = measure_throughput(minimum, async || {
                batches += 1;
                Ok(RunResult {
                    queries: 1,
                    transactions: 0,
                    rows: 3,
                    id_sum: 7,
                    max_active_transactions: 0,
                })
            })
            .await?;
            assert!(measured.seconds >= minimum);
            assert!(measured.latencies.is_empty());
            assert_eq!(measured.result.queries, batches);
            assert_eq!(measured.result.rows, batches * 3);
            assert_eq!(measured.result.id_sum, batches as i64 * 7);
        }
        Ok(())
    }

    #[test]
    fn latency_percentiles_use_individual_samples_and_nearest_rank() {
        let mut samples = [100.0, 1.0, 4.0, 2.0, 3.0];
        assert_eq!(percentile(&mut samples, 50), "3.000000000");
        assert_eq!(percentile(&mut samples, 95), "100.000000000");
        assert_eq!(percentile(&mut samples, 99), "100.000000000");
        assert_eq!(percentile(&mut [], 50), "");
        let mut samples: Vec<f64> = (1..=100).rev().map(f64::from).collect();
        assert_eq!(percentile(&mut samples, 50), "50.000000000");
        assert_eq!(percentile(&mut samples, 95), "95.000000000");
        assert_eq!(percentile(&mut samples, 99), "99.000000000");
    }

    #[tokio::test]
    async fn concurrent_search_validates_all_targets_with_uneven_query_counts() -> Result<()> {
        for (state, queries) in [("warm", "17"), ("first", "3")] {
            run(Args::try_parse_from([
                "fts-benchmark",
                "--benchmark",
                "search",
                "--documents",
                "203",
                "--connections",
                "3",
                "--queries",
                queries,
                "--state",
                state,
                "--runs",
                "1",
            ])?)
            .await?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_fts5_matches_turso_wal_and_mvcc() -> Result<()> {
        for (state, connections, execution) in [
            (QueryState::First, 1, Execution::Queries(1)),
            (QueryState::Warm, 1, Execution::Queries(3)),
            (
                QueryState::Warm,
                2,
                Execution::Transactions {
                    per_connection: 3,
                    queries_per_transaction: 2,
                },
            ),
        ] {
            for &query in QueryCase::value_variants() {
                let mut config = FtsConfig {
                    query,
                    state,
                    documents: 203,
                    corpus: CorpusConfig::default(),
                    mode: JournalMode::Wal,
                    connections,
                    execution,
                };
                let mut sqlite = sqlite::SqliteWorkload::prepare(config).await?;
                let expected = sqlite.run().await?;
                validate_result(query, 203, expected.queries, expected.rows, expected.id_sum)?;
                assert_eq!(
                    expected.queries,
                    if connections == 2 {
                        12
                    } else if matches!(state, QueryState::First) {
                        1
                    } else {
                        3
                    }
                );
                assert_eq!(expected.transactions, if connections == 2 { 6 } else { 0 });
                for mode in [JournalMode::Wal, JournalMode::Mvcc] {
                    config.mode = mode;
                    let turso = FtsWorkload::prepare(config, &mut ()).await?;
                    let actual = turso.run(&mut ()).await?;
                    assert_eq!(actual.queries, expected.queries);
                    assert_eq!(actual.transactions, expected.transactions);
                    assert_eq!(
                        actual.max_active_transactions,
                        expected.max_active_transactions
                    );
                    assert_eq!(actual.rows, expected.rows);
                    if !matches!(query, QueryCase::Ranked) {
                        assert_eq!(actual.id_sum, expected.id_sum);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn checks_phrase_order_and_non_multiple_document_counts() {
        assert!(validate_result(QueryCase::Rare, 103, 3, 6, 300).is_ok());
        assert!(validate_result(QueryCase::Phrase, 103, 3, 3, 0).is_ok());
        assert!(validate_result(QueryCase::Phrase, 103, 3, 6, 300).is_err());
        assert!(validate_result(QueryCase::And, 7, 2, 4, 12).is_ok());
        assert!(validate_result(QueryCase::And, 7, 2, 4, 10).is_err());
        assert!(validate_result(QueryCase::Ranked, 5, 2, 8, 18).is_ok());
        assert!(validate_result(QueryCase::Ranked, 5, 2, 20, 18).is_err());
    }
}
