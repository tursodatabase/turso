use super::*;
use crate::{
    cli::{settings, Target},
    db::Value,
    retry::{Retry, TransactionRunner},
};
use std::cell::Cell;

#[test]
fn write_conflicts_are_retryable_but_other_generic_errors_are_not() {
    assert!(Error::Turso(turso::Error::Error("Write-write conflict".into())).is_conflict());
    assert!(!Error::Turso(turso::Error::Error("database is closed".into())).is_conflict());
}

#[test]
fn startup_failure_does_not_block_other_workers() {
    let started = Instant::now();
    let report = fake_run(
        spec(Limit::Count(3)),
        2,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                if worker.id == 0 {
                    return Err(Error::Workload("prepare failed".into()));
                }
                assert!(worker.next_stage().await.is_none());
                Ok(())
            })
        }),
    );
    assert!(!report.complete);
    assert!(report.errors.iter().any(|e| e.contains("prepare failed")));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn unmeasured_stage_error_is_not_reported_as_complete() {
    let mut stage = spec(Limit::Once);
    stage.measurement = Measurement::Off;
    let report = fake_run(
        stage,
        2,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(stage) = worker.next_stage().await {
                    if worker.id == 0 {
                        return Err(Error::Workload("schema creation failed".into()));
                    }
                    assert!(matches!(
                        Signal::new().wait(stage.cancellation()).await,
                        Err(Error::Cancelled)
                    ));
                }
                Ok(())
            })
        }),
    );
    assert!(!report.complete);
    assert!(!report.stages[0].complete);
    assert_eq!(
        report.stages[0].workers[0].error.as_deref(),
        Some("schema creation failed")
    );
}

#[test]
fn adapters_preserve_bound_values_and_transaction_state() {
    for target in [Target::SqliteWal, Target::TursoWal, Target::TursoMvcc] {
        let dir = tempfile::tempdir().unwrap();
        runtime().block_on(async {
            let db = Database::create(&dir.path().join("db"), settings(target))
                .await
                .unwrap();
            let conn = db.connect().await.unwrap();
            let values = vec![
                Value::Null,
                Value::Integer(-73),
                Value::Real(1.25),
                Value::Text("a'β".into()),
                Value::Blob(vec![0, 255, 13]),
            ];
            let mut statement = conn.prepare("SELECT ?, ?, ?, ?, ?").await.unwrap();
            for _ in 0..2 {
                let mut rows = statement.query(&values).await.unwrap();
                assert_eq!(rows.next().await.unwrap(), Some(values.clone()));
                assert_eq!(rows.next().await.unwrap(), None);
            }
            assert!(conn.is_autocommit().unwrap());
            conn.execute("BEGIN", &[]).await.unwrap();
            assert!(!conn.is_autocommit().unwrap());
            conn.execute("ROLLBACK", &[]).await.unwrap();
            assert!(conn.is_autocommit().unwrap());
        });
    }
    let mut invalid = settings(Target::SqliteWal);
    invalid.journal = db::Journal::Mvcc;
    assert!(invalid.validate().is_err());
    invalid.journal = db::Journal::Wal;
    invalid.transaction = db::Transaction::Concurrent;
    assert!(invalid.validate().is_err());
}

#[test]
fn persistent_worker_prepares_before_next_stage_clock() {
    let dir = tempfile::tempdir().unwrap();
    let db = runtime()
        .block_on(Database::create(
            &dir.path().join("db"),
            settings(Target::SqliteWal),
        ))
        .unwrap();
    let mut first = spec(Limit::Once);
    first.name = "first".into();
    first.measurement = Measurement::Off;
    let second = spec(Limit::Count(7));
    let group = Group {
        name: "fake".into(),
        workers: 1,
        task: Arc::new(|conn, mut worker| {
            Box::pin(async move {
                let mut stmt = conn.prepare("SELECT ?").await?;
                let stage = worker.next_stage().await.unwrap();
                assert_eq!(stage.name(), "first");
                drop(stage);
                std::thread::sleep(Duration::from_millis(80));
                while let Some(mut stage) = worker.next_stage().await {
                    assert!(stage.elapsed() < Duration::from_millis(40));
                    while let Some(admission) = stage.next().await {
                        stage
                            .measure("read", admission, async {
                                let mut rows = stmt.query(&[Value::Integer(19)]).await?;
                                assert_eq!(rows.next().await?, Some(vec![Value::Integer(19)]));
                                assert_eq!(rows.next().await?, None);
                                Ok(())
                            })
                            .await?;
                    }
                }
                Ok(())
            })
        }),
    };
    let report = run(
        db,
        Plan {
            stages: vec![first, second],
            seed: 9,
            startup_timeout: Duration::from_secs(2),
        },
        vec![group],
    )
    .unwrap();
    assert!(report.complete, "{:?}", report.errors);
    assert_eq!(report.stages[1].operations["read"].successes, 7);
    assert!(report.stages[1].elapsed_ns < 40_000_000);
}

#[test]
fn delayed_operation_drains_without_admitting_more_work() {
    let mut stage = spec(Limit::Duration(Duration::from_millis(40)));
    stage.load = Load::Fixed { per_second: 100.0 };
    let report = fake_run(
        stage,
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(mut stage) = worker.next_stage().await {
                    while let Some(admission) = stage.next().await {
                        stage
                            .measure("slow", admission, async {
                                std::thread::sleep(Duration::from_millis(90));
                                Ok(())
                            })
                            .await?;
                    }
                }
                Ok(())
            })
        }),
    );
    assert!(report.complete, "{:?}", report.errors);
    let stage = &report.stages[0];
    assert_eq!(stage.operations["slow"].successes, 1);
    assert_eq!(stage.operations["slow"].drain_completions, 1);
    assert_eq!(stage.operations["slow"].window_completions, 0);
    assert_eq!(stage.skipped, 3);
    assert_eq!(stage.backlog_at_deadline, 4);
    assert!(stage.drain_ns >= 50_000_000);
}

#[test]
fn overdue_arrivals_keep_original_schedule() {
    let mut stage = spec(Limit::Count(3));
    stage.load = Load::Fixed { per_second: 100.0 };
    let report = fake_run(
        stage,
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(mut stage) = worker.next_stage().await {
                    while let Some(admission) = stage.next().await {
                        stage
                            .measure("slow", admission, async {
                                std::thread::sleep(Duration::from_millis(30));
                                Ok(())
                            })
                            .await?;
                    }
                }
                Ok(())
            })
        }),
    );
    assert!(report.complete);
    let metrics = &report.stages[0].workers[0].operations["slow"];
    assert_eq!(
        metrics
            .samples
            .iter()
            .map(|s| s.scheduled_ns)
            .collect::<Vec<_>>(),
        vec![0, 10_000_000, 20_000_000]
    );
    assert!(metrics.samples[2].started_ns >= 60_000_000);
    assert!(metrics.scheduling_delay.p99_ns >= 40_000_000);
}

#[test]
fn windows_use_scheduled_arrival_and_exclude_end_boundary() {
    let measurement = Measurement::Windows(vec![Window {
        start: Duration::from_millis(10),
        end: Duration::from_millis(30),
    }]);
    assert!(!measurement.contains(Duration::from_millis(9)));
    assert!(measurement.contains(Duration::from_millis(10)));
    assert!(measurement.contains(Duration::from_millis(29)));
    assert!(!measurement.contains(Duration::from_millis(30)));
    assert_eq!(
        measurement.end(Duration::from_millis(10), Duration::from_secs(1)),
        Duration::from_millis(30)
    );
    let mut stage = spec(Limit::Count(4));
    stage.load = Load::Fixed { per_second: 100.0 };
    stage.measurement = measurement;
    let report = fake_run(
        stage,
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(mut stage) = worker.next_stage().await {
                    while let Some(admission) = stage.next().await {
                        stage
                            .measure("delayed", admission, async {
                                std::thread::sleep(Duration::from_millis(35));
                                Ok(())
                            })
                            .await?;
                    }
                }
                Ok(())
            })
        }),
    );
    assert!(report.complete);
    let metrics = &report.stages[0].operations["delayed"];
    assert_eq!(metrics.successes, 2);
    assert_eq!(metrics.drain_completions, 2);
    assert_eq!(metrics.throughput_per_second, 100.0);
    assert_eq!(report.stages[0].workers[0].all_successes, 4);
}

#[test]
fn cancellation_wakes_signal_waiters_and_stops_tasks() {
    let report = fake_run(
        spec(Limit::Duration(Duration::from_millis(20))),
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(stage) = worker.next_stage().await {
                    assert!(matches!(
                        Signal::new().wait(stage.cancellation()).await,
                        Err(Error::Cancelled)
                    ));
                }
                Ok(())
            })
        }),
    );
    assert!(report.complete, "{:?}", report.errors);
    runtime().block_on(async {
        let signal = Signal::new();
        signal.set();
        signal.wait(&Signal::new()).await.unwrap();
        let cancel = Signal::new();
        cancel.set();
        assert!(matches!(signal.wait(&cancel).await, Err(Error::Cancelled)));
    });
}

#[test]
fn missing_count_and_dropped_operation_are_incomplete() {
    let report = fake_run(
        spec(Limit::Count(3)),
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(mut stage) = worker.next_stage().await {
                    let admission = stage.next().await.unwrap();
                    let future =
                        stage.measure::<()>("unfinished", admission, std::future::pending());
                    assert!(tokio::time::timeout(Duration::from_millis(1), future)
                        .await
                        .is_err());
                }
                Ok(())
            })
        }),
    );
    assert!(!report.complete);
    assert_eq!(report.stages[0].skipped, 2);
    assert_eq!(report.stages[0].operations["unfinished"].unfinished, 1);
}

#[test]
fn retry_reuses_inputs_rolls_back_and_counts_attempts() {
    runtime().block_on(async {
        let conn = Connection::Sqlite(rusqlite::Connection::open_in_memory().unwrap());
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)", &[])
            .await
            .unwrap();
        let mut insert = conn.prepare("INSERT INTO t VALUES (?, ?)").await.unwrap();
        let mut transaction = TransactionRunner::prepare(&conn, db::Transaction::Immediate)
            .await
            .unwrap();
        let input = vec![Value::Integer(73), Value::Text("same input".into())];
        let retry = Retry {
            max_attempts: 3,
            timeout: Duration::from_secs(1),
            backoff: Duration::from_millis(1),
        };
        let attempts = Cell::new(0);
        let mut seen = Vec::new();
        transaction
            .run(&input, &retry, &Signal::new(), &attempts, async |values| {
                seen.push(values.clone());
                insert.execute(values).await?;
                if seen.len() < 3 {
                    return Err(Error::Turso(turso::Error::BusySnapshot(
                        "injected conflict".into(),
                    )));
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(attempts.get(), 3);
        assert_eq!(seen, vec![input.clone(); 3]);
        assert_eq!(
            conn.query("SELECT * FROM t", &[]).await.unwrap(),
            vec![input]
        );
        assert!(conn.is_autocommit().unwrap());
        let attempts = Cell::new(0);
        let failure = transaction
            .run(&(), &retry, &Signal::new(), &attempts, async |_| {
                Err(Error::Workload("not a conflict".into()))
            })
            .await;
        assert!(failure.is_err());
        assert_eq!(attempts.get(), 1);
        assert!(conn.is_autocommit().unwrap());
    });
}

#[test]
fn fixed_rate_does_not_accumulate_rounding_error_at_deadline() {
    let mut schedule = Schedule {
        next: 0,
        due: Duration::ZERO,
        rng: ChaCha8Rng::seed_from_u64(0),
        admitted: 3,
        completed: 3,
        completed_before_deadline: 3,
    };
    let load = Load::Fixed { per_second: 3.0 };
    for index in 1..=3 {
        schedule.next = index;
        advance(&mut schedule, &load);
    }
    assert_eq!(schedule.due, Duration::from_secs(1));
    let mut stage = spec(Limit::Duration(Duration::from_secs(1)));
    stage.load = load;
    assert_eq!(skipped(&mut schedule, &stage), 0);
}

#[test]
fn poisson_schedule_is_seeded_and_does_not_reschedule() {
    let make = |seed| Schedule {
        next: 0,
        due: Duration::ZERO,
        rng: ChaCha8Rng::seed_from_u64(seed),
        admitted: 0,
        completed: 0,
        completed_before_deadline: 0,
    };
    let (mut a, mut b, mut different) = (make(19), make(19), make(20));
    let load = Load::Poisson { per_second: 170.0 };
    let mut intervals = BTreeSet::new();
    for _ in 0..100 {
        let before = a.due;
        advance(&mut a, &load);
        advance(&mut b, &load);
        advance(&mut different, &load);
        assert_eq!(a.due, b.due);
        assert!(a.due > before);
        intervals.insert(a.due - before);
    }
    assert_ne!(a.due, different.due);
    assert!(intervals.len() > 90);
}

#[test]
fn histogram_aggregation_uses_samples_not_worker_percentiles() {
    let mut a = Recorder::new();
    let mut b = Recorder::new();
    for _ in 0..99 {
        a.record(
            Sample {
                scheduled_ns: 0,
                started_ns: 3,
                completed_ns: 13,
                attempts: 1,
                success: true,
                drain: false,
            },
            false,
        );
    }
    b.record(
        Sample {
            scheduled_ns: 0,
            started_ns: 100,
            completed_ns: 1000,
            attempts: 3,
            success: false,
            drain: true,
        },
        false,
    );
    a.merge(&b);
    let metrics = a.finish(2.0);
    assert_eq!(metrics.response.p99_ns, 13);
    assert_eq!(metrics.response.max_ns, 1000);
    assert_eq!(metrics.successes, 99);
    assert_eq!(metrics.failures, 1);
    assert_eq!(metrics.attempts, 102);
    assert_eq!(metrics.retries, 2);
    assert_eq!(metrics.throughput_per_second, 49.5);
}

#[test]
fn submeasurements_inherit_parent_boundary_and_preserve_results() {
    let mut stage = spec(Limit::Count(2));
    stage.load = Load::Fixed { per_second: 100.0 };
    stage.measurement = Measurement::Windows(vec![Window {
        start: Duration::ZERO,
        end: Duration::from_millis(5),
    }]);
    let report = fake_run(
        stage,
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(mut stage) = worker.next_stage().await {
                    while let Some(admission) = stage.next().await {
                        let result = stage
                            .measure_with("logical", admission, async |measurements| {
                                std::thread::sleep(Duration::from_millis(10));
                                measurements.measure("part", async { Ok(73) }).await
                            })
                            .await?;
                        assert_eq!(result, 73);
                    }
                }
                Ok(())
            })
        }),
    );
    assert!(report.complete);
    assert_eq!(report.stages[0].operations["logical"].successes, 1);
    assert_eq!(report.stages[0].operations["logical/part"].successes, 1);
    assert_eq!(
        report.stages[0].operations["logical/part"].drain_completions,
        1
    );
    assert_eq!(
        report.stages[0].operations["logical/part"]
            .scheduling_delay
            .max_ns,
        0
    );
}

#[test]
fn drain_timeout_reports_missing_work_and_signals_abort() {
    let mut stage = spec(Limit::Duration(Duration::from_millis(10)));
    stage.drain_timeout = Duration::from_millis(10);
    let report = fake_run(
        stage,
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                while let Some(mut stage) = worker.next_stage().await {
                    let admission = stage.next().await.unwrap();
                    let abort = stage.abort().clone();
                    stage
                        .measure("blocked", admission, async {
                            Signal::new().wait(&abort).await
                        })
                        .await?;
                }
                Ok(())
            })
        }),
    );
    assert!(!report.complete);
    assert_eq!(report.stages[0].unfinished_workers, 1);
    assert_eq!(report.stages[0].unfinished_operations, 1);
    assert!(report
        .errors
        .iter()
        .any(|e| e.contains("worker wait failed")));
    assert!(!report
        .errors
        .iter()
        .any(|e| e.contains("worker did not stop")));
}

#[test]
fn retry_exhaustion_and_timeout_leave_no_transaction() {
    runtime().block_on(async {
        let conn = Connection::Sqlite(rusqlite::Connection::open_in_memory().unwrap());
        let mut transaction = TransactionRunner::prepare(&conn, db::Transaction::Immediate)
            .await
            .unwrap();
        for (max_attempts, timeout, expected) in [
            (2, Duration::from_secs(1), 2),
            (100, Duration::from_millis(1), 1),
        ] {
            let attempts = Cell::new(0);
            let retry = Retry {
                max_attempts,
                timeout,
                backoff: Duration::ZERO,
            };
            let result = transaction
                .run(&41, &retry, &Signal::new(), &attempts, async |input| {
                    assert_eq!(*input, 41);
                    std::thread::sleep(Duration::from_millis(2));
                    Err(Error::Turso(turso::Error::Busy("injected".into())))
                })
                .await;
            assert!(result.unwrap_err().is_conflict());
            assert_eq!(attempts.get(), expected);
            assert!(conn.is_autocommit().unwrap());
        }
    });
}

#[test]
fn repeated_callback_keeps_mutable_state() {
    let report = fake_run(
        spec(Limit::Count(9)),
        1,
        Arc::new(|_, mut worker| {
            Box::pin(async move {
                let mut count = 0;
                while let Some(mut stage) = worker.next_stage().await {
                    stage
                        .repeat("repeat", &mut count, |count| {
                            Box::pin(async move {
                                *count += 1;
                                Ok(())
                            })
                        })
                        .await?;
                }
                assert_eq!(count, 9);
                Ok(())
            })
        }),
    );
    assert!(report.complete);
    assert_eq!(report.stages[0].operations["repeat"].successes, 9);
}

fn fake_run(stage: Stage, workers: usize, task: Task) -> Report {
    let dir = tempfile::tempdir().unwrap();
    let db = runtime()
        .block_on(Database::create(
            &dir.path().join("db"),
            settings(Target::SqliteWal),
        ))
        .unwrap();
    run(
        db,
        Plan {
            stages: vec![stage],
            seed: 19,
            startup_timeout: Duration::from_secs(1),
        },
        vec![Group {
            name: "fake".into(),
            workers,
            task,
        }],
    )
    .unwrap()
}

fn spec(limit: Limit) -> Stage {
    Stage {
        name: "test".into(),
        groups: vec!["fake".into()],
        limit,
        load: Load::Continuous,
        measurement: Measurement::All,
        timeout: Duration::from_secs(1),
        drain_timeout: Duration::from_millis(200),
        raw_samples: true,
    }
}
