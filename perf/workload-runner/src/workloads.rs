use crate::{
    db::{Error, Result, Transaction, Value},
    retry::{Retry, TransactionRunner},
    Group, Limit, Load, Measurement, Plan, Signal, Stage, Window,
};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Clone, Debug, Serialize, Deserialize, clap::ValueEnum)]
pub enum Scenario {
    Insert,
    HeldSnapshot,
    Noop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub scenario: Scenario,
    pub connections: usize,
    pub batch_size: u64,
    pub warmup: Duration,
    pub duration: Duration,
    pub count: Option<u64>,
    pub load: Load,
    pub seed: u64,
    pub retry: Retry,
    pub checkpoint_interval: Duration,
    pub raw_samples: bool,
}

pub struct Workload {
    pub plan: Plan,
    pub groups: Vec<Group>,
    pub verified: Arc<AtomicBool>,
}

pub fn build(config: Config, mode: Transaction) -> Result<Workload> {
    if config.connections == 0 || config.batch_size == 0 || config.checkpoint_interval.is_zero() {
        return Err(Error::Workload(
            "connections, batch size and checkpoint interval must be positive".into(),
        ));
    }
    let noop = matches!(config.scenario, Scenario::Noop);
    let held = matches!(config.scenario, Scenario::HeldSnapshot);
    let schema = Signal::new();
    let snapshot = Signal::new();
    if !held {
        snapshot.set();
    }
    let finished = Signal::new();
    let remaining = Arc::new(AtomicU64::new(config.connections as u64));
    let next_id = Arc::new(AtomicU64::new(1));
    let committed_rows = Arc::new(AtomicU64::new(0));
    let committed_sum = Arc::new(AtomicU64::new(0));
    let verified = Arc::new(AtomicBool::new(false));
    let mut groups = Vec::new();
    groups.push(Group {
        name: "writers".into(), workers: config.connections,
        task: Arc::new({
            let snapshot = snapshot.clone();
            let finished = finished.clone();
            let rows = committed_rows.clone();
            let sum = committed_sum.clone();
            let config = config.clone();
            move |conn, mut worker| {
                let schema = schema.clone();
                let snapshot = snapshot.clone();
                let finished = finished.clone();
                let remaining = remaining.clone();
                let next_id = next_id.clone();
                let rows = rows.clone();
                let sum = sum.clone();
                let config = config.clone();
                Box::pin(async move {
                    let mut insert = None;
                    let mut transaction = None;
                    while let Some(mut stage) = worker.next_stage().await {
                        if stage.name() == "setup" {
                            if worker.id == 0 {
                                conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)", &[]).await?;
                                schema.set();
                            } else { schema.wait(stage.cancellation()).await?; }
                            insert = Some(conn.prepare("INSERT INTO items VALUES (?, ?)").await?);
                            transaction = Some(TransactionRunner::prepare(conn, mode).await?);
                            continue;
                        }
                        snapshot.wait(stage.cancellation()).await?;
                        let insert = insert.as_mut().unwrap();
                        let transaction = transaction.as_mut().unwrap();
                        while let Some(admission) = stage.next().await {
                            if noop {
                                stage.measure("noop", admission, async { Ok(()) }).await?;
                                continue;
                            }
                            let base = next_id.fetch_add(config.batch_size, Ordering::Relaxed);
                            let values: Vec<_> = (base..base + config.batch_size).map(|id| {
                                vec![Value::Integer(id as i64), Value::Text(format!("{}:{id}", config.seed))]
                            }).collect();
                            let attempts = Cell::new(0);
                            let cancel = stage.abort().clone();
                            stage.measure_attempts("insert", admission, &attempts,
                                transaction.run(&values, &config.retry, &cancel, &attempts, async |values| {
                                    for value in values { insert.execute(value).await?; }
                                    Ok(())
                                })
                            ).await?;
                            rows.fetch_add(config.batch_size, Ordering::Relaxed);
                            sum.fetch_add((base..base + config.batch_size).sum::<u64>(), Ordering::Relaxed);
                        }
                        if remaining.fetch_sub(1, Ordering::AcqRel) == 1 { finished.set(); }
                    }
                    Ok(())
                })
            }
        }),
    });
    if held {
        groups.push(Group {
            name: "reader".into(),
            workers: 1,
            task: Arc::new({
                let snapshot = snapshot.clone();
                let finished = finished.clone();
                move |conn, mut worker| {
                    let snapshot = snapshot.clone();
                    let finished = finished.clone();
                    Box::pin(async move {
                        let mut count = None;
                        while let Some(mut stage) = worker.next_stage().await {
                            if stage.name() == "prepare-reader" {
                                count = Some(conn.prepare("SELECT count(*) FROM items").await?);
                                continue;
                            }
                            let count = count.as_mut().unwrap();
                            let cancel = stage.cancellation().clone();
                            stage
                                .measure_custom("held_snapshot", async {
                                    conn.execute("BEGIN", &[]).await?;
                                    let before = scalar_statement(count).await?;
                                    snapshot.set();
                                    let wait = finished.wait(&cancel).await;
                                    if !matches!(wait, Ok(()) | Err(Error::Cancelled)) {
                                        wait?;
                                    }
                                    let after = scalar_statement(count).await?;
                                    conn.execute("COMMIT", &[]).await?;
                                    if before != after {
                                        return Err(Error::Workload(format!(
                                            "snapshot changed from {before} to {after}"
                                        )));
                                    }
                                    Ok(())
                                })
                                .await?;
                        }
                        Ok(())
                    })
                }
            }),
        });
        groups.push(Group {
            name: "checkpointer".into(), workers: 1,
            task: Arc::new({
                let interval = config.checkpoint_interval;
                move |conn, mut worker| {
                    let snapshot = snapshot.clone();
                    let finished = finished.clone();
                    Box::pin(async move {
                        let mut checkpoint = conn.prepare("PRAGMA wal_checkpoint(PASSIVE)").await?;
                        while let Some(mut stage) = worker.next_stage().await {
                            let cancel = stage.cancellation().clone();
                            snapshot.wait(&cancel).await?;
                            loop {
                                tokio::select! {
                                    _ = finished.wait(&cancel) => break,
                                    result = cancel.sleep(interval) => { if result.is_err() { break; } },
                                }
                                stage.measure_custom("checkpoint", async {
                                    let mut rows = checkpoint.query(&[]).await?;
                                    while rows.next().await?.is_some() {}
                                    Ok(())
                                }).await?;
                            }
                        }
                        Ok(())
                    })
                }
            }),
        });
    }
    groups.push(Group {
        name: "verify".into(),
        workers: 1,
        task: Arc::new({
            let verified = verified.clone();
            let seed = config.seed;
            move |conn, mut worker| {
                let expected_rows = committed_rows.clone();
                let expected_sum = committed_sum.clone();
                let verified = verified.clone();
                Box::pin(async move {
                    while let Some(mut stage) = worker.next_stage().await {
                        stage
                            .measure_custom("verify", async {
                                let rows = conn
                                    .query("SELECT id, payload FROM items ORDER BY id", &[])
                                    .await?;
                                let mut sum = 0u64;
                                for row in &rows {
                                    let [Value::Integer(id), Value::Text(payload)] = row.as_slice()
                                    else {
                                        return Err(Error::Workload("invalid row values".into()));
                                    };
                                    if *payload != format!("{seed}:{id}") {
                                        return Err(Error::Workload("incorrect payload".into()));
                                    }
                                    sum += *id as u64;
                                }
                                if rows.len() as u64 != expected_rows.load(Ordering::Relaxed)
                                    || sum != expected_sum.load(Ordering::Relaxed)
                                {
                                    return Err(Error::Workload(
                                        "row count or id sum does not match committed inputs"
                                            .into(),
                                    ));
                                }
                                if !noop && rows.is_empty() {
                                    return Err(Error::Workload("no writes completed".into()));
                                }
                                verified.store(true, Ordering::Release);
                                Ok(())
                            })
                            .await?;
                    }
                    Ok(())
                })
            }
        }),
    });
    let mut stages = vec![stage(
        "setup",
        &["writers"],
        Limit::Once,
        Load::Continuous,
        Measurement::Off,
        false,
    )];
    if held {
        stages.push(stage(
            "prepare-reader",
            &["reader"],
            Limit::Once,
            Load::Continuous,
            Measurement::Off,
            false,
        ));
    }
    let limit = config.count.map_or(
        Limit::Duration(config.warmup + config.duration),
        Limit::Count,
    );
    let measurement = if config.count.is_some() {
        Measurement::All
    } else {
        Measurement::Windows(vec![Window {
            start: config.warmup,
            end: config.warmup + config.duration,
        }])
    };
    let active = if held {
        vec!["writers", "reader", "checkpointer"]
    } else {
        vec!["writers"]
    };
    stages.push(stage(
        "load",
        &active,
        limit,
        config.load,
        measurement,
        config.raw_samples,
    ));
    stages.push(stage(
        "verify",
        &["verify"],
        Limit::Once,
        Load::Continuous,
        Measurement::Off,
        false,
    ));
    Ok(Workload {
        plan: Plan {
            stages,
            seed: config.seed,
            startup_timeout: Duration::from_secs(10),
        },
        groups,
        verified,
    })
}

fn stage(
    name: &str,
    groups: &[&str],
    limit: Limit,
    load: Load,
    measurement: Measurement,
    raw_samples: bool,
) -> Stage {
    let timeout = match limit {
        Limit::Duration(d) => d + Duration::from_secs(10),
        _ => Duration::from_secs(30),
    };
    Stage {
        name: name.into(),
        groups: groups.iter().map(|g| (*g).into()).collect(),
        limit,
        load,
        measurement,
        timeout,
        drain_timeout: Duration::from_secs(5),
        raw_samples,
    }
}

async fn scalar_statement(statement: &mut crate::db::Statement<'_>) -> Result<i64> {
    let mut rows = statement.query(&[]).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| Error::Workload("missing count row".into()))?;
    let [Value::Integer(value)] = row.as_slice() else {
        return Err(Error::Workload("count is not an integer".into()));
    };
    if rows.next().await?.is_some() {
        return Err(Error::Workload("extra count row".into()));
    }
    Ok(*value)
}
