use crate::workload::{self, Generator, Param, Population, Table, Txn};
use crate::{
    Checkpoint, Clock, Config, Outcome, PopulateConfig, Run, Sample, TableCounts, TxnMode,
    COMPLETED,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};
use turso::{Builder, Connection, Database, Statement, Value};

pub fn populate(config: &PopulateConfig) -> TableCounts {
    block_on(async {
        let db = open(&config.db_path, &config.io, config.mode).await;
        let conn = connect(&db, config.timeout).await;
        let journal_mode = match config.mode {
            TxnMode::Immediate => "wal",
            TxnMode::Concurrent => "mvcc",
        };
        conn.pragma_update("journal_mode", journal_mode)
            .await
            .unwrap();
        // Nothing needs to survive a crash while populating: bench.sh syncs
        // the finished database before any run.
        conn.pragma_update("synchronous", "OFF").await.unwrap();
        conn.execute_batch(workload::SCHEMA).await.unwrap();
        let mut inserts = [
            conn.prepare(workload::INSERT_SUBSCRIBER).await.unwrap(),
            conn.prepare(workload::INSERT_ACCESS_INFO).await.unwrap(),
            conn.prepare(workload::INSERT_SPECIAL_FACILITY)
                .await
                .unwrap(),
            conn.prepare(workload::INSERT_CALL_FORWARDING)
                .await
                .unwrap(),
        ];
        let mut population = Population::new(config.subscribers, config.seed);
        while let Some(rows) = population.next_block() {
            conn.execute("BEGIN", ()).await.unwrap();
            for (table, values) in rows {
                let insert = match table {
                    Table::Subscriber => &mut inserts[0],
                    Table::AccessInfo => &mut inserts[1],
                    Table::SpecialFacility => &mut inserts[2],
                    Table::CallForwarding => &mut inserts[3],
                };
                let values: Vec<Value> = values.into_iter().map(to_value).collect();
                insert.execute(values).await.unwrap();
            }
            conn.execute("COMMIT", ()).await.unwrap();
        }
        drop(inserts);
        checkpoint(&conn, "TRUNCATE").await.unwrap();
        count_tables(&conn).await
    })
}

pub fn run(config: &Config) -> Run {
    // One OS thread per connection, each with its own single-threaded tokio
    // runtime. A shared multi-thread runtime does not work here: with the
    // syscall IO backend nothing in a transaction ever yields, so a task
    // that keeps a worker busy can starve the others.
    let (db, subscribers) = block_on(setup(config));
    assert_eq!(
        subscribers, config.subscribers,
        "the database holds another number of subscribers than --subscribers"
    );

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let clock = Arc::new(Clock::new(config));
    let stop = Arc::new(AtomicBool::new(false));

    let checkpointer = config.checkpointer.map(|interval| {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let timeout = config.timeout;
        thread::spawn(move || block_on(checkpointer(db, timeout, interval, stop)))
    });

    let mut handles = Vec::new();
    for connection in 0..config.connections {
        let db = db.clone();
        let ready = Arc::clone(&ready);
        let clock = Arc::clone(&clock);
        let timeout = config.timeout;
        let cache_size_mb = config.cache_size_mb;
        let mode = config.mode;
        let generator = Generator::new(
            config.seed + connection as u64,
            config.mix,
            subscribers,
            config.uniform,
        );
        handles.push(thread::spawn(move || {
            block_on(client(
                db,
                timeout,
                cache_size_mb,
                mode,
                generator,
                ready,
                clock,
            ))
        }));
    }

    ready.wait();

    let per_connection: Vec<Vec<Sample>> = handles
        .into_iter()
        .map(|h| h.join().expect("connection thread panicked"))
        .collect();
    let elapsed = clock.elapsed();

    stop.store(true, Ordering::Relaxed);
    let start = clock.started_at();
    let checkpoints = checkpointer
        .map(|h| h.join().expect("checkpointer thread panicked"))
        .unwrap_or_default()
        .into_iter()
        .map(|(started, took)| Checkpoint {
            at: started.saturating_duration_since(start),
            took,
        })
        .collect();

    eprintln!(
        "[turso] io backend {}, group commit {}",
        config.io, config.group_commit
    );

    Run {
        subscribers,
        per_connection,
        checkpoints,
        elapsed,
    }
}

async fn setup(config: &Config) -> (Database, u64) {
    let db = open(&config.db_path, &config.io, config.mode).await;
    let conn = connect(&db, config.timeout).await;
    let mut rows = conn.query("PRAGMA journal_mode", ()).await.unwrap();
    let row = rows
        .next()
        .await
        .unwrap()
        .expect("journal_mode returns a row");
    let journal_mode = row.get::<String>(0).unwrap();
    drop(rows);
    let expected = match config.mode {
        TxnMode::Immediate => "wal",
        TxnMode::Concurrent => "mvcc",
    };
    assert_eq!(
        journal_mode, expected,
        "the database was populated for another --mode"
    );
    if config.mode == TxnMode::Concurrent {
        conn.pragma_update("mvcc_group_commit", config.group_commit)
            .await
            .unwrap();
        if config.checkpointer.is_some() {
            // -1 turns the writers' auto-checkpoint off; the checkpointer
            // connection does it instead.
            conn.pragma_update("mvcc_checkpoint_threshold", -1)
                .await
                .unwrap();
        }
    } else if config.checkpointer.is_some() {
        conn.pragma_update("wal_autocheckpoint", 0).await.unwrap();
    }
    let subscribers = count_tables(&conn).await.subscriber;
    (db, subscribers)
}

#[allow(clippy::too_many_arguments)]
async fn client(
    db: Database,
    timeout: Duration,
    cache_size_mb: u64,
    mode: TxnMode,
    mut generator: Generator,
    ready: Arc<Barrier>,
    clock: Arc<Clock>,
) -> Vec<Sample> {
    let conn = connect(&db, timeout).await;
    conn.pragma_update("cache_size", -((cache_size_mb * 1024) as i64))
        .await
        .unwrap();
    let mut statements = Statements::prepare(&conn, mode).await;

    let mut samples = Vec::new();
    ready.wait();
    while let Some(start) = clock.next() {
        let txn = generator.next_txn();
        let kind = txn.kind();
        // A concurrent transaction can conflict with another one, and the
        // only cure is to start over with the same keys. The restarts stay
        // inside the sample, because the caller is still waiting through
        // them.
        let mut restarts = 0u32;
        let outcome = loop {
            let begin = if kind.writes() {
                &mut statements.begin_write
            } else {
                &mut statements.begin_read
            };
            begin.execute(()).await.unwrap();
            let result = match statements.execute(&txn).await {
                Ok(found) => statements.commit.execute(()).await.map(|_| found),
                Err(e) => Err(e),
            };
            match result {
                Ok(true) => break Outcome::Found,
                Ok(false) => break Outcome::NotFound,
                Err(e) if is_conflict(&e) => {
                    rollback(&conn).await;
                    restarts += 1;
                }
                Err(turso::Error::Constraint(_))
                    if matches!(txn, Txn::InsertCallForwarding { .. }) =>
                {
                    rollback(&conn).await;
                    break Outcome::Rejected;
                }
                Err(e) => panic!("{} failed: {e}", kind.name()),
            }
        };
        let ended = Instant::now();
        if outcome != Outcome::Rejected {
            COMPLETED.fetch_add(1, Ordering::Relaxed);
        }
        samples.push(Sample {
            started_ns: start.offset.as_nanos() as u64,
            warmup: start.warmup,
            kind,
            outcome,
            restarts,
            total_ns: (ended - start.at).as_nanos() as u64,
        });
    }
    samples
}

/// An error that says another transaction got in the way, after which the
/// transaction has to start over.
fn is_conflict(e: &turso::Error) -> bool {
    match e {
        turso::Error::BusySnapshot(_) => true,
        turso::Error::Error(message) => message == "Write-write conflict",
        _ => false,
    }
}

/// Ends the transaction a failed statement left open. Some errors end it
/// already.
async fn rollback(conn: &Connection) {
    if !conn.is_autocommit().unwrap() {
        conn.execute("ROLLBACK", ()).await.unwrap();
    }
}

/// Every statement a client runs, prepared before the clock starts so
/// parsing is not charged to a transaction.
struct Statements {
    begin_read: Statement,
    begin_write: Statement,
    commit: Statement,
    get_subscriber_data: Statement,
    get_new_destination: Statement,
    get_access_data: Statement,
    update_subscriber_bit: Statement,
    update_special_facility: Statement,
    update_location: Statement,
    subscriber_by_number: Statement,
    special_facility_types: Statement,
    insert_call_forwarding: Statement,
    delete_call_forwarding: Statement,
}

impl Statements {
    async fn prepare(conn: &Connection, mode: TxnMode) -> Self {
        let (begin_read, begin_write) = match mode {
            TxnMode::Immediate => ("BEGIN", "BEGIN IMMEDIATE"),
            TxnMode::Concurrent => ("BEGIN CONCURRENT", "BEGIN CONCURRENT"),
        };
        let prepare = |sql: &'static str| async move { conn.prepare(sql).await.unwrap() };
        Self {
            begin_read: prepare(begin_read).await,
            begin_write: prepare(begin_write).await,
            commit: prepare("COMMIT").await,
            get_subscriber_data: prepare(workload::GET_SUBSCRIBER_DATA).await,
            get_new_destination: prepare(workload::GET_NEW_DESTINATION).await,
            get_access_data: prepare(workload::GET_ACCESS_DATA).await,
            update_subscriber_bit: prepare(workload::UPDATE_SUBSCRIBER_BIT).await,
            update_special_facility: prepare(workload::UPDATE_SPECIAL_FACILITY).await,
            update_location: prepare(workload::UPDATE_LOCATION).await,
            subscriber_by_number: prepare(workload::SUBSCRIBER_BY_NUMBER).await,
            special_facility_types: prepare(workload::SPECIAL_FACILITY_TYPES).await,
            insert_call_forwarding: prepare(workload::INSERT_CALL_FORWARDING).await,
            delete_call_forwarding: prepare(workload::DELETE_CALL_FORWARDING).await,
        }
    }

    /// Runs the statements of one transaction inside an open transaction
    /// and says whether it found the rows it was after.
    async fn execute(&mut self, txn: &Txn) -> turso::Result<bool> {
        match txn {
            Txn::GetSubscriberData { s_id } => {
                read_all(&mut self.get_subscriber_data, (*s_id,)).await
            }
            Txn::GetNewDestination {
                s_id,
                sf_type,
                start_time,
                end_time,
            } => {
                read_all(
                    &mut self.get_new_destination,
                    (*s_id, *sf_type, *start_time, *end_time),
                )
                .await
            }
            Txn::GetAccessData { s_id, ai_type } => {
                read_all(&mut self.get_access_data, (*s_id, *ai_type)).await
            }
            Txn::UpdateSubscriberData {
                s_id,
                bit_1,
                sf_type,
                data_a,
            } => {
                self.update_subscriber_bit.execute((*bit_1, *s_id)).await?;
                let changed = self
                    .update_special_facility
                    .execute((*data_a, *s_id, *sf_type))
                    .await?;
                Ok(changed > 0)
            }
            Txn::UpdateLocation {
                sub_nbr,
                vlr_location,
            } => {
                let changed = self
                    .update_location
                    .execute((*vlr_location, sub_nbr.as_str()))
                    .await?;
                Ok(changed > 0)
            }
            Txn::InsertCallForwarding {
                sub_nbr,
                sf_type,
                start_time,
                end_time,
                numberx,
            } => {
                let Some(s_id) = self.subscriber_by_number(sub_nbr).await? else {
                    return Ok(false);
                };
                read_all(&mut self.special_facility_types, (s_id,)).await?;
                self.insert_call_forwarding
                    .execute((s_id, *sf_type, *start_time, *end_time, numberx.as_str()))
                    .await?;
                Ok(true)
            }
            Txn::DeleteCallForwarding {
                sub_nbr,
                sf_type,
                start_time,
            } => {
                let Some(s_id) = self.subscriber_by_number(sub_nbr).await? else {
                    return Ok(false);
                };
                let changed = self
                    .delete_call_forwarding
                    .execute((s_id, *sf_type, *start_time))
                    .await?;
                Ok(changed > 0)
            }
        }
    }

    async fn subscriber_by_number(&mut self, sub_nbr: &str) -> turso::Result<Option<i64>> {
        let mut rows = self.subscriber_by_number.query((sub_nbr,)).await?;
        let mut s_id = None;
        while let Some(row) = rows.next().await? {
            s_id = Some(row.get::<i64>(0)?);
        }
        Ok(s_id)
    }
}

/// Reads every row a query returns and says whether there was any.
async fn read_all(stmt: &mut Statement, params: impl turso::IntoParams) -> turso::Result<bool> {
    let mut rows = stmt.query(params).await?;
    let mut found = false;
    while rows.next().await?.is_some() {
        found = true;
    }
    Ok(found)
}

/// Runs a passive checkpoint every `interval` until told to stop, and returns
/// when each one started and how long it took.
async fn checkpointer(
    db: Database,
    timeout: Duration,
    interval: Duration,
    stop: Arc<AtomicBool>,
) -> Vec<(Instant, Duration)> {
    let conn = connect(&db, timeout).await;
    let mut checkpoints = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(interval).await;
        let started = Instant::now();
        // An error (typically Busy) means this round could not checkpoint;
        // the next round tries again.
        let _ = checkpoint(&conn, "PASSIVE").await;
        checkpoints.push((started, started.elapsed()));
    }
    checkpoints
}

async fn checkpoint(conn: &Connection, mode: &str) -> turso::Result<()> {
    let mut rows = conn
        .query(format!("PRAGMA wal_checkpoint({mode})"), ())
        .await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

async fn count_tables(conn: &Connection) -> TableCounts {
    let mut counts = [0u64; 4];
    for (count, table) in counts.iter_mut().zip([
        "subscriber",
        "access_info",
        "special_facility",
        "call_forwarding",
    ]) {
        let mut rows = conn
            .query(format!("SELECT count(*) FROM {table}"), ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("count(*) returns a row");
        *count = row.get::<i64>(0).unwrap() as u64;
    }
    TableCounts {
        subscriber: counts[0],
        access_info: counts[1],
        special_facility: counts[2],
        call_forwarding: counts[3],
    }
}

fn to_value(param: Param) -> Value {
    match param {
        Param::Int(i) => Value::Integer(i),
        Param::Text(s) => Value::Text(s),
    }
}

async fn open(db_path: &str, io: &str, mode: TxnMode) -> Database {
    Builder::new_local(db_path)
        .with_io(io)
        .experimental_mvcc_passive_checkpoint(mode == TxnMode::Concurrent)
        .build()
        .await
        .unwrap()
}

async fn connect(db: &Database, timeout: Duration) -> Connection {
    let conn = db.connect().unwrap();
    conn.busy_timeout(timeout).unwrap();
    conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();
    conn.execute("PRAGMA foreign_keys = ON", ()).await.unwrap();
    conn
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
