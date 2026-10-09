use crate::workload::{self, Generator, Param, Population, Table, Txn};
use crate::{
    Checkpoint, Clock, Config, Outcome, PopulateConfig, Run, Sample, TableCounts, COMPLETED,
};
use rusqlite::{params_from_iter, Connection, ErrorCode, OptionalExtension};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};

pub fn populate(config: &PopulateConfig) -> TableCounts {
    let conn = open(&config.db_path, config.timeout);
    // Nothing needs to survive a crash while populating: bench.sh syncs the
    // finished database before any run.
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF;")
        .unwrap();
    conn.execute_batch(workload::SCHEMA).unwrap();
    let mut population = Population::new(config.subscribers, config.seed);
    while let Some(rows) = population.next_block() {
        let tx = conn.unchecked_transaction().unwrap();
        for (table, values) in rows {
            let sql = match table {
                Table::Subscriber => workload::INSERT_SUBSCRIBER,
                Table::AccessInfo => workload::INSERT_ACCESS_INFO,
                Table::SpecialFacility => workload::INSERT_SPECIAL_FACILITY,
                Table::CallForwarding => workload::INSERT_CALL_FORWARDING,
            };
            tx.prepare_cached(sql)
                .unwrap()
                .execute(params_from_iter(values.iter().map(to_sql)))
                .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .unwrap();
    drop(conn);
    count_tables(&config.db_path, config.timeout)
}

pub fn run(config: &Config) -> Run {
    let subscribers = count_tables(&config.db_path, config.timeout).subscriber;
    assert_eq!(
        subscribers, config.subscribers,
        "the database holds another number of subscribers than --subscribers"
    );

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let clock = Arc::new(Clock::new(config));
    let stop = Arc::new(AtomicBool::new(false));

    let checkpointer = config.checkpointer.map(|interval| {
        let stop = Arc::clone(&stop);
        let db_path = config.db_path.clone();
        let timeout = config.timeout;
        thread::spawn(move || checkpointer(&db_path, timeout, interval, &stop))
    });

    let mut handles = Vec::new();
    for connection in 0..config.connections {
        let ready = Arc::clone(&ready);
        let clock = Arc::clone(&clock);
        let db_path = config.db_path.clone();
        let timeout = config.timeout;
        let cache_size_mb = config.cache_size_mb;
        let own_checkpoints = config.checkpointer.is_none();
        let generator = Generator::new(
            config.seed + connection as u64,
            config.mix,
            subscribers,
            config.uniform,
        );

        handles.push(thread::spawn(move || {
            let conn = open(&db_path, timeout);
            conn.execute_batch(&format!("PRAGMA cache_size = -{}", cache_size_mb * 1024))
                .unwrap();
            if !own_checkpoints {
                conn.execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
            }
            client(&conn, generator, &ready, &clock)
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

    Run {
        subscribers,
        per_connection,
        checkpoints,
        elapsed,
    }
}

fn client(
    conn: &Connection,
    mut generator: Generator,
    ready: &Barrier,
    clock: &Clock,
) -> Vec<Sample> {
    // Every statement is prepared before the clock starts, so parsing is
    // not charged to a transaction.
    for sql in [
        "BEGIN",
        "BEGIN IMMEDIATE",
        "COMMIT",
        "ROLLBACK",
        workload::GET_SUBSCRIBER_DATA,
        workload::GET_NEW_DESTINATION,
        workload::GET_ACCESS_DATA,
        workload::UPDATE_SUBSCRIBER_BIT,
        workload::UPDATE_SPECIAL_FACILITY,
        workload::UPDATE_LOCATION,
        workload::SUBSCRIBER_BY_NUMBER,
        workload::SPECIAL_FACILITY_TYPES,
        workload::INSERT_CALL_FORWARDING,
        workload::DELETE_CALL_FORWARDING,
    ] {
        conn.prepare_cached(sql).unwrap();
    }

    let mut samples = Vec::new();
    ready.wait();
    while let Some(start) = clock.next() {
        let txn = generator.next_txn();
        let kind = txn.kind();
        // A writer takes the write lock up front. A deferred BEGIN would
        // take it at the first write and, when another writer committed in
        // the meantime, fail with SQLITE_BUSY_SNAPSHOT instead of waiting.
        let begin = if kind.writes() {
            "BEGIN IMMEDIATE"
        } else {
            "BEGIN"
        };
        // SQLite's busy handler is not fair: a writer that sleeps between
        // tries can keep missing the short moments the write lock is free
        // and wait out the whole busy timeout. It then tries again, and the
        // wait stays in its response time.
        let mut restarts = 0u32;
        loop {
            match conn.prepare_cached(begin).unwrap().execute([]) {
                Ok(_) => break,
                Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == ErrorCode::DatabaseBusy => {
                    restarts += 1;
                }
                Err(e) => panic!("{begin} failed: {e}"),
            }
        }
        let outcome = match execute(conn, &txn) {
            Ok(found) => {
                conn.prepare_cached("COMMIT").unwrap().execute([]).unwrap();
                if found {
                    Outcome::Found
                } else {
                    Outcome::NotFound
                }
            }
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == ErrorCode::ConstraintViolation
                    && matches!(txn, Txn::InsertCallForwarding { .. }) =>
            {
                conn.prepare_cached("ROLLBACK")
                    .unwrap()
                    .execute([])
                    .unwrap();
                Outcome::Rejected
            }
            Err(e) => panic!("{} failed: {e}", kind.name()),
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

/// Runs the statements of one transaction inside an open transaction and
/// says whether it found the rows it was after.
fn execute(conn: &Connection, txn: &Txn) -> rusqlite::Result<bool> {
    match txn {
        Txn::GetSubscriberData { s_id } => {
            let mut stmt = conn.prepare_cached(workload::GET_SUBSCRIBER_DATA)?;
            let mut rows = stmt.query([s_id])?;
            let mut found = false;
            while rows.next()?.is_some() {
                found = true;
            }
            Ok(found)
        }
        Txn::GetNewDestination {
            s_id,
            sf_type,
            start_time,
            end_time,
        } => {
            let mut stmt = conn.prepare_cached(workload::GET_NEW_DESTINATION)?;
            let mut rows = stmt.query((s_id, sf_type, start_time, end_time))?;
            let mut found = false;
            while rows.next()?.is_some() {
                found = true;
            }
            Ok(found)
        }
        Txn::GetAccessData { s_id, ai_type } => {
            let mut stmt = conn.prepare_cached(workload::GET_ACCESS_DATA)?;
            let mut rows = stmt.query((s_id, ai_type))?;
            let mut found = false;
            while rows.next()?.is_some() {
                found = true;
            }
            Ok(found)
        }
        Txn::UpdateSubscriberData {
            s_id,
            bit_1,
            sf_type,
            data_a,
        } => {
            conn.prepare_cached(workload::UPDATE_SUBSCRIBER_BIT)?
                .execute((bit_1, s_id))?;
            let changed = conn
                .prepare_cached(workload::UPDATE_SPECIAL_FACILITY)?
                .execute((data_a, s_id, sf_type))?;
            Ok(changed > 0)
        }
        Txn::UpdateLocation {
            sub_nbr,
            vlr_location,
        } => {
            let changed = conn
                .prepare_cached(workload::UPDATE_LOCATION)?
                .execute((vlr_location, sub_nbr))?;
            Ok(changed > 0)
        }
        Txn::InsertCallForwarding {
            sub_nbr,
            sf_type,
            start_time,
            end_time,
            numberx,
        } => {
            let Some(s_id) = subscriber_by_number(conn, sub_nbr)? else {
                return Ok(false);
            };
            let mut stmt = conn.prepare_cached(workload::SPECIAL_FACILITY_TYPES)?;
            let mut rows = stmt.query([s_id])?;
            while rows.next()?.is_some() {}
            drop(rows);
            conn.prepare_cached(workload::INSERT_CALL_FORWARDING)?
                .execute((s_id, sf_type, start_time, end_time, numberx))?;
            Ok(true)
        }
        Txn::DeleteCallForwarding {
            sub_nbr,
            sf_type,
            start_time,
        } => {
            let Some(s_id) = subscriber_by_number(conn, sub_nbr)? else {
                return Ok(false);
            };
            let changed = conn
                .prepare_cached(workload::DELETE_CALL_FORWARDING)?
                .execute((s_id, sf_type, start_time))?;
            Ok(changed > 0)
        }
    }
}

fn subscriber_by_number(conn: &Connection, sub_nbr: &str) -> rusqlite::Result<Option<i64>> {
    conn.prepare_cached(workload::SUBSCRIBER_BY_NUMBER)?
        .query_row([sub_nbr], |row| row.get(0))
        .optional()
}

/// Runs a passive checkpoint every `interval` until told to stop, and returns
/// when each one started and how long it took.
fn checkpointer(
    db_path: &str,
    timeout: Duration,
    interval: Duration,
    stop: &AtomicBool,
) -> Vec<(Instant, Duration)> {
    let conn = open(db_path, timeout);
    let mut checkpoints = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        thread::sleep(interval);
        let started = Instant::now();
        // The pragma returns one row (busy, log, checkpointed). Busy just means
        // it could not finish this time; the next round picks it up.
        let _ = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
        checkpoints.push((started, started.elapsed()));
    }
    checkpoints
}

fn to_sql(param: &Param) -> rusqlite::types::Value {
    match param {
        Param::Int(i) => rusqlite::types::Value::Integer(*i),
        Param::Text(s) => rusqlite::types::Value::Text(s.clone()),
    }
}

fn count_tables(db_path: &str, timeout: Duration) -> TableCounts {
    let conn = open(db_path, timeout);
    let count = |table: &str| {
        conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap() as u64
    };
    TableCounts {
        subscriber: count("subscriber"),
        access_info: count("access_info"),
        special_facility: count("special_facility"),
        call_forwarding: count("call_forwarding"),
    }
}

fn open(db_path: &str, timeout: Duration) -> Connection {
    let conn = Connection::open(db_path).unwrap();
    conn.busy_timeout(timeout).unwrap();
    conn.execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
        .unwrap();
    conn
}
